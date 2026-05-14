"""
Applies a single approved tag to the Confluent Stream Catalog.
Called by the review API when a recommendation is approved.
"""

import logging
import os
from typing import List, Optional

import httpx

from sr_schema import fetch_field_paths_cached, fetch_schema_meta_cached

logger = logging.getLogger("catalog_client")

SR_FIELD_TYPE = "sr_field"
CLASSIFIER_VERSION = "3.1.0"

REQUIRED_SR_ENV_VARS = (
    "CONFLUENT_SR_URL",
    "CONFLUENT_SR_API_KEY",
    "CONFLUENT_SR_API_SECRET",
    "CONFLUENT_SR_CLUSTER_ID",
)


# ---------------------------------------------------------------------------
# Tag definitions — mirror of kafka-pipeline/catalog_tagger.py:TAG_DEFINITIONS
#
# Stream Catalog is Atlas-based: every tag (PII, PCI, …) must be registered as
# a *type definition* before it can be applied to an entity. Calling apply on
# an unregistered tag returns the cryptic "Type ENTITY with name null does not
# exist" 400. We POST these once on the first batch submit and cache the result
# on the module so subsequent submits skip the GET.
# ---------------------------------------------------------------------------
def _tag_def(name: str, description: str) -> dict:
    return {
        "name": name,
        "entityTypes": [SR_FIELD_TYPE],
        "description": description,
        "attributeDefs": [
            {"name": "entity_types",  "typeName": "string", "isOptional": True},
            {"name": "classified_by", "typeName": "string", "isOptional": True},
        ],
    }


TAG_DEFINITIONS = [
    _tag_def("PII",           "Personally Identifiable Information — name, email, phone, date of birth."),
    _tag_def("PHI",           "Protected Health Information — medical records, diagnoses, medications, NPI/DEA numbers."),
    _tag_def("PCI",           "Payment Card data — credit/debit cards, IBAN, SWIFT codes, crypto wallets."),
    _tag_def("CREDENTIALS",   "Authentication secrets — passwords, API keys, tokens, connection strings."),
    _tag_def("FINANCIAL",     "Financial account data — bank account numbers, routing numbers."),
    _tag_def("GOVERNMENT_ID", "Government-issued identifiers — SSN, passport, driver's licence, national tax IDs."),
    _tag_def("BIOMETRIC",     "Biometric identifiers — fingerprints, facial geometry, retina scans, voice prints."),
    _tag_def("GENETIC",       "Genetic and genomic data — DNA sequences, genotypes, genome data."),
    _tag_def("NPI",           "Non-Public Information — insider financials, pre-release earnings, M&A data."),
    _tag_def("LOCATION",      "Precise geolocation and network identifiers — GPS coordinates, IP addresses."),
    _tag_def("MINOR",         "Data relating to a person under 13 or 16 (COPPA / GDPR)."),
]


# Cache: True once we've successfully ensured tag defs exist in this process.
_tag_defs_bootstrapped = False


async def _ensure_tag_definitions(client: httpx.AsyncClient, sr_url: str, auth: tuple) -> None:
    """Idempotent: POST any TAG_DEFINITIONS missing from the catalog."""
    global _tag_defs_bootstrapped
    if _tag_defs_bootstrapped:
        return
    url = f"{sr_url}/catalog/v1/types/tagdefs"
    try:
        existing = await client.get(url, auth=auth)
        existing.raise_for_status()
        existing_names = {t["name"] for t in existing.json()}
        missing = [t for t in TAG_DEFINITIONS if t["name"] not in existing_names]
        if missing:
            resp = await client.post(url, json=missing, auth=auth)
            resp.raise_for_status()
            logger.info("Registered catalog tag definitions: %s", [t["name"] for t in missing])
        _tag_defs_bootstrapped = True
    except httpx.HTTPError as exc:
        logger.warning("Tag-def bootstrap failed (catalog may already have them): %s", exc)
        # Don't flip the cache flag — retry on the next batch submit.


class CatalogNotConfiguredError(RuntimeError):
    """Raised when Stream Catalog env vars are missing.

    Distinct from a tagging failure (apply_tag returns False) so the API layer
    can surface a clear "configure these env vars" message instead of a generic
    502 — the user shouldn't have to check the server log to diagnose this.
    """


def _field_qualified_name(
    sr_cluster_id: str, schema_id: int, record_qualifier: str, field_path: str,
) -> str:
    """Stream Catalog sr_field entityName format.

    Per Confluent's Stream Catalog REST API:
        {cluster_id}:.:{schema_id}:{namespace}.{record_name}.{field_path}

    The record_qualifier is `{namespace}.{record_name}` extracted from the
    Avro schema body (see sr_schema.extract_record_qualifier). We tried
    `{cluster}:.:{subject}.v{version}.{field}` historically — that format
    is REJECTED by Atlas with "Type ENTITY with name null does not exist".
    """
    return f"{sr_cluster_id}:.:{schema_id}:{record_qualifier}.{field_path}"


async def _resolve_version(
    client: httpx.AsyncClient,
    sr_base: str,
    auth: tuple,
    subject: str,
    schema_id: Optional[int],
) -> Optional[int]:
    """Resolve schema_id → version, or fall back to latest if schema_id is None.

    When schema_id is set, we require an exact match — silently falling back to
    'latest' on a transient SR error would tag the field against a different
    schema_id than the recommendation was created with, producing
    qualifiedNames that don't match the entity the reviewer thought they were
    tagging. Return None on any failure; the caller will drop the item.
    """
    if schema_id is not None:
        try:
            resp = await client.get(f"{sr_base}/subjects/{subject}/versions", auth=auth)
            resp.raise_for_status()
            for v in resp.json():
                detail = await client.get(
                    f"{sr_base}/subjects/{subject}/versions/{v}", auth=auth
                )
                if detail.status_code == 200 and detail.json().get("id") == schema_id:
                    return v
            logger.warning(
                "Schema id %d not found in any version of '%s' — dropping",
                schema_id, subject,
            )
        except httpx.HTTPError as e:
            logger.warning(
                "Version lookup failed for '%s' schema_id=%d: %s — dropping (no fallback)",
                subject, schema_id, e,
            )
        return None

    try:
        resp = await client.get(f"{sr_base}/subjects/{subject}/versions/latest", auth=auth)
        resp.raise_for_status()
        return resp.json().get("version")
    except httpx.HTTPError as e:
        logger.error("Cannot resolve latest version for '%s': %s", subject, e)
        return None


async def apply_tags_batch(items: List[dict]) -> tuple[bool, str, list[dict]]:
    """Apply many tags to Stream Catalog in a SINGLE POST.

    Each item must have keys: subject, schema_id, field_path, tag_name,
    entity_type. The Stream Catalog `/catalog/v1/entity/tags` endpoint
    accepts an array of tag entries — batching them avoids creating one
    new schema-version-per-tag, which is what the user complained about.

    SR is the source of truth: every item's field_path is verified against
    the live SR schema body for (subject, version) before POST. Items whose
    field doesn't exist (or whose schema we couldn't fetch) are dropped and
    reported in the third return value rather than blindly POSTed — that's
    what triggers Atlas's cryptic "Type ENTITY with name null" error.

    Returns (ok, error_msg, dropped). On full success, ok=True / err="" /
    dropped=[]. On partial success, ok=True with non-empty dropped. On full
    failure (catalog error or all items dropped), ok=False with err set.
    `dropped` is a list of {"subject", "field_path", "reason"} dicts.
    """
    missing = [v for v in REQUIRED_SR_ENV_VARS if not os.environ.get(v)]
    if missing:
        raise CatalogNotConfiguredError(
            "Stream Catalog not configured — missing env vars: "
            + ", ".join(missing)
        )
    if not items:
        return True, "", []

    sr_url     = os.environ["CONFLUENT_SR_URL"].rstrip("/")
    sr_api_key = os.environ["CONFLUENT_SR_API_KEY"]
    sr_secret  = os.environ["CONFLUENT_SR_API_SECRET"]
    cluster_id = os.environ["CONFLUENT_SR_CLUSTER_ID"]
    auth = (sr_api_key, sr_secret)

    dropped: list[dict] = []

    async with httpx.AsyncClient() as client:
        # Make sure every tag we're about to apply has a registered type def.
        # Without this, the catalog returns "Type ENTITY with name null does
        # not exist" — Atlas refuses to apply unregistered classifications.
        await _ensure_tag_definitions(client, sr_url, auth)

        # One pass over items: resolve version (cached per subject+schema_id),
        # fetch the live schema field-path set + record qualifier (both cached
        # per subject+version), validate, build the POST payload only for
        # survivors. Stream Catalog wants the FLAT entityType+entityName+
        # typeName shape, not a nested entity with classifications.
        version_cache: dict[tuple[str, Optional[int]], Optional[int]] = {}
        payload = []
        for item in items:
            v_key = (item["subject"], item["schema_id"])
            if v_key not in version_cache:
                version_cache[v_key] = await _resolve_version(
                    client, sr_url, auth, item["subject"], item["schema_id"]
                )
            version = version_cache[v_key]
            if version is None:
                dropped.append({
                    "subject":    item["subject"],
                    "field_path": item["field_path"],
                    "reason":     "could not resolve schema version in SR",
                })
                continue

            valid_paths = await fetch_field_paths_cached(
                client, sr_url, auth, item["subject"], version
            )
            schema_id, record_qualifier = await fetch_schema_meta_cached(
                client, sr_url, auth, item["subject"], version
            )
            clean_path = item["field_path"].replace("[", ".").replace("]", "").strip(".")
            if not valid_paths or schema_id is None or not record_qualifier:
                # Fail closed: never POST a tag against a schema we couldn't
                # read or parse. Empty set means fetch failed OR the schema
                # type is one we don't yet support (JSON/Protobuf).
                dropped.append({
                    "subject":    item["subject"],
                    "field_path": item["field_path"],
                    "reason":     "could not fetch/parse SR schema",
                })
                continue
            if clean_path not in valid_paths:
                dropped.append({
                    "subject":    item["subject"],
                    "field_path": item["field_path"],
                    "reason":     "field not in current SR schema",
                })
                continue

            payload.append({
                "entityType": SR_FIELD_TYPE,
                "entityName": _field_qualified_name(
                    cluster_id, schema_id, record_qualifier, clean_path,
                ),
                "typeName": item["tag_name"],
            })

        if dropped:
            logger.warning("Batch apply: dropped %d items not in live SR schema: %s",
                           len(dropped), dropped)
        if not payload:
            return False, (
                f"all {len(items)} item(s) dropped — none match the live SR schema "
                f"(see dropped list)"
            ), dropped

        try:
            resp = await client.post(
                f"{sr_url}/catalog/v1/entity/tags",
                json=payload, auth=auth,
            )
        except httpx.HTTPError as e:
            return False, f"catalog API error: {e}", dropped

        if resp.status_code in (200, 201, 204, 409):
            logger.info("Batch tagged %d field(s) in 1 POST (%d dropped pre-flight)",
                        len(payload), len(dropped))
            return True, "", dropped

        # Defensive fallback: Atlas's "Type ENTITY with name null" 400 should
        # no longer happen now that we pre-validate, but keep the translation
        # in case SR's view and Atlas's view ever diverge.
        body = resp.text[:500]
        if resp.status_code == 400 and "Type ENTITY" in body and "null" in body:
            paths = ", ".join(f"{i['subject']}/{i['field_path']}" for i in items[:3])
            more  = f" (+{len(items)-3} more)" if len(items) > 3 else ""
            return False, (
                f"Catalog rejected the batch: at least one field doesn't exist "
                f"as a registered schema field. Tried: {paths}{more}. Verify the "
                f"field_path matches a real field in the SR schema for the "
                f"subject. Raw: {body}"
            ), dropped
        return False, f"catalog returned {resp.status_code}: {body}", dropped
