"""
Confluent Stream Catalog field tagger.

After classification, applies data tags directly to schema fields in the
Confluent Stream Catalog so data stewards get meaningful, actionable labels.

Tags applied per field (one tag per field, highest-priority tag wins):
  PHI           → Protected Health Information
  CREDENTIALS   → Passwords, tokens, API keys
  PCI           → Payment Card / financial account data
  FINANCIAL     → Bank accounts, routing numbers
  GOVERNMENT_ID → SSN, passport, driver's licence, national IDs
  BIOMETRIC     → Fingerprints, facial geometry, retina scans
  GENETIC       → DNA sequences, genomic data
  NPI           → Non-Public Information — insider financials, M&A data
  PII           → Personally Identifiable Information
  LOCATION      → GPS coordinates, IP addresses
  MINOR         → Data relating to a person under 13 or 16

All operations are idempotent — safe to call on every message.
A local cache prevents redundant API calls for already-tagged fields.
"""

import logging
import struct
from typing import Any, Dict, List, Optional, Set

import httpx

logger = logging.getLogger("catalog_tagger")

SR_FIELD_TYPE = "sr_field"

# ---------------------------------------------------------------------------
# Tag definitions — one entry per DataTag (11 total)
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

# Tag priority — highest wins when multiple tags detected on the same field
_CATEGORY_PRIORITY = [
    "PHI", "CREDENTIALS", "PCI", "FINANCIAL", "GOVERNMENT_ID",
    "BIOMETRIC", "GENETIC", "NPI", "PII", "LOCATION", "MINOR",
]


def _field_qualified_name(sr_cluster_id: str, subject: str, version: int, field_path: str) -> str:
    """
    Build the Confluent Stream Catalog qualified name for an sr_field entity.
    Format: {sr_cluster_id}:.:{subject}.v{version}.{field_path}
    """
    return f"{sr_cluster_id}:.:{subject}.v{version}.{field_path}"


def extract_schema_id_from_wire(raw: bytes) -> Optional[int]:
    """
    Extract the schema ID from a Confluent wire-format message.
    Wire format: 0x00 (magic) | 4-byte big-endian schema ID | payload
    Returns None if the message is not in Confluent wire format.
    """
    if len(raw) < 5 or raw[0] != 0x00:
        return None
    _, schema_id = struct.unpack(">bI", raw[:5])
    return schema_id


def _highest_category(categories: List[str]) -> str:
    """Return the highest-priority tag from a list."""
    for cat in _CATEGORY_PRIORITY:
        if cat in categories:
            return cat
    return "PII"


class CatalogTagger:
    def __init__(
        self,
        sr_url: str,
        sr_api_key: str,
        sr_api_secret: str,
        sr_cluster_id: str,
        classifier_version: str = "3.1.0",
    ):
        self._base_url = sr_url.rstrip("/")
        self._auth = (sr_api_key, sr_api_secret)
        self._sr_cluster_id = sr_cluster_id
        self._classifier_version = classifier_version

        # Cache of (subject, version, field_path, tag) tuples already applied
        self._tagged: Set[tuple] = set()
        self._tags_bootstrapped = False

    # -------------------------------------------------------------------------
    # Bootstrap
    # -------------------------------------------------------------------------
    async def ensure_tag_definitions(self, client: httpx.AsyncClient) -> None:
        """Create catalog tag definitions if they don't already exist."""
        if self._tags_bootstrapped:
            return

        url = f"{self._base_url}/catalog/v1/types/tagdefs"
        try:
            existing_resp = await client.get(url, auth=self._auth)
            existing_resp.raise_for_status()
            existing_names = {t["name"] for t in existing_resp.json()}

            to_create = [t for t in TAG_DEFINITIONS if t["name"] not in existing_names]
            if to_create:
                resp = await client.post(url, json=to_create, auth=self._auth)
                resp.raise_for_status()
                logger.info("Created catalog tag definitions: %s", [t["name"] for t in to_create])
            else:
                logger.debug("All tag definitions already exist.")

            self._tags_bootstrapped = True
        except httpx.HTTPError as e:
            logger.error("Failed to bootstrap tag definitions: %s", e)

    # -------------------------------------------------------------------------
    # Schema version lookup
    # -------------------------------------------------------------------------
    async def _get_latest_version(self, client: httpx.AsyncClient, subject: str) -> Optional[int]:
        url = f"{self._base_url}/subjects/{subject}/versions/latest"
        try:
            resp = await client.get(url, auth=self._auth)
            resp.raise_for_status()
            return resp.json().get("version")
        except httpx.HTTPError as e:
            logger.warning("Could not fetch latest version for '%s': %s", subject, e)
            return None

    async def _get_version_for_schema_id(
        self, client: httpx.AsyncClient, subject: str, schema_id: int
    ) -> Optional[int]:
        url = f"{self._base_url}/subjects/{subject}/versions"
        try:
            resp = await client.get(url, auth=self._auth)
            resp.raise_for_status()
            for version in resp.json():
                v_resp = await client.get(
                    f"{self._base_url}/subjects/{subject}/versions/{version}",
                    auth=self._auth,
                )
                if v_resp.status_code == 200 and v_resp.json().get("id") == schema_id:
                    return version
        except httpx.HTTPError as e:
            logger.warning("Version lookup failed for '%s' schema %d: %s", subject, schema_id, e)
        return None

    # -------------------------------------------------------------------------
    # Tag application
    # -------------------------------------------------------------------------
    async def _apply_tag(
        self,
        client: httpx.AsyncClient,
        subject: str,
        version: int,
        field_path: str,
        tag_name: str,
        entity_types: List[str],
    ) -> None:
        cache_key = (subject, version, field_path, tag_name)
        if cache_key in self._tagged:
            return

        qualified_name = _field_qualified_name(
            self._sr_cluster_id, subject, version, field_path
        )

        payload = [
            {
                "typeName": SR_FIELD_TYPE,
                "attributes": {"qualifiedName": qualified_name},
                "classifications": [
                    {
                        "typeName": tag_name,
                        "attributes": {
                            "entity_types": ", ".join(entity_types),
                            "classified_by": f"auto-classifier-v{self._classifier_version}",
                        },
                    }
                ],
            }
        ]

        try:
            resp = await client.post(
                f"{self._base_url}/catalog/v1/entity/tags",
                json=payload,
                auth=self._auth,
            )
            if resp.status_code in (200, 201, 204):
                self._tagged.add(cache_key)
                logger.info("Tagged '%s' (subject=%s v%d) → %s", field_path, subject, version, tag_name)
            elif resp.status_code == 409:
                self._tagged.add(cache_key)  # already exists — idempotent
            else:
                logger.warning("Unexpected status %d tagging '%s': %s", resp.status_code, field_path, resp.text)
        except httpx.HTTPError as e:
            logger.error("Failed to tag field '%s': %s", field_path, e)

    # -------------------------------------------------------------------------
    # Public entry point
    # -------------------------------------------------------------------------
    async def apply_classifications(
        self,
        client: httpx.AsyncClient,
        topic: str,
        detected_entities: Dict[str, List[Dict[str, Any]]],
        schema_id: Optional[int] = None,
    ) -> None:
        """
        Apply category-based catalog tags for all fields that had entities detected.

        Each field receives the highest-priority category tag among its detected
        entities (e.g. a field with both PERSON and MEDICAL_CONDITION gets PHI).

        Args:
            client:            shared httpx client
            topic:             source Kafka topic (subject = topic + "-value")
            detected_entities: from classifier response — {field_path: [entity, ...]}
                               each entity dict must have "entity_type" and "category"
            schema_id:         Confluent wire-format schema ID (optional)
        """
        if not detected_entities:
            return

        await self.ensure_tag_definitions(client)

        subject = f"{topic}-value"

        version = None
        if schema_id is not None:
            version = await self._get_version_for_schema_id(client, subject, schema_id)
        if version is None:
            version = await self._get_latest_version(client, subject)
        if version is None:
            logger.warning("Cannot resolve schema version for '%s' — skipping catalog tagging", subject)
            return

        for field_path, entities in detected_entities.items():
            if not entities:
                continue

            entity_types = [e["entity_type"] for e in entities]
            # Use category from classifier response if present; fall back to entity_type lookup
            tags = [e.get("tag", "PII") for e in entities]
            tag = _highest_category(tags)

            # Strip array indices (e.g. "items[0].name" → "items.name")
            clean_path = field_path.replace("[", ".").replace("]", "").strip(".")

            await self._apply_tag(client, subject, version, clean_path, tag, entity_types)
