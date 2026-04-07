"""
Applies a single approved tag to the Confluent Stream Catalog.
Called by the review API when a recommendation is approved.
"""

import logging
import os
from typing import List, Optional

import httpx

logger = logging.getLogger("catalog_client")

SR_FIELD_TYPE = "sr_field"
CLASSIFIER_VERSION = "3.1.0"


def _field_qualified_name(sr_cluster_id: str, subject: str, version: int, field_path: str) -> str:
    return f"{sr_cluster_id}:.:{subject}.v{version}.{field_path}"


async def _resolve_version(
    client: httpx.AsyncClient,
    sr_base: str,
    auth: tuple,
    subject: str,
    schema_id: Optional[int],
) -> Optional[int]:
    """Resolve schema_id → version, or fall back to latest."""
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
        except httpx.HTTPError as e:
            logger.warning("Version lookup failed for schema %d: %s — falling back to latest", schema_id, e)

    # Fall back to latest
    try:
        resp = await client.get(f"{sr_base}/subjects/{subject}/versions/latest", auth=auth)
        resp.raise_for_status()
        return resp.json().get("version")
    except httpx.HTTPError as e:
        logger.error("Cannot resolve latest version for '%s': %s", subject, e)
        return None


async def apply_tag(
    subject: str,
    schema_id: Optional[int],
    field_path: str,
    tag_name: str,
    entity_type: str,
) -> bool:
    """
    Apply a single tag to a schema field in the Confluent Stream Catalog.
    Returns True on success (including 409 already-exists), False on error.
    """
    sr_url     = os.environ["CONFLUENT_SR_URL"].rstrip("/")
    sr_api_key = os.environ["CONFLUENT_SR_API_KEY"]
    sr_secret  = os.environ["CONFLUENT_SR_API_SECRET"]
    cluster_id = os.environ["CONFLUENT_SR_CLUSTER_ID"]
    auth = (sr_api_key, sr_secret)

    async with httpx.AsyncClient() as client:
        version = await _resolve_version(client, sr_url, auth, subject, schema_id)
        if version is None:
            logger.error("Cannot tag field '%s' — schema version unresolvable", field_path)
            return False

        # Strip array indices  e.g. "items[0].name" → "items.name"
        clean_path = field_path.replace("[", ".").replace("]", "").strip(".")
        qualified_name = _field_qualified_name(cluster_id, subject, version, clean_path)

        payload = [
            {
                "typeName": SR_FIELD_TYPE,
                "attributes": {"qualifiedName": qualified_name},
                "classifications": [
                    {
                        "typeName": tag_name,
                        "attributes": {
                            "entity_types": entity_type,
                            "classified_by": f"auto-classifier-v{CLASSIFIER_VERSION}",
                        },
                    }
                ],
            }
        ]

        try:
            resp = await client.post(
                f"{sr_url}/catalog/v1/entity/tags",
                json=payload,
                auth=auth,
            )
            if resp.status_code in (200, 201, 204, 409):
                logger.info(
                    "Tagged '%s' (subject=%s v%d) → %s", field_path, subject, version, tag_name
                )
                return True
            logger.warning("Unexpected status %d tagging '%s': %s", resp.status_code, field_path, resp.text)
            return False
        except httpx.HTTPError as e:
            logger.error("Catalog API error tagging '%s': %s", field_path, e)
            return False
