"""
Confluent Stream Catalog field tagger.

After classification, applies sensitivity tags to schema fields in the
Confluent Stream Catalog so data stewards can see PII exposure across
the entire catalog — without looking at message content.

Tag hierarchy applied:
  HIGH sensitivity  → "PII" tag
  MEDIUM sensitivity → "SENSITIVE" tag
  LOW sensitivity   → "INTERNAL" tag

All operations are idempotent — safe to call on every message.
A local cache prevents redundant API calls for already-tagged fields.
"""

import logging
import struct
from typing import Any, Dict, List, Optional, Set

import httpx

logger = logging.getLogger("catalog_tagger")

# Confluent entity type for a schema field in the Stream Catalog
SR_FIELD_TYPE = "sr_field"

# Maps sensitivity level → catalog tag name
SENSITIVITY_TAG_MAP = {
    "HIGH":   "PII",
    "MEDIUM": "SENSITIVE",
    "LOW":    "INTERNAL",
}

# Tag definitions to ensure exist before use
TAG_DEFINITIONS = [
    {
        "name": "PII",
        "entityTypes": [SR_FIELD_TYPE],
        "description": "Field contains Personally Identifiable Information",
        "attributeDefs": [
            {"name": "entity_types", "typeName": "string", "isOptional": True},
            {"name": "classified_by", "typeName": "string", "isOptional": True},
        ],
    },
    {
        "name": "SENSITIVE",
        "entityTypes": [SR_FIELD_TYPE],
        "description": "Field contains sensitive data (medium sensitivity)",
        "attributeDefs": [
            {"name": "entity_types", "typeName": "string", "isOptional": True},
            {"name": "classified_by", "typeName": "string", "isOptional": True},
        ],
    },
    {
        "name": "INTERNAL",
        "entityTypes": [SR_FIELD_TYPE],
        "description": "Field contains internal data (low sensitivity)",
        "attributeDefs": [
            {"name": "entity_types", "typeName": "string", "isOptional": True},
            {"name": "classified_by", "typeName": "string", "isOptional": True},
        ],
    },
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


class CatalogTagger:
    def __init__(
        self,
        sr_url: str,
        sr_api_key: str,
        sr_api_secret: str,
        sr_cluster_id: str,
        classifier_version: str = "1.0.0",
    ):
        self._base_url = sr_url.rstrip("/")
        self._auth = (sr_api_key, sr_api_secret)
        self._sr_cluster_id = sr_cluster_id
        self._classifier_version = classifier_version

        # Cache of (subject, version, field_path, tag) already applied
        # Prevents duplicate API calls within a single process lifetime
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
            logger.warning("Could not fetch latest version for subject '%s': %s", subject, e)
            return None

    async def _get_version_for_schema_id(
        self, client: httpx.AsyncClient, subject: str, schema_id: int
    ) -> Optional[int]:
        """Resolve a schema ID to a version number within a subject."""
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
            logger.warning("Version lookup failed for subject '%s' schema %d: %s", subject, schema_id, e)
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
                logger.info(
                    "Tagged field '%s' (subject=%s v%d) as %s",
                    field_path, subject, version, tag_name,
                )
            elif resp.status_code == 409:
                # Already tagged — treat as success
                self._tagged.add(cache_key)
            else:
                logger.warning(
                    "Unexpected status %d tagging field '%s': %s",
                    resp.status_code, field_path, resp.text,
                )
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
        Apply catalog tags for all detected PII fields.

        Args:
            client:           shared httpx client
            topic:            source Kafka topic (used to derive SR subject)
            detected_entities: from classifier — {field_path: [entity, ...]}
            schema_id:        Confluent schema ID from wire format (optional)
        """
        if not detected_entities:
            return

        await self.ensure_tag_definitions(client)

        subject = f"{topic}-value"

        # Resolve which schema version to tag
        if schema_id is not None:
            version = await self._get_version_for_schema_id(client, subject, schema_id)
        else:
            version = None

        if version is None:
            version = await self._get_latest_version(client, subject)

        if version is None:
            logger.warning("Cannot resolve schema version for subject '%s' — skipping catalog tagging", subject)
            return

        # Apply tags per field
        for field_path, entities in detected_entities.items():
            if not entities:
                continue

            # Determine highest sensitivity for this field
            entity_types = [e["entity_type"] for e in entities]
            high_types = {e for e in entity_types if e in _HIGH_ENTITIES}
            medium_types = {e for e in entity_types if e in _MEDIUM_ENTITIES}

            if high_types:
                tag = "PII"
            elif medium_types:
                tag = "SENSITIVE"
            else:
                tag = "INTERNAL"

            # Strip array indices from path (e.g. "items[0].name" → "items.name")
            clean_path = field_path.replace("[", ".").replace("]", "").strip(".")

            await self._apply_tag(client, subject, version, clean_path, tag, entity_types)


# Entity type sets (mirrors classifier/main.py — keep in sync)
_HIGH_ENTITIES = {
    "US_SSN", "CREDIT_CARD", "IBAN_CODE", "BANK_ACCOUNT",
    "US_BANK_ROUTING", "PASSPORT", "DRIVER_LICENSE", "MEDICAL_RECORD",
    "US_ITIN", "SWIFT_CODE",
}
_MEDIUM_ENTITIES = {
    "PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER", "LOCATION", "DATE_TIME", "IP_ADDRESS",
}
