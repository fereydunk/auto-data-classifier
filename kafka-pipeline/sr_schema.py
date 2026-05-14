"""
SR-as-source-of-truth helpers for Stream Catalog tagging.

Whenever we POST a tag to the catalog, we must first prove that the
field actually exists in the live SR schema for that subject — otherwise
Atlas rejects the POST with the cryptic "Type ENTITY with name null does
not exist" error and the whole batch fails.

Public surface:
    fetch_schema_body(...)        — GET subjects/{s}/versions/{v}
    extract_field_paths(...)      — walk an Avro schema → set of dotted paths
    fetch_field_paths_cached(...) — combo + 60-s TTL cache
"""

import json
import logging
import time
from typing import Any, Optional

import httpx

logger = logging.getLogger("sr_schema")

_TTL_SECONDS = 60
_cache: dict[tuple[str, int], tuple[set[str], float]] = {}


async def fetch_schema_body(
    client: httpx.AsyncClient,
    sr_base: str,
    auth: tuple[str, str],
    subject: str,
    version: int,
) -> Optional[dict]:
    """GET the registered schema for (subject, version) and return the parsed body.

    Returns the dict {schema, schemaType, ...} or None on any failure (with a
    warning log). Callers MUST treat None as "fail closed" — never POST a tag
    when we couldn't read the live schema.
    """
    try:
        resp = await client.get(
            f"{sr_base}/subjects/{subject}/versions/{version}",
            auth=auth,
            timeout=10.0,
        )
        if resp.status_code != 200:
            logger.warning(
                "SR fetch failed for %s v%d: HTTP %d", subject, version, resp.status_code
            )
            return None
        return resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("SR fetch error for %s v%d: %s", subject, version, exc)
        return None


def extract_record_qualifier(schema_body: Optional[dict]) -> Optional[str]:
    """Return the {namespace}.{name} qualifier of the top-level Avro record.

    Stream Catalog's sr_field entities use this as part of the qualifiedName:
        {cluster_id}:.:{schema_id}:{namespace}.{record_name}.{field_path}
    Returns None if the schema body is unusable.
    """
    if not schema_body:
        return None
    raw = schema_body.get("schema")
    if not raw:
        return None
    try:
        schema = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, json.JSONDecodeError):
        return None
    name = schema.get("name")
    if not name:
        return None
    ns = schema.get("namespace")
    return f"{ns}.{name}" if ns else name


def extract_field_paths(schema_body: Optional[dict]) -> set[str]:
    """Walk an Avro schema dict and return every dotted field path.

    The body is what SR returns from /subjects/{s}/versions/{v}, i.e.:
        {"subject": "...", "version": 1, "id": 100004,
         "schema": "<JSON-encoded Avro schema>",
         "schemaType": "AVRO" (or absent — defaults to AVRO)}

    Returns paths matching the convention used by
    catalog_client._field_qualified_name AFTER its [→.] / strip(".")
    normalization. Examples:
        {name: ssn, type: string}                → {"ssn"}
        {name: customer, type: record{email}}    → {"customer", "customer.email"}
        {name: items, type: array<record{sku}>}  → {"items", "items.sku"}

    Returns the empty set on any failure (unsupported schema type, malformed
    body, parse error). Callers MUST treat empty as "fail closed".
    """
    if not schema_body:
        return set()
    schema_type = (schema_body.get("schemaType") or "AVRO").upper()
    if schema_type != "AVRO":
        logger.warning(
            "schemaType=%s not supported for field validation (subject=%s v%s) — "
            "treating as zero valid fields",
            schema_type, schema_body.get("subject"), schema_body.get("version"),
        )
        return set()

    raw = schema_body.get("schema")
    if not raw:
        logger.warning("SR body missing 'schema' field for %s v%s",
                       schema_body.get("subject"), schema_body.get("version"))
        return set()

    try:
        schema = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, json.JSONDecodeError) as exc:
        logger.warning("malformed Avro JSON: %s", exc)
        return set()

    out: set[str] = set()
    _walk_avro(schema, "", out)
    return out


def _walk_avro(node: Any, prefix: str, out: set[str]) -> None:
    """Recursively collect dotted field paths from an Avro schema node.

    Handles: record (with fields[]), union (e.g. ["null", record]), array
    (with items), map (with values). Primitive / enum / fixed leaves are
    already accounted for by the parent's path being recorded.
    """
    if isinstance(node, list):
        for branch in node:
            _walk_avro(branch, prefix, out)
        return

    if not isinstance(node, dict):
        return

    t = node.get("type")
    if t == "record":
        for f in node.get("fields", []) or []:
            name = f.get("name")
            if not name:
                continue
            child_path = f"{prefix}.{name}" if prefix else name
            out.add(child_path)
            _walk_avro(f.get("type"), child_path, out)
    elif t == "array":
        items = node.get("items")
        if items is not None:
            _walk_avro(items, prefix, out)
    elif t == "map":
        values = node.get("values")
        if values is not None:
            _walk_avro(values, prefix, out)
    elif isinstance(t, (list, dict)):
        _walk_avro(t, prefix, out)


async def fetch_field_paths_cached(
    client: httpx.AsyncClient,
    sr_base: str,
    auth: tuple[str, str],
    subject: str,
    version: int,
) -> set[str]:
    """Return the valid field-path set for (subject, version), with 60-s TTL.

    Cache miss → fetch + parse + memoize. Empty result is also cached (so a
    flapping SR doesn't get hammered) — callers should already treat empty
    as fail-closed regardless.
    """
    now = time.monotonic()
    cached = _cache.get((subject, version))
    if cached and cached[1] > now:
        return cached[0]
    body = await fetch_schema_body(client, sr_base, auth, subject, version)
    paths = extract_field_paths(body)
    _cache[(subject, version)] = (paths, now + _TTL_SECONDS)
    return paths


# Second cache: (subject, version) → (schema_id, record_qualifier). Same TTL.
# Stream Catalog's sr_field entityName needs both schema_id AND the Avro
# {namespace}.{record_name} prefix, neither of which is available from
# extract_field_paths alone.
_meta_cache: dict[tuple[str, int], tuple[Optional[int], Optional[str], float]] = {}


async def fetch_schema_meta_cached(
    client: httpx.AsyncClient,
    sr_base: str,
    auth: tuple[str, str],
    subject: str,
    version: int,
) -> tuple[Optional[int], Optional[str]]:
    """Return (schema_id, record_qualifier) for (subject, version)."""
    now = time.monotonic()
    cached = _meta_cache.get((subject, version))
    if cached and cached[2] > now:
        return cached[0], cached[1]
    body = await fetch_schema_body(client, sr_base, auth, subject, version)
    sid = body.get("id") if body else None
    rq = extract_record_qualifier(body)
    _meta_cache[(subject, version)] = (sid, rq, now + _TTL_SECONDS)
    return sid, rq


def _clear_cache_for_tests() -> None:
    """Test hook: drop the in-memory schema-paths cache."""
    _cache.clear()
    _meta_cache.clear()
