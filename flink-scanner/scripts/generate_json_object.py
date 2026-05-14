#!/usr/bin/env python3
"""Generate the JSON_OBJECT field list for scan.sql from the live SR schema.

Why this exists: scan.sql USED to hardcode ~50 KEY/VALUE pairs inside
JSON_OBJECT(...). That meant adding/removing fields in SR required editing
SQL by hand, and a stale list silently sent NULLs to the classifier. SR is
the source of truth — we now fetch the actual subject schema at start_scan
time and emit the exact field list it contains.

Invocation:
    python3 generate_json_object.py <sr_url> <sr_key> <sr_secret> <subject>

Stdout: a comma-separated KEY '<name>' VALUE p.`<name>` block ready to
substitute into the {json_object_fields} placeholder in scan.sql.

Failure modes:
    - SR fetch fails / non-200 → exit 2 with stderr message
    - Schema is not Avro → exit 3 (JSON/Protobuf not yet supported here)
    - Schema has no record fields → exit 4
The caller (start_scan.sh) MUST treat any non-zero exit as a hard fail.
"""
from __future__ import annotations

import base64
import json
import sys
import urllib.request
import urllib.error


def fetch_schema(sr_url: str, sr_key: str, sr_secret: str, subject: str) -> dict:
    base = sr_url.rstrip("/")
    auth = base64.b64encode(f"{sr_key}:{sr_secret}".encode()).decode()
    req = urllib.request.Request(
        f"{base}/subjects/{subject}/versions/latest",
        headers={"Authorization": f"Basic {auth}"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def top_level_field_names(schema_body: dict) -> list[str]:
    """Return the top-level field names of an Avro record schema."""
    schema_type = (schema_body.get("schemaType") or "AVRO").upper()
    if schema_type != "AVRO":
        raise SystemExit(f"schemaType={schema_type} not supported (only AVRO)")
    raw = schema_body.get("schema")
    if not raw:
        raise SystemExit("SR response missing 'schema' field")
    schema = json.loads(raw)
    if schema.get("type") != "record":
        raise SystemExit(f"top-level schema is not a record (got type={schema.get('type')})")
    fields = [f["name"] for f in schema.get("fields", []) if f.get("name")]
    if not fields:
        raise SystemExit("schema has zero fields — nothing to classify")
    return fields


def render_json_object(fields: list[str]) -> str:
    """Emit a multi-line KEY/VALUE block for use inside JSON_OBJECT(...)."""
    # 16-space indent matches the surrounding scan.sql formatting.
    return ",\n".join(
        f"                KEY '{name}' VALUE p.`{name}`"
        for name in fields
    )


def main(argv: list[str]) -> int:
    if len(argv) != 5:
        print(f"Usage: {argv[0]} <sr_url> <sr_key> <sr_secret> <subject>", file=sys.stderr)
        return 1
    _, sr_url, sr_key, sr_secret, subject = argv
    try:
        body = fetch_schema(sr_url, sr_key, sr_secret, subject)
    except urllib.error.HTTPError as e:
        print(f"ERROR: SR fetch failed for {subject}: HTTP {e.code}: {e.read().decode()[:200]}",
              file=sys.stderr)
        return 2
    except (urllib.error.URLError, OSError) as e:
        print(f"ERROR: SR connection failed: {e}", file=sys.stderr)
        return 2
    try:
        names = top_level_field_names(body)
    except SystemExit as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 3
    print(render_json_object(names))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
