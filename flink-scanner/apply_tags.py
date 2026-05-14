#!/usr/bin/env python3
"""
apply_tags.py — Interactive schema tagger

Reads the latest batch of field classifications from the Flink scan-results
topic, lets the user approve or reject each new/changed tag, then applies all
approved tags in a single schema update (one new schema version).

Supports: AVRO · JSON Schema · Protobuf

Usage:
    python apply_tags.py \\
        --topic     payments \\
        --sr-url    https://psrc-xxx.us-east-1.aws.confluent.cloud \\
        --sr-key    KEY \\
        --sr-secret SECRET \\
        --bootstrap https://pkc-xxx.confluent.cloud:9092 \\
        --kafka-key KAFKA_KEY \\
        --kafka-secret KAFKA_SECRET \\
        [--subject  payments-value]   # default: {topic}-value
        [--yes]                       # approve all without prompting

Requires:
    pip install confluent-kafka
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import sys
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

try:
    from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING
    _KAFKA_AVAILABLE = True
except ImportError:
    _KAFKA_AVAILABLE = False


# ── Schema Registry client ────────────────────────────────────────────────────

class SchemaRegistryClient:
    def __init__(self, url: str, key: str, secret: str) -> None:
        self.base = url.rstrip("/")
        self._auth = "Basic " + base64.b64encode(f"{key}:{secret}".encode()).decode()

    def _request(self, method: str, path: str, body: Any = None) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        req = Request(
            self.base + path,
            data=data,
            headers={
                "Authorization": self._auth,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method=method,
        )
        try:
            with urlopen(req) as resp:
                return json.loads(resp.read())
        except HTTPError as e:
            raise RuntimeError(
                f"SR {method} {path} → HTTP {e.code}: {e.read().decode()}"
            ) from e

    def get_latest(self, subject: str) -> dict:
        return self._request("GET", f"/subjects/{subject}/versions/latest")

    def register(self, subject: str, schema_str: str, schema_type: str) -> dict:
        payload: dict[str, Any] = {"schema": schema_str}
        if schema_type != "AVRO":          # AVRO is the SR default; omitting avoids a 422 on older SR versions
            payload["schemaType"] = schema_type
        return self._request("POST", f"/subjects/{subject}/versions", payload)


# ── Avro patcher ──────────────────────────────────────────────────────────────

class AvroPatcher:
    """
    Adds  "confluent:tags": ["TAG"]  to Avro field definitions.

    Handles:
      - Top-level fields
      - Nested records (dot-notation path, any depth)
      - Nullable unions:  ["null", {record}]
      - Array/map item records
      - Existing confluent:tags (merges, no duplicates)
    """

    def patch(self, schema_str: str, approvals: list[dict]) -> str:
        schema = json.loads(schema_str)
        for a in approvals:
            self._add_tag(schema, a["field_path"].split("."), a["tag"])
        return json.dumps(schema, indent=2)

    def _add_tag(self, node: Any, parts: list[str], tag: str) -> None:
        record = self._extract_record(node)
        if record is None or not parts:
            return
        target = parts[0]
        for field in record.get("fields", []):
            if field["name"] == target:
                if len(parts) == 1:
                    existing: list[str] = field.get("confluent:tags", [])
                    if tag not in existing:
                        existing.append(tag)
                    field["confluent:tags"] = existing
                else:
                    self._add_tag(field["type"], parts[1:], tag)
                break

    def _extract_record(self, node: Any) -> dict | None:
        if isinstance(node, dict):
            if node.get("type") == "record":
                return node
            # Unwrap array/map containers
            inner = node.get("items") or node.get("values")
            if inner:
                return self._extract_record(inner)
        if isinstance(node, list):
            for item in node:
                r = self._extract_record(item)
                if r:
                    return r
        return None


# ── JSON Schema patcher ───────────────────────────────────────────────────────

class JsonSchemaPatcher:
    """
    Adds  "confluent:tags": ["TAG"]  to JSON Schema field definitions.

    Traverses  properties → {field} → properties → ...  following the
    dot-notation path.  Also handles  items  (array element schemas).
    """

    def patch(self, schema_str: str, approvals: list[dict]) -> str:
        schema = json.loads(schema_str)
        for a in approvals:
            self._add_tag(schema, a["field_path"].split("."), a["tag"])
        return json.dumps(schema, indent=2)

    def _add_tag(self, node: dict, parts: list[str], tag: str) -> None:
        if not parts:
            return
        target = parts[0]
        props = node.get("properties", {})
        if target not in props:
            # Descend into array item schema if present
            items = node.get("items")
            if isinstance(items, dict):
                self._add_tag(items, parts, tag)
            return
        field = props[target]
        if len(parts) == 1:
            existing: list[str] = field.get("confluent:tags", [])
            if tag not in existing:
                existing.append(tag)
            field["confluent:tags"] = existing
        else:
            self._add_tag(field, parts[1:], tag)


# ── Protobuf patcher ──────────────────────────────────────────────────────────

class ProtobufPatcher:
    """
    Text-based .proto patcher.

    Adds  [(confluent.field_meta) = {tags: ["TAG"]}]  to field declarations.
    When a field already carries confluent.field_meta the existing tags list
    is extended (no duplicates).

    Handles:
      - Top-level and nested message fields (dot-notation path, any depth)
      - Fields with no options, unrelated options, or existing confluent.field_meta
        in either scalar form  .tags = "X"  or array form  = {tags: ["X"]}
      - repeated / optional / required field labels
      - Auto-inserts  import "confluent/meta.proto";  when absent
    """

    _META_IMPORT = 'import "confluent/meta.proto";'

    def patch(self, schema_str: str, approvals: list[dict]) -> str:
        if self._META_IMPORT not in schema_str:
            schema_str = self._insert_import(schema_str)
        for a in approvals:
            schema_str = self._add_tag(schema_str, a["field_path"].split("."), a["tag"])
        return schema_str

    # ── helpers ───────────────────────────────────────────────────────────────

    def _insert_import(self, text: str) -> str:
        """Insert  import "confluent/meta.proto";  after the last existing import
        or after the syntax declaration."""
        matches = list(re.finditer(r'^import\s+"[^"]+";', text, re.MULTILINE))
        if matches:
            pos = matches[-1].end()
        else:
            m = re.search(r'^syntax\s*=\s*"[^"]+"\s*;', text, re.MULTILINE)
            pos = m.end() if m else 0
        return text[:pos] + f"\n{self._META_IMPORT}" + text[pos:]

    def _message_block(self, text: str, message_name: str) -> tuple[int, int, int] | tuple[None, None, None]:
        """Return (block_start, content_start, content_end) for a named message."""
        m = re.search(rf'\bmessage\s+{re.escape(message_name)}\s*\{{', text)
        if not m:
            return None, None, None
        depth, i = 1, m.end()
        while i < len(text) and depth:
            if text[i] == '{':
                depth += 1
            elif text[i] == '}':
                depth -= 1
            i += 1
        return m.start(), m.end(), i - 1

    def _field_message_type(self, text: str, field_name: str) -> str | None:
        """Extract the message type of a named field, skipping repeated/optional/required."""
        m = re.search(
            rf'\b(?:repeated\s+|optional\s+|required\s+)?(\w[\w.]*)\s+{re.escape(field_name)}\s*=\s*\d+',
            text,
        )
        return m.group(1) if m else None

    def _add_tag(self, text: str, parts: list[str], tag: str) -> str:
        if len(parts) == 1:
            return self._tag_field(text, parts[0], tag)
        # Nested path: find the message type of parts[0], recurse inside its block
        msg_type = self._field_message_type(text, parts[0])
        if not msg_type:
            return text
        _, c_start, c_end = self._message_block(text, msg_type)
        if c_start is None:
            return text
        patched_inner = self._add_tag(text[c_start:c_end], parts[1:], tag)
        return text[:c_start] + patched_inner + text[c_end:]

    def _tag_field(self, text: str, field_name: str, tag: str) -> str:
        """Add confluent:tags to a single field declaration within a text block."""
        # Matches: [repeated|optional|required] type field_name = number [options] ;
        pattern = rf'(\b(?:(?:repeated|optional|required)\s+)?\w[\w.]*\s+{re.escape(field_name)}\s*=\s*\d+\s*)(\[([^\]]*)\])?\s*;'

        def replacer(m: re.Match) -> str:
            prefix = m.group(1)
            existing_opts = m.group(3) or ""

            if "confluent.field_meta" in existing_opts:
                merged = self._merge_confluent_tag(existing_opts, tag)
                return f"{prefix}[{merged}];"

            new_opt = f'(confluent.field_meta) = {{tags: ["{tag}"]}}'
            if existing_opts.strip():
                return f"{prefix}[{existing_opts}, {new_opt}];"
            return f"{prefix}[{new_opt}];"

        return re.sub(pattern, replacer, text, count=1)

    def _merge_confluent_tag(self, opts: str, tag: str) -> str:
        """Merge a new tag into an existing confluent.field_meta option string."""
        # Array form: (confluent.field_meta) = {tags: ["A", "B"]}
        array_m = re.search(r'\(confluent\.field_meta\)\s*=\s*\{([^}]*)\}', opts)
        if array_m:
            inner = array_m.group(1)
            tags_m = re.search(r'tags\s*:\s*\[([^\]]*)\]', inner)
            if tags_m:
                existing = [t.strip().strip('"') for t in tags_m.group(1).split(',') if t.strip()]
                if tag in existing:
                    return opts
                existing.append(tag)
                new_tags = ", ".join(f'"{t}"' for t in existing)
                new_inner = re.sub(r'tags\s*:\s*\[[^\]]*\]', f'tags: [{new_tags}]', inner)
            else:
                new_inner = inner.rstrip() + f', tags: ["{tag}"]'
            return opts[: array_m.start(1)] + new_inner + opts[array_m.end(1) :]

        # Scalar form: (confluent.field_meta).tags = "A"
        scalar_m = re.search(r'\(confluent\.field_meta\)\.tags\s*=\s*"([^"]+)"', opts)
        if scalar_m:
            existing_tag = scalar_m.group(1)
            if existing_tag == tag:
                return opts
            new_opt = f'(confluent.field_meta) = {{tags: ["{existing_tag}", "{tag}"]}}'
            return opts[: scalar_m.start()] + new_opt + opts[scalar_m.end() :]

        return opts


# ── Dispatch ──────────────────────────────────────────────────────────────────

PATCHERS: dict[str, Any] = {
    "AVRO":     AvroPatcher(),
    "JSON":     JsonSchemaPatcher(),
    "PROTOBUF": ProtobufPatcher(),
}


# ── Existing-tag extraction ───────────────────────────────────────────────────
# These functions read the current schema and return a set of
# (field_path, tag) pairs that are already applied, so apply_tags.py
# can skip re-confirming classifications that haven't changed.

def _existing_tags_avro(schema_str: str) -> set[tuple[str, str]]:
    """Walk an Avro schema and collect all (field_path, tag) pairs already tagged."""
    schema = json.loads(schema_str)
    result: set[tuple[str, str]] = set()

    def walk(node: Any, prefix: str) -> None:
        record = AvroPatcher()._extract_record(node)
        if record is None:
            return
        for field in record.get("fields", []):
            path = f"{prefix}.{field['name']}" if prefix else field["name"]
            for tag in field.get("confluent:tags", []):
                result.add((path, tag))
            walk(field.get("type"), path)

    walk(schema, "")
    return result


def _existing_tags_json_schema(schema_str: str) -> set[tuple[str, str]]:
    """Walk a JSON Schema and collect all (field_path, tag) pairs already tagged."""
    schema = json.loads(schema_str)
    result: set[tuple[str, str]] = set()

    def walk(node: dict, prefix: str) -> None:
        for name, field in node.get("properties", {}).items():
            path = f"{prefix}.{name}" if prefix else name
            for tag in field.get("confluent:tags", []):
                result.add((path, tag))
            walk(field, path)
            items = field.get("items")
            if isinstance(items, dict):
                walk(items, path)

    walk(schema, "")
    return result


def _existing_tags_protobuf(schema_str: str) -> set[tuple[str, str]]:
    """
    Extract (field_path, tag) pairs from a Protobuf schema text.
    Handles both array form  = {tags: ["A"]}  and scalar form  .tags = "A".
    """
    result: set[tuple[str, str]] = set()
    # Field line: optional type field_name = number [(confluent.field_meta)...];
    field_re = re.compile(
        r'\b(?:(?:repeated|optional|required)\s+)?\w[\w.]*\s+(\w+)\s*=\s*\d+\s*(\[[^\]]*\])?\s*;'
    )
    for m in field_re.finditer(schema_str):
        field_name = m.group(1)
        opts = m.group(2) or ""
        # Array form
        for tag in re.findall(r'"(\w+)"', opts) if "confluent.field_meta" in opts else []:
            result.add((field_name, tag))
        # Scalar form
        scalar = re.search(r'\(confluent\.field_meta\)\.tags\s*=\s*"(\w+)"', opts)
        if scalar:
            result.add((field_name, scalar.group(1)))
    return result


def get_existing_tags(schema_str: str, schema_type: str) -> set[tuple[str, str]]:
    if schema_type == "AVRO":
        return _existing_tags_avro(schema_str)
    if schema_type == "JSON":
        return _existing_tags_json_schema(schema_str)
    if schema_type == "PROTOBUF":
        return _existing_tags_protobuf(schema_str)
    return set()

# ── Kafka results reader ──────────────────────────────────────────────────────

def read_latest_scan_batch(
    results_topic: str,
    bootstrap: str,
    kafka_key: str,
    kafka_secret: str,
    source_topic: str,
) -> list[dict]:
    """
    Reads all messages from the scan-results topic and returns only the rows
    from the most recent scanned_at window for this source_topic.

    The Flink job emits one batch per TUMBLE window. We take the latest batch
    so the user always reviews the freshest classifications.
    """
    if not _KAFKA_AVAILABLE:
        print("ERROR: confluent-kafka not installed. Run: pip install confluent-kafka")
        sys.exit(1)

    consumer = Consumer({
        "bootstrap.servers":            bootstrap,
        "security.protocol":            "SASL_SSL",
        "sasl.mechanisms":              "PLAIN",
        "sasl.username":                kafka_key,
        "sasl.password":                kafka_secret,
        "group.id":                     f"apply-tags-reader-{source_topic}",
        "auto.offset.reset":            "earliest",
        "enable.auto.commit":           False,
    })

    # Assign all partitions from the beginning so we read the full topic
    metadata = consumer.list_topics(results_topic, timeout=10)
    if results_topic not in metadata.topics:
        print(f"ERROR: results topic '{results_topic}' not found.")
        print("Has the Flink scan job run at least once? Check: ./scripts/start_scan.sh --status")
        sys.exit(1)

    partitions = [
        TopicPartition(results_topic, p, OFFSET_BEGINNING)
        for p in metadata.topics[results_topic].partitions
    ]
    consumer.assign(partitions)

    # Drain the topic — collect all messages for this source_topic
    all_rows: list[dict] = []
    empty_polls = 0
    while empty_polls < 3:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                empty_polls += 1
            continue
        empty_polls = 0
        try:
            row = json.loads(msg.value().decode())
            if row.get("source_topic") == source_topic:
                all_rows.append(row)
        except (json.JSONDecodeError, AttributeError):
            continue

    consumer.close()

    if not all_rows:
        return []

    # Find the most recent scanned_at timestamp and return only that batch
    latest_ts = max(r.get("scanned_at", "") for r in all_rows)
    latest_batch = [r for r in all_rows if r.get("scanned_at") == latest_ts]

    # Deduplicate: keep highest confidence per (field_path, tag)
    best: dict[tuple[str, str], dict] = {}
    for r in latest_batch:
        key = (r["field_path"], r["tag"])
        if key not in best or r.get("confidence", 0) > best[key].get("confidence", 0):
            best[key] = r

    print(f"  Latest scan batch: {latest_ts}  ({len(best)} unique field/tag pair(s))")
    return sorted(best.values(), key=lambda r: -r.get("confidence", 0))


# ── Interactive review ────────────────────────────────────────────────────────

_GREEN  = "\033[32m"
_YELLOW = "\033[33m"
_RED    = "\033[31m"
_RESET  = "\033[0m"


def _confidence_style(score: float) -> tuple[str, str]:
    if score >= 0.85:
        return "HIGH  ", _GREEN
    if score >= 0.60:
        return "MEDIUM", _YELLOW
    return "LOW   ", _RED


def prompt_approvals(classifications: list[dict], auto_yes: bool) -> list[dict]:
    approved: list[dict] = []
    total = len(classifications)
    print(f"\n{'─' * 62}")
    print(f"  Reviewing {total} classification(s)")
    print(f"{'─' * 62}")

    for i, c in enumerate(classifications, 1):
        score = float(c.get("confidence", 0))
        label, color = _confidence_style(score)
        print(f"\n[{i}/{total}]")
        print(f"  Field:      {c['field_path']}")
        print(f"  Tag:        {c['tag']}")
        print(f"  Confidence: {color}{label}{_RESET}  ({score:.2f}  via {c.get('source', '?')})")
        if c.get("example"):
            print(f"  Example:    {c['example']}")

        if auto_yes:
            print("  → approved (--yes)")
            approved.append(c)
            continue

        while True:
            ans = input("  Approve? [y/n/q] ").strip().lower()
            if ans in ("y", "yes"):
                approved.append(c)
                break
            elif ans in ("n", "no", ""):
                break
            elif ans in ("q", "quit"):
                print("\nAborted.")
                sys.exit(0)

    return approved


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apply approved data tags to a Confluent Schema Registry schema (AVRO / JSON Schema / Protobuf)"
    )
    parser.add_argument("--topic",        required=True,  help="Source Kafka topic name (e.g. payments)")
    parser.add_argument("--sr-url",       required=True,  help="Schema Registry base URL")
    parser.add_argument("--sr-key",       required=True,  help="Schema Registry API key")
    parser.add_argument("--sr-secret",    required=True,  help="Schema Registry API secret")
    parser.add_argument("--bootstrap",    required=True,  help="Kafka bootstrap server (e.g. pkc-xxx.confluent.cloud:9092)")
    parser.add_argument("--kafka-key",    required=True,  help="Kafka API key")
    parser.add_argument("--kafka-secret", required=True,  help="Kafka API secret")
    parser.add_argument("--subject",      help="Override subject name (default: {topic}-value)")
    parser.add_argument("--yes",          action="store_true", help="Auto-approve all without prompting")
    args = parser.parse_args()

    results_topic = f"{args.topic}-scan-results"
    print(f"Reading latest scan results from: {results_topic}")
    classifications: list[dict] = read_latest_scan_batch(
        results_topic=results_topic,
        bootstrap=args.bootstrap,
        kafka_key=args.kafka_key,
        kafka_secret=args.kafka_secret,
        source_topic=args.topic,
    )

    if not classifications:
        print("No classifications found in input file.")
        sys.exit(0)

    # ── Fetch current schema early so we can skip already-tagged fields ────────
    subject = args.subject or f"{args.topic}-value"
    client = SchemaRegistryClient(args.sr_url, args.sr_key, args.sr_secret)
    try:
        latest = client.get_latest(subject)
    except RuntimeError as e:
        print(f"ERROR fetching schema: {e}")
        sys.exit(1)

    schema_type: str = latest.get("schemaType") or "AVRO"
    schema_str: str  = latest["schema"]
    version: int     = latest["version"]
    existing = get_existing_tags(schema_str, schema_type)

    already_tagged = [c for c in classifications if (c["field_path"], c["tag"]) in existing]
    new_or_changed = [c for c in classifications if (c["field_path"], c["tag"]) not in existing]

    if already_tagged:
        print(f"\n  Skipping {len(already_tagged)} already-tagged classification(s) (no change):")
        for c in already_tagged:
            print(f"    {c['field_path']:<35} {c['tag']}  (already in schema)")

    if not new_or_changed:
        print("\nAll classifications already applied — schema is up to date.")
        sys.exit(0)

    approved = prompt_approvals(new_or_changed, args.yes)

    if not approved:
        print("\nNo tags approved — schema unchanged.")
        sys.exit(0)

    print(f"\n{'─' * 62}")
    print(f"  Schema type:     {schema_type}")
    print(f"  Current version: v{version}")
    print(f"\n  Applying {len(approved)} tag(s):")

    patcher = PATCHERS.get(schema_type)
    if patcher is None:
        print(f"ERROR: unsupported schema type '{schema_type}'")
        sys.exit(1)

    for a in approved:
        print(f"    {a['field_path']:<35} → {a['tag']}")

    try:
        patched = patcher.patch(schema_str, approved)
    except Exception as e:
        print(f"\nERROR during schema patching: {e}")
        sys.exit(1)

    try:
        result = client.register(subject, patched, schema_type)
        new_id = result.get("id", "?")
        print(f"\n  Registered new schema version (id={new_id}).")
        print("  Done.")
    except RuntimeError as e:
        print(f"\nERROR registering schema: {e}")
        print("\nPatched schema (not registered):")
        print(patched)
        sys.exit(1)


if __name__ == "__main__":
    main()
