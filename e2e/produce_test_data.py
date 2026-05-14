"""Produce realistic test messages to a Confluent Cloud topic.

The Avro schema is fetched FROM SCHEMA REGISTRY at runtime by --schema-id;
no schema literal is hardcoded here. Sample data per field comes from
setup_wizard.field_pool when the field name matches a pool entry, or from
a generic string fallback otherwise.

Usage:
    python e2e/produce_test_data.py \\
        --bootstrap   pkc-xxx.us-east-1.aws.confluent.cloud:9092 \\
        --api-key     KAFKA_API_KEY \\
        --api-secret  KAFKA_API_SECRET \\
        --sr-url      https://psrc-xxx.us-east-2.aws.confluent.cloud \\
        --sr-key      SR_API_KEY \\
        --sr-secret   SR_API_SECRET \\
        --topic       customer-profiles-demo \\
        --schema-id   100123 \\
        --count       200
"""
from __future__ import annotations

import argparse
import base64
import io as _io
import json
import os
import random
import string
import struct
import sys
import urllib.error
import urllib.request
from pathlib import Path

# Make setup_wizard.field_pool importable so we can reuse the generators.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from confluent_kafka import Producer
except ImportError:
    print("ERROR: confluent-kafka not installed. Run: pip install confluent-kafka")
    sys.exit(1)

try:
    import fastavro
except ImportError:
    print("ERROR: fastavro not installed. Run: pip install fastavro")
    sys.exit(1)

# Reuse the generator vocabulary from the wizard's field pool.
from setup_wizard.field_pool import FIELD_POOL  # noqa: E402

_GENERATOR_BY_NAME = {f.name: f.sample for f in FIELD_POOL}


def _fetch_schema_by_id(sr_url: str, key: str, secret: str, schema_id: int) -> dict:
    """GET /schemas/ids/{id} → return parsed schema dict."""
    base = sr_url.rstrip("/")
    auth = base64.b64encode(f"{key}:{secret}".encode()).decode()
    req = urllib.request.Request(
        f"{base}/schemas/ids/{schema_id}",
        headers={"Authorization": f"Basic {auth}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            body = json.loads(r.read())
    except urllib.error.HTTPError as e:
        print(f"ERROR: SR fetch for id={schema_id} failed: HTTP {e.code}: {e.read().decode()[:200]}",
              file=sys.stderr)
        sys.exit(2)
    schema_str = body.get("schema")
    if not schema_str:
        print(f"ERROR: SR returned id={schema_id} but no 'schema' field", file=sys.stderr)
        sys.exit(2)
    return json.loads(schema_str)


def _generic_value(name: str, avro_type, rng: random.Random):
    """Fallback when a field has no matching generator in FIELD_POOL.

    Picks a sane default based on the field's Avro inner type.
    """
    inner = avro_type[1] if isinstance(avro_type, list) else avro_type
    if inner == "string":
        suffix = "".join(rng.choices(string.ascii_lowercase, k=6))
        return f"sample-{suffix}"
    if inner in ("int", "long"):
        return rng.randint(0, 1_000_000)
    if inner == "double":
        return round(rng.random() * 1000, 2)
    if inner == "boolean":
        return rng.choice([True, False])
    return None  # nullable handles it


def _make_record(schema: dict, rng: random.Random) -> dict:
    """Produce one record: ~50% of fields populated, the rest null."""
    rec: dict = {}
    for f in schema.get("fields", []):
        name = f["name"]
        if rng.random() < 0.5:
            gen = _GENERATOR_BY_NAME.get(name)
            rec[name] = gen(rng) if gen else _generic_value(name, f["type"], rng)
        else:
            rec[name] = None
    return rec


def _avro_encode(parsed_schema, schema_id: int, record: dict) -> bytes:
    buf = _io.BytesIO()
    buf.write(b"\x00")
    buf.write(struct.pack(">I", schema_id))
    fastavro.schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()


def run(args):
    schema_dict = _fetch_schema_by_id(args.sr_url, args.sr_key, args.sr_secret, args.schema_id)
    parsed = fastavro.parse_schema(schema_dict)
    field_count = len(schema_dict.get("fields", []))
    print(f"Fetched schema id={args.schema_id} from SR ({field_count} fields)")

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms":   "PLAIN",
        "sasl.username":     args.api_key,
        "sasl.password":     args.api_secret,
    })
    delivered = errors = 0

    def on_delivery(err, _msg):
        nonlocal delivered, errors
        if err:
            errors += 1
        else:
            delivered += 1

    rng = random.Random()
    print(f"Producing {args.count} messages to '{args.topic}' (schema_id={args.schema_id})…")
    for i in range(args.count):
        producer.produce(
            topic=args.topic,
            value=_avro_encode(parsed, args.schema_id, _make_record(schema_dict, rng)),
            on_delivery=on_delivery,
        )
        if (i + 1) % 50 == 0:
            producer.flush()
            print(f"  {i + 1}/{args.count} sent")

    producer.flush()
    print(f"\nDone. Delivered: {delivered}  Errors: {errors}")


def main():
    p = argparse.ArgumentParser(description="Produce test data — schema fetched from SR by id.")
    p.add_argument("--bootstrap",  default=os.getenv("CONFLUENT_BOOTSTRAP_SERVERS"))
    p.add_argument("--api-key",    default=os.getenv("CONFLUENT_API_KEY"))
    p.add_argument("--api-secret", default=os.getenv("CONFLUENT_API_SECRET"))
    p.add_argument("--sr-url",     default=os.getenv("CONFLUENT_SR_URL"))
    p.add_argument("--sr-key",     default=os.getenv("CONFLUENT_SR_API_KEY"))
    p.add_argument("--sr-secret",  default=os.getenv("CONFLUENT_SR_API_SECRET"))
    p.add_argument("--topic",      required=True, help="Destination Kafka topic")
    p.add_argument("--schema-id",  type=int, required=True,
                   help="Confluent SR schema ID — schema body is fetched from SR by this id")
    p.add_argument("--count",      type=int, default=200)
    args = p.parse_args()

    missing = [k for k, v in {
        "--bootstrap":  args.bootstrap,
        "--api-key":    args.api_key,
        "--api-secret": args.api_secret,
        "--sr-url":     args.sr_url,
        "--sr-key":     args.sr_key,
        "--sr-secret":  args.sr_secret,
    }.items() if not v]
    if missing:
        p.error(f"Missing: {', '.join(missing)}. Set via flags or env.")

    run(args)


if __name__ == "__main__":
    main()
