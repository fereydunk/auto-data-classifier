"""
Local end-to-end scanner — replicates the Flink classify_fields UDF pipeline.

Reads from a Kafka topic, calls the classifier service, and writes results to
the scan-results topic. Functionally identical to Flink Statement C, but runs
locally without requiring Confluent Cloud Flink.

WHY THIS EXISTS — FALLBACK PATH
────────────────────────────────
The primary classification path is Flink Statement C (scan driver), which uses
classify_fields() with USING CONNECTIONS to call POST /classify on the external
classifier URL. USING CONNECTIONS requires the feature to be enabled for your
Confluent Cloud organisation by the Confluent account team.

This script is a fallback for orgs where USING CONNECTIONS is not yet enabled.
It runs the same classify → publish logic locally, writing to the same
{topic}-scan-results topic that apply_tags.py consumes. The rest of the pipeline
(apply_tags.py → Schema Registry) is identical regardless of which path produced
the results.

apply_tags.py handles both result formats transparently:
  • Flink Statement C writes Confluent Avro wire format (0x00 + schema ID + bytes)
  • This script writes plain JSON

WHEN TO USE THIS SCRIPT
────────────────────────
Use local_scanner.py only if:
  1. USING CONNECTIONS is not enabled for your org, OR
  2. The classifier-service Flink connection is not configured

Otherwise, use the native Flink path:
  bash flink-scanner/scripts/start_scan.sh --now   (twice, ~60s apart)

See docs/ARCHITECTURE.md — "Flink USING CONNECTIONS — internet egress for UDFs"

USAGE
─────
    source flink-scanner/scan.env
    python e2e/local_scanner.py \\
        --bootstrap  $KAFKA_BOOTSTRAP \\
        --api-key    $KAFKA_KEY \\
        --api-secret $KAFKA_SECRET \\
        --classifier-url http://localhost:8000 \\
        --count 200

All connection params default to the values in flink-scanner/scan.env
(loaded automatically if python-dotenv is installed).

KEY INVARIANT — batch timestamp
────────────────────────────────
All results written in one run share a single scanned_at timestamp (set once
before the processing loop). apply_tags.py groups by max(scanned_at) to find
the "latest batch" — if timestamps differ per message, only the last second's
results are processed and most detections are silently lost.
"""

import argparse
import base64
import io as _io
import json
import os
import struct
import sys
import time
import urllib.request
import urllib.error

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from confluent_kafka import Consumer, Producer, KafkaError
except ImportError:
    print("ERROR: confluent-kafka not installed. Run: pip install confluent-kafka")
    sys.exit(1)

try:
    import fastavro
except ImportError:
    print("ERROR: fastavro not installed. Run: pip install fastavro")
    sys.exit(1)


# ── Schema fetch from SR ─────────────────────────────────────────────────────
# We never embed a schema literal here: the wire-format header gives us the
# schema_id, and we fetch the parsed schema from SR (cached per-id).
_SCHEMA_CACHE: dict[int, "fastavro.types.Schema"] = {}


def _fetch_parsed_schema(sr_url: str, sr_key: str, sr_secret: str, schema_id: int):
    """GET /schemas/ids/{id} from SR, parse with fastavro, cache."""
    cached = _SCHEMA_CACHE.get(schema_id)
    if cached is not None:
        return cached
    base = sr_url.rstrip("/")
    auth = base64.b64encode(f"{sr_key}:{sr_secret}".encode()).decode()
    req = urllib.request.Request(
        f"{base}/schemas/ids/{schema_id}",
        headers={"Authorization": f"Basic {auth}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            body = json.loads(r.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(
            f"SR fetch for schema id={schema_id} failed: HTTP {e.code}: {e.read().decode()[:200]}"
        ) from e
    schema_str = body.get("schema")
    if not schema_str:
        raise RuntimeError(f"SR returned id={schema_id} but no 'schema' field")
    parsed = fastavro.parse_schema(json.loads(schema_str))
    _SCHEMA_CACHE[schema_id] = parsed
    return parsed


def _avro_decode(data: bytes, sr_url: str, sr_key: str, sr_secret: str) -> dict:
    """Decode a Confluent Avro wire-format message: 0x00 | schema_id (BE32) | bytes.

    Schema is fetched from SR by id (cached) — there is no local schema copy.
    """
    if len(data) < 5 or data[0] != 0:
        raise ValueError("Not a Confluent Avro wire-format message")
    schema_id = struct.unpack(">I", data[1:5])[0]
    parsed = _fetch_parsed_schema(sr_url, sr_key, sr_secret, schema_id)
    buf = _io.BytesIO(data[5:])
    return fastavro.schemaless_reader(buf, parsed)


def _avro_encode_result(result: dict, schema_id: int) -> bytes:
    """Encode a scan-result record as JSON bytes (scan-results has no schema in SR)."""
    return json.dumps(result).encode()


def classify_fields(classifier_url: str, max_layer: int, fields: dict) -> list:
    """Call the classifier and return a list of result dicts."""
    # Remove null values — classifier skips them, no need to send
    payload = {k: v for k, v in fields.items() if v is not None}
    if not payload:
        return []

    body = json.dumps({"fields": payload, "max_layer": max_layer}).encode()
    url = classifier_url.rstrip("/") + "/classify"

    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            response = json.loads(resp.read())
    except Exception as e:
        print(f"  [warn] classify HTTP error: {e}", file=sys.stderr)
        return []

    detected = response.get("detected_entities", {})
    results = []
    for field_path, entities in detected.items():
        if not isinstance(entities, list):
            continue
        # Keep best (highest score) per tag
        best_per_tag: dict = {}
        for entity in entities:
            tag   = entity.get("tag", "PII")
            score = entity.get("score", 0.0)
            if tag not in best_per_tag or score > best_per_tag[tag].get("score", 0.0):
                best_per_tag[tag] = entity

        for tag, best in best_per_tag.items():
            snippet = best.get("text_snippet", "")[:50]
            results.append({
                "field_path": field_path,
                "tag":        tag,
                "confidence": round(best.get("score", 0.0), 2),
                "layer":      best.get("layer", 3),
                "source":     best.get("source", "ai_model"),
                "example":    snippet,
            })
    return results


def run(bootstrap, api_key, api_secret, sr_url, sr_key, sr_secret,
        source_topic, classifier_url, max_layer, count):
    kafka_conf = {
        "bootstrap.servers":  bootstrap,
        "security.protocol":  "SASL_SSL",
        "sasl.mechanisms":    "PLAIN",
        "sasl.username":      api_key,
        "sasl.password":      api_secret,
        "group.id":           f"local-scanner-e2e-{int(time.time())}",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    }
    producer_conf = {k: v for k, v in kafka_conf.items()
                     if k not in ("group.id", "auto.offset.reset", "enable.auto.commit")}

    consumer = Consumer(kafka_conf)
    producer = Producer(producer_conf)

    results_topic = f"{source_topic}-scan-results"
    consumer.subscribe([source_topic])

    print(f"Reading up to {count} messages from '{source_topic}'…")
    print(f"Classifier: {classifier_url}  (layer {max_layer})")
    print(f"Writing results to: {results_topic}")
    print()

    processed   = 0
    total_tags  = 0
    tag_summary: dict = {}  # tag → count
    # One batch timestamp for the entire run so apply_tags.py sees all results together
    scanned_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    try:
        while processed < count:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                print(f"  [timeout] no message after 5s (processed {processed}/{count})")
                if processed == 0:
                    continue
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"  [eof] partition {msg.partition()} at offset {msg.offset()}")
                    break
                print(f"  [error] {msg.error()}", file=sys.stderr)
                continue

            processed += 1
            raw = msg.value()

            try:
                record = _avro_decode(raw, sr_url, sr_key, sr_secret)
            except Exception as e:
                print(f"  [skip] msg {processed}: decode error: {e}", file=sys.stderr)
                continue

            # Classify — fields list comes from the decoded record itself
            # (which mirrors the SR schema's fields), never a local copy.
            fields = {k: v for k, v in record.items() if v is not None}
            results = classify_fields(classifier_url, max_layer, fields)

            if results:
                for r in results:
                    result_row = {
                        "field_path":   r["field_path"],
                        "tag":          r["tag"],
                        "confidence":   r["confidence"],
                        "layer":        r["layer"],
                        "source":       r["source"],
                        "example":      r["example"],
                        "source_topic": source_topic,
                        "trigger_type": "manual",
                        "scanned_at":   scanned_at,
                    }
                    producer.produce(
                        topic=results_topic,
                        value=json.dumps(result_row).encode(),
                    )
                    tag_summary[r["tag"]] = tag_summary.get(r["tag"], 0) + 1
                    total_tags += 1
                producer.poll(0)

            if processed % 25 == 0 or processed == count:
                print(f"  {processed}/{count} messages processed  ({total_tags} tag detections so far)")

    finally:
        consumer.close()
        producer.flush()

    print()
    print(f"Done. Processed {processed} messages → {total_tags} tag detections.")
    print()
    print("Tag summary:")
    for tag, cnt in sorted(tag_summary.items(), key=lambda x: -x[1]):
        print(f"  {tag:<20s} {cnt:>4d}")
    print()
    print(f"Results written to Kafka topic: {results_topic}")
    print("Next step: python flink-scanner/apply_tags.py --topic raw-messages ...")


def main():
    parser = argparse.ArgumentParser(description="Local end-to-end scanner (no Flink required)")
    parser.add_argument("--bootstrap",       default=os.getenv("KAFKA_BOOTSTRAP") or os.getenv("CONFLUENT_BOOTSTRAP_SERVERS"))
    parser.add_argument("--api-key",         default=os.getenv("KAFKA_KEY") or os.getenv("CONFLUENT_API_KEY"))
    parser.add_argument("--api-secret",      default=os.getenv("KAFKA_SECRET") or os.getenv("CONFLUENT_API_SECRET"))
    parser.add_argument("--sr-url",          default=os.getenv("SR_URL") or os.getenv("CONFLUENT_SR_URL"),
                        help="SR base URL — required for decoding messages by their wire-format schema id")
    parser.add_argument("--sr-key",          default=os.getenv("SR_KEY") or os.getenv("CONFLUENT_SR_API_KEY"))
    parser.add_argument("--sr-secret",       default=os.getenv("SR_SECRET") or os.getenv("CONFLUENT_SR_API_SECRET"))
    parser.add_argument("--topic",           default=os.getenv("SOURCE_TOPIC", "raw-messages"))
    parser.add_argument("--classifier-url",  default=os.getenv("CLASSIFIER_URL", "http://localhost:8000"))
    parser.add_argument("--max-layer",       type=int, default=int(os.getenv("CLASSIFIER_MAX_LAYER", "3")))
    parser.add_argument("--count",           type=int, default=200,
                        help="Max messages to read (default: 200)")
    args = parser.parse_args()

    missing = [k for k, v in {
        "--bootstrap":  args.bootstrap,
        "--api-key":    args.api_key,
        "--api-secret": args.api_secret,
        "--sr-url":     args.sr_url,
        "--sr-key":     args.sr_key,
        "--sr-secret":  args.sr_secret,
    }.items() if not v]
    if missing:
        parser.error(f"Missing: {', '.join(missing)}")

    run(
        bootstrap      = args.bootstrap,
        api_key        = args.api_key,
        api_secret     = args.api_secret,
        sr_url         = args.sr_url,
        sr_key         = args.sr_key,
        sr_secret      = args.sr_secret,
        source_topic   = args.topic,
        classifier_url = args.classifier_url,
        max_layer      = args.max_layer,
        count          = args.count,
    )


if __name__ == "__main__":
    main()
