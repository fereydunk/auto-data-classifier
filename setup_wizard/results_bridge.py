"""Consume {topic}-scan-results from Kafka, POST each row to review-api.

Long-running subprocess started by the wizard's "Run test demo" button.
Reads Kafka + Schema Registry creds from `.env` (already populated by Card 3).

The Flink scan emits one row per (field, tag) detection into
`{topic}-scan-results`. This bridge reads them and POSTs each as a
Recommendation to review-api. Review-api's create endpoint already does
upsert (keeps highest confidence per field+tag), so re-processing on
restart is safe.

CLI:
    python -m setup_wizard.results_bridge --topic <SOURCE_TOPIC>
"""

from __future__ import annotations

import argparse
import base64
import io as _io
import json
import logging
import os
import struct
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE  = REPO_ROOT / ".env"

# Cache: schema_id → fastavro parsed schema. Wire-format Avro messages carry
# their schema_id in bytes 1..4; we fetch from SR once per id and reuse.
_SCHEMA_CACHE: dict[int, object] = {}

# Cache: subject → (schema_id, expiry_unix_ts). 60-s TTL so a mid-run schema
# bump (e.g. user re-runs Card 5 with a tweaked schema) is picked up within
# ~1 minute. Keyed by subject because the bridge can in principle handle
# multiple topics; in practice it's pinned to one --topic per process.
_SOURCE_SCHEMA_ID_CACHE: dict[str, tuple[int, float]] = {}
_SOURCE_SCHEMA_ID_TTL_S = 60

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
log = logging.getLogger("results-bridge")


def _load_dotenv(path: Path) -> dict[str, str]:
    """Tiny .env parser — KEY=VALUE per line. Strips quotes + skips comments."""
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
            val = val[1:-1]
        out[key.strip()] = val
    return out


def _post_recommendation(review_url: str, rec: dict) -> bool:
    """POST one recommendation to review-api. Logs + returns False on error."""
    import urllib.error
    import urllib.request
    req = urllib.request.Request(
        f"{review_url.rstrip('/')}/recommendations",
        data=json.dumps(rec).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return 200 <= resp.status < 300
    except urllib.error.HTTPError as exc:
        log.warning("review-api %d for %s/%s: %s",
                    exc.code, rec["field_path"], rec["proposed_tag"], exc.read()[:200])
        return False
    except Exception as exc:    # noqa: BLE001 — connection refused / DNS / timeout
        log.warning("review-api unreachable: %s", exc)
        return False


def _fetch_source_schema_id(subject: str, sr_url: str, sr_key: str, sr_secret: str) -> int | None:
    """Return the LATEST schema_id for `subject` from SR, with 60-s TTL cache.

    Used to stamp every recommendation with the schema_id of the source
    topic's currently-registered schema, so review-api's _resolve_version /
    validate-staged can pin the rec to a specific schema version instead
    of always falling back to "latest". Returns None on any SR error —
    caller falls back to omitting schema_id (preserves prior behavior).

    The TTL means a Card 5 re-run that registers a new schema_id is
    picked up within ~60s without restarting the bridge process.
    """
    cached = _SOURCE_SCHEMA_ID_CACHE.get(subject)
    now = time.time()
    if cached and cached[1] > now:
        return cached[0]
    base = sr_url.rstrip("/")
    auth = base64.b64encode(f"{sr_key}:{sr_secret}".encode()).decode()
    try:
        req = urllib.request.Request(
            f"{base}/subjects/{subject}/versions/latest",
            headers={"Authorization": f"Basic {auth}"},
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            sid = json.loads(r.read()).get("id")
        if isinstance(sid, int):
            _SOURCE_SCHEMA_ID_CACHE[subject] = (sid, now + _SOURCE_SCHEMA_ID_TTL_S)
            return sid
    except urllib.error.HTTPError as exc:
        log.warning("SR latest fetch for %s failed: HTTP %d", subject, exc.code)
    except Exception as exc:    # noqa: BLE001
        log.warning("SR latest fetch for %s failed: %s", subject, exc)
    return None


def _decode_wire_avro(raw: bytes, sr_url: str, sr_key: str, sr_secret: str) -> dict | None:
    """Decode a Confluent Avro wire-format message: 0x00 | schema_id (BE32) | bytes.

    Schema is fetched from SR by id (cached). Returns None if the message
    isn't wire-format (e.g. Flink emitted JSON for some reason).
    """
    try:
        import fastavro
    except ImportError:
        log.error("fastavro not installed. Run: pip install fastavro")
        sys.exit(1)
    if len(raw) < 5 or raw[0] != 0x00:
        return None  # not wire-format — caller can fall back to JSON
    schema_id = struct.unpack(">I", raw[1:5])[0]
    parsed = _SCHEMA_CACHE.get(schema_id)
    if parsed is None:
        base = sr_url.rstrip("/")
        auth = base64.b64encode(f"{sr_key}:{sr_secret}".encode()).decode()
        req = urllib.request.Request(
            f"{base}/schemas/ids/{schema_id}",
            headers={"Authorization": f"Basic {auth}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                body = json.loads(r.read())
        except urllib.error.HTTPError as exc:
            log.warning("SR fetch for id=%d failed: HTTP %d", schema_id, exc.code)
            return None
        schema_str = body.get("schema")
        if not schema_str:
            log.warning("SR returned id=%d but no 'schema' field", schema_id)
            return None
        parsed = fastavro.parse_schema(json.loads(schema_str))
        _SCHEMA_CACHE[schema_id] = parsed
    return fastavro.schemaless_reader(_io.BytesIO(raw[5:]), parsed)


def run_bridge(*, topic: str, bootstrap: str, kafka_key: str, kafka_secret: str,
               sr_url: str, sr_key: str, sr_secret: str,
               review_url: str, group_suffix: str = "") -> None:
    """Drive the consume → POST loop. Blocks forever; killed by SIGTERM."""
    try:
        from confluent_kafka import Consumer, KafkaError
    except ImportError:
        log.error("confluent-kafka not installed. Run: pip install confluent-kafka")
        sys.exit(1)

    results_topic = f"{topic}-scan-results"
    subject       = f"{topic}-value"
    log.info("Bridging %s → %s/recommendations", results_topic, review_url)

    cfg = {
        "bootstrap.servers":  bootstrap,
        "security.protocol":  "SASL_SSL",
        "sasl.mechanisms":    "PLAIN",
        "sasl.username":      kafka_key,
        "sasl.password":      kafka_secret,
        "group.id":           f"results-bridge-{topic}{group_suffix}",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(cfg)

    # Wait until the results topic exists — the Flink scan creates it lazily
    # on first write, which can take a minute after start_scan.sh runs.
    log.info("Waiting for topic %s to appear…", results_topic)
    while True:
        try:
            md = consumer.list_topics(results_topic, timeout=10)
            if results_topic in md.topics and not md.topics[results_topic].error:
                break
        except Exception as exc:    # noqa: BLE001
            log.debug("topic poll: %s", exc)
        time.sleep(5)

    log.info("Topic ready, subscribing…")
    consumer.subscribe([results_topic])

    posted = 0
    while True:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            log.warning("consumer error: %s", msg.error())
            continue

        raw = msg.value()
        # Flink writes scan-results in Confluent Avro wire format by default
        # (auto-registered schema in SR). Fall back to JSON if the producer
        # wrote raw JSON (e.g. e2e/local_scanner.py fallback path).
        row = _decode_wire_avro(raw, sr_url, sr_key, sr_secret)
        if row is None:
            try:
                row = json.loads(raw.decode())
            except (json.JSONDecodeError, AttributeError, UnicodeDecodeError):
                log.warning("skipping un-decodable scan-result message (%d bytes)", len(raw or b""))
                continue

        # Filter to this run's source topic — scan-results may carry rows
        # from prior runs against other topics.
        if row.get("source_topic") != topic:
            continue

        # Stamp every rec with the source topic's CURRENT SR schema_id so
        # review-api's _resolve_version / validate-staged can pin the rec
        # to a specific schema version (instead of always assuming "latest").
        # 60-s TTL inside _fetch_source_schema_id keeps SR load bounded.
        rec = {
            "topic":         row.get("source_topic", topic),
            "subject":       subject,
            "schema_id":     _fetch_source_schema_id(subject, sr_url, sr_key, sr_secret),
            "field_path":    row["field_path"],
            "proposed_tag":  row["tag"],
            # Flink scan-results doesn't carry entity_type separately —
            # use the tag as a sensible default for catalog attributes.
            "entity_type":   row["tag"],
            "confidence":    float(row["confidence"]),
            "layer":         int(row.get("layer", 3)),
            "source":        row.get("source", "ai_model"),
            "is_free_text":  False,
        }
        if _post_recommendation(review_url, rec):
            posted += 1
            if posted % 5 == 0:
                log.info("posted %d recommendations", posted)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True,
                        help="Source topic (the one Flink scans). "
                             "Bridge reads <topic>-scan-results.")
    parser.add_argument("--review-url", default=None,
                        help="Review-api base URL (default: REVIEW_API_URL from .env or http://localhost:8001)")
    args = parser.parse_args()

    env = {**_load_dotenv(ENV_FILE), **os.environ}
    bootstrap    = env.get("CONFLUENT_BOOTSTRAP_SERVERS", "")
    kafka_key    = env.get("CONFLUENT_API_KEY", "")
    kafka_secret = env.get("CONFLUENT_API_SECRET", "")
    sr_url       = env.get("CONFLUENT_SR_URL", "")
    sr_key       = env.get("CONFLUENT_SR_API_KEY", "")
    sr_secret    = env.get("CONFLUENT_SR_API_SECRET", "")
    review_url   = args.review_url or env.get("REVIEW_API_URL", "http://localhost:8001")

    missing = [k for k, v in {
        "CONFLUENT_BOOTSTRAP_SERVERS": bootstrap,
        "CONFLUENT_API_KEY":           kafka_key,
        "CONFLUENT_API_SECRET":        kafka_secret,
        "CONFLUENT_SR_URL":            sr_url,
        "CONFLUENT_SR_API_KEY":        sr_key,
        "CONFLUENT_SR_API_SECRET":     sr_secret,
    }.items() if not v]
    if missing:
        sys.exit(f"ERROR: missing in .env: {', '.join(missing)}")

    run_bridge(
        topic=args.topic,
        bootstrap=bootstrap,
        kafka_key=kafka_key,
        kafka_secret=kafka_secret,
        sr_url=sr_url,
        sr_key=sr_key,
        sr_secret=sr_secret,
        review_url=review_url,
    )


if __name__ == "__main__":
    main()
