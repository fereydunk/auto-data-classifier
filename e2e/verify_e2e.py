"""
End-to-end smoke test for the auto data classifier stack.

Checks:
  1. Classifier /health is reachable
  2. /classify correctly identifies PII, PCI, PHI, CREDENTIALS, GOVERNMENT_ID,
     FINANCIAL across all three layers
  3. (optional) Confluent Cloud connectivity — produces 1 message, reads it back

Usage:
    # Local classifier only
    python e2e/verify_e2e.py --classifier-url http://localhost:8000

    # Full end-to-end including Confluent
    python e2e/verify_e2e.py \
        --classifier-url https://xxxx.ngrok-free.app \
        --bootstrap      pkc-xxx.us-east-1.aws.confluent.cloud:9092 \
        --api-key        KAFKA_KEY \
        --api-secret     KAFKA_SECRET \
        --topic          scanner-test
"""

import argparse
import json
import os
import sys
import time

try:
    import urllib.request
    import urllib.error
except ImportError:
    pass

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

GREEN = "\033[92m"
RED   = "\033[91m"
RESET = "\033[0m"
BOLD  = "\033[1m"

passed = 0
failed = 0


def ok(msg):
    global passed
    passed += 1
    print(f"  {GREEN}✓{RESET} {msg}")


def fail(msg):
    global failed
    failed += 1
    print(f"  {RED}✗{RESET} {msg}")


def _post(url, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def _get(url):
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.loads(resp.read())


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------
CLASSIFY_CASES = [
    # (description, fields, max_layer, expected_tag)
    ("Layer 1 — field name 'ssn'",
     {"ssn": "ignored"},                   1, "GOVERNMENT_ID"),
    ("Layer 1 — field name 'email'",
     {"email": "alice@example.com"},       1, "PII"),
    ("Layer 1 — field name 'password'",
     {"password": "s3cr3t"},               1, "CREDENTIALS"),
    ("Layer 1 — field name 'credit_card_number'",
     {"credit_card_number": "ignored"},    1, "PCI"),
    ("Layer 1 — field name 'patient_id'",
     {"patient_id": "MRN-001"},            1, "PHI"),
    ("Layer 2 — routing number in value → FINANCIAL",
     {"ref": "routing number 026009593"},  2, "FINANCIAL"),
    ("Layer 2 — regex credit card in value",
     {"ref": "card 4111111111111111"},     2, "PCI"),
    ("Layer 2 — regex email in value",
     {"message": "contact alice@ex.com"},  2, "PII"),
    ("Layer 3 — AI model detects PHI in free-text",
     {"freetext": "Patient diagnosed with Type 2 Diabetes, prescribed Metformin 500mg"}, 3, "PHI"),
    ("max_layer=1 skips regex/AI",
     {"ref": "4111111111111111"},          1, None),   # 'ref' has no L1 match
    ("nested fields flattened",
     {"customer": {"email": "a@b.com"}},  1, "PII"),
]


def test_classifier(base_url: str):
    print(f"\n{BOLD}── Classifier service ({base_url}) ──{RESET}")

    # Health check
    try:
        h = _get(f"{base_url}/health")
        if h.get("status") == "ok" and h.get("analyzer_ready"):
            ok(f"Health OK — version {h.get('version')}")
        else:
            fail(f"Health returned unexpected body: {h}")
            return
    except Exception as e:
        fail(f"Health check failed: {e}")
        return

    # Classification cases
    print(f"\n{BOLD}── /classify{RESET}")
    for desc, fields, max_layer, expected_tag in CLASSIFY_CASES:
        try:
            resp = _post(f"{base_url}/classify", {"fields": fields, "max_layer": max_layer})
            tags = resp.get("tags", [])
            if expected_tag is None:
                if tags:
                    fail(f"{desc} → expected no tags, got {tags}")
                else:
                    ok(f"{desc} → (no tags, as expected)")
            elif expected_tag in tags:
                layers_used = resp.get("layers_used", [])
                ok(f"{desc} → {expected_tag}  (layers_used={layers_used})")
            else:
                fail(f"{desc} → expected {expected_tag}, got tags={tags}")
        except Exception as e:
            fail(f"{desc} → error: {e}")


def test_kafka(bootstrap: str, api_key: str, api_secret: str, topic: str):
    print(f"\n{BOLD}── Kafka connectivity (topic: {topic}) ──{RESET}")
    try:
        from confluent_kafka import Producer, Consumer, KafkaError
    except ImportError:
        print("  (skipped — confluent-kafka not installed)")
        return

    conf = {
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": api_key,
        "sasl.password": api_secret,
    }

    # Produce a canary message
    canary = {"test": "e2e_canary", "email": "test@example.com", "ssn": "999-99-9999"}
    try:
        p = Producer(conf)
        p.produce(topic, value=json.dumps(canary).encode())
        p.flush(timeout=10)
        ok(f"Produced canary message to '{topic}'")
    except Exception as e:
        fail(f"Produce failed: {e}")
        return

    # Consume it back (give it 10s to arrive)
    try:
        c_conf = {**conf, "group.id": "e2e-verify", "auto.offset.reset": "earliest"}
        c = Consumer(c_conf)
        c.subscribe([topic])
        deadline = time.time() + 10
        found = False
        while time.time() < deadline:
            msg = c.poll(1.0)
            if msg and not msg.error():
                body = json.loads(msg.value())
                if body.get("test") == "e2e_canary":
                    found = True
                    break
        c.close()
        if found:
            ok(f"Consumed canary message back from '{topic}'")
        else:
            fail(f"Canary message not found in '{topic}' within 10s")
    except Exception as e:
        fail(f"Consume failed: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--classifier-url", default=os.getenv("CLASSIFIER_URL", "http://localhost:8000"))
    parser.add_argument("--bootstrap",      default=os.getenv("CONFLUENT_BOOTSTRAP_SERVERS"))
    parser.add_argument("--api-key",        default=os.getenv("CONFLUENT_API_KEY"))
    parser.add_argument("--api-secret",     default=os.getenv("CONFLUENT_API_SECRET"))
    parser.add_argument("--topic",          default="scanner-test")
    args = parser.parse_args()

    test_classifier(args.classifier_url.rstrip("/"))

    if args.bootstrap and args.api_key and args.api_secret:
        test_kafka(args.bootstrap, args.api_key, args.api_secret, args.topic)
    else:
        print(f"\n  (Kafka test skipped — pass --bootstrap/--api-key/--api-secret to enable)")

    print(f"\n{BOLD}── Result: {GREEN}{passed} passed{RESET}{BOLD}, {RED}{failed} failed{RESET}")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
