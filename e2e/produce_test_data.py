"""
Produce realistic test messages to a Confluent Cloud topic.

Covers all 11 data tags so the Flink scanner has interesting data to classify:
  PII, PHI, PCI, CREDENTIALS, FINANCIAL, GOVERNMENT_ID,
  BIOMETRIC, GENETIC, NPI, LOCATION, MINOR

Usage:
    python e2e/produce_test_data.py \
        --bootstrap  pkc-xxx.us-east-1.aws.confluent.cloud:9092 \
        --api-key    KAFKA_API_KEY \
        --api-secret KAFKA_API_SECRET \
        --topic      scanner-test \
        --count      200

Or set CONFLUENT_BOOTSTRAP_SERVERS / CONFLUENT_API_KEY / CONFLUENT_API_SECRET
in a .env file and run without flags.
"""

import argparse
import json
import os
import random
import sys
import time

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


# ---------------------------------------------------------------------------
# Sample data pools
# ---------------------------------------------------------------------------
FIRST_NAMES   = ["Alice", "Bob", "Carlos", "Diana", "Eve", "Frank", "Grace", "Henry"]
LAST_NAMES    = ["Smith", "Jones", "Patel", "Kim", "Mueller", "Tanaka", "Silva"]
DOMAINS       = ["example.com", "acme.org", "test.net", "corp.io"]
COMMENTS      = [
    "Patient presented with chest pain and shortness of breath.",
    "Customer called to dispute a charge on their credit card.",
    "She reported feeling dizzy after taking her medication.",
    "The account holder confirmed their date of birth over the phone.",
    "User's IP address was flagged for suspicious login attempts.",
    "Child account linked to parent guardian John Smith.",
    "Genome sequencing results uploaded for research study.",
    "Merger discussions with target company are confidential.",
]

def _email(fn, ln):
    return f"{fn.lower()}.{ln.lower()}@{random.choice(DOMAINS)}"

def _phone():
    return f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

def _ssn():
    return f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"

def _cc():
    # Luhn-valid Visa test number prefix
    return f"4111-1111-1111-{random.randint(1000,9999)}"

def _iban():
    return f"GB{random.randint(10,99)}BARC{random.randint(10000000,99999999):08d}{random.randint(10000000,99999999):08d}"

def _mrn():
    return f"MRN-{random.randint(100000,999999)}"

def _routing():
    return f"02100002{random.randint(1,9)}"

def _bank_account():
    return f"{random.randint(10000000,99999999)}"

def _dna():
    return "".join(random.choices("ATCG", k=60))

def _ip():
    return f"192.168.{random.randint(0,255)}.{random.randint(1,254)}"

def _dob():
    return f"{random.randint(1950,2005)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"

def _password():
    import string
    chars = string.ascii_letters + string.digits + "!@#$"
    return "".join(random.choices(chars, k=16))

def _api_key():
    import string
    return "ak_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=32))


# ---------------------------------------------------------------------------
# Message templates — each exercises different field types/tags
# ---------------------------------------------------------------------------
def _make_message():
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)

    templates = [
        # PII-heavy
        lambda: {
            "customer_id": f"CUST-{random.randint(10000,99999)}",
            "first_name": fn,
            "last_name": ln,
            "email": _email(fn, ln),
            "phone_number": _phone(),
            "date_of_birth": _dob(),
            "ip_address": _ip(),
            "status": random.choice(["active", "inactive", "pending"]),
        },
        # PCI + FINANCIAL
        lambda: {
            "transaction_id": f"TXN-{random.randint(100000,999999)}",
            "credit_card_number": _cc(),
            "iban": _iban(),
            "routing_number": _routing(),
            "account_number": _bank_account(),
            "amount": round(random.uniform(1.0, 9999.99), 2),
            "currency": random.choice(["USD", "EUR", "GBP"]),
        },
        # PHI
        lambda: {
            "patient_id": _mrn(),
            "mrn": _mrn(),
            "diagnosis": random.choice(["ICD10:J45", "ICD10:E11", "ICD10:I10", "ICD10:F32"]),
            "medication": random.choice(["Metformin 500mg", "Lisinopril 10mg", "Atorvastatin 20mg"]),
            "npi": f"1{random.randint(000000000,999999999):09d}",
            "insurance_id": f"INS-{random.randint(100000,999999)}",
            "comment": random.choice(COMMENTS),
        },
        # GOVERNMENT_ID
        lambda: {
            "applicant_name": f"{fn} {ln}",
            "ssn": _ssn(),
            "passport_number": f"P{random.randint(10000000,99999999)}",
            "driver_license": f"DL{random.randint(1000000,9999999)}",
            "nationality": random.choice(["US", "UK", "DE", "FR"]),
        },
        # CREDENTIALS
        lambda: {
            "service": random.choice(["database", "cache", "api", "queue"]),
            "username": f"{fn.lower()}{random.randint(1,99)}",
            "password": _password(),
            "api_key": _api_key(),
            "connection_string": f"postgresql://user:{_password()}@db.internal:5432/prod",
        },
        # GENETIC + BIOMETRIC
        lambda: {
            "sample_id": f"BIO-{random.randint(10000,99999)}",
            "dna_sequence": _dna(),
            "genome": f"GRCh38:{random.randint(1,22)}:{random.randint(1000000,9000000)}",
            "fingerprint": f"FP:{random.randint(1000000,9999999):08x}",
            "facial_recognition": f"FR:{random.randint(1000000,9999999):08x}",
        },
        # MINOR
        lambda: {
            "child_id": f"MINOR-{random.randint(1000,9999)}",
            "guardian_email": _email(fn, ln),
            "minor_data": "age_verified=false",
            "age": random.randint(5, 12),
        },
        # NPI (Non-Public Information) + free-text note
        lambda: {
            "deal_id": f"DEAL-{random.randint(1000,9999)}",
            "mnpi": "pre-announcement earnings data",
            "notes": "Merger discussions with target company are strictly confidential. "
                     "Do not share outside the deal team.",
            "amount": round(random.uniform(1e6, 1e9), 2),
        },
        # Mixed / realistic e-commerce order
        lambda: {
            "order_id": f"ORD-{random.randint(100000,999999)}",
            "customer": {
                "first_name": fn,
                "last_name": ln,
                "email_address": _email(fn, ln),
                "phone": _phone(),
            },
            "payment": {
                "credit_card_number": _cc(),
                "billing_address": {
                    "street": f"{random.randint(1,999)} Main St",
                    "city": random.choice(["New York", "London", "Berlin"]),
                    "postal_code": f"{random.randint(10000,99999)}",
                },
            },
            "notes": random.choice(COMMENTS),
        },
    ]

    return random.choice(templates)()


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------
def run(bootstrap: str, api_key: str, api_secret: str, topic: str, count: int):
    conf = {
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": api_key,
        "sasl.password": api_secret,
    }

    producer = Producer(conf)
    delivered = [0]
    errors    = [0]

    def on_delivery(err, msg):
        if err:
            errors[0] += 1
        else:
            delivered[0] += 1

    print(f"Producing {count} messages to '{topic}'…")
    for i in range(count):
        msg = _make_message()
        producer.produce(
            topic=topic,
            value=json.dumps(msg).encode("utf-8"),
            on_delivery=on_delivery,
        )
        if (i + 1) % 50 == 0:
            producer.flush()
            print(f"  {i+1}/{count} sent")

    producer.flush()
    print(f"\nDone. Delivered: {delivered[0]}  Errors: {errors[0]}")


def main():
    parser = argparse.ArgumentParser(description="Produce test data for the Flink scanner")
    parser.add_argument("--bootstrap",  default=os.getenv("CONFLUENT_BOOTSTRAP_SERVERS"))
    parser.add_argument("--api-key",    default=os.getenv("CONFLUENT_API_KEY"))
    parser.add_argument("--api-secret", default=os.getenv("CONFLUENT_API_SECRET"))
    parser.add_argument("--topic",      default="scanner-test")
    parser.add_argument("--count",      type=int, default=200)
    args = parser.parse_args()

    missing = [k for k, v in {
        "--bootstrap":  args.bootstrap,
        "--api-key":    args.api_key,
        "--api-secret": args.api_secret,
    }.items() if not v]

    if missing:
        parser.error(f"Missing: {', '.join(missing)}. Set via flags or .env file.")

    run(args.bootstrap, args.api_key, args.api_secret, args.topic, args.count)


if __name__ == "__main__":
    main()
