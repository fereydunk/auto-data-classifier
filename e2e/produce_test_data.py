"""
Produce realistic test messages to a Confluent Cloud topic.

Covers all 11 data tags so the Flink scanner has interesting data to classify:
  PII, PHI, PCI, CREDENTIALS, FINANCIAL, GOVERNMENT_ID,
  BIOMETRIC, GENETIC, NPI, LOCATION, MINOR

Messages are serialized as Confluent Avro wire format using the proper RawMessage schema
registered in Schema Registry (subject: raw-messages-value).

Each template populates only the fields relevant to its tag type; all other fields are null.
Confluent Cloud Flink auto-discovers the topic as a table with named columns corresponding
to the Avro schema fields.

Usage:
    python e2e/produce_test_data.py \\
        --bootstrap  pkc-xxx.us-east-1.aws.confluent.cloud:9092 \\
        --api-key    KAFKA_API_KEY \\
        --api-secret KAFKA_API_SECRET \\
        --topic      raw-messages \\
        --count      200 \\
        --schema-id  100060
"""

import argparse
import io as _io
import json
import os
import random
import struct
import sys

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


# ---------------------------------------------------------------------------
# Avro schema — must match the schema registered in Schema Registry.
# All fields nullable so each template only sets the fields it needs.
# ---------------------------------------------------------------------------
_AVRO_SCHEMA = fastavro.parse_schema({
    "type": "record",
    "name": "RawMessage",
    "namespace": "io.confluent.scanner",
    "doc": "Synthetic test messages covering all 11 sensitive data tags",
    "fields": [
        # PII
        {"name": "customer_id",        "type": ["null", "string"], "default": None},
        {"name": "first_name",         "type": ["null", "string"], "default": None},
        {"name": "last_name",          "type": ["null", "string"], "default": None},
        {"name": "email",              "type": ["null", "string"], "default": None},
        {"name": "phone_number",       "type": ["null", "string"], "default": None},
        {"name": "date_of_birth",      "type": ["null", "string"], "default": None},
        {"name": "ip_address",         "type": ["null", "string"], "default": None},
        {"name": "status",             "type": ["null", "string"], "default": None},
        # PCI + FINANCIAL
        {"name": "transaction_id",     "type": ["null", "string"], "default": None},
        {"name": "credit_card_number", "type": ["null", "string"], "default": None},
        {"name": "iban",               "type": ["null", "string"], "default": None},
        {"name": "routing_number",     "type": ["null", "string"], "default": None},
        {"name": "account_number",     "type": ["null", "string"], "default": None},
        {"name": "amount",             "type": ["null", "double"], "default": None},
        {"name": "currency",           "type": ["null", "string"], "default": None},
        # PHI
        {"name": "patient_id",         "type": ["null", "string"], "default": None},
        {"name": "mrn",                "type": ["null", "string"], "default": None},
        {"name": "diagnosis",          "type": ["null", "string"], "default": None},
        {"name": "medication",         "type": ["null", "string"], "default": None},
        {"name": "npi_number",         "type": ["null", "string"], "default": None},
        {"name": "insurance_id",       "type": ["null", "string"], "default": None},
        {"name": "comment",            "type": ["null", "string"], "default": None},
        # GOVERNMENT_ID
        {"name": "applicant_name",     "type": ["null", "string"], "default": None},
        {"name": "ssn",                "type": ["null", "string"], "default": None},
        {"name": "passport_number",    "type": ["null", "string"], "default": None},
        {"name": "driver_license",     "type": ["null", "string"], "default": None},
        {"name": "nationality",        "type": ["null", "string"], "default": None},
        # CREDENTIALS
        {"name": "service",            "type": ["null", "string"], "default": None},
        {"name": "username",           "type": ["null", "string"], "default": None},
        {"name": "password",           "type": ["null", "string"], "default": None},
        {"name": "api_key",            "type": ["null", "string"], "default": None},
        {"name": "connection_string",  "type": ["null", "string"], "default": None},
        # GENETIC + BIOMETRIC
        {"name": "sample_id",          "type": ["null", "string"], "default": None},
        {"name": "dna_sequence",       "type": ["null", "string"], "default": None},
        {"name": "genome",             "type": ["null", "string"], "default": None},
        {"name": "fingerprint",        "type": ["null", "string"], "default": None},
        {"name": "facial_recognition", "type": ["null", "string"], "default": None},
        # MINOR
        {"name": "child_id",           "type": ["null", "string"], "default": None},
        {"name": "guardian_email",     "type": ["null", "string"], "default": None},
        {"name": "minor_data",         "type": ["null", "string"], "default": None},
        {"name": "age",                "type": ["null", "int"],    "default": None},
        # NPI
        {"name": "deal_id",            "type": ["null", "string"], "default": None},
        {"name": "mnpi",               "type": ["null", "string"], "default": None},
        {"name": "notes",              "type": ["null", "string"], "default": None},
        # E-COMMERCE (flattened)
        {"name": "order_id",           "type": ["null", "string"], "default": None},
        {"name": "cust_first_name",    "type": ["null", "string"], "default": None},
        {"name": "cust_last_name",     "type": ["null", "string"], "default": None},
        {"name": "cust_email",         "type": ["null", "string"], "default": None},
        {"name": "cust_phone",         "type": ["null", "string"], "default": None},
        {"name": "payment_cc_number",  "type": ["null", "string"], "default": None},
        {"name": "billing_street",     "type": ["null", "string"], "default": None},
        {"name": "billing_city",       "type": ["null", "string"], "default": None},
        {"name": "billing_postal_code","type": ["null", "string"], "default": None},
    ],
})

# All field names (for building empty records)
_ALL_FIELDS = [f["name"] for f in _AVRO_SCHEMA["fields"]]


def _empty_record():
    """Return a record with all fields set to None (Avro null)."""
    return {f: None for f in _ALL_FIELDS}


# ---------------------------------------------------------------------------
# Sample data helpers
# ---------------------------------------------------------------------------
FIRST_NAMES = ["Alice", "Bob", "Carlos", "Diana", "Eve", "Frank", "Grace", "Henry"]
LAST_NAMES  = ["Smith", "Jones", "Patel", "Kim", "Mueller", "Tanaka", "Silva"]
DOMAINS     = ["example.com", "acme.org", "test.net", "corp.io"]
COMMENTS    = [
    "Patient presented with chest pain and shortness of breath.",
    "Customer called to dispute a charge on their credit card.",
    "She reported feeling dizzy after taking her medication.",
    "The account holder confirmed their date of birth over the phone.",
    "Genome sequencing results uploaded for research study.",
    "Child account linked to parent guardian John Smith.",
    "Merger discussions with target company are confidential.",
]


def _email(fn, ln): return f"{fn.lower()}.{ln.lower()}@{random.choice(DOMAINS)}"
def _phone():       return f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"
def _ssn():         return f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"
def _cc():          return f"4111-1111-1111-{random.randint(1000,9999)}"
def _iban():        return f"GB{random.randint(10,99)}BARC{random.randint(10000000,99999999):08d}{random.randint(10000000,99999999):08d}"
def _mrn():         return f"MRN-{random.randint(100000,999999)}"
def _routing():     return f"02100002{random.randint(1,9)}"
def _bank_acct():   return str(random.randint(10000000, 99999999))
def _dna():         return "".join(random.choices("ATCG", k=60))
def _ip():          return f"192.168.{random.randint(0,255)}.{random.randint(1,254)}"
def _dob():         return f"{random.randint(1950,2005)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"

def _password():
    import string
    return "".join(random.choices(string.ascii_letters + string.digits + "!@#$", k=16))

def _api_key():
    import string
    return "ak_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=32))


# ---------------------------------------------------------------------------
# Message templates — each returns a partial record dict (only set fields)
# ---------------------------------------------------------------------------
def _make_record():
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)

    templates = [
        # PII
        lambda: {
            "customer_id": f"CUST-{random.randint(10000,99999)}",
            "first_name": fn, "last_name": ln,
            "email": _email(fn, ln), "phone_number": _phone(),
            "date_of_birth": _dob(), "ip_address": _ip(),
            "status": random.choice(["active", "inactive", "pending"]),
        },
        # PCI + FINANCIAL
        lambda: {
            "transaction_id": f"TXN-{random.randint(100000,999999)}",
            "credit_card_number": _cc(), "iban": _iban(),
            "routing_number": _routing(), "account_number": _bank_acct(),
            "amount": round(random.uniform(1.0, 9999.99), 2),
            "currency": random.choice(["USD", "EUR", "GBP"]),
        },
        # PHI
        lambda: {
            "patient_id": _mrn(), "mrn": _mrn(),
            "diagnosis": random.choice(["ICD10:J45", "ICD10:E11", "ICD10:I10", "ICD10:F32"]),
            "medication": random.choice(["Metformin 500mg", "Lisinopril 10mg", "Atorvastatin 20mg"]),
            "npi_number": f"1{random.randint(0,999999999):09d}",
            "insurance_id": f"INS-{random.randint(100000,999999)}",
            "comment": random.choice(COMMENTS),
        },
        # GOVERNMENT_ID
        lambda: {
            "applicant_name": f"{fn} {ln}", "ssn": _ssn(),
            "passport_number": f"P{random.randint(10000000,99999999)}",
            "driver_license": f"DL{random.randint(1000000,9999999)}",
            "nationality": random.choice(["US", "UK", "DE", "FR"]),
        },
        # CREDENTIALS
        lambda: {
            "service": random.choice(["database", "cache", "api", "queue"]),
            "username": f"{fn.lower()}{random.randint(1,99)}",
            "password": _password(), "api_key": _api_key(),
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
        # NPI
        lambda: {
            "deal_id": f"DEAL-{random.randint(1000,9999)}",
            "mnpi": "pre-announcement earnings data",
            "notes": "Merger discussions with target company are strictly confidential.",
            "amount": round(random.uniform(1e6, 1e9), 2),
        },
        # E-commerce (flattened)
        lambda: {
            "order_id": f"ORD-{random.randint(100000,999999)}",
            "cust_first_name": fn, "cust_last_name": ln,
            "cust_email": _email(fn, ln), "cust_phone": _phone(),
            "payment_cc_number": _cc(),
            "billing_street": f"{random.randint(1,999)} Main St",
            "billing_city": random.choice(["New York", "London", "Berlin"]),
            "billing_postal_code": str(random.randint(10000, 99999)),
            "notes": random.choice(COMMENTS),
        },
    ]

    record = _empty_record()
    record.update(random.choice(templates)())
    return record


# ---------------------------------------------------------------------------
# Confluent Avro wire-format: 0x00 | schema_id (4 bytes BE) | avro bytes
# ---------------------------------------------------------------------------
def _avro_encode(schema_id: int, record: dict) -> bytes:
    buf = _io.BytesIO()
    buf.write(b"\x00")
    buf.write(struct.pack(">I", schema_id))
    fastavro.schemaless_writer(buf, _AVRO_SCHEMA, record)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------
def run(bootstrap: str, api_key: str, api_secret: str, topic: str, count: int, schema_id: int):
    conf = {
        "bootstrap.servers":  bootstrap,
        "security.protocol":  "SASL_SSL",
        "sasl.mechanisms":    "PLAIN",
        "sasl.username":      api_key,
        "sasl.password":      api_secret,
    }
    producer  = Producer(conf)
    delivered = [0]
    errors    = [0]

    def on_delivery(err, msg):
        if err:
            errors[0] += 1
        else:
            delivered[0] += 1

    print(f"Producing {count} messages to '{topic}' (schema_id={schema_id})…")
    for i in range(count):
        record     = _make_record()
        avro_bytes = _avro_encode(schema_id, record)
        producer.produce(topic=topic, value=avro_bytes, on_delivery=on_delivery)
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
    parser.add_argument("--topic",      default="raw-messages")
    parser.add_argument("--count",      type=int, default=200)
    parser.add_argument("--schema-id",  type=int, default=100060,
                        help="Confluent SR schema ID for raw-messages-value (default: 100060)")
    args = parser.parse_args()

    missing = [k for k, v in {
        "--bootstrap":  args.bootstrap,
        "--api-key":    args.api_key,
        "--api-secret": args.api_secret,
    }.items() if not v]
    if missing:
        parser.error(f"Missing: {', '.join(missing)}. Set via flags or env.")

    run(args.bootstrap, args.api_key, args.api_secret, args.topic, args.count, args.schema_id)


if __name__ == "__main__":
    main()
