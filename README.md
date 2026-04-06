# Auto Data Classifier

Domain-agnostic, real-time data classification for Confluent Cloud.
Inspects Kafka messages in-flight, identifies sensitive data (PII, PHI, PCI, credentials, and more), and surfaces field-level tags directly in the Confluent Stream Catalog — without sending data outside your infrastructure.

## Who is this for?

Any organisation running Confluent Cloud that needs to know what sensitive data is flowing through their Kafka topics — before it lands in downstream systems:

| Industry | Use case |
|---|---|
| **Retail / E-commerce** | Customer PII in order events, loyalty data |
| **Healthcare** | PHI in patient records, clinical notes |
| **Financial Services** | PCI in payment streams, wire transfers |
| **HR / Payroll** | Employee SSNs, bank accounts, salary data |
| **DevOps / SaaS** | Leaked credentials in log events, config payloads |
| **Web3 / Crypto** | Wallet addresses in transaction logs |

## How it works

```
Confluent Cloud Kafka
  └─ Source topic (raw messages, Avro or JSON)
          │
          ▼
  Kafka Pipeline (your compute)
  ├─ Avro deserialization via Schema Registry
  ├─ POST to local Classifier Service
  │     ├─ Presidio regex recognizers  (fast path: IBAN, JWT, AWS keys …)
  │     └─ GLiNER NER model            (slow path: names, addresses, diagnoses …)
  ├─ Taxonomy: entity type → DataCategory → SensitivityLevel
  ├─ Route enriched message to tiered output topic
  └─ Tag schema fields in Confluent Stream Catalog
          │
          ▼
  Output topics              Stream Catalog
  ├─ classified-pii          customer.email  → PII
  ├─ classified-medium       patient.mrn     → PHI
  ├─ classified-safe         order.id        → INTERNAL
  └─ classification-audit
```

All model inference is **100% local** — no data leaves your environment at runtime.

## Classification taxonomy

```
DataCategory    SensitivityLevel    Example entities
────────────    ────────────────    ────────────────────────────────────────
PHI             CRITICAL            MEDICAL_RECORD, MEDICATION, NPI, DEA
CREDENTIALS     CRITICAL            AWS_ACCESS_KEY, JWT_TOKEN, CONNECTION_STRING
PII             HIGH                PERSON, EMAIL_ADDRESS, US_SSN, PASSPORT
PCI             HIGH                CREDIT_CARD, IBAN_CODE, CRYPTO_WALLET
CONFIDENTIAL    MEDIUM              ORGANIZATION, CONTRACT_NUMBER
INTERNAL        LOW                 ORDER_NUMBER, LOYALTY_CARD, SHIPMENT_ID
(none)          CLEAN               —
```

See [docs/TAXONOMY.md](docs/TAXONOMY.md) for the full entity type catalogue.

## Project structure

```
auto-data-classifier/
├── classifier-service/          # FastAPI app — Presidio + GLiNER
│   ├── classification/
│   │   └── taxonomy.py          # DataCategory / SensitivityLevel definitions
│   ├── recognizers/
│   │   ├── gliner_recognizer.py # Zero-shot NER (90+ entity labels)
│   │   ├── pci_recognizers.py   # IBAN, SWIFT, routing, crypto wallets
│   │   ├── phi_recognizers.py   # NPI, DEA number, health insurance IDs
│   │   └── credentials_recognizers.py  # AWS keys, JWTs, connection strings
│   ├── main.py                  # /classify and /health endpoints
│   ├── requirements.txt
│   └── Dockerfile
├── kafka-pipeline/              # Consumer → classify → route → tag
│   ├── pipeline.py              # Main async consumer/producer loop
│   ├── catalog_tagger.py        # Confluent Stream Catalog field tagging
│   ├── config.py                # All config from environment variables
│   ├── requirements.txt
│   └── Dockerfile
├── flink-sql/
│   └── routing.sql              # Confluent Cloud Flink SQL routing rules
├── tests/                       # 130 unit tests, zero external dependencies
├── e2e_test.py                  # End-to-end demo across 7 industries
├── docker-compose.yml
├── pytest.ini
└── .env.example
```

## Quick start

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env — fill in Confluent Cloud and Schema Registry credentials
```

### 2. Run with Docker Compose

```bash
docker compose up --build
```

The classifier service pre-downloads spaCy and GLiNER at image build time.
After the first build, `TRANSFORMERS_OFFLINE=1` prevents any outbound calls at runtime.

### 3. Verify the classifier is up

```bash
curl http://localhost:8000/health
# {"status":"ok","analyzer_ready":true,"version":"2.0.0"}
```

### 4. Classify a message manually

```bash
curl -s -X POST http://localhost:8000/classify \
  -H "Content-Type: application/json" \
  -d '{
    "fields": {
      "customer": {"name": "Alice Johnson", "email": "alice@example.com"},
      "payment": {"card": "4111111111111111"}
    }
  }' | python3 -m json.tool
```

Example response:

```json
{
  "sensitivity_level": "HIGH",
  "categories": ["PCI", "PII"],
  "detected_entities": {
    "customer.name":  [{"entity_type": "PERSON",       "category": "PII", "score": 0.85}],
    "customer.email": [{"entity_type": "EMAIL_ADDRESS", "category": "PII", "score": 1.0}],
    "payment.card":   [{"entity_type": "CREDIT_CARD",  "category": "PCI", "score": 1.0}]
  },
  "classified_at": "2026-04-06T10:00:00Z",
  "classifier_version": "2.0.0"
}
```

## Configuration

All configuration is via environment variables. See `.env.example` for a full reference.

| Variable | Required | Description |
|---|---|---|
| `CONFLUENT_BOOTSTRAP_SERVERS` | Yes | Kafka bootstrap (e.g. `pkc-xxx.confluent.cloud:9092`) |
| `CONFLUENT_API_KEY` | Yes | Kafka cluster API key |
| `CONFLUENT_API_SECRET` | Yes | Kafka cluster API secret |
| `CONFLUENT_SR_URL` | Yes | Schema Registry URL (e.g. `https://psrc-xxx.confluent.cloud`) |
| `CONFLUENT_SR_API_KEY` | Yes | Schema Registry API key |
| `CONFLUENT_SR_API_SECRET` | Yes | Schema Registry API secret |
| `CONFLUENT_SR_CLUSTER_ID` | Yes | Schema Registry cluster ID (`lsrc-xxx`) — found in Confluent Console → Schema Registry → Cluster settings |
| `SOURCE_TOPIC` | No | Topic to consume (default: `raw-messages`) |
| `SINK_TOPIC_PII` | No | HIGH sensitivity output (default: `classified-pii`) |
| `SINK_TOPIC_MEDIUM` | No | MEDIUM sensitivity output (default: `classified-medium`) |
| `SINK_TOPIC_SAFE` | No | LOW/CLEAN output (default: `classified-safe`) |
| `SINK_TOPIC_AUDIT` | No | Lightweight audit log topic (default: `classification-audit`) |
| `CONSUMER_GROUP` | No | Kafka consumer group (default: `auto-data-classifier`) |
| `CLASSIFIER_URL` | No | Classifier service URL (default: `http://localhost:8000`) |

## Running tests

```bash
# Create venv and install test dependencies
python3 -m venv .venv
.venv/bin/pip install pytest pytest-asyncio httpx fastapi presidio-analyzer pydantic

# Run the full test suite (no Confluent Cloud or GLiNER required)
.venv/bin/pytest tests/ -v
```

```
130 passed in 0.46s
```

See [docs/TESTING.md](docs/TESTING.md) for details on test strategy and adding new tests.

## Running the end-to-end demo

```bash
# Requires: presidio-analyzer, spacy + en_core_web_sm (see docs/TESTING.md)
.venv/bin/python e2e_test.py
```

Runs classification across 7 industry scenarios and prints colour-coded results.
GLiNER is stubbed in the demo venv (no GPU/model download needed) — regex-based detections are real.

## Confluent Cloud Flink SQL routing

After the pipeline routes classified messages to output topics, you can add Flink SQL jobs in Confluent Cloud to further route, filter, or aggregate:

```sql
-- Route HIGH sensitivity messages (copy routing.sql into Flink SQL workspace)
INSERT INTO sink_pii
SELECT payload, classification, CURRENT_TIMESTAMP
FROM classified_messages
WHERE classification.sensitivity_level IN ('HIGH', 'CRITICAL');
```

See [flink-sql/routing.sql](flink-sql/routing.sql) for the full routing setup.

## Adding a new industry vertical

1. Add new entity types to `classifier-service/classification/taxonomy.py` under the appropriate `DataCategory`
2. Add GLiNER labels to `classifier-service/recognizers/gliner_recognizer.py` (`GLINER_ENTITY_MAP`)
3. Optionally add a regex recognizer file (e.g. `recognizers/retail_recognizers.py`) for structural patterns
4. Register the new recognizers in `classifier-service/main.py` → `build_analyzer()`
5. Add tests in `tests/test_<vertical>_recognizers.py`

No changes needed to sensitivity logic, catalog tagging, or the Kafka pipeline.

## Known limitations

See [docs/TUNING.md](docs/TUNING.md) for known false positives and score-tuning guidance.

The most significant current limitation is that **GLiNER requires a GPU or beefy CPU** for production throughput. At 200–500 msg/s on CPU (GLiNER medium model), it is well suited for moderate-volume topics. Very high-throughput topics (>1000 msg/s) should consider:
- Running multiple classifier replicas
- Using the fast path (regex only) for numeric/structured fields
- Reserving GLiNER for free-text fields only
