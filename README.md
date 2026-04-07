# Auto Data Classifier

Domain-agnostic, real-time data classification for Confluent Cloud.
Inspects Kafka messages in-flight, identifies sensitive data across 11 standard tags, and surfaces field-level labels directly in the Confluent Stream Catalog — without sending data outside your infrastructure.

## Who is this for?

Any organisation running Confluent Cloud that needs to know what sensitive data flows through their Kafka topics — before it lands in downstream systems:

| Industry | Use case |
|---|---|
| **Retail / E-commerce** | Customer PII in order events, loyalty data |
| **Healthcare** | PHI in patient records, clinical notes |
| **Financial Services** | PCI in payment streams, wire transfers |
| **HR / Payroll** | Employee SSNs, bank accounts, salary data |
| **DevOps / SaaS** | Leaked credentials in log events, config payloads |
| **Web3 / Crypto** | Wallet addresses, crypto wallets in transaction logs |

---

## How it works

Two complementary modes — pick one or run both:

### Mode 1 — Streaming Pipeline (continuous)

Every message flowing through a Kafka topic is classified in real time.

```
Source topic (raw Kafka messages)
        │
        ▼
kafka-pipeline  ──────────────────────────────────────────────────────────┐
  ├─ Deserialize (Avro via Schema Registry, or plain JSON)                │
  ├─ POST /classify → classifier-service                                  │
  │     ├─ Layer 1: field name / schema metadata  (always on)            │
  │     ├─ Layer 2: regex / structural patterns   (on by default)        │
  │     └─ Layer 3: spaCy NER + GLiNER AI model   (on by default)       │
  ├─ Route enriched message:                                              │
  │     ├─ classified-messages  (tags detected)                          │
  │     └─ classified-safe      (no tags)                                │
  ├─ Write to classification-audit topic                                  │
  └─ POST recommendations → review-api                                   │
                                                                          │
review-api  (field-level human review)  ──────────────────────────────────┘
  ├─ GET  /recommendations          sorted by confidence desc
  ├─ POST /recommendations/{id}/approve  → Stream Catalog tag applied
  ├─ POST /recommendations/{id}/reject
  └─ POST /recommendations/bulk-approve  (approve all score ≥ 0.85)
```

### Mode 2 — Interactive Flink SQL Scanner (on-demand)

Run a one-shot Flink SQL query to profile a topic for N minutes, review the field-level recommendations in the results pane, then approve and apply tags — all from the Confluent Cloud Flink workspace.

```
Flink SQL workspace
  │
  ├─ Statement A — classify_fields() UDTF
  │     Reads topic for N minutes, classifies each message,
  │     shows (field_path, tag, confidence, layer, source, example)
  │
  ├─ User reviews results table
  │
  └─ Statement B — apply_tag() scalar UDF
        Applies approved tags to Confluent Stream Catalog
```

All model inference is **100% local** — no data leaves your environment at runtime.

---

## Classification taxonomy — 11 flat tags

| Tag | What it covers |
|---|---|
| `PII` | Names, email, phone, date of birth, username |
| `PHI` | Medical records, diagnoses, medications, DEA/NPI numbers |
| `PCI` | Credit/debit cards, IBAN, SWIFT codes, crypto wallets |
| `CREDENTIALS` | Passwords, API keys, tokens, connection strings |
| `FINANCIAL` | Bank account numbers, routing numbers |
| `GOVERNMENT_ID` | SSN, passport, driver's licence, national tax IDs |
| `BIOMETRIC` | Fingerprints, facial geometry, retina/iris scans |
| `GENETIC` | DNA sequences, genome/genotype data |
| `NPI` | Non-Public Information — insider financials, M&A data |
| `LOCATION` | GPS coordinates, IP addresses, precise geolocation |
| `MINOR` | Data relating to a person under 13 or 16 (COPPA/GDPR) |

> Sensitivity levels (HIGH/MEDIUM/LOW) are deliberately **not** baked in — organisations define these differently. The 11 tags are the facts; you apply your own sensitivity policy on top.

---

## Three-layer classification

```
Layer 1 — Field name / schema metadata        (always on, zero data access)
  Inspects only the field name.
  "email", "ssn", "creditCardNumber" → instant, deterministic match.
  Handles camelCase, snake_case, PascalCase, abbreviations.

Layer 2 — Regex / structural patterns         (on by default, opt-out available)
  Runs Presidio's built-in pattern recognisers + custom recognisers for
  IBAN, SWIFT, crypto wallets, bank accounts, routing numbers, credentials.

Layer 3 — AI model: spaCy + GLiNER            (on by default, opt-out available)
  spaCy en_core_web_lg for named entity recognition.
  GLiNER zero-shot model for entity types not covered by regex.
  Handles free-text fields (comment, notes, description) that would be
  missed by field-name and regex layers.
```

Customers control depth with `MAX_LAYER` (1 | 2 | 3, default 3).
Each detected entity in the response carries `layer` and `source` so reviewers know exactly how a tag was found.

---

## Services

| Service | Port | Purpose |
|---|---|---|
| `classifier-service` | 8000 | FastAPI — classifies field values, three-layer engine |
| `review-api` | 8001 | FastAPI — stores recommendations, drives human review |
| `kafka-pipeline` | — | Confluent Cloud consumer/producer, routes messages |
| `flink-scanner` | — | Java UDFs for Confluent Cloud Flink SQL |

---

## Repository layout

```
auto-data-classifier/
├── classifier-service/          FastAPI classification engine
│   ├── main.py                  Endpoints: /classify, /health
│   ├── classification/
│   │   └── taxonomy.py          DataTag enum, tag_entity()
│   └── recognizers/
│       ├── field_name_recognizer.py   Layer 1
│       ├── phi_recognizers.py         PHI patterns
│       ├── pci_recognizers.py         PCI patterns
│       ├── financial_recognizers.py   FINANCIAL patterns
│       ├── credentials_recognizers.py CREDENTIALS patterns
│       └── gliner_recognizer.py       Layer 3 GLiNER bridge
│
├── review-api/                  Human-in-the-loop review service
│   ├── main.py                  REST endpoints
│   ├── store.py                 SQLite persistence (aiosqlite)
│   ├── models.py                Pydantic models
│   └── catalog_client.py        Stream Catalog tag application
│
├── kafka-pipeline/              Confluent Cloud consumer/producer
│   ├── pipeline.py              Main async loop
│   ├── config.py                All environment variable config
│   └── catalog_tagger.py        Stream Catalog tagger + tag definitions
│
├── flink-scanner/               Confluent Cloud Flink SQL UDFs
│   ├── pom.xml                  Maven build (Java 11, Flink 1.19)
│   ├── src/.../ClassifyFieldsUDF.java   UDTF — calls /classify per message
│   ├── src/.../ApplyTagUDF.java         Scalar — applies tag to Stream Catalog
│   ├── sql/scan.sql             Interactive scan template (2 statements)
│   └── scripts/
│       ├── build.sh             mvn package → fat JAR
│       └── register.sh          Upload JAR, CREATE FUNCTION in Flink
│
├── e2e/                         End-to-end test tooling
│   ├── start_classifier.sh      Start classifier natively (no Docker needed)
│   ├── produce_test_data.py     Produce 200 realistic messages to Confluent
│   └── verify_e2e.py            Smoke test: 11 /classify assertions + Kafka round-trip
│
├── tests/                       Unit / integration test suite (222 tests)
├── docs/                        Architecture, taxonomy, testing, tuning guides
├── flink-sql/                   Confluent Cloud Flink SQL routing queries
└── docker-compose.yml           All three services (classifier + review-api + pipeline)
```

---

## Quick start

### Run the classifier locally (no Docker)

```bash
# One-time setup
pip install gliner
python -m spacy download en_core_web_lg

# Start the service
cd classifier-service
uvicorn main:app --port 8000

# Smoke test
curl -s http://localhost:8000/health | python3 -m json.tool
curl -s -X POST http://localhost:8000/classify \
  -H "Content-Type: application/json" \
  -d '{"fields": {"email": "alice@example.com", "ssn": "123-45-6789"}}' \
  | python3 -m json.tool
```

### Run the full stack with Docker Compose

```bash
cp .env.example .env   # fill in Confluent credentials
docker-compose up
```

### Run the Flink SQL scanner

```bash
# 1. Build the UDF JAR
bash flink-scanner/scripts/build.sh

# 2. Register UDFs in Confluent Cloud
export CONFLUENT_ENVIRONMENT=env-xxxxx
export CONFLUENT_COMPUTE_POOL=lfcp-xxxxx
bash flink-scanner/scripts/register.sh

# 3. Open Confluent Cloud → Flink SQL workspace
#    Paste flink-scanner/sql/scan.sql, set your classifier URL and topic, run.
```

### End-to-end test

```bash
# Start classifier + expose with ngrok
bash e2e/start_classifier.sh           # terminal 1
ngrok http 8000                        # terminal 2

# Verify all layers
.venv/bin/python e2e/verify_e2e.py --classifier-url https://xxxx.ngrok-free.app

# Produce test data
.venv/bin/python e2e/produce_test_data.py \
  --topic scanner-test --count 200

# Run unit + integration tests
pytest tests/ -q
```

---

## Configuration

All configuration is via environment variables. See `.env.example` for the full list.

| Variable | Default | Purpose |
|---|---|---|
| `MAX_LAYER` | `3` | Classification depth: 1=field name, 2=+regex, 3=+AI |
| `CLASSIFIER_URL` | `http://localhost:8000` | Classifier service endpoint (pipeline) |
| `REVIEW_API_URL` | `http://localhost:8001` | Review API endpoint (pipeline) |
| `SOURCE_TOPIC` | `raw-messages` | Kafka topic to consume |
| `SINK_TOPIC_CLASSIFIED` | `classified-messages` | Output for messages with tags |
| `SINK_TOPIC_SAFE` | `classified-safe` | Output for clean messages |
| `SINK_TOPIC_AUDIT` | `classification-audit` | Audit log topic |
| `BATCH_SIZE` | `50` | Messages per commit batch |
| `MAX_CONCURRENT` | `10` | Concurrent classification requests |

---

## Docs

- [Architecture](docs/ARCHITECTURE.md) — component design, data flows, sequence diagrams
- [Taxonomy](docs/TAXONOMY.md) — all 11 tags, entity type mappings, confidence scoring
- [Testing](docs/TESTING.md) — unit tests, integration tests, end-to-end test guide
- [Tuning](docs/TUNING.md) — performance, confidence thresholds, layer configuration
