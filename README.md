# Auto Data Classifier

Domain-agnostic, real-time data classification for Confluent Cloud.
Inspects Kafka messages in-flight, identifies sensitive data across 11 standard tags, and applies field-level tags directly to Schema Registry schema definitions — without sending data outside your infrastructure.

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
review-api  (field-level human review + staging)  ───────────────────────┘
  ├─ GET  /recommendations          sorted by confidence desc
  ├─ POST /recommendations/{id}/approve  → marks STAGED (no catalog push yet)
  ├─ POST /recommendations/{id}/unstage  → STAGED back to PENDING
  ├─ POST /recommendations/{id}/reject
  ├─ POST /recommendations/submit-staged → batch POST every STAGED → Stream
  │                                        Catalog in ONE call (avoids creating
  │                                        a new schema version per tag)
  └─ POST /recommendations/bulk-approve   (stage all ≥ 0.85, then submit batch)

The browser UI at http://localhost:8001/ surfaces this as: "Stage" button per
row → bottom panel shows the staged queue → "Submit N to Stream Catalog" button
fires the batch.
```

### Mode 2 — Flink SQL Scanner (scheduled + event-driven)

Three long-running Flink statements profile topics automatically and write classification results to a results topic. Tags are applied to the Schema Registry schema itself — not the Stream Catalog.

```
Confluent Cloud Flink (3 continuous statements)
  │
  ├─ Statement A — Scheduled trigger
  │     TUMBLE window fires every SCAN_INTERVAL_MINUTES
  │     → writes trigger to {topic}-scan-triggers
  │
  ├─ Statement B — Schema-evolution trigger
  │     schema_watcher() UDF polls SR for new schema versions
  │     → writes trigger to {topic}-scan-triggers on version change
  │
  └─ Statement C — Scan driver
        interval-joins trigger topic with source topic
        classify_fields() UDTF on last SAMPLE_WINDOW_MINUTES of data
        → writes (field_path, tag, confidence, layer, source) to {topic}-scan-results

apply_tags.py (run by operator when ready)
  ├─ reads latest batch from {topic}-scan-results
  ├─ skips fields already tagged in the schema (no re-prompt)
  ├─ presents new/changed tags interactively [y/n/q]  (or --yes to approve all)
  └─ fetches schema → patches all approved fields → registers one new version
```

Manual trigger: `start_scan.sh --now` inserts a row into the trigger topic;
Statement C reacts within seconds.

> **USING CONNECTIONS — internet egress for UDFs:**
> `classify_fields()` calls the classifier service via Confluent's **USING CONNECTIONS** feature,
> which must be enabled for your Confluent Cloud organisation by the Confluent account team.
> The `classifier-service` connection (type=rest) is created once with `confluent flink connection create`
> and bound to the UDF at registration time (`register.sh`).
> See [Architecture](docs/ARCHITECTURE.md) for full setup details.

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
| `setup_wizard` | 8002 | FastAPI — local orchestrator for the end-to-end demo (CC sign-in, env pick, AI start, ngrok tunnel, Flink UDF register, demo provision) |
| `classifier-service` | 8000 | FastAPI — classifies field values, three-layer engine |
| `review-api` | 8001 | FastAPI — stores recommendations, browser UI for stage/reject + batch submit to Stream Catalog |
| `kafka-pipeline` | — | Confluent Cloud consumer/producer, routes messages (streaming mode) |
| `flink-scanner` | — | Java UDFs for Confluent Cloud Flink SQL (scanner mode) |
| `setup_wizard.results_bridge` | — | Long-running consumer that bridges `{topic}-scan-results` → review-api recommendations |

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
├── review-api/                  Human-in-the-loop review service + UI
│   ├── main.py                  REST endpoints (stage / unstage / submit-staged
│   │                            / validate-staged / sources / DELETE by topic)
│   ├── store.py                 SQLite persistence (aiosqlite)
│   ├── models.py                Pydantic models (PENDING / STAGED / APPROVED / REJECTED)
│   ├── catalog_client.py        Stream Catalog tag application + batch submit
│   │                            with SR field-existence validation gate
│   ├── sr_schema.py             SR schema fetch + Avro field-path extractor
│   │                            (60-s TTL cache, fail-closed)
│   └── static/index.html        Browser UI: per-row Stage/Reject + bottom
│                                staged queue with stale-row badge + Submit-all
│
├── setup_wizard/                Local end-to-end demo orchestrator (port 8002)
│   ├── main.py                  FastAPI: prereqs, CC sign-in (auto-mints
│   │                            cloud key), env pick, demo orchestration with
│   │                            clean-slate reset (recs → SR subject → topic)
│   ├── static/index.html        Browser UI: 6-card flow with field-count
│   │                            number input on Card 5
│   ├── field_pool.py            46-field vocabulary (name, Avro type, generator,
│   │                            category) — composed dynamically per demo run
│   ├── schema_builder.py        build_demo_schema(N) → fresh Avro from pool
│   └── results_bridge.py        Kafka consumer: scan-results → review-api
│                                (decodes Confluent wire-format Avro via SR)
│
├── kafka-pipeline/              Confluent Cloud consumer/producer
│   ├── pipeline.py              Main async loop
│   ├── config.py                All environment variable config
│   ├── catalog_tagger.py        Stream Catalog tagger with SR validation gate
│   └── sr_schema.py             Service-isolated copy of the SR field-path helper
│
├── flink-scanner/               Confluent Cloud Flink SQL UDFs + scanner
│   ├── pom.xml                  Maven build (Java 11, Flink 1.19)
│   ├── scan.env                 All configuration (schedule, credentials)
│   ├── apply_tags.py            Interactive review + schema patching (Python)
│   ├── src/.../ClassifyFieldsUDF.java   UDTF — calls /classify per message
│   ├── src/.../SchemaWatcherUDF.java    UDTF — polls SR, emits on schema change
│   ├── src/.../ApplyTagUDF.java         Scalar — tags fields after fetching
│   │                                    + validating against the live SR schema
│   ├── sql/scan.sql             Three-trigger Flink SQL (statements A, B, C);
│   │                            JSON_OBJECT field list is generated per run
│   └── scripts/
│       ├── build.sh             mvn package → fat JAR
│       ├── register.sh          Upload JAR, CREATE FUNCTION (3 UDFs)
│       ├── generate_json_object.py  Fetches SR schema, emits the
│       │                            JSON_OBJECT(KEY/VALUE …) for scan.sql
│       └── start_scan.sh        start | --now | --stop | --status
│                                (delete-then-poll-then-create — race-free)
│
├── e2e/                         End-to-end test tooling
│   ├── start_classifier.sh      Start classifier natively (no Docker needed)
│   ├── produce_test_data.py     Produce realistic messages (Confluent Avro wire format)
│   ├── local_scanner.py         Fallback scanner — reads Kafka, calls classifier locally,
│   │                            writes to scan-results topic (use if USING CONNECTIONS
│   │                            is not enabled for your org)
│   └── verify_e2e.py            Smoke test: 11 /classify assertions + Kafka round-trip
│
├── tests/                       Unit / integration test suite (298 tests)
├── startup.sh                   Launch the setup wizard at http://localhost:8002
├── docs/                        Architecture, taxonomy, testing, tuning guides
├── flink-sql/                   Confluent Cloud Flink SQL routing queries
└── docker-compose.yml           All three services (classifier + review-api + pipeline)
```

---

## Quick start

### Easiest path — the setup wizard

For the Flink-UDF demo flow (sign in to CC, pick an env, start the AI
model + ngrok tunnel, register UDFs, then per-run provision a fresh
N-field demo topic — N picked in Card 5's UI input, schema built
dynamically and registered to SR — and watch recommendations land in the
review UI):

```bash
./startup.sh                       # browser opens at http://localhost:8002
```

Walk the cards top-to-bottom: prereqs → CC sign-in → pick env → "Start
auto-data-classifier" → "Run test demo" → "Open Review UI". The wizard
mints API keys, writes `.env` + `flink-scanner/scan.env`, and orchestrates
every subprocess. Each card has its own live log panel.

For lower-level usage (run only the classifier, or the streaming pipeline
without the wizard), use the manual paths below.

### Run the classifier locally (no Docker)

```bash
# One-time setup
pip install -r classifier-service/requirements.txt
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

> **Mac laptop note:** GLiNER (Layer 3) is compute-heavy. Run the pipeline with reduced concurrency and a longer timeout to avoid inference timeouts:
> ```bash
> MAX_CONCURRENT=3 CLASSIFIER_TIMEOUT_S=15 python kafka-pipeline/pipeline.py
> ```

### Run the full stack with Docker Compose

```bash
cp .env.example .env   # fill in Confluent credentials
docker-compose up
```

### Run the Flink SQL scanner

```bash
# 1. Fill in scan.env (classifier URL, SR creds, Kafka creds, topic, schedule)
vim flink-scanner/scan.env

# 2. Build the UDF JAR
bash flink-scanner/scripts/build.sh

# 3. Register UDFs in Confluent Cloud (once per environment)
#    classify_fields is registered USING CONNECTIONS (classifier-service)
bash flink-scanner/scripts/register.sh

# 4. Start all three Flink statements (runs continuously)
bash flink-scanner/scripts/start_scan.sh

# 5. Trigger a manual scan at any time
bash flink-scanner/scripts/start_scan.sh --now
# Fire a second trigger ~60s later to advance the watermark and unblock the interval join
bash flink-scanner/scripts/start_scan.sh --now

# 6. Review and apply tags when ready
source flink-scanner/scan.env
python flink-scanner/apply_tags.py \
  --topic     $SOURCE_TOPIC \
  --sr-url    $SR_URL --sr-key $SR_KEY --sr-secret $SR_SECRET \
  --bootstrap $KAFKA_BOOTSTRAP --kafka-key $KAFKA_KEY --kafka-secret $KAFKA_SECRET
# Add --yes to auto-approve all (skips interactive prompt)
```

### End-to-end test

```bash
# Start classifier (terminal 1)
bash e2e/start_classifier.sh

# Expose via ngrok — copy the https URL into CLASSIFIER_URL in scan.env
# and update the classifier-service Flink connection endpoint to match
ngrok http 8000                        # terminal 2

# Verify all layers
python e2e/verify_e2e.py --classifier-url http://localhost:8000

# Produce test data to Confluent Cloud
source flink-scanner/scan.env
python e2e/produce_test_data.py \
  --bootstrap  $KAFKA_BOOTSTRAP \
  --api-key    $KAFKA_KEY \
  --api-secret $KAFKA_SECRET \
  --count      200

# Start the Flink scanner (statements A, B, C)
bash flink-scanner/scripts/start_scan.sh

# Trigger a manual scan (fire twice ~60s apart to advance watermark)
bash flink-scanner/scripts/start_scan.sh --now
# ... wait 60s ...
bash flink-scanner/scripts/start_scan.sh --now

# Apply tags to Schema Registry
python flink-scanner/apply_tags.py \
  --topic     raw-messages \
  --sr-url    $SR_URL --sr-key $SR_KEY --sr-secret $SR_SECRET \
  --bootstrap $KAFKA_BOOTSTRAP --kafka-key $KAFKA_KEY --kafka-secret $KAFKA_SECRET \
  --yes

# Run unit + integration tests
pytest tests/ -q
```

---

## Schema Registry as the source of truth

Every code path that reads or writes a schema fetches it from Confluent
Schema Registry. There is no local schema literal that can drift:

| Step | Source of truth |
|---|---|
| Wizard demo schema generation | Built dynamically from `setup_wizard.field_pool` (vocabulary only — not schema), POSTed to SR, then re-fetched by id BEFORE the producer encodes |
| Producer (wizard + e2e/produce_test_data.py) | `GET /schemas/ids/{schema_id}` — encodes with the SR-canonical schema |
| Local scanner decode (e2e/local_scanner.py) | Reads schema_id from each Confluent wire-format message, fetches from SR, caches per id |
| Flink scan-driver `JSON_OBJECT(...)` field list | `flink-scanner/scripts/generate_json_object.py` queries SR for the latest subject schema and rewrites the SQL placeholder at start_scan time |
| Stream Catalog field validation | `review-api/sr_schema.py` fetches the schema body and checks every staged field's existence before any tag is POSTed |
| Tag-application path | Atlas's `qualifiedName` for `sr_field` entities is built as `{cluster_id}:.:{schema_id}:{namespace}.{record_name}.{field_path}` — schema_id and `namespace.record_name` come from the SR fetch, never from local state |

The wizard's "Run test demo" button performs a full clean-slate reset before
each run: DELETE recommendations for the topic in review-api → DELETE the SR
subject (soft + permanent) → DELETE the Kafka topic → CREATE the topic →
build a fresh dynamic schema → register to SR. Old runs leave nothing behind.

### Stale-row protection

`apply_tags_batch` in `review-api/catalog_client.py` short-circuits any
recommendation whose `field_path` no longer exists in the live schema and
auto-rejects it with `reviewed_by="auto: field not in current SR schema"`.
The review UI calls `GET /recommendations/validate-staged` on render and
flags stale staged rows in red BEFORE the user clicks Submit.

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
| `MAX_CONCURRENT` | `10` | Concurrent classification requests (use `3` on Mac with Layer 3) |
| `CLASSIFIER_TIMEOUT_S` | `5.0` | Per-request timeout in seconds (use `15.0` on Mac with Layer 3) |
| `SCAN_INTERVAL_MINUTES` | `60` | Flink scanner: how often to scan (minutes between scans) |
| `SAMPLE_WINDOW_MINUTES` | `2` | Flink scanner: how many minutes of data to sample per scan |

Note: `SCAN_INTERVAL_MINUTES` and `SAMPLE_WINDOW_MINUTES` are set in `flink-scanner/scan.env`, not in `.env`.

---

## Docs

- [Architecture](docs/ARCHITECTURE.md) — component design, data flows, sequence diagrams
- [Taxonomy](docs/TAXONOMY.md) — all 11 tags, entity type mappings, confidence scoring
- [Testing](docs/TESTING.md) — unit tests, integration tests, end-to-end test guide
- [Tuning](docs/TUNING.md) — performance, confidence thresholds, layer configuration
- [Learnings](docs/LEARNINGS.md) — running log of non-obvious gotchas (Atlas qualifiedName, Flink interval-join watermarks, CC describe race, etc.)
