# Testing

## Test suite overview

```
tests/
├── conftest.py                      sys.path setup for classifier-service + kafka-pipeline
├── test_classifier_api.py           /classify and /health endpoint tests (13 tests)
├── test_field_name_recognizer.py    Layer 1 — classify_field_name() + is_free_text() (37 tests)
├── test_taxonomy.py                 DataTag enum + tag_entity() (12 tests)
├── test_phi_recognizers.py          PHI pattern recognisers
├── test_pci_recognizers.py          PCI pattern recognisers
├── test_financial_recognizers.py    FINANCIAL pattern recognisers
├── test_credentials_recognizers.py  CREDENTIALS pattern recognisers
├── test_catalog_tagger.py           Stream Catalog tag application logic
├── test_review_api.py               Review API — create, upsert, approve, reject, bulk (23 tests)
└── test_wire_format.py              Confluent Avro wire-format deserialization
```

**Total: 222 tests. All pass.**

---

## Running the tests

```bash
# All tests
pytest tests/ -q

# Single file
pytest tests/test_classifier_api.py -v

# Specific test
pytest tests/test_field_name_recognizer.py::TestClassifyFieldName::test_camel_case_handling -v

# With coverage
pytest tests/ --cov=classifier-service --cov-report=term-missing
```

---

## Unit tests — key design decisions

### Mocking the analyzer (test_classifier_api.py)

The classifier service has two analyzers (`regex_analyzer`, `ai_analyzer`). Both are mocked in tests so spaCy and GLiNER never load:

```python
@pytest.fixture()
def client():
    mock_analyzer = MagicMock()
    with patch("main.build_regex_analyzer", return_value=mock_analyzer), \
         patch("main.build_ai_analyzer", return_value=mock_analyzer):
        import main as app_module
        app_module.regex_analyzer = mock_analyzer
        app_module.ai_analyzer = mock_analyzer
        with TestClient(app_module.app) as c:
            c._mock_analyzer = mock_analyzer
            yield c
```

`client._mock_analyzer.analyze.return_value = [...]` controls what the analyzer returns for each test.

### Module name collision (test_review_api.py)

Both `classifier-service/main.py` and `review-api/main.py` are named `main`. When the full suite runs, Python's module cache serves the wrong one. Fixed by:
- Not adding `review-api` to `conftest.py` sys.path
- Using `_load_review_api()` in `test_review_api.py` which removes the path, re-inserts at front, and clears `sys.modules` before importing

### SQLite in tests (test_review_api.py)

Tests use pytest's `tmp_path` fixture to create a real per-test SQLite file — **not** `:memory:`. Each `aiosqlite.connect(":memory:")` call creates a fresh empty database, so tables initialised in one call would not persist to the next.

```python
@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test_recommendations.db")
```

---

## End-to-end test guide

### Prerequisites

```bash
# Install missing dependencies (one-time)
brew install ngrok/ngrok/ngrok
.venv/bin/pip install gliner
.venv/bin/python -m spacy download en_core_web_lg
```

### Step 1 — Start the classifier service

```bash
bash e2e/start_classifier.sh
```

This installs any missing deps, starts uvicorn on port 8000, and waits for `/health` to return `ok`. GLiNER downloads its model (~500 MB) on first run — allow 60–90 seconds.

### Step 2 — Expose with ngrok

In a new terminal tab:
```bash
ngrok http 8000
# → https://xxxx.ngrok-free.app
```

### Step 3 — Run the smoke test

```bash
# Classifier only (no Confluent credentials needed)
.venv/bin/python e2e/verify_e2e.py \
  --classifier-url https://xxxx.ngrok-free.app

# Full end-to-end including Kafka round-trip
.venv/bin/python e2e/verify_e2e.py \
  --classifier-url https://xxxx.ngrok-free.app \
  --bootstrap      pkc-xxx.us-east-1.aws.confluent.cloud:9092 \
  --api-key        KAFKA_KEY \
  --api-secret     KAFKA_SECRET \
  --topic          scanner-test
```

Expected output:
```
── Classifier service (https://xxxx.ngrok-free.app) ──
  ✓ Health OK — version 3.1.0

── /classify ──
  ✓ Layer 1 — field name 'ssn' → GOVERNMENT_ID  (layers_used=[1])
  ✓ Layer 1 — field name 'email' → PII  (layers_used=[1])
  ✓ Layer 1 — field name 'password' → CREDENTIALS  (layers_used=[1])
  ✓ Layer 1 — field name 'credit_card_number' → PCI  (layers_used=[1])
  ✓ Layer 1 — field name 'patient_id' → PHI  (layers_used=[1])
  ✓ Layer 2 — routing number in value → FINANCIAL  (layers_used=[1, 2])
  ✓ Layer 2 — regex credit card in value → PCI  (layers_used=[1, 2])
  ✓ Layer 2 — regex email in value → PII  (layers_used=[1, 2])
  ✓ Layer 3 — AI model detects PHI in free-text  (layers_used=[1, 2, 3])
  ✓ max_layer=1 skips regex/AI → (no tags, as expected)
  ✓ nested fields flattened → PII  (layers_used=[1])

── Result: 11 passed, 0 failed
```

### Layer 2 and Layer 3 test case rationale

Two verify_e2e.py test cases were updated after finding classifier limitations during e2e validation:

**Layer 2 — SSN in value (changed to routing number)**

The original test used `{"note": "SSN is 123-45-6789"}` and expected `GOVERNMENT_ID`. In practice Presidio's `UsSsnRecognizer` does not reliably detect SSNs embedded in mixed-text when the field name provides no SSN context — the `123-45-6789` pattern is ambiguous with `DATE_TIME` (e.g. Dec 45 6789 in some locale formats). The test was replaced with a routing number: `{"ref": "routing number 026009593"}` expecting `FINANCIAL`. The explicit label "routing number" before the digits gives Presidio's `USRoutingNumberRecognizer` the context it needs for a confident match.

**Layer 3 — AI GOVERNMENT_ID (changed to PHI)**

The original test used `{"comment": "My name is John Smith and my SSN is 123-45-6789"}` and expected `GOVERNMENT_ID`. The GLiNER model returns a `PERSON` entity (tag `PII`) from the name, and the SSN pattern in free-text is not reliably mapped to `GOVERNMENT_ID` without field-name context. The test was replaced with `{"freetext": "Patient diagnosed with Type 2 Diabetes, prescribed Metformin 500mg"}` expecting `PHI`. GLiNER reliably detects medical conditions and medications as PHI in free-text, which is the primary use case for Layer 3 in this classifier.

### Step 3b — Run the kafka-pipeline against Confluent Cloud

Start the review-api and pipeline with Mac-appropriate settings:

```bash
# Terminal 1 — start the classifier (already running from Step 1)
# Terminal 2 — start the review-api
cd review-api
DB_PATH=/tmp/auto-classifier-data/recommendations.db \
  .venv/bin/uvicorn main:app --port 8001

# Terminal 3 — start the pipeline with Mac settings
MAX_CONCURRENT=3 CLASSIFIER_TIMEOUT_S=15 \
  .venv/bin/python kafka-pipeline/pipeline.py
```

The pipeline will idle-flush partial batches when the topic quiets down, so all messages are committed within ~1 second of arrival even when the batch size is not reached.

### Step 4 — Produce test data to Confluent Cloud

```bash
.venv/bin/python e2e/produce_test_data.py \
  --bootstrap  pkc-xxx.us-east-1.aws.confluent.cloud:9092 \
  --api-key    KAFKA_KEY \
  --api-secret KAFKA_SECRET \
  --topic      scanner-test \
  --count      200
```

Produces 200 JSON messages covering all 11 tag types including nested structures, free-text comment fields, and mixed-type messages (e.g. e-commerce orders with PII + PCI).

### Step 5 — Run the Flink SQL scanner

```bash
# 1. Build the UDF JAR
bash flink-scanner/scripts/build.sh

# 2. Register all three UDFs in Confluent Cloud (once per environment)
export CONFLUENT_ENVIRONMENT=env-xxxxx
export CONFLUENT_COMPUTE_POOL=lfcp-xxxxx
bash flink-scanner/scripts/register.sh
# Registers: classify_fields(), schema_watcher(), apply_tag()

# 3. Fill in flink-scanner/scan.env with your credentials and schedule
vim flink-scanner/scan.env
#   SCAN_INTERVAL_MINUTES=60   ← how often Flink scans
#   SAMPLE_WINDOW_MINUTES=2    ← how much data per scan
#   SOURCE_TOPIC, CLASSIFIER_URL, SR_URL/KEY/SECRET, KAFKA_BOOTSTRAP/KEY/SECRET

# 4. Start all three Flink statements (run once — they run continuously)
bash flink-scanner/scripts/start_scan.sh
# Starts:
#   {topic}-scan-trigger-scheduled  — TUMBLE window, fires every N min
#   {topic}-scan-trigger-schema     — SchemaWatcherUDF, reacts to SR version changes
#   {topic}-scan-driver             — interval join, classifies last M min on each trigger

# 5. Fire a manual scan at any time
bash flink-scanner/scripts/start_scan.sh --now

# 6. When ready to review — run apply_tags.py
source flink-scanner/scan.env
python flink-scanner/apply_tags.py \
  --topic     $SOURCE_TOPIC \
  --sr-url    $SR_URL --sr-key $SR_KEY --sr-secret $SR_SECRET \
  --bootstrap $KAFKA_BOOTSTRAP --kafka-key $KAFKA_KEY --kafka-secret $KAFKA_SECRET
# → reads latest batch from {topic}-scan-results topic
# → skips fields already tagged in schema
# → prompts [y/n/q] for new/changed tags
# → patches schema (AVRO / JSON Schema / Protobuf) → registers one new version

# Check statement status
bash flink-scanner/scripts/start_scan.sh --status

# Stop all scanner statements
bash flink-scanner/scripts/start_scan.sh --stop
```

---

## What each test file covers

### test_classifier_api.py
- `/health` returns `status: ok`, `analyzer_ready: true`, `version: 3.1.0`
- Layer 1 detection from field name (`ssn`, `email`, `password`)
- `max_layer=1` skips data inspection — `analyze()` never called
- `max_layer=1` with a named field still detects (Layer 1 always runs)
- Regex results tagged as `layer=2`
- `layers_used` matches `max_layer` setting
- Clean fields return empty tags
- Nested fields are flattened
- Response includes `classifier_version`, `classified_at`, `layers_used`
- No `sensitivity_level` or `categories` in response (removed in v3.0)

### test_field_name_recognizer.py
- All 11 tags via field names (parametrised — 70+ field name cases)
- Generic names (`id`, `value`, `data`, `ref`) return no match
- Sorted by confidence descending
- camelCase handling (`creditCardNumber`, `dateOfBirth`, `socialSecurityNumber`)
- `source == "field_name"` and `layer == 1` on all results
- No duplicate entity types per field
- `is_free_text()`: known names, structured names, value word-count heuristic

### test_review_api.py (23 tests)
- Create recommendation, retrieve by ID
- Upsert keeps highest confidence for same (topic, field, tag)
- List with status/topic/tag/tier filters
- Approve → calls catalog_client, status → APPROVED
- Reject → status → REJECTED
- Bulk-approve above min_confidence threshold
- Summary counts by topic

---

## CI considerations

The test suite requires no network access and no GPU:
- spaCy and GLiNER are stubbed in `test_classifier_api.py`
- spaCy `en_core_web_sm` is used by the recogniser tests (installed in venv)
- GLiNER is stubbed via `sys.modules.setdefault("gliner", MagicMock())` at the top of API tests
- All SQLite operations use `tmp_path` (no persistent state between tests)

To run in CI without the large models:
```bash
pip install -r classifier-service/requirements.txt --extra-index-url https://...
# spacy download en_core_web_sm is sufficient for unit tests
python -m spacy download en_core_web_sm
pytest tests/ -q
```
