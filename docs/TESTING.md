# Testing Guide

The test suite is designed to run with **zero external dependencies** — no Confluent Cloud, no GLiNER model download, no GPU.

## Test structure

```
tests/
├── conftest.py                      # sys.path setup for classifier-service and kafka-pipeline
├── test_taxonomy.py                 # DataCategory / SensitivityLevel logic (pure Python)
├── test_sensitivity_logic.py        # End-to-end entity type → sensitivity level
├── test_pci_recognizers.py          # IBAN, SWIFT, routing, crypto wallet regex
├── test_phi_recognizers.py          # NPI, DEA number, health insurance regex
├── test_credentials_recognizers.py  # AWS keys, JWTs, connection strings
├── test_wire_format.py              # Confluent wire-format schema ID extraction
├── test_catalog_tagger.py           # Stream Catalog tagging (mocked HTTP)
└── test_classifier_api.py           # /classify and /health endpoints (mocked analyzer)
```

**130 tests, 0 external calls.**

## Running the tests

```bash
# One-time setup
python3 -m venv .venv
.venv/bin/pip install pytest pytest-asyncio httpx fastapi presidio-analyzer pydantic

# Run all tests
.venv/bin/pytest tests/ -v

# Run a specific module
.venv/bin/pytest tests/test_taxonomy.py -v

# Run with short output
.venv/bin/pytest tests/ -q
```

## Test strategy by module

### `test_taxonomy.py` (27 tests)
- Every known entity type maps to the expected `DataCategory`
- Unknown entity types default to `INTERNAL`
- Sensitivity level precedence (CRITICAL beats HIGH beats MEDIUM …)
- All `DataCategory` values have a sensitivity mapping (coverage guard)
- All `ENTITY_CATEGORY` values are valid `DataCategory` members (type guard)

### `test_sensitivity_logic.py` (19 tests)
- Parametrised: entity type strings → expected sensitivity level string
- Covers all four levels plus CLEAN
- PHI beats PCI, CREDENTIALS beats PII

### `test_pci_recognizers.py` (13 tests)
- Parametrised valid inputs for IBAN, SWIFT, routing, crypto wallets
- Negative tests for non-matching inputs
- `get_pci_recognizers()` returns all five recognizer instances

### `test_phi_recognizers.py` (8 tests)
- NPI (10-digit), DEA number (2L+7D), health insurance ID formats
- Negative tests for short/non-matching strings

### `test_credentials_recognizers.py` (11 tests)
- AWS access key (`AKIA…`) — valid and negative
- JWT (three base64url segments)
- Connection strings: MongoDB, PostgreSQL, Redis, Kafka
- Negative: URL without embedded credentials

### `test_wire_format.py` (7 tests)
- Valid Confluent wire format (magic byte + schema ID)
- Large schema IDs
- Plain JSON → None
- Wrong magic byte → None
- Too short → None
- Empty bytes → None
- Schema ID zero (edge case)

### `test_catalog_tagger.py` (16 tests)
All HTTP calls mocked via `unittest.mock.AsyncMock`.

- `_field_qualified_name` format
- `_highest_category` priority order (PHI > CREDENTIALS > PCI > PII > …)
- `ensure_tag_definitions`: creates missing tags, skips existing, bootstrap flag
- `apply_classifications`: PHI → "PHI" tag, PII → "PII" tag, CREDENTIALS → "CREDENTIALS" tag
- PHI wins over PII on the same field
- Empty entities → no API call
- In-process cache prevents duplicate tag API calls
- HTTP 409 treated as success (idempotent)

### `test_classifier_api.py` (9 tests)
GLiNER is stubbed via `sys.modules`. Real `presidio-analyzer` and `pydantic` are used.

- `/health` returns `{"status":"ok","analyzer_ready":true,"version":"2.0.0"}`
- SSN detected → HIGH + PII category
- MEDICAL_RECORD detected → CRITICAL + PHI category
- PASSWORD detected → CRITICAL + CREDENTIALS category
- Clean message → CLEAN + empty categories
- Multiple categories returned in single message
- Nested fields flattened and each leaf analyzed
- Response always includes `classifier_version` and `classified_at`
- Numeric fields coerced to string and analyzed

## Mocking strategy

| Dependency | Strategy | Reason |
|---|---|---|
| GLiNER | `sys.modules['gliner'] = MagicMock()` | 300MB model not in test venv |
| Confluent Kafka | Not imported in test scope | catalog_tagger.py has no Kafka import |
| httpx (catalog) | `unittest.mock.AsyncMock` | Deterministic HTTP responses |
| AnalyzerEngine | `unittest.mock.MagicMock` | Isolates API endpoint logic from NLP |

## Running the end-to-end demo

The e2e demo uses real Presidio regex recognizers and a real spaCy model (`en_core_web_sm`):

```bash
# One-time: install small spaCy model
.venv/bin/pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.8.0/en_core_web_sm-3.8.0-py3-none-any.whl

# Run demo
.venv/bin/python e2e_test.py
```

GLiNER is stubbed in the demo — NER-based entities (names, addresses, diagnoses) will not appear.
All regex-based detections (IBAN, JWT, AWS key, credit card, NPI, DEA, connection strings) are real.

## Adding tests for a new recognizer

1. Create `tests/test_<name>_recognizers.py`
2. Import from `recognizers/<name>_recognizers.py`
3. Add parametrised positive and negative tests for each pattern
4. Add the recognizer's entity types to `test_taxonomy.py::TestCategorizeEntity`
5. Run `pytest tests/ -q` — all 130 existing tests must still pass

## CI integration

The test suite is designed for CI with no secrets or external services required:

```yaml
# Example GitHub Actions step
- name: Run tests
  run: |
    python -m venv .venv
    .venv/bin/pip install pytest pytest-asyncio httpx fastapi presidio-analyzer pydantic
    .venv/bin/pytest tests/ -q
```
