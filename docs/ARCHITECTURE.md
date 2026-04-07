# Architecture

## Overview

The auto data classifier has two operating modes that share the same classification engine:

1. **Streaming pipeline** — continuously classifies every message on a Kafka topic
2. **Flink SQL scanner** — on-demand interactive profiling with human approval in the Flink workspace

Both modes use the same three-layer classifier and write tags to the same Confluent Stream Catalog.

---

## Component map

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Confluent Cloud                                  │
│                                                                         │
│  Kafka Topics                        Stream Catalog (Schema Registry)   │
│  ┌──────────────────┐                ┌────────────────────────────────┐ │
│  │ raw-messages     │                │ payments-value                 │ │
│  │ classified-msgs  │                │   customer.email  → PII        │ │
│  │ classified-safe  │                │   card.number     → PCI        │ │
│  │ classification-  │                │   patient_id      → PHI        │ │
│  │   audit          │                └────────────────────────────────┘ │
│  └──────────────────┘                                                   │
│          │                  Flink SQL Workspace                         │
│          │                  ┌──────────────────────────────────────┐    │
│          │                  │  classify_fields() UDTF              │    │
│          │                  │  apply_tag() scalar UDF              │    │
│          │                  │  sql/scan.sql template               │    │
│          │                  └──────────────────────────────────────┘    │
└──────────┼──────────────────────────────────────────────────────────────┘
           │
           ▼ (your compute — local or cloud VM)
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  kafka-pipeline                   review-api (:8001)                   │
│  ┌──────────────────────────┐     ┌──────────────────────────────────┐  │
│  │ consumer.poll()          │     │ POST /recommendations (upsert)   │  │
│  │ deserialize (Avro/JSON)  │────▶│ GET  /recommendations            │  │
│  │ POST /classify           │     │ POST /recommendations/bulk-      │  │
│  │ route_message()          │     │       approve                    │  │
│  │ post_recommendations()   │     │ POST /recommendations/{id}/      │  │
│  └──────────────────────────┘     │       approve | reject           │  │
│                                   │ → catalog_client.apply_tag()     │  │
│                                   └──────────────────────────────────┘  │
│                                                                         │
│  classifier-service (:8000)                                             │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  POST /classify                                                   │  │
│  │                                                                   │  │
│  │  Layer 1 ── field_name_recognizer.py                             │  │
│  │             Consecutive token matching on field name              │  │
│  │             Zero data access — instant, deterministic             │  │
│  │                                                                   │  │
│  │  Layer 2 ── Presidio AnalyzerEngine (regex only)                 │  │
│  │             build_regex_analyzer() — no NLP engine               │  │
│  │             Built-in + custom: PHI, PCI, FINANCIAL, CREDENTIALS  │  │
│  │                                                                   │  │
│  │  Layer 3 ── Presidio AnalyzerEngine (spaCy + GLiNER)             │  │
│  │             build_ai_analyzer() — en_core_web_lg + GLiNER        │  │
│  │             Handles free-text: comment, notes, description …     │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Streaming pipeline — detailed flow

### Message lifecycle

```
1. Consumer polls raw-messages topic (Confluent Cloud)
2. Deserialize:
     Confluent wire format → AvroDeserializer → dict
     Plain bytes           → json.loads()      → dict
3. Flatten nested fields (recursive)
     {"customer": {"email": "..."}}  →  {"customer.email": "..."}
4. POST /classify with {fields, max_layer}
5. Response contains tags[] and detected_entities{field_path: [entity…]}
6. route_message():
     tags non-empty → classified-messages topic (enriched payload)
     tags empty     → classified-safe topic
     always         → classification-audit topic
7. post_recommendations():
     For each (field, tag) pair → POST /recommendations
     Upsert: keeps highest-confidence entity for same (topic, field, tag)
8. consumer.commit() — manual offset commit after batch
```

### Idle-flush — partial batch handling

The pipeline accumulates asyncio tasks up to `BATCH_SIZE` (default 50) before committing. If the topic goes quiet before a full batch is ready, `consumer.poll()` returns `None`. In that case the pipeline immediately gathers all pending tasks, flushes the producer, and commits the consumer offset — preventing messages from stalling indefinitely in a low-throughput window:

```python
if msg is None:
    # Flush partial batch when idle — avoids stalling on < BATCH_SIZE messages
    if pending:
        await asyncio.gather(*pending)
        pending.clear()
        producer.flush()
        consumer.commit(asynchronous=False)
    continue
```

This means end-to-end latency for a small burst of messages is bounded by `consumer.poll(timeout=1.0)` — at most ~1 second — rather than waiting for 49 more messages to arrive.

### Classification response shape

```json
{
  "tags": ["PII", "PCI"],
  "detected_entities": {
    "customer.email": [
      {
        "entity_type": "EMAIL_ADDRESS",
        "tag": "PII",
        "score": 0.97,
        "start": 0, "end": 17,
        "text_snippet": "alice@example.com",
        "layer": 1,
        "source": "field_name"
      }
    ],
    "card.number": [
      {
        "entity_type": "CREDIT_CARD",
        "tag": "PCI",
        "score": 0.99,
        "layer": 2,
        "source": "regex"
      }
    ]
  },
  "layers_used": [1, 2, 3],
  "classified_at": "2026-04-06T10:00:00+00:00",
  "classifier_version": "3.1.0"
}
```

---

## Flink SQL scanner — detailed flow

```
Operator opens Confluent Cloud Flink SQL workspace
        │
        ▼
Statement A — sample + classify
┌────────────────────────────────────────────────────────────────┐
│ SELECT field_path, tag, MAX(score), MIN(layer), ANY_VALUE(...) │
│ FROM `payments`,                                               │
│      LATERAL TABLE(classify_fields(url, max_layer, $value))    │
│ WHERE $rowtime >= CURRENT_TIMESTAMP - INTERVAL '2' MINUTES     │
│ GROUP BY field_path, tag                                       │
│ ORDER BY confidence DESC                                       │
└────────────────────────────────────────────────────────────────┘
        │
        ▼  (results accumulate in Flink results pane)

  field_path          tag       confidence  layer  source
  ──────────────────────────────────────────────────────
  customer.email      PII       0.97        1      field_name
  card.number         PCI       0.99        2      regex
  notes               PII       0.74        3      ai_model

        │  (operator stops query after N minutes, reviews table)
        ▼
Statement B — approve + apply
┌────────────────────────────────────────────────────────────────┐
│ SELECT apply_tag(sr_url, key, secret, cluster_id,             │
│                  subject, field_path, tag) AS result           │
│ FROM (VALUES                                                   │
│   ('payments-value', 'customer.email', 'PII'),                │
│   ('payments-value', 'card.number',    'PCI')                  │
│ ) AS approvals(subject, field_path, tag)                      │
└────────────────────────────────────────────────────────────────┘
        │
        ▼  apply_tag() calls:
           1. GET  {SR_URL}/subjects/{subject}/versions/latest  → version
           2. POST {SR_URL}/catalog/v1/entity/tags              → tag applied

        Result column shows: "OK: PII → payments-value.customer.email"
```

The two UDFs (`ClassifyFieldsUDF`, `ApplyTagUDF`) are packaged as a single fat JAR and registered once per Flink environment.

---

## Review API — recommendation lifecycle

```
State machine per (topic, field_path, proposed_tag):

  PENDING ──── approve ──▶ APPROVED ──▶ tag applied in Stream Catalog
     │
     └───────── reject ──▶ REJECTED

Upsert rule: if a new recommendation arrives for an existing (topic, field, tag)
with a higher confidence score, the existing record is updated and status
reset to PENDING.

Confidence tiers (for display in the review UI):
  HIGH   ≥ 0.85  — safe to bulk-approve
  MEDIUM  0.60–0.84  — review individually
  LOW    < 0.60  — inspect the example snippet before approving
```

### Key endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/recommendations` | Upsert from pipeline (idempotent) |
| `GET` | `/recommendations` | List all, filter by status/topic/tag/tier |
| `GET` | `/recommendations/summary` | Counts by topic |
| `POST` | `/recommendations/bulk-approve` | Approve all ≥ min_confidence (default 0.85) |
| `POST` | `/recommendations/{id}/approve` | Approve one → apply tag |
| `POST` | `/recommendations/{id}/reject` | Reject one |

---

## Three-layer engine — decision logic

```python
for field_path, value in flat_fields.items():
    leaf = _leaf_name(field_path)           # "customer.email" → "email"
    free_text = is_free_text(leaf, value)   # known name OR word count ≥ 6

    # Layer 1 — always runs
    for match in classify_field_name(leaf):
        emit(layer=1, source="field_name", ...)

    # Layer 2 — runs if max_layer >= 2
    if max_layer >= 2:
        for r in regex_analyzer.analyze(value):
            emit(layer=2, source="regex", ...)

    # Layer 3 — runs if max_layer >= 3
    if max_layer >= 3:
        for r in ai_analyzer.analyze(value):
            emit(layer=3, source="ai_model", ...)
```

All enabled layers always run. There is no early-exit on a match — a field can have results from all three layers simultaneously. The review API and Flink scanner deduplicate by keeping the highest-confidence entity per `(field, tag)` pair.

### Free-text field detection

Fields named `comment`, `notes`, `description`, `message`, `feedback`, etc. are flagged as free-text regardless of value. Fields with a generic name but a value containing ≥ 6 words are also flagged. Free-text fields are always sent through Layer 3 (AI) when `max_layer >= 3`.

### Field name matching algorithm

Layer 1 uses **consecutive token subsequence matching**:
- The field name is tokenized: `creditCardNumber` → `["credit", "card", "number"]`
- Each keyword is also tokenized: `credit_card` → `["credit", "card"]`
- Match if keyword tokens appear as a contiguous run in field tokens
- This prevents `id` from matching `patient_id` while allowing `credit_card_number` to match `credit_card`

---

## Stream Catalog — tag application

Tags are applied to Schema Registry field entities. The Confluent Stream Catalog qualified name format is:

```
{sr_cluster_id}:.:{subject}.v{version}.{field_path}

Example:
lsrc-abc123:.:.payments-value.v3.customer.email
```

Tag definitions are bootstrapped once on first run (idempotent `POST /catalog/v1/types/tagdefs`). Tag application is idempotent — HTTP 409 (already tagged) is treated as success.

---

## Confluent Cloud test environment

The stack was validated end-to-end against the following Confluent Cloud resources:

| Resource | Value |
|---|---|
| Environment | DEVTEST |
| Cluster name | claude-test-cl |
| Cluster ID | lkc-2pk6ro |
| Bootstrap servers | pkc-921jm.us-east-2.aws.confluent.cloud:9092 |
| Schema Registry ID | lsrc-jwp0w |
| Schema Registry URL | psrc-lq3wm.eu-central-1.aws.confluent.cloud |
| Topics | raw-messages → classified-messages / classified-safe / classification-audit |

80 messages were produced, 0 errors, 89 tag recommendations generated in the review-api.

### Recommended runtime settings for Mac (local development)

Running with full Layer 3 (GLiNER) on a Mac requires lower concurrency to avoid inference timeouts:

```bash
MAX_CONCURRENT=3         # GLiNER is compute-heavy; 10 concurrent will timeout on Mac
CLASSIFIER_TIMEOUT_S=15  # GLiNER inference takes 2–8s per message on Apple silicon/Intel
```

See [TUNING.md](TUNING.md) for a full explanation of these settings.

---

## Sequence diagram — streaming pipeline

```
kafka-pipeline          classifier-service      review-api          Stream Catalog
      │                        │                    │                     │
      │─── poll() ────────────▶│                    │                     │
      │◀── message ────────────│                    │                     │
      │                        │                    │                     │
      │─── POST /classify ────▶│                    │                     │
      │                        │── Layer 1 ─────────│                     │
      │                        │── Layer 2 ─────────│                     │
      │                        │── Layer 3 ─────────│                     │
      │◀── classification ─────│                    │                     │
      │                        │                    │                     │
      │── produce to topic ───▶│ (classified-msgs)  │                     │
      │── produce to topic ───▶│ (audit)            │                     │
      │                        │                    │                     │
      │─── POST /recommendations ────────────────▶ │                     │
      │◀── 200 OK ──────────────────────────────── │                     │
      │                        │                    │                     │
      │── commit offset ───────│                    │                     │
      │                        │     (human reviews /recommendations)     │
      │                        │                    │── approve ─────────▶│
      │                        │                    │◀── 200 OK ──────────│
```
