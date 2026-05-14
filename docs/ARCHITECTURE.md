# Architecture

## Overview

The auto data classifier has two operating modes that share the same classification engine:

1. **Streaming pipeline** — continuously classifies every message on a Kafka topic
2. **Flink SQL scanner** — scheduled + event-driven topic profiling with human approval via apply_tags.py

Both modes use the same three-layer classifier. The Flink scanner writes tags directly to the Schema Registry schema definition.

---

## Component map

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Confluent Cloud                                  │
│                                                                         │
│  Kafka Topics                        Schema Registry                    │
│  ┌──────────────────────┐            ┌────────────────────────────────┐ │
│  │ {topic}              │            │ {topic}-value  v2              │ │
│  │ {topic}-scan-triggers│            │   customer.email               │ │
│  │ {topic}-scan-results │            │     "confluent:tags": ["PII"]  │ │
│  │ raw-messages         │            │   card.number                  │ │
│  │ classified-msgs      │            │     "confluent:tags": ["PCI"]  │ │
│  │ classified-safe      │            └────────────────────────────────┘ │
│  │ classification-audit │                         ▲                     │
│  └──────────────────────┘                         │ register new version│
│          │                                        │                     │
│          │                  Flink SQL (3 statements)                    │
│          │                  ┌──────────────────────────────────────┐    │
│          │                  │  A: TUMBLE → scan-triggers           │    │
│          ├─────────────────▶│  B: schema_watcher() → scan-triggers │    │
│          │                  │  C: scan driver → scan-results        │    │
│          │                  │     (classify_fields() UDTF)          │    │
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
│                                   └──────────────────────────────────┘  │
│                                                                         │
│  apply_tags.py  (Flink scanner review step)                             │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  reads {topic}-scan-results  → latest batch                       │  │
│  │  fetches schema from SR      → extracts existing tags             │  │
│  │  presents new/changed tags   → [y/n/q] per field                  │  │
│  │  patches schema (AVRO/JSON Schema/Protobuf)                       │  │
│  │  registers new schema version in SR                               │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  classifier-service (:8000)                                             │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  POST /classify  [same as before]                                 │  │
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

### Three triggers, one results topic

All three trigger mechanisms write to `{topic}-scan-triggers`. The scan driver (Statement C) reacts to every trigger regardless of source.

```
Trigger 1 — Scheduled (Statement A)
  TUMBLE(source_topic, INTERVAL 'N' MINUTES)
  → on each window close: INSERT INTO scan-triggers {trigger_type: 'scheduled'}

Trigger 2 — Schema evolution (Statement B)
  For each message in source_topic:
    schema_watcher(sr_url, sr_key, sr_secret, subject)
    ├─ rate-limited: checks SR at most once per minute
    ├─ on first check: records current version, no emit
    └─ on version increase: INSERT INTO scan-triggers {trigger_type: 'schema_evolution'}

Trigger 3 — Manual (start_scan.sh --now)
  One-shot Flink statement:
    INSERT INTO scan-triggers VALUES ('manual', topic, CURRENT_TIMESTAMP)

Statement C — Scan driver (always running)
  scan-triggers AS t
  JOIN source_topic AS p
    ON p.$rowtime BETWEEN t.triggered_at - INTERVAL 'M' MINUTES AND t.triggered_at
  → classify_fields(url, max_layer, $value) UDTF
  → INSERT INTO scan-results (field_path, tag, confidence, layer, source, example, trigger_type, scanned_at)
```

### apply_tags.py — human review and schema patching

```
apply_tags.py
  │
  ├─ 1. Read {topic}-scan-results → latest scanned_at batch, deduplicated
  │
  ├─ 2. Fetch schema from SR → detect schema type (AVRO / JSON Schema / Protobuf)
  │       Extract existing (field_path, tag) pairs already in schema
  │
  ├─ 3. Filter
  │       Already in schema → silently skipped
  │       New or changed   → shown for approval
  │
  ├─ 4. Interactive prompt (new tags only)
  │       [1/N] Field: customer.email  Tag: PII  Confidence: HIGH (0.97)
  │       Approve? [y/n/q]
  │
  └─ 5. One schema update
          Fetch schema once
          Patch all approved fields:
            AVRO:        "confluent:tags": ["PII"]  on field object
            JSON Schema: "confluent:tags": ["PII"]  on property
            Protobuf:    [(confluent.field_meta) = {tags: ["PII"]}]  on field
          Register new version → SR v{N+1}
```

### SchemaWatcherUDF — how it works

`SchemaWatcherUDF` is a Flink UDTF that acts as an event source for schema changes:

- Called once per incoming message on the source topic (the topic acts as a heartbeat)
- Internally rate-limited: only calls `GET /subjects/{subject}/versions/latest` at most once per minute regardless of message volume
- Maintains `lastKnownVersion` as a transient instance variable (persists for the job lifetime; resets on restart)
- On first successful check: records version, does **not** emit (no baseline to compare)
- On version increase: emits one row `(triggered_at)` → written to scan-triggers topic

### Configuration

Both scheduling parameters live in `flink-scanner/scan.env`:

```
SCAN_INTERVAL_MINUTES=60    # TUMBLE window size — how often Statement A fires
SAMPLE_WINDOW_MINUTES=2     # interval join lookback — how much data Statement C classifies
```

These are independent. Change either in `scan.env` and restart with `start_scan.sh`.

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

## Schema Registry — tag application

Tags are embedded directly in the schema definition, not as Stream Catalog metadata. This makes tags portable — any consumer or governance tool that reads the schema sees them.

**AVRO** — `confluent:tags` field property:
```json
{
  "name": "email",
  "type": "string",
  "confluent:tags": ["PII"]
}
```

**JSON Schema** — `confluent:tags` vendor extension on property:
```json
{
  "properties": {
    "email": { "type": "string", "confluent:tags": ["PII"] }
  }
}
```

**Protobuf** — `confluent.field_meta` option:
```proto
import "confluent/meta.proto";
string email = 1 [(confluent.field_meta) = {tags: ["PII"]}];
```

`apply_tags.py` detects the schema type automatically, patches all approved fields in one pass, and registers a single new version. Tags already present in the schema are never re-prompted.

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

---

## Future considerations

### Layer 1.5 — Structural fingerprinting (no plaintext access)

A potential intermediate layer between Layer 1 (field name) and Layer 2 (Presidio regex) that detects sensitive data based on the **shape and statistics** of values rather than reading their content:

**Structural pattern fingerprinting**
- Analyse character-class structure of values (`3-2-4` digit groups → likely SSN/phone, `*@*.*` → likely email, 16 digits in groups of 4 → likely credit card) without reading plaintext
- Value length distribution, entropy, and character-set cardinality — high entropy + fixed length suggests a token or credential; all-numeric 9-char values suggest SSN or zip

**MinHash / reference PII hashing (customer opt-in)**
- Customer pre-hashes a reference set of known PII (employee emails, SSNs, etc.)
- At classification time incoming values are hashed and compared — zero plaintext exposure, high precision for known data
- Useful for detecting data leakage ("is our HR dataset appearing in this Kafka topic?")
- Limitation: only detects data in the reference set; novel PII (a new person's email) is missed

**Where it would sit**

```
Layer 1   — field name only          (zero data access, <1ms)
Layer 1.5 — structural fingerprint   (shape/entropy/minhash, no plaintext)
Layer 2   — Presidio regex           (reads plaintext values, 5–20ms)
Layer 3   — spaCy + GLiNER AI        (reads plaintext, NLP, 50–500ms)
```

The structural fingerprinting variant overlaps with what Presidio already does in Layer 2 but could be run in environments where plaintext data must not leave the customer's perimeter. The reference hashing variant is a premium, customer opt-in feature that would require a secure key-management workflow for the hash seeds.
