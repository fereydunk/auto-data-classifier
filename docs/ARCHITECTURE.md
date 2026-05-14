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
│          │                  │  A: TUMBLE → scan-triggers  RUNNING  │    │
│          ├─────────────────▶│  B: schema_watcher() → scan-triggers │    │
│          │                  │     RUNNING                          │    │
│          │                  │  C: scan driver → scan-results       │    │
│          │                  │     RUNNING (classify_fields() calls │    │
│          │                  │     classifier via USING CONNECTIONS) │    │
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
│  │  presents new/changed tags   → [y/n] per field (or --yes)        │  │
│  │  patches schema (AVRO/JSON Schema/Protobuf)                       │  │
│  │  registers new schema version in SR                               │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  classifier-service (:8000)                                             │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  POST /classify  [same for both modes]                            │  │
│  │  Exposed publicly via ngrok / Cloud Run / Fly.io for Flink access │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Flink USING CONNECTIONS — internet egress for UDFs

Confluent Cloud Flink UDFs can make outbound HTTP calls to external services via the **USING CONNECTIONS** feature. This must be explicitly enabled for your organisation by your Confluent account team.

### Requirement: org must have USING CONNECTIONS enabled

To request enablement, contact your Confluent CSE or account team and specify:
- Organisation / environment ID
- Use case: Java TableFunction (`ClassifyFieldsUDF`) calling a REST endpoint
- The endpoint URL (or ngrok / public proxy URL)

Without this feature, `classify_fields()` UDF calls silently time out — Statement C runs but emits zero rows. Statements A and B are unaffected.

### How USING CONNECTIONS works

A Flink Connection object binds a name to an external endpoint. When a UDF is registered with `USING CONNECTIONS`, Confluent Cloud opens a network route from the Flink compute pool to that endpoint:

```bash
# 1. Create the connection (once per environment)
confluent flink connection create classifier-service \
    --type rest \
    --endpoint https://your-classifier.example.com \
    --environment env-xxxxx \
    --cloud aws \
    --region us-west-2

# 2. Register classify_fields bound to the connection
#    (register.sh does this automatically)
CREATE FUNCTION classify_fields
  AS 'io.confluent.scanner.ClassifyFieldsUDF'
  USING JAR 'confluent-artifact://<artifact-id>'
  USING CONNECTIONS (`classifier-service`);
```

**Important:** USING CONNECTIONS provides network egress. The UDF still receives the classifier URL as a SQL parameter — the connection name and the URL in the SQL call must point to the same endpoint. The `FunctionContext.getJobParameter()` API does not expose connection properties to the UDF code.

To update the classifier endpoint, update both the connection and `CLASSIFIER_URL` in `scan.env`:

```bash
confluent flink connection update classifier-service \
    --endpoint https://new-url.example.com \
    --environment env-xxxxx --cloud aws --region us-west-2
# Then update CLASSIFIER_URL in flink-scanner/scan.env and restart with start_scan.sh
```

### Fallback — local_scanner.py

For organisations without USING CONNECTIONS enabled, `e2e/local_scanner.py` replicates Statement C's logic in Python, running on any machine that can reach both the Kafka cluster and the classifier:

```
local_scanner.py
  ├─ Connects to Kafka (Confluent Cloud, SASL_SSL)
  ├─ Consumes {topic} from beginning with a fresh consumer group
  ├─ Decodes Confluent Avro wire format (0x00 + 4-byte schema ID + avro bytes)
  ├─ Calls POST http://localhost:8000/classify for each message
  ├─ Groups by (field_path, tag) — keeps highest-confidence entity per pair
  └─ Publishes JSON results to {topic}-scan-results
       → apply_tags.py reads this topic identically to Flink output
```

The key invariant: all messages in a single run share **one `scanned_at` timestamp** (set before the loop). `apply_tags.py` groups by `max(scanned_at)` to find the latest batch — if timestamps differ per message, only the last second's results are processed.

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
  "classified_at": "2026-04-09T00:00:00+00:00",
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
  → classify_fields(classifier_url, max_layer, JSON_OBJECT(...all fields...)) UDTF
  → INSERT INTO scan-results (field_path, tag, confidence, layer, source, example, trigger_type, scanned_at)
```

### Scan-results table — append-only design

The `{topic}-scan-results` table is **append-only** (no PRIMARY KEY, no `changelog.mode`). Statement C emits one row per `(field_path, tag)` pair per classified message — no aggregation inside Flink.

Deduplication happens downstream in `apply_tags.py`: it groups all rows from the latest batch by `(field_path, tag)` and keeps the highest-confidence entity per pair. This avoids the watermark deadlock that a GROUP BY inside Flink streaming would cause (aggregates never flush without a time window or all-channel watermark advancement).

```sql
-- Correct: append-only, no GROUP BY in Flink
CREATE TABLE IF NOT EXISTS `{topic}-scan-results` (
    `field_path`    STRING,
    `tag`           STRING,
    `confidence`    DOUBLE,
    `layer`         INT,
    `source`        STRING,
    `example`       STRING,
    `source_topic`  STRING,
    `trigger_type`  STRING,
    `scanned_at`    TIMESTAMP_LTZ(3)
) WITH (
    'kafka.retention.time' = '604800000'   -- 7 days
);
```

### Watermark design — interval join

Statement C uses an **interval join** between scan-triggers and the source topic:

```sql
FROM `{topic}-scan-triggers` AS t
JOIN `{topic}` AS p
  ON p.`$rowtime` BETWEEN t.triggered_at - INTERVAL 'M' MINUTES
                      AND t.triggered_at
```

For this join to emit results, the watermark of the source topic must advance past `triggered_at`. This requires fresh data arriving after the trigger fires. The scheduled trigger (Statement A, TUMBLE window) continuously provides new trigger rows — the scan-triggers watermark advances with each window close, keeping the join unblocked during normal operation.

Manual triggers (one-shot INSERT) fire a single row. To unblock the join after a manual trigger, fire a **second** manual trigger ~1 minute later (or produce more messages to the source topic). The second trigger's watermark advances the trigger side, allowing the interval join to emit results for the first trigger's window.

### apply_tags.py — human review and schema patching

```
apply_tags.py
  │
  ├─ 1. Read {topic}-scan-results → latest scanned_at batch, deduplicated
  │       Groups by max(scanned_at): all results from a single scan run
  │       share one timestamp — this is the "latest batch"
  │
  ├─ 2. Fetch schema from SR → detect schema type (AVRO / JSON Schema / Protobuf)
  │       Extract existing (field_path, tag) pairs already in schema
  │
  ├─ 3. Filter
  │       Already in schema → silently skipped
  │       New or changed   → shown for approval
  │
  ├─ 4. Interactive prompt (new tags only)  [--yes skips this step]
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

**Note on result format:** Flink Statement C writes Avro-encoded results (Confluent wire format). `local_scanner.py` writes JSON. `apply_tags.py` handles both formats transparently.

### SchemaWatcherUDF — how it works

`SchemaWatcherUDF` is a Flink UDTF that acts as an event source for schema changes:

- Called once per incoming message on the source topic (the topic acts as a heartbeat)
- Internally rate-limited: only calls `GET /subjects/{subject}/versions/latest` at most once per minute regardless of message volume
- Maintains `lastKnownVersion` as a transient instance variable (persists for the job lifetime; resets on restart)
- On first successful check: records version, does **not** emit (no baseline to compare)
- On version increase: emits one row `(triggered_at)` → written to scan-triggers topic
- SchemaWatcherUDF calls Schema Registry, which is reachable from inside Confluent Cloud — no egress constraint

### Configuration

Both scheduling parameters live in `flink-scanner/scan.env`:

```
SCAN_INTERVAL_MINUTES=60    # TUMBLE window size — how often Statement A fires
SAMPLE_WINDOW_MINUTES=2     # interval join lookback — how much data Statement C classifies
CLASSIFIER_URL=https://...  # must match the endpoint of the classifier-service connection
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

### State machine — extended for the staging flow

```
PENDING ── approve ──▶ STAGED ── submit-staged ──▶ APPROVED  (catalog push)
   │           ▲                       │
   │           └────── unstage ────────┘
   │
   └── reject ──▶ REJECTED   (also reachable from STAGED)
```

`approve` no longer pushes to the catalog directly — it parks the row in
`STAGED`. The reviewer accumulates rows, then a single `submit-staged` call
pushes the whole queue to Stream Catalog in one POST so we don't create one
new schema version per tag.

### Key endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/recommendations` | Upsert from pipeline (idempotent) |
| `GET` | `/recommendations` | List all, filter by status/topic/tag/tier |
| `GET` | `/recommendations/summary` | Counts by topic |
| `GET` | `/recommendations/sources` | For each topic: subject + latest SR version + schema_id (powers the topic label in the UI) |
| `POST` | `/recommendations/{id}/approve` | PENDING → STAGED (no catalog push) |
| `POST` | `/recommendations/{id}/unstage` | STAGED → PENDING |
| `POST` | `/recommendations/{id}/reject` | PENDING or STAGED → REJECTED |
| `POST` | `/recommendations/bulk-approve` | Stage all ≥ min_confidence (default 0.85), then submit batch |
| `POST` | `/recommendations/submit-staged` | Push every STAGED to Stream Catalog in one POST; flips them all to APPROVED |
| `GET` | `/recommendations/validate-staged` | Read-only preview: which STAGED items would be auto-rejected on submit (field no longer in live SR schema) |
| `DELETE` | `/recommendations?topic=X` | Wipe every recommendation for one topic (called by the wizard before each demo run) |

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

## SR-as-source-of-truth + Stream Catalog tagging

Two tagging paths share the same architectural rule: **SR is the only source
of schema truth, and the catalog is only ever told things that we have just
verified in SR**.

### 1. apply_tags.py — schema-embedded tags

Patches the schema body itself with `confluent:tags` (Avro) or vendor
extensions (JSON Schema / Protobuf) and registers a new version. See section
above. This path doesn't touch Stream Catalog at all.

### 2. review-api → Stream Catalog — entity classifications

Tags are applied to `sr_field` entities in Confluent Stream Catalog. This is
the path used by the wizard demo flow.

```
recommendations (review-api SQLite)
        │
        │  user clicks "Submit N to Stream Catalog"
        ▼
review-api/catalog_client.py:apply_tags_batch
        │
        ├─ ensure tag definitions exist in catalog (POST /catalog/v1/types/tagdefs)
        │
        ├─ for each item:
        │   ├─ resolve schema_id → version (cached per subject+schema_id)
        │   ├─ fetch schema body from SR (cached 60s per subject+version)
        │   ├─ extract record qualifier {namespace}.{record_name} + valid field paths
        │   └─ DROP item if field_path not in valid set
        │       → review-api auto-REJECTs with reason "field not in current SR schema"
        │
        ├─ build batch payload (FLAT shape — not nested classifications):
        │   [{
        │     "entityType": "sr_field",
        │     "entityName": "{cluster_id}:.:{schema_id}:{namespace}.{record_name}.{field_path}",
        │     "typeName":   "<TagName>"
        │   }, ...]
        │
        └─ POST /catalog/v1/entity/tags  (single batch)
                │
                ▼
        Confluent Stream Catalog (Atlas)
        materializes sr_field entity if missing, applies the classification
```

### Atlas qualifiedName format — the bug we shipped to fix

The historical format we used (`{cluster}:.:{subject}.v{version}.{field}`)
is **rejected by Atlas** with `Type ENTITY with name null does not exist`.
The actual format Atlas uses for sr_field entities is:

```
{sr_cluster_id}:.:{schema_id}:{avro_namespace}.{record_name}.{field_path}
```

Both `schema_id` and `{namespace}.{record_name}` come from the SR fetch
(`sr_schema.fetch_schema_meta_cached`). They are NOT derivable from the
subject name + version alone. See `review-api/sr_schema.py:extract_record_qualifier`
and `review-api/catalog_client.py:_field_qualified_name`.

Payload shape: the `/catalog/v1/entity/tags` endpoint expects the **flat**
`[{entityType, entityName, typeName}]` shape — NOT the nested
`[{typeName: 'sr_field', attributes: {qualifiedName}, classifications: [{typeName: 'PII', attributes: {...}}]}]`
shape that some older Atlas docs describe. The flat shape is documented under
"Tags API examples" in the Confluent Stream Catalog REST APIs page.

### Pre-flight UI surface

`GET /recommendations/validate-staged` returns `{checked, valid: [], stale: [{id, subject, field_path, reason}]}`.
The review UI calls this on every render of the staged section and red-tints
stale rows (with a tooltip showing the reason) BEFORE the user clicks Submit.
Submit still works on stale items — they're auto-REJECTed server-side rather
than hitting Atlas with a dead reference.

---

## Dynamic schema generation (wizard demo)

Each "Run test demo" run picks N from the Card-5 UI input (default 20, range
5–46) and constructs a fresh Avro record schema:

```
setup_wizard.field_pool.FIELD_POOL  (46 entries: name, avro_type, generator, category)
        │
        ▼
setup_wizard.schema_builder.build_demo_schema(N)
        │   round-robin one field from each priority category
        │   (PII, GOVERNMENT_ID, PCI, FINANCIAL, CREDENTIALS,
        │    LOCATION, PHI, FREE_TEXT) until N reached
        ▼
{type: record, name: DemoRecord, namespace: io.confluent.scanner.demo, fields: [...]}
        │
        │  POST /subjects/{source_topic}-value/versions
        ▼
SR returns new schema_id
        │
        │  GET /schemas/ids/{new_id}    ← canonical schema body fetched BACK
        ▼
producer encodes test messages with the SR-canonical schema
        Flink scan-driver classifies via classify_fields UDF
        bridge POSTs detections to review-api as recommendations
```

The local schema dict that was POSTed to SR is discarded after registration —
the producer only ever sees the canonical body fetched back from SR.

### Clean-slate reset per demo run

Before building the new schema, `_create_demo_topic()` performs (where
`{topic}` is the source topic name from Card 3's text box — the wizard's
single source of truth):

1. `DELETE /recommendations?topic={topic}` — wipes review-api state
2. SR `DELETE /subjects/{topic}-value` (soft + `?permanent=true`)
3. `confluent kafka topic delete {topic} --force`
4. `confluent kafka topic create {topic} --partitions 6`

Then `_register_demo_schema()` builds + registers + re-fetches.

### Flink scan statements — race-free recreate

`flink-scanner/scripts/start_scan.sh:submit_statement` is delete-then-poll-
until-gone-then-create instead of skip-if-exists. CC's `confluent flink
statement describe` is eventually-consistent: a `--stop` followed immediately
by a check often returned the just-deleted statement as "still running",
which made the wizard re-use stale SQL with field names from a previous run.
Polling until 404 then create eliminates the race.

### Two-wave produce around manual trigger

Flink interval-join (`p.$rowtime BETWEEN t.triggered_at - INTERVAL '2' MINUTES
AND t.triggered_at`) cannot emit until the source watermark advances PAST the
trigger row's timestamp. A one-shot batch produced before the trigger leaves
the watermark stuck at the last message's rowtime, so the join blocks
indefinitely. The wizard now produces 40 messages, fires the trigger, then
produces 10 more with `time.sleep` between each — the trailing wave pushes
the watermark past the trigger time so the join emits.

### scan.sql JSON_OBJECT — generated per run

`flink-scanner/sql/scan.sql` contains a `{json_object_fields}` placeholder
inside `JSON_OBJECT(...)`. `flink-scanner/scripts/generate_json_object.py`
fetches the live SR schema for `{topic}-value` and renders the
`KEY 'name' VALUE p.\`name\`` lines. `start_scan.sh` substitutes this in
before submitting Statement C.

---

## Validated E2E results (Confluent Cloud, April 2026)

The full pipeline was validated end-to-end against a live Confluent Cloud environment with USING CONNECTIONS enabled. The validation infrastructure (DEVTEST env Kafka cluster, Flink compute pool, all credentials) was torn down on 2026-05-14 — the results below are preserved as evidence the pipeline works against real CC; re-running requires re-provisioning via the wizard.

| Resource | Value (at validation time, no longer exists) |
|---|---|
| Environment | DEVTEST (env-m2qxq) — env still exists |
| Flink compute pool | lfcp-dw3qy7 (us-west-2, AWS) — **deleted 2026-05-14** |
| Kafka cluster | lkc-1j6rd3 / pkc-pgq85 (us-west-2, AWS) — **deleted 2026-05-14** |
| Schema Registry | psrc-lq3wm (eu-central-1, AWS) — env-scoped, retained |
| Flink connection | classifier-service → ngrok tunnel → localhost:8000 (still registered, ngrok URL stale) |
| Source topic | raw-messages |
| Result topic | raw-messages-scan-results |

**Flink scan driver results — 280 messages, layer 3, via USING CONNECTIONS:**

| Tag | Detections |
|---|---|
| PII | 918 |
| PCI | 181 |
| PHI | 159 |
| LOCATION | 144 |
| FINANCIAL | 100 |
| CREDENTIALS | 91 |
| GOVERNMENT_ID | 90 |
| MINOR | 60 |
| GENETIC | 50 |
| BIOMETRIC | 50 |
| NPI | 31 |
| **Total** | **1,874** |

All 11 tags detected. Results produced natively by Flink Statement C via `classify_fields()` UDF with USING CONNECTIONS — no local_scanner.py involved.

**Flink statement status:**
- `raw-messages-scan-trigger-scheduled`: RUNNING — fires every 2 minutes (TUMBLE)
- `raw-messages-scan-trigger-schema`: RUNNING — watches SR for schema version changes
- `raw-messages-scan-driver`: RUNNING — classify_fields() calling classifier via USING CONNECTIONS

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

## Sequence diagram — Flink scanner (current path, USING CONNECTIONS)

```
Flink (Statement C)     classifier-service      Kafka (CC)          apply_tags.py
      │                        │                    │                     │
      │◀── scan-triggers ─────────────────────────│                     │
      │◀── {topic} messages ──────────────────────│  (interval join)    │
      │                        │                    │                     │
      │─── classify_fields() ─▶│                    │                     │
      │    (USING CONNECTIONS)  │                    │                     │
      │◀── detected_entities ──│                    │                     │
      │                        │                    │                     │
      │─── INSERT INTO ────────────────────────────▶│                     │
      │    ({topic}-scan-results, Avro encoded)     │                     │
      │                        │                    │                     │
      │    [continuous, per trigger]                │                     │
      │                        │                 (operator runs apply_tags.py)
      │                        │                    │◀─ consume ─────────│
      │                        │                    │                     │
      │                        │                    │  group by max(ts)  │
      │                        │                    │  dedupe by tag     │
      │                        │                    │  fetch SR schema   │
      │                        │                    │  patch + register  │
```

---

## Sequence diagram — Flink scanner fallback (local_scanner.py)

Used when USING CONNECTIONS is not enabled for the org, or when the classifier is not publicly accessible.

```
local_scanner.py        classifier-service      Kafka (CC)          apply_tags.py
      │                        │                    │                     │
      │─── consumer.poll() ───────────────────────▶│                     │
      │◀── Avro message ──────────────────────────│                     │
      │                        │                    │                     │
      │─── decode wire format  │                    │                     │
      │─── POST /classify ────▶│                    │                     │
      │◀── classification ─────│                    │                     │
      │                        │                    │                     │
      │─── producer.produce() ────────────────────▶│                     │
      │    ({topic}-scan-results, JSON encoded)     │                     │
      │                        │                    │                     │
      │    [loop until count]  │                    │                     │
      │                        │                 (operator runs apply_tags.py)
      │                        │                    │◀─ consume ─────────│
      │                        │                    │                     │
      │                        │                    │  group by max(ts)  │
      │                        │                    │  dedupe by tag     │
      │                        │                    │  fetch SR schema   │
      │                        │                    │  patch + register  │
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
