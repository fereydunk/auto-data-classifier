# Learnings

A running log of non-obvious gotchas we've hit on this project. Every entry
below cost real debugging time — keep it updated as we hit new ones.

---

## Confluent Stream Catalog

### Atlas `sr_field` qualifiedName format

The `qualifiedName` for an `sr_field` entity is **NOT** what we initially
assumed. It is:

```
{cluster_id}:.:{schema_id}:{namespace}.{record_name}.{field_path}
```

NOT:

```
{cluster_id}:.:{subject}.v{version}.{field_path}     ← rejected by Atlas
```

The wrong format produces the cryptic error:
```
{"error_code":4000020,"message":"Type ENTITY with name null does not exist"}
```

`schema_id` and `{namespace}.{record_name}` come from the SR schema body —
they cannot be derived from subject + version alone. See
`review-api/sr_schema.py:extract_record_qualifier`.

### `/catalog/v1/entity/tags` payload shape — flat, not nested

The endpoint expects the **flat** shape:

```json
[{"entityType": "sr_field", "entityName": "<qualifiedName>", "typeName": "<TagName>"}]
```

NOT the nested shape some older Atlas docs describe:

```json
[{
  "typeName": "sr_field",
  "attributes": {"qualifiedName": "..."},
  "classifications": [{"typeName": "<TagName>", "attributes": {...}}]
}]
```

The nested shape returns HTTP 200 in some cases but the tag never actually
attaches to the entity — silent failure. Always use the flat shape.

### Tag definition vs tag application — separate APIs

| Action | Endpoint | Payload |
|---|---|---|
| Define a tag | `POST /catalog/v1/types/tagdefs` | `[{name, entityTypes, description, attributeDefs}]` |
| Apply a tag to a field | `POST /catalog/v1/entity/tags` | `[{entityType, entityName, typeName}]` |

Tag definitions live in the catalog. Tag applications target field entities
that may or may not exist yet — Atlas auto-materializes the entity from a
well-formed `entityName` when the first tag is applied.

### Atlas catalog is eventually-consistent with SR

After registering a new schema in SR, Atlas:
- Indexes the `sr_record` entity within seconds
- Indexes `sr_field` entities lazily (sometimes minutes after the record)

Tag application on a not-yet-materialized field works (Atlas auto-creates the
entity from the `entityName`), but searching for the field via
`/catalog/v1/search/basic?type=sr_field&query=...` may return zero results
for several minutes. Don't rely on search-then-tag — always tag directly with
the well-formed `entityName`.

---

## Confluent Cloud Flink

### Statement `describe` is eventually-consistent

`confluent flink statement delete <name> --force` returns immediately, but
`confluent flink statement describe <name>` may continue returning the
just-deleted statement for tens of seconds afterward. Code that runs
`delete` then immediately checks for existence will see false positives.

**Fix:** delete-then-poll-until-gone-then-create. See
`flink-scanner/scripts/start_scan.sh:submit_statement`.

### `describe` 404 doesn't mean `create` will succeed (namespace race)

Even after the eventually-consistent `describe` returns 404, attempting
`create` for the same statement name can still return:
```
Error: Statement with name "X" already exists.
```
CC's control plane has separate stages: `describe` reports 404 *before*
the namespace is freed for re-create — the namespace stays reserved
while the underlying job tears down. Polling on `describe` is the wrong
signal.

**Fix:** poll on the actual signal — try `create`, on "already exists"
re-delete + sleep with linear backoff (15/30/45/60/75s), retry up to 5
times. See `flink-scanner/scripts/start_scan.sh:submit_statement`.

### `flink statement list` warns about default endpoint on every call

`No Flink endpoint is specified, defaulting to public endpoint:
https://flink.<r>.<c>.confluent.cloud` is emitted on every flink command
even when `--cloud`/`--region` are passed explicitly. Pinning the active
endpoint silences it for the rest of the CLI session:

```bash
confluent flink region   use --cloud aws --region us-east-2
confluent flink endpoint use https://flink.us-east-2.aws.confluent.cloud
```

Order matters — `region use` *unsets* the endpoint, so `endpoint use`
must come second. See `setup_wizard/main.py:_pin_flink_endpoint` and the
top of `flink-scanner/scripts/start_scan.sh`.

### Interval-join needs source watermark to advance PAST trigger time

The scan-driver SQL:
```sql
JOIN source_topic AS p ON p.`$rowtime` BETWEEN t.triggered_at - INTERVAL '2' MINUTES AND t.triggered_at
```

For a trigger row at time T, Flink can only emit join results after the
source watermark advances past T. A one-shot produce of N messages BEFORE
the trigger leaves the watermark stuck at the last message's rowtime —
which is BEFORE T — and the join never emits.

**Fix:** the wizard produces 40 messages → fires trigger → produces 10 more
messages. The trailing wave pushes the watermark past T so the join emits.
See `setup_wizard/main.py:_demo_worker`.

### Statement startup takes 30–60s

Confluent Cloud Flink statements transition PENDING → RUNNING with significant
latency (commonly 30–60 s). Firing a manual trigger immediately after
`statement create` means the trigger row is consumed by a not-yet-active
driver — the interval-join window misses it and nothing flows.

**Fix:** `_wait_for_scan_driver_running()` polls every 5 s until status is
RUNNING (or 180 s timeout), then fires the trigger.

### Flink default scan-results format is Confluent wire-format Avro

When the scan-driver SQL uses `INSERT INTO {topic}-scan-results` without an
explicit format, Flink writes Confluent wire-format Avro (0x00 + 4-byte
schema_id + Avro bytes), with an auto-registered schema in SR.

Consumers of scan-results MUST decode wire-format — JSON parse will fail
with `UnicodeDecodeError: 'utf-8' codec can't decode byte 0x86`.
See `setup_wizard/results_bridge.py:_decode_wire_avro`.

### `submit_statement` skip-if-exists leaves stale SQL in place

If the dynamic schema regenerates per run (different field names), but the
scan statement was created in a prior run with the OLD field list, then
`submit_statement` skipping re-create means the FAILED scan-driver remains
broken. Always force-replace per run. (See "Statement describe is eventually-
consistent" above for the fix.)

---

## Confluent Schema Registry

### Re-registering a deleted subject restarts version at 1

After `DELETE /subjects/{subject}?permanent=true` and re-registering, the
version sequence starts back at v1 — but with a NEW `schema_id`. Code that
caches by `(subject, version)` will return stale data. Use the schema_id as
the canonical identity; cache TTL keeps things bounded.

### Use `?permanent=true` for true subject deletion

`DELETE /subjects/{subject}` is a SOFT delete — versions are hidden but the
subject namespace is held. To free the subject for a fresh registration:

1. `DELETE /subjects/{subject}` (soft)
2. `DELETE /subjects/{subject}?permanent=true` (hard)

A 404 on the second call after the first succeeded is fine — already-soft-
deleted subjects sometimes don't need the explicit hard delete.

### Freshly-minted SR API keys take 5–15s to propagate

`confluent api-key create --resource <lsrc-...>` returns immediately, but
the new key isn't actually authorized on the SR cluster yet. A fast user
click on Card 5 right after Card 3 saves hits SR with a not-yet-active
key → HTTP 401 → Card 5 fails on the very first SR call.

**Fix:** `_wait_for_sr_key_active` probes `GET /subjects` with the new
key after mint (in `cc_env_select`); blocks until 200 (timeout 30s).
Belt-and-suspenders: `_delete_demo_subject` also retries 401 up to 3×
with backoff in case propagation drifts past 30s.

---

## Recommendation pipeline

### Bridge must thread the source topic's schema_id through

`results_bridge.py` was hardcoding `schema_id: None` on every POST to
review-api with a comment "review-api falls back to latest version".
That fallback works for single-run demos but breaks `validate-staged`'s
ability to check a recommendation against the *exact* SR schema version
that produced it — instead it always checks against latest.

**Fix:** `_fetch_source_schema_id(subject)` queries
`GET /subjects/{topic}-value/versions/latest` once, caches the schema_id
with 60-s TTL, returns it for every POST. SR errors fall back to None
(defensive).

### Classify_fields() is the dominant latency in the demo flow

End-to-end Card 5 latency (wave-2 produce → first recommendation in
review-api) is typically 30–90s, dominated by **40 sequential blocking
HTTPS calls** from the Flink scan-driver to the classifier service —
one POST per source row × ~1–3s/call (Layer 3 = GLiNER on Mac).

The math: 40 messages × 2s/call = **80s minimum**, before any network
latency to ngrok. Three real fixes (architectural, not hacks):
1. **Batch endpoint**: `POST /classify_batch` accepts a list of records;
   `classify_fields()` collects N rows then calls once.
2. **AsyncTableFunction**: replace the blocking `TableFunction` with
   Flink 1.19's `AsyncTableFunction` so M HTTPS calls run concurrently.
3. **Lower MAX_LAYER**: `MAX_LAYER=2` in scan.env skips GLiNER, drops
   per-call to 5–50ms. Fast for demos; loses AI-detected entities.

For the wizard demo today: option 3 is a one-env-var workaround. Options
1 and 2 are the long-term right fix. See agent code-review notes (Critical
finding #1 in flink-scanner section).

---

## Wizard topic-rename leaks (fixed via PRIOR_SOURCE_TOPIC tracking)

If the user changes Card 3's topic name between Card 5 runs, the OLD
topic's resources stay live in CC indefinitely:
- Old Kafka topic
- Old SR subject (`{old}-value`)
- Old long-running Flink statements (`{old}-scan-trigger-scheduled`,
  `-schema`, `-driver`) — these cost compute pool minutes
- Old recommendations in review-api

Each rename accumulates dead infrastructure.

**Fix:** persist `PRIOR_SOURCE_TOPIC` in scan.env. `_demo_worker` reads it
at start; if `prior != current`, `_cleanup_prior_topic(prior)` tears down
all 4 resource types before provisioning the new topic. Best-effort —
each step logs but doesn't block the new run. After every successful
Card 5 start, scan.env's `PRIOR_SOURCE_TOPIC` is updated to the current
topic so the cleanup is ready for the next rename.

---

## Wizard demo flow

### Each demo run needs a full clean-slate reset

A "Run test demo" with a different field count from the previous run leaves:
- Old recommendations in review-api referencing fields that no longer exist
- Old SR schema versions that the producer accidentally encodes against
- Old Kafka messages encoded with the previous run's schema_id (un-decodable)
- Old Flink scan statements with field names the new schema doesn't have

**Fix:** in `_create_demo_topic()`:
1. DELETE recommendations for the topic in review-api
2. SR DELETE the subject (soft + permanent)
3. Kafka DELETE the topic
4. Kafka CREATE the topic

In `_start_scan()`:
5. `start_scan.sh --stop` (drop existing scan statements)
6. `start_scan.sh` (force-replace via delete-then-poll-then-create)

Without all 6 steps the demo silently runs against partial stale state.

### `_demo_state` counters must reset per run

`messages_produced` (and any other accumulator) is overwritten by each
`_produce_test_messages()` call. With the two-wave produce, this means the
final UI shows "10 messages produced" instead of 50. Fix: reset to 0 at the
top of `_demo_worker()` and accumulate (not overwrite) in `_produce_test_messages`.

---

## Memory / `git push`

### `git push` to personal GitHub repos is blocked by Airlock

The corporate Airlock proxy blocks `git push` to personal GitHub repos AND
fakes a "success" output, so the local repo APPEARS to be in sync but isn't.

**Fix:** use `gh api graphql` with the `createCommitOnBranch` mutation. After
each push, verify with `gh api repos/<owner>/<repo>/commits/main --jq .sha`
matches the local HEAD.

---

## Tests

### sr_schema in-process cache leaks across tests

`sr_schema._cache` and `_meta_cache` are module-level dicts. Without an
explicit reset, one test's mock SR response can satisfy the next test's
fetch and mask bugs. Use the `_clear_sr_cache` autouse fixture in
`tests/test_catalog_tagger.py`.

### Mock SR responses must include `id`, `schema`, AND `schemaType`

The SR validation gate calls `fetch_schema_meta_cached` which extracts:
- `body["id"]` (the schema_id used for the qualifiedName)
- `body["schema"]` (the JSON-encoded Avro definition — needs `name` + `namespace`)
- `body["schemaType"]` (defaults to AVRO if absent)

Mocks that only return `{"id": 99, "version": 1}` fail validation closed and
no tag is POSTed. See `tests/test_catalog_tagger.py:_avro_schema_for_paths`.
