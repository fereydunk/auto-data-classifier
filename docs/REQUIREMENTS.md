# Auto Data Classifier — 1-Pager Requirements

**Version:** v1.0 (PRFAQ-aligned)
**Date:** 2026-05-14
**Status:** Working draft. Validated PoC; not yet GA.
**Companion docs:** `PRFAQ.md`, `ARCHITECTURE.md`, `TAXONOMY.md`

---

## Problem

Confluent Cloud customers have Schema Registry to know *what fields*
exist in their Kafka topics, but no automated way to know *which of those
fields contain regulated data*. The status-quo answer is one of:

1. **Manual review** by a data steward, topic by topic. Doesn't scale
   beyond ~30 topics. Goes stale every schema change.
2. **At-rest scanners** (Macie, DLP, Purview) that hit data after it
   lands in S3/GCS. Late, lossy, and tied to a single cloud.
3. **Nothing.** The team accepts the audit risk and hopes producers
   are diligent about not putting PHI in `comment` fields.

None of these are acceptable for a regulated industry running > 100
Kafka topics. We need automated, in-pipeline, per-message classification
that follows the data into Schema Registry tags so downstream consumers
inherit the answer.

---

## Goals

1. **Tag every sensitive field** in every Kafka topic the customer points
   us at, across 11 standard data categories (PII, PHI, PCI, FINANCIAL,
   GOVERNMENT_ID, CREDENTIALS, BIOMETRIC, GENETIC, NPI, LOCATION, MINOR).
2. **No data egress.** Classification models run inside the customer's
   perimeter; no payload reaches a third-party API.
3. **Schema-Registry-anchored.** Tags written as `confluent:tags` on
   schema fields and as `sr_field` entity tags in Atlas — both surfaces
   show the same classification, both are authoritative.
4. **Human-in-the-loop by default.** Every detection lands in a review
   queue; a data steward stages tags and pushes them to the catalog in a
   single batch per submit. No "auto-apply" mode in v1.
5. **Schema-evolution-aware.** A new schema version triggers a fresh
   scan of the topic within 60 seconds.
6. **Two operating modes** so customers pick the right cost/coverage
   trade-off:
   - **Streaming pipeline** — every message classified, sub-second tag
     emission, Mac-scale ~10 msg/s sustainable with Layer 3.
   - **Flink-SQL scanner** — periodic + event-driven sampling using CC
     Flink statements, much cheaper at high topic volumes.
7. **Setup wizard** that takes a customer from `git clone` to
   first-recommendation in under 5 minutes (excluding GLiNER's one-time
   model download).

## Non-Goals

1. **Custom user-defined entity types** beyond the 11 built-in tags. Add
   a 12th tag → file an issue, ship a release. (Customers wanting their
   own taxonomies — e.g., "INTELLECTUAL_PROPERTY" — are a future SKU.)
2. **Multi-tenancy / RBAC inside the review UI.** Single-team
   deployment model. The reviewer is a single SQLite DB; if multiple
   teams need separate review queues, run multiple instances.
3. **Push to non-Confluent catalogs.** Atlas standalone, Collibra,
   Alation are post-v1. Confluent Stream Catalog only.
4. **Automatic policy enforcement** (CSFLE encryption, consumer
   filtering, network ACLs) based on tag detection. We classify and
   surface; enforcement is the customer's pipeline.
5. **Image / audio / binary classification.** Text/JSON only.
6. **Fine-tuning the underlying models** per customer. Zero-shot GLiNER
   + Presidio recognizers ship as-is.
7. **Backfill of pre-existing topic data.** v1 classifies messages from
   the moment the pipeline / scanner starts. To classify history, the
   operator runs the scanner against the topic with `--start-from
   earliest`.

---

## Personas

| Persona | Primary need | What they touch |
|---|---|---|
| **Data steward / governance lead** | Confidence that every regulated field in every topic is tagged correctly; ability to review and approve before tags hit the catalog | Review UI (Card 6), Stream Catalog reports |
| **Data engineer** | Setup wizard works on first try; classifier doesn't bottleneck their topic | Setup wizard (Cards 1-5), `scan.env`, classifier-service logs |
| **Security / compliance officer** | Audit trail of who tagged what when; assurance that no payload leaves the perimeter | review-api SQLite (`reviewed_by` + `reviewed_at`), classifier-service deployment topology |
| **ML / data-science consumer** | Can query "which topics have PII?" before building a feature pipeline against them | Confluent Stream Catalog REST API, Schema Registry `confluent:tags` |
| **Producer team** | Doesn't want to be paged because their topic suddenly got flagged for PHI | Documentation explaining classifier is recommendation, not enforcement |

---

## Functional Requirements

### F1 — Classification engine
- **F1.1** Accept arbitrary JSON payloads. Walk nested objects to
  produce dotted-path field paths.
- **F1.2** Three-layer pipeline: field-name patterns (Layer 1), Presidio
  regex + checksum recognizers + spaCy NER (Layer 2), GLiNER zero-shot
  (Layer 3).
- **F1.3** Caller controls `max_layer` per request (1, 2, or 3).
- **F1.4** Each detected entity carries `tag`, `entity_type`, `score`,
  `layer`, `source` so the reviewer can audit how a detection was made.
- **F1.5** Free-text fields (`comment`, `notes`, `description`, …) get
  routed through Layer 3 even when other fields don't.

### F2 — Streaming pipeline
- **F2.1** Consume from configurable source topic. Decode Confluent Avro
  wire format (schema fetched by ID from SR per message) OR plain JSON.
- **F2.2** Route classified messages to:
  - `classified-messages` topic (tags ≥ 1)
  - `classified-safe` topic (zero tags)
  - `classification-audit` topic (every message — tags + entity types)
- **F2.3** POST a recommendation to review-api per (field_path, tag).
  Upsert: re-detection of same (topic, field, tag) updates existing
  PENDING row only if confidence is higher.
- **F2.4** Idempotent producer (`enable.idempotence=true`). Offset
  commits only after every message in the batch successfully classifies
  AND posts a recommendation — partial failures cause the batch to
  re-process.

### F3 — Flink-SQL scanner
- **F3.1** Three long-running CC Flink statements per source topic:
  - **A** — TUMBLE-window scheduled trigger. Configurable interval
    (default 60 min).
  - **B** — Schema-evolution trigger. SR poll once per minute, fires on
    version bump.
  - **C** — Scan driver. Interval-joins triggers with source topic over
    a configurable lookback window (default 2 min). Calls
    `classify_fields()` UDF per source row, writes to
    `{topic}-scan-results` topic in Confluent Avro wire format.
- **F3.2** `classify_fields()` UDF reaches the classifier service via
  CC's USING CONNECTIONS feature — no public internet required.
- **F3.3** Manual trigger via `start_scan.sh --now` for ad-hoc
  re-classification.
- **F3.4** Results-bridge subprocess consumes scan-results, decodes Avro
  via SR, POSTs each row to review-api as a recommendation.

### F4 — Review workflow
- **F4.1** Recommendation lifecycle: PENDING → STAGED → APPROVED, with
  REJECTED reachable from PENDING or STAGED. No state-machine
  short-cuts.
- **F4.2** Browser UI lists PENDING recommendations sorted by confidence
  desc; per-row Stage / Reject buttons.
- **F4.3** "Submit N to Stream Catalog" button POSTs every STAGED row
  in a single batch — one POST per submit, regardless of count.
- **F4.4** Pre-submit validation: `validate-staged` endpoint checks
  every STAGED row's field_path against the live SR schema; UI red-tints
  stale rows.
- **F4.5** Stale rows (field no longer in current SR schema) are
  auto-REJECTED with `reviewed_by="auto: <reason>"` instead of being
  silently dropped.
- **F4.6** Bulk-approve endpoint stages every PENDING row above a
  confidence threshold (default 0.85), then submits the batch.

### F5 — Tag application
- **F5.1** Two surfaces, one POST: `confluent:tags` field property on
  the schema (Avro / JSON Schema / Protobuf) AND `sr_field` entity tag
  in Atlas via `/catalog/v1/entity/tags`.
- **F5.2** Atlas `qualifiedName` format:
  `{cluster_id}:.:{schema_id}:{namespace}.{record_name}.{field_path}`.
  Pre-flight validation against live SR schema for every field.
- **F5.3** Tag definitions registered in Atlas once on first batch
  submit; cached per process.
- **F5.4** Single-flight lock around batch submit so concurrent submits
  don't double-tag.

### F6 — Setup wizard
- **F6.1** 6-card UI walks: prereqs check → CC sign-in → env + topic
  pick → start AI/ngrok/Flink connection → run test demo → review.
- **F6.2** Card 3's source-topic text box is the **single source of
  truth** for the topic name across the entire wizard. Changing it
  triggers automatic teardown of the prior topic's resources (Kafka
  topic, SR subject, Flink statements, review-api recommendations).
- **F6.3** Card 5 doesn't mark itself complete until at least one
  recommendation lands in review-api (180-second budget).
- **F6.4** Idempotent re-runs: every Card 5 click does a full clean-slate
  reset (delete topic → delete SR subject → fresh schema → fresh scan
  statements).

---

## Non-Functional Requirements

### NF1 — Latency
- **NF1.1** Streaming pipeline: 95th percentile end-to-end latency
  (message arrival → recommendation visible in review-api) < 2 seconds
  with `MAX_LAYER=2`, < 30 seconds with `MAX_LAYER=3`.
- **NF1.2** Flink scanner: 95th percentile end-to-end latency (scan
  trigger → first recommendation) < 30 seconds with `MAX_LAYER=2`,
  < 90 seconds with `MAX_LAYER=3`. (Today's PoC floor for L3 is 80s
  due to sequential per-message classify calls — addressed by the
  batch-classifier-endpoint roadmap item.)

### NF2 — Accuracy
- **NF2.1** Layer 1 (field name) precision ≥ 0.99 — false positives
  are unacceptable since this layer runs without value inspection.
- **NF2.2** Layer 2 (regex) precision ≥ 0.95 at HIGH confidence
  (score ≥ 0.85).
- **NF2.3** Layer 3 (GLiNER) precision ≥ 0.85 at HIGH confidence.
  Recall is the priority; review UI catches false positives.
- **NF2.4** Recall on canonical regression suite ≥ 0.95 across all 11
  tags.

### NF3 — Scale
- **NF3.1** Streaming pipeline sustains 1,000 msg/s with `MAX_LAYER=2`
  on a single classifier-service instance (4 CPU, 8 GB RAM).
- **NF3.2** Flink scanner handles 100+ topics per environment without
  exceeding 10% of a single Flink compute pool.
- **NF3.3** review-api SQLite handles 10K recommendations without
  query latency exceeding 100 ms.

### NF4 — Security & privacy
- **NF4.1** No payload data is sent to a third-party service. Verified by:
  classifier-service runs in-VPC; GLiNER and spaCy weights are local
  (downloaded once on first start).
- **NF4.2** All SR / Stream Catalog API calls authenticated via
  per-cluster API keys minted by the wizard; keys scoped to single
  resource (not org-wide).
- **NF4.3** No secrets logged. Bridge log captures only `(field_path,
  tag, score)` from scan-results — never the source-topic payload.
- **NF4.4** classifier-service's stdout log can contain payload data
  during inference; the wizard documents this and gitignores
  `scripts/logs/` accordingly.

### NF5 — Reliability
- **NF5.1** Streaming pipeline survives classifier outages — messages
  whose classification fails are not committed; re-processed on next
  poll.
- **NF5.2** Bridge survives review-api outages — fails individual POSTs,
  retries on next consumer cycle (Kafka offset commits remain
  conservative).
- **NF5.3** Wizard's clean-slate reset is idempotent: re-running Card 5
  after a failure leaves the system in the same state as a successful
  first run.

### NF6 — Operability
- **NF6.1** Every wizard card has a live log panel; logs persist
  across page reload via SSE backfill.
- **NF6.2** `start_scan.sh --status` reports the live status of the
  three long-running Flink statements.
- **NF6.3** Subprocess logs (`scripts/logs/*.log`) truncated per run —
  no unbounded growth across sessions.
- **NF6.4** Wizard FastAPI lifespan handler kills child subprocesses
  (classifier, ngrok, results-bridge) on shutdown; no orphans.

---

## Open Questions

1. **Should `MAX_LAYER=3` be the default?** It's slower and noisier. PoC
   defaults to 3 because the demo's compelling moment is GLiNER catching
   PHI in a free-text comment field. For production: probably `MAX_LAYER=2`
   with `MAX_LAYER=3` as a per-topic opt-in for known-free-text topics.
2. **Auto-approve at HIGH confidence?** Today every detection requires
   human review. For high-volume customers this is the bottleneck.
   Possible compromise: auto-approve Layer 1 (essentially deterministic)
   + Layer 2 at score ≥ 0.95; everything else stays in PENDING.
3. **Cross-cluster lineage?** When a topic is mirrored via cluster
   linking, should the destination cluster's Stream Catalog inherit the
   source cluster's tags automatically? Today: no — the destination is
   classified independently. Customers ask for this; it's a v1.5 item.
4. **What happens when the classifier returns an entity type not in our
   11-tag taxonomy?** Today: discarded silently. Should we log it for
   taxonomy expansion? Surface it as `OTHER`?
5. **Should the wizard support multi-topic scanning in one run?** Today:
   one source topic per Card 5 run. Real customers have 100+ topics —
   they'd want to point the scanner at a wildcard or a topic group.
6. **Pricing model for the AI inference cost.** Customer pays compute
   for GLiNER inference; should that be per-classification, per-MB, or
   bundled into a flat rate? Affects how we expose the `MAX_LAYER` knob.

---

## Success Metrics

| Metric | 6 mo target | 12 mo target |
|---|---|---|
| Time-to-first-tag (wizard launch → first rec in review UI) | < 5 min | < 2 min |
| End-to-end classify latency (message → catalog tag) at p95, MAX_LAYER=3 | < 90 s | < 30 s |
| False-positive rate at HIGH confidence tier | < 5% | < 2% |
| Recall on canonical 11-tag regression suite | ≥ 0.95 | ≥ 0.98 |
| Design partners running in production | 10 | 100 |
| % of customer's CC topics with at least one classified field | n/a (per-customer) | ≥ 80% within 30 days of install |

---

## Risks (mitigations referenced)

| Risk | Mitigation |
|---|---|
| Classifier latency floor (80s for 40 messages with L3) becomes user-facing pain | Three architectural fixes scoped: batch endpoint, Flink AsyncTableFunction, MAX_LAYER=2 trade-off documented (`docs/TUNING.md`, `docs/LEARNINGS.md`) |
| GLiNER model behavior shifts between releases | Pin model version in `gliner_recognizer.py`; canonical regression suite per release |
| Atlas API changes break tag application | SR-validation gate pre-checks every field path before catalog POST; fail-closed if unparseable |
| Customer assumes "tagged" means "protected" | Clear documentation that classification is recommendation, not enforcement; PRFAQ FAQ entry; review UI footer messaging |
| Schema-Registry rate limits trip on large customer estates | Per-(subject, version) cache with 60-s TTL in `sr_schema.py`; per-id immutable cache in bridge |
