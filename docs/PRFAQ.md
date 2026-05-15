# PRFAQ — Auto Data Classifier

> Working back from the launch announcement. The press release is what we
> would publish on day-one; the FAQ is what stakeholders ask in week-two.
> Use this document as the strategic anchor for scope decisions.

**Status:** Draft v1 — 2026-05-14
**Owner:** Data Governance team
**Stage:** Proof-of-concept validated against live Confluent Cloud
(`docs/ARCHITECTURE.md` → "Validated E2E results")

---

## Press Release

### Confluent Cloud customers can now see — and tag — every piece of sensitive data flowing through their Kafka topics, in real time, without sending a byte to a third party.

**Auto Data Classifier** is now generally available for Confluent Cloud.
The service inspects every message on every Kafka topic, identifies 11
categories of sensitive data — from credit card numbers and government IDs
to medical conditions and biometric identifiers — and applies the
classification directly to the Schema Registry schema and Stream Catalog
metadata. Every detection happens against models that run inside the
customer's own infrastructure; no payload ever leaves the customer
perimeter.

Mountain View, CA — May 2026

> "Before this, we knew that PII was somewhere in our 1,400 Kafka topics.
> We didn't know which fields, which topics, or how much of it. Six weeks
> of human review only covered the top 30 topics. With Auto Data Classifier,
> the whole estate was tagged in two days, and every new topic is tagged
> automatically the moment a producer sends its first message." — *Hypothetical
> Director of Data Governance, large US bank*

The classifier solves the gap between *having* a Schema Registry and
*understanding* what the registered schemas actually carry. Most
organizations adopt CC Schema Registry for the technical contract — what
fields exist, what their types are — but the question their compliance
teams keep asking is the harder one: *which of those fields contain
regulated data, and which downstream consumers see it?*

Auto Data Classifier answers that question in two complementary modes:

- **Streaming pipeline** — every message on a topic gets inspected
  in-flight as it flows through the pipeline. Classifications are
  written to the Stream Catalog within seconds of the data arriving.
  Use this for new topics and for high-assurance environments where
  every record must be classified.

- **Flink-SQL scanner** — periodic + event-driven sampling. Three
  long-running Confluent Cloud Flink statements (a TUMBLE-window
  schedule, a Schema-Registry change watcher, and an interval-join scan
  driver) call the classifier on a representative sample of recent
  messages. Use this for established topics where a continuous
  re-inspection on every message is overkill but you still want
  classifications to track schema evolution.

Both modes share the same three-layer classification engine:

| Layer | Mechanism | What it adds |
|---|---|---|
| 1 | Field name patterns | Free signal from `email`, `ssn`, `creditCardNumber`, etc. — zero data access. <1 ms. |
| 2 | Presidio regex + checksum recognizers + spaCy NER | Credit-card Luhn validation, IBAN, SWIFT, SSN, phone, person/org/location. 5-20 ms. |
| 3 | GLiNER zero-shot model | Free-text fields where pattern matching fails — clinical notes, comment fields, support tickets. 50 ms – 8 s. |

Detections from all three layers feed a human-in-the-loop review queue.
The data steward sees the field, the proposed tag, the confidence, the
layer that made the call, and a snippet of the classified value — then
either stages the recommendation for batch submission to the Stream
Catalog, or rejects it. Approved tags are pushed to Atlas in a single
batch POST per submit, so a topic with fifty new tags doesn't bump the
Schema Registry version fifty times.

Get started: install the wizard, walk the six setup cards, point Card 3
at any source topic in your environment. The wizard mints scoped API
keys, deploys the Flink statements, exposes the classifier behind your
own Confluent Cloud Flink connection, and lands the first recommendation
in the review UI in under 90 seconds.

---

## Frequently Asked Questions

### Customer-facing

**Q: Where does my data go? Are you sending payloads to a hosted model?**

No. The classifier — including the GLiNER zero-shot model — runs as a
container inside your environment. The Flink scanner reaches it via
Confluent's USING CONNECTIONS feature, which routes only the JSON you've
already encoded into the Kafka topic across a connection you provision.
Nothing transits a third-party API. The only outbound calls go to
Confluent Schema Registry and Stream Catalog (your own).

**Q: What categories of sensitive data does this detect?**

Eleven flat tags, no nested hierarchy: PII, PHI, PCI, CREDENTIALS,
FINANCIAL, GOVERNMENT_ID, BIOMETRIC, GENETIC, NPI (non-public
information — insider financials, M&A data), LOCATION, MINOR. Each tag
maps to multiple underlying entity types — see `docs/TAXONOMY.md`.
Sensitivity levels (HIGH/MEDIUM/LOW) are deliberately not baked in —
your organization's policy decides what severity each tag triggers.

**Q: My fields are nested. Does it walk into nested records?**

The streaming pipeline walks nested Avro records and emits dotted-path
field paths (`customer.address.zipcode`). The Flink scanner currently
classifies only top-level fields; nested-record support is on the
roadmap. For nested-heavy topics, use the streaming pipeline.

**Q: How accurate is it?**

The April 2026 validation run against a live Confluent Cloud cluster
detected 1,874 tag instances across 280 messages with all 11 categories
represented (`docs/ARCHITECTURE.md` → "Validated E2E results"). False
positives are surfaced in the review UI with their confidence and the
matching value snippet, so a human catches them before they reach the
Stream Catalog. Layer 1 (field name) is essentially deterministic;
Layer 2 (regex) is high-precision; Layer 3 (GLiNER) is the recall layer
and the one most likely to over-flag — that's why every tag is staged
for review by default.

**Q: How does it handle schema evolution?**

The Flink scanner has a dedicated schema-evolution trigger
(SchemaWatcherUDF) that polls Schema Registry once per minute and fires
a fresh scan within seconds of any version bump. The streaming pipeline
re-fetches the schema by ID per message — every record is decoded against
its own canonical schema, so producers and consumers running against
different schema versions are both classified correctly.

**Q: How does it interact with Confluent Stream Catalog?**

The review UI's "Submit to Stream Catalog" button POSTs every staged tag
in a single batch — one POST per submit, regardless of how many tags.
This avoids the schema-version-per-tag bump that would otherwise occur.
Tags applied: `confluent:tags` (Avro / JSON Schema / Protobuf) on
schema fields, plus `sr_field` entity tags in Atlas. Both surfaces show
the classification, so any consumer reading the schema *or* querying the
catalog sees the same answer.

**Q: Does it support non-Avro schemas?**

Avro is fully supported (with nested records in the streaming pipeline).
JSON Schema and Protobuf are supported in `apply_tags.py` (the schema
patcher used by the Flink scanner's apply step). The Flink scanner's
`generate_json_object.py` SQL generator currently handles Avro only.

**Q: How is this different from AWS Macie / Google DLP / Microsoft Purview?**

Three differences:

1. **In-pipeline**, not in-storage. The cloud-provider scanners hit data
   at rest in S3 / GCS / Blob. Auto Data Classifier hits data in motion
   in Kafka — typically *before* it lands in any storage layer. The
   classifications follow the schema, so every downstream consumer
   inherits them automatically.

2. **No data egress.** GLiNER and Presidio run inside your perimeter.
   Macie and DLP send fragments to the cloud provider's hosted model.

3. **Schema-Registry-anchored.** Tags are written to the schema
   definition itself (`confluent:tags`) — they are part of the contract
   the producer publishes and every consumer subscribes to. Macie tags
   live as object metadata; nothing forces downstream consumers to
   honor them.

### Internal / strategic

**Q: Why now?**

Two trends crossed in the last year: (a) regulator focus on Kafka /
streaming as a regulated data path (GDPR Art. 30 records of processing
now explicitly call out streaming systems; HIPAA breach notification
covers transit, not just storage), and (b) zero-shot NER models like
GLiNER became fast enough to run on CPU-only inference at acceptable
cost. The combination makes a per-message in-pipeline classifier
commercially viable for the first time.

**Q: What's the differentiation moat against a competitor building the same thing?**

The Schema-Registry-as-source-of-truth design and the Stream-Catalog
integration are the moat. Anyone can run Presidio + GLiNER. The hard
parts are: (a) decoding Confluent Avro wire-format reliably across schema
versions, (b) building the Atlas qualifiedName format correctly so tags
actually attach (the project ate a real bug here — see
`docs/LEARNINGS.md`), (c) the Flink-USING-CONNECTIONS network plumbing
that lets a UDF call out to a private classifier, and (d) the
human-in-the-loop staging flow with stale-row protection. Each of those
is a multi-week build-out for a competitor.

**Q: What's explicitly out of scope for v1?**

- Non-Avro schemas in the Flink scanner (JSON Schema / Protobuf works
  for streaming + manual apply only)
- Nested-record classification in the Flink scanner (top-level fields
  only — see `docs/LEARNINGS.md` → "Recommendation pipeline")
- Custom user-defined entity types beyond the 11 built-in tags
- Multi-tenancy / RBAC inside the review UI (single-team deployment
  model assumed)
- Automatic re-classification on schema evolution (the Flink scanner
  re-classifies the *same* topic; renaming a topic requires a wizard
  re-run — see `PRIOR_SOURCE_TOPIC` cleanup in
  `docs/ARCHITECTURE.md`)
- Push to non-Confluent catalogs (Apache Atlas standalone, Collibra,
  Alation) — Confluent Stream Catalog only

**Q: What does success look like at 6 / 12 / 24 months?**

| Horizon | Metric |
|---|---|
| 6 months | 10 design partners running in production. Median time-to-first-tag < 5 minutes from setup wizard launch. < 5% false-positive rate at HIGH confidence tier. |
| 12 months | 100 customers. < 60s end-to-end latency from message arrival to Stream Catalog tag application (today's floor is ~80s; requires the batch-classifier-endpoint optimization). Support for JSON Schema + Protobuf in the Flink scanner. |
| 24 months | A new sensitive-data type discovered in production triggers an automatic governance workflow (PagerDuty alert, ticket creation, automated quarantine of the topic) without human intervention. Cross-cluster lineage — show every downstream cluster that received a sensitive field. |

**Q: What are the biggest risks?**

1. **Classifier latency.** The Flink scanner makes one synchronous HTTPS
   call per source row. With Layer 3 enabled, that's ~80s per 40-message
   scan. Three architectural fixes are scoped (batch endpoint, Flink
   AsyncTableFunction, lower MAX_LAYER for non-precision use cases) —
   see `docs/TUNING.md` and `docs/LEARNINGS.md`.

2. **GLiNER label drift.** Zero-shot models can shift behavior between
   versions. We pin the model version in `gliner_recognizer.py` and
   maintain a regression suite of canonical detections — but a customer
   training data set with new vocabulary may behave differently than our
   test corpus.

3. **Atlas API changes.** The `qualifiedName` format and the flat
   payload shape are documented but not contractual. A Confluent SR /
   Atlas upgrade could change either; we'd need to revalidate. Mitigated
   by the SR-validation gate that pre-checks every field path against
   the live schema before any catalog POST.

4. **Customer perception of "tagging means done."** A tag in the catalog
   is a recommendation, not an enforcement. Customers must still build
   downstream policy enforcement (CSFLE, network ACLs, consumer
   filtering). We'll need clear documentation about what classification
   does and does not solve.

**Q: How does pricing work?**

Two SKUs:

- **Streaming pipeline**: per-MB-classified, similar to Confluent's
  cluster pricing. Volume discounts at standard tiers.
- **Flink scanner**: priced as standard Flink compute usage (the
  scanner runs on the customer's compute pool); the classifier service
  itself is included.

Free tier: 1 GB/month classified for non-production use.

**Q: What about data residency?**

The classifier service runs as a customer-deployed container — typically
in the same region as the Kafka cluster. No payload transits a Confluent
or third-party model service. The only Confluent-side state is the tag
metadata itself, which lives in the customer's Schema Registry and
Stream Catalog (already region-pinned).

**Q: Can a customer disable Layer 3 (the AI model) for compliance reasons?**

Yes. `MAX_LAYER=2` (in the streaming pipeline's `.env`) or
`CLASSIFIER_MAX_LAYER=2` (in the Flink scanner's `scan.env`) skips
Layer 3 entirely — only field-name patterns and Presidio regex run.
Some regulated environments forbid free-text inspection of message
values; this gives them an opt-out without losing structured-data
detection.

**Q: Why not just train a single model for everything?**

We tried. A single fine-tuned classifier needed a labeled corpus per
customer to get production-quality precision, and the inference cost
was 5–10× the three-layer approach. More importantly, Layer 1 (field
name) is essentially free — using a model where a regex would do is
expensive and worse. The three-layer design lets each layer do what
it's best at: schema metadata (Layer 1), structural patterns (Layer 2),
free-text NER (Layer 3).
