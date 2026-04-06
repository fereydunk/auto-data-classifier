# Architecture

## Component overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Confluent Cloud                              │
│                                                                     │
│  ┌──────────────┐    ┌─────────────────┐    ┌──────────────────┐   │
│  │ Source topic  │    │  Flink SQL       │    │ Output topics    │   │
│  │ (raw-messages)│───►│  routing.sql     │    │ classified-pii   │   │
│  └──────────────┘    │  (optional post- │    │ classified-medium │   │
│                       │   processing)    │    │ classified-safe   │   │
│  ┌──────────────┐    └─────────────────┘    │ classification-   │   │
│  │ Schema        │                           │   audit           │   │
│  │ Registry      │    ┌─────────────────┐    └──────────────────┘   │
│  │ (Avro schemas)│    │  Stream Catalog  │                          │
│  └──────────────┘    │  (field tags)    │                          │
│                       └─────────────────┘                          │
└──────────────────────────────┬──────────────────────────────────────┘
                                │  SASL/TLS (Kafka + SR REST API)
                                │
┌──────────────────────────────▼──────────────────────────────────────┐
│                      Your Compute (VM / K8s)                        │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  kafka-pipeline  (pipeline.py)                              │   │
│  │                                                             │   │
│  │  Consumer ──► Avro deserializer ──► classify() ──► Producer │   │
│  │                     │                   │                   │   │
│  │          Schema Registry           catalog_tagger           │   │
│  │          (schema ID → fields)      (field tags → SR)        │   │
│  └──────────────────────────┬────────────────────────────────┘    │
│                              │  HTTP (localhost)                    │
│  ┌───────────────────────────▼────────────────────────────────┐    │
│  │  classifier-service  (main.py)                             │    │
│  │                                                            │    │
│  │  POST /classify                                            │    │
│  │    ├─ flatten_fields()                                     │    │
│  │    ├─ AnalyzerEngine.analyze() per field                   │    │
│  │    │    ├─ Presidio built-in (email, phone, SSN, CC …)     │    │
│  │    │    ├─ PCI recognizers  (IBAN, SWIFT, routing …)       │    │
│  │    │    ├─ PHI recognizers  (NPI, DEA, insurance ID …)     │    │
│  │    │    ├─ Credentials     (AWS key, JWT, conn string …)   │    │
│  │    │    └─ GLiNER NER      (names, diagnoses, free text …) │    │
│  │    └─ taxonomy: entity → category → sensitivity            │    │
│  └────────────────────────────────────────────────────────────┘    │
│                  (all model files local — zero egress)              │
└─────────────────────────────────────────────────────────────────────┘
```

## Data flow

### Per-message flow

```
1. Consumer polls source topic
2. Raw bytes inspected for Confluent magic byte (0x00)
   ├─ Wire format → AvroDeserializer (schema from Registry)
   └─ Plain bytes → JSON.parse fallback
3. POST /classify  {"fields": {flattened message dict}}
4. Classifier iterates over leaf fields:
   a. analyzer.analyze(text, language="en")
   b. Results: list of RecognizerResult (entity_type, start, end, score)
   c. categorize_entity(entity_type) → DataCategory
5. sensitivity_for_categories({all categories}) → SensitivityLevel
6. Response: {sensitivity_level, categories, detected_entities}
7. Pipeline routes enriched message to output topic by sensitivity
8. Audit record always written to classification-audit
9. catalog_tagger.apply_classifications() — async, best-effort
   a. Resolve subject = "{topic}-value"
   b. Resolve schema version from schema_id or /versions/latest
   c. For each detected field: POST /catalog/v1/entity/tags
      with tag = highest DataCategory for that field
```

### Catalog tagging (async, per-field)

```
field: "patient.diagnosis"
entities: [MEDICAL_CONDITION (PHI)]

_highest_category(["PHI"]) → "PHI"

POST /catalog/v1/entity/tags
{
  "typeName": "sr_field",
  "attributes": {"qualifiedName": "lsrc-xxx:.:patients-value.v1.patient.diagnosis"},
  "classifications": [{"typeName": "PHI", "attributes": {...}}]
}
```

Tagging is **idempotent** — 409 responses are treated as success. An in-process set prevents re-tagging the same (subject, version, field, tag) within a process lifetime.

## Classifier service internals

### Recognition layers (fast → slow)

| Layer | Recognizers | Latency | Strength |
|---|---|---|---|
| Regex (Presidio built-in) | Email, phone, SSN, credit card, IP | <1ms | Structural PII, high precision |
| Regex (custom) | IBAN, SWIFT, routing, JWT, AWS key, NPI, DEA | <1ms | Domain-specific structural patterns |
| spaCy NER | PERSON, LOCATION, ORGANIZATION, DATE_TIME | 5–20ms | Named entities in English text |
| GLiNER NER | 90+ custom labels across all categories | 10–50ms | Contextual, zero-shot, extensible |

### Taxonomy design decisions

**Why separate DataCategory from SensitivityLevel?**
A field with `ORGANIZATION` (CONFIDENTIAL → MEDIUM) and `CREDIT_CARD` (PCI → HIGH) should be tagged as `PCI` in the catalog, not as `SENSITIVE`. The category is the meaningful business label; the sensitivity level is the operational urgency.

**Why does PHI → CRITICAL but PCI → HIGH?**
HIPAA penalties are significantly more severe than PCI DSS for equivalent breaches. Healthcare data also carries ethical obligations beyond regulatory compliance.

**Why does GLiNER run on every free-text field?**
GLiNER is zero-shot — it classifies against all 90+ labels in a single forward pass. Adding new entity types does not require model retraining or a new inference call.

## Schema Registry integration

### Deserialization

```python
# Wire format detection
if raw[0] == 0x00:
    schema_id = struct.unpack(">bI", raw[:5])[1]  # bytes 1–4
    payload = AvroDeserializer(sr_client)(raw, ctx)
else:
    payload = json.loads(raw)  # JSON fallback
```

The schema ID from the wire format is passed to the catalog tagger to tag the exact schema version that was observed in production — not just the latest version.

### Stream Catalog qualified names

Confluent Stream Catalog identifies schema fields by qualified name:

```
{sr_cluster_id}:.:{subject}.v{version}.{field_path}

Example:
lsrc-abc123:.:payments-value.v3.customer.email
```

`sr_cluster_id` is the `lsrc-xxx` value from Confluent Console → Schema Registry → Cluster settings.

## Scaling considerations

| Bottleneck | Mitigation |
|---|---|
| GLiNER CPU inference | Run multiple classifier replicas behind a load balancer; use `MAX_CONCURRENT` env var to control pipeline concurrency |
| Catalog API rate limits | In-process cache (`_tagged` set) eliminates redundant calls; same field is only tagged once per process lifetime |
| Schema version resolution | O(n) version scan per schema_id; acceptable for low schema churn; add Redis caching for high-churn environments |
| Kafka consumer lag | Increase `BATCH_SIZE` and `MAX_CONCURRENT`; add consumer replicas with same group ID |
