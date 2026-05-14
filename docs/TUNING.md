# Tuning Guide

## Classification depth — MAX_LAYER

The single most impactful configuration knob. Controls which layers run on every message.

```
MAX_LAYER=1   Field name only        Latency: <1ms    No data inspection
MAX_LAYER=2   + Regex patterns       Latency: 5-20ms  Pattern matching on values
MAX_LAYER=3   + AI model (default)   Latency: 50-500ms Contextual NER on values
```

Set in `docker-compose.yml` or as an environment variable:

```bash
MAX_LAYER=2  # skip AI model — good for high-throughput topics or sensitive environments
MAX_LAYER=1  # field name only — zero data access, useful for schema-only classification
```

### When to use each level

| Scenario | Recommended |
|---|---|
| High-volume topic (>10k msg/s), schema well-known | `MAX_LAYER=1` |
| Mixed schema topics, regex patterns reliable | `MAX_LAYER=2` |
| Free-text fields (comment, notes), unknown schema | `MAX_LAYER=3` |
| Regulated environment, no value inspection allowed | `MAX_LAYER=1` |
| Initial topic discovery / onboarding | `MAX_LAYER=3` (via Flink scanner) |

---

## Confidence thresholds

### Review API bulk-approve threshold

Default: `0.85`. Adjustable per call:

```bash
curl -X POST http://localhost:8001/recommendations/bulk-approve \
  -H "Content-Type: application/json" \
  -d '{"min_confidence": 0.90}'
```

### Flink scanner bulk-approve shortcut

Pass `--yes` to `apply_tags.py` to approve all classifications without interactive prompting, or set a minimum confidence threshold:

```bash
# Auto-approve all (default min_confidence = 0 when using --yes)
python flink-scanner/apply_tags.py --topic payments ... --yes

# apply_tags.py shows HIGH/MEDIUM/LOW per classification — review LOW ones carefully
```

To skip low-confidence results entirely, review interactively and press `n` for anything coloured red (LOW tier, score < 0.60).

### Recogniser-level score tuning

Each recogniser has a default confidence score. Override in the recogniser file:

```python
# classifier-service/recognizers/financial_recognizers.py
class BankAccountRecognizer(PatternRecognizer):
    PATTERNS = [
        Pattern("BANK_ACCOUNT", r"\b\d{8,17}\b", 0.4)  # ← lower = less confident
    ]
```

Scores to tune by use case:

| Recogniser | Default | Notes |
|---|---|---|
| `BankAccountRecognizer` | 0.40 | 8-17 digit sequences match many non-financial IDs |
| `USRoutingNumberRecognizer` | 0.60 | 9-digit sequences — moderate false positive rate |
| `GLiNERRecognizer` | model output | Varies by entity type and context |
| Layer 1 (field names) | 0.78–0.95 | Generally reliable; tune rarely |

---

## Throughput tuning

### Pipeline batch size

```bash
BATCH_SIZE=50       # messages per commit (default)
BATCH_SIZE=100      # higher throughput, larger latency window
```

Larger batches amortise Kafka commit overhead. Set to 100–200 for high-volume topics.

### Idle-flush behaviour

The pipeline does not wait for a full `BATCH_SIZE` batch before committing. When `consumer.poll()` returns `None` (no new messages within the poll timeout), any pending asyncio tasks are immediately gathered, the producer is flushed, and the consumer offset is committed. This bounds end-to-end latency for low-volume topics or small bursts to approximately the `poll(timeout=1.0)` interval — at most ~1 second — regardless of `BATCH_SIZE`. No tuning is required; this is automatic.

### Concurrent classifications

```bash
MAX_CONCURRENT=10   # parallel /classify calls (default)
MAX_CONCURRENT=25   # if classifier service has headroom
MAX_CONCURRENT=3    # recommended for Mac laptop with Layer 3 (GLiNER is compute-heavy;
                    # 10 concurrent will cause timeout cascades on Apple silicon/Intel)
```

### Classifier timeout

```bash
CLASSIFIER_TIMEOUT_S=5.0    # per-request timeout (default)
CLASSIFIER_TIMEOUT_S=10.0   # if Layer 3 is slow under load
CLASSIFIER_TIMEOUT_S=15.0   # recommended for Mac laptop with Layer 3
                             # (GLiNER inference runs 2–8s per message locally)
```

### Classifier service — expected latency by layer

| Layer | p50 | p95 | p99 |
|---|---|---|---|
| 1 only | <1ms | 2ms | 5ms |
| 1+2 | 5ms | 20ms | 50ms |
| 1+2+3 (CPU) | 100ms | 300ms | 500ms |

Layer 3 latency depends on value length and hardware. On Apple M-series or modern Intel, expect 100–200ms for typical field values (<200 chars). GPU would be 5–10× faster but is not required.

---

## Scaling the classifier service

The classifier service is stateless — scale horizontally behind a load balancer:

```yaml
# docker-compose.yml
classifier:
  deploy:
    replicas: 3
```

Or on Kubernetes:
```yaml
spec:
  replicas: 3
  resources:
    requests:
      memory: "2Gi"
      cpu: "1000m"
    limits:
      memory: "4Gi"
```

GLiNER loads the model into memory once per process (~500 MB). Each replica needs ~1.5 GB RAM (500 MB model + 750 MB spaCy en_core_web_lg + overhead).

---

## Flink scanner — schedule and window sizing

Both parameters live in `flink-scanner/scan.env`. Change either and restart with `start_scan.sh`.

```
SCAN_INTERVAL_MINUTES=60    # TUMBLE window size — how often Statement A fires
SAMPLE_WINDOW_MINUTES=2     # interval join lookback — how much data Statement C classifies
```

These are independent:
- Increasing `SCAN_INTERVAL_MINUTES` reduces Flink compute cost (fewer classification runs)
- Increasing `SAMPLE_WINDOW_MINUTES` improves sample coverage per run but increases UDF calls

### Choosing SAMPLE_WINDOW_MINUTES

The sample window is a tradeoff between coverage and cost:

| Shorter window | Longer window |
|---|---|
| Less representative sample | More representative sample |
| Fewer messages, lower cost | More messages, higher cost |
| Better for high-volume topics | Better for low-volume topics |

**Rule of thumb:** aim for 500–2,000 unique messages per scan window. For a topic doing 1,000 msg/min, 1–2 minutes is sufficient. For a topic doing 10 msg/min, use 10–15 minutes.

### Schema-evolution trigger timing

The `schema_watcher()` UDF checks Schema Registry at most once per minute regardless of topic message rate. This means a schema change will be detected and trigger a scan within 1 minute of the schema registration.

### Three trigger types and when they fire

| Trigger | When | Statement |
|---|---|---|
| Scheduled | Every `SCAN_INTERVAL_MINUTES` | A (TUMBLE) |
| Schema evolution | Within 1 min of SR version change | B (schema_watcher UDF) |
| Manual | Immediately on `start_scan.sh --now` | one-shot INSERT |

All three write to the same `{topic}-scan-triggers` topic. Statement C (scan driver) reacts to each one identically.

---

## Reducing false positives

### Layer 1 — generic field names

By design, `id`, `value`, `data`, and `ref` do not match any patterns. If you have domain-specific generic names that should not be classified (e.g. `ref_code`, `internal_id`), add them to an exclusion list:

```python
# classifier-service/recognizers/field_name_recognizer.py
_EXCLUDED_FIELD_NAMES = {"ref_code", "internal_id", "legacy_ref"}

def classify_field_name(field_name: str) -> list[FieldNameMatch]:
    if field_name in _EXCLUDED_FIELD_NAMES:
        return []
    ...
```

### Layer 2 — bank account false positives

The `BankAccountRecognizer` matches any 8–17 digit sequence, which catches order IDs, timestamps, and other numeric identifiers. Options:

1. Lower the confidence threshold — it's already at 0.4 (below the MEDIUM tier of 0.60), so it will show as LOW confidence in the review UI
2. Require field name context — only emit if the field name also suggests financial data
3. Disable entirely — remove from `get_financial_recognizers()`

### Layer 3 — AI model hallucinations

GLiNER can occasionally produce false positives on short, ambiguous values. Mitigations:

1. Set a minimum value length before running Layer 3: skip values shorter than 10 characters for AI analysis
2. Raise the review threshold for AI-only results — treat `source="ai_model"` results with `score < 0.75` as LOW confidence regardless of the raw score
3. Use Layer 3 only for known free-text fields (`is_free_text() == True`)

---

## Deployment options for the classifier service

### Mac laptop (development)

```bash
bash e2e/start_classifier.sh   # starts natively, no Docker
ngrok http 8000                 # expose for Flink scanner
```

### Docker Compose (local / single-node)

```bash
docker-compose up classifier
```

### Azure Container Instances (persistent, low-cost)

```bash
az container create \
  --resource-group myRG \
  --name classifier \
  --image your-registry/classifier-service:latest \
  --cpu 2 --memory 4 \
  --ports 8000 \
  --environment-variables \
    TRANSFORMERS_OFFLINE=0 \
  --dns-name-label my-classifier
```

The public endpoint is `https://my-classifier.{region}.azurecontainer.io:8000`. Use this as `CLASSIFIER_URL` in `scan.sql` and `register.sh`.

GLiNER downloads its model on first start (~500 MB). Mount an Azure File Share to persist the HuggingFace cache across restarts:

```bash
az container create \
  ... \
  --azure-file-volume-account-name STORAGE_ACCOUNT \
  --azure-file-volume-account-key  STORAGE_KEY \
  --azure-file-volume-share-name   hf-cache \
  --azure-file-volume-mount-path   /root/.cache/huggingface
```

### Cloud Run / Fly.io (serverless)

Both work well for the classifier service since it's a stateless HTTP service. The main consideration is cold-start time — GLiNER takes 60–90 seconds to load, so keep minimum instances = 1 to avoid cold starts during scanning sessions.
