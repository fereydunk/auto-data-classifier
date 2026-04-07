-- ============================================================
-- Flink SQL Data Scanner  —  Interactive Topic Profiler
-- ============================================================
--
-- WORKFLOW
--   1. Edit the ── Configuration ── block below.
--   2. Run STATEMENT A.  Flink accumulates results for N minutes.
--   3. Stop the query manually once the results look representative.
--   4. Review the output table.
--   5. Edit the VALUES block in STATEMENT B with the tags you approve.
--   6. Run STATEMENT B.  Tags are written to the Stream Catalog.
--
-- The only thing that changes between topics is the configuration
-- block and the VALUES list in Statement B.
-- ============================================================


-- ── Configuration ────────────────────────────────────────────────────────────
-- Edit these before running. Keep the quotes.

-- Classifier service (needs a public HTTPS endpoint, not localhost)
-- For local dev: expose with  ngrok http 8000  and use the ngrok URL.
SET 'classifier.url'   = 'https://your-classifier.example.com';
SET 'classifier.layer' = '3';         -- 1 = field name only | 2 = +regex | 3 = +AI

-- Your Confluent Schema Registry / Stream Catalog credentials
SET 'sr.url'           = 'https://psrc-xxxxx.us-east-1.aws.confluent.cloud';
SET 'sr.api.key'       = 'YOUR_SR_API_KEY';
SET 'sr.api.secret'    = 'YOUR_SR_API_SECRET';
SET 'sr.cluster.id'    = 'lsrc-xxxxx';

-- Topic to profile — must be visible in the current Flink catalog
SET 'source.topic'     = 'payments';

-- How many minutes of data to sample.
-- Flink reads from the topic right now; stop the query after N minutes.
SET 'sample.minutes'   = '2';

-- ── STATEMENT A: Sample + Classify ───────────────────────────────────────────
-- Run this statement.  Results appear in the panel as messages arrive.
-- After ~N minutes (or once you see enough unique fields), stop the query.
--
-- Output columns:
--   field_path   dot-notation path to the field, e.g. "customer.email"
--   tag          detected data tag: PII | PHI | PCI | CREDENTIALS | FINANCIAL |
--                  GOVERNMENT_ID | BIOMETRIC | GENETIC | NPI | LOCATION | MINOR
--   confidence   highest score across all detections for this field+tag pair
--   layer        which layer first found it: 1=field name, 2=regex, 3=AI
--   source       "field_name" | "regex" | "ai_model"
--   example      a short snippet from the field value (truncated to 50 chars)

SELECT
    field_path,
    tag,
    ROUND(MAX(score), 2)    AS confidence,
    MIN(layer)              AS layer,
    ANY_VALUE(source)       AS source,
    ANY_VALUE(sample_value) AS example
FROM
    `payments`,                                 -- ← replace with ${source.topic}
    LATERAL TABLE(
        classify_fields(
            'https://your-classifier.example.com',  -- ← ${classifier.url}
            3,                                       -- ← ${classifier.layer}
            CAST(`$value` AS STRING)                 -- raw JSON message value
        )
    )
-- Time-bound the sample to the last N minutes.
-- On first run you may want to remove this line to see all available data.
WHERE `$rowtime` >= CURRENT_TIMESTAMP - INTERVAL '2' MINUTES  -- ← ${sample.minutes}
GROUP BY
    field_path,
    tag
ORDER BY
    confidence DESC;

-- ── Reading the results ───────────────────────────────────────────────────────
--
--   field_path          tag          confidence  layer  source      example
--   ─────────────────────────────────────────────────────────────────────────
--   customer.email      PII          0.97        1      field_name  alice@ex…
--   card.number         PCI          0.99        2      regex       4111111…
--   customer.dob        PII          0.91        1      field_name  (empty)
--   routing_number      FINANCIAL    0.88        2      regex       02100002…
--   notes               PII          0.74        3      ai_model    She repo…
--   ref_code            (no match)
--
-- HIGH confidence (≥ 0.85): safe to bulk-approve
-- MEDIUM (0.60–0.84):       review individually
-- LOW (< 0.60):             inspect the example snippet before approving
--
-- ── STATEMENT B: Approve + Apply ─────────────────────────────────────────────
-- After reviewing the output above, fill in the VALUES block with the
-- (subject, field_path, tag) triples you want to approve.
-- Then run this statement — each row shows "OK: ..." or "ERROR: ..." live.

SELECT
    apply_tag(
        'https://psrc-xxxxx.us-east-1.aws.confluent.cloud',  -- ← ${sr.url}
        'YOUR_SR_API_KEY',                                    -- ← ${sr.api.key}
        'YOUR_SR_API_SECRET',                                 -- ← ${sr.api.secret}
        'lsrc-xxxxx',                                         -- ← ${sr.cluster.id}
        subject,
        field_path,
        tag
    ) AS result
FROM (VALUES
    --  subject               field_path            tag
    ('payments-value',  'customer.email',    'PII'),
    ('payments-value',  'card.number',       'PCI'),
    ('payments-value',  'customer.dob',      'PII'),
    ('payments-value',  'routing_number',    'FINANCIAL')
    -- add / remove rows here
) AS approvals(subject, field_path, tag);

-- ── Bulk-approve shortcut ─────────────────────────────────────────────────────
-- If you trust all HIGH-confidence results (score ≥ 0.85), skip editing the
-- VALUES block and run this instead.  It re-classifies the same window and
-- immediately applies every tag with confidence ≥ 0.85.
--
-- USE WITH CARE — review Statement A results first.

-- SELECT
--     apply_tag(
--         'https://psrc-xxxxx.us-east-1.aws.confluent.cloud',
--         'YOUR_SR_API_KEY',
--         'YOUR_SR_API_SECRET',
--         'lsrc-xxxxx',
--         CONCAT('payments', '-value'),
--         field_path,
--         tag
--     ) AS result
-- FROM
--     `payments`,
--     LATERAL TABLE(
--         classify_fields(
--             'https://your-classifier.example.com',
--             3,
--             CAST(`$value` AS STRING)
--         )
--     )
-- WHERE `$rowtime` >= CURRENT_TIMESTAMP - INTERVAL '2' MINUTES
--   AND score >= 0.85
-- GROUP BY field_path, tag
-- HAVING MAX(score) >= 0.85;
