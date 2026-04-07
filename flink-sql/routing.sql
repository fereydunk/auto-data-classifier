-- ============================================================
-- Confluent Cloud Flink SQL — message routing queries
--
-- These queries run as continuous streaming jobs alongside the
-- kafka-pipeline service.  They provide SQL-native alternatives
-- or complements to the Python pipeline routing logic.
--
-- Prerequisites:
--   - classify_fields() and apply_tag() UDFs registered
--     (see flink-scanner/scripts/register.sh)
--   - Source topic exists in the Confluent catalog
-- ============================================================


-- ── 1. Continuous classification view ────────────────────────────────────────
-- Shows every incoming message with its detected tags in real time.
-- Run this to monitor ongoing classification without the pipeline.

SELECT
    `$rowtime`                          AS event_time,
    field_path,
    tag,
    ROUND(score, 2)                     AS confidence,
    layer,
    source,
    sample_value
FROM
    `raw-messages`,
    LATERAL TABLE(
        classify_fields(
            'https://your-classifier.example.com',  -- CLASSIFIER_URL
            3,                                       -- max_layer
            CAST(`$value` AS STRING)
        )
    )
ORDER BY event_time DESC, confidence DESC;


-- ── 2. Sink: route classified messages to output topics ───────────────────────
-- Alternative to the Python kafka-pipeline for pure SQL environments.
-- Creates a streaming INSERT that continuously routes messages.

-- First, ensure the output topics exist as Flink tables.
-- (In Confluent Cloud, topics in the catalog are auto-available as tables.)

INSERT INTO `classified-messages`
SELECT
    `$key`          AS `key`,
    CAST(MAP[
        'payload',        CAST(`$value` AS STRING),
        'tags',           LISTAGG(tag, ','),
        'classified_at',  CAST(CURRENT_TIMESTAMP AS STRING)
    ] AS STRING)    AS `value`
FROM
    `raw-messages`,
    LATERAL TABLE(
        classify_fields(
            'https://your-classifier.example.com',
            3,
            CAST(`$value` AS STRING)
        )
    )
GROUP BY `$key`, `$value`;


-- ── 3. Field-level tag summary across a topic ─────────────────────────────────
-- Aggregate: for each unique field path, what tag does it most commonly get?
-- Useful for understanding the data profile of a topic over time.

SELECT
    field_path,
    tag,
    COUNT(*)                AS detections,
    ROUND(AVG(score), 3)    AS avg_confidence,
    MIN(layer)              AS earliest_layer
FROM
    `raw-messages`,
    LATERAL TABLE(
        classify_fields(
            'https://your-classifier.example.com',
            3,
            CAST(`$value` AS STRING)
        )
    )
GROUP BY field_path, tag
ORDER BY detections DESC;


-- ── 4. Alert: high-confidence sensitive fields in unexpected topics ────────────
-- Detect PHI or CREDENTIALS appearing in topics that should be clean.

SELECT
    `$rowtime`      AS detected_at,
    field_path,
    tag,
    ROUND(score, 2) AS confidence,
    sample_value
FROM
    `raw-messages`,        -- ← the topic that should NOT have PHI/CREDENTIALS
    LATERAL TABLE(
        classify_fields(
            'https://your-classifier.example.com',
            3,
            CAST(`$value` AS STRING)
        )
    )
WHERE
    tag IN ('PHI', 'CREDENTIALS', 'GOVERNMENT_ID', 'GENETIC')
    AND score >= 0.85;


-- ── 5. Interactive scanner — see flink-scanner/sql/scan.sql ──────────────────
-- For the full interactive workflow (sample N minutes → review → approve tags),
-- use the dedicated scanner template at:
--   flink-scanner/sql/scan.sql
