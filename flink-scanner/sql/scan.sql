-- ============================================================
-- Flink SQL Data Scanner  —  Three-Trigger Architecture
-- ============================================================
--
-- Four statements, submitted once via start_scan.sh.
-- All run continuously in Confluent Cloud Flink.
--
-- Trigger 1 — Scheduled:        TUMBLE window emits a trigger every N minutes.
-- Trigger 2 — Schema evolution:  schema_watcher() UDF detects new SR version.
-- Trigger 3 — Manual:           one-shot INSERT into the trigger topic.
--
-- All three write to {topic}-scan-triggers.
-- One unified scan driver reads every trigger, classifies the last M minutes
-- of source data, and writes results to {topic}-scan-results.
--
-- Placeholders replaced by start_scan.sh at submit time:
--   {source_topic}           e.g. payments
--   {scan_interval_minutes}  e.g. 60
--   {sample_window_minutes}  e.g. 2
--   {classifier_url}         e.g. https://classifier.example.com
--   {max_layer}              e.g. 3
--   {sr_url}                 e.g. https://psrc-xxx.confluent.cloud
--   {sr_key}                 e.g. SR API key
--   {sr_secret}              e.g. SR API secret
-- ============================================================


-- ── Step 1: create supporting tables (run once) ───────────────────────────────

-- Trigger topic — receives events from all three trigger sources.
-- The scan driver reacts to every row written here.
CREATE TABLE IF NOT EXISTS `{source_topic}-scan-triggers` (
    `trigger_type`  STRING,                   -- 'scheduled' | 'schema_evolution' | 'manual'
    `source_topic`  STRING,
    `triggered_at`  TIMESTAMP(3),
    WATERMARK FOR `triggered_at` AS `triggered_at` - INTERVAL '10' SECONDS
);

-- Results topic — receives classification output from the scan driver.
-- apply_tags.py reads the latest batch from here.
CREATE TABLE IF NOT EXISTS `{source_topic}-scan-results` (
    `field_path`    STRING,
    `tag`           STRING,
    `confidence`    DOUBLE,
    `layer`         INT,
    `source`        STRING,
    `example`       STRING,
    `source_topic`  STRING,
    `trigger_type`  STRING,
    `scanned_at`    TIMESTAMP(3)
) WITH (
    'kafka.retention.ms' = '604800000'   -- 7 days
);


-- ── Statement A: Scheduled trigger ───────────────────────────────────────────
-- Emits one trigger row at the close of every TUMBLE window.
-- The window size IS the scan interval.

INSERT INTO `{source_topic}-scan-triggers`
SELECT
    'scheduled'                 AS trigger_type,
    '{source_topic}'            AS source_topic,
    MAX(window_end)             AS triggered_at
FROM TABLE(
    TUMBLE(
        TABLE `{source_topic}`,
        DESCRIPTOR(`$rowtime`),
        INTERVAL '{scan_interval_minutes}' MINUTES
    )
)
GROUP BY window_start, window_end;


-- ── Statement B: Schema-evolution trigger ─────────────────────────────────────
-- schema_watcher() is called for each incoming message on the source topic.
-- The UDF rate-limits SR checks to once per minute and only emits a row
-- when the schema version has increased since the last check.

INSERT INTO `{source_topic}-scan-triggers`
SELECT
    'schema_evolution'          AS trigger_type,
    '{source_topic}'            AS source_topic,
    triggered_at
FROM
    `{source_topic}`,
    LATERAL TABLE(
        schema_watcher(
            '{sr_url}',
            '{sr_key}',
            '{sr_secret}',
            '{source_topic}-value'
        )
    );


-- ── Statement C: Unified scan driver ─────────────────────────────────────────
-- Reacts to every trigger in the trigger topic.
-- For each trigger, interval-joins with the source topic to fetch messages
-- from the last SAMPLE_WINDOW_MINUTES, classifies them, and writes results.
--
-- The interval join condition:
--   p.$rowtime BETWEEN t.triggered_at - INTERVAL 'M' MINUTES AND t.triggered_at
-- pulls only the messages that arrived in the sample window before the trigger.

INSERT INTO `{source_topic}-scan-results`
SELECT
    c.field_path,
    c.tag,
    ROUND(MAX(c.score), 2)      AS confidence,
    MIN(c.layer)                AS layer,
    ANY_VALUE(c.source)         AS source,
    ANY_VALUE(c.sample_value)   AS example,
    t.source_topic,
    t.trigger_type,
    t.triggered_at              AS scanned_at
FROM
    `{source_topic}-scan-triggers` AS t
JOIN
    `{source_topic}` AS p
    ON p.`$rowtime` BETWEEN t.triggered_at - INTERVAL '{sample_window_minutes}' MINUTES
                        AND t.triggered_at,
    LATERAL TABLE(
        classify_fields(
            '{classifier_url}',
            {max_layer},
            CAST(p.`$value` AS STRING)
        )
    ) AS c
GROUP BY
    c.field_path,
    c.tag,
    t.trigger_type,
    t.triggered_at,
    t.source_topic;


-- ── Statement D: Manual trigger (one-shot) ────────────────────────────────────
-- Submitted by start_scan.sh --now.
-- Inserts a single row into the trigger topic; the scan driver reacts immediately.
-- This statement is NOT part of the long-running job — it is submitted separately.

-- INSERT INTO `{source_topic}-scan-triggers`
-- VALUES ('manual', '{source_topic}', CURRENT_TIMESTAMP);
