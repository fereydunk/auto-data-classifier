-- =============================================================================
-- Auto Data Classifier — Confluent Cloud Flink SQL Routing
-- Run these statements in Confluent Cloud's Flink SQL workspace.
-- Replace <ENVIRONMENT_ID> and <CLUSTER_ID> with your actual values.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Source table — classified messages produced by the kafka-pipeline
-- ---------------------------------------------------------------------------
CREATE TABLE classified_messages (
    `payload`           STRING,
    `classification`    ROW<
        sensitivity_level   STRING,
        detected_entities   STRING,   -- JSON blob
        classified_at       STRING,
        classifier_version  STRING
    >,
    `event_time`        TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '10' SECOND
) WITH (
    'connector'                     = 'confluent',
    'kafka.topic'                   = 'classified-pii',
    'value.format'                  = 'json',
    'value.json.ignore-parse-errors'= 'true'
);

-- ---------------------------------------------------------------------------
-- 2. Sink — HIGH sensitivity (PII) topic
-- ---------------------------------------------------------------------------
CREATE TABLE sink_pii (
    `payload`        STRING,
    `classification` STRING,
    `routed_at`      TIMESTAMP_LTZ(3)
) WITH (
    'connector'    = 'confluent',
    'kafka.topic'  = 'classified-pii-confirmed',
    'value.format' = 'json'
);

-- ---------------------------------------------------------------------------
-- 3. Sink — MEDIUM sensitivity topic
-- ---------------------------------------------------------------------------
CREATE TABLE sink_medium (
    `payload`        STRING,
    `classification` STRING,
    `routed_at`      TIMESTAMP_LTZ(3)
) WITH (
    'connector'    = 'confluent',
    'kafka.topic'  = 'classified-medium-confirmed',
    'value.format' = 'json'
);

-- ---------------------------------------------------------------------------
-- 4. Sink — CLEAN / safe topic
-- ---------------------------------------------------------------------------
CREATE TABLE sink_safe (
    `payload`        STRING,
    `classification` STRING,
    `routed_at`      TIMESTAMP_LTZ(3)
) WITH (
    'connector'    = 'confluent',
    'kafka.topic'  = 'classified-safe-confirmed',
    'value.format' = 'json'
);

-- ---------------------------------------------------------------------------
-- 5. Routing statements
-- ---------------------------------------------------------------------------

-- HIGH sensitivity → PII sink
INSERT INTO sink_pii
SELECT
    `payload`,
    CAST(`classification` AS STRING),
    CURRENT_TIMESTAMP
FROM classified_messages
WHERE `classification`.sensitivity_level = 'HIGH';

-- MEDIUM sensitivity → medium sink
INSERT INTO sink_medium
SELECT
    `payload`,
    CAST(`classification` AS STRING),
    CURRENT_TIMESTAMP
FROM classified_messages
WHERE `classification`.sensitivity_level = 'MEDIUM';

-- LOW / CLEAN → safe sink
INSERT INTO sink_safe
SELECT
    `payload`,
    CAST(`classification` AS STRING),
    CURRENT_TIMESTAMP
FROM classified_messages
WHERE `classification`.sensitivity_level IN ('LOW', 'CLEAN');

-- ---------------------------------------------------------------------------
-- 6. Optional: alert view — HIGH sensitivity in the last 5 minutes
-- ---------------------------------------------------------------------------
SELECT
    TUMBLE_START(`event_time`, INTERVAL '5' MINUTE) AS window_start,
    COUNT(*)                                          AS pii_message_count,
    `classification`.classifier_version               AS classifier_version
FROM classified_messages
WHERE `classification`.sensitivity_level = 'HIGH'
GROUP BY
    TUMBLE(`event_time`, INTERVAL '5' MINUTE),
    `classification`.classifier_version;
