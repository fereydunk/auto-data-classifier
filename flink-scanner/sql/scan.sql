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
-- ── Network egress constraint ────────────────────────────────────────────────
-- Confluent Cloud Flink compute pools have NO outbound internet access.
-- classify_fields() calls POST /classify on an external URL; those HTTP calls
-- silently time out inside the UDF — Statement C runs but emits zero rows.
-- Statements A and B are unaffected (A uses native Flink; B calls SR internally).
--
-- Workaround: use e2e/local_scanner.py, which runs the same classify → publish
-- logic locally and writes to the same {topic}-scan-results topic.
-- apply_tags.py reads that topic identically regardless of which path wrote it.
--
-- Future — USING CONNECTIONS (Early Access):
-- Once enabled for your org, re-register classify_fields with:
--   CREATE FUNCTION classify_fields
--     AS 'io.confluent.scanner.ClassifyFieldsUDF'
--     USING JAR 'confluent-artifact://<artifact-id>'
--     USING CONNECTIONS (`classifier-service`);
-- The Connection object is created with:
--   confluent flink connection create classifier-service \
--     --type rest --endpoint https://your-classifier.example.com
-- After that, Statement C works end-to-end with no local scanner needed.
-- See flink-scanner/scripts/register.sh for the exact command (commented out).
-- ─────────────────────────────────────────────────────────────────────────────
--
-- Placeholders replaced by start_scan.sh at submit time:
--   {source_topic}           e.g. raw-messages
--   {scan_interval_minutes}  e.g. 60  (2 for testing)
--   {sample_window_minutes}  e.g. 2   (1 for testing)
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
    `triggered_at`  TIMESTAMP_LTZ(3),
    WATERMARK FOR `triggered_at` AS `triggered_at` - INTERVAL '10' SECONDS
);

-- Results topic — one row per classifier result per message.
-- Append-only: no GROUP BY aggregation in Flink, no watermark deadlock.
-- apply_tags.py post-processes for max confidence per (field_path, tag).
CREATE TABLE IF NOT EXISTS `{source_topic}-scan-results` (
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
    ROUND(c.score, 2)           AS confidence,
    c.layer,
    c.source,
    c.sample_value              AS example,
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
            -- Reconstruct the message as a JSON string from the Avro columns.
            -- Null fields are included as JSON null; the classifier skips them.
            JSON_OBJECT(
                KEY 'customer_id'         VALUE p.`customer_id`,
                KEY 'first_name'          VALUE p.`first_name`,
                KEY 'last_name'           VALUE p.`last_name`,
                KEY 'email'               VALUE p.`email`,
                KEY 'phone_number'        VALUE p.`phone_number`,
                KEY 'date_of_birth'       VALUE p.`date_of_birth`,
                KEY 'ip_address'          VALUE p.`ip_address`,
                KEY 'status'              VALUE p.`status`,
                KEY 'transaction_id'      VALUE p.`transaction_id`,
                KEY 'credit_card_number'  VALUE p.`credit_card_number`,
                KEY 'iban'                VALUE p.`iban`,
                KEY 'routing_number'      VALUE p.`routing_number`,
                KEY 'account_number'      VALUE p.`account_number`,
                KEY 'currency'            VALUE p.`currency`,
                KEY 'patient_id'          VALUE p.`patient_id`,
                KEY 'mrn'                 VALUE p.`mrn`,
                KEY 'diagnosis'           VALUE p.`diagnosis`,
                KEY 'medication'          VALUE p.`medication`,
                KEY 'npi_number'          VALUE p.`npi_number`,
                KEY 'insurance_id'        VALUE p.`insurance_id`,
                KEY 'comment'             VALUE p.`comment`,
                KEY 'applicant_name'      VALUE p.`applicant_name`,
                KEY 'ssn'                 VALUE p.`ssn`,
                KEY 'passport_number'     VALUE p.`passport_number`,
                KEY 'driver_license'      VALUE p.`driver_license`,
                KEY 'nationality'         VALUE p.`nationality`,
                KEY 'service'             VALUE p.`service`,
                KEY 'username'            VALUE p.`username`,
                KEY 'password'            VALUE p.`password`,
                KEY 'api_key'             VALUE p.`api_key`,
                KEY 'connection_string'   VALUE p.`connection_string`,
                KEY 'sample_id'           VALUE p.`sample_id`,
                KEY 'dna_sequence'        VALUE p.`dna_sequence`,
                KEY 'genome'              VALUE p.`genome`,
                KEY 'fingerprint'         VALUE p.`fingerprint`,
                KEY 'facial_recognition'  VALUE p.`facial_recognition`,
                KEY 'child_id'            VALUE p.`child_id`,
                KEY 'guardian_email'      VALUE p.`guardian_email`,
                KEY 'minor_data'          VALUE p.`minor_data`,
                KEY 'deal_id'             VALUE p.`deal_id`,
                KEY 'mnpi'                VALUE p.`mnpi`,
                KEY 'notes'               VALUE p.`notes`,
                KEY 'order_id'            VALUE p.`order_id`,
                KEY 'cust_first_name'     VALUE p.`cust_first_name`,
                KEY 'cust_last_name'      VALUE p.`cust_last_name`,
                KEY 'cust_email'          VALUE p.`cust_email`,
                KEY 'cust_phone'          VALUE p.`cust_phone`,
                KEY 'payment_cc_number'   VALUE p.`payment_cc_number`,
                KEY 'billing_street'      VALUE p.`billing_street`,
                KEY 'billing_city'        VALUE p.`billing_city`,
                KEY 'billing_postal_code' VALUE p.`billing_postal_code`
            )
        )
    ) AS c;


-- ── Statement D: Manual trigger (one-shot) ────────────────────────────────────
-- Submitted by start_scan.sh --now.
-- Inserts a single row into the trigger topic; the scan driver reacts immediately.
-- This statement is NOT part of the long-running job — it is submitted separately.

-- INSERT INTO `{source_topic}-scan-triggers`
-- VALUES ('manual', '{source_topic}', CURRENT_TIMESTAMP);
