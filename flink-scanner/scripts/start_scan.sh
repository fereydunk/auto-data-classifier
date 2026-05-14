#!/usr/bin/env bash
# start_scan.sh — Manage the Flink scanner statements
#
# Usage:
#   ./scripts/start_scan.sh              # start all three long-running statements
#   ./scripts/start_scan.sh --now        # fire a manual scan immediately
#   ./scripts/start_scan.sh --stop       # stop all running statements
#   ./scripts/start_scan.sh --status     # show statement status
#
# Three long-running Flink statements are submitted and run continuously:
#   A — scheduled trigger   (TUMBLE window, fires every SCAN_INTERVAL_MINUTES)
#   B — schema-evolution trigger (schema_watcher UDF, reacts to SR version changes)
#   C — scan driver         (interval join, classifies on every trigger)
#
# Manual scan (--now) submits a one-shot INSERT into the trigger topic.
# The running scan driver picks it up and classifies immediately.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCANNER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${SCANNER_DIR}/scan.env"
SQL_TEMPLATE="${SCANNER_DIR}/sql/scan.sql"

# shellcheck source=/dev/null
[[ -f "${ENV_FILE}" ]] && source "${ENV_FILE}"

: "${CONFLUENT_ENVIRONMENT:?Set CONFLUENT_ENVIRONMENT in scan.env}"
: "${CONFLUENT_COMPUTE_POOL:?Set CONFLUENT_COMPUTE_POOL in scan.env}"
: "${CONFLUENT_CLOUD_REGION:=us-east-1}"
: "${CONFLUENT_CLOUD_PROVIDER:=aws}"
: "${CONFLUENT_KAFKA_CLUSTER:?Set CONFLUENT_KAFKA_CLUSTER in scan.env (Kafka cluster ID, e.g. lkc-xxxxx)}"
: "${SOURCE_TOPIC:?Set SOURCE_TOPIC in scan.env}"
: "${CLASSIFIER_URL:?Set CLASSIFIER_URL in scan.env}"
: "${CLASSIFIER_MAX_LAYER:=3}"
: "${SCAN_INTERVAL_MINUTES:=60}"
: "${SAMPLE_WINDOW_MINUTES:=2}"
: "${SR_URL:?Set SR_URL in scan.env}"
: "${SR_KEY:?Set SR_KEY in scan.env}"
: "${SR_SECRET:?Set SR_SECRET in scan.env}"

# Statement names — predictable so we can stop/describe them later
STMT_A="${SOURCE_TOPIC}-scan-trigger-scheduled"
STMT_B="${SOURCE_TOPIC}-scan-trigger-schema"
STMT_C="${SOURCE_TOPIC}-scan-driver"

MODE=start
for arg in "$@"; do
    case "$arg" in
        --now)    MODE=now ;;
        --stop)   MODE=stop ;;
        --status) MODE=status ;;
        *) echo "Unknown argument: $arg" >&2; exit 1 ;;
    esac
done

# ── Shared flags ──────────────────────────────────────────────────────────────
# flink statement create:  --environment --compute-pool --database
# flink statement describe: --environment --cloud --region
# flink statement delete:  --environment --cloud --region (no --compute-pool!)
# flink statement list:    --environment --cloud --region (no --compute-pool!)
FLINK_FLAGS=(
    --environment   "${CONFLUENT_ENVIRONMENT}"
    --compute-pool  "${CONFLUENT_COMPUTE_POOL}"
    --database      "${CONFLUENT_KAFKA_CLUSTER}"
)
# For describe/list/delete — requires cloud+region, does NOT accept --compute-pool
FLINK_FLAGS_GEO=(
    --environment   "${CONFLUENT_ENVIRONMENT}"
    --cloud         "${CONFLUENT_CLOUD_PROVIDER}"
    --region        "${CONFLUENT_CLOUD_REGION}"
)

# ── Generate the JSON_OBJECT field list from the live SR schema ───────────────
# scan.sql contains a {json_object_fields} placeholder where the per-field
# KEY/VALUE pairs go. We fetch the actual subject schema at submit time so
# the scanner always tracks SR — no hand-edited field lists.
JSON_OBJECT_FIELDS=""
generate_json_object_fields() {
    if [[ -n "${JSON_OBJECT_FIELDS}" ]]; then
        return  # cached for this script run
    fi
    JSON_OBJECT_FIELDS=$(python3 "${SCRIPT_DIR}/generate_json_object.py" \
        "${SR_URL}" "${SR_KEY}" "${SR_SECRET}" "${SOURCE_TOPIC}-value")
    if [[ -z "${JSON_OBJECT_FIELDS}" ]]; then
        echo "ERROR: generate_json_object.py returned empty — refusing to submit a scanner with no fields." >&2
        exit 1
    fi
}

# ── Substitute placeholders in SQL template ───────────────────────────────────
resolve_sql() {
    local block="$1"

    # Only Statement C uses {json_object_fields}; generate lazily so the
    # DDL/Statement A/B paths don't require SR connectivity.
    if echo "${block}" | grep -q "{json_object_fields}"; then
        generate_json_object_fields
    fi

    # Pass everything via env vars + stdin so backticks (in JSON_OBJECT
    # field list) and other shell metachars don't get re-evaluated.
    SQL_BLOCK="${block}" \
    SOURCE_TOPIC="${SOURCE_TOPIC}" \
    SCAN_INTERVAL_MINUTES="${SCAN_INTERVAL_MINUTES}" \
    SAMPLE_WINDOW_MINUTES="${SAMPLE_WINDOW_MINUTES}" \
    CLASSIFIER_URL="${CLASSIFIER_URL}" \
    CLASSIFIER_MAX_LAYER="${CLASSIFIER_MAX_LAYER}" \
    SR_URL="${SR_URL}" \
    SR_KEY="${SR_KEY}" \
    SR_SECRET="${SR_SECRET}" \
    JSON_OBJECT_FIELDS="${JSON_OBJECT_FIELDS}" \
    python3 <<'PYEOF'
import os
block = os.environ["SQL_BLOCK"]
subs = {
    "{source_topic}":          os.environ["SOURCE_TOPIC"],
    "{scan_interval_minutes}": os.environ["SCAN_INTERVAL_MINUTES"],
    "{sample_window_minutes}": os.environ["SAMPLE_WINDOW_MINUTES"],
    "{classifier_url}":        os.environ["CLASSIFIER_URL"],
    "{max_layer}":             os.environ["CLASSIFIER_MAX_LAYER"],
    "{sr_url}":                os.environ["SR_URL"],
    "{sr_key}":                os.environ["SR_KEY"],
    "{sr_secret}":             os.environ["SR_SECRET"],
    "{json_object_fields}":    os.environ["JSON_OBJECT_FIELDS"],
}
for k, v in subs.items():
    block = block.replace(k, v)
print(block)
PYEOF
}

# Extract a named SQL block from the template (between two marker comment lines)
extract_block() {
    local label="$1"
    python3 - "${SQL_TEMPLATE}" "${label}" <<'PYEOF'
import sys, re
path, label = sys.argv[1], sys.argv[2]
lines = open(path).readlines()
inside = False
buf = []
for line in lines:
    if not inside:
        if label in line and line.strip().startswith('--'):
            inside = True
    else:
        # stop at the next comment marker line (-- ── ...)
        if re.match(r"--\s*\u2500\u2500", line) and label not in line:
            break
        buf.append(line)
print("".join(buf).strip())
PYEOF
}

# Submit a single long-running statement. Always replaces the existing one
# (delete + poll-until-gone + create) so the SQL stays in sync with the
# current SR schema. Skip-if-exists left stale SQL in place after a schema
# rebuild AND raced with CC's eventually-consistent describe.
submit_statement() {
    local name="$1"
    local sql="$2"

    # Best-effort delete; ignore failure (statement may not exist).
    confluent flink statement delete "${name}" "${FLINK_FLAGS_GEO[@]}" --force &>/dev/null || true

    # Poll until describe reports the statement is gone — CC's control plane
    # is eventually-consistent and `create` would fail with "already exists"
    # if we proceed too eagerly.
    for _ in $(seq 1 24); do  # up to ~60s
        if ! confluent flink statement describe "${name}" "${FLINK_FLAGS_GEO[@]}" &>/dev/null; then
            break
        fi
        sleep 2.5
    done

    confluent flink statement create "${name}" \
        --sql "${sql}" \
        "${FLINK_FLAGS[@]}"
    echo "  [ok]   ${name} submitted"
}

# ── Actions ───────────────────────────────────────────────────────────────────
case "${MODE}" in

    status)
        echo "Statement status:"
        for name in "${STMT_A}" "${STMT_B}" "${STMT_C}"; do
            STATUS=$(confluent flink statement describe "${name}" \
                "${FLINK_FLAGS_GEO[@]}" --output json 2>/dev/null \
                | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status','NOT FOUND'))" \
                2>/dev/null || echo "NOT FOUND")
            printf "  %-50s %s\n" "${name}" "${STATUS}"
        done
        ;;

    stop)
        echo "Stopping scanner statements..."
        for name in "${STMT_A}" "${STMT_B}" "${STMT_C}"; do
            confluent flink statement delete "${name}" \
                "${FLINK_FLAGS_GEO[@]}" --force 2>/dev/null \
                && echo "  [stopped] ${name}" \
                || echo "  [skip]    ${name} not found"
        done
        echo "Done."
        ;;

    now)
        # Manual trigger: insert one row into the trigger topic.
        # The running scan driver reacts and classifies the last SAMPLE_WINDOW_MINUTES.
        MANUAL_SQL="INSERT INTO \`${SOURCE_TOPIC}-scan-triggers\`
VALUES ('manual', '${SOURCE_TOPIC}', CURRENT_TIMESTAMP);"

        MANUAL_STMT="${SOURCE_TOPIC}-scan-trigger-manual-$(date +%Y%m%d%H%M%S)"
        echo "Firing manual scan trigger..."
        confluent flink statement create "${MANUAL_STMT}" \
            --sql "${MANUAL_SQL}" \
            "${FLINK_FLAGS[@]}"
        echo "  Trigger submitted. The scan driver will classify the last ${SAMPLE_WINDOW_MINUTES} minute(s) of data."
        echo "  Results will appear in: ${SOURCE_TOPIC}-scan-results"
        ;;

    start)
        echo "Scan configuration:"
        echo "  Source topic:    ${SOURCE_TOPIC}"
        echo "  Scan interval:   every ${SCAN_INTERVAL_MINUTES} minute(s)    [scheduled trigger]"
        echo "  Sample window:   last  ${SAMPLE_WINDOW_MINUTES} minute(s) per scan"
        echo "  Schema Registry: ${SR_URL}  [schema-evolution trigger]"
        echo "  Classifier:      ${CLASSIFIER_URL}  (layer ${CLASSIFIER_MAX_LAYER}, via classifier-service connection)"
        echo ""

        # Step 1: create the two Kafka-backed tables (idempotent DDL via statement create)
        echo "Creating trigger and results tables if not exist..."

        DDL_TRIGGERS=$(resolve_sql "CREATE TABLE IF NOT EXISTS \`${SOURCE_TOPIC}-scan-triggers\` (
    \`trigger_type\`  STRING,
    \`source_topic\`  STRING,
    \`triggered_at\`  TIMESTAMP_LTZ(3),
    WATERMARK FOR \`triggered_at\` AS \`triggered_at\` - INTERVAL '10' SECONDS
);")

        DDL_RESULTS=$(resolve_sql "CREATE TABLE IF NOT EXISTS \`${SOURCE_TOPIC}-scan-results\` (
    \`field_path\`    STRING,
    \`tag\`           STRING,
    \`confidence\`    DOUBLE,
    \`layer\`         INT,
    \`source\`        STRING,
    \`example\`       STRING,
    \`source_topic\`  STRING,
    \`trigger_type\`  STRING,
    \`scanned_at\`    TIMESTAMP_LTZ(3)
) WITH (
    'kafka.retention.time' = '604800000'
);")

        STMT_DDL_TRIGGERS="${SOURCE_TOPIC}-ddl-scan-triggers"
        STMT_DDL_RESULTS="${SOURCE_TOPIC}-ddl-scan-results"

        # Submit DDL statements — idempotent (IF NOT EXISTS in SQL)
        # Use a unique name per run so re-runs always succeed
        TS=$(date +%Y%m%d%H%M%S)
        confluent flink statement create "${STMT_DDL_TRIGGERS}-${TS}" \
            --sql "${DDL_TRIGGERS}" \
            "${FLINK_FLAGS[@]}" \
            && echo "  [ok]   scan-triggers table created" \
            || echo "  [warn] scan-triggers DDL failed (table may already exist)"

        confluent flink statement create "${STMT_DDL_RESULTS}-${TS}" \
            --sql "${DDL_RESULTS}" \
            "${FLINK_FLAGS[@]}" \
            && echo "  [ok]   scan-results table created" \
            || echo "  [warn] scan-results DDL failed (table may already exist)"

        echo ""

        # Step 2: submit the three long-running statements
        echo "Submitting Flink statements..."

        STMT_A_SQL=$(resolve_sql "$(extract_block 'Statement A')")
        submit_statement "${STMT_A}" "${STMT_A_SQL}"

        STMT_B_SQL=$(resolve_sql "$(extract_block 'Statement B')")
        submit_statement "${STMT_B}" "${STMT_B_SQL}"

        STMT_C_SQL=$(resolve_sql "$(extract_block 'Statement C')")
        submit_statement "${STMT_C}" "${STMT_C_SQL}"

        echo ""
        echo "All statements running."
        echo ""
        echo "  Statement A — scheduled trigger:       fires every ${SCAN_INTERVAL_MINUTES} min"
        echo "  Statement B — schema-evolution trigger: watches ${SOURCE_TOPIC}-value in SR"
        echo "  Statement C — scan driver:             classifies last ${SAMPLE_WINDOW_MINUTES} min on each trigger"
        echo ""
        echo "Commands:"
        echo "  Check status:    ./scripts/start_scan.sh --status"
        echo "  Manual scan now: ./scripts/start_scan.sh --now"
        echo "  Stop all:        ./scripts/start_scan.sh --stop"
        echo ""
        echo "When ready to review and apply tags:"
        echo "  python flink-scanner/apply_tags.py \\"
        echo "      --topic     ${SOURCE_TOPIC} \\"
        echo "      --sr-url    \$SR_URL --sr-key \$SR_KEY --sr-secret \$SR_SECRET \\"
        echo "      --bootstrap \$KAFKA_BOOTSTRAP \\"
        echo "      --kafka-key \$KAFKA_KEY --kafka-secret \$KAFKA_SECRET"
        ;;
esac
