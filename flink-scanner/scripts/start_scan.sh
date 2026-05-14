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
FLINK_FLAGS=(
    --environment   "${CONFLUENT_ENVIRONMENT}"
    --compute-pool  "${CONFLUENT_COMPUTE_POOL}"
    --cloud         "${CONFLUENT_CLOUD_PROVIDER}"
    --region        "${CONFLUENT_CLOUD_REGION}"
)

# ── Substitute placeholders in SQL template ───────────────────────────────────
resolve_sql() {
    local block="$1"
    echo "${block}" \
        | sed "s|{source_topic}|${SOURCE_TOPIC}|g" \
        | sed "s|{scan_interval_minutes}|${SCAN_INTERVAL_MINUTES}|g" \
        | sed "s|{sample_window_minutes}|${SAMPLE_WINDOW_MINUTES}|g" \
        | sed "s|{classifier_url}|${CLASSIFIER_URL}|g" \
        | sed "s|{max_layer}|${CLASSIFIER_MAX_LAYER}|g" \
        | sed "s|{sr_url}|${SR_URL}|g" \
        | sed "s|{sr_key}|${SR_KEY}|g" \
        | sed "s|{sr_secret}|${SR_SECRET}|g"
}

# Extract a named SQL block from the template (between two ── markers)
extract_block() {
    local label="$1"
    sed -n "/── ${label}/,/── /{ /── ${label}/d; /── /q; p }" "${SQL_TEMPLATE}"
}

# Submit a single long-running statement (idempotent — skips if already running)
submit_statement() {
    local name="$1"
    local sql="$2"

    if confluent flink statement describe "${name}" "${FLINK_FLAGS[@]}" &>/dev/null; then
        echo "  [skip] ${name} already running"
        return
    fi

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
                "${FLINK_FLAGS[@]}" --output json 2>/dev/null \
                | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status',{}).get('phase','NOT FOUND'))" \
                2>/dev/null || echo "NOT FOUND")
            printf "  %-50s %s\n" "${name}" "${STATUS}"
        done
        ;;

    stop)
        echo "Stopping scanner statements..."
        for name in "${STMT_A}" "${STMT_B}" "${STMT_C}"; do
            confluent flink statement delete "${name}" \
                "${FLINK_FLAGS[@]}" --force 2>/dev/null \
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
        echo "  Classifier:      ${CLASSIFIER_URL}  (layer ${CLASSIFIER_MAX_LAYER})"
        echo ""

        # Step 1: create the two Kafka-backed tables (idempotent)
        echo "Creating trigger and results tables if not exist..."
        SETUP_SQL=$(resolve_sql "$(grep -A 40 'Step 1' "${SQL_TEMPLATE}" | \
            sed -n '/CREATE TABLE/,/^);$/p' | head -60)")
        confluent flink shell "${FLINK_FLAGS[@]}" <<< "${SETUP_SQL}"
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
