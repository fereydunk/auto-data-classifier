#!/usr/bin/env bash
# Register the scanner UDFs in Confluent Cloud Flink.
#
# Prerequisites:
#   1. Confluent CLI installed and logged in:      confluent login
#   2. Environment and compute pool set in scan.env (or exported):
#        export CONFLUENT_ENVIRONMENT=env-xxxxx
#        export CONFLUENT_COMPUTE_POOL=lfcp-xxxxx
#   3. JAR built:  ./scripts/build.sh
#
# After running this script, the three functions are available in every
# Flink SQL statement in this environment:
#   classify_fields(classifier_url, max_layer, message)  → TABLE
#   schema_watcher(sr_url, sr_key, sr_secret, subject)   → TABLE
#   apply_tag(sr_url, sr_key, sr_secret, cluster_id, subject, field, tag)  → STRING
#
# ── Network egress note ────────────────────────────────────────────────────────
# classify_fields calls POST /classify on an external URL.
# Confluent Cloud Flink compute pools have NO outbound internet access, so
# classify_fields will silently time out and emit no rows in Statement C.
# Use e2e/local_scanner.py as the classification path instead.
#
# ── Future: USING CONNECTIONS (Early Access) ──────────────────────────────────
# Once the USING CONNECTIONS feature is enabled for your org, replace the
# classify_fields registration below with:
#
#   confluent flink connection create classifier-service \
#       --type rest \
#       --endpoint https://your-classifier.example.com \
#       --environment "$CONFLUENT_ENVIRONMENT" \
#       --cloud "$CONFLUENT_CLOUD_PROVIDER" \
#       --region "$CONFLUENT_CLOUD_REGION"
#
#   confluent flink statement create "reg-classify_fields-${TS}" \
#       --sql "CREATE FUNCTION classify_fields
#     AS 'io.confluent.scanner.ClassifyFieldsUDF'
#     USING JAR 'confluent-artifact://${ARTIFACT_ID}'
#     USING CONNECTIONS (\`classifier-service\`);" \
#       --environment "$CONFLUENT_ENVIRONMENT" \
#       --compute-pool "$CONFLUENT_COMPUTE_POOL" \
#       --database "$CONFLUENT_KAFKA_CLUSTER"
#
# After that, Statement C works end-to-end without local_scanner.py.
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="$SCRIPT_DIR/../target/flink-scanner-udf-1.0.0.jar"

: "${CONFLUENT_ENVIRONMENT:?Set CONFLUENT_ENVIRONMENT (e.g. env-xxxxx)}"
: "${CONFLUENT_COMPUTE_POOL:?Set CONFLUENT_COMPUTE_POOL (e.g. lfcp-xxxxx)}"
: "${CONFLUENT_CLOUD_REGION:=us-east-1}"
: "${CONFLUENT_CLOUD_PROVIDER:=aws}"

if [[ ! -f "$JAR" ]]; then
    echo "ERROR: JAR not found. Run ./scripts/build.sh first." >&2
    exit 1
fi

ARTIFACT_NAME="flink-scanner-udf"

echo "Uploading artifact to Confluent Cloud..."
ARTIFACT_JSON=$(confluent flink artifact create "$ARTIFACT_NAME" \
    --artifact-file "$JAR" \
    --cloud "$CONFLUENT_CLOUD_PROVIDER" \
    --region "$CONFLUENT_CLOUD_REGION" \
    --environment "$CONFLUENT_ENVIRONMENT" \
    --output json)

ARTIFACT_ID=$(echo "$ARTIFACT_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "Artifact uploaded: $ARTIFACT_ID"

echo "Registering UDFs..."
TS=$(date +%Y%m%d%H%M%S)
for FUNC_PAIR in \
    "classify_fields:io.confluent.scanner.ClassifyFieldsUDF" \
    "schema_watcher:io.confluent.scanner.SchemaWatcherUDF" \
    "apply_tag:io.confluent.scanner.ApplyTagUDF"; do

    FUNC_NAME="${FUNC_PAIR%%:*}"
    CLASS_NAME="${FUNC_PAIR##*:}"

    confluent flink statement create "reg-${FUNC_NAME}-${TS}" \
        --sql "CREATE FUNCTION IF NOT EXISTS ${FUNC_NAME}
    AS '${CLASS_NAME}'
    USING JAR 'confluent-artifact://${ARTIFACT_ID}';" \
        --environment "$CONFLUENT_ENVIRONMENT" \
        --compute-pool "$CONFLUENT_COMPUTE_POOL" \
        --database "$CONFLUENT_KAFKA_CLUSTER" \
        > /dev/null \
        && echo "  [ok]   ${FUNC_NAME}" \
        || echo "  [warn] ${FUNC_NAME} may already exist"
done

echo ""
echo "Done. UDFs registered:"
echo "  classify_fields(classifier_url STRING, max_layer INT, message STRING)"
echo "  schema_watcher(sr_url STRING, sr_key STRING, sr_secret STRING, subject STRING)"
echo "  apply_tag(sr_url, sr_key, sr_secret, cluster_id, subject, field_path, tag)"
echo ""
echo "NOTE: classify_fields HTTP calls will silently time out in Statement C until"
echo "      USING CONNECTIONS (EA) is enabled for your org."
echo "      Use e2e/local_scanner.py as the classification path in the meantime."
echo "      See the USING CONNECTIONS registration example at the top of this script."
echo ""
echo "Run ./scripts/start_scan.sh to start all three trigger statements."
