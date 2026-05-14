#!/usr/bin/env bash
# Register the scanner UDFs in Confluent Cloud Flink.
#
# Prerequisites:
#   1. Confluent CLI installed and logged in:      confluent login
#   2. Environment and compute pool set in scan.env (or exported):
#        export CONFLUENT_ENVIRONMENT=env-xxxxx
#        export CONFLUENT_COMPUTE_POOL=lfcp-xxxxx
#   3. 'classifier-service' Flink connection exists pointing to the classifier endpoint:
#        confluent flink connection create classifier-service \
#            --type rest \
#            --endpoint https://your-classifier.example.com \
#            --environment "$CONFLUENT_ENVIRONMENT" \
#            --cloud "$CONFLUENT_CLOUD_PROVIDER" \
#            --region "$CONFLUENT_CLOUD_REGION"
#   4. JAR built:  ./scripts/build.sh
#
# After running this script, the three functions are available in every
# Flink SQL statement in this environment:
#   classify_fields(max_layer, message)                              → TABLE
#   schema_watcher(sr_url, sr_key, sr_secret, subject)              → TABLE
#   apply_tag(sr_url, sr_key, sr_secret, cluster_id, subject, field, tag)  → STRING

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

# Artifact names must be unique per cloud/region/environment — use a timestamp suffix.
ARTIFACT_NAME="flink-scanner-udf-$(date +%Y%m%d%H%M%S)"

echo "Uploading artifact to Confluent Cloud..."
ARTIFACT_JSON=$(confluent flink artifact create "$ARTIFACT_NAME" \
    --artifact-file "$JAR" \
    --cloud "$CONFLUENT_CLOUD_PROVIDER" \
    --region "$CONFLUENT_CLOUD_REGION" \
    --environment "$CONFLUENT_ENVIRONMENT" \
    --output json)

ARTIFACT_ID=$(echo "$ARTIFACT_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "Artifact uploaded: $ARTIFACT_ID"

echo "Dropping existing UDFs (required before re-create)..."
TS=$(date +%Y%m%d%H%M%S)
for func_stmt in "classify-fields" "schema-watcher" "apply-tag"; do
    sql_func="${func_stmt//-/_}"
    confluent flink statement create "drop-${func_stmt}-${TS}" \
        --sql "DROP FUNCTION IF EXISTS ${sql_func};" \
        --environment "$CONFLUENT_ENVIRONMENT" \
        --compute-pool "$CONFLUENT_COMPUTE_POOL" \
        --database "$CONFLUENT_KAFKA_CLUSTER" \
        > /dev/null \
        && echo "  [ok]   dropped ${sql_func}" \
        || echo "  [warn] could not drop ${sql_func}"
done

# Brief pause for drops to propagate before re-creating
sleep 5

echo "Registering UDFs..."
TS=$(date +%Y%m%d%H%M%S)

# classify_fields uses USING CONNECTIONS so it can reach the classifier service over the internet
confluent flink statement create "reg-classify-fields-${TS}" \
    --sql "CREATE FUNCTION classify_fields
  AS 'io.confluent.scanner.ClassifyFieldsUDF'
  USING JAR 'confluent-artifact://${ARTIFACT_ID}'
  USING CONNECTIONS (\`classifier-service\`);" \
    --environment "$CONFLUENT_ENVIRONMENT" \
    --compute-pool "$CONFLUENT_COMPUTE_POOL" \
    --database "$CONFLUENT_KAFKA_CLUSTER" \
    > /dev/null \
    && echo "  [ok]   classify_fields" \
    || echo "  [warn] classify_fields registration failed"

# schema_watcher and apply_tag call Confluent-internal endpoints — no connection needed
for FUNC_PAIR in \
    "schema-watcher:schema_watcher:io.confluent.scanner.SchemaWatcherUDF" \
    "apply-tag:apply_tag:io.confluent.scanner.ApplyTagUDF"; do

    STMT_SUFFIX="${FUNC_PAIR%%:*}"
    FUNC_NAME="${FUNC_PAIR#*:}"; FUNC_NAME="${FUNC_NAME%%:*}"
    CLASS_NAME="${FUNC_PAIR##*:}"

    confluent flink statement create "reg-${STMT_SUFFIX}-${TS}" \
        --sql "CREATE FUNCTION ${FUNC_NAME}
  AS '${CLASS_NAME}'
  USING JAR 'confluent-artifact://${ARTIFACT_ID}';" \
        --environment "$CONFLUENT_ENVIRONMENT" \
        --compute-pool "$CONFLUENT_COMPUTE_POOL" \
        --database "$CONFLUENT_KAFKA_CLUSTER" \
        > /dev/null \
        && echo "  [ok]   ${FUNC_NAME}" \
        || echo "  [warn] ${FUNC_NAME} registration failed"
done

echo ""
echo "Done. UDFs registered:"
echo "  classify_fields(max_layer INT, message STRING)  [via classifier-service connection]"
echo "  schema_watcher(sr_url STRING, sr_key STRING, sr_secret STRING, subject STRING)"
echo "  apply_tag(sr_url, sr_key, sr_secret, cluster_id, subject, field_path, tag)"
echo ""
echo "Run ./scripts/start_scan.sh to start all three trigger statements."
