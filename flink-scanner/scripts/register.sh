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
# After running this script, the two functions are available in every
# Flink SQL statement in this environment:
#   classify_fields(max_layer, message)                              → TABLE
#   schema_watcher(sr_url, sr_key, sr_secret, subject)              → TABLE
#
# (Tag application happens server-side via review-api/catalog_client.py,
# triggered by the human reviewer clicking Submit. The previous apply_tag
# UDF was never invoked from any SQL and was removed.)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="$SCRIPT_DIR/../target/flink-scanner-udf-1.0.0.jar"

: "${CONFLUENT_ENVIRONMENT:?Set CONFLUENT_ENVIRONMENT (e.g. env-xxxxx)}"
: "${CONFLUENT_COMPUTE_POOL:?Set CONFLUENT_COMPUTE_POOL (e.g. lfcp-xxxxx)}"
: "${CONFLUENT_CLOUD_REGION:=us-east-1}"
: "${CONFLUENT_CLOUD_PROVIDER:=aws}"

# Pin CLI's active Flink endpoint to silence "No Flink endpoint is specified,
# defaulting to public endpoint…" on every subsequent flink command.
confluent flink region use --cloud "$CONFLUENT_CLOUD_PROVIDER" \
    --region "$CONFLUENT_CLOUD_REGION" >/dev/null 2>&1 || true
confluent flink endpoint use \
    "https://flink.${CONFLUENT_CLOUD_REGION}.${CONFLUENT_CLOUD_PROVIDER}.confluent.cloud" \
    >/dev/null 2>&1 || true

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
# Also drop the legacy apply_tag UDF if a prior register.sh registered it —
# it's no longer in this script. Other names should be benign no-ops.
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

# schema_watcher calls Confluent-internal endpoints — no connection needed
confluent flink statement create "reg-schema-watcher-${TS}" \
    --sql "CREATE FUNCTION schema_watcher
  AS 'io.confluent.scanner.SchemaWatcherUDF'
  USING JAR 'confluent-artifact://${ARTIFACT_ID}';" \
    --environment "$CONFLUENT_ENVIRONMENT" \
    --compute-pool "$CONFLUENT_COMPUTE_POOL" \
    --database "$CONFLUENT_KAFKA_CLUSTER" \
    > /dev/null \
    && echo "  [ok]   schema_watcher" \
    || echo "  [warn] schema_watcher registration failed"

echo ""
echo "Done. UDFs registered:"
echo "  classify_fields(max_layer INT, message STRING)  [via classifier-service connection]"
echo "  schema_watcher(sr_url STRING, sr_key STRING, sr_secret STRING, subject STRING)"
echo ""
echo "Run ./scripts/start_scan.sh to start all three trigger statements."
