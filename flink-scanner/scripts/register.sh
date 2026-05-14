#!/usr/bin/env bash
# Register the scanner UDFs in Confluent Cloud Flink.
#
# Prerequisites:
#   1. Confluent CLI installed and logged in:      confluent login
#   2. Environment and compute pool set:
#        export CONFLUENT_ENVIRONMENT=env-xxxxx
#        export CONFLUENT_COMPUTE_POOL=lfcp-xxxxx
#   3. JAR built:  ./scripts/build.sh
#
# After running this script, the two functions are available in every
# Flink SQL statement in this environment:
#   classify_fields(classifier_url, max_layer, message)  → TABLE
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

echo "Uploading artifact to Confluent Cloud..."
ARTIFACT_JSON=$(confluent flink artifact create \
    --artifact-file "$JAR" \
    --cloud "$CONFLUENT_CLOUD_PROVIDER" \
    --region "$CONFLUENT_CLOUD_REGION" \
    --environment "$CONFLUENT_ENVIRONMENT" \
    --output json)

ARTIFACT_ID=$(echo "$ARTIFACT_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "Artifact uploaded: $ARTIFACT_ID"

echo "Registering UDFs..."
confluent flink shell \
    --environment "$CONFLUENT_ENVIRONMENT" \
    --compute-pool "$CONFLUENT_COMPUTE_POOL" << SQL

CREATE FUNCTION IF NOT EXISTS classify_fields
    AS 'io.confluent.scanner.ClassifyFieldsUDF'
    USING JAR 'confluent-artifact://${ARTIFACT_ID}'
    LANGUAGE JAVA;

CREATE FUNCTION IF NOT EXISTS schema_watcher
    AS 'io.confluent.scanner.SchemaWatcherUDF'
    USING JAR 'confluent-artifact://${ARTIFACT_ID}'
    LANGUAGE JAVA;

CREATE FUNCTION IF NOT EXISTS apply_tag
    AS 'io.confluent.scanner.ApplyTagUDF'
    USING JAR 'confluent-artifact://${ARTIFACT_ID}'
    LANGUAGE JAVA;

SHOW FUNCTIONS;

SQL

echo ""
echo "Done. UDFs registered:"
echo "  classify_fields(classifier_url STRING, max_layer INT, message STRING)"
echo "  schema_watcher(sr_url STRING, sr_key STRING, sr_secret STRING, subject STRING)"
echo "  apply_tag(sr_url, sr_key, sr_secret, cluster_id, subject, field_path, tag)"
echo ""
echo "Run ./scripts/start_scan.sh to start all three trigger statements."
