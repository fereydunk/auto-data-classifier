#!/usr/bin/env bash
# Build the scanner UDF fat JAR.
# Output: flink-scanner/target/flink-scanner-udf-1.0.0.jar
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/.."

echo "Building flink-scanner-udf..."
cd "$PROJECT_DIR"
mvn clean package -q

JAR="$PROJECT_DIR/target/flink-scanner-udf-1.0.0.jar"
if [[ -f "$JAR" ]]; then
    SIZE=$(du -sh "$JAR" | cut -f1)
    echo "Built: $JAR  ($SIZE)"
else
    echo "ERROR: JAR not found after build." >&2
    exit 1
fi
