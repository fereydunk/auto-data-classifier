#!/usr/bin/env bash
# startup.sh — launch the auto-data-classifier setup wizard
#
# What this does:
#   1. Kills any old wizard process on port 8002
#   2. Starts setup_wizard/main.py via uvicorn (blocking — Ctrl-C to stop)
#   3. Opens the browser to http://localhost:8002
#
# The wizard handles:
#   - Prerequisite checks (confluent CLI, mvn, ngrok, docker, .venv)
#   - Sign in to Confluent Cloud (email + password) + auto-mint Cloud API key
#   - Pick an environment + Kafka cluster + Flink pool
#   - Mint per-resource API keys (Kafka + SR) and write into .env + flink-scanner/scan.env
#
# Iteration 2 (next) will add: classifier-service start, ngrok tunnel,
# Flink connection create, JAR build/register, demo topic + test data,
# scan statements, and the scan-results → review-api bridge.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT=8002

# Demo schema is built dynamically at demo time by setup_wizard/schema_builder.py.
# The number of fields is picked in the Card 5 UI input on the wizard page —
# no CLI prompt here. (Legacy override: set FIELD_COUNT env var before launch.)

# Kill any existing wizard
pkill -f "setup_wizard.main:app" 2>/dev/null && echo "  Stopped old wizard server." || true
sleep 1

# Open browser after a short delay (no-op if 'open' isn't available, e.g. on Linux)
( sleep 2 && (command -v open >/dev/null 2>&1 && open "http://localhost:${PORT}") ) &

# Pick a Python — prefer the project venv (consistent with installed deps)
if [[ -x "${REPO_DIR}/.venv/bin/uvicorn" ]]; then
  UVICORN="${REPO_DIR}/.venv/bin/uvicorn"
else
  UVICORN="$(command -v uvicorn 2>/dev/null || true)"
  if [[ -z "${UVICORN}" ]]; then
    echo "ERROR: uvicorn not found. Activate the project venv or install uvicorn:" >&2
    echo "  ${REPO_DIR}/.venv/bin/pip install -r ${REPO_DIR}/setup_wizard/requirements.txt" >&2
    exit 1
  fi
fi

echo ""
echo "Starting setup wizard on http://localhost:${PORT} ..."
echo "(Ctrl-C to stop)"
echo ""

# uvicorn needs to find the package; cd to repo root so 'setup_wizard.main' resolves
cd "${REPO_DIR}"
exec "${UVICORN}" --app-dir "${REPO_DIR}" --host 127.0.0.1 --port "${PORT}" \
     setup_wizard.main:app
