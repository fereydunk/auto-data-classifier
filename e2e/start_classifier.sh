#!/usr/bin/env bash
# Start the classifier service natively (no Docker required).
#
# Prerequisites — run these once if not already installed:
#   brew install ngrok/ngrok/ngrok
#   .venv/bin/pip install gliner
#   .venv/bin/python -m spacy download en_core_web_lg
#
# What this script does:
#   1. Starts uvicorn on port 8000
#   2. Waits for the /health endpoint to return ok
#   3. Prints the local URL and reminds you to start ngrok
#
# In a SECOND terminal, run:
#   ngrok http 8000
# Copy the https://xxxx.ngrok-free.app URL — that is your CLASSIFIER_URL
# for scan.sql and register.sh.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="$REPO_ROOT/.venv/bin"
SVC="$REPO_ROOT/classifier-service"

# Check prerequisites
if [[ ! -x "$VENV/python" ]]; then
    echo "ERROR: .venv not found. Run: python3 -m venv .venv && .venv/bin/pip install -r classifier-service/requirements.txt" >&2
    exit 1
fi

if ! "$VENV/python" -c "import gliner" 2>/dev/null; then
    echo "Installing gliner..."
    "$VENV/pip" install gliner -q
fi

if ! "$VENV/python" -c "import spacy; spacy.load('en_core_web_lg')" 2>/dev/null; then
    echo "Downloading en_core_web_lg (~600 MB, one-time)..."
    "$VENV/python" -m spacy download en_core_web_lg
fi

echo "Starting classifier service on http://localhost:8000 ..."
echo "(GLiNER model loads on first start — allow 60-90 seconds)"
echo ""

# Run uvicorn in the background so this script can poll /health
cd "$SVC"
PYTHONPATH="$SVC" "$VENV/uvicorn" main:app --host 0.0.0.0 --port 8000 &
UVICORN_PID=$!

# Wait up to 120s for the service to be healthy
echo -n "Waiting for /health "
for i in $(seq 1 24); do
    sleep 5
    RESP=$(curl -sf http://localhost:8000/health 2>/dev/null || true)
    if echo "$RESP" | grep -q '"status":"ok"'; then
        echo " ready!"
        echo ""
        echo "Classifier is up: http://localhost:8000"
        echo "  Health: $RESP"
        echo ""
        echo "Next: open a NEW terminal tab and run:"
        echo "  ngrok http 8000"
        echo "Copy the  https://xxxx.ngrok-free.app  URL, then:"
        echo "  1. Set CLASSIFIER_URL in flink-scanner/scan.env"
        echo "  2. Update the classifier-service Flink connection endpoint:"
        echo "     confluent flink connection update classifier-service \\"
        echo "         --endpoint https://xxxx.ngrok-free.app \\"
        echo "         --environment \$CONFLUENT_ENVIRONMENT --cloud aws --region us-west-2"
        echo ""
        echo "Press Ctrl+C here to stop the classifier."
        wait $UVICORN_PID
        exit 0
    fi
    echo -n "."
done

echo ""
echo "ERROR: classifier did not become healthy within 120s."
echo "Check for errors above (GLiNER download, port conflict, etc.)"
kill $UVICORN_PID 2>/dev/null || true
exit 1
