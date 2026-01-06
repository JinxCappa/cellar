#!/usr/bin/env bash
# Test 07: cellarctl GC CLI
# Tests gc run (wait) and status output

set -euo pipefail

echo "[INFO] Test 07: cellarctl GC CLI"

export CELLAR_SERVER="${CELLAR_SERVER_URL}"
export CELLAR_TOKEN="${CELLAR_TEST_TOKEN}"

echo "[INFO] Running GC job..."
GC_OUTPUT=$(cellarctl gc run --job-type upload_gc --wait)

JOB_ID=$(echo "$GC_OUTPUT" | sed -n 's/^GC job queued: //p' | head -1)
if [ -z "${JOB_ID:-}" ]; then
    echo "[ERROR] Failed to parse GC job ID"
    echo "$GC_OUTPUT"
    exit 1
fi

echo "[INFO] Checking GC status for job: $JOB_ID"
STATUS_OUTPUT=$(cellarctl gc status "$JOB_ID")

if ! echo "$STATUS_OUTPUT" | grep -q "State: finished"; then
    echo "[ERROR] GC job not finished"
    echo "$STATUS_OUTPUT"
    exit 1
fi

echo ""
echo "========================================"
echo "[PASS] cellarctl GC CLI test passed!"
echo "========================================"
