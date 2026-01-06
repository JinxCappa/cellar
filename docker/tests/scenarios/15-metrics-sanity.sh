#!/bin/bash
# Test 15: Metrics Sanity
# Ensures key Prometheus counters are exposed and increase after an upload

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 15: Metrics Sanity"

get_metric_value() {
    local metric="$1"
    local metrics="$2"
    echo "$metrics" | awk -v m="$metric" '$1 == m {print $2}' | tail -1
}

METRICS_STATUS=$(curl -s -o /tmp/metrics_before.txt -w "%{http_code}" "${CELLAR_SERVER_URL}/metrics")
if [ "$METRICS_STATUS" != "200" ]; then
    log_info "Metrics endpoint not available (HTTP $METRICS_STATUS); skipping"
    exit 0
fi

METRICS_BEFORE=$(cat /tmp/metrics_before.txt)
BEFORE_CREATED=$(get_metric_value "cellar_upload_sessions_created_total" "$METRICS_BEFORE")
BEFORE_COMMITTED=$(get_metric_value "cellar_upload_sessions_committed_total" "$METRICS_BEFORE")

if [ -z "$BEFORE_CREATED" ] || [ -z "$BEFORE_COMMITTED" ]; then
    log_error "Required metrics not found in /metrics"
    exit 1
fi

# Create a small upload to bump metrics
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT
DATA_FILE="$TEMP_DIR/metrics.bin"
openssl rand 2048 > "$DATA_FILE"
DATA_SIZE=$(stat -c%s "$DATA_FILE" 2>/dev/null || stat -f%z "$DATA_FILE")
CHUNK_HASH=$(sha256sum "$DATA_FILE" | cut -d' ' -f1)
NAR_HASH="sha256-$(sha256sum "$DATA_FILE" | cut -d' ' -f1 | xxd -r -p | base64 | tr -d '\n')"
STORE_PATH=$(generate_store_path "metrics-test")

SESSION_STATUS=$(curl -s -o "$TEMP_DIR/session.json" -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -X POST -d "{
    \"store_path\": \"$STORE_PATH\",
    \"nar_hash\": \"$NAR_HASH\",
    \"nar_size\": $DATA_SIZE
}" "${CELLAR_SERVER_URL}/v1/uploads")

if [ "$SESSION_STATUS" != "200" ] && [ "$SESSION_STATUS" != "201" ]; then
    log_error "Failed to create upload session (HTTP $SESSION_STATUS)"
    log_error "Response: $(cat "$TEMP_DIR/session.json" 2>/dev/null || echo '')"
    exit 1
fi

UPLOAD_ID=$(jq -r '.upload_id' "$TEMP_DIR/session.json")
assert_not_empty "$UPLOAD_ID" "Upload ID should not be empty"

CHUNK_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -H "Content-Type: application/octet-stream" \
    -X PUT --data-binary @"$DATA_FILE" \
    "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/chunks/$CHUNK_HASH")

if [ "$CHUNK_STATUS" != "200" ] && [ "$CHUNK_STATUS" != "201" ]; then
    log_error "Chunk upload failed (HTTP $CHUNK_STATUS)"
    exit 1
fi

COMMIT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -H "Content-Type: application/json" \
    -X POST -d "{\"manifest\": [\"$CHUNK_HASH\"]}" \
    "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/commit")

if [ "$COMMIT_STATUS" != "200" ] && [ "$COMMIT_STATUS" != "201" ]; then
    log_error "Commit failed (HTTP $COMMIT_STATUS)"
    exit 1
fi

METRICS_STATUS=$(curl -s -o /tmp/metrics_after.txt -w "%{http_code}" "${CELLAR_SERVER_URL}/metrics")
if [ "$METRICS_STATUS" != "200" ]; then
    log_error "Metrics endpoint unavailable after upload (HTTP $METRICS_STATUS)"
    exit 1
fi

METRICS_AFTER=$(cat /tmp/metrics_after.txt)
AFTER_CREATED=$(get_metric_value "cellar_upload_sessions_created_total" "$METRICS_AFTER")
AFTER_COMMITTED=$(get_metric_value "cellar_upload_sessions_committed_total" "$METRICS_AFTER")

assert_true "[[ $AFTER_CREATED -ge $((BEFORE_CREATED + 1)) ]]" "Upload sessions created did not increase"
assert_true "[[ $AFTER_COMMITTED -ge $((BEFORE_COMMITTED + 1)) ]]" "Upload sessions committed did not increase"

log_info "Metrics sanity tests completed!"
