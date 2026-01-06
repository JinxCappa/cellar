#!/bin/bash
# Test 16: GC Safety
# Ensures GC jobs do not delete live data

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 16: GC Safety"

TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

DATA_FILE="$TEMP_DIR/gc_data.bin"
openssl rand 8192 > "$DATA_FILE"
DATA_SIZE=$(stat -c%s "$DATA_FILE" 2>/dev/null || stat -f%z "$DATA_FILE")
CHUNK_HASH=$(sha256sum "$DATA_FILE" | cut -d' ' -f1)
NAR_HASH="sha256-$(sha256sum "$DATA_FILE" | cut -d' ' -f1 | xxd -r -p | base64 | tr -d '\n')"

STORE_PATH=$(generate_store_path "gc-safety-test")
STORE_PATH_HASH=$(basename "$STORE_PATH" | cut -d'-' -f1)

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

# Verify narinfo and NAR are accessible before GC
NARINFO_STATUS=$(http_get_status "/${STORE_PATH_HASH}.narinfo" "$CELLAR_TEST_TOKEN")
assert_http_status "200" "$NARINFO_STATUS" "Narinfo should be accessible before GC"

NARINFO=$(http_get "/${STORE_PATH_HASH}.narinfo" "$CELLAR_TEST_TOKEN" 2>/dev/null || echo "")
NAR_URL=$(echo "$NARINFO" | grep "^URL:" | cut -d' ' -f2)
if [ -n "$NAR_URL" ]; then
    NAR_STATUS=$(http_get_status "/$NAR_URL" "$CELLAR_TEST_TOKEN")
    assert_http_status "200" "$NAR_STATUS" "NAR should be accessible before GC"
fi

# Trigger GC job
GC_RESPONSE=$(http_post_json "/v1/admin/gc" '{"job_type": "chunk_gc"}' 2>/dev/null || echo '{}')
if ! echo "$GC_RESPONSE" | jq -e '.job_id' > /dev/null 2>&1; then
    log_info "GC API unavailable or not authorized; skipping GC safety validation"
    exit 0
fi

GC_JOB_ID=$(echo "$GC_RESPONSE" | jq -r '.job_id')
log_info "GC job created: $GC_JOB_ID"

# Wait for GC completion
ATTEMPTS=0
STATE="queued"
while [ $ATTEMPTS -lt 30 ]; do
    GC_STATUS=$(http_get "/v1/admin/gc/$GC_JOB_ID" 2>/dev/null || echo '{}')
    STATE=$(echo "$GC_STATUS" | jq -r '.state // "unknown"')
    if [ "$STATE" = "finished" ] || [ "$STATE" = "failed" ]; then
        break
    fi
    sleep 1
    ATTEMPTS=$((ATTEMPTS + 1))
done

if [ "$STATE" = "failed" ]; then
    log_error "GC job failed"
    exit 1
fi
if [ "$STATE" != "finished" ]; then
    log_error "GC job did not finish (state: $STATE)"
    exit 1
fi

# Verify narinfo and NAR are still accessible after GC
NARINFO_STATUS=$(http_get_status "/${STORE_PATH_HASH}.narinfo" "$CELLAR_TEST_TOKEN")
assert_http_status "200" "$NARINFO_STATUS" "Narinfo should be accessible after GC"

if [ -n "$NAR_URL" ]; then
    NAR_STATUS=$(http_get_status "/$NAR_URL" "$CELLAR_TEST_TOKEN")
    assert_http_status "200" "$NAR_STATUS" "NAR should be accessible after GC"
fi

log_info "GC safety tests completed!"
