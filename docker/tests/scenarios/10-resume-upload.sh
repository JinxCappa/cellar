#!/bin/bash
# Test 10: Resumable Upload
# Tests the ability to resume an interrupted upload

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 10: Resumable Upload"

# Configuration
TOTAL_SIZE=52428800  # 50 MiB - large enough for multiple chunks
INTERRUPT_AFTER=3    # Stop after uploading N chunks

# Test 1: Create upload session
log_info "Creating upload session for resume test..."

STORE_PATH="/nix/store/$(random_string 32)-resume-test"
NAR_HASH="sha256:$(random_string 64)"

SESSION_RESPONSE=$(http_post_json "/v1/uploads" "{
    \"store_path\": \"$STORE_PATH\",
    \"nar_hash\": \"$NAR_HASH\",
    \"nar_size\": $TOTAL_SIZE
}" 2>/dev/null || echo '{}')

if ! echo "$SESSION_RESPONSE" | jq -e '.upload_id' > /dev/null 2>&1; then
    log_error "Failed to create upload session"
    exit 1
fi

UPLOAD_ID=$(echo "$SESSION_RESPONSE" | jq -r '.upload_id')
CHUNK_SIZE=$(echo "$SESSION_RESPONSE" | jq -r '.chunk_size')
TOTAL_CHUNKS=$(( (TOTAL_SIZE + CHUNK_SIZE - 1) / CHUNK_SIZE ))

log_info "Upload ID: $UPLOAD_ID"
log_info "Chunk size: $CHUNK_SIZE"
log_info "Total chunks: $TOTAL_CHUNKS"

# Test 2: Upload partial chunks (simulate interruption)
log_info "Phase 1: Uploading first $INTERRUPT_AFTER chunks (simulating partial upload)..."

for i in $(seq 0 $((INTERRUPT_AFTER - 1))); do
    OFFSET=$((i * CHUNK_SIZE))
    REMAINING=$((TOTAL_SIZE - OFFSET))
    THIS_CHUNK_SIZE=$CHUNK_SIZE
    if [ $REMAINING -lt $CHUNK_SIZE ]; then
        THIS_CHUNK_SIZE=$REMAINING
    fi

    CHUNK_DATA=$(generate_binary_data $THIS_CHUNK_SIZE)

    HTTP_CODE=$(echo -n "$CHUNK_DATA" | curl -s -w "%{http_code}" -o /dev/null \
        -X PUT "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/chunks/$i" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -H "Content-Type: application/octet-stream" \
        --data-binary @-)

    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "204" ]; then
        log_info "Chunk $i uploaded successfully"
    else
        log_error "Chunk $i failed with HTTP $HTTP_CODE"
        exit 1
    fi
done

log_info "=== SIMULATED INTERRUPTION ==="

# Test 3: Check session status (should be open/partial)
log_info "Checking session status after interruption..."
SESSION_STATUS=$(http_get "/v1/uploads/$UPLOAD_ID" 2>/dev/null || echo '{}')
STATE=$(echo "$SESSION_STATUS" | jq -r '.state // "unknown"')
log_info "Session state: $STATE"

if [ "$STATE" != "open" ] && [ "$STATE" != "partial" ] && [ "$STATE" != "uploading" ]; then
    log_warn "Unexpected session state: $STATE (expected open/partial/uploading)"
fi

# Check which chunks are recorded as received
if echo "$SESSION_STATUS" | jq -e '.chunks_received' > /dev/null 2>&1; then
    CHUNKS_RECEIVED=$(echo "$SESSION_STATUS" | jq -r '.chunks_received')
    log_info "Chunks received: $CHUNKS_RECEIVED"
fi

# Test 4: Resume upload (upload remaining chunks)
log_info "Phase 2: Resuming upload (chunks $INTERRUPT_AFTER to $((TOTAL_CHUNKS - 1)))..."

for i in $(seq $INTERRUPT_AFTER $((TOTAL_CHUNKS - 1))); do
    OFFSET=$((i * CHUNK_SIZE))
    REMAINING=$((TOTAL_SIZE - OFFSET))
    THIS_CHUNK_SIZE=$CHUNK_SIZE
    if [ $REMAINING -lt $CHUNK_SIZE ]; then
        THIS_CHUNK_SIZE=$REMAINING
    fi

    CHUNK_DATA=$(generate_binary_data $THIS_CHUNK_SIZE)

    HTTP_CODE=$(echo -n "$CHUNK_DATA" | curl -s -w "%{http_code}" -o /dev/null \
        -X PUT "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/chunks/$i" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -H "Content-Type: application/octet-stream" \
        --data-binary @-)

    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "204" ]; then
        log_info "Chunk $i uploaded successfully"
    else
        log_error "Chunk $i failed with HTTP $HTTP_CODE"
        exit 1
    fi
done

# Test 5: Commit the upload
log_info "Committing resumed upload..."
COMMIT_RESPONSE=$(http_post_json "/v1/uploads/$UPLOAD_ID/commit" '{}' 2>/dev/null || echo '{}')
log_info "Commit response: $COMMIT_RESPONSE"

COMMIT_STATUS=$(http_post_status "/v1/uploads/$UPLOAD_ID/commit" '{}' 2>/dev/null || echo "500")
if [ "$COMMIT_STATUS" = "200" ] || [ "$COMMIT_STATUS" = "201" ] || [ "$COMMIT_STATUS" = "409" ]; then
    log_info "Upload committed successfully"
else
    log_warn "Commit returned status $COMMIT_STATUS"
fi

# Test 6: Verify final session status
log_info "Verifying final session status..."
FINAL_STATUS=$(http_get "/v1/uploads/$UPLOAD_ID" 2>/dev/null || echo '{}')
FINAL_STATE=$(echo "$FINAL_STATUS" | jq -r '.state // "unknown"')
log_info "Final session state: $FINAL_STATE"

if [ "$FINAL_STATE" = "committed" ] || [ "$FINAL_STATE" = "complete" ]; then
    log_info "Resume test successful - upload completed after interruption"
else
    log_warn "Final state is '$FINAL_STATE' (expected committed/complete)"
fi

# Test 7: Test idempotent chunk upload (re-upload already uploaded chunk)
log_info "Testing idempotent chunk re-upload..."

# Create a new session for idempotency test
IDEM_PATH="/nix/store/$(random_string 32)-idempotent-test"
IDEM_SIZE=5242880  # 5 MiB

IDEM_SESSION=$(http_post_json "/v1/uploads" "{
    \"store_path\": \"$IDEM_PATH\",
    \"nar_hash\": \"sha256:$(random_string 64)\",
    \"nar_size\": $IDEM_SIZE
}" 2>/dev/null || echo '{}')

if echo "$IDEM_SESSION" | jq -e '.upload_id' > /dev/null 2>&1; then
    IDEM_ID=$(echo "$IDEM_SESSION" | jq -r '.upload_id')
    IDEM_CHUNK_SIZE=$(echo "$IDEM_SESSION" | jq -r '.chunk_size')

    # Upload chunk 0
    CHUNK_DATA=$(generate_binary_data $IDEM_CHUNK_SIZE)
    FIRST_UPLOAD=$(echo -n "$CHUNK_DATA" | curl -s -w "%{http_code}" -o /dev/null \
        -X PUT "${CELLAR_SERVER_URL}/v1/uploads/$IDEM_ID/chunks/0" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -H "Content-Type: application/octet-stream" \
        --data-binary @-)

    # Re-upload same chunk
    SECOND_UPLOAD=$(echo -n "$CHUNK_DATA" | curl -s -w "%{http_code}" -o /dev/null \
        -X PUT "${CELLAR_SERVER_URL}/v1/uploads/$IDEM_ID/chunks/0" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -H "Content-Type: application/octet-stream" \
        --data-binary @-)

    log_info "First upload status: $FIRST_UPLOAD"
    log_info "Second upload status: $SECOND_UPLOAD"

    if [ "$FIRST_UPLOAD" = "$SECOND_UPLOAD" ]; then
        log_info "Chunk upload is idempotent"
    else
        log_warn "Chunk upload may not be idempotent (different status codes)"
    fi
fi

log_info "Resumable upload tests completed!"
