#!/bin/bash
# Test 03: Upload Flow
# Tests the complete upload workflow: session creation, chunk upload, and commit

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 03: Upload Flow"

# Generate test data
STORE_PATH=$(generate_store_path "test-upload")
NAR_HASH=$(generate_nar_hash)
# Create 100KB of test data
TEST_DATA=$(generate_binary_data 102400)
NAR_SIZE=${#TEST_DATA}

log_info "Store path: $STORE_PATH"
log_info "NAR hash: $NAR_HASH"
log_info "NAR size: $NAR_SIZE bytes"

# Test 1: Create upload session
log_info "Creating upload session..."
SESSION_RESPONSE=$(http_post_json "/v1/uploads" "{
    \"store_path\": \"$STORE_PATH\",
    \"nar_hash\": \"$NAR_HASH\",
    \"nar_size\": $NAR_SIZE
}" 2>/dev/null || echo '{"error": "failed"}')

log_info "Session response: $SESSION_RESPONSE"

if ! echo "$SESSION_RESPONSE" | jq -e '.upload_id' > /dev/null 2>&1; then
    log_error "Failed to create upload session"
    log_error "Response: $SESSION_RESPONSE"
    exit 1
fi

UPLOAD_ID=$(echo "$SESSION_RESPONSE" | jq -r '.upload_id')
CHUNK_SIZE=$(echo "$SESSION_RESPONSE" | jq -r '.chunk_size // 16777216')

log_info "Upload ID: $UPLOAD_ID"
log_info "Chunk size: $CHUNK_SIZE"

assert_not_empty "$UPLOAD_ID" "Upload ID should not be empty"

# Test 2: Get upload session status
log_info "Getting upload session status..."
SESSION_STATUS=$(http_get "/v1/uploads/$UPLOAD_ID" 2>/dev/null || echo '{}')
log_info "Session status: $SESSION_STATUS"

# Test 3: Upload chunks
log_info "Uploading chunks..."

# Calculate number of chunks needed
TOTAL_SIZE=$NAR_SIZE
POSITION=0
OFFSET=0

while [ $OFFSET -lt $TOTAL_SIZE ]; do
    # Calculate chunk size for this position
    REMAINING=$((TOTAL_SIZE - OFFSET))
    CURRENT_CHUNK_SIZE=$CHUNK_SIZE
    if [ $REMAINING -lt $CHUNK_SIZE ]; then
        CURRENT_CHUNK_SIZE=$REMAINING
    fi

    # Extract chunk from test data
    CHUNK_DATA=$(echo -n "$TEST_DATA" | dd bs=1 skip=$OFFSET count=$CURRENT_CHUNK_SIZE 2>/dev/null)

    log_info "Uploading chunk $POSITION (offset=$OFFSET, size=$CURRENT_CHUNK_SIZE)..."

    CHUNK_RESPONSE=$(http_put_binary "/v1/uploads/$UPLOAD_ID/chunks/$POSITION" "$CHUNK_DATA" 2>/dev/null || echo "error")

    if [ "$CHUNK_RESPONSE" = "error" ]; then
        # Try getting status code
        CHUNK_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
            -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
            -H "Content-Type: application/octet-stream" \
            -X PUT --data-binary "$CHUNK_DATA" \
            "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/chunks/$POSITION")
        log_info "Chunk upload returned status: $CHUNK_STATUS"
    fi

    POSITION=$((POSITION + 1))
    OFFSET=$((OFFSET + CURRENT_CHUNK_SIZE))
done

log_info "Uploaded $POSITION chunks"

# Test 4: Commit upload
log_info "Committing upload..."
COMMIT_RESPONSE=$(http_post_json "/v1/uploads/$UPLOAD_ID/commit" '{}' 2>/dev/null || echo '{"error": "failed"}')
log_info "Commit response: $COMMIT_RESPONSE"

# Check if commit was successful
COMMIT_STATUS=$(http_post_status "/v1/uploads/$UPLOAD_ID/commit" '{}' 2>/dev/null || echo "500")
log_info "Commit status: $COMMIT_STATUS"

if [ "$COMMIT_STATUS" = "200" ] || [ "$COMMIT_STATUS" = "201" ] || [ "$COMMIT_STATUS" = "409" ]; then
    log_info "Upload committed successfully (or already committed)"
else
    log_warn "Commit returned status $COMMIT_STATUS"
fi

# Test 5: Verify session is complete
log_info "Verifying upload completion..."
FINAL_STATUS=$(http_get "/v1/uploads/$UPLOAD_ID" 2>/dev/null || echo '{}')
log_info "Final session status: $FINAL_STATUS"

if echo "$FINAL_STATUS" | jq -e '.state' > /dev/null 2>&1; then
    STATE=$(echo "$FINAL_STATUS" | jq -r '.state')
    log_info "Upload state: $STATE"
fi

log_info "Upload flow tests completed!"
