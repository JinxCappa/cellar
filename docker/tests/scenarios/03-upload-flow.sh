#!/bin/bash
# Test 03: Upload Flow
# Tests the complete upload workflow: session creation, chunk upload, and commit

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 03: Upload Flow"

# Create temp file for binary data (bash variables can't handle null bytes)
TEST_DATA_FILE=$(mktemp)
trap "rm -f $TEST_DATA_FILE" EXIT

# Generate 100KB of random binary test data
NAR_SIZE=102400
openssl rand "$NAR_SIZE" > "$TEST_DATA_FILE"
ACTUAL_SIZE=$(stat -c%s "$TEST_DATA_FILE" 2>/dev/null || stat -f%z "$TEST_DATA_FILE")

# Generate store path
STORE_PATH=$(generate_store_path "test-upload")

# Compute actual NAR hash from test data (SHA-256 in SRI format: sha256-<base64>)
NAR_HASH_B64=$(sha256sum "$TEST_DATA_FILE" | cut -d' ' -f1 | xxd -r -p | base64 | tr -d '\n')
NAR_HASH="sha256-${NAR_HASH_B64}"

log_info "Store path: $STORE_PATH"
log_info "NAR hash: $NAR_HASH"
log_info "NAR size: $ACTUAL_SIZE bytes"

# Test 1: Create upload session
log_info "Creating upload session..."
log_info "Using token: ${CELLAR_TEST_TOKEN:0:20}..."

# Get both status code and response body for debugging
SESSION_HTTP_CODE=$(curl -s -o /tmp/session_response.json -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -X POST -d "{
    \"store_path\": \"$STORE_PATH\",
    \"nar_hash\": \"$NAR_HASH\",
    \"nar_size\": $ACTUAL_SIZE
}" "${CELLAR_SERVER_URL}/v1/uploads")

SESSION_RESPONSE=$(cat /tmp/session_response.json 2>/dev/null || echo '{}')
log_info "Session HTTP status: $SESSION_HTTP_CODE"
log_info "Session response: $SESSION_RESPONSE"

if [ "$SESSION_HTTP_CODE" != "200" ] && [ "$SESSION_HTTP_CODE" != "201" ]; then
    log_error "Failed to create upload session (HTTP $SESSION_HTTP_CODE)"
    log_error "Response: $SESSION_RESPONSE"
    exit 1
fi

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
TOTAL_SIZE=$ACTUAL_SIZE
POSITION=0
OFFSET=0

# Collect chunk hashes for the manifest
CHUNK_HASHES=()

while [ $OFFSET -lt $TOTAL_SIZE ]; do
    # Calculate chunk size for this position
    REMAINING=$((TOTAL_SIZE - OFFSET))
    CURRENT_CHUNK_SIZE=$CHUNK_SIZE
    if [ $REMAINING -lt $CHUNK_SIZE ]; then
        CURRENT_CHUNK_SIZE=$REMAINING
    fi

    # Create temp file for this chunk
    CHUNK_FILE=$(mktemp)

    # Extract chunk from test data file (use large block size for performance)
    dd if="$TEST_DATA_FILE" of="$CHUNK_FILE" bs=1M iflag=skip_bytes,count_bytes skip=$OFFSET count=$CURRENT_CHUNK_SIZE 2>/dev/null

    # Compute the SHA256 hash of the chunk (required for the API endpoint)
    CHUNK_HASH=$(sha256sum "$CHUNK_FILE" | cut -d' ' -f1)
    CHUNK_HASHES+=("$CHUNK_HASH")

    log_info "Uploading chunk $POSITION (offset=$OFFSET, size=$CURRENT_CHUNK_SIZE, hash=$CHUNK_HASH)..."

    # Upload chunk using curl with file
    CHUNK_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -H "Content-Type: application/octet-stream" \
        -X PUT --data-binary @"$CHUNK_FILE" \
        "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/chunks/$CHUNK_HASH")

    rm -f "$CHUNK_FILE"

    log_info "Chunk upload returned status: $CHUNK_STATUS"

    if [ "$CHUNK_STATUS" != "200" ] && [ "$CHUNK_STATUS" != "201" ]; then
        log_error "Chunk upload failed with status $CHUNK_STATUS"
        exit 1
    fi

    POSITION=$((POSITION + 1))
    OFFSET=$((OFFSET + CURRENT_CHUNK_SIZE))
done

log_info "Uploaded $POSITION chunks"

# Test 4: Commit upload
log_info "Committing upload..."

# Build the manifest JSON from collected chunk hashes
MANIFEST_JSON=$(printf '%s\n' "${CHUNK_HASHES[@]}" | jq -R . | jq -s '{"manifest": .}')
log_info "Commit manifest: $MANIFEST_JSON"

# Single request to get both status and response (avoid double-commit issue)
COMMIT_STATUS=$(curl -s -o /tmp/commit_response.json -w "%{http_code}" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -H "Content-Type: application/json" \
    -X POST -d "$MANIFEST_JSON" \
    "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/commit")

COMMIT_RESPONSE=$(cat /tmp/commit_response.json 2>/dev/null || echo '{}')
log_info "Commit status: $COMMIT_STATUS"
log_info "Commit response: $COMMIT_RESPONSE"

if [ "$COMMIT_STATUS" = "200" ] || [ "$COMMIT_STATUS" = "201" ]; then
    log_info "Upload committed successfully"
else
    log_error "Commit failed with status $COMMIT_STATUS"
    exit 1
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
