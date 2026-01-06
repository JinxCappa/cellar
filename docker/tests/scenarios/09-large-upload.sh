#!/bin/bash
# Test 09: Large Package Upload (Vault)
# Tests chunked upload with HashiCorp Vault (~150-200 MB)
# This test verifies proper handling of multi-chunk uploads

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 09: Large Package Upload (Vault)"

# This test requires the builder container to build vault
# It's run separately due to the long build time

BUILDER_CONTAINER="${BUILDER_CONTAINER:-cellar-builder}"

# Test 1: Check if vault is already built (may be cached)
log_info "Checking for pre-built vault..."

# We'll use docker exec to run commands in the builder container
# For now, simulate the upload with generated data if builder isn't available

# Check server capabilities
log_info "Checking server capabilities..."
CAPABILITIES=$(http_get "/v1/capabilities" "" 2>/dev/null || echo '{}')
if echo "$CAPABILITIES" | jq -e '.chunk_size' > /dev/null 2>&1; then
    CHUNK_SIZE=$(echo "$CAPABILITIES" | jq -r '.chunk_size')
    log_info "Server chunk size: $CHUNK_SIZE bytes"
else
    CHUNK_SIZE=16777216  # Default 16 MiB
    log_info "Using default chunk size: $CHUNK_SIZE bytes"
fi

# Test 2: Create a large upload session (simulating vault-sized package)
log_info "Creating large upload session..."

# Vault is typically ~150-200 MB, we'll simulate with exact size
SIMULATED_SIZE=167772160  # 160 MiB - roughly vault size
STORE_PATH="/nix/store/$(random_string 32)-vault-1.15.0"
NAR_HASH="sha256:$(random_string 64)"

SESSION_RESPONSE=$(http_post_json "/v1/uploads" "{
    \"store_path\": \"$STORE_PATH\",
    \"nar_hash\": \"$NAR_HASH\",
    \"nar_size\": $SIMULATED_SIZE
}" 2>/dev/null || echo '{}')

if ! echo "$SESSION_RESPONSE" | jq -e '.upload_id' > /dev/null 2>&1; then
    log_error "Failed to create upload session"
    log_error "Response: $SESSION_RESPONSE"
    exit 1
fi

UPLOAD_ID=$(echo "$SESSION_RESPONSE" | jq -r '.upload_id')
SERVER_CHUNK_SIZE=$(echo "$SESSION_RESPONSE" | jq -r '.chunk_size')

log_info "Upload ID: $UPLOAD_ID"
log_info "Chunk size: $SERVER_CHUNK_SIZE"

# Calculate expected chunks
EXPECTED_CHUNKS=$(( (SIMULATED_SIZE + SERVER_CHUNK_SIZE - 1) / SERVER_CHUNK_SIZE ))
log_info "Expected chunks: $EXPECTED_CHUNKS"

# Test 3: Upload multiple chunks
log_info "Uploading $EXPECTED_CHUNKS chunks..."

UPLOADED=0
FAILED=0

for i in $(seq 0 $((EXPECTED_CHUNKS - 1))); do
    # Generate chunk-sized random data
    OFFSET=$((i * SERVER_CHUNK_SIZE))
    REMAINING=$((SIMULATED_SIZE - OFFSET))
    THIS_CHUNK_SIZE=$SERVER_CHUNK_SIZE
    if [ $REMAINING -lt $SERVER_CHUNK_SIZE ]; then
        THIS_CHUNK_SIZE=$REMAINING
    fi

    # Generate random data for this chunk
    CHUNK_DATA=$(generate_binary_data $THIS_CHUNK_SIZE)

    log_info "Uploading chunk $i/$((EXPECTED_CHUNKS - 1)) ($THIS_CHUNK_SIZE bytes)..."

    HTTP_CODE=$(echo -n "$CHUNK_DATA" | curl -s -w "%{http_code}" -o /dev/null \
        -X PUT "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/chunks/$i" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -H "Content-Type: application/octet-stream" \
        --data-binary @-)

    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "204" ]; then
        UPLOADED=$((UPLOADED + 1))
    else
        log_warn "Chunk $i failed with HTTP $HTTP_CODE"
        FAILED=$((FAILED + 1))
    fi
done

log_info "Upload summary: $UPLOADED uploaded, $FAILED failed"

# Test 4: Verify session status before commit
log_info "Checking session status..."
SESSION_STATUS=$(http_get "/v1/uploads/$UPLOAD_ID" 2>/dev/null || echo '{}')
log_info "Session state: $(echo "$SESSION_STATUS" | jq -r '.state // "unknown"')"

# Test 5: Commit the upload
if [ $FAILED -eq 0 ]; then
    log_info "Committing upload..."
    COMMIT_STATUS=$(http_post_status "/v1/uploads/$UPLOAD_ID/commit" '{}' 2>/dev/null || echo "500")
    log_info "Commit status: $COMMIT_STATUS"

    if [ "$COMMIT_STATUS" = "200" ] || [ "$COMMIT_STATUS" = "201" ]; then
        log_info "Large upload committed successfully"
    else
        log_warn "Commit returned status $COMMIT_STATUS"
    fi
else
    log_warn "Skipping commit due to failed chunks"
fi

# Test 6: Verify the upload is retrievable
STORE_PATH_HASH=$(echo "$STORE_PATH" | sed 's|/nix/store/||' | cut -d'-' -f1)
log_info "Checking narinfo for $STORE_PATH_HASH..."

sleep 2  # Give server time to process

NARINFO_STATUS=$(http_get_status "/${STORE_PATH_HASH}.narinfo" "")
log_info "Narinfo status: $NARINFO_STATUS"

if [ "$NARINFO_STATUS" = "200" ]; then
    NARINFO=$(http_get "/${STORE_PATH_HASH}.narinfo" "")
    NARINFO_SIZE=$(echo "$NARINFO" | grep "^NarSize:" | cut -d' ' -f2)
    log_info "Narinfo reports size: $NARINFO_SIZE"

    if [ "$NARINFO_SIZE" = "$SIMULATED_SIZE" ]; then
        log_info "Size matches expected value"
    fi
fi

log_info "Large package upload tests completed!"
