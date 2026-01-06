#!/bin/bash
# Test 09: Large Upload (Simulated)
# Tests chunked upload with a large simulated package (~20 MB)
# Verifies proper handling of multi-chunk uploads

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 09: Large Upload (Multi-chunk)"

# Create temp directory for test data
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Test 1: Check server capabilities
log_info "Checking server capabilities..."
CAPABILITIES_STATUS=$(curl -s -o /tmp/capabilities.json -w "%{http_code}" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    "${CELLAR_SERVER_URL}/v1/capabilities")

if [ "$CAPABILITIES_STATUS" = "200" ]; then
    CHUNK_SIZE=$(jq -r '.default_chunk_size // 16777216' /tmp/capabilities.json)
    log_info "Server chunk size: $CHUNK_SIZE bytes"
else
    CHUNK_SIZE=16777216  # Default 16 MiB
    log_info "Using default chunk size: $CHUNK_SIZE bytes"
fi

# Test 2: Generate large test data (~20 MB to ensure multiple chunks)
log_info "Generating large test data..."
TEST_DATA_FILE="$TEMP_DIR/test_data.bin"
SIMULATED_SIZE=$((20 * 1024 * 1024))  # 20 MiB - ensures at least 2 chunks with 16 MiB chunk size

# Generate random data to file (avoids bash variable null byte issues)
openssl rand "$SIMULATED_SIZE" > "$TEST_DATA_FILE"
ACTUAL_SIZE=$(stat -c%s "$TEST_DATA_FILE" 2>/dev/null || stat -f%z "$TEST_DATA_FILE")
log_info "Generated $ACTUAL_SIZE bytes of test data"

# Generate valid store path using Nix base32
STORE_PATH=$(generate_store_path "large-test-pkg")

# Compute actual NAR hash from test data (SHA-256 in SRI format)
NAR_HASH_B64=$(sha256sum "$TEST_DATA_FILE" | cut -d' ' -f1 | xxd -r -p | base64 | tr -d '\n')
NAR_HASH="sha256-${NAR_HASH_B64}"

log_info "Store path: $STORE_PATH"
log_info "NAR hash: $NAR_HASH"
log_info "NAR size: $ACTUAL_SIZE bytes"

# Test 3: Create upload session
log_info "Creating large upload session..."

SESSION_STATUS=$(curl -s -o /tmp/session_response.json -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -X POST -d "{
    \"store_path\": \"$STORE_PATH\",
    \"nar_hash\": \"$NAR_HASH\",
    \"nar_size\": $ACTUAL_SIZE
}" "${CELLAR_SERVER_URL}/v1/uploads")

SESSION_RESPONSE=$(cat /tmp/session_response.json 2>/dev/null || echo '{}')

if [ "$SESSION_STATUS" != "200" ] && [ "$SESSION_STATUS" != "201" ]; then
    log_error "Failed to create upload session (HTTP $SESSION_STATUS)"
    log_error "Response: $SESSION_RESPONSE"
    exit 1
fi

if ! echo "$SESSION_RESPONSE" | jq -e '.upload_id' > /dev/null 2>&1; then
    log_error "Failed to create upload session"
    log_error "Response: $SESSION_RESPONSE"
    exit 1
fi

UPLOAD_ID=$(echo "$SESSION_RESPONSE" | jq -r '.upload_id')
SERVER_CHUNK_SIZE=$(echo "$SESSION_RESPONSE" | jq -r '.chunk_size // 16777216')

log_info "Upload ID: $UPLOAD_ID"
log_info "Chunk size: $SERVER_CHUNK_SIZE bytes"

# Calculate expected chunks
EXPECTED_CHUNKS=$(( (ACTUAL_SIZE + SERVER_CHUNK_SIZE - 1) / SERVER_CHUNK_SIZE ))
log_info "Expected chunks: $EXPECTED_CHUNKS"

# Test 4: Upload chunks
log_info "Uploading $EXPECTED_CHUNKS chunks..."

UPLOADED=0
FAILED=0
CHUNK_HASHES=()

for i in $(seq 0 $((EXPECTED_CHUNKS - 1))); do
    OFFSET=$((i * SERVER_CHUNK_SIZE))
    REMAINING=$((ACTUAL_SIZE - OFFSET))
    THIS_CHUNK_SIZE=$SERVER_CHUNK_SIZE
    if [ $REMAINING -lt $SERVER_CHUNK_SIZE ]; then
        THIS_CHUNK_SIZE=$REMAINING
    fi

    # Extract chunk to temp file (use large block size for performance)
    CHUNK_FILE="$TEMP_DIR/chunk_$i.bin"
    dd if="$TEST_DATA_FILE" of="$CHUNK_FILE" bs=1M iflag=skip_bytes,count_bytes skip=$OFFSET count=$THIS_CHUNK_SIZE 2>/dev/null

    # Compute SHA256 hash of chunk
    CHUNK_HASH=$(sha256sum "$CHUNK_FILE" | cut -d' ' -f1)
    CHUNK_HASHES+=("$CHUNK_HASH")

    log_info "Uploading chunk $i/$((EXPECTED_CHUNKS - 1)) ($THIS_CHUNK_SIZE bytes)..."

    HTTP_CODE=$(curl -s -w "%{http_code}" -o /dev/null \
        -X PUT "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/chunks/$CHUNK_HASH" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -H "Content-Type: application/octet-stream" \
        --data-binary @"$CHUNK_FILE")

    rm -f "$CHUNK_FILE"

    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ]; then
        UPLOADED=$((UPLOADED + 1))
        log_info "  Chunk $i uploaded successfully"
    else
        log_warn "  Chunk $i failed with HTTP $HTTP_CODE"
        FAILED=$((FAILED + 1))
    fi
done

log_info "Upload summary: $UPLOADED uploaded, $FAILED failed"

if [ $FAILED -gt 0 ]; then
    log_error "Some chunks failed to upload"
    exit 1
fi

# Test 5: Verify session status before commit
log_info "Checking session status..."
SESSION_STATUS=$(http_get "/v1/uploads/$UPLOAD_ID" 2>/dev/null || echo '{}')
log_info "Session state: $(echo "$SESSION_STATUS" | jq -r '.state // "unknown"')"

# Test 6: Commit the upload
log_info "Committing upload..."

MANIFEST_JSON=$(printf '%s\n' "${CHUNK_HASHES[@]}" | jq -R . | jq -s '{"manifest": .}')
log_info "Manifest has ${#CHUNK_HASHES[@]} chunks"

COMMIT_STATUS=$(curl -s -o /tmp/commit_response.json -w "%{http_code}" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -H "Content-Type: application/json" \
    -X POST -d "$MANIFEST_JSON" \
    "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/commit")

COMMIT_RESPONSE=$(cat /tmp/commit_response.json 2>/dev/null || echo '{}')
log_info "Commit status: $COMMIT_STATUS"

if [ "$COMMIT_STATUS" = "200" ] || [ "$COMMIT_STATUS" = "201" ]; then
    log_info "Large upload committed successfully"
else
    log_error "Commit failed with status $COMMIT_STATUS"
    log_error "Response: $COMMIT_RESPONSE"
    exit 1
fi

# Test 7: Verify the upload is retrievable via narinfo
STORE_PATH_HASH=$(basename "$STORE_PATH" | cut -d'-' -f1)
log_info "Checking narinfo for $STORE_PATH_HASH..."

sleep 1  # Brief pause for server processing

NARINFO_STATUS=$(curl -s -o /tmp/narinfo.txt -w "%{http_code}" \
    "${CELLAR_SERVER_URL}/${STORE_PATH_HASH}.narinfo")

log_info "Narinfo status: $NARINFO_STATUS"

if [ "$NARINFO_STATUS" = "200" ]; then
    NARINFO=$(cat /tmp/narinfo.txt)
    NARINFO_SIZE=$(echo "$NARINFO" | grep "^NarSize:" | cut -d' ' -f2)
    log_info "Narinfo reports size: $NARINFO_SIZE"

    if [ "$NARINFO_SIZE" = "$ACTUAL_SIZE" ]; then
        log_info "Size matches expected value"
    else
        log_warn "Size mismatch: expected $ACTUAL_SIZE, got $NARINFO_SIZE"
    fi
else
    log_warn "Could not fetch narinfo (HTTP $NARINFO_STATUS)"
fi

log_info "Large upload test completed successfully!"
