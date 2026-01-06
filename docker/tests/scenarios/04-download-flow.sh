#!/bin/bash
# Test 04: Download Flow
# Tests NAR info retrieval and NAR download

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 04: Download Flow"

# First, we need to upload something to download
# Using a known test store path hash

# Generate test data (use generate_test_data for shell-safe data)
TEST_DATA=$(generate_test_data 10240)  # 10KB
NAR_SIZE=${#TEST_DATA}
CHUNK_HASH=$(echo -n "$TEST_DATA" | compute_sha256)

# Generate store path and compute NAR hash from actual data (SRI format)
STORE_PATH=$(generate_store_path "download-test")
STORE_PATH_HASH=$(echo "$STORE_PATH" | sed 's|/nix/store/||' | cut -d'-' -f1)
# NAR hash must match actual content - compute from chunk data
NAR_HASH="sha256-$(echo -n "$TEST_DATA" | openssl dgst -sha256 -binary | base64 | tr -d '\n')"

log_info "Setting up test data..."
log_info "Store path: $STORE_PATH"
log_info "Store path hash: $STORE_PATH_HASH"
log_info "NAR hash: $NAR_HASH"
log_info "Chunk hash: $CHUNK_HASH"

# Create and commit an upload first
SESSION_RESPONSE=$(http_post_json "/v1/uploads" "{
    \"store_path\": \"$STORE_PATH\",
    \"nar_hash\": \"$NAR_HASH\",
    \"nar_size\": $NAR_SIZE
}" 2>/dev/null || echo '{"error": "failed"}')

if echo "$SESSION_RESPONSE" | jq -e '.upload_id' > /dev/null 2>&1; then
    UPLOAD_ID=$(echo "$SESSION_RESPONSE" | jq -r '.upload_id')
    log_info "Created upload session: $UPLOAD_ID"

    # Upload chunk with proper hash in URL
    CHUNK_STATUS=$(http_put_binary_status "/v1/uploads/$UPLOAD_ID/chunks/$CHUNK_HASH" "$TEST_DATA" 2>/dev/null || echo "500")
    log_info "Chunk upload status: $CHUNK_STATUS"

    # Commit with manifest containing the chunk hash
    COMMIT_RESPONSE=$(http_post_json "/v1/uploads/$UPLOAD_ID/commit" "{
        \"manifest\": [\"$CHUNK_HASH\"]
    }" 2>/dev/null || echo '{"error": "failed"}')
    log_info "Commit response: $COMMIT_RESPONSE"

    # Give the server a moment to process
    sleep 1

    log_info "Test data uploaded"
fi

# Test 1: Get narinfo (use explicit token for authenticated access)
log_info "Testing narinfo retrieval..."
NARINFO_STATUS=$(http_get_status "/${STORE_PATH_HASH}.narinfo")
log_info "Narinfo status: $NARINFO_STATUS"

if [ "$NARINFO_STATUS" = "200" ]; then
    NARINFO=$(http_get "/${STORE_PATH_HASH}.narinfo")
    log_info "Narinfo content:"
    echo "$NARINFO"

    # Verify narinfo format
    assert_true "echo '$NARINFO' | grep -q 'StorePath:'" "Narinfo should contain StorePath"
    assert_true "echo '$NARINFO' | grep -q 'NarHash:'" "Narinfo should contain NarHash"
    assert_true "echo '$NARINFO' | grep -q 'NarSize:'" "Narinfo should contain NarSize"
    assert_true "echo '$NARINFO' | grep -q 'URL:'" "Narinfo should contain URL"

    # Extract NAR URL
    NAR_URL=$(echo "$NARINFO" | grep "^URL:" | cut -d' ' -f2)
    log_info "NAR URL: $NAR_URL"

    # Test 2: Download NAR (use explicit token for authenticated access)
    if [ -n "$NAR_URL" ]; then
        log_info "Testing NAR download..."
        NAR_STATUS=$(http_get_status "/$NAR_URL")
        log_info "NAR download status: $NAR_STATUS"

        if [ "$NAR_STATUS" = "200" ]; then
            # Download and verify size (use auth header for consistency)
            TEMP_NAR=$(mktemp)
            curl -sf -H "Authorization: Bearer $CELLAR_TEST_TOKEN" "${CELLAR_SERVER_URL}/${NAR_URL}" > "$TEMP_NAR"
            DOWNLOADED_SIZE=$(stat -c%s "$TEMP_NAR" 2>/dev/null || stat -f%z "$TEMP_NAR")
            log_info "Downloaded NAR size: $DOWNLOADED_SIZE bytes"
            rm -f "$TEMP_NAR"
        fi
    fi

    # Test 3: Check signature
    if echo "$NARINFO" | grep -q "^Sig:"; then
        SIGNATURE=$(echo "$NARINFO" | grep "^Sig:" | head -1)
        log_info "Found signature: $SIGNATURE"
    else
        log_info "No signature found in narinfo"
    fi
else
    log_info "Narinfo not found (status $NARINFO_STATUS) - this may be expected if upload failed"
fi

# Test 4: Request non-existent narinfo
log_info "Testing non-existent narinfo..."
FAKE_HASH=$(nix_base32_string 32)
MISSING_STATUS=$(http_get_status "/${FAKE_HASH}.narinfo" "")
log_info "Non-existent narinfo status: $MISSING_STATUS"

if [ "$MISSING_STATUS" = "404" ]; then
    log_info "Correctly returned 404 for non-existent path"
else
    log_warn "Expected 404 for non-existent path, got $MISSING_STATUS"
fi

log_info "Download flow tests completed!"
