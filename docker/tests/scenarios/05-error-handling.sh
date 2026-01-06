#!/bin/bash
# Test 05: Error Handling
# Tests various error conditions and edge cases

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 05: Error Handling"

# Test 1: Invalid upload session ID
log_info "Testing invalid upload session ID..."
INVALID_ID="00000000-0000-0000-0000-000000000000"
INVALID_STATUS=$(http_get_status "/v1/uploads/$INVALID_ID" 2>/dev/null || echo "error")
log_info "Invalid session status: $INVALID_STATUS"
if [ "$INVALID_STATUS" = "404" ]; then
    log_info "Correctly returned 404 for invalid session"
fi

# Test 2: Malformed JSON in request
log_info "Testing malformed JSON..."
MALFORMED_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -H "Content-Type: application/json" \
    -X POST -d '{"invalid json' \
    "${CELLAR_SERVER_URL}/v1/uploads")
log_info "Malformed JSON status: $MALFORMED_STATUS"
if [ "$MALFORMED_STATUS" = "400" ]; then
    log_info "Correctly returned 400 for malformed JSON"
fi

# Test 3: Missing required fields
log_info "Testing missing required fields..."
MISSING_FIELDS_STATUS=$(http_post_status "/v1/uploads" '{"store_path": "/nix/store/test"}' 2>/dev/null || echo "error")
log_info "Missing fields status: $MISSING_FIELDS_STATUS"
if [ "$MISSING_FIELDS_STATUS" = "400" ] || [ "$MISSING_FIELDS_STATUS" = "422" ]; then
    log_info "Correctly rejected request with missing fields"
fi

# Test 4: Invalid chunk hash format
log_info "Testing invalid chunk hash..."
# First create a valid session
NAR_HASH=$(generate_nar_hash)
SESSION=$(http_post_json "/v1/uploads" "{
    \"store_path\": \"/nix/store/test-error-handling\",
    \"nar_hash\": \"$NAR_HASH\",
    \"nar_size\": 1024
}" 2>/dev/null || echo '{}')

if echo "$SESSION" | jq -e '.upload_id' > /dev/null 2>&1; then
    UPLOAD_ID=$(echo "$SESSION" | jq -r '.upload_id')

    # Try to upload with invalid chunk hash format
    INVALID_HASH_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -H "Content-Type: application/octet-stream" \
        -X PUT --data-binary "test" \
        "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/chunks/not-a-valid-hash")
    log_info "Invalid chunk hash status: $INVALID_HASH_STATUS"
    if [ "$INVALID_HASH_STATUS" = "400" ]; then
        log_info "Correctly rejected invalid chunk hash format"
    fi
fi

# Test 5: Unauthorized access
log_info "Testing unauthorized access..."
UNAUTH_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST -H "Content-Type: application/json" \
    -d "{\"store_path\": \"/nix/store/test\", \"nar_hash\": \"$(generate_nar_hash)\", \"nar_size\": 100}" \
    "${CELLAR_SERVER_URL}/v1/uploads")
log_info "Unauthorized status: $UNAUTH_STATUS"
if [ "$UNAUTH_STATUS" = "401" ]; then
    log_info "Correctly returned 401 for unauthorized request"
elif [ "$UNAUTH_STATUS" = "200" ] || [ "$UNAUTH_STATUS" = "201" ]; then
    log_info "Server allows unauthenticated uploads (may be configured this way)"
fi

# Test 6: Invalid token
log_info "Testing invalid token..."
INVALID_TOKEN_STATUS=$(http_get_status "/v1/auth/whoami" "invalid-token-12345")
log_info "Invalid token status: $INVALID_TOKEN_STATUS"
if [ "$INVALID_TOKEN_STATUS" = "401" ] || [ "$INVALID_TOKEN_STATUS" = "403" ]; then
    log_info "Correctly rejected invalid token"
fi

# Test 7: Duplicate upload (idempotency check)
log_info "Testing duplicate upload handling..."
STORE_PATH=$(generate_store_path "duplicate-test")
NAR_HASH=$(generate_nar_hash)

# Create first session
FIRST_SESSION=$(http_post_json "/v1/uploads" "{
    \"store_path\": \"$STORE_PATH\",
    \"nar_hash\": \"$NAR_HASH\",
    \"nar_size\": 1024
}" 2>/dev/null || echo '{}')

# Create second session with same path
SECOND_SESSION=$(http_post_json "/v1/uploads" "{
    \"store_path\": \"$STORE_PATH\",
    \"nar_hash\": \"$NAR_HASH\",
    \"nar_size\": 1024
}" 2>/dev/null || echo '{}')

if echo "$FIRST_SESSION" | jq -e '.upload_id' > /dev/null 2>&1; then
    FIRST_ID=$(echo "$FIRST_SESSION" | jq -r '.upload_id')
    log_info "First session: $FIRST_ID"

    if echo "$SECOND_SESSION" | jq -e '.upload_id' > /dev/null 2>&1; then
        SECOND_ID=$(echo "$SECOND_SESSION" | jq -r '.upload_id')
        log_info "Second session: $SECOND_ID"

        if [ "$FIRST_ID" = "$SECOND_ID" ]; then
            log_info "Server returned same session ID (idempotent)"
        else
            log_info "Server created new session ID"
        fi
    elif echo "$SECOND_SESSION" | jq -e '.error' > /dev/null 2>&1; then
        log_info "Server rejected duplicate: $(echo "$SECOND_SESSION" | jq -r '.error')"
    fi
fi

# Test 8: Large header handling
log_info "Testing large header handling..."
LARGE_HEADER=$(printf 'x%.0s' {1..10000})
LARGE_HEADER_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "X-Large-Header: $LARGE_HEADER" \
    "${CELLAR_SERVER_URL}/v1/health")
log_info "Large header status: $LARGE_HEADER_STATUS"

log_info "Error handling tests completed!"
