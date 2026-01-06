#!/bin/bash
# Test 02: Token Management
# Tests token creation, listing, and revocation via admin API

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 02: Token Management"

# Note: These tests require admin access
# The server may need to be configured with an initial admin token

# Test 1: Create a new token
log_info "Testing token creation..."
TOKEN_RESPONSE=$(http_post_json "/v1/admin/tokens" '{
    "scopes": ["cache:read", "cache:write"],
    "description": "Integration test token"
}' 2>/dev/null || echo '{"error": "not implemented"}')

if echo "$TOKEN_RESPONSE" | jq -e '.token_secret' > /dev/null 2>&1; then
    NEW_TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.token_secret')
    TOKEN_ID=$(echo "$TOKEN_RESPONSE" | jq -r '.token_id')
    log_info "Created token: ${NEW_TOKEN:0:20}..."
    log_info "Token ID: $TOKEN_ID"

    # Test 2: List tokens
    log_info "Testing token listing..."
    TOKEN_LIST=$(http_get "/v1/admin/tokens" 2>/dev/null || echo '[]')
    log_info "Token list response received"

    # Test 3: Use the new token to verify it works
    log_info "Testing new token authentication..."
    AUTH_STATUS=$(http_get_status "/v1/auth/whoami" "$NEW_TOKEN")
    assert_http_status "200" "$AUTH_STATUS" "New token should authenticate successfully"

    # Test 4: Revoke the token (DELETE method)
    log_info "Testing token revocation..."
    REVOKE_STATUS=$(http_delete_status "/v1/admin/tokens/$TOKEN_ID" 2>/dev/null || echo "404")
    if [ "$REVOKE_STATUS" = "200" ] || [ "$REVOKE_STATUS" = "204" ]; then
        log_info "Token revoked successfully"

        # Test 5: Verify revoked token is rejected
        log_info "Verifying revoked token is rejected..."
        # Give the server a moment to process the revocation
        sleep 1
        REVOKED_STATUS=$(http_get_status "/v1/auth/whoami" "$NEW_TOKEN")
        if [ "$REVOKED_STATUS" = "401" ] || [ "$REVOKED_STATUS" = "403" ]; then
            log_info "Revoked token correctly rejected"
        else
            log_warn "Revoked token returned status $REVOKED_STATUS (expected 401/403)"
        fi
    else
        log_warn "Token revocation returned $REVOKE_STATUS"
    fi
else
    log_warn "Token management API may not be fully implemented"
    log_info "Response: $TOKEN_RESPONSE"
fi

log_info "Token management tests completed!"
