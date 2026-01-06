#!/bin/bash
# Test 11: Cache Management
# Tests cache CRUD and default cache behavior

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 11: Cache Management"

CACHE_NAME="cache-$(random_string 6)"
CREATE_BODY=$(jq -n --arg name "$CACHE_NAME" '{cache_name:$name,is_public:true}')

log_info "Creating cache: $CACHE_NAME"
CREATE_RESPONSE=$(http_post_json "/v1/admin/caches" "$CREATE_BODY" 2>/dev/null || echo '{}')

if ! echo "$CREATE_RESPONSE" | jq -e '.cache_id' > /dev/null 2>&1; then
    log_error "Failed to create cache"
    log_error "Response: $CREATE_RESPONSE"
    exit 1
fi

CACHE_ID=$(echo "$CREATE_RESPONSE" | jq -r '.cache_id')
log_info "Created cache ID: $CACHE_ID"

# List caches and verify presence
LIST_RESPONSE=$(http_get "/v1/admin/caches" 2>/dev/null || echo '{"caches":[]}')
if echo "$LIST_RESPONSE" | jq -e --arg id "$CACHE_ID" '.caches[]? | select(.cache_id==$id)' > /dev/null 2>&1; then
    log_info "Cache appears in list"
else
    log_error "Cache missing from list"
    exit 1
fi

# Get cache details
CACHE_RESPONSE=$(http_get "/v1/admin/caches/$CACHE_ID" 2>/dev/null || echo '{}')
assert_json_field "$CACHE_RESPONSE" ".cache_id" "$CACHE_ID" "Cache ID mismatch"

# Update cache name
UPDATED_NAME="${CACHE_NAME}-updated"
UPDATE_BODY=$(jq -n --arg name "$UPDATED_NAME" '{cache_name:$name}')
UPDATE_STATUS=$(curl -s -o /tmp/cache_update.json -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -X PUT -d "$UPDATE_BODY" \
    "${CELLAR_SERVER_URL}/v1/admin/caches/$CACHE_ID")

if [ "$UPDATE_STATUS" != "200" ]; then
    log_error "Failed to update cache (HTTP $UPDATE_STATUS)"
    log_error "Response: $(cat /tmp/cache_update.json 2>/dev/null || echo '')"
    exit 1
fi
log_info "Cache updated"

# Default cache behavior
DEFAULT_CACHE_ID=$(echo "$LIST_RESPONSE" | jq -r '.caches[]? | select(.is_default==true) | .cache_id' | head -1)
if [ -z "$DEFAULT_CACHE_ID" ] || [ "$DEFAULT_CACHE_ID" = "null" ]; then
    log_info "No default cache set - attempting to set default"
    SET_DEFAULT_BODY=$(jq -n '{is_default:true,is_public:true}')
    SET_DEFAULT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -X PUT -d "$SET_DEFAULT_BODY" \
        "${CELLAR_SERVER_URL}/v1/admin/caches/$CACHE_ID")

    if [ "$SET_DEFAULT_STATUS" != "200" ]; then
        log_error "Failed to set default cache (HTTP $SET_DEFAULT_STATUS)"
        exit 1
    fi

    log_info "Default cache set successfully"

    # Unset default to keep test environment clean
    UNSET_BODY=$(jq -n '{is_default:false}')
    UNSET_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -X PUT -d "$UNSET_BODY" \
        "${CELLAR_SERVER_URL}/v1/admin/caches/$CACHE_ID")
    if [ "$UNSET_STATUS" != "200" ]; then
        log_error "Failed to unset default cache (HTTP $UNSET_STATUS)"
        exit 1
    fi
    log_info "Default cache unset"
else
    log_info "Default cache exists ($DEFAULT_CACHE_ID) - verifying conflict behavior"
    SET_DEFAULT_BODY=$(jq -n '{is_default:true,is_public:true}')
    SET_DEFAULT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -X PUT -d "$SET_DEFAULT_BODY" \
        "${CELLAR_SERVER_URL}/v1/admin/caches/$CACHE_ID")

    if [ "$SET_DEFAULT_STATUS" = "200" ]; then
        log_error "Was able to set a second default cache (unexpected)"
        exit 1
    fi
    log_info "Server rejected second default cache (HTTP $SET_DEFAULT_STATUS)"
fi

# Delete cache
DELETE_STATUS=$(http_delete_status "/v1/admin/caches/$CACHE_ID" 2>/dev/null || echo "000")
if [ "$DELETE_STATUS" != "200" ] && [ "$DELETE_STATUS" != "204" ]; then
    log_error "Failed to delete cache (HTTP $DELETE_STATUS)"
    exit 1
fi

GET_STATUS=$(http_get_status "/v1/admin/caches/$CACHE_ID" 2>/dev/null || echo "000")
if [ "$GET_STATUS" = "404" ]; then
    log_info "Cache deletion confirmed"
else
    log_warn "Cache still accessible after deletion (HTTP $GET_STATUS)"
fi

log_info "Cache management tests completed!"
