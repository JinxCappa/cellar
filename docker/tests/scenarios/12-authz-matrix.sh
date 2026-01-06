#!/bin/bash
# Test 12: AuthZ Matrix
# Verifies read/write/admin access boundaries and cache scoping

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 12: AuthZ Matrix"

CACHE_NAME="authz-cache-$(random_string 6)"
CREATE_BODY=$(jq -n --arg name "$CACHE_NAME" '{cache_name:$name,is_public:false}')
CREATE_RESPONSE=$(http_post_json "/v1/admin/caches" "$CREATE_BODY" 2>/dev/null || echo '{}')

if ! echo "$CREATE_RESPONSE" | jq -e '.cache_id' > /dev/null 2>&1; then
    log_error "Failed to create cache for authz test"
    log_error "Response: $CREATE_RESPONSE"
    exit 1
fi

CACHE_ID=$(echo "$CREATE_RESPONSE" | jq -r '.cache_id')
log_info "Created cache ID: $CACHE_ID"

READ_RESP=$(http_post_json "/v1/admin/tokens" "$(jq -n --arg id "$CACHE_ID" '{scopes:["cache:read"],cache_id:$id,description:"authz-read"}')" 2>/dev/null || echo '{}')
WRITE_RESP=$(http_post_json "/v1/admin/tokens" "$(jq -n --arg id "$CACHE_ID" '{scopes:["cache:write"],cache_id:$id,description:"authz-write"}')" 2>/dev/null || echo '{}')

if ! echo "$READ_RESP" | jq -e '.token_secret' > /dev/null 2>&1; then
    log_error "Failed to create read token"
    log_error "Response: $READ_RESP"
    exit 1
fi

if ! echo "$WRITE_RESP" | jq -e '.token_secret' > /dev/null 2>&1; then
    log_error "Failed to create write token"
    log_error "Response: $WRITE_RESP"
    exit 1
fi

READ_TOKEN=$(echo "$READ_RESP" | jq -r '.token_secret')
READ_ID=$(echo "$READ_RESP" | jq -r '.token_id')
WRITE_TOKEN=$(echo "$WRITE_RESP" | jq -r '.token_secret')
WRITE_ID=$(echo "$WRITE_RESP" | jq -r '.token_id')

log_info "Created read/write tokens"

# Creating a cache-scoped admin token should fail
BAD_ADMIN_STATUS=$(http_post_status "/v1/admin/tokens" "$(jq -n --arg id "$CACHE_ID" '{scopes:["cache:admin"],cache_id:$id,description:"bad-admin"}')" 2>/dev/null || echo "000")
if [ "$BAD_ADMIN_STATUS" = "200" ] || [ "$BAD_ADMIN_STATUS" = "201" ]; then
    log_error "Cache-scoped admin token was created (unexpected)"
    exit 1
fi
log_info "Cache-scoped admin token rejected (HTTP $BAD_ADMIN_STATUS)"

# Use write token to create an upload
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT
DATA_FILE="$TEMP_DIR/authz_data.bin"
openssl rand 4096 > "$DATA_FILE"
DATA_SIZE=$(stat -c%s "$DATA_FILE" 2>/dev/null || stat -f%z "$DATA_FILE")
STORE_PATH=$(generate_store_path "authz-test")
STORE_PATH_HASH=$(basename "$STORE_PATH" | cut -d'-' -f1)
NAR_HASH="sha256-$(sha256sum "$DATA_FILE" | cut -d' ' -f1 | xxd -r -p | base64 | tr -d '\n')"
CHUNK_HASH=$(sha256sum "$DATA_FILE" | cut -d' ' -f1)

SESSION_STATUS=$(curl -s -o "$TEMP_DIR/session.json" -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $WRITE_TOKEN" \
    -X POST -d "{
    \"store_path\": \"$STORE_PATH\",
    \"nar_hash\": \"$NAR_HASH\",
    \"nar_size\": $DATA_SIZE
}" "${CELLAR_SERVER_URL}/v1/uploads")

if [ "$SESSION_STATUS" != "200" ] && [ "$SESSION_STATUS" != "201" ]; then
    log_error "Failed to create upload session with write token (HTTP $SESSION_STATUS)"
    log_error "Response: $(cat "$TEMP_DIR/session.json" 2>/dev/null || echo '')"
    exit 1
fi

UPLOAD_ID=$(jq -r '.upload_id' "$TEMP_DIR/session.json")
assert_not_empty "$UPLOAD_ID" "Upload ID should not be empty"

CHUNK_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $WRITE_TOKEN" \
    -H "Content-Type: application/octet-stream" \
    -X PUT --data-binary @"$DATA_FILE" \
    "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/chunks/$CHUNK_HASH")

if [ "$CHUNK_STATUS" != "200" ] && [ "$CHUNK_STATUS" != "201" ]; then
    log_error "Chunk upload failed with write token (HTTP $CHUNK_STATUS)"
    exit 1
fi

COMMIT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $WRITE_TOKEN" \
    -H "Content-Type: application/json" \
    -X POST -d "{\"manifest\": [\"$CHUNK_HASH\"]}" \
    "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/commit")

if [ "$COMMIT_STATUS" != "200" ] && [ "$COMMIT_STATUS" != "201" ]; then
    log_error "Commit failed with write token (HTTP $COMMIT_STATUS)"
    exit 1
fi

# Read token should be able to read narinfo and whoami
WHOAMI_STATUS=$(http_get_status "/v1/auth/whoami" "$READ_TOKEN")
assert_http_status "200" "$WHOAMI_STATUS" "Read token should access /v1/auth/whoami"

NARINFO_STATUS=$(http_get_status "/${STORE_PATH_HASH}.narinfo" "$READ_TOKEN")
assert_http_status "200" "$NARINFO_STATUS" "Read token should access narinfo"

NARINFO=$(http_get "/${STORE_PATH_HASH}.narinfo" "$READ_TOKEN" 2>/dev/null || echo "")
NAR_URL=$(echo "$NARINFO" | grep "^URL:" | cut -d' ' -f2)
if [ -n "$NAR_URL" ]; then
    NAR_STATUS=$(http_get_status "/$NAR_URL" "$READ_TOKEN")
    assert_http_status "200" "$NAR_STATUS" "Read token should download NAR"
fi

# Read token should not be able to create uploads
READ_UPLOAD_STATUS=$(http_post_status "/v1/uploads" "{\"store_path\":\"/nix/store/unauthz\",\"nar_hash\":\"$(generate_nar_hash)\",\"nar_size\":1}" "$READ_TOKEN" 2>/dev/null || echo "000")
if [ "$READ_UPLOAD_STATUS" = "200" ] || [ "$READ_UPLOAD_STATUS" = "201" ]; then
    log_error "Read token was able to upload (unexpected)"
    exit 1
fi
log_info "Read token blocked from upload (HTTP $READ_UPLOAD_STATUS)"

# Write token should not access admin endpoints
WRITE_ADMIN_STATUS=$(http_post_status "/v1/admin/tokens" '{"scopes":["cache:read"]}' "$WRITE_TOKEN" 2>/dev/null || echo "000")
if [ "$WRITE_ADMIN_STATUS" = "200" ] || [ "$WRITE_ADMIN_STATUS" = "201" ]; then
    log_error "Write token was able to access admin endpoint (unexpected)"
    exit 1
fi
log_info "Write token blocked from admin endpoints (HTTP $WRITE_ADMIN_STATUS)"

# Cleanup tokens and cache
http_delete_status "/v1/admin/tokens/$READ_ID" 2>/dev/null || true
http_delete_status "/v1/admin/tokens/$WRITE_ID" 2>/dev/null || true
http_delete_status "/v1/admin/caches/$CACHE_ID" 2>/dev/null || true

log_info "AuthZ matrix tests completed!"
