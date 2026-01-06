#!/bin/bash
# Test 13: Commit Integrity
# Validates hash mismatch handling, invalid manifests, and commit idempotency

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 13: Commit Integrity"

TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

DATA_FILE="$TEMP_DIR/data.bin"
openssl rand 4096 > "$DATA_FILE"
DATA_SIZE=$(stat -c%s "$DATA_FILE" 2>/dev/null || stat -f%z "$DATA_FILE")
CHUNK_HASH=$(sha256sum "$DATA_FILE" | cut -d' ' -f1)
NAR_HASH="sha256-$(sha256sum "$DATA_FILE" | cut -d' ' -f1 | xxd -r -p | base64 | tr -d '\n')"

STORE_PATH=$(generate_store_path "integrity-test")
log_info "Store path: $STORE_PATH"

SESSION_STATUS=$(curl -s -o "$TEMP_DIR/session.json" -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -X POST -d "{
    \"store_path\": \"$STORE_PATH\",
    \"nar_hash\": \"$NAR_HASH\",
    \"nar_size\": $DATA_SIZE
}" "${CELLAR_SERVER_URL}/v1/uploads")

if [ "$SESSION_STATUS" != "200" ] && [ "$SESSION_STATUS" != "201" ]; then
    log_error "Failed to create upload session (HTTP $SESSION_STATUS)"
    log_error "Response: $(cat "$TEMP_DIR/session.json" 2>/dev/null || echo '')"
    exit 1
fi

UPLOAD_ID=$(jq -r '.upload_id' "$TEMP_DIR/session.json")
assert_not_empty "$UPLOAD_ID" "Upload ID should not be empty"

# Upload with wrong hash should fail
BAD_HASH=$(openssl rand -hex 32)
BAD_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -H "Content-Type: application/octet-stream" \
    -X PUT --data-binary @"$DATA_FILE" \
    "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/chunks/$BAD_HASH")

if [ "$BAD_STATUS" != "400" ]; then
    log_error "Chunk hash mismatch returned HTTP $BAD_STATUS"
    exit 1
fi
log_info "Chunk hash mismatch correctly rejected"

# Upload correct chunk
GOOD_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -H "Content-Type: application/octet-stream" \
    -X PUT --data-binary @"$DATA_FILE" \
    "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/chunks/$CHUNK_HASH")

if [ "$GOOD_STATUS" != "200" ] && [ "$GOOD_STATUS" != "201" ]; then
    log_error "Valid chunk upload failed (HTTP $GOOD_STATUS)"
    exit 1
fi

# Commit with correct manifest should succeed
GOOD_COMMIT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -H "Content-Type: application/json" \
    -X POST -d "{\"manifest\": [\"$CHUNK_HASH\"]}" \
    "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/commit")

if [ "$GOOD_COMMIT_STATUS" != "200" ] && [ "$GOOD_COMMIT_STATUS" != "201" ]; then
    log_error "Commit failed (HTTP $GOOD_COMMIT_STATUS)"
    exit 1
fi

# Commit again should fail (idempotency guard)
RECOMMIT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -H "Content-Type: application/json" \
    -X POST -d "{\"manifest\": [\"$CHUNK_HASH\"]}" \
    "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/commit")

if [ "$RECOMMIT_STATUS" = "200" ] || [ "$RECOMMIT_STATUS" = "201" ]; then
    log_error "Second commit unexpectedly succeeded"
    exit 1
fi
log_info "Second commit rejected (HTTP $RECOMMIT_STATUS)"

# Upload after commit should fail
POST_COMMIT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -H "Content-Type: application/octet-stream" \
    -X PUT --data-binary @"$DATA_FILE" \
    "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/chunks/$CHUNK_HASH")

if [ "$POST_COMMIT_STATUS" != "400" ]; then
    log_error "Upload after commit returned HTTP $POST_COMMIT_STATUS"
    exit 1
fi
log_info "Upload after commit correctly rejected"

# Commit with invalid manifest should fail on a fresh session
BAD_SESSION_PATH=$(generate_store_path "bad-manifest-test")
BAD_SESSION_STATUS=$(curl -s -o "$TEMP_DIR/bad_session.json" -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -X POST -d "{
    \"store_path\": \"$BAD_SESSION_PATH\",
    \"nar_hash\": \"$NAR_HASH\",
    \"nar_size\": $DATA_SIZE
}" "${CELLAR_SERVER_URL}/v1/uploads")

if [ "$BAD_SESSION_STATUS" = "200" ] || [ "$BAD_SESSION_STATUS" = "201" ]; then
    BAD_SESSION_ID=$(jq -r '.upload_id' "$TEMP_DIR/bad_session.json")

    # Upload the correct chunk, then commit with a bad manifest
    curl -s -o /dev/null -w "%{http_code}" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -H "Content-Type: application/octet-stream" \
        -X PUT --data-binary @"$DATA_FILE" \
        "${CELLAR_SERVER_URL}/v1/uploads/$BAD_SESSION_ID/chunks/$CHUNK_HASH" > /dev/null

    BAD_COMMIT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -H "Content-Type: application/json" \
        -X POST -d "{\"manifest\": [\"$BAD_HASH\"]}" \
        "${CELLAR_SERVER_URL}/v1/uploads/$BAD_SESSION_ID/commit")

    if [ "$BAD_COMMIT_STATUS" != "400" ]; then
        log_error "Invalid manifest returned HTTP $BAD_COMMIT_STATUS"
        exit 1
    fi
    log_info "Invalid manifest correctly rejected"
else
    log_warn "Failed to create bad-manifest session (HTTP $BAD_SESSION_STATUS)"
fi

# Commit with missing chunks should fail (use expected_chunks)
MISSING_PATH=$(generate_store_path "missing-chunk-test")
MISSING_HASH=$(openssl rand -hex 32)
MISSING_SIZE=1024
MISSING_NAR_SIZE=$((DATA_SIZE + MISSING_SIZE))
MISSING_BODY=$(jq -n \
    --arg store "$MISSING_PATH" \
    --arg narhash "$NAR_HASH" \
    --arg chunkhash "$CHUNK_HASH" \
    --arg missinghash "$MISSING_HASH" \
    --argjson narsize "$MISSING_NAR_SIZE" \
    --argjson chunksize "$DATA_SIZE" \
    --argjson missingsize "$MISSING_SIZE" \
    '{
        store_path:$store,
        nar_hash:$narhash,
        nar_size:$narsize,
        expected_chunks:[
            {hash:$chunkhash,size:$chunksize},
            {hash:$missinghash,size:$missingsize}
        ]
    }')

MISSING_STATUS=$(curl -s -o "$TEMP_DIR/missing_session.json" -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -X POST -d "$MISSING_BODY" \
    "${CELLAR_SERVER_URL}/v1/uploads")

if [ "$MISSING_STATUS" = "200" ] || [ "$MISSING_STATUS" = "201" ]; then
    MISSING_ID=$(jq -r '.upload_id' "$TEMP_DIR/missing_session.json")

    # Upload only the first expected chunk
    curl -s -o /dev/null -w "%{http_code}" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -H "Content-Type: application/octet-stream" \
        -X PUT --data-binary @"$DATA_FILE" \
        "${CELLAR_SERVER_URL}/v1/uploads/$MISSING_ID/chunks/$CHUNK_HASH" > /dev/null

    MISSING_COMMIT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -H "Content-Type: application/json" \
        -X POST -d "{\"manifest\": [\"$CHUNK_HASH\",\"$MISSING_HASH\"]}" \
        "${CELLAR_SERVER_URL}/v1/uploads/$MISSING_ID/commit")

    if [ "$MISSING_COMMIT_STATUS" != "400" ]; then
        log_error "Missing chunks commit returned HTTP $MISSING_COMMIT_STATUS"
        exit 1
    fi
    log_info "Missing chunks correctly rejected"
else
    log_warn "Failed to create missing-chunk session (HTTP $MISSING_STATUS)"
fi

log_info "Commit integrity tests completed!"
