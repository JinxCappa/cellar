#!/bin/bash
# Test 14: Narinfo Integrity
# Validates narinfo fields, compression, file hash, and signatures

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 14: Narinfo Integrity"

TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

DATA_FILE="$TEMP_DIR/nar_data.bin"
openssl rand 65536 > "$DATA_FILE"
DATA_SIZE=$(stat -c%s "$DATA_FILE" 2>/dev/null || stat -f%z "$DATA_FILE")
CHUNK_HASH=$(sha256sum "$DATA_FILE" | cut -d' ' -f1)
NAR_HASH="sha256-$(sha256sum "$DATA_FILE" | cut -d' ' -f1 | xxd -r -p | base64 | tr -d '\n')"

STORE_PATH=$(generate_store_path "narinfo-test")
STORE_PATH_HASH=$(basename "$STORE_PATH" | cut -d'-' -f1)

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

CHUNK_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -H "Content-Type: application/octet-stream" \
    -X PUT --data-binary @"$DATA_FILE" \
    "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/chunks/$CHUNK_HASH")

if [ "$CHUNK_STATUS" != "200" ] && [ "$CHUNK_STATUS" != "201" ]; then
    log_error "Chunk upload failed (HTTP $CHUNK_STATUS)"
    exit 1
fi

COMMIT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -H "Content-Type: application/json" \
    -X POST -d "{\"manifest\": [\"$CHUNK_HASH\"]}" \
    "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/commit")

if [ "$COMMIT_STATUS" != "200" ] && [ "$COMMIT_STATUS" != "201" ]; then
    log_error "Commit failed (HTTP $COMMIT_STATUS)"
    exit 1
fi

NARINFO=$(http_get "/${STORE_PATH_HASH}.narinfo" "$CELLAR_TEST_TOKEN" 2>/dev/null || echo "")
assert_not_empty "$NARINFO" "Narinfo should not be empty"

NARINFO_NAR_HASH=$(echo "$NARINFO" | grep "^NarHash:" | cut -d' ' -f2)
NARINFO_NAR_SIZE=$(echo "$NARINFO" | grep "^NarSize:" | cut -d' ' -f2)
NARINFO_FILE_HASH=$(echo "$NARINFO" | grep "^FileHash:" | cut -d' ' -f2)
NARINFO_FILE_SIZE=$(echo "$NARINFO" | grep "^FileSize:" | cut -d' ' -f2)
NARINFO_URL=$(echo "$NARINFO" | grep "^URL:" | cut -d' ' -f2)
NARINFO_COMP=$(echo "$NARINFO" | grep "^Compression:" | cut -d' ' -f2)

assert_eq "$NAR_HASH" "$NARINFO_NAR_HASH" "NarHash mismatch"
assert_eq "$DATA_SIZE" "$NARINFO_NAR_SIZE" "NarSize mismatch"

case "$NARINFO_COMP" in
    none) EXPECTED_EXT=".nar" ;;
    zstd) EXPECTED_EXT=".nar.zst" ;;
    xz) EXPECTED_EXT=".nar.xz" ;;
    gzip) EXPECTED_EXT=".nar.gz" ;;
    bzip2) EXPECTED_EXT=".nar.bz2" ;;
    *) EXPECTED_EXT="" ;;
esac

if [ -n "$EXPECTED_EXT" ]; then
    assert_true "echo \"$NARINFO_URL\" | grep -q \"$EXPECTED_EXT$\"" "Unexpected NAR URL extension"
fi

if [ -z "$NARINFO_URL" ]; then
    log_error "Narinfo missing URL"
    exit 1
fi

NAR_FILE="$TEMP_DIR/nar.bin"
curl -sf -H "Authorization: Bearer $CELLAR_TEST_TOKEN" "${CELLAR_SERVER_URL}/${NARINFO_URL}" > "$NAR_FILE"
DOWNLOADED_SIZE=$(stat -c%s "$NAR_FILE" 2>/dev/null || stat -f%z "$NAR_FILE")
FILE_HASH_B64=$(sha256sum "$NAR_FILE" | cut -d' ' -f1 | xxd -r -p | base64 | tr -d '\n')
DOWNLOADED_HASH="sha256-${FILE_HASH_B64}"

assert_eq "$NARINFO_FILE_SIZE" "$DOWNLOADED_SIZE" "FileSize mismatch"
assert_eq "$NARINFO_FILE_HASH" "$DOWNLOADED_HASH" "FileHash mismatch"

if echo "$NARINFO" | grep -q "^Sig: test-cache-1:"; then
    log_info "Signature present for test-cache-1"
else
    log_error "Expected signature for test-cache-1 not found"
    exit 1
fi

log_info "Narinfo integrity tests completed!"
