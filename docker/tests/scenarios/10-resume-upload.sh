#!/bin/bash
# Test 10: Resumable Upload
# Tests the ability to resume an interrupted upload

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 10: Resumable Upload"

# Create temp directory for test data
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Configuration - 50 MiB to ensure multiple chunks with 16 MiB chunk size
TOTAL_SIZE=$((50 * 1024 * 1024))
INTERRUPT_AFTER=2  # Stop after uploading N chunks

# Generate test data file
log_info "Generating test data ($TOTAL_SIZE bytes)..."
TEST_DATA_FILE="$TEMP_DIR/test_data.bin"
openssl rand "$TOTAL_SIZE" > "$TEST_DATA_FILE"
ACTUAL_SIZE=$(stat -c%s "$TEST_DATA_FILE" 2>/dev/null || stat -f%z "$TEST_DATA_FILE")

# Generate valid store path and compute actual hash
STORE_PATH=$(generate_store_path "resume-test")
NAR_HASH_B64=$(sha256sum "$TEST_DATA_FILE" | cut -d' ' -f1 | xxd -r -p | base64 | tr -d '\n')
NAR_HASH="sha256-${NAR_HASH_B64}"

log_info "Store path: $STORE_PATH"
log_info "NAR hash: $NAR_HASH"

# Test 1: Create upload session
log_info "Creating upload session for resume test..."

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
CHUNK_SIZE=$(echo "$SESSION_RESPONSE" | jq -r '.chunk_size // 16777216')
TOTAL_CHUNKS=$(( (ACTUAL_SIZE + CHUNK_SIZE - 1) / CHUNK_SIZE ))

log_info "Upload ID: $UPLOAD_ID"
log_info "Chunk size: $CHUNK_SIZE bytes"
log_info "Total chunks: $TOTAL_CHUNKS"

# Collect all chunk hashes for manifest
CHUNK_HASHES=()

# Pre-compute all chunk hashes (we need them all for the manifest)
log_info "Pre-computing chunk hashes..."
for i in $(seq 0 $((TOTAL_CHUNKS - 1))); do
    OFFSET=$((i * CHUNK_SIZE))
    REMAINING=$((ACTUAL_SIZE - OFFSET))
    THIS_CHUNK_SIZE=$CHUNK_SIZE
    if [ $REMAINING -lt $CHUNK_SIZE ]; then
        THIS_CHUNK_SIZE=$REMAINING
    fi

    CHUNK_FILE="$TEMP_DIR/chunk_$i.bin"
    # Use large block size for performance (iflag for byte-level skip/count)
    dd if="$TEST_DATA_FILE" of="$CHUNK_FILE" bs=1M iflag=skip_bytes,count_bytes skip=$OFFSET count=$THIS_CHUNK_SIZE 2>/dev/null
    CHUNK_HASH=$(sha256sum "$CHUNK_FILE" | cut -d' ' -f1)
    CHUNK_HASHES+=("$CHUNK_HASH")
done

log_info "Computed ${#CHUNK_HASHES[@]} chunk hashes"

# Test 2: Upload partial chunks (simulate interruption)
log_info "Phase 1: Uploading first $INTERRUPT_AFTER chunks (simulating partial upload)..."

for i in $(seq 0 $((INTERRUPT_AFTER - 1))); do
    CHUNK_FILE="$TEMP_DIR/chunk_$i.bin"
    CHUNK_HASH="${CHUNK_HASHES[$i]}"

    HTTP_CODE=$(curl -s -w "%{http_code}" -o /dev/null \
        -X PUT "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/chunks/$CHUNK_HASH" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -H "Content-Type: application/octet-stream" \
        --data-binary @"$CHUNK_FILE")

    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ]; then
        log_info "Chunk $i uploaded successfully"
    else
        log_error "Chunk $i failed with HTTP $HTTP_CODE"
        exit 1
    fi
done

log_info "=== SIMULATED INTERRUPTION ==="

# Test 3: Check session status (should be open)
log_info "Checking session status after interruption..."
SESSION_CHECK=$(http_get "/v1/uploads/$UPLOAD_ID" 2>/dev/null || echo '{}')
STATE=$(echo "$SESSION_CHECK" | jq -r '.state // "unknown"')
log_info "Session state: $STATE"

# Check received/missing chunks
RECEIVED=$(echo "$SESSION_CHECK" | jq -r '.received_chunks | length // 0')
MISSING=$(echo "$SESSION_CHECK" | jq -r '.missing_chunks | length // 0')
log_info "Chunks received: $RECEIVED, missing: $MISSING"

# Test 4: Resume upload (upload remaining chunks)
log_info "Phase 2: Resuming upload (chunks $INTERRUPT_AFTER to $((TOTAL_CHUNKS - 1)))..."

for i in $(seq $INTERRUPT_AFTER $((TOTAL_CHUNKS - 1))); do
    CHUNK_FILE="$TEMP_DIR/chunk_$i.bin"
    CHUNK_HASH="${CHUNK_HASHES[$i]}"

    HTTP_CODE=$(curl -s -w "%{http_code}" -o /dev/null \
        -X PUT "${CELLAR_SERVER_URL}/v1/uploads/$UPLOAD_ID/chunks/$CHUNK_HASH" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -H "Content-Type: application/octet-stream" \
        --data-binary @"$CHUNK_FILE")

    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ]; then
        log_info "Chunk $i uploaded successfully"
    else
        log_error "Chunk $i failed with HTTP $HTTP_CODE"
        exit 1
    fi
done

# Test 5: Commit the upload
log_info "Committing resumed upload..."

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
    log_info "Upload committed successfully"
else
    log_error "Commit failed with status $COMMIT_STATUS"
    log_error "Response: $COMMIT_RESPONSE"
    exit 1
fi

# Test 6: Verify final session status
log_info "Verifying final session status..."
FINAL_STATUS=$(http_get "/v1/uploads/$UPLOAD_ID" 2>/dev/null || echo '{}')
FINAL_STATE=$(echo "$FINAL_STATUS" | jq -r '.state // "unknown"')
log_info "Final session state: $FINAL_STATE"

# Test 7: Test idempotent chunk upload (re-upload already uploaded chunk)
log_info "Testing idempotent chunk re-upload..."

# Create a new session for idempotency test
IDEM_DATA_FILE="$TEMP_DIR/idem_data.bin"
IDEM_SIZE=$((5 * 1024 * 1024))  # 5 MiB
openssl rand "$IDEM_SIZE" > "$IDEM_DATA_FILE"

IDEM_PATH=$(generate_store_path "idempotent-test")
IDEM_HASH_B64=$(sha256sum "$IDEM_DATA_FILE" | cut -d' ' -f1 | xxd -r -p | base64 | tr -d '\n')
IDEM_HASH="sha256-${IDEM_HASH_B64}"

IDEM_SESSION_STATUS=$(curl -s -o /tmp/idem_session.json -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
    -X POST -d "{
    \"store_path\": \"$IDEM_PATH\",
    \"nar_hash\": \"$IDEM_HASH\",
    \"nar_size\": $IDEM_SIZE
}" "${CELLAR_SERVER_URL}/v1/uploads")

if [ "$IDEM_SESSION_STATUS" = "200" ] || [ "$IDEM_SESSION_STATUS" = "201" ]; then
    IDEM_SESSION=$(cat /tmp/idem_session.json)
    IDEM_ID=$(echo "$IDEM_SESSION" | jq -r '.upload_id')

    # Compute chunk hash
    IDEM_CHUNK_HASH=$(sha256sum "$IDEM_DATA_FILE" | cut -d' ' -f1)

    # Upload chunk first time
    FIRST_UPLOAD=$(curl -s -w "%{http_code}" -o /dev/null \
        -X PUT "${CELLAR_SERVER_URL}/v1/uploads/$IDEM_ID/chunks/$IDEM_CHUNK_HASH" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -H "Content-Type: application/octet-stream" \
        --data-binary @"$IDEM_DATA_FILE")

    # Re-upload same chunk (idempotency test)
    SECOND_UPLOAD=$(curl -s -w "%{http_code}" -o /dev/null \
        -X PUT "${CELLAR_SERVER_URL}/v1/uploads/$IDEM_ID/chunks/$IDEM_CHUNK_HASH" \
        -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
        -H "Content-Type: application/octet-stream" \
        --data-binary @"$IDEM_DATA_FILE")

    log_info "First upload status: $FIRST_UPLOAD"
    log_info "Second upload status: $SECOND_UPLOAD"

    if [ "$FIRST_UPLOAD" = "200" ] || [ "$FIRST_UPLOAD" = "201" ]; then
        if [ "$SECOND_UPLOAD" = "200" ] || [ "$SECOND_UPLOAD" = "201" ]; then
            log_info "Chunk upload is idempotent"
        else
            log_warn "Re-upload failed (HTTP $SECOND_UPLOAD) - may not be idempotent"
        fi
    else
        log_warn "First upload failed (HTTP $FIRST_UPLOAD)"
    fi
else
    log_warn "Could not create idempotency test session (HTTP $IDEM_SESSION_STATUS)"
fi

log_info "Resumable upload tests completed!"
