#!/bin/bash
# Test 07: S3 Storage (MinIO)
# Tests specific to S3 storage backend - only runs in sqlite-s3 configuration

set -euo pipefail
source /usr/local/lib/test-utils.sh

log_info "Test 07: S3 Storage (MinIO)"

# Check if we're in S3 configuration
if [[ "${CELLAR_CONFIG:-}" != *"s3"* ]]; then
    log_info "Skipping S3 tests - not in S3 configuration"
    exit 0
fi

# S3/MinIO configuration
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin123}"
BUCKET_NAME="cellar"

# Configure AWS CLI for MinIO
export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY

log_info "MinIO endpoint: $MINIO_ENDPOINT"

# Test 1: Verify MinIO connectivity
log_info "Testing MinIO connectivity..."
MINIO_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${MINIO_ENDPOINT}/minio/health/live" 2>/dev/null || echo "000")
if [ "$MINIO_STATUS" = "200" ]; then
    log_info "MinIO is healthy"
else
    log_warn "MinIO health check returned: $MINIO_STATUS"
fi

# Test 2: Verify bucket exists
log_info "Checking bucket existence..."
BUCKET_CHECK=$(aws --endpoint-url "$MINIO_ENDPOINT" s3 ls "s3://$BUCKET_NAME" 2>&1 || echo "error")
if [[ "$BUCKET_CHECK" != *"error"* ]] && [[ "$BUCKET_CHECK" != *"NoSuchBucket"* ]]; then
    log_info "Bucket '$BUCKET_NAME' exists"
else
    log_warn "Bucket check result: $BUCKET_CHECK"
fi

# Test 3: Upload data and verify it lands in S3
log_info "Testing upload to S3..."
STORE_PATH=$(generate_store_path "s3-test")
STORE_PATH_HASH=$(echo "$STORE_PATH" | sed 's|/nix/store/||' | cut -d'-' -f1)
TEST_DATA=$(generate_test_data 51200)  # 50KB
NAR_SIZE=${#TEST_DATA}
CHUNK_HASH=$(echo -n "$TEST_DATA" | compute_sha256)
# NAR hash must match actual content - compute from chunk data (SRI format)
NAR_HASH="sha256-$(echo -n "$TEST_DATA" | openssl dgst -sha256 -binary | base64 | tr -d '\n')"

log_info "Store path: $STORE_PATH"
log_info "Chunk hash: $CHUNK_HASH"

# Create upload session
SESSION_RESPONSE=$(http_post_json "/v1/uploads" "{
    \"store_path\": \"$STORE_PATH\",
    \"nar_hash\": \"$NAR_HASH\",
    \"nar_size\": $NAR_SIZE
}" 2>/dev/null || echo '{}')

if echo "$SESSION_RESPONSE" | jq -e '.upload_id' > /dev/null 2>&1; then
    UPLOAD_ID=$(echo "$SESSION_RESPONSE" | jq -r '.upload_id')
    log_info "Upload session: $UPLOAD_ID"

    # Upload chunk with hash in URL
    CHUNK_STATUS=$(http_put_binary_status "/v1/uploads/$UPLOAD_ID/chunks/$CHUNK_HASH" "$TEST_DATA" 2>/dev/null || echo "500")
    log_info "Chunk upload status: $CHUNK_STATUS"

    # Commit with manifest
    COMMIT_RESPONSE=$(http_post_json "/v1/uploads/$UPLOAD_ID/commit" "{
        \"manifest\": [\"$CHUNK_HASH\"]
    }" 2>/dev/null || echo '{}')
    log_info "Commit response: $COMMIT_RESPONSE"

    # Wait for commit to process
    sleep 2

    # Test 4: Verify data in S3
    log_info "Verifying data in S3..."
    S3_OBJECTS=$(aws --endpoint-url "$MINIO_ENDPOINT" s3 ls "s3://$BUCKET_NAME/cache/" --recursive 2>/dev/null || echo "")
    if [ -n "$S3_OBJECTS" ]; then
        log_info "Found objects in S3:"
        echo "$S3_OBJECTS" | head -10
    else
        log_warn "No objects found in S3 bucket"
    fi

    # Test 5: Verify NAR is accessible via S3 directly
    log_info "Testing direct S3 NAR access..."

    # Get narinfo to find NAR URL
    NARINFO=$(http_get "/${STORE_PATH_HASH}.narinfo" 2>/dev/null || echo "")
    if [ -n "$NARINFO" ]; then
        NAR_URL=$(echo "$NARINFO" | grep "^URL:" | cut -d' ' -f2)
        if [ -n "$NAR_URL" ]; then
            log_info "NAR URL from narinfo: $NAR_URL"

            # Try to access via S3 directly
            # Note: S3 path includes "public/" prefix for global token uploads (cache_id = None)
            # Full S3 key structure: {storage_prefix}/{cache_prefix}/{nar_url}
            #                      = cache/public/nar/{hash}.nar.zst
            S3_NAR_STATUS=$(aws --endpoint-url "$MINIO_ENDPOINT" s3 ls "s3://$BUCKET_NAME/cache/public/$NAR_URL" 2>&1 || echo "not found")
            log_info "S3 NAR check: $S3_NAR_STATUS"
        fi
    fi
fi

# Test 6: Test S3 multipart upload (for large files)
log_info "Testing large file upload (multipart)..."

# Use temp file for large data (bash variables can't handle large binary data efficiently)
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT
LARGE_FILE="$TEMP_DIR/large_data.bin"
LARGE_SIZE=$((1024 * 1024))  # 1MB

# Generate random data to file
openssl rand "$LARGE_SIZE" > "$LARGE_FILE"
ACTUAL_SIZE=$(stat -c%s "$LARGE_FILE" 2>/dev/null || stat -f%z "$LARGE_FILE")

LARGE_PATH=$(generate_store_path "large-s3-test")
# NAR hash from actual file content (SRI format)
LARGE_NAR_HASH="sha256-$(sha256sum "$LARGE_FILE" | cut -d' ' -f1 | xxd -r -p | base64 | tr -d '\n')"

log_info "Large file size: $ACTUAL_SIZE bytes"
log_info "Large file NAR hash: $LARGE_NAR_HASH"

LARGE_SESSION=$(http_post_json "/v1/uploads" "{
    \"store_path\": \"$LARGE_PATH\",
    \"nar_hash\": \"$LARGE_NAR_HASH\",
    \"nar_size\": $ACTUAL_SIZE
}" 2>/dev/null || echo '{}')

if echo "$LARGE_SESSION" | jq -e '.upload_id' > /dev/null 2>&1; then
    LARGE_ID=$(echo "$LARGE_SESSION" | jq -r '.upload_id')
    log_info "Large upload session: $LARGE_ID"

    # Upload in chunks using file-based extraction
    CHUNK_SIZE=262144  # 256KB chunks
    OFFSET=0
    CHUNK_HASHES=()
    CHUNK_NUM=0

    while [ $OFFSET -lt $ACTUAL_SIZE ]; do
        REMAINING=$((ACTUAL_SIZE - OFFSET))
        CURRENT_SIZE=$CHUNK_SIZE
        if [ $REMAINING -lt $CHUNK_SIZE ]; then
            CURRENT_SIZE=$REMAINING
        fi

        # Extract chunk to temp file (use large block size with byte-level skip/count for performance)
        CHUNK_FILE="$TEMP_DIR/chunk_$CHUNK_NUM.bin"
        dd if="$LARGE_FILE" of="$CHUNK_FILE" bs=1M iflag=skip_bytes,count_bytes skip=$OFFSET count=$CURRENT_SIZE 2>/dev/null

        # Compute SHA256 hash of chunk
        CHUNK_HASH=$(sha256sum "$CHUNK_FILE" | cut -d' ' -f1)
        CHUNK_HASHES+=("$CHUNK_HASH")

        # Upload chunk with hash in URL
        curl -sf \
            -H "Authorization: Bearer $CELLAR_TEST_TOKEN" \
            -H "Content-Type: application/octet-stream" \
            -X PUT --data-binary @"$CHUNK_FILE" \
            "${CELLAR_SERVER_URL}/v1/uploads/$LARGE_ID/chunks/$CHUNK_HASH" 2>/dev/null || true

        rm -f "$CHUNK_FILE"
        OFFSET=$((OFFSET + CURRENT_SIZE))
        CHUNK_NUM=$((CHUNK_NUM + 1))
    done

    log_info "Uploaded ${#CHUNK_HASHES[@]} chunks for large file"

    # Build manifest JSON array
    MANIFEST_JSON=$(printf '%s\n' "${CHUNK_HASHES[@]}" | jq -R . | jq -s .)

    # Commit with all chunk hashes
    COMMIT_RESPONSE=$(http_post_json "/v1/uploads/$LARGE_ID/commit" "{
        \"manifest\": $MANIFEST_JSON
    }" 2>/dev/null || echo '{}')
    log_info "Large file commit response: $COMMIT_RESPONSE"
fi

# Test 7: Verify S3 object metadata
log_info "Checking S3 object metadata..."
OBJECTS=$(aws --endpoint-url "$MINIO_ENDPOINT" s3api list-objects-v2 --bucket "$BUCKET_NAME" --prefix "cache/" --max-keys 5 2>/dev/null || echo '{}')
if echo "$OBJECTS" | jq -e '.Contents' > /dev/null 2>&1; then
    OBJECT_COUNT=$(echo "$OBJECTS" | jq '.Contents | length')
    log_info "Total objects in cache prefix: $OBJECT_COUNT"

    # Get details of first object
    FIRST_KEY=$(echo "$OBJECTS" | jq -r '.Contents[0].Key // empty')
    if [ -n "$FIRST_KEY" ]; then
        OBJECT_HEAD=$(aws --endpoint-url "$MINIO_ENDPOINT" s3api head-object --bucket "$BUCKET_NAME" --key "$FIRST_KEY" 2>/dev/null || echo '{}')
        log_info "Sample object metadata:"
        echo "$OBJECT_HEAD" | jq '{ContentLength, ContentType, ETag}'
    fi
fi

log_info "S3 storage tests completed!"
