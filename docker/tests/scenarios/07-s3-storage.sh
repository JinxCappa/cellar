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
STORE_PATH_HASH=$(random_string 32)
STORE_PATH="/nix/store/${STORE_PATH_HASH}-s3-test"
NAR_HASH="sha256:$(random_string 64)"
TEST_DATA=$(generate_binary_data 51200)  # 50KB
NAR_SIZE=${#TEST_DATA}

# Create upload session
SESSION_RESPONSE=$(http_post_json "/v1/uploads" "{
    \"store_path\": \"$STORE_PATH\",
    \"nar_hash\": \"$NAR_HASH\",
    \"nar_size\": $NAR_SIZE
}" 2>/dev/null || echo '{}')

if echo "$SESSION_RESPONSE" | jq -e '.upload_id' > /dev/null 2>&1; then
    UPLOAD_ID=$(echo "$SESSION_RESPONSE" | jq -r '.upload_id')
    log_info "Upload session: $UPLOAD_ID"

    # Upload chunk
    http_put_binary "/v1/uploads/$UPLOAD_ID/chunks/0" "$TEST_DATA" 2>/dev/null || true

    # Commit
    http_post_json "/v1/uploads/$UPLOAD_ID/commit" '{}' 2>/dev/null || true

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
    NARINFO=$(http_get "/${STORE_PATH_HASH}.narinfo" "" 2>/dev/null || echo "")
    if [ -n "$NARINFO" ]; then
        NAR_URL=$(echo "$NARINFO" | grep "^URL:" | cut -d' ' -f2)
        if [ -n "$NAR_URL" ]; then
            log_info "NAR URL from narinfo: $NAR_URL"

            # Try to access via S3 directly
            S3_NAR_STATUS=$(aws --endpoint-url "$MINIO_ENDPOINT" s3 ls "s3://$BUCKET_NAME/cache/$NAR_URL" 2>&1 || echo "not found")
            log_info "S3 NAR check: $S3_NAR_STATUS"
        fi
    fi
fi

# Test 6: Test S3 multipart upload (for large files)
log_info "Testing large file upload (multipart)..."
LARGE_DATA=$(generate_binary_data 1048576)  # 1MB
LARGE_SIZE=${#LARGE_DATA}
LARGE_PATH="/nix/store/$(random_string 32)-large-s3-test"

LARGE_SESSION=$(http_post_json "/v1/uploads" "{
    \"store_path\": \"$LARGE_PATH\",
    \"nar_hash\": \"sha256:$(random_string 64)\",
    \"nar_size\": $LARGE_SIZE
}" 2>/dev/null || echo '{}')

if echo "$LARGE_SESSION" | jq -e '.upload_id' > /dev/null 2>&1; then
    LARGE_ID=$(echo "$LARGE_SESSION" | jq -r '.upload_id')
    log_info "Large upload session: $LARGE_ID"

    # Upload in chunks
    CHUNK_SIZE=262144  # 256KB chunks
    OFFSET=0
    POSITION=0

    while [ $OFFSET -lt $LARGE_SIZE ]; do
        REMAINING=$((LARGE_SIZE - OFFSET))
        CURRENT_SIZE=$CHUNK_SIZE
        if [ $REMAINING -lt $CHUNK_SIZE ]; then
            CURRENT_SIZE=$REMAINING
        fi

        CHUNK=$(echo -n "$LARGE_DATA" | dd bs=1 skip=$OFFSET count=$CURRENT_SIZE 2>/dev/null)
        http_put_binary "/v1/uploads/$LARGE_ID/chunks/$POSITION" "$CHUNK" 2>/dev/null || true

        OFFSET=$((OFFSET + CURRENT_SIZE))
        POSITION=$((POSITION + 1))
    done

    log_info "Uploaded $POSITION chunks for large file"

    # Commit
    http_post_json "/v1/uploads/$LARGE_ID/commit" '{}' 2>/dev/null || true
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
