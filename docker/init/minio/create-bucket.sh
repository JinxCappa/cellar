#!/bin/bash
# MinIO bucket initialization script
# This script creates the cellar bucket and sets up access policies

set -euo pipefail

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin123}"
BUCKET_NAME="${BUCKET_NAME:-cellar}"

echo "Initializing MinIO bucket: $BUCKET_NAME"

# Configure mc alias
mc alias set local "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# Create bucket if it doesn't exist
if mc ls local/"$BUCKET_NAME" >/dev/null 2>&1; then
    echo "Bucket $BUCKET_NAME already exists"
else
    echo "Creating bucket $BUCKET_NAME..."
    mc mb local/"$BUCKET_NAME"
fi

# Set anonymous read policy for NAR downloads
# This allows clients to download NARs without authentication
echo "Setting access policy for NAR downloads..."
mc anonymous set download local/"$BUCKET_NAME"/cache/nar

echo "MinIO bucket initialization complete"
