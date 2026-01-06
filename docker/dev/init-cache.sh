#!/usr/bin/env bash
# Create default public cache on the dev server
set -euo pipefail

SERVER="${CELLAR_SERVER:-http://server:8080}"
TOKEN="${CELLAR_TOKEN:-cellar-test-token-12345}"

echo "Waiting for server..."
for i in $(seq 1 30); do
    if curl -sf "$SERVER/v1/health" > /dev/null 2>&1; then
        break
    fi
    sleep 1
done

# Check if a default cache already exists
EXISTING=$(curl -sf -H "Authorization: Bearer $TOKEN" "$SERVER/v1/admin/caches" 2>/dev/null || echo '{}')
if echo "$EXISTING" | jq -e '.caches[]? | select(.is_default==true)' > /dev/null 2>&1; then
    echo "Default cache already exists, skipping creation"
    CACHE_ID=$(echo "$EXISTING" | jq -r '.caches[] | select(.is_default==true) | .cache_id')
    echo "Cache ID: $CACHE_ID"
    exit 0
fi

echo "Creating default public cache..."
RESPONSE=$(curl -sf -X POST \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"cache_name":"dev","is_public":true,"is_default":true}' \
    "$SERVER/v1/admin/caches")

CACHE_ID=$(echo "$RESPONSE" | jq -r '.cache_id')
echo "Created cache: $CACHE_ID"
