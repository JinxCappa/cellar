#!/usr/bin/env bash
# Test 04: cellarctl cache CRUD
# Tests create/list/show/update/delete cache operations

set -euo pipefail

echo "[INFO] Test 04: cellarctl cache CRUD"

export CELLAR_SERVER="${CELLAR_SERVER_URL}"
export CELLAR_TOKEN="${CELLAR_TEST_TOKEN}"

CACHE_NAME="cli-cache-${RANDOM}"
UPDATED_NAME="${CACHE_NAME}-updated"
BASE_URL="http://cache.example.test"

echo "[INFO] Creating cache: $CACHE_NAME"
CREATE_OUTPUT=$(cellarctl cache create --name "$CACHE_NAME" --public --base-url "$BASE_URL")
CACHE_ID=$(echo "$CREATE_OUTPUT" | sed -n 's/^Cache ID: //p' | head -1)

if [ -z "${CACHE_ID:-}" ]; then
    echo "[ERROR] Failed to parse cache ID"
    echo "$CREATE_OUTPUT"
    exit 1
fi

echo "[INFO] Cache ID: $CACHE_ID"

echo "[INFO] Listing caches..."
LIST_OUTPUT=$(cellarctl cache list)
if ! echo "$LIST_OUTPUT" | grep -q "$CACHE_ID"; then
    echo "[ERROR] Cache not found in list"
    exit 1
fi

echo "[INFO] Showing cache..."
SHOW_OUTPUT=$(cellarctl cache show "$CACHE_ID")
if ! echo "$SHOW_OUTPUT" | grep -q "Name: $CACHE_NAME"; then
    echo "[ERROR] Cache show did not include expected name"
    echo "$SHOW_OUTPUT"
    exit 1
fi

echo "[INFO] Updating cache..."
cellarctl cache update "$CACHE_ID" --name "$UPDATED_NAME"

SHOW_OUTPUT=$(cellarctl cache show "$CACHE_ID")
if ! echo "$SHOW_OUTPUT" | grep -q "Name: $UPDATED_NAME"; then
    echo "[ERROR] Cache name not updated"
    echo "$SHOW_OUTPUT"
    exit 1
fi

echo "[INFO] Deleting cache..."
cellarctl cache delete "$CACHE_ID" --force

LIST_OUTPUT=$(cellarctl cache list)
if echo "$LIST_OUTPUT" | grep -q "$CACHE_ID"; then
    echo "[ERROR] Cache still present after deletion"
    exit 1
fi

echo ""
echo "========================================"
echo "[PASS] cellarctl cache CRUD test passed!"
echo "========================================"
