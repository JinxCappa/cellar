#!/usr/bin/env bash
# Test 03: cellarctl token CRUD
# Tests offline token generation and server-backed create/list/revoke

set -euo pipefail

echo "[INFO] Test 03: cellarctl token CRUD"

export CELLAR_SERVER="${CELLAR_SERVER_URL}"
export CELLAR_TOKEN="${CELLAR_TEST_TOKEN}"

# Test 1: Offline token generation
echo "[INFO] Generating offline token..."
GENERATE_OUTPUT=$(cellarctl token generate --description "cli offline token")

if ! echo "$GENERATE_OUTPUT" | grep -q "Secret:"; then
    echo "[ERROR] Token generate output missing Secret"
    exit 1
fi

if ! echo "$GENERATE_OUTPUT" | grep -q "sha256:"; then
    echo "[ERROR] Token generate output missing sha256 hash"
    exit 1
fi

# Test 2: Create token
echo "[INFO] Creating token..."
CREATE_OUTPUT=$(cellarctl token create --scopes "cache:read,cache:write" --description "cli token test")

TOKEN_ID=$(echo "$CREATE_OUTPUT" | sed -n 's/^Token ID: //p' | head -1)
TOKEN_SECRET=$(echo "$CREATE_OUTPUT" | sed -n 's/^Token secret: //p' | head -1)

if [ -z "${TOKEN_ID:-}" ] || [ -z "${TOKEN_SECRET:-}" ]; then
    echo "[ERROR] Failed to parse token ID/secret"
    echo "$CREATE_OUTPUT"
    exit 1
fi

echo "[INFO] Token ID: $TOKEN_ID"

# Test 3: List tokens and verify active
echo "[INFO] Listing tokens..."
LIST_OUTPUT=$(cellarctl token list)

if ! echo "$LIST_OUTPUT" | grep -q "$TOKEN_ID"; then
    echo "[ERROR] Token not found in list"
    exit 1
fi

TOKEN_LINE=$(echo "$LIST_OUTPUT" | grep "$TOKEN_ID" || true)
if ! echo "$TOKEN_LINE" | grep -q "active"; then
    echo "[ERROR] Token not marked active in list"
    echo "$TOKEN_LINE"
    exit 1
fi

# Test 4: Revoke token and verify revoked
echo "[INFO] Revoking token..."
cellarctl token revoke "$TOKEN_ID"

LIST_OUTPUT=$(cellarctl token list)
TOKEN_LINE=$(echo "$LIST_OUTPUT" | grep "$TOKEN_ID" || true)

if ! echo "$TOKEN_LINE" | grep -q "revoked"; then
    echo "[ERROR] Token not marked revoked in list"
    echo "$TOKEN_LINE"
    exit 1
fi

echo ""
echo "========================================"
echo "[PASS] cellarctl token CRUD test passed!"
echo "========================================"
