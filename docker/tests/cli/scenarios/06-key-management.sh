#!/usr/bin/env bash
# Test 06: cellarctl key management
# Tests key generation and public key extraction

set -euo pipefail

echo "[INFO] Test 06: cellarctl key management"

KEY_NAME="cli-key-${RANDOM}"
KEY_FILE="/tmp/${KEY_NAME}.sec"

echo "[INFO] Generating key: $KEY_NAME"
GENERATE_OUTPUT=$(cellarctl key generate --name "$KEY_NAME" --output "$KEY_FILE")

if [ ! -s "$KEY_FILE" ]; then
    echo "[ERROR] Secret key file not created"
    echo "$GENERATE_OUTPUT"
    exit 1
fi

echo "[INFO] Reading public key..."
PUBLIC_OUTPUT=$(cellarctl key public --key-file "$KEY_FILE")

if ! echo "$PUBLIC_OUTPUT" | grep -q "Public key:"; then
    echo "[ERROR] key public output missing header"
    echo "$PUBLIC_OUTPUT"
    exit 1
fi

if ! echo "$PUBLIC_OUTPUT" | grep -q "^${KEY_NAME}:"; then
    echo "[ERROR] public key missing expected name prefix"
    echo "$PUBLIC_OUTPUT"
    exit 1
fi

echo ""
echo "========================================"
echo "[PASS] cellarctl key management test passed!"
echo "========================================"
