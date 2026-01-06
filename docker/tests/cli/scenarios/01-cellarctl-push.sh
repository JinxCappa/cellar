#!/usr/bin/env bash
# Test 01: cellarctl push - Basic Push Test
# Tests pushing a simple Nix package using cellarctl

set -euo pipefail

echo "[INFO] Test 01: cellarctl push - Basic Push Test"

# Setup cellarctl configuration
# cellarctl can use CELLAR_SERVER and CELLAR_TOKEN env vars
export CELLAR_SERVER="${CELLAR_SERVER_URL}"
export CELLAR_TOKEN="${CELLAR_TEST_TOKEN}"

echo "[INFO] Server URL: $CELLAR_SERVER"
echo "[INFO] Token: ${CELLAR_TOKEN:0:20}..."

# Test 1: Verify cellarctl is installed
echo "[INFO] Checking cellarctl installation..."
if ! command -v cellarctl &> /dev/null; then
    echo "[ERROR] cellarctl not found"
    exit 1
fi
echo "[INFO] cellarctl version: $(cellarctl --version 2>/dev/null || echo 'unknown')"

# Test 2: Login to server
echo "[INFO] Logging in to server..."
cellarctl login test-cache "$CELLAR_SERVER" --token "$CELLAR_TOKEN" --set-default

echo "[INFO] Login successful"

# Test 3: Build a simple test derivation
echo "[INFO] Building test derivation..."
STORE_PATH=$(nix-build --no-out-link -E "$(cat <<'NIXEOF'
  with import <nixpkgs> {};
  runCommand "cellar-test-pkg" {} ''
    mkdir -p $out/bin
    echo "#!/bin/sh" > $out/bin/hello
    echo "echo Hello from Cellar CLI test!" >> $out/bin/hello
    chmod +x $out/bin/hello
  ''
NIXEOF
)")

if [ -z "$STORE_PATH" ] || [ ! -d "$STORE_PATH" ]; then
    echo "[ERROR] Failed to build test derivation"
    exit 1
fi

echo "[INFO] Built: $STORE_PATH"

# Get store path info
NAR_SIZE=$(nix-store --query --size "$STORE_PATH")
NAR_HASH=$(nix-store --query --hash "$STORE_PATH")
STORE_PATH_HASH=$(basename "$STORE_PATH" | cut -d'-' -f1)

echo "[INFO] Store path hash: $STORE_PATH_HASH"
echo "[INFO] NAR hash: $NAR_HASH"
echo "[INFO] NAR size: $NAR_SIZE bytes"

# Test 4: Push using cellarctl
echo "[INFO] Pushing store path with cellarctl..."
cellarctl push "$STORE_PATH"

echo "[INFO] Push completed"

# Test 5: Verify the store path was uploaded
echo "[INFO] Verifying upload..."

# Check narinfo endpoint (requires auth since we're using a global token)
NARINFO_URL="$CELLAR_SERVER/${STORE_PATH_HASH}.narinfo"
echo "[INFO] Fetching narinfo from $NARINFO_URL"

NARINFO=$(curl -sf -H "Authorization: Bearer $CELLAR_TOKEN" "$NARINFO_URL" || echo "")
if [ -z "$NARINFO" ]; then
    echo "[ERROR] Failed to fetch narinfo - store path not found in cache"
    exit 1
fi

echo "[INFO] Narinfo received:"
echo "$NARINFO" | head -10

# Verify narinfo contains expected fields
if ! echo "$NARINFO" | grep -q "^StorePath:"; then
    echo "[ERROR] narinfo missing StorePath field"
    exit 1
fi

if ! echo "$NARINFO" | grep -q "^URL:"; then
    echo "[ERROR] narinfo missing URL field"
    exit 1
fi

if ! echo "$NARINFO" | grep -q "^NarHash:"; then
    echo "[ERROR] narinfo missing NarHash field"
    exit 1
fi

echo "[INFO] narinfo validation passed"

# Test 6: Verify NAR can be downloaded (requires auth since we're using a global token)
NAR_URL=$(echo "$NARINFO" | grep "^URL:" | cut -d' ' -f2)
echo "[INFO] Fetching NAR from $CELLAR_SERVER/$NAR_URL"

NAR_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $CELLAR_TOKEN" "$CELLAR_SERVER/$NAR_URL")
if [ "$NAR_STATUS" != "200" ]; then
    echo "[ERROR] Failed to fetch NAR - got HTTP $NAR_STATUS"
    exit 1
fi

echo "[INFO] NAR download successful (HTTP 200)"

echo ""
echo "========================================"
echo "[PASS] cellarctl push test passed!"
echo "========================================"
