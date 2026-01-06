#!/usr/bin/env bash
# Test 08: cellarctl use --set-nix-conf
# Tests login with a cache-scoped token and nix.conf updates

set -euo pipefail

echo "[INFO] Test 08: cellarctl use --set-nix-conf"

export CELLAR_SERVER="${CELLAR_SERVER_URL}"
export CELLAR_TOKEN="${CELLAR_TEST_TOKEN}"

CACHE_NAME="cli-public-${RANDOM}"
BASE_URL="http://server:8080"

echo "[INFO] Creating public cache..."
CREATE_OUTPUT=$(cellarctl cache create --name "$CACHE_NAME" --public --base-url "$BASE_URL")
CACHE_ID=$(echo "$CREATE_OUTPUT" | sed -n 's/^Cache ID: //p' | head -1)

if [ -z "${CACHE_ID:-}" ]; then
    echo "[ERROR] Failed to parse cache ID"
    echo "$CREATE_OUTPUT"
    exit 1
fi

echo "[INFO] Creating cache-scoped token..."
TOKEN_OUTPUT=$(cellarctl token create --scopes "cache:read" --cache-id "$CACHE_ID" --description "cli use test")
TOKEN_ID=$(echo "$TOKEN_OUTPUT" | sed -n 's/^Token ID: //p' | head -1)
TOKEN_SECRET=$(echo "$TOKEN_OUTPUT" | sed -n 's/^Token secret: //p' | head -1)

if [ -z "${TOKEN_ID:-}" ] || [ -z "${TOKEN_SECRET:-}" ]; then
    echo "[ERROR] Failed to parse token output"
    echo "$TOKEN_OUTPUT"
    exit 1
fi

CONFIG_PATH="/tmp/cellar-client-use-${RANDOM}.toml"
export CELLAR_CLIENT_CONFIG="$CONFIG_PATH"
export XDG_CONFIG_HOME="/tmp/cellar-xdg-${RANDOM}"

echo "[INFO] Logging in with cache-scoped token..."
cellarctl login usecache "$CELLAR_SERVER" --token "$TOKEN_SECRET" --set-default

echo "[INFO] Updating nix.conf via use command..."
cellarctl use usecache --set-default --set-nix-conf

NIX_CONF="${XDG_CONFIG_HOME}/nix/nix.conf"
if [ ! -f "$NIX_CONF" ]; then
    echo "[ERROR] nix.conf was not created"
    exit 1
fi

if ! grep -q "substituters = ${BASE_URL}" "$NIX_CONF"; then
    echo "[ERROR] nix.conf missing substituters entry"
    cat "$NIX_CONF"
    exit 1
fi

if ! grep -q "trusted-public-keys = " "$NIX_CONF"; then
    echo "[ERROR] nix.conf missing trusted-public-keys entry"
    cat "$NIX_CONF"
    exit 1
fi

echo "[INFO] Cleaning up token and cache..."
cellarctl token revoke "$TOKEN_ID"
cellarctl cache delete "$CACHE_ID" --force

echo ""
echo "========================================"
echo "[PASS] cellarctl use --set-nix-conf test passed!"
echo "========================================"
