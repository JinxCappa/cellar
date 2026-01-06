#!/usr/bin/env bash
# Test 05: cellarctl whoami + login config
# Validates whoami output and config file creation on login

set -euo pipefail

echo "[INFO] Test 05: cellarctl whoami + login config"

export CELLAR_SERVER="${CELLAR_SERVER_URL}"
export CELLAR_TOKEN="${CELLAR_TEST_TOKEN}"

CONFIG_PATH="/tmp/cellar-client-${RANDOM}.toml"
export CELLAR_CLIENT_CONFIG="$CONFIG_PATH"

echo "[INFO] Running whoami..."
WHOAMI_OUTPUT=$(cellarctl whoami)

if ! echo "$WHOAMI_OUTPUT" | grep -q "Token ID:"; then
    echo "[ERROR] whoami missing Token ID"
    echo "$WHOAMI_OUTPUT"
    exit 1
fi

if ! echo "$WHOAMI_OUTPUT" | grep -q "Scopes:"; then
    echo "[ERROR] whoami missing Scopes"
    echo "$WHOAMI_OUTPUT"
    exit 1
fi

echo "[INFO] Logging in with config path: $CONFIG_PATH"
cellarctl login clitest "$CELLAR_SERVER" --token "$CELLAR_TOKEN" --set-default

if [ ! -f "$CONFIG_PATH" ]; then
    echo "[ERROR] Client config not created"
    exit 1
fi

if ! grep -q 'default_cache = "clitest"' "$CONFIG_PATH"; then
    echo "[ERROR] default_cache not set in client config"
    cat "$CONFIG_PATH"
    exit 1
fi

if ! grep -q '\[caches.clitest\]' "$CONFIG_PATH"; then
    echo "[ERROR] cache profile not written in client config"
    cat "$CONFIG_PATH"
    exit 1
fi

if ! grep -q 'token = "' "$CONFIG_PATH"; then
    echo "[ERROR] token missing in client config"
    cat "$CONFIG_PATH"
    exit 1
fi

echo ""
echo "========================================"
echo "[PASS] cellarctl whoami/login config test passed!"
echo "========================================"
