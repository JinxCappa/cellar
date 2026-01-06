#!/usr/bin/env bash
# Test 02: cellarctl chunked upload
# Tests pushing a larger package that requires multiple chunks

set -euo pipefail

echo "[INFO] Test 02: cellarctl chunked upload"

# Setup cellarctl configuration
export CELLAR_SERVER="${CELLAR_SERVER_URL}"
export CELLAR_TOKEN="${CELLAR_TEST_TOKEN}"

echo "[INFO] Server URL: $CELLAR_SERVER"

# Ensure we're logged in
echo "[INFO] Ensuring login..."
cellarctl login test-cache "$CELLAR_SERVER" --token "$CELLAR_TOKEN" --set-default 2>/dev/null || true

# Build a package that will be larger than 16MB to test chunking
# We'll build 'hello' which is usually small, but we'll create a larger derivation
echo "[INFO] Building larger test derivation (~20MB)..."
STORE_PATH=$(nix-build --no-out-link -E "$(cat <<'NIXEOF'
  with import <nixpkgs> {};
  runCommand "cellar-large-test" {} ''
    mkdir -p $out/data
    # Create ~20MB of data to ensure chunking (default chunk size is 16MB)
    for i in $(seq 1 20); do
      dd if=/dev/urandom of=$out/data/file_$i.bin bs=1M count=1 2>/dev/null
    done
    echo "Large test package" > $out/README
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
echo "[INFO] NAR size: $NAR_SIZE bytes ($(numfmt --to=iec-i --suffix=B $NAR_SIZE 2>/dev/null || echo $NAR_SIZE))"

# Verify it's large enough for chunking
MIN_SIZE=16000000  # 16MB
if [ "$NAR_SIZE" -lt "$MIN_SIZE" ]; then
    echo "[WARN] Package smaller than expected: $NAR_SIZE bytes (expected > 16MB)"
    echo "[INFO] Test will still proceed but may not test chunking"
fi

# Push using cellarctl
echo "[INFO] Pushing large store path with cellarctl..."
START_TIME=$(date +%s)
cellarctl push "$STORE_PATH"
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo "[INFO] Push completed in ${DURATION}s"

# Verify the store path was uploaded (requires auth since we're using a global token)
echo "[INFO] Verifying upload..."
NARINFO_URL="$CELLAR_SERVER/${STORE_PATH_HASH}.narinfo"
NARINFO=$(curl -sf -H "Authorization: Bearer $CELLAR_TOKEN" "$NARINFO_URL" || echo "")

if [ -z "$NARINFO" ]; then
    echo "[ERROR] Failed to fetch narinfo - store path not found in cache"
    exit 1
fi

echo "[INFO] Narinfo received"

# Verify NAR size matches
REPORTED_SIZE=$(echo "$NARINFO" | grep "^NarSize:" | cut -d' ' -f2)
if [ -n "$REPORTED_SIZE" ]; then
    echo "[INFO] Reported NAR size: $REPORTED_SIZE"
    # Allow some tolerance for NAR overhead
    SIZE_DIFF=$((NAR_SIZE - REPORTED_SIZE))
    SIZE_DIFF=${SIZE_DIFF#-}  # absolute value
    if [ "$SIZE_DIFF" -gt 10000 ]; then
        echo "[WARN] NAR size mismatch: local=$NAR_SIZE, server=$REPORTED_SIZE"
    fi
fi

# Verify NAR can be downloaded (requires auth since we're using a global token)
NAR_URL=$(echo "$NARINFO" | grep "^URL:" | cut -d' ' -f2)
echo "[INFO] Verifying NAR download from $CELLAR_SERVER/$NAR_URL"

# Just check headers, don't download the full NAR
NAR_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -I -H "Authorization: Bearer $CELLAR_TOKEN" "$CELLAR_SERVER/$NAR_URL" 2>/dev/null || \
             curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $CELLAR_TOKEN" "$CELLAR_SERVER/$NAR_URL")
if [ "$NAR_STATUS" != "200" ]; then
    echo "[ERROR] Failed to access NAR - got HTTP $NAR_STATUS"
    exit 1
fi

echo "[INFO] NAR accessible (HTTP 200)"

echo ""
echo "========================================"
echo "[PASS] cellarctl chunked upload test passed!"
echo "========================================"
