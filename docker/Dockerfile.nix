# Dockerfile for Nix-enabled builder and client containers
# Used for integration testing with real Nix tooling
FROM nixos/nix:2.24.10 AS nix-base

# Enable flakes and nix-command
RUN mkdir -p /etc/nix && \
    echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf && \
    echo "sandbox = false" >> /etc/nix/nix.conf

# Install common utilities
RUN nix-env -iA nixpkgs.curl nixpkgs.jq nixpkgs.bash nixpkgs.coreutils nixpkgs.gnugrep

# Create working directory
WORKDIR /workspace

# -------------------------------------------------------------------
# Builder image - for building and pushing packages to the cache
# -------------------------------------------------------------------
FROM nix-base AS builder

# Copy test scripts
COPY docker/tests/lib/test-utils.sh /usr/local/lib/
COPY docker/scripts/wait-for-it.sh /usr/local/bin/

RUN chmod +x /usr/local/bin/wait-for-it.sh

# Environment variables for cache configuration
ENV CELLAR_SERVER_URL=http://server:8080
ENV CELLAR_TOKEN=""

# Script to push a store path to the cache (supports large files and resume)
COPY --chmod=755 <<'EOF' /usr/local/bin/push-to-cache
#!/usr/bin/env bash
set -euo pipefail

STORE_PATH="${1:?Store path required}"
SERVER_URL="${CELLAR_SERVER_URL:?Server URL required}"
TOKEN="${CELLAR_TOKEN:?Token required}"
RESUME_ID="${2:-}"  # Optional: resume an existing upload

echo "========================================"
echo "Pushing $STORE_PATH to $SERVER_URL"
echo "========================================"

# Get NAR info
NAR_HASH=$(nix-store --query --hash "$STORE_PATH")
NAR_SIZE=$(nix-store --query --size "$STORE_PATH")
STORE_PATH_HASH=$(basename "$STORE_PATH" | cut -d'-' -f1)

echo "Store Path Hash: $STORE_PATH_HASH"
echo "NAR Hash: $NAR_HASH"
echo "NAR Size: $NAR_SIZE bytes ($(numfmt --to=iec-i --suffix=B $NAR_SIZE))"

# Dump NAR to temp file
TEMP_NAR=$(mktemp)
trap "rm -f $TEMP_NAR" EXIT
echo "Dumping NAR to temp file..."
nix-store --dump "$STORE_PATH" > "$TEMP_NAR"
ACTUAL_SIZE=$(stat -c%s "$TEMP_NAR")
echo "NAR dumped: $ACTUAL_SIZE bytes"

# Start or resume upload session
if [ -n "$RESUME_ID" ]; then
    echo "Resuming upload session: $RESUME_ID"
    UPLOAD_ID="$RESUME_ID"
    # Get session info to find chunk size and completed chunks
    SESSION=$(curl -sf -X GET "$SERVER_URL/v1/uploads/$UPLOAD_ID" \
        -H "Authorization: Bearer $TOKEN")
    CHUNK_SIZE=$(echo "$SESSION" | jq -r '.chunk_size')
    # Get list of already uploaded chunks
    COMPLETED_CHUNKS=$(echo "$SESSION" | jq -r '.completed_chunks // [] | .[]' 2>/dev/null || echo "")
else
    echo "Creating new upload session..."
    SESSION=$(curl -sf -X POST "$SERVER_URL/v1/uploads" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d "{
            \"store_path\": \"$STORE_PATH\",
            \"nar_hash\": \"$NAR_HASH\",
            \"nar_size\": $NAR_SIZE
        }")
    UPLOAD_ID=$(echo "$SESSION" | jq -r '.upload_id')
    CHUNK_SIZE=$(echo "$SESSION" | jq -r '.chunk_size')
    COMPLETED_CHUNKS=""
fi

echo "Upload ID: $UPLOAD_ID"
echo "Chunk Size: $CHUNK_SIZE bytes ($(numfmt --to=iec-i --suffix=B $CHUNK_SIZE))"

# Calculate total chunks
TOTAL_CHUNKS=$(( (ACTUAL_SIZE + CHUNK_SIZE - 1) / CHUNK_SIZE ))
echo "Total Chunks: $TOTAL_CHUNKS"
echo "----------------------------------------"

# Upload chunks
UPLOADED=0
SKIPPED=0
FAILED=0

for POSITION in $(seq 0 $((TOTAL_CHUNKS - 1))); do
    # Check if chunk already uploaded (for resume)
    if echo "$COMPLETED_CHUNKS" | grep -q "^${POSITION}$"; then
        echo "Chunk $POSITION/$((TOTAL_CHUNKS - 1)): SKIPPED (already uploaded)"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Calculate offset and size for this chunk
    OFFSET=$((POSITION * CHUNK_SIZE))
    REMAINING=$((ACTUAL_SIZE - OFFSET))
    THIS_CHUNK_SIZE=$CHUNK_SIZE
    if [ $REMAINING -lt $CHUNK_SIZE ]; then
        THIS_CHUNK_SIZE=$REMAINING
    fi

    echo -n "Chunk $POSITION/$((TOTAL_CHUNKS - 1)): uploading $THIS_CHUNK_SIZE bytes... "

    # Upload chunk using dd for precise byte handling
    HTTP_CODE=$(dd if="$TEMP_NAR" bs=1 skip=$OFFSET count=$THIS_CHUNK_SIZE 2>/dev/null | \
        curl -s -w "%{http_code}" -o /dev/null \
        -X PUT "$SERVER_URL/v1/uploads/$UPLOAD_ID/chunks/$POSITION" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/octet-stream" \
        --data-binary @-)

    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "204" ]; then
        echo "OK"
        UPLOADED=$((UPLOADED + 1))
    else
        echo "FAILED (HTTP $HTTP_CODE)"
        FAILED=$((FAILED + 1))
    fi
done

echo "----------------------------------------"
echo "Upload Summary:"
echo "  Uploaded: $UPLOADED"
echo "  Skipped:  $SKIPPED"
echo "  Failed:   $FAILED"

if [ $FAILED -gt 0 ]; then
    echo "ERROR: Some chunks failed to upload"
    echo "Resume with: push-to-cache \"$STORE_PATH\" \"$UPLOAD_ID\""
    exit 1
fi

# Commit upload
echo "Committing upload..."
COMMIT_RESPONSE=$(curl -sf -X POST "$SERVER_URL/v1/uploads/$UPLOAD_ID/commit" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{}')

echo "========================================"
echo "SUCCESS: Pushed $STORE_PATH"
echo "========================================"
EOF

# Script to build and push a simple derivation (quick smoke test)
COPY --chmod=755 <<'EOF' /usr/local/bin/build-and-push-simple
#!/usr/bin/env bash
set -euo pipefail

NAME="${1:-test-package}"
CONTENT="${2:-Hello from Cellar test!}"

# Validate NAME to prevent command injection - only allow alphanumeric, hyphens, and underscores
if [[ ! "$NAME" =~ ^[a-zA-Z0-9_-]+$ ]]; then
  echo "Error: NAME must contain only alphanumeric characters, hyphens, and underscores" >&2
  exit 1
fi

echo "Building simple test derivation: $NAME"

# Create a simple derivation
# Use printf %q to safely quote CONTENT to prevent injection
STORE_PATH=$(nix-build --no-out-link -E "
  with import <nixpkgs> {};
  runCommand \"$NAME\" {} ''
    mkdir -p \$out/bin
    echo '#!${bash}/bin/bash' > \$out/bin/$NAME
    printf 'echo %s\n' $(printf %q "$CONTENT") >> \$out/bin/$NAME
    chmod +x \$out/bin/$NAME
  ''
")

echo "Built: $STORE_PATH"
push-to-cache "$STORE_PATH"
EOF

# Script to build and push a real nixpkgs package (for chunked upload testing)
COPY --chmod=755 <<'EOF' /usr/local/bin/build-and-push-package
#!/usr/bin/env bash
set -euo pipefail

PACKAGE="${1:-vault}"

echo "========================================"
echo "Building nixpkgs package: $PACKAGE"
echo "========================================"

# Build the package
echo "This may take several minutes for large packages..."
STORE_PATH=$(nix-build '<nixpkgs>' -A "$PACKAGE" --no-out-link 2>&1 | tee /dev/stderr | tail -1)

if [ ! -d "$STORE_PATH" ]; then
    echo "ERROR: Build failed or produced unexpected output"
    exit 1
fi

# Get package info
PKG_SIZE=$(nix-store --query --size "$STORE_PATH")
echo "Package built: $STORE_PATH"
echo "Package size: $(numfmt --to=iec-i --suffix=B $PKG_SIZE)"

# Push to cache
push-to-cache "$STORE_PATH"

# Also push runtime dependencies if requested
if [ "${PUSH_DEPS:-0}" = "1" ]; then
    echo ""
    echo "Pushing runtime dependencies..."
    for DEP in $(nix-store --query --references "$STORE_PATH"); do
        if [ "$DEP" != "$STORE_PATH" ]; then
            echo "Pushing dependency: $DEP"
            push-to-cache "$DEP" || echo "Warning: Failed to push $DEP"
        fi
    done
fi
EOF

# Script to build and push vault specifically (the recommended large package test)
COPY --chmod=755 <<'EOF' /usr/local/bin/build-and-push-vault
#!/usr/bin/env bash
set -euo pipefail

echo "========================================"
echo "Building HashiCorp Vault"
echo "========================================"
echo "This is a large Go binary (~150-200 MB NAR)"
echo "Build time: approximately 4-5 minutes"
echo "========================================"

build-and-push-package vault
EOF

# Wrapper script that calls the appropriate build script
COPY --chmod=755 <<'EOF' /usr/local/bin/build-and-push
#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-simple}"

case "$MODE" in
    simple|smoke|quick)
        shift || true
        build-and-push-simple "$@"
        ;;
    vault)
        build-and-push-vault
        ;;
    large|chunked)
        # Default large package is vault
        build-and-push-vault
        ;;
    package|pkg)
        shift
        build-and-push-package "$@"
        ;;
    *)
        # Assume it's a package name
        build-and-push-package "$MODE"
        ;;
esac
EOF

# Script to test resumable uploads (simulates interruption)
COPY --chmod=755 <<'EOF' /usr/local/bin/test-resume-upload
#!/usr/bin/env bash
set -euo pipefail

PACKAGE="${1:-hello}"  # Use a smaller package for resume testing
INTERRUPT_AT="${2:-2}" # Interrupt after N chunks

echo "========================================"
echo "Testing Resumable Upload"
echo "========================================"
echo "Package: $PACKAGE"
echo "Will interrupt after chunk: $INTERRUPT_AT"
echo "========================================"

SERVER_URL="${CELLAR_SERVER_URL:?Server URL required}"
TOKEN="${CELLAR_TOKEN:?Token required}"

# Build the package
echo "Building package..."
STORE_PATH=$(nix-build '<nixpkgs>' -A "$PACKAGE" --no-out-link)
echo "Built: $STORE_PATH"

# Get NAR info
NAR_HASH=$(nix-store --query --hash "$STORE_PATH")
NAR_SIZE=$(nix-store --query --size "$STORE_PATH")

# Dump NAR
TEMP_NAR=$(mktemp)
trap "rm -f $TEMP_NAR" EXIT
nix-store --dump "$STORE_PATH" > "$TEMP_NAR"
ACTUAL_SIZE=$(stat -c%s "$TEMP_NAR")

# Create upload session
echo "Creating upload session..."
SESSION=$(curl -sf -X POST "$SERVER_URL/v1/uploads" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{
        \"store_path\": \"$STORE_PATH\",
        \"nar_hash\": \"$NAR_HASH\",
        \"nar_size\": $NAR_SIZE
    }")

UPLOAD_ID=$(echo "$SESSION" | jq -r '.upload_id')
CHUNK_SIZE=$(echo "$SESSION" | jq -r '.chunk_size')
TOTAL_CHUNKS=$(( (ACTUAL_SIZE + CHUNK_SIZE - 1) / CHUNK_SIZE ))

echo "Upload ID: $UPLOAD_ID"
echo "Total chunks: $TOTAL_CHUNKS"

# Upload only first N chunks (simulating interruption)
echo ""
echo "Phase 1: Partial upload (simulating interruption)..."
for POSITION in $(seq 0 $((INTERRUPT_AT - 1))); do
    if [ $POSITION -ge $TOTAL_CHUNKS ]; then
        break
    fi
    OFFSET=$((POSITION * CHUNK_SIZE))
    REMAINING=$((ACTUAL_SIZE - OFFSET))
    THIS_CHUNK_SIZE=$CHUNK_SIZE
    if [ $REMAINING -lt $CHUNK_SIZE ]; then
        THIS_CHUNK_SIZE=$REMAINING
    fi

    echo "Uploading chunk $POSITION..."
    dd if="$TEMP_NAR" bs=1 skip=$OFFSET count=$THIS_CHUNK_SIZE 2>/dev/null | \
        curl -sf -X PUT "$SERVER_URL/v1/uploads/$UPLOAD_ID/chunks/$POSITION" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/octet-stream" \
        --data-binary @-
done

echo ""
echo "=== SIMULATED INTERRUPTION ==="
echo "Upload ID for resume: $UPLOAD_ID"
echo ""

# Check session status
echo "Checking session status..."
STATUS=$(curl -sf "$SERVER_URL/v1/uploads/$UPLOAD_ID" \
    -H "Authorization: Bearer $TOKEN")
echo "Session state: $(echo "$STATUS" | jq -r '.state')"

# Resume upload
echo ""
echo "Phase 2: Resuming upload..."
push-to-cache "$STORE_PATH" "$UPLOAD_ID"

echo ""
echo "========================================"
echo "Resume test completed successfully!"
echo "========================================"
EOF

CMD ["bash"]

# -------------------------------------------------------------------
# Client image - for pulling packages from the cache
# -------------------------------------------------------------------
FROM nix-base AS client

# Copy test scripts
COPY docker/tests/lib/test-utils.sh /usr/local/lib/
COPY docker/scripts/wait-for-it.sh /usr/local/bin/

RUN chmod +x /usr/local/bin/wait-for-it.sh

# Environment variables
ENV CELLAR_SERVER_URL=http://server:8080
ENV CELLAR_PUBLIC_KEY=""

# Script to configure Nix to use the cache
COPY --chmod=755 <<'EOF' /usr/local/bin/configure-cache
#!/usr/bin/env bash
set -euo pipefail

SERVER_URL="${CELLAR_SERVER_URL:?Server URL required}"
PUBLIC_KEY="${CELLAR_PUBLIC_KEY:-}"

echo "Configuring Nix to use cache at $SERVER_URL..."

# Add cache as substituter
mkdir -p /etc/nix
cat >> /etc/nix/nix.conf <<NIX_CONF
substituters = $SERVER_URL https://cache.nixos.org
trusted-substituters = $SERVER_URL
NIX_CONF

if [ -n "$PUBLIC_KEY" ]; then
    echo "trusted-public-keys = $PUBLIC_KEY cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=" >> /etc/nix/nix.conf
fi

echo "Cache configured successfully"
EOF

# Script to fetch a store path from cache
COPY --chmod=755 <<'EOF' /usr/local/bin/fetch-from-cache
#!/usr/bin/env bash
set -euo pipefail

STORE_PATH_HASH="${1:?Store path hash required}"
SERVER_URL="${CELLAR_SERVER_URL:?Server URL required}"

echo "Fetching $STORE_PATH_HASH from $SERVER_URL..."

# Get narinfo
NARINFO=$(curl -sf "$SERVER_URL/$STORE_PATH_HASH.narinfo")
if [ -z "$NARINFO" ]; then
    echo "ERROR: narinfo not found for $STORE_PATH_HASH"
    exit 1
fi

echo "Found narinfo:"
echo "$NARINFO"

# Extract NAR URL
NAR_URL=$(echo "$NARINFO" | grep "^URL:" | cut -d' ' -f2)
STORE_PATH=$(echo "$NARINFO" | grep "^StorePath:" | cut -d' ' -f2)

echo "Fetching NAR from $NAR_URL..."

# Download NAR
TEMP_NAR=$(mktemp)
trap "rm -f $TEMP_NAR" EXIT
curl -sf "$SERVER_URL/$NAR_URL" > "$TEMP_NAR"

# Import into store
echo "Importing into Nix store..."
nix-store --restore "$STORE_PATH" < "$TEMP_NAR"

echo "Successfully fetched $STORE_PATH"
EOF

CMD ["bash"]
