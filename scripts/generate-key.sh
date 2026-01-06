#!/usr/bin/env bash
# Generate a signing key pair for the Cellar cache

set -euo pipefail

KEY_NAME="${1:-cache.example.com-1}"
OUTPUT_DIR="${2:-./data}"

mkdir -p "$OUTPUT_DIR"

echo "Generating signing key pair for: $KEY_NAME"
echo

# Use nix-store to generate key if available, otherwise use our CLI
if command -v nix-store &> /dev/null; then
    nix-store --generate-binary-cache-key "$KEY_NAME" \
        "$OUTPUT_DIR/cache-key.sec" \
        "$OUTPUT_DIR/cache-key.pub"
    echo "Secret key: $OUTPUT_DIR/cache-key.sec"
    echo "Public key: $OUTPUT_DIR/cache-key.pub"
else
    echo "nix-store not found, using cellarctl..."
    cargo run --bin cellarctl -- key generate --name "$KEY_NAME" --output "$OUTPUT_DIR/cache-key.sec"
fi

echo
echo "Add this to your nix.conf:"
echo "  trusted-public-keys = $(cat "$OUTPUT_DIR/cache-key.pub" 2>/dev/null || echo '<public-key>')"
