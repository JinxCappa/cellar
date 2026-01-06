#!/bin/bash
# Generate Ed25519 signing keypair for testing
# Usage: generate-test-key.sh [key-name] [output-dir]

set -euo pipefail

KEY_NAME="${1:-test-cache-1}"
OUTPUT_DIR="${2:-.}"

PRIVATE_KEY_FILE="${OUTPUT_DIR}/${KEY_NAME}.sec"
PUBLIC_KEY_FILE="${OUTPUT_DIR}/${KEY_NAME}.pub"

echo "Generating Ed25519 keypair: $KEY_NAME"

# Check if cellarctl is available
if command -v cellarctl &> /dev/null; then
    echo "Using cellarctl to generate key..."
    cellarctl key generate --name "$KEY_NAME" --output "$PRIVATE_KEY_FILE"
    cellarctl key public --key "$PRIVATE_KEY_FILE" > "$PUBLIC_KEY_FILE"
else
    # Fallback to OpenSSL
    echo "Using OpenSSL to generate key..."

    # Generate private key
    openssl genpkey -algorithm Ed25519 -out "${OUTPUT_DIR}/${KEY_NAME}.pem"

    # Extract raw private key bytes and encode
    PRIVATE_KEY=$(openssl pkey -in "${OUTPUT_DIR}/${KEY_NAME}.pem" -outform DER | tail -c 32 | base64 -w0)

    # Extract public key
    PUBLIC_KEY=$(openssl pkey -in "${OUTPUT_DIR}/${KEY_NAME}.pem" -pubout -outform DER | tail -c 32 | base64 -w0)

    # Format in Nix style
    echo "${KEY_NAME}:${PRIVATE_KEY}" > "$PRIVATE_KEY_FILE"
    echo "${KEY_NAME}:${PUBLIC_KEY}" > "$PUBLIC_KEY_FILE"

    # Clean up PEM file
    rm -f "${OUTPUT_DIR}/${KEY_NAME}.pem"
fi

echo "Private key: $PRIVATE_KEY_FILE"
echo "Public key: $PUBLIC_KEY_FILE"

# Set restrictive permissions on private key
chmod 600 "$PRIVATE_KEY_FILE"
chmod 644 "$PUBLIC_KEY_FILE"

echo "Done!"
