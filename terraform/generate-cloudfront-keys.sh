#!/bin/bash

# Script to generate CloudFront RSA key pair for signing URLs
# The keys are stored locally and NOT committed to git

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
KEYS_DIR="${SCRIPT_DIR}/.cloudfront-keys"

# Create keys directory if it doesn't exist
mkdir -p "$KEYS_DIR"

# Check if keys already exist
if [ -f "$KEYS_DIR/private_key.pem" ] && [ -f "$KEYS_DIR/public_key.pem" ]; then
    echo "✅ CloudFront keys already exist at: $KEYS_DIR"
    exit 0
fi

echo "🔑 Generating CloudFront RSA key pair..."

# Generate RSA private key (2048 bit)
openssl genrsa -out "$KEYS_DIR/private_key.pem" 2048

# Extract public key from private key
openssl rsa -pubout -in "$KEYS_DIR/private_key.pem" -out "$KEYS_DIR/public_key.pem"

# Generate base64 encoded version of private key (for SSM)
base64 -i "$KEYS_DIR/private_key.pem" -o "$KEYS_DIR/private_key_base64.txt"

echo "✅ Keys generated successfully!"
echo ""
echo "📁 Keys location: $KEYS_DIR"
echo "  - private_key.pem: RSA private key (upload to SSM parameter)"
echo "  - public_key.pem: RSA public key (used by Terraform)"
echo "  - private_key_base64.txt: Base64 encoded private key (for SSM console)"
echo ""