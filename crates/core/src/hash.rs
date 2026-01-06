//! Cryptographic hash types and utilities.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

/// A SHA-256 content hash represented as 32 bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContentHash([u8; 32]);

impl ContentHash {
    /// Create a new ContentHash from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Get the raw bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Compute SHA-256 hash of data.
    pub fn compute(data: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        Self(result.into())
    }

    /// Create an incremental hasher.
    pub fn hasher() -> ContentHasher {
        ContentHasher(Sha256::new())
    }

    /// Parse from base64 string.
    pub fn from_base64(s: &str) -> crate::Result<Self> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(|e| crate::Error::InvalidHash(e.to_string()))?;
        if bytes.len() != 32 {
            return Err(crate::Error::InvalidHash(format!(
                "expected 32 bytes, got {}",
                bytes.len()
            )));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }

    /// Encode as base64 string.
    pub fn to_base64(&self) -> String {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(self.0)
    }

    /// Parse from hex string.
    pub fn from_hex(s: &str) -> crate::Result<Self> {
        if s.len() != 64 {
            return Err(crate::Error::InvalidHash(format!(
                "expected 64 hex chars, got {}",
                s.len()
            )));
        }
        let mut bytes = [0u8; 32];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            let hex_str = std::str::from_utf8(chunk)
                .map_err(|e| crate::Error::InvalidHash(e.to_string()))?;
            bytes[i] = u8::from_str_radix(hex_str, 16)
                .map_err(|e| crate::Error::InvalidHash(e.to_string()))?;
        }
        Ok(Self(bytes))
    }

    /// Encode as lowercase hex string.
    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }
}

impl fmt::Debug for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContentHash({})", &self.to_hex()[..16])
    }
}

impl fmt::Display for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Incremental SHA-256 hasher.
pub struct ContentHasher(Sha256);

impl ContentHasher {
    /// Update the hasher with data.
    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    /// Finalize and return the hash.
    pub fn finalize(self) -> ContentHash {
        ContentHash(self.0.finalize().into())
    }
}

/// A NAR hash in Nix's SRI format (sha256-<base64>).
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NarHash(ContentHash);

impl NarHash {
    /// Create from a ContentHash.
    pub fn from_content_hash(hash: ContentHash) -> Self {
        Self(hash)
    }

    /// Get the underlying content hash.
    pub fn content_hash(&self) -> &ContentHash {
        &self.0
    }

    /// Parse from SRI format (sha256-<base64>).
    pub fn from_sri(s: &str) -> crate::Result<Self> {
        let prefix = "sha256-";
        if !s.starts_with(prefix) {
            return Err(crate::Error::InvalidHash(format!(
                "expected sha256- prefix, got: {s}"
            )));
        }
        let b64 = &s[prefix.len()..];
        Ok(Self(ContentHash::from_base64(b64)?))
    }

    /// Encode as SRI format.
    pub fn to_sri(&self) -> String {
        format!("sha256-{}", self.0.to_base64())
    }

    /// Parse from Nix base32 format.
    pub fn from_nix_base32(s: &str) -> crate::Result<Self> {
        let bytes = decode_nix_base32(s)?;
        if bytes.len() != 32 {
            return Err(crate::Error::InvalidHash(format!(
                "expected 32 bytes, got {}",
                bytes.len()
            )));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Self(ContentHash(arr)))
    }

    /// Encode as Nix base32 format.
    pub fn to_nix_base32(&self) -> String {
        nix_base32::to_nix_base32(&self.0 .0)
    }
}

impl fmt::Debug for NarHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NarHash({})", self.to_sri())
    }
}

impl fmt::Display for NarHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_sri())
    }
}

/// Incremental hasher for NAR content.
///
/// Wraps ContentHasher to compute a NAR hash incrementally.
pub struct NarHasher(ContentHasher);

impl NarHasher {
    /// Create a new NAR hasher.
    pub fn new() -> Self {
        Self(ContentHash::hasher())
    }

    /// Update the hasher with data.
    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    /// Finalize and return the NAR hash.
    pub fn finalize(self) -> NarHash {
        NarHash::from_content_hash(self.0.finalize())
    }
}

impl Default for NarHasher {
    fn default() -> Self {
        Self::new()
    }
}

/// Decode a Nix base32 string to bytes.
/// Nix base32 uses the alphabet: 0123456789abcdfghijklmnpqrsvwxyz
fn decode_nix_base32(s: &str) -> crate::Result<Vec<u8>> {
    const BASE32_CHARS: &[u8] = b"0123456789abcdfghijklmnpqrsvwxyz";

    fn char_to_value(c: char) -> Option<u8> {
        BASE32_CHARS.iter().position(|&x| x == c as u8).map(|p| p as u8)
    }

    let len = s.len();
    if len == 0 {
        return Ok(Vec::new());
    }

    // Calculate output length: 5 bits per char, 8 bits per byte
    let out_len = len * 5 / 8;
    let mut out = vec![0u8; out_len];

    for (i, c) in s.chars().rev().enumerate() {
        let value = char_to_value(c).ok_or_else(|| {
            crate::Error::InvalidHash(format!("invalid nix base32 character: {c}"))
        })?;

        let b = i * 5;
        let byte_idx = b / 8;
        let bit_offset = b % 8;

        if byte_idx < out_len {
            out[byte_idx] |= value << bit_offset;
        }
        if byte_idx + 1 < out_len && bit_offset > 3 {
            out[byte_idx + 1] |= value >> (8 - bit_offset);
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_hash_roundtrip() {
        let data = b"hello world";
        let hash = ContentHash::compute(data);

        let hex = hash.to_hex();
        let parsed = ContentHash::from_hex(&hex).unwrap();
        assert_eq!(hash, parsed);

        let b64 = hash.to_base64();
        let parsed = ContentHash::from_base64(&b64).unwrap();
        assert_eq!(hash, parsed);
    }

    #[test]
    fn test_nar_hash_sri() {
        let hash = ContentHash::compute(b"test");
        let nar_hash = NarHash::from_content_hash(hash);
        let sri = nar_hash.to_sri();
        assert!(sri.starts_with("sha256-"));
        let parsed = NarHash::from_sri(&sri).unwrap();
        assert_eq!(nar_hash, parsed);
    }
}
