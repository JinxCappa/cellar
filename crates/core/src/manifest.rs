//! Manifest types and hashing.

use crate::chunk::ChunkHash;
use crate::hash::ContentHash;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use uuid::Uuid;

/// A manifest hash (SHA-256 of ordered chunk hashes).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ManifestHash(ContentHash);

impl ManifestHash {
    /// Compute the manifest hash from ordered chunk hashes.
    pub fn compute(chunk_hashes: &[ChunkHash]) -> Self {
        let mut hasher = Sha256::new();
        for hash in chunk_hashes {
            hasher.update(hash.content_hash().as_bytes());
        }
        Self(ContentHash::from_bytes(hasher.finalize().into()))
    }

    /// Get the underlying content hash.
    pub fn content_hash(&self) -> &ContentHash {
        &self.0
    }

    /// Parse from hex string.
    pub fn from_hex(s: &str) -> crate::Result<Self> {
        Ok(Self(ContentHash::from_hex(s)?))
    }

    /// Encode as hex string.
    pub fn to_hex(&self) -> String {
        self.0.to_hex()
    }

    /// Get the object store key for this manifest.
    pub fn to_object_key(&self) -> String {
        let hex = self.to_hex();
        format!("manifests/{}/{}/{}.json", &hex[..2], &hex[2..4], hex)
    }

    /// Get the object store key for this manifest within a storage domain.
    pub fn to_object_key_with_domain(&self, domain_id: Uuid) -> String {
        let hex = self.to_hex();
        format!(
            "domains/{}/manifests/{}/{}/{}.json",
            domain_id,
            &hex[..2],
            &hex[2..4],
            hex
        )
    }
}

impl fmt::Debug for ManifestHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ManifestHash({})", &self.to_hex()[..16])
    }
}

impl fmt::Display for ManifestHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// A manifest describing the ordered chunks that make up a NAR.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    /// The manifest hash (derived from chunk hashes).
    pub hash: ManifestHash,
    /// Ordered list of chunk hashes.
    pub chunks: Vec<ChunkHash>,
    /// Size of each chunk (except possibly the last).
    pub chunk_size: u64,
    /// Total NAR size.
    pub nar_size: u64,
}

impl Manifest {
    /// Create a new manifest from chunk hashes.
    pub fn new(chunks: Vec<ChunkHash>, chunk_size: u64, nar_size: u64) -> Self {
        let hash = ManifestHash::compute(&chunks);
        Self {
            hash,
            chunks,
            chunk_size,
            nar_size,
        }
    }

    /// Verify the manifest hash matches the expected value.
    pub fn verify_hash(&self, expected: &ManifestHash) -> crate::Result<()> {
        let computed = ManifestHash::compute(&self.chunks);
        if &computed != expected {
            return Err(crate::Error::HashMismatch {
                expected: expected.to_hex(),
                actual: computed.to_hex(),
            });
        }
        Ok(())
    }

    /// Get the number of chunks.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Calculate expected size of a chunk at a given position.
    ///
    /// Returns `None` if the position is out of bounds.
    pub fn expected_chunk_size(&self, position: usize) -> Option<u64> {
        if position >= self.chunks.len() {
            return None;
        }

        if position + 1 < self.chunks.len() {
            // Not the last chunk
            Some(self.chunk_size)
        } else {
            // Last chunk may be smaller
            let full_chunks = self.chunks.len().saturating_sub(1) as u64;
            Some(self.nar_size.saturating_sub(full_chunks * self.chunk_size))
        }
    }

    /// Serialize to JSON.
    pub fn to_json(&self) -> crate::Result<String> {
        serde_json::to_string(self).map_err(|e| crate::Error::Serialization(e.to_string()))
    }

    /// Deserialize from JSON.
    pub fn from_json(json: &str) -> crate::Result<Self> {
        serde_json::from_str(json).map_err(|e| crate::Error::Serialization(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_hash_deterministic() {
        let chunks = vec![ChunkHash::compute(b"chunk1"), ChunkHash::compute(b"chunk2")];
        let hash1 = ManifestHash::compute(&chunks);
        let hash2 = ManifestHash::compute(&chunks);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_manifest_expected_chunk_size() {
        let chunks = vec![
            ChunkHash::compute(b"a"),
            ChunkHash::compute(b"b"),
            ChunkHash::compute(b"c"),
        ];
        let manifest = Manifest::new(chunks, 100, 250);

        assert_eq!(manifest.expected_chunk_size(0), Some(100));
        assert_eq!(manifest.expected_chunk_size(1), Some(100));
        assert_eq!(manifest.expected_chunk_size(2), Some(50)); // Last chunk
        assert_eq!(manifest.expected_chunk_size(3), None); // Out of bounds
        assert_eq!(manifest.expected_chunk_size(100), None); // Way out of bounds
    }
}
