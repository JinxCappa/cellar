//! Chunk types and hashing.

use crate::hash::ContentHash;
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// A chunk hash (SHA-256 of chunk contents).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkHash(ContentHash);

impl ChunkHash {
    /// Create from a ContentHash.
    pub fn from_content_hash(hash: ContentHash) -> Self {
        Self(hash)
    }

    /// Compute the hash of chunk data.
    pub fn compute(data: &[u8]) -> Self {
        Self(ContentHash::compute(data))
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

    /// Get the object store key for this chunk.
    pub fn to_object_key(&self) -> String {
        let hex = self.to_hex();
        format!("chunks/{}/{}/{}", &hex[..2], &hex[2..4], hex)
    }

    /// Get the object store key for this chunk within a storage domain.
    pub fn to_object_key_with_domain(&self, domain_id: Uuid) -> String {
        let hex = self.to_hex();
        format!(
            "domains/{}/chunks/{}/{}/{}",
            domain_id,
            &hex[..2],
            &hex[2..4],
            hex
        )
    }
}

impl fmt::Debug for ChunkHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChunkHash({})", &self.to_hex()[..16])
    }
}

impl fmt::Display for ChunkHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Metadata about a chunk.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkInfo {
    /// The chunk hash.
    pub hash: ChunkHash,
    /// Size in bytes.
    pub size: u64,
    /// Position in the manifest (0-indexed).
    pub position: u32,
}

impl ChunkInfo {
    /// Create new chunk info.
    pub fn new(hash: ChunkHash, size: u64, position: u32) -> Self {
        Self {
            hash,
            size,
            position,
        }
    }
}

/// A chunk with its data.
#[derive(Clone)]
pub struct Chunk {
    /// The chunk hash (computed from data).
    pub hash: ChunkHash,
    /// The chunk data.
    pub data: bytes::Bytes,
}

impl Chunk {
    /// Create a new chunk from data, computing the hash.
    pub fn new(data: bytes::Bytes) -> Self {
        let hash = ChunkHash::compute(&data);
        Self { hash, data }
    }

    /// Verify that the data matches the expected hash.
    pub fn verify(&self, expected: &ChunkHash) -> crate::Result<()> {
        if &self.hash != expected {
            return Err(crate::Error::HashMismatch {
                expected: expected.to_hex(),
                actual: self.hash.to_hex(),
            });
        }
        Ok(())
    }

    /// Get the chunk size.
    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }
}

impl fmt::Debug for Chunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Chunk")
            .field("hash", &self.hash)
            .field("size", &self.data.len())
            .finish()
    }
}

/// Split data into chunks of the given size.
pub fn chunk_data(data: &[u8], chunk_size: u64) -> Vec<ChunkInfo> {
    let chunk_size = chunk_size as usize;
    data.chunks(chunk_size)
        .enumerate()
        .map(|(i, chunk_data)| {
            let hash = ChunkHash::compute(chunk_data);
            ChunkInfo::new(hash, chunk_data.len() as u64, i as u32)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_hash_object_key() {
        let hash = ChunkHash::compute(b"test");
        let key = hash.to_object_key();
        assert!(key.starts_with("chunks/"));
        let parts: Vec<_> = key.split('/').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[1].len(), 2);
        assert_eq!(parts[2].len(), 2);
        assert_eq!(parts[3].len(), 64);
    }

    #[test]
    fn test_chunk_data_splitting() {
        let data = vec![0u8; 100];
        let chunks = chunk_data(&data, 30);
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].size, 30);
        assert_eq!(chunks[3].size, 10); // Last chunk is smaller
    }
}
