//! Test fixtures for generating test data.

use bytes::Bytes;
use cellar_core::chunk::ChunkHash;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};

/// Counter for generating unique store path hashes.
static STORE_PATH_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generate deterministic test data based on a seed.
pub fn seeded_bytes(seed: u64, len: usize) -> Bytes {
    let mut data = vec![0u8; len];
    let mut state = seed;

    for chunk in data.chunks_mut(8) {
        // Simple LCG for deterministic data
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let bytes = state.to_le_bytes();
        for (i, byte) in chunk.iter_mut().enumerate() {
            *byte = bytes[i % 8];
        }
    }

    Bytes::from(data)
}

/// Compute SHA-256 hash of data as hex string.
/// Note: #[allow(dead_code)] because each test file compiles common/ separately.
#[allow(dead_code)]
pub fn sha256_hash(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    // Format as hex without external dependency
    result.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Create a ChunkHash from data.
#[allow(dead_code)]
pub fn chunk_hash_from_data(data: &[u8]) -> ChunkHash {
    ChunkHash::compute(data)
}

/// Generate a valid Nix store path hash (32 chars, nix-base32).
/// Uses a counter for uniqueness across test runs.
#[allow(dead_code)]
pub fn random_store_path_hash() -> String {
    let counter = STORE_PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let data = seeded_bytes(counter, 20);
    nix_base32::to_nix_base32(&data)
}

/// Generate a test store path.
#[allow(dead_code)]
pub fn test_store_path(name: &str) -> String {
    let hash = random_store_path_hash();
    format!("/nix/store/{}-{}", hash, name)
}

/// Generate test NAR data (simplified, not valid NAR format but useful for testing).
#[allow(dead_code)]
pub fn test_nar_data(size: usize) -> Bytes {
    // NAR magic header
    let mut data = Vec::with_capacity(size);
    data.extend_from_slice(b"\x0d\x00\x00\x00\x00\x00\x00\x00nix-archive-1");

    // Pad with deterministic data
    while data.len() < size {
        let remaining = size - data.len();
        let chunk = seeded_bytes(data.len() as u64, remaining.min(1024));
        data.extend_from_slice(&chunk[..remaining.min(1024)]);
    }

    Bytes::from(data)
}

/// Split data into chunks of specified size.
pub fn split_into_chunks(data: &[u8], chunk_size: usize) -> Vec<Bytes> {
    data.chunks(chunk_size)
        .map(Bytes::copy_from_slice)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seeded_bytes_deterministic() {
        let a = seeded_bytes(42, 100);
        let b = seeded_bytes(42, 100);
        assert_eq!(a, b);

        let c = seeded_bytes(43, 100);
        assert_ne!(a, c);
    }

    #[test]
    fn test_split_into_chunks() {
        let data = seeded_bytes(1, 100);
        let chunks = split_into_chunks(&data, 30);
        assert_eq!(chunks.len(), 4); // 30 + 30 + 30 + 10

        let reassembled: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
        assert_eq!(reassembled, data.as_ref());
    }
}
