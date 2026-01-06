use bytes::Bytes;
use sha2::{Digest, Sha256};

/// Compute SHA-256 hash of data as hex string
pub fn sha256_hash(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    result.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Generate deterministic test data using a seeded pseudo-random generator
/// Same seed produces same output (reproducible tests)
pub fn seeded_bytes(seed: u64, len: usize) -> Bytes {
    let mut data = vec![0u8; len];
    let mut state = seed;

    // Simple LCG (Linear Congruential Generator)
    for chunk in data.chunks_mut(8) {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let bytes = state.to_le_bytes();
        for (i, byte) in chunk.iter_mut().enumerate() {
            *byte = bytes[i % 8];
        }
    }

    Bytes::from(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_hash() {
        let data = b"hello world";
        let hash = sha256_hash(data);
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_seeded_bytes_deterministic() {
        let data1 = seeded_bytes(42, 1000);
        let data2 = seeded_bytes(42, 1000);
        assert_eq!(data1, data2);
    }

    #[test]
    fn test_seeded_bytes_different_seeds() {
        let data1 = seeded_bytes(42, 1000);
        let data2 = seeded_bytes(43, 1000);
        assert_ne!(data1, data2);
    }
}
