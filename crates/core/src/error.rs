//! Error types for the core domain.

use thiserror::Error;

/// Core domain error type.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid store path: {0}")]
    InvalidStorePath(String),

    #[error("invalid hash: {0}")]
    InvalidHash(String),

    #[error("hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("invalid chunk size: {size} (must be between {min} and {max})")]
    InvalidChunkSize { size: u64, min: u64, max: u64 },

    #[error("manifest integrity error: {0}")]
    ManifestIntegrity(String),

    #[error("upload session error: {0}")]
    UploadSession(String),

    #[error("invalid token: {0}")]
    InvalidToken(String),

    #[error("narinfo parse error: {0}")]
    NarInfoParse(String),

    #[error("serialization error: {0}")]
    Serialization(String),
}

/// Result type alias for core operations.
pub type Result<T> = std::result::Result<T, Error>;
