//! Storage error types.

use thiserror::Error;

/// Storage operation errors.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object not found: {0}")]
    NotFound(String),

    #[error("object already exists: {0}")]
    AlreadyExists(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("S3 error: {0}")]
    S3(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("invalid path: {0}")]
    InvalidPath(String),

    #[error("invalid key: {0}")]
    InvalidKey(String),

    #[error("invalid range: {0}")]
    InvalidRange(String),

    #[error("hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("configuration error: {0}")]
    Config(String),

    #[error("invalid continuation token: {0}")]
    InvalidContinuationToken(String),

    #[error("listing not resumable: this backend does not support continuation tokens")]
    ListingNotResumable,
}

/// Result type for storage operations.
pub type StorageResult<T> = std::result::Result<T, StorageError>;
