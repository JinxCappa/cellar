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

    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

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
}

/// Result type for storage operations.
pub type StorageResult<T> = std::result::Result<T, StorageError>;
