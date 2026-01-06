//! Metadata store error types.

use thiserror::Error;

/// Metadata store operation errors.
#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("invalid state transition: {from} -> {to}")]
    InvalidStateTransition { from: String, to: String },

    #[error("upload expired: {0}")]
    UploadExpired(String),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("constraint violation: {0}")]
    Constraint(String),

    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type for metadata operations.
pub type MetadataResult<T> = std::result::Result<T, MetadataError>;
