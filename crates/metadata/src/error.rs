//! Metadata store error types.

use thiserror::Error;

/// Format missing chunks for display, capping at MAX_DISPLAYED to prevent log/response bloat.
fn format_missing_chunks(chunks: &[String]) -> String {
    const MAX_DISPLAYED: usize = 5;
    if chunks.len() <= MAX_DISPLAYED {
        format!("{:?}", chunks)
    } else {
        let sample: Vec<_> = chunks.iter().take(MAX_DISPLAYED).collect();
        format!("{:?} (and {} more)", sample, chunks.len() - MAX_DISPLAYED)
    }
}

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

    #[error("missing {} chunks for manifest {manifest_hash}: {}", .missing_chunks.len(), format_missing_chunks(.missing_chunks))]
    MissingChunks {
        manifest_hash: String,
        missing_chunks: Vec<String>,
    },
}

/// Result type for metadata operations.
pub type MetadataResult<T> = std::result::Result<T, MetadataError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_missing_chunks_format_small() {
        let err = MetadataError::MissingChunks {
            manifest_hash: "abc".to_string(),
            missing_chunks: vec!["a".to_string(), "b".to_string()],
        };
        let msg = err.to_string();
        assert!(msg.contains("missing 2 chunks for manifest abc"));
        assert!(msg.contains("[\"a\", \"b\"]"));
    }

    #[test]
    fn test_missing_chunks_format_large() {
        let err = MetadataError::MissingChunks {
            manifest_hash: "xyz".to_string(),
            missing_chunks: vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
                "e".to_string(),
                "f".to_string(),
            ],
        };
        let msg = err.to_string();
        assert!(msg.contains("missing 6 chunks for manifest xyz"));
        assert!(msg.contains("and 1 more"));
    }
}
