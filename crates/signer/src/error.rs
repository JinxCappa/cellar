//! Signer error types.

use thiserror::Error;

/// Signing operation errors.
#[derive(Debug, Error)]
pub enum SignerError {
    #[error("key generation error: {0}")]
    KeyGeneration(String),

    #[error("key parsing error: {0}")]
    KeyParsing(String),

    #[error("signing error: {0}")]
    Signing(String),

    #[error("verification failed")]
    VerificationFailed,

    #[error("invalid signature format: {0}")]
    InvalidSignature(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type for signing operations.
pub type SignerResult<T> = std::result::Result<T, SignerError>;
