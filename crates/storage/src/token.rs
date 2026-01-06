//! Continuation token envelope format for resumable listings.
//!
//! This module defines a versioned envelope format for continuation tokens that
//! wraps backend-specific tokens with metadata for validation and identity checking.

use crate::error::{StorageError, StorageResult};
use crate::traits::{ContinuationToken, ListingOptions, MAX_TOKEN_SIZE};
use serde::{Deserialize, Serialize};

/// Current token envelope format version.
pub const TOKEN_VERSION: u8 = 1;

/// Backend identity for token validation.
///
/// This allows us to detect when a token from one backend is incorrectly
/// used with a different backend (e.g., using an S3 token with a different
/// bucket or endpoint).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum BackendIdentity {
    /// S3-compatible backend with specific endpoint and region.
    S3 {
        /// S3 endpoint (e.g., "s3.amazonaws.com" or custom endpoint).
        endpoint: String,
        /// S3 region (e.g., "us-east-1").
        region: String,
        /// Bucket name.
        bucket: String,
        /// Optional prefix within the bucket.
        prefix: Option<String>,
    },
    /// Filesystem backend with root path.
    Filesystem {
        /// Canonical root path.
        root: String,
    },
}

/// Versioned continuation token envelope.
///
/// This wraps a backend-specific continuation token with metadata for validation.
/// The envelope ensures that tokens are only used with the same backend configuration
/// and listing parameters they were created with.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenEnvelope {
    /// Envelope format version (currently 1).
    pub version: u8,

    /// Backend identity for validation.
    pub backend: BackendIdentity,

    /// Listing prefix.
    pub prefix: String,

    /// Normalized listing options (page_size after clamping).
    pub page_size: usize,

    /// Backend-specific continuation token (opaque bytes).
    #[serde(with = "serde_bytes")]
    pub provider_token: Vec<u8>,
}

impl TokenEnvelope {
    /// Create a new token envelope.
    ///
    /// # Arguments
    ///
    /// * `backend` - Backend identity
    /// * `prefix` - Listing prefix
    /// * `options` - Normalized listing options
    /// * `provider_token` - Backend-specific token bytes
    ///
    /// # Errors
    ///
    /// Returns an error if the envelope would exceed MAX_TOKEN_SIZE when serialized.
    pub fn new(
        backend: BackendIdentity,
        prefix: String,
        options: &ListingOptions,
        provider_token: Vec<u8>,
    ) -> StorageResult<Self> {
        let envelope = Self {
            version: TOKEN_VERSION,
            backend,
            prefix,
            page_size: options.normalized_page_size(),
            provider_token,
        };

        // Validate size
        envelope.validate_size()?;

        Ok(envelope)
    }

    /// Serialize to ContinuationToken.
    ///
    /// Uses JSON encoding for the envelope structure.
    pub fn to_token(&self) -> StorageResult<ContinuationToken> {
        let json = serde_json::to_vec(self).map_err(|e| {
            StorageError::InvalidContinuationToken(format!(
                "failed to serialize token envelope: {}",
                e
            ))
        })?;

        ContinuationToken::new(json)
    }

    /// Deserialize from ContinuationToken.
    pub fn from_token(token: &ContinuationToken) -> StorageResult<Self> {
        let envelope: Self = serde_json::from_slice(token.as_bytes()).map_err(|e| {
            StorageError::InvalidContinuationToken(format!(
                "failed to deserialize token envelope: {}",
                e
            ))
        })?;

        // Validate version
        if envelope.version != TOKEN_VERSION {
            return Err(StorageError::InvalidContinuationToken(format!(
                "unsupported token version: {} (expected {})",
                envelope.version, TOKEN_VERSION
            )));
        }

        Ok(envelope)
    }

    /// Validate that this token matches the given backend and listing parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Backend identity doesn't match
    /// - Prefix doesn't match
    /// - Normalized page size doesn't match
    pub fn validate(
        &self,
        backend: &BackendIdentity,
        prefix: &str,
        options: &ListingOptions,
    ) -> StorageResult<()> {
        // Check backend identity
        if &self.backend != backend {
            return Err(StorageError::InvalidContinuationToken(format!(
                "backend mismatch: token is for {:?}, but current backend is {:?}",
                self.backend, backend
            )));
        }

        // Check prefix
        if self.prefix != prefix {
            return Err(StorageError::InvalidContinuationToken(format!(
                "prefix mismatch: token is for '{}', but current prefix is '{}'",
                self.prefix, prefix
            )));
        }

        // Check normalized page size
        let normalized_page_size = options.normalized_page_size();
        if self.page_size != normalized_page_size {
            return Err(StorageError::InvalidContinuationToken(format!(
                "page_size mismatch: token is for {}, but current page_size is {}",
                self.page_size, normalized_page_size
            )));
        }

        Ok(())
    }

    /// Validate that the serialized size is within limits.
    fn validate_size(&self) -> StorageResult<()> {
        let json = serde_json::to_vec(self).map_err(|e| {
            StorageError::InvalidContinuationToken(format!(
                "failed to serialize token envelope: {}",
                e
            ))
        })?;

        if json.len() > MAX_TOKEN_SIZE {
            return Err(StorageError::InvalidContinuationToken(format!(
                "token too large: {} bytes (max: {})",
                json.len(),
                MAX_TOKEN_SIZE
            )));
        }

        Ok(())
    }

    /// Get the backend-specific provider token.
    pub fn provider_token(&self) -> &[u8] {
        &self.provider_token
    }
}

// Helper module for serde_bytes
mod serde_bytes {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use base64::{Engine as _, engine::general_purpose};
        let encoded = general_purpose::STANDARD.encode(bytes);
        encoded.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        use base64::{Engine as _, engine::general_purpose};
        let s = String::deserialize(deserializer)?;
        general_purpose::STANDARD
            .decode(&s)
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_envelope_roundtrip() {
        let backend = BackendIdentity::S3 {
            endpoint: "s3.amazonaws.com".to_string(),
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            prefix: Some("chunks/".to_string()),
        };

        let options = ListingOptions::new(1000);
        let provider_token = b"s3-continuation-token-abc123".to_vec();

        let envelope = TokenEnvelope::new(
            backend.clone(),
            "chunks/".to_string(),
            &options,
            provider_token.clone(),
        )
        .unwrap();

        // Serialize to token
        let token = envelope.to_token().unwrap();

        // Deserialize back
        let decoded = TokenEnvelope::from_token(&token).unwrap();

        assert_eq!(decoded.version, TOKEN_VERSION);
        assert_eq!(decoded.backend, backend);
        assert_eq!(decoded.prefix, "chunks/");
        assert_eq!(decoded.page_size, 1000);
        assert_eq!(decoded.provider_token, provider_token);
    }

    #[test]
    fn test_token_validation_success() {
        let backend = BackendIdentity::S3 {
            endpoint: "s3.amazonaws.com".to_string(),
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            prefix: None,
        };

        let options = ListingOptions::new(1000);
        let provider_token = b"token".to_vec();

        let envelope = TokenEnvelope::new(
            backend.clone(),
            "chunks/".to_string(),
            &options,
            provider_token,
        )
        .unwrap();

        // Validation should succeed with matching parameters
        envelope.validate(&backend, "chunks/", &options).unwrap();
    }

    #[test]
    fn test_token_validation_backend_mismatch() {
        let backend1 = BackendIdentity::S3 {
            endpoint: "s3.amazonaws.com".to_string(),
            region: "us-east-1".to_string(),
            bucket: "bucket1".to_string(),
            prefix: None,
        };

        let backend2 = BackendIdentity::S3 {
            endpoint: "s3.amazonaws.com".to_string(),
            region: "us-east-1".to_string(),
            bucket: "bucket2".to_string(),
            prefix: None,
        };

        let options = ListingOptions::new(1000);
        let provider_token = b"token".to_vec();

        let envelope =
            TokenEnvelope::new(backend1, "chunks/".to_string(), &options, provider_token).unwrap();

        // Validation should fail with different backend
        let result = envelope.validate(&backend2, "chunks/", &options);
        assert!(result.is_err());
    }

    #[test]
    fn test_token_validation_prefix_mismatch() {
        let backend = BackendIdentity::S3 {
            endpoint: "s3.amazonaws.com".to_string(),
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            prefix: None,
        };

        let options = ListingOptions::new(1000);
        let provider_token = b"token".to_vec();

        let envelope = TokenEnvelope::new(
            backend.clone(),
            "chunks/".to_string(),
            &options,
            provider_token,
        )
        .unwrap();

        // Validation should fail with different prefix
        let result = envelope.validate(&backend, "manifests/", &options);
        assert!(result.is_err());
    }

    #[test]
    fn test_token_size_limit() {
        let backend = BackendIdentity::S3 {
            endpoint: "s3.amazonaws.com".to_string(),
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            prefix: None,
        };

        let options = ListingOptions::new(1000);
        // Create a provider token that's too large
        let provider_token = vec![0u8; MAX_TOKEN_SIZE];

        let result = TokenEnvelope::new(backend, "chunks/".to_string(), &options, provider_token);
        assert!(result.is_err());
    }
}
