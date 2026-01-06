// Token validation tests for continuation token envelope
// Tests all mismatch scenarios to ensure tokens are properly validated

use cellar_storage::error::StorageError;
use cellar_storage::token::{BackendIdentity, TokenEnvelope};
use cellar_storage::traits::{ContinuationToken, ListingOptions, MAX_TOKEN_SIZE};

#[test]
fn test_token_page_size_mismatch() {
    let backend = BackendIdentity::S3 {
        endpoint: "s3.amazonaws.com".to_string(),
        region: "us-east-1".to_string(),
        bucket: "test-bucket".to_string(),
        prefix: None,
    };

    // Create token with page_size=1000
    let options_original = ListingOptions::new(1000);
    let envelope = TokenEnvelope::new(
        backend.clone(),
        "chunks/".to_string(),
        &options_original,
        b"s3-token".to_vec(),
    )
    .unwrap();

    let token = envelope.to_token().unwrap();
    let decoded = TokenEnvelope::from_token(&token).unwrap();

    // Attempt to validate with different page_size=500
    let options_different = ListingOptions::new(500);
    let result = decoded.validate(&backend, "chunks/", &options_different);

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("page_size mismatch"),
        "Expected page_size mismatch error, got: {}",
        err_msg
    );
    assert!(err_msg.contains("1000"));
    assert!(err_msg.contains("500"));
}

#[test]
fn test_token_backend_mismatch() {
    let backend1 = BackendIdentity::S3 {
        endpoint: "s3.amazonaws.com".to_string(),
        region: "us-east-1".to_string(),
        bucket: "bucket-A".to_string(),
        prefix: None,
    };

    let backend2 = BackendIdentity::S3 {
        endpoint: "s3.us-west-2.amazonaws.com".to_string(),
        region: "us-west-2".to_string(),
        bucket: "bucket-A".to_string(),
        prefix: None,
    };

    let options = ListingOptions::new(1000);
    let envelope = TokenEnvelope::new(
        backend1.clone(),
        "chunks/".to_string(),
        &options,
        b"token-data".to_vec(),
    )
    .unwrap();

    let token = envelope.to_token().unwrap();
    let decoded = TokenEnvelope::from_token(&token).unwrap();

    let result = decoded.validate(&backend2, "chunks/", &options);

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("backend mismatch"),
        "Expected backend mismatch error, got: {}",
        err_msg
    );
}

#[test]
fn test_token_oversized() {
    let backend = BackendIdentity::Filesystem {
        root: "/tmp/storage".to_string(),
    };

    let options = ListingOptions::new(1000);

    // Create provider token that will exceed MAX_TOKEN_SIZE when wrapped
    // Account for JSON overhead: version, backend, prefix, page_size fields
    // A 2KB provider token should definitely exceed the limit
    let large_token = vec![0u8; MAX_TOKEN_SIZE];

    let result = TokenEnvelope::new(backend, "prefix/".to_string(), &options, large_token);

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("token too large"),
        "Expected token too large error, got: {}",
        err_msg
    );
    assert!(err_msg.contains(&MAX_TOKEN_SIZE.to_string()));
}

#[test]
fn test_token_malformed_base64() {
    // Create invalid base64 token
    let result = ContinuationToken::from_base64("not-valid-base64!@#$");

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("invalid continuation token base64"),
        "Expected base64 error, got: {}",
        err_msg
    );
}

#[test]
fn test_token_corrupt_json() {
    // Create token with corrupt JSON payload
    let corrupt_data = b"{ invalid json }".to_vec();
    let token = ContinuationToken::new(corrupt_data).unwrap();

    let result = TokenEnvelope::from_token(&token);

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("failed to deserialize token envelope"),
        "Expected deserialization error, got: {}",
        err_msg
    );
}

#[test]
fn test_token_version_mismatch() {
    use serde_json::json;

    // Manually construct token with future version
    let future_envelope = json!({
        "version": 99,
        "backend": {
            "S3": {
                "endpoint": "s3.amazonaws.com",
                "region": "us-east-1",
                "bucket": "test",
                "prefix": null
            }
        },
        "prefix": "chunks/",
        "page_size": 1000,
        "provider_token": "" // base64 empty string
    });

    let json_bytes = serde_json::to_vec(&future_envelope).unwrap();
    let token = ContinuationToken::new(json_bytes).unwrap();

    let result = TokenEnvelope::from_token(&token);

    assert!(result.is_err());
    match result.unwrap_err() {
        StorageError::InvalidContinuationToken(msg) => {
            assert!(
                msg.contains("unsupported token version"),
                "Expected version error, got: {}",
                msg
            );
            assert!(msg.contains("99"));
        }
        other => panic!("Expected InvalidContinuationToken, got: {:?}", other),
    }
}

#[test]
fn test_token_prefix_mismatch() {
    let backend = BackendIdentity::Filesystem {
        root: "/tmp/storage".to_string(),
    };

    let options = ListingOptions::new(1000);
    let envelope = TokenEnvelope::new(
        backend.clone(),
        "chunks/".to_string(),
        &options,
        b"token".to_vec(),
    )
    .unwrap();

    let token = envelope.to_token().unwrap();
    let decoded = TokenEnvelope::from_token(&token).unwrap();

    // Try to use with different prefix
    let result = decoded.validate(&backend, "manifests/", &options);

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("prefix mismatch"),
        "Expected prefix mismatch error, got: {}",
        err_msg
    );
    assert!(err_msg.contains("chunks/"));
    assert!(err_msg.contains("manifests/"));
}

#[test]
fn test_token_valid_roundtrip() {
    // Positive test: valid token should work
    let backend = BackendIdentity::S3 {
        endpoint: "s3.amazonaws.com".to_string(),
        region: "us-east-1".to_string(),
        bucket: "test-bucket".to_string(),
        prefix: Some("data/".to_string()),
    };

    let options = ListingOptions::new(1500); // Will be normalized to 1500
    let envelope = TokenEnvelope::new(
        backend.clone(),
        "chunks/".to_string(),
        &options,
        b"provider-token-data".to_vec(),
    )
    .unwrap();

    // Serialize
    let token = envelope.to_token().unwrap();

    // Deserialize
    let decoded = TokenEnvelope::from_token(&token).unwrap();

    // Validate with same parameters
    let result = decoded.validate(&backend, "chunks/", &options);
    assert!(result.is_ok(), "Valid token should pass validation");

    // Check provider token is preserved
    assert_eq!(decoded.provider_token(), b"provider-token-data");
}

#[test]
fn test_token_base64_roundtrip() {
    // Test base64 encoding/decoding
    let backend = BackendIdentity::Filesystem {
        root: "/tmp/test".to_string(),
    };

    let options = ListingOptions::new(1000);
    let envelope = TokenEnvelope::new(
        backend.clone(),
        "test/".to_string(),
        &options,
        b"test-data".to_vec(),
    )
    .unwrap();

    let token = envelope.to_token().unwrap();

    // Convert to base64
    let base64_str = token.to_base64();

    // Parse from base64
    let parsed = ContinuationToken::from_base64(&base64_str).unwrap();

    // Deserialize
    let decoded = TokenEnvelope::from_token(&parsed).unwrap();

    // Validate
    let result = decoded.validate(&backend, "test/", &options);
    assert!(result.is_ok());
}
