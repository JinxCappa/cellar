//! Tests for bootstrap token initialization behavior.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cellar_core::config::AdminConfig;
use cellar_core::storage_domain::default_storage_domain_id;
use cellar_metadata::models::TokenRow;
use cellar_server::bootstrap::ensure_admin_token;
use common::TestServer;
use common::fixtures::sha256_hash;
use serde_json::Value;
use time::{Duration, OffsetDateTime};
use tower::ServiceExt;
use uuid::Uuid;

/// Helper to make JSON requests.
async fn json_request(
    router: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    auth_token: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);

    if let Some(token) = auth_token {
        builder = builder.header("Authorization", format!("Bearer {}", token));
    }

    let body = match body {
        Some(v) => {
            builder = builder.header("Content-Type", "application/json");
            Body::from(serde_json::to_vec(&v).unwrap())
        }
        None => Body::empty(),
    };

    let request = builder.body(body).unwrap();
    let response = router.clone().oneshot(request).await.unwrap();

    let status = response.status();
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    let json: Value = if body_bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body_bytes).unwrap_or(Value::Null)
    };

    (status, json)
}

/// Create a test token and return the raw token value.
async fn create_test_token(server: &TestServer, scopes: &str) -> String {
    let token_id = Uuid::new_v4();
    let raw_token = format!("test-token-{}", Uuid::new_v4());
    let token_hash = sha256_hash(raw_token.as_bytes());

    let token = TokenRow {
        token_id,
        cache_id: None,
        token_hash,
        scopes: scopes.to_string(),
        expires_at: None,
        revoked_at: None,
        created_at: OffsetDateTime::now_utc(),
        last_used_at: None,
        description: Some("Test Token".to_string()),
    };

    server
        .metadata()
        .create_token(&token)
        .await
        .expect("Failed to create token");

    raw_token
}

// =============================================================================
// Bootstrap token tests
// =============================================================================

#[tokio::test]
async fn test_bootstrap_creates_token_when_none_exists() {
    let server = TestServer::new().await;
    let metadata = server.metadata();

    // No bootstrap token should exist initially
    let initial = metadata.get_bootstrap_token_id().await.unwrap();
    assert!(
        initial.is_none(),
        "No bootstrap token should exist initially"
    );

    // Create a bootstrap config with a token hash
    let raw_token = "my-secret-bootstrap-token";
    let token_hash = sha256_hash(raw_token.as_bytes());
    let config = AdminConfig {
        token_hash: format!("sha256:{}", token_hash),
        token_scopes: None,
        token_description: Some("Bootstrap test token".to_string()),
    };

    // Run bootstrap
    ensure_admin_token(metadata.as_ref(), &config)
        .await
        .expect("Bootstrap should succeed");

    // Verify token was created
    let bootstrap_id = metadata
        .get_bootstrap_token_id()
        .await
        .unwrap()
        .expect("Bootstrap token should exist");

    let token = metadata
        .get_token(bootstrap_id)
        .await
        .unwrap()
        .expect("Token should exist");

    assert_eq!(token.token_hash, token_hash);
    assert!(token.revoked_at.is_none());
    assert!(token.expires_at.is_none());
}

#[tokio::test]
async fn test_bootstrap_reuses_existing_valid_token() {
    let server = TestServer::new().await;
    let metadata = server.metadata();

    // Create an existing token
    let raw_token = "existing-bootstrap-token";
    let token_hash = sha256_hash(raw_token.as_bytes());
    let existing_token_id = Uuid::new_v4();

    let existing = TokenRow {
        token_id: existing_token_id,
        cache_id: None,
        token_hash: token_hash.clone(),
        scopes: r#"["cache:admin"]"#.to_string(),
        expires_at: None,
        revoked_at: None,
        created_at: OffsetDateTime::now_utc(),
        last_used_at: None,
        description: Some("Pre-existing token".to_string()),
    };
    metadata.create_token(&existing).await.unwrap();

    // Bootstrap with the same hash
    let config = AdminConfig {
        token_hash: format!("sha256:{}", token_hash),
        token_scopes: None,
        token_description: None,
    };

    ensure_admin_token(metadata.as_ref(), &config)
        .await
        .expect("Bootstrap should succeed");

    // Should reuse the existing token
    let bootstrap_id = metadata
        .get_bootstrap_token_id()
        .await
        .unwrap()
        .expect("Bootstrap token should exist");

    assert_eq!(
        bootstrap_id, existing_token_id,
        "Should reuse existing token"
    );
}

#[tokio::test]
async fn test_bootstrap_rejects_revoked_token() {
    let server = TestServer::new().await;
    let metadata = server.metadata();

    // Create a revoked token
    let raw_token = "revoked-bootstrap-token";
    let token_hash = sha256_hash(raw_token.as_bytes());

    let revoked = TokenRow {
        token_id: Uuid::new_v4(),
        cache_id: None,
        token_hash: token_hash.clone(),
        scopes: r#"["cache:admin"]"#.to_string(),
        expires_at: None,
        revoked_at: Some(OffsetDateTime::now_utc()), // Revoked!
        created_at: OffsetDateTime::now_utc(),
        last_used_at: None,
        description: Some("Revoked token".to_string()),
    };
    metadata.create_token(&revoked).await.unwrap();

    // Bootstrap with the revoked token's hash should fail
    let config = AdminConfig {
        token_hash: format!("sha256:{}", token_hash),
        token_scopes: None,
        token_description: None,
    };

    let result = ensure_admin_token(metadata.as_ref(), &config).await;

    assert!(result.is_err(), "Bootstrap should fail for revoked token");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("revoked"),
        "Error should mention revoked: {}",
        err
    );
}

#[tokio::test]
async fn test_bootstrap_rejects_expired_token() {
    let server = TestServer::new().await;
    let metadata = server.metadata();

    // Create an expired token
    let raw_token = "expired-bootstrap-token";
    let token_hash = sha256_hash(raw_token.as_bytes());
    let past = OffsetDateTime::now_utc() - Duration::hours(1);

    let expired = TokenRow {
        token_id: Uuid::new_v4(),
        cache_id: None,
        token_hash: token_hash.clone(),
        scopes: r#"["cache:admin"]"#.to_string(),
        expires_at: Some(past), // Expired!
        revoked_at: None,
        created_at: OffsetDateTime::now_utc() - Duration::hours(2),
        last_used_at: None,
        description: Some("Expired token".to_string()),
    };
    metadata.create_token(&expired).await.unwrap();

    // Bootstrap with the expired token's hash should fail
    let config = AdminConfig {
        token_hash: format!("sha256:{}", token_hash),
        token_scopes: None,
        token_description: None,
    };

    let result = ensure_admin_token(metadata.as_ref(), &config).await;

    assert!(result.is_err(), "Bootstrap should fail for expired token");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("expired"),
        "Error should mention expired: {}",
        err
    );
}

#[tokio::test]
async fn test_bootstrap_revokes_old_token_when_hash_changes() {
    let server = TestServer::new().await;
    let metadata = server.metadata();

    // Create initial bootstrap token
    let old_raw = "old-bootstrap-token";
    let old_hash = sha256_hash(old_raw.as_bytes());
    let old_config = AdminConfig {
        token_hash: format!("sha256:{}", old_hash),
        token_scopes: None,
        token_description: Some("Old token".to_string()),
    };

    ensure_admin_token(metadata.as_ref(), &old_config)
        .await
        .expect("First bootstrap should succeed");

    let old_token_id = metadata
        .get_bootstrap_token_id()
        .await
        .unwrap()
        .expect("Old token should exist");

    // Bootstrap with a new hash
    let new_raw = "new-bootstrap-token";
    let new_hash = sha256_hash(new_raw.as_bytes());
    let new_config = AdminConfig {
        token_hash: format!("sha256:{}", new_hash),
        token_scopes: None,
        token_description: Some("New token".to_string()),
    };

    ensure_admin_token(metadata.as_ref(), &new_config)
        .await
        .expect("Second bootstrap should succeed");

    // Old token should be revoked
    let old_token = metadata
        .get_token(old_token_id)
        .await
        .unwrap()
        .expect("Old token should still exist");
    assert!(
        old_token.revoked_at.is_some(),
        "Old token should be revoked"
    );

    // New token should be active
    let new_token_id = metadata
        .get_bootstrap_token_id()
        .await
        .unwrap()
        .expect("New token should exist");
    assert_ne!(new_token_id, old_token_id, "Should be a different token");

    let new_token = metadata
        .get_token(new_token_id)
        .await
        .unwrap()
        .expect("New token should exist");
    assert!(
        new_token.revoked_at.is_none(),
        "New token should not be revoked"
    );
}

#[tokio::test]
async fn test_bootstrap_with_custom_scopes() {
    let server = TestServer::new().await;
    let metadata = server.metadata();

    let raw_token = "scoped-bootstrap-token";
    let token_hash = sha256_hash(raw_token.as_bytes());
    let config = AdminConfig {
        token_hash: format!("sha256:{}", token_hash),
        token_scopes: Some(vec!["cache:admin".to_string(), "cache:write".to_string()]),
        token_description: Some("Custom scopes token".to_string()),
    };

    ensure_admin_token(metadata.as_ref(), &config)
        .await
        .expect("Bootstrap should succeed");

    let token_id = metadata
        .get_bootstrap_token_id()
        .await
        .unwrap()
        .expect("Token should exist");

    let token = metadata.get_token(token_id).await.unwrap().unwrap();

    let scopes: Vec<String> = serde_json::from_str(&token.scopes).unwrap();
    assert!(scopes.contains(&"cache:admin".to_string()));
    assert!(scopes.contains(&"cache:write".to_string()));
}

#[tokio::test]
async fn test_bootstrap_rejects_invalid_hash_format() {
    let server = TestServer::new().await;
    let metadata = server.metadata();

    // Too short
    let config = AdminConfig {
        token_hash: "sha256:abc123".to_string(),
        token_scopes: None,
        token_description: None,
    };

    let result = ensure_admin_token(metadata.as_ref(), &config).await;
    assert!(result.is_err(), "Should reject too-short hash");

    // Invalid characters
    let config = AdminConfig {
        token_hash: "sha256:gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg"
            .to_string(),
        token_scopes: None,
        token_description: None,
    };

    let result = ensure_admin_token(metadata.as_ref(), &config).await;
    assert!(result.is_err(), "Should reject invalid hex chars");
}

// =============================================================================
// Token listing endpoint tests
// =============================================================================

#[tokio::test]
async fn test_list_tokens_returns_expected_fields() {
    let server = TestServer::new().await;

    // Create an admin token
    let admin_token = create_test_token(&server, r#"["cache:admin"]"#).await;

    // Create a few tokens with various properties
    let now = OffsetDateTime::now_utc();

    let token1 = TokenRow {
        token_id: Uuid::new_v4(),
        cache_id: None,
        token_hash: sha256_hash(b"token1"),
        scopes: r#"["cache:read"]"#.to_string(),
        expires_at: Some(now + Duration::days(30)),
        revoked_at: None,
        created_at: now,
        last_used_at: Some(now),
        description: Some("Token with expiry".to_string()),
    };
    server.metadata().create_token(&token1).await.unwrap();

    let token2 = TokenRow {
        token_id: Uuid::new_v4(),
        cache_id: None,
        token_hash: sha256_hash(b"token2"),
        scopes: r#"["cache:write"]"#.to_string(),
        expires_at: None,
        revoked_at: Some(now),
        created_at: now - Duration::days(7),
        last_used_at: None,
        description: None,
    };
    server.metadata().create_token(&token2).await.unwrap();

    // List tokens
    let (status, response) = json_request(
        &server.router,
        "GET",
        "/v1/admin/tokens",
        None,
        Some(&admin_token),
    )
    .await;

    assert_eq!(status, StatusCode::OK);

    let tokens = response.as_array().expect("Response should be an array");
    assert!(tokens.len() >= 2, "Should have at least 2 tokens");

    // Find our tokens and verify fields
    let found_token1 = tokens
        .iter()
        .find(|t| t.get("token_id").and_then(|v| v.as_str()) == Some(&token1.token_id.to_string()));
    assert!(found_token1.is_some(), "Should find token1");

    let t1 = found_token1.unwrap();
    assert!(t1.get("token_id").is_some());
    assert!(t1.get("scopes").is_some());
    assert!(t1.get("created_at").is_some());
    assert!(t1.get("expires_at").is_some());
    assert!(t1.get("last_used_at").is_some());
    assert_eq!(
        t1.get("description").and_then(|v| v.as_str()),
        Some("Token with expiry")
    );

    // Token hash should NOT be exposed
    assert!(
        t1.get("token_hash").is_none(),
        "Token hash should not be exposed in list"
    );

    // Find revoked token
    let found_token2 = tokens
        .iter()
        .find(|t| t.get("token_id").and_then(|v| v.as_str()) == Some(&token2.token_id.to_string()));
    assert!(found_token2.is_some(), "Should find token2");

    let t2 = found_token2.unwrap();
    assert!(
        t2.get("revoked_at").is_some(),
        "Revoked token should have revoked_at"
    );
}

#[tokio::test]
async fn test_list_tokens_requires_admin_scope() {
    let server = TestServer::new().await;

    // Token without admin scope
    let read_token = create_test_token(&server, r#"["cache:read"]"#).await;

    let (status, _) = json_request(
        &server.router,
        "GET",
        "/v1/admin/tokens",
        None,
        Some(&read_token),
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN, "Should require admin scope");
}

#[tokio::test]
async fn test_list_tokens_cache_scoped_sees_own_cache_only() {
    let server = TestServer::new().await;
    let now = OffsetDateTime::now_utc();

    // Create two caches
    let cache1_id = Uuid::new_v4();
    let cache2_id = Uuid::new_v4();

    for (id, name) in [(cache1_id, "cache1"), (cache2_id, "cache2")] {
        let cache = cellar_metadata::models::CacheRow {
            cache_id: id,
            domain_id: default_storage_domain_id(),
            cache_name: name.to_string(),
            public_base_url: None,
            is_public: false,
            is_default: false,
            created_at: now,
            updated_at: now,
        };
        server.metadata().create_cache(&cache).await.unwrap();
    }

    // Create a token scoped to cache1
    let cache1_token = TokenRow {
        token_id: Uuid::new_v4(),
        cache_id: Some(cache1_id),
        token_hash: sha256_hash(b"cache1-token"),
        scopes: r#"["cache:write"]"#.to_string(),
        expires_at: None,
        revoked_at: None,
        created_at: now,
        last_used_at: None,
        description: Some("Cache1 token".to_string()),
    };
    server.metadata().create_token(&cache1_token).await.unwrap();

    // Create a token scoped to cache2
    let cache2_token = TokenRow {
        token_id: Uuid::new_v4(),
        cache_id: Some(cache2_id),
        token_hash: sha256_hash(b"cache2-token"),
        scopes: r#"["cache:write"]"#.to_string(),
        expires_at: None,
        revoked_at: None,
        created_at: now,
        last_used_at: None,
        description: Some("Cache2 token".to_string()),
    };
    server.metadata().create_token(&cache2_token).await.unwrap();

    // Create an admin token scoped to cache1
    let cache1_admin_raw = format!("cache1-admin-{}", Uuid::new_v4());
    let cache1_admin = TokenRow {
        token_id: Uuid::new_v4(),
        cache_id: Some(cache1_id),
        token_hash: sha256_hash(cache1_admin_raw.as_bytes()),
        scopes: r#"["cache:admin"]"#.to_string(),
        expires_at: None,
        revoked_at: None,
        created_at: now,
        last_used_at: None,
        description: Some("Cache1 admin".to_string()),
    };
    server.metadata().create_token(&cache1_admin).await.unwrap();

    // List tokens as cache1 admin
    let (status, response) = json_request(
        &server.router,
        "GET",
        "/v1/admin/tokens",
        None,
        Some(&cache1_admin_raw),
    )
    .await;

    assert_eq!(status, StatusCode::OK);

    let tokens = response.as_array().expect("Response should be an array");

    // Should see cache1 tokens only
    for token in tokens {
        let cache_id = token.get("cache_id");
        // Should either be cache1's tokens or global tokens (cache_id null)
        // But NOT cache2's tokens
        if let Some(cid) = cache_id
            && !cid.is_null()
        {
            assert_eq!(
                cid.as_str().unwrap(),
                cache1_id.to_string(),
                "Should only see cache1 tokens"
            );
        }
    }

    // Should NOT see cache2's token
    let has_cache2_token = tokens.iter().any(|t| {
        t.get("cache_id")
            .and_then(|v| v.as_str())
            .map(|id| id == cache2_id.to_string())
            .unwrap_or(false)
    });
    assert!(
        !has_cache2_token,
        "Cache1 admin should not see cache2 tokens"
    );
}
