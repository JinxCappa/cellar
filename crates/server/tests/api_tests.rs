//! Integration tests for HTTP API endpoints.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cellar_core::storage_domain::default_storage_domain_id;
use cellar_metadata::models::TokenRow;
use common::TestServer;
use common::fixtures::{sha256_hash, test_nar_data, test_store_path};
use serde_json::{Value, json};
use time::OffsetDateTime;
use tower::ServiceExt;
use uuid::Uuid;

fn default_domain_id() -> Uuid {
    default_storage_domain_id()
}

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

#[tokio::test]
async fn test_capabilities_endpoint() {
    let server = TestServer::new().await;

    let (status, body) = json_request(&server.router, "GET", "/v1/capabilities", None, None).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.get("api_version").is_some());
    assert!(body.get("max_chunk_size").is_some());
}

#[tokio::test]
async fn test_health_check() {
    let server = TestServer::new().await;

    // Health endpoint is at /v1/health (not /v1/admin/health) to indicate it's intentionally unauthenticated
    let (status, body) = json_request(&server.router, "GET", "/v1/health", None, None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.get("status").and_then(|v| v.as_str()), Some("ok"));
}

#[tokio::test]
async fn test_nix_cache_info() {
    let server = TestServer::new().await;

    let request = Request::builder()
        .method("GET")
        .uri("/nix-cache-info")
        .body(Body::empty())
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

    assert!(body_str.contains("StoreDir: /nix/store"));
    assert!(body_str.contains("WantMassQuery: 1"));
}

#[tokio::test]
async fn test_create_upload_requires_auth() {
    let server = TestServer::new().await;

    let body = json!({
        "store_path": test_store_path("test"),
        "nar_hash": "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        "nar_size": 1000
    });

    let (status, _) = json_request(&server.router, "POST", "/v1/uploads", Some(body), None).await;

    // Should fail without auth
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_create_upload_with_auth() {
    let server = TestServer::new().await;

    // Create a token with write access
    let token = create_test_token(&server, r#"["cache:write"]"#).await;

    let body = json!({
        "store_path": test_store_path("test-package"),
        "nar_hash": "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        "nar_size": 1000
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/uploads",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert!(response.get("upload_id").is_some());
}

#[tokio::test]
async fn test_get_upload_status() {
    let server = TestServer::new().await;

    let token = create_test_token(&server, r#"["cache:write"]"#).await;

    // Create upload
    let body = json!({
        "store_path": test_store_path("another-package"),
        "nar_hash": "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        "nar_size": 500
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/uploads",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let upload_id = response.get("upload_id").unwrap().as_str().unwrap();

    // Get upload status
    let (status, response) = json_request(
        &server.router,
        "GET",
        &format!("/v1/uploads/{}", upload_id),
        None,
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    // Response should have expected fields
    assert!(response.get("received_chunks").is_some());
    assert!(response.get("missing_chunks").is_some());
    assert!(response.get("expires_at").is_some());
}

#[tokio::test]
async fn test_narinfo_not_found() {
    let server = TestServer::new().await;

    // Create a token with read access
    let token = create_test_token(&server, r#"["cache:read"]"#).await;

    let request = Request::builder()
        .method("GET")
        .uri("/narinfo/abcdef1234567890abcdef1234567890")
        .header("Authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_narinfo_requires_auth() {
    let server = TestServer::new().await;

    // Request without auth token returns 404 for nonexistent paths when a default
    // public cache is configured (unauthenticated access falls back to default cache).
    let request = Request::builder()
        .method("GET")
        .uri("/narinfo/abcdef1234567890abcdef1234567890")
        .body(Body::empty())
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_token_creation_requires_admin() {
    let server = TestServer::new().await;

    // Token with only read/write access (not admin)
    let token = create_test_token(&server, r#"["cache:read","cache:write"]"#).await;

    let body = json!({
        "name": "New Token",
        "scopes": ["cache:read"]
    });

    let (status, _) = json_request(
        &server.router,
        "POST",
        "/v1/admin/tokens",
        Some(body),
        Some(&token),
    )
    .await;

    // Should fail without admin scope
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_gc_trigger_requires_admin() {
    let server = TestServer::new().await;

    let token = create_test_token(&server, r#"["cache:write"]"#).await;

    let body = json!({
        "job_type": "chunk_gc"
    });

    let (status, _) = json_request(
        &server.router,
        "POST",
        "/v1/admin/gc",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_admin_gc_trigger() {
    let server = TestServer::new().await;

    // Token with admin access (cache:admin scope)
    let token = create_test_token(&server, r#"["cache:admin"]"#).await;

    let body = json!({
        "job_type": "chunk_gc"
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/gc",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(response.get("job_id").is_some());
}

#[tokio::test]
async fn test_metrics_endpoint() {
    let server = TestServer::new().await;

    // Metrics requires admin auth
    let token = create_test_token(&server, r#"["cache:admin"]"#).await;

    let (status, body) = json_request(
        &server.router,
        "GET",
        "/v1/admin/metrics",
        None,
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    // Metrics should have some structure
    assert!(body.is_object());
    assert!(body.get("chunks_count").is_some());
}

#[tokio::test]
async fn test_full_upload_cycle_with_nar_hash_verification() {
    use sha2::{Digest, Sha256};

    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:write"]"#).await;

    // Create test data with proper NAR header
    let nar_data = test_nar_data(64);

    // Compute the correct NAR hash (need raw bytes for base64)
    let mut hasher = Sha256::new();
    hasher.update(&nar_data);
    let nar_hash_bytes = hasher.finalize();
    let nar_hash_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, nar_hash_bytes);
    let nar_hash_sri = format!("sha256-{}", nar_hash_b64);

    // Compute chunk hash (hex)
    let chunk_hash_hex = sha256_hash(&nar_data);

    // Create upload session
    let body = json!({
        "store_path": test_store_path("verified-package"),
        "nar_hash": nar_hash_sri,
        "nar_size": nar_data.len(),
        "chunk_hashes": [chunk_hash_hex]
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/uploads",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let upload_id = response.get("upload_id").unwrap().as_str().unwrap();

    // Upload the chunk
    let chunk_upload_uri = format!("/v1/uploads/{}/chunks/{}", upload_id, chunk_hash_hex);
    let request = Request::builder()
        .method("PUT")
        .uri(&chunk_upload_uri)
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/octet-stream")
        .body(Body::from(nar_data.to_vec()))
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();
    assert!(
        response.status() == StatusCode::CREATED || response.status() == StatusCode::OK,
        "Chunk upload failed with status: {:?}",
        response.status()
    );

    // Commit the upload
    let commit_body = json!({
        "manifest": [chunk_hash_hex]
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        &format!("/v1/uploads/{}/commit", upload_id),
        Some(commit_body),
        Some(&token),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::CREATED,
        "Commit should succeed with correct NAR hash (status={status}, body={response:?})"
    );
}

#[tokio::test]
async fn test_commit_fails_with_wrong_nar_hash() {
    use sha2::{Digest, Sha256};

    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:write"]"#).await;

    // Create test data with proper NAR header
    let nar_data = test_nar_data(64);

    // Compute the chunk hash (correct)
    let chunk_hash_hex = sha256_hash(&nar_data);

    // Use a WRONG NAR hash (hash of different content)
    let mut wrong_hasher = Sha256::new();
    wrong_hasher.update(b"different content");
    let wrong_hash_bytes = wrong_hasher.finalize();
    let wrong_hash_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, wrong_hash_bytes);
    let wrong_nar_hash_sri = format!("sha256-{}", wrong_hash_b64);

    // Create upload session with wrong NAR hash
    let body = json!({
        "store_path": test_store_path("wrong-hash-package"),
        "nar_hash": wrong_nar_hash_sri,
        "nar_size": nar_data.len(),
        "chunk_hashes": [chunk_hash_hex]
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/uploads",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let upload_id = response.get("upload_id").unwrap().as_str().unwrap();

    // Upload the chunk (this succeeds)
    let chunk_upload_uri = format!("/v1/uploads/{}/chunks/{}", upload_id, chunk_hash_hex);
    let request = Request::builder()
        .method("PUT")
        .uri(&chunk_upload_uri)
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/octet-stream")
        .body(Body::from(nar_data.to_vec()))
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();
    assert!(response.status() == StatusCode::CREATED || response.status() == StatusCode::OK);

    // Commit should FAIL because NAR hash doesn't match
    let commit_body = json!({
        "manifest": [chunk_hash_hex]
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        &format!("/v1/uploads/{}/commit", upload_id),
        Some(commit_body),
        Some(&token),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "Commit should fail with wrong NAR hash"
    );
    assert_eq!(
        response.get("code").and_then(|v| v.as_str()),
        Some("hash_mismatch"),
        "Error code should be hash_mismatch"
    );
}

#[tokio::test]
async fn test_commit_fails_with_empty_manifest() {
    use sha2::{Digest, Sha256};

    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:write"]"#).await;

    let nar_data = test_nar_data(64);
    let chunk_hash_hex = sha256_hash(&nar_data);
    let mut nar_hasher = Sha256::new();
    nar_hasher.update(&nar_data);
    let nar_hash_bytes = nar_hasher.finalize();
    let nar_hash_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, nar_hash_bytes);
    let nar_hash_sri = format!("sha256-{}", nar_hash_b64);

    let body = json!({
        "store_path": test_store_path("empty-manifest-package"),
        "nar_hash": nar_hash_sri,
        "nar_size": nar_data.len(),
        "chunk_hashes": [chunk_hash_hex]
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/uploads",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let upload_id = response.get("upload_id").unwrap().as_str().unwrap();

    let commit_body = json!({
        "manifest": []
    });

    let (status, _response) = json_request(
        &server.router,
        "POST",
        &format!("/v1/uploads/{}/commit", upload_id),
        Some(commit_body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_rate_limiting_disabled_by_default() {
    // Rate limiting is disabled by default in test configuration,
    // so all requests should succeed without rate limit errors
    let server = TestServer::new().await;

    // Make multiple requests in rapid succession
    for _ in 0..10 {
        let (status, _) = json_request(&server.router, "GET", "/v1/capabilities", None, None).await;
        assert_eq!(status, StatusCode::OK);
    }
}

#[tokio::test]
async fn test_global_admin_can_revoke_cache_scoped_token() {
    // REGRESSION TEST: Verify global admins can revoke cache-scoped tokens.
    // Previously, the check `token.cache_id != auth.token.cache_id` prevented
    // global admins (cache_id=None) from revoking cache-scoped tokens (cache_id=Some(uuid)).
    let server = TestServer::new().await;

    // Create a cache
    let cache_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();
    let cache = cellar_metadata::models::CacheRow {
        cache_id,
        domain_id: default_domain_id(),
        cache_name: "test-cache".to_string(),
        public_base_url: None,
        is_public: false,
        is_default: false,
        created_at: now,
        updated_at: now,
    };
    server
        .metadata()
        .create_cache(&cache)
        .await
        .expect("Failed to create cache");

    // Create a cache-scoped token (not admin)
    let cache_token_id = Uuid::new_v4();
    let cache_token_raw = format!("cache-token-{}", Uuid::new_v4());
    let cache_token_hash = sha256_hash(cache_token_raw.as_bytes());

    let cache_token = TokenRow {
        token_id: cache_token_id,
        cache_id: Some(cache_id),
        token_hash: cache_token_hash,
        scopes: r#"["cache:write"]"#.to_string(),
        expires_at: None,
        revoked_at: None,
        created_at: OffsetDateTime::now_utc(),
        last_used_at: None,
        description: Some("Cache-scoped token".to_string()),
    };

    server
        .metadata()
        .create_token(&cache_token)
        .await
        .expect("Failed to create cache-scoped token");

    // Create a global admin token
    let global_admin_token = create_test_token(&server, r#"["cache:admin"]"#).await;

    // Global admin should be able to revoke the cache-scoped token
    let (status, _) = json_request(
        &server.router,
        "DELETE",
        &format!("/v1/admin/tokens/{}", cache_token_id),
        None,
        Some(&global_admin_token),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "Global admin should be able to revoke cache-scoped token"
    );

    // Verify token is revoked
    let revoked_token = server
        .metadata()
        .get_token(cache_token_id)
        .await
        .expect("Get token failed")
        .expect("Token not found");
    assert!(
        revoked_token.revoked_at.is_some(),
        "Token should be revoked"
    );
}

#[tokio::test]
async fn test_cache_scoped_admin_cannot_revoke_other_cache_token() {
    // Verify cache-scoped admins (if they existed) can't revoke tokens from other caches.
    // Note: Currently cache-scoped admin tokens are prevented at creation time.
    let server = TestServer::new().await;

    // Create two caches
    let cache1_id = Uuid::new_v4();
    let cache2_id = Uuid::new_v4();

    let now = OffsetDateTime::now_utc();
    for (id, name) in [(cache1_id, "cache1"), (cache2_id, "cache2")] {
        let cache = cellar_metadata::models::CacheRow {
            cache_id: id,
            domain_id: default_domain_id(),
            cache_name: name.to_string(),
            public_base_url: None,
            is_public: false,
            is_default: false,
            created_at: now,
            updated_at: now,
        };
        server
            .metadata()
            .create_cache(&cache)
            .await
            .expect("Failed to create cache");
    }

    // Create a token scoped to cache2
    let cache2_token_id = Uuid::new_v4();
    let cache2_token_raw = format!("cache2-token-{}", Uuid::new_v4());
    let cache2_token_hash = sha256_hash(cache2_token_raw.as_bytes());

    let cache2_token = TokenRow {
        token_id: cache2_token_id,
        cache_id: Some(cache2_id),
        token_hash: cache2_token_hash,
        scopes: r#"["cache:write"]"#.to_string(),
        expires_at: None,
        revoked_at: None,
        created_at: OffsetDateTime::now_utc(),
        last_used_at: None,
        description: Some("Cache2 token".to_string()),
    };

    server
        .metadata()
        .create_token(&cache2_token)
        .await
        .expect("Failed to create cache2 token");

    // Create a token scoped to cache1 with admin scope
    // NOTE: This manually creates a cache-scoped admin token by bypassing the API validation.
    // In production, the API prevents creating cache:admin tokens with cache_id != None.
    let cache1_admin_id = Uuid::new_v4();
    let cache1_admin_raw = format!("cache1-admin-{}", Uuid::new_v4());
    let cache1_admin_hash = sha256_hash(cache1_admin_raw.as_bytes());

    let cache1_admin = TokenRow {
        token_id: cache1_admin_id,
        cache_id: Some(cache1_id),
        token_hash: cache1_admin_hash,
        scopes: r#"["cache:admin"]"#.to_string(),
        expires_at: None,
        revoked_at: None,
        created_at: OffsetDateTime::now_utc(),
        last_used_at: None,
        description: Some("Cache1 admin token".to_string()),
    };

    server
        .metadata()
        .create_token(&cache1_admin)
        .await
        .expect("Failed to create cache1 admin token");

    // Cache1 admin should NOT be able to revoke cache2 token
    let (status, _) = json_request(
        &server.router,
        "DELETE",
        &format!("/v1/admin/tokens/{}", cache2_token_id),
        None,
        Some(&cache1_admin_raw),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "Cache-scoped admin should not be able to revoke token from another cache"
    );

    // Verify token is NOT revoked
    let token = server
        .metadata()
        .get_token(cache2_token_id)
        .await
        .expect("Get token failed")
        .expect("Token not found");
    assert!(token.revoked_at.is_none(), "Token should not be revoked");
}

// =============================================================================
// Security fix tests
// =============================================================================

#[tokio::test]
async fn test_prometheus_metrics_endpoint_enabled_by_default() {
    let server = TestServer::new().await;

    // Prometheus /metrics endpoint should be accessible without auth
    let request = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Prometheus metrics endpoint should be enabled by default"
    );
}

#[tokio::test]
async fn test_prometheus_metrics_endpoint_disabled_via_config() {
    let server = TestServer::with_config(|config| {
        config.server.metrics_enabled = false;
    })
    .await;

    // Prometheus /metrics endpoint should NOT return 200 when disabled
    // (it falls through to the narinfo fallback handler which returns 401)
    let request = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();
    assert_ne!(
        response.status(),
        StatusCode::OK,
        "Prometheus metrics endpoint should not return 200 when disabled"
    );
}

#[tokio::test]
async fn test_chunk_upload_rejects_oversized_chunks() {
    use sha2::{Digest, Sha256};

    // MIN_CHUNK_SIZE is 1 MiB, set max_chunk_size to minimum allowed value
    let max_chunk_size = cellar_core::MIN_CHUNK_SIZE;

    let server = TestServer::with_config(|config| {
        config.server.max_chunk_size = max_chunk_size;
        config.server.default_chunk_size = max_chunk_size;
    })
    .await;

    let token = create_test_token(&server, r#"["cache:write"]"#).await;

    // Create data that exceeds max_chunk_size by 1 byte
    let oversized_data = vec![0u8; max_chunk_size as usize + 1];
    let chunk_hash_hex = sha256_hash(&oversized_data);

    // Compute NAR hash
    let mut hasher = Sha256::new();
    hasher.update(&oversized_data);
    let nar_hash_bytes = hasher.finalize();
    let nar_hash_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, nar_hash_bytes);
    let nar_hash_sri = format!("sha256-{}", nar_hash_b64);

    // Create upload session
    let body = json!({
        "store_path": test_store_path("oversized-chunk-test"),
        "nar_hash": nar_hash_sri,
        "nar_size": oversized_data.len(),
        "chunk_hashes": [chunk_hash_hex]
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/uploads",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let upload_id = response.get("upload_id").unwrap().as_str().unwrap();

    // Upload oversized chunk - should be rejected
    let chunk_upload_uri = format!("/v1/uploads/{}/chunks/{}", upload_id, chunk_hash_hex);
    let request = Request::builder()
        .method("PUT")
        .uri(&chunk_upload_uri)
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/octet-stream")
        .body(Body::from(oversized_data))
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Oversized chunks should be rejected"
    );
}

#[tokio::test]
async fn test_chunk_upload_accepts_max_size_chunks() {
    use sha2::{Digest, Sha256};

    // MIN_CHUNK_SIZE is 1 MiB, set max_chunk_size to minimum allowed value
    let max_chunk_size = cellar_core::MIN_CHUNK_SIZE;

    let server = TestServer::with_config(|config| {
        config.server.max_chunk_size = max_chunk_size;
        config.server.default_chunk_size = max_chunk_size;
    })
    .await;

    let token = create_test_token(&server, r#"["cache:write"]"#).await;

    // Create data exactly at max_chunk_size
    let exact_data = vec![0u8; max_chunk_size as usize];
    let chunk_hash_hex = sha256_hash(&exact_data);

    // Compute NAR hash
    let mut hasher = Sha256::new();
    hasher.update(&exact_data);
    let nar_hash_bytes = hasher.finalize();
    let nar_hash_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, nar_hash_bytes);
    let nar_hash_sri = format!("sha256-{}", nar_hash_b64);

    // Create upload session
    let body = json!({
        "store_path": test_store_path("exact-size-chunk-test"),
        "nar_hash": nar_hash_sri,
        "nar_size": exact_data.len(),
        "chunk_hashes": [chunk_hash_hex]
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/uploads",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let upload_id = response.get("upload_id").unwrap().as_str().unwrap();

    // Upload exact-size chunk - should succeed
    let chunk_upload_uri = format!("/v1/uploads/{}/chunks/{}", upload_id, chunk_hash_hex);
    let request = Request::builder()
        .method("PUT")
        .uri(&chunk_upload_uri)
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/octet-stream")
        .body(Body::from(exact_data))
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();
    assert!(
        response.status() == StatusCode::CREATED || response.status() == StatusCode::OK,
        "Chunks at exactly max_chunk_size should be accepted, got {:?}",
        response.status()
    );
}

#[tokio::test]
async fn test_chunk_dedup_shortcircuit_disabled_validates_hash() {
    use sha2::{Digest, Sha256};

    // Create server with shortcircuit disabled
    let server = TestServer::with_config(|config| {
        config.server.disable_chunk_dedup_shortcircuit = true;
    })
    .await;

    let token = create_test_token(&server, r#"["cache:write"]"#).await;

    // Create original chunk data
    let original_data = test_nar_data(64);
    let chunk_hash_hex = sha256_hash(&original_data);

    // Compute NAR hash
    let mut hasher = Sha256::new();
    hasher.update(&original_data);
    let nar_hash_bytes = hasher.finalize();
    let nar_hash_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, nar_hash_bytes);
    let nar_hash_sri = format!("sha256-{}", nar_hash_b64);

    // First upload - create the chunk
    let body = json!({
        "store_path": test_store_path("dedup-test-1"),
        "nar_hash": nar_hash_sri,
        "nar_size": original_data.len(),
        "chunk_hashes": [chunk_hash_hex]
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/uploads",
        Some(body),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let upload_id = response.get("upload_id").unwrap().as_str().unwrap();

    // Upload the original chunk
    let chunk_upload_uri = format!("/v1/uploads/{}/chunks/{}", upload_id, chunk_hash_hex);
    let request = Request::builder()
        .method("PUT")
        .uri(&chunk_upload_uri)
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/octet-stream")
        .body(Body::from(original_data.to_vec()))
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();
    assert!(
        response.status() == StatusCode::CREATED || response.status() == StatusCode::OK,
        "Initial chunk upload should succeed"
    );

    // Second upload - try to re-upload the same hash with WRONG data
    // This tests the side-channel protection
    let body2 = json!({
        "store_path": test_store_path("dedup-test-2"),
        "nar_hash": nar_hash_sri,
        "nar_size": original_data.len(),
        "chunk_hashes": [chunk_hash_hex]
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/uploads",
        Some(body2),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let upload_id2 = response.get("upload_id").unwrap().as_str().unwrap();

    // Try to upload WRONG data with the same chunk hash
    let wrong_data = b"totally different data that should fail hash verification";
    let chunk_upload_uri2 = format!("/v1/uploads/{}/chunks/{}", upload_id2, chunk_hash_hex);
    let request = Request::builder()
        .method("PUT")
        .uri(&chunk_upload_uri2)
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/octet-stream")
        .body(Body::from(wrong_data.to_vec()))
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "With shortcircuit disabled, re-uploading existing chunk with wrong body should fail hash verification"
    );
}

// =============================================================================
// GC listing tests
// =============================================================================

#[tokio::test]
async fn test_gc_list_global_admin_sees_global_jobs_only_by_default() {
    // Global admin with no cache_id filter should see only global GC jobs (cache_id IS NULL)
    // This ensures tenant isolation - admins don't accidentally see other tenants' jobs.
    let server = TestServer::new().await;
    let global_admin = create_test_token(&server, r#"["cache:admin"]"#).await;

    // Create a cache for scoped GC job
    let cache_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();
    let cache = cellar_metadata::models::CacheRow {
        cache_id,
        domain_id: default_domain_id(),
        cache_name: "gc-list-test".to_string(),
        public_base_url: None,
        is_public: false,
        is_default: false,
        created_at: now,
        updated_at: now,
    };
    server
        .metadata()
        .create_cache(&cache)
        .await
        .expect("Failed to create cache");

    // Create a cache-scoped GC job directly in the database
    let cache_scoped_job = cellar_metadata::models::GcJobRow {
        gc_job_id: Uuid::new_v4(),
        cache_id: Some(cache_id),
        job_type: "chunk_gc".to_string(),
        state: "finished".to_string(),
        started_at: Some(now),
        finished_at: Some(now),
        stats_json: None,
        sweep_checkpoint_json: None,
    };
    server
        .metadata()
        .create_gc_job(&cache_scoped_job)
        .await
        .expect("Failed to create cache-scoped GC job");

    // Create a global GC job
    let global_job = cellar_metadata::models::GcJobRow {
        gc_job_id: Uuid::new_v4(),
        cache_id: None,
        job_type: "manifest_gc".to_string(),
        state: "finished".to_string(),
        started_at: Some(now),
        finished_at: Some(now),
        stats_json: None,
        sweep_checkpoint_json: None,
    };
    server
        .metadata()
        .create_gc_job(&global_job)
        .await
        .expect("Failed to create global GC job");

    // List GC jobs as global admin without cache_id filter
    let (status, response) = json_request(
        &server.router,
        "GET",
        "/v1/admin/gc",
        None,
        Some(&global_admin),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let jobs = response.as_array().expect("Response should be an array");

    // Should see only global jobs (cache_id = NULL), not cache-scoped jobs
    assert!(!jobs.is_empty(), "Global admin should see global jobs");

    // Verify only global jobs are present (no cache-scoped jobs)
    let has_cache_scoped = jobs
        .iter()
        .any(|j| j.get("cache_id").and_then(|v| v.as_str()).is_some());
    let has_global = jobs
        .iter()
        .any(|j| j.get("cache_id").map(|v| v.is_null()).unwrap_or(false));

    assert!(
        !has_cache_scoped,
        "Should NOT include cache-scoped jobs when no cache_id filter provided"
    );
    assert!(has_global, "Should include global job in results");
}

#[tokio::test]
async fn test_gc_list_global_admin_can_filter_by_cache_id() {
    // Global admin can explicitly filter by cache_id to see that cache's jobs
    let server = TestServer::new().await;
    let global_admin = create_test_token(&server, r#"["cache:admin"]"#).await;

    // Create a cache for scoped GC job
    let cache_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();
    let cache = cellar_metadata::models::CacheRow {
        cache_id,
        domain_id: default_domain_id(),
        cache_name: "gc-filter-test".to_string(),
        public_base_url: None,
        is_public: false,
        is_default: false,
        created_at: now,
        updated_at: now,
    };
    server
        .metadata()
        .create_cache(&cache)
        .await
        .expect("Failed to create cache");

    // Create a cache-scoped GC job
    let cache_scoped_job = cellar_metadata::models::GcJobRow {
        gc_job_id: Uuid::new_v4(),
        cache_id: Some(cache_id),
        job_type: "chunk_gc".to_string(),
        state: "finished".to_string(),
        started_at: Some(now),
        finished_at: Some(now),
        stats_json: None,
        sweep_checkpoint_json: None,
    };
    server
        .metadata()
        .create_gc_job(&cache_scoped_job)
        .await
        .expect("Failed to create cache-scoped GC job");

    // List GC jobs with explicit cache_id filter
    let (status, response) = json_request(
        &server.router,
        "GET",
        &format!("/v1/admin/gc?cache_id={}", cache_id),
        None,
        Some(&global_admin),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let jobs = response.as_array().expect("Response should be an array");

    // Should see the cache-scoped job when explicitly filtering
    assert!(
        !jobs.is_empty(),
        "Global admin should see cache jobs when filtering by cache_id"
    );

    // Verify we got the right job
    let has_our_cache_job = jobs.iter().any(|j| {
        j.get("cache_id")
            .and_then(|v| v.as_str())
            .map(|id| id == cache_id.to_string())
            .unwrap_or(false)
    });
    assert!(has_our_cache_job, "Should include the filtered cache's job");
}

#[tokio::test]
async fn test_gc_list_cache_scoped_admin_sees_only_own_cache() {
    // Cache-scoped admin should only see their own cache's GC jobs, not other caches' or global jobs
    let server = TestServer::new().await;

    let now = OffsetDateTime::now_utc();

    // Create cache 1 with a scoped admin token
    let cache1_id = Uuid::new_v4();
    let cache1 = cellar_metadata::models::CacheRow {
        cache_id: cache1_id,
        domain_id: default_domain_id(),
        cache_name: "cache1-gc-scope-test".to_string(),
        public_base_url: None,
        is_public: false,
        is_default: false,
        created_at: now,
        updated_at: now,
    };
    server
        .metadata()
        .create_cache(&cache1)
        .await
        .expect("Failed to create cache1");

    // Create cache 2
    let cache2_id = Uuid::new_v4();
    let cache2 = cellar_metadata::models::CacheRow {
        cache_id: cache2_id,
        domain_id: default_domain_id(),
        cache_name: "cache2-gc-scope-test".to_string(),
        public_base_url: None,
        is_public: false,
        is_default: false,
        created_at: now,
        updated_at: now,
    };
    server
        .metadata()
        .create_cache(&cache2)
        .await
        .expect("Failed to create cache2");

    // Create a cache-scoped admin token for cache1
    let cache1_admin_id = Uuid::new_v4();
    let cache1_admin_raw = format!("cache1-admin-{}", cache1_admin_id);
    let cache1_admin_hash = sha256_hash(cache1_admin_raw.as_bytes());
    let cache1_admin_token = cellar_metadata::models::TokenRow {
        token_id: cache1_admin_id,
        cache_id: Some(cache1_id),
        token_hash: cache1_admin_hash,
        scopes: r#"["cache:admin"]"#.to_string(),
        expires_at: None,
        revoked_at: None,
        created_at: now,
        last_used_at: None,
        description: Some("Cache1 Admin".to_string()),
    };
    server
        .metadata()
        .create_token(&cache1_admin_token)
        .await
        .expect("Failed to create cache1 admin token");

    // Create GC job for cache1
    let cache1_job = cellar_metadata::models::GcJobRow {
        gc_job_id: Uuid::new_v4(),
        cache_id: Some(cache1_id),
        job_type: "chunk_gc".to_string(),
        state: "finished".to_string(),
        started_at: Some(now),
        finished_at: Some(now),
        stats_json: None,
        sweep_checkpoint_json: None,
    };
    server
        .metadata()
        .create_gc_job(&cache1_job)
        .await
        .expect("Failed to create cache1 GC job");

    // Create GC job for cache2
    let cache2_job = cellar_metadata::models::GcJobRow {
        gc_job_id: Uuid::new_v4(),
        cache_id: Some(cache2_id),
        job_type: "chunk_gc".to_string(),
        state: "finished".to_string(),
        started_at: Some(now),
        finished_at: Some(now),
        stats_json: None,
        sweep_checkpoint_json: None,
    };
    server
        .metadata()
        .create_gc_job(&cache2_job)
        .await
        .expect("Failed to create cache2 GC job");

    // Create a global GC job
    let global_job = cellar_metadata::models::GcJobRow {
        gc_job_id: Uuid::new_v4(),
        cache_id: None,
        job_type: "manifest_gc".to_string(),
        state: "finished".to_string(),
        started_at: Some(now),
        finished_at: Some(now),
        stats_json: None,
        sweep_checkpoint_json: None,
    };
    server
        .metadata()
        .create_gc_job(&global_job)
        .await
        .expect("Failed to create global GC job");

    // List GC jobs as cache1 scoped admin
    let (status, response) = json_request(
        &server.router,
        "GET",
        "/v1/admin/gc",
        None,
        Some(&cache1_admin_raw),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let jobs = response.as_array().expect("Response should be an array");

    // Should see exactly 1 job (cache1's job)
    assert_eq!(
        jobs.len(),
        1,
        "Cache-scoped admin should see exactly their own cache's jobs, found {}",
        jobs.len()
    );

    // Verify it's cache1's job
    let job_cache_id = jobs[0]
        .get("cache_id")
        .and_then(|v| v.as_str())
        .expect("Job should have cache_id");
    assert_eq!(
        job_cache_id,
        cache1_id.to_string(),
        "Should only see cache1's job"
    );
}

#[tokio::test]
async fn test_gc_trigger_uses_target_cache_id() {
    // Verify that trigger_gc with explicit cache_id targets that cache, not the token's cache
    let server = TestServer::new().await;
    let global_admin = create_test_token(&server, r#"["cache:admin"]"#).await;

    // Create a target cache
    let target_cache_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();
    let cache = cellar_metadata::models::CacheRow {
        cache_id: target_cache_id,
        domain_id: default_domain_id(),
        cache_name: "gc-target-test".to_string(),
        public_base_url: None,
        is_public: false,
        is_default: false,
        created_at: now,
        updated_at: now,
    };
    server
        .metadata()
        .create_cache(&cache)
        .await
        .expect("Failed to create cache");

    // Trigger GC with explicit cache_id
    let body = json!({
        "job_type": "chunk_gc",
        "cache_id": target_cache_id.to_string()
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/gc",
        Some(body),
        Some(&global_admin),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED);
    let job_id = response.get("job_id").unwrap().as_str().unwrap();
    let job_uuid = Uuid::parse_str(job_id).expect("Invalid job_id");

    // Wait a moment for the job to be created
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify the job was created with the target cache_id
    let job = server
        .metadata()
        .get_gc_job(job_uuid)
        .await
        .expect("Failed to get GC job")
        .expect("GC job not found");

    assert_eq!(
        job.cache_id,
        Some(target_cache_id),
        "GC job should be scoped to target cache"
    );
}

// =============================================================================
// Public cache access tests
// =============================================================================

#[tokio::test]
async fn test_private_cache_returns_not_found_to_prevent_enumeration() {
    let server = TestServer::new().await;

    // Create a private cache
    let cache_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();
    let cache = cellar_metadata::models::CacheRow {
        cache_id,
        domain_id: default_domain_id(),
        cache_name: "private-secret-cache".to_string(),
        public_base_url: None,
        is_public: false,
        is_default: false,
        created_at: now,
        updated_at: now,
    };
    server
        .metadata()
        .create_cache(&cache)
        .await
        .expect("Failed to create cache");

    // Try to access the private cache without auth
    let request = Request::builder()
        .method("GET")
        .uri("/abcdef12345678901234567890123456.narinfo")
        .header("X-Cache-Name", "private-secret-cache")
        .body(Body::empty())
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();

    // Should return NOT_FOUND, not UNAUTHORIZED (to prevent enumeration)
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "Private cache should return NOT_FOUND to prevent enumeration"
    );
}

#[tokio::test]
async fn test_nonexistent_cache_returns_not_found() {
    let server = TestServer::new().await;

    // Try to access a cache that doesn't exist
    let request = Request::builder()
        .method("GET")
        .uri("/abcdef12345678901234567890123456.narinfo")
        .header("X-Cache-Name", "does-not-exist-cache")
        .body(Body::empty())
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "Non-existent cache should return NOT_FOUND"
    );
}

#[tokio::test]
async fn test_private_and_nonexistent_cache_same_error() {
    // Verify that private cache and non-existent cache return the same error
    // to prevent cache name enumeration via error differentiation
    let server = TestServer::new().await;

    // Create a private cache
    let cache_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();
    let cache = cellar_metadata::models::CacheRow {
        cache_id,
        domain_id: default_domain_id(),
        cache_name: "enumeration-test-private".to_string(),
        public_base_url: None,
        is_public: false,
        is_default: false,
        created_at: now,
        updated_at: now,
    };
    server
        .metadata()
        .create_cache(&cache)
        .await
        .expect("Failed to create cache");

    // Request to private cache
    let private_request = Request::builder()
        .method("GET")
        .uri("/abcdef12345678901234567890123456.narinfo")
        .header("X-Cache-Name", "enumeration-test-private")
        .body(Body::empty())
        .unwrap();

    let private_response = server
        .router
        .clone()
        .oneshot(private_request)
        .await
        .unwrap();

    // Request to non-existent cache
    let nonexistent_request = Request::builder()
        .method("GET")
        .uri("/abcdef12345678901234567890123456.narinfo")
        .header("X-Cache-Name", "enumeration-test-nonexistent")
        .body(Body::empty())
        .unwrap();

    let nonexistent_response = server
        .router
        .clone()
        .oneshot(nonexistent_request)
        .await
        .unwrap();

    // Both should return the same status code
    assert_eq!(
        private_response.status(),
        nonexistent_response.status(),
        "Private and non-existent caches should return the same status to prevent enumeration"
    );

    // Both should be NOT_FOUND
    assert_eq!(private_response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_public_cache_unauthenticated_access_works() {
    // Verify that unauthenticated requests to public caches pass the auth layer
    // and reach the narinfo handler (which then checks store path visibility).
    let server = TestServer::new().await;

    // Create a public cache
    let cache_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();
    let domain_id = default_domain_id();
    let cache = cellar_metadata::models::CacheRow {
        cache_id,
        domain_id,
        cache_name: "public-access-test".to_string(),
        public_base_url: None,
        is_public: true, // Public cache
        is_default: false,
        created_at: now,
        updated_at: now,
    };
    server
        .metadata()
        .create_cache(&cache)
        .await
        .expect("Failed to create public cache");

    // Create a manifest first (required for store path foreign key)
    let store_path_hash = "abcdef12345678901234567890123456";
    let manifest_hash = sha256_hash(b"test-manifest");
    let manifest = cellar_metadata::models::ManifestRow {
        domain_id,
        manifest_hash: manifest_hash.clone(),
        chunk_size: 1024 * 1024,
        chunk_count: 1,
        nar_size: 1024,
        object_key: None,
        refcount: 1,
        created_at: now,
    };
    server
        .metadata()
        .create_manifest(&manifest, &[])
        .await
        .expect("Failed to create manifest");

    // Create a visible store path for this cache
    let store_path = cellar_metadata::models::StorePathRow {
        store_path_hash: store_path_hash.to_string(),
        cache_id: Some(cache_id),
        domain_id,
        store_path: "/nix/store/test-public-access".to_string(),
        nar_hash: sha256_hash(b"test-nar"),
        nar_size: 1024,
        manifest_hash: manifest_hash.clone(),
        created_at: now,
        committed_at: Some(now),
        visibility_state: "visible".to_string(),
        uploader_token_id: None,
        ca: None,
        chunks_verified: true,
    };
    server
        .metadata()
        .create_store_path(&store_path)
        .await
        .expect("Failed to create store path");

    // Write a narinfo file to storage (required for full success path)
    let narinfo_key = format!("{}/narinfo/{}.narinfo", cache_id, store_path_hash);
    let narinfo_content = format!(
        "StorePath: /nix/store/test-public-access\n\
         URL: nar/{}.nar\n\
         Compression: none\n\
         FileHash: sha256:{}\n\
         FileSize: 1024\n\
         NarHash: sha256:{}\n\
         NarSize: 1024\n",
        store_path_hash,
        sha256_hash(b"test-nar"),
        sha256_hash(b"test-nar"),
    );
    server
        .state
        .storage
        .put(&narinfo_key, bytes::Bytes::from(narinfo_content))
        .await
        .expect("Failed to write narinfo");

    // Make unauthenticated request with X-Cache-Name header
    let request = Request::builder()
        .method("GET")
        .uri(format!("/{}.narinfo", store_path_hash))
        .header("X-Cache-Name", "public-access-test")
        .body(Body::empty())
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();

    // Should get OK - proving auth passed and narinfo was retrieved
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Public cache should be accessible without authentication"
    );
}

/// Test that unauthenticated requests fall back to the default public cache.
#[tokio::test]
async fn test_unauthenticated_fallback_to_default_cache() {
    let server = TestServer::new().await;

    // Use the default public cache created by TestServer
    let default_cache = server
        .metadata()
        .get_default_public_cache()
        .await
        .expect("Failed to query default cache")
        .expect("Default public cache should exist");
    let cache_id = default_cache.cache_id;
    let now = OffsetDateTime::now_utc();
    let domain_id = default_domain_id();

    // Create a manifest first (required for store path foreign key)
    let store_path_hash = "defaultcache1234567890123456789a"; // 32 chars
    let manifest_hash = sha256_hash(b"default-manifest");
    let manifest = cellar_metadata::models::ManifestRow {
        domain_id,
        manifest_hash: manifest_hash.clone(),
        chunk_size: 1024 * 1024,
        chunk_count: 1,
        nar_size: 1024,
        object_key: None,
        refcount: 1,
        created_at: now,
    };
    server
        .metadata()
        .create_manifest(&manifest, &[])
        .await
        .expect("Failed to create manifest");

    // Create a visible store path for this cache
    let store_path = cellar_metadata::models::StorePathRow {
        store_path_hash: store_path_hash.to_string(),
        cache_id: Some(cache_id),
        domain_id,
        store_path: "/nix/store/test-default-access".to_string(),
        nar_hash: sha256_hash(b"test-nar-default"),
        nar_size: 1024,
        manifest_hash: manifest_hash.clone(),
        created_at: now,
        committed_at: Some(now),
        visibility_state: "visible".to_string(),
        uploader_token_id: None,
        ca: None,
        chunks_verified: true,
    };
    server
        .metadata()
        .create_store_path(&store_path)
        .await
        .expect("Failed to create store path");

    // Write a narinfo file to storage (required for full success path)
    let narinfo_key = format!("{}/narinfo/{}.narinfo", cache_id, store_path_hash);
    let narinfo_content = format!(
        "StorePath: /nix/store/test-default-access\n\
         URL: nar/{}.nar\n\
         Compression: none\n\
         FileHash: sha256:{}\n\
         FileSize: 1024\n\
         NarHash: sha256:{}\n\
         NarSize: 1024\n",
        store_path_hash,
        sha256_hash(b"test-nar-default"),
        sha256_hash(b"test-nar-default"),
    );
    server
        .state
        .storage
        .put(&narinfo_key, bytes::Bytes::from(narinfo_content))
        .await
        .expect("Failed to write narinfo");

    // Make unauthenticated request WITHOUT X-Cache-Name header
    // Should fall back to the default cache
    let request = Request::builder()
        .method("GET")
        .uri(format!("/{}.narinfo", store_path_hash))
        .body(Body::empty())
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();

    // Should get OK - proving fallback to default cache worked
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Default cache should be accessible without authentication or X-Cache-Name header"
    );
}

#[tokio::test]
async fn test_create_upload_rejects_invalid_manifest_hash() {
    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:write"]"#).await;

    // Test with invalid hex (not 64 chars)
    let body = json!({
        "store_path": test_store_path("bad-manifest-short"),
        "nar_hash": "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        "nar_size": 1024,
        "manifest_hash": "tooshort"
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/uploads",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "Should reject manifest_hash that's too short: {response:?}"
    );
    assert!(
        response
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .contains("manifest_hash"),
        "Error message should mention manifest_hash: {response:?}"
    );

    // Test with invalid hex (non-hex chars)
    let body = json!({
        "store_path": test_store_path("bad-manifest-nonhex"),
        "nar_hash": "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        "nar_size": 1024,
        "manifest_hash": "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/uploads",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "Should reject manifest_hash with non-hex chars: {response:?}"
    );

    // Test with valid manifest_hash (should succeed)
    let body = json!({
        "store_path": test_store_path("good-manifest"),
        "nar_hash": "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        "nar_size": 1024,
        "manifest_hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    });

    let (status, _) = json_request(
        &server.router,
        "POST",
        "/v1/uploads",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::CREATED,
        "Should accept valid 64-char hex manifest_hash"
    );
}

#[tokio::test]
async fn test_get_nar_returns_404_when_chunks_missing() {
    use sha2::{Digest, Sha256};

    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:write", "cache:read"]"#).await;

    // Create test data
    let nar_data = test_nar_data(64);

    // Compute hashes
    let mut hasher = Sha256::new();
    hasher.update(&nar_data);
    let nar_hash_bytes = hasher.finalize();
    let nar_hash_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, nar_hash_bytes);
    let nar_hash_sri = format!("sha256-{}", nar_hash_b64);
    let chunk_hash_hex = sha256_hash(&nar_data);

    // Create and complete a full upload cycle
    let store_path = test_store_path("missing-chunks-test");
    let body = json!({
        "store_path": &store_path,
        "nar_hash": nar_hash_sri,
        "nar_size": nar_data.len(),
        "chunk_hashes": [&chunk_hash_hex]
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/uploads",
        Some(body),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let upload_id = response.get("upload_id").unwrap().as_str().unwrap();

    // Upload the chunk
    let chunk_upload_uri = format!("/v1/uploads/{}/chunks/{}", upload_id, chunk_hash_hex);
    let request = Request::builder()
        .method("PUT")
        .uri(&chunk_upload_uri)
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/octet-stream")
        .body(Body::from(nar_data.to_vec()))
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();
    assert!(response.status() == StatusCode::CREATED || response.status() == StatusCode::OK);

    // Commit the upload
    let commit_body = json!({
        "manifest": [&chunk_hash_hex]
    });

    let (status, _) = json_request(
        &server.router,
        "POST",
        &format!("/v1/uploads/{}/commit", upload_id),
        Some(commit_body),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "Commit should succeed");

    // Extract the store path hash from the full path
    // Format: /nix/store/{hash}-{name}
    let store_path_hash = store_path
        .strip_prefix("/nix/store/")
        .unwrap()
        .split('-')
        .next()
        .unwrap();

    // Verify NAR is accessible before deleting chunk
    let request = Request::builder()
        .method("GET")
        .uri(format!("/nar/{}.nar", store_path_hash))
        .header("Authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "NAR should be accessible before chunk deletion"
    );

    // Set chunk refcount to 0 (simulating GC having decremented it)
    // This makes the chunk "missing" from the perspective of get_manifest_chunks_verified
    // which checks for refcount > 0
    server
        .metadata()
        .set_chunk_refcount(default_domain_id(), &chunk_hash_hex, 0)
        .await
        .expect("Failed to set chunk refcount to 0");

    // Try to get the NAR again - should now return 404
    let request = Request::builder()
        .method("GET")
        .uri(format!("/nar/{}.nar", store_path_hash))
        .header("Authorization", format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "NAR request should return 404 when chunks are missing (not truncated data)"
    );
}
