//! Integration tests for HTTP API endpoints.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cellar_metadata::models::TokenRow;
use common::fixtures::{sha256_hash, test_nar_data, test_store_path};
use common::TestServer;
use serde_json::{json, Value};
use time::OffsetDateTime;
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

    let (status, body) = json_request(&server.router, "GET", "/v1/admin/health", None, None).await;

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

    // Request without auth token should fail with 401
    let request = Request::builder()
        .method("GET")
        .uri("/narinfo/abcdef1234567890abcdef1234567890")
        .body(Body::empty())
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
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
    let nar_hash_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        nar_hash_bytes,
    );
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

    assert_eq!(status, StatusCode::CREATED, "Commit should succeed with correct NAR hash");
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
    let wrong_hash_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        wrong_hash_bytes,
    );
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
    assert!(
        response.status() == StatusCode::CREATED || response.status() == StatusCode::OK
    );

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

    assert_eq!(status, StatusCode::BAD_REQUEST, "Commit should fail with wrong NAR hash");
    assert_eq!(
        response.get("code").and_then(|v| v.as_str()),
        Some("hash_mismatch"),
        "Error code should be hash_mismatch"
    );
}

#[tokio::test]
async fn test_rate_limiting_disabled_by_default() {
    // Rate limiting is disabled by default in test configuration,
    // so all requests should succeed without rate limit errors
    let server = TestServer::new().await;

    // Make multiple requests in rapid succession
    for _ in 0..10 {
        let (status, _) =
            json_request(&server.router, "GET", "/v1/capabilities", None, None).await;
        assert_eq!(status, StatusCode::OK);
    }
}
