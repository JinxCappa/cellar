//! Tests for admin token creation validation paths.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cellar_core::storage_domain::default_storage_domain_id;
use cellar_metadata::models::{CacheRow, TokenRow};
use common::TestServer;
use common::fixtures::sha256_hash;
use serde_json::{Value, json};
use time::OffsetDateTime;
use tower::ServiceExt;
use uuid::Uuid;

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

async fn create_token_for_cache(
    server: &TestServer,
    scopes: &str,
    cache_id: Option<Uuid>,
) -> String {
    let token_id = Uuid::new_v4();
    let raw_token = format!("test-token-{}", Uuid::new_v4());
    let token_hash = sha256_hash(raw_token.as_bytes());

    let token = TokenRow {
        token_id,
        cache_id,
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
        .expect("Failed to create test token");

    raw_token
}

async fn create_cache(server: &TestServer, name: &str) -> Uuid {
    let now = OffsetDateTime::now_utc();
    let cache_id = Uuid::new_v4();
    let cache = CacheRow {
        cache_id,
        domain_id: default_storage_domain_id(),
        cache_name: name.to_string(),
        public_base_url: None,
        is_public: true,
        is_default: false,
        created_at: now,
        updated_at: now,
    };
    server
        .metadata()
        .create_cache(&cache)
        .await
        .expect("Failed to create cache");
    cache_id
}

#[tokio::test]
async fn test_create_token_rejects_invalid_scope() {
    let server = TestServer::new().await;
    let token = create_token_for_cache(&server, r#"["cache:admin"]"#, None).await;

    let body = json!({
        "scopes": ["cache:read", "cache:invalid"]
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/tokens",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        response.get("code").and_then(|v| v.as_str()),
        Some("bad_request")
    );
}

#[tokio::test]
async fn test_create_token_rejects_cache_admin_with_cache_id() {
    let server = TestServer::new().await;
    let token = create_token_for_cache(&server, r#"["cache:admin"]"#, None).await;
    let cache_id = create_cache(&server, "token-cache").await;

    let body = json!({
        "scopes": ["cache:admin"],
        "cache_id": cache_id.to_string()
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/tokens",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        response.get("code").and_then(|v| v.as_str()),
        Some("bad_request")
    );
}

#[tokio::test]
async fn test_create_token_rejects_missing_cache() {
    let server = TestServer::new().await;
    let token = create_token_for_cache(&server, r#"["cache:admin"]"#, None).await;
    let cache_id = Uuid::new_v4();

    let body = json!({
        "scopes": ["cache:read"],
        "cache_id": cache_id.to_string()
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/tokens",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        response.get("code").and_then(|v| v.as_str()),
        Some("bad_request")
    );
}

#[tokio::test]
async fn test_create_token_rejects_invalid_cache_id() {
    let server = TestServer::new().await;
    let token = create_token_for_cache(&server, r#"["cache:admin"]"#, None).await;

    let body = json!({
        "scopes": ["cache:read"],
        "cache_id": "not-a-uuid"
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/tokens",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        response.get("code").and_then(|v| v.as_str()),
        Some("bad_request")
    );
}

#[tokio::test]
async fn test_create_token_rejects_expires_in_too_large() {
    let server = TestServer::new().await;
    let token = create_token_for_cache(&server, r#"["cache:admin"]"#, None).await;

    let body = json!({
        "scopes": ["cache:read"],
        "expires_in_secs": (i64::MAX as u64) + 1
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/tokens",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        response.get("code").and_then(|v| v.as_str()),
        Some("bad_request")
    );
}

#[tokio::test]
async fn test_cache_scoped_admin_ignores_body_cache_id() {
    let server = TestServer::new().await;
    let cache_id = create_cache(&server, "scoped-cache").await;
    let token = create_token_for_cache(&server, r#"["cache:admin"]"#, Some(cache_id)).await;

    let body = json!({
        "scopes": ["cache:read"],
        "cache_id": "not-a-uuid"
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/tokens",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let token_id = response.get("token_id").unwrap().as_str().unwrap();
    let token_id = Uuid::parse_str(token_id).unwrap();

    let created = server
        .metadata()
        .get_token(token_id)
        .await
        .unwrap()
        .expect("token should exist");
    assert_eq!(created.cache_id, Some(cache_id));
}
