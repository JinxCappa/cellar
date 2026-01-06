//! Tests for expected_chunks behavior and incomplete upload handling.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cellar_core::chunk::ChunkHash;
use cellar_core::hash::{ContentHash, NarHash};
use cellar_core::manifest::ManifestHash;
use cellar_metadata::models::TokenRow;
use common::TestServer;
use common::fixtures::{sha256_hash, split_into_chunks, test_nar_data, test_store_path};
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

fn nar_hash_sri(data: &[u8]) -> String {
    NarHash::from_content_hash(ContentHash::compute(data)).to_sri()
}

#[tokio::test]
async fn test_create_upload_requires_expected_chunks_when_enabled() {
    let server = TestServer::with_config(|config| {
        config.server.require_expected_chunks = true;
    })
    .await;

    let token = create_test_token(&server, r#"["cache:write"]"#).await;
    let nar_data = test_nar_data(64);

    let body = json!({
        "store_path": test_store_path("expected-chunks-required"),
        "nar_hash": nar_hash_sri(&nar_data),
        "nar_size": nar_data.len(),
        "chunk_size": 32
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/uploads",
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
async fn test_commit_reports_incomplete_upload_with_expected_chunks() {
    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:write"]"#).await;

    let chunk_size = cellar_core::MIN_CHUNK_SIZE as usize;
    let nar_data = test_nar_data(chunk_size * 2 + 128);
    let chunks = split_into_chunks(&nar_data, chunk_size);
    let chunk_hashes: Vec<ChunkHash> = chunks.iter().map(|c| ChunkHash::compute(c)).collect();
    let manifest_hash = ManifestHash::compute(&chunk_hashes).to_hex();

    let expected_chunks: Vec<Value> = chunk_hashes
        .iter()
        .zip(chunks.iter())
        .map(|(hash, data)| {
            json!({
                "hash": hash.to_hex(),
                "size": data.len()
            })
        })
        .collect();

    let body = json!({
        "store_path": test_store_path("incomplete-upload"),
        "nar_hash": nar_hash_sri(&nar_data),
        "nar_size": nar_data.len(),
        "chunk_size": cellar_core::MIN_CHUNK_SIZE,
        "manifest_hash": manifest_hash,
        "expected_chunks": expected_chunks
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

    let first_hash = chunk_hashes[0].to_hex();
    let request = Request::builder()
        .method("PUT")
        .uri(format!("/v1/uploads/{}/chunks/{}", upload_id, first_hash))
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/octet-stream")
        .body(Body::from(chunks[0].clone()))
        .unwrap();

    let response = server.router.clone().oneshot(request).await.unwrap();
    assert!(response.status().is_success());

    let commit_body = json!({
        "manifest": chunk_hashes.iter().map(|h| h.to_hex()).collect::<Vec<_>>()
    });
    let (status, response) = json_request(
        &server.router,
        "POST",
        &format!("/v1/uploads/{}/commit", upload_id),
        Some(commit_body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        response.get("code").and_then(|v| v.as_str()),
        Some("incomplete_upload")
    );
}
