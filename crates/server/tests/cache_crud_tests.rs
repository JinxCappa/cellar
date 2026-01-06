//! Integration tests for cache CRUD operations.

mod common;

use axum::http::StatusCode;
use cellar_core::storage_domain::default_storage_domain_id;
use common::TestServer;
use serde_json::json;
use uuid::Uuid;

fn default_domain_id() -> Uuid {
    default_storage_domain_id()
}

// Helper to make JSON requests (duplicated for test isolation)
async fn json_request(
    router: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
    auth_token: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

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
    let body_json: serde_json::Value = if body_bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&body_bytes).unwrap_or(serde_json::Value::Null)
    };

    (status, body_json)
}

// Helper to create test tokens
// Note: scopes should be passed as JSON array string, e.g. r#"["cache:admin"]"#
async fn create_test_token(server: &TestServer, scopes: &str) -> String {
    use cellar_metadata::models::TokenRow;
    use common::fixtures::sha256_hash;
    use time::OffsetDateTime;

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
        .expect("Failed to create test token");

    raw_token
}

// =============================================================================
// Cache Management Tests
// =============================================================================

#[tokio::test]
async fn test_create_cache_requires_admin() {
    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:write"]"#).await;

    let body = json!({
        "cache_name": "test-cache",
        "is_public": false
    });

    let (status, _) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_create_cache_with_valid_data() {
    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:admin"]"#).await;

    let body = json!({
        "cache_name": "my-cache",
        "public_base_url": "https://cache.example.com",
        "is_public": true
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert!(response.get("cache_id").is_some());
    assert_eq!(
        response.get("cache_name").and_then(|v| v.as_str()),
        Some("my-cache")
    );
    assert_eq!(
        response.get("public_base_url").and_then(|v| v.as_str()),
        Some("https://cache.example.com")
    );
}

#[tokio::test]
async fn test_create_cache_empty_string_normalized() {
    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:admin"]"#).await;

    let body = json!({
        "cache_name": "cache-no-url",
        "public_base_url": "",
        "is_public": false
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let cache_id = response.get("cache_id").unwrap().as_str().unwrap();

    // Verify stored as null, not empty string
    let (status, cache) = json_request(
        &server.router,
        "GET",
        &format!("/v1/admin/caches/{}", cache_id),
        None,
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(cache.get("public_base_url").unwrap().is_null());
}

#[tokio::test]
async fn test_create_cache_invalid_name() {
    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:admin"]"#).await;

    let long_name = "a".repeat(65);
    let invalid_names: Vec<String> = vec![
        "ab".to_string(),         // Too short
        long_name,                // Too long
        "My Cache".to_string(),   // Space
        "cache_name".to_string(), // Underscore
        "Cache-1".to_string(),    // Uppercase
        "-cache".to_string(),     // Leading hyphen
        "cache-".to_string(),     // Trailing hyphen
    ];

    for invalid_name in invalid_names.iter() {
        let body = json!({
            "cache_name": invalid_name,
            "is_public": false
        });

        let (status, _) = json_request(
            &server.router,
            "POST",
            "/v1/admin/caches",
            Some(body),
            Some(&token),
        )
        .await;

        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "Name '{}' should be rejected",
            invalid_name
        );
    }
}

#[tokio::test]
async fn test_create_cache_duplicate_name() {
    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:admin"]"#).await;

    let body = json!({
        "cache_name": "duplicate",
        "is_public": false
    });

    let (status, _) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(body.clone()),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(body),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_list_caches() {
    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:admin"]"#).await;

    for i in 1..=3 {
        let body = json!({
            "cache_name": format!("cache-{}", i),
            "is_public": false
        });

        let (status, _) = json_request(
            &server.router,
            "POST",
            "/v1/admin/caches",
            Some(body),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    let (status, response) = json_request(
        &server.router,
        "GET",
        "/v1/admin/caches",
        None,
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let caches = response.get("caches").unwrap().as_array().unwrap();
    assert!(caches.len() >= 3);
}

#[tokio::test]
async fn test_get_cache_by_id() {
    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:admin"]"#).await;

    let body = json!({
        "cache_name": "get-test",
        "public_base_url": "https://cache.test",
        "is_public": true
    });

    let (status, create_resp) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(body),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let cache_id = create_resp.get("cache_id").unwrap().as_str().unwrap();

    let (status, get_resp) = json_request(
        &server.router,
        "GET",
        &format!("/v1/admin/caches/{}", cache_id),
        None,
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        get_resp.get("cache_name").and_then(|v| v.as_str()),
        Some("get-test")
    );
}

#[tokio::test]
async fn test_update_cache() {
    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:admin"]"#).await;

    let body = json!({
        "cache_name": "update-test",
        "public_base_url": "https://old.url",
        "is_public": false
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(body),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let cache_id = response.get("cache_id").unwrap().as_str().unwrap();

    let update_body = json!({
        "cache_name": "updated",
        "public_base_url": "https://new.url",
        "is_public": true
    });

    let (status, update_resp) = json_request(
        &server.router,
        "PUT",
        &format!("/v1/admin/caches/{}", cache_id),
        Some(update_body),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        update_resp.get("cache_name").and_then(|v| v.as_str()),
        Some("updated")
    );
}

#[tokio::test]
async fn test_delete_cache_with_tokens() {
    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:admin"]"#).await;

    let body = json!({
        "cache_name": "delete-test",
        "is_public": false
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(body),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let cache_id = response.get("cache_id").unwrap().as_str().unwrap();

    // Create cache-scoped token
    let token_body = json!({
        "scopes": ["cache:write"],
        "cache_id": cache_id,
        "description": "Test token"
    });

    let (status, _) = json_request(
        &server.router,
        "POST",
        "/v1/admin/tokens",
        Some(token_body),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Try to delete - should fail
    let (status, _) = json_request(
        &server.router,
        "DELETE",
        &format!("/v1/admin/caches/{}", cache_id),
        None,
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_delete_cache_success() {
    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:admin"]"#).await;

    let body = json!({
        "cache_name": "delete-ok",
        "is_public": false
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(body),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let cache_id = response.get("cache_id").unwrap().as_str().unwrap();

    let (status, _) = json_request(
        &server.router,
        "DELETE",
        &format!("/v1/admin/caches/{}", cache_id),
        None,
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::NO_CONTENT);

    // Verify deleted
    let (status, _) = json_request(
        &server.router,
        "GET",
        &format!("/v1/admin/caches/{}", cache_id),
        None,
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_delete_cache_with_active_uploads() {
    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:admin"]"#).await;

    // Create cache
    let body = json!({
        "cache_name": "delete-with-uploads",
        "is_public": false
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(body),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let cache_id_str = response.get("cache_id").unwrap().as_str().unwrap();
    let cache_id = Uuid::parse_str(cache_id_str).unwrap();
    let domain_id = default_domain_id();

    // Create an active upload session
    use cellar_metadata::models::UploadSessionRow;
    use common::fixtures::sha256_hash;
    use time::{Duration, OffsetDateTime};

    let upload = UploadSessionRow {
        upload_id: Uuid::new_v4(),
        cache_id: Some(cache_id),
        domain_id,
        store_path: "/nix/store/test".to_string(),
        store_path_hash: sha256_hash(b"/nix/store/test"),
        nar_size: 1024,
        nar_hash: sha256_hash(b"nar_content"),
        chunk_size: 256,
        manifest_hash: None,
        state: "pending".to_string(),
        owner_token_id: None,
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
        expires_at: OffsetDateTime::now_utc() + Duration::hours(1),
        trace_id: None,
        error_code: None,
        error_detail: None,
        commit_started_at: None,
        commit_progress: None,
        chunks_predeclared: false,
    };

    server
        .metadata()
        .create_session(&upload)
        .await
        .expect("Failed to create upload session");

    // Try to delete - should fail
    let (status, response) = json_request(
        &server.router,
        "DELETE",
        &format!("/v1/admin/caches/{}", cache_id_str),
        None,
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert!(
        response
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .contains("active upload")
    );
}

#[tokio::test]
async fn test_delete_cache_with_store_paths() {
    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:admin"]"#).await;

    // Create cache
    let body = json!({
        "cache_name": "delete-with-paths",
        "is_public": false
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(body),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let cache_id_str = response.get("cache_id").unwrap().as_str().unwrap();
    let cache_id = Uuid::parse_str(cache_id_str).unwrap();

    // Create chunks, then manifest, then store path (foreign key dependencies)
    use cellar_metadata::models::{ChunkRow, ManifestChunkRow, ManifestRow, StorePathRow};
    use common::fixtures::sha256_hash;
    use time::OffsetDateTime;
    let domain_id = default_domain_id();

    // Create chunk first
    let chunk_hash = sha256_hash(b"chunk1");
    let chunk = ChunkRow {
        domain_id,
        chunk_hash: chunk_hash.clone(),
        size_bytes: 256,
        object_key: None,
        refcount: 0,
        created_at: OffsetDateTime::now_utc(),
        last_accessed_at: None,
    };
    server
        .metadata()
        .upsert_chunk(&chunk)
        .await
        .expect("Failed to create chunk");

    // Create manifest
    let manifest_hash = sha256_hash(b"manifest");
    let manifest = ManifestRow {
        domain_id,
        manifest_hash: manifest_hash.clone(),
        chunk_size: 256,
        chunk_count: 1,
        nar_size: 1024,
        object_key: None,
        refcount: 1,
        created_at: OffsetDateTime::now_utc(),
    };

    let chunks = vec![ManifestChunkRow {
        domain_id,
        manifest_hash: manifest_hash.clone(),
        position: 0,
        chunk_hash,
    }];

    server
        .metadata()
        .create_manifest(&manifest, &chunks)
        .await
        .expect("Failed to create manifest");

    // Create a store path
    let store_path = StorePathRow {
        store_path_hash: sha256_hash(b"/nix/store/test"),
        cache_id: Some(cache_id),
        domain_id,
        store_path: "/nix/store/test".to_string(),
        nar_hash: sha256_hash(b"nar_content"),
        nar_size: 1024,
        manifest_hash,
        created_at: OffsetDateTime::now_utc(),
        committed_at: Some(OffsetDateTime::now_utc()),
        visibility_state: "public".to_string(),
        uploader_token_id: None,
        ca: None,
        chunks_verified: true,
    };

    server
        .metadata()
        .create_store_path(&store_path)
        .await
        .expect("Failed to create store path");

    // Try to delete - should fail
    let (status, response) = json_request(
        &server.router,
        "DELETE",
        &format!("/v1/admin/caches/{}", cache_id_str),
        None,
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert!(
        response
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .contains("store path")
    );
}

#[tokio::test]
async fn test_delete_cache_with_expired_tokens() {
    let server = TestServer::new().await;
    let token = create_test_token(&server, r#"["cache:admin"]"#).await;

    // Create cache
    let body = json!({
        "cache_name": "delete-with-expired",
        "is_public": false
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(body),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let cache_id_str = response.get("cache_id").unwrap().as_str().unwrap();
    let cache_id = Uuid::parse_str(cache_id_str).unwrap();

    // Create an expired token
    use cellar_metadata::models::TokenRow;
    use common::fixtures::sha256_hash;
    use time::{Duration, OffsetDateTime};

    let expired_token = TokenRow {
        token_id: Uuid::new_v4(),
        cache_id: Some(cache_id),
        token_hash: sha256_hash(b"expired-token"),
        scopes: r#"["cache:write"]"#.to_string(),
        expires_at: Some(OffsetDateTime::now_utc() - Duration::hours(1)), // Expired 1 hour ago
        revoked_at: None,
        created_at: OffsetDateTime::now_utc(),
        last_used_at: None,
        description: Some("Expired token".to_string()),
    };

    server
        .metadata()
        .create_token(&expired_token)
        .await
        .expect("Failed to create expired token");

    // Try to delete - should succeed because expired tokens don't block deletion
    let (status, _) = json_request(
        &server.router,
        "DELETE",
        &format!("/v1/admin/caches/{}", cache_id_str),
        None,
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn test_delete_cache_requires_global_admin() {
    let server = TestServer::new().await;
    let admin_token = create_test_token(&server, r#"["cache:admin"]"#).await;

    // Create cache
    let body = json!({
        "cache_name": "delete-admin-test",
        "is_public": false
    });

    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(body),
        Some(&admin_token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let cache_id_str = response.get("cache_id").unwrap().as_str().unwrap();
    let cache_id = Uuid::parse_str(cache_id_str).unwrap();

    // Create a cache-scoped token
    use cellar_metadata::models::TokenRow;
    use common::fixtures::sha256_hash;
    use time::OffsetDateTime;

    let cache_scoped_token_id = Uuid::new_v4();
    let cache_scoped_token_raw = format!("cache-scoped-{}", cache_scoped_token_id);
    let cache_scoped_token = TokenRow {
        token_id: cache_scoped_token_id,
        cache_id: Some(cache_id),
        token_hash: sha256_hash(cache_scoped_token_raw.as_bytes()),
        scopes: r#"["cache:admin"]"#.to_string(),
        expires_at: None,
        revoked_at: None,
        created_at: OffsetDateTime::now_utc(),
        last_used_at: None,
        description: Some("Cache-scoped token".to_string()),
    };

    server
        .metadata()
        .create_token(&cache_scoped_token)
        .await
        .expect("Failed to create cache-scoped token");

    // Try to delete with cache-scoped token - should fail
    let (status, _) = json_request(
        &server.router,
        "DELETE",
        &format!("/v1/admin/caches/{}", cache_id_str),
        None,
        Some(&cache_scoped_token_raw),
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_create_default_cache() {
    let server = TestServer::new().await;
    let admin_token = create_test_token(&server, r#"["cache:admin"]"#).await;

    // TestServer already creates a default public cache, so attempting to create
    // another one should return a conflict.
    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(json!({
            "cache_name": "default-test-cache",
            "is_public": true,
            "is_default": true
        })),
        Some(&admin_token),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert!(
        response["message"]
            .as_str()
            .unwrap_or("")
            .contains("default")
    );
}

#[tokio::test]
async fn test_update_cache_to_default() {
    let server = TestServer::new().await;
    let admin_token = create_test_token(&server, r#"["cache:admin"]"#).await;

    // Create a public cache without is_default
    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(json!({
            "cache_name": "update-default-test",
            "is_public": true
        })),
        Some(&admin_token),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let cache_id = response["cache_id"].as_str().unwrap();

    // Update to set is_default=true should conflict because TestServer already
    // creates a default public cache.
    let (status, response) = json_request(
        &server.router,
        "PUT",
        &format!("/v1/admin/caches/{}", cache_id),
        Some(json!({
            "is_default": true
        })),
        Some(&admin_token),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert!(
        response["message"]
            .as_str()
            .unwrap_or("")
            .contains("default")
    );
}

#[tokio::test]
async fn test_default_cache_conflict() {
    let server = TestServer::new().await;
    let admin_token = create_test_token(&server, r#"["cache:admin"]"#).await;

    // TestServer already creates a default public cache.
    // Creating another default cache should conflict.
    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(json!({
            "cache_name": "another-default-cache",
            "is_public": true,
            "is_default": true
        })),
        Some(&admin_token),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert!(
        response["message"]
            .as_str()
            .unwrap_or("")
            .contains("default")
    );
}

#[tokio::test]
async fn test_update_to_default_cache_conflict() {
    let server = TestServer::new().await;
    let admin_token = create_test_token(&server, r#"["cache:admin"]"#).await;

    // TestServer already creates a default public cache.
    // Create a non-default cache.
    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(json!({
            "cache_name": "non-default-cache",
            "is_public": true
        })),
        Some(&admin_token),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let cache_id = response["cache_id"].as_str().unwrap();

    // Try to update second cache to be default - should conflict
    let (status, response) = json_request(
        &server.router,
        "PUT",
        &format!("/v1/admin/caches/{}", cache_id),
        Some(json!({
            "is_default": true
        })),
        Some(&admin_token),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert!(
        response["message"]
            .as_str()
            .unwrap_or("")
            .contains("default")
    );
}

#[tokio::test]
async fn test_default_on_private_cache_rejected() {
    let server = TestServer::new().await;
    let admin_token = create_test_token(&server, r#"["cache:admin"]"#).await;

    // Try to create a private cache with is_default=true - should fail
    let (status, response) = json_request(
        &server.router,
        "POST",
        "/v1/admin/caches",
        Some(json!({
            "cache_name": "private-default-cache",
            "is_public": false,
            "is_default": true
        })),
        Some(&admin_token),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        response["message"]
            .as_str()
            .unwrap_or("")
            .contains("public")
    );
}
