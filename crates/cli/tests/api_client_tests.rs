#[path = "../src/api_client.rs"]
#[allow(dead_code)] // Some methods are used by the binary but not by tests
mod api_client;

use api_client::{
    ApiClient, CreateCacheRequest, CreateDomainRequest, CreateTokenRequest, TriggerGcRequest,
    UpdateCacheRequest, UpdateDomainRequest,
};
use httpmock::Method::{DELETE, GET, POST, PUT};
use httpmock::MockServer;
use serde_json::json;
use std::net::TcpListener;

fn can_bind_localhost() -> bool {
    TcpListener::bind("127.0.0.1:0").is_ok()
}

fn touch_domain(domain: &api_client::DomainResponse) {
    let _ = (
        &domain.domain_id,
        &domain.domain_name,
        domain.is_default,
        &domain.created_at,
        &domain.updated_at,
    );
}

fn touch_cache(cache: &api_client::CacheResponse) {
    let _ = (
        &cache.cache_id,
        &cache.cache_name,
        &cache.domain_id,
        &cache.domain_name,
        &cache.public_base_url,
        cache.is_public,
        cache.is_default,
        &cache.created_at,
        &cache.updated_at,
    );
}

fn touch_token_response(token: &api_client::CreateTokenResponse) {
    let _ = (&token.token_id, &token.token_secret, &token.expires_at);
}

fn touch_token_info(token: &api_client::TokenInfo) {
    let _ = (
        &token.token_id,
        &token.cache_id,
        &token.scopes,
        &token.expires_at,
        &token.revoked_at,
        &token.created_at,
        &token.last_used_at,
        &token.description,
    );
}

fn touch_gc_stats(stats: &api_client::GcStats) {
    let _ = (
        stats.items_processed,
        stats.items_deleted,
        stats.bytes_reclaimed,
        stats.errors,
    );
}

fn touch_gc_job(job: &api_client::GcJobResponse) {
    let _ = (
        &job.job_id,
        &job.cache_id,
        &job.job_type,
        &job.state,
        &job.started_at,
        &job.finished_at,
        &job.stats,
    );
    if let Some(stats) = job.stats.as_ref() {
        touch_gc_stats(stats);
    }
}

fn touch_metrics(stats: &api_client::MetricsResponse) {
    let _ = (
        stats.store_paths_count,
        stats.chunks_count,
        stats.chunks_total_size,
        stats.chunks_referenced,
        stats.chunks_unreferenced,
        stats.upload_sessions_created,
        stats.upload_sessions_committed,
        stats.upload_sessions_resumed,
        stats.upload_sessions_expired,
        stats.chunks_uploaded,
        stats.chunks_deduplicated,
        stats.bytes_uploaded,
        stats.bytes_deduplicated,
        stats.chunk_hash_mismatches,
    );
}

#[tokio::test]
async fn api_client_success_paths() {
    if !can_bind_localhost() {
        eprintln!("Skipping httpmock tests: cannot bind to localhost");
        return;
    }

    let server = MockServer::start();
    let token = "secret-token";
    let cache_id = "00000000-0000-0000-0000-000000000001";
    let domain_id = "00000000-0000-0000-0000-000000000099";
    let domain_name = "default";
    let token_id = "00000000-0000-0000-0000-000000000002";
    let gc_job_id = "00000000-0000-0000-0000-000000000003";
    let updated_domain_name = "default-renamed";

    let domain_response = json!({
        "domain_id": domain_id,
        "domain_name": domain_name,
        "is_default": true,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z"
    });

    let updated_domain_response = json!({
        "domain_id": domain_id,
        "domain_name": updated_domain_name,
        "is_default": false,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T01:00:00Z"
    });

    let cache_response = json!({
        "cache_id": cache_id,
        "cache_name": "default",
        "domain_id": domain_id,
        "domain_name": domain_name,
        "public_base_url": null,
        "is_public": true,
        "is_default": true,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z"
    });

    server.mock(|when, then| {
        when.method(POST)
            .path("/v1/admin/domains")
            .header("authorization", format!("Bearer {token}"));
        then.status(200).json_body(domain_response.clone());
    });

    server.mock(|when, then| {
        when.method(GET)
            .path("/v1/admin/domains")
            .header("authorization", format!("Bearer {token}"));
        then.status(200)
            .json_body(json!({ "domains": [domain_response.clone()] }));
    });

    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/v1/admin/domains/{domain_id}"))
            .header("authorization", format!("Bearer {token}"));
        then.status(200).json_body(domain_response.clone());
    });

    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/v1/admin/domains/by-name/{domain_name}"))
            .header("authorization", format!("Bearer {token}"));
        then.status(200).json_body(domain_response.clone());
    });

    server.mock(|when, then| {
        when.method(PUT)
            .path(format!("/v1/admin/domains/{domain_id}"))
            .header("authorization", format!("Bearer {token}"));
        then.status(200).json_body(updated_domain_response.clone());
    });

    server.mock(|when, then| {
        when.method(DELETE)
            .path(format!("/v1/admin/domains/{domain_id}"))
            .header("authorization", format!("Bearer {token}"));
        then.status(204);
    });

    server.mock(|when, then| {
        when.method(POST)
            .path("/v1/admin/caches")
            .header("authorization", format!("Bearer {token}"));
        then.status(200).json_body(cache_response.clone());
    });

    server.mock(|when, then| {
        when.method(GET)
            .path("/v1/admin/caches")
            .header("authorization", format!("Bearer {token}"));
        then.status(200)
            .json_body(json!({ "caches": [cache_response.clone()] }));
    });

    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/v1/admin/caches/{cache_id}"))
            .header("authorization", format!("Bearer {token}"));
        then.status(200).json_body(cache_response.clone());
    });

    server.mock(|when, then| {
        when.method(PUT)
            .path(format!("/v1/admin/caches/{cache_id}"))
            .header("authorization", format!("Bearer {token}"));
        then.status(200).json_body(cache_response.clone());
    });

    server.mock(|when, then| {
        when.method(DELETE)
            .path(format!("/v1/admin/caches/{cache_id}"))
            .header("authorization", format!("Bearer {token}"));
        then.status(204);
    });

    server.mock(|when, then| {
        when.method(POST)
            .path("/v1/admin/tokens")
            .header("authorization", format!("Bearer {token}"));
        then.status(200).json_body(json!({
            "token_id": token_id,
            "token_secret": "token-secret",
            "expires_at": null
        }));
    });

    server.mock(|when, then| {
        when.method(GET)
            .path("/v1/admin/tokens")
            .header("authorization", format!("Bearer {token}"));
        then.status(200).json_body(json!([{
            "token_id": token_id,
            "cache_id": null,
            "scopes": ["cache:admin"],
            "expires_at": null,
            "revoked_at": null,
            "created_at": "2024-01-01T00:00:00Z",
            "last_used_at": null,
            "description": null
        }]));
    });

    server.mock(|when, then| {
        when.method(DELETE)
            .path(format!("/v1/admin/tokens/{token_id}"))
            .header("authorization", format!("Bearer {token}"));
        then.status(204);
    });

    server.mock(|when, then| {
        when.method(POST)
            .path("/v1/admin/gc")
            .header("authorization", format!("Bearer {token}"));
        then.status(200).json_body(json!({ "job_id": gc_job_id }));
    });

    server.mock(|when, then| {
        when.method(GET)
            .path("/v1/admin/gc")
            .query_param("cache_id", cache_id)
            .header("authorization", format!("Bearer {token}"));
        then.status(200).json_body(json!([{
            "job_id": gc_job_id,
            "cache_id": cache_id,
            "job_type": "upload_gc",
            "state": "finished",
            "started_at": "2024-01-01T00:00:00Z",
            "finished_at": "2024-01-01T00:01:00Z",
            "stats": {
                "items_processed": 1,
                "items_deleted": 1,
                "bytes_reclaimed": 10,
                "errors": 0
            }
        }]));
    });

    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/v1/admin/gc/{gc_job_id}"))
            .header("authorization", format!("Bearer {token}"));
        then.status(200).json_body(json!({
            "job_id": gc_job_id,
            "cache_id": cache_id,
            "job_type": "upload_gc",
            "state": "finished",
            "started_at": "2024-01-01T00:00:00Z",
            "finished_at": "2024-01-01T00:01:00Z",
            "stats": null
        }));
    });

    server.mock(|when, then| {
        when.method(GET)
            .path("/v1/admin/metrics")
            .header("authorization", format!("Bearer {token}"));
        then.status(200).json_body(json!({
            "store_paths_count": 1,
            "chunks_count": 2,
            "chunks_total_size": 3,
            "chunks_referenced": 2,
            "chunks_unreferenced": 0,
            "upload_sessions_created": 1,
            "upload_sessions_committed": 1,
            "upload_sessions_resumed": 0,
            "upload_sessions_expired": 0,
            "chunks_uploaded": 2,
            "chunks_deduplicated": 0,
            "bytes_uploaded": 3,
            "bytes_deduplicated": 0,
            "chunk_hash_mismatches": 0
        }));
    });

    let client = ApiClient::new(&server.base_url(), token).unwrap();

    let created_domain = client
        .create_domain(CreateDomainRequest {
            domain_name: domain_name.to_string(),
        })
        .await
        .unwrap();
    assert_eq!(created_domain.domain_id, domain_id);
    touch_domain(&created_domain);

    let domains = client.list_domains().await.unwrap();
    assert_eq!(domains.len(), 1);
    touch_domain(&domains[0]);

    let fetched_domain = client.get_domain(domain_id).await.unwrap();
    assert_eq!(fetched_domain.domain_name, domain_name);
    touch_domain(&fetched_domain);

    let fetched_domain_by_name = client.get_domain_by_name(domain_name).await.unwrap();
    assert_eq!(fetched_domain_by_name.domain_id, domain_id);
    touch_domain(&fetched_domain_by_name);

    let updated_domain = client
        .update_domain(
            domain_id,
            UpdateDomainRequest {
                domain_name: updated_domain_name.to_string(),
            },
        )
        .await
        .unwrap();
    assert_eq!(updated_domain.domain_name, updated_domain_name);
    touch_domain(&updated_domain);

    client.delete_domain(domain_id).await.unwrap();

    let created = client
        .create_cache(CreateCacheRequest {
            cache_name: "default".to_string(),
            public_base_url: None,
            is_public: true,
            is_default: true,
            domain_id: None,
            domain_name: None,
        })
        .await
        .unwrap();
    assert_eq!(created.cache_id, cache_id);
    touch_cache(&created);

    let caches = client.list_caches().await.unwrap();
    assert_eq!(caches.len(), 1);
    touch_cache(&caches[0]);

    let fetched = client.get_cache(cache_id).await.unwrap();
    assert_eq!(fetched.cache_id, cache_id);
    touch_cache(&fetched);

    let updated_cache = client
        .update_cache(
            cache_id,
            UpdateCacheRequest {
                cache_name: Some("default".to_string()),
                public_base_url: None,
                is_public: Some(true),
                is_default: Some(true),
                domain_id: None,
                domain_name: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(updated_cache.cache_id, cache_id);
    touch_cache(&updated_cache);

    client.delete_cache(cache_id).await.unwrap();

    let created_token = client
        .create_token(CreateTokenRequest {
            scopes: vec!["cache:admin".to_string()],
            cache_id: None,
            expires_in_secs: None,
            description: None,
        })
        .await
        .unwrap();
    assert_eq!(created_token.token_id, token_id);
    touch_token_response(&created_token);

    let tokens = client.list_tokens().await.unwrap();
    assert_eq!(tokens.len(), 1);
    let listed_token = &tokens[0];
    assert_eq!(listed_token.token_id, token_id);
    touch_token_info(listed_token);

    client.revoke_token(token_id).await.unwrap();

    let gc = client
        .trigger_gc(TriggerGcRequest {
            job_type: "upload_gc".to_string(),
            cache_id: Some(cache_id.to_string()),
        })
        .await
        .unwrap();
    assert_eq!(gc.job_id, gc_job_id);

    let jobs = client.list_gc_jobs(Some(cache_id), None).await.unwrap();
    assert_eq!(jobs.len(), 1);
    let listed_job = &jobs[0];
    assert_eq!(listed_job.job_id, gc_job_id);
    touch_gc_job(listed_job);

    let job = client.get_gc_job(gc_job_id).await.unwrap();
    assert_eq!(job.job_id, gc_job_id);
    touch_gc_job(&job);

    let stats = client.get_stats().await.unwrap();
    assert_eq!(stats.store_paths_count, 1);
    touch_metrics(&stats);
}

#[tokio::test]
async fn api_client_returns_error_body_on_failure() {
    if !can_bind_localhost() {
        eprintln!("Skipping httpmock tests: cannot bind to localhost");
        return;
    }

    let server = MockServer::start();
    let token = "secret-token";

    server.mock(|when, then| {
        when.method(GET)
            .path("/v1/admin/caches/bad")
            .header("authorization", format!("Bearer {token}"));
        then.status(500).body("boom");
    });

    let client = ApiClient::new(&server.base_url(), token).unwrap();
    let err = client.get_cache("bad").await.unwrap_err();
    assert!(err.to_string().contains("API error (500"));
    assert!(err.to_string().contains("boom"));
}
