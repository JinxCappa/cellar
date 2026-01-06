//! PostgreSQL integration tests using testcontainers.
//!
//! These tests verify the PostgreSQL backend implementation works correctly.
//! They require Docker to be running. Set SKIP_POSTGRES_TESTS=1 to skip.

mod common;

use cellar_metadata::models::*;
use common::fixtures::sha256_hash;
use common::{POSTGRES_CONTAINER_START_ERR_PREFIX, PostgresTestMetadata};
use time::OffsetDateTime;
use uuid::Uuid;

/// Try to create a PostgreSQL test store, skipping if Docker is unavailable
/// or SKIP_POSTGRES_TESTS is set.
///
/// Only container-start failures (Docker unavailable) cause a skip.
/// Schema, migration, or connection errors still panic so real regressions
/// are not silently swallowed.
async fn postgres_or_skip() -> Option<PostgresTestMetadata> {
    if std::env::var("SKIP_POSTGRES_TESTS").is_ok() {
        return None;
    }
    match PostgresTestMetadata::new().await {
        Ok(metadata) => Some(metadata),
        Err(err) => {
            let msg = err.to_string();
            if msg.contains(POSTGRES_CONTAINER_START_ERR_PREFIX) {
                eprintln!("Skipping PostgreSQL test (Docker unavailable): {msg}");
                None
            } else {
                panic!("PostgreSQL test setup failed: {msg}");
            }
        }
    }
}

/// Generate a simple hash string for testing.
fn test_hash(seed: &str) -> String {
    sha256_hash(seed.as_bytes())
}

/// Generate a store path hash (first 32 chars).
fn store_path_hash(name: &str) -> String {
    sha256_hash(name.as_bytes())[..32].to_string()
}

fn default_domain_id() -> Uuid {
    cellar_core::storage_domain::default_storage_domain_id()
}

#[tokio::test]
async fn test_postgres_upload_session_lifecycle() {
    let Some(metadata) = postgres_or_skip().await else {
        return;
    };
    let store = metadata.store();

    let upload_id = Uuid::new_v4();
    let sp_hash = store_path_hash("pg-test");
    let store_path = format!("/nix/store/{}-test-package", &sp_hash);
    let now = OffsetDateTime::now_utc();
    let domain_id = default_domain_id();

    let session = UploadSessionRow {
        upload_id,
        cache_id: None,
        domain_id,
        store_path: store_path.clone(),
        store_path_hash: sp_hash.clone(),
        nar_size: 1000,
        nar_hash: format!("sha256:{}", test_hash("nar")),
        chunk_size: 64 * 1024,
        manifest_hash: None,
        state: "open".to_string(),
        owner_token_id: None,
        created_at: now,
        updated_at: now,
        expires_at: now + time::Duration::hours(1),
        trace_id: None,
        error_code: None,
        error_detail: None,
        commit_started_at: None,
        commit_progress: None,
        chunks_predeclared: false,
    };

    store
        .create_session(&session)
        .await
        .expect("Create session failed");

    // Retrieve the session
    let retrieved = store
        .get_session(upload_id)
        .await
        .expect("Get session failed")
        .expect("Session not found");

    assert_eq!(retrieved.store_path, store_path);
    assert_eq!(retrieved.nar_size, 1000);
    assert_eq!(retrieved.state, "open");

    // Update state
    store
        .update_state(upload_id, "committed", OffsetDateTime::now_utc())
        .await
        .expect("Update state failed");

    let retrieved = store
        .get_session(upload_id)
        .await
        .expect("Get session failed")
        .expect("Session not found");
    assert_eq!(retrieved.state, "committed");

    // Delete session
    store
        .delete_session(upload_id)
        .await
        .expect("Delete session failed");

    let retrieved = store
        .get_session(upload_id)
        .await
        .expect("Get session failed");
    assert!(retrieved.is_none());
}

#[tokio::test]
async fn test_postgres_chunk_operations() {
    let Some(metadata) = postgres_or_skip().await else {
        return;
    };
    let store = metadata.store();

    let hash1 = test_hash("pg-chunk1");
    let hash2 = test_hash("pg-chunk2");
    let now = OffsetDateTime::now_utc();
    let domain_id = default_domain_id();

    let chunk1 = ChunkRow {
        domain_id,
        chunk_hash: hash1.clone(),
        size_bytes: 100,
        object_key: Some(format!("domains/{}/chunks/{}", domain_id, &hash1[..4])),
        refcount: 0,
        created_at: now,
        last_accessed_at: None,
    };

    let chunk2 = ChunkRow {
        domain_id,
        chunk_hash: hash2.clone(),
        size_bytes: 200,
        object_key: Some(format!("domains/{}/chunks/{}", domain_id, &hash2[..4])),
        refcount: 0,
        created_at: now,
        last_accessed_at: None,
    };

    store
        .upsert_chunk(&chunk1)
        .await
        .expect("Upsert chunk 1 failed");
    store
        .upsert_chunk(&chunk2)
        .await
        .expect("Upsert chunk 2 failed");

    // Verify chunks exist
    assert!(
        store
            .chunk_exists(domain_id, &hash1)
            .await
            .expect("Check failed")
    );
    assert!(
        store
            .chunk_exists(domain_id, &hash2)
            .await
            .expect("Check failed")
    );
    assert!(
        !store
            .chunk_exists(domain_id, "nonexistent")
            .await
            .expect("Check failed")
    );

    // Get chunk details
    let chunk = store
        .get_chunk(domain_id, &hash1)
        .await
        .expect("Get chunk failed")
        .expect("Chunk not found");
    assert_eq!(chunk.chunk_hash, hash1);
    assert_eq!(chunk.size_bytes, 100);
    assert_eq!(chunk.refcount, 0);

    // Test refcount operations
    store
        .increment_refcount(domain_id, &hash1)
        .await
        .expect("Increment failed");
    store
        .increment_refcount(domain_id, &hash1)
        .await
        .expect("Increment failed");

    let chunk = store
        .get_chunk(domain_id, &hash1)
        .await
        .expect("Get failed")
        .expect("Not found");
    assert_eq!(chunk.refcount, 2);

    store
        .decrement_refcount(domain_id, &hash1)
        .await
        .expect("Decrement failed");
    let chunk = store
        .get_chunk(domain_id, &hash1)
        .await
        .expect("Get failed")
        .expect("Not found");
    assert_eq!(chunk.refcount, 1);

    // Test touch_chunk
    store
        .touch_chunk(domain_id, &hash1, now)
        .await
        .expect("Touch failed");
    let chunk = store
        .get_chunk(domain_id, &hash1)
        .await
        .expect("Get failed")
        .expect("Not found");
    assert!(chunk.last_accessed_at.is_some());

    // Test delete_chunk
    store
        .delete_chunk(domain_id, &hash2)
        .await
        .expect("Delete failed");
    assert!(
        !store
            .chunk_exists(domain_id, &hash2)
            .await
            .expect("Check failed")
    );
}

#[tokio::test]
async fn test_postgres_manifest_operations() {
    let Some(metadata) = postgres_or_skip().await else {
        return;
    };
    let store = metadata.store();

    let now = OffsetDateTime::now_utc();
    let manifest_hash = test_hash("pg-manifest");
    let domain_id = default_domain_id();

    // Create chunks first
    let mut chunk_hashes = Vec::new();
    for i in 0..3 {
        let hash = test_hash(&format!("pg-manifest-chunk{}", i));
        let chunk = ChunkRow {
            domain_id,
            chunk_hash: hash.clone(),
            size_bytes: 100,
            object_key: None,
            refcount: 0,
            created_at: now,
            last_accessed_at: None,
        };
        store.upsert_chunk(&chunk).await.expect("Upsert failed");
        chunk_hashes.push(hash);
    }

    // Create manifest
    let manifest = ManifestRow {
        domain_id,
        manifest_hash: manifest_hash.clone(),
        chunk_size: 100,
        chunk_count: 3,
        nar_size: 300,
        object_key: None,
        refcount: 0,
        created_at: now,
    };

    let manifest_chunks: Vec<ManifestChunkRow> = chunk_hashes
        .iter()
        .enumerate()
        .map(|(i, hash)| ManifestChunkRow {
            domain_id,
            manifest_hash: manifest_hash.clone(),
            position: i as i32,
            chunk_hash: hash.clone(),
        })
        .collect();

    store
        .create_manifest(&manifest, &manifest_chunks)
        .await
        .expect("Create manifest failed");

    // Verify manifest exists
    assert!(
        store
            .manifest_exists(domain_id, &manifest_hash)
            .await
            .expect("Check failed")
    );

    // Get manifest
    let retrieved = store
        .get_manifest(domain_id, &manifest_hash)
        .await
        .expect("Get manifest failed")
        .expect("Manifest not found");
    assert_eq!(retrieved.chunk_count, 3);
    assert_eq!(retrieved.nar_size, 300);

    // Get manifest chunks (should be in order)
    let chunks = store
        .get_manifest_chunks(domain_id, &manifest_hash)
        .await
        .expect("Get chunks failed");
    assert_eq!(chunks.len(), 3);
    for (i, chunk_hash) in chunks.iter().enumerate() {
        assert_eq!(*chunk_hash, chunk_hashes[i]);
    }

    // Test get_manifests_using_chunk
    let manifests = store
        .get_manifests_using_chunk(domain_id, &chunk_hashes[0])
        .await
        .expect("Get manifests using chunk failed");
    assert_eq!(manifests.len(), 1);
    assert_eq!(manifests[0], manifest_hash);

    // Test delete_manifest
    store
        .delete_manifest(domain_id, &manifest_hash)
        .await
        .expect("Delete manifest failed");
    assert!(
        !store
            .manifest_exists(domain_id, &manifest_hash)
            .await
            .expect("Check failed")
    );
}

#[tokio::test]
async fn test_postgres_store_path_operations() {
    let Some(metadata) = postgres_or_skip().await else {
        return;
    };
    let store = metadata.store();

    let now = OffsetDateTime::now_utc();
    let path_hash = store_path_hash("pg-visibility-test");
    let manifest_hash = test_hash("pg-vis-manifest");
    let domain_id = default_domain_id();

    // Create manifest first (foreign key requirement)
    let manifest = ManifestRow {
        domain_id,
        manifest_hash: manifest_hash.clone(),
        chunk_size: 100,
        chunk_count: 0,
        nar_size: 1000,
        object_key: None,
        refcount: 1,
        created_at: now,
    };
    store
        .create_manifest(&manifest, &[])
        .await
        .expect("Create manifest failed");

    let store_path_row = StorePathRow {
        store_path_hash: path_hash.clone(),
        cache_id: None,
        domain_id,
        store_path: format!("/nix/store/{}-test", &path_hash),
        nar_hash: test_hash("pg-vis-nar"),
        nar_size: 1000,
        manifest_hash: manifest_hash.clone(),
        created_at: now,
        committed_at: Some(now),
        visibility_state: "visible".to_string(),
        uploader_token_id: None,
        ca: None,
        chunks_verified: true,
    };

    store
        .create_store_path(&store_path_row)
        .await
        .expect("Create store path failed");

    // Get store path (None cache_id for public/default cache)
    let retrieved = store
        .get_store_path(None, &path_hash)
        .await
        .expect("Get store path failed")
        .expect("Store path not found");
    assert_eq!(retrieved.nar_size, 1000);

    // Should be visible by default
    assert!(
        store
            .store_path_visible(None, &path_hash)
            .await
            .expect("Check failed")
    );

    // Hide it
    store
        .update_visibility(None, &path_hash, "hidden")
        .await
        .expect("Update visibility failed");
    assert!(
        !store
            .store_path_visible(None, &path_hash)
            .await
            .expect("Check failed")
    );

    // Make visible again
    store
        .update_visibility(None, &path_hash, "visible")
        .await
        .expect("Update visibility failed");
    assert!(
        store
            .store_path_visible(None, &path_hash)
            .await
            .expect("Check failed")
    );

    // Test get_store_paths_by_manifest (None for admin view of all caches)
    let paths = store
        .get_store_paths_by_manifest(None, &manifest_hash)
        .await
        .expect("Get paths by manifest failed");
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0].store_path_hash, path_hash);

    // Test count_store_paths (None = public cache, matching the test store path)
    let count = store.count_store_paths(None).await.expect("Count failed");
    assert!(count >= 1);
}

#[tokio::test]
async fn test_postgres_token_operations() {
    let Some(metadata) = postgres_or_skip().await else {
        return;
    };
    let store = metadata.store();

    let token_id = Uuid::new_v4();
    let token_hash = test_hash("pg-token");
    let now = OffsetDateTime::now_utc();

    let token = TokenRow {
        token_id,
        cache_id: None,
        token_hash: token_hash.clone(),
        scopes: r#"["cache:read","cache:write"]"#.to_string(),
        expires_at: None,
        revoked_at: None,
        created_at: now,
        last_used_at: None,
        description: Some("PostgreSQL Test Token".to_string()),
    };

    store
        .create_token(&token)
        .await
        .expect("Create token failed");

    // Get by hash
    let retrieved = store
        .get_token_by_hash(&token_hash)
        .await
        .expect("Get by hash failed")
        .expect("Token not found");
    assert_eq!(retrieved.token_id, token_id);
    assert_eq!(
        retrieved.description,
        Some("PostgreSQL Test Token".to_string())
    );

    // Get by ID
    let retrieved = store
        .get_token(token_id)
        .await
        .expect("Get by ID failed")
        .expect("Token not found");
    assert_eq!(retrieved.token_hash, token_hash);

    // Touch token
    store
        .touch_token(token_id, OffsetDateTime::now_utc())
        .await
        .expect("Touch failed");
    let retrieved = store
        .get_token(token_id)
        .await
        .expect("Get failed")
        .expect("Not found");
    assert!(retrieved.last_used_at.is_some());

    // Revoke token
    store
        .revoke_token(token_id, OffsetDateTime::now_utc())
        .await
        .expect("Revoke failed");
    let retrieved = store
        .get_token(token_id)
        .await
        .expect("Get failed")
        .expect("Not found");
    assert!(retrieved.revoked_at.is_some());

    // List tokens
    let tokens = store.list_tokens(None).await.expect("List tokens failed");
    assert!(!tokens.is_empty());

    // Delete token
    store.delete_token(token_id).await.expect("Delete failed");
    let retrieved = store.get_token(token_id).await.expect("Get failed");
    assert!(retrieved.is_none());
}

#[tokio::test]
async fn test_postgres_gc_job_operations() {
    let Some(metadata) = postgres_or_skip().await else {
        return;
    };
    let store = metadata.store();

    let job_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();

    let job = GcJobRow {
        gc_job_id: job_id,
        cache_id: None,
        job_type: "chunk_gc".to_string(),
        state: "queued".to_string(),
        started_at: None,
        finished_at: None,
        stats_json: None,
        sweep_checkpoint_json: None,
    };

    store
        .create_gc_job(&job)
        .await
        .expect("Create GC job failed");

    // Get job
    let retrieved = store
        .get_gc_job(job_id)
        .await
        .expect("Get job failed")
        .expect("Job not found");
    assert_eq!(retrieved.job_type, "chunk_gc");
    assert_eq!(retrieved.state, "queued");

    // Update to running
    store
        .update_gc_job_state(job_id, "running", Some(now), None, None)
        .await
        .expect("Update state failed");

    // Check running jobs (None = public cache, matching the job's cache_id)
    let running = store
        .get_running_gc_jobs(None)
        .await
        .expect("Get running failed");
    assert!(!running.is_empty());

    // Complete the job
    let stats = r#"{"items_deleted": 10, "bytes_reclaimed": 1024}"#;
    store
        .update_gc_job_state(job_id, "finished", Some(now), Some(stats), None)
        .await
        .expect("Update state failed");

    let retrieved = store
        .get_gc_job(job_id)
        .await
        .expect("Get failed")
        .expect("Not found");
    assert_eq!(retrieved.state, "finished");
    assert!(retrieved.stats_json.is_some());

    // Check recent jobs (None = public cache, matching the job's cache_id)
    let recent = store
        .get_recent_gc_jobs(None, 10)
        .await
        .expect("Get recent failed");
    assert!(!recent.is_empty());
}

#[tokio::test]
async fn test_postgres_chunk_stats() {
    let Some(metadata) = postgres_or_skip().await else {
        return;
    };
    let store = metadata.store();

    let now = OffsetDateTime::now_utc();
    let domain_id = default_domain_id();

    // Create some chunks
    for i in 0..5 {
        let chunk = ChunkRow {
            domain_id,
            chunk_hash: test_hash(&format!("pg-stats-chunk-{}", i)),
            size_bytes: 100 * (i + 1) as i64,
            object_key: None,
            refcount: 0,
            created_at: now,
            last_accessed_at: None,
        };
        store.upsert_chunk(&chunk).await.expect("Upsert failed");
    }

    // Reference some of them
    store
        .increment_refcount(domain_id, &test_hash("pg-stats-chunk-0"))
        .await
        .expect("Inc failed");
    store
        .increment_refcount(domain_id, &test_hash("pg-stats-chunk-1"))
        .await
        .expect("Inc failed");

    let stats = store.get_stats(domain_id).await.expect("Get stats failed");

    assert_eq!(stats.count, 5);
    assert_eq!(stats.total_size, 100 + 200 + 300 + 400 + 500);
    assert_eq!(stats.referenced_count, 2);
    assert_eq!(stats.unreferenced_count, 3);
}

#[tokio::test]
async fn test_postgres_signing_key_operations() {
    let Some(metadata) = postgres_or_skip().await else {
        return;
    };
    let store = metadata.store();

    let key_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();

    let key = SigningKeyRow {
        key_id,
        cache_id: None,
        key_name: "test-key".to_string(),
        public_key: "ed25519:testpubkey".to_string(),
        private_key_ref: "file:/path/to/key".to_string(),
        status: "active".to_string(),
        created_at: now,
        rotated_at: None,
    };

    store
        .create_signing_key(&key)
        .await
        .expect("Create signing key failed");

    // Get by ID
    let retrieved = store
        .get_signing_key(key_id)
        .await
        .expect("Get signing key failed")
        .expect("Signing key not found");
    assert_eq!(retrieved.key_name, "test-key");
    assert_eq!(retrieved.status, "active");

    // Get by name
    let retrieved = store
        .get_signing_key_by_name("test-key")
        .await
        .expect("Get by name failed")
        .expect("Signing key not found");
    assert_eq!(retrieved.key_id, key_id);

    // Get active signing key
    let active = store
        .get_active_signing_key(None)
        .await
        .expect("Get active failed")
        .expect("No active key");
    assert_eq!(active.key_id, key_id);

    // Update status
    store
        .update_signing_key_status(key_id, "rotated")
        .await
        .expect("Update status failed");
    let retrieved = store
        .get_signing_key(key_id)
        .await
        .expect("Get failed")
        .expect("Not found");
    assert_eq!(retrieved.status, "rotated");

    // List signing keys
    let keys = store.list_signing_keys(None).await.expect("List failed");
    assert!(!keys.is_empty());
}
