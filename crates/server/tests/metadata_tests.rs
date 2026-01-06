//! Integration tests for MetadataStore implementations.

mod common;

use cellar_metadata::models::*;
use common::fixtures::sha256_hash;
use common::TestMetadata;
use time::OffsetDateTime;
use uuid::Uuid;

/// Generate a simple hash string for testing.
fn test_hash(seed: &str) -> String {
    sha256_hash(seed.as_bytes())
}

/// Generate a store path hash (first 32 chars).
fn store_path_hash(name: &str) -> String {
    sha256_hash(name.as_bytes())[..32].to_string()
}

#[tokio::test]
async fn test_upload_session_lifecycle() {
    let metadata = TestMetadata::in_memory().await.expect("Failed to create metadata");
    let store = metadata.store();

    let upload_id = Uuid::new_v4();
    let sp_hash = store_path_hash("test");
    let store_path = format!("/nix/store/{}-test-package", &sp_hash);
    let now = OffsetDateTime::now_utc();

    let session = UploadSessionRow {
        upload_id,
        cache_id: None,
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
}

#[tokio::test]
async fn test_chunk_tracking() {
    let metadata = TestMetadata::in_memory().await.expect("Failed to create metadata");
    let store = metadata.store();

    let hash1 = test_hash("chunk1");
    let hash2 = test_hash("chunk2");
    let now = OffsetDateTime::now_utc();

    let chunk1 = ChunkRow {
        chunk_hash: hash1.clone(),
        size_bytes: 100,
        object_key: Some(format!("chunks/{}", &hash1[..4])),
        refcount: 0,
        created_at: now,
        last_accessed_at: None,
    };

    let chunk2 = ChunkRow {
        chunk_hash: hash2.clone(),
        size_bytes: 200,
        object_key: Some(format!("chunks/{}", &hash2[..4])),
        refcount: 0,
        created_at: now,
        last_accessed_at: None,
    };

    store.upsert_chunk(&chunk1).await.expect("Upsert chunk 1 failed");
    store.upsert_chunk(&chunk2).await.expect("Upsert chunk 2 failed");

    // Verify chunks exist
    assert!(store.chunk_exists(&hash1).await.expect("Check failed"));
    assert!(store.chunk_exists(&hash2).await.expect("Check failed"));
    assert!(!store.chunk_exists("nonexistent").await.expect("Check failed"));

    // Get chunk details
    let chunk = store
        .get_chunk(&hash1)
        .await
        .expect("Get chunk failed")
        .expect("Chunk not found");
    assert_eq!(chunk.chunk_hash, hash1);
    assert_eq!(chunk.size_bytes, 100);
    assert_eq!(chunk.refcount, 0);
}

#[tokio::test]
async fn test_refcount_management() {
    let metadata = TestMetadata::in_memory().await.expect("Failed to create metadata");
    let store = metadata.store();

    let hash = test_hash("refcount-test");
    let now = OffsetDateTime::now_utc();

    let chunk = ChunkRow {
        chunk_hash: hash.clone(),
        size_bytes: 100,
        object_key: None,
        refcount: 0,
        created_at: now,
        last_accessed_at: None,
    };

    store.upsert_chunk(&chunk).await.expect("Upsert failed");

    // Increment refcount
    store.increment_refcount(&hash).await.expect("Increment failed");
    store.increment_refcount(&hash).await.expect("Increment failed");

    let chunk = store
        .get_chunk(&hash)
        .await
        .expect("Get failed")
        .expect("Not found");
    assert_eq!(chunk.refcount, 2);

    // Decrement refcount
    store.decrement_refcount(&hash).await.expect("Decrement failed");

    let chunk = store
        .get_chunk(&hash)
        .await
        .expect("Get failed")
        .expect("Not found");
    assert_eq!(chunk.refcount, 1);
}

#[tokio::test]
async fn test_expected_chunks() {
    let metadata = TestMetadata::in_memory().await.expect("Failed to create metadata");
    let store = metadata.store();

    let upload_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();

    // Create session first
    let session = UploadSessionRow {
        upload_id,
        cache_id: None,
        store_path: "/nix/store/abc-test".to_string(),
        store_path_hash: "abc123".to_string(),
        nar_size: 1000,
        nar_hash: "sha256:test".to_string(),
        chunk_size: 100,
        manifest_hash: None,
        state: "open".to_string(),
        owner_token_id: None,
        created_at: now,
        updated_at: now,
        expires_at: now + time::Duration::hours(1),
        trace_id: None,
        error_code: None,
        error_detail: None,
    };

    store.create_session(&session).await.expect("Create session failed");

    // Add expected chunks
    let chunk_hashes: Vec<String> = (0..5)
        .map(|i| test_hash(&format!("chunk{}", i)))
        .collect();

    let expected_chunks: Vec<UploadExpectedChunkRow> = chunk_hashes
        .iter()
        .enumerate()
        .map(|(i, hash)| UploadExpectedChunkRow {
            upload_id,
            position: i as i32,
            chunk_hash: hash.clone(),
            size_bytes: 100,
            received_at: None,
        })
        .collect();

    store
        .add_expected_chunks(&expected_chunks)
        .await
        .expect("Add expected chunks failed");

    // Check missing chunks (all should be missing)
    let missing = store
        .get_missing_chunks(upload_id)
        .await
        .expect("Get missing failed");
    assert_eq!(missing.len(), 5);

    // Mark some as received
    store
        .mark_chunk_received(upload_id, &chunk_hashes[0], OffsetDateTime::now_utc())
        .await
        .expect("Mark received failed");
    store
        .mark_chunk_received(upload_id, &chunk_hashes[1], OffsetDateTime::now_utc())
        .await
        .expect("Mark received failed");

    // Check missing again
    let missing = store
        .get_missing_chunks(upload_id)
        .await
        .expect("Get missing failed");
    assert_eq!(missing.len(), 3);

    // Check received
    let received = store
        .get_received_chunks(upload_id)
        .await
        .expect("Get received failed");
    assert_eq!(received.len(), 2);
}

#[tokio::test]
async fn test_manifest_creation() {
    let metadata = TestMetadata::in_memory().await.expect("Failed to create metadata");
    let store = metadata.store();

    let now = OffsetDateTime::now_utc();
    let manifest_hash = test_hash("manifest");

    // Create chunks first
    let mut chunk_hashes = Vec::new();
    for i in 0..3 {
        let hash = test_hash(&format!("manifest-chunk{}", i));
        let chunk = ChunkRow {
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
        manifest_hash: manifest_hash.clone(),
        chunk_size: 100,
        chunk_count: 3,
        nar_size: 300,
        object_key: None,
        created_at: now,
    };

    let manifest_chunks: Vec<ManifestChunkRow> = chunk_hashes
        .iter()
        .enumerate()
        .map(|(i, hash)| ManifestChunkRow {
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
    assert!(store
        .manifest_exists(&manifest_hash)
        .await
        .expect("Check failed"));

    // Get manifest chunks (should be in order as hash strings)
    let chunks = store
        .get_manifest_chunks(&manifest_hash)
        .await
        .expect("Get chunks failed");
    assert_eq!(chunks.len(), 3);
    for (i, chunk_hash) in chunks.iter().enumerate() {
        assert_eq!(*chunk_hash, chunk_hashes[i]);
    }
}

#[tokio::test]
async fn test_store_path_visibility() {
    let metadata = TestMetadata::in_memory().await.expect("Failed to create metadata");
    let store = metadata.store();

    let now = OffsetDateTime::now_utc();
    let path_hash = store_path_hash("visibility-test");
    let manifest_hash = test_hash("vis-manifest");

    // Create manifest first (foreign key requirement)
    let manifest = ManifestRow {
        manifest_hash: manifest_hash.clone(),
        chunk_size: 100,
        chunk_count: 0,
        nar_size: 1000,
        object_key: None,
        created_at: now,
    };
    store.create_manifest(&manifest, &[]).await.expect("Create manifest failed");

    let store_path_row = StorePathRow {
        store_path_hash: path_hash.clone(),
        cache_id: None,
        store_path: format!("/nix/store/{}-test", &path_hash),
        nar_hash: test_hash("vis-nar"),
        nar_size: 1000,
        manifest_hash: manifest_hash.clone(),
        created_at: now,
        committed_at: Some(now),
        visibility_state: "visible".to_string(),
        uploader_token_id: None,
        ca: None,
    };

    store
        .create_store_path(&store_path_row)
        .await
        .expect("Create store path failed");

    // Should be visible by default (None cache_id for public/default cache)
    assert!(store
        .store_path_visible(None, &path_hash)
        .await
        .expect("Check failed"));

    // Hide it
    store
        .update_visibility(None, &path_hash, "hidden")
        .await
        .expect("Update visibility failed");

    assert!(!store
        .store_path_visible(None, &path_hash)
        .await
        .expect("Check failed"));

    // Make visible again
    store
        .update_visibility(None, &path_hash, "visible")
        .await
        .expect("Update visibility failed");

    assert!(store
        .store_path_visible(None, &path_hash)
        .await
        .expect("Check failed"));
}

#[tokio::test]
async fn test_token_management() {
    let metadata = TestMetadata::in_memory().await.expect("Failed to create metadata");
    let store = metadata.store();

    let token_id = Uuid::new_v4();
    let token_hash = test_hash("token");
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
        description: Some("Test Token".to_string()),
    };

    store.create_token(&token).await.expect("Create token failed");

    // Get by hash
    let retrieved = store
        .get_token_by_hash(&token_hash)
        .await
        .expect("Get by hash failed")
        .expect("Token not found");

    assert_eq!(retrieved.token_id, token_id);
    assert_eq!(retrieved.description, Some("Test Token".to_string()));
    assert!(retrieved.revoked_at.is_none());

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
}

#[tokio::test]
async fn test_gc_job_tracking() {
    let metadata = TestMetadata::in_memory().await.expect("Failed to create metadata");
    let store = metadata.store();

    let job_id = Uuid::new_v4();

    let job = GcJobRow {
        gc_job_id: job_id,
        cache_id: None,
        job_type: "chunk_gc".to_string(),
        state: "queued".to_string(),
        started_at: None,
        finished_at: None,
        stats_json: None,
    };

    store.create_gc_job(&job).await.expect("Create GC job failed");

    // Get job
    let retrieved = store
        .get_gc_job(job_id)
        .await
        .expect("Get job failed")
        .expect("Job not found");

    assert_eq!(retrieved.job_type, "chunk_gc");
    assert_eq!(retrieved.state, "queued");

    // Update state to running
    store
        .update_gc_job_state(job_id, "running", None, None)
        .await
        .expect("Update state failed");

    let retrieved = store
        .get_gc_job(job_id)
        .await
        .expect("Get job failed")
        .expect("Job not found");
    assert_eq!(retrieved.state, "running");

    // Complete the job with stats
    let stats = r#"{"items_deleted": 10, "bytes_reclaimed": 1024}"#;
    store
        .update_gc_job_state(job_id, "finished", Some(OffsetDateTime::now_utc()), Some(stats))
        .await
        .expect("Update state failed");

    let retrieved = store
        .get_gc_job(job_id)
        .await
        .expect("Get job failed")
        .expect("Job not found");
    assert_eq!(retrieved.state, "finished");
    assert!(retrieved.finished_at.is_some());
    assert!(retrieved.stats_json.is_some());
}

#[tokio::test]
async fn test_unreferenced_chunks() {
    let metadata = TestMetadata::in_memory().await.expect("Failed to create metadata");
    let store = metadata.store();

    let now = OffsetDateTime::now_utc();
    let old_time = now - time::Duration::hours(2);

    // Create some chunks - one will be referenced, one not
    let referenced_hash = test_hash("referenced");
    let unreferenced_hash = test_hash("unreferenced");

    let ref_chunk = ChunkRow {
        chunk_hash: referenced_hash.clone(),
        size_bytes: 100,
        object_key: None,
        refcount: 0,
        created_at: old_time,
        last_accessed_at: Some(old_time),
    };

    let unref_chunk = ChunkRow {
        chunk_hash: unreferenced_hash.clone(),
        size_bytes: 100,
        object_key: None,
        refcount: 0,
        created_at: old_time,
        last_accessed_at: Some(old_time),
    };

    store.upsert_chunk(&ref_chunk).await.expect("Upsert failed");
    store.upsert_chunk(&unref_chunk).await.expect("Upsert failed");

    // Increment refcount for one
    store
        .increment_refcount(&referenced_hash)
        .await
        .expect("Increment failed");

    // Get unreferenced chunks (older than 1 hour ago)
    let unreferenced = store
        .get_unreferenced_chunks(now - time::Duration::hours(1), 10)
        .await
        .expect("Get unreferenced failed");

    assert_eq!(unreferenced.len(), 1);
    assert_eq!(unreferenced[0].chunk_hash, unreferenced_hash);
}

#[tokio::test]
async fn test_chunk_stats() {
    let metadata = TestMetadata::in_memory().await.expect("Failed to create metadata");
    let store = metadata.store();

    let now = OffsetDateTime::now_utc();

    // Create some chunks
    for i in 0..5 {
        let chunk = ChunkRow {
            chunk_hash: test_hash(&format!("stats-chunk-{}", i)),
            size_bytes: 100 * (i + 1) as i64,
            object_key: None,
            refcount: 0,
            created_at: now,
            last_accessed_at: None,
        };
        store.upsert_chunk(&chunk).await.expect("Upsert failed");
    }

    // Reference some of them
    store.increment_refcount(&test_hash("stats-chunk-0")).await.expect("Inc failed");
    store.increment_refcount(&test_hash("stats-chunk-1")).await.expect("Inc failed");

    let stats = store.get_stats().await.expect("Get stats failed");

    assert_eq!(stats.count, 5);
    assert_eq!(stats.total_size, 100 + 200 + 300 + 400 + 500);
    assert_eq!(stats.referenced_count, 2);
    assert_eq!(stats.unreferenced_count, 3);
}
