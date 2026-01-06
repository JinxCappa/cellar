//! Integration tests for MetadataStore implementations.

mod common;

use cellar_metadata::models::*;
use common::fixtures::sha256_hash;
use common::{TestMetadata, run_metadata_test_both};
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

fn default_domain_id() -> Uuid {
    cellar_core::storage_domain::default_storage_domain_id()
}

#[tokio::test]
async fn test_upload_session_lifecycle() {
    let metadata = TestMetadata::in_memory()
        .await
        .expect("Failed to create metadata");
    let store = metadata.store();

    let upload_id = Uuid::new_v4();
    let sp_hash = store_path_hash("test");
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
}

#[tokio::test]
async fn test_chunk_tracking() {
    let metadata = TestMetadata::in_memory()
        .await
        .expect("Failed to create metadata");
    let store = metadata.store();

    let hash1 = test_hash("chunk1");
    let hash2 = test_hash("chunk2");
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
}

#[tokio::test]
async fn test_refcount_management() {
    let metadata = TestMetadata::in_memory()
        .await
        .expect("Failed to create metadata");
    let store = metadata.store();

    let hash = test_hash("refcount-test");
    let now = OffsetDateTime::now_utc();
    let domain_id = default_domain_id();

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

    // Increment refcount
    store
        .increment_refcount(domain_id, &hash)
        .await
        .expect("Increment failed");
    store
        .increment_refcount(domain_id, &hash)
        .await
        .expect("Increment failed");

    let chunk = store
        .get_chunk(domain_id, &hash)
        .await
        .expect("Get failed")
        .expect("Not found");
    assert_eq!(chunk.refcount, 2);

    // Decrement refcount
    store
        .decrement_refcount(domain_id, &hash)
        .await
        .expect("Decrement failed");

    let chunk = store
        .get_chunk(domain_id, &hash)
        .await
        .expect("Get failed")
        .expect("Not found");
    assert_eq!(chunk.refcount, 1);
}

#[tokio::test]
async fn test_expected_chunks() {
    let metadata = TestMetadata::in_memory()
        .await
        .expect("Failed to create metadata");
    let store = metadata.store();

    let upload_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();
    let domain_id = default_domain_id();

    // Create session first
    let session = UploadSessionRow {
        upload_id,
        cache_id: None,
        domain_id,
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
        commit_started_at: None,
        commit_progress: None,
        chunks_predeclared: true,
    };

    store
        .create_session(&session)
        .await
        .expect("Create session failed");

    // Add expected chunks
    let chunk_hashes: Vec<String> = (0..5).map(|i| test_hash(&format!("chunk{}", i))).collect();

    let expected_chunks: Vec<UploadExpectedChunkRow> = chunk_hashes
        .iter()
        .enumerate()
        .map(|(i, hash)| UploadExpectedChunkRow {
            upload_id,
            position: i as i32,
            chunk_hash: hash.clone(),
            size_bytes: 100,
            received_at: None,
            uploaded_data: false,
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

    // Mark some as received (uploaded_data=true since we're simulating actual uploads)
    // chunks_predeclared=false matches the session state
    store
        .mark_chunk_received(
            upload_id,
            &chunk_hashes[0],
            OffsetDateTime::now_utc(),
            true,
            false,
        )
        .await
        .expect("Mark received failed");
    store
        .mark_chunk_received(
            upload_id,
            &chunk_hashes[1],
            OffsetDateTime::now_utc(),
            true,
            false,
        )
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
    let metadata = TestMetadata::in_memory()
        .await
        .expect("Failed to create metadata");
    let store = metadata.store();

    let now = OffsetDateTime::now_utc();
    let manifest_hash = test_hash("manifest");
    let domain_id = default_domain_id();

    // Create chunks first
    let mut chunk_hashes = Vec::new();
    for i in 0..3 {
        let hash = test_hash(&format!("manifest-chunk{}", i));
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

    // Get manifest chunks (should be in order as hash strings)
    let chunks = store
        .get_manifest_chunks(domain_id, &manifest_hash)
        .await
        .expect("Get chunks failed");
    assert_eq!(chunks.len(), 3);
    for (i, chunk_hash) in chunks.iter().enumerate() {
        assert_eq!(*chunk_hash, chunk_hashes[i]);
    }
}

#[tokio::test]
async fn test_store_path_visibility() {
    let metadata = TestMetadata::in_memory()
        .await
        .expect("Failed to create metadata");
    let store = metadata.store();

    let now = OffsetDateTime::now_utc();
    let path_hash = store_path_hash("visibility-test");
    let manifest_hash = test_hash("vis-manifest");
    let domain_id = default_domain_id();

    // Create manifest first (foreign key requirement)
    let manifest = ManifestRow {
        domain_id,
        manifest_hash: manifest_hash.clone(),
        chunk_size: 100,
        chunk_count: 0,
        nar_size: 1000,
        object_key: None,
        refcount: 0,
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
        nar_hash: test_hash("vis-nar"),
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

    // Should be visible by default (None cache_id for public/default cache)
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
}

#[tokio::test]
async fn test_token_management() {
    let metadata = TestMetadata::in_memory()
        .await
        .expect("Failed to create metadata");
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
    let metadata = TestMetadata::in_memory()
        .await
        .expect("Failed to create metadata");
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

    // Update state to running
    store
        .update_gc_job_state(job_id, "running", None, None, None)
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
        .update_gc_job_state(
            job_id,
            "finished",
            Some(OffsetDateTime::now_utc()),
            Some(stats),
            None,
        )
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
    let metadata = TestMetadata::in_memory()
        .await
        .expect("Failed to create metadata");
    let store = metadata.store();

    let now = OffsetDateTime::now_utc();
    let old_time = now - time::Duration::hours(2);
    let domain_id = default_domain_id();

    // Create some chunks - one will be referenced, one not
    let referenced_hash = test_hash("referenced");
    let unreferenced_hash = test_hash("unreferenced");

    let ref_chunk = ChunkRow {
        domain_id,
        chunk_hash: referenced_hash.clone(),
        size_bytes: 100,
        object_key: None,
        refcount: 0,
        created_at: old_time,
        last_accessed_at: Some(old_time),
    };

    let unref_chunk = ChunkRow {
        domain_id,
        chunk_hash: unreferenced_hash.clone(),
        size_bytes: 100,
        object_key: None,
        refcount: 0,
        created_at: old_time,
        last_accessed_at: Some(old_time),
    };

    store.upsert_chunk(&ref_chunk).await.expect("Upsert failed");
    store
        .upsert_chunk(&unref_chunk)
        .await
        .expect("Upsert failed");

    // Increment refcount for one
    store
        .increment_refcount(domain_id, &referenced_hash)
        .await
        .expect("Increment failed");

    // Get unreferenced chunks (older than 1 hour ago)
    let unreferenced = store
        .get_unreferenced_chunks(domain_id, now - time::Duration::hours(1), 10)
        .await
        .expect("Get unreferenced failed");

    assert_eq!(unreferenced.len(), 1);
    assert_eq!(unreferenced[0].chunk_hash, unreferenced_hash);
}

#[tokio::test]
async fn test_chunk_stats() {
    let metadata = TestMetadata::in_memory()
        .await
        .expect("Failed to create metadata");
    let store = metadata.store();

    let now = OffsetDateTime::now_utc();
    let domain_id = default_domain_id();

    // Create some chunks
    for i in 0..5 {
        let chunk = ChunkRow {
            domain_id,
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
    store
        .increment_refcount(domain_id, &test_hash("stats-chunk-0"))
        .await
        .expect("Inc failed");
    store
        .increment_refcount(domain_id, &test_hash("stats-chunk-1"))
        .await
        .expect("Inc failed");

    let stats = store.get_stats(domain_id).await.expect("Get stats failed");

    assert_eq!(stats.count, 5);
    assert_eq!(stats.total_size, 100 + 200 + 300 + 400 + 500);
    assert_eq!(stats.referenced_count, 2);
    assert_eq!(stats.unreferenced_count, 3);
}

#[tokio::test]
async fn test_gc_job_checkpoint_roundtrip() {
    let metadata = TestMetadata::in_memory()
        .await
        .expect("Failed to create metadata");
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

    let checkpoint = r#"{"version":1,"chunks_token":"tok","chunks_processed":3,"manifests_token":null,"manifests_processed":1,"last_updated":"ts"}"#;

    store
        .update_gc_job_state(job_id, "running", Some(now), None, Some(checkpoint))
        .await
        .expect("Update with checkpoint failed");

    let retrieved = store
        .get_gc_job(job_id)
        .await
        .expect("Get job failed")
        .expect("Job not found");
    assert_eq!(retrieved.sweep_checkpoint_json.as_deref(), Some(checkpoint));

    store
        .update_gc_job_state(job_id, "finished", Some(now), None, None)
        .await
        .expect("Clearing checkpoint failed");

    let cleared = store
        .get_gc_job(job_id)
        .await
        .expect("Get job failed")
        .expect("Job not found");
    assert!(cleared.sweep_checkpoint_json.is_none());
}

#[tokio::test]
async fn test_manifest_refcount_on_reupload() {
    // REGRESSION TEST: Verify that re-uploading the same manifest increments refcount.
    // Previously, create_manifest_with_initial_refcount only inserted refcount for
    // new manifests, causing re-uploads to not bump refcounts. This led to premature
    // GC deletion when the original refcount dropped to 0.
    let metadata = TestMetadata::in_memory()
        .await
        .expect("Failed to create metadata");
    let store = metadata.store();

    let now = OffsetDateTime::now_utc();
    let cache_id = Uuid::new_v4();
    let manifest_hash = test_hash("reupload-test");
    let domain_id = default_domain_id();

    // Create a cache first
    let cache = cellar_metadata::models::CacheRow {
        cache_id,
        domain_id,
        cache_name: "test-cache".to_string(),
        public_base_url: None,
        is_public: false,
        is_default: false,
        created_at: now,
        updated_at: now,
    };
    store
        .create_cache(&cache)
        .await
        .expect("Create cache failed");

    let manifest = ManifestRow {
        domain_id,
        manifest_hash: manifest_hash.clone(),
        chunk_size: 100,
        chunk_count: 0,
        nar_size: 1000,
        object_key: None,
        refcount: 0,
        created_at: now,
    };

    // First upload: create manifest with initial refcount
    let inserted = store
        .create_manifest_with_initial_refcount(&manifest, &[])
        .await
        .expect("First upload failed");
    assert!(inserted, "First upload should insert the manifest");

    // Verify refcount is 1 using decrement and check it doesn't go below 0
    store
        .decrement_manifest_refcount(domain_id, &manifest_hash)
        .await
        .expect("Decrement failed");

    // Second upload (re-upload): same manifest, should increment refcount
    let inserted = store
        .create_manifest_with_initial_refcount(&manifest, &[])
        .await
        .expect("Second upload failed");
    assert!(!inserted, "Re-upload should not re-insert the manifest");

    // CRITICAL: Verify refcount is now 1 (not 0!)
    // If the bug exists, refcount would still be 0 and the manifest would be GC'd
    store
        .decrement_manifest_refcount(domain_id, &manifest_hash)
        .await
        .expect("Decrement failed");

    // Third upload should bring it back to 1
    store
        .create_manifest_with_initial_refcount(&manifest, &[])
        .await
        .expect("Third upload failed");

    // Manifest should still exist after two decrements and one increment
    assert!(
        store
            .manifest_exists(domain_id, &manifest_hash)
            .await
            .expect("Check exists failed"),
        "Manifest should still exist after re-upload increments refcount"
    );
}

#[tokio::test]
async fn test_manifest_refcount_public_cache() {
    // REGRESSION TEST: Verify that manifests in the default storage domain create refcount entries.
    // Previously, refcount insertion could be skipped in some paths, leading to premature GC.
    let metadata = TestMetadata::in_memory()
        .await
        .expect("Failed to create metadata");
    let store = metadata.store();

    let now = OffsetDateTime::now_utc();
    let manifest_hash = test_hash("public-cache-test");
    let domain_id = default_domain_id();

    let manifest = ManifestRow {
        domain_id,
        manifest_hash: manifest_hash.clone(),
        chunk_size: 100,
        chunk_count: 0,
        nar_size: 1000,
        object_key: None,
        refcount: 0,
        created_at: now,
    };

    // Upload in the default storage domain
    let inserted = store
        .create_manifest_with_initial_refcount(&manifest, &[])
        .await
        .expect("Public cache upload failed");
    assert!(inserted, "First upload should insert the manifest");

    // Verify refcount entry exists by decrementing
    store
        .decrement_manifest_refcount(domain_id, &manifest_hash)
        .await
        .expect("Decrement failed");

    // Re-upload in the default storage domain
    let inserted = store
        .create_manifest_with_initial_refcount(&manifest, &[])
        .await
        .expect("Public cache re-upload failed");
    assert!(!inserted, "Re-upload should not re-insert the manifest");

    // Verify refcount is incremented by checking manifest still exists after decrement
    store
        .decrement_manifest_refcount(domain_id, &manifest_hash)
        .await
        .expect("Decrement failed");

    // Third upload should bring refcount back to 1
    store
        .create_manifest_with_initial_refcount(&manifest, &[])
        .await
        .expect("Third upload failed");

    // Manifest should still exist after re-uploads incremented refcount
    assert!(
        store
            .manifest_exists(domain_id, &manifest_hash)
            .await
            .expect("Check exists failed"),
        "Manifest should still exist in public cache after re-upload"
    );
}

#[tokio::test]
async fn test_default_public_cache_lookup() {
    let metadata = TestMetadata::in_memory()
        .await
        .expect("Failed to create metadata");
    let store = metadata.store();
    let now = OffsetDateTime::now_utc();
    let domain_id = default_domain_id();

    // Initially, no default cache should exist
    let default = store
        .get_default_public_cache()
        .await
        .expect("Query failed");
    assert!(default.is_none(), "No default cache should exist initially");

    // Create a non-default public cache
    let cache1 = CacheRow {
        cache_id: Uuid::new_v4(),
        domain_id,
        cache_name: "non-default-cache".to_string(),
        public_base_url: None,
        is_public: true,
        is_default: false,
        created_at: now,
        updated_at: now,
    };
    store
        .create_cache(&cache1)
        .await
        .expect("Create cache 1 failed");

    // Still no default
    let default = store
        .get_default_public_cache()
        .await
        .expect("Query failed");
    assert!(
        default.is_none(),
        "Non-default cache should not be returned"
    );

    // Create a default public cache
    let cache2 = CacheRow {
        cache_id: Uuid::new_v4(),
        domain_id,
        cache_name: "default-cache".to_string(),
        public_base_url: None,
        is_public: true,
        is_default: true,
        created_at: now,
        updated_at: now,
    };
    store
        .create_cache(&cache2)
        .await
        .expect("Create cache 2 failed");

    // Now we should get the default cache
    let default = store
        .get_default_public_cache()
        .await
        .expect("Query failed");
    assert!(default.is_some(), "Default cache should be found");
    let default = default.unwrap();
    assert_eq!(default.cache_name, "default-cache");
    assert!(default.is_default);
    assert!(default.is_public);
}

#[tokio::test]
async fn test_begin_commit_session_and_progress_tracking() {
    run_metadata_test_both(|store| async move {
        let upload_id = Uuid::new_v4();
        let now = OffsetDateTime::now_utc();
        let sp_hash = store_path_hash("commit-progress");
        let store_path = format!("/nix/store/{}-commit-progress", sp_hash);
        let domain_id = default_domain_id();

        let session = UploadSessionRow {
            upload_id,
            cache_id: None,
            domain_id,
            store_path,
            store_path_hash: sp_hash,
            nar_size: 123,
            nar_hash: format!("sha256:{}", test_hash("nar")),
            chunk_size: 64,
            manifest_hash: None,
            state: "open".to_string(),
            owner_token_id: None,
            created_at: now,
            updated_at: now,
            expires_at: now + time::Duration::minutes(5),
            trace_id: None,
            error_code: None,
            error_detail: None,
            commit_started_at: None,
            commit_progress: None,
            chunks_predeclared: false,
        };

        store.create_session(&session).await.unwrap();

        let updated = store
            .begin_commit_session(upload_id, now)
            .await
            .unwrap()
            .expect("session should exist");
        assert_eq!(updated.state, "committing");

        store
            .update_commit_progress(upload_id, r#"{"phase":"hashing"}"#, now)
            .await
            .unwrap();

        let reloaded = store.get_session(upload_id).await.unwrap().unwrap();
        assert_eq!(reloaded.state, "committing");
        assert_eq!(
            reloaded.commit_progress.as_deref(),
            Some(r#"{"phase":"hashing"}"#)
        );

        let second = store
            .begin_commit_session(upload_id, now)
            .await
            .unwrap()
            .expect("session should exist");
        assert_eq!(second.state, "committing");
    })
    .await;
}

#[tokio::test]
async fn test_expired_and_stuck_sessions_queries() {
    run_metadata_test_both(|store| async move {
        let now = OffsetDateTime::now_utc();
        let expired_id = Uuid::new_v4();
        let stuck_id = Uuid::new_v4();
        let domain_id = default_domain_id();

        let expired_session = UploadSessionRow {
            upload_id: expired_id,
            cache_id: None,
            domain_id,
            store_path: "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-expired".to_string(),
            store_path_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            nar_size: 10,
            nar_hash: format!("sha256:{}", test_hash("expired")),
            chunk_size: 64,
            manifest_hash: None,
            state: "open".to_string(),
            owner_token_id: None,
            created_at: now - time::Duration::hours(2),
            updated_at: now - time::Duration::hours(2),
            expires_at: now - time::Duration::hours(1),
            trace_id: None,
            error_code: None,
            error_detail: None,
            commit_started_at: None,
            commit_progress: None,
            chunks_predeclared: false,
        };

        let stuck_session = UploadSessionRow {
            upload_id: stuck_id,
            cache_id: None,
            domain_id,
            store_path: "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-stuck".to_string(),
            store_path_hash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            nar_size: 10,
            nar_hash: format!("sha256:{}", test_hash("stuck")),
            chunk_size: 64,
            manifest_hash: None,
            state: "committing".to_string(),
            owner_token_id: None,
            created_at: now - time::Duration::hours(2),
            updated_at: now - time::Duration::hours(2),
            expires_at: now + time::Duration::hours(1),
            trace_id: None,
            error_code: None,
            error_detail: None,
            commit_started_at: Some(now - time::Duration::hours(2)),
            commit_progress: None,
            chunks_predeclared: false,
        };

        store.create_session(&expired_session).await.unwrap();
        store.create_session(&stuck_session).await.unwrap();

        let expired = store.get_expired_sessions(None, 10).await.unwrap();
        assert!(expired.iter().any(|s| s.upload_id == expired_id));

        let stuck = store
            .get_stuck_committing_sessions(None, now - time::Duration::minutes(30), 10)
            .await
            .unwrap();
        assert!(stuck.iter().any(|s| s.upload_id == stuck_id));
    })
    .await;
}
