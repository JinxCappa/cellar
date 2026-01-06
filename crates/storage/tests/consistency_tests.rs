// Consistency tests for concurrent mutations during listing
// Ensures that listings handle concurrent creates/deletes gracefully

use bytes::Bytes;
use cellar_storage::traits::{ListingOptions, ObjectStore};
use futures::StreamExt;
use std::collections::HashSet;
use tempfile::TempDir;
use tokio::time::Duration;

#[tokio::test]
async fn test_concurrent_create_during_listing() {
    // Test creating objects while listing
    let temp_dir = TempDir::new().unwrap();
    let backend =
        cellar_storage::backends::filesystem::FilesystemBackend::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

    // Create initial 100 objects
    for i in 0..100 {
        backend
            .put(&format!("test/{}", i), Bytes::from(vec![i as u8]))
            .await
            .unwrap();
    }

    // Clone backend for concurrent access
    let backend_clone = std::sync::Arc::new(backend);
    let backend_for_list = backend_clone.clone();
    let backend_for_create = backend_clone.clone();

    // Spawn listing task
    let list_handle = tokio::spawn(async move {
        let mut stream = backend_for_list.list_pages("test/", ListingOptions::new(50), None);
        let mut keys = Vec::new();
        while let Some(page) = stream.next().await {
            keys.extend(page.unwrap().keys);
            // Small delay between pages to allow interleaving
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        keys
    });

    // Spawn concurrent creation task
    let create_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        for i in 100..150 {
            backend_for_create
                .put(&format!("test/{}", i), Bytes::from(vec![i as u8]))
                .await
                .unwrap();
        }
    });

    let (keys, create_result) = tokio::join!(list_handle, create_handle);
    let keys = keys.unwrap();
    create_result.unwrap(); // Ensure spawned task didn't panic or error

    // Should get at least the original 100, may or may not see the new 50
    assert!(
        keys.len() >= 100,
        "Should see at least original objects, got {}",
        keys.len()
    );
    assert!(
        keys.len() <= 150,
        "Should not see more than total objects, got {}",
        keys.len()
    );

    println!(
        "Listed {} keys during concurrent creation (expected 100-150)",
        keys.len()
    );
}

#[tokio::test]
async fn test_concurrent_delete_during_listing() {
    // Test deleting objects while listing
    let temp_dir = TempDir::new().unwrap();
    let backend =
        cellar_storage::backends::filesystem::FilesystemBackend::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

    // Create 200 objects
    for i in 0..200 {
        backend
            .put(&format!("test/{}", i), Bytes::from(vec![i as u8]))
            .await
            .unwrap();
    }

    let backend_clone = std::sync::Arc::new(backend);
    let backend_for_list = backend_clone.clone();
    let backend_for_delete = backend_clone.clone();

    let list_handle = tokio::spawn(async move {
        let mut stream = backend_for_list.list_pages("test/", ListingOptions::new(50), None);
        let mut keys = Vec::new();
        while let Some(page) = stream.next().await {
            keys.extend(page.unwrap().keys);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        keys
    });

    // Delete objects during listing
    let delete_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        for i in 50..150 {
            let _ = backend_for_delete.delete(&format!("test/{}", i)).await;
        }
    });

    let (keys, delete_result) = tokio::join!(list_handle, delete_handle);
    let keys = keys.unwrap();
    delete_result.unwrap(); // Ensure spawned task didn't panic or error

    // May see between 100-200 keys depending on timing
    assert!(
        keys.len() >= 100,
        "Should see at least undeleted objects, got {}",
        keys.len()
    );
    assert!(
        keys.len() <= 200,
        "Should not exceed total created, got {}",
        keys.len()
    );

    println!(
        "Listed {} keys during concurrent deletion (expected 100-200)",
        keys.len()
    );
}

#[tokio::test]
async fn test_missing_object_during_get() {
    // Test that getting a missing object (deleted mid-operation) fails gracefully
    let temp_dir = TempDir::new().unwrap();
    let backend =
        cellar_storage::backends::filesystem::FilesystemBackend::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

    // Create and immediately delete an object
    backend
        .put("test/ephemeral", Bytes::from(&b"temporary"[..]))
        .await
        .unwrap();

    backend.delete("test/ephemeral").await.unwrap();

    // Try to get the deleted object
    let result = backend.get("test/ephemeral").await;

    assert!(result.is_err(), "Getting deleted object should fail");

    if let Err(e) = result {
        assert!(
            matches!(e, cellar_storage::error::StorageError::NotFound(_)),
            "Should be NotFound error, got: {:?}",
            e
        );
        println!("Correctly returned NotFound error: {}", e);
    }
}

#[tokio::test]
async fn test_duplicate_keys_not_returned() {
    // Verify that concurrent operations don't cause duplicate keys in listing
    let temp_dir = TempDir::new().unwrap();
    let backend =
        cellar_storage::backends::filesystem::FilesystemBackend::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

    // Create initial objects
    for i in 0..100 {
        backend
            .put(&format!("test/{:03}", i), Bytes::from(vec![i as u8]))
            .await
            .unwrap();
    }

    let backend = std::sync::Arc::new(backend);
    let backend_for_list = backend.clone();
    let backend_for_modify = backend.clone();

    // Spawn listing task
    let list_handle = tokio::spawn(async move {
        let mut stream = backend_for_list.list_pages("test/", ListingOptions::new(25), None);
        let mut keys = Vec::new();
        while let Some(page) = stream.next().await {
            keys.extend(page.unwrap().keys);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        keys
    });

    // Spawn modification task (update existing objects)
    let modify_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(2)).await;
        for i in 25..75 {
            // Overwrite existing objects
            backend_for_modify
                .put(
                    &format!("test/{:03}", i),
                    Bytes::from(vec![(i + 100) as u8]),
                )
                .await
                .unwrap();
        }
    });

    let (keys, modify_result) = tokio::join!(list_handle, modify_handle);
    let keys = keys.unwrap();
    modify_result.unwrap(); // Ensure spawned task didn't panic or error

    // Check for duplicates
    let mut seen = HashSet::new();
    let mut duplicates = Vec::new();

    for key in &keys {
        if !seen.insert(key.clone()) {
            duplicates.push(key.clone());
        }
    }

    assert!(
        duplicates.is_empty(),
        "Found {} duplicate keys during concurrent updates: {:?}",
        duplicates.len(),
        duplicates
    );

    println!(
        "Verified {} unique keys with no duplicates during concurrent updates",
        keys.len()
    );
}

#[tokio::test]
async fn test_listing_consistent_snapshot() {
    // Test that a listing represents a reasonable snapshot, not a constantly changing view
    let temp_dir = TempDir::new().unwrap();
    let backend =
        cellar_storage::backends::filesystem::FilesystemBackend::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

    // Create objects
    for i in 0..50 {
        backend
            .put(&format!("test/{}", i), Bytes::from(vec![i as u8]))
            .await
            .unwrap();
    }

    // List twice
    let mut stream1 = backend.list_pages("test/", ListingOptions::new(25), None);
    let mut keys1 = Vec::new();
    while let Some(page) = stream1.next().await {
        keys1.extend(page.unwrap().keys);
    }

    let mut stream2 = backend.list_pages("test/", ListingOptions::new(25), None);
    let mut keys2 = Vec::new();
    while let Some(page) = stream2.next().await {
        keys2.extend(page.unwrap().keys);
    }

    // Both listings should return the same keys (since no modifications occurred)
    keys1.sort();
    keys2.sort();

    assert_eq!(
        keys1, keys2,
        "Two consecutive listings should return same results"
    );

    println!("Both listings returned {} keys consistently", keys1.len());
}
