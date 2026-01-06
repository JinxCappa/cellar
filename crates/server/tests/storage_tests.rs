//! Integration tests for ObjectStore implementations.

mod common;

use cellar_storage::StorageError;
use common::{TestStorage, fixtures};

#[tokio::test]
async fn test_put_get_roundtrip() {
    let storage = TestStorage::new().await.expect("Failed to create storage");
    let store = storage.store();

    let key = "test/object.bin";
    let data = fixtures::seeded_bytes(42, 1024);

    // Put the object
    store.put(key, data.clone()).await.expect("Put failed");

    // Verify it exists
    assert!(store.exists(key).await.expect("Exists check failed"));

    // Get the object
    let retrieved = store.get(key).await.expect("Get failed");
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_put_if_not_exists() {
    let storage = TestStorage::new().await.expect("Failed to create storage");
    let store = storage.store();

    let key = "test/conditional.bin";
    let data1 = fixtures::seeded_bytes(1, 100);
    let data2 = fixtures::seeded_bytes(2, 100);

    // First put should succeed
    let created = store
        .put_if_not_exists(key, data1.clone())
        .await
        .expect("First put_if_not_exists failed");
    assert!(created);

    // Second put should return false (already exists)
    let created = store
        .put_if_not_exists(key, data2.clone())
        .await
        .expect("Second put_if_not_exists failed");
    assert!(!created);

    // Original data should be preserved
    let retrieved = store.get(key).await.expect("Get failed");
    assert_eq!(retrieved, data1);
}

#[tokio::test]
async fn test_get_not_found() {
    let storage = TestStorage::new().await.expect("Failed to create storage");
    let store = storage.store();

    let result = store.get("nonexistent/key").await;
    assert!(matches!(result, Err(StorageError::NotFound(_))));
}

#[tokio::test]
async fn test_head_metadata() {
    let storage = TestStorage::new().await.expect("Failed to create storage");
    let store = storage.store();

    let key = "test/metadata.bin";
    let data = fixtures::seeded_bytes(42, 512);

    store.put(key, data.clone()).await.expect("Put failed");

    let meta = store.head(key).await.expect("Head failed");
    assert_eq!(meta.size, 512);
}

#[tokio::test]
async fn test_delete() {
    let storage = TestStorage::new().await.expect("Failed to create storage");
    let store = storage.store();

    let key = "test/to_delete.bin";
    let data = fixtures::seeded_bytes(42, 100);

    store.put(key, data).await.expect("Put failed");
    assert!(store.exists(key).await.expect("Exists check failed"));

    store.delete(key).await.expect("Delete failed");
    assert!(!store.exists(key).await.expect("Exists check failed"));
}

#[tokio::test]
async fn test_list_prefix() {
    let storage = TestStorage::new().await.expect("Failed to create storage");
    let store = storage.store();

    // Create several objects with different prefixes
    store
        .put("prefix/a/1.bin", fixtures::seeded_bytes(1, 10))
        .await
        .expect("Put failed");
    store
        .put("prefix/a/2.bin", fixtures::seeded_bytes(2, 10))
        .await
        .expect("Put failed");
    store
        .put("prefix/b/1.bin", fixtures::seeded_bytes(3, 10))
        .await
        .expect("Put failed");
    store
        .put("other/1.bin", fixtures::seeded_bytes(4, 10))
        .await
        .expect("Put failed");

    // List all with prefix/a
    let keys = store.list("prefix/a/").await.expect("List failed");
    assert_eq!(keys.len(), 2);
    assert!(keys.iter().any(|k| k.contains("1.bin")));
    assert!(keys.iter().any(|k| k.contains("2.bin")));

    // List all with prefix/
    let keys = store.list("prefix/").await.expect("List failed");
    assert_eq!(keys.len(), 3);
}

#[tokio::test]
async fn test_get_range() {
    let storage = TestStorage::new().await.expect("Failed to create storage");
    let store = storage.store();

    let key = "test/range.bin";
    let data = fixtures::seeded_bytes(42, 1000);

    store.put(key, data.clone()).await.expect("Put failed");

    // Get a range in the middle
    let range = store
        .get_range(key, 100, 200)
        .await
        .expect("Get range failed");
    assert_eq!(range.len(), 100);
    assert_eq!(range.as_ref(), &data[100..200]);
}

#[tokio::test]
async fn test_streaming_upload() {
    let storage = TestStorage::new().await.expect("Failed to create storage");
    let store = storage.store();

    let key = "test/streamed.bin";

    // Start streaming upload
    let mut upload = store.put_stream(key).await.expect("Put stream failed");

    // Write multiple chunks
    let chunk1 = fixtures::seeded_bytes(1, 500);
    let chunk2 = fixtures::seeded_bytes(2, 500);
    let chunk3 = fixtures::seeded_bytes(3, 500);

    upload.write(chunk1.clone()).await.expect("Write 1 failed");
    upload.write(chunk2.clone()).await.expect("Write 2 failed");
    upload.write(chunk3.clone()).await.expect("Write 3 failed");

    // Finish the upload
    let total = upload.finish().await.expect("Finish failed");
    assert_eq!(total, 1500);

    // Verify the combined content
    let retrieved = store.get(key).await.expect("Get failed");
    assert_eq!(retrieved.len(), 1500);

    // Verify content matches
    let expected: Vec<u8> = [chunk1, chunk2, chunk3]
        .iter()
        .flat_map(|c| c.iter().copied())
        .collect();
    assert_eq!(retrieved.as_ref(), &expected[..]);
}

#[tokio::test]
async fn test_streaming_upload_abort() {
    let storage = TestStorage::new().await.expect("Failed to create storage");
    let store = storage.store();

    let key = "test/aborted.bin";

    // Start streaming upload
    let mut upload = store.put_stream(key).await.expect("Put stream failed");

    // Write some data
    upload
        .write(fixtures::seeded_bytes(1, 500))
        .await
        .expect("Write failed");

    // Abort the upload
    upload.abort().await.expect("Abort failed");

    // Object should not exist
    assert!(!store.exists(key).await.expect("Exists check failed"));
}

#[tokio::test]
async fn test_copy() {
    let storage = TestStorage::new().await.expect("Failed to create storage");
    let store = storage.store();

    let src = "test/source.bin";
    let dst = "test/destination.bin";
    let data = fixtures::seeded_bytes(42, 256);

    store.put(src, data.clone()).await.expect("Put failed");
    store.copy(src, dst).await.expect("Copy failed");

    // Both should exist with same content
    let src_data = store.get(src).await.expect("Get src failed");
    let dst_data = store.get(dst).await.expect("Get dst failed");
    assert_eq!(src_data, data);
    assert_eq!(dst_data, data);
}

#[tokio::test]
async fn test_large_object() {
    let storage = TestStorage::new().await.expect("Failed to create storage");
    let store = storage.store();

    let key = "test/large.bin";
    // 10 MB object
    let data = fixtures::seeded_bytes(42, 10 * 1024 * 1024);

    store.put(key, data.clone()).await.expect("Put failed");

    let retrieved = store.get(key).await.expect("Get failed");
    assert_eq!(retrieved.len(), data.len());
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_nested_paths() {
    let storage = TestStorage::new().await.expect("Failed to create storage");
    let store = storage.store();

    let key = "deeply/nested/path/structure/file.bin";
    let data = fixtures::seeded_bytes(42, 100);

    store.put(key, data.clone()).await.expect("Put failed");

    let retrieved = store.get(key).await.expect("Get failed");
    assert_eq!(retrieved, data);
}
