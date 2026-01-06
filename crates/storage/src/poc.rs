//! Proof of Concept module for validating the streaming listing API design.
//!
//! This module contains compile-time tests to ensure:
//! 1. The trait is object-safe (can be used with dyn ObjectStore)
//! 2. Lifetime behavior is correct (no 'static requirement for borrowed contexts)
//! 3. Both borrowed and owned contexts work as expected
//!
//! This module will be deleted once the design is validated.

#![allow(dead_code, unused_imports)]

use crate::context::{ListingContext, ListingContextOwned};
use crate::error::StorageResult;
use crate::traits::{ListingOptions, ObjectStore};
use futures::StreamExt;
use std::sync::Arc;

/// Compile-time test: Verify that ObjectStore can be used with dyn.
///
/// This function signature ensures that the trait is object-safe.
fn _test_object_safety(_store: &dyn ObjectStore) {
    // If this compiles, the trait is object-safe
}

/// Compile-time test: Verify that ObjectStore can be used with Arc<dyn>.
fn _test_arc_dyn(_store: Arc<dyn ObjectStore>) {
    // If this compiles, Arc<dyn ObjectStore> works
}

/// Compile-time test: Verify borrowed context lifetime behavior.
///
/// This ensures that ListingContext<'a> can borrow from a local store
/// without requiring 'static.
async fn _test_borrowed_context_lifetime(store: &dyn ObjectStore) -> StorageResult<Vec<String>> {
    let ctx = ListingContext::new(store, "prefix/", ListingOptions::default(), None);

    // Stream should have lifetime tied to store
    let mut stream = ctx.stream();

    let mut keys = Vec::new();
    while let Some(result) = stream.next().await {
        keys.push(result?);
    }

    Ok(keys)
}

/// Compile-time test: Verify owned context can be created from Arc.
async fn _test_owned_context(store: Arc<dyn ObjectStore>) -> StorageResult<Vec<String>> {
    let ctx = ListingContextOwned::new(store, "prefix/", ListingOptions::default(), None);

    // Stream should have 'static lifetime
    let mut stream = ctx.into_stream();

    let mut keys = Vec::new();
    while let Some(result) = stream.next().await {
        keys.push(result?);
    }

    Ok(keys)
}

/// Compile-time test: Verify owned context can be borrowed.
async fn _test_owned_context_borrow(store: Arc<dyn ObjectStore>) -> StorageResult<()> {
    let ctx = ListingContextOwned::new(store, "prefix/", ListingOptions::default(), None);

    // Borrow the context
    let borrowed = ctx.borrow();

    // Use the borrowed context
    let mut stream = borrowed.stream();
    let _ = stream.next().await;

    Ok(())
}

/// Compile-time test: Verify owned context can be spawned.
async fn _test_owned_context_spawn(store: Arc<dyn ObjectStore>) -> StorageResult<Vec<String>> {
    let ctx = ListingContextOwned::new(store, "prefix/", ListingOptions::default(), None);

    // Spawn a task with the owned context
    let handle = tokio::spawn(async move {
        let mut stream = ctx.into_stream();
        let mut keys = Vec::new();
        while let Some(result) = stream.next().await {
            keys.push(result.unwrap());
        }
        keys
    });

    Ok(handle.await.unwrap())
}

/// Compile-time test: Verify page streaming with borrowed context.
async fn _test_borrowed_context_pages(store: &dyn ObjectStore) -> StorageResult<Vec<Vec<String>>> {
    let ctx = ListingContext::new(store, "prefix/", ListingOptions::default(), None);

    let mut stream = ctx.stream_pages();
    let mut pages = Vec::new();

    while let Some(result) = stream.next().await {
        let page = result?;
        pages.push(page.keys);
        if page.next_token.is_none() {
            break;
        }
    }

    Ok(pages)
}

/// Compile-time test: Verify page streaming with owned context.
async fn _test_owned_context_pages(store: Arc<dyn ObjectStore>) -> StorageResult<Vec<Vec<String>>> {
    let ctx = ListingContextOwned::new(store, "prefix/", ListingOptions::default(), None);

    let mut stream = ctx.into_stream_pages();
    let mut pages = Vec::new();

    while let Some(result) = stream.next().await {
        let page = result?;
        pages.push(page.keys);
        if page.next_token.is_none() {
            break;
        }
    }

    Ok(pages)
}

/// Compile-time test: Verify that we can pass dyn ObjectStore to functions.
async fn _test_dyn_parameter() -> StorageResult<()> {
    async fn helper(store: &dyn ObjectStore) -> StorageResult<()> {
        let ctx = ListingContext::new(store, "prefix/", ListingOptions::default(), None);
        let _ = ctx.stream();
        Ok(())
    }

    // This would be called with a concrete backend in real code
    // helper(&some_backend).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::filesystem::FilesystemBackend;
    use bytes::Bytes;
    use tempfile::tempdir;

    async fn build_store() -> (tempfile::TempDir, Arc<dyn ObjectStore>) {
        let temp = tempdir().unwrap();
        let backend = FilesystemBackend::new(temp.path()).await.unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(backend);

        store
            .put("prefix/a.txt", Bytes::from_static(b"a"))
            .await
            .unwrap();
        store
            .put("prefix/b.txt", Bytes::from_static(b"b"))
            .await
            .unwrap();
        store
            .put("other/c.txt", Bytes::from_static(b"c"))
            .await
            .unwrap();

        (temp, store)
    }

    #[test]
    fn poc_compiles() {
        // This test just ensures the PoC module compiles.
        // All the real validation happens at compile time via the function signatures above.
    }

    #[tokio::test]
    async fn poc_listing_helpers_execute() {
        let (_temp, store) = build_store().await;

        let borrowed = _test_borrowed_context_lifetime(store.as_ref())
            .await
            .unwrap();
        assert!(borrowed.iter().any(|k| k.ends_with("prefix/a.txt")));

        let owned = _test_owned_context(store.clone()).await.unwrap();
        assert!(owned.iter().any(|k| k.ends_with("prefix/b.txt")));

        _test_owned_context_borrow(store.clone()).await.unwrap();

        let spawned = _test_owned_context_spawn(store.clone()).await.unwrap();
        assert!(spawned.iter().any(|k| k.ends_with("prefix/b.txt")));

        let pages = _test_borrowed_context_pages(store.as_ref()).await.unwrap();
        assert!(!pages.is_empty());

        let owned_pages = _test_owned_context_pages(store.clone()).await.unwrap();
        assert!(!owned_pages.is_empty());

        _test_dyn_parameter().await.unwrap();
    }
}
