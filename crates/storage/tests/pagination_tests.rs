// Pagination regression tests for listing API
// Ensures proper pagination behavior without accidental buffering

mod common;

use bytes::Bytes;
use cellar_storage::traits::{ListingOptions, MAX_PAGE_SIZE, MIN_PAGE_SIZE, ObjectStore};
use common::{InstrumentedBackend, MockLargeListingBackend, format_memory_size, get_process_rss};
use futures::StreamExt;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use tempfile::TempDir;
use tokio::time::Duration;

#[tokio::test]
async fn test_large_listing_pagination() {
    // Test 10K objects with various page sizes
    let temp_dir = TempDir::new().unwrap();
    let backend =
        cellar_storage::backends::filesystem::FilesystemBackend::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

    // Create 2000 test objects (reduced from 10K for faster execution)
    println!("Creating test files...");
    for i in 0..2000 {
        backend
            .put(&format!("test/{:05}", i), Bytes::from(vec![i as u8]))
            .await
            .unwrap();
    }

    // Test various page sizes
    for page_size in &[100, 500, 1000] {
        println!("Testing page_size={}", page_size);

        let mut stream = backend.list_pages("test/", ListingOptions::new(*page_size), None);
        let mut total_keys = 0;
        let mut page_count = 0;

        while let Some(page) = stream.next().await {
            let page = page.unwrap();
            let keys_in_page = page.keys.len();

            // Each page should have up to page_size keys
            assert!(
                keys_in_page <= *page_size,
                "Page {} has {} keys, exceeds page_size {}",
                page_count,
                keys_in_page,
                page_size
            );

            total_keys += keys_in_page;
            page_count += 1;

            // Continue until stream is exhausted (next_token being None doesn't mean stream is done)
        }

        assert_eq!(
            total_keys, 2000,
            "page_size={}: Expected 2000 keys, got {}",
            page_size, total_keys
        );

        println!(
            "page_size={}: {} pages, {} total keys",
            page_size, page_count, total_keys
        );
    }
}

#[tokio::test]
async fn test_no_accidental_buffering() {
    // Verify that streaming doesn't accumulate all keys in memory
    let mock = MockLargeListingBackend::new(100_000);

    let baseline_rss = get_process_rss();

    let mut stream = mock.list_pages("", ListingOptions::new(1000), None);
    let mut page_count = 0;

    // Process pages one at a time without accumulating
    while let Some(page) = stream.next().await {
        let _page = page.unwrap();

        // Process keys (don't store them)
        // Just count, don't accumulate

        page_count += 1;

        // Check memory every 10 pages
        if page_count % 10 == 0 {
            let current_rss = get_process_rss();
            let growth = current_rss.saturating_sub(baseline_rss);

            // Memory should not grow proportionally to pages processed
            // Allow some growth for async runtime overhead
            assert!(
                growth < 10 * 1024 * 1024,
                "Memory grew by {} after {} pages - possible buffering",
                format_memory_size(growth),
                page_count
            );
        }
    }

    println!("Processed {} pages without buffering", page_count);
}

#[tokio::test]
async fn test_backpressure_works() {
    // Test that slow consumer doesn't cause producer to over-fetch
    let (backend, pages_fetched) = InstrumentedBackend::new(10_000);

    let mut stream = backend.list_pages("", ListingOptions::new(100), None);

    // Slowly consume only 5 pages
    for _ in 0..5 {
        if let Some(page) = stream.next().await {
            let _ = page.unwrap();

            // Simulate slow processing
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    let fetched = pages_fetched.load(Ordering::SeqCst);

    // Should have fetched ~5-7 pages, not all 100
    println!("Consumed 5 pages, backend fetched {} pages", fetched);

    assert!(
        fetched <= 10,
        "Backpressure failed: fetched {} pages when only 5 consumed",
        fetched
    );
}

#[tokio::test]
async fn test_empty_prefix_listing() {
    // Test handling of empty results
    let temp_dir = TempDir::new().unwrap();
    let backend =
        cellar_storage::backends::filesystem::FilesystemBackend::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

    // No objects exist
    let mut stream = backend.list_pages("nonexistent/", ListingOptions::default(), None);

    // Should complete immediately with no pages or empty page
    let result = stream.next().await;

    match result {
        None => {
            // Correct: no pages for empty listing
            println!("Empty listing returned None (no pages)");
        }
        Some(Ok(page)) => {
            assert!(
                page.keys.is_empty(),
                "Empty prefix should yield empty page, got {} keys",
                page.keys.len()
            );
            assert!(
                page.next_token.is_none(),
                "Empty listing should have no continuation"
            );
            println!("Empty listing returned empty page");
        }
        Some(Err(e)) => {
            panic!("Empty listing should not error: {}", e);
        }
    }
}

#[test]
fn test_page_size_clamping() {
    // Test that page size is clamped to valid range

    // Test too small
    let opts = ListingOptions::new(10);
    assert_eq!(opts.normalized_page_size(), MIN_PAGE_SIZE);

    // Test too large
    let opts = ListingOptions::new(999_999);
    assert_eq!(opts.normalized_page_size(), MAX_PAGE_SIZE);

    // Test in range
    let opts = ListingOptions::new(1000);
    assert_eq!(opts.normalized_page_size(), 1000);

    // Test edge cases
    let opts = ListingOptions::new(MIN_PAGE_SIZE);
    assert_eq!(opts.normalized_page_size(), MIN_PAGE_SIZE);

    let opts = ListingOptions::new(MAX_PAGE_SIZE);
    assert_eq!(opts.normalized_page_size(), MAX_PAGE_SIZE);
}

#[tokio::test]
async fn test_no_duplicate_keys_in_pagination() {
    // Verify backend doesn't return duplicate keys across pages
    let temp_dir = TempDir::new().unwrap();
    let backend =
        cellar_storage::backends::filesystem::FilesystemBackend::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

    // Create objects
    for i in 0..100 {
        backend
            .put(&format!("test/{:03}", i), Bytes::from(vec![i as u8]))
            .await
            .unwrap();
    }

    // Stream all pages
    let mut stream = backend.list_pages("test/", ListingOptions::new(25), None);
    let mut all_keys = Vec::new();

    while let Some(page) = stream.next().await {
        all_keys.extend(page.unwrap().keys);
    }

    // Check for duplicates
    let mut seen = HashSet::new();
    let mut duplicates = Vec::new();

    for key in &all_keys {
        if !seen.insert(key.clone()) {
            duplicates.push(key.clone());
        }
    }

    assert!(
        duplicates.is_empty(),
        "Found {} duplicate keys: {:?}",
        duplicates.len(),
        duplicates
    );

    println!(
        "Verified {} unique keys across multiple pages",
        all_keys.len()
    );
}

#[tokio::test]
async fn test_page_boundaries_consistent() {
    // Test that page boundaries are consistent and predictable
    let mock = MockLargeListingBackend::new(1000);

    let mut stream = mock.list_pages("chunks/", ListingOptions::new(100), None);
    let mut page_sizes = Vec::new();

    while let Some(page) = stream.next().await {
        let page = page.unwrap();
        page_sizes.push(page.keys.len());
    }

    println!("Page sizes: {:?}", page_sizes);

    // All pages except the last should have exactly 100 keys
    for (i, size) in page_sizes.iter().enumerate() {
        if i < page_sizes.len() - 1 {
            assert_eq!(
                *size, 100,
                "Non-final page {} should have exactly 100 keys, got {}",
                i, size
            );
        }
    }

    // Last page may have fewer keys
    if let Some(last_size) = page_sizes.last() {
        assert!(
            *last_size > 0 && *last_size <= 100,
            "Last page should have 1-100 keys, got {}",
            last_size
        );
    }
}
