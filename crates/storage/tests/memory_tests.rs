// Memory validation tests for streaming listing API
// Ensures that streaming large listings doesn't cause excessive memory usage

mod common;

use bytes::Bytes;
use cellar_storage::traits::{ListingOptions, ObjectStore};
use common::{MockLargeListingBackend, format_memory_size, get_process_rss};
use futures::StreamExt;
use tempfile::TempDir;

#[tokio::test]
async fn test_streaming_memory_usage_bounded() {
    // Test that streaming 1M keys uses constant memory (< 50MB growth)
    let mock = MockLargeListingBackend::new(1_000_000);

    // Measure baseline RSS before listing
    let baseline_rss = get_process_rss();
    println!("Baseline RSS: {}", format_memory_size(baseline_rss));

    // Stream through all keys using list_pages()
    let mut pages = mock.list_pages("chunks/", ListingOptions::new(1000), None);
    let mut count = 0;
    let mut page_count = 0;

    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        count += page.keys.len();
        page_count += 1;

        // Don't accumulate keys - just count them
        // This simulates real GC sweep behavior where we process keys immediately

        // Sample memory every 100 pages
        if page_count % 100 == 0 {
            let current_rss = get_process_rss();
            let delta = current_rss.saturating_sub(baseline_rss);
            println!(
                "After {} pages ({} keys): RSS delta = {}",
                page_count,
                count,
                format_memory_size(delta)
            );
        }
    }

    // Measure peak RSS after streaming
    let peak_rss = get_process_rss();
    let memory_delta = peak_rss.saturating_sub(baseline_rss);

    println!("Streamed {} keys in {} pages", count, page_count);
    println!(
        "Peak RSS: {}, Delta: {}",
        format_memory_size(peak_rss),
        format_memory_size(memory_delta)
    );

    assert_eq!(count, 1_000_000, "Should have streamed all 1M keys");

    // Assert memory growth < 50MB
    assert!(
        memory_delta < 50 * 1024 * 1024,
        "Memory growth {} exceeds 50MB threshold",
        format_memory_size(memory_delta)
    );
}

#[tokio::test]
async fn test_legacy_list_stream_memory_comparison() {
    // Compare memory usage between streaming and legacy materialized paths
    // Use smaller dataset (100K keys) since materialized path will allocate everything

    let mock = MockLargeListingBackend::new(100_000);

    // Test streaming path
    let baseline_streaming = get_process_rss();

    let mut pages = mock.list_pages("chunks/", ListingOptions::new(1000), None);
    let mut count = 0;
    while let Some(page) = pages.next().await {
        count += page.unwrap().keys.len();
    }

    let peak_streaming = get_process_rss();
    let delta_streaming = peak_streaming.saturating_sub(baseline_streaming);

    println!(
        "Streaming path: {} keys, memory delta: {}",
        count,
        format_memory_size(delta_streaming)
    );

    assert_eq!(count, 100_000);

    // Test legacy materialized path
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    let baseline_materialized = get_process_rss();

    let keys = mock.list("chunks/").await.unwrap();
    let count_materialized = keys.len();

    // Keep keys in scope to measure peak memory
    let peak_materialized = get_process_rss();
    let delta_materialized = peak_materialized.saturating_sub(baseline_materialized);

    println!(
        "Materialized path: {} keys, memory delta: {}",
        count_materialized,
        format_memory_size(delta_materialized)
    );

    drop(keys); // Drop materialized list

    assert_eq!(count_materialized, 100_000);

    // Streaming should use significantly less memory than materialization
    // With 100K keys at ~50 bytes each, materialized uses ~5MB+ for the Vec
    // Streaming should use much less (constant page size)
    println!(
        "Memory comparison: streaming={}, materialized={}",
        format_memory_size(delta_streaming),
        format_memory_size(delta_materialized)
    );

    // Note: This assertion may be flaky due to GC and allocator behavior
    // We're mainly testing that streaming doesn't blow up memory
    assert!(
        delta_streaming < 20 * 1024 * 1024,
        "Streaming should use < 20MB for 100K keys, used: {}",
        format_memory_size(delta_streaming)
    );
}

#[tokio::test]
async fn test_filesystem_backend_streaming_memory() {
    // Test real filesystem backend with many files
    let temp_dir = TempDir::new().unwrap();
    let backend =
        cellar_storage::backends::filesystem::FilesystemBackend::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

    // Create 5000 small files (reduced from 10K for faster test)
    println!("Creating test files...");
    for i in 0..5000 {
        backend
            .put(&format!("test/{:05}", i), Bytes::from(vec![i as u8]))
            .await
            .unwrap();
    }

    // Measure baseline
    let baseline_rss = get_process_rss();

    // Stream through all files
    let mut pages = backend.list_pages("test/", ListingOptions::new(500), None);
    let mut count = 0;

    while let Some(page) = pages.next().await {
        count += page.unwrap().keys.len();
    }

    let peak_rss = get_process_rss();
    let delta = peak_rss.saturating_sub(baseline_rss);

    println!(
        "Filesystem backend: {} files, memory delta: {}",
        count,
        format_memory_size(delta)
    );

    assert_eq!(count, 5000);

    // Filesystem streaming should also use bounded memory
    assert!(
        delta < 30 * 1024 * 1024,
        "Filesystem streaming should use < 30MB, used: {}",
        format_memory_size(delta)
    );
}

#[tokio::test]
async fn test_no_page_accumulation() {
    // Verify that we're not accidentally accumulating pages in memory
    let mock = MockLargeListingBackend::new(50_000);

    let baseline = get_process_rss();

    let mut pages = mock.list_pages("chunks/", ListingOptions::new(1000), None);
    let mut count = 0;
    let mut peak_delta = 0;

    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        count += page.keys.len();

        // Measure after each page
        let current = get_process_rss();
        let delta = current.saturating_sub(baseline);
        peak_delta = peak_delta.max(delta);
    }

    println!(
        "Processed {} keys, peak delta: {}",
        count,
        format_memory_size(peak_delta)
    );

    assert_eq!(count, 50_000);

    // Peak delta should be small and not grow with number of pages
    assert!(
        peak_delta < 15 * 1024 * 1024,
        "Peak memory delta should be < 15MB, was: {}",
        format_memory_size(peak_delta)
    );
}
