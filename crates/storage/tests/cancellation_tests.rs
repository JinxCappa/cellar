// Cancellation tests for streaming listing API
// Ensures that dropping a stream stops fetching additional pages

mod common;

use cellar_storage::traits::{ListingOptions, ObjectStore};
use common::InstrumentedBackend;
use futures::StreamExt;
use std::sync::atomic::Ordering;
use tokio::time::Duration;

#[tokio::test]
async fn test_stream_drop_stops_pagination() {
    // Test that dropping stream after N keys prevents fetching all pages
    let page_size = 100;
    let total_keys = 10_000;
    let keys_to_read = 250; // Read 2.5 pages, then drop

    let (backend, pages_fetched) = InstrumentedBackend::new(total_keys);

    let mut stream = backend.list_pages("", ListingOptions::new(page_size), None);
    let mut count = 0;

    // Consume keys until we hit target
    while let Some(page) = stream.next().await {
        let page = page.unwrap();
        for _key in page.keys {
            count += 1;
            if count >= keys_to_read {
                break;
            }
        }
        if count >= keys_to_read {
            break;
        }
    }

    drop(stream); // Explicit drop

    // Give async runtime time to settle
    tokio::time::sleep(Duration::from_millis(50)).await;

    let fetched = pages_fetched.load(Ordering::SeqCst);

    println!(
        "Read {} keys, backend fetched {} pages (expected ~3)",
        count, fetched
    );

    // Should have fetched ~3 pages (250 keys / 100 per page = 2.5, rounded up to 3)
    // Allow 1 additional page due to async prefetching
    assert!(
        fetched <= 4,
        "Expected <= 4 pages fetched after cancellation, got {}",
        fetched
    );

    // Should NOT have fetched all 100 pages
    assert!(
        fetched < total_keys / page_size,
        "Stream should have stopped early, but fetched {} pages",
        fetched
    );
}

#[tokio::test]
async fn test_early_return_from_stream() -> cellar_storage::error::StorageResult<()> {
    // Test that early return (not just drop) stops pagination
    let (backend, pages_fetched) = InstrumentedBackend::new(5000);

    async fn process_limited(
        backend: std::sync::Arc<InstrumentedBackend>,
    ) -> cellar_storage::error::StorageResult<usize> {
        let mut stream = backend.list_pages("", ListingOptions::new(100), None);
        let mut count = 0;

        while let Some(page) = stream.next().await {
            let page = page?;
            count += page.keys.len();
            if count >= 150 {
                return Ok(count); // Early return
            }
        }
        Ok(count)
    }

    let count = process_limited(backend.clone()).await?;
    assert_eq!(count, 200); // Should have consumed 2 full pages (200 keys)

    tokio::time::sleep(Duration::from_millis(50)).await;

    let fetched = pages_fetched.load(Ordering::SeqCst);
    println!(
        "Early return after {} keys, fetched {} pages",
        count, fetched
    );

    assert!(
        fetched <= 3,
        "Expected <= 3 pages after early return, got {}",
        fetched
    );

    Ok(())
}

#[tokio::test]
async fn test_backpressure_prevents_overfetching() {
    // Test that slow consumer doesn't cause producer to over-fetch
    let (backend, pages_fetched) = InstrumentedBackend::new(10_000);

    let mut stream = backend.list_pages("", ListingOptions::new(100), None);

    // Slowly consume only 5 pages
    let mut consumed = 0;
    for _ in 0..5 {
        if let Some(page) = stream.next().await {
            let page = page.unwrap();
            consumed += page.keys.len();

            // Simulate slow processing
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    let fetched = pages_fetched.load(Ordering::SeqCst);

    println!(
        "Consumed {} keys in 5 pages, backend fetched {} pages",
        consumed, fetched
    );

    // Should have fetched ~5-7 pages, not all 100
    // Allow some buffer for async scheduling
    assert!(
        fetched <= 10,
        "Backpressure failed: fetched {} pages when only 5 consumed",
        fetched
    );
}

#[tokio::test]
async fn test_multiple_stream_independence() {
    // Test that multiple concurrent streams don't interfere
    let (backend, pages_fetched) = InstrumentedBackend::new(1000);

    // Create 3 concurrent streams
    let stream1 = async {
        let mut stream = backend.list_pages("", ListingOptions::new(100), None);
        let mut count = 0;
        while let Some(page) = stream.next().await {
            count += page.unwrap().keys.len();
            if count >= 200 {
                break;
            }
        }
        count
    };

    let stream2 = async {
        let mut stream = backend.list_pages("", ListingOptions::new(100), None);
        let mut count = 0;
        while let Some(page) = stream.next().await {
            count += page.unwrap().keys.len();
            if count >= 300 {
                break;
            }
        }
        count
    };

    let stream3 = async {
        let mut stream = backend.list_pages("", ListingOptions::new(100), None);
        let mut count = 0;
        while let Some(page) = stream.next().await {
            count += page.unwrap().keys.len();
        }
        count
    };

    let (count1, count2, count3) = tokio::join!(stream1, stream2, stream3);

    let fetched = pages_fetched.load(Ordering::SeqCst);

    println!(
        "Stream1: {} keys, Stream2: {} keys, Stream3: {} keys, Total pages fetched: {}",
        count1, count2, count3, fetched
    );

    assert_eq!(count1, 200);
    assert_eq!(count2, 300);
    assert_eq!(count3, 1000);

    // Total pages should be reasonable (not 3x the full dataset)
    // Each stream counts its own pages, so we expect roughly:
    // stream1: 2 pages, stream2: 3 pages, stream3: 10 pages = 15 pages
    // Allow some buffer for scheduling
    assert!(fetched <= 20, "Expected ~15 pages total, got {}", fetched);
}
