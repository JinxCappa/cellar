// Lifetime safety tests for listing contexts.
// Ensures owned contexts keep the backend alive for the lifetime of streams.

mod common;

use async_trait::async_trait;
use bytes::Bytes;
use cellar_storage::context::{ListingContext, ListingContextOwned};
use cellar_storage::error::{StorageError, StorageResult};
use cellar_storage::traits::{
    ByteStream, ContinuationToken, KeyStream, ListingCapabilities, ListingOptions, ListingPage,
    ListingResume, ObjectMeta, ObjectStore, StreamingUpload,
};
use futures::Stream;
use futures::StreamExt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::MockLargeListingBackend;

/// Backend that records when it is dropped.
struct DropAwareBackend {
    dropped: Arc<AtomicBool>,
}

impl Drop for DropAwareBackend {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl ObjectStore for DropAwareBackend {
    async fn exists(&self, _key: &str) -> StorageResult<bool> {
        Ok(false)
    }

    async fn head(&self, _key: &str) -> StorageResult<ObjectMeta> {
        Err(StorageError::NotFound("drop-test".to_string()))
    }

    async fn get(&self, _key: &str) -> StorageResult<Bytes> {
        Err(StorageError::NotFound("drop-test".to_string()))
    }

    async fn get_stream(&self, _key: &str) -> StorageResult<ByteStream> {
        Err(StorageError::NotFound("drop-test".to_string()))
    }

    async fn get_range(&self, _key: &str, _start: u64, _end: u64) -> StorageResult<Bytes> {
        Err(StorageError::NotFound("drop-test".to_string()))
    }

    async fn put(&self, _key: &str, _data: Bytes) -> StorageResult<()> {
        Err(StorageError::InvalidKey("read-only".to_string()))
    }

    async fn put_if_not_exists(&self, _key: &str, _data: Bytes) -> StorageResult<bool> {
        Err(StorageError::InvalidKey("read-only".to_string()))
    }

    async fn put_stream(&self, _key: &str) -> StorageResult<Box<dyn StreamingUpload>> {
        Err(StorageError::InvalidKey("read-only".to_string()))
    }

    async fn delete(&self, _key: &str) -> StorageResult<()> {
        Err(StorageError::InvalidKey("read-only".to_string()))
    }

    async fn list(&self, _prefix: &str) -> StorageResult<Vec<String>> {
        Ok(Vec::new())
    }

    async fn list_stream(&self, _prefix: &str) -> StorageResult<KeyStream> {
        Ok(Box::pin(futures::stream::empty()))
    }

    fn backend_name(&self) -> &'static str {
        "drop-aware"
    }

    fn listing_capabilities(&self) -> ListingCapabilities {
        ListingCapabilities { resumable: false }
    }

    fn list_pages<'a>(
        &'a self,
        _prefix: &str,
        _options: ListingOptions,
        _resume: Option<ListingResume>,
    ) -> Pin<Box<dyn Stream<Item = StorageResult<ListingPage>> + Send + 'a>> {
        Box::pin(async_stream::try_stream! {
            yield ListingPage {
                keys: vec!["a".to_string(), "b".to_string()],
                next_token: None,
            };
        })
    }

    async fn copy(&self, _from: &str, _to: &str) -> StorageResult<()> {
        Err(StorageError::InvalidKey("read-only".to_string()))
    }
}

#[tokio::test]
async fn owned_context_keeps_backend_alive_for_key_stream() {
    let dropped = Arc::new(AtomicBool::new(false));
    let backend = Arc::new(DropAwareBackend {
        dropped: dropped.clone(),
    });

    let ctx = ListingContextOwned::new(backend.clone(), "", ListingOptions::new(10), None);
    let mut stream = ctx.into_stream();

    // Drop the caller's Arc; the stream should keep the backend alive.
    drop(backend);
    assert!(
        !dropped.load(Ordering::SeqCst),
        "Backend dropped while stream is still alive"
    );

    let collected: Vec<_> = stream
        .by_ref()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<StorageResult<Vec<_>>>()
        .expect("Stream failed");
    assert_eq!(collected, vec!["a".to_string(), "b".to_string()]);

    drop(stream);
    assert!(
        dropped.load(Ordering::SeqCst),
        "Backend should drop after stream is dropped"
    );
}

#[tokio::test]
async fn owned_context_keeps_backend_alive_for_page_stream() {
    let dropped = Arc::new(AtomicBool::new(false));
    let backend = Arc::new(DropAwareBackend {
        dropped: dropped.clone(),
    });

    let ctx = ListingContextOwned::new(backend.clone(), "", ListingOptions::new(10), None);
    let mut stream = ctx.into_stream_pages();

    drop(backend);
    assert!(
        !dropped.load(Ordering::SeqCst),
        "Backend dropped while page stream is still alive"
    );

    let mut pages = Vec::new();
    while let Some(page) = stream.next().await {
        pages.push(page.expect("page failed"));
    }
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].keys, vec!["a".to_string(), "b".to_string()]);

    drop(stream);
    assert!(
        dropped.load(Ordering::SeqCst),
        "Backend should drop after page stream is dropped"
    );
}

#[tokio::test]
async fn listing_context_stream_respects_resume() {
    let backend = MockLargeListingBackend::new(200);
    let resume = ListingResume::new(ContinuationToken::new(b"20".to_vec()).expect("token"));

    let ctx = ListingContext::new(backend.as_ref(), "", ListingOptions::new(10), Some(resume));

    let mut stream = ctx.stream();
    let first = stream
        .next()
        .await
        .expect("missing item")
        .expect("stream error");

    let index = 20usize;
    let expected = format!(
        "chunks/{:02x}/{:02x}/{:064x}",
        (index >> 8) & 0xff,
        index & 0xff,
        index
    );
    assert_eq!(first, expected);
}
