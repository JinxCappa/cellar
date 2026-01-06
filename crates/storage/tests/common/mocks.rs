use async_trait::async_trait;
use bytes::Bytes;
use cellar_storage::error::{StorageError, StorageResult};
use cellar_storage::traits::{
    ByteStream, ContinuationToken, KeyStream, ListingCapabilities, ListingOptions, ListingPage,
    ListingResume, ObjectMeta, ObjectStore, StreamingUpload,
};
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::Duration;

/// Mock backend for large listing tests that generates synthetic keys on-the-fly
/// without storing them in memory. This allows testing memory usage with millions
/// of keys without actually allocating memory for the keys.
#[allow(dead_code)]
pub struct MockLargeListingBackend {
    pub total_keys: usize,
}

#[allow(dead_code)]
impl MockLargeListingBackend {
    pub fn new(total_keys: usize) -> Arc<Self> {
        Arc::new(Self { total_keys })
    }

    /// Generate a deterministic key at the given index
    fn generate_key(&self, i: usize) -> String {
        format!("chunks/{:02x}/{:02x}/{:064x}", (i >> 8) & 0xff, i & 0xff, i)
    }
}

#[async_trait]
impl ObjectStore for MockLargeListingBackend {
    async fn exists(&self, _key: &str) -> StorageResult<bool> {
        Ok(false)
    }

    async fn head(&self, _key: &str) -> StorageResult<ObjectMeta> {
        Err(StorageError::NotFound("mock backend".to_string()))
    }

    async fn get(&self, _key: &str) -> StorageResult<Bytes> {
        Err(StorageError::NotFound("mock backend".to_string()))
    }

    async fn get_stream(&self, _key: &str) -> StorageResult<ByteStream> {
        Err(StorageError::NotFound("mock backend".to_string()))
    }

    async fn get_range(&self, _key: &str, _start: u64, _end: u64) -> StorageResult<Bytes> {
        Err(StorageError::NotFound("mock backend".to_string()))
    }

    async fn put(&self, _key: &str, _data: Bytes) -> StorageResult<()> {
        Err(StorageError::InvalidKey(
            "mock backend is read-only".to_string(),
        ))
    }

    async fn put_if_not_exists(&self, _key: &str, _data: Bytes) -> StorageResult<bool> {
        Err(StorageError::InvalidKey(
            "mock backend is read-only".to_string(),
        ))
    }

    async fn put_stream(&self, _key: &str) -> StorageResult<Box<dyn StreamingUpload>> {
        Err(StorageError::InvalidKey(
            "mock backend is read-only".to_string(),
        ))
    }

    async fn delete(&self, _key: &str) -> StorageResult<()> {
        Err(StorageError::InvalidKey(
            "mock backend is read-only".to_string(),
        ))
    }

    async fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
        // Materialized list (old API) - should not be used in new tests
        let keys: Vec<String> = (0..self.total_keys)
            .filter(|i| {
                let key = self.generate_key(*i);
                key.starts_with(prefix)
            })
            .map(|i| self.generate_key(i))
            .collect();
        Ok(keys)
    }

    async fn list_stream(&self, prefix: &str) -> StorageResult<KeyStream> {
        // Legacy streaming API - use list_pages() instead
        let prefix = prefix.to_string();
        let total_keys = self.total_keys;

        Ok(Box::pin(async_stream::stream! {
            for i in 0..total_keys {
                let key = format!("chunks/{:02x}/{:02x}/{:064x}", (i >> 8) & 0xff, i & 0xff, i);
                if key.starts_with(&prefix) {
                    yield Ok(key);
                }
            }
        }))
    }

    fn backend_name(&self) -> &'static str {
        "mock-large-listing"
    }

    fn listing_capabilities(&self) -> ListingCapabilities {
        ListingCapabilities { resumable: false }
    }

    fn list_pages<'a>(
        &'a self,
        prefix: &str,
        options: ListingOptions,
        resume: Option<ListingResume>,
    ) -> Pin<Box<dyn Stream<Item = StorageResult<ListingPage>> + Send + 'a>> {
        let page_size = options.normalized_page_size();
        let prefix = prefix.to_string();

        // Parse resume token (simple index encoding)
        let start_index = if let Some(resume) = resume {
            String::from_utf8(resume.start_token.as_bytes().to_vec())
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0)
        } else {
            0
        };

        let total_keys = self.total_keys;

        Box::pin(async_stream::try_stream! {
            let mut index = start_index;
            while index < total_keys {
                let end = (index + page_size).min(total_keys);

                // Generate keys for this page
                let keys: Vec<String> = (index..end)
                    .map(|i| format!("chunks/{:02x}/{:02x}/{:064x}", (i >> 8) & 0xff, i & 0xff, i))
                    .filter(|key| key.starts_with(&prefix))
                    .collect();

                // Create continuation token if there are more pages
                let next_token = if end < total_keys {
                    Some(ContinuationToken::new(end.to_string().into_bytes())?)
                } else {
                    None
                };

                yield ListingPage { keys, next_token };

                index = end;
            }
        })
    }

    async fn copy(&self, _from: &str, _to: &str) -> StorageResult<()> {
        Err(StorageError::InvalidKey(
            "mock backend does not support copy".to_string(),
        ))
    }
}

/// Instrumented backend that counts the number of pages fetched
/// Useful for testing cancellation and backpressure behavior
#[allow(dead_code)]
pub struct InstrumentedBackend {
    pub total_keys: usize,
    pub pages_fetched: Arc<AtomicUsize>,
}

#[allow(dead_code)]
impl InstrumentedBackend {
    pub fn new(total_keys: usize) -> (Arc<Self>, Arc<AtomicUsize>) {
        let counter = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(Self {
            total_keys,
            pages_fetched: counter.clone(),
        });
        (backend, counter)
    }
}

#[async_trait]
impl ObjectStore for InstrumentedBackend {
    async fn exists(&self, _key: &str) -> StorageResult<bool> {
        Ok(false)
    }

    async fn head(&self, _key: &str) -> StorageResult<ObjectMeta> {
        Err(StorageError::NotFound("instrumented backend".to_string()))
    }

    async fn get(&self, _key: &str) -> StorageResult<Bytes> {
        Err(StorageError::NotFound("instrumented backend".to_string()))
    }

    async fn get_stream(&self, _key: &str) -> StorageResult<ByteStream> {
        Err(StorageError::NotFound("instrumented backend".to_string()))
    }

    async fn get_range(&self, _key: &str, _start: u64, _end: u64) -> StorageResult<Bytes> {
        Err(StorageError::NotFound("instrumented backend".to_string()))
    }

    async fn put(&self, _key: &str, _data: Bytes) -> StorageResult<()> {
        Err(StorageError::InvalidKey(
            "instrumented backend is read-only".to_string(),
        ))
    }

    async fn put_if_not_exists(&self, _key: &str, _data: Bytes) -> StorageResult<bool> {
        Err(StorageError::InvalidKey(
            "instrumented backend is read-only".to_string(),
        ))
    }

    async fn put_stream(&self, _key: &str) -> StorageResult<Box<dyn StreamingUpload>> {
        Err(StorageError::InvalidKey(
            "instrumented backend is read-only".to_string(),
        ))
    }

    async fn delete(&self, _key: &str) -> StorageResult<()> {
        Err(StorageError::InvalidKey(
            "instrumented backend is read-only".to_string(),
        ))
    }

    async fn list(&self, _prefix: &str) -> StorageResult<Vec<String>> {
        Ok(Vec::new())
    }

    async fn list_stream(&self, _prefix: &str) -> StorageResult<KeyStream> {
        Ok(Box::pin(futures::stream::empty()))
    }

    fn backend_name(&self) -> &'static str {
        "instrumented"
    }

    fn listing_capabilities(&self) -> ListingCapabilities {
        ListingCapabilities { resumable: false }
    }

    fn list_pages<'a>(
        &'a self,
        prefix: &str,
        options: ListingOptions,
        resume: Option<ListingResume>,
    ) -> Pin<Box<dyn Stream<Item = StorageResult<ListingPage>> + Send + 'a>> {
        let page_size = options.normalized_page_size();
        let prefix = prefix.to_string();
        let pages_counter = self.pages_fetched.clone();

        // Parse resume token (simple index encoding)
        let start_index = if let Some(resume) = resume {
            String::from_utf8(resume.start_token.as_bytes().to_vec())
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0)
        } else {
            0
        };

        let total_keys = self.total_keys;

        Box::pin(async_stream::try_stream! {
            let mut index = start_index;
            while index < total_keys {
                // Increment counter BEFORE yielding page (important for cancellation tests)
                pages_counter.fetch_add(1, Ordering::SeqCst);

                let end = (index + page_size).min(total_keys);

                // Generate keys for this page
                let keys: Vec<String> = (index..end)
                    .map(|i| format!("key-{}", i))
                    .filter(|key| key.starts_with(&prefix) || prefix.is_empty())
                    .collect();

                // Create continuation token if there are more pages
                let next_token = if end < total_keys {
                    Some(ContinuationToken::new(end.to_string().into_bytes())?)
                } else {
                    None
                };

                yield ListingPage { keys, next_token };

                index = end;

                // Small delay to ensure async scheduling works properly
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    }

    async fn copy(&self, _from: &str, _to: &str) -> StorageResult<()> {
        Err(StorageError::InvalidKey(
            "instrumented backend does not support copy".to_string(),
        ))
    }
}
