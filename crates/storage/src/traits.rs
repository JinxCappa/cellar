//! Storage trait definitions.

use crate::error::StorageResult;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::pin::Pin;

/// A boxed stream of bytes for streaming reads.
pub type ByteStream = Pin<Box<dyn Stream<Item = StorageResult<Bytes>> + Send>>;

/// A boxed stream of object keys for streaming list operations.
pub type KeyStream = Pin<Box<dyn Stream<Item = StorageResult<String>> + Send>>;

// ===== Streaming Listing API Types =====

/// Page size constraints for listing operations.
pub const DEFAULT_PAGE_SIZE: usize = 1000;
pub const MIN_PAGE_SIZE: usize = 100;
pub const MAX_PAGE_SIZE: usize = 10000;

/// Maximum size for continuation tokens (2 KB).
pub const MAX_TOKEN_SIZE: usize = 2048;

/// An opaque continuation token for resuming listing operations.
///
/// This token is backend-specific and should not be parsed or modified.
/// Tokens are typically base64-encoded and may contain sensitive information
/// about the backend state (e.g., S3 continuation tokens).
///
/// Maximum size is 2 KB to prevent abuse and ensure reasonable memory usage.
#[derive(Clone, PartialEq, Eq)]
pub struct ContinuationToken(Vec<u8>);

impl ContinuationToken {
    /// Create a new continuation token from raw bytes.
    ///
    /// Returns an error if the token exceeds MAX_TOKEN_SIZE.
    pub fn new(data: Vec<u8>) -> StorageResult<Self> {
        if data.len() > MAX_TOKEN_SIZE {
            return Err(crate::error::StorageError::InvalidContinuationToken(
                format!(
                    "continuation token too large: {} bytes (max: {})",
                    data.len(),
                    MAX_TOKEN_SIZE
                ),
            ));
        }
        Ok(Self(data))
    }

    /// Get the raw token bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Convert to base64 for serialization.
    pub fn to_base64(&self) -> String {
        use base64::{Engine as _, engine::general_purpose};
        general_purpose::STANDARD.encode(&self.0)
    }

    /// Parse from base64.
    pub fn from_base64(s: &str) -> StorageResult<Self> {
        // Pre-check input length to prevent DoS/OOM attacks.
        // Base64 encoding increases size by ~33% (4/3), so max encoded size is:
        // MAX_TOKEN_SIZE * 4/3 ≈ 2731 bytes. Use 2x for safety margin with padding.
        const MAX_BASE64_INPUT: usize = MAX_TOKEN_SIZE * 2;
        if s.len() > MAX_BASE64_INPUT {
            return Err(crate::error::StorageError::InvalidContinuationToken(
                format!(
                    "continuation token base64 too large: {} bytes (max: {})",
                    s.len(),
                    MAX_BASE64_INPUT
                ),
            ));
        }

        use base64::{Engine as _, engine::general_purpose};
        let data = general_purpose::STANDARD.decode(s).map_err(|e| {
            crate::error::StorageError::InvalidContinuationToken(format!(
                "invalid continuation token base64: {}",
                e
            ))
        })?;
        Self::new(data)
    }
}

impl std::fmt::Debug for ContinuationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ContinuationToken")
            .field(&"<redacted>")
            .finish()
    }
}

/// A single page of listing results.
///
/// Contains a batch of object keys and an optional continuation token
/// for fetching the next page.
#[derive(Clone, Debug)]
pub struct ListingPage {
    /// Object keys in this page.
    pub keys: Vec<String>,

    /// Continuation token for the next page, if there are more results.
    /// If None, this is the last page.
    pub next_token: Option<ContinuationToken>,
}

/// Backend capabilities for listing operations.
#[derive(Clone, Debug)]
pub struct ListingCapabilities {
    /// Whether the backend supports resumable listings with continuation tokens.
    ///
    /// If true, the backend can resume a listing from a previous checkpoint
    /// using a continuation token. This is typically supported by cloud storage
    /// backends like S3.
    ///
    /// If false, listings must start from the beginning and cannot be resumed.
    /// This is typical for local filesystem backends.
    pub resumable: bool,
}

/// Options for listing operations.
#[derive(Clone, Debug)]
pub struct ListingOptions {
    /// Number of keys to fetch per page.
    ///
    /// This value will be clamped to [MIN_PAGE_SIZE, MAX_PAGE_SIZE].
    /// Use normalize() to get the actual page size that will be used.
    pub page_size: usize,
}

impl ListingOptions {
    /// Create new listing options with the given page size.
    pub fn new(page_size: usize) -> Self {
        Self { page_size }
    }

    /// Normalize the page size to the valid range.
    pub fn normalize(&self) -> Self {
        Self {
            page_size: self.page_size.clamp(MIN_PAGE_SIZE, MAX_PAGE_SIZE),
        }
    }

    /// Get the normalized page size.
    pub fn normalized_page_size(&self) -> usize {
        self.page_size.clamp(MIN_PAGE_SIZE, MAX_PAGE_SIZE)
    }
}

impl Default for ListingOptions {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
        }
    }
}

/// Resume options for listing operations.
#[derive(Clone, Debug)]
pub struct ListingResume {
    /// Continuation token from a previous listing operation.
    ///
    /// If provided, the listing will resume from where the token indicates.
    /// The backend must validate that the token is compatible with the current
    /// listing request (same prefix, options, backend identity).
    pub start_token: ContinuationToken,
}

impl ListingResume {
    /// Create new resume options with the given token.
    pub fn new(start_token: ContinuationToken) -> Self {
        Self { start_token }
    }
}

/// Object store abstraction for content-addressed storage.
#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    /// Check if an object exists.
    async fn exists(&self, key: &str) -> StorageResult<bool>;

    /// Get an object's size without fetching content.
    async fn head(&self, key: &str) -> StorageResult<ObjectMeta>;

    /// Get an object's content.
    async fn get(&self, key: &str) -> StorageResult<Bytes>;

    /// Get an object as a byte stream.
    async fn get_stream(&self, key: &str) -> StorageResult<ByteStream>;

    /// Get a range of bytes from an object.
    async fn get_range(&self, key: &str, start: u64, end: u64) -> StorageResult<Bytes>;

    /// Put an object atomically.
    async fn put(&self, key: &str, data: Bytes) -> StorageResult<()>;

    /// Put an object only if it doesn't exist.
    async fn put_if_not_exists(&self, key: &str, data: Bytes) -> StorageResult<bool>;

    /// Start a streaming upload.
    async fn put_stream(&self, key: &str) -> StorageResult<Box<dyn StreamingUpload>>;

    /// Delete an object.
    async fn delete(&self, key: &str) -> StorageResult<()>;

    /// List objects with a prefix.
    async fn list(&self, prefix: &str) -> StorageResult<Vec<String>>;

    /// List objects with a prefix, returning a stream of keys.
    /// This is more memory-efficient for large listings than list().
    ///
    /// DEPRECATED: Use list_pages() or the extension trait list_stream_ext() instead.
    /// This method will be removed in a future version.
    async fn list_stream(&self, prefix: &str) -> StorageResult<KeyStream>;

    /// Get the name of this storage backend.
    ///
    /// Returns a static string identifier for the backend type (e.g., "s3", "filesystem").
    /// Used for metrics and logging.
    fn backend_name(&self) -> &'static str;

    /// Get the listing capabilities of this backend.
    ///
    /// Returns information about whether the backend supports resumable listings
    /// with continuation tokens.
    fn listing_capabilities(&self) -> ListingCapabilities;

    /// List objects with a prefix, returning a stream of pages.
    ///
    /// Each page contains a batch of object keys and an optional continuation token
    /// for fetching the next page. This enables true streaming without materializing
    /// the full listing in memory.
    ///
    /// The stream yields pages with a lifetime tied to the ObjectStore borrow ('a).
    /// For spawned tasks that need 'static lifetimes, use ListingContextOwned instead.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Only return objects with this prefix
    /// * `options` - Page size and other listing options
    /// * `resume` - Optional continuation token to resume from
    ///
    /// # Returns
    ///
    /// A stream of ListingPage results. Each page contains up to page_size keys
    /// and an optional continuation token for the next page.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The backend does not support resumption but a start_token was provided
    /// - The continuation token is invalid or expired
    /// - There is a backend-specific error fetching the listing
    fn list_pages<'a>(
        &'a self,
        prefix: &str,
        options: ListingOptions,
        resume: Option<ListingResume>,
    ) -> Pin<Box<dyn Stream<Item = StorageResult<ListingPage>> + Send + 'a>>;

    /// Copy an object.
    async fn copy(&self, from: &str, to: &str) -> StorageResult<()>;

    /// Verify storage backend connectivity.
    ///
    /// This method performs a lightweight operation to verify the storage backend
    /// is reachable and properly configured. It should be called during server
    /// startup to ensure the storage is available before accepting requests.
    ///
    /// The default implementation returns Ok(()), suitable for backends that
    /// don't require connectivity verification (e.g., local filesystem).
    ///
    /// # Errors
    ///
    /// Returns an error if the backend is not reachable or misconfigured.
    async fn health_check(&self) -> StorageResult<()> {
        Ok(())
    }
}

/// Metadata about a stored object.
#[derive(Clone, Debug)]
pub struct ObjectMeta {
    /// Object size in bytes.
    pub size: u64,
    /// Last modification time (if available).
    pub last_modified: Option<time::OffsetDateTime>,
    /// Content type (if available).
    pub content_type: Option<String>,
}

/// Trait for streaming uploads.
#[async_trait]
pub trait StreamingUpload: Send {
    /// Write a chunk of data.
    async fn write(&mut self, data: Bytes) -> StorageResult<()>;

    /// Finish the upload and return the total bytes written.
    async fn finish(self: Box<Self>) -> StorageResult<u64>;

    /// Abort the upload.
    async fn abort(self: Box<Self>) -> StorageResult<()>;
}

// ===== Extension Trait for Backwards Compatibility =====

/// Extension trait providing list_stream_ext() by flattening list_pages().
///
/// This trait is automatically implemented for all ObjectStore types and provides
/// a convenient way to get a flat stream of keys from the paginated list_pages() API.
///
/// DO NOT IMPLEMENT THIS TRAIT MANUALLY. It is automatically provided via a blanket
/// implementation for all types that implement ObjectStore.
///
/// # Why this exists
///
/// The list_pages() method returns pages of keys with continuation tokens, which
/// enables true streaming and resumable listings. However, many use cases just want
/// a flat stream of keys without dealing with pagination.
///
/// This extension trait bridges the gap by providing list_stream_ext() that internally
/// calls list_pages() and flattens the results into a single stream.
///
/// # Lifetime behavior
///
/// The stream returned by list_stream_ext() has a lifetime tied to the ObjectStore
/// borrow ('a). For spawned tasks that need 'static lifetimes, use ListingContextOwned
/// instead.
pub trait ObjectStoreListStreamExt: ObjectStore {
    /// List objects with a prefix, returning a flat stream of keys.
    ///
    /// This is a convenience method that calls list_pages() and flattens the results.
    /// For more control over pagination and resumption, use list_pages() directly.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Only return objects with this prefix
    /// * `options` - Page size and other listing options (optional, defaults to DEFAULT_PAGE_SIZE)
    ///
    /// # Returns
    ///
    /// A stream of object keys. Keys are yielded in the order they appear in the
    /// backend's listing (which may not be lexicographic).
    fn list_stream_ext<'a>(
        &'a self,
        prefix: &str,
        options: Option<ListingOptions>,
    ) -> Pin<Box<dyn Stream<Item = StorageResult<String>> + Send + 'a>> {
        let options = options.unwrap_or_default();
        let page_stream = self.list_pages(prefix, options, None);

        // Flatten pages into a stream of keys
        let key_stream = page_stream.flat_map(|page_result| {
            match page_result {
                Ok(page) => {
                    // Convert Vec<String> to a stream
                    futures::stream::iter(page.keys.into_iter().map(Ok)).boxed()
                }
                Err(e) => {
                    // Propagate errors as a single-item stream
                    futures::stream::once(async move { Err(e) }).boxed()
                }
            }
        });

        Box::pin(key_stream)
    }
}

// Blanket implementation for all ObjectStore types
impl<T: ObjectStore + ?Sized> ObjectStoreListStreamExt for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_continuation_token_from_base64_rejects_oversized_input() {
        // Create a base64 string that exceeds MAX_BASE64_INPUT (4096 bytes).
        // Note: 5000 base64 chars would decode to ~3750 bytes, which exceeds MAX_TOKEN_SIZE (2048).
        // This test verifies we reject based on input size BEFORE decoding.
        let huge_base64 = "A".repeat(5000);

        let result = ContinuationToken::from_base64(&huge_base64);

        // Should fail with input size check, NOT after decoding
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("base64 too large"));
    }

    #[test]
    fn test_continuation_token_from_base64_valid() {
        // Valid small token should work
        let data = vec![1, 2, 3, 4, 5];
        let token = ContinuationToken::new(data.clone()).unwrap();
        let base64 = token.to_base64();

        let decoded = ContinuationToken::from_base64(&base64).unwrap();
        assert_eq!(decoded.as_bytes(), &data);
    }
}
