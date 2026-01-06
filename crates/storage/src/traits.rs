//! Storage trait definitions.

use crate::error::StorageResult;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;

/// A boxed stream of bytes for streaming reads.
pub type ByteStream = Pin<Box<dyn Stream<Item = StorageResult<Bytes>> + Send>>;

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

    /// Copy an object.
    async fn copy(&self, from: &str, to: &str) -> StorageResult<()>;
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
