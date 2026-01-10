//! Chunk repository.

use crate::error::MetadataResult;
use crate::models::ChunkRow;
use async_trait::async_trait;
use time::OffsetDateTime;
use uuid::Uuid;

/// Repository for chunk operations.
#[async_trait]
pub trait ChunkRepo: Send + Sync {
    /// Create or update a chunk record.
    async fn upsert_chunk(&self, chunk: &ChunkRow) -> MetadataResult<()>;

    /// Get a chunk by hash.
    async fn get_chunk(&self, chunk_hash: &str) -> MetadataResult<Option<ChunkRow>>;

    /// Check if a chunk exists.
    async fn chunk_exists(&self, chunk_hash: &str) -> MetadataResult<bool>;

    /// Increment chunk reference count.
    async fn increment_refcount(&self, chunk_hash: &str) -> MetadataResult<()>;

    /// Decrement chunk reference count.
    async fn decrement_refcount(&self, chunk_hash: &str) -> MetadataResult<()>;

    /// Increment per-cache chunk reference count.
    /// This also increments the global refcount.
    async fn increment_cache_refcount(
        &self,
        cache_id: Option<Uuid>,
        chunk_hash: &str,
    ) -> MetadataResult<()>;

    /// Decrement per-cache chunk reference count.
    /// This also decrements the global refcount.
    async fn decrement_cache_refcount(
        &self,
        cache_id: Option<Uuid>,
        chunk_hash: &str,
    ) -> MetadataResult<()>;

    /// Update last accessed time.
    async fn touch_chunk(&self, chunk_hash: &str, accessed_at: OffsetDateTime) -> MetadataResult<()>;

    /// Get chunks with zero refcount older than the given time.
    async fn get_unreferenced_chunks(
        &self,
        older_than: OffsetDateTime,
        limit: u32,
    ) -> MetadataResult<Vec<ChunkRow>>;

    /// Delete a chunk record.
    async fn delete_chunk(&self, chunk_hash: &str) -> MetadataResult<()>;

    /// Get total chunk count and size.
    async fn get_stats(&self) -> MetadataResult<ChunkStats>;
}

/// Chunk statistics.
#[derive(Debug, Clone, Default)]
pub struct ChunkStats {
    /// Total number of chunks.
    pub count: u64,
    /// Total size in bytes.
    pub total_size: u64,
    /// Number of chunks with refcount > 0.
    pub referenced_count: u64,
    /// Number of chunks with refcount == 0.
    pub unreferenced_count: u64,
}
