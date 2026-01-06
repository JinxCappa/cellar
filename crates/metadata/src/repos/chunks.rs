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
    async fn get_chunk(
        &self,
        domain_id: Uuid,
        chunk_hash: &str,
    ) -> MetadataResult<Option<ChunkRow>>;

    /// Get multiple chunks by their hashes in a single query.
    ///
    /// This is an optimization for operations that need to look up many chunks,
    /// reducing N database round-trips to 1. Returns a HashMap for O(1) lookups.
    ///
    /// Chunks that don't exist are simply not included in the result.
    async fn get_chunks_batch(
        &self,
        domain_id: Uuid,
        chunk_hashes: &[String],
    ) -> MetadataResult<std::collections::HashMap<String, ChunkRow>>;

    /// Check if a chunk exists.
    async fn chunk_exists(&self, domain_id: Uuid, chunk_hash: &str) -> MetadataResult<bool>;

    /// Increment chunk reference count.
    async fn increment_refcount(&self, domain_id: Uuid, chunk_hash: &str) -> MetadataResult<()>;

    /// Decrement chunk reference count.
    async fn decrement_refcount(&self, domain_id: Uuid, chunk_hash: &str) -> MetadataResult<()>;

    /// Batch increment chunk reference counts atomically within a domain.
    ///
    /// This method increments refcounts for multiple chunks in a single transaction,
    /// preventing partial increments if the server crashes mid-commit.
    ///
    /// All increments succeed or all fail together (atomic transaction).
    /// This eliminates the need for CommittingGc recovery of partial refcount increments.
    ///
    /// # Arguments
    /// * `domain_id` - Storage domain ID to scope the refcounts to
    /// * `chunk_hashes` - Slice of chunk hashes to increment
    ///
    /// # Returns
    /// * `Ok(())` if all refcounts were incremented successfully
    /// * `Err` if the transaction failed (no refcounts are incremented)
    async fn batch_increment_refcounts(
        &self,
        domain_id: Uuid,
        chunk_hashes: &[String],
    ) -> MetadataResult<()>;

    /// Update last accessed time.
    async fn touch_chunk(
        &self,
        domain_id: Uuid,
        chunk_hash: &str,
        accessed_at: OffsetDateTime,
    ) -> MetadataResult<()>;

    /// Get chunks with zero refcount older than the given time.
    async fn get_unreferenced_chunks(
        &self,
        domain_id: Uuid,
        older_than: OffsetDateTime,
        limit: u32,
    ) -> MetadataResult<Vec<ChunkRow>>;

    /// Atomically delete unreferenced chunks from metadata and return the deleted chunks.
    ///
    /// This method prevents the Time-Of-Check-Time-Of-Use race where:
    /// 1. GC selects chunks with refcount=0
    /// 2. Concurrent commit increments refcount
    /// 3. GC deletes chunk anyway (data loss!)
    ///
    /// **Atomic deletion** ensures locks are held from SELECT through DELETE:
    /// - **Postgres**: Uses CTE with `FOR UPDATE SKIP LOCKED` in same transaction as DELETE.
    ///   Row locks are held until DELETE completes, preventing concurrent modifications.
    /// - **SQLite**: Wraps SELECT and DELETE in explicit transaction with `BEGIN IMMEDIATE`.
    ///   Exclusive write lock prevents all concurrent writes until COMMIT.
    ///
    /// Returns the list of chunks that were successfully deleted from metadata.
    /// Storage cleanup (deleting actual chunk data) should happen AFTER this call,
    /// allowing orphaned storage objects to be cleaned up on the next GC run if deletion fails.
    ///
    /// CRITICAL FIX: Now correlates chunks to sessions by creation time, protecting
    /// chunks uploaded during any active session's lifetime. This fixes the bug where
    /// uncorrelated queries blocked ALL chunk deletion when ANY upload was active.
    async fn delete_unreferenced_chunks_atomic(
        &self,
        domain_id: Uuid,
        older_than: OffsetDateTime,
        limit: u32,
    ) -> MetadataResult<Vec<ChunkRow>>;

    /// Delete a chunk record.
    async fn delete_chunk(&self, domain_id: Uuid, chunk_hash: &str) -> MetadataResult<()>;

    /// Get total chunk count and size.
    async fn get_stats(&self, domain_id: Uuid) -> MetadataResult<ChunkStats>;
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
