//! Tombstone repository trait for soft-delete tracking (F-005).

use crate::error::MetadataResult;
use crate::models::TombstoneRow;
use async_trait::async_trait;
use time::OffsetDateTime;
use uuid::Uuid;

/// Repository for tombstone management.
#[async_trait]
pub trait TombstoneRepo: Send + Sync {
    /// Create a tombstone record for a soft-deleted entity.
    async fn create_tombstone(&self, tombstone: &TombstoneRow) -> MetadataResult<()>;

    /// Get a tombstone by entity type and ID.
    async fn get_tombstone(
        &self,
        entity_type: &str,
        entity_id: &str,
    ) -> MetadataResult<Option<TombstoneRow>>;

    /// Check if an entity is tombstoned.
    async fn is_tombstoned(&self, entity_type: &str, entity_id: &str) -> MetadataResult<bool>;

    /// Get tombstones that are eligible for garbage collection.
    async fn get_gc_eligible_tombstones(&self, limit: u32) -> MetadataResult<Vec<TombstoneRow>>;

    /// Mark a tombstone as GC completed.
    async fn mark_gc_completed(
        &self,
        tombstone_id: Uuid,
        completed_at: OffsetDateTime,
    ) -> MetadataResult<()>;

    /// Delete a tombstone after hard deletion is complete.
    async fn delete_tombstone(&self, tombstone_id: Uuid) -> MetadataResult<()>;

    /// Delete all tombstones for a cache (cascade cleanup).
    async fn delete_tombstones_for_cache(&self, cache_id: Uuid) -> MetadataResult<u64>;
}
