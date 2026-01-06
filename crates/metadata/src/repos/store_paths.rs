//! Store path repository.

use crate::error::MetadataResult;
use crate::models::{NarInfoRow, SignatureRow, StorePathReferenceRow, StorePathRow};
use async_trait::async_trait;
use uuid::Uuid;

/// Repository for store path operations.
#[async_trait]
pub trait StorePathRepo: Send + Sync {
    /// Create a store path record.
    async fn create_store_path(&self, store_path: &StorePathRow) -> MetadataResult<()>;

    /// Get a store path by hash, scoped to cache_id for tenant isolation.
    /// If cache_id is None, returns store paths from the public cache only (cache_id IS NULL).
    /// Cross-cache queries are not supported to maintain tenant isolation.
    async fn get_store_path(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
    ) -> MetadataResult<Option<StorePathRow>>;

    /// Check if a store path exists and is visible, scoped to cache_id.
    /// If cache_id is None, checks the public cache only (cache_id IS NULL).
    async fn store_path_visible(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
    ) -> MetadataResult<bool>;

    /// Update visibility state, scoped to cache_id.
    async fn update_visibility(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
        visibility: &str,
    ) -> MetadataResult<()>;

    /// Complete reupload by atomically updating visibility to 'visible' and new metadata.
    /// Atomically updates visibility to 'visible' and replaces metadata (nar_hash/size/manifest) for reupload completion.
    /// Can be called when the existing state is 'visible' (for reuploads of already-visible paths).
    async fn complete_reupload(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
        nar_hash: &str,
        nar_size: i64,
        manifest_hash: &str,
    ) -> MetadataResult<()>;

    /// Create or update narinfo record.
    async fn upsert_narinfo(&self, narinfo: &NarInfoRow) -> MetadataResult<()>;

    /// Get narinfo record, scoped to cache_id.
    async fn get_narinfo(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
    ) -> MetadataResult<Option<NarInfoRow>>;

    /// Add a signature for a store path, scoped to cache_id.
    async fn add_signature(&self, signature: &SignatureRow) -> MetadataResult<()>;

    /// Get signatures for a store path, scoped to cache_id.
    async fn get_signatures(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
    ) -> MetadataResult<Vec<SignatureRow>>;

    /// Delete a store path and related records, scoped to cache_id.
    async fn delete_store_path(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
    ) -> MetadataResult<()>;

    /// Get store paths by manifest hash, optionally scoped to cache_id.
    /// If cache_id is None, returns store paths from the public cache only (cache_id IS NULL).
    /// Cross-cache queries are not supported to maintain tenant isolation.
    async fn get_store_paths_by_manifest(
        &self,
        cache_id: Option<Uuid>,
        manifest_hash: &str,
    ) -> MetadataResult<Vec<StorePathRow>>;

    /// Get total store path count for a specific cache.
    async fn count_store_paths(&self, cache_id: Option<Uuid>) -> MetadataResult<u64>;

    // F-003: Store path reference methods

    /// Add references for a store path, scoped to cache_id.
    async fn add_references(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
        refs: &[StorePathReferenceRow],
    ) -> MetadataResult<()>;

    /// Get references for a store path, scoped to cache_id.
    async fn get_references(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
    ) -> MetadataResult<Vec<String>>;

    /// Get the deriver for a store path, scoped to cache_id.
    async fn get_deriver(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
    ) -> MetadataResult<Option<String>>;

    /// Get all store paths that reference this hash (reverse lookup), scoped to cache_id.
    /// If cache_id is None, returns referrers only from public caches (cache_id IS NULL).
    /// Admin/GC operations should use a separate admin-only method if cross-cache visibility is needed.
    async fn get_referrers(
        &self,
        cache_id: Option<Uuid>,
        reference_hash: &str,
    ) -> MetadataResult<Vec<String>>;

    /// Delete all references for a store path, scoped to cache_id.
    async fn delete_references(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
    ) -> MetadataResult<()>;

    /// Delete failed store paths older than the specified duration, scoped to cache_id.
    /// Returns the number of deleted store paths.
    /// This is used by GC to clean up failed uploads after a grace period.
    /// If cache_id is None, only deletes failed store paths for the global/public cache.
    async fn delete_failed_store_paths_older_than(
        &self,
        cache_id: Option<Uuid>,
        age_seconds: i64,
    ) -> MetadataResult<u64>;

    /// Mark a store path as having all chunks verified in storage.
    /// This should be called after successful upload when all chunks have been confirmed
    /// to exist in storage. Used to prevent serving truncated NARs.
    async fn mark_chunks_verified(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
    ) -> MetadataResult<()>;
}
