//! Manifest repository.

use crate::error::MetadataResult;
use crate::models::{ManifestChunkRow, ManifestRow};
use async_trait::async_trait;
use time::OffsetDateTime;
use uuid::Uuid;

/// Repository for manifest operations.
#[async_trait]
pub trait ManifestRepo: Send + Sync {
    /// Create a manifest with its chunk mappings.
    ///
    /// Returns `Ok(true)` if the manifest was created, `Ok(false)` if it already existed.
    /// This uses INSERT ON CONFLICT DO NOTHING to handle concurrent creates atomically.
    async fn create_manifest(
        &self,
        manifest: &ManifestRow,
        chunks: &[ManifestChunkRow],
    ) -> MetadataResult<bool>;

    /// Create a manifest with its chunk mappings AND atomically initialize domain-scoped refcount.
    ///
    /// This method combines manifest creation and refcount initialization in a single transaction,
    /// eliminating the race condition where GC could delete a manifest between creation and
    /// refcount increment (P1 Issue #4).
    ///
    /// The transaction performs:
    /// 1. INSERT manifest (DO NOTHING on conflict to preserve original created_at)
    /// 2. INSERT manifest_chunks (only if newly created)
    /// 3. Increment manifest refcount (always, even if manifest already existed)
    ///
    /// Returns `Ok(true)` if the manifest was created, `Ok(false)` if it already existed.
    async fn create_manifest_with_initial_refcount(
        &self,
        manifest: &ManifestRow,
        chunks: &[ManifestChunkRow],
    ) -> MetadataResult<bool>;

    /// Get a manifest by hash.
    async fn get_manifest(
        &self,
        domain_id: Uuid,
        manifest_hash: &str,
    ) -> MetadataResult<Option<ManifestRow>>;

    /// Check if a manifest exists.
    async fn manifest_exists(&self, domain_id: Uuid, manifest_hash: &str) -> MetadataResult<bool>;

    /// Get chunk hashes for a manifest in order.
    async fn get_manifest_chunks(
        &self,
        domain_id: Uuid,
        manifest_hash: &str,
    ) -> MetadataResult<Vec<String>>;

    /// Get chunk hashes for a manifest in order, verifying all chunks exist in storage.
    ///
    /// This combines the manifest chunk lookup with a verification that all referenced
    /// chunks exist in the `chunks` table with `refcount > 0`. This prevents streaming
    /// NAR data to clients when chunks are missing, which would result in truncated data.
    ///
    /// Returns `Ok(chunk_hashes)` if all chunks exist, or an error with the list of
    /// missing chunk hashes if any are not found.
    async fn get_manifest_chunks_verified(
        &self,
        domain_id: Uuid,
        manifest_hash: &str,
    ) -> MetadataResult<Vec<String>>;

    /// Get manifests that reference a chunk.
    async fn get_manifests_using_chunk(
        &self,
        domain_id: Uuid,
        chunk_hash: &str,
    ) -> MetadataResult<Vec<String>>;

    /// Delete a manifest and its chunk mappings.
    async fn delete_manifest(&self, domain_id: Uuid, manifest_hash: &str) -> MetadataResult<()>;

    /// Delete a manifest and atomically decrement domain-scoped refcounts for all its chunks.
    ///
    /// This method wraps the entire operation in a transaction:
    /// 1. Get all chunk hashes for the manifest
    /// 2. Decrement chunk refcounts for each chunk (domain-scoped)
    /// 3. Delete manifest_chunks rows
    /// 4. Delete manifest row
    ///
    /// Returns the number of chunks whose refcounts were decremented.
    ///
    /// If any step fails, the entire transaction is rolled back, preventing refcount
    /// corruption and orphaned manifest/chunk mappings.
    async fn delete_manifest_with_refcount_decrement(
        &self,
        domain_id: Uuid,
        manifest_hash: &str,
    ) -> MetadataResult<u64>;

    /// Get orphaned manifests (refcount = 0) within a domain.
    async fn get_orphaned_manifests(
        &self,
        domain_id: Uuid,
        older_than: OffsetDateTime,
        limit: u32,
    ) -> MetadataResult<Vec<ManifestRow>>;

    /// Increment manifest reference count within a domain.
    async fn increment_manifest_refcount(
        &self,
        domain_id: Uuid,
        manifest_hash: &str,
    ) -> MetadataResult<()>;

    /// Decrement manifest reference count within a domain.
    async fn decrement_manifest_refcount(
        &self,
        domain_id: Uuid,
        manifest_hash: &str,
    ) -> MetadataResult<()>;
}
