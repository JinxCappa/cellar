//! Manifest repository.

use crate::error::MetadataResult;
use crate::models::{ManifestChunkRow, ManifestRow};
use async_trait::async_trait;

/// Repository for manifest operations.
#[async_trait]
pub trait ManifestRepo: Send + Sync {
    /// Create a manifest with its chunk mappings.
    async fn create_manifest(
        &self,
        manifest: &ManifestRow,
        chunks: &[ManifestChunkRow],
    ) -> MetadataResult<()>;

    /// Get a manifest by hash.
    async fn get_manifest(&self, manifest_hash: &str) -> MetadataResult<Option<ManifestRow>>;

    /// Check if a manifest exists.
    async fn manifest_exists(&self, manifest_hash: &str) -> MetadataResult<bool>;

    /// Get chunk hashes for a manifest in order.
    async fn get_manifest_chunks(&self, manifest_hash: &str) -> MetadataResult<Vec<String>>;

    /// Get manifests that reference a chunk.
    async fn get_manifests_using_chunk(&self, chunk_hash: &str) -> MetadataResult<Vec<String>>;

    /// Delete a manifest and its chunk mappings.
    async fn delete_manifest(&self, manifest_hash: &str) -> MetadataResult<()>;

    /// Get orphaned manifests (not referenced by any store path).
    async fn get_orphaned_manifests(&self, limit: u32) -> MetadataResult<Vec<ManifestRow>>;
}
