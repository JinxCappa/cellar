//! Cache repository trait for multi-tenant scoping (F-001).

use crate::error::MetadataResult;
use crate::models::{CacheRow, CascadeDeleteStats};
use async_trait::async_trait;
use uuid::Uuid;

/// Repository for cache management.
#[async_trait]
pub trait CacheRepo: Send + Sync {
    /// Create a new cache.
    async fn create_cache(&self, cache: &CacheRow) -> MetadataResult<()>;

    /// Get a cache by ID.
    async fn get_cache(&self, cache_id: Uuid) -> MetadataResult<Option<CacheRow>>;

    /// Get a cache by name.
    async fn get_cache_by_name(&self, name: &str) -> MetadataResult<Option<CacheRow>>;

    /// Update an existing cache.
    async fn update_cache(&self, cache: &CacheRow) -> MetadataResult<()>;

    /// Delete a cache by ID.
    async fn delete_cache(&self, cache_id: Uuid) -> MetadataResult<()>;

    /// Delete a cache and all associated operational data atomically.
    /// Returns statistics about what was deleted.
    async fn delete_cache_with_cascade(&self, cache_id: Uuid)
    -> MetadataResult<CascadeDeleteStats>;

    /// List all caches.
    async fn list_caches(&self) -> MetadataResult<Vec<CacheRow>>;

    /// Get the default public cache (if one is set).
    /// Used for unauthenticated requests without X-Cache-Name header.
    async fn get_default_public_cache(&self) -> MetadataResult<Option<CacheRow>>;
}
