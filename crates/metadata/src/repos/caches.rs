//! Cache repository trait for multi-tenant scoping (F-001).

use crate::error::MetadataResult;
use crate::models::CacheRow;
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

    /// List all caches.
    async fn list_caches(&self) -> MetadataResult<Vec<CacheRow>>;
}
