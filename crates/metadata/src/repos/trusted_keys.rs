//! Trusted builder keys repository trait (F-002).

use crate::error::MetadataResult;
use crate::models::TrustedBuilderKeyRow;
use async_trait::async_trait;
use time::OffsetDateTime;
use uuid::Uuid;

/// Repository for trusted builder key management.
#[async_trait]
pub trait TrustedKeyRepo: Send + Sync {
    /// Add a trusted builder key.
    async fn add_trusted_key(&self, key: &TrustedBuilderKeyRow) -> MetadataResult<()>;

    /// Get a trusted key by ID.
    async fn get_trusted_key(&self, key_id: Uuid) -> MetadataResult<Option<TrustedBuilderKeyRow>>;

    /// Get a trusted key by name, optionally scoped to a cache.
    async fn get_trusted_key_by_name(
        &self,
        cache_id: Option<Uuid>,
        key_name: &str,
    ) -> MetadataResult<Option<TrustedBuilderKeyRow>>;

    /// List all trusted keys for a cache (or global if cache_id is None).
    async fn list_trusted_keys(
        &self,
        cache_id: Option<Uuid>,
    ) -> MetadataResult<Vec<TrustedBuilderKeyRow>>;

    /// Revoke a trusted key.
    async fn revoke_trusted_key(
        &self,
        key_id: Uuid,
        revoked_at: OffsetDateTime,
    ) -> MetadataResult<()>;

    /// Check if a signature from a given key name is trusted.
    async fn is_signature_trusted(
        &self,
        cache_id: Option<Uuid>,
        key_name: &str,
    ) -> MetadataResult<bool>;

    /// Delete all trusted keys for a cache (cascade cleanup).
    async fn delete_trusted_keys_for_cache(&self, cache_id: Uuid) -> MetadataResult<u64>;
}
