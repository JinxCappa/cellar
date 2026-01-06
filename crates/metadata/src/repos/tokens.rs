//! Token repository.

use crate::error::MetadataResult;
use crate::models::{SigningKeyRow, TokenRow};
use async_trait::async_trait;
use time::OffsetDateTime;
use uuid::Uuid;

/// Repository for token operations.
#[async_trait]
pub trait TokenRepo: Send + Sync {
    /// Create a token.
    async fn create_token(&self, token: &TokenRow) -> MetadataResult<()>;

    /// Get a token by hash.
    async fn get_token_by_hash(&self, token_hash: &str) -> MetadataResult<Option<TokenRow>>;

    /// Get a token by ID.
    async fn get_token(&self, token_id: Uuid) -> MetadataResult<Option<TokenRow>>;

    /// Update last used time.
    async fn touch_token(&self, token_id: Uuid, used_at: OffsetDateTime) -> MetadataResult<()>;

    /// Revoke a token.
    async fn revoke_token(&self, token_id: Uuid, revoked_at: OffsetDateTime) -> MetadataResult<()>;

    /// Delete a token.
    async fn delete_token(&self, token_id: Uuid) -> MetadataResult<()>;

    /// List tokens for a cache.
    async fn list_tokens(&self, cache_id: Option<Uuid>) -> MetadataResult<Vec<TokenRow>>;

    /// Count tokens for a cache (or all if cache_id is None).
    async fn count_tokens_for_cache(&self, cache_id: Option<Uuid>) -> MetadataResult<u64>;

    /// Create a signing key.
    async fn create_signing_key(&self, key: &SigningKeyRow) -> MetadataResult<()>;

    /// Get active signing key.
    async fn get_active_signing_key(
        &self,
        cache_id: Option<Uuid>,
    ) -> MetadataResult<Option<SigningKeyRow>>;

    /// Get signing key by ID.
    async fn get_signing_key(&self, key_id: Uuid) -> MetadataResult<Option<SigningKeyRow>>;

    /// Get signing key by name.
    async fn get_signing_key_by_name(
        &self,
        key_name: &str,
    ) -> MetadataResult<Option<SigningKeyRow>>;

    /// Update signing key status.
    async fn update_signing_key_status(&self, key_id: Uuid, status: &str) -> MetadataResult<()>;

    /// List signing keys.
    async fn list_signing_keys(&self, cache_id: Option<Uuid>)
    -> MetadataResult<Vec<SigningKeyRow>>;

    /// Delete all signing keys for a cache (cascade cleanup).
    async fn delete_signing_keys_for_cache(&self, cache_id: Uuid) -> MetadataResult<u64>;
}
