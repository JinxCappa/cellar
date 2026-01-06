//! Bootstrap token marker repository.

use crate::error::MetadataResult;
use async_trait::async_trait;
use uuid::Uuid;

/// Repository for bootstrap token marker operations.
#[async_trait]
pub trait BootstrapRepo: Send + Sync {
    /// Get the active bootstrap token ID, if set.
    async fn get_bootstrap_token_id(&self) -> MetadataResult<Option<Uuid>>;

    /// Set the active bootstrap token ID.
    async fn set_bootstrap_token_id(&self, token_id: Uuid) -> MetadataResult<()>;

    /// Clear the bootstrap token ID marker.
    async fn clear_bootstrap_token_id(&self) -> MetadataResult<()>;
}
