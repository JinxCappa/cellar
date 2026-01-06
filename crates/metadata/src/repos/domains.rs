//! Storage domain repository.

use crate::error::MetadataResult;
use crate::models::DomainRow;
use async_trait::async_trait;
use uuid::Uuid;

/// Domain usage counts across metadata tables.
#[derive(Debug, Clone, Default)]
pub struct DomainUsage {
    pub caches: u64,
    pub upload_sessions: u64,
    pub store_paths: u64,
    pub chunks: u64,
    pub manifests: u64,
}

/// Repository for storage domain operations.
#[async_trait]
pub trait DomainRepo: Send + Sync {
    /// Create a new storage domain.
    async fn create_domain(&self, domain: &DomainRow) -> MetadataResult<()>;

    /// List all storage domains.
    async fn list_domains(&self) -> MetadataResult<Vec<DomainRow>>;

    /// Get a domain by ID.
    async fn get_domain(&self, domain_id: Uuid) -> MetadataResult<Option<DomainRow>>;

    /// Get a domain by name.
    async fn get_domain_by_name(&self, domain_name: &str) -> MetadataResult<Option<DomainRow>>;

    /// Update a domain record (name and timestamps).
    async fn update_domain(&self, domain: &DomainRow) -> MetadataResult<()>;

    /// Delete a domain by ID.
    async fn delete_domain(&self, domain_id: Uuid) -> MetadataResult<()>;

    /// Get usage counts for a domain.
    async fn get_domain_usage(&self, domain_id: Uuid) -> MetadataResult<DomainUsage>;
}
