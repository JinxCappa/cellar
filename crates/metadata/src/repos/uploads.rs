//! Upload session repository.

use crate::error::MetadataResult;
use crate::models::{UploadExpectedChunkRow, UploadSessionRow};
use async_trait::async_trait;
use time::OffsetDateTime;
use uuid::Uuid;

/// Repository for upload session operations.
#[async_trait]
pub trait UploadRepo: Send + Sync {
    /// Create a new upload session.
    async fn create_session(&self, session: &UploadSessionRow) -> MetadataResult<()>;

    /// Get an upload session by ID.
    async fn get_session(&self, upload_id: Uuid) -> MetadataResult<Option<UploadSessionRow>>;

    /// Get an upload session by store path hash (for resume).
    /// Filters by cache_id to ensure tenant isolation.
    async fn get_session_by_store_path(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
    ) -> MetadataResult<Option<UploadSessionRow>>;

    /// Update session state.
    async fn update_state(
        &self,
        upload_id: Uuid,
        state: &str,
        updated_at: OffsetDateTime,
    ) -> MetadataResult<()>;

    /// Set the manifest hash for a session.
    async fn set_manifest_hash(
        &self,
        upload_id: Uuid,
        manifest_hash: &str,
    ) -> MetadataResult<()>;

    /// Add expected chunks for an upload.
    async fn add_expected_chunks(
        &self,
        chunks: &[UploadExpectedChunkRow],
    ) -> MetadataResult<()>;

    /// Mark a chunk as received.
    async fn mark_chunk_received(
        &self,
        upload_id: Uuid,
        chunk_hash: &str,
        received_at: OffsetDateTime,
    ) -> MetadataResult<()>;

    /// Get expected chunks for an upload.
    async fn get_expected_chunks(
        &self,
        upload_id: Uuid,
    ) -> MetadataResult<Vec<UploadExpectedChunkRow>>;

    /// Get received chunk hashes for an upload.
    async fn get_received_chunks(&self, upload_id: Uuid) -> MetadataResult<Vec<String>>;

    /// Get missing chunk hashes for an upload.
    async fn get_missing_chunks(&self, upload_id: Uuid) -> MetadataResult<Vec<String>>;

    /// Get expired sessions.
    async fn get_expired_sessions(&self, limit: u32) -> MetadataResult<Vec<UploadSessionRow>>;

    /// Delete a session and its expected chunks.
    async fn delete_session(&self, upload_id: Uuid) -> MetadataResult<()>;

    // F-004: Error tracking methods

    /// Fail a session with error details.
    async fn fail_session(
        &self,
        upload_id: Uuid,
        error_code: &str,
        error_detail: Option<&str>,
        failed_at: OffsetDateTime,
    ) -> MetadataResult<()>;

    /// Get failed sessions by error code.
    async fn get_failed_sessions_by_error(
        &self,
        error_code: &str,
        limit: u32,
    ) -> MetadataResult<Vec<UploadSessionRow>>;
}
