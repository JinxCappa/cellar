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

    /// Atomically get session with exclusive lock and transition to 'committing' state.
    /// Returns None if session doesn't exist.
    /// Returns Some(session) with state='committing' if the transition succeeded (session was 'open').
    /// Returns Some(session) with original state if the transition failed (session was not 'open').
    /// Callers must check the returned session's state field to determine if the transition succeeded.
    async fn begin_commit_session(
        &self,
        upload_id: Uuid,
        updated_at: OffsetDateTime,
    ) -> MetadataResult<Option<UploadSessionRow>>;

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
    async fn set_manifest_hash(&self, upload_id: Uuid, manifest_hash: &str) -> MetadataResult<()>;

    /// Add expected chunks for an upload.
    async fn add_expected_chunks(&self, chunks: &[UploadExpectedChunkRow]) -> MetadataResult<()>;

    /// Mark a chunk as received.
    ///
    /// `uploaded_data` should be `true` if actual chunk data was uploaded, or `false`
    /// if the chunk was just marked received via dedup shortcircuit (data already exists).
    /// This distinction is used to prevent cross-session chunk reference attacks.
    ///
    /// `chunks_predeclared` should be `true` if the upload session had expected_chunks
    /// declared at creation. When true, this method will refuse to insert new rows
    /// (only update existing expected chunk rows), providing defense-in-depth against
    /// undeclared chunk injection.
    async fn mark_chunk_received(
        &self,
        upload_id: Uuid,
        chunk_hash: &str,
        received_at: OffsetDateTime,
        uploaded_data: bool,
        chunks_predeclared: bool,
    ) -> MetadataResult<()>;

    /// Get chunk hashes that were only marked via dedup (not actually uploaded in this session).
    /// Used to validate manifest references when require_expected_chunks=false.
    async fn get_deduped_only_chunks(&self, upload_id: Uuid) -> MetadataResult<Vec<String>>;

    /// Get expected chunks for an upload.
    async fn get_expected_chunks(
        &self,
        upload_id: Uuid,
    ) -> MetadataResult<Vec<UploadExpectedChunkRow>>;

    /// Get a single expected chunk by hash for an upload.
    /// Returns None if no expected chunk with the given hash exists for this upload.
    async fn get_expected_chunk(
        &self,
        upload_id: Uuid,
        chunk_hash: &str,
    ) -> MetadataResult<Option<UploadExpectedChunkRow>>;

    /// Get received chunk hashes for an upload.
    async fn get_received_chunks(&self, upload_id: Uuid) -> MetadataResult<Vec<String>>;

    /// Get missing chunk hashes for an upload.
    async fn get_missing_chunks(&self, upload_id: Uuid) -> MetadataResult<Vec<String>>;

    /// Get expired sessions, scoped to cache_id for tenant isolation.
    /// If cache_id is None, returns expired sessions for the global/public cache only.
    async fn get_expired_sessions(
        &self,
        cache_id: Option<Uuid>,
        limit: u32,
    ) -> MetadataResult<Vec<UploadSessionRow>>;

    /// Get sessions stuck in 'committing' state (for background recovery), scoped to cache_id.
    /// Returns sessions that have been in 'committing' state and not updated since `older_than`.
    /// If cache_id is None, returns stuck sessions for the global/public cache only.
    async fn get_stuck_committing_sessions(
        &self,
        cache_id: Option<Uuid>,
        older_than: OffsetDateTime,
        limit: u32,
    ) -> MetadataResult<Vec<UploadSessionRow>>;

    /// Check if there are any active upload sessions since `since_time`.
    /// Used by storage sweep to avoid deleting objects during in-flight uploads.
    async fn has_active_sessions_since(&self, since_time: OffsetDateTime) -> MetadataResult<bool>;

    /// Check if there are any active upload sessions for a cache since `since_time`.
    /// Used by cache-scoped storage sweep to avoid deleting chunks for active uploads.
    async fn has_active_sessions_for_cache_since(
        &self,
        cache_id: Uuid,
        since_time: OffsetDateTime,
    ) -> MetadataResult<bool>;

    /// Check if a chunk hash is in expected_chunks for any active sessions since `since_time`.
    /// Used by storage sweep to protect chunks during active uploads from being deleted.
    async fn is_chunk_expected_in_active_sessions_since(
        &self,
        chunk_hash: &str,
        since_time: OffsetDateTime,
    ) -> MetadataResult<bool>;

    /// Delete a session and its expected chunks.
    async fn delete_session(&self, upload_id: Uuid) -> MetadataResult<()>;

    /// Count active upload sessions for a cache (or all if cache_id is None).
    async fn count_active_uploads(&self, cache_id: Option<Uuid>) -> MetadataResult<u64>;

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

    /// Update commit progress tracking (for stuck detection and recovery).
    /// Updates both `commit_progress` (JSON tracking state) and `updated_at` (staleness detection).
    /// This keeps the session "alive" during long commits and enables refcount rollback on failure.
    async fn update_commit_progress(
        &self,
        upload_id: Uuid,
        progress_json: &str,
        updated_at: OffsetDateTime,
    ) -> MetadataResult<()>;
}
