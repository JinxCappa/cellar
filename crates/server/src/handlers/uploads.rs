//! Upload control plane handlers.

use crate::auth::{get_trace_id, require_auth, AuthenticatedUser};
use crate::error::{ApiError, ApiResult};
use crate::metrics::{
    record_upload_error, BYTES_DEDUPLICATED, BYTES_UPLOADED, CHUNKS_DEDUPLICATED,
    CHUNKS_UPLOADED, CHUNK_HASH_MISMATCHES, CHUNK_UPLOAD_DURATION, UPLOAD_COMMIT_DURATION,
    UPLOAD_SESSIONS_COMMITTED, UPLOAD_SESSIONS_CREATED, UPLOAD_SESSIONS_RESUMED,
};
use crate::state::AppState;
use axum::body::Bytes;
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::Json;
use cellar_core::chunk::ChunkHash;
use cellar_core::hash::{NarHash, NarHasher};
use cellar_core::manifest::{Manifest, ManifestHash};
use cellar_core::store_path::StorePath;
use cellar_core::token::TokenScope;
use cellar_core::upload::{
    CreateUploadRequest, CreateUploadResponse, MissingChunks, UploadStateResponse,
};
use cellar_metadata::models::{
    ChunkRow, ManifestChunkRow, ManifestRow, NarInfoRow, StorePathRow, UploadExpectedChunkRow,
    UploadSessionRow,
};
use serde::Deserialize;
use std::time::Instant;
use time::OffsetDateTime;
use uuid::Uuid;

/// Maximum request body size for create upload requests (1 MiB).
const MAX_CREATE_BODY_SIZE: usize = 1024 * 1024;

/// Generate a cache-prefixed storage key path.
/// For tenant isolation, all NAR and narinfo objects are prefixed with the cache_id.
/// Public caches (cache_id = None) use "public" as the prefix.
fn cache_prefix(cache_id: Option<Uuid>) -> String {
    cache_id
        .map(|id| id.to_string())
        .unwrap_or_else(|| "public".to_string())
}

/// Maximum request body size for commit requests (10 MiB).
const MAX_COMMIT_BODY_SIZE: usize = 10 * 1024 * 1024;

/// Additional buffer for chunk uploads beyond the max chunk size.
const CHUNK_UPLOAD_BUFFER: usize = 1024;

/// POST /v1/uploads - Create a new upload session.
#[tracing::instrument(skip(state, req), fields(store_path))]
pub async fn create_upload(
    State(state): State<AppState>,
    req: Request,
) -> ApiResult<(StatusCode, Json<CreateUploadResponse>)> {
    let trace_id = get_trace_id(&req).cloned().unwrap_or_default();
    let auth = require_auth(&req)?.clone();
    auth.require_scope(TokenScope::CacheWrite)?;

    let body: CreateUploadRequest = {
        let bytes = axum::body::to_bytes(req.into_body(), MAX_CREATE_BODY_SIZE)
            .await
            .map_err(|e| ApiError::BadRequest(format!("failed to read body: {e}")))?;
        serde_json::from_slice(&bytes)
            .map_err(|e| ApiError::BadRequest(format!("invalid JSON: {e}")))?
    };

    // Parse and validate input
    let store_path = StorePath::parse(&body.store_path)?;
    let _nar_hash = NarHash::from_sri(&body.nar_hash)?;
    let chunk_size = body.chunk_size.unwrap_or(state.config.server.default_chunk_size);

    // Record store_path in current span
    tracing::Span::current().record("store_path", store_path.hash().as_str());

    // Validate nar_size is within i64 bounds (for database storage)
    // Values > i64::MAX (~9.2 exabytes) are rejected to prevent overflow
    if body.nar_size > i64::MAX as u64 {
        return Err(ApiError::BadRequest(format!(
            "nar_size {} exceeds maximum supported size {}",
            body.nar_size,
            i64::MAX
        )));
    }

    // Validate chunk_size is within i64 bounds
    if chunk_size > i64::MAX as u64 {
        return Err(ApiError::BadRequest(format!(
            "chunk_size {} exceeds maximum supported size {}",
            chunk_size,
            i64::MAX
        )));
    }

    // Validate chunk size
    if chunk_size < cellar_core::MIN_CHUNK_SIZE || chunk_size > state.config.server.max_chunk_size
    {
        return Err(ApiError::BadRequest(format!(
            "chunk_size must be between {} and {}",
            cellar_core::MIN_CHUNK_SIZE,
            state.config.server.max_chunk_size
        )));
    }

    // Check if an open session already exists for this store path (scoped to cache_id)
    if let Some(existing) = state
        .metadata
        .get_session_by_store_path(auth.token.cache_id, store_path.hash().as_str())
        .await?
    {
        // Check if the existing session has expired
        if existing.expires_at < OffsetDateTime::now_utc() {
            // Session expired - clean it up and create a new one
            tracing::info!(
                upload_id = %existing.upload_id,
                trace_id = %trace_id,
                "Existing session expired, cleaning up"
            );
            state.metadata.delete_session(existing.upload_id).await?;
            // Fall through to create a new session
        } else {
            // Session is still valid - verify parameters match before allowing resume.
            // If parameters changed, the client is starting a new upload for the same
            // store path, so we should invalidate the old session.
            let params_match = existing.nar_hash == body.nar_hash
                && existing.nar_size == body.nar_size as i64
                && existing.chunk_size == chunk_size as i64
                && existing.manifest_hash == body.manifest_hash;

            if !params_match {
                tracing::info!(
                    upload_id = %existing.upload_id,
                    trace_id = %trace_id,
                    "Session parameters changed, invalidating old session"
                );
                state.metadata.delete_session(existing.upload_id).await?;
                // Fall through to create a new session
            } else {
                // Parameters match - verify ownership before allowing resume
                verify_session_ownership(&auth, &existing)?;
                // For resume: check expected chunks (if defined) or received chunks
                let expected = state.metadata.get_expected_chunks(existing.upload_id).await?;

                let missing_chunks = if !expected.is_empty() {
                    // Expected chunks were defined - return actual missing list
                    let missing = state.metadata.get_missing_chunks(existing.upload_id).await?;
                    if missing.is_empty() {
                        MissingChunks::List(vec![]) // All chunks received
                    } else {
                        MissingChunks::List(missing)
                    }
                } else {
                    // No expected chunks defined - check received chunks
                    let received = state.metadata.get_received_chunks(existing.upload_id).await?;
                    if received.is_empty() {
                        MissingChunks::All("all".to_string())
                    } else {
                        // Some progress made but we can't compute missing chunks without
                        // a pre-defined expected list. Return `Unknown` to signal to the client
                        // that it should GET /v1/uploads/{id} to see the received_chunks list
                        // and determine what remains to be uploaded.
                        MissingChunks::Unknown {
                            received_count: received.len(),
                        }
                    }
                };

                // Record metrics and log for resumed session
                UPLOAD_SESSIONS_RESUMED.inc();
                tracing::info!(
                    upload_id = %existing.upload_id,
                    trace_id = %trace_id,
                    "Resumed existing upload session"
                );

                return Ok((
                    StatusCode::OK,
                    Json(CreateUploadResponse {
                        upload_id: existing.upload_id.to_string(),
                        missing_chunks,
                        max_parallel_chunks: state.config.server.max_parallel_chunks,
                    }),
                ));
            }
        }
    }

    // Create new session
    let upload_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();
    let expires_at = now + state.config.server.upload_timeout();

    let session = UploadSessionRow {
        upload_id,
        cache_id: auth.token.cache_id,
        store_path: store_path.to_path_string(),
        store_path_hash: store_path.hash().to_string(),
        nar_size: body.nar_size as i64,
        nar_hash: body.nar_hash.clone(),
        chunk_size: chunk_size as i64,
        manifest_hash: body.manifest_hash.clone(),
        state: "open".to_string(),
        owner_token_id: Some(*auth.token.id.as_uuid()),
        created_at: now,
        updated_at: now,
        expires_at,
        trace_id: Some(trace_id.0.clone()),
        error_code: None,
        error_detail: None,
    };

    state.metadata.create_session(&session).await?;

    // Record metrics and log for new session
    UPLOAD_SESSIONS_CREATED.inc();
    tracing::info!(
        upload_id = %upload_id,
        trace_id = %trace_id,
        nar_size = body.nar_size,
        "Created new upload session"
    );

    // If expected chunks provided, store them for resume tracking
    if let Some(ref expected_chunks) = body.expected_chunks {
        let chunk_rows: Vec<UploadExpectedChunkRow> = expected_chunks
            .iter()
            .enumerate()
            .map(|(i, chunk)| UploadExpectedChunkRow {
                upload_id,
                position: i as i32,
                chunk_hash: chunk.hash.clone(),
                size_bytes: chunk.size as i64,
                received_at: None,
            })
            .collect();

        state.metadata.add_expected_chunks(&chunk_rows).await?;
    }

    // Determine initial missing chunks response
    let missing_chunks = if body.expected_chunks.is_some() {
        // With expected chunks, return the full list as missing
        let missing = state.metadata.get_missing_chunks(upload_id).await?;
        MissingChunks::List(missing)
    } else {
        MissingChunks::All("all".to_string())
    };

    Ok((
        StatusCode::CREATED,
        Json(CreateUploadResponse {
            upload_id: upload_id.to_string(),
            missing_chunks,
            max_parallel_chunks: state.config.server.max_parallel_chunks,
        }),
    ))
}

/// Verify the requesting token owns the upload session.
fn verify_session_ownership(auth: &AuthenticatedUser, session: &UploadSessionRow) -> ApiResult<()> {
    // Verify the session belongs to the same cache as the token.
    // This prevents cross-cache access when owner_token_id is NULL (legacy/manual rows).
    if session.cache_id != auth.token.cache_id {
        return Err(ApiError::Forbidden(
            "upload session belongs to a different cache".to_string(),
        ));
    }

    // If session has an owner token, verify the requesting token matches
    if let Some(owner_id) = session.owner_token_id {
        if *auth.token.id.as_uuid() != owner_id {
            return Err(ApiError::Forbidden(
                "upload session belongs to a different token".to_string(),
            ));
        }
    }
    Ok(())
}

/// GET /v1/uploads/{upload_id} - Query upload state.
#[tracing::instrument(skip(state, req), fields(upload_id = %upload_id))]
pub async fn get_upload(
    State(state): State<AppState>,
    Path(upload_id): Path<String>,
    req: Request,
) -> ApiResult<Json<UploadStateResponse>> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheWrite)?;

    let upload_id =
        Uuid::parse_str(&upload_id).map_err(|e| ApiError::BadRequest(format!("invalid upload ID: {e}")))?;

    let session = state
        .metadata
        .get_session(upload_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("upload session not found".to_string()))?;

    // Verify ownership
    verify_session_ownership(&auth, &session)?;

    // Check expiration
    if session.expires_at < OffsetDateTime::now_utc() {
        return Err(ApiError::UploadExpired);
    }

    let received = state.metadata.get_received_chunks(upload_id).await?;
    let missing = state.metadata.get_missing_chunks(upload_id).await?;

    let expires_at = session
        .expires_at
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|e| ApiError::Internal(format!("failed to format expires_at: {e}")))?;

    Ok(Json(UploadStateResponse {
        received_chunks: received,
        missing_chunks: missing,
        expires_at,
    }))
}

/// PUT /v1/uploads/{upload_id}/chunks/{chunk_hash} - Upload a chunk.
#[tracing::instrument(skip(state, req), fields(upload_id = %upload_id, chunk_hash = %chunk_hash))]
pub async fn upload_chunk(
    State(state): State<AppState>,
    Path((upload_id, chunk_hash)): Path<(String, String)>,
    req: Request,
) -> ApiResult<StatusCode> {
    let start_time = Instant::now();
    let trace_id = get_trace_id(&req).cloned().unwrap_or_default();
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheWrite)?;

    // Normalize chunk hash to lowercase to ensure consistent storage and lookups
    // from_hex accepts both upper/lowercase, but to_hex always returns lowercase
    let chunk_hash = chunk_hash.to_lowercase();

    let upload_id =
        Uuid::parse_str(&upload_id).map_err(|e| ApiError::BadRequest(format!("invalid upload ID: {e}")))?;
    let expected_hash =
        ChunkHash::from_hex(&chunk_hash).map_err(|e| ApiError::BadRequest(format!("invalid chunk hash: {e}")))?;

    // Check session exists and is open
    let session = state
        .metadata
        .get_session(upload_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("upload session not found".to_string()))?;

    // Verify ownership
    verify_session_ownership(&auth, &session)?;

    if session.state != "open" {
        return Err(ApiError::BadRequest(format!(
            "upload session is {}, not open",
            session.state
        )));
    }

    if session.expires_at < OffsetDateTime::now_utc() {
        return Err(ApiError::UploadExpired);
    }

    // Check if chunk already exists in BOTH storage AND metadata (deduplication)
    // We need to verify both to handle partial failures where storage has the chunk
    // but metadata doesn't (which would cause commit to fail with "chunk not found")
    let object_key = expected_hash.to_object_key();
    let storage_exists = state.storage.exists(&object_key).await?;
    let metadata_exists = state.metadata.get_chunk(&chunk_hash).await?.is_some();

    if storage_exists && metadata_exists {
        // Chunk fully exists in both storage and metadata, just mark as received
        state
            .metadata
            .mark_chunk_received(upload_id, &chunk_hash, OffsetDateTime::now_utc())
            .await?;

        // Record deduplication metrics
        CHUNKS_DEDUPLICATED.inc();

        tracing::debug!(
            upload_id = %upload_id,
            chunk_hash = %chunk_hash,
            trace_id = %trace_id,
            "Chunk deduplicated (already exists)"
        );

        CHUNK_UPLOAD_DURATION.observe(start_time.elapsed().as_secs_f64());
        return Ok(StatusCode::OK);
    }

    // If storage has the chunk but metadata doesn't, we need to repair metadata.
    // Fall through to read chunk data, verify hash, and upsert metadata.

    // Read chunk data
    let chunk_data = axum::body::to_bytes(req.into_body(), state.config.server.max_chunk_size as usize + CHUNK_UPLOAD_BUFFER)
        .await
        .map_err(|e| ApiError::BadRequest(format!("failed to read chunk: {e}")))?;

    // Verify hash
    let actual_hash = ChunkHash::compute(&chunk_data);
    if actual_hash != expected_hash {
        // Record hash mismatch metrics and log error
        CHUNK_HASH_MISMATCHES.inc();
        record_upload_error("hash_mismatch");
        tracing::warn!(
            upload_id = %upload_id,
            chunk_hash = %chunk_hash,
            expected_hash = %expected_hash.to_hex(),
            actual_hash = %actual_hash.to_hex(),
            trace_id = %trace_id,
            "Chunk hash verification failed"
        );
        return Err(ApiError::HashMismatch {
            expected: expected_hash.to_hex(),
            actual: actual_hash.to_hex(),
        });
    }

    // Store chunk
    let chunk_size = chunk_data.len() as u64;
    let was_new = state.storage.put_if_not_exists(&object_key, chunk_data.clone()).await?;

    // Determine if we need to upsert metadata. We need to do this if:
    // 1. We just created new storage (was_new = true), OR
    // 2. Storage already existed but metadata is missing (repair case)
    // IMPORTANT: Check metadata existence AFTER put_if_not_exists to avoid TOCTOU race.
    let now = OffsetDateTime::now_utc();
    let needs_metadata_upsert = if was_new {
        // We just created the storage, so we definitely need to create metadata
        true
    } else {
        // Storage already existed. Check if metadata exists (may have been created by another thread)
        state.metadata.get_chunk(&chunk_hash).await?.is_none()
    };

    if was_new || needs_metadata_upsert {
        let chunk_row = ChunkRow {
            chunk_hash: chunk_hash.clone(),
            size_bytes: chunk_size as i64,
            object_key: Some(object_key.clone()),
            refcount: 0,
            created_at: now,
            last_accessed_at: Some(now),
        };

        // If metadata upsert fails and we created new storage, attempt cleanup.
        // Don't cleanup if we're just repairing metadata for existing storage.
        if let Err(e) = state.metadata.upsert_chunk(&chunk_row).await {
            tracing::warn!(
                upload_id = %upload_id,
                chunk_hash = %chunk_hash,
                error = %e,
                "Failed to record chunk in metadata, attempting cleanup"
            );
            // Best-effort cleanup - only if we created new storage
            // Don't delete if we were repairing metadata for existing storage
            if was_new {
                if let Err(cleanup_err) = state.storage.delete(&object_key).await {
                    tracing::warn!(
                        upload_id = %upload_id,
                        chunk_hash = %chunk_hash,
                        error = %cleanup_err,
                        "Failed to clean up orphaned chunk from storage"
                    );
                }
            }
            return Err(e.into());
        }

        if !was_new && needs_metadata_upsert {
            tracing::info!(
                upload_id = %upload_id,
                chunk_hash = %chunk_hash,
                trace_id = %trace_id,
                "Repaired missing chunk metadata for existing storage"
            );
        }

        // Record metrics
        if was_new {
            CHUNKS_UPLOADED.inc();
            BYTES_UPLOADED.inc_by(chunk_size);
        } else {
            // Metadata repair case - count as deduplicated since storage existed
            CHUNKS_DEDUPLICATED.inc();
            BYTES_DEDUPLICATED.inc_by(chunk_size);
        }
    } else {
        // Chunk was already stored by another concurrent upload
        CHUNKS_DEDUPLICATED.inc();
        BYTES_DEDUPLICATED.inc_by(chunk_size);
    }

    // Mark chunk as received for this upload
    state
        .metadata
        .mark_chunk_received(upload_id, &chunk_hash, now)
        .await?;

    // Record timing
    CHUNK_UPLOAD_DURATION.observe(start_time.elapsed().as_secs_f64());

    tracing::debug!(
        upload_id = %upload_id,
        chunk_hash = %chunk_hash,
        size_bytes = chunk_size,
        was_new = was_new,
        trace_id = %trace_id,
        "Chunk uploaded"
    );

    // Always return OK to prevent cross-cache dedup side-channel.
    // Returning CREATED vs OK would leak whether a chunk exists globally,
    // allowing tenants to probe for chunk presence across caches.
    Ok(StatusCode::OK)
}

/// Commit request body.
#[derive(Debug, Deserialize)]
pub struct CommitRequest {
    /// Ordered list of chunk hashes forming the manifest.
    pub manifest: Vec<String>,
}

/// Tracks artifacts created during commit for compensation on failure.
struct CommitArtifacts {
    /// Compressed NAR storage key (if created).
    compressed_nar_key: Option<String>,
    /// Temporary NAR key used during streaming compression.
    temp_nar_key: Option<String>,
    /// Narinfo storage key (if created).
    narinfo_key: Option<String>,
    /// Store path hash (if store path record was created/updated).
    store_path_hash: Option<String>,
    /// Cache ID for the store path.
    cache_id: Option<Uuid>,
    /// Whether this was a re-upload (existing visible path).
    is_reupload: bool,
    /// Chunk hashes with incremented refcounts (for rollback on failure).
    incremented_refcounts: Vec<String>,
}

impl CommitArtifacts {
    fn new() -> Self {
        Self {
            compressed_nar_key: None,
            temp_nar_key: None,
            narinfo_key: None,
            store_path_hash: None,
            cache_id: None,
            is_reupload: false,
            incremented_refcounts: Vec::new(),
        }
    }

    /// Clean up artifacts on commit failure.
    /// This is a best-effort cleanup - failures are logged but don't propagate.
    async fn cleanup(&self, state: &AppState, upload_id: Uuid, trace_id: &str) {
        // Mark session as failed so it can be cleaned up by GC
        // This prevents sessions from being stuck in 'committing' state forever
        if let Err(e) = state.metadata.fail_session(
            upload_id,
            "commit_failed",
            Some("Commit operation failed during artifact creation"),
            OffsetDateTime::now_utc(),
        ).await {
            tracing::warn!(
                upload_id = %upload_id,
                error = %e,
                trace_id = %trace_id,
                "Failed to mark session as failed after commit failure"
            );
        }

        // Rollback incremented refcounts to prevent orphaned chunks
        for chunk_hash in &self.incremented_refcounts {
            if let Err(e) = state.metadata.decrement_refcount(chunk_hash).await {
                tracing::warn!(
                    upload_id = %upload_id,
                    chunk_hash = %chunk_hash,
                    error = %e,
                    trace_id = %trace_id,
                    "Failed to rollback chunk refcount after commit failure"
                );
            }
        }

        // Delete compressed NAR if it was stored
        if let Some(key) = &self.compressed_nar_key {
            if let Err(e) = state.storage.delete(key).await {
                tracing::warn!(
                    upload_id = %upload_id,
                    key = %key,
                    error = %e,
                    trace_id = %trace_id,
                    "Failed to clean up compressed NAR after commit failure"
                );
            }
        }

        // Delete temporary NAR key if streaming compression was in progress
        if let Some(key) = &self.temp_nar_key {
            if let Err(e) = state.storage.delete(key).await {
                tracing::warn!(
                    upload_id = %upload_id,
                    key = %key,
                    error = %e,
                    trace_id = %trace_id,
                    "Failed to clean up temp NAR key after commit failure"
                );
            }
        }

        // Delete narinfo if it was stored
        if let Some(key) = &self.narinfo_key {
            if let Err(e) = state.storage.delete(key).await {
                tracing::warn!(
                    upload_id = %upload_id,
                    key = %key,
                    error = %e,
                    trace_id = %trace_id,
                    "Failed to clean up narinfo after commit failure"
                );
            }
        }

        // Handle store path visibility on failure.
        // Update visibility for both tenant caches (cache_id = Some) and public cache (cache_id = None)
        if let Some(hash) = &self.store_path_hash {
            if self.is_reupload {
                // For reuploads: path already remains 'visible' with old metadata (no action needed).
                // The create_store_path UPSERT preserves visibility='visible' during reuploads,
                // so the original content continues to be served. No cleanup required.
                tracing::debug!(
                    upload_id = %upload_id,
                    store_path_hash = %hash,
                    cache_id = ?self.cache_id,
                    trace_id = %trace_id,
                    "Reupload failed, but original content remains visible (no state change)"
                );
            } else {
                // For new uploads: mark as failed
                if let Err(e) = state.metadata.update_visibility(self.cache_id, hash, "failed").await {
                    tracing::warn!(
                        upload_id = %upload_id,
                        store_path_hash = %hash,
                        cache_id = ?self.cache_id,
                        error = %e,
                        trace_id = %trace_id,
                        "Failed to mark store path as failed after commit failure"
                    );
                }
            }
        }
    }
}

/// POST /v1/uploads/{upload_id}/commit - Atomically commit an upload.
#[tracing::instrument(skip(state, req), fields(upload_id = %upload_id))]
pub async fn commit_upload(
    State(state): State<AppState>,
    Path(upload_id): Path<String>,
    req: Request,
) -> ApiResult<StatusCode> {
    let start_time = Instant::now();
    let trace_id = get_trace_id(&req).cloned().unwrap_or_default();
    let auth = require_auth(&req)?.clone();
    auth.require_scope(TokenScope::CacheWrite)?;

    let upload_id =
        Uuid::parse_str(&upload_id).map_err(|e| ApiError::BadRequest(format!("invalid upload ID: {e}")))?;

    // Parse request body
    let body: CommitRequest = {
        let bytes = axum::body::to_bytes(req.into_body(), MAX_COMMIT_BODY_SIZE)
            .await
            .map_err(|e| ApiError::BadRequest(format!("failed to read body: {e}")))?;
        serde_json::from_slice(&bytes)
            .map_err(|e| ApiError::BadRequest(format!("invalid JSON: {e}")))?
    };

    // Atomically get session with exclusive lock and transition to 'committing' state.
    // This prevents concurrent commits from both seeing state='open' and proceeding.
    // The operation is atomic (wrapped in a transaction), ensuring only one commit succeeds.
    let now = OffsetDateTime::now_utc();
    let session = state
        .metadata
        .begin_commit_session(upload_id, now)
        .await?
        .ok_or_else(|| ApiError::NotFound("upload session not found".to_string()))?;

    // Check state - should be 'committing' if transition succeeded
    // begin_commit_session only transitions from 'open' -> 'committing'
    if session.state != "committing" {
        // Session was not in 'open' state, so transition didn't happen
        // No need to call fail_session since state wasn't changed
        return Err(ApiError::BadRequest(format!(
            "upload session is {}, not open",
            session.state
        )));
    }

    // CRITICAL: After this point, session is in 'committing' state.
    // All error paths MUST call fail_session to prevent DoS (session stuck in committing forever).

    // Verify ownership - MUST fail_session on error since we're now in 'committing' state
    if let Err(e) = verify_session_ownership(&auth, &session) {
        let _ = state.metadata.fail_session(
            upload_id,
            "unauthorized",
            Some("Ownership verification failed after state transition"),
            OffsetDateTime::now_utc(),
        ).await;
        return Err(e);
    }

    // Check expiry - MUST fail_session on error since we're now in 'committing' state
    if session.expires_at < now {
        let _ = state.metadata.fail_session(
            upload_id,
            "expired",
            Some("Session expired after state transition"),
            OffsetDateTime::now_utc(),
        ).await;
        return Err(ApiError::UploadExpired);
    }

    // Session is now in 'committing' state (atomically transitioned)

    // Parse chunk hashes
    let chunk_hashes: Vec<ChunkHash> = body
        .manifest
        .iter()
        .map(|h| ChunkHash::from_hex(h))
        .collect::<Result<Vec<_>, _>>()?;

    // Verify all chunks are present
    let missing = state.metadata.get_missing_chunks(upload_id).await?;
    if !missing.is_empty() {
        record_upload_error("incomplete_upload");
        tracing::warn!(
            upload_id = %upload_id,
            missing_count = missing.len(),
            trace_id = %trace_id,
            "Commit failed: upload incomplete"
        );
        // Mark session as failed
        let _ = state.metadata.fail_session(
            upload_id,
            "incomplete_upload",
            Some(&format!("{} chunks missing", missing.len())),
            OffsetDateTime::now_utc(),
        ).await;
        return Err(ApiError::IncompleteUpload {
            missing: missing.len(),
        });
    }

    // Verify NAR hash by streaming chunks in order
    // Use streaming compression to avoid buffering entire NAR in memory
    let expected_nar_hash = NarHash::from_sri(&session.nar_hash)?;
    let mut nar_hasher = NarHasher::new();
    let mut total_bytes_read: u64 = 0;
    let compression_config = state.config.server.compression;
    let needs_compression = compression_config != cellar_core::config::CompressionConfig::None;

    // Check if expected_chunks were pre-defined for this upload.
    // If they were, we can distinguish between server errors (chunk unexpectedly missing)
    // and client errors (manifest references unknown chunk).
    let has_expected_chunks = !state.metadata.get_expected_chunks(upload_id).await?.is_empty();

    // Create true streaming compressor if compression is enabled.
    // This compresses data and streams it directly to storage, avoiding memory pressure.
    let temp_nar_key = format!("tmp/nar/{}", upload_id);
    let mut streaming_compressor = if needs_compression {
        let upload = state
            .storage
            .put_stream(&temp_nar_key)
            .await
            .map_err(|e| ApiError::Internal(format!("failed to create streaming upload: {e}")))?;
        Some(crate::compression::TrueStreamingCompressor::new(
            upload,
            compression_config,
            temp_nar_key.clone(),
        ))
    } else {
        None
    };

    for chunk_hash in &chunk_hashes {
        // Get chunk location from metadata
        let chunk_info = state
            .metadata
            .get_chunk(&chunk_hash.to_hex())
            .await?
            .ok_or_else(|| {
                if has_expected_chunks {
                    // Server knew what to expect but chunk is missing - internal error
                    ApiError::Internal(format!(
                        "chunk {} unexpectedly missing from metadata",
                        chunk_hash.to_hex()
                    ))
                } else {
                    // Client provided manifest with unknown chunk - client error
                    ApiError::BadRequest(format!(
                        "manifest references unknown chunk: {}",
                        chunk_hash.to_hex()
                    ))
                }
            })?;

        let object_key = chunk_info.object_key.ok_or_else(|| {
            ApiError::Internal(format!("chunk {} has no object key", chunk_hash.to_hex()))
        })?;

        // Read chunk data from storage
        let chunk_data = state.storage.get(&object_key).await?;
        total_bytes_read += chunk_data.len() as u64;
        nar_hasher.update(&chunk_data);

        // Stream chunk through compressor if compression is enabled
        if let Some(ref mut comp) = streaming_compressor {
            comp.write_chunk(&chunk_data)
                .await
                .map_err(|e| ApiError::Internal(format!("compression failed: {e}")))?;
        }
    }

    // Validate that total bytes matches declared nar_size
    let expected_nar_size = session.nar_size as u64;
    if total_bytes_read != expected_nar_size {
        // Clean up streaming compressor's temp data on validation failure
        if let Some(comp) = streaming_compressor {
            if let Err(e) = comp.abort().await {
                tracing::warn!(
                    upload_id = %upload_id,
                    key = %temp_nar_key,
                    error = %e,
                    trace_id = %trace_id,
                    "Failed to abort streaming compressor after size mismatch"
                );
            }
        }
        record_upload_error("nar_size_mismatch");
        tracing::warn!(
            upload_id = %upload_id,
            expected_size = expected_nar_size,
            actual_size = total_bytes_read,
            trace_id = %trace_id,
            "Commit failed: NAR size mismatch"
        );
        // Mark session as failed
        let _ = state.metadata.fail_session(
            upload_id,
            "nar_size_mismatch",
            Some(&format!("Expected {} bytes but got {}", expected_nar_size, total_bytes_read)),
            OffsetDateTime::now_utc(),
        ).await;
        return Err(ApiError::BadRequest(format!(
            "NAR size mismatch: declared {} bytes but chunks total {} bytes",
            expected_nar_size, total_bytes_read
        )));
    }

    let actual_nar_hash = nar_hasher.finalize();
    if actual_nar_hash != expected_nar_hash {
        // Clean up streaming compressor's temp data on validation failure
        if let Some(comp) = streaming_compressor {
            if let Err(e) = comp.abort().await {
                tracing::warn!(
                    upload_id = %upload_id,
                    key = %temp_nar_key,
                    error = %e,
                    trace_id = %trace_id,
                    "Failed to abort streaming compressor after hash mismatch"
                );
            }
        }
        record_upload_error("nar_hash_mismatch");
        tracing::warn!(
            upload_id = %upload_id,
            expected_hash = %expected_nar_hash.to_sri(),
            actual_hash = %actual_nar_hash.to_sri(),
            trace_id = %trace_id,
            "Commit failed: NAR hash mismatch"
        );
        // Mark session as failed
        let _ = state.metadata.fail_session(
            upload_id,
            "nar_hash_mismatch",
            Some(&format!("Expected {} but got {}", expected_nar_hash.to_sri(), actual_nar_hash.to_sri())),
            OffsetDateTime::now_utc(),
        ).await;
        return Err(ApiError::HashMismatch {
            expected: expected_nar_hash.to_sri(),
            actual: actual_nar_hash.to_sri(),
        });
    }

    // Compute manifest hash
    let manifest_hash = ManifestHash::compute(&chunk_hashes);

    // Verify manifest hash if provided at session creation
    if let Some(expected) = &session.manifest_hash {
        let expected_hash = ManifestHash::from_hex(expected)?;
        if manifest_hash != expected_hash {
            // Mark session as failed since we're in 'committing' state
            let _ = state.metadata.fail_session(
                upload_id,
                "manifest_hash_mismatch",
                Some(&format!("Expected {} but got {}", expected, manifest_hash.to_hex())),
                OffsetDateTime::now_utc(),
            ).await;
            return Err(ApiError::HashMismatch {
                expected: expected.clone(),
                actual: manifest_hash.to_hex(),
            });
        }
    }

    let now = OffsetDateTime::now_utc();
    let store_path = StorePath::parse(&session.store_path)?;

    // Initialize artifact tracker for compensation on failure (early to avoid scope issues)
    let mut artifacts = CommitArtifacts::new();

    // Helper macro to run cleanup on error (defined early to avoid scope issues)
    macro_rules! try_with_cleanup {
        ($expr:expr) => {
            match $expr {
                Ok(val) => val,
                Err(e) => {
                    artifacts.cleanup(&state, upload_id, &trace_id.0).await;
                    return Err(e.into());
                }
            }
        };
    }

    // Create manifest using atomic INSERT ON CONFLICT DO NOTHING.
    // This eliminates the TOCTOU race condition from the previous check-then-insert pattern.
    let manifest_row = ManifestRow {
        manifest_hash: manifest_hash.to_hex(),
        chunk_size: session.chunk_size,
        chunk_count: chunk_hashes.len() as i32,
        nar_size: session.nar_size,
        object_key: Some(manifest_hash.to_object_key()),
        created_at: now,
    };

    let chunk_mappings: Vec<ManifestChunkRow> = chunk_hashes
        .iter()
        .enumerate()
        .map(|(i, hash)| ManifestChunkRow {
            manifest_hash: manifest_hash.to_hex(),
            position: i as i32,
            chunk_hash: hash.to_hex(),
        })
        .collect();

    let manifest_created = state
        .metadata
        .create_manifest(&manifest_row, &chunk_mappings)
        .await?;

    // Only increment refcounts and store manifest JSON if we created it.
    // If the manifest already existed, these operations were already done.
    if manifest_created {
        // Increment refcounts for all chunks
        // Track incremented refcounts in artifacts for rollback on failure
        for hash in &chunk_hashes {
            let hex_hash = hash.to_hex();
            try_with_cleanup!(state.metadata.increment_refcount(&hex_hash).await);
            artifacts.incremented_refcounts.push(hex_hash);
        }

        // Store manifest JSON in object store
        let manifest = Manifest::new(chunk_hashes.clone(), session.chunk_size as u64, session.nar_size as u64);
        let manifest_json = manifest.to_json()?;
        try_with_cleanup!(
            state
                .storage
                .put(&manifest_hash.to_object_key(), Bytes::from(manifest_json))
                .await
        );
    }

    // Check for existing visible store path to handle re-uploads correctly.
    // For re-uploads, create_store_path preserves both the old metadata AND visibility='visible'.
    // This ensures the original content continues to be served without 404s during the upload.
    // Only complete_reupload atomically updates metadata to the new content when commit succeeds.
    let existing = state
        .metadata
        .get_store_path(session.cache_id, store_path.hash().as_str())
        .await?;

    let is_reupload = existing
        .as_ref()
        .map(|sp| sp.visibility_state == "visible")
        .unwrap_or(false);

    // For new uploads use "pending", for reuploads the UPSERT will preserve "visible".
    // This prevents 404s during reuploads while keeping old content accessible.
    let initial_visibility = if is_reupload { "visible" } else { "pending" };

    // Preserve original created_at for re-uploads
    let created_at = existing.as_ref().map(|e| e.created_at).unwrap_or(now);

    // Set artifact tracking fields now that we have computed is_reupload
    artifacts.is_reupload = is_reupload;
    artifacts.cache_id = session.cache_id;
    artifacts.store_path_hash = Some(store_path.hash().to_string());

    // Create store path record (scoped to cache_id for tenant isolation)
    let store_path_row = StorePathRow {
        store_path_hash: store_path.hash().to_string(),
        cache_id: session.cache_id,
        store_path: session.store_path.clone(),
        nar_hash: session.nar_hash.clone(),
        nar_size: session.nar_size,
        manifest_hash: manifest_hash.to_hex(),
        created_at,
        committed_at: Some(now),
        visibility_state: initial_visibility.to_string(),
        uploader_token_id: session.owner_token_id,
        ca: None, // Content-addressable field, populated from narinfo if present
    };

    try_with_cleanup!(state.metadata.create_store_path(&store_path_row).await);

    // Generate and sign narinfo
    let nar_hash = NarHash::from_sri(&session.nar_hash)?;
    let mut narinfo = if let Some(comp) = streaming_compressor {
        // Track temp key for cleanup in case of failure
        artifacts.temp_nar_key = Some(temp_nar_key.clone());

        // Finalize streaming compression (data is already in temp storage key)
        let compression_result = match comp.finish().await {
            Ok(r) => r,
            Err(e) => {
                artifacts.cleanup(&state, upload_id, &trace_id.0).await;
                return Err(ApiError::Internal(format!("compression finalization failed: {e}")));
            }
        };

        // Copy from temp key to final content-addressed key
        let compression = compression_config.to_compression();
        let extension = compression.extension();
        let nar_key = format!("{}/nar/{}.nar{}", cache_prefix(session.cache_id), store_path.hash(), extension);
        try_with_cleanup!(state.storage.copy(&temp_nar_key, &nar_key).await);
        artifacts.compressed_nar_key = Some(nar_key);

        // Delete temp key now that copy succeeded
        if let Err(e) = state.storage.delete(&temp_nar_key).await {
            tracing::warn!(
                upload_id = %upload_id,
                key = %temp_nar_key,
                error = %e,
                trace_id = %trace_id,
                "Failed to delete temp NAR key after successful copy"
            );
        }
        // Clear temp key from artifacts since we've handled it
        artifacts.temp_nar_key = None;

        // Create narinfo for compressed NAR
        cellar_core::narinfo::NarInfo::new_compressed(
            store_path.clone(),
            nar_hash,
            session.nar_size as u64,
            compression,
            compression_result.file_hash,
            compression_result.file_size,
        )
    } else {
        // No compression - use uncompressed narinfo
        cellar_core::narinfo::NarInfo::new_uncompressed(
            store_path.clone(),
            nar_hash,
            session.nar_size as u64,
        )
    };

    if let Some(signer) = &state.signer {
        signer.sign(&mut narinfo);
    }

    // Store narinfo
    let narinfo_key = format!("{}/narinfo/{}.narinfo", cache_prefix(session.cache_id), store_path.hash());
    let narinfo_text = narinfo.to_narinfo_text();
    try_with_cleanup!(state.storage.put(&narinfo_key, Bytes::from(narinfo_text)).await);
    artifacts.narinfo_key = Some(narinfo_key.clone());

    // Record narinfo in metadata (scoped to cache_id for tenant isolation)
    let narinfo_row = NarInfoRow {
        cache_id: session.cache_id,
        store_path_hash: store_path.hash().to_string(),
        narinfo_object_key: narinfo_key,
        content_hash: None,
        created_at: now,
        updated_at: now,
    };
    try_with_cleanup!(state.metadata.upsert_narinfo(&narinfo_row).await);

    // Now that all storage operations succeeded, make the store path visible.
    // This is the "commit point" - only now can clients see and download this path.
    // For reuploads, use complete_reupload to atomically update both visibility and metadata.
    if artifacts.is_reupload {
        try_with_cleanup!(
            state
                .metadata
                .complete_reupload(
                    session.cache_id,
                    store_path.hash().as_str(),
                    &session.nar_hash,
                    session.nar_size,
                    &manifest_hash.to_hex(),
                )
                .await
        );
    } else {
        try_with_cleanup!(
            state
                .metadata
                .update_visibility(session.cache_id, store_path.hash().as_str(), "visible")
                .await
        );
    }

    // Mark session as committed (bookkeeping only - upload is already complete)
    // If this fails, we log a warning but don't cleanup since data is already visible
    if let Err(e) = state
        .metadata
        .update_state(upload_id, "committed", now)
        .await
    {
        tracing::warn!(
            upload_id = %upload_id,
            error = %e,
            trace_id = %trace_id,
            "Failed to update session state to committed, but upload data is already visible"
        );
    }

    // Record commit metrics and log success
    UPLOAD_SESSIONS_COMMITTED.inc();
    UPLOAD_COMMIT_DURATION.observe(start_time.elapsed().as_secs_f64());

    tracing::info!(
        upload_id = %upload_id,
        store_path = %session.store_path,
        nar_size = session.nar_size,
        duration_secs = start_time.elapsed().as_secs_f64(),
        trace_id = %trace_id,
        "Upload committed successfully"
    );

    Ok(StatusCode::CREATED)
}
