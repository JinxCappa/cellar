//! Upload control plane handlers.

use crate::auth::{AuthenticatedUser, get_trace_id, require_auth};
use crate::error::{ApiError, ApiResult};
use crate::handlers::{resolve_domain_id, resolve_upload_cache_id};
use crate::metrics::{
    BYTES_DEDUPLICATED, BYTES_UPLOADED, CHUNK_HASH_MISMATCHES, CHUNK_UPLOAD_DURATION,
    CHUNKS_DEDUPLICATED, CHUNKS_UPLOADED, UPLOAD_COMMIT_DURATION, UPLOAD_SESSIONS_COMMITTED,
    UPLOAD_SESSIONS_CREATED, UPLOAD_SESSIONS_RESUMED, record_upload_error,
};
use crate::state::AppState;
use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use cellar_core::chunk::ChunkHash;
use cellar_core::hash::{NarHash, NarHasher};
use cellar_core::manifest::{Manifest, ManifestHash};
use cellar_core::store_path::StorePath;
use cellar_core::token::TokenScope;
use cellar_core::upload::{
    CreateUploadRequest, CreateUploadResponse, MissingChunks, UploadStateResponse,
};
use cellar_metadata::models::{
    ChunkRow, ManifestChunkRow, ManifestRow, NarInfoRow, StorePathReferenceRow, StorePathRow,
    UploadExpectedChunkRow, UploadSessionRow,
};
use serde::Deserialize;
use std::time::Instant;
use time::OffsetDateTime;
use uuid::Uuid;

/// Maximum request body size for create upload requests (10 MiB).
///
/// This limit accommodates large `expected_chunks` arrays for big NAR files.
/// At ~100 bytes per chunk entry (64-char hash + size + JSON overhead),
/// 10 MiB supports ~100,000 chunks, or ~1.6 TB NARs at 16 MiB chunk size.
///
/// **Note**: If running behind a reverse proxy (nginx, haproxy, etc.), ensure the
/// proxy's `client_max_body_size` (nginx) or equivalent setting is >= this value
/// to avoid inconsistent 413 responses where the proxy rejects before we can.
const MAX_CREATE_BODY_SIZE: usize = 10 * 1024 * 1024;

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

/// Validate an expected chunk's hash and size.
///
/// Returns an error if:
/// - Hash is not 64 hex characters (SHA-256)
/// - Size exceeds reasonable bounds
fn validate_expected_chunk(
    hash: &str,
    size: u64,
    position: usize,
    chunk_size: u64,
    server_max_chunk_size: u64,
) -> ApiResult<()> {
    // Validate hash format: must be 64 hex characters (SHA-256)
    if hash.len() != 64 {
        return Err(ApiError::BadRequest(format!(
            "expected_chunks[{}]: hash must be 64 hex characters, got {}",
            position,
            hash.len()
        )));
    }

    if !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ApiError::BadRequest(format!(
            "expected_chunks[{}]: hash contains non-hex characters",
            position
        )));
    }

    // Validate size is reasonable (not zero, not larger than chunk_size + some overhead)
    // The last chunk can be smaller, but no chunk should exceed chunk_size.
    // Allow some tolerance for padding/alignment, but clamp to server_max_chunk_size
    // to prevent impossible sessions when chunk_size == max_chunk_size.
    let max_allowed = (chunk_size + 1024).min(server_max_chunk_size);
    if size == 0 {
        return Err(ApiError::BadRequest(format!(
            "expected_chunks[{}]: size cannot be zero",
            position
        )));
    }

    if size > max_allowed {
        return Err(ApiError::BadRequest(format!(
            "expected_chunks[{}]: size {} exceeds maximum chunk size {}",
            position, size, max_allowed
        )));
    }

    Ok(())
}

/// Additional buffer for chunk uploads beyond the max chunk size.
const CHUNK_UPLOAD_BUFFER: usize = 1024;

/// Validate and create a commit progress JSON string with schema validation.
///
/// This prevents corruption issues during rollback by:
/// - Validating all chunk hashes are 64-char hex strings
/// - Validating manifest hash format if provided
/// - Adding metadata for corruption detection
/// - Ensuring the JSON can be parsed back
fn create_commit_progress_json(
    phase: &str,
    chunks_done: usize,
    chunks_total: usize,
    incremented_refcounts: &[String],
    manifest_hash: Option<&str>,
) -> ApiResult<String> {
    // Validate all chunk hashes BEFORE writing to DB
    for chunk_hash in incremented_refcounts {
        if chunk_hash.len() != 64 || !chunk_hash.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(ApiError::Internal(format!(
                "Invalid chunk hash format in commit progress: {} (must be 64 hex chars)",
                chunk_hash
            )));
        }
    }

    // Validate manifest hash if provided
    if let Some(hash) = manifest_hash
        && (hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()))
    {
        return Err(ApiError::Internal(format!(
            "Invalid manifest hash format in commit progress: {} (must be 64 hex chars)",
            hash
        )));
    }

    // Create progress JSON with metadata for validation
    let mut progress = serde_json::json!({
        "phase": phase,
        "chunks_done": chunks_done,
        "chunks_total": chunks_total,
        "incremented_refcounts": incremented_refcounts,
        "refcount_count": incremented_refcounts.len(), // Redundant check for corruption detection
    });

    if let Some(hash) = manifest_hash {
        progress["manifest_hash"] = serde_json::json!(hash);
    }

    let json_str = progress.to_string();

    // CRITICAL: Verify the JSON can be parsed back (corruption check)
    match serde_json::from_str::<serde_json::Value>(&json_str) {
        Ok(parsed) => {
            // Verify refcount_count matches array length (corruption detection)
            if let Some(refcounts) = parsed["incremented_refcounts"].as_array()
                && refcounts.len() != incremented_refcounts.len()
            {
                return Err(ApiError::Internal(
                    "Commit progress JSON validation failed: refcount mismatch after serialization"
                        .to_string(),
                ));
            }
            Ok(json_str)
        }
        Err(e) => Err(ApiError::Internal(format!(
            "Commit progress JSON validation failed: cannot parse back: {}",
            e
        ))),
    }
}

/// POST /v1/uploads - Create a new upload session.
#[tracing::instrument(skip(state, req), fields(store_path))]
pub async fn create_upload(
    State(state): State<AppState>,
    req: Request,
) -> ApiResult<(StatusCode, Json<CreateUploadResponse>)> {
    let trace_id = get_trace_id(&req).cloned().unwrap_or_default();
    let auth = require_auth(&req)?.clone();
    auth.require_scope(TokenScope::CacheWrite)?;
    let cache_id = resolve_upload_cache_id(&state, auth.token.cache_id).await?;

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
    let chunk_size = body
        .chunk_size
        .unwrap_or(state.config.server.default_chunk_size);

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
    if chunk_size < cellar_core::MIN_CHUNK_SIZE || chunk_size > state.config.server.max_chunk_size {
        return Err(ApiError::BadRequest(format!(
            "chunk_size must be between {} and {}",
            cellar_core::MIN_CHUNK_SIZE,
            state.config.server.max_chunk_size
        )));
    }

    // Validate manifest_hash format if provided (must be valid hex)
    // This prevents sessions from being created with invalid manifest hashes that would
    // cause parsing failures during commit, leaving sessions stuck without proper cleanup.
    if let Some(ref manifest_hash) = body.manifest_hash {
        ManifestHash::from_hex(manifest_hash)
            .map_err(|e| ApiError::BadRequest(format!("invalid manifest_hash: {e}")))?;
    }

    // Check if an open session already exists for this store path (scoped to cache_id)
    if let Some(existing) = state
        .metadata
        .get_session_by_store_path(cache_id, store_path.hash().as_str())
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
                // CRITICAL FIX: Verify ownership BEFORE invalidating the session.
                // Previously, any token in the same cache could DoS another user's upload
                // by sending mismatched parameters, causing session invalidation without
                // ownership verification.
                verify_session_ownership(cache_id, &auth, &existing)?;

                tracing::info!(
                    upload_id = %existing.upload_id,
                    trace_id = %trace_id,
                    "Session parameters changed, invalidating old session"
                );
                state.metadata.delete_session(existing.upload_id).await?;
                // Fall through to create a new session
            } else {
                // Parameters match - verify ownership before allowing resume
                verify_session_ownership(cache_id, &auth, &existing)?;
                // For resume: use chunks_predeclared flag to determine resume semantics.
                // Previously this checked if upload_expected_chunks was non-empty, but that's
                // incorrect for dynamic uploads where chunks get INSERT'd as they arrive,
                // making the table non-empty even without pre-declaration.
                let missing_chunks = if existing.chunks_predeclared {
                    // Expected chunks were pre-declared at session creation - return actual missing list
                    let missing = state
                        .metadata
                        .get_missing_chunks(existing.upload_id)
                        .await?;
                    if missing.is_empty() {
                        MissingChunks::List(vec![]) // All chunks received
                    } else {
                        MissingChunks::List(missing)
                    }
                } else {
                    // Dynamic upload (no pre-declared chunks) - check received chunks
                    let received = state
                        .metadata
                        .get_received_chunks(existing.upload_id)
                        .await?;
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

    // === VALIDATION PHASE ===
    // All validation must happen BEFORE creating the session in the database.
    // This prevents orphan sessions if validation fails.

    // CRITICAL: expected_chunks is required by default to prevent GC data loss.
    // Without expected_chunks, long-running uploads are vulnerable to chunk deletion
    // if GC runs and finds chunks with refcount=0 that have existed longer than the grace period.
    // Set server.require_expected_chunks=false to allow uploads without expected_chunks (not recommended).
    let has_expected_chunks = body.expected_chunks.as_ref().is_some_and(|v| !v.is_empty());
    if !has_expected_chunks && state.config.server.require_expected_chunks {
        return Err(ApiError::BadRequest(
            "expected_chunks is required. Provide a list of chunk hashes and sizes to enable \
             resume support and prevent GC data loss during long-running uploads. \
             Set server.require_expected_chunks=false to disable this check (not recommended)."
                .to_string(),
        ));
    }

    // If expected chunks provided, validate them before creating the session
    if let Some(ref expected_chunks) = body.expected_chunks {
        // Validate each chunk
        for (i, chunk) in expected_chunks.iter().enumerate() {
            validate_expected_chunk(
                &chunk.hash,
                chunk.size,
                i,
                chunk_size,
                state.config.server.max_chunk_size,
            )?;
        }

        // Validate the total number of chunks is reasonable
        let max_expected_chunks = (body.nar_size / chunk_size as u64) + 2; // +2 for rounding tolerance
        if expected_chunks.len() as u64 > max_expected_chunks {
            return Err(ApiError::BadRequest(format!(
                "expected_chunks count {} exceeds maximum {} for NAR size {} with chunk size {}",
                expected_chunks.len(),
                max_expected_chunks,
                body.nar_size,
                chunk_size
            )));
        }
    }

    // === SESSION CREATION PHASE ===
    // Validation passed, now create the session

    let upload_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();
    let expires_at = now + state.config.server.upload_timeout();
    let domain_id = resolve_domain_id(&state, cache_id).await?;

    let session = UploadSessionRow {
        upload_id,
        cache_id,
        domain_id,
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
        commit_started_at: None, // Set when commit begins
        commit_progress: None,   // Set when commit begins
        // Track whether chunks were pre-declared for correct error classification in commit.
        chunks_predeclared: has_expected_chunks,
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

    // Log warning if no expected_chunks (validation already passed, so require_expected_chunks=false)
    if !has_expected_chunks {
        tracing::warn!(
            upload_id = %upload_id,
            store_path = %body.store_path,
            trace_id = %trace_id.0,
            "Upload created WITHOUT expected_chunks - vulnerable to GC data loss in long-running uploads"
        );
    }

    // If expected chunks provided (non-empty), store them for resume tracking.
    // Skip storage for explicitly empty lists to keep state consistent with has_expected_chunks.
    if let Some(ref expected_chunks) = body.expected_chunks
        && !expected_chunks.is_empty()
    {
        // Validation already done above, just store them
        let chunk_rows: Vec<UploadExpectedChunkRow> = expected_chunks
            .iter()
            .enumerate()
            .map(|(i, chunk)| UploadExpectedChunkRow {
                upload_id,
                position: i as i32,
                // Normalize to lowercase for consistent matching with upload_chunk()
                chunk_hash: chunk.hash.to_lowercase(),
                size_bytes: chunk.size as i64,
                received_at: None,
                uploaded_data: false, // Not uploaded yet
            })
            .collect();

        state.metadata.add_expected_chunks(&chunk_rows).await?;
    }

    // Determine initial missing chunks response
    let missing_chunks = if has_expected_chunks {
        // With expected chunks (non-empty), return the full list as missing
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
///
/// `resolved_cache_id` should be the cache_id after resolving global tokens
/// (i.e., `None` is resolved to the default public cache UUID).
fn verify_session_ownership(
    resolved_cache_id: Option<Uuid>,
    auth: &AuthenticatedUser,
    session: &UploadSessionRow,
) -> ApiResult<()> {
    // Verify the session belongs to the same cache as the (resolved) token.
    // This prevents cross-cache access when owner_token_id is NULL (legacy/manual rows).
    //
    // Legacy compatibility: sessions created by global tokens before the
    // resolve_upload_cache_id fix have cache_id = None, while the resolved
    // cache_id is now Some(default_cache_uuid). Allow this combination when
    // the requesting token is itself a global token.
    let cache_matches = session.cache_id == resolved_cache_id
        || (session.cache_id.is_none() && auth.token.cache_id.is_none());
    if !cache_matches {
        return Err(ApiError::Forbidden(
            "upload session belongs to a different cache".to_string(),
        ));
    }

    // If session has an owner token, verify the requesting token matches
    if let Some(owner_id) = session.owner_token_id
        && *auth.token.id.as_uuid() != owner_id
    {
        return Err(ApiError::Forbidden(
            "upload session belongs to a different token".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellar_core::token::{Token, TokenId, TokenScope};
    use std::collections::HashSet;

    fn make_token(cache_id: Option<Uuid>, token_id: Uuid) -> Token {
        let mut scopes = HashSet::new();
        scopes.insert(TokenScope::CacheWrite);
        let token_id = TokenId::parse(&token_id.to_string()).unwrap();

        Token {
            id: token_id,
            cache_id,
            scopes,
            expires_at: None,
            revoked_at: None,
            created_at: OffsetDateTime::now_utc(),
            description: None,
        }
    }

    fn build_session(cache_id: Option<Uuid>, owner_token_id: Option<Uuid>) -> UploadSessionRow {
        UploadSessionRow {
            upload_id: Uuid::new_v4(),
            cache_id,
            domain_id: cellar_core::storage_domain::default_storage_domain_id(),
            store_path: "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test".to_string(),
            store_path_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            nar_size: 10,
            nar_hash: "sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa=".to_string(),
            chunk_size: 1,
            manifest_hash: None,
            state: "open".to_string(),
            owner_token_id,
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
            expires_at: OffsetDateTime::now_utc(),
            trace_id: None,
            error_code: None,
            error_detail: None,
            commit_started_at: None,
            commit_progress: None,
            chunks_predeclared: false,
        }
    }

    #[test]
    fn cache_prefix_formats_public_and_cache() {
        assert_eq!(cache_prefix(None), "public");

        let cache_id = Uuid::new_v4();
        assert_eq!(cache_prefix(Some(cache_id)), cache_id.to_string());
    }

    #[test]
    fn validate_expected_chunk_rejects_invalid_inputs() {
        assert!(validate_expected_chunk("abcd", 1, 0, 10, 100).is_err());
        assert!(validate_expected_chunk(&"g".repeat(64), 1, 0, 10, 100).is_err());
        assert!(validate_expected_chunk(&"a".repeat(64), 0, 0, 10, 100).is_err());
        assert!(validate_expected_chunk(&"a".repeat(64), 9999, 0, 10, 100).is_err());
    }

    #[test]
    fn validate_expected_chunk_accepts_valid() {
        let hash = "a".repeat(64);
        validate_expected_chunk(&hash, 10, 0, 10, 100).unwrap();
    }

    #[test]
    fn validate_expected_chunk_clamps_to_server_max() {
        let hash = "a".repeat(64);
        // When chunk_size == max_chunk_size, tolerance is clamped to max_chunk_size
        // chunk_size=100 + 1024 tolerance = 1124, but server_max=100, so max_allowed=100
        assert!(validate_expected_chunk(&hash, 101, 0, 100, 100).is_err());
        // Exactly at max_chunk_size should work
        validate_expected_chunk(&hash, 100, 0, 100, 100).unwrap();
        // When server_max is higher, tolerance applies
        validate_expected_chunk(&hash, 110, 0, 100, 200).unwrap();
    }

    #[test]
    fn create_commit_progress_json_includes_metadata() {
        let chunks = vec!["a".repeat(64), "b".repeat(64)];
        let json =
            create_commit_progress_json("phase1", 1, 2, &chunks, Some(&"c".repeat(64))).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["phase"], "phase1");
        assert_eq!(value["chunks_done"], 1);
        assert_eq!(value["refcount_count"], 2);
        assert_eq!(value["manifest_hash"], "c".repeat(64));
    }

    #[test]
    fn create_commit_progress_json_rejects_bad_hashes() {
        let err =
            create_commit_progress_json("phase", 0, 0, &[String::from("bad")], None).unwrap_err();
        match err {
            ApiError::Internal(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }

        let err = create_commit_progress_json("phase", 0, 0, &[], Some("not-hex")).unwrap_err();
        match err {
            ApiError::Internal(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn verify_session_ownership_enforces_cache_and_owner() {
        let cache_id = Uuid::new_v4();
        let token_id = Uuid::new_v4();
        let auth = AuthenticatedUser {
            token: make_token(Some(cache_id), token_id),
        };

        let session = build_session(Some(cache_id), Some(token_id));
        verify_session_ownership(Some(cache_id), &auth, &session).unwrap();

        let other_cache = build_session(Some(Uuid::new_v4()), Some(token_id));
        assert!(verify_session_ownership(Some(cache_id), &auth, &other_cache).is_err());

        let other_owner = build_session(Some(cache_id), Some(Uuid::new_v4()));
        assert!(verify_session_ownership(Some(cache_id), &auth, &other_owner).is_err());
    }

    #[test]
    fn verify_session_ownership_allows_legacy_global_token_sessions() {
        let token_id = Uuid::new_v4();
        let resolved_cache_id = Uuid::new_v4(); // default public cache UUID

        // Global token (cache_id = None) with resolved cache_id
        let auth = AuthenticatedUser {
            token: make_token(None, token_id),
        };

        // Legacy session created before resolve_upload_cache_id fix (cache_id = None)
        let legacy_session = build_session(None, Some(token_id));
        verify_session_ownership(Some(resolved_cache_id), &auth, &legacy_session).unwrap();

        // New session created after fix (cache_id = resolved UUID)
        let new_session = build_session(Some(resolved_cache_id), Some(token_id));
        verify_session_ownership(Some(resolved_cache_id), &auth, &new_session).unwrap();

        // Cache-scoped token must NOT match legacy None sessions
        let scoped_auth = AuthenticatedUser {
            token: make_token(Some(Uuid::new_v4()), token_id),
        };
        assert!(
            verify_session_ownership(Some(resolved_cache_id), &scoped_auth, &legacy_session)
                .is_err()
        );
    }
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
    let cache_id = resolve_upload_cache_id(&state, auth.token.cache_id).await?;

    let upload_id = Uuid::parse_str(&upload_id)
        .map_err(|e| ApiError::BadRequest(format!("invalid upload ID: {e}")))?;

    let session = state
        .metadata
        .get_session(upload_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("upload session not found".to_string()))?;

    // Verify ownership
    verify_session_ownership(cache_id, auth, &session)?;

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
        chunks_predeclared: session.chunks_predeclared,
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
    let cache_id = resolve_upload_cache_id(&state, auth.token.cache_id).await?;

    // Normalize chunk hash to lowercase to ensure consistent storage and lookups
    // from_hex accepts both upper/lowercase, but to_hex always returns lowercase
    let chunk_hash = chunk_hash.to_lowercase();

    let upload_id = Uuid::parse_str(&upload_id)
        .map_err(|e| ApiError::BadRequest(format!("invalid upload ID: {e}")))?;
    let expected_hash = ChunkHash::from_hex(&chunk_hash)
        .map_err(|e| ApiError::BadRequest(format!("invalid chunk hash: {e}")))?;

    // Check session exists and is open
    let session = state
        .metadata
        .get_session(upload_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("upload session not found".to_string()))?;

    // Verify ownership
    verify_session_ownership(cache_id, auth, &session)?;

    if session.state != "open" {
        return Err(ApiError::BadRequest(format!(
            "upload session is {}, not open",
            session.state
        )));
    }

    if session.expires_at < OffsetDateTime::now_utc() {
        return Err(ApiError::UploadExpired);
    }

    let domain_id = session.domain_id;

    // SECURITY: For predeclared uploads, strictly enforce that only declared chunks can be uploaded.
    // This prevents cross-session chunk reference attacks where a client could "claim" chunks
    // from other sessions by uploading/deduping chunks they didn't declare upfront.
    if session.chunks_predeclared {
        let is_expected = state
            .metadata
            .get_expected_chunk(upload_id, &chunk_hash)
            .await?
            .is_some();

        if !is_expected {
            record_upload_error("chunk_not_in_expected_list");
            return Err(ApiError::BadRequest(format!(
                "chunk {} not in expected_chunks list for predeclared upload",
                chunk_hash
            )));
        }
    }

    // Check if chunk already exists in BOTH storage AND metadata (deduplication)
    // We need to verify both to handle partial failures where storage has the chunk
    // but metadata doesn't (which would cause commit to fail with "chunk not found")
    let object_key = expected_hash.to_object_key_with_domain(domain_id);
    let storage_exists = state.storage.exists(&object_key).await?;
    let metadata_exists = state
        .metadata
        .get_chunk(domain_id, &chunk_hash)
        .await?
        .is_some();

    if storage_exists && metadata_exists {
        // SECURITY: When shortcircuit is disabled, always read & verify body hash
        // to prevent timing-based probing of chunk existence across caches.
        if state.config.server.disable_chunk_dedup_shortcircuit {
            let chunk_data = axum::body::to_bytes(
                req.into_body(),
                state.config.server.max_chunk_size as usize + CHUNK_UPLOAD_BUFFER,
            )
            .await
            .map_err(|e| ApiError::BadRequest(format!("failed to read chunk: {e}")))?;

            // SECURITY: Strictly enforce max_chunk_size to prevent side-channel.
            // Without this check, oversized chunks would succeed for existing items
            // but fail for new items, leaking chunk existence information.
            if chunk_data.len() > state.config.server.max_chunk_size as usize {
                record_upload_error("chunk_too_large");
                return Err(ApiError::BadRequest(format!(
                    "chunk size {} exceeds maximum {}",
                    chunk_data.len(),
                    state.config.server.max_chunk_size
                )));
            }

            // Verify hash matches to prevent abuse
            let actual_hash = ChunkHash::compute(&chunk_data);
            if actual_hash != expected_hash {
                CHUNK_HASH_MISMATCHES.inc();
                record_upload_error("hash_mismatch");
                return Err(ApiError::HashMismatch {
                    expected: expected_hash.to_hex(),
                    actual: actual_hash.to_hex(),
                });
            }
        }

        // Chunk fully exists in both storage and metadata, just mark as received
        // Pass uploaded_data=false since we're not actually uploading data (dedup shortcircuit)
        let accessed_at = OffsetDateTime::now_utc();
        state
            .metadata
            .mark_chunk_received(
                upload_id,
                &chunk_hash,
                accessed_at,
                false,
                session.chunks_predeclared,
            )
            .await?;
        state
            .metadata
            .touch_chunk(domain_id, &chunk_hash, accessed_at)
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
    let chunk_data = axum::body::to_bytes(
        req.into_body(),
        state.config.server.max_chunk_size as usize + CHUNK_UPLOAD_BUFFER,
    )
    .await
    .map_err(|e| ApiError::BadRequest(format!("failed to read chunk: {e}")))?;

    // SECURITY: Strictly enforce max_chunk_size after read.
    // CHUNK_UPLOAD_BUFFER exists for HTTP framing overhead, not for oversized chunks.
    if chunk_data.len() > state.config.server.max_chunk_size as usize {
        record_upload_error("chunk_too_large");
        return Err(ApiError::BadRequest(format!(
            "chunk size {} exceeds maximum {}",
            chunk_data.len(),
            state.config.server.max_chunk_size
        )));
    }

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

    // SECURITY FIX: Validate chunk size against expected_chunks (if predeclared).
    // This ensures uploaded chunks match the sizes declared at session creation,
    // making expected_chunks size data reliable for resume and diagnostics.
    if session.chunks_predeclared {
        if let Some(expected_chunk) = state
            .metadata
            .get_expected_chunk(upload_id, &chunk_hash)
            .await?
        {
            let uploaded_size = chunk_data.len() as i64;
            if uploaded_size != expected_chunk.size_bytes {
                record_upload_error("chunk_size_mismatch");
                tracing::warn!(
                    upload_id = %upload_id,
                    chunk_hash = %chunk_hash,
                    expected_size = expected_chunk.size_bytes,
                    actual_size = uploaded_size,
                    trace_id = %trace_id,
                    "Chunk size mismatch with expected_chunks declaration"
                );
                return Err(ApiError::BadRequest(format!(
                    "chunk size mismatch: declared {} bytes in expected_chunks but uploaded {} bytes",
                    expected_chunk.size_bytes, uploaded_size
                )));
            }
        } else {
            // Chunk hash not found in expected_chunks - reject it
            record_upload_error("chunk_not_in_expected");
            tracing::warn!(
                upload_id = %upload_id,
                chunk_hash = %chunk_hash,
                trace_id = %trace_id,
                "Chunk not found in expected_chunks"
            );
            return Err(ApiError::BadRequest(format!(
                "chunk {} was not declared in expected_chunks",
                chunk_hash
            )));
        }
    }

    // Store chunk
    let chunk_size = chunk_data.len() as u64;
    let was_new = state
        .storage
        .put_if_not_exists(&object_key, chunk_data.clone())
        .await?;

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
        state
            .metadata
            .get_chunk(domain_id, &chunk_hash)
            .await?
            .is_none()
    };

    if was_new || needs_metadata_upsert {
        let chunk_row = ChunkRow {
            domain_id,
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
            if was_new && let Err(cleanup_err) = state.storage.delete(&object_key).await {
                tracing::warn!(
                    upload_id = %upload_id,
                    chunk_hash = %chunk_hash,
                    error = %cleanup_err,
                    "Failed to clean up orphaned chunk from storage"
                );
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
    // Pass uploaded_data=true since actual data was uploaded
    state
        .metadata
        .mark_chunk_received(
            upload_id,
            &chunk_hash,
            now,
            true,
            session.chunks_predeclared,
        )
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
    /// References (full store paths) for the narinfo.
    #[serde(default)]
    pub references: Option<Vec<String>>,
    /// Deriver (full store path) for the narinfo.
    #[serde(default)]
    pub deriver: Option<String>,
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
    /// Storage domain ID for refcount operations.
    domain_id: Option<Uuid>,
    /// Whether this was a re-upload (existing visible path).
    is_reupload: bool,
    /// Chunk hashes with incremented refcounts (for rollback on failure).
    incremented_refcounts: Vec<String>,
    /// Manifest hash (if manifest was created with initial refcount, for rollback on failure).
    manifest_hash: Option<String>,
}

impl CommitArtifacts {
    fn new() -> Self {
        Self {
            compressed_nar_key: None,
            temp_nar_key: None,
            narinfo_key: None,
            store_path_hash: None,
            cache_id: None,
            domain_id: None,
            is_reupload: false,
            incremented_refcounts: Vec::new(),
            manifest_hash: None,
        }
    }

    /// Clean up artifacts on commit failure.
    /// This is a best-effort cleanup - failures are logged but don't propagate.
    async fn cleanup(&self, state: &AppState, upload_id: Uuid, trace_id: &str) {
        // Mark session as failed so it can be cleaned up by GC
        // This prevents sessions from being stuck in 'committing' state forever
        if let Err(e) = state
            .metadata
            .fail_session(
                upload_id,
                "commit_failed",
                Some("Commit operation failed during artifact creation"),
                OffsetDateTime::now_utc(),
            )
            .await
        {
            tracing::warn!(
                upload_id = %upload_id,
                error = %e,
                trace_id = %trace_id,
                "Failed to mark session as failed after commit failure"
            );
        }

        // Rollback incremented refcounts to prevent orphaned chunks
        if let Some(domain_id) = self.domain_id {
            for chunk_hash in &self.incremented_refcounts {
                if let Err(e) = state
                    .metadata
                    .decrement_refcount(domain_id, chunk_hash)
                    .await
                {
                    tracing::warn!(
                        upload_id = %upload_id,
                        chunk_hash = %chunk_hash,
                        domain_id = %domain_id,
                        error = %e,
                        trace_id = %trace_id,
                        "Failed to rollback chunk refcount after commit failure"
                    );
                }
            }
        }

        // Rollback manifest refcount if it was created (Issue #2 fix).
        // Without this, failed commits leave manifests with refcount=1 but no store_path,
        // causing permanent manifest leaks that prevent GC.
        if let (Some(domain_id), Some(manifest_hash)) = (self.domain_id, &self.manifest_hash)
            && let Err(e) = state
                .metadata
                .decrement_manifest_refcount(domain_id, manifest_hash)
                .await
        {
            tracing::warn!(
                upload_id = %upload_id,
                manifest_hash = %manifest_hash,
                domain_id = %domain_id,
                error = %e,
                trace_id = %trace_id,
                "Failed to rollback manifest refcount after commit failure"
            );
        }

        // Delete compressed NAR if it was stored
        if let Some(key) = &self.compressed_nar_key
            && let Err(e) = state.storage.delete(key).await
        {
            tracing::warn!(
                upload_id = %upload_id,
                key = %key,
                error = %e,
                trace_id = %trace_id,
                "Failed to clean up compressed NAR after commit failure"
            );
        }

        // Delete temporary NAR key if streaming compression was in progress
        if let Some(key) = &self.temp_nar_key
            && let Err(e) = state.storage.delete(key).await
        {
            tracing::warn!(
                upload_id = %upload_id,
                key = %key,
                error = %e,
                trace_id = %trace_id,
                "Failed to clean up temp NAR key after commit failure"
            );
        }

        // Delete narinfo if it was stored
        if let Some(key) = &self.narinfo_key
            && let Err(e) = state.storage.delete(key).await
        {
            tracing::warn!(
                upload_id = %upload_id,
                key = %key,
                error = %e,
                trace_id = %trace_id,
                "Failed to clean up narinfo after commit failure"
            );
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
                if let Err(e) = state
                    .metadata
                    .update_visibility(self.cache_id, hash, "failed")
                    .await
                {
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
    let cache_id = resolve_upload_cache_id(&state, auth.token.cache_id).await?;

    let upload_id = Uuid::parse_str(&upload_id)
        .map_err(|e| ApiError::BadRequest(format!("invalid upload ID: {e}")))?;

    // Parse request body
    let body: CommitRequest = {
        let bytes = axum::body::to_bytes(req.into_body(), MAX_COMMIT_BODY_SIZE)
            .await
            .map_err(|e| ApiError::BadRequest(format!("failed to read body: {e}")))?;
        serde_json::from_slice(&bytes)
            .map_err(|e| ApiError::BadRequest(format!("invalid JSON: {e}")))?
    };

    if body.manifest.is_empty() {
        return Err(ApiError::BadRequest(
            "manifest must contain at least one chunk".to_string(),
        ));
    }

    // Atomically get session with exclusive lock and transition to 'committing' state.
    // This prevents concurrent commits from both seeing state='open' and proceeding.
    // The operation is atomic (wrapped in a transaction), ensuring only one commit succeeds.
    let now = OffsetDateTime::now_utc();
    let session = state
        .metadata
        .begin_commit_session(upload_id, now)
        .await?
        .ok_or_else(|| ApiError::NotFound("upload session not found".to_string()))?;
    let domain_id = session.domain_id;

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

    // Helper macro to fail session on error during committing phase.
    // CRITICAL: After begin_commit_session succeeds, all error paths MUST call fail_session
    // to prevent sessions from being stuck in 'committing' state forever.
    macro_rules! fail_session_on_error {
        ($expr:expr, $code:literal) => {
            match $expr {
                Ok(val) => val,
                Err(e) => {
                    let error_msg = e.to_string();
                    let metadata = state.metadata.clone();
                    tokio::spawn(async move {
                        let _ = metadata
                            .fail_session(
                                upload_id,
                                $code,
                                Some(&error_msg),
                                OffsetDateTime::now_utc(),
                            )
                            .await;
                    });
                    return Err(e.into());
                }
            }
        };
    }

    // Verify ownership - MUST fail_session on error since we're now in 'committing' state
    if let Err(e) = verify_session_ownership(cache_id, &auth, &session) {
        let _ = state
            .metadata
            .fail_session(
                upload_id,
                "unauthorized",
                Some("Ownership verification failed after state transition"),
                OffsetDateTime::now_utc(),
            )
            .await;
        return Err(e);
    }

    // Check expiry - MUST fail_session on error since we're now in 'committing' state
    if session.expires_at < now {
        let _ = state
            .metadata
            .fail_session(
                upload_id,
                "expired",
                Some("Session expired after state transition"),
                OffsetDateTime::now_utc(),
            )
            .await;
        return Err(ApiError::UploadExpired);
    }

    // Session is now in 'committing' state (atomically transitioned)

    // Parse chunk hashes
    let chunk_hashes: Vec<ChunkHash> = body
        .manifest
        .iter()
        .map(|h| ChunkHash::from_hex(h))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            // CRITICAL: Session is in 'committing' state. Must call fail_session before returning.
            // Use spawn for fire-and-forget cleanup to avoid blocking the current task.
            let metadata = state.metadata.clone();
            let error_msg = format!("Failed to parse chunk hash: {e}");
            tokio::spawn(async move {
                if let Err(fail_err) = metadata
                    .fail_session(
                        upload_id,
                        "invalid_manifest",
                        Some(&error_msg),
                        OffsetDateTime::now_utc(),
                    )
                    .await
                {
                    tracing::warn!(
                        upload_id = %upload_id,
                        error = %fail_err,
                        "Failed to mark session as failed after manifest parse error"
                    );
                    tracing::debug!(
                        upload_id = %upload_id,
                        fail_err = %fail_err,
                        "session_fail_spawn_error: transient DB failure in cleanup task"
                    );
                    record_upload_error("session_fail_spawn_error");
                }
            });
            ApiError::BadRequest(format!("invalid chunk hash in manifest: {e}"))
        })?;

    // SECURITY FIX: Validate manifest chunks against expected_chunks (if predeclared).
    // This prevents "server-side copy" attacks where a client references chunks they never
    // declared, potentially accessing chunks from other uploads/caches.
    if session.chunks_predeclared {
        let expected_chunks = fail_session_on_error!(
            state.metadata.get_expected_chunks(upload_id).await,
            "metadata_fetch_failed"
        );
        let expected_set: std::collections::HashSet<&str> = expected_chunks
            .iter()
            .map(|c| c.chunk_hash.as_str())
            .collect();

        for chunk_hash in &chunk_hashes {
            let hex = chunk_hash.to_hex();
            if !expected_set.contains(hex.as_str()) {
                record_upload_error("manifest_chunk_not_in_expected");
                tracing::warn!(
                    upload_id = %upload_id,
                    chunk_hash = %hex,
                    trace_id = %trace_id,
                    "Commit failed: manifest references chunk not in expected_chunks"
                );
                // Mark session as failed
                let _ = state
                    .metadata
                    .fail_session(
                        upload_id,
                        "manifest_chunk_not_in_expected",
                        Some(&format!("chunk {} not in expected_chunks", hex)),
                        OffsetDateTime::now_utc(),
                    )
                    .await;
                return Err(ApiError::BadRequest(format!(
                    "manifest references chunk {} which was not in expected_chunks",
                    hex
                )));
            }
        }
    } else {
        // SECURITY FIX: When chunks are NOT predeclared (require_expected_chunks=false),
        // verify that ALL manifest chunks were actually received in this session.
        // This prevents cross-session chunk reference attacks where a malicious client could
        // reference chunks that exist globally but were never uploaded in the current session.
        //
        // The previous check only rejected deduped-only chunks (uploaded_data=false), but missed
        // the case where a chunk was never sent to upload_chunk at all - if the chunk exists
        // globally but has no row in upload_expected_chunks, it would pass through unchecked.
        let received = fail_session_on_error!(
            state.metadata.get_received_chunks(upload_id).await,
            "metadata_fetch_failed"
        );
        let received_set: std::collections::HashSet<&str> =
            received.iter().map(|s| s.as_str()).collect();

        let unreceived: Vec<_> = chunk_hashes
            .iter()
            .map(|h| h.to_hex())
            .filter(|h| !received_set.contains(h.as_str()))
            .collect();

        if !unreceived.is_empty() {
            record_upload_error("unauthorized_chunk_reference");
            tracing::warn!(
                upload_id = %upload_id,
                unreceived_count = unreceived.len(),
                trace_id = %trace_id,
                "Commit failed: manifest references chunks not received in this session"
            );
            let _ = state
                .metadata
                .fail_session(
                    upload_id,
                    "unauthorized_chunk_reference",
                    Some(&format!(
                        "{} chunk(s) referenced but not received in this session",
                        unreceived.len()
                    )),
                    OffsetDateTime::now_utc(),
                )
                .await;
            return Err(ApiError::BadRequest(format!(
                "manifest references {} chunk(s) not received in this session",
                unreceived.len()
            )));
        }
    }

    // Verify all chunks are present
    let missing = fail_session_on_error!(
        state.metadata.get_missing_chunks(upload_id).await,
        "metadata_fetch_failed"
    );
    if !missing.is_empty() {
        record_upload_error("incomplete_upload");
        tracing::warn!(
            upload_id = %upload_id,
            missing_count = missing.len(),
            trace_id = %trace_id,
            "Commit failed: upload incomplete"
        );
        // Mark session as failed
        let _ = state
            .metadata
            .fail_session(
                upload_id,
                "incomplete_upload",
                Some(&format!("{} chunks missing", missing.len())),
                OffsetDateTime::now_utc(),
            )
            .await;
        return Err(ApiError::IncompleteUpload {
            missing: missing.len(),
        });
    }

    // Verify NAR hash by streaming chunks in order
    // Use streaming compression to avoid buffering entire NAR in memory
    let expected_nar_hash =
        fail_session_on_error!(NarHash::from_sri(&session.nar_hash), "invalid_nar_hash");
    let mut nar_hasher = NarHasher::new();
    let mut total_bytes_read: u64 = 0;
    let compression_config = state.config.server.compression;
    let needs_compression = compression_config != cellar_core::config::CompressionConfig::None;

    // Check if expected_chunks were pre-defined for this upload.
    // If they were, we can distinguish between server errors (chunk unexpectedly missing)
    // and client errors (manifest references unknown chunk).
    // NOTE: We use the chunks_predeclared flag instead of checking if expected_chunks table
    // is non-empty, because dynamic uploads (without pre-declared chunks) will INSERT rows
    // as chunks arrive, making the table non-empty even though chunks were never pre-declared.
    let has_expected_chunks = session.chunks_predeclared;

    // OPTIMIZATION: Batch fetch all chunk metadata in a single query instead of N queries.
    // This reduces database round-trips from N to 1, significantly improving commit latency
    // for large manifests with thousands of chunks.
    let chunk_hash_strings: Vec<String> = chunk_hashes.iter().map(|h| h.to_hex()).collect();
    let chunks_metadata = fail_session_on_error!(
        state
            .metadata
            .get_chunks_batch(domain_id, &chunk_hash_strings)
            .await,
        "metadata_fetch_failed"
    );

    // Create true streaming compressor if compression is enabled.
    // This compresses data and streams it directly to storage, avoiding memory pressure.
    // Wrapped in StreamingCompressorGuard for automatic cleanup on early returns.
    let temp_nar_key = format!("tmp/nar/{}", upload_id);
    let mut streaming_compressor_guard = if needs_compression {
        let upload = match state.storage.put_stream(&temp_nar_key).await {
            Ok(u) => u,
            Err(e) => {
                let error_msg = format!("failed to create streaming upload: {e}");
                let metadata = state.metadata.clone();
                tokio::spawn(async move {
                    let _ = metadata
                        .fail_session(
                            upload_id,
                            "storage_init_failed",
                            Some(&error_msg),
                            OffsetDateTime::now_utc(),
                        )
                        .await;
                });
                return Err(ApiError::Internal(format!(
                    "failed to create streaming upload: {e}"
                )));
            }
        };
        let compressor = crate::compression::TrueStreamingCompressor::new(
            upload,
            compression_config,
            temp_nar_key.clone(),
        );
        Some(crate::compression::StreamingCompressorGuard::new(
            compressor,
        ))
    } else {
        None
    };

    for (chunk_index, chunk_hash) in chunk_hashes.iter().enumerate() {
        // Get chunk location from pre-fetched metadata (O(1) HashMap lookup instead of DB query)
        let chunk_hex = chunk_hash.to_hex();
        let chunk_info = chunks_metadata.get(&chunk_hex).ok_or_else(|| {
            // CRITICAL: Session is in 'committing' state. Must call fail_session before returning.
            // Use spawn for fire-and-forget cleanup to avoid blocking the current task.
            let error_code = if has_expected_chunks {
                "chunk_missing_from_metadata"
            } else {
                "unknown_chunk_in_manifest"
            };
            let error_detail = format!("chunk {} missing", chunk_hex);

            let metadata = state.metadata.clone();
            let error_detail_clone = error_detail.clone();
            tokio::spawn(async move {
                let _ = metadata
                    .fail_session(
                        upload_id,
                        error_code,
                        Some(&error_detail_clone),
                        OffsetDateTime::now_utc(),
                    )
                    .await;
            });

            if has_expected_chunks {
                // Server knew what to expect but chunk is missing - internal error
                ApiError::Internal(format!(
                    "chunk {} unexpectedly missing from metadata",
                    chunk_hex
                ))
            } else {
                // Client provided manifest with unknown chunk - client error
                ApiError::BadRequest(format!("manifest references unknown chunk: {}", chunk_hex))
            }
        })?;

        let object_key = chunk_info.object_key.as_ref().ok_or_else(|| {
            // CRITICAL: Session is in 'committing' state. Must call fail_session before returning.
            // Use spawn for fire-and-forget cleanup to avoid blocking the current task.
            let error_detail = format!("chunk {} has no object key", chunk_hex);
            let metadata = state.metadata.clone();
            let error_detail_clone = error_detail.clone();
            tokio::spawn(async move {
                let _ = metadata
                    .fail_session(
                        upload_id,
                        "missing_object_key",
                        Some(&error_detail_clone),
                        OffsetDateTime::now_utc(),
                    )
                    .await;
            });
            ApiError::Internal(error_detail)
        })?;

        // Read chunk data from storage
        let chunk_data = state.storage.get(object_key).await.map_err(|e| {
            // CRITICAL: Session is in 'committing' state. Must call fail_session before returning.
            // Use spawn for fire-and-forget cleanup to avoid blocking the current task.
            let metadata = state.metadata.clone();
            let error_msg = format!("Failed to read chunk {}: {e}", chunk_hex);
            tokio::spawn(async move {
                let _ = metadata
                    .fail_session(
                        upload_id,
                        "storage_read_failure",
                        Some(&error_msg),
                        OffsetDateTime::now_utc(),
                    )
                    .await;
            });
            ApiError::Internal(format!("failed to read chunk from storage: {e}"))
        })?;
        total_bytes_read += chunk_data.len() as u64;
        nar_hasher.update(&chunk_data);

        // Stream chunk through compressor if compression is enabled
        if let Some(comp) = streaming_compressor_guard.as_mut().and_then(|g| g.as_mut()) {
            comp.write_chunk(&chunk_data).await.map_err(|e| {
                // CRITICAL: Session is in 'committing' state. Must call fail_session.
                // The StreamingCompressorGuard will automatically abort the upload on drop.
                let metadata = state.metadata.clone();
                let error_msg = format!("Compression failed: {e}");
                tokio::spawn(async move {
                    let _ = metadata
                        .fail_session(
                            upload_id,
                            "compression_failure",
                            Some(&error_msg),
                            OffsetDateTime::now_utc(),
                        )
                        .await;
                });
                ApiError::Internal(format!("compression failed: {e}"))
            })?;
        }

        // Update progress every 100 chunks to show liveness during hash/streaming phase.
        // This prevents committing_gc from misclassifying long commits as stuck (Issue #4 fix).
        // For large uploads (>1000 chunks), this provides regular heartbeats without
        // overwhelming the database with updates.
        if (chunk_index + 1) % 100 == 0 {
            let progress = serde_json::json!({
                "phase": "hashing_and_streaming",
                "chunks_done": chunk_index + 1,
                "chunks_total": chunk_hashes.len(),
                "bytes_processed": total_bytes_read,
            });
            // Progress updates are best-effort. If they fail, log but continue processing.
            if let Err(e) = state
                .metadata
                .update_commit_progress(upload_id, &progress.to_string(), OffsetDateTime::now_utc())
                .await
            {
                tracing::warn!(
                    upload_id = %upload_id,
                    error = %e,
                    "Failed to update commit progress during hash/streaming phase"
                );
            }
        }
    }

    // Validate that total bytes matches declared nar_size
    let expected_nar_size = session.nar_size as u64;
    if total_bytes_read != expected_nar_size {
        // StreamingCompressorGuard will automatically abort on drop
        record_upload_error("nar_size_mismatch");
        tracing::warn!(
            upload_id = %upload_id,
            expected_size = expected_nar_size,
            actual_size = total_bytes_read,
            trace_id = %trace_id,
            "Commit failed: NAR size mismatch"
        );
        // Mark session as failed
        let _ = state
            .metadata
            .fail_session(
                upload_id,
                "nar_size_mismatch",
                Some(&format!(
                    "Expected {} bytes but got {}",
                    expected_nar_size, total_bytes_read
                )),
                OffsetDateTime::now_utc(),
            )
            .await;
        return Err(ApiError::BadRequest(format!(
            "NAR size mismatch: declared {} bytes but chunks total {} bytes",
            expected_nar_size, total_bytes_read
        )));
    }

    let actual_nar_hash = nar_hasher.finalize();
    if actual_nar_hash != expected_nar_hash {
        // StreamingCompressorGuard will automatically abort on drop
        record_upload_error("nar_hash_mismatch");
        tracing::warn!(
            upload_id = %upload_id,
            expected_hash = %expected_nar_hash.to_sri(),
            actual_hash = %actual_nar_hash.to_sri(),
            trace_id = %trace_id,
            "Commit failed: NAR hash mismatch"
        );
        // Mark session as failed
        let _ = state
            .metadata
            .fail_session(
                upload_id,
                "nar_hash_mismatch",
                Some(&format!(
                    "Expected {} but got {}",
                    expected_nar_hash.to_sri(),
                    actual_nar_hash.to_sri()
                )),
                OffsetDateTime::now_utc(),
            )
            .await;
        return Err(ApiError::HashMismatch {
            expected: expected_nar_hash.to_sri(),
            actual: actual_nar_hash.to_sri(),
        });
    }

    // Compute manifest hash
    let manifest_hash = ManifestHash::compute(&chunk_hashes);

    // Verify manifest hash if provided at session creation
    if let Some(expected) = &session.manifest_hash {
        let expected_hash =
            fail_session_on_error!(ManifestHash::from_hex(expected), "invalid_manifest_hash");
        if manifest_hash != expected_hash {
            // StreamingCompressorGuard will automatically abort on drop
            // Mark session as failed since we're in 'committing' state
            let _ = state
                .metadata
                .fail_session(
                    upload_id,
                    "manifest_hash_mismatch",
                    Some(&format!(
                        "Expected {} but got {}",
                        expected,
                        manifest_hash.to_hex()
                    )),
                    OffsetDateTime::now_utc(),
                )
                .await;
            return Err(ApiError::HashMismatch {
                expected: expected.clone(),
                actual: manifest_hash.to_hex(),
            });
        }
    }

    let now = OffsetDateTime::now_utc();
    let store_path =
        fail_session_on_error!(StorePath::parse(&session.store_path), "invalid_store_path");

    // Initialize artifact tracker for compensation on failure (early to avoid scope issues)
    let mut artifacts = CommitArtifacts::new();
    artifacts.domain_id = Some(domain_id);

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
        domain_id,
        manifest_hash: manifest_hash.to_hex(),
        chunk_size: session.chunk_size,
        chunk_count: chunk_hashes.len() as i32,
        nar_size: session.nar_size,
        object_key: Some(manifest_hash.to_object_key_with_domain(domain_id)),
        refcount: 0,
        created_at: now,
    };

    let chunk_mappings: Vec<ManifestChunkRow> = chunk_hashes
        .iter()
        .enumerate()
        .map(|(i, hash)| ManifestChunkRow {
            domain_id,
            manifest_hash: manifest_hash.to_hex(),
            position: i as i32,
            chunk_hash: hash.to_hex(),
        })
        .collect();

    // CRITICAL FIX: Persist manifest_hash in commit_progress BEFORE creating manifest.
    // This ensures CommittingGC can rollback the manifest refcount if a crash occurs
    // between create_manifest_with_initial_refcount and the next commit_progress update.
    // Without this, crashes in that window would leak manifest refcounts.
    let manifest_prep_progress = try_with_cleanup!(create_commit_progress_json(
        "preparing_manifest_creation",
        0,
        0,
        &[], // No chunk refcounts yet
        Some(&manifest_hash.to_hex())
    ));
    try_with_cleanup!(
        state
            .metadata
            .update_commit_progress(
                upload_id,
                &manifest_prep_progress,
                OffsetDateTime::now_utc()
            )
            .await
    );

    // ATOMIC: Create manifest and initialize manifest refcount in a single transaction.
    // Previously, there was a race window between create_manifest() and refcount increment
    // where GC could delete manifests during reuploads. This atomic method eliminates that race.
    let manifest_created = try_with_cleanup!(
        state
            .metadata
            .create_manifest_with_initial_refcount(&manifest_row, &chunk_mappings)
            .await
    );

    // Track manifest hash for rollback on failure (Issue #2 fix).
    // If commit fails after this point, cleanup() will decrement the manifest refcount.
    artifacts.manifest_hash = Some(manifest_hash.to_hex());

    // Increment chunk refcounts only when the manifest is newly created.
    // Chunk refcounts track manifest presence within a domain; re-uploads only
    // increment the manifest refcount and do not add additional chunk refs.
    // Atomically increment all chunk refcounts in a single transaction to avoid
    // partial increments if the server crashes mid-commit.
    let hex_hashes: Vec<String> = chunk_hashes.iter().map(|h| h.to_hex()).collect();
    let refcount_hashes = if manifest_created {
        hex_hashes.clone()
    } else {
        Vec::new()
    };

    // Initialize commit_progress with the refcount list BEFORE incrementing.
    // If the server crashes after refcounts are incremented but before commit_progress is saved,
    // the recovery path would not know what to rollback, causing zombie refcounts.
    //
    // By persisting the refcount list BEFORE incrementing, we ensure recovery can always rollback:
    // - If crash during batch increment: refcounts not fully incremented (atomic all-or-nothing),
    //   recovery will attempt rollback but decrements will be no-ops (safe).
    // - If crash after batch increment: refcounts incremented, commit_progress has full list,
    //   recovery can successfully rollback all refcounts.
    let progress_json_before = try_with_cleanup!(create_commit_progress_json(
        "preparing_refcount_increment",
        0,                     // No refcounts actually incremented yet
        refcount_hashes.len(), // Total refcounts to increment
        &refcount_hashes,      // Full list of what we're ABOUT to increment
        Some(&manifest_hash.to_hex())
    ));
    try_with_cleanup!(
        state
            .metadata
            .update_commit_progress(upload_id, &progress_json_before, OffsetDateTime::now_utc())
            .await
    );

    // Now batch increment all refcounts atomically
    // If this succeeds, all refcounts are incremented
    // If this fails, artifacts cleanup will use commit_progress to rollback
    try_with_cleanup!(
        state
            .metadata
            .batch_increment_refcounts(domain_id, &refcount_hashes)
            .await
    );

    // Track incremented refcounts for in-memory cleanup tracking
    artifacts.incremented_refcounts = refcount_hashes.clone();

    // Update commit progress to show completion with validation
    // Now that refcounts are actually incremented, update phase to reflect completion
    let progress_json_after = try_with_cleanup!(create_commit_progress_json(
        "refcounts_incremented",
        refcount_hashes.len(),
        refcount_hashes.len(), // All refcounts now incremented
        &artifacts.incremented_refcounts,
        Some(&manifest_hash.to_hex())
    ));
    try_with_cleanup!(
        state
            .metadata
            .update_commit_progress(upload_id, &progress_json_after, OffsetDateTime::now_utc())
            .await
    );

    // Only store manifest JSON if we created it (storage optimization - avoid redundant uploads)
    if manifest_created {
        let manifest = Manifest::new(
            chunk_hashes.clone(),
            session.chunk_size as u64,
            session.nar_size as u64,
        );
        let manifest_json = try_with_cleanup!(manifest.to_json());
        try_with_cleanup!(
            state
                .storage
                .put(
                    &manifest_hash.to_object_key_with_domain(domain_id),
                    Bytes::from(manifest_json),
                )
                .await
        );
    }

    // Check for existing visible store path to handle re-uploads correctly.
    // For re-uploads, create_store_path preserves both the old metadata AND visibility='visible'.
    // This ensures the original content continues to be served without 404s during the upload.
    // Only complete_reupload atomically updates metadata to the new content when commit succeeds.
    let existing = try_with_cleanup!(
        state
            .metadata
            .get_store_path(session.cache_id, store_path.hash().as_str())
            .await
    );

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
        domain_id,
        store_path: session.store_path.clone(),
        nar_hash: session.nar_hash.clone(),
        nar_size: session.nar_size,
        manifest_hash: manifest_hash.to_hex(),
        created_at,
        committed_at: Some(now),
        visibility_state: initial_visibility.to_string(),
        uploader_token_id: session.owner_token_id,
        ca: None, // Content-addressable field, populated from narinfo if present
        chunks_verified: false, // Set to true later by mark_chunks_verified() after storage completes
    };

    try_with_cleanup!(state.metadata.create_store_path(&store_path_row).await);

    // Generate and sign narinfo
    let nar_hash = try_with_cleanup!(NarHash::from_sri(&session.nar_hash));
    let mut narinfo = if let Some(guard) = streaming_compressor_guard {
        // Track temp key for cleanup in case of failure
        artifacts.temp_nar_key = Some(temp_nar_key.clone());

        // Finalize streaming compression with abort protection
        // The guard's finish() ensures the upload is aborted if finish() fails
        let compression_result = match guard.finish().await {
            Ok(r) => r,
            Err(e) => {
                artifacts.cleanup(&state, upload_id, &trace_id.0).await;
                return Err(ApiError::Internal(format!(
                    "compression finalization failed: {e}"
                )));
            }
        };

        // Copy from temp key to final content-addressed key
        let compression = compression_config.to_compression();
        let extension = compression.extension();
        let nar_key = format!(
            "{}/nar/{}.nar{}",
            cache_prefix(session.cache_id),
            store_path.hash(),
            extension
        );
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

    // Populate narinfo references and deriver from commit request (before signing)
    if let Some(ref refs) = body.references {
        for ref_path_str in refs {
            match StorePath::parse(ref_path_str) {
                Ok(sp) => narinfo.references.push(sp),
                Err(e) => {
                    tracing::warn!(
                        upload_id = %upload_id,
                        reference = %ref_path_str,
                        error = %e,
                        "Ignoring invalid reference store path"
                    );
                }
            }
        }
    }
    if let Some(ref deriver_str) = body.deriver {
        match StorePath::parse(deriver_str) {
            Ok(sp) => narinfo.deriver = Some(sp),
            Err(e) => {
                tracing::warn!(
                    upload_id = %upload_id,
                    deriver = %deriver_str,
                    error = %e,
                    "Ignoring invalid deriver store path"
                );
            }
        }
    }

    if let Some(signer) = &state.signer {
        signer.sign(&mut narinfo);
    }

    // Store narinfo
    let narinfo_key = format!(
        "{}/narinfo/{}.narinfo",
        cache_prefix(session.cache_id),
        store_path.hash()
    );
    let narinfo_text = narinfo.to_narinfo_text();
    try_with_cleanup!(
        state
            .storage
            .put(&narinfo_key, Bytes::from(narinfo_text))
            .await
    );
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

    // Mark chunks as verified now that all storage operations have completed.
    // This flag indicates the NAR can be safely served without risk of truncation.
    try_with_cleanup!(
        state
            .metadata
            .mark_chunks_verified(session.cache_id, store_path.hash().as_str())
            .await
    );

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

    // Store references in DB (best-effort, don't abort commit on failure)
    if !narinfo.references.is_empty() {
        let ref_rows: Vec<StorePathReferenceRow> = narinfo
            .references
            .iter()
            .map(|r| StorePathReferenceRow {
                cache_id: session.cache_id,
                store_path_hash: store_path.hash().to_string(),
                reference_hash: r.hash().to_string(),
                reference_type: "reference".to_string(),
            })
            .collect();

        if let Err(e) = state
            .metadata
            .add_references(session.cache_id, store_path.hash().as_str(), &ref_rows)
            .await
        {
            tracing::warn!(
                upload_id = %upload_id,
                error = %e,
                trace_id = %trace_id,
                "Failed to store references in DB (best-effort)"
            );
        }
    }

    if let Some(ref deriver_sp) = narinfo.deriver {
        let deriver_row = StorePathReferenceRow {
            cache_id: session.cache_id,
            store_path_hash: store_path.hash().to_string(),
            reference_hash: deriver_sp.hash().to_string(),
            reference_type: "deriver".to_string(),
        };
        if let Err(e) = state
            .metadata
            .add_references(session.cache_id, store_path.hash().as_str(), &[deriver_row])
            .await
        {
            tracing::warn!(
                upload_id = %upload_id,
                error = %e,
                trace_id = %trace_id,
                "Failed to store deriver in DB (best-effort)"
            );
        }
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
