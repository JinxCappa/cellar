//! Administrative endpoints.

use crate::auth::require_auth;
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::Json;
use cellar_core::token::TokenScope;
use cellar_metadata::models::{GcJobRow, TokenRow};
use cellar_metadata::repos::gc::{GcJobState, GcJobType, GcStats};
use cellar_storage::error::StorageError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use uuid::Uuid;

/// Maximum request body size for admin endpoints (1 MiB).
const MAX_ADMIN_BODY_SIZE: usize = 1024 * 1024;

/// Health check response.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: &'static str,
}

/// GET /v1/admin/health - Health check.
pub async fn health_check(State(state): State<AppState>) -> ApiResult<Json<HealthResponse>> {
    // Check metadata store connectivity
    state.metadata.health_check().await?;

    Ok(Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    }))
}

/// Metrics response.
#[derive(Debug, Serialize)]
pub struct MetricsResponse {
    // Database stats
    pub store_paths_count: u64,
    pub chunks_count: u64,
    pub chunks_total_size: u64,
    pub chunks_referenced: u64,
    pub chunks_unreferenced: u64,
    // Upload metrics (from Prometheus counters)
    pub upload_sessions_created: u64,
    pub upload_sessions_committed: u64,
    pub upload_sessions_resumed: u64,
    pub upload_sessions_expired: u64,
    pub chunks_uploaded: u64,
    pub chunks_deduplicated: u64,
    pub bytes_uploaded: u64,
    pub bytes_deduplicated: u64,
    pub chunk_hash_mismatches: u64,
}

/// GET /v1/admin/metrics - Get cache metrics.
pub async fn get_metrics(
    State(state): State<AppState>,
    req: Request,
) -> ApiResult<Json<MetricsResponse>> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    // Scope metrics to the authenticated cache for tenant isolation
    let store_paths_count = state.metadata.count_store_paths(auth.token.cache_id).await?;
    // Note: chunk_stats remain global since chunks are shared across caches for deduplication
    let chunk_stats = state.metadata.get_stats().await?;

    // Get prometheus metrics
    use crate::metrics::{
        BYTES_DEDUPLICATED, BYTES_UPLOADED, CHUNKS_DEDUPLICATED, CHUNKS_UPLOADED,
        CHUNK_HASH_MISMATCHES, UPLOAD_SESSIONS_COMMITTED, UPLOAD_SESSIONS_CREATED,
        UPLOAD_SESSIONS_EXPIRED, UPLOAD_SESSIONS_RESUMED,
    };

    Ok(Json(MetricsResponse {
        store_paths_count,
        chunks_count: chunk_stats.count,
        chunks_total_size: chunk_stats.total_size,
        chunks_referenced: chunk_stats.referenced_count,
        chunks_unreferenced: chunk_stats.unreferenced_count,
        upload_sessions_created: UPLOAD_SESSIONS_CREATED.get(),
        upload_sessions_committed: UPLOAD_SESSIONS_COMMITTED.get(),
        upload_sessions_resumed: UPLOAD_SESSIONS_RESUMED.get(),
        upload_sessions_expired: UPLOAD_SESSIONS_EXPIRED.get(),
        chunks_uploaded: CHUNKS_UPLOADED.get(),
        chunks_deduplicated: CHUNKS_DEDUPLICATED.get(),
        bytes_uploaded: BYTES_UPLOADED.get(),
        bytes_deduplicated: BYTES_DEDUPLICATED.get(),
        chunk_hash_mismatches: CHUNK_HASH_MISMATCHES.get(),
    }))
}

/// Create token request.
#[derive(Debug, Deserialize)]
pub struct CreateTokenRequest {
    pub scopes: Vec<String>,
    pub expires_in_secs: Option<u64>,
    pub description: Option<String>,
}

/// Create token response.
#[derive(Debug, Serialize)]
pub struct CreateTokenResponse {
    pub token_id: String,
    pub token_secret: String,
    pub expires_at: Option<String>,
}

/// POST /v1/admin/tokens - Create a new token.
pub async fn create_token(
    State(state): State<AppState>,
    req: Request,
) -> ApiResult<(StatusCode, Json<CreateTokenResponse>)> {
    let auth = require_auth(&req)?.clone();
    auth.require_scope(TokenScope::CacheAdmin)?;

    let body: CreateTokenRequest = {
        let bytes = axum::body::to_bytes(req.into_body(), MAX_ADMIN_BODY_SIZE)
            .await
            .map_err(|e| ApiError::BadRequest(format!("failed to read body: {e}")))?;
        serde_json::from_slice(&bytes)
            .map_err(|e| ApiError::BadRequest(format!("invalid JSON: {e}")))?
    };

    // Validate all scopes before storing - reject unknown scopes
    for scope in &body.scopes {
        TokenScope::parse(scope).map_err(|_| {
            ApiError::BadRequest(format!("invalid scope: {scope}"))
        })?;
    }

    // Generate token secret
    let token_secret = generate_token_secret();
    let token_hash = hash_token(&token_secret);

    let now = OffsetDateTime::now_utc();
    let expires_at = match body.expires_in_secs {
        Some(secs) => {
            let secs_i64: i64 = secs.try_into().map_err(|_| {
                ApiError::BadRequest(format!(
                    "expires_in_secs too large: {secs} exceeds maximum of {}",
                    i64::MAX
                ))
            })?;
            Some(now + time::Duration::seconds(secs_i64))
        }
        None => None,
    };

    let token_id = Uuid::new_v4();

    let scopes_json = serde_json::to_string(&body.scopes)
        .map_err(|e| ApiError::Internal(format!("failed to serialize scopes: {e}")))?;

    let token_row = TokenRow {
        token_id,
        cache_id: auth.token.cache_id,
        token_hash,
        scopes: scopes_json,
        expires_at,
        revoked_at: None,
        created_at: now,
        last_used_at: None,
        description: body.description,
    };

    state.metadata.create_token(&token_row).await?;

    let expires_at_str = expires_at
        .map(|t| {
            t.format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| ApiError::Internal(format!("failed to format expires_at: {e}")))
        })
        .transpose()?;

    Ok((
        StatusCode::CREATED,
        Json(CreateTokenResponse {
            token_id: token_id.to_string(),
            token_secret,
            expires_at: expires_at_str,
        }),
    ))
}

/// DELETE /v1/admin/tokens/{token_id} - Revoke a token.
pub async fn revoke_token(
    State(state): State<AppState>,
    Path(token_id): Path<String>,
    req: Request,
) -> ApiResult<StatusCode> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    let token_id =
        Uuid::parse_str(&token_id).map_err(|e| ApiError::BadRequest(format!("invalid token ID: {e}")))?;

    // Verify token ownership - only allow revoking tokens from the same cache
    let token = state
        .metadata
        .get_token(token_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("token not found".to_string()))?;

    if token.cache_id != auth.token.cache_id {
        return Err(ApiError::Forbidden(
            "cannot revoke token from another cache".to_string(),
        ));
    }

    state
        .metadata
        .revoke_token(token_id, OffsetDateTime::now_utc())
        .await?;

    Ok(StatusCode::NO_CONTENT)
}

/// Trigger GC request.
#[derive(Debug, Deserialize)]
pub struct TriggerGcRequest {
    pub job_type: String,
}

/// Trigger GC response.
#[derive(Debug, Serialize)]
pub struct TriggerGcResponse {
    pub job_id: String,
}

/// POST /v1/admin/gc - Trigger garbage collection.
pub async fn trigger_gc(
    State(state): State<AppState>,
    req: Request,
) -> ApiResult<(StatusCode, Json<TriggerGcResponse>)> {
    let auth = require_auth(&req)?.clone();
    auth.require_scope(TokenScope::CacheAdmin)?;

    let body: TriggerGcRequest = {
        let bytes = axum::body::to_bytes(req.into_body(), MAX_ADMIN_BODY_SIZE)
            .await
            .map_err(|e| ApiError::BadRequest(format!("failed to read body: {e}")))?;
        serde_json::from_slice(&bytes)
            .map_err(|e| ApiError::BadRequest(format!("invalid JSON: {e}")))?
    };

    let job_type = match body.job_type.as_str() {
        "upload_gc" => GcJobType::UploadGc,
        "chunk_gc" => GcJobType::ChunkGc,
        "manifest_gc" => GcJobType::ManifestGc,
        _ => return Err(ApiError::BadRequest("invalid job_type".to_string())),
    };

    let job_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();

    let job = GcJobRow {
        gc_job_id: job_id,
        cache_id: auth.token.cache_id,
        job_type: job_type.as_str().to_string(),
        state: GcJobState::Queued.as_str().to_string(),
        started_at: Some(now),
        finished_at: None,
        stats_json: None,
    };

    state.metadata.create_gc_job(&job).await?;

    // Spawn GC task
    let metadata = state.metadata.clone();
    let storage = state.storage.clone();
    let gc_config = state.config.gc.clone();

    tokio::spawn(async move {
        let result = run_gc_job(job_id, job_type, metadata.clone(), storage, gc_config).await;

        let (state, stats) = match result {
            Ok(stats) => (GcJobState::Finished, Some(stats)),
            Err(e) => {
                tracing::error!(job_id = %job_id, error = %e, "GC job failed");
                (GcJobState::Failed, None)
            }
        };

        let stats_json = stats.and_then(|s| {
            serde_json::to_string(&s)
                .map_err(|e| {
                    tracing::error!(job_id = %job_id, error = %e, "Failed to serialize GC stats");
                    e
                })
                .ok()
        });
        if let Err(e) = metadata
            .update_gc_job_state(
                job_id,
                state.as_str(),
                Some(OffsetDateTime::now_utc()),
                stats_json.as_deref(),
            )
            .await
        {
            tracing::error!(job_id = %job_id, error = %e, "Failed to update GC job state");
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(TriggerGcResponse {
            job_id: job_id.to_string(),
        }),
    ))
}

/// GC job status response.
#[derive(Debug, Serialize)]
pub struct GcJobResponse {
    pub job_id: String,
    pub job_type: String,
    pub state: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub stats: Option<GcStats>,
}

/// GET /v1/admin/gc/{job_id} - Get GC job status.
pub async fn get_gc_job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
    req: Request,
) -> ApiResult<Json<GcJobResponse>> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    let job_id =
        Uuid::parse_str(&job_id).map_err(|e| ApiError::BadRequest(format!("invalid job ID: {e}")))?;

    let job = state
        .metadata
        .get_gc_job(job_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("GC job not found".to_string()))?;

    // Verify GC job ownership - only allow viewing jobs from the same cache
    if job.cache_id != auth.token.cache_id {
        return Err(ApiError::Forbidden(
            "cannot view GC job from another cache".to_string(),
        ));
    }

    let stats: Option<GcStats> = job
        .stats_json
        .as_ref()
        .and_then(|s| serde_json::from_str(s).ok());

    let started_at_str = job
        .started_at
        .map(|t| {
            t.format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| ApiError::Internal(format!("failed to format started_at: {e}")))
        })
        .transpose()?;

    let finished_at_str = job
        .finished_at
        .map(|t| {
            t.format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| ApiError::Internal(format!("failed to format finished_at: {e}")))
        })
        .transpose()?;

    Ok(Json(GcJobResponse {
        job_id: job.gc_job_id.to_string(),
        job_type: job.job_type,
        state: job.state,
        started_at: started_at_str,
        finished_at: finished_at_str,
        stats,
    }))
}

/// Maximum number of GC iterations to prevent infinite loops.
const MAX_GC_ITERATIONS: u32 = 10000;

/// Run a GC job.
async fn run_gc_job(
    job_id: Uuid,
    job_type: GcJobType,
    metadata: std::sync::Arc<dyn cellar_metadata::MetadataStore>,
    storage: std::sync::Arc<dyn cellar_storage::ObjectStore>,
    config: cellar_core::config::GcConfig,
) -> Result<GcStats, ApiError> {
    let mut stats = GcStats::default();

    // Update job state to running
    metadata
        .update_gc_job_state(job_id, GcJobState::Running.as_str(), None, None)
        .await?;

    match job_type {
        GcJobType::UploadGc => {
            // Clean up expired upload sessions
            let mut iterations = 0;
            loop {
                iterations += 1;
                if iterations > MAX_GC_ITERATIONS {
                    tracing::warn!(
                        job_id = %job_id,
                        iterations = iterations,
                        "GC upload job exceeded max iterations, stopping to prevent infinite loop"
                    );
                    break;
                }

                let expired = metadata.get_expired_sessions(config.batch_size).await?;
                if expired.is_empty() {
                    break;
                }

                for session in expired {
                    stats.items_processed += 1;
                    if let Err(e) = metadata.delete_session(session.upload_id).await {
                        tracing::warn!(
                            job_id = %job_id,
                            session_id = %session.upload_id,
                            error = %e,
                            "Failed to delete expired session"
                        );
                        stats.errors += 1;
                    } else {
                        stats.items_deleted += 1;
                        // Record expired session metric
                        crate::metrics::UPLOAD_SESSIONS_EXPIRED.inc();
                    }
                }
            }
        }
        GcJobType::ChunkGc => {
            // Clean up unreferenced chunks
            let grace_time = OffsetDateTime::now_utc() - config.grace_period();
            let mut iterations = 0;

            loop {
                iterations += 1;
                if iterations > MAX_GC_ITERATIONS {
                    tracing::warn!(
                        job_id = %job_id,
                        iterations = iterations,
                        "GC chunk job exceeded max iterations, stopping to prevent infinite loop"
                    );
                    break;
                }

                let unreferenced = metadata
                    .get_unreferenced_chunks(grace_time, config.batch_size)
                    .await?;
                if unreferenced.is_empty() {
                    break;
                }

                for chunk in unreferenced {
                    stats.items_processed += 1;
                    let key = format!(
                        "chunks/{}/{}/{}",
                        &chunk.chunk_hash[..2],
                        &chunk.chunk_hash[2..4],
                        chunk.chunk_hash
                    );

                    // Delete from storage. NotFound is OK (already gone), other errors
                    // are logged but we still proceed with metadata cleanup to avoid orphans.
                    let storage_ok = match storage.delete(&key).await {
                        Ok(()) => true,
                        Err(StorageError::NotFound(_)) => {
                            tracing::debug!(
                                job_id = %job_id,
                                chunk_hash = %chunk.chunk_hash,
                                "Chunk already missing from storage, cleaning up metadata"
                            );
                            true
                        }
                        Err(e) => {
                            tracing::warn!(
                                job_id = %job_id,
                                chunk_hash = %chunk.chunk_hash,
                                error = %e,
                                "Failed to delete chunk from storage, proceeding with metadata cleanup"
                            );
                            stats.errors += 1;
                            false
                        }
                    };

                    // Always delete from metadata to prevent permanent orphaned rows
                    if let Err(e) = metadata.delete_chunk(&chunk.chunk_hash).await {
                        tracing::warn!(
                            job_id = %job_id,
                            chunk_hash = %chunk.chunk_hash,
                            error = %e,
                            "Failed to delete chunk from metadata"
                        );
                        stats.errors += 1;
                    } else if storage_ok {
                        // Only count as fully deleted if storage was successful or already gone
                        stats.items_deleted += 1;
                        stats.bytes_reclaimed += chunk.size_bytes as u64;
                    }
                }
            }
        }
        GcJobType::ManifestGc => {
            // Clean up orphaned manifests
            let mut iterations = 0;
            loop {
                iterations += 1;
                if iterations > MAX_GC_ITERATIONS {
                    tracing::warn!(
                        job_id = %job_id,
                        iterations = iterations,
                        "GC manifest job exceeded max iterations, stopping to prevent infinite loop"
                    );
                    break;
                }

                let orphaned = metadata.get_orphaned_manifests(config.batch_size).await?;
                if orphaned.is_empty() {
                    break;
                }

                for manifest in orphaned {
                    stats.items_processed += 1;

                    // Get chunks to decrement refcounts
                    let chunks = metadata.get_manifest_chunks(&manifest.manifest_hash).await?;
                    for chunk_hash in chunks {
                        if let Err(e) = metadata.decrement_refcount(&chunk_hash).await {
                            tracing::warn!(
                                job_id = %job_id,
                                manifest_hash = %manifest.manifest_hash,
                                chunk_hash = %chunk_hash,
                                error = %e,
                                "Failed to decrement chunk refcount"
                            );
                            stats.errors += 1;
                        }
                    }

                    // Delete manifest from storage
                    if let Some(key) = &manifest.object_key {
                        if let Err(e) = storage.delete(key).await {
                            tracing::warn!(
                                job_id = %job_id,
                                manifest_hash = %manifest.manifest_hash,
                                object_key = %key,
                                error = %e,
                                "Failed to delete manifest from storage"
                            );
                            stats.errors += 1;
                        }
                    }

                    // Delete from metadata
                    if let Err(e) = metadata.delete_manifest(&manifest.manifest_hash).await {
                        tracing::warn!(
                            job_id = %job_id,
                            manifest_hash = %manifest.manifest_hash,
                            error = %e,
                            "Failed to delete manifest from metadata"
                        );
                        stats.errors += 1;
                    } else {
                        stats.items_deleted += 1;
                    }
                }
            }

            // Also clean up failed store_paths that are older than the grace period.
            // Failed uploads should be retained for a grace period to allow debugging,
            // but cleaned up eventually to prevent accumulation.
            let grace_period_seconds = config.grace_period().whole_seconds();
            match metadata.delete_failed_store_paths_older_than(grace_period_seconds).await {
                Ok(deleted_count) => {
                    stats.items_deleted += deleted_count;
                    tracing::info!(
                        job_id = %job_id,
                        deleted = deleted_count,
                        grace_period_secs = grace_period_seconds,
                        "Cleaned up old failed store_paths"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        job_id = %job_id,
                        error = %e,
                        "Failed to clean up old failed store_paths"
                    );
                    stats.errors += 1;
                }
            }
        }
    }

    Ok(stats)
}

/// Generate a random token secret using cryptographically secure RNG.
fn generate_token_secret() -> String {
    use base64::Engine;
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Hash a token for storage.
fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    let result = hasher.finalize();
    result.iter().map(|b| format!("{b:02x}")).collect()
}
