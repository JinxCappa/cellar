//! Administrative endpoints.

use crate::auth::require_auth;
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use axum::Json;
use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use cellar_core::storage_domain::default_storage_domain_id;
use cellar_core::token::TokenScope;
use cellar_metadata::models::{CacheRow, DomainRow, GcJobRow, TokenRow};
use cellar_metadata::repos::chunks::ChunkRepo;
use cellar_metadata::repos::gc::{GcJobState, GcJobType, GcStats};
use cellar_metadata::repos::manifests::ManifestRepo;
use cellar_metadata::repos::uploads::UploadRepo;
use cellar_storage::error::StorageError;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use uuid::Uuid;

/// Maximum request body size for admin endpoints (1 MiB).
const MAX_ADMIN_BODY_SIZE: usize = 1024 * 1024;

/// Storage sweep checkpoint for resumable GC operations.
/// Tracks progress during chunk/manifest sweeps to allow resumption after crashes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepCheckpoint {
    /// Schema version for forward compatibility
    pub version: u8,
    /// Continuation token for chunks sweep (base64-encoded)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunks_token: Option<String>,
    /// Continuation token for manifests sweep (base64-encoded)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifests_token: Option<String>,
    /// Number of chunks processed so far
    pub chunks_processed: u64,
    /// Number of manifests processed so far
    pub manifests_processed: u64,
    /// Last checkpoint update timestamp (for debugging/monitoring)
    pub last_updated: String,
}

impl SweepCheckpoint {
    /// Current checkpoint schema version
    const VERSION: u8 = 1;

    /// Create a new empty checkpoint
    pub fn new() -> Self {
        Self {
            version: Self::VERSION,
            chunks_token: None,
            manifests_token: None,
            chunks_processed: 0,
            manifests_processed: 0,
            last_updated: OffsetDateTime::now_utc().to_string(),
        }
    }

    /// Deserialize checkpoint from JSON string
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        let checkpoint: Self = serde_json::from_str(json)?;
        // TODO: Handle version migration if needed in the future
        if checkpoint.version != Self::VERSION {
            tracing::warn!(
                version = checkpoint.version,
                expected = Self::VERSION,
                "Checkpoint version mismatch, may need migration"
            );
        }
        Ok(checkpoint)
    }

    /// Serialize checkpoint to JSON string
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Update the last_updated timestamp
    pub fn touch(&mut self) {
        self.last_updated = OffsetDateTime::now_utc().to_string();
    }
}

impl Default for SweepCheckpoint {
    fn default() -> Self {
        Self::new()
    }
}

/// Health check response.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: &'static str,
}

/// GET /v1/health - Health check.
///
/// This endpoint is intentionally unauthenticated to support:
/// - Kubernetes liveness/readiness probes
/// - Load balancer health checks
/// - Monitoring systems (Prometheus, Datadog, etc.)
///
/// Returns only non-sensitive information (status and version).
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
    let store_paths_count = state
        .metadata
        .count_store_paths(auth.token.cache_id)
        .await?;
    let domain_id = resolve_domain_id_for_cache(&*state.metadata, auth.token.cache_id).await?;
    let chunk_stats = state.metadata.get_stats(domain_id).await?;

    // Get prometheus metrics
    use crate::metrics::{
        BYTES_DEDUPLICATED, BYTES_UPLOADED, CHUNK_HASH_MISMATCHES, CHUNKS_DEDUPLICATED,
        CHUNKS_UPLOADED, UPLOAD_SESSIONS_COMMITTED, UPLOAD_SESSIONS_CREATED,
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

// =============================================================================
// Domain Management Types
// =============================================================================

/// Request to create a new storage domain.
#[derive(Debug, Deserialize)]
pub struct CreateDomainRequest {
    pub domain_name: String,
}

/// Request to update a storage domain.
#[derive(Debug, Deserialize)]
pub struct UpdateDomainRequest {
    pub domain_name: String,
}

/// Response for domain details (used by GET operations).
#[derive(Debug, Serialize)]
pub struct DomainResponse {
    pub domain_id: String,
    pub domain_name: String,
    pub is_default: bool,
    pub created_at: String,
    pub updated_at: String,
}

/// Response for listing domains.
#[derive(Debug, Serialize)]
pub struct ListDomainsResponse {
    pub domains: Vec<DomainResponse>,
}

fn domain_row_to_response(domain: DomainRow) -> ApiResult<DomainResponse> {
    let created_at = domain
        .created_at
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|e| ApiError::Internal(format!("failed to format created_at: {e}")))?;
    let updated_at = domain
        .updated_at
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|e| ApiError::Internal(format!("failed to format updated_at: {e}")))?;

    Ok(DomainResponse {
        domain_id: domain.domain_id.to_string(),
        domain_name: domain.domain_name,
        is_default: domain.is_default,
        created_at,
        updated_at,
    })
}

// =============================================================================
// Cache Management Types
// =============================================================================

/// Request to create a new cache.
#[derive(Debug, Deserialize)]
pub struct CreateCacheRequest {
    pub cache_name: String,
    pub public_base_url: Option<String>,
    #[serde(default)]
    pub is_public: bool,
    /// Set as the default public cache for unauthenticated requests.
    /// Only one cache can be the default at a time.
    #[serde(default)]
    pub is_default: bool,
    /// Optional storage domain ID for isolation.
    pub domain_id: Option<String>,
    /// Optional storage domain name for isolation.
    pub domain_name: Option<String>,
}

/// Response from creating a cache.
#[derive(Debug, Serialize)]
pub struct CreateCacheResponse {
    pub cache_id: String,
    pub cache_name: String,
    pub domain_id: String,
    pub domain_name: Option<String>,
    pub public_base_url: Option<String>,
    pub is_public: bool,
    pub is_default: bool,
    pub created_at: String,
    pub updated_at: String,
}

/// Request to update an existing cache.
#[derive(Debug, Deserialize)]
pub struct UpdateCacheRequest {
    pub cache_name: Option<String>,
    pub public_base_url: Option<String>,
    pub is_public: Option<bool>,
    /// Set as the default public cache. Setting to true when another cache
    /// is already default will fail - unset the other cache first.
    pub is_default: Option<bool>,
    /// Optional storage domain ID for isolation.
    pub domain_id: Option<String>,
    /// Optional storage domain name for isolation.
    pub domain_name: Option<String>,
}

/// Response for cache details (used by GET operations).
#[derive(Debug, Serialize)]
pub struct CacheResponse {
    pub cache_id: String,
    pub cache_name: String,
    pub domain_id: String,
    pub domain_name: Option<String>,
    pub public_base_url: Option<String>,
    pub is_public: bool,
    pub is_default: bool,
    pub created_at: String,
    pub updated_at: String,
}

/// Response for listing caches.
#[derive(Debug, Serialize)]
pub struct ListCachesResponse {
    pub caches: Vec<CacheResponse>,
}

/// Create token request.
#[derive(Debug, Deserialize)]
pub struct CreateTokenRequest {
    pub scopes: Vec<String>,
    pub cache_id: Option<String>,
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

/// Token listing response.
#[derive(Debug, Serialize)]
pub struct TokenInfo {
    pub token_id: String,
    pub cache_id: Option<String>,
    pub scopes: Vec<String>,
    pub expires_at: Option<String>,
    pub revoked_at: Option<String>,
    pub created_at: String,
    pub last_used_at: Option<String>,
    pub description: Option<String>,
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
        TokenScope::parse(scope)
            .map_err(|_| ApiError::BadRequest(format!("invalid scope: {scope}")))?;
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

    // Determine final cache_id based on authorization:
    // - Global admin (auth.token.cache_id = None): can specify any cache_id or None
    // - Cache-scoped admin: must use own cache_id (ignore request)
    let final_cache_id = if auth.token.cache_id.is_some() {
        // Cache-scoped admin: always use own cache_id
        auth.token.cache_id
    } else {
        // Global admin: parse requested cache_id if provided
        body.cache_id
            .as_ref()
            .map(|id_str| Uuid::parse_str(id_str))
            .transpose()
            .map_err(|e| ApiError::BadRequest(format!("invalid cache_id: {e}")))?
    };

    // Prevent creation of cache-scoped admin tokens
    if final_cache_id.is_some() && body.scopes.iter().any(|s| s == "cache:admin") {
        return Err(ApiError::BadRequest(
            "cache:admin scope requires global token (cache_id = None)".to_string(),
        ));
    }

    // Validate cache exists if cache_id provided (prevent orphaned tokens)
    if let Some(id) = final_cache_id
        && state.metadata.get_cache(id).await?.is_none()
    {
        return Err(ApiError::BadRequest(format!("cache not found: {id}")));
    }

    let scopes_json = serde_json::to_string(&body.scopes)
        .map_err(|e| ApiError::Internal(format!("failed to serialize scopes: {e}")))?;

    let token_row = TokenRow {
        token_id,
        cache_id: final_cache_id,
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

/// GET /v1/admin/tokens - List tokens.
pub async fn list_tokens(
    State(state): State<AppState>,
    req: Request,
) -> ApiResult<Json<Vec<TokenInfo>>> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    let cache_filter = if auth.token.cache_id.is_some() {
        auth.token.cache_id
    } else {
        None
    };

    let tokens = state.metadata.list_tokens(cache_filter).await?;

    let response: Result<Vec<TokenInfo>, ApiError> = tokens
        .into_iter()
        .map(|token| {
            let scopes: Vec<String> = serde_json::from_str(&token.scopes)
                .map_err(|e| ApiError::Internal(format!("invalid token scopes: {e}")))?;

            let expires_at = token
                .expires_at
                .map(|t| {
                    t.format(&time::format_description::well_known::Rfc3339)
                        .map_err(|e| {
                            ApiError::Internal(format!("failed to format expires_at: {e}"))
                        })
                })
                .transpose()?;
            let revoked_at = token
                .revoked_at
                .map(|t| {
                    t.format(&time::format_description::well_known::Rfc3339)
                        .map_err(|e| {
                            ApiError::Internal(format!("failed to format revoked_at: {e}"))
                        })
                })
                .transpose()?;
            let created_at = token
                .created_at
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| ApiError::Internal(format!("failed to format created_at: {e}")))?;
            let last_used_at = token
                .last_used_at
                .map(|t| {
                    t.format(&time::format_description::well_known::Rfc3339)
                        .map_err(|e| {
                            ApiError::Internal(format!("failed to format last_used_at: {e}"))
                        })
                })
                .transpose()?;

            Ok(TokenInfo {
                token_id: token.token_id.to_string(),
                cache_id: token.cache_id.map(|id| id.to_string()),
                scopes,
                expires_at,
                revoked_at,
                created_at,
                last_used_at,
                description: token.description,
            })
        })
        .collect();

    Ok(Json(response?))
}

/// DELETE /v1/admin/tokens/{token_id} - Revoke a token.
pub async fn revoke_token(
    State(state): State<AppState>,
    Path(token_id): Path<String>,
    req: Request,
) -> ApiResult<StatusCode> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    let token_id = Uuid::parse_str(&token_id)
        .map_err(|e| ApiError::BadRequest(format!("invalid token ID: {e}")))?;

    // Verify token ownership - only allow revoking tokens from the same cache
    let token = state
        .metadata
        .get_token(token_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("token not found".to_string()))?;

    // CRITICAL FIX: Global admins (cache_id = None) can revoke ANY token.
    // Cache-scoped admins (cache_id = Some) can only revoke tokens from their own cache.
    // Previously: `token.cache_id != auth.token.cache_id` prevented global admins from
    // revoking cache-scoped tokens (None != Some(uuid) is always true).
    if auth.token.cache_id.is_some() && token.cache_id != auth.token.cache_id {
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
    /// Optional target cache_id for GC (global admin only).
    /// If not specified, uses the token's cache_id (or None for global admin).
    pub cache_id: Option<String>,
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
        "committing_gc" => GcJobType::CommittingGc,
        "storage_sweep" => GcJobType::StorageSweep,
        _ => return Err(ApiError::BadRequest("invalid job_type".to_string())),
    };

    // Determine target cache_id for the GC job:
    // - Cache-scoped tokens can only GC their own cache
    // - Global admin tokens can target any cache via body.cache_id, or default to None (global)
    let target_cache_id = if auth.token.cache_id.is_some() {
        // Cache-scoped token: can only GC own cache, ignore body.cache_id
        auth.token.cache_id
    } else {
        // Global admin: use body.cache_id if provided, otherwise None (global resources)
        body.cache_id
            .as_ref()
            .map(|id| Uuid::parse_str(id))
            .transpose()
            .map_err(|e| ApiError::BadRequest(format!("invalid cache_id: {e}")))?
    };

    // REMOVED: Racy check for active jobs (TOCTOU vulnerability - P0 Issue #2).
    // Database constraint now enforces job isolation atomically:
    // - Unique index: prevents duplicate jobs of same type for same cache
    //   (allows different job types to run concurrently, e.g., chunk_gc + manifest_gc)
    // FIXED (P2): Removed overly restrictive EXCLUDE constraint that blocked ALL job types.
    // Let database reject concurrent job creation attempts of the same type.

    let job_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();

    let job = GcJobRow {
        gc_job_id: job_id,
        cache_id: target_cache_id,
        job_type: job_type.as_str().to_string(),
        state: GcJobState::Queued.as_str().to_string(),
        started_at: Some(now),
        finished_at: None,
        stats_json: None,
        sweep_checkpoint_json: None,
    };

    // Create job - database will reject if another job of the same type is active (via unique index)
    if let Err(e) = state.metadata.create_gc_job(&job).await {
        // Map Constraint errors to user-friendly message
        if matches!(e, cellar_metadata::error::MetadataError::Constraint(_)) {
            return Err(ApiError::Conflict(format!(
                "Another {} job is already active for this cache. Please wait for it to complete.",
                body.job_type
            )));
        }
        return Err(e.into());
    }

    // Increment active GC jobs metric
    crate::metrics::GC_JOBS_ACTIVE.inc();

    // Spawn GC task and register with watchdog for panic detection (P2 Issue #7)
    let metadata = state.metadata.clone();
    let storage = state.storage.clone();
    let gc_config = state.config.gc.clone();
    let gc_registry = state.gc_task_registry.clone();
    let start_time = std::time::Instant::now();

    let handle = tokio::spawn(async move {
        let result = run_gc_job(
            job_id,
            job_type,
            target_cache_id,
            metadata.clone(),
            storage,
            gc_config,
        )
        .await;

        let (state, stats) = match result {
            Ok(stats) => {
                if stats.errors > 0 {
                    tracing::warn!(
                        job_id = %job_id,
                        errors = stats.errors,
                        "GC job completed with errors, marking as failed"
                    );
                    (GcJobState::Failed, Some(stats))
                } else {
                    (GcJobState::Finished, Some(stats))
                }
            }
            Err(e) => {
                tracing::error!(job_id = %job_id, error = %e, "GC job failed");
                (GcJobState::Failed, None)
            }
        };

        // Record metrics
        let duration = start_time.elapsed().as_secs_f64();
        crate::metrics::GC_JOB_DURATION
            .with_label_values(&[job_type.as_str(), state.as_str()])
            .observe(duration);

        if let Some(ref s) = stats {
            crate::metrics::GC_ITEMS_DELETED
                .with_label_values(&[job_type.as_str()])
                .inc_by(s.items_deleted);
            crate::metrics::GC_BYTES_RECLAIMED
                .with_label_values(&[job_type.as_str()])
                .inc_by(s.bytes_reclaimed);
        }

        // Decrement active jobs metric
        crate::metrics::GC_JOBS_ACTIVE.dec();

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
                None, // sweep_checkpoint_json
            )
            .await
        {
            tracing::error!(job_id = %job_id, error = %e, "Failed to update GC job state");
        }
    });

    // Register task with watchdog for panic detection
    gc_registry.register(job_id, handle).await;

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
    pub cache_id: Option<String>,
    pub job_type: String,
    pub state: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub stats: Option<GcStats>,
}

/// Query parameters for listing GC jobs.
#[derive(Debug, Deserialize)]
pub struct ListGcJobsParams {
    /// Filter by cache_id (global admin only). If not specified, uses token's cache_id.
    pub cache_id: Option<String>,
    /// Maximum number of jobs to return (default: 20, max: 100).
    pub limit: Option<u32>,
}

/// GET /v1/admin/gc - List recent GC jobs.
pub async fn list_gc_jobs(
    State(state): State<AppState>,
    Query(params): Query<ListGcJobsParams>,
    req: Request,
) -> ApiResult<Json<Vec<GcJobResponse>>> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    // Determine target cache_id for filtering:
    // - Cache-scoped admin can only see their own cache's jobs
    // - Global admin can filter by cache_id or see all if not specified
    let target_cache_id = if auth.token.cache_id.is_some() {
        // Cache-scoped: always filter to own cache
        auth.token.cache_id
    } else {
        // Global admin: use params.cache_id if provided
        params
            .cache_id
            .as_ref()
            .map(|id| Uuid::parse_str(id))
            .transpose()
            .map_err(|e| ApiError::BadRequest(format!("invalid cache_id: {e}")))?
    };

    let limit = params.limit.unwrap_or(20).min(100);

    let jobs = state
        .metadata
        .get_recent_gc_jobs(target_cache_id, limit)
        .await?;

    let responses: Result<Vec<GcJobResponse>, ApiError> = jobs
        .into_iter()
        .map(|job| {
            let started_at_str = job
                .started_at
                .map(|t| {
                    t.format(&time::format_description::well_known::Rfc3339)
                        .map_err(|e| {
                            ApiError::Internal(format!("failed to format started_at: {e}"))
                        })
                })
                .transpose()?;

            let finished_at_str = job
                .finished_at
                .map(|t| {
                    t.format(&time::format_description::well_known::Rfc3339)
                        .map_err(|e| {
                            ApiError::Internal(format!("failed to format finished_at: {e}"))
                        })
                })
                .transpose()?;

            let stats: Option<GcStats> = job
                .stats_json
                .as_ref()
                .and_then(|s| serde_json::from_str(s).ok());

            Ok(GcJobResponse {
                job_id: job.gc_job_id.to_string(),
                cache_id: job.cache_id.map(|id| id.to_string()),
                job_type: job.job_type,
                state: job.state,
                started_at: started_at_str,
                finished_at: finished_at_str,
                stats,
            })
        })
        .collect();

    Ok(Json(responses?))
}

/// GET /v1/admin/gc/{job_id} - Get GC job status.
pub async fn get_gc_job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
    req: Request,
) -> ApiResult<Json<GcJobResponse>> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    let job_id = Uuid::parse_str(&job_id)
        .map_err(|e| ApiError::BadRequest(format!("invalid job ID: {e}")))?;

    let job = state
        .metadata
        .get_gc_job(job_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("GC job not found".to_string()))?;

    // Verify GC job ownership:
    // - Global admin (cache_id = None) can view any job
    // - Cache-scoped admin can only view jobs from their own cache
    if auth.token.cache_id.is_some() && job.cache_id != auth.token.cache_id {
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
        cache_id: job.cache_id.map(|id| id.to_string()),
        job_type: job.job_type,
        state: job.state,
        started_at: started_at_str,
        finished_at: finished_at_str,
        stats,
    }))
}

/// Maximum number of GC iterations is now configured via GcConfig.max_gc_iterations
/// (default: 100,000). This allows large deployments to adjust the limit as needed.
/// Extract hash from storage key, handling both chunk and manifest formats.
///
/// Chunk format: `domains/<uuid>/chunks/<ab>/<cd>/<hash>` (64 hex chars, no extension)
/// Manifest format: `domains/<uuid>/manifests/<ab>/<cd>/<hash>.json` (64 hex chars + .json extension)
///
/// Returns the 64-character hash if valid, None otherwise.
fn extract_hash_from_key(key: &str, expect_json_extension: bool) -> Option<&str> {
    let filename = key.rsplit('/').next()?;
    let hash = if expect_json_extension {
        filename.strip_suffix(".json")?
    } else {
        filename
    };
    if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(hash)
    } else {
        None
    }
}

/// Result of processing an orphaned chunk/manifest.
#[derive(Debug)]
enum OrphanProcessResult {
    /// Object has active references, keep it
    Kept,
    /// Object was deleted
    Deleted,
    /// Object was skipped (benign reason - within grace period, wrong cache, etc.)
    Skipped,
    /// Error occurred (critical)
    Error,
}

/// Process a single chunk for orphan cleanup.
///
/// Handles all logic for checking metadata, cache ownership, age verification,
/// and deletion. Returns OrphanProcessResult to indicate what happened.
#[allow(clippy::too_many_arguments)]
async fn process_chunk_orphan(
    key: String,
    metadata: &(dyn ChunkRepo + Send + Sync),
    upload_repo: &(dyn UploadRepo + Send + Sync),
    storage: &(dyn cellar_storage::ObjectStore + Send + Sync),
    config: &cellar_core::config::GcConfig,
    domain_id: Uuid,
    grace_period: time::Duration,
    grace_time: OffsetDateTime,
    job_id: Uuid,
) -> OrphanProcessResult {
    // Extract chunk hash from key
    let chunk_hash = match extract_hash_from_key(&key, false) {
        Some(hash) => hash,
        None => {
            tracing::debug!(
                job_id = %job_id,
                key = %key,
                "Invalid chunk key format (folder marker or non-standard object), skipping"
            );
            return OrphanProcessResult::Skipped;
        }
    };

    // Get full chunk metadata
    match metadata.get_chunk(domain_id, chunk_hash).await {
        Ok(Some(chunk)) => {
            // Chunk exists in metadata
            if chunk.refcount > 0 {
                // Active references exist, keep it
                tracing::trace!(
                    job_id = %job_id,
                    chunk_hash = %chunk_hash,
                    refcount = chunk.refcount,
                    "Chunk has active references, keeping"
                );
                return OrphanProcessResult::Kept;
            }

            // refcount = 0, check age against grace period using last access when available
            let last_accessed_at = chunk.last_accessed_at.unwrap_or(chunk.created_at);
            let age: time::Duration = OffsetDateTime::now_utc() - last_accessed_at;
            if age < grace_period {
                tracing::trace!(
                    job_id = %job_id,
                    chunk_hash = %chunk_hash,
                    last_accessed_at = %last_accessed_at,
                    age_secs = age.whole_seconds(),
                    "Chunk within grace period, keeping"
                );
                return OrphanProcessResult::Skipped;
            }

            // Old chunk with refcount=0 - apply protections before deletion

            // Protection 1: Check if chunk is in expected_chunks for active sessions
            match upload_repo
                .is_chunk_expected_in_active_sessions_since(chunk_hash, grace_time)
                .await
            {
                Ok(true) => {
                    tracing::trace!(
                        job_id = %job_id,
                        chunk_hash = %chunk_hash,
                        grace_time = %grace_time,
                        "Chunk is in expected_chunks for active session, keeping for safety"
                    );
                    return OrphanProcessResult::Skipped;
                }
                Ok(false) => {
                    // Safe to proceed with other checks
                }
                Err(e) => {
                    tracing::warn!(
                        job_id = %job_id,
                        chunk_hash = %chunk_hash,
                        error = %e,
                        "Failed to check expected_chunks for active sessions, skipping for safety"
                    );
                    return OrphanProcessResult::Error;
                }
            }

            // All protection checks passed - safe to proceed with cleanup
            if config.dry_run {
                // Dry-run: just log what would be deleted, don't mutate
                tracing::info!(
                    job_id = %job_id,
                    chunk_hash = %chunk_hash,
                    created_at = %chunk.created_at,
                    last_accessed_at = ?chunk.last_accessed_at,
                    refcount = chunk.refcount,
                    age_secs = age.whole_seconds(),
                    "DRY-RUN: Would delete stale chunk metadata (refcount=0)"
                );
                // Don't count as deleted in dry-run
                return OrphanProcessResult::Skipped;
            } else {
                // Actually delete metadata
                tracing::warn!(
                    job_id = %job_id,
                    chunk_hash = %chunk_hash,
                    created_at = %chunk.created_at,
                    last_accessed_at = ?chunk.last_accessed_at,
                    refcount = chunk.refcount,
                    age_secs = age.whole_seconds(),
                    "Chunk has refcount=0 after grace period, deleting stale metadata"
                );

                match metadata.delete_chunk(domain_id, chunk_hash).await {
                    Ok(()) => {
                        tracing::info!(
                            job_id = %job_id,
                            chunk_hash = %chunk_hash,
                            "Deleted stale chunk metadata (refcount=0)"
                        );
                        // Fall through to delete storage
                    }
                    Err(e) => {
                        tracing::error!(
                            job_id = %job_id,
                            chunk_hash = %chunk_hash,
                            error = %e,
                            "Failed to delete stale chunk metadata"
                        );
                        return OrphanProcessResult::Error;
                    }
                }
            }
        }
        Ok(None) => {
            // No metadata - potential orphan
        }
        Err(e) => {
            tracing::warn!(
                job_id = %job_id,
                chunk_hash = %chunk_hash,
                error = %e,
                "Failed to get chunk metadata, skipping"
            );
            return OrphanProcessResult::Error;
        }
    }

    // At this point, chunk has no metadata (true orphan candidate)

    // Get object age from storage to verify it's actually old
    let object_age = match storage.head(&key).await {
        Ok(meta) => {
            if let Some(last_modified) = meta.last_modified {
                OffsetDateTime::now_utc() - last_modified
            } else {
                // CONSERVATIVE: Use 7x grace period multiplier for missing timestamps
                // This ensures only truly old objects are deleted when we can't verify exact age
                // Example: 24h grace period → requires 168h (7 days) age for deletion
                tracing::warn!(
                    job_id = %job_id,
                    chunk_hash = %chunk_hash,
                    key = %key,
                    "Storage backend returned no last_modified timestamp, using conservative 7x grace period"
                );
                grace_period * 7
            }
        }
        Err(StorageError::NotFound(_)) => {
            // Object already deleted
            return OrphanProcessResult::Skipped;
        }
        Err(e) => {
            tracing::warn!(
                job_id = %job_id,
                chunk_hash = %chunk_hash,
                error = %e,
                "Failed to get chunk age from storage, skipping"
            );
            return OrphanProcessResult::Error;
        }
    };

    // Only check active sessions if object is recent (could be from active upload)
    if object_age < grace_period {
        match upload_repo.has_active_sessions_since(grace_time).await {
            Ok(true) => {
                tracing::trace!(
                    job_id = %job_id,
                    chunk_hash = %chunk_hash,
                    object_age_secs = object_age.whole_seconds(),
                    grace_time = %grace_time,
                    "Recent orphan with active uploads, skipping to avoid race"
                );
                return OrphanProcessResult::Skipped;
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    job_id = %job_id,
                    chunk_hash = %chunk_hash,
                    error = %e,
                    "Failed to check active sessions, skipping for safety"
                );
                return OrphanProcessResult::Error;
            }
        }
    }

    // Safe to delete - old orphan with no metadata
    if config.dry_run {
        tracing::info!(
            job_id = %job_id,
            chunk_hash = %chunk_hash,
            key = %key,
            object_age_secs = object_age.whole_seconds(),
            "DRY-RUN: Would delete orphaned chunk from storage"
        );
        return OrphanProcessResult::Skipped;
    }

    // Actually delete the orphaned chunk
    tracing::info!(
        job_id = %job_id,
        chunk_hash = %chunk_hash,
        key = %key,
        object_age_secs = object_age.whole_seconds(),
        "Deleting orphaned chunk from storage"
    );

    match storage.delete(&key).await {
        Ok(()) => {
            tracing::debug!(
                job_id = %job_id,
                chunk_hash = %chunk_hash,
                "Deleted orphaned chunk"
            );
            OrphanProcessResult::Deleted
        }
        Err(StorageError::NotFound(_)) => {
            // Already gone
            tracing::trace!(
                job_id = %job_id,
                chunk_hash = %chunk_hash,
                "Orphaned chunk already deleted"
            );
            OrphanProcessResult::Deleted
        }
        Err(e) => {
            tracing::warn!(
                job_id = %job_id,
                chunk_hash = %chunk_hash,
                error = %e,
                "Failed to delete orphaned chunk"
            );
            OrphanProcessResult::Error
        }
    }
}

/// Process a single manifest for orphan cleanup.
///
/// Similar to process_chunk_orphan but for manifests.
#[allow(clippy::too_many_arguments)]
async fn process_manifest_orphan(
    key: String,
    metadata: &(dyn ManifestRepo + Send + Sync),
    upload_repo: &(dyn UploadRepo + Send + Sync),
    storage: &(dyn cellar_storage::ObjectStore + Send + Sync),
    config: &cellar_core::config::GcConfig,
    domain_id: Uuid,
    grace_period: time::Duration,
    grace_time: OffsetDateTime,
    job_id: Uuid,
) -> OrphanProcessResult {
    // Extract manifest hash from key
    let manifest_hash = match extract_hash_from_key(&key, true) {
        Some(hash) => hash,
        None => {
            tracing::debug!(
                job_id = %job_id,
                key = %key,
                "Invalid manifest key format (folder marker or non-standard object), skipping"
            );
            return OrphanProcessResult::Skipped;
        }
    };

    // Get full manifest metadata
    match metadata.get_manifest(domain_id, manifest_hash).await {
        Ok(Some(_manifest)) => {
            // Manifest exists in metadata, keep it
            tracing::trace!(
                job_id = %job_id,
                manifest_hash = %manifest_hash,
                "Manifest has metadata, keeping"
            );
            return OrphanProcessResult::Kept;
        }
        Ok(None) => {
            // No metadata - potential orphan
        }
        Err(e) => {
            tracing::warn!(
                job_id = %job_id,
                manifest_hash = %manifest_hash,
                error = %e,
                "Failed to get manifest metadata, skipping"
            );
            return OrphanProcessResult::Error;
        }
    }

    // At this point, manifest has no metadata (true orphan candidate)

    // Get object age from storage to verify it's actually old
    let object_age = match storage.head(&key).await {
        Ok(meta) => {
            if let Some(last_modified) = meta.last_modified {
                OffsetDateTime::now_utc() - last_modified
            } else {
                // CONSERVATIVE: Use 5x grace period multiplier for missing timestamps
                // This ensures only truly old objects are deleted when we can't verify exact age
                // Example: 24h grace period → requires 120h (5 days) age for deletion
                tracing::warn!(
                    job_id = %job_id,
                    manifest_hash = %manifest_hash,
                    key = %key,
                    "Storage backend returned no last_modified timestamp, using conservative 5x grace period"
                );
                grace_period * 5
            }
        }
        Err(StorageError::NotFound(_)) => {
            // Object already deleted
            return OrphanProcessResult::Skipped;
        }
        Err(e) => {
            tracing::warn!(
                job_id = %job_id,
                manifest_hash = %manifest_hash,
                error = %e,
                "Failed to get manifest age from storage, skipping"
            );
            return OrphanProcessResult::Error;
        }
    };

    // Only check active sessions if object is recent (could be from active upload)
    if object_age < grace_period {
        match upload_repo.has_active_sessions_since(grace_time).await {
            Ok(true) => {
                tracing::trace!(
                    job_id = %job_id,
                    manifest_hash = %manifest_hash,
                    object_age_secs = object_age.whole_seconds(),
                    grace_time = %grace_time,
                    "Recent orphan with active uploads, skipping to avoid race"
                );
                return OrphanProcessResult::Skipped;
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    job_id = %job_id,
                    manifest_hash = %manifest_hash,
                    error = %e,
                    "Failed to check active sessions, skipping for safety"
                );
                return OrphanProcessResult::Error;
            }
        }
    }

    // Safe to delete - old orphan with no metadata
    if config.dry_run {
        tracing::info!(
            job_id = %job_id,
            manifest_hash = %manifest_hash,
            key = %key,
            object_age_secs = object_age.whole_seconds(),
            "DRY-RUN: Would delete orphaned manifest from storage"
        );
        return OrphanProcessResult::Skipped;
    }

    // Actually delete the orphaned manifest
    tracing::info!(
        job_id = %job_id,
        manifest_hash = %manifest_hash,
        key = %key,
        object_age_secs = object_age.whole_seconds(),
        "Deleting orphaned manifest from storage"
    );

    match storage.delete(&key).await {
        Ok(()) => {
            tracing::debug!(
                job_id = %job_id,
                manifest_hash = %manifest_hash,
                "Deleted orphaned manifest"
            );
            OrphanProcessResult::Deleted
        }
        Err(StorageError::NotFound(_)) => {
            // Already gone
            tracing::trace!(
                job_id = %job_id,
                manifest_hash = %manifest_hash,
                "Orphaned manifest already deleted"
            );
            OrphanProcessResult::Deleted
        }
        Err(e) => {
            tracing::warn!(
                job_id = %job_id,
                manifest_hash = %manifest_hash,
                error = %e,
                "Failed to delete orphaned manifest"
            );
            OrphanProcessResult::Error
        }
    }
}

/// Execute a batch operation without tokio timeout wrapper.
///
/// IMPORTANT: tokio::time::timeout does NOT cancel database queries!
/// When timeout occurs, the Rust future is dropped but the database query
/// continues running, potentially holding locks indefinitely.
///
/// Proper timeout handling should be done at database level:
/// - PostgreSQL: Configure statement_timeout in postgresql.conf or connection string
/// - SQLite: Limited timeout support, use connection pool settings
///
/// For now, we rely on:
/// - Database connection timeouts (configured by DBA)
/// - Natural completion of queries (with reasonable batch sizes)
/// - The GC watchdog to detect panics/hangs and mark jobs as failed
///
/// Future improvement: Set statement_timeout per-transaction in metadata layer.
async fn with_batch_timeout<F, T>(
    future: F,
    _timeout: std::time::Duration, // Unused, kept for API compatibility
    _job_id: Uuid,
    _batch_type: &str,
) -> Result<T, ApiError>
where
    F: std::future::Future<Output = Result<T, ApiError>>,
{
    // No tokio timeout - just execute the operation
    // Database-level timeouts (if configured) will prevent hung queries
    future.await
}

async fn resolve_domain_id_for_cache(
    metadata: &dyn cellar_metadata::MetadataStore,
    cache_id: Option<Uuid>,
) -> ApiResult<Uuid> {
    if let Some(cache_id) = cache_id {
        let cache = metadata
            .get_cache(cache_id)
            .await?
            .ok_or_else(|| ApiError::NotFound("cache not found".to_string()))?;
        Ok(cache.domain_id)
    } else {
        Ok(default_storage_domain_id())
    }
}

async fn resolve_domain_from_request(
    metadata: &dyn cellar_metadata::MetadataStore,
    domain_id: Option<&str>,
    domain_name: Option<&str>,
) -> ApiResult<(Uuid, Option<String>)> {
    if let Some(name) = domain_name {
        validate_domain_name(name)?;
        let domain = metadata
            .get_domain_by_name(name)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("domain '{}' not found", name)))?;
        if let Some(id_str) = domain_id {
            let parsed = Uuid::parse_str(id_str)
                .map_err(|e| ApiError::BadRequest(format!("invalid domain_id: {e}")))?;
            if parsed != domain.domain_id {
                return Err(ApiError::BadRequest(
                    "domain_id does not match domain_name".to_string(),
                ));
            }
        }
        return Ok((domain.domain_id, Some(domain.domain_name)));
    }

    if let Some(id_str) = domain_id {
        let parsed = Uuid::parse_str(id_str)
            .map_err(|e| ApiError::BadRequest(format!("invalid domain_id: {e}")))?;
        let domain = metadata
            .get_domain(parsed)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("domain '{}' not found", parsed)))?;
        return Ok((domain.domain_id, Some(domain.domain_name)));
    }

    let default_id = default_storage_domain_id();
    let domain = metadata
        .get_domain(default_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("default domain not found".to_string()))?;
    Ok((domain.domain_id, Some(domain.domain_name)))
}

/// Run a GC job.
pub async fn run_gc_job(
    job_id: Uuid,
    job_type: GcJobType,
    cache_id: Option<Uuid>,
    metadata: std::sync::Arc<dyn cellar_metadata::MetadataStore>,
    storage: std::sync::Arc<dyn cellar_storage::ObjectStore>,
    config: cellar_core::config::GcConfig,
) -> Result<GcStats, ApiError> {
    let mut stats = GcStats::default();

    // Update job state to running
    metadata
        .update_gc_job_state(job_id, GcJobState::Running.as_str(), None, None, None)
        .await?;

    match job_type {
        GcJobType::UploadGc => {
            // Clean up expired upload sessions
            let mut iterations = 0;
            loop {
                iterations += 1;
                if iterations > config.max_gc_iterations as u32 {
                    tracing::warn!(
                        job_id = %job_id,
                        iterations = iterations,
                        max_iterations = config.max_gc_iterations,
                        "GC upload job exceeded max iterations, stopping to prevent infinite loop"
                    );
                    break;
                }

                let expired = with_batch_timeout(
                    async {
                        metadata
                            .get_expired_sessions(cache_id, config.batch_size)
                            .await
                            .map_err(ApiError::from)
                    },
                    config.batch_timeout(),
                    job_id,
                    "get_expired_sessions",
                )
                .await?;
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
            let grace_period = config.grace_period();
            let grace_time = OffsetDateTime::now_utc() - grace_period;
            let domain_id = resolve_domain_id_for_cache(&*metadata, cache_id).await?;
            let mut iterations = 0;

            loop {
                iterations += 1;
                if iterations > config.max_gc_iterations as u32 {
                    tracing::warn!(
                        job_id = %job_id,
                        iterations = iterations,
                        max_iterations = config.max_gc_iterations,
                        "GC chunk job exceeded max iterations, stopping to prevent infinite loop"
                    );
                    break;
                }

                // Atomically delete unreferenced chunks from metadata.
                // This prevents TOCTOU race: locks are held from SELECT through DELETE,
                // so concurrent refcount increments block until deletion completes.
                // Returns chunks that were successfully deleted from metadata.
                // CRITICAL FIX: Now correlates chunks to sessions by creation time,
                // protecting chunks from ANY active upload regardless of session age.
                let deleted_chunks = with_batch_timeout(
                    async {
                        metadata
                            .delete_unreferenced_chunks_atomic(
                                domain_id,
                                grace_time,
                                config.batch_size,
                            )
                            .await
                            .map_err(ApiError::from)
                    },
                    config.batch_timeout(),
                    job_id,
                    "delete_unreferenced_chunks",
                )
                .await?;

                if deleted_chunks.is_empty() {
                    break;
                }

                // Storage cleanup happens AFTER metadata deletion.
                // This ordering allows orphaned storage objects (from failed deletions)
                // to be cleaned up on the next GC run.
                for chunk in deleted_chunks {
                    stats.items_processed += 1;

                    let key = format!(
                        "domains/{}/chunks/{}/{}/{}",
                        chunk.domain_id,
                        &chunk.chunk_hash[..2],
                        &chunk.chunk_hash[2..4],
                        chunk.chunk_hash
                    );

                    // Delete from storage. NotFound is OK (already deleted or never existed).
                    // Failed deletions create orphaned storage objects, but metadata is already gone,
                    // so they'll be invisible to users and can be cleaned up manually if needed.
                    match storage.delete(&key).await {
                        Ok(()) => {
                            stats.items_deleted += 1;
                            stats.bytes_reclaimed += chunk.size_bytes as u64;
                            tracing::debug!(
                                job_id = %job_id,
                                chunk_hash = %chunk.chunk_hash,
                                size_bytes = chunk.size_bytes,
                                "Deleted chunk from storage and metadata"
                            );
                        }
                        Err(StorageError::NotFound(_)) => {
                            // Already gone from storage, but metadata was cleaned up
                            stats.items_deleted += 1;
                            tracing::debug!(
                                job_id = %job_id,
                                chunk_hash = %chunk.chunk_hash,
                                "Chunk already missing from storage, metadata cleaned up"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                job_id = %job_id,
                                chunk_hash = %chunk.chunk_hash,
                                error = %e,
                                "Failed to delete chunk from storage (orphaned object created, metadata already deleted)"
                            );
                            stats.errors += 1;
                        }
                    }
                }
            }
        }
        GcJobType::ManifestGc => {
            // Clean up orphaned manifests (domain-scoped for tenant isolation)
            let grace_time = OffsetDateTime::now_utc() - config.grace_period();
            let domain_id = resolve_domain_id_for_cache(&*metadata, cache_id).await?;
            let mut iterations = 0;
            loop {
                iterations += 1;
                if iterations > config.max_gc_iterations as u32 {
                    tracing::warn!(
                        job_id = %job_id,
                        iterations = iterations,
                        max_iterations = config.max_gc_iterations,
                        "GC manifest job exceeded max iterations, stopping to prevent infinite loop"
                    );
                    break;
                }

                // Use domain-scoped query for tenant isolation.
                // Only GC manifests within this storage domain (prevents cross-domain deletion).
                let orphaned = with_batch_timeout(
                    async {
                        metadata
                            .get_orphaned_manifests(domain_id, grace_time, config.batch_size)
                            .await
                            .map_err(ApiError::from)
                    },
                    config.batch_timeout(),
                    job_id,
                    "get_orphaned_manifests",
                )
                .await?;
                if orphaned.is_empty() {
                    break;
                }

                for manifest in orphaned {
                    stats.items_processed += 1;

                    // Delete from metadata FIRST with atomic refcount decrement.
                    // This ensures metadata always reflects storage state - if we crash,
                    // storage objects may be orphaned (can be cleaned up later) but
                    // metadata will never point to missing storage objects (which causes 404s).
                    // Uses domain_id from job (domain-scoped refcount decrement)
                    // Transaction ensures: get chunks -> decrement all refcounts -> delete manifest
                    // If any step fails, entire transaction rolls back
                    match metadata
                        .delete_manifest_with_refcount_decrement(domain_id, &manifest.manifest_hash)
                        .await
                    {
                        Ok(chunk_count) => {
                            // CRITICAL FIX: Only proceed if metadata was actually deleted (chunk_count > 0).
                            // If chunk_count == 0, the metadata deletion was skipped due to concurrent
                            // refcount changes, so we MUST NOT delete from storage (would cause 404s).
                            if chunk_count > 0 {
                                stats.items_deleted += 1;
                                tracing::debug!(
                                    job_id = %job_id,
                                    manifest_hash = %manifest.manifest_hash,
                                    chunks = chunk_count,
                                    "Deleted manifest from metadata and decremented chunk refcounts atomically"
                                );

                                // Now delete from storage. Failures create orphaned storage objects
                                // (harmless - invisible to users, can be cleaned up with storage sweep).
                                // This ordering prevents orphaned metadata (which causes user-visible errors).
                                if let Some(key) = &manifest.object_key
                                    && let Err(e) = storage.delete(key).await
                                {
                                    tracing::warn!(
                                        job_id = %job_id,
                                        manifest_hash = %manifest.manifest_hash,
                                        object_key = %key,
                                        error = %e,
                                        "Failed to delete manifest from storage (orphaned storage object created, metadata already cleaned up)"
                                    );
                                    stats.errors += 1;
                                }
                            } else {
                                // Metadata deletion was skipped due to concurrent refcount change.
                                // Manifest is now referenced by another cache/commit, DO NOT delete from storage.
                                tracing::debug!(
                                    job_id = %job_id,
                                    manifest_hash = %manifest.manifest_hash,
                                    "Manifest metadata deletion skipped (concurrent refcount change), storage preserved"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                job_id = %job_id,
                                manifest_hash = %manifest.manifest_hash,
                                error = %e,
                                "Failed to delete manifest from metadata, skipping storage deletion"
                            );
                            stats.errors += 1;
                        }
                    }
                }
            }

            // Also clean up failed store_paths that are older than the grace period.
            // Failed uploads should be retained for a grace period to allow debugging,
            // but cleaned up eventually to prevent accumulation.
            let grace_period_seconds = config.grace_period().whole_seconds();
            match metadata
                .delete_failed_store_paths_older_than(cache_id, grace_period_seconds)
                .await
            {
                Ok(deleted_count) => {
                    stats.items_processed += deleted_count;
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
        GcJobType::CommittingGc => {
            // Recover sessions stuck in 'committing' state
            let grace_time = OffsetDateTime::now_utc() - config.grace_period();
            let mut iterations = 0;

            loop {
                iterations += 1;
                if iterations > config.max_gc_iterations as u32 {
                    tracing::warn!(
                        job_id = %job_id,
                        iterations = iterations,
                        max_iterations = config.max_gc_iterations,
                        "GC committing job exceeded max iterations, stopping to prevent infinite loop"
                    );
                    break;
                }

                let stuck = with_batch_timeout(
                    async {
                        metadata
                            .get_stuck_committing_sessions(cache_id, grace_time, config.batch_size)
                            .await
                            .map_err(ApiError::from)
                    },
                    config.batch_timeout(),
                    job_id,
                    "get_stuck_committing_sessions",
                )
                .await?;
                if stuck.is_empty() {
                    break;
                }

                for session in stuck {
                    stats.items_processed += 1;

                    // P2 Issue #8: Rollback partial refcount increments before marking as failed.
                    // Parse commit_progress to find which refcounts were incremented.
                    let mut rollback_count = 0;

                    if let Some(progress_json) = &session.commit_progress {
                        // CRITICAL FIX: Validate JSON structure before parsing to detect corruption early
                        if progress_json.trim().is_empty()
                            || !progress_json.trim_start().starts_with('{')
                        {
                            tracing::error!(
                                job_id = %job_id,
                                upload_id = %session.upload_id,
                                progress_json = %progress_json,
                                "CRITICAL: commit_progress is not valid JSON (empty or not an object). Session will be marked failed but refcounts NOT rolled back."
                            );
                            stats.errors += 1;
                            crate::metrics::GC_ROLLBACK_PARSE_FAILURES.inc();
                        } else {
                            match serde_json::from_str::<serde_json::Value>(progress_json) {
                                Ok(progress) => {
                                    // Validate schema - check if required fields exist
                                    let has_refcounts =
                                        progress.get("incremented_refcounts").is_some();
                                    let has_manifest = progress.get("manifest_hash").is_some();

                                    if !has_refcounts && !has_manifest {
                                        tracing::warn!(
                                            job_id = %job_id,
                                            upload_id = %session.upload_id,
                                            "commit_progress exists but contains no refcount data (early commit phase), no rollback needed"
                                        );
                                    }

                                    // CRITICAL: Detect JSON corruption via refcount_count mismatch
                                    if let (Some(refcounts_array), Some(refcount_count)) = (
                                        progress["incremented_refcounts"].as_array(),
                                        progress["refcount_count"].as_u64(),
                                    ) && refcounts_array.len() != refcount_count as usize
                                    {
                                        tracing::error!(
                                            job_id = %job_id,
                                            upload_id = %session.upload_id,
                                            array_len = refcounts_array.len(),
                                            expected_count = refcount_count,
                                            "CRITICAL: JSON corruption detected - refcount_count mismatch! \
                                             Possible truncation or data loss."
                                        );
                                        stats.errors += 1;
                                        crate::metrics::GC_ROLLBACK_PARSE_FAILURES.inc();
                                    }

                                    // Rollback chunk refcounts
                                    if let Some(incremented) =
                                        progress["incremented_refcounts"].as_array()
                                    {
                                        tracing::info!(
                                            job_id = %job_id,
                                            upload_id = %session.upload_id,
                                            refcounts_to_rollback = incremented.len(),
                                            "Rolling back partial refcount increments"
                                        );

                                        for chunk_hash_value in incremented {
                                            if let Some(chunk_hash) = chunk_hash_value.as_str() {
                                                // Validate chunk hash format before attempting rollback
                                                if chunk_hash.len() != 64
                                                    || !chunk_hash
                                                        .chars()
                                                        .all(|c| c.is_ascii_hexdigit())
                                                {
                                                    tracing::error!(
                                                        job_id = %job_id,
                                                        upload_id = %session.upload_id,
                                                        chunk_hash = %chunk_hash,
                                                        "CRITICAL: Invalid chunk hash format in commit_progress, cannot rollback"
                                                    );
                                                    stats.errors += 1;
                                                    continue;
                                                }

                                                if let Err(e) = metadata
                                                    .decrement_refcount(
                                                        session.domain_id,
                                                        chunk_hash,
                                                    )
                                                    .await
                                                {
                                                    tracing::warn!(
                                                        job_id = %job_id,
                                                        upload_id = %session.upload_id,
                                                        chunk_hash = %chunk_hash,
                                                        domain_id = %session.domain_id,
                                                        error = %e,
                                                        "Failed to rollback chunk refcount"
                                                    );
                                                    stats.errors += 1;
                                                } else {
                                                    rollback_count += 1;
                                                    crate::metrics::GC_REFCOUNT_ROLLBACKS.inc();
                                                }
                                            } else {
                                                tracing::error!(
                                                    job_id = %job_id,
                                                    upload_id = %session.upload_id,
                                                    "CRITICAL: incremented_refcounts array contains non-string value"
                                                );
                                                stats.errors += 1;
                                            }
                                        }
                                    }

                                    // Rollback manifest refcount if it was incremented
                                    if let Some(manifest_hash) = progress["manifest_hash"].as_str()
                                    {
                                        // Validate manifest hash format
                                        if manifest_hash.len() != 64
                                            || !manifest_hash.chars().all(|c| c.is_ascii_hexdigit())
                                        {
                                            tracing::error!(
                                                job_id = %job_id,
                                                upload_id = %session.upload_id,
                                                manifest_hash = %manifest_hash,
                                                "CRITICAL: Invalid manifest hash format in commit_progress, cannot rollback"
                                            );
                                            stats.errors += 1;
                                        } else if let Err(e) = metadata
                                            .decrement_manifest_refcount(
                                                session.domain_id,
                                                manifest_hash,
                                            )
                                            .await
                                        {
                                            tracing::warn!(
                                                job_id = %job_id,
                                                upload_id = %session.upload_id,
                                                manifest_hash = %manifest_hash,
                                                domain_id = %session.domain_id,
                                                error = %e,
                                                "Failed to rollback manifest refcount"
                                            );
                                            stats.errors += 1;
                                        } else {
                                            rollback_count += 1;
                                            crate::metrics::GC_REFCOUNT_ROLLBACKS.inc();
                                        }
                                    }
                                }
                                Err(e) => {
                                    // CRITICAL ERROR: Cannot parse progress JSON, unable to rollback refcounts!
                                    // This will leave "zombie" refcounts that prevent chunks from being GC'd.
                                    // This is a data integrity issue that requires manual intervention.
                                    tracing::error!(
                                        job_id = %job_id,
                                        upload_id = %session.upload_id,
                                        error = %e,
                                        progress_json = %progress_json,
                                        "CRITICAL: Failed to parse commit_progress JSON, cannot rollback refcounts! \
                                         This creates refcount leaks. Manual intervention required: \
                                         1. Query upload_sessions for upload_id={} to get store_path and manifest info. \
                                         2. Identify affected chunks from session metadata. \
                                         3. Manually decrement chunk refcounts for domain_id={}. \
                                         4. Check GC_ROLLBACK_PARSE_FAILURES metric for frequency.",
                                        session.upload_id, session.domain_id
                                    );
                                    stats.errors += 1;
                                    crate::metrics::GC_ROLLBACK_PARSE_FAILURES.inc();
                                }
                            }
                        }
                    }

                    // Mark session as failed to unblock and allow cleanup
                    if let Err(e) = metadata.fail_session(
                        session.upload_id,
                        "commit_timeout",
                        Some("Session stuck in committing state, recovered by GC with refcount rollback"),
                        OffsetDateTime::now_utc(),
                    ).await {
                        tracing::warn!(
                            job_id = %job_id,
                            upload_id = %session.upload_id,
                            error = %e,
                            "Failed to mark stuck session as failed"
                        );
                        stats.errors += 1;
                    } else {
                        tracing::info!(
                            job_id = %job_id,
                            upload_id = %session.upload_id,
                            store_path = %session.store_path,
                            stuck_since = ?session.commit_started_at,
                            refcounts_rolled_back = rollback_count,
                            "Recovered stuck committing session with refcount rollback"
                        );
                    }
                }
            }
        }
        GcJobType::StorageSweep => {
            // Clean up orphaned storage objects (chunks/manifests with no metadata)
            // This is a best-effort sweep that lists storage and checks for metadata existence.
            // Uses grace period and active session checks to avoid race conditions with in-flight uploads.
            // Note: This is an expensive operation and should be run infrequently.
            tracing::info!(
                job_id = %job_id,
                cache_id = ?cache_id,
                dry_run = config.dry_run,
                batch_delay_ms = ?config.batch_delay_ms,
                max_error_rate = ?config.max_error_rate,
                "Starting storage sweep for orphaned objects"
            );

            if let Some(id) = cache_id {
                tracing::info!(
                    job_id = %job_id,
                    cache_id = %id,
                    "DOMAIN-SCOPED SWEEP: Only deleting objects within this storage domain"
                );
            }

            if config.dry_run {
                tracing::warn!(
                    job_id = %job_id,
                    "DRY-RUN MODE: Will report what would be deleted without actually deleting"
                );
            }

            let grace_period = config.grace_period();
            let grace_time = OffsetDateTime::now_utc() - grace_period;
            let domain_id = resolve_domain_id_for_cache(&*metadata, cache_id).await?;
            let chunk_prefix = format!("domains/{}/chunks/", domain_id);
            let manifest_prefix = format!("domains/{}/manifests/", domain_id);

            let mut critical_errors = 0u64;
            let mut chunks_completed = true;
            let mut manifests_completed = true;

            // Load checkpoint if it exists (for streaming mode)
            let mut checkpoint = if config.use_streaming_listing {
                // Try to load existing checkpoint
                match metadata.get_gc_job(job_id).await {
                    Ok(Some(job_row)) => {
                        if let Some(ref checkpoint_json) = job_row.sweep_checkpoint_json {
                            match SweepCheckpoint::from_json(checkpoint_json) {
                                Ok(cp) => {
                                    tracing::info!(
                                        job_id = %job_id,
                                        chunks_processed = cp.chunks_processed,
                                        manifests_processed = cp.manifests_processed,
                                        has_chunks_token = cp.chunks_token.is_some(),
                                        has_manifests_token = cp.manifests_token.is_some(),
                                        "Resuming storage sweep from checkpoint"
                                    );
                                    crate::metrics::GC_CHECKPOINT_RESUMED.inc();
                                    cp
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        job_id = %job_id,
                                        error = %e,
                                        "Failed to parse checkpoint, starting fresh"
                                    );
                                    crate::metrics::GC_CHECKPOINT_INVALIDATED.inc();
                                    SweepCheckpoint::new()
                                }
                            }
                        } else {
                            SweepCheckpoint::new()
                        }
                    }
                    Ok(None) => {
                        tracing::debug!(
                            job_id = %job_id,
                            "No existing job found, starting fresh"
                        );
                        SweepCheckpoint::new()
                    }
                    Err(e) => {
                        tracing::warn!(
                            job_id = %job_id,
                            error = %e,
                            "Failed to load checkpoint, starting fresh"
                        );
                        SweepCheckpoint::new()
                    }
                }
            } else {
                SweepCheckpoint::new()
            };

            // Sweep chunks
            tracing::debug!(
                job_id = %job_id,
                use_streaming = config.use_streaming_listing,
                "Sweeping orphaned chunks from storage"
            );

            if config.use_streaming_listing {
                // NEW STREAMING PATH: Use page-based streaming with checkpoints
                use cellar_storage::traits::{ContinuationToken, ListingOptions, ListingResume};

                let caps = storage.listing_capabilities();
                let listing_opts = ListingOptions::new(1000); // 1000 keys per page

                // Prepare resume token if available
                let resume = if let Some(token_b64) = checkpoint.chunks_token.as_ref() {
                    if caps.resumable {
                        match ContinuationToken::from_base64(token_b64) {
                            Ok(token) => Some(ListingResume::new(token)),
                            Err(e) => {
                                tracing::warn!(
                                    job_id = %job_id,
                                    error = %e,
                                    "Invalid chunks continuation token, starting from beginning"
                                );
                                crate::metrics::STORAGE_LISTING_TOKENS_INVALID
                                    .with_label_values(&[storage.backend_name()])
                                    .inc();
                                crate::metrics::GC_CHECKPOINT_INVALIDATED.inc();
                                // Clear the invalid token
                                checkpoint.chunks_token = None;
                                checkpoint.chunks_processed = 0;
                                None
                            }
                        }
                    } else {
                        tracing::debug!(
                            job_id = %job_id,
                            "Backend doesn't support resumable listings, ignoring checkpoint token"
                        );
                        None
                    }
                } else {
                    None
                };

                let mut page_stream = storage.list_pages(&chunk_prefix, listing_opts, resume);

                tracing::info!(
                    job_id = %job_id,
                    resumable = caps.resumable,
                    "Streaming chunks from storage (page-based), checking for orphans"
                );

                let mut page_count = 0u64;
                let mut stop_chunks = false;
                while let Some(page_result) = page_stream.next().await {
                    let page = match page_result {
                        Ok(p) => p,
                        Err(e) => {
                            // Check if it's an invalid token error - trigger recovery
                            if matches!(
                                e,
                                StorageError::ListingNotResumable
                                    | StorageError::InvalidContinuationToken(_)
                            ) {
                                tracing::warn!(
                                    job_id = %job_id,
                                    error = %e,
                                    "Continuation token invalid or listing not resumable, clearing checkpoint and restarting"
                                );
                                crate::metrics::STORAGE_LISTING_TOKENS_INVALID
                                    .with_label_values(&[storage.backend_name()])
                                    .inc();
                                crate::metrics::GC_CHECKPOINT_INVALIDATED.inc();
                                checkpoint.chunks_token = None;
                                checkpoint.chunks_processed = 0;
                                checkpoint.touch();
                                // Save cleared checkpoint
                                if let Ok(checkpoint_json) = checkpoint.to_json() {
                                    let _ = metadata
                                        .update_gc_job_state(
                                            job_id,
                                            GcJobState::Running.as_str(),
                                            None,
                                            None,
                                            Some(&checkpoint_json),
                                        )
                                        .await;
                                }
                                stop_chunks = true;
                                break; // Will need to restart manually
                            }

                            tracing::warn!(
                                job_id = %job_id,
                                error = %e,
                                "Failed to fetch listing page, stopping sweep"
                            );
                            critical_errors += 1;
                            stats.errors += 1;
                            stop_chunks = true;
                            break;
                        }
                    };

                    page_count += 1;
                    crate::metrics::STORAGE_LISTING_PAGES_FETCHED
                        .with_label_values(&[storage.backend_name()])
                        .inc();

                    // Process all keys in this page
                    let mut page_complete = true;
                    for key in page.keys {
                        // Check if we've hit max iterations limit
                        if stats.items_processed >= config.max_gc_iterations {
                            tracing::warn!(
                                job_id = %job_id,
                                items_processed = stats.items_processed,
                                max_iterations = config.max_gc_iterations,
                                "Storage sweep reached max iterations, stopping (re-run to continue)"
                            );
                            stop_chunks = true;
                            page_complete = false;
                            break;
                        }

                        // Check error rate threshold
                        if let Some(max_error_rate) = config.max_error_rate
                            && stats.items_processed >= 100
                        {
                            let error_rate = critical_errors as f64 / stats.items_processed as f64;
                            if error_rate > max_error_rate {
                                tracing::error!(
                                    job_id = %job_id,
                                    error_rate = format!("{:.2}%", error_rate * 100.0),
                                    max_error_rate = format!("{:.2}%", max_error_rate * 100.0),
                                    items_processed = stats.items_processed,
                                    critical_errors = critical_errors,
                                    "Critical error rate exceeds threshold, aborting storage sweep"
                                );
                                stop_chunks = true;
                                page_complete = false;
                                break;
                            }
                        }

                        stats.items_processed += 1;
                        checkpoint.chunks_processed += 1;

                        // Process this chunk using the helper function
                        let result = process_chunk_orphan(
                            key,
                            metadata.as_ref(),
                            metadata.as_ref(), // Also implements UploadRepo
                            storage.as_ref(),
                            &config,
                            domain_id,
                            grace_period,
                            grace_time,
                            job_id,
                        )
                        .await;

                        match result {
                            OrphanProcessResult::Deleted => {
                                stats.items_deleted += 1;
                            }
                            OrphanProcessResult::Error => {
                                critical_errors += 1;
                                stats.errors += 1;
                            }
                            OrphanProcessResult::Kept | OrphanProcessResult::Skipped => {
                                // No action needed
                            }
                        }

                        // Rate limiting: delay after processing each item if configured
                        if let Some(delay_ms) = config.batch_delay_ms
                            && stats.items_processed % 100 == 0
                        {
                            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                        }
                    }

                    // Save checkpoint after each page (if backend supports resumption)
                    if page_complete
                        && caps.resumable
                        && let Some(next_token) = page.next_token
                    {
                        checkpoint.chunks_token = Some(next_token.to_base64());
                        checkpoint.touch();

                        // Persist checkpoint to database
                        if let Ok(checkpoint_json) = checkpoint.to_json() {
                            if let Err(e) = metadata
                                .update_gc_job_state(
                                    job_id,
                                    GcJobState::Running.as_str(),
                                    None,
                                    None,
                                    Some(&checkpoint_json),
                                )
                                .await
                            {
                                tracing::warn!(
                                    job_id = %job_id,
                                    error = %e,
                                    "Failed to save checkpoint (non-fatal)"
                                );
                            } else {
                                crate::metrics::GC_CHECKPOINT_SAVED.inc();
                                tracing::debug!(
                                    job_id = %job_id,
                                    page = page_count,
                                    chunks_processed = checkpoint.chunks_processed,
                                    "Saved chunks sweep checkpoint"
                                );
                            }
                        }
                    }

                    if stop_chunks {
                        break;
                    }

                    // Check if we should stop
                    if stats.items_processed >= config.max_gc_iterations {
                        stop_chunks = true;
                        break;
                    }
                }

                if stop_chunks {
                    chunks_completed = false;
                }

                tracing::info!(
                    job_id = %job_id,
                    pages_fetched = page_count,
                    processed = stats.items_processed,
                    deleted = stats.items_deleted,
                    "Completed chunk sweep (streaming mode)"
                );
            } else {
                // LEGACY PATH: Use old Vec-based materialization
                match storage.list_stream(&chunk_prefix).await {
                    Ok(mut chunk_stream) => {
                        tracing::info!(
                            job_id = %job_id,
                            "Streaming chunks from storage (legacy mode), checking for orphans"
                        );

                        while let Some(key_result) = chunk_stream.next().await {
                            // Check if we've hit max iterations limit
                            if stats.items_processed >= config.max_gc_iterations {
                                tracing::warn!(
                                    job_id = %job_id,
                                    items_processed = stats.items_processed,
                                    max_iterations = config.max_gc_iterations,
                                    "Storage sweep reached max iterations, stopping (re-run to continue)"
                                );
                                break;
                            }

                            // Check error rate threshold
                            if let Some(max_error_rate) = config.max_error_rate
                                && stats.items_processed >= 100
                            {
                                let error_rate =
                                    critical_errors as f64 / stats.items_processed as f64;
                                if error_rate > max_error_rate {
                                    tracing::error!(
                                        job_id = %job_id,
                                        error_rate = format!("{:.2}%", error_rate * 100.0),
                                        max_error_rate = format!("{:.2}%", max_error_rate * 100.0),
                                        items_processed = stats.items_processed,
                                        critical_errors = critical_errors,
                                        "Critical error rate exceeds threshold, aborting storage sweep"
                                    );
                                    break;
                                }
                            }

                            let key = match key_result {
                                Ok(k) => k,
                                Err(e) => {
                                    tracing::warn!(
                                        job_id = %job_id,
                                        error = %e,
                                        "Failed to list chunk key, skipping"
                                    );
                                    critical_errors += 1;
                                    stats.errors += 1;
                                    continue;
                                }
                            };

                            stats.items_processed += 1;

                            // Process this chunk using the helper function
                            let result = process_chunk_orphan(
                                key,
                                metadata.as_ref(),
                                metadata.as_ref(), // Also implements UploadRepo
                                storage.as_ref(),
                                &config,
                                domain_id,
                                grace_period,
                                grace_time,
                                job_id,
                            )
                            .await;

                            match result {
                                OrphanProcessResult::Deleted => {
                                    stats.items_deleted += 1;
                                }
                                OrphanProcessResult::Error => {
                                    critical_errors += 1;
                                    stats.errors += 1;
                                }
                                OrphanProcessResult::Kept | OrphanProcessResult::Skipped => {
                                    // No action needed
                                }
                            }

                            // Rate limiting: delay after processing each item if configured
                            if let Some(delay_ms) = config.batch_delay_ms
                                && stats.items_processed % 100 == 0
                            {
                                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms))
                                    .await;
                            }
                        }

                        tracing::info!(
                            job_id = %job_id,
                            processed = stats.items_processed,
                            deleted = stats.items_deleted,
                            "Completed chunk sweep (legacy mode)"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            job_id = %job_id,
                            error = %e,
                            "Failed to list chunks from storage"
                        );
                        critical_errors += 1;
                        stats.errors += 1;
                    }
                }
            }

            // Sweep manifests
            tracing::debug!(
                job_id = %job_id,
                use_streaming = config.use_streaming_listing,
                "Sweeping orphaned manifests from storage"
            );

            if config.use_streaming_listing {
                // NEW STREAMING PATH: Use page-based streaming with checkpoints
                use cellar_storage::traits::{ContinuationToken, ListingOptions, ListingResume};

                let caps = storage.listing_capabilities();
                let listing_opts = ListingOptions::new(1000); // 1000 keys per page

                // Prepare resume token if available
                let resume = if let Some(token_b64) = checkpoint.manifests_token.as_ref() {
                    if caps.resumable {
                        match ContinuationToken::from_base64(token_b64) {
                            Ok(token) => Some(ListingResume::new(token)),
                            Err(e) => {
                                tracing::warn!(
                                    job_id = %job_id,
                                    error = %e,
                                    "Invalid manifests continuation token, starting from beginning"
                                );
                                crate::metrics::STORAGE_LISTING_TOKENS_INVALID
                                    .with_label_values(&[storage.backend_name()])
                                    .inc();
                                crate::metrics::GC_CHECKPOINT_INVALIDATED.inc();
                                // Clear the invalid token
                                checkpoint.manifests_token = None;
                                checkpoint.manifests_processed = 0;
                                None
                            }
                        }
                    } else {
                        tracing::debug!(
                            job_id = %job_id,
                            "Backend doesn't support resumable listings, ignoring checkpoint token"
                        );
                        None
                    }
                } else {
                    None
                };

                let mut page_stream = storage.list_pages(&manifest_prefix, listing_opts, resume);

                tracing::info!(
                    job_id = %job_id,
                    resumable = caps.resumable,
                    "Streaming manifests from storage (page-based), checking for orphans"
                );

                let mut page_count = 0u64;
                let mut stop_manifests = false;
                while let Some(page_result) = page_stream.next().await {
                    let page = match page_result {
                        Ok(p) => p,
                        Err(e) => {
                            // Check if it's an invalid token error - trigger recovery
                            if matches!(
                                e,
                                StorageError::ListingNotResumable
                                    | StorageError::InvalidContinuationToken(_)
                            ) {
                                tracing::warn!(
                                    job_id = %job_id,
                                    error = %e,
                                    "Continuation token invalid or listing not resumable, clearing checkpoint and restarting"
                                );
                                crate::metrics::STORAGE_LISTING_TOKENS_INVALID
                                    .with_label_values(&[storage.backend_name()])
                                    .inc();
                                crate::metrics::GC_CHECKPOINT_INVALIDATED.inc();
                                checkpoint.manifests_token = None;
                                checkpoint.manifests_processed = 0;
                                checkpoint.touch();
                                // Save cleared checkpoint
                                if let Ok(checkpoint_json) = checkpoint.to_json() {
                                    let _ = metadata
                                        .update_gc_job_state(
                                            job_id,
                                            GcJobState::Running.as_str(),
                                            None,
                                            None,
                                            Some(&checkpoint_json),
                                        )
                                        .await;
                                }
                                stop_manifests = true;
                                break; // Will need to restart manually
                            }

                            tracing::warn!(
                                job_id = %job_id,
                                error = %e,
                                "Failed to fetch listing page, stopping sweep"
                            );
                            critical_errors += 1;
                            stats.errors += 1;
                            stop_manifests = true;
                            break;
                        }
                    };

                    page_count += 1;
                    crate::metrics::STORAGE_LISTING_PAGES_FETCHED
                        .with_label_values(&[storage.backend_name()])
                        .inc();

                    // Process all keys in this page
                    let mut page_complete = true;
                    for key in page.keys {
                        // Check if we've hit max iterations limit
                        if stats.items_processed >= config.max_gc_iterations {
                            tracing::warn!(
                                job_id = %job_id,
                                items_processed = stats.items_processed,
                                max_iterations = config.max_gc_iterations,
                                "Storage sweep reached max iterations, stopping (re-run to continue)"
                            );
                            stop_manifests = true;
                            page_complete = false;
                            break;
                        }

                        // Check error rate threshold
                        if let Some(max_error_rate) = config.max_error_rate
                            && stats.items_processed >= 100
                        {
                            let error_rate = critical_errors as f64 / stats.items_processed as f64;
                            if error_rate > max_error_rate {
                                tracing::error!(
                                    job_id = %job_id,
                                    error_rate = format!("{:.2}%", error_rate * 100.0),
                                    max_error_rate = format!("{:.2}%", max_error_rate * 100.0),
                                    items_processed = stats.items_processed,
                                    critical_errors = critical_errors,
                                    "Critical error rate exceeds threshold, aborting storage sweep"
                                );
                                stop_manifests = true;
                                page_complete = false;
                                break;
                            }
                        }

                        stats.items_processed += 1;
                        checkpoint.manifests_processed += 1;

                        // Process this manifest using the helper function
                        let result = process_manifest_orphan(
                            key,
                            metadata.as_ref(),
                            metadata.as_ref(),
                            storage.as_ref(),
                            &config,
                            domain_id,
                            grace_period,
                            grace_time,
                            job_id,
                        )
                        .await;

                        match result {
                            OrphanProcessResult::Deleted => {
                                stats.items_deleted += 1;
                            }
                            OrphanProcessResult::Error => {
                                critical_errors += 1;
                                stats.errors += 1;
                            }
                            OrphanProcessResult::Kept | OrphanProcessResult::Skipped => {
                                // No action needed
                            }
                        }

                        // Rate limiting: delay after processing each item if configured
                        if let Some(delay_ms) = config.batch_delay_ms
                            && stats.items_processed % 100 == 0
                        {
                            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                        }
                    }

                    // Save checkpoint after each page (if backend supports resumption)
                    if page_complete
                        && caps.resumable
                        && let Some(next_token) = page.next_token
                    {
                        checkpoint.manifests_token = Some(next_token.to_base64());
                        checkpoint.touch();

                        // Persist checkpoint to database
                        if let Ok(checkpoint_json) = checkpoint.to_json() {
                            if let Err(e) = metadata
                                .update_gc_job_state(
                                    job_id,
                                    GcJobState::Running.as_str(),
                                    None,
                                    None,
                                    Some(&checkpoint_json),
                                )
                                .await
                            {
                                tracing::warn!(
                                    job_id = %job_id,
                                    error = %e,
                                    "Failed to save checkpoint (non-fatal)"
                                );
                            } else {
                                crate::metrics::GC_CHECKPOINT_SAVED.inc();
                                tracing::debug!(
                                    job_id = %job_id,
                                    page = page_count,
                                    manifests_processed = checkpoint.manifests_processed,
                                    "Saved manifests sweep checkpoint"
                                );
                            }
                        }
                    }

                    if stop_manifests {
                        break;
                    }

                    // Check if we should stop
                    if stats.items_processed >= config.max_gc_iterations {
                        stop_manifests = true;
                        break;
                    }
                }

                if stop_manifests {
                    manifests_completed = false;
                }

                tracing::info!(
                    job_id = %job_id,
                    pages_fetched = page_count,
                    processed = stats.items_processed,
                    deleted = stats.items_deleted,
                    "Completed manifest sweep (streaming mode)"
                );
            } else {
                // LEGACY PATH: Use old Vec-based materialization
                match storage.list_stream(&manifest_prefix).await {
                    Ok(mut manifest_stream) => {
                        tracing::info!(
                            job_id = %job_id,
                            "Streaming manifests from storage (legacy mode), checking for orphans"
                        );

                        while let Some(key_result) = manifest_stream.next().await {
                            // Check if we've hit max iterations limit
                            if stats.items_processed >= config.max_gc_iterations {
                                tracing::warn!(
                                    job_id = %job_id,
                                    items_processed = stats.items_processed,
                                    max_iterations = config.max_gc_iterations,
                                    "Storage sweep reached max iterations, stopping (re-run to continue)"
                                );
                                break;
                            }

                            // Check error rate threshold
                            if let Some(max_error_rate) = config.max_error_rate
                                && stats.items_processed >= 100
                            {
                                let error_rate =
                                    critical_errors as f64 / stats.items_processed as f64;
                                if error_rate > max_error_rate {
                                    tracing::error!(
                                        job_id = %job_id,
                                        error_rate = format!("{:.2}%", error_rate * 100.0),
                                        max_error_rate = format!("{:.2}%", max_error_rate * 100.0),
                                        items_processed = stats.items_processed,
                                        critical_errors = critical_errors,
                                        "Critical error rate exceeds threshold, aborting storage sweep"
                                    );
                                    break;
                                }
                            }

                            let key = match key_result {
                                Ok(k) => k,
                                Err(e) => {
                                    tracing::warn!(
                                        job_id = %job_id,
                                        error = %e,
                                        "Failed to list manifest key, skipping"
                                    );
                                    critical_errors += 1;
                                    stats.errors += 1;
                                    continue;
                                }
                            };

                            stats.items_processed += 1;

                            // Process this manifest using the helper function
                            let result = process_manifest_orphan(
                                key,
                                metadata.as_ref(),
                                metadata.as_ref(),
                                storage.as_ref(),
                                &config,
                                domain_id,
                                grace_period,
                                grace_time,
                                job_id,
                            )
                            .await;

                            match result {
                                OrphanProcessResult::Deleted => {
                                    stats.items_deleted += 1;
                                }
                                OrphanProcessResult::Error => {
                                    critical_errors += 1;
                                    stats.errors += 1;
                                }
                                OrphanProcessResult::Kept | OrphanProcessResult::Skipped => {
                                    // No action needed
                                }
                            }

                            // Rate limiting: delay after processing each item if configured
                            if let Some(delay_ms) = config.batch_delay_ms
                                && stats.items_processed % 100 == 0
                            {
                                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms))
                                    .await;
                            }
                        }

                        tracing::info!(
                            job_id = %job_id,
                            processed = stats.items_processed,
                            deleted = stats.items_deleted,
                            "Completed manifest sweep (legacy mode)"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            job_id = %job_id,
                            error = %e,
                            "Failed to list manifests from storage"
                        );
                        critical_errors += 1;
                        stats.errors += 1;
                    }
                }
            }

            // Sweep temporary NAR objects (tmp/nar/)
            // These are orphaned multipart uploads from TrueStreamingCompressor that were
            // dropped without calling finish() or abort(). They have no metadata, so we
            // only check object age against the grace period.
            tracing::debug!(
                job_id = %job_id,
                "Sweeping orphaned tmp/nar/ objects from storage"
            );

            match storage.list_stream("tmp/nar/").await {
                Ok(mut tmp_stream) => {
                    let mut tmp_processed = 0u64;
                    let mut tmp_deleted = 0u64;

                    while let Some(key_result) = tmp_stream.next().await {
                        let key = match key_result {
                            Ok(k) => k,
                            Err(e) => {
                                tracing::warn!(
                                    job_id = %job_id,
                                    error = %e,
                                    "Failed to list tmp/nar/ key, skipping"
                                );
                                stats.errors += 1;
                                continue;
                            }
                        };

                        tmp_processed += 1;
                        stats.items_processed += 1;

                        // Get object metadata to check age
                        let obj_meta = match storage.head(&key).await {
                            Ok(meta) => meta,
                            Err(cellar_storage::StorageError::NotFound(_)) => {
                                // Object disappeared, skip
                                tracing::debug!(
                                    job_id = %job_id,
                                    key = %key,
                                    "tmp/nar/ object not found, likely already cleaned up"
                                );
                                continue;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    job_id = %job_id,
                                    key = %key,
                                    error = %e,
                                    "Failed to get tmp/nar/ object metadata"
                                );
                                stats.errors += 1;
                                continue;
                            }
                        };

                        // Check if object is old enough to delete
                        let obj_time = obj_meta.last_modified.unwrap_or(grace_time);
                        if obj_time > grace_time {
                            let age = OffsetDateTime::now_utc() - obj_time;
                            tracing::debug!(
                                job_id = %job_id,
                                key = %key,
                                age_secs = age.whole_seconds(),
                                "tmp/nar/ object within grace period, keeping"
                            );
                            continue;
                        }

                        // Object is old enough, delete it
                        let age = OffsetDateTime::now_utc() - obj_time;
                        if config.dry_run {
                            tracing::info!(
                                job_id = %job_id,
                                key = %key,
                                age_secs = age.whole_seconds(),
                                "[DRY-RUN] Would delete orphaned tmp/nar/ object"
                            );
                            tmp_deleted += 1;
                            stats.items_deleted += 1;
                        } else {
                            match storage.delete(&key).await {
                                Ok(()) => {
                                    tracing::info!(
                                        job_id = %job_id,
                                        key = %key,
                                        age_secs = age.whole_seconds(),
                                        "Deleted orphaned tmp/nar/ object"
                                    );
                                    tmp_deleted += 1;
                                    stats.items_deleted += 1;
                                    crate::metrics::GC_ITEMS_DELETED
                                        .with_label_values(&["tmp_nar"])
                                        .inc();
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        job_id = %job_id,
                                        key = %key,
                                        error = %e,
                                        "Failed to delete orphaned tmp/nar/ object"
                                    );
                                    stats.errors += 1;
                                }
                            }
                        }
                    }

                    tracing::info!(
                        job_id = %job_id,
                        processed = tmp_processed,
                        deleted = tmp_deleted,
                        "Completed tmp/nar/ sweep"
                    );
                }
                Err(e) => {
                    // tmp/nar/ prefix might not exist, which is fine
                    tracing::debug!(
                        job_id = %job_id,
                        error = %e,
                        "No tmp/nar/ objects found (prefix may not exist)"
                    );
                }
            }

            // Clear checkpoint on successful completion (if using streaming)
            if config.use_streaming_listing && chunks_completed && manifests_completed {
                tracing::debug!(
                    job_id = %job_id,
                    "Clearing checkpoint after sweep completion"
                );
                if let Err(e) = metadata
                    .update_gc_job_state(
                        job_id,
                        GcJobState::Running.as_str(),
                        None,
                        None,
                        None, // Clear checkpoint
                    )
                    .await
                {
                    tracing::warn!(
                        job_id = %job_id,
                        error = %e,
                        "Failed to clear checkpoint (non-fatal)"
                    );
                }
            } else if config.use_streaming_listing {
                tracing::debug!(
                    job_id = %job_id,
                    chunks_completed = chunks_completed,
                    manifests_completed = manifests_completed,
                    "Retaining checkpoint after partial sweep"
                );
            }

            tracing::info!(
                job_id = %job_id,
                total_processed = stats.items_processed,
                total_deleted = stats.items_deleted,
                total_errors = stats.errors,
                critical_errors = critical_errors,
                "Storage sweep completed"
            );
        }
    }

    Ok(stats)
}

/// Generate a random token secret using cryptographically secure RNG.
fn generate_token_secret() -> String {
    use base64::Engine;
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Hash a token for storage.
fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    let result = hasher.finalize();
    result.iter().map(|b| format!("{b:02x}")).collect()
}

// =============================================================================
// Cache/Domain Management Validation Functions
// =============================================================================

/// Validate cache name format.
fn validate_cache_name(name: &str) -> ApiResult<()> {
    if name.len() < 3 || name.len() > 64 {
        return Err(ApiError::BadRequest(
            "cache name must be 3-64 characters".to_string(),
        ));
    }

    // Must be lowercase alphanumeric with hyphens, no leading/trailing hyphens
    let valid = name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !name.starts_with('-')
        && !name.ends_with('-');

    if !valid {
        return Err(ApiError::BadRequest(
            "cache name must be lowercase alphanumeric with hyphens (no leading/trailing hyphens)"
                .to_string(),
        ));
    }

    Ok(())
}

/// Validate domain name format.
fn validate_domain_name(name: &str) -> ApiResult<()> {
    if name.len() < 3 || name.len() > 64 {
        return Err(ApiError::BadRequest(
            "domain name must be 3-64 characters".to_string(),
        ));
    }

    let valid = name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !name.starts_with('-')
        && !name.ends_with('-');

    if !valid {
        return Err(ApiError::BadRequest(
            "domain name must be lowercase alphanumeric with hyphens (no leading/trailing hyphens)"
                .to_string(),
        ));
    }

    Ok(())
}

/// Validate base URL format. Empty string is valid for UPDATE operations (means "clear").
/// For CREATE operations, empty strings should be normalized to None before storage.
fn validate_base_url(url: &str) -> ApiResult<()> {
    // Empty string means "clear the URL" - this is valid
    if url.is_empty() {
        return Ok(());
    }

    if url.ends_with('/') {
        return Err(ApiError::BadRequest(
            "public_base_url must not have trailing slash".to_string(),
        ));
    }

    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(ApiError::BadRequest(
            "public_base_url must be HTTP or HTTPS URL".to_string(),
        ));
    }

    Ok(())
}

// =============================================================================
// Domain Management Handlers
// =============================================================================

/// POST /v1/admin/domains - Create a new domain.
pub async fn create_domain(
    State(state): State<AppState>,
    req: Request,
) -> ApiResult<(StatusCode, Json<DomainResponse>)> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    if auth.token.cache_id.is_some() {
        return Err(ApiError::Forbidden(
            "domain management requires global admin token (cache_id must be None)".to_string(),
        ));
    }

    let bytes = axum::body::to_bytes(req.into_body(), MAX_ADMIN_BODY_SIZE)
        .await
        .map_err(|e| ApiError::BadRequest(format!("failed to read body: {e}")))?;
    let body: CreateDomainRequest = serde_json::from_slice(&bytes)
        .map_err(|e| ApiError::BadRequest(format!("invalid JSON: {e}")))?;

    validate_domain_name(&body.domain_name)?;

    if state
        .metadata
        .get_domain_by_name(&body.domain_name)
        .await?
        .is_some()
    {
        return Err(ApiError::Conflict(format!(
            "domain '{}' already exists",
            body.domain_name
        )));
    }

    let now = OffsetDateTime::now_utc();
    let domain = DomainRow {
        domain_id: Uuid::new_v4(),
        domain_name: body.domain_name,
        is_default: false,
        created_at: now,
        updated_at: now,
    };

    state.metadata.create_domain(&domain).await?;

    Ok((StatusCode::CREATED, Json(domain_row_to_response(domain)?)))
}

/// GET /v1/admin/domains - List all domains.
pub async fn list_domains(
    State(state): State<AppState>,
    req: Request,
) -> ApiResult<Json<ListDomainsResponse>> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    if auth.token.cache_id.is_some() {
        return Err(ApiError::Forbidden(
            "domain management requires global admin token (cache_id must be None)".to_string(),
        ));
    }

    let domains = state
        .metadata
        .list_domains()
        .await?
        .into_iter()
        .map(domain_row_to_response)
        .collect::<ApiResult<Vec<_>>>()?;

    Ok(Json(ListDomainsResponse { domains }))
}

/// GET /v1/admin/domains/{domain_id} - Get a domain by ID.
pub async fn get_domain(
    State(state): State<AppState>,
    Path(domain_id): Path<String>,
    req: Request,
) -> ApiResult<Json<DomainResponse>> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    if auth.token.cache_id.is_some() {
        return Err(ApiError::Forbidden(
            "domain management requires global admin token (cache_id must be None)".to_string(),
        ));
    }

    let domain_id = Uuid::parse_str(&domain_id)
        .map_err(|e| ApiError::BadRequest(format!("invalid domain ID: {e}")))?;

    let domain = state
        .metadata
        .get_domain(domain_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("domain not found".to_string()))?;

    Ok(Json(domain_row_to_response(domain)?))
}

/// GET /v1/admin/domains/by-name/{domain_name} - Get a domain by name.
pub async fn get_domain_by_name(
    State(state): State<AppState>,
    Path(domain_name): Path<String>,
    req: Request,
) -> ApiResult<Json<DomainResponse>> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    if auth.token.cache_id.is_some() {
        return Err(ApiError::Forbidden(
            "domain management requires global admin token (cache_id must be None)".to_string(),
        ));
    }

    validate_domain_name(&domain_name)?;

    let domain = state
        .metadata
        .get_domain_by_name(&domain_name)
        .await?
        .ok_or_else(|| ApiError::NotFound("domain not found".to_string()))?;

    Ok(Json(domain_row_to_response(domain)?))
}

/// PUT /v1/admin/domains/{domain_id} - Update a domain.
pub async fn update_domain(
    State(state): State<AppState>,
    Path(domain_id): Path<String>,
    req: Request,
) -> ApiResult<Json<DomainResponse>> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    if auth.token.cache_id.is_some() {
        return Err(ApiError::Forbidden(
            "domain management requires global admin token (cache_id must be None)".to_string(),
        ));
    }

    let domain_id = Uuid::parse_str(&domain_id)
        .map_err(|e| ApiError::BadRequest(format!("invalid domain ID: {e}")))?;

    let bytes = axum::body::to_bytes(req.into_body(), MAX_ADMIN_BODY_SIZE)
        .await
        .map_err(|e| ApiError::BadRequest(format!("failed to read body: {e}")))?;
    let body: UpdateDomainRequest = serde_json::from_slice(&bytes)
        .map_err(|e| ApiError::BadRequest(format!("invalid JSON: {e}")))?;

    validate_domain_name(&body.domain_name)?;

    let mut domain = state
        .metadata
        .get_domain(domain_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("domain not found".to_string()))?;

    if domain.is_default {
        return Err(ApiError::BadRequest(
            "default domain name cannot be changed".to_string(),
        ));
    }

    if let Some(existing) = state.metadata.get_domain_by_name(&body.domain_name).await?
        && existing.domain_id != domain.domain_id
    {
        return Err(ApiError::Conflict(format!(
            "domain '{}' already exists",
            body.domain_name
        )));
    }

    domain.domain_name = body.domain_name;
    domain.updated_at = OffsetDateTime::now_utc();

    state.metadata.update_domain(&domain).await?;

    Ok(Json(domain_row_to_response(domain)?))
}

/// DELETE /v1/admin/domains/{domain_id} - Delete a domain.
pub async fn delete_domain(
    State(state): State<AppState>,
    Path(domain_id): Path<String>,
    req: Request,
) -> ApiResult<StatusCode> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    if auth.token.cache_id.is_some() {
        return Err(ApiError::Forbidden(
            "domain management requires global admin token (cache_id must be None)".to_string(),
        ));
    }

    let domain_id = Uuid::parse_str(&domain_id)
        .map_err(|e| ApiError::BadRequest(format!("invalid domain ID: {e}")))?;

    let domain = state
        .metadata
        .get_domain(domain_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("domain not found".to_string()))?;

    if domain.is_default {
        return Err(ApiError::BadRequest(
            "default domain cannot be deleted".to_string(),
        ));
    }

    let usage = state.metadata.get_domain_usage(domain_id).await?;
    if usage.caches > 0
        || usage.upload_sessions > 0
        || usage.store_paths > 0
        || usage.chunks > 0
        || usage.manifests > 0
    {
        return Err(ApiError::Conflict(format!(
            "domain is in use (caches={}, upload_sessions={}, store_paths={}, chunks={}, manifests={})",
            usage.caches, usage.upload_sessions, usage.store_paths, usage.chunks, usage.manifests
        )));
    }

    state.metadata.delete_domain(domain_id).await?;

    Ok(StatusCode::NO_CONTENT)
}

// =============================================================================
// Cache Management Handlers
// =============================================================================

/// POST /v1/admin/caches - Create a new cache.
pub async fn create_cache(
    State(state): State<AppState>,
    req: Request,
) -> ApiResult<(StatusCode, Json<CreateCacheResponse>)> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    // Enforce global-only admin: cache management requires cache_id = None
    if auth.token.cache_id.is_some() {
        return Err(ApiError::Forbidden(
            "cache management requires global admin token (cache_id must be None)".to_string(),
        ));
    }

    let bytes = axum::body::to_bytes(req.into_body(), MAX_ADMIN_BODY_SIZE)
        .await
        .map_err(|e| ApiError::BadRequest(format!("failed to read body: {e}")))?;
    let body: CreateCacheRequest = serde_json::from_slice(&bytes)
        .map_err(|e| ApiError::BadRequest(format!("invalid JSON: {e}")))?;

    // Validate cache_name
    validate_cache_name(&body.cache_name)?;

    // Validate public_base_url if provided
    if let Some(ref url) = body.public_base_url {
        validate_base_url(url)?;
    }

    // Check for duplicate name
    if state
        .metadata
        .get_cache_by_name(&body.cache_name)
        .await?
        .is_some()
    {
        return Err(ApiError::Conflict(format!(
            "cache with name '{}' already exists",
            body.cache_name
        )));
    }

    // If setting as default, verify no other cache is already default
    if body.is_default {
        if !body.is_public {
            return Err(ApiError::BadRequest(
                "is_default can only be set on public caches".to_string(),
            ));
        }
        if let Some(existing_default) = state.metadata.get_default_public_cache().await? {
            return Err(ApiError::Conflict(format!(
                "cache '{}' is already set as default. Unset it first before setting a new default.",
                existing_default.cache_name
            )));
        }
    }

    let now = OffsetDateTime::now_utc();
    let cache_id = Uuid::new_v4();
    let (domain_id, domain_name) = resolve_domain_from_request(
        &*state.metadata,
        body.domain_id.as_deref(),
        body.domain_name.as_deref(),
    )
    .await?;

    if body.is_public && domain_id != default_storage_domain_id() {
        return Err(ApiError::BadRequest(
            "public caches must use the default storage domain".to_string(),
        ));
    }

    let cache_row = CacheRow {
        cache_id,
        domain_id,
        cache_name: body.cache_name.clone(),
        public_base_url: body
            .public_base_url
            .as_ref()
            .filter(|s| !s.is_empty())
            .cloned(),
        is_public: body.is_public,
        is_default: body.is_default,
        created_at: now,
        updated_at: now,
    };

    state.metadata.create_cache(&cache_row).await?;

    let now_str = now
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|e| ApiError::Internal(format!("failed to format timestamp: {e}")))?;

    Ok((
        StatusCode::CREATED,
        Json(CreateCacheResponse {
            cache_id: cache_id.to_string(),
            cache_name: cache_row.cache_name,
            domain_id: cache_row.domain_id.to_string(),
            domain_name,
            public_base_url: cache_row.public_base_url,
            is_public: cache_row.is_public,
            is_default: cache_row.is_default,
            created_at: now_str.clone(),
            updated_at: now_str,
        }),
    ))
}

/// GET /v1/admin/caches - List all caches.
pub async fn list_caches(
    State(state): State<AppState>,
    req: Request,
) -> ApiResult<Json<ListCachesResponse>> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    // Enforce global-only admin: cache management requires cache_id = None
    if auth.token.cache_id.is_some() {
        return Err(ApiError::Forbidden(
            "cache management requires global admin token (cache_id must be None)".to_string(),
        ));
    }

    let cache_rows = state.metadata.list_caches().await?;
    let domain_map: std::collections::HashMap<Uuid, String> = state
        .metadata
        .list_domains()
        .await?
        .into_iter()
        .map(|domain| (domain.domain_id, domain.domain_name))
        .collect();

    let caches = cache_rows
        .into_iter()
        .map(|row| {
            Ok(CacheResponse {
                cache_id: row.cache_id.to_string(),
                cache_name: row.cache_name,
                domain_id: row.domain_id.to_string(),
                domain_name: domain_map.get(&row.domain_id).cloned(),
                public_base_url: row.public_base_url,
                is_public: row.is_public,
                is_default: row.is_default,
                created_at: row
                    .created_at
                    .format(&time::format_description::well_known::Rfc3339)
                    .map_err(|e| ApiError::Internal(format!("failed to format created_at: {e}")))?,
                updated_at: row
                    .updated_at
                    .format(&time::format_description::well_known::Rfc3339)
                    .map_err(|e| ApiError::Internal(format!("failed to format updated_at: {e}")))?,
            })
        })
        .collect::<ApiResult<Vec<_>>>()?;

    Ok(Json(ListCachesResponse { caches }))
}

/// GET /v1/admin/caches/{cache_id} - Get a specific cache.
pub async fn get_cache(
    State(state): State<AppState>,
    Path(cache_id): Path<String>,
    req: Request,
) -> ApiResult<Json<CacheResponse>> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    // Enforce global-only admin: cache management requires cache_id = None
    if auth.token.cache_id.is_some() {
        return Err(ApiError::Forbidden(
            "cache management requires global admin token (cache_id must be None)".to_string(),
        ));
    }

    let cache_id = Uuid::parse_str(&cache_id)
        .map_err(|e| ApiError::BadRequest(format!("invalid cache ID: {e}")))?;

    let cache = state
        .metadata
        .get_cache(cache_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("cache not found".to_string()))?;
    let domain_name = state
        .metadata
        .get_domain(cache.domain_id)
        .await?
        .map(|domain| domain.domain_name);

    Ok(Json(CacheResponse {
        cache_id: cache.cache_id.to_string(),
        cache_name: cache.cache_name,
        domain_id: cache.domain_id.to_string(),
        domain_name,
        public_base_url: cache.public_base_url,
        is_public: cache.is_public,
        is_default: cache.is_default,
        created_at: cache
            .created_at
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|e| ApiError::Internal(format!("failed to format created_at: {e}")))?,
        updated_at: cache
            .updated_at
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|e| ApiError::Internal(format!("failed to format updated_at: {e}")))?,
    }))
}

/// PUT /v1/admin/caches/{cache_id} - Update an existing cache.
pub async fn update_cache(
    State(state): State<AppState>,
    Path(cache_id): Path<String>,
    req: Request,
) -> ApiResult<Json<CacheResponse>> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    // Enforce global-only admin: cache management requires cache_id = None
    if auth.token.cache_id.is_some() {
        return Err(ApiError::Forbidden(
            "cache management requires global admin token (cache_id must be None)".to_string(),
        ));
    }

    let cache_id = Uuid::parse_str(&cache_id)
        .map_err(|e| ApiError::BadRequest(format!("invalid cache ID: {e}")))?;

    let bytes = axum::body::to_bytes(req.into_body(), MAX_ADMIN_BODY_SIZE)
        .await
        .map_err(|e| ApiError::BadRequest(format!("failed to read body: {e}")))?;
    let body: UpdateCacheRequest = serde_json::from_slice(&bytes)
        .map_err(|e| ApiError::BadRequest(format!("invalid JSON: {e}")))?;

    // Get existing cache
    let mut cache = state
        .metadata
        .get_cache(cache_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("cache not found".to_string()))?;

    let mut target_domain_id = cache.domain_id;
    if body.domain_id.is_some() || body.domain_name.is_some() {
        let (resolved_id, _) = resolve_domain_from_request(
            &*state.metadata,
            body.domain_id.as_deref(),
            body.domain_name.as_deref(),
        )
        .await?;
        target_domain_id = resolved_id;
    }

    let mut target_is_public = cache.is_public;
    if let Some(is_public) = body.is_public {
        target_is_public = is_public;
    }

    let mut target_is_default = cache.is_default;
    if !target_is_public {
        target_is_default = false;
    }
    if let Some(is_default) = body.is_default {
        target_is_default = is_default;
    }

    // Apply updates
    if let Some(name) = body.cache_name {
        validate_cache_name(&name)?;

        // Check for duplicate name (excluding current cache)
        if let Some(existing) = state.metadata.get_cache_by_name(&name).await?
            && existing.cache_id != cache_id
        {
            return Err(ApiError::Conflict(format!(
                "cache with name '{}' already exists",
                name
            )));
        }

        cache.cache_name = name;
    }

    if let Some(url) = body.public_base_url {
        validate_base_url(&url)?;

        // Empty string means clear/unset
        cache.public_base_url = if url.is_empty() { None } else { Some(url) };
    }

    if target_is_default && !target_is_public {
        return Err(ApiError::BadRequest(
            "is_default can only be set on public caches".to_string(),
        ));
    }

    if target_is_public && target_domain_id != default_storage_domain_id() {
        return Err(ApiError::BadRequest(
            "public caches must use the default storage domain".to_string(),
        ));
    }
    if target_is_default && target_domain_id != default_storage_domain_id() {
        return Err(ApiError::BadRequest(
            "default public cache must use the default storage domain".to_string(),
        ));
    }

    if target_is_default
        && let Some(existing_default) = state.metadata.get_default_public_cache().await?
        && existing_default.cache_id != cache_id
    {
        return Err(ApiError::Conflict(format!(
            "cache '{}' is already set as default. Unset it first before setting a new default.",
            existing_default.cache_name
        )));
    }

    if target_domain_id != cache.domain_id {
        let store_paths = state.metadata.count_store_paths(Some(cache_id)).await?;
        if store_paths > 0 {
            return Err(ApiError::Conflict(
                "cannot change storage domain while store paths exist".to_string(),
            ));
        }

        let active_uploads = state.metadata.count_active_uploads(Some(cache_id)).await?;
        if active_uploads > 0 {
            return Err(ApiError::Conflict(
                "cannot change storage domain while active uploads exist".to_string(),
            ));
        }
        cache.domain_id = target_domain_id;
    }

    cache.is_public = target_is_public;
    cache.is_default = target_is_default;

    cache.updated_at = OffsetDateTime::now_utc();

    state.metadata.update_cache(&cache).await?;

    Ok(Json(CacheResponse {
        cache_id: cache.cache_id.to_string(),
        cache_name: cache.cache_name,
        domain_id: cache.domain_id.to_string(),
        domain_name: state
            .metadata
            .get_domain(cache.domain_id)
            .await?
            .map(|domain| domain.domain_name),
        public_base_url: cache.public_base_url,
        is_public: cache.is_public,
        is_default: cache.is_default,
        created_at: cache
            .created_at
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|e| ApiError::Internal(format!("failed to format created_at: {e}")))?,
        updated_at: cache
            .updated_at
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|e| ApiError::Internal(format!("failed to format updated_at: {e}")))?,
    }))
}

/// DELETE /v1/admin/caches/{cache_id} - Delete a cache.
///
/// This operation is only allowed if:
/// - No active upload sessions exist for this cache
/// - No store paths exist for this cache
/// - No tokens are scoped to this cache
pub async fn delete_cache(
    State(state): State<AppState>,
    Path(cache_id): Path<String>,
    req: Request,
) -> ApiResult<StatusCode> {
    let auth = require_auth(&req)?;
    auth.require_scope(TokenScope::CacheAdmin)?;

    // Enforce global-only admin: cache management requires cache_id = None
    if auth.token.cache_id.is_some() {
        return Err(ApiError::Forbidden(
            "cache management requires global admin token (cache_id must be None)".to_string(),
        ));
    }

    let cache_id = Uuid::parse_str(&cache_id)
        .map_err(|e| ApiError::BadRequest(format!("invalid cache ID: {e}")))?;

    // Cascade delete operational data atomically (includes precondition checks)
    tracing::info!(cache_id = %cache_id, "Performing atomic cascade deletion");

    let stats = state.metadata.delete_cache_with_cascade(cache_id).await?;

    tracing::info!(
        cache_id = %cache_id,
        gc_jobs = stats.gc_jobs,
        signing_keys = stats.signing_keys,
        trusted_keys = stats.trusted_keys,
        tombstones = stats.tombstones,
        upload_sessions = stats.upload_sessions,
        upload_expected_chunks = stats.upload_expected_chunks,
        tokens = stats.tokens,
        "Cascade deletion complete"
    );

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_hash_from_key_validates_formats() {
        let hash = "a".repeat(64);
        let domain = "00000000-0000-0000-0000-000000000001";
        let chunk_key = format!("domains/{domain}/chunks/ab/cd/{hash}");
        let manifest_key = format!("domains/{domain}/manifests/ab/cd/{hash}.json");

        assert_eq!(
            extract_hash_from_key(&chunk_key, false),
            Some(hash.as_str())
        );
        assert_eq!(
            extract_hash_from_key(&manifest_key, true),
            Some(hash.as_str())
        );
        assert!(
            extract_hash_from_key(
                "domains/00000000-0000-0000-0000-000000000001/chunks/ab/cd/nothex",
                false
            )
            .is_none()
        );
        assert!(
            extract_hash_from_key(
                "domains/00000000-0000-0000-0000-000000000001/manifests/ab/cd/bad.json",
                true
            )
            .is_none()
        );
    }

    #[test]
    fn token_helpers_generate_and_hash() {
        let secret = generate_token_secret();
        assert!(!secret.is_empty());
        assert!(!secret.contains('='));

        let digest = hash_token(&secret);
        assert_eq!(digest.len(), 64);
    }

    #[test]
    fn validate_cache_name_rules() {
        validate_cache_name("cache-1").unwrap();
        assert!(validate_cache_name("A").is_err());
        assert!(validate_cache_name("-bad").is_err());
        assert!(validate_cache_name("bad-").is_err());
        assert!(validate_cache_name("bad_name").is_err());
    }

    #[test]
    fn validate_base_url_rules() {
        validate_base_url("").unwrap();
        validate_base_url("https://example.com").unwrap();
        assert!(validate_base_url("http://example.com/").is_err());
        assert!(validate_base_url("ftp://example.com").is_err());
    }
}
