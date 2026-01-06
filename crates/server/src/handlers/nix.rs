//! Nix compatibility endpoints (read path).

use crate::auth::get_auth;
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use axum::response::{IntoResponse, Response};
use cellar_core::token::TokenScope;
use futures::StreamExt;
use serde::Serialize;
use uuid::Uuid;

/// Generate a cache-prefixed storage key path.
/// For tenant isolation, all NAR and narinfo objects are prefixed with the cache_id.
/// Public caches (cache_id = None) use "public" as the prefix.
fn cache_prefix(cache_id: Option<Uuid>) -> String {
    cache_id
        .map(|id| id.to_string())
        .unwrap_or_else(|| "public".to_string())
}

/// Extracted auth and cache info from request (for Send-safe async handling).
struct ReadAccessInfo {
    /// Authenticated user's cache_id if valid CacheRead scope
    auth_cache_id: Option<Option<Uuid>>,
    /// X-Cache-Name header value if present
    cache_name: Option<String>,
}

/// Extract auth and cache info from request synchronously.
fn extract_read_access_info(req: &Request) -> ReadAccessInfo {
    let auth_cache_id = get_auth(req)
        .filter(|auth| auth.has_scope(TokenScope::CacheRead))
        .map(|auth| auth.token.cache_id);

    let cache_name = req
        .headers()
        .get("x-cache-name")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    ReadAccessInfo {
        auth_cache_id,
        cache_name,
    }
}

/// Get cache_id for read access, allowing unauthenticated access for public caches.
///
/// Resolution order:
/// 1. If authenticated with valid CacheRead scope -> use token's cache_id (even if None)
/// 2. If unauthenticated + X-Cache-Name header present + cache is public -> use that cache
/// 3. If unauthenticated + no header + default public cache exists -> use default
/// 4. Otherwise -> Unauthorized error
///
/// This enables public caches to be read without authentication while maintaining
/// security for private caches.
///
/// # Global tokens and cache access
///
/// Global tokens (`cache_id = None`) are designed for administrative operations like
/// managing caches and creating user tokens. When authenticated with a global token:
///
/// - The token's `cache_id = None` is returned directly (step 1)
/// - The `X-Cache-Name` header is NOT consulted (that's only for unauthenticated requests)
/// - This means global tokens cannot target specific caches for read operations
///
/// This is intentional for security:
///
/// - **Separation of duties**: Administration and data access are separate roles
/// - **Least privilege**: Admin tokens should not expose private cache data
/// - **Blast radius**: A leaked admin token cannot read all private cache contents
///
/// If an admin needs read access to a specific cache, they should create a cache-specific
/// `CacheRead` token for that cache.
async fn get_read_cache_id(state: &AppState, info: ReadAccessInfo) -> ApiResult<Option<Uuid>> {
    // Try authentication first
    if let Some(cache_id) = info.auth_cache_id {
        // Global tokens (cache_id = None) resolve to the default public cache,
        // consistent with the upload path which stores data under the default cache UUID.
        if cache_id.is_none() {
            let default_cache = state
                .metadata
                .get_default_public_cache()
                .await?
                .ok_or_else(|| {
                    ApiError::BadRequest(
                        "global token requires a default public cache to be configured".to_string(),
                    )
                })?;
            return Ok(Some(default_cache.cache_id));
        }
        return Ok(cache_id);
    }

    // No valid auth - check for public cache access via X-Cache-Name header
    if let Some(name) = info.cache_name {
        if let Some(cache) = state.metadata.get_cache_by_name(&name).await?
            && cache.is_public
        {
            return Ok(Some(cache.cache_id));
        }
        // Cache doesn't exist or is not public - return same error to prevent
        // enumeration of private cache names.
        return Err(ApiError::NotFound(format!("cache not found: {name}")));
    }

    // No auth and no X-Cache-Name header - try default public cache
    if let Some(default_cache) = state.metadata.get_default_public_cache().await? {
        return Ok(Some(default_cache.cache_id));
    }

    // No auth, no header, no default cache
    Err(ApiError::Unauthorized(
        "authentication required (or set X-Cache-Name header, or configure a default public cache)"
            .to_string(),
    ))
}

/// Nix cache info response.
#[derive(Debug, Serialize)]
pub struct NixCacheInfo {
    #[serde(rename = "StoreDir")]
    store_dir: &'static str,
    #[serde(rename = "WantMassQuery")]
    want_mass_query: u8,
    #[serde(rename = "Priority")]
    priority: u32,
}

/// GET /nix-cache-info - Standard Nix cache metadata.
pub async fn get_nix_cache_info() -> impl IntoResponse {
    let info = NixCacheInfo {
        store_dir: "/nix/store",
        want_mass_query: 1,
        priority: 40,
    };

    // Format as key: value pairs
    let body = format!(
        "StoreDir: {}\nWantMassQuery: {}\nPriority: {}\n",
        info.store_dir, info.want_mass_query, info.priority
    );

    (
        StatusCode::OK,
        [(CONTENT_TYPE, "text/x-nix-cache-info")],
        body,
    )
}

/// Fallback handler for narinfo requests.
/// Handles /{hash}.narinfo paths since axum doesn't support /{param}.suffix patterns.
/// Supports both authenticated access and unauthenticated access for public caches.
pub async fn narinfo_fallback(State(state): State<AppState>, req: Request) -> Response {
    let path = req.uri().path().to_string();

    // Extract auth info before async call (Request is not Sync)
    let info = extract_read_access_info(&req);

    // Get cache_id with public cache support
    let cache_id = match get_read_cache_id(&state, info).await {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

    // Check if this is a narinfo request
    if let Some(hash) = path
        .strip_prefix('/')
        .and_then(|p| p.strip_suffix(".narinfo"))
    {
        // Validate hash format (32 chars, base32)
        if hash.len() == 32 && hash.chars().all(|c| c.is_ascii_alphanumeric()) {
            match get_narinfo_internal(&state, cache_id, hash).await {
                Ok(response) => return response,
                Err(e) => return e.into_response(),
            }
        }
    }

    // Not a narinfo request - return 404
    (StatusCode::NOT_FOUND, "Not Found").into_response()
}

/// GET /{store_path_hash}.narinfo - Get signed narinfo.
/// Supports both authenticated access and unauthenticated access for public caches.
pub async fn get_narinfo(
    State(state): State<AppState>,
    Path(store_path_hash): Path<String>,
    req: Request,
) -> ApiResult<Response> {
    // Extract auth info before async call (Request is not Sync)
    let info = extract_read_access_info(&req);
    let cache_id = get_read_cache_id(&state, info).await?;
    get_narinfo_internal(&state, cache_id, &store_path_hash).await
}

/// Internal narinfo handler.
async fn get_narinfo_internal(
    state: &AppState,
    cache_id: Option<Uuid>,
    hash: &str,
) -> ApiResult<Response> {
    // Check if store path exists and is visible (scoped to cache_id)
    if !state.metadata.store_path_visible(cache_id, hash).await? {
        return Err(ApiError::NotFound(format!("store path not found: {hash}")));
    }

    // Get narinfo from storage (cache_id prefixed for tenant isolation)
    let narinfo_key = format!("{}/narinfo/{}.narinfo", cache_prefix(cache_id), hash);
    let narinfo_data = state.storage.get(&narinfo_key).await?;

    Ok((
        StatusCode::OK,
        [(CONTENT_TYPE, "text/x-nix-narinfo")],
        Body::from(narinfo_data),
    )
        .into_response())
}

/// GET /nar/{nar_path} - Serve NAR file (compressed or uncompressed).
///
/// This handler serves NAR files. If compression is enabled and a compressed
/// version exists, it serves that directly. Otherwise, it reconstructs the
/// NAR from chunks.
///
/// Supports both authenticated access and unauthenticated access for public caches.
pub async fn get_nar(
    State(state): State<AppState>,
    Path(nar_path): Path<String>,
    req: Request,
) -> ApiResult<Response> {
    // Extract auth info before async call (Request is not Sync)
    let info = extract_read_access_info(&req);
    let cache_id = get_read_cache_id(&state, info).await?;

    // Parse the path to extract hash and determine if compressed version is requested
    let (hash, requested_compression) = parse_nar_path(&nar_path);

    // Get store path record (scoped to cache_id for tenant isolation)
    let store_path = state
        .metadata
        .get_store_path(cache_id, &hash)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("store path not found: {hash}")))?;

    if store_path.visibility_state != "visible" {
        return Err(ApiError::NotFound(format!(
            "store path not visible: {hash}"
        )));
    }

    // Handle compressed NAR requests
    if let Some(compression) = requested_compression {
        // Compression::None is just an uncompressed .nar file
        if compression != cellar_core::narinfo::Compression::None {
            let extension = compression.extension();
            let nar_key = format!("{}/nar/{}.nar{}", cache_prefix(cache_id), hash, extension);

            // Use streaming to avoid loading entire compressed file into memory.
            // First get metadata for Content-Length header.
            match state.storage.head(&nar_key).await {
                Ok(meta) => {
                    // Stream directly from storage to HTTP response
                    let stream = state.storage.get_stream(&nar_key).await?;
                    let body_stream = stream
                        .map(|result| result.map_err(|e| std::io::Error::other(e.to_string())));

                    let content_type = match compression {
                        cellar_core::narinfo::Compression::Zstd => "application/zstd",
                        cellar_core::narinfo::Compression::Xz => "application/x-xz",
                        cellar_core::narinfo::Compression::Gzip => "application/gzip",
                        cellar_core::narinfo::Compression::Bzip2 => "application/x-bzip2",
                        cellar_core::narinfo::Compression::None => "application/x-nix-nar",
                    };

                    return Ok((
                        StatusCode::OK,
                        [
                            (CONTENT_TYPE, content_type),
                            (CONTENT_LENGTH, &meta.size.to_string()),
                        ],
                        Body::from_stream(body_stream),
                    )
                        .into_response());
                }
                Err(e) => {
                    // Check if it's a NotFound error or a real storage error
                    match e {
                        cellar_storage::StorageError::NotFound(_) => {
                            // Compressed version not available - do NOT fall back to uncompressed
                            // as that would corrupt the response (client expects compressed data)
                            return Err(ApiError::NotFound(format!(
                                "compressed NAR not available: {}.nar{}",
                                hash, extension
                            )));
                        }
                        _ => {
                            // Propagate other storage errors (IO failures, etc.) as 500
                            return Err(e.into());
                        }
                    }
                }
            }
        }
    }

    // Serve uncompressed NAR by reconstructing from chunks.
    // Use verified query to ensure all chunks exist in metadata before streaming,
    // returning 404 if any chunks are missing instead of truncated data.
    let domain_id = store_path.domain_id;
    let chunk_hashes = state
        .metadata
        .get_manifest_chunks_verified(domain_id, &store_path.manifest_hash)
        .await?;

    // CHUNK VERIFICATION: Use the chunks_verified flag to avoid truncated responses.
    // If chunks_verified is true (set during upload after all chunks confirmed), we can
    // skip the preflight check since the NAR is known to be complete.
    // If chunks_verified is false (legacy data or failed uploads), fall back to the
    // preflight check which verifies at least the first chunk exists.
    if !store_path.chunks_verified {
        // PREFLIGHT CHECK: Verify first chunk exists in storage before sending 200 response.
        // This catches the common case where metadata references chunks that don't exist.
        // Without this check, we'd send 200 + Content-Length headers, then fail mid-stream,
        // causing client hangs or truncated data.
        //
        // LIMITATION: For unverified paths, we only check the first chunk for efficiency.
        // Missing later chunks will still cause truncated responses. This is a fallback
        // for legacy data uploaded before chunks_verified was introduced.
        if let Some(first_hash) = chunk_hashes.first() {
            let first_key = format!(
                "domains/{}/chunks/{}/{}/{}",
                domain_id,
                &first_hash[..2],
                &first_hash[2..4],
                first_hash
            );
            if !state.storage.exists(&first_key).await? {
                return Err(ApiError::NotFound(format!(
                    "NAR chunk {} missing from storage",
                    first_hash
                )));
            }
        }
    }

    // Create a stream that yields chunk data in order.
    let nar_size = store_path.nar_size;
    let storage = state.storage.clone();
    let stream = futures::stream::iter(chunk_hashes)
        .then(move |chunk_hash| {
            let storage = storage.clone();
            async move {
                let key = format!(
                    "domains/{}/chunks/{}/{}/{}",
                    domain_id,
                    &chunk_hash[..2],
                    &chunk_hash[2..4],
                    &chunk_hash
                );
                match storage.get(&key).await {
                    Ok(data) => Ok(data),
                    Err(e) => {
                        tracing::error!(
                            chunk_hash = %chunk_hash,
                            error = %e,
                            "NAR streaming failed mid-transfer"
                        );
                        Err(e)
                    }
                }
            }
        })
        .map(|result| result.map_err(|e| std::io::Error::other(e.to_string())));

    let body = Body::from_stream(stream);

    Ok((
        StatusCode::OK,
        [
            (CONTENT_TYPE, "application/x-nix-nar"),
            (CONTENT_LENGTH, &nar_size.to_string()),
        ],
        body,
    )
        .into_response())
}

/// Parse NAR path to extract hash and compression type.
fn parse_nar_path(path: &str) -> (String, Option<cellar_core::narinfo::Compression>) {
    // Try to match known extensions
    for (ext, compression) in [
        (".nar.zst", cellar_core::narinfo::Compression::Zstd),
        (".nar.xz", cellar_core::narinfo::Compression::Xz),
        (".nar.gz", cellar_core::narinfo::Compression::Gzip),
        (".nar.bz2", cellar_core::narinfo::Compression::Bzip2),
    ] {
        if let Some(hash) = path.strip_suffix(ext) {
            return (hash.to_string(), Some(compression));
        }
    }

    // Uncompressed or unknown extension
    let hash = path.strip_suffix(".nar").unwrap_or(path);
    (hash.to_string(), None)
}
