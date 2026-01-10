//! Nix compatibility endpoints (read path).

use crate::auth::require_auth;
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use cellar_core::token::TokenScope;
use futures::StreamExt;
use serde::Serialize;
use uuid::Uuid;

/// Extract cache_id from request context and verify CacheRead scope.
/// For authenticated requests, verifies the token has CacheRead scope and returns the cache_id.
/// Returns an error if the token lacks the required scope.
fn require_read_access(req: &Request) -> ApiResult<Option<Uuid>> {
    let auth = require_auth(req)?;
    auth.require_scope(TokenScope::CacheRead)?;
    Ok(auth.token.cache_id)
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
pub async fn narinfo_fallback(State(state): State<AppState>, req: Request) -> Response {
    let path = req.uri().path().to_string();

    // Verify CacheRead scope before processing
    let cache_id = match require_read_access(&req) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

    // Check if this is a narinfo request
    if let Some(hash) = path.strip_prefix('/').and_then(|p| p.strip_suffix(".narinfo")) {
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
pub async fn get_narinfo(
    State(state): State<AppState>,
    Path(store_path_hash): Path<String>,
    req: Request,
) -> ApiResult<Response> {
    let cache_id = require_read_access(&req)?;
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
        return Err(ApiError::NotFound(format!(
            "store path not found: {hash}"
        )));
    }

    // Get narinfo from storage
    let narinfo_key = format!("narinfo/{}.narinfo", hash);
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
pub async fn get_nar(
    State(state): State<AppState>,
    Path(nar_path): Path<String>,
    req: Request,
) -> ApiResult<Response> {
    let cache_id = require_read_access(&req)?;

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
            let nar_key = format!("nar/{}.nar{}", hash, extension);

            // Use streaming to avoid loading entire compressed file into memory.
            // First get metadata for Content-Length header.
            match state.storage.head(&nar_key).await {
                Ok(meta) => {
                    // Stream directly from storage to HTTP response
                    let stream = state.storage.get_stream(&nar_key).await?;
                    let body_stream = stream.map(|result| {
                        result.map_err(|e| {
                            std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
                        })
                    });

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

    // Serve uncompressed NAR by reconstructing from chunks
    let chunk_hashes = state
        .metadata
        .get_manifest_chunks(&store_path.manifest_hash)
        .await?;

    // Create a stream that yields chunk data in order
    let storage = state.storage.clone();
    let stream = futures::stream::iter(chunk_hashes)
        .then(move |chunk_hash| {
            let storage = storage.clone();
            async move {
                let key = format!(
                    "chunks/{}/{}/{}",
                    &chunk_hash[..2],
                    &chunk_hash[2..4],
                    chunk_hash
                );
                storage.get(&key).await
            }
        })
        .map(|result| {
            result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        });

    let body = Body::from_stream(stream);

    Ok((
        StatusCode::OK,
        [
            (CONTENT_TYPE, "application/x-nix-nar"),
            (CONTENT_LENGTH, &store_path.nar_size.to_string()),
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
