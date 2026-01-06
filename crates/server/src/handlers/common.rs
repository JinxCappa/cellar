//! Shared handler helpers.

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use cellar_core::storage_domain::default_storage_domain_id;
use uuid::Uuid;

/// Resolve cache_id for upload operations.
/// Global tokens (cache_id = None) are resolved to the default public cache.
/// Returns an error if no default public cache is configured.
pub async fn resolve_upload_cache_id(
    state: &AppState,
    cache_id: Option<Uuid>,
) -> ApiResult<Option<Uuid>> {
    match cache_id {
        Some(_) => Ok(cache_id),
        None => {
            let cache = state
                .metadata
                .get_default_public_cache()
                .await?
                .ok_or_else(|| {
                    ApiError::BadRequest(
                        "global token requires a default public cache to be configured".to_string(),
                    )
                })?;
            Ok(Some(cache.cache_id))
        }
    }
}

pub async fn resolve_domain_id(state: &AppState, cache_id: Option<Uuid>) -> ApiResult<Uuid> {
    if let Some(cache_id) = cache_id {
        let cache = state
            .metadata
            .get_cache(cache_id)
            .await?
            .ok_or_else(|| ApiError::NotFound("cache not found".to_string()))?;
        Ok(cache.domain_id)
    } else {
        Ok(default_storage_domain_id())
    }
}
