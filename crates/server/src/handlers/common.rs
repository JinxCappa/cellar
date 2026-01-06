//! Shared handler helpers.

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use cellar_core::storage_domain::default_storage_domain_id;
use uuid::Uuid;

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
