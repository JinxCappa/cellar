//! Capability discovery endpoint.

use crate::error::ApiResult;
use crate::state::AppState;
use axum::Json;
use axum::extract::State;
use serde::Serialize;

/// Capabilities response.
#[derive(Debug, Serialize)]
pub struct CapabilitiesResponse {
    /// Maximum chunk size in bytes.
    pub max_chunk_size: u64,
    /// Default chunk size in bytes.
    pub default_chunk_size: u64,
    /// Whether resume is supported.
    pub supports_resume: bool,
    /// Recommended parallel upload count.
    pub recommended_parallelism: u32,
    /// API version.
    pub api_version: &'static str,
}

/// GET /v1/capabilities
pub async fn get_capabilities(
    State(state): State<AppState>,
) -> ApiResult<Json<CapabilitiesResponse>> {
    Ok(Json(CapabilitiesResponse {
        max_chunk_size: state.config.server.max_chunk_size,
        default_chunk_size: state.config.server.default_chunk_size,
        supports_resume: true,
        recommended_parallelism: state.config.server.max_parallel_chunks,
        api_version: "v1",
    }))
}
