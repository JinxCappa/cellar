//! Route configuration.

use crate::auth::auth_middleware;
use crate::handlers;
use crate::metrics::metrics_handler;
use crate::ratelimit::{ip_rate_limit_middleware, token_rate_limit_middleware};
use crate::state::AppState;
use axum::middleware;
use axum::routing::{delete, get, post, put};
use axum::Router;
use tower_http::trace::TraceLayer;

/// Create the application router.
pub fn create_router(state: AppState) -> Router {
    let api_routes = Router::new()
        // Capability discovery
        .route("/v1/capabilities", get(handlers::get_capabilities))
        // Upload control plane
        .route("/v1/uploads", post(handlers::create_upload))
        .route("/v1/uploads/{upload_id}", get(handlers::get_upload))
        .route(
            "/v1/uploads/{upload_id}/chunks/{chunk_hash}",
            put(handlers::upload_chunk),
        )
        .route(
            "/v1/uploads/{upload_id}/commit",
            post(handlers::commit_upload),
        )
        // Admin endpoints
        .route("/v1/admin/health", get(handlers::health_check))
        .route("/v1/admin/metrics", get(handlers::get_metrics))
        .route("/v1/admin/tokens", post(handlers::create_token))
        .route("/v1/admin/tokens/{token_id}", delete(handlers::revoke_token))
        .route("/v1/admin/gc", post(handlers::trigger_gc))
        .route("/v1/admin/gc/{job_id}", get(handlers::get_gc_job));

    let nix_routes = Router::new()
        // Nix compatibility endpoints
        .route("/nix-cache-info", get(handlers::get_nix_cache_info))
        .route("/nar/{nar_path}", get(handlers::get_nar))
        // Narinfo routes use a fallback handler since axum doesn't support /{param}.suffix
        .fallback(handlers::narinfo_fallback);

    // Metrics endpoint (no auth required for Prometheus scraping).
    // SECURITY: This endpoint MUST be network-restricted to Prometheus IPs only.
    // See crate::metrics module documentation for details.
    let metrics_routes = Router::new().route("/metrics", get(metrics_handler));

    // Extract rate limit state for the middleware
    let rate_limit_state = state.rate_limit.clone();

    // Middleware layers are applied in reverse order (outermost first).
    // Order of execution: TraceLayer -> IP rate limit -> Auth -> Token rate limit -> Handler
    Router::new()
        .merge(api_routes)
        .merge(nix_routes)
        .merge(metrics_routes)
        // Per-token rate limiting (innermost layer after auth, runs after auth sets token info)
        .layer(middleware::from_fn_with_state(rate_limit_state.clone(), token_rate_limit_middleware))
        // Auth middleware (validates token and sets AuthenticatedUser extension)
        .layer(middleware::from_fn_with_state(state.clone(), auth_middleware))
        // Per-IP rate limiting (runs before auth, catches unauthenticated abuse)
        .layer(middleware::from_fn_with_state(rate_limit_state, ip_rate_limit_middleware))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
