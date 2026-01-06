//! Route configuration.

use crate::auth::auth_middleware;
use crate::handlers;
use crate::metrics::metrics_handler;
use crate::ratelimit::{ip_rate_limit_middleware, token_rate_limit_middleware};
use crate::state::AppState;
use axum::Router;
use axum::middleware;
use axum::routing::{delete, get, post, put};
use tower_http::trace::TraceLayer;

/// Create the application router.
pub fn create_router(state: AppState) -> Router {
    let api_routes = Router::new()
        // Capability discovery
        .route("/v1/capabilities", get(handlers::get_capabilities))
        // Auth discovery
        .route("/v1/auth/whoami", get(handlers::whoami))
        // Health check (intentionally unauthenticated for load balancers/k8s probes)
        .route("/v1/health", get(handlers::health_check))
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
        // Admin endpoints (all require CacheAdmin scope)
        .route("/v1/admin/metrics", get(handlers::get_metrics))
        .route(
            "/v1/admin/tokens",
            post(handlers::create_token).get(handlers::list_tokens),
        )
        .route(
            "/v1/admin/tokens/{token_id}",
            delete(handlers::revoke_token),
        )
        .route(
            "/v1/admin/gc",
            get(handlers::list_gc_jobs).post(handlers::trigger_gc),
        )
        .route("/v1/admin/gc/{job_id}", get(handlers::get_gc_job))
        // Domain management endpoints
        .route(
            "/v1/admin/domains",
            post(handlers::create_domain).get(handlers::list_domains),
        )
        .route(
            "/v1/admin/domains/{domain_id}",
            get(handlers::get_domain)
                .put(handlers::update_domain)
                .delete(handlers::delete_domain),
        )
        .route(
            "/v1/admin/domains/by-name/{domain_name}",
            get(handlers::get_domain_by_name),
        )
        // Cache management endpoints
        .route("/v1/admin/caches", post(handlers::create_cache))
        .route("/v1/admin/caches", get(handlers::list_caches))
        .route("/v1/admin/caches/{cache_id}", get(handlers::get_cache))
        .route("/v1/admin/caches/{cache_id}", put(handlers::update_cache))
        .route(
            "/v1/admin/caches/{cache_id}",
            delete(handlers::delete_cache),
        );

    let nix_routes = Router::new()
        // Nix compatibility endpoints
        .route("/nix-cache-info", get(handlers::get_nix_cache_info))
        .route("/nar/{nar_path}", get(handlers::get_nar))
        // Narinfo routes use a fallback handler since axum doesn't support /{param}.suffix
        .fallback(handlers::narinfo_fallback);

    // Build base router with API and Nix routes
    let mut router = Router::new().merge(api_routes).merge(nix_routes);

    // Conditionally add metrics endpoint based on config.
    // SECURITY: When enabled, this endpoint MUST be network-restricted
    // to authorized Prometheus scraper IPs only.
    // See crate::metrics module documentation for details.
    if state.config.server.metrics_enabled {
        let metrics_routes = Router::new().route("/metrics", get(metrics_handler));
        router = router.merge(metrics_routes);
    }

    // Extract rate limit state for the middleware
    let rate_limit_state = state.rate_limit.clone();

    // Middleware layers are applied in reverse order (outermost first).
    // Order of execution: TraceLayer -> IP rate limit -> Auth -> Token rate limit -> Handler
    router
        // Per-token rate limiting (innermost layer after auth, runs after auth sets token info)
        .layer(middleware::from_fn_with_state(
            rate_limit_state.clone(),
            token_rate_limit_middleware,
        ))
        // Auth middleware (validates token and sets AuthenticatedUser extension)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        // Per-IP rate limiting (runs before auth, catches unauthenticated abuse)
        .layer(middleware::from_fn_with_state(
            rate_limit_state,
            ip_rate_limit_middleware,
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
