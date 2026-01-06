//! Rate limiting middleware using token bucket algorithm.
//!
//! Provides dual-layer rate limiting:
//! - Per-IP limiting for all requests (applied first)
//! - Per-token limiting for authenticated requests (applied after auth)
//!
//! # Security Note
//!
//! By default, X-Forwarded-For and X-Real-IP headers are NOT trusted to prevent
//! IP spoofing attacks. You must explicitly configure `trusted_proxies` to enable
//! header-based IP detection:
//!
//! - Empty list (default): Only direct connection IP is used (most secure)
//! - List of IPs/CIDRs: Headers trusted only when request comes from these IPs
//! - ["*"]: Trust headers from all sources (NOT recommended for production)

use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use governor::{
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::InMemoryState,
    Quota, RateLimiter,
};
use cellar_core::config::RateLimitConfig;
use ipnet::IpNet;
use std::{net::{IpAddr, SocketAddr}, num::NonZeroU32, sync::Arc};

/// Type alias for the keyed rate limiter (per-IP or per-token).
type KeyedLimiter = RateLimiter<String, dashmap::DashMap<String, InMemoryState>, DefaultClock, NoOpMiddleware>;

/// Rate limiter state shared across requests.
#[derive(Clone)]
pub struct RateLimitState {
    /// Per-IP rate limiter.
    ip_limiter: Option<Arc<KeyedLimiter>>,
    /// Per-token rate limiter.
    token_limiter: Option<Arc<KeyedLimiter>>,
    /// Whether rate limiting is enabled.
    enabled: bool,
    /// Trusted proxy configuration.
    trusted_proxies: TrustedProxies,
}

/// A parsed trusted proxy entry (either an IP or CIDR range).
#[derive(Clone, Debug)]
enum TrustedEntry {
    Ip(IpAddr),
    Cidr(IpNet),
}

/// Trusted proxy configuration for IP extraction.
#[derive(Clone, Debug)]
enum TrustedProxies {
    /// Never trust forwarded headers (default, most secure).
    None,
    /// Trust headers from all sources (dangerous, for development only).
    All,
    /// Trust headers only from specific IPs/CIDRs.
    List(Vec<TrustedEntry>),
}

impl TrustedProxies {
    fn from_config(proxies: &[String]) -> Self {
        if proxies.is_empty() {
            Self::None
        } else if proxies.len() == 1 && proxies[0] == "*" {
            Self::All
        } else {
            let entries: Vec<TrustedEntry> = proxies
                .iter()
                .filter_map(|p| {
                    if p.contains('/') {
                        // Parse as CIDR
                        match p.parse::<IpNet>() {
                            Ok(net) => Some(TrustedEntry::Cidr(net)),
                            Err(e) => {
                                tracing::warn!("Invalid CIDR in trusted_proxies: '{}': {}", p, e);
                                None
                            }
                        }
                    } else {
                        // Parse as single IP
                        match p.parse::<IpAddr>() {
                            Ok(ip) => Some(TrustedEntry::Ip(ip)),
                            Err(e) => {
                                tracing::warn!("Invalid IP in trusted_proxies: '{}': {}", p, e);
                                None
                            }
                        }
                    }
                })
                .collect();
            Self::List(entries)
        }
    }

    /// Check if the given connection IP is a trusted proxy.
    fn is_trusted(&self, connection_ip: &str) -> bool {
        match self {
            Self::None => false,
            Self::All => true,
            Self::List(entries) => {
                let ip: IpAddr = match connection_ip.parse() {
                    Ok(ip) => ip,
                    Err(_) => return false,
                };
                entries.iter().any(|entry| match entry {
                    TrustedEntry::Ip(trusted) => *trusted == ip,
                    TrustedEntry::Cidr(network) => network.contains(&ip),
                })
            }
        }
    }
}

impl RateLimitState {
    /// Create a new rate limit state from configuration.
    pub fn new(config: &RateLimitConfig) -> Self {
        let trusted_proxies = TrustedProxies::from_config(&config.trusted_proxies);

        if !config.enabled {
            return Self {
                ip_limiter: None,
                token_limiter: None,
                enabled: false,
                trusted_proxies,
            };
        }

        // Per-IP limiter
        let ip_quota = Quota::per_minute(
            NonZeroU32::new(config.ip_requests_per_minute).unwrap_or(NonZeroU32::new(60).unwrap()),
        )
        .allow_burst(
            NonZeroU32::new(config.burst_size).unwrap_or(NonZeroU32::new(1).unwrap()),
        );
        let ip_limiter = Arc::new(RateLimiter::dashmap(ip_quota));

        // Per-token limiter (higher limits for authenticated requests)
        let token_quota = Quota::per_minute(
            NonZeroU32::new(config.token_requests_per_minute).unwrap_or(NonZeroU32::new(600).unwrap()),
        )
        .allow_burst(
            NonZeroU32::new(config.burst_size * 2).unwrap_or(NonZeroU32::new(1).unwrap()),
        );
        let token_limiter = Arc::new(RateLimiter::dashmap(token_quota));

        Self {
            ip_limiter: Some(ip_limiter),
            token_limiter: Some(token_limiter),
            enabled: true,
            trusted_proxies,
        }
    }

    /// Check if a request from the given IP is allowed.
    pub fn check_ip(&self, ip: &str) -> Result<(), RateLimitError> {
        if let Some(limiter) = &self.ip_limiter {
            match limiter.check_key(&ip.to_string()) {
                Ok(_) => Ok(()),
                Err(not_until) => {
                    let wait_time = not_until.wait_time_from(governor::clock::Clock::now(&DefaultClock::default()));
                    Err(RateLimitError {
                        retry_after_secs: wait_time.as_secs() + 1,
                    })
                }
            }
        } else {
            Ok(())
        }
    }

    /// Check if a request from the given token is allowed.
    pub fn check_token(&self, token_id: &str) -> Result<(), RateLimitError> {
        if let Some(limiter) = &self.token_limiter {
            match limiter.check_key(&token_id.to_string()) {
                Ok(_) => Ok(()),
                Err(not_until) => {
                    let wait_time = not_until.wait_time_from(governor::clock::Clock::now(&DefaultClock::default()));
                    Err(RateLimitError {
                        retry_after_secs: wait_time.as_secs() + 1,
                    })
                }
            }
        } else {
            Ok(())
        }
    }

    /// Check if rate limiting is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// Error returned when rate limit is exceeded.
#[derive(Debug)]
pub struct RateLimitError {
    /// Number of seconds to wait before retrying.
    pub retry_after_secs: u64,
}

impl IntoResponse for RateLimitError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({
            "code": "rate_limit_exceeded",
            "message": format!("Rate limit exceeded. Retry after {} seconds.", self.retry_after_secs),
            "retry_after": self.retry_after_secs,
        });

        (
            StatusCode::TOO_MANY_REQUESTS,
            [("Retry-After", self.retry_after_secs.to_string())],
            axum::Json(body),
        )
            .into_response()
    }
}

/// Extract client IP address from request headers (only if trusted).
fn extract_forwarded_ip(req: &Request<Body>) -> Option<String> {
    // Try to get IP from X-Forwarded-For header first (for reverse proxy setups)
    if let Some(forwarded) = req.headers().get("x-forwarded-for") {
        if let Ok(s) = forwarded.to_str() {
            // Take the first IP in the chain (client IP)
            if let Some(ip) = s.split(',').next() {
                return Some(ip.trim().to_string());
            }
        }
    }

    // Try X-Real-IP header
    if let Some(real_ip) = req.headers().get("x-real-ip") {
        if let Ok(s) = real_ip.to_str() {
            return Some(s.trim().to_string());
        }
    }

    None
}

/// Extract connection IP from request extensions (set by ConnectInfo).
fn extract_connection_ip(req: &Request<Body>) -> Option<String> {
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip().to_string())
}

/// Extract client IP address from request.
///
/// # Security
///
/// This function respects the trusted_proxies configuration:
/// - If no proxies are trusted, always uses the direct connection IP
/// - If specific proxies are trusted, only reads forwarded headers when
///   the connection comes from a trusted proxy IP
/// - If all proxies are trusted ("*"), always reads forwarded headers (unsafe)
fn extract_ip(req: &Request<Body>, trusted_proxies: &TrustedProxies) -> String {
    let connection_ip = extract_connection_ip(req);

    // Check if we should trust forwarded headers
    let trust_headers = match (&connection_ip, trusted_proxies) {
        // No connection info available and proxies configured - trust headers
        // (This happens when ConnectInfo isn't set up in the router)
        (None, TrustedProxies::All) => true,
        (None, TrustedProxies::List(_)) => {
            // Can't verify proxy without connection IP, default to not trusting
            tracing::warn!(
                "Cannot verify trusted proxy: ConnectInfo not available. \
                 Add .into_make_service_with_connect_info::<SocketAddr>() to your server."
            );
            false
        }
        (None, TrustedProxies::None) => false,
        // Have connection info - check if it's from a trusted proxy
        (Some(conn_ip), trusted_proxies) => trusted_proxies.is_trusted(conn_ip),
    };

    if trust_headers {
        if let Some(forwarded_ip) = extract_forwarded_ip(req) {
            return forwarded_ip;
        }
    }

    // Use connection IP if available, otherwise "unknown"
    connection_ip.unwrap_or_else(|| "unknown".to_string())
}

/// Per-IP rate limiting middleware.
///
/// This middleware checks rate limits based on client IP address.
/// It should be applied as an outer layer, before authentication.
///
/// # Security Note
///
/// By default, X-Forwarded-For and X-Real-IP headers are NOT trusted.
/// Configure `trusted_proxies` in the rate_limit config to enable
/// forwarded header parsing when behind a reverse proxy.
pub async fn ip_rate_limit_middleware(
    State(rate_limit): State<RateLimitState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    if !rate_limit.is_enabled() {
        return next.run(req).await;
    }

    let ip = extract_ip(&req, &rate_limit.trusted_proxies);

    match rate_limit.check_ip(&ip) {
        Ok(_) => next.run(req).await,
        Err(e) => e.into_response(),
    }
}

/// Per-token rate limiting middleware.
///
/// This middleware checks rate limits based on authenticated token.
/// It should be applied after authentication middleware.
/// If the request is not authenticated, it falls through without checking.
pub async fn token_rate_limit_middleware(
    State(rate_limit): State<RateLimitState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    if !rate_limit.is_enabled() {
        return next.run(req).await;
    }

    // Check if request has authentication info
    // The auth middleware stores the token ID in request extensions
    if let Some(token_id) = req.extensions().get::<TokenIdExtension>() {
        match rate_limit.check_token(&token_id.0) {
            Ok(_) => next.run(req).await,
            Err(e) => e.into_response(),
        }
    } else {
        // Not authenticated, let IP rate limit handle it
        next.run(req).await
    }
}

/// Extension to store token ID for rate limiting.
#[derive(Clone)]
pub struct TokenIdExtension(pub String);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_state_disabled() {
        let config = RateLimitConfig {
            enabled: false,
            ..Default::default()
        };
        let state = RateLimitState::new(&config);
        assert!(!state.is_enabled());
        assert!(state.check_ip("127.0.0.1").is_ok());
        assert!(state.check_token("test-token").is_ok());
    }

    #[test]
    fn test_rate_limit_state_enabled() {
        let config = RateLimitConfig {
            enabled: true,
            ip_requests_per_minute: 60, // 1 per second
            token_requests_per_minute: 120,
            burst_size: 5,
            ..Default::default()
        };
        let state = RateLimitState::new(&config);
        assert!(state.is_enabled());

        // First burst of requests should succeed (up to burst_size)
        for _ in 0..5 {
            assert!(state.check_ip("127.0.0.1").is_ok());
        }

        // Additional requests should be rate limited
        let result = state.check_ip("127.0.0.1");
        assert!(result.is_err(), "Should be rate limited after burst is exhausted");

        // Different IP should have its own limit
        assert!(state.check_ip("192.168.1.1").is_ok());
    }
}
