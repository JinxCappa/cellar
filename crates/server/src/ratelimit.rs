//! Rate limiting middleware using token bucket algorithm.
//!
//! Provides dual-layer rate limiting:
//! - Per-IP limiting for all requests (applied first)
//! - Per-token limiting for authenticated requests (applied after auth)
//!
//! # Memory Safety
//!
//! This implementation includes protection against memory exhaustion attacks:
//! - Configurable maximum entries (default: 100,000)
//! - Automatic eviction of stale entries based on TTL
//! - Background cleanup task that runs periodically
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
use cellar_core::config::RateLimitConfig;
use dashmap::{DashMap, mapref::entry::Entry};
use governor::{
    Quota, RateLimiter, clock::DefaultClock, middleware::NoOpMiddleware, state::InMemoryState,
};
use ipnet::IpNet;
use std::{
    net::{IpAddr, SocketAddr},
    num::NonZeroU32,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

/// Type alias for the keyed rate limiter (per-IP or per-token).
type KeyedLimiter =
    RateLimiter<String, DashMap<String, InMemoryState>, DefaultClock, NoOpMiddleware>;

/// Rate limiter state shared across requests.
#[derive(Clone)]
pub struct RateLimitState {
    inner: Option<Arc<RateLimitStateInner>>,
}

/// Minimum eviction threshold as a fraction of current entries before triggering rebuild.
/// This prevents rebuilding (which resets rate-limit state) for small cleanups.
const REBUILD_EVICTION_THRESHOLD_FRACTION: f64 = 0.10; // 10% of entries

/// Minimum number of evictions to trigger a rebuild regardless of fraction.
const REBUILD_EVICTION_MIN_COUNT: usize = 100;

/// Minimum interval between rebuilds. Even if eviction threshold is not met,
/// we'll rebuild after this interval to eventually reclaim memory.
const REBUILD_MIN_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes

/// Inner state that's only allocated when rate limiting is enabled.
struct RateLimitStateInner {
    /// Per-IP rate limiter (wrapped in RwLock to allow rebuilding for memory cleanup).
    ip_limiter: RwLock<KeyedLimiter>,
    /// Per-token rate limiter (wrapped in RwLock to allow rebuilding for memory cleanup).
    token_limiter: RwLock<KeyedLimiter>,
    /// Last access timestamps for IP limiter entries (for eviction).
    ip_last_access: DashMap<String, Instant>,
    /// Last access timestamps for token limiter entries (for eviction).
    token_last_access: DashMap<String, Instant>,
    /// Trusted proxy configuration.
    trusted_proxies: TrustedProxies,
    /// Maximum entries before rejecting new keys.
    max_entries: u32,
    /// Time-to-live for entries.
    entry_ttl: Duration,
    /// Whether ConnectInfo missing warning has been logged.
    connect_info_warned: AtomicBool,
    /// Whether IP limiter at-capacity warning has been logged (prevents log spam during DoS).
    at_capacity_warned_ip: AtomicBool,
    /// Whether token limiter at-capacity warning has been logged (prevents log spam during DoS).
    at_capacity_warned_token: AtomicBool,
    /// Quota configuration for IP limiter (needed for rebuilding).
    ip_quota: Quota,
    /// Quota configuration for token limiter (needed for rebuilding).
    token_quota: Quota,
    /// Timestamp of last IP limiter rebuild.
    last_ip_rebuild: RwLock<Instant>,
    /// Timestamp of last token limiter rebuild.
    last_token_rebuild: RwLock<Instant>,
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
            return Self { inner: None };
        }

        // Per-IP limiter
        let ip_quota = Quota::per_minute(
            NonZeroU32::new(config.ip_requests_per_minute).unwrap_or(NonZeroU32::new(60).unwrap()),
        )
        .allow_burst(NonZeroU32::new(config.burst_size).unwrap_or(NonZeroU32::new(1).unwrap()));
        let ip_limiter = RateLimiter::dashmap(ip_quota);

        // Per-token limiter (higher limits for authenticated requests)
        let token_quota = Quota::per_minute(
            NonZeroU32::new(config.token_requests_per_minute)
                .unwrap_or(NonZeroU32::new(600).unwrap()),
        )
        .allow_burst(
            NonZeroU32::new(config.burst_size.saturating_mul(2))
                .unwrap_or(NonZeroU32::new(1).unwrap()),
        );
        let token_limiter = RateLimiter::dashmap(token_quota);

        let now = Instant::now();
        Self {
            inner: Some(Arc::new(RateLimitStateInner {
                ip_limiter: RwLock::new(ip_limiter),
                token_limiter: RwLock::new(token_limiter),
                ip_last_access: DashMap::new(),
                token_last_access: DashMap::new(),
                trusted_proxies,
                max_entries: config.max_entries,
                entry_ttl: Duration::from_secs(config.entry_ttl_secs),
                connect_info_warned: AtomicBool::new(false),
                at_capacity_warned_ip: AtomicBool::new(false),
                at_capacity_warned_token: AtomicBool::new(false),
                ip_quota,
                token_quota,
                last_ip_rebuild: RwLock::new(now),
                last_token_rebuild: RwLock::new(now),
            })),
        }
    }

    /// Check if a request from the given IP is allowed.
    pub fn check_ip(&self, ip: &str) -> Result<(), RateLimitError> {
        let inner = match &self.inner {
            Some(inner) => inner,
            None => return Ok(()),
        };

        let now = Instant::now();
        let ip_string = ip.to_string();

        // Check capacity before acquiring entry lock to avoid deadlock.
        // DashMap's len() can deadlock if called while holding an entry lock.
        // This check is slightly racy (another thread could insert between check and entry),
        // but the entry API ensures we don't have duplicate insertions, and the worst case
        // is we slightly exceed max_entries temporarily (by at most num_concurrent_threads).
        let current_len = inner.ip_last_access.len();
        let at_capacity = current_len >= inner.max_entries as usize;

        // Use entry API for atomic check-and-insert to avoid TOCTOU race on key existence.
        match inner.ip_last_access.entry(ip_string.clone()) {
            Entry::Occupied(mut entry) => {
                // Existing entry - just update timestamp
                entry.insert(now);
            }
            Entry::Vacant(entry) => {
                // New entry - check if we're at capacity
                if at_capacity {
                    // Don't insert - we're at capacity
                    Self::warn_at_capacity(
                        &inner.at_capacity_warned_ip,
                        "IP",
                        current_len,
                        inner.max_entries,
                    );
                    return Err(RateLimitError {
                        retry_after_secs: 60, // Suggest retry after cleanup
                        reason: RateLimitReason::AtCapacity,
                    });
                }
                entry.insert(now);
            }
        }

        // Check rate limit (acquire read lock for governor access)
        let ip_limiter = inner.ip_limiter.read().unwrap_or_else(|poisoned| {
            tracing::warn!("inner.ip_limiter RwLock was poisoned, recovering with into_inner()");
            poisoned.into_inner()
        });
        match ip_limiter.check_key(&ip_string) {
            Ok(_) => Ok(()),
            Err(not_until) => {
                let wait_time =
                    not_until.wait_time_from(governor::clock::Clock::now(&DefaultClock::default()));
                Err(RateLimitError {
                    retry_after_secs: wait_time.as_secs() + 1,
                    reason: RateLimitReason::RateLimited,
                })
            }
        }
    }

    /// Check if a request from the given token is allowed.
    pub fn check_token(&self, token_id: &str) -> Result<(), RateLimitError> {
        let inner = match &self.inner {
            Some(inner) => inner,
            None => return Ok(()),
        };

        let now = Instant::now();
        let token_string = token_id.to_string();

        // Check capacity before acquiring entry lock to avoid deadlock.
        // DashMap's len() can deadlock if called while holding an entry lock.
        let current_len = inner.token_last_access.len();
        let at_capacity = current_len >= inner.max_entries as usize;

        // Use entry API for atomic check-and-insert to avoid TOCTOU race on key existence.
        match inner.token_last_access.entry(token_string.clone()) {
            Entry::Occupied(mut entry) => {
                // Existing entry - just update timestamp
                entry.insert(now);
            }
            Entry::Vacant(entry) => {
                // New entry - check if we're at capacity
                if at_capacity {
                    // Don't insert - we're at capacity
                    Self::warn_at_capacity(
                        &inner.at_capacity_warned_token,
                        "token",
                        current_len,
                        inner.max_entries,
                    );
                    return Err(RateLimitError {
                        retry_after_secs: 60,
                        reason: RateLimitReason::AtCapacity,
                    });
                }
                entry.insert(now);
            }
        }

        // Check rate limit (acquire read lock for governor access)
        let token_limiter = inner.token_limiter.read().unwrap_or_else(|poisoned| {
            tracing::warn!("inner.token_limiter RwLock was poisoned, recovering with into_inner()");
            poisoned.into_inner()
        });
        match token_limiter.check_key(&token_string) {
            Ok(_) => Ok(()),
            Err(not_until) => {
                let wait_time =
                    not_until.wait_time_from(governor::clock::Clock::now(&DefaultClock::default()));
                Err(RateLimitError {
                    retry_after_secs: wait_time.as_secs() + 1,
                    reason: RateLimitReason::RateLimited,
                })
            }
        }
    }

    /// Check if rate limiting is enabled.
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Clean up stale entries from both limiters.
    /// Returns the number of entries evicted.
    ///
    /// This method uses atomic `remove_if` to prevent race conditions where a
    /// freshly-accessed entry could be incorrectly evicted.
    ///
    /// When entries are evicted, the governor rate limiters are rebuilt to reclaim
    /// memory. Governor's internal DashMap doesn't support key removal, so we must
    /// rebuild the entire limiter to prevent unbounded memory growth.
    pub fn cleanup(&self) -> usize {
        let inner = match &self.inner {
            Some(inner) => inner,
            None => return 0,
        };

        let now = Instant::now();
        let ttl = inner.entry_ttl;
        let mut ip_evicted = 0;
        let mut token_evicted = 0;

        // Collect keys to evict from IP limiter, then use remove_if for atomic removal.
        // This prevents the race where an entry is accessed between collection and removal.
        let ip_keys_to_check: Vec<String> = inner
            .ip_last_access
            .iter()
            .filter(|entry| now.duration_since(*entry.value()) > ttl)
            .map(|entry| entry.key().clone())
            .collect();

        for key in ip_keys_to_check {
            // Atomically remove only if the entry is still stale.
            // If another thread updated the timestamp, this will not remove.
            if inner
                .ip_last_access
                .remove_if(&key, |_, last_access| {
                    now.duration_since(*last_access) > ttl
                })
                .is_some()
            {
                ip_evicted += 1;
            }
        }

        // Collect keys to evict from token limiter
        let token_keys_to_check: Vec<String> = inner
            .token_last_access
            .iter()
            .filter(|entry| now.duration_since(*entry.value()) > ttl)
            .map(|entry| entry.key().clone())
            .collect();

        for key in token_keys_to_check {
            // Atomically remove only if the entry is still stale.
            if inner
                .token_last_access
                .remove_if(&key, |_, last_access| {
                    now.duration_since(*last_access) > ttl
                })
                .is_some()
            {
                token_evicted += 1;
            }
        }

        let total_evicted = ip_evicted + token_evicted;

        // Rebuild governor rate limiters if we evicted enough entries to justify it.
        // Governor's internal DashMap doesn't support key removal, so without this
        // rebuild, memory would grow unbounded as new keys are added over time.
        //
        // However, rebuilding resets rate-limit state for all active clients, which
        // weakens throttling. We gate rebuilds to avoid this on small cleanups:
        // - Only rebuild if evictions exceed threshold (fraction of entries or min count)
        // - OR if minimum interval has passed since last rebuild (fallback for memory)
        if ip_evicted > 0 {
            let ip_entries_before = inner.ip_last_access.len() + ip_evicted;
            if self.should_rebuild(ip_evicted, ip_entries_before, &inner.last_ip_rebuild, now) {
                self.rebuild_ip_limiter(inner);
                tracing::debug!(
                    evicted = ip_evicted,
                    remaining = inner.ip_last_access.len(),
                    "Rebuilt IP rate limiter after cleanup"
                );
            } else {
                tracing::trace!(
                    evicted = ip_evicted,
                    remaining = inner.ip_last_access.len(),
                    "Skipped IP rate limiter rebuild (below threshold)"
                );
            }
        }

        if token_evicted > 0 {
            let token_entries_before = inner.token_last_access.len() + token_evicted;
            if self.should_rebuild(
                token_evicted,
                token_entries_before,
                &inner.last_token_rebuild,
                now,
            ) {
                self.rebuild_token_limiter(inner);
                tracing::debug!(
                    evicted = token_evicted,
                    remaining = inner.token_last_access.len(),
                    "Rebuilt token rate limiter after cleanup"
                );
            } else {
                tracing::trace!(
                    evicted = token_evicted,
                    remaining = inner.token_last_access.len(),
                    "Skipped token rate limiter rebuild (below threshold)"
                );
            }
        }

        if total_evicted > 0 {
            // Reset at-capacity warnings so they can fire again if we fill up
            if ip_evicted > 0 {
                inner.at_capacity_warned_ip.store(false, Ordering::Relaxed);
            }
            if token_evicted > 0 {
                inner
                    .at_capacity_warned_token
                    .store(false, Ordering::Relaxed);
            }
            tracing::debug!(
                total_evicted = total_evicted,
                ip_evicted = ip_evicted,
                token_evicted = token_evicted,
                ip_entries = inner.ip_last_access.len(),
                token_entries = inner.token_last_access.len(),
                "Rate limiter cleanup completed"
            );
        }

        total_evicted
    }

    /// Determine whether a rebuild should be triggered based on eviction metrics.
    ///
    /// A rebuild is triggered if:
    /// - The eviction count exceeds the threshold (fraction of entries or min count), OR
    /// - The minimum interval since last rebuild has passed (fallback for memory reclamation)
    fn should_rebuild(
        &self,
        evicted: usize,
        entries_before_eviction: usize,
        last_rebuild: &RwLock<Instant>,
        now: Instant,
    ) -> bool {
        // Check if eviction count exceeds threshold
        let threshold_by_fraction =
            (entries_before_eviction as f64 * REBUILD_EVICTION_THRESHOLD_FRACTION) as usize;
        let threshold = threshold_by_fraction.max(REBUILD_EVICTION_MIN_COUNT);

        if evicted >= threshold {
            return true;
        }

        // Check if minimum interval since last rebuild has passed
        let last = last_rebuild.read().unwrap_or_else(|poisoned| {
            tracing::warn!("last_rebuild RwLock was poisoned, recovering");
            poisoned.into_inner()
        });
        now.duration_since(*last) >= REBUILD_MIN_INTERVAL
    }

    /// Rebuild the IP rate limiter to reclaim memory from governor's internal DashMap.
    ///
    /// This creates a fresh limiter, losing rate limit state for active IPs.
    /// This is acceptable because:
    /// 1. Active IPs still have entries in `ip_last_access` so they're tracked
    /// 2. The rate limit "resets" which is less impactful than memory exhaustion
    /// 3. This only happens when significant eviction occurs or after minimum interval
    fn rebuild_ip_limiter(&self, inner: &RateLimitStateInner) {
        let new_limiter = RateLimiter::dashmap(inner.ip_quota);
        let mut limiter = inner.ip_limiter.write().unwrap_or_else(|poisoned| {
            tracing::warn!(
                "inner.ip_limiter RwLock was poisoned during rebuild, recovering with into_inner()"
            );
            poisoned.into_inner()
        });
        *limiter = new_limiter;

        // Update last rebuild timestamp
        let mut last_rebuild = inner.last_ip_rebuild.write().unwrap_or_else(|poisoned| {
            tracing::warn!("last_ip_rebuild RwLock was poisoned, recovering");
            poisoned.into_inner()
        });
        *last_rebuild = Instant::now();
    }

    /// Rebuild the token rate limiter to reclaim memory from governor's internal DashMap.
    fn rebuild_token_limiter(&self, inner: &RateLimitStateInner) {
        let new_limiter = RateLimiter::dashmap(inner.token_quota);
        let mut limiter = inner.token_limiter.write().unwrap_or_else(|poisoned| {
            tracing::warn!(
                "inner.token_limiter RwLock was poisoned during rebuild, recovering with into_inner()"
            );
            poisoned.into_inner()
        });
        *limiter = new_limiter;

        // Update last rebuild timestamp
        let mut last_rebuild = inner.last_token_rebuild.write().unwrap_or_else(|poisoned| {
            tracing::warn!("last_token_rebuild RwLock was poisoned, recovering");
            poisoned.into_inner()
        });
        *last_rebuild = Instant::now();
    }

    /// Get the current number of tracked entries.
    pub fn entry_count(&self) -> (usize, usize) {
        match &self.inner {
            Some(inner) => (inner.ip_last_access.len(), inner.token_last_access.len()),
            None => (0, 0),
        }
    }

    /// Log a warning if ConnectInfo is not available (only once).
    fn warn_connect_info_missing(&self) {
        if let Some(inner) = &self.inner
            && !inner.connect_info_warned.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                "ConnectInfo not available for rate limiting. All requests will share a single \
                     rate limit bucket ('unknown' IP). Add .into_make_service_with_connect_info::<SocketAddr>() \
                     to your server configuration to enable per-IP rate limiting."
            );
        }
    }

    /// Log a warning when rate limiter is at capacity (only once per capacity event).
    /// This prevents log spam during DoS attacks where thousands of requests might
    /// be rejected per second.
    ///
    /// Uses a separate `AtomicBool` flag for each limiter type (IP vs token) so that
    /// capacity warnings for one limiter don't suppress warnings for the other.
    fn warn_at_capacity(
        warned_flag: &AtomicBool,
        entry_type: &str,
        current_entries: usize,
        max_entries: u32,
    ) {
        if !warned_flag.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                current_entries = current_entries,
                max_entries = max_entries,
                entry_type = entry_type,
                "Rate limiter at capacity, rejecting new entries. \
                 This warning is logged once per capacity event to prevent log spam."
            );
        }
    }
}

/// Reason for rate limit rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitReason {
    /// Request exceeded rate limit.
    RateLimited,
    /// Rate limiter at capacity, cannot track new entries.
    AtCapacity,
}

/// Error returned when rate limit is exceeded.
#[derive(Debug)]
pub struct RateLimitError {
    /// Number of seconds to wait before retrying.
    pub retry_after_secs: u64,
    /// Reason for the rate limit.
    pub reason: RateLimitReason,
}

impl IntoResponse for RateLimitError {
    fn into_response(self) -> Response {
        let (code, message) = match self.reason {
            RateLimitReason::RateLimited => (
                "rate_limit_exceeded",
                format!(
                    "Rate limit exceeded. Retry after {} seconds.",
                    self.retry_after_secs
                ),
            ),
            RateLimitReason::AtCapacity => (
                "rate_limiter_at_capacity",
                "Server is experiencing high load. Please retry later.".to_string(),
            ),
        };

        let body = serde_json::json!({
            "code": code,
            "message": message,
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
    if let Some(forwarded) = req.headers().get("x-forwarded-for")
        && let Ok(s) = forwarded.to_str()
    {
        // Take the first IP in the chain (client IP)
        if let Some(ip) = s.split(',').next() {
            return Some(ip.trim().to_string());
        }
    }

    // Try X-Real-IP header
    if let Some(real_ip) = req.headers().get("x-real-ip")
        && let Ok(s) = real_ip.to_str()
    {
        return Some(s.trim().to_string());
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
fn extract_ip(req: &Request<Body>, state: &RateLimitState) -> String {
    let inner = match &state.inner {
        Some(inner) => inner,
        None => return "unknown".to_string(),
    };

    let connection_ip = extract_connection_ip(req);

    // Check if we should trust forwarded headers
    let trust_headers = match (&connection_ip, &inner.trusted_proxies) {
        // No connection info available and proxies configured - trust headers
        // (This happens when ConnectInfo isn't set up in the router)
        (None, TrustedProxies::All) => true,
        (None, TrustedProxies::List(_)) => {
            // Can't verify proxy without connection IP, default to not trusting
            false
        }
        (None, TrustedProxies::None) => false,
        // Have connection info - check if it's from a trusted proxy
        (Some(conn_ip), trusted_proxies) => trusted_proxies.is_trusted(conn_ip),
    };

    if trust_headers && let Some(forwarded_ip) = extract_forwarded_ip(req) {
        return forwarded_ip;
    }

    // Use connection IP if available, otherwise "unknown"
    match connection_ip {
        Some(ip) => ip,
        None => {
            state.warn_connect_info_missing();
            "unknown".to_string()
        }
    }
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

    let ip = extract_ip(&req, &rate_limit);

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

/// Spawn a background task that periodically cleans up stale rate limiter entries.
/// Returns a handle that can be used to stop the cleanup task.
pub fn spawn_cleanup_task(
    state: RateLimitState,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;
            let evicted = state.cleanup();
            if evicted > 0 {
                tracing::info!(
                    evicted = evicted,
                    "Rate limiter cleanup task evicted stale entries"
                );
            }
        }
    })
}

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
            ip_requests_per_minute: 60,
            token_requests_per_minute: 120,
            burst_size: 5,
            max_entries: 1000,
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
        assert!(
            result.is_err(),
            "Should be rate limited after burst is exhausted"
        );

        // Different IP should have its own limit
        assert!(state.check_ip("192.168.1.1").is_ok());
    }

    #[test]
    fn test_rate_limit_max_entries() {
        let config = RateLimitConfig {
            enabled: true,
            ip_requests_per_minute: 60,
            burst_size: 5,
            max_entries: 3, // Very small for testing
            ..Default::default()
        };
        let state = RateLimitState::new(&config);

        // Fill up to max_entries
        assert!(state.check_ip("1.1.1.1").is_ok());
        assert!(state.check_ip("2.2.2.2").is_ok());
        assert!(state.check_ip("3.3.3.3").is_ok());

        // Next new IP should be rejected
        let result = state.check_ip("4.4.4.4");
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.reason, RateLimitReason::AtCapacity);
        }

        // Existing IPs should still work
        assert!(state.check_ip("1.1.1.1").is_ok());
    }

    #[test]
    fn test_rate_limit_cleanup() {
        let config = RateLimitConfig {
            enabled: true,
            ip_requests_per_minute: 60,
            burst_size: 5,
            max_entries: 1000,
            entry_ttl_secs: 0, // Immediate expiry for testing
            ..Default::default()
        };
        let state = RateLimitState::new(&config);

        // Add some entries
        assert!(state.check_ip("1.1.1.1").is_ok());
        assert!(state.check_ip("2.2.2.2").is_ok());

        let (ip_count, _) = state.entry_count();
        assert_eq!(ip_count, 2);

        // Wait a tiny bit and cleanup
        std::thread::sleep(std::time::Duration::from_millis(10));
        let evicted = state.cleanup();
        assert_eq!(evicted, 2);

        let (ip_count, _) = state.entry_count();
        assert_eq!(ip_count, 0);
    }

    #[test]
    fn test_trusted_proxies_none() {
        let proxies = TrustedProxies::from_config(&[]);
        assert!(!proxies.is_trusted("127.0.0.1"));
        assert!(!proxies.is_trusted("10.0.0.1"));
    }

    #[test]
    fn test_trusted_proxies_all() {
        let proxies = TrustedProxies::from_config(&["*".to_string()]);
        assert!(proxies.is_trusted("127.0.0.1"));
        assert!(proxies.is_trusted("10.0.0.1"));
        assert!(proxies.is_trusted("anything"));
    }

    #[test]
    fn test_trusted_proxies_list() {
        let proxies =
            TrustedProxies::from_config(&["127.0.0.1".to_string(), "10.0.0.0/8".to_string()]);
        assert!(proxies.is_trusted("127.0.0.1"));
        assert!(proxies.is_trusted("10.0.0.1"));
        assert!(proxies.is_trusted("10.255.255.255"));
        assert!(!proxies.is_trusted("192.168.1.1"));
        assert!(!proxies.is_trusted("11.0.0.1"));
    }
}
