//! Prometheus metrics for the Cellar server.
//!
//! Exposes metrics for upload operations, chunk deduplication, and request latency.
//!
//! # Security Note
//!
//! The `/metrics` endpoint is unauthenticated to allow Prometheus scraping.
//! While metrics don't contain tenant-specific data (no cache IDs, paths, or hashes),
//! they do expose aggregate system usage (total chunks, bytes, active sessions).
//!
//! **Deployment Requirement**: The `/metrics` endpoint MUST be network-restricted
//! to authorized Prometheus scraper IPs only. This should be enforced at the
//! infrastructure level (firewall, load balancer, or reverse proxy rules).
//! Do NOT expose `/metrics` on public networks.

use axum::http::StatusCode;
use axum::response::IntoResponse;
use lazy_static::lazy_static;
use prometheus::{
    self, Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};

lazy_static! {
    /// Global Prometheus registry for all metrics.
    pub static ref REGISTRY: Registry = Registry::new();

    // Upload session metrics
    pub static ref UPLOAD_SESSIONS_CREATED: IntCounter = IntCounter::new(
        "cellar_upload_sessions_created_total",
        "Total number of upload sessions created"
    ).expect("metric creation failed");

    pub static ref UPLOAD_SESSIONS_COMMITTED: IntCounter = IntCounter::new(
        "cellar_upload_sessions_committed_total",
        "Total number of upload sessions successfully committed"
    ).expect("metric creation failed");

    pub static ref UPLOAD_SESSIONS_EXPIRED: IntCounter = IntCounter::new(
        "cellar_upload_sessions_expired_total",
        "Total number of upload sessions that expired"
    ).expect("metric creation failed");

    pub static ref UPLOAD_SESSIONS_RESUMED: IntCounter = IntCounter::new(
        "cellar_upload_sessions_resumed_total",
        "Total number of upload sessions resumed"
    ).expect("metric creation failed");

    // Chunk metrics
    pub static ref CHUNKS_UPLOADED: IntCounter = IntCounter::new(
        "cellar_chunks_uploaded_total",
        "Total number of chunks uploaded"
    ).expect("metric creation failed");

    pub static ref CHUNKS_DEDUPLICATED: IntCounter = IntCounter::new(
        "cellar_chunks_deduplicated_total",
        "Total number of chunks skipped due to deduplication"
    ).expect("metric creation failed");

    pub static ref BYTES_UPLOADED: IntCounter = IntCounter::new(
        "cellar_bytes_uploaded_total",
        "Total bytes uploaded (new chunks only)"
    ).expect("metric creation failed");

    pub static ref BYTES_DEDUPLICATED: IntCounter = IntCounter::new(
        "cellar_bytes_deduplicated_total",
        "Total bytes saved through deduplication"
    ).expect("metric creation failed");

    // Timing metrics
    pub static ref UPLOAD_COMMIT_DURATION: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "cellar_upload_commit_duration_seconds",
            "Time taken to commit an upload session"
        ).buckets(vec![0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0])
    ).expect("metric creation failed");

    pub static ref CHUNK_UPLOAD_DURATION: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "cellar_chunk_upload_duration_seconds",
            "Time taken to upload a single chunk"
        ).buckets(vec![0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0])
    ).expect("metric creation failed");

    // Error metrics
    pub static ref CHUNK_HASH_MISMATCHES: IntCounter = IntCounter::new(
        "cellar_chunk_hash_mismatches_total",
        "Total number of chunk hash verification failures"
    ).expect("metric creation failed");

    pub static ref UPLOAD_ERRORS: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "cellar_upload_errors_total",
            "Total upload errors by error type"
        ),
        &["error_type"]
    ).expect("metric creation failed");

    // Current state gauges
    pub static ref ACTIVE_UPLOAD_SESSIONS: IntGauge = IntGauge::new(
        "cellar_active_upload_sessions",
        "Current number of active upload sessions"
    ).expect("metric creation failed");
}

/// Register all metrics with the global registry.
pub fn register_metrics() {
    REGISTRY
        .register(Box::new(UPLOAD_SESSIONS_CREATED.clone()))
        .expect("metric registration failed");
    REGISTRY
        .register(Box::new(UPLOAD_SESSIONS_COMMITTED.clone()))
        .expect("metric registration failed");
    REGISTRY
        .register(Box::new(UPLOAD_SESSIONS_EXPIRED.clone()))
        .expect("metric registration failed");
    REGISTRY
        .register(Box::new(UPLOAD_SESSIONS_RESUMED.clone()))
        .expect("metric registration failed");
    REGISTRY
        .register(Box::new(CHUNKS_UPLOADED.clone()))
        .expect("metric registration failed");
    REGISTRY
        .register(Box::new(CHUNKS_DEDUPLICATED.clone()))
        .expect("metric registration failed");
    REGISTRY
        .register(Box::new(BYTES_UPLOADED.clone()))
        .expect("metric registration failed");
    REGISTRY
        .register(Box::new(BYTES_DEDUPLICATED.clone()))
        .expect("metric registration failed");
    REGISTRY
        .register(Box::new(UPLOAD_COMMIT_DURATION.clone()))
        .expect("metric registration failed");
    REGISTRY
        .register(Box::new(CHUNK_UPLOAD_DURATION.clone()))
        .expect("metric registration failed");
    REGISTRY
        .register(Box::new(CHUNK_HASH_MISMATCHES.clone()))
        .expect("metric registration failed");
    REGISTRY
        .register(Box::new(UPLOAD_ERRORS.clone()))
        .expect("metric registration failed");
    REGISTRY
        .register(Box::new(ACTIVE_UPLOAD_SESSIONS.clone()))
        .expect("metric registration failed");
}

/// GET /metrics - Prometheus metrics endpoint.
pub async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();

    let mut buffer = Vec::new();
    match encoder.encode(&metric_families, &mut buffer) {
        Ok(()) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
            buffer,
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("content-type", "text/plain; charset=utf-8")],
            format!("Failed to encode metrics: {e}").into_bytes(),
        ),
    }
}

/// Helper to record upload errors by type.
pub fn record_upload_error(error_type: &str) {
    UPLOAD_ERRORS.with_label_values(&[error_type]).inc();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registration() {
        // This would panic if any metric creation failed
        register_metrics();
    }
}
