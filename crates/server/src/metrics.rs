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
use prometheus::{
    self, Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::sync::{LazyLock, Once};

/// Global Prometheus registry for all metrics.
pub static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

// Upload session metrics
pub static UPLOAD_SESSIONS_CREATED: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "cellar_upload_sessions_created_total",
        "Total number of upload sessions created",
    )
    .expect("metric creation failed")
});

pub static UPLOAD_SESSIONS_COMMITTED: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "cellar_upload_sessions_committed_total",
        "Total number of upload sessions successfully committed",
    )
    .expect("metric creation failed")
});

pub static UPLOAD_SESSIONS_EXPIRED: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "cellar_upload_sessions_expired_total",
        "Total number of upload sessions that expired",
    )
    .expect("metric creation failed")
});

pub static UPLOAD_SESSIONS_RESUMED: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "cellar_upload_sessions_resumed_total",
        "Total number of upload sessions resumed",
    )
    .expect("metric creation failed")
});

// Chunk metrics
pub static CHUNKS_UPLOADED: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "cellar_chunks_uploaded_total",
        "Total number of chunks uploaded",
    )
    .expect("metric creation failed")
});

pub static CHUNKS_DEDUPLICATED: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "cellar_chunks_deduplicated_total",
        "Total number of chunks skipped due to deduplication",
    )
    .expect("metric creation failed")
});

pub static BYTES_UPLOADED: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "cellar_bytes_uploaded_total",
        "Total bytes uploaded (new chunks only)",
    )
    .expect("metric creation failed")
});

pub static BYTES_DEDUPLICATED: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "cellar_bytes_deduplicated_total",
        "Total bytes saved through deduplication",
    )
    .expect("metric creation failed")
});

// Timing metrics
pub static UPLOAD_COMMIT_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::with_opts(
        HistogramOpts::new(
            "cellar_upload_commit_duration_seconds",
            "Time taken to commit an upload session",
        )
        .buckets(vec![0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0]),
    )
    .expect("metric creation failed")
});

pub static CHUNK_UPLOAD_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::with_opts(
        HistogramOpts::new(
            "cellar_chunk_upload_duration_seconds",
            "Time taken to upload a single chunk",
        )
        .buckets(vec![0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
    )
    .expect("metric creation failed")
});

// Error metrics
pub static CHUNK_HASH_MISMATCHES: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "cellar_chunk_hash_mismatches_total",
        "Total number of chunk hash verification failures",
    )
    .expect("metric creation failed")
});

pub static UPLOAD_ERRORS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "cellar_upload_errors_total",
            "Total upload errors by error type",
        ),
        &["error_type"],
    )
    .expect("metric creation failed")
});

// Current state gauges
pub static ACTIVE_UPLOAD_SESSIONS: LazyLock<IntGauge> = LazyLock::new(|| {
    IntGauge::new(
        "cellar_active_upload_sessions",
        "Current number of active upload sessions",
    )
    .expect("metric creation failed")
});

// GC metrics
pub static GC_JOBS_PANICKED: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "cellar_gc_jobs_panicked_total",
        "Total number of GC jobs that panicked",
    )
    .expect("metric creation failed")
});

pub static GC_JOBS_ACTIVE: LazyLock<IntGauge> = LazyLock::new(|| {
    IntGauge::new(
        "cellar_gc_jobs_active",
        "Number of currently active GC jobs (queued + running)",
    )
    .expect("metric creation failed")
});

pub static GC_REFCOUNT_ROLLBACKS: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "cellar_gc_refcount_rollbacks_total",
        "Total number of refcounts rolled back during commit recovery",
    )
    .expect("metric creation failed")
});

pub static GC_ROLLBACK_PARSE_FAILURES: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "cellar_gc_rollback_parse_failures_total",
        "Total number of commit_progress JSON parse failures during rollback (creates refcount leaks)",
    )
    .expect("metric creation failed")
});

pub static GC_JOB_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "cellar_gc_job_duration_seconds",
            "GC job duration by type and state",
        )
        .buckets(vec![
            1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0,
        ]),
        &["job_type", "state"],
    )
    .expect("metric creation failed")
});

pub static GC_ITEMS_DELETED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "cellar_gc_items_deleted_total",
            "Total items deleted by GC job type",
        ),
        &["job_type"],
    )
    .expect("metric creation failed")
});

pub static GC_BYTES_RECLAIMED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "cellar_gc_bytes_reclaimed_total",
            "Total bytes reclaimed by GC job type",
        ),
        &["job_type"],
    )
    .expect("metric creation failed")
});

pub static GC_BATCH_TIMEOUTS: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "cellar_gc_batch_timeouts_total",
        "Total number of GC batch operations that timed out",
    )
    .expect("metric creation failed")
});

// Storage listing metrics
pub static STORAGE_LISTING_PAGES_FETCHED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "cellar_storage_listing_pages_fetched_total",
            "Total number of listing pages fetched from storage backend",
        ),
        &["backend"],
    )
    .expect("metric creation failed")
});

pub static STORAGE_LISTING_TOKENS_INVALID: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "cellar_storage_listing_tokens_invalid_total",
            "Total number of invalid continuation tokens encountered",
        ),
        &["backend"],
    )
    .expect("metric creation failed")
});

pub static STORAGE_LISTING_MEMORY_PEAK_BYTES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "cellar_storage_listing_memory_peak_bytes",
            "Peak memory usage during storage listing operations",
        ),
        &["backend"],
    )
    .expect("metric creation failed")
});

pub static STORAGE_LISTING_CANCELLED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "cellar_storage_listing_cancelled_total",
            "Total number of storage listing operations cancelled before completion",
        ),
        &["backend"],
    )
    .expect("metric creation failed")
});

// GC checkpoint metrics
pub static GC_CHECKPOINT_SAVED: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "cellar_gc_checkpoint_saved_total",
        "Total number of GC sweep checkpoints saved",
    )
    .expect("metric creation failed")
});

pub static GC_CHECKPOINT_RESUMED: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "cellar_gc_checkpoint_resumed_total",
        "Total number of GC sweeps resumed from checkpoint",
    )
    .expect("metric creation failed")
});

pub static GC_CHECKPOINT_INVALIDATED: LazyLock<IntCounter> = LazyLock::new(|| {
    IntCounter::new(
        "cellar_gc_checkpoint_invalidated_total",
        "Total number of GC checkpoints invalidated due to config changes or errors",
    )
    .expect("metric creation failed")
});

/// Guard to ensure metrics are only registered once.
static REGISTER_ONCE: Once = Once::new();

/// Register all metrics with the global registry.
///
/// This function is idempotent - subsequent calls after the first are no-ops.
/// This allows safe use in integration tests or when embedding multiple routers.
pub fn register_metrics() {
    REGISTER_ONCE.call_once(|| {
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

        // GC metrics
        REGISTRY
            .register(Box::new(GC_JOBS_PANICKED.clone()))
            .expect("metric registration failed");
        REGISTRY
            .register(Box::new(GC_JOBS_ACTIVE.clone()))
            .expect("metric registration failed");
        REGISTRY
            .register(Box::new(GC_REFCOUNT_ROLLBACKS.clone()))
            .expect("metric registration failed");
        REGISTRY
            .register(Box::new(GC_ROLLBACK_PARSE_FAILURES.clone()))
            .expect("metric registration failed");
        REGISTRY
            .register(Box::new(GC_JOB_DURATION.clone()))
            .expect("metric registration failed");
        REGISTRY
            .register(Box::new(GC_ITEMS_DELETED.clone()))
            .expect("metric registration failed");
        REGISTRY
            .register(Box::new(GC_BYTES_RECLAIMED.clone()))
            .expect("metric registration failed");
        REGISTRY
            .register(Box::new(GC_BATCH_TIMEOUTS.clone()))
            .expect("metric registration failed");

        // Storage listing metrics
        REGISTRY
            .register(Box::new(STORAGE_LISTING_PAGES_FETCHED.clone()))
            .expect("metric registration failed");
        REGISTRY
            .register(Box::new(STORAGE_LISTING_TOKENS_INVALID.clone()))
            .expect("metric registration failed");
        REGISTRY
            .register(Box::new(STORAGE_LISTING_MEMORY_PEAK_BYTES.clone()))
            .expect("metric registration failed");
        REGISTRY
            .register(Box::new(STORAGE_LISTING_CANCELLED.clone()))
            .expect("metric registration failed");

        // GC checkpoint metrics
        REGISTRY
            .register(Box::new(GC_CHECKPOINT_SAVED.clone()))
            .expect("metric registration failed");
        REGISTRY
            .register(Box::new(GC_CHECKPOINT_RESUMED.clone()))
            .expect("metric registration failed");
        REGISTRY
            .register(Box::new(GC_CHECKPOINT_INVALIDATED.clone()))
            .expect("metric registration failed");
    });
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
