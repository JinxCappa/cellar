//! Application state shared across handlers.

use crate::ratelimit::RateLimitState;
use cellar_core::config::AppConfig;
use cellar_metadata::MetadataStore;
use cellar_signer::NarInfoSigner;
use cellar_storage::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Registry for tracking background GC tasks and detecting panics.
///
/// When a GC task panics, it gets stuck in 'running' state forever, blocking future GC
/// until server restart. This registry tracks spawned tasks and automatically marks
/// panicked jobs as 'failed', allowing recovery within ~10s instead of requiring restart.
pub struct GcTaskRegistry {
    /// Map of job_id -> task handle
    tasks: Arc<Mutex<HashMap<Uuid, JoinHandle<()>>>>,
    /// Metadata store for updating job state
    metadata: Arc<dyn MetadataStore>,
}

impl GcTaskRegistry {
    /// Create a new GC task registry.
    pub fn new(metadata: Arc<dyn MetadataStore>) -> Self {
        Self {
            tasks: Arc::new(Mutex::new(HashMap::new())),
            metadata,
        }
    }

    /// Register a spawned GC task.
    pub async fn register(&self, job_id: Uuid, handle: JoinHandle<()>) {
        self.tasks.lock().await.insert(job_id, handle);
    }

    /// Spawn a watchdog task that periodically checks for panicked tasks.
    /// Returns the watchdog's JoinHandle (caller should keep it to prevent early termination).
    pub fn spawn_watchdog(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                self.check_tasks().await;
            }
        })
    }

    /// Check all tracked tasks for completion or panics.
    async fn check_tasks(&self) {
        let mut finished_handles = Vec::new();

        // Scope 1: Collect finished task handles (hold mutex only briefly)
        {
            let mut tasks = self.tasks.lock().await;
            let mut finished_jobs = Vec::new();

            // Find all finished tasks
            for (job_id, handle) in tasks.iter() {
                if handle.is_finished() {
                    finished_jobs.push(*job_id);
                }
            }

            // Remove finished tasks and collect their handles
            for job_id in finished_jobs {
                if let Some(handle) = tasks.remove(&job_id) {
                    finished_handles.push((job_id, handle));
                }
            }
        } // Mutex is released here

        // Scope 2: Process finished tasks WITHOUT holding the mutex
        for (job_id, handle) in finished_handles {
            match handle.await {
                Err(join_err) if join_err.is_panic() => {
                    crate::metrics::GC_JOBS_ACTIVE.dec();
                    // CRITICAL: Task panicked, mark job as failed
                    tracing::error!(
                        job_id = %job_id,
                        panic = ?join_err,
                        "GC task panicked, marking job as failed"
                    );

                    // Increment panic metric
                    crate::metrics::GC_JOBS_PANICKED.inc();

                    let stats = serde_json::json!({
                        "items_processed": 0,
                        "items_deleted": 0,
                        "bytes_reclaimed": 0,
                        "errors": 1,
                        "panic": true
                    });
                    let stats_json = stats.to_string();

                    if let Err(e) = self
                        .metadata
                        .update_gc_job_state(
                            job_id,
                            "failed",
                            Some(time::OffsetDateTime::now_utc()),
                            Some(&stats_json),
                            None,
                        )
                        .await
                    {
                        tracing::error!(
                            job_id = %job_id,
                            error = %e,
                            "Failed to mark panicked job as failed"
                        );
                    }
                }
                Err(join_err) if join_err.is_cancelled() => {
                    crate::metrics::GC_JOBS_ACTIVE.dec();
                    tracing::warn!(job_id = %job_id, "GC task was cancelled");
                }
                Ok(_) => {
                    tracing::debug!(job_id = %job_id, "GC task completed successfully");
                }
                Err(e) => {
                    crate::metrics::GC_JOBS_ACTIVE.dec();
                    tracing::error!(job_id = %job_id, error = ?e, "GC task failed with unknown error");
                }
            }
        }
    }
}

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    /// Application configuration.
    pub config: Arc<AppConfig>,
    /// Object storage backend.
    pub storage: Arc<dyn ObjectStore>,
    /// Metadata store.
    pub metadata: Arc<dyn MetadataStore>,
    /// Narinfo signer (optional).
    pub signer: Option<Arc<NarInfoSigner>>,
    /// Rate limiting state.
    pub rate_limit: RateLimitState,
    /// GC task registry for panic detection (P2 Issue #7).
    pub gc_task_registry: Arc<GcTaskRegistry>,
}

impl AppState {
    /// Create a new application state.
    ///
    /// This performs configuration validation and logs warnings for potentially
    /// dangerous settings. Panics if configuration is invalid.
    ///
    /// # Panics
    ///
    /// Panics if rate limit configuration validation fails with an error.
    pub fn new(
        config: AppConfig,
        storage: Arc<dyn ObjectStore>,
        metadata: Arc<dyn MetadataStore>,
        signer: Option<NarInfoSigner>,
        gc_task_registry: Arc<GcTaskRegistry>,
    ) -> Self {
        // Validate rate limit configuration - fail fast on errors, log warnings
        match config.rate_limit.validate() {
            Ok(warnings) => {
                for warning in warnings {
                    tracing::warn!("Configuration warning: {}", warning);
                }
            }
            Err(error) => {
                panic!("Invalid rate limit configuration: {}", error);
            }
        }

        // Validate GC configuration - fail fast on errors
        if let Err(error) = config.gc.validate() {
            panic!("Invalid GC configuration: {}", error);
        }

        let rate_limit = RateLimitState::new(&config.rate_limit);

        Self {
            config: Arc::new(config),
            storage,
            metadata,
            signer: signer.map(Arc::new),
            rate_limit,
            gc_task_registry,
        }
    }

    /// Get the cleanup interval for rate limiter, if enabled.
    /// Returns None if rate limiting is disabled.
    /// Returns a default of 60 seconds if cleanup interval is configured as zero
    /// (to prevent tokio::time::interval from panicking).
    pub fn rate_limit_cleanup_interval(&self) -> Option<Duration> {
        if self.rate_limit.is_enabled() {
            let interval_secs = self.config.rate_limit.cleanup_interval_secs;
            // Guard against zero interval which would cause tokio::time::interval to panic
            if interval_secs == 0 {
                tracing::warn!(
                    "rate_limit.cleanup_interval_secs is 0, using default of 60 seconds"
                );
                Some(Duration::from_secs(60))
            } else {
                Some(Duration::from_secs(interval_secs))
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellar_core::config::AppConfig;
    use cellar_metadata::SqliteStore;
    use cellar_storage::backends::filesystem::FilesystemBackend;
    use std::time::Duration;
    use tempfile::tempdir;

    async fn build_state(config: AppConfig) -> (tempfile::TempDir, AppState) {
        let temp = tempdir().unwrap();
        let storage: Arc<dyn cellar_storage::ObjectStore> =
            Arc::new(FilesystemBackend::new(temp.path()).await.unwrap());

        let db_path = temp.path().join("metadata.db");
        let metadata: Arc<dyn MetadataStore> =
            Arc::new(SqliteStore::new(&db_path, None).await.unwrap());
        let gc_task_registry = Arc::new(GcTaskRegistry::new(metadata.clone()));

        let state = AppState::new(config, storage, metadata, None, gc_task_registry);
        (temp, state)
    }

    #[tokio::test]
    async fn rate_limit_cleanup_interval_none_when_disabled() {
        let (_temp, state) = build_state(AppConfig::for_testing()).await;
        assert!(state.rate_limit_cleanup_interval().is_none());
    }

    #[tokio::test]
    async fn rate_limit_cleanup_interval_enabled_respects_config() {
        let mut config = AppConfig::for_testing();
        config.rate_limit.enabled = true;
        config.rate_limit.cleanup_interval_secs = 12;

        let (_temp, state) = build_state(config).await;
        assert_eq!(
            state.rate_limit_cleanup_interval(),
            Some(Duration::from_secs(12))
        );
    }

    #[tokio::test]
    async fn rate_limit_cleanup_interval_zero_uses_default() {
        let temp = tempdir().unwrap();
        let storage: Arc<dyn cellar_storage::ObjectStore> =
            Arc::new(FilesystemBackend::new(temp.path()).await.unwrap());

        let db_path = temp.path().join("metadata.db");
        let metadata: Arc<dyn MetadataStore> =
            Arc::new(SqliteStore::new(&db_path, None).await.unwrap());
        let gc_task_registry = Arc::new(GcTaskRegistry::new(metadata.clone()));

        let mut config = AppConfig::for_testing();
        config.rate_limit.enabled = true;
        config.rate_limit.cleanup_interval_secs = 0;

        let rate_limit = RateLimitState::new(&config.rate_limit);

        let state = AppState {
            config: Arc::new(config),
            storage,
            metadata,
            signer: None,
            rate_limit,
            gc_task_registry,
        };

        assert_eq!(
            state.rate_limit_cleanup_interval(),
            Some(Duration::from_secs(60))
        );
    }
}
