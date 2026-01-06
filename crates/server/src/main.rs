//! Cellar server binary.

use anyhow::{Context, Result};
use cellar_core::config::{AppConfig, PrivateKeyConfig, SigningConfig};
use cellar_metadata::MetadataStore;
use cellar_metadata::repos::gc::{GcJobState, GcStats};
use cellar_server::bootstrap::ensure_admin_token;
use cellar_server::{AppState, create_router};
use cellar_signer::NarInfoSigner;
use clap::Parser;
use figment::Figment;
use figment::providers::{Env, Format, Toml};
use std::net::SocketAddr;
use std::sync::Arc;
use time::OffsetDateTime;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Cellar - A Nix binary cache server
#[derive(Parser, Debug)]
#[command(name = "cellard")]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(
        short,
        long,
        env = "CELLAR_CONFIG",
        default_value = "config/server.toml"
    )]
    config: String,
}

/// Spawn a GC job for a specific cache (or public cache if cache_id = None).
/// Helper function to deduplicate GC job spawning logic for auto-scheduler.
async fn spawn_gc_job_for_cache(
    cache_id: Option<uuid::Uuid>,
    job_type: cellar_metadata::repos::gc::GcJobType,
    job_type_str: &str,
    state: &cellar_server::AppState,
) -> Result<()> {
    let job_id = uuid::Uuid::new_v4();
    let now = time::OffsetDateTime::now_utc();

    let job = cellar_metadata::models::GcJobRow {
        gc_job_id: job_id,
        cache_id,
        job_type: job_type.as_str().to_string(),
        state: cellar_metadata::repos::gc::GcJobState::Queued
            .as_str()
            .to_string(),
        started_at: Some(now),
        finished_at: None,
        stats_json: None,
        sweep_checkpoint_json: None,
    };

    // Try to create job - database will reject if another job of same type is running
    match state.metadata.create_gc_job(&job).await {
        Ok(_) => {
            tracing::info!(
                job_id = %job_id,
                cache_id = ?cache_id,
                job_type = %job_type_str,
                "Automatic GC job created"
            );

            // Increment active GC jobs metric (matches watchdog dec on panic/cancel)
            cellar_server::metrics::GC_JOBS_ACTIVE.inc();

            // Spawn GC task
            let metadata = state.metadata.clone();
            let storage = state.storage.clone();
            let gc_config = state.config.gc.clone();
            let gc_registry = state.gc_task_registry.clone();

            let handle = tokio::spawn(async move {
                use cellar_server::handlers::admin::run_gc_job;
                let result = run_gc_job(
                    job_id,
                    job_type,
                    cache_id,
                    metadata.clone(),
                    storage,
                    gc_config,
                )
                .await;

                let (job_state, stats) = match result {
                    Ok(stats) => {
                        if stats.errors > 0 {
                            (cellar_metadata::repos::gc::GcJobState::Failed, Some(stats))
                        } else {
                            (
                                cellar_metadata::repos::gc::GcJobState::Finished,
                                Some(stats),
                            )
                        }
                    }
                    Err(e) => {
                        tracing::error!(job_id = %job_id, error = %e, "Automatic GC job failed");
                        (cellar_metadata::repos::gc::GcJobState::Failed, None)
                    }
                };

                // Decrement active GC jobs metric (matches inc before spawn)
                cellar_server::metrics::GC_JOBS_ACTIVE.dec();

                let stats_json = stats.and_then(|s| serde_json::to_string(&s).ok());
                if let Err(e) = metadata
                    .update_gc_job_state(
                        job_id,
                        job_state.as_str(),
                        Some(time::OffsetDateTime::now_utc()),
                        stats_json.as_deref(),
                        None,
                    )
                    .await
                {
                    tracing::error!(job_id = %job_id, error = %e, "Failed to update automatic GC job state");
                }
            });

            // Register with watchdog
            gc_registry.register(job_id, handle).await;
            Ok(())
        }
        Err(cellar_metadata::error::MetadataError::Constraint(_)) => {
            tracing::debug!(
                cache_id = ?cache_id,
                job_type = %job_type_str,
                "Another GC job of this type is already running for this cache, skipping"
            );
            Ok(()) // Not an error - just skip
        }
        Err(e) => Err(e.into()),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI arguments
    let args = Args::parse();

    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Startup banner
    tracing::info!("Cellar v{}", env!("CARGO_PKG_VERSION"));

    // Load configuration (file is optional, env vars can provide/override everything)
    let config_path = std::path::Path::new(&args.config);
    let mut figment = Figment::new();
    let has_config_file = config_path.exists();

    if has_config_file {
        tracing::info!(config_path = %args.config, "Loading configuration from file");
        figment = figment.merge(Toml::file(&args.config));
    } else {
        tracing::debug!("No config file found at {}", args.config);
    }

    // Check for CELLAR_ environment variables (excluding CELLAR_CONFIG which is just the path)
    let has_env_config =
        std::env::vars().any(|(key, _)| key.starts_with("CELLAR_") && key != "CELLAR_CONFIG");

    if !has_config_file && !has_env_config {
        anyhow::bail!(
            "No configuration provided.\n\n\
             Provide configuration via one of:\n  \
             1. Config file: cellard --config /path/to/config.toml\n  \
             2. Environment variables: CELLAR_SERVER__BIND=0.0.0.0:8080 \
             CELLAR_ADMIN__TOKEN_HASH=sha256:YOUR_TOKEN_HASH_HERE cellard\n\n\
             See config/server.example.toml for example configuration.\n\
             Set CELLAR_CONFIG env var to specify a default config file path."
        );
    }

    if !has_config_file {
        tracing::info!("Using environment variables for configuration");
    }

    let config: AppConfig = figment
        .merge(Env::prefixed("CELLAR_").split("__"))
        .extract()
        .context("failed to load configuration")?;

    // Register Prometheus metrics
    cellar_server::metrics::register_metrics();
    tracing::info!("Prometheus metrics registered");

    // Initialize storage backend
    let storage = cellar_storage::from_config(&config.storage)
        .await
        .context("failed to initialize storage")?;
    tracing::info!("Storage backend initialized");

    // Verify storage connectivity before accepting requests.
    // This catches configuration errors and connectivity issues early,
    // preventing the server from reporting healthy when storage is unreachable.
    storage
        .health_check()
        .await
        .context("storage health check failed")?;
    tracing::info!("Storage backend connectivity verified");

    // Initialize metadata store
    let metadata = cellar_metadata::from_config(&config.metadata)
        .await
        .context("failed to initialize metadata store")?;
    tracing::info!("Metadata store initialized");

    // Initialize admin token
    ensure_admin_token(metadata.as_ref(), &config.admin).await?;

    // Recover orphaned GC jobs
    recover_orphaned_gc_jobs(&metadata).await?;
    tracing::info!("Orphaned GC jobs recovered");

    // Initialize signer if configured
    let signer = if let Some(signing_config) = &config.signing {
        Some(load_signer(signing_config).await?)
    } else {
        tracing::warn!("No signing key configured, narinfo will be unsigned");
        None
    };

    // Create GC task registry for panic detection (P2 Issue #7)
    let gc_task_registry = Arc::new(cellar_server::state::GcTaskRegistry::new(metadata.clone()));

    // Spawn watchdog task to detect panicked GC jobs
    let _watchdog_handle = gc_task_registry.clone().spawn_watchdog();
    tracing::info!("GC task watchdog spawned");

    // Create application state
    let state = AppState::new(config.clone(), storage, metadata, signer, gc_task_registry);

    // Spawn rate limiter cleanup task if rate limiting is enabled
    if let Some(cleanup_interval) = state.rate_limit_cleanup_interval() {
        let rate_limit_state = state.rate_limit.clone();
        cellar_server::ratelimit::spawn_cleanup_task(rate_limit_state, cleanup_interval);
        tracing::info!(
            interval_secs = cleanup_interval.as_secs(),
            "Rate limiter cleanup task spawned"
        );
    }

    // Spawn automatic GC scheduler if enabled
    if config.gc.auto_schedule_enabled {
        let state_clone = state.clone();
        let interval = config.gc.auto_schedule_interval();
        let job_types = config.gc.auto_schedule_jobs.clone();

        tokio::spawn(async move {
            tracing::info!(
                interval_secs = interval.as_secs(),
                job_types = ?job_types,
                "Automatic GC scheduler enabled"
            );

            loop {
                tokio::time::sleep(interval).await;

                for job_type_str in &job_types {
                    tracing::info!(job_type = %job_type_str, "Triggering automatic GC for all caches");

                    // Parse job type
                    let job_type = match job_type_str.as_str() {
                        "upload_gc" => cellar_metadata::repos::gc::GcJobType::UploadGc,
                        "chunk_gc" => cellar_metadata::repos::gc::GcJobType::ChunkGc,
                        "manifest_gc" => cellar_metadata::repos::gc::GcJobType::ManifestGc,
                        "committing_gc" => cellar_metadata::repos::gc::GcJobType::CommittingGc,
                        "storage_sweep" => cellar_metadata::repos::gc::GcJobType::StorageSweep,
                        _ => {
                            tracing::warn!(job_type = %job_type_str, "Unknown GC job type in auto_schedule_jobs, skipping");
                            continue;
                        }
                    };

                    // FIXED (P0): Iterate through ALL caches (tenant caches + public cache).
                    // Previously used cache_id=None which metadata queries interpreted as
                    // "cache_id IS NULL" (public cache only), causing tenant caches to never be GC'd.

                    // Get all caches (tenant caches with UUIDs)
                    let caches = match state_clone.metadata.list_caches().await {
                        Ok(caches) => caches,
                        Err(e) => {
                            tracing::error!(
                                job_type = %job_type_str,
                                error = %e,
                                "Failed to list caches for automatic GC, skipping this run"
                            );
                            continue;
                        }
                    };

                    // Trigger GC for each tenant cache
                    for cache in &caches {
                        let cache_id = Some(cache.cache_id);

                        if let Err(e) =
                            spawn_gc_job_for_cache(cache_id, job_type, job_type_str, &state_clone)
                                .await
                        {
                            tracing::error!(
                                cache_id = %cache.cache_id,
                                cache_name = %cache.cache_name,
                                job_type = %job_type_str,
                                error = %e,
                                "Failed to spawn automatic GC job for cache"
                            );
                        }
                    }

                    // Also trigger GC for public/global cache (cache_id = None)
                    if let Err(e) =
                        spawn_gc_job_for_cache(None, job_type, job_type_str, &state_clone).await
                    {
                        tracing::error!(
                            job_type = %job_type_str,
                            error = %e,
                            "Failed to spawn automatic GC job for public cache"
                        );
                    }
                }
            }
        });

        tracing::info!("Automatic GC scheduler spawned");
    } else {
        tracing::info!("Automatic GC scheduling disabled");
    }

    // Create router
    let app = create_router(state);

    // Parse bind address
    let addr: SocketAddr = config.server.bind.parse().context("invalid bind address")?;

    tracing::info!("Listening on {}", addr);

    // Start server with ConnectInfo for client IP extraction
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind to {}", addr))?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}

/// Load the narinfo signer from configuration.
async fn load_signer(config: &SigningConfig) -> Result<NarInfoSigner> {
    match &config.private_key {
        PrivateKeyConfig::File { path } => {
            let key_data = tokio::fs::read_to_string(path)
                .await
                .with_context(|| format!("failed to read key file: {}", path.display()))?;
            let signer = NarInfoSigner::from_nix_secret_key(key_data.trim())
                .context("failed to parse signing key")?;
            tracing::info!("Loaded signing key: {}", signer.key_name());
            Ok(signer)
        }
        PrivateKeyConfig::Env { var } => {
            let key_data = std::env::var(var)
                .with_context(|| format!("signing key env var not set: {var}"))?;
            let signer = NarInfoSigner::from_nix_secret_key(key_data.trim())
                .context("failed to parse signing key")?;
            tracing::info!("Loaded signing key from env: {}", signer.key_name());
            Ok(signer)
        }
        PrivateKeyConfig::Generate => {
            tracing::warn!("Generating ephemeral signing key (not suitable for production)");
            let signer = NarInfoSigner::generate(&config.key_name);
            tracing::info!("Generated signing key: {}", signer.key_name());
            tracing::info!("Public key: {}", signer.nix_public_key());
            Ok(signer)
        }
        PrivateKeyConfig::Value { key } => {
            tracing::warn!("Using inline signing key (not recommended for production)");
            let signer = NarInfoSigner::from_nix_secret_key(key.trim())
                .context("failed to parse signing key")?;
            tracing::info!("Loaded signing key: {}", signer.key_name());
            Ok(signer)
        }
    }
}

/// Recover orphaned GC jobs on server startup.
///
/// When the server crashes or is killed, GC jobs can be left in incomplete states:
/// - 'queued': Server crashed before spawning the background task
/// - 'running': Server crashed while GC was executing
///
/// This function marks ALL such orphaned jobs (both global and cache-scoped) as 'failed'
/// to prevent them from blocking future GC operations.
async fn recover_orphaned_gc_jobs(metadata: &Arc<dyn MetadataStore>) -> Result<()> {
    let orphaned_jobs = metadata
        .get_orphaned_gc_jobs()
        .await
        .context("failed to query orphaned GC jobs")?;

    if orphaned_jobs.is_empty() {
        return Ok(());
    }

    tracing::warn!(
        count = orphaned_jobs.len(),
        "Found orphaned GC jobs from previous server instance, marking as failed"
    );

    for job in orphaned_jobs {
        let stats = GcStats {
            items_processed: 0,
            items_deleted: 0,
            bytes_reclaimed: 0,
            errors: 1, // Mark as error to indicate recovery
        };

        let stats_json =
            serde_json::to_string(&stats).context("failed to serialize recovery stats")?;

        metadata
            .update_gc_job_state(
                job.gc_job_id,
                GcJobState::Failed.as_str(),
                Some(OffsetDateTime::now_utc()),
                Some(&stats_json),
                None,
            )
            .await
            .with_context(|| format!("failed to mark orphaned job {} as failed", job.gc_job_id))?;

        tracing::warn!(
            job_id = %job.gc_job_id,
            job_type = %job.job_type,
            job_state = %job.state,
            cache_id = ?job.cache_id,
            "Marked orphaned GC job as failed"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellar_core::config::AppConfig;
    use cellar_metadata::SqliteStore;
    use cellar_metadata::models::GcJobRow;
    use cellar_metadata::repos::gc::{GcJobState, GcJobType};
    use cellar_storage::backends::filesystem::FilesystemBackend;
    use std::time::Duration;
    use tempfile::tempdir;
    use time::OffsetDateTime;

    async fn build_state() -> (tempfile::TempDir, AppState) {
        let temp = tempdir().unwrap();
        let storage: Arc<dyn cellar_storage::ObjectStore> =
            Arc::new(FilesystemBackend::new(temp.path()).await.unwrap());
        let db_path = temp.path().join("metadata.db");
        let metadata: Arc<dyn MetadataStore> =
            Arc::new(SqliteStore::new(&db_path, None).await.unwrap());
        let gc_task_registry =
            Arc::new(cellar_server::state::GcTaskRegistry::new(metadata.clone()));

        let config = AppConfig::for_testing();
        let state = AppState::new(config, storage, metadata, None, gc_task_registry);
        (temp, state)
    }

    #[tokio::test]
    async fn load_signer_from_env() {
        let signer = NarInfoSigner::generate("test-cache");
        let secret = signer.nix_secret_key();
        let prev = std::env::var("CELLAR_TEST_SIGNING_KEY").ok();
        // SAFETY: Tests run with --test-threads=1 so no concurrent access
        unsafe { std::env::set_var("CELLAR_TEST_SIGNING_KEY", &secret) };

        let config = SigningConfig {
            key_name: "test-cache".to_string(),
            private_key: PrivateKeyConfig::Env {
                var: "CELLAR_TEST_SIGNING_KEY".to_string(),
            },
        };

        let loaded = load_signer(&config).await.unwrap();
        assert_eq!(loaded.key_name(), "test-cache");

        // SAFETY: Tests run with --test-threads=1 so no concurrent access
        unsafe {
            if let Some(value) = prev {
                std::env::set_var("CELLAR_TEST_SIGNING_KEY", value);
            } else {
                std::env::remove_var("CELLAR_TEST_SIGNING_KEY");
            }
        }
    }

    #[tokio::test]
    async fn load_signer_from_file() {
        let signer = NarInfoSigner::generate("test-cache");
        let secret = signer.nix_secret_key();
        let temp = tempdir().unwrap();
        let key_path = temp.path().join("signing.key");
        tokio::fs::write(&key_path, secret).await.unwrap();

        let config = SigningConfig {
            key_name: "test-cache".to_string(),
            private_key: PrivateKeyConfig::File { path: key_path },
        };

        let loaded = load_signer(&config).await.unwrap();
        assert_eq!(loaded.key_name(), "test-cache");
    }

    #[tokio::test]
    async fn load_signer_generate() {
        let config = SigningConfig {
            key_name: "ephemeral".to_string(),
            private_key: PrivateKeyConfig::Generate,
        };

        let loaded = load_signer(&config).await.unwrap();
        assert_eq!(loaded.key_name(), "ephemeral");
    }

    #[tokio::test]
    async fn load_signer_from_value() {
        let signer = NarInfoSigner::generate("inline-test");
        let secret = signer.nix_secret_key();

        let config = SigningConfig {
            key_name: "inline-test".to_string(),
            private_key: PrivateKeyConfig::Value { key: secret },
        };

        let loaded = load_signer(&config).await.unwrap();
        assert_eq!(loaded.key_name(), "inline-test");
    }

    #[tokio::test]
    async fn recover_orphaned_gc_jobs_marks_failed() {
        let (_temp, state) = build_state().await;
        let metadata = state.metadata.clone();

        let job = GcJobRow {
            gc_job_id: uuid::Uuid::new_v4(),
            cache_id: None,
            job_type: GcJobType::UploadGc.as_str().to_string(),
            state: GcJobState::Queued.as_str().to_string(),
            started_at: Some(OffsetDateTime::now_utc()),
            finished_at: None,
            stats_json: None,
            sweep_checkpoint_json: None,
        };

        metadata.create_gc_job(&job).await.unwrap();
        recover_orphaned_gc_jobs(&metadata).await.unwrap();

        let updated = metadata.get_gc_job(job.gc_job_id).await.unwrap().unwrap();
        assert_eq!(updated.state, GcJobState::Failed.as_str());
        assert!(
            updated
                .stats_json
                .as_ref()
                .is_some_and(|s| s.contains("\"errors\":1"))
        );
    }

    #[tokio::test]
    async fn spawn_gc_job_for_cache_runs() {
        let (_temp, state) = build_state().await;

        spawn_gc_job_for_cache(None, GcJobType::UploadGc, "upload_gc", &state)
            .await
            .unwrap();

        let job = state
            .metadata
            .get_recent_gc_jobs(None, 1)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let row = state
                .metadata
                .get_gc_job(job.gc_job_id)
                .await
                .unwrap()
                .unwrap();
            if row.state == GcJobState::Finished.as_str()
                || row.state == GcJobState::Failed.as_str()
            {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("GC job did not finish in time");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn spawn_gc_job_for_cache_skips_duplicate() {
        let (_temp, state) = build_state().await;
        let metadata = state.metadata.clone();

        let job = GcJobRow {
            gc_job_id: uuid::Uuid::new_v4(),
            cache_id: None,
            job_type: GcJobType::UploadGc.as_str().to_string(),
            state: GcJobState::Running.as_str().to_string(),
            started_at: Some(OffsetDateTime::now_utc()),
            finished_at: None,
            stats_json: None,
            sweep_checkpoint_json: None,
        };
        metadata.create_gc_job(&job).await.unwrap();

        spawn_gc_job_for_cache(None, GcJobType::UploadGc, "upload_gc", &state)
            .await
            .unwrap();
    }
}
