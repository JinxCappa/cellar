//! Cellar server binary.

use anyhow::{Context, Result};
use figment::providers::{Env, Format, Toml};
use figment::Figment;
use cellar_core::config::{AppConfig, PrivateKeyConfig, SigningConfig};
use cellar_server::{create_router, AppState};
use cellar_signer::NarInfoSigner;
use std::net::SocketAddr;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "cellar_server=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load configuration
    let config: AppConfig = Figment::new()
        .merge(Toml::file("config/server.toml"))
        .merge(Env::prefixed("CELLAR_").split("__"))
        .extract()
        .context("failed to load configuration")?;

    tracing::info!("Starting Cellar server");

    // Register Prometheus metrics
    cellar_server::metrics::register_metrics();
    tracing::info!("Prometheus metrics registered");

    // Initialize storage backend
    let storage = cellar_storage::from_config(&config.storage)
        .await
        .context("failed to initialize storage")?;
    tracing::info!("Storage backend initialized");

    // Initialize metadata store
    let metadata = cellar_metadata::from_config(&config.metadata)
        .await
        .context("failed to initialize metadata store")?;
    tracing::info!("Metadata store initialized");

    // Initialize signer if configured
    let signer = if let Some(signing_config) = &config.signing {
        Some(load_signer(signing_config).await?)
    } else {
        tracing::warn!("No signing key configured, narinfo will be unsigned");
        None
    };

    // Create application state
    let state = AppState::new(config.clone(), storage, metadata, signer);

    // Create router
    let app = create_router(state);

    // Parse bind address
    let addr: SocketAddr = config
        .server
        .bind
        .parse()
        .context("invalid bind address")?;

    tracing::info!("Listening on {}", addr);

    // Start server with ConnectInfo for client IP extraction
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;

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
    }
}
