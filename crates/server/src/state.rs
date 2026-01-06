//! Application state shared across handlers.

use crate::ratelimit::RateLimitState;
use cellar_core::config::AppConfig;
use cellar_metadata::MetadataStore;
use cellar_signer::NarInfoSigner;
use cellar_storage::ObjectStore;
use std::sync::Arc;

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
}

impl AppState {
    /// Create a new application state.
    pub fn new(
        config: AppConfig,
        storage: Arc<dyn ObjectStore>,
        metadata: Arc<dyn MetadataStore>,
        signer: Option<NarInfoSigner>,
    ) -> Self {
        let rate_limit = RateLimitState::new(&config.rate_limit);
        Self {
            config: Arc::new(config),
            storage,
            metadata,
            signer: signer.map(Arc::new),
            rate_limit,
        }
    }
}
