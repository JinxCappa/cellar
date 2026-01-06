//! Server test utilities.

use cellar_core::config::{AppConfig, MetadataConfig, ServerConfig, StorageConfig};
use cellar_metadata::{MetadataStore, SqliteStore};
use cellar_server::{create_router, AppState};
use cellar_storage::{FilesystemBackend, ObjectStore};
use std::sync::Arc;
use tempfile::TempDir;

/// A test server wrapper with all dependencies.
/// Note: #[allow(dead_code)] because each test file compiles common/ separately.
#[allow(dead_code)]
pub struct TestServer {
    pub router: axum::Router,
    pub state: AppState,
    _temp_dir: TempDir,
}

#[allow(dead_code)]
impl TestServer {
    /// Create a new test server with temporary storage.
    pub async fn new() -> Self {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");

        // Create storage
        let storage_path = temp_dir.path().join("storage");
        std::fs::create_dir_all(&storage_path).expect("Failed to create storage directory");
        let storage: Arc<dyn ObjectStore> = Arc::new(
            FilesystemBackend::new(&storage_path)
                .await
                .expect("Failed to create storage backend"),
        );

        // Create metadata
        let db_path = temp_dir.path().join("metadata.db");
        let metadata: Arc<dyn MetadataStore> = Arc::new(
            SqliteStore::new(&db_path)
                .await
                .expect("Failed to create metadata store"),
        );

        // Create config
        let config = AppConfig {
            server: ServerConfig::default(),
            storage: StorageConfig::Filesystem {
                path: storage_path.clone(),
            },
            metadata: MetadataConfig::Sqlite { path: db_path },
            signing: None,
            gc: Default::default(),
            rate_limit: Default::default(),
        };

        // Create state
        let state = AppState::new(config, storage, metadata, None);

        // Create router
        let router = create_router(state.clone());

        Self {
            router,
            state,
            _temp_dir: temp_dir,
        }
    }

    /// Get access to the underlying metadata.
    pub fn metadata(&self) -> Arc<dyn MetadataStore> {
        self.state.metadata.clone()
    }
}
