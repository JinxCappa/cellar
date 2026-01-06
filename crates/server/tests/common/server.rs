//! Server test utilities.

use cellar_core::config::{AdminConfig, AppConfig, MetadataConfig, ServerConfig, StorageConfig};
use cellar_core::storage_domain::default_storage_domain_id;
use cellar_metadata::models::CacheRow;
use cellar_metadata::{MetadataStore, SqliteStore};
use cellar_server::{AppState, create_router};
use cellar_storage::{FilesystemBackend, ObjectStore};
use std::sync::Arc;
use tempfile::TempDir;
use time::OffsetDateTime;
use uuid::Uuid;

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
            SqliteStore::new(&db_path, None)
                .await
                .expect("Failed to create metadata store"),
        );

        // Create GC registry (required by AppState::new)
        let gc_task_registry =
            Arc::new(cellar_server::state::GcTaskRegistry::new(metadata.clone()));

        // Create a default public cache so global tokens can upload
        Self::create_default_public_cache(&metadata).await;

        // Create config with test-friendly defaults
        // Disable require_expected_chunks for tests since most tests don't need it
        // and it would require adding expected_chunks to every test
        let server_config = ServerConfig {
            require_expected_chunks: false,
            ..Default::default()
        };

        let config = AppConfig {
            server: server_config,
            storage: StorageConfig::Filesystem {
                path: storage_path.clone(),
            },
            metadata: MetadataConfig::Sqlite {
                path: db_path,
                query_timeout_secs: None,
            },
            signing: None,
            gc: Default::default(),
            rate_limit: Default::default(),
            admin: AdminConfig::for_testing(),
        };

        // Create state
        let state = AppState::new(config, storage, metadata, None, gc_task_registry);

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

    /// Create a default public cache for tests so global tokens can resolve.
    async fn create_default_public_cache(metadata: &Arc<dyn MetadataStore>) {
        let now = OffsetDateTime::now_utc();
        let cache = CacheRow {
            cache_id: Uuid::new_v4(),
            domain_id: default_storage_domain_id(),
            cache_name: "default-public".to_string(),
            public_base_url: None,
            is_public: true,
            is_default: true,
            created_at: now,
            updated_at: now,
        };
        metadata
            .create_cache(&cache)
            .await
            .expect("Failed to create default public cache");
    }

    /// Create a test server with custom config modifications.
    pub async fn with_config<F>(modifier: F) -> Self
    where
        F: FnOnce(&mut AppConfig),
    {
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
            SqliteStore::new(&db_path, None)
                .await
                .expect("Failed to create metadata store"),
        );

        // Create GC registry (required by AppState::new)
        let gc_task_registry =
            Arc::new(cellar_server::state::GcTaskRegistry::new(metadata.clone()));

        // Create a default public cache so global tokens can upload
        Self::create_default_public_cache(&metadata).await;

        // Create config with test-friendly defaults
        // Disable require_expected_chunks for tests since most tests don't need it
        let server_config = ServerConfig {
            require_expected_chunks: false,
            ..Default::default()
        };

        let mut config = AppConfig {
            server: server_config,
            storage: StorageConfig::Filesystem {
                path: storage_path.clone(),
            },
            metadata: MetadataConfig::Sqlite {
                path: db_path,
                query_timeout_secs: None,
            },
            signing: None,
            gc: Default::default(),
            rate_limit: Default::default(),
            admin: AdminConfig::for_testing(),
        };

        // Apply user modifications
        modifier(&mut config);

        // Create state
        let state = AppState::new(config, storage, metadata, None, gc_task_registry);

        // Create router
        let router = create_router(state.clone());

        Self {
            router,
            state,
            _temp_dir: temp_dir,
        }
    }
}
