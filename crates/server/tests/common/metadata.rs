//! Metadata store test utilities.

use cellar_metadata::{MetadataResult, MetadataStore, PostgresStore, SqliteStore};
use std::sync::Arc;
use tempfile::TempDir;
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

/// A test metadata store wrapper that cleans up on drop.
pub struct TestMetadata {
    pub store: Arc<dyn MetadataStore>,
    _temp_dir: TempDir,
}

impl TestMetadata {
    /// Create a new test metadata store with in-memory SQLite.
    pub async fn new() -> MetadataResult<Self> {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let db_path = temp_dir.path().join("test.db");
        let store = SqliteStore::new(&db_path).await?;

        Ok(Self {
            store: Arc::new(store),
            _temp_dir: temp_dir,
        })
    }

    /// Create a new in-memory SQLite store (faster for tests).
    #[allow(dead_code)]
    pub async fn in_memory() -> MetadataResult<Self> {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        // SQLite in-memory with shared cache
        let store = SqliteStore::new(":memory:").await?;

        Ok(Self {
            store: Arc::new(store),
            _temp_dir: temp_dir,
        })
    }

    /// Get a reference to the metadata store.
    pub fn store(&self) -> Arc<dyn MetadataStore> {
        self.store.clone()
    }
}

/// PostgreSQL test metadata store wrapper that manages a testcontainer.
pub struct PostgresTestMetadata {
    pub store: Arc<dyn MetadataStore>,
    _container: ContainerAsync<Postgres>,
}

impl PostgresTestMetadata {
    /// Create a new PostgreSQL test store with a testcontainer.
    pub async fn new() -> MetadataResult<Self> {
        // Start PostgreSQL container
        let container = Postgres::default()
            .with_tag("15-alpine")
            .start()
            .await
            .expect("Failed to start PostgreSQL container");

        let host = container.get_host().await.expect("Failed to get host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("Failed to get port");

        // Default credentials from testcontainers-modules postgres
        let url = format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            host, port
        );

        let store = PostgresStore::new(&url, 5).await?;

        Ok(Self {
            store: Arc::new(store),
            _container: container,
        })
    }

    /// Get a reference to the metadata store.
    pub fn store(&self) -> Arc<dyn MetadataStore> {
        self.store.clone()
    }
}

/// Run a test against both SQLite and PostgreSQL backends.
#[allow(dead_code)]
pub async fn run_metadata_test_both<F, Fut>(test_fn: F)
where
    F: Fn(Arc<dyn MetadataStore>) -> Fut + Clone,
    Fut: std::future::Future<Output = ()>,
{
    // Test with SQLite backend
    let sqlite = TestMetadata::new().await.expect("Failed to create SQLite test metadata");
    test_fn.clone()(sqlite.store()).await;

    // Test with PostgreSQL backend (requires Docker)
    if std::env::var("SKIP_POSTGRES_TESTS").is_err() {
        let postgres = PostgresTestMetadata::new()
            .await
            .expect("Failed to create PostgreSQL test metadata");
        test_fn(postgres.store()).await;
    }
}
