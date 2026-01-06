//! Storage test utilities.

use cellar_storage::{FilesystemBackend, ObjectStore, StorageResult};
use std::sync::Arc;
use tempfile::TempDir;

/// A test storage wrapper that cleans up on drop.
/// Note: #[allow(dead_code)] because each test file compiles common/ separately.
#[allow(dead_code)]
pub struct TestStorage {
    pub backend: Arc<dyn ObjectStore>,
    _temp_dir: TempDir,
}

#[allow(dead_code)]
impl TestStorage {
    /// Create a new test storage with a temporary directory.
    pub async fn new() -> StorageResult<Self> {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let backend = FilesystemBackend::new(temp_dir.path()).await?;

        Ok(Self {
            backend: Arc::new(backend),
            _temp_dir: temp_dir,
        })
    }

    /// Get a reference to the object store.
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        self.backend.clone()
    }
}
