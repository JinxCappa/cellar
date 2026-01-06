//! Object storage abstraction and backends for Cellar.
//!
//! This crate provides:
//! - Content-addressed chunk storage with atomic writes
//! - Manifest storage keyed by manifest hash
//! - Narinfo storage keyed by store path hash
//! - Backends: local filesystem and S3-compatible

pub mod backends;
pub mod error;
pub mod traits;

pub use backends::{filesystem::FilesystemBackend, s3::S3Backend};
pub use error::{StorageError, StorageResult};
pub use traits::{ObjectStore, StreamingUpload};

use cellar_core::config::StorageConfig;
use std::sync::Arc;

/// Create an object store from configuration.
pub async fn from_config(config: &StorageConfig) -> StorageResult<Arc<dyn ObjectStore>> {
    match config {
        StorageConfig::Filesystem { path } => {
            let backend = FilesystemBackend::new(path).await?;
            Ok(Arc::new(backend))
        }
        StorageConfig::S3 {
            bucket,
            endpoint,
            region,
            prefix,
        } => {
            let backend = S3Backend::new(bucket, endpoint.clone(), region.clone(), prefix.clone())
                .await?;
            Ok(Arc::new(backend))
        }
    }
}
