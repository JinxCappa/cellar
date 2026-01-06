//! Object storage abstraction and backends for Cellar.
//!
//! This crate provides:
//! - Content-addressed chunk storage with atomic writes
//! - Manifest storage keyed by manifest hash
//! - Narinfo storage keyed by store path hash
//! - Backends: local filesystem and S3-compatible

pub mod backends;
pub mod context;
pub mod error;
pub mod token;
pub mod traits;

pub use backends::{filesystem::FilesystemBackend, s3::S3Backend};
pub use context::{ListingContext, ListingContextOwned};
pub use error::{StorageError, StorageResult};
pub use traits::{
    ContinuationToken, KeyStream, ListingCapabilities, ListingOptions, ListingPage, ListingResume,
    ObjectStore, ObjectStoreListStreamExt, StreamingUpload,
};

use cellar_core::config::StorageConfig;
use std::sync::Arc;

/// Create an object store from configuration.
pub async fn from_config(config: &StorageConfig) -> StorageResult<Arc<dyn ObjectStore>> {
    config.validate().map_err(StorageError::Config)?;

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
            access_key_id,
            secret_access_key,
            force_path_style,
        } => {
            let backend = S3Backend::new(
                bucket,
                endpoint.clone(),
                region.clone(),
                prefix.clone(),
                access_key_id.clone(),
                secret_access_key.clone(),
                *force_path_style,
            )
            .await?;
            Ok(Arc::new(backend))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use cellar_core::config::StorageConfig;
    use tempfile::tempdir;

    #[tokio::test]
    async fn from_config_filesystem_ok() {
        let temp = tempdir().unwrap();
        let config = StorageConfig::Filesystem {
            path: temp.path().join("store"),
        };

        let store = from_config(&config).await.unwrap();
        store
            .put("hello.txt", Bytes::from_static(b"hi"))
            .await
            .unwrap();
        assert!(store.exists("hello.txt").await.unwrap());
    }

    #[tokio::test]
    async fn from_config_s3_ok() {
        let config = StorageConfig::S3 {
            bucket: "bucket".to_string(),
            endpoint: Some("minio:9000".to_string()),
            region: Some("us-east-1".to_string()),
            prefix: Some("cellar".to_string()),
            access_key_id: None,
            secret_access_key: None,
            force_path_style: true,
        };

        let store = from_config(&config).await.unwrap();
        drop(store);
    }

    #[tokio::test]
    async fn from_config_rejects_partial_credentials() {
        let config = StorageConfig::S3 {
            bucket: "bucket".to_string(),
            endpoint: None,
            region: None,
            prefix: None,
            access_key_id: Some("access".to_string()),
            secret_access_key: None,
            force_path_style: false,
        };

        match from_config(&config).await {
            Ok(_) => panic!("expected error"),
            Err(StorageError::Config(_)) => {}
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
}
