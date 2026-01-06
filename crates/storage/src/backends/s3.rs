//! S3-compatible storage backend.

use crate::error::{StorageError, StorageResult};
use crate::traits::{ByteStream, ObjectMeta, ObjectStore, StreamingUpload};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore as ObjStore;
use std::sync::Arc;
use tracing::instrument;

/// S3-compatible object store.
pub struct S3Backend {
    store: Arc<dyn ObjStore>,
    prefix: Option<String>,
}

impl S3Backend {
    /// Create a new S3 backend.
    pub async fn new(
        bucket: &str,
        endpoint: Option<String>,
        region: Option<String>,
        prefix: Option<String>,
    ) -> StorageResult<Self> {
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);

        if let Some(endpoint) = endpoint {
            builder = builder.with_endpoint(&endpoint).with_allow_http(true);
        }

        if let Some(region) = region {
            builder = builder.with_region(&region);
        }

        let store = builder.build().map_err(|e| {
            StorageError::Config(format!("failed to create S3 client: {e}"))
        })?;

        Ok(Self {
            store: Arc::new(store),
            prefix,
        })
    }

    /// Get the full object path for a key.
    fn object_path(&self, key: &str) -> ObjectPath {
        match &self.prefix {
            Some(prefix) => ObjectPath::from(format!("{prefix}/{key}")),
            None => ObjectPath::from(key),
        }
    }
}

#[async_trait]
impl ObjectStore for S3Backend {
    #[instrument(skip(self), fields(backend = "s3"))]
    async fn exists(&self, key: &str) -> StorageResult<bool> {
        let path = self.object_path(key);
        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(StorageError::ObjectStore(e)),
        }
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn head(&self, key: &str) -> StorageResult<ObjectMeta> {
        let path = self.object_path(key);
        let meta = self.store.head(&path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => StorageError::NotFound(key.to_string()),
            e => StorageError::ObjectStore(e),
        })?;

        // Convert chrono DateTime to time OffsetDateTime via timestamp
        let last_modified = time::OffsetDateTime::from_unix_timestamp(
            meta.last_modified.timestamp()
        ).ok();

        Ok(ObjectMeta {
            size: meta.size as u64,
            last_modified,
            content_type: None,
        })
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn get(&self, key: &str) -> StorageResult<Bytes> {
        let path = self.object_path(key);
        let result = self.store.get(&path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => StorageError::NotFound(key.to_string()),
            e => StorageError::ObjectStore(e),
        })?;

        let bytes = result.bytes().await?;
        Ok(bytes)
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn get_stream(&self, key: &str) -> StorageResult<ByteStream> {
        let path = self.object_path(key);
        let result = self.store.get(&path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => StorageError::NotFound(key.to_string()),
            e => StorageError::ObjectStore(e),
        })?;

        let stream = result.into_stream().map(|r| {
            r.map_err(StorageError::ObjectStore)
        });

        Ok(Box::pin(stream))
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn get_range(&self, key: &str, start: u64, end: u64) -> StorageResult<Bytes> {
        // Validate range parameters to prevent underflow and huge allocations
        if end < start {
            return Err(StorageError::InvalidRange(format!(
                "end ({}) < start ({})",
                end, start
            )));
        }

        let path = self.object_path(key);
        let range = std::ops::Range {
            start: start as usize,
            end: end as usize,
        };

        let result = self.store.get_range(&path, range).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => StorageError::NotFound(key.to_string()),
            e => StorageError::ObjectStore(e),
        })?;

        Ok(result)
    }

    #[instrument(skip(self, data), fields(backend = "s3", size = data.len()))]
    async fn put(&self, key: &str, data: Bytes) -> StorageResult<()> {
        let path = self.object_path(key);
        self.store
            .put(&path, data.into())
            .await
            .map_err(StorageError::ObjectStore)?;
        Ok(())
    }

    #[instrument(skip(self, data), fields(backend = "s3", size = data.len()))]
    async fn put_if_not_exists(&self, key: &str, data: Bytes) -> StorageResult<bool> {
        let path = self.object_path(key);

        // Check if exists first
        match self.store.head(&path).await {
            Ok(_) => return Ok(false),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(StorageError::ObjectStore(e)),
        }

        // Try to put (race condition possible, but acceptable for our use case)
        self.store
            .put(&path, data.into())
            .await
            .map_err(StorageError::ObjectStore)?;
        Ok(true)
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn put_stream(&self, key: &str) -> StorageResult<Box<dyn StreamingUpload>> {
        let path = self.object_path(key);
        let upload = self
            .store
            .put_multipart(&path)
            .await
            .map_err(StorageError::ObjectStore)?;

        Ok(Box::new(S3Upload {
            upload,
            bytes_written: 0,
        }))
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn delete(&self, key: &str) -> StorageResult<()> {
        let path = self.object_path(key);
        self.store
            .delete(&path)
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => StorageError::NotFound(key.to_string()),
                e => StorageError::ObjectStore(e),
            })?;
        Ok(())
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
        let path = self.object_path(prefix);
        let mut results = Vec::new();

        let mut stream = self.store.list(Some(&path));
        while let Some(item) = stream.next().await {
            let meta = item.map_err(StorageError::ObjectStore)?;
            results.push(meta.location.to_string());
        }

        Ok(results)
    }

    #[instrument(skip(self), fields(backend = "s3"))]
    async fn copy(&self, from: &str, to: &str) -> StorageResult<()> {
        let from_path = self.object_path(from);
        let to_path = self.object_path(to);
        self.store
            .copy(&from_path, &to_path)
            .await
            .map_err(StorageError::ObjectStore)?;
        Ok(())
    }
}

/// Streaming upload for S3 backend.
struct S3Upload {
    upload: Box<dyn object_store::MultipartUpload>,
    bytes_written: u64,
}

#[async_trait]
impl StreamingUpload for S3Upload {
    async fn write(&mut self, data: Bytes) -> StorageResult<()> {
        self.upload
            .put_part(data.clone().into())
            .await
            .map_err(StorageError::ObjectStore)?;
        self.bytes_written += data.len() as u64;
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> StorageResult<u64> {
        self.upload
            .complete()
            .await
            .map_err(StorageError::ObjectStore)?;
        Ok(self.bytes_written)
    }

    async fn abort(mut self: Box<Self>) -> StorageResult<()> {
        self.upload.abort().await.map_err(StorageError::ObjectStore)?;
        Ok(())
    }
}
