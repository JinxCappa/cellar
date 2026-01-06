//! Local filesystem storage backend.

use crate::error::{StorageError, StorageResult};
use crate::traits::{ByteStream, ObjectMeta, ObjectStore, StreamingUpload};
use async_trait::async_trait;
use bytes::Bytes;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::instrument;
use uuid::Uuid;

/// Default chunk size for streaming reads (64 KiB).
const STREAM_CHUNK_SIZE: usize = 64 * 1024;

/// Local filesystem object store.
pub struct FilesystemBackend {
    root: PathBuf,
}

impl FilesystemBackend {
    /// Create a new filesystem backend.
    pub async fn new(root: impl AsRef<Path>) -> StorageResult<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).await?;
        Ok(Self { root })
    }

    /// Get the full path for a key, with path traversal protection.
    ///
    /// Returns an error if the key would escape the storage root.
    fn key_path(&self, key: &str) -> StorageResult<PathBuf> {
        // Reject keys with obvious path traversal attempts
        if key.contains("..") || key.starts_with('/') || key.starts_with('\\') {
            return Err(StorageError::InvalidKey(format!(
                "path traversal not allowed: {key}"
            )));
        }

        // Validate all path components are normal (no .., ., root, etc.)
        for component in std::path::Path::new(key).components() {
            match component {
                std::path::Component::Normal(_) => {}
                _ => {
                    return Err(StorageError::InvalidKey(format!(
                        "contains unsafe path component: {key}"
                    )));
                }
            }
        }

        Ok(self.root.join(key))
    }

    /// Ensure parent directory exists.
    async fn ensure_parent(&self, path: &Path) -> StorageResult<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for FilesystemBackend {
    #[instrument(skip(self), fields(backend = "filesystem"))]
    async fn exists(&self, key: &str) -> StorageResult<bool> {
        let path = self.key_path(key)?;
        Ok(path.exists())
    }

    #[instrument(skip(self), fields(backend = "filesystem"))]
    async fn head(&self, key: &str) -> StorageResult<ObjectMeta> {
        let path = self.key_path(key)?;
        let metadata = fs::metadata(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(key.to_string())
            } else {
                StorageError::Io(e)
            }
        })?;

        Ok(ObjectMeta {
            size: metadata.len(),
            last_modified: metadata.modified().ok().map(|t| t.into()),
            content_type: None,
        })
    }

    #[instrument(skip(self), fields(backend = "filesystem"))]
    async fn get(&self, key: &str) -> StorageResult<Bytes> {
        let path = self.key_path(key)?;
        let data = fs::read(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(key.to_string())
            } else {
                StorageError::Io(e)
            }
        })?;
        Ok(Bytes::from(data))
    }

    #[instrument(skip(self), fields(backend = "filesystem"))]
    async fn get_stream(&self, key: &str) -> StorageResult<ByteStream> {
        use tokio::io::AsyncReadExt;

        let path = self.key_path(key)?;
        let file = fs::File::open(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(key.to_string())
            } else {
                StorageError::Io(e)
            }
        })?;

        // Stream the file in chunks instead of loading entirely into memory
        let stream = async_stream::try_stream! {
            let mut file = file;
            let mut buf = vec![0u8; STREAM_CHUNK_SIZE];
            loop {
                let n = file.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                yield Bytes::copy_from_slice(&buf[..n]);
            }
        };

        Ok(Box::pin(stream))
    }

    #[instrument(skip(self), fields(backend = "filesystem"))]
    async fn get_range(&self, key: &str, start: u64, end: u64) -> StorageResult<Bytes> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        // Validate range parameters to prevent underflow and huge allocations
        if end < start {
            return Err(StorageError::InvalidRange(format!(
                "end ({}) < start ({})",
                end, start
            )));
        }

        let path = self.key_path(key)?;
        let mut file = fs::File::open(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(key.to_string())
            } else {
                StorageError::Io(e)
            }
        })?;

        file.seek(std::io::SeekFrom::Start(start)).await?;
        let len = (end - start) as usize;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf).await?;

        Ok(Bytes::from(buf))
    }

    #[instrument(skip(self, data), fields(backend = "filesystem", size = data.len()))]
    async fn put(&self, key: &str, data: Bytes) -> StorageResult<()> {
        let path = self.key_path(key)?;
        self.ensure_parent(&path).await?;

        // Write to temp file with unique name, fsync, then rename for atomicity and durability
        // Use UUID to avoid conflicts during concurrent writes to the same key
        let temp_name = format!(".tmp.{}", Uuid::new_v4());
        let temp_path = path.with_file_name(
            path.file_name()
                .map(|n| format!("{}{}", n.to_string_lossy(), temp_name))
                .unwrap_or_else(|| temp_name.clone()),
        );
        {
            let mut file = fs::File::create(&temp_path).await?;
            file.write_all(&data).await?;
            // Ensure data is flushed to disk before rename
            file.sync_all().await?;
        }
        fs::rename(&temp_path, &path).await?;

        Ok(())
    }

    #[instrument(skip(self, data), fields(backend = "filesystem", size = data.len()))]
    async fn put_if_not_exists(&self, key: &str, data: Bytes) -> StorageResult<bool> {
        let path = self.key_path(key)?;

        // Note: This check-then-write has a race condition between exists() and put().
        // For content-addressed storage (like chunks), this is acceptable because:
        // 1. The content is deterministic based on the key (hash)
        // 2. Concurrent writes of the same key will produce the same content
        // 3. At worst, we do redundant work, but data integrity is preserved
        if path.exists() {
            return Ok(false);
        }

        self.put(key, data).await?;
        Ok(true)
    }

    #[instrument(skip(self), fields(backend = "filesystem"))]
    async fn put_stream(&self, key: &str) -> StorageResult<Box<dyn StreamingUpload>> {
        let path = self.key_path(key)?;
        self.ensure_parent(&path).await?;

        // Use UUID to avoid conflicts during concurrent streaming writes to the same key
        let temp_name = format!(".tmp.{}", Uuid::new_v4());
        let temp_path = path.with_file_name(
            path.file_name()
                .map(|n| format!("{}{}", n.to_string_lossy(), temp_name))
                .unwrap_or_else(|| temp_name.clone()),
        );
        let file = fs::File::create(&temp_path).await?;

        Ok(Box::new(FilesystemUpload {
            file,
            temp_path,
            final_path: path,
            bytes_written: 0,
        }))
    }

    #[instrument(skip(self), fields(backend = "filesystem"))]
    async fn delete(&self, key: &str) -> StorageResult<()> {
        let path = self.key_path(key)?;
        fs::remove_file(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(key.to_string())
            } else {
                StorageError::Io(e)
            }
        })?;
        Ok(())
    }

    #[instrument(skip(self), fields(backend = "filesystem"))]
    async fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
        let base_path = self.key_path(prefix)?;
        let mut results = Vec::new();

        if !base_path.exists() {
            return Ok(results);
        }

        let mut stack = vec![base_path];
        while let Some(dir) = stack.pop() {
            let mut entries = fs::read_dir(&dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if let Ok(rel) = path.strip_prefix(&self.root) {
                    results.push(rel.to_string_lossy().to_string());
                }
            }
        }

        Ok(results)
    }

    #[instrument(skip(self), fields(backend = "filesystem"))]
    async fn copy(&self, from: &str, to: &str) -> StorageResult<()> {
        let from_path = self.key_path(from)?;
        let to_path = self.key_path(to)?;
        self.ensure_parent(&to_path).await?;
        fs::copy(&from_path, &to_path).await?;
        Ok(())
    }
}

/// Streaming upload for filesystem backend.
struct FilesystemUpload {
    file: fs::File,
    temp_path: PathBuf,
    final_path: PathBuf,
    bytes_written: u64,
}

#[async_trait]
impl StreamingUpload for FilesystemUpload {
    async fn write(&mut self, data: Bytes) -> StorageResult<()> {
        self.file.write_all(&data).await?;
        self.bytes_written += data.len() as u64;
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> StorageResult<u64> {
        // Ensure all data is flushed to disk before rename
        self.file.sync_all().await?;
        drop(self.file);
        fs::rename(&self.temp_path, &self.final_path).await?;
        Ok(self.bytes_written)
    }

    async fn abort(self: Box<Self>) -> StorageResult<()> {
        drop(self.file);
        let _ = fs::remove_file(&self.temp_path).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_get_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(dir.path()).await.unwrap();

        let key = "test/object";
        let data = Bytes::from("hello world");

        backend.put(key, data.clone()).await.unwrap();
        assert!(backend.exists(key).await.unwrap());

        let retrieved = backend.get(key).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_put_if_not_exists() {
        let dir = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(dir.path()).await.unwrap();

        let key = "test/unique";
        let data1 = Bytes::from("first");
        let data2 = Bytes::from("second");

        assert!(backend.put_if_not_exists(key, data1.clone()).await.unwrap());
        assert!(!backend.put_if_not_exists(key, data2).await.unwrap());

        let retrieved = backend.get(key).await.unwrap();
        assert_eq!(retrieved, data1);
    }

    #[tokio::test]
    async fn test_path_traversal_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(dir.path()).await.unwrap();

        // Test various path traversal attempts
        assert!(backend.exists("../escape").await.is_err());
        assert!(backend.exists("/absolute/path").await.is_err());
        assert!(backend.exists("foo/../bar").await.is_err());
        assert!(backend.exists("foo/../../etc/passwd").await.is_err());

        // Valid keys should work
        assert!(backend.exists("valid/nested/key").await.is_ok());
    }
}
