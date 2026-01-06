//! Local filesystem storage backend.

use crate::error::{StorageError, StorageResult};
use crate::traits::{ByteStream, KeyStream, ObjectMeta, ObjectStore, StreamingUpload};
use async_trait::async_trait;
use bytes::Bytes;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::instrument;
use uuid::Uuid;

/// Default chunk size for streaming reads (64 KiB).
const STREAM_CHUNK_SIZE: usize = 64 * 1024;

/// Maximum range size for get_range operations (128 MiB).
/// This prevents large memory allocations from user-controlled range requests.
const MAX_RANGE_SIZE: u64 = 128 * 1024 * 1024;

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
    /// This is an async wrapper around `key_path_sync` that uses `spawn_blocking`
    /// to avoid blocking the Tokio runtime during filesystem operations like
    /// `canonicalize` and `symlink_metadata`.
    async fn key_path(&self, key: &str) -> StorageResult<PathBuf> {
        let root = self.root.clone();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || Self::key_path_sync(&root, &key))
            .await
            .map_err(|e| {
                StorageError::Io(std::io::Error::other(format!("spawn_blocking failed: {e}")))
            })?
    }

    /// Synchronous key path validation with path traversal protection.
    ///
    /// Returns an error if the key would escape the storage root.
    /// This includes protection against symlink-based traversal attacks.
    fn key_path_sync(root: &Path, key: &str) -> StorageResult<PathBuf> {
        // Reject keys with obvious path traversal attempts (fast path)
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

        let path = root.join(key);

        let root_canonical = root.canonicalize().map_err(|e| {
            StorageError::Io(std::io::Error::new(
                e.kind(),
                format!("failed to canonicalize root: {e}"),
            ))
        })?;

        // For existing paths (or symlinks, even if broken), canonicalize and verify
        // they don't escape the root. This catches symlink-based traversal attacks
        // where a symlink inside the storage root points to a location outside of it.
        match std::fs::symlink_metadata(&path) {
            Ok(meta) => {
                let canonical = path.canonicalize().map_err(|e| {
                    if meta.file_type().is_symlink() {
                        StorageError::InvalidKey(format!(
                            "symlink target missing or invalid: {key}"
                        ))
                    } else {
                        StorageError::Io(std::io::Error::new(
                            e.kind(),
                            format!("failed to canonicalize path: {e}"),
                        ))
                    }
                })?;

                if !canonical.starts_with(&root_canonical) {
                    return Err(StorageError::InvalidKey(format!(
                        "resolved path escapes storage root: {key}"
                    )));
                }

                // Return the original path (not canonical) to preserve consistency
                // with root in list operations. The security check above ensures
                // the path is safe.
                return Ok(path);
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(StorageError::Io(std::io::Error::new(
                    err.kind(),
                    format!("failed to stat path: {err}"),
                )));
            }
        }

        // For new paths, find the nearest existing ancestor and verify it's within the root.
        // This prevents creating files through symlinked directories, even when intermediate
        // directories don't exist yet. For example: if root/a -> /tmp/out (symlink), and
        // key is "a/b/file", the parent "root/a/b" doesn't exist, but ancestor "root/a"
        // does and would escape the root via symlink.

        // Walk up the path to find the nearest existing ancestor
        let mut ancestor = path.as_path();
        while let Some(parent) = ancestor.parent() {
            match std::fs::symlink_metadata(parent) {
                Ok(meta) => {
                    let parent_canonical = parent.canonicalize().map_err(|e| {
                        if meta.file_type().is_symlink() {
                            StorageError::InvalidKey(format!(
                                "ancestor symlink target missing or invalid: {key}"
                            ))
                        } else {
                            StorageError::Io(std::io::Error::new(
                                e.kind(),
                                format!("failed to canonicalize ancestor: {e}"),
                            ))
                        }
                    })?;

                    if !parent_canonical.starts_with(&root_canonical) {
                        return Err(StorageError::InvalidKey(format!(
                            "ancestor path escapes storage root: {key}"
                        )));
                    }
                    // Found a valid existing ancestor within root
                    break;
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(StorageError::Io(std::io::Error::new(
                        err.kind(),
                        format!("failed to stat ancestor: {err}"),
                    )));
                }
            }
            ancestor = parent;
        }

        Ok(path)
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
        let path = self.key_path(key).await?;
        fs::try_exists(&path).await.map_err(StorageError::Io)
    }

    #[instrument(skip(self), fields(backend = "filesystem"))]
    async fn head(&self, key: &str) -> StorageResult<ObjectMeta> {
        let path = self.key_path(key).await?;
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
        let path = self.key_path(key).await?;
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

        let path = self.key_path(key).await?;
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

        let range_size = end - start;
        if range_size > MAX_RANGE_SIZE {
            return Err(StorageError::InvalidRange(format!(
                "range size {} exceeds maximum {} bytes",
                range_size, MAX_RANGE_SIZE
            )));
        }

        // Convert range bounds to usize, failing on 32-bit overflow
        let len = usize::try_from(end - start).map_err(|_| {
            StorageError::InvalidRange(format!(
                "range size {} exceeds platform address space",
                end - start
            ))
        })?;

        let path = self.key_path(key).await?;
        let mut file = fs::File::open(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(key.to_string())
            } else {
                StorageError::Io(e)
            }
        })?;

        file.seek(std::io::SeekFrom::Start(start)).await?;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf).await?;

        Ok(Bytes::from(buf))
    }

    #[instrument(skip(self, data), fields(backend = "filesystem", size = data.len()))]
    async fn put(&self, key: &str, data: Bytes) -> StorageResult<()> {
        let path = self.key_path(key).await?;
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
        let path = self.key_path(key).await?;

        // Note: This check-then-write has a race condition between exists() and put().
        // For content-addressed storage (like chunks), this is acceptable because:
        // 1. The content is deterministic based on the key (hash)
        // 2. Concurrent writes of the same key will produce the same content
        // 3. At worst, we do redundant work, but data integrity is preserved
        if fs::try_exists(&path).await.map_err(StorageError::Io)? {
            return Ok(false);
        }

        self.put(key, data).await?;
        Ok(true)
    }

    #[instrument(skip(self), fields(backend = "filesystem"))]
    async fn put_stream(&self, key: &str) -> StorageResult<Box<dyn StreamingUpload>> {
        let path = self.key_path(key).await?;
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
        let path = self.key_path(key).await?;
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
        let base_path = self.key_path(prefix).await?;
        let mut results = Vec::new();

        match fs::try_exists(&base_path).await {
            Ok(false) => return Ok(results),
            Ok(true) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(results),
            Err(e) => return Err(StorageError::Io(e)),
        }

        let mut stack = vec![base_path];
        while let Some(dir) = stack.pop() {
            let mut entries = fs::read_dir(&dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                // Use file_type() instead of path.is_dir() to avoid following symlinks.
                // This prevents directory traversal outside the storage root via symlinks.
                let file_type = entry.file_type().await?;
                if file_type.is_dir() {
                    stack.push(path);
                } else if file_type.is_file()
                    && let Ok(rel) = path.strip_prefix(&self.root)
                {
                    results.push(rel.to_string_lossy().to_string());
                }
                // Ignore symlinks to prevent traversal outside storage root
            }
        }

        Ok(results)
    }

    #[instrument(skip(self), fields(backend = "filesystem"))]
    async fn list_stream(&self, prefix: &str) -> StorageResult<KeyStream> {
        let base_path = self.key_path(prefix).await?;
        let root = self.root.clone();

        // Check existence upfront, propagating IO errors but treating NotFound as empty
        let base_path_exists = match fs::try_exists(&base_path).await {
            Ok(exists) => exists,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => return Err(StorageError::Io(e)),
        };

        // Create a stream that walks the directory tree and yields keys
        let stream = async_stream::try_stream! {
            if !base_path_exists {
                return;
            }

            let mut stack = vec![base_path];
            while let Some(dir) = stack.pop() {
                let mut entries = fs::read_dir(&dir).await?;
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    // Use file_type() instead of path.is_dir() to avoid following symlinks.
                    // This prevents directory traversal outside the storage root via symlinks.
                    let file_type = entry.file_type().await?;
                    if file_type.is_dir() {
                        stack.push(path);
                    } else if file_type.is_file()
                        && let Ok(rel) = path.strip_prefix(&root) {
                            yield rel.to_string_lossy().to_string();
                        }
                    // Ignore symlinks to prevent traversal outside storage root
                }
            }
        };

        Ok(Box::pin(stream))
    }

    #[instrument(skip(self), fields(backend = "filesystem"))]
    async fn copy(&self, from: &str, to: &str) -> StorageResult<()> {
        let from_path = self.key_path(from).await?;
        let to_path = self.key_path(to).await?;
        self.ensure_parent(&to_path).await?;
        fs::copy(&from_path, &to_path).await?;
        Ok(())
    }
    fn backend_name(&self) -> &'static str {
        "filesystem"
    }

    #[instrument(skip(self), fields(backend = "filesystem"))]
    async fn health_check(&self) -> StorageResult<()> {
        // Verify the root directory exists and is accessible
        let metadata = fs::metadata(&self.root).await.map_err(|e| {
            StorageError::Io(std::io::Error::new(
                e.kind(),
                format!("storage root not accessible: {}", e),
            ))
        })?;

        if !metadata.is_dir() {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::NotADirectory,
                format!("storage root is not a directory: {:?}", self.root),
            )));
        }

        Ok(())
    }

    fn listing_capabilities(&self) -> crate::traits::ListingCapabilities {
        // Filesystem listings cannot be resumed from arbitrary positions.
        // We provide page-based batching for memory efficiency, but no resumability.
        crate::traits::ListingCapabilities { resumable: false }
    }

    fn list_pages<'a>(
        &'a self,
        prefix: &str,
        options: crate::traits::ListingOptions,
        resume: Option<crate::traits::ListingResume>,
    ) -> std::pin::Pin<
        Box<dyn futures::Stream<Item = StorageResult<crate::traits::ListingPage>> + Send + 'a>,
    > {
        // Reject resume tokens since filesystem listings are not resumable
        if resume.is_some() {
            return Box::pin(futures::stream::once(async {
                Err(StorageError::ListingNotResumable)
            }));
        }

        let root = self.root.clone();
        let prefix = prefix.to_string();
        let page_size = options.normalized_page_size();

        // Create a stream that walks the directory tree and yields pages of keys
        // key_path validation happens inside the stream since it's now async
        let stream = async_stream::try_stream! {
            let base_path = self.key_path(&prefix).await?;

            // Check existence, propagating IO errors but treating NotFound as empty
            let base_path_exists = match fs::try_exists(&base_path).await {
                Ok(exists) => exists,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
                Err(e) => Err(StorageError::Io(e))?,
            };
            if !base_path_exists {
                return;
            }

            let mut stack = vec![base_path];
            let mut current_page = Vec::with_capacity(page_size);

            while let Some(dir) = stack.pop() {
                let mut entries = fs::read_dir(&dir).await?;
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    // Use file_type() instead of path.is_dir() to avoid following symlinks.
                    // This prevents directory traversal outside the storage root via symlinks.
                    let file_type = entry.file_type().await?;
                    if file_type.is_dir() {
                        stack.push(path);
                    } else if file_type.is_file()
                        && let Ok(rel) = path.strip_prefix(&root) {
                            let key = rel.to_string_lossy().to_string();
                            current_page.push(key);

                            // Yield a page when we reach the target size
                            if current_page.len() >= page_size {
                                let page = crate::traits::ListingPage {
                                    keys: std::mem::replace(&mut current_page, Vec::with_capacity(page_size)),
                                    next_token: None, // Filesystem is not resumable
                                };
                                yield page;
                            }
                        }
                    // Ignore symlinks to prevent traversal outside storage root
                }
            }

            // Yield final partial page if any keys remain
            if !current_page.is_empty() {
                let page = crate::traits::ListingPage {
                    keys: current_page,
                    next_token: None,
                };
                yield page;
            }
        };

        Box::pin(stream)
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

    #[tokio::test]
    #[cfg(unix)]
    async fn test_symlink_traversal_rejected() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let outside_dir = tempfile::tempdir().unwrap();

        // Create a file outside the storage root
        let outside_file = outside_dir.path().join("secret.txt");
        std::fs::write(&outside_file, "secret data").unwrap();

        let backend = FilesystemBackend::new(dir.path()).await.unwrap();

        // Create a symlink inside storage root pointing outside
        let symlink_path = dir.path().join("malicious_link");
        symlink(&outside_file, &symlink_path).unwrap();

        // Attempting to read through the symlink should fail
        let result = backend.get("malicious_link").await;
        assert!(result.is_err(), "symlink traversal should be rejected");

        // Verify the error is about escaping storage root
        if let Err(StorageError::InvalidKey(msg)) = result {
            assert!(
                msg.contains("escapes storage root"),
                "error should mention escaping root: {msg}"
            );
        } else {
            panic!("expected InvalidKey error, got: {result:?}");
        }

        // Also test symlinked directory traversal
        let symlink_dir = dir.path().join("link_to_outside");
        symlink(outside_dir.path(), &symlink_dir).unwrap();

        // Attempting to access a file through the symlinked directory should fail
        let result = backend.get("link_to_outside/secret.txt").await;
        assert!(
            result.is_err(),
            "directory symlink traversal should be rejected"
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn test_ancestor_symlink_traversal_rejected() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let outside_dir = tempfile::tempdir().unwrap();

        let backend = FilesystemBackend::new(dir.path()).await.unwrap();

        // Create a symlink inside storage root pointing to outside directory
        // This simulates: root/escape -> /tmp/outside
        let symlink_path = dir.path().join("escape");
        symlink(outside_dir.path(), &symlink_path).unwrap();

        // Try to write to a nested path where intermediate dirs don't exist
        // Key: "escape/nested/deep/file.txt"
        // - root/escape exists (symlink to outside)
        // - root/escape/nested doesn't exist
        // - root/escape/nested/deep doesn't exist
        // Without proper ancestor checking, create_dir_all would follow the symlink
        // and create directories outside the storage root.
        let result = backend
            .put("escape/nested/deep/file.txt", bytes::Bytes::from("data"))
            .await;

        assert!(
            result.is_err(),
            "ancestor symlink traversal should be rejected on write"
        );

        // Verify the error mentions escaping storage root
        if let Err(StorageError::InvalidKey(msg)) = result {
            assert!(
                msg.contains("escapes storage root"),
                "error should mention escaping root: {msg}"
            );
        } else {
            panic!("expected InvalidKey error, got: {result:?}");
        }

        // Verify nothing was created outside the storage root
        assert!(
            !outside_dir.path().join("nested").exists(),
            "should not have created directories outside storage root"
        );
    }
}
