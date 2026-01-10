//! Metadata store trait and implementations.

use crate::error::MetadataResult;
use crate::repos::{
    CacheRepo, ChunkRepo, GcRepo, ManifestRepo, ReachabilityRepo, StorePathRepo, TokenRepo,
    TombstoneRepo, TrustedKeyRepo, UploadRepo,
};
use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, Sqlite};
use std::path::Path;
use std::str::FromStr;

/// Combined metadata store trait.
#[async_trait]
pub trait MetadataStore:
    UploadRepo
    + ChunkRepo
    + ManifestRepo
    + StorePathRepo
    + TokenRepo
    + GcRepo
    + CacheRepo
    + TrustedKeyRepo
    + TombstoneRepo
    + ReachabilityRepo
    + Send
    + Sync
{
    /// Run database migrations.
    async fn migrate(&self) -> MetadataResult<()>;

    /// Begin a transaction (placeholder for future use).
    async fn health_check(&self) -> MetadataResult<()>;
}

/// SQLite-based metadata store.
pub struct SqliteStore {
    pool: Pool<Sqlite>,
}

impl SqliteStore {
    /// Create a new SQLite store.
    pub async fn new(path: impl AsRef<Path>) -> MetadataResult<Self> {
        let path = path.as_ref();

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let opts = SqliteConnectOptions::from_str(&format!("sqlite:{}?mode=rwc", path.display()))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .foreign_keys(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect_with(opts)
            .await?;

        let store = Self { pool };
        store.migrate().await?;

        Ok(store)
    }

    /// Get a reference to the connection pool.
    pub fn pool(&self) -> &Pool<Sqlite> {
        &self.pool
    }
}

#[async_trait]
impl MetadataStore for SqliteStore {
    async fn migrate(&self) -> MetadataResult<()> {
        sqlx::query(SCHEMA_SQL).execute(&self.pool).await?;
        Ok(())
    }

    async fn health_check(&self) -> MetadataResult<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }
}

// Implement all the repository traits for SqliteStore
mod sqlite_impl {
    use super::*;
    use crate::models::*;
    use crate::repos::chunks::ChunkStats;
    use time::OffsetDateTime;
    use uuid::Uuid;

    #[async_trait]
    impl UploadRepo for SqliteStore {
        async fn create_session(&self, session: &UploadSessionRow) -> MetadataResult<()> {
            sqlx::query(
                r#"
                INSERT INTO upload_sessions (
                    upload_id, cache_id, store_path, store_path_hash,
                    nar_size, nar_hash, chunk_size, manifest_hash, state,
                    owner_token_id, created_at, updated_at, expires_at, trace_id,
                    error_code, error_detail
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(session.upload_id)
            .bind(session.cache_id)
            .bind(&session.store_path)
            .bind(&session.store_path_hash)
            .bind(session.nar_size)
            .bind(&session.nar_hash)
            .bind(session.chunk_size)
            .bind(&session.manifest_hash)
            .bind(&session.state)
            .bind(session.owner_token_id)
            .bind(session.created_at)
            .bind(session.updated_at)
            .bind(session.expires_at)
            .bind(&session.trace_id)
            .bind(&session.error_code)
            .bind(&session.error_detail)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn get_session(&self, upload_id: Uuid) -> MetadataResult<Option<UploadSessionRow>> {
            let row = sqlx::query_as::<_, UploadSessionRow>(
                "SELECT * FROM upload_sessions WHERE upload_id = ?",
            )
            .bind(upload_id)
            .fetch_optional(&self.pool)
            .await?;
            Ok(row)
        }

        async fn get_session_by_store_path(
            &self,
            cache_id: Option<Uuid>,
            store_path_hash: &str,
        ) -> MetadataResult<Option<UploadSessionRow>> {
            // Filter by cache_id to ensure tenant isolation
            let row = match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, UploadSessionRow>(
                        "SELECT * FROM upload_sessions WHERE cache_id = ? AND store_path_hash = ? AND state = 'open'",
                    )
                    .bind(id)
                    .bind(store_path_hash)
                    .fetch_optional(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as::<_, UploadSessionRow>(
                        "SELECT * FROM upload_sessions WHERE cache_id IS NULL AND store_path_hash = ? AND state = 'open'",
                    )
                    .bind(store_path_hash)
                    .fetch_optional(&self.pool)
                    .await?
                }
            };
            Ok(row)
        }

        async fn update_state(
            &self,
            upload_id: Uuid,
            state: &str,
            updated_at: OffsetDateTime,
        ) -> MetadataResult<()> {
            sqlx::query("UPDATE upload_sessions SET state = ?, updated_at = ? WHERE upload_id = ?")
                .bind(state)
                .bind(updated_at)
                .bind(upload_id)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn set_manifest_hash(
            &self,
            upload_id: Uuid,
            manifest_hash: &str,
        ) -> MetadataResult<()> {
            sqlx::query("UPDATE upload_sessions SET manifest_hash = ? WHERE upload_id = ?")
                .bind(manifest_hash)
                .bind(upload_id)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn add_expected_chunks(
            &self,
            chunks: &[UploadExpectedChunkRow],
        ) -> MetadataResult<()> {
            for chunk in chunks {
                sqlx::query(
                    r#"
                    INSERT INTO upload_expected_chunks (upload_id, position, chunk_hash, size_bytes)
                    VALUES (?, ?, ?, ?)
                    "#,
                )
                .bind(chunk.upload_id)
                .bind(chunk.position)
                .bind(&chunk.chunk_hash)
                .bind(chunk.size_bytes)
                .execute(&self.pool)
                .await?;
            }
            Ok(())
        }

        async fn mark_chunk_received(
            &self,
            upload_id: Uuid,
            chunk_hash: &str,
            received_at: OffsetDateTime,
        ) -> MetadataResult<()> {
            sqlx::query(
                "UPDATE upload_expected_chunks SET received_at = ? WHERE upload_id = ? AND chunk_hash = ?",
            )
            .bind(received_at)
            .bind(upload_id)
            .bind(chunk_hash)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn get_expected_chunks(
            &self,
            upload_id: Uuid,
        ) -> MetadataResult<Vec<UploadExpectedChunkRow>> {
            let rows = sqlx::query_as::<_, UploadExpectedChunkRow>(
                "SELECT * FROM upload_expected_chunks WHERE upload_id = ? ORDER BY position",
            )
            .bind(upload_id)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows)
        }

        async fn get_received_chunks(&self, upload_id: Uuid) -> MetadataResult<Vec<String>> {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT chunk_hash FROM upload_expected_chunks WHERE upload_id = ? AND received_at IS NOT NULL ORDER BY position",
            )
            .bind(upload_id)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows.into_iter().map(|r| r.0).collect())
        }

        async fn get_missing_chunks(&self, upload_id: Uuid) -> MetadataResult<Vec<String>> {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT chunk_hash FROM upload_expected_chunks WHERE upload_id = ? AND received_at IS NULL ORDER BY position",
            )
            .bind(upload_id)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows.into_iter().map(|r| r.0).collect())
        }

        async fn get_expired_sessions(&self, limit: u32) -> MetadataResult<Vec<UploadSessionRow>> {
            let rows = sqlx::query_as::<_, UploadSessionRow>(
                "SELECT * FROM upload_sessions WHERE state = 'open' AND expires_at < ? LIMIT ?",
            )
            .bind(OffsetDateTime::now_utc())
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows)
        }

        async fn delete_session(&self, upload_id: Uuid) -> MetadataResult<()> {
            sqlx::query("DELETE FROM upload_expected_chunks WHERE upload_id = ?")
                .bind(upload_id)
                .execute(&self.pool)
                .await?;
            sqlx::query("DELETE FROM upload_sessions WHERE upload_id = ?")
                .bind(upload_id)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn fail_session(
            &self,
            upload_id: Uuid,
            error_code: &str,
            error_detail: Option<&str>,
            failed_at: OffsetDateTime,
        ) -> MetadataResult<()> {
            sqlx::query(
                "UPDATE upload_sessions SET state = 'failed', error_code = ?, error_detail = ?, updated_at = ? WHERE upload_id = ?",
            )
            .bind(error_code)
            .bind(error_detail)
            .bind(failed_at)
            .bind(upload_id)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn get_failed_sessions_by_error(
            &self,
            error_code: &str,
            limit: u32,
        ) -> MetadataResult<Vec<UploadSessionRow>> {
            let rows = sqlx::query_as::<_, UploadSessionRow>(
                "SELECT * FROM upload_sessions WHERE state = 'failed' AND error_code = ? ORDER BY updated_at DESC LIMIT ?",
            )
            .bind(error_code)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows)
        }
    }

    #[async_trait]
    impl ChunkRepo for SqliteStore {
        async fn upsert_chunk(&self, chunk: &ChunkRow) -> MetadataResult<()> {
            sqlx::query(
                r#"
                INSERT INTO chunks (chunk_hash, size_bytes, object_key, refcount, created_at, last_accessed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(chunk_hash) DO UPDATE SET last_accessed_at = excluded.last_accessed_at
                "#,
            )
            .bind(&chunk.chunk_hash)
            .bind(chunk.size_bytes)
            .bind(&chunk.object_key)
            .bind(chunk.refcount)
            .bind(chunk.created_at)
            .bind(chunk.last_accessed_at)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn get_chunk(&self, chunk_hash: &str) -> MetadataResult<Option<ChunkRow>> {
            let row =
                sqlx::query_as::<_, ChunkRow>("SELECT * FROM chunks WHERE chunk_hash = ?")
                    .bind(chunk_hash)
                    .fetch_optional(&self.pool)
                    .await?;
            Ok(row)
        }

        async fn chunk_exists(&self, chunk_hash: &str) -> MetadataResult<bool> {
            let row: Option<(i32,)> =
                sqlx::query_as("SELECT 1 FROM chunks WHERE chunk_hash = ?")
                    .bind(chunk_hash)
                    .fetch_optional(&self.pool)
                    .await?;
            Ok(row.is_some())
        }

        async fn increment_refcount(&self, chunk_hash: &str) -> MetadataResult<()> {
            sqlx::query("UPDATE chunks SET refcount = refcount + 1 WHERE chunk_hash = ?")
                .bind(chunk_hash)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn decrement_refcount(&self, chunk_hash: &str) -> MetadataResult<()> {
            sqlx::query(
                "UPDATE chunks SET refcount = MAX(0, refcount - 1) WHERE chunk_hash = ?",
            )
            .bind(chunk_hash)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn touch_chunk(
            &self,
            chunk_hash: &str,
            accessed_at: OffsetDateTime,
        ) -> MetadataResult<()> {
            sqlx::query("UPDATE chunks SET last_accessed_at = ? WHERE chunk_hash = ?")
                .bind(accessed_at)
                .bind(chunk_hash)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn get_unreferenced_chunks(
            &self,
            older_than: OffsetDateTime,
            limit: u32,
        ) -> MetadataResult<Vec<ChunkRow>> {
            let rows = sqlx::query_as::<_, ChunkRow>(
                "SELECT * FROM chunks WHERE refcount = 0 AND created_at < ? LIMIT ?",
            )
            .bind(older_than)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows)
        }

        async fn delete_chunk(&self, chunk_hash: &str) -> MetadataResult<()> {
            sqlx::query("DELETE FROM chunks WHERE chunk_hash = ?")
                .bind(chunk_hash)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn get_stats(&self) -> MetadataResult<ChunkStats> {
            let total: (i64, i64) = sqlx::query_as(
                "SELECT COUNT(*), COALESCE(SUM(size_bytes), 0) FROM chunks",
            )
            .fetch_one(&self.pool)
            .await?;

            let referenced: (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM chunks WHERE refcount > 0",
            )
            .fetch_one(&self.pool)
            .await?;

            Ok(ChunkStats {
                count: total.0 as u64,
                total_size: total.1 as u64,
                referenced_count: referenced.0 as u64,
                unreferenced_count: (total.0 - referenced.0) as u64,
            })
        }
    }

    #[async_trait]
    impl ManifestRepo for SqliteStore {
        async fn create_manifest(
            &self,
            manifest: &ManifestRow,
            chunks: &[ManifestChunkRow],
        ) -> MetadataResult<bool> {
            // Use a transaction to ensure atomicity: both the manifest and all its
            // chunk mappings are inserted together, or neither is.
            let mut tx = self.pool.begin().await?;

            // Use INSERT OR IGNORE to handle concurrent creates atomically.
            // This eliminates the TOCTOU race condition from check-then-insert.
            let result = sqlx::query(
                r#"
                INSERT OR IGNORE INTO manifests (manifest_hash, chunk_size, chunk_count, nar_size, object_key, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(&manifest.manifest_hash)
            .bind(manifest.chunk_size)
            .bind(manifest.chunk_count)
            .bind(manifest.nar_size)
            .bind(&manifest.object_key)
            .bind(manifest.created_at)
            .execute(&mut *tx)
            .await?;

            // If no rows were affected, the manifest already exists
            if result.rows_affected() == 0 {
                tx.rollback().await?;
                return Ok(false);
            }

            // Manifest was created, now insert chunk mappings
            for chunk in chunks {
                sqlx::query(
                    "INSERT INTO manifest_chunks (manifest_hash, position, chunk_hash) VALUES (?, ?, ?)",
                )
                .bind(&chunk.manifest_hash)
                .bind(chunk.position)
                .bind(&chunk.chunk_hash)
                .execute(&mut *tx)
                .await?;
            }

            tx.commit().await?;
            Ok(true)
        }

        async fn get_manifest(&self, manifest_hash: &str) -> MetadataResult<Option<ManifestRow>> {
            let row = sqlx::query_as::<_, ManifestRow>(
                "SELECT * FROM manifests WHERE manifest_hash = ?",
            )
            .bind(manifest_hash)
            .fetch_optional(&self.pool)
            .await?;
            Ok(row)
        }

        async fn manifest_exists(&self, manifest_hash: &str) -> MetadataResult<bool> {
            let row: Option<(i32,)> =
                sqlx::query_as("SELECT 1 FROM manifests WHERE manifest_hash = ?")
                    .bind(manifest_hash)
                    .fetch_optional(&self.pool)
                    .await?;
            Ok(row.is_some())
        }

        async fn get_manifest_chunks(&self, manifest_hash: &str) -> MetadataResult<Vec<String>> {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT chunk_hash FROM manifest_chunks WHERE manifest_hash = ? ORDER BY position",
            )
            .bind(manifest_hash)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows.into_iter().map(|r| r.0).collect())
        }

        async fn get_manifests_using_chunk(&self, chunk_hash: &str) -> MetadataResult<Vec<String>> {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT DISTINCT manifest_hash FROM manifest_chunks WHERE chunk_hash = ?",
            )
            .bind(chunk_hash)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows.into_iter().map(|r| r.0).collect())
        }

        async fn delete_manifest(&self, manifest_hash: &str) -> MetadataResult<()> {
            sqlx::query("DELETE FROM manifest_chunks WHERE manifest_hash = ?")
                .bind(manifest_hash)
                .execute(&self.pool)
                .await?;
            sqlx::query("DELETE FROM manifests WHERE manifest_hash = ?")
                .bind(manifest_hash)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn get_orphaned_manifests(&self, limit: u32) -> MetadataResult<Vec<ManifestRow>> {
            // Only count visible store paths as references. Failed/pending paths should not
            // prevent GC of manifests, as they represent incomplete or failed uploads.
            let rows = sqlx::query_as::<_, ManifestRow>(
                r#"
                SELECT m.* FROM manifests m
                LEFT JOIN store_paths sp ON m.manifest_hash = sp.manifest_hash
                    AND sp.visibility_state = 'visible'
                WHERE sp.store_path_hash IS NULL
                ORDER BY m.created_at
                LIMIT ?
                "#,
            )
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows)
        }
    }

    #[async_trait]
    impl StorePathRepo for SqliteStore {
        async fn create_store_path(&self, store_path: &StorePathRow) -> MetadataResult<()> {
            // Use UPSERT with the unique index on (COALESCE(cache_id, zeroblob(16)), store_path_hash).
            // CRITICAL: cache_id is NOT updated on conflict - this prevents tenant hijacking where
            // an attacker could overwrite another tenant's store path by uploading the same hash.
            sqlx::query(
                r#"
                INSERT INTO store_paths (
                    cache_id, store_path_hash, store_path, nar_hash, nar_size, manifest_hash,
                    created_at, committed_at, visibility_state, uploader_token_id, ca
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(COALESCE(cache_id, zeroblob(16)), store_path_hash) DO UPDATE SET
                    nar_hash = excluded.nar_hash,
                    nar_size = excluded.nar_size,
                    manifest_hash = excluded.manifest_hash,
                    committed_at = excluded.committed_at,
                    visibility_state = excluded.visibility_state,
                    ca = excluded.ca
                "#,
            )
            .bind(store_path.cache_id)
            .bind(&store_path.store_path_hash)
            .bind(&store_path.store_path)
            .bind(&store_path.nar_hash)
            .bind(store_path.nar_size)
            .bind(&store_path.manifest_hash)
            .bind(store_path.created_at)
            .bind(store_path.committed_at)
            .bind(&store_path.visibility_state)
            .bind(store_path.uploader_token_id)
            .bind(&store_path.ca)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn get_store_path(
            &self,
            cache_id: Option<Uuid>,
            store_path_hash: &str,
        ) -> MetadataResult<Option<StorePathRow>> {
            // Filter by cache_id for tenant isolation
            let row = match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, StorePathRow>(
                        "SELECT * FROM store_paths WHERE cache_id = ? AND store_path_hash = ?",
                    )
                    .bind(id)
                    .bind(store_path_hash)
                    .fetch_optional(&self.pool)
                    .await?
                }
                None => {
                    // None cache_id means either admin access or public cache
                    // For public caches, we look for cache_id IS NULL
                    sqlx::query_as::<_, StorePathRow>(
                        "SELECT * FROM store_paths WHERE cache_id IS NULL AND store_path_hash = ?",
                    )
                    .bind(store_path_hash)
                    .fetch_optional(&self.pool)
                    .await?
                }
            };
            Ok(row)
        }

        async fn store_path_visible(
            &self,
            cache_id: Option<Uuid>,
            store_path_hash: &str,
        ) -> MetadataResult<bool> {
            // Filter by cache_id for tenant isolation
            let row: Option<(i32,)> = match cache_id {
                Some(id) => {
                    sqlx::query_as(
                        "SELECT 1 FROM store_paths WHERE cache_id = ? AND store_path_hash = ? AND visibility_state = 'visible'",
                    )
                    .bind(id)
                    .bind(store_path_hash)
                    .fetch_optional(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as(
                        "SELECT 1 FROM store_paths WHERE cache_id IS NULL AND store_path_hash = ? AND visibility_state = 'visible'",
                    )
                    .bind(store_path_hash)
                    .fetch_optional(&self.pool)
                    .await?
                }
            };
            Ok(row.is_some())
        }

        async fn update_visibility(
            &self,
            cache_id: Option<Uuid>,
            store_path_hash: &str,
            visibility: &str,
        ) -> MetadataResult<()> {
            // Filter by cache_id for tenant isolation
            match cache_id {
                Some(id) => {
                    sqlx::query("UPDATE store_paths SET visibility_state = ? WHERE cache_id = ? AND store_path_hash = ?")
                        .bind(visibility)
                        .bind(id)
                        .bind(store_path_hash)
                        .execute(&self.pool)
                        .await?;
                }
                None => {
                    sqlx::query("UPDATE store_paths SET visibility_state = ? WHERE cache_id IS NULL AND store_path_hash = ?")
                        .bind(visibility)
                        .bind(store_path_hash)
                        .execute(&self.pool)
                        .await?;
                }
            }
            Ok(())
        }

        async fn upsert_narinfo(&self, narinfo: &NarInfoRow) -> MetadataResult<()> {
            // Use composite key for upsert - narinfo is scoped to cache_id
            sqlx::query(
                r#"
                INSERT INTO narinfo_records (cache_id, store_path_hash, narinfo_object_key, content_hash, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(COALESCE(cache_id, zeroblob(16)), store_path_hash) DO UPDATE SET
                    narinfo_object_key = excluded.narinfo_object_key,
                    content_hash = excluded.content_hash,
                    updated_at = excluded.updated_at
                "#,
            )
            .bind(narinfo.cache_id)
            .bind(&narinfo.store_path_hash)
            .bind(&narinfo.narinfo_object_key)
            .bind(&narinfo.content_hash)
            .bind(narinfo.created_at)
            .bind(narinfo.updated_at)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn get_narinfo(
            &self,
            cache_id: Option<Uuid>,
            store_path_hash: &str,
        ) -> MetadataResult<Option<NarInfoRow>> {
            // Query narinfo_records directly since it has its own cache_id column
            let row = match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, NarInfoRow>(
                        r#"
                        SELECT * FROM narinfo_records
                        WHERE cache_id = ? AND store_path_hash = ?
                        "#,
                    )
                    .bind(id)
                    .bind(store_path_hash)
                    .fetch_optional(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as::<_, NarInfoRow>(
                        r#"
                        SELECT * FROM narinfo_records
                        WHERE cache_id IS NULL AND store_path_hash = ?
                        "#,
                    )
                    .bind(store_path_hash)
                    .fetch_optional(&self.pool)
                    .await?
                }
            };
            Ok(row)
        }

        async fn add_signature(&self, signature: &SignatureRow) -> MetadataResult<()> {
            // Use composite key - signatures are scoped to cache_id
            sqlx::query(
                r#"
                INSERT INTO signatures (cache_id, store_path_hash, key_id, signature, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(COALESCE(cache_id, zeroblob(16)), store_path_hash, key_id) DO UPDATE SET signature = excluded.signature
                "#,
            )
            .bind(signature.cache_id)
            .bind(&signature.store_path_hash)
            .bind(signature.key_id)
            .bind(&signature.signature)
            .bind(signature.created_at)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn get_signatures(
            &self,
            cache_id: Option<Uuid>,
            store_path_hash: &str,
        ) -> MetadataResult<Vec<SignatureRow>> {
            // Filter by cache_id for tenant isolation
            let rows = match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, SignatureRow>(
                        "SELECT * FROM signatures WHERE cache_id = ? AND store_path_hash = ?",
                    )
                    .bind(id)
                    .bind(store_path_hash)
                    .fetch_all(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as::<_, SignatureRow>(
                        "SELECT * FROM signatures WHERE cache_id IS NULL AND store_path_hash = ?",
                    )
                    .bind(store_path_hash)
                    .fetch_all(&self.pool)
                    .await?
                }
            };
            Ok(rows)
        }

        async fn delete_store_path(
            &self,
            cache_id: Option<Uuid>,
            store_path_hash: &str,
        ) -> MetadataResult<()> {
            // Delete child records first (using composite key), then store_paths
            // FK cascade should handle this, but be explicit for safety
            match cache_id {
                Some(id) => {
                    sqlx::query("DELETE FROM signatures WHERE cache_id = ? AND store_path_hash = ?")
                        .bind(id)
                        .bind(store_path_hash)
                        .execute(&self.pool)
                        .await?;
                    sqlx::query("DELETE FROM narinfo_records WHERE cache_id = ? AND store_path_hash = ?")
                        .bind(id)
                        .bind(store_path_hash)
                        .execute(&self.pool)
                        .await?;
                    sqlx::query("DELETE FROM store_path_references WHERE cache_id = ? AND store_path_hash = ?")
                        .bind(id)
                        .bind(store_path_hash)
                        .execute(&self.pool)
                        .await?;
                    sqlx::query("DELETE FROM store_paths WHERE cache_id = ? AND store_path_hash = ?")
                        .bind(id)
                        .bind(store_path_hash)
                        .execute(&self.pool)
                        .await?;
                }
                None => {
                    sqlx::query("DELETE FROM signatures WHERE cache_id IS NULL AND store_path_hash = ?")
                        .bind(store_path_hash)
                        .execute(&self.pool)
                        .await?;
                    sqlx::query("DELETE FROM narinfo_records WHERE cache_id IS NULL AND store_path_hash = ?")
                        .bind(store_path_hash)
                        .execute(&self.pool)
                        .await?;
                    sqlx::query("DELETE FROM store_path_references WHERE cache_id IS NULL AND store_path_hash = ?")
                        .bind(store_path_hash)
                        .execute(&self.pool)
                        .await?;
                    sqlx::query("DELETE FROM store_paths WHERE cache_id IS NULL AND store_path_hash = ?")
                        .bind(store_path_hash)
                        .execute(&self.pool)
                        .await?;
                }
            }
            Ok(())
        }

        async fn get_store_paths_by_manifest(
            &self,
            cache_id: Option<Uuid>,
            manifest_hash: &str,
        ) -> MetadataResult<Vec<StorePathRow>> {
            // If cache_id is provided, filter by it; otherwise return all (admin use)
            let rows = match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, StorePathRow>(
                        "SELECT * FROM store_paths WHERE cache_id = ? AND manifest_hash = ?",
                    )
                    .bind(id)
                    .bind(manifest_hash)
                    .fetch_all(&self.pool)
                    .await?
                }
                None => {
                    // Admin use - return all store paths for this manifest
                    sqlx::query_as::<_, StorePathRow>(
                        "SELECT * FROM store_paths WHERE manifest_hash = ?",
                    )
                    .bind(manifest_hash)
                    .fetch_all(&self.pool)
                    .await?
                }
            };
            Ok(rows)
        }

        async fn count_store_paths(&self) -> MetadataResult<u64> {
            let (count,): (i64,) =
                sqlx::query_as("SELECT COUNT(*) FROM store_paths WHERE visibility_state = 'visible'")
                    .fetch_one(&self.pool)
                    .await?;
            Ok(count as u64)
        }

        async fn add_references(
            &self,
            cache_id: Option<Uuid>,
            store_path_hash: &str,
            refs: &[StorePathReferenceRow],
        ) -> MetadataResult<()> {
            for r in refs {
                sqlx::query(
                    r#"
                    INSERT INTO store_path_references (cache_id, store_path_hash, reference_hash, reference_type)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(COALESCE(cache_id, zeroblob(16)), store_path_hash, reference_hash, reference_type) DO NOTHING
                    "#,
                )
                .bind(cache_id)
                .bind(store_path_hash)
                .bind(&r.reference_hash)
                .bind(&r.reference_type)
                .execute(&self.pool)
                .await?;
            }
            Ok(())
        }

        async fn get_references(
            &self,
            cache_id: Option<Uuid>,
            store_path_hash: &str,
        ) -> MetadataResult<Vec<String>> {
            let rows: Vec<(String,)> = match cache_id {
                Some(id) => {
                    sqlx::query_as(
                        "SELECT reference_hash FROM store_path_references WHERE cache_id = ? AND store_path_hash = ? AND reference_type = 'reference'",
                    )
                    .bind(id)
                    .bind(store_path_hash)
                    .fetch_all(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as(
                        "SELECT reference_hash FROM store_path_references WHERE cache_id IS NULL AND store_path_hash = ? AND reference_type = 'reference'",
                    )
                    .bind(store_path_hash)
                    .fetch_all(&self.pool)
                    .await?
                }
            };
            Ok(rows.into_iter().map(|r| r.0).collect())
        }

        async fn get_deriver(
            &self,
            cache_id: Option<Uuid>,
            store_path_hash: &str,
        ) -> MetadataResult<Option<String>> {
            let row: Option<(String,)> = match cache_id {
                Some(id) => {
                    sqlx::query_as(
                        "SELECT reference_hash FROM store_path_references WHERE cache_id = ? AND store_path_hash = ? AND reference_type = 'deriver' LIMIT 1",
                    )
                    .bind(id)
                    .bind(store_path_hash)
                    .fetch_optional(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as(
                        "SELECT reference_hash FROM store_path_references WHERE cache_id IS NULL AND store_path_hash = ? AND reference_type = 'deriver' LIMIT 1",
                    )
                    .bind(store_path_hash)
                    .fetch_optional(&self.pool)
                    .await?
                }
            };
            Ok(row.map(|r| r.0))
        }

        async fn get_referrers(
            &self,
            cache_id: Option<Uuid>,
            reference_hash: &str,
        ) -> MetadataResult<Vec<String>> {
            // Filter by cache_id for tenant isolation
            let rows: Vec<(String,)> = match cache_id {
                Some(id) => {
                    sqlx::query_as(
                        "SELECT store_path_hash FROM store_path_references WHERE cache_id = ? AND reference_hash = ?",
                    )
                    .bind(id)
                    .bind(reference_hash)
                    .fetch_all(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as(
                        "SELECT store_path_hash FROM store_path_references WHERE cache_id IS NULL AND reference_hash = ?",
                    )
                    .bind(reference_hash)
                    .fetch_all(&self.pool)
                    .await?
                }
            };
            Ok(rows.into_iter().map(|r| r.0).collect())
        }

        async fn delete_references(
            &self,
            cache_id: Option<Uuid>,
            store_path_hash: &str,
        ) -> MetadataResult<()> {
            match cache_id {
                Some(id) => {
                    sqlx::query("DELETE FROM store_path_references WHERE cache_id = ? AND store_path_hash = ?")
                        .bind(id)
                        .bind(store_path_hash)
                        .execute(&self.pool)
                        .await?;
                }
                None => {
                    sqlx::query("DELETE FROM store_path_references WHERE cache_id IS NULL AND store_path_hash = ?")
                        .bind(store_path_hash)
                        .execute(&self.pool)
                        .await?;
                }
            }
            Ok(())
        }

        async fn delete_failed_store_paths_older_than(
            &self,
            age_seconds: i64,
        ) -> MetadataResult<u64> {
            // Delete failed store paths that are older than the specified age.
            // This allows a grace period for debugging before cleanup.
            // Compute cutoff time in Rust to ensure consistent RFC3339 format
            // (SQLite's datetime() returns 'YYYY-MM-DD HH:MM:SS' which doesn't
            // compare correctly with RFC3339 'YYYY-MM-DDTHH:MM:SSZ' stored by sqlx).
            // Use COALESCE to handle NULL committed_at (failed uploads that never completed).
            let cutoff = OffsetDateTime::now_utc() - time::Duration::seconds(age_seconds);
            let result = sqlx::query(
                r#"
                DELETE FROM store_paths
                WHERE visibility_state = 'failed'
                  AND COALESCE(committed_at, created_at) < ?
                "#,
            )
            .bind(cutoff)
            .execute(&self.pool)
            .await?;
            Ok(result.rows_affected())
        }
    }

    #[async_trait]
    impl TokenRepo for SqliteStore {
        async fn create_token(&self, token: &TokenRow) -> MetadataResult<()> {
            sqlx::query(
                r#"
                INSERT INTO tokens (
                    token_id, cache_id, token_hash, scopes, expires_at,
                    revoked_at, created_at, last_used_at, description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(token.token_id)
            .bind(token.cache_id)
            .bind(&token.token_hash)
            .bind(&token.scopes)
            .bind(token.expires_at)
            .bind(token.revoked_at)
            .bind(token.created_at)
            .bind(token.last_used_at)
            .bind(&token.description)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn get_token_by_hash(&self, token_hash: &str) -> MetadataResult<Option<TokenRow>> {
            let row =
                sqlx::query_as::<_, TokenRow>("SELECT * FROM tokens WHERE token_hash = ?")
                    .bind(token_hash)
                    .fetch_optional(&self.pool)
                    .await?;
            Ok(row)
        }

        async fn get_token(&self, token_id: Uuid) -> MetadataResult<Option<TokenRow>> {
            let row =
                sqlx::query_as::<_, TokenRow>("SELECT * FROM tokens WHERE token_id = ?")
                    .bind(token_id)
                    .fetch_optional(&self.pool)
                    .await?;
            Ok(row)
        }

        async fn touch_token(&self, token_id: Uuid, used_at: OffsetDateTime) -> MetadataResult<()> {
            sqlx::query("UPDATE tokens SET last_used_at = ? WHERE token_id = ?")
                .bind(used_at)
                .bind(token_id)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn revoke_token(
            &self,
            token_id: Uuid,
            revoked_at: OffsetDateTime,
        ) -> MetadataResult<()> {
            sqlx::query("UPDATE tokens SET revoked_at = ? WHERE token_id = ?")
                .bind(revoked_at)
                .bind(token_id)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn delete_token(&self, token_id: Uuid) -> MetadataResult<()> {
            sqlx::query("DELETE FROM tokens WHERE token_id = ?")
                .bind(token_id)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn list_tokens(&self, cache_id: Option<Uuid>) -> MetadataResult<Vec<TokenRow>> {
            let rows = match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, TokenRow>(
                        "SELECT * FROM tokens WHERE cache_id = ? ORDER BY created_at DESC",
                    )
                    .bind(id)
                    .fetch_all(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as::<_, TokenRow>(
                        "SELECT * FROM tokens ORDER BY created_at DESC",
                    )
                    .fetch_all(&self.pool)
                    .await?
                }
            };
            Ok(rows)
        }

        async fn create_signing_key(&self, key: &SigningKeyRow) -> MetadataResult<()> {
            sqlx::query(
                r#"
                INSERT INTO signing_keys (
                    key_id, cache_id, key_name, public_key, private_key_ref, status, created_at, rotated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(key.key_id)
            .bind(key.cache_id)
            .bind(&key.key_name)
            .bind(&key.public_key)
            .bind(&key.private_key_ref)
            .bind(&key.status)
            .bind(key.created_at)
            .bind(key.rotated_at)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn get_active_signing_key(
            &self,
            cache_id: Option<Uuid>,
        ) -> MetadataResult<Option<SigningKeyRow>> {
            let row = match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, SigningKeyRow>(
                        "SELECT * FROM signing_keys WHERE cache_id = ? AND status = 'active' ORDER BY created_at DESC LIMIT 1",
                    )
                    .bind(id)
                    .fetch_optional(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as::<_, SigningKeyRow>(
                        "SELECT * FROM signing_keys WHERE cache_id IS NULL AND status = 'active' ORDER BY created_at DESC LIMIT 1",
                    )
                    .fetch_optional(&self.pool)
                    .await?
                }
            };
            Ok(row)
        }

        async fn get_signing_key(&self, key_id: Uuid) -> MetadataResult<Option<SigningKeyRow>> {
            let row = sqlx::query_as::<_, SigningKeyRow>(
                "SELECT * FROM signing_keys WHERE key_id = ?",
            )
            .bind(key_id)
            .fetch_optional(&self.pool)
            .await?;
            Ok(row)
        }

        async fn get_signing_key_by_name(
            &self,
            key_name: &str,
        ) -> MetadataResult<Option<SigningKeyRow>> {
            let row = sqlx::query_as::<_, SigningKeyRow>(
                "SELECT * FROM signing_keys WHERE key_name = ?",
            )
            .bind(key_name)
            .fetch_optional(&self.pool)
            .await?;
            Ok(row)
        }

        async fn update_signing_key_status(
            &self,
            key_id: Uuid,
            status: &str,
        ) -> MetadataResult<()> {
            sqlx::query("UPDATE signing_keys SET status = ? WHERE key_id = ?")
                .bind(status)
                .bind(key_id)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn list_signing_keys(
            &self,
            cache_id: Option<Uuid>,
        ) -> MetadataResult<Vec<SigningKeyRow>> {
            let rows = match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, SigningKeyRow>(
                        "SELECT * FROM signing_keys WHERE cache_id = ? ORDER BY created_at DESC",
                    )
                    .bind(id)
                    .fetch_all(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as::<_, SigningKeyRow>(
                        "SELECT * FROM signing_keys ORDER BY created_at DESC",
                    )
                    .fetch_all(&self.pool)
                    .await?
                }
            };
            Ok(rows)
        }
    }

    #[async_trait]
    impl GcRepo for SqliteStore {
        async fn create_gc_job(&self, job: &GcJobRow) -> MetadataResult<()> {
            sqlx::query(
                r#"
                INSERT INTO gc_jobs (gc_job_id, cache_id, job_type, state, started_at, finished_at, stats_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(job.gc_job_id)
            .bind(job.cache_id)
            .bind(&job.job_type)
            .bind(&job.state)
            .bind(job.started_at)
            .bind(job.finished_at)
            .bind(&job.stats_json)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn get_gc_job(&self, job_id: Uuid) -> MetadataResult<Option<GcJobRow>> {
            let row =
                sqlx::query_as::<_, GcJobRow>("SELECT * FROM gc_jobs WHERE gc_job_id = ?")
                    .bind(job_id)
                    .fetch_optional(&self.pool)
                    .await?;
            Ok(row)
        }

        async fn update_gc_job_state(
            &self,
            job_id: Uuid,
            state: &str,
            finished_at: Option<OffsetDateTime>,
            stats_json: Option<&str>,
        ) -> MetadataResult<()> {
            sqlx::query(
                "UPDATE gc_jobs SET state = ?, finished_at = ?, stats_json = ? WHERE gc_job_id = ?",
            )
            .bind(state)
            .bind(finished_at)
            .bind(stats_json)
            .bind(job_id)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn get_recent_gc_jobs(&self, limit: u32) -> MetadataResult<Vec<GcJobRow>> {
            let rows = sqlx::query_as::<_, GcJobRow>(
                "SELECT * FROM gc_jobs ORDER BY started_at DESC LIMIT ?",
            )
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows)
        }

        async fn get_running_gc_jobs(&self) -> MetadataResult<Vec<GcJobRow>> {
            let rows = sqlx::query_as::<_, GcJobRow>(
                "SELECT * FROM gc_jobs WHERE state = 'running'",
            )
            .fetch_all(&self.pool)
            .await?;
            Ok(rows)
        }
    }

    // F-001: CacheRepo implementation
    #[async_trait]
    impl crate::repos::CacheRepo for SqliteStore {
        async fn create_cache(&self, cache: &CacheRow) -> MetadataResult<()> {
            sqlx::query(
                r#"
                INSERT INTO caches (cache_id, cache_name, public_base_url, is_public, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(cache.cache_id)
            .bind(&cache.cache_name)
            .bind(&cache.public_base_url)
            .bind(cache.is_public)
            .bind(cache.created_at)
            .bind(cache.updated_at)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn get_cache(&self, cache_id: Uuid) -> MetadataResult<Option<CacheRow>> {
            let row = sqlx::query_as::<_, CacheRow>("SELECT * FROM caches WHERE cache_id = ?")
                .bind(cache_id)
                .fetch_optional(&self.pool)
                .await?;
            Ok(row)
        }

        async fn get_cache_by_name(&self, name: &str) -> MetadataResult<Option<CacheRow>> {
            let row = sqlx::query_as::<_, CacheRow>("SELECT * FROM caches WHERE cache_name = ?")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?;
            Ok(row)
        }

        async fn update_cache(&self, cache: &CacheRow) -> MetadataResult<()> {
            sqlx::query(
                "UPDATE caches SET cache_name = ?, public_base_url = ?, is_public = ?, updated_at = ? WHERE cache_id = ?",
            )
            .bind(&cache.cache_name)
            .bind(&cache.public_base_url)
            .bind(cache.is_public)
            .bind(cache.updated_at)
            .bind(cache.cache_id)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn delete_cache(&self, cache_id: Uuid) -> MetadataResult<()> {
            sqlx::query("DELETE FROM caches WHERE cache_id = ?")
                .bind(cache_id)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn list_caches(&self) -> MetadataResult<Vec<CacheRow>> {
            let rows = sqlx::query_as::<_, CacheRow>("SELECT * FROM caches ORDER BY cache_name")
                .fetch_all(&self.pool)
                .await?;
            Ok(rows)
        }
    }

    // F-002: TrustedKeyRepo implementation
    #[async_trait]
    impl crate::repos::TrustedKeyRepo for SqliteStore {
        async fn add_trusted_key(&self, key: &TrustedBuilderKeyRow) -> MetadataResult<()> {
            sqlx::query(
                r#"
                INSERT INTO trusted_builder_keys (
                    trusted_key_id, cache_id, key_name, public_key, trust_level,
                    added_by_token_id, created_at, expires_at, revoked_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(key.trusted_key_id)
            .bind(key.cache_id)
            .bind(&key.key_name)
            .bind(&key.public_key)
            .bind(&key.trust_level)
            .bind(key.added_by_token_id)
            .bind(key.created_at)
            .bind(key.expires_at)
            .bind(key.revoked_at)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn get_trusted_key(&self, key_id: Uuid) -> MetadataResult<Option<TrustedBuilderKeyRow>> {
            let row = sqlx::query_as::<_, TrustedBuilderKeyRow>(
                "SELECT * FROM trusted_builder_keys WHERE trusted_key_id = ?",
            )
            .bind(key_id)
            .fetch_optional(&self.pool)
            .await?;
            Ok(row)
        }

        async fn get_trusted_key_by_name(
            &self,
            cache_id: Option<Uuid>,
            key_name: &str,
        ) -> MetadataResult<Option<TrustedBuilderKeyRow>> {
            let row = match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, TrustedBuilderKeyRow>(
                        "SELECT * FROM trusted_builder_keys WHERE cache_id = ? AND key_name = ?",
                    )
                    .bind(id)
                    .bind(key_name)
                    .fetch_optional(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as::<_, TrustedBuilderKeyRow>(
                        "SELECT * FROM trusted_builder_keys WHERE cache_id IS NULL AND key_name = ?",
                    )
                    .bind(key_name)
                    .fetch_optional(&self.pool)
                    .await?
                }
            };
            Ok(row)
        }

        async fn list_trusted_keys(
            &self,
            cache_id: Option<Uuid>,
        ) -> MetadataResult<Vec<TrustedBuilderKeyRow>> {
            let rows = match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, TrustedBuilderKeyRow>(
                        "SELECT * FROM trusted_builder_keys WHERE cache_id = ? ORDER BY created_at DESC",
                    )
                    .bind(id)
                    .fetch_all(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as::<_, TrustedBuilderKeyRow>(
                        "SELECT * FROM trusted_builder_keys WHERE cache_id IS NULL ORDER BY created_at DESC",
                    )
                    .fetch_all(&self.pool)
                    .await?
                }
            };
            Ok(rows)
        }

        async fn revoke_trusted_key(
            &self,
            key_id: Uuid,
            revoked_at: OffsetDateTime,
        ) -> MetadataResult<()> {
            sqlx::query("UPDATE trusted_builder_keys SET revoked_at = ? WHERE trusted_key_id = ?")
                .bind(revoked_at)
                .bind(key_id)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn is_signature_trusted(
            &self,
            cache_id: Option<Uuid>,
            key_name: &str,
        ) -> MetadataResult<bool> {
            let now = OffsetDateTime::now_utc();
            let row: Option<(i32,)> = match cache_id {
                Some(id) => {
                    sqlx::query_as(
                        r#"
                        SELECT 1 FROM trusted_builder_keys
                        WHERE (cache_id = ? OR cache_id IS NULL)
                          AND key_name = ?
                          AND revoked_at IS NULL
                          AND (expires_at IS NULL OR expires_at > ?)
                        "#,
                    )
                    .bind(id)
                    .bind(key_name)
                    .bind(now)
                    .fetch_optional(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as(
                        r#"
                        SELECT 1 FROM trusted_builder_keys
                        WHERE cache_id IS NULL
                          AND key_name = ?
                          AND revoked_at IS NULL
                          AND (expires_at IS NULL OR expires_at > ?)
                        "#,
                    )
                    .bind(key_name)
                    .bind(now)
                    .fetch_optional(&self.pool)
                    .await?
                }
            };
            Ok(row.is_some())
        }
    }

    // F-005: TombstoneRepo implementation
    #[async_trait]
    impl crate::repos::TombstoneRepo for SqliteStore {
        async fn create_tombstone(&self, tombstone: &TombstoneRow) -> MetadataResult<()> {
            sqlx::query(
                r#"
                INSERT INTO tombstones (
                    tombstone_id, entity_type, entity_id, cache_id, deleted_at,
                    deleted_by_token_id, reason, gc_eligible_at, gc_completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(tombstone.tombstone_id)
            .bind(&tombstone.entity_type)
            .bind(&tombstone.entity_id)
            .bind(tombstone.cache_id)
            .bind(tombstone.deleted_at)
            .bind(tombstone.deleted_by_token_id)
            .bind(&tombstone.reason)
            .bind(tombstone.gc_eligible_at)
            .bind(tombstone.gc_completed_at)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn get_tombstone(
            &self,
            entity_type: &str,
            entity_id: &str,
        ) -> MetadataResult<Option<TombstoneRow>> {
            let row = sqlx::query_as::<_, TombstoneRow>(
                "SELECT * FROM tombstones WHERE entity_type = ? AND entity_id = ?",
            )
            .bind(entity_type)
            .bind(entity_id)
            .fetch_optional(&self.pool)
            .await?;
            Ok(row)
        }

        async fn is_tombstoned(&self, entity_type: &str, entity_id: &str) -> MetadataResult<bool> {
            let row: Option<(i32,)> = sqlx::query_as(
                "SELECT 1 FROM tombstones WHERE entity_type = ? AND entity_id = ? AND gc_completed_at IS NULL",
            )
            .bind(entity_type)
            .bind(entity_id)
            .fetch_optional(&self.pool)
            .await?;
            Ok(row.is_some())
        }

        async fn get_gc_eligible_tombstones(&self, limit: u32) -> MetadataResult<Vec<TombstoneRow>> {
            let now = OffsetDateTime::now_utc();
            let rows = sqlx::query_as::<_, TombstoneRow>(
                "SELECT * FROM tombstones WHERE gc_completed_at IS NULL AND gc_eligible_at <= ? ORDER BY gc_eligible_at LIMIT ?",
            )
            .bind(now)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows)
        }

        async fn mark_gc_completed(
            &self,
            tombstone_id: Uuid,
            completed_at: OffsetDateTime,
        ) -> MetadataResult<()> {
            sqlx::query("UPDATE tombstones SET gc_completed_at = ? WHERE tombstone_id = ?")
                .bind(completed_at)
                .bind(tombstone_id)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn delete_tombstone(&self, tombstone_id: Uuid) -> MetadataResult<()> {
            sqlx::query("DELETE FROM tombstones WHERE tombstone_id = ?")
                .bind(tombstone_id)
                .execute(&self.pool)
                .await?;
            Ok(())
        }
    }

    // F-006: ReachabilityRepo implementation
    #[async_trait]
    impl crate::repos::ReachabilityRepo for SqliteStore {
        async fn get_all_visible_manifests(&self) -> MetadataResult<Vec<String>> {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT DISTINCT manifest_hash FROM store_paths WHERE visibility_state = 'visible'",
            )
            .fetch_all(&self.pool)
            .await?;
            Ok(rows.into_iter().map(|r| r.0).collect())
        }

        async fn get_chunks_for_manifest(&self, manifest_hash: &str) -> MetadataResult<Vec<String>> {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT chunk_hash FROM manifest_chunks WHERE manifest_hash = ? ORDER BY position",
            )
            .bind(manifest_hash)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows.into_iter().map(|r| r.0).collect())
        }

        async fn get_chunk_refcount(&self, chunk_hash: &str) -> MetadataResult<Option<i32>> {
            let row: Option<(i32,)> =
                sqlx::query_as("SELECT refcount FROM chunks WHERE chunk_hash = ?")
                    .bind(chunk_hash)
                    .fetch_optional(&self.pool)
                    .await?;
            Ok(row.map(|r| r.0))
        }

        async fn set_chunk_refcount(&self, chunk_hash: &str, refcount: i32) -> MetadataResult<()> {
            sqlx::query("UPDATE chunks SET refcount = ? WHERE chunk_hash = ?")
                .bind(refcount)
                .bind(chunk_hash)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn batch_update_refcounts(&self, updates: &[(String, i32)]) -> MetadataResult<u64> {
            let mut count = 0u64;
            for (chunk_hash, refcount) in updates {
                let result = sqlx::query("UPDATE chunks SET refcount = ? WHERE chunk_hash = ?")
                    .bind(refcount)
                    .bind(chunk_hash)
                    .execute(&self.pool)
                    .await?;
                count += result.rows_affected();
            }
            Ok(count)
        }
    }
}

impl std::convert::From<std::io::Error> for crate::MetadataError {
    fn from(e: std::io::Error) -> Self {
        crate::MetadataError::Config(e.to_string())
    }
}

/// SQL schema for SQLite.
const SCHEMA_SQL: &str = r#"
-- F-001: Caches table
CREATE TABLE IF NOT EXISTS caches (
    cache_id BLOB PRIMARY KEY,
    cache_name TEXT NOT NULL UNIQUE,
    public_base_url TEXT,
    is_public INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_caches_name ON caches(cache_name);

-- Upload sessions (with F-004 error tracking)
CREATE TABLE IF NOT EXISTS upload_sessions (
    upload_id BLOB PRIMARY KEY,
    cache_id BLOB,
    store_path TEXT NOT NULL,
    store_path_hash TEXT NOT NULL,
    nar_size INTEGER NOT NULL,
    nar_hash TEXT NOT NULL,
    chunk_size INTEGER NOT NULL,
    manifest_hash TEXT,
    state TEXT NOT NULL DEFAULT 'open',
    owner_token_id BLOB,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    trace_id TEXT,
    error_code TEXT,
    error_detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_upload_sessions_state ON upload_sessions(state, expires_at);
CREATE INDEX IF NOT EXISTS idx_upload_sessions_store_path ON upload_sessions(cache_id, store_path_hash);

-- Upload expected chunks
CREATE TABLE IF NOT EXISTS upload_expected_chunks (
    upload_id BLOB NOT NULL,
    position INTEGER NOT NULL,
    chunk_hash TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    received_at TEXT,
    PRIMARY KEY (upload_id, position),
    FOREIGN KEY (upload_id) REFERENCES upload_sessions(upload_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_upload_chunks_received ON upload_expected_chunks(upload_id, received_at);

-- Chunks
CREATE TABLE IF NOT EXISTS chunks (
    chunk_hash TEXT PRIMARY KEY,
    size_bytes INTEGER NOT NULL,
    object_key TEXT,
    refcount INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    last_accessed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_chunks_refcount ON chunks(refcount, created_at);

-- Manifests
CREATE TABLE IF NOT EXISTS manifests (
    manifest_hash TEXT PRIMARY KEY,
    chunk_size INTEGER NOT NULL,
    chunk_count INTEGER NOT NULL,
    nar_size INTEGER NOT NULL,
    object_key TEXT,
    created_at TEXT NOT NULL
);

-- Manifest chunks
CREATE TABLE IF NOT EXISTS manifest_chunks (
    manifest_hash TEXT NOT NULL,
    position INTEGER NOT NULL,
    chunk_hash TEXT NOT NULL,
    PRIMARY KEY (manifest_hash, position),
    FOREIGN KEY (manifest_hash) REFERENCES manifests(manifest_hash) ON DELETE CASCADE,
    FOREIGN KEY (chunk_hash) REFERENCES chunks(chunk_hash)
);
CREATE INDEX IF NOT EXISTS idx_manifest_chunks_chunk ON manifest_chunks(chunk_hash);

-- Store paths (with F-003 CA field and tenant isolation)
-- Composite PRIMARY KEY (cache_id, store_path_hash) ensures tenant isolation:
-- the same store path can exist in different caches with different metadata.
-- cache_id = NULL represents a public/shared cache.
CREATE TABLE IF NOT EXISTS store_paths (
    cache_id BLOB,
    store_path_hash TEXT NOT NULL,
    store_path TEXT NOT NULL,
    nar_hash TEXT NOT NULL,
    nar_size INTEGER NOT NULL,
    manifest_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    committed_at TEXT,
    visibility_state TEXT NOT NULL DEFAULT 'visible',
    uploader_token_id BLOB,
    ca TEXT,
    PRIMARY KEY (cache_id, store_path_hash),
    FOREIGN KEY (manifest_hash) REFERENCES manifests(manifest_hash)
);
CREATE INDEX IF NOT EXISTS idx_store_paths_manifest ON store_paths(manifest_hash);
CREATE INDEX IF NOT EXISTS idx_store_paths_hash ON store_paths(store_path_hash);
-- Unique index for upsert targeting with NULL cache_id handling.
-- COALESCE treats NULL as a sentinel value so ON CONFLICT works correctly.
CREATE UNIQUE INDEX IF NOT EXISTS idx_store_paths_cache_hash ON store_paths(
    COALESCE(cache_id, zeroblob(16)), store_path_hash
);

-- F-003: Store path references (with cache_id for tenant isolation)
CREATE TABLE IF NOT EXISTS store_path_references (
    cache_id BLOB,
    store_path_hash TEXT NOT NULL,
    reference_hash TEXT NOT NULL,
    reference_type TEXT NOT NULL DEFAULT 'reference',
    PRIMARY KEY (cache_id, store_path_hash, reference_hash, reference_type),
    FOREIGN KEY (cache_id, store_path_hash) REFERENCES store_paths(cache_id, store_path_hash) ON DELETE CASCADE
);
-- Unique index for upsert targeting with NULL cache_id handling.
CREATE UNIQUE INDEX IF NOT EXISTS idx_store_path_refs_cache ON store_path_references(
    COALESCE(cache_id, zeroblob(16)), store_path_hash, reference_hash, reference_type
);
CREATE INDEX IF NOT EXISTS idx_store_path_refs_ref ON store_path_references(reference_hash);

-- Narinfo records (with cache_id for tenant isolation)
CREATE TABLE IF NOT EXISTS narinfo_records (
    cache_id BLOB,
    store_path_hash TEXT NOT NULL,
    narinfo_object_key TEXT NOT NULL,
    content_hash TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (cache_id, store_path_hash),
    FOREIGN KEY (cache_id, store_path_hash) REFERENCES store_paths(cache_id, store_path_hash) ON DELETE CASCADE
);
-- Unique index for upsert targeting with NULL cache_id handling.
CREATE UNIQUE INDEX IF NOT EXISTS idx_narinfo_cache_hash ON narinfo_records(
    COALESCE(cache_id, zeroblob(16)), store_path_hash
);

-- Signatures (with cache_id for tenant isolation)
CREATE TABLE IF NOT EXISTS signatures (
    cache_id BLOB,
    store_path_hash TEXT NOT NULL,
    key_id BLOB NOT NULL,
    signature TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (cache_id, store_path_hash, key_id),
    FOREIGN KEY (cache_id, store_path_hash) REFERENCES store_paths(cache_id, store_path_hash) ON DELETE CASCADE
);
-- Unique index for upsert targeting with NULL cache_id handling.
CREATE UNIQUE INDEX IF NOT EXISTS idx_signatures_cache_hash_key ON signatures(
    COALESCE(cache_id, zeroblob(16)), store_path_hash, key_id
);

-- Tokens
CREATE TABLE IF NOT EXISTS tokens (
    token_id BLOB PRIMARY KEY,
    cache_id BLOB,
    token_hash TEXT NOT NULL UNIQUE,
    scopes TEXT NOT NULL,
    expires_at TEXT,
    revoked_at TEXT,
    created_at TEXT NOT NULL,
    last_used_at TEXT,
    description TEXT
);
CREATE INDEX IF NOT EXISTS idx_tokens_hash ON tokens(token_hash);

-- Signing keys
CREATE TABLE IF NOT EXISTS signing_keys (
    key_id BLOB PRIMARY KEY,
    cache_id BLOB,
    key_name TEXT NOT NULL UNIQUE,
    public_key TEXT NOT NULL,
    private_key_ref TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    rotated_at TEXT
);

-- GC jobs
CREATE TABLE IF NOT EXISTS gc_jobs (
    gc_job_id BLOB PRIMARY KEY,
    cache_id BLOB,
    job_type TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'queued',
    started_at TEXT,
    finished_at TEXT,
    stats_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_gc_jobs_state ON gc_jobs(state);

-- F-002: Trusted builder keys
CREATE TABLE IF NOT EXISTS trusted_builder_keys (
    trusted_key_id BLOB PRIMARY KEY,
    cache_id BLOB,
    key_name TEXT NOT NULL,
    public_key TEXT NOT NULL,
    trust_level TEXT NOT NULL DEFAULT 'trusted',
    added_by_token_id BLOB,
    created_at TEXT NOT NULL,
    expires_at TEXT,
    revoked_at TEXT,
    UNIQUE(cache_id, key_name)
);
CREATE INDEX IF NOT EXISTS idx_trusted_keys_cache ON trusted_builder_keys(cache_id);
CREATE INDEX IF NOT EXISTS idx_trusted_keys_name ON trusted_builder_keys(key_name);

-- F-005: Tombstones
CREATE TABLE IF NOT EXISTS tombstones (
    tombstone_id BLOB PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    cache_id BLOB,
    deleted_at TEXT NOT NULL,
    deleted_by_token_id BLOB,
    reason TEXT,
    gc_eligible_at TEXT NOT NULL,
    gc_completed_at TEXT,
    UNIQUE(entity_type, entity_id)
);
CREATE INDEX IF NOT EXISTS idx_tombstones_gc ON tombstones(gc_eligible_at);
CREATE INDEX IF NOT EXISTS idx_tombstones_entity ON tombstones(entity_type, entity_id);
"#;
