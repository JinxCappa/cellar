//! PostgreSQL-based metadata store implementation.

use crate::error::{MetadataError, MetadataResult};
use crate::models::*;
use crate::repos::{
    chunks::ChunkStats, CacheRepo, ChunkRepo, GcRepo, ManifestRepo, ReachabilityRepo,
    StorePathRepo, TokenRepo, TombstoneRepo, TrustedKeyRepo, UploadRepo,
};
use crate::store::MetadataStore;
use async_trait::async_trait;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Pool, Postgres};
use std::str::FromStr;
use time::OffsetDateTime;
use uuid::Uuid;

/// PostgreSQL schema (embedded).
const POSTGRES_SCHEMA: &str = include_str!("postgres_schema.sql");

/// PostgreSQL-based metadata store.
pub struct PostgresStore {
    pool: Pool<Postgres>,
}

impl PostgresStore {
    /// Create a new PostgreSQL store.
    pub async fn new(url: &str, max_connections: u32) -> MetadataResult<Self> {
        let opts = PgConnectOptions::from_str(url)?;

        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect_with(opts)
            .await?;

        let store = Self { pool };
        store.migrate().await?;

        Ok(store)
    }

    /// Get a reference to the connection pool.
    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }
}

#[async_trait]
impl MetadataStore for PostgresStore {
    async fn migrate(&self) -> MetadataResult<()> {
        // PostgreSQL doesn't allow multiple statements in a single prepared statement,
        // so we split the schema and execute each statement separately.
        for statement in POSTGRES_SCHEMA.split(';') {
            let trimmed = statement.trim();
            // Skip empty statements
            if trimmed.is_empty() {
                continue;
            }
            // Check if this is a pure comment (no actual SQL)
            let has_sql = trimmed
                .lines()
                .any(|line| {
                    let line = line.trim();
                    !line.is_empty() && !line.starts_with("--")
                });
            if has_sql {
                sqlx::query(trimmed).execute(&self.pool).await?;
            }
        }
        Ok(())
    }

    async fn health_check(&self) -> MetadataResult<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }
}


#[async_trait]
impl UploadRepo for PostgresStore {
    async fn create_session(&self, session: &UploadSessionRow) -> MetadataResult<()> {
        sqlx::query(
            r#"
            INSERT INTO upload_sessions (
                upload_id, cache_id, store_path, store_path_hash,
                nar_size, nar_hash, chunk_size, manifest_hash, state,
                owner_token_id, created_at, updated_at, expires_at, trace_id,
                error_code, error_detail
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
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
            "SELECT * FROM upload_sessions WHERE upload_id = $1",
        )
        .bind(upload_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn get_session_for_commit(&self, upload_id: Uuid) -> MetadataResult<Option<UploadSessionRow>> {
        // SELECT FOR UPDATE acquires an exclusive row lock, preventing concurrent commits
        // on the same upload_id. The lock is held until the transaction commits/rolls back.
        let row = sqlx::query_as::<_, UploadSessionRow>(
            "SELECT * FROM upload_sessions WHERE upload_id = $1 FOR UPDATE",
        )
        .bind(upload_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn begin_commit_session(&self, upload_id: Uuid, updated_at: OffsetDateTime) -> MetadataResult<Option<UploadSessionRow>> {
        // Atomically get session with FOR UPDATE lock and transition to 'committing' state.
        // This prevents the race condition where two commits both see state='open' and proceed.
        // Only transitions if current state is 'open'.
        let mut tx = self.pool.begin().await?;

        // Get session with exclusive lock
        let mut session = sqlx::query_as::<_, UploadSessionRow>(
            "SELECT * FROM upload_sessions WHERE upload_id = $1 FOR UPDATE",
        )
        .bind(upload_id)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(ref mut s) = session {
            if s.state == "open" {
                // Only update state if currently 'open'
                let result = sqlx::query("UPDATE upload_sessions SET state = $1, updated_at = $2 WHERE upload_id = $3 AND state = 'open'")
                    .bind("committing")
                    .bind(updated_at)
                    .bind(upload_id)
                    .execute(&mut *tx)
                    .await?;

                // Update the session object if the UPDATE succeeded
                if result.rows_affected() > 0 {
                    s.state = "committing".to_string();
                    s.updated_at = updated_at;
                }
            }
        }

        tx.commit().await?;

        // Return session (state will be 'committing' if transition succeeded, original state otherwise)
        Ok(session)
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
                    "SELECT * FROM upload_sessions WHERE cache_id = $1 AND store_path_hash = $2 AND state = 'open' ORDER BY created_at DESC LIMIT 1",
                )
                .bind(id)
                .bind(store_path_hash)
                .fetch_optional(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, UploadSessionRow>(
                    "SELECT * FROM upload_sessions WHERE cache_id IS NULL AND store_path_hash = $1 AND state = 'open' ORDER BY created_at DESC LIMIT 1",
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
        sqlx::query("UPDATE upload_sessions SET state = $1, updated_at = $2 WHERE upload_id = $3")
            .bind(state)
            .bind(updated_at)
            .bind(upload_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn set_manifest_hash(&self, upload_id: Uuid, manifest_hash: &str) -> MetadataResult<()> {
        sqlx::query("UPDATE upload_sessions SET manifest_hash = $1 WHERE upload_id = $2")
            .bind(manifest_hash)
            .bind(upload_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn add_expected_chunks(&self, chunks: &[UploadExpectedChunkRow]) -> MetadataResult<()> {
        for chunk in chunks {
            sqlx::query(
                r#"
                INSERT INTO upload_expected_chunks (upload_id, position, chunk_hash, size_bytes)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (upload_id, position) DO NOTHING
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
        // Two-phase approach to handle both expected chunks and dynamic tracking:
        // 1. First try UPDATE (for pre-defined expected chunks)
        // 2. If no rows updated, INSERT new row (for dynamic tracking without expected chunks)

        // Phase 1: Try to update existing expected chunk row
        let update_result = sqlx::query(
            "UPDATE upload_expected_chunks SET received_at = $1 WHERE upload_id = $2 AND chunk_hash = $3",
        )
        .bind(received_at)
        .bind(upload_id)
        .bind(chunk_hash)
        .execute(&self.pool)
        .await?;

        if update_result.rows_affected() > 0 {
            // Successfully updated existing expected chunk
            return Ok(());
        }

        // Phase 2: No expected chunk found, insert for dynamic tracking
        // Use negative positions (starting from -1, -2, ...) to avoid conflicts with
        // expected chunks (which use positions 0, 1, 2, ...).
        // Retry on PK conflict (race condition when multiple chunks insert concurrently).
        const MAX_RETRIES: u32 = 5;
        for attempt in 0..MAX_RETRIES {
            // Get next available negative position
            let next_position: i32 = sqlx::query_scalar(
                "SELECT COALESCE(MIN(position), 0) - 1 FROM upload_expected_chunks WHERE upload_id = $1",
            )
            .bind(upload_id)
            .fetch_one(&self.pool)
            .await?;

            let insert_result = sqlx::query(
                r#"
                INSERT INTO upload_expected_chunks (upload_id, position, chunk_hash, size_bytes, received_at)
                VALUES ($1, $2, $3, 0, $4)
                ON CONFLICT (upload_id, chunk_hash) DO UPDATE SET received_at = EXCLUDED.received_at
                "#,
            )
            .bind(upload_id)
            .bind(next_position)
            .bind(chunk_hash)
            .bind(received_at)
            .execute(&self.pool)
            .await;

            match insert_result {
                Ok(_) => return Ok(()),
                Err(sqlx::Error::Database(db_err)) => {
                    // Check if it's a unique/PK constraint violation (position conflict)
                    // PostgreSQL error code 23505 = unique_violation
                    if db_err.code().as_deref() == Some("23505")
                        && db_err.message().contains("upload_expected_chunks_pkey")
                    {
                        // Position race condition - retry with new position
                        if attempt < MAX_RETRIES - 1 {
                            continue;
                        }
                    }
                    return Err(sqlx::Error::Database(db_err).into());
                }
                Err(e) => return Err(e.into()),
            }
        }

        // This should rarely happen - give up after retries
        Err(MetadataError::Internal(
            "failed to mark chunk received after retries".to_string(),
        ))
    }

    async fn get_received_chunks(&self, upload_id: Uuid) -> MetadataResult<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT chunk_hash FROM upload_expected_chunks WHERE upload_id = $1 AND received_at IS NOT NULL ORDER BY position",
        )
        .bind(upload_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn get_missing_chunks(&self, upload_id: Uuid) -> MetadataResult<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT chunk_hash FROM upload_expected_chunks WHERE upload_id = $1 AND received_at IS NULL ORDER BY position",
        )
        .bind(upload_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn get_expected_chunks(&self, upload_id: Uuid) -> MetadataResult<Vec<UploadExpectedChunkRow>> {
        let rows = sqlx::query_as::<_, UploadExpectedChunkRow>(
            "SELECT * FROM upload_expected_chunks WHERE upload_id = $1 ORDER BY position",
        )
        .bind(upload_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn get_expired_sessions(&self, limit: u32) -> MetadataResult<Vec<UploadSessionRow>> {
        let now = OffsetDateTime::now_utc();
        let rows = sqlx::query_as::<_, UploadSessionRow>(
            "SELECT * FROM upload_sessions WHERE expires_at < $1 AND state = 'open' ORDER BY expires_at LIMIT $2",
        )
        .bind(now)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn delete_session(&self, upload_id: Uuid) -> MetadataResult<()> {
        sqlx::query("DELETE FROM upload_sessions WHERE upload_id = $1")
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
            "UPDATE upload_sessions SET state = 'failed', error_code = $1, error_detail = $2, updated_at = $3 WHERE upload_id = $4",
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
            "SELECT * FROM upload_sessions WHERE state = 'failed' AND error_code = $1 ORDER BY updated_at DESC LIMIT $2",
        )
        .bind(error_code)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }
}

#[async_trait]
impl ChunkRepo for PostgresStore {
    async fn upsert_chunk(&self, chunk: &ChunkRow) -> MetadataResult<()> {
        sqlx::query(
            r#"
            INSERT INTO chunks (chunk_hash, size_bytes, object_key, refcount, created_at, last_accessed_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (chunk_hash) DO UPDATE SET last_accessed_at = $6
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
            sqlx::query_as::<_, ChunkRow>("SELECT * FROM chunks WHERE chunk_hash = $1")
                .bind(chunk_hash)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row)
    }

    async fn chunk_exists(&self, chunk_hash: &str) -> MetadataResult<bool> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE chunk_hash = $1")
                .bind(chunk_hash)
                .fetch_one(&self.pool)
                .await?;
        Ok(count > 0)
    }

    async fn increment_refcount(&self, chunk_hash: &str) -> MetadataResult<()> {
        sqlx::query("UPDATE chunks SET refcount = refcount + 1 WHERE chunk_hash = $1")
            .bind(chunk_hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn decrement_refcount(&self, chunk_hash: &str) -> MetadataResult<()> {
        sqlx::query("UPDATE chunks SET refcount = refcount - 1 WHERE chunk_hash = $1 AND refcount > 0")
            .bind(chunk_hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn increment_cache_refcount(
        &self,
        cache_id: Option<Uuid>,
        chunk_hash: &str,
    ) -> MetadataResult<()> {
        // Upsert per-cache refcount and increment global refcount atomically
        let mut tx = self.pool.begin().await?;

        // Upsert per-cache reference count
        sqlx::query(
            r#"
            INSERT INTO cache_chunk_refs (cache_id, chunk_hash, refcount, created_at)
            VALUES ($1, $2, 1, NOW())
            ON CONFLICT (COALESCE(cache_id, '00000000-0000-0000-0000-000000000000'::UUID), chunk_hash)
            DO UPDATE SET refcount = cache_chunk_refs.refcount + 1
            "#,
        )
        .bind(cache_id)
        .bind(chunk_hash)
        .execute(&mut *tx)
        .await?;

        // Increment global refcount
        sqlx::query("UPDATE chunks SET refcount = refcount + 1 WHERE chunk_hash = $1")
            .bind(chunk_hash)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn decrement_cache_refcount(
        &self,
        cache_id: Option<Uuid>,
        chunk_hash: &str,
    ) -> MetadataResult<()> {
        // Decrement per-cache refcount and global refcount atomically
        let mut tx = self.pool.begin().await?;

        // Decrement per-cache reference count (prevent going below 0)
        let result = sqlx::query(
            r#"
            UPDATE cache_chunk_refs
            SET refcount = refcount - 1
            WHERE (cache_id = $1 OR ($1 IS NULL AND cache_id IS NULL))
              AND chunk_hash = $2
              AND refcount > 0
            "#,
        )
        .bind(cache_id)
        .bind(chunk_hash)
        .execute(&mut *tx)
        .await?;

        // Only decrement global refcount if the per-cache decrement actually affected a row.
        // This prevents desync when the per-cache ref doesn't exist or is already 0.
        if result.rows_affected() > 0 {
            sqlx::query(
                "UPDATE chunks SET refcount = refcount - 1 WHERE chunk_hash = $1 AND refcount > 0",
            )
            .bind(chunk_hash)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn touch_chunk(&self, chunk_hash: &str, accessed_at: OffsetDateTime) -> MetadataResult<()> {
        sqlx::query("UPDATE chunks SET last_accessed_at = $1 WHERE chunk_hash = $2")
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
        // Exclude chunks that are part of open OR committing upload sessions to prevent
        // GC from deleting chunks for in-progress uploads (which temporarily have refcount=0).
        // CRITICAL: Must exclude 'committing' state to prevent race where GC deletes chunks
        // mid-commit before refcounts are incremented.
        let rows = sqlx::query_as::<_, ChunkRow>(
            r#"
            SELECT * FROM chunks
            WHERE refcount = 0
              AND created_at < $1
              AND chunk_hash NOT IN (
                SELECT uec.chunk_hash FROM upload_expected_chunks uec
                INNER JOIN upload_sessions us ON uec.upload_id = us.upload_id
                WHERE us.state IN ('open', 'committing')
              )
            ORDER BY created_at
            LIMIT $2
            "#,
        )
        .bind(older_than)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn delete_chunk(&self, chunk_hash: &str) -> MetadataResult<()> {
        sqlx::query("DELETE FROM chunks WHERE chunk_hash = $1")
            .bind(chunk_hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_stats(&self) -> MetadataResult<ChunkStats> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chunks")
            .fetch_one(&self.pool)
            .await?;
        // Cast to BIGINT since PostgreSQL SUM returns NUMERIC
        let total_size: i64 = sqlx::query_scalar("SELECT COALESCE(SUM(size_bytes)::BIGINT, 0) FROM chunks")
            .fetch_one(&self.pool)
            .await?;
        let referenced_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE refcount > 0")
                .fetch_one(&self.pool)
                .await?;
        let unreferenced_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE refcount = 0")
                .fetch_one(&self.pool)
                .await?;

        Ok(ChunkStats {
            count: count as u64,
            total_size: total_size as u64,
            referenced_count: referenced_count as u64,
            unreferenced_count: unreferenced_count as u64,
        })
    }
}

#[async_trait]
impl ManifestRepo for PostgresStore {
    async fn create_manifest(
        &self,
        manifest: &ManifestRow,
        chunks: &[ManifestChunkRow],
    ) -> MetadataResult<bool> {
        // Use a transaction to ensure atomicity: both the manifest and all its
        // chunk mappings are inserted together, or neither is.
        let mut tx = self.pool.begin().await?;

        // Use ON CONFLICT DO NOTHING to handle concurrent creates atomically.
        // This eliminates the TOCTOU race condition from check-then-insert.
        let result = sqlx::query(
            r#"
            INSERT INTO manifests (manifest_hash, chunk_size, chunk_count, nar_size, object_key, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (manifest_hash) DO NOTHING
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
                "INSERT INTO manifest_chunks (manifest_hash, position, chunk_hash) VALUES ($1, $2, $3)",
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
        let row =
            sqlx::query_as::<_, ManifestRow>("SELECT * FROM manifests WHERE manifest_hash = $1")
                .bind(manifest_hash)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row)
    }

    async fn manifest_exists(&self, manifest_hash: &str) -> MetadataResult<bool> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM manifests WHERE manifest_hash = $1")
                .bind(manifest_hash)
                .fetch_one(&self.pool)
                .await?;
        Ok(count > 0)
    }

    async fn get_manifest_chunks(&self, manifest_hash: &str) -> MetadataResult<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT chunk_hash FROM manifest_chunks WHERE manifest_hash = $1 ORDER BY position",
        )
        .bind(manifest_hash)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn get_manifests_using_chunk(&self, chunk_hash: &str) -> MetadataResult<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT manifest_hash FROM manifest_chunks WHERE chunk_hash = $1",
        )
        .bind(chunk_hash)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn delete_manifest(&self, manifest_hash: &str) -> MetadataResult<()> {
        // Delete chunk mappings first (cascade should handle this, but be explicit)
        sqlx::query("DELETE FROM manifest_chunks WHERE manifest_hash = $1")
            .bind(manifest_hash)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM manifests WHERE manifest_hash = $1")
            .bind(manifest_hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_orphaned_manifests(&self, limit: u32) -> MetadataResult<Vec<ManifestRow>> {
        // Count visible, pending, and pending_reupload store paths as references.
        // Only failed/aborted uploads should not prevent GC of manifests.
        // CRITICAL: Add grace period to prevent race where manifest is created but store_path
        // not yet created during commit. Don't GC manifests created in last 10 minutes.
        // This prevents GC from running in the window between create_manifest and create_store_path.
        let grace_period = OffsetDateTime::now_utc() - time::Duration::minutes(10);
        let rows = sqlx::query_as::<_, ManifestRow>(
            r#"
            SELECT m.* FROM manifests m
            LEFT JOIN store_paths sp ON sp.manifest_hash = m.manifest_hash
                AND sp.visibility_state IN ('visible', 'pending', 'pending_reupload')
            WHERE sp.manifest_hash IS NULL
              AND m.created_at < $1
            ORDER BY m.created_at
            LIMIT $2
            "#,
        )
        .bind(grace_period)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }
}

#[async_trait]
impl StorePathRepo for PostgresStore {
    async fn create_store_path(&self, store_path: &StorePathRow) -> MetadataResult<()> {
        // Use UPSERT with the unique index on (COALESCE(cache_id, '00000000-...'), store_path_hash).
        // CRITICAL: cache_id is NOT updated on conflict - this prevents tenant hijacking where
        // an attacker could overwrite another tenant's store path by uploading the same hash.
        // CRITICAL: For reuploads (visibility_state='visible'), preserve BOTH metadata AND visibility
        // to prevent 404s during reupload. The path stays 'visible' serving old content until
        // complete_reupload atomically updates both metadata and confirms visibility.
        sqlx::query(
            r#"
            INSERT INTO store_paths (
                cache_id, store_path_hash, store_path, nar_hash, nar_size, manifest_hash,
                created_at, committed_at, visibility_state, uploader_token_id, ca
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (COALESCE(cache_id, '00000000-0000-0000-0000-000000000000'::UUID), store_path_hash) DO UPDATE SET
                nar_hash = CASE
                    WHEN store_paths.visibility_state = 'visible' THEN store_paths.nar_hash
                    ELSE EXCLUDED.nar_hash
                END,
                nar_size = CASE
                    WHEN store_paths.visibility_state = 'visible' THEN store_paths.nar_size
                    ELSE EXCLUDED.nar_size
                END,
                manifest_hash = CASE
                    WHEN store_paths.visibility_state = 'visible' THEN store_paths.manifest_hash
                    ELSE EXCLUDED.manifest_hash
                END,
                committed_at = EXCLUDED.committed_at,
                visibility_state = CASE
                    WHEN store_paths.visibility_state = 'visible' THEN 'visible'
                    ELSE EXCLUDED.visibility_state
                END,
                ca = EXCLUDED.ca
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
                    "SELECT * FROM store_paths WHERE cache_id = $1 AND store_path_hash = $2",
                )
                .bind(id)
                .bind(store_path_hash)
                .fetch_optional(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, StorePathRow>(
                    "SELECT * FROM store_paths WHERE cache_id IS NULL AND store_path_hash = $1",
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
        let row: Option<String> = match cache_id {
            Some(id) => {
                sqlx::query_scalar(
                    "SELECT visibility_state FROM store_paths WHERE cache_id = $1 AND store_path_hash = $2",
                )
                .bind(id)
                .bind(store_path_hash)
                .fetch_optional(&self.pool)
                .await?
            }
            None => {
                sqlx::query_scalar(
                    "SELECT visibility_state FROM store_paths WHERE cache_id IS NULL AND store_path_hash = $1",
                )
                .bind(store_path_hash)
                .fetch_optional(&self.pool)
                .await?
            }
        };
        Ok(row.as_deref() == Some("visible"))
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
                sqlx::query("UPDATE store_paths SET visibility_state = $1 WHERE cache_id = $2 AND store_path_hash = $3")
                    .bind(visibility)
                    .bind(id)
                    .bind(store_path_hash)
                    .execute(&self.pool)
                    .await?;
            }
            None => {
                sqlx::query("UPDATE store_paths SET visibility_state = $1 WHERE cache_id IS NULL AND store_path_hash = $2")
                    .bind(visibility)
                    .bind(store_path_hash)
                    .execute(&self.pool)
                    .await?;
            }
        }
        Ok(())
    }

    async fn complete_reupload(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
        nar_hash: &str,
        nar_size: i64,
        manifest_hash: &str,
    ) -> MetadataResult<()> {
        // Atomically update visibility and metadata for successful reupload
        match cache_id {
            Some(id) => {
                sqlx::query(
                    "UPDATE store_paths SET visibility_state = 'visible', nar_hash = $1, nar_size = $2, manifest_hash = $3 WHERE cache_id = $4 AND store_path_hash = $5"
                )
                .bind(nar_hash)
                .bind(nar_size)
                .bind(manifest_hash)
                .bind(id)
                .bind(store_path_hash)
                .execute(&self.pool)
                .await?;
            }
            None => {
                sqlx::query(
                    "UPDATE store_paths SET visibility_state = 'visible', nar_hash = $1, nar_size = $2, manifest_hash = $3 WHERE cache_id IS NULL AND store_path_hash = $4"
                )
                .bind(nar_hash)
                .bind(nar_size)
                .bind(manifest_hash)
                .bind(store_path_hash)
                .execute(&self.pool)
                .await?;
            }
        }
        Ok(())
    }

    async fn upsert_narinfo(&self, narinfo: &NarInfoRow) -> MetadataResult<()> {
        // Use the COALESCE index for upsert targeting (handles NULL cache_id)
        sqlx::query(
            r#"
            INSERT INTO narinfo_records (cache_id, store_path_hash, narinfo_object_key, content_hash, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (COALESCE(cache_id, '00000000-0000-0000-0000-000000000000'::UUID), store_path_hash) DO UPDATE SET
                narinfo_object_key = $3, content_hash = $4, updated_at = $6
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
                    WHERE cache_id = $1 AND store_path_hash = $2
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
                    WHERE cache_id IS NULL AND store_path_hash = $1
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
        // Use the COALESCE index for upsert targeting (handles NULL cache_id)
        sqlx::query(
            r#"
            INSERT INTO signatures (cache_id, store_path_hash, key_id, signature, created_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (COALESCE(cache_id, '00000000-0000-0000-0000-000000000000'::UUID), store_path_hash, key_id) DO UPDATE SET signature = $4
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
                    "SELECT * FROM signatures WHERE cache_id = $1 AND store_path_hash = $2",
                )
                .bind(id)
                .bind(store_path_hash)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, SignatureRow>(
                    "SELECT * FROM signatures WHERE cache_id IS NULL AND store_path_hash = $1",
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
                sqlx::query("DELETE FROM signatures WHERE cache_id = $1 AND store_path_hash = $2")
                    .bind(id)
                    .bind(store_path_hash)
                    .execute(&self.pool)
                    .await?;
                sqlx::query("DELETE FROM narinfo_records WHERE cache_id = $1 AND store_path_hash = $2")
                    .bind(id)
                    .bind(store_path_hash)
                    .execute(&self.pool)
                    .await?;
                sqlx::query("DELETE FROM store_path_references WHERE cache_id = $1 AND store_path_hash = $2")
                    .bind(id)
                    .bind(store_path_hash)
                    .execute(&self.pool)
                    .await?;
                sqlx::query("DELETE FROM store_paths WHERE cache_id = $1 AND store_path_hash = $2")
                    .bind(id)
                    .bind(store_path_hash)
                    .execute(&self.pool)
                    .await?;
            }
            None => {
                sqlx::query("DELETE FROM signatures WHERE cache_id IS NULL AND store_path_hash = $1")
                    .bind(store_path_hash)
                    .execute(&self.pool)
                    .await?;
                sqlx::query("DELETE FROM narinfo_records WHERE cache_id IS NULL AND store_path_hash = $1")
                    .bind(store_path_hash)
                    .execute(&self.pool)
                    .await?;
                sqlx::query("DELETE FROM store_path_references WHERE cache_id IS NULL AND store_path_hash = $1")
                    .bind(store_path_hash)
                    .execute(&self.pool)
                    .await?;
                sqlx::query("DELETE FROM store_paths WHERE cache_id IS NULL AND store_path_hash = $1")
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
                    "SELECT * FROM store_paths WHERE cache_id = $1 AND manifest_hash = $2",
                )
                .bind(id)
                .bind(manifest_hash)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                // Public cache only - consistent with other functions
                sqlx::query_as::<_, StorePathRow>(
                    "SELECT * FROM store_paths WHERE cache_id IS NULL AND manifest_hash = $1",
                )
                .bind(manifest_hash)
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows)
    }

    async fn count_store_paths(&self, cache_id: Option<Uuid>) -> MetadataResult<u64> {
        // Filter by cache_id for tenant isolation
        let count: i64 = match cache_id {
            Some(id) => {
                sqlx::query_scalar("SELECT COUNT(*) FROM store_paths WHERE cache_id = $1")
                    .bind(id)
                    .fetch_one(&self.pool)
                    .await?
            }
            None => {
                sqlx::query_scalar("SELECT COUNT(*) FROM store_paths WHERE cache_id IS NULL")
                    .fetch_one(&self.pool)
                    .await?
            }
        };
        Ok(count as u64)
    }

    async fn add_references(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
        refs: &[StorePathReferenceRow],
    ) -> MetadataResult<()> {
        for r in refs {
            // Use the COALESCE index for upsert targeting (handles NULL cache_id)
            sqlx::query(
                r#"
                INSERT INTO store_path_references (cache_id, store_path_hash, reference_hash, reference_type)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (COALESCE(cache_id, '00000000-0000-0000-0000-000000000000'::UUID), store_path_hash, reference_hash, reference_type) DO NOTHING
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
        let rows: Vec<String> = match cache_id {
            Some(id) => {
                sqlx::query_scalar(
                    "SELECT reference_hash FROM store_path_references WHERE cache_id = $1 AND store_path_hash = $2 AND reference_type = 'reference'",
                )
                .bind(id)
                .bind(store_path_hash)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_scalar(
                    "SELECT reference_hash FROM store_path_references WHERE cache_id IS NULL AND store_path_hash = $1 AND reference_type = 'reference'",
                )
                .bind(store_path_hash)
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows)
    }

    async fn get_deriver(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
    ) -> MetadataResult<Option<String>> {
        let row: Option<String> = match cache_id {
            Some(id) => {
                sqlx::query_scalar(
                    "SELECT reference_hash FROM store_path_references WHERE cache_id = $1 AND store_path_hash = $2 AND reference_type = 'deriver' LIMIT 1",
                )
                .bind(id)
                .bind(store_path_hash)
                .fetch_optional(&self.pool)
                .await?
            }
            None => {
                sqlx::query_scalar(
                    "SELECT reference_hash FROM store_path_references WHERE cache_id IS NULL AND store_path_hash = $1 AND reference_type = 'deriver' LIMIT 1",
                )
                .bind(store_path_hash)
                .fetch_optional(&self.pool)
                .await?
            }
        };
        Ok(row)
    }

    async fn get_referrers(
        &self,
        cache_id: Option<Uuid>,
        reference_hash: &str,
    ) -> MetadataResult<Vec<String>> {
        // Filter by cache_id for tenant isolation
        let rows: Vec<String> = match cache_id {
            Some(id) => {
                sqlx::query_scalar(
                    "SELECT store_path_hash FROM store_path_references WHERE cache_id = $1 AND reference_hash = $2",
                )
                .bind(id)
                .bind(reference_hash)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_scalar(
                    "SELECT store_path_hash FROM store_path_references WHERE cache_id IS NULL AND reference_hash = $1",
                )
                .bind(reference_hash)
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows)
    }

    async fn delete_references(
        &self,
        cache_id: Option<Uuid>,
        store_path_hash: &str,
    ) -> MetadataResult<()> {
        match cache_id {
            Some(id) => {
                sqlx::query("DELETE FROM store_path_references WHERE cache_id = $1 AND store_path_hash = $2")
                    .bind(id)
                    .bind(store_path_hash)
                    .execute(&self.pool)
                    .await?;
            }
            None => {
                sqlx::query("DELETE FROM store_path_references WHERE cache_id IS NULL AND store_path_hash = $1")
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
        // Use COALESCE to handle NULL committed_at (failed uploads that never completed).
        let result = sqlx::query(
            r#"
            DELETE FROM store_paths
            WHERE visibility_state = 'failed'
              AND COALESCE(committed_at, created_at) < NOW() - INTERVAL '1 second' * $1
            "#,
        )
        .bind(age_seconds)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}

#[async_trait]
impl TokenRepo for PostgresStore {
    async fn create_token(&self, token: &TokenRow) -> MetadataResult<()> {
        sqlx::query(
            r#"
            INSERT INTO tokens (
                token_id, cache_id, token_hash, scopes, expires_at,
                revoked_at, created_at, last_used_at, description
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
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
            sqlx::query_as::<_, TokenRow>("SELECT * FROM tokens WHERE token_hash = $1")
                .bind(token_hash)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row)
    }

    async fn get_token(&self, token_id: Uuid) -> MetadataResult<Option<TokenRow>> {
        let row = sqlx::query_as::<_, TokenRow>("SELECT * FROM tokens WHERE token_id = $1")
            .bind(token_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    async fn touch_token(&self, token_id: Uuid, used_at: OffsetDateTime) -> MetadataResult<()> {
        sqlx::query("UPDATE tokens SET last_used_at = $1 WHERE token_id = $2")
            .bind(used_at)
            .bind(token_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn revoke_token(&self, token_id: Uuid, revoked_at: OffsetDateTime) -> MetadataResult<()> {
        sqlx::query("UPDATE tokens SET revoked_at = $1 WHERE token_id = $2")
            .bind(revoked_at)
            .bind(token_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_token(&self, token_id: Uuid) -> MetadataResult<()> {
        sqlx::query("DELETE FROM tokens WHERE token_id = $1")
            .bind(token_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_tokens(&self, cache_id: Option<Uuid>) -> MetadataResult<Vec<TokenRow>> {
        let rows = match cache_id {
            Some(id) => {
                sqlx::query_as::<_, TokenRow>(
                    "SELECT * FROM tokens WHERE cache_id = $1 ORDER BY created_at DESC",
                )
                .bind(id)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, TokenRow>("SELECT * FROM tokens ORDER BY created_at DESC")
                    .fetch_all(&self.pool)
                    .await?
            }
        };
        Ok(rows)
    }

    async fn create_signing_key(&self, key: &SigningKeyRow) -> MetadataResult<()> {
        sqlx::query(
            r#"
            INSERT INTO signing_keys (key_id, cache_id, key_name, public_key, private_key_ref, status, created_at, rotated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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
                    "SELECT * FROM signing_keys WHERE cache_id = $1 AND status = 'active' ORDER BY created_at DESC LIMIT 1",
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
        let row =
            sqlx::query_as::<_, SigningKeyRow>("SELECT * FROM signing_keys WHERE key_id = $1")
                .bind(key_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row)
    }

    async fn get_signing_key_by_name(&self, key_name: &str) -> MetadataResult<Option<SigningKeyRow>> {
        let row =
            sqlx::query_as::<_, SigningKeyRow>("SELECT * FROM signing_keys WHERE key_name = $1")
                .bind(key_name)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row)
    }

    async fn update_signing_key_status(&self, key_id: Uuid, status: &str) -> MetadataResult<()> {
        sqlx::query("UPDATE signing_keys SET status = $1 WHERE key_id = $2")
            .bind(status)
            .bind(key_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_signing_keys(&self, cache_id: Option<Uuid>) -> MetadataResult<Vec<SigningKeyRow>> {
        let rows = match cache_id {
            Some(id) => {
                sqlx::query_as::<_, SigningKeyRow>(
                    "SELECT * FROM signing_keys WHERE cache_id = $1 ORDER BY created_at DESC",
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
impl GcRepo for PostgresStore {
    async fn create_gc_job(&self, job: &GcJobRow) -> MetadataResult<()> {
        sqlx::query(
            r#"
            INSERT INTO gc_jobs (gc_job_id, cache_id, job_type, state, started_at, finished_at, stats_json)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
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

    async fn get_gc_job(&self, gc_job_id: Uuid) -> MetadataResult<Option<GcJobRow>> {
        let row =
            sqlx::query_as::<_, GcJobRow>("SELECT * FROM gc_jobs WHERE gc_job_id = $1")
                .bind(gc_job_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row)
    }

    async fn update_gc_job_state(
        &self,
        gc_job_id: Uuid,
        state: &str,
        finished_at: Option<OffsetDateTime>,
        stats_json: Option<&str>,
    ) -> MetadataResult<()> {
        sqlx::query(
            "UPDATE gc_jobs SET state = $1, finished_at = $2, stats_json = $3 WHERE gc_job_id = $4",
        )
        .bind(state)
        .bind(finished_at)
        .bind(stats_json)
        .bind(gc_job_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_recent_gc_jobs(
        &self,
        cache_id: Option<Uuid>,
        limit: u32,
    ) -> MetadataResult<Vec<GcJobRow>> {
        // Filter by cache_id for tenant isolation
        let rows = match cache_id {
            Some(id) => {
                sqlx::query_as::<_, GcJobRow>(
                    "SELECT * FROM gc_jobs WHERE cache_id = $1 ORDER BY COALESCE(started_at, 'epoch') DESC LIMIT $2",
                )
                .bind(id)
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, GcJobRow>(
                    "SELECT * FROM gc_jobs WHERE cache_id IS NULL ORDER BY COALESCE(started_at, 'epoch') DESC LIMIT $1",
                )
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows)
    }

    async fn get_running_gc_jobs(&self, cache_id: Option<Uuid>) -> MetadataResult<Vec<GcJobRow>> {
        // Filter by cache_id for tenant isolation
        let rows = match cache_id {
            Some(id) => {
                sqlx::query_as::<_, GcJobRow>(
                    "SELECT * FROM gc_jobs WHERE cache_id = $1 AND state = 'running' ORDER BY started_at DESC",
                )
                .bind(id)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, GcJobRow>(
                    "SELECT * FROM gc_jobs WHERE cache_id IS NULL AND state = 'running' ORDER BY started_at DESC",
                )
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows)
    }
}

// F-001: CacheRepo implementation
#[async_trait]
impl CacheRepo for PostgresStore {
    async fn create_cache(&self, cache: &CacheRow) -> MetadataResult<()> {
        sqlx::query(
            r#"
            INSERT INTO caches (cache_id, cache_name, public_base_url, is_public, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6)
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
        let row = sqlx::query_as::<_, CacheRow>("SELECT * FROM caches WHERE cache_id = $1")
            .bind(cache_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    async fn get_cache_by_name(&self, name: &str) -> MetadataResult<Option<CacheRow>> {
        let row = sqlx::query_as::<_, CacheRow>("SELECT * FROM caches WHERE cache_name = $1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    async fn update_cache(&self, cache: &CacheRow) -> MetadataResult<()> {
        sqlx::query(
            "UPDATE caches SET cache_name = $1, public_base_url = $2, is_public = $3, updated_at = $4 WHERE cache_id = $5",
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
        sqlx::query("DELETE FROM caches WHERE cache_id = $1")
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
impl TrustedKeyRepo for PostgresStore {
    async fn add_trusted_key(&self, key: &TrustedBuilderKeyRow) -> MetadataResult<()> {
        sqlx::query(
            r#"
            INSERT INTO trusted_builder_keys (
                trusted_key_id, cache_id, key_name, public_key, trust_level,
                added_by_token_id, created_at, expires_at, revoked_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
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
            "SELECT * FROM trusted_builder_keys WHERE trusted_key_id = $1",
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
                    "SELECT * FROM trusted_builder_keys WHERE cache_id = $1 AND key_name = $2",
                )
                .bind(id)
                .bind(key_name)
                .fetch_optional(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, TrustedBuilderKeyRow>(
                    "SELECT * FROM trusted_builder_keys WHERE cache_id IS NULL AND key_name = $1",
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
                    "SELECT * FROM trusted_builder_keys WHERE cache_id = $1 ORDER BY created_at DESC",
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
        sqlx::query("UPDATE trusted_builder_keys SET revoked_at = $1 WHERE trusted_key_id = $2")
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
        let count: i64 = match cache_id {
            Some(id) => {
                sqlx::query_scalar(
                    r#"
                    SELECT COUNT(*) FROM trusted_builder_keys
                    WHERE (cache_id = $1 OR cache_id IS NULL)
                      AND key_name = $2
                      AND revoked_at IS NULL
                      AND (expires_at IS NULL OR expires_at > $3)
                    "#,
                )
                .bind(id)
                .bind(key_name)
                .bind(now)
                .fetch_one(&self.pool)
                .await?
            }
            None => {
                sqlx::query_scalar(
                    r#"
                    SELECT COUNT(*) FROM trusted_builder_keys
                    WHERE cache_id IS NULL
                      AND key_name = $1
                      AND revoked_at IS NULL
                      AND (expires_at IS NULL OR expires_at > $2)
                    "#,
                )
                .bind(key_name)
                .bind(now)
                .fetch_one(&self.pool)
                .await?
            }
        };
        Ok(count > 0)
    }
}

// F-005: TombstoneRepo implementation
#[async_trait]
impl TombstoneRepo for PostgresStore {
    async fn create_tombstone(&self, tombstone: &TombstoneRow) -> MetadataResult<()> {
        sqlx::query(
            r#"
            INSERT INTO tombstones (
                tombstone_id, entity_type, entity_id, cache_id, deleted_at,
                deleted_by_token_id, reason, gc_eligible_at, gc_completed_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
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
            "SELECT * FROM tombstones WHERE entity_type = $1 AND entity_id = $2",
        )
        .bind(entity_type)
        .bind(entity_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn is_tombstoned(&self, entity_type: &str, entity_id: &str) -> MetadataResult<bool> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tombstones WHERE entity_type = $1 AND entity_id = $2 AND gc_completed_at IS NULL",
        )
        .bind(entity_type)
        .bind(entity_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(count > 0)
    }

    async fn get_gc_eligible_tombstones(&self, limit: u32) -> MetadataResult<Vec<TombstoneRow>> {
        let now = OffsetDateTime::now_utc();
        let rows = sqlx::query_as::<_, TombstoneRow>(
            "SELECT * FROM tombstones WHERE gc_completed_at IS NULL AND gc_eligible_at <= $1 ORDER BY gc_eligible_at LIMIT $2",
        )
        .bind(now)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn mark_gc_completed(
        &self,
        tombstone_id: Uuid,
        completed_at: OffsetDateTime,
    ) -> MetadataResult<()> {
        sqlx::query("UPDATE tombstones SET gc_completed_at = $1 WHERE tombstone_id = $2")
            .bind(completed_at)
            .bind(tombstone_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_tombstone(&self, tombstone_id: Uuid) -> MetadataResult<()> {
        sqlx::query("DELETE FROM tombstones WHERE tombstone_id = $1")
            .bind(tombstone_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

// F-006: ReachabilityRepo implementation
#[async_trait]
impl ReachabilityRepo for PostgresStore {
    async fn get_all_visible_manifests(&self, cache_id: Option<Uuid>) -> MetadataResult<Vec<String>> {
        // Filter by cache_id for tenant isolation
        let rows = match cache_id {
            Some(id) => {
                sqlx::query_scalar::<_, String>(
                    "SELECT DISTINCT manifest_hash FROM store_paths WHERE visibility_state = 'visible' AND cache_id = $1",
                )
                .bind(id)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_scalar::<_, String>(
                    "SELECT DISTINCT manifest_hash FROM store_paths WHERE visibility_state = 'visible' AND cache_id IS NULL",
                )
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows)
    }

    async fn get_chunks_for_manifest(&self, manifest_hash: &str) -> MetadataResult<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT chunk_hash FROM manifest_chunks WHERE manifest_hash = $1 ORDER BY position",
        )
        .bind(manifest_hash)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn get_chunk_refcount(&self, chunk_hash: &str) -> MetadataResult<Option<i32>> {
        let row = sqlx::query_scalar::<_, i32>("SELECT refcount FROM chunks WHERE chunk_hash = $1")
            .bind(chunk_hash)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    async fn set_chunk_refcount(&self, chunk_hash: &str, refcount: i32) -> MetadataResult<()> {
        sqlx::query("UPDATE chunks SET refcount = $1 WHERE chunk_hash = $2")
            .bind(refcount)
            .bind(chunk_hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn batch_update_refcounts(&self, updates: &[(String, i32)]) -> MetadataResult<u64> {
        let mut count = 0u64;
        for (chunk_hash, refcount) in updates {
            let result = sqlx::query("UPDATE chunks SET refcount = $1 WHERE chunk_hash = $2")
                .bind(refcount)
                .bind(chunk_hash)
                .execute(&self.pool)
                .await?;
            count += result.rows_affected();
        }
        Ok(count)
    }
}
