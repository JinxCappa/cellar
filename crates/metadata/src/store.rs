//! Metadata store trait and implementations.

use crate::error::{MetadataError, MetadataResult};
use crate::repos::{
    BootstrapRepo, CacheRepo, ChunkRepo, DomainRepo, GcRepo, ManifestRepo, ReachabilityRepo,
    StorePathRepo, TokenRepo, TombstoneRepo, TrustedKeyRepo, UploadRepo,
};
use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, Sqlite};
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

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
    + DomainRepo
    + TrustedKeyRepo
    + TombstoneRepo
    + ReachabilityRepo
    + BootstrapRepo
    + Send
    + Sync
{
    /// Run database migrations.
    async fn migrate(&self) -> MetadataResult<()>;

    /// Check database connectivity and health.
    async fn health_check(&self) -> MetadataResult<()>;
}

/// SQLite-based metadata store.
pub struct SqliteStore {
    pool: Pool<Sqlite>,
    #[allow(dead_code)] // Reserved for future timeout wrapper implementation
    query_timeout_secs: u64,
}

impl SqliteStore {
    /// Create a new SQLite store.
    pub async fn new(
        path: impl AsRef<Path>,
        query_timeout_secs: Option<u64>,
    ) -> MetadataResult<Self> {
        let path = path.as_ref();
        let query_timeout_secs = query_timeout_secs.unwrap_or(600); // 10 minutes default

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let opts = SqliteConnectOptions::from_str(&format!("sqlite:{}?mode=rwc", path.display()))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .foreign_keys(true)
            // Prevent transient "database is locked" errors under concurrent access.
            .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            // SQLite permits limited write concurrency; using a single connection avoids
            // persistent "database is locked" failures under test/axum concurrency.
            .max_connections(1)
            .connect_with(opts)
            .await?;

        let store = Self {
            pool,
            query_timeout_secs,
        };
        store.migrate().await?;

        // Log warning about SQLite timeout limitations (FIXED P2)
        tracing::warn!(
            query_timeout_secs = query_timeout_secs,
            "SQLite query timeout is advisory only - long queries may exceed timeout. \
             SQLite lacks statement cancellation like PostgreSQL's statement_timeout. \
             For production deployments with strict timeout requirements and multiple caches, \
             use PostgreSQL instead. SQLite is recommended for testing and single-cache deployments only."
        );

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
        // Drop the unique index on (upload_id, chunk_hash) if it exists (migration)
        // This index was added in commit a019380 but incorrectly prevents valid manifests
        // with duplicate chunks at different positions (e.g., NARs with repeated content).
        sqlx::query("DROP INDEX IF EXISTS idx_upload_chunks_hash")
            .execute(&self.pool)
            .await?;
        sqlx::query("DROP INDEX IF EXISTS idx_upload_expected_chunks_hash")
            .execute(&self.pool)
            .await?;

        // Drop old GC jobs partial index (renamed to idx_gc_jobs_cache_type_active)
        // This migration fixes the job isolation race condition where two concurrent
        // trigger_gc calls could both insert 'queued' jobs for the same cache+type.
        sqlx::query("DROP INDEX IF EXISTS idx_gc_jobs_cache_type_running")
            .execute(&self.pool)
            .await?;

        // Check for incompatible old schema: store_paths missing required columns.
        // The new schema requires cache_id as part of the composite unique index,
        // which can't be altered in SQLite. Users must delete the old database.
        let table_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='store_paths')",
        )
        .fetch_one(&self.pool)
        .await?;

        if table_exists {
            // Check if all required columns exist using PRAGMA
            let columns: Vec<(i32, String, String, i32, Option<String>, i32)> =
                sqlx::query_as("PRAGMA table_info(store_paths)")
                    .fetch_all(&self.pool)
                    .await?;

            let column_names: std::collections::HashSet<&str> = columns
                .iter()
                .map(|(_, name, _, _, _, _)| name.as_str())
                .collect();

            // Required columns that must exist for the current schema to work.
            // cache_id is critical (part of unique index), visibility_state and ca
            // are used in queries and would cause runtime errors if missing.
            let required_columns = ["cache_id", "visibility_state", "ca"];
            let missing: Vec<&str> = required_columns
                .iter()
                .filter(|col| !column_names.contains(*col))
                .copied()
                .collect();

            if !missing.is_empty() {
                return Err(MetadataError::Internal(format!(
                    "Incompatible database schema detected: store_paths table is missing columns: {}. \
                     This is an older schema version that cannot be automatically migrated. \
                     Please delete the database file and restart to create a fresh schema. \
                     For Docker: run 'docker compose down -v' to remove volumes.",
                    missing.join(", ")
                )));
            }
        }

        // Migrate upload_expected_chunks: add uploaded_data column if missing.
        // SQLite doesn't support ADD COLUMN IF NOT EXISTS, so we check first.
        let uec_table_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='upload_expected_chunks')",
        )
        .fetch_one(&self.pool)
        .await?;

        if uec_table_exists {
            let uec_columns: Vec<(i32, String, String, i32, Option<String>, i32)> =
                sqlx::query_as("PRAGMA table_info(upload_expected_chunks)")
                    .fetch_all(&self.pool)
                    .await?;

            let has_uploaded_data = uec_columns
                .iter()
                .any(|(_, name, _, _, _, _)| name == "uploaded_data");

            if !has_uploaded_data {
                sqlx::query("ALTER TABLE upload_expected_chunks ADD COLUMN uploaded_data INTEGER NOT NULL DEFAULT 0")
                    .execute(&self.pool)
                    .await?;
            }
        }

        // Migrate store_paths: add chunks_verified column if missing.
        // This column indicates all chunks were verified to exist in storage after upload.
        if table_exists {
            let sp_columns: Vec<(i32, String, String, i32, Option<String>, i32)> =
                sqlx::query_as("PRAGMA table_info(store_paths)")
                    .fetch_all(&self.pool)
                    .await?;

            let has_chunks_verified = sp_columns
                .iter()
                .any(|(_, name, _, _, _, _)| name == "chunks_verified");

            if !has_chunks_verified {
                sqlx::query(
                    "ALTER TABLE store_paths ADD COLUMN chunks_verified INTEGER NOT NULL DEFAULT 0",
                )
                .execute(&self.pool)
                .await?;
            }
        }

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
    use crate::repos::DomainUsage;
    use crate::repos::chunks::ChunkStats;
    use time::OffsetDateTime;
    use uuid::Uuid;

    #[async_trait]
    impl DomainRepo for SqliteStore {
        async fn create_domain(&self, domain: &DomainRow) -> MetadataResult<()> {
            if self.get_domain(domain.domain_id).await?.is_some() {
                return Err(MetadataError::AlreadyExists(format!(
                    "domain_id {} already exists",
                    domain.domain_id
                )));
            }
            if self
                .get_domain_by_name(&domain.domain_name)
                .await?
                .is_some()
            {
                return Err(MetadataError::AlreadyExists(format!(
                    "domain_name '{}' already exists",
                    domain.domain_name
                )));
            }

            sqlx::query(
                "INSERT INTO domains (domain_id, domain_name, is_default, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(domain.domain_id)
            .bind(&domain.domain_name)
            .bind(domain.is_default)
            .bind(domain.created_at)
            .bind(domain.updated_at)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn list_domains(&self) -> MetadataResult<Vec<DomainRow>> {
            let rows = sqlx::query_as::<_, DomainRow>("SELECT * FROM domains ORDER BY domain_name")
                .fetch_all(&self.pool)
                .await?;
            Ok(rows)
        }

        async fn get_domain(&self, domain_id: Uuid) -> MetadataResult<Option<DomainRow>> {
            let row = sqlx::query_as::<_, DomainRow>("SELECT * FROM domains WHERE domain_id = ?")
                .bind(domain_id)
                .fetch_optional(&self.pool)
                .await?;
            Ok(row)
        }

        async fn get_domain_by_name(&self, domain_name: &str) -> MetadataResult<Option<DomainRow>> {
            let row = sqlx::query_as::<_, DomainRow>("SELECT * FROM domains WHERE domain_name = ?")
                .bind(domain_name)
                .fetch_optional(&self.pool)
                .await?;
            Ok(row)
        }

        async fn update_domain(&self, domain: &DomainRow) -> MetadataResult<()> {
            let result = sqlx::query(
                "UPDATE domains SET domain_name = ?, updated_at = ? WHERE domain_id = ?",
            )
            .bind(&domain.domain_name)
            .bind(domain.updated_at)
            .bind(domain.domain_id)
            .execute(&self.pool)
            .await?;

            if result.rows_affected() == 0 {
                return Err(MetadataError::NotFound(format!(
                    "domain_id {} not found",
                    domain.domain_id
                )));
            }
            Ok(())
        }

        async fn delete_domain(&self, domain_id: Uuid) -> MetadataResult<()> {
            let result = sqlx::query("DELETE FROM domains WHERE domain_id = ?")
                .bind(domain_id)
                .execute(&self.pool)
                .await?;
            if result.rows_affected() == 0 {
                return Err(MetadataError::NotFound(format!(
                    "domain_id {} not found",
                    domain_id
                )));
            }
            Ok(())
        }

        async fn get_domain_usage(&self, domain_id: Uuid) -> MetadataResult<DomainUsage> {
            let caches: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM caches WHERE domain_id = ?")
                .bind(domain_id)
                .fetch_one(&self.pool)
                .await?;
            let upload_sessions: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM upload_sessions WHERE domain_id = ?")
                    .bind(domain_id)
                    .fetch_one(&self.pool)
                    .await?;
            let store_paths: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM store_paths WHERE domain_id = ?")
                    .bind(domain_id)
                    .fetch_one(&self.pool)
                    .await?;
            let chunks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE domain_id = ?")
                .bind(domain_id)
                .fetch_one(&self.pool)
                .await?;
            let manifests: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM manifests WHERE domain_id = ?")
                    .bind(domain_id)
                    .fetch_one(&self.pool)
                    .await?;

            Ok(DomainUsage {
                caches: caches as u64,
                upload_sessions: upload_sessions as u64,
                store_paths: store_paths as u64,
                chunks: chunks as u64,
                manifests: manifests as u64,
            })
        }
    }

    #[async_trait]
    impl UploadRepo for SqliteStore {
        async fn create_session(&self, session: &UploadSessionRow) -> MetadataResult<()> {
            sqlx::query(
                r#"
                INSERT INTO upload_sessions (
                    upload_id, cache_id, domain_id, store_path, store_path_hash,
                    nar_size, nar_hash, chunk_size, manifest_hash, state,
                    owner_token_id, created_at, updated_at, expires_at, trace_id,
                    error_code, error_detail, commit_started_at, commit_progress,
                    chunks_predeclared
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(session.upload_id)
            .bind(session.cache_id)
            .bind(session.domain_id)
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
            .bind(session.commit_started_at)
            .bind(&session.commit_progress)
            .bind(session.chunks_predeclared)
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

        async fn begin_commit_session(
            &self,
            upload_id: Uuid,
            updated_at: OffsetDateTime,
        ) -> MetadataResult<Option<UploadSessionRow>> {
            // Atomically get session and transition to 'committing' state.
            // The write operation acquires SQLite's exclusive lock, preventing concurrent commits.
            // Only transitions if current state is 'open'.
            let mut tx = self.pool.begin().await?;

            // Get session
            let mut session = sqlx::query_as::<_, UploadSessionRow>(
                "SELECT * FROM upload_sessions WHERE upload_id = ?",
            )
            .bind(upload_id)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some(ref mut s) = session
                && s.state == "open"
            {
                // Only update state if currently 'open'
                let result = sqlx::query("UPDATE upload_sessions SET state = ?, updated_at = ? WHERE upload_id = ? AND state = 'open'")
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

            tx.commit().await?;

            // Return session (state will be 'committing' if transition succeeded, original state otherwise)
            Ok(session)
        }

        async fn get_session_by_store_path(
            &self,
            cache_id: Option<Uuid>,
            store_path_hash: &str,
        ) -> MetadataResult<Option<UploadSessionRow>> {
            // Filter by cache_id to ensure tenant isolation.
            // Use ORDER BY created_at DESC LIMIT 1 to pick the most recent session
            // deterministically when multiple open sessions exist for the same store path.
            let row = match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, UploadSessionRow>(
                        "SELECT * FROM upload_sessions WHERE cache_id = ? AND store_path_hash = ? AND state = 'open' ORDER BY created_at DESC LIMIT 1",
                    )
                    .bind(id)
                    .bind(store_path_hash)
                    .fetch_optional(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as::<_, UploadSessionRow>(
                        "SELECT * FROM upload_sessions WHERE cache_id IS NULL AND store_path_hash = ? AND state = 'open' ORDER BY created_at DESC LIMIT 1",
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
            uploaded_data: bool,
            chunks_predeclared: bool,
        ) -> MetadataResult<()> {
            // Try UPDATE first (for pre-defined expected_chunks)
            // Only upgrade uploaded_data from false to true, never downgrade (use MAX logic for SQLite)
            let result = sqlx::query(
                "UPDATE upload_expected_chunks SET received_at = ?, uploaded_data = MAX(uploaded_data, ?) WHERE upload_id = ? AND chunk_hash = ?",
            )
            .bind(received_at)
            .bind(uploaded_data)
            .bind(upload_id)
            .bind(chunk_hash)
            .execute(&self.pool)
            .await?;

            if result.rows_affected() > 0 {
                // Successfully updated existing expected chunk
                return Ok(());
            }

            // SECURITY: For predeclared uploads, refuse to insert new rows.
            // This provides defense-in-depth against undeclared chunk injection.
            // The handler should have already validated this, but we enforce here too.
            if chunks_predeclared {
                return Err(MetadataError::NotFound(format!(
                    "chunk {} not in expected_chunks for predeclared upload {}",
                    chunk_hash, upload_id
                )));
            }

            // If UPDATE didn't match any rows, INSERT a new row (for dynamic uploads without expected_chunks)
            // This matches PostgreSQL behavior and fixes resume/missing endpoints for uploads without pre-defined chunks
            // Use negative positions (starting from -1, -2, ...) to avoid conflicts with
            // expected chunks (which use positions 0, 1, 2, ...).
            //
            // Use atomic INSERT...SELECT WHERE NOT EXISTS to prevent TOCTOU race conditions.
            // Without this, two concurrent uploads of the same chunk could both check existence,
            // both see nothing, and both INSERT with different negative positions, creating duplicates.
            const MAX_RETRIES: u32 = 5;
            let mut last_error = None;

            for _attempt in 0..MAX_RETRIES {
                // Atomic insert: compute next position and insert only if chunk_hash doesn't exist.
                // This single query prevents the TOCTOU race between checking existence and inserting.
                // Uses a CTE to ensure we always get a position value even when no rows exist yet.
                let insert_result = sqlx::query(
                    r#"
                    WITH new_position AS (
                        SELECT COALESCE(MIN(position), 0) - 1 AS pos
                        FROM upload_expected_chunks
                        WHERE upload_id = ?1
                    )
                    INSERT INTO upload_expected_chunks (upload_id, position, chunk_hash, size_bytes, received_at, uploaded_data)
                    SELECT ?1, np.pos, ?2, 0, ?3, ?4
                    FROM new_position np
                    WHERE NOT EXISTS (
                        SELECT 1 FROM upload_expected_chunks
                        WHERE upload_id = ?1 AND chunk_hash = ?2
                    )
                    "#,
                )
                .bind(upload_id)
                .bind(chunk_hash)
                .bind(received_at)
                .bind(uploaded_data)
                .execute(&self.pool)
                .await;

                match insert_result {
                    Ok(result) => {
                        if result.rows_affected() > 0 {
                            // Successfully inserted new tracking row
                            return Ok(());
                        }
                        // rows_affected == 0: chunk_hash already exists (concurrent insert won the race)
                        // Update received_at and uploaded_data (only upgrade, never downgrade)
                        sqlx::query(
                            "UPDATE upload_expected_chunks SET received_at = COALESCE(received_at, ?), uploaded_data = MAX(uploaded_data, ?) WHERE upload_id = ? AND chunk_hash = ?",
                        )
                        .bind(received_at)
                        .bind(uploaded_data)
                        .bind(upload_id)
                        .bind(chunk_hash)
                        .execute(&self.pool)
                        .await?;
                        return Ok(());
                    }
                    Err(e) => {
                        // Check if it's a primary key constraint violation
                        // SQLite error: "UNIQUE constraint failed: upload_expected_chunks.upload_id, upload_expected_chunks.position"
                        if let sqlx::Error::Database(ref db_err) = e
                            && db_err.message().contains("UNIQUE constraint")
                            && db_err.message().contains("position")
                        {
                            // Position race condition - retry with new position
                            last_error = Some(e);
                            continue;
                        }
                        // Other error - return immediately
                        return Err(e.into());
                    }
                }
            }

            // Exhausted retries
            if let Some(e) = last_error {
                return Err(e.into());
            }
            Err(MetadataError::Internal(
                "failed to mark chunk received after retries".to_string(),
            ))
        }

        async fn get_deduped_only_chunks(&self, upload_id: Uuid) -> MetadataResult<Vec<String>> {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT chunk_hash FROM upload_expected_chunks WHERE upload_id = ? AND received_at IS NOT NULL AND uploaded_data = 0 ORDER BY position",
            )
            .bind(upload_id)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows.into_iter().map(|(h,)| h).collect())
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

        async fn get_expected_chunk(
            &self,
            upload_id: Uuid,
            chunk_hash: &str,
        ) -> MetadataResult<Option<UploadExpectedChunkRow>> {
            let row = sqlx::query_as::<_, UploadExpectedChunkRow>(
                "SELECT * FROM upload_expected_chunks WHERE upload_id = ? AND chunk_hash = ?",
            )
            .bind(upload_id)
            .bind(chunk_hash)
            .fetch_optional(&self.pool)
            .await?;
            Ok(row)
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

        async fn get_expired_sessions(
            &self,
            cache_id: Option<Uuid>,
            limit: u32,
        ) -> MetadataResult<Vec<UploadSessionRow>> {
            let now = OffsetDateTime::now_utc();
            let rows = match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, UploadSessionRow>(
                        "SELECT * FROM upload_sessions WHERE cache_id = ? AND state = 'open' AND expires_at < ? LIMIT ?",
                    )
                    .bind(id)
                    .bind(now)
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as::<_, UploadSessionRow>(
                        "SELECT * FROM upload_sessions WHERE cache_id IS NULL AND state = 'open' AND expires_at < ? LIMIT ?",
                    )
                    .bind(now)
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await?
                }
            };
            Ok(rows)
        }

        async fn get_stuck_committing_sessions(
            &self,
            cache_id: Option<Uuid>,
            older_than: OffsetDateTime,
            limit: u32,
        ) -> MetadataResult<Vec<UploadSessionRow>> {
            let rows = match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, UploadSessionRow>(
                        "SELECT * FROM upload_sessions WHERE cache_id = ? AND state = 'committing' AND updated_at < ? ORDER BY updated_at LIMIT ?",
                    )
                    .bind(id)
                    .bind(older_than)
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as::<_, UploadSessionRow>(
                        "SELECT * FROM upload_sessions WHERE cache_id IS NULL AND state = 'committing' AND updated_at < ? ORDER BY updated_at LIMIT ?",
                    )
                    .bind(older_than)
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await?
                }
            };
            Ok(rows)
        }

        async fn has_active_sessions_since(
            &self,
            since_time: OffsetDateTime,
        ) -> MetadataResult<bool> {
            let exists = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(
                    SELECT 1 FROM upload_sessions
                    WHERE (created_at >= ? OR updated_at >= ?)
                      AND state IN ('open', 'committing')
                )",
            )
            .bind(since_time)
            .bind(since_time)
            .fetch_one(&self.pool)
            .await?;
            Ok(exists)
        }

        async fn has_active_sessions_for_cache_since(
            &self,
            cache_id: Uuid,
            since_time: OffsetDateTime,
        ) -> MetadataResult<bool> {
            let exists = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(
                    SELECT 1 FROM upload_sessions
                    WHERE cache_id = ?
                      AND (created_at >= ? OR updated_at >= ?)
                      AND state IN ('open', 'committing')
                )",
            )
            .bind(cache_id)
            .bind(since_time)
            .bind(since_time)
            .fetch_one(&self.pool)
            .await?;
            Ok(exists)
        }

        async fn is_chunk_expected_in_active_sessions_since(
            &self,
            chunk_hash: &str,
            since_time: OffsetDateTime,
        ) -> MetadataResult<bool> {
            let exists = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(
                    SELECT 1
                    FROM upload_expected_chunks
                    JOIN upload_sessions ON upload_sessions.upload_id = upload_expected_chunks.upload_id
                    WHERE upload_expected_chunks.chunk_hash = ?
                      AND (upload_sessions.created_at >= ? OR upload_sessions.updated_at >= ?)
                      AND upload_sessions.state IN ('open', 'committing')
                )",
            )
            .bind(chunk_hash)
            .bind(since_time)
            .bind(since_time)
            .fetch_one(&self.pool)
            .await?;
            Ok(exists)
        }

        async fn delete_session(&self, upload_id: Uuid) -> MetadataResult<()> {
            // Wrap both DELETEs in transaction to ensure atomicity
            // If either fails, both are rolled back
            let mut tx = self.pool.begin().await?;

            sqlx::query("DELETE FROM upload_expected_chunks WHERE upload_id = ?")
                .bind(upload_id)
                .execute(&mut *tx)
                .await?;

            sqlx::query("DELETE FROM upload_sessions WHERE upload_id = ?")
                .bind(upload_id)
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;
            Ok(())
        }

        async fn count_active_uploads(&self, cache_id: Option<Uuid>) -> MetadataResult<u64> {
            let count: i64 = match cache_id {
                Some(id) => {
                    sqlx::query_scalar(
                        "SELECT COUNT(*) FROM upload_sessions WHERE cache_id = ? AND state IN ('open', 'committing')",
                    )
                    .bind(id)
                    .fetch_one(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_scalar(
                        "SELECT COUNT(*) FROM upload_sessions WHERE state IN ('open', 'committing')",
                    )
                    .fetch_one(&self.pool)
                    .await?
                }
            };
            Ok(count as u64)
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

        async fn update_commit_progress(
            &self,
            upload_id: Uuid,
            progress_json: &str,
            updated_at: OffsetDateTime,
        ) -> MetadataResult<()> {
            sqlx::query(
                "UPDATE upload_sessions
                 SET commit_progress = ?, updated_at = ?
                 WHERE upload_id = ?",
            )
            .bind(progress_json)
            .bind(updated_at)
            .bind(upload_id)
            .execute(&self.pool)
            .await?;
            Ok(())
        }
    }

    #[async_trait]
    impl ChunkRepo for SqliteStore {
        async fn upsert_chunk(&self, chunk: &ChunkRow) -> MetadataResult<()> {
            sqlx::query(
                r#"
                INSERT INTO chunks (domain_id, chunk_hash, size_bytes, object_key, refcount, created_at, last_accessed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(domain_id, chunk_hash) DO UPDATE SET last_accessed_at = excluded.last_accessed_at
                "#,
            )
            .bind(chunk.domain_id)
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

        async fn get_chunk(
            &self,
            domain_id: Uuid,
            chunk_hash: &str,
        ) -> MetadataResult<Option<ChunkRow>> {
            let row = sqlx::query_as::<_, ChunkRow>(
                "SELECT * FROM chunks WHERE domain_id = ? AND chunk_hash = ?",
            )
            .bind(domain_id)
            .bind(chunk_hash)
            .fetch_optional(&self.pool)
            .await?;
            Ok(row)
        }

        async fn get_chunks_batch(
            &self,
            domain_id: Uuid,
            chunk_hashes: &[String],
        ) -> MetadataResult<std::collections::HashMap<String, ChunkRow>> {
            use std::collections::HashMap;

            if chunk_hashes.is_empty() {
                return Ok(HashMap::new());
            }

            // Build query with dynamic IN clause
            // SQLite has a limit of ~999 parameters, so we batch if needed
            const BATCH_SIZE: usize = 900;
            let mut result = HashMap::with_capacity(chunk_hashes.len());

            for batch in chunk_hashes.chunks(BATCH_SIZE) {
                let placeholders: Vec<&str> = batch.iter().map(|_| "?").collect();
                let query = format!(
                    "SELECT * FROM chunks WHERE domain_id = ? AND chunk_hash IN ({})",
                    placeholders.join(", ")
                );

                let mut query_builder = sqlx::query_as::<_, ChunkRow>(&query);
                query_builder = query_builder.bind(domain_id);
                for hash in batch {
                    query_builder = query_builder.bind(hash);
                }

                let rows: Vec<ChunkRow> = query_builder.fetch_all(&self.pool).await?;
                for row in rows {
                    result.insert(row.chunk_hash.clone(), row);
                }
            }

            Ok(result)
        }

        async fn chunk_exists(&self, domain_id: Uuid, chunk_hash: &str) -> MetadataResult<bool> {
            let row: Option<(i32,)> =
                sqlx::query_as("SELECT 1 FROM chunks WHERE domain_id = ? AND chunk_hash = ?")
                    .bind(domain_id)
                    .bind(chunk_hash)
                    .fetch_optional(&self.pool)
                    .await?;
            Ok(row.is_some())
        }

        async fn increment_refcount(
            &self,
            domain_id: Uuid,
            chunk_hash: &str,
        ) -> MetadataResult<()> {
            sqlx::query(
                "UPDATE chunks SET refcount = refcount + 1 WHERE domain_id = ? AND chunk_hash = ?",
            )
            .bind(domain_id)
            .bind(chunk_hash)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn decrement_refcount(
            &self,
            domain_id: Uuid,
            chunk_hash: &str,
        ) -> MetadataResult<()> {
            sqlx::query(
                "UPDATE chunks SET refcount = MAX(0, refcount - 1) WHERE domain_id = ? AND chunk_hash = ?",
            )
            .bind(domain_id)
            .bind(chunk_hash)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn batch_increment_refcounts(
            &self,
            domain_id: Uuid,
            chunk_hashes: &[String],
        ) -> MetadataResult<()> {
            if chunk_hashes.is_empty() {
                return Ok(());
            }

            // Single transaction for all increments - all succeed or all fail
            let mut tx = self.pool.begin().await?;

            for chunk_hash in chunk_hashes {
                sqlx::query(
                    "UPDATE chunks SET refcount = refcount + 1 WHERE domain_id = ? AND chunk_hash = ?",
                )
                .bind(domain_id)
                .bind(chunk_hash)
                .execute(&mut *tx)
                .await?;
            }

            tx.commit().await?;
            Ok(())
        }

        async fn touch_chunk(
            &self,
            domain_id: Uuid,
            chunk_hash: &str,
            accessed_at: OffsetDateTime,
        ) -> MetadataResult<()> {
            sqlx::query(
                "UPDATE chunks SET last_accessed_at = ? WHERE domain_id = ? AND chunk_hash = ?",
            )
            .bind(accessed_at)
            .bind(domain_id)
            .bind(chunk_hash)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn get_unreferenced_chunks(
            &self,
            domain_id: Uuid,
            older_than: OffsetDateTime,
            limit: u32,
        ) -> MetadataResult<Vec<ChunkRow>> {
            // Exclude chunks that are part of open or committing upload sessions to prevent
            // GC from deleting chunks for in-progress uploads (which temporarily have refcount=0).
            // Protection covers BOTH uploads with expected_chunks AND uploads without them.
            let rows = sqlx::query_as::<_, ChunkRow>(
                r#"
                SELECT * FROM chunks
                WHERE domain_id = ?
                  AND refcount = 0
                  AND created_at < ?
                  AND chunk_hash NOT IN (
                    -- Exclude chunks from uploads with expected_chunks
                    SELECT uec.chunk_hash FROM upload_expected_chunks uec
                    INNER JOIN upload_sessions us ON uec.upload_id = us.upload_id
                    WHERE us.state IN ('open', 'committing')
                      AND us.domain_id = ?
                  )
                  AND (
                    -- Also protect chunks created after any open/committing session started
                    -- (for uploads without expected_chunks that upload dynamically)
                    NOT EXISTS (
                      SELECT 1 FROM upload_sessions us2
                      WHERE us2.state IN ('open', 'committing')
                        AND us2.domain_id = ?
                        AND chunks.created_at >= us2.created_at
                    )
                  )
                ORDER BY created_at
                LIMIT ?
                "#,
            )
            .bind(domain_id)
            .bind(older_than)
            .bind(domain_id)
            .bind(domain_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows)
        }

        async fn delete_unreferenced_chunks_atomic(
            &self,
            domain_id: Uuid,
            older_than: OffsetDateTime,
            limit: u32,
        ) -> MetadataResult<Vec<ChunkRow>> {
            // ATOMIC DELETION using DELETE...RETURNING to prevent TOCTOU race:
            // SQLite's DELETE with RETURNING is atomic - it deletes and returns rows
            // in a single operation with an exclusive lock, preventing concurrent
            // refcount increments from causing data loss.
            //
            // Previously, this used BEGIN IMMEDIATE inside self.pool.begin(), which
            // caused "cannot start a transaction within a transaction" errors.
            //
            // Now uses DELETE...RETURNING with a subquery (atomic in SQLite):
            // - Subquery identifies chunks to delete with refcount=0
            // - DELETE atomically removes them and returns the deleted rows
            // - Concurrent commits block until DELETE completes (no race possible)
            //
            // Note: We cannot use DELETE...RETURNING with NOT IN subquery directly,
            // so we use a two-step approach with a transaction.
            let mut tx = self.pool.begin().await?;

            // Select chunks to delete (holds read lock)
            // Protection covers BOTH uploads with expected_chunks AND uploads without them
            // Three-layer protection to prevent deletion of chunks being used by active uploads
            let chunks = sqlx::query_as::<_, ChunkRow>(
                r#"
                SELECT * FROM chunks
                WHERE domain_id = ?
                  AND refcount = 0
                  AND created_at < ?
                  AND chunk_hash NOT IN (
                    -- Protection 1: Exclude chunks from uploads with expected_chunks.
                    -- This protects uploads that pre-declare their chunk list.
                    SELECT uec.chunk_hash FROM upload_expected_chunks uec
                    INNER JOIN upload_sessions us ON uec.upload_id = us.upload_id
                    WHERE us.state IN ('open', 'committing')
                      AND us.domain_id = ?
                  )
                  AND NOT EXISTS (
                    -- Protection 2: Protect newly uploaded chunks.
                    -- Correlate chunk to session by creation time - protects chunks
                    -- uploaded during ANY active session's lifetime.
                    SELECT 1 FROM upload_sessions us2
                    WHERE us2.state IN ('open', 'committing')
                      AND us2.domain_id = ?
                      AND chunks.created_at >= us2.created_at
                  )
                ORDER BY created_at
                LIMIT ?
                "#,
            )
            .bind(domain_id)
            .bind(older_than)
            .bind(domain_id)
            .bind(domain_id)
            .bind(limit)
            .fetch_all(&mut *tx)
            .await?;

            // Delete while holding transaction lock (defensive refcount=0 check)
            // Transaction lock ensures no concurrent writes between SELECT and DELETE
            // CRITICAL FIX: Only return chunks that were actually deleted
            let mut deleted_chunks = Vec::new();
            for chunk in chunks {
                // TOCTOU FIX: Re-check that no cache has incremented refcount since selection
                let deleted = sqlx::query(
                    r#"
                    DELETE FROM chunks
                    WHERE domain_id = ?
                      AND chunk_hash = ?
                      AND refcount = 0
                    "#,
                )
                .bind(domain_id)
                .bind(&chunk.chunk_hash)
                .execute(&mut *tx)
                .await?;

                if deleted.rows_affected() > 0 {
                    // Chunk was actually deleted, safe to return for storage cleanup
                    deleted_chunks.push(chunk);
                } else {
                    // Refcount changed between SELECT and DELETE, skip this chunk
                    tracing::debug!(
                        chunk_hash = %chunk.chunk_hash,
                        "Chunk refcount changed, skipping deletion (storage preserved)"
                    );
                }
            }

            // Commit atomically (releases lock)
            tx.commit().await?;

            // CRITICAL: Only return chunks that were actually deleted from metadata.
            // Returning chunks that weren't deleted would cause storage deletion for
            // chunks that are still referenced (DATA LOSS).
            Ok(deleted_chunks)
        }

        async fn delete_chunk(&self, domain_id: Uuid, chunk_hash: &str) -> MetadataResult<()> {
            sqlx::query("DELETE FROM chunks WHERE domain_id = ? AND chunk_hash = ?")
                .bind(domain_id)
                .bind(chunk_hash)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn get_stats(&self, domain_id: Uuid) -> MetadataResult<ChunkStats> {
            let total: (i64, i64) = sqlx::query_as(
                "SELECT COUNT(*), COALESCE(SUM(size_bytes), 0) FROM chunks WHERE domain_id = ?",
            )
            .bind(domain_id)
            .fetch_one(&self.pool)
            .await?;

            let referenced: (i64,) =
                sqlx::query_as("SELECT COUNT(*) FROM chunks WHERE domain_id = ? AND refcount > 0")
                    .bind(domain_id)
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

            // Try to insert the manifest first
            let result = sqlx::query(
                r#"
                INSERT OR IGNORE INTO manifests (domain_id, manifest_hash, chunk_size, chunk_count, nar_size, object_key, refcount, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(manifest.domain_id)
            .bind(&manifest.manifest_hash)
            .bind(manifest.chunk_size)
            .bind(manifest.chunk_count)
            .bind(manifest.nar_size)
            .bind(&manifest.object_key)
            .bind(manifest.refcount)
            .bind(manifest.created_at)
            .execute(&mut *tx)
            .await?;

            let inserted = result.rows_affected() > 0;

            if inserted {
                // Manifest was created, insert chunk mappings
                for chunk in chunks {
                    sqlx::query(
                        "INSERT INTO manifest_chunks (domain_id, manifest_hash, position, chunk_hash) VALUES (?, ?, ?, ?)",
                    )
                    .bind(chunk.domain_id)
                    .bind(&chunk.manifest_hash)
                    .bind(chunk.position)
                    .bind(&chunk.chunk_hash)
                    .execute(&mut *tx)
                    .await?;
                }
            } else {
                // Manifest already exists, refresh created_at timestamp to protect against GC.
                // When reusing an old manifest (from hours/days ago), the stale timestamp would
                // bypass grace period protection, allowing GC to delete the manifest mid-reupload.
                // By updating created_at, we reset the grace period clock and prevent premature deletion.
                sqlx::query(
                    "UPDATE manifests SET created_at = ? WHERE domain_id = ? AND manifest_hash = ?",
                )
                .bind(manifest.created_at)
                .bind(manifest.domain_id)
                .bind(&manifest.manifest_hash)
                .execute(&mut *tx)
                .await?;
            }

            tx.commit().await?;
            Ok(inserted)
        }

        async fn create_manifest_with_initial_refcount(
            &self,
            manifest: &ManifestRow,
            chunks: &[ManifestChunkRow],
        ) -> MetadataResult<bool> {
            // ATOMIC TRANSACTION: Combines manifest creation and refcount initialization.
            // This eliminates the race condition where GC could delete a manifest
            // between create_manifest() and increment_manifest_refcount().
            let mut tx = self.pool.begin().await?;

            // Step 1: Try to insert the manifest
            let result = sqlx::query(
                r#"
                INSERT OR IGNORE INTO manifests (domain_id, manifest_hash, chunk_size, chunk_count, nar_size, object_key, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(manifest.domain_id)
            .bind(&manifest.manifest_hash)
            .bind(manifest.chunk_size)
            .bind(manifest.chunk_count)
            .bind(manifest.nar_size)
            .bind(&manifest.object_key)
            .bind(manifest.created_at)
            .execute(&mut *tx)
            .await?;

            let inserted = result.rows_affected() > 0;

            // Step 2: Insert chunk mappings or refresh timestamp
            if inserted {
                // Manifest was created, insert chunk mappings
                for chunk in chunks {
                    sqlx::query(
                        "INSERT INTO manifest_chunks (domain_id, manifest_hash, position, chunk_hash) VALUES (?, ?, ?, ?)",
                    )
                    .bind(chunk.domain_id)
                    .bind(&chunk.manifest_hash)
                    .bind(chunk.position)
                    .bind(&chunk.chunk_hash)
                    .execute(&mut *tx)
                    .await?;
                }
            } else {
                // Manifest already exists, refresh created_at timestamp for reupload protection
                sqlx::query(
                    "UPDATE manifests SET created_at = ? WHERE domain_id = ? AND manifest_hash = ?",
                )
                .bind(manifest.created_at)
                .bind(manifest.domain_id)
                .bind(&manifest.manifest_hash)
                .execute(&mut *tx)
                .await?;
            }

            // Step 3: Increment manifest refcount within the domain.
            sqlx::query(
                "UPDATE manifests SET refcount = refcount + 1 WHERE domain_id = ? AND manifest_hash = ?",
            )
            .bind(manifest.domain_id)
            .bind(&manifest.manifest_hash)
            .execute(&mut *tx)
            .await?;

            // All three steps committed atomically
            tx.commit().await?;
            Ok(inserted)
        }

        async fn get_manifest(
            &self,
            domain_id: Uuid,
            manifest_hash: &str,
        ) -> MetadataResult<Option<ManifestRow>> {
            let row = sqlx::query_as::<_, ManifestRow>(
                "SELECT * FROM manifests WHERE domain_id = ? AND manifest_hash = ?",
            )
            .bind(domain_id)
            .bind(manifest_hash)
            .fetch_optional(&self.pool)
            .await?;
            Ok(row)
        }

        async fn manifest_exists(
            &self,
            domain_id: Uuid,
            manifest_hash: &str,
        ) -> MetadataResult<bool> {
            let row: Option<(i32,)> =
                sqlx::query_as("SELECT 1 FROM manifests WHERE domain_id = ? AND manifest_hash = ?")
                    .bind(domain_id)
                    .bind(manifest_hash)
                    .fetch_optional(&self.pool)
                    .await?;
            Ok(row.is_some())
        }

        async fn get_manifest_chunks(
            &self,
            domain_id: Uuid,
            manifest_hash: &str,
        ) -> MetadataResult<Vec<String>> {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT chunk_hash FROM manifest_chunks WHERE domain_id = ? AND manifest_hash = ? ORDER BY position",
            )
            .bind(domain_id)
            .bind(manifest_hash)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows.into_iter().map(|r| r.0).collect())
        }

        async fn get_manifest_chunks_verified(
            &self,
            domain_id: Uuid,
            manifest_hash: &str,
        ) -> MetadataResult<Vec<String>> {
            // First, get the expected chunk count from the manifest.
            // This detects incomplete manifest_chunks (partial insert/corruption).
            let expected_count: Option<(i64,)> = sqlx::query_as(
                "SELECT chunk_count FROM manifests WHERE domain_id = ? AND manifest_hash = ?",
            )
            .bind(domain_id)
            .bind(manifest_hash)
            .fetch_optional(&self.pool)
            .await?;

            let expected_count = match expected_count {
                Some((count,)) => count as usize,
                None => {
                    return Err(crate::error::MetadataError::NotFound(format!(
                        "manifest not found: {manifest_hash}"
                    )));
                }
            };

            // Query chunk hashes with existence check via LEFT JOIN to chunks table.
            // This verifies all referenced chunks exist with refcount > 0 in a single query.
            // Note: SQLite INTEGER maps to i64 in sqlx (unlike Postgres where INTEGER is i32).
            let rows: Vec<(String, Option<i64>)> = sqlx::query_as(
                "SELECT mc.chunk_hash, c.refcount \
                 FROM manifest_chunks mc \
                 LEFT JOIN chunks c \
                   ON c.domain_id = mc.domain_id \
                  AND c.chunk_hash = mc.chunk_hash \
                  AND c.refcount > 0 \
                 WHERE mc.domain_id = ? \
                   AND mc.manifest_hash = ? \
                 ORDER BY mc.position",
            )
            .bind(domain_id)
            .bind(manifest_hash)
            .fetch_all(&self.pool)
            .await?;

            // Verify we got the expected number of chunk mappings
            if rows.len() != expected_count {
                return Err(crate::error::MetadataError::Internal(format!(
                    "manifest {manifest_hash} has incomplete chunk mappings: expected {expected_count}, found {}",
                    rows.len()
                )));
            }

            let mut chunk_hashes = Vec::with_capacity(rows.len());
            let mut missing_chunks = Vec::new();

            for (chunk_hash, refcount) in rows {
                if refcount.is_none() {
                    missing_chunks.push(chunk_hash.clone());
                }
                chunk_hashes.push(chunk_hash);
            }

            if !missing_chunks.is_empty() {
                return Err(crate::error::MetadataError::MissingChunks {
                    manifest_hash: manifest_hash.to_string(),
                    missing_chunks,
                });
            }

            Ok(chunk_hashes)
        }

        async fn get_manifests_using_chunk(
            &self,
            domain_id: Uuid,
            chunk_hash: &str,
        ) -> MetadataResult<Vec<String>> {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT DISTINCT manifest_hash FROM manifest_chunks WHERE domain_id = ? AND chunk_hash = ?",
            )
            .bind(domain_id)
            .bind(chunk_hash)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows.into_iter().map(|r| r.0).collect())
        }

        async fn delete_manifest(
            &self,
            domain_id: Uuid,
            manifest_hash: &str,
        ) -> MetadataResult<()> {
            // Wrap both DELETEs in transaction to ensure atomicity
            // If either fails, both are rolled back
            let mut tx = self.pool.begin().await?;

            sqlx::query("DELETE FROM manifest_chunks WHERE domain_id = ? AND manifest_hash = ?")
                .bind(domain_id)
                .bind(manifest_hash)
                .execute(&mut *tx)
                .await?;

            sqlx::query("DELETE FROM manifests WHERE domain_id = ? AND manifest_hash = ?")
                .bind(domain_id)
                .bind(manifest_hash)
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;
            Ok(())
        }

        async fn delete_manifest_with_refcount_decrement(
            &self,
            domain_id: Uuid,
            manifest_hash: &str,
        ) -> MetadataResult<u64> {
            // Wrap entire operation in transaction for atomicity
            let mut tx = self.pool.begin().await?;

            let refcount: Option<i64> = sqlx::query_scalar(
                "SELECT refcount FROM manifests WHERE domain_id = ? AND manifest_hash = ?",
            )
            .bind(domain_id)
            .bind(manifest_hash)
            .fetch_optional(&mut *tx)
            .await?;

            let Some(refcount) = refcount else {
                tx.rollback().await?;
                return Ok(0);
            };

            if refcount > 0 {
                tx.rollback().await?;
                return Ok(0);
            }

            // Get all chunk hashes for this manifest
            let mut chunks: Vec<(String,)> = sqlx::query_as(
                "SELECT chunk_hash FROM manifest_chunks WHERE domain_id = ? AND manifest_hash = ? ORDER BY position",
            )
            .bind(domain_id)
            .bind(manifest_hash)
            .fetch_all(&mut *tx)
            .await?;

            let chunk_count = chunks.len() as u64;

            // Sort chunk hashes to keep lock order deterministic.
            chunks.sort_by(|a, b| a.0.cmp(&b.0));

            for (chunk_hash,) in chunks {
                sqlx::query(
                    "UPDATE chunks SET refcount = MAX(0, refcount - 1) WHERE domain_id = ? AND chunk_hash = ?",
                )
                .bind(domain_id)
                .bind(&chunk_hash)
                .execute(&mut *tx)
                .await?;
            }

            sqlx::query("DELETE FROM manifest_chunks WHERE domain_id = ? AND manifest_hash = ?")
                .bind(domain_id)
                .bind(manifest_hash)
                .execute(&mut *tx)
                .await?;

            let deleted_rows = sqlx::query(
                "DELETE FROM manifests WHERE domain_id = ? AND manifest_hash = ? AND refcount = 0",
            )
            .bind(domain_id)
            .bind(manifest_hash)
            .execute(&mut *tx)
            .await?;

            if deleted_rows.rows_affected() == 0 {
                tx.rollback().await?;
                return Ok(0);
            }

            tx.commit().await?;
            Ok(chunk_count)
        }

        async fn get_orphaned_manifests(
            &self,
            domain_id: Uuid,
            older_than: OffsetDateTime,
            limit: u32,
        ) -> MetadataResult<Vec<ManifestRow>> {
            let rows = sqlx::query_as::<_, ManifestRow>(
                r#"
                SELECT * FROM manifests
                WHERE domain_id = ?
                  AND refcount = 0
                  AND created_at < ?
                ORDER BY created_at
                LIMIT ?
                "#,
            )
            .bind(domain_id)
            .bind(older_than)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows)
        }

        async fn increment_manifest_refcount(
            &self,
            domain_id: Uuid,
            manifest_hash: &str,
        ) -> MetadataResult<()> {
            sqlx::query(
                "UPDATE manifests SET refcount = refcount + 1 WHERE domain_id = ? AND manifest_hash = ?",
            )
            .bind(domain_id)
            .bind(manifest_hash)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn decrement_manifest_refcount(
            &self,
            domain_id: Uuid,
            manifest_hash: &str,
        ) -> MetadataResult<()> {
            sqlx::query(
                "UPDATE manifests SET refcount = refcount - 1 WHERE domain_id = ? AND manifest_hash = ? AND refcount > 0",
            )
            .bind(domain_id)
            .bind(manifest_hash)
            .execute(&self.pool)
            .await?;
            Ok(())
        }
    }

    #[async_trait]
    impl StorePathRepo for SqliteStore {
        async fn create_store_path(&self, store_path: &StorePathRow) -> MetadataResult<()> {
            // Use UPSERT with the unique index on (COALESCE(cache_id, zeroblob(16)), store_path_hash).
            // CRITICAL: cache_id is NOT updated on conflict - this prevents tenant hijacking where
            // an attacker could overwrite another tenant's store path by uploading the same hash.
            // CRITICAL: For reuploads, nar_hash/nar_size/manifest_hash are NOT updated here.
            // They will be updated atomically by complete_reupload() only after storage succeeds.
            // This prevents metadata corruption if reupload fails mid-commit.
            // NOTE: SQLite UPSERT conflict targets cannot use expressions like COALESCE(...),
            // so we use partial unique indexes and branch on cache_id.
            // NOTE: chunks_verified starts as false and is set to true by mark_chunks_verified()
            // after all storage operations complete successfully.
            let upsert_sql = if store_path.cache_id.is_some() {
                r#"
                INSERT INTO store_paths (
                    cache_id, domain_id, store_path_hash, store_path, nar_hash, nar_size, manifest_hash,
                    created_at, committed_at, visibility_state, uploader_token_id, ca, chunks_verified
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cache_id, store_path_hash) DO UPDATE SET
                    committed_at = excluded.committed_at,
                    visibility_state = excluded.visibility_state,
                    ca = excluded.ca,
                    chunks_verified = excluded.chunks_verified
                "#
            } else {
                r#"
                INSERT INTO store_paths (
                    cache_id, domain_id, store_path_hash, store_path, nar_hash, nar_size, manifest_hash,
                    created_at, committed_at, visibility_state, uploader_token_id, ca, chunks_verified
                ) VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(store_path_hash) WHERE cache_id IS NULL DO UPDATE SET
                    committed_at = excluded.committed_at,
                    visibility_state = excluded.visibility_state,
                    ca = excluded.ca,
                    chunks_verified = excluded.chunks_verified
                "#
            };
            let mut q = sqlx::query(upsert_sql);
            if store_path.cache_id.is_some() {
                q = q.bind(store_path.cache_id);
            }
            q.bind(store_path.domain_id)
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
                .bind(store_path.chunks_verified)
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

        async fn complete_reupload(
            &self,
            cache_id: Option<Uuid>,
            store_path_hash: &str,
            nar_hash: &str,
            nar_size: i64,
            manifest_hash: &str,
        ) -> MetadataResult<()> {
            let mut tx = self.pool.begin().await?;

            let existing: Option<(Uuid, String)> = match cache_id {
                Some(id) => {
                    sqlx::query_as(
                        "SELECT domain_id, manifest_hash FROM store_paths WHERE cache_id = ? AND store_path_hash = ?",
                    )
                    .bind(id)
                    .bind(store_path_hash)
                    .fetch_optional(&mut *tx)
                    .await?
                }
                None => {
                    sqlx::query_as(
                        "SELECT domain_id, manifest_hash FROM store_paths WHERE cache_id IS NULL AND store_path_hash = ?",
                    )
                    .bind(store_path_hash)
                    .fetch_optional(&mut *tx)
                    .await?
                }
            };

            let Some((domain_id, old_manifest_hash)) = existing else {
                tx.rollback().await?;
                return Err(MetadataError::NotFound(format!(
                    "store path not found: {}",
                    store_path_hash
                )));
            };

            let result = match cache_id {
                Some(id) => {
                    sqlx::query(
                        "UPDATE store_paths SET visibility_state = 'visible', nar_hash = ?, nar_size = ?, manifest_hash = ? WHERE cache_id = ? AND store_path_hash = ?",
                    )
                    .bind(nar_hash)
                    .bind(nar_size)
                    .bind(manifest_hash)
                    .bind(id)
                    .bind(store_path_hash)
                    .execute(&mut *tx)
                    .await?
                }
                None => {
                    sqlx::query(
                        "UPDATE store_paths SET visibility_state = 'visible', nar_hash = ?, nar_size = ?, manifest_hash = ? WHERE cache_id IS NULL AND store_path_hash = ?",
                    )
                    .bind(nar_hash)
                    .bind(nar_size)
                    .bind(manifest_hash)
                    .bind(store_path_hash)
                    .execute(&mut *tx)
                    .await?
                }
            };

            if result.rows_affected() == 0 {
                tx.rollback().await?;
                return Err(MetadataError::NotFound(format!(
                    "store path not found: {}",
                    store_path_hash
                )));
            }

            sqlx::query(
                "UPDATE manifests SET refcount = refcount - 1 WHERE domain_id = ? AND manifest_hash = ? AND refcount > 0",
            )
            .bind(domain_id)
            .bind(&old_manifest_hash)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(())
        }

        async fn upsert_narinfo(&self, narinfo: &NarInfoRow) -> MetadataResult<()> {
            // Use composite key for upsert - narinfo is scoped to cache_id
            // NOTE: SQLite UPSERT conflict targets cannot use expressions like COALESCE(...),
            // so we use partial unique indexes and branch on cache_id.
            let upsert_sql = if narinfo.cache_id.is_some() {
                r#"
                INSERT INTO narinfo_records (cache_id, store_path_hash, narinfo_object_key, content_hash, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(cache_id, store_path_hash) DO UPDATE SET
                    narinfo_object_key = excluded.narinfo_object_key,
                    content_hash = excluded.content_hash,
                    updated_at = excluded.updated_at
                "#
            } else {
                r#"
                INSERT INTO narinfo_records (cache_id, store_path_hash, narinfo_object_key, content_hash, created_at, updated_at)
                VALUES (NULL, ?, ?, ?, ?, ?)
                ON CONFLICT(store_path_hash) WHERE cache_id IS NULL DO UPDATE SET
                    narinfo_object_key = excluded.narinfo_object_key,
                    content_hash = excluded.content_hash,
                    updated_at = excluded.updated_at
                "#
            };
            let mut q = sqlx::query(upsert_sql);
            if narinfo.cache_id.is_some() {
                q = q.bind(narinfo.cache_id);
            }
            q.bind(&narinfo.store_path_hash)
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
            // NOTE: SQLite UPSERT conflict targets cannot use expressions like COALESCE(...),
            // so we use partial unique indexes and branch on cache_id.
            let upsert_sql = if signature.cache_id.is_some() {
                r#"
                INSERT INTO signatures (cache_id, store_path_hash, key_id, signature, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(cache_id, store_path_hash, key_id) DO UPDATE SET signature = excluded.signature
                "#
            } else {
                r#"
                INSERT INTO signatures (cache_id, store_path_hash, key_id, signature, created_at)
                VALUES (NULL, ?, ?, ?, ?)
                ON CONFLICT(store_path_hash, key_id) WHERE cache_id IS NULL DO UPDATE SET signature = excluded.signature
                "#
            };
            let mut q = sqlx::query(upsert_sql);
            if signature.cache_id.is_some() {
                q = q.bind(signature.cache_id);
            }
            q.bind(&signature.store_path_hash)
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
            let rows =
                match cache_id {
                    Some(id) => {
                        sqlx::query_as::<_, SignatureRow>(
                            "SELECT * FROM signatures WHERE cache_id = ? AND store_path_hash = ?",
                        )
                        .bind(id)
                        .bind(store_path_hash)
                        .fetch_all(&self.pool)
                        .await?
                    }
                    None => sqlx::query_as::<_, SignatureRow>(
                        "SELECT * FROM signatures WHERE cache_id IS NULL AND store_path_hash = ?",
                    )
                    .bind(store_path_hash)
                    .fetch_all(&self.pool)
                    .await?,
                };
            Ok(rows)
        }

        async fn delete_store_path(
            &self,
            cache_id: Option<Uuid>,
            store_path_hash: &str,
        ) -> MetadataResult<()> {
            let mut tx = self.pool.begin().await?;

            let existing: Option<(Uuid, String)> = match cache_id {
                Some(id) => {
                    sqlx::query_as(
                        "SELECT domain_id, manifest_hash FROM store_paths WHERE cache_id = ? AND store_path_hash = ?",
                    )
                    .bind(id)
                    .bind(store_path_hash)
                    .fetch_optional(&mut *tx)
                    .await?
                }
                None => {
                    sqlx::query_as(
                        "SELECT domain_id, manifest_hash FROM store_paths WHERE cache_id IS NULL AND store_path_hash = ?",
                    )
                    .bind(store_path_hash)
                    .fetch_optional(&mut *tx)
                    .await?
                }
            };

            let Some((domain_id, manifest_hash)) = existing else {
                tx.rollback().await?;
                return Ok(());
            };

            sqlx::query(
                "UPDATE manifests SET refcount = refcount - 1 WHERE domain_id = ? AND manifest_hash = ? AND refcount > 0",
            )
            .bind(domain_id)
            .bind(&manifest_hash)
            .execute(&mut *tx)
            .await?;

            // Delete child records first (using composite key), then store_paths
            // FK cascade should handle this, but be explicit for safety
            match cache_id {
                Some(id) => {
                    sqlx::query(
                        "DELETE FROM signatures WHERE cache_id = ? AND store_path_hash = ?",
                    )
                    .bind(id)
                    .bind(store_path_hash)
                    .execute(&mut *tx)
                    .await?;
                    sqlx::query(
                        "DELETE FROM narinfo_records WHERE cache_id = ? AND store_path_hash = ?",
                    )
                    .bind(id)
                    .bind(store_path_hash)
                    .execute(&mut *tx)
                    .await?;
                    sqlx::query("DELETE FROM store_path_references WHERE cache_id = ? AND store_path_hash = ?")
                        .bind(id)
                        .bind(store_path_hash)
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query(
                        "DELETE FROM store_paths WHERE cache_id = ? AND store_path_hash = ?",
                    )
                    .bind(id)
                    .bind(store_path_hash)
                    .execute(&mut *tx)
                    .await?;
                }
                None => {
                    sqlx::query(
                        "DELETE FROM signatures WHERE cache_id IS NULL AND store_path_hash = ?",
                    )
                    .bind(store_path_hash)
                    .execute(&mut *tx)
                    .await?;
                    sqlx::query("DELETE FROM narinfo_records WHERE cache_id IS NULL AND store_path_hash = ?")
                        .bind(store_path_hash)
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query("DELETE FROM store_path_references WHERE cache_id IS NULL AND store_path_hash = ?")
                        .bind(store_path_hash)
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query(
                        "DELETE FROM store_paths WHERE cache_id IS NULL AND store_path_hash = ?",
                    )
                    .bind(store_path_hash)
                    .execute(&mut *tx)
                    .await?;
                }
            }

            tx.commit().await?;
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

        async fn count_store_paths(&self, cache_id: Option<Uuid>) -> MetadataResult<u64> {
            // Filter by cache_id for tenant isolation
            let (count,): (i64,) = match cache_id {
                Some(id) => {
                    sqlx::query_as("SELECT COUNT(*) FROM store_paths WHERE visibility_state = 'visible' AND cache_id = ?")
                        .bind(id)
                        .fetch_one(&self.pool)
                        .await?
                }
                None => {
                    sqlx::query_as("SELECT COUNT(*) FROM store_paths WHERE visibility_state = 'visible' AND cache_id IS NULL")
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
                // NOTE: SQLite UPSERT conflict targets cannot use expressions like COALESCE(...),
                // so we use partial unique indexes and branch on cache_id.
                let insert_sql = if cache_id.is_some() {
                    r#"
                    INSERT INTO store_path_references (cache_id, store_path_hash, reference_hash, reference_type)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(cache_id, store_path_hash, reference_hash, reference_type) DO NOTHING
                    "#
                } else {
                    r#"
                    INSERT INTO store_path_references (cache_id, store_path_hash, reference_hash, reference_type)
                    VALUES (NULL, ?, ?, ?)
                    ON CONFLICT(store_path_hash, reference_hash, reference_type) WHERE cache_id IS NULL DO NOTHING
                    "#
                };
                let mut q = sqlx::query(insert_sql);
                if cache_id.is_some() {
                    q = q.bind(cache_id);
                }
                q.bind(store_path_hash)
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
            cache_id: Option<Uuid>,
            age_seconds: i64,
        ) -> MetadataResult<u64> {
            // Delete failed store paths that are older than the specified age, scoped to cache_id.
            // This allows a grace period for debugging before cleanup.
            // Compute cutoff time in Rust to ensure consistent RFC3339 format
            // (SQLite's datetime() returns 'YYYY-MM-DD HH:MM:SS' which doesn't
            // compare correctly with RFC3339 'YYYY-MM-DDTHH:MM:SSZ' stored by sqlx).
            // Use COALESCE to handle NULL committed_at (failed uploads that never completed).
            let cutoff = OffsetDateTime::now_utc() - time::Duration::seconds(age_seconds);
            let result = match cache_id {
                Some(id) => {
                    sqlx::query(
                        r#"
                        DELETE FROM store_paths
                        WHERE cache_id = ?
                          AND visibility_state = 'failed'
                          AND COALESCE(committed_at, created_at) < ?
                        "#,
                    )
                    .bind(id)
                    .bind(cutoff)
                    .execute(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query(
                        r#"
                        DELETE FROM store_paths
                        WHERE cache_id IS NULL
                          AND visibility_state = 'failed'
                          AND COALESCE(committed_at, created_at) < ?
                        "#,
                    )
                    .bind(cutoff)
                    .execute(&self.pool)
                    .await?
                }
            };
            Ok(result.rows_affected())
        }

        async fn mark_chunks_verified(
            &self,
            cache_id: Option<Uuid>,
            store_path_hash: &str,
        ) -> MetadataResult<()> {
            match cache_id {
                Some(id) => {
                    sqlx::query(
                        "UPDATE store_paths SET chunks_verified = 1 WHERE cache_id = ? AND store_path_hash = ?",
                    )
                    .bind(id)
                    .bind(store_path_hash)
                    .execute(&self.pool)
                    .await?;
                }
                None => {
                    sqlx::query(
                        "UPDATE store_paths SET chunks_verified = 1 WHERE cache_id IS NULL AND store_path_hash = ?",
                    )
                    .bind(store_path_hash)
                    .execute(&self.pool)
                    .await?;
                }
            }
            Ok(())
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
            let row = sqlx::query_as::<_, TokenRow>("SELECT * FROM tokens WHERE token_hash = ?")
                .bind(token_hash)
                .fetch_optional(&self.pool)
                .await?;
            Ok(row)
        }

        async fn get_token(&self, token_id: Uuid) -> MetadataResult<Option<TokenRow>> {
            let row = sqlx::query_as::<_, TokenRow>("SELECT * FROM tokens WHERE token_id = ?")
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
                    sqlx::query_as::<_, TokenRow>("SELECT * FROM tokens ORDER BY created_at DESC")
                        .fetch_all(&self.pool)
                        .await?
                }
            };
            Ok(rows)
        }

        async fn count_tokens_for_cache(&self, cache_id: Option<Uuid>) -> MetadataResult<u64> {
            let count: i64 = match cache_id {
                Some(id) => {
                    sqlx::query_scalar(
                        "SELECT COUNT(*) FROM tokens WHERE cache_id = ? AND revoked_at IS NULL AND (expires_at IS NULL OR datetime(expires_at) > datetime('now'))",
                    )
                    .bind(id)
                    .fetch_one(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_scalar(
                        "SELECT COUNT(*) FROM tokens WHERE revoked_at IS NULL AND (expires_at IS NULL OR datetime(expires_at) > datetime('now'))",
                    )
                    .fetch_one(&self.pool)
                    .await?
                }
            };
            Ok(count as u64)
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
            let row =
                sqlx::query_as::<_, SigningKeyRow>("SELECT * FROM signing_keys WHERE key_id = ?")
                    .bind(key_id)
                    .fetch_optional(&self.pool)
                    .await?;
            Ok(row)
        }

        async fn get_signing_key_by_name(
            &self,
            key_name: &str,
        ) -> MetadataResult<Option<SigningKeyRow>> {
            let row =
                sqlx::query_as::<_, SigningKeyRow>("SELECT * FROM signing_keys WHERE key_name = ?")
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
            let rows =
                match cache_id {
                    Some(id) => sqlx::query_as::<_, SigningKeyRow>(
                        "SELECT * FROM signing_keys WHERE cache_id = ? ORDER BY created_at DESC",
                    )
                    .bind(id)
                    .fetch_all(&self.pool)
                    .await?,
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

        async fn delete_signing_keys_for_cache(&self, cache_id: Uuid) -> MetadataResult<u64> {
            let result = sqlx::query("DELETE FROM signing_keys WHERE cache_id = ?")
                .bind(cache_id)
                .execute(&self.pool)
                .await?;
            Ok(result.rows_affected())
        }
    }

    #[async_trait]
    impl BootstrapRepo for SqliteStore {
        async fn get_bootstrap_token_id(&self) -> MetadataResult<Option<Uuid>> {
            let value: Option<String> =
                sqlx::query_scalar("SELECT bootstrap_token_id FROM bootstrap_state WHERE id = 1")
                    .fetch_optional(&self.pool)
                    .await?;
            Ok(value
                .filter(|id| !id.is_empty()) // Filter out empty strings (SQLite NULL handling)
                .map(|id| {
                    Uuid::parse_str(&id).map_err(|e| {
                        MetadataError::Internal(format!(
                            "invalid bootstrap_token_id uuid '{id}': {e}"
                        ))
                    })
                })
                .transpose()?)
        }

        async fn set_bootstrap_token_id(&self, token_id: Uuid) -> MetadataResult<()> {
            sqlx::query(
                r#"
                INSERT INTO bootstrap_state (id, bootstrap_token_id)
                VALUES (1, ?)
                ON CONFLICT(id) DO UPDATE
                SET bootstrap_token_id = excluded.bootstrap_token_id
                "#,
            )
            .bind(token_id.to_string())
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn clear_bootstrap_token_id(&self) -> MetadataResult<()> {
            sqlx::query("UPDATE bootstrap_state SET bootstrap_token_id = NULL WHERE id = 1")
                .execute(&self.pool)
                .await?;
            Ok(())
        }
    }

    #[async_trait]
    impl GcRepo for SqliteStore {
        async fn create_gc_job(&self, job: &GcJobRow) -> MetadataResult<()> {
            match sqlx::query(
                r#"
                INSERT INTO gc_jobs (gc_job_id, cache_id, job_type, state, started_at, finished_at, stats_json, sweep_checkpoint_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(job.gc_job_id)
            .bind(job.cache_id)
            .bind(&job.job_type)
            .bind(&job.state)
            .bind(job.started_at)
            .bind(job.finished_at)
            .bind(&job.stats_json)
            .bind(&job.sweep_checkpoint_json)
            .execute(&self.pool)
            .await
            {
                Ok(_) => Ok(()),
                Err(sqlx::Error::Database(db_err)) => {
                    // Check for unique constraint violation on the active jobs index.
                    // SQLite reports UNIQUE violations in two formats:
                    // 1. With columns: "UNIQUE constraint failed: gc_jobs.cache_id, gc_jobs.job_type"
                    // 2. With index name: "UNIQUE constraint failed: index 'idx_gc_jobs_cache_type_active'"
                    //
                    // We check for:
                    // 1. "UNIQUE constraint" in message (generic check)
                    // 2. "gc_jobs" in message (table name or index name containing table)
                    let msg = db_err.message();
                    let is_unique_violation =
                        msg.contains("UNIQUE constraint") && msg.contains("gc_jobs");

                    if is_unique_violation {
                        Err(crate::error::MetadataError::Constraint(
                            "Another GC job is already active for this cache".to_string()
                        ))
                    } else {
                        Err(sqlx::Error::Database(db_err).into())
                    }
                }
                Err(e) => Err(e.into()),
            }
        }

        async fn get_gc_job(&self, job_id: Uuid) -> MetadataResult<Option<GcJobRow>> {
            let row = sqlx::query_as::<_, GcJobRow>("SELECT * FROM gc_jobs WHERE gc_job_id = ?")
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
            sweep_checkpoint_json: Option<&str>,
        ) -> MetadataResult<()> {
            sqlx::query(
                "UPDATE gc_jobs SET state = ?, finished_at = ?, stats_json = ?, sweep_checkpoint_json = ? WHERE gc_job_id = ?",
            )
            .bind(state)
            .bind(finished_at)
            .bind(stats_json)
            .bind(sweep_checkpoint_json)
            .bind(job_id)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn get_recent_gc_jobs(
            &self,
            cache_id: Option<Uuid>,
            limit: u32,
        ) -> MetadataResult<Vec<GcJobRow>> {
            // Filter by cache_id for tenant isolation.
            // None = public/global jobs only (cache_id IS NULL).
            // Some(id) = filter to specific cache's jobs.
            let rows = match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, GcJobRow>(
                        "SELECT * FROM gc_jobs WHERE cache_id = ? ORDER BY started_at DESC LIMIT ?",
                    )
                    .bind(id)
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await?
                }
                None => {
                    // Filter to public/global jobs only (no cross-tenant visibility)
                    sqlx::query_as::<_, GcJobRow>(
                        "SELECT * FROM gc_jobs WHERE cache_id IS NULL ORDER BY started_at DESC LIMIT ?",
                    )
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await?
                }
            };
            Ok(rows)
        }

        async fn get_running_gc_jobs(
            &self,
            cache_id: Option<Uuid>,
        ) -> MetadataResult<Vec<GcJobRow>> {
            // Filter by cache_id for tenant isolation
            let rows = match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, GcJobRow>(
                        "SELECT * FROM gc_jobs WHERE cache_id = ? AND state = 'running'",
                    )
                    .bind(id)
                    .fetch_all(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as::<_, GcJobRow>(
                        "SELECT * FROM gc_jobs WHERE cache_id IS NULL AND state = 'running'",
                    )
                    .fetch_all(&self.pool)
                    .await?
                }
            };
            Ok(rows)
        }

        async fn get_active_gc_jobs(
            &self,
            cache_id: Option<Uuid>,
        ) -> MetadataResult<Vec<GcJobRow>> {
            // Get jobs in 'queued' or 'running' state for duplicate detection and job isolation
            // Filter by cache_id for tenant isolation
            let rows = match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, GcJobRow>(
                        "SELECT * FROM gc_jobs WHERE cache_id = ? AND state IN ('queued', 'running') ORDER BY started_at DESC",
                    )
                    .bind(id)
                    .fetch_all(&self.pool)
                    .await?
                }
                None => {
                    sqlx::query_as::<_, GcJobRow>(
                        "SELECT * FROM gc_jobs WHERE cache_id IS NULL AND state IN ('queued', 'running') ORDER BY started_at DESC",
                    )
                    .fetch_all(&self.pool)
                    .await?
                }
            };
            Ok(rows)
        }

        async fn get_orphaned_gc_jobs(&self) -> MetadataResult<Vec<GcJobRow>> {
            // Get ALL jobs (global and cache-scoped) in 'queued' or 'running' state.
            // No cache_id filter - we need to recover orphaned jobs for all caches.
            // These jobs are orphaned because the server crashed before they completed.
            let rows = sqlx::query_as::<_, GcJobRow>(
                "SELECT * FROM gc_jobs WHERE state IN ('queued', 'running') ORDER BY started_at DESC",
            )
            .fetch_all(&self.pool)
            .await?;
            Ok(rows)
        }

        async fn delete_gc_jobs_for_cache(&self, cache_id: Uuid) -> MetadataResult<u64> {
            let result = sqlx::query("DELETE FROM gc_jobs WHERE cache_id = ?")
                .bind(cache_id)
                .execute(&self.pool)
                .await?;
            Ok(result.rows_affected())
        }
    }

    // F-001: CacheRepo implementation
    #[async_trait]
    impl crate::repos::CacheRepo for SqliteStore {
        async fn create_cache(&self, cache: &CacheRow) -> MetadataResult<()> {
            match sqlx::query(
                r#"
                INSERT INTO caches (cache_id, domain_id, cache_name, public_base_url, is_public, is_default, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(cache.cache_id)
            .bind(cache.domain_id)
            .bind(&cache.cache_name)
            .bind(&cache.public_base_url)
            .bind(cache.is_public)
            .bind(cache.is_default)
            .bind(cache.created_at)
            .bind(cache.updated_at)
            .execute(&self.pool)
            .await
            {
                Ok(_) => Ok(()),
                Err(sqlx::Error::Database(db_err)) => {
                    let msg = db_err.message();
                    // Check for unique constraint violation on is_default index
                    // SQLite error: "UNIQUE constraint failed: caches.is_default"
                    if msg.contains("UNIQUE constraint") && msg.contains("is_default") {
                        Err(crate::error::MetadataError::Constraint(
                            "Another cache is already set as default".to_string(),
                        ))
                    } else {
                        Err(sqlx::Error::Database(db_err).into())
                    }
                }
                Err(e) => Err(e.into()),
            }
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
            match sqlx::query(
                "UPDATE caches SET cache_name = ?, public_base_url = ?, is_public = ?, is_default = ?, domain_id = ?, updated_at = ? WHERE cache_id = ?",
            )
            .bind(&cache.cache_name)
            .bind(&cache.public_base_url)
            .bind(cache.is_public)
            .bind(cache.is_default)
            .bind(cache.domain_id)
            .bind(cache.updated_at)
            .bind(cache.cache_id)
            .execute(&self.pool)
            .await
            {
                Ok(_) => Ok(()),
                Err(sqlx::Error::Database(db_err)) => {
                    let msg = db_err.message();
                    // Check for unique constraint violation on is_default index
                    if msg.contains("UNIQUE constraint") && msg.contains("is_default") {
                        Err(crate::error::MetadataError::Constraint(
                            "Another cache is already set as default".to_string(),
                        ))
                    } else {
                        Err(sqlx::Error::Database(db_err).into())
                    }
                }
                Err(e) => Err(e.into()),
            }
        }

        async fn delete_cache(&self, cache_id: Uuid) -> MetadataResult<()> {
            sqlx::query("DELETE FROM caches WHERE cache_id = ?")
                .bind(cache_id)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn delete_cache_with_cascade(
            &self,
            cache_id: Uuid,
        ) -> MetadataResult<CascadeDeleteStats> {
            let mut tx = self.pool.begin().await?;

            // Lock cache row to prevent concurrent modifications
            let cache_exists: Option<Vec<u8>> =
                sqlx::query_scalar("SELECT cache_id FROM caches WHERE cache_id = ?")
                    .bind(cache_id)
                    .fetch_optional(&mut *tx)
                    .await?;

            if cache_exists.is_none() {
                return Err(crate::error::MetadataError::NotFound(
                    "cache not found".to_string(),
                ));
            }

            // Check preconditions inside transaction to prevent TOCTOU races
            let upload_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM upload_sessions WHERE cache_id = ? AND state NOT IN ('committed', 'failed')"
            )
            .bind(cache_id)
            .fetch_one(&mut *tx)
            .await?;

            if upload_count > 0 {
                return Err(crate::error::MetadataError::Constraint(format!(
                    "cannot delete cache with {} active upload(s)",
                    upload_count
                )));
            }

            let store_path_count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM store_paths WHERE cache_id = ?")
                    .bind(cache_id)
                    .fetch_one(&mut *tx)
                    .await?;

            if store_path_count > 0 {
                return Err(crate::error::MetadataError::Constraint(format!(
                    "cannot delete cache with {} store path(s)",
                    store_path_count
                )));
            }

            let token_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM tokens WHERE cache_id = ? AND revoked_at IS NULL AND (expires_at IS NULL OR datetime(expires_at) > datetime('now'))"
            )
            .bind(cache_id)
            .fetch_one(&mut *tx)
            .await?;

            if token_count > 0 {
                return Err(crate::error::MetadataError::Constraint(format!(
                    "cannot delete cache with {} active token(s)",
                    token_count
                )));
            }

            // Delete cascade data within transaction
            let gc_jobs = sqlx::query("DELETE FROM gc_jobs WHERE cache_id = ?")
                .bind(cache_id)
                .execute(&mut *tx)
                .await?
                .rows_affected();

            let signing_keys = sqlx::query("DELETE FROM signing_keys WHERE cache_id = ?")
                .bind(cache_id)
                .execute(&mut *tx)
                .await?
                .rows_affected();

            let trusted_keys = sqlx::query("DELETE FROM trusted_builder_keys WHERE cache_id = ?")
                .bind(cache_id)
                .execute(&mut *tx)
                .await?
                .rows_affected();

            let tombstones = sqlx::query("DELETE FROM tombstones WHERE cache_id = ?")
                .bind(cache_id)
                .execute(&mut *tx)
                .await?
                .rows_affected();

            // Delete revoked or expired tokens (active tokens are prevented by the check above)
            let tokens = sqlx::query(
                "DELETE FROM tokens WHERE cache_id = ? AND \
                 (revoked_at IS NOT NULL OR (expires_at IS NOT NULL AND datetime(expires_at) <= datetime('now')))"
            )
            .bind(cache_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();

            // Delete upload data (expected chunks first due to foreign key)
            let upload_expected_chunks = sqlx::query(
                "DELETE FROM upload_expected_chunks WHERE upload_id IN \
                 (SELECT upload_id FROM upload_sessions WHERE cache_id = ?)",
            )
            .bind(cache_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();

            let upload_sessions = sqlx::query("DELETE FROM upload_sessions WHERE cache_id = ?")
                .bind(cache_id)
                .execute(&mut *tx)
                .await?
                .rows_affected();

            // Delete the cache itself
            sqlx::query("DELETE FROM caches WHERE cache_id = ?")
                .bind(cache_id)
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;

            Ok(CascadeDeleteStats {
                gc_jobs,
                signing_keys,
                trusted_keys,
                tombstones,
                upload_sessions,
                upload_expected_chunks,
                tokens,
            })
        }

        async fn list_caches(&self) -> MetadataResult<Vec<CacheRow>> {
            let rows = sqlx::query_as::<_, CacheRow>("SELECT * FROM caches ORDER BY cache_name")
                .fetch_all(&self.pool)
                .await?;
            Ok(rows)
        }

        async fn get_default_public_cache(&self) -> MetadataResult<Option<CacheRow>> {
            let row = sqlx::query_as::<_, CacheRow>(
                "SELECT * FROM caches WHERE is_default = 1 AND is_public = 1",
            )
            .fetch_optional(&self.pool)
            .await?;
            Ok(row)
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

        async fn get_trusted_key(
            &self,
            key_id: Uuid,
        ) -> MetadataResult<Option<TrustedBuilderKeyRow>> {
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
                None => sqlx::query_as::<_, TrustedBuilderKeyRow>(
                    "SELECT * FROM trusted_builder_keys WHERE cache_id IS NULL AND key_name = ?",
                )
                .bind(key_name)
                .fetch_optional(&self.pool)
                .await?,
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

        async fn delete_trusted_keys_for_cache(&self, cache_id: Uuid) -> MetadataResult<u64> {
            let result = sqlx::query("DELETE FROM trusted_builder_keys WHERE cache_id = ?")
                .bind(cache_id)
                .execute(&self.pool)
                .await?;
            Ok(result.rows_affected())
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

        async fn get_gc_eligible_tombstones(
            &self,
            limit: u32,
        ) -> MetadataResult<Vec<TombstoneRow>> {
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

        async fn delete_tombstones_for_cache(&self, cache_id: Uuid) -> MetadataResult<u64> {
            let result = sqlx::query("DELETE FROM tombstones WHERE cache_id = ?")
                .bind(cache_id)
                .execute(&self.pool)
                .await?;
            Ok(result.rows_affected())
        }
    }

    // F-006: ReachabilityRepo implementation
    #[async_trait]
    impl crate::repos::ReachabilityRepo for SqliteStore {
        async fn get_all_visible_manifests(&self, domain_id: Uuid) -> MetadataResult<Vec<String>> {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT DISTINCT manifest_hash FROM store_paths WHERE visibility_state = 'visible' AND domain_id = ?",
            )
            .bind(domain_id)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows.into_iter().map(|r| r.0).collect())
        }

        async fn get_chunks_for_manifest(
            &self,
            domain_id: Uuid,
            manifest_hash: &str,
        ) -> MetadataResult<Vec<String>> {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT chunk_hash FROM manifest_chunks WHERE domain_id = ? AND manifest_hash = ? ORDER BY position",
            )
            .bind(domain_id)
            .bind(manifest_hash)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows.into_iter().map(|r| r.0).collect())
        }

        async fn get_chunk_refcount(
            &self,
            domain_id: Uuid,
            chunk_hash: &str,
        ) -> MetadataResult<Option<i32>> {
            let row: Option<(i32,)> = sqlx::query_as(
                "SELECT refcount FROM chunks WHERE domain_id = ? AND chunk_hash = ?",
            )
            .bind(domain_id)
            .bind(chunk_hash)
            .fetch_optional(&self.pool)
            .await?;
            Ok(row.map(|r| r.0))
        }

        async fn set_chunk_refcount(
            &self,
            domain_id: Uuid,
            chunk_hash: &str,
            refcount: i32,
        ) -> MetadataResult<()> {
            sqlx::query("UPDATE chunks SET refcount = ? WHERE domain_id = ? AND chunk_hash = ?")
                .bind(refcount)
                .bind(domain_id)
                .bind(chunk_hash)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn batch_update_refcounts(
            &self,
            domain_id: Uuid,
            updates: &[(String, i32)],
        ) -> MetadataResult<u64> {
            let mut count = 0u64;
            for (chunk_hash, refcount) in updates {
                let result = sqlx::query(
                    "UPDATE chunks SET refcount = ? WHERE domain_id = ? AND chunk_hash = ?",
                )
                .bind(refcount)
                .bind(domain_id)
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
-- Storage domains
CREATE TABLE IF NOT EXISTS domains (
    domain_id BLOB PRIMARY KEY,
    domain_name TEXT NOT NULL UNIQUE,
    is_default INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_domains_name ON domains(domain_name);
CREATE UNIQUE INDEX IF NOT EXISTS idx_domains_default ON domains(is_default) WHERE is_default = 1;
INSERT OR IGNORE INTO domains (domain_id, domain_name, is_default, created_at, updated_at)
VALUES (X'00000000000000000000000000000001', 'default', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- F-001: Caches table
CREATE TABLE IF NOT EXISTS caches (
    cache_id BLOB PRIMARY KEY,
    domain_id BLOB NOT NULL DEFAULT X'00000000000000000000000000000001' REFERENCES domains(domain_id),
    cache_name TEXT NOT NULL UNIQUE,
    public_base_url TEXT,
    is_public INTEGER NOT NULL DEFAULT 0,
    is_default INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_caches_name ON caches(cache_name);
CREATE INDEX IF NOT EXISTS idx_caches_domain ON caches(domain_id);
-- Only one cache can be the default at a time (partial unique index)
CREATE UNIQUE INDEX IF NOT EXISTS idx_caches_default ON caches(is_default) WHERE is_default = 1;

-- Upload sessions (with F-004 error tracking and commit progress tracking)
CREATE TABLE IF NOT EXISTS upload_sessions (
    upload_id BLOB PRIMARY KEY,
    cache_id BLOB,
    domain_id BLOB NOT NULL DEFAULT X'00000000000000000000000000000001',
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
    error_detail TEXT,
    -- Commit progress tracking for stuck detection and recovery
    commit_started_at TEXT,
    commit_progress TEXT,
    -- Tracks whether expected_chunks were pre-declared at session creation.
    -- Used to correctly classify missing chunk errors as client vs server errors.
    chunks_predeclared INTEGER NOT NULL DEFAULT 0
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
    -- Tracks whether actual data was uploaded (1) vs just marked via dedup shortcircuit (0).
    -- Used to prevent cross-session chunk reference attacks when require_expected_chunks=false.
    uploaded_data INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (upload_id, position),
    FOREIGN KEY (upload_id) REFERENCES upload_sessions(upload_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_upload_chunks_received ON upload_expected_chunks(upload_id, received_at);
-- NOTE: No unique index on (upload_id, chunk_hash) - duplicate chunk hashes at different
-- positions are valid (e.g., NAR files with repeated content patterns).

-- Chunks
CREATE TABLE IF NOT EXISTS chunks (
    domain_id BLOB NOT NULL,
    chunk_hash TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    object_key TEXT,
    refcount INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    last_accessed_at TEXT,
    PRIMARY KEY (domain_id, chunk_hash)
);
CREATE INDEX IF NOT EXISTS idx_chunks_refcount ON chunks(domain_id, refcount, created_at);

-- Manifests
CREATE TABLE IF NOT EXISTS manifests (
    domain_id BLOB NOT NULL,
    manifest_hash TEXT NOT NULL,
    chunk_size INTEGER NOT NULL,
    chunk_count INTEGER NOT NULL,
    nar_size INTEGER NOT NULL,
    object_key TEXT,
    refcount INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    PRIMARY KEY (domain_id, manifest_hash)
);
CREATE INDEX IF NOT EXISTS idx_manifests_refcount ON manifests(domain_id, refcount, created_at);

-- Manifest chunks
CREATE TABLE IF NOT EXISTS manifest_chunks (
    domain_id BLOB NOT NULL,
    manifest_hash TEXT NOT NULL,
    position INTEGER NOT NULL,
    chunk_hash TEXT NOT NULL,
    PRIMARY KEY (domain_id, manifest_hash, position),
    FOREIGN KEY (domain_id, manifest_hash) REFERENCES manifests(domain_id, manifest_hash) ON DELETE CASCADE,
    FOREIGN KEY (domain_id, chunk_hash) REFERENCES chunks(domain_id, chunk_hash)
);
CREATE INDEX IF NOT EXISTS idx_manifest_chunks_chunk ON manifest_chunks(domain_id, chunk_hash);

-- Store paths (with F-003 CA field and tenant isolation)
-- Composite PRIMARY KEY (cache_id, store_path_hash) ensures tenant isolation:
-- the same store path can exist in different caches with different metadata.
-- cache_id = NULL represents a public/shared cache.
CREATE TABLE IF NOT EXISTS store_paths (
    cache_id BLOB,
    domain_id BLOB NOT NULL DEFAULT X'00000000000000000000000000000001',
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
    -- Indicates all chunks were verified to exist in storage after successful upload.
    -- Used to prevent serving truncated NARs if metadata references missing chunks.
    chunks_verified INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (cache_id, store_path_hash),
    FOREIGN KEY (domain_id, manifest_hash) REFERENCES manifests(domain_id, manifest_hash)
);
CREATE INDEX IF NOT EXISTS idx_store_paths_manifest ON store_paths(domain_id, manifest_hash);
CREATE INDEX IF NOT EXISTS idx_store_paths_hash ON store_paths(store_path_hash);
-- Unique index for upsert targeting with NULL cache_id handling.
-- COALESCE treats NULL as a sentinel value so ON CONFLICT works correctly.
CREATE UNIQUE INDEX IF NOT EXISTS idx_store_paths_cache_hash ON store_paths(
    COALESCE(cache_id, zeroblob(16)), store_path_hash
);
-- SQLite UPSERT conflict targets can't use expressions like COALESCE(...), so we also
-- create a partial unique index for the NULL cache_id (public cache) case.
CREATE UNIQUE INDEX IF NOT EXISTS idx_store_paths_null ON store_paths(store_path_hash)
    WHERE cache_id IS NULL;

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
-- SQLite UPSERT conflict targets can't use expressions like COALESCE(...), so we also
-- create a partial unique index for the NULL cache_id (public cache) case.
CREATE UNIQUE INDEX IF NOT EXISTS idx_store_path_refs_null ON store_path_references(store_path_hash, reference_hash, reference_type)
    WHERE cache_id IS NULL;
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
-- SQLite UPSERT conflict targets can't use expressions like COALESCE(...), so we also
-- create a partial unique index for the NULL cache_id (public cache) case.
CREATE UNIQUE INDEX IF NOT EXISTS idx_narinfo_null ON narinfo_records(store_path_hash)
    WHERE cache_id IS NULL;

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
-- SQLite UPSERT conflict targets can't use expressions like COALESCE(...), so we also
-- create a partial unique index for the NULL cache_id (public cache) case.
CREATE UNIQUE INDEX IF NOT EXISTS idx_signatures_null ON signatures(store_path_hash, key_id)
    WHERE cache_id IS NULL;

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

-- Bootstrap marker
CREATE TABLE IF NOT EXISTS bootstrap_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    bootstrap_token_id TEXT
);
INSERT OR IGNORE INTO bootstrap_state (id, bootstrap_token_id) VALUES (1, NULL);

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
    stats_json TEXT,
    sweep_checkpoint_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_gc_jobs_state ON gc_jobs(state);
-- Drop old partial index (renamed to idx_gc_jobs_cache_type_active)
DROP INDEX IF EXISTS idx_gc_jobs_cache_type_running;
-- Unique index to prevent concurrent GC jobs of same type for same cache
-- COALESCE handles NULL cache_id for global caches (maps to all-zeros BLOB)
-- Extended to cover both 'queued' and 'running' states (fixes race condition)
CREATE UNIQUE INDEX IF NOT EXISTS idx_gc_jobs_cache_type_active
ON gc_jobs(COALESCE(cache_id, zeroblob(16)), job_type)
WHERE state IN ('queued', 'running');

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
