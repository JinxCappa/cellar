//! PostgreSQL-based metadata store implementation.

use crate::error::{MetadataError, MetadataResult};
use crate::models::*;
use crate::repos::{
    BootstrapRepo, CacheRepo, ChunkRepo, DomainRepo, DomainUsage, GcRepo, ManifestRepo,
    ReachabilityRepo, StorePathRepo, TokenRepo, TombstoneRepo, TrustedKeyRepo, UploadRepo,
    chunks::ChunkStats,
};
use crate::store::MetadataStore;
use async_trait::async_trait;
use cellar_core::config::PgSslMode;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode as SqlxPgSslMode};
use sqlx::{Pool, Postgres};
use std::str::FromStr;
use time::OffsetDateTime;
use uuid::Uuid;

/// PostgreSQL schema (embedded).
const POSTGRES_SCHEMA: &str = include_str!("postgres_schema.sql");

fn postgres_schema_statements(schema: &str) -> Vec<&str> {
    schema
        .split(';')
        .filter_map(|statement| {
            let trimmed = statement.trim();
            if trimmed.is_empty() {
                return None;
            }
            let has_sql = trimmed.lines().any(|line| {
                let line = line.trim();
                !line.is_empty() && !line.starts_with("--")
            });
            has_sql.then_some(trimmed)
        })
        .collect()
}

/// PostgreSQL-based metadata store.
pub struct PostgresStore {
    pool: Pool<Postgres>,
}

impl PostgresStore {
    /// Create a new PostgreSQL store from a connection URL.
    ///
    /// This is the legacy constructor that accepts a full connection URL.
    /// For new deployments, prefer `from_params()` which allows individual
    /// credential fields and better secret management.
    pub async fn from_url(
        url: &str,
        max_connections: u32,
        statement_timeout_ms: Option<u64>,
    ) -> MetadataResult<Self> {
        let opts = PgConnectOptions::from_str(url)?;
        Self::connect(opts, max_connections, statement_timeout_ms).await
    }

    /// Create a new PostgreSQL store from individual connection parameters.
    ///
    /// This allows credentials to be passed separately, enabling better
    /// secret management (e.g., passwords via environment variables).
    #[allow(clippy::too_many_arguments)]
    pub async fn from_params(
        host: &str,
        port: u16,
        username: Option<&str>,
        password: Option<&str>,
        database: &str,
        ssl_mode: Option<PgSslMode>,
        max_connections: u32,
        statement_timeout_ms: Option<u64>,
    ) -> MetadataResult<Self> {
        let mut opts = PgConnectOptions::new()
            .host(host)
            .port(port)
            .database(database);

        if let Some(user) = username {
            opts = opts.username(user);
        }

        if let Some(pass) = password {
            opts = opts.password(pass);
        }

        if let Some(mode) = ssl_mode {
            let sqlx_mode = match mode {
                PgSslMode::Disable => SqlxPgSslMode::Disable,
                PgSslMode::Prefer => SqlxPgSslMode::Prefer,
                PgSslMode::Require => SqlxPgSslMode::Require,
            };
            opts = opts.ssl_mode(sqlx_mode);
        }

        // Log connection info without password
        tracing::info!(
            host = host,
            port = port,
            database = database,
            username = username.unwrap_or("<none>"),
            ssl_mode = ?ssl_mode,
            "Connecting to PostgreSQL with individual parameters"
        );

        Self::connect(opts, max_connections, statement_timeout_ms).await
    }

    /// Legacy constructor - delegates to `from_url`.
    ///
    /// Kept for backward compatibility. New code should use `from_url` or `from_params`.
    pub async fn new(
        url: &str,
        max_connections: u32,
        statement_timeout_ms: Option<u64>,
    ) -> MetadataResult<Self> {
        Self::from_url(url, max_connections, statement_timeout_ms).await
    }

    /// Internal: Connect to PostgreSQL with the given options.
    async fn connect(
        mut opts: PgConnectOptions,
        max_connections: u32,
        statement_timeout_ms: Option<u64>,
    ) -> MetadataResult<Self> {
        // Set statement_timeout if configured to prevent hung queries during GC.
        // This is critical for the batch timeout fix (P1 issue).
        if let Some(timeout_ms) = statement_timeout_ms {
            opts = opts.options([("statement_timeout", format!("{}ms", timeout_ms))]);
            tracing::info!("PostgreSQL statement_timeout set to {}ms", timeout_ms);
        }

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
        // Drop the unique index on (upload_id, chunk_hash) if it exists (migration)
        // This index was added in commit a019380 but incorrectly prevents valid manifests
        // with duplicate chunks at different positions (e.g., NARs with repeated content).
        sqlx::query("DROP INDEX IF EXISTS idx_upload_chunks_hash")
            .execute(&self.pool)
            .await?;
        sqlx::query("DROP INDEX IF EXISTS idx_upload_expected_chunks_hash")
            .execute(&self.pool)
            .await?;

        // PostgreSQL doesn't allow multiple statements in a single prepared statement,
        // so we split the schema and execute each statement separately.
        for statement in postgres_schema_statements(POSTGRES_SCHEMA) {
            sqlx::query(statement).execute(&self.pool).await?;
        }

        Ok(())
    }

    async fn health_check(&self) -> MetadataResult<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }
}

#[async_trait]
impl BootstrapRepo for PostgresStore {
    async fn get_bootstrap_token_id(&self) -> MetadataResult<Option<Uuid>> {
        // Use Option<Option<Uuid>> to handle:
        // - None: no row found
        // - Some(None): row exists but bootstrap_token_id is NULL
        // - Some(Some(uuid)): row exists with a value
        let value: Option<Option<Uuid>> =
            sqlx::query_scalar("SELECT bootstrap_token_id FROM bootstrap_state WHERE id = 1")
                .fetch_optional(&self.pool)
                .await?;
        Ok(value.flatten())
    }

    async fn set_bootstrap_token_id(&self, token_id: Uuid) -> MetadataResult<()> {
        sqlx::query(
            r#"
            INSERT INTO bootstrap_state (id, bootstrap_token_id)
            VALUES (1, $1)
            ON CONFLICT(id) DO UPDATE
            SET bootstrap_token_id = EXCLUDED.bootstrap_token_id
            "#,
        )
        .bind(token_id)
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
impl DomainRepo for PostgresStore {
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
            "INSERT INTO domains (domain_id, domain_name, is_default, created_at, updated_at) VALUES ($1, $2, $3, $4, $5)",
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
        let row = sqlx::query_as::<_, DomainRow>("SELECT * FROM domains WHERE domain_id = $1")
            .bind(domain_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    async fn get_domain_by_name(&self, domain_name: &str) -> MetadataResult<Option<DomainRow>> {
        let row = sqlx::query_as::<_, DomainRow>("SELECT * FROM domains WHERE domain_name = $1")
            .bind(domain_name)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    async fn update_domain(&self, domain: &DomainRow) -> MetadataResult<()> {
        let result = sqlx::query(
            "UPDATE domains SET domain_name = $1, updated_at = $2 WHERE domain_id = $3",
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
        let result = sqlx::query("DELETE FROM domains WHERE domain_id = $1")
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
        let caches: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM caches WHERE domain_id = $1")
            .bind(domain_id)
            .fetch_one(&self.pool)
            .await?;
        let upload_sessions: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM upload_sessions WHERE domain_id = $1")
                .bind(domain_id)
                .fetch_one(&self.pool)
                .await?;
        let store_paths: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM store_paths WHERE domain_id = $1")
                .bind(domain_id)
                .fetch_one(&self.pool)
                .await?;
        let chunks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE domain_id = $1")
            .bind(domain_id)
            .fetch_one(&self.pool)
            .await?;
        let manifests: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM manifests WHERE domain_id = $1")
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
impl UploadRepo for PostgresStore {
    async fn create_session(&self, session: &UploadSessionRow) -> MetadataResult<()> {
        sqlx::query(
            r#"
            INSERT INTO upload_sessions (
                upload_id, cache_id, domain_id, store_path, store_path_hash,
                nar_size, nar_hash, chunk_size, manifest_hash, state,
                owner_token_id, created_at, updated_at, expires_at, trace_id,
                error_code, error_detail, commit_started_at, commit_progress,
                chunks_predeclared
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
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
            "SELECT * FROM upload_sessions WHERE upload_id = $1",
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

        if let Some(ref mut s) = session
            && s.state == "open"
        {
            // Only update state if currently 'open'.
            // Set commit_started_at to track commit duration (separate from updated_at for staleness).
            // Initialize commit_progress as empty JSON object for tracking incremental progress.
            let result = sqlx::query(
                "UPDATE upload_sessions
                     SET state = $1, updated_at = $2, commit_started_at = $2, commit_progress = $3
                     WHERE upload_id = $4 AND state = 'open'",
            )
            .bind("committing")
            .bind(updated_at)
            .bind("{}") // Initialize with empty JSON object
            .bind(upload_id)
            .execute(&mut *tx)
            .await?;

            // Update the session object if the UPDATE succeeded
            if result.rows_affected() > 0 {
                s.state = "committing".to_string();
                s.updated_at = updated_at;
                s.commit_started_at = Some(updated_at);
                s.commit_progress = Some("{}".to_string());
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
        uploaded_data: bool,
        chunks_predeclared: bool,
    ) -> MetadataResult<()> {
        // Two-phase approach to handle both expected chunks and dynamic tracking:
        // 1. First try UPDATE (for pre-defined expected chunks)
        // 2. If no rows updated AND chunks_predeclared=false, INSERT new row (for dynamic tracking)
        //    If chunks_predeclared=true, return error (defense-in-depth against undeclared chunks)

        // Phase 1: Try to update existing expected chunk row
        // Only upgrade uploaded_data from false to true, never downgrade (use OR logic)
        let update_result = sqlx::query(
            "UPDATE upload_expected_chunks SET received_at = $1, uploaded_data = uploaded_data OR $4 WHERE upload_id = $2 AND chunk_hash = $3",
        )
        .bind(received_at)
        .bind(upload_id)
        .bind(chunk_hash)
        .bind(uploaded_data)
        .execute(&self.pool)
        .await?;

        if update_result.rows_affected() > 0 {
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

        // Phase 2: No expected chunk found, insert for dynamic tracking
        // Use negative positions (starting from -1, -2, ...) to avoid conflicts with
        // expected chunks (which use positions 0, 1, 2, ...).
        //
        // Use atomic INSERT...SELECT WHERE NOT EXISTS to prevent TOCTOU race conditions.
        // Without this, two concurrent uploads of the same chunk could both check existence,
        // both see nothing, and both INSERT with different negative positions, creating duplicates.
        const MAX_RETRIES: u32 = 5;
        for attempt in 0..MAX_RETRIES {
            // Atomic insert: compute next position and insert only if chunk_hash doesn't exist.
            // This single query prevents the TOCTOU race between checking existence and inserting.
            // Uses a CTE to ensure we always get a position value even when no rows exist yet.
            let insert_result = sqlx::query(
                r#"
                WITH new_position AS (
                    SELECT COALESCE(MIN(position), 0) - 1 AS pos
                    FROM upload_expected_chunks
                    WHERE upload_id = $1
                )
                INSERT INTO upload_expected_chunks (upload_id, position, chunk_hash, size_bytes, received_at, uploaded_data)
                SELECT $1, np.pos, $2, 0, $3, $4
                FROM new_position np
                WHERE NOT EXISTS (
                    SELECT 1 FROM upload_expected_chunks
                    WHERE upload_id = $1 AND chunk_hash = $2
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
                        "UPDATE upload_expected_chunks SET received_at = COALESCE(received_at, $1), uploaded_data = uploaded_data OR $4 WHERE upload_id = $2 AND chunk_hash = $3",
                    )
                    .bind(received_at)
                    .bind(upload_id)
                    .bind(chunk_hash)
                    .bind(uploaded_data)
                    .execute(&self.pool)
                    .await?;
                    return Ok(());
                }
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

    async fn get_deduped_only_chunks(&self, upload_id: Uuid) -> MetadataResult<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT chunk_hash FROM upload_expected_chunks WHERE upload_id = $1 AND received_at IS NOT NULL AND uploaded_data = FALSE ORDER BY position",
        )
        .bind(upload_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
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

    async fn get_expected_chunks(
        &self,
        upload_id: Uuid,
    ) -> MetadataResult<Vec<UploadExpectedChunkRow>> {
        let rows = sqlx::query_as::<_, UploadExpectedChunkRow>(
            "SELECT * FROM upload_expected_chunks WHERE upload_id = $1 ORDER BY position",
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
            "SELECT * FROM upload_expected_chunks WHERE upload_id = $1 AND chunk_hash = $2",
        )
        .bind(upload_id)
        .bind(chunk_hash)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
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
                    "SELECT * FROM upload_sessions WHERE cache_id = $1 AND expires_at < $2 AND state = 'open' ORDER BY expires_at LIMIT $3",
                )
                .bind(id)
                .bind(now)
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, UploadSessionRow>(
                    "SELECT * FROM upload_sessions WHERE cache_id IS NULL AND expires_at < $1 AND state = 'open' ORDER BY expires_at LIMIT $2",
                )
                .bind(now)
                .bind(limit as i64)
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
        // Session is stuck if BOTH conditions are met:
        // 1. commit_started_at < older_than: commit has been running longer than grace period
        // 2. updated_at < older_than - 5 minutes: no progress in last 5 minutes beyond grace period
        //
        // This allows long commits (>grace_period) as long as they're making progress.
        // Example: If grace_period=1h and commit has been running for 2h but was updated 30s ago,
        // it's NOT stuck (making progress). But if it hasn't been updated in 1h5m, it IS stuck.
        let rows = match cache_id {
            Some(id) => {
                sqlx::query_as::<_, UploadSessionRow>(
                    "SELECT * FROM upload_sessions
                     WHERE cache_id = $1
                       AND state = 'committing'
                       AND commit_started_at < $2
                       AND updated_at < $2 - INTERVAL '5 minutes'
                     ORDER BY commit_started_at
                     LIMIT $3",
                )
                .bind(id)
                .bind(older_than)
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, UploadSessionRow>(
                    "SELECT * FROM upload_sessions
                     WHERE cache_id IS NULL
                       AND state = 'committing'
                       AND commit_started_at < $1
                       AND updated_at < $1 - INTERVAL '5 minutes'
                     ORDER BY commit_started_at
                     LIMIT $2",
                )
                .bind(older_than)
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows)
    }

    async fn has_active_sessions_since(&self, since_time: OffsetDateTime) -> MetadataResult<bool> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM upload_sessions
                WHERE (created_at >= $1 OR updated_at >= $1)
                  AND state IN ('open', 'committing')
            )",
        )
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
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM upload_sessions
                WHERE cache_id = $2
                  AND (created_at >= $1 OR updated_at >= $1)
                  AND state IN ('open', 'committing')
            )",
        )
        .bind(since_time)
        .bind(cache_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
    }

    async fn is_chunk_expected_in_active_sessions_since(
        &self,
        chunk_hash: &str,
        since_time: OffsetDateTime,
    ) -> MetadataResult<bool> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1
                FROM upload_expected_chunks
                JOIN upload_sessions ON upload_sessions.upload_id = upload_expected_chunks.upload_id
                WHERE upload_expected_chunks.chunk_hash = $1
                  AND (upload_sessions.created_at >= $2 OR upload_sessions.updated_at >= $2)
                  AND upload_sessions.state IN ('open', 'committing')
            )",
        )
        .bind(chunk_hash)
        .bind(since_time)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
    }

    async fn delete_session(&self, upload_id: Uuid) -> MetadataResult<()> {
        sqlx::query("DELETE FROM upload_sessions WHERE upload_id = $1")
            .bind(upload_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn count_active_uploads(&self, cache_id: Option<Uuid>) -> MetadataResult<u64> {
        let count: i64 = match cache_id {
            Some(id) => {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM upload_sessions WHERE cache_id = $1 AND state IN ('open', 'committing')",
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

    async fn update_commit_progress(
        &self,
        upload_id: Uuid,
        progress_json: &str,
        updated_at: OffsetDateTime,
    ) -> MetadataResult<()> {
        sqlx::query(
            "UPDATE upload_sessions
             SET commit_progress = $1, updated_at = $2
             WHERE upload_id = $3",
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
impl ChunkRepo for PostgresStore {
    async fn upsert_chunk(&self, chunk: &ChunkRow) -> MetadataResult<()> {
        sqlx::query(
            r#"
            INSERT INTO chunks (domain_id, chunk_hash, size_bytes, object_key, refcount, created_at, last_accessed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (domain_id, chunk_hash) DO UPDATE SET last_accessed_at = $7
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
            "SELECT * FROM chunks WHERE domain_id = $1 AND chunk_hash = $2",
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

        // PostgreSQL supports ANY($1) with array parameter - much cleaner than dynamic IN
        let rows: Vec<ChunkRow> = sqlx::query_as::<_, ChunkRow>(
            "SELECT * FROM chunks WHERE domain_id = $1 AND chunk_hash = ANY($2)",
        )
        .bind(domain_id)
        .bind(chunk_hashes)
        .fetch_all(&self.pool)
        .await?;

        let mut result = HashMap::with_capacity(rows.len());
        for row in rows {
            result.insert(row.chunk_hash.clone(), row);
        }

        Ok(result)
    }

    async fn chunk_exists(&self, domain_id: Uuid, chunk_hash: &str) -> MetadataResult<bool> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM chunks WHERE domain_id = $1 AND chunk_hash = $2",
        )
        .bind(domain_id)
        .bind(chunk_hash)
        .fetch_one(&self.pool)
        .await?;
        Ok(count > 0)
    }

    async fn increment_refcount(&self, domain_id: Uuid, chunk_hash: &str) -> MetadataResult<()> {
        sqlx::query(
            "UPDATE chunks SET refcount = refcount + 1 WHERE domain_id = $1 AND chunk_hash = $2",
        )
        .bind(domain_id)
        .bind(chunk_hash)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn decrement_refcount(&self, domain_id: Uuid, chunk_hash: &str) -> MetadataResult<()> {
        sqlx::query(
            "UPDATE chunks SET refcount = refcount - 1 WHERE domain_id = $1 AND chunk_hash = $2 AND refcount > 0",
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
                "UPDATE chunks SET refcount = refcount + 1 WHERE domain_id = $1 AND chunk_hash = $2",
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
            "UPDATE chunks SET last_accessed_at = $1 WHERE domain_id = $2 AND chunk_hash = $3",
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
        // Exclude chunks that are part of open OR committing upload sessions to prevent
        // GC from deleting chunks for in-progress uploads (which temporarily have refcount=0).
        // CRITICAL: Must exclude 'committing' state to prevent race where GC deletes chunks
        // mid-commit before refcounts are incremented.
        // Protection covers BOTH uploads with expected_chunks AND uploads without them.
        let rows = sqlx::query_as::<_, ChunkRow>(
            r#"
            SELECT * FROM chunks
            WHERE domain_id = $1
              AND refcount = 0
              AND created_at < $2
              AND chunk_hash NOT IN (
                -- Exclude chunks from uploads with expected_chunks
                SELECT uec.chunk_hash FROM upload_expected_chunks uec
                INNER JOIN upload_sessions us ON uec.upload_id = us.upload_id
                WHERE us.state IN ('open', 'committing')
                  AND us.domain_id = $1
              )
              AND NOT EXISTS (
                -- Also protect chunks created after any open/committing session started
                -- (for uploads without expected_chunks that upload dynamically)
                SELECT 1 FROM upload_sessions us2
                WHERE us2.state IN ('open', 'committing')
                  AND us2.domain_id = $1
                  AND chunks.created_at >= us2.created_at
              )
            ORDER BY created_at
            LIMIT $3
            "#,
        )
        .bind(domain_id)
        .bind(older_than)
        .bind(limit as i64)
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
        // ATOMIC DELETION using CTE to prevent TOCTOU race:
        // Row locks acquired by FOR UPDATE are held until DELETE completes,
        // preventing concurrent refcount increments from causing data loss.
        //
        // Previously, locks were released after SELECT, creating a race window where:
        // - GC selects chunks with refcount=0, locks released
        // - Concurrent commit increments refcount
        // - GC deletes chunk anyway (DATA LOSS!)
        //
        // Now the entire SELECT + DELETE happens in one query/transaction:
        // - FOR UPDATE acquires row locks
        // - DELETE executes while locks are still held
        // - RETURNING provides deleted chunks for storage cleanup
        // - Concurrent commits block until this completes (no race possible)
        //
        // Protection covers BOTH uploads with expected_chunks AND uploads without them.
        //
        // CRITICAL FIX: Uses protection_window_secs parameter instead of hardcoded '1 hour'
        // to sync with grace_period config. This prevents divergence where grace_period=2h
        // but protection window remains 1h, causing premature chunk deletion.
        let rows = sqlx::query_as::<_, ChunkRow>(
            r#"
            WITH chunks_to_delete AS (
                SELECT * FROM chunks
                WHERE domain_id = $1
                  AND refcount = 0
                  AND created_at < $2
                  AND chunk_hash NOT IN (
                    -- Protection 1: Exclude chunks from uploads with expected_chunks.
                    -- This protects uploads that pre-declare their chunk list.
                    SELECT uec.chunk_hash FROM upload_expected_chunks uec
                    INNER JOIN upload_sessions us ON uec.upload_id = us.upload_id
                    WHERE us.state IN ('open', 'committing')
                      AND us.domain_id = $1
                  )
                  AND NOT EXISTS (
                    -- Protection 2: Protect newly uploaded chunks.
                    -- Correlate chunk to session by creation time - protects chunks
                    -- uploaded during ANY active session's lifetime.
                    --
                    -- Previous bug: Uncorrelated query checked if ANY session existed
                    -- within protection window, blocking ALL chunk deletion globally.
                    -- This caused GC stalls under normal upload load.
                    --
                    -- Fixed: Now checks if THIS CHUNK was created after an active
                    -- session started, protecting only chunks from active uploads.
                    SELECT 1 FROM upload_sessions us2
                    WHERE us2.state IN ('open', 'committing')
                      AND us2.domain_id = $1
                      AND chunks.created_at >= us2.created_at
                  )
                ORDER BY created_at
                LIMIT $3
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM chunks
            WHERE (domain_id, chunk_hash) IN (SELECT domain_id, chunk_hash FROM chunks_to_delete)
              AND refcount = 0
            RETURNING *
            "#,
        )
        .bind(domain_id)
        .bind(older_than)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }
    async fn delete_chunk(&self, domain_id: Uuid, chunk_hash: &str) -> MetadataResult<()> {
        sqlx::query("DELETE FROM chunks WHERE domain_id = $1 AND chunk_hash = $2")
            .bind(domain_id)
            .bind(chunk_hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_stats(&self, domain_id: Uuid) -> MetadataResult<ChunkStats> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE domain_id = $1")
            .bind(domain_id)
            .fetch_one(&self.pool)
            .await?;
        // Cast to BIGINT since PostgreSQL SUM returns NUMERIC
        let total_size: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(size_bytes)::BIGINT, 0) FROM chunks WHERE domain_id = $1",
        )
        .bind(domain_id)
        .fetch_one(&self.pool)
        .await?;
        let referenced_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE domain_id = $1 AND refcount > 0")
                .bind(domain_id)
                .fetch_one(&self.pool)
                .await?;
        let unreferenced_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE domain_id = $1 AND refcount = 0")
                .bind(domain_id)
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

        // Refresh created_at timestamp on conflict to protect against GC during reuploads.
        // When reusing an old manifest (from hours/days ago), the stale timestamp would
        // bypass grace period protection, allowing GC to delete the manifest mid-reupload.
        // By updating created_at, we reset the grace period clock and prevent premature deletion.
        //
        // Uses xmax system column to detect INSERT vs UPDATE:
        // - xmax = 0: Row was inserted (newly created manifest)
        // - xmax != 0: Row was updated (existing manifest, timestamp refreshed)
        let row: (bool,) = sqlx::query_as(
            r#"
            INSERT INTO manifests (domain_id, manifest_hash, chunk_size, chunk_count, nar_size, object_key, refcount, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (domain_id, manifest_hash) DO UPDATE
            SET created_at = EXCLUDED.created_at
            RETURNING (xmax = 0) AS inserted
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
        .fetch_one(&mut *tx)
        .await?;

        let inserted = row.0;

        // Only insert chunk mappings if this is a new manifest
        // (existing manifests already have their chunk mappings)
        if inserted {
            for chunk in chunks {
                sqlx::query(
                    "INSERT INTO manifest_chunks (domain_id, manifest_hash, position, chunk_hash) VALUES ($1, $2, $3, $4)",
                )
                .bind(chunk.domain_id)
                .bind(&chunk.manifest_hash)
                .bind(chunk.position)
                .bind(&chunk.chunk_hash)
                .execute(&mut *tx)
                .await?;
            }
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

        // Step 1: Create manifest (do nothing on conflict to preserve original created_at).
        // Note: ON CONFLICT DO NOTHING returns 0 rows, so we use fetch_optional().
        // If None, the manifest already existed (conflict). If Some, it was inserted.
        let row: Option<(bool,)> = sqlx::query_as(
            r#"
            INSERT INTO manifests (domain_id, manifest_hash, chunk_size, chunk_count, nar_size, object_key, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (domain_id, manifest_hash) DO NOTHING
            RETURNING (xmax = 0) AS inserted
            "#,
        )
        .bind(manifest.domain_id)
        .bind(&manifest.manifest_hash)
        .bind(manifest.chunk_size)
        .bind(manifest.chunk_count)
        .bind(manifest.nar_size)
        .bind(&manifest.object_key)
        .bind(manifest.created_at)
        .fetch_optional(&mut *tx)
        .await?;

        let inserted = row.map(|r| r.0).unwrap_or(false);

        // Step 2: Insert chunk mappings (only for new manifests)
        if inserted {
            for chunk in chunks {
                sqlx::query(
                    "INSERT INTO manifest_chunks (domain_id, manifest_hash, position, chunk_hash) VALUES ($1, $2, $3, $4)",
                )
                .bind(chunk.domain_id)
                .bind(&chunk.manifest_hash)
                .bind(chunk.position)
                .bind(&chunk.chunk_hash)
                .execute(&mut *tx)
                .await?;
            }
        }

        // Step 3: Increment manifest refcount within the domain (always).
        sqlx::query(
            "UPDATE manifests SET refcount = refcount + 1 WHERE domain_id = $1 AND manifest_hash = $2",
        )
        .bind(manifest.domain_id)
        .bind(&manifest.manifest_hash)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(inserted)
    }

    async fn get_manifest(
        &self,
        domain_id: Uuid,
        manifest_hash: &str,
    ) -> MetadataResult<Option<ManifestRow>> {
        let row = sqlx::query_as::<_, ManifestRow>(
            "SELECT * FROM manifests WHERE domain_id = $1 AND manifest_hash = $2",
        )
        .bind(domain_id)
        .bind(manifest_hash)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn manifest_exists(&self, domain_id: Uuid, manifest_hash: &str) -> MetadataResult<bool> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM manifests WHERE domain_id = $1 AND manifest_hash = $2",
        )
        .bind(domain_id)
        .bind(manifest_hash)
        .fetch_one(&self.pool)
        .await?;
        Ok(count > 0)
    }

    async fn get_manifest_chunks(
        &self,
        domain_id: Uuid,
        manifest_hash: &str,
    ) -> MetadataResult<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT chunk_hash FROM manifest_chunks WHERE domain_id = $1 AND manifest_hash = $2 ORDER BY position",
        )
        .bind(domain_id)
        .bind(manifest_hash)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn get_manifest_chunks_verified(
        &self,
        domain_id: Uuid,
        manifest_hash: &str,
    ) -> MetadataResult<Vec<String>> {
        // First, get the expected chunk count from the manifest.
        // This detects incomplete manifest_chunks (partial insert/corruption).
        // Note: manifests.chunk_count is INTEGER (i32 in Postgres).
        let expected_count: Option<(i32,)> = sqlx::query_as(
            "SELECT chunk_count FROM manifests WHERE domain_id = $1 AND manifest_hash = $2",
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
        // Note: chunks.refcount is INTEGER (i32 in Postgres), so we use Option<i32>.
        let rows: Vec<(String, Option<i32>)> = sqlx::query_as(
            "SELECT mc.chunk_hash, c.refcount \
             FROM manifest_chunks mc \
             LEFT JOIN chunks c \
               ON c.domain_id = mc.domain_id \
              AND c.chunk_hash = mc.chunk_hash \
              AND c.refcount > 0 \
             WHERE mc.domain_id = $1 \
               AND mc.manifest_hash = $2 \
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
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT manifest_hash FROM manifest_chunks WHERE domain_id = $1 AND chunk_hash = $2",
        )
        .bind(domain_id)
        .bind(chunk_hash)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn delete_manifest(&self, domain_id: Uuid, manifest_hash: &str) -> MetadataResult<()> {
        // Wrap both DELETEs in transaction to ensure atomicity
        // If either fails, both are rolled back
        let mut tx = self.pool.begin().await?;

        // Delete chunk mappings first (cascade should handle this, but be explicit)
        sqlx::query("DELETE FROM manifest_chunks WHERE domain_id = $1 AND manifest_hash = $2")
            .bind(domain_id)
            .bind(manifest_hash)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM manifests WHERE domain_id = $1 AND manifest_hash = $2")
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

        let refcount: Option<i32> = sqlx::query_scalar(
            "SELECT refcount FROM manifests WHERE domain_id = $1 AND manifest_hash = $2 FOR UPDATE",
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
            "SELECT chunk_hash FROM manifest_chunks WHERE domain_id = $1 AND manifest_hash = $2",
        )
        .bind(domain_id)
        .bind(manifest_hash)
        .fetch_all(&mut *tx)
        .await?;

        let chunk_count = chunks.len() as u64;

        // Sort chunk hashes alphabetically to prevent deadlocks.
        chunks.sort_by(|a, b| a.0.cmp(&b.0));

        for (chunk_hash,) in chunks {
            sqlx::query(
                "UPDATE chunks SET refcount = GREATEST(0, refcount - 1) WHERE domain_id = $1 AND chunk_hash = $2",
            )
            .bind(domain_id)
            .bind(&chunk_hash)
            .execute(&mut *tx)
            .await?;
        }

        sqlx::query("DELETE FROM manifest_chunks WHERE domain_id = $1 AND manifest_hash = $2")
            .bind(domain_id)
            .bind(manifest_hash)
            .execute(&mut *tx)
            .await?;

        let deleted_manifest: Option<ManifestRow> = sqlx::query_as(
            "DELETE FROM manifests WHERE domain_id = $1 AND manifest_hash = $2 AND refcount = 0 RETURNING *",
        )
        .bind(domain_id)
        .bind(manifest_hash)
        .fetch_optional(&mut *tx)
        .await?;

        if deleted_manifest.is_none() {
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
            WHERE domain_id = $1
              AND refcount = 0
              AND created_at < $2
            ORDER BY created_at
            LIMIT $3
            "#,
        )
        .bind(domain_id)
        .bind(older_than)
        .bind(limit as i64)
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
            "UPDATE manifests SET refcount = refcount + 1 WHERE domain_id = $1 AND manifest_hash = $2",
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
            "UPDATE manifests SET refcount = refcount - 1 WHERE domain_id = $1 AND manifest_hash = $2 AND refcount > 0",
        )
        .bind(domain_id)
        .bind(manifest_hash)
        .execute(&self.pool)
        .await?;
        Ok(())
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
        // NOTE: chunks_verified starts as false and is set to true by mark_chunks_verified()
        // after all storage operations complete successfully.
        sqlx::query(
            r#"
            INSERT INTO store_paths (
                cache_id, domain_id, store_path_hash, store_path, nar_hash, nar_size, manifest_hash,
                created_at, committed_at, visibility_state, uploader_token_id, ca, chunks_verified
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
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
                ca = EXCLUDED.ca,
                chunks_verified = EXCLUDED.chunks_verified
            "#,
        )
        .bind(store_path.cache_id)
        .bind(store_path.domain_id)
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
        let mut tx = self.pool.begin().await?;

        let existing: Option<(Uuid, String)> = match cache_id {
            Some(id) => {
                sqlx::query_as(
                    "SELECT domain_id, manifest_hash FROM store_paths WHERE cache_id = $1 AND store_path_hash = $2 FOR UPDATE",
                )
                .bind(id)
                .bind(store_path_hash)
                .fetch_optional(&mut *tx)
                .await?
            }
            None => {
                sqlx::query_as(
                    "SELECT domain_id, manifest_hash FROM store_paths WHERE cache_id IS NULL AND store_path_hash = $1 FOR UPDATE",
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
                    "UPDATE store_paths SET visibility_state = 'visible', nar_hash = $1, nar_size = $2, manifest_hash = $3 WHERE cache_id = $4 AND store_path_hash = $5",
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
                    "UPDATE store_paths SET visibility_state = 'visible', nar_hash = $1, nar_size = $2, manifest_hash = $3 WHERE cache_id IS NULL AND store_path_hash = $4",
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
            "UPDATE manifests SET refcount = refcount - 1 WHERE domain_id = $1 AND manifest_hash = $2 AND refcount > 0",
        )
        .bind(domain_id)
        .bind(&old_manifest_hash)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
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
        let mut tx = self.pool.begin().await?;

        let existing: Option<(Uuid, String)> = match cache_id {
            Some(id) => {
                sqlx::query_as(
                    "SELECT domain_id, manifest_hash FROM store_paths WHERE cache_id = $1 AND store_path_hash = $2 FOR UPDATE",
                )
                .bind(id)
                .bind(store_path_hash)
                .fetch_optional(&mut *tx)
                .await?
            }
            None => {
                sqlx::query_as(
                    "SELECT domain_id, manifest_hash FROM store_paths WHERE cache_id IS NULL AND store_path_hash = $1 FOR UPDATE",
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
            "UPDATE manifests SET refcount = refcount - 1 WHERE domain_id = $1 AND manifest_hash = $2 AND refcount > 0",
        )
        .bind(domain_id)
        .bind(&manifest_hash)
        .execute(&mut *tx)
        .await?;

        // Delete child records first (using composite key), then store_paths
        // FK cascade should handle this, but be explicit for safety
        match cache_id {
            Some(id) => {
                sqlx::query("DELETE FROM signatures WHERE cache_id = $1 AND store_path_hash = $2")
                    .bind(id)
                    .bind(store_path_hash)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query(
                    "DELETE FROM narinfo_records WHERE cache_id = $1 AND store_path_hash = $2",
                )
                .bind(id)
                .bind(store_path_hash)
                .execute(&mut *tx)
                .await?;
                sqlx::query("DELETE FROM store_path_references WHERE cache_id = $1 AND store_path_hash = $2")
                    .bind(id)
                    .bind(store_path_hash)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("DELETE FROM store_paths WHERE cache_id = $1 AND store_path_hash = $2")
                    .bind(id)
                    .bind(store_path_hash)
                    .execute(&mut *tx)
                    .await?;
            }
            None => {
                sqlx::query(
                    "DELETE FROM signatures WHERE cache_id IS NULL AND store_path_hash = $1",
                )
                .bind(store_path_hash)
                .execute(&mut *tx)
                .await?;
                sqlx::query(
                    "DELETE FROM narinfo_records WHERE cache_id IS NULL AND store_path_hash = $1",
                )
                .bind(store_path_hash)
                .execute(&mut *tx)
                .await?;
                sqlx::query("DELETE FROM store_path_references WHERE cache_id IS NULL AND store_path_hash = $1")
                    .bind(store_path_hash)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query(
                    "DELETE FROM store_paths WHERE cache_id IS NULL AND store_path_hash = $1",
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
        cache_id: Option<Uuid>,
        age_seconds: i64,
    ) -> MetadataResult<u64> {
        // Delete failed store paths that are older than the specified age, scoped to cache_id.
        // This allows a grace period for debugging before cleanup.
        // Use COALESCE to handle NULL committed_at (failed uploads that never completed).
        let result = match cache_id {
            Some(id) => {
                sqlx::query(
                    r#"
                    DELETE FROM store_paths
                    WHERE cache_id = $1
                      AND visibility_state = 'failed'
                      AND COALESCE(committed_at, created_at) < NOW() - INTERVAL '1 second' * $2
                    "#,
                )
                .bind(id)
                .bind(age_seconds)
                .execute(&self.pool)
                .await?
            }
            None => {
                sqlx::query(
                    r#"
                    DELETE FROM store_paths
                    WHERE cache_id IS NULL
                      AND visibility_state = 'failed'
                      AND COALESCE(committed_at, created_at) < NOW() - INTERVAL '1 second' * $1
                    "#,
                )
                .bind(age_seconds)
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
                    "UPDATE store_paths SET chunks_verified = TRUE WHERE cache_id = $1 AND store_path_hash = $2",
                )
                .bind(id)
                .bind(store_path_hash)
                .execute(&self.pool)
                .await?;
            }
            None => {
                sqlx::query(
                    "UPDATE store_paths SET chunks_verified = TRUE WHERE cache_id IS NULL AND store_path_hash = $1",
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
        let row = sqlx::query_as::<_, TokenRow>("SELECT * FROM tokens WHERE token_hash = $1")
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

    async fn count_tokens_for_cache(&self, cache_id: Option<Uuid>) -> MetadataResult<u64> {
        let count: i64 = match cache_id {
            Some(id) => {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM tokens WHERE cache_id = $1 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > NOW())",
                )
                .bind(id)
                .fetch_one(&self.pool)
                .await?
            }
            None => {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM tokens WHERE revoked_at IS NULL AND (expires_at IS NULL OR expires_at > NOW())",
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

    async fn get_signing_key_by_name(
        &self,
        key_name: &str,
    ) -> MetadataResult<Option<SigningKeyRow>> {
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

    async fn list_signing_keys(
        &self,
        cache_id: Option<Uuid>,
    ) -> MetadataResult<Vec<SigningKeyRow>> {
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

    async fn delete_signing_keys_for_cache(&self, cache_id: Uuid) -> MetadataResult<u64> {
        let result = sqlx::query("DELETE FROM signing_keys WHERE cache_id = $1")
            .bind(cache_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}

#[async_trait]
impl GcRepo for PostgresStore {
    async fn create_gc_job(&self, job: &GcJobRow) -> MetadataResult<()> {
        // Insert GC job, handling constraint violations for job isolation.
        // Two constraints protect against concurrent jobs:
        // 1. Unique index: prevents same job_type for same cache in 'queued'/'running' states
        // 2. EXCLUDE constraint: prevents ANY job_type for same cache in 'queued'/'running' states
        match sqlx::query(
            r#"
            INSERT INTO gc_jobs (gc_job_id, cache_id, job_type, state, started_at, finished_at, stats_json, sweep_checkpoint_json)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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
                // Check for constraint violations (exclusion or unique)
                // Extract code and constraint before matching to avoid borrow issues
                let is_exclusion = db_err.code().as_deref() == Some("23P01");
                let is_unique = db_err.code().as_deref() == Some("23505");
                let is_gc_constraint = db_err.constraint() == Some("idx_gc_jobs_cache_type_active")
                    || db_err.constraint() == Some("gc_jobs_one_per_cache");

                if is_exclusion || (is_unique && is_gc_constraint) {
                    Err(MetadataError::Constraint(
                        "Another GC job is already active for this cache".to_string()
                    ))
                } else {
                    Err(sqlx::Error::Database(db_err).into())
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn get_gc_job(&self, gc_job_id: Uuid) -> MetadataResult<Option<GcJobRow>> {
        let row = sqlx::query_as::<_, GcJobRow>("SELECT * FROM gc_jobs WHERE gc_job_id = $1")
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
        sweep_checkpoint_json: Option<&str>,
    ) -> MetadataResult<()> {
        sqlx::query(
            "UPDATE gc_jobs SET state = $1, finished_at = $2, stats_json = $3, sweep_checkpoint_json = $4 WHERE gc_job_id = $5",
        )
        .bind(state)
        .bind(finished_at)
        .bind(stats_json)
        .bind(sweep_checkpoint_json)
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
        // Filter by cache_id for tenant isolation.
        // None = public/global jobs only (cache_id IS NULL).
        // Some(id) = filter to specific cache's jobs.
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
                // Filter to public/global jobs only (no cross-tenant visibility)
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

    async fn get_active_gc_jobs(&self, cache_id: Option<Uuid>) -> MetadataResult<Vec<GcJobRow>> {
        // Get jobs in 'queued' or 'running' state for duplicate detection and job isolation
        // Filter by cache_id for tenant isolation
        let rows = match cache_id {
            Some(id) => {
                sqlx::query_as::<_, GcJobRow>(
                    "SELECT * FROM gc_jobs WHERE cache_id = $1 AND state IN ('queued', 'running') ORDER BY COALESCE(started_at, 'epoch') DESC",
                )
                .bind(id)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, GcJobRow>(
                    "SELECT * FROM gc_jobs WHERE cache_id IS NULL AND state IN ('queued', 'running') ORDER BY COALESCE(started_at, 'epoch') DESC",
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
        let result = sqlx::query("DELETE FROM gc_jobs WHERE cache_id = $1")
            .bind(cache_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}

// F-001: CacheRepo implementation
#[async_trait]
impl CacheRepo for PostgresStore {
    async fn create_cache(&self, cache: &CacheRow) -> MetadataResult<()> {
        match sqlx::query(
            r#"
            INSERT INTO caches (cache_id, domain_id, cache_name, public_base_url, is_public, is_default, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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
                // PostgreSQL error: "duplicate key value violates unique constraint \"idx_caches_default\""
                let msg = db_err.message();
                if msg.contains("idx_caches_default") || (msg.contains("unique") && msg.contains("is_default")) {
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
        match sqlx::query(
            "UPDATE caches SET cache_name = $1, public_base_url = $2, is_public = $3, is_default = $4, domain_id = $5, updated_at = $6 WHERE cache_id = $7",
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
                // PostgreSQL error: "duplicate key value violates unique constraint \"idx_caches_default\""
                let msg = db_err.message();
                if msg.contains("idx_caches_default") || (msg.contains("unique") && msg.contains("is_default")) {
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
        sqlx::query("DELETE FROM caches WHERE cache_id = $1")
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
        let cache_exists: Option<Uuid> =
            sqlx::query_scalar("SELECT cache_id FROM caches WHERE cache_id = $1 FOR UPDATE")
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
            "SELECT COUNT(*) FROM upload_sessions WHERE cache_id = $1 AND state NOT IN ('committed', 'failed')"
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
            sqlx::query_scalar("SELECT COUNT(*) FROM store_paths WHERE cache_id = $1")
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
            "SELECT COUNT(*) FROM tokens WHERE cache_id = $1 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > NOW())"
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
        let gc_jobs = sqlx::query("DELETE FROM gc_jobs WHERE cache_id = $1")
            .bind(cache_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();

        let signing_keys = sqlx::query("DELETE FROM signing_keys WHERE cache_id = $1")
            .bind(cache_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();

        let trusted_keys = sqlx::query("DELETE FROM trusted_builder_keys WHERE cache_id = $1")
            .bind(cache_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();

        let tombstones = sqlx::query("DELETE FROM tombstones WHERE cache_id = $1")
            .bind(cache_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();

        // Delete revoked or expired tokens (active tokens are prevented by the check above)
        let tokens = sqlx::query(
            "DELETE FROM tokens WHERE cache_id = $1 AND \
             (revoked_at IS NOT NULL OR (expires_at IS NOT NULL AND expires_at <= NOW()))",
        )
        .bind(cache_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        // Delete upload data (expected chunks first due to foreign key)
        let upload_expected_chunks = sqlx::query(
            "DELETE FROM upload_expected_chunks WHERE upload_id IN \
             (SELECT upload_id FROM upload_sessions WHERE cache_id = $1)",
        )
        .bind(cache_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        let upload_sessions = sqlx::query("DELETE FROM upload_sessions WHERE cache_id = $1")
            .bind(cache_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();

        // Delete the cache itself
        sqlx::query("DELETE FROM caches WHERE cache_id = $1")
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
            "SELECT * FROM caches WHERE is_default = TRUE AND is_public = TRUE",
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
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
        let row =
            match cache_id {
                Some(id) => {
                    sqlx::query_as::<_, TrustedBuilderKeyRow>(
                        "SELECT * FROM trusted_builder_keys WHERE cache_id = $1 AND key_name = $2",
                    )
                    .bind(id)
                    .bind(key_name)
                    .fetch_optional(&self.pool)
                    .await?
                }
                None => sqlx::query_as::<_, TrustedBuilderKeyRow>(
                    "SELECT * FROM trusted_builder_keys WHERE cache_id IS NULL AND key_name = $1",
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

    async fn delete_trusted_keys_for_cache(&self, cache_id: Uuid) -> MetadataResult<u64> {
        let result = sqlx::query("DELETE FROM trusted_builder_keys WHERE cache_id = $1")
            .bind(cache_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
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

    async fn delete_tombstones_for_cache(&self, cache_id: Uuid) -> MetadataResult<u64> {
        let result = sqlx::query("DELETE FROM tombstones WHERE cache_id = $1")
            .bind(cache_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}

// F-006: ReachabilityRepo implementation
#[async_trait]
impl ReachabilityRepo for PostgresStore {
    async fn get_all_visible_manifests(&self, domain_id: Uuid) -> MetadataResult<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT manifest_hash FROM store_paths WHERE visibility_state = 'visible' AND domain_id = $1",
        )
        .bind(domain_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn get_chunks_for_manifest(
        &self,
        domain_id: Uuid,
        manifest_hash: &str,
    ) -> MetadataResult<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT chunk_hash FROM manifest_chunks WHERE domain_id = $1 AND manifest_hash = $2 ORDER BY position",
        )
        .bind(domain_id)
        .bind(manifest_hash)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn get_chunk_refcount(
        &self,
        domain_id: Uuid,
        chunk_hash: &str,
    ) -> MetadataResult<Option<i32>> {
        let row = sqlx::query_scalar::<_, i32>(
            "SELECT refcount FROM chunks WHERE domain_id = $1 AND chunk_hash = $2",
        )
        .bind(domain_id)
        .bind(chunk_hash)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn set_chunk_refcount(
        &self,
        domain_id: Uuid,
        chunk_hash: &str,
        refcount: i32,
    ) -> MetadataResult<()> {
        sqlx::query("UPDATE chunks SET refcount = $1 WHERE domain_id = $2 AND chunk_hash = $3")
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
                "UPDATE chunks SET refcount = $1 WHERE domain_id = $2 AND chunk_hash = $3",
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

#[cfg(test)]
mod tests {
    use super::postgres_schema_statements;

    #[test]
    fn postgres_schema_statements_skips_empty_and_comment_only() {
        let schema = r#"
            -- comment only

            CREATE TABLE foo (id int);
            ;
            -- another comment
            CREATE TABLE bar (id int);
        "#;

        let statements = postgres_schema_statements(schema);
        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("CREATE TABLE foo"));
        assert!(statements[1].contains("CREATE TABLE bar"));
    }
}
