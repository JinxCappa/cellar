//! Configuration types shared across crates.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use time::Duration;

/// Server configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Bind address (e.g., "0.0.0.0:8080").
    #[serde(default = "default_bind")]
    pub bind: String,
    /// Maximum parallel chunk uploads per session.
    #[serde(default = "default_max_parallel_chunks")]
    pub max_parallel_chunks: u32,
    /// Default chunk size in bytes.
    #[serde(default = "default_chunk_size")]
    pub default_chunk_size: u64,
    /// Maximum chunk size in bytes.
    #[serde(default = "default_max_chunk_size")]
    pub max_chunk_size: u64,
    /// Upload session timeout in seconds.
    #[serde(default = "default_upload_timeout_secs")]
    pub upload_timeout_secs: u64,
    /// Enable request tracing.
    #[serde(default)]
    pub enable_tracing: bool,
    /// Compression algorithm for stored NARs.
    #[serde(default)]
    pub compression: CompressionConfig,
    /// Enable the /metrics endpoint for Prometheus scraping (default: true).
    /// SECURITY: When enabled, ensure this endpoint is network-restricted
    /// to authorized Prometheus scraper IPs only at the infrastructure level.
    #[serde(default = "default_metrics_enabled")]
    pub metrics_enabled: bool,
    /// Disable chunk upload short-circuit optimization for enhanced security (default: false).
    /// When true, the server always reads and verifies the request body hash
    /// even if the chunk already exists, preventing timing-based side-channel
    /// attacks that could probe for chunk existence across caches.
    #[serde(default)]
    pub disable_chunk_dedup_shortcircuit: bool,
    /// Require expected_chunks in upload requests (default: true).
    /// When true, upload requests without expected_chunks are rejected.
    /// This prevents data loss from long-running uploads where chunks may be
    /// garbage collected before commit if expected_chunks is not provided.
    /// Set to false only for backwards compatibility with legacy clients.
    #[serde(default = "default_require_expected_chunks")]
    pub require_expected_chunks: bool,
}

/// Admin token configuration.
///
/// The admin token is required for server operation. It provides initial access
/// to create caches and manage the server. If the token hash changes between
/// restarts, the previous admin token is automatically revoked and a new one
/// is created.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminConfig {
    /// Pre-computed hash of the admin token (SHA256 hex, 64 characters).
    /// Generate with: `echo -n "your-secret-token" | sha256sum`
    pub token_hash: String,
    /// Scopes for the admin token (default: ["cache:admin"]).
    pub token_scopes: Option<Vec<String>>,
    /// Description for the admin token.
    pub token_description: Option<String>,
}

impl AdminConfig {
    /// Create a test configuration with a dummy token hash.
    ///
    /// **For testing only.** The hash is deterministic but not a real token.
    pub fn for_testing() -> Self {
        Self {
            // SHA256 of "test-admin-token"
            token_hash: "9f735e0df9a1ddc702bf0a1a7b83033f9f7153a00c29de82cedadc9957289b05"
                .to_string(),
            token_scopes: None,
            token_description: Some("Test admin token".to_string()),
        }
    }
}

fn default_bind() -> String {
    "127.0.0.1:8080".to_string()
}

fn default_max_parallel_chunks() -> u32 {
    8
}

fn default_chunk_size() -> u64 {
    crate::DEFAULT_CHUNK_SIZE
}

fn default_max_chunk_size() -> u64 {
    crate::MAX_CHUNK_SIZE
}

fn default_upload_timeout_secs() -> u64 {
    86400 // 24 hours
}

fn default_metrics_enabled() -> bool {
    true // Enabled by default for backwards compatibility
}

fn default_require_expected_chunks() -> bool {
    true // Required by default to prevent GC data loss
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            max_parallel_chunks: default_max_parallel_chunks(),
            default_chunk_size: default_chunk_size(),
            max_chunk_size: default_max_chunk_size(),
            upload_timeout_secs: default_upload_timeout_secs(),
            enable_tracing: false,
            compression: CompressionConfig::default(),
            metrics_enabled: default_metrics_enabled(),
            disable_chunk_dedup_shortcircuit: false,
            require_expected_chunks: default_require_expected_chunks(),
        }
    }
}

impl ServerConfig {
    /// Get the upload timeout as a Duration.
    pub fn upload_timeout(&self) -> Duration {
        // Saturate at i64::MAX to prevent overflow wrapping to negative
        let secs = i64::try_from(self.upload_timeout_secs).unwrap_or(i64::MAX);
        Duration::seconds(secs)
    }
}

/// Storage backend configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageConfig {
    /// Local filesystem storage.
    Filesystem {
        /// Root directory for storage.
        path: PathBuf,
    },
    /// S3-compatible storage.
    S3 {
        /// Bucket name.
        bucket: String,
        /// Optional endpoint URL (for MinIO, etc.).
        endpoint: Option<String>,
        /// AWS region.
        region: Option<String>,
        /// Optional key prefix.
        prefix: Option<String>,
        /// AWS access key ID. Falls back to AWS_ACCESS_KEY_ID env var if not set.
        /// WARNING: Prefer env vars or IAM roles over storing secrets in config files.
        access_key_id: Option<String>,
        /// AWS secret access key. Falls back to AWS_SECRET_ACCESS_KEY env var if not set.
        /// WARNING: Prefer env vars or IAM roles over storing secrets in config files.
        secret_access_key: Option<String>,
        /// Force path-style URLs (e.g., `endpoint/bucket/key` instead of `bucket.endpoint/key`).
        /// Required for MinIO and some S3-compatible services.
        /// AWS S3 (including VPC endpoints, FIPS, and dual-stack) requires virtual-hosted style (false).
        /// Defaults to false (virtual-hosted style).
        #[serde(default)]
        force_path_style: bool,
    },
}

/// Compression algorithm configuration.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CompressionConfig {
    /// No compression.
    None,
    /// Zstd compression (recommended: fast, good ratio).
    #[default]
    Zstd,
    /// XZ compression (slow but high ratio).
    Xz,
}

impl CompressionConfig {
    /// Convert to narinfo Compression type.
    pub fn to_compression(self) -> crate::narinfo::Compression {
        match self {
            Self::None => crate::narinfo::Compression::None,
            Self::Zstd => crate::narinfo::Compression::Zstd,
            Self::Xz => crate::narinfo::Compression::Xz,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::Filesystem {
            path: PathBuf::from("./data/storage"),
        }
    }
}

impl StorageConfig {
    /// Validate storage configuration invariants.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            StorageConfig::S3 {
                access_key_id,
                secret_access_key,
                ..
            } => match (access_key_id.as_ref(), secret_access_key.as_ref()) {
                (Some(_), Some(_)) | (None, None) => Ok(()),
                _ => Err(
                    "s3 config requires both access_key_id and secret_access_key when either is set"
                        .to_string(),
                ),
            },
            _ => Ok(()),
        }
    }
}

/// PostgreSQL SSL mode configuration.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PgSslMode {
    /// Disable SSL/TLS entirely.
    Disable,
    /// Prefer SSL/TLS but allow unencrypted connections (default).
    #[default]
    Prefer,
    /// Require SSL/TLS for all connections.
    Require,
}

/// Metadata store configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum MetadataConfig {
    /// SQLite database (recommended for testing and small deployments only).
    Sqlite {
        /// Database file path.
        path: PathBuf,
        /// Query timeout in seconds (advisory only - SQLite cannot force-cancel queries).
        /// Logs warnings for queries exceeding this duration.
        /// Unlike PostgreSQL's statement_timeout, this does NOT actually cancel queries.
        /// For production deployments with strict timeout requirements, use PostgreSQL.
        /// Recommended: 600 (10 minutes) for development.
        #[serde(default = "default_sqlite_query_timeout_secs")]
        query_timeout_secs: Option<u64>,
    },
    /// PostgreSQL database.
    Postgres {
        /// Connection URL (optional if using individual fields).
        /// Takes precedence over individual fields if both are provided.
        url: Option<String>,
        /// Database host (e.g., "localhost" or "db.example.com").
        host: Option<String>,
        /// Database port (default: 5432).
        #[serde(default = "default_pg_port")]
        port: Option<u16>,
        /// Database username.
        username: Option<String>,
        /// Database password.
        /// WARNING: Prefer CELLAR_METADATA__PASSWORD env var over storing in config.
        password: Option<String>,
        /// Database name.
        database: Option<String>,
        /// SSL mode for connections.
        ssl_mode: Option<PgSslMode>,
        /// Maximum connections in the pool.
        #[serde(default = "default_max_connections")]
        max_connections: u32,
        /// Statement timeout in milliseconds (prevents hung queries).
        /// PostgreSQL will cancel queries that exceed this duration.
        /// Recommended: 300000 (5 minutes) for GC operations.
        #[serde(default = "default_statement_timeout_ms")]
        statement_timeout_ms: Option<u64>,
    },
}

fn default_max_connections() -> u32 {
    10
}

fn default_pg_port() -> Option<u16> {
    Some(5432)
}

fn default_statement_timeout_ms() -> Option<u64> {
    Some(300000) // 5 minutes
}

fn default_sqlite_query_timeout_secs() -> Option<u64> {
    Some(600) // 10 minutes (advisory only)
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self::Sqlite {
            path: PathBuf::from("./data/metadata.db"),
            query_timeout_secs: default_sqlite_query_timeout_secs(),
        }
    }
}

impl MetadataConfig {
    /// Validate metadata configuration invariants.
    /// Returns an error if the configuration is invalid.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            MetadataConfig::Sqlite { .. } => Ok(()),
            MetadataConfig::Postgres {
                url,
                host,
                database,
                ..
            } => {
                // Must have either url OR (host + database)
                match (url.as_ref(), host.as_ref(), database.as_ref()) {
                    // URL provided - valid
                    (Some(_), _, _) => Ok(()),
                    // No URL, but host and database provided - valid
                    (None, Some(_), Some(_)) => Ok(()),
                    // No URL and missing host or database - invalid
                    (None, None, _) => Err(
                        "postgres config requires either 'url' or 'host' + 'database'".to_string(),
                    ),
                    (None, Some(_), None) => Err(
                        "postgres config requires 'database' when using individual fields"
                            .to_string(),
                    ),
                }
            }
        }
    }
}

/// Signing configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SigningConfig {
    /// Key name (e.g., "cache.example.com-1").
    pub key_name: String,
    /// Private key source.
    pub private_key: PrivateKeyConfig,
}

/// Private key source configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum PrivateKeyConfig {
    /// Key stored in a file.
    File {
        /// Path to the private key file.
        path: PathBuf,
    },
    /// Key stored in environment variable.
    Env {
        /// Environment variable name.
        var: String,
    },
    /// Key provided directly as a value (NOT recommended for production).
    Value {
        /// The signing key in Nix format.
        key: String,
    },
    /// Generate a new key (for development only).
    Generate,
}

/// Garbage collection configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GcConfig {
    /// Grace period in seconds before deleting unreferenced chunks.
    #[serde(default = "default_gc_grace_period_secs")]
    pub grace_period_secs: u64,
    /// Batch size for GC operations.
    #[serde(default = "default_gc_batch_size")]
    pub batch_size: u32,
    /// Per-batch timeout in seconds (prevents hung GC jobs).
    #[serde(default = "default_gc_batch_timeout_secs")]
    pub batch_timeout_secs: u64,
    /// Enable automatic GC scheduling (disabled by default).
    /// When enabled, GC jobs will run automatically on a schedule.
    #[serde(default)]
    pub auto_schedule_enabled: bool,
    /// Interval in seconds between automatic GC runs (default: 1 hour).
    /// Only used if auto_schedule_enabled is true.
    #[serde(default = "default_gc_auto_schedule_interval_secs")]
    pub auto_schedule_interval_secs: u64,
    /// Which GC job types to run automatically (default: all).
    /// Valid values: "upload_gc", "chunk_gc", "manifest_gc", "committing_gc"
    #[serde(default = "default_gc_auto_schedule_jobs")]
    pub auto_schedule_jobs: Vec<String>,
    /// Dry-run mode: report what would be deleted without actually deleting (default: false).
    /// Useful for testing and validation before running actual cleanup.
    #[serde(default)]
    pub dry_run: bool,
    /// Delay in milliseconds between batch iterations (default: None = no delay).
    /// Used for rate limiting to avoid overloading storage/metadata backends.
    /// Example: 100ms delay between processing each 1000 objects.
    #[serde(default)]
    pub batch_delay_ms: Option<u64>,
    /// Maximum acceptable error rate before stopping GC job (default: None = no limit).
    /// Value between 0.0 and 1.0 (e.g., 0.05 = 5% error rate).
    /// Job will abort if error rate exceeds this threshold after processing at least 100 items.
    #[serde(default)]
    pub max_error_rate: Option<f64>,
    /// Maximum number of GC iterations per job run (default: 100,000).
    /// Prevents infinite loops and allows incremental sweeps of very large storage.
    /// Storage sweep will stop after processing this many objects and can be re-run.
    #[serde(default = "default_max_gc_iterations")]
    pub max_gc_iterations: u64,
    /// Enable streaming S3 listings without memory materialization (default: true).
    /// When enabled, listing operations stream results page-by-page instead of
    /// materializing the entire list in memory. This reduces memory usage for
    /// large buckets (>1M objects) from 50-100+ MB to <50 MB.
    /// Supports checkpointing for resumable sweeps if backend supports it.
    #[serde(default = "default_use_streaming_listing")]
    pub use_streaming_listing: bool,
}

fn default_gc_grace_period_secs() -> u64 {
    3600 // 1 hour
}

fn default_gc_batch_size() -> u32 {
    1000
}

fn default_gc_batch_timeout_secs() -> u64 {
    300 // 5 minutes per batch
}

fn default_gc_auto_schedule_interval_secs() -> u64 {
    3600 // 1 hour
}

fn default_gc_auto_schedule_jobs() -> Vec<String> {
    vec![
        "upload_gc".to_string(),
        "chunk_gc".to_string(),
        "manifest_gc".to_string(),
        "committing_gc".to_string(),
        // Note: storage_sweep is NOT enabled by default due to its expense and behavior:
        // - Lists ALL objects in storage (very expensive for large deployments)
        // - Skips deletion if ANY active upload sessions exist within grace_period
        // - Frequent uploads may prevent orphan cleanup (by design for safety)
        // - Requires grace_period to be set appropriately for your upload patterns
        // Recommended: Run manually or schedule with long intervals (e.g., weekly)
        // Enable by adding: "storage_sweep".to_string()
    ]
}

fn default_max_gc_iterations() -> u64 {
    100_000 // 10x original hardcoded limit
}

fn default_use_streaming_listing() -> bool {
    true // Memory-safe default for large buckets
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            grace_period_secs: default_gc_grace_period_secs(),
            batch_size: default_gc_batch_size(),
            batch_timeout_secs: default_gc_batch_timeout_secs(),
            auto_schedule_enabled: false,
            auto_schedule_interval_secs: default_gc_auto_schedule_interval_secs(),
            auto_schedule_jobs: default_gc_auto_schedule_jobs(),
            dry_run: false,
            batch_delay_ms: None,
            max_error_rate: None,
            max_gc_iterations: default_max_gc_iterations(),
            use_streaming_listing: default_use_streaming_listing(),
        }
    }
}

impl GcConfig {
    /// Get the grace period as a Duration.
    pub fn grace_period(&self) -> Duration {
        Duration::seconds(self.grace_period_secs as i64)
    }

    /// Get the batch timeout as a std::time::Duration.
    pub fn batch_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.batch_timeout_secs)
    }

    /// Get the auto schedule interval as a std::time::Duration.
    pub fn auto_schedule_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.auto_schedule_interval_secs)
    }

    /// Validate GC configuration for dangerous settings.
    /// Returns an error for configs that would cause runtime issues.
    pub fn validate(&self) -> Result<(), String> {
        // Check grace_period_secs doesn't overflow when cast to i64
        // (used by Duration::seconds which takes i64)
        if self.grace_period_secs > i64::MAX as u64 {
            return Err(format!(
                "gc.grace_period_secs {} exceeds maximum value {} (would overflow Duration)",
                self.grace_period_secs,
                i64::MAX
            ));
        }

        // Same check for other duration fields that get cast to i64
        if self.batch_timeout_secs > i64::MAX as u64 {
            return Err(format!(
                "gc.batch_timeout_secs {} exceeds maximum value {}",
                self.batch_timeout_secs,
                i64::MAX
            ));
        }

        if self.auto_schedule_interval_secs > i64::MAX as u64 {
            return Err(format!(
                "gc.auto_schedule_interval_secs {} exceeds maximum value {}",
                self.auto_schedule_interval_secs,
                i64::MAX
            ));
        }

        Ok(())
    }
}

/// Rate limiting configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Enable rate limiting.
    #[serde(default)]
    pub enabled: bool,
    /// Requests per minute per IP (for unauthenticated/global limiting).
    #[serde(default = "default_ip_requests_per_minute")]
    pub ip_requests_per_minute: u32,
    /// Requests per minute per token (for authenticated requests).
    #[serde(default = "default_token_requests_per_minute")]
    pub token_requests_per_minute: u32,
    /// Burst size (allows temporary burst above rate limit).
    #[serde(default = "default_burst_size")]
    pub burst_size: u32,
    /// Trusted proxy IP addresses/CIDR ranges.
    /// Only requests from these IPs will have X-Forwarded-For/X-Real-IP headers trusted.
    /// If empty, forwarded headers are never trusted (only direct connection IP is used).
    /// Use ["*"] to trust all proxies (NOT recommended for production).
    #[serde(default)]
    pub trusted_proxies: Vec<String>,
    /// Maximum number of unique IPs/tokens to track before rejecting new entries (default: 100000).
    /// Prevents memory exhaustion from attackers spraying unique IPs.
    /// When limit is reached, new IPs are rejected with 429 until cleanup runs.
    #[serde(default = "default_max_entries")]
    pub max_entries: u32,
    /// Interval in seconds between cleanup sweeps of stale entries (default: 60).
    #[serde(default = "default_cleanup_interval_secs")]
    pub cleanup_interval_secs: u64,
    /// Time-to-live in seconds for rate limit entries (default: 300).
    /// Entries not accessed within this period are evicted during cleanup.
    /// Should be at least 2x the rate limit window (e.g., 2x 60s = 120s minimum).
    #[serde(default = "default_entry_ttl_secs")]
    pub entry_ttl_secs: u64,
}

fn default_ip_requests_per_minute() -> u32 {
    60 // 1 request per second for unauthenticated
}

fn default_token_requests_per_minute() -> u32 {
    600 // 10 requests per second for authenticated
}

fn default_burst_size() -> u32 {
    20
}

fn default_max_entries() -> u32 {
    100_000 // 100k unique IPs/tokens before rejecting new entries
}

fn default_cleanup_interval_secs() -> u64 {
    60 // Sweep every minute
}

fn default_entry_ttl_secs() -> u64 {
    300 // 5 minutes - entries not accessed within this period are evicted
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ip_requests_per_minute: default_ip_requests_per_minute(),
            token_requests_per_minute: default_token_requests_per_minute(),
            burst_size: default_burst_size(),
            trusted_proxies: Vec::new(),
            max_entries: default_max_entries(),
            cleanup_interval_secs: default_cleanup_interval_secs(),
            entry_ttl_secs: default_entry_ttl_secs(),
        }
    }
}

impl RateLimitConfig {
    /// Validate rate limit configuration for dangerous settings.
    /// Returns warnings for configs that are insecure but allowed,
    /// and errors for configs that are unsafe and should be rejected.
    pub fn validate(&self) -> Result<Vec<String>, String> {
        let mut warnings = Vec::new();

        if !self.enabled {
            return Ok(warnings);
        }

        // Error on zero cleanup interval (would cause tokio::time::interval to panic)
        if self.cleanup_interval_secs == 0 {
            return Err("rate_limit.cleanup_interval_secs cannot be 0. \
                 This would cause a panic when creating the cleanup timer. \
                 Use a value >= 1 second."
                .to_string());
        }

        // Warn about trusting all proxies
        if self.trusted_proxies.len() == 1 && self.trusted_proxies[0] == "*" {
            warnings.push(
                "rate_limit.trusted_proxies=['*'] trusts ALL forwarded headers. \
                 This allows clients to spoof their IP address and bypass rate limits. \
                 Only use this setting in development or behind a trusted reverse proxy."
                    .to_string(),
            );
        }

        // Warn if TTL is too short relative to rate limit window
        if self.entry_ttl_secs < 120 {
            warnings.push(format!(
                "rate_limit.entry_ttl_secs={} is very short. \
                 Entries may be evicted before rate limits reset, \
                 allowing attackers to bypass limits by waiting. \
                 Recommended minimum: 120 seconds.",
                self.entry_ttl_secs
            ));
        }

        Ok(warnings)
    }
}

/// Complete application configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppConfig {
    /// Server configuration.
    #[serde(default)]
    pub server: ServerConfig,
    /// Storage backend configuration.
    #[serde(default)]
    pub storage: StorageConfig,
    /// Metadata store configuration.
    #[serde(default)]
    pub metadata: MetadataConfig,
    /// Admin token configuration (required).
    pub admin: AdminConfig,
    /// Signing configuration (optional).
    pub signing: Option<SigningConfig>,
    /// Garbage collection configuration.
    #[serde(default)]
    pub gc: GcConfig,
    /// Rate limiting configuration.
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
}

impl AppConfig {
    /// Create a test configuration with sensible defaults.
    ///
    /// **For testing only.** Uses filesystem storage, SQLite metadata,
    /// and a dummy admin token.
    pub fn for_testing() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            metadata: MetadataConfig::default(),
            admin: AdminConfig::for_testing(),
            signing: None,
            gc: GcConfig::default(),
            rate_limit: RateLimitConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gc_config_defaults_to_streaming_listing() {
        let config = GcConfig::default();
        assert!(
            config.use_streaming_listing,
            "use_streaming_listing should default to true for memory safety"
        );
    }

    #[test]
    fn test_gc_config_deserialize_without_streaming_field() {
        let json = r#"{"grace_period_secs": 3600}"#;
        let config: GcConfig = serde_json::from_str(json).unwrap();
        assert!(
            config.use_streaming_listing,
            "use_streaming_listing should default to true when not specified in JSON"
        );
    }

    #[test]
    fn test_server_config_defaults_metrics_enabled() {
        let config = ServerConfig::default();
        assert!(
            config.metrics_enabled,
            "metrics_enabled should default to true for backwards compatibility"
        );
    }

    #[test]
    fn test_server_config_defaults_shortcircuit_enabled() {
        let config = ServerConfig::default();
        assert!(
            !config.disable_chunk_dedup_shortcircuit,
            "disable_chunk_dedup_shortcircuit should default to false (optimization enabled)"
        );
    }

    #[test]
    fn test_storage_config_s3_roundtrip_without_credentials() {
        let config = StorageConfig::S3 {
            bucket: "bucket".to_string(),
            endpoint: Some("http://localhost:9000".to_string()),
            region: Some("us-east-1".to_string()),
            prefix: Some("cache".to_string()),
            access_key_id: None,
            secret_access_key: None,
            force_path_style: true,
        };

        let json = serde_json::to_string(&config).unwrap();
        let decoded: StorageConfig = serde_json::from_str(&json).unwrap();

        match decoded {
            StorageConfig::S3 {
                access_key_id,
                secret_access_key,
                force_path_style,
                ..
            } => {
                assert!(access_key_id.is_none());
                assert!(secret_access_key.is_none());
                assert!(force_path_style);
            }
            _ => panic!("expected S3 config"),
        }
    }

    #[test]
    fn test_storage_config_s3_roundtrip_with_credentials() {
        let config = StorageConfig::S3 {
            bucket: "bucket".to_string(),
            endpoint: None,
            region: None,
            prefix: None,
            access_key_id: Some("access-key".to_string()),
            secret_access_key: Some("secret-key".to_string()),
            force_path_style: false,
        };

        let json = serde_json::to_string(&config).unwrap();
        let decoded: StorageConfig = serde_json::from_str(&json).unwrap();

        match decoded {
            StorageConfig::S3 {
                access_key_id,
                secret_access_key,
                force_path_style,
                ..
            } => {
                assert_eq!(access_key_id.as_deref(), Some("access-key"));
                assert_eq!(secret_access_key.as_deref(), Some("secret-key"));
                assert!(!force_path_style);
            }
            _ => panic!("expected S3 config"),
        }
    }

    #[test]
    fn test_storage_config_s3_validate_partial_credentials() {
        let invalid = StorageConfig::S3 {
            bucket: "bucket".to_string(),
            endpoint: None,
            region: None,
            prefix: None,
            access_key_id: Some("access-key".to_string()),
            secret_access_key: None,
            force_path_style: false,
        };
        assert!(invalid.validate().is_err());

        let valid = StorageConfig::S3 {
            bucket: "bucket".to_string(),
            endpoint: None,
            region: None,
            prefix: None,
            access_key_id: Some("access-key".to_string()),
            secret_access_key: Some("secret-key".to_string()),
            force_path_style: false,
        };
        assert!(valid.validate().is_ok());
    }

    #[test]
    fn test_storage_config_s3_force_path_style_defaults_to_false() {
        // Deserialize config without force_path_style field - should default to false
        let json = r#"{"type":"s3","bucket":"test","endpoint":"https://s3.amazonaws.com"}"#;
        let config: StorageConfig = serde_json::from_str(json).unwrap();

        match config {
            StorageConfig::S3 {
                force_path_style, ..
            } => {
                assert!(
                    !force_path_style,
                    "force_path_style should default to false for AWS S3 compatibility"
                );
            }
            _ => panic!("expected S3 config"),
        }
    }
}
