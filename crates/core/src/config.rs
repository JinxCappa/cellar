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
        }
    }
}

impl ServerConfig {
    /// Get the upload timeout as a Duration.
    pub fn upload_timeout(&self) -> Duration {
        Duration::seconds(self.upload_timeout_secs as i64)
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

/// Metadata store configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum MetadataConfig {
    /// SQLite database.
    Sqlite {
        /// Database file path.
        path: PathBuf,
    },
    /// PostgreSQL database.
    Postgres {
        /// Connection URL.
        url: String,
        /// Maximum connections in the pool.
        #[serde(default = "default_max_connections")]
        max_connections: u32,
    },
}

fn default_max_connections() -> u32 {
    10
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self::Sqlite {
            path: PathBuf::from("./data/metadata.db"),
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
}

fn default_gc_grace_period_secs() -> u64 {
    3600 // 1 hour
}

fn default_gc_batch_size() -> u32 {
    1000
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            grace_period_secs: default_gc_grace_period_secs(),
            batch_size: default_gc_batch_size(),
        }
    }
}

impl GcConfig {
    /// Get the grace period as a Duration.
    pub fn grace_period(&self) -> Duration {
        Duration::seconds(self.grace_period_secs as i64)
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

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ip_requests_per_minute: default_ip_requests_per_minute(),
            token_requests_per_minute: default_token_requests_per_minute(),
            burst_size: default_burst_size(),
            trusted_proxies: Vec::new(),
        }
    }
}

/// Complete application configuration.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
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
    /// Signing configuration (optional).
    pub signing: Option<SigningConfig>,
    /// Garbage collection configuration.
    #[serde(default)]
    pub gc: GcConfig,
    /// Rate limiting configuration.
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
}
