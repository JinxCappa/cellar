//! Database models mapping to the metadata schema.

use sqlx::FromRow;
use time::OffsetDateTime;
use uuid::Uuid;

// =============================================================================
// F-001: Cache record
// =============================================================================

/// Cache record for multi-tenant scoping.
#[derive(Debug, Clone, FromRow)]
pub struct CacheRow {
    pub cache_id: Uuid,
    pub cache_name: String,
    pub public_base_url: Option<String>,
    pub is_public: bool,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

// =============================================================================
// Upload Sessions (with F-004 error tracking)
// =============================================================================

/// Upload session record.
#[derive(Debug, Clone, FromRow)]
pub struct UploadSessionRow {
    pub upload_id: Uuid,
    pub cache_id: Option<Uuid>,
    pub store_path: String,
    pub store_path_hash: String,
    pub nar_size: i64,
    pub nar_hash: String,
    pub chunk_size: i64,
    pub manifest_hash: Option<String>,
    pub state: String,
    pub owner_token_id: Option<Uuid>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
    pub trace_id: Option<String>,
    // F-004: Error tracking fields
    pub error_code: Option<String>,
    pub error_detail: Option<String>,
}

/// Expected chunk for an upload.
#[derive(Debug, Clone, FromRow)]
pub struct UploadExpectedChunkRow {
    pub upload_id: Uuid,
    pub position: i32,
    pub chunk_hash: String,
    pub size_bytes: i64,
    pub received_at: Option<OffsetDateTime>,
}

/// Global chunk record.
#[derive(Debug, Clone, FromRow)]
pub struct ChunkRow {
    pub chunk_hash: String,
    pub size_bytes: i64,
    pub object_key: Option<String>,
    pub refcount: i32,
    pub created_at: OffsetDateTime,
    pub last_accessed_at: Option<OffsetDateTime>,
}

/// Manifest record.
#[derive(Debug, Clone, FromRow)]
pub struct ManifestRow {
    pub manifest_hash: String,
    pub chunk_size: i64,
    pub chunk_count: i32,
    pub nar_size: i64,
    pub object_key: Option<String>,
    pub created_at: OffsetDateTime,
}

/// Manifest chunk mapping.
#[derive(Debug, Clone, FromRow)]
pub struct ManifestChunkRow {
    pub manifest_hash: String,
    pub position: i32,
    pub chunk_hash: String,
}

/// Store path record.
#[derive(Debug, Clone, FromRow)]
pub struct StorePathRow {
    pub store_path_hash: String,
    pub cache_id: Option<Uuid>,
    pub store_path: String,
    pub nar_hash: String,
    pub nar_size: i64,
    pub manifest_hash: String,
    pub created_at: OffsetDateTime,
    pub committed_at: Option<OffsetDateTime>,
    pub visibility_state: String,
    pub uploader_token_id: Option<Uuid>,
    // F-003: Content-addressable field (e.g., "fixed:sha256:...")
    pub ca: Option<String>,
}

// =============================================================================
// F-003: Store path references
// =============================================================================

/// Store path reference record for tracking dependencies.
#[derive(Debug, Clone, FromRow)]
pub struct StorePathReferenceRow {
    pub cache_id: Option<Uuid>,
    pub store_path_hash: String,
    pub reference_hash: String,
    pub reference_type: String, // "reference" or "deriver"
}

/// Narinfo record.
#[derive(Debug, Clone, FromRow)]
pub struct NarInfoRow {
    pub cache_id: Option<Uuid>,
    pub store_path_hash: String,
    pub narinfo_object_key: String,
    pub content_hash: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// Signature record.
#[derive(Debug, Clone, FromRow)]
pub struct SignatureRow {
    pub cache_id: Option<Uuid>,
    pub store_path_hash: String,
    pub key_id: Uuid,
    pub signature: String,
    pub created_at: OffsetDateTime,
}

/// Token record.
#[derive(Debug, Clone, FromRow)]
pub struct TokenRow {
    pub token_id: Uuid,
    pub cache_id: Option<Uuid>,
    pub token_hash: String,
    pub scopes: String, // JSON array
    pub expires_at: Option<OffsetDateTime>,
    pub revoked_at: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    pub last_used_at: Option<OffsetDateTime>,
    pub description: Option<String>,
}

/// Signing key record.
#[derive(Debug, Clone, FromRow)]
pub struct SigningKeyRow {
    pub key_id: Uuid,
    pub cache_id: Option<Uuid>,
    pub key_name: String,
    pub public_key: String,
    pub private_key_ref: String,
    pub status: String,
    pub created_at: OffsetDateTime,
    pub rotated_at: Option<OffsetDateTime>,
}

/// GC job record.
#[derive(Debug, Clone, FromRow)]
pub struct GcJobRow {
    pub gc_job_id: Uuid,
    pub cache_id: Option<Uuid>,
    pub job_type: String,
    pub state: String,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub stats_json: Option<String>,
}

// =============================================================================
// F-002: Trusted builder keys
// =============================================================================

/// Trusted builder key for multi-signer narinfo verification.
#[derive(Debug, Clone, FromRow)]
pub struct TrustedBuilderKeyRow {
    pub trusted_key_id: Uuid,
    pub cache_id: Option<Uuid>,
    pub key_name: String,
    pub public_key: String,
    pub trust_level: String, // "trusted", "verified", "untrusted"
    pub added_by_token_id: Option<Uuid>,
    pub created_at: OffsetDateTime,
    pub expires_at: Option<OffsetDateTime>,
    pub revoked_at: Option<OffsetDateTime>,
}

// =============================================================================
// F-005: Tombstone tracking
// =============================================================================

/// Tombstone record for soft-delete tracking.
#[derive(Debug, Clone, FromRow)]
pub struct TombstoneRow {
    pub tombstone_id: Uuid,
    pub entity_type: String,  // "store_path", "chunk", "manifest"
    pub entity_id: String,    // the hash/id of the deleted entity
    pub cache_id: Option<Uuid>,
    pub deleted_at: OffsetDateTime,
    pub deleted_by_token_id: Option<Uuid>,
    pub reason: Option<String>,
    pub gc_eligible_at: OffsetDateTime,
    pub gc_completed_at: Option<OffsetDateTime>,
}
