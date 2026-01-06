//! Upload session types and lifecycle.

use crate::chunk::ChunkHash;
use crate::hash::NarHash;
use crate::manifest::ManifestHash;
use crate::store_path::StorePath;
use crate::token::TokenId;
use serde::{Deserialize, Serialize};
use std::fmt;
use time::OffsetDateTime;
use uuid::Uuid;

/// Unique identifier for an upload session.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UploadId(Uuid);

impl UploadId {
    /// Generate a new random upload ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Parse from a string.
    pub fn parse(s: &str) -> crate::Result<Self> {
        Uuid::parse_str(s)
            .map(Self)
            .map_err(|e| crate::Error::UploadSession(format!("invalid upload ID: {e}")))
    }

    /// Get the underlying UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for UploadId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for UploadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UploadId({})", self.0)
    }
}

impl fmt::Display for UploadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Upload session state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UploadState {
    /// Session is open and accepting chunks.
    Open,
    /// Session was successfully committed.
    Committed,
    /// Session was explicitly aborted.
    Aborted,
    /// Session expired without completing.
    Expired,
}

impl UploadState {
    /// Check if the session is still active (can receive chunks).
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Open)
    }

    /// Check if the session reached a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Committed | Self::Aborted | Self::Expired)
    }
}

/// An upload session tracking resumable upload state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadSession {
    /// Unique session identifier.
    pub id: UploadId,
    /// The store path being uploaded.
    pub store_path: StorePath,
    /// Expected NAR size in bytes.
    pub nar_size: u64,
    /// Expected NAR hash (for verification at commit).
    pub nar_hash: NarHash,
    /// Chunk size for this upload.
    pub chunk_size: u64,
    /// Expected manifest hash (if known at creation).
    pub manifest_hash: Option<ManifestHash>,
    /// Current session state.
    pub state: UploadState,
    /// Token that owns this session.
    pub owner_token_id: Option<TokenId>,
    /// When the session was created.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    /// When the session was last updated.
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    /// When the session expires.
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
    /// Distributed tracing ID.
    pub trace_id: Option<String>,
}

impl UploadSession {
    /// Create a new upload session.
    pub fn new(
        store_path: StorePath,
        nar_size: u64,
        nar_hash: NarHash,
        chunk_size: u64,
        expires_in: time::Duration,
    ) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            id: UploadId::new(),
            store_path,
            nar_size,
            nar_hash,
            chunk_size,
            manifest_hash: None,
            state: UploadState::Open,
            owner_token_id: None,
            created_at: now,
            updated_at: now,
            expires_at: now + expires_in,
            trace_id: None,
        }
    }

    /// Check if the session has expired.
    pub fn is_expired(&self) -> bool {
        OffsetDateTime::now_utc() > self.expires_at
    }

    /// Calculate the expected number of chunks.
    pub fn expected_chunk_count(&self) -> u64 {
        self.nar_size.div_ceil(self.chunk_size)
    }
}

/// Request to create an upload session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateUploadRequest {
    /// The store path to upload.
    pub store_path: String,
    /// NAR size in bytes.
    pub nar_size: u64,
    /// NAR hash in SRI format.
    pub nar_hash: String,
    /// Chunk size (optional, uses default if not specified).
    pub chunk_size: Option<u64>,
    /// Manifest hash if already computed client-side.
    pub manifest_hash: Option<String>,
    /// Expected chunk hashes in order (enables server-side resume tracking).
    /// When provided, the server can track exactly which chunks are missing
    /// and return them in the missing_chunks response for efficient resume.
    #[serde(default)]
    pub expected_chunks: Option<Vec<ExpectedChunk>>,
}

/// Expected chunk information for upload resume support.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExpectedChunk {
    /// The chunk hash (hex-encoded).
    pub hash: String,
    /// The chunk size in bytes.
    pub size: u64,
}

/// Response from creating an upload session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateUploadResponse {
    /// The upload session ID.
    pub upload_id: String,
    /// Which chunks are missing ("all" for new uploads).
    pub missing_chunks: MissingChunks,
    /// Maximum parallel chunk uploads allowed.
    pub max_parallel_chunks: u32,
}

/// Indicator of which chunks are missing.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MissingChunks {
    /// All chunks are missing (new upload).
    All(String),
    /// Specific chunk hashes that are missing.
    List(Vec<String>),
    /// Unknown missing chunks - client should query GET /v1/uploads/{id}
    /// to retrieve the list of received chunks and compute what's missing.
    /// This occurs when resuming an upload that was created without expected_chunks.
    Unknown {
        /// Number of chunks received so far.
        received_count: usize,
    },
}

/// Response from querying upload state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadStateResponse {
    /// Chunk hashes that have been received.
    pub received_chunks: Vec<String>,
    /// Chunk hashes that are still missing.
    /// NOTE: For dynamic uploads where `chunks_predeclared` is false, this list
    /// is NOT authoritative - it only reflects chunks that were received but somehow
    /// marked incomplete, not chunks that were never sent. Clients should use
    /// `chunks_predeclared` to determine if `missing_chunks` can be trusted for resumption.
    pub missing_chunks: Vec<String>,
    /// Whether expected chunks were pre-declared at session creation.
    /// When true, `missing_chunks` is authoritative (you can trust it for resumption).
    /// When false, `missing_chunks` may be empty even if the upload is incomplete,
    /// because the server doesn't know which chunks to expect.
    pub chunks_predeclared: bool,
    /// When the session expires.
    pub expires_at: String,
}

/// Request to commit an upload.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitUploadRequest {
    /// Ordered list of chunk hashes forming the manifest.
    pub manifest: Vec<String>,
    /// References (full store paths) for the narinfo.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<String>>,
    /// Deriver (full store path) for the narinfo.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deriver: Option<String>,
}

/// Information about a received chunk for an upload.
#[derive(Clone, Debug)]
pub struct ReceivedChunk {
    /// Position in the upload (0-indexed).
    pub position: u32,
    /// The chunk hash.
    pub chunk_hash: ChunkHash,
    /// Size in bytes.
    pub size: u64,
    /// When the chunk was received.
    pub received_at: OffsetDateTime,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::ContentHash;
    use crate::store_path::StorePath;

    fn sample_store_path() -> StorePath {
        StorePath::parse("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo").unwrap()
    }

    fn sample_nar_hash() -> NarHash {
        NarHash::from_content_hash(ContentHash::compute(b"nar"))
    }

    #[test]
    fn test_upload_id_roundtrip() {
        let id = UploadId::new();
        let as_str = id.to_string();
        let parsed = UploadId::parse(&as_str).unwrap();
        assert_eq!(id, parsed);
        assert_eq!(id.as_uuid(), parsed.as_uuid());
        assert!(UploadId::parse("not-a-uuid").is_err());
    }

    #[test]
    fn test_upload_state_flags() {
        assert!(UploadState::Open.is_active());
        assert!(!UploadState::Open.is_terminal());
        for state in [
            UploadState::Committed,
            UploadState::Aborted,
            UploadState::Expired,
        ] {
            assert!(!state.is_active());
            assert!(state.is_terminal());
        }
    }

    #[test]
    fn test_upload_session_expected_chunk_count() {
        let session = UploadSession::new(
            sample_store_path(),
            100,
            sample_nar_hash(),
            64,
            time::Duration::seconds(60),
        );
        assert_eq!(session.expected_chunk_count(), 2);

        let session = UploadSession::new(
            sample_store_path(),
            128,
            sample_nar_hash(),
            64,
            time::Duration::seconds(60),
        );
        assert_eq!(session.expected_chunk_count(), 2);

        let session = UploadSession::new(
            sample_store_path(),
            0,
            sample_nar_hash(),
            64,
            time::Duration::seconds(60),
        );
        assert_eq!(session.expected_chunk_count(), 0);
    }

    #[test]
    fn test_upload_session_expired() {
        let session = UploadSession::new(
            sample_store_path(),
            1,
            sample_nar_hash(),
            1,
            time::Duration::seconds(-1),
        );
        assert!(session.is_expired());
    }
}
