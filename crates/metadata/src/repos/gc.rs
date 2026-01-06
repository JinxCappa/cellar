//! Garbage collection repository.

use crate::error::MetadataResult;
use crate::models::GcJobRow;
use async_trait::async_trait;
#[allow(unused_imports)]
use time::OffsetDateTime;
use uuid::Uuid;

/// Repository for garbage collection operations.
#[async_trait]
pub trait GcRepo: Send + Sync {
    /// Create a GC job.
    async fn create_gc_job(&self, job: &GcJobRow) -> MetadataResult<()>;

    /// Get a GC job by ID.
    async fn get_gc_job(&self, job_id: Uuid) -> MetadataResult<Option<GcJobRow>>;

    /// Update GC job state.
    async fn update_gc_job_state(
        &self,
        job_id: Uuid,
        state: &str,
        finished_at: Option<OffsetDateTime>,
        stats_json: Option<&str>,
    ) -> MetadataResult<()>;

    /// Get recent GC jobs.
    async fn get_recent_gc_jobs(&self, limit: u32) -> MetadataResult<Vec<GcJobRow>>;

    /// Get running GC jobs.
    async fn get_running_gc_jobs(&self) -> MetadataResult<Vec<GcJobRow>>;
}

/// GC job types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcJobType {
    /// Clean up expired upload sessions.
    UploadGc,
    /// Clean up unreferenced chunks.
    ChunkGc,
    /// Clean up orphaned manifests.
    ManifestGc,
}

impl GcJobType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UploadGc => "upload_gc",
            Self::ChunkGc => "chunk_gc",
            Self::ManifestGc => "manifest_gc",
        }
    }
}

/// GC job state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcJobState {
    Queued,
    Running,
    Finished,
    Failed,
}

impl GcJobState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Finished => "finished",
            Self::Failed => "failed",
        }
    }
}

/// GC job statistics.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct GcStats {
    /// Number of items processed.
    pub items_processed: u64,
    /// Number of items deleted.
    pub items_deleted: u64,
    /// Bytes reclaimed.
    pub bytes_reclaimed: u64,
    /// Errors encountered.
    pub errors: u64,
}

// =============================================================================
// F-006: Reachability GC types
// =============================================================================

/// Reachability GC mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReachabilityMode {
    /// Report discrepancies without modifying.
    DryRun,
    /// Correct refcount mismatches.
    Correct,
    /// Correct and delete unreferenced chunks.
    Aggressive,
}

impl ReachabilityMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DryRun => "dry_run",
            Self::Correct => "correct",
            Self::Aggressive => "aggressive",
        }
    }
}

/// Reachability verification result for a single chunk.
#[derive(Debug, Clone)]
pub struct RefcountDiscrepancy {
    pub chunk_hash: String,
    pub stored_refcount: i32,
    pub computed_refcount: i32,
}

/// Reachability GC statistics.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ReachabilityStats {
    /// Total chunks verified.
    pub chunks_verified: u64,
    /// Discrepancies found.
    pub discrepancies_found: u64,
    /// Refcounts corrected.
    pub refcounts_corrected: u64,
    /// Orphaned chunks deleted (aggressive mode).
    pub orphans_deleted: u64,
    /// Bytes reclaimed.
    pub bytes_reclaimed: u64,
}

/// Repository trait extension for reachability GC operations.
#[async_trait]
pub trait ReachabilityRepo: Send + Sync {
    /// Get all manifest hashes from visible store paths.
    async fn get_all_visible_manifests(&self) -> MetadataResult<Vec<String>>;

    /// Get chunk hashes for a manifest (for walking the reference graph).
    async fn get_chunks_for_manifest(&self, manifest_hash: &str) -> MetadataResult<Vec<String>>;

    /// Get current refcount for a chunk.
    async fn get_chunk_refcount(&self, chunk_hash: &str) -> MetadataResult<Option<i32>>;

    /// Set refcount for a chunk (for corrections).
    async fn set_chunk_refcount(&self, chunk_hash: &str, refcount: i32) -> MetadataResult<()>;

    /// Batch update refcounts (more efficient for corrections).
    async fn batch_update_refcounts(
        &self,
        updates: &[(String, i32)],
    ) -> MetadataResult<u64>;
}
