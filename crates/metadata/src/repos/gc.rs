//! Garbage collection repository.

use crate::error::MetadataResult;
use crate::models::GcJobRow;
use async_trait::async_trait;
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
        sweep_checkpoint_json: Option<&str>,
    ) -> MetadataResult<()>;

    /// Get recent GC jobs for a specific cache.
    async fn get_recent_gc_jobs(
        &self,
        cache_id: Option<Uuid>,
        limit: u32,
    ) -> MetadataResult<Vec<GcJobRow>>;

    /// Get running GC jobs for a specific cache.
    async fn get_running_gc_jobs(&self, cache_id: Option<Uuid>) -> MetadataResult<Vec<GcJobRow>>;

    /// Get active GC jobs (queued or running) for a specific cache.
    /// Used for duplicate detection and job isolation.
    async fn get_active_gc_jobs(&self, cache_id: Option<Uuid>) -> MetadataResult<Vec<GcJobRow>>;

    /// Get orphaned GC jobs (queued or running) that need recovery on server restart.
    /// Returns ALL jobs in 'queued' or 'running' state, regardless of cache_id.
    /// These jobs are considered orphaned because:
    /// - 'queued': Server crashed before spawning the background task
    /// - 'running': Server crashed while GC was executing
    async fn get_orphaned_gc_jobs(&self) -> MetadataResult<Vec<GcJobRow>>;

    /// Delete all GC jobs for a cache (cascade cleanup).
    async fn delete_gc_jobs_for_cache(&self, cache_id: Uuid) -> MetadataResult<u64>;
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
    /// Recover sessions stuck in 'committing' state.
    CommittingGc,
    /// Clean up orphaned storage objects (no metadata).
    StorageSweep,
}

impl GcJobType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UploadGc => "upload_gc",
            Self::ChunkGc => "chunk_gc",
            Self::ManifestGc => "manifest_gc",
            Self::CommittingGc => "committing_gc",
            Self::StorageSweep => "storage_sweep",
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
    /// Get all manifest hashes from visible store paths for a specific domain.
    async fn get_all_visible_manifests(&self, domain_id: Uuid) -> MetadataResult<Vec<String>>;

    /// Get chunk hashes for a manifest (for walking the reference graph).
    async fn get_chunks_for_manifest(
        &self,
        domain_id: Uuid,
        manifest_hash: &str,
    ) -> MetadataResult<Vec<String>>;

    /// Get current refcount for a chunk.
    async fn get_chunk_refcount(
        &self,
        domain_id: Uuid,
        chunk_hash: &str,
    ) -> MetadataResult<Option<i32>>;

    /// Set refcount for a chunk (for corrections).
    async fn set_chunk_refcount(
        &self,
        domain_id: Uuid,
        chunk_hash: &str,
        refcount: i32,
    ) -> MetadataResult<()>;

    /// Batch update refcounts (more efficient for corrections).
    async fn batch_update_refcounts(
        &self,
        domain_id: Uuid,
        updates: &[(String, i32)],
    ) -> MetadataResult<u64>;
}
