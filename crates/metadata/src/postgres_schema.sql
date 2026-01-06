-- PostgreSQL schema for cellar-metadata
-- This schema mirrors the SQLite schema with PostgreSQL-specific types

-- Storage domains
CREATE TABLE IF NOT EXISTS domains (
    domain_id UUID PRIMARY KEY,
    domain_name TEXT NOT NULL UNIQUE,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    -- Prevent nil UUID to avoid collision with NULL sentinel in COALESCE indexes
    CHECK (domain_id != '00000000-0000-0000-0000-000000000000'::UUID)
);
CREATE INDEX IF NOT EXISTS idx_domains_name ON domains(domain_name);
CREATE UNIQUE INDEX IF NOT EXISTS idx_domains_default ON domains(is_default) WHERE is_default = TRUE;
INSERT INTO domains (domain_id, domain_name, is_default, created_at, updated_at)
VALUES ('00000000-0000-0000-0000-000000000001'::UUID, 'default', TRUE, NOW(), NOW())
ON CONFLICT (domain_id) DO NOTHING;

-- F-001: Caches table
CREATE TABLE IF NOT EXISTS caches (
    cache_id UUID PRIMARY KEY,
    domain_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001'::UUID REFERENCES domains(domain_id),
    cache_name TEXT NOT NULL UNIQUE,
    public_base_url TEXT,
    is_public BOOLEAN NOT NULL DEFAULT FALSE,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    -- Prevent nil UUID to avoid collision with NULL sentinel in COALESCE indexes
    CHECK (cache_id != '00000000-0000-0000-0000-000000000000'::UUID)
);
CREATE INDEX IF NOT EXISTS idx_caches_name ON caches(cache_name);
CREATE INDEX IF NOT EXISTS idx_caches_domain ON caches(domain_id);
-- Only one cache can be the default at a time (partial unique index)
CREATE UNIQUE INDEX IF NOT EXISTS idx_caches_default ON caches(is_default) WHERE is_default = TRUE;
ALTER TABLE caches ADD COLUMN IF NOT EXISTS domain_id UUID;

-- Upload sessions (with F-004 error tracking and commit progress tracking)
CREATE TABLE IF NOT EXISTS upload_sessions (
    upload_id UUID PRIMARY KEY,
    cache_id UUID,
    domain_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001'::UUID,
    store_path TEXT NOT NULL,
    store_path_hash TEXT NOT NULL,
    nar_size BIGINT NOT NULL,
    nar_hash TEXT NOT NULL,
    chunk_size BIGINT NOT NULL,
    manifest_hash TEXT,
    state TEXT NOT NULL DEFAULT 'open',
    owner_token_id UUID,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    trace_id TEXT,
    error_code TEXT,
    error_detail TEXT,
    -- Commit progress tracking for stuck detection and recovery
    commit_started_at TIMESTAMPTZ,
    commit_progress TEXT,
    -- Tracks whether expected_chunks were pre-declared at session creation.
    -- Used to correctly classify missing chunk errors as client vs server errors.
    chunks_predeclared BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_upload_sessions_state ON upload_sessions(state, expires_at);
CREATE INDEX IF NOT EXISTS idx_upload_sessions_store_path ON upload_sessions(cache_id, store_path_hash);
ALTER TABLE upload_sessions ADD COLUMN IF NOT EXISTS domain_id UUID;

-- Upload expected chunks
CREATE TABLE IF NOT EXISTS upload_expected_chunks (
    upload_id UUID NOT NULL,
    position INTEGER NOT NULL,
    chunk_hash TEXT NOT NULL,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    received_at TIMESTAMPTZ,
    -- Tracks whether actual data was uploaded (true) vs just marked via dedup shortcircuit (false).
    -- Used to prevent cross-session chunk reference attacks when require_expected_chunks=false.
    uploaded_data BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (upload_id, position),
    FOREIGN KEY (upload_id) REFERENCES upload_sessions(upload_id) ON DELETE CASCADE
);
-- Migration: Add uploaded_data column if table exists from older schema version.
ALTER TABLE upload_expected_chunks ADD COLUMN IF NOT EXISTS uploaded_data BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX IF NOT EXISTS idx_upload_chunks_received ON upload_expected_chunks(upload_id, received_at);
-- NOTE: No unique index on (upload_id, chunk_hash) - duplicate chunk hashes at different
-- positions are valid (e.g., NAR files with repeated content patterns).

-- Chunks
CREATE TABLE IF NOT EXISTS chunks (
    domain_id UUID NOT NULL,
    chunk_hash TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    object_key TEXT,
    refcount INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    last_accessed_at TIMESTAMPTZ,
    PRIMARY KEY (domain_id, chunk_hash)
);
CREATE INDEX IF NOT EXISTS idx_chunks_refcount ON chunks(domain_id, refcount, created_at);

-- Manifests
CREATE TABLE IF NOT EXISTS manifests (
    domain_id UUID NOT NULL,
    manifest_hash TEXT NOT NULL,
    chunk_size BIGINT NOT NULL,
    chunk_count INTEGER NOT NULL,
    nar_size BIGINT NOT NULL,
    object_key TEXT,
    refcount INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (domain_id, manifest_hash)
);
CREATE INDEX IF NOT EXISTS idx_manifests_refcount ON manifests(domain_id, refcount, created_at);

-- Manifest chunks
CREATE TABLE IF NOT EXISTS manifest_chunks (
    domain_id UUID NOT NULL,
    manifest_hash TEXT NOT NULL,
    position INTEGER NOT NULL,
    chunk_hash TEXT NOT NULL,
    PRIMARY KEY (domain_id, manifest_hash, position),
    FOREIGN KEY (domain_id, manifest_hash) REFERENCES manifests(domain_id, manifest_hash) ON DELETE CASCADE,
    FOREIGN KEY (domain_id, chunk_hash) REFERENCES chunks(domain_id, chunk_hash)
);
CREATE INDEX IF NOT EXISTS idx_manifest_chunks_chunk ON manifest_chunks(domain_id, chunk_hash);

-- Store paths (with F-003 CA field and tenant isolation)
-- Using UNIQUE constraint instead of composite PK because PostgreSQL PKs cannot contain NULL.
-- cache_id = NULL represents a public/shared cache.
-- The unique index with COALESCE ensures tenant isolation while allowing NULL.
CREATE TABLE IF NOT EXISTS store_paths (
    cache_id UUID,
    domain_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001'::UUID,
    store_path_hash TEXT NOT NULL,
    store_path TEXT NOT NULL,
    nar_hash TEXT NOT NULL,
    nar_size BIGINT NOT NULL,
    manifest_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    committed_at TIMESTAMPTZ,
    visibility_state TEXT NOT NULL DEFAULT 'visible',
    uploader_token_id UUID,
    ca TEXT,
    -- Indicates all chunks were verified to exist in storage after successful upload.
    -- Used to prevent serving truncated NARs if metadata references missing chunks.
    chunks_verified BOOLEAN NOT NULL DEFAULT FALSE,
    FOREIGN KEY (domain_id, manifest_hash) REFERENCES manifests(domain_id, manifest_hash),
    -- Prevent sentinel UUID to avoid collision with NULL in COALESCE indexes
    CHECK (cache_id IS NULL OR cache_id != '00000000-0000-0000-0000-000000000000'::UUID)
);
-- Migration: Add chunks_verified column if table exists from older schema version.
ALTER TABLE store_paths ADD COLUMN IF NOT EXISTS chunks_verified BOOLEAN NOT NULL DEFAULT FALSE;
-- Migration: Add cache_id column if table exists from older schema version.
-- PostgreSQL 9.6+ supports ADD COLUMN IF NOT EXISTS.
ALTER TABLE store_paths ADD COLUMN IF NOT EXISTS cache_id UUID;
ALTER TABLE store_paths ADD COLUMN IF NOT EXISTS domain_id UUID;
CREATE INDEX IF NOT EXISTS idx_store_paths_manifest ON store_paths(domain_id, manifest_hash);
CREATE INDEX IF NOT EXISTS idx_store_paths_hash ON store_paths(store_path_hash);
-- Unique index for tenant isolation and upsert targeting.
-- COALESCE treats NULL as a sentinel UUID so ON CONFLICT works correctly.
-- This is the effective primary key for lookups.
CREATE UNIQUE INDEX IF NOT EXISTS idx_store_paths_cache_hash ON store_paths(
    COALESCE(cache_id, '00000000-0000-0000-0000-000000000000'::UUID), store_path_hash
);

-- F-003: Store path references (with cache_id for tenant isolation)
-- Note: No FK to store_paths because PostgreSQL doesn't support FK refs to UNIQUE INDEX with NULL.
-- Referential integrity is enforced at application level in delete_store_path().
CREATE TABLE IF NOT EXISTS store_path_references (
    cache_id UUID,
    store_path_hash TEXT NOT NULL,
    reference_hash TEXT NOT NULL,
    reference_type TEXT NOT NULL DEFAULT 'reference',
    -- Prevent sentinel UUID to avoid collision with NULL in COALESCE indexes
    CHECK (cache_id IS NULL OR cache_id != '00000000-0000-0000-0000-000000000000'::UUID)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_store_path_refs_pk ON store_path_references(
    COALESCE(cache_id, '00000000-0000-0000-0000-000000000000'::UUID), store_path_hash, reference_hash, reference_type
);
CREATE INDEX IF NOT EXISTS idx_store_path_refs_ref ON store_path_references(reference_hash);

-- Narinfo records (with cache_id for tenant isolation)
-- Note: No FK to store_paths - referential integrity enforced at application level.
CREATE TABLE IF NOT EXISTS narinfo_records (
    cache_id UUID,
    store_path_hash TEXT NOT NULL,
    narinfo_object_key TEXT NOT NULL,
    content_hash TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    -- Prevent sentinel UUID to avoid collision with NULL in COALESCE indexes
    CHECK (cache_id IS NULL OR cache_id != '00000000-0000-0000-0000-000000000000'::UUID)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_narinfo_records_pk ON narinfo_records(
    COALESCE(cache_id, '00000000-0000-0000-0000-000000000000'::UUID), store_path_hash
);

-- Signatures (with cache_id for tenant isolation)
-- Note: No FK to store_paths - referential integrity enforced at application level.
CREATE TABLE IF NOT EXISTS signatures (
    cache_id UUID,
    store_path_hash TEXT NOT NULL,
    key_id UUID NOT NULL,
    signature TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    -- Prevent sentinel UUID to avoid collision with NULL in COALESCE indexes
    CHECK (cache_id IS NULL OR cache_id != '00000000-0000-0000-0000-000000000000'::UUID)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_signatures_pk ON signatures(
    COALESCE(cache_id, '00000000-0000-0000-0000-000000000000'::UUID), store_path_hash, key_id
);

-- Tokens
CREATE TABLE IF NOT EXISTS tokens (
    token_id UUID PRIMARY KEY,
    cache_id UUID,
    token_hash TEXT NOT NULL UNIQUE,
    scopes TEXT NOT NULL,
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    last_used_at TIMESTAMPTZ,
    description TEXT
);
CREATE INDEX IF NOT EXISTS idx_tokens_hash ON tokens(token_hash);

-- Bootstrap marker
CREATE TABLE IF NOT EXISTS bootstrap_state (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    bootstrap_token_id UUID
);
INSERT INTO bootstrap_state (id, bootstrap_token_id)
VALUES (1, NULL)
ON CONFLICT (id) DO NOTHING;

-- Signing keys
CREATE TABLE IF NOT EXISTS signing_keys (
    key_id UUID PRIMARY KEY,
    cache_id UUID,
    key_name TEXT NOT NULL UNIQUE,
    public_key TEXT NOT NULL,
    private_key_ref TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL,
    rotated_at TIMESTAMPTZ
);

-- GC jobs
CREATE TABLE IF NOT EXISTS gc_jobs (
    gc_job_id UUID PRIMARY KEY,
    cache_id UUID,
    job_type TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'queued',
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    stats_json TEXT,
    sweep_checkpoint_json TEXT,
    -- Prevent sentinel UUID to avoid collision with NULL in COALESCE indexes
    CHECK (cache_id IS NULL OR cache_id != '00000000-0000-0000-0000-000000000000'::UUID)
);
CREATE INDEX IF NOT EXISTS idx_gc_jobs_state ON gc_jobs(state);

-- Drop old partial index (only covered 'running' state)
DROP INDEX IF EXISTS idx_gc_jobs_cache_type_running;

-- Unique index to prevent concurrent GC jobs of same type for same cache
-- COALESCE handles NULL cache_id for global caches (maps to all-zeros UUID)
-- Extended to cover both 'queued' and 'running' states (fixes race condition)
-- CRITICAL FIX: This index is SUFFICIENT for concurrency control. It allows
-- different job types to run concurrently (e.g., chunk_gc + manifest_gc), which
-- improves GC efficiency without risking data corruption (different job types
-- operate on independent data).
CREATE UNIQUE INDEX IF NOT EXISTS idx_gc_jobs_cache_type_active
ON gc_jobs(COALESCE(cache_id, '00000000-0000-0000-0000-000000000000'::UUID), job_type)
WHERE state IN ('queued', 'running');

-- REMOVED: Overly restrictive EXCLUDE constraint that blocked ALL job types per cache.
-- The UNIQUE INDEX above is sufficient - it prevents duplicate jobs of the same type
-- while allowing different types to run concurrently (e.g., upload_gc + chunk_gc).
--
-- Previous constraint (now removed for P2 fix):
-- ALTER TABLE gc_jobs ADD CONSTRAINT gc_jobs_one_per_cache
-- EXCLUDE USING gist (COALESCE(cache_id, ...) WITH =) WHERE (state IN ('queued', 'running'));
--
-- Drop the constraint if it exists (for migration from old schema)
ALTER TABLE gc_jobs DROP CONSTRAINT IF EXISTS gc_jobs_one_per_cache;

-- F-002: Trusted builder keys
CREATE TABLE IF NOT EXISTS trusted_builder_keys (
    trusted_key_id UUID PRIMARY KEY,
    cache_id UUID,
    key_name TEXT NOT NULL,
    public_key TEXT NOT NULL,
    trust_level TEXT NOT NULL DEFAULT 'trusted',
    added_by_token_id UUID,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    UNIQUE(cache_id, key_name)
);
CREATE INDEX IF NOT EXISTS idx_trusted_keys_cache ON trusted_builder_keys(cache_id);
CREATE INDEX IF NOT EXISTS idx_trusted_keys_name ON trusted_builder_keys(key_name);

-- F-005: Tombstones
CREATE TABLE IF NOT EXISTS tombstones (
    tombstone_id UUID PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    cache_id UUID,
    deleted_at TIMESTAMPTZ NOT NULL,
    deleted_by_token_id UUID,
    reason TEXT,
    gc_eligible_at TIMESTAMPTZ NOT NULL,
    gc_completed_at TIMESTAMPTZ,
    UNIQUE(entity_type, entity_id)
);
CREATE INDEX IF NOT EXISTS idx_tombstones_gc ON tombstones(gc_eligible_at) WHERE gc_completed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_tombstones_entity ON tombstones(entity_type, entity_id);
