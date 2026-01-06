-- PostgreSQL initialization script for Cellar
-- This script runs automatically when the postgres container starts

-- F-001: Caches table
CREATE TABLE IF NOT EXISTS caches (
    cache_id UUID PRIMARY KEY,
    cache_name TEXT NOT NULL UNIQUE,
    public_base_url TEXT,
    is_public BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_caches_name ON caches(cache_name);

-- Upload sessions (with F-004 error tracking)
CREATE TABLE IF NOT EXISTS upload_sessions (
    upload_id UUID PRIMARY KEY,
    cache_id UUID,
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
    error_detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_upload_sessions_state ON upload_sessions(state, expires_at);
CREATE INDEX IF NOT EXISTS idx_upload_sessions_store_path ON upload_sessions(store_path_hash);

-- Upload expected chunks
CREATE TABLE IF NOT EXISTS upload_expected_chunks (
    upload_id UUID NOT NULL,
    position INTEGER NOT NULL,
    chunk_hash TEXT NOT NULL,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    received_at TIMESTAMPTZ,
    PRIMARY KEY (upload_id, position),
    FOREIGN KEY (upload_id) REFERENCES upload_sessions(upload_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_upload_chunks_received ON upload_expected_chunks(upload_id, received_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_upload_chunks_hash ON upload_expected_chunks(upload_id, chunk_hash);

-- Chunks
CREATE TABLE IF NOT EXISTS chunks (
    chunk_hash TEXT PRIMARY KEY,
    size_bytes BIGINT NOT NULL,
    object_key TEXT,
    refcount INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    last_accessed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_chunks_refcount ON chunks(refcount, created_at);

-- Manifests
CREATE TABLE IF NOT EXISTS manifests (
    manifest_hash TEXT PRIMARY KEY,
    chunk_size BIGINT NOT NULL,
    chunk_count INTEGER NOT NULL,
    nar_size BIGINT NOT NULL,
    object_key TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

-- Manifest chunks
CREATE TABLE IF NOT EXISTS manifest_chunks (
    manifest_hash TEXT NOT NULL,
    position INTEGER NOT NULL,
    chunk_hash TEXT NOT NULL,
    PRIMARY KEY (manifest_hash, position),
    FOREIGN KEY (manifest_hash) REFERENCES manifests(manifest_hash) ON DELETE CASCADE,
    FOREIGN KEY (chunk_hash) REFERENCES chunks(chunk_hash)
);
CREATE INDEX IF NOT EXISTS idx_manifest_chunks_chunk ON manifest_chunks(chunk_hash);

-- Store paths (with F-003 CA field)
CREATE TABLE IF NOT EXISTS store_paths (
    store_path_hash TEXT PRIMARY KEY,
    store_path TEXT NOT NULL UNIQUE,
    nar_hash TEXT NOT NULL,
    nar_size BIGINT NOT NULL,
    manifest_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    committed_at TIMESTAMPTZ,
    visibility_state TEXT NOT NULL DEFAULT 'visible',
    uploader_token_id UUID,
    ca TEXT,
    FOREIGN KEY (manifest_hash) REFERENCES manifests(manifest_hash)
);
CREATE INDEX IF NOT EXISTS idx_store_paths_manifest ON store_paths(manifest_hash);

-- F-003: Store path references
CREATE TABLE IF NOT EXISTS store_path_references (
    store_path_hash TEXT NOT NULL,
    reference_hash TEXT NOT NULL,
    reference_type TEXT NOT NULL DEFAULT 'reference',
    PRIMARY KEY (store_path_hash, reference_hash, reference_type),
    FOREIGN KEY (store_path_hash) REFERENCES store_paths(store_path_hash) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_store_path_refs_ref ON store_path_references(reference_hash);

-- Narinfo records
CREATE TABLE IF NOT EXISTS narinfo_records (
    store_path_hash TEXT PRIMARY KEY,
    narinfo_object_key TEXT NOT NULL,
    content_hash TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (store_path_hash) REFERENCES store_paths(store_path_hash) ON DELETE CASCADE
);

-- Signatures
CREATE TABLE IF NOT EXISTS signatures (
    store_path_hash TEXT NOT NULL,
    key_id UUID NOT NULL,
    signature TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (store_path_hash, key_id),
    FOREIGN KEY (store_path_hash) REFERENCES store_paths(store_path_hash) ON DELETE CASCADE
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
    stats_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_gc_jobs_state ON gc_jobs(state);

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

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO cellar;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO cellar;

-- Log completion
DO $$
BEGIN
    RAISE NOTICE 'Cellar database schema initialized successfully';
END $$;
