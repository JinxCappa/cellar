-- PostgreSQL initialization script for Cellar
-- This script runs automatically when the postgres container starts
--
-- NOTE: The actual schema is managed by the server's migrate() function in
-- postgres_schema.sql to ensure a single source of truth and avoid schema drift.
--
-- No explicit privilege grants are needed here because:
-- - POSTGRES_USER=cellar makes cellar the database owner
-- - As owner, cellar already has full privileges on all objects it creates
-- - The migrate() function runs as the cellar user, so all tables are owned by cellar

-- Log completion
DO $$
BEGIN
    RAISE NOTICE 'Cellar database initialized - schema will be created by server migration';
END $$;
