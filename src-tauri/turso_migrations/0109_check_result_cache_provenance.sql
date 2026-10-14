-- Add executor attribution and runner-local toolchain claims to cached check verdicts.
-- Shared by private and team lineages. Plain additive changes keep legacy rows valid
-- and avoid rebuilding the synced check_result_cache table.

ALTER TABLE check_result_cache ADD COLUMN executor_id TEXT;
ALTER TABLE check_result_cache ADD COLUMN executor_device_id TEXT;
ALTER TABLE check_result_cache ADD COLUMN executor_connection_generation INTEGER;
ALTER TABLE check_result_cache ADD COLUMN executor_slot_id TEXT;
ALTER TABLE check_result_cache ADD COLUMN executor_lease_epoch INTEGER;
ALTER TABLE check_result_cache ADD COLUMN executor_started_at_unix_ms INTEGER;
ALTER TABLE check_result_cache ADD COLUMN executor_finished_at_unix_ms INTEGER;
ALTER TABLE check_result_cache ADD COLUMN toolchain_fingerprint TEXT;

CREATE INDEX idx_check_result_cache_executor_generation
    ON check_result_cache(project_id, executor_id, executor_connection_generation);
