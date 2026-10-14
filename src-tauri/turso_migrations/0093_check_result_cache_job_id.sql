-- Migration: durable per-job project-check listing index (CAIRN-2368)
--
-- Shared across private and team lineages. `job_id` is a listing index only: the
-- cache identity remains `(project_id, check_name, input_hash)`, with `tree_hash`
-- as the current sealed-tree pointer. `cached` records whether the latest
-- evaluation reused a prior verdict, which cannot be reconstructed from `ran_at`
-- because cache-hit re-stamps update it. Plain ADD COLUMNs keep this sync-safe;
-- do not rebuild synced replica tables (see TEAM_TAIL in migrations.rs).

ALTER TABLE check_result_cache ADD COLUMN job_id TEXT;
ALTER TABLE check_result_cache ADD COLUMN cached INTEGER;
CREATE INDEX idx_check_result_cache_job ON check_result_cache(job_id);
