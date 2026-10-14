-- Migration: classify a failing check's terminal outcome (CAIRN-2623)
--
-- Shared across the private and team lineages. `failure_kind` REFINES the binary
-- `passed` verdict for a FAILING check so a slow-suite timeout renders as a
-- timeout, a spawn failure names itself, and a signal kill is distinguishable
-- from an ordinary non-zero exit -- instead of every abnormal death collapsing
-- into an opaque `exit -1`. Values written going forward: 'timed_out' (killed
-- at its timeout budget), 'spawn_error' (the process could not be spawned), or
-- 'killed' (died by signal mid-run). NULL for a pass, for an ordinary non-zero
-- exit failure, and for every legacy row (consumers fall back to passed/exit
-- code there). A plain ADD COLUMN keeps this sync-safe; do not rebuild synced
-- replica tables (see TEAM_TAIL in migrations.rs).

ALTER TABLE check_result_cache ADD COLUMN failure_kind TEXT;
