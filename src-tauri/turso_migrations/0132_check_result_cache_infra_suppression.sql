-- Migration: bound the re-execution of an infrastructure-failing check (CAIRN-3245)
--
-- Shared across the private and team lineages. An infrastructure failure is not
-- a verdict: `get_check_result` reuses only rows with `passed = 1 AND
-- failure_kind IS NULL`, so an infra-failing (project, check, input_hash) triple
-- re-executes on EVERY subsequent evaluation, forever. These two columns are the
-- kill switch's memory.
--
-- `infra_failure_streak` counts CONSECUTIVE infrastructure failures at this
-- triple. It increments on each fresh infrastructure failure, resets to 0 on any
-- genuine verdict (a pass or an ordinary/timeout/killed red), and is untouched by
-- a cache-hit re-stamp. At `INFRA_FAILURE_BOUND` the triple stops being executed.
--
-- `infra_escalated_at` is the epoch-second stamp of the ONE operator escalation
-- the triple is allowed. It is claimed by a conditional UPDATE gated on IS NULL,
-- which is what makes exactly-once a durable database fact rather than an
-- invariant inferred from the counter, and it is cleared alongside a reset.
--
-- Plain ADD COLUMNs keep this sync-safe; do not rebuild synced replica tables
-- (see TEAM_TAIL in migrations.rs).

ALTER TABLE check_result_cache ADD COLUMN infra_failure_streak INTEGER NOT NULL DEFAULT 0;
ALTER TABLE check_result_cache ADD COLUMN infra_escalated_at INTEGER;
