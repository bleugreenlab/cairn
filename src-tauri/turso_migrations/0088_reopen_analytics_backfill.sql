-- CAIRN-2270: reopen the one-time historical analytics-rollup backfill so it
-- re-folds every run into the hour-grain token_rollup that the shared migration
-- 0087 recreated empty.
--
-- Private-only: analytics_rollup_backfill_state is a runner-transient table that
-- exists solely in the private lineage (migration 0086), so this reset cannot
-- live in the shared 0087 re-grain -- it would fail with "no such table" on a
-- team replica. Setting the marker back to NULL makes analytics::
-- run_historical_backfill re-run on next startup; with token_rollup_runs cleared
-- by 0087, that pass re-folds ALL runs at the hour grain.

UPDATE analytics_rollup_backfill_state SET backfilled_at = NULL WHERE id = 1;
