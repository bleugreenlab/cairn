-- lineage: private
-- cairn/7286: the historical backfill's reported-model pass records the last
-- run it finished, so a restart before the backfill completes resumes after it
-- instead of re-reading (and re-reconstructing, for archived events) every
-- event it already examined. NULL means start from the first run; marking the
-- backfill complete clears it, so a later reopen starts the pass over.
--
-- Private-only: analytics_rollup_backfill_state exists solely in the private
-- lineage.

ALTER TABLE analytics_rollup_backfill_state ADD COLUMN reported_models_through TEXT;
