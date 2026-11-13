-- lineage: private
-- cairn/7127: reopen the one-time historical analytics backfill so it fills
-- events.reported_model for the events of runs whose job selected a bare
-- Claude alias (it already covered jobs that selected no model), and its fold
-- re-derives every run keyed by the reported generation
-- (token_rollup_reported_generation_keys cleared the per-run watermarks).
--
-- Private-only: analytics_rollup_backfill_state exists solely in the private
-- lineage, as with 0347_reopen_analytics_backfill_for_model_keys.

UPDATE analytics_rollup_backfill_state SET backfilled_at = NULL WHERE id = 1;
