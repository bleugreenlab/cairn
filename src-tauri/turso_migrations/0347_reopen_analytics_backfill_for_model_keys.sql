-- lineage: private
-- cairn/5979: reopen the one-time historical analytics backfill so it fills
-- events.reported_model for model-less jobs and its fold re-derives every run
-- under the canonical model key (0346_event_reported_model added the column
-- and cleared the per-run watermarks).
--
-- Private-only: analytics_rollup_backfill_state exists solely in the private
-- lineage, as with 0334_reopen_analytics_backfill_for_long_prompt_components.

UPDATE analytics_rollup_backfill_state SET backfilled_at = NULL WHERE id = 1;
