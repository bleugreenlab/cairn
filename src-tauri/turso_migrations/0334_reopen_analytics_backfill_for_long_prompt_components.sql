-- lineage: private
-- cairn/5218: reopen the one-time historical analytics backfill so its fold
-- re-derives every run with the long-prompt components that the shared
-- migration 0333_token_rollup_long_prompt_components added (it also cleared the
-- per-run watermarks).
--
-- Private-only: analytics_rollup_backfill_state exists solely in the private
-- lineage, as with 0088_reopen_analytics_backfill.

UPDATE analytics_rollup_backfill_state SET backfilled_at = NULL WHERE id = 1;
