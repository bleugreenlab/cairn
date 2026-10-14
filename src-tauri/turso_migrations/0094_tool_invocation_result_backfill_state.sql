-- Track the one-time reconciliation that fills historical tool_invocations.result_ts.
--
-- Migration 0092 added result_ts and reset the per-run tool backfill watermarks,
-- but installs that had already completed analytics_rollup_backfill_state could
-- skip startup backfill entirely. This private marker lets startup run a targeted
-- repair pass once without reopening the full token/tool historical backfill.

ALTER TABLE analytics_rollup_backfill_state ADD COLUMN tool_results_backfilled_at INTEGER;
