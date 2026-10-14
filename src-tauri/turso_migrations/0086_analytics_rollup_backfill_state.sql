-- One-time historical analytics-rollup backfill marker.
--
-- The token/cost (Tier A) and tool-invocation (Tier B) rollups are now
-- maintained INCREMENTALLY as events are inserted
-- (analytics::queries::maintain_rollups_on_insert, hooked into the event-insert
-- transaction), so the analytics page never folds/backfills on read. Events
-- that predate that per-event seam were never folded, so a one-time historical
-- backfill (analytics::run_historical_backfill) populates both rollups for them
-- on first startup. This single-row table records that the one-time pass has run
-- so it never re-scans the whole events table again.
--
-- Private / runner-transient: it tracks a per-install maintenance pass over the
-- local database, not durable collaboration data, so it never syncs to a team
-- replica (mirrors archival_backfill_state).

CREATE TABLE IF NOT EXISTS analytics_rollup_backfill_state (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    backfilled_at INTEGER  -- epoch seconds the one-time backfill completed; NULL until then
);

INSERT OR IGNORE INTO analytics_rollup_backfill_state (id, backfilled_at) VALUES (1, NULL);
