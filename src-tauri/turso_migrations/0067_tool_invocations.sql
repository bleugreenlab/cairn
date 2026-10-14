-- Tool-invocation rollup powering Tier B (target/error) analytics.
--
-- One row per agent tool use, extracted from reconstructed events. `is_error`
-- is linked from the matching `tool_result` event. Populated by an idempotent
-- incremental backfill (analytics::backfill_tool_invocations); rows are upserted
-- by `id` = "{event_id}:{tool_use_id}". Project/role/model are derived at query
-- time by joining run_id -> runs -> jobs, mirroring the Tier A token CTE.

CREATE TABLE IF NOT EXISTS tool_invocations (
    id              TEXT PRIMARY KEY,         -- {event_id}:{tool_use_id}
    event_id        TEXT NOT NULL,            -- assistant event carrying the tool use
    tool_use_id     TEXT,                     -- provider tool-use id (links the result)
    run_id          TEXT NOT NULL,
    ts              INTEGER NOT NULL,         -- source event created_at (epoch seconds)
    verb            TEXT NOT NULL,            -- read | write | run | other
    tool_name       TEXT NOT NULL,            -- normalized base tool name
    target_scheme   TEXT NOT NULL,            -- file | cairn | http | shell | other
    target_kind     TEXT NOT NULL,            -- extension | resource-kind | host | command
    target_path     TEXT,                     -- representative target (truncated)
    is_error        INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_tool_invocations_run ON tool_invocations(run_id);
CREATE INDEX IF NOT EXISTS idx_tool_invocations_ts ON tool_invocations(ts);
CREATE INDEX IF NOT EXISTS idx_tool_invocations_shape
    ON tool_invocations(verb, target_scheme, target_kind);

-- Per-run high-water mark so the incremental backfill skips runs with no new
-- events instead of rescanning every run each pass.
CREATE TABLE IF NOT EXISTS tool_invocation_runs (
    run_id            TEXT PRIMARY KEY,
    processed_through INTEGER NOT NULL,       -- max event created_at processed for this run
    updated_at        INTEGER NOT NULL
);

-- Speeds the backfill's per-run source scan over assistant/tool_result events.
CREATE INDEX IF NOT EXISTS idx_events_type_run ON events(event_type, run_id);
