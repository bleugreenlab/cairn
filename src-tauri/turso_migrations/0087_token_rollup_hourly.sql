-- CAIRN-2270: re-grain token_rollup from the UTC-day floor to the UTC-hour
-- floor, so ONE derived cache answers every analytics time query: day/week/month
-- floor an already-hour-floored epoch (exact, since an hour nests wholly inside
-- its day/week/month), the 24h window reads the rollup directly, and the usage
-- heatmap sums billable tokens per local hour. Supersedes the day-grain rollup
-- migration 0080 built.
--
-- token_rollup is a DERIVED cache with no inbound foreign keys, so a plain
-- drop-and-recreate is clean: no rebuild_fk_off, no copy-forward. Dropping the
-- table guarantees zero day-grain rows survive to mix with the hour-grain rows
-- the startup backfill re-derives. The only schema change is the `day` column
-- (UTC-day floor) becoming `bucket_start` (UTC-hour floor) and the indexes that
-- named it.
--
-- SHARED migration: token_rollup is a project-scoped table present in BOTH the
-- private and team lineages (the team_schema_matches_private test enforces
-- byte-identical schema), so the re-grain is written once here in the SHARED_TAIL
-- and reaches both. The private-only backfill-marker reset that forces the
-- historical fold to re-derive every run lives in the sibling private migration
-- 0088_reopen_analytics_backfill.

DROP TABLE IF EXISTS token_rollup;

CREATE TABLE token_rollup (
    id                  TEXT PRIMARY KEY,        -- hour:run:session:model:backend:agent_config
    bucket_start        INTEGER NOT NULL,        -- UTC hour floor of the event (epoch seconds)
    project_id          TEXT,                    -- COALESCE(runs.project_id, jobs.project_id)
    run_id              TEXT NOT NULL,
    session_id          TEXT,                    -- the event's session_id
    job_id              TEXT,                    -- COALESCE(runs.job_id, sessions.job_id)
    node_name           TEXT,                    -- jobs.node_name
    model               TEXT,                    -- jobs.model
    agent_config_id     TEXT,                    -- jobs.agent_config_id
    backend             TEXT NOT NULL,           -- LOWER(COALESCE(sessions.backend, 'claude'))
    -- Token sums over billable top-level events only.
    input_tokens        INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens   INTEGER NOT NULL DEFAULT 0,
    cache_create_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens       INTEGER NOT NULL DEFAULT 0,
    thinking_tokens     INTEGER NOT NULL DEFAULT 0,
    billable_tokens     INTEGER NOT NULL DEFAULT 0,
    token_event_count   INTEGER NOT NULL DEFAULT 0, -- billable events folded (0 => cost-only row)
    -- Real metered cost over any event with cost_usd (broader than billable).
    exact_cost          REAL NOT NULL DEFAULT 0,
    exact_cost_count    INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_token_rollup_bucket_start ON token_rollup(bucket_start);
CREATE INDEX idx_token_rollup_project_bucket_start ON token_rollup(project_id, bucket_start);
CREATE INDEX idx_token_rollup_run ON token_rollup(run_id);

-- Clear every per-run watermark so the fold re-derives ALL runs at the new grain
-- instead of skipping them as already-processed. Harmless on the team lineage
-- (nothing folds there); keeps "empty table + cleared watermarks" together
-- wherever the schema migration runs.
DELETE FROM token_rollup_runs;
