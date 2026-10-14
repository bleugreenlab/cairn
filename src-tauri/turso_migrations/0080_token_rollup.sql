-- Token/cost rollup powering Tier A (economics) analytics.
--
-- One row per (UTC-day x run x resolved dimensions), pre-aggregating the
-- per-event token/cost data the five Tier A read paths used to recompute live
-- over the ~780k-row `events` table. Populated by an idempotent, watermark-gated
-- incremental fold (analytics::queries::fold_token_rollup), mirroring the
-- tool-invocation rollup (migration 0067). Dimensions are resolved with the same
-- run -> runs/sessions/jobs joins the live token CTE used.
--
-- Token measures sum BILLABLE top-level events only (gated on the backend-shaped
-- billable predicate); `token_event_count` is the count of those folded events,
-- so a row with token_event_count = 0 is cost-only and is excluded wherever a
-- read derives from billable tokens. `exact_cost`/`exact_cost_count` range over
-- any event carrying a real `cost_usd` (a broader population -- the metered
-- settlement event is not itself a billable token event).

CREATE TABLE IF NOT EXISTS token_rollup (
    id                  TEXT PRIMARY KEY,        -- day:run:session:model:backend:agent_config
    day                 INTEGER NOT NULL,        -- UTC day floor of the event (epoch seconds)
    project_id          TEXT,                    -- COALESCE(runs.project_id, jobs.project_id)
    run_id              TEXT NOT NULL,
    session_id          TEXT,                    -- the event's session_id
    job_id              TEXT,                    -- COALESCE(runs.job_id, sessions.job_id)
    node_name           TEXT,                    -- jobs.node_name
    model               TEXT,                    -- jobs.model
    agent_config_id     TEXT,                    -- jobs.agent_config_id
    backend             TEXT NOT NULL,           -- LOWER(COALESCE(runs.backend, sessions.backend, 'claude'))
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

CREATE INDEX IF NOT EXISTS idx_token_rollup_day ON token_rollup(day);
CREATE INDEX IF NOT EXISTS idx_token_rollup_project_day ON token_rollup(project_id, day);
CREATE INDEX IF NOT EXISTS idx_token_rollup_run ON token_rollup(run_id);

-- Per-run high-water mark so the incremental fold skips runs with no new
-- relevant events instead of rescanning every run each pass.
CREATE TABLE IF NOT EXISTS token_rollup_runs (
    run_id            TEXT PRIMARY KEY,
    processed_through INTEGER NOT NULL,          -- max relevant event created_at folded for this run
    updated_at        INTEGER NOT NULL
);
