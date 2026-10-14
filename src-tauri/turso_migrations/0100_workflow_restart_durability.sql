-- CAIRN-2498: workflow restart durability — journaled replay + re-dispatch.
--
-- Two per-machine runner-transient tables that make a crashed workflow's
-- script replayable on the SAME host. Both are Private(RunnerTransient) in
-- TABLE_SCOPES (like workflow_journal, migration 0099): never synced to a team
-- replica, and losing them degrades gracefully to cache-miss re-runs, never to
-- corruption.
--
-- workflow_run records the durable spawn parameters for each workflow run so
-- startup re-dispatch can re-spawn `bun <script>` for the SAME run_id (the
-- journal is keyed by that run_id). Without a durable home the script path and
-- args are lost on crash — the run row alone cannot reconstruct the process.
-- The row is inserted when the workflow run is prepared and deleted when the
-- run reaches a terminal outcome; re-dispatch only reconstructs runs whose
-- latest run status is still non-terminal (live / starting / crashed).
CREATE TABLE workflow_run (
    run_id      TEXT PRIMARY KEY,
    job_id      TEXT NOT NULL,
    session_id  TEXT,
    workflow_id TEXT NOT NULL,
    script_path TEXT NOT NULL,
    args_json   TEXT NOT NULL,
    working_dir TEXT NOT NULL,
    output_name TEXT NOT NULL,
    node_path   TEXT,
    created_at  INTEGER NOT NULL
);

-- workflow_call links an in-flight ephemeral call run back to the journal key it
-- must write on completion: the parent workflow run and the harness dispatch
-- ordinal, plus the prompt_hash guard. It is written when a workflow-parented
-- call is spawned live (a journal cache miss) and consumed — read once, then
-- deleted — when that call's run finalizes, at which point the validated result
-- (or a failure) is stored into workflow_journal keyed by (workflow_run_id,
-- ordinal). Keyed by the call's own run_id, so the completion path is a point
-- lookup. A lost row (host crash mid-call) simply means the call re-runs on
-- replay rather than replaying from the journal — the graceful-degradation
-- contract the journal is designed around.
CREATE TABLE workflow_call (
    run_id          TEXT PRIMARY KEY,
    workflow_run_id TEXT NOT NULL,
    ordinal         INTEGER NOT NULL,
    prompt_hash     TEXT NOT NULL,
    created_at      INTEGER NOT NULL
);
