-- CAIRN-2487: durable journal for the workflow harness's agent() calls.
--
-- A workflow node's script drives ephemeral agent() calls straight-line. Each
-- call's validated result is memoized here, keyed by (run_id, ordinal) where
-- run_id is the workflow run and ordinal is the harness's monotonic dispatch
-- counter. prompt_hash guards the key: on host-restart replay the supervisor
-- re-runs the script from the top, and at each ordinal a matching prompt_hash
-- returns the journaled result instantly (a cache hit, spawning no new call)
-- while a mismatch is a cache miss that runs the call live and overwrites the
-- row. result_json is the validated result JSON, or NULL for a journaled
-- failure (status='failure'); either way replay never re-spawns a
-- known-resolved call.
--
-- This is a per-machine replay cache: the supervisor re-runs a crashed workflow
-- on the SAME host, and cross-node workflow resume is out of scope. So the
-- journal is runner-transient Private state (like analytics_rollup_backfill_
-- state), classified Private(RunnerTransient) in TABLE_SCOPES and living ONLY in
-- the private lineage's tail -- never synced to a team replica. Losing it (e.g.
-- a team run that moves machines) degrades gracefully to cache-miss re-runs,
-- never to corruption.
--
-- The UNIQUE(run_id, ordinal) constraint's implicit index already serves the
-- (run_id, ordinal) point lookup, so no separate index is created.
CREATE TABLE workflow_journal (
    id          TEXT PRIMARY KEY,
    run_id      TEXT NOT NULL,
    ordinal     INTEGER NOT NULL,
    prompt_hash TEXT NOT NULL,
    result_json TEXT,
    status      TEXT NOT NULL CHECK(status IN ('success', 'failure')),
    created_at  INTEGER NOT NULL,
    UNIQUE(run_id, ordinal)
);
