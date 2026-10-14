-- CAIRN-2499: durable phase/log progress timeline for a workflow node.
--
-- The workflow harness's phase(name) / log(msg) verbs append typed entries here,
-- keyed by the workflow node's job_id with a monotonic per-job seq. This is the
-- durable reader the workflow monitoring panel renders as a phase spine plus a
-- log stream. Progress is typed execution state, NOT transcript events, so it
-- lives in its own table rather than in the events stream -- storing it there
-- would pollute both chat reconstruction and the analytics token/tool extraction
-- that scan events.
--
-- This is a project-scoped SHARED table (present in BOTH the private and team
-- lineages, so team_schema_matches_private holds). It lives alongside the
-- jobs/runs the monitor joins it against: the progress resource routes writes
-- through for_project(project) and the monitor query reads through
-- owning_db_for_job, so both land in whichever database owns the workflow node
-- (the team replica for a team project, else local), and a team workflow's
-- progress syncs so teammates see the panel. A private-only classification would
-- silently break team workflows -- for_project would target a team replica that
-- did not carry the table, and every phase()/log() write would no-op.
--
-- The UNIQUE(job_id, seq) constraint's implicit index already serves the
-- (job_id, seq) ordered scan the panel query and read renderer perform, so no
-- separate index is created.
CREATE TABLE workflow_progress (
    id          TEXT PRIMARY KEY,        -- job_id ':' seq
    job_id      TEXT NOT NULL,           -- the workflow node's job
    seq         INTEGER NOT NULL,        -- monotonic per job (append order)
    kind        TEXT NOT NULL CHECK(kind IN ('phase', 'log')),
    text        TEXT NOT NULL,           -- phase name, or log message
    created_at  INTEGER NOT NULL,
    UNIQUE(job_id, seq)
);
