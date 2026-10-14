-- The durable identity of a REPL. One row per (job, slug), spanning every
-- interpreter process that has served it: the namespace dies with a process, but
-- the REPL itself -- its language, its dependency set, its transcript -- survives
-- so it can be reviewed and resumed. `generation` names which process incarnation
-- is current; a resume bumps it and continues the same transcript.
--
-- `bindings` is the namespace snapshot taken at the end of the last eval (python
-- only). A namespace can only change as the result of a send, so the snapshot is
-- the live namespace while the process runs, and the last known one after it dies.
CREATE TABLE IF NOT EXISTS "job_repls" (
    id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL,
    project_id TEXT,
    slug TEXT NOT NULL,
    interpreter TEXT NOT NULL,
    deps TEXT,
    generation INTEGER NOT NULL,
    status TEXT NOT NULL,
    exit_reason TEXT,
    bindings TEXT,
    created_at INTEGER NOT NULL,
    exited_at INTEGER,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
);

CREATE INDEX idx_job_repls_job_id ON job_repls(job_id);

CREATE INDEX idx_job_repls_project_id ON job_repls(project_id);

-- Unconditional: a slug is one logical REPL for the life of the job, running or
-- exited. That is what lets create resume rather than duplicate.
CREATE UNIQUE INDEX idx_job_repls_job_slug ON job_repls(job_id, slug);
