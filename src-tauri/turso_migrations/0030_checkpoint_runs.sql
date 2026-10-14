-- Durable history of programmatic command-checkpoint runs.
--
-- One row per execution of a checkpoint node's command (pass or fail). This is
-- the source of truth for the auto-fix loop: attempt count (row count),
-- last-run commit SHA (latest row), and the failure output used to wake the
-- upstream agent. It survives the checkpoint-artifact deletion that re-arming
-- performs, which is why this data lives here and not on the artifact.
CREATE TABLE checkpoint_runs (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL,
    command TEXT,
    commit_sha TEXT,
    exit_code INTEGER NOT NULL,
    passed INTEGER NOT NULL,
    stdout_tail TEXT,
    stderr_tail TEXT,
    ran_at INTEGER NOT NULL
);

CREATE INDEX idx_checkpoint_runs_job ON checkpoint_runs(job_id, ran_at);
