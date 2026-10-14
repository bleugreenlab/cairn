-- The durable REPL transcript: one row per send. Inserted `pending` when the send
-- is submitted and updated in place when it settles, so a send in flight is
-- visible rather than invisible until it lands.
--
-- `seq` stays monotonic across generations so the transcript reads as one
-- continuous history punctuated by where the session restarted. stdout/stderr are
-- already bounded by the host's output cap before they reach a row.
CREATE TABLE IF NOT EXISTS "repl_exchanges" (
    id TEXT PRIMARY KEY NOT NULL,
    repl_id TEXT NOT NULL,
    project_id TEXT,
    generation INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    origin TEXT NOT NULL,
    code TEXT NOT NULL,
    status TEXT NOT NULL,
    value TEXT,
    stdout TEXT,
    stderr TEXT,
    error TEXT,
    note TEXT,
    duration_ms INTEGER,
    truncated INTEGER NOT NULL DEFAULT 0,
    started_at INTEGER NOT NULL,
    FOREIGN KEY (repl_id) REFERENCES job_repls(id) ON DELETE CASCADE,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX idx_repl_exchanges_repl_seq ON repl_exchanges(repl_id, seq);

CREATE INDEX idx_repl_exchanges_project_id ON repl_exchanges(project_id);
