-- lineage: private
-- cairn/6259: terminal output retained outside disposable scratch.

-- One row per terminal capture. The bytes live in this runner's
-- `<cairn_home>/terminal-output/<id>.log`, which is why the table is private:
-- a team replica has no copy of the file a row would name.
--
-- `terminal_id` is the `job_terminals` row capturing into this record now. A
-- recovery restart re-points it at the replacement row; a same-slug create
-- mints a new record and leaves this one naming a row that no longer exists.
-- `terminal_uri` is the canonical terminal URI the capture was created under:
-- a retained read must name exactly this owner.
--
-- `capture` is `capturing` while the terminal runs, then `complete`,
-- `incomplete` with `capture_note` saying why, or `unreported` when the
-- terminal's executor does not report the end of its output, so whether the
-- capture is whole is unknown. `dropped_bytes` totals what
-- compaction dropped. `expires_at` is seven days after the later of exit and
-- resolution (exit alone for `expiry_basis = 'exit'`, project and thread
-- terminals), NULL while that is not yet known, and never moves earlier.
CREATE TABLE terminal_outputs (
    id TEXT PRIMARY KEY NOT NULL,
    terminal_id TEXT,
    job_id TEXT,
    project_id TEXT NOT NULL,
    slug TEXT NOT NULL,
    terminal_uri TEXT NOT NULL,
    expiry_basis TEXT NOT NULL CHECK (expiry_basis IN ('resolution', 'exit')),
    capture TEXT NOT NULL DEFAULT 'capturing'
        CHECK (capture IN ('capturing', 'complete', 'incomplete', 'unreported')),
    capture_note TEXT,
    dropped_bytes INTEGER NOT NULL DEFAULT 0,
    bytes INTEGER NOT NULL DEFAULT 0,
    exit_code INTEGER,
    exit_reason TEXT,
    exited_at INTEGER,
    resolved_at INTEGER,
    expires_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX idx_terminal_outputs_terminal ON terminal_outputs(terminal_id);
CREATE INDEX idx_terminal_outputs_job ON terminal_outputs(job_id);
CREATE INDEX idx_terminal_outputs_project ON terminal_outputs(project_id);
CREATE INDEX idx_terminal_outputs_uri ON terminal_outputs(terminal_uri, created_at);
CREATE INDEX idx_terminal_outputs_expires ON terminal_outputs(expires_at, id);
