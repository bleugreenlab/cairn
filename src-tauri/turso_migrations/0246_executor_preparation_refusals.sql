-- A machine that could not prepare a cell for a project because a host
-- toolchain one of the project's setup commands runs is not on its PATH (exit
-- 127 for a bare-named program the executor itself cannot resolve).
--
-- That is a fact about the machine and the project, not about any batch or
-- branch: every cell for the project runs the same setup, so the next batch
-- placed there fails the same way. The row keeps placement from sending the
-- project back until the machine's evidence changes.
--
-- environment_identity is the machine's probe-derived identity when the
-- refusal was recorded. A re-attach that advertises a different identity (the
-- toolchain now present) drops the row; identical evidence keeps it. An
-- operator's `repaired` deletes the machine's rows outright. setup_identity is
-- a digest of the setup commands it failed under: a project whose setup has
-- since changed is not refused on this row, and the row is dropped.
CREATE TABLE IF NOT EXISTS executor_preparation_refusals (
    executor_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    environment_identity TEXT NOT NULL,
    setup_identity TEXT NOT NULL,
    command TEXT NOT NULL,
    program TEXT NOT NULL,
    diagnostic TEXT NOT NULL,
    recorded_at INTEGER NOT NULL,
    PRIMARY KEY (executor_id, project_id)
);
