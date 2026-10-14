-- An operator's declaration that a machine has been repaired.
--
-- Most machine repairs are visible in the executor's own attach-time probes: fix
-- a toolchain, restart the executor, and its measured facts change. Some are
-- not — a corrupt sccache directory cleared, a helper binary replaced — and for
-- those an operator needs a way to say so. Restarting Cairn is not a repair API.
--
-- Keyed by executor_id so the colocated executor participates on the same terms
-- as an enrolled one; it holds no credential and belongs beside neither.
CREATE TABLE IF NOT EXISTS executor_repairs (
    executor_id TEXT PRIMARY KEY,
    repair_epoch INTEGER NOT NULL DEFAULT 0,
    repaired_at INTEGER NOT NULL
);
