-- lineage: shared
-- cairn/6173: durable fences for budgeted reconcile rollbacks that could not be
-- verified. A row is written before the failed attempt releases its store lock,
-- survives restart, and is cleared only by a verified targeted repair, never by
-- tip movement, a new base, cooldown expiry, or an explicit replay request.
--
-- `scope` is `branch` (only the bookmarks in `affected` are fenced) or `store`
-- (nothing may write the store). `coordinates` is the typed recovery record as
-- JSON: pre-attempt and failed targets per bookmark, Git expectations, the
-- saved operation (diagnostic only; deferred repair never restores it), and any
-- writer not proven gone. `affected` is the JSON array of fenced bookmark names,
-- kept beside it for admission checks. Local runtime safety state: it names
-- this runner's store path and is not exported with project history.
CREATE TABLE jj_branch_recoveries (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    store_path TEXT NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('branch', 'store')),
    status TEXT NOT NULL CHECK (status IN ('fenced', 'repaired')),
    phase TEXT NOT NULL,
    budget_class TEXT NOT NULL,
    principal TEXT,
    operation TEXT NOT NULL,
    affected TEXT NOT NULL,
    coordinates TEXT NOT NULL,
    diagnostic TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 1,
    repair_attempts INTEGER NOT NULL DEFAULT 0,
    next_repair_at INTEGER NOT NULL,
    last_repair_outcome TEXT,
    notice_pending INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    repaired_at INTEGER
);

CREATE INDEX idx_jj_branch_recoveries_store_status
    ON jj_branch_recoveries (store_path, status);
CREATE INDEX idx_jj_branch_recoveries_project
    ON jj_branch_recoveries (project_id);
