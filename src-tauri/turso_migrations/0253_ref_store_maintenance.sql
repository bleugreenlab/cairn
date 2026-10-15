-- Project ref-store maintenance (cairn/4975).
--
-- `ref_store_samples` is the measured size of a project's shared Git ref store
-- over time, and what each maintenance prune removed. A `measure` row is a
-- lock-free census taken hourly; a `prune` row records one maintenance pass,
-- including the census before and after it in `detail_json`. Measurement rows
-- are kept for 30 days.
--
-- `ref_store_maintenance` holds the operator's explicit choice for a store, if
-- any. Pruning is the default and a store with no row follows it, except that
-- a store above the size thresholds waits for one confirmation before its
-- first prune. `confirmed` gives that confirmation; `opted_out` keeps the
-- store measured and never pruned.
--
-- Both are private: a ref store is a file on this machine, and the store path
-- and its numbers describe this installation only. No foreign key to
-- `projects`, because a team project's row lives in its replica.
CREATE TABLE ref_store_samples (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL,
    store_path TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('measure', 'prune')),
    started_at INTEGER NOT NULL,
    finished_at INTEGER NOT NULL,
    -- `ok`, `skipped:<reason>`, or `failed:<error>`.
    outcome TEXT NOT NULL,
    packed_bytes INTEGER,
    packed_entries INTEGER,
    loose_refs INTEGER,
    ref_locks INTEGER,
    namespaces_json TEXT,
    jj_store_bytes INTEGER,
    removed_keep_refs INTEGER,
    removed_cairn_refs INTEGER,
    ops_abandoned INTEGER,
    -- Set only when the pass truncated the operation log.
    ops_reparented INTEGER,
    detail_json TEXT,
    duration_ms INTEGER NOT NULL
);

CREATE INDEX idx_ref_store_samples_project
ON ref_store_samples(project_id, kind, started_at);

CREATE INDEX idx_ref_store_samples_store
ON ref_store_samples(store_path, kind, started_at);

CREATE TABLE ref_store_maintenance (
    store_path TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL,
    prune_policy TEXT NOT NULL CHECK (prune_policy IN ('confirmed', 'opted_out')),
    updated_at INTEGER NOT NULL
);
