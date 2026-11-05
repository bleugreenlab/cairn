-- lineage: private
-- history_tier: the catalog of history packs (cairn/5967). The archive's file
-- store cannot be enumerated, so these rows are the only list of packs.
-- One row per packed execution; `content_hash` is the pack's object key, and
-- `evicted_at` is what sends reads to the pack instead of the hot rows. Nothing
-- sets it yet, so every production read stays hot.
--
-- `history_tier_runs` keys reads by run, so a later pack kind can map a thread's
-- runs onto range packs (cairn/5587). It has no foreign key: rows are written
-- before their `history_tier` row, which is the commit point, and a run row with
-- no `history_tier` row is ignored by the read seam's join.
CREATE TABLE history_tier (
    execution_id TEXT PRIMARY KEY NOT NULL,
    content_hash TEXT NOT NULL,
    format INTEGER NOT NULL,
    bytes INTEGER NOT NULL,
    hot_bytes_estimate INTEGER NOT NULL,
    row_counts TEXT NOT NULL,
    resolved_at INTEGER NOT NULL,
    packed_at INTEGER NOT NULL,
    evicted_at INTEGER
);

CREATE TABLE history_tier_runs (
    run_id TEXT PRIMARY KEY NOT NULL,
    execution_id TEXT NOT NULL
);

CREATE INDEX idx_history_tier_runs_execution ON history_tier_runs(execution_id);
