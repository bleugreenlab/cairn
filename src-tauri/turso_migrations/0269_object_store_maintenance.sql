-- Object-store maintenance beside ref-store maintenance (cairn/5085).
--
-- `ref_store_samples` now also records the shape of each store's Git object
-- database, and a third kind of row:
--
-- - `measure`: the hourly lock-free census of the store's refs and, in the
--   object columns, of its packs and loose objects.
-- - `prune`: one gated maintenance prune. When the prune also ran the cruft
--   reclaim of unreferenced objects, `reclaimed_bytes` is set (the pack bytes it
--   freed) and the object columns hold the census after it; `detail_json` then
--   carries the before and after census under `objects`.
-- - `compact`: one geometric compaction of the object database, which removes
--   nothing and runs whenever git's own pack-count or loose-object thresholds
--   trip. The object columns hold the census after it, and `detail_json` the
--   census before and after.
--
-- `loose_objects` is an estimate, made the way `git gc --auto` makes it (the
-- entries of `objects/17` times 256). `multi_pack_index` is 0 or 1.
--
-- SQLite cannot alter a CHECK in place, so this is the ordinary
-- create-copy-drop-rename rebuild. The table is private per-runner state with
-- no inbound foreign keys.
CREATE TABLE ref_store_samples_new (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL,
    store_path TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('measure', 'prune', 'compact')),
    started_at INTEGER NOT NULL,
    finished_at INTEGER NOT NULL,
    -- `ok`, `partial:<refusal>`, `skipped:<reason>`, or `failed:<error>`.
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
    duration_ms INTEGER NOT NULL,
    packs INTEGER,
    kept_packs INTEGER,
    pack_bytes INTEGER,
    loose_objects INTEGER,
    multi_pack_index INTEGER,
    -- Set only when the prune ran the cruft reclaim.
    reclaimed_bytes INTEGER
);

INSERT INTO ref_store_samples_new (
    id, project_id, store_path, kind, started_at, finished_at, outcome,
    packed_bytes, packed_entries, loose_refs, ref_locks, namespaces_json,
    jj_store_bytes, removed_keep_refs, removed_cairn_refs, ops_abandoned,
    ops_reparented, detail_json, duration_ms
)
SELECT id, project_id, store_path, kind, started_at, finished_at, outcome,
       packed_bytes, packed_entries, loose_refs, ref_locks, namespaces_json,
       jj_store_bytes, removed_keep_refs, removed_cairn_refs, ops_abandoned,
       ops_reparented, detail_json, duration_ms
FROM ref_store_samples;

DROP TABLE ref_store_samples;
ALTER TABLE ref_store_samples_new RENAME TO ref_store_samples;

CREATE INDEX idx_ref_store_samples_project
ON ref_store_samples(project_id, kind, started_at);

CREATE INDEX idx_ref_store_samples_store
ON ref_store_samples(store_path, kind, started_at);
