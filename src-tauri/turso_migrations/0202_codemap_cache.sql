-- Migration: codemap cache (CAIRN-4225)
--
-- A code map is a pure projection of one materialized commit: the source-file
-- inventory and the import graph over it. Nothing here is authored, so a row is
-- a cache in the strict sense -- deletable at any moment and rebuildable from
-- the repository alone. It is private to this install for that reason
-- (`PrivateReason::RebuildableCache`) rather than synced into a team replica,
-- which would ship hundreds of kilobytes per merge for a projection the replica
-- can derive from the same commit it already has.
--
-- What is NOT stored here is churn. Churn is per-file merged activity over a
-- trailing window, so it is a function of the clock as well as the tree: the
-- same commit yields different churn tomorrow. Baking it into a row keyed by
-- commit would make that row silently wrong as time passed, with no event to
-- invalidate it -- a base that never moves would serve month-old activity
-- forever. It is joined from `file_changes` at read time instead, which keeps
-- everything stored here a true function of `base_commit_sha` alone.
--
-- Keyed by (project_id, base_commit_sha) so one map exists per base tree: a base
-- that advances and then reverts finds its map already computed. `invalidated_at`
-- marks a row the project's base has moved past. The read path still serves that
-- row, flagged stale, while the recompute runs -- which is what keeps the first
-- read instant instead of blocking on a materialization and a walk.
CREATE TABLE codemap_cache (
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    base_commit_sha TEXT NOT NULL,
    computed_at INTEGER NOT NULL,
    invalidated_at INTEGER,
    file_count INTEGER NOT NULL,
    edge_count INTEGER NOT NULL,
    codemap_json TEXT NOT NULL,
    PRIMARY KEY (project_id, base_commit_sha)
);

-- Serves "the newest map this project has", the fallback when the current base
-- commit has no row of its own yet.
CREATE INDEX idx_codemap_cache_project_computed_at
    ON codemap_cache(project_id, computed_at DESC);
