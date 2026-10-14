-- Migration: check result cache (CAIRN-2251)
--
-- Project-declared checks are keyed by the sealed tree they ran against and the
-- check name. The first slices only need an aggregate result; `target_results_json`
-- is reserved for later per-target result details without changing the cache key.
CREATE TABLE check_result_cache (
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    tree_hash TEXT NOT NULL,
    check_name TEXT NOT NULL,
    exit_code INTEGER NOT NULL,
    passed INTEGER NOT NULL,
    output_tail TEXT NOT NULL,
    duration_ms INTEGER NOT NULL,
    ran_at INTEGER NOT NULL,
    target_results_json TEXT,
    PRIMARY KEY (project_id, tree_hash, check_name)
);

CREATE INDEX idx_check_result_cache_project_ran_at
    ON check_result_cache(project_id, ran_at DESC);
