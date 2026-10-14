-- Migration: re-key check_result_cache by per-check impact-scoped input hash (CAIRN-2281)
--
-- A project check's cached verdict is now keyed by the content identity of ONLY
-- the files matching that check's impact globs (its "input hash"), not the whole
-- sealed tree. A commit that touches none of a check's inputs (e.g. a doc-only
-- commit landing after a src-tauri commit) therefore reuses the stored verdict
-- instead of re-running the check.
--
-- `tree_hash` is retained as a mutable pointer to the most recent sealed tree at
-- which each verdict was confirmed: the `/checks` listing and PR-node render look
-- rows up by whole-tree hash (`list_check_results`), and every applicable check is
-- re-stamped onto the current tree on each evaluation (run OR cache hit), so that
-- listing keeps surfacing each check at the current tree.
--
-- Existing rows are a transient performance cache keyed by the OLD whole-tree
-- scheme and carry no `input_hash`; they are dropped so the table is rebuilt
-- clean. The only cost is a one-time re-run of each check, which is safe. Nothing
-- holds a foreign key to this table, so the drop-and-recreate needs no FK toggle.
DROP TABLE IF EXISTS check_result_cache;

CREATE TABLE check_result_cache (
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    tree_hash TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    check_name TEXT NOT NULL,
    exit_code INTEGER NOT NULL,
    passed INTEGER NOT NULL,
    output_tail TEXT NOT NULL,
    duration_ms INTEGER NOT NULL,
    ran_at INTEGER NOT NULL,
    target_results_json TEXT,
    PRIMARY KEY (project_id, check_name, input_hash)
);

-- The /checks listing and PR-node render look verdicts up by the whole-tree hash.
CREATE INDEX idx_check_result_cache_project_tree
    ON check_result_cache(project_id, tree_hash);

CREATE INDEX idx_check_result_cache_project_ran_at
    ON check_result_cache(project_id, ran_at DESC);
