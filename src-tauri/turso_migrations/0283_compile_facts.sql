-- cairn/5224, cairn/5236: the compile fact. One row per attempt to compile a
-- tree, in the shape cairn/5236 defines; the landing compile gate writes one for
-- every merge result it judges, and any check lane whose output is a cargo
-- build failure writes one for the tree it ran on. Read by the lane hold, red
-- attribution, `after: {fact: "compiles"}` gates, and the PR surface, none of
-- which compile again.
--
-- `outcome` is closed by 5236: a tree builds, does not build, or was not
-- compiled (no cell within budget, placement refused). `not-run` is a refusal
-- fact, never a pass. `repair_commit` is the one column a row gains later,
-- when the commit that closed the break becomes known.
CREATE TABLE compile_facts (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL,
    -- The commit compiled, and its git tree id, which reuse keys on.
    tree_commit TEXT NOT NULL,
    tree_hash TEXT NOT NULL,
    -- For a merge-result compile: the target tip it was built on and the
    -- branch head merged onto it, and the pull request. NULL for a plain tree.
    base_commit TEXT,
    head_commit TEXT,
    merge_request_id TEXT,
    -- The executor's verdict platform and the executor; never the
    -- orchestrator's OS. NULL when nothing ran.
    platform TEXT,
    executor_id TEXT,
    -- The compile command exactly as run, and the check that ran it.
    scope TEXT NOT NULL,
    check_name TEXT,
    outcome TEXT NOT NULL CHECK (outcome IN ('builds', 'does-not-build', 'not-run')),
    -- On does-not-build: the ordered [{code, path, line}] of rustc's errors
    -- (attribution matches on code and path), the first crate target cargo
    -- could not compile, and the diagnostic text. On not-run: what placement
    -- said, in `diagnostic_tail`.
    signature_json TEXT,
    first_failing_crate TEXT,
    first_failing_target TEXT,
    diagnostic_tail TEXT,
    -- landing-gate | lane:<check> | main-cadence
    source TEXT NOT NULL,
    -- The check observation the row was read from, when one was recorded.
    observation_id TEXT,
    repair_commit TEXT,
    written_at INTEGER NOT NULL
);

CREATE INDEX idx_compile_facts_tree
ON compile_facts(project_id, tree_hash, written_at DESC);

CREATE INDEX idx_compile_facts_head
ON compile_facts(head_commit, written_at DESC);
