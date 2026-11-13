-- lineage: shared
-- cairn/7110: declared regenerable paths a replay landed stale, and the placed
-- batch that regenerates them.
--
-- When every conflict of a replay falls inside one or more `landing.regenerate`
-- rules declared at the base, the replay takes the branch's bytes for those
-- paths and installs. One row per rule is written under the store lock before
-- that install, so a branch never carries a taken side without its marker.
-- After the install, a batch placed on a machine matching the rule's selector
-- runs the rule's `regenerate` and `check` commands at the installed head and
-- seals the result as a Cairn-authored commit.
--
-- A row in `pending` or `running` holds the branch at the mutation boundary,
-- exactly as an in-flight run batch does: no writer moves it, and a merge
-- waits for it to settle. Only its own batch's sealed result is exempt.
--
-- `rule_json`: the rule as the base declared it, so the batch runs what the
-- base said, never what the branch says.
-- `stale_json`: `[{path, blob}]`, the branch's blobs that were taken; a later
-- head holding different blobs was changed by someone else.
-- `install_group`: the install that took these paths. One install can take
-- paths of several rules, one row each; they are recorded together, run one
-- after another, and each one's commit is admitted past its siblings' holds.
-- `state`:
--   pending     installed, the batch not yet admitted.
--   running     the batch was admitted (`request_id`).
--   succeeded   the pair is fresh: sealed as `commit_id`, or already was.
--   failed      `failure_json` `{command, exit, outputTail, reason}`; the paths
--               stay stale and the owner was told.
--   superseded  the branch moved and the stale paths changed under it, a later
--               install replaced this row, or after a restart its install was
--               found never to have happened.
--   cleared     it had failed, and a lane whose impact covers its paths then
--               passed at the branch's head: the paths are fresh after all.
-- `dispatches`: how many batches this row has run; a second supersession
-- fails it.
CREATE TABLE branch_regenerations (
    id INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    store_path TEXT NOT NULL,
    bookmark TEXT NOT NULL,
    install_group TEXT NOT NULL,
    rule_json TEXT NOT NULL,
    stale_json TEXT NOT NULL,
    installed_head TEXT NOT NULL,
    base_commit TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'running', 'succeeded', 'failed', 'superseded', 'cleared')),
    request_id TEXT,
    failure_json TEXT,
    commit_id TEXT,
    dispatches INTEGER NOT NULL DEFAULT 0,
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL
);

CREATE INDEX idx_branch_regenerations_branch
    ON branch_regenerations (project_id, bookmark, state, id);

CREATE INDEX idx_branch_regenerations_holding
    ON branch_regenerations (state)
    WHERE state IN ('pending', 'running');
