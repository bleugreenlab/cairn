-- lineage: shared
-- cairn/6080: a per-branch catch-up mode the operator sets. A branch in mode
-- `merge` is brought current with its base by ONE merge commit of the base,
-- in the automatic advance and at landing, and is never replayed. One row per
-- (project, store, bookmark); there is no project-wide or settings-wide form.
--
-- `job_id` is the owning job: its latest run receives the merge wakes, and its
-- issue's resolution (or the branch's landing) ends the mode. `review_paths`
-- is the JSON array of globs the operator declared review-required when the
-- mode was set; only re-setting the mode changes it. `reviewed_through` is the
-- one base commit the owner has reviewed the incoming changes through; a
-- landing whose base changed a declared path since that commit is refused.
-- Local runtime state: it names this runner's store path.
CREATE TABLE branch_catch_up_modes (
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    store_path TEXT NOT NULL,
    bookmark TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode = 'merge'),
    job_id TEXT NOT NULL,
    set_by TEXT NOT NULL,
    set_at INTEGER NOT NULL,
    review_paths TEXT NOT NULL,
    reviewed_through TEXT NOT NULL,
    reviewed_by TEXT NOT NULL,
    reviewed_at INTEGER NOT NULL,
    PRIMARY KEY (project_id, store_path, bookmark)
);

CREATE INDEX idx_branch_catch_up_modes_job ON branch_catch_up_modes (job_id);

-- The audit trail of every set, clear, expiry, use and review of a branch's
-- catch-up mode. It outlives the mode row, so a cleared mode's history stays
-- readable on the branch's rebase page.
CREATE TABLE branch_catch_up_events (
    id INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    store_path TEXT NOT NULL,
    bookmark TEXT NOT NULL,
    kind TEXT NOT NULL,
    actor TEXT NOT NULL,
    detail TEXT NOT NULL,
    at INTEGER NOT NULL
);

CREATE INDEX idx_branch_catch_up_events_branch
    ON branch_catch_up_events (project_id, store_path, bookmark, at);
