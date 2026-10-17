-- A routed batch's sealed delta that reached this machine's project store but
-- whose publication onto the branch failed (cairn/4986).
--
-- The delta commit's objects are installed in the project repository and kept
-- alive by `reference` (under refs/cairn/stranded/). The row is what lets the
-- next commit barrier on the same branch (a `write`, a `run` with a commit_msg,
-- or a PR artifact action) republish it, oldest first, ahead of its own
-- changes. `status` moves held -> landed when the commit reaches the branch, or
-- held -> stranded when it can no longer be merged onto what the branch holds.
-- A commit that reached the branch here but not the remote its open pull
-- request reads (the export or push failed) is `unpublished`, with
-- `landed_head` naming it, until a later push carries it; a merge is refused
-- while any row on the branch is held or unpublished.
--
-- Machine-local like the store paths it names, so it is private. No foreign
-- key to `projects`, because a team project's row lives in its replica.
CREATE TABLE IF NOT EXISTS held_publications (
    attempt_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    store_path TEXT NOT NULL,
    repository_path TEXT NOT NULL,
    branch TEXT NOT NULL,
    job_id TEXT,
    base_commit TEXT NOT NULL,
    delta_commit TEXT NOT NULL,
    commit_message TEXT NOT NULL,
    author_name TEXT,
    author_email TEXT,
    reference TEXT,
    status TEXT NOT NULL DEFAULT 'held' CHECK (status IN ('held', 'unpublished', 'landed', 'stranded')),
    last_error TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 1,
    landed_head TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_held_publications_branch
    ON held_publications (project_id, branch, status, created_at);
