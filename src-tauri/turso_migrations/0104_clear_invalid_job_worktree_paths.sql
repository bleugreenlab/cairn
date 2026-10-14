-- Clear the invalid sentinel paths that older child-task persistence could leave
-- behind. Worktree-less jobs are represented by NULL; a branch cannot remain
-- meaningful once the path assignment is removed.
UPDATE jobs
SET worktree_path = NULL,
    branch = NULL
WHERE worktree_path IS NOT NULL
  AND trim(worktree_path) IN ('', '/');
