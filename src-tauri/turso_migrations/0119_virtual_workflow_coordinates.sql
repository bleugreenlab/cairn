-- Workflow process restart state is private runner-local data. Its durable
-- identity is the run/job and branch coordinate, never the process cwd.
ALTER TABLE workflow_run RENAME COLUMN worktree_mode TO branch_mode;
ALTER TABLE workflow_run DROP COLUMN working_dir;
