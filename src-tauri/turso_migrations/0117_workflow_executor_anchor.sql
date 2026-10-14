-- Durable inputs for reconstructing workflow executor leases. The legacy local
-- paths remain during the migration window but are no longer the spawn anchor.
ALTER TABLE workflow_run ADD COLUMN project_id TEXT;
ALTER TABLE workflow_run ADD COLUMN execution_id TEXT;
ALTER TABLE workflow_run ADD COLUMN anchor_branch TEXT;
ALTER TABLE workflow_run ADD COLUMN worktree_mode TEXT NOT NULL DEFAULT 'none'
    CHECK (worktree_mode IN ('none', 'inherit'));
ALTER TABLE workflow_run ADD COLUMN package_path TEXT;

UPDATE workflow_run
SET project_id = (SELECT jobs.project_id FROM jobs WHERE jobs.id = workflow_run.job_id),
    execution_id = (SELECT jobs.execution_id FROM jobs WHERE jobs.id = workflow_run.job_id),
    anchor_branch = COALESCE(
        (SELECT jobs.branch FROM jobs WHERE jobs.id = workflow_run.job_id),
        (SELECT jobs.base_branch FROM jobs WHERE jobs.id = workflow_run.job_id)
    );