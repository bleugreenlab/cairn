-- Explicit child-issue -> spawning coordinator job edge (CAIRN-1302).
--
-- Child-attention wakes must resume the exact job that spawned the child
-- issue. The only durable link a child issue previously carried was
-- parent_issue_id (issue -> issue), which forced wake resolution to *guess*
-- the coordinator job among all jobs on the parent issue (and mis-picked a
-- delegated sub-task job once the coordinator spawned one). Recording the
-- spawning job here lets the wake target that job directly.
--
-- NULL when the child issue was not created by an agent (e.g. linked by a
-- human, or created before this column existed); resolution then falls back to
-- the parent-issue heuristic. ON DELETE SET NULL so a deleted coordinator job
-- degrades to that fallback rather than dangling.
ALTER TABLE issues ADD COLUMN parent_job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL;
CREATE INDEX idx_issues_parent_job_id ON issues(parent_job_id);
