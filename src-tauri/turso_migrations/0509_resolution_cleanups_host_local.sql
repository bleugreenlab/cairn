-- lineage: private
-- cairn/7685: the host-local half of the resolution-cleanup backfill. Terminal
-- rows and their output capture live only in this runner's private database,
-- so a resolved issue here that a job still holding a terminal that has not
-- exited, or output still owed its resolution stamp or final capture, settles
-- under is owed a settlement pass. The walk climbs from that evidence as the
-- shared backfill's does.
INSERT OR IGNORE INTO resolution_cleanups (kind, subject_id, run_id, resolution, resolution_generation, token, recorded_at)
WITH RECURSIVE held(job_id) AS (
    SELECT job_id FROM job_terminals WHERE status != 'exited' AND job_id IS NOT NULL
    UNION SELECT job_id FROM terminal_outputs
     WHERE expiry_basis = 'resolution' AND (resolved_at IS NULL OR capture = 'capturing')
       AND job_id IS NOT NULL
), up(id, issue_id, parent_job_id, project_id, base_branch) AS (
    SELECT id, issue_id, parent_job_id, project_id, base_branch FROM jobs
     WHERE id IN (SELECT job_id FROM held)
    UNION SELECT p.id, p.issue_id, p.parent_job_id, p.project_id, p.base_branch
      FROM jobs p JOIN up ON p.id = up.parent_job_id
), sources(issue_id) AS (
    SELECT issue_id FROM up
    UNION SELECT s.issue_id FROM jobs s JOIN up
        ON s.project_id = up.project_id AND s.branch = up.base_branch
)
SELECT 'issue', i.id, NULL, i.status, i.resolution_generation, lower(hex(randomblob(16))), unixepoch()
  FROM issues i
 WHERE i.id IN (SELECT issue_id FROM sources) AND i.status IN ('merged', 'closed');
