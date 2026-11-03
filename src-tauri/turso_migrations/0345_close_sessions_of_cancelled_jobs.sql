-- lineage: shared
-- Close every open session of a cancelled job, with the best reason the job's
-- issue still records: `issue_merged` or `issue_closed` when the issue is
-- resolved, `job_cancelled` otherwise (no issue, or one since reopened).
--
-- Older builds cancelled jobs without closing their sessions: the projection
-- sweep's derived cancel updated only the `jobs` row, and resolutions before
-- the resolution path closed sessions at all left theirs open. A cancelled job
-- holding an open session gave the launch funnel nothing to refuse it on and
-- the operator no reason. The writers now close the sessions in the same
-- write as the cancel, so this converges the rows they left behind once.
--
-- `closed_at` takes the job's own completion time, when the cancel happened,
-- rather than the migration's.
UPDATE sessions
   SET status = 'closed',
       terminal_reason = COALESCE(
         (SELECT CASE i.status
                   WHEN 'merged' THEN 'issue_merged'
                   WHEN 'closed' THEN 'issue_closed'
                 END
            FROM jobs j JOIN issues i ON i.id = j.issue_id
           WHERE j.id = sessions.job_id),
         'job_cancelled'),
       closed_at = COALESCE(
         (SELECT completed_at FROM jobs WHERE id = sessions.job_id),
         unixepoch()),
       updated_at = unixepoch()
 WHERE status = 'open'
   AND job_id IN (SELECT id FROM jobs WHERE status = 'cancelled');
