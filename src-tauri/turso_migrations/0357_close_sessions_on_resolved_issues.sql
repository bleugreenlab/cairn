-- lineage: shared
-- Close every open session of a job whose issue is resolved, whatever the
-- job's status, with the resolution's reason: `issue_merged` or
-- `issue_closed`.
--
-- Older builds left these open. Resolutions before the resolution path closed
-- sessions at all left every job's session open, and the path that did close
-- them skipped any job whose memory review had been sent, so a review the
-- resolution's stop ended never came back to close its session. An open
-- session on a resolved issue's job gave the launch funnel nothing to refuse a
-- turn on and the operator no reason. The resolution now closes every job's
-- sessions in the same write, holding one back only while its review turn is
-- live, so this converges the rows older builds left behind once.
--
-- No memory-review hold applies here: migrations run before any agent process
-- exists, so no review turn the database records as live is actually running.
-- 0345 has already closed the cancelled jobs' sessions.
--
-- `closed_at` takes the time the issue resolved, rather than the migration's.
UPDATE sessions
   SET status = 'closed',
       terminal_reason = (
         SELECT CASE i.status WHEN 'merged' THEN 'issue_merged' ELSE 'issue_closed' END
           FROM jobs j JOIN issues i ON i.id = j.issue_id
          WHERE j.id = sessions.job_id),
       closed_at = COALESCE(
         (SELECT CASE i.status WHEN 'merged' THEN i.merged_at ELSE i.closed_at END
            FROM jobs j JOIN issues i ON i.id = j.issue_id
           WHERE j.id = sessions.job_id),
         (SELECT i.completed_at
            FROM jobs j JOIN issues i ON i.id = j.issue_id
           WHERE j.id = sessions.job_id),
         unixepoch()),
       updated_at = unixepoch()
 WHERE status = 'open'
   AND job_id IN (SELECT j.id FROM jobs j JOIN issues i ON i.id = j.issue_id
                   WHERE i.status IN ('merged', 'closed'));
