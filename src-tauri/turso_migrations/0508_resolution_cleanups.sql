-- lineage: shared
-- cairn/7685: the resolution cleanups still owed, stored as a fact instead of
-- re-derived from every live job and every historical cancelled turn on each
-- terminal-lifecycle pass.
--
-- `issue`: this resolved issue may still hold resources (processes, terminals,
-- batches, check requests, an execution environment). Written when the issue
-- resolves and when a resource is acquired for an already-resolved owner;
-- deleted on reopen, or once settlement reports nothing pending at the
-- generation it was written for.
--
-- `calls`: this turn, cancelled by its issue's resolution, may still have
-- unanswered tool calls. Survives reopen (the calls are still unanswered);
-- deleted once the turn has no open call.
--
-- Every writer lives in `execution/resolution_cleanups.rs`, except the turn
-- trigger below: a turn is cancelled for resolution by several paths (the
-- resolution stop, the generic turn transition, launch unwinds), and the
-- trigger records the entry in that same transaction whichever path did it.
CREATE TABLE resolution_cleanups (
    kind TEXT NOT NULL CHECK (kind IN ('issue', 'calls')),
    subject_id TEXT NOT NULL,
    run_id TEXT,
    resolution TEXT NOT NULL CHECK (resolution IN ('merged', 'closed')),
    resolution_generation INTEGER,
    -- issue only: minted afresh by every write of the entry. A settlement
    -- deletes the enrolment it started from, never one written since.
    token TEXT,
    recorded_at INTEGER NOT NULL,
    PRIMARY KEY (kind, subject_id)
);

CREATE TRIGGER resolution_cleanups_cancelled_turn AFTER UPDATE OF state, end_reason ON turns
WHEN NEW.state = 'cancelled'
  AND NEW.end_reason IN ('issue_merged', 'issue_closed')
  AND NEW.run_id IS NOT NULL
  AND (OLD.state IS NOT 'cancelled' OR OLD.end_reason IS NOT NEW.end_reason)
BEGIN
    INSERT OR IGNORE INTO resolution_cleanups (kind, subject_id, run_id, resolution, resolution_generation, recorded_at)
    VALUES ('calls', NEW.id, NEW.run_id,
            CASE NEW.end_reason WHEN 'issue_merged' THEN 'merged' ELSE 'closed' END,
            NULL, unixepoch());
END;

-- Every turn a resolution already cancelled. The first pass clears those whose
-- calls are all answered, once.
INSERT OR IGNORE INTO resolution_cleanups (kind, subject_id, run_id, resolution, resolution_generation, recorded_at)
SELECT 'calls', id, run_id,
       CASE end_reason WHEN 'issue_merged' THEN 'merged' ELSE 'closed' END,
       NULL, unixepoch()
  FROM turns
 WHERE state = 'cancelled' AND end_reason IN ('issue_merged', 'issue_closed') AND run_id IS NOT NULL;

-- Resolved issues a job still holding durable evidence of a resource in this
-- database settles under: a process, an open check request, a detached batch
-- that commits or holds the branch, or a parked batch wait. The walk starts
-- from that evidence (a handful of rows) and climbs to the issues, as
-- `resolved_resource_issues` does for one job: its own and its ancestors'
-- issues, and the issue whose branch it was cut from. Host-local resources
-- (terminals, output capture) are the private migration's; an execution
-- environment is re-observed when its executor re-advertises it.
INSERT OR IGNORE INTO resolution_cleanups (kind, subject_id, run_id, resolution, resolution_generation, token, recorded_at)
WITH RECURSIVE held(job_id) AS (
    SELECT job_id FROM runs WHERE status IN ('starting', 'live') AND job_id IS NOT NULL
    UNION SELECT requester_job_id FROM check_requests
     WHERE state IN ('accepted', 'queued', 'granted', 'running') AND requester_job_id IS NOT NULL
    UNION SELECT job_id FROM run_batch_detached
     WHERE state IN ('parked', 'running') AND (commits = 1 OR holds_branch = 1)
    UNION SELECT job_id FROM agent_waits
     WHERE state IN ('pending', 'resolving') AND kind = 'run_batch'
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
