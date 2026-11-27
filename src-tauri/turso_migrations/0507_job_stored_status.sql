-- lineage: shared
-- cairn/7675: per-job facts stored on the job row as explicit values, each
-- written by the site that causes it through one setter in
-- `cairn_db::stored_status::jobs` (shared with cairn/7687). Readers combine
-- them with fixed CASEs over the same row; nothing re-derives them from other
-- tables.
--
-- head_turn: what the job's current turn is doing: 'running' (pending or
--   running), 'resume_owed' (a claimed dependency_unblock marker whose resume
--   has not launched: no run, no end_reason), or 'idle'.
-- activity_at: when head_turn last changed.
-- turn_end_attempt_state: the state of the attempt turn_end_check_attempt_id
--   names, or NULL.
-- open_cairn_waits: the job's waits Cairn is resolving or will resolve:
--   resolving, or pending on anything but a person.
-- hold_standing: the job's one active provider-limit hold, if any: 'queue'
--   (the run queue owes it), 'timer' (a known reset and no gate), or 'other'.
-- open_check_requests: check requests the job filed that have not settled.
ALTER TABLE jobs ADD COLUMN head_turn TEXT NOT NULL DEFAULT 'idle'
    CHECK (head_turn IN ('running', 'resume_owed', 'idle'));
ALTER TABLE jobs ADD COLUMN activity_at INTEGER;
ALTER TABLE jobs ADD COLUMN turn_end_attempt_state TEXT;
ALTER TABLE jobs ADD COLUMN open_cairn_waits INTEGER NOT NULL DEFAULT 0
    CHECK (open_cairn_waits >= 0);
ALTER TABLE jobs ADD COLUMN hold_standing TEXT NOT NULL DEFAULT 'none'
    CHECK (hold_standing IN ('none', 'queue', 'timer', 'other'));
ALTER TABLE jobs ADD COLUMN open_check_requests INTEGER NOT NULL DEFAULT 0
    CHECK (open_check_requests >= 0);

-- The run queue reads only jobs that are active or wanting. The predicate is
-- `stored_status::jobs::QUEUE_LISTED_SQL`, verbatim: the read names it, so the
-- planner uses this index, and a job status or pointer change re-evaluates it
-- with no writer of its own.
CREATE INDEX idx_jobs_queue_activity ON jobs(execution_id) WHERE ((turn_end_attempt_state IN ('requested', 'claimed', 'runtime_started', 'deferred', 'submitted') OR open_check_requests > 0 OR (status IN ('pending', 'running') AND (head_turn IN ('running', 'resume_owed') OR (status = 'running' AND current_turn_id IS NULL) OR open_cairn_waits > 0 OR hold_standing = 'timer'))) OR hold_standing = 'queue');

-- Backfill each fact once from its source rows.
UPDATE jobs SET head_turn = 'running',
       activity_at = (SELECT COALESCE(t.started_at, t.created_at) FROM turns t WHERE t.id = jobs.current_turn_id)
 WHERE current_turn_id IS NOT NULL
   AND (SELECT t.state FROM turns t WHERE t.id = jobs.current_turn_id) IN ('pending', 'running');

UPDATE jobs SET head_turn = 'resume_owed'
 WHERE current_turn_id IS NOT NULL
   AND EXISTS (SELECT 1 FROM turns t
                WHERE t.id = jobs.current_turn_id AND t.state = 'complete'
                  AND t.start_reason = 'dependency_unblock'
                  AND t.run_id IS NULL AND t.end_reason IS NULL);

UPDATE jobs SET turn_end_attempt_state =
       (SELECT ta.state FROM turn_end_check_attempts ta WHERE ta.id = jobs.turn_end_check_attempt_id)
 WHERE turn_end_check_attempt_id IS NOT NULL;

UPDATE jobs SET open_cairn_waits =
       (SELECT COUNT(*) FROM agent_waits aw
         WHERE aw.job_id = jobs.id
           AND (aw.state = 'resolving' OR (aw.state = 'pending' AND aw.kind NOT IN ('operator_request'))))
 WHERE id IN (SELECT job_id FROM agent_waits WHERE state IN ('pending', 'resolving'));

UPDATE jobs SET hold_standing =
       (SELECT CASE WHEN h.boundary_source = 'queue' THEN 'queue'
                    WHEN h.retry_not_before IS NOT NULL AND h.resume_gate IS NULL THEN 'timer'
                    ELSE 'other' END
          FROM provider_limit_holds h WHERE h.job_id = jobs.id AND h.state = 'active')
 WHERE id IN (SELECT job_id FROM provider_limit_holds WHERE state = 'active');

UPDATE jobs SET open_check_requests =
       (SELECT COUNT(*) FROM check_requests cr
         WHERE cr.requester_job_id = jobs.id AND cr.state IN ('accepted', 'queued', 'granted', 'running'))
 WHERE id IN (SELECT requester_job_id FROM check_requests
               WHERE state IN ('accepted', 'queued', 'granted', 'running'));
