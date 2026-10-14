-- CAIRN-2386: first-class PR nodes briefly stored merge_requests.job_id as the
-- `action_runs.id` for the pr action. The exact producing job is already stored
-- on that action run as `parent_job_id`. Relink only those exact rows, and leave
-- anything that is not an action-run-owned PR untouched rather than guessing.

UPDATE merge_requests
SET job_id = (
    SELECT ar.parent_job_id
    FROM action_runs ar
    JOIN jobs parent ON parent.id = ar.parent_job_id
    WHERE ar.id = merge_requests.job_id
      AND parent.project_id = merge_requests.project_id
      AND parent.branch = merge_requests.source_branch
)
WHERE EXISTS (
    SELECT 1
    FROM action_runs ar
    JOIN jobs parent ON parent.id = ar.parent_job_id
    WHERE ar.id = merge_requests.job_id
      AND parent.project_id = merge_requests.project_id
      AND parent.branch = merge_requests.source_branch
)
AND NOT EXISTS (
    SELECT 1
    FROM merge_requests other
    WHERE other.id != merge_requests.id
      AND other.job_id = (
          SELECT ar.parent_job_id
          FROM action_runs ar
          WHERE ar.id = merge_requests.job_id
      )
);
