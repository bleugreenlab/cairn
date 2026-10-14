-- Give already-spawned thread children the thread id they were never stamped
-- with.
--
-- `jobs.thread_id` is the whole of what a thread can see: `list_jobs_for_thread`
-- selects on that column alone, and the thread pane lists, opens, and rolls up
-- status and artifacts from what it returns. Nothing ever wrote the column for a
-- delegated child, so every task a thread spawned after the threads cutover is
-- invisible to the thread that spawned it. (The pre-cutover tasks that DO carry
-- an id got it incidentally from 0157, which re-pointed the thread-issue's jobs
-- wholesale.) Job creation now inherits the column from the delegating parent;
-- this carries the same rule back over the rows that already exist, so those
-- tasks become openable instead of permanently orphaned.
--
-- Recursive so a task that itself delegated is reached too: the anchor is every
-- job that already knows its thread, and each step adopts the unstamped children
-- of a job the walk has resolved.
WITH RECURSIVE thread_owned(job_id, thread_id) AS (
    SELECT id, thread_id FROM jobs WHERE thread_id IS NOT NULL
    UNION ALL
    SELECT j.id, o.thread_id
    FROM jobs j
    JOIN thread_owned o ON j.parent_job_id = o.job_id
    WHERE j.thread_id IS NULL
)
UPDATE jobs
SET thread_id = (SELECT o.thread_id FROM thread_owned o WHERE o.job_id = jobs.id)
WHERE thread_id IS NULL
  AND parent_job_id IS NOT NULL
  AND EXISTS (SELECT 1 FROM thread_owned o WHERE o.job_id = jobs.id);
