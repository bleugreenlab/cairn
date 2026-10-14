-- Migration: add job_id + uri_segment to permission_requests
--
-- Mirrors prompts 0007/0008. Gives each permission request a stable, addressable
-- per-node segment (`perm-1`, `perm-2`, ...) so it can be read and answered via
-- the cairn://.../permissions/{segment} resource, and denormalizes the owning
-- node job so the (job_id, uri_segment) pair is unique at the DB level. Both the
-- legacy tool permission prompt and the worktree fence stamp these at insert
-- time; existing rows are backfilled deterministically below.

ALTER TABLE permission_requests ADD COLUMN job_id TEXT;
ALTER TABLE permission_requests ADD COLUMN uri_segment TEXT;

-- Backfill job_id from the owning run for existing rows.
UPDATE permission_requests
SET job_id = (SELECT r.job_id FROM runs r WHERE r.id = permission_requests.run_id)
WHERE job_id IS NULL;

-- Backfill uri_segment per node by (created_at, id) ordinal, matching the
-- deterministic scheme new rows compute at insert time.
WITH ranked AS (
    SELECT
        pr.id AS request_id,
        (
            SELECT COUNT(*)
            FROM permission_requests pr2
            JOIN runs r2 ON pr2.run_id = r2.id
            WHERE r2.job_id = r.job_id
              AND (
                  pr2.created_at < pr.created_at
                  OR (pr2.created_at = pr.created_at AND pr2.id <= pr.id)
              )
        ) AS ord
    FROM permission_requests pr
    JOIN runs r ON pr.run_id = r.id
    WHERE pr.uri_segment IS NULL
)
UPDATE permission_requests
SET uri_segment = 'perm-' || (SELECT ord FROM ranked WHERE ranked.request_id = permission_requests.id)
WHERE id IN (SELECT request_id FROM ranked);

CREATE UNIQUE INDEX idx_permission_requests_job_uri_segment
    ON permission_requests(job_id, uri_segment)
    WHERE job_id IS NOT NULL AND uri_segment IS NOT NULL;
