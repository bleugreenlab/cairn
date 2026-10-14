-- Migration: add job_id to prompts
--
-- Denormalizes the owning node job onto prompts so the per-node question
-- segment (`q-N`) is enforced unique at the DB level. Closes a race where two
-- concurrent question appends to one node could compute the same COUNT(*)+1
-- ordinal and both insert. The 0007 backfill already assigned segments per node
-- by (created_at, id) ordinal, so existing rows are unique per job.

ALTER TABLE prompts ADD COLUMN job_id TEXT;

UPDATE prompts
SET job_id = (SELECT r.job_id FROM runs r WHERE r.id = prompts.run_id)
WHERE job_id IS NULL;

CREATE UNIQUE INDEX idx_prompts_job_uri_segment
    ON prompts(job_id, uri_segment)
    WHERE job_id IS NOT NULL AND uri_segment IS NOT NULL;
