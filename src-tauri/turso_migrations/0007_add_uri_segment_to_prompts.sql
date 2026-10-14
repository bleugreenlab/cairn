-- Migration: add uri_segment to prompts
--
-- Gives each user question a stable, addressable segment (`q-1`, `q-2`, ...)
-- scoped per node job, consistent with the stored-segment identity model used
-- for jobs (CAIRN-1125). Segments are assigned deterministically per node by
-- the ordinal position of the prompt among the node's prompts, ordered by
-- (created_at, id). New rows compute their segment at insert time.

ALTER TABLE prompts ADD COLUMN uri_segment TEXT;

WITH ranked AS (
    SELECT
        p.id AS prompt_id,
        (
            SELECT COUNT(*)
            FROM prompts p2
            JOIN runs r2 ON p2.run_id = r2.id
            WHERE r2.job_id = r.job_id
              AND (
                  p2.created_at < p.created_at
                  OR (p2.created_at = p.created_at AND p2.id <= p.id)
              )
        ) AS ord
    FROM prompts p
    JOIN runs r ON p.run_id = r.id
    WHERE p.uri_segment IS NULL
)
UPDATE prompts
SET uri_segment = 'q-' || (SELECT ord FROM ranked WHERE ranked.prompt_id = prompts.id)
WHERE id IN (SELECT prompt_id FROM ranked);

CREATE INDEX idx_prompts_uri_segment ON prompts(uri_segment);
