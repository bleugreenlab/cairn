ALTER TABLE jobs ADD COLUMN uri_segment TEXT;
ALTER TABLE chats ADD COLUMN uri_segment TEXT;

WITH
    top_level_raw AS (
        SELECT
            id,
            issue_id,
            execution_id,
            created_at,
            lower(trim(coalesce(nullif(node_name, ''), nullif(recipe_node_id, ''), 'node'))) AS raw_segment
        FROM jobs
        WHERE uri_segment IS NULL
          AND parent_job_id IS NULL
          AND issue_id IS NOT NULL
          AND execution_id IS NOT NULL
    ),
    top_level_sanitized AS (
        SELECT
            id,
            issue_id,
            execution_id,
            created_at,
            replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(raw_segment, ' ', '-'), '/', '-'), '?', '-'), '#', '-'), '&', '-'), '.', '-'), ',', '-'), ':', '-'), ';', '-'), '(', '-'), ')', '-'), '_', '-') AS slug
        FROM top_level_raw
    ),
    top_level_collapsed AS (
        SELECT
            id,
            issue_id,
            execution_id,
            created_at,
            replace(replace(replace(replace(replace(replace(replace(replace(slug, '--', '-'), '--', '-'), '--', '-'), '--', '-'), '--', '-'), '--', '-'), '--', '-'), '--', '-') AS slug
        FROM top_level_sanitized
    ),
    top_level_bases AS (
        SELECT
            id,
            issue_id,
            execution_id,
            created_at,
            CASE
                WHEN coalesce(nullif(trim(slug, '-'), ''), 'node') GLOB '*[^a-z0-9-]*' THEN 'node-' || id
                ELSE coalesce(nullif(trim(slug, '-'), ''), 'node')
            END AS base_segment
        FROM top_level_collapsed
    ),
    top_level_ranked AS (
        SELECT
            current.id,
            current.base_segment,
            EXISTS (
                SELECT 1
                FROM top_level_bases prior
                WHERE prior.issue_id = current.issue_id
                  AND prior.execution_id = current.execution_id
                  AND prior.base_segment = current.base_segment
                  AND (prior.created_at < current.created_at OR (prior.created_at = current.created_at AND prior.id < current.id))
            ) AS has_prior_duplicate
        FROM top_level_bases current
    )
UPDATE jobs
SET uri_segment = (
    SELECT CASE
        WHEN has_prior_duplicate THEN base_segment || '-' || id
        ELSE base_segment
    END
    FROM top_level_ranked
    WHERE top_level_ranked.id = jobs.id
)
WHERE id IN (SELECT id FROM top_level_ranked);

WITH
    child_raw AS (
        SELECT
            id,
            parent_job_id,
            created_at,
            task_index,
            lower(trim(coalesce(nullif(node_name, ''), nullif(task_description, ''), nullif(agent_config_id, ''), 'task'))) AS raw_segment
        FROM jobs
        WHERE uri_segment IS NULL
          AND parent_job_id IS NOT NULL
    ),
    child_sanitized AS (
        SELECT
            id,
            parent_job_id,
            created_at,
            task_index,
            replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(raw_segment, ' ', '-'), '/', '-'), '?', '-'), '#', '-'), '&', '-'), '.', '-'), ',', '-'), ':', '-'), ';', '-'), '(', '-'), ')', '-'), '_', '-') AS slug
        FROM child_raw
    ),
    child_collapsed AS (
        SELECT
            id,
            parent_job_id,
            created_at,
            task_index,
            replace(replace(replace(replace(replace(replace(replace(replace(slug, '--', '-'), '--', '-'), '--', '-'), '--', '-'), '--', '-'), '--', '-'), '--', '-'), '--', '-') AS slug
        FROM child_sanitized
    ),
    child_bases AS (
        SELECT
            id,
            parent_job_id,
            created_at,
            task_index,
            CASE
                WHEN coalesce(nullif(trim(slug, '-'), ''), 'task') GLOB '*[^a-z0-9-]*' THEN 'task-' || id
                ELSE coalesce(nullif(trim(slug, '-'), ''), 'task')
            END AS base_segment
        FROM child_collapsed
    ),
    child_ranked AS (
        SELECT
            current.id,
            current.base_segment,
            EXISTS (
                SELECT 1
                FROM child_bases prior
                WHERE prior.parent_job_id = current.parent_job_id
                  AND prior.base_segment = current.base_segment
                  AND (
                      prior.created_at < current.created_at
                      OR (prior.created_at = current.created_at AND prior.task_index IS NOT NULL AND current.task_index IS NULL)
                      OR (prior.created_at = current.created_at AND prior.task_index = current.task_index AND prior.id < current.id)
                      OR (prior.created_at = current.created_at AND prior.task_index IS NULL AND current.task_index IS NULL AND prior.id < current.id)
                  )
            ) AS has_prior_duplicate
        FROM child_bases current
    )
UPDATE jobs
SET uri_segment = (
    SELECT CASE
        WHEN has_prior_duplicate THEN base_segment || '-' || id
        ELSE base_segment
    END
    FROM child_ranked
    WHERE child_ranked.id = jobs.id
)
WHERE id IN (SELECT id FROM child_ranked);

WITH
    project_raw AS (
        SELECT
            id,
            created_at,
            lower(trim(coalesce(nullif(node_name, ''), nullif(agent_config_id, ''), 'job'))) AS raw_segment
        FROM jobs
        WHERE uri_segment IS NULL
          AND parent_job_id IS NULL
          AND issue_id IS NULL
          AND execution_id IS NULL
    ),
    project_sanitized AS (
        SELECT
            id,
            created_at,
            replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(raw_segment, ' ', '-'), '/', '-'), '?', '-'), '#', '-'), '&', '-'), '.', '-'), ',', '-'), ':', '-'), ';', '-'), '(', '-'), ')', '-'), '_', '-') AS slug
        FROM project_raw
    ),
    project_collapsed AS (
        SELECT
            id,
            created_at,
            replace(replace(replace(replace(replace(replace(replace(replace(slug, '--', '-'), '--', '-'), '--', '-'), '--', '-'), '--', '-'), '--', '-'), '--', '-'), '--', '-') AS slug
        FROM project_sanitized
    ),
    project_bases AS (
        SELECT
            id,
            created_at,
            CASE
                WHEN coalesce(nullif(trim(slug, '-'), ''), 'job') GLOB '*[^a-z0-9-]*' THEN 'job-' || id
                ELSE coalesce(nullif(trim(slug, '-'), ''), 'job')
            END AS base_segment
        FROM project_collapsed
    ),
    project_ranked AS (
        SELECT
            current.id,
            current.base_segment,
            EXISTS (
                SELECT 1
                FROM project_bases prior
                WHERE prior.base_segment = current.base_segment
                  AND (prior.created_at < current.created_at OR (prior.created_at = current.created_at AND prior.id < current.id))
            ) AS has_prior_duplicate
        FROM project_bases current
    )
UPDATE jobs
SET uri_segment = (
    SELECT CASE
        WHEN has_prior_duplicate THEN base_segment || '-' || id
        ELSE base_segment
    END
    FROM project_ranked
    WHERE project_ranked.id = jobs.id
)
WHERE id IN (SELECT id FROM project_ranked);

WITH first_project_chats AS (
    SELECT current.id
    FROM chats current
    WHERE current.uri_segment IS NULL
      AND NOT EXISTS (
          SELECT 1
          FROM chats prior
          WHERE prior.uri_segment IS NULL
            AND prior.project_id = current.project_id
            AND (prior.created_at < current.created_at OR (prior.created_at = current.created_at AND prior.id < current.id))
      )
)
UPDATE chats
SET uri_segment = 'chat'
WHERE id IN (SELECT id FROM first_project_chats);

UPDATE chats
SET uri_segment = 'chat-' || id
WHERE uri_segment IS NULL;

CREATE UNIQUE INDEX idx_jobs_issue_execution_uri_segment
    ON jobs(issue_id, execution_id, uri_segment)
    WHERE parent_job_id IS NULL
      AND issue_id IS NOT NULL
      AND execution_id IS NOT NULL
      AND uri_segment IS NOT NULL;

CREATE UNIQUE INDEX idx_jobs_parent_uri_segment
    ON jobs(parent_job_id, uri_segment)
    WHERE parent_job_id IS NOT NULL
      AND uri_segment IS NOT NULL;

CREATE UNIQUE INDEX idx_chats_project_uri_segment
    ON chats(project_id, uri_segment)
    WHERE uri_segment IS NOT NULL;
