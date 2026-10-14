-- Move legacy issue-backed threads into the first-class threads entity.
INSERT INTO threads (
    id, project_id, name, title, status, attention,
    migrated_from_number, created_at, updated_at
)
SELECT
    i.id,
    i.project_id,
    CASE
        WHEN p.key = 'CAIRN' AND i.number = 3404 THEN 'general'
        WHEN p.key = 'CAIRN' AND i.number = 3528 THEN 'thread-ux'
        WHEN p.key = 'CAIRN' AND i.number = 3532 THEN 'cairnbench'
        WHEN p.key = 'CAIRN' AND i.number = 3631 THEN 'resource-library'
        WHEN p.key = 'CAIRN' AND i.number = 3652 THEN 'performance'
        WHEN p.key = 'CAIRN' AND i.number = 3443 THEN 'settings-ui'
        WHEN p.key = 'CAIRN' AND i.number = 3509 THEN 'ui-nitpicks'
        WHEN p.key = 'AXN' AND i.number = 19 THEN 'field-reports'
        WHEN p.key = 'AXN' AND i.number = 25 THEN 'cross-platform'
        WHEN p.key = 'AGG' AND i.number = 247 THEN 'plant-engine'
        WHEN p.key = 'MOVING' AND i.number = 1 THEN 'general'
        ELSE 'thread-' || i.number
    END,
    i.title,
    CASE WHEN i.status = 'closed' THEN 'closed' ELSE 'active' END,
    'none',
    i.number,
    i.created_at,
    i.updated_at
FROM issues i
JOIN projects p ON p.id = i.project_id
WHERE i.kind = 'thread'
   OR (p.key = 'CAIRN' AND i.number IN (3443, 3509));

UPDATE jobs
SET thread_id = issue_id, issue_id = NULL, execution_id = NULL
WHERE issue_id IN (SELECT id FROM threads WHERE migrated_from_number IS NOT NULL);

UPDATE runs
SET issue_id = NULL
WHERE job_id IN (SELECT id FROM jobs WHERE thread_id IN (
    SELECT id FROM threads WHERE migrated_from_number IS NOT NULL
));

UPDATE messages
SET channel_type = 'thread',
    channel_id = (
        SELECT t.id
        FROM threads t
        JOIN projects p ON p.id = t.project_id
        WHERE p.key || '/' || t.migrated_from_number = messages.channel_id
    )
WHERE channel_type = 'issue'
  AND channel_id IN (
      SELECT p.key || '/' || t.migrated_from_number
      FROM threads t
      JOIN projects p ON p.id = t.project_id
      WHERE t.migrated_from_number IS NOT NULL
  );

UPDATE comments
SET thread_id = issue_id, issue_id = NULL
WHERE issue_id IN (SELECT id FROM threads WHERE migrated_from_number IS NOT NULL);

UPDATE issues
SET parent_thread_id = parent_issue_id, parent_issue_id = NULL
WHERE parent_issue_id IN (SELECT id FROM threads WHERE migrated_from_number IS NOT NULL);

DELETE FROM executions
WHERE issue_id IN (SELECT id FROM threads WHERE migrated_from_number IS NOT NULL);

DELETE FROM issue_labels
WHERE issue_id IN (SELECT id FROM threads WHERE migrated_from_number IS NOT NULL);

DELETE FROM issue_workspaces
WHERE issue_id IN (SELECT id FROM threads WHERE migrated_from_number IS NOT NULL);

DELETE FROM issues
WHERE id IN (SELECT id FROM threads WHERE migrated_from_number IS NOT NULL);

ALTER TABLE issues DROP COLUMN kind;