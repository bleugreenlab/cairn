-- lineage: shared
ALTER TABLE provider_limit_holds ADD COLUMN launch_failures_json TEXT NOT NULL DEFAULT '[]';

-- Legacy attempt-count gates carry no diagnosis; let the ordinary release path
-- re-evaluate them without changing their age or queue position.
UPDATE provider_limit_holds
SET resume_gate = NULL,
    retry_not_before = CAST(strftime('%s', 'now') AS INTEGER) + 300,
    updated_at = CAST(strftime('%s', 'now') AS INTEGER)
WHERE state = 'active' AND resume_gate = 'launch_failed'
  AND launch_failures_json = '[]';
