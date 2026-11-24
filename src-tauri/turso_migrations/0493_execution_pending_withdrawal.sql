-- lineage: shared
-- Filing authority is distinct from the credential resolver. Node and thread
-- ids survive run replacement; unknown legacy creators deliberately stay NULL.
ALTER TABLE executions ADD COLUMN filing_principal_kind TEXT;
ALTER TABLE executions ADD COLUMN filing_principal_id TEXT;

-- Only exact recorded thread identities are unambiguous without interpreting a
-- historical node URI. CLI/machinery labels and credential subjects prove no
-- authenticated filing authority.
UPDATE executions
   SET filing_principal_kind = 'thread',
       filing_principal_id = (
           SELECT t.id FROM recipe_resolutions r
           JOIN projects p ON p.id = r.project_id
           JOIN threads t ON t.project_id = p.id AND t.created_at <= r.created_at
            AND r.created_by = 'cairn://p/' || p.key || '/' || t.name
           WHERE r.execution_id = executions.id AND r.created_by_kind = 'thread'
       )
 WHERE id IN (
     SELECT r.execution_id FROM recipe_resolutions r
     JOIN projects p ON p.id = r.project_id
     JOIN threads t ON t.project_id = p.id AND t.created_at <= r.created_at
      AND r.created_by = 'cairn://p/' || p.key || '/' || t.name
     WHERE r.created_by_kind = 'thread'
 );

-- Record replacements before changing lifecycle. The keeper ordering is total,
-- including legacy NULL sequences and ties. No canonical or parallel row moves.
INSERT INTO execution_supersessions
    (id, issue_id, superseded_execution_id, successor_execution_id,
     reason, actor_kind, actor_ref, created_at)
SELECT 'pending-dedup:' || older.id, older.issue_id, older.id, keeper.id,
       'successor_created', 'system', 'migration:execution_pending_withdrawal', unixepoch()
  FROM executions older
  JOIN executions keeper ON keeper.issue_id = older.issue_id
   AND keeper.lifecycle = 'pending'
 WHERE older.lifecycle = 'pending' AND older.issue_id IS NOT NULL
   AND (COALESCE(keeper.seq, 0) > COALESCE(older.seq, 0)
     OR (COALESCE(keeper.seq, 0) = COALESCE(older.seq, 0) AND keeper.rowid > older.rowid))
   AND NOT EXISTS (
       SELECT 1 FROM executions newer
        WHERE newer.issue_id = keeper.issue_id AND newer.lifecycle = 'pending'
          AND (COALESCE(newer.seq, 0) > COALESCE(keeper.seq, 0)
            OR (COALESCE(newer.seq, 0) = COALESCE(keeper.seq, 0) AND newer.rowid > keeper.rowid))
   );

UPDATE executions SET lifecycle = 'superseded', status = 'superseded'
 WHERE lifecycle = 'pending' AND issue_id IS NOT NULL
   AND id IN (
       SELECT superseded_execution_id FROM execution_supersessions
        WHERE actor_kind = 'system' AND actor_ref = 'migration:execution_pending_withdrawal'
   );

CREATE UNIQUE INDEX idx_executions_one_pending_per_issue
    ON executions(issue_id)
 WHERE lifecycle = 'pending' AND issue_id IS NOT NULL;
