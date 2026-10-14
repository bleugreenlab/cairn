-- Immutable issue-creation provenance. The indexed principal is the queryable
-- projection; the decision-time appearance remains opaque typed JSON.
ALTER TABLE issues ADD COLUMN author_principal_json TEXT;
ALTER TABLE issues ADD COLUMN appearance_snapshot_json TEXT;

CREATE INDEX idx_issues_author_principal
  ON issues(project_id, author_principal_json, created_at DESC);

CREATE TRIGGER validate_issue_authorship_insert BEFORE INSERT ON issues
WHEN (NEW.author_principal_json IS NULL) != (NEW.appearance_snapshot_json IS NULL)
BEGIN SELECT RAISE(ABORT, 'issue authorship must contain both principal and appearance or neither'); END;

CREATE TRIGGER validate_issue_authorship_update BEFORE UPDATE OF author_principal_json, appearance_snapshot_json ON issues
WHEN (NEW.author_principal_json IS NULL) != (NEW.appearance_snapshot_json IS NULL)
BEGIN SELECT RAISE(ABORT, 'issue authorship must contain both principal and appearance or neither'); END;

CREATE TRIGGER preserve_issue_authorship_update BEFORE UPDATE OF author_principal_json, appearance_snapshot_json ON issues
WHEN NEW.author_principal_json IS NOT OLD.author_principal_json
  OR NEW.appearance_snapshot_json IS NOT OLD.appearance_snapshot_json
BEGIN SELECT RAISE(ABORT, 'issue authorship is immutable'); END;