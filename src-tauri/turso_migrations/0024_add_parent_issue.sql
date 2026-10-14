ALTER TABLE issues ADD COLUMN parent_issue_id TEXT REFERENCES issues(id) ON DELETE SET NULL;
CREATE INDEX idx_issues_parent_id ON issues(parent_issue_id);