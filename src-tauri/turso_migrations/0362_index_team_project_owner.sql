-- lineage: team
-- cairn/5956: the team lineage roots `projects` at `teams`, so the owner key
-- private indexes as `idx_projects_workspace_id` is `team_id` here.
CREATE INDEX idx_projects_team_id ON projects(team_id);
