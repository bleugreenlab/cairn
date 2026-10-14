-- discord_surface is private-only, so its repair follows the shared projects
-- repair as a private-tail migration.
UPDATE discord_surface
SET project_key = lower(project_key)
WHERE project_key <> lower(project_key);
