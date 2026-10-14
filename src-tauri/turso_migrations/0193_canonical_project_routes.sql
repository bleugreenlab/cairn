-- Routes can be minted after the historical 0174 repair. Keep the private
-- routing catalog aligned with canonical lowercase project identity.
UPDATE project_routes
SET project_key = lower(project_key)
WHERE project_key <> lower(project_key);