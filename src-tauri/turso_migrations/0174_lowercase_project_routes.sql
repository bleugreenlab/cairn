-- The private routing catalog stores project keys rather than project ids.
UPDATE project_routes SET project_key = lower(project_key);