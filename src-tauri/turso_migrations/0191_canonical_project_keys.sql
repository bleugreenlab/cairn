-- Project keys are canonical lowercase identity. This update is intentionally
-- total: a case collision must abort rather than leave an unreachable row.
UPDATE projects SET key = lower(key) WHERE key <> lower(key);
