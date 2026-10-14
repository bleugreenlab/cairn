-- Project keys are canonical lowercase. Only rewrite the key column itself;
-- persisted prose and URI text remain valid case-insensitive input.
UPDATE projects SET key = lower(key);