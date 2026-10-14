-- Per-project disable of inherited workspace config artifacts.
--
-- A row here suppresses one inherited workspace artifact for a single project
-- WITHOUT copying or redefining it. Covers the file/DB config types
-- (recipe | agent | skill | action). MCP servers keep their own yaml
-- `enabled: false` flag and are not represented here.
--
-- config_key is whatever the type shadows on: the id (file stem) for
-- recipes/agents/skills, the action name for actions.
CREATE TABLE config_disables (
    project_id  TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    config_key  TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    PRIMARY KEY (project_id, entity_type, config_key)
);
