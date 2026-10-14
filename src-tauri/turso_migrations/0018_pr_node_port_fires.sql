-- CAIRN-1214: PR nodes fire fixed lifecycle ports (`open`, `merge`, `close`).
-- Unlike condition_evaluations, a PR node can fire more than once over its
-- lifetime: `open` when the pull request is created, then exactly one terminal
-- resolution port.

CREATE TABLE IF NOT EXISTS pr_node_port_fires (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    recipe_node_id TEXT NOT NULL,
    port TEXT NOT NULL CHECK (port IN ('open', 'merge', 'close')),
    fired_at INTEGER NOT NULL,
    UNIQUE(execution_id, recipe_node_id, port)
);

CREATE INDEX idx_pr_node_port_fires_execution_node
    ON pr_node_port_fires(execution_id, recipe_node_id);
