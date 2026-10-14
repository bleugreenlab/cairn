CREATE TABLE wake_conditions (
    id TEXT PRIMARY KEY,
    home_kind TEXT NOT NULL CHECK (home_kind IN ('thread')),
    home_id TEXT NOT NULL,
    sql TEXT NOT NULL,
    reason TEXT NOT NULL,
    once INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'muted', 'spent')),
    last_outcome TEXT CHECK (last_outcome IN ('matched', 'empty', 'error')),
    last_error TEXT,
    last_fired_at INTEGER,
    last_evaluated_at INTEGER,
    created_by TEXT NOT NULL CHECK (created_by IN ('agent', 'user', 'system')),
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE INDEX idx_wake_conditions_home ON wake_conditions(home_kind, home_id);
CREATE INDEX idx_wake_conditions_active ON wake_conditions(home_id) WHERE state = 'active';