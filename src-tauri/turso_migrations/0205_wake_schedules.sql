CREATE TABLE wake_schedules (
    id TEXT PRIMARY KEY,
    home_kind TEXT NOT NULL CHECK (home_kind IN ('thread')),
    home_id TEXT NOT NULL,
    every_ms INTEGER NOT NULL,
    anchor_at INTEGER NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'muted')),
    condition_json TEXT,
    last_fired_occurrence_at INTEGER,
    last_evaluated_at INTEGER,
    created_by TEXT NOT NULL CHECK (created_by IN ('agent', 'user', 'system')),
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE INDEX idx_wake_schedules_home
    ON wake_schedules(home_kind, home_id);

CREATE INDEX idx_wake_schedules_active
    ON wake_schedules(state) WHERE state = 'active';
