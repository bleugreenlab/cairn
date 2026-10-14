CREATE TABLE thread_automations (
    id                       TEXT PRIMARY KEY,
    thread_id                TEXT NOT NULL,
    procedure_name           TEXT NOT NULL,
    args_json                TEXT,
    every_ms                 INTEGER NOT NULL,
    anchor_at                INTEGER NOT NULL,
    state                    TEXT NOT NULL DEFAULT 'active'
                             CHECK (state IN ('active', 'muted')),
    last_fired_occurrence_at INTEGER,
    last_evaluated_at        INTEGER,
    last_run_id              TEXT,
    last_status              TEXT CHECK (last_status IN ('ok', 'failed')),
    last_error               TEXT,
    consecutive_failures     INTEGER NOT NULL DEFAULT 0,
    created_by               TEXT NOT NULL CHECK (created_by IN ('agent', 'user', 'system')),
    created_at               INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at               INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE INDEX idx_thread_automations_thread ON thread_automations(thread_id);
CREATE INDEX idx_thread_automations_active ON thread_automations(state) WHERE state = 'active';
