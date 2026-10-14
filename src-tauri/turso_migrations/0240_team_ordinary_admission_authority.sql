CREATE TABLE team_ordinary_admission_authority (
    request_id TEXT PRIMARY KEY NOT NULL REFERENCES prepared_ordinary_launches(request_id) ON DELETE CASCADE,
    lease_owner TEXT NOT NULL,
    lease_generation INTEGER NOT NULL,
    lease_host_id TEXT NOT NULL,
    pending_terminal_action TEXT CHECK (pending_terminal_action IN ('released','cancelled','failed')),
    pending_terminal_detail TEXT,
    updated_at INTEGER NOT NULL
);

CREATE INDEX idx_team_ordinary_admission_pending_terminal
ON team_ordinary_admission_authority(pending_terminal_action)
WHERE pending_terminal_action IS NOT NULL;
