ALTER TABLE events ADD COLUMN change_preview_status TEXT;
ALTER TABLE events ADD COLUMN change_applied_at INTEGER;

CREATE INDEX idx_events_change_preview_status ON events(change_preview_status);
