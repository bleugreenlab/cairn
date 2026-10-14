CREATE INDEX idx_events_run_type_created ON events(run_id, event_type, created_at);
CREATE INDEX idx_messages_sender ON messages(sender_run_id) WHERE sender_run_id IS NOT NULL;
