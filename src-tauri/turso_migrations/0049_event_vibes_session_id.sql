ALTER TABLE event_vibes ADD COLUMN session_id TEXT;

UPDATE event_vibes
SET session_id = (
    SELECT e.session_id
    FROM events e
    WHERE e.id = event_vibes.event_id
);

CREATE INDEX idx_event_vibes_session ON event_vibes(session_id);
