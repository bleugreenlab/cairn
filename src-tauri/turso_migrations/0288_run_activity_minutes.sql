-- Agent activity at minute grain: one row per run per UTC minute in which the
-- run emitted model output (an `assistant` event). Analytics' agent time is the
-- count of these minutes, so a run that is warm-parked, waiting on a question,
-- or holding a watch contributes nothing while it emits nothing. A minute fits
-- the event cadence: consecutive assistant events in a working run are under a
-- minute apart for the large majority of gaps.
--
-- Maintained by trigger so every event insert path (the live stream, sync,
-- project migration) keeps it current; the row is idempotent, so a re-insert
-- or a copied row never double-counts.
CREATE TABLE run_activity_minutes (
    run_id       TEXT NOT NULL,
    minute_start INTEGER NOT NULL,
    PRIMARY KEY (run_id, minute_start)
);

CREATE INDEX idx_run_activity_minutes_minute
    ON run_activity_minutes(minute_start);

CREATE TRIGGER run_activity_after_event_insert AFTER INSERT ON events
WHEN NEW.event_type = 'assistant'
BEGIN
  INSERT OR IGNORE INTO run_activity_minutes (run_id, minute_start)
  VALUES (NEW.run_id, (NEW.created_at / 60) * 60);
END;

INSERT OR IGNORE INTO run_activity_minutes (run_id, minute_start)
SELECT DISTINCT run_id, (created_at / 60) * 60
FROM events
WHERE event_type = 'assistant';
