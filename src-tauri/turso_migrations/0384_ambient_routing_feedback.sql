-- lineage: private
-- The ambient Discord route's label set: one row per routing outcome, swept
-- from the fact log by the channels module, so the refused side of the floor
-- and an unlabelled delivery both have rows (cairn/6290). `recipient_thread`
-- is NULL for the one refused-side row per event. Runner-private beside
-- `fact_log` and `route_firings`, which it is swept from. Every timestamp is
-- epoch seconds; `event_at` is the fact's mint time, the 48-hour clock.
CREATE TABLE ambient_routing_feedback (
  id INTEGER PRIMARY KEY,
  project_key TEXT NOT NULL,
  event_id INTEGER NOT NULL,
  recipient_thread TEXT,
  outcome TEXT NOT NULL,
  pick TEXT,
  probability REAL,
  measured_by TEXT,
  revision TEXT,
  picks_json TEXT,
  addresses_from TEXT NOT NULL CHECK (addresses_from IN ('firings','decision_record','none','label')),
  verdict TEXT CHECK (verdict IN ('not_mine','reroute')),
  reroute_to TEXT,
  note TEXT,
  labelled_by TEXT,
  first_labelled_at INTEGER,
  labelled_at INTEGER,
  engaged INTEGER,
  engaged_by TEXT,
  engagement_checked_at INTEGER,
  event_at INTEGER NOT NULL,
  swept_at INTEGER NOT NULL,
  UNIQUE (project_key, event_id, recipient_thread)
);
CREATE UNIQUE INDEX idx_ambient_feedback_refused_side
  ON ambient_routing_feedback (project_key, event_id) WHERE recipient_thread IS NULL;
CREATE INDEX idx_ambient_feedback_engagement
  ON ambient_routing_feedback (engagement_checked_at, event_at);
-- The sweep's position in the fact log: the highest row id it has settled.
CREATE TABLE ambient_routing_feedback_cursor (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  fact_log_id INTEGER NOT NULL
);
