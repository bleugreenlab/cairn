-- lineage: private
-- One runner-private label set for every decision-routed fact. No foreign keys:
-- facts and firings may be evicted while their labelled examples remain.
CREATE TABLE route_decision_feedback (
  id INTEGER PRIMARY KEY,
  project_key TEXT NOT NULL,
  event_id INTEGER NOT NULL,
  route_id TEXT NOT NULL,
  decision_node TEXT NOT NULL,
  question TEXT NOT NULL,
  -- Asked instructions when retained. A route-definition fallback is approximate:
  -- edits during the settle window can differ from the question originally asked.
  question_text TEXT,
  source TEXT NOT NULL,
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
  UNIQUE (project_key, event_id, route_id, decision_node, question, recipient_thread)
);
CREATE UNIQUE INDEX idx_route_feedback_refused_side
  ON route_decision_feedback (project_key, event_id, route_id, decision_node, question) WHERE recipient_thread IS NULL;
CREATE INDEX idx_route_feedback_engagement
  ON route_decision_feedback (engagement_checked_at, event_at);
-- The sweep's position in the fact log: the highest row id it has settled.
CREATE TABLE route_decision_feedback_cursor (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  fact_log_id INTEGER NOT NULL
);

INSERT INTO route_decision_feedback (id,project_key,event_id,recipient_thread,outcome,pick,probability,measured_by,revision,picks_json,addresses_from,verdict,reroute_to,note,labelled_by,first_labelled_at,labelled_at,engaged,engaged_by,engagement_checked_at,event_at,swept_at,route_id,decision_node,question,source)
SELECT id,project_key,event_id,recipient_thread,outcome,pick,probability,measured_by,revision,picks_json,addresses_from,verdict,reroute_to,note,labelled_by,first_labelled_at,labelled_at,engaged,engaged_by,engagement_checked_at,event_at,swept_at,'ambient-channel','pick','thread','channel_message' FROM ambient_routing_feedback;

-- Keep the settled position; existing ambient examples must not be swept again.
INSERT INTO route_decision_feedback_cursor (id, fact_log_id)
SELECT id, fact_log_id FROM ambient_routing_feedback_cursor;
DROP TABLE ambient_routing_feedback;
DROP TABLE ambient_routing_feedback_cursor;
