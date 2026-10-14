CREATE TABLE route_firings (
  id              TEXT PRIMARY KEY,
  route_id        TEXT NOT NULL,
  scope_key       TEXT NOT NULL,
  seq             INTEGER NOT NULL,
  project_id      TEXT,
  trigger_source  TEXT NOT NULL,
  fact_identity   TEXT NOT NULL,
  status          TEXT NOT NULL CHECK(status IN ('fired','dropped','failed')),
  drop_reason     TEXT,
  transforms_json TEXT,
  sink_kind       TEXT NOT NULL,
  sink_ref        TEXT,
  error           TEXT,
  created_at      INTEGER NOT NULL,
  UNIQUE(scope_key, route_id, seq)
);
CREATE INDEX idx_route_firings_lookup
  ON route_firings(scope_key, route_id, seq DESC);
CREATE INDEX idx_route_firings_identity
  ON route_firings(scope_key, route_id, fact_identity, created_at DESC);
