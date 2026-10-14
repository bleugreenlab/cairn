CREATE TABLE response_invocations (
  id              TEXT PRIMARY KEY,
  response_id     TEXT NOT NULL,
  scope_key       TEXT NOT NULL,
  seq             INTEGER NOT NULL,
  project_id      TEXT,
  caller_kind     TEXT NOT NULL CHECK(caller_kind IN ('internal','agent','workflow')),
  caller_label    TEXT,
  caller_run_id   TEXT,
  rendered_prompt TEXT NOT NULL,
  args_json       TEXT,
  status          TEXT NOT NULL CHECK(status IN ('ok','failed')),
  output_text     TEXT,
  error           TEXT,
  model           TEXT,
  backend         TEXT,
  latency_ms      INTEGER,
  input_tokens    INTEGER,
  output_tokens   INTEGER,
  cost            REAL,
  created_at      INTEGER NOT NULL,
  UNIQUE(scope_key, response_id, seq)
);
CREATE INDEX idx_response_invocations_lookup
  ON response_invocations(scope_key, response_id, seq DESC);