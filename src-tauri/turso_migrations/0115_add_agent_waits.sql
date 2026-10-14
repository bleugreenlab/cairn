-- Migration: add agent waits

CREATE TABLE agent_waits (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  predecessor_turn_id TEXT NOT NULL REFERENCES turns(id) ON DELETE CASCADE,
  tool_use_id TEXT NOT NULL,
  condition_json TEXT NOT NULL,
  deadline_ms INTEGER,
  state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'resolving', 'resolved', 'cancelled')),
  resolution_json TEXT,
  successor_turn_id TEXT REFERENCES turns(id) ON DELETE SET NULL,
  result_stored_at INTEGER,
  created_at INTEGER NOT NULL,
  resolved_at INTEGER,
  UNIQUE(run_id, tool_use_id)
);
CREATE UNIQUE INDEX idx_agent_waits_one_pending_job ON agent_waits(job_id) WHERE state IN ('pending', 'resolving');
CREATE INDEX idx_agent_waits_pending_deadline ON agent_waits(deadline_ms) WHERE state = 'pending' AND deadline_ms IS NOT NULL;
CREATE INDEX idx_agent_waits_pending_terminal ON agent_waits(state) WHERE state = 'pending' AND deadline_ms IS NULL;
