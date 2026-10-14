CREATE TABLE remote_intents (
  id TEXT PRIMARY KEY NOT NULL,
  execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN (
    'node_message',
    'permission_response',
    'prompt_response',
    'issue_comment'
  )),
  target_uri TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN (
    'pending', 'processing', 'succeeded', 'failed'
  )),
  claimed_by_device_id TEXT,
  claimed_at INTEGER,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  completed_at INTEGER,
  result_json TEXT,
  error TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE INDEX idx_remote_intents_pending
  ON remote_intents(status, created_at);
CREATE INDEX idx_remote_intents_execution
  ON remote_intents(execution_id);
