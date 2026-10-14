CREATE TABLE prompts_rebuilt (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  questions TEXT NOT NULL,
  response TEXT,
  created_at INTEGER NOT NULL,
  answered_at INTEGER,
  turn_id TEXT,
  uri_segment TEXT,
  job_id TEXT,
  tool_use_id TEXT,
  resolution_id TEXT,
  resolution_surface TEXT CHECK (resolution_surface IN ('resource_patch','channel_reply','remote_intent','non_operator_invoke','local_invoke','authenticated_operator','authenticated_desktop','legacy_unknown')),
  resolution_provider TEXT,
  resolution_conversation TEXT,
  resolution_actor TEXT,
  mcp_wait_id TEXT,
  mcp_consumed_at INTEGER,
  FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
  FOREIGN KEY (turn_id) REFERENCES turns(id),
  FOREIGN KEY (mcp_wait_id) REFERENCES agent_waits(id) ON DELETE CASCADE
);

INSERT INTO prompts_rebuilt (
  id, run_id, questions, response, created_at, answered_at, turn_id,
  uri_segment, job_id, tool_use_id, resolution_id, resolution_surface,
  resolution_provider, resolution_conversation, resolution_actor,
  mcp_wait_id, mcp_consumed_at
)
SELECT
  id, run_id, questions, response, created_at, answered_at, turn_id,
  uri_segment, job_id, tool_use_id, resolution_id, resolution_surface,
  resolution_provider, resolution_conversation, resolution_actor,
  mcp_wait_id, mcp_consumed_at
FROM prompts;

DROP TABLE prompts;
ALTER TABLE prompts_rebuilt RENAME TO prompts;

CREATE INDEX idx_prompts_run_id ON prompts(run_id);
CREATE INDEX idx_prompts_turn_id ON prompts(turn_id);
CREATE INDEX idx_prompts_uri_segment ON prompts(uri_segment);
CREATE UNIQUE INDEX idx_prompts_job_uri_segment
  ON prompts(job_id, uri_segment)
  WHERE job_id IS NOT NULL AND uri_segment IS NOT NULL;
CREATE INDEX idx_prompts_mcp_wait
  ON prompts(mcp_wait_id)
  WHERE mcp_wait_id IS NOT NULL;

CREATE TRIGGER validate_prompt_provenance_update BEFORE UPDATE OF resolution_surface,resolution_provider,resolution_conversation,resolution_actor ON prompts
WHEN (NEW.resolution_surface = 'channel_reply' AND (COALESCE(NEW.resolution_provider,'') = '' OR COALESCE(NEW.resolution_conversation,'') = '' OR COALESCE(NEW.resolution_actor,'') = ''))
  OR (NEW.resolution_surface <> 'channel_reply' AND (NEW.resolution_provider IS NOT NULL OR NEW.resolution_conversation IS NOT NULL))
  OR (NEW.resolution_surface = 'resource_patch' AND COALESCE(NEW.resolution_actor,'') = '')
BEGIN SELECT RAISE(ABORT, 'invalid prompt provenance'); END;
