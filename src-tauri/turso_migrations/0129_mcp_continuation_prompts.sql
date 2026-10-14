-- CAIRN-3265: explicitly bind internal MCP input prompts to their durable wait.
--
-- Ownership remains after consumption so replay can distinguish an already-consumed
-- MCP answer from an ordinary question. The timestamp is the atomic claim marker.
ALTER TABLE prompts ADD COLUMN mcp_wait_id TEXT REFERENCES agent_waits(id) ON DELETE CASCADE;
ALTER TABLE prompts ADD COLUMN mcp_consumed_at INTEGER;

CREATE INDEX idx_prompts_mcp_wait
  ON prompts(mcp_wait_id)
  WHERE mcp_wait_id IS NOT NULL;
