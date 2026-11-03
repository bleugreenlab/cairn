-- lineage: shared
-- cairn/5898: the retention evictor walks settled agent waits by when they
-- settled. Partial on the terminal states, spelled exactly as the walk spells
-- them, so the planner substitutes it; created_at serves the walk over rows
-- that settled without a settle time. Both columns sit after the large
-- condition_json and resolution_json, so the build reads each record once.
CREATE INDEX idx_agent_waits_settled
  ON agent_waits(resolved_at, created_at)
  WHERE state IN ('resolved', 'cancelled');

-- Deleting a wait cascades into the MCP prompts bound to it. The pinned engine
-- finds those children through an ordinary index on the child key but not a
-- partial one, so with 0129's `WHERE mcp_wait_id IS NOT NULL` every wait delete
-- scanned the whole of `prompts`. Equality lookups by wait keep the same index.
DROP INDEX IF EXISTS idx_prompts_mcp_wait;
CREATE INDEX idx_prompts_mcp_wait ON prompts(mcp_wait_id);
