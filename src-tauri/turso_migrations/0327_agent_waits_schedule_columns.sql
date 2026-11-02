-- lineage: shared
-- cairn/5866: take the durable-suspend scheduler off the `agent_waits` history.
--
-- The scheduler asked every row, every 250 ms, for `state` and fields of
-- `condition_json`. `state` is stored after the JSON and after
-- `resolution_json`, so testing it walked each row's overflow chain: the tick
-- cost the whole table's history rather than the handful of pending waits.
--
-- The schedule now lives in columns derived from the condition and the
-- `deadline_ms` column, written in the same statement as `condition_json` by
-- the one writer in `mcp::handlers::durable_suspend` (`ConditionColumns`):
--
-- * `kind`: the condition's serde tag.
-- * `due_at_ms`: the earliest moment an MCP continuation's own state makes it
--   due (`mcp_continuation::schedule` is the rule).
-- * `next_poll_at_ms`, `pending_prompt_id`: mirrored from the MCP state, for
--   the scheduler's ordering and its answered-prompt probe.
--
-- Only rows that can be read again are backfilled: active rows, and a
-- run-batch wait retired by a transfer, which `reclaim_from_inner_wait` can
-- return to `pending`. Terminal rows written before this migration keep a NULL
-- `kind`; rewriting them would copy their resolution payloads for no reader.
ALTER TABLE agent_waits ADD COLUMN kind TEXT;
ALTER TABLE agent_waits ADD COLUMN due_at_ms INTEGER;
ALTER TABLE agent_waits ADD COLUMN next_poll_at_ms INTEGER;
ALTER TABLE agent_waits ADD COLUMN pending_prompt_id TEXT;

UPDATE agent_waits
   SET kind = json_extract(condition_json, '$.kind')
 WHERE state IN ('pending', 'resolving')
    OR resolution_json = '{"outcome":"transferred_to_inner_wait"}';

-- The predicates are the scheduler's previous JSON expressions, so a row
-- written by an earlier host is due exactly when it was before, until its next
-- store rewrites the columns from the Rust rule.
UPDATE agent_waits
   SET next_poll_at_ms = json_extract(condition_json, '$.state.next_poll_at_ms'),
       pending_prompt_id = json_extract(condition_json, '$.state.pending_prompt_id'),
       due_at_ms = CASE
         WHEN json_type(condition_json, '$.state.arguments.__cairn_complete') IS NOT NULL THEN 0
         WHEN json_type(condition_json, '$.state.pending_operation') NOT IN ('null') THEN 0
         WHEN json_type(condition_json, '$.state.pending_input_requests') NOT IN ('null')
              AND json_type(condition_json, '$.state.pending_prompt_id') IN ('null') THEN 0
         WHEN json_type(condition_json, '$.state.task') NOT IN ('null') THEN
           CASE WHEN deadline_ms IS NULL
                  OR deadline_ms > COALESCE(json_extract(condition_json, '$.state.next_poll_at_ms'), 0)
                THEN COALESCE(json_extract(condition_json, '$.state.next_poll_at_ms'), 0)
                ELSE deadline_ms
           END
         ELSE deadline_ms
       END
 WHERE kind = 'mcp_continuation';

-- Every column the scheduler reads is in the index, so a tick never fetches
-- the table row and its overflow pages. The predicate names `kind` as well as
-- `state` so lookups by id with `state='pending'` cannot imply it and plan
-- through it instead of the primary key.
CREATE INDEX idx_agent_waits_mcp_due
  ON agent_waits(created_at, due_at_ms, next_poll_at_ms, deadline_ms, pending_prompt_id, id)
  WHERE state = 'pending' AND kind = 'mcp_continuation';

-- The stuck-resolution sweep's candidates, read every 30 s.
CREATE INDEX idx_agent_waits_resolving
  ON agent_waits(resolving_at, created_at)
  WHERE state = 'resolving';

-- No query spells `deadline_ms IS [NOT] NULL`, so neither could be used.
DROP INDEX IF EXISTS idx_agent_waits_pending_deadline;
DROP INDEX IF EXISTS idx_agent_waits_pending_terminal;
