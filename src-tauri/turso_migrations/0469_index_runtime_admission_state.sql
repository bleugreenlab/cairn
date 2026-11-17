-- lineage: private
-- cairn/5864: the stale-run reaper asks which runs hold a queued, leased or
-- starting admission row (`pending_launch_run_ids`), and nothing here led with
-- `state`: `idx_runtime_admission_queue` leads with the workspace, and the
-- lease-expiry index is partial and leaves out `queued`. Each ask walked the
-- whole ledger history to find a pending set that is usually empty.
--
-- Plain rather than partial: a partial index serves `state IN (...)` only when
-- the planner proves the list implies its WHERE, which the pinned engine is not
-- known to do. Seeking each listed state keeps the cost at the pending rows.
-- `launch_key` carries the run id and `request_id` answers the team-authority
-- probe, so the reaper's read never opens a row.
--
-- Private-only: admission rows are runner-local.

CREATE INDEX idx_runtime_admission_state
    ON runtime_admission_requests(state, launch_key, request_id);
