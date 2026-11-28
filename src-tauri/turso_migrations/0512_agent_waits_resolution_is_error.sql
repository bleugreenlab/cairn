-- lineage: shared
-- A resolved call's answer is either an ordinary tool result or a failed one
-- (a run batch whose item exited non-zero, timed out, or was lost). The text
-- alone cannot say which, so it is recorded beside it, and a replay of a
-- resolving row answers exactly as the live resolver would have.
ALTER TABLE agent_waits ADD COLUMN resolution_is_error INTEGER NOT NULL DEFAULT 0;
