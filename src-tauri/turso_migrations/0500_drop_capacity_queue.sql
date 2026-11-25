-- lineage: shared
-- The run queue replaces system-filed capacity gates and lane rotation.
-- Authored after-gates and their early-release audit remain intact.
DELETE FROM execution_gates
 WHERE json_extract(predicate_json, '$.fact') IN ('fleet_slot', 'thread_focus');
DROP TABLE queue_lane_turns;
DROP INDEX idx_execution_gates_queue_key;
ALTER TABLE execution_gates DROP COLUMN queue_key;
ALTER TABLE execution_gates DROP COLUMN detail;
-- threads.max_open_children was already dropped by migration 0323.
-- executions.release_override_json still records authored-gate early release.
