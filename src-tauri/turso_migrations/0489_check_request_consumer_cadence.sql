-- lineage: shared
-- The consumer column is authority; requester_ref is only a display label.
-- Keep handoff/carried provenance supplied by the cadence that filed the row.
UPDATE check_requests
SET cadence = consumer
WHERE cadence IS NULL AND consumer IN ('requested', 'write', 'main_cadence');
