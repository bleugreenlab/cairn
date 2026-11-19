-- lineage: shared
-- The dependency-wait count needs only unfinished packets for one parent.
-- Keep the predicate identical to the lookup so terminal history is excluded
-- and checking status never fetches the wide packet row.
CREATE INDEX idx_delegated_packets_pending_parent
    ON delegated_packets(parent_job_id)
    WHERE status NOT IN ('completed', 'failed', 'cancelled');
