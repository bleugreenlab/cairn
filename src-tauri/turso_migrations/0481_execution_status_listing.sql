-- lineage: shared
-- Status-filtered pages seek their lifecycle band before excluding launch holds.
-- The order key also serves pagination without reading historical snapshots.
CREATE INDEX idx_executions_status_listing
    ON executions(status, started_at DESC);
