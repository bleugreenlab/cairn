-- lineage: shared
-- Hold age is provenance; queue order is independently mutable.
ALTER TABLE provider_limit_holds ADD COLUMN queue_key INTEGER;
CREATE INDEX idx_provider_limit_holds_queue_key
    ON provider_limit_holds(boundary_source, queue_key, created_at, id)
    WHERE state = 'active';
