-- lineage: private
-- Routine and recovery evidence have different windows. Cover kind so each
-- bounded eviction read avoids payloads and does not assume append-time order.
CREATE INDEX idx_codex_watchdog_lifecycle_retention
    ON codex_watchdog_lifecycle(created_at, kind);
