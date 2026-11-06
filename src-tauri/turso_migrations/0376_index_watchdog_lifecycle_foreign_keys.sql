-- lineage: private
-- Full indexes on codex_watchdog_lifecycle's two foreign-key child columns, for
-- the reason docs/database.md "Deleting Rows With Dependents" gives: a session
-- delete cascades through session_id, and a run delete checks successor_run_id.
-- 0361 held them back because the build over the un-evicted table (about 1.0M
-- rows) cost 45 s of boot; the watchdog_lifecycle retention rule keeps the
-- table near a quarter of that.
CREATE INDEX idx_codex_watchdog_lifecycle_session_id
    ON codex_watchdog_lifecycle(session_id);
CREATE INDEX idx_codex_watchdog_lifecycle_successor_run_id
    ON codex_watchdog_lifecycle(successor_run_id);
