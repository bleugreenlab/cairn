-- lineage: shared
-- cairn/5584: `message_streams.final_event_id` references `events(id) ON DELETE
-- SET NULL`, so deleting an event runs
-- `UPDATE message_streams SET final_event_id = NULL WHERE final_event_id = ?`.
-- Without an index on the child column that action scans every stream: about
-- 95 ms per deleted event at 587k streams, which made every events delete (job,
-- issue and project cleanup, any evictor) a multi-second write transaction.
--
-- Shared: `message_streams` is ProjectScoped and both lineages carry it.
-- Full, not partial on `IS NOT NULL`: Turso does not infer `col IS NOT NULL`
-- from `col = ?`, so the FK action's lookup could not use a partial index.
CREATE INDEX IF NOT EXISTS idx_message_streams_final_event
    ON message_streams (final_event_id);
