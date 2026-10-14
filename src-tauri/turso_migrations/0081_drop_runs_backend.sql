-- CAIRN-2086: drop the dead runs.backend column.
--
-- runs.backend was NULL on every row -- production run inserts never set it, so
-- backend identity has always lived on sessions.backend (NOT NULL DEFAULT
-- 'claude'). Analytics and context-token accounting resolved the backend via
-- COALESCE(runs.backend, sessions.backend, 'claude'), which -- because the run
-- column was always NULL -- already collapsed to sessions.backend. Dropping the
-- column changes no resolved value; it only removes the vestigial source from
-- when Cairn was Claude-only.
--
-- No index, trigger, or view references runs.backend, so this is a clean
-- in-place ALTER (libsql supports ALTER TABLE ... DROP COLUMN on this pin; see
-- migrations 0032 and 0075). Inbound foreign keys reference runs.id, not
-- runs.backend, so no table rebuild is required.

ALTER TABLE runs DROP COLUMN backend;
