-- CAIRN-3327: an executor's public name is its address, so the runner records
-- which machine holds each name and refuses a second claimant.
--
-- Existing rows are deliberately left unnamed. This table has never held a name:
-- an executor's address comes from what it advertises, and manufacturing one
-- here from the opaque enrollment identity would write a name no machine answers
-- to and then reject the legitimate reconnection that presented the real one. An
-- unnamed row is unclaimed, and the next connection claims it.
ALTER TABLE executor_enrollments ADD COLUMN executor_name TEXT;

-- Active enrollments only. A revoked machine releases its name for whatever
-- takes its place, and an unnamed row constrains nothing.
CREATE UNIQUE INDEX IF NOT EXISTS executor_enrollments_active_name
    ON executor_enrollments (executor_name)
    WHERE executor_name IS NOT NULL AND revoked_at IS NULL;
