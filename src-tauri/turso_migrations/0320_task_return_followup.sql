-- lineage: shared
-- cairn/5783: a delegated task owes its parent a `return`. When a work turn
-- ends without one, Cairn sends the task one system re-prompt; when a later
-- work turn that saw that re-prompt also ends without one, the task fails as
-- having ended without returning.
--
-- `return_followup_turn_id` is the work turn whose end sent the re-prompt. It
-- is written once, so it both records that the re-prompt went out and lets a
-- replay of that same turn end recognize itself. `return_abandoned_at` is when
-- the task was given up on; the job-status derivation reads it, so the task
-- derives Failed instead of resting Blocked on its missing output. Both are
-- NULL for every job that has never ended a turn owing its result.
ALTER TABLE jobs ADD COLUMN return_followup_turn_id TEXT;
ALTER TABLE jobs ADD COLUMN return_abandoned_at INTEGER;
