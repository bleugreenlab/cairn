-- CAIRN-4613: rotation used to write only `replaced_by_id` to the session it
-- replaced, so every predecessor a job ever rotated off still reads `open` with
-- no record of why it stopped being used. Closing them is safe by construction:
-- something already replaced the row, the job points at the successor, and no
-- path resumes through a replaced session.
--
-- The reason is the unspecific `rotated` because these rows never recorded which
-- rotation ended them; rows closed from here on name their own trigger
-- (`rotated_backend_change`, `rotated_reseed`, `rotated_compaction`,
-- `rotated_after_recovery`). `updated_at` is deliberately left alone: the row's
-- last real change was the replacement, and this backfill is recording that fact
-- rather than adding one.
UPDATE sessions
   SET status = 'closed',
       terminal_reason = 'rotated',
       closed_at = COALESCE(closed_at, updated_at)
 WHERE replaced_by_id IS NOT NULL
   AND status = 'open';
