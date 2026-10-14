-- Pinning a Discord state card is optional decoration, so its outcome is
-- recorded apart from the delivery retry state in `attempts`/`last_error`.
ALTER TABLE channel_state_card ADD COLUMN pin_attempted_at INTEGER;
ALTER TABLE channel_state_card ADD COLUMN pin_error TEXT;

-- Every existing card that carries a pin outcome has already spent its one
-- attempt: a success is recorded by `pinned`, and a failure sits in the
-- delivery slot because it used to be charged there. Backfilling both keeps
-- the first sweep after upgrade from re-pinning cards that are already pinned,
-- and lets channel health report the permission gap immediately rather than
-- waiting for a new card to be posted.
--
-- `created_at` dates the attempt, not `updated_at`: a card is pinned on the
-- first sweep after it is posted, while `updated_at` advances on every render
-- afterwards. Dating an old successful pin by its most recent edit would
-- manufacture a success newer than a genuinely later failure in the same
-- channel, and channel health reads exactly that ordering to decide a gap has
-- been repaired -- so the wrong column here would hide a live permission gap.
UPDATE channel_state_card
SET pin_attempted_at = created_at,
    pin_error = CASE WHEN pinned = 0 THEN last_error END
WHERE pinned = 1
   OR last_error LIKE 'could not pin%';

-- A failed pin used to fail the whole card, ending its live edits. Restore the
-- cards whose agents may still be running -- the last day on the table's own
-- clock -- rather than re-editing hundreds of long-finished historical threads.
UPDATE channel_state_card
SET status = 'live',
    attempts = 0,
    next_attempt_at = 0,
    last_error = NULL
WHERE status = 'failed'
  AND last_error LIKE 'could not pin%'
  AND updated_at >= (SELECT MAX(updated_at) FROM channel_state_card) - 86400;
