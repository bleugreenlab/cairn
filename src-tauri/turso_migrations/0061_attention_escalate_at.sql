-- CAIRN-1647: pending-blocker escalation timer.
--
-- A question/permission item opens passive (folds into the parent's next
-- briefing, does not wake) and arms a one-shot escalation for now + the
-- workspace `pendingBlockerTimeoutSecs`. If the user answers within the window
-- the item resolves and the timer no-ops; if it's still open at the deadline a
-- background worker escalates it to a steering wake so an autonomous parent
-- isn't stalled forever. escalate_at is the absolute deadline (NULL once
-- consumed or for non-blocker items).
ALTER TABLE attention_items ADD COLUMN escalate_at INTEGER;

-- The escalation worker scans for the soonest / due deadlines; a partial index
-- keeps that O(armed blockers), not a full-table scan.
CREATE INDEX idx_attention_items_escalation
  ON attention_items(escalate_at)
  WHERE escalate_at IS NOT NULL;
