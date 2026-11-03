-- lineage: shared
-- cairn/6025: the proposing thread's disposition of one evidence row, "judged,
-- stands". The judge can misread concluded work; a thread that reads the
-- contradiction wake's evidence and judges the tenet stands records that on
-- the row itself: when, which thread, and why. The row is never deleted or
-- re-signed, because a dismissal with its reason is, with supersessions, the
-- only production label the judge gets (cairn/6024).
--
-- The three columns are set together, once, by `tenets::evidence::dismissal`.
-- A dismissed contradiction no longer counts toward the wake, so the wake's
-- partial index is rebuilt to match the predicate its queries spell.
ALTER TABLE tenet_evidence ADD COLUMN dismissed_at INTEGER;
ALTER TABLE tenet_evidence ADD COLUMN dismissed_by_thread_id TEXT REFERENCES threads(id);
ALTER TABLE tenet_evidence ADD COLUMN dismissed_reason TEXT;

DROP INDEX tenet_evidence_unconsumed;
CREATE INDEX tenet_evidence_unconsumed ON tenet_evidence(tenet_id)
  WHERE sign = 'contradicts' AND consumed_by_wake IS NULL AND dismissed_at IS NULL;
