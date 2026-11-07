-- lineage: shared
-- A held descendant must replay only its own commits, not obsolete parent history.
-- A tip without a cut is a durable refusal to guess the missing boundary.
ALTER TABLE jj_reconcile_items ADD COLUMN held_replay_source_cut TEXT;
ALTER TABLE jj_reconcile_items ADD COLUMN held_replay_source_tip TEXT;
