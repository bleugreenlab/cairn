-- A rolled-back conflicting rebase is the only moment the three-way coordinates
-- exist: the rollback restores the branch, and a later probe finds a clean tree
-- with nothing left to enumerate. Persist the diagnostic captured in that window
-- so the resolution session outlives the reconcile pass that discovered it, and
-- so `cairn:~/rebase` can recompute both sides of the merge from immutable
-- objects rather than from a tree that no longer holds the conflict.
--
-- Coordinates and classification are stored; patches are not. The commits are
-- immutable, so any patch is recomputable, and storing one would age.
ALTER TABLE jj_reconcile_items ADD COLUMN diagnostic_version INTEGER;
ALTER TABLE jj_reconcile_items ADD COLUMN conflict_condition TEXT;
ALTER TABLE jj_reconcile_items ADD COLUMN base_commit TEXT;
ALTER TABLE jj_reconcile_items ADD COLUMN ours_commit TEXT;
ALTER TABLE jj_reconcile_items ADD COLUMN theirs_commit TEXT;
ALTER TABLE jj_reconcile_items ADD COLUMN conflicted_tip TEXT;
ALTER TABLE jj_reconcile_items ADD COLUMN incoming_pr_number INTEGER;
ALTER TABLE jj_reconcile_items ADD COLUMN incoming_issue TEXT;
ALTER TABLE jj_reconcile_items ADD COLUMN incoming_base_branch TEXT;

-- Marker state is what the executor CONFIRMED, never what was requested. A wake
-- may only name marker-bearing files once this reads 'materialized'.
ALTER TABLE jj_reconcile_items ADD COLUMN marker_state TEXT;
ALTER TABLE jj_reconcile_items ADD COLUMN marker_diagnostic TEXT;
ALTER TABLE jj_reconcile_items ADD COLUMN resolution_state TEXT;

-- The incoming change's COMPLETE file set, normalized. The per-file row is what
-- makes the cross-file trap visible: a conflict report that named only the
-- conflicting path let a coordinated multi-file change land half-applied.
CREATE TABLE jj_reconcile_incoming_files (
    intent_id TEXT NOT NULL REFERENCES jj_reconcile_intents(id) ON DELETE CASCADE,
    bookmark TEXT NOT NULL,
    path TEXT NOT NULL,
    status TEXT NOT NULL,
    classification TEXT NOT NULL
        CHECK (classification IN ('conflicting', 'clean_on_retry')),
    marker_disposition TEXT,
    PRIMARY KEY (intent_id, bookmark, path)
);

CREATE INDEX idx_jj_reconcile_incoming_files_session
    ON jj_reconcile_incoming_files(intent_id, bookmark, classification);

-- The resource resolves a session by branch, newest open first.
CREATE INDEX idx_jj_reconcile_items_session
    ON jj_reconcile_items(bookmark, resolution_state, updated_at);
