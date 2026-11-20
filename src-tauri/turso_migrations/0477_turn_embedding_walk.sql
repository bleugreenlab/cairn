-- lineage: private
-- cairn/7351: the turn sweep walks `turns` in rowid order from durable cursors
-- instead of re-deriving its pending set with an anti-join over every turn on
-- every pass.
--
-- `turn_walk_head` is the highest rowid the forward (live) walk has read.
-- `turn_walk_tail` is where the backward (history) walk resumes, reading rowids
-- below it, and is 0 once that walk has reached the oldest turn. NULL means no
-- walk has started in the recorded space; a switch of space clears both.
-- `turn_walk_open` is a JSON array of the turns a cursor passed before they
-- could be settled (still running, or a call or write that failed), which the
-- forward walk re-reads every pass. `turn_walk_consented` records whether every
-- pass so far had history consent: gaining it restarts the backward walk, so
-- the pre-boundary turns it passed over are reached. `turn_walk_epoch` is bumped
-- by every reset, so a pass that read the state before a switch cannot write its
-- cursors back over the reset.
--
-- Private-only, like the rest of `embedding_state`.

ALTER TABLE embedding_state ADD COLUMN turn_walk_head INTEGER;
ALTER TABLE embedding_state ADD COLUMN turn_walk_tail INTEGER;
ALTER TABLE embedding_state ADD COLUMN turn_walk_open TEXT NOT NULL DEFAULT '[]';
ALTER TABLE embedding_state ADD COLUMN turn_walk_consented INTEGER NOT NULL DEFAULT 0;
ALTER TABLE embedding_state ADD COLUMN turn_walk_epoch INTEGER NOT NULL DEFAULT 0;

-- The deferred retry rotates through the `dims = -1` markers in turn-id order.
-- Through `idx_turn_embeddings_space` that seeks every row from its cursor until
-- it has a page of markers: at about 850 markers among 175k rows, most of the
-- index per page. Only the markers are indexed here.
CREATE INDEX idx_turn_embeddings_deferred ON turn_embeddings(turn_id) WHERE dims = -1;
