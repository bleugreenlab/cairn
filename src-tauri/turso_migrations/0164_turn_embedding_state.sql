-- Where this install's own participation in the transcript begins (CAIRN-3810).
--
-- The semantic lane's sweep separates two things that are easy to conflate:
--
--   * CONSENT. Uploading transcripts that predate this install is irreversible
--     and requires a connected account.
--   * CORRECTNESS. The region this install watched happen must be reconciled,
--     because a turn can miss the pass that reaches it — it was still running,
--     its events would not reconstruct, or its write failed — and the sweep's
--     fast scan only looks above the newest embedded turn. Reconciling that
--     region is not a history upload and needs no account.
--
-- Those are orthogonal, and one flag cannot carry both: gating reconciliation
-- on the account gate would strand turns forever on exactly the installs the
-- consent split exists to protect. This boundary is what lets them separate.
-- It is written once, when the sweep first runs, and never moves.

CREATE TABLE turn_embedding_state (
    id                INTEGER PRIMARY KEY CHECK (id = 1),
    -- `turns.created_at` at the moment this install first swept. Turns at or
    -- after it are this install's own; older ones are pre-existing history.
    install_boundary  INTEGER,
    created_at        INTEGER NOT NULL
);

INSERT INTO turn_embedding_state (id, install_boundary, created_at)
VALUES (1, NULL, 0);
