-- One canonical execution lineage per issue.
--
-- An issue's current attempt is owned by exactly one execution. Starting a new
-- ordinary execution IS the supersede decision: it fences every prior canonical
-- execution of that issue so no later wake, provider recovery, check settlement,
-- or stale runner callback can make the old one runnable or writable again.
--
-- Why `lifecycle` is a NEW column rather than a value of `executions.status`:
-- `status` is RECOMPUTED from job states by `transitions::recompute_execution_status`
-- and is overwritten whenever a job of that execution moves. The cairn/4658
-- specimen is the proof -- execution 1 reads `running` today with `completed_at`
-- still stamped at its failure, because the recompute flipped it back the moment
-- the resurrected job resumed. Outcome and ownership are two questions and need
-- two columns; nothing derives `lifecycle`.
ALTER TABLE executions ADD COLUMN lifecycle TEXT NOT NULL DEFAULT 'canonical';

-- All but the newest execution of each issue is superseded. This reproduces the
-- implicit rule the child-attention router already applied ("this job's
-- execution has a higher-seq sibling") as explicit durable state, so no live
-- issue changes behaviour at the moment of migration, and the partial unique
-- index below has something true to constrain.
--
-- The keeper set is computed in a SUBQUERY that correlates only among its own
-- aliases: the engine cannot compile a subquery that correlates back to the row
-- an UPDATE is writing. `rowid` breaks a `seq` tie so exactly one row per issue
-- survives as canonical even if two rows somehow share a sequence number.
UPDATE executions
   SET lifecycle = 'superseded'
 WHERE issue_id IS NOT NULL
   AND id NOT IN (
         SELECT keeper.id
           FROM executions keeper
          WHERE keeper.issue_id IS NOT NULL
            AND NOT EXISTS (
                  SELECT 1 FROM executions newer
                   WHERE newer.issue_id = keeper.issue_id
                     AND (COALESCE(newer.seq, 0) > COALESCE(keeper.seq, 0)
                       OR (COALESCE(newer.seq, 0) = COALESCE(keeper.seq, 0)
                           AND newer.rowid > keeper.rowid))
                )
       );

-- The durable record of who stopped owning an issue's attempt, when, and on
-- whose authority. cairn/4658 took a long investigation precisely because
-- nothing recorded the supersession; one row is what makes the next one a
-- lookup instead of an archaeology exercise.
CREATE TABLE execution_supersessions (
    id TEXT PRIMARY KEY,
    issue_id TEXT NOT NULL,
    superseded_execution_id TEXT NOT NULL,
    successor_execution_id TEXT,
    reason TEXT NOT NULL,
    actor_kind TEXT NOT NULL,
    actor_ref TEXT,
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_execution_supersessions_issue
    ON execution_supersessions(issue_id, created_at);

-- The invariant itself. At most one canonical execution per issue; a declared
-- parallel alternative and every superseded attempt are excluded, and
-- project-scoped executions (no issue) are unconstrained.
CREATE UNIQUE INDEX idx_executions_one_canonical_per_issue
    ON executions(issue_id)
 WHERE lifecycle = 'canonical' AND issue_id IS NOT NULL;
