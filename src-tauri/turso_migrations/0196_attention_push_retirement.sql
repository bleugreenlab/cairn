-- Retirement: the third durable state of an attention push (CAIRN-4182).
--
-- 0070 gave a push exactly two states: pending (delivered_event_id IS NULL) and
-- delivered. That binary is why the queue is monotonic. A push whose referent
-- has resolved -- the PR merged, the question answered, the permission decided
-- -- is filtered out of every drain by the liveness predicate, but nothing ever
-- writes that conclusion down, so the row stays pending forever and is
-- re-resolved on every subsequent drain. The oldest such `review:` row in
-- production is 55 days old, and 4,213 rows are undelivered across 1,124
-- recipients. CAIRN-4181 made re-resolving them cheap; it could not make them
-- stop accumulating, because retiring a row is a delivery-semantics change.
--
-- Retirement is INDEPENDENT of delivery, and that independence is the whole
-- design:
--
--   pending    undelivered and unretired -- the queue population
--   delivered  sealed by a durable carrying event (delivered_event_id)
--   retired    referent terminal; retained only as audit history
--   suspended  still pending, temporarily not deliverable (a dormant thread
--              recipient). NOT a state of this row -- suspension is a property
--              of the recipient, which is exactly why it must never be encoded
--              as retirement. Closing a thread and reopening it has to return
--              the SAME row to deliverability.
--
-- Retiring never sets delivered_event_id, never deletes a row, and never
-- advances a read cursor. A retired row is invisible to delivery and visible to
-- audit, forever.

-- Two nullable columns, added in place. A table rebuild is deliberately avoided:
-- `attention_pushes` is ProjectScoped, so it exists in the team lineage too, and
-- `team_schema_matches_private` compares the two lineages' DDL as text derived by
-- string replacement. A rebuild would have to reproduce the team snapshot's
-- foreign-key spelling exactly to stay convergent, whereas ALTER TABLE ADD COLUMN
-- applies identically to both and cannot drift.
ALTER TABLE attention_pushes ADD COLUMN retired_at INTEGER;

-- The reason vocabulary is the set of resolver outcomes that can prove a
-- referent terminal, and the both-or-neither clause makes a half-written
-- retirement unrepresentable rather than merely discouraged. SQLite evaluates
-- every CHECK on the table for every INSERT and UPDATE, not only the ones naming
-- the column being written, so this constraint covers a write that sets
-- `retired_at` alone just as it covers one that sets the reason alone.
--
-- Existing rows are not re-validated by ADD COLUMN, and they do not need to be:
-- both columns are NULL on every one of them, which satisfies the constraint.
ALTER TABLE attention_pushes ADD COLUMN retirement_reason TEXT
  CHECK (
    (retired_at IS NULL) = (retirement_reason IS NULL)
    AND (
      retirement_reason IS NULL
      OR retirement_reason IN (
        'review_resolved',
        'question_resolved',
        'permission_resolved'
      )
    )
  );

-- The pending-drain index. Its PARTIAL predicate, not its column list, is what
-- defines the queue population for the planner: the index physically contains
-- pending rows and nothing else, so a drain cannot walk history even in
-- principle. Delivered and retired rows leave it, which is what makes the
-- drain's cost track the LIVE backlog rather than the table.
--
-- Two engine facts decided this shape, both measured rather than assumed:
--
-- 1. `IS NULL` is NOT a seekable equality against an index column here. A plain
--    index on (recipient, delivered_event_id, retired_at, created_at) is used
--    only for `recipient=?`; the null checks stay residual and the ORDER BY
--    falls to `USE SORTER FOR ORDER BY`. Carrying the discriminating columns in
--    the key therefore buys nothing.
-- 2. Turso DOES use a partial index -- but only when it can see that the query's
--    predicate matches the index's. 0195's note about `idx_mr_issue` is the
--    narrower fact it looks like: that index's predicate was `issue_id IS NOT
--    NULL` while the query said `issue_id IN (...)`, and the planner will not
--    infer one from the other. It is predicate implication that is missing, not
--    partial-index support. Here the drain's WHERE clause is written to match
--    this predicate exactly, term for term, so the index is chosen.
--
-- `boundary` is deliberately absent from the key. It would sit between the seek
-- column and the ordering column and push `list_pending` back onto a sorter,
-- whereas as a residual filter it costs a comparison over a per-recipient set
-- that retirement now keeps small. (recipient, created_at, id) matches the
-- drain's `ORDER BY created_at ASC, id ASC` exactly, so neither drain sorts.
--
-- `storage::migrations::migration_0196_retirement_defines_the_pending_population`
-- asserts the plan for the exact production queries, because Turso keeps no
-- table statistics and a plan is the only evidence an index is load-bearing.
DROP INDEX IF EXISTS idx_attention_pushes_pending;
CREATE INDEX IF NOT EXISTS idx_attention_pushes_pending
  ON attention_pushes (recipient, created_at, id)
  WHERE delivered_event_id IS NULL AND retired_at IS NULL;

-- Supersede-by-key now covers only ACTIVE pending rows. This one stays partial
-- because it is a uniqueness CONSTRAINT rather than an access path -- enforcement
-- does not go through the query planner, so the planner's refusal to seek partial
-- indexes does not apply.
--
-- Narrowing it to `retired_at IS NULL` is what lets a genuinely new push for a
-- re-opened subject insert a fresh row: a retired same-key row has left the
-- index, exactly as a delivered one does, so it neither blocks the insert nor
-- gets superseded in place and silently resurrected.
DROP INDEX IF EXISTS idx_attention_pushes_supersede;
CREATE UNIQUE INDEX IF NOT EXISTS idx_attention_pushes_supersede
  ON attention_pushes (recipient, "key")
  WHERE delivered_event_id IS NULL AND retired_at IS NULL;
