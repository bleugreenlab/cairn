-- lineage: private
-- Which runner incarnations' execution queue entries a later runner has closed.
--
-- Each runner start closes the entries previous incarnations left non-terminal
-- (`fleet::queue::expire_previous_incarnations`). Whether an entry is still open
-- is a question about its LATEST journal row, which an append-only journal
-- cannot index: asked of the whole journal it costs a pass over every row, and
-- that cost grows with the journal forever (on a 577k-row journal every shape
-- of it took minutes, and the original self-join never finished). Asked of one
-- incarnation's rows it is a range of `idx_execution_queue_live`. This table is
-- what lets each start ask only about the incarnations no earlier start closed,
-- so its cost follows the previous process's activity, not the journal's age.
--
-- One row per closed incarnation, written in the same transaction as the
-- expiry rows it accounts for, so an incarnation is never marked closed
-- without them. Runner-local like the journal it indexes.
CREATE TABLE execution_queue_closed_incarnations (
    runner_incarnation TEXT PRIMARY KEY NOT NULL,
    -- The incarnation whose start closed it.
    closed_by TEXT NOT NULL,
    -- How many of its entries that start found open and expired.
    expired_entries INTEGER NOT NULL,
    closed_at INTEGER NOT NULL
);
