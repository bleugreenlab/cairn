-- lineage: shared
-- cairn/5943: the execution queue releases lanes in rotation, least recently
-- served first. A lane is the parent that filed the work (`thread:<id>`,
-- `issue:<id>`, or `shared`), and this is when each lane last started work: a
-- turn stamp minted under the admission lock, unique and increasing across
-- every database, written in the same transaction as the release it records.
-- No backfill: a lane with no row has never been served, so the first round
-- falls back to each lane's oldest held execution, which is the old order.
CREATE TABLE queue_lane_turns (
    lane TEXT PRIMARY KEY,
    turn INTEGER NOT NULL,
    execution_id TEXT NOT NULL,
    served_at INTEGER NOT NULL
);
-- The newest stamp in this database, read by ordering down to it.
CREATE INDEX idx_queue_lane_turns_turn ON queue_lane_turns(turn);
