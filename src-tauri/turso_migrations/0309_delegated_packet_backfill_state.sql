-- lineage: private
-- Progress of the one-time move of inline snapshot packets into
-- `delegated_packets` (cairn/5597). The backfill walks `executions` by rowid,
-- one execution per write, and records how far it got so a restart resumes
-- rather than rescans. `completed_at` is set once a walk reaches the end, after
-- which boots skip it. Runner-transient, like `archival_backfill_state`.
CREATE TABLE delegated_packet_backfill_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    cursor_rowid INTEGER NOT NULL DEFAULT 0,
    moved INTEGER NOT NULL DEFAULT 0,
    completed_at INTEGER
);

INSERT INTO delegated_packet_backfill_state (id, cursor_rowid, moved, completed_at)
VALUES (1, 0, 0, NULL);
