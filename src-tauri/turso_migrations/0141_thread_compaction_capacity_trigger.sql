-- Rename the warm thread-compaction trigger to what it now measures (CAIRN-3427).
--
-- `ratio` named a comparison between the dropped prefix and its own chapter
-- lines. Both were properties of the dropped range, so the comparison was
-- scale-free and fired whenever anything at all became compactable. The warm
-- trigger now fires on context-window occupancy instead: a rebuild costs a
-- thread its own reasoning and its continuity, and no byte saving offsets that,
-- so while the cache is live it is done only because the window is running out.
--
-- `ratio` stays in the constraint. Those rows are a true record of rotations
-- that really did happen under the old rule, and rewriting them to `capacity`
-- would claim a measurement that was never taken. No new row uses it.
--
-- SQLite cannot alter a CHECK in place, so this is the ordinary
-- create-copy-drop-rename rebuild. The table is private per-runner state with no
-- inbound foreign keys, so nothing else has to move with it.
CREATE TABLE thread_compactions_new (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    source_session_id TEXT NOT NULL,
    compacted_through_block INTEGER,
    recency_start_block INTEGER NOT NULL,
    seed_bytes INTEGER,
    source_bytes INTEGER NOT NULL,
    candidate_bytes INTEGER NOT NULL,
    trigger TEXT NOT NULL CHECK (trigger IN ('expiry', 'ratio', 'capacity', 'manual')),
    created_at INTEGER NOT NULL,
    PRIMARY KEY (job_id, generation)
);

INSERT INTO thread_compactions_new (
    job_id, generation, source_session_id, compacted_through_block,
    recency_start_block, seed_bytes, source_bytes, candidate_bytes, trigger, created_at
)
SELECT job_id, generation, source_session_id, compacted_through_block,
       recency_start_block, seed_bytes, source_bytes, candidate_bytes, trigger, created_at
FROM thread_compactions;

DROP TABLE thread_compactions;
ALTER TABLE thread_compactions_new RENAME TO thread_compactions;
