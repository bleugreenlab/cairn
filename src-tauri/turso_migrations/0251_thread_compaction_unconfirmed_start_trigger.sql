-- Name the compaction that replaces a session whose first launch never
-- confirmed (cairn/4978).
--
-- The provider was launched under the session's id and created a conversation
-- there, but the launch was rejected before its handle was confirmed. A retry
-- under the same id is then refused by the provider, so the thread rebuilds its
-- seed onto a fresh session. That is neither an operator's `manual` reset nor a
-- cache or capacity decision, and recording it as either would misstate why the
-- generation exists.
--
-- SQLite cannot alter a CHECK in place, so this is the ordinary
-- create-copy-drop-rename rebuild, keeping the column order the earlier
-- `ALTER TABLE ... ADD COLUMN` migrations produced. The table is private
-- per-runner state with no inbound foreign keys.
CREATE TABLE thread_compactions_new (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    source_session_id TEXT NOT NULL,
    compacted_through_block INTEGER,
    recency_start_block INTEGER NOT NULL,
    seed_bytes INTEGER,
    source_bytes INTEGER NOT NULL,
    candidate_bytes INTEGER NOT NULL,
    trigger TEXT NOT NULL CHECK (
        trigger IN ('expiry', 'ratio', 'capacity', 'manual', 'unconfirmed_start')
    ),
    created_at INTEGER NOT NULL,
    arc_bytes INTEGER,
    eras_through_seq INTEGER,
    PRIMARY KEY (job_id, generation)
);

INSERT INTO thread_compactions_new (
    job_id, generation, source_session_id, compacted_through_block,
    recency_start_block, seed_bytes, source_bytes, candidate_bytes, trigger,
    created_at, arc_bytes, eras_through_seq
)
SELECT job_id, generation, source_session_id, compacted_through_block,
       recency_start_block, seed_bytes, source_bytes, candidate_bytes, trigger,
       created_at, arc_bytes, eras_through_seq
FROM thread_compactions;

DROP TABLE thread_compactions;
ALTER TABLE thread_compactions_new RENAME TO thread_compactions;
