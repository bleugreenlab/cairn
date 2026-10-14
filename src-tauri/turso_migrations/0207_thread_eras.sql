-- Persisted era summaries for thread seed composition (CAIRN-4302).
--
-- Era spans are immutable and generation-independent. Range URIs are derived
-- from live thread coordinates because threads can rename.
ALTER TABLE thread_compactions ADD COLUMN arc_bytes INTEGER;
ALTER TABLE thread_compactions ADD COLUMN eras_through_seq INTEGER;

CREATE TABLE thread_eras (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    start_block INTEGER NOT NULL,
    end_block INTEGER NOT NULL,
    started_at INTEGER NOT NULL,
    ended_at INTEGER NOT NULL,
    summary TEXT NOT NULL,
    summary_status TEXT NOT NULL CHECK (summary_status IN ('fallback', 'generated')),
    summary_model TEXT,
    source_tokens INTEGER NOT NULL,
    minted_generation INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    summarized_at INTEGER,
    PRIMARY KEY (job_id, seq)
);

CREATE TABLE thread_era_children (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    era_seq INTEGER NOT NULL,
    child_issue_id TEXT NOT NULL,
    child_issue_uri TEXT NOT NULL,
    child_title TEXT NOT NULL,
    final_status TEXT NOT NULL,
    PRIMARY KEY (job_id, era_seq, child_issue_id)
);

CREATE INDEX idx_thread_era_children_era
    ON thread_era_children(job_id, era_seq);
