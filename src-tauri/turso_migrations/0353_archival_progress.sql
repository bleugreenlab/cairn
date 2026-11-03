-- lineage: private
-- archival_progress: one row per archival maintenance pass (the sweep, and the
-- two phases of the archive offload), holding where the pass resumes. A
-- watermark is an integer bound (the sweep's resolution cutoff); a cursor is the
-- last key a paged pass finished (an execution id, a blob hash). The `archive`
-- row records the archive root; once set, boot never creates an empty archive
-- in place of a missing one, so the table is not clearable.
CREATE TABLE IF NOT EXISTS archival_progress (
    pass TEXT PRIMARY KEY,
    watermark INTEGER,
    cursor TEXT,
    updated_at INTEGER NOT NULL
);
