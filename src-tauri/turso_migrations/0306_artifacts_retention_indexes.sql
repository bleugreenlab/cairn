-- lineage: shared
-- Indexes the retention evictor's arc rule needs (cairn-db storage/retention.rs),
-- in both lineages because `artifacts` is project-scoped.
--
-- `version`, `output_name` and `parent_version_id` sit after the `data` column,
-- so without an index every read of them walks each row's overflow pages: the
-- newest-version lookup behind every artifact write
-- (`insert_next_artifact_version`) read every prior version of its chain, 401 MB
-- for the largest thread arc on the 10-30 production clone. On that clone the
-- two indexes built in 1.4 s and 1.2 s with a warm cache and 9.3 s and 4.9 s
-- cold, which is this migration's one-time startup cost.
--
-- The parent index serves the self-reference check: `parent_version_id`
-- references `artifacts(id)`, so deleting a version looks up the rows that
-- point at it, and without the index that lookup scans the table (cairn/5584
-- is the same defect for `message_streams`).
CREATE INDEX idx_artifacts_output_job_version ON artifacts (output_name, job_id, version);
CREATE INDEX idx_artifacts_parent_version ON artifacts (parent_version_id);
