-- lineage: shared
-- The resource-embedding sweep's latest-artifact read (cairn/6131,
-- `embeddings::resources::latest_artifacts`) walks every job-bound artifact
-- version to pick each chain's head. `artifacts` is a wide-row table: plan and
-- PR bodies sit in `data`, before `version` and `output_name`, and Turso
-- assembles a row's whole record, overflow pages included, to fetch any column
-- of it (cairn/5894). With no index holding all five columns the read was
-- `SCAN artifacts`: 35,577 versions on the 11-07 live census, each fetched whole.
--
-- This index carries every column the read projects, so the walk opens only
-- the index and never the table. It does not shrink the walk: the head of a
-- chain is decided over all history (a higher version created before the
-- boundary still suppresses a later lower one), so the read stays proportional
-- to job-bound artifact versions. `created_at` leads only so the key is a
-- useful order to inspect; nothing seeks on it. `id` and the implicit rowid
-- are carried for the result and its tie-break.
--
-- Partial on `job_id IS NOT NULL`, which the read repeats verbatim: Turso
-- substitutes a partial index only when the query's terms imply its predicate.
-- The read names the index with `INDEXED BY`, so the table walk cannot return
-- silently; that also makes this index required in both lineages.
--
-- The trade: a one-time build that reads every artifact row once at startup,
-- then one more index entry to write and store per artifact version, in
-- exchange for not reading every wide row on each sweep. The build cost on a
-- production-sized database was not measured; 0335's comparable artifact index
-- took 49.6 s on the 11-02 clone.
CREATE INDEX IF NOT EXISTS idx_artifacts_embedding_scan
    ON artifacts (
        created_at, job_id, COALESCE(output_name, artifact_type), version, id
    ) WHERE job_id IS NOT NULL;
