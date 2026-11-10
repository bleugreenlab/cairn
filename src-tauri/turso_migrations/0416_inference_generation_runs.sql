-- lineage: private
-- Which runs an inference profile generation holds to its scope (cairn/6093).
-- A row is written for every local run on a generation's issue or job before
-- that run can authenticate: by the run's own launch, and for runs that
-- already existed, by the generation's launch. The dispatch-time scope check
-- reads only this table to decide whether a run is a generation's at all, so
-- a run with no row is ordinary and costs nothing more. There are no foreign
-- keys and rows are never deleted: erasing the run, its job or its issue must
-- not release a still-live process from its generation's scope, and a row
-- is never rewritten once it names a generation.
CREATE TABLE inference_generation_runs (
    run_id TEXT PRIMARY KEY,
    generation_id TEXT NOT NULL,
    job_id TEXT,
    issue_id TEXT,
    marked_at INTEGER NOT NULL
);

-- Marking a run finds the generation that names its issue.
CREATE INDEX idx_inference_generations_issue ON inference_generations (issue_id);
