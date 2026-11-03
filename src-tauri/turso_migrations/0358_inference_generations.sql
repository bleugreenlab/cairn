-- lineage: private
-- cairn/5885 (tenet 1364): generating a profile is an agent session, the
-- shipped `inference-generate` workflow launched from Settings > Inference.
--
-- `inference_generations` is one row per generation: the model and pool it
-- was started for, the workflow job and node running it (the node is the
-- provenance `run` of the profile it writes), its anchor issue, the draft
-- profile it has written, the hardware the operator stated for machines that
-- report none, and how it ended. `state` is `running` until the workflow
-- reports `verified`, `cannotRun` or `gaveUp`, the operator cancels it, or
-- its workflow ends without an outcome (`failed`); it has no CHECK so later
-- endings need no table rebuild. `draft_written_at` is when the draft was
-- last written, which a verification must start after to count, and
-- `approved_surface` the digest of the draft's runnable surface the operator
-- approved, without which the draft is not verified.
CREATE TABLE inference_generations (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    issue_id TEXT,
    job_id TEXT NOT NULL,
    node_uri TEXT NOT NULL,
    model TEXT NOT NULL,
    pool TEXT NOT NULL,
    state TEXT NOT NULL,
    draft_profile TEXT,
    draft_written_at INTEGER,
    approved_surface TEXT,
    hardware_json TEXT,
    outcome_json TEXT,
    started_at INTEGER NOT NULL,
    finished_at INTEGER
);

CREATE INDEX idx_inference_generations_job ON inference_generations (job_id, state);
