-- lineage: shared
-- cairn/6456: what branch replays actually cost, and the one durable
-- consequence a cost may have.
--
-- `replay_cost_samples` holds one versioned measurement per preparation that
-- reached detached computation: the coordinate, the native engine's own
-- phase observation, the preparation's phase timings, budget class, host,
-- load and outcome, as `evidence_json` (versioned; a row of an unknown
-- version is shown as unreadable, never guessed at). Descriptive only:
-- nothing is refused because of a sample. Bounded per project, newest kept
-- (orchestrator::base_advance::replay_cost::record_sample).
--
-- `replay_timeout_blockers` is separately keyed and never evicted by sample
-- retention: an explicit replay whose detached computation exhausted its
-- compute budget at exactly this (store, branch, head, base, mode, effective
-- resolution, engine version). An identical explicit attempt is refused before
-- computing. A row at any other head or base can never match again and is
-- deleted when the branch is next looked up; an install of the branch deletes
-- them all. Local runtime state: it names this runner's store path.
CREATE TABLE replay_cost_samples (
    id INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    store_path TEXT NOT NULL,
    branch TEXT NOT NULL,
    engine_version INTEGER NOT NULL,
    budget_class TEXT NOT NULL CHECK (budget_class IN ('explicit', 'background')),
    outcome TEXT NOT NULL,
    recorded_at INTEGER NOT NULL,
    evidence_json TEXT NOT NULL
);

CREATE INDEX idx_replay_cost_samples_branch
    ON replay_cost_samples (project_id, store_path, branch, recorded_at);
CREATE INDEX idx_replay_cost_samples_engine
    ON replay_cost_samples (project_id, engine_version, recorded_at);

CREATE TABLE replay_timeout_blockers (
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    store_path TEXT NOT NULL,
    branch TEXT NOT NULL,
    head TEXT NOT NULL,
    base TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('replay', 'merge')),
    resolution TEXT NOT NULL,
    engine_version INTEGER NOT NULL,
    base_branch TEXT NOT NULL,
    recorded_at INTEGER NOT NULL,
    evidence_json TEXT NOT NULL,
    PRIMARY KEY (project_id, store_path, branch, head, base, mode, resolution, engine_version)
);
