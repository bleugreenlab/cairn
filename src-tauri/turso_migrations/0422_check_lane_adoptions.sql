-- lineage: private
-- Runner-local ownership of remote check attempts, including retained results.
CREATE TABLE check_lane_adoptions (
    request_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    check_names TEXT NOT NULL,
    platform TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    input_hashes TEXT NOT NULL,
    consumer TEXT NOT NULL CHECK (consumer IN ('main_cadence','turn_end','manual','write')),
    consumer_ref TEXT NOT NULL,
    executor_id TEXT NOT NULL,
    machine TEXT NOT NULL,
    granted_at_ms INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('running','awaiting_adoption','joined','recorded','lost','cancelled')),
    stopped_at_ms INTEGER,
    evidence TEXT,
    observation_id TEXT,
    updated_ms INTEGER NOT NULL,
    recording_plans TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (request_id, attempt_id)
);
CREATE INDEX idx_check_lane_adoptions_state ON check_lane_adoptions(state);

CREATE TABLE check_lane_adoption_waiters (
    request_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    item_index INTEGER NOT NULL CHECK (item_index >= 0),
    consumer TEXT NOT NULL CHECK (consumer IN ('main_cadence','turn_end','manual','write')),
    consumer_ref TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('running','awaiting_adoption','joined','recorded','recorded_undelivered','delivered','lost','cancelled')),
    observation_id TEXT,
    evidence TEXT,
    updated_ms INTEGER NOT NULL,
    PRIMARY KEY (request_id,attempt_id,check_name,consumer,consumer_ref),
    FOREIGN KEY (request_id,attempt_id) REFERENCES check_lane_adoptions(request_id,attempt_id) ON DELETE CASCADE
);
CREATE INDEX idx_check_lane_adoption_waiters_state ON check_lane_adoption_waiters(state);
