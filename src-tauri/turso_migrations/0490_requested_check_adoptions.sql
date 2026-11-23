-- lineage: private
-- Preserve physical attempts and their consumers while making requested identity explicit.
CREATE TABLE check_lane_adoptions_requested (
    request_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    check_names TEXT NOT NULL,
    platform TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    input_hashes TEXT NOT NULL,
    consumer TEXT NOT NULL CHECK (consumer IN ('main_cadence','turn_end','requested','write')),
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
INSERT INTO check_lane_adoptions_requested
SELECT request_id,attempt_id,project_id,check_names,platform,commit_sha,input_hashes,
       CASE consumer WHEN 'manual' THEN 'requested' ELSE consumer END,
       consumer_ref,executor_id,machine,granted_at_ms,state,stopped_at_ms,evidence,observation_id,updated_ms,
       recording_plans
FROM check_lane_adoptions;

CREATE TABLE check_lane_adoption_waiters_requested (
    request_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    item_index INTEGER NOT NULL CHECK (item_index >= 0),
    consumer TEXT NOT NULL CHECK (consumer IN ('main_cadence','turn_end','requested','write')),
    consumer_ref TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('running','awaiting_adoption','joined','recorded','recorded_undelivered','delivered','lost','cancelled')),
    observation_id TEXT,
    evidence TEXT,
    updated_ms INTEGER NOT NULL,
    PRIMARY KEY (request_id,attempt_id,check_name,consumer,consumer_ref),
    FOREIGN KEY (request_id,attempt_id) REFERENCES check_lane_adoptions_requested(request_id,attempt_id) ON DELETE CASCADE
);
INSERT INTO check_lane_adoption_waiters_requested
SELECT request_id,attempt_id,check_name,item_index,
       CASE consumer WHEN 'manual' THEN 'requested' ELSE consumer END,
       consumer_ref,state,observation_id,evidence,updated_ms
FROM check_lane_adoption_waiters;

DROP TABLE check_lane_adoption_waiters;
DROP TABLE check_lane_adoptions;
ALTER TABLE check_lane_adoptions_requested RENAME TO check_lane_adoptions;
ALTER TABLE check_lane_adoption_waiters_requested RENAME TO check_lane_adoption_waiters;
CREATE INDEX idx_check_lane_adoptions_state ON check_lane_adoptions(state);
CREATE INDEX idx_check_lane_adoption_waiters_state ON check_lane_adoption_waiters(state);

-- Old plans inherit the physical producer's durable authority, never a stream prefix.
-- Keep null entries and existing explicit metadata intact.
UPDATE check_lane_adoptions
SET recording_plans = (
    SELECT json_group_array(json(
        CASE WHEN json_type(value) = 'object' AND json_type(value, '$.producer') IS NULL
        THEN json_set(value, '$.producer', json_object(
            'consumer', check_lane_adoptions.consumer,
            'consumer_ref', check_lane_adoptions.consumer_ref,
            'request_id', CASE WHEN check_lane_adoptions.consumer = 'requested'
                               THEN check_lane_adoptions.consumer_ref ELSE NULL END))
        ELSE value END))
    FROM json_each(check_lane_adoptions.recording_plans)
)
WHERE json_array_length(recording_plans) > 0;
