-- lineage: private
-- cairn/6227: resolve a placement attempt id, a check lane's attempt id, or
-- the observation a lane recorded to its request without a pass over the
-- never-pruned queue journal or the lane table (`requests/<id>`).
CREATE INDEX idx_execution_queue_attempt ON execution_queue(attempt_id);
CREATE INDEX idx_check_lane_adoptions_attempt ON check_lane_adoptions(attempt_id);
CREATE INDEX idx_check_lane_adoptions_observation ON check_lane_adoptions(observation_id);
