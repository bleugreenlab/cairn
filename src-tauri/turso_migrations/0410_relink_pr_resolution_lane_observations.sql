-- lineage: shared
-- cairn/6368: migration 0393 removed each historical lane's jobId, requestId,
-- failureNames and failureKind, and those lanes cited no observation. This
-- relinks a lane to the observation that produced its verdict wherever the
-- surviving facts name exactly one: the resolution's merge request gives the
-- job, and the lane still carries its check name and ranAt. A reader recovers
-- the failure kind and the failing tests through that citation. A lane with no
-- ranAt, no match, or more than one match stays uncited, and a lane that
-- already cites (or says why it cannot) is left alone. requestId has no
-- surviving source and is not restored.
CREATE TABLE pr_resolution_lane_relinks (
    attribution_id TEXT NOT NULL,
    lane INTEGER NOT NULL,
    observation_id TEXT NOT NULL,
    PRIMARY KEY (attribution_id, lane)
);

-- Each candidate lane seeks idx_check_result_observations_job (0367).
INSERT INTO pr_resolution_lane_relinks (attribution_id, lane, observation_id)
SELECT a.id, lane.key, MIN(o.id)
  FROM pr_resolution_attributions a
  JOIN merge_requests m ON m.id = a.merge_request_id
  JOIN json_each(a.lane_snapshot, '$.systematicChecks') lane
  JOIN check_result_observations o
    ON o.job_id = m.job_id
   AND o.ran_at = json_extract(lane.value, '$.ranAt')
   AND o.check_name = json_extract(lane.value, '$.name')
 WHERE json_valid(a.lane_snapshot)
   AND json_type(a.lane_snapshot, '$.systematicChecks') = 'array'
   AND lane.type = 'object'
   AND json_type(lane.value, '$.ranAt') = 'integer'
   AND json_type(lane.value, '$.name') = 'text'
   AND COALESCE(json_type(lane.value, '$.observation'), 'null') = 'null'
   AND COALESCE(json_type(lane.value, '$.observationId'), 'null') = 'null'
 GROUP BY a.id, lane.key
HAVING COUNT(*) = 1;

-- The LEFT JOIN keeps json_each as the outer loop, so lanes keep their order.
-- json_each hands back an element's SQL value, so each element is rebuilt by
-- its JSON type: an unrelinked sibling of any shape is kept as it was.
UPDATE pr_resolution_attributions
   SET lane_snapshot = json_set(
           lane_snapshot,
           '$.systematicChecks',
           json((SELECT json_group_array(
                        CASE WHEN r.observation_id IS NOT NULL
                                  THEN json(json_set(lane.value, '$.observation',
                                           json(json_object('kind', 'cited',
                                                            'id', r.observation_id))))
                             WHEN lane.type IN ('object', 'array') THEN json(lane.value)
                             WHEN lane.type IN ('true', 'false') THEN json(lane.type)
                             ELSE lane.value
                        END)
                   FROM json_each(lane_snapshot, '$.systematicChecks') lane
                   LEFT JOIN pr_resolution_lane_relinks r
                     ON r.attribution_id = pr_resolution_attributions.id
                    AND r.lane = lane.key)))
 WHERE id IN (SELECT attribution_id FROM pr_resolution_lane_relinks);

DROP TABLE pr_resolution_lane_relinks;
