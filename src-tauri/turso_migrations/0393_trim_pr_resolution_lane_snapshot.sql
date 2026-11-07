-- lineage: shared
-- cairn/6257: a PR resolution's attribution copied the producing execution's
-- whole snapshot (every agent's prompt, the recipe graph, every skill) into
-- lane_snapshot, about 215 KB a row, and each check lane's full status with
-- its output tail. New rows name the execution by id and keep only each
-- lane's identity, state, counts and verified head. This shrinks the rows
-- journaled before that to the same fields; the execution's own resource
-- still holds its snapshot.
UPDATE pr_resolution_attributions
   SET lane_snapshot = json_remove(lane_snapshot, '$.execution.executionSnapshot')
 WHERE json_valid(lane_snapshot)
   AND json_type(lane_snapshot, '$.execution.executionSnapshot') IS NOT NULL;

UPDATE pr_resolution_attributions
   SET lane_snapshot = json_set(
           lane_snapshot,
           '$.systematicChecks',
           json((SELECT json_group_array(json(json_remove(
                        value,
                        '$.jobId', '$.requestId', '$.outputTail', '$.failureNames',
                        '$.failureKind', '$.notice', '$.declaredUnverified',
                        '$.progress', '$.baseAttribution', '$.evidence',
                        '$.planAuditNote', '$.selection', '$.parser',
                        '$.executor', '$.verifiedOn.base')))
                   FROM json_each(lane_snapshot, '$.systematicChecks'))))
 WHERE json_valid(lane_snapshot)
   AND json_type(lane_snapshot, '$.systematicChecks') = 'array'
   AND json_array_length(lane_snapshot, '$.systematicChecks') > 0;
