-- lineage: shared
-- cairn/5848: the execution queue's order key. Every reader of the queue (the
-- sweep's release order, the places in line, the queue page) orders capacity
-- holds by `queue_key`, which is unique within a database, so a thread can
-- permute the keys its own cap-held set occupies without moving anyone else.
-- `created_at` stays the true age held. NULL on authored `after:` gates.
ALTER TABLE execution_gates ADD COLUMN queue_key INTEGER;

-- A capacity row's key is its second scaled by a million, plus its rank among
-- the capacity rows held in the same second: by issue number (none last), then
-- execution seq, then gate id, which is the order the sweep released ties in.
UPDATE execution_gates
   SET queue_key = created_at * 1000000 + (
       SELECT COUNT(*)
         FROM execution_gates o
         JOIN executions oe ON oe.id = o.execution_id
         LEFT JOIN issues oi ON oi.id = oe.issue_id
         JOIN executions xe ON xe.id = execution_gates.execution_id
         LEFT JOIN issues xi ON xi.id = xe.issue_id
        WHERE json_extract(o.predicate_json, '$.fact') IN ('fleet_slot', 'thread_focus')
          AND o.created_at = execution_gates.created_at
          AND (COALESCE(oi.number, 9223372036854775807) < COALESCE(xi.number, 9223372036854775807)
               OR (COALESCE(oi.number, 9223372036854775807) = COALESCE(xi.number, 9223372036854775807)
                   AND (COALESCE(oe.seq, 0) < COALESCE(xe.seq, 0)
                        OR (COALESCE(oe.seq, 0) = COALESCE(xe.seq, 0)
                            AND o.id < execution_gates.id)))))
 WHERE json_extract(predicate_json, '$.fact') IN ('fleet_slot', 'thread_focus');

-- A new hold's key is minted past the newest one, read through this index.
CREATE INDEX idx_execution_gates_queue_key ON execution_gates(queue_key);
