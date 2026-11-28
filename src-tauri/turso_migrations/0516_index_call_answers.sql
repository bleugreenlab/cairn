-- lineage: shared
-- cairn/7752: a turn's settlement answers, keyed by turn, for exactly the rows
-- that carry them.
--
-- Every resume composes a note from the calls a settlement answered on the
-- turn before it (`orchestrator::lifecycle::unanswered_calls::lost_calls_note`),
-- after nearly every turn. A settlement's answer is a `tool_result` whose
-- `raw` names its `reason`, and nothing else in the table marks it: the read
-- planned on `idx_events_type_run` by `event_type` alone and walked every
-- assistant and tool_result event in the workspace (2.87M on the 11-27
-- production database) to answer one turn. This index holds only answers, so
-- the read seeks the turn and fetches those rows, which on nearly every turn
-- is none.
--
-- The predicate must stay written exactly as `CALL_ANSWERS_SQL` spells it:
-- Turso substitutes a partial index only while the query's terms imply its
-- predicate. The `json_valid` guard keeps an insert of a body that is not JSON
-- from failing in `json_extract`.
--
-- Building it reads each existing tool_result once at startup.
CREATE INDEX idx_events_turn_call_answers
    ON events(turn_id, sequence)
    WHERE event_type = 'tool_result'
      AND CASE WHEN json_valid(data) THEN json_extract(data, '$.raw.reason') END IS NOT NULL;
