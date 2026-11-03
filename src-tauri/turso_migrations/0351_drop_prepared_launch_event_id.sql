-- lineage: private
-- cairn/5050: a prepared ordinary launch's prompt lives only in its `prompt`
-- column. The event id this column named was the turn's newest event, which
-- holds at most one part of an assembled continuation prompt and sometimes none.
ALTER TABLE prepared_ordinary_launches DROP COLUMN launch_event_id;
