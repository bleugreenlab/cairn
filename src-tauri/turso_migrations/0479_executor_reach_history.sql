-- lineage: private
-- Independent of runner_events retention: the last report and lifetime union
-- must still describe a machine after its link and the journal rows are gone.
CREATE TABLE executor_reach_history (
    executor_id TEXT PRIMARY KEY NOT NULL,
    history_json TEXT NOT NULL
);
