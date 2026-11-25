-- lineage: private
-- cairn/7546: the run queue. One ordered list per runner of every issue
-- execution it wants to run, running ones included. Position (`rank`) and the
-- two caps (global, per thread) decide what runs; nothing else does. A row
-- with `parked_at` set is parked: out of the list until unparked. A row leaves
-- when its execution stops being live (superseded, withdrawn, or its issue
-- merged or closed). Rows reference executions in whichever database owns
-- them, so there is no foreign key.
CREATE TABLE run_queue (
    execution_id TEXT PRIMARY KEY NOT NULL,
    issue_id TEXT NOT NULL,
    rank INTEGER NOT NULL,
    parked_at INTEGER,
    added_at INTEGER NOT NULL
);
CREATE INDEX idx_run_queue_rank ON run_queue(rank, execution_id);
