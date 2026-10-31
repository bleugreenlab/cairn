-- lineage: private
-- The cairn-inference pool controller (cairn/5661, tenet 1321): the orch
-- decides by itself which deployment set each controlled pool runs.
--
-- `inference_pool_control` holds one row per pool that has ever been taken
-- under Cairn's control: the facet's `PoolControl` serde form (whether the
-- controller drives the pool, the operator's settings, Cairn's pin with its
-- holder, and the controller's active set with why). The active set is the
-- pool's desired state, which the act side converges on and placement reacts
-- to. `generation` mirrors the record's own and advances only when what the
-- pool wants changes, so a consumer watches it instead of re-reading every
-- tick's bookkeeping. Kept apart from `inference_pools`, whose rows mirror
-- bginf and are replaced by every re-import.
--
-- `inference_requests` is the queue of callers asking for a model: each row
-- names the one deployment it asked for and waits until that deployment is
-- healthy, fails, or is cancelled. Answered rows are demand history for the
-- idle windows; the controller deletes them a week after they were answered.
-- Times are epoch milliseconds.
CREATE TABLE inference_pool_control (
    pool TEXT PRIMARY KEY,
    body_json TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL
);

CREATE TABLE inference_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pool TEXT NOT NULL,
    deployment TEXT NOT NULL,
    model TEXT NOT NULL,
    requester TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('waiting', 'ready', 'failed', 'cancelled')),
    state_at INTEGER,
    reason TEXT
);

CREATE INDEX idx_inference_requests_state ON inference_requests (state, pool);
