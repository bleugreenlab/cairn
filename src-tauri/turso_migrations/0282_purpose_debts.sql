-- Every purpose item (one answer to one standing question, such as a main
-- cadence lane) that was granted to a machine and whose death the runner has
-- not yet observed.
--
-- A runner restart forgets in-memory state, but a remote machine running the
-- work does not restart with it. A row is written when an item is granted,
-- updated when it is displaced or its attempt ends without an answer, and
-- deleted when evidence settles it. On boot every row reloads as an
-- unconfirmed debt that blocks its question until that machine's own account,
-- the machine's loss, or an operator settles it.
--
-- state is 'live' while the item is still its question's answer and
-- 'retiring' once it is a debt. cause_json and confirmation_json describe a
-- debt (null while live). generation is the connection generation the grant
-- went out on; it is meaningful only within the runner process that wrote it.
CREATE TABLE IF NOT EXISTS purpose_debts (
    purpose_key TEXT NOT NULL,
    request_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('live', 'retiring')),
    phase TEXT NOT NULL,
    executor_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    dispatched_at INTEGER NOT NULL,
    cause_json TEXT,
    confirmation_json TEXT,
    retired_at INTEGER,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (purpose_key, request_id, attempt_id)
);
