-- The cairn-inference facet's records: recipes, deployments and pools, each
-- held as the facet's own serde form in `body_json`. `source` says who owns a
-- record: `bginf` rows mirror bginf's control service and are replaced by every
-- re-import; `cairn` rows are Cairn's own (none yet).
--
-- No foreign keys on purpose: integrity is the facet's `Inventory::validate()`,
-- and the host writes a validated inventory's changed rows in one transaction.
CREATE TABLE inference_recipes (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL CHECK (source IN ('bginf', 'cairn')),
    body_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE inference_deployments (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL CHECK (source IN ('bginf', 'cairn')),
    body_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE inference_pools (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL CHECK (source IN ('bginf', 'cairn')),
    body_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- The bginf link's one row: the last attempt and its error (with when the
-- current run of failures began), the last good read's raw `/profiles` and
-- `/state` JSON, what the last import refused, and the last report that changed
-- something. One row, so a 30-second poll grows no history. Times are epoch
-- milliseconds.
CREATE TABLE inference_bginf_link (
    link TEXT PRIMARY KEY CHECK (link = 'bginf'),
    last_attempt_at INTEGER,
    last_error TEXT,
    failing_since INTEGER,
    last_ok_at INTEGER,
    profiles_json TEXT,
    state_json TEXT,
    last_warnings_json TEXT,
    last_change_at INTEGER,
    last_change_report_json TEXT
);
