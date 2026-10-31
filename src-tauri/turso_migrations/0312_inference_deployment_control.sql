-- lineage: private
-- cairn/5660: what Cairn has done to the inference deployments it acts on.
--
-- `inference_deployment_state` holds one row per deployment Cairn has started
-- or stopped: its phase as the cairn-inference facet's serde form, and the id
-- of the command its head is running, so a report after a runner restart is
-- still matched to its command. A deployment Cairn never touched has no row and
-- is neither probed nor reserved. Private: it describes this runner's commands
-- to this installation's machines.
--
-- `inference_deployment_loads` is every finished load: when it was requested
-- and sent, how it ended, how long it took to answer its health check, and
-- the GPU-allocated memory each machine held once it had settled. Demand-driven
-- loading sets its idle windows from these durations. Pruned to the newest
-- loads per deployment on insert. Times are epoch milliseconds.
CREATE TABLE inference_deployment_state (
    deployment_id TEXT PRIMARY KEY,
    phase_json TEXT NOT NULL,
    command_id TEXT,
    updated_at INTEGER NOT NULL
);

CREATE TABLE inference_deployment_loads (
    id INTEGER PRIMARY KEY,
    deployment_id TEXT NOT NULL,
    recipe_id TEXT NOT NULL,
    requested_at INTEGER NOT NULL,
    sent_at INTEGER,
    finished_at INTEGER NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('healthy', 'failed')),
    load_ms INTEGER,
    reason TEXT,
    footprint_json TEXT
);

CREATE INDEX idx_inference_deployment_loads_deployment
    ON inference_deployment_loads (deployment_id, id);
