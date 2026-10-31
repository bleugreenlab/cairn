-- lineage: private
-- The portable node scratch (cairn/5253, tenet #1258).
--
-- Every node (a thread, an issue node, and the sub-agent tasks beneath it, which
-- share their parent's) owns one scratch: its `$TMPDIR` on every machine. Its
-- content lives in an isolated bare Git store on this runner,
-- `<cairn home>/node-scratch/<project_id>/<id>.git`; this row is its authority.
-- `head_commit` is the scratch's current state, and every write-back advances
-- it by compare-and-swap. The store's one ref is derived from it.
--
-- `owner_uri` is the node's canonical URI with any `/task/...` suffix
-- stripped. It is unique among open scratches only: a closed row stays as a
-- tombstone until every machine that held a replica acknowledges deleting it,
-- and a thread that is closed and reopened gets a fresh scratch beside it.
--
-- `written_bytes` is a monotonic total of the blob bytes write-backs have
-- added. With a replica's `synced_written_bytes` it gives placement an O(1)
-- estimate of what a machine is missing.
--
-- Private: the store these rows describe is a directory on this machine, and
-- the replicas are this runner's executors.
CREATE TABLE node_scratches (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL,
    owner_uri TEXT NOT NULL,
    thread_id TEXT,
    issue_id TEXT,
    head_commit TEXT,
    head_bytes INTEGER NOT NULL DEFAULT 0,
    -- How many times the head has advanced. Every state is a parentless
    -- commit, so this is the only order between two heads: a machine moves its
    -- replica forward to a higher sequence and never back to a lower one.
    head_seq INTEGER NOT NULL DEFAULT 0,
    written_bytes INTEGER NOT NULL DEFAULT 0,
    -- The machine whose write-back produced `head_commit`, for the run
    -- result's "from <machine>".
    head_executor_id TEXT,
    created_at INTEGER NOT NULL,
    closed_at INTEGER
);

CREATE UNIQUE INDEX idx_node_scratches_open_owner
    ON node_scratches(owner_uri) WHERE closed_at IS NULL;
CREATE INDEX idx_node_scratches_issue
    ON node_scratches(issue_id) WHERE issue_id IS NOT NULL;
CREATE INDEX idx_node_scratches_thread
    ON node_scratches(thread_id) WHERE thread_id IS NOT NULL;

-- One row per machine holding a copy of a scratch. `synced_commit` is the head
-- that machine last brought itself to; `dirty` says it holds writes the head
-- does not; `refused_bytes` is set while those writes are held back by a
-- ceiling refusal.
CREATE TABLE node_scratch_replicas (
    scratch_id TEXT NOT NULL,
    executor_id TEXT NOT NULL,
    synced_commit TEXT,
    synced_written_bytes INTEGER NOT NULL DEFAULT 0,
    dirty INTEGER NOT NULL DEFAULT 0,
    refused_bytes INTEGER,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (scratch_id, executor_id)
);