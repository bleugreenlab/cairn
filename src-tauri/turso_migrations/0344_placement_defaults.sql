-- lineage: shared
-- cairn/5920: a node's placement default. One declared executor selector per
-- home that the home's own free-placement run batches, terminals and REPLs
-- inherit when a call names no `executor` of its own. A declaration, never
-- inferred from where earlier batches landed.
--
-- Keyed by `job_id` for an execution node, a sub-agent task, or a thread's
-- task, and by `thread_id` for a thread's own session: a thread can change
-- session jobs, and its default belongs to the thread. A task has its own job
-- row, so it finds its own default or none, which is what keeps a parent's
-- default from reaching the work it fans out.
--
-- `selector` is the JSON `ExecutorSelector`. `executor_id` is the machine a
-- `{name}` selector resolved to when it was set, so removing that machine from
-- the fleet clears the default (`cleared_at`, `cleared_reason`) rather than
-- carrying a name a later machine could reuse. `clear_noticed` records that the
-- node's next result has said so. A cleared row stays until the next set or an
-- explicit clear, so the overview keeps saying what happened.
CREATE TABLE placement_defaults (
    id TEXT PRIMARY KEY,
    job_id TEXT UNIQUE REFERENCES jobs(id) ON DELETE CASCADE,
    thread_id TEXT UNIQUE REFERENCES threads(id) ON DELETE CASCADE,
    selector TEXT NOT NULL,
    executor_id TEXT,
    set_by TEXT NOT NULL,
    set_at INTEGER NOT NULL,
    cleared_reason TEXT,
    cleared_at INTEGER,
    clear_noticed INTEGER NOT NULL DEFAULT 0,
    CHECK ((job_id IS NULL) <> (thread_id IS NULL))
);
-- Removing a machine clears every default that named it.
CREATE INDEX idx_placement_defaults_executor ON placement_defaults(executor_id);
