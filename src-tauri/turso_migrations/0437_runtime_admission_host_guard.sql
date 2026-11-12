-- lineage: private
-- cairn/6352: the row every runtime admission spawn on this host writes.
--
-- A lease admits a process against host memory: it sums the spawns in flight
-- on every lane, then leases. Each lane otherwise writes only its own cursor
-- and request rows, so under `BEGIN CONCURRENT` two lanes leasing at once
-- write disjoint rows, both see the same headroom, and both commit past it
-- (docs/database.md, "Write skew"). Every transaction that puts a new process
-- in flight updates this one row, so two of them conflict and the later one
-- re-runs its memory check with the winner's spawn counted.
--
-- `process_leases` only carries the write; nothing reads it for a decision.
-- The in-flight total stays derived from `runtime_admission_requests`.
CREATE TABLE runtime_admission_host_guard (
    host TEXT PRIMARY KEY NOT NULL,
    process_leases INTEGER NOT NULL
);
