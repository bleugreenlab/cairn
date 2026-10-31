-- lineage: shared
-- Delegated packets move out of the execution snapshot (cairn/5597).
--
-- An UPDATE writes the whole new row into the MVCC logical log. Packets lived
-- inline in `executions.snapshot`, so every packet's creation and every status
-- change rewrote the snapshot with all of its execution's packets: on a thread
-- host with 1,154 packets one 21-byte status change wrote 106,732,397 bytes of
-- log (measured on a clone, 10-30). Here each packet is its own row, and a
-- status change rewrites only that row.
--
-- The columns are what changes (`status`, `materialized_node_ids`,
-- `result_artifact_job_id`), what is looked up (`result_artifact_job_id`), and
-- the ids a move into a team re-keys. `spec` holds the rest of the packet as
-- JSON and is never rewritten.
--
-- `problem_statement` is non-NULL only while the packet is `pending`: expansion
-- copies the prompt into the context node it creates, and every status change
-- away from `pending` clears it, so a prompt is written to the log once. A
-- call's prompt goes to its own launch and is never stored here.
--
-- `seq` is the packet's position among its execution's packets. Packets still
-- inline in a snapshot keep their array index as their position, and the
-- background backfill copies them here under it.
CREATE TABLE delegated_packets (
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    parent_job_id TEXT NOT NULL,
    parent_turn_id TEXT,
    result_artifact_job_id TEXT,
    status TEXT NOT NULL,
    materialized_node_ids TEXT NOT NULL DEFAULT '[]',
    problem_statement TEXT,
    spec TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (execution_id, id)
);

-- A child's completion finds its packet by the child's job id.
CREATE INDEX idx_delegated_packets_result_job
    ON delegated_packets(result_artifact_job_id)
    WHERE result_artifact_job_id IS NOT NULL;
