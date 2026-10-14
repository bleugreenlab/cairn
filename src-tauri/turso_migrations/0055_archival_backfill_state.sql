-- Migration: archival backfill state
--
-- A one-time, idempotent background task (CAIRN-1555) zstd-compresses the events
-- of already-torn-down executions whose worktrees are gone (history the teardown
-- writer in CAIRN-1554 can never reach). This single-row table records when the
-- backfill finished so it does not re-scan on every startup.
--
-- `backfill_completed_at` is set only once no compressible-or-deferred event
-- remains, so a partially-embedded corpus keeps re-running cheaply (it picks up
-- rows the pre-Cohere embedding fallback was still reading raw once they embed).
--
-- `vacuum_completed_at` is reserved for the in-place file-compaction follow-up.
-- Backfilling only frees pages to the freelist; reclaiming the file itself needs
-- a crash-safe swap of the compacted `VACUUM INTO` image, because no in-place
-- VACUUM is safe in this embedded Turso build (its TRUNCATE WAL checkpoint, the
-- only mode MVCC allows, corrupts on the real schema). That swap is its own
-- reviewed change and will record completion here.
CREATE TABLE archival_backfill_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    backfill_completed_at INTEGER,
    vacuum_completed_at INTEGER
);

INSERT INTO archival_backfill_state (id, backfill_completed_at, vacuum_completed_at)
VALUES (1, NULL, NULL);
