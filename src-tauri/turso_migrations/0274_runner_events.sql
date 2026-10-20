-- The runner's own lifecycle facts, as rows (cairn/5194).
--
-- `runner_events` holds what the runner concluded about itself and its links:
-- Tokio runtime stalls and recoveries, executor generations attaching and
-- detaching, colocated link resets, listener restages, substrate failures, the
-- runner's own handoff and transport outage. Each fact is written once, at the
-- point the runner decided it, by one writer thread that does not share the
-- runner's runtime, so a stall's own row lands while the stall is in progress.
--
-- `kind` is deliberately unconstrained: its vocabulary is the Rust enum
-- `RunnerEventKind` (fleet/runner_events.rs), pinned there by a test, and it is
-- expected to grow. A CHECK here would cost a table rebuild per new kind.
--
-- `generation` is the fleet's in-memory connection generation, which restarts
-- at 1 with every runner process. Each process opens with a `runner_started`
-- row; an `(executor_id, generation)` pair is unambiguous only within the span
-- between two of them.
--
-- What clears it: rows older than 14 days, then any beyond the 200,000 newest.
-- Every sweep that deletes rows, and every batch of events the writer had to
-- drop, leaves a `journal_gap` row saying how many and why.
--
-- Private: these are facts about this runner process and this machine's fleet
-- links, never replicated.
--
-- Lanes lost to a generation ending. For each generation end since ?1 (unix
-- ms), the check observations on that executor generation that ended as
-- infrastructure failures at or after it, having started before it in the same
-- runner incarnation. A colocated reset leaves a link_reset row and then a
-- detached row for one end; each lane is credited to the first of them. A lane
-- whose machine vanished carries no executor-side start, so its start is
-- `ran_at - duration_ms`. This is `LANES_LOST_TO_GENERATION_END_SQL` in
-- fleet/runner_events.rs:
--
--   SELECT e.id, COUNT(o.id) AS lanes_lost
--   FROM check_result_observations o
--   JOIN runner_events e
--     ON e.executor_id = o.executor_id
--    AND e.generation = o.executor_connection_generation
--   WHERE o.failure_kind = 'infrastructure'
--     AND o.ran_at >= ?1
--     AND e.kind IN ('executor_detached', 'executor_link_reset')
--     AND e.occurred_at_ms >= ?1
--     AND o.ran_at >= e.occurred_at_ms
--     AND COALESCE(o.executor_started_at_unix_ms, o.ran_at - o.duration_ms)
--         <= e.occurred_at_ms
--     AND NOT EXISTS (
--         SELECT 1 FROM runner_events s
--         WHERE s.kind = 'runner_started'
--           AND s.occurred_at_ms > COALESCE(o.executor_started_at_unix_ms,
--                                           o.ran_at - o.duration_ms)
--           AND s.occurred_at_ms <= e.occurred_at_ms)
--     AND NOT EXISTS (
--         SELECT 1 FROM runner_events f
--         WHERE f.kind IN ('executor_detached', 'executor_link_reset')
--           AND f.runner_incarnation = e.runner_incarnation
--           AND f.executor_id = e.executor_id
--           AND f.generation = e.generation
--           AND (f.occurred_at_ms < e.occurred_at_ms
--                OR (f.occurred_at_ms = e.occurred_at_ms AND f.id < e.id)))
--   GROUP BY e.id
CREATE TABLE runner_events (
    -- uuid v7, so ids sort in the order the writer stored them.
    id TEXT PRIMARY KEY NOT NULL,
    -- The runner process that recorded the row; the same string the
    -- execution_queue journal records.
    runner_incarnation TEXT NOT NULL,
    kind TEXT NOT NULL,
    -- When the fact happened, captured where it was decided, not when written.
    occurred_at_ms INTEGER NOT NULL,
    executor_id TEXT,
    generation INTEGER,
    -- The diagnostic sentence the matching log line carries.
    cause TEXT,
    detail_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX idx_runner_events_kind ON runner_events(kind, occurred_at_ms);
CREATE INDEX idx_runner_events_executor ON runner_events(executor_id, occurred_at_ms);
CREATE INDEX idx_runner_events_time ON runner_events(occurred_at_ms);
