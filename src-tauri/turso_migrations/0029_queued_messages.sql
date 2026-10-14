-- CAIRN-1309: User-controlled message queue for the job chat composer.
--
-- While a job's agent is running (or blocked on a question/permission), a
-- user-typed follow-up is persisted here instead of starting a competing turn.
-- The `delivery` discriminator records the user's chosen timing:
--   'queue' -> deliver at turn end / on the next resume (Enter, default)
--   'steer' -> deliver at the next tool boundary (Cmd+Enter), ASAP
--
-- A row is pending while `delivered_at` is NULL. The tool-boundary claim
-- (dispatch) takes only 'steer' rows; the turn-end/resume claim
-- (continue_job_impl) takes everything still pending, so a 'steer' that never
-- reached a tool boundary is never stranded.
CREATE TABLE queued_messages (
    id           TEXT PRIMARY KEY NOT NULL,
    job_id       TEXT NOT NULL,
    content      TEXT NOT NULL,
    delivery     TEXT NOT NULL,   -- 'queue' | 'steer'
    created_at   INTEGER NOT NULL,
    delivered_at INTEGER
);

-- Pending-message lookups are per-job and run on every tool boundary and
-- every resume, so they want a fast filtered scan.
CREATE INDEX idx_queued_messages_pending
    ON queued_messages(job_id, delivered_at);
