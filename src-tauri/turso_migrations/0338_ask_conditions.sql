-- lineage: shared
-- cairn/5935: a condition on a pending ask, and the index that keeps reading
-- which approvals are pending off the artifact rows.
--
-- `ask_conditions` is one row per condition an approver puts on an ask that is
-- waiting on them. This change writes only `ask_kind = 'artifact'` (an
-- unconfirmed gated artifact, `ask_id` the exact pending version's
-- `artifacts.id`) with `effect = 'resurface'`: when every clause of
-- `until_json` is true, or one can never become true, the approver is woken
-- once. Questions join as `ask_kind = 'question'` rows (`moot_when`, cairn/5519)
-- with no new table; only the effect a fired row runs differs by kind.
--
-- `until_json` is a JSON array of gate clauses in the `after:` vocabulary, AND
-- semantics, evaluated by the same gate sweep pass. `fired_at` is a latch set
-- once, by a guarded update; `fired_kind` says why (satisfied | foreclosed |
-- cleared | superseded). `delivered_at` records that the effect ran: a re-wake
-- message exists, or the approver is the operator and the notification center
-- re-surfaces the entry instead. Kind and effect are validated in code, not by
-- CHECK, so a new kind needs no table rebuild.
CREATE TABLE ask_conditions (
    id TEXT PRIMARY KEY,
    ask_kind TEXT NOT NULL,
    ask_id TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    issue_id TEXT,
    effect TEXT NOT NULL,
    until_json TEXT NOT NULL,
    created_by TEXT,
    created_at INTEGER NOT NULL,
    fired_at INTEGER,
    fired_kind TEXT,
    fired_reason TEXT,
    delivered_at INTEGER
);
-- At most one open condition per ask: deferring again supersedes the last.
CREATE UNIQUE INDEX idx_ask_conditions_open
    ON ask_conditions(ask_kind, ask_id) WHERE fired_at IS NULL;
-- Every condition on an ask, open or fired, for the readers that mark it.
CREATE INDEX idx_ask_conditions_ask ON ask_conditions(ask_kind, ask_id);
-- What the sweep still has to do: open conditions to evaluate, and fired ones
-- whose effect has not run.
CREATE INDEX idx_ask_conditions_owed
    ON ask_conditions(fired_at) WHERE delivered_at IS NULL;

-- Which approvals are pending is read on every children census, every
-- notification-center rebuild and every gated-artifact read. `artifacts` is a
-- wide-row table (plan and PR bodies live in `data`), and Turso assembles a
-- row's whole record to fetch any column of it (cairn/5894), so these reads must
-- never touch an artifact row. This index carries every column they take, for
-- exactly the rows they can match: an artifact is stored unconfirmed only when a
-- `user` confirm policy wrote it. Its `newest version of the chain` probe is
-- covered by `idx_artifacts_output_job_version`.
CREATE INDEX idx_artifacts_awaiting_confirmation
    ON artifacts(job_id, output_name, version, created_at, id) WHERE confirmed = 0;
