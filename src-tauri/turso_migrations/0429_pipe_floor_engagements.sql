-- lineage: private
-- cairn/6440: one row per engagement of a host's fresh-pipe floor, and one per
-- idle session the floor released while engaged.
--
-- `host` names the executor whose host the floor measured (`local` for the
-- runner's own host, which is the colocated executor's). `build_id` is the
-- runner build that recorded the row: a `not_recovered` verdict disables
-- reclamation only for the build that reached it, so a new build runs the
-- experiment once more. `census_json` is the descriptor census taken when the
-- floor engaged; `reclamation` is the bounded release experiment's verdict.
-- Private: it describes this install's hardware and is rebuilt from nowhere.
CREATE TABLE pipe_floor_engagements (
    id TEXT PRIMARY KEY,
    host TEXT NOT NULL,
    build_id TEXT NOT NULL,
    opened_at_ms INTEGER NOT NULL,
    opening_reading_bytes INTEGER,
    opening_at_least INTEGER NOT NULL DEFAULT 0,
    opening_assessment TEXT NOT NULL,
    cleared_at_ms INTEGER,
    clearance TEXT,
    census_json TEXT,
    census_at_ms INTEGER,
    reclamation TEXT NOT NULL DEFAULT 'pending' CHECK (reclamation IN (
        'pending', 'running', 'recovered', 'not_recovered', 'inconclusive', 'disabled', 'skipped'
    )),
    reclamation_detail TEXT
);
CREATE INDEX idx_pipe_floor_engagements_host ON pipe_floor_engagements(host, opened_at_ms DESC);

-- `leader_exit_confirmed` proves the session's leader process exited, nothing
-- more: its descendants are signalled by nobody, so those still alive at the
-- `after` reading are listed in `surviving_descendants_json`.
CREATE TABLE pipe_floor_releases (
    engagement_id TEXT NOT NULL REFERENCES pipe_floor_engagements(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    session_id TEXT,
    job_id TEXT,
    backend TEXT,
    released_at_ms INTEGER NOT NULL,
    leader_exit_confirmed INTEGER NOT NULL,
    before_bytes INTEGER,
    after_bytes INTEGER,
    after_at_least INTEGER NOT NULL DEFAULT 0,
    census_delta_json TEXT,
    surviving_descendants_json TEXT,
    outcome TEXT NOT NULL,
    PRIMARY KEY (engagement_id, seq)
);
