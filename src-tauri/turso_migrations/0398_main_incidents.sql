-- lineage: shared
-- Incident identities and their issue generations are project history, not verdicts.
CREATE TABLE main_incidents (
    project_id TEXT NOT NULL REFERENCES projects(id),
    fingerprint TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    identity TEXT NOT NULL,
    signature TEXT NOT NULL,
    class TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (project_id, fingerprint, generation)
);

-- The first delivery is a durable intent; filing only fills its linkage fields.
-- Observation ids intentionally have no FK: check retention must not erase evidence.
CREATE TABLE main_incident_evidence (
    project_id TEXT NOT NULL REFERENCES projects(id),
    fingerprint TEXT NOT NULL,
    event_key TEXT NOT NULL,
    draft_json TEXT NOT NULL,
    body TEXT NOT NULL,
    observation_id TEXT,
    author_principal_json TEXT NOT NULL,
    appearance_snapshot_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    issue_id TEXT,
    issue_uri TEXT,
    generation INTEGER,
    comment_id TEXT,
    filed_at INTEGER,
    last_error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (project_id, fingerprint, event_key),
    CHECK ((issue_id IS NULL AND issue_uri IS NULL AND generation IS NULL
            AND comment_id IS NULL AND filed_at IS NULL)
        OR (issue_id IS NOT NULL AND issue_uri IS NOT NULL AND generation IS NOT NULL
            AND comment_id IS NOT NULL AND filed_at IS NOT NULL))
);
CREATE INDEX idx_main_incident_pending
    ON main_incident_evidence(created_at, project_id, fingerprint, event_key)
    WHERE filed_at IS NULL;

CREATE TRIGGER main_incidents_immutable
BEFORE UPDATE OF project_id, fingerprint, generation, identity, signature, class, issue_id, created_at ON main_incidents
BEGIN
    SELECT RAISE(ABORT, 'incident generations are immutable');
END;
CREATE TRIGGER main_incident_evidence_immutable
BEFORE UPDATE OF project_id, fingerprint, event_key, draft_json, body, observation_id,
    author_principal_json, appearance_snapshot_json, created_at ON main_incident_evidence
BEGIN
    SELECT RAISE(ABORT, 'incident evidence is immutable');
END;
CREATE TRIGGER main_incident_evidence_filed_immutable
BEFORE UPDATE OF issue_id, issue_uri, generation, comment_id, filed_at ON main_incident_evidence
WHEN OLD.filed_at IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'filed incident linkage is immutable');
END;

-- The primary key serializes competing hosts against one durable rate budget.
CREATE TABLE main_incident_budget (
    project_id TEXT PRIMARY KEY NOT NULL REFERENCES projects(id),
    window_started INTEGER NOT NULL,
    created_count INTEGER NOT NULL DEFAULT 0 CHECK (created_count BETWEEN 0 AND 10)
);

-- Prospective durable source work is committed by the observation's own transaction.
-- No backfill: a new consumer does not turn retained historical reds into new work.
CREATE TABLE main_incident_sources (
    project_id TEXT NOT NULL REFERENCES projects(id),
    event_key TEXT NOT NULL,
    observation_id TEXT,
    check_name TEXT NOT NULL,
    platform TEXT NOT NULL,
    commit_sha TEXT,
    notify TEXT,
    lane_kind TEXT,
    cause_class TEXT,
    cause TEXT,
    created_at INTEGER NOT NULL,
    processed_at INTEGER,
    error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(project_id,event_key)
);
CREATE INDEX idx_main_incident_sources_pending
    ON main_incident_sources(attempts,created_at,project_id,event_key)
    WHERE processed_at IS NULL;
CREATE TRIGGER main_incident_source_enqueue
AFTER INSERT ON check_result_observations
WHEN NEW.cadence = 'main'
BEGIN
    INSERT INTO main_incident_sources
        (project_id,event_key,observation_id,check_name,platform,commit_sha,created_at)
    VALUES (NEW.project_id,'observation:' || NEW.id,NEW.id,NEW.check_name,
            COALESCE(NEW.verdict_platform,'unknown'),NEW.commit_sha,NEW.ran_at / 1000)
    ON CONFLICT(project_id,event_key) DO NOTHING;
END;

CREATE TRIGGER main_incident_sources_immutable
BEFORE UPDATE OF project_id,event_key,observation_id,check_name,platform,commit_sha,
    notify,lane_kind,cause_class,cause,created_at ON main_incident_sources
BEGIN SELECT RAISE(ABORT, 'incident sources are immutable'); END;
CREATE INDEX idx_main_incident_sources_observation_pending
    ON main_incident_sources(observation_id)
    WHERE processed_at IS NULL AND observation_id IS NOT NULL;
-- Protect only unprocessed canonical evidence; ordinary retention resumes on ack.
CREATE TRIGGER main_incident_source_retention_guard
BEFORE DELETE ON check_result_observations
WHEN EXISTS (
    SELECT 1 FROM main_incident_sources
    WHERE observation_id=OLD.id AND observation_id IS NOT NULL AND processed_at IS NULL
)
BEGIN SELECT RAISE(IGNORE); END;
