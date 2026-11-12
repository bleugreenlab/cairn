-- lineage: private
-- The publication ledger (cairn/5962): one row per publication, one per
-- (publication, target, leg). The UI reads these tables and nothing else.
-- This install's own record of what it built and where, never synced.
CREATE TABLE publications (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('executor', 'toolPin')),
    -- The commit the publication builds; resolved when the publication starts.
    "commit" TEXT,
    protocol INTEGER,
    trigger TEXT NOT NULL CHECK (trigger IN ('merge', 'heartbeat', 'manual')),
    requested_by TEXT,
    force_targets_json TEXT NOT NULL DEFAULT '[]',
    tool TEXT,
    -- The pure plan's output, verbatim.
    plan_json TEXT,
    mode TEXT NOT NULL CHECK (mode IN ('shadow', 'live')),
    state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'settled')),
    summary TEXT,
    created_at INTEGER NOT NULL,
    ended_at INTEGER,
    CHECK ((state = 'settled') = (ended_at IS NOT NULL)),
    CHECK (kind <> 'toolPin' OR tool IS NOT NULL)
);
CREATE INDEX idx_publications_recent ON publications(project_id, created_at DESC, id DESC);

CREATE TABLE publication_legs (
    publication_id TEXT NOT NULL REFERENCES publications(id),
    target TEXT NOT NULL,
    leg TEXT NOT NULL CHECK (leg IN ('plan', 'build', 'sign', 'publish', 'combine', 'verify')),
    seq INTEGER NOT NULL,
    -- A placement target's public name, once placement answered.
    builder TEXT,
    builder_kind TEXT NOT NULL CHECK (builder_kind IN ('fleet', 'hosted')),
    state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'deferred', 'refused')),
    reason TEXT,
    started_at INTEGER,
    ended_at INTEGER,
    assets_json TEXT NOT NULL DEFAULT '[]',
    -- A release URL or S3 URI; always NULL for a shadow publication.
    artifact_location TEXT,
    signer_json TEXT,
    verifier_json TEXT,
    build_jobs_json TEXT,
    log_ref TEXT,
    request_id TEXT,
    PRIMARY KEY (publication_id, target, leg),
    CHECK (state NOT IN ('deferred', 'refused', 'failed') OR reason IS NOT NULL),
    CHECK ((state IN ('succeeded', 'failed', 'deferred', 'refused')) = (ended_at IS NOT NULL))
);

-- A terminal row is a record, not a state: it never moves again.
CREATE TRIGGER publication_legs_terminal_immutable
BEFORE UPDATE ON publication_legs
WHEN OLD.state IN ('succeeded', 'failed', 'deferred', 'refused')
BEGIN
    SELECT RAISE(ABORT, 'a settled publication leg is immutable');
END;
CREATE TRIGGER publications_settled_immutable
BEFORE UPDATE ON publications
WHEN OLD.state = 'settled'
BEGIN
    SELECT RAISE(ABORT, 'a settled publication is immutable');
END;
CREATE TRIGGER publications_identity_immutable
BEFORE UPDATE OF id, project_id, kind, trigger, requested_by, force_targets_json, tool, mode, created_at
ON publications
BEGIN
    SELECT RAISE(ABORT, 'a publication''s identity is immutable');
END;
-- Shadow publications build evidence only: their bytes are never uploaded,
-- so no shadow leg can name an artifact location.
CREATE TRIGGER publication_legs_shadow_never_uploads_insert
BEFORE INSERT ON publication_legs
WHEN NEW.artifact_location IS NOT NULL
    AND (SELECT mode FROM publications WHERE id = NEW.publication_id) = 'shadow'
BEGIN
    SELECT RAISE(ABORT, 'a shadow publication never uploads');
END;
CREATE TRIGGER publication_legs_shadow_never_uploads_update
BEFORE UPDATE OF artifact_location ON publication_legs
WHEN NEW.artifact_location IS NOT NULL
    AND (SELECT mode FROM publications WHERE id = NEW.publication_id) = 'shadow'
BEGIN
    SELECT RAISE(ABORT, 'a shadow publication never uploads');
END;
