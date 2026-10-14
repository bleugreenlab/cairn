ALTER TABLE projects ADD COLUMN repository_id TEXT;
UPDATE projects SET repository_id = id WHERE repository_id IS NULL;

ALTER TABLE execution_history ADD COLUMN repository_id TEXT;
UPDATE execution_history
SET repository_id = (
    SELECT COALESCE(p.repository_id, p.id)
    FROM executions e
    JOIN jobs j ON j.execution_id = e.id
    JOIN projects p ON p.id = j.project_id
    WHERE e.id = execution_history.execution_id
    LIMIT 1
)
WHERE repository_id IS NULL;

-- Canonical metadata authority for immutable pack objects. Heavy bytes remain in
-- the content store; these rows are the mark set consumed by object GC.
CREATE TABLE pack_catalog (
    content_hash TEXT NOT NULL CHECK(length(content_hash) = 64),
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    repository_id TEXT NOT NULL,
    object_format TEXT NOT NULL CHECK(object_format IN ('sha1')),
    byte_count INTEGER NOT NULL CHECK(byte_count > 0),
    pack_checksum TEXT NOT NULL CHECK(length(pack_checksum) = 40),
    object_count INTEGER NOT NULL CHECK(object_count > 0),
    kind TEXT NOT NULL CHECK(kind IN ('reachable', 'execution_range', 'mutation_delta')),
    base_commit TEXT,
    tip_commit TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    publication_state TEXT NOT NULL CHECK(publication_state IN ('published')),
    PRIMARY KEY(project_id, repository_id, object_format, content_hash)
);

CREATE INDEX idx_pack_catalog_hash ON pack_catalog(content_hash);
CREATE INDEX idx_pack_catalog_repository_tip
    ON pack_catalog(project_id, repository_id, object_format, tip_commit);

CREATE TABLE pack_catalog_references (
    content_hash TEXT NOT NULL,
    project_id TEXT NOT NULL,
    repository_id TEXT NOT NULL,
    object_format TEXT NOT NULL,
    owner_kind TEXT NOT NULL CHECK(owner_kind IN ('execution_history', 'sealed_commit', 'mutation_delta')),
    owner_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY(owner_kind, owner_id, project_id, repository_id),
    FOREIGN KEY(project_id, repository_id, object_format, content_hash)
      REFERENCES pack_catalog(project_id, repository_id, object_format, content_hash)
      ON DELETE CASCADE
);

CREATE INDEX idx_pack_catalog_references_hash ON pack_catalog_references(content_hash);


-- Failed legacy reconstruction is recorded per pointer generation so a missing
-- or corrupt early object cannot pin the bounded backfill scan forever. A
-- changed pack_hash is a new generation and is eligible immediately.
CREATE TABLE pack_catalog_backfill_attempts (
    execution_id TEXT NOT NULL REFERENCES execution_history(execution_id) ON DELETE CASCADE,
    content_hash TEXT NOT NULL,
    attempted_at INTEGER NOT NULL,
    outcome TEXT NOT NULL CHECK(outcome IN ('missing', 'invalid')),
    PRIMARY KEY(execution_id, content_hash)
);
