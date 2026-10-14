-- Durable composer attachments: one record table for every uploaded object.
--
-- `image_refs` (0127) already separated an object's STORAGE identity (the
-- sha256 that keys the content store) from its ADDRESS (a short ordinal scoped
-- to where it entered the system). That indirection was right; it was only ever
-- image-shaped. A composer upload of any other kind had no record at all and was
-- copied into the orchestrator host's job scratch directory, so the path handed
-- to the agent named nothing on any other machine.
--
-- `attachments` generalizes the same indirection to arbitrary files, and images
-- move onto it rather than beside it: one ordinal sequence per scope, so an
-- already-minted `cairn://p/KEY/{issue}/images/{n}` keeps resolving to exactly
-- the row it always did while `.../attachments/{n}` reaches the same row through
-- the general address. `kind` is what entitles the `images/{n}` form to resolve a
-- row at all.
--
-- Three scopes, one ordinal allocator. `scope_key` is the namespace key, and it
-- is one NOT NULL column rather than a nullable pair because SQLite does not
-- compare NULLs as equal in a primary key -- the same reason `image_refs`
-- carried a `0` sentinel for its project scope. This generalizes that sentinel
-- instead of adding a second one. `issue_number` and `thread_id` are typed
-- projections of it, carrying the foreign key that makes a thread's attachments
-- cascade with the thread; the CHECK binds them to the discriminator so the
-- three columns cannot disagree.
--
-- `media_type` and `byte_size` are nullable, and NULL means NOT YET MEASURED
-- rather than absent: a row migrated from `image_refs` has its bytes in the
-- content store, unreachable from SQL here, so the fetch path sniffs and
-- backfills on first read. Every newly minted row populates both.
--
-- `image_refs` itself is deliberately NOT dropped. It is a project-scoped shared
-- table, and destructive changes to synced replica tables break Turso sync
-- triggers (docs/database.md, "Shared-space schema"); the same constraint is why
-- `jobs.child_base` survives saying nothing. Nothing reads or writes it after
-- this migration -- it is inert schema residue, not a second implementation --
-- and it can be dropped once CAIRN-3178 makes a team-lineage destructive change
-- verifiable.
CREATE TABLE attachments (
    project_id          TEXT    NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    scope               TEXT    NOT NULL CHECK (scope IN ('issue', 'thread', 'project')),
    scope_key           TEXT    NOT NULL,
    issue_number        INTEGER,
    thread_id           TEXT    REFERENCES threads(id) ON DELETE CASCADE,
    ordinal             INTEGER NOT NULL,
    content_hash        TEXT    NOT NULL,
    kind                TEXT    NOT NULL CHECK (kind IN ('image', 'file')),
    -- Measured from the bytes. The client's claim is recorded beside it and is
    -- never consulted when deciding how to render or handle the object.
    media_type          TEXT,
    declared_media_type TEXT,
    file_name           TEXT    NOT NULL,
    byte_size           INTEGER,
    origin_node_uri     TEXT,
    origin_message_id   TEXT,
    author_principal_json    TEXT,
    appearance_snapshot_json TEXT,
    created_at          INTEGER NOT NULL,
    -- Reference retirement, not reclamation: a retired address stops resolving
    -- and the object it named is left exactly where it is. Physical reclamation
    -- needs a globally authoritative reference inventory (cairn/4648) and is out
    -- of scope here.
    retired_at          INTEGER,
    PRIMARY KEY (project_id, scope, scope_key, ordinal),
    CHECK (
        (scope = 'issue'   AND issue_number IS NOT NULL AND thread_id IS NULL
                           AND scope_key = CAST(issue_number AS TEXT))
     OR (scope = 'thread'  AND thread_id IS NOT NULL AND issue_number IS NULL
                           AND scope_key = thread_id)
     OR (scope = 'project' AND issue_number IS NULL AND thread_id IS NULL
                           AND scope_key = '')
    )
);

CREATE INDEX idx_attachments_hash ON attachments(content_hash);
CREATE INDEX idx_attachments_thread ON attachments(thread_id, ordinal);
CREATE INDEX idx_attachments_author_principal
  ON attachments(project_id, author_principal_json, created_at DESC);

-- Ordinals carry over exactly: `image_refs`'s per-(project_id, issue_number)
-- density maps one-to-one onto (project_id, scope, scope_key), which is what
-- keeps every already-minted image URI resolving. `OR IGNORE` because a team
-- replica may pull rows an earlier-migrated peer already pushed.
INSERT OR IGNORE INTO attachments
    (project_id, scope, scope_key, issue_number, ordinal, content_hash,
     kind, file_name, created_at)
SELECT project_id,
       CASE WHEN issue_number = 0 THEN 'project' ELSE 'issue' END,
       CASE WHEN issue_number = 0 THEN ''       ELSE CAST(issue_number AS TEXT) END,
       CASE WHEN issue_number = 0 THEN NULL     ELSE issue_number END,
       ordinal, content_hash, 'image', 'image', created_at
FROM image_refs;

-- One attribution vocabulary, enforced exactly as 0187 enforces it on issues:
-- principal and appearance are present or absent together, and neither can be
-- rewritten after insert.
CREATE TRIGGER validate_attachment_authorship_insert BEFORE INSERT ON attachments
WHEN (NEW.author_principal_json IS NULL) != (NEW.appearance_snapshot_json IS NULL)
BEGIN SELECT RAISE(ABORT, 'attachment authorship must contain both principal and appearance or neither'); END;

CREATE TRIGGER validate_attachment_authorship_update BEFORE UPDATE OF author_principal_json, appearance_snapshot_json ON attachments
WHEN (NEW.author_principal_json IS NULL) != (NEW.appearance_snapshot_json IS NULL)
BEGIN SELECT RAISE(ABORT, 'attachment authorship must contain both principal and appearance or neither'); END;

CREATE TRIGGER preserve_attachment_authorship_update BEFORE UPDATE OF author_principal_json, appearance_snapshot_json ON attachments
WHEN NEW.author_principal_json IS NOT OLD.author_principal_json
  OR NEW.appearance_snapshot_json IS NOT OLD.appearance_snapshot_json
BEGIN SELECT RAISE(ABORT, 'attachment authorship is immutable'); END;
