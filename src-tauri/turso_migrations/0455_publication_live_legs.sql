-- lineage: private
-- Publish as placement, Phase B (cairn/7074): a publication can carry live
-- legs beside shadow ones. Each leg records its own mode, fixed when it is laid
-- out, and the never-uploads rule follows the leg rather than the publication:
-- a shadow leg in a live publication still uploads nothing.
ALTER TABLE publication_legs ADD COLUMN mode TEXT NOT NULL DEFAULT 'shadow' CHECK (mode IN ('shadow', 'live'));

-- The project's publish.liveTargets as the publication's commit declared them
-- when it started. Part of the publication's identity.
ALTER TABLE publications ADD COLUMN live_targets_json TEXT NOT NULL DEFAULT '[]';

CREATE TRIGGER publication_legs_mode_immutable
BEFORE UPDATE OF mode ON publication_legs
WHEN NEW.mode <> OLD.mode
BEGIN
    SELECT RAISE(ABORT, 'a publication leg''s mode is immutable');
END;

DROP TRIGGER publications_identity_immutable;
CREATE TRIGGER publications_identity_immutable
BEFORE UPDATE OF id, project_id, kind, trigger, requested_by, force_targets_json, tool, mode, live_targets_json, created_at
ON publications
BEGIN
    SELECT RAISE(ABORT, 'a publication''s identity is immutable');
END;

DROP TRIGGER publication_legs_shadow_never_uploads_insert;
DROP TRIGGER publication_legs_shadow_never_uploads_update;
CREATE TRIGGER publication_legs_shadow_never_uploads_insert
BEFORE INSERT ON publication_legs
WHEN NEW.artifact_location IS NOT NULL AND NEW.mode = 'shadow'
BEGIN
    SELECT RAISE(ABORT, 'a shadow leg never uploads');
END;
CREATE TRIGGER publication_legs_shadow_never_uploads_update
BEFORE UPDATE OF artifact_location ON publication_legs
WHEN NEW.artifact_location IS NOT NULL AND NEW.mode = 'shadow'
BEGIN
    SELECT RAISE(ABORT, 'a shadow leg never uploads');
END;
