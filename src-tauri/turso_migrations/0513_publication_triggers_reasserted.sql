-- lineage: private
-- cairn/7751: the production store recorded 0455 as applied while its three
-- publications triggers still carried 0438's text, so live_targets_json was
-- not part of a publication's identity and the never-uploads rule followed
-- the publication rather than the leg. This states 0455's definitions again.
-- On a store that already has them it replaces each with the same text.
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
