-- lineage: private
-- Regenerating a generated profile in place (cairn/6093). An operator edit
-- clears a generated profile's promotion; the operator recovers it by starting
-- one fresh generation aimed at that exact profile.
--
-- `inventory_revision` counts every write of a profile's inventory row, so a
-- capture can tell an edit-and-undo (or two saves in one millisecond) from an
-- untouched profile. The draft door's `revision` is the agent's draft count
-- and does not move on an operator edit.
ALTER TABLE inference_generated_profiles
    ADD COLUMN inventory_revision INTEGER NOT NULL DEFAULT 0;

-- What a regenerating generation captured at launch: the target profile's
-- literal stored body, exactly as saved, with no secret reference expanded,
-- and the ownership and inventory revisions it was captured at. Written once,
-- before the generation's agent runs, and never rewritten or deleted: its
-- first accepted draft takes the profile from its old owner only while the
-- profile is still exactly this. No foreign keys, so erasing the generation's
-- issue or job never releases the check.
CREATE TABLE inference_generation_replacements (
    generation_id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    model TEXT NOT NULL,
    prior_generation_id TEXT NOT NULL,
    prior_revision INTEGER NOT NULL,
    inventory_revision INTEGER NOT NULL,
    profile_digest TEXT NOT NULL,
    surface_digest TEXT NOT NULL,
    body_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    captured_at INTEGER NOT NULL
);

-- A capture refuses a profile another open generation already captured.
CREATE INDEX idx_inference_generation_replacements_profile
    ON inference_generation_replacements (profile_id);
