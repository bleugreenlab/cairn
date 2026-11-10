-- lineage: private
-- Which inventory profiles a generation wrote, kept by profile id so the record
-- outlives the generation row and any rewrite of the profile itself. The final
-- container-start gate and the inventory guard read it: a profile with an
-- active row may start only once promoted_verification_id names a pass of
-- exactly this revision and digest. Nothing an agent, an import or an edit
-- sends reaches this table; only the generated-draft door and verification
-- settlement write it.
CREATE TABLE inference_generated_profiles (
    profile_id TEXT PRIMARY KEY,
    generation_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    profile_digest TEXT NOT NULL,
    surface_digest TEXT NOT NULL,
    promoted_verification_id TEXT,
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1))
);
CREATE INDEX idx_inference_generated_profiles_generation
    ON inference_generated_profiles (generation_id);
