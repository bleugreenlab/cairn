-- lineage: private
-- cairn/5774: inference recipes are now called profiles, so the word no longer
-- collides with workflow recipes. Every row is kept: the table and the columns
-- naming a profile are renamed in place, and each deployment body's `recipe`
-- key (the profile it runs) becomes `profile`. A profile's own body has no key
-- that names it, so a verification's digest still matches the profile it
-- measured.
ALTER TABLE inference_recipes RENAME TO inference_profiles;

DROP INDEX idx_inference_verifications_recipe_pool;
ALTER TABLE inference_verifications RENAME COLUMN recipe_id TO profile_id;
ALTER TABLE inference_verifications RENAME COLUMN recipe_digest TO profile_digest;
CREATE INDEX idx_inference_verifications_profile_pool
    ON inference_verifications (profile_id, pool_id, started_at);

ALTER TABLE inference_deployment_loads RENAME COLUMN recipe_id TO profile_id;

UPDATE inference_deployments
   SET body_json = json_set(json_remove(body_json, '$.recipe'), '$.profile',
                            json_extract(body_json, '$.recipe'))
 WHERE json_extract(body_json, '$.recipe') IS NOT NULL;
