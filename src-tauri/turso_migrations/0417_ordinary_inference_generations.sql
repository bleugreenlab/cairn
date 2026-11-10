-- lineage: private
-- Launch authority and revision-bound consent stay on this runner, independent
-- of the owning project's issue/conversation rows. Historical workflows retain
-- their discriminator so startup can fence them before redispatch.
ALTER TABLE inference_generations ADD COLUMN mode TEXT NOT NULL DEFAULT 'workflow';
ALTER TABLE inference_generations ADD COLUMN execution_id TEXT;
ALTER TABLE inference_generations ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 3);
ALTER TABLE inference_generations ADD COLUMN draft_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE inference_generations ADD COLUMN draft_digest TEXT;
ALTER TABLE inference_generations ADD COLUMN approval_prompt_id TEXT;
ALTER TABLE inference_generations ADD COLUMN approval_run_id TEXT;
ALTER TABLE inference_generations ADD COLUMN approval_revision INTEGER;
ALTER TABLE inference_generations ADD COLUMN verification_id TEXT;
ALTER TABLE inference_generations ADD COLUMN cleanup_pending INTEGER NOT NULL DEFAULT 0 CHECK (cleanup_pending IN (0, 1));

CREATE UNIQUE INDEX idx_inference_generations_execution
    ON inference_generations (execution_id) WHERE execution_id IS NOT NULL;
