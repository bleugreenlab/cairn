-- Surface is appearance; actor is principal. Surface never implies authority.
-- Runtime values are owned directly by AppearanceTransport. legacy_unknown is
-- reserved for historical answered rows whose origin cannot be established.
ALTER TABLE channel_ask_resolution ADD COLUMN resolution_id TEXT;
ALTER TABLE channel_ask_resolution ADD COLUMN winner_surface TEXT
  CHECK (winner_surface IN ('resource_patch','channel_reply','remote_intent','non_operator_invoke','local_invoke','authenticated_operator','authenticated_desktop','legacy_unknown'));
ALTER TABLE channel_ask_resolution ADD COLUMN winner_actor TEXT;
UPDATE channel_ask_resolution
   SET resolution_id = lower(hex(randomblob(16))), winner_surface = 'legacy_unknown'
 WHERE resolution_id IS NULL;
CREATE UNIQUE INDEX idx_channel_ask_resolution_id ON channel_ask_resolution(resolution_id);

ALTER TABLE prompts ADD COLUMN resolution_id TEXT;
ALTER TABLE prompts ADD COLUMN resolution_surface TEXT
  CHECK (resolution_surface IN ('resource_patch','channel_reply','remote_intent','non_operator_invoke','local_invoke','authenticated_operator','authenticated_desktop','legacy_unknown'));
ALTER TABLE prompts ADD COLUMN resolution_provider TEXT;
ALTER TABLE prompts ADD COLUMN resolution_conversation TEXT;
ALTER TABLE prompts ADD COLUMN resolution_actor TEXT;
UPDATE prompts SET resolution_id = lower(hex(randomblob(16))), resolution_surface = 'legacy_unknown'
 WHERE response IS NOT NULL AND resolution_id IS NULL;

ALTER TABLE permission_requests ADD COLUMN resolution_id TEXT;
ALTER TABLE permission_requests ADD COLUMN resolution_surface TEXT
  CHECK (resolution_surface IN ('resource_patch','channel_reply','remote_intent','non_operator_invoke','local_invoke','authenticated_operator','authenticated_desktop','legacy_unknown'));
ALTER TABLE permission_requests ADD COLUMN resolution_provider TEXT;
ALTER TABLE permission_requests ADD COLUMN resolution_conversation TEXT;
ALTER TABLE permission_requests ADD COLUMN resolution_actor TEXT;
UPDATE permission_requests SET resolution_id = lower(hex(randomblob(16))), resolution_surface = 'legacy_unknown'
 WHERE status <> 'pending' AND resolution_id IS NULL;

CREATE TRIGGER validate_channel_ask_resolution_provenance_insert BEFORE INSERT ON channel_ask_resolution
WHEN (NEW.winner_surface = 'channel_reply' AND (NEW.winner_provider = '' OR NEW.winner_conversation = '' OR COALESCE(NEW.winner_actor,'') = ''))
  OR (NEW.winner_surface <> 'channel_reply' AND (NEW.winner_provider <> '' OR NEW.winner_conversation <> ''))
  OR (NEW.winner_surface = 'resource_patch' AND COALESCE(NEW.winner_actor,'') = '')
BEGIN SELECT RAISE(ABORT, 'invalid ask resolution provenance'); END;
CREATE TRIGGER validate_channel_ask_resolution_provenance_update BEFORE UPDATE OF winner_surface,winner_provider,winner_conversation,winner_actor ON channel_ask_resolution
WHEN (NEW.winner_surface = 'channel_reply' AND (NEW.winner_provider = '' OR NEW.winner_conversation = '' OR COALESCE(NEW.winner_actor,'') = ''))
  OR (NEW.winner_surface <> 'channel_reply' AND (NEW.winner_provider <> '' OR NEW.winner_conversation <> ''))
  OR (NEW.winner_surface = 'resource_patch' AND COALESCE(NEW.winner_actor,'') = '')
BEGIN SELECT RAISE(ABORT, 'invalid ask resolution provenance'); END;

CREATE TRIGGER validate_prompt_provenance_update BEFORE UPDATE OF resolution_surface,resolution_provider,resolution_conversation,resolution_actor ON prompts
WHEN (NEW.resolution_surface = 'channel_reply' AND (COALESCE(NEW.resolution_provider,'') = '' OR COALESCE(NEW.resolution_conversation,'') = '' OR COALESCE(NEW.resolution_actor,'') = ''))
  OR (NEW.resolution_surface <> 'channel_reply' AND (NEW.resolution_provider IS NOT NULL OR NEW.resolution_conversation IS NOT NULL))
  OR (NEW.resolution_surface = 'resource_patch' AND COALESCE(NEW.resolution_actor,'') = '')
BEGIN SELECT RAISE(ABORT, 'invalid prompt provenance'); END;
CREATE TRIGGER validate_permission_provenance_update BEFORE UPDATE OF resolution_surface,resolution_provider,resolution_conversation,resolution_actor ON permission_requests
WHEN (NEW.resolution_surface = 'channel_reply' AND (COALESCE(NEW.resolution_provider,'') = '' OR COALESCE(NEW.resolution_conversation,'') = '' OR COALESCE(NEW.resolution_actor,'') = ''))
  OR (NEW.resolution_surface <> 'channel_reply' AND (NEW.resolution_provider IS NOT NULL OR NEW.resolution_conversation IS NOT NULL))
  OR (NEW.resolution_surface = 'resource_patch' AND COALESCE(NEW.resolution_actor,'') = '')
BEGIN SELECT RAISE(ABORT, 'invalid permission provenance'); END;
