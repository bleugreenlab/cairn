-- lineage: shared
-- Answers share delivery timing with operator messages, not their authorship.
ALTER TABLE queued_messages ADD COLUMN kind TEXT NOT NULL DEFAULT 'operator'
  CHECK (kind IN ('operator', 'question_answer'));
ALTER TABLE queued_messages ADD COLUMN resolution TEXT;

-- Existing answers were keyed by prompt id. Preserve their recorded provenance;
-- never infer an operator from the text or repair an unknown historical actor.
UPDATE queued_messages
SET kind = 'question_answer',
    resolution = (
      SELECT json_object('id', resolution_id, 'surface', COALESCE(resolution_surface, 'legacy_unknown'),
                         'provider', resolution_provider, 'conversation', resolution_conversation,
                         'actor', resolution_actor, 'resolvedAt', answered_at * 1000)
      FROM prompts WHERE prompts.id = queued_messages.id
    )
WHERE EXISTS (SELECT 1 FROM prompts WHERE prompts.id = queued_messages.id);
