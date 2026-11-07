-- lineage: shared
-- The selected model belongs to the report, not the job's mutable current choice.
-- Historical rows stay NULL: the selection cannot be recovered without guessing.
ALTER TABLE events ADD COLUMN selected_model TEXT;

CREATE INDEX idx_events_selected_model_init
ON events(selected_model, timestamp DESC, created_at DESC, sequence DESC, id DESC)
WHERE event_type = 'system:init' AND parent_tool_use_id IS NULL
  AND reported_model IS NOT NULL AND reported_model <> '';

CREATE INDEX idx_events_latest_init
ON events(timestamp DESC, created_at DESC, sequence DESC, id DESC)
WHERE event_type = 'system:init' AND parent_tool_use_id IS NULL;
