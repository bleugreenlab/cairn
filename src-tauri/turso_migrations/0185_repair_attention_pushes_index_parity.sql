DROP INDEX IF EXISTS idx_attention_pushes_supersede;

CREATE UNIQUE INDEX IF NOT EXISTS idx_attention_pushes_supersede
  ON attention_pushes (recipient, "key") WHERE delivered_event_id IS NULL;
