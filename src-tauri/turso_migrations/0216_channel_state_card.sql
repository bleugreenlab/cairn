CREATE TABLE channel_state_card (
  provider TEXT NOT NULL,
  conversation TEXT NOT NULL,
  subject_uri TEXT NOT NULL,
  target_uri TEXT NOT NULL,
  project_key TEXT NOT NULL,
  job_id TEXT NOT NULL,
  provider_guid TEXT,
  pinned INTEGER NOT NULL DEFAULT 0,
  desired_fingerprint TEXT,
  rendered_fingerprint TEXT,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','live','settled','failed')),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  last_edit_at INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (provider, conversation, subject_uri)
);

CREATE INDEX channel_state_card_due
  ON channel_state_card(provider, next_attempt_at)
  WHERE status IN ('pending','live');
