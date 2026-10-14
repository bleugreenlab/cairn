-- Move legacy child side-channel notices into the generalized wake digest store.
-- Live side-channel messages are represented as message-bearing wake rows with
-- subscription_id NULL; muted/suppressed message rows carry the muted
-- subscription_id and are claimed by the suppressed digest path.
INSERT INTO suppressed_wakes (
  id,
  subscription_id,
  job_id,
  source_kind,
  source_ref,
  fact_kind,
  occurrences,
  latest_detail_uri,
  content,
  created_at,
  updated_at,
  delivered_at
)
SELECT
  id,
  NULL,
  parent_job_id,
  'issue',
  child_uri,
  'message',
  1,
  child_uri,
  '[Side-channel] the user messaged your child ' || child_uri || ':
' || content,
  created_at,
  COALESCE(delivered_at, created_at),
  delivered_at
FROM child_side_channel_notices
WHERE EXISTS (
  SELECT 1
  FROM sqlite_master
  WHERE type = 'table' AND name = 'child_side_channel_notices'
);

DROP TABLE IF EXISTS child_side_channel_notices;
