ALTER TABLE github_app ADD COLUMN relay_key_rotated_at TEXT;

UPDATE github_app
SET relay_key_rotated_at = strftime('%Y-%m-%dT%H:%M:%SZ', updated_at, 'unixepoch')
WHERE relay_public_key IS NOT NULL
  AND relay_private_key_encrypted IS NOT NULL
  AND relay_key_rotated_at IS NULL;
