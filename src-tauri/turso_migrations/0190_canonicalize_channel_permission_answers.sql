-- Channel replies historically persisted provider labels and accepted aliases.
-- Keep unknown values untouched so the domain resolver continues to fail closed.
UPDATE channel_ask_resolution
SET answer = CASE
  WHEN lower(trim(answer)) IN ('1', 'yes', 'y', 'approve', 'allow') THEN 'allow'
  WHEN lower(trim(answer)) IN ('2', 'no', 'n', 'deny', 'denied') THEN 'deny'
END
WHERE kind = 'permission'
  AND lower(trim(answer)) IN (
    '1', 'yes', 'y', 'approve', 'allow',
    '2', 'no', 'n', 'deny', 'denied'
  );