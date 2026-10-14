-- CAIRN-3980: distinguish policy rejection from provider allowlist rejection.
ALTER TABLE channel_inbound ADD COLUMN rejection_reason TEXT
  CHECK (rejection_reason IN ('allowlist', 'policy', 'unmatched'));
