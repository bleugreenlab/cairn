-- File writes release the repository store lock before remote publication. A
-- durable per-call owner serializes that completion and lets a later delivery
-- take over after a crashed claimant without allowing the stale claimant to win.
ALTER TABLE write_replay_ledger ADD COLUMN owner_token TEXT;
ALTER TABLE write_replay_ledger ADD COLUMN finalized INTEGER NOT NULL DEFAULT 1;
ALTER TABLE write_replay_ledger ADD COLUMN claimed_at INTEGER;
