-- CAIRN-1906: drop the dead attention ledger.
--
-- The push-queue rebuild (docs/attention-redesign.md) replaced the
-- level-triggered attention_items ledger, its per-watcher attention_seen cursor,
-- and the coalesced attention_evaluations trigger. Nothing reads or writes these
-- tables anymore; the attention_pushes queue is the sole delivery path. Dropping
-- attention_items also removes its indexes and the vestigial escalate_at
-- (migration 0061) and fingerprint (migration 0062) columns. FK-aware order:
-- the standalone evaluations table, then the attention_seen child, then the
-- attention_items parent.

DROP TABLE IF EXISTS attention_evaluations;
DROP TABLE IF EXISTS attention_seen;
DROP TABLE IF EXISTS attention_items;
