-- lineage: private
-- cairn/6792: what reconstruction made of a pack's events, beside its row
-- counts, so the pass can say it and eviction can refuse a pack whose rows are
-- not provably the content the agent saw.
--   reconstruction_degraded    events kept in their stored form (a stub or an
--                              undecodable row)
--   reconstruction_unverified  git-coordinate reads restored completely whose
--                              recorded render sha matches no known composition
--   reconstruction_legacy      git-coordinate reads that verified only under an
--                              older read composition
-- NULL on every row packed before this was recorded: not known, which is not
-- the same as clean. The pack pass re-packs each such unevicted scope once, and
-- eviction holds one until it does.
ALTER TABLE history_tier ADD COLUMN reconstruction_degraded INTEGER;
ALTER TABLE history_tier ADD COLUMN reconstruction_unverified INTEGER;
ALTER TABLE history_tier ADD COLUMN reconstruction_legacy INTEGER;
