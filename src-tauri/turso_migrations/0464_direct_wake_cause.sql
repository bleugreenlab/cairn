-- lineage: shared
-- Automatic attention delivery also uses direct messages. Unknown historic
-- authorship remains system; only immutable agent provenance or the typed
-- operator-message push marker establishes an addressed legacy delivery.
ALTER TABLE messages ADD COLUMN wake_cause TEXT NOT NULL DEFAULT 'system'
    CHECK (wake_cause IN ('addressed', 'system'));
UPDATE messages SET wake_cause='addressed'
 WHERE channel_type='direct' AND (sender_run_id IS NOT NULL OR id IN (
    SELECT substr(p.key,8,instr(substr(p.key,8),':operator_message:')-1)
      FROM attention_pushes p
     WHERE p.key LIKE 'direct:%:operator_message:%'
 ));
