-- lineage: shared
-- A thread keeps no artifacts (tenet 1235): its intent and running record are
-- the `intent` and `journal` tables of its own database (thread migration
-- 0005), and the `arc` preset is gone. Thread session jobs minted before that
-- carry the arc preset as their output contract, either by name or with the
-- schema inlined beside it; clear both forms. Arc artifact rows stay as
-- history.
UPDATE jobs SET output_contract = NULL
WHERE thread_id IS NOT NULL
  AND output_contract IS NOT NULL
  AND json_valid(output_contract)
  AND json_extract(output_contract, '$.schemaType') = 'arc';
