-- lineage: shared
-- A tenet's rationale has one home, the `rationale` column (cairn/6036).
--
-- 0286 added the column for new proposals and left a migrated ruling's
-- rationale where the ruling import had put it, in `legacy_json.rationale`,
-- so every reader split on origin. Copying that one verbatim field into the
-- column made for it re-splits no statement, so it is copied here; the import
-- now writes the column itself.
--
-- `legacy_json` is left as it is. Its `legacy_key` is what the import is
-- idempotent by, and its `rationale` key, now a copy nothing reads, is not
-- worth a second write to the same rows.
--
-- A blank rationale stays NULL, as does `No rationale recorded`: the import's
-- placeholder for an arc ruling that carried none, which is not a rationale.
-- `json_valid` guards the extraction, so a malformed row is skipped rather
-- than failing the migration.
UPDATE tenets
   SET rationale = json_extract(legacy_json, '$.rationale')
 WHERE rationale IS NULL
   AND origin = 'migrated-ruling'
   AND CASE WHEN json_valid(legacy_json)
            THEN json_type(legacy_json, '$.rationale') = 'text'
                 AND trim(json_extract(legacy_json, '$.rationale'))
                     NOT IN ('', 'No rationale recorded')
            ELSE 0 END;
