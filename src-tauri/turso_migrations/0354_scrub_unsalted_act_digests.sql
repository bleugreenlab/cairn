-- lineage: private
-- cairn/6010: the act a Once grant binds is now salted per card, and an act's
-- unsalted digest never leaves memory. Rows written before that hold the
-- unsalted digest of an act whose content can carry a credential, such as an
-- `accounts` settings value. Scrub it, so a reader of this database cannot
-- confirm a guessed secret against it in one hash or match identical acts
-- across rows.
--
-- A card stored with one drops it. It still parses as the same authority
-- prompt; it now names no act binding, so a Once answer to it mints nothing and
-- the operator is asked again.
UPDATE permission_requests
   SET tool_input = json_remove(tool_input, '$.authority.facts.act')
 WHERE json_valid(tool_input)
   AND json_extract(tool_input, '$.kind') = 'authority_request'
   AND json_extract(tool_input, '$.authority.facts.act') IS NOT NULL;

-- A Once grant stored with one keeps its exact_act constraint, so it still
-- reads and lists, and without a salt it already covers nothing; only the
-- digest goes. The constraint follows the mutation mode and, for an MCP write,
-- the configuration identity, so it sits at index 1 or 2.
UPDATE authority_grants
   SET constraints_json = json_set(constraints_json, '$.constraints[1].digest.digest', '')
 WHERE json_valid(constraints_json)
   AND json_extract(constraints_json, '$.constraints[1].constraint') = 'exact_act'
   AND json_extract(constraints_json, '$.constraints[1].digest.salt') IS NULL;

UPDATE authority_grants
   SET constraints_json = json_set(constraints_json, '$.constraints[2].digest.digest', '')
 WHERE json_valid(constraints_json)
   AND json_extract(constraints_json, '$.constraints[2].constraint') = 'exact_act'
   AND json_extract(constraints_json, '$.constraints[2].digest.salt') IS NULL;
