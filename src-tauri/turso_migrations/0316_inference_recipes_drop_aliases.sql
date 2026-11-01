-- lineage: private
-- cairn/5729: models go by their real name (tenet 1336), so a recipe carries no
-- aliases. Stored recipes lose the retired field; a recipe body is the
-- cairn-inference facet's serde form, which no longer has it.
UPDATE inference_recipes
   SET body_json = json_remove(body_json, '$.aliases')
 WHERE json_extract(body_json, '$.aliases') IS NOT NULL;
