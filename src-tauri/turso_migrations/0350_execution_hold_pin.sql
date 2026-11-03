-- lineage: shared
-- cairn/5942: a capacity hold records WHO pinned its place (the operator or a
-- granted thread), not just that it is pinned. Every pin written before this
-- was the operator's fleet move (cairn/5895), so `pinned:true` becomes
-- `pin:{by:"operator"}`, and a row cairn/5943 froze ahead of that move
-- (`pinned:true, frozen:true`) becomes `pin:{by:"operator",kind:"frozen"}`.
-- The LIKE prefilter keeps json functions off authored gate rows, whose
-- detail is not a hold. One representation, no alias: the hold's serde reads
-- only `pin`.
UPDATE execution_gates
   SET detail = json_set(
         json_remove(detail, '$.pinned', '$.frozen'),
         '$.pin',
         json(CASE WHEN json_extract(detail, '$.frozen') = 1
                   THEN '{"by":"operator","kind":"frozen"}'
                   ELSE '{"by":"operator"}' END))
 WHERE detail LIKE '%"pinned":true%'
   AND json_extract(detail, '$.pinned') = 1;
