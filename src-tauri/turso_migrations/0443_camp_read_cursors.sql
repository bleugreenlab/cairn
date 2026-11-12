-- lineage: private
-- cairn/7002: how far the operator has read each camp, and the camp clause
-- refs pending wakes carry.
--
-- The operator is never a delivery recipient; a camp the operator is a member
-- of shows an unread count instead, of the posts by others with an id above
-- this cursor. Private like `posts`, whose ids it counts, and like
-- `headline_read_cursors`, whose shape it takes: one person's reading on this
-- install. Camps live in the project's owning database, so `camp_id` cannot
-- be a foreign key. Writes only move `seen_through_post_id` forward.
CREATE TABLE camp_read_cursors (
  reader               TEXT NOT NULL,
  camp_id              TEXT NOT NULL,
  seen_through_post_id INTEGER NOT NULL,
  seen_at              INTEGER NOT NULL,
  PRIMARY KEY (camp_id, reader)
);

-- A passive camp line's clause ref is `camp-passive:` (it was `camp-watch:`),
-- and a camp clause ref no longer carries a `@role` suffix.
UPDATE fact_log_wakes
   SET clause_ref = 'camp-passive:' || substr(clause_ref, 12)
 WHERE substr(clause_ref, 1, 11) = 'camp-watch:';
UPDATE fact_log_wakes
   SET clause_ref = substr(clause_ref, 1, instr(clause_ref, '@') - 1)
 WHERE (substr(clause_ref, 1, 13) = 'camp-passive:' OR substr(clause_ref, 1, 10) = 'addressed:')
   AND (clause_ref LIKE '%@lead' OR clause_ref LIKE '%@wake'
        OR clause_ref LIKE '%@watch' OR clause_ref LIKE '%@confirm');
