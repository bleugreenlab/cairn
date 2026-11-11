-- lineage: private
-- cairn/6449: a post may be addressed to one camp of its project.
--
-- Posts live in the install's private database and never sync; camps live in
-- the database that owns their project, which for a team project is a team
-- replica. So `camp_id` cannot be a foreign key. It is the camp's immutable id
-- (never its slug, which a reader resolves at render time), validated when the
-- post is published (`cairn-core/src/posts.rs`), which also holds the rule a
-- column constraint cannot: a camp post is scoped to that camp's project.
ALTER TABLE posts ADD COLUMN camp_id TEXT;
CREATE INDEX idx_posts_camp_created ON posts(camp_id, created_at DESC, id DESC)
    WHERE camp_id IS NOT NULL;
