-- A thread has exactly one identifier: its name.
--
-- `title` was a nullable prose column every surface rendered as `title ?? name`,
-- which is two sources for one fact. Dropping it rather than mirroring it is what
-- makes the ruling structural. Existing prose titles are discarded; every thread
-- already carries a NOT NULL, project-unique `name`, so nothing becomes
-- unaddressable. Names are deliberately NOT backfilled from titles: slugified
-- prose is long and can collide with UNIQUE(project_id, name).
ALTER TABLE threads DROP COLUMN title;
