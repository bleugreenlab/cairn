-- lineage: private
-- cairn/5501: an era's source weight was written at mint and read by nothing,
-- and measuring it meant tokenizing the raw payloads of the whole folded span
-- on every thread digest preview and resume.
ALTER TABLE thread_eras DROP COLUMN source_tokens;
