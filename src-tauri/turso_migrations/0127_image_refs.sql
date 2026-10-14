-- Migration: human image references (CAIRN-3242)
--
-- A stored image's sha256 is its STORAGE identity: the content-store key that
-- makes cross-member dedup and integrity verification work. It was also its
-- public address, which put a 64-character hash in front of every reader of a
-- transcript, a message body, or an issue description.
--
-- This table is the indirection that separates the two. A reference row mints a
-- short, scoped, human-meaningful ordinal -- `cairn://p/KEY/{issue}/images/{n}`
-- -- that points at the blob hash. Two pastes of identical bytes mint two
-- references to one blob; that is correct, because references are cheap and the
-- dedup that matters still happens in the content store.
--
-- `issue_number` is 0 for the project-scoped fallback, the contexts that
-- genuinely have no issue (a paste into an issue that does not exist yet).
-- Issue numbers are always positive, so 0 is unambiguous, and a sentinel rather
-- than NULL is what lets the primary key enforce per-scope ordinal uniqueness:
-- SQLite does not compare NULLs as equal, so a nullable key column would admit
-- duplicate project-scoped ordinals.
--
-- Rows are append-only and never rewritten: a minted reference is an address
-- that has already been written into durable transcripts.
CREATE TABLE image_refs (
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    issue_number INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (project_id, issue_number, ordinal)
);
