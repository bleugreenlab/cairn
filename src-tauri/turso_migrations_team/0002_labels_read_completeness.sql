-- Team-DB read-resolution completeness for issue labels (CAIRN-2186).
--
-- The team head schema (0001) deliberately omitted `labels`: team-scoped label
-- MANAGEMENT is a deferred cross-scope decision, and `issue_labels` was
-- re-rooted to drop its FK to `labels` accordingly. But the routed issue-content
-- paths (create / list / get) resolve labels for EVERY issue via the
-- `issue_labels JOIN labels` in `labels::attach::list_labels_for_issue`. With
-- issue routing live (CAIRN-2181 / CAIRN-2184) that JOIN now runs against the
-- team replica, where the absent table failed `no such table: labels` on the
-- very first team-issue create.
--
-- This adds the table (and its unique index) so the label JOIN resolves to an
-- EMPTY set uniformly across the private and team databases -- read-resolution
-- completeness ONLY. It does NOT wire team label management: nothing inserts
-- into the team `labels` table this slice, so it stays empty and a team issue
-- simply carries no labels. Team-scoped label management remains the deferred
-- CAIRN-2122 follow-up.
--
-- The DDL is byte-identical (after whitespace normalization) to the private
-- lineage's 0023, which the `team_schema_matches_private` test enforces -- so
-- `labels` is a plain shared table, not a re-rooting. The `workspace_id` FK
-- references `workspaces`, which does not exist in the team DB; that FK is
-- therefore dangling, but harmless -- libsql enforces a foreign key only on a
-- row write and this table is never written (same tolerated-dangling-FK shape
-- as the dropped-`chats` child FKs; see storage/migrations.rs tests).
--
-- This is a tracked TEAM-lineage migration (not a SHARED_TAIL entry): the
-- private lineage already has `labels` from 0023, so only the team lineage needs
-- the catch-up. As a new tracked version it also applies to a team replica
-- provisioned before this slice, which gains the table on its next open.

CREATE TABLE IF NOT EXISTS labels (
    id           TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    color        TEXT NOT NULL,
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_labels_workspace_name ON labels(workspace_id, name COLLATE NOCASE);
