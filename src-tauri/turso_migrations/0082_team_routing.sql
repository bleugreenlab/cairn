-- Private-DB routing catalog for the project -> team-DB router (CAIRN-2129).
--
-- These two tables live ONLY in the private database and answer "where does a
-- project live?" without opening any team DB. The `teams` table here is a local
-- REGISTRY of sync config + replica paths; it is distinct from the team DB's
-- `teams` ROOT table (same name, different database and schema).

CREATE TABLE teams (
    id           TEXT PRIMARY KEY NOT NULL,
    name         TEXT NOT NULL,
    sync_url     TEXT NOT NULL,
    auth_token   TEXT,                 -- NULL for an unauthenticated local sync server
    replica_path TEXT NOT NULL,
    created_at   INTEGER NOT NULL
);

-- A project lives WHOLLY in exactly one database. team_id NULL = the private
-- (local) database; a team id routes the project to that team's synced replica.
-- project_key is the normalized (upper-cased) project key, matching the lookup
-- used everywhere else.
CREATE TABLE project_routes (
    project_key TEXT PRIMARY KEY NOT NULL,
    team_id     TEXT REFERENCES teams(id),
    created_at  INTEGER NOT NULL
);
