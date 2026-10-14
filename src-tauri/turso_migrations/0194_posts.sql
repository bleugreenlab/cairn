CREATE TABLE posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT REFERENCES projects(id) ON DELETE RESTRICT,
    title TEXT,
    content TEXT NOT NULL CHECK (length(trim(content)) > 0),
    author_principal_json TEXT NOT NULL,
    appearance_snapshot_json TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);
CREATE INDEX idx_posts_project_created ON posts(project_id, created_at DESC, id DESC);
CREATE TABLE post_comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE RESTRICT,
    content TEXT NOT NULL CHECK (length(trim(content)) > 0),
    author_principal_json TEXT NOT NULL,
    appearance_snapshot_json TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);
CREATE INDEX idx_post_comments_post_created ON post_comments(post_id, created_at, id);
-- One reading position per durable home. `home_kind` is 'thread' (the stable
-- threads.id, which survives session replacement, compaction, and rename) or
-- 'job' (the jobs.id, so every node and every sub-agent task reads its own
-- feed). The position is server-owned: a caller can only name a nonce this
-- table issued, never a post id.
--
-- The two nonce columns answer two different questions and both are needed.
-- `last_issued_nonce` is the one OUTSTANDING issuance: a read overwrites it, so
-- a token from a superseded read is stale and moves nothing.
-- `acknowledged_nonce` is the token that last successfully advanced the
-- position, which is what makes replaying a crash-interrupted acknowledgement a
-- no-op success instead of being indistinguishable from that stale case.
CREATE TABLE feed_cursors (
    home_kind TEXT NOT NULL,
    home_id TEXT NOT NULL,
    acknowledged_post_id INTEGER NOT NULL DEFAULT 0 CHECK (acknowledged_post_id >= 0),
    last_issued_nonce TEXT,
    last_issued_through INTEGER CHECK (last_issued_through IS NULL OR last_issued_through >= 0),
    acknowledged_nonce TEXT,
    updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (home_kind, home_id)
);
CREATE TRIGGER search_posts_insert AFTER INSERT ON posts BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'posts', CAST(NEW.id AS TEXT), 'post', 'upsert', 'pending', NEW.created_at);
END;
CREATE TRIGGER search_posts_delete AFTER DELETE ON posts BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'posts', CAST(OLD.id AS TEXT), 'post', 'delete', 'pending', unixepoch());
END;
CREATE TRIGGER search_post_comments_insert AFTER INSERT ON post_comments BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'post_comments', CAST(NEW.id AS TEXT), 'post_comment', 'upsert', 'pending', NEW.created_at);
END;
CREATE TRIGGER search_post_comments_delete AFTER DELETE ON post_comments BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'post_comments', CAST(OLD.id AS TEXT), 'post_comment', 'delete', 'pending', unixepoch());
END;