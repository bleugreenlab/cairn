-- First-class thread entity and nullable ownership spine.
CREATE TABLE threads (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    title TEXT,
    jurisdiction TEXT,
    status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active', 'closed')),
    attention TEXT NOT NULL DEFAULT 'none',
    definition TEXT,
    migrated_from_number INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(project_id, name),
    UNIQUE(project_id, migrated_from_number)
);

ALTER TABLE jobs ADD COLUMN thread_id TEXT REFERENCES threads(id);
ALTER TABLE issues ADD COLUMN parent_thread_id TEXT REFERENCES threads(id);

CREATE TABLE comments_new (
    id TEXT PRIMARY KEY,
    issue_id TEXT REFERENCES issues(id) ON DELETE CASCADE,
    thread_id TEXT REFERENCES threads(id),
    content TEXT NOT NULL,
    source TEXT NOT NULL CHECK(source IN ('user', 'agent')),
    created_at INTEGER NOT NULL,
    seq INTEGER NOT NULL DEFAULT 0
);
INSERT INTO comments_new (id, issue_id, content, source, created_at, seq)
SELECT id, issue_id, content, source, created_at, seq FROM comments;
DROP TABLE comments;
ALTER TABLE comments_new RENAME TO comments;
CREATE INDEX idx_comments_issue ON comments(issue_id);
CREATE INDEX idx_comments_thread ON comments(thread_id);
CREATE TRIGGER search_comments_insert AFTER INSERT ON comments BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'comments', NEW.id, 'comment', 'upsert', 'pending', NEW.created_at);
END;
CREATE TRIGGER search_comments_update AFTER UPDATE ON comments
WHEN OLD.content != NEW.content OR OLD.issue_id IS NOT NEW.issue_id OR OLD.thread_id IS NOT NEW.thread_id BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'comments', NEW.id, 'comment', 'upsert', 'pending', NEW.created_at);
END;
CREATE TRIGGER search_comments_delete AFTER DELETE ON comments BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'comments', OLD.id, 'comment', 'delete', 'pending', unixepoch());
END;

CREATE TABLE messages_new (
    id TEXT NOT NULL PRIMARY KEY,
    channel_type TEXT NOT NULL CHECK(channel_type IN ('project', 'issue', 'thread', 'direct')),
    channel_id TEXT,
    sender_run_id TEXT,
    sender_name TEXT NOT NULL,
    recipient_run_id TEXT,
    content TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    urgency TEXT
);
INSERT INTO messages_new (id, channel_type, channel_id, sender_run_id, sender_name, recipient_run_id, content, created_at, urgency)
SELECT id, channel_type, channel_id, sender_run_id, sender_name, recipient_run_id, content, created_at, urgency FROM messages;
DROP TABLE messages;
ALTER TABLE messages_new RENAME TO messages;
CREATE INDEX idx_messages_channel ON messages(channel_type, channel_id, created_at);
CREATE INDEX idx_messages_created_at ON messages(created_at);
CREATE INDEX idx_messages_recipient ON messages(recipient_run_id) WHERE recipient_run_id IS NOT NULL;
CREATE TRIGGER search_messages_insert AFTER INSERT ON messages BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'messages', NEW.id, 'message', 'upsert', 'pending', NEW.created_at);
END;
CREATE TRIGGER search_messages_update AFTER UPDATE ON messages
WHEN OLD.content != NEW.content OR OLD.channel_type != NEW.channel_type OR OLD.channel_id IS NOT NEW.channel_id BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'messages', NEW.id, 'message', 'upsert', 'pending', NEW.created_at);
END;
CREATE TRIGGER search_messages_delete AFTER DELETE ON messages BEGIN
    INSERT INTO search_outbox(id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'messages', OLD.id, 'message', 'delete', 'pending', unixepoch());
END;
