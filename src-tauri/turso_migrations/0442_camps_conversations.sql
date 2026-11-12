-- lineage: shared
-- rebuild-fk-off: Conserved(camps), Reshaped(wake_subscriptions), Conserved(suppressed_wakes), Conserved(issues)
-- cairn/7002: camps become group conversations plus one passive base camp.
--
-- Base camp's members are its project's open threads, implicitly, so its
-- member rows go. A camp's members are a flat set of camp-owned
-- subscription rows (a thread's bare id, or the literal `operator`), with no
-- level, reason or assigner: the delivery level is the camp's kind. Lead,
-- lead offers, the policy epoch, the journal and issue joins are gone, and
-- `pitched`/`struck` become `active`/`archived`.

DROP TABLE camp_events;

-- `issues.camp_id` carries a foreign key, which the engine will not drop in
-- place, so `issues` is rebuilt without it: every other column in its order,
-- then its indexes and triggers as they stood.
CREATE TABLE issues_conversations_rebuild (
    id TEXT NOT NULL PRIMARY KEY,
    project_id TEXT NOT NULL,
    number INTEGER NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'backlog',
    priority INTEGER,
    completed_at INTEGER,
    dismissed_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    model TEXT,
    merged_at INTEGER,
    closed_at INTEGER,
    progress TEXT NOT NULL DEFAULT 'backlog',
    attention TEXT NOT NULL DEFAULT 'none',
    parent_issue_id TEXT,
    parent_job_id TEXT,
    parent_thread_id TEXT,
    author_principal_json TEXT,
    appearance_snapshot_json TEXT,
    resolution_generation INTEGER NOT NULL DEFAULT 0,
    quiet_failure_generation INTEGER,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (parent_issue_id) REFERENCES issues(id) ON DELETE SET NULL,
    FOREIGN KEY (parent_job_id) REFERENCES jobs(id) ON DELETE SET NULL,
    FOREIGN KEY (parent_thread_id) REFERENCES threads(id)
);
INSERT INTO issues_conversations_rebuild (
    id, project_id, number, title, description, status, priority, completed_at,
    dismissed_at, created_at, updated_at, model, merged_at, closed_at, progress,
    attention, parent_issue_id, parent_job_id, parent_thread_id,
    author_principal_json, appearance_snapshot_json, resolution_generation,
    quiet_failure_generation
)
SELECT id, project_id, number, title, description, status, priority, completed_at,
       dismissed_at, created_at, updated_at, model, merged_at, closed_at, progress,
       attention, parent_issue_id, parent_job_id, parent_thread_id,
       author_principal_json, appearance_snapshot_json, resolution_generation,
       quiet_failure_generation
  FROM issues;
DROP TABLE issues;
ALTER TABLE issues_conversations_rebuild RENAME TO issues;
CREATE INDEX idx_issues_parent_id ON issues (parent_issue_id);
CREATE INDEX idx_issues_project_id ON issues (project_id);
CREATE UNIQUE INDEX idx_issues_project_number ON issues (project_id, number);
CREATE INDEX idx_issues_parent_job_id ON issues (parent_job_id);
CREATE INDEX idx_issues_author_principal ON issues (project_id, author_principal_json, created_at DESC);
CREATE INDEX idx_issues_number ON issues (number);
CREATE INDEX idx_issues_parent_thread_id ON issues (parent_thread_id);
CREATE TRIGGER search_issues_insert AFTER INSERT ON issues BEGIN
    INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'issues', NEW.id, 'issue', 'upsert', 'pending', NEW.updated_at);
END;
CREATE TRIGGER search_issues_update AFTER UPDATE ON issues
WHEN OLD.title != NEW.title OR OLD.description IS NOT NEW.description OR OLD.project_id != NEW.project_id
BEGIN
    INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'issues', NEW.id, 'issue', 'upsert', 'pending', NEW.updated_at);
END;
CREATE TRIGGER search_issues_delete AFTER DELETE ON issues BEGIN
    INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at)
    VALUES (lower(hex(randomblob(16))), 'issues', OLD.id, 'issue', 'delete', 'pending', unixepoch());
END;
CREATE TRIGGER validate_issue_authorship_insert BEFORE INSERT ON issues
WHEN (NEW.author_principal_json IS NULL) != (NEW.appearance_snapshot_json IS NULL)
BEGIN
    SELECT RAISE(ABORT, 'issue authorship must contain both principal and appearance or neither');
END;
CREATE TRIGGER validate_issue_authorship_update BEFORE UPDATE OF author_principal_json, appearance_snapshot_json ON issues
WHEN (NEW.author_principal_json IS NULL) != (NEW.appearance_snapshot_json IS NULL)
BEGIN
    SELECT RAISE(ABORT, 'issue authorship must contain both principal and appearance or neither');
END;
CREATE TRIGGER preserve_issue_authorship_update BEFORE UPDATE OF author_principal_json, appearance_snapshot_json ON issues
WHEN NEW.author_principal_json IS NOT OLD.author_principal_json OR NEW.appearance_snapshot_json IS NOT OLD.appearance_snapshot_json
BEGIN
    SELECT RAISE(ABORT, 'issue authorship is immutable');
END;

-- Foreign keys are off for this file, so the SET NULL a deleted subscription
-- would cascade to its suppressed wakes is written out.
UPDATE suppressed_wakes SET subscription_id = NULL
 WHERE subscription_id IN (
     SELECT ws.id FROM wake_subscriptions ws
       JOIN camps c ON c.id = ws.camp_id
      WHERE c.kind = 'base');
DELETE FROM wake_subscriptions
 WHERE camp_id IN (SELECT id FROM camps WHERE kind = 'base');

-- A human member was always the operator: keep one operator row per camp.
UPDATE suppressed_wakes SET subscription_id = NULL
 WHERE subscription_id IN (
     SELECT ws.id FROM wake_subscriptions ws
      WHERE ws.subject_key LIKE 'human:%'
        AND EXISTS (
            SELECT 1 FROM wake_subscriptions o
             WHERE o.camp_id = ws.camp_id
               AND (o.subject_key = 'operator'
                    OR (o.subject_key LIKE 'human:%' AND o.id < ws.id))));
DELETE FROM wake_subscriptions
 WHERE subject_key LIKE 'human:%'
   AND EXISTS (
       SELECT 1 FROM wake_subscriptions o
        WHERE o.camp_id = wake_subscriptions.camp_id
          AND (o.subject_key = 'operator'
               OR (o.subject_key LIKE 'human:%' AND o.id < wake_subscriptions.id)));
UPDATE wake_subscriptions
   SET subject_key = 'operator', subject_json = '{"kind":"operator"}'
 WHERE subject_key LIKE 'human:%';

-- A camp's lead was a member through `camps.lead_thread_id` alone; it stays
-- one as an ordinary member row.
INSERT INTO wake_subscriptions (
    id, job_id, source_kind, source_ref, fact_kinds_json, state, created_by,
    created_at, updated_at, one_shot, camp_id, subject_key, subject_json,
    camp_level, reason
)
SELECT lower(hex(randomblob(16))), NULL, 'camp', NULL, NULL, 'active', 'system',
       c.created_at, c.created_at, 0, c.id, c.lead_thread_id,
       '{"kind":"thread","thread_id":"' || c.lead_thread_id || '"}',
       'wake', 'the camp''s lead'
  FROM camps c
 WHERE c.kind = 'camp'
   AND NOT EXISTS (
       SELECT 1 FROM wake_subscriptions w
        WHERE w.camp_id = c.id AND w.subject_key = c.lead_thread_id);

CREATE TABLE camps_conversations_rebuild (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    slug TEXT NOT NULL CHECK (length(trim(slug)) > 0),
    title TEXT NOT NULL CHECK (length(trim(title)) > 0),
    kind TEXT NOT NULL CHECK (kind IN ('base', 'camp')),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'archived')),
    archived_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (project_id, slug),
    CHECK ((kind = 'base') = (slug = 'base')),
    CHECK (kind = 'camp' OR status = 'active'),
    CHECK ((status = 'archived') = (archived_at IS NOT NULL))
);
INSERT INTO camps_conversations_rebuild (
    id, project_id, slug, title, kind, status, archived_at, created_at, updated_at
)
SELECT id, project_id, slug, title, kind,
       CASE WHEN status = 'struck' THEN 'archived' ELSE 'active' END,
       CASE WHEN status = 'struck' THEN struck_at ELSE NULL END,
       created_at, updated_at
  FROM camps;
DROP TABLE camps;
ALTER TABLE camps_conversations_rebuild RENAME TO camps;
CREATE UNIQUE INDEX idx_camps_one_base ON camps(project_id) WHERE kind = 'base';

-- A membership row is a camp, a subject key and the subject it names; a job
-- row means exactly what it meant before. Built under another name and filled
-- before the original is dropped, as 0401 did.
CREATE TABLE wake_subscriptions_conversations_rebuild (
    id TEXT PRIMARY KEY,
    job_id TEXT,
    source_kind TEXT NOT NULL,
    source_ref TEXT,
    fact_kinds_json TEXT,
    state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'muted', 'unsubscribed')),
    mute_until_kind TEXT,
    mute_until_ref TEXT,
    created_by TEXT NOT NULL DEFAULT 'agent' CHECK (created_by IN ('agent', 'user', 'system')),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    one_shot INTEGER NOT NULL DEFAULT 0,
    match_phrase TEXT,
    camp_id TEXT,
    subject_key TEXT,
    subject_json TEXT,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE,
    FOREIGN KEY (camp_id) REFERENCES camps(id) ON DELETE CASCADE,
    CHECK (
        (job_id IS NOT NULL AND camp_id IS NULL AND subject_key IS NULL
            AND subject_json IS NULL)
        OR (job_id IS NULL AND camp_id IS NOT NULL AND source_kind = 'camp'
            AND subject_key IS NOT NULL AND subject_json IS NOT NULL)
    )
);
INSERT INTO wake_subscriptions_conversations_rebuild (
    id, job_id, source_kind, source_ref, fact_kinds_json, state, mute_until_kind,
    mute_until_ref, created_by, created_at, updated_at, one_shot, match_phrase,
    camp_id, subject_key, subject_json
)
SELECT id, job_id, source_kind, source_ref, fact_kinds_json, state, mute_until_kind,
       mute_until_ref, created_by, created_at, updated_at, one_shot, match_phrase,
       camp_id, subject_key, subject_json
  FROM wake_subscriptions;
DROP TABLE wake_subscriptions;
ALTER TABLE wake_subscriptions_conversations_rebuild RENAME TO wake_subscriptions;
CREATE INDEX idx_wake_subscriptions_job ON wake_subscriptions(job_id, state);
CREATE UNIQUE INDEX idx_wake_subscriptions_unique_scope
    ON wake_subscriptions(job_id, source_kind, COALESCE(source_ref, ''), COALESCE(fact_kinds_json, ''));
CREATE UNIQUE INDEX idx_wake_subscriptions_camp_subject
    ON wake_subscriptions(camp_id, subject_key);
CREATE INDEX idx_wake_subscriptions_subject
    ON wake_subscriptions(subject_key) WHERE subject_key IS NOT NULL;

-- A node mutes one addressee field with a subscription row on its clause ref.
-- Those refs follow the clause-ref rewrite: `camp-watch:` becomes
-- `camp-passive:` and a camp ref's `@role` suffix goes. A role-suffixed
-- `addressed:` ref now means what the plain ref beside it means, so where both
-- were muted the plain row is kept (and among suffixed ones, the oldest).
CREATE TABLE camps_source_ref_rewrite (
    id TEXT PRIMARY KEY,
    new_ref TEXT NOT NULL
);
INSERT INTO camps_source_ref_rewrite (id, new_ref)
SELECT id,
       CASE WHEN ref LIKE '%@lead' OR ref LIKE '%@wake' OR ref LIKE '%@watch' OR ref LIKE '%@confirm'
            THEN substr(ref, 1, instr(ref, '@') - 1)
            ELSE ref END
  FROM (
      SELECT id,
             CASE WHEN substr(source_ref, 1, 11) = 'camp-watch:'
                  THEN 'camp-passive:' || substr(source_ref, 12)
                  ELSE source_ref END AS ref
        FROM wake_subscriptions
       WHERE substr(source_ref, 1, 11) = 'camp-watch:'
          OR (substr(source_ref, 1, 10) = 'addressed:'
              AND (source_ref LIKE '%@lead' OR source_ref LIKE '%@wake'
                   OR source_ref LIKE '%@watch' OR source_ref LIKE '%@confirm'))
  );
CREATE TABLE camps_source_ref_dropped (id TEXT PRIMARY KEY);
INSERT INTO camps_source_ref_dropped (id)
SELECT w.id
  FROM wake_subscriptions w
  JOIN camps_source_ref_rewrite r ON r.id = w.id
 WHERE EXISTS (
     SELECT 1 FROM wake_subscriptions o
      WHERE o.id <> w.id
        AND o.job_id IS w.job_id
        AND o.source_kind = w.source_kind
        AND COALESCE(o.fact_kinds_json, '') = COALESCE(w.fact_kinds_json, '')
        AND (
            (o.source_ref = r.new_ref
                AND NOT EXISTS (SELECT 1 FROM camps_source_ref_rewrite ro WHERE ro.id = o.id))
            OR EXISTS (
                SELECT 1 FROM camps_source_ref_rewrite ro
                 WHERE ro.id = o.id AND ro.new_ref = r.new_ref
                   AND (o.created_at < w.created_at
                        OR (o.created_at = w.created_at AND o.id < w.id)))));
UPDATE suppressed_wakes SET subscription_id = NULL
 WHERE subscription_id IN (SELECT id FROM camps_source_ref_dropped);
DELETE FROM wake_subscriptions WHERE id IN (SELECT id FROM camps_source_ref_dropped);
UPDATE wake_subscriptions
   SET source_ref = (SELECT new_ref FROM camps_source_ref_rewrite r WHERE r.id = wake_subscriptions.id)
 WHERE id IN (SELECT id FROM camps_source_ref_rewrite);
DROP TABLE camps_source_ref_dropped;
DROP TABLE camps_source_ref_rewrite;

-- Stored camp reasons collapse to one `camp` reason; the level they reached
-- is kept beside them, so nothing about a queued receipt's delivery changes.
UPDATE delivery_receipts
   SET reasons_json = replace(replace(reasons_json,
           '"kind":"camp_lead"', '"kind":"camp"'),
           '"kind":"camp_role"', '"kind":"camp"')
 WHERE reasons_json LIKE '%"kind":"camp_lead"%'
    OR reasons_json LIKE '%"kind":"camp_role"%';
