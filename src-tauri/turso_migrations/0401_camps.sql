-- lineage: shared
-- rebuild-fk-off: Conserved(wake_subscriptions), Conserved(suppressed_wakes)
-- Camps (cairn/6255): durable project-level coordination containers.
--
-- A camp owns intent and references; its live state (issues, PRs, checks,
-- gates) is read from those rows, never mirrored here. Every project has
-- exactly one base camp (slug `base`, never struck). `lead_thread_id` is the
-- camp's one lead and the only place lead lives.
--
-- `policy_epoch` is the serialization point for role transitions: every role,
-- lead, or issue-membership change updates this row, so two concurrent writes
-- that would leave contradictory roles across the subscription and artifact
-- requirement stores conflict on it instead of both committing.
CREATE TABLE camps (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    slug TEXT NOT NULL CHECK (length(trim(slug)) > 0),
    title TEXT NOT NULL CHECK (length(trim(title)) > 0),
    kind TEXT NOT NULL CHECK (kind IN ('base', 'camp')),
    lead_thread_id TEXT NOT NULL REFERENCES threads(id),
    -- The thread the lead has offered the lead to, until it accepts. A lead
    -- changes hands only when the receiving thread takes it.
    lead_offer_thread_id TEXT REFERENCES threads(id),
    status TEXT NOT NULL DEFAULT 'pitched' CHECK (status IN ('pitched', 'struck')),
    policy_epoch INTEGER NOT NULL DEFAULT 0 CHECK (policy_epoch >= 0),
    struck_at INTEGER,
    struck_reason TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (project_id, slug),
    CHECK ((kind = 'base') = (slug = 'base')),
    CHECK (kind = 'camp' OR status = 'pitched'),
    CHECK ((status = 'struck') = (struck_at IS NOT NULL))
);
CREATE UNIQUE INDEX idx_camps_one_base ON camps(project_id) WHERE kind = 'base';
CREATE INDEX idx_camps_lead ON camps(lead_thread_id);
CREATE INDEX idx_camps_lead_offer ON camps(lead_offer_thread_id);

-- The camp's append-only journal: who changed what and why. A camp's history
-- page is generated from these rows. Actor and appearance are both present or
-- both absent; absent means the system acted (birth, backfill), and no human
-- or agent identity is fabricated for it.
CREATE TABLE camp_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    camp_id TEXT NOT NULL REFERENCES camps(id) ON DELETE CASCADE,
    policy_epoch INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN (
        'pitched', 'joined', 'role_changed', 'left', 'lead_offered', 'lead_changed',
        'retitled', 'struck', 'issue_joined', 'issue_left'
    )),
    subject_key TEXT,
    subject_json TEXT,
    old_value TEXT,
    new_value TEXT,
    reason TEXT,
    actor_json TEXT,
    appearance_json TEXT,
    created_at INTEGER NOT NULL,
    CHECK ((actor_json IS NULL) = (appearance_json IS NULL))
);
CREATE INDEX idx_camp_events_camp ON camp_events(camp_id, id);

-- An issue belongs to at most one camp, separately from its parent: the parent
-- stays the one ancestry and attention edge.
ALTER TABLE issues ADD COLUMN camp_id TEXT REFERENCES camps(id);
CREATE INDEX idx_issues_camp ON issues(camp_id);

-- Wake subscriptions gain a durable camp-subject owner beside the existing job
-- owner. A job row means exactly what it meant before. A camp row records one
-- principal's watch/wake involvement in one camp, keyed by the principal's
-- stable subject (a thread's bare `threads.id`, which move-to-team rekeys like
-- any id column, or `human:<issuer>` + U+001F + `<subject>`, which it leaves
-- alone), so it survives renames and session replacement and has no
-- session-job lifetime. `subject_json` keeps the validated PrincipalRef. Confirm is not stored here: it lives with the artifact
-- gate, and lead lives on the camp row.
--
-- Rebuilt rather than altered because `job_id` loses its NOT NULL. The new
-- table is built and filled under a different name before the original is
-- dropped, so no statement reads a table renamed earlier in this batch.
CREATE TABLE wake_subscriptions_camp_rebuild (
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
    camp_level TEXT,
    reason TEXT,
    assigned_by_json TEXT,
    assigned_appearance_json TEXT,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE,
    FOREIGN KEY (camp_id) REFERENCES camps(id) ON DELETE CASCADE,
    CHECK (
        (job_id IS NOT NULL AND camp_id IS NULL AND subject_key IS NULL
            AND subject_json IS NULL AND camp_level IS NULL)
        OR (job_id IS NULL AND camp_id IS NOT NULL AND source_kind = 'camp'
            AND subject_key IS NOT NULL AND subject_json IS NOT NULL
            AND camp_level IN ('watch', 'wake')
            AND reason IS NOT NULL AND length(trim(reason)) > 0)
    ),
    CHECK ((assigned_by_json IS NULL) = (assigned_appearance_json IS NULL))
);
INSERT INTO wake_subscriptions_camp_rebuild (
    id, job_id, source_kind, source_ref, fact_kinds_json, state, mute_until_kind,
    mute_until_ref, created_by, created_at, updated_at, one_shot, match_phrase
)
SELECT id, job_id, source_kind, source_ref, fact_kinds_json, state, mute_until_kind,
       mute_until_ref, created_by, created_at, updated_at, one_shot, match_phrase
  FROM wake_subscriptions;
DROP TABLE wake_subscriptions;
ALTER TABLE wake_subscriptions_camp_rebuild RENAME TO wake_subscriptions;
CREATE INDEX idx_wake_subscriptions_job ON wake_subscriptions(job_id, state);
CREATE UNIQUE INDEX idx_wake_subscriptions_unique_scope
    ON wake_subscriptions(job_id, source_kind, COALESCE(source_ref, ''), COALESCE(fact_kinds_json, ''));
-- Full rather than partial so a camp's deletion seeks its subscriptions; job
-- rows carry NULL in both columns, which a unique index treats as distinct.
CREATE UNIQUE INDEX idx_wake_subscriptions_camp_subject
    ON wake_subscriptions(camp_id, subject_key);
CREATE INDEX idx_wake_subscriptions_subject
    ON wake_subscriptions(subject_key) WHERE subject_key IS NOT NULL;
