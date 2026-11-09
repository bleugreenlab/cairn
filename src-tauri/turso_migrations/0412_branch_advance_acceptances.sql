-- lineage: shared
-- cairn/5387: the acceptance journal of branch preparation. One row per
-- install attempt of a demand-prepared branch, written under the store writer
-- lock BEFORE the local install (`pending`) and settled once: `accepted` when the
-- branch's literal head is exactly the candidate head, `abandoned` when it is
-- still exactly the head the attempt started from, `unknown` when it is neither
-- or a store-wide mutation moved the branch (final: the row keeps blocking
-- publication of the branch until a repair deletes it).
--
-- A replay can remove a commit from the branch and leave no successor on it to
-- prove it was superseded: the commit became empty, a heal absorbed it, or an
-- exact newer version on the base supersedes it (a squash receipt for a landed
-- parent's whole proven prefix, or an amended parent commit).
-- `dropped_commits_json` is the exact list of commits the engine
-- reported removed from this branch by this install; it is the bounded index of
-- that evidence, so a later publication need not walk back to the receipt.
-- `inherited_drops_json` carries the removals of every earlier accepted row
-- this attempt's starting head descends from, so a chain of local advances
-- keeps every claim until the branch publishes. Content
-- columns are immutable after insert; only status, accepted_operation and
-- settled_at change, once, by the settlement rule. An `unknown` row leaves only
-- by an explicit dismissal (`dismissed`, with the literal head and a reason);
-- it never becomes `accepted`. Rows are never pruned automatically.
CREATE TABLE branch_advance_acceptances (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    store_path TEXT NOT NULL,
    branch TEXT NOT NULL,
    head_before TEXT NOT NULL,
    head_after TEXT NOT NULL,
    base_branch TEXT NOT NULL,
    base_commit TEXT NOT NULL,
    captured_operation TEXT NOT NULL,
    accepted_operation TEXT,
    dropped_commits_json TEXT NOT NULL,
    inherited_drops_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'accepted', 'abandoned', 'unknown', 'dismissed')),
    created_at INTEGER NOT NULL,
    settled_at INTEGER,
    -- The fence a store-wide mutation (an import, a settlement) sets on an
    -- owed row before it mutates: the branch's bookmark and Git ref as read
    -- then (NULL = absent). Cleared when the bracket finishes; a fence left by
    -- a crash is resolved against these copies before any settlement.
    bracket_id TEXT,
    bracket_bookmark TEXT,
    bracket_git_ref TEXT,
    -- An operator's explicit dismissal of an `unknown` row: the exact literal
    -- head it was dismissed at and why. A dismissed row stays as history and
    -- never authorizes a claim.
    dismissed_at INTEGER,
    repair_head TEXT,
    repair_reason TEXT,
    -- Whether the Git side reflects an accepted install: written `pending` with
    -- the row before the install, `exported` once a verified export lands, and
    -- `superseded` when a later writer moved the branch (it owns its export).
    -- A crash between acceptance and export leaves `pending`, which the next
    -- preparation of the branch exports while the head is exactly head_after.
    export_state TEXT NOT NULL DEFAULT 'pending'
        CHECK (export_state IN ('pending', 'exported', 'superseded')),
    exported_at INTEGER,
    -- The claims reset: a confirmed publication of the branch at exactly this
    -- commit, recorded once on the newest accepted row. A later install whose
    -- starting head descends from it inherits no earlier claims (origin no
    -- longer holds what they covered); its own drops are still recorded. If
    -- origin is later rewound behind it, the lost claims make the proof
    -- refuse, never pass. The claim bytes are kept.
    published_checkpoint TEXT,
    published_at INTEGER
);

-- Serves both the outstanding-debt lookup and the newest-first, LIMITed claims
-- read (preparation::accepted_drop_claims) without a sort.
CREATE INDEX idx_branch_advance_acceptances_branch
    ON branch_advance_acceptances(project_id, store_path, branch, status, created_at);
