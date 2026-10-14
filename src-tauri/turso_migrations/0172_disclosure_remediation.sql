-- CAIRN-3828: disclosure incidents, their affected-record inventory, and the
-- quarantine that stops an affected record from being served.
--
-- Private-only, deliberately. Two reasons, both load-bearing:
--
--  1. A disclosure is discovered by a process scanning its OWN registry against
--     its own stores. The finding is this host's response record, not a fact
--     about the team.
--  2. The inventory is a map of exactly which records carry a credential. Syncing
--     that map to a team replica would publish the locations of the very thing
--     the subsystem exists to contain -- a disclosure hint travelling further
--     than the disclosure. Replicas remediate from their own scan.
--
-- No column here may carry matched plaintext. The inventory records WHERE and
-- HOW MANY, never WHAT. See `security::remediation` for the type-level side of
-- that rule.

-- One incident per (credential, discovery). The credential is named by its
-- non-secret registry id; the value never appears.
CREATE TABLE disclosure_incidents (
    id TEXT PRIMARY KEY,
    -- The registry `SecretId`. Names the producer ('github-installation:42'),
    -- never derived from the value.
    secret_id TEXT NOT NULL,
    -- `SecretCategory::as_str`, or NULL when a structural match named no entry.
    category TEXT,
    -- How the disclosure came to light: 'crossing' (a live detection reported
    -- output carrying it) or 'declared' (an operator said so).
    discovered_via TEXT NOT NULL CHECK (discovered_via IN ('crossing', 'declared')),
    -- `Crossing::as_str` when discovered_via = 'crossing'.
    crossing TEXT,
    -- Free-text operator note for a declared disclosure. Operator-authored, so
    -- it is their words about the event, never a quoted match.
    note TEXT,
    -- Lifecycle: open -> inventoried -> (contained | action_required) -> resolved.
    --
    -- 'contained' is claimed only when nothing found is still being served.
    -- When a record sits in a store with no read gate the response ends at
    -- 'action_required' instead: the automatic part is done and a person still
    -- has to finish it. A believable 'contained' is what stops an operator
    -- looking further, so it is the one state that must never be guessed at.
    status TEXT NOT NULL CHECK (
        status IN ('open', 'inventoried', 'contained', 'action_required', 'resolved')
    ),
    -- Counts of what revocation actually took, for the operator's report.
    leases_revoked INTEGER NOT NULL DEFAULT 0,
    grants_revoked INTEGER NOT NULL DEFAULT 0,
    -- Whether the credential still needs rotating at its provider. Cairn cannot
    -- rotate a third party's credential, so this stays 1 until an operator
    -- confirms they did.
    rotation_required INTEGER NOT NULL DEFAULT 1,
    rotation_confirmed_at INTEGER,
    discovered_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX idx_disclosure_incidents_secret ON disclosure_incidents(secret_id);
CREATE INDEX idx_disclosure_incidents_status ON disclosure_incidents(status);

-- One row per durable record found to carry the disclosed credential.
--
-- `sink` names the store, `locator` addresses the record within it, and
-- `occurrences` counts the matches. There is deliberately no column for the
-- matched text: an incident report that quotes the credential has copied it into
-- a new durable store, which is the failure this table exists to remediate.
CREATE TABLE disclosure_affected_records (
    id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL REFERENCES disclosure_incidents(id),
    -- `SinkKind::as_str` -- 'transcript_event', 'process_log', etc.
    sink TEXT NOT NULL,
    -- Non-secret address within the sink: an event id, an artifact id, a file
    -- path. Stable enough that the quarantine gate can match on it.
    locator TEXT NOT NULL,
    -- `RecordClass::as_str` -- 'source', 'derived', 'ephemeral'. Decides what
    -- may happen to the record without an operator saying so.
    record_class TEXT NOT NULL CHECK (record_class IN ('source', 'derived', 'ephemeral')),
    -- How many registered forms matched. A count is evidence; a span is a copy.
    occurrences INTEGER NOT NULL,
    -- What was done: 'quarantined' (a read gate now withholds it), 'reported'
    -- (found in a store with no read gate, so it is named and still served),
    -- 'rebuilt', 'purged', or 'repaired' (an explicit, operator-invoked
    -- redaction of a source record).
    --
    -- 'quarantined' and 'reported' are deliberately distinct values rather than
    -- one "handled" state: only the first means a reader is actually stopped,
    -- and a schema that could not tell them apart would let an incident record
    -- containment that never happened.
    disposition TEXT NOT NULL CHECK (
        disposition IN ('quarantined', 'reported', 'rebuilt', 'purged', 'repaired')
    ),
    found_at INTEGER NOT NULL,
    -- When the disposition was carried out; NULL while still pending.
    settled_at INTEGER
);

CREATE INDEX idx_disclosure_affected_incident ON disclosure_affected_records(incident_id);

-- The quarantine gate reads this. Kept as its own table rather than a query over
-- affected records because it is consulted on the read path for every served
-- record: it must be loadable in one scan of a table that holds only what is
-- actually withheld, and it must survive an incident being resolved for other
-- reasons.
CREATE TABLE quarantined_records (
    sink TEXT NOT NULL,
    locator TEXT NOT NULL,
    incident_id TEXT NOT NULL REFERENCES disclosure_incidents(id),
    quarantined_at INTEGER NOT NULL,
    -- Set when an operator explicitly releases the record back to serving,
    -- having repaired or accepted it. A released row is kept for the audit
    -- trail rather than deleted.
    released_at INTEGER,
    released_by TEXT,
    PRIMARY KEY (sink, locator)
);

-- Every state-changing step of a response, in order, so the whole remediation is
-- reconstructable after the fact. This is the auditability half of the
-- acceptance bar: what was revoked, what was rebuilt, what an operator chose.
CREATE TABLE disclosure_actions (
    id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL REFERENCES disclosure_incidents(id),
    seq INTEGER NOT NULL,
    -- 'revoke_leases', 'revoke_grants', 'inventory', 'quarantine', 'rebuild',
    -- 'purge', 'repair', 'release', 'rotation_confirmed'.
    action TEXT NOT NULL,
    -- Who: 'system' for an automatic step, or an operator identifier.
    actor TEXT NOT NULL,
    -- Non-secret detail: counts, sink names, locators. Never matched text.
    detail TEXT,
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_disclosure_actions_incident ON disclosure_actions(incident_id, seq);
