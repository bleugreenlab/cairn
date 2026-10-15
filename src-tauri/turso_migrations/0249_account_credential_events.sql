-- Provider sign-in lifecycle provenance (cairn/4817).
--
-- One row when a door starts a subscription sign-in, and one when that sign-in
-- reaches its terminal outcome. `authorization_events` records decisions; this
-- records what the flow did, so "a grant allowed the start" and "the provider
-- flow completed" stay distinguishable facts. A start names who initiated it
-- (the server-resolved principal and its decision-time appearance) and cites
-- the decision row when a grant authorized it. A terminal row names no
-- principal: it says the flow started by that initiator reached this outcome,
-- not who performed the provider's browser step.
--
-- Private: an install's credential history is its own, like the authorization
-- journal. Append-only by trigger, and no foreign key to the account, so
-- removing an account leaves its history.
--
-- `failure_kind` is a closed set. Upstream failure text (a CLI error line, a
-- provider refusal) can carry a code, a URL, or account data, so it is never
-- stored here.
CREATE TABLE account_credential_events (
    id TEXT PRIMARY KEY NOT NULL,
    sign_in_ref TEXT NOT NULL,
    provider TEXT NOT NULL CHECK (provider IN ('claude', 'codex')),
    account_id TEXT,
    kind TEXT NOT NULL CHECK (kind IN (
        'sign_in_started', 'sign_in_completed', 'sign_in_failed', 'sign_in_cancelled'
    )),
    initiator_json TEXT,
    appearance_json TEXT,
    authorization_event_id TEXT,
    failure_kind TEXT CHECK (failure_kind IS NULL OR failure_kind IN (
        'timeout', 'invalid_code', 'provider_rejected', 'cli_unavailable', 'wrong_account',
        'abandoned', 'other'
    )),
    email TEXT,
    plan TEXT,
    occurred_at INTEGER NOT NULL,
    -- Attribution is both-or-neither: neither only when the starting door
    -- truthfully carried no attributable appearance.
    CHECK ((initiator_json IS NULL) = (appearance_json IS NULL)),
    -- Only a start names an initiator or cites a decision.
    CHECK (kind = 'sign_in_started' OR (initiator_json IS NULL AND authorization_event_id IS NULL)),
    -- A failure says why, from the closed set; nothing else carries a reason.
    CHECK ((kind = 'sign_in_failed') = (failure_kind IS NOT NULL)),
    -- What the provider reported is learned at completion only.
    CHECK (kind = 'sign_in_completed' OR (email IS NULL AND plan IS NULL))
);

CREATE UNIQUE INDEX idx_account_credential_events_one_start
ON account_credential_events(sign_in_ref) WHERE kind = 'sign_in_started';

CREATE UNIQUE INDEX idx_account_credential_events_one_end
ON account_credential_events(sign_in_ref)
WHERE kind IN ('sign_in_completed', 'sign_in_failed', 'sign_in_cancelled');

CREATE INDEX idx_account_credential_events_account
ON account_credential_events(account_id, occurred_at);

CREATE TRIGGER account_credential_events_no_update
BEFORE UPDATE ON account_credential_events
BEGIN
    SELECT RAISE(ABORT, 'account credential events are immutable');
END;

CREATE TRIGGER account_credential_events_no_delete
BEFORE DELETE ON account_credential_events
BEGIN
    SELECT RAISE(ABORT, 'account credential events are immutable');
END;
