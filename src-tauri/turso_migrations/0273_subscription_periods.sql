-- Which subscription plan each account was on, and when (cairn/5120).
--
-- One row per stretch of one account on one plan. A period opens when a
-- subscription account appears, closes and is followed by a new one when the
-- account's resolved plan or the operator's plan choice changes, and closes
-- when the account is removed. Removing an account deletes its plan from the
-- credential store, and token_rollup carries no account, so this table is the
-- only record of which subscriptions a past month paid for.
--
-- `provider` is the backend the subscription serves (claude, codex,
-- opencode-go). `plan_key` names an entry of the shipped price table
-- (cairn-models-types `subscription::PLAN_PRICES`), and is NULL when the plan
-- has no price there: unpriced, or ambiguous with no choice made. The labels
-- are what the plan and the account were called while the period was open,
-- kept so a removed account's months still read.
--
-- Private: an install's subscriptions are its own, like its credential
-- history. No foreign key to the account, which lives in the credential
-- store rather than this database.
CREATE TABLE subscription_periods (
    id TEXT PRIMARY KEY NOT NULL,
    account_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    plan_key TEXT,
    plan_label TEXT NOT NULL,
    account_label TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);

-- At most one open period per account.
CREATE UNIQUE INDEX idx_subscription_periods_open
    ON subscription_periods (account_id)
    WHERE ended_at IS NULL;
