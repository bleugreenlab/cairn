-- lineage: private
-- cairn/7356: the production token forecast's labels and calibrations.
--
-- A label is the decide-score answerer's reading of one issue's filing text,
-- for one answerer and question version: the refit's reproducible input,
-- written at filing (source 'filing') or by the background backfill
-- (source 'backfill'). An issue the backfill looked at and left out (not filed
-- with an Anthropic model, or text that would not scrub) is recorded as
-- 'skipped', and a model refusal as 'refused', so neither is reconsidered for
-- the same versions. Issue ids are global, so labels for issues in a team
-- replica live here too.
CREATE TABLE token_forecast_labels (
    issue_id TEXT NOT NULL,
    answerer_version TEXT NOT NULL,
    question_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('labelled', 'skipped', 'refused')),
    skip_reason TEXT,
    effort REAL,
    scope REAL,
    score REAL,
    distributions_json TEXT,
    -- sha256 of the scrubbed filing text the answerer saw.
    text_hash TEXT,
    -- 'filing': handed over at filing; 'event': the creation call's payload;
    -- 'current': the issue's text when it was labelled.
    text_source TEXT CHECK (text_source IN ('filing', 'event', 'current')),
    filing_maker TEXT,
    filing_maker_method TEXT,
    source TEXT NOT NULL CHECK (source IN ('filing', 'backfill')),
    model TEXT,
    provider TEXT,
    zdr_basis TEXT,
    cost_usd REAL,
    labeled_at INTEGER NOT NULL,
    PRIMARY KEY (issue_id, answerer_version, question_version)
);
CREATE INDEX idx_token_forecast_labels_versions
    ON token_forecast_labels(answerer_version, question_version, filing_maker);
CREATE INDEX idx_token_forecast_labels_issue ON token_forecast_labels(issue_id);
CREATE INDEX idx_token_forecast_labels_spend ON token_forecast_labels(source, labeled_at);

-- Every refit, accepted or rejected. At most one is active per answerer and
-- question version; a new acceptance supersedes it.
CREATE TABLE token_forecast_calibrations (
    -- tfc-<digest of the code version, answerer and question version, as_of,
    -- and every issue the refit read>.
    version TEXT PRIMARY KEY,
    answerer_version TEXT NOT NULL,
    question_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'superseded', 'rejected')),
    reason TEXT,
    as_of INTEGER NOT NULL,
    table_from INTEGER NOT NULL,
    table_to INTEGER NOT NULL,
    shift_from INTEGER NOT NULL,
    shift_to INTEGER NOT NULL,
    fit_n INTEGER NOT NULL,
    shift_n INTEGER NOT NULL,
    grp TEXT NOT NULL,
    -- Score bin edges, and per bin the unshifted ln-token p50/p80/p90, the
    -- soft-p90 basis, and the held-out shifted p90 coverage.
    edges_json TEXT,
    bins_json TEXT,
    -- The p80 and p90 conformal shifts (ln).
    shifts_json TEXT,
    -- Held-out shifted p90 coverage of the shift window's largest fifth by
    -- actual tokens; below 0.85 it softens every p90 of the calibration.
    large_actuals_coverage REAL,
    exclusions_json TEXT NOT NULL,
    code_version TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX idx_token_forecast_calibrations_active
    ON token_forecast_calibrations(answerer_version, question_version)
    WHERE status = 'active';
CREATE INDEX idx_token_forecast_calibrations_versions
    ON token_forecast_calibrations(answerer_version, question_version, created_at);

-- The backfill's spend ledger: every model call it makes is reserved here at
-- a conservative bound before it is sent and settled to the reply's reported
-- cost when there is one. A refusal, a failure, a timeout or a reply with no
-- cost keeps its reservation, so the daily cap counts every paid call.
CREATE TABLE token_forecast_spend (
    id TEXT PRIMARY KEY,
    issue_id TEXT NOT NULL,
    reserved_at INTEGER NOT NULL,
    reserved_usd REAL NOT NULL,
    settled_usd REAL,
    settled_at INTEGER
);
CREATE INDEX idx_token_forecast_spend_reserved ON token_forecast_spend(reserved_at);
