CREATE TABLE route_fact_samples (
    scope_key TEXT NOT NULL,
    source TEXT NOT NULL,
    identity TEXT NOT NULL,
    fields_json TEXT NOT NULL,
    summary TEXT,
    observed_at INTEGER NOT NULL,
    PRIMARY KEY (scope_key, source)
);