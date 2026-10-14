CREATE TABLE prepared_runtime_calls (
    request_id TEXT PRIMARY KEY NOT NULL REFERENCES runtime_admission_requests(request_id) ON DELETE CASCADE,
    run_id TEXT NOT NULL UNIQUE,
    prepared_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
