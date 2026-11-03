-- lineage: shared
-- cairn/5894: every column a held-execution read takes from `executions`, so
-- those reads never touch a table row. Turso assembles the whole record when it
-- fetches any column, so a row read pays for the row's snapshot (hundreds of
-- kilobytes in overflow pages) even when the statement names only `id`. Keyed
-- by `id`, so a read that starts from `execution_gates` seeks each execution
-- here. Full rather than partial: the capacity rows, the fleet count and the
-- census also read released executions. `release_override_json` is a short
-- principal-and-time record (144 bytes at most on the live database, 11-02).
CREATE INDEX idx_executions_hold_facts
    ON executions(id, issue_id, seq, lifecycle, released_at, runner_device_id,
                  release_override_json);
