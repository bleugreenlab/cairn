use super::*;
use crate::storage::{LocalDb, MigrationRunner, RowExt, TURSO_MIGRATIONS};
use std::collections::HashMap;
use tempfile::tempdir;

async fn fixture_db() -> LocalDb {
    let temp = tempdir().unwrap();
    let db = LocalDb::open(temp.path().join("analytics-test.db"))
        .await
        .unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    // A claude job (sonnet/Builder) and a codex job (gpt-5/Planner-0), each
    // with billable turn events plus one event the backend rule must skip:
    // claude skips cumulative `result%`, codex skips `assistant`.
    db.execute_script(
            "
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj', 'default', 'Proj', 'PRJ', '/tmp/prj', 1, 1);
            INSERT INTO jobs(id, project_id, status, model, node_name, agent_config_id, created_at, updated_at)
             VALUES ('job-claude', 'proj', 'merged', 'sonnet', 'builder', 'builder', 1, 1);
            INSERT INTO jobs(id, project_id, status, model, node_name, agent_config_id, created_at, updated_at)
             VALUES ('job-codex', 'proj', 'running', 'gpt-5', 'planner-0', 'planner-0', 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES ('sess-claude', 'job-claude', 'claude', 'open', 1, 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES ('sess-codex', 'job-codex', 'codex', 'open', 1, 1, 1);
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES ('run-claude', 'proj', 'job-claude', 'exited', 'sess-claude', 1, 1);
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES ('run-codex', 'proj', 'job-codex', 'exited', 'sess-codex', 1, 1);

            -- Claude: two assistant events count (135 + 20 = 155); result skipped.
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens)
             VALUES ('c1', 'run-claude', 'sess-claude', 1, 1, 'assistant', '{}',
                NULL, 90000, 10, 100, 20, 5, 3);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens)
             VALUES ('c2', 'run-claude', 'sess-claude', 2, 2, 'assistant', '{}',
                NULL, 90001, 12, 0, 0, 8, 1);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens)
             VALUES ('c3', 'run-claude', 'sess-claude', 3, 3, 'result:success', '{}',
                NULL, 90002, 99999, 99999, 99999, 99999, 0);

            -- Codex: two result events count (110 + 220 = 330); assistant skipped.
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens)
             VALUES ('x1', 'run-codex', 'sess-codex', 1, 1, 'result:success', '{}',
                NULL, 90000, 100, 50, 0, 10, 0);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens)
             VALUES ('x2', 'run-codex', 'sess-codex', 2, 2, 'result:success', '{}',
                NULL, 90001, 200, 80, 0, 20, 0);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens)
             VALUES ('x3', 'run-codex', 'sess-codex', 3, 3, 'assistant', '{}',
                NULL, 90002, 88888, 0, 0, 88888, 0);

            INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch,
                target_branch, status, opened_at, updated_at, merged_at, additions, deletions)
             VALUES ('mr1', 'job-claude', 'proj', NULL, 'PR', 'agent/x', 'main', 'merged',
                90100, 90100, 90100, 100, 55);
            ",
        )
        .await
        .unwrap();
    // These tests seed events via raw SQL, bypassing the per-event insert
    // seam that keeps the rollup current in production. The read paths no
    // longer fold on open, so run the one-time backfill once here — exactly
    // as startup does for pre-seam historical events.
    queries::fold_token_rollup(&db).await.unwrap();
    db
}

fn econ<'a>(rows: &'a [EconomicsRow], key: &str) -> &'a EconomicsRow {
    rows.iter()
        .find(|r| r.key == key)
        .unwrap_or_else(|| panic!("missing key {key}"))
}

#[tokio::test]
async fn economics_applies_backend_billable_rule() {
    let db = fixture_db().await;
    let out = model_role_economics(&db, &Scope::default(), &TimeRange::default())
        .await
        .unwrap();

    // Claude assistant billable = input + cache_read + cache_create + output.
    assert_eq!(econ(&out.by_model, "sonnet").billable_tokens, 155);
    // Codex result billable = input + output (cache subsumed, never re-added).
    assert_eq!(econ(&out.by_model, "gpt-5").billable_tokens, 330);
    assert_eq!(econ(&out.by_role, "Builder").billable_tokens, 155);
    assert_eq!(econ(&out.by_role, "Planner").billable_tokens, 330);
    assert_eq!(econ(&out.by_model, "sonnet").runs, 1);
    assert_eq!(out.price_source_date, pricing::PRICE_SOURCE_DATE);
    // Both priced models produce a positive cost.
    assert!(econ(&out.by_model, "sonnet").cost_usd > 0.0);
    assert!(econ(&out.by_model, "gpt-5").cost_usd > 0.0);
}

#[tokio::test]
async fn by_role_uses_agent_config_not_task_title() {
    let db = fixture_db().await;
    // A delegated task: node_name is a free-form task title, but the job's
    // agent_config_id names the real agent. The role breakdown must show the
    // agent ("Explore"), never the task title.
    db.execute_script(
            "
            INSERT INTO jobs(id, project_id, status, model, node_name, agent_config_id, created_at, updated_at)
             VALUES ('job-task', 'proj', 'running', 'sonnet', 'Trace the parser flow', 'explore', 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES ('sess-task', 'job-task', 'claude', 'open', 1, 1, 1);
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES ('run-task', 'proj', 'job-task', 'exited', 'sess-task', 1, 1);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens)
             VALUES ('tk1', 'run-task', 'sess-task', 1, 1, 'assistant', '{}',
                NULL, 90000, 10, 0, 0, 5, 0);
            ",
        )
        .await
        .unwrap();
    // Fold again so the after-fixture event (run-task) is in the rollup.
    queries::fold_token_rollup(&db).await.unwrap();

    let out = model_role_economics(&db, &Scope::default(), &TimeRange::default())
        .await
        .unwrap();
    assert!(out.by_role.iter().any(|r| r.key == "Explore"));
    assert!(!out.by_role.iter().any(|r| r.key == "Trace The Parser Flow"));
}

#[tokio::test]
async fn tokens_per_loc_divides_billable_by_lines() {
    let db = fixture_db().await;
    let rows = tokens_per_loc(&db, &Scope::default(), &TimeRange::default())
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r.job_id, "job-claude");
    assert_eq!(r.billable_tokens, 155);
    assert_eq!(r.lines_changed, 155);
    assert!((r.tokens_per_line - 1.0).abs() < 1e-9);
    assert_eq!(r.role, "Builder");
}

#[tokio::test]
async fn merged_pr_economics_normalizes_per_pr() {
    let db = fixture_db().await;
    let out = merged_pr_economics(&db, &Scope::default(), &TimeRange::default())
        .await
        .unwrap();
    // Only job-claude shipped a merged PR (mr1); job-codex produced none.
    let m = out
        .by_model
        .iter()
        .find(|r| r.key == "sonnet")
        .expect("sonnet model row");
    assert_eq!(m.pr_count, 1);
    assert_eq!(m.billable_tokens, 155);
    assert_eq!(m.lines_changed, 155);
    assert!((m.tokens_per_pr - 155.0).abs() < 1e-9);
    assert!((m.tokens_per_line - 1.0).abs() < 1e-9);
    assert!(m.cost_usd > 0.0);
    // One PR, so cost_per_pr equals the total cost.
    assert!((m.cost_per_pr - m.cost_usd).abs() < 1e-9);
    // Codex model never shipped a PR in the fixture.
    assert!(!out.by_model.iter().any(|r| r.key == "gpt-5"));
    // Role groups on agent_config_id ("builder" -> "Builder").
    let r = out
        .by_role
        .iter()
        .find(|r| r.key == "Builder")
        .expect("Builder role row");
    assert_eq!(r.pr_count, 1);
    assert_eq!(r.billable_tokens, 155);
    assert_eq!(out.price_source_date, pricing::PRICE_SOURCE_DATE);
}

#[tokio::test]
async fn merged_pr_cost_trend_buckets_by_merge_day() {
    let db = fixture_db().await;
    let pts = merged_pr_cost_trend(&db, &Scope::default(), &TimeRange::default(), Bucket::Day)
        .await
        .unwrap();
    assert_eq!(pts.len(), 1);
    // mr1 merged_at 90100 falls in the day bucket starting at 86400.
    assert_eq!(pts[0].bucket_start, 86400);
    assert_eq!(pts[0].pr_count, 1);
    assert_eq!(pts[0].billable_tokens, 155);
    assert_eq!(pts[0].lines_changed, 155);
    assert!(pts[0].cost_usd > 0.0);
}

#[tokio::test]
async fn issue_lead_times_measures_open_to_first_merge() {
    let db = fixture_db().await;
    // Seed an issue whose PR merges 3600s after it opened. Its own job is
    // needed because merge_requests.job_id is UNIQUE (one PR per job), and
    // the fixture's mr1 has a NULL issue_id so it yields no lead-time point.
    db.execute_script(
            "
            INSERT INTO jobs(id, project_id, status, model, node_name, agent_config_id, created_at, updated_at)
             VALUES ('job-lead', 'proj', 'merged', 'sonnet', 'builder', 'builder', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('iss1', 'proj', 1, 'Feature', 'merged', 1000, 1000);
            INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch,
                target_branch, status, opened_at, updated_at, merged_at, additions, deletions)
             VALUES ('mr-lead', 'job-lead', 'proj', 'iss1', 'PR', 'agent/y', 'main', 'merged',
                2000, 4600, 4600, 10, 5);
            ",
        )
        .await
        .unwrap();
    let pts = issue_lead_times(&db, &Scope::default(), &TimeRange::default())
        .await
        .unwrap();
    assert_eq!(pts.len(), 1);
    assert_eq!(pts[0].lead_seconds, 3600); // 4600 - 1000
    assert_eq!(pts[0].merged_at, 4600);
}

#[tokio::test]
async fn avg_tokens_per_session_buckets_by_day() {
    let db = fixture_db().await;
    let rows = avg_tokens_per_session(&db, &Scope::default(), &TimeRange::default(), Bucket::Day)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    // 90000s falls in day bucket starting at 86400.
    assert_eq!(rows[0].bucket_start, 86400);
    assert_eq!(rows[0].session_count, 2);
    // (155 + 330) / 2 = 242.5
    assert!((rows[0].avg_tokens - 242.5).abs() < 1e-9);
}

#[tokio::test]
async fn scope_filter_restricts_to_project() {
    let db = fixture_db().await;
    let out = model_role_economics(
        &db,
        &Scope::new(Some("other-proj".to_string())),
        &TimeRange::default(),
    )
    .await
    .unwrap();
    assert!(out.by_model.is_empty());
}

#[tokio::test]
async fn tool_backfill_extracts_and_breaks_down_shapes() {
    let db = fixture_db().await;
    // An assistant event with two tool uses (a cairn file read + a shell
    // run), plus a tool_result marking the read as an error.
    db.execute_script(
            "
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES ('run-tools', 'proj', 'job-claude', 'exited', 'sess-claude', 1, 1);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at)
             VALUES ('ev-a', 'run-tools', 'sess-claude', 1, 1, 'assistant',
                '{\"eventType\":\"assistant\",\"isError\":false,\"toolUses\":[{\"id\":\"tu1\",\"name\":\"mcp__cairn__read\",\"input\":{\"paths\":[\"file:src/lib.rs\"]}},{\"id\":\"tu2\",\"name\":\"run\",\"input\":{\"commands\":[{\"command\":\"cargo test\"}]}}]}',
                NULL, 90000);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at)
             VALUES ('ev-r', 'run-tools', 'sess-claude', 2, 2, 'tool_result',
                '{\"eventType\":\"tool_result\",\"toolUseId\":\"tu1\",\"toolName\":\"read\",\"toolResult\":\"boom\",\"isError\":true}',
                NULL, 90001);
            ",
        )
        .await
        .unwrap();

    let summary = backfill_tool_invocations(&db).await.unwrap();
    assert_eq!(summary.rows_written, 2);
    assert_eq!(summary.total_indexed, 2);

    // A second pass is a no-op: the run watermark has advanced.
    let again = backfill_tool_invocations(&db).await.unwrap();
    assert_eq!(again.runs_processed, 0);

    let breakdown = target_breakdown(&db, &Scope::default(), &TimeRange::default())
        .await
        .unwrap();
    assert_eq!(breakdown.total, 2);
    let read_shape = breakdown
        .shapes
        .iter()
        .find(|s| s.verb == "read" && s.scheme == "file")
        .expect("read/file shape");
    assert_eq!(read_shape.count, 1);
    assert_eq!(read_shape.error_count, 1);
    assert!((read_shape.error_rate - 1.0).abs() < 1e-9);
    assert!(breakdown
        .shapes
        .iter()
        .any(|s| s.verb == "run" && s.scheme == "shell"));
    assert!(breakdown
        .top_targets
        .iter()
        .any(|t| t.target == "src/lib.rs"));
}

#[tokio::test]
async fn tool_backfill_populates_result_timestamps() {
    let db = fixture_db().await;
    db.execute_script(
            r#"
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES ('run-result-ts', 'proj', 'job-claude', 'exited', 'sess-claude', 1, 1);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at)
             VALUES ('rt-a', 'run-result-ts', 'sess-claude', 1, 1, 'assistant',
                '{"eventType":"assistant","isError":false,"toolUses":[{"id":"rtu","name":"read","input":{"paths":["file:src/lib.rs"]}}]}',
                NULL, 91000);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at)
             VALUES ('rt-r', 'run-result-ts', 'sess-claude', 2, 2, 'tool_result',
                '{"eventType":"tool_result","toolUseId":"rtu","toolName":"read","toolResult":"ok","isError":false}',
                'rtu', 91012);
            "#,
        )
        .await
        .unwrap();

    backfill_tool_invocations(&db).await.unwrap();
    let result_ts = db
        .query_one(
            "SELECT result_ts FROM tool_invocations WHERE tool_use_id = 'rtu'",
            (),
            |row| row.opt_i64(0),
        )
        .await
        .unwrap();
    assert_eq!(result_ts, Some(91012));
}

#[tokio::test]
async fn live_tool_maintenance_sets_result_timestamp() {
    let db = fixture_db().await;
    db.write(|conn| {
            Box::pin(async move {
                queries::maintain_rollups_on_insert(
                    conn,
                    "live-a",
                    "run-claude",
                    92000,
                    "assistant",
                    r#"{"eventType":"assistant","isError":false,"toolUses":[{"id":"live-tu","name":"run","input":{"commands":[{"command":"cargo test"}]}}]}"#,
                    None,
                )
                .await?;
                queries::maintain_rollups_on_insert(
                    conn,
                    "live-r",
                    "run-claude",
                    92009,
                    "tool_result",
                    r#"{"eventType":"tool_result","toolUseId":"live-tu","toolName":"run","toolResult":"ok","isError":true}"#,
                    None,
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

    let row = db
        .query_one(
            "SELECT is_error, result_ts FROM tool_invocations WHERE tool_use_id = 'live-tu'",
            (),
            |row| Ok((row.i64(0)?, row.opt_i64(1)?)),
        )
        .await
        .unwrap();
    assert_eq!(row, (1, Some(92009)));
}

#[tokio::test]
async fn startup_reconciles_legacy_null_result_rows_after_historical_marker() {
    let db = fixture_db().await;
    db.execute_script(
            r#"
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES ('run-legacy-result-ts', 'proj', 'job-claude', 'exited', 'sess-claude', 1, 1);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at)
             VALUES ('legacy-a', 'run-legacy-result-ts', 'sess-claude', 1, 1, 'assistant',
                '{"eventType":"assistant","isError":false,"toolUses":[{"id":"legacy-tu","name":"read","input":{"paths":["file:src/lib.rs"]}}]}',
                NULL, 93000);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at)
             VALUES ('legacy-r', 'run-legacy-result-ts', 'sess-claude', 2, 2, 'tool_result',
                '{"eventType":"tool_result","toolUseId":"legacy-tu","toolName":"read","toolResult":"boom","isError":true}',
                'legacy-tu', 93012);

            INSERT INTO tool_invocations(id, event_id, tool_use_id, run_id, ts, verb, tool_name,
                target_scheme, target_kind, target_path, is_error, result_ts)
             VALUES ('legacy-a:legacy-tu', 'legacy-a', 'legacy-tu', 'run-legacy-result-ts', 93000,
                'read', 'read', 'file', 'rs', 'src/lib.rs', 0, NULL);
            INSERT INTO tool_invocation_runs(run_id, processed_through, updated_at)
             VALUES ('run-legacy-result-ts', 93012, 93012);
            UPDATE analytics_rollup_backfill_state
             SET backfilled_at = 1, tool_results_backfilled_at = NULL
             WHERE id = 1;
            "#,
        )
        .await
        .unwrap();

    queries::backfill_tool_invocations(&db).await.unwrap();
    let before_reconcile = db
        .query_one(
            "SELECT result_ts FROM tool_invocations WHERE tool_use_id = 'legacy-tu'",
            (),
            |row| row.opt_i64(0),
        )
        .await
        .unwrap();
    assert_eq!(before_reconcile, None);

    run_historical_backfill(&db).await.unwrap();

    let row = db
        .query_one(
            "SELECT is_error, result_ts FROM tool_invocations WHERE tool_use_id = 'legacy-tu'",
            (),
            |row| Ok((row.i64(0)?, row.opt_i64(1)?)),
        )
        .await
        .unwrap();
    assert_eq!(row, (1, Some(93012)));

    let marker = db
        .query_one(
            "SELECT tool_results_backfilled_at FROM analytics_rollup_backfill_state WHERE id = 1",
            (),
            |row| row.opt_i64(0),
        )
        .await
        .unwrap();
    assert!(marker.is_some());
}

#[tokio::test]
async fn duration_queries_aggregate_tool_and_session_time() {
    let db = fixture_db().await;
    db.execute_script(
            r#"
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES ('sess-duration-a', 'job-claude', 'claude', 'open', 10, 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES ('sess-duration-b', 'job-claude', 'claude', 'open', 11, 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES ('sess-overrun', 'job-claude', 'claude', 'open', 12, 1, 1);

            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, started_at, exited_at, updated_at)
             VALUES ('run-duration-a', 'proj', 'job-claude', 'exited', 'sess-duration-a', 1, 90000, 90100, 90100);
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, started_at, exited_at, updated_at)
             VALUES ('run-duration-b', 'proj', 'job-claude', 'exited', 'sess-duration-b', 1, 90020, 90040, 90040);
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, started_at, exited_at, updated_at)
             VALUES ('run-overrun', 'proj', 'job-claude', 'exited', 'sess-overrun', 1, 200000, 200010, 200010);

            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, parent_tool_use_id, created_at)
             VALUES ('dur-a1', 'run-duration-a', 'sess-duration-a', 1, 1, 'assistant',
                '{"eventType":"assistant","isError":false,"toolUses":[{"id":"dur-read","name":"read","input":{"paths":["file:src/lib.rs"]}},{"id":"dur-run","name":"run","input":{"commands":[{"command":"cargo test"}]}}]}',
                NULL, 90010);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, parent_tool_use_id, created_at)
             VALUES ('dur-r1', 'run-duration-a', 'sess-duration-a', 2, 2, 'tool_result',
                '{"eventType":"tool_result","toolUseId":"dur-read","toolName":"read","toolResult":"ok","isError":false}',
                'dur-read', 90040);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, parent_tool_use_id, created_at)
             VALUES ('dur-r2', 'run-duration-a', 'sess-duration-a', 3, 3, 'tool_result',
                '{"eventType":"tool_result","toolUseId":"dur-run","toolName":"run","toolResult":"ok","isError":true}',
                'dur-run', 90050);

            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, parent_tool_use_id, created_at)
             VALUES ('over-a1', 'run-overrun', 'sess-overrun', 1, 1, 'assistant',
                '{"eventType":"assistant","isError":false,"toolUses":[{"id":"over-run","name":"run","input":{"commands":[{"command":"sleep 20"}]}}]}',
                NULL, 200000);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, parent_tool_use_id, created_at)
             VALUES ('over-r1', 'run-overrun', 'sess-overrun', 2, 2, 'tool_result',
                '{"eventType":"tool_result","toolUseId":"over-run","toolName":"run","toolResult":"ok","isError":false}',
                'over-run', 200020);
            "#,
        )
        .await
        .unwrap();
    backfill_tool_invocations(&db).await.unwrap();

    let composition = time_composition(&db, &Scope::default(), &TimeRange::default(), Bucket::Day)
        .await
        .unwrap();
    let first = composition
        .iter()
        .find(|p| p.bucket_start == 86400)
        .expect("first duration bucket");
    assert_eq!(first.wall_s, 120);
    assert_eq!(first.tool_s, 70);
    assert_eq!(first.model_overhead_s, 50);
    assert_eq!(first.run_count, 2);
    let clamped = composition
        .iter()
        .find(|p| p.bucket_start == 172800)
        .expect("overrun bucket");
    assert_eq!(clamped.wall_s, 10);
    // Composition caps summed tool-call time to the completed run wall time,
    // so concurrent or late result spans cannot make the headline chart
    // exceed 100%. Per-verb and command-duration queries below still report
    // raw per-call durations.
    assert_eq!(clamped.tool_s, 10);
    assert_eq!(clamped.model_overhead_s, 0);

    let mix = tool_time_mix(&db, &Scope::default(), &TimeRange::default(), Bucket::Day)
        .await
        .unwrap();
    let read = mix
        .iter()
        .find(|p| p.bucket_start == 86400 && p.verb == "read")
        .expect("read time mix");
    assert_eq!(read.seconds, 30);
    assert_eq!(read.count, 1);
    let run = mix
        .iter()
        .find(|p| p.bucket_start == 86400 && p.verb == "run")
        .expect("run time mix");
    assert_eq!(run.seconds, 40);

    let commands = command_durations(&db, &Scope::default(), &TimeRange::default(), 10)
        .await
        .unwrap();
    let cargo = commands
        .iter()
        .find(|row| row.verb == "run" && row.kind == "cargo")
        .expect("cargo command duration");
    assert_eq!(cargo.total_s, 40);
    assert_eq!(cargo.max_s, 40);
    assert_eq!(cargo.error_count, 1);
    assert!((cargo.avg_s - 40.0).abs() < 1e-9);

    let sessions = session_durations(&db, &Scope::default(), &TimeRange::default(), Bucket::Day)
        .await
        .unwrap();
    let first_sessions = sessions
        .iter()
        .find(|p| p.bucket_start == 86400)
        .expect("session duration bucket");
    assert_eq!(first_sessions.session_count, 2);
    assert!((first_sessions.avg_s - 60.0).abs() < 1e-9);
}

#[tokio::test]
async fn tool_calls_per_session_averages_calls() {
    let db = fixture_db().await;
    // A run on sess-claude with two tool uses; backfill turns those into two
    // tool_invocations rows, so the session has 2 tool calls.
    db.execute_script(
            "
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES ('run-tools', 'proj', 'job-claude', 'exited', 'sess-claude', 1, 1);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at)
             VALUES ('ev-a', 'run-tools', 'sess-claude', 1, 1, 'assistant',
                '{\"eventType\":\"assistant\",\"isError\":false,\"toolUses\":[{\"id\":\"tu1\",\"name\":\"mcp__cairn__read\",\"input\":{\"paths\":[\"file:src/lib.rs\"]}},{\"id\":\"tu2\",\"name\":\"run\",\"input\":{\"commands\":[{\"command\":\"cargo test\"}]}}]}',
                NULL, 90000);
            ",
        )
        .await
        .unwrap();
    backfill_tool_invocations(&db).await.unwrap();

    let rows =
        avg_tool_calls_per_session(&db, &Scope::default(), &TimeRange::default(), Bucket::Day)
            .await
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].session_count, 1);
    assert!((rows[0].avg_calls - 2.0).abs() < 1e-9);
    // 90000s falls in the day bucket starting at 86400.
    assert_eq!(rows[0].bucket_start, 86400);
}

// --- Effective (subscription-normalized) cost ---

/// 2025-01-01 and 2025-02-01 00:00:00 UTC, the month-bucket boundaries used
/// by the effective-cost tests.
const JAN_2025: i64 = 1_735_689_600;
const FEB_2025: i64 = 1_738_368_000;

async fn cost_db() -> LocalDb {
    let temp = tempdir().unwrap();
    let db = LocalDb::open(temp.path().join("eff-cost-test.db"))
        .await
        .unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    db.execute(
        "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj', 'default', 'Proj', 'PRJ', '/tmp/prj', 1, 1)",
        (),
    )
    .await
    .unwrap();
    db
}

/// Seed a job/session/run triple keyed by `suffix` for one backend+model.
async fn seed_run(db: &LocalDb, suffix: &str, backend: &str, model: &str) {
    db.execute_script(&format!(
            "INSERT INTO jobs(id, project_id, status, model, node_name, agent_config_id, created_at, updated_at)
              VALUES ('job-{s}', 'proj', 'running', '{model}', 'builder', 'builder', 1, 1);
             INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
              VALUES ('sess-{s}', 'job-{s}', '{backend}', 'open', 1, 1, 1);
             INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
              VALUES ('run-{s}', 'proj', 'job-{s}', 'exited', 'sess-{s}', 1, 1);",
            s = suffix,
            model = model,
            backend = backend
        ))
        .await
        .unwrap();
}

/// Insert one event on `run-{suffix}` with the given type, timestamp, input
/// and output tokens, and optional real `cost_usd`.
#[allow(clippy::too_many_arguments)]
async fn add_event(
    db: &LocalDb,
    id: &str,
    suffix: &str,
    etype: &str,
    ts: i64,
    input: i64,
    output: i64,
    cost: Option<f64>,
) {
    let cost_sql = cost.map_or_else(|| "NULL".to_string(), |c| c.to_string());
    db.execute_script(&format!(
        "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens, cost_usd)
             VALUES ('{id}', 'run-{suffix}', 'sess-{suffix}', 1, {ts}, '{etype}', '{{}}',
                NULL, {ts}, {input}, 0, 0, {output}, 0, {cost_sql});"
    ))
    .await
    .unwrap();
    // Raw-SQL seed bypasses the per-event insert seam; fold so the rollup
    // stays current the way the live insert path keeps it.
    queries::fold_token_rollup(db).await.unwrap();
}

fn fees(pairs: &[(&str, f64)]) -> HashMap<String, f64> {
    pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

fn point<'a>(
    points: &'a [EffectiveCostPoint],
    month_start: i64,
    backend: &str,
) -> &'a EffectiveCostPoint {
    points
        .iter()
        .find(|p| p.month_start == month_start && p.backend == backend)
        .unwrap_or_else(|| panic!("missing point for {backend} @ {month_start}"))
}

#[tokio::test]
async fn effective_cost_ratio_allocates_the_fee() {
    let db = cost_db().await;
    // One claude assistant turn: 1M input + 1M output of sonnet =>
    // list cost (1e6*3 + 1e6*15)/1e6 = $18.
    seed_run(&db, "c", "claude", "sonnet").await;
    add_event(
        &db,
        "c1",
        "c",
        "assistant",
        JAN_2025 + 100,
        1_000_000,
        1_000_000,
        None,
    )
    .await;

    let points = effective_cost(
        &db,
        &Scope::default(),
        &TimeRange::default(),
        &fees(&[("claude", 200.0)]),
        Bucket::Month,
    )
    .await
    .unwrap();

    let p = point(&points, JAN_2025, "claude");
    assert!(!p.metered);
    assert!(
        (p.list_cost - 18.0).abs() < 1e-9,
        "list_cost {}",
        p.list_cost
    );
    assert!((p.subscription_fee - 200.0).abs() < 1e-9);
    let ratio = p.ratio.expect("ratio present");
    assert!((ratio - 200.0 / 18.0).abs() < 1e-9, "ratio {ratio}");
    // Workspace scope: effective cost allocates the whole flat fee.
    assert!(
        (p.effective_cost - 200.0).abs() < 1e-6,
        "effective {}",
        p.effective_cost
    );
    assert_eq!(p.billable_tokens, 2_000_000);
    // Month bucket is the identity case: the fine bucket IS the month.
    assert_eq!(p.bucket_start, p.month_start);
    assert_eq!(p.month_start, JAN_2025);
    // Input/output split sums back to the effective cost.
    assert!(
        (p.effective_input_cost + p.effective_output_cost - p.effective_cost).abs() < 1e-6,
        "split {} + {} vs {}",
        p.effective_input_cost,
        p.effective_output_cost,
        p.effective_cost
    );
    // 1M input @ $3/Mtok = $3 list, 1M output @ $15/Mtok = $15 list, so the
    // input side is 3/18 of the allocated fee.
    assert!(
        (p.effective_input_cost - 200.0 * (3.0 / 18.0)).abs() < 1e-6,
        "input side {}",
        p.effective_input_cost
    );
    assert_eq!(p.input_tokens, 1_000_000);
    assert_eq!(p.output_tokens, 1_000_000);
}

#[tokio::test]
async fn effective_cost_sums_to_fee_across_models() {
    let db = cost_db().await;
    // Two claude models in the same month roll up to one (month, backend)
    // point whose effective cost is exactly the fee.
    seed_run(&db, "son", "claude", "sonnet").await;
    seed_run(&db, "op", "claude", "opus").await;
    add_event(
        &db,
        "s1",
        "son",
        "assistant",
        JAN_2025 + 10,
        1_000_000,
        1_000_000,
        None,
    )
    .await;
    add_event(
        &db,
        "o1",
        "op",
        "assistant",
        JAN_2025 + 20,
        1_000_000,
        1_000_000,
        None,
    )
    .await;

    let points = effective_cost(
        &db,
        &Scope::default(),
        &TimeRange::default(),
        &fees(&[("claude", 200.0)]),
        Bucket::Month,
    )
    .await
    .unwrap();
    let claude: Vec<_> = points.iter().filter(|p| p.backend == "claude").collect();
    assert_eq!(claude.len(), 1, "one claude point per month");
    assert!((claude[0].effective_cost - 200.0).abs() < 1e-6);
}

#[tokio::test]
async fn effective_cost_zero_priced_usage_yields_no_ratio() {
    let db = cost_db().await;
    // An unpriced model: billable tokens exist but list cost is $0, so the
    // ratio is undefined rather than NaN/Inf.
    seed_run(&db, "m", "claude", "mystery-model").await;
    add_event(&db, "m1", "m", "assistant", JAN_2025 + 5, 1000, 1000, None).await;

    let points = effective_cost(
        &db,
        &Scope::default(),
        &TimeRange::default(),
        &fees(&[("claude", 200.0)]),
        Bucket::Month,
    )
    .await
    .unwrap();
    let p = point(&points, JAN_2025, "claude");
    assert!(p.ratio.is_none());
    assert_eq!(p.effective_cost, 0.0);
    assert!((p.subscription_fee - 200.0).abs() < 1e-9);
    assert!(p.effective_cost.is_finite());
}

#[tokio::test]
async fn effective_cost_subscription_without_fee_is_not_metered() {
    let db = cost_db().await;
    // Claude is a subscription backend: with no configured fee and no real
    // per-generation cost, it must NOT be reported as metered. The card
    // shows the list-price estimate ($18) rather than a fabricated "actual".
    seed_run(&db, "c", "claude", "sonnet").await;
    add_event(
        &db,
        "c1",
        "c",
        "assistant",
        JAN_2025 + 100,
        1_000_000,
        1_000_000,
        None,
    )
    .await;

    let points = effective_cost(
        &db,
        &Scope::default(),
        &TimeRange::default(),
        &fees(&[]),
        Bucket::Month,
    )
    .await
    .unwrap();
    let p = point(&points, JAN_2025, "claude");
    assert!(!p.metered, "claude is a subscription backend, not metered");
    assert_eq!(p.subscription_fee, 0.0);
    assert!(p.ratio.is_none());
    assert!(
        (p.scoped_cost - 18.0).abs() < 1e-9,
        "list estimate {}",
        p.scoped_cost
    );
}

#[tokio::test]
async fn effective_cost_metered_backend_uses_real_cost() {
    let db = cost_db().await;
    // OpenRouter (no configured fee) bills its assistant event for tokens and
    // settles the real dollar cost on the result event. The real cost ($0.42)
    // must win over the price-table estimate ($0.018 for sonnet).
    seed_run(&db, "or", "openrouter", "sonnet").await;
    add_event(
        &db,
        "or1",
        "or",
        "assistant",
        JAN_2025 + 30,
        1000,
        1000,
        None,
    )
    .await;
    add_event(
        &db,
        "or2",
        "or",
        "result:success",
        JAN_2025 + 31,
        0,
        0,
        Some(0.42),
    )
    .await;

    let points = effective_cost(
        &db,
        &Scope::default(),
        &TimeRange::default(),
        &fees(&[("claude", 200.0)]),
        Bucket::Month,
    )
    .await
    .unwrap();
    let p = point(&points, JAN_2025, "openrouter");
    assert!(p.metered);
    assert_eq!(p.ratio, Some(1.0));
    assert!(
        (p.effective_cost - 0.42).abs() < 1e-9,
        "effective {}",
        p.effective_cost
    );
    assert!((p.scoped_cost - 0.42).abs() < 1e-9);
}

#[tokio::test]
async fn effective_cost_fine_buckets_inherit_month_ratio_and_sum_to_fee() {
    let db = cost_db().await;
    // Two claude turns in January on different days. At Day granularity each
    // day is its own point, both attributed to January, each inheriting
    // January's workspace ratio, and the day points sum to the flat fee.
    seed_run(&db, "d1", "claude", "sonnet").await;
    seed_run(&db, "d2", "claude", "sonnet").await;
    add_event(
        &db,
        "e1",
        "d1",
        "assistant",
        JAN_2025 + 100,
        1_000_000,
        1_000_000,
        None,
    )
    .await;
    add_event(
        &db,
        "e2",
        "d2",
        "assistant",
        JAN_2025 + 8 * 86_400,
        1_000_000,
        1_000_000,
        None,
    )
    .await;

    let points = effective_cost(
        &db,
        &Scope::default(),
        &TimeRange::default(),
        &fees(&[("claude", 200.0)]),
        Bucket::Day,
    )
    .await
    .unwrap();

    let claude: Vec<_> = points.iter().filter(|p| p.backend == "claude").collect();
    assert_eq!(claude.len(), 2, "two distinct day points");
    let mut days: Vec<i64> = claude.iter().map(|p| p.bucket_start).collect();
    days.sort();
    assert_eq!(days, vec![JAN_2025, JAN_2025 + 8 * 86_400]);
    for p in &claude {
        assert_eq!(p.month_start, JAN_2025, "both days are January");
        // Workspace January list = 2 days * $18 = $36, so ratio = 200/36.
        let ratio = p.ratio.expect("ratio");
        assert!((ratio - 200.0 / 36.0).abs() < 1e-9, "ratio {ratio}");
    }
    let total: f64 = claude.iter().map(|p| p.effective_cost).sum();
    assert!(
        (total - 200.0).abs() < 1e-6,
        "day points sum to fee: {total}"
    );
}

#[tokio::test]
async fn effective_cost_metered_split_apportions_by_list_fraction() {
    let db = cost_db().await;
    // OpenRouter sonnet: 1M input + 1M output (list $3 + $15 = $18), settled at
    // a real $9. The real cost is apportioned by the list-price fraction:
    // input 3/18, output 15/18.
    seed_run(&db, "or", "openrouter", "sonnet").await;
    add_event(
        &db,
        "or1",
        "or",
        "assistant",
        JAN_2025 + 30,
        1_000_000,
        1_000_000,
        None,
    )
    .await;
    add_event(
        &db,
        "or2",
        "or",
        "result:success",
        JAN_2025 + 31,
        0,
        0,
        Some(9.0),
    )
    .await;

    let points = effective_cost(
        &db,
        &Scope::default(),
        &TimeRange::default(),
        &fees(&[]),
        Bucket::Month,
    )
    .await
    .unwrap();
    let p = point(&points, JAN_2025, "openrouter");
    assert!(p.metered);
    assert!(
        (p.effective_cost - 9.0).abs() < 1e-9,
        "effective {}",
        p.effective_cost
    );
    assert!(
        (p.effective_input_cost - 9.0 * (3.0 / 18.0)).abs() < 1e-6,
        "input {}",
        p.effective_input_cost
    );
    assert!(
        (p.effective_output_cost - 9.0 * (15.0 / 18.0)).abs() < 1e-6,
        "output {}",
        p.effective_output_cost
    );
    assert!((p.effective_input_cost + p.effective_output_cost - 9.0).abs() < 1e-6);
}

#[tokio::test]
async fn model_role_economics_uses_real_metered_cost() {
    let db = cost_db().await;
    // A metered model absent from the price table (price-table cost == $0).
    // The economics table must still report the real recorded dollar cost.
    seed_run(&db, "z", "openrouter", "z-ai/glm").await;
    add_event(&db, "z1", "z", "assistant", JAN_2025 + 10, 1000, 1000, None).await;
    add_event(
        &db,
        "z2",
        "z",
        "result:success",
        JAN_2025 + 11,
        0,
        0,
        Some(0.33),
    )
    .await;

    // The price table genuinely returns $0 for this model.
    assert_eq!(
        pricing::cost_usd("openrouter", Some("z-ai/glm"), 1000, 0, 0, 1000),
        0.0
    );

    let out = model_role_economics(&db, &Scope::default(), &TimeRange::default())
        .await
        .unwrap();
    let row = econ(&out.by_model, "z-ai/glm");
    assert!(
        (row.cost_usd - 0.33).abs() < 1e-9,
        "real metered cost surfaced: {}",
        row.cost_usd
    );
}

#[tokio::test]
async fn effective_cost_buckets_by_calendar_month_and_flags_provisional() {
    use chrono::{Datelike, TimeZone, Utc};
    let db = cost_db().await;
    seed_run(&db, "c", "claude", "sonnet").await;
    // One event in January, one in February: two distinct month buckets.
    add_event(&db, "j", "c", "assistant", JAN_2025 + 100, 1000, 1000, None).await;
    add_event(&db, "f", "c", "assistant", FEB_2025 + 100, 1000, 1000, None).await;
    // One event in the current wall-clock month: must be flagged provisional.
    let now = Utc::now();
    let now_month = Utc
        .with_ymd_and_hms(now.year(), now.month(), 1, 0, 0, 0)
        .single()
        .unwrap()
        .timestamp();
    add_event(
        &db,
        "n",
        "c",
        "assistant",
        now.timestamp(),
        1000,
        1000,
        None,
    )
    .await;

    let points = effective_cost(
        &db,
        &Scope::default(),
        &TimeRange::default(),
        &fees(&[]),
        Bucket::Month,
    )
    .await
    .unwrap();

    assert!(!point(&points, JAN_2025, "claude").provisional);
    assert!(!point(&points, FEB_2025, "claude").provisional);
    assert!(point(&points, now_month, "claude").provisional);
    assert_ne!(JAN_2025, FEB_2025);
}

#[tokio::test]
async fn cost_timeseries_prefers_exact_metered_cost() {
    let db = cost_db().await;
    seed_run(&db, "or", "openrouter", "sonnet").await;
    add_event(
        &db,
        "or1",
        "or",
        "assistant",
        JAN_2025 + 30,
        1000,
        1000,
        None,
    )
    .await;
    add_event(
        &db,
        "or2",
        "or",
        "result:success",
        JAN_2025 + 31,
        0,
        0,
        Some(0.42),
    )
    .await;

    let points = cost_timeseries(&db, &Scope::default(), &TimeRange::default(), Bucket::Month)
        .await
        .unwrap();
    let p = point_cost(&points, JAN_2025);
    // Real metered cost ($0.42), not the price-table estimate ($0.018).
    assert!((p.cost_usd - 0.42).abs() < 1e-9, "cost {}", p.cost_usd);
}

#[tokio::test]
async fn provider_model_costs_uses_real_metered_cost_and_filters_backend() {
    let db = cost_db().await;
    // OpenRouter: bills tokens on the assistant event, settles the real dollar
    // cost ($0.42) on the result event.
    seed_run(&db, "or", "openrouter", "sonnet").await;
    add_event(
        &db,
        "or1",
        "or",
        "assistant",
        JAN_2025 + 30,
        1000,
        1000,
        None,
    )
    .await;
    add_event(
        &db,
        "or2",
        "or",
        "result:success",
        JAN_2025 + 31,
        0,
        0,
        Some(0.42),
    )
    .await;
    // A claude run in the same workspace must be excluded by the backend filter.
    seed_run(&db, "c", "claude", "opus").await;
    add_event(&db, "c1", "c", "assistant", JAN_2025 + 40, 1000, 1000, None).await;

    let rows = provider_model_costs(&db, "openrouter", &Scope::new(None), &TimeRange::default())
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "only the openrouter model is returned");
    assert_eq!(rows[0].model, "sonnet");
    // Real metered cost ($0.42), not the price-table estimate (~$0.018).
    assert!(
        (rows[0].cost_usd - 0.42).abs() < 1e-9,
        "cost {}",
        rows[0].cost_usd
    );
    assert_eq!(rows[0].billable_tokens, 2000);
    assert_eq!(rows[0].runs, 1);
}

#[tokio::test]
async fn provider_model_costs_empty_without_usage() {
    let db = cost_db().await;
    let rows = provider_model_costs(&db, "openrouter", &Scope::new(None), &TimeRange::default())
        .await
        .unwrap();
    assert!(rows.is_empty());
}

fn point_cost(points: &[CostPoint], bucket_start: i64) -> &CostPoint {
    points
        .iter()
        .find(|p| p.bucket_start == bucket_start)
        .unwrap_or_else(|| panic!("missing cost point @ {bucket_start}"))
}

// --- Tier A token/cost rollup: oracle equality, idempotency, concurrency ---

/// Two UTC days used by the rollup oracle seed (day floors 86400 / 172800).
const DAY1: i64 = 100_000;
const DAY2: i64 = 180_000;

/// A workspace spanning two projects, two days, and the Claude / codex /
/// OpenRouter backends, including the two cost-population edge cases the
/// rollup must reproduce: a metered settlement event that is NOT a billable
/// token event (OpenRouter), and a (model, backend) group that has cost but
/// no billable tokens at all (the tok-side guard).
async fn oracle_db() -> LocalDb {
    let temp = tempdir().unwrap();
    let db = LocalDb::open(temp.path().join("rollup-oracle.db"))
        .await
        .unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    db.execute_script(
            "
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p1', 'default', 'P1', 'P1', '/tmp/p1', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p2', 'default', 'P2', 'P2', '/tmp/p2', 1, 1);

            -- claude (p1, sonnet, builder): two billable assistant turns spanning
            -- two days, plus a cumulative result the claude rule must skip.
            INSERT INTO jobs(id, project_id, status, model, node_name, agent_config_id, created_at, updated_at)
             VALUES ('jc', 'p1', 'merged', 'sonnet', 'builder', 'builder', 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES ('sc', 'jc', 'claude', 'open', 1, 1, 1);
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES ('rc', 'p1', 'jc', 'exited', 'sc', 1, 1);

            -- codex (p1, gpt-5, planner): two billable result turns, plus an
            -- assistant the codex rule must skip.
            INSERT INTO jobs(id, project_id, status, model, node_name, agent_config_id, created_at, updated_at)
             VALUES ('jx', 'p1', 'running', 'gpt-5', 'planner-0', 'planner-0', 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES ('sx', 'jx', 'codex', 'open', 1, 1, 1);
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES ('rx', 'p1', 'jx', 'exited', 'sx', 1, 1);

            -- openrouter (p1, sonnet, builder): a billable assistant on day 1, a
            -- settlement result carrying real cost on day 1 (same group, not a
            -- billable token event), and a cost-only settlement on day 2 (a
            -- group with cost but no billable tokens on that day).
            INSERT INTO jobs(id, project_id, status, model, node_name, agent_config_id, created_at, updated_at)
             VALUES ('jo', 'p1', 'running', 'sonnet', 'builder', 'builder', 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES ('so', 'jo', 'openrouter', 'open', 1, 1, 1);
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES ('ro', 'p1', 'jo', 'exited', 'so', 1, 1);

            -- openrouter (p1, gemini, explore): ONLY a cost settlement event, no
            -- billable tokens. Locks the tok-side guard: this (model, backend)
            -- group must never surface in the token-driven views.
            INSERT INTO jobs(id, project_id, status, model, node_name, agent_config_id, created_at, updated_at)
             VALUES ('jg', 'p1', 'running', 'gemini', 'explore', 'explore', 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES ('sg', 'jg', 'openrouter', 'open', 1, 1, 1);
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES ('rg', 'p1', 'jg', 'exited', 'sg', 1, 1);

            -- p2 claude (opus, builder): a second project for scope filtering.
            INSERT INTO jobs(id, project_id, status, model, node_name, agent_config_id, created_at, updated_at)
             VALUES ('jp2', 'p2', 'merged', 'opus', 'builder', 'builder', 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES ('sp2', 'jp2', 'claude', 'open', 1, 1, 1);
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES ('rp2', 'p2', 'jp2', 'exited', 'sp2', 1, 1);
            ",
        )
        .await
        .unwrap();

    // claude: c1 (day1) billable 160, c2 (day2) billable 240, c3 result skipped.
    ins_ev(
        &db,
        "c1",
        "rc",
        "sc",
        1,
        "assistant",
        DAY1,
        100,
        10,
        20,
        30,
        5,
        None,
    )
    .await;
    ins_ev(
        &db,
        "c2",
        "rc",
        "sc",
        2,
        "assistant",
        DAY2,
        200,
        0,
        0,
        40,
        2,
        None,
    )
    .await;
    ins_ev(
        &db,
        "c3",
        "rc",
        "sc",
        3,
        "result:success",
        DAY1,
        9999,
        9999,
        9999,
        9999,
        0,
        None,
    )
    .await;
    // codex: x1 (day1) billable 110, x2 (day2) billable 220, x3 assistant skipped.
    ins_ev(
        &db,
        "x1",
        "rx",
        "sx",
        1,
        "result:success",
        DAY1,
        100,
        50,
        0,
        10,
        0,
        None,
    )
    .await;
    ins_ev(
        &db,
        "x2",
        "rx",
        "sx",
        2,
        "result:success",
        DAY2,
        200,
        80,
        0,
        20,
        0,
        None,
    )
    .await;
    ins_ev(
        &db,
        "x3",
        "rx",
        "sx",
        3,
        "assistant",
        DAY1,
        8888,
        0,
        0,
        8888,
        0,
        None,
    )
    .await;
    // openrouter sonnet: o1 (day1) billable 2000; o2 (day1) settlement $0.42 on
    // a non-billable result; o3 (day2) cost-only $0.10 (no billable that day).
    ins_ev(
        &db,
        "o1",
        "ro",
        "so",
        1,
        "assistant",
        DAY1,
        1000,
        0,
        0,
        1000,
        0,
        None,
    )
    .await;
    ins_ev(
        &db,
        "o2",
        "ro",
        "so",
        2,
        "result:success",
        DAY1,
        0,
        0,
        0,
        0,
        0,
        Some(0.42),
    )
    .await;
    ins_ev(
        &db,
        "o3",
        "ro",
        "so",
        3,
        "result:success",
        DAY2,
        0,
        0,
        0,
        0,
        0,
        Some(0.10),
    )
    .await;
    // openrouter gemini: cost-only, no billable tokens anywhere.
    ins_ev(
        &db,
        "g1",
        "rg",
        "sg",
        1,
        "result:success",
        DAY1,
        0,
        0,
        0,
        0,
        0,
        Some(0.05),
    )
    .await;
    // p2 claude opus: billable 550 on day1.
    ins_ev(
        &db,
        "p1e",
        "rp2",
        "sp2",
        1,
        "assistant",
        DAY1,
        500,
        0,
        0,
        50,
        0,
        None,
    )
    .await;

    // Merge requests for tokens_per_loc: jc opened day1, jo opened day2.
    db.execute_script(
            "
            INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch,
                target_branch, status, opened_at, updated_at, additions, deletions)
             VALUES ('mrc', 'jc', 'p1', NULL, 'PR c', 'agent/c', 'main', 'merged', 100000, 100000, 100, 60);
            INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch,
                target_branch, status, opened_at, updated_at, additions, deletions)
             VALUES ('mro', 'jo', 'p1', NULL, 'PR o', 'agent/o', 'main', 'merged', 180000, 180000, 50, 50);
            ",
        )
        .await
        .unwrap();
    db
}

/// Insert one event with all token columns, a session id, and an optional
/// real `cost_usd`.
#[allow(clippy::too_many_arguments)]
async fn ins_ev(
    db: &LocalDb,
    id: &str,
    run: &str,
    sess: &str,
    seq: i64,
    etype: &str,
    ts: i64,
    input: i64,
    cread: i64,
    ccreate: i64,
    output: i64,
    thinking: i64,
    cost: Option<f64>,
) {
    let cost_sql = cost.map_or_else(|| "NULL".to_string(), |c| c.to_string());
    db.execute_script(&format!(
        "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens, cost_usd)
             VALUES ('{id}', '{run}', '{sess}', {seq}, {ts}, '{etype}', '{{}}',
                NULL, {ts}, {input}, {cread}, {ccreate}, {output}, {thinking}, {cost_sql});"
    ))
    .await
    .unwrap();
}

fn assert_cost_components_eq(
    rollup: Vec<queries::CostComponentRow>,
    live: Vec<queries::CostComponentRow>,
    ctx: &str,
) {
    let key = |r: &queries::CostComponentRow| {
        (
            r.bucket_start,
            r.model.clone().unwrap_or_default(),
            r.backend.clone(),
        )
    };
    let mut r = rollup;
    let mut l = live;
    r.sort_by_key(key);
    l.sort_by_key(key);
    assert_eq!(
        r.len(),
        l.len(),
        "cost_components len ({ctx}):\n{r:#?}\nvs\n{l:#?}"
    );
    for (a, b) in r.iter().zip(l.iter()) {
        assert_eq!(a.bucket_start, b.bucket_start, "{ctx}");
        assert_eq!(a.model, b.model, "{ctx}");
        assert_eq!(a.backend, b.backend, "{ctx}");
        assert_eq!(a.input, b.input, "{ctx}");
        assert_eq!(a.cache_read, b.cache_read, "{ctx}");
        assert_eq!(a.cache_create, b.cache_create, "{ctx}");
        assert_eq!(a.output, b.output, "{ctx}");
        assert_eq!(a.billable, b.billable, "{ctx}");
        assert!((a.exact_cost - b.exact_cost).abs() < 1e-9, "{ctx} cost");
        assert_eq!(a.exact_cost_count, b.exact_cost_count, "{ctx}");
    }
}

fn assert_token_components_eq(
    rollup: Vec<queries::TokenComponentRow>,
    live: Vec<queries::TokenComponentRow>,
    ctx: &str,
) {
    let mut r = rollup;
    let mut l = live;
    r.sort_by_key(|x| x.bucket_start);
    l.sort_by_key(|x| x.bucket_start);
    assert_eq!(
        r.len(),
        l.len(),
        "token_components len ({ctx}):\n{r:#?}\nvs\n{l:#?}"
    );
    for (a, b) in r.iter().zip(l.iter()) {
        assert_eq!(a.bucket_start, b.bucket_start, "{ctx}");
        assert_eq!(a.input, b.input, "{ctx}");
        assert_eq!(a.cache_read, b.cache_read, "{ctx}");
        assert_eq!(a.cache_create, b.cache_create, "{ctx}");
        assert_eq!(a.output, b.output, "{ctx}");
        assert_eq!(a.thinking, b.thinking, "{ctx}");
    }
}

fn assert_provider_eq(
    rollup: Vec<queries::ProviderModelComponentRow>,
    live: Vec<queries::ProviderModelComponentRow>,
    ctx: &str,
) {
    let key = |r: &queries::ProviderModelComponentRow| {
        (r.model.clone().unwrap_or_default(), r.backend.clone())
    };
    let mut r = rollup;
    let mut l = live;
    r.sort_by_key(key);
    l.sort_by_key(key);
    assert_eq!(
        r.len(),
        l.len(),
        "provider len ({ctx}):\n{r:#?}\nvs\n{l:#?}"
    );
    for (a, b) in r.iter().zip(l.iter()) {
        assert_eq!(a.model, b.model, "{ctx}");
        assert_eq!(a.backend, b.backend, "{ctx}");
        assert_eq!(a.input, b.input, "{ctx}");
        assert_eq!(a.cache_read, b.cache_read, "{ctx}");
        assert_eq!(a.cache_create, b.cache_create, "{ctx}");
        assert_eq!(a.output, b.output, "{ctx}");
        assert_eq!(a.billable, b.billable, "{ctx}");
        assert_eq!(a.runs, b.runs, "{ctx}");
        assert!((a.exact_cost - b.exact_cost).abs() < 1e-9, "{ctx} cost");
        assert_eq!(a.exact_cost_count, b.exact_cost_count, "{ctx}");
    }
}

fn assert_granular_eq(
    rollup: Vec<queries::GranularRow>,
    live: Vec<queries::GranularRow>,
    ctx: &str,
) {
    let key = |r: &queries::GranularRow| {
        (
            r.model.clone().unwrap_or_default(),
            r.agent_config_id.clone().unwrap_or_default(),
            r.backend.clone(),
        )
    };
    let mut r = rollup;
    let mut l = live;
    r.sort_by_key(key);
    l.sort_by_key(key);
    assert_eq!(
        r.len(),
        l.len(),
        "granular len ({ctx}):\n{r:#?}\nvs\n{l:#?}"
    );
    for (a, b) in r.iter().zip(l.iter()) {
        assert_eq!(a.model, b.model, "{ctx}");
        assert_eq!(a.agent_config_id, b.agent_config_id, "{ctx}");
        assert_eq!(a.backend, b.backend, "{ctx}");
        assert_eq!(a.input, b.input, "{ctx}");
        assert_eq!(a.cache_read, b.cache_read, "{ctx}");
        assert_eq!(a.cache_create, b.cache_create, "{ctx}");
        assert_eq!(a.output, b.output, "{ctx}");
        assert_eq!(a.thinking, b.thinking, "{ctx}");
        assert_eq!(a.billable, b.billable, "{ctx}");
        assert_eq!(a.runs, b.runs, "{ctx}");
        assert!((a.exact_cost - b.exact_cost).abs() < 1e-9, "{ctx} cost");
        assert_eq!(a.exact_cost_count, b.exact_cost_count, "{ctx}");
    }
}

fn assert_session_eq(
    rollup: Vec<queries::SessionBucketRow>,
    live: Vec<queries::SessionBucketRow>,
    ctx: &str,
) {
    let mut r = rollup;
    let mut l = live;
    r.sort_by_key(|s| s.bucket_start);
    l.sort_by_key(|s| s.bucket_start);
    assert_eq!(r.len(), l.len(), "session len ({ctx}):\n{r:#?}\nvs\n{l:#?}");
    for (a, b) in r.iter().zip(l.iter()) {
        assert_eq!(a.bucket_start, b.bucket_start, "{ctx}");
        assert_eq!(a.session_count, b.session_count, "{ctx}");
        assert!((a.avg_tokens - b.avg_tokens).abs() < 1e-9, "{ctx} avg");
    }
}

fn assert_loc_eq(rollup: Vec<queries::LocRow>, live: Vec<queries::LocRow>, ctx: &str) {
    let mut r = rollup;
    let mut l = live;
    r.sort_by(|a, b| a.job_id.cmp(&b.job_id));
    l.sort_by(|a, b| a.job_id.cmp(&b.job_id));
    assert_eq!(r.len(), l.len(), "loc len ({ctx}):\n{r:#?}\nvs\n{l:#?}");
    for (a, b) in r.iter().zip(l.iter()) {
        assert_eq!(a.job_id, b.job_id, "{ctx}");
        assert_eq!(a.ts, b.ts, "{ctx}");
        assert_eq!(a.billable, b.billable, "{ctx}");
        assert_eq!(a.lines, b.lines, "{ctx}");
        assert_eq!(a.model, b.model, "{ctx}");
        assert_eq!(a.node_name, b.node_name, "{ctx}");
        assert_eq!(a.backend, b.backend, "{ctx}");
        assert_eq!(a.input, b.input, "{ctx}");
        assert_eq!(a.cache_read, b.cache_read, "{ctx}");
        assert_eq!(a.cache_create, b.cache_create, "{ctx}");
        assert_eq!(a.output, b.output, "{ctx}");
        assert!((a.exact_cost - b.exact_cost).abs() < 1e-9, "{ctx} cost");
        assert_eq!(a.exact_cost_count, b.exact_cost_count, "{ctx}");
    }
}

/// A stable snapshot of the rollup table (floats stringified for `Eq`).
async fn dump_rollup(db: &LocalDb) -> Vec<(String, i64, i64, String, i64)> {
    db.query_all(
        "SELECT id, billable_tokens, token_event_count, exact_cost, exact_cost_count
             FROM token_rollup ORDER BY id",
        (),
        |row| {
            Ok((
                row.text(0)?,
                row.i64(1)?,
                row.i64(2)?,
                format!("{:.6}", row.opt_f64(3)?.unwrap_or(0.0)),
                row.i64(4)?,
            ))
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn rollup_matches_live_oracle() {
    let db = oracle_db().await;
    // oracle_db seeds via raw SQL (ins_ev), leaving the rollup cold so the
    // idempotency/concurrency tests below can fold it from scratch. Fold here
    // before asserting rollup == live, as the one-time backfill would.
    queries::fold_token_rollup(&db).await.unwrap();
    let scopes = [Scope::new(None), Scope::new(Some("p1".to_string()))];
    // Every range start is UTC-hour-aligned (here day-aligned), so the
    // hour-grain rollup matches the per-event live scan exactly at every
    // bucket -- including Hour, where bucket_expr on the hour-floored
    // bucket_start is the identity floor: unbounded, day-1 only, day-2 onward.
    let ranges = [
        TimeRange::new(None, None),
        TimeRange::new(Some(86_400), Some(172_800)),
        TimeRange::new(Some(172_800), None),
    ];
    let buckets = [Bucket::Hour, Bucket::Day, Bucket::Week, Bucket::Month];

    for scope in &scopes {
        for range in &ranges {
            for &bucket in &buckets {
                let ctx = format!(
                    "scope={:?} range={:?} bucket={:?}",
                    scope.project_id,
                    (range.start, range.end),
                    bucket
                );
                assert_cost_components_eq(
                    queries::cost_components(&db, scope, range, bucket)
                        .await
                        .unwrap(),
                    queries::cost_components_live(&db, scope, range, bucket)
                        .await
                        .unwrap(),
                    &ctx,
                );
                assert_session_eq(
                    queries::avg_tokens_per_session(&db, scope, range, bucket)
                        .await
                        .unwrap(),
                    queries::avg_tokens_per_session_live(&db, scope, range, bucket)
                        .await
                        .unwrap(),
                    &ctx,
                );
                assert_token_components_eq(
                    queries::token_components(&db, scope, range, bucket)
                        .await
                        .unwrap(),
                    queries::token_components_live(&db, scope, range, bucket)
                        .await
                        .unwrap(),
                    &ctx,
                );
            }
            let ctx = format!(
                "scope={:?} range={:?}",
                scope.project_id,
                (range.start, range.end)
            );
            assert_granular_eq(
                queries::economics_granular(&db, scope, range)
                    .await
                    .unwrap(),
                queries::economics_granular_live(&db, scope, range)
                    .await
                    .unwrap(),
                &ctx,
            );
            for backend in ["claude", "codex", "openrouter"] {
                assert_provider_eq(
                    queries::provider_model_components(&db, backend, scope, range)
                        .await
                        .unwrap(),
                    queries::provider_model_components_live(&db, backend, scope, range)
                        .await
                        .unwrap(),
                    &format!("{ctx} backend={backend}"),
                );
            }
            assert_loc_eq(
                queries::tokens_per_loc(&db, scope, range).await.unwrap(),
                queries::tokens_per_loc_live(&db, scope, range)
                    .await
                    .unwrap(),
                &ctx,
            );
        }
    }

    // The tok-side guard: the cost-only (gemini, openrouter) group never
    // appears in any token-driven view, even though it carries real cost.
    let costs =
        queries::cost_components(&db, &Scope::new(None), &TimeRange::default(), Bucket::Day)
            .await
            .unwrap();
    assert!(
        !costs.iter().any(|r| r.model.as_deref() == Some("gemini")),
        "cost-only group must be dropped from the token-driven view"
    );
    let provider = queries::provider_model_components(
        &db,
        "openrouter",
        &Scope::new(None),
        &TimeRange::default(),
    )
    .await
    .unwrap();
    assert!(
        !provider
            .iter()
            .any(|r| r.model.as_deref() == Some("gemini")),
        "cost-only group must be dropped from the provider view"
    );
}

#[tokio::test]
async fn fold_token_rollup_is_idempotent() {
    let db = oracle_db().await;
    queries::fold_token_rollup(&db).await.unwrap();
    let first = dump_rollup(&db).await;
    assert!(!first.is_empty());
    queries::fold_token_rollup(&db).await.unwrap();
    let second = dump_rollup(&db).await;
    assert_eq!(first, second, "a second fold must change nothing");
}

#[tokio::test]
async fn fold_token_rollup_concurrent_matches_single() {
    // A single fold defines the expected rollup contents.
    let single = oracle_db().await;
    queries::fold_token_rollup(&single).await.unwrap();
    let baseline = dump_rollup(&single).await;

    // Several folds racing on a cold rollup (as an analytics page's parallel
    // queries do) must converge on exactly the same rows -- no double
    // delete-then-reinsert, no PK conflict, no partial state.
    let concurrent = oracle_db().await;
    let (a, b, c, d) = tokio::join!(
        queries::fold_token_rollup(&concurrent),
        queries::fold_token_rollup(&concurrent),
        queries::fold_token_rollup(&concurrent),
        queries::fold_token_rollup(&concurrent),
    );
    a.unwrap();
    b.unwrap();
    c.unwrap();
    d.unwrap();
    assert_eq!(dump_rollup(&concurrent).await, baseline);
}

// --- Per-event maintenance equality + sub-day (hourly) read path ---

/// Insert one event through the real durable-event seam
/// (`insert_event_conn`), so the per-event rollup maintenance runs exactly as
/// it does in production.
async fn seam_insert(db: &LocalDb, ev: crate::transcripts::stream_store::EventInsert) {
    db.write(|conn| {
        let ev = ev.clone();
        Box::pin(async move {
            crate::transcripts::stream_store::insert_event_conn(conn, &ev).await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
fn ev_insert(
    id: &str,
    run: &str,
    sess: &str,
    seq: i32,
    etype: &str,
    ts: i32,
    input: i32,
    cread: i32,
    ccreate: i32,
    output: i32,
    thinking: i32,
    cost: Option<f64>,
) -> crate::transcripts::stream_store::EventInsert {
    crate::transcripts::stream_store::EventInsert {
        id: id.to_string(),
        run_id: run.to_string(),
        session_id: Some(sess.to_string()),
        sequence: seq,
        timestamp: ts,
        event_type: etype.to_string(),
        data: "{}".to_string(),
        parent_tool_use_id: None,
        created_at: ts,
        input_tokens: Some(input),
        cache_read_tokens: Some(cread),
        cache_create_tokens: Some(ccreate),
        output_tokens: Some(output),
        thinking_tokens: Some(thinking),
        turn_id: None,
        cost_usd: cost,
    }
}

#[tokio::test]
async fn per_event_rollup_matches_refold() {
    // Seed only dimension rows, then insert events through the real insert
    // seam so the per-event maintenance (not the fold) populates the rollup.
    let db = cost_db().await;
    seed_run(&db, "a", "claude", "sonnet").await;
    seed_run(&db, "b", "codex", "gpt-5").await;
    seed_run(&db, "c", "openrouter", "sonnet").await;

    let events = [
        ev_insert(
            "a1",
            "run-a",
            "sess-a",
            1,
            "assistant",
            100,
            10,
            20,
            30,
            40,
            5,
            None,
        ),
        // claude skips a cumulative result event.
        ev_insert(
            "a2",
            "run-a",
            "sess-a",
            2,
            "result:success",
            110,
            9,
            9,
            9,
            9,
            0,
            None,
        ),
        // codex bills its result event.
        ev_insert(
            "b1",
            "run-b",
            "sess-b",
            1,
            "result:success",
            100,
            100,
            50,
            0,
            10,
            0,
            None,
        ),
        // codex skips assistant.
        ev_insert(
            "b2",
            "run-b",
            "sess-b",
            2,
            "assistant",
            110,
            8,
            0,
            0,
            8,
            0,
            None,
        ),
        ev_insert(
            "c1",
            "run-c",
            "sess-c",
            1,
            "assistant",
            100,
            1000,
            0,
            0,
            1000,
            0,
            None,
        ),
        // a metered settlement event: real cost but not a billable token event.
        ev_insert(
            "c2",
            "run-c",
            "sess-c",
            2,
            "result:success",
            150,
            0,
            0,
            0,
            0,
            0,
            Some(0.42),
        ),
    ];
    for ev in events {
        seam_insert(&db, ev).await;
    }

    let incremental = dump_rollup(&db).await;
    assert!(
        !incremental.is_empty(),
        "per-event maintenance populated the rollup"
    );

    // A full re-fold (delete + reinsert per run) must reproduce identical
    // rows: summing the per-event increments equals the batch fold.
    queries::fold_token_rollup(&db).await.unwrap();
    assert_eq!(
        incremental,
        dump_rollup(&db).await,
        "per-event rollup must equal a full re-fold"
    );
}

#[tokio::test]
async fn hourly_bucket_serves_subday_window_from_rollup() {
    let db = cost_db().await;
    seed_run(&db, "h", "claude", "sonnet").await;
    // Three assistant turns one hour apart inside a 24h window. add_event
    // folds after each insert, so the hour rows are in the rollup. base is
    // hour-aligned so the rollup's bucket_start range filter includes the
    // first hour exactly, as the frontend's hour-floored window start does.
    let base: i64 = 1_700_000_000 / 3600 * 3600;
    add_event(&db, "h1", "h", "assistant", base, 1000, 100, None).await;
    add_event(&db, "h2", "h", "assistant", base + 3600, 1000, 100, None).await;
    add_event(&db, "h3", "h", "assistant", base + 7200, 1000, 100, None).await;

    // A 24h window at hour granularity yields one point per populated hour,
    // served DIRECTLY from the hour-grain rollup (bucket_expr(Hour) on the
    // hour-floored bucket_start is the identity floor).
    let rows = queries::cost_components(
        &db,
        &Scope::new(None),
        &TimeRange::new(Some(base), Some(base + 86_400)),
        Bucket::Hour,
    )
    .await
    .unwrap();
    let floor = |t: i64| (t / 3600) * 3600;
    let buckets: std::collections::BTreeSet<i64> = rows.iter().map(|r| r.bucket_start).collect();
    assert_eq!(buckets.len(), 3, "three distinct hourly buckets: {rows:#?}");
    assert!(buckets.contains(&floor(base)));
    assert!(buckets.contains(&floor(base + 3600)));
    assert!(buckets.contains(&floor(base + 7200)));
}

/// §D acceptance guard: a metered settlement event that lands in a DIFFERENT
/// UTC hour than its turn's billable tokens must still have its cost counted
/// by the aggregate economics views. This FAILS on a naive hour-grain of the
/// old co-location gate (the cost-only hour row is dropped) and PASSES under
/// the independent-cost attribution -- the guard `rollup_matches_live_oracle`
/// structurally cannot be, since it re-grains both sides together.
#[tokio::test]
async fn economics_metered_cost_survives_cross_hour_settlement() {
    let db = cost_db().await;
    seed_run(&db, "or", "openrouter", "glm").await;
    // A billable turn at hour H, then a SEPARATE metered settlement event two
    // hours later (same UTC day, same run/session/model/agent). At the hour
    // grain these land in two distinct rollup rows: one with tokens/no cost,
    // one with cost/no tokens.
    let h: i64 = JAN_2025 + 100;
    add_event(&db, "or1", "or", "assistant", h, 1000, 500, None).await;
    add_event(
        &db,
        "or2",
        "or",
        "result:success",
        h + 7200,
        0,
        0,
        Some(0.50),
    )
    .await;

    let out = model_role_economics(&db, &Scope::default(), &TimeRange::default())
        .await
        .unwrap();
    // The metered $0.50 must appear in BOTH the by-model and by-role views.
    assert!(
        (econ(&out.by_model, "glm").cost_usd - 0.50).abs() < 1e-9,
        "by_model cost {}",
        econ(&out.by_model, "glm").cost_usd
    );
    assert!(
        (econ(&out.by_role, "Builder").cost_usd - 0.50).abs() < 1e-9,
        "by_role cost {}",
        econ(&out.by_role, "Builder").cost_usd
    );
    // cost_by_project (already independent-cost) must include it too.
    let projects = cost_by_project(&db, &Scope::default(), &TimeRange::default())
        .await
        .unwrap();
    let proj = projects
        .iter()
        .find(|p| p.project_id == "proj")
        .expect("proj");
    assert!(
        proj.cost_usd >= 0.50 - 1e-9,
        "project cost {}",
        proj.cost_usd
    );
}

#[tokio::test]
async fn cost_by_backend_sums_to_blended_total() {
    let db = oracle_db().await;
    queries::fold_token_rollup(&db).await.unwrap();
    let split =
        cost_by_backend_timeseries(&db, &Scope::new(None), &TimeRange::default(), Bucket::Day)
            .await
            .unwrap();
    let blended = cost_timeseries(&db, &Scope::new(None), &TimeRange::default(), Bucket::Day)
        .await
        .unwrap();
    let split_total: f64 = split.iter().map(|p| p.cost_usd).sum();
    let blended_total: f64 = blended.iter().map(|p| p.cost_usd).sum();
    assert!(
        (split_total - blended_total).abs() < 1e-9,
        "provider split {split_total} must sum to blended {blended_total}"
    );
    // OpenRouter settled $0.42 on a day-1 billable group; the $0.10 day-2
    // settlement has no billable token that day, so the token-side guard drops
    // it from this (and the blended) view. The metered $0.42 must still beat
    // the ~$0 price-table estimate.
    let or: f64 = split
        .iter()
        .filter(|p| p.backend == "openrouter")
        .map(|p| p.cost_usd)
        .sum();
    assert!((or - 0.42).abs() < 1e-9, "openrouter metered cost {or}");
}

#[tokio::test]
async fn cost_by_project_folds_prefers_exact_and_joins_names() {
    let db = oracle_db().await;
    queries::fold_token_rollup(&db).await.unwrap();
    let rows = cost_by_project(&db, &Scope::new(None), &TimeRange::default())
        .await
        .unwrap();
    let p1 = rows.iter().find(|r| r.project_id == "p1").expect("p1");
    let p2 = rows.iter().find(|r| r.project_id == "p2").expect("p2");
    assert_eq!(p1.project_name, "P1");
    assert_eq!(p2.project_name, "P2");
    // p1's openrouter usage settled $0.52 of real cost, which must be folded in.
    assert!(p1.cost_usd >= 0.52 - 1e-9, "p1 cost {}", p1.cost_usd);
    // Sorted by cost descending.
    assert!(rows.windows(2).all(|w| w[0].cost_usd >= w[1].cost_usd));
}

#[tokio::test]
async fn tokens_per_loc_cost_prefers_exact_metered() {
    let db = oracle_db().await;
    queries::fold_token_rollup(&db).await.unwrap();
    let rows = tokens_per_loc(&db, &Scope::new(None), &TimeRange::default())
        .await
        .unwrap();
    // job jo (openrouter) settled $0.42 + $0.10 = $0.52; exact wins.
    let jo = rows.iter().find(|r| r.job_id == "jo").expect("jo");
    assert!((jo.cost_usd - 0.52).abs() < 1e-9, "jo cost {}", jo.cost_usd);
    // job jc (claude) has no metered cost: price-table estimate, still > 0.
    let jc = rows.iter().find(|r| r.job_id == "jc").expect("jc");
    assert!(jc.cost_usd > 0.0, "jc cost {}", jc.cost_usd);
}

#[tokio::test]
async fn tool_error_rate_is_errors_over_total_per_bucket() {
    let db = fixture_db().await;
    // One assistant event with two tool uses, plus a tool_result marking one
    // as an error: total 2, errors 1 in the day-1 bucket.
    db.execute_script(
            "
            INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES ('run-tools', 'proj', 'job-claude', 'exited', 'sess-claude', 1, 1);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at)
             VALUES ('ev-a', 'run-tools', 'sess-claude', 1, 1, 'assistant',
                '{\"eventType\":\"assistant\",\"isError\":false,\"toolUses\":[{\"id\":\"tu1\",\"name\":\"mcp__cairn__read\",\"input\":{\"paths\":[\"file:src/lib.rs\"]}},{\"id\":\"tu2\",\"name\":\"run\",\"input\":{\"commands\":[{\"command\":\"cargo test\"}]}}]}',
                NULL, 90000);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at)
             VALUES ('ev-r', 'run-tools', 'sess-claude', 2, 2, 'tool_result',
                '{\"eventType\":\"tool_result\",\"toolUseId\":\"tu1\",\"toolName\":\"read\",\"toolResult\":\"boom\",\"isError\":true}',
                NULL, 90001);
            ",
        )
        .await
        .unwrap();
    backfill_tool_invocations(&db).await.unwrap();
    let rows = tool_error_rate(&db, &Scope::default(), &TimeRange::default(), Bucket::Day)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].bucket_start, 86400);
    assert_eq!(rows[0].total, 2);
    assert_eq!(rows[0].errors, 1);
    assert!((rows[0].error_rate - 0.5).abs() < 1e-9);
}

#[tokio::test]
async fn usage_heatmap_applies_tz_offset_to_local_day() {
    let db = cost_db().await;
    seed_run(&db, "h", "claude", "sonnet").await;
    // 1970-01-04 (Sunday) 01:00:00 UTC = epoch 262800 (a whole-hour floor).
    // A claude assistant turn there: billable = input + output = 10 + 5 = 15.
    add_event(&db, "h1", "h", "assistant", 262800, 10, 5, None).await;
    // UTC: Sunday (dow 0) at 01:00 (hour 1), summing billable tokens.
    let utc = usage_heatmap(&db, &Scope::default(), &TimeRange::default(), 0)
        .await
        .unwrap();
    assert_eq!(utc.len(), 1);
    assert_eq!(utc[0].day_of_week, 0);
    assert_eq!(utc[0].hour, 1);
    assert_eq!(utc[0].tokens, 15);
    // UTC-2 shifts it back across the day boundary to Saturday (dow 6) at 23:00.
    let local = usage_heatmap(&db, &Scope::default(), &TimeRange::default(), -120)
        .await
        .unwrap();
    assert_eq!(local.len(), 1);
    assert_eq!(local[0].day_of_week, 6);
    assert_eq!(local[0].hour, 23);
    assert_eq!(local[0].tokens, 15);
}

#[tokio::test]
async fn usage_heatmap_sums_billable_tokens_per_cell() {
    let db = cost_db().await;
    seed_run(&db, "h", "claude", "sonnet").await;
    // Two assistant turns in the same UTC hour (Sunday 01:00): 15 + 25 = 40.
    add_event(&db, "h1", "h", "assistant", 262800, 10, 5, None).await;
    add_event(&db, "h2", "h", "assistant", 262800 + 120, 20, 5, None).await;
    // A turn one hour later (Sunday 02:00): a separate cell of 30.
    add_event(&db, "h3", "h", "assistant", 262800 + 3600, 25, 5, None).await;
    // A cost-only settlement in the first hour contributes no billable tokens.
    add_event(
        &db,
        "h4",
        "h",
        "result:success",
        262800 + 200,
        0,
        0,
        Some(0.1),
    )
    .await;

    let cells = usage_heatmap(&db, &Scope::default(), &TimeRange::default(), 0)
        .await
        .unwrap();
    let sun_1 = cells
        .iter()
        .find(|c| c.day_of_week == 0 && c.hour == 1)
        .expect("sunday 01:00");
    let sun_2 = cells
        .iter()
        .find(|c| c.day_of_week == 0 && c.hour == 2)
        .expect("sunday 02:00");
    assert_eq!(sun_1.tokens, 40); // 15 + 25
    assert_eq!(sun_2.tokens, 30);
    // The cost-only settlement produces no zero-token cell.
    assert!(!cells.iter().any(|c| c.tokens == 0));
}

#[tokio::test]
async fn token_composition_sums_components_per_bucket() {
    let db = fixture_db().await;
    let rows =
        token_composition_timeseries(&db, &Scope::default(), &TimeRange::default(), Bucket::Day)
            .await
            .unwrap();
    assert_eq!(rows.len(), 1, "single day bucket");
    let r = &rows[0];
    assert_eq!(r.bucket_start, 86400);
    // Claude c1 (10/100/20/5/3) + c2 (12/0/0/8/1); codex x1 (100/50/0/10/0)
    // + x2 (200/80/0/20/0). Columns are input/cache_read/cache_create/output/
    // thinking. (codex cache subsumed in billing but components still stored.)
    assert_eq!(r.input, 322); // 10 + 12 + 100 + 200
    assert_eq!(r.cache_read, 230); // 100 + 50 + 80
    assert_eq!(r.cache_create, 20); // 20 only
    assert_eq!(r.output, 43); // 5 + 8 + 10 + 20
    assert_eq!(r.thinking, 4); // 3 + 1
}

#[test]
fn bucket_parses_hour() {
    assert_eq!(Bucket::parse(Some("hour")), Bucket::Hour);
    assert_eq!(Bucket::parse(Some("HOUR")), Bucket::Hour);
    assert_eq!(Bucket::parse(Some("week")), Bucket::Week);
    assert_eq!(Bucket::parse(None), Bucket::Day);
}
