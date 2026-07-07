//! Integration coverage for terminal-exit wake subscriptions: the agent-facing
//! `kind:"terminal"` subscribe alias, already-exited immediate fire, and the
//! precise unknown-slug error.

use crate::common;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::common::{change_resource, resource_orchestrator_fixture};
use cairn_core::internal::db::DbState;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::{wakes, Orchestrator};
use cairn_core::internal::services::{
    testing::TestServicesBuilder, RealPtyFactory, TerminalOutputWatcher,
};
use cairn_core::internal::storage::{LocalDb, RowExt, SearchIndex};
use cairn_db::turso::params;
use serde_json::{json, Value};

async fn real_pty_orchestrator_fixture() -> (tempfile::TempDir, Arc<LocalDb>, Orchestrator) {
    let temp = tempfile::tempdir().unwrap();
    let (_db_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db.clone(), search_index));
    let services = Arc::new(
        TestServicesBuilder::new()
            .with_pty_factory(RealPtyFactory)
            .build(),
    );
    let orch = Orchestrator::builder(db_state, services, temp.path().join("config")).build();
    (temp, db, orch)
}

async fn set_project_repo_path(db: &LocalDb, repo_path: &std::path::Path) {
    let repo_path = repo_path.to_string_lossy().to_string();
    db.execute(
        "UPDATE projects SET repo_path = ?1 WHERE key = 'TXW'",
        params![repo_path.as_str()],
    )
    .await
    .unwrap();
}

async fn change_resource_as_run(orch: &Orchestrator, changes: Value, run_id: &str) -> String {
    let request = McpCallbackRequest {
        thread_id: None,
        cwd: String::new(),
        run_id: Some(run_id.to_string()),
        tool: "write".to_string(),
        payload: json!({ "changes": changes }),
        tool_use_id: None,
    };
    handle_write(orch, &request).await
}

async fn terminal_count(db: &LocalDb, slug: &str) -> i64 {
    let slug = slug.to_string();
    db.read(|conn| {
        let slug = slug.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(*) FROM job_terminals WHERE slug = ?1",
                    params![slug.as_str()],
                )
                .await?;
            let row = rows.next().await?.expect("count row");
            row.i64(0)
        })
    })
    .await
    .unwrap()
}

async fn wait_for_output_wake_consumed(db: &LocalDb, job_id: &str, phrase: &str) {
    for _ in 0..50 {
        let subs = wakes::list_subscriptions_for_job(db, job_id).await.unwrap();
        if !subs
            .iter()
            .any(|sub| sub.match_phrase.as_deref() == Some(phrase))
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("output wake for {phrase:?} was not consumed");
}

async fn seed_node(db: &LocalDb) {
    let project_id = common::create_project(db, "TXW").await;
    db.write(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
                 VALUES ('issue-1', ?1, 1, 'Terminal exit wakes', 'active', 'none', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
                 VALUES ('exec-1', 'recipe-1', 'issue-1', ?1, 'running', 1, 1, '{}', 'manual')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
                 VALUES ('job-1', 'exec-1', 'issue-1', ?1, 'builder', 'running', 'session-1', 'builder', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            // A live session + run + active turn: a delivered terminal-exit wake
            // queues a system direct against the active turn rather than
            // spawning a live agent process in the test harness.
            conn.execute(
                "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                 VALUES ('session-1', 'job-1', 'active', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, job_id, chat_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('r-1', ?1, 'job-1', NULL, 'live', 'session-1', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
                 VALUES ('turn-1', 'session-1', 'r-1', 'job-1', 1, 'running', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_turn_id = 'turn-1' WHERE id = 'job-1'",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn insert_terminal(
    db: &LocalDb,
    slug: &str,
    status: &str,
    exit_code: Option<i64>,
    output_tail: Option<&str>,
) {
    let id = uuid::Uuid::new_v4().to_string();
    let slug = slug.to_string();
    let status = status.to_string();
    let tail = output_tail.map(str::to_string);
    let exited_at = if status == "exited" {
        Some(50i64)
    } else {
        None
    };
    db.write(|conn| {
        let id = id.clone();
        let slug = slug.clone();
        let status = status.clone();
        let tail = tail.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO job_terminals
                   (id, job_id, project_id, session_id, command, status, exit_code, created_at, exited_at, slug, output_tail)
                 VALUES (?1, 'job-1', NULL, ?2, 'sleep 999', ?3, ?4, 1, ?5, ?6, ?7)",
                params![
                    id.as_str(),
                    format!("sess-{slug}"),
                    status.as_str(),
                    exit_code,
                    exited_at,
                    slug.as_str(),
                    tail.as_deref()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn subscribe_running_terminal_creates_one_shot_process_subscription() {
    let (_t, db, orch) = resource_orchestrator_fixture().await;
    seed_node(&db).await;
    insert_terminal(&db, "run-1", "running", None, None).await;

    let out = change_resource(
        &orch,
        json!([{
            "target": "cairn://p/TXW/1/1/builder/wakes",
            "mode": "append",
            "payload": {"subscribe": {"kind": "terminal", "ref": "cairn:~/terminal/run-1", "on": "exit"}}
        }]),
    )
    .await;
    assert!(out.contains("run-1"), "{out}");

    let subs = wakes::list_subscriptions_for_job(&db, "job-1")
        .await
        .unwrap();
    let term = subs
        .iter()
        .find(|s| s.source_kind == "process")
        .expect("terminal subscription normalized to a process source");
    assert!(term.one_shot, "terminal subscription must be one-shot");
    assert_eq!(
        term.fact_kinds.as_deref(),
        Some(&["terminal_exit".to_string()][..])
    );
    // Source ref is the canonical terminal URI (scoped), not the bare slug.
    let source_ref = term.source_ref.as_deref().expect("process source ref");
    assert!(
        source_ref.ends_with("/terminal/run-1"),
        "source_ref must be the canonical terminal URI, got {source_ref}"
    );
    assert!(source_ref.starts_with("cairn://p/"), "{source_ref}");
}

#[tokio::test(flavor = "current_thread")]
async fn same_slug_terminal_in_another_job_does_not_cross_wake() {
    let (_t, db, orch) = resource_orchestrator_fixture().await;
    seed_node(&db).await;
    insert_terminal(&db, "run-1", "running", None, None).await;

    change_resource(
        &orch,
        json!([{
            "target": "cairn://p/TXW/1/1/builder/wakes",
            "mode": "append",
            "payload": {"subscribe": {"kind": "terminal", "ref": "run-1", "on": "exit"}}
        }]),
    )
    .await;

    // Read back the canonical URI this subscription is keyed on.
    let subs = wakes::list_subscriptions_for_job(&db, "job-1")
        .await
        .unwrap();
    let uri_a = subs
        .iter()
        .find(|s| s.source_kind == "process")
        .and_then(|s| s.source_ref.clone())
        .expect("process source ref");

    // Another job's same-slug terminal exiting (different canonical URI, same
    // slug) must NOT wake job A and must leave its subscription intact.
    let other = wakes::route_terminal_exit_async(
        &orch,
        "run-1",
        "cairn://p/TXW/9/1/builder/terminal/run-1",
        Some(0),
        Some(1),
        None,
    )
    .await
    .unwrap();
    assert_eq!(other, wakes::WakeRouteAction::Dropped);
    assert!(
        wakes::list_subscriptions_for_job(&db, "job-1")
            .await
            .unwrap()
            .iter()
            .any(|s| s.source_kind == "process"),
        "a same-slug exit in another job must not consume this subscription"
    );

    // The real terminal's exit delivers and consumes the one-shot subscription.
    let mine = wakes::route_terminal_exit_async(&orch, "run-1", &uri_a, Some(0), Some(1), None)
        .await
        .unwrap();
    assert_eq!(mine, wakes::WakeRouteAction::Delivered);
    assert!(
        !wakes::list_subscriptions_for_job(&db, "job-1")
            .await
            .unwrap()
            .iter()
            .any(|s| s.source_kind == "process"),
        "the matching exit must consume the one-shot subscription"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn subscribe_already_exited_terminal_fires_and_consumes() {
    let (_t, db, orch) = resource_orchestrator_fixture().await;
    seed_node(&db).await;
    insert_terminal(&db, "run-1", "exited", Some(0), Some("build complete")).await;

    let out = change_resource(
        &orch,
        json!([{
            "target": "cairn://p/TXW/1/1/builder/wakes",
            "mode": "append",
            "payload": {"subscribe": {"kind": "terminal", "ref": "run-1", "on": "exit"}}
        }]),
    )
    .await;
    assert!(out.contains("already exited"), "{out}");

    // The immediate fire consumes the one-shot subscription, so no sleeping row
    // remains for the already-finished terminal.
    let subs = wakes::list_subscriptions_for_job(&db, "job-1")
        .await
        .unwrap();
    assert!(
        !subs
            .iter()
            .any(|s| s.source_kind == "process" && s.source_ref.as_deref() == Some("run-1")),
        "already-exited subscribe must not leave a sleeping subscription"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn subscribe_unknown_terminal_errors_with_existing_slugs() {
    let (_t, db, orch) = resource_orchestrator_fixture().await;
    seed_node(&db).await;
    insert_terminal(&db, "run-1", "running", None, None).await;

    let out = change_resource(
        &orch,
        json!([{
            "target": "cairn://p/TXW/1/1/builder/wakes",
            "mode": "append",
            "payload": {"subscribe": {"kind": "terminal", "ref": "nope", "on": "exit"}}
        }]),
    )
    .await;
    assert!(out.contains("nope"), "{out}");
    assert!(
        out.contains("run-1"),
        "error should list existing slugs: {out}"
    );

    // No subscription was created for the bad slug.
    let subs = wakes::list_subscriptions_for_job(&db, "job-1")
        .await
        .unwrap();
    assert!(!subs.iter().any(|s| s.source_kind == "process"));
}

#[tokio::test(flavor = "current_thread")]
async fn create_terminal_with_exit_wake_subscribes_to_canonical_uri() {
    let (t, db, orch) = real_pty_orchestrator_fixture().await;
    let repo_path = t.path().join("repo");
    std::fs::create_dir_all(&repo_path).unwrap();
    seed_node(&db).await;
    set_project_repo_path(&db, &repo_path).await;

    let out = change_resource_as_run(
        &orch,
        json!([{
            "target": "cairn://p/TXW/1/1/builder/terminal/wait-exit",
            "mode": "create",
            "payload": {"command": "sleep 30", "wake": "exit"}
        }]),
        "r-1",
    )
    .await;
    assert!(out.contains("subscribed to exit"), "{out}");
    assert!(
        out.contains("cairn://p/TXW/1/1/builder/terminal/wait-exit"),
        "{out}"
    );
    assert!(out.contains("end your turn"), "{out}");

    let subs = wakes::list_subscriptions_for_job(&db, "job-1")
        .await
        .unwrap();
    let term = subs
        .iter()
        .find(|s| s.source_kind == "process")
        .expect("terminal subscription normalized to a process source");
    assert!(term.one_shot);
    assert_eq!(
        term.source_ref.as_deref(),
        Some("cairn://p/TXW/1/1/builder/terminal/wait-exit")
    );
    assert_eq!(
        term.fact_kinds.as_deref(),
        Some(&["terminal_exit".to_string()][..])
    );

    let _ = change_resource(
        &orch,
        json!([{
            "target": "cairn://p/TXW/1/1/builder/terminal/wait-exit",
            "mode": "delete"
        }]),
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_terminal_with_output_wake_routes_when_phrase_prints() {
    let (t, db, orch) = real_pty_orchestrator_fixture().await;
    let repo_path = t.path().join("repo");
    std::fs::create_dir_all(&repo_path).unwrap();
    seed_node(&db).await;
    set_project_repo_path(&db, &repo_path).await;

    let out = change_resource_as_run(
        &orch,
        json!([{
            "target": "cairn://p/TXW/1/1/builder/terminal/wait-ready",
            "mode": "create",
            "payload": {"command": "sleep 1; echo x y z | tr -d ' '; sleep 30", "wake": "xyz"}
        }]),
        "r-1",
    )
    .await;
    assert!(out.contains("watching output for \\\"xyz\\\""), "{out}");

    let subs = wakes::list_subscriptions_for_job(&db, "job-1")
        .await
        .unwrap();
    let term = subs
        .iter()
        .find(|s| s.match_phrase.as_deref() == Some("xyz"))
        .expect("output wake row exists before phrase routes");
    assert_eq!(
        term.source_ref.as_deref(),
        Some("cairn://p/TXW/1/1/builder/terminal/wait-ready")
    );
    let fact_kinds = term.fact_kinds.as_deref().expect("terminal fact kinds");
    assert!(fact_kinds.contains(&"terminal_output".to_string()));
    assert!(fact_kinds.contains(&"terminal_exit".to_string()));

    let watchers = Arc::new(Mutex::new(vec![TerminalOutputWatcher {
        subscription_id: term.id.clone(),
        job_id: "job-1".to_string(),
        phrase: "xyz".to_string(),
        carry: String::new(),
        terminal_uri: "cairn://p/TXW/1/1/builder/terminal/wait-ready".to_string(),
    }]));
    wakes::scan_and_route_terminal_output(&orch, &watchers, "xyz\n");
    wait_for_output_wake_consumed(&db, "job-1", "xyz").await;

    let _ = change_resource(
        &orch,
        json!([{
            "target": "cairn://p/TXW/1/1/builder/terminal/wait-ready",
            "mode": "delete"
        }]),
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn create_terminal_rejects_invalid_wake_before_spawning() {
    let (t, db, orch) = real_pty_orchestrator_fixture().await;
    let repo_path = t.path().join("repo");
    std::fs::create_dir_all(&repo_path).unwrap();
    seed_node(&db).await;
    set_project_repo_path(&db, &repo_path).await;

    let empty = change_resource_as_run(
        &orch,
        json!([{
            "target": "cairn://p/TXW/1/1/builder/terminal/bad-empty",
            "mode": "create",
            "payload": {"command": "sleep 30", "wake": ""}
        }]),
        "r-1",
    )
    .await;
    assert!(empty.contains("payload.wake must not be empty"), "{empty}");
    assert_eq!(terminal_count(&db, "bad-empty").await, 0);

    let non_string = change_resource_as_run(
        &orch,
        json!([{
            "target": "cairn://p/TXW/1/1/builder/terminal/bad-type",
            "mode": "create",
            "payload": {"command": "sleep 30", "wake": {"on": "exit"}}
        }]),
        "r-1",
    )
    .await;
    assert!(
        non_string.contains("payload.wake must be a string"),
        "{non_string}"
    );
    assert_eq!(terminal_count(&db, "bad-type").await, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn project_terminal_create_wake_subscribes_calling_job() {
    let (t, db, orch) = real_pty_orchestrator_fixture().await;
    let repo_path = t.path().join("repo");
    std::fs::create_dir_all(&repo_path).unwrap();
    seed_node(&db).await;
    set_project_repo_path(&db, &repo_path).await;

    let out = change_resource_as_run(
        &orch,
        json!([{
            "target": "cairn://p/TXW/terminal/project-wait",
            "mode": "create",
            "payload": {"command": "sleep 30", "wake": "exit"}
        }]),
        "r-1",
    )
    .await;
    assert!(out.contains("subscribed to exit"), "{out}");

    let subs = wakes::list_subscriptions_for_job(&db, "job-1")
        .await
        .unwrap();
    let term = subs
        .iter()
        .find(|s| s.source_kind == "process")
        .expect("project terminal exit subscription");
    assert_eq!(
        term.source_ref.as_deref(),
        Some("cairn://p/TXW/terminal/project-wait")
    );

    let _ = change_resource(
        &orch,
        json!([{
            "target": "cairn://p/TXW/terminal/project-wait",
            "mode": "delete"
        }]),
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn project_terminal_wake_requires_agent_caller_before_spawning() {
    let (t, db, orch) = real_pty_orchestrator_fixture().await;
    let repo_path = t.path().join("repo");
    std::fs::create_dir_all(&repo_path).unwrap();
    seed_node(&db).await;
    set_project_repo_path(&db, &repo_path).await;

    let out = change_resource(
        &orch,
        json!([{
            "target": "cairn://p/TXW/terminal/no-agent",
            "mode": "create",
            "payload": {"command": "sleep 30", "wake": "exit"}
        }]),
    )
    .await;
    assert!(
        out.contains("payload.wake requires an agent caller"),
        "{out}"
    );
    assert_eq!(terminal_count(&db, "no-agent").await, 0);
}
