//! Integration coverage for terminal-exit wake subscriptions: the agent-facing
//! `kind:"terminal"` subscribe alias, already-exited immediate fire, and the
//! precise unknown-slug error.

use crate::common;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use crate::common::{change_resource, resource_orchestrator_fixture};
use cairn_core::internal::db::DbState;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::{wakes, Orchestrator};
use cairn_core::internal::services::{testing::TestServicesBuilder, RealPtyFactory};
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
    common::load_fleet_settings(&orch);
    common::attach_test_executor(&orch);
    (temp, db, orch)
}

async fn prepare_terminal_repository(
    db: &LocalDb,
    repo_path: &std::path::Path,
    config_dir: &std::path::Path,
) {
    std::fs::create_dir_all(repo_path).unwrap();
    for args in [
        &["init", "-b", "main", "-q"][..],
        &["config", "user.email", "test@example.com"][..],
        &["config", "user.name", "Test"][..],
    ] {
        assert!(Command::new("git")
            .args(args)
            .current_dir(repo_path)
            .status()
            .unwrap()
            .success());
    }
    std::fs::write(repo_path.join("fixture"), "terminal fixture\n").unwrap();
    for args in [&["add", "fixture"][..], &["commit", "-qm", "fixture"][..]] {
        assert!(Command::new("git")
            .args(args)
            .current_dir(repo_path)
            .status()
            .unwrap()
            .success());
    }
    let base_commit = common::head_sha(repo_path);
    let branch = "agent/txw-1-builder-0";
    let worktree = repo_path.parent().unwrap().join("agent-worktree");
    common::provision_jj_workspace(config_dir, repo_path, &worktree, branch);
    let repo_path = repo_path.to_string_lossy().to_string();
    db.execute(
        "UPDATE projects SET repo_path = ?1 WHERE key = 'txw'",
        params![repo_path.as_str()],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE jobs SET branch = 'agent/txw-1-builder-0', base_commit = ?1 WHERE id = 'job-1'",
        params![base_commit.as_str()],
    )
    .await
    .unwrap();
}

async fn change_resource_as_run(orch: &Orchestrator, changes: Value, run_id: &str) -> String {
    let request = McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: String::new(),
        run_id: Some(run_id.to_string()),
        tool: "write".to_string(),
        payload: json!({ "changes": changes }),
        tool_use_id: None,
        origin: None,
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

async fn seed_node(db: &LocalDb) {
    let project_id = common::create_project(db, "txw").await;
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
            "target": "cairn://p/txw/1/1/builder/wakes",
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
            "target": "cairn://p/txw/1/1/builder/wakes",
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
        "cairn://p/txw/9/1/builder/terminal/run-1",
        Some(0),
        Some(1),
        None,
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
    let mine =
        wakes::route_terminal_exit_async(&orch, "run-1", &uri_a, Some(0), Some(1), None, None)
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
            "target": "cairn://p/txw/1/1/builder/wakes",
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
async fn output_phrase_subscription_is_woken_by_exit_before_phrase() {
    let (_t, db, orch) = resource_orchestrator_fixture().await;
    seed_node(&db).await;
    let uri = "cairn://p/txw/1/1/builder/terminal/run-1";
    // An agent waiting for a phrase subscribes an output one-shot.
    wakes::subscribe_terminal_output_one_shot(&db, "job-1", uri, "ready", "agent")
        .await
        .unwrap();

    // The terminal dies (nonzero) before ever printing "ready".
    let action =
        wakes::route_terminal_exit_async(&orch, "run-1", uri, Some(1), Some(2), Some("boom"), None)
            .await
            .unwrap();
    assert_eq!(
        action,
        wakes::WakeRouteAction::Delivered,
        "exit before phrase must wake the agent"
    );

    // The one-shot output subscription is consumed, so the agent is not stranded.
    let subs = wakes::list_subscriptions_for_job(&db, "job-1")
        .await
        .unwrap();
    assert!(
        !subs
            .iter()
            .any(|s| s.match_phrase.as_deref() == Some("ready")),
        "exit before phrase must consume the output-phrase subscription"
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
            "target": "cairn://p/txw/1/1/builder/wakes",
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

/// The (status, exit_code) a terminal row currently records for a slug.
async fn terminal_state(db: &LocalDb, slug: &str) -> (String, Option<i64>) {
    let slug = slug.to_string();
    db.read(|conn| {
        let slug = slug.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status, exit_code FROM job_terminals WHERE slug = ?1",
                    params![slug.as_str()],
                )
                .await?;
            let row = rows.next().await?.expect("terminal row");
            Ok((row.text(0)?, row.opt_i64(1)?))
        })
    })
    .await
    .unwrap()
}

/// Block until a terminal finalizes, returning its recorded exit code. Generous
/// bound: a real PTY start goes through lease acquisition and a sandbox policy
/// build before the command ever runs.
async fn wait_for_terminal_exit(db: &LocalDb, slug: &str) -> Option<i64> {
    for _ in 0..200 {
        let (status, exit_code) = terminal_state(db, slug).await;
        if status == "exited" {
            return exit_code;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("terminal {slug} never finalized");
}

#[tokio::test(flavor = "current_thread")]
async fn create_terminal_rejects_invalid_wake_before_spawning() {
    let (t, db, orch) = real_pty_orchestrator_fixture().await;
    let repo_path = t.path().join("repo");
    seed_node(&db).await;
    prepare_terminal_repository(&db, &repo_path, &t.path().join("config")).await;

    let empty = change_resource_as_run(
        &orch,
        json!([{
            "target": "cairn://p/txw/1/1/builder/terminal/bad-empty",
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
            "target": "cairn://p/txw/1/1/builder/terminal/bad-type",
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
async fn project_terminal_wake_requires_agent_caller_before_spawning() {
    let (t, db, orch) = real_pty_orchestrator_fixture().await;
    let repo_path = t.path().join("repo");
    seed_node(&db).await;
    prepare_terminal_repository(&db, &repo_path, &t.path().join("config")).await;

    let out = change_resource(
        &orch,
        json!([{
            "target": "cairn://p/txw/terminal/no-agent",
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

/// cairn/6410, through a real PTY: a kill the job continues past (watchdog
/// recovery) leaves the job's background terminal running for the successor;
/// an operator kill still ends it.
#[tokio::test(flavor = "current_thread")]
async fn a_recovery_kill_keeps_the_jobs_terminal_and_an_operator_kill_ends_it() {
    if cairn_sandbox::skip_without_nested_sandbox(
        "a_recovery_kill_keeps_the_jobs_terminal_and_an_operator_kill_ends_it",
    ) {
        return;
    }
    use cairn_core::internal::orchestrator::lifecycle;

    let (t, db, orch) = real_pty_orchestrator_fixture().await;
    let repo_path = t.path().join("repo");
    seed_node(&db).await;
    prepare_terminal_repository(&db, &repo_path, &t.path().join("config")).await;

    change_resource_as_run(
        &orch,
        json!([{
            "target": "cairn://p/txw/1/1/builder/terminal/build",
            "mode": "create",
            "payload": {"command": "sleep 300"}
        }]),
        "r-1",
    )
    .await;
    assert_eq!(terminal_state(&db, "build").await.0, "running");

    let recovery = orch.clone();
    tokio::task::spawn_blocking(move || {
        lifecycle::kill_session_with_reason(
            &recovery,
            "r-1",
            "provider_silence_recovery",
            lifecycle::TerminalPolicy::Keep,
        )
    })
    .await
    .unwrap()
    .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        terminal_state(&db, "build").await,
        ("running".to_string(), None),
        "the job's compile outlives the provider process recovery ended"
    );

    let operator = orch.clone();
    tokio::task::spawn_blocking(move || lifecycle::kill_session(&operator, "r-1"))
        .await
        .unwrap()
        .unwrap();
    wait_for_terminal_exit(&db, "build").await;
}
