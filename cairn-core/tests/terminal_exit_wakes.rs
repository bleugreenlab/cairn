//! Integration coverage for terminal-exit wake subscriptions: the agent-facing
//! `kind:"terminal"` subscribe alias, already-exited immediate fire, and the
//! precise unknown-slug error.

mod common;

use cairn_core::internal::orchestrator::wakes;
use cairn_core::internal::storage::LocalDb;
use common::{change_resource, resource_orchestrator_fixture};
use serde_json::json;
use turso::params;

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
                "INSERT INTO runs(id, project_id, job_id, chat_id, status, session_id, backend, created_at, updated_at, start_mode)
                 VALUES ('r-1', ?1, 'job-1', NULL, 'live', 'session-1', 'codex', 1, 1, 'resume')",
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
