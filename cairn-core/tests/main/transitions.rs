//! Integration tests for job outcome transitions and failure cascades.

use crate::common;
use std::sync::Arc;

use cairn_core::internal::db::DbState;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::{CapturingEmitter, TestServicesBuilder};
use cairn_core::internal::services::EventEmitter;
use cairn_core::internal::storage::{LocalDb, RowExt, SearchIndex};
use serde_json::{json, Value};
use tempfile::{tempdir, TempDir};
use turso::params;

#[derive(Clone)]
struct SharedEmitter(Arc<CapturingEmitter>);

impl EventEmitter for SharedEmitter {
    fn emit(&self, event: &str, payload: Value) -> Result<(), String> {
        self.0.emit(event, payload)
    }

    fn emit_empty(&self, event: &str) -> Result<(), String> {
        self.0.emit_empty(event)
    }
}

struct TransitionContext {
    orch: Orchestrator,
    db: Arc<LocalDb>,
    emitter: Arc<CapturingEmitter>,
    _db_temp: TempDir,
    _temp: TempDir,
}

async fn transition_context() -> TransitionContext {
    let temp = tempdir().unwrap();
    let (db_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db.clone(), search_index));
    let emitter = Arc::new(CapturingEmitter::new());
    let services = Arc::new(
        TestServicesBuilder::new()
            .with_emitter(SharedEmitter(emitter.clone()))
            .build(),
    );
    let orch = Orchestrator::builder(db_state, services, temp.path().join("config")).build();

    TransitionContext {
        orch,
        db,
        emitter,
        _db_temp: db_temp,
        _temp: temp,
    }
}

async fn insert_execution_with_snapshot(
    db: &LocalDb,
    exec_id: &str,
    issue_id: Option<&str>,
    snapshot: &Value,
) {
    let exec_id = exec_id.to_string();
    let issue_id = issue_id.map(str::to_string);
    let snapshot = serde_json::to_string(snapshot).unwrap();
    let now = chrono::Utc::now().timestamp();

    db.execute(
        "
        INSERT INTO executions (
            id, recipe_id, issue_id, status, started_at, seq, snapshot
        )
        VALUES (?1, 'recipe-1', ?2, 'running', ?3, 1, ?4)
        ",
        params![
            exec_id.as_str(),
            issue_id.as_deref(),
            now,
            snapshot.as_str()
        ],
    )
    .await
    .unwrap();
}

async fn insert_issue(db: &LocalDb, issue_id: &str, project_id: &str) {
    let issue_id = issue_id.to_string();
    let project_id = project_id.to_string();
    let now = chrono::Utc::now().timestamp();

    db.execute(
        "
        INSERT INTO issues (
            id, project_id, number, title, status, priority, created_at, updated_at
        )
        VALUES (?1, ?2, 1, 'Test', 'active', 0, ?3, ?4)
        ",
        params![issue_id.as_str(), project_id.as_str(), now, now],
    )
    .await
    .unwrap();
}

async fn insert_job(
    db: &LocalDb,
    id: &str,
    status: &str,
    project_id: &str,
    execution_id: Option<&str>,
    recipe_node_id: Option<&str>,
) {
    let id = id.to_string();
    let status = status.to_string();
    let project_id = project_id.to_string();
    let execution_id = execution_id.map(str::to_string);
    let recipe_node_id = recipe_node_id.map(str::to_string);
    let now = chrono::Utc::now().timestamp();

    db.execute(
        "
        INSERT INTO jobs (
            id, execution_id, recipe_node_id, status, project_id, created_at, updated_at
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ",
        params![
            id.as_str(),
            execution_id.as_deref(),
            recipe_node_id.as_deref(),
            status.as_str(),
            project_id.as_str(),
            now,
            now
        ],
    )
    .await
    .unwrap();
}

fn build_snapshot(edges: Vec<(&str, &str, &str)>) -> Value {
    let mut node_ids = std::collections::HashSet::new();
    let mut edge_list = Vec::new();

    for (id_suffix, source, target) in &edges {
        node_ids.insert(*source);
        node_ids.insert(*target);
        edge_list.push(json!({
            "id": format!("edge-{id_suffix}"),
            "edgeType": "control",
            "sourceNodeId": source,
            "sourceHandle": "out",
            "targetNodeId": target,
            "targetHandle": "in"
        }));
    }

    let nodes: Vec<Value> = node_ids
        .into_iter()
        .map(|node_id| {
            json!({
                "id": node_id,
                "name": node_id,
                "nodeType": "agent",
                "position": { "x": 0.0, "y": 0.0 },
                "agentConfig": { "agentConfigId": "test-agent" }
            })
        })
        .collect();

    json!({
        "recipe": {
            "id": "recipe-1",
            "name": "test-recipe",
            "trigger": "issue",
            "nodes": nodes,
            "edges": edge_list
        },
        "agents": {},
        "skills": {},
        "tools": {},
        "triggerContext": {
            "projectId": "test-project",
            "triggerType": "manual"
        },
        "createdAt": 0
    })
}

/// Fail a job the way the runtime now does: record the failure as a turn fact,
/// then let the projection derive Failed and cascade to downstream jobs.
fn apply_failure(orch: &Orchestrator, job_id: &str) {
    cairn_core::internal::execution::advancement::mark_job_failed(orch, job_id, "test failure")
        .expect("mark_job_failed");
}

/// Attach a live (running) turn to a job so the projection derives Running.
async fn insert_running_turn(db: &LocalDb, turn_id: &str, job_id: &str) {
    let turn_id = turn_id.to_string();
    let job_id = job_id.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let turn_id = turn_id.clone();
        let job_id = job_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason,
                                    created_at, started_at, updated_at)
                 VALUES (?1, ?2, ?3, 1, 'running', 'initial', ?4, ?4, ?4)",
                params![
                    turn_id.as_str(),
                    format!("session-{job_id}").as_str(),
                    job_id.as_str(),
                    now
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// Attach an interrupted turn to a job — the state a user-initiated stop leaves
/// the latest turn in. An interrupt is a pause, not a failure.
async fn insert_interrupted_turn(db: &LocalDb, turn_id: &str, job_id: &str) {
    let turn_id = turn_id.to_string();
    let job_id = job_id.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let turn_id = turn_id.clone();
        let job_id = job_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason,
                                    created_at, started_at, updated_at)
                 VALUES (?1, ?2, ?3, 1, 'interrupted', 'initial', ?4, ?4, ?4)",
                params![
                    turn_id.as_str(),
                    format!("session-{job_id}").as_str(),
                    job_id.as_str(),
                    now
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn status(db: &LocalDb, table: &'static str, id: &str) -> String {
    let id = id.to_string();
    let sql = format!("SELECT status FROM {table} WHERE id = ?1");
    db.read(|conn| {
        let id = id.clone();
        let sql = sql.clone();
        Box::pin(async move {
            let mut rows = conn.query(&sql, params![id.as_str()]).await?;
            let row = rows.next().await?.expect("status row");
            row.text(0)
        })
    })
    .await
    .unwrap()
}

async fn completed_at(db: &LocalDb, job_id: &str) -> Option<i64> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT completed_at FROM jobs WHERE id = ?1",
                    params![job_id.as_str()],
                )
                .await?;
            let row = rows.next().await?.expect("job row");
            row.opt_i64(0)
        })
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn cascade_failure_linear_chain() {
    let ctx = transition_context().await;
    let project_id = common::create_project(&ctx.db, "TCFL").await;
    let snapshot = build_snapshot(vec![("1", "node-a", "node-b"), ("2", "node-b", "node-c")]);
    insert_execution_with_snapshot(&ctx.db, "exec-cascade-1", None, &snapshot).await;
    insert_job(
        &ctx.db,
        "job-a",
        "running",
        &project_id,
        Some("exec-cascade-1"),
        Some("node-a"),
    )
    .await;
    insert_job(
        &ctx.db,
        "job-b",
        "pending",
        &project_id,
        Some("exec-cascade-1"),
        Some("node-b"),
    )
    .await;
    insert_job(
        &ctx.db,
        "job-c",
        "pending",
        &project_id,
        Some("exec-cascade-1"),
        Some("node-c"),
    )
    .await;

    apply_failure(&ctx.orch, "job-a");

    assert_eq!(status(&ctx.db, "jobs", "job-b").await, "failed");
    assert_eq!(status(&ctx.db, "jobs", "job-c").await, "failed");
}

#[tokio::test]
async fn interrupt_does_not_cascade_to_downstream() {
    // Regression: interrupting an upstream job (e.g. stopping a planner mid-run)
    // must NOT fail it or cascade to downstream builder/reviewer jobs. An
    // interrupt is a resumable pause — downstream stay pending so that continuing
    // the job and approving its plan can still start them.
    let ctx = transition_context().await;
    let project_id = common::create_project(&ctx.db, "TINT").await;
    let snapshot = build_snapshot(vec![("1", "node-a", "node-b"), ("2", "node-b", "node-c")]);
    insert_execution_with_snapshot(&ctx.db, "exec-interrupt-1", None, &snapshot).await;
    insert_job(
        &ctx.db,
        "job-ia",
        "running",
        &project_id,
        Some("exec-interrupt-1"),
        Some("node-a"),
    )
    .await;
    insert_job(
        &ctx.db,
        "job-ib",
        "pending",
        &project_id,
        Some("exec-interrupt-1"),
        Some("node-b"),
    )
    .await;
    insert_job(
        &ctx.db,
        "job-ic",
        "pending",
        &project_id,
        Some("exec-interrupt-1"),
        Some("node-c"),
    )
    .await;

    // The upstream job's latest turn was interrupted, then recompute runs.
    insert_interrupted_turn(&ctx.db, "turn-ia", "job-ia").await;
    cairn_core::internal::execution::advancement::recompute_execution_jobs(
        &ctx.orch,
        "exec-interrupt-1",
    )
    .expect("recompute");

    // Upstream is not failed (resumable), and the interrupt never cascaded.
    assert_ne!(status(&ctx.db, "jobs", "job-ia").await, "failed");
    assert_eq!(status(&ctx.db, "jobs", "job-ib").await, "pending");
    assert_eq!(status(&ctx.db, "jobs", "job-ic").await, "pending");
}

#[tokio::test]
async fn cascade_failure_fan_out() {
    let ctx = transition_context().await;
    let project_id = common::create_project(&ctx.db, "TCFO").await;
    let snapshot = build_snapshot(vec![("1", "node-a", "node-b"), ("2", "node-a", "node-c")]);
    insert_execution_with_snapshot(&ctx.db, "exec-cascade-2", None, &snapshot).await;
    insert_job(
        &ctx.db,
        "job-a2",
        "running",
        &project_id,
        Some("exec-cascade-2"),
        Some("node-a"),
    )
    .await;
    insert_job(
        &ctx.db,
        "job-b2",
        "pending",
        &project_id,
        Some("exec-cascade-2"),
        Some("node-b"),
    )
    .await;
    insert_job(
        &ctx.db,
        "job-c2",
        "pending",
        &project_id,
        Some("exec-cascade-2"),
        Some("node-c"),
    )
    .await;

    apply_failure(&ctx.orch, "job-a2");

    assert_eq!(status(&ctx.db, "jobs", "job-b2").await, "failed");
    assert_eq!(status(&ctx.db, "jobs", "job-c2").await, "failed");
}

#[tokio::test]
async fn cascade_failure_fan_in_only_affects_downstream_of_failed_job() {
    let ctx = transition_context().await;
    let project_id = common::create_project(&ctx.db, "TCFI").await;
    let snapshot = build_snapshot(vec![("1", "node-a", "node-c"), ("2", "node-b", "node-c")]);
    insert_execution_with_snapshot(&ctx.db, "exec-cascade-3", None, &snapshot).await;
    insert_job(
        &ctx.db,
        "job-a3",
        "running",
        &project_id,
        Some("exec-cascade-3"),
        Some("node-a"),
    )
    .await;
    insert_job(
        &ctx.db,
        "job-b3",
        "running",
        &project_id,
        Some("exec-cascade-3"),
        Some("node-b"),
    )
    .await;
    // node-b3 is genuinely running (live turn) and is not downstream of the
    // failed node-a3, so it must stay Running.
    insert_running_turn(&ctx.db, "turn-b3", "job-b3").await;
    insert_job(
        &ctx.db,
        "job-c3",
        "pending",
        &project_id,
        Some("exec-cascade-3"),
        Some("node-c"),
    )
    .await;

    apply_failure(&ctx.orch, "job-a3");

    assert_eq!(status(&ctx.db, "jobs", "job-c3").await, "failed");
    assert_eq!(status(&ctx.db, "jobs", "job-b3").await, "running");
}

#[tokio::test]
async fn cascade_failure_skips_running_downstream() {
    let ctx = transition_context().await;
    let project_id = common::create_project(&ctx.db, "TCSR").await;
    let snapshot = build_snapshot(vec![("1", "node-a", "node-b")]);
    insert_execution_with_snapshot(&ctx.db, "exec-cascade-4", None, &snapshot).await;
    insert_job(
        &ctx.db,
        "job-a4",
        "running",
        &project_id,
        Some("exec-cascade-4"),
        Some("node-a"),
    )
    .await;
    insert_job(
        &ctx.db,
        "job-b4",
        "running",
        &project_id,
        Some("exec-cascade-4"),
        Some("node-b"),
    )
    .await;
    // A genuinely running downstream job has a live turn; the projection keeps it
    // Running rather than cascade-failing active work.
    insert_running_turn(&ctx.db, "turn-b4", "job-b4").await;

    apply_failure(&ctx.orch, "job-a4");

    assert_eq!(status(&ctx.db, "jobs", "job-b4").await, "running");
}

#[tokio::test]
async fn cascade_failure_standalone_job_no_execution() {
    let ctx = transition_context().await;
    let project_id = common::create_project(&ctx.db, "TCSJ").await;
    insert_job(
        &ctx.db,
        "job-standalone",
        "running",
        &project_id,
        None,
        None,
    )
    .await;

    apply_failure(&ctx.orch, "job-standalone");

    assert_eq!(status(&ctx.db, "jobs", "job-standalone").await, "failed");
}

#[tokio::test]
async fn cascade_failure_recomputes_execution_and_issue() {
    let ctx = transition_context().await;
    let project_id = common::create_project(&ctx.db, "TCRE").await;
    insert_issue(&ctx.db, "issue-cascade", &project_id).await;
    let snapshot = build_snapshot(vec![("1", "node-a", "node-b")]);
    insert_execution_with_snapshot(&ctx.db, "exec-cascade-5", Some("issue-cascade"), &snapshot)
        .await;
    insert_job(
        &ctx.db,
        "job-a5",
        "running",
        &project_id,
        Some("exec-cascade-5"),
        Some("node-a"),
    )
    .await;
    insert_job(
        &ctx.db,
        "job-b5",
        "pending",
        &project_id,
        Some("exec-cascade-5"),
        Some("node-b"),
    )
    .await;

    apply_failure(&ctx.orch, "job-a5");

    assert_eq!(
        status(&ctx.db, "executions", "exec-cascade-5").await,
        "failed"
    );
    assert_eq!(status(&ctx.db, "issues", "issue-cascade").await, "failed");
}

#[tokio::test]
async fn cascade_failure_sets_completed_at_on_downstream_jobs() {
    let ctx = transition_context().await;
    let project_id = common::create_project(&ctx.db, "TCCA").await;
    let snapshot = build_snapshot(vec![("1", "node-a", "node-b")]);
    insert_execution_with_snapshot(&ctx.db, "exec-cascade-ca", None, &snapshot).await;
    insert_job(
        &ctx.db,
        "job-ca-a",
        "running",
        &project_id,
        Some("exec-cascade-ca"),
        Some("node-a"),
    )
    .await;
    insert_job(
        &ctx.db,
        "job-ca-b",
        "pending",
        &project_id,
        Some("exec-cascade-ca"),
        Some("node-b"),
    )
    .await;

    apply_failure(&ctx.orch, "job-ca-a");

    assert!(completed_at(&ctx.db, "job-ca-b").await.is_some());
}

#[tokio::test]
async fn cascade_failure_emits_core_db_change_events() {
    let ctx = transition_context().await;
    let project_id = common::create_project(&ctx.db, "TCDE").await;
    let snapshot = build_snapshot(vec![("1", "node-a", "node-b"), ("2", "node-b", "node-c")]);
    insert_execution_with_snapshot(&ctx.db, "exec-cascade-ev", None, &snapshot).await;
    insert_job(
        &ctx.db,
        "job-ev-a",
        "running",
        &project_id,
        Some("exec-cascade-ev"),
        Some("node-a"),
    )
    .await;
    insert_job(
        &ctx.db,
        "job-ev-b",
        "pending",
        &project_id,
        Some("exec-cascade-ev"),
        Some("node-b"),
    )
    .await;
    insert_job(
        &ctx.db,
        "job-ev-c",
        "pending",
        &project_id,
        Some("exec-cascade-ev"),
        Some("node-c"),
    )
    .await;

    apply_failure(&ctx.orch, "job-ev-a");

    let db_changes = ctx.emitter.events_named("db-change");
    assert!(db_changes
        .iter()
        .any(|payload| payload.get("table").and_then(|value| value.as_str()) == Some("jobs")));
    assert!(db_changes.iter().any(
        |payload| payload.get("table").and_then(|value| value.as_str()) == Some("executions")
    ));
    assert!(db_changes
        .iter()
        .any(|payload| payload.get("table").and_then(|value| value.as_str()) == Some("issues")));
}

#[tokio::test]
async fn cascade_failure_diamond_pattern() {
    let ctx = transition_context().await;
    let project_id = common::create_project(&ctx.db, "TCDP").await;
    let snapshot = build_snapshot(vec![
        ("1", "node-a", "node-b"),
        ("2", "node-a", "node-c"),
        ("3", "node-b", "node-d"),
        ("4", "node-c", "node-d"),
    ]);
    insert_execution_with_snapshot(&ctx.db, "exec-diamond", None, &snapshot).await;
    for (job_id, node_id, status) in [
        ("job-dia-a", "node-a", "running"),
        ("job-dia-b", "node-b", "pending"),
        ("job-dia-c", "node-c", "pending"),
        ("job-dia-d", "node-d", "pending"),
    ] {
        insert_job(
            &ctx.db,
            job_id,
            status,
            &project_id,
            Some("exec-diamond"),
            Some(node_id),
        )
        .await;
    }

    apply_failure(&ctx.orch, "job-dia-a");

    for job_id in ["job-dia-b", "job-dia-c", "job-dia-d"] {
        assert_eq!(status(&ctx.db, "jobs", job_id).await, "failed");
    }
}
