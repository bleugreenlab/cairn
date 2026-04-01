//! Integration tests for the transitions module.
//!
//! These tests exercise cascade_failure and the full transition_job cascade
//! with realistic DAG setups (execution + snapshot + jobs).

mod common;

use cairn_core::internal::schema::{executions, jobs};
use cairn_core::internal::services::testing::CapturingEmitter;
use cairn_core::models::JobStatus;
use diesel::prelude::*;
use serde_json::json;

fn insert_execution_with_snapshot(
    conn: &mut diesel::sqlite::SqliteConnection,
    exec_id: &str,
    issue_id: Option<&str>,
    snapshot: &serde_json::Value,
) {
    let now = chrono::Utc::now().timestamp() as i32;
    let snapshot_str = serde_json::to_string(snapshot).unwrap();
    diesel::sql_query(
        "INSERT INTO executions (id, recipe_id, issue_id, status, started_at, seq, snapshot) \
         VALUES (?, 'recipe-1', ?, 'running', ?, 1, ?)",
    )
    .bind::<diesel::sql_types::Text, _>(exec_id)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(issue_id)
    .bind::<diesel::sql_types::Integer, _>(now)
    .bind::<diesel::sql_types::Text, _>(&snapshot_str)
    .execute(conn)
    .unwrap();
}

fn insert_job(
    conn: &mut diesel::sqlite::SqliteConnection,
    id: &str,
    status: &str,
    project_id: &str,
    execution_id: &str,
    recipe_node_id: &str,
) {
    let now = chrono::Utc::now().timestamp() as i32;
    diesel::sql_query(
        "INSERT INTO jobs (id, execution_id, recipe_node_id, status, project_id, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind::<diesel::sql_types::Text, _>(id)
    .bind::<diesel::sql_types::Text, _>(execution_id)
    .bind::<diesel::sql_types::Text, _>(recipe_node_id)
    .bind::<diesel::sql_types::Text, _>(status)
    .bind::<diesel::sql_types::Text, _>(project_id)
    .bind::<diesel::sql_types::Integer, _>(now)
    .bind::<diesel::sql_types::Integer, _>(now)
    .execute(conn)
    .unwrap();
}

/// Build a minimal execution snapshot with the given edges.
/// Nodes are auto-generated from unique node IDs in edges.
fn build_snapshot(edges: Vec<(&str, &str, &str)>) -> serde_json::Value {
    // Collect unique node IDs
    let mut node_ids = std::collections::HashSet::new();
    let mut edge_list = Vec::new();

    for (id_suffix, source, target) in &edges {
        node_ids.insert(*source);
        node_ids.insert(*target);
        edge_list.push(json!({
            "id": format!("edge-{}", id_suffix),
            "edgeType": "control",
            "sourceNodeId": source,
            "sourceHandle": "out",
            "targetNodeId": target,
            "targetHandle": "in"
        }));
    }

    let nodes: Vec<serde_json::Value> = node_ids
        .into_iter()
        .map(|nid| {
            json!({
                "id": nid,
                "name": nid,
                "nodeType": "agent",
                "position": { "x": 0.0, "y": 0.0 },
                "agentConfig": {
                    "agentConfigId": "test-agent"
                }
            })
        })
        .collect();

    json!({
        "recipe": {
            "id": "recipe-1",
            "name": "test-recipe",
            "trigger": "issue",
            "context": "issue",
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

// === cascade_failure tests ===

/// DAG: A → B → C (linear chain)
/// Fail A → B and C should be cascade-failed.
#[test]
fn test_cascade_failure_linear_chain() {
    let mut conn = common::test_conn();
    let project_id = common::create_test_project(&mut conn, "Test", "TCFL");
    let emitter = CapturingEmitter::new();

    let snapshot = build_snapshot(vec![("1", "node-a", "node-b"), ("2", "node-b", "node-c")]);
    insert_execution_with_snapshot(&mut conn, "exec-cascade-1", None, &snapshot);

    // A is running, B and C are pending
    insert_job(
        &mut conn,
        "job-a",
        "running",
        &project_id,
        "exec-cascade-1",
        "node-a",
    );
    insert_job(
        &mut conn,
        "job-b",
        "pending",
        &project_id,
        "exec-cascade-1",
        "node-b",
    );
    insert_job(
        &mut conn,
        "job-c",
        "pending",
        &project_id,
        "exec-cascade-1",
        "node-c",
    );

    // Fail job A — should cascade to B and C
    cairn_core::transitions::transition_job(
        &mut conn,
        &emitter,
        "job-a",
        JobStatus::Failed,
        &cairn_core::transitions::job::test_trigger_tx(),
    )
    .unwrap();

    let b_status: String = jobs::table
        .find("job-b")
        .select(jobs::status)
        .first(&mut conn)
        .unwrap();
    assert_eq!(b_status, "failed", "downstream B should be cascade-failed");

    let c_status: String = jobs::table
        .find("job-c")
        .select(jobs::status)
        .first(&mut conn)
        .unwrap();
    assert_eq!(
        c_status, "failed",
        "transitive downstream C should be cascade-failed"
    );
}

/// DAG: A → B, A → C (fan-out)
/// Fail A → both B and C should be cascade-failed.
#[test]
fn test_cascade_failure_fan_out() {
    let mut conn = common::test_conn();
    let project_id = common::create_test_project(&mut conn, "Test", "TCFO");
    let emitter = CapturingEmitter::new();

    let snapshot = build_snapshot(vec![("1", "node-a", "node-b"), ("2", "node-a", "node-c")]);
    insert_execution_with_snapshot(&mut conn, "exec-cascade-2", None, &snapshot);

    insert_job(
        &mut conn,
        "job-a2",
        "running",
        &project_id,
        "exec-cascade-2",
        "node-a",
    );
    insert_job(
        &mut conn,
        "job-b2",
        "pending",
        &project_id,
        "exec-cascade-2",
        "node-b",
    );
    insert_job(
        &mut conn,
        "job-c2",
        "ready",
        &project_id,
        "exec-cascade-2",
        "node-c",
    );

    cairn_core::transitions::transition_job(
        &mut conn,
        &emitter,
        "job-a2",
        JobStatus::Failed,
        &cairn_core::transitions::job::test_trigger_tx(),
    )
    .unwrap();

    let b_status: String = jobs::table
        .find("job-b2")
        .select(jobs::status)
        .first(&mut conn)
        .unwrap();
    assert_eq!(b_status, "failed");

    let c_status: String = jobs::table
        .find("job-c2")
        .select(jobs::status)
        .first(&mut conn)
        .unwrap();
    assert_eq!(
        c_status, "failed",
        "ready jobs should also be cascade-failed"
    );
}

/// DAG: A → C, B → C (fan-in)
/// Fail A → C should be cascade-failed (even though B could still succeed).
/// This matches the implementation: downstream of the failed node is failed.
#[test]
fn test_cascade_failure_fan_in() {
    let mut conn = common::test_conn();
    let project_id = common::create_test_project(&mut conn, "Test", "TCFI");
    let emitter = CapturingEmitter::new();

    let snapshot = build_snapshot(vec![("1", "node-a", "node-c"), ("2", "node-b", "node-c")]);
    insert_execution_with_snapshot(&mut conn, "exec-cascade-3", None, &snapshot);

    insert_job(
        &mut conn,
        "job-a3",
        "running",
        &project_id,
        "exec-cascade-3",
        "node-a",
    );
    insert_job(
        &mut conn,
        "job-b3",
        "running",
        &project_id,
        "exec-cascade-3",
        "node-b",
    );
    insert_job(
        &mut conn,
        "job-c3",
        "pending",
        &project_id,
        "exec-cascade-3",
        "node-c",
    );

    // Fail A — C is downstream of A, so it gets cascade-failed
    cairn_core::transitions::transition_job(
        &mut conn,
        &emitter,
        "job-a3",
        JobStatus::Failed,
        &cairn_core::transitions::job::test_trigger_tx(),
    )
    .unwrap();

    let c_status: String = jobs::table
        .find("job-c3")
        .select(jobs::status)
        .first(&mut conn)
        .unwrap();
    assert_eq!(c_status, "failed", "C is downstream of failed A");

    // B should be unaffected (not downstream of A)
    let b_status: String = jobs::table
        .find("job-b3")
        .select(jobs::status)
        .first(&mut conn)
        .unwrap();
    assert_eq!(b_status, "running", "B is not downstream of A");
}

/// Cascade should only affect pending/ready jobs, not running ones.
#[test]
fn test_cascade_failure_skips_running_downstream() {
    let mut conn = common::test_conn();
    let project_id = common::create_test_project(&mut conn, "Test", "TCSR");
    let emitter = CapturingEmitter::new();

    let snapshot = build_snapshot(vec![("1", "node-a", "node-b")]);
    insert_execution_with_snapshot(&mut conn, "exec-cascade-4", None, &snapshot);

    insert_job(
        &mut conn,
        "job-a4",
        "running",
        &project_id,
        "exec-cascade-4",
        "node-a",
    );
    insert_job(
        &mut conn,
        "job-b4",
        "running",
        &project_id,
        "exec-cascade-4",
        "node-b",
    );

    cairn_core::transitions::transition_job(
        &mut conn,
        &emitter,
        "job-a4",
        JobStatus::Failed,
        &cairn_core::transitions::job::test_trigger_tx(),
    )
    .unwrap();

    // B is already running, cascade should not touch it
    let b_status: String = jobs::table
        .find("job-b4")
        .select(jobs::status)
        .first(&mut conn)
        .unwrap();
    assert_eq!(
        b_status, "running",
        "running downstream should not be cascade-failed"
    );
}

/// Standalone job (no execution_id) — cascade is a no-op.
#[test]
fn test_cascade_failure_standalone_job_no_execution() {
    let mut conn = common::test_conn();
    let project_id = common::create_test_project(&mut conn, "Test", "TCSJ");
    let emitter = CapturingEmitter::new();

    let now = chrono::Utc::now().timestamp() as i32;
    diesel::sql_query(
        "INSERT INTO jobs (id, status, project_id, created_at, updated_at) VALUES (?, 'running', ?, ?, ?)",
    )
    .bind::<diesel::sql_types::Text, _>("job-standalone")
    .bind::<diesel::sql_types::Text, _>(&project_id)
    .bind::<diesel::sql_types::Integer, _>(now)
    .bind::<diesel::sql_types::Integer, _>(now)
    .execute(&mut conn)
    .unwrap();

    // Should succeed without errors (no cascade since no execution)
    cairn_core::transitions::transition_job(
        &mut conn,
        &emitter,
        "job-standalone",
        JobStatus::Failed,
        &cairn_core::transitions::job::test_trigger_tx(),
    )
    .unwrap();
}

/// Full cascade: failing a job cascades to downstream, recomputes execution to failed,
/// and recomputes issue to failed.
#[test]
fn test_cascade_failure_recomputes_execution_and_issue() {
    let mut conn = common::test_conn();
    let project_id = common::create_test_project(&mut conn, "Test", "TCRE");
    let emitter = CapturingEmitter::new();

    let now = chrono::Utc::now().timestamp() as i32;

    // Insert issue
    diesel::sql_query(
        "INSERT INTO issues (id, project_id, number, title, status, priority, created_at, updated_at) \
         VALUES ('issue-cascade', ?, 1, 'Test', 'active', 0, ?, ?)",
    )
    .bind::<diesel::sql_types::Text, _>(&project_id)
    .bind::<diesel::sql_types::Integer, _>(now)
    .bind::<diesel::sql_types::Integer, _>(now)
    .execute(&mut conn)
    .unwrap();

    let snapshot = build_snapshot(vec![("1", "node-a", "node-b")]);
    insert_execution_with_snapshot(
        &mut conn,
        "exec-cascade-5",
        Some("issue-cascade"),
        &snapshot,
    );

    insert_job(
        &mut conn,
        "job-a5",
        "running",
        &project_id,
        "exec-cascade-5",
        "node-a",
    );
    insert_job(
        &mut conn,
        "job-b5",
        "pending",
        &project_id,
        "exec-cascade-5",
        "node-b",
    );

    // Fail A → B cascaded, execution becomes failed, issue becomes failed
    cairn_core::transitions::transition_job(
        &mut conn,
        &emitter,
        "job-a5",
        JobStatus::Failed,
        &cairn_core::transitions::job::test_trigger_tx(),
    )
    .unwrap();

    let exec_status: String = executions::table
        .find("exec-cascade-5")
        .select(executions::status)
        .first(&mut conn)
        .unwrap();
    assert_eq!(
        exec_status, "failed",
        "execution should be failed after all jobs failed"
    );

    let issue_status: String = cairn_core::internal::schema::issues::table
        .find("issue-cascade")
        .select(cairn_core::internal::schema::issues::status)
        .first(&mut conn)
        .unwrap();
    assert_eq!(
        issue_status, "failed",
        "issue should be failed after execution failed"
    );
}

/// Cascade failure sets completed_at on downstream jobs (via transition_job).
#[test]
fn test_cascade_failure_sets_completed_at() {
    let mut conn = common::test_conn();
    let project_id = common::create_test_project(&mut conn, "Test", "TCCA");
    let emitter = CapturingEmitter::new();

    let snapshot = build_snapshot(vec![("1", "node-a", "node-b")]);
    insert_execution_with_snapshot(&mut conn, "exec-cascade-ca", None, &snapshot);

    insert_job(
        &mut conn,
        "job-ca-a",
        "running",
        &project_id,
        "exec-cascade-ca",
        "node-a",
    );
    insert_job(
        &mut conn,
        "job-ca-b",
        "pending",
        &project_id,
        "exec-cascade-ca",
        "node-b",
    );

    cairn_core::transitions::transition_job(
        &mut conn,
        &emitter,
        "job-ca-a",
        JobStatus::Failed,
        &cairn_core::transitions::job::test_trigger_tx(),
    )
    .unwrap();

    // Cascaded job should have completed_at set (via transition_job, not bulk update)
    let completed_at: Option<i32> = jobs::table
        .find("job-ca-b")
        .select(jobs::completed_at)
        .first(&mut conn)
        .unwrap();
    assert!(
        completed_at.is_some(),
        "cascade-failed job should have completed_at set"
    );
}

/// Cascade emits db-change events for each downstream job (via transition_job).
#[test]
fn test_cascade_failure_emits_db_change_events() {
    let mut conn = common::test_conn();
    let project_id = common::create_test_project(&mut conn, "Test", "TCDE");
    let emitter = CapturingEmitter::new();

    let snapshot = build_snapshot(vec![("1", "node-a", "node-b"), ("2", "node-b", "node-c")]);
    insert_execution_with_snapshot(&mut conn, "exec-cascade-ev", None, &snapshot);

    insert_job(
        &mut conn,
        "job-ev-a",
        "running",
        &project_id,
        "exec-cascade-ev",
        "node-a",
    );
    insert_job(
        &mut conn,
        "job-ev-b",
        "pending",
        &project_id,
        "exec-cascade-ev",
        "node-b",
    );
    insert_job(
        &mut conn,
        "job-ev-c",
        "pending",
        &project_id,
        "exec-cascade-ev",
        "node-c",
    );

    cairn_core::transitions::transition_job(
        &mut conn,
        &emitter,
        "job-ev-a",
        JobStatus::Failed,
        &cairn_core::transitions::job::test_trigger_tx(),
    )
    .unwrap();

    // Should have multiple db-change events (one per transition: A, B via cascade, C via cascade)
    let db_changes = emitter.events_named("db-change");
    let job_changes: Vec<_> = db_changes
        .iter()
        .filter(|p| p.get("table").and_then(|v| v.as_str()) == Some("jobs"))
        .collect();
    // A's transition + B's cascade transition + C's cascade transition = 3
    assert!(
        job_changes.len() >= 3,
        "expected at least 3 jobs db-change events, got {}",
        job_changes.len()
    );
}

/// Diamond pattern: A → B, A → C, B → D, C → D.
/// Failing A should cascade to B, C, and D without errors.
#[test]
fn test_cascade_failure_diamond_pattern() {
    let mut conn = common::test_conn();
    let project_id = common::create_test_project(&mut conn, "Test", "TCDP");
    let emitter = CapturingEmitter::new();

    let snapshot = build_snapshot(vec![
        ("1", "node-a", "node-b"),
        ("2", "node-a", "node-c"),
        ("3", "node-b", "node-d"),
        ("4", "node-c", "node-d"),
    ]);
    insert_execution_with_snapshot(&mut conn, "exec-diamond", None, &snapshot);

    insert_job(
        &mut conn,
        "job-dia-a",
        "running",
        &project_id,
        "exec-diamond",
        "node-a",
    );
    insert_job(
        &mut conn,
        "job-dia-b",
        "pending",
        &project_id,
        "exec-diamond",
        "node-b",
    );
    insert_job(
        &mut conn,
        "job-dia-c",
        "pending",
        &project_id,
        "exec-diamond",
        "node-c",
    );
    insert_job(
        &mut conn,
        "job-dia-d",
        "pending",
        &project_id,
        "exec-diamond",
        "node-d",
    );

    // Fail A — all downstream should be cascade-failed without errors
    cairn_core::transitions::transition_job(
        &mut conn,
        &emitter,
        "job-dia-a",
        JobStatus::Failed,
        &cairn_core::transitions::job::test_trigger_tx(),
    )
    .unwrap();

    for job_id in &["job-dia-b", "job-dia-c", "job-dia-d"] {
        let status: String = jobs::table
            .find(*job_id)
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(
            status, "failed",
            "job {} should be cascade-failed in diamond pattern",
            job_id
        );
    }
}
