//! Who may confirm a gated artifact, through the real `write` dispatcher.
//!
//! Issue `cfa/1` runs the bundled planbuild recipe: its planner holds an
//! unconfirmed `plan`, the planner has a delegated task, and the builder is a
//! sibling node on the same issue. Around it sit thread `design` (the child's
//! filing parent in most cases), an unrelated thread `other`, and, for the
//! coordinator case, issue `cfa/2` whose coordinator node filed `cfa/1`.
//! Only the gate owner, through a parent edge carrying filing proof, or the
//! operator may confirm; every refusal is journaled and leaves the gate
//! pending.

use crate::common;

use std::collections::HashMap;
use std::sync::Arc;

use cairn_core::internal::execution::checkpoints::{
    confirm_gated_artifact, ConfirmError, Confirmer,
};
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::storage::{LocalDb, RowExt};
use cairn_core::models::{
    ExecutionSnapshot, RecipeFile, RecipeNodeType, RecipeSnapshot, TriggerContext, TriggerType,
};
use serde_json::json;

const PLAN_URI: &str = "cairn://p/cfa/1/1/planner/plan";

/// The bundled planbuild recipe as `exec-1`'s snapshot, plus the imported
/// node id of each agent keyed by agent config.
fn planbuild_snapshot() -> (String, HashMap<String, String>) {
    let file = RecipeFile::from_yaml(include_str!(
        "../../../../packs/core/recipes/planbuild.yaml"
    ))
    .expect("planbuild parses");
    let recipe = file.into_recipe(None, Some("p-cfa".to_string()));
    let agents = recipe
        .nodes
        .iter()
        .filter(|n| n.node_type == RecipeNodeType::Agent)
        .filter_map(|n| {
            let agent = n.agent_config.as_ref()?.agent_config_id.clone()?;
            Some((agent, n.id.clone()))
        })
        .collect::<HashMap<_, _>>();
    let snapshot = ExecutionSnapshot::new(
        RecipeSnapshot {
            id: recipe.id.clone(),
            name: recipe.name.clone(),
            description: recipe.description.clone(),
            trigger: recipe.trigger.clone(),
            nodes: recipe.nodes.clone(),
            edges: recipe.edges.clone(),
        },
        HashMap::new(),
        HashMap::new(),
        TriggerContext {
            issue_id: Some("i-1".to_string()),
            project_id: "p-cfa".to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    );
    (snapshot.to_json().unwrap(), agents)
}

/// How `cfa/1` was filed: its parent edge and creating job, written in the
/// issue's own INSERT as a real filing writes them.
#[derive(Clone, Copy)]
struct Filing {
    parent_issue: Option<&'static str>,
    parent_thread: Option<&'static str>,
    creator_job: Option<&'static str>,
}

/// Filed by thread `design`, with the thread's session job as proof.
const THREAD_FILED: Filing = Filing {
    parent_issue: None,
    parent_thread: Some("thread-design"),
    creator_job: Some("job-design"),
};

/// Filed by `cfa/2`'s coordinator node.
const COORDINATOR_FILED: Filing = Filing {
    parent_issue: Some("i-2"),
    parent_thread: None,
    creator_job: Some("job-coord"),
};

/// Routed under thread `design` with no creator proof: a legacy child, or one
/// adopted rather than filed.
const THREAD_ROUTED_ONLY: Filing = Filing {
    parent_issue: None,
    parent_thread: Some("thread-design"),
    creator_job: None,
};

fn sql_text(value: Option<&str>) -> String {
    value.map_or_else(|| "NULL".to_string(), |value| format!("'{value}'"))
}

/// A thread with a live session job and run.
fn thread_sql(name: &str) -> String {
    format!(
        "INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
           VALUES ('thread-{name}', 'p-cfa', '{name}', 'active', 'idle', 1, 1);
         INSERT INTO executions(id, recipe_id, project_id, status, started_at)
           VALUES ('host-{name}', 'host-job-{name}', 'p-cfa', 'running', 1);
         INSERT INTO jobs(id, execution_id, thread_id, project_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
           VALUES ('job-{name}', 'host-{name}', 'thread-{name}', 'p-cfa', 'Thread', 'running', 's-{name}', 'thread', 1, 1);
         INSERT INTO sessions(id, job_id, status, created_at, updated_at)
           VALUES ('s-{name}', 'job-{name}', 'active', 1, 1);
         INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at, start_mode)
           VALUES ('run-{name}', 'p-cfa', 'job-{name}', 'live', 's-{name}', 1, 1, 'resume');"
    )
}

/// An issue-hosted job with a live session and run on `issue`/`exec`.
fn node_sql(
    job: &str,
    exec: &str,
    issue: &str,
    node: &str,
    segment: &str,
    parent: Option<&str>,
) -> String {
    format!(
        "INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, parent_job_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
           VALUES ('{job}', '{exec}', '{node}', '{issue}', 'p-cfa', {parent}, '{segment}', 'running', 's-{job}', '{segment}', 1, 1);
         INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
           VALUES ('s-{job}', '{job}', 'codex', 'open', 1, 1, 1);
         INSERT INTO runs(id, issue_id, project_id, job_id, status, session_id, created_at, updated_at, start_mode)
           VALUES ('run-{job}', '{issue}', 'p-cfa', '{job}', 'live', 's-{job}', 1, 1, 'resume');",
        parent = sql_text(parent),
    )
}

async fn seed(db: &LocalDb, filing: Filing) {
    let (snapshot, agents) = planbuild_snapshot();
    let mut sql = String::from(
        "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('p-cfa', 'default', 'Confirm', 'cfa', '/tmp/cfa', 1, 1);",
    );
    sql.push_str(&thread_sql("design"));
    sql.push_str(&thread_sql("other"));
    // `cfa/2`, driven by its coordinator node, exists before `cfa/1` so it can
    // be named as `cfa/1`'s filing parent.
    sql.push_str(
        "INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
           VALUES ('i-2', 'p-cfa', 2, 'Mission', 'active', 'none', 1, 1);
         INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
           VALUES ('exec-2', 'coordinator', 'i-2', 'p-cfa', 'running', 1, 1);",
    );
    sql.push_str(&node_sql(
        "job-coord",
        "exec-2",
        "i-2",
        "coordinator",
        "coordinator",
        None,
    ));
    sql.push_str(&format!(
        "INSERT INTO issues(id, project_id, number, title, status, attention, parent_issue_id, parent_thread_id, parent_job_id, created_at, updated_at)
           VALUES ('i-1', 'p-cfa', 1, 'Child', 'active', 'none', {}, {}, {}, 1, 1);",
        sql_text(filing.parent_issue),
        sql_text(filing.parent_thread),
        sql_text(filing.creator_job),
    ));
    sql.push_str(&format!(
        "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
           VALUES ('exec-1', 'planbuild', 'i-1', 'p-cfa', 'running', 1, 1, '{}');",
        snapshot.replace('\'', "''"),
    ));
    sql.push_str(&node_sql(
        "job-planner",
        "exec-1",
        "i-1",
        &agents["planner"],
        "planner",
        None,
    ));
    sql.push_str(&node_sql(
        "job-builder",
        "exec-1",
        "i-1",
        &agents["build"],
        "builder",
        None,
    ));
    sql.push_str(&node_sql(
        "job-task",
        "exec-1",
        "i-1",
        "task",
        "probe",
        Some("job-planner"),
    ));
    sql.push_str(
        "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at)
           VALUES ('t-planner', 's-job-planner', 'job-planner', 1, 'complete', 'initial', 1, 2, 2);
         INSERT INTO artifacts(id, job_id, artifact_type, data, version, output_name, created_at, updated_at, confirmed)
           VALUES ('artifact-plan', 'job-planner', 'plan', '{\"title\":\"Plan\",\"summary\":\"s\",\"content\":\"c\"}', 1, 'plan', 3, 3, 0);",
    );
    db.execute_script(&sql).await.unwrap();
}

async fn fixture(filing: Filing) -> (tempfile::TempDir, Arc<LocalDb>, Orchestrator) {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db, filing).await;
    let orch = common::orchestrator(&temp, db.clone());
    (temp, db, orch)
}

async fn confirm_as(orch: &Orchestrator, run_id: Option<&str>) -> String {
    common::change_resource_as_run(
        orch,
        json!([{"target": PLAN_URI, "mode": "patch", "payload": {"confirmed": true}}]),
        run_id,
    )
    .await
}

async fn plan_confirmed(db: &LocalDb) -> bool {
    db.query_one(
        "SELECT confirmed FROM artifacts WHERE id = 'artifact-plan'",
        (),
        |row| row.i64(0),
    )
    .await
    .unwrap()
        != 0
}

async fn refusals(db: &LocalDb) -> i64 {
    db.query_one(
        "SELECT COUNT(*) FROM authorization_events
          WHERE reason = 'artifact_confirmation_authority' AND outcome = 'forbidden'",
        (),
        |row| row.i64(0),
    )
    .await
    .unwrap()
}

/// `run_id`'s confirmation is refused naming `may_confirm`, journaled, and
/// leaves the plan unconfirmed.
async fn assert_refused(
    orch: &Orchestrator,
    db: &LocalDb,
    run_id: Option<&str>,
    may_confirm: &str,
) {
    let before = refusals(db).await;
    let result = confirm_as(orch, run_id).await;
    assert!(
        result.contains(&format!(
            "Only {may_confirm} may confirm this gated artifact"
        )),
        "{run_id:?}: {result}"
    );
    assert!(result.contains("The gate is still pending"), "{result}");
    assert!(!result.contains("gate resolved"), "{run_id:?}: {result}");
    assert!(!plan_confirmed(db).await, "{run_id:?} confirmed the plan");
    assert_eq!(
        refusals(db).await,
        before + 1,
        "{run_id:?}'s refusal is journaled"
    );
}

async fn assert_confirms(orch: &Orchestrator, db: &LocalDb, run_id: &str) {
    let result = confirm_as(orch, Some(run_id)).await;
    assert!(result.contains("gate resolved"), "{run_id}: {result}");
    assert!(plan_confirmed(db).await, "{run_id}");
}

const DESIGN_OR_OPERATOR: &str = "cairn://p/cfa/design (this issue's gate owner) or the operator";

#[tokio::test]
async fn the_producer_and_its_own_tasks_cannot_confirm_its_plan() {
    let (_temp, db, orch) = fixture(THREAD_FILED).await;
    assert_refused(&orch, &db, Some("run-job-planner"), DESIGN_OR_OPERATOR).await;
    assert_refused(&orch, &db, Some("run-job-task"), DESIGN_OR_OPERATOR).await;
    let journal = db
        .query_one(
            "SELECT summary FROM authorization_events
              WHERE reason = 'artifact_confirmation_authority' ORDER BY rowid LIMIT 1",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();
    assert!(journal.contains("cairn://p/cfa/1/1/planner"), "{journal}");
    assert!(
        journal.contains("nobody confirms their own gate"),
        "{journal}"
    );
    // Still pending and answerable: the real gate owner confirms it after.
    assert_confirms(&orch, &db, "run-design").await;
}

#[tokio::test]
async fn an_unrelated_thread_a_sibling_node_and_an_unrun_caller_cannot_confirm() {
    let (_temp, db, orch) = fixture(THREAD_FILED).await;
    assert_refused(&orch, &db, Some("run-other"), DESIGN_OR_OPERATOR).await;
    assert_refused(&orch, &db, Some("run-job-builder"), DESIGN_OR_OPERATOR).await;
    // A coordinator elsewhere in the project is no closer.
    assert_refused(&orch, &db, Some("run-job-coord"), DESIGN_OR_OPERATOR).await;
    assert_refused(&orch, &db, None, DESIGN_OR_OPERATOR).await;
    // Forging a confirmer in the payload changes nothing.
    let before = refusals(&db).await;
    let forged = common::change_resource_as_run(
        &orch,
        json!([{"target": PLAN_URI, "mode": "patch", "payload": {
            "confirmed": true,
            "actor": "cairn://p/cfa/design",
            "run_id": "run-design",
        }}]),
        Some("run-other"),
    )
    .await;
    assert!(!forged.contains("gate resolved"), "{forged}");
    assert!(!plan_confirmed(&db).await);
    assert!(refusals(&db).await > before, "{forged}");
    assert_confirms(&orch, &db, "run-design").await;
}

#[tokio::test]
async fn a_dry_run_reports_the_refusal_without_journaling_it() {
    let (_temp, db, orch) = fixture(THREAD_FILED).await;
    let preview = cairn_core::internal::mcp::handlers::write::handle_write(
        &orch,
        &cairn_core::internal::mcp::types::McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: String::new(),
            run_id: Some("run-job-planner".to_string()),
            tool: "write".to_string(),
            payload: json!({
                "changes": [{"target": PLAN_URI, "mode": "patch", "payload": {"confirmed": true}}],
                "preview": true,
            }),
            tool_use_id: None,
            origin: None,
        },
    )
    .await;
    assert!(
        preview.contains(&format!("Only {DESIGN_OR_OPERATOR} may confirm")),
        "{preview}"
    );
    assert_eq!(refusals(&db).await, 0);
    assert!(!plan_confirmed(&db).await);
}

#[tokio::test]
async fn the_coordinator_node_that_filed_the_child_confirms_its_plan() {
    let (_temp, db, orch) = fixture(COORDINATOR_FILED).await;
    let coordinator_or_operator =
        "cairn://p/cfa/2/1/coordinator (this issue's gate owner) or the operator";
    // A thread that is no parent of this child is refused, and so is the producer.
    assert_refused(&orch, &db, Some("run-design"), coordinator_or_operator).await;
    assert_refused(&orch, &db, Some("run-job-planner"), coordinator_or_operator).await;
    assert_confirms(&orch, &db, "run-job-coord").await;
}

#[tokio::test]
async fn a_routed_but_unproven_thread_edge_leaves_the_gate_to_the_operator() {
    let (_temp, db, orch) = fixture(THREAD_ROUTED_ONLY).await;
    let result = confirm_as(&orch, Some("run-design")).await;
    assert!(result.contains("carries no filing proof"), "{result}");
    assert_refused(&orch, &db, Some("run-design"), "the operator").await;
    operator_confirms(&orch, &db).await;
}

#[tokio::test]
async fn reparenting_a_filed_child_revokes_its_threads_confirm_authority_for_good() {
    let (_temp, db, orch) = fixture(THREAD_FILED).await;
    // Orphan, then re-adopt under the same thread: the filing edge is revoked
    // permanently, though routing still reaches the thread.
    db.execute(
        "UPDATE issues SET parent_thread_id = NULL WHERE id = 'i-1'",
        (),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE issues SET parent_thread_id = 'thread-design' WHERE id = 'i-1'",
        (),
    )
    .await
    .unwrap();
    assert_refused(&orch, &db, Some("run-design"), "the operator").await;
    operator_confirms(&orch, &db).await;
}

#[tokio::test]
async fn with_no_parent_only_the_operator_confirms() {
    let (_temp, db, orch) = fixture(Filing {
        parent_issue: None,
        parent_thread: None,
        creator_job: None,
    })
    .await;
    assert_refused(&orch, &db, Some("run-design"), "the operator").await;
    assert_refused(&orch, &db, Some("run-job-builder"), "the operator").await;
    operator_confirms(&orch, &db).await;
}

/// The operator's confirmation, as the `approve_job` invoke makes it once
/// `has_operator_authority` has proven the caller.
async fn operator_confirms(orch: &Orchestrator, db: &LocalDb) {
    let before = refusals(db).await;
    let operator = Confirmer::Operator(cairn_common::identity::PrincipalRef::Machine {
        device_id: "desktop-device".to_string(),
    });
    confirm_gated_artifact(orch, "job-planner", &operator)
        .await
        .expect("the operator confirms any gate");
    assert!(plan_confirmed(db).await);
    assert_eq!(refusals(db).await, before);
    // A confirmed gate has nothing left to confirm, for anyone.
    let again = confirm_gated_artifact(orch, "job-planner", &operator).await;
    assert!(
        matches!(&again, Err(ConfirmError::Failed(error)) if error.contains("no unconfirmed artifact")),
        "{again:?}"
    );
}
