//! Integration tests for execution snapshot job materialization.

use crate::common;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cairn_core::internal::execution::creation::create_jobs_for_execution;
use cairn_core::internal::storage::LocalDb;
use cairn_core::models::{
    AgentGitConfig, AgentNodeConfig, BranchMode, DelegatedOutputContract, DelegatedOwnershipScope,
    DelegatedSessionStrategy, DelegatedStatus, DelegatedWorkPacket, DelegationOrigin,
    ExecutionSnapshot, Job, JobStatus, NodePosition, OutputSchema, RecipeEdge, RecipeEdgeType,
    RecipeNode, RecipeNodeType, RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
};
use cairn_db::turso::params;

fn node(id: &str, node_type: RecipeNodeType, agent_config_id: Option<&str>) -> RecipeNode {
    RecipeNode {
        id: id.to_string(),
        node_type,
        name: id.to_string(),
        position: NodePosition { x: 0.0, y: 0.0 },
        parent_id: None,
        trigger_config: None,
        agent_config: agent_config_id.map(|id| AgentNodeConfig {
            agent_config_id: Some(id.to_string()),
            output_schema: None,
            git_config: None,
        }),
        action_config: None,
        checkpoint_config: None,
        artifact_config: None,
        condition_config: None,
        context_config: None,
    }
}

fn inherited_agent(id: &str, agent_config_id: &str) -> RecipeNode {
    RecipeNode {
        agent_config: Some(AgentNodeConfig {
            agent_config_id: Some(agent_config_id.to_string()),
            output_schema: None,
            git_config: Some(AgentGitConfig {
                require_parent_head: false,
                branch_mode: BranchMode::Inherit,
            }),
        }),
        ..node(id, RecipeNodeType::Agent, None)
    }
}

fn control_edge(id: &str, source: &str, target: &str) -> RecipeEdge {
    RecipeEdge {
        id: id.to_string(),
        edge_type: RecipeEdgeType::Control,
        source_node_id: source.to_string(),
        source_handle: "output".to_string(),
        target_node_id: target.to_string(),
        target_handle: "input".to_string(),
    }
}

fn snapshot(project_id: &str, nodes: Vec<RecipeNode>, edges: Vec<RecipeEdge>) -> ExecutionSnapshot {
    ExecutionSnapshot::new(
        RecipeSnapshot {
            id: "recipe-1".to_string(),
            name: "Test Recipe".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes,
            edges,
        },
        HashMap::new(),
        HashMap::new(),
        TriggerContext {
            issue_id: None,
            project_id: project_id.to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    )
}

async fn insert_execution(db: &LocalDb, execution_id: &str, snapshot: &ExecutionSnapshot) {
    let project_id = snapshot.trigger_context.project_id.clone();
    let snapshot_json = snapshot.to_json().unwrap();
    let execution_id = execution_id.to_string();
    let now = chrono::Utc::now().timestamp();

    db.write(|conn| {
        let execution_id = execution_id.clone();
        let project_id = project_id.clone();
        let snapshot_json = snapshot_json.clone();
        Box::pin(async move {
            conn.execute(
                "
                INSERT INTO executions (
                    id, recipe_id, project_id, status, started_at, seq, snapshot, triggered_by
                )
                VALUES (?1, 'recipe-1', ?2, 'running', ?3, 1, ?4, 'manual')
                ",
                params![
                    execution_id.as_str(),
                    project_id.as_str(),
                    now,
                    snapshot_json.as_str()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

fn node_ids(jobs: &[Job]) -> HashSet<String> {
    jobs.iter()
        .filter_map(|job| job.recipe_node_id.clone())
        .collect()
}

/// The packet a thread's session writes when it delegates a task, already
/// materialized onto `agent_node_id` — the state the DAG expander leaves behind
/// just before job creation runs.
fn delegated_packet(parent_job_id: &str, agent_node_id: &str) -> DelegatedWorkPacket {
    DelegatedWorkPacket {
        id: "packet-1".to_string(),
        parent_job_id: parent_job_id.to_string(),
        parent_turn_id: None,
        parent_tool_use_id: Some("tool-1".to_string()),
        origin: DelegationOrigin::TaskTool,
        title: "Survey".to_string(),
        problem_statement: "Look into it".to_string(),
        agent_config_id: "Explore".to_string(),
        ownership: DelegatedOwnershipScope {
            cwd: "/scratch".to_string(),
            fence: None,
            sandbox: None,
            on_escape: None,
        },
        session: DelegatedSessionStrategy::default(),
        acceptance: vec![],
        output_contract: DelegatedOutputContract {
            schema_type: OutputSchema::Preset("return".to_string()),
            tool_name: None,
            description: None,
        },
        status: DelegatedStatus::Materialized,
        materialized_node_ids: vec![agent_node_id.to_string()],
        result_artifact_job_id: None,
        task_index: Some(0),
        tier_override: None,
        backend_preference: None,
        background: false,
        created_at: 1,
    }
}

/// A task delegated by a thread's session belongs to that thread, and the proof
/// is the query the thread pane actually consumes.
///
/// `list_jobs_for_thread` selects on `jobs.thread_id` alone, so a task created
/// without it is not merely mislabeled — it cannot appear in the thread's task
/// rollup, cannot be opened as a tab, and contributes nothing to the thread's
/// status or artifacts. Nothing stamped that column for a delegated child
/// (CAIRN-3861), and asserting on the INSERT alone would not have caught it:
/// the delegating parent is named by the packet, not by the execution graph the
/// insert derives its `parent_job_id` from.
#[tokio::test]
async fn a_task_delegated_by_a_thread_session_is_listed_under_that_thread() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "jct").await;
    let execution_id = "execution-thread";
    let agent_node_id = "delegated-packet-1-agent";

    // The thread and its branchless session job, exactly as `ensure_thread_session`
    // mints them: no execution, no issue, the reserved `thread` segment.
    db.execute(
        "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
         VALUES ('th', ?1, 'general', 'active', 'none', 1, 1)",
        params![project_id.as_str()],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO jobs (id, thread_id, project_id, status, uri_segment, node_name,
                           created_at, updated_at)
         VALUES ('j-session', 'th', ?1, 'idle', 'thread', 'Thread', 1, 1)",
        params![project_id.as_str()],
    )
    .await
    .unwrap();

    // Delegation books its packets in a synthetic, issue-less execution whose
    // graph holds only the materialized task — the session owns no node in it.
    let mut snapshot = snapshot(
        &project_id,
        vec![
            node("delegated-packet-1-trigger", RecipeNodeType::Trigger, None),
            node(agent_node_id, RecipeNodeType::Agent, Some("Explore")),
        ],
        vec![control_edge(
            "edge-1",
            "delegated-packet-1-trigger",
            agent_node_id,
        )],
    );
    snapshot.delegated_packets = vec![delegated_packet("j-session", agent_node_id)];
    insert_execution(&db, execution_id, &snapshot).await;

    let jobs = create_jobs_for_execution(db.clone(), execution_id).unwrap();
    let task = jobs
        .iter()
        .find(|job| job.recipe_node_id.as_deref() == Some(agent_node_id))
        .expect("the delegated agent node materializes a job");

    let listed = cairn_core::internal::jobs::queries::list_jobs_for_thread(&db, "th")
        .await
        .unwrap();
    assert!(
        listed.iter().any(|job| job.id == task.id),
        "the thread's own pane cannot see the task it spawned: {:?}",
        listed.iter().map(|job| job.id.as_str()).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn creates_jobs_for_reachable_agent_nodes_only() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "jcr").await;
    let execution_id = "execution-1";
    let snapshot = snapshot(
        &project_id,
        vec![
            node("trigger", RecipeNodeType::Trigger, None),
            node("planner", RecipeNodeType::Agent, Some("planner-agent")),
            node("action", RecipeNodeType::Action, None),
            node("artifact", RecipeNodeType::Artifact, None),
            node("builder", RecipeNodeType::Agent, Some("builder-agent")),
            node("isolated", RecipeNodeType::Agent, Some("isolated-agent")),
        ],
        vec![
            control_edge("edge-1", "trigger", "planner"),
            control_edge("edge-2", "planner", "action"),
            control_edge("edge-3", "action", "artifact"),
            control_edge("edge-4", "artifact", "builder"),
        ],
    );
    insert_execution(&db, execution_id, &snapshot).await;

    let jobs = create_jobs_for_execution(db.clone(), execution_id).unwrap();

    assert_eq!(
        node_ids(&jobs),
        HashSet::from(["planner".into(), "builder".into()])
    );
    assert!(jobs.iter().all(|job| job.status == JobStatus::Pending));
    assert!(jobs
        .iter()
        .all(|job| job.execution_id.as_deref() == Some(execution_id)));
}

#[tokio::test]
async fn preserves_agent_config_ids() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "jcp").await;
    let execution_id = "execution-2";
    let snapshot = snapshot(
        &project_id,
        vec![
            node("trigger", RecipeNodeType::Trigger, None),
            node("builder", RecipeNodeType::Agent, Some("custom-agent")),
        ],
        vec![control_edge("edge-1", "trigger", "builder")],
    );
    insert_execution(&db, execution_id, &snapshot).await;

    let jobs = create_jobs_for_execution(db.clone(), execution_id).unwrap();

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].recipe_node_id.as_deref(), Some("builder"));
    assert_eq!(jobs[0].agent_config_id.as_deref(), Some("custom-agent"));
}

#[tokio::test]
async fn inherited_agent_uses_upstream_agent_job_as_parent() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "jci").await;
    let execution_id = "execution-3";
    let snapshot = snapshot(
        &project_id,
        vec![
            node("trigger", RecipeNodeType::Trigger, None),
            node("planner", RecipeNodeType::Agent, Some("planner-agent")),
            inherited_agent("builder", "builder-agent"),
        ],
        vec![
            control_edge("edge-1", "trigger", "planner"),
            control_edge("edge-2", "planner", "builder"),
        ],
    );
    insert_execution(&db, execution_id, &snapshot).await;

    let jobs = create_jobs_for_execution(db.clone(), execution_id).unwrap();
    let planner = jobs
        .iter()
        .find(|job| job.recipe_node_id.as_deref() == Some("planner"))
        .unwrap();
    let builder = jobs
        .iter()
        .find(|job| job.recipe_node_id.as_deref() == Some("builder"))
        .unwrap();

    assert_eq!(builder.parent_job_id.as_deref(), Some(planner.id.as_str()));
}

#[tokio::test]
async fn creates_no_jobs_without_trigger_reachability() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "jcn").await;
    let execution_id = "execution-4";
    let snapshot = snapshot(
        &project_id,
        vec![
            node("planner", RecipeNodeType::Agent, Some("planner-agent")),
            node("builder", RecipeNodeType::Agent, Some("builder-agent")),
        ],
        vec![control_edge("edge-1", "planner", "builder")],
    );
    insert_execution(&db, execution_id, &snapshot).await;

    let jobs = create_jobs_for_execution(db.clone(), execution_id).unwrap();

    assert!(jobs.is_empty());
}

#[tokio::test]
async fn creates_one_job_per_reachable_agent_in_complex_branching_graph() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "jcb").await;
    let execution_id = "execution-5";
    let snapshot = snapshot(
        &project_id,
        vec![
            node("trigger", RecipeNodeType::Trigger, None),
            node("lint", RecipeNodeType::Agent, Some("lint-agent")),
            node("test", RecipeNodeType::Agent, Some("test-agent")),
            node("build", RecipeNodeType::Agent, Some("build-agent")),
            node(
                "integration",
                RecipeNodeType::Agent,
                Some("integration-agent"),
            ),
            node("deploy", RecipeNodeType::Agent, Some("deploy-agent")),
        ],
        vec![
            control_edge("edge-1", "trigger", "lint"),
            control_edge("edge-2", "trigger", "test"),
            control_edge("edge-3", "lint", "build"),
            control_edge("edge-4", "test", "build"),
            control_edge("edge-5", "build", "integration"),
            control_edge("edge-6", "integration", "deploy"),
        ],
    );
    insert_execution(&db, execution_id, &snapshot).await;

    let jobs = create_jobs_for_execution(db.clone(), execution_id).unwrap();

    assert_eq!(
        node_ids(&jobs),
        HashSet::from([
            "lint".into(),
            "test".into(),
            "build".into(),
            "integration".into(),
            "deploy".into()
        ])
    );
    assert_eq!(jobs.len(), 5);
    assert!(jobs
        .iter()
        .all(|job| job.execution_id.as_deref() == Some(execution_id)));
}
