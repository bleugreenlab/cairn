//! Integration tests for execution snapshot job materialization.

mod common;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cairn_core::internal::execution::creation::create_jobs_for_execution;
use cairn_core::internal::storage::LocalDb;
use cairn_core::models::{
    AgentGitConfig, AgentNodeConfig, ExecutionSnapshot, Job, JobStatus, NodePosition, RecipeEdge,
    RecipeEdgeType, RecipeNode, RecipeNodeType, RecipeSnapshot, RecipeTrigger, TriggerContext,
    TriggerType, WorktreeMode,
};
use turso::params;

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
                worktree_mode: WorktreeMode::Inherit,
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

#[tokio::test]
async fn creates_jobs_for_reachable_agent_nodes_only() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "JCR").await;
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
    let project_id = common::create_project(&db, "JCP").await;
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
    let project_id = common::create_project(&db, "JCI").await;
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
    let project_id = common::create_project(&db, "JCN").await;
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
    let project_id = common::create_project(&db, "JCB").await;
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
