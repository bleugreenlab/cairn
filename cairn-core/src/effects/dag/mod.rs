//! DAG reduction for the effect loop.
//!
//! `reduce_dag` advances the execution DAG and produces typed `WorkflowEffect`s
//! for each operation that requires host resources (process spawning, worktree
//! creation, shell commands, LLM calls).
//!
//! DB reads/writes happen inside `reduce_dag` (claiming pending→running,
//! creating action_run records). Everything that
//! crosses the host boundary is returned as an effect.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::config::agents as config_agents;
use crate::config::derive_unique_task_slug;
use crate::config::presets::{
    load_effective_presets, resolve_agent_snapshot, resolve_runtime_selection,
    LaunchSelectionOverride, PresetsConfig,
};
use crate::db_records::{DbJob, DbRecipeNode};
use crate::execution::advancement::{
    advance_execution_impl, create_jobs_for_new_nodes, find_ready_condition_nodes,
    load_execution_snapshot, load_job, load_nodes_from_execution, load_project_repo_path,
    run_advancement_db, update_execution_snapshot,
};
use crate::execution::cache::{get_current_head_sha, is_worktree_dirty, normalize_command};
use crate::models::{
    AgentGitConfig, AgentNodeConfig, ContextNodeConfig, DelegatedOutputContract, DelegatedStatus,
    DelegatedWorkPacket, ExecutionSnapshot, Job, Model, NodePosition, RecipeEdge, RecipeEdgeType,
    RecipeNode, RecipeNodeType, SchemaConfig, WorktreeMode,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};
use turso::params;
use uuid::Uuid;

use super::types::{ConditionSpec, EffectContext, EffectSource, WorkflowEffect};

mod checkpoints;
mod conditions;
mod delegation;
mod graph;

use checkpoints::*;
use conditions::*;
use delegation::*;
use graph::*;

/// Advance the DAG for an execution and produce effects.
///
/// Returns `(agent_jobs, effects)`:
/// - `agent_jobs`: Agent-type jobs ready to start (host spawns processes)
/// - `effects`: Follow-on effects for non-agent node types
///
/// This is the synchronous replacement for `advance_execution_with_actions`.
/// All DB mutations happen here; host-crossing operations are returned as effects.
pub fn reduce_dag(
    orch: &Orchestrator,
    execution_id: &str,
) -> Result<(Vec<Job>, Vec<WorkflowEffect>), String> {
    let mut effects = Vec::new();

    // Step 0: Re-arm any Blocked command checkpoints whose upstream agent has
    // committed a fix since the last run. This re-pends them so the pending-scan
    // below re-runs the command against the new worktree HEAD. Gated by the
    // SHA-change progress check, upstream-not-running, and the attempt cap, so
    // it is a cheap no-op on every other advance.
    crate::execution::advancement::rearm_blocked_checkpoints(orch, execution_id)?;

    // Step 1: Materialize pending delegated packets, then advance the DAG.
    expand_delegated_packets(orch, execution_id)?;
    let ready_jobs = advance_execution_impl(orch, execution_id)?;

    // Step 2: Categorize jobs by their node type
    let mut agent_jobs = Vec::new();
    let mut node_jobs: Vec<(DbJob, DbRecipeNode)> = Vec::new();

    {
        let mut node_map: HashMap<String, DbRecipeNode> = HashMap::new();
        if let Some(first_job) = ready_jobs.first() {
            if let Some(exec_id) = &first_job.execution_id {
                if let Ok(nodes) = load_nodes_from_execution(orch.db.local.clone(), exec_id) {
                    for node in nodes {
                        node_map.insert(node.id.clone(), node);
                    }
                }
            }
        }

        for job in ready_jobs {
            if let Some(node_id) = &job.recipe_node_id {
                if let Some(node) = node_map.get(node_id) {
                    let db_job = load_job(orch.db.local.clone(), &job.id)?
                        .ok_or_else(|| format!("Failed to load job: {}", job.id))?;
                    node_jobs.push((db_job, node.clone()));
                    continue;
                }
            }
            agent_jobs.push(job);
        }
    }

    // Step 3: Handle node-based jobs
    for (db_job, node) in node_jobs {
        match node.node_type.as_str() {
            "checkpoint" => {
                handle_checkpoint_node(orch, &db_job, &node, execution_id, &mut effects)?;
            }
            "agent" => {
                agent_jobs.push(Job::try_from(db_job)?);
            }
            "action" => {
                log::warn!(
                    "Found job for action node - this shouldn't happen with new architecture"
                );
            }
            _ => {
                log::warn!("Unknown node type '{}', skipping", node.node_type);
            }
        }
    }

    // Step 4: Dispatch ready action nodes
    {
        let ready_action_nodes = crate::execution::advancement::find_ready_action_nodes(
            orch.db.local.clone(),
            execution_id,
        )?;

        for node in ready_action_nodes {
            let action_run_id =
                crate::execution::advancement::create_action_run(orch, execution_id, &node)?;

            effects.push(WorkflowEffect::ExecuteAction {
                action_run_id,
                execution_id: execution_id.to_string(),
                node_id: node.id.clone(),
                ctx: EffectContext {
                    job_id: None,
                    run_id: None,
                    execution_id: Some(execution_id.to_string()),
                    source: EffectSource::DagAdvancement,
                },
            });
        }
    }

    // Step 5: Dispatch ready condition nodes
    {
        let ready_condition_nodes =
            find_ready_condition_nodes(orch.db.local.clone(), execution_id)?;

        for node in ready_condition_nodes {
            run_advancement_db({
                let db = orch.db.local.clone();
                let execution_id = execution_id.to_string();
                let node_id = node.id.clone();
                async move {
                    crate::execution::conditions::gather_condition_context(
                        db.as_ref(),
                        &execution_id,
                        &node_id,
                    )
                    .await
                    .map(|_| ())
                }
            })?;

            // Extract condition spec from the node config
            let condition_spec = extract_condition_spec(&node);

            effects.push(WorkflowEffect::EvaluateCondition {
                execution_id: execution_id.to_string(),
                node_id: node.id.clone(),
                node_name: node.name.clone(),
                condition: condition_spec,
                ctx: EffectContext {
                    job_id: None,
                    run_id: None,
                    execution_id: Some(execution_id.to_string()),
                    source: EffectSource::DagAdvancement,
                },
            });
        }
    }

    Ok((agent_jobs, effects))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(config: Option<&str>) -> DbRecipeNode {
        DbRecipeNode {
            id: "cond-1".into(),
            recipe_id: "r-1".into(),
            node_type: "condition".into(),
            name: "Test Condition".into(),
            position_x: 0.0,
            position_y: 0.0,
            config: config.map(|s| s.to_string()),
            created_at: 0,
            updated_at: 0,
            parent_id: None,
        }
    }

    #[test]
    fn extract_condition_spec_full_config() {
        let node = make_node(Some(
            r#"{
            "conditionType": "llm",
            "expression": "x > 0",
            "question": "Is the value positive?",
            "ports": ["yes", "no"],
            "errorHandling": "fail_closed"
        }"#,
        ));
        let spec = extract_condition_spec(&node);

        assert_eq!(spec.condition_type, "llm");
        assert_eq!(spec.expression.as_deref(), Some("x > 0"));
        assert_eq!(spec.question.as_deref(), Some("Is the value positive?"));
        assert_eq!(spec.ports, vec!["yes", "no"]);
        assert_eq!(spec.error_handling, "fail_closed");
    }

    #[test]
    fn extract_condition_spec_defaults_on_missing_fields() {
        let node = make_node(Some("{}"));
        let spec = extract_condition_spec(&node);

        assert_eq!(spec.condition_type, "programmatic");
        assert!(spec.expression.is_none());
        assert!(spec.question.is_none());
        assert!(spec.ports.is_empty());
        assert_eq!(spec.error_handling, "use_default");
    }

    #[test]
    fn extract_condition_spec_defaults_on_no_config() {
        let node = make_node(None);
        let spec = extract_condition_spec(&node);

        assert_eq!(spec.condition_type, "programmatic");
        assert!(spec.expression.is_none());
        assert!(spec.question.is_none());
        assert!(spec.ports.is_empty());
        assert_eq!(spec.error_handling, "use_default");
    }

    #[test]
    fn extract_condition_spec_invalid_json_uses_defaults() {
        let node = make_node(Some("not valid json"));
        let spec = extract_condition_spec(&node);

        assert_eq!(spec.condition_type, "programmatic");
        assert_eq!(spec.error_handling, "use_default");
    }

    #[test]
    fn extract_condition_spec_ignores_non_string_ports() {
        let node = make_node(Some(r#"{"ports": ["yes", 42, "no", null]}"#));
        let spec = extract_condition_spec(&node);

        assert_eq!(spec.ports, vec!["yes", "no"]);
    }

    // ---- reparent_delegated_jobs disambiguation -------------------------------

    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};

    async fn migrated_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.keep().join("dag-reparent-test.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) \
                     VALUES ('proj', 'default', 'Proj', 'PROJ', '/tmp/proj', 0, 0)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        db
    }

    async fn insert_job(
        db: &LocalDb,
        id: &str,
        recipe_node_id: Option<&str>,
        parent_job_id: Option<&str>,
        uri_segment: Option<&str>,
    ) {
        let id = id.to_string();
        let recipe_node_id = recipe_node_id.map(str::to_string);
        let parent_job_id = parent_job_id.map(str::to_string);
        let uri_segment = uri_segment.map(str::to_string);
        db.write(move |conn| {
            let id = id.clone();
            let recipe_node_id = recipe_node_id.clone();
            let parent_job_id = parent_job_id.clone();
            let uri_segment = uri_segment.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO jobs(id, status, project_id, created_at, updated_at, \
                     recipe_node_id, parent_job_id, uri_segment) \
                     VALUES (?1, 'pending', 'proj', 0, 0, ?2, ?3, ?4)",
                    params![
                        id.as_str(),
                        recipe_node_id.as_deref(),
                        parent_job_id.as_deref(),
                        uri_segment.as_deref()
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn job_row(db: &LocalDb, id: &str) -> (Option<String>, Option<String>) {
        let id = id.to_string();
        db.read(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT uri_segment, parent_job_id FROM jobs WHERE id = ?1",
                        (id.as_str(),),
                    )
                    .await?;
                let row = rows.next().await?.unwrap();
                Ok((row.opt_text(0)?, row.opt_text(1)?))
            })
        })
        .await
        .unwrap()
    }

    fn packet(
        id: &str,
        parent: &str,
        node_id: &str,
        title: &str,
        task_index: i32,
    ) -> DelegatedWorkPacket {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "parentJobId": parent,
            "origin": "task_tool",
            "title": title,
            "problemStatement": "x",
            "agentConfigId": "Explore",
            "ownership": { "cwd": "/tmp" },
            "outputContract": { "schemaType": "return" },
            "status": "materialized",
            "materializedNodeIds": [node_id],
            "taskIndex": task_index,
            "createdAt": 0
        }))
        .unwrap()
    }

    /// A batch whose task titles collide with an existing child of the same node
    /// must disambiguate with -N suffixes (not violate the unique index) and
    /// re-parent every job (no top-level sibling leakage). This is the exact
    /// failure that sank the first attempt.
    #[tokio::test]
    async fn reparent_disambiguates_against_existing_children() {
        let db = migrated_db().await;
        insert_job(&db, "parent", None, None, Some("builder")).await;
        insert_job(&db, "existing", None, Some("parent"), Some("explore")).await;
        // New jobs start top-level with stale colliding segments.
        insert_job(&db, "new-1", Some("node-1"), None, Some("explore")).await;
        insert_job(&db, "new-2", Some("node-2"), None, Some("explore")).await;

        let packets = vec![
            packet("pkt-1", "parent", "node-1", "Explore", 0),
            packet("pkt-2", "parent", "node-2", "Explore", 1),
        ];
        let ordered = vec![
            ("new-1".to_string(), Some("node-1".to_string())),
            ("new-2".to_string(), Some("node-2".to_string())),
        ];
        reparent_delegated_jobs(&db, ordered, packets)
            .await
            .unwrap();

        let (seg1, parent1) = job_row(&db, "new-1").await;
        let (seg2, parent2) = job_row(&db, "new-2").await;
        assert_eq!(parent1.as_deref(), Some("parent"));
        assert_eq!(parent2.as_deref(), Some("parent"));
        let mut segs = vec![seg1.unwrap(), seg2.unwrap()];
        segs.sort();
        assert_eq!(segs, vec!["explore-2".to_string(), "explore-3".to_string()]);
    }

    /// Two jobs in the same batch with identical titles must disambiguate against
    /// each other via the per-parent reserved set.
    #[tokio::test]
    async fn reparent_disambiguates_within_batch() {
        let db = migrated_db().await;
        insert_job(&db, "parent", None, None, Some("builder")).await;
        insert_job(&db, "a", Some("node-a"), None, Some("explore")).await;
        insert_job(&db, "b", Some("node-b"), None, Some("explore")).await;

        let packets = vec![
            packet("pkt-a", "parent", "node-a", "Explore", 0),
            packet("pkt-b", "parent", "node-b", "Explore", 1),
        ];
        let ordered = vec![
            ("a".to_string(), Some("node-a".to_string())),
            ("b".to_string(), Some("node-b".to_string())),
        ];
        reparent_delegated_jobs(&db, ordered, packets)
            .await
            .unwrap();

        let (seg_a, _) = job_row(&db, "a").await;
        let (seg_b, _) = job_row(&db, "b").await;
        let mut segs = vec![seg_a.unwrap(), seg_b.unwrap()];
        segs.sort();
        assert_eq!(segs, vec!["explore".to_string(), "explore-2".to_string()]);
    }

    async fn job_link(db: &LocalDb, id: &str) -> (Option<String>, Option<i64>) {
        let id = id.to_string();
        db.read(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT parent_tool_use_id, task_index FROM jobs WHERE id = ?1",
                        (id.as_str(),),
                    )
                    .await?;
                let row = rows.next().await?.unwrap();
                Ok((row.opt_text(0)?, row.opt_i64(1)?))
            })
        })
        .await
        .unwrap()
    }

    fn packet_with_tool_use(
        id: &str,
        parent: &str,
        node_id: &str,
        title: &str,
        task_index: i32,
        parent_tool_use_id: &str,
    ) -> DelegatedWorkPacket {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "parentJobId": parent,
            "parentToolUseId": parent_tool_use_id,
            "origin": "task_tool",
            "title": title,
            "problemStatement": "x",
            "agentConfigId": "Explore",
            "ownership": { "cwd": "/tmp" },
            "outputContract": { "schemaType": "return" },
            "status": "materialized",
            "materializedNodeIds": [node_id],
            "taskIndex": task_index,
            "createdAt": 0
        }))
        .unwrap()
    }

    /// Child jobs spawned by one change→tasks call must carry the originating
    /// tool-use id (shared across the batch) and their task_index, so the
    /// transcript can locate them via `list_child_jobs` ordered by task_index.
    /// Without the parent_tool_use_id write the column stays NULL and the live
    /// task windows can never resolve — the core CAIRN-1149 linking fix.
    #[tokio::test]
    async fn reparent_links_children_by_parent_tool_use_id() {
        let db = migrated_db().await;
        insert_job(&db, "parent", None, None, Some("builder")).await;
        insert_job(&db, "child-0", Some("node-0"), None, Some("explore")).await;
        insert_job(&db, "child-1", Some("node-1"), None, Some("build")).await;

        let packets = vec![
            packet_with_tool_use("pkt-0", "parent", "node-0", "Explore", 0, "toolu_change_1"),
            packet_with_tool_use("pkt-1", "parent", "node-1", "Build", 1, "toolu_change_1"),
        ];
        let ordered = vec![
            ("child-0".to_string(), Some("node-0".to_string())),
            ("child-1".to_string(), Some("node-1".to_string())),
        ];
        reparent_delegated_jobs(&db, ordered, packets)
            .await
            .unwrap();

        let (tool_use_0, idx_0) = job_link(&db, "child-0").await;
        let (tool_use_1, idx_1) = job_link(&db, "child-1").await;
        assert_eq!(tool_use_0.as_deref(), Some("toolu_change_1"));
        assert_eq!(tool_use_1.as_deref(), Some("toolu_change_1"));
        assert_eq!(idx_0, Some(0));
        assert_eq!(idx_1, Some(1));
    }

    /// A single-task change append persists the link just the same.
    #[tokio::test]
    async fn reparent_links_single_child() {
        let db = migrated_db().await;
        insert_job(&db, "parent", None, None, Some("builder")).await;
        insert_job(&db, "child", Some("node-0"), None, Some("explore")).await;

        let packets = vec![packet_with_tool_use(
            "pkt",
            "parent",
            "node-0",
            "Explore",
            0,
            "toolu_solo",
        )];
        let ordered = vec![("child".to_string(), Some("node-0".to_string()))];
        reparent_delegated_jobs(&db, ordered, packets)
            .await
            .unwrap();

        let (tool_use, idx) = job_link(&db, "child").await;
        assert_eq!(tool_use.as_deref(), Some("toolu_solo"));
        assert_eq!(idx, Some(0));
    }
}
