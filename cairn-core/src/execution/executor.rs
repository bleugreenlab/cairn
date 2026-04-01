//! Executor node expansion — reads a TaskList artifact and injects task nodes into the DAG.
//!
//! When an executor node becomes "ready" during DAG advancement, this module:
//! 1. Reads the TaskList artifact from the upstream context edge
//! 2. Validates the task dependency graph (no cycles)
//! 3. Injects new nodes (context + agent per task) and edges into the execution snapshot
//! 4. Creates jobs for the new agent nodes
//! 5. The existing DAG advancement engine handles the rest (readiness, dispatch, completion)

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;

use diesel::prelude::*;
use uuid::Uuid;

use crate::config::agents as config_agents;
use crate::config::presets::{
    load_effective_presets, normalize_authored_selection, resolve_agent_snapshot,
    resolve_runtime_selection, PresetsConfig,
};
use crate::diesel_models::{DbJob, DbRecipeNode, UpdateExecutionChangeset};
use crate::models::{
    AgentGitConfig, AgentNodeConfig, AgentSnapshot, ContextNodeConfig, ExecutionSnapshot, Model,
    NodePosition, RecipeEdge, RecipeEdgeType, RecipeNode, RecipeNodeType, WorktreeMode,
};
use crate::schema::{artifacts, executions, jobs};
use crate::services::EventEmitter;

use super::dag::create_jobs_for_new_nodes;

/// A single task from the TaskList artifact.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct TaskListTask {
    pub id: String,
    pub title: String,
    pub agent: String,
    pub prompt: String,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(alias = "model")]
    pub tier: Option<String>,
    #[serde(rename = "backend", alias = "backendPreference")]
    pub backend_preference: Option<String>,
}

/// The TaskList artifact structure.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct TaskList {
    pub objective: String,
    #[serde(default)]
    pub requirements: Vec<String>,
    pub tasks: Vec<TaskListTask>,
}

/// Expand an executor node: read TaskList, inject nodes/edges, create jobs.
///
/// The executor_job_id is used as parent_job_id for all created task jobs,
/// so they render as children in the timeline. After expansion, the executor
/// job is marked complete (its role is done). A new Collector agent node is
/// injected that receives control + context edges from all leaf tasks. When
/// leaf tasks finish, the collector becomes ready and runs the collection
/// phase (build, test, submit PR details). The executor's pre-existing
/// downstream edges (e.g. → create-pr) are transferred to the collector.
///
/// Returns the IDs of newly created agent nodes (tasks + collector).
pub fn expand_executor_node(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    executor_node: &DbRecipeNode,
    executor_job_id: &str,
    config_dir: &Path,
    project_path: Option<&Path>,
    emitter: &dyn EventEmitter,
) -> Result<HashSet<String>, String> {
    log::info!(
        "Expanding executor node '{}' in execution {}",
        executor_node.name,
        execution_id
    );

    // 1. Load the current snapshot
    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(conn)
        .map_err(|e| format!("Failed to load execution: {}", e))?;

    let snapshot_json = snapshot_json.ok_or("Execution has no snapshot")?;
    let mut snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json)
        .map_err(|e| format!("Failed to parse snapshot: {}", e))?;

    // 2. Resolve TaskList from upstream artifact
    let tasklist = resolve_tasklist_input(conn, execution_id, &executor_node.id)?;

    log::info!(
        "TaskList: objective='{}', {} tasks",
        tasklist.objective,
        tasklist.tasks.len()
    );

    // 3. Validate DAG (no cycles)
    validate_task_dependencies(&tasklist)?;

    // 4. Capture existing outgoing edges from executor (e.g. executor → create-pr)
    //    so we can transfer them to the collector node later.
    let existing_outgoing_edge_ids: Vec<String> = snapshot
        .recipe
        .edges
        .iter()
        .filter(|e| e.source_node_id == executor_node.id)
        .map(|e| e.id.clone())
        .collect();

    // 5. Build node/edge IDs
    let exec_prefix = &executor_node.id;
    let start_id = format!("{}-start", exec_prefix);

    // Position offset from executor node
    let base_x = executor_node.position_x;
    let base_y = executor_node.position_y + 100.0;

    // 5. Create Start context node (provides TaskList overview to root tasks)
    let overview = format!(
        "## Task Execution\n\n**Objective:** {}\n\n**Requirements:**\n{}\n\n**Tasks:** {}",
        tasklist.objective,
        tasklist
            .requirements
            .iter()
            .map(|r| format!("- {}", r))
            .collect::<Vec<_>>()
            .join("\n"),
        tasklist
            .tasks
            .iter()
            .map(|t| format!("- {} ({})", t.title, t.id))
            .collect::<Vec<_>>()
            .join("\n"),
    );

    snapshot.recipe.nodes.push(RecipeNode {
        id: start_id.clone(),
        node_type: RecipeNodeType::Context,
        name: "Task Overview".to_string(),
        position: NodePosition {
            x: base_x,
            y: base_y,
        },
        parent_id: None,
        trigger_config: None,
        agent_config: None,
        action_config: None,
        checkpoint_config: None,
        artifact_config: None,
        condition_config: None,
        context_config: Some(ContextNodeConfig { content: overview }),
    });

    // Edge: executor → start (control, so start inherits executor's readiness)
    snapshot.recipe.edges.push(make_edge(
        &executor_node.id,
        "control-out",
        &start_id,
        "control-in",
        RecipeEdgeType::Control,
    ));

    // Load presets config for agent resolution
    let presets = snapshot
        .presets
        .as_ref()
        .map(PresetsConfig::from)
        .unwrap_or_else(|| load_effective_presets(config_dir, project_path));

    // 6. For each task: create ContextNode (prompt) + AgentNode
    let mut new_agent_node_ids = HashSet::new();

    for (i, task) in tasklist.tasks.iter().enumerate() {
        let ctx_id = format!("{}-ctx-{}", exec_prefix, task.id);
        let agent_id = format!("{}-task-{}", exec_prefix, task.id);
        let y_offset = base_y + 100.0 + (i as f32 * 150.0);

        // Context node with task prompt
        snapshot.recipe.nodes.push(RecipeNode {
            id: ctx_id.clone(),
            node_type: RecipeNodeType::Context,
            name: format!("{} (prompt)", task.title),
            position: NodePosition {
                x: base_x - 200.0,
                y: y_offset,
            },
            parent_id: None,
            trigger_config: None,
            agent_config: None,
            action_config: None,
            checkpoint_config: None,
            artifact_config: None,
            condition_config: None,
            context_config: Some(ContextNodeConfig {
                content: task.prompt.clone(),
            }),
        });

        // Agent node for the task
        let task_agent_id = task.agent.clone();

        let task_tier_override = task.tier.as_deref();

        // Load agent config from files and populate snapshot
        match snapshot.agents.entry(task_agent_id.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                let agent_snapshot =
                    match config_agents::get_agent(config_dir, entry.key(), project_path) {
                        Ok(Some(mut fa)) => {
                            if task.backend_preference.is_some() {
                                fa.backend_preference = task.backend_preference.clone();
                            }
                            resolve_agent_snapshot(&fa, task_tier_override, &presets)?
                        }
                        _ => {
                            log::warn!(
                                "Agent config '{}' not found in files, using placeholder",
                                entry.key()
                            );
                            make_placeholder_agent(
                                entry.key(),
                                task_tier_override,
                                task.backend_preference.as_deref(),
                                &presets,
                            )
                        }
                    };
                entry.insert(agent_snapshot);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                // If a task specifies a tier override, resolve and apply it
                if task_tier_override.is_some() || task.backend_preference.is_some() {
                    let agent = entry.get_mut();
                    let authored_tier =
                        task_tier_override.or_else(|| agent.tier.as_ref().map(Model::as_str));
                    let authored_backend = task
                        .backend_preference
                        .as_deref()
                        .or(agent.backend_preference.as_deref());
                    let (model, backend, extras) =
                        resolve_runtime_selection(authored_tier, authored_backend, &presets)?;
                    if let Some(tier_ref) = task_tier_override {
                        agent.tier = Some(Model::new(
                            &crate::config::presets::normalize_tier_selection(tier_ref, &presets),
                        ));
                    }
                    if task.backend_preference.is_some() {
                        agent.backend_preference = task.backend_preference.clone();
                    }
                    agent.model = Some(model);
                    agent.resolved_backend = Some(backend);
                    agent.extras = Some(extras);
                }
            }
        }

        snapshot.recipe.nodes.push(RecipeNode {
            id: agent_id.clone(),
            node_type: RecipeNodeType::Agent,
            name: task.title.clone(),
            position: NodePosition {
                x: base_x,
                y: y_offset,
            },
            parent_id: None,
            trigger_config: None,
            agent_config: Some(AgentNodeConfig {
                agent_config_id: Some(task_agent_id),
                checkpoint: None,
                output_schema: None,
                git_config: Some(AgentGitConfig {
                    worktree_mode: WorktreeMode::Inherit,
                }),
            }),
            action_config: None,
            checkpoint_config: None,
            artifact_config: None,
            condition_config: None,
            context_config: None,
        });

        new_agent_node_ids.insert(agent_id.clone());
    }

    // 8. Create edges between tasks.
    //    Order matters — context edges are resolved in order, and we want:
    //    [overview, upstream task outputs, task prompt]
    for task in &tasklist.tasks {
        let agent_id = format!("{}-task-{}", exec_prefix, task.id);
        let ctx_id = format!("{}-ctx-{}", exec_prefix, task.id);

        // a) Overview context (objective, requirements, task list)
        snapshot.recipe.edges.push(make_edge(
            &start_id,
            "context-out",
            &agent_id,
            "context-in",
            RecipeEdgeType::Context,
        ));

        // b) Control + context from dependencies (upstream task outputs)
        if task.dependencies.is_empty() {
            // Root task: control edge from executor node for reachability + scheduling.
            snapshot.recipe.edges.push(make_edge(
                &executor_node.id,
                "control-out",
                &agent_id,
                "control-in",
                RecipeEdgeType::Control,
            ));
        }

        for dep in &task.dependencies {
            let dep_agent_id = format!("{}-task-{}", exec_prefix, dep);
            snapshot.recipe.edges.push(make_edge(
                &dep_agent_id,
                "control-out",
                &agent_id,
                "control-in",
                RecipeEdgeType::Control,
            ));
            snapshot.recipe.edges.push(make_edge(
                &dep_agent_id,
                "context-out",
                &agent_id,
                "context-in",
                RecipeEdgeType::Context,
            ));
        }

        // c) Task prompt (last, so agent sees overview + upstream first)
        snapshot.recipe.edges.push(make_edge(
            &ctx_id,
            "context-out",
            &agent_id,
            "context-in",
            RecipeEdgeType::Context,
        ));
    }

    // 9. Create Collector node — a separate agent node that runs after all leaf tasks complete.
    //    The executor stays as-is (type Executor) and marks complete after expansion.
    //    The collector inherits the executor's downstream edges (e.g. → create-pr).
    let leaf_task_ids = find_leaf_tasks(&tasklist);
    let collector_id = format!("{}-collector", exec_prefix);

    // Load collector agent config into snapshot
    let collector_agent_config_id = "collector".to_string();
    match snapshot.agents.entry(collector_agent_config_id.clone()) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            let agent_snapshot =
                match config_agents::get_agent(config_dir, entry.key(), project_path) {
                    Ok(Some(fa)) => resolve_agent_snapshot(&fa, None, &presets)?,
                    _ => {
                        log::warn!("Collector agent config not found in files, using placeholder");
                        make_placeholder_agent("collector", None, None, &presets)
                    }
                };
            entry.insert(agent_snapshot);
        }
        std::collections::hash_map::Entry::Occupied(_) => {}
    }

    // Create collector node
    snapshot.recipe.nodes.push(RecipeNode {
        id: collector_id.clone(),
        node_type: RecipeNodeType::Agent,
        name: "Collector".to_string(),
        position: NodePosition {
            x: executor_node.position_x + 200.0,
            y: executor_node.position_y,
        },
        parent_id: None,
        trigger_config: None,
        agent_config: Some(AgentNodeConfig {
            agent_config_id: Some(collector_agent_config_id.clone()),
            checkpoint: None,
            output_schema: None,
            git_config: Some(AgentGitConfig {
                worktree_mode: WorktreeMode::Inherit,
            }),
        }),
        action_config: None,
        checkpoint_config: None,
        artifact_config: None,
        condition_config: None,
        context_config: None,
    });

    new_agent_node_ids.insert(collector_id.clone());

    // Leaf tasks → collector (control + context)
    for leaf_id in &leaf_task_ids {
        let agent_id = format!("{}-task-{}", exec_prefix, leaf_id);
        snapshot.recipe.edges.push(make_edge(
            &agent_id,
            "control-out",
            &collector_id,
            "control-in",
            RecipeEdgeType::Control,
        ));
        snapshot.recipe.edges.push(make_edge(
            &agent_id,
            "context-out",
            &collector_id,
            "context-in",
            RecipeEdgeType::Context,
        ));
    }

    // Overview → collector (context, for objective/requirements)
    snapshot.recipe.edges.push(make_edge(
        &start_id,
        "context-out",
        &collector_id,
        "context-in",
        RecipeEdgeType::Context,
    ));

    // 10. Transfer executor's pre-existing downstream edges to the collector.
    //     E.g. executor → create-pr becomes collector → create-pr.
    for edge in &mut snapshot.recipe.edges {
        if existing_outgoing_edge_ids.contains(&edge.id) {
            edge.source_node_id = collector_id.clone();
        }
    }

    // 11. Save updated snapshot
    let snapshot_json = serde_json::to_string(&snapshot)
        .map_err(|e| format!("Failed to serialize snapshot: {}", e))?;

    diesel::update(executions::table.find(execution_id))
        .set(UpdateExecutionChangeset {
            snapshot: Some(snapshot_json),
            ..Default::default()
        })
        .execute(conn)
        .map_err(|e| format!("Failed to update execution snapshot: {}", e))?;

    // 12. Create jobs for new agent nodes (includes collector)
    let created_jobs =
        create_jobs_for_new_nodes(conn, execution_id, &new_agent_node_ids, &snapshot)?;

    // 13. Set parent_job_id on all created jobs so they render under the executor
    for job in &created_jobs {
        diesel::update(jobs::table.find(&job.id))
            .set(jobs::parent_job_id.eq(Some(executor_job_id)))
            .execute(conn)
            .map_err(|e| format!("Failed to set parent_job_id: {}", e))?;
    }

    // 14. Mark executor job complete. The executor's role is finished — it expanded
    //     the task list and created the DAG. Root tasks have control edges from the
    //     executor, so they become ready immediately. The collector node (separate)
    //     will be dispatched when all leaf tasks complete.
    //
    // NOTE: Intentionally NOT routed through transition_job. Executor nodes are
    // internal orchestration — routing through transition_job would fire
    // TriggerEvent::JobEnded for these internal nodes, which is undesirable.
    let now = chrono::Utc::now().timestamp() as i32;
    diesel::update(jobs::table.find(executor_job_id))
        .set((
            jobs::status.eq("complete"),
            jobs::completed_at.eq(Some(now)),
            jobs::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| format!("Failed to update executor job: {}", e))?;

    // Recompute execution and issue status now that the executor job is terminal
    crate::transitions::recompute_execution_status(conn, execution_id);

    // Also recompute issue status for full cascade consistency
    let issue_id: Option<String> = executions::table
        .find(execution_id)
        .select(executions::issue_id)
        .first(conn)
        .ok()
        .flatten();
    if let Some(ref iid) = issue_id {
        crate::transitions::recompute_issue_status(conn, iid);
    }

    log::info!(
        "Executor expansion created {} jobs for {} tasks",
        created_jobs.len(),
        tasklist.tasks.len()
    );

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "jobs", "action": "update"}),
    );

    Ok(new_agent_node_ids)
}

/// Resolve the TaskList artifact from upstream context edges.
fn resolve_tasklist_input(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    executor_node_id: &str,
) -> Result<TaskList, String> {
    // Load snapshot to find context edges pointing to this executor
    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(conn)
        .map_err(|e| format!("Failed to load execution: {}", e))?;

    let snapshot_json = snapshot_json.ok_or("Execution has no snapshot")?;
    let snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json)
        .map_err(|e| format!("Failed to parse snapshot: {}", e))?;

    // Find context edges targeting this executor
    let context_edges: Vec<&RecipeEdge> = snapshot
        .recipe
        .edges
        .iter()
        .filter(|e| e.target_node_id == executor_node_id && e.edge_type == RecipeEdgeType::Context)
        .collect();

    // Look for a tasklist artifact from upstream jobs
    for edge in context_edges {
        // Find the job for the source node
        let source_job: Result<DbJob, _> = jobs::table
            .filter(jobs::execution_id.eq(execution_id))
            .filter(jobs::recipe_node_id.eq(&edge.source_node_id))
            .first(conn);

        let source_job = match source_job {
            Ok(j) => j,
            Err(_) => continue,
        };

        // Look for a tasklist artifact on this job
        let artifact_data: Result<String, _> = artifacts::table
            .filter(artifacts::job_id.eq(&source_job.id))
            .filter(artifacts::artifact_type.eq("tasklist"))
            .order(artifacts::version.desc())
            .select(artifacts::data)
            .first(conn);

        if let Ok(data) = artifact_data {
            let tasklist: TaskList = serde_json::from_str(&data)
                .map_err(|e| format!("Failed to parse TaskList artifact: {}", e))?;
            return Ok(tasklist);
        }

        // Also try any artifact on this job (might be stored with generic type)
        let any_artifact_data: Result<String, _> = artifacts::table
            .filter(artifacts::job_id.eq(&source_job.id))
            .order(artifacts::version.desc())
            .select(artifacts::data)
            .first(conn);

        if let Ok(data) = any_artifact_data {
            if let Ok(tasklist) = serde_json::from_str::<TaskList>(&data) {
                // Validate it looks like a TaskList
                if !tasklist.tasks.is_empty() {
                    return Ok(tasklist);
                }
            }
        }
    }

    Err("No TaskList artifact found on upstream jobs".to_string())
}

/// Validate that task dependencies form a DAG (no cycles).
fn validate_task_dependencies(tasklist: &TaskList) -> Result<(), String> {
    let task_ids: HashSet<&str> = tasklist.tasks.iter().map(|t| t.id.as_str()).collect();

    // Check all dependencies reference valid task IDs
    for task in &tasklist.tasks {
        for dep in &task.dependencies {
            if !task_ids.contains(dep.as_str()) {
                return Err(format!(
                    "Task '{}' depends on unknown task '{}'",
                    task.id, dep
                ));
            }
        }
        // Check self-reference
        if task.dependencies.contains(&task.id) {
            return Err(format!("Task '{}' depends on itself", task.id));
        }
    }

    // Topological sort to detect cycles
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();

    for task in &tasklist.tasks {
        in_degree.entry(task.id.as_str()).or_insert(0);
        for dep in &task.dependencies {
            adj.entry(dep.as_str()).or_default().push(task.id.as_str());
            *in_degree.entry(task.id.as_str()).or_insert(0) += 1;
        }
    }

    let mut queue: VecDeque<&str> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(&id, _)| id)
        .collect();

    let mut visited = 0;
    while let Some(node) = queue.pop_front() {
        visited += 1;
        if let Some(neighbors) = adj.get(node) {
            for &next in neighbors {
                if let Some(deg) = in_degree.get_mut(next) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(next);
                    }
                }
            }
        }
    }

    if visited != task_ids.len() {
        return Err("TaskList contains cyclic dependencies".to_string());
    }

    Ok(())
}

/// Find leaf tasks (tasks that no other task depends on).
fn find_leaf_tasks(tasklist: &TaskList) -> Vec<String> {
    let depended_on: HashSet<&str> = tasklist
        .tasks
        .iter()
        .flat_map(|t| t.dependencies.iter().map(|d| d.as_str()))
        .collect();

    tasklist
        .tasks
        .iter()
        .filter(|t| !depended_on.contains(t.id.as_str()))
        .map(|t| t.id.clone())
        .collect()
}

/// Build a placeholder AgentSnapshot when the agent config file is missing.
fn make_placeholder_agent(
    agent_id: &str,
    tier_override: Option<&str>,
    backend_preference: Option<&str>,
    presets: &PresetsConfig,
) -> AgentSnapshot {
    let authored = normalize_authored_selection(tier_override, backend_preference, presets);
    let (model, backend, extras) = resolve_runtime_selection(
        Some(authored.tier.as_str()),
        authored.backend.as_deref(),
        presets,
    )
    .unwrap_or_else(|_| {
        (
            authored.tier.clone(),
            authored
                .backend
                .clone()
                .unwrap_or_else(|| presets.active_backend.clone()),
            Default::default(),
        )
    });

    // Capitalize first letter for display
    let name = {
        let mut chars = agent_id.chars();
        match chars.next() {
            None => String::new(),
            Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
        }
    };

    AgentSnapshot {
        id: agent_id.to_string(),
        name,
        description: String::new(),
        prompt: String::new(),
        tools: vec![],
        tier: Some(authored.tier),
        backend_preference: authored.backend,
        model: Some(model),
        disallowed_tools: None,
        skills: None,
        approval_policy: None,
        filesystem_scope: None,
        resolved_backend: Some(backend),
        extras: Some(extras),
    }
}

/// Helper to create a RecipeEdge.
fn make_edge(
    source_id: &str,
    source_handle: &str,
    target_id: &str,
    target_handle: &str,
    edge_type: RecipeEdgeType,
) -> RecipeEdge {
    RecipeEdge {
        id: Uuid::new_v4().to_string(),
        edge_type,
        source_node_id: source_id.to_string(),
        source_handle: source_handle.to_string(),
        target_node_id: target_id.to_string(),
        target_handle: target_handle.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::{NewArtifact, NewExecution, NewJob};
    use crate::models::{RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType};
    use crate::schema::{artifacts, executions, issues, jobs};
    use crate::services::testing::CapturingEmitter;
    use crate::test_utils::{create_test_issue, create_test_project, test_diesel_conn};
    use std::collections::HashMap;

    fn make_task(id: &str, deps: Vec<&str>) -> TaskListTask {
        TaskListTask {
            id: id.to_string(),
            title: format!("Task {}", id),
            agent: "build".to_string(),
            prompt: format!("Do {}", id),
            dependencies: deps.into_iter().map(String::from).collect(),
            tier: None,
            backend_preference: None,
        }
    }

    fn make_tasklist(tasks: Vec<TaskListTask>) -> TaskList {
        TaskList {
            objective: "Test".to_string(),
            requirements: vec![],
            tasks,
        }
    }

    #[test]
    fn test_validate_no_deps() {
        let tl = make_tasklist(vec![make_task("a", vec![]), make_task("b", vec![])]);
        assert!(validate_task_dependencies(&tl).is_ok());
    }

    #[test]
    fn test_validate_linear_deps() {
        let tl = make_tasklist(vec![
            make_task("a", vec![]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["b"]),
        ]);
        assert!(validate_task_dependencies(&tl).is_ok());
    }

    #[test]
    fn test_validate_diamond_deps() {
        let tl = make_tasklist(vec![
            make_task("a", vec![]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["a"]),
            make_task("d", vec!["b", "c"]),
        ]);
        assert!(validate_task_dependencies(&tl).is_ok());
    }

    #[test]
    fn test_validate_cycle() {
        let tl = make_tasklist(vec![
            make_task("a", vec!["c"]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["b"]),
        ]);
        assert!(validate_task_dependencies(&tl).is_err());
    }

    #[test]
    fn test_validate_self_reference() {
        let tl = make_tasklist(vec![make_task("a", vec!["a"])]);
        assert!(validate_task_dependencies(&tl).is_err());
    }

    #[test]
    fn test_validate_unknown_dep() {
        let tl = make_tasklist(vec![make_task("a", vec!["nonexistent"])]);
        assert!(validate_task_dependencies(&tl).is_err());
    }

    #[test]
    fn test_find_leaf_tasks_all_roots() {
        let tl = make_tasklist(vec![make_task("a", vec![]), make_task("b", vec![])]);
        let leaves = find_leaf_tasks(&tl);
        assert_eq!(leaves.len(), 2);
    }

    #[test]
    fn test_find_leaf_tasks_linear() {
        let tl = make_tasklist(vec![
            make_task("a", vec![]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["b"]),
        ]);
        let leaves = find_leaf_tasks(&tl);
        assert_eq!(leaves, vec!["c"]);
    }

    #[test]
    fn test_find_leaf_tasks_diamond() {
        let tl = make_tasklist(vec![
            make_task("a", vec![]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["a"]),
            make_task("d", vec!["b", "c"]),
        ]);
        let leaves = find_leaf_tasks(&tl);
        assert_eq!(leaves, vec!["d"]);
    }

    // ========================================================================
    // Integration tests for expand_executor_node (two-phase lifecycle)
    // ========================================================================

    /// Set up DB fixtures for executor expansion tests.
    /// Returns (conn, execution_id, executor_node, executor_job_id, project_id).
    fn setup_executor_expansion(
        tasks: Vec<TaskListTask>,
    ) -> (
        diesel::SqliteConnection,
        String,
        DbRecipeNode,
        String,
        String,
    ) {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test issue");

        let execution_id = uuid::Uuid::new_v4().to_string();
        let trigger_node_id = "trigger-1";
        let executor_node_id = "executor-1";
        let planner_node_id = "planner-1";
        let downstream_node_id = "create-pr";

        // Build a snapshot with: trigger → planner → executor → create-pr
        let snapshot = ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Test Recipe".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![
                    RecipeNode {
                        id: trigger_node_id.to_string(),
                        node_type: RecipeNodeType::Trigger,
                        name: "Issue Trigger".to_string(),
                        position: NodePosition { x: -200.0, y: 0.0 },
                        parent_id: None,
                        trigger_config: None,
                        agent_config: None,
                        action_config: None,
                        checkpoint_config: None,
                        artifact_config: None,
                        condition_config: None,
                        context_config: None,
                    },
                    RecipeNode {
                        id: planner_node_id.to_string(),
                        node_type: RecipeNodeType::Agent,
                        name: "Planner".to_string(),
                        position: NodePosition { x: 0.0, y: 0.0 },
                        parent_id: None,
                        trigger_config: None,
                        agent_config: Some(AgentNodeConfig {
                            agent_config_id: Some("planner".to_string()),
                            checkpoint: None,
                            output_schema: None,
                            git_config: None,
                        }),
                        action_config: None,
                        checkpoint_config: None,
                        artifact_config: None,
                        condition_config: None,
                        context_config: None,
                    },
                    RecipeNode {
                        id: executor_node_id.to_string(),
                        node_type: RecipeNodeType::Executor,
                        name: "Executor".to_string(),
                        position: NodePosition { x: 200.0, y: 0.0 },
                        parent_id: None,
                        trigger_config: None,
                        agent_config: None,
                        action_config: None,
                        checkpoint_config: None,
                        artifact_config: None,
                        condition_config: None,
                        context_config: None,
                    },
                    RecipeNode {
                        id: downstream_node_id.to_string(),
                        node_type: RecipeNodeType::Action,
                        name: "Create PR".to_string(),
                        position: NodePosition { x: 400.0, y: 0.0 },
                        parent_id: None,
                        trigger_config: None,
                        agent_config: None,
                        action_config: None,
                        checkpoint_config: None,
                        artifact_config: None,
                        condition_config: None,
                        context_config: None,
                    },
                ],
                edges: vec![
                    // trigger → planner (control)
                    RecipeEdge {
                        id: "e0".to_string(),
                        edge_type: RecipeEdgeType::Control,
                        source_node_id: trigger_node_id.to_string(),
                        source_handle: "control-out".to_string(),
                        target_node_id: planner_node_id.to_string(),
                        target_handle: "control-in".to_string(),
                    },
                    // planner → executor (context, provides tasklist)
                    RecipeEdge {
                        id: "e1".to_string(),
                        edge_type: RecipeEdgeType::Context,
                        source_node_id: planner_node_id.to_string(),
                        source_handle: "context-out".to_string(),
                        target_node_id: executor_node_id.to_string(),
                        target_handle: "context-in".to_string(),
                    },
                    // planner → executor (control)
                    RecipeEdge {
                        id: "e2".to_string(),
                        edge_type: RecipeEdgeType::Control,
                        source_node_id: planner_node_id.to_string(),
                        source_handle: "control-out".to_string(),
                        target_node_id: executor_node_id.to_string(),
                        target_handle: "control-in".to_string(),
                    },
                    // executor → create-pr (control)
                    RecipeEdge {
                        id: "e3".to_string(),
                        edge_type: RecipeEdgeType::Control,
                        source_node_id: executor_node_id.to_string(),
                        source_handle: "control-out".to_string(),
                        target_node_id: downstream_node_id.to_string(),
                        target_handle: "control-in".to_string(),
                    },
                ],
            },
            agents: HashMap::from([(
                "planner".to_string(),
                AgentSnapshot {
                    id: "planner".to_string(),
                    name: "Planner".to_string(),
                    description: String::new(),
                    prompt: "Plan tasks".to_string(),
                    tools: vec![],
                    tier: Some(Model::default()),
                    backend_preference: None,
                    model: Some(Model::default()),
                    disallowed_tools: None,
                    skills: None,
                    approval_policy: None,
                    filesystem_scope: None,
                    resolved_backend: None,
                    extras: None,
                },
            )]),
            skills: HashMap::new(),
            tools: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: Some(issue_id.clone()),
                project_id: project_id.clone(),
                trigger_type: TriggerType::Manual,

                event_payload: None,
            },
            presets: None,
            delegated_packets: vec![],
            created_at: 1000,
        };

        let snapshot_json = serde_json::to_string(&snapshot).unwrap();

        // Insert execution
        diesel::insert_into(executions::table)
            .values(NewExecution {
                id: &execution_id,
                recipe_id: "recipe-1",
                issue_id: Some(&issue_id),
                project_id: Some(&project_id),
                status: "running",
                started_at: 1000,
                completed_at: None,
                snapshot: Some(&snapshot_json),
                seq: Some(1),
                initiator_sub: None,
                initiator_auth_mode: None,
                initiator_org_id: None,
                triggered_by: "manual",
            })
            .execute(&mut conn)
            .unwrap();

        // Insert planner job (complete, with tasklist artifact)
        let planner_job_id = uuid::Uuid::new_v4().to_string();
        diesel::insert_into(jobs::table)
            .values(NewJob {
                id: &planner_job_id,
                execution_id: Some(&execution_id),
                manager_id: None,
                recipe_node_id: Some(planner_node_id),
                parent_job_id: None,
                worktree_path: None,
                branch: None,
                base_commit: None,
                current_session_id: None,
                resume_session_id: None,
                status: "complete",
                agent_config_id: Some("planner"),
                issue_id: Some(&issue_id),
                project_id: &project_id,
                task_description: None,
                created_at: 1000,
                updated_at: 1001,
                completed_at: Some(1001),
                parent_tool_use_id: None,
                task_index: None,
                started_at: Some(1000),
                model: None,
                node_name: Some("Planner"),
                base_branch: None,
                current_turn_id: None,
            })
            .execute(&mut conn)
            .unwrap();

        // Insert tasklist artifact
        let tasklist = TaskList {
            objective: "Build features".to_string(),
            requirements: vec!["No regressions".to_string()],
            tasks,
        };
        let tasklist_json = serde_json::to_string(&tasklist).unwrap();
        let artifact_id = uuid::Uuid::new_v4().to_string();
        diesel::insert_into(artifacts::table)
            .values(NewArtifact {
                id: &artifact_id,
                job_id: Some(&planner_job_id),
                artifact_type: "tasklist",
                schema_version: 1,
                data: &tasklist_json,
                version: 1,
                parent_version_id: None,
                output_name: None,
                created_at: 1001,
                updated_at: 1001,
            })
            .execute(&mut conn)
            .unwrap();

        // Insert executor job
        let executor_job_id = uuid::Uuid::new_v4().to_string();
        diesel::insert_into(jobs::table)
            .values(NewJob {
                id: &executor_job_id,
                execution_id: Some(&execution_id),
                manager_id: None,
                recipe_node_id: Some(executor_node_id),
                parent_job_id: None,
                worktree_path: None,
                branch: None,
                base_commit: None,
                current_session_id: None,
                resume_session_id: None,
                status: "pending",
                agent_config_id: None,
                issue_id: Some(&issue_id),
                project_id: &project_id,
                task_description: None,
                created_at: 1000,
                updated_at: 1002,
                completed_at: None,
                parent_tool_use_id: None,
                task_index: None,
                started_at: None,
                model: None,
                node_name: Some("Executor"),
                base_branch: None,
                current_turn_id: None,
            })
            .execute(&mut conn)
            .unwrap();

        let executor_node = DbRecipeNode {
            id: executor_node_id.to_string(),
            recipe_id: "recipe-1".to_string(),
            node_type: "executor".to_string(),
            name: "Executor".to_string(),
            position_x: 200.0,
            position_y: 0.0,
            config: None,
            created_at: 1000,
            updated_at: 1000,
            parent_id: None,
        };

        (
            conn,
            execution_id,
            executor_node,
            executor_job_id,
            project_id,
        )
    }

    #[test]
    fn expand_executor_wires_leaf_tasks_to_collector() {
        let tasks = vec![make_task("a", vec![]), make_task("b", vec!["a"])];
        let (mut conn, execution_id, executor_node, executor_job_id, _) =
            setup_executor_expansion(tasks);
        let emitter = CapturingEmitter::new();
        let config_dir = std::path::Path::new("/nonexistent");

        expand_executor_node(
            &mut conn,
            &execution_id,
            &executor_node,
            &executor_job_id,
            config_dir,
            None,
            &emitter,
        )
        .unwrap();

        let snapshot_json: String = executions::table
            .find(&execution_id)
            .select(executions::snapshot)
            .first::<Option<String>>(&mut conn)
            .unwrap()
            .unwrap();
        let snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json).unwrap();

        // "b" is the leaf task (a→b). It should have control + context edges to collector.
        let collector_id = format!("{}-collector", executor_node.id);
        let leaf_agent_id = format!("{}-task-b", executor_node.id);
        let control_to_collector: Vec<_> = snapshot
            .recipe
            .edges
            .iter()
            .filter(|e| {
                e.source_node_id == leaf_agent_id
                    && e.target_node_id == collector_id
                    && e.edge_type == RecipeEdgeType::Control
            })
            .collect();
        assert_eq!(
            control_to_collector.len(),
            1,
            "leaf task should have a control edge to collector"
        );

        let context_to_collector: Vec<_> = snapshot
            .recipe
            .edges
            .iter()
            .filter(|e| {
                e.source_node_id == leaf_agent_id
                    && e.target_node_id == collector_id
                    && e.edge_type == RecipeEdgeType::Context
            })
            .collect();
        assert_eq!(
            context_to_collector.len(),
            1,
            "leaf task should have a context edge to collector"
        );

        // Non-leaf task "a" should NOT have edges to collector
        let non_leaf_id = format!("{}-task-a", executor_node.id);
        let non_leaf_to_collector: Vec<_> = snapshot
            .recipe
            .edges
            .iter()
            .filter(|e| e.source_node_id == non_leaf_id && e.target_node_id == collector_id)
            .collect();
        assert!(
            non_leaf_to_collector.is_empty(),
            "non-leaf task 'a' should not wire to collector"
        );
    }

    #[test]
    fn expand_executor_wires_overview_to_collector() {
        let tasks = vec![make_task("a", vec![])];
        let (mut conn, execution_id, executor_node, executor_job_id, _) =
            setup_executor_expansion(tasks);
        let emitter = CapturingEmitter::new();
        let config_dir = std::path::Path::new("/nonexistent");

        expand_executor_node(
            &mut conn,
            &execution_id,
            &executor_node,
            &executor_job_id,
            config_dir,
            None,
            &emitter,
        )
        .unwrap();

        let snapshot_json: String = executions::table
            .find(&execution_id)
            .select(executions::snapshot)
            .first::<Option<String>>(&mut conn)
            .unwrap()
            .unwrap();
        let snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json).unwrap();

        let start_id = format!("{}-start", executor_node.id);
        let collector_id = format!("{}-collector", executor_node.id);
        let overview_to_collector: Vec<_> = snapshot
            .recipe
            .edges
            .iter()
            .filter(|e| {
                e.source_node_id == start_id
                    && e.target_node_id == collector_id
                    && e.edge_type == RecipeEdgeType::Context
            })
            .collect();
        assert_eq!(
            overview_to_collector.len(),
            1,
            "overview context should wire to collector"
        );
    }

    #[test]
    fn expand_executor_loads_collector_agent_config() {
        let tasks = vec![make_task("a", vec![])];
        let (mut conn, execution_id, executor_node, executor_job_id, _) =
            setup_executor_expansion(tasks);
        let emitter = CapturingEmitter::new();
        let config_dir = std::path::Path::new("/nonexistent");

        expand_executor_node(
            &mut conn,
            &execution_id,
            &executor_node,
            &executor_job_id,
            config_dir,
            None,
            &emitter,
        )
        .unwrap();

        let snapshot_json: String = executions::table
            .find(&execution_id)
            .select(executions::snapshot)
            .first::<Option<String>>(&mut conn)
            .unwrap()
            .unwrap();
        let snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json).unwrap();

        assert!(
            snapshot.agents.contains_key("collector"),
            "snapshot should contain collector agent config"
        );
        let collector = &snapshot.agents["collector"];
        assert_eq!(collector.id, "collector");
        assert_eq!(collector.name, "Collector");
    }

    #[test]
    fn expand_executor_stays_executor_and_creates_collector_node() {
        let tasks = vec![make_task("a", vec![])];
        let (mut conn, execution_id, executor_node, executor_job_id, _) =
            setup_executor_expansion(tasks);
        let emitter = CapturingEmitter::new();
        let config_dir = std::path::Path::new("/nonexistent");

        expand_executor_node(
            &mut conn,
            &execution_id,
            &executor_node,
            &executor_job_id,
            config_dir,
            None,
            &emitter,
        )
        .unwrap();

        let snapshot_json: String = executions::table
            .find(&execution_id)
            .select(executions::snapshot)
            .first::<Option<String>>(&mut conn)
            .unwrap()
            .unwrap();
        let snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json).unwrap();

        // Executor node should remain Executor type (not mutated)
        let exec_node = snapshot
            .recipe
            .nodes
            .iter()
            .find(|n| n.id == executor_node.id)
            .expect("executor node should still exist in snapshot");
        assert_eq!(
            exec_node.node_type,
            RecipeNodeType::Executor,
            "executor node should stay Executor type"
        );

        // Collector node should exist as a separate Agent node
        let collector_id = format!("{}-collector", executor_node.id);
        let collector_node = snapshot
            .recipe
            .nodes
            .iter()
            .find(|n| n.id == collector_id)
            .expect("collector node should exist in snapshot");
        assert_eq!(
            collector_node.node_type,
            RecipeNodeType::Agent,
            "collector node should be Agent type"
        );

        let agent_cfg = collector_node.agent_config.as_ref().unwrap();
        assert_eq!(
            agent_cfg.agent_config_id.as_deref(),
            Some("collector"),
            "collector agent config should reference collector"
        );
        assert_eq!(
            agent_cfg.git_config.as_ref().unwrap().worktree_mode,
            WorktreeMode::Inherit,
            "collector should use Inherit worktree mode"
        );
    }

    #[test]
    fn expand_executor_marks_job_complete_and_creates_collector_job() {
        let tasks = vec![make_task("a", vec![])];
        let (mut conn, execution_id, executor_node, executor_job_id, _) =
            setup_executor_expansion(tasks);
        let emitter = CapturingEmitter::new();
        let config_dir = std::path::Path::new("/nonexistent");

        expand_executor_node(
            &mut conn,
            &execution_id,
            &executor_node,
            &executor_job_id,
            config_dir,
            None,
            &emitter,
        )
        .unwrap();

        // Executor job should be complete (its role is done)
        let executor_job: DbJob = jobs::table.find(&executor_job_id).first(&mut conn).unwrap();
        assert_eq!(
            executor_job.status, "complete",
            "executor job should be complete after expansion"
        );
        assert!(
            executor_job.completed_at.is_some(),
            "executor job should have completed_at timestamp"
        );

        // Collector job should exist as pending with collector agent config
        let collector_id = format!("{}-collector", executor_node.id);
        let collector_job: DbJob = jobs::table
            .filter(jobs::execution_id.eq(&execution_id))
            .filter(jobs::recipe_node_id.eq(&collector_id))
            .first(&mut conn)
            .expect("collector job should exist");
        assert_eq!(
            collector_job.status, "pending",
            "collector job should be pending"
        );
        assert_eq!(
            collector_job.agent_config_id.as_deref(),
            Some("collector"),
            "collector job should have collector agent config"
        );
    }

    #[test]
    fn expand_executor_sets_parent_job_id_on_created_jobs() {
        let tasks = vec![make_task("a", vec![]), make_task("b", vec!["a"])];
        let (mut conn, execution_id, executor_node, executor_job_id, _) =
            setup_executor_expansion(tasks);
        let emitter = CapturingEmitter::new();
        let config_dir = std::path::Path::new("/nonexistent");

        expand_executor_node(
            &mut conn,
            &execution_id,
            &executor_node,
            &executor_job_id,
            config_dir,
            None,
            &emitter,
        )
        .unwrap();

        // All task jobs should have parent_job_id = executor_job_id
        let child_jobs: Vec<DbJob> = jobs::table
            .filter(jobs::parent_job_id.eq(Some(&executor_job_id)))
            .load(&mut conn)
            .unwrap();

        assert_eq!(
            child_jobs.len(),
            3,
            "2 task jobs + 1 collector job should have executor as parent"
        );

        // Verify they correspond to the tasks + collector
        let node_ids: HashSet<String> = child_jobs
            .iter()
            .filter_map(|j| j.recipe_node_id.clone())
            .collect();
        assert!(node_ids.contains("executor-1-task-a"));
        assert!(node_ids.contains("executor-1-task-b"));
        assert!(node_ids.contains("executor-1-collector"));
    }

    #[test]
    fn expand_executor_transfers_downstream_edges_to_collector() {
        let tasks = vec![make_task("a", vec![])];
        let (mut conn, execution_id, executor_node, executor_job_id, _) =
            setup_executor_expansion(tasks);
        let emitter = CapturingEmitter::new();
        let config_dir = std::path::Path::new("/nonexistent");

        expand_executor_node(
            &mut conn,
            &execution_id,
            &executor_node,
            &executor_job_id,
            config_dir,
            None,
            &emitter,
        )
        .unwrap();

        let snapshot_json: String = executions::table
            .find(&execution_id)
            .select(executions::snapshot)
            .first::<Option<String>>(&mut conn)
            .unwrap()
            .unwrap();
        let snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json).unwrap();

        let collector_id = format!("{}-collector", executor_node.id);

        // Downstream edge should now come from collector, not executor
        let collector_to_pr: Vec<_> = snapshot
            .recipe
            .edges
            .iter()
            .filter(|e| {
                e.source_node_id == collector_id
                    && e.target_node_id == "create-pr"
                    && e.edge_type == RecipeEdgeType::Control
            })
            .collect();
        assert_eq!(
            collector_to_pr.len(),
            1,
            "collector → create-pr edge should exist"
        );

        // Executor should no longer have the downstream edge
        let executor_to_pr: Vec<_> = snapshot
            .recipe
            .edges
            .iter()
            .filter(|e| {
                e.source_node_id == executor_node.id
                    && e.target_node_id == "create-pr"
                    && e.edge_type == RecipeEdgeType::Control
            })
            .collect();
        assert!(
            executor_to_pr.is_empty(),
            "executor should no longer have direct edge to create-pr"
        );
    }

    #[test]
    fn expand_executor_diamond_tasks_wires_both_leaves() {
        // Diamond: a → b, a → c, b+c → d
        // Leaf is d. Both b and c are non-leaves.
        let tasks = vec![
            make_task("a", vec![]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["a"]),
            make_task("d", vec!["b", "c"]),
        ];
        let (mut conn, execution_id, executor_node, executor_job_id, _) =
            setup_executor_expansion(tasks);
        let emitter = CapturingEmitter::new();
        let config_dir = std::path::Path::new("/nonexistent");

        expand_executor_node(
            &mut conn,
            &execution_id,
            &executor_node,
            &executor_job_id,
            config_dir,
            None,
            &emitter,
        )
        .unwrap();

        let snapshot_json: String = executions::table
            .find(&execution_id)
            .select(executions::snapshot)
            .first::<Option<String>>(&mut conn)
            .unwrap()
            .unwrap();
        let snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json).unwrap();

        let collector_id = format!("{}-collector", executor_node.id);

        // Only task "d" is a leaf — it should wire to collector
        let d_agent_id = format!("{}-task-d", executor_node.id);
        let d_control: Vec<_> = snapshot
            .recipe
            .edges
            .iter()
            .filter(|e| {
                e.source_node_id == d_agent_id
                    && e.target_node_id == collector_id
                    && e.edge_type == RecipeEdgeType::Control
            })
            .collect();
        assert_eq!(d_control.len(), 1, "diamond leaf d → collector control");

        // Tasks b, c should NOT connect to collector
        for non_leaf in &["b", "c"] {
            let id = format!("{}-task-{}", executor_node.id, non_leaf);
            let edges: Vec<_> = snapshot
                .recipe
                .edges
                .iter()
                .filter(|e| e.source_node_id == id && e.target_node_id == collector_id)
                .collect();
            assert!(
                edges.is_empty(),
                "non-leaf task '{}' should not wire to collector",
                non_leaf
            );
        }

        // 4 task jobs + 1 collector job created
        let child_jobs: Vec<DbJob> = jobs::table
            .filter(jobs::parent_job_id.eq(Some(&executor_job_id)))
            .load(&mut conn)
            .unwrap();
        assert_eq!(child_jobs.len(), 5);
    }

    #[test]
    fn expand_executor_cascades_issue_status() {
        // The execution has an associated issue. After executor expansion,
        // recompute_issue_status should run, keeping the issue "active"
        // (since the execution is still running with new child jobs).
        let tasks = vec![make_task("a", vec![])];
        let (mut conn, execution_id, executor_node, executor_job_id, _) =
            setup_executor_expansion(tasks);
        let emitter = CapturingEmitter::new();
        let config_dir = std::path::Path::new("/nonexistent");

        // Get the issue_id from the execution
        let issue_id: String = executions::table
            .find(&execution_id)
            .select(executions::issue_id)
            .first::<Option<String>>(&mut conn)
            .unwrap()
            .unwrap();

        // Issue starts as backlog (default from create_test_issue)
        let status_before: String = issues::table
            .find(&issue_id)
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status_before, "backlog");

        expand_executor_node(
            &mut conn,
            &execution_id,
            &executor_node,
            &executor_job_id,
            config_dir,
            None,
            &emitter,
        )
        .unwrap();

        // After expansion, the execution is still running (child jobs are pending).
        // recompute_issue_status should have updated the issue to "active".
        let status_after: String = issues::table
            .find(&issue_id)
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(
            status_after, "active",
            "issue should be active after executor expansion (execution still running)"
        );

        // completed_at should NOT be set (issue is active, not terminal)
        let completed_at: Option<i32> = issues::table
            .find(&issue_id)
            .select(issues::completed_at)
            .first(&mut conn)
            .unwrap();
        assert!(
            completed_at.is_none(),
            "active issue should not have completed_at"
        );
    }
}
