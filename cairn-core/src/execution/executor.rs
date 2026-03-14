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
use crate::diesel_models::{DbJob, DbRecipeNode, UpdateExecutionChangeset};
use crate::models::{
    AgentGitConfig, AgentNodeConfig, AgentSnapshot, ContextNodeConfig, ExecutionSnapshot, Model,
    NodePosition, RecipeEdge, RecipeEdgeType, RecipeNode, RecipeNodeType, WorktreeMode,
};
use crate::schema::{artifacts, executions, jobs};
use crate::services::EventEmitter;

use super::dag::create_jobs_for_new_nodes;

/// A single task from the TaskList artifact.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct TaskListTask {
    pub id: String,
    pub title: String,
    pub agent: String,
    pub prompt: String,
    #[serde(default)]
    pub dependencies: Vec<String>,
    pub model: Option<String>,
}

/// The TaskList artifact structure.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct TaskList {
    pub objective: String,
    #[serde(default)]
    pub requirements: Vec<String>,
    pub tasks: Vec<TaskListTask>,
}

/// Expand an executor node: read TaskList, inject nodes/edges, create jobs.
///
/// The executor_job_id is used as parent_job_id for all created task jobs,
/// so they render as children in the timeline. The executor job is marked
/// complete after expansion.
///
/// Returns the IDs of newly created agent jobs (for the caller to dispatch).
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

    // 4. Build node/edge IDs
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

    // 6. Find executor's downstream nodes in the parent recipe (e.g. create_pr node).
    //    Leaf tasks will be wired directly to these instead of an integrator.
    let downstream_targets: Vec<(String, String, RecipeEdgeType)> = snapshot
        .recipe
        .edges
        .iter()
        .filter(|e| e.source_node_id == executor_node.id)
        .map(|e| {
            (
                e.target_node_id.clone(),
                e.target_handle.clone(),
                e.edge_type.clone(),
            )
        })
        .collect();

    // 7. For each task: create ContextNode (prompt) + AgentNode
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

        // Resolve model override
        let model_override = task.model.as_ref().and_then(|m| m.parse::<Model>().ok());

        // Load agent config from files and populate snapshot
        match snapshot.agents.entry(task_agent_id.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                let agent_snapshot =
                    match config_agents::get_agent(config_dir, entry.key(), project_path) {
                        Ok(Some(fa)) => AgentSnapshot {
                            id: fa.id,
                            name: fa.name,
                            description: fa.description,
                            prompt: fa.prompt,
                            tools: fa.tools,
                            model: model_override.clone().or(fa.model).or(Some(Model::Sonnet)),
                            disallowed_tools: fa.disallowed_tools,
                            skills: fa.skills,
                            permission_mode: fa.permission_mode,
                        },
                        _ => {
                            log::warn!(
                                "Agent config '{}' not found in files, using placeholder",
                                entry.key()
                            );
                            AgentSnapshot {
                                id: entry.key().clone(),
                                name: entry.key().clone(),
                                description: String::new(),
                                prompt: String::new(),
                                tools: vec![],
                                model: model_override.clone().or(Some(Model::Sonnet)),
                                disallowed_tools: None,
                                skills: None,
                                permission_mode: None,
                            }
                        }
                    };
                entry.insert(agent_snapshot);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if let Some(model) = model_override {
                    entry.get_mut().model = Some(model);
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

    // 9. Wire leaf tasks to executor's downstream nodes in the parent recipe.
    //    This replaces the integrator — leaf tasks connect directly to whatever
    //    was downstream of the executor (e.g. create_pr action node).
    //    Deduplicate targets since executor may have multiple edge types to same node.
    let leaf_task_ids = find_leaf_tasks(&tasklist);
    let downstream_node_ids: HashSet<String> = downstream_targets
        .iter()
        .map(|(id, _, _)| id.clone())
        .filter(|id| *id != start_id && !new_agent_node_ids.contains(id))
        .collect();

    for target_id in &downstream_node_ids {
        for leaf_id in &leaf_task_ids {
            let agent_id = format!("{}-task-{}", exec_prefix, leaf_id);
            snapshot.recipe.edges.push(make_edge(
                &agent_id,
                "control-out",
                target_id,
                "control-in",
                RecipeEdgeType::Control,
            ));
            snapshot.recipe.edges.push(make_edge(
                &agent_id,
                "context-out",
                target_id,
                "context-in",
                RecipeEdgeType::Context,
            ));
        }

        // Also connect overview as context to downstream (for objective/requirements)
        snapshot.recipe.edges.push(make_edge(
            &start_id,
            "context-out",
            target_id,
            "context-in",
            RecipeEdgeType::Context,
        ));
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

    // 12. Copy upstream agent's worktree onto executor job so child tasks can inherit it.
    //     Find the upstream agent via control edges pointing to executor.
    let upstream_agent_job: Option<(String, Option<String>)> = snapshot
        .recipe
        .edges
        .iter()
        .filter(|e| e.target_node_id == executor_node.id && e.edge_type == RecipeEdgeType::Control)
        .find_map(|e| {
            // Find the job for this upstream node
            jobs::table
                .filter(jobs::execution_id.eq(execution_id))
                .filter(jobs::recipe_node_id.eq(&e.source_node_id))
                .select((jobs::worktree_path, jobs::branch))
                .first::<(Option<String>, Option<String>)>(conn)
                .ok()
                .and_then(|(wt, br)| wt.map(|w| (w, br)))
        });

    if let Some((worktree_path, branch)) = upstream_agent_job {
        diesel::update(jobs::table.find(executor_job_id))
            .set((
                jobs::worktree_path.eq(Some(&worktree_path)),
                jobs::branch.eq(branch.as_deref()),
            ))
            .execute(conn)
            .map_err(|e| format!("Failed to set executor worktree: {}", e))?;
        log::info!("Executor inherits worktree: {}", worktree_path);
    }

    // 13. Create jobs for new agent nodes
    let created_jobs =
        create_jobs_for_new_nodes(conn, execution_id, &new_agent_node_ids, &snapshot)?;

    // 14. Set parent_job_id on all created jobs so they render under the executor
    for job in &created_jobs {
        diesel::update(jobs::table.find(&job.id))
            .set(jobs::parent_job_id.eq(Some(executor_job_id)))
            .execute(conn)
            .map_err(|e| format!("Failed to set parent_job_id: {}", e))?;
    }

    // 15. Mark executor job as complete
    let now = chrono::Utc::now().timestamp() as i32;
    diesel::update(jobs::table.find(executor_job_id))
        .set((
            jobs::status.eq("complete"),
            jobs::completed_at.eq(Some(now)),
            jobs::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| format!("Failed to complete executor job: {}", e))?;

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

    fn make_task(id: &str, deps: Vec<&str>) -> TaskListTask {
        TaskListTask {
            id: id.to_string(),
            title: format!("Task {}", id),
            agent: "build".to_string(),
            prompt: format!("Do {}", id),
            dependencies: deps.into_iter().map(String::from).collect(),
            model: None,
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
}
