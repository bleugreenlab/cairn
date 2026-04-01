//! DAG helper functions for recipe execution.
//!
//! Graph traversal, node reachability analysis, and schema resolution for recipe nodes.
//! All recipe data is now loaded from execution snapshots rather than database tables.

use std::collections::{HashMap, HashSet, VecDeque};

use diesel::prelude::*;
use uuid::Uuid;

use crate::diesel_models::{
    DbActionRun, DbConditionEvaluation, DbJob, DbRecipeEdge, DbRecipeNode, NewJob,
};
use crate::models::{DelegatedSessionMode, ExecutionSnapshot, Job, RecipeEdge, RecipeNode};
use crate::schema::{action_runs, condition_evaluations, executions, jobs};

/// Convert RecipeNode (from snapshot) to DbRecipeNode (for job creation).
pub fn recipe_node_to_db(node: &RecipeNode, recipe_id: &str) -> DbRecipeNode {
    // Serialize the config parts back to JSON
    let config = {
        let mut config_map = serde_json::Map::new();
        if let Some(ref agent_cfg) = node.agent_config {
            if let Ok(serde_json::Value::Object(map)) = serde_json::to_value(agent_cfg) {
                config_map.extend(map);
            }
        }
        if let Some(ref action_cfg) = node.action_config {
            if let Ok(serde_json::Value::Object(map)) = serde_json::to_value(action_cfg) {
                config_map.extend(map);
            }
        }
        if let Some(ref checkpoint_cfg) = node.checkpoint_config {
            if let Ok(serde_json::Value::Object(map)) = serde_json::to_value(checkpoint_cfg) {
                config_map.extend(map);
            }
        }
        if let Some(ref condition_cfg) = node.condition_config {
            if let Ok(serde_json::Value::Object(map)) = serde_json::to_value(condition_cfg) {
                config_map.extend(map);
            }
        }
        if let Some(ref context_cfg) = node.context_config {
            if let Ok(serde_json::Value::Object(map)) = serde_json::to_value(context_cfg) {
                config_map.extend(map);
            }
        }
        if config_map.is_empty() {
            None
        } else {
            Some(serde_json::Value::Object(config_map).to_string())
        }
    };

    DbRecipeNode {
        id: node.id.clone(),
        recipe_id: recipe_id.to_string(),
        node_type: node.node_type.to_string(),
        name: node.name.clone(),
        position_x: node.position.x,
        position_y: node.position.y,
        config,
        created_at: 0, // Not used for job creation
        updated_at: 0, // Not used for job creation
        parent_id: node.parent_id.clone(),
    }
}

/// Convert RecipeEdge (from snapshot) to DbRecipeEdge (for job creation).
pub fn recipe_edge_to_db(edge: &RecipeEdge, recipe_id: &str) -> DbRecipeEdge {
    DbRecipeEdge {
        id: edge.id.clone(),
        recipe_id: recipe_id.to_string(),
        source_node_id: edge.source_node_id.clone(),
        target_node_id: edge.target_node_id.clone(),
        source_handle: edge.source_handle.clone(),
        target_handle: edge.target_handle.clone(),
        edge_type: edge.edge_type.to_string(),
        created_at: 0, // Not used for job creation
    }
}

/// Find all nodes reachable from trigger nodes via control edges.
/// Returns nodes in BFS order (distance from trigger), ensuring parents before children.
pub fn find_reachable_nodes(nodes: &[DbRecipeNode], edges: &[DbRecipeEdge]) -> Vec<String> {
    // Find all trigger nodes
    let triggers: Vec<_> = nodes
        .iter()
        .filter(|n| n.node_type == "trigger")
        .map(|n| n.id.clone())
        .collect();

    // Build adjacency list from control edges
    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
    for edge in edges {
        if edge.edge_type == "control" {
            adjacency
                .entry(edge.source_node_id.clone())
                .or_default()
                .push(edge.target_node_id.clone());
        }
    }

    // BFS from all triggers - preserve order for deterministic parent-before-child processing
    let mut reachable_ordered = Vec::new();
    let mut visited = HashSet::new();
    let mut queue: VecDeque<String> = triggers.into();

    while let Some(node_id) = queue.pop_front() {
        if visited.insert(node_id.clone()) {
            reachable_ordered.push(node_id.clone());
            if let Some(neighbors) = adjacency.get(&node_id) {
                queue.extend(neighbors.iter().cloned());
            }
        }
    }

    reachable_ordered
}

/// Create jobs from recipe nodes (DAG-based execution).
pub fn create_jobs_from_nodes(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    nodes: &[DbRecipeNode],
    edges: &[DbRecipeEdge],
    issue_id: &str,
    project_id: &str,
    snapshot: &ExecutionSnapshot,
) -> Result<Vec<Job>, String> {
    use crate::execution::step_behavior::resolve_node_behavior;

    // Find nodes reachable from triggers
    let reachable = find_reachable_nodes(nodes, edges);

    let node_map: HashMap<_, _> = nodes.iter().map(|n| (n.id.clone(), n)).collect();

    // Build reverse adjacency for control edges (target -> sources)
    // Used to find parent agent for inherit mode
    let mut reverse_control_edges: HashMap<String, Vec<String>> = HashMap::new();
    for edge in edges {
        if edge.edge_type == "control" {
            reverse_control_edges
                .entry(edge.target_node_id.clone())
                .or_default()
                .push(edge.source_node_id.clone());
        }
    }

    let now = chrono::Utc::now().timestamp() as i32;
    let mut created_jobs = Vec::new();

    // First pass: create jobs for all agent nodes
    // Track node_id -> job_id mapping for parent lookups
    let mut node_to_job: HashMap<String, String> = HashMap::new();

    for node_id in &reachable {
        let node = node_map.get(node_id).unwrap();

        match node.node_type.as_str() {
            // No jobs for these node types
            "trigger" => continue,  // Entry point, provides context
            "artifact" => continue, // Schema definition, stores output

            // Executor nodes get jobs so tasks render as children in the timeline.
            // No agent_config_id or worktree — expansion happens in advance_execution_with_actions.
            "executor" => {
                let job = insert_job_for_node(
                    conn,
                    execution_id,
                    issue_id,
                    project_id,
                    &node.id,
                    &node.name,
                    None, // no agent
                    None, // no parent
                    now,
                    snapshot,
                )?;

                node_to_job.insert(node_id.clone(), job.id.clone());
                created_jobs.push(job);
            }

            // Create jobs for executable nodes
            "agent" => {
                // Parse config to get agent_config_id
                let agent_config_id: Option<String> = node
                    .config
                    .as_ref()
                    .and_then(|c| serde_json::from_str::<serde_json::Value>(c).ok())
                    .and_then(|v| {
                        v.get("agentConfigId")
                            .and_then(|id| id.as_str())
                            .map(String::from)
                    });

                // Check if this node inherits worktree from parent
                let behavior = resolve_node_behavior(node);
                let parent_job_id = if behavior.inherits_worktree {
                    // Find upstream agent node via control edges
                    let parent = find_parent_agent_job_id(
                        node_id,
                        &reverse_control_edges,
                        &node_map,
                        &node_to_job,
                    );
                    if parent.is_none() {
                        log::warn!(
                            "Agent node '{}' has worktree_mode 'inherit' but no upstream agent in control flow",
                            node.name
                        );
                    }
                    parent
                } else {
                    None
                };

                let job = insert_job_for_node(
                    conn,
                    execution_id,
                    issue_id,
                    project_id,
                    &node.id,
                    &node.name,
                    agent_config_id.as_deref(),
                    parent_job_id.as_deref(),
                    now,
                    snapshot,
                )?;

                node_to_job.insert(node_id.clone(), job.id.clone());
                created_jobs.push(job);
            }

            // Action nodes don't create jobs - they create action_runs during DAG advancement
            "action" => continue,

            _ => {} // Unknown node type, skip
        }
    }

    Ok(created_jobs)
}

/// Create jobs for newly added nodes in an existing execution.
///
/// Used when the recipe is edited mid-execution to add new agent nodes.
/// Builds node_to_job mapping from existing jobs to support inherit mode.
pub fn create_jobs_for_new_nodes(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    new_node_ids: &HashSet<String>,
    snapshot: &ExecutionSnapshot,
) -> Result<Vec<Job>, String> {
    use crate::execution::step_behavior::resolve_node_behavior;

    // Get issue_id and project_id from trigger context
    let issue_id = snapshot.trigger_context.issue_id.as_deref().unwrap_or("");
    let project_id = &snapshot.trigger_context.project_id;

    // Convert snapshot to Db types for reachability analysis
    let recipe_id = &snapshot.recipe.id;
    let db_nodes: Vec<DbRecipeNode> = snapshot
        .recipe
        .nodes
        .iter()
        .map(|n| recipe_node_to_db(n, recipe_id))
        .collect();
    let db_edges: Vec<DbRecipeEdge> = snapshot
        .recipe
        .edges
        .iter()
        .map(|e| recipe_edge_to_db(e, recipe_id))
        .collect();

    let node_map: HashMap<_, _> = db_nodes.iter().map(|n| (n.id.clone(), n)).collect();

    // Build reverse adjacency for control edges (target -> sources)
    let mut reverse_control_edges: HashMap<String, Vec<String>> = HashMap::new();
    for edge in &db_edges {
        if edge.edge_type == "control" {
            reverse_control_edges
                .entry(edge.target_node_id.clone())
                .or_default()
                .push(edge.source_node_id.clone());
        }
    }

    // Build node_to_job mapping from existing jobs
    let existing_jobs: Vec<(Option<String>, String)> = jobs::table
        .filter(jobs::execution_id.eq(execution_id))
        .select((jobs::recipe_node_id, jobs::id))
        .load(conn)
        .map_err(|e| format!("Failed to load existing jobs: {}", e))?;

    let mut node_to_job: HashMap<String, String> = HashMap::new();
    for (node_id, job_id) in existing_jobs {
        if let Some(nid) = node_id {
            node_to_job.insert(nid, job_id);
        }
    }

    let now = chrono::Utc::now().timestamp() as i32;
    let mut created_jobs = Vec::new();

    // Find reachable nodes to ensure new nodes are reachable from triggers
    let reachable: HashSet<String> = find_reachable_nodes(&db_nodes, &db_edges)
        .into_iter()
        .collect();

    // Create jobs for new agent nodes that are reachable
    for node_id in new_node_ids {
        if !reachable.contains(node_id) {
            continue; // Skip unreachable nodes
        }

        let node = match node_map.get(node_id) {
            Some(n) => n,
            None => continue,
        };

        if node.node_type != "agent" && node.node_type != "executor" {
            continue; // Only create jobs for agent and executor nodes
        }

        // Parse config to get agent_config_id
        let agent_config_id: Option<String> = node
            .config
            .as_ref()
            .and_then(|c| serde_json::from_str::<serde_json::Value>(c).ok())
            .and_then(|v| {
                v.get("agentConfigId")
                    .and_then(|id| id.as_str())
                    .map(String::from)
            });

        // Check if this node inherits worktree from parent
        let behavior = resolve_node_behavior(node);
        let parent_job_id = if behavior.inherits_worktree {
            find_parent_agent_job_id(node_id, &reverse_control_edges, &node_map, &node_to_job)
        } else {
            None
        };

        let job = insert_job_for_node(
            conn,
            execution_id,
            issue_id,
            project_id,
            &node.id,
            &node.name,
            agent_config_id.as_deref(),
            parent_job_id.as_deref(),
            now,
            snapshot,
        )?;

        node_to_job.insert(node_id.clone(), job.id.clone());
        created_jobs.push(job);
    }

    Ok(created_jobs)
}

/// Find the parent agent's job ID for an inherit-mode agent node.
/// Traverses backwards via control edges to find the first agent or executor node.
/// Executor nodes are included because they own worktrees that downstream
/// inherit-mode nodes need to inherit.
fn find_parent_agent_job_id(
    node_id: &str,
    reverse_edges: &HashMap<String, Vec<String>>,
    node_map: &HashMap<String, &DbRecipeNode>,
    node_to_job: &HashMap<String, String>,
) -> Option<String> {
    let mut visited = HashSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();

    // Start with immediate predecessors
    if let Some(parents) = reverse_edges.get(node_id) {
        queue.extend(parents.iter().cloned());
    }

    while let Some(parent_id) = queue.pop_front() {
        if !visited.insert(parent_id.clone()) {
            continue;
        }

        if let Some(parent_node) = node_map.get(&parent_id) {
            if parent_node.node_type == "agent" || parent_node.node_type == "executor" {
                // Found an agent or executor - return its job ID if already created
                return node_to_job.get(&parent_id).cloned();
            }
        }

        // Continue searching backwards
        if let Some(grandparents) = reverse_edges.get(&parent_id) {
            queue.extend(grandparents.iter().cloned());
        }
    }

    None
}

#[allow(clippy::too_many_arguments)]
fn insert_job_for_node(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    issue_id: &str,
    project_id: &str,
    node_id: &str,
    name: &str,
    agent_config_id: Option<&str>,
    parent_job_id: Option<&str>,
    now: i32,
    snapshot: &ExecutionSnapshot,
) -> Result<Job, String> {
    let job_id = Uuid::new_v4().to_string();

    // Load resolved model from snapshot for this agent
    let model_str = agent_config_id
        .and_then(|id| snapshot.agents.get(id))
        .and_then(|a| a.model.as_ref())
        .map(|m| m.to_string());

    let delegated_packet = snapshot.delegated_packets.iter().find(|packet| {
        packet
            .materialized_node_ids
            .iter()
            .any(|materialized_node_id| materialized_node_id == node_id)
    });
    let requested_forked_session = delegated_packet
        .map(|packet| packet.session.mode == DelegatedSessionMode::Fork)
        .unwrap_or(false);
    let parent_backend = delegated_packet
        .and_then(|packet| {
            jobs::table
                .find(&packet.parent_job_id)
                .select(jobs::model)
                .first::<Option<String>>(conn)
                .ok()
                .flatten()
                .as_deref()
                .and_then(crate::backends::backend_for_model)
                .map(str::to_string)
        });
    let child_backend = backend_for_job_session(snapshot, agent_config_id);
    let create_forked_session = requested_forked_session
        && parent_backend
            .as_deref()
            .map(|backend| backend == child_backend)
            .unwrap_or(false);
    let session_id = (!create_forked_session).then(|| Uuid::new_v4().to_string());
    let new_job = NewJob {
        id: &job_id,
        execution_id: Some(execution_id),
        manager_id: None,
        recipe_node_id: Some(node_id),
        parent_job_id,
        worktree_path: None,
        branch: None,
        base_commit: None,
        current_session_id: session_id.as_deref(),
        resume_session_id: None,
        status: "pending",
        agent_config_id,
        issue_id: if issue_id.is_empty() {
            None
        } else {
            Some(issue_id)
        },
        project_id,
        task_description: Some(name),
        created_at: now,
        updated_at: now,
        completed_at: None,
        parent_tool_use_id: None,
        task_index: None,
        started_at: None,
        model: model_str.as_deref(),
        node_name: Some(name),
        base_branch: None,
        current_turn_id: None,
    };

    diesel::insert_into(jobs::table)
        .values(&new_job)
        .execute(conn)
        .map_err(|e| format!("Failed to create job: {}", e))?;

    // Create Session record (after Job exists to satisfy FK).
    // Must succeed — Session is the durable conversation identity.
    if create_forked_session {
        let packet = delegated_packet.expect("delegated fork packet must exist");
        let source_session_id: Option<String> = jobs::table
            .find(&packet.parent_job_id)
            .select(jobs::current_session_id)
            .first(conn)
            .map_err(|e| format!("Failed to load parent session for delegated fork: {}", e))?;
        if let Some(source_session_id) = source_session_id {
            let source_session = crate::sessions::queries::get(conn, &source_session_id)?;
            crate::sessions::queries::fork_job_session(conn, &source_session, &job_id, true)?;
        } else {
            let session_id = Uuid::new_v4().to_string();
            crate::sessions::queries::create_with_id(conn, &session_id, Some(&job_id), None, &child_backend)?;
            diesel::update(jobs::table.find(&job_id))
                .set(jobs::current_session_id.eq(Some(&session_id)))
                .execute(conn)
                .map_err(|e| format!("Failed to attach fallback session to delegated job: {}", e))?;
        }
    } else {
        crate::sessions::queries::create_with_id(
            conn,
            session_id
                .as_deref()
                .ok_or_else(|| "Missing generated session id for new job".to_string())?,
            Some(&job_id),
            None,
            &child_backend,
        )?;
    }

    let db_job: DbJob = jobs::table
        .find(&job_id)
        .first(conn)
        .map_err(|e| format!("Failed to load created job: {}", e))?;

    Job::try_from(db_job)
}

fn backend_for_job_session(snapshot: &ExecutionSnapshot, agent_config_id: Option<&str>) -> String {
    agent_config_id
        .and_then(|id| snapshot.agents.get(id))
        .and_then(|agent| {
            agent
                .resolved_backend
                .clone()
                .or_else(|| agent.model.as_ref().and_then(|model| crate::backends::backend_for_model(model.as_str()).map(str::to_string)))
                .or_else(|| agent.backend_preference.clone())
        })
        .unwrap_or_else(|| "claude".to_string())
}

/// Load execution snapshot from database
fn load_execution_snapshot(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
) -> Result<ExecutionSnapshot, String> {
    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(conn)
        .map_err(|e| format!("Failed to load execution: {}", e))?;

    let snapshot_json = snapshot_json.ok_or_else(|| "Execution has no snapshot".to_string())?;

    serde_json::from_str(&snapshot_json)
        .map_err(|e| format!("Failed to parse execution snapshot: {}", e))
}

/// Load nodes from an execution's snapshot (for file-based recipes).
pub fn load_nodes_from_execution(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
) -> Result<Vec<DbRecipeNode>, String> {
    let snapshot = load_execution_snapshot(conn, execution_id)?;
    let recipe_id = &snapshot.recipe.id;
    Ok(snapshot
        .recipe
        .nodes
        .iter()
        .map(|n| recipe_node_to_db(n, recipe_id))
        .collect())
}

/// Load edges from an execution's snapshot (for file-based recipes).
pub fn load_edges_from_execution(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
) -> Result<Vec<DbRecipeEdge>, String> {
    let snapshot = load_execution_snapshot(conn, execution_id)?;
    let recipe_id = &snapshot.recipe.id;
    Ok(snapshot
        .recipe
        .edges
        .iter()
        .map(|e| recipe_edge_to_db(e, recipe_id))
        .collect())
}

/// Find output schema for an agent node from the execution snapshot.
///
/// Sources are checked in priority order:
/// 1. **Source node's own outputSchema** - AgentNode config may have a direct outputSchema slot
/// 2. **Downstream ActionNode's inputSchema** - when connected via context edge
/// 3. **Downstream ArtifactNode's schema** - when connected via context edge
///
/// Returns None if no valid schema is found from any source.
pub fn find_downstream_artifact_schema(
    conn: &mut diesel::sqlite::SqliteConnection,
    node_id: &str,
    execution_id: &str,
) -> Result<Option<crate::models::OutputSchemaInfo>, String> {
    // Load snapshot
    let snapshot = load_execution_snapshot(conn, execution_id)?;

    // Build lookup maps for nodes and edges
    let node_map: HashMap<&str, &RecipeNode> = snapshot
        .recipe
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), n))
        .collect();
    let context_edges: Vec<&RecipeEdge> = snapshot
        .recipe
        .edges
        .iter()
        .filter(|e| e.edge_type.to_string() == "context" && e.source_node_id == node_id)
        .collect();

    // PRIORITY 1: Check if source node has its own outputSchema
    if let Some(node) = node_map.get(node_id) {
        if node.node_type.to_string() == "agent" {
            if let Some(ref agent_cfg) = node.agent_config {
                if let Some(ref output_schema) = agent_cfg.output_schema {
                    if let Some(schema_info) = extract_schema_from_slot_config(output_schema)? {
                        return Ok(Some(schema_info));
                    }
                }
            }
        }
    }

    log::info!(
        "find_downstream_artifact_schema: node_id={}, execution_id={}, found {} context edges",
        node_id,
        execution_id,
        context_edges.len()
    );

    // PRIORITY 2 & 3: Check downstream nodes via context edges
    for edge in context_edges {
        log::info!(
            "  Edge: {} -> {} (handle: {})",
            edge.source_node_id,
            edge.target_node_id,
            edge.source_handle
        );

        if let Some(target_node) = node_map.get(edge.target_node_id.as_str()) {
            log::info!(
                "  Target node: id={}, type={}",
                target_node.id,
                target_node.node_type
            );

            // PRIORITY 2: Action node with inputSchema
            if target_node.node_type.to_string() == "action" {
                if let Some(ref action_cfg) = target_node.action_config {
                    // Check inputSchema on the action node config
                    if let Some(ref input_schema) = action_cfg.input_schema {
                        log::info!(
                            "  Found action inputSchema: schemaType={:?}",
                            input_schema.schema_type
                        );
                        if let Some(schema_info) = extract_schema_from_slot_config(input_schema)? {
                            return Ok(Some(schema_info));
                        }
                    }

                    // If no inline schema, look up ActionConfig from database
                    let action_config_id = action_cfg.action_config_id.clone().or_else(|| {
                        // Fall back to legacy action field for builtins
                        if !action_cfg.action.is_empty() {
                            Some(format!("builtin:{}", action_cfg.action))
                        } else {
                            None
                        }
                    });

                    if let Some(ref ac_id) = action_config_id {
                        log::info!("  Looking up ActionConfig: {}", ac_id);
                        let action_config: Option<crate::diesel_models::DbActionConfig> =
                            crate::schema::action_configs::table
                                .find(ac_id)
                                .first(conn)
                                .ok();

                        if let Some(ac) = action_config {
                            if let Some(ref schema_str) = ac.input_schema {
                                if let Ok(schema_json) =
                                    serde_json::from_str::<serde_json::Value>(schema_str)
                                {
                                    log::info!(
                                        "  Found ActionConfig input_schema with {} properties, tool_name={:?}",
                                        schema_json
                                            .get("properties")
                                            .and_then(|p| p.as_object())
                                            .map(|o| o.len())
                                            .unwrap_or(0),
                                        ac.tool_name
                                    );
                                    return Ok(Some(crate::models::OutputSchemaInfo {
                                        schema: crate::models::OutputSchema::Custom(schema_json),
                                        tool_name: ac.tool_name.clone(),
                                        description: ac.tool_description.clone(),
                                    }));
                                }
                            }
                        }
                    }
                }
            }

            // PRIORITY 3: Artifact node (existing logic)
            if target_node.node_type.to_string() == "artifact" {
                // Artifact nodes store schema info in the node config
                // Extract artifactType from the config JSON
                if let Some(ref agent_cfg) = target_node.agent_config {
                    // Agent config might have artifact info
                    if let Some(ref output_schema) = agent_cfg.output_schema {
                        if let Some(schema_info) = extract_schema_from_slot_config(output_schema)? {
                            return Ok(Some(schema_info));
                        }
                    }
                }
                // Try to extract from raw node data if available
                // Artifact nodes may have schema in a different format
            }
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: &str, node_type: &str) -> DbRecipeNode {
        DbRecipeNode {
            id: id.to_string(),
            recipe_id: "r".to_string(),
            node_type: node_type.to_string(),
            name: id.to_string(),
            position_x: 0.0,
            position_y: 0.0,
            config: None,
            created_at: 0,
            updated_at: 0,
            parent_id: None,
        }
    }

    /// Build reverse control edges: target → [sources]
    fn reverse_edges(edges: &[(&str, &str)]) -> HashMap<String, Vec<String>> {
        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        for (src, tgt) in edges {
            map.entry(tgt.to_string())
                .or_default()
                .push(src.to_string());
        }
        map
    }

    // ========================================================================
    // find_parent_agent_job_id tests
    // ========================================================================

    #[test]
    fn find_parent_matches_agent_node() {
        // agent-a → task-b (control). task-b should find agent-a.
        let nodes = vec![make_node("agent-a", "agent"), make_node("task-b", "agent")];
        let node_map: HashMap<String, &DbRecipeNode> =
            nodes.iter().map(|n| (n.id.clone(), n)).collect();
        let rev = reverse_edges(&[("agent-a", "task-b")]);
        let jobs: HashMap<String, String> = [("agent-a".to_string(), "job-a".to_string())].into();

        let result = find_parent_agent_job_id("task-b", &rev, &node_map, &jobs);
        assert_eq!(result, Some("job-a".to_string()));
    }

    #[test]
    fn find_parent_matches_executor_node() {
        // executor → root-task (control). root-task should find executor.
        let nodes = vec![
            make_node("executor-1", "executor"),
            make_node("root-task", "agent"),
        ];
        let node_map: HashMap<String, &DbRecipeNode> =
            nodes.iter().map(|n| (n.id.clone(), n)).collect();
        let rev = reverse_edges(&[("executor-1", "root-task")]);
        let jobs: HashMap<String, String> =
            [("executor-1".to_string(), "exec-job".to_string())].into();

        let result = find_parent_agent_job_id("root-task", &rev, &node_map, &jobs);
        assert_eq!(result, Some("exec-job".to_string()));
    }

    #[test]
    fn find_parent_traverses_through_non_agent_nodes() {
        // executor → context-node → task (control chain).
        // task should traverse past context-node and find executor.
        let nodes = vec![
            make_node("executor-1", "executor"),
            make_node("ctx", "context"),
            make_node("task-a", "agent"),
        ];
        let node_map: HashMap<String, &DbRecipeNode> =
            nodes.iter().map(|n| (n.id.clone(), n)).collect();
        let rev = reverse_edges(&[("executor-1", "ctx"), ("ctx", "task-a")]);
        let jobs: HashMap<String, String> =
            [("executor-1".to_string(), "exec-job".to_string())].into();

        let result = find_parent_agent_job_id("task-a", &rev, &node_map, &jobs);
        assert_eq!(result, Some("exec-job".to_string()));
    }

    #[test]
    fn find_parent_returns_none_when_no_agent_or_executor_upstream() {
        // trigger → task (control). Trigger is neither agent nor executor.
        let nodes = vec![
            make_node("trigger-1", "trigger"),
            make_node("task-a", "agent"),
        ];
        let node_map: HashMap<String, &DbRecipeNode> =
            nodes.iter().map(|n| (n.id.clone(), n)).collect();
        let rev = reverse_edges(&[("trigger-1", "task-a")]);
        let jobs: HashMap<String, String> =
            [("trigger-1".to_string(), "trig-job".to_string())].into();

        let result = find_parent_agent_job_id("task-a", &rev, &node_map, &jobs);
        assert_eq!(result, None);
    }

    #[test]
    fn find_parent_returns_none_when_agent_has_no_job_yet() {
        // agent-a → task-b, but agent-a has no job in node_to_job.
        let nodes = vec![make_node("agent-a", "agent"), make_node("task-b", "agent")];
        let node_map: HashMap<String, &DbRecipeNode> =
            nodes.iter().map(|n| (n.id.clone(), n)).collect();
        let rev = reverse_edges(&[("agent-a", "task-b")]);
        let jobs: HashMap<String, String> = HashMap::new(); // no jobs

        let result = find_parent_agent_job_id("task-b", &rev, &node_map, &jobs);
        assert_eq!(result, None);
    }

    #[test]
    fn find_parent_stops_at_first_agent_does_not_continue() {
        // executor → agent-a → task-b. task-b should find agent-a, not executor.
        let nodes = vec![
            make_node("executor-1", "executor"),
            make_node("agent-a", "agent"),
            make_node("task-b", "agent"),
        ];
        let node_map: HashMap<String, &DbRecipeNode> =
            nodes.iter().map(|n| (n.id.clone(), n)).collect();
        let rev = reverse_edges(&[("executor-1", "agent-a"), ("agent-a", "task-b")]);
        let jobs: HashMap<String, String> = [
            ("executor-1".to_string(), "exec-job".to_string()),
            ("agent-a".to_string(), "job-a".to_string()),
        ]
        .into();

        let result = find_parent_agent_job_id("task-b", &rev, &node_map, &jobs);
        assert_eq!(result, Some("job-a".to_string()));
    }

    #[test]
    fn create_jobs_for_new_nodes_forks_delegated_session_from_parent() {
        use crate::diesel_models::{NewExecution, NewJob};
        use crate::models::{
            DelegatedOutputContract, DelegatedOwnershipScope, DelegatedSessionMode,
            DelegatedSessionStrategy, DelegatedStatus, DelegatedWorkPacket, ExecutionSnapshot,
            RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
        };
        use crate::schema::{executions, jobs, sessions};
        use crate::test_utils::{create_test_project, test_diesel_conn};

        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let parent_job_id = Uuid::new_v4().to_string();
        let parent_session_id = Uuid::new_v4().to_string();
        let execution_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;

        diesel::insert_into(jobs::table)
            .values(NewJob {
                id: &parent_job_id,
                execution_id: None,
                manager_id: None,
                recipe_node_id: None,
                parent_job_id: None,
                worktree_path: None,
                branch: None,
                base_commit: None,
                current_session_id: Some(&parent_session_id),
                resume_session_id: None,
                status: "running",
                agent_config_id: Some("Explore"),
                issue_id: None,
                project_id: &project_id,
                task_description: Some("Parent"),
                created_at: now,
                updated_at: now,
                completed_at: None,
                parent_tool_use_id: None,
                task_index: None,
                started_at: Some(now),
                model: Some("gpt-5"),
                node_name: Some("Parent"),
                base_branch: None,
                current_turn_id: None,
            })
            .execute(&mut conn)
            .unwrap();
        crate::sessions::queries::create_with_id(
            &mut conn,
            &parent_session_id,
            Some(&parent_job_id),
            None,
            "codex",
        )
        .unwrap();

        let snapshot = ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Delegation".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![RecipeNode {
                    id: "delegated-packet-1-agent".to_string(),
                    node_type: crate::models::RecipeNodeType::Agent,
                    name: "Explore".to_string(),
                    position: crate::models::NodePosition { x: 0.0, y: 0.0 },
                    parent_id: None,
                    trigger_config: None,
                    agent_config: Some(crate::models::AgentNodeConfig {
                        agent_config_id: Some("Explore".to_string()),
                        checkpoint: None,
                        output_schema: None,
                        git_config: None,
                    }),
                    action_config: None,
                    checkpoint_config: None,
                    artifact_config: None,
                    condition_config: None,
                    context_config: None,
                }],
                edges: vec![],
            },
            agents: HashMap::from([(
                "Explore".to_string(),
                crate::models::AgentSnapshot {
                    id: "Explore".to_string(),
                    name: "Explore".to_string(),
                    description: String::new(),
                    prompt: String::new(),
                    tools: vec![],
                    tier: None,
                    backend_preference: Some("codex".to_string()),
                    model: Some(crate::models::Model::default()),
                    disallowed_tools: None,
                    skills: None,
                    approval_policy: None,
                    filesystem_scope: None,
                    resolved_backend: Some("codex".to_string()),
                    extras: None,
                },
            )]),
            skills: HashMap::new(),
            tools: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: None,
                project_id: project_id.clone(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
            },
            presets: None,
            delegated_packets: vec![DelegatedWorkPacket {
                id: "packet-1".to_string(),
                parent_job_id: parent_job_id.clone(),
                parent_turn_id: None,
                parent_tool_use_id: Some("tool-1".to_string()),
                origin: crate::models::DelegationOrigin::TaskTool,
                title: "Explore".to_string(),
                problem_statement: "Inspect".to_string(),
                agent_config_id: "Explore".to_string(),
                ownership: DelegatedOwnershipScope {
                    cwd: "/tmp/test".to_string(),
                    filesystem_scope: None,
                    approval_policy: None,
                },
                session: DelegatedSessionStrategy {
                    mode: DelegatedSessionMode::Fork,
                },
                acceptance: vec![],
                output_contract: DelegatedOutputContract {
                    schema_type: "return".to_string(),
                    tool_name: None,
                    description: None,
                },
                status: DelegatedStatus::Materialized,
                materialized_node_ids: vec!["delegated-packet-1-agent".to_string()],
                result_artifact_job_id: None,
                task_index: Some(0),
                tier_override: None,
                backend_preference: Some("codex".to_string()),
                created_at: now as i64,
            }],
            created_at: now as i64,
        };
        let snapshot_json = serde_json::to_string(&snapshot).unwrap();
        diesel::insert_into(executions::table)
            .values(NewExecution {
                id: &execution_id,
                recipe_id: "recipe-1",
                issue_id: None,
                project_id: Some(&project_id),
                status: "running",
                started_at: now,
                completed_at: None,
                snapshot: Some(&snapshot_json),
                seq: None,
                initiator_sub: None,
                initiator_auth_mode: None,
                initiator_org_id: None,
                triggered_by: "manual",
            })
            .execute(&mut conn)
            .unwrap();

        let child_job = insert_job_for_node(
            &mut conn,
            &execution_id,
            "",
            &project_id,
            "delegated-packet-1-agent",
            "Explore",
            Some("Explore"),
            None,
            now,
            &snapshot,
        )
        .unwrap();
        let child_session_id: Option<String> = jobs::table
            .find(&child_job.id)
            .select(jobs::current_session_id)
            .first(&mut conn)
            .unwrap();
        let child_session_id = child_session_id.unwrap();
        let lineage: (Option<String>, Option<String>) = sessions::table
            .find(&child_session_id)
            .select((sessions::parent_session_id, sessions::replaced_by_id))
            .first(&mut conn)
            .unwrap();

        assert_eq!(lineage.0.as_deref(), Some(parent_session_id.as_str()));
        assert!(lineage.1.is_none());
    }

    #[test]
    fn find_parent_returns_none_when_no_edges() {
        // Isolated node with no incoming control edges.
        let nodes = vec![make_node("task-a", "agent")];
        let node_map: HashMap<String, &DbRecipeNode> =
            nodes.iter().map(|n| (n.id.clone(), n)).collect();
        let rev: HashMap<String, Vec<String>> = HashMap::new();
        let jobs: HashMap<String, String> = HashMap::new();

        let result = find_parent_agent_job_id("task-a", &rev, &node_map, &jobs);
        assert_eq!(result, None);
    }
}

// ============================================================================
// DAG readiness checks — shared by advancement.rs and conditions.rs
// ============================================================================

/// Check if an edge from a condition node is satisfied (matches the evaluated port).
pub fn is_condition_edge_satisfied_dag(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    edge: &DbRecipeEdge,
) -> Result<bool, String> {
    let evaluation: Option<DbConditionEvaluation> = condition_evaluations::table
        .filter(condition_evaluations::execution_id.eq(execution_id))
        .filter(condition_evaluations::recipe_node_id.eq(&edge.source_node_id))
        .first(conn)
        .optional()
        .map_err(|e| format!("Failed to check condition evaluation: {}", e))?;

    match evaluation {
        Some(eval) => Ok(edge.source_handle == eval.result_port),
        None => Ok(false),
    }
}

/// Check if an action node's dependencies are satisfied using pre-loaded snapshot data.
pub fn is_action_node_ready_with_snapshot(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    node_id: &str,
    all_edges: &[DbRecipeEdge],
    node_map: &std::collections::HashMap<&str, &DbRecipeNode>,
) -> Result<bool, String> {
    let incoming_edges: Vec<&DbRecipeEdge> = all_edges
        .iter()
        .filter(|e| e.target_node_id == node_id && e.edge_type == "control")
        .collect();

    if incoming_edges.is_empty() {
        let trigger_edge_count = all_edges
            .iter()
            .filter(|e| e.target_node_id == node_id)
            .filter(|e| {
                node_map
                    .get(e.source_node_id.as_str())
                    .map(|n| n.node_type == "trigger")
                    .unwrap_or(false)
            })
            .count();
        return Ok(trigger_edge_count > 0);
    }

    for edge in incoming_edges {
        if edge.target_handle == "continue" {
            continue;
        }

        let source_node_type = node_map
            .get(edge.source_node_id.as_str())
            .map(|n| n.node_type.as_str());

        match source_node_type {
            Some("trigger") => continue,
            Some("action") => {
                let action_run_complete: bool = action_runs::table
                    .filter(action_runs::execution_id.eq(execution_id))
                    .filter(action_runs::recipe_node_id.eq(&edge.source_node_id))
                    .filter(action_runs::status.eq("complete"))
                    .first::<DbActionRun>(conn)
                    .is_ok();
                if !action_run_complete {
                    return Ok(false);
                }
            }
            Some("condition") => {
                if !is_condition_edge_satisfied_dag(conn, execution_id, edge)? {
                    return Ok(false);
                }
            }
            _ => {
                let source_job: Result<DbJob, _> = jobs::table
                    .filter(jobs::execution_id.eq(execution_id))
                    .filter(jobs::recipe_node_id.eq(&edge.source_node_id))
                    .first(conn);
                match source_job {
                    Ok(j) if j.status == "complete" => continue,
                    _ => return Ok(false),
                }
            }
        }
    }

    Ok(true)
}

/// Check if an action/condition node's dependencies are satisfied.
/// Loads the snapshot from the execution to check dependencies.
pub fn is_action_node_ready(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    node_id: &str,
    _recipe_id: &str, // Kept for API compatibility
) -> Result<bool, String> {
    let all_nodes = load_nodes_from_execution(conn, execution_id)?;
    let all_edges = load_edges_from_execution(conn, execution_id)?;
    let node_map: std::collections::HashMap<&str, &DbRecipeNode> =
        all_nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    is_action_node_ready_with_snapshot(conn, execution_id, node_id, &all_edges, &node_map)
}

/// Helper to extract schema from a SchemaConfig slot (outputSchema or inputSchema).
fn extract_schema_from_slot_config(
    schema_config: &crate::models::SchemaConfig,
) -> Result<Option<crate::models::OutputSchemaInfo>, String> {
    let schema_type = schema_config.schema_type.as_str();

    // Extract custom tool name and description if provided
    let tool_name = schema_config.tool_name.clone();
    let description = schema_config.description.clone();

    // Check for preset schema type (plan, implementation, document, summary)
    if !schema_type.is_empty() && schema_type != "custom" {
        return Ok(Some(crate::models::OutputSchemaInfo {
            schema: crate::models::OutputSchema::Preset(schema_type.to_string()),
            tool_name,
            description,
        }));
    }

    // Custom schema with fields - convert to JSON
    if schema_config.fields.is_some() {
        let schema_json = serde_json::to_value(schema_config)
            .map_err(|e| format!("Failed to serialize schema config: {}", e))?;
        return Ok(Some(crate::models::OutputSchemaInfo {
            schema: crate::models::OutputSchema::Custom(schema_json),
            tool_name,
            description,
        }));
    }

    Ok(None)
}
