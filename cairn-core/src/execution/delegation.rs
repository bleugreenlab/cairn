//! DAG-native delegated work packet expansion.
//!
//! The task MCP tool records a durable packet in the execution snapshot. This
//! module materializes pending packets into ordinary DAG nodes/jobs so the rest
//! of execution, status, and completion flows stay on the normal path.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use diesel::prelude::*;
use uuid::Uuid;

use crate::config::agents as config_agents;
use crate::config::presets::{
    load_effective_presets, resolve_agent_snapshot, resolve_runtime_selection, PresetsConfig,
};
use crate::diesel_models::UpdateExecutionChangeset;
use crate::models::{
    AgentGitConfig, AgentNodeConfig, AgentSnapshot, ContextNodeConfig,
    DelegatedOutputContract, DelegatedSessionStrategy, DelegatedStatus, DelegatedWorkPacket,
    ExecutionSnapshot, Model, NodePosition, RecipeEdge, RecipeEdgeType, RecipeNode,
    RecipeNodeType, SchemaConfig, WorktreeMode,
};
use crate::schema::{executions, jobs, projects, runs};
use crate::services::EventEmitter;

use super::dag::create_jobs_for_new_nodes;

pub struct CreateDelegatedPacketInput<'a> {
    pub parent_job_id: &'a str,
    pub parent_turn_id: Option<&'a str>,
    pub parent_tool_use_id: Option<&'a str>,
    pub title: &'a str,
    pub problem_statement: &'a str,
    pub agent_config_id: &'a str,
    pub cwd: &'a str,
    pub filesystem_scope: Option<crate::models::FilesystemScope>,
    pub approval_policy: Option<crate::models::ApprovalPolicy>,
    pub acceptance: Vec<String>,
    pub output_contract: DelegatedOutputContract,
    pub session: DelegatedSessionStrategy,
    pub task_index: Option<i32>,
    pub tier_override: Option<&'a str>,
    #[allow(dead_code)]
    pub backend_preference: Option<&'a str>,
}

pub fn create_or_reuse_task_packet(
    snapshot: &mut ExecutionSnapshot,
    input: CreateDelegatedPacketInput<'_>,
) -> DelegatedWorkPacket {
    if let Some(existing) = snapshot
        .delegated_packets
        .iter()
        .find(|packet| {
            packet.parent_job_id == input.parent_job_id
                && packet.parent_tool_use_id.as_deref() == input.parent_tool_use_id
                && packet.agent_config_id == input.agent_config_id
                && packet.title == input.title
                && packet.problem_statement == input.problem_statement
                && packet.task_index == input.task_index
                && packet.tier_override.as_deref() == input.tier_override
                && packet.backend_preference.as_deref() == input.backend_preference
                && packet.session == input.session
        })
        .cloned()
    {
        return existing;
    }

    let packet = DelegatedWorkPacket {
        id: Uuid::new_v4().to_string(),
        parent_job_id: input.parent_job_id.to_string(),
        parent_turn_id: input.parent_turn_id.map(str::to_string),
        parent_tool_use_id: input.parent_tool_use_id.map(str::to_string),
        origin: crate::models::DelegationOrigin::TaskTool,
        title: input.title.to_string(),
        problem_statement: input.problem_statement.to_string(),
        agent_config_id: input.agent_config_id.to_string(),
        ownership: crate::models::DelegatedOwnershipScope {
            cwd: input.cwd.to_string(),
            filesystem_scope: input.filesystem_scope,
            approval_policy: input.approval_policy,
        },
        session: input.session,
        acceptance: input.acceptance,
        output_contract: input.output_contract,
        status: DelegatedStatus::Pending,
        materialized_node_ids: vec![],
        result_artifact_job_id: None,
        task_index: input.task_index,
        tier_override: input.tier_override.map(str::to_string),
        backend_preference: input.backend_preference.map(str::to_string),
        created_at: chrono::Utc::now().timestamp(),
    };

    snapshot.delegated_packets.push(packet.clone());
    packet
}

pub fn expand_delegated_packets(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    config_dir: &Path,
    emitter: &dyn EventEmitter,
) -> Result<HashSet<String>, String> {
    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(conn)
        .map_err(|e| format!("Failed to load execution: {}", e))?;
    let snapshot_json = snapshot_json.ok_or("Execution has no snapshot")?;
    let mut snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json)
        .map_err(|e| format!("Failed to parse snapshot: {}", e))?;

    let project_path: Option<PathBuf> = projects::table
        .find(&snapshot.trigger_context.project_id)
        .select(projects::repo_path)
        .first::<String>(conn)
        .ok()
        .map(PathBuf::from);

    let pending_packet_ids: Vec<String> = snapshot
        .delegated_packets
        .iter()
        .filter(|packet| packet.status == DelegatedStatus::Pending)
        .map(|packet| packet.id.clone())
        .collect();

    if pending_packet_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let presets = snapshot
        .presets
        .as_ref()
        .map(PresetsConfig::from)
        .unwrap_or_else(|| load_effective_presets(config_dir, project_path.as_deref()));
    let mut new_agent_node_ids = HashSet::new();

    for packet_id in pending_packet_ids {
        let packet_index = snapshot
            .delegated_packets
            .iter()
            .position(|packet| packet.id == packet_id)
            .ok_or_else(|| format!("Delegated packet missing from snapshot: {}", packet_id))?;
        let packet_view = snapshot.delegated_packets[packet_index].clone();

        ensure_agent_snapshot(
            &mut snapshot,
            &packet_view.agent_config_id,
            packet_view.tier_override.as_deref(),
            packet_view.backend_preference.as_deref(),
            config_dir,
            project_path.as_deref(),
            &presets,
        );

        let trigger_id = format!("delegated-{}-trigger", packet_view.id);
        let context_id = format!("delegated-{}-context", packet_view.id);
        let agent_id = format!("delegated-{}-agent", packet_view.id);

        if !snapshot
            .recipe
            .nodes
            .iter()
            .any(|node| node.id == trigger_id)
        {
            snapshot.recipe.nodes.push(RecipeNode {
                id: trigger_id.clone(),
                node_type: RecipeNodeType::Trigger,
                name: format!("{} trigger", packet_view.title),
                position: NodePosition { x: 0.0, y: 0.0 },
                parent_id: None,
                trigger_config: None,
                agent_config: None,
                action_config: None,
                checkpoint_config: None,
                artifact_config: None,
                condition_config: None,
                context_config: None,
            });
        }

        if !snapshot
            .recipe
            .nodes
            .iter()
            .any(|node| node.id == context_id)
        {
            let acceptance = if packet_view.acceptance.is_empty() {
                String::new()
            } else {
                format!(
                    "\n\nAcceptance criteria:\n{}",
                    packet_view
                        .acceptance
                        .iter()
                        .map(|item| format!("- {}", item))
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            };
            let policy = format!("\n\nWorking directory: {}", packet_view.ownership.cwd);
            snapshot.recipe.nodes.push(RecipeNode {
                id: context_id.clone(),
                node_type: RecipeNodeType::Context,
                name: format!("{} context", packet_view.title),
                position: NodePosition { x: 200.0, y: 0.0 },
                parent_id: None,
                trigger_config: None,
                agent_config: None,
                action_config: None,
                checkpoint_config: None,
                artifact_config: None,
                condition_config: None,
                context_config: Some(ContextNodeConfig {
                    content: format!("{}{}{}", packet_view.problem_statement, acceptance, policy),
                }),
            });
        }

        if !snapshot.recipe.nodes.iter().any(|node| node.id == agent_id) {
            snapshot.recipe.nodes.push(RecipeNode {
                id: agent_id.clone(),
                node_type: RecipeNodeType::Agent,
                name: packet_view.title.clone(),
                position: NodePosition { x: 400.0, y: 0.0 },
                parent_id: None,
                trigger_config: None,
                agent_config: Some(AgentNodeConfig {
                    agent_config_id: Some(packet_view.agent_config_id.clone()),
                    checkpoint: None,
                    output_schema: Some(schema_config_from_output_contract(
                        &packet_view.output_contract,
                    )),
                    git_config: Some(AgentGitConfig {
                        worktree_mode: WorktreeMode::None,
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

        push_edge_if_missing(
            &mut snapshot.recipe.edges,
            &trigger_id,
            "control-out",
            &agent_id,
            "control-in",
            RecipeEdgeType::Control,
        );
        push_edge_if_missing(
            &mut snapshot.recipe.edges,
            &context_id,
            "context-out",
            &agent_id,
            "context-in",
            RecipeEdgeType::Context,
        );

        let packet = &mut snapshot.delegated_packets[packet_index];
        packet.status = DelegatedStatus::Materialized;
        packet.materialized_node_ids = vec![trigger_id, context_id, agent_id];
    }

    let snapshot_json = serde_json::to_string(&snapshot)
        .map_err(|e| format!("Failed to serialize snapshot: {}", e))?;
    diesel::update(executions::table.find(execution_id))
        .set(UpdateExecutionChangeset {
            snapshot: Some(snapshot_json),
            ..Default::default()
        })
        .execute(conn)
        .map_err(|e| format!("Failed to update execution snapshot: {}", e))?;

    if new_agent_node_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let created_jobs =
        create_jobs_for_new_nodes(conn, execution_id, &new_agent_node_ids, &snapshot)?;

    for job in &created_jobs {
        let recipe_node_id = match &job.recipe_node_id {
            Some(id) => id,
            None => continue,
        };
        let packet = match snapshot.delegated_packets.iter().find(|packet| {
            packet
                .materialized_node_ids
                .iter()
                .any(|node_id| node_id == recipe_node_id)
        }) {
            Some(packet) => packet,
            None => continue,
        };

        let parent_worktree: Option<String> = jobs::table
            .find(&packet.parent_job_id)
            .select(jobs::worktree_path)
            .first(conn)
            .ok()
            .flatten();

        diesel::update(jobs::table.find(&job.id))
            .set((
                jobs::parent_job_id.eq(Some(packet.parent_job_id.as_str())),
                jobs::worktree_path.eq(parent_worktree.as_deref()),
                jobs::task_index.eq(packet.task_index),
            ))
            .execute(conn)
            .map_err(|e| format!("Failed to update delegated job: {}", e))?;
    }

    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(conn)
        .map_err(|e| format!("Failed to reload execution: {}", e))?;
    let snapshot_json = snapshot_json.ok_or("Execution has no snapshot")?;
    let mut snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json)
        .map_err(|e| format!("Failed to parse snapshot: {}", e))?;

    for packet in &mut snapshot.delegated_packets {
        if packet.status != DelegatedStatus::Materialized || packet.result_artifact_job_id.is_some()
        {
            continue;
        }
        let agent_node_id = packet
            .materialized_node_ids
            .iter()
            .find(|node_id| node_id.ends_with("-agent"))
            .cloned();
        let Some(agent_node_id) = agent_node_id else {
            continue;
        };
        let job_id: Option<String> = jobs::table
            .filter(jobs::execution_id.eq(execution_id))
            .filter(jobs::recipe_node_id.eq(&agent_node_id))
            .select(jobs::id)
            .first(conn)
            .optional()
            .map_err(|e| format!("Failed to find delegated job: {}", e))?;
        packet.result_artifact_job_id = job_id;
    }

    let snapshot_json = serde_json::to_string(&snapshot)
        .map_err(|e| format!("Failed to serialize snapshot: {}", e))?;
    diesel::update(executions::table.find(execution_id))
        .set(UpdateExecutionChangeset {
            snapshot: Some(snapshot_json),
            ..Default::default()
        })
        .execute(conn)
        .map_err(|e| format!("Failed to persist delegated packet state: {}", e))?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "jobs", "action": "insert"}),
    );

    Ok(new_agent_node_ids)
}

pub fn refresh_packet_state(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    packet_id: &str,
) -> Result<Option<DelegatedWorkPacket>, String> {
    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(conn)
        .map_err(|e| format!("Failed to load execution: {}", e))?;
    let snapshot_json = snapshot_json.ok_or("Execution has no snapshot")?;
    let mut snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json)
        .map_err(|e| format!("Failed to parse snapshot: {}", e))?;

    let mut changed = false;
    let packet = snapshot
        .delegated_packets
        .iter_mut()
        .find(|packet| packet.id == packet_id);
    let Some(packet) = packet else {
        return Ok(None);
    };

    if let Some(job_id) = &packet.result_artifact_job_id {
        let job_status: Option<String> = jobs::table
            .find(job_id)
            .select(jobs::status)
            .first(conn)
            .optional()
            .map_err(|e| format!("Failed to load delegated job: {}", e))?;
        let run_status: Option<String> = latest_run_for_job(conn, job_id)?
            .and_then(|run_id| {
                runs::table
                    .find(run_id)
                    .select(runs::status)
                    .first(conn)
                    .ok()
            })
            .flatten();

        let next_status = match job_status.as_deref() {
            Some("complete") => DelegatedStatus::Completed,
            Some("failed") => DelegatedStatus::Failed,
            Some("running") => DelegatedStatus::Running,
            Some("ready") | Some("pending") => DelegatedStatus::Materialized,
            _ if matches!(run_status.as_deref(), Some("live") | Some("starting")) => {
                DelegatedStatus::Running
            }
            _ => packet.status.clone(),
        };

        if packet.status != next_status {
            packet.status = next_status;
            changed = true;
        }
    }

    let result = packet.clone();
    if changed {
        let snapshot_json = serde_json::to_string(&snapshot)
            .map_err(|e| format!("Failed to serialize snapshot: {}", e))?;
        diesel::update(executions::table.find(execution_id))
            .set(UpdateExecutionChangeset {
                snapshot: Some(snapshot_json),
                ..Default::default()
            })
            .execute(conn)
            .map_err(|e| format!("Failed to update delegated packet status: {}", e))?;
    }

    Ok(Some(result))
}

pub fn latest_run_for_job(
    conn: &mut diesel::sqlite::SqliteConnection,
    job_id: &str,
) -> Result<Option<String>, String> {
    runs::table
        .filter(runs::job_id.eq(job_id))
        .order(runs::created_at.desc())
        .select(runs::id)
        .first(conn)
        .optional()
        .map_err(|e| format!("Failed to load delegated run: {}", e))
}

fn ensure_agent_snapshot(
    snapshot: &mut ExecutionSnapshot,
    agent_id: &str,
    tier_override: Option<&str>,
    backend_preference: Option<&str>,
    config_dir: &Path,
    project_path: Option<&Path>,
    presets: &PresetsConfig,
) {
    match snapshot.agents.entry(agent_id.to_string()) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            let agent_snapshot =
                match config_agents::get_agent(config_dir, entry.key(), project_path) {
                    Ok(Some(mut file_agent)) => {
                        if backend_preference.is_some() {
                            file_agent.backend_preference = backend_preference.map(str::to_string);
                        }
                        resolve_agent_snapshot(&file_agent, tier_override, presets).unwrap_or_else(
                            |_| {
                                make_placeholder_agent(
                                    agent_id,
                                    tier_override,
                                    backend_preference,
                                    presets,
                                )
                            },
                        )
                    }
                    _ => {
                        make_placeholder_agent(agent_id, tier_override, backend_preference, presets)
                    }
                };
            entry.insert(agent_snapshot);
        }
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            if tier_override.is_some() || backend_preference.is_some() {
                let agent = entry.get_mut();
                let authored_tier =
                    tier_override.or_else(|| agent.tier.as_ref().map(Model::as_str));
                let authored_backend = backend_preference.or(agent.backend_preference.as_deref());
                let (model, backend, extras) =
                    resolve_runtime_selection(authored_tier, authored_backend, presets)
                        .unwrap_or_else(|_| {
                            (
                                agent
                                    .model
                                    .clone()
                                    .unwrap_or_else(|| Model::new(Model::SONNET)),
                                agent
                                    .resolved_backend
                                    .clone()
                                    .unwrap_or_else(|| presets.active_backend.clone()),
                                agent.extras.clone().unwrap_or_default(),
                            )
                        });
                if let Some(tier) = tier_override {
                    agent.tier = Some(Model::new(
                        &crate::config::presets::normalize_tier_selection(tier, presets),
                    ));
                }
                if let Some(backend_preference) = backend_preference {
                    agent.backend_preference = Some(backend_preference.to_string());
                }
                agent.model = Some(model);
                agent.resolved_backend = Some(backend);
                agent.extras = Some(extras);
            }
        }
    }
}

fn make_placeholder_agent(
    agent_id: &str,
    tier_override: Option<&str>,
    backend_preference: Option<&str>,
    presets: &PresetsConfig,
) -> AgentSnapshot {
    let authored = crate::config::presets::normalize_authored_selection(
        tier_override,
        backend_preference,
        presets,
    );
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

    AgentSnapshot {
        id: agent_id.to_string(),
        name: agent_id.to_string(),
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

fn schema_config_from_output_contract(contract: &DelegatedOutputContract) -> SchemaConfig {
    SchemaConfig {
        name: contract
            .tool_name
            .clone()
            .unwrap_or_else(|| "return".to_string()),
        schema_type: contract.schema_type.clone(),
        fields: None,
        tool_name: contract.tool_name.clone(),
        description: contract.description.clone(),
    }
}

fn push_edge_if_missing(
    edges: &mut Vec<RecipeEdge>,
    source_id: &str,
    source_handle: &str,
    target_id: &str,
    target_handle: &str,
    edge_type: RecipeEdgeType,
) {
    if edges.iter().any(|edge| {
        edge.source_node_id == source_id
            && edge.source_handle == source_handle
            && edge.target_node_id == target_id
            && edge.target_handle == target_handle
            && edge.edge_type == edge_type
    }) {
        return;
    }

    edges.push(RecipeEdge {
        id: Uuid::new_v4().to_string(),
        edge_type,
        source_node_id: source_id.to_string(),
        source_handle: source_handle.to_string(),
        target_node_id: target_id.to_string(),
        target_handle: target_handle.to_string(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::{NewExecution, NewJob};
    use crate::models::{
        AgentSnapshot, DelegationOrigin, ExecutionSnapshot, RecipeSnapshot, RecipeTrigger,
        TriggerContext, TriggerType,
    };
    use crate::schema::{executions, jobs, projects};
    use crate::services::testing::CapturingEmitter;
    use crate::test_utils::{create_test_project, test_diesel_conn};
    use std::collections::HashMap;

    #[test]
    fn create_or_reuse_task_packet_reuses_stable_identity() {
        let mut snapshot = ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Delegation".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![],
                edges: vec![],
            },
            agents: HashMap::new(),
            skills: HashMap::new(),
            tools: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: None,
                project_id: "project-1".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
            },
            presets: None,
            delegated_packets: vec![DelegatedWorkPacket {
                id: "packet-1".to_string(),
                parent_job_id: "parent-1".to_string(),
                parent_turn_id: None,
                parent_tool_use_id: Some("tool-1".to_string()),
                origin: DelegationOrigin::TaskTool,
                title: "Explore".to_string(),
                problem_statement: "Inspect the code".to_string(),
                agent_config_id: "Explore".to_string(),
                ownership: crate::models::DelegatedOwnershipScope {
                    cwd: "/tmp/test".to_string(),
                    filesystem_scope: None,
                    approval_policy: None,
                },
                session: DelegatedSessionStrategy::default(),
                acceptance: vec![],
                output_contract: DelegatedOutputContract {
                    schema_type: "return".to_string(),
                    tool_name: None,
                    description: None,
                },
                status: DelegatedStatus::Pending,
                materialized_node_ids: vec![],
                result_artifact_job_id: None,
                task_index: Some(0),
                tier_override: None,
                backend_preference: None,
                created_at: 1,
            }],
            created_at: 1,
        };

        let packet = create_or_reuse_task_packet(
            &mut snapshot,
            CreateDelegatedPacketInput {
                parent_job_id: "parent-1",
                parent_turn_id: None,
                parent_tool_use_id: Some("tool-1"),
                title: "Explore",
                problem_statement: "Inspect the code",
                agent_config_id: "Explore",
                cwd: "/tmp/test",
                filesystem_scope: None,
                approval_policy: None,
                acceptance: vec![],
                output_contract: DelegatedOutputContract {
                    schema_type: "return".to_string(),
                    tool_name: None,
                    description: None,
                },
                session: DelegatedSessionStrategy::default(),
                task_index: Some(0),
                tier_override: None,
                backend_preference: None,
            },
        );

        assert_eq!(packet.id, "packet-1");
        assert_eq!(snapshot.delegated_packets.len(), 1);
    }

    #[test]
    fn create_or_reuse_task_packet_distinguishes_sibling_deliveries() {
        let mut snapshot = ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Delegation".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![],
                edges: vec![],
            },
            agents: HashMap::new(),
            skills: HashMap::new(),
            tools: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: None,
                project_id: "project-1".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
            },
            presets: None,
            delegated_packets: vec![],
            created_at: 1,
        };

        let first = create_or_reuse_task_packet(
            &mut snapshot,
            CreateDelegatedPacketInput {
                parent_job_id: "parent-1",
                parent_turn_id: None,
                parent_tool_use_id: Some("tool-1"),
                title: "Explore",
                problem_statement: "Inspect the code",
                agent_config_id: "Explore",
                cwd: "/tmp/test",
                filesystem_scope: None,
                approval_policy: None,
                acceptance: vec![],
                output_contract: DelegatedOutputContract {
                    schema_type: "return".to_string(),
                    tool_name: None,
                    description: None,
                },
                session: DelegatedSessionStrategy::default(),
                task_index: Some(0),
                tier_override: None,
                backend_preference: None,
            },
        );

        let second = create_or_reuse_task_packet(
            &mut snapshot,
            CreateDelegatedPacketInput {
                parent_job_id: "parent-1",
                parent_turn_id: None,
                parent_tool_use_id: Some("tool-2"),
                title: "Explore",
                problem_statement: "Inspect the code",
                agent_config_id: "Explore",
                cwd: "/tmp/test",
                filesystem_scope: None,
                approval_policy: None,
                acceptance: vec![],
                output_contract: DelegatedOutputContract {
                    schema_type: "return".to_string(),
                    tool_name: None,
                    description: None,
                },
                session: DelegatedSessionStrategy::default(),
                task_index: Some(0),
                tier_override: None,
                backend_preference: None,
            },
        );

        assert_ne!(first.id, second.id);
        assert_eq!(snapshot.delegated_packets.len(), 2);
    }

    #[test]
    fn create_or_reuse_task_packet_distinguishes_session_strategy() {
        let mut snapshot = ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Delegation".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![],
                edges: vec![],
            },
            agents: HashMap::new(),
            skills: HashMap::new(),
            tools: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: None,
                project_id: "project-1".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
            },
            presets: None,
            delegated_packets: vec![],
            created_at: 1,
        };

        let first = create_or_reuse_task_packet(
            &mut snapshot,
            CreateDelegatedPacketInput {
                parent_job_id: "parent-1",
                parent_turn_id: None,
                parent_tool_use_id: Some("tool-1"),
                title: "Explore",
                problem_statement: "Inspect the code",
                agent_config_id: "Explore",
                cwd: "/tmp/test",
                filesystem_scope: None,
                approval_policy: None,
                acceptance: vec![],
                output_contract: DelegatedOutputContract {
                    schema_type: "return".to_string(),
                    tool_name: None,
                    description: None,
                },
                session: DelegatedSessionStrategy::default(),
                task_index: Some(0),
                tier_override: None,
                backend_preference: None,
            },
        );

        let second = create_or_reuse_task_packet(
            &mut snapshot,
            CreateDelegatedPacketInput {
                parent_job_id: "parent-1",
                parent_turn_id: None,
                parent_tool_use_id: Some("tool-1"),
                title: "Explore",
                problem_statement: "Inspect the code",
                agent_config_id: "Explore",
                cwd: "/tmp/test",
                filesystem_scope: None,
                approval_policy: None,
                acceptance: vec![],
                output_contract: DelegatedOutputContract {
                    schema_type: "return".to_string(),
                    tool_name: None,
                    description: None,
                },
                session: DelegatedSessionStrategy {
                    mode: crate::models::DelegatedSessionMode::Fork,
                },
                task_index: Some(0),
                tier_override: None,
                backend_preference: None,
            },
        );

        assert_ne!(first.id, second.id);
        assert_eq!(snapshot.delegated_packets.len(), 2);
    }

    #[test]
    fn expand_delegated_packets_creates_packet_job() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let project_path: String = projects::table
            .find(&project_id)
            .select(projects::repo_path)
            .first(&mut conn)
            .unwrap();
        let parent_job_id = Uuid::new_v4().to_string();

        diesel::insert_into(jobs::table)
            .values(NewJob {
                id: &parent_job_id,
                execution_id: None,
                manager_id: None,
                recipe_node_id: None,
                parent_job_id: None,
                worktree_path: Some(&project_path),
                branch: None,
                base_commit: None,
                current_session_id: Some("session-1"),
                resume_session_id: None,
                status: "running",
                agent_config_id: Some("builder"),
                issue_id: None,
                project_id: &project_id,
                task_description: Some("Parent"),
                created_at: 1,
                updated_at: 1,
                completed_at: None,
                parent_tool_use_id: None,
                task_index: None,
                started_at: Some(1),
                model: None,
                node_name: Some("Builder"),
                base_branch: None,
                current_turn_id: None,
            })
            .execute(&mut conn)
            .unwrap();

        let execution_id = Uuid::new_v4().to_string();
        let snapshot = ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Delegation".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![],
                edges: vec![],
            },
            agents: HashMap::from([(
                "Explore".to_string(),
                AgentSnapshot {
                    id: "Explore".to_string(),
                    name: "Explore".to_string(),
                    description: String::new(),
                    prompt: String::new(),
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
                origin: DelegationOrigin::TaskTool,
                title: "Explore".to_string(),
                problem_statement: "Inspect the code".to_string(),
                agent_config_id: "Explore".to_string(),
                ownership: crate::models::DelegatedOwnershipScope {
                    cwd: project_path.clone(),
                    filesystem_scope: None,
                    approval_policy: None,
                },
                session: DelegatedSessionStrategy::default(),
                acceptance: vec![],
                output_contract: DelegatedOutputContract {
                    schema_type: "return".to_string(),
                    tool_name: None,
                    description: None,
                },
                status: DelegatedStatus::Pending,
                materialized_node_ids: vec![],
                result_artifact_job_id: None,
                task_index: Some(0),
                tier_override: None,
                backend_preference: None,
                created_at: 1,
            }],
            created_at: 1,
        };
        let snapshot_json = serde_json::to_string(&snapshot).unwrap();
        diesel::insert_into(executions::table)
            .values(NewExecution {
                id: &execution_id,
                recipe_id: "recipe-1",
                issue_id: None,
                project_id: Some(&project_id),
                status: "running",
                started_at: 1,
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

        let emitter = CapturingEmitter::default();
        let created =
            expand_delegated_packets(&mut conn, &execution_id, Path::new("/tmp"), &emitter)
                .unwrap();
        assert_eq!(created.len(), 1);

        let packet = refresh_packet_state(&mut conn, &execution_id, "packet-1")
            .unwrap()
            .unwrap();
        assert!(packet.result_artifact_job_id.is_some());

        let child_worktree: Option<String> = jobs::table
            .find(packet.result_artifact_job_id.unwrap())
            .select(jobs::worktree_path)
            .first(&mut conn)
            .ok()
            .flatten();
        assert_eq!(child_worktree.as_deref(), Some(project_path.as_str()));
    }
}
