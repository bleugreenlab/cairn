//! Recipe execution tracking and management.
//!
//! Core DB and config operations for recipe executions — listing, creating,
//! and managing the lifecycle of recipe execution records.
//!
//! Execution creation functions.
//! Functions taking `&Orchestrator` also need config/settings access.

use cairn_common::ids;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use crate::config::get_recipe_from_files;
use crate::config::presets::{
    available_selections, load_effective_presets, resolve_agent_snapshot,
    resolve_selection_with_provenance, LaunchSelectionOverride, PresetsConfig, ResolutionSource,
};
use crate::config::{agents as config_agents, skills as config_skills, ConfigResult};
use crate::execution::Initiator;
use crate::models::{
    Execution, ExecutionSnapshot, ExecutionStatus, Fence, Job, Model, ModelSelection, RecipeNode,
    RecipeNodeType, RecipeSnapshot, RuntimeExtras, SkillSnapshot, SnapshotOverrides,
    TriggerContext, TriggerType,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use serde::Serialize;
use std::collections::HashMap;

// Re-export execution queries with _impl aliases for backward compatibility.
#[allow(unused_imports)]
pub use super::queries::get_execution_for_issue as get_execution_for_issue_impl;
#[allow(unused_imports)]
pub use super::queries::list_executions_for_issue as list_executions_for_issue_impl;

pub fn create_jobs_for_execution(
    orch: &Orchestrator,
    execution_id: &str,
) -> Result<Vec<Job>, String> {
    let db = run_recipe_db({
        let dbs = orch.db.clone();
        let execution_id = execution_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_execution(&dbs, &execution_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    crate::execution::advancement::create_jobs_for_execution(db, execution_id)
}

/// Start a recipe execution, create its jobs, and kick off DAG advancement.
///
/// Shared start sequence for the Tauri command and the MCP executions-append
/// handler so both go through the same path. Advancement is enqueued on the
/// orchestrator effect queue; hosts without an effect queue (`effect_tx == None`)
/// get the execution + jobs created and must advance the DAG themselves.
#[allow(clippy::too_many_arguments)]
pub fn start_recipe_execution_and_advance(
    orch: &Orchestrator,
    issue_id: &str,
    recipe_id: Option<&str>,
    project_id: &str,
    backend: Option<&str>,
    initiated_via: Option<&str>,
    trigger_type: TriggerType,
) -> Result<Execution, String> {
    let execution = start_recipe_execution_impl(
        orch,
        issue_id,
        recipe_id,
        project_id,
        None,
        backend,
        None,
        initiated_via,
        trigger_type,
    )?;
    let _jobs = create_jobs_for_execution(orch, &execution.id)?;
    orch.notifier.emit_change("executions");
    orch.notifier.emit_change("jobs");
    if let Some(ref tx) = orch.effect_tx {
        let _ = tx.send(crate::effects::types::WorkflowEffect::AdvanceDag {
            execution_id: execution.id.clone(),
            outbox_entry_id: None,
        });
    }
    Ok(execution)
}

/// Start executing a recipe for an issue.
///
/// Creates an execution record with a snapshot of the recipe and agents captured
/// at the current point in time. Caller is responsible for creating jobs
/// (`create_jobs_for_execution`) and advancing the DAG.
///
/// Optional `overrides` can customize the recipe graph and agents before execution starts.
///
/// `initiated_via` stamps attribution on the snapshot's trigger context
/// (`Some("external")` for an authenticated external/CLI caller, `None` for the
/// default user start). Display/audit only.
#[allow(clippy::too_many_arguments)]
pub fn start_recipe_execution_impl(
    orch: &Orchestrator,
    issue_id: &str,
    recipe_id: Option<&str>,
    project_id: &str,
    overrides: Option<SnapshotOverrides>,
    backend: Option<&str>,
    initiator: Option<Initiator>,
    initiated_via: Option<&str>,
    trigger_type: TriggerType,
) -> Result<Execution, String> {
    let now = chrono::Utc::now().timestamp() as i32;

    // Resolve the owning database ONCE (fail-closed): a team project's rows live
    // wholly in its synced replica, so the execution and its jobs must be created
    // there, not in the private DB. The id-keyed resolvers cannot be used yet —
    // the execution row does not exist — so resolve by project id.
    let db = resolve_owning_db_for_project(orch, project_id)?;

    // Get project path for file-based recipe loading
    let project_path = project_path_for_recipe(db.clone(), project_id.to_string())?;

    // Get the recipe (use provided or find default)
    let recipe = match recipe_id {
        Some(id) => get_recipe_from_files(&orch.config_dir, Some(&project_path), id)?,
        None => return Err("No recipe specified for execution".to_string()),
    };

    // Build execution snapshot. An execution-wide backend override (external
    // driver / quickstart) becomes a per-agent Backend override applied at
    // resolve-early time; the snapshot then stores fully concrete selections.
    let override_sel = backend.map(|b| LaunchSelectionOverride::Backend(b.to_string()));
    let mut snapshot = build_execution_snapshot_from_files(
        &orch.config_dir,
        Some(&project_path),
        &recipe.id,
        Some(issue_id),
        project_id,
        trigger_type.clone(),
        None,
        override_sel.as_ref(),
    )?;
    snapshot.trigger_context.initiated_via = initiated_via.map(str::to_string);
    let effective_presets = load_effective_presets(&orch.config_dir, Some(&project_path));

    // Apply overrides if provided
    if let Some(overrides) = overrides {
        if let Some(recipe_override) = overrides.recipe {
            snapshot.recipe = recipe_override;
        }
        if let Some(agents_override) = overrides.agents {
            for (agent_id, agent_snapshot) in agents_override {
                // Concrete composer output is stored verbatim — its custom
                // selection + extras survive unchanged. Anything without a
                // resolved selection runs through the unified resolver once.
                let normalized = if agent_snapshot.selection.is_some() {
                    agent_snapshot
                } else {
                    resolve_agent_snapshot(
                        &crate::config::agents::FileAgent {
                            id: agent_snapshot.id.clone(),
                            name: agent_snapshot.name.clone(),
                            description: agent_snapshot.description.clone(),
                            prompt: agent_snapshot.prompt.clone(),
                            tools: agent_snapshot.tools.clone(),
                            tier: agent_snapshot.tier.clone().or_else(|| {
                                agent_snapshot.selection.as_ref().map(|s| s.model.clone())
                            }),
                            fence: agent_snapshot.fence,
                            disallowed_tools: agent_snapshot.disallowed_tools.clone(),
                            skills: agent_snapshot.skills.clone(),
                            hooks: None,
                            backend_preference: agent_snapshot.backend_preference.clone(),
                            is_project_scoped: true,
                            file_path: std::path::PathBuf::new(),
                        },
                        override_sel.as_ref(),
                        &effective_presets,
                    )?
                };
                snapshot.agents.insert(agent_id, normalized);
            }
        }

        // Load any new agents referenced by the overridden recipe that aren't in the snapshot
        let new_agent_ids: Vec<String> = snapshot
            .recipe
            .nodes
            .iter()
            .filter(|n| n.node_type == RecipeNodeType::Agent)
            .filter_map(|n| {
                n.agent_config
                    .as_ref()
                    .and_then(|cfg| cfg.agent_config_id.clone())
            })
            .filter(|id| !snapshot.agents.contains_key(id))
            .collect();

        if !new_agent_ids.is_empty() {
            // Load each new agent
            for agent_id in new_agent_ids {
                if let Ok(Some(file_agent)) =
                    config_agents::get_agent(&orch.config_dir, &agent_id, Some(&project_path))
                {
                    let agent_snapshot = resolve_agent_snapshot(
                        &file_agent,
                        override_sel.as_ref(),
                        &effective_presets,
                    )?;
                    snapshot.agents.insert(agent_id.clone(), agent_snapshot);
                }
            }
        }
    }

    let snapshot_json = snapshot.to_json()?;

    // Calculate next seq for this issue
    let next_seq = next_execution_seq_for_issue(db.clone(), issue_id.to_string())?;

    // Create execution
    let exec_id = ids::mint_child(issue_id);
    insert_execution(
        db.clone(),
        NewExecution {
            id: exec_id.clone(),
            recipe_id: recipe.id.clone(),
            issue_id: Some(issue_id.to_string()),
            project_id: None,
            status: "running".to_string(),
            started_at: now,
            completed_at: None,
            snapshot: Some(snapshot_json),
            seq: Some(next_seq),
            initiator: initiator.clone(),
            triggered_by: trigger_type.to_string(),
        },
    )?;

    Ok(Execution {
        id: exec_id,
        recipe_id: recipe.id,
        issue_id: Some(issue_id.to_string()),
        project_id: None,
        status: ExecutionStatus::Running,
        started_at: now as i64,
        completed_at: None,
        seq: Some(next_seq),
        initiator: initiator.clone(),
        triggered_by: trigger_type,
    })
}

/// Start executing a recipe manually (no issue, just project).
///
/// Creates an execution record for a manual trigger. Caller is responsible for
/// creating jobs and advancing the DAG.
pub fn start_manual_execution_impl(
    orch: &Orchestrator,
    recipe_id: &str,
    project_id: &str,
    initiator: Option<Initiator>,
) -> Result<Execution, String> {
    let now = chrono::Utc::now().timestamp() as i32;

    let db = resolve_owning_db_for_project(orch, project_id)?;

    // Get project path for file-based recipe loading
    let project_path = project_path_for_recipe(db.clone(), project_id.to_string())?;

    // Verify recipe exists (load from files)
    let recipe = get_recipe_from_files(&orch.config_dir, Some(&project_path), recipe_id)?;

    // Build execution snapshot
    let snapshot = build_execution_snapshot_from_files(
        &orch.config_dir,
        Some(&project_path),
        &recipe.id,
        None,
        project_id,
        TriggerType::Manual,
        None,
        None,
    )?;
    let snapshot_json = snapshot.to_json()?;

    // Create execution
    let exec_id = ids::mint_child(project_id);
    insert_execution(
        db.clone(),
        NewExecution {
            id: exec_id.clone(),
            recipe_id: recipe.id.clone(),
            issue_id: None,
            project_id: Some(project_id.to_string()),
            status: "running".to_string(),
            started_at: now,
            completed_at: None,
            snapshot: Some(snapshot_json),
            seq: None,
            initiator: initiator.clone(),
            triggered_by: "manual".to_string(),
        },
    )?;

    Ok(Execution {
        id: exec_id,
        recipe_id: recipe.id,
        issue_id: None,
        project_id: Some(project_id.to_string()),
        status: ExecutionStatus::Running,
        started_at: now as i64,
        completed_at: None,
        seq: None,
        initiator: initiator.clone(),
        triggered_by: TriggerType::Manual,
    })
}

/// Start a recipe execution triggered by an event (JobEnded or SkillCalled).
///
/// Similar to `start_manual_execution_impl` but:
/// - Sets `trigger_type` to the event's type (JobEnded/SkillCalled)
/// - Stores `event_payload` in TriggerContext
/// - Uses the triggering execution's initiator for credential resolution
/// - Supports optional issue_id (Issue-scoped vs Project-scoped triggers)
pub fn start_event_triggered_execution(
    orch: &Orchestrator,
    recipe: &crate::models::Recipe,
    trigger_type: TriggerType,
    event_payload: serde_json::Value,
    issue_id: Option<&str>,
    project_id: &str,
    initiator: Option<Initiator>,
) -> Result<Execution, String> {
    let now = chrono::Utc::now().timestamp() as i32;

    let trigger_type_str = trigger_type.to_string();

    let db = resolve_owning_db_for_project(orch, project_id)?;

    // Build execution snapshot with event payload
    let project_path = project_path_for_recipe(db.clone(), project_id.to_string())?;
    let snapshot = build_execution_snapshot_from_files(
        &orch.config_dir,
        Some(&project_path),
        &recipe.id,
        issue_id,
        project_id,
        trigger_type,
        Some(event_payload),
        None,
    )?;
    let snapshot_json = snapshot.to_json()?;

    // Calculate next seq if issue-scoped
    let next_seq: Option<i32> = issue_id
        .map(|iid| next_execution_seq_for_issue(db.clone(), iid.to_string()))
        .transpose()?;

    // Create execution
    let exec_id = ids::mint_child(project_id);
    insert_execution(
        db.clone(),
        NewExecution {
            id: exec_id.clone(),
            recipe_id: recipe.id.clone(),
            issue_id: issue_id.map(str::to_string),
            project_id: if issue_id.is_none() {
                Some(project_id.to_string())
            } else {
                None
            },
            status: "running".to_string(),
            started_at: now,
            completed_at: None,
            snapshot: Some(snapshot_json),
            seq: next_seq,
            initiator: initiator.clone(),
            triggered_by: trigger_type_str.clone(),
        },
    )?;

    Ok(Execution {
        id: exec_id,
        recipe_id: recipe.id.clone(),
        issue_id: issue_id.map(|s| s.to_string()),
        project_id: if issue_id.is_none() {
            Some(project_id.to_string())
        } else {
            None
        },
        status: ExecutionStatus::Running,
        started_at: now as i64,
        completed_at: None,
        seq: next_seq,
        initiator: initiator.clone(),
        triggered_by: trigger_type_str.parse().unwrap_or_default(),
    })
}

#[allow(unused_imports)]
pub use super::queries::{
    get_execution_detail as get_execution_detail_impl,
    list_all_executions as list_all_executions_impl,
};

// ===========================================================================
// Launch-composer resolution contract
// ===========================================================================
//
// `resolve_recipe_launch` is the single entry point a launch-composer UI calls
// to preview a recipe's fully-resolved per-agent settings before starting it.
// It returns one row per agent node carrying the chosen atomic `selection`,
// orthogonal `extras`, permissions, and the resolution `source` (provenance) —
// or a loud per-node `error` when that node cannot resolve. The whole plan only
// fails (`Err`) for systemic problems (recipe not found, project missing); a
// single unresolvable agent surfaces as `error: Some(..)` on its row.

/// Fully-resolved launch preview for a recipe: the recipe graph plus one row per
/// agent node. Documented contract for the launch-composer UI.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LaunchPlan {
    pub recipe: RecipeSnapshot,
    pub nodes: Vec<ResolvedLaunchNode>,
}

/// One agent node's resolution row. `resolved` is `None` (with `error: Some`)
/// when this node failed to resolve; the rest of the plan is unaffected.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedLaunchNode {
    pub node_id: String,
    pub agent_id: String,
    pub agent_name: String,
    /// Prompt source signal (avoids shipping the full prompt by default).
    pub prompt_present: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved: Option<ResolvedAgentRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Atomic dropdown options for this node: the global tier-resolved list unioned
    /// with the row's own resolved selection. Present on every node, including
    /// error rows, so an unresolvable node still renders a selectable dropdown.
    pub available: Vec<ModelSelection>,
}

/// The concrete resolved settings for one agent node.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedAgentRow {
    /// The chosen atomic backend+model (single-dropdown value).
    pub selection: ModelSelection,
    pub extras: RuntimeExtras,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fence: Option<Fence>,
    /// Which level decided the selection.
    pub source: ResolutionSource,
    /// Atomic options for the dropdown (each a backend+model pair). MAY ship
    /// empty now and be filled in the composer follow-up.
    pub available: Vec<ModelSelection>,
}

/// Per-node launch overrides, keyed by node id — one selection knob per node.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LaunchOverrides {
    #[serde(default)]
    pub nodes: HashMap<String, LaunchSelectionOverride>,
    /// Optional edited recipe structure. When the composer appends/removes nodes
    /// or rewires the graph, it sends the working snapshot so resolution reflects
    /// the edited structure rather than the file-loaded recipe. Echoed back in
    /// `LaunchPlan.recipe`.
    #[serde(default)]
    pub recipe: Option<RecipeSnapshot>,
}

/// Resolve a recipe's per-agent-node launch settings with provenance and loud,
/// per-node errors. The composer's contract.
pub fn resolve_recipe_launch(
    orch: &Orchestrator,
    recipe_id: &str,
    project_id: &str,
    overrides: Option<LaunchOverrides>,
) -> Result<LaunchPlan, String> {
    let db = resolve_owning_db_for_project(orch, project_id)?;
    let project_path = project_path_for_recipe(db, project_id.to_string())?;
    let recipe = get_recipe_from_files(&orch.config_dir, Some(&project_path), recipe_id)?;
    let presets = load_effective_presets(&orch.config_dir, Some(&project_path));
    let overrides = overrides.unwrap_or_default();

    // A composer that has edited the recipe structure (append/remove/graph-edit)
    // supplies the working RecipeSnapshot; resolve against it so appended nodes
    // resolve authoritatively. Otherwise resolve the file-loaded recipe.
    let recipe_snapshot = overrides.recipe.clone().unwrap_or_else(|| RecipeSnapshot {
        id: recipe.id.clone(),
        name: recipe.name.clone(),
        description: recipe.description.clone(),
        trigger: recipe.trigger.clone(),
        nodes: recipe.nodes.clone(),
        edges: recipe.edges.clone(),
    });

    let config_dir = orch.config_dir.clone();
    let nodes = resolve_launch_nodes(&recipe_snapshot.nodes, &presets, &overrides, |agent_id| {
        config_agents::get_agent(&config_dir, agent_id, Some(&project_path))
            .map_err(|e| e.to_string())
    });

    Ok(LaunchPlan {
        recipe: recipe_snapshot,
        nodes,
    })
}

/// Pure per-node resolver: maps recipe agent nodes to resolved launch rows using a
/// supplied agent loader. Every node carries the global `available` option list
/// (unioned with the row's own resolved selection) so even an unresolvable row can
/// render the model dropdown. Loud, per-node errors; never fails the whole plan.
fn resolve_launch_nodes(
    recipe_nodes: &[RecipeNode],
    presets: &PresetsConfig,
    overrides: &LaunchOverrides,
    load_agent: impl Fn(&str) -> Result<Option<config_agents::FileAgent>, String>,
) -> Vec<ResolvedLaunchNode> {
    let global = available_selections(presets);
    let union = |selection: Option<&ModelSelection>| -> Vec<ModelSelection> {
        let mut list = global.clone();
        if let Some(sel) = selection {
            if !list
                .iter()
                .any(|s| s.backend == sel.backend && s.model.as_str() == sel.model.as_str())
            {
                list.push(sel.clone());
            }
        }
        list
    };

    let mut nodes = Vec::new();
    for node in recipe_nodes {
        if node.node_type != RecipeNodeType::Agent {
            continue;
        }
        let Some(agent_id) = node
            .agent_config
            .as_ref()
            .and_then(|c| c.agent_config_id.clone())
        else {
            continue;
        };

        let file_agent = match load_agent(&agent_id) {
            Ok(Some(fa)) => fa,
            Ok(None) => {
                nodes.push(ResolvedLaunchNode {
                    node_id: node.id.clone(),
                    agent_id: agent_id.clone(),
                    agent_name: agent_id.clone(),
                    prompt_present: false,
                    resolved: None,
                    error: Some(format!("Agent config not found: {agent_id}")),
                    available: union(None),
                });
                continue;
            }
            Err(e) => {
                nodes.push(ResolvedLaunchNode {
                    node_id: node.id.clone(),
                    agent_id: agent_id.clone(),
                    agent_name: agent_id.clone(),
                    prompt_present: false,
                    resolved: None,
                    error: Some(format!("Failed to load agent '{agent_id}': {e}")),
                    available: union(None),
                });
                continue;
            }
        };

        let resolved = match overrides.nodes.get(&node.id) {
            Some(LaunchSelectionOverride::Concrete(selection)) => {
                Ok(crate::config::presets::ResolvedSelection {
                    selection: selection.clone(),
                    extras: RuntimeExtras::default(),
                    source: ResolutionSource::ExecutionOverride,
                })
            }
            Some(LaunchSelectionOverride::Tier(tier)) => resolve_selection_with_provenance(
                Some(tier),
                None,
                file_agent.backend_preference.as_deref(),
                presets,
            ),
            Some(LaunchSelectionOverride::Backend(backend)) => resolve_selection_with_provenance(
                file_agent.tier.as_ref().map(Model::as_str),
                Some(backend),
                file_agent.backend_preference.as_deref(),
                presets,
            ),
            None => resolve_selection_with_provenance(
                file_agent.tier.as_ref().map(Model::as_str),
                None,
                file_agent.backend_preference.as_deref(),
                presets,
            ),
        };

        let (resolved_row, error, available) = match resolved {
            Ok(r) => {
                let available = union(Some(&r.selection));
                (
                    Some(ResolvedAgentRow {
                        selection: r.selection,
                        extras: r.extras,
                        fence: file_agent.fence,
                        source: r.source,
                        available: available.clone(),
                    }),
                    None,
                    available,
                )
            }
            Err(e) => (None, Some(e), union(None)),
        };

        nodes.push(ResolvedLaunchNode {
            node_id: node.id.clone(),
            agent_id: agent_id.clone(),
            agent_name: file_agent.name.clone(),
            prompt_present: !file_agent.prompt.is_empty(),
            resolved: resolved_row,
            error,
            available,
        });
    }
    nodes
}

fn project_path_for_recipe(
    db: Arc<LocalDb>,
    project_id: String,
) -> Result<std::path::PathBuf, String> {
    run_recipe_db(async move {
        let repo_path = project_repo_path(&db, &project_id).await?;
        repo_path
            .map(std::path::PathBuf::from)
            .ok_or_else(|| format!("Project not found: {}", project_id))
    })
}

fn next_execution_seq_for_issue(db: Arc<LocalDb>, issue_id: String) -> Result<i32, String> {
    run_recipe_db(async move {
        db.read(|conn| {
            let issue_id = issue_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT MAX(seq) FROM executions WHERE issue_id = ?1",
                        (issue_id.as_str(),),
                    )
                    .await?;
                let max_seq = rows
                    .next()
                    .await?
                    .map(|row| row.opt_i64(0))
                    .transpose()?
                    .flatten()
                    .map(|seq| seq as i32);
                Ok(max_seq.map(|seq| seq + 1).unwrap_or(1))
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

struct NewExecution {
    id: String,
    recipe_id: String,
    issue_id: Option<String>,
    project_id: Option<String>,
    status: String,
    started_at: i32,
    completed_at: Option<i32>,
    snapshot: Option<String>,
    seq: Option<i32>,
    initiator: Option<Initiator>,
    triggered_by: String,
}

fn insert_execution(db: Arc<LocalDb>, execution: NewExecution) -> Result<(), String> {
    run_recipe_db(async move {
        db.write(|conn| {
            let execution = NewExecution {
                id: execution.id.clone(),
                recipe_id: execution.recipe_id.clone(),
                issue_id: execution.issue_id.clone(),
                project_id: execution.project_id.clone(),
                status: execution.status.clone(),
                started_at: execution.started_at,
                completed_at: execution.completed_at,
                snapshot: execution.snapshot.clone(),
                seq: execution.seq,
                initiator: execution.initiator.clone(),
                triggered_by: execution.triggered_by.clone(),
            };
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO executions (
                        id, recipe_id, issue_id, project_id, status, started_at,
                        completed_at, snapshot, seq, initiator_sub, initiator_auth_mode,
                        initiator_org_id, triggered_by
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                    params![
                        execution.id.as_str(),
                        execution.recipe_id.as_str(),
                        execution.issue_id.as_deref(),
                        execution.project_id.as_deref(),
                        execution.status.as_str(),
                        execution.started_at,
                        execution.completed_at,
                        execution.snapshot.as_deref(),
                        execution.seq,
                        execution.initiator.as_ref().map(|i| i.sub.as_str()),
                        execution.initiator.as_ref().map(|i| i.auth_mode.as_str()),
                        execution.initiator.as_ref().map(|i| i.org_id.as_str()),
                        execution.triggered_by.as_str(),
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

async fn project_repo_path(db: &LocalDb, project_id: &str) -> Result<Option<String>, String> {
    let project_id = project_id.to_string();
    db.query_text(
        "SELECT repo_path FROM projects WHERE id = ?1",
        (project_id,),
    )
    .await
    .map_err(|e| e.to_string())
}

/// Fail-closed sync resolve of a project's owning database, bridged onto the
/// recipe DB runtime. Used at execution-start sites where no execution/job/run
/// row exists yet, so the id-keyed resolvers cannot be used.
fn resolve_owning_db_for_project(
    orch: &Orchestrator,
    project_id: &str,
) -> Result<Arc<LocalDb>, String> {
    let dbs = orch.db.clone();
    let project_id = project_id.to_string();
    run_recipe_db(async move {
        crate::execution::routing::owning_db_for_project(&dbs, &project_id)
            .await
            .map_err(|e| e.to_string())
    })
}

fn run_recipe_db<T, Fut>(future: Fut) -> Result<T, String>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("Failed to start recipe database runtime: {}", e))?
            .block_on(future)
    })
    .join()
    .map_err(|_| "Recipe database task panicked".to_string())?
}

#[allow(clippy::too_many_arguments)]
fn build_execution_snapshot_from_files(
    config_dir: &Path,
    project_path: Option<&Path>,
    recipe_id: &str,
    issue_id: Option<&str>,
    project_id: &str,
    trigger_type: TriggerType,
    event_payload: Option<serde_json::Value>,
    override_sel: Option<&LaunchSelectionOverride>,
) -> Result<ExecutionSnapshot, String> {
    let recipe = get_recipe_from_files(config_dir, project_path, recipe_id)?;
    let recipe_snapshot = RecipeSnapshot {
        id: recipe.id.clone(),
        name: recipe.name,
        description: recipe.description,
        trigger: recipe.trigger,
        nodes: recipe.nodes.clone(),
        edges: recipe.edges,
    };
    let presets = load_effective_presets(config_dir, project_path);
    let all_skills = load_all_skills(config_dir, project_path);
    let mut agents = std::collections::HashMap::new();
    let mut skills = std::collections::HashMap::new();
    let agent_ids: Vec<String> = recipe
        .nodes
        .iter()
        .filter(|node| node.node_type == RecipeNodeType::Agent)
        .filter_map(|node| {
            node.agent_config
                .as_ref()
                .and_then(|config| config.agent_config_id.clone())
        })
        .collect();
    for agent_id in agent_ids {
        if let Ok(Some(file_agent)) = config_agents::get_agent(config_dir, &agent_id, project_path)
        {
            for (skill_id, skill_snapshot) in &all_skills {
                skills
                    .entry(skill_id.clone())
                    .or_insert_with(|| skill_snapshot.clone());
            }
            let snapshot = resolve_agent_snapshot(&file_agent, override_sel, &presets)?;
            agents.insert(agent_id, snapshot);
        }
    }
    let trigger_context = TriggerContext {
        issue_id: issue_id.map(str::to_string),
        project_id: project_id.to_string(),
        trigger_type,
        event_payload,
        initiated_via: None,
    };
    // Resolve-early: the snapshot now stores fully concrete per-agent selections;
    // no frozen preset matrix is captured.
    Ok(ExecutionSnapshot::new(
        recipe_snapshot,
        agents,
        skills,
        trigger_context,
    ))
}

fn load_all_skills(
    config_dir: &Path,
    project_path: Option<&Path>,
) -> std::collections::HashMap<String, SkillSnapshot> {
    let mut skills = std::collections::HashMap::new();
    if let Ok(file_skills) = config_skills::list_skills(config_dir, project_path) {
        for result in file_skills {
            if let ConfigResult::Ok(skill) = result {
                // config_root_subdirs yields project first, so keep the first
                // occurrence for each id to let project skills shadow workspace.
                skills
                    .entry(skill.id.clone())
                    .or_insert_with(|| SkillSnapshot {
                        id: skill.id,
                        name: skill.name,
                        description: skill.description,
                        prompt: skill.prompt,
                        allowed_tools: skill.allowed_tools,
                    });
            }
        }
    }
    skills
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::presets::default_presets_config;
    use crate::models::{AgentNodeConfig, NodePosition};

    fn agent_node(id: &str, agent_id: &str) -> RecipeNode {
        RecipeNode {
            id: id.to_string(),
            node_type: RecipeNodeType::Agent,
            name: format!("Node {id}"),
            position: NodePosition { x: 0.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: Some(AgentNodeConfig {
                agent_config_id: Some(agent_id.to_string()),
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

    fn file_agent(id: &str, tier: Option<&str>) -> config_agents::FileAgent {
        config_agents::FileAgent {
            id: id.to_string(),
            name: format!("Agent {id}"),
            description: String::new(),
            prompt: "do work".to_string(),
            tools: Vec::new(),
            tier: tier.map(Model::new),
            fence: None,
            disallowed_tools: None,
            skills: None,
            hooks: None,
            backend_preference: None,
            is_project_scoped: true,
            file_path: std::path::PathBuf::new(),
        }
    }

    #[test]
    fn resolved_row_carries_nonempty_available() {
        let presets = default_presets_config(Some(31999));
        let nodes = [agent_node("n1", "builder")];
        let overrides = LaunchOverrides::default();
        let out = resolve_launch_nodes(&nodes, &presets, &overrides, |_id| {
            Ok(Some(file_agent("builder", Some("md"))))
        });
        assert_eq!(out.len(), 1);
        let row = &out[0];
        assert!(row.error.is_none());
        let resolved = row.resolved.as_ref().unwrap();
        assert_eq!(resolved.selection.model.as_str(), "sonnet");
        assert!(!resolved.available.is_empty());
        assert!(!row.available.is_empty());
        assert!(row
            .available
            .iter()
            .any(|s| s.backend == "claude" && s.model.as_str() == "sonnet"));
    }

    #[test]
    fn undefined_tier_produces_error_with_available() {
        let presets = default_presets_config(Some(31999));
        let nodes = [agent_node("n1", "weird")];
        let overrides = LaunchOverrides::default();
        let out = resolve_launch_nodes(&nodes, &presets, &overrides, |_id| {
            Ok(Some(file_agent("weird", Some("xl"))))
        });
        let row = &out[0];
        assert!(row.resolved.is_none());
        assert!(row.error.is_some());
        // Error rows still render a selectable dropdown.
        assert!(!row.available.is_empty());
    }

    #[test]
    fn concrete_override_pins_verbatim_and_is_selectable() {
        let presets = default_presets_config(Some(31999));
        let nodes = [agent_node("n1", "builder")];
        let mut overrides = LaunchOverrides::default();
        let pin = ModelSelection {
            backend: "claude".to_string(),
            model: Model::new("custom-xl"),
        };
        overrides.nodes.insert(
            "n1".to_string(),
            LaunchSelectionOverride::Concrete(pin.clone()),
        );
        let out = resolve_launch_nodes(&nodes, &presets, &overrides, |_id| {
            Ok(Some(file_agent("builder", Some("md"))))
        });
        let resolved = out[0].resolved.as_ref().unwrap();
        assert_eq!(resolved.selection.model.as_str(), "custom-xl");
        assert_eq!(resolved.selection.backend, "claude");
        assert!(out[0]
            .available
            .iter()
            .any(|s| s.model.as_str() == "custom-xl"));
    }

    #[test]
    fn missing_agent_is_per_node_error() {
        let presets = default_presets_config(Some(31999));
        let nodes = [agent_node("n1", "ghost")];
        let overrides = LaunchOverrides::default();
        let out = resolve_launch_nodes(&nodes, &presets, &overrides, |_id| Ok(None));
        assert!(out[0].error.as_ref().unwrap().contains("not found"));
        assert!(out[0].resolved.is_none());
        assert!(!out[0].available.is_empty());
    }
}
