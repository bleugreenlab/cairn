//! Recipe execution tracking and management.
//!
//! Core DB and config operations for recipe executions — listing, creating,
//! and managing the lifecycle of recipe execution records.
//!
//! Execution creation functions.
//! Functions taking `&Orchestrator` also need config/settings access.

use cairn_common::ids;
use cairn_models::types::catalog::VerbKind;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use crate::config::get_recipe_from_files;
use crate::models::Continuity;

fn get_selected_recipe(
    config_dir: &std::path::Path,
    project_path: &std::path::Path,
    project_id: &str,
    recipe_id: &str,
) -> Result<crate::models::Recipe, String> {
    let file_recipe =
        crate::config::recipes::get_recipe(config_dir, recipe_id, Some(project_path))?
            .ok_or_else(|| format!("Recipe not found: {recipe_id}"))?;
    let policy = crate::config::contextual_packages::load_contextual_packages(Some(project_path));
    let selection = policy.selection(
        crate::config::contextual_packages::ContextualPackageKind::Recipe,
        recipe_id,
        &file_recipe.bundles,
    );
    if !policy.is_selected(
        crate::config::contextual_packages::ContextualPackageKind::Recipe,
        recipe_id,
        &file_recipe.bundles,
    ) {
        let reason = if matches!(
            selection,
            crate::config::contextual_packages::ContextualPackageSelection::ExplicitlyDisabled
        ) {
            "disabled"
        } else {
            "not enabled"
        };
        return Err(format!(
            "Recipe '{}' is {reason} for project {project_id}",
            file_recipe.recipe.name
        ));
    }
    Ok(file_recipe.recipe)
}
use crate::config::model_routing::IssueLabels;
use crate::config::presets::{
    available_selections, load_effective_presets, resolve_agent_snapshot,
    resolve_selection_with_provenance, LaunchSelectionOverride, PresetsConfig, ResolutionSource,
};
use crate::config::{agents as config_agents, skills as config_skills, ConfigResult};
use crate::execution::canonical;
use crate::execution::model_routing::LaunchSelectionPlan;
use crate::execution::Initiator;
use crate::models::{
    BranchTarget, Execution, ExecutionSnapshot, ExecutionStatus, Fence, Job, LaunchCustomization,
    Model, ModelRoutingDecision, ModelSelection, RecipeNode, RecipeNodeType, RecipeSnapshot,
    RuntimeExtras, SkillSnapshot, TriggerContext, TriggerType,
};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use cairn_db::turso::params;
use serde::Serialize;
use std::collections::HashMap;

/// What starting this execution means for the issue's canonical lineage.
///
/// There is deliberately **no retry variant**, which makes "automatic retry must
/// not mint a competing execution" structural rather than a convention. The
/// retry paths cannot reach this function at all: `continue_automatic_retry`
/// resumes the SAME job through `continue_job_impl` with a pre-claimed turn, and
/// `restart_node` re-runs a node inside its existing execution. Neither has an
/// intent to pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LaunchIntent {
    /// The issue's next attempt. Supersedes every prior canonical execution of
    /// that issue, atomically, as part of its own insert. The operator who chose
    /// "New execution" has already made the supersede decision; there is no
    /// second confirmation.
    #[default]
    NewAttempt,
    /// A declared alternative. Never canonical, never superseding, never fenced.
    ParallelAlternative,
    /// A launch nobody chose: a `job_ended` trigger, a skill call, a webhook, a
    /// schedule firing on an issue that already exists.
    ///
    /// Such a launch has no operator authority to take the issue's attempt away
    /// from live work, so it **never supersedes**. It becomes canonical only
    /// when nothing else is — the ordinary case of a trigger on a fresh or
    /// finished issue — and otherwise runs as a declared parallel alternative
    /// beside the canonical attempt, which is what it actually is.
    ///
    /// Inferring ownership transfer from the mere presence of an `issue_id` is
    /// the mistake this variant exists to make unavailable: it would let an
    /// unrelated automatic event cancel an issue's builder, its review node and
    /// its delegated sub-tasks, with nobody having decided anything.
    AutomaticTrigger,
}

impl LaunchIntent {
    /// Whether this launch may fence the execution that owns the issue now.
    fn supersedes(self) -> bool {
        matches!(self, LaunchIntent::NewAttempt)
    }
}

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
///
/// A launch with `after:` gates is split here. Its first half inserts the
/// execution and its gates and STOPS: no jobs, so no admission request and no
/// slot. The second half — job creation and the advancement tail below — is
/// [`crate::execution::gates::sweep::release`], applied to the same row once
/// every gate is true. The returned execution is `Held`; the caller evaluates it
/// at once (`gates::sweep::evaluate_execution`) so a gate already true releases
/// immediately, and the notifier poke here covers a caller that does not.
#[allow(clippy::too_many_arguments)]
pub(crate) fn start_recipe_execution_and_advance(
    orch: &Orchestrator,
    issue_id: &str,
    recipe_id: Option<&str>,
    project_id: &str,
    backend: Option<&str>,
    initiated_via: Option<&str>,
    branch_target: Option<BranchTarget>,
    continuity: Option<Continuity>,
    customization: Option<LaunchCustomization>,
    trigger_type: TriggerType,
    intent: LaunchIntent,
    after: &[crate::routes::TriggerClause],
) -> Result<Execution, String> {
    let gates = crate::execution::gates::authoring::canonical_predicates(after, "after")?;
    let execution = launch_recipe_execution(
        orch,
        issue_id,
        recipe_id,
        project_id,
        customization,
        backend,
        None,
        initiated_via,
        None,
        branch_target,
        continuity,
        trigger_type,
        intent,
        &gates,
    )?;
    let execution_change =
        crate::notify::execution_db_change_ids("insert", &execution.id, Some(issue_id), project_id);
    if !gates.is_empty() {
        let _ = orch.services.emitter.emit("db-change", execution_change);
        crate::execution::gates::sweep::notify_gate_evidence_changed();
        return Ok(execution);
    }
    let jobs = create_jobs_for_execution(orch, &execution.id)?;
    let _ = orch.services.emitter.emit("db-change", execution_change);
    for job in &jobs {
        let _ = orch
            .services
            .emitter
            .emit("db-change", crate::notify::job_db_change(job, "insert"));
    }
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
/// Optional `customization` adjusts the graph and agents before the snapshot is
/// frozen: a composer's finished graph, or an agent caller's delta compiled
/// against the resolved recipe. Both arrive here as one [`SnapshotOverrides`]
/// and take the same application path below.
///
/// `initiated_via` stamps attribution on the snapshot's trigger context
/// (`Some("external")` for an authenticated external/CLI caller, `None` for the
/// default user start). Display/audit only.
///
/// `branch_target` says where this execution's work lands (`None` = the recipe's
/// first declared target, which is `new` for every recipe that doesn't say
/// otherwise). It is resolved into node topology once, here, so every downstream
/// derivation reads a snapshot that already has the right shape.
#[allow(clippy::too_many_arguments)]
pub fn start_recipe_execution_impl(
    orch: &Orchestrator,
    issue_id: &str,
    recipe_id: Option<&str>,
    project_id: &str,
    customization: Option<LaunchCustomization>,
    backend: Option<&str>,
    initiator: Option<Initiator>,
    initiated_via: Option<&str>,
    // CAIRN-2629: the machine that should OWN this execution. `None` (or the local
    // id) stamps this machine; a peer id from the runner picker stamps that peer,
    // whose owner-side sweep then claims and runs the jobs. The picker enforces
    // that the chosen device has a clone; core stamps the resolved id.
    runner_device_id: Option<String>,
    branch_target: Option<BranchTarget>,
    continuity: Option<Continuity>,
    trigger_type: TriggerType,
    // CAIRN-4665: whether this launch claims the issue's current attempt. A
    // `NewAttempt` supersedes every prior canonical execution of the issue in
    // the same transaction that inserts this one.
    intent: LaunchIntent,
) -> Result<Execution, String> {
    launch_recipe_execution(
        orch,
        issue_id,
        recipe_id,
        project_id,
        customization,
        backend,
        initiator,
        initiated_via,
        runner_device_id,
        branch_target,
        continuity,
        trigger_type,
        intent,
        &[],
    )
}

/// [`start_recipe_execution_impl`] with `after:` gates: `gates` are canonical
/// predicate JSON, already validated. Non-empty gates insert the execution
/// HELD, with its gate rows, in the same transaction as the insert — a held
/// execution must never be observable without the gates that make it held,
/// because with no gates and no jobs the projection derives `complete`.
#[allow(clippy::too_many_arguments)]
fn launch_recipe_execution(
    orch: &Orchestrator,
    issue_id: &str,
    recipe_id: Option<&str>,
    project_id: &str,
    customization: Option<LaunchCustomization>,
    backend: Option<&str>,
    initiator: Option<Initiator>,
    initiated_via: Option<&str>,
    runner_device_id: Option<String>,
    branch_target: Option<BranchTarget>,
    continuity: Option<Continuity>,
    trigger_type: TriggerType,
    intent: LaunchIntent,
    gates: &[String],
) -> Result<Execution, String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let this_device = orch.anon_device_manager.device_id();
    let owner_device = runner_device_id.unwrap_or_else(|| this_device.clone());

    // Resolve the owning database ONCE (fail-closed): a team project's rows live
    // wholly in its synced replica, so the execution and its jobs must be created
    // there, not in the private DB. The id-keyed resolvers cannot be used yet —
    // the execution row does not exist — so resolve by project id.
    let db = resolve_owning_db_for_project(orch, project_id)?;

    // CAIRN-2629: if a PEER device was chosen as the runner, enforce that it has a
    // local clone of this project before stamping ownership on it — otherwise its
    // sweep would claim the job and fail in prepare_job with no clone. This is the
    // authoritative guard (the picker filters as a convenience). This machine is
    // trusted (the launch flow already blocks starting without a clone).
    if owner_device != this_device {
        let db_check = db.clone();
        let owner = owner_device.clone();
        let project_id_owned = project_id.to_string();
        let can_run = run_recipe_db(async move {
            let key = crate::execution::ownership::project_key(&db_check, &project_id_owned)
                .await
                .ok_or_else(|| "Project not found".to_string())?;
            Ok::<bool, String>(
                crate::execution::ownership::device_has_clone(&db_check, &owner, &key).await,
            )
        })?;
        if !can_run {
            return Err(format!(
                "Selected runner device {owner_device} has no local clone of this project"
            ));
        }
    }

    // Get project path for file-based recipe loading
    let project_path = project_path_for_recipe(db.clone(), project_id.to_string())?;

    // Get the recipe (use provided or find default)
    let recipe = match recipe_id {
        Some(id) => get_selected_recipe(&orch.config_dir, &project_path, project_id, id)?,
        None => return Err("No recipe specified for execution".to_string()),
    };

    let effective_presets = load_effective_presets(&orch.config_dir, Some(&project_path));

    // The per-agent resolution plan for this launch, built once and consulted by
    // every resolve_agent_snapshot call below. It carries three inputs: the
    // execution-wide backend override (external driver / quickstart), the
    // project's label bindings applied to this issue's labels, and the agents a
    // human already chose a model for. Computing the pinned set here, before the
    // customization is consumed, is what keeps "an explicit choice wins" a single
    // decision rather than a property of the order later layers apply in.
    let pinned = customization
        .as_ref()
        .map(LaunchCustomization::pinned_agent_ids)
        .unwrap_or_default();
    let plan = plan_for_launch(
        &db,
        &project_path,
        Some(issue_id),
        backend,
        pinned,
        &effective_presets,
    )?;

    // Build execution snapshot. Resolution is early: the snapshot stores fully
    // concrete per-agent selections rather than the inputs they came from.
    let mut snapshot = build_execution_snapshot_from_files(
        orch,
        Some(&project_path),
        &recipe.id,
        Some(issue_id),
        project_id,
        trigger_type.clone(),
        None,
        &plan,
    )?;
    snapshot.trigger_context.initiated_via = initiated_via.map(str::to_string);

    // A delta is compiled against the recipe as it just resolved, into exactly
    // the overrides the composer would have sent. Everything below this line
    // sees one grammar and cannot tell which door the launch came through.
    let overrides = match customization {
        None => None,
        Some(LaunchCustomization::Snapshot(overrides)) => Some(overrides),
        Some(LaunchCustomization::Deltas(deltas)) => {
            let resolve_agent =
                |agent_id: &str| -> Result<Option<crate::models::AgentSnapshot>, String> {
                    match config_agents::get_agent(&orch.config_dir, agent_id, Some(&project_path))
                    {
                        Ok(Some(file_agent)) => {
                            let routing = plan
                                .for_agent(agent_id, file_agent.tier.as_ref().map(Model::as_str));
                            resolve_agent_snapshot(
                                &file_agent,
                                routing.selection.as_ref(),
                                &effective_presets,
                            )
                            .map(Some)
                        }
                        Ok(None) => Ok(None),
                        Err(error) => Err(format!("Failed to load agent '{agent_id}': {error}")),
                    }
                };
            Some(deltas.compile(&snapshot, &resolve_agent)?)
        }
    };

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
                    let authored = agent_snapshot.tier.clone();
                    let routing = plan.for_agent(&agent_id, authored.as_ref().map(Model::as_str));
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
                            icon: None,
                            bundles: Vec::new(),
                            is_project_scoped: true,
                            file_path: std::path::PathBuf::new(),
                            seed_budget_tokens: None,
                            seed_eras: None,
                        },
                        routing.selection.as_ref(),
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
                    let routing =
                        plan.for_agent(&agent_id, file_agent.tier.as_ref().map(Model::as_str));
                    let agent_snapshot = resolve_agent_snapshot(
                        &file_agent,
                        routing.selection.as_ref(),
                        &effective_presets,
                    )?;
                    snapshot.agents.insert(agent_id.clone(), agent_snapshot);
                }
            }
        }
    }

    // Resolve the branch target LAST, so a launch-time graph edit from the
    // composer is transformed too rather than bypassing it.
    crate::execution::branch_target::apply_branch_target(
        &mut snapshot,
        branch_target,
        &recipe.branch_targets,
    )?;

    // What this execution does with committed work its predecessors left. Read
    // at branch-mint time by `execution::continuity`; `None` is `Auto`, which is
    // what every launch that does not think about it should get.
    snapshot.continuity = continuity.unwrap_or_default();

    // Freeze why every agent got the model it got, against the agent map the
    // execution will actually run.
    plan.record_frozen(&snapshot.agents, &effective_presets);
    snapshot.model_routing = Some(plan.into_provenance());

    let snapshot_json = snapshot.to_json()?;

    // Gates whose evidence is already in wait on nothing, so the cycle refusal
    // must not count them. Resolved before the transaction: evidence reads route
    // to other databases.
    let satisfied = if gates.is_empty() {
        std::collections::HashSet::new()
    } else {
        let (orch, gates) = (orch.clone(), gates.to_vec());
        run_recipe_db(async move {
            Ok(crate::execution::gates::authoring::already_satisfied(&orch, &gates).await)
        })?
    };

    // Phase 1: sequence, fence, cascade, and insert as one transaction.
    let exec_id = ids::mint_child(issue_id);
    let launch = insert_issue_execution_atomic(
        db.clone(),
        orch.db.clone(),
        issue_id.to_string(),
        NewExecution {
            id: exec_id.clone(),
            recipe_id: recipe.id.clone(),
            issue_id: Some(issue_id.to_string()),
            project_id: None,
            status: if gates.is_empty() { "running" } else { "held" }.to_string(),
            started_at: now,
            completed_at: None,
            snapshot: Some(snapshot_json),
            // Both are assigned inside the transaction from the issue's maxima.
            seq: None,
            initiator: initiator.clone(),
            triggered_by: trigger_type.to_string(),
            runner_device_id: owner_device.clone(),
            // Resolved inside the transaction from the issue's own lineage.
            lifecycle: crate::models::ExecutionLifecycle::Canonical,
        },
        intent,
        match initiated_via {
            Some("external") => canonical::SupersessionActor::external(None),
            _ => canonical::SupersessionActor::operator_ui(),
        },
        LaunchGates {
            predicates: gates.to_vec(),
            satisfied,
        },
    )?;

    // Phase 2, after the fence has committed.
    stop_superseded_agents(orch, &launch.supersessions);

    Ok(Execution {
        id: exec_id,
        recipe_id: recipe.id,
        issue_id: Some(issue_id.to_string()),
        project_id: None,
        status: if gates.is_empty() {
            ExecutionStatus::Running
        } else {
            ExecutionStatus::Held
        },
        started_at: now as i64,
        completed_at: None,
        seq: Some(launch.seq),
        initiator: initiator.clone(),
        triggered_by: trigger_type,
        runner_device_id: Some(owner_device),
        lifecycle: launch.lifecycle,
        // An ungated launch is never held, so there is nothing to release it
        // from; a gated one has not been released yet.
        released_at: None,
        release_override_json: None,
        gate_census: None,
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
    branch_target: Option<BranchTarget>,
) -> Result<Execution, String> {
    let now = chrono::Utc::now().timestamp() as i32;

    let db = resolve_owning_db_for_project(orch, project_id)?;

    // Get project path for file-based recipe loading
    let project_path = project_path_for_recipe(db.clone(), project_id.to_string())?;

    // Verify recipe exists (load from files)
    let recipe = get_selected_recipe(&orch.config_dir, &project_path, project_id, recipe_id)?;

    // Build execution snapshot
    let mut snapshot = build_execution_snapshot_from_files(
        orch,
        Some(&project_path),
        &recipe.id,
        None,
        project_id,
        TriggerType::Manual,
        None,
        // No issue means no labels, so nothing to route from.
        &LaunchSelectionPlan::empty(None),
    )?;
    crate::execution::branch_target::apply_branch_target(
        &mut snapshot,
        branch_target,
        &recipe.branch_targets,
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
            runner_device_id: orch.anon_device_manager.device_id(),
            // A project-scoped execution owns no issue attempt, so it contends
            // for no lineage: the partial unique index does not cover it and its
            // runs stamp generation 0, which the fence answers Clear.
            lifecycle: crate::models::ExecutionLifecycle::Canonical,
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
        released_at: None,
        release_override_json: None,
        gate_census: None,
        seq: None,
        initiator: initiator.clone(),
        triggered_by: TriggerType::Manual,
        runner_device_id: Some(orch.anon_device_manager.device_id()),
        lifecycle: crate::models::ExecutionLifecycle::Canonical,
    })
}

/// Start a recipe execution triggered by an event (JobEnded or SkillCalled).
///
/// Similar to `start_manual_execution_impl` but:
/// - Sets `trigger_type` to the event's type (JobEnded/SkillCalled)
/// - Stores `event_payload` in TriggerContext
/// - Uses the triggering execution's initiator for credential resolution
/// - Supports optional issue_id (Issue-scoped vs Project-scoped triggers)
pub(crate) fn start_event_triggered_execution(
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

    // Build execution snapshot with event payload. An event-triggered launch is
    // still a launch: when it carries an issue, that issue's labels route it.
    let project_path = project_path_for_recipe(db.clone(), project_id.to_string())?;
    let effective_presets = load_effective_presets(&orch.config_dir, Some(&project_path));
    let plan = plan_for_launch(
        &db,
        &project_path,
        issue_id,
        None,
        std::collections::HashSet::new(),
        &effective_presets,
    )?;
    let mut snapshot = build_execution_snapshot_from_files(
        orch,
        Some(&project_path),
        &recipe.id,
        issue_id,
        project_id,
        trigger_type,
        Some(event_payload),
        &plan,
    )?;
    plan.record_frozen(&snapshot.agents, &effective_presets);
    snapshot.model_routing = Some(plan.into_provenance());
    let snapshot_json = snapshot.to_json()?;

    // An event-triggered launch carrying an issue goes through the same atomic
    // insert, but as an `AutomaticTrigger`: nobody chose it, so it may not take
    // the issue's attempt away from live work. It becomes canonical only when
    // nothing else holds the attempt, and otherwise runs beside the canonical
    // execution as the parallel alternative it actually is. A project-scoped
    // trigger owns no attempt at all and takes the plain insert.
    let exec_id = ids::mint_child(project_id);
    let new_execution = NewExecution {
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
        seq: None,
        initiator: initiator.clone(),
        triggered_by: trigger_type_str.clone(),
        runner_device_id: orch.anon_device_manager.device_id(),
        lifecycle: crate::models::ExecutionLifecycle::Canonical,
    };
    let (next_seq, lifecycle) = match issue_id {
        Some(issue_id) => {
            let launch = insert_issue_execution_atomic(
                db.clone(),
                orch.db.clone(),
                issue_id.to_string(),
                new_execution,
                LaunchIntent::AutomaticTrigger,
                canonical::SupersessionActor::system(),
                LaunchGates::default(),
            )?;
            stop_superseded_agents(orch, &launch.supersessions);
            (Some(launch.seq), launch.lifecycle)
        }
        None => {
            insert_execution(db.clone(), new_execution)?;
            (None, crate::models::ExecutionLifecycle::Canonical)
        }
    };

    Ok(Execution {
        id: exec_id,
        recipe_id: recipe.id.clone(),
        released_at: None,
        release_override_json: None,
        gate_census: None,
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
        runner_device_id: Some(orch.anon_device_manager.device_id()),
        lifecycle,
    })
}

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
    recipe: RecipeSnapshot,
    nodes: Vec<ResolvedLaunchNode>,
}

/// One agent node's resolution row. `resolved` is `None` (with `error: Some`)
/// when this node failed to resolve; the rest of the plan is unaffected.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedLaunchNode {
    node_id: String,
    agent_id: String,
    agent_name: String,
    /// Prompt source signal (avoids shipping the full prompt by default).
    prompt_present: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    resolved: Option<ResolvedAgentRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// Atomic dropdown options for this node: the global tier-resolved list unioned
    /// with the row's own resolved selection. Present on every node, including
    /// error rows, so an unresolvable node still renders a selectable dropdown.
    available: Vec<ModelSelection>,
}

/// The concrete resolved settings for one agent node.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedAgentRow {
    /// The chosen atomic backend+model (single-dropdown value).
    selection: ModelSelection,
    extras: RuntimeExtras,
    #[serde(skip_serializing_if = "Option::is_none")]
    fence: Option<Fence>,
    /// Which level decided the selection.
    source: ResolutionSource,
    /// Atomic options for the dropdown (each a backend+model pair). MAY ship
    /// empty now and be filled in the composer follow-up.
    available: Vec<ModelSelection>,
    /// Why this row resolved the way it did, when the launching issue's labels
    /// had a say. Present on every routed row so the operator can see the reason
    /// before pressing Start rather than discover it afterwards.
    #[serde(skip_serializing_if = "Option::is_none")]
    routing: Option<ModelRoutingDecision>,
}

/// Per-node launch overrides, keyed by node id — one selection knob per node.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LaunchOverrides {
    #[serde(default)]
    nodes: HashMap<String, LaunchSelectionOverride>,
    /// Optional edited recipe structure. When the composer appends/removes nodes
    /// or rewires the graph, it sends the working snapshot so resolution reflects
    /// the edited structure rather than the file-loaded recipe. Echoed back in
    /// `LaunchPlan.recipe`.
    #[serde(default)]
    recipe: Option<RecipeSnapshot>,
}

/// Resolve a recipe's per-agent-node launch settings with provenance and loud,
/// per-node errors. The composer's contract.
pub fn resolve_recipe_launch(
    orch: &Orchestrator,
    recipe_id: &str,
    project_id: &str,
    issue_id: Option<&str>,
    overrides: Option<LaunchOverrides>,
) -> Result<LaunchPlan, String> {
    let db = resolve_owning_db_for_project(orch, project_id)?;
    let project_path = project_path_for_recipe(db.clone(), project_id.to_string())?;
    let recipe = get_selected_recipe(&orch.config_dir, &project_path, project_id, recipe_id)?;
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

    // The preview applies the same plan the start path will. Nothing is pinned
    // yet -- the composer's pin is the operator confirming what this shows them,
    // which is only a meaningful confirmation if they saw the routed model here.
    let plan = plan_for_launch(
        &db,
        &project_path,
        issue_id,
        None,
        std::collections::HashSet::new(),
        &presets,
    )?;

    let config_dir = orch.config_dir.clone();
    // One provider record per backend, asked whether a selection can hold a
    // session: the same answer the session door gives at launch.
    let providers = std::cell::RefCell::new(HashMap::new());
    let serves_session = |selection: &ModelSelection| -> Result<(), String> {
        let mut providers = providers.borrow_mut();
        let provider = providers
            .entry(selection.backend.clone())
            .or_insert_with(|| orch.catalog_provider(&selection.backend, Some(project_id)));
        provider
            .admit(selection.model.as_str(), VerbKind::Session)
            .map(|_| ())
            .map_err(|refusal| {
                refusal
                    .detail
                    .clone()
                    .unwrap_or_else(|| refusal.to_string())
            })
    };
    let nodes = resolve_launch_nodes(
        &recipe_snapshot.nodes,
        &presets,
        &overrides,
        &plan,
        |agent_id| {
            config_agents::get_agent(&config_dir, agent_id, Some(&project_path))
                .map_err(|e| e.to_string())
        },
        serves_session,
    );

    Ok(LaunchPlan {
        recipe: recipe_snapshot,
        nodes,
    })
}

/// Pure per-node resolver: maps recipe agent nodes to resolved launch rows using a
/// supplied agent loader. Every node carries the global `available` option list
/// (unioned with the row's own resolved selection) so even an unresolvable row can
/// render the model dropdown. Loud, per-node errors; never fails the whole plan.
///
/// `serves_session` says whether a selection can hold a session. A selection
/// that cannot is never offered, and a row that resolves to one is reported
/// as an error naming its tier rather than quietly given another model.
fn resolve_launch_nodes(
    recipe_nodes: &[RecipeNode],
    presets: &PresetsConfig,
    overrides: &LaunchOverrides,
    plan: &LaunchSelectionPlan,
    load_agent: impl Fn(&str) -> Result<Option<config_agents::FileAgent>, String>,
    serves_session: impl Fn(&ModelSelection) -> Result<(), String>,
) -> Vec<ResolvedLaunchNode> {
    let global: Vec<ModelSelection> = available_selections(presets)
        .into_iter()
        .filter(|selection| serves_session(selection).is_ok())
        .collect();
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

        // A per-node override in the preview payload is an explicit choice and
        // is never contested; every other row asks the plan.
        let node_override = overrides.nodes.get(&node.id);
        let routing = match node_override {
            Some(_) => None,
            None => Some(plan.for_agent(&agent_id, file_agent.tier.as_ref().map(Model::as_str))),
        };
        let effective =
            node_override.or_else(|| routing.as_ref().and_then(|r| r.selection.as_ref()));

        let resolved = match effective {
            Some(LaunchSelectionOverride::Concrete(selection)) => {
                crate::config::presets::resolve_concrete_selection(selection, presets)
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

        // The tier this row asked for, to name in a refusal.
        let asked_tier = match effective {
            Some(LaunchSelectionOverride::Tier(tier)) => Some(tier.as_str()),
            Some(LaunchSelectionOverride::Concrete(_)) => None,
            Some(LaunchSelectionOverride::Backend(_)) | None => {
                file_agent.tier.as_ref().map(Model::as_str)
            }
        };
        let resolved = resolved.and_then(|r| match serves_session(&r.selection) {
            Ok(()) => Ok(r),
            Err(why) => {
                let landed = format!("{} on {}", r.selection.model, r.selection.backend);
                Err(match asked_tier {
                    Some(tier) => format!("the '{tier}' tier resolves to {landed}: {why}"),
                    None => format!("{landed}: {why}"),
                })
            }
        });
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
                        routing: routing.as_ref().map(|r| r.decision.clone()),
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

/// Build the per-agent resolution plan for one launch.
///
/// A launch with no issue has no labels, so it gets an empty plan whose behavior
/// is exactly what it was before routing existed. A launch with an issue reads
/// that issue's labels and the project's binding table -- and a malformed table
/// refuses the launch here, before an execution row exists, rather than
/// degrading into "everything resolves to its default" in silence.
fn plan_for_launch(
    db: &Arc<LocalDb>,
    project_path: &Path,
    issue_id: Option<&str>,
    backend: Option<&str>,
    pinned: std::collections::HashSet<String>,
    presets: &PresetsConfig,
) -> Result<LaunchSelectionPlan, String> {
    let backend = backend.map(str::to_string);
    let Some(issue_id) = issue_id else {
        return Ok(LaunchSelectionPlan::empty(backend));
    };
    let table = crate::config::model_routing::load_model_routing(project_path)?;
    let labels = issue_labels_for_launch(db.clone(), issue_id.to_string())?;
    Ok(LaunchSelectionPlan::new(
        backend, table, labels, pinned, presets,
    ))
}

fn issue_labels_for_launch(db: Arc<LocalDb>, issue_id: String) -> Result<IssueLabels, String> {
    run_recipe_db(async move {
        db.read(|conn| {
            let issue_id = issue_id.clone();
            Box::pin(
                async move { crate::labels::attach::list_labels_for_issue(conn, &issue_id).await },
            )
        })
        .await
        .map_err(|e| format!("Failed to read issue labels for model routing: {e}"))
    })
    .map(|labels| IssueLabels::new(labels.into_iter().map(|label| (label.id, label.name))))
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
    runner_device_id: String,
    lifecycle: crate::models::ExecutionLifecycle,
}

/// Test seam: file a held execution as if the satisfaction preflight had
/// observed `satisfied`, so a test can take the evidence away between that
/// observation and the transaction that acts on it.
#[cfg(test)]
pub(crate) fn file_held_with_observed_satisfaction(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    issue_id: &str,
    recipe_id: &str,
    snapshot: String,
    gates: Vec<String>,
    satisfied: std::collections::HashSet<String>,
) -> Result<String, String> {
    let exec_id = ids::mint_child(issue_id);
    insert_issue_execution_atomic(
        db,
        orch.db.clone(),
        issue_id.to_string(),
        NewExecution {
            id: exec_id.clone(),
            recipe_id: recipe_id.to_string(),
            issue_id: Some(issue_id.to_string()),
            project_id: None,
            status: "held".to_string(),
            started_at: 1,
            completed_at: None,
            snapshot: Some(snapshot),
            seq: None,
            initiator: None,
            triggered_by: TriggerType::Manual.to_string(),
            runner_device_id: orch.anon_device_manager.device_id(),
            lifecycle: crate::models::ExecutionLifecycle::Canonical,
        },
        LaunchIntent::NewAttempt,
        canonical::SupersessionActor::operator_ui(),
        LaunchGates {
            predicates: gates,
            satisfied,
        },
    )?;
    Ok(exec_id)
}

/// The `after:` gates a launch is filed with: their canonical predicate JSON,
/// and the subset the satisfaction preflight already observed satisfied. The
/// two travel together because that observation is what waives a predicate's
/// wait edge, and it is latched on the gate row in the same transaction.
#[derive(Default)]
struct LaunchGates {
    predicates: Vec<String>,
    satisfied: std::collections::HashSet<String>,
}

/// What starting an issue's execution actually did to its lineage.
struct IssueLaunch {
    seq: i32,
    lifecycle: crate::models::ExecutionLifecycle,
    /// Phase-1 audit rows, each owing a phase-2 stop.
    supersessions: Vec<crate::execution::canonical::Supersession>,
}

/// Phase 1 of supersession, as ONE write transaction (CAIRN-4665).
///
/// Sequence allocation, the fence, the suspension cascade, the successor insert,
/// and the audit rows all commit together or not at all. They were three
/// separate round-trips before, and the window between them is exactly where a
/// concurrent launch could mint two canonical executions for one issue.
///
/// A `ParallelAlternative` takes the next sequence and generation but supersedes
/// nothing and is inserted `parallel`, so it can never be mistaken for the
/// issue's implementation.
///
/// `gates` are the `after:` gates of a held launch (empty for an unheld one).
/// Their wait-cycle refusal and their rows land in this same transaction: the
/// refusal so two concurrent filings cannot each pass and together close a
/// cycle, the rows so the execution is never visible without what holds it.
///
/// A held `NewAttempt` supersedes NOTHING here. It is inserted `pending`, outside
/// the canonical slot, and takes the attempt when it is released — from
/// whatever owns it then (`gates::sweep::release_conn`). Fencing at filing would
/// stop the very run a "re-run after X merges" is waiting to follow, and if that
/// run has finished by release time there is nothing to fence at all.
fn insert_issue_execution_atomic(
    db: Arc<LocalDb>,
    dbs: Arc<crate::db::DbState>,
    issue_id: String,
    mut execution: NewExecution,
    intent: LaunchIntent,
    actor: crate::execution::canonical::SupersessionActor,
    gates: LaunchGates,
) -> Result<IssueLaunch, String> {
    let LaunchGates {
        predicates: gates,
        satisfied,
    } = gates;
    if !gates.is_empty() && intent == LaunchIntent::AutomaticTrigger {
        return Err(
            "an automatic launch cannot be held: `after:` gates are set by whoever files the \
             execution"
                .to_string(),
        );
    }
    let own_db = db.clone();
    run_recipe_db(async move {
        db.write(move |conn| {
            let issue_id = issue_id.clone();
            let actor = actor.clone();
            let gates = gates.clone();
            let satisfied = satisfied.clone();
            let dbs = dbs.clone();
            let own_db = own_db.clone();
            let mut execution = NewExecution {
                id: execution.id.clone(),
                recipe_id: execution.recipe_id.clone(),
                issue_id: execution.issue_id.clone(),
                project_id: execution.project_id.clone(),
                status: execution.status.clone(),
                started_at: execution.started_at,
                completed_at: execution.completed_at,
                snapshot: execution.snapshot.take(),
                seq: execution.seq,
                initiator: execution.initiator.clone(),
                triggered_by: execution.triggered_by.clone(),
                runner_device_id: execution.runner_device_id.clone(),
                lifecycle: execution.lifecycle,
            };
            Box::pin(async move {
                let now = chrono::Utc::now().timestamp();
                crate::execution::gates::authoring::refuse_cycles_conn(
                    conn, &dbs, &own_db, &issue_id, None, &gates, &satisfied,
                )
                .await?;
                let seq = crate::execution::canonical::next_seq_conn(conn, &issue_id).await?;
                let held = !gates.is_empty();

                // The fence is published BEFORE the successor exists, so there is
                // never an instant at which two rows read canonical.
                let supersessions = if intent.supersedes() && !held {
                    crate::execution::canonical::supersede_canonical_conn(
                        conn,
                        &issue_id,
                        execution.id.as_str(),
                        &actor,
                        now,
                    )
                    .await?
                } else {
                    Vec::new()
                };

                // Resolved AFTER the fence, in the same transaction. An
                // automatic trigger takes the attempt only when the read above
                // finds nobody holding it -- and since it never supersedes, that
                // means nobody held it to begin with.
                let lifecycle = match intent {
                    LaunchIntent::NewAttempt if held => crate::models::ExecutionLifecycle::Pending,
                    LaunchIntent::NewAttempt => crate::models::ExecutionLifecycle::Canonical,
                    LaunchIntent::ParallelAlternative => {
                        crate::models::ExecutionLifecycle::Parallel
                    }
                    LaunchIntent::AutomaticTrigger => {
                        if crate::execution::canonical::issue_has_canonical_execution(
                            conn, &issue_id,
                        )
                        .await?
                        {
                            crate::models::ExecutionLifecycle::Parallel
                        } else {
                            crate::models::ExecutionLifecycle::Canonical
                        }
                    }
                };

                execution.seq = Some(seq);
                execution.lifecycle = lifecycle;
                insert_execution_conn(conn, &execution).await?;
                if held {
                    let gate_ids = cairn_workflow::gates::store::insert_gates_conn(
                        conn,
                        execution.id.as_str(),
                        &gates,
                        now,
                    )
                    .await?;
                    // The satisfaction that waived a wait edge above is latched
                    // with the gate: evidence that goes away before the first
                    // sweep must not leave behind the very edge the refusal
                    // skipped because it was met.
                    for (gate_id, predicate) in gate_ids.iter().zip(&gates) {
                        if satisfied.contains(predicate) {
                            cairn_workflow::gates::store::record_outcome_conn(
                                conn,
                                gate_id,
                                "satisfied",
                                Some(now),
                            )
                            .await?;
                        }
                    }
                    // A held filing beside live work must not drag the issue's
                    // projection; one on a fresh issue projects it backlog.
                    crate::transitions::outcome::recompute_issue_status_conn(conn, &issue_id)
                        .await?;
                }

                Ok(IssueLaunch {
                    seq,
                    lifecycle,
                    supersessions,
                })
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

/// Step 2: interrupt the agents step 1 froze.
///
/// Deliberately best-effort, and that is safe because of the ORDER. By the time
/// this runs the fence has committed, so a process that survives it can do
/// nothing durable: its next turn is refused at admission and its next tool call
/// is refused at dispatch. All this saves is the tokens it would spend before
/// reaching one of those, which for an agent is one tool call away.
///
/// That is also why there is no reconciler behind it. A crash between the fence
/// and this loop costs at most one turn of an agent whose work already cannot
/// land — a bounded, self-closing cost, not a correctness hole.
pub(crate) fn stop_superseded_agents(
    orch: &Orchestrator,
    supersessions: &[crate::execution::canonical::Supersession],
) {
    for supersession in supersessions {
        log::info!(
            "superseded execution {}: interrupting {} frozen job(s)",
            &supersession.superseded_execution_id
                [..supersession.superseded_execution_id.len().min(8)],
            supersession.stop_targets.len(),
        );
        for job_id in &supersession.stop_targets {
            if let Err(error) = crate::orchestrator::lifecycle::stop_job(orch, job_id) {
                log::warn!(
                    "superseded job {}: interrupt failed, leaving it fenced: {error}",
                    &job_id[..job_id.len().min(8)]
                );
            }
        }
    }
}

async fn insert_execution_conn(
    conn: &cairn_db::turso::Connection,
    execution: &NewExecution,
) -> crate::storage::DbResult<()> {
    conn.execute(
        "INSERT INTO executions (
            id, recipe_id, issue_id, project_id, status, started_at,
            completed_at, snapshot, seq, initiator_sub,
            initiator_org_id, triggered_by, runner_device_id, lifecycle
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
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
            execution.initiator.as_ref().map(|i| i.org_id.as_str()),
            execution.triggered_by.as_str(),
            execution.runner_device_id.as_str(),
            execution.lifecycle.to_string(),
        ],
    )
    .await?;
    Ok(())
}

/// Insert a PROJECT-scoped execution (no issue), which owns no issue attempt and
/// therefore has no lineage to contend for.
fn insert_execution(db: Arc<LocalDb>, execution: NewExecution) -> Result<(), String> {
    run_recipe_db(async move {
        db.write(move |conn| {
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
                runner_device_id: execution.runner_device_id.clone(),
                lifecycle: execution.lifecycle,
            };
            Box::pin(async move { insert_execution_conn(conn, &execution).await })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

/// The recipe id a filing names, as its launch will resolve it.
///
/// The id of the file [`crate::config::recipes::get_recipe`] loads for it in
/// this project, which is what the launch snapshots: `Memory-Triage` comes
/// back as the file's own `memory-triage`. A path, a file name, or anything
/// else that is not a recipe id is refused here, naming the canonical id when
/// one resolves. A plain id no file answers to comes back as named, and the
/// launch then refuses it as not found. A filing's policy checks (the tenet
/// gate) read this id and its launch is handed this same id, so the two can
/// never disagree about which recipe is filed.
pub(crate) async fn resolve_filed_recipe_id(
    config_dir: &Path,
    db: &LocalDb,
    project_id: &str,
    recipe: Option<&str>,
) -> Result<Option<String>, String> {
    let Some(recipe) = recipe.map(str::trim).filter(|recipe| !recipe.is_empty()) else {
        return Ok(None);
    };
    let project_path = project_repo_path(db, project_id)
        .await?
        .map(std::path::PathBuf::from);
    let resolved = crate::config::recipes::get_recipe(config_dir, recipe, project_path.as_deref())?;
    Ok(Some(resolved.map_or_else(
        || recipe.to_string(),
        |file_recipe| file_recipe.recipe.id,
    )))
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
    .map_err(|payload| {
        format!(
            "Recipe database task panicked: {}",
            crate::storage::panic_message(&*payload)
        )
    })?
}

#[allow(clippy::too_many_arguments)]
fn build_execution_snapshot_from_files(
    orch: &Orchestrator,
    project_path: Option<&Path>,
    recipe_id: &str,
    issue_id: Option<&str>,
    project_id: &str,
    trigger_type: TriggerType,
    event_payload: Option<serde_json::Value>,
    plan: &LaunchSelectionPlan,
) -> Result<ExecutionSnapshot, String> {
    let config_dir = orch.config_dir.as_path();
    let recipe = get_recipe_from_files(config_dir, project_path, recipe_id)?;
    // Every launch path builds its snapshot here, so this is where a decision
    // node with nothing to answer it is refused: at launch, never mid-run
    // where it would only block.
    orch.refuse_unanswerable_decisions(&recipe.nodes, Some(project_id))?;
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
            let routing = plan.for_agent(&agent_id, file_agent.tier.as_ref().map(Model::as_str));
            let snapshot =
                resolve_agent_snapshot(&file_agent, routing.selection.as_ref(), &presets)?;
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
            decision_config: None,
            context_config: None,
        }
    }

    /// A plan that routes nothing, for the rows these tests are about.
    fn plan() -> LaunchSelectionPlan {
        LaunchSelectionPlan::empty(None)
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
            icon: None,
            bundles: Vec::new(),
            is_project_scoped: true,
            file_path: std::path::PathBuf::new(),
            seed_budget_tokens: None,
            seed_eras: None,
        }
    }

    #[test]
    fn resolved_row_carries_nonempty_available() {
        let presets = default_presets_config(Some(31999));
        let nodes = [agent_node("n1", "builder")];
        let overrides = LaunchOverrides::default();
        let out = resolve_launch_nodes(
            &nodes,
            &presets,
            &overrides,
            &plan(),
            |_id| Ok(Some(file_agent("builder", Some("md")))),
            |_| Ok(()),
        );
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
        let out = resolve_launch_nodes(
            &nodes,
            &presets,
            &overrides,
            &plan(),
            |_id| Ok(Some(file_agent("weird", Some("xl")))),
            |_| Ok(()),
        );
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
        let out = resolve_launch_nodes(
            &nodes,
            &presets,
            &overrides,
            &plan(),
            |_id| Ok(Some(file_agent("builder", Some("md")))),
            |_| Ok(()),
        );
        let resolved = out[0].resolved.as_ref().unwrap();
        assert_eq!(resolved.selection.model.as_str(), "custom-xl");
        assert_eq!(resolved.selection.backend, "claude");
        assert!(out[0]
            .available
            .iter()
            .any(|s| s.model.as_str() == "custom-xl"));
    }

    /// The regression guard for generation zero: with no labels and no rules,
    /// every preview row resolves exactly as it did before routing existed, and
    /// says so rather than staying silent.
    #[test]
    fn an_unrouted_row_resolves_to_the_agent_default_and_records_why() {
        let presets = default_presets_config(Some(31999));
        let nodes = [agent_node("n1", "builder")];
        let overrides = LaunchOverrides::default();
        let out = resolve_launch_nodes(
            &nodes,
            &presets,
            &overrides,
            &plan(),
            |_id| Ok(Some(file_agent("builder", Some("md")))),
            |_| Ok(()),
        );
        let resolved = out[0].resolved.as_ref().unwrap();
        assert_eq!(resolved.selection.model.as_str(), "sonnet");
        let routing = resolved.routing.as_ref().expect("every row records why");
        assert_eq!(
            routing.source,
            crate::models::ModelRoutingSource::AgentDefault
        );
        assert_eq!(routing.note, "no labels on issue");
    }

    /// The composer's preview applies the same plan the start path will, so the
    /// operator sees the routed model -- and its reason -- before pressing Start.
    #[test]
    fn a_matching_rule_moves_the_preview_row_and_names_itself() {
        let presets = default_presets_config(Some(31999));
        let table: crate::config::model_routing::ModelRoutingTable = serde_yaml::from_str(
            "rules:\n  - id: migration-builder\n    when:\n      all: [migration]\n    agents: [builder]\n    tier: lg\n",
        )
        .expect("test table parses");
        let labels = IssueLabels::new([("migration".to_string(), "Migration".to_string())]);
        let routed = LaunchSelectionPlan::new(
            None,
            table,
            labels,
            std::collections::HashSet::new(),
            &presets,
        );

        let nodes = [agent_node("n1", "builder"), agent_node("n2", "review")];
        let overrides = LaunchOverrides::default();
        let out = resolve_launch_nodes(
            &nodes,
            &presets,
            &overrides,
            &routed,
            |id| Ok(Some(file_agent(id, Some("md")))),
            |_| Ok(()),
        );

        let builder = out[0].resolved.as_ref().unwrap();
        assert_eq!(builder.selection.model.as_str(), "opus");
        let routing = builder.routing.as_ref().expect("a routed row says why");
        assert_eq!(
            routing.source,
            crate::models::ModelRoutingSource::LabelBinding
        );
        assert_eq!(routing.rule.as_deref(), Some("migration-builder"));
        assert_eq!(routing.tier.as_deref(), Some("lg"));

        // The rule is scoped to the builder; nothing else moves.
        let review = out[1].resolved.as_ref().unwrap();
        assert_eq!(review.selection.model.as_str(), "sonnet");
    }

    /// A per-node override in the preview payload is an explicit choice: the
    /// table is not consulted for that row at all.
    #[test]
    fn a_node_override_is_never_contested_by_a_rule() {
        let presets = default_presets_config(Some(31999));
        let table: crate::config::model_routing::ModelRoutingTable = serde_yaml::from_str(
            "rules:\n  - id: migration-builder\n    when:\n      all: [migration]\n    tier: lg\n",
        )
        .unwrap();
        let routed = LaunchSelectionPlan::new(
            None,
            table,
            IssueLabels::new([("migration".to_string(), "Migration".to_string())]),
            std::collections::HashSet::new(),
            &presets,
        );
        let nodes = [agent_node("n1", "builder")];
        let mut overrides = LaunchOverrides::default();
        overrides.nodes.insert(
            "n1".to_string(),
            LaunchSelectionOverride::Tier("sm".to_string()),
        );
        let out = resolve_launch_nodes(
            &nodes,
            &presets,
            &overrides,
            &routed,
            |_id| Ok(Some(file_agent("builder", Some("md")))),
            |_| Ok(()),
        );
        let resolved = out[0].resolved.as_ref().unwrap();
        assert_eq!(resolved.selection.model.as_str(), "haiku");
        assert!(
            resolved.routing.is_none(),
            "a row the operator chose carries no routing reason"
        );
    }

    #[test]
    fn missing_agent_is_per_node_error() {
        let presets = default_presets_config(Some(31999));
        let nodes = [agent_node("n1", "ghost")];
        let overrides = LaunchOverrides::default();
        let out = resolve_launch_nodes(
            &nodes,
            &presets,
            &overrides,
            &plan(),
            |_id| Ok(None),
            |_| Ok(()),
        );
        assert!(out[0].error.as_ref().unwrap().contains("not found"));
        assert!(out[0].resolved.is_none());
        assert!(!out[0].available.is_empty());
    }

    #[test]
    fn a_selection_that_cannot_hold_a_session_is_never_offered_and_a_row_landing_on_it_says_so() {
        let mut presets = default_presets_config(Some(31999));
        presets
            .backends
            .entry("codex".to_string())
            .or_default()
            .insert(
                "lg".to_string(),
                crate::models::Preset {
                    model: Model::new("gpt-image-2"),
                    options: Default::default(),
                },
            );
        let image_only = |selection: &ModelSelection| {
            if selection.model.as_str() == "gpt-image-2" {
                Err("'gpt-image-2' on codex serves only image".to_string())
            } else {
                Ok(())
            }
        };
        assert!(available_selections(&presets)
            .iter()
            .any(|s| s.model.as_str() == "gpt-image-2"));

        let nodes = [agent_node("n1", "builder"), agent_node("n2", "review")];
        let mut overrides = LaunchOverrides::default();
        overrides.nodes.insert(
            "n1".to_string(),
            LaunchSelectionOverride::Tier("codex/lg".to_string()),
        );
        let out = resolve_launch_nodes(
            &nodes,
            &presets,
            &overrides,
            &plan(),
            |id| Ok(Some(file_agent(id, Some("md")))),
            image_only,
        );

        assert!(out[0].resolved.is_none());
        assert_eq!(
            out[0].error.as_deref(),
            Some("the 'codex/lg' tier resolves to gpt-image-2 on codex: 'gpt-image-2' on codex serves only image")
        );
        for row in &out {
            assert!(!row.available.is_empty());
            assert!(row
                .available
                .iter()
                .all(|s| s.model.as_str() != "gpt-image-2"));
        }
        assert_eq!(
            out[1].resolved.as_ref().unwrap().selection.model.as_str(),
            "sonnet"
        );
    }
}
