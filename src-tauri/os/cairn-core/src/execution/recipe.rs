//! Recipe execution tracking and management.
//!
//! Core DB and config operations for recipe executions — listing, creating,
//! and managing the lifecycle of recipe execution records.
//!
//! Execution creation functions.
//! Functions taking `&Orchestrator` also need config/settings access.

use cairn_common::ids;
use cairn_models::types::catalog::VerbKind;
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
use crate::config::{agents as config_agents, skills as config_skills, ConfigResult};
use crate::execution::canonical;
use crate::execution::model_selection::{ResolvedConstraints, SelectionRecord};
use crate::execution::Initiator;
use crate::models::{
    BranchTarget, Execution, ExecutionSnapshot, ExecutionStatus, Fence, Job, LaunchCustomization,
    ModelSelection, RecipeEdgeType, RecipeNode, RecipeNodeType, RecipeSnapshot, RuntimeExtras,
    SkillSnapshot, TriggerContext, TriggerType,
};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use cairn_config::allotment::{model_ref, parse_model_ref, AllotmentEntry};
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

/// Who is starting an execution, as the execution queue judges it
/// (`execution::gates::capacity`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Starter {
    /// The operator, proven by the invoke's credentials
    /// (`has_operator_authority`): the composer's start. Takes a slot
    /// immediately, even over either cap, and still counts toward both.
    Operator,
    /// Everyone else: an agent's or the CLI's write to an executions
    /// collection, an issue created with an execution, the route dispatcher, a
    /// schedule. Holds while a cap is full, and starts on its own when it
    /// clears.
    Capped,
}

pub async fn create_jobs_for_execution(
    orch: &Orchestrator,
    execution_id: &str,
) -> Result<Vec<Job>, String> {
    let db = crate::execution::routing::owning_db_for_execution(&orch.db, execution_id)
        .await
        .map_err(|e| e.to_string())?;
    let execution_id = execution_id.to_string();
    db.write(|conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            crate::execution::advancement::create_jobs_for_execution_conn(conn, &execution_id).await
        })
    })
    .await
    .map_err(|e| format!("Failed to create jobs for execution: {e}"))
}

/// What a launch did: the execution, and, when the execution queue held it
/// without `after:` gates of its own, the caps that hold it as the admission
/// decision found them. A filing reports those without deciding again.
pub(crate) struct Launched {
    pub execution: Execution,
    pub held_by: Vec<crate::models::CapacityHold>,
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
///
/// A [`Starter::Capped`] launch can also come back `Held` with no `after:`
/// gates at all: the execution queue's caps hold it, the same way. That hold
/// pokes no sweep: a new hold frees nothing, and the admission decision that
/// made it is returned in [`Launched::held_by`].
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_recipe_execution_and_advance(
    orch: &Orchestrator,
    issue_id: &str,
    recipe_id: Option<&str>,
    project_id: &str,
    initiated_via: Option<&str>,
    branch_target: Option<BranchTarget>,
    continuity: Option<Continuity>,
    customization: Option<LaunchCustomization>,
    trigger_type: TriggerType,
    intent: LaunchIntent,
    after: &[crate::routes::TriggerClause],
    starter: Starter,
    place: QueuePlace,
) -> Result<Launched, String> {
    let gates = crate::execution::gates::authoring::canonical_predicates(after, "after")?;
    let launched = launch_recipe_execution(
        orch,
        issue_id,
        recipe_id,
        project_id,
        customization,
        None,
        initiated_via,
        None,
        branch_target,
        continuity,
        trigger_type,
        intent,
        &gates,
        starter,
        place,
    )
    .await?;
    let execution = &launched.execution;
    let execution_change =
        crate::notify::execution_db_change_ids("insert", &execution.id, Some(issue_id), project_id);
    if execution.status == ExecutionStatus::Held {
        let _ = orch.services.emitter.emit("db-change", execution_change);
        if !gates.is_empty() {
            crate::execution::gates::sweep::notify_gate_evidence_changed();
        }
        return Ok(launched);
    }
    let jobs = create_jobs_for_execution(orch, &execution.id).await?;
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
    Ok(launched)
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
pub async fn start_recipe_execution_impl(
    orch: &Orchestrator,
    issue_id: &str,
    recipe_id: Option<&str>,
    project_id: &str,
    customization: Option<LaunchCustomization>,
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
    // cairn/5802: whether the execution queue's caps may hold this start. The
    // returned execution is `Held` when they do, and the caller must then
    // create no jobs.
    starter: Starter,
) -> Result<Execution, String> {
    launch_recipe_execution(
        orch,
        issue_id,
        recipe_id,
        project_id,
        customization,
        initiator,
        initiated_via,
        runner_device_id,
        branch_target,
        continuity,
        trigger_type,
        intent,
        &[],
        starter,
        QueuePlace::Tail,
    )
    .await
    .map(|launched| launched.execution)
}

/// Where a capped start that holds takes its place in line (cairn/5848).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum QueuePlace {
    /// Behind everything already held.
    #[default]
    Tail,
    /// At the head of its thread's own cap-held set: the earliest places that
    /// set already occupies, never ahead of another thread's work. `placed_by`
    /// is who asked, as the hold's hover names it.
    Head { placed_by: String },
}

/// [`start_recipe_execution_impl`] with `after:` gates: `gates` are canonical
/// predicate JSON, already validated. Non-empty gates insert the execution
/// HELD, with its gate rows, in the same transaction as the insert — a held
/// execution must never be observable without the gates that make it held,
/// because with no gates and no jobs the projection derives `complete`.
#[allow(clippy::too_many_arguments)]
async fn launch_recipe_execution(
    orch: &Orchestrator,
    issue_id: &str,
    recipe_id: Option<&str>,
    project_id: &str,
    customization: Option<LaunchCustomization>,
    initiator: Option<Initiator>,
    initiated_via: Option<&str>,
    runner_device_id: Option<String>,
    branch_target: Option<BranchTarget>,
    continuity: Option<Continuity>,
    trigger_type: TriggerType,
    intent: LaunchIntent,
    gates: &[String],
    starter: Starter,
    place: QueuePlace,
) -> Result<Launched, String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let this_device = orch.anon_device_manager.device_id();
    let owner_device = runner_device_id.unwrap_or_else(|| this_device.clone());

    // Resolve the owning database ONCE (fail-closed): a team project's rows live
    // wholly in its synced replica, so the execution and its jobs must be created
    // there, not in the private DB. The id-keyed resolvers cannot be used yet —
    // the execution row does not exist — so resolve by project id.
    let db = resolve_owning_db_for_project(orch, project_id).await?;

    // CAIRN-2629: if a PEER device was chosen as the runner, enforce that it has a
    // local clone of this project before stamping ownership on it — otherwise its
    // sweep would claim the job and fail in prepare_job with no clone. This is the
    // authoritative guard (the picker filters as a convenience). This machine is
    // trusted (the launch flow already blocks starting without a clone).
    if owner_device != this_device {
        let key = crate::execution::ownership::project_key(&db, project_id)
            .await
            .ok_or_else(|| "Project not found".to_string())?;
        let can_run = crate::execution::ownership::device_has_clone(&db, &owner_device, &key).await;
        if !can_run {
            return Err(format!(
                "Selected runner device {owner_device} has no local clone of this project"
            ));
        }
    }

    // Get project path for file-based recipe loading
    let project_path = project_path_for_recipe(&db, project_id).await?;

    // Get the recipe (use provided or find default)
    let recipe = match recipe_id {
        Some(id) => get_selected_recipe(&orch.config_dir, &project_path, project_id, id)?,
        None => return Err("No recipe specified for execution".to_string()),
    };

    // The snapshot carries each agent's allotment, and a pin only where an
    // operator chose one. Each node selects its model at its own launch.
    let mut snapshot = build_execution_snapshot_from_files(
        orch,
        Some(&project_path),
        &recipe.id,
        Some(issue_id),
        project_id,
        trigger_type.clone(),
        None,
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
                        Ok(Some(file_agent)) => Ok(Some(file_agent.snapshot())),
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
            // Composer output is stored verbatim: a pin with its extras, or an
            // allotment the node selects within at launch.
            for (agent_id, agent_snapshot) in agents_override {
                snapshot.agents.insert(agent_id, agent_snapshot);
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
                    snapshot
                        .agents
                        .insert(agent_id.clone(), file_agent.snapshot());
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

    // A constraint reads an upstream node's stored selection at launch, so the
    // graph the execution will actually run is checked now, before any row
    // exists, rather than refusing mid-run.
    check_node_constraints(&snapshot.recipe)?;

    let snapshot_json = snapshot.to_json()?;

    // Gates whose evidence is already in wait on nothing, so the cycle refusal
    // must not count them. Resolved before the transaction: evidence reads route
    // to other databases.
    let satisfied = if gates.is_empty() {
        std::collections::HashSet::new()
    } else {
        crate::execution::gates::authoring::already_satisfied(orch, gates).await
    };

    // The execution queue (cairn/5802). A capped start with no `after:` gates
    // is decided here, under the admission lock, which is held until the
    // insert below commits so no other admission can take the same slot in
    // between. One with `after:` gates files as before: its caps are decided
    // when those gates clear, so it takes no place in line while it waits on
    // something else.
    let (admission_guard, caps, turn) = if starter == Starter::Capped && gates.is_empty() {
        // A busy lock refuses the filing before anything is written, so the
        // caller can retry it as it stands.
        let guard = orch
            .admission_lock
            .acquire(
                crate::execution::gates::capacity::AdmissionSite::Filing,
                issue_id,
            )
            .await
            .map_err(|busy| busy.to_string())?;
        let admission =
            crate::execution::gates::capacity::decide(orch, &db, Some(issue_id), None).await?;
        (Some(guard), admission.blocks, Some(admission.turn))
    } else {
        (None, Vec::new(), None)
    };
    let held = !gates.is_empty() || !caps.is_empty();

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
            status: if held { "held" } else { "running" }.to_string(),
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
            caps: caps.clone(),
            filed_at: orch.services.clock.now(),
            place,
            turn,
        },
    )
    .await?;
    drop(admission_guard);

    // Phase 2, after the fence has committed.
    stop_superseded_agents(orch, &launch.supersessions);

    let execution = Execution {
        id: exec_id,
        recipe_id: recipe.id,
        issue_id: Some(issue_id.to_string()),
        project_id: None,
        status: if held {
            ExecutionStatus::Held
        } else {
            ExecutionStatus::Running
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
    };
    Ok(Launched {
        execution,
        held_by: caps,
    })
}

/// Start executing a recipe manually (no issue, just project).
///
/// Creates an execution record for a manual trigger. Caller is responsible for
/// creating jobs and advancing the DAG.
pub async fn start_manual_execution_impl(
    orch: &Orchestrator,
    recipe_id: &str,
    project_id: &str,
    initiator: Option<Initiator>,
    branch_target: Option<BranchTarget>,
) -> Result<Execution, String> {
    let now = chrono::Utc::now().timestamp() as i32;

    let db = resolve_owning_db_for_project(orch, project_id).await?;

    // Get project path for file-based recipe loading
    let project_path = project_path_for_recipe(&db, project_id).await?;

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
    )
    .await?;

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
pub(crate) async fn start_event_triggered_execution(
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

    let db = resolve_owning_db_for_project(orch, project_id).await?;

    // Build execution snapshot with event payload.
    let project_path = project_path_for_recipe(&db, project_id).await?;
    let snapshot = build_execution_snapshot_from_files(
        orch,
        Some(&project_path),
        &recipe.id,
        issue_id,
        project_id,
        trigger_type,
        Some(event_payload),
    )?;
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
            )
            .await?;
            stop_superseded_agents(orch, &launch.supersessions);
            (Some(launch.seq), launch.lifecycle)
        }
        None => {
            insert_execution(db.clone(), new_execution).await?;
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

/// Refuse a graph in which a node's constraint names anything but an agent
/// node upstream of it. Recipe files are checked when they load; this checks
/// the graph a launch will actually run, which a composer may have edited.
pub fn check_node_constraints(recipe: &RecipeSnapshot) -> Result<(), String> {
    let problems: Vec<String> = recipe
        .nodes
        .iter()
        .filter_map(|node| constraint_problem(recipe, node))
        .collect();
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems.join("\n"))
    }
}

fn constraint_problem(recipe: &RecipeSnapshot, node: &RecipeNode) -> Option<String> {
    let constraints = &node.agent_config.as_ref()?.constraints;
    if constraints.is_empty() {
        return None;
    }
    let upstream = upstream_of(recipe, &node.id);
    constraints
        .check_references(&node.id, |id| {
            upstream.contains(id)
                && recipe
                    .nodes
                    .iter()
                    .any(|other| other.id == id && other.node_type == RecipeNodeType::Agent)
        })
        .err()
}

/// Every node an edge path of either kind leads from to `node_id`.
fn upstream_of(recipe: &RecipeSnapshot, node_id: &str) -> std::collections::HashSet<String> {
    let mut seen = std::collections::HashSet::new();
    let mut frontier = vec![node_id.to_string()];
    while let Some(current) = frontier.pop() {
        for edge in &recipe.edges {
            if matches!(
                edge.edge_type,
                RecipeEdgeType::Control | RecipeEdgeType::Context
            ) && edge.target_node_id == current
                && seen.insert(edge.source_node_id.clone())
            {
                frontier.push(edge.source_node_id.clone());
            }
        }
    }
    seen
}

// ===========================================================================
// Launch-composer preview contract
// ===========================================================================
//
// `resolve_recipe_launch` is the single entry point a launch-composer UI calls
// to preview a recipe's per-agent settings before starting it. Nothing is
// chosen here: each node selects its own model at its own launch. A row says
// what the node would pick now and why (`preview`), or that it is pinned, or
// carries a loud per-node `error` when it cannot launch. The whole plan only
// fails (`Err`) for systemic problems (recipe not found, project missing).

/// A launch-time override for one agent node: an allotment the node selects
/// within at its launch, or a concrete pin it runs verbatim.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", content = "value")]
pub enum LaunchSelectionOverride {
    Allotment(Vec<AllotmentEntry>),
    Concrete(ModelSelection),
}

/// What one node would run if it launched now, and why.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectionPreview {
    /// The allotment the node selects within: the launch override, else the
    /// node's own, else its agent's, else the workspace default. Empty for a
    /// pinned node.
    allotment: Vec<AllotmentEntry>,
    /// The node runs exactly the row's `selection`; no selection is made.
    pinned: bool,
    /// The would-pick-now selection. Absent for a pin the catalog cannot
    /// place.
    #[serde(skip_serializing_if = "Option::is_none")]
    record: Option<SelectionRecord>,
    /// The upstream nodes (by name) a constrained preview read a predicted
    /// pick from: the real choice waits for theirs.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    depends_on: Vec<String>,
    /// The row's one-line answer to "what would this run, and why".
    line: String,
}

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
    /// Models a pin may name: every ladder entry and unranked model that can
    /// hold a session, unioned with the row's own selection. Present on every
    /// node, including error rows, so an unplaceable node can still be pinned.
    available: Vec<ModelSelection>,
}

/// One agent node's preview.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedAgentRow {
    /// The model the node would run now: its pin, or the preview's choice.
    selection: ModelSelection,
    extras: RuntimeExtras,
    #[serde(skip_serializing_if = "Option::is_none")]
    fence: Option<Fence>,
    preview: SelectionPreview,
    available: Vec<ModelSelection>,
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

/// Preview a recipe's per-agent-node launch settings, with loud per-node
/// errors. The composer's contract.
pub async fn resolve_recipe_launch(
    orch: &Orchestrator,
    recipe_id: &str,
    project_id: &str,
    _issue_id: Option<&str>,
    overrides: Option<LaunchOverrides>,
) -> Result<LaunchPlan, String> {
    let db = resolve_owning_db_for_project(orch, project_id).await?;
    let project_path = project_path_for_recipe(&db, project_id).await?;
    let recipe = get_selected_recipe(&orch.config_dir, &project_path, project_id, recipe_id)?;
    let overrides = overrides.unwrap_or_default();

    // A composer that has edited the recipe structure (append/remove/graph-edit)
    // supplies the working RecipeSnapshot; preview against it so appended nodes
    // preview authoritatively. Otherwise preview the file-loaded recipe.
    let recipe_snapshot = overrides.recipe.clone().unwrap_or_else(|| RecipeSnapshot {
        id: recipe.id.clone(),
        name: recipe.name.clone(),
        description: recipe.description.clone(),
        trigger: recipe.trigger.clone(),
        nodes: recipe.nodes.clone(),
        edges: recipe.edges.clone(),
    });

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
    let ladder = orch.models_ladder(Some(project_id))?;
    let mut offered: Vec<ModelSelection> = Vec::new();
    for raw in ladder
        .rungs
        .iter()
        .map(|rung| rung.model.as_str())
        .chain(ladder.unranked.iter().map(String::as_str))
    {
        if let Ok(selection) = parse_model_ref(raw) {
            if !offered.contains(&selection) && serves_session(&selection).is_ok() {
                offered.push(selection);
            }
        }
    }
    let nodes = resolve_launch_nodes(
        &recipe_snapshot,
        &overrides,
        &ladder.default_models,
        offered,
        |agent_id| {
            config_agents::get_agent(&config_dir, agent_id, Some(&project_path))
                .map_err(|e| e.to_string())
        },
        |allotment, constraints| {
            orch.select_models(
                Some(project_id),
                allotment,
                VerbKind::Session,
                constraints,
                Default::default(),
            )
            .map_err(String::from)
        },
        serves_session,
    );

    Ok(LaunchPlan {
        recipe: recipe_snapshot,
        nodes,
    })
}

/// The node's own preview before constraints are applied.
struct NodePreview {
    agent: config_agents::FileAgent,
    allotment: Vec<AllotmentEntry>,
    pin: Option<ModelSelection>,
    record: Result<SelectionRecord, String>,
}

/// Pure per-node previewer: maps recipe agent nodes to preview rows using a
/// supplied agent loader and selection. Every node carries `available` so even
/// an unplaceable row can be pinned. Loud, per-node errors; never fails the
/// whole plan.
///
/// Nodes are previewed in two passes: each node on its own, then each
/// constrained node again against the first-pass pick of the nodes it names,
/// which is the best prediction of what those nodes will store at launch.
fn resolve_launch_nodes(
    recipe: &RecipeSnapshot,
    overrides: &LaunchOverrides,
    default_models: &[AllotmentEntry],
    offered: Vec<ModelSelection>,
    load_agent: impl Fn(&str) -> Result<Option<config_agents::FileAgent>, String>,
    select: impl Fn(&[AllotmentEntry], &ResolvedConstraints) -> Result<SelectionRecord, String>,
    serves_session: impl Fn(&ModelSelection) -> Result<(), String>,
) -> Vec<ResolvedLaunchNode> {
    let union = |selection: Option<&ModelSelection>| -> Vec<ModelSelection> {
        let mut list = offered.clone();
        if let Some(selection) = selection {
            if !list.contains(selection) {
                list.push(selection.clone());
            }
        }
        list
    };
    let agent_nodes: Vec<(&RecipeNode, String)> = recipe
        .nodes
        .iter()
        .filter(|node| node.node_type == RecipeNodeType::Agent)
        .filter_map(|node| {
            let agent_id = node.agent_config.as_ref()?.agent_config_id.clone()?;
            Some((node, agent_id))
        })
        .collect();

    // First pass: every node on its own.
    let mut first: HashMap<String, Result<NodePreview, String>> = HashMap::new();
    for (node, agent_id) in &agent_nodes {
        let preview = match load_agent(agent_id) {
            Ok(Some(agent)) => {
                let node_models = node.agent_config.as_ref().and_then(|c| c.models.clone());
                let (allotment, pin) = match overrides.nodes.get(&node.id) {
                    Some(LaunchSelectionOverride::Concrete(pin)) => (Vec::new(), Some(pin.clone())),
                    Some(LaunchSelectionOverride::Allotment(allotment)) => {
                        (allotment.clone(), None)
                    }
                    None => (
                        crate::execution::model_selection::effective_allotment(
                            node_models.as_deref(),
                            &agent.models,
                            default_models,
                        ),
                        None,
                    ),
                };
                let record = match &pin {
                    Some(pin) => serves_session(pin)
                        .map_err(|why| format!("{}: {why}", model_ref(pin)))
                        .and_then(|()| {
                            select(
                                &[AllotmentEntry::Model(pin.clone())],
                                &ResolvedConstraints::default(),
                            )
                        }),
                    None => select(&allotment, &ResolvedConstraints::default()),
                };
                Ok(NodePreview {
                    agent,
                    allotment,
                    pin,
                    record,
                })
            }
            Ok(None) => Err(format!("Agent config not found: {agent_id}")),
            Err(e) => Err(format!("Failed to load agent '{agent_id}': {e}")),
        };
        first.insert(node.id.clone(), preview);
    }

    let name_of = |id: &str| {
        recipe
            .nodes
            .iter()
            .find(|node| node.id == id)
            .map_or_else(|| id.to_string(), |node| node.name.clone())
    };
    let picks: HashMap<String, SelectionRecord> = first
        .iter()
        .filter_map(|(id, preview)| {
            let record = preview.as_ref().ok()?.record.as_ref().ok()?;
            Some((id.clone(), record.clone()))
        })
        .collect();
    let picked = |id: &str| picks.get(id).cloned();

    let mut nodes = Vec::new();
    for (node, agent_id) in &agent_nodes {
        let preview = match first.remove(&node.id) {
            Some(Ok(preview)) => preview,
            failed => {
                let error = match failed {
                    Some(Err(error)) => error,
                    _ => format!("Agent config not found: {agent_id}"),
                };
                nodes.push(ResolvedLaunchNode {
                    node_id: node.id.clone(),
                    agent_id: agent_id.clone(),
                    agent_name: agent_id.clone(),
                    prompt_present: false,
                    resolved: None,
                    error: Some(error),
                    available: union(None),
                });
                continue;
            }
        };
        let NodePreview {
            agent,
            allotment,
            pin,
            record,
        } = preview;

        // Second pass: a constrained node reads its upstream's predicted pick.
        let constraints = node
            .agent_config
            .as_ref()
            .map(|config| config.constraints.clone())
            .unwrap_or_default();
        let mut depends_on = Vec::new();
        let constraint_error = if pin.is_none() && !constraints.is_empty() {
            constraint_problem(recipe, node)
        } else {
            None
        };
        let record = if pin.is_some() || constraints.is_empty() || constraint_error.is_some() {
            record
        } else {
            let mut resolved = ResolvedConstraints::default();
            for (slot, reference) in [
                (&mut resolved.same_band_as, &constraints.same_band_as),
                (
                    &mut resolved.different_maker_from,
                    &constraints.different_maker_from,
                ),
            ] {
                if let Some(reference) = reference {
                    let name = name_of(reference);
                    if !depends_on.contains(&name) {
                        depends_on.push(name.clone());
                    }
                    // The provenance names the upstream node as the recipe
                    // shows it, not by its id.
                    *slot = picked(reference).map(|record| (name, record));
                }
            }
            select(&allotment, &resolved)
        };

        let row = match (constraint_error, record, &pin) {
            (Some(error), _, _) => Err(error),
            (None, Ok(record), _) => Ok(record),
            (None, Err(_), Some(pin)) if serves_session(pin).is_ok() => Err(String::new()),
            (None, Err(error), _) => Err(error),
        };
        let (resolved, error) = match (row, pin) {
            (Ok(record), pin) => {
                let selection = pin.clone().unwrap_or_else(|| record.chosen.clone());
                let line = match (&pin, depends_on.is_empty()) {
                    (Some(pin), _) => format!("pinned to {}", model_ref(pin)),
                    (None, true) => record.provenance.clone(),
                    (None, false) => format!(
                        "depends on {}: would pick now {}",
                        depends_on.join(", "),
                        record.provenance
                    ),
                };
                (
                    Some(ResolvedAgentRow {
                        extras: if pin.is_some() {
                            RuntimeExtras::default()
                        } else {
                            record.extras.clone()
                        },
                        available: union(Some(&selection)),
                        selection,
                        fence: agent.fence,
                        preview: SelectionPreview {
                            allotment: if pin.is_some() { Vec::new() } else { allotment },
                            pinned: pin.is_some(),
                            record: Some(record),
                            depends_on,
                            line,
                        },
                    }),
                    None,
                )
            }
            // A pin the selection cannot place but the session door admits
            // still runs as pinned; only its provenance is missing.
            (Err(error), Some(pin)) if error.is_empty() => (
                Some(ResolvedAgentRow {
                    extras: RuntimeExtras::default(),
                    available: union(Some(&pin)),
                    fence: agent.fence,
                    preview: SelectionPreview {
                        allotment: Vec::new(),
                        pinned: true,
                        record: None,
                        depends_on,
                        line: format!("pinned to {}", model_ref(&pin)),
                    },
                    selection: pin,
                }),
                None,
            ),
            (Err(error), _) => (None, Some(error)),
        };
        let available = resolved
            .as_ref()
            .map(|row| row.available.clone())
            .unwrap_or_else(|| union(None));
        nodes.push(ResolvedLaunchNode {
            node_id: node.id.clone(),
            agent_id: agent_id.clone(),
            agent_name: agent.name.clone(),
            prompt_present: !agent.prompt.is_empty(),
            resolved,
            error,
            available,
        });
    }
    nodes
}

async fn project_path_for_recipe(
    db: &LocalDb,
    project_id: &str,
) -> Result<std::path::PathBuf, String> {
    project_repo_path(db, project_id)
        .await?
        .map(std::path::PathBuf::from)
        .ok_or_else(|| format!("Project not found: {}", project_id))
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
pub(crate) async fn file_held_with_observed_satisfaction(
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
            caps: Vec::new(),
            filed_at: orch.services.clock.now(),
            place: QueuePlace::Tail,
            turn: None,
        },
    )
    .await?;
    Ok(exec_id)
}

/// The `after:` gates a launch is filed with: their canonical predicate JSON,
/// and the subset the satisfaction preflight already observed satisfied. The
/// two travel together because that observation is what waives a predicate's
/// wait edge, and it is latched on the gate row in the same transaction.
///
/// `caps` are the execution queue's holds the admission decision found, filed
/// as capacity gate rows in the same transaction. `filed_at` is the services
/// clock's instant, stamped on every gate row: it is the execution's place in
/// the queue (oldest held first), so it reads the one clock everything else
/// does rather than the wall clock.
///
/// `place` is where a capped hold takes its place in line: a `Head` filing
/// rotates its thread's cap-held set in the same transaction, so the hold is
/// never visible at the tail.
///
/// `turn` is the stamp the admission decision minted for a capped filing. One
/// that starts at once spends its lane's turn in the same transaction, as a
/// release does (`gates::rotation`).
#[derive(Default)]
struct LaunchGates {
    predicates: Vec<String>,
    satisfied: std::collections::HashSet<String>,
    caps: Vec<crate::models::CapacityHold>,
    filed_at: i64,
    place: QueuePlace,
    turn: Option<i64>,
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
async fn insert_issue_execution_atomic(
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
        caps,
        filed_at,
        place,
        turn,
    } = gates;
    if !gates.is_empty() && intent == LaunchIntent::AutomaticTrigger {
        return Err(
            "an automatic launch cannot be held: `after:` gates are set by whoever files the \
             execution"
                .to_string(),
        );
    }
    let own_db = db.clone();
    db.write(move |conn| {
        let issue_id = issue_id.clone();
        let actor = actor.clone();
        let gates = gates.clone();
        let satisfied = satisfied.clone();
        let caps = caps.clone();
        let place = place.clone();
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
            let held = !gates.is_empty() || !caps.is_empty();

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
                LaunchIntent::ParallelAlternative => crate::models::ExecutionLifecycle::Parallel,
                LaunchIntent::AutomaticTrigger => {
                    if crate::execution::canonical::issue_has_canonical_execution(conn, &issue_id)
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
            if let (Some(turn), false) = (turn, held) {
                crate::execution::gates::rotation::record_turn_conn(
                    conn,
                    Some(&issue_id),
                    execution.id.as_str(),
                    turn,
                    filed_at,
                )
                .await?;
            }
            for cap in &caps {
                use crate::execution::gates::capacity::{
                    capacity_predicate, encode_hold, CapacityHoldExt,
                };
                cairn_workflow::gates::store::hold_capacity_gate_conn(
                    conn,
                    execution.id.as_str(),
                    &capacity_predicate(cap.fact()),
                    cap.seq(),
                    &encode_hold(cap),
                    filed_at,
                )
                .await?;
            }
            // A filing at the head of its thread's queue takes the earliest
            // places its thread's cap-held set holds, in this transaction:
            // statements beside the inserts, under the lock the filing
            // already holds, and nothing that waits on anything else.
            if let (QueuePlace::Head { placed_by }, false) = (&place, caps.is_empty()) {
                let thread_id = crate::storage::query_opt_text_conn(
                    conn,
                    "SELECT parent_thread_id FROM issues WHERE id = ?1",
                    params![issue_id.as_str()],
                )
                .await?;
                if let Some(thread_id) = thread_id {
                    crate::execution::gates::reorder::rotate_to_front_conn(
                        conn,
                        &thread_id,
                        std::slice::from_ref(&execution.id),
                        placed_by,
                    )
                    .await?;
                }
            }
            if held {
                let gate_ids = cairn_workflow::gates::store::insert_gates_conn(
                    conn,
                    execution.id.as_str(),
                    &gates,
                    filed_at,
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
                crate::transitions::outcome::recompute_issue_status_conn(conn, &issue_id).await?;
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
async fn insert_execution(db: Arc<LocalDb>, execution: NewExecution) -> Result<(), String> {
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
}

/// The recipe id a filing names, as its launch will resolve it.
///
/// The id of the file [`crate::config::recipes::get_recipe`] loads for it in
/// this project, which is what the launch snapshots: `Memory-Triage` comes
/// back as the file's own `memory-triage`. A path, a file name, or anything
/// else that is not a recipe id is refused here, naming the canonical id when
/// one resolves. A plain id no file answers to comes back as named, and the
/// launch then refuses it as not found. The launch is handed this id, so a
/// filing's result names the recipe the file declares, not the caller's
/// spelling.
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

/// Fail-closed resolve of a project's owning database. Used at execution-start
/// sites where no execution/job/run row exists yet, so the id-keyed resolvers
/// cannot be used.
async fn resolve_owning_db_for_project(
    orch: &Orchestrator,
    project_id: &str,
) -> Result<Arc<LocalDb>, String> {
    crate::execution::routing::owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|e| e.to_string())
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
    check_node_constraints(&recipe_snapshot)?;
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
            agents.insert(agent_id, file_agent.snapshot());
        }
    }
    let trigger_context = TriggerContext {
        issue_id: issue_id.map(str::to_string),
        project_id: project_id.to_string(),
        trigger_type,
        event_payload,
        initiated_via: None,
    };
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
    use crate::execution::model_selection::tests::{run_entries, windows};
    use crate::models::{AgentNodeConfig, Model, NodePosition, RecipeEdge, RecipeTrigger};
    use cairn_config::allotment::NodeConstraints;

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
                models: None,
                constraints: NodeConstraints::default(),
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

    fn reviewing(id: &str, builder: &str) -> RecipeNode {
        let mut node = agent_node(id, "review");
        node.agent_config.as_mut().unwrap().constraints = NodeConstraints {
            same_band_as: Some(builder.to_string()),
            different_maker_from: Some(builder.to_string()),
        };
        node
    }

    fn recipe(nodes: Vec<RecipeNode>, edges: &[(&str, &str)]) -> RecipeSnapshot {
        RecipeSnapshot {
            id: "r".to_string(),
            name: "R".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes,
            edges: edges
                .iter()
                .map(|(from, to)| RecipeEdge {
                    id: format!("{from}-{to}"),
                    edge_type: RecipeEdgeType::Control,
                    source_node_id: from.to_string(),
                    source_handle: "control-out".to_string(),
                    target_node_id: to.to_string(),
                    target_handle: "control-in".to_string(),
                })
                .collect(),
        }
    }

    fn file_agent(id: &str, models: &[&str]) -> config_agents::FileAgent {
        config_agents::FileAgent {
            id: id.to_string(),
            name: format!("Agent {id}"),
            description: String::new(),
            prompt: "do work".to_string(),
            tools: Vec::new(),
            models: cairn_config::allotment::parse_allotment(models).unwrap_or_default(),
            fence: None,
            disallowed_tools: None,
            skills: None,
            hooks: None,
            icon: None,
            bundles: Vec::new(),
            is_project_scoped: true,
            file_path: std::path::PathBuf::new(),
            seed_budget_tokens: None,
            seed_eras: None,
        }
    }

    fn offered() -> Vec<ModelSelection> {
        ["claude/sonnet", "claude/opus", "codex/gpt-5.6-sol"]
            .into_iter()
            .map(|raw| parse_model_ref(raw).unwrap())
            .collect()
    }

    fn preview(
        recipe: &RecipeSnapshot,
        overrides: &LaunchOverrides,
        agent_models: &[&str],
        serves_session: impl Fn(&ModelSelection) -> Result<(), String>,
    ) -> Vec<ResolvedLaunchNode> {
        resolve_launch_nodes(
            recipe,
            overrides,
            &[AllotmentEntry::Pool("md".into())],
            offered(),
            |id| Ok((id != "ghost").then(|| file_agent(id, agent_models))),
            |allotment, constraints| {
                run_entries(allotment, constraints, &windows).map_err(String::from)
            },
            serves_session,
        )
    }

    fn chosen(row: &ResolvedLaunchNode) -> String {
        model_ref(&row.resolved.as_ref().expect("a preview").selection)
    }

    /// A row says what its node would pick now and why, from the agent's
    /// allotment, and still offers every pin.
    #[test]
    fn a_row_previews_the_pick_with_its_provenance() {
        let out = preview(
            &recipe(vec![agent_node("n1", "builder")], &[]),
            &LaunchOverrides::default(),
            &["lg"],
            |_| Ok(()),
        );
        let row = out[0].resolved.as_ref().unwrap();
        assert_eq!(chosen(&out[0]), "codex/gpt-5.6-sol");
        assert!(!row.preview.pinned);
        assert_eq!(
            row.preview.allotment,
            vec![AllotmentEntry::Pool("lg".into())]
        );
        assert_eq!(
            row.preview.line,
            row.preview.record.as_ref().unwrap().provenance
        );
        assert_eq!(row.available.len(), 3);
    }

    /// An agent that names no allotment previews the workspace default; a
    /// launch override replaces both.
    #[test]
    fn the_default_and_an_allotment_override_are_what_the_row_selects_within() {
        let nodes = recipe(vec![agent_node("n1", "builder")], &[]);
        let out = preview(&nodes, &LaunchOverrides::default(), &[], |_| Ok(()));
        assert_eq!(
            out[0].resolved.as_ref().unwrap().preview.allotment,
            vec![AllotmentEntry::Pool("md".into())]
        );
        let mut overrides = LaunchOverrides::default();
        overrides.nodes.insert(
            "n1".into(),
            LaunchSelectionOverride::Allotment(vec![AllotmentEntry::Pool("sm".into())]),
        );
        let out = preview(&nodes, &overrides, &["lg"], |_| Ok(()));
        assert_eq!(chosen(&out[0]), "claude/haiku");
    }

    #[test]
    fn an_unknown_pool_is_a_row_error_that_can_still_be_pinned() {
        let out = preview(
            &recipe(vec![agent_node("n1", "weird")], &[]),
            &LaunchOverrides::default(),
            &["xl"],
            |_| Ok(()),
        );
        assert!(out[0].resolved.is_none());
        assert!(out[0].error.as_ref().unwrap().contains("xl"));
        assert!(!out[0].available.is_empty());
    }

    /// A concrete override runs verbatim, even one the ladder does not know.
    #[test]
    fn a_concrete_override_pins_verbatim_and_is_selectable() {
        let mut overrides = LaunchOverrides::default();
        let pin = ModelSelection::new("claude", Model::new("custom-xl"));
        overrides
            .nodes
            .insert("n1".into(), LaunchSelectionOverride::Concrete(pin.clone()));
        let out = preview(
            &recipe(vec![agent_node("n1", "builder")], &[]),
            &overrides,
            &["lg"],
            |_| Ok(()),
        );
        let row = out[0].resolved.as_ref().unwrap();
        assert_eq!(row.selection, pin);
        assert!(row.preview.pinned);
        assert_eq!(row.preview.line, "pinned to claude/custom-xl");
        assert!(out[0].available.contains(&pin));
    }

    /// A reviewer previews against its builder's predicted pick, and says
    /// that its real choice waits for the builder's.
    #[test]
    fn a_constrained_row_depends_on_its_upstream_pick() {
        let out = preview(
            &recipe(
                vec![agent_node("n1", "builder"), reviewing("n2", "n1")],
                &[("n1", "n2")],
            ),
            &LaunchOverrides::default(),
            &["lg"],
            |_| Ok(()),
        );
        assert_eq!(chosen(&out[0]), "codex/gpt-5.6-sol");
        assert_eq!(chosen(&out[1]), "claude/opus");
        let review = &out[1].resolved.as_ref().unwrap().preview;
        assert_eq!(review.depends_on, vec!["Node n1"]);
        assert!(
            review.line.starts_with("depends on Node n1: "),
            "{}",
            review.line
        );
    }

    /// In a workspace with one maker, a reviewer's `differentMakerFrom` gives
    /// way: the row previews a pick and says why, so the composer has no
    /// unplaceable row to block the launch on.
    #[test]
    fn a_single_maker_reviewer_row_previews_the_waived_maker_rule() {
        let out = preview(
            &recipe(
                vec![agent_node("n1", "builder"), reviewing("n2", "n1")],
                &[("n1", "n2")],
            ),
            &LaunchOverrides::default(),
            &["claude/opus", "claude/sonnet"],
            |_| Ok(()),
        );
        assert_eq!(out[1].error, None);
        assert_eq!(chosen(&out[1]), "claude/opus");
        let line = &out[1].resolved.as_ref().unwrap().preview.line;
        assert!(
            line.ends_with(
                "no model from a maker other than anthropic in [claude/opus, claude/sonnet]; same maker as Node n1"
            ),
            "{line}"
        );
    }

    #[test]
    fn a_constraint_naming_a_node_that_is_not_upstream_is_a_row_error() {
        let nodes = recipe(
            vec![agent_node("n1", "builder"), reviewing("n2", "n1")],
            &[("n2", "n1")],
        );
        let out = preview(&nodes, &LaunchOverrides::default(), &["lg"], |_| Ok(()));
        assert!(out[1]
            .error
            .as_ref()
            .unwrap()
            .contains("not an agent node upstream"));
        assert!(check_node_constraints(&nodes).is_err());
    }

    #[test]
    fn a_missing_agent_is_a_row_error() {
        let out = preview(
            &recipe(vec![agent_node("n1", "ghost")], &[]),
            &LaunchOverrides::default(),
            &["lg"],
            |_| Ok(()),
        );
        assert!(out[0].error.as_ref().unwrap().contains("not found"));
        assert!(!out[0].available.is_empty());
    }

    /// A pin that cannot hold a session is refused on its row, naming why.
    #[test]
    fn a_pin_that_cannot_hold_a_session_says_so() {
        let mut overrides = LaunchOverrides::default();
        overrides.nodes.insert(
            "n1".into(),
            LaunchSelectionOverride::Concrete(ModelSelection::new(
                "codex",
                Model::new("gpt-image-2"),
            )),
        );
        let out = preview(
            &recipe(vec![agent_node("n1", "builder")], &[]),
            &overrides,
            &["lg"],
            |selection| {
                if selection.model.as_str() == "gpt-image-2" {
                    Err("'gpt-image-2' on codex serves only image".to_string())
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(
            out[0].error.as_deref(),
            Some("codex/gpt-image-2: 'gpt-image-2' on codex serves only image")
        );
    }

    #[test]
    fn a_launch_override_round_trips_its_wire_shape() {
        let allotment: LaunchSelectionOverride =
            serde_json::from_value(serde_json::json!({"kind": "allotment", "value": ["lg"]}))
                .unwrap();
        assert!(matches!(allotment, LaunchSelectionOverride::Allotment(_)));
        let pin: LaunchSelectionOverride = serde_json::from_value(serde_json::json!({
            "kind": "concrete", "value": {"backend": "claude", "model": "opus"}
        }))
        .unwrap();
        assert!(matches!(pin, LaunchSelectionOverride::Concrete(_)));
        assert!(serde_json::from_value::<LaunchSelectionOverride>(
            serde_json::json!({"kind": "tier", "value": "lg"})
        )
        .is_err());
    }
}
