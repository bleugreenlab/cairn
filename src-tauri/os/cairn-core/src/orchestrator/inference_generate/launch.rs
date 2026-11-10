//! Starting a generation: an ordinary issue whose one agent the operator can
//! watch and message (tenet 1364).
//!
//! [`launch`] files `Generate profile: <model> on <pool>` in the project
//! Settings has open, records the generation `starting` in the private ledger
//! under the id its execution will take, and only then starts the
//! `inference-generate` recipe on the issue through the ordinary start, so the
//! agent's model is chosen by ordinary selection and its execution can be held
//! or queued like any other.
//!
//! [`admit_launch`] runs at the ordinary launch handoff
//! (`jobs::start_prepared_job_session`) for every job of an execution, before
//! admission can queue or spawn it. A generation's execution admits exactly
//! one job, the job of its one agent node: the first launch of that job binds
//! the generation to it (job and node), and every launch of another job in the
//! execution is refused. The binding exists before any process does, so the
//! generation's doors never see an agent they cannot place.
//!
//! The generation's scope is enforced at Cairn's resource gate, which sees
//! only Cairn's verbs, so both refuse a backend that keeps provider-native
//! shell or file-edit tools ([`AgentBackend::native_tools_removed`]): before
//! anything is filed when the selection it would make is known, and at every
//! launch of the bound job, whose model a selection patch can move. Neither
//! substitutes another model.
//!
//! [`AgentBackend::native_tools_removed`]: crate::backends::AgentBackend::native_tools_removed

use std::collections::BTreeMap;

use cairn_db::turso::params;

use super::{
    executor_snapshots, load_inventory, pool_hardware, preflight, resolve_pool, select_generations,
    GenerationRow, RUNNING,
};
use crate::models::{ExecutionSnapshot, RecipeNode, RecipeNodeType};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// The shipped recipe a generation's issue runs.
pub const RECIPE: &str = "inference-generate";

/// Filed, with its execution named, and its agent not yet launched.
pub const STARTING: &str = "starting";

/// Ended before its agent ran: the start failed, the launch was refused, or
/// the execution ended first. Its outcome says why.
pub const LAUNCH_FAILED: &str = "launchFailed";

/// The mode of a generation run by an ordinary issue's agent.
pub const AGENT_MODE: &str = "agent";

/// Why a team project cannot generate yet. Until cairn/6489 lands, the
/// operator's answers to a team project's questions are refused by a missing
/// ledger table and stay pending, so the generation could never be approved.
fn team_refusal(project_id: &str) -> Option<String> {
    match cairn_common::ids::parse_route_scope(project_id) {
        Ok(cairn_common::ids::RouteScope::Local) => None,
        Ok(cairn_common::ids::RouteScope::Team(_)) => Some(
            "a team project cannot generate a profile yet: the operator's answers to its \
             questions stay pending until cairn://p/cairn/6489 gives team projects their answer \
             ledger. Open a local project in Settings and generate there."
                .to_string(),
        ),
        Err(error) => Some(format!("project {project_id} has no valid scope: {error}")),
    }
}

/// The one agent node a generation's snapshot runs, or why the topology is
/// not one Cairn can bind: anything that would run a second agent, open a pull
/// request, act on its own or carry an output contract is refused, whichever
/// recipe file or override produced it. The agent must also be long-running
/// (`execution::jobs::is_long_running_node`): its turn end leaves it idle and
/// messageable, where a completing node would end the generation at its first
/// turn and delete its draft.
pub(crate) fn sole_agent(snapshot: &ExecutionSnapshot) -> Result<&RecipeNode, String> {
    let agent = only_agent(&snapshot.recipe.nodes)?;
    if snapshot.recipe.edges.iter().any(|edge| {
        edge.edge_type == crate::models::RecipeEdgeType::Control && edge.source_node_id == agent.id
    }) {
        return Err(format!(
            "the {RECIPE} recipe's agent node {} must end the graph, and a control edge leaves it",
            agent.name
        ));
    }
    // Artifact nodes are refused above, so no downstream output contract can
    // exist; `false` states that.
    if !crate::execution::jobs::is_long_running_node(snapshot, &agent.id, false) {
        return Err(format!(
            "the {RECIPE} recipe's agent node {} would complete at its first turn end; a \
             generation's agent must stand idle between turns",
            agent.name
        ));
    }
    Ok(agent)
}

fn only_agent(nodes: &[RecipeNode]) -> Result<&RecipeNode, String> {
    let mut agents = Vec::new();
    for node in nodes {
        match &node.node_type {
            RecipeNodeType::Agent => agents.push(node),
            RecipeNodeType::Trigger | RecipeNodeType::Context | RecipeNodeType::Instruction => {}
            other => {
                return Err(format!(
                    "the {RECIPE} recipe must run one agent and nothing else, and its node \
                     {} is a {other} node",
                    node.name
                ))
            }
        }
    }
    match agents.as_slice() {
        [agent]
            if agent
                .agent_config
                .as_ref()
                .and_then(|config| config.agent_config_id.as_deref())
                .is_some() =>
        {
            Ok(agent)
        }
        [agent] => Err(format!(
            "the {RECIPE} recipe's agent node {} names no agent",
            agent.name
        )),
        _ => Err(format!(
            "the {RECIPE} recipe must run exactly one agent, and it runs {}",
            agents.len()
        )),
    }
}

/// Why `model` on `backend` cannot run a generation's agent.
fn native_tools_refusal(model: &str, backend: &str) -> Option<String> {
    crate::backends::backend_for_name(Some(backend))
        .native_tools_removed()
        .err()
        .map(|why| {
            format!(
                "the generation's agent would run {model} on {backend}, and {why}. Its scope is \
                 enforced at Cairn's resource gate, so it may not keep tools that reach the \
                 machine around it. Give the Inference Profiler agent a model whose backend \
                 removes them (Claude does) and start the generation again; Cairn does not \
                 substitute another model."
            )
        })
}

/// The refusal the selection `snapshot`'s agent would make now earns, when
/// that selection can be predicted. A prediction that cannot be made is left
/// to the launch, which decides again with the model it actually chose.
fn predicted_refusal(
    orch: &Orchestrator,
    project_id: &str,
    snapshot: &ExecutionSnapshot,
    node_id: &str,
) -> Option<String> {
    use crate::execution::launch_preview::StepPick;
    let step = crate::execution::launch_preview::preview_snapshot(orch, snapshot, project_id)
        .ok()?
        .into_iter()
        .find(|step| step.node_id == node_id)?;
    let model = match &step.pick {
        Ok(StepPick::Chosen(record) | StepPick::Predicted(record)) => {
            record.chosen.model.as_str().to_string()
        }
        Ok(StepPick::PinnedUnplaced) => step.pin.as_ref()?.model.as_str().to_string(),
        Err(_) => return None,
    };
    let pin = step
        .agent_id
        .as_deref()
        .and_then(|agent| snapshot.agents.get(agent))
        .and_then(|agent| agent.selection.as_ref());
    let backend = crate::backends::effective_backend_name(pin, Some(&model))
        .unwrap_or_else(|| crate::backends::CLAUDE_FAMILY_BACKEND.to_string());
    native_tools_refusal(&model, &backend)
}

/// The ordinary issue a generation is filed as: its title and description.
/// A regeneration keeps the ordinary title; its description carries the
/// profile exactly as it was saved at the start, so the operator can read it
/// there whatever later happens to the inventory, and the agent starts from it.
fn filing(
    id: &str,
    model: &str,
    pool: &str,
    target: Option<&super::replacement::Target>,
) -> (String, String) {
    let title = format!("Generate profile: {model} on {pool}");
    let mut description = format!(
        "Generate an inference profile for `{model}` on pool `{pool}`.\n\n\
         Generation: `{id}` (`cairn://inference/generations/{id}`).\n\n\
         The Inference Profiler researches how the model runs best on the pool's machines, \
         asks its questions here, drafts a profile and has verification measure it. The \
         operator approves what a draft runs before it is verified, and only a verified \
         draft is kept. Message the agent on this issue at any time."
    );
    let Some(target) = target else {
        return (title, description);
    };
    let profile = target.profile_id.as_str();
    let fence = "`".repeat(longest_backtick_run(&target.body).max(2) + 1);
    description.push_str(&format!(
        "\n\n## Regenerating profile `{profile}`\n\n\
         The operator started this generation to regenerate the existing profile `{profile}` \
         in place.\n\n\
         - Starting it does not change the saved profile. Until this generation's first draft \
         is accepted, `{profile}` stays exactly as saved, and it stays unstartable until a \
         verification passes it.\n\
         - The first accepted draft overwrites `{profile}`. If the generation then ends \
         without a verification pass (it fails, is cancelled or is abandoned), that draft is \
         removed.\n\
         - The saved profile as it stood at the start is kept below, on this issue, whatever \
         happens to the inventory.\n\
         - Starting a regeneration approves nothing a draft runs. The operator approves the \
         actual draft's runnable surface and hash before it is verified, even when the draft \
         is the saved profile unchanged.\n\n\
         Captured at the start: revision {prior_revision} of generation `{prior}`, inventory \
         revision {inventory_revision}, profile digest `{digest}`, runnable-surface digest \
         `{surface}`.\n\n\
         Begin from the saved profile, and write the draft as `generated.profile` under the \
         same id, `{profile}`: no other existing profile can be replaced.\n\n\
         {fence}json\n{body}\n{fence}\n",
        prior = target.prior_generation,
        prior_revision = target.prior_revision,
        inventory_revision = target.inventory_revision,
        digest = target.profile_digest,
        surface = target.surface_digest,
        body = target.body,
    ));
    (title, description)
}

/// The longest run of backticks in `text`, so a fence around it is longer.
fn longest_backtick_run(text: &str) -> usize {
    text.split(|c| c != '`').map(str::len).max().unwrap_or(0)
}

/// Whether the issue `issue_id` holds `target`'s literal saved body, read back
/// from the database that owns it. A regeneration is not recorded, so never
/// dispatched, unless its snapshot is readable from its issue.
async fn snapshot_on_issue(
    owning: &LocalDb,
    issue_id: &str,
    target: &super::replacement::Target,
) -> Result<(), String> {
    let issue = issue_id.to_string();
    let description = owning
        .read(move |conn| {
            let issue = issue.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT description FROM issues WHERE id = ?1",
                        params![issue.as_str()],
                    )
                    .await?;
                Ok(match rows.next().await? {
                    Some(row) => row.opt_text(0)?,
                    None => None,
                })
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    if description.is_some_and(|text| text.contains(&target.body)) {
        Ok(())
    } else {
        Err(format!(
            "the saved snapshot of profile {} is not readable from the issue",
            target.profile_id
        ))
    }
}

/// Write the generation's row and, for a regeneration, its capture in one
/// private transaction: a regeneration never starts without the capture its
/// first accepted draft is checked against, and a capture that no longer
/// holds leaves no generation behind.
async fn record(
    db: &LocalDb,
    row: &GenerationRow,
    target: Option<&super::replacement::Target>,
) -> Result<(), String> {
    let (row, target) = (row.clone(), target.cloned());
    db.write(move |conn| {
        let (row, target) = (row.clone(), target.clone());
        Box::pin(async move {
            super::insert_generation_in(conn, &row).await?;
            if let Some(target) = &target {
                super::replacement::record_in(conn, &row.id, target).await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// A started generation and the ordinary issue it runs on.
#[derive(Debug, Clone)]
pub struct LaunchedGeneration {
    /// Still `starting`: its agent binds when it launches.
    pub generation: GenerationRow,
    pub issue_uri: String,
}

/// Start generating a profile for `model` on `pool` as an ordinary issue in
/// `project_id`, and answer its generation with the issue's URI.
/// `replacement` names an existing profile to regenerate in place; only the
/// operator's start reaches this with one.
///
/// Everything that can be refused is refused before the issue exists: a team
/// project, a replacement target that cannot be regenerated as it stands, the
/// preflight, a recipe that is not one agent, and a backend the selection
/// would choose that keeps native tools. A start that fails after the issue
/// exists ends the generation `launchFailed` on that issue, or records no
/// generation at all, and the error names the issue. Nothing reaches dispatch
/// until a regeneration's saved snapshot reads back from its issue and its
/// capture is recorded with its generation. Launch never writes the inventory
/// or the ownership ledger. Every machine's hardware is its executor's report:
/// a pool with a machine that reports none is refused by the preflight.
pub async fn launch(
    orch: &Orchestrator,
    project_id: &str,
    model: &str,
    pool: &str,
    replacement: Option<&str>,
) -> Result<LaunchedGeneration, String> {
    if let Some(refusal) = team_refusal(project_id) {
        return Err(refusal);
    }
    let target = match replacement {
        Some(profile) => Some(super::replacement::capture(&orch.db.local, model, profile).await?),
        None => None,
    };
    let inventory = load_inventory(&orch.db.local).await?;
    let detected = resolve_pool(orch, pool)?;
    let reported = pool_hardware(&detected, &executor_snapshots(orch), &BTreeMap::new());
    preflight(
        &inventory,
        &orch.fleet.network(),
        model,
        &detected,
        reported,
    )?;
    let snapshot = crate::execution::recipe::snapshot_for_launch(orch, project_id, RECIPE).await?;
    let agent = sole_agent(&snapshot)?;
    if let Some(refusal) = predicted_refusal(orch, project_id, &snapshot, &agent.id) {
        return Err(refusal);
    }

    let owning = crate::execution::routing::owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| error.to_string())?;
    let project_key = crate::execution::ownership::project_key(&owning, project_id)
        .await
        .ok_or_else(|| format!("project {project_id} not found"))?;
    let id = uuid::Uuid::new_v4().to_string();
    let pool_key = detected.key.0.clone();
    let authorship = crate::issues::crud::installation_machine_authorship(
        orch.anon_device_manager.device_id(),
        orch.services.clock.now(),
    )
    .map_err(|error| error.to_string())?;
    let (title, description) = filing(&id, model, &pool_key, target.as_ref());
    // Filed without work: the execution starts only once the ledger names it.
    let issue = crate::mcp::handlers::issues::create_issue_in_project(
        orch,
        &project_key,
        title,
        Some(description),
        None,
        None,
        None,
        None,
        authorship,
    )
    .await?;
    let issue_uri = cairn_uri::build_issue_uri(&project_key, issue.number);

    let execution_id = cairn_common::ids::mint_child(&issue.issue_id);
    let row = GenerationRow {
        id: id.clone(),
        project_id: project_id.to_string(),
        issue_id: Some(issue.issue_id.clone()),
        job_id: String::new(),
        mode: AGENT_MODE.into(),
        execution_id: Some(execution_id.clone()),
        attempts: 0,
        draft_revision: 0,
        draft_digest: None,
        approval_prompt_id: None,
        approval_run_id: None,
        approval_revision: None,
        verification_id: None,
        cleanup_pending: false,
        node_uri: String::new(),
        model: model.to_string(),
        pool: pool_key,
        state: STARTING.to_string(),
        draft_profile: None,
        draft_written_at: None,
        approved_surface: None,
        hardware: BTreeMap::new(),
        outcome: None,
        started_at: super::super::inference::now_ms(),
        finished_at: None,
    };
    let clock = &*orch.services.clock;
    let regenerating = target.as_ref().map(|target| target.profile_id.as_str());
    if let Some(target) = &target {
        if let Err(error) = snapshot_on_issue(&owning, &issue.issue_id, target).await {
            tell_unrecorded(orch, &owning, &issue.issue_id, &id, regenerating, &error).await;
            return Err(format!(
                "{issue_uri} was filed, but its generation was not started: {error}"
            ));
        }
    }
    if let Err(error) = record(&orch.db.local, &row, target.as_ref()).await {
        tell_unrecorded(orch, &owning, &issue.issue_id, &id, regenerating, &error).await;
        return Err(format!(
            "{issue_uri} was filed, but its generation could not be recorded: {error}"
        ));
    }
    orch.notifier.emit_change("inference_generations");
    // The issue is new, so this finds no run; it is the same rule every
    // later binding applies, and a run that started in between is caught.
    if let Err(error) = super::scope::mark_issue_runs(&orch.db.local, &id, &issue.issue_id).await {
        let _ = end_starting(&orch.db.local, &owning, clock, &row, &error).await;
        orch.notifier.emit_change("comments");
        return Err(format!(
            "{issue_uri} was filed, but its runs could not be scoped: {error}"
        ));
    }

    let started = crate::execution::recipe::start_recipe_execution_and_advance_as(
        orch,
        execution_id.clone(),
        &issue.issue_id,
        Some(RECIPE),
        project_id,
        None,
        crate::models::TriggerType::Manual,
        crate::execution::recipe::LaunchIntent::NewAttempt,
        crate::execution::recipe::Starter::Capped,
    )
    .await
    .and_then(|launched| {
        (launched.execution.id == execution_id)
            .then_some(())
            .ok_or_else(|| {
                format!(
                    "the execution started as {}, not {execution_id}",
                    launched.execution.id
                )
            })
    });
    if let Err(error) = started {
        let _ = end_starting(&orch.db.local, &owning, clock, &row, &error).await;
        orch.notifier.emit_change("inference_generations");
        orch.notifier.emit_change("comments");
        return Err(format!(
            "{issue_uri} was filed, but its agent could not be started: {error}"
        ));
    }
    Ok(LaunchedGeneration {
        generation: row,
        issue_uri,
    })
}

/// The ordinary issue a generation runs on, by its canonical URI.
pub async fn issue_uri(orch: &Orchestrator, generation: &GenerationRow) -> Option<String> {
    let issue_id = generation.issue_id.clone()?;
    let owning = crate::execution::routing::owning_db_for_project(&orch.db, &generation.project_id)
        .await
        .ok()?;
    owning
        .read(move |conn| {
            let issue_id = issue_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT p.key, i.number FROM issues i JOIN projects p ON p.id = i.project_id \
                         WHERE i.id = ?1",
                        params![issue_id.as_str()],
                    )
                    .await?;
                Ok(match rows.next().await? {
                    Some(row) => Some(cairn_uri::build_issue_uri(&row.text(0)?, row.i64(1)? as i32)),
                    None => None,
                })
            })
        })
        .await
        .ok()
        .flatten()
}

/// End a generation that is still `starting`. Answers whether this call
/// ended it.
async fn fail_starting(db: &LocalDb, id: &str, reason: &str) -> Result<bool, String> {
    let (id, outcome) = (
        id.to_string(),
        serde_json::json!({ "reason": reason }).to_string(),
    );
    let now = super::super::inference::now_ms();
    db.write(move |conn| {
        let (id, outcome) = (id.clone(), outcome.clone());
        Box::pin(async move {
            let changed = conn
                .execute(
                    "UPDATE inference_generations SET state = ?2, outcome_json = ?3, finished_at = ?4 \
                     WHERE id = ?1 AND state = ?5",
                    params![id.as_str(), LAUNCH_FAILED, outcome.as_str(), now, STARTING],
                )
                .await?;
            Ok(changed > 0)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Tell an issue whose generation failed before it was recorded: there is no
/// row to end, so the issue alone says the launch failed.
async fn tell_unrecorded(
    orch: &Orchestrator,
    owning: &LocalDb,
    issue_id: &str,
    generation_id: &str,
    regenerating: Option<&str>,
    reason: &str,
) {
    if let Err(error) = note_failed_launch(
        owning,
        &*orch.services.clock,
        issue_id,
        generation_id,
        false,
        regenerating,
        reason,
    )
    .await
    {
        log::warn!(
            "generation {generation_id}: its issue could not be told its launch failed: {error}"
        );
    }
    orch.notifier.emit_change("comments");
}

/// End `generation`, still `starting`, and say so on its ordinary issue,
/// where the operator watches it: the private row alone would leave the issue
/// looking as if work were under way. Answers whether this call ended it, and
/// only that call writes the note, so it is written once. A note that cannot
/// be written is logged; the generation has ended either way.
async fn end_starting(
    private: &LocalDb,
    owning: &LocalDb,
    clock: &dyn crate::services::Clock,
    generation: &GenerationRow,
    reason: &str,
) -> Result<bool, String> {
    if !fail_starting(private, &generation.id, reason).await? {
        return Ok(false);
    }
    if let Some(issue_id) = generation.issue_id.as_deref() {
        let regenerating = super::replacement::load(private, &generation.id)
            .await
            .ok()
            .flatten()
            .map(|target| target.profile_id);
        if let Err(error) = note_failed_launch(
            owning,
            clock,
            issue_id,
            &generation.id,
            true,
            regenerating.as_deref(),
            reason,
        )
        .await
        {
            log::warn!(
                "generation {}: its issue could not be told its launch failed: {error}",
                generation.id
            );
        }
    }
    Ok(true)
}

/// The comment that tells a generation's issue its launch failed. `recorded`
/// says whether the generation's row exists to carry `launchFailed`.
async fn note_failed_launch(
    owning: &LocalDb,
    clock: &dyn crate::services::Clock,
    issue_id: &str,
    generation_id: &str,
    recorded: bool,
    regenerating: Option<&str>,
    reason: &str,
) -> Result<(), String> {
    let ended = if recorded {
        format!("Generation `{generation_id}` ended `{LAUNCH_FAILED}` before its agent ran")
    } else {
        format!("Generation `{generation_id}` was never recorded, so it did not start")
    };
    let kept = regenerating
        .map(|profile| {
            format!(
                " Profile `{profile}` was not changed and is not being regenerated: it is still \
                 the saved profile shown above."
            )
        })
        .unwrap_or_default();
    let content = format!(
        "**Launch failed.** {ended}: {reason}. No agent works on this issue.{kept} To try \
         again, start a new generation from Settings > Inference."
    );
    crate::issues::comments::create(
        owning,
        clock,
        crate::models::CreateComment {
            issue_id: issue_id.to_string(),
            content,
            source: crate::models::CommentSource::Agent,
        },
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

async fn generation_for_execution(
    db: &LocalDb,
    execution_id: &str,
) -> Result<Option<GenerationRow>, String> {
    Ok(select_generations(
        db,
        "WHERE execution_id = ?1",
        Some(execution_id.to_string()),
    )
    .await?
    .pop())
}

/// A job as its launch admission reads it.
struct JobFacts {
    issue_id: Option<String>,
    execution_id: Option<String>,
    recipe_node_id: Option<String>,
    parent_job_id: Option<String>,
}

async fn job_facts(db: &LocalDb, job_id: &str) -> Result<Option<JobFacts>, String> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT issue_id, execution_id, recipe_node_id, parent_job_id FROM jobs \
                     WHERE id = ?1",
                    params![job_id.as_str()],
                )
                .await?;
            Ok(match rows.next().await? {
                Some(row) => Some(JobFacts {
                    issue_id: row.opt_text(0)?,
                    execution_id: row.opt_text(1)?,
                    recipe_node_id: row.opt_text(2)?,
                    parent_job_id: row.opt_text(3)?,
                }),
                None => None,
            })
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn stored_snapshot(db: &LocalDb, execution_id: &str) -> Result<ExecutionSnapshot, String> {
    let execution_id = execution_id.to_string();
    let json = db
        .read(move |conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT snapshot FROM executions WHERE id = ?1",
                        params![execution_id.as_str()],
                    )
                    .await?;
                Ok(match rows.next().await? {
                    Some(row) => row.opt_text(0)?,
                    None => None,
                })
            })
        })
        .await
        .map_err(|error| error.to_string())?
        .ok_or("its execution has no snapshot")?;
    crate::config::snapshot_migrate::load(&json)
        .map_err(|error| format!("its execution's snapshot does not parse: {error}"))
}

/// How a launch in a generation's execution was refused.
enum Refusal {
    /// The generation cannot go on: nothing else will bind it.
    Ends(String),
    /// This job may not launch; the generation is untouched.
    Job(String),
}

/// Admit the launch of `job_id` in `execution_id` on `backend`, binding the
/// generation the execution hosts to it when it is that generation's agent.
/// An execution that hosts no generation admits every launch. A lookup that
/// fails refuses: a generation's job is never let through on a missing row.
pub(crate) async fn admit_launch(
    orch: &Orchestrator,
    execution_id: &str,
    job_id: &str,
    backend: &str,
) -> Result<(), String> {
    let Some(generation) = generation_for_execution(&orch.db.local, execution_id).await? else {
        return Ok(());
    };
    let owning = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| format!("generation {}: {error}", generation.id))?;
    let admitted = admit(
        &orch.db.local,
        &owning,
        &*orch.services.clock,
        &generation,
        execution_id,
        job_id,
        backend,
    )
    .await;
    orch.notifier.emit_change("inference_generations");
    if admitted.is_err() {
        orch.notifier.emit_change("comments");
    }
    admitted
}

async fn admit(
    private: &LocalDb,
    owning: &LocalDb,
    clock: &dyn crate::services::Clock,
    generation: &GenerationRow,
    execution_id: &str,
    job_id: &str,
    backend: &str,
) -> Result<(), String> {
    match bind(private, owning, generation, execution_id, job_id, backend).await {
        Ok(()) => Ok(()),
        Err(Refusal::Job(why)) => Err(why),
        Err(Refusal::Ends(why)) => {
            end_starting(private, owning, clock, generation, &why).await?;
            Err(why)
        }
    }
}

async fn bind(
    private: &LocalDb,
    owning: &LocalDb,
    generation: &GenerationRow,
    execution_id: &str,
    job_id: &str,
    backend: &str,
) -> Result<(), Refusal> {
    let id = &generation.id;
    let facts = job_facts(owning, job_id)
        .await
        .map_err(Refusal::Job)?
        .ok_or_else(|| Refusal::Job(format!("generation {id}: job {job_id} not found")))?;
    let model = owning_job_model(owning, job_id)
        .await
        .map_err(Refusal::Job)?;

    if !generation.job_id.is_empty() {
        if generation.job_id != job_id {
            return Err(Refusal::Job(format!(
                "execution {execution_id} runs generation {id}'s agent as job {}; job {job_id} \
                 may not launch in it",
                generation.job_id
            )));
        }
        // The bound agent launching again (a cold resume, a restart): a
        // selection patch moves its model from its next turn, so its backend
        // is asked again.
        return match native_tools_refusal(&model, backend) {
            Some(why) => Err(Refusal::Job(why)),
            None => Ok(()),
        };
    }
    if generation.state != STARTING || generation.mode != super::launch::AGENT_MODE {
        return Err(Refusal::Job(format!(
            "generation {id} is {} and binds no agent; job {job_id} may not launch in its \
             execution",
            generation.state
        )));
    }
    if facts.execution_id.as_deref() != Some(execution_id)
        || facts.issue_id.is_none()
        || facts.issue_id != generation.issue_id
        || facts.parent_job_id.is_some()
    {
        return Err(Refusal::Job(format!(
            "job {job_id} is not the agent of generation {id}'s issue and execution"
        )));
    }
    let snapshot = stored_snapshot(owning, execution_id)
        .await
        .map_err(|why| Refusal::Ends(format!("generation {id}: {why}")))?;
    let agent = sole_agent(&snapshot).map_err(Refusal::Ends)?;
    if facts.recipe_node_id.as_deref() != Some(agent.id.as_str()) {
        return Err(Refusal::Job(format!(
            "job {job_id} does not run generation {id}'s agent node {}",
            agent.name
        )));
    }
    if let Some(why) = native_tools_refusal(&model, backend) {
        return Err(Refusal::Ends(why));
    }
    let node_uri = crate::jobs::queries::home_uri_for_job(owning, job_id)
        .await
        .map_err(|error| Refusal::Job(error.to_string()))?
        .ok_or_else(|| Refusal::Job(format!("job {job_id} has no node address")))?;

    let issue_id = generation.issue_id.as_deref().unwrap_or_default();
    let claimed = claim(private, id, execution_id, job_id, &node_uri, issue_id)
        .await
        .map_err(Refusal::Job)?;
    if claimed {
        return Ok(());
    }
    // Another launch bound it first: only the same job is let through.
    match generation_for_execution(private, execution_id).await {
        Ok(Some(current)) if current.job_id == job_id => Ok(()),
        Ok(_) => Err(Refusal::Job(format!(
            "generation {id} was bound to another job while job {job_id} launched"
        ))),
        Err(error) => Err(Refusal::Job(error)),
    }
}

/// The model a job runs, as its launch stored it.
async fn owning_job_model(db: &LocalDb, job_id: &str) -> Result<String, String> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT model FROM jobs WHERE id = ?1",
                    params![job_id.as_str()],
                )
                .await?;
            Ok(match rows.next().await? {
                Some(row) => row.opt_text(0)?,
                None => None,
            })
        })
    })
    .await
    .map_err(|error| error.to_string())
    .map(Option::unwrap_or_default)
}

/// Bind a `starting` generation to its agent's job and node, once, and in
/// the same transaction mark every run already on its issue (the bound job's
/// prepared run among them). A binding never commits without those markers:
/// if they cannot be written, the binding rolls back with them.
async fn claim(
    db: &LocalDb,
    id: &str,
    execution_id: &str,
    job_id: &str,
    node_uri: &str,
    issue_id: &str,
) -> Result<bool, String> {
    let values = [id, execution_id, job_id, node_uri, issue_id].map(str::to_string);
    db.write(move |conn| {
        let [id, execution_id, job_id, node_uri, issue_id] = values.clone();
        Box::pin(async move {
            let changed = conn
                .execute(
                    "UPDATE inference_generations SET job_id = ?3, node_uri = ?4, state = ?5 \
                     WHERE id = ?1 AND execution_id = ?2 AND state = ?6 AND job_id = ''",
                    params![
                        id.as_str(),
                        execution_id.as_str(),
                        job_id.as_str(),
                        node_uri.as_str(),
                        RUNNING,
                        STARTING
                    ],
                )
                .await?;
            if changed > 0 {
                super::scope::mark_issue_runs_conn(conn, &id, &issue_id).await?;
            }
            Ok(changed > 0)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Where a `starting` generation's execution stands.
#[derive(Debug, PartialEq, Eq)]
enum ExecutionState {
    Missing,
    /// Held, queued, or with a job yet to end: its agent may still launch.
    Live,
    Ended(String),
}

async fn execution_state(db: &LocalDb, execution_id: &str) -> Result<ExecutionState, String> {
    let execution_id = execution_id.to_string();
    db.read(move |conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT e.status, \
                            (SELECT COUNT(*) FROM jobs j WHERE j.execution_id = e.id), \
                            (SELECT COUNT(*) FROM jobs j WHERE j.execution_id = e.id \
                               AND j.status NOT IN ('complete', 'failed', 'cancelled')) \
                     FROM executions e WHERE e.id = ?1",
                    params![execution_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(ExecutionState::Missing);
            };
            let status = row.text(0)?;
            let (jobs, live) = (row.i64(1)?, row.i64(2)?);
            Ok(
                if matches!(status.as_str(), "complete" | "failed" | "cancelled")
                    || (jobs > 0 && live == 0)
                {
                    ExecutionState::Ended(status)
                } else {
                    ExecutionState::Live
                },
            )
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// End every generation left `starting` that can no longer bind: its
/// execution was never created (a start interrupted between the ledger row and
/// the execution insert) or ended before its agent launched. A held or queued
/// execution keeps its generation `starting`, however long it waits. Run at
/// startup only, when no launch is between those two writes. A generation that
/// cannot be settled is logged and stays `starting`; it never keeps the rest
/// from settling. Answers how many it ended.
pub async fn reconcile_starting(orch: &Orchestrator) -> Result<usize, String> {
    let starting = select_generations(
        &orch.db.local,
        "WHERE state = ?1",
        Some(STARTING.to_string()),
    )
    .await?;
    let ended = settle_each(&starting, |generation| settle_starting(orch, generation)).await;
    if ended > 0 {
        orch.notifier.emit_change("inference_generations");
    }
    Ok(ended)
}

/// Settle each generation in turn with `settle`, answering how many it ended.
/// One whose settling fails is logged and skipped.
async fn settle_each<'a, F, Fut>(starting: &'a [GenerationRow], mut settle: F) -> usize
where
    F: FnMut(&'a GenerationRow) -> Fut,
    Fut: std::future::Future<Output = Result<bool, String>>,
{
    let mut ended = 0;
    for generation in starting {
        match settle(generation).await {
            Ok(true) => ended += 1,
            Ok(false) => {}
            Err(error) => log::warn!(
                "generation {}: it could not be settled and stays starting: {error}",
                generation.id
            ),
        }
    }
    ended
}

/// End `generation`, still `starting`, if it can no longer bind: it names no
/// execution, or its execution was never created or ended before its agent
/// launched. A held or queued execution keeps it waiting. Its issue is told
/// when this ends it. Answers whether this call ended it.
pub(super) async fn settle_starting(
    orch: &Orchestrator,
    generation: &GenerationRow,
) -> Result<bool, String> {
    let owning = crate::execution::routing::owning_db_for_project(&orch.db, &generation.project_id)
        .await
        .map_err(|error| error.to_string())?;
    let reason = match generation.execution_id.as_deref() {
        None => "it names no execution".to_string(),
        Some(execution_id) => match execution_state(&owning, execution_id).await? {
            ExecutionState::Missing => {
                "its start was interrupted before its execution was created".to_string()
            }
            ExecutionState::Ended(status) => {
                format!("its execution ended ({status}) before its agent launched")
            }
            ExecutionState::Live => return Ok(false),
        },
    };
    let ended = end_starting(
        &orch.db.local,
        &owning,
        &*orch.services.clock,
        generation,
        &reason,
    )
    .await?;
    if ended {
        orch.notifier.emit_change("comments");
    }
    Ok(ended)
}

#[cfg(test)]
mod tests {
    use super::super::insert_generation;
    use super::*;

    const SHIPPED: &str = include_str!("../../../../../packs/core/recipes/inference-generate.yaml");

    /// The shipped recipe as one snapshot. Parsing mints fresh node ids, so
    /// everything a test compares comes from this one parse.
    fn shipped_snapshot() -> ExecutionSnapshot {
        let recipe = crate::models::RecipeFile::from_yaml(SHIPPED)
            .expect("the shipped recipe parses")
            .into_recipe(Some("default".to_string()), None);
        ExecutionSnapshot {
            branch_target: Default::default(),
            continuity: Default::default(),
            recipe: crate::models::RecipeSnapshot {
                id: RECIPE.into(),
                name: recipe.name,
                description: None,
                trigger: crate::models::RecipeTrigger::Manual,
                nodes: recipe.nodes,
                edges: recipe.edges,
            },
            agents: std::collections::HashMap::new(),
            skills: std::collections::HashMap::new(),
            trigger_context: crate::models::TriggerContext {
                issue_id: Some("issue-1".into()),
                project_id: "proj-1".into(),
                trigger_type: crate::models::TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
            inline_delegated_packets: vec![],
            created_at: 1,
        }
    }

    #[test]
    fn the_shipped_recipe_runs_one_long_running_inference_profiler_and_nothing_else() {
        let snapshot = shipped_snapshot();
        let agent = sole_agent(&snapshot).expect("one agent");
        assert_eq!(
            agent
                .agent_config
                .as_ref()
                .and_then(|config| config.agent_config_id.as_deref()),
            Some("inference-profiler")
        );
        assert!(crate::execution::jobs::is_long_running_node(
            &snapshot, &agent.id, false
        ));
    }

    #[test]
    fn a_second_agent_a_pr_no_agent_or_a_completing_agent_is_refused_whatever_produced_it() {
        let snapshot = shipped_snapshot();
        let agent = sole_agent(&snapshot).unwrap().clone();

        let mut two = snapshot.clone();
        let mut second = agent.clone();
        second.id = "reviewer-1".into();
        two.recipe.nodes.push(second);
        assert!(sole_agent(&two).unwrap_err().contains("exactly one agent"));

        let mut with_pr = snapshot.clone();
        let mut pr = agent.clone();
        pr.id = "pr-1".into();
        pr.name = "PR".into();
        pr.node_type = RecipeNodeType::Pr;
        with_pr.recipe.nodes.push(pr);
        assert!(sole_agent(&with_pr).unwrap_err().contains("PR"));

        let mut none = snapshot.clone();
        none.recipe
            .nodes
            .retain(|node| node.node_type != RecipeNodeType::Agent);
        assert!(sole_agent(&none).unwrap_err().contains("runs 0"));

        // An override that hangs anything off the agent's control port (even
        // an instruction) would make it complete at turn end.
        let mut completing = snapshot.clone();
        let mut onward = completing
            .recipe
            .edges
            .iter()
            .find(|edge| edge.edge_type == crate::models::RecipeEdgeType::Control)
            .expect("the trigger's control edge")
            .clone();
        onward.source_node_id = agent.id.clone();
        completing.recipe.edges.push(onward);
        assert!(sole_agent(&completing)
            .unwrap_err()
            .contains("control edge leaves it"));
    }

    #[test]
    fn a_team_project_is_refused_before_anything_is_filed() {
        assert_eq!(team_refusal("proj-1"), None);
        let team = format!("acme~{}", uuid::Uuid::new_v4());
        let refusal = team_refusal(&team).expect("a team project is refused");
        assert!(refusal.contains("cairn://p/cairn/6489"), "{refusal}");
    }

    /// One test per backend that can serve an agent session: Claude's launch
    /// removes its built-ins (`--tools ""`), so it runs the agent; Codex keeps
    /// shell and apply_patch on a thread with Cairn's server, so it is refused
    /// by name, without another model put in its place; a backend that has not
    /// shown the removal is refused too.
    #[test]
    fn only_a_backend_that_removes_native_tools_runs_the_generations_agent() {
        assert_eq!(native_tools_refusal("opus", "claude"), None);

        let codex = native_tools_refusal("gpt-5.6-sol", "codex").expect("codex is refused");
        assert!(codex.contains("gpt-5.6-sol on codex"), "{codex}");
        assert!(codex.contains("apply_patch"), "{codex}");
        assert!(codex.contains("does not substitute"), "{codex}");

        for backend in [
            "openrouter",
            crate::backends::opencode::OPENCODE_BACKEND_KEY,
            "ollama",
            crate::backends::inference::INFERENCE_BACKEND_KEY,
        ] {
            assert!(
                native_tools_refusal("some-model", backend).is_some(),
                "{backend} has not shown it removes native tools"
            );
        }
    }

    fn target(body: &str) -> super::super::replacement::Target {
        super::super::replacement::Target {
            profile_id: "edited".into(),
            model: "Qwen/Qwen3-Embedding-4B".into(),
            prior_generation: "g-old".into(),
            prior_revision: 2,
            inventory_revision: 5,
            profile_digest: "d".repeat(64),
            surface_digest: "s".repeat(64),
            body: body.into(),
            updated_at: 9,
            captured_at: 10,
        }
    }

    /// A regeneration is filed under the ordinary title. Its description adds
    /// the disclosure and ends with the profile exactly as saved, secret
    /// references unexpanded, in a fence its own backticks cannot close.
    #[test]
    fn a_regeneration_keeps_the_ordinary_title_and_carries_its_saved_profile_literally() {
        let (title, ordinary) = filing("g1", "org/M", "bbs", None);
        assert_eq!(title, "Generate profile: org/M on bbs");
        assert!(!ordinary.contains("Regenerating"));

        let body = r#"{"id":"edited","engine":{"env":{"HF_TOKEN":"${secret:hf}"}},"note":"````"}"#;
        let (regeneration, description) = filing("g1", "org/M", "bbs", Some(&target(body)));
        assert_eq!(regeneration, title);
        assert!(description.starts_with(&ordinary));
        assert!(
            description.ends_with(&format!("`````json\n{body}\n`````\n")),
            "{description}"
        );
        for disclosed in [
            "Starting it does not change the saved profile",
            "The first accepted draft overwrites `edited`",
            "that draft is removed",
            "kept below, on this issue",
            "Starting a regeneration approves nothing a draft runs",
            "revision 2 of generation `g-old`",
            "inventory revision 5",
            &"d".repeat(64),
            &"s".repeat(64),
        ] {
            assert!(
                description.contains(disclosed),
                "{disclosed}: {description}"
            );
        }
        assert_eq!(longest_backtick_run("a``b```c"), 3);
        assert_eq!(longest_backtick_run("none"), 0);
    }

    /// A capture that no longer holds rolls back the generation row it was
    /// to be recorded with, so nothing is left to dispatch.
    #[tokio::test]
    async fn a_capture_that_does_not_hold_records_no_generation() {
        let db = seeded("generation-record-capture").await;
        let mut other_model = target("{}");
        other_model.model = "other/Model".into();
        assert!(record(&db, &starting("exec-1"), Some(&other_model))
            .await
            .is_err());
        assert!(super::super::get_generation(&db, "g1")
            .await
            .unwrap()
            .is_none());

        record(&db, &starting("exec-1"), None).await.unwrap();
        assert_eq!(current(&db).await.state, STARTING);
    }

    /// A regeneration goes on only once its issue reads back the literal
    /// saved body.
    #[tokio::test]
    async fn a_regeneration_starts_only_once_its_snapshot_reads_back_from_its_issue() {
        let db = seeded("generation-snapshot-readback").await;
        let saved = target(r#"{"id":"edited"}"#);
        assert!(snapshot_on_issue(&db, "issue-1", &saved).await.is_err());
        assert!(snapshot_on_issue(&db, "no-such-issue", &saved)
            .await
            .is_err());

        let (_, description) = filing("g1", "m", "bbs", Some(&saved));
        db.execute(
            "UPDATE issues SET description = ?1 WHERE id = 'issue-1'",
            params![description.as_str()],
        )
        .await
        .unwrap();
        snapshot_on_issue(&db, "issue-1", &saved).await.unwrap();
    }

    async fn seeded(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        let snapshot = shipped_snapshot();
        let agent_node = sole_agent(&snapshot).unwrap().id.clone();
        let snapshot = snapshot.to_json().unwrap();
        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('proj-1', 'default', 'Cairn', 'cairn', '/tmp/cairn', 'main', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
             VALUES ('issue-1', 'proj-1', 42, 'Generate profile: m on bbs', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
             VALUES ('exec-1', 'inference-generate', 'issue-1', 'proj-1', 'running', 1, 1, ?1)",
            params![snapshot.as_str()],
        )
        .await
        .unwrap();
        for (job, node, parent) in [
            ("j-agent", agent_node.as_str(), None),
            ("j-task", agent_node.as_str(), Some("j-agent")),
            ("j-other", "trigger-1", None),
        ] {
            db.execute(
                "INSERT INTO jobs (id, execution_id, recipe_node_id, parent_job_id, issue_id, project_id,
                                   node_name, uri_segment, status, model, created_at, updated_at)
                 VALUES (?1, 'exec-1', ?2, ?3, 'issue-1', 'proj-1', ?1, ?1, 'running', 'opus', 1, 1)",
                params![job, node, parent],
            )
            .await
            .unwrap();
        }
        db
    }

    fn starting(execution: &str) -> GenerationRow {
        GenerationRow {
            id: "g1".into(),
            project_id: "proj-1".into(),
            issue_id: Some("issue-1".into()),
            job_id: String::new(),
            mode: AGENT_MODE.into(),
            execution_id: Some(execution.into()),
            attempts: 0,
            draft_revision: 0,
            draft_digest: None,
            approval_prompt_id: None,
            approval_run_id: None,
            approval_revision: None,
            verification_id: None,
            cleanup_pending: false,
            node_uri: String::new(),
            model: "Qwen/Qwen3-Embedding-4B".into(),
            pool: "bbs".into(),
            state: STARTING.into(),
            draft_profile: None,
            draft_written_at: None,
            approved_surface: None,
            hardware: BTreeMap::new(),
            outcome: None,
            started_at: 1,
            finished_at: None,
        }
    }

    async fn current(db: &LocalDb) -> GenerationRow {
        super::super::get_generation(db, "g1")
            .await
            .unwrap()
            .unwrap()
    }

    async fn admit_as(db: &LocalDb, job: &str, backend: &str) -> Result<(), String> {
        let generation = current(db).await;
        admit(
            db,
            db,
            &crate::services::RealClock,
            &generation,
            "exec-1",
            job,
            backend,
        )
        .await
    }

    async fn issue_comments(db: &LocalDb) -> Vec<String> {
        crate::issues::comments::list(db, "issue-1")
            .await
            .unwrap()
            .into_iter()
            .map(|comment| comment.content)
            .collect()
    }

    /// A launch that ends its generation says so on the ordinary issue, once,
    /// beside the private row's `launchFailed`.
    #[tokio::test]
    async fn a_failed_launch_is_told_on_its_issue_once() {
        let db = seeded("generation-launch-failed-note").await;
        insert_generation(&db, &starting("exec-1")).await.unwrap();
        assert!(issue_comments(&db).await.is_empty());

        assert!(admit_as(&db, "j-agent", "codex").await.is_err());
        assert_eq!(current(&db).await.state, LAUNCH_FAILED);
        let notes = issue_comments(&db).await;
        assert_eq!(notes.len(), 1, "{notes:?}");
        assert!(notes[0].starts_with("**Launch failed.**"), "{}", notes[0]);
        assert!(
            notes[0].contains("`g1` ended `launchFailed`"),
            "{}",
            notes[0]
        );
        assert!(notes[0].contains("codex"), "{}", notes[0]);
        assert!(!notes[0].contains("regenerated"), "{}", notes[0]);

        // Ended already: a later refusal ends nothing and notes nothing.
        assert!(admit_as(&db, "j-agent", "codex").await.is_err());
        assert_eq!(issue_comments(&db).await.len(), 1);
    }

    /// A regeneration that failed before its generation was recorded tells
    /// its issue, in the database that owns the issue rather than the private
    /// one, that nothing was recorded and the saved profile is untouched.
    #[tokio::test]
    async fn an_unrecorded_regeneration_tells_its_issue_in_the_owning_database() {
        let private = crate::storage::migrated_test_db("generation-note-private").await;
        let owning = seeded("generation-note-owning").await;
        note_failed_launch(
            &owning,
            &crate::services::RealClock,
            "issue-1",
            "g1",
            false,
            Some("edited"),
            "the saved snapshot of profile edited is not readable from the issue",
        )
        .await
        .unwrap();
        let notes = issue_comments(&owning).await;
        assert_eq!(notes.len(), 1, "{notes:?}");
        for said in [
            "`g1` was never recorded, so it did not start",
            "not readable from the issue",
            "Profile `edited` was not changed and is not being regenerated",
            "Settings > Inference",
        ] {
            assert!(notes[0].contains(said), "{said}: {}", notes[0]);
        }
        let private_comments = private
            .query_opt_i64("SELECT COUNT(*) FROM comments", ())
            .await
            .unwrap()
            .unwrap_or(0);
        assert_eq!(private_comments, 0);
    }

    #[tokio::test]
    async fn the_agents_first_launch_binds_its_exact_job_and_node_and_no_other_job_launches() {
        let db = seeded("generation-bind").await;
        insert_generation(&db, &starting("exec-1")).await.unwrap();

        // Before the binding: a delegated task and a job of another node are
        // refused, and the generation still waits for its agent.
        assert!(admit_as(&db, "j-task", "claude").await.is_err());
        assert!(admit_as(&db, "j-other", "claude").await.is_err());
        assert_eq!(current(&db).await.state, STARTING);

        admit_as(&db, "j-agent", "claude").await.unwrap();
        let bound = current(&db).await;
        assert_eq!(bound.state, RUNNING);
        assert_eq!(bound.job_id, "j-agent");
        assert_eq!(bound.node_uri, "cairn://p/cairn/42/1/j-agent");
        assert_eq!(bound.execution_id.as_deref(), Some("exec-1"));

        // The bound agent launches again; nothing else ever does.
        admit_as(&db, "j-agent", "claude").await.unwrap();
        let refusal = admit_as(&db, "j-task", "claude").await.unwrap_err();
        assert!(refusal.contains("as job j-agent"), "{refusal}");
        assert_eq!(current(&db).await.job_id, "j-agent");
    }

    #[tokio::test]
    async fn a_backend_that_keeps_native_tools_ends_a_starting_generation_and_never_binds() {
        let db = seeded("generation-bind-codex").await;
        insert_generation(&db, &starting("exec-1")).await.unwrap();

        let refusal = admit_as(&db, "j-agent", "codex").await.unwrap_err();
        assert!(refusal.contains("apply_patch"), "{refusal}");
        let ended = current(&db).await;
        assert_eq!(ended.state, LAUNCH_FAILED);
        assert_eq!(ended.job_id, "");
        assert!(ended.outcome.unwrap()["reason"]
            .as_str()
            .unwrap()
            .contains("codex"));
        // Ended, it binds nothing afterwards either.
        assert!(admit_as(&db, "j-agent", "claude").await.is_err());
    }

    #[tokio::test]
    async fn a_bound_agent_moved_onto_a_backend_with_native_tools_is_refused_at_relaunch() {
        let db = seeded("generation-relaunch-codex").await;
        insert_generation(&db, &starting("exec-1")).await.unwrap();
        admit_as(&db, "j-agent", "claude").await.unwrap();

        assert!(admit_as(&db, "j-agent", "codex").await.is_err());
        // Refused, not rebound or ended: its lifecycle is the generation's own.
        let generation = current(&db).await;
        assert_eq!(
            (generation.state.as_str(), generation.job_id.as_str()),
            (RUNNING, "j-agent")
        );
    }

    /// The binding and the markers of the issue's runs commit together: a run
    /// already on the issue is marked in the same commit that binds, and a
    /// marker that cannot be written leaves the generation unbound.
    #[tokio::test]
    async fn a_binding_never_commits_without_the_markers_of_its_issues_runs() {
        let db = seeded("generation-bind-markers").await;
        insert_generation(&db, &starting("exec-1")).await.unwrap();
        db.execute(
            "INSERT INTO runs (id, project_id, issue_id, job_id, status, created_at, updated_at)
             VALUES ('r-agent', 'proj-1', 'issue-1', 'j-agent', 'starting', 1, 1),
                    ('r-other', 'proj-1', NULL, 'j-other', 'live', 1, 1)",
            (),
        )
        .await
        .unwrap();
        async fn marked(db: &LocalDb) -> i64 {
            db.query_opt_i64(
                "SELECT COUNT(*) FROM inference_generation_runs WHERE generation_id = 'g1'",
                (),
            )
            .await
            .unwrap()
            .unwrap_or(0)
        }

        // The marker table refuses every insert: the claim fails whole.
        db.execute_script(
            "CREATE TRIGGER refuse_markers BEFORE INSERT ON inference_generation_runs
             BEGIN SELECT RAISE(ABORT, 'marker refused'); END;",
        )
        .await
        .unwrap();
        assert!(admit_as(&db, "j-agent", "claude").await.is_err());
        let unbound = current(&db).await;
        assert_eq!(
            (unbound.state.as_str(), unbound.job_id.as_str()),
            (STARTING, "")
        );
        assert_eq!(marked(&db).await, 0);

        db.execute_script("DROP TRIGGER refuse_markers;")
            .await
            .unwrap();
        admit_as(&db, "j-agent", "claude").await.unwrap();
        assert_eq!(current(&db).await.job_id, "j-agent");
        // Both the bound job's prepared run and the run of another job on
        // the issue carry the generation's scope.
        assert_eq!(marked(&db).await, 2);
    }

    #[tokio::test]
    async fn an_execution_with_no_generation_admits_every_launch() {
        let db = seeded("generation-none").await;
        assert_eq!(generation_for_execution(&db, "exec-1").await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_held_execution_keeps_waiting_and_a_missing_or_ended_one_fails_the_start() {
        let db = seeded("generation-reconcile").await;
        assert_eq!(
            execution_state(&db, "exec-1").await.unwrap(),
            ExecutionState::Live
        );
        assert_eq!(
            execution_state(&db, "exec-gone").await.unwrap(),
            ExecutionState::Missing
        );

        // On an issue of its own: an issue holds one canonical execution.
        db.execute(
            "INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
             VALUES ('issue-2', 'proj-1', 43, 'Generate profile: m on bbs', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-held', 'inference-generate', 'issue-2', 'proj-1', 'held', 1, 1)",
            (),
        )
        .await
        .unwrap();
        // Held by the queue's caps: no jobs yet, and nothing has ended.
        assert_eq!(
            execution_state(&db, "exec-held").await.unwrap(),
            ExecutionState::Live
        );

        db.execute(
            "UPDATE jobs SET status = 'failed' WHERE execution_id = 'exec-1'",
            (),
        )
        .await
        .unwrap();
        assert!(matches!(
            execution_state(&db, "exec-1").await.unwrap(),
            ExecutionState::Ended(_)
        ));

        insert_generation(&db, &starting("exec-1")).await.unwrap();
        assert!(fail_starting(&db, "g1", "its execution ended")
            .await
            .unwrap());
        // Idempotent: an ended generation is not ended again.
        assert!(!fail_starting(&db, "g1", "again").await.unwrap());
        assert_eq!(current(&db).await.state, LAUNCH_FAILED);
    }

    /// A generation whose settling fails is skipped, not fatal: every later
    /// one is still settled and counted.
    #[tokio::test]
    async fn one_generation_that_cannot_settle_does_not_stop_the_rest() {
        let rows: Vec<GenerationRow> = ["g-bad", "g2", "g3"]
            .into_iter()
            .map(|id| GenerationRow {
                id: id.into(),
                ..starting("exec-1")
            })
            .collect();
        let mut seen = Vec::new();
        let ended = settle_each(&rows, |generation| {
            seen.push(generation.id.clone());
            let poisoned = generation.id == "g-bad";
            async move {
                if poisoned {
                    Err("its project's database is unreachable".to_string())
                } else {
                    Ok(true)
                }
            }
        })
        .await;
        assert_eq!(seen, ["g-bad", "g2", "g3"]);
        assert_eq!(ended, 2);
    }
}
