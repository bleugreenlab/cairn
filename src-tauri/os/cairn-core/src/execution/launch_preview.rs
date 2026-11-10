//! What each agent step of a recipe would run if it launched now.
//!
//! One engine, two callers. The launch composer previews a recipe it has not
//! started (`recipe::resolve_recipe_launch`), and an execution create previews
//! the snapshot it just stored ([`preview_execution`]), the way each step's own
//! launch will read it (`jobs::node_selection`). Both ask `select()` what that
//! launch will ask it, so a row is a prediction of the launch rather than a
//! lock: supply moves hourly, and a constrained step waits for the choice its
//! upstream stores.
//!
//! Steps are previewed in two passes: each on its own, then each constrained
//! step again against what the steps it names stored, or failing that their
//! first-pass pick, which is the best prediction of what they will store.

use std::collections::HashMap;

use cairn_config::allotment::{model_ref, AllotmentEntry};
use cairn_db::turso::params;
use cairn_models::types::catalog::VerbKind;

use crate::execution::model_selection::{
    effective_allotment, ResolvedConstraints, SelectionRecord,
};
use crate::models::{ExecutionSnapshot, ModelSelection, RecipeNodeType, RecipeSnapshot};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// One agent step to preview.
#[derive(Debug, Clone)]
pub(crate) struct StepInput {
    pub(crate) node_id: String,
    /// The agent the step runs, which a snapshot patch names.
    pub(crate) agent_id: Option<String>,
    /// What the step selects within; ignored for a pin.
    pub(crate) allotment: Vec<AllotmentEntry>,
    pub(crate) pin: Option<ModelSelection>,
    /// The choice the step stored at its own launch. A launched step reports
    /// it, and a constraint naming the step reads it.
    pub(crate) stored: Option<SelectionRecord>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StepPick {
    /// Stored at the step's launch.
    Chosen(SelectionRecord),
    /// What the step would pick if it launched now.
    Predicted(SelectionRecord),
    /// A pin the catalog cannot place but the session door admits: it runs
    /// as pinned, with no record to explain it.
    PinnedUnplaced,
}

#[derive(Debug, Clone)]
pub(crate) struct StepPreview {
    pub(crate) node_id: String,
    pub(crate) agent_id: Option<String>,
    /// The step as the recipe shows it.
    pub(crate) name: String,
    /// Empty for a pin.
    pub(crate) allotment: Vec<AllotmentEntry>,
    pub(crate) pin: Option<ModelSelection>,
    /// The upstream steps (by name) a constrained preview read.
    pub(crate) depends_on: Vec<String>,
    pub(crate) pick: Result<StepPick, String>,
}

impl StepPreview {
    pub(crate) fn record(&self) -> Option<&SelectionRecord> {
        match &self.pick {
            Ok(StepPick::Chosen(record) | StepPick::Predicted(record)) => Some(record),
            _ => None,
        }
    }

    /// The model the step runs or would run.
    fn model(&self) -> Option<String> {
        self.pin
            .as_ref()
            .map(model_ref)
            .or_else(|| self.record().map(|record| model_ref(&record.chosen)))
    }

    /// The reply's line for this step. `now` marks a prediction, which each
    /// step makes for itself at its own launch. It names the model and effort
    /// only, never the account or its usage.
    pub(crate) fn line(&self) -> String {
        let name = &self.name;
        match (&self.pick, &self.pin) {
            (Ok(StepPick::Chosen(record)), _) => format!("{name} chose: {}", record.pick_line()),
            (Ok(_), Some(pin)) => format!("{name} pinned: {}", model_ref(pin)),
            (Ok(StepPick::Predicted(record)), None) => match self.allotment.as_slice() {
                [AllotmentEntry::Model(model)] => format!("{name} = {} (exact)", model_ref(model)),
                _ if self.depends_on.is_empty() => format!("{name} now: {}", record.pick_line()),
                _ => format!(
                    "{name} now, after {}: {}",
                    self.depends_on.join(", "),
                    record.pick_line()
                ),
            },
            (Ok(StepPick::PinnedUnplaced), None) => format!("{name} pinned"),
            (Err(error), _) => format!("{name} cannot launch now: {error}"),
        }
    }

    /// The step as a resolution record stores it.
    pub(crate) fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "node": self.node_id,
            "allotment": self.allotment,
            "pinned": self.pin.is_some(),
            "model": self.model(),
            "provenance": self.record().map(|record| record.provenance.clone()),
            "dependsOn": self.depends_on,
            "stored": matches!(self.pick, Ok(StepPick::Chosen(_))),
            "error": self.pick.as_ref().err(),
        })
    }
}

/// Preview `steps`, in order, of `recipe`.
///
/// `select` is the selection a launch makes; `serves_session` is the session
/// door's admission for a pin.
pub(crate) fn preview_steps(
    recipe: &RecipeSnapshot,
    steps: &[StepInput],
    select: impl Fn(&[AllotmentEntry], &ResolvedConstraints) -> Result<SelectionRecord, String>,
    serves_session: impl Fn(&ModelSelection) -> Result<(), String>,
) -> Vec<StepPreview> {
    let name_of = |id: &str| {
        recipe
            .nodes
            .iter()
            .find(|node| node.id == id)
            .map_or_else(|| id.to_string(), |node| node.name.clone())
    };

    // First pass: every step that has not launched, on its own. A pin is
    // previewed as a one-model allotment, where its launch stores
    // `pinned_selection`'s record: both name the same model, band and maker,
    // but a pin the catalog cannot place has no first-pass pick here, so a
    // step constrained against it previews without that constraint.
    // `PinnedUnplaced` says so on the pinned step's own line.
    let first: Vec<Option<Result<SelectionRecord, String>>> = steps
        .iter()
        .map(|step| {
            if step.stored.is_some() {
                return None;
            }
            Some(match &step.pin {
                Some(pin) => serves_session(pin)
                    .map_err(|why| format!("{}: {why}", model_ref(pin)))
                    .and_then(|()| {
                        select(
                            &[AllotmentEntry::Model(pin.clone())],
                            &ResolvedConstraints::default(),
                        )
                    }),
                None => select(&step.allotment, &ResolvedConstraints::default()),
            })
        })
        .collect();
    let picks: HashMap<String, SelectionRecord> = steps
        .iter()
        .zip(&first)
        .filter_map(|(step, first)| {
            let record = match (&step.stored, first) {
                (Some(stored), _) => stored,
                (None, Some(Ok(record))) => record,
                _ => return None,
            };
            Some((step.node_id.clone(), record.clone()))
        })
        .collect();

    steps
        .iter()
        .zip(first)
        .map(|(step, first)| {
            let allotment = if step.pin.is_some() {
                Vec::new()
            } else {
                step.allotment.clone()
            };
            let mut preview = StepPreview {
                node_id: step.node_id.clone(),
                agent_id: step.agent_id.clone(),
                name: name_of(&step.node_id),
                allotment,
                pin: step.pin.clone(),
                depends_on: Vec::new(),
                pick: Err(String::new()),
            };
            let first = match (&step.stored, first) {
                (Some(stored), _) => {
                    preview.pick = Ok(StepPick::Chosen(stored.clone()));
                    return preview;
                }
                (None, Some(first)) => first,
                (None, None) => Err(format!("{} was not previewed", preview.name)),
            };

            // Second pass: a constrained step reads its upstream's pick.
            let node = recipe.nodes.iter().find(|node| node.id == step.node_id);
            let constraints = node
                .and_then(|node| node.agent_config.as_ref())
                .map(|config| config.constraints.clone())
                .unwrap_or_default();
            let constraint_error = if step.pin.is_none() && !constraints.is_empty() {
                node.and_then(|node| crate::execution::recipe::constraint_problem(recipe, node))
            } else {
                None
            };
            let record =
                if step.pin.is_some() || constraints.is_empty() || constraint_error.is_some() {
                    first
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
                            if !preview.depends_on.contains(&name) {
                                preview.depends_on.push(name.clone());
                            }
                            // The provenance names the upstream step as the
                            // recipe shows it, not by its id.
                            *slot = picks.get(reference).cloned().map(|record| (name, record));
                        }
                    }
                    select(&step.allotment, &resolved)
                };
            preview.pick = match (constraint_error, record, &step.pin) {
                (Some(error), _, _) => Err(error),
                (None, Ok(record), _) => Ok(StepPick::Predicted(record)),
                (None, Err(_), Some(pin)) if serves_session(pin).is_ok() => {
                    Ok(StepPick::PinnedUnplaced)
                }
                (None, Err(error), _) => Err(error),
            };
            preview
        })
        .collect()
}

/// Whether a selection can hold a session in `project_id`: the answer the
/// session door gives at launch, asked of one provider record per backend.
pub(crate) fn session_admitter<'a>(
    orch: &'a Orchestrator,
    project_id: &'a str,
) -> impl Fn(&ModelSelection) -> Result<(), String> + 'a {
    let providers = std::cell::RefCell::new(HashMap::new());
    move |selection: &ModelSelection| {
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
    }
}

/// One node's live job, as the launch reads it.
struct NodeJob {
    model: Option<String>,
    selection: Option<SelectionRecord>,
}

async fn load_snapshot_and_jobs(
    db: &LocalDb,
    execution_id: &str,
) -> Result<(ExecutionSnapshot, HashMap<String, NodeJob>), String> {
    let execution_id = execution_id.to_string();
    let (snapshot, jobs) = db
        .read(|conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT snapshot FROM executions WHERE id = ?1",
                        params![execution_id.as_str()],
                    )
                    .await?;
                let snapshot = match rows.next().await? {
                    Some(row) => row.opt_text(0)?,
                    None => None,
                };
                drop(rows);
                // The live attempt per node, as `load_node_selection_conn`
                // reads it: the newest job that was not cancelled.
                let mut rows = conn
                    .query(
                        "SELECT recipe_node_id, model, selection FROM jobs
                          WHERE execution_id = ?1 AND recipe_node_id IS NOT NULL
                            AND status <> 'cancelled'
                          ORDER BY created_at DESC",
                        params![execution_id.as_str()],
                    )
                    .await?;
                let mut jobs = Vec::new();
                while let Some(row) = rows.next().await? {
                    jobs.push((row.text(0)?, row.opt_text(1)?, row.opt_text(2)?));
                }
                Ok((snapshot, jobs))
            })
        })
        .await
        .map_err(|error| format!("Failed to load the execution: {error}"))?;
    let snapshot = snapshot.ok_or("the execution has no snapshot")?;
    let snapshot = crate::config::snapshot_migrate::load(&snapshot)
        .map_err(|error| format!("the execution's snapshot does not parse: {error}"))?;
    let mut by_node = HashMap::new();
    for (node, model, selection) in jobs {
        if by_node.contains_key(&node) {
            continue;
        }
        let selection = selection
            .map(|json| serde_json::from_str(&json))
            .transpose()
            .map_err(|error| format!("a stored selection does not parse: {error}"))?;
        by_node.insert(node, NodeJob { model, selection });
    }
    Ok((snapshot, by_node))
}

/// Preview every agent step of a stored execution, held or started, the way
/// each step's own launch will read its snapshot: its node's allotment, else
/// its agent's, else the workspace default; its job's pin; and the choices
/// steps that have launched already stored.
pub(crate) async fn preview_execution(
    orch: &Orchestrator,
    db: &LocalDb,
    execution_id: &str,
    project_id: &str,
) -> Result<Vec<StepPreview>, String> {
    let (snapshot, jobs) = load_snapshot_and_jobs(db, execution_id).await?;
    preview_with_jobs(orch, &snapshot, &jobs, project_id)
}

/// Preview every agent step of a snapshot no execution holds yet: what each
/// step's launch would select now.
pub(crate) fn preview_snapshot(
    orch: &Orchestrator,
    snapshot: &ExecutionSnapshot,
    project_id: &str,
) -> Result<Vec<StepPreview>, String> {
    preview_with_jobs(orch, snapshot, &HashMap::new(), project_id)
}

fn preview_with_jobs(
    orch: &Orchestrator,
    snapshot: &ExecutionSnapshot,
    jobs: &HashMap<String, NodeJob>,
    project_id: &str,
) -> Result<Vec<StepPreview>, String> {
    let defaults = orch.effective_models(Some(project_id))?.default_models;
    let steps: Vec<StepInput> = snapshot
        .recipe
        .nodes
        .iter()
        .filter(|node| node.node_type == RecipeNodeType::Agent)
        .filter_map(|node| {
            let config = node.agent_config.as_ref()?;
            let agent_id = config.agent_config_id.as_deref()?;
            let agent = snapshot.agents.get(agent_id);
            let job = jobs.get(&node.id);
            let stored = job.and_then(|job| job.selection.clone());
            // A launched job's model is its stored choice, not a pin.
            let pin = match stored {
                Some(_) => None,
                None => crate::execution::jobs::pinned(
                    job.and_then(|job| job.model.as_deref()),
                    agent.and_then(|agent| agent.selection.as_ref()),
                ),
            };
            Some(StepInput {
                node_id: node.id.clone(),
                agent_id: Some(agent_id.to_string()),
                allotment: effective_allotment(
                    config.models.as_deref(),
                    agent.map_or(&[][..], |agent| agent.models.as_slice()),
                    &defaults,
                ),
                pin,
                stored,
            })
        })
        .collect();
    let serves_session = session_admitter(orch, project_id);
    Ok(preview_steps(
        &snapshot.recipe,
        &steps,
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
    ))
}
