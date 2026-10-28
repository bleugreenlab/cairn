//! The commissioning gate: a thread's filing of a high-rung recipe is born
//! HELD on the operator's verdict.
//!
//! A thread may file repair (`build`) on its own judgment, but the recipes
//! that commission new work (the project's operator-only
//! `tenet_gated_recipes`: planbuild, coordinator, and PlanCoord by default)
//! wait for the operator. Such a filing carries exactly one of:
//!
//! - `proposal: {statement, rationale?}`, proposed to the operator on the spot; or
//! - `tenet: <n>`, one of the same thread's proposed or approved tenets, which
//!   is how one approval releases every filing of a whole plan.
//!
//! The filing then gains the gate clause
//! `{fact: "tenet_verdict", tenet: <uri>, outcome: <release policy>}`, with the
//! policy stamped at filing time so the gate describes itself. The two keys
//! mean the same on an ungated recipe and are honored there; they are simply
//! not required.
//!
//! Only a thread's filing is gated. A person, or a builder or coordinator
//! working an issue, acts for no thread and files exactly as before.

use serde_json::Value;

use super::{
    tenet_uri, Proposal, Proposer, TenetOrigin, TenetRelease, TenetRow, TenetStatus,
    TENET_GATE_FACT,
};
use crate::orchestrator::Orchestrator;
use crate::routes::TriggerClause;
use crate::storage::LocalDb;

/// What a filing says about its tenet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenetClaim {
    /// Propose this statement now and hold on its verdict.
    Proposal {
        statement: String,
        rationale: Option<String>,
    },
    /// Hold on this existing tenet of the filing thread.
    Existing(i64),
}

/// A filing's tenet keys, and the run that filed it.
#[derive(Debug, Clone, Default)]
pub struct TenetFiling {
    /// The authenticated run behind the filing, when an agent filed it.
    pub caller_run_id: Option<String>,
    pub claim: Option<TenetClaim>,
    /// Where the keys sit in the payload (`payload`, `payload.execution`), for
    /// refusals that name them as the caller wrote them.
    pub key_prefix: &'static str,
}

impl TenetFiling {
    /// A filing from a caller with no run and no tenet keys: a person, a
    /// schedule, a route, the memory triage.
    pub fn unattributed() -> Self {
        Self::default()
    }

    /// The tenet keys of a payload object, with `key_prefix` naming where the
    /// object sits.
    pub fn from_payload(
        caller_run_id: Option<String>,
        object: Option<&serde_json::Map<String, Value>>,
        key_prefix: &'static str,
    ) -> Result<Self, String> {
        let field = |key: &str| {
            object
                .and_then(|object| object.get(key))
                .filter(|v| !v.is_null())
        };
        let claim = match (field("proposal"), field("tenet")) {
            (Some(_), Some(_)) => {
                return Err(format!(
                    "send exactly one of {key_prefix}.proposal or {key_prefix}.tenet, not both: \
                     a filing holds on one tenet"
                ))
            }
            (Some(proposal), None) => Some(parse_proposal(proposal, key_prefix)?),
            (None, Some(tenet)) => Some(TenetClaim::Existing(
                tenet
                    .as_i64()
                    .filter(|number| *number > 0)
                    .ok_or_else(|| format!("{key_prefix}.tenet must be a tenet number, like 12"))?,
            )),
            (None, None) => None,
        };
        Ok(Self {
            caller_run_id,
            claim,
            key_prefix,
        })
    }
}

fn parse_proposal(value: &Value, key_prefix: &str) -> Result<TenetClaim, String> {
    let shape = || {
        format!(
            "{key_prefix}.proposal must be {{statement: string, rationale?: string}}: the statement \
             is one sentence of how the project does things, for the operator to answer yes or no; \
             the rationale carries why, the alternatives ruled out, and where it was argued"
        )
    };
    let object = value.as_object().ok_or_else(shape)?;
    if object.contains_key("question") || object.contains_key("options") {
        return Err(super::retired_proposal_shape_refusal(&format!(
            "{key_prefix}.proposal."
        )));
    }
    if object
        .keys()
        .any(|key| key != "statement" && key != "rationale")
    {
        return Err(shape());
    }
    let statement = object
        .get("statement")
        .and_then(Value::as_str)
        .ok_or_else(shape)?
        .to_string();
    let rationale = match object.get("rationale") {
        None | Some(Value::Null) => None,
        Some(value) => Some(value.as_str().ok_or_else(shape)?.to_string()),
    };
    Ok(TenetClaim::Proposal {
        statement,
        rationale,
    })
}

/// The refusal for a caller that names a tenet while acting for no thread.
pub const NON_THREAD_FILING_REFUSAL: &str = "`proposal` and `tenet` belong to a thread's filing, and this caller acts for no thread, so its filing is not tenet-gated: omit them. To hold an execution on a tenet anyway, name it in `after` as {fact:\"tenet_verdict\", tenet:\"cairn://p/PROJECT/tenets/N\", outcome:\"answered\"}.";

fn missing_tenet_refusal(recipe: &str, key_prefix: &str) -> String {
    format!(
        "a thread files `{recipe}` only behind a tenet the operator answers. Send exactly one of \
         {key_prefix}.proposal: {{statement, rationale?}}, one sentence of how the project does \
         things proposed to the operator now for a yes or no, or {key_prefix}.tenet: <n>, one of your thread's proposed or approved \
         tenets (one approval can release every filing of a plan). The execution is filed HELD \
         until the operator answers. `build` is for repair only, not a way around this."
    )
}

/// A checked filing, before anything is written: who files, and on which
/// tenet it will hold.
#[derive(Debug, Clone)]
pub(crate) struct FilingPlan {
    project_key: String,
    project_id: String,
    thread_id: String,
    run_id: String,
    release: TenetRelease,
    claim: PlannedClaim,
}

#[derive(Debug, Clone)]
enum PlannedClaim {
    Propose(Proposal),
    Existing(Box<TenetRow>),
}

/// The final recipe id a filing launches. There is no project or workspace
/// default recipe (the launch refuses a filing that names none), so a filing
/// without one has nothing to gate and fails at launch.
fn final_recipe(recipe: Option<&str>) -> Option<&str> {
    recipe.map(str::trim).filter(|recipe| !recipe.is_empty())
}

/// The refusal for a thread commissioning work in a project that is not its
/// own: its tenets live in its own project, so it has none to hold on here.
pub const CROSS_PROJECT_REFUSAL: &str = "a thread commissions work only in its own project: this caller is a thread of another project, so it cannot propose or hold on a tenet here, file a recipe this project gates behind a tenet, or lift a tenet gate here. Ask a thread of this project, or put the request to the operator.";

/// The thread an authenticated run acts for, `(thread_id, thread's
/// project_id)`, read on the database that owns the RUN, which need not be
/// the one the filing lands in.
async fn caller_thread(
    dbs: &crate::db::DbState,
    run_id: &str,
) -> Result<Option<(String, String)>, String> {
    let db = crate::execution::routing::routing_db_for_id(dbs, run_id)
        .await
        .map_err(|error| format!("Failed to route the filing run: {error}"))?;
    let run = run_id.to_string();
    db.read(move |conn| {
        let run = run.clone();
        Box::pin(
            async move { crate::mcp::handlers::issues::acting_thread_for_run(conn, &run).await },
        )
    })
    .await
    .map_err(|error| format!("Failed to resolve the filing thread: {error}"))
}

/// Check a filing against the commissioning gate without writing anything.
///
/// `None` when the filing is not tenet-gated: the caller acts for no thread
/// and named no tenet, or the recipe is ungated and it named none. Refuses a
/// thread's gated filing that names no tenet, a filing naming two, a tenet
/// that cannot hold it, a malformed proposal, and a thread of another project
/// commissioning gated work here.
///
/// `recipe` must be the id the launch resolves
/// ([`crate::execution::recipe::resolve_filed_recipe_id`]), never the
/// caller's spelling: the gate compares ids, and only the resolved id is the
/// recipe that will run.
pub(crate) async fn plan(
    dbs: &crate::db::DbState,
    db: &LocalDb,
    project_key: &str,
    project_id: &str,
    recipe: Option<&str>,
    filing: &TenetFiling,
) -> Result<Option<FilingPlan>, String> {
    let thread = match filing.caller_run_id.as_deref() {
        Some(run_id) => caller_thread(dbs, run_id)
            .await?
            .map(|(thread_id, thread_project)| (thread_id, thread_project, run_id.to_string())),
        None => None,
    };
    let Some((thread_id, thread_project, run_id)) = thread else {
        return match filing.claim {
            Some(_) => Err(NON_THREAD_FILING_REFUSAL.to_string()),
            None => Ok(None),
        };
    };
    let policy = super::load_policy(db, project_id).await?;
    let recipe = final_recipe(recipe);
    let gated = recipe.is_some_and(|recipe| policy.gates_recipe(recipe));
    if thread_project != project_id {
        return if filing.claim.is_some() || gated {
            Err(CROSS_PROJECT_REFUSAL.to_string())
        } else {
            Ok(None)
        };
    }
    let claim = match (&filing.claim, recipe) {
        (Some(claim), _) => claim.clone(),
        (None, Some(recipe)) if gated => {
            return Err(missing_tenet_refusal(recipe, filing.key_prefix))
        }
        (None, _) => return Ok(None),
    };
    let claim = match claim {
        TenetClaim::Proposal {
            statement,
            rationale,
        } => {
            let proposal = Proposal {
                statement,
                rationale,
                supersedes: None,
                origin: TenetOrigin::FilingGate,
            };
            super::write::validate(&proposal)?;
            PlannedClaim::Propose(proposal)
        }
        TenetClaim::Existing(number) => PlannedClaim::Existing(Box::new(
            existing_tenet(db, project_id, &thread_id, number).await?,
        )),
    };
    Ok(Some(FilingPlan {
        project_key: project_key.to_string(),
        project_id: project_id.to_string(),
        thread_id,
        run_id,
        release: policy.release,
        claim,
    }))
}

/// Tenet `number` when it can hold a filing by `thread_id`: one of that
/// thread's own tenets, still awaiting a verdict or approved and in force.
async fn existing_tenet(
    db: &LocalDb,
    project_id: &str,
    thread_id: &str,
    number: i64,
) -> Result<TenetRow, String> {
    let project = project_id.to_string();
    let row = db
        .read(move |conn| {
            let project = project.clone();
            Box::pin(async move { super::row_conn(conn, &project, number).await })
        })
        .await
        .map_err(|error| format!("Failed to read tenet #{number}: {error}"))?
        .ok_or_else(|| format!("no tenet #{number} in this project"))?;
    if row.proposing_thread_id != thread_id {
        return Err(format!(
            "tenet #{number} was proposed by another thread; a filing holds on one of its own \
             thread's tenets, or proposes one"
        ));
    }
    match row.status {
        TenetStatus::Proposed | TenetStatus::Approved => Ok(row),
        TenetStatus::Declined => Err(format!(
            "tenet #{number} was declined, so nothing filed on it could ever start; propose again"
        )),
        TenetStatus::Withdrawn => Err(format!("tenet #{number} was withdrawn; propose again")),
        TenetStatus::Superseded => Err(format!(
            "tenet #{number} was superseded by a later tenet; name the tenet that replaced it"
        )),
    }
}

/// Refuse a thread's change to a held execution's gates that would lift a
/// tenet gate around the operator.
///
/// The commissioning gate would mean nothing if the thread it holds could
/// release its own filing early or patch the tenet off. So while an execution
/// waits on an unmet tenet gate, a caller acting for a thread may not release
/// it, and may replace its gates only with a list that still holds on one of
/// its own proposed or approved tenets under the project's release policy
/// (how it asks again after a decline). `replacement` is `None` for a
/// release. A caller acting for no thread (a person, a coordinator) is not
/// held to this; the gate binds threads. A thread of another project may not
/// lift a tenet gate here at all.
pub(crate) async fn guard_gate_change(
    dbs: &crate::db::DbState,
    db: &LocalDb,
    project_key: &str,
    project_id: &str,
    execution_id: &str,
    caller_run_id: Option<&str>,
    replacement: Option<&[TriggerClause]>,
) -> Result<(), String> {
    let Some(run_id) = caller_run_id else {
        return Ok(());
    };
    let Some((thread_id, thread_project)) = caller_thread(dbs, run_id).await? else {
        return Ok(());
    };
    let execution = execution_id.to_string();
    let waiting = db
        .read(move |conn| {
            let execution = execution.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT json_extract(predicate_json, '$.tenet') FROM execution_gates
                          WHERE execution_id = ?1 AND satisfied_at IS NULL
                            AND json_extract(predicate_json, '$.fact') = ?2",
                        cairn_db::turso::params![execution.as_str(), TENET_GATE_FACT],
                    )
                    .await?;
                let mut waiting = Vec::new();
                while let Some(row) = rows.next().await? {
                    use crate::storage::RowExt;
                    waiting.push(row.opt_text(0)?.unwrap_or_default());
                }
                Ok(waiting)
            })
        })
        .await
        .map_err(|error| format!("Failed to read the execution's tenet gates: {error}"))?;
    if waiting.is_empty() {
        return Ok(());
    }
    if thread_project != project_id {
        return Err(CROSS_PROJECT_REFUSAL.to_string());
    }
    let waits_on = waiting.join(", ");
    let Some(replacement) = replacement else {
        return Err(format!(
            "this execution waits on the operator's verdict on {waits_on}; a thread does not \
             release its own tenet-gated filing early. Only the operator answers it."
        ));
    };
    let policy = super::load_policy(db, project_id).await?;
    let mut holds = false;
    for clause in replacement {
        if clause.get("fact").and_then(Value::as_str) != Some(TENET_GATE_FACT) {
            continue;
        }
        let raw = clause
            .get("tenet")
            .and_then(Value::as_str)
            .unwrap_or_default();
        // The whole identity, not just the number: a same-numbered tenet in
        // another project is a different tenet, and it is the one the sweep
        // would read.
        let (tenet_project, number) = crate::execution::gates::vocabulary::parse_tenet_uri(raw)?;
        if !tenet_project.eq_ignore_ascii_case(project_key) {
            return Err(format!(
                "{raw} is another project's tenet; a filing here holds on one of this project's tenets"
            ));
        }
        existing_tenet(db, project_id, &thread_id, number).await?;
        if clause.get("outcome").and_then(Value::as_str) != Some(policy.release.as_str()) {
            return Err(format!(
                "a tenet gate in this project releases on `{}`; write outcome:\"{}\"",
                policy.release.as_str(),
                policy.release.as_str()
            ));
        }
        holds = true;
    }
    if holds {
        Ok(())
    } else {
        Err(format!(
            "this execution waits on the operator's verdict on {waits_on}; a thread may move it \
             onto another of its own tenets, but not off a tenet gate. To ask again, propose a \
             new tenet and keep a tenet_verdict clause naming it in `after`."
        ))
    }
}

/// The tenet a filing holds on, once its proposal (if any) is written.
#[derive(Debug, Clone)]
pub(crate) struct FilingGate {
    pub(crate) uri: String,
    pub(crate) number: i64,
    /// The clause appended to the filing's `after`.
    pub(crate) clause: TriggerClause,
    /// This filing proposed the tenet, and withdraws it if the launch fails.
    proposed: bool,
    project_id: String,
    thread_id: String,
}

impl FilingPlan {
    /// Propose the tenet when the filing asked to, and build its gate clause.
    pub(crate) async fn apply(
        self,
        orch: &Orchestrator,
        db: &LocalDb,
    ) -> Result<FilingGate, String> {
        let (tenet, proposed) = match self.claim {
            PlannedClaim::Existing(row) => (*row, false),
            PlannedClaim::Propose(proposal) => {
                let agent = crate::mcp::handlers::run_context::authenticated_agent_for_run(
                    &orch.db,
                    &self.run_id,
                )
                .await?;
                let (principal, _) = agent.appearance(orch.services.clock.now())?;
                let proposer = Proposer {
                    project_id: self.project_id.clone(),
                    thread_id: self.thread_id.clone(),
                    principal,
                };
                let row = super::propose(
                    db,
                    orch.services.emitter.as_ref(),
                    orch.services.clock.now(),
                    &proposer,
                    proposal,
                )
                .await?;
                (row, true)
            }
        };
        let uri = tenet_uri(&self.project_key, tenet.number);
        let clause = TriggerClause::from([
            ("fact".to_string(), Value::String(TENET_GATE_FACT.into())),
            ("tenet".to_string(), Value::String(uri.clone())),
            (
                "outcome".to_string(),
                Value::String(self.release.as_str().into()),
            ),
        ]);
        Ok(FilingGate {
            uri,
            number: tenet.number,
            clause,
            proposed,
            project_id: self.project_id,
            thread_id: self.thread_id,
        })
    }
}

impl FilingGate {
    /// Undo this filing's proposal after its launch failed, so the operator is
    /// never asked a question whose filing does not exist. Best effort: a
    /// proposal that cannot be withdrawn is logged, not raised over the
    /// launch failure the caller is already reporting.
    pub(crate) async fn compensate(&self, orch: &Orchestrator, db: &LocalDb) {
        if !self.proposed {
            return;
        }
        if let Err(error) = super::withdraw(
            db,
            orch.services.emitter.as_ref(),
            orch.services.clock.now(),
            &self.project_id,
            self.number,
            &self.thread_id,
        )
        .await
        {
            log::warn!(
                "tenet {} was proposed for a filing whose launch failed, and could not be withdrawn: {error}",
                self.uri
            );
        }
    }

    /// The tenet's URI when this filing proposed it: the proposal the operator
    /// is now asked to answer, as opposed to an existing tenet it names.
    pub(crate) fn proposed_uri(&self) -> Option<&str> {
        self.proposed.then_some(self.uri.as_str())
    }

    /// What a held filing says about its tenet.
    pub(crate) fn held_phrase(&self) -> String {
        let how = if self.proposed {
            "proposed by this filing"
        } else {
            "awaiting the operator"
        };
        format!(
            " It waits on tenet {} ({how}) for the operator's verdict; you hear the verdict \
             when it is given.",
            self.uri
        )
    }

    /// What a filing that started at once says about its tenet: only an
    /// approved tenet's gate is met at creation.
    pub(crate) fn started_phrase(&self) -> String {
        format!(" Tenet {} is already approved.", self.uri)
    }
}
