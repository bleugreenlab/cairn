//! Resolving a gated artifact: who may confirm it, and the one write that does.
//!
//! Every door that confirms a `confirmPolicy: user` artifact — a resource
//! `patch {confirmed:true}`, the `approve_job` invoke, and `update_job_status`
//! to `complete` — goes through [`confirm_gated_artifact`]. The confirming
//! principal is what the carrier authenticated, never anything in the payload.
//! The rule itself is stated in `docs/authorization.md` ("Who may confirm a
//! gated artifact").

use cairn_attention::wakes::GateOwner;
use cairn_common::authorization::{
    AuthorityAction, AuthorityAudience, AuthorityPlace, AuthorityPrincipal, AuthorityScope,
};
use cairn_db::lineage::FilingParent;
use cairn_db::turso::params;

use crate::models::Job;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt, TrackedConnection};
pub use cairn_workflow::checkpoints::*;

/// Who is confirming, as the carrier authenticated them.
#[derive(Debug, Clone)]
pub enum Confirmer {
    /// The operator, proven by the invoke adapter's own authentication context.
    Operator(cairn_common::identity::PrincipalRef),
    /// An agent's live run id, taken from its authenticated request. Its job
    /// and home are resolved from stored rows here.
    AgentRun(String),
    /// A carrier that proved neither, named for the refusal and its journal row.
    Unproven(&'static str),
}

/// Why a confirmation did not happen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfirmError {
    /// The confirmer may not confirm this gate. Nothing changed and the gate
    /// is still pending.
    Refused(ConfirmRefusal),
    /// The gate could not be confirmed for another reason (nothing to confirm,
    /// an unknown job, a storage failure).
    Failed(String),
}

impl std::fmt::Display for ConfirmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfirmError::Refused(refusal) => refusal.fmt(f),
            ConfirmError::Failed(error) => f.write_str(error),
        }
    }
}

impl From<ConfirmError> for String {
    fn from(error: ConfirmError) -> Self {
        error.to_string()
    }
}

/// A refused confirmation: whom the gate waits on, who may therefore confirm
/// it, and why this confirmer is not one of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmRefusal {
    pub gate_owner: GateOwner,
    /// Display of everyone who may confirm, e.g. `cairn://p/P/design or the operator`.
    pub may_confirm: String,
    pub reason: String,
}

impl std::fmt::Display for ConfirmRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Only {} may confirm this gated artifact. {} The gate is still pending.",
            self.may_confirm, self.reason
        )
    }
}

/// Confirm `job_id`'s gated artifact as `confirmer`, then advance the DAG.
///
/// The gate is validated first, so "nothing to confirm" reads the same to
/// everyone. An unauthorized confirmer is then refused before anything is
/// written, and the refusal is journaled.
pub async fn confirm_gated_artifact(
    orch: &Orchestrator,
    job_id: &str,
    confirmer: &Confirmer,
) -> Result<Vec<Job>, ConfirmError> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| ConfirmError::Failed(error.to_string()))?;
    validate_confirmable(db.as_ref(), job_id)
        .await
        .map_err(ConfirmError::Failed)?;
    if let Err(error) = authorize(orch, &db, job_id, confirmer).await {
        if let ConfirmError::Refused(refusal) = &error {
            journal_refusal(orch, &db, job_id, confirmer, refusal).await?;
        }
        return Err(error);
    }

    // Record the resolution fact (artifact.confirmed), then let the projection
    // derive Complete and advance the DAG. Agent jobs from the resulting
    // AdvanceDag are started by the executor directly.
    let job_id_owned = job_id.to_string();
    let artifact = db
        .write(|conn| {
            let job_id = job_id_owned.clone();
            Box::pin(async move { confirm_latest_artifact_conn(conn, &job_id).await })
        })
        .await
        .map_err(|error| ConfirmError::Failed(error.to_string()))?;
    orch.notifier.artifact(&db, &artifact).await;
    crate::execution::advancement::recompute_job(orch, job_id).map_err(ConfirmError::Failed)?;
    Ok(vec![])
}

/// The authorization [`confirm_gated_artifact`] applies, without confirming or
/// journaling: what a dry run reports.
pub async fn check_confirmation(
    orch: &Orchestrator,
    job_id: &str,
    confirmer: &Confirmer,
) -> Result<(), ConfirmError> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| ConfirmError::Failed(error.to_string()))?;
    authorize(orch, &db, job_id, confirmer).await
}

/// What the gate's own rows say: the chain that produced the artifact, whom
/// the gate waits on, and whether the parent edge that routing followed
/// carries filing proof.
struct GateFacts {
    producer_root: String,
    owner: GateOwner,
    owner_label: Option<String>,
    edge_proven: bool,
}

/// The authenticated confirmer's own rows.
struct ConfirmerFacts {
    job_id: String,
    root: String,
    thread_session_of: Option<String>,
}

async fn authorize(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    confirmer: &Confirmer,
) -> Result<(), ConfirmError> {
    if matches!(confirmer, Confirmer::Operator(_)) {
        return Ok(());
    }
    let gate = gate_facts(db, job_id).await?;
    let may_confirm = match (&gate.owner, &gate.owner_label, gate.edge_proven) {
        (GateOwner::Operator, _, _) | (_, _, false) => "the operator".to_string(),
        (_, Some(label), true) => format!("{label} (this issue's gate owner) or the operator"),
        (_, None, true) => "this issue's gate owner or the operator".to_string(),
    };
    let refuse = |reason: String| {
        Err(ConfirmError::Refused(ConfirmRefusal {
            gate_owner: gate.owner.clone(),
            may_confirm: may_confirm.clone(),
            reason,
        }))
    };
    let run_id = match confirmer {
        Confirmer::AgentRun(run_id) => run_id.as_str(),
        Confirmer::Unproven(carrier) => {
            return refuse(format!(
                "This {carrier} carries neither operator authority nor an authenticated agent run."
            ))
        }
        Confirmer::Operator(_) => return Ok(()),
    };
    let caller = match confirmer_facts(orch, run_id).await {
        Ok(Some(caller)) => caller,
        Ok(None) | Err(_) => {
            return refuse("The confirming run is not an authenticated agent run.".to_string())
        }
    };
    if caller.root == gate.producer_root {
        return refuse(
            "The confirming run belongs to the chain that produced this artifact, and nobody confirms their own gate."
                .to_string(),
        );
    }
    let is_owner = match &gate.owner {
        GateOwner::Job(owner) => &caller.job_id == owner,
        GateOwner::Thread(owner) => caller.thread_session_of.as_ref() == Some(owner),
        GateOwner::Operator => false,
    };
    match (&gate.owner, is_owner, gate.edge_proven) {
        (GateOwner::Operator, _, _) => refuse(
            "No thread or node coordinates this issue, so its gate waits on the operator."
                .to_string(),
        ),
        (_, false, _) => refuse("The confirming run is not this issue's gate owner.".to_string()),
        (_, true, false) => refuse(
            "The issue's parent edge carries no filing proof (it was adopted, reparented, or filed before filing proof existed), so routing to its parent confers no authority to confirm."
                .to_string(),
        ),
        (_, true, true) => Ok(()),
    }
}

async fn gate_facts(db: &LocalDb, job_id: &str) -> Result<GateFacts, ConfirmError> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let producer_root = chain_root_conn(conn, &job_id)
                .await?
                .unwrap_or_else(|| job_id.clone());
            let mut rows = conn
                .query(
                    "SELECT COALESCE(j.issue_id, e.issue_id) FROM jobs j
                     LEFT JOIN executions e ON e.id = j.execution_id WHERE j.id = ?1",
                    params![producer_root.as_str()],
                )
                .await?;
            let issue_id = match rows.next().await? {
                Some(row) => row.opt_text(0)?,
                None => None,
            };
            drop(rows);
            // An artifact that belongs to no issue has no parent to route to.
            let Some(issue_id) = issue_id else {
                return Ok(GateFacts {
                    producer_root,
                    owner: GateOwner::Operator,
                    owner_label: None,
                    edge_proven: false,
                });
            };
            let owner =
                crate::orchestrator::wakes::gate_owner_for_child_conn(conn, &issue_id).await?;
            let (edge_proven, owner_label) = match &owner {
                GateOwner::Job(owner_job) => (
                    filing_edge_proven_conn(conn, &issue_id, FilingParent::Issue).await?,
                    cairn_db::lineage::home_uri_for_job_conn(conn, owner_job).await?,
                ),
                GateOwner::Thread(thread_id) => (
                    filing_edge_proven_conn(conn, &issue_id, FilingParent::Thread).await?,
                    thread_uri_conn(conn, thread_id).await?,
                ),
                GateOwner::Operator => (false, None),
            };
            Ok(GateFacts {
                producer_root,
                owner,
                owner_label,
                edge_proven,
            })
        })
    })
    .await
    .map_err(|error| ConfirmError::Failed(error.to_string()))
}

async fn confirmer_facts(
    orch: &Orchestrator,
    run_id: &str,
) -> Result<Option<ConfirmerFacts>, String> {
    let agent =
        crate::mcp::handlers::run_context::authenticated_agent_for_run(&orch.db, run_id).await?;
    let db = crate::execution::routing::routing_db_for_id(&orch.db, run_id)
        .await
        .map_err(|error| error.to_string())?;
    let job_id = agent.job_id.clone();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let Some(root) = chain_root_conn(conn, &job_id).await? else {
                return Ok(None);
            };
            let mut rows = conn
                .query(
                    &format!(
                        "SELECT CASE WHEN {} THEN j.thread_id END FROM jobs j WHERE j.id = ?1",
                        cairn_db::lineage::SESSION_JOB_SHAPE
                    ),
                    params![job_id.as_str()],
                )
                .await?;
            let thread_session_of = match rows.next().await? {
                Some(row) => row.opt_text(0)?,
                None => None,
            };
            Ok(Some(ConfirmerFacts {
                job_id: job_id.clone(),
                root,
                thread_session_of,
            }))
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The recipe-root job above `job_id` (itself when it has no parent): the
/// producing node of a chain whose delegated tasks hang beneath it.
async fn chain_root_conn(conn: &TrackedConnection, job_id: &str) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "WITH RECURSIVE up(id, parent, depth) AS (
                 SELECT id, parent_job_id, 0 FROM jobs WHERE id = ?1
                 UNION ALL
                 SELECT j.id, j.parent_job_id, up.depth + 1 FROM jobs j
                 JOIN up ON j.id = up.parent WHERE up.depth < 64
             )
             SELECT id FROM up WHERE parent IS NULL LIMIT 1",
            params![job_id],
        )
        .await?;
    crate::storage::next_text(&mut rows, 0).await
}

/// Whether the child's parent edge on `arm`, the one gate routing followed,
/// still confers authority under the filing-proof rule permission answers
/// also apply ([`FilingParent::proof_sql`]). A merely routed or adopted edge
/// confers nothing.
async fn filing_edge_proven_conn(
    conn: &TrackedConnection,
    issue_id: &str,
    arm: FilingParent,
) -> DbResult<bool> {
    // Routing reaches a parent thread only when the child has no parent issue,
    // so the thread edge speaks for the gate only on such a child.
    let routed = match arm {
        FilingParent::Issue => "",
        FilingParent::Thread => " AND i.parent_issue_id IS NULL",
    };
    let mut rows = conn
        .query(
            &format!(
                "SELECT 1 FROM issues i JOIN jobs j ON {proof}
                 WHERE i.id = ?1{routed} LIMIT 1",
                proof = arm.proof_sql()
            ),
            params![issue_id],
        )
        .await?;
    Ok(rows.next().await?.is_some())
}

async fn thread_uri_conn(conn: &TrackedConnection, thread_id: &str) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT p.key, t.name FROM threads t JOIN projects p ON p.id = t.project_id
             WHERE t.id = ?1",
            params![thread_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(cairn_uri::build_thread_uri(
            &row.text(0)?,
            &row.text(1)?,
        ))),
        None => Ok(None),
    }
}

/// Journal a refused confirmation. The gate is untouched, so the journal row
/// is the only trace of the attempt.
async fn journal_refusal(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    confirmer: &Confirmer,
    refusal: &ConfirmRefusal,
) -> Result<(), ConfirmError> {
    let now = chrono::Utc::now().timestamp();
    let home = cairn_db::lineage::home_uri_for_job(db, job_id)
        .await
        .ok()
        .flatten();
    let (run_id, attribution, carrier) = match confirmer {
        Confirmer::AgentRun(run_id) => {
            let attribution =
                crate::mcp::handlers::run_context::authenticated_agent_for_run(&orch.db, run_id)
                    .await
                    .ok()
                    .and_then(|agent| agent.appearance(now).ok());
            (Some(run_id.clone()), attribution, "agent run".to_string())
        }
        Confirmer::Unproven(carrier) => (None, None, (*carrier).to_string()),
        Confirmer::Operator(_) => return Ok(()),
    };
    let who = attribution
        .as_ref()
        .and_then(|(actor, _)| match actor {
            cairn_common::identity::PrincipalRef::Agent { node, .. } => Some(node.clone()),
            _ => None,
        })
        .unwrap_or_else(|| format!("an unattributed {carrier}"));
    let event = cairn_db::storage::authority::NewAuthorizationEvent {
        scope: AuthorityScope::new(
            AuthorityPlace::Resource {
                canonical_uri: home.clone().unwrap_or_else(|| "cairn://db".to_string()),
            },
            AuthorityAction::Write,
        ),
        mutation: "update".to_string(),
        summary: format!(
            "Refused confirming the gated artifact of job {job_id} by {who}: {}",
            refusal.reason
        ),
        outcome: "forbidden".to_string(),
        reason: "artifact_confirmation_authority".to_string(),
        principal: AuthorityPrincipal {
            actor: attribution.as_ref().map(|(actor, _)| actor.clone()),
            ..Default::default()
        },
        audience: AuthorityAudience::workspace(crate::authorization::WORKSPACE_ID),
        run_id,
        request_uri: Some(home.unwrap_or_else(|| job_id.to_string())),
        grant_id: None,
        request_acceptance: None,
        decision_actor: attribution.as_ref().map(|(actor, _)| actor.clone()),
        appearance_snapshot: attribution.map(|(_, appearance)| appearance),
        decided_at: now,
    };
    cairn_db::storage::authority::append_event(&orch.db.local, event)
        .await
        .map(|_| ())
        .map_err(|error| {
            ConfirmError::Failed(format!(
                "{refusal} The refusal could not be journaled: {error}"
            ))
        })
}

#[cfg(test)]
#[path = "checkpoints_filing_proof_tests.rs"]
mod filing_proof_tests;
