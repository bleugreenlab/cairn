//! The fleet's execution queue as a resource (`cairn://p/<project>/queue`), and
//! the two queue acts a thread asks the operator for: moving held work across
//! threads, and a held execution's `{start_now:true}` (cairn/5942).
//!
//! Neither builds grant plumbing of its own. Both go through the path a
//! capability-bearing settings write uses: [`queue_authority`] names the
//! authority request ahead of any side effect (`authorization::prepare`), the
//! write handler raises the operator's authority card when no grant covers
//! it, and the dispatch arm re-derives the same request and calls
//! `authorization::authorize_recorded`, which consumes a once grant, just
//! before the one implementation acts (`reorder::move_held_execution`,
//! `sweep::release` over every cap).
//!
//! Only a run acting for a thread may ask: the grant is bound to that thread
//! (`AuthorityConstraint::ActingThread`). The operator's own move and Start
//! now stay on the credential-proven invoke behind Settings → Queue.

use cairn_common::authorization::{
    AuthorityAction, AuthorityDecision, AuthorityLifetime, AuthorityMutation, AuthorityPlace,
    AuthorityRequest, AuthorityScope,
};
use cairn_common::uri::CairnResource;

use super::super::{build_failure, target_resource_for_request, ResourceMutationResult};
use crate::authorization::normalize;
use crate::authorization::prepare::Prepared;
use crate::execution::gates::reorder::{self, PlannedMove, QueueMover};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;

/// A change that is one of the two queue acts.
enum QueueAct {
    Move {
        project: String,
        payload: serde_json::Value,
    },
    StartNow {
        project: String,
        number: i32,
        exec_seq: i32,
    },
}

/// The thread asking for a queue act.
pub(crate) struct Asker {
    pub thread_id: String,
    pub thread_name: String,
}

/// Why no thread can ask.
enum NoAsker {
    /// No run, or a page: the write handler's own refusals apply, and the
    /// operator's path is Settings → Queue.
    Unauthenticated,
    /// A run that acts for no thread.
    NoThread,
    /// A project service's run, even one acting for a thread: unattended
    /// code, which the card ("alpha asks") does not describe.
    Service,
}

const NO_THREAD: &str = "Refused: queue acts across threads are a thread's to ask for, and \
     this run acts for no thread. Nothing changed.";

const SERVICE: &str = "Refused: a project service may not ask for queue acts, even acting for \
     a thread. The operator's card says a thread asked, and a grant to that thread does not \
     cover unattended code moving or starting work on a loop. Nothing changed.";

async fn queue_act(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    item: &ChangeItem,
) -> Option<QueueAct> {
    if item.mode != ChangeMode::Patch {
        return None;
    }
    let payload = item.payload.as_ref()?;
    let is_move = payload.get("move").is_some() || payload.get("order").is_some();
    let is_start = payload.get("start_now").is_some();
    if !is_move && !is_start {
        return None;
    }
    match target_resource_for_request(orch, request, item)
        .await
        .ok()?
    {
        CairnResource::ProjectQueue { project } if is_move => Some(QueueAct::Move {
            project,
            payload: payload.clone(),
        }),
        CairnResource::IssueExecution {
            project,
            number,
            exec_seq,
        } if is_start => Some(QueueAct::StartNow {
            project,
            number,
            exec_seq,
        }),
        _ => None,
    }
}

/// Why `request` cannot ask for a queue act as a thread, whichever thread it
/// would resolve to: no run (a page's row write included), or a project
/// service's run.
fn excluded(orch: &Orchestrator, request: &McpCallbackRequest) -> Option<NoAsker> {
    let no_run = request
        .run_id
        .as_deref()
        .filter(|run| !run.is_empty())
        .is_none();
    if no_run || request.row_write_origin().is_some() {
        return Some(NoAsker::Unauthenticated);
    }
    // Checked before the thread resolves: a delegated service run would
    // otherwise resolve to the thread it acts for and match that thread's
    // grant. Excluded on security's ruling (cairn/5942).
    if request
        .run_id
        .as_deref()
        .is_some_and(|run| orch.project_services.runs.get(run).is_some())
    {
        return Some(NoAsker::Service);
    }
    None
}

/// The thread a queue grant would bind for `request`, by id: what holds an
/// `ActingThread` grant when it is revoked (cairn/5998). The same exclusions
/// as [`asker`], then the run's own job's thread.
pub(crate) async fn asking_thread_id(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Option<String> {
    if excluded(orch, request).is_some() {
        return None;
    }
    super::threads::run_thread_id(orch, request).await
}

async fn asker(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
) -> Result<Asker, NoAsker> {
    if let Some(refusal) = excluded(orch, request) {
        return Err(refusal);
    }
    super::threads::acting_thread(orch, request, project)
        .await
        .map(|(thread_id, thread_name)| Asker {
            thread_id,
            thread_name,
        })
        .ok_or(NoAsker::NoThread)
}

/// The request an act with no asking thread names, so the write handler's
/// refusal for a page or a run-less caller can say what was asked. It binds no
/// thread, so no grant could ever cover it.
fn unattributed(action: AuthorityAction) -> AuthorityRequest {
    AuthorityRequest::new(
        AuthorityScope::new(
            AuthorityPlace::ExecutionQueue {
                workspace_id: crate::authorization::WORKSPACE_ID.to_string(),
            },
            action,
        ),
        AuthorityMutation::Update,
        match action {
            AuthorityAction::Run => "start held work now, past every cap",
            _ => "move held work across threads in the fleet's order",
        }
        .to_string(),
    )
}

/// The authority a queue act crosses, for `authorization::prepare`: `None`
/// unless the change is a project-queue move or an execution's `start_now`.
///
/// The refusals that no approval could change are checked here, so the
/// operator is never asked about a move that would be refused: a run acting
/// for no thread, a move that would displace a row the operator placed, an
/// execution that is not held.
pub(crate) async fn queue_authority(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    item: &ChangeItem,
) -> Option<Prepared> {
    let act = queue_act(orch, request, item).await?;
    let (project, action) = match &act {
        QueueAct::Move { project, .. } => (project.clone(), AuthorityAction::Write),
        QueueAct::StartNow { project, .. } => (project.clone(), AuthorityAction::Run),
    };
    let asker = match asker(orch, request, &project).await {
        Ok(asker) => asker,
        Err(NoAsker::Unauthenticated) => return Some(Prepared::Authority(unattributed(action))),
        Err(NoAsker::NoThread) => return Some(Prepared::Refused(NO_THREAD.to_string())),
        Err(NoAsker::Service) => return Some(Prepared::Refused(SERVICE.to_string())),
    };
    Some(match act {
        QueueAct::Move { project, payload } => {
            match move_request(orch, &project, &payload, &asker).await {
                Ok((authority, _, _)) => Prepared::Authority(authority),
                Err(MoveRefusal::Invalid(error)) => Prepared::Invalid(error),
                Err(MoveRefusal::Refused(error)) => Prepared::Refused(format!("Refused: {error}")),
            }
        }
        QueueAct::StartNow {
            project,
            number,
            exec_seq,
        } => match start_now_request(orch, &project, number, exec_seq, &asker).await {
            Ok((authority, _)) => Prepared::Authority(authority),
            Err(error) => Prepared::Refused(format!("Refused: {error}")),
        },
    })
}

enum MoveRefusal {
    /// The payload names nothing resolvable.
    Invalid(String),
    /// It resolves, and the queue refuses it.
    Refused(String),
}

/// The moves a project-queue patch asks for, as `(execution_id, position)`:
/// `move` is one entry to one place, `order` puts its entries at places 1…n.
/// An issue number resolves to that issue's cap-held execution in `project`.
async fn resolve_moves(
    orch: &Orchestrator,
    project: &str,
    payload: &serde_json::Value,
) -> Result<Vec<(String, usize)>, MoveRefusal> {
    let entries: Vec<(&serde_json::Value, usize)> =
        match (payload.get("move"), payload.get("order")) {
            (Some(_), Some(_)) | (None, None) => {
                return Err(MoveRefusal::Invalid(
                    "send exactly one of payload.move ({issue:N, position:K}) and payload.order \
                 ([N, …]). Nothing changed."
                        .to_string(),
                ))
            }
            (Some(entry), None) => {
                let position = entry
                    .get("position")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| {
                        MoveRefusal::Invalid(
                        "payload.move.position is the 1-based place in the fleet's order to move \
                         it to. Nothing changed."
                            .to_string(),
                    )
                    })?;
                vec![(entry, position as usize)]
            }
            (None, Some(order)) => {
                let order = order
                    .as_array()
                    .filter(|order| !order.is_empty())
                    .ok_or_else(|| {
                        MoveRefusal::Invalid(
                    "payload.order lists issue numbers (or {execution:ID}) to take fleet places \
                     1…n in that order. Nothing changed."
                        .to_string(),
                )
                    })?;
                order
                    .iter()
                    .enumerate()
                    .map(|(index, entry)| (entry, index + 1))
                    .collect()
            }
        };

    let line = crate::execution::gates::capacity::fleet_order(orch)
        .await
        .map_err(MoveRefusal::Invalid)?;
    let key = cairn_common::uri::canonical_project(project);
    let mut moves = Vec::new();
    let mut refusals = Vec::new();
    for (entry, position) in entries {
        let number = entry
            .as_i64()
            .or_else(|| entry.get("issue").and_then(serde_json::Value::as_i64));
        let execution = entry.get("execution").and_then(serde_json::Value::as_str);
        match (number, execution) {
            (Some(number), None) => {
                let found = line.iter().find(|held| {
                    held.queued.issue_number == Some(number)
                        && held
                            .queued
                            .project_key
                            .as_deref()
                            .map(cairn_common::uri::canonical_project)
                            .as_deref()
                            == Some(key.as_str())
                });
                match found {
                    Some(held) => moves.push((held.queued.execution_id.clone(), position)),
                    None => refusals.push(format!(
                        "{key}/{number} has no execution the queue's caps hold"
                    )),
                }
            }
            (None, Some(execution)) => moves.push((execution.to_string(), position)),
            _ => refusals.push(format!(
                "{entry} is neither an issue number nor {{execution:ID}}"
            )),
        }
    }
    if !refusals.is_empty() {
        return Err(MoveRefusal::Refused(format!(
            "{}. Nothing changed.",
            refusals.join("; ")
        )));
    }
    Ok(moves)
}

/// The authority a thread's move crosses, its resolved moves, and its plan,
/// against the queue as it reads now. Prepare and dispatch both derive it
/// here, so they name the same request.
async fn move_request(
    orch: &Orchestrator,
    project: &str,
    payload: &serde_json::Value,
    asker: &Asker,
) -> Result<(AuthorityRequest, Vec<(String, usize)>, Vec<PlannedMove>), MoveRefusal> {
    let moves = resolve_moves(orch, project, payload).await?;
    let mover = QueueMover::Thread {
        name: asker.thread_name.clone(),
    };
    let planned = reorder::plan_moves(orch, &moves, &mover)
        .await
        .map_err(MoveRefusal::Refused)?;
    Ok((
        normalize::queue_move_request(
            crate::authorization::WORKSPACE_ID,
            &asker.thread_id,
            &asker.thread_name,
            &moves,
            &planned,
        ),
        moves,
        planned,
    ))
}

/// The capacity holds blocking an execution right now, from its gate rows
/// alone (`execution_gates`, never the execution's own row).
async fn blocking_holds(
    db: &crate::storage::LocalDb,
    execution_id: &str,
) -> Result<Vec<crate::models::CapacityHold>, String> {
    let id = execution_id.to_string();
    let gates = db
        .read(move |conn| {
            let id = id.clone();
            Box::pin(async move { cairn_workflow::gates::store::list_gates_conn(conn, &id).await })
        })
        .await
        .map_err(|error| format!("failed to read the execution's gates: {error}"))?;
    Ok(gates
        .iter()
        .filter(|gate| !gate.is_satisfied() && gate.last_outcome.as_deref() == Some("no_evidence"))
        .filter_map(|gate| {
            let fact = crate::execution::gates::capacity::capacity_fact(&gate.predicate_json)?;
            crate::execution::gates::capacity::decode_hold(fact, gate.detail.as_deref()?)
        })
        .collect())
}

/// The authority a thread's `start_now` crosses, and the execution it names,
/// which must be held. Prepare and dispatch both derive it here.
async fn start_now_request(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    asker: &Asker,
) -> Result<(AuthorityRequest, String), String> {
    let db = orch.db.for_project(project).await;
    let execution_id =
        crate::mcp::handlers::executions::resolve_execution_id(&db, project, number, exec_seq)
            .await?;
    let issue =
        cairn_common::uri::build_issue_uri(&cairn_common::uri::canonical_project(project), number);
    let label = format!("execution {exec_seq} on {issue}");
    let state = crate::execution::gates::authoring::hold_state(&db, &execution_id).await?;
    crate::execution::gates::authoring::require_held(&state, &label)?;
    // A thread starts only its own held work: the caps constrain what a thread
    // starts itself (tenet 1319), and putting another thread's work ahead is
    // the move's boundary, not this one. The card says "its held work".
    let owner = db
        .query_opt_text(
            "SELECT i.parent_thread_id FROM issues i JOIN projects p ON p.id = i.project_id
              WHERE p.key = ?1 AND i.number = ?2",
            (cairn_common::uri::canonical_project(project), number),
        )
        .await
        .map_err(|error| error.to_string())?;
    if owner.as_deref() != Some(asker.thread_id.as_str()) {
        return Err(format!(
            "{label} is not {}'s work, and a thread starts only its own held work now. \
             Nothing started.",
            asker.thread_name
        ));
    }
    let holds = blocking_holds(&db, &execution_id).await?;
    let title = super::threads::issue_title(orch, project, number).await;
    let what = match title {
        Some(title) => format!("{label} ('{title}')"),
        None => label,
    };
    Ok((
        normalize::start_now_request(
            crate::authorization::WORKSPACE_ID,
            &asker.thread_id,
            &asker.thread_name,
            &execution_id,
            &what,
            &holds,
        ),
        execution_id,
    ))
}

/// Who started a held execution past every cap, under which decision: what a
/// granted Start now records on its override.
pub(crate) struct GrantedStart {
    pub thread: String,
    pub decision: Option<String>,
}

/// The final check of a thread's `{start_now:true}`, immediately before the
/// release: re-derives the request the card named and consumes a once grant.
/// An approval revoked or spent since the card was answered is refused here.
pub(crate) async fn authorize_start_now(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    number: i32,
    exec_seq: i32,
) -> Result<GrantedStart, String> {
    let asker = match asker(orch, request, project).await {
        Ok(asker) => asker,
        Err(NoAsker::Unauthenticated) => {
            return Err(
                "Denied: starting held work past every cap is the operator's to approve, \
                 and this request comes from no run acting for a thread. The operator starts \
                 held work from Settings → Queue."
                    .to_string(),
            )
        }
        Err(NoAsker::NoThread) => return Err(NO_THREAD.to_string()),
        Err(NoAsker::Service) => return Err(SERVICE.to_string()),
    };
    let (authority, _) = start_now_request(orch, project, number, exec_seq, &asker).await?;
    let decision = authorize(orch, request, &authority).await?;
    Ok(GrantedStart {
        thread: asker.thread_name,
        decision: decision.event_id,
    })
}

/// An allowed decision: its journal row, and whether it spent a once grant.
struct Allowed {
    event_id: Option<String>,
    spent_once: bool,
}

async fn authorize(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    authority: &AuthorityRequest,
) -> Result<Allowed, String> {
    let Some(actor) = crate::authorization::resolve_actor(orch, request).await else {
        return Err(format!(
            "Denied: only an authenticated run can be authorized to {}.",
            authority.summary
        ));
    };
    let (decision, event_id) = crate::authorization::authorize_recorded(&actor, authority).await?;
    match decision {
        AuthorityDecision::Direct => Ok(Allowed {
            event_id,
            spent_once: false,
        }),
        AuthorityDecision::AllowedByGrant { grant_id, .. } => {
            let spent_once = crate::authorization::get_grant(&actor.db, &grant_id)
                .await
                .ok()
                .flatten()
                .is_some_and(|grant| matches!(grant.lifetime, AuthorityLifetime::Once { .. }));
            Ok(Allowed {
                event_id,
                spent_once,
            })
        }
        // The race the card cannot see: the grant was revoked, or another
        // write spent it, between the operator's answer and this write.
        AuthorityDecision::ApprovalRequired(reason) | AuthorityDecision::Forbidden(reason) => {
            Err(crate::authorization::refusal_message(authority, reason))
        }
    }
}

/// `cairn/5901 to fleet 1st (was 14th)`.
fn landed(planned: &PlannedMove, position: usize) -> String {
    use crate::execution::gates::census::ordinal;
    if planned.from == position {
        return format!("{} stays at fleet {}", planned.label, ordinal(position));
    }
    format!(
        "{} to fleet {} (was {})",
        planned.label,
        ordinal(position),
        ordinal(planned.from)
    )
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let (CairnResource::ProjectQueue { project }, ChangeMode::Patch) = (resource, item.mode) else {
        return Ok(None);
    };
    let fail = |error: String| build_failure(index, item, error);
    let payload = item.payload.as_ref().ok_or_else(|| {
        fail(
            "mode=patch requires payload: {move:{issue:N, position:K}} or {order:[N, …]}"
                .to_string(),
        )
    })?;
    let asker =
        match asker(orch, request, project).await {
            Ok(asker) => asker,
            Err(NoAsker::Unauthenticated) => return Err(fail(
                "Denied: moving held work across threads is the operator's to approve, and this \
                 request comes from no run acting for a thread. The operator moves held work \
                 from Settings → Queue."
                    .to_string(),
            )),
            Err(NoAsker::NoThread) => return Err(fail(NO_THREAD.to_string())),
            Err(NoAsker::Service) => return Err(fail(SERVICE.to_string())),
        };
    let (authority, moves, planned) =
        move_request(orch, project, payload, &asker)
            .await
            .map_err(|refusal| match refusal {
                MoveRefusal::Invalid(error) | MoveRefusal::Refused(error) => fail(error),
            })?;
    if dry_run {
        return Ok(Some(format!("Would {}", authority.summary)));
    }
    // Consumed before the admission lock and outside it, so a once grant is
    // never spent while holding the lock every filing and sweep waits on.
    let allowed = authorize(orch, request, &authority).await.map_err(fail)?;
    let mover = QueueMover::Thread {
        name: asker.thread_name.clone(),
    };
    let outcomes = match reorder::move_held_execution(orch, &moves, mover).await {
        Ok(outcomes) => outcomes,
        Err(error) if allowed.spent_once => {
            return Err(fail(format!(
                "{error} The operator's single-use approval for this move was spent on it; ask \
                 again."
            )))
        }
        Err(error) => return Err(fail(error)),
    };
    let lines: Vec<String> = planned
        .iter()
        .zip(&outcomes)
        .map(|(planned, outcome)| landed(planned, outcome.position))
        .collect();
    Ok(Some(format!(
        "Moved {} in the fleet's order, placed by {}: its thread's own order now leaves {}.",
        lines.join("; "),
        asker.thread_name,
        if lines.len() == 1 { "it" } else { "them" }
    )))
}
