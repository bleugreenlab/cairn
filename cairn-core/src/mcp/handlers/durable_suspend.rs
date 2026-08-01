//! The durable suspend/resume core.
//!
//! Suspending a turn is one mechanism with two clients: an explicit `waitFor`
//! condition ([`super::owned_wait`]) and a `run` batch that outlived its grace
//! window ([`super::run`]). Both persist an `agent_waits` row, park the warm
//! process, and later deliver exactly one synthetic tool result plus exactly one
//! continuation. Only the *trigger* differs, so only the trigger lives with its
//! client; everything here -- the row, the first-writer-wins claim, the
//! successor-collision handling, and startup reconciliation -- is shared.
//!
//! Order matters and is load-bearing: persist the row and yield the turn BEFORE
//! arming anything that can resolve. Arming first let an already-satisfied
//! condition resume the agent, which the subsequent park then re-suspended --
//! the "never resumed" race (CAIRN-2970).

use super::permission::{
    emit_successor_turn_events, ensure_wait_resolved_successor, yield_turn_for_host, WaitSuccessor,
};
use super::run::TerminalWaitEvent;
use crate::execution::jobs::{continue_job_impl, ResumeContext};
use crate::models::{TurnState, TurnYieldReason};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;
use std::{sync::Arc, time::Duration};

// In-process poll for a racing continuation to START the predecessor's successor
// it just created. There is deliberately NO fixed cutoff: a healthy continuation
// can legitimately take a while before `start_turn` (cold process spawn, DB
// contention), so the resolver rechecks with exponential backoff (capped) for as
// long as the host lives. Shutdown drops this task, handing ownership to startup
// reconciliation.
const COLLISION_POLL_INITIAL: Duration = Duration::from_millis(50);
const COLLISION_POLL_MAX: Duration = Duration::from_secs(2);

/// What a suspension is waiting for. Serialized into `agent_waits.condition_json`;
/// the tag keeps the encoding forward-compatible, so adding a variant never
/// invalidates rows written by an earlier host.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Condition {
    Duration,
    Terminal {
        uri: String,
        slug: String,
        on: TerminalWaitEvent,
        phrase: Option<String>,
    },
    /// A `run` batch that outlived its grace window. Unlike the wait conditions
    /// there is nothing to poll: the trigger is the awaited executor result,
    /// held in memory by the host that suspended. A restart therefore cannot
    /// re-drive it, and reconciliation resolves it to a typed failure instead
    /// of stranding the agent (see [`reconcile`]).
    McpContinuation {
        state: super::mcp_continuation::McpContinuationState,
    },
    RunBatch {
        /// The request the batch is placed under. Attempts of it are not named
        /// here on purpose: a batch waiting for room is presented more than
        /// once, so an attempt id would name whichever one happened to be in
        /// flight when the call parked, and the request id is what cancellation
        /// and reconciliation both address.
        request_id: String,
        /// Whether the batch carried a `commit_msg`, so a host-restart failure
        /// can be honest about what could have landed.
        commits: bool,
        /// A short name for the batch, so its result can be told from a
        /// sibling's when one turn resumes with several parked calls at once.
        /// Optional with a serde default because rows written before this
        /// existed carry no label and must stay readable.
        #[serde(default)]
        label: Option<String>,
    },
}

impl Condition {
    /// A short, human-readable name for the call this suspension parked.
    ///
    /// It exists to distinguish one result from another when a turn resumes
    /// with several, so it names what the agent asked for rather than what the
    /// resolver polls: the question it answers is "which of the calls I issued
    /// is this?".
    fn label(&self) -> String {
        match self {
            Condition::Duration => "waitFor: duration".to_string(),
            Condition::Terminal { slug, on, .. } => {
                let on = match on {
                    TerminalWaitEvent::Exit => "exit",
                    TerminalWaitEvent::Output => "output",
                };
                format!("waitFor: terminal {slug} {on}")
            }
            Condition::McpContinuation { state } => format!("MCP: {}/{}", state.server, state.tool),
            Condition::RunBatch {
                label: Some(label), ..
            } => format!("run: {label}"),
            Condition::RunBatch { label: None, .. } => "run batch".to_string(),
        }
    }
}

/// One suspended call: the parked turn, the tool call awaiting a result, and
/// what it is waiting for.
#[derive(Clone)]
pub(crate) struct Record {
    pub(crate) id: String,
    pub(crate) job_id: String,
    pub(crate) run_id: String,
    pub(crate) session_id: String,
    pub(crate) turn_id: String,
    pub(crate) tool_use_id: String,
    pub(crate) condition: Condition,
    pub(crate) deadline: Option<i64>,
    pub(crate) created: i64,
}

/// The turn a callback that is about to suspend belongs to.
///
/// Normally this is the process's own live turn. But a turn that has already
/// parked one of its calls reports no live turn at all: parking transitions the
/// process to idle, which is what makes a long wait's warm process reclaimable.
/// A sibling call still crossing its own grace window would then find nothing to
/// bind to and fall back to awaiting inline — the exact loss concurrent
/// suspension exists to prevent, reintroduced by a few hundred milliseconds of
/// scheduling jitter between two calls that crossed grace together.
///
/// So a parked run answers with the turn its own active suspension belongs to. A
/// job runs one turn at a time, so an active row names the turn that is parked,
/// and a callback arriving while it is parked came from that turn.
///
/// Guessing wrong is harmless by construction: correlation is scoped to that
/// turn's transcript, so a callback that did not come from it finds no call of
/// its own to claim and falls back to awaiting inline exactly as it does today.
/// Nothing here can answer a call that belongs to someone else.
pub(crate) async fn suspending_turn_id(
    orch: &Orchestrator,
    db: &LocalDb,
    run_id: &str,
) -> Option<String> {
    if let Some(turn_id) = orch.process_state.get_current_turn_id(run_id) {
        return Some(turn_id);
    }
    let run_id = run_id.to_string();
    db.read(|c| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = c
                .query(
                    "SELECT predecessor_turn_id FROM agent_waits WHERE run_id=?1 AND state IN ('pending','resolving') ORDER BY created_at DESC LIMIT 1",
                    params![run_id],
                )
                .await?;
            rows.next().await?.map(|row| row.text(0)).transpose()
        })
    })
    .await
    .ok()
    .flatten()
}

/// The run's session id, which a suspension needs to bind its synthetic result.
pub(crate) async fn run_session(db: &LocalDb, id: &str) -> Result<Option<String>, String> {
    let id = id.to_string();
    db.read(|c| {
        let id = id.clone();
        Box::pin(async move {
            let mut r = c
                .query("SELECT session_id FROM runs WHERE id=?1", params![id])
                .await?;
            r.next()
                .await?
                .map(|x| x.opt_text(0))
                .transpose()
                .map(Option::flatten)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// The typed outcome that marks a suspension abandoned, in the transcript and
/// in the row's own stored resolution. Startup reads it to tell a decision
/// already made apart from a wait still to be driven.
const ABANDONED_OUTCOME: &str = "abandoned";

/// What an abandoned suspension's own call is told. A wait row can outlive the
/// turn that created it, and that turn's tool use would otherwise dangle in the
/// transcript with no result at all.
fn abandoned_result() -> String {
    serde_json::json!({
        "outcome":ABANDONED_OUTCOME,
        "error":"This call's turn was replaced before its wait finished, so the wait was abandoned. Reissue it if the work still matters."
    })
    .to_string()
}

/// What any failure to establish suspension is told. The underlying error
/// is storage detail an agent cannot act on, so it goes to the log.
const SUSPENSION_UNAVAILABLE: &str =
    "This call could not be suspended, so nothing is waiting on it now. Reissue it if the work still matters.";

/// A suspension's deferred park, handed to the caller that must wait for it.
///
/// The park is what interrupts the agent's turn, and it is deliberately not
/// immediate: the suspension's own tool result has to reach the agent first, or
/// the CLI attributes the cancelled call to the user (see
/// [`crate::orchestrator::lifecycle::SUSPEND_HANDOFF_GRACE`]). Resolution is
/// therefore gated on this instead of starting the moment [`suspend`] returns.
pub(crate) struct ParkHandoff(tokio::sync::oneshot::Receiver<()>);

impl ParkHandoff {
    /// Wait for the predecessor turn to be parked.
    ///
    /// `false` means the park failed, so the predecessor is still live: the
    /// caller must NOT resolve, and leaves its pending row for startup
    /// reconciliation rather than resuming a run that was never suspended.
    pub(crate) async fn parked(self) -> bool {
        self.0.await.is_ok()
    }
}

/// Establish durable suspension: persist the row, yield the turn, and schedule
/// the park.
///
/// Two phases, and the split is load-bearing in both directions. The row and the
/// yielded turn land synchronously, before this returns, so nothing can resolve
/// ahead of them (CAIRN-2970). The park is deferred by the handoff grace so the
/// suspension's own tool result reaches the agent ahead of the interrupt
/// (CAIRN-3162). Callers arm their resolver on the returned [`ParkHandoff`],
/// which restores the original order: row, yield, park, then resolve.
pub(crate) async fn suspend(
    orch: &Orchestrator,
    db: &LocalDb,
    record: &Record,
) -> Result<ParkHandoff, String> {
    // A suspension exists to answer one specific provider call, so it cannot be
    // established without one. Every caller binds an id before it gets here;
    // refusing rather than inserting is what keeps a blank id out of the table,
    // where the per-call uniqueness bound would silently stop bounding anything.
    if record.tool_use_id.trim().is_empty() {
        log::warn!(
            "refusing to suspend run {} on no bound tool call",
            record.run_id
        );
        return Err(SUSPENSION_UNAVAILABLE.to_string());
    }
    supersede_abandoned(orch, db, record).await?;
    let yielded = insert(db, record).await?;
    // Say out loud that the turn parked. The row and the yield land inside one
    // write, which announces nothing on its own, and every reader of turn state
    // caches until something invalidates it -- so without this the node keeps
    // reading as it did before the park for as long as the park lasts. The
    // transcript is the sharpest case: a parked call's row has no verdict to
    // draw and no turn it can call running, which is how a live batch came to
    // render as neither (CAIRN-3340). The prompt and permission suspensions
    // announce their own yields the same way (`planning`, `permission`).
    if yielded {
        let change = crate::notify::turn_db_change_for_id(db, &record.turn_id, "update").await;
        let _ = orch.services.emitter.emit("db-change", change);
    }
    orch.process_state
        .yield_for_host(&record.run_id, &record.turn_id);
    let (parked_tx, parked_rx) = tokio::sync::oneshot::channel();
    crate::orchestrator::lifecycle::suspend_run_for_durable_wait_after_handoff_then(
        orch,
        &record.run_id,
        "durable_suspend",
        move |parked| async move {
            if parked.is_ok() {
                let _ = parked_tx.send(());
            }
        },
    );
    Ok(ParkHandoff(parked_rx))
}

/// Persist the suspension and yield its turn in one write, reporting whether the
/// turn was the one that yielded (it is already parked when a sibling call of the
/// same turn got there first).
async fn insert(db: &LocalDb, r: &Record) -> Result<bool, String> {
    let run_id = r.run_id.clone();
    let r = r.clone();
    db.write(|c|{let r=r.clone();Box::pin(async move{let json=serde_json::to_string(&r.condition).map_err(|e|DbError::internal(e.to_string()))?;c.execute("INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,deadline_ms,state,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'pending',?9)",params![r.id.as_str(),r.job_id.as_str(),r.run_id.as_str(),r.session_id.as_str(),r.turn_id.as_str(),r.tool_use_id.as_str(),json,r.deadline,r.created]).await?;let yielded=yield_turn_for_host(c,&r.turn_id,TurnYieldReason::Wait).await.map_err(|e|DbError::internal(e.to_string()))?;Ok(yielded)})}).await.map_err(|error|{log::warn!("durable suspension insert failed for run {run_id}: {error}");SUSPENSION_UNAVAILABLE.to_string()})
}

/// What answering one suspension's call needs: which row, and which tool call in
/// which turn it belongs to.
struct SuspendedCall {
    id: String,
    run_id: String,
    session_id: String,
    turn_id: String,
    tool_use_id: String,
}

impl SuspendedCall {
    fn of(r: &Record) -> Self {
        Self {
            id: r.id.clone(),
            run_id: r.run_id.clone(),
            session_id: r.session_id.clone(),
            turn_id: r.turn_id.clone(),
            tool_use_id: r.tool_use_id.clone(),
        }
    }
}

/// Clear the way for a new suspension.
///
/// A job parks exactly one turn at a time, and a partial unique index on
/// `(job_id, tool_use_id)` bounds that: at most one active `agent_waits` row per
/// CALL. A row that outlives the turn which created it therefore refuses that
/// call's next suspension for the rest of the job's life -- permanently
/// degrading it to inline waiting -- unless it is superseded here (CAIRN-3159).
///
/// Rows do outlive their turn. A session reconstruction (an inactivity reseed, a
/// backend rotation) moves the job onto a fresh session, run, and turn while the
/// previous turn's row is still pending, and nothing ever claims it: startup
/// reconciliation only runs when the host restarts, and this reconstruction
/// happened mid-life. Such a row is disposable state, not a permanent fact --
/// the same principle restore-on-entry applies to a worktree.
///
/// The incoming suspension is the evidence, and what it proves is about the
/// OTHER turns. It comes from a turn that is running right now, and a job runs
/// one turn at a time, so an active row belonging to any other turn was
/// abandoned and is superseded. An active row belonging to the SAME turn proves
/// nothing of the kind: it is a sibling call of the very turn now suspending,
/// parked alongside this one, and is left exactly where it is (CAIRN-3232).
async fn supersede_abandoned(orch: &Orchestrator, db: &LocalDb, r: &Record) -> Result<(), String> {
    for wait in active_waits_for_job(db, &r.job_id).await? {
        if wait.turn_id == r.turn_id {
            continue;
        }
        log::warn!(
            "superseding abandoned suspension {} (turn {}) so job {} can suspend turn {}",
            wait.id,
            wait.turn_id,
            r.job_id,
            r.turn_id
        );
        abandon(orch, db, &wait).await?;
    }
    Ok(())
}

/// Abandon one suspension: record the decision, answer its call, close the row.
///
/// The order is the crash contract, and each step covers a different way a host
/// can die mid-abandonment.
///
/// The typed resolution is written FIRST, before anything observable, because it
/// is what startup reads to tell an abandonment apart from a live wait. Without
/// it, a restart would re-drive the row as an ordinary suspension, create a
/// `WaitResolved` successor for a turn the job has already left, and resume an
/// obsolete run — resurrecting the very suspension being discarded.
///
/// Closing the row comes LAST, because a row still in the re-driven set is a row
/// reconciliation can finish. Closing first would hide an unanswered call inside
/// a terminal state where nothing would ever look for it again.
async fn abandon(orch: &Orchestrator, db: &LocalDb, call: &SuspendedCall) -> Result<(), String> {
    if !mark_abandoned(db, &call.id).await? {
        // Its own resolver owns the resume, so this must not answer or cancel it.
        // But a row whose resolver already did everything a resolution does --
        // claimed its successor, delivered its result -- is finished in fact and
        // missing only its final state write, and leaving it active is what makes
        // waiting single-use for the rest of the job's life. Complete it instead.
        // A resolver genuinely mid-flight is left alone; this suspension is then
        // refused rather than raced, and the retry succeeds.
        return complete_resolution_in_fact(db, &call.id).await;
    }
    finish_abandonment(orch, db, call).await
}

/// Everything after the decision is recorded, and therefore also what startup
/// runs to finish an abandonment a restart interrupted. It never creates a
/// successor and never resumes the run.
async fn finish_abandonment(
    orch: &Orchestrator,
    db: &LocalDb,
    call: &SuspendedCall,
) -> Result<(), String> {
    deliver_abandonment(orch, db, call).await;
    cancel_abandoned(db, &call.id).await
}

/// Write the terminal state a resolution earned but never recorded.
///
/// A resolution that claimed its successor and delivered its result has done
/// everything a resolution does; if it then returned before its final
/// `resolving -> resolved` write — a failed continuation, a dropped task — the row
/// stays in the active set forever, and the partial unique index turns that into
/// "the first wait works and every later one fails" for the rest of the job's
/// life. The evidence for finishing it is the row's own columns, so this claims
/// nothing and races nothing: a resolver still mid-flight has not set them yet
/// and is left alone.
async fn complete_resolution_in_fact(db: &LocalDb, id: &str) -> Result<(), String> {
    let id = id.to_string();
    let now = chrono::Utc::now().timestamp_millis();
    db.write(|c| {
        let id = id.clone();
        Box::pin(async move {
            let completed = c
                .execute(
                    "UPDATE agent_waits SET state='resolved',resolved_at=COALESCE(resolved_at,?2) WHERE id=?1 AND state='resolving' AND successor_turn_id IS NOT NULL AND result_stored_at IS NOT NULL",
                    params![id.clone(), now],
                )
                .await?;
            if completed > 0 {
                log::warn!("suspension {id} had resolved in fact but never recorded it; completed");
            }
            Ok(())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Record the decision to abandon a suspension, reporting whether abandonment
/// won the race against the row's own resolver.
///
/// `resolving` keeps the row in the set startup re-drives, and the typed
/// resolution is the durable discriminator that sends it to
/// [`finish_abandonment`] instead of a resume. Refusing a row whose continuation
/// is already owned is the other half of [`claim_continuation`]'s exclusion:
/// between abandoning this suspension and resuming it, exactly one write wins.
async fn mark_abandoned(db: &LocalDb, id: &str) -> Result<bool, String> {
    let id = id.to_string();
    db.write(|c| {
        let (id, result) = (id.clone(), abandoned_result());
        Box::pin(async move {
            let marked = c
                .execute(
                    "UPDATE agent_waits SET state='resolving',resolution_json=?2 WHERE id=?1 AND state IN ('pending','resolving') AND successor_turn_id IS NULL",
                    params![id, result],
                )
                .await?;
            Ok(marked > 0)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Answer an abandoned call, at most once, by winning the same claim the row's
/// own resolver would take. Best effort past the claim: the abandoned turn may
/// already be gone, in which case there is nothing to answer.
async fn deliver_abandonment(orch: &Orchestrator, db: &LocalDb, call: &SuspendedCall) {
    match claim_result_delivery(db, &call.id).await {
        Ok(true) => {}
        Ok(false) => return,
        Err(error) => {
            log::warn!(
                "could not claim delivery for suspension {}: {error}",
                call.id
            );
            return;
        }
    }
    if let Err(error) = crate::execution::jobs::store_tool_result_event_with_turn(
        orch,
        &call.run_id,
        &call.session_id,
        &call.tool_use_id,
        &abandoned_result(),
        true,
        chrono::Utc::now().timestamp() as i32,
        Some(&call.turn_id),
    ) {
        log::warn!(
            "abandoned suspension {} left no result on its own turn: {error}",
            call.id
        );
        release_result_delivery(db, &call.id).await;
    }
}

/// Whether this suspension has visibly been overtaken — abandoned by a
/// reconstructed turn, or cancelled by a user Stop — since the resolver claimed
/// it.
///
/// This is an early out, deliberately not the fence. A read can only report the
/// past, so acting on it is a race no matter how close it sits to the action;
/// [`claim_continuation`] is what actually decides who resumes. What this buys
/// is not correctness but tidiness: an abandoned suspension stops before
/// building a successor turn nobody will ever start.
///
/// A row that has vanished is overtaken. A row that cannot be read is not: a
/// transient read error should not strand a healthy wait forever.
async fn overtaken(db: &LocalDb, id: &str) -> bool {
    match load_resolution(db, id).await {
        Ok(Some((state, resolution, _))) => {
            let overtaken = state == "cancelled" || is_abandonment(resolution.as_deref());
            if overtaken {
                log::warn!("suspension {id} was overtaken while resolving; standing down");
            }
            overtaken
        }
        Ok(None) => true,
        Err(error) => {
            log::warn!("could not re-read the decision for suspension {id}: {error}");
            false
        }
    }
}

/// Whether a stored resolution is an abandonment. This is the durable record of
/// a decision already made, so startup finishes it rather than reopening it.
fn is_abandonment(resolution: Option<&str>) -> bool {
    resolution
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
        .and_then(|value| {
            value
                .get("outcome")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .as_deref()
        == Some(ABANDONED_OUTCOME)
}

async fn active_waits_for_job(db: &LocalDb, job_id: &str) -> Result<Vec<SuspendedCall>, String> {
    let job_id = job_id.to_string();
    db.read(|c| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = c.query(
                "SELECT id,run_id,session_id,predecessor_turn_id,tool_use_id FROM agent_waits WHERE job_id=?1 AND state IN ('pending','resolving')",
                params![job_id],
            ).await?;
            let mut found = Vec::new();
            while let Some(row) = rows.next().await? {
                found.push(SuspendedCall {
                    id: row.text(0)?,
                    run_id: row.text(1)?,
                    session_id: row.text(2)?,
                    turn_id: row.text(3)?,
                    tool_use_id: row.text(4)?,
                });
            }
            Ok(found)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Take an abandoned row out of the pending set with a typed outcome, so the
/// job can suspend again. Delivering that outcome is a separate step behind the
/// shared delivery claim, so cancelling never races another writer's answer.
async fn cancel_abandoned(db: &LocalDb, id: &str) -> Result<(), String> {
    let id = id.to_string();
    let now = chrono::Utc::now().timestamp_millis();
    db.write(|c| {
        let (id, result) = (id.clone(), abandoned_result());
        Box::pin(async move {
            c.execute(
                "UPDATE agent_waits SET state='cancelled',resolution_json=COALESCE(resolution_json,?2),resolved_at=COALESCE(resolved_at,?3) WHERE id=?1 AND state IN ('pending','resolving')",
                params![id, result, now],
            ).await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Answer a suspended call, and — if it is the last of its turn's parked calls
/// to settle — resume the turn exactly once.
///
/// A turn may park several of its calls at once, so this splits into per-call
/// work and per-turn work. Answering the call on its own provider id is
/// per-call, and every resolver does it. Building the successor turn, claiming
/// the continuation and driving the resume are per-turn, and belong to whichever
/// call settles last. "Last one out drives" is not a policy choice: a turn
/// cannot resume while one of its own calls is still unanswered, so it is the
/// only correct rule.
pub(crate) async fn resolve(
    orch: &Orchestrator,
    db: &LocalDb,
    r: &Record,
    result: &str,
    replay: bool,
) -> Result<(), String> {
    // First-writer-wins claim: pending -> resolving. The row count distinguishes
    // the live resolver that won the transition from a duplicate that lost it; a
    // startup replay of an existing `resolving` row re-drives idempotently. A crash
    // from here on leaves a `resolving` row that replay re-drives idempotently
    // instead of losing the resume behind a `resolved` row.
    let (id, out) = (r.id.clone(), result.to_string());
    let claimed = db
        .write(|c| {
            let (id, out) = (id.clone(), out.clone());
            Box::pin(async move {
                let changed = c
                    .execute(
                        "UPDATE agent_waits SET state='resolving',resolution_json=?2 WHERE id=?1 AND state='pending'",
                        params![id, out],
                    )
                    .await?;
                Ok(changed)
            })
        })
        .await
        .map_err(|e| e.to_string())?;

    let Some((state, stored_result, stored_successor)) = load_resolution(db, &r.id).await? else {
        return Err(format!("suspension disappeared: {}", r.id));
    };
    match state.as_str() {
        // User Stop cancelled the suspension, or a prior resolution finished it.
        "cancelled" | "resolved" => return Ok(()),
        "resolving" => {}
        other => return Err(format!("suspension has invalid resolution state: {other}")),
    }
    // A live resolver that did not win the pending -> resolving transition defers to
    // the winner. Only a startup replay legitimately re-drives a `resolving` row.
    if !replay && claimed == 0 {
        return Ok(());
    }
    let result = stored_result.as_deref().unwrap_or(result);

    // Per-call work, done by every resolver of this turn: answer this call on
    // its own tool use id, exactly once.
    //
    // It happens BEFORE the election because a resolver that stands down never
    // reaches the per-turn work below, and its call is owed an answer either
    // way. Delivering ahead of `claim_continuation` also settles one race
    // deliberately: a resolver later overtaken by abandonment has already
    // written its REAL result, so the abandonment text loses the delivery claim
    // for that call. A real answer beats "this call's turn was replaced".
    store_result_once(orch, db, r, result).await?;

    // The election. A sibling call of this same turn still parked means this
    // resolver is not the last one out, and it is finished here.
    if stood_down_for_active_sibling(db, r).await? {
        return Ok(());
    }

    // Per-turn work from here down, reached by exactly one of the turn's
    // resolvers: resolve this turn's WaitResolved successor by explicit
    // identity, record it, and drive exactly one continuation carrying every
    // parked call's result. A racing continuation that already claimed the
    // predecessor's single successor yields a collision: defer to it rather
    // than hijacking a foreign turn.
    let prompt = resume_prompt_for_turn(db, r, result).await;
    let mut backoff = COLLISION_POLL_INITIAL;
    loop {
        // A cheap early out, not the fence: it keeps an already-abandoned
        // suspension from building a successor turn nobody will start. The
        // decision that actually governs resuming is the CAS below.
        if overtaken(db, &r.id).await {
            return Ok(());
        }
        match ensure_successor(orch, db, r, stored_successor.clone()).await? {
            SuccessorOutcome::Ready { turn_id, state } => {
                // The fence: win the row, or stand down. A supersession that
                // abandoned this suspension between the check above and here
                // takes the row instead, and this resolver resumes nothing.
                if !claim_continuation(db, &r.id, &turn_id).await? {
                    return Ok(());
                }
                // Drive continuation only while the successor still awaits its start.
                // A successor already started (or interrupted by a crash mid-resume)
                // means the resume was already delivered once; replay must not launch
                // a second one -- crash recovery owns an interrupted successor here.
                if state == TurnState::Pending {
                    continue_job_impl(
                        orch,
                        &r.job_id,
                        Some(&prompt),
                        None,
                        Some(ResumeContext {
                            suppress_user_event: true,
                            preclaimed_successor_turn_id: Some(turn_id),
                            ..Default::default()
                        }),
                    )?;
                }
                break;
            }
            SuccessorOutcome::Collision {
                state: TurnState::Pending,
                ..
            } => {
                // The predecessor's single successor was created by another
                // continuation that has NOT started it yet, so the run is still parked
                // -- it has not resumed. Recheck (with backoff) until that continuation
                // starts it (running) or terminalizes it, rather than falsely
                // resolving now (which would strand the parked agent) or giving up on
                // an arbitrary cutoff a slow-but-healthy continuation could exceed.
                // Host shutdown drops this task; startup reconciliation re-drives the
                // still-`resolving` row.
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(COLLISION_POLL_MAX);
            }
            SuccessorOutcome::Collision {
                turn_id,
                start_reason,
                state,
            } => {
                // A running or already-finished continuation resumed the run through
                // the predecessor's successor (e.g. a user steer mid-suspension): the
                // run resumed exactly once via that path. The awaited result is
                // already in the transcript (delivered above), so resolve without a
                // second, competing resume. Live delivery into an already-active turn
                // is precluded by one-active-turn, so the result is preserved there
                // rather than reaching this turn's prompt.
                log::warn!(
                    "suspension {} resolved into an existing {start_reason} successor {turn_id} (state {state}); recorded result without a second resume",
                    r.id
                );
                break;
            }
        }
    }

    // Mark resolved (idempotent CAS on the resolving state).
    let id = r.id.clone();
    db.write(|c|{let id=id.clone();Box::pin(async move{c.execute("UPDATE agent_waits SET state='resolved',resolved_at=?2 WHERE id=?1 AND state='resolving'",params![id,chrono::Utc::now().timestamp_millis()]).await?;Ok(())})}).await.map_err(|e|e.to_string())?;
    Ok(())
}

/// Decide, in one statement, whether this resolver is the last of its turn's
/// parked calls to settle.
///
/// `true` means a sibling of the same turn is still parked, so this row stood
/// down: it is marked resolved and its work is over. `false` means it is the
/// last one out and owns the turn's single resume.
///
/// It is ONE statement on purpose, and that is what makes it a single-winner
/// election rather than a race. Two siblings both reach it; whichever the writer
/// serializes second sees the first already `resolved`, fails the `EXISTS`, and
/// becomes the driver. Neither can stand down on the other, and neither can both
/// drive. Split into a read and a write, both would observe an active sibling in
/// the window between them and both would retire, stranding the parked turn.
///
/// Standing down is crash-safe because it happens only after this row has
/// delivered its own result: leaving the set startup re-drives costs nothing,
/// while the driver stays `resolving` until after its continuation, so a crash
/// leaves exactly one row for reconciliation to re-drive.
///
/// With a single parked call there is no sibling, the `EXISTS` is false, nothing
/// is updated, and everything below runs exactly as it always has.
async fn stood_down_for_active_sibling(db: &LocalDb, r: &Record) -> Result<bool, String> {
    let (id, job_id, turn_id) = (r.id.clone(), r.job_id.clone(), r.turn_id.clone());
    let now = chrono::Utc::now().timestamp_millis();
    let stood_down = db
        .write(|c| {
            let (id, job_id, turn_id) = (id.clone(), job_id.clone(), turn_id.clone());
            Box::pin(async move {
                let changed = c
                    .execute(
                        "UPDATE agent_waits SET state='resolved',resolved_at=?4 WHERE id=?1 AND state='resolving' AND EXISTS (SELECT 1 FROM agent_waits sibling WHERE sibling.job_id=?2 AND sibling.predecessor_turn_id=?3 AND sibling.id<>?1 AND sibling.state IN ('pending','resolving'))",
                        params![id, job_id, turn_id, now],
                    )
                    .await?;
                Ok(changed)
            })
        })
        .await
        .map_err(|e| e.to_string())?;
    if stood_down > 0 {
        log::debug!(
            "suspension {} settled while a sibling call of turn {} was still parked; standing down",
            r.id,
            r.turn_id
        );
        return Ok(true);
    }
    // Zero rows also covers a row that is no longer `resolving` -- cancelled by
    // a Stop, or abandoned by a reconstructed turn. Falling through is right:
    // `overtaken` and `claim_continuation` below are precisely the checks that
    // handle it, and they already say so in the log.
    Ok(false)
}

/// One parked call of a turn, as the composed resume prompt needs it.
struct ParkedCall {
    condition: Condition,
    tool_use_id: String,
    resolution: Option<String>,
}

/// The single prompt the resumed turn wakes with, carrying every one of its
/// parked calls' results.
///
/// This is the channel the MODEL reads, and it is why the turn resumes once
/// rather than N times. Each parked call separately delivers its own synthetic
/// `tool_result` to its own provider id, which keeps the transcript well-formed
/// and the UI honest -- but the resumed agent reads this string, forwarded as
/// its resume prompt (`execution::jobs::lifecycle::assemble_resume_prompt`). So
/// this is the one place N results have to become one message.
async fn resume_prompt_for_turn(db: &LocalDb, r: &Record, driver_result: &str) -> String {
    let calls = match parked_calls_of_turn(db, r).await {
        Ok(calls) => calls,
        Err(error) => {
            // A prompt is owed regardless. Resuming with this call's own result
            // gives the agent less than it should have; resuming with nothing
            // strands it.
            log::warn!(
                "could not read the parked calls of turn {} ({error}); resuming with only this call's result",
                r.turn_id
            );
            Vec::new()
        }
    };
    compose_resume_prompt(&calls, driver_result)
}

/// Compose one resume prompt from a turn's parked calls.
///
/// With a single answered call the prompt is that call's result VERBATIM. That
/// is the overwhelmingly common path and its bytes are the contract: every
/// suspended `run` batch and `waitFor` reads exactly what it read before this
/// existed. Only a genuinely concurrent turn gets the labeled form, because an
/// unlabeled concatenation would ask the model to guess which answer belongs to
/// which call.
///
/// A row with no recorded resolution is skipped rather than rendered empty. It
/// cannot happen in practice -- a row only leaves the active set with its
/// resolution written, and an active sibling would have won the election -- and
/// if it somehow did, degrading to this call's own result beats emitting a
/// section that answers nothing.
fn compose_resume_prompt(calls: &[ParkedCall], driver_result: &str) -> String {
    let answered: Vec<&ParkedCall> = calls
        .iter()
        .filter(|call| call.resolution.is_some())
        .collect();
    if answered.len() < 2 {
        return driver_result.to_string();
    }
    let sections: Vec<String> = answered
        .iter()
        .map(|call| {
            format!(
                "--- {} ({}) ---\n{}",
                call.condition.label(),
                call.tool_use_id,
                call.resolution.as_deref().unwrap_or_default()
            )
        })
        .collect();
    format!(
        "{} calls in this turn were suspended and have now finished. Each result below is labeled with the call it answers.\n\n{}",
        answered.len(),
        sections.join("\n\n")
    )
}

/// Every call one turn parked, oldest first, whatever state each has reached.
/// Siblings that already stood down are included: their results are exactly
/// what the composed prompt exists to carry.
async fn parked_calls_of_turn(db: &LocalDb, r: &Record) -> Result<Vec<ParkedCall>, String> {
    let (job_id, turn_id) = (r.job_id.clone(), r.turn_id.clone());
    db.read(|c| {
        let (job_id, turn_id) = (job_id.clone(), turn_id.clone());
        Box::pin(async move {
            let mut rows = c
                .query(
                    "SELECT condition_json,tool_use_id,resolution_json FROM agent_waits WHERE job_id=?1 AND predecessor_turn_id=?2 ORDER BY created_at,id",
                    params![job_id, turn_id],
                )
                .await?;
            let mut found = Vec::new();
            while let Some(row) = rows.next().await? {
                let condition_json = row.text(0)?;
                let condition = serde_json::from_str::<Condition>(&condition_json)
                    .map_err(|error| DbError::internal(error.to_string()))?;
                found.push(ParkedCall {
                    condition,
                    tool_use_id: row.text(1)?,
                    resolution: row.opt_text(2)?,
                });
            }
            Ok(found)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Resolution of a suspension's successor turn.
enum SuccessorOutcome {
    /// This suspension's own `WaitResolved` successor and its current state. A
    /// `Pending` state still awaits its start (drive the continuation); any other
    /// state means the resume was already delivered (replay must not re-drive it).
    Ready { turn_id: String, state: TurnState },
    /// A racing continuation already owns the predecessor's single successor; the
    /// suspension must not hijack that foreign turn. `state` decides the handling
    /// -- a pending foreign turn means the run has not resumed yet.
    Collision {
        turn_id: String,
        start_reason: String,
        state: TurnState,
    },
}

/// Resolve this suspension's `WaitResolved` successor by explicit identity (never
/// by predecessor alone): reuse its own successor on replay, create it pending
/// when absent, and report a collision when a foreign continuation already claimed
/// the predecessor's single successor.
async fn ensure_successor(
    orch: &Orchestrator,
    db: &LocalDb,
    r: &Record,
    stored_successor: Option<String>,
) -> Result<SuccessorOutcome, String> {
    let outcome = ensure_wait_resolved_successor(db, &r.run_id, &r.turn_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("suspension successor context missing for run {}", r.run_id))?;
    match outcome {
        WaitSuccessor::Ready(update) => {
            // Emits the insert db-change only when this attempt created the turn.
            emit_successor_turn_events(db, &*orch.services.emitter, &update).await;
            if let Some(stored) = stored_successor.as_deref() {
                if stored != update.turn_id {
                    log::warn!(
                        "suspension {} recorded successor {stored} but resolved {}",
                        r.id,
                        update.turn_id
                    );
                }
            }
            let state = load_turn_state(db, &update.turn_id).await?;
            Ok(SuccessorOutcome::Ready {
                turn_id: update.turn_id,
                state,
            })
        }
        WaitSuccessor::Collision {
            turn_id,
            start_reason,
            state,
        } => Ok(SuccessorOutcome::Collision {
            turn_id,
            start_reason,
            state,
        }),
    }
}

async fn load_turn_state(db: &LocalDb, turn_id: &str) -> Result<TurnState, String> {
    let turn_id = turn_id.to_string();
    db.read(|c| {
        let turn_id = turn_id.clone();
        Box::pin(async move {
            let mut rows = c
                .query(
                    "SELECT state FROM turns WHERE id=?1 LIMIT 1",
                    params![turn_id.clone()],
                )
                .await?;
            let state = rows
                .next()
                .await?
                .ok_or_else(|| DbError::Row(format!("turn not found: {turn_id}")))?
                .text(0)?;
            state.parse::<TurnState>().map_err(DbError::Row)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn load_resolution(
    db: &LocalDb,
    id: &str,
) -> Result<Option<(String, Option<String>, Option<String>)>, String> {
    let id = id.to_string();
    db.read(|c| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = c
                .query(
                    "SELECT state,resolution_json,successor_turn_id FROM agent_waits WHERE id=?1",
                    params![id],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| Ok((row.text(0)?, row.opt_text(1)?, row.opt_text(2)?)))
                .transpose()
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Claim the sole right to resume this run, reporting whether this resolver won
/// it.
///
/// This is the linearization point between resolving a suspension and
/// abandoning it. It and [`mark_abandoned`] are two updates to one row with
/// mutually exclusive predicates — this one refuses a row already marked
/// abandoned, that one refuses a row whose continuation is already owned — so
/// the database picks exactly one winner and the loser stands down.
///
/// Re-reading the decision before resuming cannot do this job. However close the
/// read sits to the resume, an abandonment can commit in between, and the stale
/// resolver resumes anyway; moving the read only shrinks the window. The winner
/// has to be decided by the same write that acts on it.
///
/// Owning the successor is re-entrant for a replay re-driving its own recorded
/// successor, which is how crash recovery re-enters this path after an
/// interrupted resume.
///
/// The abandonment test is an exact payload comparison rather than SQL-side JSON
/// extraction, because `resolution_json` also carries settled `run` batch output,
/// which is not JSON at all.
async fn claim_continuation(db: &LocalDb, id: &str, successor: &str) -> Result<bool, String> {
    let (id, successor) = (id.to_string(), successor.to_string());
    db.write(|c| {
        let (id, successor, abandoned) = (id.clone(), successor.clone(), abandoned_result());
        Box::pin(async move {
            let owned = c
                .execute(
                    "UPDATE agent_waits SET successor_turn_id=?2 WHERE id=?1 AND state='resolving' AND (successor_turn_id IS NULL OR successor_turn_id=?2) AND (resolution_json IS NULL OR resolution_json<>?3)",
                    params![id, successor, abandoned],
                )
                .await?;
            Ok(owned > 0)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Claim the sole right to answer a suspended call, reporting whether this
/// writer won it.
///
/// Two writers can answer one suspended call: its live resolver and a
/// supersession that found the row abandoned. Both claim through this single
/// statement and only the winner appends an event, so one tool use is never
/// answered twice.
///
/// The claim must be taken BEFORE the append, and atomically. Reading the slot,
/// appending, then marking it leaves a window in which a racing writer reads an
/// unclaimed slot and appends behind the first — which is what a
/// read-then-append-then-mark resolver did, defeating the marker entirely.
async fn claim_result_delivery(db: &LocalDb, id: &str) -> Result<bool, String> {
    let id = id.to_string();
    let now = chrono::Utc::now().timestamp_millis();
    db.write(|c| {
        let id = id.clone();
        Box::pin(async move {
            let claimed = c
                .execute(
                    "UPDATE agent_waits SET result_stored_at=?2 WHERE id=?1 AND result_stored_at IS NULL",
                    params![id, now],
                )
                .await?;
            Ok(claimed > 0)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Whether this call's answer is already in the transcript.
///
/// The event is the fact; `result_stored_at` is only a lock taken over it. They
/// cannot be written atomically together — the event insert is routed to the
/// run's owning replica through its own transaction — so anything that must
/// survive a restart asks the transcript, not the lock.
async fn result_already_delivered(
    db: &LocalDb,
    run_id: &str,
    tool_use_id: &str,
) -> Result<bool, String> {
    let (run_id, tool_use_id) = (run_id.to_string(), tool_use_id.to_string());
    db.read(|c| {
        let (run_id, tool_use_id) = (run_id.clone(), tool_use_id.clone());
        Box::pin(async move {
            let mut rows = c
                .query(
                    "SELECT 1 FROM events WHERE run_id=?1 AND event_type='tool_result' AND json_extract(data,'$.toolUseId')=?2 LIMIT 1",
                    params![run_id, tool_use_id],
                )
                .await?;
            Ok(rows.next().await?.is_some())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Release a delivery claim that a restart interrupted, so the re-drive can
/// still answer the call.
///
/// A claim is a lock, not a fact. A host that died between committing the claim
/// and appending the event leaves it held with nothing behind it, and every
/// later writer — including startup's own replay — would then decline to answer,
/// losing the result permanently. No delivery survives a restart, so a claim
/// with no event behind it at startup is stale by construction. An unreadable
/// transcript is treated as delivered: declining to answer twice is the safer
/// failure.
async fn reclaim_interrupted_delivery(db: &LocalDb, call: &SuspendedCall) {
    let held = matches!(load_delivery_claim(db, &call.id).await, Ok(Some(_)));
    if !held {
        return;
    }
    if result_already_delivered(db, &call.run_id, &call.tool_use_id)
        .await
        .unwrap_or(true)
    {
        return;
    }
    log::warn!(
        "suspension {} held a delivery claim with no result behind it; reclaimed after restart",
        call.id
    );
    release_result_delivery(db, &call.id).await;
}

async fn load_delivery_claim(db: &LocalDb, id: &str) -> Result<Option<i64>, String> {
    let id = id.to_string();
    db.read(|c| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = c
                .query(
                    "SELECT result_stored_at FROM agent_waits WHERE id=?1",
                    params![id],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| row.opt_i64(0))
                .transpose()
                .map(Option::flatten)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Give back a claim whose append never landed, so a later attempt can retry it.
/// This covers a returned error; a host that dies mid-append is covered by
/// [`reclaim_interrupted_delivery`] at startup.
async fn release_result_delivery(db: &LocalDb, id: &str) {
    let owned = id.to_string();
    let released = db
        .write(|c| {
            let id = owned.clone();
            Box::pin(async move {
                c.execute(
                    "UPDATE agent_waits SET result_stored_at=NULL WHERE id=?1",
                    params![id],
                )
                .await?;
                Ok(())
            })
        })
        .await;
    if let Err(error) = released {
        log::warn!("failed to release the delivery claim for suspension {id}: {error}");
    }
}

/// Append the answer this writer has already claimed, releasing the claim if the
/// append fails. Split from the claim so the two halves are separately
/// observable: everything between them is the window a racing writer must not
/// be able to answer in.
async fn append_claimed_result(
    orch: &Orchestrator,
    db: &LocalDb,
    r: &Record,
    result: &str,
) -> Result<(), String> {
    if let Err(error) = crate::execution::jobs::store_tool_result_event_with_turn(
        orch,
        &r.run_id,
        &r.session_id,
        &r.tool_use_id,
        result,
        false,
        chrono::Utc::now().timestamp() as i32,
        Some(&r.turn_id),
    ) {
        release_result_delivery(db, &r.id).await;
        return Err(error);
    }
    Ok(())
}

async fn store_result_once(
    orch: &Orchestrator,
    db: &LocalDb,
    r: &Record,
    result: &str,
) -> Result<(), String> {
    if !claim_result_delivery(db, &r.id).await? {
        return Ok(());
    }
    append_claimed_result(orch, db, r, result).await
}

/// Re-drive every suspension left pending or mid-resolution by a host restart.
///
/// A waitFor condition is level-triggered and can simply be re-armed. A run
/// batch cannot: its trigger was an in-memory join handle and its executor
/// result channel is gone, so it resolves to a typed failure that names what
/// happened -- fail closed and unblock the agent rather than strand it -- and
/// cancels the abandoned request so a surviving executor stops a child whose
/// result nobody will read.
pub(crate) async fn reconcile(orch: &Orchestrator) {
    for db in orch.db.all_dbs().await {
        let records = db.read(|conn| Box::pin(async move {
            let mut rows = conn.query(
                "SELECT id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,deadline_ms,created_at,state,resolution_json FROM agent_waits WHERE state IN ('pending','resolving')",
                (),
            ).await?;
            let mut found = Vec::new();
            while let Some(row) = rows.next().await? {
                let condition_json = row.text(6)?;
                let condition = serde_json::from_str::<Condition>(&condition_json)
                    .map_err(|error| DbError::internal(error.to_string()))?;
                found.push((Record {
                    id: row.text(0)?, job_id: row.text(1)?, run_id: row.text(2)?,
                    session_id: row.text(3)?, turn_id: row.text(4)?, tool_use_id: row.text(5)?,
                    condition, deadline: row.opt_i64(7)?, created: row.i64(8)?,
                }, row.text(9)?, row.opt_text(10)?));
            }
            Ok(found)
        })).await;
        let Ok(records) = records else {
            log::warn!("failed to load pending suspensions during startup");
            continue;
        };
        for (record, state, stored_result) in records {
            // MCP continuations are owned by the one host-global scheduler. At
            // startup their durable rows are already normalized; do not create a
            // per-wait task or wait for their next poll here.
            if let Condition::McpContinuation { mut state } = record.condition.clone() {
                super::mcp_continuation::ensure_deadline(
                    &mut state,
                    chrono::Utc::now().timestamp_millis(),
                );
                if let Err(error) = super::mcp_continuation::store(&db, &record.id, &state).await {
                    log::warn!(
                        "startup could not normalize MCP wait {}: {error}",
                        record.id
                    );
                }
                reclaim_interrupted_delivery(&db, &SuspendedCall::of(&record)).await;
                continue;
            }
            let (owned_orch, owned_db) = (orch.clone(), db.clone());
            tokio::spawn(async move {
                let call = SuspendedCall::of(&record);
                // Before re-driving, give back any delivery claim this row was
                // holding when the host died: the writer that held it is gone,
                // and the re-drive is what will actually answer the call.
                reclaim_interrupted_delivery(&owned_db, &call).await;
                // An abandonment the restart interrupted is FINISHED, never
                // re-driven. Its turn is gone, so driving it as an ordinary wait
                // would create a successor and resume a run the job has already
                // moved past -- resurrecting the suspension that was discarded.
                if is_abandonment(stored_result.as_deref()) {
                    if let Err(error) = finish_abandonment(&owned_orch, &owned_db, &call).await {
                        log::warn!("startup could not finish an abandoned suspension: {error}");
                    }
                    return;
                }
                let result = resolution_after_restart(
                    &owned_orch,
                    &owned_db,
                    &record,
                    &state,
                    stored_result,
                )
                .await;
                if let Err(error) = resolve(&owned_orch, &owned_db, &record, &result, true).await {
                    log::warn!("startup suspension resolution failed: {error}");
                }
            });
        }
    }
}

async fn resolution_after_restart(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    record: &Record,
    state: &str,
    stored_result: Option<String>,
) -> String {
    if state == "resolving" {
        return stored_result.unwrap_or_else(|| {
            serde_json::json!({"outcome":"error","error":"wait resolution payload missing"})
                .to_string()
        });
    }
    match &record.condition {
        Condition::McpContinuation { .. } => {
            match super::mcp_continuation::drive(orch.clone(), db.clone(), record.clone()).await {
                super::mcp_continuation::DriveOutcome::Terminal(result) => result,
                super::mcp_continuation::DriveOutcome::Pending => {
                    serde_json::json!({"outcome":"pending"}).to_string()
                }
            }
        }
        Condition::RunBatch {
            request_id,
            commits,
            ..
        } => {
            orch.cancel_cell_request(request_id);
            super::run::run_batch_lost_to_restart_text(*commits)
        }
        Condition::Duration | Condition::Terminal { .. } => {
            match super::owned_wait::trigger(orch.clone(), db.clone(), record.clone()).await {
                Ok(result) => result,
                Err(error) => serde_json::json!({"outcome":"error","error":error}).to_string(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::internal::services::testing::CapturingEmitter;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use async_trait::async_trait;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    use crate::config::mcp_servers::McpServerConfig;
    use crate::mcp::gateway::{
        McpCallOutcome, McpGateway, McpResourceDef, McpTaskOutcome, McpToolCallResult,
        McpToolCatalog,
    };

    // ---- Durable resolution path -------------------------------------------

    /// Full continue-ready seed: an open session, a live run, and a `running`
    /// predecessor turn wired as the job's current turn. `insert` yields the
    /// predecessor and creates the pending wait row before resolution runs.
    async fn durable_env() -> (Orchestrator, Record, Condition) {
        let (orch, record, _) = durable_env_capturing().await;
        (orch, record, Condition::Duration)
    }

    /// An `EventEmitter` several handles can share, so a test can read what the
    /// orchestrator announced.
    struct SharedCapturingEmitter(Arc<CapturingEmitter>);

    impl crate::services::EventEmitter for SharedCapturingEmitter {
        fn emit(&self, event: &str, payload: serde_json::Value) -> Result<(), String> {
            self.0.emit(event, payload)
        }

        fn emit_empty(&self, event: &str) -> Result<(), String> {
            self.0.emit_empty(event)
        }
    }

    /// The same seed, handing back the emitter so a test can assert on what the
    /// suspension told clients.
    async fn durable_env_capturing() -> (Orchestrator, Record, Arc<CapturingEmitter>) {
        let root = tempfile::tempdir().unwrap().keep();
        let local = LocalDb::open(root.join("test.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        for sql in [
            "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1)",
            "INSERT INTO issues (id,project_id,number,title,status,created_at,updated_at) VALUES ('i','p',1,'T','active',1,1)",
            "INSERT INTO executions (id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id,execution_id,issue_id,project_id,node_name,status,created_at,updated_at,uri_segment) VALUES ('job-1','e','i','p','Builder','running',1,1,'builder')",
            "INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-1','job-1','open','handle-1',1,1)",
            "INSERT INTO runs (id,issue_id,project_id,job_id,status,session_id,created_at,updated_at) VALUES ('run-1','i','p','job-1','live','session-1',1,1)",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('pred-turn','session-1','run-1','job-1',1,'running','initial',1,1)",
            // current_session_id / current_turn_id carry FKs, so wire them only
            // after the session and turn rows exist.
            "UPDATE jobs SET current_session_id='session-1', current_turn_id='pred-turn' WHERE id='job-1'",
        ] {
            local.execute(sql, ()).await.unwrap();
        }
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let emitter = Arc::new(CapturingEmitter::new());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(
                TestServicesBuilder::new()
                    .with_emitter(SharedCapturingEmitter(emitter.clone()))
                    .build(),
            ),
            root,
        )
        .build();
        let now = chrono::Utc::now().timestamp_millis();
        let record = Record {
            id: "wait-1".into(),
            job_id: "job-1".into(),
            run_id: "run-1".into(),
            session_id: "session-1".into(),
            turn_id: "pred-turn".into(),
            tool_use_id: "tool-1".into(),
            condition: Condition::Duration,
            deadline: Some(now - 1000),
            created: now,
        };
        (orch, record, emitter)
    }

    /// Every `db-change` this test's orchestrator announced for the turns table.
    fn turn_changes(emitter: &CapturingEmitter) -> Vec<serde_json::Value> {
        emitter
            .events_named("db-change")
            .into_iter()
            .filter(|payload| payload.get("table").and_then(|t| t.as_str()) == Some("turns"))
            .collect()
    }

    /// A `run` batch suspension: no pollable condition, only the request the
    /// suspending host was awaiting.
    fn run_batch_record(base: &Record, commits: bool) -> Record {
        Record {
            condition: run_batch_condition(commits, None),
            deadline: None,
            ..base.clone()
        }
    }

    fn run_batch_condition(commits: bool, label: Option<&str>) -> Condition {
        Condition::RunBatch {
            request_id: "request-1".into(),
            commits,
            label: label.map(str::to_string),
        }
    }

    /// A second call parked by the SAME turn: its own row and its own provider
    /// call, sharing everything that makes it a sibling.
    fn sibling_record(base: &Record) -> Record {
        Record {
            id: "wait-2".into(),
            tool_use_id: "tool-2".into(),
            ..base.clone()
        }
    }

    /// A run batch cannot be re-armed after a restart: the join handle and the
    /// executor result channel were in memory. Reconciliation must therefore
    /// resolve it exactly once to a typed failure that unblocks the agent, rather
    /// than leaving a pending row nobody will ever satisfy.
    #[tokio::test]
    async fn a_pending_run_batch_resolves_to_a_typed_host_restart_failure() {
        let (orch, base, _) = durable_env().await;
        register_warm(&orch);
        let record = run_batch_record(&base, true);
        insert(&orch.db.local, &record).await.unwrap();

        reconcile(&orch).await;

        // Reconciliation drives its resolvers on spawned tasks.
        for _ in 0..200 {
            if wait_state(&orch).await == "resolved" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(wait_state(&orch).await, "resolved");

        let resolution = orch
            .db
            .local
            .query_one(
                "SELECT resolution_json FROM agent_waits WHERE id='wait-1'",
                (),
                |r| r.text(0),
            )
            .await
            .unwrap();
        assert!(
            resolution.contains("restarted") && resolution.contains("nothing was committed"),
            "unexpected restart resolution: {resolution}"
        );
        // Exactly one resume, exactly one delivered result.
        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
        assert_eq!(tool_result_count(&orch).await, 1);
    }

    /// A run batch has no waitFor condition to await; asking for one is a bug,
    /// not a silent no-op that would hang the resolver.
    #[tokio::test]
    async fn a_run_batch_has_no_pollable_wait_condition() {
        let (orch, base, _) = durable_env().await;
        let record = run_batch_record(&base, false);
        let error = super::super::owned_wait::trigger(orch.clone(), orch.db.local.clone(), record)
            .await
            .unwrap_err();
        assert!(error.contains("no waitFor condition"), "{error}");
    }

    type RecordedToolCall = (Option<serde_json::Value>, Option<String>, Option<String>);
    type RecordedTaskUpdate = (String, serde_json::Value, Option<String>);

    #[derive(Default)]
    struct FakeMcpGateway {
        calls: Mutex<Vec<RecordedToolCall>>,
        call_outcomes: Mutex<VecDeque<McpCallOutcome>>,
        fail_call_after_dispatch: AtomicBool,
        task_outcomes: Mutex<VecDeque<Result<McpTaskOutcome, String>>>,
        task_polls: Mutex<Vec<String>>,
        updates: Mutex<Vec<RecordedTaskUpdate>>,
        fail_update_after_dispatch: AtomicBool,
    }

    #[async_trait]
    impl McpGateway for FakeMcpGateway {
        async fn list_tools(
            &self,
            _: &str,
            _: &str,
            _: &McpServerConfig,
        ) -> Result<McpToolCatalog, String> {
            Ok(McpToolCatalog::default())
        }
        async fn list_resources(
            &self,
            _: &str,
            _: &str,
            _: &McpServerConfig,
        ) -> Result<Vec<McpResourceDef>, String> {
            Ok(vec![])
        }
        async fn read_resource(
            &self,
            _: &str,
            _: &str,
            _: &McpServerConfig,
            _: &str,
        ) -> Result<String, String> {
            Ok(String::new())
        }
        async fn call_tool_once(
            &self,
            _: &str,
            _: &str,
            _: &McpServerConfig,
            _: &str,
            _: serde_json::Value,
            input: Option<serde_json::Value>,
            request_state: Option<String>,
            _: Option<u32>,
            operation_id: Option<&str>,
        ) -> Result<McpCallOutcome, String> {
            self.calls.lock().unwrap().push((
                input,
                request_state,
                operation_id.map(str::to_owned),
            ));
            if self.fail_call_after_dispatch.swap(false, Ordering::SeqCst) {
                return Err("injected crash after tools/call dispatch".into());
            }
            self.call_outcomes
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| "unexpected tool retry".into())
        }
        async fn get_task(
            &self,
            _: &str,
            _: &str,
            _: &McpServerConfig,
            task_id: &str,
        ) -> Result<McpTaskOutcome, String> {
            self.task_polls.lock().unwrap().push(task_id.into());
            self.task_outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err("task was lost".into()))
        }
        async fn update_task(
            &self,
            _: &str,
            _: &str,
            _: &McpServerConfig,
            task_id: &str,
            input: serde_json::Value,
            operation_id: Option<&str>,
        ) -> Result<(), String> {
            self.updates.lock().unwrap().push((
                task_id.into(),
                input,
                operation_id.map(str::to_owned),
            ));
            if self
                .fail_update_after_dispatch
                .swap(false, Ordering::SeqCst)
            {
                return Err("injected crash after tasks/update dispatch".into());
            }
            Ok(())
        }
        async fn close_session(&self, _: &str) {}
    }

    fn mcp_state() -> super::super::mcp_continuation::McpContinuationState {
        super::super::mcp_continuation::McpContinuationState {
            server: "fake".into(),
            session_key: "job-1".into(),
            config: serde_json::from_value(serde_json::json!({})).unwrap(),
            tool: "durable".into(),
            arguments: serde_json::json!({"original": true}),
            request_state: None,
            timeout_ms: None,
            pending_operation: None,
            pending_input_requests: None,
            task_input_pending: false,
            mrtr_round: 0,
            pending_prompt_id: None,
            task: None,
            next_poll_at_ms: None,
            deadline_ms: None,
        }
    }

    async fn drive_mcp_until_terminal(
        orch: Orchestrator,
        db: Arc<LocalDb>,
        record: Record,
    ) -> String {
        loop {
            match super::super::mcp_continuation::drive(orch.clone(), db.clone(), record.clone())
                .await
            {
                super::super::mcp_continuation::DriveOutcome::Terminal(result) => return result,
                super::super::mcp_continuation::DriveOutcome::Pending => {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    }

    async fn answer_latest_prompt(orch: &Orchestrator, answer: serde_json::Value) -> String {
        let prompt_id = loop {
            if let Ok(id) = orch.db.local.query_one(
                "SELECT id FROM prompts WHERE mcp_wait_id='wait-1' ORDER BY created_at DESC LIMIT 1", (), |r| r.text(0)
            ).await { break id; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let encoded = answer.to_string();
        orch.db
            .local
            .execute(
                "UPDATE prompts SET response=?1, answered_at=1 WHERE id=?2",
                (encoded.clone(), prompt_id.clone()),
            )
            .await
            .unwrap();
        let _ = orch.prompt_responses.send((prompt_id.clone(), encoded));
        prompt_id
    }

    async fn leave_prompt_pending_after_host_loss(orch: &Orchestrator, record: &Record) -> String {
        let driver = tokio::spawn(drive_mcp_until_terminal(
            orch.clone(),
            orch.db.local.clone(),
            record.clone(),
        ));
        let prompt_id = loop {
            if let Ok(id) = orch.db.local.query_one(
                "SELECT id FROM prompts WHERE mcp_wait_id='wait-1' ORDER BY created_at DESC LIMIT 1", (), |r| r.text(0)
            ).await {
                let persisted = super::super::mcp_continuation::load(&orch.db.local, "wait-1")
                    .await
                    .unwrap()
                    .and_then(|state| state.pending_prompt_id);
                if persisted.as_deref() == Some(id.as_str()) {
                    break id;
                }
            }
            tokio::task::yield_now().await;
        };
        driver.abort();
        let _ = driver.await;
        prompt_id
    }

    async fn wait_for_reconciled_resolution(orch: &Orchestrator) {
        for _ in 0..200 {
            super::super::mcp_continuation::scheduler_tick(orch).await;
            if wait_state(orch).await == "resolved" {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("restart reconciliation did not resolve the MCP wait");
    }

    #[tokio::test]
    async fn restart_reconciliation_redrives_a_pending_direct_prompt_once() {
        let (orch, mut record, _) = durable_env().await;
        register_warm(&orch);
        let gateway = Arc::new(FakeMcpGateway::default());
        gateway
            .call_outcomes
            .lock()
            .unwrap()
            .push_back(McpCallOutcome::Complete(McpToolCallResult {
                text: "restart complete".into(),
                images: vec![],
            }));
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut state = mcp_state();
        state.pending_input_requests = Some(serde_json::json!([{"id":"approval"}]));
        state.request_state = Some("restart-round".into());
        record.condition = Condition::McpContinuation { state };
        record.deadline = None;
        insert(&orch.db.local, &record).await.unwrap();
        leave_prompt_pending_after_host_loss(&orch, &record).await;
        answer_latest_prompt(&orch, serde_json::json!({"approval":true})).await;

        reconcile(&orch).await;
        wait_for_reconciled_resolution(&orch).await;

        assert_eq!(gateway.calls.lock().unwrap().len(), 1);
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
    }

    #[tokio::test]
    async fn restart_reconciliation_reacquires_and_completes_a_pending_mcp_task() {
        let (orch, mut record, _) = durable_env().await;
        register_warm(&orch);
        let gateway = Arc::new(FakeMcpGateway::default());
        gateway
            .task_outcomes
            .lock()
            .unwrap()
            .push_back(Ok(McpTaskOutcome::Complete(McpToolCallResult {
                text: "reacquired task".into(),
                images: vec![],
            })));
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut state = mcp_state();
        super::super::mcp_continuation::set_task(
            &mut state,
            "task-restart".into(),
            1,
            chrono::Utc::now().timestamp_millis() - 1_000,
            Some(30_000),
        );
        record.condition = Condition::McpContinuation { state };
        record.deadline = None;
        insert(&orch.db.local, &record).await.unwrap();

        reconcile(&orch).await;
        wait_for_reconciled_resolution(&orch).await;

        assert_eq!(
            gateway.task_polls.lock().unwrap().as_slice(),
            &["task-restart"]
        );
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
    }

    #[tokio::test]
    async fn duplicate_answer_and_poll_wakes_emit_one_update_result_and_successor() {
        let (orch, mut record, _) = durable_env().await;
        register_warm(&orch);
        let gateway = Arc::new(FakeMcpGateway::default());
        gateway
            .task_outcomes
            .lock()
            .unwrap()
            .push_back(Ok(McpTaskOutcome::Complete(McpToolCallResult {
                text: "race complete".into(),
                images: vec![],
            })));
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut state = mcp_state();
        super::super::mcp_continuation::set_task(
            &mut state,
            "task-race".into(),
            1,
            chrono::Utc::now().timestamp_millis() - 1_000,
            Some(30_000),
        );
        state.pending_input_requests = Some(serde_json::json!([{"id":"choice"}]));
        state.task_input_pending = true;
        record.condition = Condition::McpContinuation { state };
        record.deadline = None;
        insert(&orch.db.local, &record).await.unwrap();
        leave_prompt_pending_after_host_loss(&orch, &record).await;
        answer_latest_prompt(&orch, serde_json::json!({"choice":"a"})).await;

        tokio::join!(reconcile(&orch), reconcile(&orch));
        wait_for_reconciled_resolution(&orch).await;

        let (update_count, updated_task) = {
            let updates = gateway.updates.lock().unwrap();
            (updates.len(), updates[0].0.clone())
        };
        assert_eq!(update_count, 1);
        assert_eq!(updated_task, "task-race");
        assert_eq!(
            gateway.task_polls.lock().unwrap().as_slice(),
            &["task-race"]
        );
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
    }

    #[tokio::test]
    async fn one_shot_drive_returns_while_prompt_or_future_poll_is_pending() {
        let (orch, mut record, _) = durable_env().await;
        let gateway = Arc::new(FakeMcpGateway::default());
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut prompt_state = mcp_state();
        prompt_state.pending_input_requests = Some(serde_json::json!([{"id":"approval"}]));
        record.condition = Condition::McpContinuation {
            state: prompt_state,
        };
        insert(&orch.db.local, &record).await.unwrap();

        assert_eq!(
            super::super::mcp_continuation::drive(
                orch.clone(),
                orch.db.local.clone(),
                record.clone()
            )
            .await,
            super::super::mcp_continuation::DriveOutcome::Pending
        );
        assert_eq!(
            super::super::mcp_continuation::drive(
                orch.clone(),
                orch.db.local.clone(),
                record.clone()
            )
            .await,
            super::super::mcp_continuation::DriveOutcome::Pending
        );

        let mut future = mcp_state();
        super::super::mcp_continuation::set_task(
            &mut future,
            "future-task".into(),
            60_000,
            chrono::Utc::now().timestamp_millis(),
            Some(120_000),
        );
        super::super::mcp_continuation::store(&orch.db.local, &record.id, &future)
            .await
            .unwrap();
        assert_eq!(
            super::super::mcp_continuation::drive(orch.clone(), orch.db.local.clone(), record)
                .await,
            super::super::mcp_continuation::DriveOutcome::Pending
        );
        assert!(gateway.task_polls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn mcp_input_prompt_consumes_one_answer_and_completes_the_original_call_once() {
        let (orch, mut record, _) = durable_env().await;
        register_warm(&orch);
        let gateway = Arc::new(FakeMcpGateway::default());
        gateway
            .call_outcomes
            .lock()
            .unwrap()
            .push_back(McpCallOutcome::Complete(McpToolCallResult {
                text: "finished".into(),
                images: vec![],
            }));
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut state = mcp_state();
        state.pending_input_requests = Some(serde_json::json!([{"id":"approval"}]));
        state.request_state = Some("round-1".into());
        record.condition = Condition::McpContinuation { state };
        record.deadline = None;
        insert(&orch.db.local, &record).await.unwrap();

        let driver = tokio::spawn(drive_mcp_until_terminal(
            orch.clone(),
            orch.db.local.clone(),
            record.clone(),
        ));
        let prompt_id = answer_latest_prompt(&orch, serde_json::json!({"approval":true})).await;
        let result = driver.await.unwrap();
        assert!(result.contains("finished"), "{result}");
        let prompt = orch
            .db
            .local
            .query_one(
                "SELECT mcp_wait_id,turn_id FROM prompts WHERE id=?1",
                (prompt_id,),
                |r| Ok((r.text(0)?, r.opt_text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            prompt,
            ("wait-1".into(), None),
            "prompt must be visibly owned by the MCP wait, not an independent turn"
        );
        assert_eq!(
            gateway.calls.lock().unwrap()[0].0,
            Some(serde_json::json!({"approval":true}))
        );

        resolve(&orch, &orch.db.local, &record, &result, false)
            .await
            .unwrap();
        assert_eq!(tool_result_count(&orch).await, 1);
        let correlation = orch.db.local.query_one("SELECT run_id,json_extract(data,'$.toolUseId') FROM events WHERE event_type='tool_result'", (), |r| Ok((r.text(0)?, r.text(1)?))).await.unwrap();
        assert_eq!(correlation, ("run-1".into(), "tool-1".into()));
    }

    #[tokio::test]
    async fn mcp_task_working_input_and_terminal_outcomes_are_driven_without_an_executor() {
        let (orch, mut record, _) = durable_env().await;
        let gateway = Arc::new(FakeMcpGateway::default());
        gateway.task_outcomes.lock().unwrap().extend([
            Ok(McpTaskOutcome::Working {
                poll_interval_ms: Some(1),
            }),
            Ok(McpTaskOutcome::InputRequired {
                input_requests: serde_json::json!([{"id":"choice"}]),
            }),
            Ok(McpTaskOutcome::Complete(McpToolCallResult {
                text: "task done".into(),
                images: vec![],
            })),
        ]);
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut state = mcp_state();
        super::super::mcp_continuation::set_task(
            &mut state,
            "task-1".into(),
            1,
            chrono::Utc::now().timestamp_millis() - 100,
            Some(30_000),
        );
        record.condition = Condition::McpContinuation { state };
        record.deadline = None;
        insert(&orch.db.local, &record).await.unwrap();

        let driver = tokio::spawn(drive_mcp_until_terminal(
            orch.clone(),
            orch.db.local.clone(),
            record,
        ));
        answer_latest_prompt(&orch, serde_json::json!({"choice":"a"})).await;
        let result = driver.await.unwrap();
        assert!(result.contains("task done"), "{result}");
        assert_eq!(gateway.updates.lock().unwrap()[0].0, "task-1");
    }

    #[tokio::test]
    async fn persisted_continue_tool_replays_same_operation_after_dispatch_crash() {
        use super::super::mcp_continuation::{load, PendingOperation};
        let (orch, mut record, _) = durable_env().await;
        let gateway = Arc::new(FakeMcpGateway::default());
        gateway
            .fail_call_after_dispatch
            .store(true, Ordering::SeqCst);
        gateway
            .call_outcomes
            .lock()
            .unwrap()
            .push_back(McpCallOutcome::Complete(McpToolCallResult {
                text: "replayed".into(),
                images: vec![],
            }));
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut state = mcp_state();
        state.pending_operation = Some(PendingOperation::ContinueTool {
            operation_id: "stable-continue".into(),
            input_responses: serde_json::json!({"approval": true}),
            request_state: Some("round-1".into()),
        });
        record.condition = Condition::McpContinuation { state };
        insert(&orch.db.local, &record).await.unwrap();

        let first =
            drive_mcp_until_terminal(orch.clone(), orch.db.local.clone(), record.clone()).await;
        assert!(first.contains("injected crash"), "{first}");
        assert!(load(&orch.db.local, &record.id)
            .await
            .unwrap()
            .unwrap()
            .pending_operation
            .is_some());
        let db = orch.db.local.clone();
        let second = drive_mcp_until_terminal(orch, db, record).await;
        assert!(second.contains("replayed"), "{second}");
        let calls = gateway.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].2.as_deref(), Some("stable-continue"));
        assert_eq!(calls[1].2, calls[0].2);
    }

    #[tokio::test]
    async fn persisted_update_task_replays_same_operation_after_dispatch_crash() {
        use super::super::mcp_continuation::{load, PendingOperation};
        let (orch, mut record, _) = durable_env().await;
        let gateway = Arc::new(FakeMcpGateway::default());
        gateway
            .fail_update_after_dispatch
            .store(true, Ordering::SeqCst);
        gateway
            .task_outcomes
            .lock()
            .unwrap()
            .push_back(Ok(McpTaskOutcome::Complete(McpToolCallResult {
                text: "task replayed".into(),
                images: vec![],
            })));
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut state = mcp_state();
        super::super::mcp_continuation::set_task(
            &mut state,
            "task-1".into(),
            1,
            chrono::Utc::now().timestamp_millis() - 100,
            Some(30_000),
        );
        state.task_input_pending = true;
        state.pending_operation = Some(PendingOperation::UpdateTask {
            operation_id: "stable-update".into(),
            task_id: "task-1".into(),
            input_responses: serde_json::json!({"choice": "a"}),
        });
        record.condition = Condition::McpContinuation { state };
        insert(&orch.db.local, &record).await.unwrap();

        let first =
            drive_mcp_until_terminal(orch.clone(), orch.db.local.clone(), record.clone()).await;
        assert!(first.contains("injected crash"), "{first}");
        assert!(load(&orch.db.local, &record.id)
            .await
            .unwrap()
            .unwrap()
            .pending_operation
            .is_some());
        let db = orch.db.local.clone();
        let second = drive_mcp_until_terminal(orch, db, record).await;
        assert!(second.contains("task replayed"), "{second}");
        let updates = gateway.updates.lock().unwrap();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].2.as_deref(), Some("stable-update"));
        assert_eq!(updates[1].2, updates[0].2);
    }

    #[tokio::test]
    async fn mcp_task_failed_cancelled_lost_and_ttl_are_terminal_errors() {
        for (outcome, expected) in [
            (
                Some(Ok(McpTaskOutcome::Failed {
                    message: "broken".into(),
                })),
                "MCP task failed: broken",
            ),
            (
                Some(Ok(McpTaskOutcome::Cancelled)),
                "MCP task was cancelled",
            ),
            (Some(Err("task was lost".into())), "task was lost"),
            (None, "MCP task TTL expired"),
        ] {
            let (orch, mut record, _) = durable_env().await;
            let gateway = Arc::new(FakeMcpGateway::default());
            if let Some(outcome) = outcome {
                gateway.task_outcomes.lock().unwrap().push_back(outcome);
            }
            assert!(orch.set_mcp_gateway(gateway).is_ok());
            let mut state = mcp_state();
            super::super::mcp_continuation::set_task(
                &mut state,
                "task-1".into(),
                1,
                chrono::Utc::now().timestamp_millis() - 100,
                Some(30_000),
            );
            if expected.contains("TTL") {
                state.deadline_ms = Some(chrono::Utc::now().timestamp_millis() - 1);
            }
            record.condition = Condition::McpContinuation { state };
            record.deadline = None;
            insert(&orch.db.local, &record).await.unwrap();
            let db = orch.db.local.clone();
            let result = drive_mcp_until_terminal(orch, db, record).await;
            assert!(result.contains(expected), "expected {expected}: {result}");
        }
    }

    /// Register an in-memory warm process for `run-1`/`session-1` so
    /// `continue_job_impl` reuses it instead of spawning a real CLI.
    fn register_warm(orch: &Orchestrator) {
        register_warm_for(orch, "run-1", "session-1");
    }

    fn register_warm_for(orch: &Orchestrator, run_id: &str, session_id: &str) {
        let mut processes = orch.process_state.processes.lock().unwrap();
        let child = Arc::new(std::sync::Mutex::new(None));
        let stdin = Arc::new(std::sync::Mutex::new(Some(
            crate::agent_process::process::wrap_plain_stdin(Box::new(Vec::<u8>::new())),
        )));
        let mut handle = crate::agent_process::process::RunHandle::new(
            child,
            stdin,
            Some(session_id.to_string()),
            Some("job-1".to_string()),
        );
        handle.transition_to_warm();
        processes.register(run_id.to_string(), handle);
    }

    async fn wait_state(orch: &Orchestrator) -> String {
        orch.db
            .local
            .query_one("SELECT state FROM agent_waits WHERE id='wait-1'", (), |r| {
                r.text(0)
            })
            .await
            .unwrap()
    }

    async fn wait_resolved_turns(orch: &Orchestrator) -> Vec<(String, String)> {
        orch.db
            .local
            .read(|c| {
                Box::pin(async move {
                    let mut rows = c
                        .query(
                            "SELECT id,state FROM turns WHERE start_reason='wait_resolved' ORDER BY sequence",
                            (),
                        )
                        .await?;
                    let mut v = Vec::new();
                    while let Some(r) = rows.next().await? {
                        v.push((r.text(0)?, r.text(1)?));
                    }
                    Ok(v)
                })
            })
            .await
            .unwrap()
    }

    async fn tool_result_count(orch: &Orchestrator) -> i64 {
        orch.db
            .local
            .query_one(
                "SELECT COUNT(*) FROM events WHERE event_type='tool_result'",
                (),
                |r| r.i64(0),
            )
            .await
            .unwrap()
    }

    async fn turn_state_of(orch: &Orchestrator, id: &str) -> String {
        let id = id.to_string();
        orch.db
            .local
            .query_one("SELECT state FROM turns WHERE id=?1", (id,), |r| r.text(0))
            .await
            .unwrap()
    }

    const ELAPSED: &str = r#"{"outcome":"elapsed","elapsedMs":1,"deadlineMs":1}"#;
    const EXITED: &str = r#"{"outcome":"exited","terminal":"t","exitCode":0}"#;

    #[tokio::test]
    async fn durable_duration_resolves_once_with_single_successor_and_resumes_warm() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();

        // Exactly one WaitResolved successor, and it is running (started on resume).
        let successors = wait_resolved_turns(&orch).await;
        assert_eq!(successors.len(), 1, "exactly one WaitResolved successor");
        assert_eq!(successors[0].1, "running", "successor started on resume");
        // The synthetic result was delivered once; the predecessor stays yielded.
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(turn_state_of(&orch, "pred-turn").await, "yielded");
        // The wait row reached resolved.
        assert_eq!(wait_state(&orch).await, "resolved");
        // The warm process was resumed onto the successor, not left parked.
        assert_eq!(
            orch.process_state.get_current_turn_id("run-1").as_deref(),
            Some(successors[0].0.as_str()),
            "warm process resumed onto the successor turn"
        );
    }

    #[tokio::test]
    async fn duplicate_resolution_creates_no_second_successor_or_result() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();
        // A duplicate live delivery on the already-resolved row is a clean no-op.
        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();

        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_state(&orch).await, "resolved");
    }

    #[tokio::test]
    async fn resolving_replay_reuses_persisted_successor_without_second_continuation() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();
        let successor_id = wait_resolved_turns(&orch).await[0].0.clone();

        // Simulate a crash after the continuation but before the resolved CAS: the
        // row is stuck at `resolving` with its successor persisted, and startup
        // replay re-drives it.
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='resolving' WHERE id='wait-1'",
                (),
            )
            .await
            .unwrap();

        resolve(&orch, &orch.db.local, &record, ELAPSED, true)
            .await
            .unwrap();

        // No second successor, no second tool result — the already-running
        // successor short-circuits the continuation.
        let successors = wait_resolved_turns(&orch).await;
        assert_eq!(successors.len(), 1);
        assert_eq!(successors[0].0, successor_id);
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_state(&orch).await, "resolved");
    }

    #[tokio::test]
    async fn terminal_condition_durable_path_resolves_and_resumes() {
        let (orch, mut record, _) = durable_env().await;
        record.condition = Condition::Terminal {
            uri: "cairn://p/PRJ/1/1/builder/terminal/tests".into(),
            slug: "tests".into(),
            on: TerminalWaitEvent::Exit,
            phrase: None,
        };
        record.deadline = None;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        let exited = r#"{"outcome":"exited","terminal":"t","exitCode":0}"#;
        resolve(&orch, &orch.db.local, &record, exited, false)
            .await
            .unwrap();

        let successors = wait_resolved_turns(&orch).await;
        assert_eq!(successors.len(), 1);
        assert_eq!(successors[0].1, "running");
        assert_eq!(wait_state(&orch).await, "resolved");
        assert_eq!(
            orch.process_state.get_current_turn_id("run-1").as_deref(),
            Some(successors[0].0.as_str())
        );
    }

    /// Insert a foreign (non-wait_resolved) successor of the predecessor in the
    /// given state, as if a racing continuation had claimed the predecessor's
    /// single successor mid-wait.
    async fn insert_foreign_successor(orch: &Orchestrator, state: &str) {
        let state = state.to_string();
        orch.db
            .local
            .execute(
                "INSERT INTO turns (id,session_id,run_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at) VALUES ('foreign','session-1','run-1','job-1',2,'pred-turn',?1,'follow_up',2,2)",
                (state,),
            )
            .await
            .unwrap();
        orch.db
            .local
            .execute(
                "UPDATE jobs SET current_turn_id='foreign' WHERE id='job-1'",
                (),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn collision_with_running_continuation_records_result_and_resolves() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        // A continuation already RESUMED the run through the predecessor's successor.
        insert_foreign_successor(&orch, "running").await;
        register_warm(&orch);

        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();

        // The run has resumed exactly once (via the foreign turn); the wait records
        // its result and resolves without creating or hijacking a turn.
        assert!(
            wait_resolved_turns(&orch).await.is_empty(),
            "no WaitResolved turn created on collision"
        );
        assert_eq!(
            turn_state_of(&orch, "foreign").await,
            "running",
            "foreign successor is left untouched"
        );
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_state(&orch).await, "resolved");
        let successor: Option<String> = orch
            .db
            .local
            .query_one(
                "SELECT successor_turn_id FROM agent_waits WHERE id='wait-1'",
                (),
                |r| r.opt_text(0),
            )
            .await
            .unwrap();
        assert_eq!(
            successor, None,
            "foreign turn is not recorded as the successor"
        );
    }

    #[tokio::test]
    async fn collision_resolves_in_process_once_foreign_successor_starts() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        // The racing continuation has created the successor but not started it yet.
        insert_foreign_successor(&orch, "pending").await;
        register_warm(&orch);

        // It starts the foreign successor shortly after the resolver first observes
        // it pending — as a warm reuse would, within the poll budget.
        let db = orch.db.local.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            db.execute("UPDATE turns SET state='running' WHERE id='foreign'", ())
                .await
                .unwrap();
        });

        // The SAME live resolver — no restart — polls the transition and resolves.
        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();

        assert!(wait_resolved_turns(&orch).await.is_empty());
        assert_eq!(turn_state_of(&orch, "foreign").await, "running");
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(
            wait_state(&orch).await,
            "resolved",
            "in-process poll resolves the wait once the foreign turn starts"
        );
    }

    #[tokio::test]
    async fn collision_polls_while_unstarted_then_reconciliation_resolves_after_restart() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        // A racing continuation created the predecessor's successor but has NOT
        // started it — the run is still parked, so the wait must not falsely resolve.
        insert_foreign_successor(&orch, "pending").await;
        register_warm(&orch);

        // While the foreign successor stays pending, the resolver keeps polling and
        // never completes (no false resolve, no arbitrary give-up cutoff). Dropping
        // the future when the timeout fires models the resolver task being cancelled
        // by host shutdown.
        let polling = tokio::time::timeout(
            Duration::from_millis(250),
            resolve(&orch, &orch.db.local, &record, ELAPSED, false),
        )
        .await;
        assert!(
            polling.is_err(),
            "resolver keeps waiting on an unstarted foreign successor instead of resolving or giving up"
        );
        assert_eq!(
            wait_state(&orch).await,
            "resolving",
            "wait stays resolving (durable ownership) while the run has not resumed"
        );
        assert!(wait_resolved_turns(&orch).await.is_empty());
        // The call itself IS answered: delivery is per-call and happens before
        // anything turn-shaped, so a resolver still waiting on a foreign
        // successor has already discharged its own obligation.
        assert_eq!(tool_result_count(&orch).await, 1);

        // After restart, the racing continuation has started the foreign turn; startup
        // reconciliation re-drives the still-`resolving` row and resolves it.
        orch.db
            .local
            .execute("UPDATE turns SET state='running' WHERE id='foreign'", ())
            .await
            .unwrap();
        resolve(&orch, &orch.db.local, &record, ELAPSED, true)
            .await
            .unwrap();

        assert!(wait_resolved_turns(&orch).await.is_empty());
        assert_eq!(turn_state_of(&orch, "foreign").await, "running");
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_state(&orch).await, "resolved");
    }

    // ---- Abandoned rows (CAIRN-3159) ---------------------------------------

    /// Rotate the job onto a fresh session, run, and turn, exactly as an
    /// inactivity reseed or a host restart does, leaving whatever wait row the
    /// previous turn established behind.
    async fn reconstruct_session(orch: &Orchestrator) {
        for sql in [
            "INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-2','job-1','open','handle-2',2,2)",
            "UPDATE sessions SET status='closed', replaced_by_id='session-2' WHERE id='session-1'",
            "INSERT INTO runs (id,issue_id,project_id,job_id,status,session_id,created_at,updated_at) VALUES ('run-2','i','p','job-1','live','session-2',2,2)",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('new-turn','session-2','run-2','job-1',1,'running','initial',2,2)",
            "UPDATE jobs SET current_session_id='session-2', current_turn_id='new-turn' WHERE id='job-1'",
        ] {
            orch.db.local.execute(sql, ()).await.unwrap();
        }
    }

    /// The suspension the reconstructed session's turn attempts.
    fn successor_record(base: &Record) -> Record {
        Record {
            id: "wait-2".into(),
            session_id: "session-2".into(),
            run_id: "run-2".into(),
            turn_id: "new-turn".into(),
            tool_use_id: "tool-2".into(),
            ..base.clone()
        }
    }

    /// state, resolution, and whether a result was delivered, for one wait row.
    async fn wait_row(orch: &Orchestrator, id: &str) -> (String, Option<String>, Option<i64>) {
        let id = id.to_string();
        orch.db
            .local
            .query_one(
                "SELECT state,resolution_json,result_stored_at FROM agent_waits WHERE id=?1",
                (id,),
                |r| Ok((r.text(0)?, r.opt_text(1)?, r.opt_i64(2)?)),
            )
            .await
            .unwrap()
    }

    /// Every delivered tool result as (turn_id, data).
    async fn tool_results(orch: &Orchestrator) -> Vec<(Option<String>, String)> {
        orch.db
            .local
            .read(|c| {
                Box::pin(async move {
                    let mut rows = c
                        .query(
                            "SELECT turn_id,data FROM events WHERE event_type='tool_result' ORDER BY sequence",
                            (),
                        )
                        .await?;
                    let mut v = Vec::new();
                    while let Some(r) = rows.next().await? {
                        v.push((r.opt_text(0)?, r.text(1)?));
                    }
                    Ok(v)
                })
            })
            .await
            .unwrap()
    }

    async fn active_wait_count(orch: &Orchestrator) -> i64 {
        orch.db
            .local
            .query_one(
                "SELECT COUNT(*) FROM agent_waits WHERE state IN ('pending','resolving')",
                (),
                |r| r.i64(0),
            )
            .await
            .unwrap()
    }

    /// Reported from the field: "the first waitFor suspends and resumes
    /// correctly; every later one fails". A wait that completed on the happy path
    /// still strands the job if its row never leaves the active set — a resolution
    /// that resumed the run but returned before its final state write leaves
    /// exactly that. The resumed turn's own next wait must not be refused by it.
    ///
    /// This is the same supersession as a session reconstruction, and for the same
    /// reason: the row belongs to a turn the job has already left. Nothing about
    /// the fix depends on *why* the row was left behind.
    #[tokio::test]
    async fn a_completed_wait_whose_row_stayed_active_does_not_block_the_next_one() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);
        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();
        let successor = wait_resolved_turns(&orch).await[0].0.clone();
        // The wait resolved and the run resumed — but the row stayed active.
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='resolving' WHERE id='wait-1'",
                (),
            )
            .await
            .unwrap();

        // The resumed turn issues its own wait.
        let next = Record {
            id: "wait-2".into(),
            turn_id: successor,
            tool_use_id: "tool-2".into(),
            ..record.clone()
        };
        suspend(&orch, &orch.db.local, &next).await.unwrap();

        assert_eq!(wait_row(&orch, "wait-2").await.0, "pending");
        assert_eq!(active_wait_count(&orch).await, 1);
        // The first wait's call keeps the answer it already got; superseding a
        // completed row must not answer it a second time.
        let results = tool_results(&orch).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].1.contains("elapsed"));
    }

    /// The load-bearing case: a wait was in flight when the session was
    /// reconstructed, so its row was left behind pending. The reconstructed
    /// session's very next suspension must succeed, and the abandoned row must
    /// be closed with a typed outcome delivered to the call it belonged to.
    #[tokio::test]
    async fn suspension_after_session_reconstruction_supersedes_the_abandoned_row() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        reconstruct_session(&orch).await;
        register_warm_for(&orch, "run-2", "session-2");

        suspend(&orch, &orch.db.local, &successor_record(&record))
            .await
            .unwrap();

        // The new suspension is the job's one live wait.
        assert_eq!(wait_row(&orch, "wait-2").await.0, "pending");
        assert_eq!(active_wait_count(&orch).await, 1);

        // The abandoned one is closed with a typed outcome...
        let (state, resolution, delivered) = wait_row(&orch, "wait-1").await;
        assert_eq!(state, "cancelled");
        assert!(
            resolution
                .as_deref()
                .unwrap_or_default()
                .contains("abandoned"),
            "unexpected abandonment resolution: {resolution:?}"
        );
        assert!(delivered.is_some(), "the abandoned call was answered");

        // ...delivered to its own turn, so its tool use does not dangle.
        let results = tool_results(&orch).await;
        assert_eq!(results.len(), 1, "exactly one delivered result");
        assert_eq!(results[0].0.as_deref(), Some("pred-turn"));
        assert!(results[0].1.contains("tool-1") && results[0].1.contains("abandoned"));

        // Superseding answers the abandoned call; it never resumes the job.
        assert!(wait_resolved_turns(&orch).await.is_empty());
    }

    /// The same for a suspended `run` batch, whose row carries no pollable
    /// condition and so can only ever be closed by something else.
    #[tokio::test]
    async fn abandoned_run_batch_row_does_not_block_the_reconstructed_session() {
        let (orch, base, _) = durable_env().await;
        insert(&orch.db.local, &run_batch_record(&base, true))
            .await
            .unwrap();
        reconstruct_session(&orch).await;
        register_warm_for(&orch, "run-2", "session-2");

        suspend(&orch, &orch.db.local, &successor_record(&base))
            .await
            .unwrap();

        assert_eq!(wait_row(&orch, "wait-1").await.0, "cancelled");
        assert_eq!(wait_row(&orch, "wait-2").await.0, "pending");
        assert_eq!(active_wait_count(&orch).await, 1);
    }

    /// Parking changes what the node IS -- self-suspended on its own work rather
    /// than working -- and nothing else says so: the wait row and the yield land
    /// inside one write, which announces nothing, and clients cache turn state
    /// until something invalidates it. The transcript is where that silence
    /// showed: a parked call has no verdict to draw and, told nothing, no turn it
    /// can call running either, so a live batch rendered as neither (CAIRN-3340).
    #[tokio::test]
    async fn parking_a_turn_announces_the_yield() {
        let (orch, record, emitter) = durable_env_capturing().await;
        register_warm(&orch);

        suspend(&orch, &orch.db.local, &run_batch_record(&record, false))
            .await
            .unwrap();

        assert_eq!(turn_state_of(&orch, "pred-turn").await, "yielded");
        let announced = turn_changes(&emitter);
        assert_eq!(
            announced.len(),
            1,
            "one park, one announcement: {announced:?}"
        );
        assert_eq!(announced[0]["action"], "update");
        // Clients invalidate by scope, so the job whose turn parked must ride along.
        assert_eq!(announced[0]["jobId"], "job-1");
    }

    /// A turn parks once however many of its calls suspend (CAIRN-2823), so the
    /// sibling that finds it already yielded has no state change to announce.
    #[tokio::test]
    async fn a_sibling_park_announces_nothing_new() {
        let (orch, record, emitter) = durable_env_capturing().await;
        register_warm(&orch);
        suspend(&orch, &orch.db.local, &record).await.unwrap();

        suspend(&orch, &orch.db.local, &sibling_record(&record))
            .await
            .unwrap();

        let announced = turn_changes(&emitter);
        assert_eq!(
            announced.len(),
            1,
            "one yield, one announcement: {announced:?}"
        );
    }

    // ---- Concurrent calls in one turn (CAIRN-3232) --------------------------

    /// A provider routinely emits several tool calls in one assistant event, and
    /// two of them can both outlive their grace window. Both must park: the
    /// bound is one active row per CALL, not per turn.
    #[tokio::test]
    async fn a_second_call_in_the_same_turn_parks_alongside_the_first() {
        let (orch, record, _) = durable_env().await;
        register_warm(&orch);
        suspend(&orch, &orch.db.local, &record).await.unwrap();

        suspend(&orch, &orch.db.local, &sibling_record(&record))
            .await
            .unwrap();

        assert_eq!(wait_row(&orch, "wait-1").await.0, "pending");
        assert_eq!(wait_row(&orch, "wait-2").await.0, "pending");
        assert_eq!(active_wait_count(&orch).await, 2);
        // Parking is not answering: neither call has been told anything yet.
        assert!(tool_results(&orch).await.is_empty());
    }

    /// What the per-call bound still refuses: one CALL parked twice. Nothing
    /// distinguishes the two rows, so the second would answer a call the first
    /// already owns. The refusal names the next step, never the constraint.
    #[tokio::test]
    async fn one_call_still_cannot_park_itself_twice() {
        let (orch, record, _) = durable_env().await;
        register_warm(&orch);
        suspend(&orch, &orch.db.local, &record).await.unwrap();

        let duplicate = Record {
            id: "wait-2".into(),
            ..record.clone()
        };
        // Matched rather than unwrapped: the success value is a park handoff, a
        // one-shot receiver that has no business implementing `Debug` just to
        // satisfy a test's error path.
        let error = match suspend(&orch, &orch.db.local, &duplicate).await {
            Ok(_) => panic!("one tool call must not be parked by two suspensions"),
            Err(error) => error,
        };

        assert_eq!(error, SUSPENSION_UNAVAILABLE);
        assert!(
            !error.contains("UNIQUE") && !error.contains("agent_waits"),
            "refusal leaks storage internals: {error}"
        );
        assert_eq!(wait_state(&orch).await, "pending");
        assert_eq!(active_wait_count(&orch).await, 1);
    }

    /// The per-call bound is only a real bound while every row names a real
    /// call, so a suspension with nothing bound is refused before it can insert
    /// one that names nothing.
    #[tokio::test]
    async fn a_suspension_with_no_bound_call_is_refused() {
        let (orch, record, _) = durable_env().await;
        register_warm(&orch);
        let unbound = Record {
            tool_use_id: String::new(),
            ..record.clone()
        };

        let error = match suspend(&orch, &orch.db.local, &unbound).await {
            Ok(_) => panic!("a suspension with no call to answer must not be established"),
            Err(error) => error,
        };

        assert_eq!(error, SUSPENSION_UNAVAILABLE);
        assert_eq!(active_wait_count(&orch).await, 0);
    }

    /// A turn that has already parked one call reports no live turn, because
    /// parking idles the process. A sibling call crossing its own grace window a
    /// moment later must still find the turn it came from, or it loses its
    /// suspension to nothing more than scheduling jitter.
    #[tokio::test]
    async fn a_parked_turn_is_still_findable_by_its_sibling_call() {
        let (orch, record, _) = durable_env().await;
        register_warm(&orch);

        // Nothing live and nothing parked: there is no turn to hand out, and a
        // callback must never be given one it cannot have come from.
        assert_eq!(
            suspending_turn_id(&orch, &orch.db.local, "run-1").await,
            None
        );

        // A live turn answers for itself.
        orch.process_state.begin_turn("run-1", "pred-turn");
        assert_eq!(
            suspending_turn_id(&orch, &orch.db.local, "run-1")
                .await
                .as_deref(),
            Some("pred-turn")
        );

        // Once one of its calls is parked the process reports no live turn — and
        // the turn is still findable through the row that call parked.
        insert(&orch.db.local, &record).await.unwrap();
        orch.process_state.transition_to_warm("run-1");
        assert_eq!(orch.process_state.get_current_turn_id("run-1"), None);
        assert_eq!(
            suspending_turn_id(&orch, &orch.db.local, "run-1")
                .await
                .as_deref(),
            Some("pred-turn")
        );
    }

    /// The whole point: two parked calls, each answered on its own provider id,
    /// and exactly ONE resume of the turn they share.
    #[tokio::test]
    async fn two_parked_calls_in_one_turn_resume_it_once() {
        let (orch, first, _) = durable_env().await;
        let second = sibling_record(&first);
        register_warm(&orch);
        insert(&orch.db.local, &first).await.unwrap();
        insert(&orch.db.local, &second).await.unwrap();

        resolve(&orch, &orch.db.local, &first, ELAPSED, false)
            .await
            .unwrap();
        resolve(&orch, &orch.db.local, &second, EXITED, false)
            .await
            .unwrap();

        assert_eq!(
            wait_resolved_turns(&orch).await.len(),
            1,
            "a turn resumes once however many of its calls parked"
        );
        let results = tool_results(&orch).await;
        assert_eq!(
            results.len(),
            2,
            "each parked call is answered on its own id"
        );
        assert!(results.iter().any(|(_, data)| data.contains("tool-1")));
        assert!(results.iter().any(|(_, data)| data.contains("tool-2")));
        assert_eq!(wait_row(&orch, "wait-1").await.0, "resolved");
        assert_eq!(wait_row(&orch, "wait-2").await.0, "resolved");
        assert_eq!(active_wait_count(&orch).await, 0);
    }

    /// The turn resumes when the LAST of its calls settles, so the first one to
    /// finish answers its own call and stops there. Resuming earlier would wake
    /// the agent with one of its calls still unanswered.
    #[tokio::test]
    async fn the_first_call_to_settle_stands_down_without_a_successor() {
        let (orch, first, _) = durable_env().await;
        let second = sibling_record(&first);
        register_warm(&orch);
        insert(&orch.db.local, &first).await.unwrap();
        insert(&orch.db.local, &second).await.unwrap();

        resolve(&orch, &orch.db.local, &first, ELAPSED, false)
            .await
            .unwrap();

        // It is finished: answered, closed, and owning no successor.
        assert_eq!(wait_row(&orch, "wait-1").await.0, "resolved");
        assert_eq!(tool_result_count(&orch).await, 1);
        let successor: Option<String> = orch
            .db
            .local
            .query_one(
                "SELECT successor_turn_id FROM agent_waits WHERE id='wait-1'",
                (),
                |r| r.opt_text(0),
            )
            .await
            .unwrap();
        assert_eq!(successor, None);
        // And the turn is still parked, waiting on its other call.
        assert!(wait_resolved_turns(&orch).await.is_empty());
        assert_eq!(orch.process_state.get_current_turn_id("run-1"), None);
        assert_eq!(wait_row(&orch, "wait-2").await.0, "pending");
    }

    /// A host that died holding the driver row re-drives only that row: the
    /// sibling that stood down is already out of the active set, and re-driving
    /// it would answer its call twice.
    #[tokio::test]
    async fn a_crash_after_a_sibling_stood_down_resumes_from_the_driver_row() {
        let (orch, first, _) = durable_env().await;
        let second = sibling_record(&first);
        register_warm(&orch);
        insert(&orch.db.local, &first).await.unwrap();
        insert(&orch.db.local, &second).await.unwrap();
        resolve(&orch, &orch.db.local, &first, ELAPSED, false)
            .await
            .unwrap();
        // The driver claimed its row and recorded its result, then the host died.
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='resolving',resolution_json=?1 WHERE id='wait-2'",
                (EXITED,),
            )
            .await
            .unwrap();

        reconcile(&orch).await;
        for _ in 0..200 {
            if wait_row(&orch, "wait-2").await.0 == "resolved" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        assert_eq!(wait_row(&orch, "wait-2").await.0, "resolved");
        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
        assert_eq!(
            tool_result_count(&orch).await,
            2,
            "each call keeps exactly one answer across the restart"
        );
    }

    /// Superseding a turn the job has left answers and closes EVERY call it
    /// parked, not just the first one found. A row left behind is what makes
    /// waiting single-use for the rest of the job's life.
    #[tokio::test]
    async fn superseding_a_turn_abandons_every_one_of_its_parked_calls() {
        let (orch, first, _) = durable_env().await;
        insert(&orch.db.local, &first).await.unwrap();
        insert(&orch.db.local, &sibling_record(&first))
            .await
            .unwrap();
        reconstruct_session(&orch).await;
        register_warm_for(&orch, "run-2", "session-2");

        // Its own row and call, distinct from both of the abandoned ones.
        let successor = Record {
            id: "wait-3".into(),
            tool_use_id: "tool-3".into(),
            ..successor_record(&first)
        };
        suspend(&orch, &orch.db.local, &successor).await.unwrap();

        for id in ["wait-1", "wait-2"] {
            let (state, resolution, delivered) = wait_row(&orch, id).await;
            assert_eq!(state, "cancelled", "{id} was left active");
            assert!(resolution
                .as_deref()
                .unwrap_or_default()
                .contains(ABANDONED_OUTCOME));
            assert!(delivered.is_some(), "{id}'s call was never answered");
        }
        assert_eq!(tool_results(&orch).await.len(), 2);
        // The reconstructed turn's own suspension is now the job's only live one.
        assert_eq!(active_wait_count(&orch).await, 1);
        assert!(wait_resolved_turns(&orch).await.is_empty());
    }

    fn parked(condition: Condition, tool_use_id: &str, resolution: Option<&str>) -> ParkedCall {
        ParkedCall {
            condition,
            tool_use_id: tool_use_id.into(),
            resolution: resolution.map(str::to_string),
        }
    }

    /// The bytes of a lone suspension's resume prompt are the contract: every
    /// `run` batch and `waitFor` that suspends today reads its own result and
    /// nothing else, so composing must not put a single word in front of it.
    #[test]
    fn a_single_parked_call_resumes_with_its_result_verbatim() {
        let calls = vec![parked(Condition::Duration, "tool-1", Some(ELAPSED))];
        assert_eq!(compose_resume_prompt(&calls, ELAPSED), ELAPSED);
        // The same holds for a driver whose turn's rows could not be read at all.
        assert_eq!(compose_resume_prompt(&[], ELAPSED), ELAPSED);
    }

    /// With several, each result is named by the call it answers -- an unlabeled
    /// concatenation would ask the model to guess.
    #[test]
    fn the_resume_prompt_carries_every_parked_call_labeled() {
        let calls = vec![
            parked(
                run_batch_condition(false, Some("bun run test:rust")),
                "toolu-tests",
                Some("suite green"),
            ),
            parked(
                Condition::Terminal {
                    uri: "cairn://p/PRJ/1/1/builder/terminal/dev".into(),
                    slug: "dev".into(),
                    on: TerminalWaitEvent::Output,
                    phrase: Some("ready".into()),
                },
                "toolu-dev",
                Some("dev is ready"),
            ),
        ];

        let prompt = compose_resume_prompt(&calls, "suite green");

        for expected in [
            "2 calls",
            "run: bun run test:rust",
            "toolu-tests",
            "suite green",
            "waitFor: terminal dev output",
            "toolu-dev",
            "dev is ready",
        ] {
            assert!(
                prompt.contains(expected),
                "missing {expected} in:\n{prompt}"
            );
        }
    }

    /// A row with no recorded result answers nothing, so it must not become a
    /// section that says nothing -- and the one real answer stays verbatim.
    #[test]
    fn a_call_with_no_recorded_result_is_left_out_of_the_prompt() {
        let calls = vec![
            parked(Condition::Duration, "tool-1", Some(ELAPSED)),
            parked(Condition::Duration, "tool-2", None),
        ];
        assert_eq!(compose_resume_prompt(&calls, ELAPSED), ELAPSED);
    }

    /// The driver builds its prompt from its TURN's rows rather than its own, so
    /// a sibling that already stood down still reaches the resumed agent.
    #[tokio::test]
    async fn the_driver_carries_a_stood_down_siblings_result_too() {
        let (orch, first, _) = durable_env().await;
        let second = sibling_record(&first);
        register_warm(&orch);
        insert(&orch.db.local, &first).await.unwrap();
        insert(&orch.db.local, &second).await.unwrap();
        resolve(&orch, &orch.db.local, &first, ELAPSED, false)
            .await
            .unwrap();
        // The state the driver holds when it composes: claimed, resolution
        // recorded, election won.
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='resolving',resolution_json=?1 WHERE id='wait-2'",
                (EXITED,),
            )
            .await
            .unwrap();

        let prompt = resume_prompt_for_turn(&orch.db.local, &second, EXITED).await;

        for expected in [ELAPSED, EXITED, "tool-1", "tool-2"] {
            assert!(
                prompt.contains(expected),
                "missing {expected} in:\n{prompt}"
            );
        }
    }

    /// A row whose result a resolver already delivered is still cleared out of
    /// the pending set, but is not answered a second time.
    #[tokio::test]
    async fn an_already_answered_abandoned_row_is_cleared_without_a_second_result() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='resolving' WHERE id='wait-1'",
                (),
            )
            .await
            .unwrap();
        // The resolver answered through the real delivery path, not a
        // hand-placed marker.
        store_result_once(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();
        reconstruct_session(&orch).await;
        register_warm_for(&orch, "run-2", "session-2");

        suspend(&orch, &orch.db.local, &successor_record(&record))
            .await
            .unwrap();

        assert_eq!(wait_row(&orch, "wait-1").await.0, "cancelled");
        assert_eq!(active_wait_count(&orch).await, 1);
        let results = tool_results(&orch).await;
        assert_eq!(
            results.len(),
            1,
            "an already-answered call must not be answered twice"
        );
        assert!(results[0].1.contains("elapsed"));
    }

    /// The interleaving the claim exists for: a live resolver has won the right
    /// to answer its call but has not appended the event yet, and a supersession
    /// runs entirely inside that window. Driving the resolver's two halves
    /// separately is what makes that window observable — a read-then-append
    /// resolver would let the supersession answer behind it, leaving one tool use
    /// with two answers.
    #[tokio::test]
    async fn supersession_cannot_answer_a_call_a_resolver_has_already_claimed() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        assert!(
            claim_result_delivery(&orch.db.local, "wait-1")
                .await
                .unwrap(),
            "the resolver wins the claim first"
        );
        reconstruct_session(&orch).await;
        register_warm_for(&orch, "run-2", "session-2");

        // The whole supersession runs while the resolver is paused mid-delivery.
        suspend(&orch, &orch.db.local, &successor_record(&record))
            .await
            .unwrap();

        assert!(
            tool_results(&orch).await.is_empty(),
            "supersession must not answer a call another writer claimed"
        );
        // The row still leaves the pending set, so the new suspension stands.
        assert_eq!(wait_row(&orch, "wait-1").await.0, "cancelled");
        assert_eq!(active_wait_count(&orch).await, 1);

        // The resolver's own append then lands, and is the only answer.
        append_claimed_result(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();
        let results = tool_results(&orch).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].1.contains("elapsed"));
    }

    /// The same exclusion from the other side: once a supersession has answered
    /// an abandoned call, the resolver that wakes afterwards stands down.
    #[tokio::test]
    async fn a_resolver_cannot_answer_a_call_supersession_already_answered() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        reconstruct_session(&orch).await;
        register_warm_for(&orch, "run-2", "session-2");
        suspend(&orch, &orch.db.local, &successor_record(&record))
            .await
            .unwrap();
        assert_eq!(tool_results(&orch).await.len(), 1);

        store_result_once(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();

        let results = tool_results(&orch).await;
        assert_eq!(results.len(), 1, "one tool use is answered exactly once");
        assert!(results[0].1.contains("abandoned"));
    }

    /// A host that dies between committing a delivery claim and appending the
    /// event leaves the claim held with nothing behind it. Startup must not read
    /// that as an answer, or the call is never answered at all: the claim is a
    /// lock, and the transcript is the fact it locks over.
    #[tokio::test]
    async fn a_claim_interrupted_by_a_restart_still_answers_its_call() {
        let (orch, base, _) = durable_env().await;
        register_warm(&orch);
        let record = run_batch_record(&base, true);
        insert(&orch.db.local, &record).await.unwrap();
        // The pre-restart host claimed delivery and died before appending.
        assert!(claim_result_delivery(&orch.db.local, "wait-1")
            .await
            .unwrap());

        reconcile(&orch).await;

        for _ in 0..200 {
            if wait_state(&orch).await == "resolved" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(wait_state(&orch).await, "resolved");
        let results = tool_results(&orch).await;
        assert_eq!(
            results.len(),
            1,
            "the interrupted claim must not swallow the answer"
        );
        assert!(results[0].1.contains("restarted"));
    }

    /// Why the reclaim is load-bearing rather than decorative: a held claim, on
    /// its own, does block delivery — which is the correct behavior against a
    /// live writer and the wrong one against a dead one. This pins both halves.
    #[tokio::test]
    async fn a_held_claim_blocks_delivery_until_it_is_reclaimed() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        assert!(claim_result_delivery(&orch.db.local, "wait-1")
            .await
            .unwrap());

        // A re-drive that trusted the claim alone declines to answer.
        store_result_once(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();
        assert!(
            tool_results(&orch).await.is_empty(),
            "a held claim blocks delivery, which is why a dead writer's claim must be reclaimed"
        );

        // Reclaiming it is exactly what lets the answer land.
        reclaim_interrupted_delivery(&orch.db.local, &SuspendedCall::of(&record)).await;
        store_result_once(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();
        assert_eq!(tool_results(&orch).await.len(), 1);
    }

    /// Put a row in the state a resolver holds while it works: claimed, and
    /// carrying the resolution it read. `successor_turn_id` carries a foreign
    /// key, so the turns a claim can name have to exist.
    async fn resolver_in_flight(orch: &Orchestrator) {
        for (id, sequence) in [("successor-turn", 2), ("other-turn", 3)] {
            orch.db
                .local
                .execute(
                    "INSERT INTO turns (id,session_id,run_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at) VALUES (?1,'session-1','run-1','job-1',?2,'pred-turn','pending','wait_resolved',2,2)",
                    (id, sequence),
                )
                .await
                .unwrap();
        }
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='resolving',resolution_json=?1 WHERE id='wait-1'",
                (ELAPSED,),
            )
            .await
            .unwrap();
    }

    /// Resuming and abandoning are two writes to one row, and exactly one wins.
    /// Both orderings are driven here by invoking the two operations directly:
    /// no timing, no sleeps, just the two linearization points in each order.
    #[tokio::test]
    async fn abandonment_wins_when_it_commits_before_the_continuation_is_owned() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        resolver_in_flight(&orch).await;

        assert!(
            mark_abandoned(&orch.db.local, "wait-1").await.unwrap(),
            "abandonment wins an unowned row"
        );
        assert!(
            !claim_continuation(&orch.db.local, "wait-1", "successor-turn")
                .await
                .unwrap(),
            "the resolver must lose the resume once the row is abandoned"
        );
        assert_eq!(
            wait_row(&orch, "wait-1").await.1.as_deref(),
            Some(abandoned_result().as_str())
        );
    }

    #[tokio::test]
    async fn the_resolver_wins_when_it_owns_the_continuation_first() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        resolver_in_flight(&orch).await;

        assert!(
            claim_continuation(&orch.db.local, "wait-1", "successor-turn")
                .await
                .unwrap(),
            "the resolver wins an unabandoned row"
        );
        assert!(
            !mark_abandoned(&orch.db.local, "wait-1").await.unwrap(),
            "abandonment must lose once the resume is owned"
        );
        // The resolver's own resolution is untouched, so it resumes as planned.
        let (_, resolution, _) = wait_row(&orch, "wait-1").await;
        assert_eq!(resolution.as_deref(), Some(ELAPSED));
    }

    /// Ownership is re-entrant for the replay that re-drives its own recorded
    /// successor — crash recovery depends on re-entering this path.
    #[tokio::test]
    async fn owning_a_continuation_is_re_entrant_for_its_own_successor() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        resolver_in_flight(&orch).await;

        assert!(
            claim_continuation(&orch.db.local, "wait-1", "successor-turn")
                .await
                .unwrap()
        );
        assert!(
            claim_continuation(&orch.db.local, "wait-1", "successor-turn")
                .await
                .unwrap(),
            "a replay re-drives the successor it already owns"
        );
        assert!(
            !claim_continuation(&orch.db.local, "wait-1", "other-turn")
                .await
                .unwrap(),
            "but never a foreign successor"
        );
    }

    /// Continuation ownership, not just delivery: a resolver already past its own
    /// state read holds a stale local result and will drive `continue_job_impl`
    /// unless something fences it. Recording the abandonment while it works is
    /// that fence — it must stand down rather than resume a run the reconstructed
    /// turn has taken over.
    #[tokio::test]
    async fn a_resolver_overtaken_by_abandonment_stands_down_without_resuming() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);
        // The resolver claimed the row and read its own resolution — the state it
        // holds while it works.
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='resolving',resolution_json=?1 WHERE id='wait-1'",
                (ELAPSED,),
            )
            .await
            .unwrap();
        // A reconstructed turn abandons that suspension inside the window.
        assert!(mark_abandoned(&orch.db.local, "wait-1").await.unwrap());

        // The resolver now proceeds with the result it read before the change.
        resolve(&orch, &orch.db.local, &record, ELAPSED, true)
            .await
            .unwrap();

        assert!(
            wait_resolved_turns(&orch).await.is_empty(),
            "an overtaken resolver must not create a successor"
        );
        assert_eq!(
            orch.process_state.get_current_turn_id("run-1"),
            None,
            "an overtaken resolver must not resume the run"
        );
        assert_eq!(
            turn_state_of(&orch, "pred-turn").await,
            "yielded",
            "the abandoned turn is left where it was"
        );
    }

    /// The fence is the decision, not the mere passage of time: an ordinary
    /// resolution still resolves and still resumes.
    #[tokio::test]
    async fn an_unovertaken_resolver_still_resumes_normally() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();

        let successors = wait_resolved_turns(&orch).await;
        assert_eq!(successors.len(), 1);
        assert_eq!(successors[0].1, "running");
        assert_eq!(wait_state(&orch).await, "resolved");
    }

    /// A host that died after answering an abandoned call but before closing its
    /// row must not have that row re-driven as an ordinary wait: its turn is
    /// gone, so a resume would resurrect the suspension being discarded.
    #[tokio::test]
    async fn a_crash_between_answering_and_closing_an_abandonment_never_resumes_the_run() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);
        let call = SuspendedCall::of(&record);
        // The pre-crash host recorded the decision and answered the call, then
        // died before closing the row.
        assert!(mark_abandoned(&orch.db.local, "wait-1").await.unwrap());
        deliver_abandonment(&orch, &orch.db.local, &call).await;
        assert_eq!(tool_results(&orch).await.len(), 1);

        reconcile(&orch).await;
        for _ in 0..200 {
            if wait_state(&orch).await == "cancelled" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        assert_eq!(wait_state(&orch).await, "cancelled", "the stale row closes");
        assert!(
            wait_resolved_turns(&orch).await.is_empty(),
            "an abandoned suspension is never resumed"
        );
        assert_eq!(
            tool_results(&orch).await.len(),
            1,
            "its call stays answered exactly once"
        );
        assert_eq!(
            turn_state_of(&orch, "pred-turn").await,
            "yielded",
            "the abandoned turn is left where it was"
        );
    }

    /// The earlier window: the decision was recorded but the call was never
    /// answered. Startup finishes the abandonment — answers, then closes — still
    /// without resuming.
    #[tokio::test]
    async fn a_crash_after_recording_an_abandonment_answers_and_closes_without_resuming() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);
        assert!(mark_abandoned(&orch.db.local, "wait-1").await.unwrap());

        reconcile(&orch).await;
        for _ in 0..200 {
            if wait_state(&orch).await == "cancelled" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        assert_eq!(wait_state(&orch).await, "cancelled");
        assert!(wait_resolved_turns(&orch).await.is_empty());
        let results = tool_results(&orch).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].1.contains(ABANDONED_OUTCOME));
    }

    /// The converse: a claim with its result already in the transcript is a
    /// completed delivery, and reclaiming must leave it alone.
    #[tokio::test]
    async fn a_delivered_result_is_never_reclaimed_or_answered_twice() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        store_result_once(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();
        let claimed_at = wait_row(&orch, "wait-1").await.2;
        assert!(claimed_at.is_some());

        reclaim_interrupted_delivery(&orch.db.local, &SuspendedCall::of(&record)).await;

        assert_eq!(
            wait_row(&orch, "wait-1").await.2,
            claimed_at,
            "a completed delivery keeps its claim"
        );
        store_result_once(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();
        assert_eq!(tool_results(&orch).await.len(), 1);
    }

    /// A delivery whose append fails holds no claim afterwards, so the answer is
    /// still owed rather than silently swallowed.
    #[tokio::test]
    async fn a_failed_append_gives_back_its_claim() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        // A record whose run does not exist cannot have an event stored against
        // it, so the append fails after the claim is taken.
        let unstorable = Record {
            run_id: "missing-run".into(),
            ..record.clone()
        };
        assert!(claim_result_delivery(&orch.db.local, "wait-1")
            .await
            .unwrap());
        assert!(
            append_claimed_result(&orch, &orch.db.local, &unstorable, ELAPSED)
                .await
                .is_err()
        );

        assert_eq!(
            wait_row(&orch, "wait-1").await.2,
            None,
            "a claim is held only by a delivery that landed"
        );
        // So the real delivery can still answer the call.
        store_result_once(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();
        assert_eq!(tool_results(&orch).await.len(), 1);
    }

    /// Upgrade path: a job stranded TODAY carries a row a pre-fix host left
    /// behind -- its turn finished, its session closed, and no resolver survives
    /// to claim it. It recovers on the next suspension, with no surgery.
    #[tokio::test]
    async fn a_job_stranded_by_a_pre_fix_row_recovers_on_its_next_suspension() {
        let (orch, record, _) = durable_env().await;
        orch.db
            .local
            .execute(
                "INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at) VALUES('stranded','job-1','run-1','session-1','pred-turn','tool-0','{\"kind\":\"duration\"}','pending',1)",
                (),
            )
            .await
            .unwrap();
        orch.db
            .local
            .execute(
                "UPDATE turns SET state='completed' WHERE id='pred-turn'",
                (),
            )
            .await
            .unwrap();
        reconstruct_session(&orch).await;
        register_warm_for(&orch, "run-2", "session-2");

        suspend(&orch, &orch.db.local, &successor_record(&record))
            .await
            .unwrap();

        assert_eq!(wait_row(&orch, "stranded").await.0, "cancelled");
        assert_eq!(wait_row(&orch, "wait-2").await.0, "pending");
        assert_eq!(active_wait_count(&orch).await, 1);
    }

    #[tokio::test]
    async fn self_suspend_preserves_pending_wait_while_user_stop_cancels() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        // A durable self-suspend must NOT cancel its own wait row.
        crate::orchestrator::lifecycle::suspend_run_for_durable_wait(
            &orch,
            "run-1",
            "owned_wait_suspended",
        )
        .unwrap();
        assert_eq!(
            wait_state(&orch).await,
            "pending",
            "self-suspend must preserve the pending wait"
        );
        assert!(
            orch.process_state
                .processes
                .lock()
                .unwrap()
                .get("run-1")
                .is_some(),
            "self-suspend keeps the process warm for resume"
        );

        // An external stop DOES cancel the wait.
        crate::orchestrator::lifecycle::stop_active_turn_for_run(&orch, "run-1", true);
        assert_eq!(
            wait_state(&orch).await,
            "cancelled",
            "external stop cancels the wait"
        );
    }
}
