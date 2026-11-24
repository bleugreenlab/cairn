//! Stopping and killing sessions: turn interruption, cascade stop, hard kill,
//! and durable-wait suspension. Sliced verbatim from the former `lifecycle.rs`.

use crate::agent_process::process::{GoalPause, GoalPauseOwner, PauseIntent, ProcessTurnOwner};
use crate::models::{RunStatus, TurnEndReason, TurnState};
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, DbResult, RowExt};
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::broadcast;

use super::common::*;
use super::unanswered_calls::{settle_unanswered_calls, InterruptCause};

pub fn stop_active_turn_for_run(orch: &Orchestrator, run_id: &str, cancel_owned_waits: bool) {
    // A durable self-suspend already yielded on its own wait; only external
    // Stop cancels that wait and relabels its turn as user_stop.
    if !cancel_owned_waits {
        return;
    }
    let job_id = job_id_for_run(orch, run_id);
    let launch_lock = job_id.as_deref().map(|id| orch.job_launch_lock(id));
    let (identity, owner, turn_id) = {
        let _guard = launch_lock
            .as_ref()
            .map(|lock| lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
        let identity = orch.process_state.capture_process_identity(run_id);
        let owner = identity
            .as_ref()
            .and_then(|identity| orch.process_state.capture_turn_owner_for_identity(identity));
        if orch.has_terminal_completion_claim(run_id) {
            return;
        }
        let turn_id = owner
            .as_ref()
            .map(|owner| owner.turn_id().to_string())
            .or_else(|| active_turn_id_for_run(orch, run_id));
        (identity, owner, turn_id)
    };
    cancel_owned_waits_for_run(orch, run_id);
    let Some(turn_id) = turn_id else {
        return;
    };
    if let Err(error) =
        settle_live_turn_calls(orch, Some(run_id), &turn_id, TurnSettlement::UserStop)
    {
        log::warn!("Failed to answer stopped turn {turn_id} for run {run_id}: {error}");
    }
    let guard = launch_lock
        .as_ref()
        .map(|lock| lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
    if orch.has_terminal_completion_claim(run_id) {
        return;
    }
    if identity.as_ref().is_some_and(|identity| {
        !orch.process_state.is_process_identity_current(identity)
            || owner.as_ref().map_or_else(
                || {
                    !matches!(
                        run_status(orch, run_id).as_deref(),
                        Some("starting" | "live")
                    )
                },
                |owner| !orch.process_state.is_turn_owner_current(owner),
            )
    }) {
        return;
    }
    let change = settle_live_turn_outcome(orch, &turn_id, TurnSettlement::UserStop);
    drop(guard);
    match change {
        Ok(Some(change)) => {
            let _ = orch.services.emitter.emit("db-change", change);
        }
        Ok(None) => {}
        Err(error) => log::warn!("Failed to stop turn {turn_id} for run {run_id}: {error}"),
    }
}

fn cancel_owned_waits_for_run(orch: &Orchestrator, run_id: &str) {
    orch.write_stops.raise(run_id);
    let cancelled = run_db_blocking({
        let dbs = orch.db.clone();
        let run_id = run_id.to_string();
        move || async move {
            let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|error| error.to_string())?;
            crate::mcp::handlers::durable_suspend::cancel_run_waits_for_stop(&db, &run_id).await
        }
    });
    match cancelled {
        // A batch that can disturb the branch still holds it through the
        // detached row the cancel wrote; any other hold the cancelled waits
        // made is gone, so serve the replay request it deferred, if any.
        Ok(()) => crate::orchestrator::base_advance::on_branch_hold_released_for_run(orch, run_id),
        Err(error) => log::warn!("Failed to cancel owned wait for run {run_id}: {error}"),
    }
}

/// Who is ending a live turn, which decides what it ends as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TurnSettlement {
    /// A warm Stop: a running turn is interrupted (resumable), anything else
    /// cancelled, both `user_stop`. Best effort, as Stop always was.
    UserStop,
    /// A cold operator Compact: every live turn is cancelled `operator_compact`,
    /// and any failure is returned rather than logged past.
    OperatorCompact,
}

/// End one live turn: answer its still-pending run and write tool calls with
/// synthetic results, then terminalize it as `settlement` dictates. Reports
/// whether the turn was live (and is now settled).
pub(crate) fn settle_live_turn(
    orch: &Orchestrator,
    run_id: Option<&str>,
    turn_id: &str,
    settlement: TurnSettlement,
    locked_job_id: &str,
) -> Result<bool, String> {
    let job_id = run_db_blocking({
        let dbs = orch.db.clone();
        let turn_id = turn_id.to_string();
        move || async move {
            let db = crate::execution::routing::owning_db_for_turn(&dbs, &turn_id)
                .await
                .map_err(|error| error.to_string())?;
            db.query_opt_text("SELECT job_id FROM turns WHERE id=?1", (turn_id,))
                .await
                .map_err(|error| error.to_string())
        }
    })?;
    let launch_lock = job_id
        .as_deref()
        .filter(|id| *id != locked_job_id)
        .map(|id| orch.job_launch_lock(id));
    let guard = launch_lock
        .as_ref()
        .map(|lock| lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
    if orch.is_turn_terminal_claimed(turn_id) {
        return Err(format!(
            "Turn {turn_id} is finishing its terminal completion; retry Compact"
        ));
    }
    drop(guard);
    settle_live_turn_calls(orch, run_id, turn_id, settlement)?;
    let guard = launch_lock
        .as_ref()
        .map(|lock| lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
    if orch.is_turn_terminal_claimed(turn_id) {
        return Err(format!(
            "Turn {turn_id} is finishing its terminal completion; retry Compact"
        ));
    }
    let change = settle_live_turn_outcome(orch, turn_id, settlement)?;
    drop(guard);
    let Some(change) = change else {
        return Ok(false);
    };
    let _ = orch.services.emitter.emit("db-change", change);
    Ok(true)
}

fn settle_live_turn_calls(
    orch: &Orchestrator,
    run_id: Option<&str>,
    turn_id: &str,
    settlement: TurnSettlement,
) -> Result<(), String> {
    let state = turn_state(orch, turn_id).ok_or_else(|| format!("turn {turn_id} was not found"))?;
    if state == "running" {
        if let Some(run_id) = run_id {
            if let Err(error) =
                settle_unanswered_calls(orch, run_id, turn_id, InterruptCause::UserStop)
            {
                let error = format!("failed to settle pending run tool results for run {run_id} turn {turn_id}: {error}");
                match settlement {
                    TurnSettlement::UserStop => log::warn!("{error}"),
                    TurnSettlement::OperatorCompact => return Err(error),
                }
            }
        }
    }
    Ok(())
}

/// The durable transition, separated from notifications so an exact-owner
/// stop can retire accounting and release occupancy before publishing.
fn settle_live_turn_outcome(
    orch: &Orchestrator,
    turn_id: &str,
    settlement: TurnSettlement,
) -> Result<Option<serde_json::Value>, String> {
    let state = turn_state(orch, turn_id).ok_or_else(|| format!("turn {turn_id} was not found"))?;
    match (state.as_str(), settlement) {
        ("running", TurnSettlement::UserStop) => {
            settle_turn_interruption(orch, turn_id, Some(TurnEndReason::UserStop)).map(Some)
        }
        ("pending" | "yielded", TurnSettlement::UserStop) => settle_turn_outcome(
            orch,
            turn_id,
            TurnState::Cancelled,
            Some(TurnEndReason::UserStop),
        )
        .map(Some),
        ("running" | "pending" | "yielded", TurnSettlement::OperatorCompact) => {
            settle_turn_outcome(
                orch,
                turn_id,
                TurnState::Cancelled,
                Some(TurnEndReason::OperatorCompact),
            )
            .map(Some)
        }
        _ => Ok(None),
    }
}

fn running_terminals_for_job(orch: &Orchestrator, job_id: &str) -> Vec<(String, String)> {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, session_id
                         FROM job_terminals
                         WHERE job_id = ?1
                           AND status = 'running'",
                        (job_id.as_str(),),
                    )
                    .await?;
                let mut terminals = Vec::new();
                while let Some(row) = rows.next().await? {
                    terminals.push((row.text(0)?, row.text(1)?));
                }
                Ok(terminals)
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .unwrap_or_default()
}

/// Park a run for durable wait without tearing down its process.
///
/// Durable waits are not crashes. We interrupt the current turn,
/// preserve foreground inline commands, and leave the process warm so it can resume when
/// the awaited dependency resolves.
///
/// Call this directly only when no agent-visible tool result is racing the
/// interrupt (startup reconciliation, tests). Every suspension that answers a
/// pending tool call goes through [`suspend_run_for_durable_wait_after_handoff`]
/// instead -- see [`SUSPEND_HANDOFF_GRACE`] for why.
pub fn suspend_run_for_durable_wait(
    orch: &Orchestrator,
    run_id: &str,
    exit_reason: &str,
    owner: GoalPauseOwner,
) -> Result<(), String> {
    let _ = exit_reason;
    // Self-suspend: do NOT reap the run's inline commands and do NOT hard-kill
    // on interrupt failure. A durable wait is a warm park, not user Stop; the run
    // must remain resumable when the awaited dependency resolves.
    stop_session_internal(orch, run_id, StopReason::Park(owner))?;
    Ok(())
}

/// How long a Cairn-initiated suspension lets its own tool result reach the
/// agent before the interrupt that ends the turn.
///
/// The interrupt cancels whatever tool call is in flight. When it lands before
/// the suspension's own result has travelled back through the MCP transport, the
/// agent CLI fills in a result for the cancelled call in its stock rejection
/// wording -- "The user doesn't want to proceed with this tool use. The tool use
/// was rejected" -- and writes it into its own session transcript. That text,
/// not ours, is then what the agent reads for the rest of the session: a routine
/// pause wearing the user's face. Agents have acted on it, concluding the
/// operator vetoed them and saying so to the operator (CAIRN-3162).
///
/// The window only has to cover an in-process MCP response travelling back
/// through `cairn-cmd` into the CLI's transcript -- single-digit milliseconds
/// normally. It is sized well above that because losing the race is permanent
/// (the false text stays in context for the whole session) while overshooting
/// costs nothing: the turn is ending regardless, and the run is about to wait on
/// something far slower. A 75ms window measured a ~10% loss rate in practice.
pub const SUSPEND_HANDOFF_GRACE: std::time::Duration = std::time::Duration::from_millis(250);

/// One run's pending durable-wait park, shared by every call the run's current
/// turn is suspending at once.
struct ParkSlot {
    /// Bumped by each suspension. Only the timer holding the current generation
    /// parks; an earlier one stands down when a sibling supersedes it.
    generation: u64,
    /// Carries the single park's outcome to every suspension waiting on it,
    /// including those whose own timer stood down.
    done: broadcast::Sender<Result<(), String>>,
}

/// The debounce state behind [`suspend_owned_run_after_handoff_then`],
/// keyed by run.
///
/// It lives on the orchestrator so every clone shares it and it is scoped to one
/// host, exactly like the processes it parks. A park is in-memory control flow
/// over a live process; nothing here needs to survive a restart, because a
/// suspension whose park never happened is left for startup reconciliation.
#[derive(Default)]
pub struct ParkSlots(Mutex<HashMap<String, ParkSlot>>);

impl ParkSlots {
    /// Whether a suspension has armed a park for `run_id` that has not yet
    /// happened: the run's turn has yielded, but its process is still within
    /// the handoff grace.
    pub(crate) fn armed(&self, run_id: &str) -> bool {
        self.0.lock().unwrap().contains_key(run_id)
    }
}

/// Park a run for a durable wait once its pending tool call has had the handoff
/// grace to receive the suspension's own result.
///
/// This is the canonical way a Cairn-initiated suspension ends a turn. See
/// [`SUSPEND_HANDOFF_GRACE`] for what parking synchronously costs.
///
/// `turn_owner` is the exact turn the caller yielded, captured when it yielded.
/// The stake and the park belong to that turn alone: a missing, finished, or
/// replaced owner stakes nothing and parks nothing, so a late suspension cannot
/// pause or interrupt a successor.
pub fn suspend_run_for_durable_wait_after_handoff(
    orch: &Orchestrator,
    run_id: &str,
    exit_reason: &str,
    turn_owner: Option<ProcessTurnOwner>,
    owner: GoalPauseOwner,
) {
    let Some(turn_owner) = turn_owner else {
        log::info!("{exit_reason} on {run_id} has no yielded turn owner; nothing to park");
        return;
    };
    if !stake_owned_goal_pause(orch, run_id, &turn_owner, owner, exit_reason) {
        return;
    }
    suspend_owned_run_after_handoff_then(orch, run_id, exit_reason, Some(turn_owner), |_| async {});
}

/// Stake a durable-wait goal pause only while `turn_owner` still owns the run,
/// under the launch authority that admission and completion share. Never holds
/// the registry mutex across provider control.
fn stake_owned_goal_pause(
    orch: &Orchestrator,
    run_id: &str,
    turn_owner: &ProcessTurnOwner,
    owner: GoalPauseOwner,
    exit_reason: &str,
) -> bool {
    let job_id = job_id_for_run(orch, run_id);
    let launch_lock = job_id.as_deref().map(|id| orch.job_launch_lock(id));
    let _guard = launch_lock
        .as_ref()
        .map(|lock| lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
    if turn_owner.run_id() != run_id || !orch.process_state.is_turn_owner_current(turn_owner) {
        log::info!(
            "{exit_reason} on {run_id}: turn {} no longer owns the run; not staking its pause",
            turn_owner.turn_id()
        );
        return false;
    }
    if let Err(error) = crate::backends::stdin::pause_goal_for_durable_wait(
        &orch.process_state,
        run_id,
        PauseIntent::Stake(GoalPause::Owned(owner)),
    ) {
        log::warn!("Failed to pause goal for {exit_reason} on {run_id}: {error}");
        return false;
    }
    true
}

/// Park the exact turn the originating callback captured, then run a
/// continuation. Its goal pause, if any, was staked before publication.
///
/// The deferred interrupt reasserts the live pause without restaking: an
/// operator Stop or newer wait may have superseded it during the grace period.
///
/// `after_park` receives the park's outcome and runs once the predecessor turn
/// has actually been interrupted. Anything that can resume the run belongs
/// there: the park must be the last thing that happens to the predecessor, or an
/// already-satisfied condition resumes a run the park then re-suspends
/// (CAIRN-2970).
///
/// The park itself is coalesced per run rather than per suspension — see
/// [`arm_coalesced_park`] — so a turn suspending several of its calls at once is
/// interrupted once, after the last of them has had its grace.
pub(crate) fn suspend_owned_run_after_handoff_then<F, Fut>(
    orch: &Orchestrator,
    run_id: &str,
    exit_reason: &str,
    turn_owner: Option<ProcessTurnOwner>,
    after_park: F,
) where
    F: FnOnce(Result<(), String>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    if turn_owner
        .as_ref()
        .is_none_or(|owner| !orch.process_state.is_turn_owner_current(owner))
    {
        cairn_common::task::spawn(after_park(Ok(())));
        return;
    }
    let mut parked = arm_coalesced_park(orch, run_id, exit_reason, turn_owner);
    cairn_common::task::spawn(async move {
        let outcome = match parked.recv().await {
            Ok(outcome) => outcome,
            // The park reported nothing at all. Report it as a failed park: a
            // caller must never resume a run it cannot know was suspended, and
            // the pending row is left for startup reconciliation.
            Err(error) => Err(format!("durable-wait park reported no outcome: {error}")),
        };
        after_park(outcome).await;
    });
}

/// Arm the run's single deferred park, coalescing this suspension with any
/// sibling call of the same turn.
///
/// Each suspension bumps the run's generation and subscribes to the slot's
/// broadcast, then spawns a timer that parks only if its generation is still
/// current when the grace elapses. A later sibling therefore supersedes an
/// earlier one's timer, and the run is parked once — [`SUSPEND_HANDOFF_GRACE`]
/// after the LAST suspension marker rather than the first.
///
/// That is what the coalescing is for. The interrupt cancels whatever call is in
/// flight, so a park fired on the first sibling's schedule can land before a
/// later sibling's marker has travelled back through the MCP transport — which
/// is precisely the CAIRN-3162 misattribution the grace exists to prevent, where
/// the CLI writes its own "the user doesn't want to proceed" text into the
/// model's context in place of ours.
///
/// Whichever timer does park broadcasts its outcome on the shared sender, so
/// every sibling learns it, including those whose own timer stood down. A
/// suspension arriving after the slot is retired opens a fresh one and parks an
/// already-parked run, which is a benign no-op.
fn arm_coalesced_park(
    orch: &Orchestrator,
    run_id: &str,
    exit_reason: &str,
    turn_owner: Option<ProcessTurnOwner>,
) -> broadcast::Receiver<Result<(), String>> {
    let (generation, done, parked) = {
        let mut slots = orch.park_slots.0.lock().unwrap();
        let slot = slots.entry(run_id.to_string()).or_insert_with(|| ParkSlot {
            generation: 0,
            done: broadcast::channel(1).0,
        });
        slot.generation += 1;
        (slot.generation, slot.done.clone(), slot.done.subscribe())
    };
    let orch = orch.clone();
    let run_id = run_id.to_string();
    let exit_reason = exit_reason.to_string();
    cairn_common::task::spawn(async move {
        tokio::time::sleep(SUSPEND_HANDOFF_GRACE).await;
        // A sibling suspension armed after this one owns the park now.
        if !park_generation_is_current(&orch, &run_id, generation) {
            return;
        }
        // Stopping the session blocks on DB work, so it never runs on
        // the async worker itself.
        let joined = {
            let orch = orch.clone();
            let run_id = run_id.clone();
            tokio::task::spawn_blocking(move || {
                park_owned_turn(&orch, &run_id, turn_owner.as_ref())
            })
            .await
        };
        let parked = match joined {
            Ok(result) => result,
            Err(join_error) => Err(format!("park worker panicked: {join_error}")),
        };
        if let Err(error) = &parked {
            log::warn!("deferred {exit_reason} park failed for run {run_id}: {error}");
        }
        // Retire the slot BEFORE broadcasting, so a suspension arriving
        // afterwards opens its own rather than waiting on a signal already sent.
        retire_park_slot(&orch, &run_id, generation);
        let parked_ok = parked.is_ok();
        let _ = done.send(parked);
        // An escalated message that reached the turn while it was in the call
        // that just parked it was left for that call's return, and a park is
        // not a return. Deliver it now that the park is the last thing that
        // happened to the predecessor (tenet 1439).
        if parked_ok {
            let _ = tokio::task::spawn_blocking(move || {
                crate::messages::delivery::steer_park_for_waiting_escalation(&orch, &run_id)
            })
            .await;
        }
    });
    parked
}

/// Admission and completion share this authority. Never hold the registry mutex
/// across provider control or database work.
pub(crate) fn park_owned_turn(
    orch: &Orchestrator,
    run_id: &str,
    owner: Option<&ProcessTurnOwner>,
) -> Result<(), String> {
    let Some(owner) = owner else {
        return Ok(());
    };
    let job_id = job_id_for_run(orch, run_id);
    let launch_lock = job_id.as_deref().map(|id| orch.job_launch_lock(id));
    let guard = launch_lock
        .as_ref()
        .map(|lock| lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
    if owner.process_identity().run_id() != run_id
        || !orch.process_state.is_turn_owner_current(owner)
    {
        return Ok(());
    }
    if turn_state(orch, owner.turn_id()).as_deref() != Some("yielded") {
        log::warn!(
            "Refusing durable park for run {run_id}: turn {} is not durably yielded",
            owner.turn_id()
        );
        return Ok(());
    }
    if !orch.process_state.retire_turn_accounting_if_owned(owner) {
        return Ok(());
    }
    // Do not restake a wait resolved during handoff or overwrite operator Stop.
    if let Err(error) = crate::backends::stdin::send_interrupt(
        &orch.process_state,
        run_id,
        PauseIntent::Reassert,
        cairn_models::interface::session::InterruptCause::DurablePark,
    ) {
        log::warn!("Failed to interrupt owned durable park for {run_id}: {error}");
    }
    let parked = orch.process_state.transition_to_warm_if_owned(owner);
    drop(guard);
    if parked {
        publish_retained_turn_idle(orch, run_id);
    }
    Ok(())
}

/// Publish an actual park or Stop, never successful completion effects.
/// Call only after compare-release and after dropping launch authority.
pub(super) fn publish_retained_turn_idle(orch: &Orchestrator, run_id: &str) {
    if let Some(job_id) = job_id_for_run(orch, run_id) {
        if let Err(error) = crate::execution::advancement::recompute_job(orch, &job_id) {
            log::warn!("Failed to recompute idle job {job_id}: {error}");
        }
        super::review_push::emit_for_turn_end(orch, &job_id);
    }
    crate::backends::codex::emit_codex_run_turn_completed(&orch.services.emitter, run_id);
}

fn park_generation_is_current(orch: &Orchestrator, run_id: &str, generation: u64) -> bool {
    orch.park_slots
        .0
        .lock()
        .unwrap()
        .get(run_id)
        .is_some_and(|slot| slot.generation == generation)
}

fn retire_park_slot(orch: &Orchestrator, run_id: &str, generation: u64) {
    let mut slots = orch.park_slots.0.lock().unwrap();
    if slots
        .get(run_id)
        .is_some_and(|slot| slot.generation == generation)
    {
        slots.remove(run_id);
    }
}

fn child_run_ids_for_run(orch: &Orchestrator, run_id: &str) -> Vec<String> {
    let Some(job_id) = job_id_for_run(orch, run_id) else {
        return Vec::new();
    };
    let dbs = orch.db.clone();
    let run_id = run_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
            .await
            .map_err(|e| e.to_string())?;
        db.read(|conn| {
            Box::pin(async move {
                let descendant_job_ids = find_descendant_job_ids(conn, &job_id).await?;
                if descendant_job_ids.is_empty() {
                    return Ok(Vec::new());
                }
                get_running_runs_for_jobs(conn, &descendant_job_ids).await
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .unwrap_or_default()
}

pub(crate) async fn find_descendant_job_ids(
    conn: &crate::storage::TrackedConnection,
    job_id: &str,
) -> DbResult<Vec<String>> {
    let mut all_descendants = Vec::new();
    let mut current_parents = vec![job_id.to_string()];

    while !current_parents.is_empty() {
        let mut children = Vec::new();
        for parent_id in &current_parents {
            let mut rows = conn
                .query(
                    "SELECT id
                     FROM jobs
                     WHERE parent_job_id = ?1",
                    (parent_id.as_str(),),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                children.push(row.text(0)?);
            }
        }

        if children.is_empty() {
            break;
        }

        all_descendants.extend(children.clone());
        current_parents = children;
    }

    Ok(all_descendants)
}

async fn get_running_runs_for_jobs(
    conn: &crate::storage::TrackedConnection,
    job_ids: &[String],
) -> DbResult<Vec<String>> {
    let mut run_ids = Vec::new();
    for job_id in job_ids {
        let mut rows = conn
            .query(
                "SELECT id
                 FROM runs
                 WHERE job_id = ?1
                   AND status IN ('starting', 'live')",
                (job_id.as_str(),),
            )
            .await?;
        while let Some(row) = rows.next().await? {
            run_ids.push(row.text(0)?);
        }
    }
    Ok(run_ids)
}

/// Resolve the run whose turn a stop should interrupt, if one exists.
///
/// The run the job's head turn runs on comes first whenever this host holds its
/// process: that process is what serves the turn, whatever its row says. Then
/// a live (`starting`/`live`) run, preferring one with a process here. Returns
/// `None` when the node has neither (already complete, failed, or never
/// started), letting the caller report a clear no-op rather than guessing.
pub fn live_run_id_for_job(orch: &Orchestrator, job_id: &str) -> Option<String> {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    let (head_run, live_runs) = run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT t.run_id FROM jobs j JOIN turns t ON t.id = j.current_turn_id
                         WHERE j.id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;
                let head_run = match rows.next().await? {
                    Some(row) => row.opt_text(0)?,
                    None => None,
                };
                drop(rows);
                let live = get_running_runs_for_jobs(conn, std::slice::from_ref(&job_id)).await?;
                Ok((head_run, live))
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .ok()?;
    if let Some(head_run) = head_run {
        if orch.process_state.has_process(&head_run) == Some(true) {
            return Some(head_run);
        }
    }
    live_runs
        .iter()
        .find(|run_id| orch.process_state.has_process(run_id) == Some(true))
        .cloned()
        .or_else(|| live_runs.into_iter().next())
}

/// Deliberately stop a running workflow node (CAIRN-2516).
///
/// A workflow node's process is a stdin-less `bun <script>`, so the ordinary
/// interrupt cannot reach it; a Stop must hard-kill it AND cascade to its
/// in-flight child calls. This marks the workflow's live run as
/// deliberately-stopped, then reuses [`stop_session`] verbatim: the child cascade
/// kills orphaned agent sessions, and the workflow run's own interrupt-send fails
/// (no stdin) and falls through to [`kill_session_with_reason`]. The workflow
/// supervisor, seeing the marker on its finalize, maps the killed process to a
/// terminal, non-crashed (Stopped) outcome and KEEPS the re-dispatch record so a
/// later Restart works — while the terminal status keeps the startup sweep from
/// resurrecting it (the trap this issue turns on). A no-op when the node has no
/// live run.
pub async fn stop_workflow(orch: &Orchestrator, workflow_job_id: &str) -> Result<(), String> {
    let Some(run_id) = live_run_id_for_job(orch, workflow_job_id) else {
        log::info!("stop_workflow: no live run for job {workflow_job_id}; nothing to stop");
        return Ok(());
    };
    // Set the marker BEFORE the kill so the supervisor's finalize (which races
    // the kill path's own finalize) observes it and maps to the cancelled,
    // non-re-dispatched outcome regardless of which finalizer wins the status
    // race.
    orch.process_state.mark_workflow_stop_requested(&run_id);
    // Preserve the existing child-call cascade; the workflow itself no longer
    // has a runner-local child handle, so its expected final error is ignored.
    let _ = stop_session(orch, &run_id);
    let fence = orch
        .process_state
        .workflow_lease(&run_id)
        .ok_or_else(|| format!("workflow {run_id} has no live executor binding"))?;
    crate::fleet::residency::stop(orch, &fence, &run_id).await
}

/// Stop an in-flight workflow child call (CAIRN-2516).
///
/// Hard-terminates the call's agent session via [`kill_session_with_reason`]
/// (`"user_stop"`) — NOT the warm-park [`stop_session`], which would leave the
/// run non-terminal and hang the workflow's awaiting `agent()`. The killed run
/// reaches `exited` with no artifact, which `terminal_call_body` maps to the call
/// failure sentinel so `agent()` resolves `null` (deep-research's salvage paths
/// handle it). `finalize_run` then journals the call as Failure(null) at its
/// `(workflow_run_id, ordinal)` and deletes the link — a stopped call is journaled
/// exactly like any failed call. Rejects a run that is not a workflow child call
/// or is already terminal.
pub fn stop_call(orch: &Orchestrator, call_run_id: &str) -> Result<(), String> {
    match run_status(orch, call_run_id).as_deref() {
        None => return Err(format!("Call run {call_run_id} not found")),
        Some(status) if is_terminal_run_status(status) => {
            log::info!("stop_call: run {call_run_id} already terminal ({status}); nothing to stop");
            return Ok(());
        }
        Some(_) => {}
    }
    if !is_workflow_child_run(orch, call_run_id) {
        return Err(format!(
            "Run {call_run_id} is not a workflow child call; refusing stop_call"
        ));
    }
    kill_session_with_reason(orch, call_run_id, "user_stop", TerminalPolicy::Finalize)
}

/// The stored run statuses that count as terminal (its process has exited).
/// Mirrors `RunStatus::is_terminal` plus the legacy stored spellings.
fn is_terminal_run_status(status: &str) -> bool {
    matches!(
        status,
        "exited" | "crashed" | "complete" | "completed" | "failed"
    )
}

/// Whether a run is a child call of a workflow node — its parent job carries the
/// synthetic `agent_config_id = "workflow"`. Gates [`stop_call`] so it acts only
/// on genuine workflow calls, validated server-side rather than trusting the UI.
fn is_workflow_child_run(orch: &Orchestrator, run_id: &str) -> bool {
    let dbs = orch.db.clone();
    let run_id = run_id.to_string();
    let parent_agent_config_id = run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
            .await
            .map_err(|e| e.to_string())?;
        db.read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT parent.agent_config_id FROM runs r \
                         JOIN jobs j ON r.job_id = j.id \
                         JOIN jobs parent ON j.parent_job_id = parent.id \
                         WHERE r.id = ?1 LIMIT 1",
                        (run_id.as_str(),),
                    )
                    .await?;
                Ok(rows.next().await?.and_then(|row| row.text(0).ok()))
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .unwrap_or(None);
    crate::execution::jobs::is_workflow_agent_config(parent_agent_config_id.as_deref())
}

/// The operator pressed Stop on a run. A user interrupt suppresses autonomous
/// successors, so the job's goal is turned off before the turn is interrupted
/// and stays off until the operator turns it back on. Internal stops (a model
/// switch, closing an issue) go through [`stop_session`] and leave the
/// operator's choice alone; an agent's escalated message never stops a session
/// ([`interrupt_turn_for_message`]).
pub fn operator_stop_run(orch: &Orchestrator, run_id: &str) -> Result<(), String> {
    let job_id = job_id_for_run(orch, run_id);
    if let Some(job_id) = &job_id {
        crate::execution::jobs::record_goal_intent(
            orch,
            job_id,
            crate::execution::jobs::GoalIntent::Off,
        )?;
    }
    let stopped = stop_session(orch, run_id);
    if let Some(job_id) = &job_id {
        // The operator's stop also cancels a profile generation the job runs.
        crate::orchestrator::inference_generate::after_operator_stop(orch, job_id);
    }
    stopped
}

/// The operator pressed Stop on a job. See [`operator_stop_run`].
pub fn operator_stop_job(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    if job_exists(orch, job_id)? {
        crate::execution::jobs::record_goal_intent(
            orch,
            job_id,
            crate::execution::jobs::GoalIntent::Off,
        )?;
    }
    let stopped = stop_job(orch, job_id);
    crate::orchestrator::inference_generate::after_operator_stop(orch, job_id);
    stopped
}

/// Stop a running backend session, cascading to child runs
pub fn stop_session(orch: &Orchestrator, run_id: &str) -> Result<(), String> {
    // First, collect child runs to stop
    let child_run_ids = child_run_ids_for_run(orch, run_id);

    // Stop child runs first
    for child_run_id in &child_run_ids {
        log::info!(
            "Stopping child run {} (parent run {} stopped)",
            child_run_id,
            run_id
        );
        let _ = stop_session_internal(orch, child_run_id, StopReason::OperatorStop);
    }

    // Stop the requested run
    stop_session_internal(orch, run_id, StopReason::OperatorStop)
}

/// Interrupt a run's turn so an agent's escalated message lands now, leaving
/// everything the job started running (tenet 1439). Used only for a turn that
/// is generating rather than in a tool call: a turn in a tool call is steered
/// at its next tool boundary instead (`messages::delivery`). Unlike
/// [`stop_session`] it does not cascade to child runs.
pub fn interrupt_turn_for_message(orch: &Orchestrator, run_id: &str) -> Result<(), String> {
    stop_session_internal(orch, run_id, StopReason::Message)
}

/// Job-level stop for a suspended/waiting job that has no live run attached.
///
/// Run-scoped [`stop_session`] needs a `run_id` to interrupt and park warm, but a
/// job that suspended on a foreground question or an inline delegated task can
/// finalize its run (the OpenRouter owned loop keeps no warm process) and rest in
/// a non-terminal state with NO run to attach to. Pressing Stop there used to fail
/// with "no active run". This idles the job from its id directly:
///
/// 1. A live run IS attached -> defer entirely to the run-scoped path, leaving the
///    existing warm-park behavior unchanged.
/// 2. Otherwise fully idle the job (Option A): cascade-stop every descendant child
///    run, withdraw any open question, and terminalize the live
///    (`pending`/`running`/`yielded`) turns — which drops a pending delegated
///    successor and ends the yielded work turn — then recompute so the projection
///    reflects a steerable, no-longer-waiting state. The user can immediately send
///    a follow-up that starts a fresh turn.
///
/// A job with no row has nothing to stop, and that is success: a Stop racing a
/// deletion must not surface as a failed recompute.
pub fn stop_job(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    // A live run is attached: the run-scoped path is the unchanged behavior.
    if let Some(run_id) = live_run_id_for_job(orch, job_id) {
        return stop_session(orch, &run_id);
    }
    if !job_exists(orch, job_id)? {
        return Ok(());
    }

    // No run to attach to. Cascade-stop descendant child runs first, mirroring
    // the child cascade `stop_session` performs from a run id.
    for child_run_id in descendant_running_run_ids_for_job(orch, job_id) {
        log::info!(
            "Stopping child run {} (job-level stop of {})",
            child_run_id,
            job_id
        );
        let _ = stop_session_internal(orch, &child_run_id, StopReason::OperatorStop);
    }

    // Cancel open input and terminalize the job's live turns (drops a pending
    // delegated successor), then recompute the projection.
    cancel_open_input_and_live_turns_for_job(orch, job_id)?;
    crate::execution::advancement::recompute_job(orch, job_id).map_err(|error| {
        format!("Failed to recompute job {job_id} after job-level stop: {error}")
    })?;

    let (prompt_change, turn_change) = run_db_blocking({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        move || async move {
            let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())?;
            Ok((
                crate::notify::prompt_db_change_for_job(&db, &job_id, "update").await,
                crate::notify::turn_db_change_for_job_id(&db, &job_id, "update").await,
            ))
        }
    })?;
    let _ = orch.services.emitter.emit("db-change", prompt_change);
    let _ = orch.services.emitter.emit("db-change", turn_change);

    Ok(())
}

/// Stable interruption cause for work ended by issue resolution.
pub fn resolution_exit_reason(resolution: crate::transitions::Resolution) -> &'static str {
    match resolution {
        crate::transitions::Resolution::Merged => "issue_merged",
        crate::transitions::Resolution::Closed => "issue_closed",
    }
}

/// Irreversibly end a job and its delegated descendants for issue resolution.
/// Unlike an operator Stop, this closes sessions and cancels queued turns before
/// stopping processes, so a delayed exit cannot resume the cancelled work.
pub fn stop_job_for_resolution(
    orch: &Orchestrator,
    job_id: &str,
    resolution: crate::transitions::Resolution,
) -> Result<(), String> {
    stop_job_for_resolution_preserving_run(orch, job_id, resolution, None)
}

/// Settle resolution without cutting off the run delivering its own merge result.
/// The caller must invoke `finish_resolution_run` after its response handoff.
/// Cancellation authority is revoked immediately even for that preserved run.
pub fn stop_job_for_resolution_preserving_run(
    orch: &Orchestrator,
    job_id: &str,
    resolution: crate::transitions::Resolution,
    response_run_id: Option<&str>,
) -> Result<(), String> {
    let reason = resolution_exit_reason(resolution);
    let (jobs, turns, runs) = run_db_blocking({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        move || async move {
            let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|error| error.to_string())?;
            db.write(|conn| {
                let job_id = job_id.clone();
                Box::pin(async move {
                    let mut jobs = find_descendant_job_ids(conn, &job_id).await?;
                    jobs.push(job_id);
                    let mut turns = Vec::new();
                    let mut runs = Vec::new();
                    let now = chrono::Utc::now().timestamp();
                    for job in &jobs {
                        let mut rows = conn.query(
                            "SELECT id, run_id FROM turns WHERE job_id = ?1 AND (state = 'running' OR (state = 'cancelled' AND end_reason = ?2))",
                            (job.as_str(), reason),
                        ).await?;
                        while let Some(row) = rows.next().await? {
                            if let Some(run) = row.opt_text(1)? {
                                turns.push((row.text(0)?, run));
                            }
                        }
                        drop(rows);
                        let mut rows = conn.query(
                            "SELECT id FROM runs WHERE job_id = ?1 AND status IN ('starting', 'live')",
                            (job.as_str(),),
                        ).await?;
                        while let Some(row) = rows.next().await? { runs.push(row.text(0)?); }
                        drop(rows);
                        crate::mcp::handlers::planning::withdraw_job_prompts_conn(
                            conn, job,
                            match resolution {
                                crate::transitions::Resolution::Merged => crate::mcp::handlers::permission::WithdrawalCause::IssueMerged,
                                crate::transitions::Resolution::Closed => crate::mcp::handlers::permission::WithdrawalCause::IssueClosed,
                            }, now,
                        ).await?;
                        conn.execute(
                            "UPDATE turns SET state = 'cancelled', end_reason = ?2, ended_at = ?3, updated_at = ?3
                             WHERE job_id = ?1 AND state IN ('pending', 'running', 'yielded')",
                            (job.as_str(), reason, now),
                        ).await?;
                        let mut rows = conn.query(
                            "SELECT DISTINCT run_id FROM agent_waits WHERE job_id = ?1
                             AND state IN ('pending', 'resolving')",
                            (job.as_str(),),
                        ).await?;
                        let mut waiting_runs = Vec::new();
                        while let Some(row) = rows.next().await? {
                            waiting_runs.push(row.text(0)?);
                        }
                        drop(rows);
                        for run in waiting_runs {
                            crate::mcp::handlers::durable_suspend::cancel_run_waits_conn(
                                conn, &run, reason, chrono::Utc::now().timestamp_millis(),
                            ).await?;
                        }
                        conn.execute(
                            "UPDATE sessions SET status = 'closed', terminal_reason = ?2, closed_at = ?3, updated_at = ?3
                             WHERE job_id = ?1 AND status != 'closed'",
                            (job.as_str(), match resolution { crate::transitions::Resolution::Merged => "issue_merged", crate::transitions::Resolution::Closed => "issue_closed" }, now),
                        ).await?;
                        conn.execute(
                            "UPDATE jobs SET status = 'cancelled', completed_at = ?2, updated_at = ?2
                             WHERE id = ?1 AND status NOT IN ('complete', 'completed', 'failed', 'cancelled', 'skipped')",
                            (job.as_str(), now),
                        ).await?;
                        conn.execute(
                            "UPDATE runs SET exit_reason = ?2 WHERE job_id = ?1 AND status IN ('starting', 'live')",
                            (job.as_str(), reason),
                        ).await?;
                    }
                    conn.on_commit(cairn_db::admission_hint::evidence_changed);
                    Ok((jobs, turns, runs))
                })
            }).await.map_err(|error| error.to_string())
        }
    })?;
    let mut errors = Vec::new();
    for job in &jobs {
        if let Err(error) = cancel_provider_holds(orch, job) {
            errors.push(error);
        }
        orch.fleet.cancel_job_requests(
            job,
            match resolution {
                crate::transitions::Resolution::Merged => crate::fleet::CancelCause::IssueMerged,
                crate::transitions::Resolution::Closed => crate::fleet::CancelCause::IssueClosed,
            },
        );
    }
    for (turn, run) in turns {
        if response_run_id == Some(run.as_str())
            || orch.resolution_responses.lock().unwrap().contains_key(&run)
        {
            continue;
        }
        orch.write_stops.raise(&run);
        if let Err(error) = settle_unanswered_calls(
            orch,
            &run,
            &turn,
            InterruptCause::Resolution {
                exit_reason: reason.to_string(),
            },
        ) {
            errors.push(error);
        }
    }
    // Include retained warm processes even when their run projection is terminal.
    let mut runs = runs;
    for run in orch.process_state.run_ids() {
        if job_id_for_run(orch, &run).is_some_and(|job| jobs.contains(&job)) && !runs.contains(&run)
        {
            runs.push(run);
        }
    }
    for run in runs {
        if response_run_id == Some(run.as_str())
            || orch.resolution_responses.lock().unwrap().contains_key(&run)
        {
            orch.resolution_responses
                .lock()
                .unwrap()
                .insert(run.clone(), Some(resolution));
            // The provider acknowledges tool results at its terminal-tool boundary.
            // Until then the cancelled run retains only its response transport.
            orch.process_state.arm_terminal_tool(&run);
            continue;
        }
        if let Err(error) = finish_resolution_run(orch, &run, resolution) {
            errors.push(error);
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

/// Called only at the provider's acknowledged terminal-tool boundary. Resolution
/// already revoked authority; the typed cause retained with its transport now
/// determines final cleanup.
pub(crate) fn finish_resolution_response(orch: &Orchestrator, run_id: &str) -> bool {
    let Some(Some(resolution)) = orch.resolution_responses.lock().unwrap().remove(run_id) else {
        return false;
    };
    if let Err(error) = finish_resolution_run(orch, run_id, resolution) {
        log::warn!("Resolution response cleanup pending for {run_id}: {error}");
    }
    true
}

/// Finish the process retained only to deliver a resolution response. Call after
/// the response is handed off, never from inside the resolving write itself.
pub fn finish_resolution_run(
    orch: &Orchestrator,
    run_id: &str,
    resolution: crate::transitions::Resolution,
) -> Result<(), String> {
    orch.resolution_responses.lock().unwrap().remove(run_id);
    orch.write_stops.raise(run_id);
    cleanup_inline_commands(orch, run_id);
    set_exit_reason(orch, run_id, resolution_exit_reason(resolution))?;
    kill_session_with_reason(
        orch,
        run_id,
        resolution_exit_reason(resolution),
        TerminalPolicy::Keep,
    )
}

/// Running (`starting`/`live`) run ids for every descendant job of `job_id`. The
/// job-level analogue of [`child_run_ids_for_run`], resolved from a job id rather
/// than a run id (a suspended job may have no run of its own).
fn descendant_running_run_ids_for_job(orch: &Orchestrator, job_id: &str) -> Vec<String> {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
        db.read(|conn| {
            Box::pin(async move {
                let descendant_job_ids = find_descendant_job_ids(conn, &job_id).await?;
                if descendant_job_ids.is_empty() {
                    return Ok(Vec::new());
                }
                get_running_runs_for_jobs(conn, &descendant_job_ids).await
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .unwrap_or_default()
}

fn job_exists(orch: &Orchestrator, job_id: &str) -> Result<bool, String> {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|error| error.to_string())?;
        db.query_opt_text("SELECT id FROM jobs WHERE id = ?1", (job_id,))
            .await
            .map(|row| row.is_some())
            .map_err(|error| error.to_string())
    })
}

fn cancel_provider_holds(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|error| error.to_string())?;
        crate::orchestrator::wakes::cancel_provider_limit_holds_for_job(&db, &job_id).await
    })
}

/// Withdraw any open `ask_user` question on the job and terminalize its live
/// (`pending`/`running`/`yielded`) turns in one write. A withdrawn question is
/// pending nowhere, which clears the issue's NeedsInput attention, and a late
/// answer is refused with the stop as its cause rather than recorded (no
/// answer was ever given, so none is written). Terminalizing the live turns
/// drops a pending delegated successor and ends the yielded work turn, so
/// nothing is left waiting on the question: the job recomputes to a steerable
/// state and the user starts a fresh turn with a follow-up (CAIRN-1907).
fn cancel_open_input_and_live_turns_for_job(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<(), String> {
    cancel_provider_holds(orch, job_id)?;
    let launch_lock = orch.job_launch_lock(job_id);
    let _guard = launch_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let claimed_turns = serde_json::to_string(&orch.terminal_completion_turn_ids())
        .map_err(|error| error.to_string())?;
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
        db.write(|conn| {
            let job_id = job_id.clone();
            let claimed_turns = claimed_turns.clone();
            Box::pin(async move {
                let now = chrono::Utc::now().timestamp();
                crate::mcp::handlers::planning::withdraw_job_prompts_conn(
                    conn,
                    &job_id,
                    crate::mcp::handlers::permission::WithdrawalCause::OperatorStop,
                    now,
                )
                .await?;
                conn.execute(
                    "UPDATE turns
                     SET state = 'cancelled', ended_at = ?1, updated_at = ?1
                     WHERE job_id = ?2 AND state IN ('pending', 'running', 'yielded')
                       AND id NOT IN (SELECT value FROM json_each(?3))",
                    (now, job_id.as_str(), claimed_turns.as_str()),
                )
                .await?;
                conn.on_commit(cairn_db::admission_hint::evidence_changed);
                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StopReason {
    /// Operator Stop and cascade cancellation reap inline commands and hard-kill
    /// if interrupt delivery fails. The native goal is paused as interrupted;
    /// whether it continues once the job runs again is the job's goal intent,
    /// which the operator Stop entrypoints turn off.
    OperatorStop,
    /// A system-owned park preserves inline siblings and warm state even when
    /// interrupt delivery fails. Only this condition may release its pause.
    Park(GoalPauseOwner),
    /// An agent's escalated message to a turn that is only generating (tenet
    /// 1439). The turn ends as a warm stop so the message lands now, and nothing
    /// the job started is touched: no batch is cancelled at its machine, no
    /// inline command or terminal is killed, no durable wait or check request
    /// is withdrawn, no child run is stopped, and a failed interrupt never falls
    /// back to a hard kill.
    Message,
}

impl StopReason {
    fn pause_intent(&self) -> PauseIntent {
        PauseIntent::Stake(match self {
            Self::OperatorStop | Self::Message => GoalPause::Interrupted,
            Self::Park(owner) => GoalPause::Owned(owner.clone()),
        })
    }

    fn is_operator_stop(&self) -> bool {
        matches!(self, Self::OperatorStop)
    }

    fn interrupt_cause(&self) -> cairn_models::interface::session::InterruptCause {
        match self {
            Self::OperatorStop | Self::Message => {
                cairn_models::interface::session::InterruptCause::OperatorStop
            }
            Self::Park(_) => cairn_models::interface::session::InterruptCause::DurablePark,
        }
    }
}

/// Internal stop without cascading (used by cascading stop)
///
/// Sends an interrupt control request via stdin and transitions the process
/// to warm state. The process is NOT killed - it stays available for follow-up
/// messages.
///
/// A self-suspend preserves a parallel batch's inline siblings so its results
/// can settle. Operator Stop cancels those siblings and the owned wait itself.
pub(crate) fn stop_session_internal(
    orch: &Orchestrator,
    run_id: &str,
    reason: StopReason,
) -> Result<(), String> {
    let intent = reason.pause_intent();
    stop_session_with_pause_intent(orch, run_id, reason, intent)
}

// Deferred parks already staked their owner before publishing the suspension.
// They reassert at interrupt time, never replacing a superseding pause claim.
fn stop_session_with_pause_intent(
    orch: &Orchestrator,
    run_id: &str,
    reason: StopReason,
    intent: PauseIntent,
) -> Result<(), String> {
    let job_id = job_id_for_run(orch, run_id);
    let launch_lock = job_id.as_deref().map(|id| orch.job_launch_lock(id));
    let (identity, owner, turn_id) = {
        let _guard = launch_lock
            .as_ref()
            .map(|lock| lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
        let identity = orch.process_state.capture_process_identity(run_id);
        let owner = identity
            .as_ref()
            .and_then(|identity| orch.process_state.capture_turn_owner_for_identity(identity));
        // An owned failure keeps its turn active while draining calls after
        // closing the run. Its released occupancy is not a DB fallback to adopt.
        if orch.has_terminal_completion_claim(run_id) {
            return Ok(());
        }
        let turn_id = owner
            .as_ref()
            .map(|owner| owner.turn_id().to_string())
            .or_else(|| active_turn_id_for_run(orch, run_id));
        (identity, owner, turn_id)
    };
    // Calls can publish events, so settle only the captured turn here, outside
    // launch authority. Its durable outcome and accounting retirement follow
    // together under that authority after ownership is checked again.
    let settle_active_turn = |orch: &Orchestrator, run_id: &str| {
        if let Some(turn_id) = turn_id.as_deref() {
            if let Err(error) =
                settle_live_turn_calls(orch, Some(run_id), turn_id, TurnSettlement::UserStop)
            {
                log::warn!("Failed to settle stopped turn's tool calls for {run_id}: {error}");
            }
        }
    };
    match &reason {
        StopReason::OperatorStop => {
            if let Some(job_id) = job_id_for_run(orch, run_id) {
                cancel_provider_holds(orch, &job_id)?;
            }
            cancel_owned_waits_for_run(orch, run_id);
            settle_active_turn(orch, run_id);
        }
        StopReason::Message => settle_active_turn(orch, run_id),
        StopReason::Park(_) => stop_active_turn_for_run(orch, run_id, false),
    }
    let settle_turn = !matches!(reason, StopReason::Park(_));
    let guard = launch_lock
        .as_ref()
        .map(|lock| lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
    if orch.has_terminal_completion_claim(run_id) {
        return Ok(());
    }
    if match identity.as_ref() {
        Some(identity) => {
            !orch.process_state.is_process_identity_current(identity)
                || owner.as_ref().map_or_else(
                    || orch.process_state.get_current_turn_id(run_id).is_some(),
                    |owner| !orch.process_state.is_turn_owner_current(owner),
                )
        }
        None => orch
            .process_state
            .capture_process_identity(run_id)
            .is_some(),
    } {
        return if reason.is_operator_stop() {
            Err(format!(
                "Run {run_id} changed ownership while Stop was settling tool calls; retry Stop"
            ))
        } else {
            Ok(())
        };
    }
    let turn_change = if settle_turn {
        match turn_id
            .as_deref()
            .map(|turn_id| settle_live_turn_outcome(orch, turn_id, TurnSettlement::UserStop))
            .transpose()
        {
            Ok(change) => change.flatten(),
            Err(error) => {
                log::warn!("Failed to durably settle stopped turn for {run_id}: {error}");
                None
            }
        }
    } else {
        None
    };
    // Direct durable-wait callers intentionally park the captured provider
    // turn without changing its logical Running/Pending state. Unlike a
    // deferred park, that API does not require a separately published yield.
    // Stop and message interruption require durable settlement before retirement.
    let disposed = !settle_turn
        || owner.as_ref().is_none_or(|owner| {
            turn_state(orch, owner.turn_id())
                .and_then(|state| state.parse::<TurnState>().ok())
                .is_some_and(|state| state.is_terminal())
        });
    if disposed {
        if let Some(owner) = owner.as_ref() {
            if !orch.process_state.retire_turn_accounting_if_owned(owner) {
                drop(guard);
                if let Some(change) = turn_change {
                    let _ = orch.services.emitter.emit("db-change", change);
                }
                return Err(format!(
                    "Run {run_id} changed ownership before Stop could retire accounting"
                ));
            }
        }
    } else {
        // Still attempt the physical interrupt. An unfinished turn cannot be
        // retired or released; its accepted terminal can retry settlement.
        log::warn!("Run {run_id} could not durably stop its captured turn; interrupting without releasing ownership");
    }

    // Send interrupt via the backend-aware stdin handler. For Codex this pauses
    // the native goal under the same stdin lock before sending `turn/interrupt`,
    // so even an immediate provider idle notification cannot start a successor.
    if let Err(e) = crate::backends::stdin::send_interrupt(
        &orch.process_state,
        run_id,
        intent,
        reason.interrupt_cause(),
    ) {
        if e.starts_with("Process not found:") {
            log::warn!(
                "Run {} missing from process map during stop; reconciling stale DB state",
                run_id
            );
        } else if reason.is_operator_stop() {
            log::warn!(
                "Failed to send interrupt to run {}; falling back to hard termination: {}",
                run_id,
                e
            );
            drop(guard);
            if let Some(change) = turn_change {
                let _ = orch.services.emitter.emit("db-change", change);
            }
            return kill_session_with_reason(orch, run_id, "user_stop", TerminalPolicy::Finalize);
        } else {
            log::warn!(
                "Failed to send interrupt to run {}; preserving its warm state and its work: {}",
                run_id,
                e
            );
        }
    }

    let warmed = disposed
        && match owner.as_ref() {
            Some(owner) => orch.process_state.transition_to_warm_if_owned(owner),
            None => identity.as_ref().is_some_and(|identity| {
                orch.process_state.transition_to_warm_if_untracked(identity)
                    || orch.process_state.is_warm(run_id)
            }),
        };
    drop(guard);
    if let Some(change) = turn_change {
        let _ = orch.services.emitter.emit("db-change", change);
    }

    // Only kill foreground bash processes — background terminals survive the
    // interrupt regardless. A self-suspend leaves the inline siblings alone.
    if reason.is_operator_stop() {
        cleanup_inline_commands(orch, run_id);
        // A stop must also reach the executor. `cleanup_inline_commands` reaps
        // only host-side children, so a routed batch — including one this run
        // suspended on — would otherwise keep running with nobody to read its
        // result.
        if let Some(job_id) = job_id_for_run(orch, run_id) {
            let cancelled = orch
                .fleet
                .cancel_job_requests(&job_id, crate::fleet::CancelCause::JobStopped);
            if cancelled > 0 {
                log::info!("stop cancelled {cancelled} in-flight batch(es) for job {job_id}");
            }
            // Its check requests end with it, durably, so a restart does not
            // re-drive work the stopped node abandoned.
            let orch = orch.clone();
            cairn_common::task::spawn(async move {
                crate::execution::checks::requests::cancel_requests_of_stopped_job(&orch, &job_id)
                    .await;
            });
        }
    }

    if !disposed {
        return Err(format!(
            "Run {run_id} was interrupted but its turn could not be durably settled"
        ));
    }
    // Transition to warm state instead of killing
    if warmed {
        publish_retained_turn_idle(orch, run_id);
        log::info!(
            "Run {} interrupted and transitioned to warm state",
            &run_id[..run_id.len().min(8)]
        );
    } else if identity.is_none() {
        log::warn!(
            "Run {} not found in process map after interrupt",
            &run_id[..run_id.len().min(8)]
        );
        if matches!(
            run_status(orch, run_id).as_deref(),
            Some("starting" | "live" | "running" | "idle")
        ) {
            let _ = set_exit_reason(orch, run_id, "user_stop");
            if let Err(error) = transition_owned_run(orch, run_id, RunStatus::Exited) {
                log::warn!("Failed to finalize stopped stale run {}: {}", run_id, error);
            }
            // This reconcile early-return transitions to Exited WITHOUT reaching
            // finalize_run, so a queued call killed here (status `starting`, no
            // process handle) would otherwise be stranded in its admission queue.
            // Dequeue it explicitly; `release` is idempotent so the finalize hook
            // covering the normal path stays harmless.
            crate::execution::jobs::on_call_run_finalized(orch, run_id);
            let _ = orch
                .services
                .emitter
                .emit("run-completed", serde_json::json!(run_id));
            let _ = orch.run_completions.send(run_id.to_string());
        }
    }

    Ok(())
}

/// Kill only foreground (inline) bash processes for a run.
///
/// Background terminals are intentionally left alive — they should survive
/// an interrupt so the agent can resume and still interact with them.
fn cleanup_inline_commands(orch: &Orchestrator, run_id: &str) {
    for child in orch.pty_state.take_inline_commands(run_id) {
        if let Ok(mut child) = child.lock() {
            let _ = child.kill();
            let _ = child.try_wait();
        }
    }
}

/// Finalize background terminals associated with a run's job on a hard kill
/// whose caller chose [`TerminalPolicy::Finalize`].
///
/// This runs on user-stop / GC eviction: the run stops but the issue/job
/// persists and may resume, so terminals are marked `exited` (retained) rather
/// than deleted — deletion is reserved for true job teardown
/// (`execution/teardown.rs`). Each terminal converges on the single finalize
/// sink, which kills the child, records an honest non-success exit code, routes
/// the exit wake, and drops the live session.
fn cleanup_job_terminals(orch: &Orchestrator, run_id: &str) {
    let job_id = job_id_for_run(orch, run_id);
    let Some(job_id) = job_id else {
        return;
    };

    let running_terminals = running_terminals_for_job(orch, &job_id);

    for (_terminal_id, session_id) in running_terminals {
        if let Err(error) =
            crate::mcp::handlers::terminal::finalize_terminal_by_session_id(orch, &session_id)
        {
            log::warn!("failed to finalize terminal {session_id} on session kill: {error}");
        }
    }
}

/// Forcefully kill a backend session and finalize.
///
/// Use this when truly terminating a session (e.g., closing an issue,
/// GC eviction, or cleanup). Unlike `stop_session`, this actually kills
/// the process and cannot be resumed.
pub fn kill_session(orch: &Orchestrator, run_id: &str) -> Result<(), String> {
    kill_session_with_reason(orch, run_id, "user_stop", TerminalPolicy::Finalize)
}

/// Exit reason for a provider turn deliberately interrupted immediately before
/// an automatic same-session recovery. It remains a crashed/interrupted turn,
/// but terminal failure attention must not fire between the two turns.
pub(crate) const PROVIDER_SILENCE_RECOVERY_EXIT_REASON: &str = "provider_silence_recovery";
pub(crate) const WATCHDOG_ARM_FAILED_EXIT_REASON: &str = "watchdog_arm_failed";
/// Exit reason for a run an operator Compact killed. Finalized as `Exited`
/// through [`super::finalize_run_for_operator_compact`], which leaves the job's
/// continuation to the Compact.
pub(crate) const OPERATOR_COMPACT_EXIT_REASON: &str = "operator_compact";

/// Whether ending a provider process also ends the work it started: the run's
/// inline commands and its job's background terminals.
///
/// Both belong to the job, not to the process, so only the caller knows whether
/// they outlive it. An inline command a kill spares runs to completion and
/// unregisters itself when its call returns, so sparing one leaks nothing; a
/// batch whose caller can no longer take its answer keeps running (tenet 1421).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalPolicy {
    /// The job's work is being ended: an operator Stop, GC eviction, a reaped
    /// one-shot call, a teardown. Inline commands and terminals end with it.
    Finalize,
    /// The job continues past this kill (watchdog recovery, a provider change,
    /// a restarted call, retiring an idle warm process), so its inline commands
    /// and terminals stay up for whatever turn runs next.
    Keep,
}

/// Release admission and stop the registered child without finalizing its run.
/// The caller owns exit bookkeeping, which can resume the job and must happen
/// outside the job launch lock. `exit_reason` is what that bookkeeping will
/// record; the interrupt sent first carries it as its cause.
pub(super) fn teardown_process(
    orch: &Orchestrator,
    run_id: &str,
    terminals: TerminalPolicy,
    exit_reason: &str,
) -> Result<(), String> {
    // Removal below destroys the handle carrying the release generation. Do not
    // proceed unless the terminal obligation has been durably recorded; the
    // caller can retry teardown without leaking the provider claim.
    if !super::finalize::release_process_admission(orch, run_id) {
        return Err(format!(
            "could not durably record provider admission release for run {run_id}"
        ));
    }
    // Only send interrupt to non-idle processes
    let is_idle = orch
        .process_state
        .get_occupancy(run_id)
        .map(|o| matches!(o, crate::agent_process::process::RunOccupancy::Idle))
        .unwrap_or(false);

    if !is_idle {
        let _ = crate::backends::stdin::send_interrupt_before_kill(
            &orch.process_state,
            run_id,
            exit_reason,
        );

        // Brief wait for graceful handling
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    if terminals == TerminalPolicy::Finalize {
        cleanup_inline_commands(orch, run_id);
        cleanup_job_terminals(orch, run_id);
    }

    // Remove the handle under the registry lock, but stop its child only after
    // releasing it: `graceful_stop` can wait seconds on a child that ignores
    // SIGTERM, and every session's reader takes this lock per stdout line.
    //
    // A panic while another thread holds the registry must not strand durable
    // run state. The child handle may still be safely removed from a poisoned
    // mutex; more importantly, exit-reason persistence and finalization below
    // must remain unconditional.
    let handle = {
        let mut processes = orch
            .process_state
            .processes
            .lock()
            .unwrap_or_else(|poisoned| {
                log::error!(
                    "Process registry lock was poisoned while killing run {}; recovering to finalize it",
                    &run_id[..run_id.len().min(8)]
                );
                poisoned.into_inner()
            });
        let handle = processes.remove(run_id);
        // `into_inner` repairs access for this caller but does not clear the
        // mutex's poison bit. Clear it while the repaired registry is still
        // locked: releasing first would let a successor observe stale poison, or
        // let a new panic's poison be cleared without this caller inspecting
        // that state.
        orch.process_state.processes.clear_poison();
        handle
    };

    if let Some(handle) = handle {
        // A pooled thread has no child of its own, and its shared server is
        // never signalled: the thread is interrupted and unsubscribed there.
        if let Some(thread) = handle.pooled_thread.as_ref() {
            thread.release();
            log::info!(
                "Released pooled Codex thread {} for run {}",
                thread.thread_id(),
                &run_id[..run_id.len().min(8)]
            );
        }
        let resolution = matches!(exit_reason, "issue_merged" | "issue_closed");
        let pending = match handle.child.lock() {
            Ok(mut child_guard) => {
                if let Some(mut child) = child_guard.take() {
                    crate::agent_process::process::graceful_stop(&mut *child);
                    if resolution && !matches!(child.try_wait(), Ok(Some(_))) {
                        *child_guard = Some(child);
                        true
                    } else {
                        log::info!("Killed process for run {}", &run_id[..run_id.len().min(8)]);
                        false
                    }
                } else {
                    false
                }
            }
            Err(_) => resolution,
        };
        if pending {
            orch.process_state
                .processes
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .register(run_id.to_string(), handle);
            return Err(format!(
                "process exit not confirmed for run {run_id}; cleanup pending"
            ));
        }
    }

    Ok(())
}

/// Kill a session with a specific exit reason, ending the job's background
/// terminals only when `terminals` says the job's work ends with it.
pub fn kill_session_with_reason(
    orch: &Orchestrator,
    run_id: &str,
    exit_reason: &str,
    terminals: TerminalPolicy,
) -> Result<(), String> {
    kill_session_with_reason_inner(orch, run_id, exit_reason, terminals, None)
}

/// The caller already owns launch authority for `locked_job_id`.
pub(crate) fn kill_session_with_reason_launch_locked(
    orch: &Orchestrator,
    run_id: &str,
    exit_reason: &str,
    terminals: TerminalPolicy,
    locked_job_id: &str,
) -> Result<(), String> {
    kill_session_with_reason_inner(orch, run_id, exit_reason, terminals, Some(locked_job_id))
}

fn kill_session_with_reason_inner(
    orch: &Orchestrator,
    run_id: &str,
    exit_reason: &str,
    terminals: TerminalPolicy,
    locked_job_id: Option<&str>,
) -> Result<(), String> {
    let job_id = job_id_for_run(orch, run_id);
    let launch_lock = job_id
        .as_deref()
        .filter(|id| Some(*id) != locked_job_id)
        .map(|id| orch.job_launch_lock(id));
    let guard = launch_lock
        .as_ref()
        .map(|lock| lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
    // A terminal completion may still be draining calls with its turn active.
    // Its claim owns durable disposition; a concurrent kill only ends transport.
    if orch.has_terminal_completion_claim(run_id) {
        drop(guard);
        return teardown_process(orch, run_id, terminals, exit_reason);
    }
    if exit_reason == RUNNER_SHUTDOWN_EXIT_REASON {
        // Persist the cause and the unfinished work BEFORE the interrupt can
        // produce a Result or EOF. Those frames describe transport cleanup, not
        // successful work or an operator Stop.
        run_db_blocking({
            let dbs = orch.db.clone();
            let run_id = run_id.to_string();
            move || async move {
                let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                    .await
                    .map_err(|error| error.to_string())?;
                db.write(|conn| {
                    let run_id = run_id.clone();
                    Box::pin(async move {
                        conn.execute(
                            "UPDATE runs SET exit_reason = ?2 WHERE id = ?1 AND status IN ('starting', 'live')",
                            (run_id.as_str(), RUNNER_SHUTDOWN_EXIT_REASON),
                        ).await?;
                        crate::runs::queries::settle_stranded_turns(
                            conn, &run_id, crate::runs::queries::StaleRunSettlement::HostRestart,
                            chrono::Utc::now().timestamp(),
                        ).await?;
                        Ok(())
                    })
                }).await.map_err(|error| error.to_string())
            }
        })?;
    }
    set_exit_reason(orch, run_id, exit_reason)?;
    drop(guard);
    teardown_process(orch, run_id, terminals, exit_reason)?;
    let guard = launch_lock
        .as_ref()
        .map(|lock| lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
    let completion_claimed = orch.has_terminal_completion_claim(run_id);
    drop(guard);
    if completion_claimed {
        return Ok(());
    }

    // Set exit reason and finalize as Exited (clean kill) or Crashed
    let final_status = if matches!(
        exit_reason,
        "crash" | PROVIDER_SILENCE_RECOVERY_EXIT_REASON | WATCHDOG_ARM_FAILED_EXIT_REASON
    ) {
        RunStatus::Crashed
    } else {
        RunStatus::Exited
    };

    if exit_reason == PROVIDER_SILENCE_RECOVERY_EXIT_REASON {
        super::finalize_run_for_recovery(orch, run_id, final_status)?;
    } else if exit_reason == OPERATOR_COMPACT_EXIT_REASON {
        super::finalize_run_for_operator_compact(orch, run_id, final_status, locked_job_id)?;
    } else {
        super::finalize::finalize_killed_run(
            orch,
            run_id,
            final_status,
            kill_cut_off_reason(exit_reason),
        );
    }

    Ok(())
}

/// How a turn a kill with `exit_reason` cut off mid-work ends, so a deliberate
/// kill never reads as a completion. `None` keeps finalization's own rule: a
/// kill that only ever ends a finished call or an idle warm process has no
/// running turn to cut off, and a crash (`crash`, `watchdog_arm_failed`) is a
/// real failure that finalizes `crash`. No deliberate kill maps to `crash`.
pub(crate) fn kill_cut_off_reason(exit_reason: &str) -> Option<TurnEndReason> {
    match exit_reason {
        PROVIDER_SILENCE_RECOVERY_EXIT_REASON => Some(TurnEndReason::WatchdogRecovery),
        // A provider change is the operator's edit; its stop settles the live
        // turn `user_stop` before the kill, and a turn started since ends so too.
        "user_stop" | "provider_change" => Some(TurnEndReason::UserStop),
        "issue_merged" => Some(TurnEndReason::IssueMerged),
        "issue_closed" => Some(TurnEndReason::IssueClosed),
        // Agrees with the boot sweep's writer, which settled the run's turns
        // `host_restart` before this kill began.
        RUNNER_SHUTDOWN_EXIT_REASON => Some(TurnEndReason::HostRestart),
        _ => None,
    }
}

/// The exit reason recorded for an agent stopped because its host is shutting
/// down. Distinguishes "we stopped it" from the blanket `crash` that startup
/// reconciliation would otherwise invent for the same row.
pub const RUNNER_SHUTDOWN_EXIT_REASON: &str = "runner_shutdown";

/// What a host's agent teardown accomplished: enough for the caller's one log
/// line, and for a test to assert on.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct HostShutdownStops {
    /// Agents stopped and finalized before the budget elapsed.
    pub stopped: usize,
    /// Agents whose stop reported an error, left to startup reconciliation.
    pub failed: usize,
    /// Agents still stopping when the budget elapsed. Their blocking work cannot
    /// be aborted, so it continues until the host process exits.
    pub timed_out: usize,
}

/// Stop every agent process this host spawned, before the host process exits.
///
/// Without this a runner abandons its agents (CAIRN-3287). A run handle's child
/// is an `Arc<Mutex<Option<Box<dyn ChildProcess>>>>` whose drop does not signal
/// the OS process, so an exiting runner leaves each agent reparented to
/// launchd/systemd and still running — while the reader thread that was
/// persisting its transcript dies with the runner. The agent then works on
/// against a wall: `dispatch_tool`'s ownership fence refuses its calls, but only
/// this stops the process from making them.
///
/// Each agent goes through [`kill_session_with_reason`] rather than a raw
/// `graceful_stop`, so its run and turn get the same bookkeeping any other stop
/// produces and the row records [`RUNNER_SHUTDOWN_EXIT_REASON`] instead of the
/// `crash` a later startup sweep would invent. In-flight work is settled
/// `host_restart` before signalling the provider, for the existing boot resume.
///
/// Latency is bounded because a successor host is waiting to bind the port:
/// `graceful_stop` alone spends up to ~3s per process on its SIGTERM poll, so the
/// stops run concurrently under one overall `budget` and the caller proceeds
/// regardless of what is still outstanding. Each stop is synchronous and blocks
/// on database work, so it runs on the blocking pool rather than an async worker.
pub async fn stop_agents_for_host_shutdown(
    orch: &Orchestrator,
    budget: std::time::Duration,
) -> HostShutdownStops {
    let mut outcome = HostShutdownStops::default();
    let run_ids = orch.process_state.run_ids();
    if run_ids.is_empty() {
        return outcome;
    }

    let mut stops = tokio::task::JoinSet::new();
    for run_id in run_ids {
        let orch = orch.clone();
        stops.spawn_blocking(move || {
            kill_session_with_reason(
                &orch,
                &run_id,
                RUNNER_SHUTDOWN_EXIT_REASON,
                TerminalPolicy::Finalize,
            )
            .map_err(|error| (run_id, error))
        });
    }

    let deadline = tokio::time::sleep(budget);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            joined = stops.join_next() => match joined {
                None => break,
                Some(Ok(Ok(()))) => outcome.stopped += 1,
                Some(Ok(Err((run_id, error)))) => {
                    outcome.failed += 1;
                    log::warn!("host shutdown could not stop agent for run {run_id}: {error}");
                }
                Some(Err(join_error)) => {
                    outcome.failed += 1;
                    log::warn!("host shutdown agent stop panicked: {join_error}");
                }
            },
            _ = &mut deadline => {
                outcome.timed_out = stops.len();
                break;
            }
        }
    }
    outcome
}
