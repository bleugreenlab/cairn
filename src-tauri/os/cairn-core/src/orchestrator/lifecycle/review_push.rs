//! Review-push creation across both trigger edges (work-turn idle and PR-open),
//! plus the turn-end attention/checks emitters shared by finalize and warm
//! transition. Sliced verbatim from the former `lifecycle.rs`.

use crate::orchestrator::attention_push::{Boundary, Wake};
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, DbError, RowExt};

use super::common::*;

/// Synchronously read `(issue_id, IssueAttentionContext)` from a job_id.
/// Returns None when the job has no issue (project-level jobs).
fn issue_for_attention_by_job(
    orch: &Orchestrator,
    job_id: &str,
) -> Option<(
    String,
    crate::orchestrator::attention::IssueAttentionContext,
)> {
    let issue_id = blocking_text_lookup(
        orch,
        job_id,
        "SELECT issue_id FROM jobs WHERE id = ?1",
        TextColumn::Optional,
    )?;
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
        let ctx = crate::orchestrator::attention::read_issue_for_attention(&db, &issue_id).await?;
        Ok(Some((issue_id, ctx)))
    })
    .ok()
    .flatten()
}

pub(crate) async fn record_bounded_rearm_lookup_failure(
    orch: &Orchestrator,
    job_id: &str,
    diagnostic: &str,
) -> Result<(), String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| error.to_string())?;
    let job_id = job_id.to_string();
    let output = format!("check wave preparation failed: {diagnostic}");
    db.write(move |conn| {
        let job_id = job_id.clone();
        let output = output.clone();
        Box::pin(async move {
            conn.execute(
                // Only INFRASTRUCTURE rows. `check_result_cache` holds two row
                // families, and the other one is the hot projection of an
                // immutable observation: repainting it would leave a row saying
                // `infrastructure` over a `source_observation_id` that says
                // `passed`, and would strip that green of reuse (the reusable
                // lookup requires `failure_kind IS NULL`) until some later run
                // healed it. Evidence is not ours to overwrite.
                //
                // Nothing is lost by the narrowing. This only ever runs for a job
                // that `bounded_rearm_candidates` already selected, and that query
                // selects on this same failure_kind set -- so the row carrying the
                // streak this statement advances is always in scope.
                "UPDATE check_result_cache
                    SET passed=0, exit_code=-1, failure_kind='infrastructure',
                        output_tail=?1, ran_at=(unixepoch() * 1000),
                        infra_failure_streak=MIN(infra_failure_streak + 1, ?2)
                  WHERE job_id=?3
                    AND failure_kind IN ('capacity', 'infrastructure')
                    AND NOT EXISTS (
                      SELECT 1 FROM check_result_cache newer
                       WHERE newer.job_id=check_result_cache.job_id
                         AND newer.check_name=check_result_cache.check_name
                         AND (newer.ran_at > check_result_cache.ran_at
                              OR (newer.ran_at = check_result_cache.ran_at
                                  AND newer.rowid > check_result_cache.rowid))
                    )",
                (
                    output.as_str(),
                    crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND,
                    job_id.as_str(),
                ),
            )
            .await?;
            Ok::<_, crate::storage::DbError>(())
        })
    })
    .await
    .map_err(|error| error.to_string())?;
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "check_result_cache", "action": "update"}),
    );
    Ok(())
}

#[derive(Debug)]
pub(super) struct BoundedRearmLookupError {
    job_id: Option<String>,
    message: String,
}

impl std::fmt::Display for BoundedRearmLookupError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

/// Emit the typed attention event for a turn that just terminalized.
/// - Issue reached a terminal status → `Resolved`
/// - Issue still needs the driver (attention != None) → `AgentIdleWithWork`
/// - Issue has an open PR work product while attention is None → `AgentIdleWithWork`
///   pointing at the producing builder's `/pr` (the freshly-opened-PR case the
///   attention projection deliberately leaves silent)
/// - Otherwise: no emit (the turn ended cleanly with no work left).
///
/// Fact construction is shared with the boundary wake (`wake_for_issue`) via
/// [`attention::idle_fact_for_issue`] so the two paths cannot diverge. The
/// terminalized `job_id` biases the open-PR lookup toward this builder's `/pr`.
pub(super) fn emit_for_turn_end(orch: &Orchestrator, job_id: &str) {
    let Some((issue_id, ctx)) = issue_for_attention_by_job(orch, job_id) else {
        return;
    };
    let issue_uri = ctx.issue_uri();
    // CAIRN-2483: this edge no longer creates the review push. Review firing is
    // gated on the whole issue being quiescent, with readiness re-evaluated from
    // turn-end checks completion, the job-terminal recompute hook, and the PR-open
    // edge. This function keeps only its idle-fact / `AttentionEvent`
    // half, which drives `cairn watch`.
    //
    // Resolve the fact (and any detail URI) synchronously via the shared helper.
    let dbs = orch.db.clone();
    let issue_id_for_fact = issue_id.clone();
    let ctx_for_fact = ctx.clone();
    let job_id_owned = job_id.to_string();
    let idle = run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id_owned)
            .await
            .map_err(|e| e.to_string())?;
        Ok::<_, String>(
            crate::orchestrator::attention::idle_fact_for_issue(
                &db,
                &issue_id_for_fact,
                &ctx_for_fact,
                Some(&job_id_owned),
            )
            .await,
        )
    })
    .ok()
    .flatten();
    let Some(idle) = idle else {
        return;
    };
    orch.emit_attention_event(crate::orchestrator::AttentionEvent {
        issue_id,
        issue_uri,
        fact: idle.fact,
        attention: ctx.attention,
        status: ctx.status,
        updated_at: idle.updated_at,
        route_provenance: None,
    });
}

/// Synchronously answer whether this job's work ships a pull request, from the
/// execution's recipe topology (`node_ships_a_pr` in `execution::jobs::inputs`,
/// which owns the derivation).
///
/// A job with no recipe node of its own — every delegated sub-agent task, whose
/// materialized node carries an inbound context edge and no outbound one — is a
/// definitive `false` decided by the job row alone, so the dominant skip costs
/// one small query and never loads a snapshot. `Err` means the topology could
/// not be read at all; the caller decides what to do with that.
fn job_ships_a_pr(orch: &Orchestrator, job_id: &str) -> Result<bool, String> {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT recipe_node_id, execution_id FROM jobs WHERE id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(false);
                };
                let (Some(node_id), Some(execution_id)) = (row.opt_text(0)?, row.opt_text(1)?)
                else {
                    return Ok(false);
                };
                crate::execution::jobs::node_ships_a_pr_conn(conn, &node_id, &execution_id).await
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

/// Fire the turn-end (`when:review`) project checks for a job whose turn just
/// ended, detached onto a background task so the minutes-long suite never blocks
/// the turn from ending. Runs UNSANDBOXED in the background; on any check
/// failure it resumes the idle builder with the failure inlined. Invoked at BOTH
/// turn-end callers (`finalize_run` and `transition_to_warm_state`) so the two
/// stay mirrored.
///
/// Four guards skip it: a job whose work never ships a PR (the topology gate
/// below), a trailing memory-review turn (not a work turn), a job still mid-work
/// rather than at a real turn end (CAIRN-3154, see the gate below), and a run
/// already in flight for the job (single-flight). A turn end refused by
/// single-flight is not dropped: the running wave owes the job one successor,
/// armed when it releases if the head has moved past the commit it checked.
pub(crate) fn spawn_turn_end_checks(orch: &Orchestrator, job_id: &str) {
    arm_review_wave(orch, job_id, WaveTrigger::TurnEnd);
}

/// Arm a review wave for a job from an edge that is NOT one of its turn ends:
/// someone waiting on its checks, the boot re-arm, the bounded infrastructure
/// re-arm, or a released wave's owed successor. Returns whether one was asked
/// for.
///
/// A wave already in flight is left alone, and owes nothing for it. Its own
/// release arms whatever successor its job is owed, so these edges never arm a
/// wave for a job whose slot is held.
pub(crate) fn spawn_review_wave_unless_in_flight(orch: &Orchestrator, job_id: &str) -> bool {
    if orch.turn_end_checks_in_flight(job_id) {
        log::debug!(
            "review wave for job {}: one is already in flight; its release arms any successor",
            &job_id[..job_id.len().min(8)]
        );
        return false;
    }
    arm_review_wave(orch, job_id, WaveTrigger::Recovery);
    true
}

/// Which edge asked for a wave, which decides what a refused claim means.
#[derive(Clone, Copy, PartialEq, Eq)]
enum WaveTrigger {
    /// A turn end. Refused by single-flight, it leaves the holder owing one
    /// successor.
    TurnEnd,
    /// Any other edge. Refused by single-flight, it owes nothing.
    Recovery,
}

/// Arm a released wave's successor immediately. The ordinary gates still apply.
pub(crate) fn arm_turn_end_successor_now(orch: &Orchestrator, job_id: &str) {
    log::info!(
        "turn-end checks for job {}: arming the successor a released wave owed",
        &job_id[..job_id.len().min(8)]
    );
    spawn_review_wave_unless_in_flight(orch, job_id);
}

/// Arm whatever successor a released wave owes its job.
///
/// The one place a successor is armed, so a wave cancelled because its tree went
/// stale and a wave that refused a later turn end are replaced by the same rule.
/// An owed successor is armed only if the job's head moved past the commit the
/// wave sealed: a turn end that changed nothing was answered by the wave that
/// refused it. A head that cannot be resolved arms one, because skipping it
/// could drop the review gate on the head the agent actually ended on.
///
/// Every ordinary gate still applies inside the arm: a job that ships no PR, a
/// memory-review turn, a resolved issue, and a live turn all arm nothing. A live
/// turn's own end arms its wave. A dormant PR owner has no future turn-end edge,
/// so a wave it had in flight is replaced here; a dormant owner with NO wave in
/// flight is armed when someone waits on its checks
/// (`checks_settlement::arm_wave_for_waiter`).
pub(crate) async fn arm_turn_end_successor(
    orch: &Orchestrator,
    job_id: &str,
    successor: crate::orchestrator::TurnEndSuccessor,
) {
    use crate::orchestrator::TurnEndSuccessor;
    match successor {
        TurnEndSuccessor::None => {}
        TurnEndSuccessor::Arm => arm_turn_end_successor_now(orch, job_id),
        TurnEndSuccessor::IfHeadMoved { sealed } => {
            match crate::execution::cache::resolve_job_logical_head(orch, job_id).await {
                Ok(head) if head == sealed => log::debug!(
                    "turn-end checks for job {}: the owed turn end left the head at {}, which the \
                     released wave checked; no successor",
                    &job_id[..job_id.len().min(8)],
                    &sealed[..sealed.len().min(12)]
                ),
                Ok(_) => arm_turn_end_successor_now(orch, job_id),
                Err(error) => {
                    log::warn!(
                        "turn-end checks for job {}: head unresolved after an owed turn end \
                         ({error:?}); arming the successor rather than risk dropping its review",
                        &job_id[..job_id.len().min(8)]
                    );
                    arm_turn_end_successor_now(orch, job_id);
                }
            }
        }
    }
}

/// The reason an attempt refused by single-flight records when it leaves the
/// holder owing a successor.
pub(crate) const OWED_SUCCESSOR_REASON: &str =
    "a wave for an earlier head is running; one successor is owed";

fn arm_review_wave(orch: &Orchestrator, job_id: &str, trigger: WaveTrigger) {
    let short = &job_id[..job_id.len().min(8)];
    let attempt_id = match crate::execution::checks_turn_end::request_turn_end_attempt(orch, job_id)
    {
        Ok(id) => id,
        Err(error) => {
            log::warn!("turn-end checks for job {short}: could not persist arm intent ({error})");
            return;
        }
    };
    let terminal = |state: &str, reason: &str| {
        if let Err(error) = crate::execution::checks_turn_end::transition_turn_end_attempt(
            orch,
            job_id,
            &attempt_id,
            state,
            Some(reason),
        ) {
            log::error!(
                "turn-end checks for job {short}: failed to persist terminal {state}: {error}"
            );
        }
    };
    // The review cadence exists to validate the tree a human will review, so it
    // is scoped to the jobs that produce one: those whose recipe node feeds a
    // `pr` node (CAIRN-3334). Without this, every delegated sub-agent's turn end
    // multiplied the heaviest exclusive suites across intermediate trees nobody
    // gates on — minutes of contended machine time per sub-agent turn, and a
    // cache full of verdicts for trees that are never reviewed.
    match job_ships_a_pr(orch, job_id) {
        Ok(true) => {}
        Ok(false) => {
            terminal("skipped", "no downstream PR node in the execution topology");
            log::debug!(
                "turn-end checks for job {short}: skipped, no downstream PR node in the execution topology"
            );
            return;
        }
        // Unresolvable topology is an infrastructure fault, not a verdict. Running
        // the suite wastes machine time; skipping it would silently drop the
        // review gate on a shipping branch, so this fails open and says why.
        Err(error) => log::warn!(
            "turn-end checks for job {short}: PR topology unresolved ({error}); running the cadence anyway"
        ),
    }
    if latest_turn_is_memory_review(orch, job_id) {
        terminal("skipped", "latest turn is a memory-review turn");
        log::debug!(
            "turn-end checks for job {short}: skipped, latest turn is a memory-review turn (not a work turn)"
        );
        return;
    }
    // Only a turn that actually ENDED earns a review wave. Two ways this hook
    // fires for a job that is still mid-work (CAIRN-3154):
    //
    // - Self-suspended. `waitFor`, a `run` batch that outlived its inline grace
    //   window, and a blocking question/task append all park the agent by
    //   YIELDING its turn, and the park reaches this hook looking like an idle.
    //   It is not one: the agent resumes when its own pending work resolves, and
    //   launching the heavy review lanes here put them in contention with the
    //   very tests the agent was suspended waiting on.
    // - Already resumed. The suspension's interrupt ack is asynchronous, so it
    //   can land after the wait resolved and the continuation turn started. A
    //   wave launched there would check the pre-resume tree AND hold the
    //   single-flight slot against the continuation's own real turn-end.
    //
    // A skipped wave is not owed: nothing is queued for replay, and the job's
    // next real turn-end runs one suite against the tree that exists then. The
    // classification is the canonical one message delivery already uses to
    // decline resuming a self-suspended agent, so the two cannot drift.
    if let Some(reason) =
        crate::messages::delivery::head_turn_for_job(orch, job_id).mid_work_reason()
    {
        terminal("skipped", reason);
        log::debug!("turn-end checks for job {short}: skipped, {reason}");
        return;
    }
    // The same launchability decision the runner takes again immediately before
    // submission, asked here so a wave for a resolved issue never claims the
    // single-flight slot or spends minutes planning against a tree nobody will
    // review. This is a gate on LIVE turn-end arming only: readiness recovery
    // re-evaluates settled jobs through `evaluate_review_readiness`, which has
    // its own origin and is not routed through here.
    if let Some(withdrawal) = withdrawn_turn_end_wave(orch, job_id) {
        terminal("withdrawn", &withdrawal);
        log::debug!("turn-end checks for job {short}: skipped, {withdrawal}");
        return;
    }
    // Claim the single-flight slot. The guard is the lever a later merge/close
    // pulls to quit this suite mid-flight (CAIRN-2648), and the only thing that
    // releases the slot. A turn end refused here leaves the holder owing one
    // successor; the loop covers the holder releasing between the refusal and
    // the debt being recorded, so a refused turn end is never silently lost.
    let mut claim = orch.try_begin_turn_end_checks(job_id, &attempt_id);
    for _ in 0..2 {
        if claim.is_ok() {
            break;
        }
        match trigger {
            WaveTrigger::TurnEnd if orch.owe_turn_end_successor(job_id) => {
                terminal("superseded", OWED_SUCCESSOR_REASON);
                log::debug!(
                    "turn-end checks for job {short}: a run is already in flight; it owes one successor"
                );
                return;
            }
            WaveTrigger::TurnEnd => {
                claim = orch.try_begin_turn_end_checks(job_id, &attempt_id);
            }
            WaveTrigger::Recovery => break,
        }
    }
    let slot = match claim {
        Ok(slot) => slot,
        Err(_) => {
            terminal(
                "superseded",
                "another turn-end check attempt already owns the slot",
            );
            log::debug!("turn-end checks for job {short}: skipped, a run is already in flight");
            return;
        }
    };
    if let Err(error) = crate::execution::checks_turn_end::transition_turn_end_attempt(
        orch,
        job_id,
        &attempt_id,
        "claimed",
        None,
    ) {
        // Dropping the guard records the attempt failed and frees the slot.
        log::error!("turn-end checks for job {short}: failed to persist claim: {error}");
        drop(slot);
        return;
    }
    let orch_clone = orch.clone();
    let job_id_owned = job_id.to_string();
    let orch_for_failure = orch.clone();
    let job_id_for_failure = job_id.to_string();
    let attempt_for_failure = attempt_id.clone();
    // On the orchestrator's runtime, not one built for this wave: the wave opens
    // shared executions and releases check refs whose sweeps must outlive it,
    // and a cancelled wave returning early would otherwise drop them with it.
    // Cancellation itself is unchanged; the slot's cancel handle stops this
    // wave's own lanes.
    orch.spawn_detached_or(
        crate::execution::checks_turn_end::run_turn_end_checks(orch_clone, job_id_owned, slot),
        move || {
            // No runtime could be had, so the wave never ran. Record the specific
            // reason; the guard inside the dropped future then releases the slot
            // without overwriting it.
            if let Err(error) = crate::execution::checks_turn_end::transition_turn_end_attempt(
                &orch_for_failure,
                &job_id_for_failure,
                &attempt_for_failure,
                "failed",
                Some("detached runtime construction failed"),
            ) {
                log::error!(
                    "turn-end checks for job {}: detached-runtime failure was not persisted: {}",
                    &job_id_for_failure[..job_id_for_failure.len().min(8)],
                    error
                );
            }
        },
    );
}

/// Why this job's turn-end wave must not be armed, if it must not be.
///
/// The synchronous face of [`crate::execution::checks_turn_end::turn_end_launchability`],
/// which is the single place the decision is made. This hook runs on an agent
/// backend's plain stdout thread with no ambient runtime, so the async guard is
/// driven the same way the PR-topology gate above drives its query.
fn withdrawn_turn_end_wave(orch: &Orchestrator, job_id: &str) -> Option<String> {
    use crate::execution::checks_turn_end::{turn_end_launchability, WaveLaunchability};
    let orch = orch.clone();
    let job_id = job_id.to_string();
    let launchability = run_db_blocking(move || async move {
        Ok::<_, String>(turn_end_launchability(&orch, &job_id).await)
    });
    match launchability {
        Ok(WaveLaunchability::Withdrawn(withdrawal)) => Some(withdrawal.describe()),
        // Ready, or a guard that could not be driven at all. Both arm the wave:
        // the runner asks again before it submits anything, so failing open here
        // costs planning time rather than a dropped review gate.
        _ => None,
    }
}

/// The turn-end half of the checks-settled edge (CAIRN-3437).
///
/// Must be called immediately AFTER [`spawn_turn_end_checks`], never before: a
/// wave that is going to run claims its single-flight slot inside that call, and
/// asking settlement first would see an idle node with no wave in flight and
/// report every lane about to run as one that never will. Detached for the same
/// reason the wave itself is -- this hook runs on an agent backend's plain
/// stdout thread with no ambient runtime, and the settlement snapshot reads the
/// repository.
pub(crate) fn spawn_checks_settled_edge(orch: &Orchestrator, job_id: &str) {
    let edge_orch = orch.clone();
    let job_id = job_id.to_string();
    orch.spawn_detached(async move {
        crate::orchestrator::wakes::route_checks_settled_edge(&edge_orch, &job_id).await;
    });
}

/// The shared review-readiness evaluator (CAIRN-2483). A review fires when
/// **(reviewable output exists) AND (issue quiescent)**, at the moment the last
/// of those becomes true. Multiple trigger edges (turn-end checks completion, the
/// job-terminal recompute hook, and the PR-open edge) may evaluate at different
/// moments; the per-watcher fingerprint dedupe inside [`create_review_push_rows`]
/// guarantees at most one wake per reviewed state.
///
/// Detached turn-end checks are feedback for the producing child, not semantic
/// child liveness. They deliberately do not participate in this gate: a slow or
/// stranded advisory suite must not suppress the coordinator's durable wake.
pub async fn evaluate_review_readiness(orch: &Orchestrator, issue_id: &str) {
    let db = match crate::issues::crud::owning_db_for_issue(&orch.db, issue_id).await {
        Ok(db) => db,
        Err(e) => {
            log::warn!("review readiness: issue db resolve failed: {e}");
            return;
        }
    };
    // 1. Reviewable output exists — resolve the producing job that owns it.
    let producing_job_id = match resolve_producing_job_for_issue(&db, issue_id).await {
        Ok(Some(job_id)) => job_id,
        Ok(None) => return,
        Err(e) => {
            log::warn!("review readiness: producing-job lookup failed: {e}");
            return;
        }
    };
    // 2. The whole issue must be quiescent (no imminent agent/action work). This
    //    liveness check *includes* a trailing memory-review turn, so a review is
    //    never fired mid-reflection; once the reflection turn terminalizes the
    //    issue settles and the review fires normally (there is deliberately no
    //    separate "latest turn is memory_review" gate: a builder's latest turn is
    //    permanently its memory-review turn, so gating on it would block the
    //    review forever — CAIRN-2483).
    match crate::execution::advancement::issue_settled(&db, issue_id).await {
        Ok(true) => {}
        Ok(false) => return,
        Err(e) => {
            log::warn!("review readiness: issue_settled failed: {e}");
            return;
        }
    }
    // 3. Resolve the issue context and create the deduped review push rows.
    let ctx = match crate::orchestrator::attention::read_issue_for_attention(&db, issue_id).await {
        Ok(ctx) => ctx,
        Err(e) => {
            log::warn!("review readiness: issue context failed: {e}");
            return;
        }
    };
    match create_review_push_rows(&db, &producing_job_id, issue_id, &ctx).await {
        Ok(recipients) => {
            orch.notifier.emit_change("attention_pushes");
            wake_review_recipients(orch, &recipients);
        }
        Err(e) => log::warn!(
            "review push creation for job {} failed: {}",
            &producing_job_id[..producing_job_id.len().min(8)],
            e
        ),
    }
}

/// SQL predicate over an `artifacts a` row: the row is reviewable output.
///
/// A `create-pr` artifact is reviewable whether or not it is confirmed (see
/// [`review_artifact_ref`]); a `plan` is reviewable only while it awaits
/// confirmation. Either way the row must be the head of its version chain.
/// Every version is its own row, `version` is scoped per `(job_id, output_name)`,
/// and confirmation flips only the version that was confirmed, so the versions
/// a confirmed plan superseded keep `confirmed = 0` forever. Testing
/// `confirmed = 0` on any row rather than the chain head would keep a confirmed
/// plan reviewable for the life of its issue, and each superseded version's
/// fingerprint would read as a changed reviewable state (cairn/4891).
///
/// The single home of the predicate: "which job owns reviewable output", "which
/// jobs re-arm at startup", and "what is reviewable" all splice it in, so they
/// cannot disagree.
const REVIEWABLE_ARTIFACT_PREDICATE: &str = "(a.artifact_type = 'create-pr'
          OR (a.artifact_type = 'plan' AND a.confirmed = 0))
     AND NOT EXISTS (
       SELECT 1 FROM artifacts newer
       WHERE newer.job_id = a.job_id
         AND newer.output_name IS a.output_name
         AND newer.version > a.version
     )";

/// Resolve the job that owns an issue's reviewable output — a create-pr /
/// unconfirmed-plan artifact, or an open PR on the job's own branch. Mirrors the
/// producing-builder resolution ordering of [`find_producing_builder_job`] but is
/// issue-scoped (no branch given): the pr-action node writes no reviewable
/// artifact, so the artifact/worktree ordering excludes it. `None` when the issue
/// has no reviewable output at all.
async fn resolve_producing_job_for_issue(
    db: &crate::storage::LocalDb,
    issue_id: &str,
) -> Result<Option<String>, String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!(
                        "SELECT j.id FROM jobs j
                     WHERE j.issue_id = ?1
                       AND (
                         EXISTS (
                           SELECT 1 FROM artifacts a
                           WHERE a.job_id = j.id
                             AND {REVIEWABLE_ARTIFACT_PREDICATE}
                         )
                         OR EXISTS (
                           SELECT 1 FROM merge_requests mr
                           WHERE mr.issue_id = ?1
                             AND mr.source_branch = j.branch
                             AND mr.status = 'open'
                         )
                       )
                     ORDER BY
                       CASE WHEN EXISTS (
                         SELECT 1 FROM artifacts a
                         WHERE a.job_id = j.id
                           AND a.artifact_type IN ('create-pr', 'plan')
                       ) THEN 0 ELSE 1 END,
                       j.created_at DESC
                     LIMIT 1"
                    ),
                    (issue_id.as_str(),),
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok::<_, DbError>(Some(row.text(0)?)),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// How many startup re-arm evaluations run at once. Enough that a runner with
/// dozens of settled jobs does not spend minutes on them one after another, and
/// few enough that the boot, when every executor is reattaching, is not handed
/// a burst of waves (cairn/5357 comment 1: 69 waves serially over 194 s).
const STARTUP_REARM_PARALLELISM: usize = 4;

/// Startup re-arm (CAIRN-2483). A restart loses every in-memory edge that would
/// otherwise have created a parent's review wake, so startup re-derives it for
/// each settled job on a non-terminal issue that still has reviewable output.
///
/// Two things happen, and the order matters. The review wake is evaluated
/// DIRECTLY for every candidate's issue, because it is the durable half and must
/// not depend on the advisory check cadence: a candidate whose branch ships no PR
/// (a planner parked at its plan gate) is skipped by that cadence entirely, and
/// routing its wake through the cadence is exactly the coupling that let plan
/// gates strand (CAIRN-3347).
///
/// Turn-end check waves are then re-spawned only where one is owed, because a
/// restart is not a fact about any lane (cairn/5357). One is owed when the job's
/// current wave was interrupted by the restart, which
/// [`reconcile_turn_end_attempts_on_startup`] records by failing the attempt
/// with [`RESTART_INTERRUPTED_REASON`] before this runs, or when a dormant
/// failed lane is due by the same fact rule the maintenance cadence applies
/// ([`bounded_candidate_is_due`]). An attempt still in a moving state belongs
/// to THIS process (a wave the reconciliation could not have seen, or one that
/// is hung, cairn/4690's case) and is never re-spawned here. No suppression is
/// reset: a repaired machine moves its own environment identity, which is the
/// repair a restart used to stand in for. Called once per boot, as
/// post-readiness background maintenance (CAIRN-3382), with its evaluations run
/// [`STARTUP_REARM_PARALLELISM`] at a time.
///
/// Returns the number of turn-end check waves re-spawned, so the caller can log
/// what this boot actually put on the executor.
///
/// IDEMPOTENT ACROSS BOOTS, and that is load-bearing now that it runs in the
/// background: an interrupted re-arm leaves the work partially done until the
/// next boot re-runs it, which is harmless precisely because every step here —
/// re-deriving a review wake, re-spawning a wave a restart interrupted or a fact
/// made due — converges on the same state when repeated. An edit that makes any
/// of it order- or once-dependent breaks that contract and must move it back
/// onto the blocking path.
///
/// [`reconcile_turn_end_attempts_on_startup`]: crate::execution::checks_turn_end::reconcile_turn_end_attempts_on_startup
/// [`RESTART_INTERRUPTED_REASON`]: crate::execution::checks_turn_end::RESTART_INTERRUPTED_REASON
pub async fn rearm_review_checks_on_startup(orch: &Orchestrator) -> usize {
    let candidates = match rearm_candidate_jobs(&orch.db.local).await {
        Ok(jobs) => jobs,
        Err(e) => {
            log::warn!("review-checks startup re-arm: candidate lookup failed: {e}");
            return 0;
        }
    };
    let mut issues: Vec<&str> = candidates
        .iter()
        .map(|(_, issue_id)| issue_id.as_str())
        .collect();
    issues.sort_unstable();
    issues.dedup();
    // Bounded by chunk rather than by a stream adaptor: the adaptors' closures
    // over borrowed items make this future unspawnable, and the caller spawns it.
    for chunk in issues.chunks(STARTUP_REARM_PARALLELISM) {
        futures_util::future::join_all(
            chunk
                .iter()
                .map(|issue_id| evaluate_review_readiness(orch, issue_id)),
        )
        .await;
    }

    let mut interrupted: Vec<&str> = Vec::new();
    for chunk in candidates.chunks(STARTUP_REARM_PARALLELISM) {
        let owed = futures_util::future::join_all(
            chunk
                .iter()
                .map(|(job_id, _)| wave_interrupted_by_restart(orch, job_id)),
        )
        .await;
        interrupted.extend(
            chunk
                .iter()
                .zip(owed)
                .filter(|(_, owed)| *owed)
                .map(|((job_id, _), _)| job_id.as_str()),
        );
    }
    let dormant = bounded_rearm_candidates(&orch.db)
        .await
        .unwrap_or_else(|error| {
            log::warn!("review-checks startup re-arm: dormant lane lookup failed: {error}");
            Vec::new()
        });
    let mut due: Vec<&BoundedRearmCandidate> = Vec::new();
    for chunk in dormant.chunks(STARTUP_REARM_PARALLELISM) {
        let answers = futures_util::future::join_all(
            chunk
                .iter()
                .map(|candidate| bounded_candidate_is_due(orch, candidate)),
        )
        .await;
        for (candidate, answer) in chunk.iter().zip(answers) {
            match answer {
                Ok(true) => due.push(candidate),
                Ok(false) => {}
                Err(error) => log::warn!(
                    "review-checks startup re-arm: dormant lane {} of job {} could not be \
                     evaluated: {error}",
                    candidate.check_name,
                    short_job(&candidate.job_id)
                ),
            }
        }
    }
    let interrupted_count = interrupted.len();
    let mut waves: Vec<&str> = interrupted;
    waves.extend(due.iter().map(|candidate| candidate.job_id.as_str()));
    waves.sort_unstable();
    waves.dedup();
    // A held slot already has a wave behind it (cairn/4690), so it is left out
    // before any counter is released for it.
    waves.retain(|job_id| !orch.turn_end_checks_in_flight(job_id));
    for candidate in due
        .iter()
        .filter(|candidate| waves.contains(&candidate.job_id.as_str()))
    {
        if let Err(error) =
            release_head_suppression_on_fact_change(orch, &candidate.job_id, &candidate.check_name)
                .await
        {
            log::warn!(
                "review-checks startup re-arm: could not release the head counter of {} for job \
                 {}: {error}",
                candidate.check_name,
                short_job(&candidate.job_id)
            );
        }
    }
    // A recovery edge: it never marks a held slot owed, even if a wave claims
    // one between the filter above and here.
    for job_id in &waves {
        spawn_review_wave_unless_in_flight(orch, job_id);
    }
    log::info!(
        "review-checks startup re-arm: re-derived the review wake for {} settled job(s); \
         re-spawned {} turn-end wave(s) for {interrupted_count} wave(s) the restart \
         interrupted and {} dormant lane(s) due on a changed fact",
        candidates.len(),
        waves.len(),
        due.len()
    );
    waves.len()
}

fn short_job(job_id: &str) -> &str {
    &job_id[..job_id.len().min(8)]
}

/// Whether the job's current turn-end wave was cut off by the restart: the
/// startup reconciliation failed it with the restart's reason because the
/// process that owned it is gone.
async fn wave_interrupted_by_restart(orch: &Orchestrator, job_id: &str) -> bool {
    let Ok(db) = crate::execution::routing::owning_db_for_job(&orch.db, job_id).await else {
        return false;
    };
    matches!(
        crate::execution::checks_turn_end::current_turn_end_attempt(&db, job_id).await,
        Ok(Some(attempt))
            if attempt.state == "failed"
                && attempt.reason.as_deref()
                    == Some(crate::execution::checks_turn_end::RESTART_INTERRUPTED_REASON)
    )
}

/// Re-arm at most one dormant review wave whose lane failed without a verdict,
/// once the fact that failed it has changed. The one-wave limit is deliberate
/// pacing: a fleet change can make many dormant lanes due at once, and
/// releasing all of them would recreate the contention that stranded them.
///
/// Time alone never makes a lane due (cairn/5357): the cooldown only spaces
/// how often the question is asked, and [`bounded_candidate_is_due`] answers it
/// from facts. Only once it says yes is the head counter of that one lane
/// released, so the wave it receives can execute it.
pub async fn rearm_one_bounded_failed_review(orch: &Orchestrator) -> bool {
    let Some(candidate) = (match bounded_rearm_candidate(orch).await {
        Ok(candidate) => candidate,
        Err(error) => {
            log::warn!("review-checks bounded re-arm: candidate lookup failed: {error}");
            let Some(job_id) = error.job_id.as_deref() else {
                return false;
            };
            if let Err(record_error) =
                record_bounded_rearm_lookup_failure(orch, job_id, &error.message).await
            {
                log::warn!(
                    "review-checks bounded re-arm: failed to surface candidate lookup failure: {record_error}"
                );
            }
            return false;
        }
    }) else {
        return false;
    };

    log::info!(
        "review-checks bounded re-arm: re-spawning one dormant wave for job {}: the fact that \
         failed its {} lane ({}) has changed",
        short_job(&candidate.job_id),
        candidate.check_name,
        candidate.failure_kind
    );
    if let Err(error) =
        release_head_suppression_on_fact_change(orch, &candidate.job_id, &candidate.check_name)
            .await
    {
        log::warn!(
            "review-checks bounded re-arm: failed to release the head counter of {} for job {}: \
             {error}",
            candidate.check_name,
            short_job(&candidate.job_id)
        );
        return false;
    }
    spawn_review_wave_unless_in_flight(orch, &candidate.job_id)
}

/// How long a dormant lane's newest row must have stood before the maintenance
/// cadence asks whether it is due. It spaces the question; it never answers it.
pub(super) const DORMANT_INFRA_RETRY_COOLDOWN_SECONDS: i64 = 5 * 60;

/// Zero the head-domain counters of one dormant lane, so the wave a changed
/// fact just made due can execute it. Called only after
/// [`bounded_candidate_is_due`] has said yes.
///
/// Only the head forms are released: they are the one counter no fact resets
/// on a dormant head, because their repair is a new commit. A machine's
/// environment counter and the fleet counter are reset by their own repairs,
/// which mint fresh scopes at zero, so a timer or a re-arm never touches them
/// (cairn/4983).
pub(super) async fn release_head_suppression_on_fact_change(
    orch: &Orchestrator,
    job_id: &str,
    check_name: &str,
) -> Result<(), String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| error.to_string())?;
    db.execute(
        "UPDATE check_result_cache
            SET infra_failure_streak = 0, infra_escalated_at = NULL
          WHERE job_id = ?1 AND check_name = ?2
            AND failure_kind IN ('capacity', 'infrastructure')
            AND infra_failure_streak > 0
            AND environment_fingerprint NOT LIKE ?3
            AND environment_fingerprint NOT LIKE ?4",
        (
            job_id,
            check_name,
            format!("{}%", crate::execution::cache::INFRA_FLEET_SCOPE_PREFIX),
            format!("{}%", crate::execution::cache::infra_env_scope("")),
        ),
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct BoundedRearmCandidate {
    pub(super) job_id: String,
    pub(super) tree_hash: String,
    pub(super) input_hash: String,
    pub(super) ran_at: i64,
    pub(super) check_name: String,
    pub(super) failure_kind: String,
    /// The scope the newest row was written under. For a death charged to a
    /// fleet counter this is that counter's scope, which is how the cadence
    /// tells that this head already met the fleet it would meet again.
    pub(super) environment_fingerprint: String,
}

pub(super) async fn bounded_rearm_candidate(
    orch: &Orchestrator,
) -> Result<Option<BoundedRearmCandidate>, BoundedRearmLookupError> {
    let candidates = bounded_rearm_candidates(&orch.db)
        .await
        .map_err(|message| BoundedRearmLookupError {
            job_id: None,
            message,
        })?;
    for candidate in candidates {
        // A job whose slot is held has a wave running; its successor is the
        // slot release's to arm (cairn/4690), never this cadence's.
        if orch.turn_end_checks_in_flight(&candidate.job_id) {
            continue;
        }
        let matches = bounded_candidate_is_due(orch, &candidate)
            .await
            .map_err(|message| BoundedRearmLookupError {
                job_id: Some(candidate.job_id.clone()),
                message,
            })?;
        if matches {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

pub(super) async fn bounded_rearm_candidates(
    dbs: &crate::db::DbState,
) -> Result<Vec<BoundedRearmCandidate>, String> {
    let mut candidates = Vec::new();
    for db in dbs.all_dbs().await {
        candidates.extend(bounded_rearm_candidates_in_db(&db).await?);
    }
    candidates.sort_by(|left, right| {
        left.ran_at
            .cmp(&right.ran_at)
            .then_with(|| left.job_id.cmp(&right.job_id))
    });
    Ok(candidates)
}

/// The dormant-review candidate set, re-read on every `BOUNDED_REARM_INTERVAL`
/// tick. Named so the regression test can plan the statement that actually runs.
///
/// What this read costs has to follow the number of CANDIDATES. It used to
/// follow the check HISTORY: the statement drove off `check_result_cache`, so
/// every cached result on the install was visited, and each of the ones carrying
/// a `capacity` or `infrastructure` failure -- on a production runner, ~12k of
/// ~34k rows, almost all belonging to issues that closed months ago --
/// separately paid for a newest-row-per-(job, check) anti-join and two sorted
/// turn lookups. That is a 17-second query re-run every 30 seconds to find
/// single-digit candidates, and it gets worse with every check ever recorded
/// (CAIRN-4196).
///
/// The fix has two halves, and both are needed.
///
/// First, it drives from live work. `live_job` is the jobs under an unresolved
/// issue -- 187 of 14,639 jobs in that same production database -- and each one
/// seeks only its own cache rows through `idx_check_result_cache_job`. That
/// excludes the history belonging to resolved issues, which is most of it.
///
/// Second, the newest row per (job, check) is chosen by RANKING that narrowed
/// set once, not by asking per row whether a newer row exists. Narrowing alone
/// would not have been enough: a long-lived unresolved issue accumulates a row
/// per input hash per check indefinitely, and an anti-join pays for that history
/// once per row of it, so the quadratic term would simply have moved from the
/// whole cache to one busy job's slice of it. `ROW_NUMBER()` reads each live
/// job's rows exactly once. The plan test asserts that single read directly,
/// because an indexed range walk that grows from tens of rows to thousands looks
/// identical to a healthy one if you only assert SEARCH rather than SCAN.
///
/// Note the order the filters run in: the ranking sees ALL of a live job's rows,
/// and `failure_kind` is applied after `recency = 1`. A candidate is the current
/// result for its check AND an infrastructure failure -- filtering first would
/// promote a superseded failure hiding behind a newer pass.
///
/// State the bound honestly, because an overclaimed invariant is worse than a
/// named limit: this is ONE linear pass over the cache rows of jobs under
/// unresolved issues, plus a bounded number of index seeks per surviving row. It
/// is not proportional to the returned candidates alone -- a live job's own
/// history is still read, once. What it no longer does is read anything twice,
/// or read the resolved-issue history that is the bulk of the table.
///
/// The `CROSS JOIN` spellings are load-bearing. Turso keeps no table statistics,
/// so it cannot know that `live_job` is the small side; given a plain `JOIN` it
/// picks the opposite order and goes straight back to scanning the whole cache.
/// `CROSS JOIN` is how the intended order is stated, and
/// `bounded_rearm_candidates_never_walk_the_check_history` pins the result --
/// the plan being the only seam this regression is visible at, since both shapes
/// are one statement returning the same rows.
///
/// One scan remains, over `issues`, and it is the intended floor: `status NOT IN
/// (...)` is not a predicate any index seeks, and the set of unresolved issues is
/// the definition of live work. It is a single pass of cheap comparisons per
/// tick, not per candidate.
pub(super) const BOUNDED_REARM_CANDIDATES_SQL: &str = "WITH live_job AS (
                        SELECT j.id AS job_id, j.issue_id AS issue_id, j.branch AS branch
                          FROM issues i
                         CROSS JOIN jobs j ON j.issue_id = i.id
                         WHERE i.status NOT IN ('merged', 'closed')
                      ),
                      latest AS (
                        SELECT lj.job_id AS job_id, lj.issue_id AS issue_id,
                               lj.branch AS branch, c.tree_hash AS tree_hash,
                               c.input_hash AS input_hash, c.check_name AS check_name,
                               c.ran_at AS ran_at, c.failure_kind AS failure_kind,
                               COALESCE(c.environment_fingerprint, '') AS environment_fingerprint,
                               ROW_NUMBER() OVER (
                                 PARTITION BY c.job_id, c.check_name
                                 ORDER BY c.ran_at DESC, c.rowid DESC
                               ) AS recency
                          FROM live_job lj
                         CROSS JOIN check_result_cache c ON c.job_id = lj.job_id
                      )
                      SELECT job_id, tree_hash, input_hash, ran_at, check_name, failure_kind,
                             environment_fingerprint
                        FROM latest
                       WHERE recency = 1
                         AND failure_kind IN ('capacity', 'infrastructure', 'not_placeable',
                                              'config_unreadable')
                         AND ran_at <= (unixepoch() - ?1) * 1000
                         AND (
                           SELECT t.state FROM turns t
                            WHERE t.job_id = latest.job_id
                            ORDER BY t.sequence DESC
                            LIMIT 1
                         ) = 'complete'
                         AND COALESCE((
                           SELECT t.start_reason FROM turns t
                            WHERE t.job_id = latest.job_id
                            ORDER BY t.created_at DESC, t.sequence DESC
                            LIMIT 1
                         ), '') != 'memory_review'
                         AND (
                           EXISTS (
                             SELECT 1 FROM artifacts a
                              WHERE a.job_id = latest.job_id
                                AND a.artifact_type = 'create-pr'
                           )
                           OR EXISTS (
                             SELECT 1 FROM merge_requests mr
                              WHERE mr.issue_id = latest.issue_id
                                AND mr.source_branch = latest.branch
                                AND mr.status = 'open'
                           )
                         )
                       ORDER BY ran_at, job_id";

async fn bounded_rearm_candidates_in_db(
    db: &crate::storage::LocalDb,
) -> Result<Vec<BoundedRearmCandidate>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    BOUNDED_REARM_CANDIDATES_SQL,
                    (DORMANT_INFRA_RETRY_COOLDOWN_SECONDS,),
                )
                .await?;
            let mut candidates = Vec::new();
            while let Some(row) = rows.next().await? {
                candidates.push(BoundedRearmCandidate {
                    job_id: row.text(0)?,
                    tree_hash: row.text(1)?,
                    input_hash: row.text(2)?,
                    ran_at: row.i64(3)?,
                    check_name: row.text(4)?,
                    failure_kind: row.text(5)?,
                    environment_fingerprint: row.text(6)?,
                });
            }
            Ok::<_, DbError>(candidates)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Whether a dormant candidate is worth a wave now: its row still describes the
/// branch's current tree, and the fact that failed it has changed (cairn/5357).
/// Never a timer alone.
///
/// The fact depends on how the lane failed:
///
/// - `config_unreadable`: the project's config loads now.
/// - `not_placeable`: a machine that could take the lane has attached or
///   enrolled.
/// - `capacity`: room has appeared on the machines the wave would ask
///   ([`capacity_relief_is_due`]).
/// - `infrastructure`: a genuine verdict for the same input has landed since
///   the newest failure, or the lane's placement facts (its fleet scope and
///   every eligible machine's repair identity) differ from the ones that
///   failure recorded. A lane the breaker tripped has the same facts by
///   definition, so it is never due until one moves. A failure recorded
///   before placement facts were persisted has nothing to compare against and
///   waits for a verdict, a new commit, or a base advance.
///
/// These are recovery edges. Nothing about a machine attaching schedules a wave
/// for a PR whose agent has gone quiet, so this cadence asks on its behalf,
/// using the check's definition at that same commit. Until the answer changes
/// the candidate is passed over, which costs a config read and no wave.
pub(super) async fn bounded_candidate_is_due(
    orch: &Orchestrator,
    candidate: &BoundedRearmCandidate,
) -> Result<bool, String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &candidate.job_id)
        .await
        .map_err(|error| error.to_string())?;
    let Some(coords) =
        crate::execution::checks_turn_end::resolve_job_coords(&db, &candidate.job_id).await?
    else {
        return Ok(false);
    };
    let repo_root = std::path::PathBuf::from(&coords.repository_path);
    let store_dir = crate::jj::project_store_dir(&orch.config_dir, &repo_root);
    let logical_repository = if crate::jj::is_jj_dir(&store_dir) {
        store_dir
    } else {
        repo_root.clone()
    };
    let sealed_commit =
        match cairn_vcs::resolve_coordinate(&logical_repository, &coords.branch).await {
            Ok(commit) => commit,
            Err(error) => {
                log::warn!(
                    "review-checks bounded re-arm: branch for job {} is unresolved ({error})",
                    &candidate.job_id[..candidate.job_id.len().min(8)]
                );
                return Ok(false);
            }
        };
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let current_tree = {
        let logical_repository = logical_repository.clone();
        let sealed_commit = sealed_commit.clone();
        tokio::task::spawn_blocking(move || {
            crate::jj::logical_tree_hash(&jj, &logical_repository, &sealed_commit)
        })
        .await
        .map_err(|error| format!("capacity re-arm tree-hash task failed: {error}"))?
        .map_err(|error| error.to_string())?
    };
    if current_tree != candidate.tree_hash {
        return Ok(false);
    }
    // Read from the project repository, as the turn-end wave reads it: the
    // managed store is not a git object database of its own.
    let loaded =
        crate::execution::checks::load_checks_contract_at_commit(&repo_root, &sealed_commit).await;
    let check = loaded
        .as_ref()
        .and_then(|loaded| loaded.contract.checks.get(&candidate.check_name));
    if candidate.failure_kind
        == crate::execution::checks::CheckFailureKind::ConfigUnreadable.as_str()
    {
        // Read where preparation reads it: the project checkout.
        return Ok(check.is_some()
            && crate::config::project_settings::load_execution_project_policy(&repo_root).is_ok());
    }
    if candidate.failure_kind == crate::execution::checks::CheckFailureKind::NotPlaceable.as_str() {
        // A check the commit no longer declares has nothing for a wave to run.
        return Ok(check.is_some_and(|check| {
            orch.fleet
                .unplaceable_reason(
                    check.executor.as_ref(),
                    &crate::execution::check_identity::verdict_platforms(check),
                )
                .is_none()
        }));
    }
    // A check the commit no longer declares has nothing for a wave to run.
    let Some(check) = check else {
        return Ok(false);
    };
    let question = crate::execution::checks_turn_end::review_check_placement_question(Some(check));
    let capacity =
        candidate.failure_kind == crate::execution::checks::CheckFailureKind::Capacity.as_str();
    let occupancy = capacity.then(|| {
        crate::execution::checks::review_placement_occupancy(
            orch,
            &coords.project_id,
            &coords.repository_path,
            &question,
        )
    });
    let scopes = crate::execution::checks_turn_end::review_infra_scopes(
        orch,
        &coords.project_id,
        &candidate.job_id,
        &sealed_commit,
        &coords.repository_path,
        &[question],
    )
    .await
    .pop()
    .expect("one scope set per placement question");
    // This head's newest attempt died against the very fleet the wave would
    // meet now. A fleet counter counts heads rather than attempts, so asking
    // again would cost a placement and a death record and could never move
    // it: nothing about the answer has changed. The fleet's identity moving
    // — a machine attaching, or a blind one reporting again — is what makes
    // the head worth asking again, and that is when this stops holding.
    if scopes.fleet.as_deref() == Some(candidate.environment_fingerprint.as_str()) {
        return Ok(false);
    }
    let current_facts = scopes.placement_facts();
    let (failure, held) = {
        let project_id = coords.project_id.clone();
        let check_name = candidate.check_name.clone();
        let input_hash = candidate.input_hash.clone();
        let failure_kind = candidate.failure_kind.clone();
        let current_facts = current_facts.clone();
        db.read(move |conn| {
            let project_id = project_id.clone();
            let check_name = check_name.clone();
            let input_hash = input_hash.clone();
            let failure_kind = failure_kind.clone();
            let current_facts = current_facts.clone();
            Box::pin(async move {
                let failure = crate::execution::lane_breaker::newest_review_failure_conn(
                    conn,
                    &project_id,
                    &check_name,
                    &input_hash,
                    &failure_kind,
                )
                .await?;
                let held = crate::execution::lane_breaker::lane_trip_conn(
                    conn,
                    &project_id,
                    &check_name,
                    &input_hash,
                    Some(&current_facts),
                )
                .await?
                .is_some();
                Ok::<_, crate::storage::DbError>((failure, held))
            })
        })
        .await
        .map_err(|error| error.to_string())?
    };
    // Asked exactly as the wave will ask it: a lane the breaker holds under
    // the facts it would meet now gets no wave, whatever its newest recorded
    // failure says. A held lane writes no observation, so that record stops
    // moving at the hold, and facts that changed and changed back would
    // otherwise read as a change on every cooldown.
    if held {
        return Ok(false);
    }
    let Some(failure) = failure else {
        return Ok(occupancy.as_ref().is_some_and(capacity_relief_is_due));
    };
    if failure.answered_since {
        return Ok(true);
    }
    // Occupancy counts only Cairn's own cells, so it cannot see a model that
    // preempted the lane; that lane waits on the placement facts instead.
    if let Some(occupancy) = occupancy.as_ref() {
        if !crate::execution::checks::is_preemption_diagnostic(&failure.output_tail) {
            return Ok(capacity_relief_is_due(occupancy));
        }
    }
    Ok(failure
        .placement_facts
        .is_some_and(|facts| facts != current_facts))
}

/// Whether the machines a capacity-refused lane would ask have room now: no
/// Cairn work holds them, or the work that does is predicted to have let go
/// already. An unforecastable occupancy is not room, and neither is a relief
/// still ahead: the lane waits for the fact, not for a timer.
pub(super) fn capacity_relief_is_due(
    occupancy: &crate::fleet::occupancy::MachineOccupancy,
) -> bool {
    match occupancy {
        crate::fleet::occupancy::MachineOccupancy::Idle => true,
        crate::fleet::occupancy::MachineOccupancy::Predicted(forecast) => forecast.relief_ms == 0,
        crate::fleet::occupancy::MachineOccupancy::Unforecastable => false,
    }
}

/// Jobs eligible for the startup review-checks re-arm, as `(job_id, issue_id)`:
/// settled jobs on a non-terminal issue that still own reviewable output.
///
/// `blocked` counts as settled here alongside `idle`/terminal. It is the status of
/// a job parked at a confirmation gate — an unconfirmed plan artifact is
/// reviewable output by the very predicate this query uses, and its owner is
/// blocked precisely because someone still owes it a review (CAIRN-3347).
///
/// `cancelled` counts too. It is terminal by `JobStatus::is_terminal` and settled
/// by `issue_settled`, and unlike the recompute hook — which filters derived
/// TRANSITIONS, and so can never see an archived job — this query filters stored
/// status, where a cancelled job holding reviewable output is perfectly visible.
/// Excluding it would make restart the one moment archived-but-reviewable work
/// became permanently invisible to its watcher. This is recovery, not live
/// coverage: archival has no evaluation edge of its own (CAIRN-3349).
async fn rearm_candidate_jobs(
    db: &crate::storage::LocalDb,
) -> Result<Vec<(String, String)>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!(
                        "SELECT DISTINCT j.id, j.issue_id FROM jobs j
                     JOIN issues i ON i.id = j.issue_id
                     WHERE i.status NOT IN ('merged', 'closed')
                       AND j.status IN ('idle', 'complete', 'failed', 'blocked', 'cancelled')
                       AND (
                         EXISTS (
                           SELECT 1 FROM artifacts a
                           WHERE a.job_id = j.id
                             AND {REVIEWABLE_ARTIFACT_PREDICATE}
                         )
                         OR EXISTS (
                           SELECT 1 FROM merge_requests mr
                           WHERE mr.issue_id = j.issue_id
                             AND mr.source_branch = j.branch
                             AND mr.status = 'open'
                         )
                       )"
                    ),
                    (),
                )
                .await?;
            let mut ids = Vec::new();
            while let Some(row) = rows.next().await? {
                ids.push((row.text(0)?, row.text(1)?));
            }
            Ok::<_, DbError>(ids)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// The PR-open edge of the review push (CAIRN-2483). A builder that writes a
/// `create-pr` artifact and idles cannot fire the review at its idle edge: the
/// PR is not open yet (branch push + open lag), and the create-pr artifact
/// auto-confirms on write (CAIRN-1219), so neither reviewable arm matches at that
/// instant. The in-process PR-opening path
/// (`execution::actions::fire_pr_open_review`, from `handle_create_pr` and
/// `handle_pr_node`) observes the PR opening and routes here.
///
/// This edge now defers to [`evaluate_review_readiness`], which carries the full
/// issue-quiescence gate. `source_branch` is retained for the
/// caller's signature and logging; the evaluator resolves the producing job from
/// the issue itself. At PR-open time the review agent or the checks suite is
/// usually still live, so this edge typically defers and a later settled edge
/// (the job-terminal recompute hook or checks completion) fires the wake; it
/// covers the case where the MR row lands only after everything else has settled.
pub async fn create_review_push_for_pr_open(
    orch: &Orchestrator,
    issue_id: &str,
    source_branch: &str,
) {
    log::debug!(
        "PR-open review: evaluating readiness for issue {} (branch {})",
        &issue_id[..issue_id.len().min(8)],
        source_branch,
    );
    evaluate_review_readiness(orch, issue_id).await;
}

/// Create the review push rows for an issue's watchers — the single shared
/// implementation behind both review edges (node-idle and PR-open). Resolves the
/// reviewable `content_ref` + change fingerprint, then pushes a `review:{issue}`
/// to each watcher except the producing node, skipping any watcher whose latest
/// review push already carries this fingerprint (CAIRN-1889 change-trigger;
/// supersede-by-key collapses an undelivered older-fingerprint row). Returns the
/// recipients that received a fresh push so the caller can wake them.
///
/// DB-only and async: the node-idle edge runs it inside `run_db_blocking`; the
/// PR-open webhook edge awaits it directly.
async fn create_review_push_rows(
    db: &crate::storage::LocalDb,
    producing_job_id: &str,
    issue_id: &str,
    ctx: &crate::orchestrator::attention::IssueAttentionContext,
) -> Result<Vec<String>, String> {
    let issue_uri = ctx.issue_uri();
    // Reviewable predicate (disjunction), resolved straight into the content_ref
    // the watcher follows plus a change fingerprint:
    //   arm 1 — an open unmerged PR -> the producing node's `/pr` URI;
    //   arm 2 — a create-pr artifact or unconfirmed plan artifact -> its artifact URI.
    // The second arm is load-bearing at the node-idle edge: at the create-pr idle
    // the PR may not be open yet, but the artifact write is already observable.
    // Neither arm -> no push.
    let Some((content_ref, fingerprint)) = reviewable_ref_and_fingerprint(
        db,
        &ctx.project_key,
        ctx.number,
        issue_id,
        producing_job_id,
    )
    .await?
    else {
        log::debug!(
            "review push: no reviewable ref (job={} issue={})",
            &producing_job_id[..producing_job_id.len().min(8)],
            issue_uri
        );
        return Ok(Vec::new());
    };
    let key = format!("review:{issue_uri}");
    let watchers = crate::orchestrator::wakes::watcher_jobs_for_issue(db, &issue_uri).await?;
    log::debug!(
        "review push: job={} content_ref={} fp={} watchers={}",
        &producing_job_id[..producing_job_id.len().min(8)],
        content_ref,
        fingerprint,
        watchers.len()
    );
    let mut pushed = Vec::new();
    for recipient in watchers {
        // Never push to the producing node itself.
        if recipient == producing_job_id {
            continue;
        }
        // CAIRN-1889 change-trigger: skip when the latest review push to this
        // recipient (delivered OR undelivered) already carries this fingerprint —
        // the reviewable state is unchanged, so re-firing would spuriously
        // re-wake. A changed fingerprint creates a new push; supersede-by-key
        // still collapses an undelivered older-fingerprint row to the newest.
        if let Some(Some(prev)) =
            crate::orchestrator::attention_push::latest_push_fingerprint(db, &recipient, &key)
                .await
                .map_err(|e| e.to_string())?
        {
            if prev == fingerprint {
                log::debug!(
                    "review push: recipient {} deduped (fingerprint unchanged)",
                    &recipient[..recipient.len().min(8)]
                );
                continue;
            }
        }
        let (_, effective) = crate::orchestrator::attention_push::push_with_fingerprint(
            db,
            &recipient,
            &content_ref,
            Wake::Wake,
            Boundary::Event,
            &key,
            Some(&fingerprint),
        )
        .await
        .map_err(|e| e.to_string())?;
        // A watcher that muted this issue gets a `Passive` review row that rides
        // along on its next run; it is not handed back to `wake_review_recipients`
        // so an idle muted watcher is never woken (CAIRN-1900).
        if effective.wakes_idle() {
            pushed.push(recipient);
        }
    }
    Ok(pushed)
}

/// Wake each watcher that received a fresh review push so an already-idle one
/// wakes now instead of only seeing the review ride along with an unrelated later
/// wake (CAIRN-1889). `nudge_job_for_urgency` is the shared resume-ladder
/// primitive: an idle recipient resumes; a busy one (a mid-turn agent OR a
/// self-suspended one, both of which read as active) is left alone for the
/// event-boundary push drain or its own-work resume to deliver the push. Steer
/// wakes idle and never stops a busy turn.
fn wake_review_recipients(orch: &Orchestrator, recipients: &[String]) {
    for recipient in recipients {
        if let Err(e) = crate::messages::delivery::nudge_job_for_urgency(
            orch,
            recipient,
            crate::messages::queued::DeliveryUrgency::Steer,
        ) {
            log::warn!(
                "review push wake for {} failed: {}",
                &recipient[..recipient.len().min(8)],
                e
            );
        }
    }
}

/// Whether the job's most recent turn was a memory-review turn. The work-turn
/// idle gate for the review push: a trailing `memory_review` turn end is not a
/// work turn and must not create a review push (CAIRN-1882).
fn latest_turn_is_memory_review(orch: &Orchestrator, job_id: &str) -> bool {
    blocking_text_lookup(
        orch,
        job_id,
        "SELECT CASE WHEN start_reason = 'memory_review' THEN '1' ELSE '0' END
         FROM turns WHERE job_id = ?1
         ORDER BY created_at DESC, sequence DESC LIMIT 1",
        TextColumn::Optional,
    )
    .as_deref()
        == Some("1")
}

/// The reviewable content_ref + change fingerprint at a work-turn idle, or `None`
/// when nothing is reviewable (CAIRN-1889). The fingerprint is the change key the
/// creator compares against the latest review push to decide whether to re-fire.
///
/// - Arm 1 (open unmerged PR), branch-precise: scoped to the producing builder's
///   own branch (`jobs.branch == merge_requests.source_branch`) so an unrelated
///   open PR on another branch for the same issue can't drive this node's review.
///   The producing node's `/pr` URI and a head-SHA-or-diffstat fingerprint (see
///   [`open_pr_review_arm`]).
/// - Arm 2 (create-pr artifact or unconfirmed plan artifact): the artifact URI
///   and an `artifact:{version}:{updated_at}` fingerprint.
async fn reviewable_ref_and_fingerprint(
    db: &crate::storage::LocalDb,
    project_key: &str,
    number: i32,
    issue_id: &str,
    job_id: &str,
) -> Result<Option<(String, String)>, String> {
    // Arm 1 is scoped to this builder's own branch. Both edges pass the builder
    // job, whose `jobs.branch` is the PR's `source_branch`, so the open-PR lookup
    // is unambiguous even when an issue carries more than one branch/PR.
    if let Some(source_branch) = job_branch(db, job_id).await? {
        if let Some(arm1) =
            open_pr_review_arm(db, project_key, number, issue_id, &source_branch, job_id).await?
        {
            return Ok(Some(arm1));
        }
    }
    review_artifact_ref(db, project_key, number, job_id).await
}

/// The producing builder's branch (`jobs.branch`), or `None` when unset (a
/// plan-only node has no worktree branch, in which case arm 1 never applies).
async fn job_branch(db: &crate::storage::LocalDb, job_id: &str) -> Result<Option<String>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT branch FROM jobs WHERE id = ?1", (job_id.as_str(),))
                .await?;
            match rows.next().await? {
                Some(row) => Ok::<_, DbError>(row.opt_text(0)?),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Arm 1: an open PR on `source_branch` for the issue, resolved to a content_ref
/// the watcher reviews plus a change fingerprint.
///
/// Reviewability is driven by the `merge_requests` row ALONE — "an open PR on
/// this branch." The content URI is built from the `builder_job_id` we already
/// resolved (cleanly joinable to its execution), NOT from `mr.job_id`: that is
/// the pr-action node, whose execution may not join, and an inner join through it
/// dropped the whole row so a real open PR read as unreviewable (the live
/// CAIRN-1891 failure). The builder node is the right review target anyway. If
/// the builder's node coordinates don't resolve, the content_ref falls back to
/// the issue URI, which still resolves for the drain/render path.
///
/// The fingerprint prefers the reviewed head commit SHA (`sha:{sha}`), which a
/// real new commit always changes and a mergeability-only settle never does; it
/// falls back to the diffstat (`pr:{mr}:{additions}:{deletions}`) when no head
/// SHA has been recorded yet. `None` when no open PR exists on that branch.
async fn open_pr_review_arm(
    db: &crate::storage::LocalDb,
    project_key: &str,
    number: i32,
    issue_id: &str,
    source_branch: &str,
    builder_job_id: &str,
) -> Result<Option<(String, String)>, String> {
    let issue_id = issue_id.to_string();
    let source_branch = source_branch.to_string();
    let builder_job_id = builder_job_id.to_string();
    #[allow(clippy::type_complexity)]
    let resolved: Option<(
        String,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<String>,
    )> = db
        .read(|conn| {
            let issue_id = issue_id.clone();
            let source_branch = source_branch.clone();
            let builder_job_id = builder_job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT mr.id, mr.additions, mr.deletions, mr.head_sha,
                                e.seq, j.uri_segment, parent.uri_segment
                         FROM merge_requests mr
                         LEFT JOIN jobs j ON j.id = ?3
                         LEFT JOIN executions e ON j.execution_id = e.id
                         LEFT JOIN jobs parent ON j.parent_job_id = parent.id
                         WHERE mr.issue_id = ?1 AND mr.source_branch = ?2
                           AND mr.status = 'open'
                         ORDER BY mr.updated_at DESC
                         LIMIT 1",
                        (
                            issue_id.as_str(),
                            source_branch.as_str(),
                            builder_job_id.as_str(),
                        ),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok::<_, DbError>(Some((
                        row.text(0)?,
                        row.opt_i64(1)?,
                        row.opt_i64(2)?,
                        row.opt_text(3)?,
                        row.opt_i64(4)?,
                        row.opt_text(5)?,
                        row.opt_text(6)?,
                    ))),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|e| e.to_string())?;
    Ok(resolved.map(
        |(mr_id, additions, deletions, head_sha, seq, uri_segment, parent_segment)| {
            let content_ref = match (seq, uri_segment) {
                (Some(seq), Some(node)) => match parent_segment {
                    Some(parent) => cairn_common::uri::build_task_artifact_uri_named(
                        project_key,
                        number,
                        seq as i32,
                        &parent,
                        &node,
                        None,
                    ),
                    None => cairn_common::uri::build_node_artifact_uri_named(
                        project_key,
                        number,
                        seq as i32,
                        &node,
                        None,
                    ),
                },
                _ => format!("cairn://p/{project_key}/{number}"),
            };
            // CAIRN-2483: unify on the reviewed head commit (`sha:{sha}`) so the
            // open-PR arm and the create-pr-artifact arm can never re-fire twice
            // for the same reviewed state across edges. Falls back to the diffstat
            // form only when no head SHA has been recorded yet.
            let fingerprint = match head_sha {
                Some(sha) => format!("sha:{sha}"),
                None => {
                    let fmt =
                        |n: Option<i64>| n.map(|v| v.to_string()).unwrap_or_else(|| "-".into());
                    format!("pr:{mr_id}:{}:{}", fmt(additions), fmt(deletions))
                }
            };
            (content_ref, fingerprint)
        },
    ))
}

/// The producing job's create-pr or unconfirmed plan artifact, resolved to its
/// node-artifact URI plus an `artifact:{version}:{updated_at}` change fingerprint
/// (CAIRN-1889), or `None` when the job has no such artifact. Arm 2 of the
/// review-push reviewable predicate. `create-pr` remains reviewable even when it
/// is already confirmed because the PR lifecycle auto-confirms that artifact
/// before every deployment shape has observed the PR-open edge.
pub(crate) async fn review_artifact_ref(
    db: &crate::storage::LocalDb,
    project_key: &str,
    number: i32,
    job_id: &str,
) -> Result<Option<(String, String)>, String> {
    let job_id = job_id.to_string();
    #[allow(clippy::type_complexity)]
    let resolved: Option<(i64, String, Option<String>, Option<String>, i64, i64)> = db
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        &format!(
                            "SELECT e.seq, j.uri_segment, parent.uri_segment, a.output_name, a.version, a.updated_at
                         FROM artifacts a
                         JOIN jobs j ON a.job_id = j.id
                         JOIN executions e ON j.execution_id = e.id
                         LEFT JOIN jobs parent ON j.parent_job_id = parent.id
                         WHERE a.job_id = ?1
                           AND {REVIEWABLE_ARTIFACT_PREDICATE}
                         ORDER BY a.version DESC
                         LIMIT 1"
                        ),
                        (job_id.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok::<_, DbError>(Some((
                        row.i64(0)?,
                        row.text(1)?,
                        row.opt_text(2)?,
                        row.opt_text(3)?,
                        row.i64(4)?,
                        row.i64(5)?,
                    ))),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|e| e.to_string())?;
    // A sub-agent task job nests its artifact under the parent node
    // (`.../{parent}/task/{task}/...`); a top-level node uses the flat node URI.
    // The flat form does not resolve for a sub-task (issue #143).
    Ok(resolved.map(
        |(seq, node, parent_segment, output_name, version, updated_at)| {
            let uri = match parent_segment {
                Some(parent) => cairn_common::uri::build_task_artifact_uri_named(
                    project_key,
                    number,
                    seq as i32,
                    &parent,
                    &node,
                    output_name.as_deref(),
                ),
                None => cairn_common::uri::build_node_artifact_uri_named(
                    project_key,
                    number,
                    seq as i32,
                    &node,
                    output_name.as_deref(),
                ),
            };
            (uri, format!("artifact:{version}:{updated_at}"))
        },
    ))
}
