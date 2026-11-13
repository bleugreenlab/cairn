//! The owner-requested replay: `write cairn:~/rebase {action:"replay"}`.
//!
//! An explicit replay is one branch-addressed preparation
//! ([`super::preparation::prepare_branch`]) performed inside the request, under
//! one cumulative explicit budget. Nothing is queued, no worker is woken, and no
//! other branch is enumerated: the request names one branch and its answer is
//! that branch's terminal outcome. A restart mid-request leaves either the
//! compare-and-swap install or nothing; the requester sees the error and asks
//! again, so no request accepted by this path can be stranded.
//!
//! # Authority
//!
//! The request carries the same authority it always has, re-derived at the
//! exact coordinate each attempt prepares:
//!
//! - A fingerprint pins the open session the request was composed against.
//! - `take-committed-tip` is accepted only on the exhaustive proof that the
//!   committed tip keeps every incoming hunk, or on the requester's stated
//!   reason, and deletions that reason attests are recorded only once the
//!   request is accepted.
//! - The resolution carry is planned for exactly the head that proof judged and
//!   laid onto the session's recorded base X, in both catch-up modes, however
//!   far the live base has moved since: a resolution of X lands on X. The
//!   preparer captures X exactly and refuses a capture at any other head; the
//!   request then re-assesses and re-plans at the new head, a bounded number of
//!   times. Paths the carry refuses are left to the merge, so the replay
//!   conflicts and the session at X is re-recorded. A conflict at X on a path
//!   the session did not list re-records the session with it and re-plans, so
//!   the carry ends up placing the whole inventory of merging the committed tip
//!   with X.
//! - Once X lands, the live base's commits since X are a following round: a
//!   plain request onto the live base, queued as this request's child and
//!   served within what is left of its budget (or by the recovery server). A
//!   conflict there opens its own session, naming only what those commits
//!   conflict on. A request is never retargeted onto a newer base and judged
//!   there.
//!
//! # Budget
//!
//! [`EXPLICIT_REPLAY_BUDGET`] is one deadline shared by the proof, the carry
//! plan and the detached rebase/export/heal, not a fresh allowance per step. A
//! step is not started with less than [`EXPLICIT_STEP_MINIMUM`] left; the answer
//! names the step and what it had. An explicit timeout never records the
//! automatic cooldown, and an automatic cooldown never blocks an explicit
//! request (both enforced by the preparer on `explicit_replay`). An explicit
//! compute exhaustion does record a blocker that refuses only an identical
//! repeat ([`super::replay_cost`]); the answer and the rebase page say so in
//! the same words.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::preparation::{
    demand_intent, live_branch_owner, load_relationship, prepare_branch, BusyReason,
    ConflictRecord, PreparationPurpose, PreparationRefusal, PreparationRequest, PreparedBranch,
    PreparedExport,
};
use super::{persist_reconcile_item, plan_resolution_carry, CarryPlan, ReconcileItemUpdate};
use crate::orchestrator::conflict_session::{
    self, AssessmentScope, ConflictSession, PendingReplayRequest, ReplayDecision, RestoreProof,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;

/// One cumulative allowance for an owner-requested replay: proof, resolution
/// carry, rebase, export and ancestry heal together. cairn/6197 measured the
/// affected resolved replays end to end at 14.9 s and 12.5 s including the
/// heal; 60 s is four times the larger. Automatic preparation keeps the
/// background bound.
pub(crate) const EXPLICIT_REPLAY_BUDGET: Duration = Duration::from_secs(60);

/// A step is not started with less than this left: each spawns several jj
/// subprocesses, and starting one only to have it killed reports a timeout
/// that names the wrong step.
pub(crate) const EXPLICIT_STEP_MINIMUM: Duration = Duration::from_secs(1);

/// How many times the request re-assesses and re-plans after the branch or
/// its base moves under it before reporting the race instead of chasing it.
const REPLAY_ATTEMPTS: u32 = 3;

/// The close kind of a request refused as an identical repeat of an explicit
/// compute timeout; the rebase page reads it back.
pub(crate) const REPEAT_REFUSED: &str = "repeat_refused";

/// The sanctioned store-side replay of one branch onto its current base.
///
/// The only surface that can move a branch's ancestry: an agent's slot is a
/// plain worktree whose refs are exports of the runner's store. It is needed
/// after every content conflict, because resolving the files with ordinary
/// writes fixes content, not the ancestry the rolled-back rebase left behind.
///
/// An open conflict session is not a precondition. Without one the branch is
/// replayed onto the base its job declares, and a conflict opens the session
/// on the way through, which is how a branch nothing ever advanced gets one.
///
/// `Err` is a refusal before anything was attempted. `Ok` is the outcome of an
/// attempted replay, landed or not, written for the requester.
pub(crate) async fn request_branch_replay(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    branch: &str,
    expected_fingerprint: Option<&str>,
    take_committed_tip: bool,
    drop_incoming_reason: Option<&str>,
) -> Result<String, String> {
    replay(
        orch,
        db,
        &ExplicitReplay {
            job_id,
            branch,
            expected_fingerprint,
            take_committed_tip,
            drop_incoming_reason,
            budget: EXPLICIT_REPLAY_BUDGET,
            step_minimum: EXPLICIT_STEP_MINIMUM,
            recovered: None,
            increment_from: None,
        },
    )
    .await
}

struct ExplicitReplay<'a> {
    job_id: &'a str,
    branch: &'a str,
    expected_fingerprint: Option<&'a str>,
    take_committed_tip: bool,
    drop_incoming_reason: Option<&'a str>,
    budget: Duration,
    step_minimum: Duration,
    /// An accepted request recovered from its durable marker. Its tip was
    /// checked against the branch before this attempt, and it is never
    /// re-proven at another tip: a move closes it as superseded.
    recovered: Option<PendingReplayRequest>,
    /// For the following round of a request that landed a session's base:
    /// that base, which the branch now carries.
    increment_from: Option<&'a str>,
}

/// How an attempt settles the request's durable marker.
enum Settle {
    /// A verdict was reached on `base_branch` at `base_commit`. `record` is
    /// the verdict to persist there, when preparation did not already persist
    /// one (it does for a conflict).
    Served {
        base_branch: String,
        base_commit: String,
        record: Option<Verdict>,
    },
    /// A run batch holds the branch; the batch's release serves it.
    Held,
    /// Nothing was attempted to a verdict; the requester is told to ask again.
    Closed { kind: &'static str },
}

struct Verdict {
    outcome: &'static str,
    failure: Option<&'static str>,
    tip: Option<String>,
}

/// The one explicit deadline every step draws on.
struct Budget {
    deadline: Instant,
    total: Duration,
    minimum: Duration,
}

impl Budget {
    fn start(total: Duration, minimum: Duration) -> Self {
        Self {
            deadline: Instant::now() + total,
            total,
            minimum,
        }
    }

    /// What `step` may spend, or the answer that it was not started.
    fn admit(&self, branch: &str, step: &str) -> Result<Duration, String> {
        let left = self.deadline.saturating_duration_since(Instant::now());
        if left < self.minimum {
            return Err(format!(
                "Explicit replay of `{branch}` stopped before {step}: its {total:.1}s explicit budget \
                 had {left:.3}s left, less than the {minimum:.3}s the step needs, so the step was \
                 not started. Nothing was installed and `{branch}` is unchanged. No automatic \
                 cooldown was recorded; the replay can be requested again at once.",
                total = self.total.as_secs_f64(),
                left = left.as_secs_f64(),
                minimum = self.minimum.as_secs_f64(),
            ));
        }
        Ok(left)
    }
}

async fn replay(
    orch: &Orchestrator,
    db: &LocalDb,
    ask: &ExplicitReplay<'_>,
) -> Result<String, String> {
    let branch = ask.branch;
    // A recovered request refused before its attempt is closed with the
    // reason, so recovery does not find it again at every start.
    let refuse = |text: String| async move {
        if let Some(recovered) = ask.recovered.as_ref() {
            if let Err(error) = conflict_session::close_replay_request(
                db,
                &recovered.id,
                &recovered.bookmark,
                "refused",
                &text,
            )
            .await
            {
                log::warn!("close replay request `{}`: {error}", recovered.id);
            }
        }
        text
    };
    let session = match request_session(db, branch, ask.recovered.as_ref()).await {
        Ok(session) => session,
        Err(error) => return Err(refuse(error).await),
    };
    if let Err(refusal) = refuse_sessionless_artifacts(branch, session.as_ref(), ask) {
        return Err(refuse(refusal).await);
    }
    if let (Some(session), Some(expected)) = (session.as_ref(), ask.expected_fingerprint) {
        let current = session.fingerprint();
        if expected != current {
            return Err(format!(
                "This rebase session has moved on: you named fingerprint `{expected}`, but it is \
                 now `{current}`. Re-read cairn:~/rebase and request the replay again."
            ));
        }
    }

    let mut project = match load_replay_project(db, ask.job_id).await {
        Ok(project) => project,
        Err(error) => return Err(refuse(error).await),
    };
    // A team project's synced path names another machine's checkout; the
    // replay runs in this machine's clone or not at all.
    project.repo_path =
        match crate::projects::crud::resolve_local_repo_path_and_key(&orch.db, &project.id).await {
            Ok((Some(path), _)) => path,
            Ok((None, _)) => {
                return Err(refuse(
                    "Clone this team project's repository before requesting replay".to_string(),
                )
                .await)
            }
            Err(error) => return Err(refuse(error.to_string()).await),
        };
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&project.repo_path));
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let merge_mode = crate::orchestrator::catch_up::mode_for(db, &project.id, &store, branch)
        .await?
        .is_some();
    // The deadline starts before any wait: base resolution's lock wait, the
    // preparer's reservation and lock waits, and every compute phase draw on it.
    let budget = Budget::start(ask.budget, ask.step_minimum);
    let preamble = match repoint_superseded_base(
        orch,
        db,
        &jj,
        &store,
        &project.id,
        ask,
        session.as_ref(),
        &budget,
    )
    .await
    {
        Ok(preamble) => preamble,
        Err(refusal) => return Err(refuse(refusal).await),
    };
    let with_preamble = |text: String| format!("{preamble}{text}");
    let mut caveats: Vec<String> = Vec::new();
    let mut last_move = String::new();
    // The durable record of this accepted request: queued once the request is
    // accepted, consumed only by a verdict or an honest close.
    let mut marker = ask.recovered.clone();
    // Only a run-batch hold leaves a request unserved: that batch's release
    // serves it. Every other attempt that could not run closes it with the
    // answer, live or recovered, so nothing waits quietly for a trigger that
    // will not come.
    let (store_ref, project_ref) = (&store, &project.id);
    let recovered = ask.recovered.is_some();
    let settle = move |marker: Option<PendingReplayRequest>, how: Settle, answer: String| {
        let store = store_ref.to_path_buf();
        let project_id = project_ref.clone();
        async move {
            if let Some(marker) = marker {
                let closed = matches!(how, Settle::Closed { .. });
                settle_marker(db, &project_id, &store, &marker, how, &answer).await;
                // A following round answered inline is part of its parent's
                // answer; only a deferred one tells the owner separately.
                if closed && recovered && ask.increment_from.is_none() {
                    note_owner_closed(orch, db, ask.job_id, &marker, &answer);
                }
            }
            answer
        }
    };
    for attempt in 1..=REPLAY_ATTEMPTS {
        let mut placed: Option<std::collections::BTreeSet<String>> = None;
        let carry = if ask.take_committed_tip {
            let Some(session) = request_session(db, branch, marker.as_ref()).await? else {
                let answer = with_preamble(format!(
                    "The rebase session this request resolves is no longer open on `{branch}`, \
                     so there is no conflict left for `take-committed-tip` to resolve. Re-read \
                     cairn:~/rebase before requesting again."
                ));
                return Err(settle(
                    marker,
                    Settle::Closed {
                        kind: "session_closed",
                    },
                    answer,
                )
                .await);
            };
            let planned = match plan_take_committed_tip(
                db,
                &jj,
                &store,
                &project.id,
                ask,
                session,
                &budget,
                &mut caveats,
            )
            .await
            {
                Ok(planned) => planned,
                Err(PlanStop::Refused(refusal)) => {
                    let answer = with_preamble(refusal);
                    return Err(settle(marker, Settle::Closed { kind: "refused" }, answer).await);
                }
                Err(PlanStop::Answer(answer)) => {
                    let answer = with_preamble(with_caveats(answer, &caveats));
                    return Ok(settle(
                        marker,
                        Settle::Closed {
                            kind: "not_attempted",
                        },
                        answer,
                    )
                    .await);
                }
            };
            placed = Some(planned.placed.clone());
            #[cfg(test)]
            tests::after_carry_plan(&store, attempt);
            if marker.is_none() {
                marker = Some(
                    accept(
                        db,
                        &project.id,
                        &store,
                        ask,
                        &planned.base_branch,
                        &planned.base,
                        &planned.plan.carry.expected_pre_tip,
                    )
                    .await
                    .map_err(with_preamble)?,
                );
            }
            Some(planned)
        } else {
            if marker.is_none() {
                let (base_branch, live, head) = literal_coordinates(db, &project.id, &store, ask)
                    .await
                    .map_err(with_preamble)?;
                // Accepted at the base the preparer will judge it against: an
                // open session's base still ahead of the branch, else the
                // live base. Its following round is then its own row.
                let base = session_base_ahead(&store, session.as_ref(), &base_branch, &live, &head)
                    .await
                    .unwrap_or(live);
                marker = Some(
                    accept(db, &project.id, &store, ask, &base_branch, &base, &head)
                        .await
                        .map_err(with_preamble)?,
                );
            }
            None
        };

        let compute_budget = match budget.admit(branch, "the rebase, export and ancestry heal") {
            Ok(left) => left,
            Err(answer) => {
                let answer = with_preamble(with_caveats(answer, &caveats));
                return Ok(settle(
                    marker,
                    Settle::Closed {
                        kind: "not_attempted",
                    },
                    answer,
                )
                .await);
            }
        };
        let mut request = PreparationRequest::new(
            project.id.clone(),
            branch,
            Some(ask.job_id.to_string()),
            PreparationPurpose::OwnerCommit,
        );
        request.explicit_replay = true;
        request.compute_budget = compute_budget;
        request.lock_wait = compute_budget;
        request.deadline = Some(budget.deadline);
        request.explicit_cost = Some(super::replay_cost::ExplicitCost {
            started: budget.deadline - budget.total,
            budget: budget.total,
            take_committed_tip: ask.take_committed_tip,
            drop_incoming: ask.drop_incoming_reason.is_some(),
        });
        // A recovered request is for exactly the tip it was accepted at,
        // checked under the snapshot guard.
        request.expected_head = ask
            .recovered
            .as_ref()
            .and_then(|recovered| recovered.requested_tip.clone());
        let mut carry_refusals: Option<String> = None;
        if let Some(planned) = carry {
            if !planned.plan.refused.is_empty() {
                carry_refusals = Some(planned.plan.describe_refusals());
            }
            if planned.exact {
                request.exact_destination = Some(planned.base.clone());
            }
            request.carry_base = Some(planned.base);
            request.carry = Some(Arc::new(planned.plan.carry));
        }

        let outcome = prepare_branch(orch, request).await;
        let (answer, how) = match outcome {
            Ok(prepared) => {
                let answer = landed(db, branch, session.is_some(), &prepared, merge_mode).await;
                let how = Settle::Served {
                    base_branch: prepared.relationship.base_branch.clone(),
                    base_commit: prepared.base_commit.clone(),
                    record: Some(Verdict {
                        outcome: if prepared.advanced && merge_mode {
                            "merged_clean"
                        } else if prepared.advanced {
                            "rebased_clean"
                        } else {
                            "silent"
                        },
                        failure: None,
                        tip: Some(prepared.head_after.clone()),
                    }),
                };
                if prepared.advanced && prepared.pinned_base {
                    // Settled first, so the following round is the branch's
                    // newest request and its parent is already served.
                    let parent = marker.clone();
                    let mut out = preamble.clone();
                    out.push_str(&answer);
                    let answer = settle(marker, how, with_caveats(out, &caveats)).await;
                    let increment = increment_round(
                        orch,
                        db,
                        ask,
                        &store,
                        &project.id,
                        &prepared,
                        parent.as_ref(),
                        &budget,
                    )
                    .await;
                    return Ok(match increment {
                        Some(increment) => format!("{answer}\n\n{increment}"),
                        None => answer,
                    });
                }
                (answer, how)
            }
            // A recovered request is never re-proven at another tip.
            Err(PreparationRefusal::Moved { what }) if ask.recovered.is_some() => {
                let answer = with_preamble(format!(
                    "`{branch}` moved since this replay request was accepted ({what}), so nothing \
                     was replayed. Request the replay again from the branch as it is now."
                ));
                return Ok(settle(marker, Settle::Closed { kind: "superseded" }, answer).await);
            }
            // The carry was proven for a head or base that no longer holds:
            // re-derive the proof and the plan at the new coordinate.
            Err(PreparationRefusal::Moved { what }) if ask.take_committed_tip => {
                log::info!("explicit replay of `{branch}` (attempt {attempt}) re-plans: {what}");
                last_move = what;
                continue;
            }
            // The landing at the session's base conflicted on a path the plan
            // did not place: the session now lists it, so prove and plan again
            // over the whole inventory rather than answering with a gap the
            // owner's tip may already have resolved.
            Err(PreparationRefusal::Conflict(conflict))
                if attempt < REPLAY_ATTEMPTS
                    && placed.as_ref().is_some_and(|placed| {
                        conflict.paths.iter().any(|path| !placed.contains(path))
                    }) =>
            {
                last_move = format!(
                    "the merge with `{}` at {} also conflicted in {}",
                    conflict.base_branch,
                    conflict.base_commit,
                    conflict.paths.join(", ")
                );
                log::info!(
                    "explicit replay of `{branch}` (attempt {attempt}) re-plans: {last_move}"
                );
                continue;
            }
            Err(refusal) => {
                let how = match &refusal {
                    PreparationRefusal::RepeatTimeout(_) => Settle::Closed {
                        kind: REPEAT_REFUSED,
                    },
                    PreparationRefusal::Conflict(conflict) => Settle::Served {
                        base_branch: conflict.base_branch.clone(),
                        base_commit: conflict.base_commit.clone(),
                        record: None,
                    },
                    PreparationRefusal::TimedOut {
                        head,
                        base_branch,
                        base_commit,
                        ..
                    } => Settle::Served {
                        base_branch: base_branch.clone(),
                        base_commit: base_commit.clone(),
                        record: Some(Verdict {
                            outcome: "failed",
                            failure: Some(crate::jj::RECONCILE_TIMEOUT),
                            tip: Some(head.clone()),
                        }),
                    },
                    PreparationRefusal::Busy(BusyReason::RunBatch(_)) => Settle::Held,
                    _ => Settle::Closed {
                        kind: "not_attempted",
                    },
                };
                match not_landed(branch, refusal, carry_refusals.as_deref(), ask) {
                    Ok(answer) => (answer, how),
                    Err(refusal) => {
                        let mut refusal = with_preamble(with_caveats(refusal, &caveats));
                        if matches!(how, Settle::Held) {
                            refusal.push_str(
                                "\n\nYour request is recorded. It is served on its own when that \
                                 batch ends, whether it completes or is cancelled, or at the \
                                 orchestrator's next start; you do not need to request it again.",
                            );
                        }
                        return Err(settle(marker, how, refusal).await);
                    }
                }
            }
        };
        let mut out = preamble.clone();
        out.push_str(&answer);
        return Ok(settle(marker, how, with_caveats(out, &caveats)).await);
    }
    let answer = with_preamble(format!(
        "`{branch}` did not settle across {REPLAY_ATTEMPTS} explicit replay attempts (last: \
         {last_move}), so the replay was not installed and `{branch}` is unchanged. Your content \
         resolution still stands; request the replay again once the branch is quiet."
    ));
    Err(settle(
        marker,
        Settle::Closed {
            kind: "not_attempted",
        },
        answer,
    )
    .await)
}

/// Tell the owner, once and without waking it, that a deferred request it
/// was told would be served was closed instead, and why. Keyed by the
/// request, so a repeat close delivers nothing.
fn note_owner_closed(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    request: &PendingReplayRequest,
    reason: &str,
) {
    let Some(run_id) = crate::messages::delivery::latest_run_for_job(db, job_id) else {
        return;
    };
    let note = format!(
        "Your recorded replay request `{}` for `{}` could not be served when the batch holding \
         the branch ended: {reason}\n\nNothing was replayed. Request it again from \
         cairn:~/rebase.",
        request.id, request.bookmark
    );
    let key = format!("explicit-replay-closed:{}", request.id);
    if let Err(error) = crate::messages::delivery::queue_system_direct_once(
        orch,
        &run_id,
        &note,
        crate::messages::queued::DeliveryUrgency::Passive,
        &key,
    ) {
        log::warn!("note the closed replay request `{}`: {error}", request.id);
    }
}

/// Record an accepted request durably at the literal coordinate it was
/// accepted against. A recovered request already has its marker.
async fn accept(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    ask: &ExplicitReplay<'_>,
    base_branch: &str,
    base: &str,
    tip: &str,
) -> Result<PendingReplayRequest, String> {
    conflict_session::queue_replay_request(
        db,
        project_id,
        store,
        base_branch,
        base,
        ask.branch,
        Some(tip),
        ask.take_committed_tip,
        ask.drop_incoming_reason,
    )
    .await
    .map_err(|error| {
        format!("The replay request could not be recorded, so it was not accepted: {error}")
    })
}

/// The session a request answers to: for an accepted request, the one it was
/// accepted against, open or superseded by a newer recording (a session
/// recorded before captures kept the base's name keys a commit-named intent,
/// so the open session at the same base stands for it); otherwise the
/// branch's open session.
async fn request_session(
    db: &LocalDb,
    branch: &str,
    accepted: Option<&PendingReplayRequest>,
) -> Result<Option<ConflictSession>, String> {
    let Some(accepted) = accepted else {
        return conflict_session::load_active_session(db, branch).await;
    };
    if let Some(session) = conflict_session::load_session_for_request(db, accepted).await? {
        return Ok(Some(session));
    }
    Ok(conflict_session::load_active_session(db, branch)
        .await?
        .filter(|session| session.destination_commit == accepted.destination_commit))
}

/// An open session's recorded base when it is still the branch's next
/// catch-up step: on `base_branch`, behind the live base in its history, and
/// not yet in the branch's ancestry. The same rule the preparer pins an
/// owner's commit by, read here without the lock to choose where a request is
/// accepted.
async fn session_base_ahead(
    store: &Path,
    session: Option<&ConflictSession>,
    base_branch: &str,
    live: &str,
    head: &str,
) -> Option<String> {
    let session = session?;
    let recorded = session.destination_commit.as_str();
    if session.target_branch != base_branch || recorded == live {
        return None;
    }
    let on_base = cairn_vcs::history::contains(store, recorded, live)
        .await
        .ok()?;
    let landed = cairn_vcs::history::contains(store, recorded, head)
        .await
        .ok()?;
    (on_base && !landed).then(|| recorded.to_string())
}

/// The branch's literal head and its declared base's literal commit, read
/// without the lock: the coordinate a plain request is accepted against.
async fn literal_coordinates(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    ask: &ExplicitReplay<'_>,
) -> Result<(String, String, String), String> {
    let relationship = load_relationship(db, project_id, ask.branch, Some(ask.job_id), None)
        .await
        .map_err(|refusal| refusal.to_string())?;
    let resolve = |name: String| async move {
        cairn_vcs::resolve_coordinate(store, &name)
            .await
            .map_err(|error| format!("`{name}` could not be resolved: {error}"))
    };
    let base = resolve(relationship.base_branch.clone()).await?;
    let head = resolve(ask.branch.to_string()).await?;
    Ok((relationship.base_branch, base, head))
}

/// Settle `marker` the way the attempt ended. Failures are logged and leave
/// the marker unserved, which recovery reads again; they never change the
/// answer the requester gets.
async fn settle_marker(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    marker: &PendingReplayRequest,
    how: Settle,
    answer: &str,
) {
    let branch = marker.bookmark.as_str();
    match how {
        Settle::Held => {}
        Settle::Closed { kind } => {
            if let Err(error) =
                conflict_session::close_replay_request(db, &marker.id, branch, kind, answer).await
            {
                log::warn!(
                    "close replay request `{}` for `{branch}`: {error}",
                    marker.id
                );
            }
        }
        Settle::Served {
            base_branch,
            base_commit,
            record,
        } => {
            // The verdict lives on the item at the coordinate actually judged,
            // and nowhere else: without that row the request stays unserved,
            // with the reason on its own row, for the next trigger.
            let serving = match demand_intent(
                db,
                project_id,
                store,
                &base_branch,
                &base_commit,
                PreparationPurpose::OwnerCommit,
            )
            .await
            {
                Ok(intent) => intent,
                Err(refusal) => {
                    let reason = format!(
                        "the verdict on `{base_branch}` at {base_commit} could not be recorded \
                         on its coordinate ({refusal}); the request stays pending"
                    );
                    log::warn!("replay request `{}` for `{branch}`: {reason}", marker.id);
                    if let Err(error) =
                        conflict_session::note_replay_request(db, marker, &reason).await
                    {
                        log::warn!("note replay request `{}`: {error}", marker.id);
                    }
                    return;
                }
            };
            if let Some(verdict) = record {
                if let Err(error) = persist_reconcile_item(
                    db,
                    ReconcileItemUpdate {
                        intent_id: &serving,
                        bookmark: branch,
                        observed_tip: verdict.tip.as_deref(),
                        status: "completed",
                        failure_kind: verdict.failure,
                        outcome_kind: Some(verdict.outcome),
                        fingerprint: None,
                        diagnostic: Some(answer),
                    },
                )
                .await
                {
                    log::warn!(
                        "record the verdict of replay request `{}`: {error}",
                        marker.id
                    );
                    return;
                }
            }
            if let Err(error) =
                conflict_session::mark_replay_request_served(db, marker, &serving).await
            {
                log::warn!(
                    "serve replay request `{}` for `{branch}`: {error}",
                    marker.id
                );
            }
        }
    }
}

/// Serve accepted explicit replay requests left unserved, in every open
/// project database (the local one and each open team replica): the newest
/// request of each branch, or of the one branch `only` names. Never by base,
/// never a sibling.
///
/// A request is replayed only while its branch is still at the tip it was
/// accepted at, for its live owner, with a readable resolution; otherwise it
/// is closed with the reason and nothing is replayed. Requests run one after
/// another, each under its own explicit budget and the preparer's normal
/// reservation and lock waits.
pub(crate) async fn serve_pending_replay_requests(
    orch: &Orchestrator,
    only: Option<(&Path, &str)>,
) {
    let (mut served, mut closed, mut held) = (0usize, 0usize, 0usize);
    for db in orch.db.all_dbs().await {
        let (s, c, h) = serve_in(orch, &db, only, EXPLICIT_REPLAY_BUDGET).await;
        (served, closed, held) = (served + s, closed + c, held + h);
    }
    if served + closed + held > 0 {
        log::info!(
            "explicit replay recovery: {served} served, {closed} closed (superseded, ownerless or \
             malformed), {held} left held"
        );
    }
}

/// [`serve_pending_replay_requests`] over one database, each request under
/// `budget`: (served, closed, held).
async fn serve_in(
    orch: &Orchestrator,
    db: &LocalDb,
    only: Option<(&Path, &str)>,
    budget: Duration,
) -> (usize, usize, usize) {
    let listed = match conflict_session::list_pending_replay_requests(db).await {
        Ok(listed) => listed,
        Err(error) => {
            log::warn!("explicit replay recovery could not list requests: {error}");
            return (0, 0, 0);
        }
    };
    let only = match only {
        Some((store, bookmark)) => match super::preparation::journal_store_key(store) {
            Ok(key) => Some((key, bookmark)),
            Err(error) => {
                log::warn!("explicit replay recovery of `{bookmark}`: {error}");
                return (0, 0, 0);
            }
        },
        None => None,
    };
    let (mut served, mut closed, mut held) = (0usize, 0usize, 0usize);
    for listed in listed {
        let store_key = match super::preparation::journal_store_key(Path::new(&listed.store_path)) {
            Ok(key) => key,
            Err(error) => {
                log::warn!(
                    "explicit replay recovery of `{}`: {error}; left pending",
                    listed.bookmark
                );
                continue;
            }
        };
        if let Some((key, bookmark)) = &only {
            if listed.bookmark != *bookmark || store_key != *key {
                continue;
            }
        }
        // One server per branch at a time; a second finds it claimed and
        // leaves the request to the first.
        let Some(_claim) = ServingClaim::take(&store_key, &listed.bookmark) else {
            continue;
        };
        // Under the claim, the request must still be the branch's newest
        // unserved one: another server may have served or superseded it.
        match conflict_session::load_pending_replay_request(
            db,
            Path::new(&listed.store_path),
            &listed.bookmark,
        )
        .await
        {
            Ok(Some(current)) if current.id == listed.id => {}
            Ok(_) => continue,
            // An unreadable resolution is the malformed case, closed below.
            Err(_) if listed.request.is_err() => {}
            Err(error) => {
                log::warn!("explicit replay recovery of `{}`: {error}", listed.bookmark);
                continue;
            }
        }
        let mut request = match listed.request.clone() {
            Ok(request) => request,
            Err(reason) => {
                close_request(db, &listed.id, &listed.bookmark, "malformed", &reason).await;
                closed += 1;
                continue;
            }
        };
        // A following round the served request opened is served here, under
        // the same claim: a server spawned for it while this one holds the
        // claim would find the branch claimed and leave it, and nothing else
        // would wake it before the next start.
        loop {
            match serve_one(orch, db, &request, budget).await {
                ServeOutcome::Served => served += 1,
                ServeOutcome::Held => {
                    held += 1;
                    break;
                }
                ServeOutcome::Closed => {
                    closed += 1;
                    break;
                }
                ServeOutcome::Skipped => break,
            }
            match conflict_session::load_pending_replay_request(
                db,
                Path::new(&request.store_path),
                &request.bookmark,
            )
            .await
            {
                Ok(Some(child)) if child.parent_request_id.as_deref() == Some(&request.id) => {
                    request = child;
                }
                _ => break,
            }
        }
    }
    (served, closed, held)
}

enum ServeOutcome {
    Served,
    Held,
    Closed,
    Skipped,
}

async fn close_request(db: &LocalDb, id: &str, bookmark: &str, kind: &str, reason: &str) {
    log::info!("explicit replay recovery closed `{id}` for `{bookmark}`: {reason}");
    if let Err(error) = conflict_session::close_replay_request(db, id, bookmark, kind, reason).await
    {
        log::warn!("close replay request `{id}`: {error}");
    }
}

/// Serve one recovered request under its branch's serving claim: only while
/// its branch is still at the tip it was accepted at, for its live owner.
async fn serve_one(
    orch: &Orchestrator,
    db: &LocalDb,
    request: &PendingReplayRequest,
    budget: Duration,
) -> ServeOutcome {
    // From here the request's live owner is known: every close tells it,
    // once, through the one path.
    let close_noted = |kind: &'static str, reason: String, owner: String| async move {
        close_request(db, &request.id, &request.bookmark, kind, &reason).await;
        note_owner_closed(orch, db, &owner, request, &reason);
    };
    let owner = match live_branch_owner(db, &request.project_id, &request.bookmark).await {
        Ok(Some(owner)) => owner,
        Ok(None) => {
            close_request(
                db,
                &request.id,
                &request.bookmark,
                "no_owner",
                &format!("no live job owns `{}` any more", request.bookmark),
            )
            .await;
            return ServeOutcome::Closed;
        }
        Err(error) => {
            log::warn!(
                "explicit replay recovery: owner of `{}`: {error}",
                request.bookmark
            );
            return ServeOutcome::Skipped;
        }
    };
    let store = Path::new(&request.store_path);
    let head = match cairn_vcs::resolve_coordinate(store, &request.bookmark).await {
        Ok(head) => head,
        Err(error) => {
            close_noted(
                "superseded",
                format!("`{}` could not be resolved: {error}", request.bookmark),
                owner,
            )
            .await;
            return ServeOutcome::Closed;
        }
    };
    if request.requested_tip.as_deref() != Some(head.as_str()) {
        let reason = format!(
            "`{}` moved from {} to {head} since this replay request was accepted; nothing was \
             replayed. Request the replay again from the branch as it is now.",
            request.bookmark,
            request
                .requested_tip
                .as_deref()
                .unwrap_or("an unrecorded tip")
        );
        close_noted("superseded", reason, owner).await;
        return ServeOutcome::Closed;
    }
    // A following round is worded against the base its parent landed.
    let landed_base = match request.parent_request_id.as_deref() {
        Some(parent) => conflict_session::replay_request_destination(db, parent)
            .await
            .unwrap_or_else(|error| {
                log::warn!("explicit replay recovery: {error}");
                None
            }),
        None => None,
    };
    let answer = replay(
        orch,
        db,
        &ExplicitReplay {
            job_id: &owner,
            branch: &request.bookmark,
            expected_fingerprint: None,
            take_committed_tip: request.take_committed_tip,
            drop_incoming_reason: request.drop_incoming_reason.as_deref(),
            budget,
            step_minimum: EXPLICIT_STEP_MINIMUM,
            recovered: Some(request.clone()),
            increment_from: landed_base.as_deref(),
        },
    )
    .await;
    log::info!(
        "explicit replay recovery of `{}` (request `{}`): {}",
        request.bookmark,
        request.id,
        match &answer {
            Ok(answer) | Err(answer) => answer.lines().next().unwrap_or_default(),
        }
    );
    match conflict_session::replay_request_state(db, request).await {
        Ok(conflict_session::ReplayRequestState::Pending) => ServeOutcome::Held,
        Ok(_) => ServeOutcome::Served,
        Err(error) => {
            log::warn!("replay request `{}` state: {error}", request.id);
            ServeOutcome::Skipped
        }
    }
}

/// The (canonical store, branch) pairs a recovery server is serving now.
fn serving() -> &'static std::sync::Mutex<std::collections::HashSet<(String, String)>> {
    static SERVING: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashSet<(String, String)>>,
    > = std::sync::OnceLock::new();
    SERVING.get_or_init(Default::default)
}

/// A claim on serving one branch's request, released on drop.
struct ServingClaim((String, String));

impl ServingClaim {
    fn take(store_key: &str, bookmark: &str) -> Option<Self> {
        let key = (store_key.to_string(), bookmark.to_string());
        let claimed = serving()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key.clone());
        // Built only after the guard is released and only on success: a claim
        // dropped here would lock the set again and release the holder's key.
        if claimed {
            Some(Self(key))
        } else {
            None
        }
    }
}

impl Drop for ServingClaim {
    fn drop(&mut self) {
        serving()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.0);
    }
}

/// The one seam a released branch hold calls: a run batch's wait resolved or
/// was cancelled by a Stop, or was settled because its node cannot resume.
///
/// Spawned, so it never runs inside the releasing batch's guard, reservation
/// or permit; it takes its own through normal preparation. It finds the job's
/// own database and exact branch, does nothing while any batch still holds
/// that branch, and otherwise serves that branch's newest unserved request.
pub(crate) fn on_branch_hold_released(orch: &Orchestrator, job_id: &str) {
    let (orch, job_id) = (orch.clone(), job_id.to_string());
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        log::warn!("branch hold of job {job_id} released outside a runtime; served at next start");
        return;
    };
    runtime.spawn(async move { release_hold(&orch, &job_id).await });
}

/// [`on_branch_hold_released`] for the run whose waits were cancelled.
pub(crate) fn on_branch_hold_released_for_run(orch: &Orchestrator, run_id: &str) {
    let (orch, run_id) = (orch.clone(), run_id.to_string());
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        log::warn!("branch hold of run {run_id} released outside a runtime; served at next start");
        return;
    };
    runtime.spawn(async move {
        let db = match crate::execution::routing::owning_db_for_run(&orch.db, &run_id).await {
            Ok(db) => db,
            Err(error) => {
                log::warn!("hold release of run {run_id}: {error}");
                return;
            }
        };
        let job = match db
            .query_text("SELECT job_id FROM runs WHERE id = ?1", (run_id.clone(),))
            .await
        {
            Ok(Some(job)) => job,
            Ok(None) => return,
            Err(error) => {
                log::warn!("hold release of run {run_id}: {error}");
                return;
            }
        };
        release_hold(&orch, &job).await;
    });
}

async fn release_hold(orch: &Orchestrator, job_id: &str) {
    let db = match crate::execution::routing::owning_db_for_job(&orch.db, job_id).await {
        Ok(db) => db,
        Err(error) => {
            log::warn!("hold release of job {job_id}: {error}");
            return;
        }
    };
    let job = job_id.to_string();
    let found = db
        .read(move |conn| {
            let job = job.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT j.branch, p.repo_path FROM jobs j
                         JOIN projects p ON p.id = j.project_id WHERE j.id = ?1",
                        params![job],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((row.opt_text(0)?, row.opt_text(1)?))),
                    None => Ok(None),
                }
            })
        })
        .await;
    let (branch, repo) = match found {
        Ok(Some((Some(branch), Some(repo)))) if !branch.is_empty() => (branch, repo),
        Ok(_) => return,
        Err(error) => {
            log::warn!("hold release of job {job_id}: {error}");
            return;
        }
    };
    match super::inflight_run_batch_on_branch(&db, job_id, &branch).await {
        Ok(None) => {}
        // Another batch still holds the branch: its own release serves it.
        Ok(Some(_)) => return,
        Err(error) => {
            log::warn!("hold release of `{branch}`: {error}");
            return;
        }
    }
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo));
    let (served, closed, held) =
        serve_in(orch, &db, Some((&store, &branch)), EXPLICIT_REPLAY_BUDGET).await;
    if served + closed + held > 0 {
        log::info!(
            "hold release of `{branch}`: {served} served, {closed} closed, {held} still held"
        );
    }
}

/// Both of these name a session's own artifacts. A request carrying one
/// without a session describes a world that does not exist, so it is answered
/// with which artifact is missing rather than reinterpreted.
fn refuse_sessionless_artifacts(
    branch: &str,
    session: Option<&ConflictSession>,
    ask: &ExplicitReplay<'_>,
) -> Result<(), String> {
    if session.is_some() {
        return Ok(());
    }
    if ask.expected_fingerprint.is_some() {
        return Err(format!(
            "`{branch}` has no open rebase session, so there are no session coordinates for a \
             fingerprint to pin. Request the replay without one."
        ));
    }
    if ask.take_committed_tip {
        return Err(format!(
            "`resolution:\"take-committed-tip\"` restores an open session's CONFLICTING paths \
             from your branch's committed tip, and `{branch}` has no open session, so there \
             is no such set of paths. Request a plain `{{action:\"replay\"}}`: it moves your \
             branch onto the current base without taking content from either side."
        ));
    }
    Ok(())
}

/// Re-point a recorded base the store has proven gone (usually a parent that
/// merged and was deleted) before anything is prepared against it, and say so
/// with every answer: an agent told only the replay's outcome learns nothing
/// about the base that changed underneath it.
#[allow(clippy::too_many_arguments)]
async fn repoint_superseded_base(
    orch: &Orchestrator,
    db: &LocalDb,
    jj: &crate::jj::JjEnv,
    store: &Path,
    project_id: &str,
    ask: &ExplicitReplay<'_>,
    session: Option<&ConflictSession>,
    budget: &Budget,
) -> Result<String, String> {
    let branch = ask.branch;
    let lock_wait = budget.admit(branch, "the base resolution")?;
    let session_base = session.map(|session| {
        if session.incoming.base_branch.is_empty() {
            session.target_branch.as_str()
        } else {
            session.incoming.base_branch.as_str()
        }
    });
    let candidates = crate::orchestrator::replay_base::load_base_candidates_for_job(db, ask.job_id)
        .await?
        .with_session_fallback(session_base);
    // An undeclared base follows the project default, as the preparer reads
    // it; there is no recorded name that could have vanished.
    if candidates.recorded.is_none() {
        return Ok(String::new());
    }
    let guard = orch
        .acquire_jj_store_lock_with_timeout(
            store,
            crate::orchestrator::StoreLockClass::Foreground,
            format!("explicit replay base resolution for {branch}"),
            Some(lock_wait.min(crate::mcp::vcs::STORE_LOCK_TIMEOUT)),
        )
        .await
        .map_err(|()| {
            format!(
                "The project's version-control store stayed busy for the {:.1}s this explicit \
                 replay had to resolve `{branch}`'s base, so no replay was accepted and `{branch}` \
                 is unchanged. No automatic cooldown was recorded; request it again at once.",
                lock_wait.as_secs_f64()
            )
        })?;
    let (resolved, recoveries) = {
        let _phase = guard.phase(format!("resolve replay destination branch={branch}"));
        let jj = jj.with_operation_budget(crate::jj::BACKGROUND_RECONCILE_BUDGET);
        let resolved =
            crate::orchestrator::replay_base::resolve_base(&jj, store, branch, &candidates);
        (resolved, jj.take_recoveries())
    };
    // Persisted before the guard is released: a resolution read that killed a
    // writer not proven gone fences the store.
    if let Some(fenced) = super::preparation::persist_preparation_recoveries(
        db,
        project_id,
        store,
        branch,
        true,
        "explicit replay's base resolution",
        recoveries,
    )
    .await
    {
        drop(guard);
        crate::orchestrator::branch_recovery::deliver_pending_notices(orch, db).await;
        return Err(fenced);
    }
    let resolved = resolved.map_err(|error| error.to_string())?;
    let Some(superseded) = resolved.superseded.as_deref() else {
        return Ok(String::new());
    };
    crate::orchestrator::replay_base::repoint_recorded_base(
        db,
        &guard,
        ask.job_id,
        &resolved.branch,
    )
    .await?;
    Ok(format!(
        "The base `{superseded}` this branch recorded no longer exists in the store — the usual \
         cause is that its parent merged and the branch was deleted with it. So it is measured \
         against `{}`, {}, and the recorded base has been re-pointed there so the surfaces that \
         read it agree.\n\n",
        resolved.branch,
        resolved.source.describe()
    ))
}

/// A take-committed-tip carry proven at one exact head and planned onto one
/// exact base commit.
struct PlannedCarry {
    base_branch: String,
    base: String,
    /// `base` is the session's recorded base, captured exactly however far the
    /// live base has moved.
    exact: bool,
    /// Every path the plan places or deliberately leaves to the merge.
    placed: std::collections::BTreeSet<String>,
    plan: CarryPlan,
}

enum PlanStop {
    /// Refused before acceptance: the proof does not hold.
    Refused(String),
    /// Accepted, but a step ran out of the explicit budget.
    Answer(String),
}

/// Prove the committed tip, record what the proof accepted, and plan the carry
/// onto the base the preparer will capture.
#[allow(clippy::too_many_arguments)]
async fn plan_take_committed_tip(
    db: &LocalDb,
    jj: &crate::jj::JjEnv,
    store: &Path,
    project_id: &str,
    ask: &ExplicitReplay<'_>,
    mut session: ConflictSession,
    budget: &Budget,
    caveats: &mut Vec<String>,
) -> Result<PlannedCarry, PlanStop> {
    let branch = ask.branch;
    const ASSESSMENT: &str = "the take-committed-tip assessment";
    // EXHAUSTIVE, not the read path's capped form: the restore covers every
    // conflicting path, so the decision has to. A stated reason is also the
    // resolution of every file the tip deletes, entered before the assessment.
    // Read once: the guard judges exactly what this replay will place.
    let merge_mode = crate::orchestrator::catch_up::mode_for(db, project_id, store, branch)
        .await
        .map_err(PlanStop::Refused)?
        .is_some();
    let placed = placed_scope(merge_mode);
    let left = budget.admit(branch, ASSESSMENT).map_err(PlanStop::Answer)?;
    let scope = AssessmentScope {
        phase: "take-committed-tip assessment",
        branch,
        candidate_paths: session
            .replay_paths(&session.resolution_basis, placed)
            .len(),
    };
    let (assessed_store, assessed_session) = (store.to_path_buf(), session.clone());
    let reason = ask.drop_incoming_reason.map(str::to_string);
    let proof = conflict_session::bounded_store_assessment(jj, left, scope, move |jj| {
        Ok(conflict_session::assess_session_tip_exhaustively(
            jj,
            &assessed_store,
            &assessed_session,
            reason.as_deref(),
            placed,
        ))
    })
    .await
    .map_err(|error| {
        if crate::jj::is_jj_timeout_error(&error) {
            PlanStop::Answer(timed_out_step(branch, ASSESSMENT, &error))
        } else {
            PlanStop::Refused(format!(
                "Refusing this replay: whether your committed tip keeps every incoming change \
                 could not be assessed ({error}). Nothing was installed; re-read cairn:~/rebase \
                 and request again."
            ))
        }
    })?;
    if let RestoreProof::Unavailable(why) = &proof {
        if crate::jj::is_jj_timeout_error(why) {
            return Err(PlanStop::Answer(timed_out_step(branch, ASSESSMENT, why)));
        }
    }
    let tip = match &proof {
        RestoreProof::Assessed(assessment) => Some(assessment.tip.clone()),
        _ => crate::jj::bookmark_commit(jj, store, branch),
    }
    .ok_or_else(|| {
        PlanStop::Refused(format!(
            "`{branch}` did not resolve to a commit in the project store, so there is no \
             committed tip to replay."
        ))
    })?;
    match conflict_session::decide_replay(&proof, ask.drop_incoming_reason) {
        ReplayDecision::Proceed => {}
        ReplayDecision::Refuse(refusal) => return Err(PlanStop::Refused(refusal)),
        ReplayDecision::ProceedOnStatedReason(caveat) => {
            log::warn!(
                "replay request for `{branch}`: proceeding with an unproven take-committed-tip \
                 restore on the requester's stated reason. {caveat}"
            );
            if !caveats.contains(&caveat) {
                caveats.push(caveat);
            }
        }
    }
    // Only an accepted request records its statement.
    if let RestoreProof::Assessed(assessment) = &proof {
        if !assessment.newly_attested.is_empty() {
            let mut stored = session.resolution_basis.clone();
            stored.extend(assessment.attested.clone());
            conflict_session::record_attested_basis(db, &session.intent_id, branch, &stored)
                .await
                .map_err(PlanStop::Refused)?;
            session.resolution_basis = stored;
            let paths: Vec<String> = assessment
                .newly_attested
                .iter()
                .map(|path| format!("`{path}`"))
                .collect();
            caveats.push(format!(
                "Recorded your statement (\"{}\") as the resolution of {} deleted file(s) \
                 against `{}`: {}. A replay carries each deletion as long as the base has \
                 not changed the file since; cairn:~/rebase lists them.",
                ask.drop_incoming_reason.unwrap_or_default(),
                paths.len(),
                assessment.recorded.theirs,
                paths.join(", ")
            ));
        }
    }

    // The base the preparer will capture: the session's recorded base, which
    // this resolution was made against, while it is still in the declared
    // base's history. The relationship is read here without the lock only to
    // choose what to plan onto; the preparer re-reads it under the lock,
    // captures exactly this commit and refuses a carry planned onto any other.
    let relationship = load_relationship(db, project_id, branch, Some(ask.job_id), None)
        .await
        .map_err(|refusal| PlanStop::Refused(refusal.to_string()))?;
    let live = cairn_vcs::resolve_coordinate(store, &relationship.base_branch)
        .await
        .map_err(|error| {
            PlanStop::Refused(format!(
                "base `{}` could not be resolved: {error}",
                relationship.base_branch
            ))
        })?;
    let recorded = session.destination_commit.clone();
    // A session recorded before captures kept the base's name has the commit
    // standing in for it.
    let same_base =
        session.target_branch == relationship.base_branch || session.target_branch == recorded;
    let exact = same_base
        && (recorded == live
            || cairn_vcs::history::contains(store, &recorded, &live)
                .await
                .unwrap_or(false));
    // A base rewritten under the session has no recorded commit left to land
    // on: the resolution is judged against the live base, as it always was.
    let base = if exact { recorded } else { live };
    // What this plan places, and so what a conflict at the same base already
    // accounts for: anything else is an inventory the session did not list.
    let mut placed: std::collections::BTreeSet<String> = session
        .replay_paths(&session.resolution_basis, placed_scope(merge_mode))
        .into_iter()
        .collect();
    if merge_mode {
        let paths = session
            .conflicting()
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        return Ok(PlannedCarry {
            base_branch: relationship.base_branch,
            base,
            exact,
            placed,
            plan: CarryPlan {
                carry: crate::jj::ResolutionCarry::from_tip(&tip, &paths),
                refused: Vec::new(),
            },
        });
    }
    // The plan's committed side is the tip just proven, whatever an earlier
    // request recorded. The request itself is recorded once, by its marker.
    session.replay_requested_tip = Some(tip.clone());

    const CARRY: &str = "the resolution carry plan";
    let left = budget.admit(branch, CARRY).map_err(PlanStop::Answer)?;
    let scope = AssessmentScope {
        phase: "resolution carry plan",
        branch,
        candidate_paths: session
            .replay_paths(
                &session.resolution_basis,
                conflict_session::ReplayScope::Carry,
            )
            .len(),
    };
    let (planned_store, planned_base) = (store.to_path_buf(), base.clone());
    let plan = conflict_session::bounded_store_assessment(jj, left, scope, move |jj| {
        plan_resolution_carry(jj, &planned_store, &session, &planned_base, &tip)
    })
    .await
    .map_err(|error| {
        if crate::jj::is_jj_timeout_error(&error) {
            PlanStop::Answer(timed_out_step(branch, CARRY, &error))
        } else {
            PlanStop::Refused(format!(
                "Your committed resolution could not be planned onto `{}` at `{base}`: {error}. \
                 Nothing was installed.",
                relationship.base_branch
            ))
        }
    })?;
    placed.extend(plan.carry.paths.iter().map(|(path, _)| path.clone()));
    placed.extend(plan.refused.iter().map(|(path, _)| path.clone()));
    Ok(PlannedCarry {
        base_branch: relationship.base_branch,
        base,
        exact,
        placed,
        plan,
    })
}

fn placed_scope(merge_mode: bool) -> conflict_session::ReplayScope {
    if merge_mode {
        conflict_session::ReplayScope::CatchUpMerge
    } else {
        conflict_session::ReplayScope::Carry
    }
}

/// The following round of a request that landed `landed` on its session's
/// base X: when the live base has moved past X, its commits since X are
/// judged as a plain request onto the live base, queued as the parent's child
/// and served here if the parent's budget still admits it, otherwise by the
/// recovery server for this branch. `None` when the live base is X: one round.
#[allow(clippy::too_many_arguments)]
async fn increment_round(
    orch: &Orchestrator,
    db: &LocalDb,
    ask: &ExplicitReplay<'_>,
    store: &Path,
    project_id: &str,
    landed: &PreparedBranch,
    parent: Option<&PendingReplayRequest>,
    budget: &Budget,
) -> Option<String> {
    let branch = ask.branch;
    let base_branch = &landed.relationship.base_branch;
    let recorded = landed.base_commit.as_str();
    let live = match cairn_vcs::resolve_coordinate(store, base_branch).await {
        Ok(live) => live,
        Err(error) => {
            return Some(format!(
                "Whether `{base_branch}` moved past `{recorded}` could not be read ({error}); \
                 request a plain replay to bring `{branch}` onto it."
            ))
        }
    };
    if live == recorded {
        return None;
    }
    // A child on its parent's own coordinate would replace the parent's row.
    let Some(parent) = parent.filter(|parent| parent.destination_commit != live) else {
        return Some(format!(
            "`{base_branch}` is now `{live}`; request a plain replay to judge its commits since \
             `{recorded}`."
        ));
    };
    let child = match conflict_session::queue_increment_request(
        db,
        project_id,
        store,
        base_branch,
        &live,
        branch,
        &landed.head_after,
        parent,
    )
    .await
    {
        Ok(child) => child,
        Err(error) => {
            return Some(format!(
                "`{base_branch}` is now `{live}`, but the round for its commits since \
                 `{recorded}` could not be recorded ({error}); request a plain replay to judge \
                 them."
            ))
        }
    };
    let lead = format!(
        "`{base_branch}` is now `{live}`, so its commits since `{recorded}` are judged as a \
         following round (request `{}`):",
        child.id
    );
    let admitted = budget.admit(branch, "the following round");
    #[cfg(test)]
    let admitted = if tests::increment_deferred(store) {
        Err(String::new())
    } else {
        admitted
    };
    let Ok(left) = admitted else {
        // A recovered parent's server holds the branch's claim and drains
        // this child itself. Otherwise the branch's own serving seam serves
        // the newest request, this one, unless a run batch still holds it.
        if ask.recovered.is_none() {
            on_branch_hold_released(orch, ask.job_id);
        }
        return Some(format!(
            "{lead} this request's budget is spent, so it is served next on its own; you do \
             not need to request it. cairn:~/rebase shows its verdict."
        ));
    };
    let answer = Box::pin(replay(
        orch,
        db,
        &ExplicitReplay {
            job_id: ask.job_id,
            branch,
            expected_fingerprint: None,
            take_committed_tip: false,
            drop_incoming_reason: None,
            budget: left,
            step_minimum: ask.step_minimum,
            recovered: Some(child),
            increment_from: Some(recorded),
        },
    ))
    .await;
    Some(match answer {
        Ok(answer) | Err(answer) => format!("{lead} {answer}"),
    })
}

fn timed_out_step(branch: &str, step: &str, error: &str) -> String {
    format!(
        "Explicit replay of `{branch}` timed out in {step}: {error}. Nothing was installed and \
         `{branch}` is unchanged. This step is not the replay's computation, so no automatic \
         cooldown and no repeat block was recorded. An unchanged branch needs the same time for \
         it again: if it times out again, report it on cairn://p/cairn/execution rather than \
         repeating it.\n\n{}",
        super::replay_cost::merge_catch_up_exit()
    )
}

/// The answer for an installed (or already carried) coordinate.
async fn landed(
    db: &LocalDb,
    branch: &str,
    had_session: bool,
    prepared: &PreparedBranch,
    merge_mode: bool,
) -> String {
    let base = &prepared.relationship.base_branch;
    let on_recorded = if prepared.pinned_base {
        " (your resolution landed on the base it was made against)"
    } else {
        ""
    };
    let mut out = if prepared.advanced && merge_mode {
        format!(
            "Merged `{base}` at `{}` into `{branch}`{on_recorded}: `{}` → `{}`, tree `{}`. No prior branch commit was rewritten. The conflict session is closed. A clean merge is not a green build: re-run your targeted build.",
            prepared.base_commit, prepared.head_before, prepared.head_after, prepared.tree,
        )
    } else if prepared.advanced {
        format!(
            "Replayed `{branch}` onto `{base}` at `{}`{on_recorded}: `{}` → `{}`, tree `{}`. The \
             rebase session is closed. A clean replay is not a green build: re-run your targeted \
             build against the new base.",
            prepared.base_commit, prepared.head_before, prepared.head_after, prepared.tree
        )
    } else {
        // Nothing to replay. An open session describes a conflict this
        // ancestry already absorbed, so it is closed rather than left to
        // demand a replay that cannot do anything.
        if had_session {
            if let Err(error) = conflict_session::close_open_sessions_for_branch(db, branch).await {
                log::warn!("close the absorbed rebase session of `{branch}`: {error}");
            }
        }
        format!(
            "`{branch}` already carries `{base}` at `{}` in its ancestry, so there is nothing to \
             replay. If its pull request still reports a conflict, that is a different problem \
             from a stale base — file it rather than replaying again.",
            prepared.base_commit
        )
    };
    if let PreparedExport::Failed { error, .. } = &prepared.export {
        out.push_str(&format!(
            "\n\n⚠️ The advance is installed in the store but its Git export failed ({error}); \
             the next preparation of this branch re-exports it."
        ));
    }
    for warning in &prepared.warnings {
        out.push_str(&format!("\n\n⚠️ {warning}"));
    }
    out
}

/// The answer for a replay that ran and did not install, or the refusal (as
/// `Err`) for one that could not be attempted.
fn not_landed(
    branch: &str,
    refusal: PreparationRefusal,
    carry_refusals: Option<&str>,
    ask: &ExplicitReplay<'_>,
) -> Result<String, String> {
    match refusal {
        PreparationRefusal::Conflict(conflict) => {
            let super::preparation::BaseConflict {
                base_branch,
                base_commit,
                head,
                paths,
                record,
                ..
            } = *conflict;
            let listed = paths.join(", ");
            let held = match record {
                ConflictRecord::Session { reopened: true, .. } => {
                    "cairn:~/rebase now holds the conflict against this base."
                }
                _ => "cairn:~/rebase still holds this conflict.",
            };
            // A stale resolution never reads as a wrong one: a following
            // round names only what the base's newer commits conflict on, and
            // a resolution judged at its own base names what it leaves open.
            let mut out = if let Some(landed) = ask.increment_from {
                format!(
                    "Resolved against `{landed}`; `{base_branch}` is now `{base_commit}`; these \
                     files conflict with `{base_commit}`'s new commits: {listed}. Nothing of this \
                     round was installed. cairn:~/rebase holds that session against \
                     `{base_commit}`."
                )
            } else if ask.take_committed_tip && !paths.is_empty() {
                format!(
                    "Explicit replay of `{branch}` at `{head}` onto `{base_branch}` at \
                     `{base_commit}` still conflicts: your committed tip does not yet resolve \
                     `{base_commit}` in: {listed}. Nothing was installed and `{branch}` is \
                     unchanged. {held}"
                )
            } else {
                format!(
                    "Explicit replay of `{branch}` at `{head}` onto `{base_branch}` at \
                     `{base_commit}` still conflicts{}. Nothing was installed and `{branch}` is \
                     unchanged. {held}",
                    if paths.is_empty() {
                        String::new()
                    } else {
                        format!(" in {listed}")
                    },
                )
            };
            if let Some(refusals) = carry_refusals {
                out.push_str("\n\n");
                out.push_str(refusals);
            }
            Ok(out)
        }
        // Guidance from the exact blocker this attempt recorded, never one
        // matched by head and base alone.
        PreparationRefusal::TimedOut {
            head,
            base_branch,
            base_commit,
            budget,
            blocker,
        } => Ok(format!(
            "Explicit replay of `{branch}` at `{head}` onto `{base_branch}` at `{base_commit}` \
             exceeded the {:.1}s left of its explicit budget during the rebase, export and \
             ancestry heal. Nothing was installed and `{branch}` is unchanged. No automatic \
             cooldown was recorded. cairn:~/rebase shows what this attempt measured.\n\n{}",
            budget.as_secs_f64(),
            super::replay_cost::timeout_guidance(blocker.as_deref())
        )),
        PreparationRefusal::RepeatTimeout(blocker) => Err(format!(
            "Explicit replay of `{branch}` was not attempted, and `{branch}` is unchanged. {}\n\n{}",
            super::replay_cost::blocked_guidance(&blocker),
            super::replay_cost::merge_catch_up_exit()
        )),
        PreparationRefusal::Busy(BusyReason::RunBatch(batch)) => Err(format!(
            "{batch} may still commit to `{branch}`, so it is not rewritten under it and no \
             replay was accepted yet. Cancelling that batch ends the hold now: request the \
             replay with `cancel_holding_batch:true`, which cancels it first, or patch \
             `action:\"cancel\"` on the batch's own address, which cairn:~/rebase names."
        )),
        PreparationRefusal::Busy(BusyReason::StoreLock { holder }) => Err(format!(
            "The project's version-control store stayed busy (held by {}) for the rest of this \
             explicit replay's budget, so no replay was accepted and `{branch}` is unchanged. No \
             automatic cooldown was recorded; request it again at once.",
            holder.as_deref().unwrap_or("another operation")
        )),
        PreparationRefusal::Busy(BusyReason::Reservation(holder)) => Err(format!(
            "`{branch}` stayed reserved by {holder} for the rest of this explicit replay's \
             budget, so no replay was accepted and `{branch}` is unchanged. No automatic cooldown \
             was recorded; request it again once that operation finishes."
        )),
        // A fence may have been left by this replay itself, so the branch is
        // never called unchanged here.
        PreparationRefusal::Fenced { refusal } => Err(format!(
            "Explicit replay of `{branch}` was not installed. {refusal}"
        )),
        PreparationRefusal::Busy(BusyReason::LegacyIntent { intent_id, status }) => Err(format!(
            "The background reconcile still owns this base coordinate (intent `{intent_id}` is \
             {status}), so no replay was accepted. Request it again once that intent settles."
        )),
        other => Err(format!(
            "Explicit replay of `{branch}` was not installed: {other}. `{branch}` is unchanged."
        )),
    }
}

fn with_caveats(mut out: String, caveats: &[String]) -> String {
    for caveat in caveats {
        out.push_str("\n\n⚠️ ");
        out.push_str(caveat);
    }
    out
}

struct ReplayProject {
    id: String,
    repo_path: String,
}

async fn load_replay_project(db: &LocalDb, job_id: &str) -> Result<ReplayProject, String> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT p.id, p.repo_path
                     FROM jobs j JOIN projects p ON j.project_id = p.id
                     WHERE j.id = ?1 LIMIT 1",
                    params![job_id],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            Ok(Some(ReplayProject {
                id: row.text(0)?,
                repo_path: row.text(1)?,
            }))
        })
    })
    .await
    .map_err(|error| format!("load replay project: {error}"))?
    .ok_or_else(|| "This node has no project repository to replay against.".to_string())
}

#[cfg(test)]
#[path = "replay_requests_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "replay_requests_corpus.rs"]
mod corpus;
