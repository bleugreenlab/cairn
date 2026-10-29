//! Turn-end (`when:idle` / `when:review`) project-check cadence.
//!
//! Where the `when:write` runner ([`crate::execution::checks`]) fires mid-turn
//! against a just-sealed commit and streams into the live transcript, this cadence
//! fires at TURN-END — when the agent goes idle — where there is no running turn
//! and no `RunContext` to stream into. It is invoked from the two turn-end hooks in
//! `orchestrator::lifecycle` (`finalize_run` and `transition_to_warm_state`),
//! detached onto a background task so the minutes-long suite never blocks the turn
//! from ending.
//!
//! ## Cadence gate
//!
//! `when:review` checks (including the `idle` legacy alias) run at every
//! turn-end; `when:write` never runs here (it is the mid-turn cadence). "Every
//! turn-end" means every turn that actually ENDS: a turn that yields by
//! self-suspension (`waitFor`, a durably-suspended `run` batch, a blocking
//! question or task append) leaves the job mid-work, and the trigger
//! (`lifecycle::review_push::spawn_turn_end_checks`) skips it — see the gate
//! there for why, and `docs/checks.md` for the trade it makes.
//! Selection reuses the write cadence's machinery
//! ([`crate::execution::selection::plan_checks`], the impact gate, placeholder
//! substitution) via [`crate::execution::checks::applicable_turn_end_checks`], and
//! results share the `check_result_cache` keyed by each check's input hash.
//!
//! ## Unsandboxed by design
//!
//! At turn-end the agent is idle, so an interactive fence permission prompt would
//! hang with no one to answer. The suite therefore runs UNCONFINED, with host
//! permissions: these are trusted, system-driven project-config commands read
//! from the live main checkout — the identical trust basis as the write cadence.
//! The decision itself lives with the batch submission, in
//! [`crate::fleet::Fleet::CHECK_CADENCE_SANDBOX_MODE`], which both cadences share.
//!
//! This is load-bearing rather than incidental. macOS sandboxes do not nest, so a
//! confined suite exits `71` the moment a test spawns its own `sandbox-exec`, and
//! the `CAIRN_SANDBOXED=1` that accompanies any policy makes fence-sensitive tests
//! self-skip — which is how this lane came to be structurally red on every branch
//! (CAIRN-3124). Confinement is not what keeps a review check from publishing:
//! [`crate::fleet::MutationPolicy::PureVerdict`] is.
//!
//! ## No fold
//!
//! The turn is over and there is no commit to amend, so check-made changes are NOT
//! folded (unlike the write cadence). Turn-end checks are pure verifies (tests),
//! not fixers; a verify that dirties tracked files would leave the worktree != HEAD
//! and is out of contract for this cadence.
//!
//! ## Slot-backed concurrency
//!
//! Cache-miss review checks run sequentially through one persistent cell lease. The
//! slot scheduler owns admission and backpressure; the shared check engine still
//! owns caching, parsing, ordered results, cancellation, and wake delivery. There
//! is no clone or in-place fallback. Substrate failures become infrastructure
//! verdicts and the command is never invoked elsewhere.
//!
//! ## Two guards keep it from looping
//!
//! - Single-flight (`Orchestrator::try_begin_turn_end_checks`): a rapid re-idle
//!   never stacks a second suite for the same job.
//! - Delivery-state dampening: red checks may intentionally execute again, but an
//!   unchanged sealed tree plus unchanged *actionable* verdicts wakes the author
//!   only once. Identity is each lane's pass/fail verdict about the change; a lane
//!   with no verdict (infrastructure, capacity, timeout, runner or spawn error,
//!   suppression, or dropped before running) contributes nothing to it, so that
//!   state lives only in the passive record and the renderer. A genuine lane
//!   appearing, disappearing, or flipping wakes normally.
//!
//! ## The node's own work has right of way
//!
//! A wave plans at turn-end but takes admission minutes later, and by then the
//! node it checks is often working again — running its own suite, or a
//! `cairn check run` it invoked. Launching there put the review lanes into
//! contention with the very commands the agent is iterating on, and the node's
//! next commit then cancelled the wave mid-suite, having already spent a
//! placement and part of a heavy compile.
//!
//! So the wave yields: it waits for the node to go quiet before it asks any
//! machine for room ([`defer_until_node_settles`]). A commit landing during that
//! wait withdraws the wave for free, before admission; a wave that reaches the
//! machine reaches it against a tree the node has stopped moving. The wait is
//! bounded and always ends in a wave that runs — see [`WAVE_SETTLE_CEILING_MS`]
//! for why deferral may never become suppression.

use std::collections::BTreeSet;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use cairn_common::uri::{build_node_checks_uri, build_task_checks_uri};
use sha2::{Digest, Sha256};

use crate::execution::checks::{
    applicable_turn_end_checks, check_input_hashes, load_checks_contract_at_commit,
    resolve_check_timeout_ms, run_planned_checks_at_commit_with_coordinates,
    submit_planned_check_batch, CheckExecMode, CheckFailureKind, CheckOutcome,
    PlannedCheckBatchItem, PlannedCheckBatchRequest, DEFAULT_REVIEW_CHECK_TIMEOUT_MS,
};
use crate::execution::inputs::{
    any_check_declares_inputs, ResolvedInputs, TreeBlobs, TreeSnapshot,
};
use crate::execution::selection::CheckPlan;
use crate::execution::wire::CheckResourceIdentityInput;
use crate::fleet::CellPriority;
use crate::jj::{logical_changed_files, logical_tree_hash, tree_entries};
use crate::orchestrator::{attention_push, Orchestrator, TurnEndCancel, TurnEndCancelReason};
use crate::storage::{LocalDb, RowExt};

/// Internal batch input carrying newline-delimited changed paths to the executor,
/// which materializes them and sets `CAIRN_CHECK_CHANGED_FILES` to the local path.
/// Chars of the live log file surfaced in the "running" render.
const LOG_TAIL_CHARS: usize = 2_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnEndAttempt {
    pub id: String,
    pub state: String,
    pub reason: Option<String>,
}

/// The immutable facts a review wave planned with: the head it pinned, the fork
/// point it measured that head's change from, the head's tree, the checks
/// contract the head declares (with the checks this build cannot run), which of
/// those checks apply, and every check's input hash at the tree.
///
/// They are everything the `/checks` projection reads about a head, so a wave
/// publishes them twice. In memory while it runs, so the live projection does no
/// VCS work of its own; and on its attempt row for good, because once a merge or
/// close retires the node's branch none of this can be recomputed, and the
/// retired projection answers from the last plan recorded here.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WavePlan {
    pub head: String,
    pub base: String,
    pub tree_hash: String,
    pub checks: std::collections::HashMap<String, crate::config::project_settings::CheckCommand>,
    pub applicable_names: std::collections::HashSet<String>,
    pub input_hashes: std::collections::HashMap<String, String>,
    /// The checks the head declares that this build cannot run. Never planned,
    /// but listed beside the lanes, so they do not vanish from `/checks` while
    /// a wave runs or after the branch retires. A plan recorded before this
    /// field existed reads as declaring none.
    #[serde(
        default,
        skip_serializing_if = "std::collections::BTreeMap::is_empty",
        with = "recorded_unsupported"
    )]
    pub unsupported:
        std::collections::BTreeMap<String, crate::config::project_settings::UnsupportedCheck>,
}

/// [`WavePlan::unsupported`] as recorded in `plan_json`: each check together
/// with the entry it was declared as. `UnsupportedCheck` does not serialize
/// that entry, and the `/checks` lane reads the check's declared `when` and
/// `policy` from it.
mod recorded_unsupported {
    use crate::config::project_settings::UnsupportedCheck;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Recorded {
        field: String,
        value: String,
        #[serde(default)]
        known: Vec<String>,
        notice: String,
        #[serde(default)]
        declared: serde_yaml::Value,
    }

    pub(super) fn serialize<S: Serializer>(
        checks: &BTreeMap<String, UnsupportedCheck>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        checks
            .iter()
            .map(|(name, check)| {
                (
                    name,
                    Recorded {
                        field: check.field.clone(),
                        value: check.value.clone(),
                        known: check.known.clone(),
                        notice: check.notice.clone(),
                        declared: check.declared.clone(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>()
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<String, UnsupportedCheck>, D::Error> {
        Ok(BTreeMap::<String, Recorded>::deserialize(deserializer)?
            .into_iter()
            .map(|(name, recorded)| {
                (
                    name,
                    UnsupportedCheck {
                        field: recorded.field,
                        value: recorded.value,
                        known: recorded.known,
                        notice: recorded.notice,
                        declared: recorded.declared,
                    },
                )
            })
            .collect())
    }
}

/// Keep the plan a wave published on its attempt row: the durable half of
/// [`WavePlan`]. Best-effort, like the deferral record. A wave that cannot
/// record its plan still runs; only the retired projection of this one head
/// goes without it.
async fn record_wave_plan(db: &LocalDb, job_id: &str, attempt_id: &str, plan: &WavePlan) {
    let recorded = match serde_json::to_string(plan) {
        Ok(json) => db
            .execute(
                "UPDATE turn_end_check_attempts SET sealed_commit = ?1, plan_json = ?2
                  WHERE id = ?3 AND job_id = ?4",
                (plan.head.as_str(), json.as_str(), attempt_id, job_id),
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    };
    if let Err(error) = recorded {
        log::warn!(
            "turn-end checks for job {}: could not record the wave's plan ({error}); \
             this head's lanes will not be readable once its branch retires",
            short_id(job_id)
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TurnEndDisposition {
    Skipped(&'static str),
    Withdrawn(String),
    CacheCovered(&'static str),
    Cancelled(String),
    CompletedWithDelivery,
}
impl TurnEndDisposition {
    fn terminal(&self) -> (&'static str, String) {
        match self {
            Self::Skipped(r) => ("skipped", (*r).into()),
            Self::Withdrawn(r) => ("withdrawn", r.clone()),
            Self::CacheCovered(r) => ("cache_covered", (*r).into()),
            Self::Cancelled(r) => ("cancelled", r.clone()),
            Self::CompletedWithDelivery => (
                "completed",
                "check wave completed and delivery was persisted".into(),
            ),
        }
    }
}

pub(crate) fn request_turn_end_attempt(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<String, String> {
    let attempt_id = cairn_common::ids::mint_child(job_id);
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    let stored_id = attempt_id.clone();
    crate::storage::run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
        db.write(move |conn| { let id=stored_id.clone(); let job=job_id.clone(); Box::pin(async move {
            conn.execute("INSERT INTO turn_end_check_attempts(id,job_id,state,reason,created_at,updated_at) VALUES (?1,?2,'requested',NULL,unixepoch()*1000,unixepoch()*1000)",(id.as_str(),job.as_str())).await?;
            conn.execute("UPDATE jobs SET turn_end_check_attempt_id=?1 WHERE id=?2 AND (turn_end_check_attempt_id IS NULL OR NOT EXISTS (SELECT 1 FROM turn_end_check_attempts current WHERE current.id=jobs.turn_end_check_attempt_id AND current.state IN ('requested','claimed','runtime_started','deferred','submitted')))",(id.as_str(),job.as_str())).await?;
            Ok::<_,crate::storage::DbError>(())
        })}).await.map_err(|e|e.to_string())
    })?;
    Ok(attempt_id)
}

pub(crate) fn transition_turn_end_attempt(
    orch: &Orchestrator,
    job_id: &str,
    attempt_id: &str,
    state: &str,
    reason: Option<&str>,
) -> Result<(), String> {
    let dbs = orch.db.clone();
    let job = job_id.to_string();
    let id = attempt_id.to_string();
    let state = state.to_string();
    let reason = reason.map(str::to_string);
    crate::storage::run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job)
            .await
            .map_err(|e| e.to_string())?;
        db.write(move |conn| { let job=job.clone(); let id=id.clone(); let state=state.clone(); let reason=reason.clone(); Box::pin(async move {
            let changed=conn.execute("UPDATE turn_end_check_attempts SET state=?1,reason=?2,updated_at=unixepoch()*1000 WHERE id=?3 AND job_id=?4",(state.as_str(),reason.as_deref(),id.as_str(),job.as_str())).await?;
            if changed != 1 { return Err(crate::storage::DbError::Row(format!("turn-end attempt {id} for job {job} was not found"))); }
            // The in-memory single-flight winner is not necessarily the first
            // requester. Transfer the durable pointer in the same transaction
            // that records its claim so every observer sees the actual owner.
            if state == "claimed" {
                conn.execute(
                    "UPDATE jobs SET turn_end_check_attempt_id=?1 WHERE id=?2",
                    (id.as_str(), job.as_str()),
                ).await?;
            }
            Ok::<_,crate::storage::DbError>(())
        })}).await.map_err(|e|e.to_string())
    })
}

pub(crate) async fn current_turn_end_attempt(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<TurnEndAttempt>, String> {
    db.read(|conn| { let job=job_id.to_string(); Box::pin(async move {
        let mut rows=conn.query("SELECT a.id,a.state,a.reason FROM jobs j LEFT JOIN turn_end_check_attempts a ON a.id=j.turn_end_check_attempt_id WHERE j.id=?1",(job.as_str(),)).await?;
        let Some(row)=rows.next().await? else{return Ok(None)}; let (Some(id),Some(state))=(row.opt_text(0)?,row.opt_text(1)?) else{return Ok(None)};
        Ok(Some(TurnEndAttempt{id,state,reason:row.opt_text(2)?}))
    })}).await.map_err(|e|e.to_string())
}

pub(crate) async fn reconcile_turn_end_attempts_on_startup(orch: &Orchestrator) {
    const REASON: &str =
        "host restarted before the turn-end check attempt reached a durable terminal state";
    for db in orch.db.all_dbs().await {
        if let Err(error)=db.write(|conn| Box::pin(async move {
            conn.execute("UPDATE turn_end_check_attempts SET state='failed',reason=?1,updated_at=unixepoch()*1000 WHERE state IN ('requested','claimed','runtime_started','deferred','submitted')",(REASON,)).await?;
            Ok::<_,crate::storage::DbError>(())
        })).await { log::warn!("startup turn-end attempt reconciliation failed: {error}"); }
    }
}

/// Background entry point: run the affected turn-end checks for a job, then
/// release the single-flight slot. The caller ([`spawn_turn_end_checks`] in
/// lifecycle) has already claimed the slot via `try_begin_turn_end_checks`; this
/// function is responsible for releasing it on every path.
pub(crate) async fn run_turn_end_checks(
    orch: Orchestrator,
    job_id: String,
    attempt_id: String,
    cancel: TurnEndCancel,
) {
    let result =
        match transition_turn_end_attempt(&orch, &job_id, &attempt_id, "runtime_started", None) {
            Ok(()) => run_turn_end_checks_inner(&orch, &job_id, &attempt_id, &cancel).await,
            Err(error) => Err(error),
        };
    let terminal = match result {
        Ok(disposition) => {
            let (state, reason) = disposition.terminal();
            transition_turn_end_attempt(&orch, &job_id, &attempt_id, state, Some(&reason))
        }
        Err(error) => {
            log::warn!("turn-end checks for job {}: {}", short_id(&job_id), error);
            transition_turn_end_attempt(&orch, &job_id, &attempt_id, "failed", Some(&error))
        }
    };
    if let Err(error) = terminal {
        log::error!("turn-end checks for job {}: terminal persistence failed; retaining single-flight ownership: {}",short_id(&job_id),error);
        return;
    }
    orch.end_turn_end_checks(&job_id);
    if let Some(issue_id) = issue_id_for_job(&orch.db.local, &job_id).await {
        crate::orchestrator::lifecycle::evaluate_review_readiness(&orch, &issue_id).await;
    }
    crate::orchestrator::wakes::route_checks_settled_edge(&orch, &job_id).await;
}

/// What delivering one completed wave's outcomes decided: the identity it was
/// keyed on, whether any lane carried a genuine verdict against the change, and
/// the delivery and effective wake level the attention ledger admitted.
#[derive(Debug)]
struct TurnCheckOutcomeDelivery {
    fingerprint: Option<String>,
    any_genuine_failed: bool,
    delivery: TurnCheckDelivery,
    wake: Option<attention_push::Wake>,
}

/// The single seam from a wave's outcomes to its attention delivery. Both the
/// wake level and the wake identity are derived here, from the same outcomes,
/// so a test that drives this function exercises exactly the composition a
/// settled wave does.
async fn deliver_turn_check_outcomes(
    db: &LocalDb,
    recipient: &str,
    checks_uri: &str,
    tree_hash: &str,
    outcomes: &[CheckOutcome],
) -> Result<TurnCheckOutcomeDelivery, String> {
    // The wake decision keys on GENUINE failures only. An infrastructure failure
    // is a fact about Cairn, not a verdict about this change, so rousing the
    // author for one asks them to fix something they have no standing to touch —
    // and because it re-executes on every evaluation, it asks repeatedly. That is
    // the loop this cadence kept generating (CAIRN-3245).
    let any_genuine_failed = outcomes.iter().any(CheckOutcome::is_genuine_failure);
    let fingerprint = turn_check_fingerprint(tree_hash, outcomes);
    let (delivery, wake) = persist_turn_check_delivery(
        db,
        recipient,
        checks_uri,
        fingerprint.as_deref(),
        any_genuine_failed,
    )
    .await?;
    Ok(TurnCheckOutcomeDelivery {
        fingerprint,
        any_genuine_failed,
        delivery,
        wake,
    })
}

async fn persist_turn_check_delivery(
    db: &LocalDb,
    recipient: &str,
    checks_uri: &str,
    fingerprint: Option<&str>,
    any_genuine_failed: bool,
) -> Result<(TurnCheckDelivery, Option<attention_push::Wake>), String> {
    let key = format!("turn-checks:{checks_uri}");
    let latest = attention_push::latest_push_fingerprint(db, recipient, &key)
        .await
        .map_err(|error| format!("failed to read turn-check fingerprint: {error}"))?;
    let delivery = turn_check_delivery(
        latest.as_ref().map(|value| value.as_deref()),
        fingerprint,
        any_genuine_failed,
    );
    if delivery == TurnCheckDelivery::Suppress {
        return Ok((delivery, None));
    }
    // CAIRN-4665, and the exact line that caused cairn/4658. A turn-end wave is
    // spawned at a turn boundary and can settle a cold cargo build later — 28
    // minutes later, in the specimen — by which time its recipient's execution
    // may have failed AND been superseded AND had its successor adopt its head.
    // Delivering this at `Wake` resurrected the dead builder.
    //
    // The push is still WRITTEN, at `Passive`: the verdict stays readable at the
    // node's `/checks` and in the attention ledger, so the attention is consumed
    // and inspectable rather than silently dropped. It simply cannot rouse.
    //
    // The fence must be HERE, at delivery, and not at spawn: the whole
    // failure-and-successor sequence happened inside the check window, so a
    // guard that refused to START checks for a dead node would have arrived
    // half an hour late.
    let wake = match crate::execution::canonical::fence_for_job(db, recipient).await {
        Ok(fence) if fence.is_superseded() => {
            log::info!(
                "canonical_fence: turn-check verdict for superseded job {} recorded passively \
                 rather than waking it",
                short_id(recipient)
            );
            attention_push::Wake::Passive
        }
        Ok(_) => delivery_wake(any_genuine_failed),
        Err(error) => {
            log::warn!(
                "canonical_fence: could not read lineage for {}; recording the turn-check verdict \
                 passively rather than risking a resurrection: {error}",
                short_id(recipient)
            );
            attention_push::Wake::Passive
        }
    };
    let (_, effective_wake) = attention_push::push_with_fingerprint(
        db,
        recipient,
        checks_uri,
        wake,
        attention_push::Boundary::Event,
        &key,
        fingerprint,
    )
    .await
    .map_err(|error| format!("failed to queue turn-check results push: {error}"))?;
    Ok((delivery, Some(effective_wake)))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TurnCheckDelivery {
    Suppress,
    Deliver(&'static str),
}

fn turn_check_delivery(
    latest: Option<Option<&str>>,
    fingerprint: Option<&str>,
    any_genuine_failed: bool,
) -> TurnCheckDelivery {
    let Some(fingerprint) = fingerprint else {
        return TurnCheckDelivery::Deliver("ambiguous/fail-open");
    };
    if latest.flatten() == Some(fingerprint) {
        return TurnCheckDelivery::Suppress;
    }
    if !any_genuine_failed {
        TurnCheckDelivery::Deliver("green")
    } else if latest.is_none() {
        TurnCheckDelivery::Deliver("new")
    } else {
        TurnCheckDelivery::Deliver("changed")
    }
}

/// Signal every in-flight turn-end (`when:review`) check suite belonging to
/// `issue_id` to quit. Fired when the issue reaches a terminal (merged/closed)
/// state: the PR the suite was validating is resolved, so a minutes-long review
/// run against it is wasted work (CAIRN-2648). Best-effort — enumerates the issue's
/// jobs from `db` (the issue's owning database) and pulls each one's cancellation
/// lever, a no-op for any job with no suite in flight.
pub(crate) async fn cancel_turn_end_checks_for_issue(
    orch: &Orchestrator,
    db: &LocalDb,
    issue_id: &str,
) {
    let issue_id_owned = issue_id.to_string();
    let job_ids = db
        .read(|conn| {
            let issue_id = issue_id_owned.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id FROM jobs WHERE issue_id = ?1",
                        (issue_id.as_str(),),
                    )
                    .await?;
                let mut ids = Vec::new();
                while let Some(row) = rows.next().await? {
                    ids.push(row.text(0)?);
                }
                Ok(ids)
            })
        })
        .await;
    match job_ids {
        Ok(ids) => {
            for job_id in &ids {
                orch.cancel_turn_end_checks(job_id, TurnEndCancelReason::IssueResolved);
            }
            log::debug!(
                "cancel_turn_end_checks_for_issue({}): signalled {} job(s)",
                short_id(issue_id),
                ids.len()
            );
        }
        Err(e) => log::warn!(
            "cancel_turn_end_checks_for_issue({}): failed to enumerate jobs: {}",
            short_id(issue_id),
            e
        ),
    }
}

/// Whether a turn-end (`when:review`) wave may still be launched for a job.
///
/// One decision, taken from the owning database and the live fleet, asked at
/// both moments that matter: before the detached suite is spawned at all, and
/// again immediately before any cell request is submitted. Two calls because the
/// window between them is minutes wide (branch resolution, changed-file diffs,
/// input planning, tree hashing), and an issue that merges inside it has already
/// made the wave pointless. Discovering that only after taking admission has
/// already spent the machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WaveLaunchability {
    /// Launch: the issue is live and the job is not cancelled.
    ///
    /// The job's own cell residency is deliberately not consulted. A wave is a
    /// `PureVerdict` batch built from the SEALED commit and published as a
    /// temporary ref, so it never runs in the owner's workspace, and an owner
    /// whose cell is awaiting reclaim is exactly the dormant PR owner whose
    /// verdicts the review gate is waiting on.
    Ready,
    /// Launch nothing and record nothing. A wave withdrawn here never asked for
    /// capacity, so there is no verdict to explain and no red infrastructure
    /// check to explain it with.
    Withdrawn(WaveWithdrawal),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WaveWithdrawal {
    /// The issue reached a terminal state: nobody will review this tree again.
    IssueResolved(String),
    /// The job's own work was cancelled.
    JobCancelled,
    /// The job's coordinates no longer resolve at all.
    JobUnknown,
}

impl WaveWithdrawal {
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::IssueResolved(status) => format!("its issue is already {status}"),
            Self::JobCancelled => "the job was cancelled".to_string(),
            Self::JobUnknown => "the job no longer resolves".to_string(),
        }
    }
}

/// Ask whether a turn-end wave for `job_id` may launch right now.
///
/// Fails OPEN on every fault it meets. This guard exists to stop work nobody
/// wants, not to become one more way a review gate disappears on a shipping
/// branch: an unreadable database says nothing about whether the issue resolved,
/// and treating it as a withdrawal would silently drop the wave.
pub(crate) async fn turn_end_launchability(orch: &Orchestrator, job_id: &str) -> WaveLaunchability {
    let db = match crate::execution::routing::owning_db_for_job(&orch.db, job_id).await {
        Ok(db) => db,
        Err(error) => {
            log::warn!(
                "turn-end launchability for job {}: owning database unresolved ({error}); \
                 treating the wave as launchable",
                short_id(job_id)
            );
            return WaveLaunchability::Ready;
        }
    };
    // Both statuses come from ONE read, so the answer describes a single instant
    // rather than two that a resolution could land between.
    let owned_job_id = job_id.to_string();
    let statuses = db
        .read(|conn| {
            let job_id = owned_job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT i.status, j.status
                         FROM jobs j JOIN issues i ON i.id = j.issue_id
                         WHERE j.id = ?1 LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((row.text(0)?, row.text(1)?))),
                    None => Ok(None),
                }
            })
        })
        .await;
    match statuses {
        Ok(Some((issue_status, job_status))) => {
            if matches!(issue_status.as_str(), "merged" | "closed") {
                return WaveLaunchability::Withdrawn(WaveWithdrawal::IssueResolved(issue_status));
            }
            if job_status == "cancelled" {
                return WaveLaunchability::Withdrawn(WaveWithdrawal::JobCancelled);
            }
        }
        // A job row that is not there is not a job whose tree anyone reviews. A
        // project-level job has no issue and never arms this cadence.
        Ok(None) => return WaveLaunchability::Withdrawn(WaveWithdrawal::JobUnknown),
        Err(error) => {
            log::warn!(
                "turn-end launchability for job {}: status read failed ({error}); \
                 treating the wave as launchable",
                short_id(job_id)
            );
            return WaveLaunchability::Ready;
        }
    }
    WaveLaunchability::Ready
}

/// The issue a job belongs to, or `None` for a project-level job.
async fn issue_id_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
    db.read(|conn| {
        let job_id = job_id.to_string();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT issue_id FROM jobs WHERE id = ?1",
                    (job_id.as_str(),),
                )
                .await?;
            crate::storage::next_opt_text(&mut rows, 0).await
        })
    })
    .await
    .ok()
    .flatten()
}

/// How often a deferred wave re-asks whether its node has gone quiet.
const WAVE_SETTLE_POLL_MS: u64 = 3_000;

/// The longest a wave defers to its node's own work before taking admission
/// anyway.
///
/// Deferral must never become suppression. Yielding is safe because a node that
/// is working reaches another turn-end, which arms another wave — but the review
/// gate may not be made to DEPEND on that. Evidence of "busy" that never clears
/// (a node that submits without pause, a cell whose advertised occupancy outlives
/// the command in it) has to end in a wave that runs rather than one that waited
/// forever. This is a bound on WAITING, never on running: nothing is killed when
/// it elapses, a wave is released.
///
/// Long enough to sit out a full `rust-tests` a builder started itself, and no
/// longer.
const WAVE_SETTLE_CEILING_MS: u64 = 10 * 60 * 1_000;

/// One node's own work, as the fleet currently reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeWorkInFlight {
    /// How many fleet requests the node's jobs hold right now.
    requests: usize,
    /// One representative command, so the record a reader sees names what the
    /// wave yielded to rather than only that it yielded.
    command: String,
}

impl NodeWorkInFlight {
    fn describe(&self) -> String {
        match self.requests {
            1 => format!("its own `{}`", self.command),
            n => format!("{n} commands of its own, including `{}`", self.command),
        }
    }
}

/// What `jobs` is running on the fleet right now, or `None` when they hold
/// nothing.
///
/// Reads exactly what the Running list reads — the cells' current occupancy and
/// the queue behind them — so the work a wave says it is yielding to is the work
/// an operator can watch it yield to. A queued request counts: it holds a place
/// in an admission queue and is about to become a running command.
///
/// Ownership is read from either the requesting job or the owner reference,
/// because different submitters populate different ones and a wave that missed
/// half the node's work would defer to nothing.
fn node_work_in_flight(
    snapshot: &cairn_common::executor_protocol::FleetSnapshot,
    jobs: &BTreeSet<String>,
) -> Option<NodeWorkInFlight> {
    if jobs.is_empty() {
        return None;
    }
    let owned = |requesting: Option<&str>,
                 owner: Option<&cairn_common::executor_protocol::CellOwnerRef>| {
        requesting.is_some_and(|job| jobs.contains(job))
            || owner
                .and_then(|owner| owner.job_id.as_deref())
                .is_some_and(|job| jobs.contains(job))
    };
    let running = snapshot
        .cells
        .iter()
        .filter_map(|cell| cell.occupancy.command.as_ref())
        .filter(|active| owned(active.requesting_job_id.as_deref(), active.owner.as_ref()))
        .map(|active| active.command.as_str());
    let queued = snapshot
        .queued_requests
        .iter()
        .filter(|queued| owned(queued.requesting_job_id.as_deref(), queued.owner.as_ref()))
        .map(|queued| queued.command.as_str());
    let mut commands: Vec<&str> = running.chain(queued).collect();
    if commands.is_empty() {
        return None;
    }
    commands.sort_unstable();
    Some(NodeWorkInFlight {
        requests: commands.len(),
        command: summarize_command(commands[0]),
    })
}

/// A command as a log line wants it: the first non-empty line, bounded.
fn summarize_command(command: &str) -> String {
    const MAX: usize = 60;
    let first = command
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or(command)
        .trim();
    match first.char_indices().nth(MAX) {
        Some((cut, _)) => format!("{}\u{2026}", &first[..cut]),
        None => first.to_string(),
    }
}

/// The environment overrides the turn-end engine evaluates its cache phase
/// under. Named once so the reuse re-check above submission and the engine's own
/// lookup below it cannot drift apart into two different verdict identities.
const TURN_END_EFFECTIVE_ENV: &[(String, String)] = &[];

/// The indices of `lanes` that still have to run.
///
/// Indices rather than a filtered list, because batch items and the check engine
/// both address a lane by its position in the ORIGINAL plan set: a lane answered
/// from cache has to keep its place so outcomes reassemble in plan order.
fn lanes_to_submit(
    lanes: &[(CheckPlan, String)],
    covered: impl Fn(&CheckPlan, &str) -> bool,
) -> Vec<usize> {
    lanes
        .iter()
        .enumerate()
        .filter(|(_, (plan, input_hash))| !covered(plan, input_hash))
        .map(|(index, _)| index)
        .collect()
}

/// Whether a wave may take admission now, keep waiting, or has waited long
/// enough that it must run regardless.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettleDecision {
    Proceed,
    Wait,
    ProceedUnsettled,
}

fn settle_decision(busy: bool, waited_ms: u64) -> SettleDecision {
    if !busy {
        return SettleDecision::Proceed;
    }
    if waited_ms >= WAVE_SETTLE_CEILING_MS {
        SettleDecision::ProceedUnsettled
    } else {
        SettleDecision::Wait
    }
}

/// How a wave's deferral ended.
///
/// Two outcomes rather than four, because the caller has exactly one decision to
/// take from this. Which of the three proceeding paths it was — never waited,
/// waited and the node settled, waited out the ceiling — is recorded on the
/// attempt row and logged at the moment it happens, where the reason is still in
/// hand; re-deriving it here would only let the two accounts drift.
#[derive(Debug, Clone, PartialEq, Eq)]
enum WaveSettle {
    /// Take admission.
    Proceed,
    /// The wave was withdrawn while waiting, having asked for no machine at all.
    Cancelled {
        waited_ms: u64,
        reason: TurnEndCancelReason,
    },
}

/// Wait for the node this wave checks to stop running work of its own.
///
/// Scoped to every job sharing the node's branch — the same scope the
/// branch-advance withdrawal uses, so the two agree about whose work is whose.
/// A sibling's wave counts as work too, and that is the point: the second wave
/// waits, and by the time it proceeds the first one's verdicts are in the cache
/// answering it, instead of both compiling the same tree at once.
async fn defer_until_node_settles(
    orch: &Orchestrator,
    job_id: &str,
    attempt_id: &str,
    cancel: &TurnEndCancel,
) -> WaveSettle {
    let started = std::time::Instant::now();
    let mut deferred = false;
    loop {
        // Branch membership is re-resolved on every pass rather than fixed at
        // entry. A wait can last minutes, and the node spawning a sub-agent task
        // during one is precisely a node that is still working: a membership set
        // frozen before the wait would see that task's cell as somebody else's
        // and call the node quiet the moment the original command cleared.
        let mut jobs: BTreeSet<String> =
            crate::execution::checks::jobs_sharing_branch(&orch.db.local, job_id)
                .await
                .into_iter()
                .collect();
        jobs.insert(job_id.to_string());
        let work = node_work_in_flight(&orch.fleet.snapshot(), &jobs);
        let waited_ms = started.elapsed().as_millis() as u64;
        match settle_decision(work.is_some(), waited_ms) {
            SettleDecision::Proceed if !deferred => return WaveSettle::Proceed,
            SettleDecision::Proceed => {
                let reason = format!(
                    "the node settled after {}s; taking admission",
                    waited_ms / 1_000
                );
                record_settle(orch, job_id, attempt_id, "runtime_started", &reason);
                log::info!("turn-end checks for job {}: {reason}", short_id(job_id));
                return WaveSettle::Proceed;
            }
            SettleDecision::ProceedUnsettled => {
                let work = work.map(|work| work.describe()).unwrap_or_default();
                let reason = format!(
                    "the node has been running {work} for {}s; running the review lanes anyway \
                     rather than letting a deferral become a dropped review gate",
                    waited_ms / 1_000
                );
                record_settle(orch, job_id, attempt_id, "runtime_started", &reason);
                log::warn!("turn-end checks for job {}: {reason}", short_id(job_id));
                return WaveSettle::Proceed;
            }
            SettleDecision::Wait => {
                if !deferred {
                    let work = work.map(|work| work.describe()).unwrap_or_default();
                    let reason = format!(
                        "deferring: the node is running {work}, which has right of way over its \
                         own review wave"
                    );
                    record_settle(orch, job_id, attempt_id, "deferred", &reason);
                    log::info!("turn-end checks for job {}: {reason}", short_id(job_id));
                    deferred = true;
                }
                tokio::select! {
                    biased;
                    reason = cancel.cancelled() => return WaveSettle::Cancelled { waited_ms, reason },
                    _ = tokio::time::sleep(std::time::Duration::from_millis(WAVE_SETTLE_POLL_MS)) => {}
                }
            }
        }
    }
}

/// Persist one step of the deferral onto the attempt row. Best-effort: a wave
/// that cannot record why it waited still runs, because the verdict is the
/// obligation and the record is the explanation.
fn record_settle(orch: &Orchestrator, job_id: &str, attempt_id: &str, state: &str, reason: &str) {
    if let Err(error) = transition_turn_end_attempt(orch, job_id, attempt_id, state, Some(reason)) {
        log::warn!(
            "turn-end checks for job {}: could not record the {state} step ({error})",
            short_id(job_id)
        );
    }
}

async fn run_turn_end_checks_inner(
    orch: &Orchestrator,
    job_id: &str,
    attempt_id: &str,
    cancel: &TurnEndCancel,
) -> Result<TurnEndDisposition, String> {
    // 1. Resolve the node's durable branch coordinate and base anchors.
    let owning_db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| error.to_string())?;
    let Some(coords) = resolve_job_coords(&owning_db, job_id).await? else {
        return Ok(TurnEndDisposition::Skipped(
            "job coordinates are unavailable",
        ));
    };
    // Nothing that follows is owed if this wave may not launch: its verdicts
    // would validate a tree nobody will review again. Asked again immediately
    // before submission, because
    // everything between here and there takes minutes. The mid-flight case (a
    // resolution landing WHILE a check runs) is the `cancel` race around the
    // suite below (CAIRN-2648).
    if let WaveLaunchability::Withdrawn(withdrawal) = turn_end_launchability(orch, job_id).await {
        log::info!(
            "turn-end checks for job {}: {}; nothing to run",
            short_id(job_id),
            withdrawal.describe()
        );
        return Ok(TurnEndDisposition::Withdrawn(withdrawal.describe()));
    }
    let repo_root = PathBuf::from(&coords.repository_path);
    let logical_repository = {
        let jj_binary_path = orch.jj_binary_path.clone();
        let config_dir = orch.config_dir.clone();
        let project_repo = repo_root.clone();
        tokio::task::spawn_blocking(move || {
            let jj = crate::jj::environment(&jj_binary_path, &config_dir);
            crate::jj::coordinate_repository(&jj, &config_dir, &project_repo)
        })
        .await
        .map_err(|error| format!("turn-end coordinate repository task failed: {error}"))??
    };
    let store_dir = logical_repository.clone();
    let sealed_commit = cairn_vcs::resolve_coordinate(&logical_repository, &coords.branch)
        .await
        .map_err(|error| {
            format!(
                "turn-end branch '{}' is unresolvable: {error}",
                coords.branch
            )
        })?;
    // The node's own delta starts at the LIVE fork point of its branch from its
    // integration target. The recorded `jobs.base_commit` row is the coordinate
    // the branch was cut at and does not follow a base advance, so diffing from
    // it absorbs every commit the target merged in the meantime — which is how a
    // zero-delta planner selected a full review suite (CAIRN-3108).
    let base = match crate::diff::live_job_branch_range(
        &orch.db.local,
        job_id,
        &orch.jj_binary_path,
        &orch.config_dir,
    )
    .await
    {
        Ok(Some(range)) => range.base,
        other => {
            log::warn!(
                "turn-end checks for job {}: no live base coordinate ({}); nothing to run",
                short_id(job_id),
                match other {
                    Err(error) => error,
                    _ => "the job has no branch of its own".to_string(),
                }
            );
            return Ok(TurnEndDisposition::Skipped(
                "job has no live base coordinate",
            ));
        }
    };

    // 2. Load the checks contract DECLARED BY the sealed commit this turn is
    // about. Definition and content come from the same tree, so a branch's own
    // `.cairn/config.yaml` edit governs its own cadence and no sibling's
    // (CAIRN-3333).
    let (checks, extra_inputs, unsupported, defined_by_commit) =
        match load_checks_contract_at_commit(&repo_root, &sealed_commit).await {
            Some(loaded) if !loaded.contract.checks.is_empty() => (
                loaded.contract.checks,
                loaded.contract.extra_inputs,
                loaded.contract.unsupported,
                loaded.defined_by_commit,
            ),
            _ => {
                log::debug!(
                    "turn-end checks for job {}: commit {} declares no checks; nothing to run",
                    short_id(job_id),
                    sealed_commit
                );
                return Ok(TurnEndDisposition::Skipped(
                    "sealed commit declares no review checks",
                ));
            }
        };
    // Hard assert, not a log: a definition arriving from any tree but the sealed
    // commit is the defect this binding exists to prevent, and it must fail
    // visibly rather than record a sibling's check against this node.
    assert_eq!(
        defined_by_commit, sealed_commit,
        "turn-end checks must be defined by the commit they evaluate"
    );

    // 3. Resolve the DB that owns this job (used below to queue the results push).
    let owning = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|e| e.to_string())?;

    // 4. Compute changed files between immutable logical coordinates.
    let wave_base = base.clone();
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let changed_jj = jj.clone();
    let changed_repo = logical_repository.clone();
    let changed_head = sealed_commit.clone();
    let changed = match tokio::task::spawn_blocking(move || {
        logical_changed_files(&changed_jj, &changed_repo, &base, &changed_head)
    })
    .await
    .map_err(|error| format!("turn-end changed-file task failed: {error}"))?
    {
        Ok(changed) => changed,
        Err(error) => {
            log::warn!(
                "turn-end checks for job {}: changed-file set unresolvable; nothing to run: {error}",
                short_id(job_id)
            );
            return Ok(TurnEndDisposition::Skipped(
                "changed-file set is unresolvable",
            ));
        }
    };
    if changed.is_empty() {
        log::debug!(
            "turn-end checks for job {}: empty changed-file set; nothing to run",
            short_id(job_id)
        );
        return Ok(TurnEndDisposition::Skipped("changed-file set is empty"));
    }

    // 5. Select the applicable turn-end checks (cadence + input gate). Resolving
    // each check's inputs reads the sealed tree, and target expansion may run
    // cargo metadata, so planning belongs on the blocking pool.
    let planning_checks = checks.clone();
    let planning_extra_inputs = extra_inputs.clone();
    let planning_changed = changed.clone();
    let planning_repo = repo_root.clone();
    let planning_jj = jj.clone();
    let planning_repository = logical_repository.clone();
    let planning_commit = sealed_commit.clone();
    let (plans, input_hashes, tree_hash) = tokio::task::spawn_blocking(move || {
        // The entry listing feeds both the derived closures and the per-check
        // cache key below, so it is read once here.
        let entries = if any_check_declares_inputs(planning_checks.values()) {
            tree_entries(&planning_jj, &planning_repository, &planning_commit).ok()
        } else {
            None
        };
        let blobs = TreeBlobs {
            jj: &planning_jj,
            repository: &planning_repository,
        };
        let snapshot = TreeSnapshot::new(entries.as_deref(), &blobs);
        let inputs = ResolvedInputs::resolve(&planning_checks, &planning_extra_inputs, &snapshot);
        let plans = applicable_turn_end_checks(
            &planning_checks,
            &inputs,
            &planning_changed,
            &planning_repo,
        );
        if plans.is_empty() {
            return Ok((plans, std::collections::HashMap::new(), String::new()));
        }
        // 6. The immutable tree identity, and every configured check's input
        // hash at it: the key this wave reuses and records under, and the key
        // the `/checks` projection reads lanes by while the wave runs.
        let tree_hash = logical_tree_hash(&planning_jj, &planning_repository, &planning_commit)
            .map_err(|error| error.to_string())?;
        let input_hashes =
            check_input_hashes(&planning_checks, &inputs, entries.as_deref(), &tree_hash);
        Ok::<_, String>((plans, input_hashes, tree_hash))
    })
    .await
    .map_err(|error| format!("turn-end check planning task failed: {error}"))??;
    if plans.is_empty() {
        log::debug!(
            "turn-end checks for job {}: no applicable review check; nothing to run",
            short_id(job_id)
        );
        return Ok(TurnEndDisposition::Skipped(
            "no review check applies to the changed files",
        ));
    }

    let applicable_names = plans
        .iter()
        .map(|plan| plan.name.clone())
        .collect::<std::collections::HashSet<_>>();
    let canonical_repo = coords.repository_path.clone();

    // Is this node's sealed tree byte-identical to its base's? See
    // `base_tree_coordinate` for why the coordinate is resolved the way it is.
    // Unresolvable leaves this false, which preserves the pre-existing behaviour
    // of running the suite.
    let tree_matches_base = match base_tree_coordinate(&coords) {
        Some(coordinate) => {
            match cairn_vcs::resolve_coordinate(&logical_repository, coordinate).await {
                Ok(base_commit) => {
                    let base_jj = jj.clone();
                    let base_repo = logical_repository.clone();
                    tokio::task::spawn_blocking(move || {
                        logical_tree_hash(&base_jj, &base_repo, &base_commit)
                    })
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .is_some_and(|base_tree| base_tree == tree_hash)
                }
                Err(error) => {
                    log::debug!(
                        "turn-end checks for job {}: base coordinate '{coordinate}' is \
                         unresolvable ({error}); falling through to the changed-file gate",
                        short_id(job_id)
                    );
                    false
                }
            }
        }
        None => false,
    };

    // Publish the immutable facts this wave planned with: on its attempt row
    // for good, and in memory for the 1 Hz status poll. The in-memory copy stays
    // valid until the single-flight slot is released because this suite is
    // pinned to the sealed tree.
    let plan = std::sync::Arc::new(WavePlan {
        head: sealed_commit.clone(),
        base: wave_base,
        tree_hash: tree_hash.clone(),
        checks: checks.clone(),
        applicable_names,
        input_hashes: input_hashes.clone(),
        unsupported,
    });
    record_wave_plan(&owning_db, job_id, attempt_id, &plan).await;
    cancel.set_runtime_status(plan);

    // 7. Loop-break gate: drop any plan whose INPUT hash (the content of just
    // that check's impact-matched files) is infrastructure-suppressed; every
    // other plan goes on to the reuse re-check and the engine. A lane the engine
    // answers from reuse records only a commit alias, and it stays visible on
    // `/checks` because that projection reads lanes by this same input hash.
    let db = orch.db.local.clone();
    let cache_db = db.clone();
    let cache_checks = checks.clone();
    let cache_input_hashes = input_hashes;
    let cache_tree_hash = tree_hash.clone();
    let cache_project_id = coords.project_id.clone();
    let cache_job_id = job_id.to_string();
    let cache_commit = sealed_commit.clone();
    // The fleet is not reachable inside the blocking task, so the scope set this
    // cadence reads suppression against is resolved out here, once, keyed by
    // check name because each check asks its own placement question.
    let cache_infra_scopes: std::collections::HashMap<
        String,
        crate::execution::cache::InfraScopes,
    > = plans
        .iter()
        .map(|plan| plan.name.clone())
        .zip(
            review_infra_scopes(
                orch,
                &cache_project_id,
                &cache_job_id,
                &cache_commit,
                &coords.repository_path,
                &plans
                    .iter()
                    .map(|plan| review_check_placement_question(cache_checks.get(&plan.name)))
                    .collect::<Vec<_>>(),
            )
            .await,
        )
        .collect();
    // Asked of the fleet out here for the same reason the scopes are.
    let cache_unplaceable: std::collections::HashSet<String> = plans
        .iter()
        .filter(|plan| {
            let check = cache_checks.get(&plan.name);
            orch.fleet
                .unplaceable_reason(
                    check.and_then(|check| check.executor.as_ref()),
                    &check
                        .map(crate::execution::check_identity::verdict_platforms)
                        .unwrap_or_default(),
                )
                .is_some()
        })
        .map(|plan| plan.name.clone())
        .collect();
    let cache_filtered = tokio::task::spawn_blocking(move || {
        hold_back_suppressed(
            &cache_db,
            &cache_project_id,
            &cache_tree_hash,
            &cache_job_id,
            &cache_commit,
            plans,
            &cache_input_hashes,
            &cache_infra_scopes,
            &cache_unplaceable,
        )
    })
    .await
    .map_err(|error| format!("turn-end cache planning task failed: {error}"))?;
    let (cache_filtered, suppressed) = cache_filtered;
    if !suppressed.is_empty() {
        log::warn!(
            "turn-end checks for job {}: running none of [{}] \u{2014} infrastructure-suppressed \
             after {} consecutive infrastructure failures",
            short_id(job_id),
            suppressed.join(", "),
            crate::execution::cache::OBSERVED_INFRA_FAILURE_BOUND
        );
    }
    let to_run = match turn_end_launch(cache_filtered, tree_matches_base) {
        TurnEndLaunch::Launch(plans) => plans,
        TurnEndLaunch::Skip(TurnEndSkip::FullyCached) => {
            log::debug!(
                "turn-end checks for job {}: every applicable check is already cached for this tree; nothing to run",
                short_id(job_id)
            );
            return Ok(TurnEndDisposition::CacheCovered(
                "every applicable check is already cached",
            ));
        }
        TurnEndLaunch::Skip(TurnEndSkip::TreeMatchesBase(names)) => {
            log::info!(
                "turn-end checks for job {}: sealed tree is identical to the base's; \
                 inheriting cached verdicts and running none of [{}]",
                short_id(job_id),
                names.join(", ")
            );
            return Ok(TurnEndDisposition::CacheCovered(
                "sealed tree matches the base tree",
            ));
        }
    };
    // 8. Yield to the node's own work before asking any machine for room. The
    // wave has spent nothing but runner-local planning up to here, so a withdrawal
    // that lands during the wait costs a placement and a partial compile less than
    // the same withdrawal landing mid-suite.
    if let WaveSettle::Cancelled { waited_ms, reason } =
        defer_until_node_settles(orch, job_id, attempt_id, cancel).await
    {
        log::info!(
            "turn-end checks for job {}: withdrawn after deferring {}s to the node's own work because {}; \
             no machine was ever asked",
            short_id(job_id),
            waited_ms / 1_000,
            reason.describe()
        );
        return Ok(TurnEndDisposition::Cancelled(format!(
            "withdrawn while deferring to the node's own work because {}, before taking admission",
            reason.describe()
        )));
    }

    // 9. Ask the cache again, now that the wait is over.
    //
    // The plan set was filtered before the deferral; a wave that waited minutes
    // for its node has given a sibling on the same branch, or the agent's own
    // `cairn check run`, time to publish a verdict for the very tree it is about
    // to check. Submitting the pre-wait plan set would execute those lanes and
    // then discard the results when the engine's cache phase answered them from
    // the stored verdict instead. The predicate is the engine's own, so a lane
    // held back here is exactly a lane the engine will report from reuse.
    let covered_db = db.clone();
    let covered_project = coords.project_id.clone();
    let covered_lanes = to_run.clone();
    let submit = tokio::task::spawn_blocking(move || {
        lanes_to_submit(&covered_lanes, |plan, input_hash| {
            crate::execution::checks::reusable_verdict_for_plan(
                covered_db.clone(),
                &covered_project,
                plan,
                input_hash,
                TURN_END_EFFECTIVE_ENV,
            )
            .is_some()
        })
    })
    .await
    .map_err(|error| format!("turn-end reuse re-check task failed: {error}"))?;
    let reused = to_run.len() - submit.len();
    log::info!(
        "turn-end checks for job {}: launching {} of {} applicable check(s) [{}] over {} changed \
         file(s){}",
        short_id(job_id),
        submit.len(),
        to_run.len(),
        submit
            .iter()
            .map(|&index| to_run[index].0.name.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        changed.len(),
        match reused {
            0 => String::new(),
            n => format!(
                "; {n} answered from verdicts recorded for this tree while the wave planned or \
                 deferred"
            ),
        }
    );

    // 10. Prepare the host-readable, job-scoped log DIRECTORY (cleared for a fresh
    // run) so the PR-node / `/checks` render can tail each check's OWN log live
    // while the suite runs. One file per check keeps a running check's preview
    // scoped to that check instead of the whole suite's interleaved output.
    let log_dir = turn_end_log_dir(orch, job_id);
    prepare_log_dir(&log_dir);
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "check_result_cache", "action": "update"}),
    );

    // 11. Build the changed-files override consumed by diff-scoped check scripts.
    // Cell checkouts are materialized at the immutable request base, so the
    // already-computed agent delta remains the canonical attribution source.
    // Turn-end waves plan only `when: review` suites, and the executor-protocol
    // comparison is a write-cadence suite, so no base-content bundle rides here.
    let extra_env = crate::execution::checks::check_transport_env(&changed, None);

    // 12. Submit every remaining miss through one sequential pure-verdict lease,
    // then feed the keyed outcomes through the shared persistence, parsing, and
    // ordering path. Lanes held back above are absent from the batch and absent
    // from its results; the engine answers each of them from the stored verdict,
    // so they still land on the tree, in plan order, in the delivery below.
    let timeouts: Vec<u32> = to_run
        .iter()
        .map(|(plan, _)| {
            resolve_check_timeout_ms(checks.get(&plan.name), DEFAULT_REVIEW_CHECK_TIMEOUT_MS)
        })
        .collect();
    let checks_tool_id = format!("turn-checks:{job_id}");
    // Resolved once, and handed to both the submission that reserves each item
    // and the runner that stores what comes back. A turn-end wave is where a
    // check with an unsatisfiable selector lives, so the fleet counter here is
    // what stops `executor-windows-check` re-arming on every commit.
    let infra_scopes = review_infra_scopes(
        orch,
        &coords.project_id,
        job_id,
        &sealed_commit,
        &coords.repository_path,
        &to_run
            .iter()
            .map(|(plan, _)| review_check_placement_question(checks.get(&plan.name)))
            .collect::<Vec<_>>(),
    )
    .await;
    let batched_results = if submit.is_empty() {
        log::info!(
            "turn-end checks for job {}: every applicable lane already holds a verdict for this \
             tree; taking no admission",
            short_id(job_id)
        );
        std::collections::HashMap::new()
    } else {
        let items: Vec<_> = submit
            .iter()
            .map(|&index| {
                let (plan, input_hash) = &to_run[index];
                let log_path = turn_end_log_path(orch, job_id, &plan.name);
                let _ = std::fs::write(log_path, b"");
                PlannedCheckBatchItem {
                    index,
                    name: plan.name.clone(),
                    input_hash: input_hash.clone(),
                    resource_identity: CheckResourceIdentityInput::Configured {
                        name: plan.name.clone(),
                        check: checks
                            .get(&plan.name)
                            .expect("planned check must retain its configured definition")
                            .clone(),
                    },
                    command: plan.command.clone(),
                    stream_id: crate::mcp::handlers::run::check_stream_id(&checks_tool_id, index),
                    env: extra_env.clone(),
                    verdict_environment_names: plan.verdict_environment_names.clone(),
                    timeout_ms: timeouts[index].into(),
                    executor: checks
                        .get(&plan.name)
                        .and_then(|check| check.executor.clone()),
                    verdict_platforms: checks
                        .get(&plan.name)
                        .map(crate::execution::check_identity::verdict_platforms)
                        .unwrap_or_default(),
                    resource_class: plan.resource_class,
                    infra_scopes: infra_scopes[index].clone(),
                    avoid_executor_ids: Vec::new(),
                }
            })
            .collect();
        let batch = PlannedCheckBatchRequest {
            project_id: coords.project_id.clone(),
            repository: canonical_repo,
            store_dir,
            sealed_commit: sealed_commit.clone(),
            requesting_job_id: job_id.to_string(),
            owner: cairn_common::executor_protocol::CellOwnerRef {
                project_id: coords.project_id.clone(),
                project_key: Some(coords.project_key.clone()),
                issue_number: Some(coords.number),
                job_id: Some(job_id.to_string()),
                execution_seq: Some(coords.exec_seq),
                node_kind: Some(coords.node_segment.clone()),
            },
            affinity_key: Some(job_id.to_string()),
            priority: CellPriority::ReviewCheck,
            env: extra_env,
            items,
            run_context: None,
            mutation_policy: crate::fleet::MutationPolicy::PureVerdict,
            status_board: None,
            // A turn-end wave answers for one node's own tip, so no other wave
            // is asking its question and there is nothing for it to displace.
            purpose: None,
        };
        // The last decision before the machine is asked for anything. Everything
        // since the first check took minutes, and a merge landing inside that window
        // is exactly the specimen this closes: the wave withdraws here having taken
        // no admission, rather than composing a red infrastructure check out of a
        // teardown Cairn performed itself.
        if let WaveLaunchability::Withdrawn(withdrawal) = turn_end_launchability(orch, job_id).await
        {
            log::info!(
                "turn-end checks for job {}: {}; withdrawing {} planned check(s) before submission",
                short_id(job_id),
                withdrawal.describe(),
                submit.len()
            );
            return Ok(TurnEndDisposition::Withdrawn(withdrawal.describe()));
        }
        transition_turn_end_attempt(orch, job_id, attempt_id, "submitted", None)?;
        let outcome = tokio::select! {
            biased;
            reason = cancel.cancelled() => {
                log::info!(
                    "turn-end checks for job {}: cancelled mid-suite because {}; abandoning {} check(s)",
                    short_id(job_id),
                    reason.describe(),
                    submit.len()
                );
                // Returning drops the batch future, and that is the whole
                // cancellation: each lane's subscriber guard releases this
                // wave's interest, and a shared execution keeps running for
                // whoever else in this job still waits on it — a manual check,
                // a write-cadence check, a merge gate. A job-wide cancel here
                // detached those consumers too, and they read it as the
                // machine vanishing.
                let _ = orch
                    .services
                    .emitter
                    .emit("substrate-health-change", serde_json::json!({}));
                return Ok(TurnEndDisposition::Cancelled(format!(
                    "cancelled while the check wave was submitted because {}",
                    reason.describe()
                )));
            }
            results = submit_planned_check_batch(orch, batch) => results
                .map_err(|error| format!("turn-end check batch configuration failed: {error}"))?,
        };
        outcome.results
    };
    for (index, result) in &batched_results {
        if let Ok(result) = result {
            let _ = std::fs::write(
                turn_end_log_path(orch, job_id, &to_run[*index].0.name),
                &result.output,
            );
        }
    }
    let batched_results = std::sync::Arc::new(std::sync::Mutex::new(batched_results));
    let sealed_coordinates: Vec<_> = to_run
        .iter()
        .map(|(_, input_hash)| {
            crate::execution::cache::SealedCheckCoordinate::new(
                &sealed_commit,
                &defined_by_commit,
                &tree_hash,
                input_hash,
            )
        })
        .collect();
    let outcomes = run_planned_checks_at_commit_with_coordinates(
        db.clone(),
        &coords.project_id,
        crate::execution::checks::CheckRunCommit {
            evaluated: &sealed_commit,
            defined_by: &defined_by_commit,
        },
        &tree_hash,
        job_id,
        &to_run,
        &sealed_coordinates,
        &infra_scopes,
        &checks_tool_id,
        CheckExecMode::Shared,
        Some(orch),
        move |index, _command, _stream_id| {
            let batched_results = batched_results.clone();
            async move {
                batched_results
                    .lock()
                    .unwrap()
                    .remove(&index)
                    .unwrap_or_else(|| {
                        Err(crate::execution::checks::CheckExecutionFailure::substrate(
                            crate::execution::checks::SubstrateFailureShape::Result,
                            format!("missing review batch outcome for plan index {index}"),
                        ))
                    })
            }
        },
        |_| {},
    )
    .await;

    let verdicts: Vec<String> = outcomes
        .iter()
        .map(|o| {
            let machine = o
                .recorded
                .as_ref()
                .and_then(|recorded| recorded.executor_id.as_deref())
                .map(|id| format!(", {}", orch.fleet.executor_name_for_text(id)))
                .unwrap_or_default();
            format!(
                "{}={} ({}ms{machine})",
                o.name,
                if o.passed { "pass" } else { "fail" },
                o.duration_ms
            )
        })
        .collect();

    // Nudge any live PR-node / `/checks` view to re-render with the fresh verdicts.
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "check_result_cache", "action": "update"}),
    );

    // 13. Deliver one push per distinct check state. Red execution evidence is
    // deliberately not reusable, so the attention ledger, not the result cache,
    // owns notification dampening. The stable lane-state key records green/red
    // transitions without treating volatile execution detail as a new state.
    let checks_uri = checks_uri_for_job(&coords);
    let TurnCheckOutcomeDelivery {
        fingerprint,
        any_genuine_failed,
        delivery,
        wake: effective_wake,
    } = deliver_turn_check_outcomes(&owning, job_id, &checks_uri, &tree_hash, &outcomes).await?;

    log::info!(
        "turn-end checks for job {}: completed \u{2014} [{}] \u{2192} {}",
        short_id(job_id),
        verdicts.join(", "),
        if any_genuine_failed {
            "wake"
        } else {
            "passive"
        }
    );

    if delivery == TurnCheckDelivery::Suppress {
        log::info!(
            "turn-end checks for job {}: deduped unchanged check state",
            short_id(job_id)
        );
    } else {
        let TurnCheckDelivery::Deliver(decision) = delivery else {
            unreachable!("suppression handled above")
        };
        log::info!(
            "turn-end checks for job {}: delivery decision {}",
            short_id(job_id),
            decision
        );
        if any_genuine_failed && effective_wake == Some(attention_push::Wake::Wake) {
            if let Err(error) = crate::messages::delivery::nudge_job_for_urgency(
                orch,
                job_id,
                crate::messages::queued::DeliveryUrgency::Steer,
            ) {
                log::warn!(
                    "turn-check failure wake for job {} failed: {}",
                    short_id(job_id),
                    error
                );
            }
        }
    }

    if reports_fresh_infrastructure(&outcomes) {
        if let (Some(issue_id), Some(fingerprint)) = (
            issue_id_for_job(&owning, job_id).await,
            fingerprint.as_deref(),
        ) {
            let operator_key = format!("turn-checks-infrastructure:{checks_uri}");
            match crate::orchestrator::parent_wake::queue_passive_parent_push(
                &owning,
                &issue_id,
                &checks_uri,
                &operator_key,
                fingerprint,
            )
            .await
            {
                Ok(true) => log::info!(
                    "turn-end checks for job {}: queued passive infrastructure signal",
                    short_id(job_id)
                ),
                Ok(false) => log::debug!(
                    "turn-end checks for job {}: infrastructure signal deduped or has no parent route",
                    short_id(job_id)
                ),
                Err(error) => log::warn!(
                    "turn-end checks for job {}: failed to route infrastructure signal: {}",
                    short_id(job_id),
                    error
                ),
            }
        } else {
            log::warn!(
                "turn-end checks for job {}: infrastructure signal fingerprint or parent issue unavailable",
                short_id(job_id)
            );
        }
    }
    Ok(TurnEndDisposition::CompletedWithDelivery)
}

const TURN_CHECK_FINGERPRINT_VERSION: &str = "turn-check-state-v3";

fn normalized_salient(value: &str) -> Option<String> {
    let normalized = value.replace("\r\n", "\n").replace('\r', "\n");
    let trimmed = normalized.trim();
    if trimmed.is_empty() || trimmed.contains('\0') {
        return None;
    }
    Some(trimmed.to_string())
}

/// The loop-break gate: split a wave's plans into the ones that go on to
/// submission and the names infrastructure suppression holds back, re-stamping
/// each held-back lane onto this head.
///
/// A lane in `unplaceable` always goes on, whatever its counters say. A bound
/// describes failures that happened while some machine could take the lane; once
/// none can, re-stamping that suppression would report a repair that can no
/// longer happen, and would keep an infrastructure row current for the dormant
/// re-arm to keep nominating. Submission refuses it as not placeable instead.
/// The bounded rows themselves are left exactly as they are: they are still the
/// record of what happened, and still bound the lane if a machine returns.
#[allow(clippy::too_many_arguments)]
/// The placement question a review-cadence check asks. It also names the fleet
/// counter that bounds the check, so every site that reads or charges that
/// counter for a review check has to ask it identically.
pub(crate) fn review_check_placement_question(
    check: Option<&crate::config::project_settings::CheckCommand>,
) -> crate::execution::checks::CheckPlacementQuestion {
    crate::execution::checks::CheckPlacementQuestion {
        selector: check.and_then(|check| check.executor.clone()),
        verdict_platforms: check
            .map(crate::execution::check_identity::verdict_platforms)
            .unwrap_or_default(),
    }
}

/// The suppression scope sets a review wave for `job_id` at `commit` reads and
/// charges, one per placement question, in order.
pub(crate) async fn review_infra_scopes(
    orch: &Orchestrator,
    project_id: &str,
    job_id: &str,
    commit: &str,
    repository: &str,
    questions: &[crate::execution::checks::CheckPlacementQuestion],
) -> Vec<crate::execution::cache::InfraScopes> {
    crate::execution::checks::plan_infra_scopes(
        Some(orch),
        crate::execution::checks::InfraScopeInputs {
            project_id,
            job_id,
            commit_sha: commit,
            repository,
            mutation_policy: crate::fleet::MutationPolicy::PureVerdict,
            work_class: crate::fleet::PlacementWorkClass::ReviewChecks,
        },
        questions,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn hold_back_suppressed(
    db: &std::sync::Arc<LocalDb>,
    project_id: &str,
    tree_hash: &str,
    job_id: &str,
    commit: &str,
    plans: Vec<CheckPlan>,
    input_hashes: &std::collections::HashMap<String, String>,
    infra_scopes: &std::collections::HashMap<String, crate::execution::cache::InfraScopes>,
    unplaceable: &std::collections::HashSet<String>,
) -> (Vec<(CheckPlan, String)>, Vec<String>) {
    let mut to_run: Vec<(CheckPlan, String)> = Vec::new();
    let mut suppressed: Vec<String> = Vec::new();
    for plan in plans {
        let input_hash = input_hashes
            .get(&plan.name)
            .expect("every configured check is keyed at the sealed tree")
            .clone();
        if unplaceable.contains(&plan.name) {
            to_run.push((plan, input_hash));
            continue;
        }
        // Turn-end checks are fleet-backed. Placement has not happened yet,
        // so a coordinator-local observation cannot prove the environment of
        // the executor that will be selected. Preserve infrastructure
        // suppression, but fail safe by sending every otherwise runnable plan
        // to fleet admission until selection supplies trusted identity.
        match crate::execution::cache::get_suppressed_check_result(
            db.clone(),
            project_id,
            &plan.name,
            &input_hash,
            &infra_scopes[&plan.name],
        ) {
            Ok(Some(bounded)) => {
                crate::execution::checks::record_suppressed_check(
                    db, tree_hash, job_id, commit, &plan.name, &bounded,
                );
                suppressed.push(plan.name.clone());
            }
            _ => to_run.push((plan, input_hash)),
        }
    }
    (to_run, suppressed)
}

/// The wake identity of a settled wave: its sealed tree plus the actionable
/// verdict of every lane that produced one, `pass` or `fail`.
///
/// A lane without a verdict about the change is left out entirely rather than
/// recorded as "no verdict", because one of the shapes such a lane arrives in
/// is absence: the turn-end pre-filter drops a suppressed lane before it runs,
/// while the engine hands back the same suppression as a present outcome whose
/// stored `failure_kind` may be any infrastructure kind. Only exclusion lets a
/// fresh substrate failure, either suppression shape, and a missing lane all
/// collapse to one identity, which they must, because they render identically
/// and give the author nothing to act on (cairn/4820). For the same reason a
/// genuine failure's kind and text are not identity: an OOM kill flapping with
/// an ordinary failure on the same tree is the same red to the author.
///
/// Fails open (`None`, delivered every time) only when identity is genuinely
/// ambiguous: no salient tree, no outcomes at all, or a pass that claims a
/// failure kind. A suite whose every lane lacks a verdict still has an identity,
/// the tree alone, so its passive deliveries keep dampening.
fn turn_check_fingerprint(tree_hash: &str, outcomes: &[CheckOutcome]) -> Option<String> {
    let tree_hash = normalized_salient(tree_hash)?;
    if outcomes.is_empty() {
        return None;
    }
    let mut canonical = Vec::with_capacity(outcomes.len());
    for outcome in outcomes {
        let name = normalized_salient(&outcome.name)?;
        let verdict = if outcome.passed {
            if outcome.failure_kind.is_some() {
                return None;
            }
            "pass"
        } else if outcome.is_genuine_failure() {
            "fail"
        } else {
            continue;
        };
        canonical.push((name, verdict));
    }
    canonical.sort_by(|left, right| left.0.cmp(&right.0));
    let payload =
        serde_json::to_vec(&(TURN_CHECK_FINGERPRINT_VERSION, tree_hash, canonical)).ok()?;
    Some(format!("sha256:{:x}", Sha256::digest(payload)))
}

/// The wake level a completed turn-end run is delivered at: a GENUINE failure
/// ROUSES the idle builder (`Wake`), while a clean run — and a run whose only red
/// is Cairn's own infrastructure — rides along `Passive`, delivered and recorded
/// without costing a turn. Pure, so the decision is unit-tested.
fn delivery_wake(any_genuine_failed: bool) -> attention_push::Wake {
    if any_genuine_failed {
        attention_push::Wake::Wake
    } else {
        attention_push::Wake::Passive
    }
}

/// Whether this wave carries infrastructure news for the parent's passive
/// operator signal: a lane that actually ran into a substrate failure on this
/// wave. A suppressed lane does not count, whichever gate caught it. A lane is
/// only suppressed after repeated fresh infrastructure failures, each of which
/// already reached the parent, and the suppression itself is the absence of a
/// run, which `/checks` shows. Counting it would also make the signal depend on
/// whether the pre-filter (which drops the lane) or submission (which keeps it
/// with its stored kind) did the suppressing.
fn reports_fresh_infrastructure(outcomes: &[CheckOutcome]) -> bool {
    outcomes.iter().any(|outcome| {
        !outcome.passed
            && outcome.suppressed_after.is_none()
            && !outcome
                .failure_kind
                .is_some_and(CheckFailureKind::attempted_nothing)
            && outcome
                .failure_kind
                .is_some_and(CheckFailureKind::is_infrastructure)
    })
}

/// First 8 chars of a job id for log lines (mirrors the ids elsewhere in this
/// module), clamped so a short id never panics.
fn short_id(job_id: &str) -> &str {
    &job_id[..job_id.len().min(8)]
}

/// Render the `### Systematic checks` section for a node job: the "running" live
/// log tail while a suite is in flight, plus the cached per-check verdicts for the
/// node's current sealed tree. Returns `None` when there is nothing to show (no
/// resolvable worktree/tree, and neither a running suite nor any cached verdict) —
/// callers omit the section entirely. Shared by the PR-node view and the `/checks`
/// read projection.
pub(crate) async fn render_turn_end_checks_section(
    orch: &Orchestrator,
    job_id: &str,
) -> Option<String> {
    if let Some(retired) = crate::execution::checks_status::node_retirement(orch, job_id).await {
        let statuses = crate::execution::checks_status::node_check_statuses(orch, job_id).await;
        return Some(format_retired_checks_section(&retired, statuses.as_deref()));
    }
    let statuses = crate::execution::checks_status::node_check_statuses(orch, job_id).await?;
    format_checks_section(
        &statuses,
        crate::execution::checks_status::WaveCadence::of(orch, job_id),
    )
}

/// Pure renderer for the `### Systematic checks` section. Returns `None` when the
/// project has no configured checks. Split out so every status renders without a
/// DB or worktree. `cadence` is the waves in flight, which is what separates a
/// lane still to come (`pending`) from one nothing will produce (`absent`).
fn format_checks_section(
    statuses: &[crate::execution::checks_status::NodeCheckStatus],
    cadence: crate::execution::checks_status::WaveCadence,
) -> Option<String> {
    if statuses.is_empty() {
        return None;
    }
    let mut out = String::from("\n### Systematic checks\n\n");
    push_check_lines(&mut out, statuses, Some(cadence));
    Some(out)
}

/// The section for a node whose issue has resolved. Always rendered: a retired
/// node's lanes are on record or they are not, and either way the reader is told
/// which, and for which head, rather than that results are still to come.
/// `statuses` is `None` when the recorded lanes could not be read.
fn format_retired_checks_section(
    retired: &crate::execution::checks_status::RetiredNode,
    statuses: Option<&[crate::execution::checks_status::NodeCheckStatus]>,
) -> String {
    let outcome = &retired.outcome;
    let mut out = String::from("\n### Systematic checks\n\n");
    let Some(head) = retired.head() else {
        out.push_str(&format!(
            "This node was {outcome} before any review wave recorded a plan, so no lanes are \
             on record for it.\n"
        ));
        return out;
    };
    let head = &head[..head.len().min(12)];
    out.push_str(&format!(
        "This node was {outcome}. Its branch is retired, so these are the lanes recorded for \
         its last head, `{head}`, and nothing will run them again.\n\n"
    ));
    match statuses {
        Some([]) => out.push_str("That head configured no checks.\n"),
        Some(statuses) => push_check_lines(&mut out, statuses, None),
        None => out.push_str("Its recorded lanes could not be read just now.\n"),
    }
    out
}

/// One line per lane, with a failing or running lane's output beneath it.
///
/// The words follow [`crate::execution::checks_status::LaneExistence`], the
/// same states the merge's verdict gate names, so the block a reader sees
/// before merging is the refusal they would get: a lane with no row is
/// `pending` while a wave of its cadence is in flight and `absent` when none
/// is, and a run that ended without a verdict reads as `died` or `breaker`,
/// never as a red. `cadence` is `None` for a retired node, whose rowless lanes
/// will never get a verdict.
fn push_check_lines(
    out: &mut String,
    statuses: &[crate::execution::checks_status::NodeCheckStatus],
    cadence: Option<crate::execution::checks_status::WaveCadence>,
) {
    use crate::execution::checks_status::{
        format_status_annotation, formatted_failure_names, lane_existence, LaneExistence,
        NodeCheckState,
    };
    for status in statuses {
        let existence = lane_existence(status, cadence.unwrap_or_default());
        match existence {
            Some(LaneExistence::Died { .. }) => {
                let annotation = format_status_annotation(status)
                    .map(|a| format!(" \u{2014} {a}"))
                    .unwrap_or_default();
                out.push_str(&format!(
                    "- \u{2298} {}: died without a verdict{annotation}\n",
                    status.name
                ));
                if let Some(detail) = status
                    .output_tail
                    .as_deref()
                    .filter(|s| !s.trim().is_empty())
                {
                    out.push_str("\n```\n");
                    out.push_str(detail.trim_end());
                    out.push_str("\n```\n");
                }
                continue;
            }
            Some(LaneExistence::NoSelectableWork) => {
                out.push_str(&format!(
                    "- {}: not applicable: no selectable work for this diff\n",
                    status.name
                ));
                continue;
            }
            Some(LaneExistence::Breaker { after }) => {
                out.push_str(&format!(
                    "- \u{2298} {}: breaker \u{2014} not run after {after} consecutive \
                     infrastructure failures, no verdict\n",
                    status.name
                ));
                continue;
            }
            _ => {}
        }
        match status.state {
            NodeCheckState::Passed => {
                let annotation = format_status_annotation(status)
                    .map(|a| format!(" ({a})"))
                    .unwrap_or_default();
                out.push_str(&format!("- \u{2713} {}{annotation}\n", status.name));
            }
            NodeCheckState::Unverified => {
                let annotation = format_status_annotation(status)
                    .map(|a| format!(" — {a}"))
                    .unwrap_or_default();
                out.push_str(&format!("- ⚠ {}{annotation}\n", status.name));
            }
            NodeCheckState::Failed => {
                let annotation = format_status_annotation(status)
                    .map(|a| format!(" \u{2014} {a}"))
                    .or_else(|| formatted_failure_names(status).map(|n| format!(" \u{2014} {n}")))
                    .unwrap_or_default();
                out.push_str(&format!("- \u{2717} {}{annotation}\n", status.name));
                if let Some(detail) = status
                    .output_tail
                    .as_deref()
                    .filter(|s| !s.trim().is_empty())
                {
                    out.push_str("\n```\n");
                    out.push_str(detail.trim_end());
                    out.push_str("\n```\n");
                }
            }
            NodeCheckState::Running => {
                out.push_str(&format!("- {}: _running\u{2026}_\n", status.name));
                if let Some(tail) = status
                    .output_tail
                    .as_deref()
                    .filter(|t| !t.trim().is_empty())
                {
                    out.push_str("\n```\n");
                    out.push_str(tail.trim_end());
                    out.push_str("\n```\n");
                }
            }
            NodeCheckState::Pending => match (cadence, existence) {
                (None, _) => out.push_str(&format!("- {}: no verdict\n", status.name)),
                (Some(_), Some(LaneExistence::Absent)) => out.push_str(&format!(
                    "- {}: absent \u{2014} no lane has run it on this head\n",
                    status.name
                )),
                (Some(_), _) => out.push_str(&format!("- {}: pending\n", status.name)),
            },
            NodeCheckState::NotApplicable => {
                out.push_str(&format!("- {}: not applicable\n", status.name));
            }
            NodeCheckState::Unsupported => {
                let notice = status.notice.as_deref().unwrap_or("needs a newer Cairn");
                out.push_str(&format!("- {}: {notice}\n", status.name));
            }
        }
    }
}

/// The node's coordinates resolved from a `job_id` in one query.
pub(crate) struct JobCoords {
    pub(crate) project_id: String,
    pub(crate) repository_path: String,
    pub(crate) branch: String,
    pub(crate) base_branch: Option<String>,
    pub(crate) project_key: String,
    pub(crate) number: i32,
    pub(crate) exec_seq: i32,
    pub(crate) node_segment: String,
    parent_segment: Option<String>,
    is_workflow: bool,
}

/// Whether a cache-filtered turn-end plan set may launch, and the plans if so.
///
/// `Launch` is the ONLY variant that carries plans, and the plan set is the only
/// input to the batch items the cadence submits. That is the point of this type:
/// a cell request cannot be constructed without first going through
/// [`turn_end_launch`], so no future edit can accidentally submit past a skip.
#[derive(Debug, PartialEq, Eq)]
enum TurnEndLaunch {
    /// Submit these plans. Non-empty by construction.
    Launch(Vec<(CheckPlan, String)>),
    /// Run nothing and take no admission.
    Skip(TurnEndSkip),
}

#[derive(Debug, PartialEq, Eq)]
enum TurnEndSkip {
    /// Every applicable check already has a verdict for this exact tree.
    FullyCached,
    /// The node's sealed tree is byte-identical to its base's. Carries the names
    /// that would otherwise have run, for the log.
    TreeMatchesBase(Vec<String>),
}

/// Decide whether a cache-filtered turn-end plan set may take slot admission.
///
/// A node whose sealed tree is byte-identical to its base's changed nothing by
/// construction, so no check run against it could produce a verdict about the
/// node rather than about the base. Such a node runs nothing at all — the point
/// is that it takes NO admission, so the decision has to be made here, above
/// submission, rather than discovered after a slot has already been leased.
///
/// The tree check is evaluated even when uncached plans remain, which is the
/// whole case that matters: a stale base coordinate makes the changed-file gate
/// upstream select real checks for a node that changed nothing (CAIRN-3108).
/// Deciding only on `uncached.is_empty()` is exactly the bug.
///
/// Verdicts already cached for this tree are re-stamped onto the job by the
/// cache-filtering pass that produces `uncached`, so that inheritance has
/// necessarily already happened before this is called: a skipped zero-delta node
/// still renders the base's results rather than an empty checklist. Whatever is
/// left uncovered stays uncovered — a check that has never run on this tree is
/// the base's business, not this node's.
fn turn_end_launch(uncached: Vec<(CheckPlan, String)>, tree_matches_base: bool) -> TurnEndLaunch {
    if uncached.is_empty() {
        return TurnEndLaunch::Skip(TurnEndSkip::FullyCached);
    }
    if tree_matches_base {
        return TurnEndLaunch::Skip(TurnEndSkip::TreeMatchesBase(
            uncached
                .into_iter()
                .map(|(plan, _)| plan.name)
                .collect::<Vec<_>>(),
        ));
    }
    TurnEndLaunch::Launch(uncached)
}

/// The coordinate whose TREE defines "unchanged" for this node.
///
/// This is the integration target's CURRENT tip, in deliberate contrast to the
/// changed-file diff in `run_turn_end_checks_inner`, which wants the node's own
/// delta and so starts at its fork point. A node whose sealed tree equals the
/// tree the base branch holds right now changed nothing that anyone will review,
/// whatever its history looks like.
///
/// A branch name resolves fresh from the store on every evaluation, so it cannot
/// go stale the way `jobs.base_commit` does, and comparing trees rather than
/// coordinates makes the answer a fact about content rather than about
/// bookkeeping (CAIRN-3108).
fn base_tree_coordinate(coords: &JobCoords) -> Option<&str> {
    coords.base_branch.as_deref()
}

pub(crate) fn checks_uri_for_job(coords: &JobCoords) -> String {
    match (coords.is_workflow, coords.parent_segment.as_deref()) {
        (true, _) => build_node_checks_uri(
            &coords.project_key,
            coords.number,
            coords.exec_seq,
            &coords.node_segment,
        ),
        (false, Some(parent)) => build_task_checks_uri(
            &coords.project_key,
            coords.number,
            coords.exec_seq,
            parent,
            &coords.node_segment,
        ),
        (false, None) => build_node_checks_uri(
            &coords.project_key,
            coords.number,
            coords.exec_seq,
            &coords.node_segment,
        ),
    }
}

/// Resolve the durable project, branch, base anchors, and URI identity for a job.
pub(crate) async fn resolve_job_coords(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<JobCoords>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT j.project_id, p.repo_path, j.branch, j.base_branch,
                            p.key, i.number, e.seq, j.uri_segment,
                            parent.uri_segment, j.agent_config_id
                     FROM jobs j
                     JOIN projects p ON p.id = j.project_id
                     JOIN issues i ON i.id = j.issue_id
                     JOIN executions e ON e.id = j.execution_id
                     LEFT JOIN jobs parent ON j.parent_job_id = parent.id
                     WHERE j.id = ?1 LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(JobCoords {
                    project_id: row.text(0)?,
                    repository_path: row.text(1)?,
                    branch: row.opt_text(2)?.filter(|s| !s.is_empty()).ok_or_else(|| {
                        crate::storage::DbError::Row(format!("job {job_id} has no branch"))
                    })?,
                    base_branch: row.opt_text(3)?.filter(|s| !s.is_empty()),
                    project_key: row.text(4)?,
                    number: row.i64(5)? as i32,
                    exec_seq: row.i64(6)? as i32,
                    node_segment: row.opt_text(7)?.unwrap_or_default(),
                    parent_segment: row.opt_text(8)?,
                    is_workflow: row.opt_text(9)?.as_deref() == Some("workflow"),
                })),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| format!("failed to resolve job coords: {e}"))
}

/// The host-readable, job-scoped directory holding ONE live log file per check
/// for a turn-end run. Lives under the app state dir (not the worktree) so it
/// survives worktree teardown for the PR-node render.
fn turn_end_log_dir(orch: &Orchestrator, job_id: &str) -> PathBuf {
    orch.config_dir.join("turn-checks").join(job_id)
}

/// The live log file for a SINGLE check within a job's turn-end run. Each check
/// tees into its OWN file (created the instant it starts), so the PR-node /
/// `/checks` render can tail exactly that check's output — several may be running
/// and tailing at once under concurrent isolation — instead of a shared blob that
/// made every running check preview the same interleaved text.
fn turn_end_log_path(orch: &Orchestrator, job_id: &str, check_name: &str) -> PathBuf {
    turn_end_log_dir(orch, job_id).join(format!("{}.log", sanitize_log_name(check_name)))
}

/// Slugify a check name into a filesystem-safe log filename stem: any character
/// outside `[A-Za-z0-9._-]` becomes `_`. Real check names are already slugs, so
/// this only guards against pathological config.
fn sanitize_log_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Clear the job's per-check log directory so a fresh suite starts clean — a stale
/// per-check log must not make a not-yet-started check look like it is running
/// with old output. Best-effort: a failure here only costs the live tail, never
/// the run.
fn prepare_log_dir(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::create_dir_all(dir);
}

/// Whether a single check's per-check log file exists yet. The runner creates the
/// file the instant a check starts — before any output — so existence marks the
/// check as actively RUNNING even while it is still silent (e.g. `tsc --noEmit`
/// before its first line). A queued check has no file after `prepare_log_dir`
/// cleared the directory, so it reads as pending instead.
pub(crate) fn turn_end_check_started(orch: &Orchestrator, job_id: &str, check_name: &str) -> bool {
    turn_end_log_path(orch, job_id, check_name).exists()
}

/// Last `max_chars` chars of a single check's live log file, or `None` when it is
/// missing/empty (that check exists but has not produced output yet). Existence is
/// a SEPARATE signal ([`turn_end_check_started`]): a running-but-silent check has
/// a file with no tail, so callers must not infer "queued" from a `None` tail.
pub(crate) fn read_turn_end_log_tail(
    orch: &Orchestrator,
    job_id: &str,
    check_name: &str,
) -> Option<String> {
    read_log_tail(&turn_end_log_path(orch, job_id, check_name), LOG_TAIL_CHARS)
}

/// Last `max_chars` chars of a log file at `path`, or `None` when it is missing or
/// blank. Reads only enough bytes from the end to hold that many UTF-8 characters,
/// so polling a multi-megabyte cargo or vitest log stays constant-cost.
///
/// Split from [`read_turn_end_log_tail`] so the missing/empty-vs-content boundary
/// and large-file behavior are unit-testable without an [`Orchestrator`].
fn read_log_tail(path: &Path, max_chars: usize) -> Option<String> {
    if max_chars == 0 {
        return None;
    }
    let mut file = std::fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    let max_bytes = max_chars.saturating_mul(4) as u64;
    let start = len.saturating_sub(max_bytes);
    file.seek(SeekFrom::Start(start)).ok()?;

    let mut bytes = Vec::with_capacity((len - start) as usize);
    file.read_to_end(&mut bytes).ok()?;
    // A concurrent writer can leave the sampled suffix on a partial UTF-8 code
    // point. Lossy decoding preserves the useful tail instead of dropping the
    // whole update; the next poll replaces any transient replacement character.
    let content = String::from_utf8_lossy(&bytes);
    let trimmed = content.trim_end();
    if trimmed.is_empty() {
        return None;
    }
    Some(tail(trimmed, max_chars))
}

/// Last `max_chars` characters of `s`, on a char boundary.
fn tail(s: &str, max_chars: usize) -> String {
    let count = s.chars().count();
    if count <= max_chars {
        return s.to_string();
    }
    s.chars().skip(count - max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::checks_status::{NodeCheckState, NodeCheckStatus};
    use cairn_common::executor_protocol::{
        ActiveCellRequest, CellAdmissionKind, CellCheckoutKind, CellCommandClass, CellOccupancy,
        CellOwnerRef, CellPriority, CellResidency, OwnerDeathPolicy, PersistentCellLifecycle,
        PersistentCellState, QueuedCellRequest, RepositoryLocator, ResidencyFootprint,
        ResidencyHolder, ResidencyPhase, ResourceReservation,
    };

    fn cell_held_by(holder: ResidencyHolder, phase: ResidencyPhase) -> PersistentCellState {
        PersistentCellState {
            observed_head: None,
            size_bytes: 0,
            warm_command_classes: Vec::new(),
            executor_id: "executor".into(),
            executor_display_name: None,
            project_id: "p".into(),
            cell_id: "cell-1".into(),
            path: "/cell".into(),
            workspace_name: "workspace".into(),
            repository: "/repo".into(),
            checkout_kind: CellCheckoutKind::default(),
            git_common_dir: None,
            authority_path: String::new(),
            lifecycle: PersistentCellLifecycle::Idle,
            cell_epoch: 1,
            last_sealed_commit: None,
            last_distance: None,
            quarantine_reason: None,
            last_used_unix_ms: 0,
            last_affinity_key: None,
            preparation_fingerprint: None,
            residency: Some(CellResidency {
                holder,
                repository: RepositoryLocator::ColocatedPath {
                    project_id: "p".into(),
                    repository_id: "p".into(),
                    absolute_path: "/repo".into(),
                },
                owner_ref: None,
                selector: None,
                incarnation_id: "inc".into(),
                current_base_commit: "base".into(),
                phase,
                last_heartbeat_unix_ms: 0,
                reclaim_deadline_unix_ms: 0,
                death_policy: OwnerDeathPolicy {
                    heartbeat_timeout_ms: 1_000,
                    reclaim_grace_ms: 1_000,
                },
                footprint: ResidencyFootprint::default(),
                state_revision: 1,
                events: Vec::new(),
            }),
            occupancy: CellOccupancy::default(),
            seeded_from: None,
        }
    }

    fn active_command(
        job: Option<&str>,
        owner_job: Option<&str>,
        command: &str,
    ) -> ActiveCellRequest {
        ActiveCellRequest {
            executor_id: "executor".into(),
            request_id: format!("request-{command}"),
            attempt_id: "attempt".into(),
            command: command.into(),
            command_class: CellCommandClass::default(),
            owner: owner_job.map(|job| CellOwnerRef {
                project_id: "p".into(),
                project_key: None,
                issue_number: None,
                job_id: Some(job.into()),
                execution_seq: None,
                node_kind: None,
            }),
            priority: CellPriority::AgentInteractive,
            requesting_job_id: job.map(str::to_string),
            affinity_key: None,
            queued_at_unix_ms: 0,
            started_at_unix_ms: Some(1),
            stage: None,
            resource_reservation: ResourceReservation::default(),
            learned_estimate: None,
            subscriber_count: 1,
        }
    }

    fn queued_command(job: Option<&str>, command: &str) -> QueuedCellRequest {
        QueuedCellRequest {
            admission_kind: CellAdmissionKind::default(),
            executor_id: "executor".into(),
            request_id: format!("request-{command}"),
            attempt_id: "attempt".into(),
            project_id: "p".into(),
            command: command.into(),
            command_class: CellCommandClass::default(),
            owner: None,
            priority: CellPriority::AgentInteractive,
            effective_priority: None,
            requesting_job_id: job.map(str::to_string),
            affinity_key: None,
            command_resource_identity: None,
            queued_at_unix_ms: 0,
            resource_reservation: ResourceReservation::default(),
            learned_estimate: None,
            subscriber_count: 1,
            substrate_hold: None,
            placement: None,
        }
    }

    fn cell_running(command: ActiveCellRequest) -> PersistentCellState {
        let mut cell = cell_held_by(
            ResidencyHolder::Job {
                job_id: "j-1".into(),
            },
            ResidencyPhase::Active,
        );
        cell.occupancy = CellOccupancy {
            command: Some(command),
            ..CellOccupancy::default()
        };
        cell
    }

    fn branch_jobs(ids: &[&str]) -> BTreeSet<String> {
        ids.iter().map(|id| id.to_string()).collect()
    }

    /// The wave yields to work the node itself is running, whoever on the branch
    /// submitted it and whether it is running or still queued behind admission.
    #[test]
    fn a_nodes_own_running_and_queued_work_both_count_as_busy() {
        let jobs = branch_jobs(&["j-1", "j-task"]);
        let running = cairn_common::executor_protocol::FleetSnapshot {
            cells: vec![cell_running(active_command(
                Some("j-1"),
                None,
                "cargo test --workspace",
            ))],
            ..Default::default()
        };
        assert_eq!(
            node_work_in_flight(&running, &jobs),
            Some(NodeWorkInFlight {
                requests: 1,
                command: "cargo test --workspace".into()
            })
        );

        let queued = cairn_common::executor_protocol::FleetSnapshot {
            queued_requests: vec![queued_command(Some("j-task"), "bun run test:frontend")],
            ..Default::default()
        };
        assert_eq!(
            node_work_in_flight(&queued, &jobs).map(|work| work.requests),
            Some(1),
            "a request holding a place in an admission queue is about to be a running command"
        );

        let owner_only = cairn_common::executor_protocol::FleetSnapshot {
            cells: vec![cell_running(active_command(
                None,
                Some("j-1"),
                "cargo build",
            ))],
            ..Default::default()
        };
        assert_eq!(
            node_work_in_flight(&owner_only, &jobs).map(|work| work.requests),
            Some(1),
            "different submitters populate different ownership fields; either one is the node's"
        );
    }

    /// Deferring to somebody else's work would park a wave on a node that is
    /// idle, which is the one state it should be running in.
    #[test]
    fn another_nodes_work_never_defers_this_wave() {
        let snapshot = cairn_common::executor_protocol::FleetSnapshot {
            cells: vec![cell_running(active_command(
                Some("j-elsewhere"),
                None,
                "cargo test",
            ))],
            queued_requests: vec![queued_command(None, "main-health")],
            ..Default::default()
        };
        assert_eq!(node_work_in_flight(&snapshot, &branch_jobs(&["j-1"])), None);
        assert_eq!(
            node_work_in_flight(&snapshot, &BTreeSet::new()),
            None,
            "an empty branch set owns no work"
        );
        assert_eq!(
            node_work_in_flight(&Default::default(), &branch_jobs(&["j-1"])),
            None,
            "an idle fleet defers nothing"
        );
    }

    /// A verdict landing while the wave waited is the reuse the deferral exists
    /// to reach. A lane holding one must not be submitted again — and must keep
    /// its index, because that is how the engine matches batch results back to
    /// plans.
    #[test]
    fn a_lane_answered_while_the_wave_waited_is_not_submitted_again() {
        let lanes = vec![uncached_plan("rust-tests"), uncached_plan("rust-lint")];
        assert_eq!(lanes_to_submit(&lanes, |_, _| false), vec![0, 1]);
        assert_eq!(
            lanes_to_submit(&lanes, |_, input_hash| input_hash == "input-rust-tests"),
            vec![1],
            "the surviving lane keeps its position in the plan set"
        );
        assert!(
            lanes_to_submit(&lanes, |_, _| true).is_empty(),
            "a fully answered set takes no admission at all"
        );
        assert!(lanes_to_submit(&[], |_, _| false).is_empty());
    }

    /// The whole point of the gate is that it ends. A node that never goes quiet
    /// gets its review lanes anyway, because a deferral that could last forever
    /// is a review gate that silently disappeared.
    #[test]
    fn deferral_always_ends_in_a_wave_that_runs() {
        assert_eq!(settle_decision(false, 0), SettleDecision::Proceed);
        assert_eq!(
            settle_decision(false, WAVE_SETTLE_CEILING_MS * 2),
            SettleDecision::Proceed
        );
        assert_eq!(settle_decision(true, 0), SettleDecision::Wait);
        assert_eq!(
            settle_decision(true, WAVE_SETTLE_CEILING_MS - 1),
            SettleDecision::Wait
        );
        assert_eq!(
            settle_decision(true, WAVE_SETTLE_CEILING_MS),
            SettleDecision::ProceedUnsettled,
            "at the ceiling the wave runs rather than waiting further"
        );
    }

    /// A deferral nobody can read is indistinguishable from a wave that never
    /// ran, so what was yielded to has to survive into the record.
    #[test]
    fn a_deferral_names_the_work_it_yielded_to() {
        assert_eq!(
            NodeWorkInFlight {
                requests: 1,
                command: "cargo test".into()
            }
            .describe(),
            "its own `cargo test`"
        );
        assert_eq!(
            NodeWorkInFlight {
                requests: 3,
                command: "cargo test".into()
            }
            .describe(),
            "3 commands of its own, including `cargo test`"
        );
    }

    #[test]
    fn a_command_summary_is_one_bounded_line() {
        assert_eq!(summarize_command("\n  cargo test  \nsecond"), "cargo test");
        let summary = summarize_command(&"x".repeat(200));
        assert!(summary.ends_with('\u{2026}'));
        assert_eq!(summary.chars().count(), 61);
        assert_eq!(summarize_command(""), "");
    }

    async fn launchability_orchestrator(issue_status: &str, job_status: &str) -> Orchestrator {
        let db = crate::storage::migrated_test_db("turn-end-launchability.db").await;
        db.execute_script(&format!(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','proj','/tmp/repo',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
               VALUES('i','p',4873,'I','{issue_status}','active','none',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
               VALUES('e','r','i','p','running',1,1);
             INSERT INTO jobs(id,project_id,issue_id,execution_id,status,node_name,uri_segment,created_at,updated_at)
               VALUES('j-1','p','i','e','{job_status}','builder','builder',1,1);"
        ))
        .await
        .unwrap();
        let root = tempfile::tempdir().unwrap().keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        let search = std::sync::Arc::new(
            crate::storage::SearchIndex::open_or_create(root.join("search")).unwrap(),
        );
        let db_state =
            std::sync::Arc::new(crate::db::DbState::new(std::sync::Arc::new(db), search));
        let services =
            std::sync::Arc::new(crate::services::testing::TestServicesBuilder::new().build());
        crate::orchestrator::OrchestratorBuilder::new(db_state, services, config_dir).build()
    }

    /// The cairn/4873 specimen. A PR owner idle for half an hour has its cell
    /// residency moved to `AwaitingReclaim`, and its wave used to be withdrawn
    /// for it, terminally, with nothing to re-arm it. A wave is a pure-verdict
    /// batch built from the sealed commit; the owner's cell is not where it runs,
    /// so neither reclaim phase may withhold it.
    #[tokio::test]
    async fn a_reclaiming_owner_cell_never_withholds_its_wave() {
        let orch = launchability_orchestrator("active", "complete").await;
        let job = ResidencyHolder::Job {
            job_id: "j-1".into(),
        };
        for phase in [ResidencyPhase::AwaitingReclaim, ResidencyPhase::Releasing] {
            orch.fleet
                .seed_cells_for_test(vec![cell_held_by(job.clone(), phase)]);
            assert_eq!(
                turn_end_launchability(&orch, "j-1").await,
                WaveLaunchability::Ready,
                "{phase:?}"
            );
        }
    }

    /// What still withdraws a wave: the lifecycle of the issue and the job.
    #[tokio::test]
    async fn a_resolved_issue_or_cancelled_job_still_withdraws_its_wave() {
        let merged = launchability_orchestrator("merged", "complete").await;
        assert_eq!(
            turn_end_launchability(&merged, "j-1").await,
            WaveLaunchability::Withdrawn(WaveWithdrawal::IssueResolved("merged".into()))
        );
        let cancelled = launchability_orchestrator("active", "cancelled").await;
        assert_eq!(
            turn_end_launchability(&cancelled, "j-1").await,
            WaveLaunchability::Withdrawn(WaveWithdrawal::JobCancelled)
        );
        assert_eq!(
            turn_end_launchability(&cancelled, "j-unknown").await,
            WaveLaunchability::Withdrawn(WaveWithdrawal::JobUnknown)
        );
    }

    /// Each withdrawal says which condition it was, because "nothing to run" in
    /// a log with no reason attached is what sent operators to the substrate
    /// records to reconstruct it.
    #[test]
    fn every_withdrawal_names_its_own_condition() {
        let described = [
            WaveWithdrawal::IssueResolved("merged".into()),
            WaveWithdrawal::JobCancelled,
            WaveWithdrawal::JobUnknown,
        ]
        .map(|withdrawal| withdrawal.describe());
        assert!(described[0].contains("merged"));
        assert!(described[1].contains("cancelled"));
        assert!(described[2].contains("no longer resolves"));
        assert_eq!(
            described
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            described.len(),
            "one sentence per condition"
        );
    }

    fn outcome(
        name: &str,
        passed: bool,
        kind: Option<CheckFailureKind>,
        output: &str,
    ) -> CheckOutcome {
        CheckOutcome {
            name: name.to_string(),
            passed,
            exit_code: if passed { Some(0) } else { Some(1) },
            failure_kind: kind,
            parsed: None,
            output_tail: output.to_string(),
            cached: false,
            recorded: None,
            duration_ms: 1,
            suppressed_after: None,
            not_recorded: None,
        }
    }

    fn status(name: &str, state: NodeCheckState) -> NodeCheckStatus {
        NodeCheckStatus {
            job_id: "job".to_string(),
            request_id: None,
            name: name.to_string(),
            state,
            policy: "advisory".to_string(),
            when: "write".to_string(),
            cached: None,
            duration_ms: Some(1234),
            ran_at: Some(1),
            passed: None,
            failed: None,
            skipped: None,
            deferred: None,
            parser: None,
            complete: None,
            selection: None,
            suite_failures: None,
            failure_names: Vec::new(),
            output_tail: None,
            failure_kind: None,
            suppressed_after: None,
            executor: None,
            verified_on: None,
            notice: None,
            declared_unverified: None,
        }
    }

    #[test]
    fn fingerprint_is_order_independent_and_state_sensitive() {
        let rust = outcome("rust", false, None, "assertion A");
        let lint = outcome("lint", true, None, "");
        let first = turn_check_fingerprint("tree", &[rust, lint]).unwrap();
        let second = turn_check_fingerprint(
            "tree",
            &[
                outcome("lint", true, None, ""),
                outcome("rust", false, None, "assertion A"),
            ],
        )
        .unwrap();
        assert_eq!(first, second);
        // Failure text is execution noise, not stable evidence about a lane state.
        assert_eq!(
            first,
            turn_check_fingerprint(
                "tree",
                &[
                    outcome("lint", true, None, ""),
                    outcome("rust", false, None, "assertion B"),
                ],
            )
            .unwrap()
        );
        assert_ne!(
            first,
            turn_check_fingerprint("tree-2", &[outcome("rust", false, None, "assertion A")])
                .unwrap()
        );
        assert_ne!(
            first,
            turn_check_fingerprint("tree", &[outcome("rust", false, None, "assertion A")]).unwrap()
        );
    }

    /// Verdict versus no verdict: an infrastructure-only lane has no actionable
    /// verdict, so it hashes as neither the green nor the red it might have been.
    #[test]
    fn fingerprint_distinguishes_green_red_and_no_verdict() {
        let green = turn_check_fingerprint("tree", &[outcome("rust", true, None, "")]).unwrap();
        let red = turn_check_fingerprint("tree", &[outcome("rust", false, None, "same")]).unwrap();
        let infrastructure = turn_check_fingerprint(
            "tree",
            &[outcome(
                "rust",
                false,
                Some(CheckFailureKind::Infrastructure),
                "same",
            )],
        )
        .unwrap();
        assert_ne!(green, red);
        assert_ne!(red, infrastructure);
    }

    #[test]
    fn fingerprint_fails_open_when_required_identity_is_ambiguous() {
        assert!(turn_check_fingerprint("", &[outcome("rust", true, None, "")]).is_none());
        assert!(turn_check_fingerprint("tree", &[]).is_none());
    }

    #[test]
    fn fingerprint_ignores_real_wave_execution_noise_on_the_same_tree() {
        // CAIRN-3515 produced these two rust-test shapes on the same tree: the
        // selected failures, progress tail, and sealed commit all changed while
        // the lane-level verdict set did not.
        let first = [
            outcome("lint", true, None, ""),
            outcome(
                "rust-tests",
                false,
                None,
                "PASS [1.147s] (1871/3246)\nproject_checkout_validation_rejects_disposable_and_missing_paths",
            ),
        ];
        let second = [
            outcome("lint", true, None, ""),
            outcome(
                "rust-tests",
                false,
                None,
                "a_full_machine_makes_a_batch_slower_not_broken\noutput_wake_fires_when_process_exits_before_phrase",
            ),
        ];
        assert_eq!(
            turn_check_fingerprint("2670caaa55", &first),
            turn_check_fingerprint("2670caaa55", &second)
        );
    }

    #[test]
    fn fingerprint_moves_when_a_lane_appears_or_disappears() {
        let rust = outcome("rust", false, None, "failure");
        let with_lint = [
            outcome("rust", false, None, "different failure"),
            outcome("lint", true, None, ""),
        ];
        assert_ne!(
            turn_check_fingerprint("tree", &[rust]),
            turn_check_fingerprint("tree", &with_lint)
        );
    }

    #[test]
    fn delivery_decision_dampens_equal_red_and_green_states() {
        assert_eq!(
            turn_check_delivery(None, Some("red"), true),
            TurnCheckDelivery::Deliver("new")
        );
        assert_eq!(
            turn_check_delivery(Some(Some("red")), Some("red"), true),
            TurnCheckDelivery::Suppress
        );
        assert_eq!(
            turn_check_delivery(Some(Some("green")), Some("green"), false),
            TurnCheckDelivery::Suppress
        );
        assert_eq!(
            turn_check_delivery(Some(Some("red")), Some("changed"), true),
            TurnCheckDelivery::Deliver("changed")
        );
        assert_eq!(
            turn_check_delivery(Some(Some("red")), None, true),
            TurnCheckDelivery::Deliver("ambiguous/fail-open")
        );
        assert_eq!(
            turn_check_delivery(None, Some("green"), false),
            TurnCheckDelivery::Deliver("green")
        );
    }

    /// The cairn/4658 line, at the seam that produced it.
    ///
    /// A turn-end wave is spawned at a turn boundary and settles whenever its
    /// slowest lane finishes -- 28 minutes later in the specimen, a cold cargo
    /// build. The whole failure-and-successor sequence happened inside that
    /// window, so by the time this function ran, the recipient's execution had
    /// failed AND been superseded AND had its successor adopt its committed
    /// head. The push went out at `Wake::Wake` and resurrected the dead builder
    /// for five more turns against the files its successor was editing.
    ///
    /// The verdict must still be RECORDED -- it stays readable at the node's
    /// `/checks` and in the attention ledger -- and must not be able to rouse.
    #[tokio::test]
    async fn a_superseded_recipients_red_verdict_is_recorded_but_cannot_rouse() {
        let db = crate::storage::migrated_test_db("turn-check-superseded.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','proj','/tmp/repo',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
               VALUES('i','p',4658,'I','active','active','none',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq, lifecycle)
               VALUES('e1','r','i','p','failed',1,1,'superseded');
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq, lifecycle)
               VALUES('e2','r','i','p','running',2,2,'canonical');
             INSERT INTO jobs(id,project_id,issue_id,execution_id,status,node_name,uri_segment,created_at,updated_at)
               VALUES('job-1','p','i','e1','cancelled','builder','builder',1,1);
             INSERT INTO jobs(id,project_id,issue_id,execution_id,status,node_name,uri_segment,created_at,updated_at)
               VALUES('job-2','p','i','e2','running','builder','builder',2,2);",
        )
        .await
        .unwrap();

        let superseded_uri = "cairn://p/proj/4658/1/builder/checks";
        let (delivery, wake) =
            persist_turn_check_delivery(&db, "job-1", superseded_uri, Some("red"), true)
                .await
                .unwrap();

        assert_eq!(
            delivery,
            TurnCheckDelivery::Deliver("new"),
            "the verdict is still delivered and readable; only its wake level changes"
        );
        assert_eq!(
            wake,
            Some(attention_push::Wake::Passive),
            "a superseded node's check verdict must not be able to wake it"
        );
        let pushes = attention_push::list_pending(&db, "job-1").await.unwrap();
        assert_eq!(
            pushes.len(),
            1,
            "the push is written, not dropped: the attention stays inspectable"
        );
        assert_eq!(pushes[0].wake, attention_push::Wake::Passive);

        // The guard that gives the assertion above teeth: the SAME call for the
        // canonical successor still rouses. Without this, a fence that
        // downgraded everything would look correct.
        let (_, wake) = persist_turn_check_delivery(
            &db,
            "job-2",
            "cairn://p/proj/4658/2/builder/checks",
            Some("red"),
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            wake,
            Some(attention_push::Wake::Wake),
            "the canonical successor's red must still rouse it"
        );
    }

    #[tokio::test]
    async fn unchanged_persistent_red_creates_one_turn_checks_wake() {
        let db = crate::storage::migrated_test_db("turn-check-delivery.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','proj','/tmp/repo',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
               VALUES('i','p',1,'I','active','active','none',1,1);
             INSERT INTO jobs(id,project_id,issue_id,status,node_name,created_at,updated_at)
               VALUES('job','p','i','complete','builder',1,1);",
        )
        .await
        .unwrap();
        let uri = "cairn://p/proj/1/1/builder/checks";
        let first = turn_check_fingerprint(
            "same-tree",
            &[outcome("rust-tests", false, None, "first flaky failure")],
        );
        let second = turn_check_fingerprint(
            "same-tree",
            &[outcome(
                "rust-tests",
                false,
                None,
                "different flaky failures and tail",
            )],
        );

        assert_eq!(
            persist_turn_check_delivery(&db, "job", uri, first.as_deref(), true)
                .await
                .unwrap(),
            (
                TurnCheckDelivery::Deliver("new"),
                Some(attention_push::Wake::Wake)
            )
        );
        assert_eq!(
            persist_turn_check_delivery(&db, "job", uri, second.as_deref(), true)
                .await
                .unwrap(),
            (TurnCheckDelivery::Suppress, None)
        );
        let pushes = attention_push::list_pending(&db, "job").await.unwrap();
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].key, format!("turn-checks:{uri}"));
        assert_eq!(pushes[0].wake, attention_push::Wake::Wake);
    }

    /// A lane Cairn declined to run, as `record_suppressed_check` builds it: the
    /// stored evidence row's kind rides along as hidden metadata, while the
    /// renderer prints the same suppression line whatever that kind is.
    fn suppressed(name: &str, stored_kind: CheckFailureKind, output: &str) -> CheckOutcome {
        CheckOutcome {
            exit_code: None,
            suppressed_after: Some(3),
            ..outcome(name, false, Some(stored_kind), output)
        }
    }

    /// cairn/4820, through the production seam. One ordinary red holds steady on
    /// one sealed tree while the machine-dependent lanes beside it cycle through
    /// every non-verdict shape a wave can hand back: a fresh substrate failure, a
    /// NoMatchingExecutor refusal, a submission-time suppression carrying any
    /// stored kind, and a lane the turn-end pre-filter dropped entirely. The agent
    /// has nothing new to act on at any step, so it must be roused exactly once.
    ///
    /// Each wake is consumed between waves, as a roused builder's turn stamps it
    /// delivered: an undelivered same-key push is superseded in place, so counting
    /// pending rows alone could never see a second wake.
    #[tokio::test]
    async fn infrastructure_churn_beside_a_fixed_genuine_red_wakes_once() {
        use CheckFailureKind::{Capacity, Infrastructure, SpawnError};

        let db = crate::storage::migrated_test_db("turn-check-infra-churn.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','proj','/tmp/repo',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
               VALUES('i','p',4812,'I','active','active','none',1,1);
             INSERT INTO jobs(id,project_id,issue_id,status,node_name,created_at,updated_at)
               VALUES('job','p','i','complete','builder',1,1);",
        )
        .await
        .unwrap();
        let uri = "cairn://p/proj/4812/1/builder/checks";
        let key = format!("turn-checks:{uri}");
        let tree = "tree-4812";

        let red = |tail: &str| outcome("rust-tests", false, None, tail);
        let lint = || outcome("lint", true, None, "");
        let windows = "executor-windows-check";
        let frontend = "frontend-tests";

        // The 11:54 and 11:56 specimen: byte-identical briefings, different
        // hidden metadata on the suppressed lanes.
        let at_1154 = vec![
            red("6 of 12 failed"),
            lint(),
            suppressed(windows, Capacity, "no machine"),
            suppressed(frontend, Infrastructure, "sccache died"),
        ];
        let at_1156 = vec![
            red("6 of 12 failed"),
            lint(),
            suppressed(windows, Infrastructure, "no machine"),
            suppressed(frontend, SpawnError, "sccache died"),
        ];
        assert_eq!(
            crate::execution::checks::format_check_summary(&at_1154),
            crate::execution::checks::format_check_summary(&at_1156),
            "the specimen pair must render the same briefing to the agent"
        );
        assert_ne!(at_1154[2].failure_kind, at_1156[2].failure_kind);
        assert_ne!(at_1154[3].failure_kind, at_1156[3].failure_kind);

        let steps: Vec<(&str, Vec<CheckOutcome>)> = vec![
            (
                "1 fresh infrastructure + capacity",
                vec![
                    red("tail one"),
                    lint(),
                    outcome(windows, false, Some(Infrastructure), "toolchain failure"),
                    outcome(frontend, false, Some(Capacity), "no room"),
                ],
            ),
            (
                "2 NoMatchingExecutor",
                vec![
                    red("tail two"),
                    lint(),
                    outcome(
                        windows,
                        false,
                        Some(Infrastructure),
                        "No enrolled machine on windows exists",
                    ),
                    outcome(frontend, false, Some(Capacity), "no room"),
                ],
            ),
            (
                "3 windows suppressed (stored capacity)",
                vec![
                    red("tail three"),
                    lint(),
                    suppressed(windows, Capacity, "no machine"),
                    outcome(frontend, false, Some(Infrastructure), "sccache died"),
                ],
            ),
            (
                "4 windows dropped by pre-filter, frontend suppressed",
                vec![
                    red("tail four"),
                    lint(),
                    suppressed(frontend, Infrastructure, "sccache died"),
                ],
            ),
            (
                "5 windows infrastructure again",
                vec![
                    red("tail five"),
                    lint(),
                    outcome(windows, false, Some(Infrastructure), "toolchain failure"),
                    suppressed(frontend, Infrastructure, "sccache died"),
                ],
            ),
            ("6 the 11:54 set", at_1154),
            ("7 the 11:56 set", at_1156),
            (
                "8 lint genuinely breaks",
                vec![
                    red("tail eight"),
                    outcome("lint", false, None, "clippy: unused import"),
                    outcome(windows, false, Some(Capacity), "no room"),
                    suppressed(frontend, SpawnError, "sccache died"),
                ],
            ),
        ];

        let mut decisions = Vec::new();
        let mut consumed_wakes = Vec::new();
        for (label, outcomes) in &steps {
            let delivered = deliver_turn_check_outcomes(&db, "job", uri, tree, outcomes)
                .await
                .unwrap();
            assert!(delivered.any_genuine_failed, "{label}: rust-tests is red");
            decisions.push(format!(
                "{label}: {:?} / {:?}",
                delivered.delivery, delivered.wake
            ));
            for push in attention_push::list_pending(&db, "job").await.unwrap() {
                if push.key == key && push.wake == attention_push::Wake::Wake {
                    consumed_wakes.push(label.to_string());
                }
                attention_push::stamp_delivered(
                    &db,
                    std::slice::from_ref(&push.id),
                    &format!("turn-{label}"),
                )
                .await
                .unwrap();
            }
        }
        let trace = decisions.join("\n");

        assert_eq!(
            consumed_wakes,
            vec![steps[0].0.to_string(), steps[7].0.to_string()],
            "infrastructure churn beside a fixed genuine red must wake once, and only a \
             genuine flip may wake again; per-step decisions:\n{trace}"
        );
        for (index, decision) in decisions.iter().enumerate().take(7).skip(1) {
            assert!(
                decision.ends_with("Suppress / None"),
                "step {} must be dampened; per-step decisions:\n{trace}",
                index + 1
            );
        }
        assert!(
            decisions[7].ends_with("Deliver(\"changed\") / Some(Wake)"),
            "a genuine flip must still wake; per-step decisions:\n{trace}"
        );
    }

    use crate::execution::checks_status::WaveCadence;

    #[test]
    fn section_is_none_when_no_configured_checks() {
        assert!(format_checks_section(&[], WaveCadence::default()).is_none());
    }

    /// A retired node's section names how it retired and the head its lanes
    /// were recorded for, and never reads as results still to come.
    #[test]
    fn retired_section_names_its_head_and_never_reads_as_awaiting_results() {
        use crate::execution::checks_status::RetiredNode;
        let plan = WavePlan {
            head: "0123456789abcdef".to_string(),
            base: "base".to_string(),
            tree_hash: "tree".to_string(),
            checks: Default::default(),
            applicable_names: Default::default(),
            input_hashes: Default::default(),
            unsupported: Default::default(),
        };
        let merged = RetiredNode {
            outcome: "merged".to_string(),
            plan: Some(std::sync::Arc::new(plan)),
        };
        let lanes = [
            status("rust", NodeCheckState::Passed),
            status("lint", NodeCheckState::Pending),
        ];
        let s = format_retired_checks_section(&merged, Some(&lanes));
        assert!(s.contains("This node was merged"), "{s}");
        assert!(s.contains("`0123456789ab`"), "{s}");
        assert!(s.contains("\u{2713} rust"), "{s}");
        assert!(s.contains("- lint: no verdict"), "{s}");
        assert!(!s.contains("pending"), "{s}");

        let unrecorded = RetiredNode {
            outcome: "closed".to_string(),
            plan: None,
        };
        let s = format_retired_checks_section(&unrecorded, None);
        assert!(
            s.contains("closed before any review wave recorded a plan"),
            "{s}"
        );
    }

    #[test]
    fn section_renders_running_with_log_tail() {
        let mut running = status("rust", NodeCheckState::Running);
        running.output_tail = Some("compiling...\nrunning tests".to_string());
        let s = format_checks_section(&[running], WaveCadence::writing_for_test()).unwrap();
        assert!(s.contains("### Systematic checks"));
        assert!(s.contains("rust: _running\u{2026}_"));
        assert!(s.contains("running tests"));
    }

    #[test]
    fn section_renders_running_without_a_log_yet() {
        let s = format_checks_section(
            &[status("rust", NodeCheckState::Running)],
            WaveCadence::writing_for_test(),
        )
        .unwrap();
        assert!(s.contains("_running\u{2026}_"));
        assert!(!s.contains("```"));
    }

    #[test]
    fn section_renders_cached_verdicts_and_inlines_failure_output() {
        // Tallies are test counts only when a test runner produced them, as
        // `status_from_row` always records alongside them.
        let mut passed = status("rust", NodeCheckState::Passed);
        passed.parser = Some("nextest".to_string());
        passed.passed = Some(12);
        passed.failed = Some(0);
        let mut failed = status("frontend", NodeCheckState::Failed);
        failed.failed = Some(2);
        failed.passed = Some(38);
        failed.output_tail = Some("assertion failed: left == right".to_string());
        let s = format_checks_section(&[passed, failed], WaveCadence::default()).unwrap();
        assert!(s.contains("\u{2713} rust (12 tests)"));
        assert!(s.contains("\u{2717} frontend \u{2014} 2 of 40 failed"));
        assert!(s.contains("assertion failed: left == right"));
    }

    #[test]
    fn section_names_a_suite_collection_failure_instead_of_a_zero_tally() {
        // The durable "Systematic checks" surface reads from the cache, so it is
        // where a dropped suite count would resurrect "0 of 881 failed" long
        // after the run that produced it.
        let mut failed = status("frontend-partial", NodeCheckState::Failed);
        failed.passed = Some(881);
        failed.failed = Some(0);
        failed.suite_failures = Some(1);
        failed.failure_names = vec!["src/components/FileTabView.test.tsx".to_string()];
        failed.output_tail = Some(
            "src/components/FileTabView.test.tsx: Cannot find module './readableMarkdown'"
                .to_string(),
        );
        let s = format_checks_section(&[failed], WaveCadence::default()).unwrap();
        assert!(
            s.contains("\u{2717} frontend-partial \u{2014} 1 suite failed to load"),
            "got: {s}"
        );
        assert!(!s.contains("0 of 881 failed"), "got: {s}");
        // The file that failed to load is still named, in the detail block.
        assert!(
            s.contains("src/components/FileTabView.test.tsx"),
            "got: {s}"
        );
    }

    #[test]
    fn section_counts_failing_tests_and_uncollected_suites_separately() {
        let mut failed = status("frontend-partial", NodeCheckState::Failed);
        failed.passed = Some(38);
        failed.failed = Some(2);
        failed.suite_failures = Some(3);
        let s = format_checks_section(&[failed], WaveCadence::default()).unwrap();
        assert!(
            s.contains(
                "\u{2717} frontend-partial \u{2014} 2 of 40 failed, 3 suites failed to load"
            ),
            "got: {s}"
        );
    }

    /// A rowless lane is `pending` only while a wave of its own cadence is in
    /// flight; with none, nothing is coming and it reads `absent`, the word the
    /// merge's verdict gate refuses with.
    #[test]
    fn section_renders_not_run_states() {
        let lanes = [
            status("docs", NodeCheckState::NotApplicable),
            status("lint", NodeCheckState::Pending),
        ];
        let s = format_checks_section(&lanes, WaveCadence::writing_for_test()).unwrap();
        assert!(s.contains("docs: not applicable"));
        assert!(s.contains("lint: pending"), "{s}");
        for idle in [WaveCadence::default(), WaveCadence::reviewing_for_test()] {
            let s = format_checks_section(&lanes, idle).unwrap();
            assert!(
                s.contains("lint: absent \u{2014} no lane has run it on this head"),
                "{s}"
            );
            assert!(!s.contains("pending"), "{s}");
        }
    }

    /// A run that ended without a verdict is not a red, and a tripped breaker
    /// says so rather than replaying the last failure as one.
    #[test]
    fn section_renders_died_and_breaker_lanes_apart_from_reds() {
        let mut died = status("external-host", NodeCheckState::Failed);
        died.failure_kind = Some("infrastructure".to_string());
        died.output_tail = Some("could not materialize commit".to_string());
        let mut breaker = status("rust-lint", NodeCheckState::Failed);
        breaker.failure_kind = Some("infrastructure".to_string());
        breaker.suppressed_after = Some(3);
        let s = format_checks_section(&[died, breaker], WaveCadence::default()).unwrap();
        assert!(
            s.contains("\u{2298} external-host: died without a verdict"),
            "{s}"
        );
        assert!(s.contains("could not materialize commit"), "{s}");
        assert!(
            s.contains("\u{2298} rust-lint: breaker \u{2014} not run after 3"),
            "{s}"
        );
        assert!(!s.contains('\u{2717}'), "{s}");
    }

    /// A lint whose cone holds no crate the change touched had nothing to
    /// judge: it reads as not applicable by content, never as a green or as an
    /// unfinished run.
    #[test]
    fn section_renders_a_lint_with_no_selectable_work_as_not_applicable() {
        let mut lint = status("rust-lint", NodeCheckState::Unverified);
        lint.parser = Some("rust-lint".to_string());
        lint.selection = Some("empty".to_string());
        lint.complete = Some(false);
        let s = format_checks_section(&[lint], WaveCadence::default()).unwrap();
        assert!(
            s.contains("- rust-lint: not applicable: no selectable work for this diff"),
            "{s}"
        );
        assert!(!s.contains('\u{2713}') && !s.contains('\u{26a0}'), "{s}");
    }

    #[test]
    fn empty_log_file_exists_but_yields_no_tail() {
        // A running-but-silent check: the file exists (started) but its tail is
        // None until it emits. The status model must key RUNNING off existence,
        // not off a non-empty tail, or a quiet check looks queued.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("cairn-checks-tail-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("frontend-build.log");

        std::fs::write(&path, b"").unwrap();
        assert!(path.exists());
        assert_eq!(read_log_tail(&path, LOG_TAIL_CHARS), None);

        std::fs::write(&path, b"compiling...\n").unwrap();
        assert_eq!(
            read_log_tail(&path, LOG_TAIL_CHARS).as_deref(),
            Some("compiling...")
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn large_log_tail_is_bounded_and_fast() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "cairn-checks-tail-large-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("rust-full.log");
        let mut file = std::fs::File::create(&path).unwrap();
        let chunk = vec![b'x'; 1024 * 1024];
        for _ in 0..16 {
            std::io::Write::write_all(&mut file, &chunk).unwrap();
        }
        std::io::Write::write_all(&mut file, b"\nfinal cargo line\n").unwrap();
        drop(file);

        let started = std::time::Instant::now();
        let output = read_log_tail(&path, LOG_TAIL_CHARS).unwrap();
        let elapsed = started.elapsed();

        assert!(output.ends_with("final cargo line"));
        assert_eq!(output.chars().count(), LOG_TAIL_CHARS);
        assert!(
            elapsed < std::time::Duration::from_millis(50),
            "16 MiB log tail took {elapsed:?}; expected a bounded low-tens-of-ms read"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn log_tail_preserves_multibyte_utf8_boundary() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "cairn-checks-tail-utf8-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("frontend.log");
        std::fs::write(&path, format!("{}DONE\n", "é".repeat(3_000))).unwrap();

        let output = read_log_tail(&path, 8).unwrap();
        assert_eq!(output, "ééééDONE");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sanitize_log_name_slugs_unsafe_chars() {
        assert_eq!(sanitize_log_name("frontend-build"), "frontend-build");
        assert_eq!(sanitize_log_name("rust_full.v2"), "rust_full.v2");
        assert_eq!(sanitize_log_name("weird/name space"), "weird_name_space");
    }

    #[test]
    fn tail_keeps_last_chars_on_boundary() {
        assert_eq!(tail("abcdef", 3), "def");
        assert_eq!(tail("abc", 10), "abc");
    }

    #[test]
    fn checks_uri_uses_canonical_job_shape() {
        let mut coords = JobCoords {
            project_id: "p".into(),
            repository_path: "/repo".into(),
            branch: "agent/test".into(),
            base_branch: None,
            project_key: "cairn".into(),
            number: 42,
            exec_seq: 2,
            node_segment: "review-rust".into(),
            parent_segment: Some("builder".into()),
            is_workflow: false,
        };
        assert_eq!(
            checks_uri_for_job(&coords),
            "cairn://p/cairn/42/2/builder/task/review-rust/checks"
        );

        coords.node_segment = "workflow".into();
        coords.parent_segment = Some("coordinator".into());
        coords.is_workflow = true;
        assert_eq!(
            checks_uri_for_job(&coords),
            "cairn://p/cairn/42/2/workflow/checks"
        );

        coords.node_segment = "builder".into();
        coords.parent_segment = None;
        coords.is_workflow = false;
        assert_eq!(
            checks_uri_for_job(&coords),
            "cairn://p/cairn/42/2/builder/checks"
        );
    }

    /// The zero-delta gate resolves a branch NAME, never a recorded commit.
    ///
    /// `jobs.base_commit` is the row that goes stale when the base-advance
    /// fanout loses its compare-and-swap race, and gating a rebased zero-delta
    /// branch on a stale coordinate is what fired full review suites on planner
    /// nodes (CAIRN-3108). `JobCoords` no longer carries that row at all, so
    /// this pins the only remaining input: a name the store resolves fresh on
    /// every evaluation.
    #[test]
    fn the_zero_delta_gate_resolves_a_branch_name_not_a_recorded_commit() {
        let mut coords = JobCoords {
            project_id: "p".into(),
            repository_path: "/repo".into(),
            branch: "agent/test".into(),
            base_branch: Some("main".into()),
            project_key: "cairn".into(),
            number: 42,
            exec_seq: 1,
            node_segment: "planner".into(),
            parent_segment: None,
            is_workflow: false,
        };
        assert_eq!(base_tree_coordinate(&coords), Some("main"));

        // No integration target is no gate: the suite runs rather than skipping
        // on a coordinate nobody resolved.
        coords.base_branch = None;
        assert_eq!(base_tree_coordinate(&coords), None);
    }

    fn uncached_plan(name: &str) -> (CheckPlan, String) {
        (
            CheckPlan {
                name: name.to_string(),
                applies: true,
                command: format!("bun run {name}"),
                scope: crate::execution::selection::CheckScope::Full,
                resource_class: crate::config::project_settings::CheckResourceClass::Shared,
                verdict_environment_names: Vec::new(),
                verdict_executor_ids: None,
                config_error: None,
                verdict_platforms: Vec::new(),
            },
            format!("input-{name}"),
        )
    }

    /// The regression this exists for. A planner node's branch is rebased onto a
    /// new base, so its tree is byte-identical to that base's, but its recorded
    /// `base_commit` still points at the old one. The changed-file gate upstream
    /// therefore selects real, uncached review checks for a node that changed
    /// nothing (job 7d9755b2: seven files, three of them Rust). Nothing may be
    /// submitted for it.
    ///
    /// Note the shape of the assertion: the plan set is NOT empty. A gate that
    /// only refuses to launch when nothing is left uncached is exactly the bug,
    /// and would pass a test that fed it an empty plan set.
    #[test]
    fn a_tree_identical_to_its_base_launches_nothing_even_with_uncached_checks() {
        let uncached = vec![
            uncached_plan("rust-full"),
            uncached_plan("rust-lint"),
            uncached_plan("rust-windows-executor-stack"),
        ];

        let decision = turn_end_launch(uncached.clone(), true);

        match decision {
            TurnEndLaunch::Skip(TurnEndSkip::TreeMatchesBase(names)) => assert_eq!(
                names,
                vec!["rust-full", "rust-lint", "rust-windows-executor-stack"],
                "the skip names what it declined to run, for the operator's log"
            ),
            other => panic!("a zero-delta node must take no admission at all, got {other:?}"),
        }

        // Control: the same uncached plans on a node whose tree genuinely differs
        // still launch, unchanged. Without this, a gate that skipped everything
        // would also pass.
        assert_eq!(
            turn_end_launch(uncached.clone(), false),
            TurnEndLaunch::Launch(uncached),
            "a node that actually changed something still runs its checks"
        );
    }

    #[test]
    fn a_fully_cached_tree_launches_nothing() {
        assert_eq!(
            turn_end_launch(Vec::new(), false),
            TurnEndLaunch::Skip(TurnEndSkip::FullyCached)
        );
    }

    #[test]
    fn green_rides_along_passively_red_wakes() {
        assert_eq!(delivery_wake(false), attention_push::Wake::Passive);
        assert_eq!(delivery_wake(true), attention_push::Wake::Wake);
    }

    /// The wake predicate. Only a red the agent's own change owns counts; an
    /// infrastructure failure and a suppression are both facts about Cairn.
    #[test]
    fn only_a_verdict_about_the_change_is_a_genuine_failure() {
        assert!(outcome("rust", false, None, "assertion failed").is_genuine_failure());
        assert!(outcome("rust", false, Some(CheckFailureKind::Killed), "oom").is_genuine_failure());
        assert!(!outcome("rust", true, None, "").is_genuine_failure());
        for kind in [
            CheckFailureKind::Infrastructure,
            CheckFailureKind::TimedOut,
            CheckFailureKind::SpawnError,
            CheckFailureKind::RunnerError,
            CheckFailureKind::TimedOut,
            CheckFailureKind::Capacity,
        ] {
            assert!(
                !outcome("rust", false, Some(kind), "sccache died").is_genuine_failure(),
                "{kind:?} is a failure inside Cairn, not a verdict"
            );
        }

        let mut suppressed = outcome("rust", false, Some(CheckFailureKind::Infrastructure), "");
        suppressed.suppressed_after = Some(3);
        assert!(!suppressed.is_genuine_failure());
    }

    /// The delivery decision an infrastructure-only suite reaches. The suite IS
    /// red — `any_failed` is true — and it still must not wake anyone.
    #[test]
    fn an_infrastructure_only_suite_is_red_yet_delivers_passively() {
        let outcomes = [
            outcome("lint", true, None, ""),
            outcome(
                "rust-full",
                false,
                Some(CheckFailureKind::Infrastructure),
                "sccache: server startup failed",
            ),
        ];
        assert!(outcomes.iter().any(|o| !o.passed));
        assert!(!outcomes.iter().any(CheckOutcome::is_genuine_failure));
        assert_eq!(
            delivery_wake(outcomes.iter().any(CheckOutcome::is_genuine_failure)),
            attention_push::Wake::Passive
        );
    }

    /// The wake-loop generator, pinned. Two runs of the same broken substrate
    /// produce different diagnostic text — a different pid, a different crate —
    /// and the delivery fingerprint must NOT move, or the dampener never fires
    /// and every re-execution rouses the author again (CAIRN-3245).
    #[test]
    fn infrastructure_flake_text_never_moves_the_fingerprint() {
        let first = turn_check_fingerprint(
            "tree",
            &[outcome(
                "rust-full",
                false,
                Some(CheckFailureKind::Infrastructure),
                "sccache: failed to connect to server at 127.0.0.1:4226 (pid 51201)",
            )],
        )
        .unwrap();
        let second = turn_check_fingerprint(
            "tree",
            &[outcome(
                "rust-full",
                false,
                Some(CheckFailureKind::Infrastructure),
                "sccache: failed to connect to server at 127.0.0.1:4226 (pid 88317)",
            )],
        )
        .unwrap();
        assert_eq!(
            first, second,
            "an infrastructure outcome contributes nothing to identity, least of all its text"
        );

        // The lane stayed ordinarily red, so different failure text is not a
        // delivery-state change.
        assert_eq!(
            turn_check_fingerprint("tree", &[outcome("rust", false, None, "failure A")]).unwrap(),
            turn_check_fingerprint("tree", &[outcome("rust", false, None, "failure B")]).unwrap(),
        );
    }

    /// The state an infrastructure failure DOES have to distinguish: whether the
    /// genuine verdicts beside it changed. A suite that is green-plus-infra must
    /// not hash the same as one that went genuinely red.
    #[test]
    fn a_genuine_verdict_beside_an_infrastructure_failure_still_moves_the_fingerprint() {
        let infra_only = turn_check_fingerprint(
            "tree",
            &[
                outcome("lint", true, None, ""),
                outcome(
                    "rust-full",
                    false,
                    Some(CheckFailureKind::Infrastructure),
                    "sccache died",
                ),
            ],
        )
        .unwrap();
        let now_genuinely_red = turn_check_fingerprint(
            "tree",
            &[
                outcome("lint", false, None, "clippy: unused import"),
                outcome(
                    "rust-full",
                    false,
                    Some(CheckFailureKind::Infrastructure),
                    "sccache died differently",
                ),
            ],
        )
        .unwrap();
        assert_ne!(infra_only, now_genuinely_red);
    }

    /// cairn/4820, as a pure function: every shape a non-verdict lane arrives in
    /// beside a fixed genuine red and pass hashes to one identity.
    #[test]
    fn a_fixed_genuine_red_hides_every_non_verdict_shape() {
        let with = |lane: Option<CheckOutcome>| {
            let mut outcomes = vec![
                outcome("rust-tests", false, None, "6 of 12 failed"),
                outcome("lint", true, None, ""),
            ];
            outcomes.extend(lane);
            turn_check_fingerprint("tree", &outcomes).unwrap()
        };
        let absent = with(None);
        let non_verdict_kinds = [
            CheckFailureKind::Infrastructure,
            CheckFailureKind::Capacity,
            CheckFailureKind::TimedOut,
            CheckFailureKind::RunnerError,
            CheckFailureKind::SpawnError,
        ];
        for kind in non_verdict_kinds {
            assert_eq!(
                with(Some(outcome("windows", false, Some(kind), "died"))),
                absent,
                "a fresh {kind:?} lane must not move identity"
            );
            assert_eq!(
                with(Some(suppressed("windows", kind, "died"))),
                absent,
                "a lane suppressed with stored kind {kind:?} must not move identity"
            );
        }
        // A suppression carries no verdict whatever its stored row claimed.
        assert_eq!(
            with(Some(CheckOutcome {
                suppressed_after: Some(3),
                ..outcome("windows", false, None, "")
            })),
            absent
        );
    }

    #[test]
    fn genuine_lane_appearance_and_flip_move_identity() {
        let red = || outcome("rust", false, None, "assertion A");
        let fp = |outcomes: &[CheckOutcome]| turn_check_fingerprint("tree", outcomes).unwrap();

        // A lane recovering from infrastructure to a real pass is a new verdict.
        assert_ne!(
            fp(&[
                red(),
                outcome("lint", false, Some(CheckFailureKind::Infrastructure), ""),
            ]),
            fp(&[red(), outcome("lint", true, None, "")])
        );
        // Pass to ordinary fail moves it.
        assert_ne!(
            fp(&[red(), outcome("lint", true, None, "")]),
            fp(&[red(), outcome("lint", false, None, "unused import")])
        );
        // A genuine failure's kind is not identity: killed and ordinary are one red.
        assert_eq!(
            fp(&[red()]),
            fp(&[outcome(
                "rust",
                false,
                Some(CheckFailureKind::Killed),
                "OOM"
            )])
        );
        // Nor is its text.
        assert_eq!(
            fp(&[red()]),
            fp(&[outcome("rust", false, None, "assertion B")])
        );
    }

    /// The parent's infrastructure signal hears about a lane that ran into the
    /// substrate on this wave, and about neither suppression shape, so it no
    /// longer depends on which gate did the suppressing.
    #[test]
    fn only_a_fresh_substrate_failure_is_parent_infrastructure_news() {
        let red = || outcome("rust-tests", false, None, "6 of 12 failed");
        assert!(reports_fresh_infrastructure(&[
            red(),
            outcome(
                "windows",
                false,
                Some(CheckFailureKind::Infrastructure),
                "no machine"
            ),
        ]));
        assert!(reports_fresh_infrastructure(&[outcome(
            "windows",
            false,
            Some(CheckFailureKind::Capacity),
            "no room"
        )]));
        // Suppressed at submission: present, carrying the stored infrastructure kind.
        assert!(!reports_fresh_infrastructure(&[
            red(),
            suppressed("windows", CheckFailureKind::Infrastructure, "no machine"),
        ]));
        // Suppressed by the pre-filter: absent.
        assert!(!reports_fresh_infrastructure(&[red()]));
        // A genuine failure is the author's news, not the operator's.
        assert!(!reports_fresh_infrastructure(&[outcome(
            "rust",
            false,
            Some(CheckFailureKind::Killed),
            "OOM"
        )]));
    }

    /// An all-non-verdict suite must keep an identity: `None` is the fail-open
    /// answer and would deliver every wave, defeating its passive dampening.
    #[test]
    fn an_all_non_verdict_suite_still_has_an_identity() {
        let infrastructure = [outcome(
            "windows",
            false,
            Some(CheckFailureKind::Infrastructure),
            "no machine",
        )];
        let capacity = [suppressed("windows", CheckFailureKind::Capacity, "no room")];
        let first = turn_check_fingerprint("tree", &infrastructure);
        assert!(first.is_some());
        assert_eq!(first, turn_check_fingerprint("tree", &capacity));
        assert_ne!(first, turn_check_fingerprint("tree-2", &capacity));
        assert_eq!(
            delivery_wake(infrastructure.iter().any(CheckOutcome::is_genuine_failure)),
            attention_push::Wake::Passive
        );
    }

    #[test]
    fn short_id_never_panics_on_a_short_string() {
        assert_eq!(short_id("abcd"), "abcd");
        assert_eq!(short_id("0123456789"), "01234567");
    }
    #[test]
    fn turn_end_dispositions_persist_distinct_terminal_states_and_reasons() {
        let cases = [
            (
                TurnEndDisposition::Skipped("nothing applies"),
                "skipped",
                "nothing applies",
            ),
            (
                TurnEndDisposition::Withdrawn("issue resolved".into()),
                "withdrawn",
                "issue resolved",
            ),
            (
                TurnEndDisposition::CacheCovered("cached"),
                "cache_covered",
                "cached",
            ),
            (
                TurnEndDisposition::Cancelled("cancelled".into()),
                "cancelled",
                "cancelled",
            ),
            (
                TurnEndDisposition::CompletedWithDelivery,
                "completed",
                "check wave completed and delivery was persisted",
            ),
        ];
        for (disposition, expected_state, expected_reason) in cases {
            let (state, reason) = disposition.terminal();
            assert_eq!(state, expected_state);
            assert_eq!(reason, expected_reason);
        }
    }
}
