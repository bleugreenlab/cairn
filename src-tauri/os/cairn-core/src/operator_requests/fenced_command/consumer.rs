//! The `fenced_command` consumer. Dispatch hands the frozen act to the fleet's
//! operator-act path on a task of its own, because the request outbox delivers
//! one intent at a time and a command may run for hours. The attempt settles
//! once, from the cell outcome, through the primitive's checked report.
//!
//! An attempt this process is not running has no live observer: a runner that
//! restarted mid-act cannot learn what the command did. Reconciliation settles
//! such a claim `Inconclusive` and never relaunches it, because the command may
//! have run, so running it again needs another acceptance.
//!
//! A project-declared `lands` act ([`super::declared`]) goes further, in
//! [`perform_lands`]: adopt the act's commit if the default branch already has
//! it; otherwise recheck the starting version, run the command at the tip with
//! its tracked changes returned, seal them as one commit on
//! `request/<attempt>` whose only parent is that tip, recheck the acceptance,
//! and land exactly that commit through the landing core. A tip that moved
//! first discards the sealed commit and runs the command again on the new tip,
//! at most [`MAX_LANDS_RUNS`] times in all. The report carries the landed
//! commit as `built_commit`. Its recovery adopts a commit that landed, waits
//! while landing recovery still owns an unsettled publication, and otherwise
//! discards the sealed branch.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cairn_common::executor_protocol::{CellOutcome, CellRequest, MutationPolicy, NodeScratchGrant};
use cairn_common::requests::{
    FencedCommandAcceptance, FencedCommandAct, FencedCommandExecutorReport, FencedCommandReport,
    RequestAcceptance, RequestAttemptOutcome, RequestKind,
};
use cairn_db::storage::requests as store;

use super::declared::{self, Standing};
use super::{OperatorAct, ACTOR, KIND};
use crate::node_scratch::NodeScratch;
use crate::operator_requests::registry::{
    ActorFacts, ConsumerReport, ExecutorTarget, RequestConsumer,
};
use crate::operator_requests::{self, AcceptedRequestContext};
use crate::orchestrator::Orchestrator;
use crate::pr_data::actions::{
    discard_request_branch, land_request_commit, retire_request_branch, RequestLandingError,
};

const RECOVERY_INTERVAL: Duration = Duration::from_secs(60);

/// Runs of a `lands` act's command within one attempt, the first included.
/// The operator accepted a command and a starting version, not one base
/// commit, so a run on a moved tip is inside the acceptance; a command that
/// cannot finish before the branch moves this many times is refused.
pub(crate) const MAX_LANDS_RUNS: u32 = 3;

pub(crate) struct FencedCommandConsumer {
    /// Held by the primitive from before the DB claim until dispatch returns,
    /// and by every reconciliation pass, so a pass never sees a claim whose
    /// act has not yet been recorded in `running`.
    gate: Arc<tokio::sync::Mutex<()>>,
    /// Attempts whose act this process is still awaiting.
    running: Arc<Mutex<HashSet<String>>>,
}

/// Register the consumer and start its reconciler. Call before the request
/// delivery worker, so no accepted act is dispatched to an unregistered kind.
pub(crate) fn install(orch: &Orchestrator) -> Result<(), String> {
    let consumer = FencedCommandConsumer::new();
    orch.operator_requests.register(consumer.clone())?;
    let orch = orch.clone();
    cairn_common::task::spawn(async move {
        let mut interval = tokio::time::interval(RECOVERY_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if let Err(error) = consumer.recover(&orch).await {
                tracing::warn!(%error, "fenced_command reconciliation retained unresolved claims");
            }
        }
    });
    Ok(())
}

/// The receipt and the cell request share one name, so the cell an attempt
/// ran is findable from the request's report and back.
fn receipt_id(attempt_id: &str) -> String {
    format!("{KIND}:{attempt_id}")
}

fn accepted(acceptance: &RequestAcceptance) -> Result<&FencedCommandAcceptance, String> {
    if acceptance.kind != RequestKind::FencedCommand || acceptance.actor_id != ACTOR {
        return Err("wrong physical actor or kind for fenced_command".into());
    }
    let accepted = acceptance
        .fenced_command
        .as_ref()
        .ok_or("fenced_command acceptance has no frozen act and executor witness")?;
    if accepted.act != super::decode_act(acceptance.target.as_deref())? {
        return Err("fenced_command acceptance names an act other than the request's".into());
    }
    Ok(accepted)
}

/// Whether the act is a declared command that lands a commit.
fn lands(act: &FencedCommandAct) -> bool {
    act.declaration
        .as_ref()
        .is_some_and(|declared| declared.lands.is_some())
}

/// What a cell outcome says about the command, reduced to what the report
/// needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Observed {
    Ran {
        exit_code: Option<i32>,
        timed_out: bool,
        output: String,
        duration_ms: Option<u64>,
        device_id: String,
        connection_generation: u64,
    },
    /// Positive knowledge that no process started.
    NotRun(String),
    /// The command may have started; what it did is unknown.
    Unknown(String),
}

fn observe(outcome: &CellOutcome) -> Observed {
    match outcome {
        CellOutcome::Completed {
            exit_code,
            output,
            timed_out,
            metadata,
            ..
        } => Observed::Ran {
            exit_code: *exit_code,
            timed_out: *timed_out,
            output: output.clone(),
            duration_ms: metadata.duration_ms,
            device_id: metadata.executor_device_id.clone(),
            connection_generation: metadata.executor_connection_generation,
        },
        CellOutcome::Unavailable { reason, diagnostic } => {
            Observed::NotRun(format!("{reason:?}: {diagnostic}"))
        }
        CellOutcome::FailedAfterExecution { diagnostic, .. } => Observed::Unknown(format!(
            "the executor failed after the command started: {diagnostic}"
        )),
        CellOutcome::StorageFailure {
            stage, diagnostic, ..
        } => Observed::Unknown(format!(
            "the cell's storage failed ({stage:?}): {diagnostic}"
        )),
        CellOutcome::Cancelled { .. } => {
            Observed::Unknown("the cell was cancelled; the command may have started".into())
        }
    }
}

/// The last `MAX_RUN_RESULT_BYTES` of the output, as a run result carries it.
fn output_tail(output: &str) -> String {
    let cap = crate::mcp::handlers::run::MAX_RUN_RESULT_BYTES;
    if output.len() <= cap {
        return output.to_string();
    }
    let mut start = output.len() - cap;
    while !output.is_char_boundary(start) {
        start += 1;
    }
    format!("[{start} earlier bytes omitted]\n{}", &output[start..])
}

fn empty_report(attempt_id: &str) -> FencedCommandReport {
    FencedCommandReport {
        ran: false,
        exit_code: None,
        timed_out: false,
        duration_ms: None,
        output: String::new(),
        executor: None,
        cell_request_id: receipt_id(attempt_id),
        cell_attempt_id: attempt_id.to_string(),
        detail: None,
        landed_commit: None,
        sealed_commit: None,
        base: None,
    }
}

/// Record what the cell outcome says in `report`, adding its notes, and
/// return the outcome a raw act reports for it.
fn apply_observed(
    report: &mut FencedCommandReport,
    observed: Observed,
    accepted: &FencedCommandAcceptance,
    name: &str,
    notes: &mut Vec<String>,
) -> RequestAttemptOutcome {
    match observed {
        Observed::Ran {
            exit_code,
            timed_out,
            output,
            duration_ms,
            device_id,
            connection_generation,
        } => {
            // A pinned request cannot land on another machine id, but a
            // re-enrolled machine keeps its id; a different device is not the
            // machine the operator accepted, so success is not claimed.
            let retargeted = !device_id.is_empty() && device_id != accepted.act.device_id;
            if retargeted {
                notes.push(format!(
                    "the command ran on device {device_id}, not the accepted {}",
                    accepted.act.device_id
                ));
            }
            if connection_generation != accepted.witness.connection_generation {
                notes.push(format!(
                    "{name} reconnected between acceptance and the run (connection generation {} to {connection_generation})",
                    accepted.witness.connection_generation
                ));
            }
            report.ran = true;
            report.exit_code = exit_code;
            report.timed_out = timed_out;
            report.duration_ms = duration_ms;
            report.output = output_tail(&output);
            report.executor = Some(FencedCommandExecutorReport {
                executor_id: accepted.act.executor_id.clone(),
                device_id: if device_id.is_empty() {
                    accepted.act.device_id.clone()
                } else {
                    device_id
                },
                name: name.to_string(),
                executor_build_id: accepted.witness.executor_build_id.clone(),
                connection_generation,
                hostname: None,
            });
            // A non-zero exit or a timeout is still the act performed: the
            // command ran and failed.
            if retargeted {
                RequestAttemptOutcome::Inconclusive
            } else {
                RequestAttemptOutcome::Done
            }
        }
        Observed::NotRun(detail) => {
            notes.push(format!("nothing ran: {detail}"));
            RequestAttemptOutcome::Failed
        }
        Observed::Unknown(detail) => {
            notes.push(detail);
            RequestAttemptOutcome::Inconclusive
        }
    }
}

fn document(report: &FencedCommandReport, notes: Vec<String>) -> serde_json::Value {
    let mut report = report.clone();
    report.detail = (!notes.is_empty()).then(|| notes.join("; "));
    serde_json::to_value(&report).unwrap_or_default()
}

fn report_for(
    observed: Observed,
    accepted: &FencedCommandAcceptance,
    name: &str,
    attempt_id: &str,
    scratch_note: Option<String>,
) -> ConsumerReport {
    let mut report = empty_report(attempt_id);
    let mut notes = Vec::new();
    let outcome = apply_observed(&mut report, observed, accepted, name, &mut notes);
    notes.extend(scratch_note);
    ConsumerReport {
        outcome,
        document: document(&report, notes),
        settled: true,
        built_commit: None,
    }
}

fn lost_report(accepted: &FencedCommandAcceptance, attempt_id: &str) -> ConsumerReport {
    report_for(
        Observed::Unknown(
            "Cairn lost track of this act (the runner stopped while it was in flight), so whether the command ran is unknown".into(),
        ),
        accepted,
        &accepted.act.executor_id,
        attempt_id,
        None,
    )
}

/// The requesting node's scratch, exactly as accepted, and the grant that
/// carries it. A node that never ran a batch has none, and the act runs with a
/// private `$TMPDIR`; the act never creates a scratch for a node. A scratch
/// whose head moved since acceptance is refused: its files are what the
/// command may run, and the operator accepted the ones it held then.
async fn node_scratch(
    orch: &Orchestrator,
    accepted: &FencedCommandAcceptance,
) -> Result<Option<(NodeScratch, NodeScratchGrant)>, String> {
    let scratch = super::node_scratch_now(orch, &accepted.act).await?;
    if scratch
        .as_ref()
        .and_then(|scratch| scratch.head_commit.as_ref())
        != accepted.scratch_head.as_ref()
    {
        return Err(super::SCRATCH_MOVED.into());
    }
    let Some(scratch) = scratch else {
        return Ok(None);
    };
    crate::node_scratch::lifecycle::ensure_subscribed(orch);
    let ceiling = crate::node_scratch::owner::ceiling_bytes(orch, &scratch.project_id).await;
    let grant = scratch.grant(ceiling);
    Ok(Some((scratch, grant)))
}

// ─── A declared act that lands its commit ─────────────────────────────────────────────────────────

/// One run of a `lands` act's command.
pub(crate) struct CommandRun {
    pub(crate) observed: Observed,
    /// The cell request as placement prepared it: the delta's origin, which
    /// the seal installs it under.
    pub(crate) request: CellRequest,
    /// The tracked changes the command made, on the base it ran at.
    pub(crate) delta: Option<crate::fleet::MutationDelta>,
}

impl CommandRun {
    fn from_cell(request: CellRequest, outcome: &CellOutcome) -> Self {
        let delta = match outcome {
            CellOutcome::Completed {
                mutation_delta: Some(delta),
                ..
            } => Some((**delta).clone()),
            _ => None,
        };
        Self {
            observed: observe(outcome),
            request,
            delta,
        }
    }
}

/// What [`perform_lands`] needs from outside: one run of the accepted command
/// at a base, and the live recheck of the acceptance before its landing.
#[async_trait]
pub(crate) trait LandsRunner: Send + Sync {
    async fn run(&self, orch: &Orchestrator, base: &str) -> CommandRun;
    async fn recheck(&self, orch: &Orchestrator) -> Result<(), String>;
}

/// How a `lands` attempt ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LandsEnd {
    /// The act's commit is on the default branch: landed now (`sealed` and
    /// `base` set), or adopted from an earlier attempt of the same act.
    Landed {
        commit: String,
        sealed: Option<String>,
        base: Option<String>,
    },
    /// Nothing landed, and nothing of this attempt is left behind.
    Failed(String),
    /// Nothing landed, and what the command did is unknown.
    Inconclusive(String),
    /// Not reported: landing recovery still owns an unsettled publication, or
    /// the sealed branch could not be discarded. The attempt stays claimed and
    /// the consumer's recovery settles it.
    LeftClaimed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LandsOutcome {
    /// The last run's observation, if the command ran in this attempt.
    pub(crate) observed: Option<Observed>,
    pub(crate) runs: u32,
    pub(crate) end: LandsEnd,
    pub(crate) notes: Vec<String>,
}

/// Perform a declared `lands` act once accepted and claimed (see the module
/// doc). Every refusal before a seal leaves nothing; every one after it
/// discards the sealed branch, which carries this attempt's trailer.
pub(crate) async fn perform_lands(
    orch: &Orchestrator,
    attempt_id: &str,
    act: &FencedCommandAct,
    runner: &dyn LandsRunner,
) -> LandsOutcome {
    let mut outcome = LandsOutcome {
        observed: None,
        runs: 0,
        end: LandsEnd::Failed(String::new()),
        notes: Vec::new(),
    };
    outcome.end = lands_loop(orch, attempt_id, act, runner, &mut outcome).await;
    outcome
}

/// The end a run's observation forces before anything is sealed, if any.
fn unclean(observed: &Observed, accepted_device: &str) -> Option<LandsEnd> {
    match observed {
        Observed::Ran { device_id, .. } if !device_id.is_empty() && device_id != accepted_device => {
            Some(LandsEnd::Inconclusive(format!(
                "the command ran on device {device_id}, not the accepted {accepted_device}; nothing lands"
            )))
        }
        Observed::Ran {
            exit_code: Some(0),
            timed_out: false,
            ..
        } => None,
        Observed::Ran {
            timed_out: true, ..
        } => Some(LandsEnd::Failed(
            "the command timed out; nothing lands".into(),
        )),
        Observed::Ran { exit_code, .. } => Some(LandsEnd::Failed(format!(
            "the command exited {}; nothing lands",
            exit_code.map_or("without a code".to_string(), |code| code.to_string())
        ))),
        Observed::NotRun(detail) => Some(LandsEnd::Failed(format!("nothing ran: {detail}"))),
        Observed::Unknown(detail) => Some(LandsEnd::Inconclusive(detail.clone())),
    }
}

/// Discard this attempt's sealed branch, then end as `end`; a branch that
/// cannot be discarded leaves the attempt to recovery instead.
async fn discarded(
    orch: &Orchestrator,
    act: &FencedCommandAct,
    branch: &str,
    sealed: &str,
    end: LandsEnd,
) -> LandsEnd {
    match discard_request_branch(orch, &act.project_id, branch, sealed).await {
        Ok(()) => end,
        Err(error) => LandsEnd::LeftClaimed(format!(
            "{end:?}, and the sealed branch {branch} was kept ({error}), so recovery settles this attempt"
        )),
    }
}

async fn lands_loop(
    orch: &Orchestrator,
    attempt_id: &str,
    act: &FencedCommandAct,
    runner: &dyn LandsRunner,
    outcome: &mut LandsOutcome,
) -> LandsEnd {
    let Some((declared, lands)) = act
        .declaration
        .as_ref()
        .and_then(|declared| Some((declared, declared.lands.as_ref()?)))
    else {
        return LandsEnd::Failed("the act is not a declared command that lands".into());
    };
    let act_sha = match declared::act_sha(act) {
        Ok(sha) => sha,
        Err(error) => return LandsEnd::Failed(error),
    };
    let branch = crate::jj::request_branch(attempt_id);
    let message = declared::sealed_message(&lands.message, &act_sha, attempt_id);
    let paths = lands.paths.clone();
    let admits = move |path: &str| crate::orchestrator::landing::path_matches(&paths, path);
    let mut moved = String::new();
    while outcome.runs < MAX_LANDS_RUNS {
        let tip = match declared::recheck(orch, act, declared).await {
            Ok(Standing::Adopted(commit)) => {
                outcome.notes.push(format!(
                    "{commit} on the default branch is this act's commit, so it was adopted and the command did not run again"
                ));
                return LandsEnd::Landed {
                    commit,
                    sealed: None,
                    base: None,
                };
            }
            Ok(Standing::Current(tip)) => tip,
            Err(error) => return LandsEnd::Failed(error),
        };
        outcome.runs += 1;
        let run = runner.run(orch, &tip.tip).await;
        outcome.observed = Some(run.observed.clone());
        if let Some(end) = unclean(&run.observed, &act.device_id) {
            return end;
        }
        let Some(delta) = run.delta else {
            return LandsEnd::Failed(
                "the command changed no tracked file, so there is nothing to land".into(),
            );
        };
        if delta.base_commit != tip.tip {
            return LandsEnd::Failed(format!(
                "the command ran at {}, not the default branch's tip {}; nothing lands",
                delta.base_commit, tip.tip
            ));
        }
        let sealed = match crate::mcp::handlers::run::seal_request_delta(
            orch,
            &tip.store,
            &run.request,
            &delta,
            &branch,
            &message,
            &admits,
        )
        .await
        {
            Ok(sealed) => sealed,
            // Nothing was sealed, so nothing is discarded before running again.
            Err(crate::mcp::handlers::run::SealRefusal::BaseMoved(why)) => {
                moved = why;
                continue;
            }
            Err(error) => return LandsEnd::Failed(format!("nothing was sealed: {error}")),
        };
        let refusal =
            match declared::sealed_reads_resolved(orch, &tip.store, &sealed.commit, declared) {
                Err(error) => Some(error),
                Ok(()) => runner.recheck(orch).await.err(),
            };
        if let Some(refusal) = refusal {
            return discarded(
                orch,
                act,
                &branch,
                &sealed.commit,
                LandsEnd::Failed(refusal),
            )
            .await;
        }
        match land_request_commit(orch, &act.project_id, attempt_id, &branch, &lands.message).await
        {
            Ok(landing) => {
                if let Err(error) = retire_request_branch(
                    orch,
                    &act.project_id,
                    &branch,
                    &tip.branch,
                    &landing.landed_commit,
                )
                .await
                {
                    outcome.notes.push(format!(
                        "the landed request branch was not retired: {error}"
                    ));
                }
                return LandsEnd::Landed {
                    commit: landing.landed_commit,
                    sealed: Some(sealed.commit),
                    base: Some(sealed.base),
                };
            }
            Err(RequestLandingError::BaseMoved(why)) => {
                if let LandsEnd::LeftClaimed(kept) = discarded(
                    orch,
                    act,
                    &branch,
                    &sealed.commit,
                    LandsEnd::Failed(String::new()),
                )
                .await
                {
                    return LandsEnd::LeftClaimed(kept);
                }
                moved = why;
            }
            Err(RequestLandingError::PolicyRefused(why) | RequestLandingError::Failed(why)) => {
                // A publication whose outcome is unknown may have landed, or
                // may yet be settled by landing recovery.
                if let Ok(Standing::Adopted(commit)) = declared::recheck(orch, act, declared).await
                {
                    return LandsEnd::Landed {
                        commit,
                        sealed: Some(sealed.commit),
                        base: Some(sealed.base),
                    };
                }
                if publication_pending(orch, &act.project_id, attempt_id).await {
                    return LandsEnd::LeftClaimed(format!(
                        "{why}; the landing's publication is unsettled, so landing recovery settles it first"
                    ));
                }
                return discarded(orch, act, &branch, &sealed.commit, LandsEnd::Failed(why)).await;
            }
        }
    }
    LandsEnd::Failed(format!(
        "main kept moving under this command ({MAX_LANDS_RUNS} runs, the last refused because {moved}); a `lands` command must finish faster than main moves"
    ))
}

/// Whether landing recovery still owns an unsettled publication of the
/// attempt. An unreadable journal counts as pending: discarding the branch
/// under a publication that landed would lose it.
async fn publication_pending(orch: &Orchestrator, project_id: &str, attempt_id: &str) -> bool {
    let db = match crate::execution::routing::owning_db_for_project(&orch.db, project_id).await {
        Ok(db) => db,
        Err(_) => return true,
    };
    crate::pr_data::actions::request_publication_pending(&db, attempt_id)
        .await
        .unwrap_or(true)
}

/// The report a `lands` attempt settles with, or `None` when it is left to
/// recovery.
fn lands_report(
    outcome: &LandsOutcome,
    accepted: &FencedCommandAcceptance,
    name: &str,
    attempt_id: &str,
) -> Option<ConsumerReport> {
    let mut report = empty_report(attempt_id);
    let mut notes = Vec::new();
    if let Some(observed) = &outcome.observed {
        apply_observed(&mut report, observed.clone(), accepted, name, &mut notes);
    }
    if outcome.runs > 1 {
        notes.push(format!(
            "the default branch moved under the command, so it ran {} times",
            outcome.runs
        ));
    }
    notes.extend(outcome.notes.iter().cloned());
    let (result, built_commit) = match &outcome.end {
        LandsEnd::LeftClaimed(_) => return None,
        LandsEnd::Landed {
            commit,
            sealed,
            base,
        } => {
            report.landed_commit = Some(commit.clone());
            report.sealed_commit = sealed.clone();
            report.base = base.clone();
            (RequestAttemptOutcome::Done, Some(commit.clone()))
        }
        LandsEnd::Failed(why) => {
            notes.push(why.clone());
            (RequestAttemptOutcome::Failed, None)
        }
        LandsEnd::Inconclusive(why) => {
            notes.push(why.clone());
            (RequestAttemptOutcome::Inconclusive, None)
        }
    };
    Some(ConsumerReport {
        outcome: result,
        document: document(&report, notes),
        settled: true,
        built_commit,
    })
}

/// The fleet's operator-act path as a [`LandsRunner`]: each run its own cell
/// on the accepted machine, at the base it is given, returning its delta.
struct FleetLands {
    act: FencedCommandAct,
    repository: String,
    attempt_id: String,
    context: AcceptedRequestContext,
    runs: AtomicU32,
}

#[async_trait]
impl LandsRunner for FleetLands {
    async fn run(&self, orch: &Orchestrator, base: &str) -> CommandRun {
        let run = self.runs.fetch_add(1, Ordering::Relaxed) + 1;
        // The first run's cell keeps the attempt's own name, as a raw act's
        // does; each later run is a cell attempt of its own.
        let cell_attempt_id = if run == 1 {
            self.attempt_id.clone()
        } else {
            format!("{}-run{run}", self.attempt_id)
        };
        let act = OperatorAct {
            act: self.act.clone(),
            repository: self.repository.clone(),
            cell_request_id: receipt_id(&self.attempt_id),
            cell_attempt_id,
            node_scratch: None,
            base_commit: base.to_string(),
            mutation_policy: MutationPolicy::AllowDelta,
        };
        let (request, outcome) = orch.fleet.submit_operator_act(orch, &act).await;
        CommandRun::from_cell(request, &outcome)
    }

    async fn recheck(&self, orch: &Orchestrator) -> Result<(), String> {
        self.context.recheck(orch).await
    }
}

/// Settle a `lands` attempt this process is not running. `None` leaves it
/// claimed for a later pass.
async fn recover_lands(
    orch: &Orchestrator,
    accepted: &FencedCommandAcceptance,
    attempt_id: &str,
) -> Result<Option<ConsumerReport>, String> {
    let act = &accepted.act;
    let (declared, lands) = act
        .declaration
        .as_ref()
        .and_then(|declared| Some((declared, declared.lands.as_ref()?)))
        .ok_or("the act is not a declared command that lands")?;
    let tip = declared::default_tip(orch, &act.project_id).await?;
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let branch = crate::jj::request_branch(attempt_id);
    let head = crate::jj::bookmark_commit_checked(&jj, &tip.store, &branch)?;
    let name = crate::fleet::management::public_name(orch, &act.executor_id);
    if let Some(commit) = declared::adopted_commit(
        &tip.store,
        &declared.defined_by_commit,
        &tip.tip,
        &declared::act_sha(act)?,
        lands,
    )? {
        let mut notes = vec![format!(
            "recovered: {commit} on {} is this act's commit",
            tip.branch
        )];
        match head.as_deref() {
            Some(head) if head == commit => {
                if let Err(error) =
                    retire_request_branch(orch, &act.project_id, &branch, &tip.branch, &commit)
                        .await
                {
                    notes.push(format!(
                        "the landed request branch was not retired: {error}"
                    ));
                }
            }
            Some(head) => discard_request_branch(orch, &act.project_id, &branch, head).await?,
            None => {}
        }
        let outcome = LandsOutcome {
            observed: None,
            runs: 0,
            end: LandsEnd::Landed {
                sealed: head.filter(|head| *head == commit),
                commit,
                base: None,
            },
            notes,
        };
        return Ok(lands_report(&outcome, accepted, &name, attempt_id));
    }
    if publication_pending(orch, &act.project_id, attempt_id).await {
        return Ok(None);
    }
    if let Some(head) = head {
        discard_request_branch(orch, &act.project_id, &branch, &head).await?;
    }
    Ok(Some(lost_report(accepted, attempt_id)))
}

impl FencedCommandConsumer {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            gate: Arc::new(tokio::sync::Mutex::new(())),
            running: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Settle every claim whose act this process is not awaiting.
    pub(crate) async fn recover(&self, orch: &Orchestrator) -> Result<(), String> {
        let _gate = self.gate.lock().await;
        let attempts = store::unresolved_claims(&orch.db.local, RequestKind::FencedCommand)
            .await
            .map_err(|error| error.to_string())?;
        let mut first_error = None;
        for attempt in attempts {
            let attempt_id = attempt.acceptance.attempt_id.clone();
            if self.running.lock().unwrap().contains(&attempt_id) {
                continue;
            }
            let result = async {
                let adapter = orch.operator_requests.consumer(KIND)?;
                let context = operator_requests::recover_claimed_request(
                    orch,
                    &attempt.authorization_event_id,
                    &attempt.acceptance,
                    adapter,
                )
                .await?;
                let accepted = accepted(&attempt.acceptance)?;
                let report = if lands(&accepted.act) {
                    match recover_lands(orch, accepted, &attempt_id).await? {
                        Some(report) => report,
                        None => return Ok(()),
                    }
                } else {
                    lost_report(accepted, &attempt_id)
                };
                context.report(orch, &receipt_id(&attempt_id), report).await
            }
            .await;
            if let Err(error) = result {
                first_error.get_or_insert(format!("attempt {attempt_id}: {error}"));
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

#[async_trait]
impl RequestConsumer for FencedCommandConsumer {
    fn kind(&self) -> &'static str {
        KIND
    }

    fn actor(&self) -> &'static str {
        ACTOR
    }

    async fn exclusion(&self) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        Some(self.gate.clone().lock_owned().await)
    }

    /// Availability is per executor and is decided by its measured floor in
    /// `facts`; the kind itself is always served.
    async fn availability(&self, _: &Orchestrator) -> Result<(), String> {
        Ok(())
    }

    async fn facts(
        &self,
        orch: &Orchestrator,
        _request_id: Option<&str>,
        target: Option<&str>,
    ) -> Result<ActorFacts, String> {
        super::facts(orch, target).await
    }

    /// At acceptance, before the claim, and again at claim: the act is the
    /// request's, the executor is the same device on the same connection with
    /// the same floor measurement, and it is online. A declared act also
    /// passes the declared recheck: its declaration is unchanged at the tip,
    /// and a `lands` act that has not landed still starts from the version it
    /// resolved from, under a base-merge policy that allows the landing.
    async fn validate_snapshot(
        &self,
        orch: &Orchestrator,
        acceptance: &RequestAcceptance,
    ) -> Result<(), String> {
        let accepted = accepted(acceptance)?;
        super::recheck_live(orch, accepted).await?;
        if let Some(declared) = &accepted.act.declaration {
            declared::recheck(orch, &accepted.act, declared).await?;
        }
        Ok(())
    }

    async fn contains_commits(
        &self,
        _: &Orchestrator,
        _: &str,
        _: &[String],
    ) -> Result<(), String> {
        Err("fenced_command has no commit requirements".into())
    }

    async fn executor_target(&self, _: &Orchestrator, _: &str) -> Result<ExecutorTarget, String> {
        Err("fenced_command names its executor inside the frozen act".into())
    }

    /// The report is this consumer's own reading of the cell outcome; what is
    /// checked here is that it describes this attempt and is internally
    /// consistent, so nothing else can settle the attempt. A `lands` act's
    /// success names its landed commit, which must be on the default branch
    /// as exactly this act's commit.
    async fn verify_report(
        &self,
        orch: &Orchestrator,
        context: &AcceptedRequestContext,
        report: ConsumerReport,
    ) -> Result<ConsumerReport, String> {
        let acceptance = context.acceptance();
        let act = &accepted(acceptance)?.act;
        let parsed: FencedCommandReport = serde_json::from_value(report.document.clone())
            .map_err(|error| format!("fenced_command report is unreadable: {error}"))?;
        if parsed.cell_attempt_id != acceptance.attempt_id
            || parsed.cell_request_id != receipt_id(&acceptance.attempt_id)
        {
            return Err("fenced_command report describes another attempt's cell".into());
        }
        if !report.settled {
            return Err("a fenced_command attempt settles once; there is no later report".into());
        }
        if lands(act) {
            match (&report.outcome, parsed.landed_commit.as_deref()) {
                (RequestAttemptOutcome::Done, Some(landed)) => {
                    if report.built_commit.as_deref() != Some(landed) {
                        return Err("a landed act's built commit is its landed commit".into());
                    }
                    declared::verify_landed(orch, act, landed).await?;
                }
                (RequestAttemptOutcome::Done, None) => {
                    return Err("a performed lands act names the commit it landed".into());
                }
                (_, Some(_)) => {
                    return Err("only a performed lands act names a landed commit".into());
                }
                (_, None) if report.built_commit.is_some() => {
                    return Err("only a performed lands act names a built commit".into());
                }
                (_, None) => {}
            }
            return Ok(report);
        }
        if report.outcome == RequestAttemptOutcome::Done && !parsed.ran {
            return Err("a performed fenced_command must have run".into());
        }
        if report.outcome == RequestAttemptOutcome::Failed && parsed.ran {
            return Err("a fenced_command that ran is never a pre-effect failure".into());
        }
        Ok(report)
    }

    async fn dispatch(
        &self,
        orch: &Orchestrator,
        context: &AcceptedRequestContext,
    ) -> Result<(), String> {
        // The primitive holds `exclusion` and has rechecked the claim.
        let acceptance = context.acceptance().clone();
        let attempt_id = acceptance.attempt_id.clone();
        let accepted = accepted(&acceptance)?.clone();
        let name = crate::fleet::management::public_name(orch, &accepted.act.executor_id);
        let prepared = async {
            let repository = crate::mcp::handlers::branch::project_repository_path(
                orch,
                &accepted.act.project_id,
            )
            .await
            .map_err(|error| format!("the project repository is unavailable: {error}"))?;
            Ok::<_, String>((repository, node_scratch(orch, &accepted).await?))
        }
        .await;
        let (repository, scratch) = match prepared {
            Ok(prepared) => prepared,
            Err(detail) => {
                // Nothing was submitted, so this is positive knowledge of no
                // effect rather than the primitive's inconclusive dispatch error.
                let report = report_for(
                    Observed::NotRun(detail),
                    &accepted,
                    &name,
                    &attempt_id,
                    None,
                );
                return context.report(orch, &receipt_id(&attempt_id), report).await;
            }
        };
        let repository = repository.to_string_lossy().into_owned();
        self.running.lock().unwrap().insert(attempt_id.clone());
        let running = self.running.clone();
        let orch = orch.clone();
        let context = context.clone();
        if lands(&accepted.act) {
            cairn_common::task::spawn(async move {
                let runner = FleetLands {
                    act: accepted.act.clone(),
                    repository,
                    attempt_id: attempt_id.clone(),
                    context: context.clone(),
                    runs: AtomicU32::new(0),
                };
                let outcome = perform_lands(&orch, &attempt_id, &accepted.act, &runner).await;
                match lands_report(&outcome, &accepted, &name, &attempt_id) {
                    Some(report) => {
                        if let Err(error) = context
                            .report(&orch, &receipt_id(&attempt_id), report)
                            .await
                        {
                            tracing::warn!(attempt = %attempt_id, %error, "fenced_command lands outcome could not be reported; reconciliation will settle it");
                        }
                    }
                    None => {
                        tracing::warn!(attempt = %attempt_id, end = ?outcome.end, "fenced_command lands attempt left claimed for reconciliation");
                    }
                }
                running.lock().unwrap().remove(&attempt_id);
            });
            return Ok(());
        }
        let act = OperatorAct {
            act: accepted.act.clone(),
            repository,
            cell_request_id: receipt_id(&attempt_id),
            cell_attempt_id: attempt_id.clone(),
            node_scratch: scratch.as_ref().map(|(_, grant)| grant.clone()),
            base_commit: accepted.act.commit.clone(),
            mutation_policy: MutationPolicy::PureVerdict,
        };
        cairn_common::task::spawn(async move {
            let (_, outcome) = orch.fleet.submit_operator_act(&orch, &act).await;
            let scratch_note = match (&scratch, &outcome) {
                (
                    Some((scratch, _)),
                    CellOutcome::Completed {
                        node_scratch: Some(report),
                        metadata,
                        ..
                    },
                ) => {
                    crate::node_scratch::report::settle(
                        &orch,
                        scratch,
                        report,
                        &metadata.executor_id,
                    )
                    .await
                }
                _ => None,
            };
            let report = report_for(
                observe(&outcome),
                &accepted,
                &name,
                &attempt_id,
                scratch_note,
            );
            if let Err(error) = context
                .report(&orch, &receipt_id(&attempt_id), report)
                .await
            {
                tracing::warn!(attempt = %attempt_id, %error, "fenced_command outcome could not be reported; reconciliation will settle it inconclusive");
            }
            running.lock().unwrap().remove(&attempt_id);
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::requests::{ExecutorFloorWitness, FencedCommandAct};

    fn accepted() -> FencedCommandAcceptance {
        FencedCommandAcceptance {
            act: FencedCommandAct {
                executor_id: "exec-1".into(),
                device_id: "device-1".into(),
                project_id: "project".into(),
                branch: "agent/x".into(),
                commit: "a".repeat(40),
                cwd: String::new(),
                command: "make install".into(),
                timeout_ms: 60_000,
                node: "cairn://p/p/1/1/builder".into(),
                declaration: None,
            },
            witness: ExecutorFloorWitness {
                connection_generation: 4,
                executor_build_id: "build-9".into(),
                boot_generation: "boot-1".into(),
                floor_fingerprint: "f".repeat(64),
                floor_measured_at_unix_ms: 1,
            },
            scratch_head: None,
        }
    }

    fn ran(exit_code: i32, device: &str, generation: u64) -> Observed {
        Observed::Ran {
            exit_code: Some(exit_code),
            timed_out: false,
            output: "done\n".into(),
            duration_ms: Some(1_200),
            device_id: device.into(),
            connection_generation: generation,
        }
    }

    fn read(report: &ConsumerReport) -> FencedCommandReport {
        serde_json::from_value(report.document.clone()).unwrap()
    }

    #[test]
    fn a_command_that_ran_is_performed_whatever_it_exited() {
        for code in [0, 2] {
            let report = report_for(ran(code, "device-1", 4), &accepted(), "mac", "att", None);
            assert_eq!(report.outcome, RequestAttemptOutcome::Done);
            assert!(report.settled);
            let body = read(&report);
            assert!(body.ran);
            assert_eq!(body.exit_code, Some(code));
            assert_eq!(body.cell_request_id, "fenced_command:att");
            assert_eq!(body.cell_attempt_id, "att");
            let executor = body.executor.unwrap();
            assert_eq!(executor.name, "mac");
            assert_eq!(executor.executor_build_id, "build-9");
            assert_eq!(executor.connection_generation, 4);
            assert_eq!(body.detail, None);
        }
    }

    #[test]
    fn a_reconnect_is_recorded_and_another_device_is_never_claimed_as_success() {
        let reconnected = report_for(ran(0, "device-1", 5), &accepted(), "mac", "att", None);
        assert_eq!(reconnected.outcome, RequestAttemptOutcome::Done);
        assert!(read(&reconnected)
            .detail
            .unwrap()
            .contains("generation 4 to 5"));

        let elsewhere = report_for(ran(0, "device-2", 4), &accepted(), "mac", "att", None);
        assert_eq!(elsewhere.outcome, RequestAttemptOutcome::Inconclusive);
        assert!(read(&elsewhere).detail.unwrap().contains("device-2"));
    }

    #[test]
    fn no_process_is_a_pre_effect_failure_and_an_unknown_one_is_inconclusive() {
        let refused = report_for(
            Observed::NotRun("no executor".into()),
            &accepted(),
            "mac",
            "att",
            None,
        );
        assert_eq!(refused.outcome, RequestAttemptOutcome::Failed);
        assert!(!read(&refused).ran);

        let lost = lost_report(&accepted(), "att");
        assert_eq!(lost.outcome, RequestAttemptOutcome::Inconclusive);
        assert!(lost.settled);
        assert!(read(&lost).detail.unwrap().contains("lost track"));
    }

    /// A lands act lands nothing unless its command ran cleanly on the
    /// accepted machine: a non-zero exit or a timeout fails, another device
    /// or an unknown outcome is inconclusive.
    #[test]
    fn a_lands_run_seals_only_after_a_clean_exit_on_the_accepted_device() {
        assert_eq!(unclean(&ran(0, "device-1", 4), "device-1"), None);
        assert_eq!(unclean(&ran(0, "", 4), "device-1"), None);
        assert!(matches!(
            unclean(&ran(1, "device-1", 4), "device-1"),
            Some(LandsEnd::Failed(why)) if why.contains("exited 1")
        ));
        let mut timed_out = ran(0, "device-1", 4);
        if let Observed::Ran { timed_out, .. } = &mut timed_out {
            *timed_out = true;
        }
        assert!(matches!(
            unclean(&timed_out, "device-1"),
            Some(LandsEnd::Failed(why)) if why.contains("timed out")
        ));
        assert!(matches!(
            unclean(&ran(0, "device-2", 4), "device-1"),
            Some(LandsEnd::Inconclusive(_))
        ));
        assert!(matches!(
            unclean(&Observed::Unknown("lost".into()), "device-1"),
            Some(LandsEnd::Inconclusive(_))
        ));
    }

    /// A landed act reports its commit as the built commit, which is what the
    /// chained rebuild is for; a run that failed after running reports
    /// `Failed` with nothing landed, and a left-claimed attempt reports
    /// nothing.
    #[test]
    fn a_lands_report_carries_the_landed_commit_and_only_then() {
        let landed = LandsOutcome {
            observed: Some(ran(0, "device-1", 4)),
            runs: 2,
            end: LandsEnd::Landed {
                commit: "c".repeat(40),
                sealed: Some("c".repeat(40)),
                base: Some("b".repeat(40)),
            },
            notes: Vec::new(),
        };
        let report = lands_report(&landed, &accepted(), "mac", "att").unwrap();
        assert_eq!(report.outcome, RequestAttemptOutcome::Done);
        assert_eq!(report.built_commit, Some("c".repeat(40)));
        let body = read(&report);
        assert_eq!(body.landed_commit, Some("c".repeat(40)));
        assert_eq!(body.base, Some("b".repeat(40)));
        assert!(body.ran);
        assert!(body.detail.unwrap().contains("ran 2 times"));

        let failed = LandsOutcome {
            end: LandsEnd::Failed("the command exited 1; nothing lands".into()),
            runs: 1,
            ..landed.clone()
        };
        let report = lands_report(&failed, &accepted(), "mac", "att").unwrap();
        assert_eq!(report.outcome, RequestAttemptOutcome::Failed);
        assert_eq!(report.built_commit, None);
        assert_eq!(read(&report).landed_commit, None);

        let kept = LandsOutcome {
            end: LandsEnd::LeftClaimed("unsettled".into()),
            ..landed
        };
        assert!(lands_report(&kept, &accepted(), "mac", "att").is_none());
    }

    #[test]
    fn the_output_keeps_its_tail_within_the_run_result_cap() {
        let cap = crate::mcp::handlers::run::MAX_RUN_RESULT_BYTES;
        let long = format!("{}é{}END", "x".repeat(cap), "y".repeat(10));
        let tail = output_tail(&long);
        assert!(tail.ends_with("END"));
        assert!(tail.starts_with('['));
        assert!(tail.len() <= cap + 40);
        assert_eq!(output_tail("short"), "short");
    }

    /// Only an accepted request reaches an unfenced spawn: no source outside
    /// the consumer and the fleet's operator-act path names that path or the
    /// value it accepts, so in particular the agent's `run` verb cannot.
    #[test]
    fn only_the_request_consumer_reaches_the_operator_act_path() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let allowed = [
            root.join("fleet/operator_act.rs"),
            root.join("operator_requests/fenced_command/mod.rs"),
            root.join("operator_requests/fenced_command/consumer.rs"),
        ];
        let mut stack = vec![root.clone()];
        let mut offenders = Vec::new();
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().is_some_and(|ext| ext == "rs")
                    && !allowed.contains(&path)
                {
                    let source = std::fs::read_to_string(&path).unwrap();
                    if source.contains("submit_operator_act") || source.contains("OperatorAct {") {
                        offenders.push(path);
                    }
                }
            }
        }
        assert!(offenders.is_empty(), "{offenders:?}");
    }
}
