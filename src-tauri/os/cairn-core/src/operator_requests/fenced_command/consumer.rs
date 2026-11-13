//! The `fenced_command` consumer. Dispatch hands the frozen act to the fleet's
//! operator-act path on a task of its own, because the request outbox delivers
//! one intent at a time and a command may run for hours. The attempt settles
//! once, from the cell outcome, through the primitive's checked report.
//!
//! An attempt this process is not running has no live observer: a runner that
//! restarted mid-act cannot learn what the command did. Reconciliation settles
//! such a claim `Inconclusive` and never relaunches it, because the command may
//! have run, so running it again needs another acceptance.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cairn_common::executor_protocol::{CellOutcome, NodeScratchGrant};
use cairn_common::requests::{
    FencedCommandAcceptance, FencedCommandExecutorReport, FencedCommandReport, RequestAcceptance,
    RequestAttemptOutcome, RequestKind,
};
use cairn_db::storage::requests as store;

use super::{OperatorAct, ACTOR, KIND};
use crate::node_scratch::NodeScratch;
use crate::operator_requests::registry::{
    ActorFacts, ConsumerReport, ExecutorTarget, RequestConsumer,
};
use crate::operator_requests::{self, AcceptedRequestContext};
use crate::orchestrator::Orchestrator;

const RECOVERY_INTERVAL: Duration = Duration::from_secs(60);

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
    tokio::spawn(async move {
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

/// What a cell outcome says about the command, reduced to what the report
/// needs.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Observed {
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

fn report_for(
    observed: Observed,
    accepted: &FencedCommandAcceptance,
    name: &str,
    attempt_id: &str,
    scratch_note: Option<String>,
) -> ConsumerReport {
    let mut report = FencedCommandReport {
        ran: false,
        exit_code: None,
        timed_out: false,
        duration_ms: None,
        output: String::new(),
        executor: None,
        cell_request_id: receipt_id(attempt_id),
        cell_attempt_id: attempt_id.to_string(),
        detail: None,
    };
    let outcome = match observed {
        Observed::Ran {
            exit_code,
            timed_out,
            output,
            duration_ms,
            device_id,
            connection_generation,
        } => {
            let mut notes = Vec::new();
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
            notes.extend(scratch_note);
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
            report.detail = (!notes.is_empty()).then(|| notes.join("; "));
            // A non-zero exit or a timeout is still the act performed: the
            // command ran and failed.
            if retargeted {
                RequestAttemptOutcome::Inconclusive
            } else {
                RequestAttemptOutcome::Done
            }
        }
        Observed::NotRun(detail) => {
            report.detail = Some(format!("nothing ran: {detail}"));
            RequestAttemptOutcome::Failed
        }
        Observed::Unknown(detail) => {
            report.detail = Some(detail);
            RequestAttemptOutcome::Inconclusive
        }
    };
    ConsumerReport {
        outcome,
        document: serde_json::to_value(&report).unwrap_or_default(),
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
    let scratch = super::node_scratch_now(orch, &accepted.act.node).await?;
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
                let report = lost_report(accepted(&attempt.acceptance)?, &attempt_id);
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

    /// At acceptance and again at claim: the act is the request's, the
    /// executor is the same device on the same connection with the same
    /// floor measurement, it is online, and the branch has not moved.
    async fn validate_snapshot(
        &self,
        orch: &Orchestrator,
        acceptance: &RequestAcceptance,
    ) -> Result<(), String> {
        super::recheck_live(orch, accepted(acceptance)?)
            .await
            .map(|_| ())
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
    /// consistent, so nothing else can settle the attempt.
    async fn verify_report(
        &self,
        _: &Orchestrator,
        context: &AcceptedRequestContext,
        report: ConsumerReport,
    ) -> Result<ConsumerReport, String> {
        let acceptance = context.acceptance();
        accepted(acceptance)?;
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
        let act = OperatorAct {
            act: accepted.act.clone(),
            repository: repository.to_string_lossy().into_owned(),
            cell_request_id: receipt_id(&attempt_id),
            cell_attempt_id: attempt_id.clone(),
            node_scratch: scratch.as_ref().map(|(_, grant)| grant.clone()),
        };
        self.running.lock().unwrap().insert(attempt_id.clone());
        let running = self.running.clone();
        let orch = orch.clone();
        let context = context.clone();
        tokio::spawn(async move {
            let outcome = orch.fleet.submit_operator_act(&orch, &act).await;
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
