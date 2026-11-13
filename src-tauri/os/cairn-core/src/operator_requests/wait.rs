//! A waiting `cairn://requests` filing: the filer's call holds for the
//! request's outcome and resumes with it rendered as the tool result.
//!
//! `wait` is a call flag, not a request fact, so it is split off the payload
//! before the filing record is parsed ([`split_filing`]). A waiting filing is a
//! blocking append (`resources::mutations::blocking`): the write applies its
//! other items first, then files and waits, and the outcome or the hand-off
//! marker rides in the report's `blocking.text` like a question's answer.
//!
//! The wait is the inline-then-park shape every durable call shares. It holds
//! inline for [`INLINE_DURABLE_WAIT_BUDGET`]; past that it parks on a
//! [`Condition::OperatorRequest`] `agent_waits` row and the turn resumes through
//! [`durable_suspend::drive_resolution`], which answers each row exactly once.
//! Nothing polls. Every request transition enqueues a `changed` intent in the
//! transaction that makes it, and that intent's post-commit delivery
//! ([`super::delivery`]) calls [`request_changed`]. A waiter re-reads the
//! request on every signal, so a redelivered intent or another install's id
//! costs one read and never a second answer, and a restart re-arms the row from
//! the request itself (`durable_suspend::reconcile`).

use std::sync::LazyLock;
use std::time::Duration;

use cairn_common::callback_contract::INLINE_DURABLE_WAIT_BUDGET;
use cairn_common::protocol::CallbackRequest;
use cairn_common::requests::{
    FencedCommandReport, FileOperatorRequest, FiledOperatorRequest, OperatorRequest,
    RequestAttemptOutcome, RequestKind, RequestState,
};
use cairn_db::storage::requests as store;
use serde_json::Value;
use tokio::sync::broadcast;

use crate::mcp::handlers::durable_suspend::{self, Condition, Record};
use crate::mcp::handlers::tool_use_correlation::{claim_tool_use_id, Claim};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;

/// What a waiting filer reads when its request outlived the inline budget. The
/// transcript mirrors this phrase (`suspensionHandoff.ts`) to keep the call in
/// flight until the outcome lands on the same tool-use id.
pub(crate) const REQUEST_WAIT_SUSPENDED_MARKER: &str =
    "Request wait suspended; the same write call will resume with the request's outcome.";

/// Request ids whose rows just changed, published after the change committed.
/// Install-wide rather than per orchestrator because requests are install-local
/// and a waiter re-reads its own database on every signal: a signal for an id
/// in another database is one wasted read, never a wrong answer.
static CHANGES: LazyLock<broadcast::Sender<String>> = LazyLock::new(|| broadcast::channel(256).0);

/// Signal that `request_id`'s row changed. Called only from post-commit outbox
/// delivery, so a waiter that wakes always reads the committed transition.
pub(crate) fn request_changed(request_id: &str) {
    // No receiver is the ordinary case: nobody is waiting.
    let _ = CHANGES.send(request_id.to_string());
}

/// Whether a filing payload asks its call to wait. Absent, it waits for
/// `fenced_command` (whose agent wants the command's result) and not for the
/// kinds whose outcome is a wake, not an answer.
pub(crate) fn wait_requested(payload: &Value) -> Result<bool, String> {
    match payload.get("wait") {
        None => Ok(payload.get("kind").and_then(Value::as_str)
            == Some(RequestKind::FencedCommand.as_str())),
        Some(Value::Bool(wait)) => Ok(*wait),
        Some(other) => Err(format!(
            "Invalid request filing: `wait` must be true or false (whether this call holds for the request's outcome), not {other}"
        )),
    }
}

/// The filing record and the call's `wait` flag, with the flag removed so the
/// record's own `deny_unknown_fields` still refuses every other stray key.
pub(crate) fn split_filing(payload: &Value) -> Result<(FileOperatorRequest, bool), String> {
    let wait = wait_requested(payload)?;
    let mut filing = payload.clone();
    if let Some(object) = filing.as_object_mut() {
        object.remove("wait");
    }
    let input = serde_json::from_value(filing)
        .map_err(|error| format!("Invalid request filing: {error}"))?;
    Ok((input, wait))
}

/// The applied-change summary for a filing, waiting or not.
pub(crate) fn filed_summary(filed: &FiledOperatorRequest) -> String {
    format!(
        "{} operator request {}",
        if filed.created { "Filed" } else { "Joined" },
        filed.request.uri
    )
}

/// File a waiting request and hold the call for its outcome: the rendered
/// outcome when it arrives inside the inline budget, otherwise the hand-off
/// marker with the call parked. Fails only when nothing was filed.
pub(crate) async fn file_and_wait(
    orch: &Orchestrator,
    request: &CallbackRequest,
    payload: &Value,
) -> Result<(FiledOperatorRequest, String), String> {
    let (input, _) = split_filing(payload)?;
    let filed = super::file(orch, request, input).await?;
    // Attempts already settled when this call joined are some earlier
    // filer's outcome; only a later one answers this wait. A fresh request has
    // none. An attempt cannot settle between the filing's commit and this read
    // without an operator acceptance in between.
    let baseline = settled_attempts(&filed.request);
    let outcome = match hold_inline(
        orch,
        &filed.request.id,
        &baseline,
        INLINE_DURABLE_WAIT_BUDGET,
    )
    .await
    {
        Ok(Some(settled)) => scrubbed_outcome(&settled),
        Ok(None) => park(orch, request, &filed.request, baseline).await,
        Err(error) => not_waiting(&filed.request, &error),
    };
    Ok((filed, outcome))
}

/// Ids of the attempts that have already reported an outcome.
fn settled_attempts(request: &OperatorRequest) -> Vec<String> {
    request
        .attempts
        .iter()
        .filter(|attempt| attempt.outcome.is_some())
        .map(|attempt| attempt.id.clone())
        .collect()
}

/// Whether the request has reached an outcome this wait answers with. A
/// settled pre-effect failure reopens the request (its state is `open` again),
/// and an inconclusive attempt leaves it `failed`, so a newly reported attempt
/// ends the wait whatever state it left behind.
fn reached(request: &OperatorRequest, baseline: &[String]) -> bool {
    matches!(
        request.state,
        RequestState::Done
            | RequestState::Failed
            | RequestState::Declined
            | RequestState::Withdrawn
    ) || request
        .attempts
        .iter()
        .any(|attempt| attempt.outcome.is_some() && !baseline.contains(&attempt.id))
}

/// Await the request's outcome, level-triggered: the row is read first and
/// again on every change signal, so an outcome committed before this was
/// called, or across a restart, is found by the first read.
async fn outcome_of(
    db: &LocalDb,
    request_id: &str,
    baseline: &[String],
) -> Result<OperatorRequest, String> {
    // Subscribe before reading, so a change committed between the read and
    // the first receive is still signalled.
    let mut changes = CHANGES.subscribe();
    loop {
        let request = store::get_request(db, request_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("operator request {request_id} no longer exists"))?;
        if reached(&request, baseline) {
            return Ok(request);
        }
        loop {
            match changes.recv().await {
                Ok(changed) if changed == request_id => break,
                Ok(_) => {}
                // Missed signals may include ours: re-read rather than guess.
                Err(broadcast::error::RecvError::Lagged(_)) => break,
                Err(broadcast::error::RecvError::Closed) => {
                    return Err("request change signals stopped".into())
                }
            }
        }
    }
}

/// The outcome if it arrives within `budget`, `None` if it does not.
async fn hold_inline(
    orch: &Orchestrator,
    request_id: &str,
    baseline: &[String],
    budget: Duration,
) -> Result<Option<OperatorRequest>, String> {
    match tokio::time::timeout(budget, outcome_of(&orch.db.local, request_id, baseline)).await {
        Ok(outcome) => outcome.map(Some),
        Err(_) => Ok(None),
    }
}

/// The text a parked wait resolves with: the rendered outcome, or why it
/// could not be read. Also what startup reconciliation re-arms a pending row
/// with, which is why it takes only the condition's own fields.
pub(crate) async fn trigger(orch: &Orchestrator, request_id: &str, baseline: &[String]) -> String {
    match outcome_of(&orch.db.local, request_id, baseline).await {
        Ok(settled) => scrubbed_outcome(&settled),
        Err(error) => super::scrub_text(format!(
            "Operator request cairn://requests/{request_id} could not be read for its outcome: {error}. Read the request for its decision and report."
        )),
    }
}

/// Resolve a parked request wait once its request reaches an outcome.
pub(crate) async fn resolve_parked(
    orch: Orchestrator,
    db: std::sync::Arc<LocalDb>,
    record: Record,
) {
    let Condition::OperatorRequest {
        request_id,
        settled_attempts,
    } = &record.condition
    else {
        return;
    };
    let result = trigger(&orch, request_id, settled_attempts).await;
    durable_suspend::drive_resolution(&orch, &db, &record, &result, false).await;
}

/// Park the filing call on its request's outcome. The row is written and the
/// turn yielded before anything that can resolve it is armed (see
/// `durable_suspend`), and the resolver re-reads the request after the park,
/// so an outcome landing during the hand-off is still found.
async fn park(
    orch: &Orchestrator,
    request: &CallbackRequest,
    filed: &OperatorRequest,
    baseline: Vec<String>,
) -> String {
    let (ctx, db) =
        match crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request).await {
            Ok(found) => found,
            Err(error) => return not_waiting(filed, &error),
        };
    let Some(turn_id) = orch.process_state.get_current_turn_id(&ctx.run_id) else {
        return not_waiting(filed, "the filing call has no active turn to park");
    };
    let session_id = match durable_suspend::run_session(&db, &ctx.run_id).await {
        Ok(Some(session)) => session,
        Ok(None) => return not_waiting(filed, "the filing call has no active session"),
        Err(error) => return not_waiting(filed, &error),
    };
    let tool_use_id = match request.tool_use_id.clone() {
        Some(id) => id,
        None => match claim_tool_use_id(&db, &ctx.run_id, &turn_id, files_a_request).await {
            Claim::One(id) => id,
            Claim::None => {
                return not_waiting(filed, "its write call could not be found in this turn")
            }
            Claim::Ambiguous(_) => {
                return not_waiting(
                    filed,
                    "several identical write calls of this turn file requests, and answering one would risk answering another",
                )
            }
        },
    };
    let record = Record {
        id: cairn_common::ids::mint_child(&ctx.run_id),
        job_id: ctx.job_id,
        run_id: ctx.run_id,
        session_id,
        turn_id,
        tool_use_id,
        condition: Condition::OperatorRequest {
            request_id: filed.id.clone(),
            settled_attempts: baseline,
        },
        deadline: None,
        created: chrono::Utc::now().timestamp_millis(),
    };
    let handoff = match durable_suspend::suspend(orch, &db, &record).await {
        Ok(handoff) => handoff,
        Err(error) => return not_waiting(filed, &error),
    };
    let orch = orch.clone();
    tokio::spawn(async move {
        if !handoff.parked().await {
            log::warn!(
                "request wait {} was never parked; leaving it for startup reconciliation",
                record.id
            );
            return;
        }
        resolve_parked(orch, db, record).await;
    });
    REQUEST_WAIT_SUSPENDED_MARKER.to_string()
}

/// The `write` invocation a waiting filing came from, for a transport that
/// carries no tool-use id.
fn files_a_request(name: &str, input: &Value) -> bool {
    (name == "write" || name.ends_with("__write"))
        && input
            .get("changes")
            .and_then(Value::as_array)
            .is_some_and(|changes| {
                changes.iter().any(|item| {
                    item.get("mode").and_then(Value::as_str) == Some("append")
                        && item
                            .get("target")
                            .and_then(Value::as_str)
                            .and_then(cairn_uri::parse_uri)
                            .is_some_and(|target| {
                                matches!(target, cairn_uri::CairnResource::Requests)
                            })
                        && item
                            .get("payload")
                            .is_some_and(|payload| wait_requested(payload) == Ok(true))
                })
            })
}

/// What the filer is told when the request is filed but the call cannot wait:
/// the request stands, and its outcome still reaches the filer as a wake.
fn not_waiting(filed: &OperatorRequest, why: &str) -> String {
    log::warn!("waiting filing of {} could not wait: {why}", filed.uri);
    super::scrub_text(format!(
        "Operator request {} is filed and pending, but this call could not wait for it ({why}). Its outcome reaches you as a wake when it settles.",
        filed.uri
    ))
}

fn scrubbed_outcome(request: &OperatorRequest) -> String {
    super::scrub_text(render_outcome(request))
}

/// The outcome a waiting filer resumes with, rendered from the request's typed
/// record: a decline's reason, a withdrawal, or the newest reported attempt.
pub(crate) fn render_outcome(request: &OperatorRequest) -> String {
    let uri = &request.uri;
    match request.state {
        RequestState::Declined => {
            let reason = request
                .report
                .as_ref()
                .and_then(|report| report.get("reason"))
                .and_then(Value::as_str)
                .unwrap_or("no reason was recorded");
            return format!(
                "Operator request {uri} was declined by the operator; nothing ran. Their reason: {reason}"
            );
        }
        RequestState::Withdrawn => {
            return format!("Operator request {uri} was withdrawn before it ran; nothing ran.")
        }
        _ => {}
    }
    // Attempts come oldest first, so the last reported one is this outcome.
    let Some((attempt, outcome)) = request
        .attempts
        .iter()
        .rev()
        .find_map(|attempt| attempt.outcome.as_ref().map(|outcome| (attempt, outcome)))
    else {
        return format!(
            "Operator request {uri} is {}; no attempt has reported an outcome.",
            request.state.as_str()
        );
    };
    let report = attempt.report.as_ref().or(request.report.as_ref());
    let evidence = report.and_then(|report| report.get("evidence"));
    let fenced = (request.kind == RequestKind::FencedCommand)
        .then(|| {
            evidence
                .cloned()
                .map(serde_json::from_value::<FencedCommandReport>)
        })
        .flatten()
        .and_then(Result::ok);
    let mut text = match (fenced, evidence) {
        (Some(fenced), _) => render_fenced_command(uri, &fenced, outcome),
        (None, Some(evidence)) => format!(
            "Operator request {uri} {}. Its report:\n```json\n{}\n```",
            outcome_phrase(outcome),
            serde_json::to_string_pretty(evidence).unwrap_or_else(|_| evidence.to_string())
        ),
        (None, None) => {
            let error = report
                .and_then(|report| report.get("error"))
                .and_then(Value::as_str);
            let phase = report
                .and_then(|report| report.get("phase"))
                .and_then(Value::as_str);
            match (error, phase) {
                (Some(error), Some("before_claim" | "before_dispatch")) => format!(
                    "Operator request {uri} failed before it ran; nothing ran. Reason: {error}"
                ),
                (Some(error), _) => format!(
                    "Operator request {uri} {}. Reason: {error}",
                    outcome_phrase(outcome)
                ),
                (None, _) => format!("Operator request {uri} {}.", outcome_phrase(outcome)),
            }
        }
    };
    if *outcome == RequestAttemptOutcome::Inconclusive {
        text.push_str(
            "\nThe attempt is inconclusive: whether the act took effect is unknown, and it is never retried automatically.",
        );
    }
    if request.state == RequestState::Open {
        text.push_str(
            "\nThe request reopened for a fresh acceptance: it runs only if the operator accepts it again.",
        );
    }
    text
}

fn outcome_phrase(outcome: &RequestAttemptOutcome) -> &'static str {
    match outcome {
        RequestAttemptOutcome::Done => "is done",
        RequestAttemptOutcome::Failed => "failed",
        RequestAttemptOutcome::RolledBack => "was rolled back",
        RequestAttemptOutcome::Cancelled => "was cancelled",
        RequestAttemptOutcome::Inconclusive => "is inconclusive",
    }
}

fn render_fenced_command(
    uri: &str,
    report: &FencedCommandReport,
    outcome: &RequestAttemptOutcome,
) -> String {
    let headline = if !report.ran && *outcome == RequestAttemptOutcome::Inconclusive {
        "whether the command ran is unknown".to_string()
    } else if !report.ran {
        "the act did not run".to_string()
    } else if report.timed_out {
        "the act ran and the command timed out".to_string()
    } else {
        match report.exit_code {
            Some(code) => format!("the act ran and the command exited {code}"),
            None => "the act ran and the command reported no exit code".to_string(),
        }
    };
    let mut lines = vec![format!("Operator request {uri}: {headline}.")];
    if let Some(ms) = report.duration_ms {
        lines.push(format!("Duration: {}", duration(ms)));
    }
    if let Some(executor) = &report.executor {
        lines.push(format!(
            "Executor: {} (build {}, connection generation {})",
            executor.name, executor.executor_build_id, executor.connection_generation
        ));
    }
    if let Some(detail) = report.detail.as_deref().filter(|detail| !detail.is_empty()) {
        lines.push(format!("Detail: {detail}"));
    }
    let output = report.output.trim_end_matches('\n');
    if !output.is_empty() {
        let fence = fence_for(output);
        lines.push(format!("Output (tail):\n{fence}text\n{output}\n{fence}"));
    } else if report.ran {
        lines.push("The command printed no output.".to_string());
    }
    lines.join("\n")
}

fn duration(ms: u64) -> String {
    if ms < 1_000 {
        format!("{ms} ms")
    } else {
        format!("{:.1} s", ms as f64 / 1_000.0)
    }
}

/// A backtick fence longer than any backtick run in `text`, so command output
/// can never close the block it is quoted in.
fn fence_for(text: &str) -> String {
    let longest = text.split(|c| c != '`').map(str::len).max().unwrap_or(0);
    "`".repeat(longest.max(2) + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::requests::{FencedCommandExecutorReport, RequestAttempt};
    use serde_json::json;

    fn request(kind: RequestKind, state: RequestState) -> OperatorRequest {
        serde_json::from_value(json!({
            "id": "r1", "uri": "cairn://requests/r1", "workspace_id": "default",
            "cohort_id": "c1", "member_request_ids": ["r1"], "kind": kind,
            "target": null, "target_identity": "machine", "coalescing_key": null,
            "revision": 1, "state": state, "created_at": 1, "updated_at": 1,
            "current_attempt": null, "fulfilled_by_attempt": null, "report": null,
            "requesters": [], "facts": null, "attempts": [], "available": true,
            "unavailable_reason": null
        }))
        .unwrap()
    }

    /// An attempt with an outcome and report. The renderer never reads its
    /// acceptance, which is only well-formed enough to construct.
    fn with_attempt(
        mut request: OperatorRequest,
        id: &str,
        outcome: RequestAttemptOutcome,
        report: Value,
    ) -> OperatorRequest {
        let attempt: RequestAttempt = RequestAttempt {
            id: id.into(),
            authorization_event_id: format!("event-{id}"),
            acceptance: acceptance(),
            state: if outcome == RequestAttemptOutcome::Done {
                RequestState::Done
            } else {
                RequestState::Failed
            },
            claim_id: Some("claim".into()),
            outcome: Some(outcome),
            report: Some(report.clone()),
            updated_at: 2,
        };
        request.report = Some(report);
        request.attempts.push(attempt);
        request
    }

    fn acceptance() -> cairn_common::requests::RequestAcceptance {
        use cairn_common::identity::*;
        let principal = PrincipalRef::Machine {
            device_id: "test-device".into(),
        };
        let verification = VerificationRecord::new(
            VerificationMethod::DesktopCredential,
            VerificationStatus::Verified,
            None,
            None,
            None,
            Some(CredentialRef::new("desktop_operator_credential").unwrap()),
            VerificationStrength::new("local_shared_secret").unwrap(),
            1,
        )
        .unwrap();
        let evidence = AppearanceEvidence::new(
            AppearanceTransport::AuthenticatedDesktop,
            Address::Desktop {
                device_id: "test-device".into(),
            },
            verification,
            1,
            None,
        )
        .unwrap();
        cairn_common::requests::RequestAcceptance {
            schema_version: 1,
            install_id: "install".into(),
            workspace_id: "default".into(),
            cohort_id: "c1".into(),
            request_ids: vec!["r1".into()],
            attempt_id: "a1".into(),
            actor_id: "actor".into(),
            kind: RequestKind::FencedCommand,
            target: None,
            target_identity: "machine".into(),
            binding: cairn_common::authorization::ActBinding::fresh(
                &super::super::registry::act_digest(RequestKind::FencedCommand, None, None),
            ),
            facts_version: 1,
            revision: 1,
            candidate_commit: None,
            base_commit: None,
            policy_project_id: None,
            drain: None,
            fenced_command: None,
            operator: principal.clone(),
            appearance: AppearanceSnapshot::new(principal, evidence, vec![], None).unwrap(),
            accepted_at: 1,
        }
    }

    fn fenced(ran: bool, exit_code: Option<i32>, output: &str) -> Value {
        json!({"evidence": FencedCommandReport {
            ran,
            exit_code,
            timed_out: false,
            duration_ms: Some(2_500),
            output: output.into(),
            executor: Some(FencedCommandExecutorReport {
                executor_id: "exec-id".into(),
                device_id: "device".into(),
                name: "bbs".into(),
                executor_build_id: "build-7".into(),
                connection_generation: 3,
                hostname: None,
            }),
            cell_request_id: "cell".into(),
            cell_attempt_id: "cell-attempt".into(),
            detail: Some("scratch written back".into()),
        }, "builtCommit": null})
    }

    #[test]
    fn a_fenced_command_report_renders_the_run_the_executor_and_the_output_tail() {
        let done = with_attempt(
            request(RequestKind::FencedCommand, RequestState::Done),
            "a1",
            RequestAttemptOutcome::Done,
            fenced(true, Some(0), "ok 12 tests\n"),
        );
        let text = render_outcome(&done);
        for expected in [
            "cairn://requests/r1: the act ran and the command exited 0.",
            "Duration: 2.5 s",
            "Executor: bbs (build build-7, connection generation 3)",
            "Detail: scratch written back",
            "```text\nok 12 tests\n```",
        ] {
            assert!(text.contains(expected), "{expected} missing from:\n{text}");
        }
    }

    #[test]
    fn a_non_zero_exit_is_an_act_that_ran_and_output_cannot_close_its_fence() {
        let failed = with_attempt(
            request(RequestKind::FencedCommand, RequestState::Done),
            "a1",
            RequestAttemptOutcome::Done,
            fenced(true, Some(101), "```\nerror[E0425]"),
        );
        let text = render_outcome(&failed);
        assert!(
            text.contains("the act ran and the command exited 101."),
            "{text}"
        );
        assert!(text.contains("````text\n```\nerror[E0425]\n````"), "{text}");
    }

    #[test]
    fn a_decline_returns_the_operators_reason() {
        let mut declined = request(RequestKind::FencedCommand, RequestState::Declined);
        declined.report = Some(json!({"decision":"declined","reason":"run it on main instead"}));
        let text = render_outcome(&declined);
        assert!(text.contains("declined"), "{text}");
        assert!(text.contains("run it on main instead"), "{text}");
        assert!(text.contains("nothing ran"), "{text}");
    }

    #[test]
    fn a_pre_effect_failure_returns_its_reason_and_says_the_request_reopened() {
        let reopened = with_attempt(
            request(RequestKind::FencedCommand, RequestState::Open),
            "a1",
            RequestAttemptOutcome::Failed,
            json!({"phase":"before_dispatch","error":"the branch head moved"}),
        );
        let text = render_outcome(&reopened);
        assert!(
            text.contains("failed before it ran; nothing ran. Reason: the branch head moved"),
            "{text}"
        );
        assert!(text.contains("reopened for a fresh acceptance"), "{text}");
    }

    #[test]
    fn an_inconclusive_attempt_says_its_effects_are_unknown_and_not_retried() {
        let lost = with_attempt(
            request(RequestKind::FencedCommand, RequestState::Failed),
            "a1",
            RequestAttemptOutcome::Inconclusive,
            json!({"phase":"dispatch","error":"link lost"}),
        );
        let text = render_outcome(&lost);
        assert!(
            text.contains("is inconclusive. Reason: link lost"),
            "{text}"
        );
        assert!(text.contains("never retried automatically"), "{text}");
        assert!(!text.contains("reopened"), "{text}");
    }

    #[test]
    fn only_an_attempt_reported_after_filing_ends_the_wait() {
        let open = request(RequestKind::FencedCommand, RequestState::Open);
        assert!(!reached(&open, &[]));
        let reopened = with_attempt(
            open,
            "earlier",
            RequestAttemptOutcome::Failed,
            json!({"phase":"before_claim","error":"executor gone"}),
        );
        assert!(
            !reached(&reopened, &["earlier".into()]),
            "a joiner waits past an older failure"
        );
        assert!(
            reached(&reopened, &[]),
            "the filer of that attempt is answered by it"
        );
        for state in [
            RequestState::Done,
            RequestState::Failed,
            RequestState::Declined,
            RequestState::Withdrawn,
        ] {
            assert!(
                reached(&request(RequestKind::Rebuild, state), &[]),
                "{state:?}"
            );
        }
        for state in [RequestState::Accepted, RequestState::Acting] {
            assert!(
                !reached(&request(RequestKind::Rebuild, state), &[]),
                "{state:?}"
            );
        }
    }

    #[test]
    fn wait_defaults_by_kind_is_stripped_and_a_non_boolean_is_refused_by_name() {
        let filing = |extra: Value| {
            let mut payload = json!({"kind":"rebuild","rationale":"r"});
            payload
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            payload
        };
        assert_eq!(wait_requested(&json!({"kind":"fenced_command"})), Ok(true));
        assert_eq!(wait_requested(&json!({"kind":"rebuild"})), Ok(false));
        assert_eq!(
            wait_requested(&json!({"kind":"fenced_command","wait":false})),
            Ok(false)
        );
        assert_eq!(
            wait_requested(&json!({"kind":"rebuild","wait":true})),
            Ok(true)
        );
        let (input, wait) = split_filing(&filing(json!({"wait":true}))).unwrap();
        assert!(wait);
        assert_eq!(input.kind, RequestKind::Rebuild);
        for bad in [json!("yes"), json!(1), json!(null), json!({"until":"done"})] {
            let error = split_filing(&filing(json!({ "wait": bad }))).unwrap_err();
            assert!(error.contains("`wait`"), "{error}");
        }
        // Only `wait` is a call flag; every other stray key still fails the record.
        assert!(split_filing(&filing(json!({"wait":false,"status":"done"}))).is_err());
    }

    #[test]
    fn the_hand_off_matcher_names_only_a_waiting_requests_append() {
        let call = |payload: Value| json!({"changes":[{"target":"cairn://requests","mode":"append","payload":payload}]});
        assert!(files_a_request(
            "mcp__cairn__write",
            &call(json!({"kind":"fenced_command"}))
        ));
        assert!(!files_a_request(
            "mcp__cairn__write",
            &call(json!({"kind":"fenced_command","wait":false}))
        ));
        assert!(!files_a_request(
            "mcp__cairn__run",
            &call(json!({"kind":"fenced_command"}))
        ));
    }

    async fn seeded() -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let local = LocalDb::open_migrated_for_test(root.join("wait.db"))
            .await
            .unwrap();
        local
            .execute_script(
                "INSERT INTO request_cohorts(id,workspace_id,kind,state,created_at,updated_at)
                 VALUES('wait-cohort','default','rebuild','open',1,1);
                 INSERT INTO requests(id,workspace_id,kind,target_identity,cohort_id,state,created_at,updated_at)
                 VALUES('wait-request','default','rebuild','install','wait-cohort','open',1,1);",
            )
            .await
            .unwrap();
        let search = std::sync::Arc::new(
            crate::storage::SearchIndex::open_or_create(root.join("search")).unwrap(),
        );
        Orchestrator::builder(
            std::sync::Arc::new(crate::db::DbState::new(std::sync::Arc::new(local), search)),
            std::sync::Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            root,
        )
        .build()
    }

    async fn decline_and_deliver(orch: &Orchestrator) {
        orch.db
            .local
            .execute_script(
                "UPDATE requests SET state='declined',report_json='{\"decision\":\"declined\",\"reason\":\"not on a Friday\"}' WHERE id='wait-request';
                 INSERT INTO request_outbox(id,request_id,kind,payload_json,created_at)
                 VALUES('wait-changed','wait-request','changed','{}',1);",
            )
            .await
            .unwrap();
        super::super::delivery::drain_once(orch).await.unwrap();
    }

    #[tokio::test]
    async fn a_held_call_returns_the_outcome_when_its_change_is_delivered() {
        let orch = seeded().await;
        let held = {
            let orch = orch.clone();
            tokio::spawn(async move {
                hold_inline(&orch, "wait-request", &[], Duration::from_secs(30)).await
            })
        };
        // The waiter subscribes before its first read, so the change may land
        // at any point after the spawn without being missed.
        tokio::task::yield_now().await;
        decline_and_deliver(&orch).await;
        let settled = tokio::time::timeout(Duration::from_secs(10), held)
            .await
            .expect("the delivered change wakes the held call")
            .unwrap()
            .unwrap()
            .expect("an outcome inside the budget");
        assert!(render_outcome(&settled).contains("not on a Friday"));
    }

    #[tokio::test]
    async fn an_already_settled_request_returns_at_once_and_an_open_one_hands_off_at_the_budget() {
        let orch = seeded().await;
        let open = hold_inline(&orch, "wait-request", &[], Duration::from_millis(50))
            .await
            .unwrap();
        assert!(open.is_none(), "an open request outlives a short budget");
        decline_and_deliver(&orch).await;
        let settled = tokio::time::timeout(
            Duration::from_secs(1),
            hold_inline(&orch, "wait-request", &[], Duration::from_secs(30)),
        )
        .await
        .expect("a settled request needs no signal")
        .unwrap();
        assert_eq!(settled.unwrap().state, RequestState::Declined);
    }
}
