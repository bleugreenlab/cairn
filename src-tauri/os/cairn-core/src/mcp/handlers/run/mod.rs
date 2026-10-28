//! Run command MCP handler.
//!
//! Routes synchronous shell commands, skill-script targets, and proxied MCP
//! tool calls through inline batch execution. The submodules split the handler
//! by seam: [`types`] (payload/outcome shapes), [`resolve`] (item -> spec),
//! [`process`] (spawn/stream/timeout), [`output`] (result composition),
//! [`sandbox_policy`] (OS confinement), [`commit_barrier`] (worktree==HEAD),
//! [`checks`] (when:write check runners), [`hygiene`] (cwd advisories), and
//! [`redact`] (secret redaction). [`handle_run`] wires them together.

mod checks;
mod commit_barrier;
pub(crate) mod held;
mod hygiene;
mod output;
mod process;
mod redact;
mod resolve;
mod sandbox_policy;
mod search;
mod tip;
mod types;

pub(crate) use checks::{check_stream_id, run_item_stream_id, CheckExecResult};
pub(crate) use process::{cache_checkpoint_callback, MAX_BUFFER_SIZE};
pub(crate) use redact::redact_command;
pub(crate) use sandbox_policy::{resolve_run_confinement, RunCheckout};
pub use types::{
    CheckStatusEntry, CheckStatusPayload, ChecksWaitEvent, ChecksWaitKind, RunCompletePayload,
    RunItem, RunItemPayload, RunOutputPayload, RunPayload, TerminalWaitEvent, TerminalWaitKind,
    WaitDuration, WaitFor,
};

use crate::mcp::vcs::{acquire_store_lock, STORE_LOCK_TIMEOUT};
use crate::orchestrator::JjStoreOperationTrace;
use crate::stack::off_frame;
use crate::storage::RowExt;
use commit_barrier::{run_commit_barrier, CommitBarrierOutcome};
use std::path::Path;

/// A batch that never ran: nothing executed and nothing was committed.
pub(crate) const RUN_NOT_EXECUTED: &str = "This run could not execute";
/// A batch whose commands ran but whose changes could not be committed.
pub(crate) const RUN_NOT_PUBLISHED: &str =
    "This run's commands ran but their changes could not be published";
/// A batch handed to a machine that never answered for it, so whether its
/// commands ran is not known.
pub(crate) const RUN_OUTCOME_UNKNOWN: &str = "This run's outcome is unknown";
/// A batch abandoned before it finished.
pub(crate) const RUN_CANCELLED: &str = "Run cancelled";

/// Openings that mark a run envelope as a failure of the run itself rather than a
/// command result.
///
/// A run envelope carries only composed text, so a caller that consumes runs
/// programmatically — action runs, in [`crate::execution::actions`] — can only
/// classify an outcome by reading it. That makes this wording a contract between
/// the two surfaces rather than free prose: reword the constants above, never the
/// literal at a use site, or a failed run silently reads as a successful one.
pub(crate) const RUN_FAILURE_OPENINGS: &[&str] = &[
    RUN_NOT_EXECUTED,
    RUN_OUTCOME_UNKNOWN,
    RUN_NOT_PUBLISHED,
    RUN_CANCELLED,
];

/// Whether a composed run envelope reports a failure of the run itself.
pub(crate) fn envelope_reports_run_failure(text: &str) -> bool {
    RUN_FAILURE_OPENINGS
        .iter()
        .any(|opening| text.starts_with(opening))
}

fn batch_is_suspended(outcomes: &[ItemOutcome]) -> bool {
    outcomes.iter().any(|outcome| outcome.suspended)
}

async fn run_transcript_event_uri(
    orch: &Orchestrator,
    db: &crate::storage::LocalDb,
    context: &crate::mcp::handlers::RunContext,
    transport_tool_use_id: Option<&str>,
    payload: &serde_json::Value,
) -> Option<String> {
    let tool_use_id = match transport_tool_use_id {
        Some(id) => id.to_string(),
        None => {
            let turn_id =
                super::durable_suspend::suspending_turn_id(orch, db, &context.run_id).await?;
            match claim_batch_tool_use_id(db, &context.run_id, &turn_id, payload).await {
                Claim::One(id) => id,
                Claim::None | Claim::Ambiguous(_) => return None,
            }
        }
    };
    let run_id = context.run_id.clone();
    let sequence = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT sequence, data FROM events WHERE run_id = ?1 AND event_type = 'assistant' ORDER BY sequence DESC LIMIT 8",
                        (run_id.as_str(),),
                    )
                    .await?;
                let mut matched = None;
                while let Some(row) = rows.next().await? {
                    let Ok(value) = serde_json::from_str::<serde_json::Value>(&row.text(1)?) else { continue };
                    let contains = value.get("toolUses").and_then(|value| value.as_array()).is_some_and(|tools| {
                        tools.iter().any(|tool| tool.get("id").or_else(|| tool.get("toolUseId")).and_then(|value| value.as_str()) == Some(tool_use_id.as_str()))
                    });
                    if contains {
                        if matched.is_some() { return Ok(None); }
                        matched = Some(row.i64(0)? as i32);
                    }
                }
                Ok(matched)
            })
        })
        .await
        .ok()
        .flatten()?;
    super::write::build_current_event_uri(db, &context.run_id, sequence).await
}

/// A failure of the run itself rather than of a command inside it.
///
/// Every such failure is composed here rather than at its return site. That is
/// what makes [`envelope_reports_run_failure`] exhaustive: an action run can only
/// classify an outcome by reading the envelope, so a site that formats its own
/// opening silently reports a failed run as a successful one. Adding a failure
/// mode means adding a variant, and the guard test matches exhaustively.
pub(crate) enum RunFailure {
    /// Nothing executed; there is nothing to inspect and nothing was committed.
    /// Only for a batch no attempt of which was ever granted to a machine.
    NotExecuted(String),
    /// The batch never produced a result and the record of whether it reached a
    /// machine could not be read. Says what `NotExecuted` says, minus the claim
    /// that nothing ran: that claim is only as good as the record behind it.
    Unaccounted(String),
    /// Granted to a machine that never answered for it. Its commands may have
    /// run, so this never says they did not.
    DispatchedWithoutResult(BatchWithoutResult),
    /// Commands ran, but their result could not be accepted or committed.
    NotPublished(String),
    /// Abandoned before it finished. The detail names why when the abandonment
    /// was the runner's own decision (the batch ceiling); a user stop or an
    /// executor cancel carries none.
    Cancelled(Option<String>),
}

impl RunFailure {
    /// The agent-facing text, always opening with one of [`RUN_FAILURE_OPENINGS`].
    pub(crate) fn text(&self) -> String {
        match self {
            Self::NotExecuted(detail) => {
                format!("{RUN_NOT_EXECUTED}: {detail} No commands ran.")
            }
            Self::Unaccounted(detail) => format!(
                "{RUN_NOT_EXECUTED}: {detail} Whether any of its commands started could not be determined, so check the working tree before re-running it."
            ),
            Self::DispatchedWithoutResult(batch) => format!(
                "{RUN_OUTCOME_UNKNOWN}: {}",
                batch.describe(crate::clock::host(), crate::fleet::unix_time_ms())
            ),
            Self::NotPublished(detail) => format!("{RUN_NOT_PUBLISHED}: {detail}"),
            Self::Cancelled(None) => format!("{RUN_CANCELLED} before it finished."),
            Self::Cancelled(Some(detail)) => format!("{RUN_CANCELLED}: {detail}"),
        }
    }
}

/// A batch granted to a machine that produced no result for it.
pub(crate) struct BatchWithoutResult {
    grant: crate::fleet::GrantWithoutResult,
    /// What ended the wait, in the words of whoever composed the outcome.
    detail: String,
    /// Whether the batch carried a `commit_msg`, which was never applied.
    commits: bool,
}

impl BatchWithoutResult {
    /// Everything after the opening: where the batch went and when, what
    /// happened to that machine's connection since, what ended the wait, and
    /// what to check before running it again.
    ///
    /// The clock and the present are parameters so the text can be pinned in a
    /// test; production passes the host clock and the current instant.
    fn describe(&self, clock: &crate::clock::HostClock, now_unix_ms: u64) -> String {
        let grant = &self.grant;
        let mut text = format!(
            "it was handed to **{}** {}, and no result came back.",
            grant.executor_name,
            clock.age_millis(grant.granted_at_unix_ms as i64, now_unix_ms as i64),
        );
        // Drops, not reconnections: the claim's history records each link that
        // ended under it, and a link can end and stay down.
        match grant.connection_drops {
            None => {}
            Some(0) => text.push_str(" That machine's connection stayed up the whole time."),
            Some(1) => {
                text.push_str(" That machine's connection dropped once while the batch was there.")
            }
            Some(count) => text.push_str(&format!(
                " That machine's connection dropped {count} times while the batch was there."
            )),
        }
        let detail = self.detail.trim();
        text.push_str(" What ended the wait: ");
        text.push_str(detail);
        if !detail.ends_with(['.', '!', '?']) {
            text.push('.');
        }
        text.push_str(
            " Its commands may have run, partly run, or still be running there, so check the working tree before re-running it.",
        );
        if self.commits {
            text.push_str(
                " Its `commit_msg` was never applied, so nothing was committed on its behalf.",
            );
        }
        text
    }
}

/// The failure for a batch that produced no result: a plain refusal when no
/// attempt of it was ever granted to a machine, and the dispatched account when
/// one was.
fn unexecuted_failure(
    detail: String,
    grant_without_result: Option<crate::fleet::GrantWithoutResult>,
    commits: bool,
) -> RunFailure {
    match grant_without_result {
        Some(grant) => {
            log::warn!(
                "run batch granted to {} got no result from it: {detail}",
                grant.executor_name
            );
            RunFailure::DispatchedWithoutResult(BatchWithoutResult {
                grant,
                detail,
                commits,
            })
        }
        None => RunFailure::NotExecuted(detail),
    }
}

/// The failure for every routed outcome except a completion, which carries a
/// result to settle and answers `None`.
///
/// One function, so no arm can compose its own "nothing ran". Every outcome
/// that ends without a result goes through [`unexecuted_failure`] and with it
/// the account of any attempt that was granted.
fn failure_without_result(
    outcome: &CellOutcome,
    grant_without_result: Option<crate::fleet::GrantWithoutResult>,
    commits: bool,
    ceiling: Option<String>,
) -> Option<RunFailure> {
    match outcome {
        CellOutcome::Completed { .. } => None,
        // The typed reason names executor internals an agent cannot act on, so
        // it goes to the log; the agent gets the fact that decides its next
        // move, which is first of all whether a machine was handed this batch.
        CellOutcome::Unavailable { reason, diagnostic } => {
            log::warn!("run batch ended without a result ({reason:?}): {diagnostic}");
            Some(unexecuted_failure(
                format!("{diagnostic}."),
                grant_without_result,
                commits,
            ))
        }
        // Only an executor reports this, so it always follows a grant. Sealing
        // a command and uploading its delta are among its stages, so without
        // the grant's account it still may not say nothing ran.
        CellOutcome::StorageFailure {
            stage,
            kind,
            diagnostic,
            ..
        } => {
            log::warn!("run batch failed on executor storage ({stage:?}/{kind:?}): {diagnostic}");
            let detail = format!("{diagnostic}.");
            Some(match grant_without_result {
                Some(grant) => unexecuted_failure(detail, Some(grant), commits),
                None => RunFailure::Unaccounted(detail),
            })
        }
        CellOutcome::FailedAfterExecution { diagnostic, .. } => {
            Some(RunFailure::NotPublished(format!("{diagnostic}.")))
        }
        CellOutcome::Cancelled { .. } => Some(RunFailure::Cancelled(ceiling)),
    }
}

/// The failure for a batch whose placement task itself died. Its record of
/// what it dispatched died with it, so this cannot say whether anything ran.
fn placement_task_failure(error: &tokio::task::JoinError) -> RunFailure {
    RunFailure::Unaccounted(format!("its batch did not run to completion ({error})."))
}

#[cfg(test)]
mod failure_contract_tests {
    use super::*;

    /// One sample per [`RunFailure`] variant, behind an exhaustive `match` so a new
    /// failure mode cannot be added without extending this list. That is the point:
    /// action runs classify outcomes by reading envelope text, so a failure mode
    /// that bypasses composition reports a failed run as a successful one, with the
    /// failure text sitting in `stdout` beside exit code 0.
    fn every_failure_variant() -> Vec<RunFailure> {
        match &RunFailure::Cancelled(None) {
            RunFailure::NotExecuted(_)
            | RunFailure::Unaccounted(_)
            | RunFailure::DispatchedWithoutResult(_)
            | RunFailure::NotPublished(_)
            | RunFailure::Cancelled(_) => {}
        }
        vec![
            RunFailure::NotExecuted("its environment could not be reached (timeout).".to_string()),
            RunFailure::Unaccounted("its batch did not run to completion (panic).".to_string()),
            RunFailure::DispatchedWithoutResult(BatchWithoutResult {
                grant: specimen_grant(Some(2)),
                detail: SPECIMEN_SILENCE.to_string(),
                commits: true,
            }),
            RunFailure::DispatchedWithoutResult(BatchWithoutResult {
                grant: specimen_grant(None),
                detail: "cache checkpoint callback: runner refused.".to_string(),
                commits: false,
            }),
            RunFailure::NotPublished("their result could not be read back (eof).".to_string()),
            RunFailure::Cancelled(None),
            RunFailure::Cancelled(Some(RUN_CEILING_DETAIL.to_string())),
            RunFailure::NotPublished(run_batch_lost_to_restart_text(true)),
        ]
    }

    #[test]
    fn every_composed_failure_is_classified_as_a_failure() {
        for failure in every_failure_variant() {
            let text = failure.text();
            assert!(
                envelope_reports_run_failure(&text),
                "unclassified failure envelope: {text}"
            );
        }
        assert!(!envelope_reports_run_failure(
            "=== bun test ===\n12 passed\nExit code: 0"
        ));
        assert!(!envelope_reports_run_failure(
            "Note: this checkout is not yours to commit to, so the commands ran but nothing was committed."
        ));
    }

    /// Each opening appears only in this module's own plumbing: its definition,
    /// its entry in [`RUN_FAILURE_OPENINGS`], and its uses in
    /// [`RunFailure::text`]. A new return site that formats its own opening — the
    /// way every failure site did before they were unified — raises the count and
    /// fails here, because such a site would bypass action-run classification.
    #[test]
    fn no_site_composes_a_failure_opening_of_its_own() {
        // Everything above this test module, so the identifiers named in the
        // assertion below do not count themselves.
        let production = include_str!("mod.rs")
            .split("mod failure_contract_tests")
            .next()
            .expect("module source splits at its own test module");
        for identifier in [
            "RUN_NOT_EXECUTED",
            "RUN_OUTCOME_UNKNOWN",
            "RUN_NOT_PUBLISHED",
            "RUN_CANCELLED",
        ] {
            // `RUN_CANCELLED` and `RUN_NOT_EXECUTED` each serve two variants.
            let expected = match identifier {
                "RUN_CANCELLED" | "RUN_NOT_EXECUTED" => 4,
                _ => 3,
            };
            assert_eq!(
                production.matches(identifier).count(),
                expected,
                "{identifier} is used outside RunFailure::text; compose failures through RunFailure \
                 so action runs still classify them as failures"
            );
        }
    }

    /// 2025-07-12 21:40 PDT.
    const SPECIMEN_GRANTED_AT: u64 = 1_752_381_600_000;

    /// The cairn/4912 specimen's shape: a batch granted to a remote machine
    /// that then stopped accounting for it.
    fn specimen_grant(connection_drops: Option<usize>) -> crate::fleet::GrantWithoutResult {
        crate::fleet::GrantWithoutResult {
            executor_name: "bgsrv-ub".to_string(),
            granted_at_unix_ms: SPECIMEN_GRANTED_AT,
            connection_drops,
        }
    }

    /// The shape of the diagnostic the runner composes when a claimed
    /// attempt's machine stops accounting for it.
    const SPECIMEN_SILENCE: &str = "bgsrv-ub gave no account of request r attempt a for 10m00s: when the runner stopped waiting, the machine was not connected, and the last time it accounted for the attempt, it listed it as running a command.";

    /// A batch a machine was granted and never answered for names where it
    /// went, when, what became of that machine's connection, and the silence
    /// that ended the wait in the machine's own terms, and it never says
    /// nothing ran or that a budget ran out.
    #[test]
    fn a_granted_batch_that_never_answered_says_where_it_went_and_not_that_nothing_ran() {
        let clock = crate::clock::HostClock::fixed("America/Los_Angeles");
        let now = SPECIMEN_GRANTED_AT + (3 * 3_600 + 20 * 60) * 1_000;
        let text = BatchWithoutResult {
            grant: specimen_grant(Some(2)),
            detail: SPECIMEN_SILENCE.to_string(),
            commits: true,
        }
        .describe(&clock, now);

        assert!(text.contains("**bgsrv-ub**"), "{text}");
        assert!(text.contains("3h 20m ago (2025-07-12 21:40 PDT)"), "{text}");
        assert!(text.contains("dropped 2 times"), "{text}");
        assert!(
            text.contains(&format!("What ended the wait: {SPECIMEN_SILENCE}")),
            "the machine's silence is what ended the wait: {text}"
        );
        assert!(!text.contains("allowance"), "{text}");
        assert!(!text.contains("budget"), "{text}");
        assert!(text.contains("check the working tree"), "{text}");
        assert!(text.contains("`commit_msg` was never applied"), "{text}");
        assert!(!text.contains("No commands ran"), "{text}");
        assert!(!text.contains(&SPECIMEN_GRANTED_AT.to_string()), "{text}");
    }

    /// The history says how often the link dropped, which is what the claim's
    /// chain records, and never that it came back: a link can drop and stay
    /// down until the wait runs out.
    #[test]
    fn connection_history_is_stated_only_when_it_was_read() {
        let clock = crate::clock::HostClock::fixed("America/Los_Angeles");
        let describe = |drops| {
            BatchWithoutResult {
                grant: specimen_grant(drops),
                detail: String::new(),
                commits: false,
            }
            .describe(&clock, SPECIMEN_GRANTED_AT)
        };
        assert!(describe(Some(0)).contains("stayed up the whole time"));
        let once = describe(Some(1));
        assert!(once.contains("dropped once"), "{once}");
        for drops in [Some(1), Some(3)] {
            let text = describe(drops);
            assert!(!text.contains("re-established"), "{text}");
            assert!(!text.contains("reconnect"), "{text}");
        }
        let unread = describe(None);
        assert!(!unread.contains("connection"), "{unread}");
        assert!(!unread.contains("commit_msg"), "{unread}");
    }

    /// A machine that answered with a refusal after its grant gets its own
    /// words in the text, and the refusal is still not taken as proof that
    /// nothing ran.
    #[test]
    fn a_refusal_from_the_granted_machine_is_quoted_not_believed() {
        let clock = crate::clock::HostClock::fixed("America/Los_Angeles");
        let text = BatchWithoutResult {
            grant: specimen_grant(None),
            detail: "cache checkpoint callback: runner refused".to_string(),
            commits: false,
        }
        .describe(&clock, SPECIMEN_GRANTED_AT);
        assert!(
            text.contains("What ended the wait: cache checkpoint callback: runner refused."),
            "{text}"
        );
        assert!(!text.contains("allowance"), "{text}");
        assert!(text.contains("may have run"), "{text}");
    }

    fn storage_failure() -> CellOutcome {
        CellOutcome::StorageFailure {
            request_id: "r".into(),
            attempt_id: "a".into(),
            stage: cairn_common::executor_protocol::StorageFailureStage::CommandSeal,
            kind: cairn_common::executor_protocol::StorageFailureKind::NoSpace,
            diagnostic: "sealing the command's changes ran out of space".into(),
            slot_retired: false,
        }
    }

    /// Every outcome without a result is composed in one place, so the grant
    /// account an earlier attempt left cannot be dropped by whichever outcome
    /// the last attempt happened to end with. That is what a storage arm with
    /// its own "nothing ran" did.
    #[test]
    fn every_outcome_without_a_result_keeps_the_grant_account() {
        let spawn = CellOutcome::Unavailable {
            reason: cairn_common::executor_protocol::CellUnavailableReason::Spawn,
            diagnostic: "cache checkpoint callback: runner refused".into(),
        };
        for outcome in [storage_failure(), spawn] {
            let text = failure_without_result(&outcome, Some(specimen_grant(None)), false, None)
                .expect("an outcome without a result is a failure")
                .text();
            assert!(text.starts_with(RUN_OUTCOME_UNKNOWN), "{outcome:?}: {text}");
            assert!(!text.contains("No commands ran"), "{text}");
        }
    }

    /// Sealing a command and uploading its delta are storage stages, so a
    /// storage failure never says nothing ran, even with no grant account to
    /// tell it otherwise.
    #[test]
    fn a_storage_failure_never_claims_nothing_ran() {
        let text = failure_without_result(&storage_failure(), None, false, None)
            .expect("a storage failure is a failure")
            .text();
        assert!(envelope_reports_run_failure(&text), "{text}");
        assert!(!text.contains("No commands ran"), "{text}");
        assert!(text.contains("check the working tree"), "{text}");
    }

    /// Which of the two a refusal is depends on one thing: whether any attempt
    /// of the batch was granted to a machine that never answered for it.
    #[test]
    fn a_refusal_says_nothing_ran_only_when_no_machine_was_left_holding_the_batch() {
        let refused = unexecuted_failure("no executor matches.".into(), None, false).text();
        assert!(refused.starts_with(RUN_NOT_EXECUTED), "{refused}");
        assert!(refused.contains("No commands ran"), "{refused}");

        let lost = unexecuted_failure(
            "executor did not return request r attempt a.".into(),
            Some(specimen_grant(None)),
            false,
        )
        .text();
        assert!(lost.starts_with(RUN_OUTCOME_UNKNOWN), "{lost}");
        assert!(lost.contains("**bgsrv-ub**"), "{lost}");
        assert!(!lost.contains("No commands ran"), "{lost}");
        assert!(envelope_reports_run_failure(&lost), "{lost}");
    }

    /// When the record of what was dispatched died with the task that held it,
    /// the refusal still returns, still classifies as a failure, and drops the
    /// one claim it can no longer back.
    #[tokio::test]
    async fn a_placement_task_that_died_does_not_claim_nothing_ran() {
        let error = tokio::spawn(async { panic!("placement task died") })
            .await
            .unwrap_err();
        let text = placement_task_failure(&error).text();
        assert!(text.starts_with(RUN_NOT_EXECUTED), "{text}");
        assert!(!text.contains("No commands ran"), "{text}");
        assert!(text.contains("check the working tree"), "{text}");
        assert!(envelope_reports_run_failure(&text), "{text}");
    }

    #[test]
    fn substrate_vocabulary_never_reaches_run_failure_text() {
        for failure in every_failure_variant() {
            crate::system_prompt::assert_no_substrate_vocabulary(
                "composed run failure",
                &failure.text(),
            );
        }
        crate::system_prompt::assert_no_substrate_vocabulary(
            "read-only checkout denial",
            process::READ_ONLY_CHECKOUT_DENIAL,
        );
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PublishedSlotDelta {
    pub commit: String,
    pub patch: String,
    pub paths: Vec<String>,
    /// The fix landed directly on the commit the batch ran against. False when
    /// the branch advanced first and the delta was merged onto (or was already
    /// carried by) a newer head: `commit` then holds code the batch never ran.
    pub landed_on_tested_base: bool,
}

/// Publish an executor delta into a managed workspace and seal it through the
/// same importer, store lock, cleanliness checks, and commit barrier as `run`.
pub(crate) async fn publish_and_seal_slot_delta(
    orch: &Orchestrator,
    store_dir: &Path,
    request: &CellRequest,
    delta: &crate::fleet::MutationDelta,
    branch: &str,
    message: &str,
    author: Option<&PublicationIdentity>,
) -> Result<PublishedSlotDelta, String> {
    let repository = request
        .repository
        .colocated_path()
        .ok_or_else(|| "delta publication requires a colocated repository".to_string())?;
    let repository = std::fs::canonicalize(repository)
        .map_err(|error| format!("canonicalize delta publication repository: {error}"))?;
    let target = RunnerPublicationTarget {
        project_id: request.project_id.clone(),
        repository_identity: request.repository.identity(),
        git_common_dir: repository.join(".git"),
        repository,
        store_dir: store_dir.to_path_buf(),
        branch: branch.to_string(),
    };
    // Prepared before the lock: installation, pinning, and the integration
    // against the head read now need no exclusion (I3, I4).
    let prepared = prepare_slot_publication(orch, &target, request, delta, message, author)
        .await
        .map_err(|error| error.to_string())?;
    let guard = acquire_store_lock(
        orch,
        Some(store_dir),
        crate::orchestrator::StoreLockClass::Foreground,
        "build-slot delta publication and seal",
        STORE_LOCK_TIMEOUT,
    )
    .await?;
    let trace = guard.as_ref().map(|guard| guard.trace());
    let landed = land_prepared_slot_delta(
        orch,
        &target,
        request,
        delta,
        message,
        author,
        trace.as_ref(),
        prepared,
    )
    .await;
    // Receipt finalization talks to the object plane and the patch capture
    // reads immutable commits; neither needs the store.
    drop(guard);
    let mut publication = match landed {
        Ok(publication) => publication,
        Err(error) => {
            if let SlotPublicationError::Held {
                consume_receipt: true,
                ..
            } = &error
            {
                if let Err(error) = finalize_delta_receipt(orch, &target, delta).await {
                    log::warn!("held system-fix delta upload was not finalized: {error}");
                }
            }
            return Err(error.to_string());
        }
    };
    // The upload was validated and installed before either outcome was reached,
    // so it is finalized on both.
    if publication.consume_receipt {
        if let Err(error) = finalize_delta_receipt(orch, &target, delta).await {
            log::warn!("system-fix delta upload was not finalized: {error}");
        }
    }
    publication.outcome.capture_patch(&target.repository);
    let (commit, patch, landed_on_tested_base) = match publication.outcome {
        SlotPublicationOutcome::Published {
            landed,
            export,
            patch,
            integration,
            ..
        } => {
            // Fails closed: a fix whose commit never reached `refs/heads/*` is
            // not published, and reporting its sha would hand the caller a
            // commit no git consumer can see.
            export.map_err(|error| {
                format!(
                    "delta {} was sealed locally but remains unpublished: {error}",
                    landed.head
                )
            })?;
            (landed.head, patch, integration.is_none())
        }
        // The branch head already carries this fix. There is nothing to seal,
        // and an empty commit is not a better answer than saying so.
        SlotPublicationOutcome::AlreadyLanded { head } => {
            let on_base = head == delta.base_commit;
            (head, String::new(), on_base)
        }
    };
    let paths = crate::jj::parse_git_diff(&patch)
        .into_iter()
        .map(|change| change.path)
        .collect();
    Ok(PublishedSlotDelta {
        commit,
        patch,
        paths,
        landed_on_tested_base,
    })
}

fn validate_publication_identity(
    managed_project_id: &str,
    request: &CellRequest,
) -> Result<(), String> {
    if request.project_id != managed_project_id
        || request.repository.project_id() != managed_project_id
    {
        return Err(format!(
            "build-slot publication identity mismatch: request project/repository {}/{} does not match managed project {}",
            request.repository.project_id(),
            request.repository.repository_id(),
            managed_project_id
        ));
    }
    Ok(())
}

fn make_delta_objects_available(
    orch: &Orchestrator,
    repository: &std::path::Path,
    request: &CellRequest,
    delta: &crate::fleet::MutationDelta,
) -> Result<(bool, Option<std::path::PathBuf>), String> {
    let Some(receipt) = delta.upload_receipt.as_ref() else {
        verify_available_delta(repository, delta)?;
        return Ok((false, None));
    };
    if receipt.coordinate.repository != request.repository.identity()
        || receipt.coordinate.request_id != request.request_id
        || receipt.coordinate.attempt_id != request.attempt_id
        || receipt.base_commit != request.base_commit
        || receipt.base_commit != delta.base_commit
        || receipt.delta_commit != delta.delta_commit
    {
        return Err("managed delta receipt does not match the routed execution".into());
    }
    let staged = orch
        .object_plane
        .staged_delta(receipt)
        .ok_or_else(|| "managed delta receipt is expired or stale".to_string())?;
    let pack = std::fs::read(&staged.path)
        .map_err(|error| format!("read staged managed delta pack: {error}"))?;
    let validated =
        cairn_codec::transfer::validate_pack(&pack, cairn_codec::transfer::PackLimits::default())
            .map_err(|error| format!("validate staged managed delta pack: {error}"))?;
    if validated.manifest.pack_checksum != receipt.pack_checksum {
        return Err("managed delta pack checksum changed after upload".into());
    }
    let objects_text = git_output(
        repository,
        &["rev-parse", "--git-path", "objects"],
        "resolve canonical repository object database",
    )?;
    let objects_dir = {
        let path = std::path::PathBuf::from(objects_text);
        if path.is_absolute() {
            path
        } else {
            repository.join(path)
        }
    };
    let installed = cairn_codec::transfer::install_pack(&objects_dir, &validated)
        .map_err(|error| format!("install managed delta pack: {error}"))?;
    // A shallow canonical repository's history stops at its own boundary, and
    // the walk's throwaway repository cannot see that file unless it is passed.
    let shallow = cairn_codec::transfer::shallow_commits(repository)
        .map_err(|error| format!("read canonical repository shallow boundary: {error}"))?;
    cairn_codec::transfer::verify_commit_closure(&objects_dir, &[], &shallow, &delta.delta_commit)
        .map_err(|error| format!("verify imported managed delta closure: {error}"))?;
    verify_available_delta(repository, delta)?;
    Ok((true, Some(installed.pack_path)))
}

fn verify_available_delta(
    repository: &std::path::Path,
    delta: &crate::fleet::MutationDelta,
) -> Result<(), String> {
    git_output(
        repository,
        &[
            "cat-file",
            "-e",
            &format!("{}^{{commit}}", delta.delta_commit),
        ],
        "verify build-slot delta object availability",
    )?;
    let relationship = std::process::Command::new("git")
        .args([
            "merge-base",
            "--is-ancestor",
            &delta.base_commit,
            &delta.delta_commit,
        ])
        .current_dir(repository)
        .status()
        .map_err(|error| format!("verify build-slot delta base relationship: {error}"))?;
    if !relationship.success() {
        return Err("build-slot delta is not descended from its declared base".into());
    }
    Ok(())
}

fn git_output(
    repository: &std::path::Path,
    args: &[&str],
    context: &str,
) -> Result<String, String> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(repository)
        .output()
        .map_err(|error| format!("{context}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{context}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[derive(Debug)]
struct RunnerPublicationTarget {
    project_id: String,
    repository_identity: cairn_common::executor_protocol::RepositoryIdentity,
    repository: std::path::PathBuf,
    store_dir: std::path::PathBuf,
    git_common_dir: std::path::PathBuf,
    branch: String,
}

/// A delta the runner accepted, whatever became of it.
///
/// `consume_receipt` sits beside the outcome rather than inside it because a
/// managed upload has been validated and installed by the time either outcome
/// is reached, so finalizing it is the caller's obligation on every branch.
/// Held here, no branch can be written that forgets it.
#[derive(Debug)]
struct SlotPublication {
    consume_receipt: bool,
    outcome: SlotPublicationOutcome,
}

#[derive(Debug)]
enum SlotPublicationOutcome {
    Published {
        landed: cairn_vcs::LogicalHeadPublication,
        /// The jj→git export leg of this publication. `Err` means the commit is
        /// sealed in the store but `refs/heads/<branch>` never moved, so it must
        /// not be reported as committed.
        export: Result<(), String>,
        /// Empty when `patch_unavailable` says why it could not be read.
        patch: String,
        /// Why the landed patch could not be captured. Capturing it happens
        /// after the bookmark moved, so its failure is a missing line count,
        /// never an unpublished commit.
        patch_unavailable: Option<String>,
        /// Present when the branch moved while the batch ran and its changes
        /// were merged onto the head the branch actually held.
        integration: Option<IntegrationNote>,
        /// The head the landing transaction was expected to find.
        published_onto: String,
    },
    /// The branch head's tree already carries this batch's content.
    AlreadyLanded { head: String },
}

#[derive(Debug)]
struct IntegrationNote {
    head: String,
    /// An amend cannot rewrite a head the batch never built on — under a
    /// straddle that commit may be a sibling's landed work. The amend became a
    /// child of the moved head instead.
    amend_converted: bool,
}

/// Why a routed batch could not be published, classified by where its sealed
/// delta was when publication stopped.
///
/// The classification is the whole point. A batch that ran and sealed a commit
/// has done real work, and what the agent must be told depends entirely on
/// whether this machine holds that commit: before the store has it there is
/// nothing to retry and the edits have to be re-applied; once it has it, the
/// commit is held and republished, and re-applying the edits would duplicate
/// them. A blanket conversion of every error into one variant is what once told
/// an agent a sealed commit had not been made.
#[derive(Debug)]
pub(crate) enum SlotPublicationError {
    /// Publication stopped before the delta was installed and verified in the
    /// project store (target resolution, upload receipt, pack install,
    /// base-ancestry check). Nothing is held host-side.
    NotInStore { delta: String, error: String },
    /// Installed in the store, but the branch could not be advanced. The delta
    /// is held for republication at the next commit barrier on the branch.
    /// `consume_receipt` is the caller's obligation exactly as on a
    /// publication: the upload was installed, so its staging copy is spent.
    Held {
        consume_receipt: bool,
        held: held::HeldDelta,
    },
    /// The branch moved while the batch ran and its changes conflict with what
    /// moved onto it. The delta is kept under a stranded ref.
    Straddled(String),
}

impl std::fmt::Display for SlotPublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotInStore { delta, error } => {
                f.write_str(&held::not_in_store_message(delta, error))
            }
            Self::Held { held, .. } => f.write_str(&held.message()),
            Self::Straddled(text) => f.write_str(text),
        }
    }
}

/// Why publishing a delta the store already holds did not land it.
#[derive(Debug)]
enum PublishFailure {
    /// Its changes conflict with what the branch now holds.
    Conflicted { head: String, paths: Vec<String> },
    /// Anything else. The delta is still in the store and can be retried.
    Failed(String),
}

// Sound only because every fallible step of [`publish_available_delta`] runs
// after the delta is installed: a failure there is always retryable.
impl From<String> for PublishFailure {
    fn from(error: String) -> Self {
        Self::Failed(error)
    }
}

/// A delta installed and verified in the project store, protected from
/// pruning for as long as this value lives.
struct AvailableDelta {
    consume_receipt: bool,
    _pin: crate::jj::DeltaObjectPin,
}

/// Everything needed to publish a delta the store already holds. Deliberately
/// free of the routed request, so a held delta can be republished long after
/// the batch that sealed it returned.
struct DeltaPublication<'a> {
    repository: &'a Path,
    store_dir: &'a Path,
    project_id: &'a str,
    branch: &'a str,
    base_commit: &'a str,
    delta_commit: &'a str,
    message: &'a str,
    author: Option<&'a PublicationIdentity>,
    /// The job whose check status a landing invalidates.
    job_id: Option<&'a str>,
    /// The enclosing store-lock hold, whose phases this publication reports.
    trace: Option<&'a JjStoreOperationTrace>,
}

fn resolve_runner_publication_target(
    resolution: &super::branch::BranchResolution,
    request: &CellRequest,
) -> Result<RunnerPublicationTarget, String> {
    validate_publication_identity(&resolution.project_id, request)?;
    let Some(request_repository) = request.repository.colocated_path() else {
        return Err("logical-head publication requires a colocated repository".to_string());
    };
    let repository =
        std::fs::canonicalize(&resolution.object_repository_path).map_err(|error| {
            format!(
                "canonicalize logical repository {}: {error}",
                resolution.object_repository_path.display()
            )
        })?;
    let request_repository = std::fs::canonicalize(request_repository).map_err(|error| {
        format!("canonicalize build-slot request repository {request_repository}: {error}")
    })?;
    if request_repository != repository {
        return Err(format!(
            "build-slot publication repository mismatch: request resolves to {}, logical project resolves to {}",
            request_repository.display(), repository.display()
        ));
    }
    let common = git_output(
        &repository,
        &["rev-parse", "--git-common-dir"],
        "resolve git common directory",
    )?;
    let common = std::path::PathBuf::from(common);
    let git_common_dir = if common.is_absolute() {
        common
    } else {
        repository.join(common)
    };
    Ok(RunnerPublicationTarget {
        project_id: resolution.project_id.clone(),
        repository_identity: request.repository.identity(),
        repository,
        store_dir: resolution.repository_path.clone(),
        git_common_dir,
        branch: resolution.rev.clone(),
    })
}

/// Install a routed delta's objects in the project store, pin them, and verify
/// the delta descends from its declared base. Every failure here leaves the
/// store without the delta, so nothing can be held.
fn make_delta_available(
    orch: &Orchestrator,
    repository: &Path,
    request: &CellRequest,
    delta: &crate::fleet::MutationDelta,
) -> Result<AvailableDelta, String> {
    let (consume_receipt, installed_pack) =
        make_delta_objects_available(orch, repository, request, delta)?;
    let pin = crate::jj::pin_validated_delta(
        repository,
        &delta.base_commit,
        &delta.delta_commit,
        installed_pack.as_deref(),
    )?;
    verify_available_delta(repository, delta)?;
    Ok(AvailableDelta {
        consume_receipt,
        _pin: pin,
    })
}

/// A routed delta made ready to land: installed, pinned, and — when it could
/// be — integrated against the head the branch held at preparation.
///
/// Everything here is computed without the store lock. The objects are pinned
/// from installation until the landing transaction commits (I4), so nothing
/// ref-less is ever unprotected, and the plan is only used under the lock if
/// the bookmark still holds the head it was computed against (I3).
struct PreparedSlot {
    available: AvailableDelta,
    plan: Option<IntegrationPlan>,
}

fn slot_delta_publication<'a>(
    target: &'a RunnerPublicationTarget,
    request: &'a CellRequest,
    delta: &'a crate::fleet::MutationDelta,
    message: &'a str,
    author: Option<&'a PublicationIdentity>,
    trace: Option<&'a JjStoreOperationTrace>,
) -> DeltaPublication<'a> {
    DeltaPublication {
        repository: &target.repository,
        store_dir: &target.store_dir,
        project_id: &target.project_id,
        branch: &target.branch,
        base_commit: &delta.base_commit,
        delta_commit: &delta.delta_commit,
        message,
        author,
        job_id: request
            .owner
            .as_ref()
            .and_then(|owner| owner.job_id.as_deref()),
        trace,
    }
}

/// Install, pin, and integrate a routed delta without holding the store lock.
///
/// Only installation can fail the publication here, and it fails exactly as it
/// would under the lock: nothing is in the store, nothing is held. An
/// integration that cannot be planned is simply not planned; the locked path
/// meets the same condition and reports it.
async fn prepare_slot_publication(
    orch: &Orchestrator,
    target: &RunnerPublicationTarget,
    request: &CellRequest,
    delta: &crate::fleet::MutationDelta,
    message: &str,
    author: Option<&PublicationIdentity>,
) -> Result<PreparedSlot, SlotPublicationError> {
    debug_assert_eq!(target.project_id, request.project_id);
    debug_assert_eq!(target.repository_identity, request.repository.identity());
    debug_assert!(target.git_common_dir.is_absolute());
    let available =
        make_delta_available(orch, &target.repository, request, delta).map_err(|error| {
            SlotPublicationError::NotInStore {
                delta: delta.delta_commit.clone(),
                error,
            }
        })?;
    let publication = slot_delta_publication(target, request, delta, message, author, None);
    let plan = match cairn_vcs::resolve_coordinate(&target.store_dir, &target.branch).await {
        Ok(head) => plan_integration(orch, &publication, head).await.ok(),
        Err(_) => None,
    };
    Ok(PreparedSlot { available, plan })
}

/// Land a prepared delta. Caller holds the store lock; on a failure that
/// leaves the delta in the store it is held or stranded here, under the same
/// hold, as before.
#[allow(clippy::too_many_arguments)]
async fn land_prepared_slot_delta(
    orch: &Orchestrator,
    target: &RunnerPublicationTarget,
    request: &CellRequest,
    delta: &crate::fleet::MutationDelta,
    message: &str,
    author: Option<&PublicationIdentity>,
    trace: Option<&JjStoreOperationTrace>,
    prepared: PreparedSlot,
) -> Result<SlotPublication, SlotPublicationError> {
    // Kept to the end of this function: its pin protects the delta's objects
    // until the landing transaction has committed or a held ref has taken over.
    let PreparedSlot { available, plan } = prepared;
    let publication = slot_delta_publication(target, request, delta, message, author, trace);
    match publish_available_delta(orch, &publication, plan).await {
        Ok(outcome) => Ok(SlotPublication {
            consume_receipt: available.consume_receipt,
            outcome,
        }),
        Err(PublishFailure::Conflicted { head, paths }) => {
            let _phase = trace.map(|trace| trace.phase("strand conflicted delta"));
            Err(SlotPublicationError::Straddled(strand_conflicted_delta(
                &target.repository,
                request,
                delta,
                &head,
                &paths,
            )))
        }
        Err(PublishFailure::Failed(error)) => {
            let _phase = trace.map(|trace| trace.phase("hold delta"));
            // Written while `available` still pins the objects, so the ref
            // takes over protecting them before the pin is released.
            let held = held::hold_delta(
                orch,
                held::Hold {
                    project_id: &target.project_id,
                    store_dir: &target.store_dir,
                    repository: &target.repository,
                    branch: &target.branch,
                    job_id: publication.job_id,
                    attempt_id: &request.attempt_id,
                    base_commit: &delta.base_commit,
                    delta_commit: &delta.delta_commit,
                    message,
                    author,
                    error: &error,
                },
            )
            .await;
            Err(SlotPublicationError::Held {
                consume_receipt: available.consume_receipt,
                held,
            })
        }
    }
}

/// Prepare, land, and capture the patch of a routed delta in one call, for a
/// caller that holds the store lock across all three. Production callers
/// prepare before acquiring the lock instead; the straddle tests drive the same
/// prepare-then-land sequence through this.
#[cfg(test)]
async fn publish_visible_slot_delta(
    orch: &Orchestrator,
    target: &RunnerPublicationTarget,
    request: &CellRequest,
    delta: &crate::fleet::MutationDelta,
    message: &str,
    author: Option<&PublicationIdentity>,
    trace: Option<&JjStoreOperationTrace>,
) -> Result<SlotPublication, SlotPublicationError> {
    let prepared = prepare_slot_publication(orch, target, request, delta, message, author).await?;
    let mut publication = land_prepared_slot_delta(
        orch, target, request, delta, message, author, trace, prepared,
    )
    .await?;
    publication.outcome.capture_patch(&target.repository);
    Ok(publication)
}

/// The merge base for a delta whose parent is a commit its batch's execution
/// home carried forward unpublished.
///
/// Once the branch carries that parent, the parent: the delta then contributes
/// exactly what its batch did, including an undo of the parent's own lines.
/// While the parent is a held delta still waiting on this branch, nothing: this
/// delta is held behind it, because publishing it first would land the batch's
/// edits of a change the branch does not have yet, and the held commit landing
/// afterwards would then re-apply whatever the batch undid. A parent the runner
/// never received (it never reached the store) is published together with the
/// delta, against the routed base.
async fn merge_base_for_carried_parent(
    orch: &Orchestrator,
    publication: &DeltaPublication<'_>,
    parent: &str,
    head: &str,
) -> Result<String, PublishFailure> {
    let repository = publication.repository;
    if matches!(
        crate::jj::integrate_delta_onto_head(repository, publication.base_commit, parent, head)?,
        crate::jj::Integration::AlreadyLanded
    ) {
        return Ok(parent.to_string());
    }
    let pending = held::held_rows(&orch.db.local, publication.project_id, publication.branch)
        .await
        .map_err(PublishFailure::Failed)?;
    if let Some(row) = pending.iter().find(|row| {
        row.delta_commit != publication.delta_commit
            && git_is_ancestor(repository, &row.delta_commit, parent)
    }) {
        return Err(PublishFailure::Failed(format!(
            "this batch was built on commit {}, which an earlier batch made and which is still \
             waiting to be published to this branch, so this one waits behind it",
            row.delta_commit
        )));
    }
    Ok(publication.base_commit.to_string())
}

fn git_is_ancestor(repository: &Path, ancestor: &str, descendant: &str) -> bool {
    std::process::Command::new("git")
        .args(["merge-base", "--is-ancestor", ancestor, descendant])
        .current_dir(repository)
        .status()
        .is_ok_and(|status| status.success())
}

/// What landing a delta on one particular head would publish.
///
/// A function of that head, the delta, and the message alone — except when the
/// delta's parent is a carried commit, whose merge base also depends on which
/// held deltas are waiting, which a head comparison cannot revalidate. Such a
/// plan is never reused under the lock.
struct IntegrationPlan {
    head: String,
    reusable: bool,
    landing: PlannedLanding,
}

enum PlannedLanding {
    Land {
        proposed: String,
        integrated: bool,
        mode: cairn_vcs::PublicationMode,
        amend_converted: bool,
        /// The merged commit, its tree, and the blobs the merge wrote are
        /// ref-less until the transaction commits.
        _pin: Option<crate::jj::DeltaObjectPin>,
    },
    AlreadyLanded,
    Conflicted {
        paths: Vec<String>,
    },
}

/// Integrate a delta onto `head`. Needs no exclusion: it writes only
/// unreferenced objects, which it pins, and reads immutable commits.
async fn plan_integration(
    orch: &Orchestrator,
    publication: &DeltaPublication<'_>,
    head: String,
) -> Result<IntegrationPlan, PublishFailure> {
    let DeltaPublication {
        repository,
        base_commit,
        delta_commit,
        message,
        ..
    } = *publication;
    // The delta's own parent is the commit its batch opened at: the routed base,
    // or an unpublished commit the execution home carried forward. In the
    // second case the batch's work is the diff against that parent, so that is
    // the merge base once the branch carries it.
    let parent = git_output(
        repository,
        &["rev-parse", &format!("{delta_commit}^1")],
        "read the delta's parent",
    )?;
    let carried = parent != base_commit;
    let merge_base = if carried {
        merge_base_for_carried_parent(orch, publication, &parent, &head).await?
    } else {
        base_commit.to_string()
    };
    let plan = |landing| IntegrationPlan {
        head: head.clone(),
        reusable: !carried,
        landing,
    };
    let integration =
        crate::jj::integrate_delta_onto_head(repository, &merge_base, delta_commit, &head)?;
    let mut pin = None;
    let (proposed, integrated) = match integration {
        crate::jj::Integration::Unmoved if parent == head => (delta_commit.to_string(), false),
        // The branch never held the carried parent, which is published together
        // with this delta: the delta's tree re-rooted onto the head.
        crate::jj::Integration::Unmoved => {
            let commit = crate::jj::reparent_delta_onto_head(repository, delta_commit, &head)?;
            pin = Some(crate::jj::pin_validated_delta(
                repository, &head, &commit, None,
            )?);
            (commit, false)
        }
        crate::jj::Integration::AlreadyLanded => return Ok(plan(PlannedLanding::AlreadyLanded)),
        crate::jj::Integration::Commit(commit) => {
            pin = Some(crate::jj::pin_validated_delta(
                repository, &head, &commit, None,
            )?);
            (commit, true)
        }
        crate::jj::Integration::Conflicted { paths } => {
            return Ok(plan(PlannedLanding::Conflicted { paths }))
        }
    };
    let head_description = if integrated && message == "^" {
        Some(git_output(
            repository,
            &["log", "-1", "--format=%B", &head],
            "read the logical head description for a converted amend",
        )?)
    } else {
        None
    };
    let (mode, amend_converted) = publication_mode(message, head_description.as_deref());
    Ok(plan(PlannedLanding::Land {
        proposed,
        integrated,
        mode,
        amend_converted,
        _pin: pin,
    }))
}

/// Land a delta the project store already holds onto its branch.
///
/// Caller holds the canonical per-store lock and keeps the delta's objects
/// pinned for the duration. Shared by a batch's own publication and by the
/// republication of a held delta, so both land through one funnel.
///
/// `prepared` is an integration planned before the lock. It is used only when
/// the bookmark, read here under the lock, still holds the head it was planned
/// against; otherwise the integration is computed again here, which is exactly
/// the fully locked path, so the outcome is the same either way (I3). The
/// transaction itself still refuses a bookmark that is no longer that head.
/// The patch is not captured here: it is a read of two immutable commits, left
/// to [`SlotPublicationOutcome::capture_patch`] once the lock is released.
async fn publish_available_delta(
    orch: &Orchestrator,
    publication: &DeltaPublication<'_>,
    prepared: Option<IntegrationPlan>,
) -> Result<SlotPublicationOutcome, PublishFailure> {
    let DeltaPublication {
        store_dir,
        branch,
        author,
        job_id,
        trace,
        ..
    } = *publication;

    // A delta declares as its parent the commit the batch was routed against,
    // and the logical-head transaction refuses a parent the bookmark no longer
    // holds. Read the bookmark here — under the same store lock the transaction
    // will re-check it under, through the same jj view, so the two cannot
    // disagree — and merge the batch's changes onto it when it moved. The
    // executor cannot do this: it cannot tell its own head having been refreshed
    // to a new tip from its head being a descendant carrying work the runner has
    // not published back yet.
    let resolving = trace.map(|trace| trace.phase("resolve head"));
    let head = cairn_vcs::resolve_coordinate(store_dir, branch)
        .await
        .map_err(|error| format!("resolve `{branch}` for delta publication: {error}"))?;
    drop(resolving);
    let plan = match prepared {
        Some(plan) if plan.reusable && plan.head == head => plan,
        prepared => {
            if let Some(stale) = prepared.filter(|plan| plan.head != head) {
                log::info!(
                    "delta publication on `{branch}`: the head moved from {} to {head} after \
                     preparation; integrating again under the store lock",
                    stale.head
                );
            }
            let _integrating = trace.map(|trace| trace.phase("integrate"));
            plan_integration(orch, publication, head).await?
        }
    };
    let IntegrationPlan { head, landing, .. } = plan;
    let (proposed, integrated, mode, amend_converted, _pin) = match landing {
        PlannedLanding::AlreadyLanded => return Ok(SlotPublicationOutcome::AlreadyLanded { head }),
        PlannedLanding::Conflicted { paths } => {
            return Err(PublishFailure::Conflicted { head, paths })
        }
        PlannedLanding::Land {
            proposed,
            integrated,
            mode,
            amend_converted,
            _pin,
        } => (proposed, integrated, mode, amend_converted, _pin),
    };
    // Carried for BOTH modes: an amend re-stamps the committer even though it
    // preserves the author, so withholding the identity here leaves the
    // amended commit unpushable.
    let identity = author.cloned();
    let crate::jj::PublishedLogicalHead {
        landed,
        export,
        timings,
    } = crate::jj::publish_logical_head_exported(
        &crate::jj::environment(&orch.jj_binary_path, &orch.config_dir),
        store_dir,
        branch,
        &head,
        crate::jj::ProposedPublication::DeltaCommit(proposed),
        identity,
        mode,
    )
    .await?;
    if let Some(trace) = trace {
        trace.record_publication(&timings);
    }
    // Nothing below may fail the publication: the bookmark has moved, and
    // reporting a landed commit as unpublished is the one wrong answer.
    if let Some(job_id) = job_id {
        orch.invalidate_node_check_status(job_id, "logical-head-tree-or-contract-change");
    }
    Ok(SlotPublicationOutcome::Published {
        landed,
        export,
        patch: String::new(),
        patch_unavailable: None,
        integration: integrated.then_some(IntegrationNote {
            head: head.clone(),
            amend_converted,
        }),
        published_onto: head,
    })
}

impl SlotPublicationOutcome {
    /// Read the landed patch. Against the head actually published onto, never
    /// the routed base: after an integration those differ, and diffing from the
    /// routed base would attribute every line the advance carried to this
    /// batch. Needs no lock: both commits are immutable.
    fn capture_patch(&mut self, repository: &Path) {
        let Self::Published {
            landed,
            patch,
            patch_unavailable,
            published_onto,
            ..
        } = self
        else {
            return;
        };
        match git_output(
            repository,
            &[
                "diff",
                "--no-ext-diff",
                "--binary",
                published_onto,
                &landed.head,
            ],
            "capture logical-head delta patch",
        ) {
            Ok(captured) => *patch = captured,
            Err(error) => *patch_unavailable = Some(error),
        }
    }
}

/// Where a landed commit's remaining publication rungs reach.
pub(crate) struct LandingRungs<'a> {
    pub(crate) project_id: &'a str,
    pub(crate) object_repository: &'a Path,
    pub(crate) store_dir: &'a Path,
    pub(crate) branch: &'a str,
    /// The job the commit is attributed to. `None` when it cannot be: the
    /// origin push then happens unconditionally, because nothing can prove no
    /// open PR depends on the branch.
    pub(crate) routed: Option<RoutedLanding<'a>>,
}

pub(crate) struct RoutedLanding<'a> {
    pub(crate) job_id: &'a str,
    pub(crate) project_id: &'a str,
    /// The database holding the job's pull-request state.
    pub(crate) db: &'a crate::storage::LocalDb,
}

/// What the rungs after the logical-head transaction did.
pub(crate) struct LandingReport {
    /// `Err` when the commit is in the store but did not reach the branch ref,
    /// or origin when an open PR requires it.
    pub(crate) published: Result<(), String>,
    /// Rungs that never fail publication (live dev instance sync, the cloud
    /// pack), each rendered as a ` — ⚠️ ...` suffix.
    pub(crate) warnings: String,
}

/// Climb the publication ladder past the logical-head transaction: the export
/// always, the origin push when an open PR is on the branch, then the live dev
/// instances and the cloud pack.
///
/// One function for a batch's own commit and a republished held one, so the two
/// cannot drift. The push requirement is the question the write verb asks,
/// asked the same way. Must run without the store lock held: the push is
/// network I/O.
pub(crate) async fn climb_landing_rungs(
    orch: &Orchestrator,
    rungs: &LandingRungs<'_>,
    head: &str,
    export: Result<(), String>,
) -> LandingReport {
    use crate::merge_requests::queries::PublicationRequirement;

    let published = match export {
        Err(error) => Err(error),
        Ok(()) => {
            let requirement = match rungs.routed.as_ref() {
                Some(routed) => {
                    crate::merge_requests::queries::publication_requirement_for_managed_branch(
                        routed.db,
                        routed.job_id,
                        routed.project_id,
                        rungs.branch,
                    )
                    .await
                }
                None => PublicationRequirement::RequiredForOpenPr,
            };
            match requirement {
                PublicationRequirement::RequiredForOpenPr => {
                    crate::orchestrator::base_advance::publish_managed_branch(
                        orch,
                        rungs.store_dir,
                        rungs.branch,
                    )
                    .await
                }
                PublicationRequirement::DeferredUntilPublication => Ok(()),
            }
        }
    };
    let mut warnings = String::new();
    let sync_failures = crate::dev_instances::sync_live_branch_instances(
        orch,
        rungs.project_id,
        rungs.branch,
        head,
    )
    .await;
    if !sync_failures.is_empty() {
        warnings.push_str(&format!(
            " — ⚠️ live dev instance sync blocked ({} instance(s)); the owner received a blocking notice",
            sync_failures.len()
        ));
    }
    if let Some(routed) = rungs.routed.as_ref() {
        if let Err(error) = crate::mcp::vcs::publish_sealed_commit_pack(
            routed.db,
            rungs.project_id,
            rungs.object_repository,
            head,
        )
        .await
        {
            warnings.push_str(&format!(
                " — ⚠️ sealed commit cloud publication failed: {error}"
            ));
        }
    }
    LandingReport {
        published,
        warnings,
    }
}

/// The publication mode a batch's `commit_msg` selects.
///
/// `^` means amend, which rewrites the commit the bookmark holds. That commit is
/// the batch's own only while the bookmark has not moved. `head_description` is
/// supplied exactly when the batch's changes had to be merged onto a head it
/// never built on — which may be a sibling's landed commit, and rewriting that
/// is strictly worse than the refusal the merge exists to fix. The amend becomes
/// a child of the moved head instead, keeping its description: the same fallback
/// the publication transaction already takes when the head carries a foreign
/// bookmark. Refusing the amend is not an option, because it would leave the
/// amend case with exactly the stranded work this integration removes.
///
/// Returns the mode and whether an amend was converted.
fn publication_mode(
    message: &str,
    head_description: Option<&str>,
) -> (cairn_vcs::PublicationMode, bool) {
    if message != "^" {
        return (
            cairn_vcs::PublicationMode::Child {
                description: message.to_string(),
            },
            false,
        );
    }
    match head_description {
        None => (cairn_vcs::PublicationMode::Amend, false),
        Some(description) => (
            cairn_vcs::PublicationMode::Child {
                description: match description.trim() {
                    "" => "amend".to_string(),
                    description => description.to_string(),
                },
            },
            true,
        ),
    }
}

/// Give a straddled delta that could not be merged a durable name, and say how
/// to get it into the branch by hand.
///
/// The delta commit exists in the working directory's object database — the
/// batch's own seal created it — so a ref is all that stands between it and the
/// next prune.
fn strand_conflicted_delta(
    repository: &std::path::Path,
    request: &CellRequest,
    delta: &crate::fleet::MutationDelta,
    head: &str,
    paths: &[String],
) -> String {
    let reference = stranded_ref_name(
        request
            .owner
            .as_ref()
            .and_then(|owner| owner.job_id.as_deref()),
        &request.attempt_id,
    );
    // This ref is the only thing standing between the agent's sealed work and
    // the next prune, and it contends for the repository's one
    // `packed-refs.lock` like every other ref transaction. Patience is what
    // keeps a lost race from turning a recoverable straddle into lost work.
    if let Err(error) = crate::jj::with_packed_refs_patience(
        crate::jj::PackedRefs::Checkout(repository),
        "record the stranded delta",
        || {
            git_output(
                repository,
                &["update-ref", &reference, &delta.delta_commit],
                "record the stranded delta",
            )
        },
    ) {
        log::warn!(
            "stranded delta {} was not recorded under {reference}: {error}",
            delta.delta_commit
        );
    }
    straddled_conflict_message(head, &delta.delta_commit, paths)
}

/// Where a stranded delta is kept: `refs/cairn/stranded/<job>/<attempt>` when the
/// batch names its job, so the ref-store sweep can retire it once that job's
/// issue has been resolved for the retention window, and the bare
/// `refs/cairn/stranded/<attempt>` otherwise, which the sweep never retires.
///
/// A held delta is kept under the same name: it needs the same lifecycle.
fn stranded_ref_name(job_id: Option<&str>, attempt_id: &str) -> String {
    match job_id.filter(|job| !job.is_empty() && !job.contains('/')) {
        Some(job) => format!("refs/cairn/stranded/{job}/{attempt_id}"),
        None => format!("refs/cairn/stranded/{attempt_id}"),
    }
}

/// How an agent sees what its straddled batch changed, so it can put that work
/// back on the branch by hand.
///
/// Deliberately inspection rather than replay. `git cherry-pick` is the obvious
/// suggestion and it does not survive contact with a batch: this message only
/// appears when there IS a conflict, so a cherry-pick inside a `commit_msg`
/// batch would commit conflict markers, and inside a batch without one the
/// batch undoes exactly the paths it changed. There is no batch shape in which
/// the replay both applies and gets resolved before the commit. Ordinary
/// editing, on the other hand, always lands. So the saved commit's job is to
/// say what was lost, not to be replayed mechanically.
fn straddle_inspection_commands(head: &str, delta_commit: &str) -> [String; 2] {
    [
        format!("git show {delta_commit}"),
        format!("git diff {head} {delta_commit}"),
    ]
}

/// What an agent is told when its batch straddled a branch move it cannot be
/// merged past.
///
/// Unlike a held delta, which republishes itself, this one needs the agent:
/// real work was sealed and nothing has been lost, but it cannot land as it is.
/// This names where the branch went, what collided,
/// where the work is, and — the part that is easy to get wrong — that simply
/// running again will not pick it up, because a batch commits only what it
/// changes while it runs and those edits are already present when it starts.
fn straddled_conflict_message(head: &str, delta_commit: &str, paths: &[String]) -> String {
    let [show, diff] = straddle_inspection_commands(head, delta_commit);
    format!(
        "The branch moved to {head} while this batch ran, and this batch's changes conflict with \
         what moved onto it in {}. Nothing was committed. Your changes are saved as commit \
         {delta_commit}: `{show}` shows them, and `{diff}` compares them with what the branch \
         holds now. Re-apply them on top of the branch's version, editing as you normally would, \
         and commit as usual. Running again without re-applying them will not pick them up.",
        paths.join(", "),
    )
}

#[cfg(test)]
mod slot_publication_tests {
    use super::*;
    use crate::db::DbState;
    use crate::jj::tests::{git, git_stdout, jj_bin};
    use crate::jj::JjEnv;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use std::path::PathBuf;
    use std::sync::Arc;

    /// A colocated repository holding a `feature` bookmark, plus everything the
    /// runner has in hand at publication time: the routed request, the sealed
    /// delta, and the publication target.
    struct Straddle {
        _home: tempfile::TempDir,
        _repo: tempfile::TempDir,
        jj: JjEnv,
        repository: PathBuf,
        target: RunnerPublicationTarget,
        request: CellRequest,
        delta: crate::fleet::MutationDelta,
        /// The commit the batch was routed at.
        base: String,
        /// Where the bookmark stands when the batch tries to publish.
        head: String,
    }

    impl Straddle {
        fn bookmark(&self) -> String {
            self.jj
                .run(
                    &self.repository,
                    &[
                        "log",
                        "-r",
                        "feature",
                        "--no-graph",
                        "-T",
                        "commit_id",
                        "--ignore-working-copy",
                    ],
                    "read the bookmark",
                )
                .unwrap()
        }

        fn blob(&self, commit: &str, path: &str) -> String {
            git_stdout(&self.repository, &["show", &format!("{commit}:{path}")])
        }

        fn description(&self, commit: &str) -> String {
            git_stdout(&self.repository, &["log", "-1", "--format=%B", commit])
        }
    }

    /// Build the straddle. `advance` is what a sibling lands on the bookmark
    /// while the batch runs, or `None` for the ordinary unmoved case; `batch` is
    /// what the agent's batch wrote. Both are sealed off to the side, parented
    /// at the routed commit and referenced by nothing, which is the shape a
    /// delta actually arrives in. `None` when jj is not resolvable here.
    async fn straddle(
        advance: Option<&[(&str, &str)]>,
        batch: &[(&str, &str)],
    ) -> Option<Straddle> {
        let bin = jj_bin()?;
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let jj = crate::jj::environment(&bin, home.path());
        let repository = std::fs::canonicalize(repo.path()).unwrap();
        let path = repository.as_path();

        git(path, &["init", "-q", "-b", "main"]);
        git(path, &["config", "user.email", "p@e.com"]);
        git(path, &["config", "user.name", "p"]);
        std::fs::write(path.join("advanced.rs"), "before\n").unwrap();
        std::fs::write(path.join("mine.rs"), "base\n").unwrap();
        git(path, &["add", "-A"]);
        git(path, &["commit", "-q", "-m", "base"]);
        let base = git_stdout(path, &["rev-parse", "HEAD"]);

        let side = |files: &[(&str, &str)], message: &str| {
            git(path, &["checkout", "-q", "--detach", &base]);
            for (name, content) in files {
                std::fs::write(path.join(name), content).unwrap();
            }
            git(path, &["add", "-A"]);
            git(path, &["commit", "-q", "-m", message]);
            git_stdout(path, &["rev-parse", "HEAD"])
        };
        let sibling = advance.map(|files| side(files, "a sibling lands"));
        let delta_commit = side(batch, "the batch");
        git(path, &["checkout", "-q", "main"]);

        jj.run(path, &["git", "init", "--colocate", "."], "colocate")
            .unwrap();
        jj.run(
            path,
            &["bookmark", "create", "feature", "-r", &base],
            "create the job bookmark",
        )
        .unwrap();
        let head = match sibling {
            None => base.clone(),
            Some(sibling) => {
                // Through the same sanctioned barrier the runner uses, so the
                // sibling's advance reaches the branch ref exactly as a real
                // landing would.
                crate::jj::publish_logical_head_exported(
                    &jj,
                    &repository,
                    "feature",
                    &base,
                    crate::jj::ProposedPublication::DeltaCommit(sibling),
                    None,
                    cairn_vcs::PublicationMode::Child {
                        description: "a sibling lands".into(),
                    },
                )
                .await
                .unwrap()
                .landed
                .head
            }
        };

        let request = CellRequest {
            request_id: "request".into(),
            attempt_id: "attempt-3231".into(),
            project_id: "project".into(),
            repository: cairn_common::executor_protocol::RepositoryLocator::ColocatedPath {
                project_id: "project".into(),
                repository_id: "repository".into(),
                absolute_path: repository.to_string_lossy().into_owned(),
            },
            base_commit: base.clone(),
            command: "true".into(),
            command_class: cairn_common::executor_protocol::CellCommandClass::Other,
            placement_work_class:
                cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: cairn_common::executor_protocol::CellPriority::ReviewCheck,
            wait_horizon_unix_ms: u64::MAX,
            waiting_since_unix_ms: 0,
            timeout_ms: 1_000,
            mutation_policy: cairn_common::executor_protocol::MutationPolicy::AllowDelta,
            requesting_job_id: None,
            affinity_key: None,
            executor: None,
            pinned_executor_id: None,
            placement_mobility: Default::default(),
            verdict_platforms: Vec::new(),
            avoided_executor_ids: Vec::new(),
            project_requirements: Default::default(),
            command_resource_identity: None,
            resource_reservation: Default::default(),
            learned_estimate: None,
        };
        let target = RunnerPublicationTarget {
            project_id: "project".into(),
            repository_identity: request.repository.identity(),
            repository: repository.clone(),
            store_dir: repository.clone(),
            git_common_dir: repository.join(".git"),
            branch: "feature".into(),
        };
        let delta = crate::fleet::MutationDelta {
            base_commit: base.clone(),
            delta_commit,
            upload_receipt: None,
        };
        Some(Straddle {
            _home: home,
            _repo: repo,
            jj,
            repository,
            target,
            request,
            delta,
            base,
            head,
        })
    }

    async fn orchestrator() -> Orchestrator {
        let db = crate::storage::migrated_test_db("run-slot-publication-test.db").await;
        // A managed delta's catalog publication carries the project id, and the
        // pack catalog holds a foreign key to `projects`. Without the row the
        // publication fails and the receipt is never finalized — a property of
        // the fixture, not of the publication path, so seed it here rather than
        // let it masquerade as behavior.
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT OR IGNORE INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('project', 'default', 'Project', 'proj', '/repo', 'main', 1, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        let config_dir = tempfile::tempdir().unwrap().keep();
        let index_path = config_dir.join("search-index.db");
        let db_state = Arc::new(DbState::new(
            Arc::new(db),
            Arc::new(SearchIndex::open_or_create(index_path).unwrap()),
        ));
        let services = Arc::new(TestServicesBuilder::new().build());
        Orchestrator::builder(db_state, services, config_dir).build()
    }

    fn published_paths(patch: &str) -> Vec<String> {
        crate::jj::parse_git_diff(patch)
            .into_iter()
            .map(|change| change.path)
            .collect()
    }

    /// The whole point, through the runner's own publication funnel. A batch
    /// routed at one commit publishes onto the commit the bookmark actually
    /// holds, the branch ends up carrying both the sibling's work and the
    /// batch's, and the patch the agent is shown covers only the batch's own
    /// changes rather than crediting it with the sibling's.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_straddled_batch_publishes_onto_the_head_the_branch_moved_to() {
        let Some(fx) = straddle(
            Some(&[("advanced.rs", "after the advance\n")]),
            &[("mine.rs", "the batch's work\n")],
        )
        .await
        else {
            eprintln!("skipping a_straddled_batch_publishes_onto_the_head: jj not resolvable");
            return;
        };
        let orch = orchestrator().await;

        let publication = publish_visible_slot_delta(
            &orch,
            &fx.target,
            &fx.request,
            &fx.delta,
            "the batch",
            None,
            None,
        )
        .await
        .expect("a straddle over disjoint paths publishes");
        let SlotPublicationOutcome::Published {
            landed,
            export,
            patch,
            integration,
            ..
        } = publication.outcome
        else {
            panic!("expected a publication");
        };
        export.expect("the publication exports its bookmark to the backing checkout");
        assert_eq!(
            git_stdout(&fx.repository, &["rev-parse", "refs/heads/feature"]),
            landed.head,
            "an integrated publication must reach the branch ref, not just the bookmark"
        );

        let note = integration.expect("the straddle is reported to the agent");
        assert_eq!(note.head, fx.head);
        assert!(!note.amend_converted);
        assert_eq!(fx.bookmark(), landed.head);
        assert_eq!(
            git_stdout(&fx.repository, &["rev-parse", &format!("{}^", landed.head)]),
            fx.head,
            "published as a child of the head the bookmark held, not of the routed base"
        );
        assert_eq!(
            published_paths(&patch),
            ["mine.rs"],
            "the patch is taken against the head published onto; against the routed base it \
             would credit this batch with the sibling's file too"
        );
        assert_eq!(fx.blob(&landed.head, "advanced.rs"), "after the advance");
        assert_eq!(fx.blob(&landed.head, "mine.rs"), "the batch's work");
    }

    /// A commit parented at `parent` carrying `files`, made in a scratch worktree
    /// so the colocated checkout is never touched.
    fn side_commit(fx: &Straddle, parent: &str, files: &[(&str, &str)]) -> String {
        let worktree = tempfile::tempdir().unwrap();
        let path = worktree.path().join("side");
        git(
            &fx.repository,
            &[
                "worktree",
                "add",
                "-q",
                "--detach",
                &path.to_string_lossy(),
                parent,
            ],
        );
        for (name, content) in files {
            std::fs::write(path.join(name), content).unwrap();
        }
        git(&path, &["add", "-A"]);
        git(&path, &["commit", "-q", "-m", "a sibling lands"]);
        let commit = git_stdout(&path, &["rev-parse", "HEAD"]);
        git(
            &fx.repository,
            &["worktree", "remove", "--force", &path.to_string_lossy()],
        );
        commit
    }

    /// The compare-and-set behind preparing outside the lock (I3). A batch is
    /// integrated against the head it finds, then a sibling lands before the
    /// batch reaches the lock. The prepared plan must be discarded and the
    /// integration done again under the lock, so the batch lands on the
    /// sibling exactly as a fully locked publication would have.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_head_that_moves_after_preparation_is_integrated_again_under_the_lock() {
        let Some(fx) = straddle(None, &[("mine.rs", "the batch's work\n")]).await else {
            eprintln!("skipping a_head_that_moves_after_preparation: jj not resolvable");
            return;
        };
        let orch = orchestrator().await;

        let prepared =
            prepare_slot_publication(&orch, &fx.target, &fx.request, &fx.delta, "the batch", None)
                .await
                .expect("the delta prepares");
        assert_eq!(
            prepared.plan.as_ref().map(|plan| plan.head.as_str()),
            Some(fx.base.as_str()),
            "planned against the head the bookmark held at preparation"
        );

        let sibling = side_commit(&fx, &fx.base, &[("advanced.rs", "after the advance\n")]);
        let moved = crate::jj::publish_logical_head_exported(
            &fx.jj,
            &fx.repository,
            "feature",
            &fx.base,
            crate::jj::ProposedPublication::DeltaCommit(sibling),
            None,
            cairn_vcs::PublicationMode::Child {
                description: "a sibling lands".into(),
            },
        )
        .await
        .unwrap()
        .landed
        .head;

        let mut publication = land_prepared_slot_delta(
            &orch,
            &fx.target,
            &fx.request,
            &fx.delta,
            "the batch",
            None,
            None,
            prepared,
        )
        .await
        .expect("the batch still publishes onto the moved head");
        publication.outcome.capture_patch(&fx.repository);
        let SlotPublicationOutcome::Published {
            landed,
            export,
            patch,
            integration,
            ..
        } = publication.outcome
        else {
            panic!("expected a publication");
        };
        export.expect("exported");
        assert_eq!(
            integration.expect("reported as integrated").head,
            moved,
            "integrated onto the head found under the lock, not the stale plan's"
        );
        assert_eq!(
            git_stdout(&fx.repository, &["rev-parse", &format!("{}^", landed.head)]),
            moved
        );
        assert_eq!(fx.bookmark(), landed.head);
        assert_eq!(fx.blob(&landed.head, "advanced.rs"), "after the advance");
        assert_eq!(fx.blob(&landed.head, "mine.rs"), "the batch's work");
        assert_eq!(published_paths(&patch), ["mine.rs"]);
    }

    /// With the head unmoved, the prepared plan is the one that lands: the
    /// sealed delta publishes untouched onto the head it was planned against.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn an_unmoved_head_lands_the_prepared_plan() {
        let Some(fx) = straddle(None, &[("mine.rs", "the batch's work\n")]).await else {
            eprintln!("skipping an_unmoved_head_lands_the_prepared_plan: jj not resolvable");
            return;
        };
        let orch = orchestrator().await;
        let prepared =
            prepare_slot_publication(&orch, &fx.target, &fx.request, &fx.delta, "the batch", None)
                .await
                .unwrap();
        assert!(matches!(
            prepared.plan.as_ref().map(|plan| &plan.landing),
            Some(PlannedLanding::Land {
                integrated: false,
                ..
            })
        ));
        let publication = land_prepared_slot_delta(
            &orch,
            &fx.target,
            &fx.request,
            &fx.delta,
            "the batch",
            None,
            None,
            prepared,
        )
        .await
        .unwrap();
        let SlotPublicationOutcome::Published {
            landed,
            integration,
            ..
        } = publication.outcome
        else {
            panic!("expected a publication");
        };
        assert!(integration.is_none());
        assert_eq!(
            git_stdout(&fx.repository, &["rev-parse", &format!("{}^", landed.head)]),
            fx.base
        );
    }

    /// Nothing about the ordinary path changes. With the bookmark still at the
    /// commit the batch was routed against, the sealed delta publishes exactly
    /// as it was sealed and nothing is reported as integrated.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn an_unmoved_bookmark_publishes_the_sealed_delta_untouched() {
        let Some(fx) = straddle(None, &[("mine.rs", "the batch's work\n")]).await else {
            eprintln!("skipping an_unmoved_bookmark_publishes_the_sealed_delta: jj not resolvable");
            return;
        };
        let orch = orchestrator().await;

        let publication = publish_visible_slot_delta(
            &orch,
            &fx.target,
            &fx.request,
            &fx.delta,
            "the batch",
            None,
            None,
        )
        .await
        .expect("an unmoved bookmark publishes");
        let SlotPublicationOutcome::Published {
            landed,
            export,
            patch,
            integration,
            ..
        } = publication.outcome
        else {
            panic!("expected a publication");
        };

        export.expect("the publication exports its bookmark to the backing checkout");
        assert!(integration.is_none());
        assert_eq!(
            git_stdout(&fx.repository, &["rev-parse", &format!("{}^", landed.head)]),
            fx.base
        );
        assert_eq!(published_paths(&patch), ["mine.rs"]);
    }

    /// `commit_msg: "^"` rewrites the commit the bookmark holds, which after a
    /// move is not the one this batch built on. It becomes a child of the moved
    /// head keeping that head's description, so a sibling's landed commit is
    /// never edited out from under it.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn an_amend_that_straddles_becomes_a_child_of_the_moved_head() {
        let Some(fx) = straddle(
            Some(&[("advanced.rs", "after the advance\n")]),
            &[("mine.rs", "the batch's work\n")],
        )
        .await
        else {
            eprintln!("skipping an_amend_that_straddles: jj not resolvable");
            return;
        };
        let orch = orchestrator().await;

        let publication =
            publish_visible_slot_delta(&orch, &fx.target, &fx.request, &fx.delta, "^", None, None)
                .await
                .expect("a straddled amend still publishes");
        let SlotPublicationOutcome::Published {
            landed,
            integration,
            ..
        } = publication.outcome
        else {
            panic!("expected a publication");
        };

        assert!(integration.expect("reported").amend_converted);
        assert_eq!(
            git_stdout(&fx.repository, &["rev-parse", &format!("{}^", landed.head)]),
            fx.head,
            "the sibling's commit is still on the branch rather than rewritten"
        );
        assert_eq!(
            fx.description(&landed.head),
            "a sibling lands",
            "the converted amend keeps the head's own description"
        );
    }

    /// Stage `fx`'s delta as a managed upload the way an executor's object
    /// channel does, and hang the resulting receipt on the delta. Returns the
    /// receipt so a test can check whether the runner finalized it.
    fn stage_managed_upload(
        orch: &Orchestrator,
        fx: &mut Straddle,
        staging: &std::path::Path,
    ) -> cairn_common::executor_protocol::DeltaUploadReceipt {
        let (pack, index) = cairn_codec::transfer::build_delta_pack(
            &fx.repository,
            &fx.delta.delta_commit,
            &fx.base,
        )
        .unwrap()
        .expect("the delta is packable");
        // Staged bytes are the raw pack; the receipt's content hash is over the
        // framed envelope. Reframed from the independently derived index, which
        // is what the upload route validates against.
        let validated = cairn_codec::transfer::validate_pack(
            &pack,
            cairn_codec::transfer::PackLimits::default(),
        )
        .unwrap();
        assert_eq!(
            validated.index, index,
            "the delta pack index is deterministic"
        );
        let framed = cairn_codec::transfer::frame_pack(&pack, &validated.index);
        let receipt = cairn_common::executor_protocol::DeltaUploadReceipt {
            receipt_id: "receipt-3231".into(),
            coordinate: cairn_common::executor_protocol::ObjectTransferCoordinate {
                repository: fx.request.repository.identity(),
                request_id: fx.request.request_id.clone(),
                attempt_id: fx.request.attempt_id.clone(),
                executor_id: "executor".into(),
                connection_generation: 1,
            },
            base_commit: fx.base.clone(),
            delta_commit: fx.delta.delta_commit.clone(),
            content_hash: crate::orchestrator::object_plane::content_sha256(&framed),
            pack_checksum: validated.manifest.pack_checksum.clone(),
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        orch.object_plane
            .issue_credential("executor", "device", "runner", 1, now);
        orch.object_plane
            .stage_delta(staging, receipt.clone(), &pack)
            .unwrap();
        fx.delta.upload_receipt = Some(receipt.clone());
        receipt
    }

    /// A managed upload is validated and installed before the runner discovers
    /// the branch already carries its content, so it has been fully handled and
    /// its receipt has to be finalized — exactly as on the published path. This
    /// is what the flag living beside the outcome rather than inside it buys:
    /// no branch can be written that skips it.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn an_already_landed_managed_delta_still_finalizes_its_upload() {
        let Some(mut fx) = straddle(
            Some(&[("mine.rs", "the same content\n")]),
            &[("mine.rs", "the same content\n")],
        )
        .await
        else {
            eprintln!("skipping an_already_landed_managed_delta: jj not resolvable");
            return;
        };
        let orch = orchestrator().await;
        let staging = tempfile::tempdir().unwrap();
        let receipt = stage_managed_upload(&orch, &mut fx, staging.path());
        assert!(orch.object_plane.staged_delta(&receipt).is_some());

        let published = publish_and_seal_slot_delta(
            &orch,
            &fx.repository,
            &fx.request,
            &fx.delta,
            "feature",
            "fix: apply write-check changes",
            None,
        )
        .await
        .expect("an already-landed managed delta is a success, not a failure");

        assert_eq!(
            published.commit, fx.head,
            "nothing new was committed: the reported commit is the head that already carries it"
        );
        assert!(published.paths.is_empty());
        assert!(
            !published.landed_on_tested_base,
            "the head carrying the fix is a sibling's commit, not the one the batch ran on"
        );
        assert_eq!(
            fx.bookmark(),
            fx.head,
            "no empty commit was placed on the branch"
        );
        assert!(
            orch.object_plane.staged_delta(&receipt).is_none(),
            "the upload was installed and fully handled, so its receipt must not be left staged"
        );
    }

    /// A write-check fix published while the branch stood still lands on the
    /// commit the batch tested; one merged onto a head that advanced mid-batch
    /// does not, and says so, because the published commit then carries code no
    /// check in the batch ran against.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_fix_merged_onto_an_advanced_head_reports_it_left_the_tested_base() {
        for (advance, expected) in [
            (None, true),
            (Some(&[("advanced.rs", "after\n")][..]), false),
        ] {
            let Some(fx) = straddle(advance, &[("mine.rs", "formatted\n")]).await else {
                eprintln!("skipping a_fix_merged_onto_an_advanced_head: jj not resolvable");
                return;
            };
            let orch = orchestrator().await;
            let published = publish_and_seal_slot_delta(
                &orch,
                &fx.repository,
                &fx.request,
                &fx.delta,
                "feature",
                "fix: apply write-check changes",
                None,
            )
            .await
            .expect("a clean fix publishes");
            assert_eq!(
                published.landed_on_tested_base,
                expected,
                "advance={}",
                advance.is_some()
            );
            assert_eq!(fx.blob(&published.commit, "mine.rs"), "formatted");
        }
    }

    /// The one publication failure that leaves real work behind. Divergent edits
    /// to one file cannot be merged, so nothing is published, the sealed delta is
    /// kept alive under a durable ref, and the agent is told what collided and
    /// how to replay it.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_straddle_that_cannot_merge_strands_the_delta_and_says_so() {
        let Some(fx) = straddle(
            Some(&[("mine.rs", "the sibling's version\n")]),
            &[("mine.rs", "the batch's version\n")],
        )
        .await
        else {
            eprintln!("skipping a_straddle_that_cannot_merge: jj not resolvable");
            return;
        };
        let orch = orchestrator().await;

        let error = publish_visible_slot_delta(
            &orch,
            &fx.target,
            &fx.request,
            &fx.delta,
            "the batch",
            None,
            None,
        )
        .await
        .expect_err("divergent edits to one file cannot be merged silently");
        let SlotPublicationError::Straddled(text) = error else {
            panic!("a straddle conflict is not an ordinary publication failure");
        };

        assert!(text.contains("mine.rs"), "{text}");
        assert!(text.contains(&fx.delta.delta_commit), "{text}");
        assert_eq!(
            git_stdout(
                &fx.repository,
                &["rev-parse", "refs/cairn/stranded/attempt-3231"]
            ),
            fx.delta.delta_commit,
            "the sealed delta is kept alive so the route back stays open"
        );
        assert_eq!(
            fx.bookmark(),
            fx.head,
            "the bookmark stayed exactly where the sibling put it"
        );
    }

    /// Hold `fx`'s delta the way a failed publication does: the store takes
    /// its objects, then the branch transaction fails. Publishing to a branch
    /// that does not exist yet is the deterministic stand-in for lock
    /// contention that outlasts patience.
    async fn hold(orch: &Orchestrator, fx: &mut Straddle, branch: &str) -> held::HeldDelta {
        fx.target.branch = branch.into();
        match publish_visible_slot_delta(
            orch,
            &fx.target,
            &fx.request,
            &fx.delta,
            "the batch",
            None,
            None,
        )
        .await
        {
            Err(SlotPublicationError::Held { held, .. }) => held,
            other => panic!("expected the delta to be held, got {other:?}"),
        }
    }

    fn create_bookmark(fx: &Straddle, name: &str, at: &str) {
        fx.jj
            .run(
                &fx.repository,
                &[
                    "bookmark",
                    "create",
                    name,
                    "-r",
                    at,
                    "--ignore-working-copy",
                ],
                "create a bookmark",
            )
            .unwrap();
    }

    fn bookmark_at(fx: &Straddle, name: &str) -> String {
        fx.jj
            .run(
                &fx.repository,
                &[
                    "log",
                    "-r",
                    name,
                    "--no-graph",
                    "-T",
                    "commit_id",
                    "--ignore-working-copy",
                ],
                "read a bookmark",
            )
            .unwrap()
    }

    fn ref_exists(fx: &Straddle, reference: &str) -> bool {
        std::process::Command::new("git")
            .args(["rev-parse", "--verify", "--quiet", reference])
            .current_dir(&fx.repository)
            .status()
            .unwrap()
            .success()
    }

    async fn held_status(orch: &Orchestrator, attempt_id: &str) -> (String, i64) {
        orch.db
            .local
            .query_opt(
                "SELECT status, attempts FROM held_publications WHERE attempt_id = ?1",
                (attempt_id.to_string(),),
                |row| Ok((row.text(0)?, row.i64(1)?)),
            )
            .await
            .unwrap()
            .expect("the hold was recorded")
    }

    /// A second batch routed at the same base but opened on the first batch's
    /// unpublished commit, as an execution home that carried that commit
    /// forward seals it: its parent is the first delta, and its own diff is
    /// `files`.
    fn absorbing_delta(
        fx: &Straddle,
        files: &[(&str, &str)],
    ) -> (CellRequest, crate::fleet::MutationDelta) {
        // Through a side index, never the working tree: jj owns the colocated
        // checkout's HEAD here, and a checkout would fight it.
        let path = fx.repository.as_path();
        let index = tempfile::tempdir().unwrap();
        let index = index.path().join("index");
        let plumb = |args: &[&str], input: Option<&str>| -> String {
            use std::io::Write as _;
            let mut child = std::process::Command::new("git")
                .args(args)
                .env("GIT_INDEX_FILE", &index)
                .current_dir(path)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(input.unwrap_or_default().as_bytes())
                .unwrap();
            let output = child.wait_with_output().unwrap();
            assert!(output.status.success(), "git {args:?} failed");
            String::from_utf8(output.stdout).unwrap().trim().to_string()
        };
        plumb(&["read-tree", &fx.delta.delta_commit], None);
        for (name, content) in files {
            let blob = plumb(&["hash-object", "-w", "--stdin"], Some(content));
            plumb(
                &[
                    "update-index",
                    "--add",
                    "--cacheinfo",
                    &format!("100644,{blob},{name}"),
                ],
                None,
            );
        }
        let tree = plumb(&["write-tree"], None);
        let commit = git_stdout(
            path,
            &[
                "commit-tree",
                &tree,
                "-p",
                &fx.delta.delta_commit,
                "-m",
                "the next batch",
            ],
        );
        let request = CellRequest {
            attempt_id: "attempt-next".into(),
            request_id: "request-next".into(),
            ..fx.request.clone()
        };
        let delta = crate::fleet::MutationDelta {
            base_commit: fx.base.clone(),
            delta_commit: commit,
            upload_receipt: None,
        };
        (request, delta)
    }

    /// The defect this exists for. The store took the delta and the branch did
    /// not move: the commit is kept under a ref, scheduled for republication,
    /// and the agent is told its work is committed and must not be redone.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_publication_that_fails_after_install_holds_the_delta() {
        let Some(mut fx) = straddle(None, &[("mine.rs", "the batch's work\n")]).await else {
            eprintln!("skipping a_publication_that_fails_after_install: jj not resolvable");
            return;
        };
        let orch = orchestrator().await;

        let held = hold(&orch, &mut fx, "later").await;

        let reference = held.reference.clone().expect("the delta is ref-protected");
        assert_eq!(reference, "refs/cairn/stranded/attempt-3231");
        assert_eq!(
            git_stdout(&fx.repository, &["rev-parse", &reference]),
            fx.delta.delta_commit
        );
        held.recorded.as_ref().expect("the hold is recorded");
        assert_eq!(held_status(&orch, "attempt-3231").await, ("held".into(), 1));
        assert_eq!(fx.bookmark(), fx.base, "no other branch moved");

        let message = held.message();
        assert!(message.contains(&fx.delta.delta_commit), "{message}");
        assert!(message.contains(&reference), "{message}");
        assert!(message.contains("Do not redo"), "{message}");
        assert!(!message.contains("not rerun locally"), "{message}");
        // The message's own wording is guarded in `held::tests`; this one embeds
        // the underlying publication error verbatim, which is not ours to word.
    }

    /// Republication lands the held commit exactly as it was sealed, under the
    /// batch's own message, and retires both the row and the ref.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_drain_lands_a_held_delta_once_the_branch_can_take_it() {
        let Some(mut fx) = straddle(None, &[("mine.rs", "the batch's work\n")]).await else {
            eprintln!("skipping a_drain_lands_a_held_delta: jj not resolvable");
            return;
        };
        let orch = orchestrator().await;
        let held = hold(&orch, &mut fx, "later").await;
        create_bookmark(&fx, "later", &fx.base);

        let drained = held::drain_held_publications(&orch, "project", "later", None).await;

        assert!(held::any_landed(&drained), "{drained:?}");
        let landed = bookmark_at(&fx, "later");
        assert_eq!(
            git_stdout(
                &fx.repository,
                &["rev-parse", &format!("{landed}^{{tree}}")]
            ),
            git_stdout(
                &fx.repository,
                &["rev-parse", &format!("{}^{{tree}}", fx.delta.delta_commit)]
            ),
        );
        assert_eq!(
            git_stdout(&fx.repository, &["rev-parse", &format!("{landed}^")]),
            fx.base
        );
        assert_eq!(fx.description(&landed), "the batch");
        assert_eq!(
            held_status(&orch, "attempt-3231").await.0,
            "unpublished",
            "a landing owes its push until it is settled"
        );
        held::settle_drained(&orch, drained).await;
        assert_eq!(
            held_status(&orch, "attempt-3231").await.0,
            "landed",
            "with no remote to push to, nothing is owed"
        );
        assert!(!ref_exists(&fx, &held.reference.unwrap()));
        assert!(
            held::drain_held_publications(&orch, "project", "later", None)
                .await
                .is_empty(),
            "a landed delta is not drained twice"
        );
    }

    /// A held delta meets the branch wherever it has moved to: merged onto a
    /// sibling's disjoint work, or stranded with the route back when it
    /// conflicts.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_drain_merges_onto_a_moved_head_or_strands_on_conflict() {
        for (sibling, conflicts) in [
            (("advanced.rs", "after the advance\n"), false),
            (("mine.rs", "the sibling's version\n"), true),
        ] {
            let Some(mut fx) =
                straddle(Some(&[sibling]), &[("mine.rs", "the batch's version\n")]).await
            else {
                eprintln!("skipping a_drain_merges_onto_a_moved_head: jj not resolvable");
                return;
            };
            let orch = orchestrator().await;
            let held = hold(&orch, &mut fx, "later").await;
            create_bookmark(&fx, "later", &fx.head);

            let drained = held::drain_held_publications(&orch, "project", "later", None).await;

            let reference = held.reference.unwrap();
            if conflicts {
                let [held::Drained::Note(note)] = drained.as_slice() else {
                    panic!("expected one note, got {drained:?}");
                };
                assert!(note.contains("can no longer be published"), "{note}");
                assert!(note.contains("mine.rs"), "{note}");
                assert_eq!(held_status(&orch, "attempt-3231").await.0, "stranded");
                assert!(ref_exists(&fx, &reference), "the route back stays open");
                assert_eq!(bookmark_at(&fx, "later"), fx.head);
            } else {
                assert!(held::any_landed(&drained), "{drained:?}");
                let landed = bookmark_at(&fx, "later");
                assert_eq!(
                    git_stdout(&fx.repository, &["rev-parse", &format!("{landed}^")]),
                    fx.head
                );
                assert_eq!(fx.blob(&landed, "advanced.rs"), "after the advance");
                assert_eq!(fx.blob(&landed, "mine.rs"), "the batch's version");
                assert!(!ref_exists(&fx, &reference));
            }
        }
    }

    /// A batch that opened on a held commit and then edited it means exactly
    /// what it did: a new path, a further edit of the held commit's line, or an
    /// undo of it. Merged against its own parent once the held commit lands,
    /// all three come out right, in either order of arrival. Publishing the
    /// later batch first is refused: it waits behind the held commit, so the
    /// held commit can never land afterwards and re-apply what was undone.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_batch_built_on_a_held_commit_lands_as_its_own_edit_in_either_order() {
        // (what the batch did, the file it wrote, that file's content, paths it touched)
        type Case<'a> = (&'a str, (&'a str, &'a str), &'a str, &'a [&'a str]);
        let cases: [Case; 3] = [
            (
                "a new path",
                ("other.rs", "the next batch\n"),
                "the batch's work",
                &["other.rs"],
            ),
            (
                "a further edit",
                ("mine.rs", "a third version\n"),
                "a third version",
                &["mine.rs"],
            ),
            ("an undo", ("mine.rs", "base\n"), "base", &["mine.rs"]),
        ];
        for (name, edit, mine_after, own_paths) in cases {
            for drain_first in [true, false] {
                let Some(mut fx) = straddle(None, &[("mine.rs", "the batch's work\n")]).await
                else {
                    eprintln!("skipping a_batch_built_on_a_held_commit: jj not resolvable");
                    return;
                };
                let orch = orchestrator().await;
                hold(&orch, &mut fx, "later").await;
                create_bookmark(&fx, "later", &fx.base);
                let (next_request, next_delta) = absorbing_delta(&fx, &[edit]);
                let publish_next = || {
                    publish_visible_slot_delta(
                        &orch,
                        &fx.target,
                        &next_request,
                        &next_delta,
                        "the next batch",
                        None,
                        None,
                    )
                };

                if drain_first {
                    let drained =
                        held::drain_held_publications(&orch, "project", "later", None).await;
                    assert!(held::any_landed(&drained), "{name}: {drained:?}");
                    held::settle_drained(&orch, drained).await;
                    let SlotPublicationOutcome::Published { patch, .. } = publish_next()
                        .await
                        .unwrap_or_else(|error| panic!("{name}: {error}"))
                        .outcome
                    else {
                        panic!("{name}: expected the next batch to publish");
                    };
                    assert_eq!(
                        published_paths(&patch),
                        own_paths,
                        "{name}: the next batch is credited only with its own paths"
                    );
                } else {
                    let refused = publish_next().await;
                    let Err(SlotPublicationError::Held { held, .. }) = &refused else {
                        panic!("{name}: expected the next batch to wait, got {refused:?}");
                    };
                    assert!(
                        held.error.contains("waits behind it"),
                        "{name}: {}",
                        held.error
                    );
                    let drained =
                        held::drain_held_publications(&orch, "project", "later", None).await;
                    assert_eq!(
                        drained
                            .iter()
                            .filter(|entry| matches!(entry, held::Drained::Landed(_)))
                            .count(),
                        2,
                        "{name}: both land, in order: {drained:?}"
                    );
                    held::settle_drained(&orch, drained).await;
                    assert_eq!(held_status(&orch, "attempt-next").await.0, "landed");
                }
                assert_eq!(held_status(&orch, "attempt-3231").await.0, "landed");
                let head = bookmark_at(&fx, "later");
                assert_eq!(
                    fx.blob(&head, "mine.rs"),
                    mine_after,
                    "{name} (drain first: {drain_first})"
                );
            }
        }
    }

    /// A carried commit the runner never received (it never reached the store,
    /// so nothing holds it) is published together with the batch built on it,
    /// rather than lost or waited on forever.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_carried_commit_nothing_holds_is_published_with_the_batch_built_on_it() {
        let Some(fx) = straddle(None, &[("mine.rs", "the batch's work\n")]).await else {
            eprintln!("skipping a_carried_commit_nothing_holds: jj not resolvable");
            return;
        };
        let orch = orchestrator().await;
        let (next_request, next_delta) = absorbing_delta(&fx, &[("other.rs", "next\n")]);

        let SlotPublicationOutcome::Published { landed, .. } = publish_visible_slot_delta(
            &orch,
            &fx.target,
            &next_request,
            &next_delta,
            "the next batch",
            None,
            None,
        )
        .await
        .expect("publishes")
        .outcome
        else {
            panic!("expected a publication");
        };

        assert_eq!(
            git_stdout(&fx.repository, &["rev-parse", &format!("{}^", landed.head)]),
            fx.base,
            "re-rooted onto the branch rather than proposed with a parent it never held"
        );
        assert_eq!(fx.blob(&landed.head, "mine.rs"), "the batch's work");
        assert_eq!(fx.blob(&landed.head, "other.rs"), "next");
    }

    /// A drain that lands a held commit and is then interrupted before its push
    /// (a runner stopping, or a merge arriving while the push is in flight)
    /// leaves the gate shut: the row is already blocking when the store lock
    /// is released, so the next barrier owes the push before any merge.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_drain_interrupted_before_its_push_still_blocks_the_merge() {
        let Some(mut fx) = straddle(None, &[("mine.rs", "the batch's work\n")]).await else {
            eprintln!("skipping a_drain_interrupted_before_its_push: jj not resolvable");
            return;
        };
        let orch = orchestrator().await;
        hold(&orch, &mut fx, "later").await;
        create_bookmark(&fx, "later", &fx.base);
        let unreachable = fx.repository.join("no-such-origin.git");
        git(
            &fx.repository,
            &["remote", "add", "origin", &unreachable.to_string_lossy()],
        );

        // Landed under the lock, and never settled.
        let drained = held::drain_held_publications(&orch, "project", "later", None).await;
        assert!(held::any_landed(&drained), "{drained:?}");
        drop(drained);
        assert_eq!(held_status(&orch, "attempt-3231").await.0, "unpublished");

        let refusal = held::pr_commit_barrier(&orch, "project", "later")
            .await
            .refuse_merge()
            .expect_err("the push is still owed");
        assert!(refusal.contains("has not reached the remote"), "{refusal}");
    }

    /// A held commit the branch already carries (a landing whose row was never
    /// updated, say) still owes its push. Clearing it on local content equality
    /// alone would let a merge ship the remote head without it.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_held_commit_already_on_the_branch_still_owes_its_push() {
        let Some(mut fx) = straddle(None, &[("mine.rs", "the batch's work\n")]).await else {
            eprintln!("skipping a_held_commit_already_on_the_branch: jj not resolvable");
            return;
        };
        let orch = orchestrator().await;
        hold(&orch, &mut fx, "later").await;
        // The branch already carries the held content: landed through the
        // sanctioned barrier, as a drain that never got to settle its row would
        // have landed it.
        create_bookmark(&fx, "later", &fx.base);
        crate::jj::publish_logical_head_exported(
            &fx.jj,
            &fx.repository,
            "later",
            &fx.base,
            crate::jj::ProposedPublication::DeltaCommit(fx.delta.delta_commit.clone()),
            None,
            cairn_vcs::PublicationMode::Child {
                description: "the batch".into(),
            },
        )
        .await
        .unwrap();
        let unreachable = fx.repository.join("no-such-origin.git");
        git(
            &fx.repository,
            &["remote", "add", "origin", &unreachable.to_string_lossy()],
        );

        let drained = held::drain_held_publications(&orch, "project", "later", None).await;
        assert!(
            !held::any_landed(&drained),
            "nothing moved the branch: {drained:?}"
        );
        assert_eq!(held_status(&orch, "attempt-3231").await.0, "unpublished");
        let notes = held::settle_drained(&orch, drained).await;
        assert_eq!(
            held_status(&orch, "attempt-3231").await.0,
            "unpublished",
            "the failed push leaves it owed: {notes:?}"
        );
        assert!(held::pr_commit_barrier(&orch, "project", "later")
            .await
            .refuse_merge()
            .is_err());
    }

    /// The merge gate's other half. A held commit that lands here but whose
    /// push fails is not on the pull request, so a merge would ship the old
    /// remote head without it. It stays merge-blocking across barriers until a
    /// push carries it.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_held_commit_that_lands_but_cannot_be_pushed_keeps_blocking_the_merge() {
        let Some(mut fx) = straddle(None, &[("mine.rs", "the batch's work\n")]).await else {
            eprintln!("skipping a_held_commit_that_lands_but_cannot_be_pushed: jj not resolvable");
            return;
        };
        let orch = orchestrator().await;
        hold(&orch, &mut fx, "later").await;
        create_bookmark(&fx, "later", &fx.base);
        let unreachable = fx.repository.join("no-such-origin.git");
        git(
            &fx.repository,
            &["remote", "add", "origin", &unreachable.to_string_lossy()],
        );

        let barrier = held::pr_commit_barrier(&orch, "project", "later").await;

        assert_eq!(
            fx.blob(&bookmark_at(&fx, "later"), "mine.rs"),
            "the batch's work",
            "the held commit landed here"
        );
        let refusal = barrier.refuse_merge().expect_err("the remote lacks it");
        assert!(refusal.contains("has not reached the remote"), "{refusal}");
        assert_eq!(held_status(&orch, "attempt-3231").await.0, "unpublished");
        assert!(
            held::pr_commit_barrier(&orch, "project", "later")
                .await
                .refuse_merge()
                .is_err(),
            "a later barrier still knows the remote lacks it"
        );

        let origin = tempfile::tempdir().unwrap();
        git(origin.path(), &["init", "-q", "--bare"]);
        git(
            &fx.repository,
            &[
                "remote",
                "set-url",
                "origin",
                &origin.path().to_string_lossy(),
            ],
        );
        let barrier = held::pr_commit_barrier(&orch, "project", "later").await;
        barrier
            .refuse_merge()
            .expect("once a push carries it, the merge may proceed");
        assert_eq!(held_status(&orch, "attempt-3231").await.0, "landed");
    }

    /// Held commits land in the order they were made: a drain stops at the
    /// first one it still cannot publish and leaves the rest for the next
    /// barrier.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_drain_stops_at_the_first_delta_it_still_cannot_publish() {
        let Some(mut fx) = straddle(None, &[("mine.rs", "the batch's work\n")]).await else {
            eprintln!("skipping a_drain_stops_at_the_first_delta: jj not resolvable");
            return;
        };
        let orch = orchestrator().await;
        hold(&orch, &mut fx, "later").await;
        let (next_request, next_delta) = absorbing_delta(&fx, &[("other.rs", "next\n")]);
        let next = publish_visible_slot_delta(
            &orch,
            &fx.target,
            &next_request,
            &next_delta,
            "the next batch",
            None,
            None,
        )
        .await;
        assert!(
            matches!(next, Err(SlotPublicationError::Held { .. })),
            "{next:?}"
        );

        let drained = held::drain_held_publications(&orch, "project", "later", None).await;

        let [held::Drained::Note(note)] = drained.as_slice() else {
            panic!("expected one note, got {drained:?}");
        };
        assert!(note.contains("still could not be published"), "{note}");
        assert!(note.contains(&fx.delta.delta_commit), "{note}");
        assert_eq!(held_status(&orch, "attempt-3231").await, ("held".into(), 2));
        assert_eq!(
            held_status(&orch, "attempt-next").await,
            ("held".into(), 1),
            "the later delta is not attempted ahead of the earlier one"
        );
    }

    /// A held managed upload was installed exactly as a published one is, so
    /// its staging copy is spent the same way.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_held_managed_delta_still_finalizes_its_upload() {
        let Some(mut fx) = straddle(None, &[("mine.rs", "the batch's work\n")]).await else {
            eprintln!("skipping a_held_managed_delta: jj not resolvable");
            return;
        };
        let orch = orchestrator().await;
        let staging = tempfile::tempdir().unwrap();
        let receipt = stage_managed_upload(&orch, &mut fx, staging.path());

        let error = publish_and_seal_slot_delta(
            &orch,
            &fx.repository,
            &fx.request,
            &fx.delta,
            "later",
            "fix: apply write-check changes",
            None,
        )
        .await
        .expect_err("a branch that does not exist cannot take the fix");

        assert!(error.contains("Do not redo"), "{error}");
        assert!(orch.object_plane.staged_delta(&receipt).is_none());
        assert_eq!(held_status(&orch, "attempt-3231").await.0, "held");
    }
}

#[cfg(test)]
mod publication_mode_tests {
    use super::*;

    #[test]
    fn an_ordinary_message_publishes_as_a_child() {
        let (mode, converted) = publication_mode("feat: add roles", None);
        assert_eq!(
            mode,
            cairn_vcs::PublicationMode::Child {
                description: "feat: add roles".to_string()
            }
        );
        assert!(!converted);
    }

    #[test]
    fn an_unmoved_amend_selects_managed_amend_publication() {
        let (mode, converted) = publication_mode("^", None);
        assert_eq!(mode, cairn_vcs::PublicationMode::Amend);
        assert!(!converted);
    }

    /// The head under a straddle is not the commit the batch built on, so an
    /// amend must not rewrite it — that would edit a sibling's landed work,
    /// which is worse than the failure this whole path exists to fix.
    #[test]
    fn an_amend_over_a_moved_head_becomes_a_child_of_it() {
        let (mode, converted) = publication_mode("^", Some("a sibling's landed commit\n"));
        assert_eq!(
            mode,
            cairn_vcs::PublicationMode::Child {
                description: "a sibling's landed commit".to_string()
            }
        );
        assert!(converted);

        let (mode, converted) = publication_mode("^", Some("   \n"));
        assert_eq!(
            mode,
            cairn_vcs::PublicationMode::Child {
                description: "amend".to_string()
            }
        );
        assert!(converted);
    }

    /// A straddle that cannot be merged is the one publication failure that
    /// leaves real work behind, so its text has to carry everything the agent
    /// needs to finish the job by hand.
    #[test]
    fn a_straddle_conflict_names_the_head_the_paths_and_the_route_back() {
        let message = straddled_conflict_message(
            "1111111111111111111111111111111111111111",
            "2222222222222222222222222222222222222222",
            &["src/lib.rs".to_string(), "src/main.rs".to_string()],
        );
        assert!(message.contains("1111111111111111111111111111111111111111"));
        assert!(message.contains("src/lib.rs, src/main.rs"), "{message}");
        for command in straddle_inspection_commands(
            "1111111111111111111111111111111111111111",
            "2222222222222222222222222222222222222222",
        ) {
            assert!(
                message.contains(&command),
                "{command} missing from {message}"
            );
        }
        assert!(
            message.contains("will not pick them up"),
            "the trap has to be named: a later batch commits only what it changes while it runs, \
             and these edits are already present when it starts — {message}"
        );
        assert!(
            !message.contains("cherry-pick"),
            "a replay cannot be resolved before the batch commits, so it must not be suggested"
        );
        assert!(
            !message.contains("not rerun locally"),
            "the generic tail is false here: the batch ran and its work is saved"
        );
        crate::system_prompt::assert_no_substrate_vocabulary("straddle conflict", &message);
    }

    /// The commands the message hands the agent have to work in the state the
    /// agent is actually in, which is either a clean checkout at the moved head
    /// (the ordinary case, once the next batch realigns) or one still carrying
    /// the batch's own edits. Run verbatim against a real repository in both.
    #[test]
    fn the_inspection_commands_work_from_either_state_a_straddle_leaves() {
        for still_dirty in [false, true] {
            let repo = tempfile::tempdir().unwrap();
            let path = std::fs::canonicalize(repo.path()).unwrap();
            crate::jj::tests::git(&path, &["init", "-q", "-b", "main"]);
            crate::jj::tests::git(&path, &["config", "user.email", "p@e.com"]);
            crate::jj::tests::git(&path, &["config", "user.name", "p"]);
            std::fs::write(path.join("shared.rs"), "one\ntwo\nthree\n").unwrap();
            crate::jj::tests::git(&path, &["add", "-A"]);
            crate::jj::tests::git(&path, &["commit", "-q", "-m", "base"]);
            let base = crate::jj::tests::git_stdout(&path, &["rev-parse", "HEAD"]);

            std::fs::write(path.join("shared.rs"), "one\nTHE BRANCH\nthree\n").unwrap();
            crate::jj::tests::git(&path, &["add", "-A"]);
            crate::jj::tests::git(&path, &["commit", "-q", "-m", "a sibling lands"]);
            let head = crate::jj::tests::git_stdout(&path, &["rev-parse", "HEAD"]);

            crate::jj::tests::git(&path, &["checkout", "-q", "--detach", &base]);
            std::fs::write(path.join("shared.rs"), "one\nTHE BATCH\nthree\n").unwrap();
            crate::jj::tests::git(&path, &["add", "-A"]);
            crate::jj::tests::git(&path, &["commit", "-q", "-m", "the batch"]);
            let delta = crate::jj::tests::git_stdout(&path, &["rev-parse", "HEAD"]);

            crate::jj::tests::git(&path, &["checkout", "-q", "--detach", &head]);
            if still_dirty {
                std::fs::write(path.join("shared.rs"), "one\nTHE BATCH\nthree\n").unwrap();
            }

            for command in straddle_inspection_commands(&head, &delta) {
                let output = std::process::Command::new("sh")
                    .args(["-c", &command])
                    .current_dir(&path)
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "`{command}` failed (dirty={still_dirty}): {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                assert!(
                    String::from_utf8_lossy(&output.stdout).contains("THE BATCH"),
                    "`{command}` did not show the batch's own version (dirty={still_dirty})"
                );
            }
        }
    }
}

async fn finalize_delta_receipt(
    orch: &Orchestrator,
    target: &RunnerPublicationTarget,
    delta: &crate::fleet::MutationDelta,
) -> Result<(), String> {
    let receipt = delta
        .upload_receipt
        .as_ref()
        .ok_or_else(|| "managed delta receipt disappeared before consumption".to_string())?;
    let staged = orch.object_plane.staged_delta(receipt).ok_or_else(|| {
        "managed delta receipt disappeared before catalog publication".to_string()
    })?;
    let pack = std::fs::read(&staged.path)
        .map_err(|error| format!("read installed delta for catalog: {error}"))?;
    let validated = crate::orchestrator::object_plane::validate_pack_bytes(pack)
        .map_err(|error| format!("validate installed delta for catalog: {error}"))?;
    if validated.content_hash != receipt.content_hash {
        return Err("installed delta no longer matches its cloud object".into());
    }
    let db =
        match orch
            .db
            .team_id_for_project(&target.project_id)
            .await
            .map_err(|error| error.to_string())?
        {
            Some(team_id) => orch.db.team_db(&team_id).await.ok_or_else(|| {
                "team database closed before delta catalog publication".to_string()
            })?,
            None => orch.db.local.clone(),
        };
    crate::orchestrator::object_plane::publish_validated_reference(
        &db,
        &validated,
        crate::storage::pack_catalog::PackCatalogPublication {
            content_hash: String::new(),
            project_id: target.project_id.clone(),
            repository_id: target.repository_identity.repository_id.clone(),
            object_format: "sha1".into(),
            byte_count: 0,
            pack_checksum: String::new(),
            object_count: 0,
            kind: crate::storage::pack_catalog::PackKind::MutationDelta,
            base_commit: Some(delta.base_commit.clone()),
            tip_commit: delta.delta_commit.clone(),
            owner_kind: "mutation_delta".into(),
            owner_id: receipt.receipt_id.clone(),
        },
    )
    .await
    .map_err(|error| format!("publish mutation delta catalog: {error}"))?;
    orch.object_plane.consume_staged_delta(receipt)?;
    Ok(())
}

use hygiene::check_cd_commands;
use output::{collect_run_images, compose_run_output, run_envelope, run_input_refusal};
use process::run_one;
pub(crate) use process::READ_ONLY_CHECKOUT_DENIAL;
use resolve::resolve_run_item;
use types::ItemOutcome;
pub(crate) use types::RunSpec;

/// The command label and class a slot-bound batch's `CellRequest` carries.
///
/// The label is the items' headers, the agent's descriptions when it gave them,
/// because the label is what the queue and the desktop show. It is never an
/// executable command line: the executor runs, and classifies its dependency
/// catalog from, the batch's items (`cairn_exec::dependency_catalog_launch`).
/// The class alone is judged from the executable commands, since it is a
/// placement input the label cannot answer.
pub(crate) fn build_slot_command_parts(
    resolved: &[(String, Result<RunSpec, String>)],
) -> (String, cairn_common::executor_protocol::CellCommandClass) {
    let display_command = resolved
        .iter()
        .map(|(header, _)| header.as_str())
        .collect::<Vec<_>>()
        .join(" && ");
    let executable_command = resolved
        .iter()
        .filter_map(|(_, spec)| match spec {
            Ok(RunSpec::Shell { command, .. }) => Some(command.clone()),
            Ok(RunSpec::Script { program, args, .. }) => Some(
                std::iter::once(program.as_str())
                    .chain(args.iter().map(String::as_str))
                    .collect::<Vec<_>>()
                    .join(" "),
            ),
            Ok(
                RunSpec::McpCall(_)
                | RunSpec::ExecutorAction(_)
                | RunSpec::ResponseCall { .. }
                | RunSpec::Decide { .. }
                | RunSpec::ReplSend { .. },
            )
            | Err(_) => None,
        })
        .collect::<Vec<_>>()
        .join(" && ");
    let command_class = cairn_common::executor_protocol::CellCommandClass::classify(
        if executable_command.is_empty() {
            &display_command
        } else {
            &executable_command
        },
    );
    (display_command, command_class)
}

/// The largest kill bound a `run` item may carry. "Too long" has exactly one
/// authority — [`RUN_BATCH_CEILING`] — and this is that ceiling expressed as an
/// item budget, so an item can never be killed by a quieter bound underneath the
/// one the batch already enforces.
const MAX_RUN_ITEM_TIMEOUT_MS: u32 = cairn_common::run_contract::RUN_BATCH_CEILING_MS;

/// The single clamp that turns a requested `timeout` into a real kill bound.
///
/// `None` means "run to completion": the item is bounded only by the batch
/// ceiling, which is the same loud guard a never-exiting command already hits.
/// An explicit value is honored up to that ceiling and never converted into
/// anything else.
///
/// Every layer that needs an item's budget routes through here — the executor
/// batch, the host-local item, the REPL send — so the maximum the schema
/// advertises is provably the maximum enforced. Three layers once answered this
/// question independently with two different answers, and the smallest of them
/// discarded a full test suite's output at eleven minutes.
pub(crate) fn clamp_run_item_timeout_ms(requested: Option<u32>) -> u32 {
    requested
        .unwrap_or(MAX_RUN_ITEM_TIMEOUT_MS)
        .min(MAX_RUN_ITEM_TIMEOUT_MS)
}

/// The largest kill bound a host item that cannot suspend may carry: a decide,
/// a REPL send, or an executor action.
///
/// A batch of only these items never suspends ([`HostBatch::may_outlive_grace`]),
/// so the call stays attached to its socket. The host answers within the grace
/// window, so such an item that outlived grace could not deliver its result at
/// all: the transport gives up first and the whole batch is discarded with no
/// output. Capping the bound at grace converts that silent transport death into
/// an ordinary item timeout with a real result block.
///
/// Response and MCP-tool items are host items too, but their batch suspends
/// ([`run_suspendable_host_batch`]), so they take the run-item ceiling instead
/// ([`response_item_bound`], [`mcp_item_bound`]).
pub(crate) const MAX_HOST_ITEM_TIMEOUT_MS: u32 =
    cairn_common::run_contract::RUN_GRACE_WINDOW_MS as u32;

/// The ordering between the two item bounds is the whole point, so it is checked
/// at compile time rather than by a test that could be deleted.
const _: () = assert!(MAX_HOST_ITEM_TIMEOUT_MS < MAX_RUN_ITEM_TIMEOUT_MS);

/// The clamp for the kill bound of a host item that cannot suspend. `None` is
/// preserved rather than filled, so a caller with a meaningful default of its
/// own below this bound keeps it; "run to completion" is a promise only a batch
/// that can suspend can keep.
pub(crate) fn clamp_host_item_timeout_ms(requested: Option<u32>) -> Option<u32> {
    requested.map(|ms| ms.min(MAX_HOST_ITEM_TIMEOUT_MS))
}

/// Why a decision made from `run` cannot outlive [`MAX_HOST_ITEM_TIMEOUT_MS`],
/// for the agent whose call just hit it. A `cairn.run` from @cairn/sdk inside a
/// shell or script item lands on this same host path, so it is named too;
/// otherwise it reads as the way around the cap.
///
/// Decide keeps the cap on purpose: it is single-pass, bounded by its own
/// per-request and probe timeouts, and a decision that needs minutes is the
/// wrong verb rather than a reason to suspend.
pub(crate) const HOST_ITEM_CAP_WHY: &str = "Decide items run on the host and cannot suspend past it, and a `cairn.run` call from @cairn/sdk inside a shell or script item is held to the same cap, so there is no longer route to retry through.";

/// The bound a decide item runs under: its own ask when that fits under the
/// host-item cap, the cap otherwise. The ask is carried along so a timeout can
/// say which of the two decided it.
pub(crate) fn host_item_bound(requested: Option<u32>) -> crate::responses::InvocationBound {
    let limit = clamp_host_item_timeout_ms(requested).unwrap_or(MAX_HOST_ITEM_TIMEOUT_MS);
    let duration = |ms: u32| std::time::Duration::from_millis(u64::from(ms));
    crate::responses::InvocationBound {
        limit: duration(limit),
        cap: (requested != Some(limit)).then(|| crate::responses::BoundCap {
            name: "host-item cap",
            requested: requested.map(duration),
            why: HOST_ITEM_CAP_WHY,
        }),
    }
}

/// Why a response or MCP-tool item cannot outlive the run-item ceiling.
pub(crate) const RUN_ITEM_CEILING_WHY: &str = "A response or MCP-tool item suspends with its batch and runs to its own bound, up to the 6-hour ceiling on a single `run` batch, which no item outlives.";

/// The bound a response item runs under: its own ask up to the run-item
/// ceiling, and the ceiling when it asked for nothing, so that the definition's
/// own timeout governs each provider request. A response item can reach the
/// ceiling because its batch suspends ([`run_suspendable_host_batch`]).
pub(crate) fn response_item_bound(requested: Option<u32>) -> crate::responses::InvocationBound {
    let limit = clamp_run_item_timeout_ms(requested);
    let duration = |ms: u32| std::time::Duration::from_millis(u64::from(ms));
    crate::responses::InvocationBound {
        limit: duration(limit),
        cap: (requested != Some(limit)).then(|| crate::responses::BoundCap {
            name: "run-item ceiling",
            requested: requested.map(duration),
            why: RUN_ITEM_CEILING_WHY,
        }),
    }
}

/// Why an MCP-tool item that declared no `timeout` is held to the gateway's
/// default rather than left to run to completion.
pub(crate) const MCP_GATEWAY_DEFAULT_WHY: &str = "An external server is arbitrary code, so a call that declares no bound is held to the gateway's default; declare `timeout` on the item, up to the 6-hour run-item ceiling, for a call you know runs long.";

/// The bound an MCP-tool item runs under: its own ask up to the run-item
/// ceiling, and the gateway's default when it asked for nothing. An undeclared
/// call to arbitrary code never rides the six-hour horizon. It can reach the
/// ceiling because its batch suspends ([`run_suspendable_host_batch`]).
pub(crate) fn mcp_item_bound(requested: Option<u32>) -> crate::responses::InvocationBound {
    match requested {
        Some(_) => response_item_bound(requested),
        None => crate::responses::InvocationBound {
            limit: crate::mcp::gateway::DEFAULT_MCP_TIMEOUT,
            cap: Some(crate::responses::BoundCap {
                name: "MCP gateway default",
                requested: None,
                why: MCP_GATEWAY_DEFAULT_WHY,
            }),
        },
    }
}

/// Why an item that could otherwise suspend ended at the host-item window: the
/// call had nothing to park on, so its answer had to arrive while the
/// transport above it was still waiting.
pub(crate) const UNPARKED_HOST_BATCH_WHY: &str = "This call had no `run` tool call of its own to suspend on, so it had to answer inside the window its transport waits for. A `cairn.run` from @cairn/sdk inside a shell or script item is the usual case; two byte-identical `run` calls issued together are the other. Make the call as a top-level `run` item to let it run to its declared timeout.";

/// What a response item journals, and an MCP-tool item reports, when the run
/// that called it was stopped.
pub(crate) const RUN_STOPPED_REASON: &str = "the run that called it was stopped before it answered";

/// What a response item journals, and an MCP-tool item reports, when the `run`
/// call carrying it was dropped before it could suspend (the transport went
/// away).
const CALL_ABANDONED_REASON: &str =
    "the `run` call that issued it was abandoned before it answered";

/// How often a suspended host batch looks for the stop that cancels it. A stop
/// marks the suspension row and nothing else, and only this batch's own task
/// can end its response and MCP-tool items, so it has to look.
const HOST_BATCH_STOP_POLL: std::time::Duration = std::time::Duration::from_secs(1);

/// What this batch may legally take: sequential items run one after another,
/// parallel items overlap, and the whole is bounded by the ceiling.
/// `CellRequest.timeout_ms` is the request's own bound, so it must say this
/// rather than a fleet default that no item in the batch is held to.
fn batch_execution_budget_ms(
    resolved: &[(String, Result<RunSpec, String>)],
    sequential: bool,
) -> u32 {
    let budgets: Vec<u32> = resolved
        .iter()
        .filter_map(|(_, spec)| match spec {
            Ok(RunSpec::Shell { timeout, .. } | RunSpec::Script { timeout, .. }) => {
                Some(clamp_run_item_timeout_ms(*timeout))
            }
            _ => None,
        })
        .collect();
    if budgets.is_empty() {
        return MAX_RUN_ITEM_TIMEOUT_MS;
    }
    let budget = if sequential {
        budgets
            .into_iter()
            .fold(0u32, |total, ms| total.saturating_add(ms))
    } else {
        budgets.into_iter().max().unwrap_or(MAX_RUN_ITEM_TIMEOUT_MS)
    };
    budget.min(MAX_RUN_ITEM_TIMEOUT_MS)
}

/// The agent's own bound on how long this batch may spend looking for room, or
/// `None` when it declared none.
///
/// An omitted `timeout` means "run to completion", and a batch that will run to
/// completion is content to wait for a machine that is merely busy: it is
/// bounded by the batch ceiling alone. A batch that bounded EVERY one of its
/// items said something narrower — this is worth this much time — and honoring
/// the letter of it while queueing for an hour would honor none of its intent.
/// Every item has to declare one, because a single unbounded item makes the
/// batch unbounded.
///
/// Read from the payload rather than from the resolved items, since
/// [`apply_run_item_timeouts`] fills in the clamp and an omitted bound becomes
/// indistinguishable from a declared six hours.
fn declared_capacity_wait_budget(
    payload: &RunPayload,
    sequential: bool,
) -> Option<std::time::Duration> {
    let declared: Option<Vec<u32>> = payload
        .commands
        .iter()
        .map(|item| item.timeout.map(|ms| ms.min(MAX_RUN_ITEM_TIMEOUT_MS)))
        .collect();
    let declared = declared?;
    if declared.is_empty() {
        return None;
    }
    let budget = if sequential {
        declared
            .into_iter()
            .fold(0u32, |total, ms| total.saturating_add(ms))
    } else {
        declared
            .into_iter()
            .max()
            .unwrap_or(MAX_RUN_ITEM_TIMEOUT_MS)
    };
    Some(std::time::Duration::from_millis(u64::from(
        budget.min(MAX_RUN_ITEM_TIMEOUT_MS),
    )))
}

/// The batch ceiling as an instant: past it, nothing about this batch is wanted.
///
/// The authority that already cancels a batch which never ends, so no horizon
/// derived for the batch ever reaches past it.
fn batch_ceiling_horizon_unix_ms() -> u64 {
    crate::fleet::unix_time_ms().saturating_add(RUN_BATCH_CEILING.as_millis() as u64)
}

/// The instant past which nobody is waiting for room for this batch.
///
/// The one number both sides honour: the executor holds this batch's queue entry
/// until here, and the runner waits for it until here. It is the batch's own
/// declared bound when it made one, and the batch ceiling otherwise.
///
/// A declared bound counts from when the batch starts waiting for room, which
/// is after its job's execution home is in hand, never from when the call
/// arrived. Provisioning the home is not waiting for a busy machine: an idle
/// host can take seconds to stand one up, and charging that to a batch whose
/// items declared one second would refuse the batch with no contention
/// anywhere.
///
/// Computed once per batch and never refreshed. A refreshed horizon measures
/// the attempt rather than the batch, which is exactly how a twenty-second queue
/// budget came to stand in for an agent's real patience.
fn room_wait_horizon_unix_ms(
    budget: Option<std::time::Duration>,
    ceiling_horizon_unix_ms: u64,
) -> u64 {
    match budget {
        Some(budget) => crate::fleet::unix_time_ms()
            .saturating_add(budget.as_millis() as u64)
            .min(ceiling_horizon_unix_ms),
        None => ceiling_horizon_unix_ms,
    }
}

/// Settle every process item's kill bound once, before the dispatch seam, so no
/// layer below has to re-derive it.
///
/// Response, decide, and MCP-tool items keep the caller's ask instead: when one
/// times out, its failure has to say what was asked as well as what was
/// enforced, and [`response_item_bound`], [`host_item_bound`], and
/// [`mcp_item_bound`] derive both where the item runs.
fn apply_run_item_timeouts(resolved: &mut [(String, Result<RunSpec, String>)]) {
    for (_, spec) in resolved {
        if let Ok(RunSpec::Shell { timeout, .. } | RunSpec::Script { timeout, .. }) = spec {
            *timeout = Some(clamp_run_item_timeout_ms(*timeout));
        } else if let Ok(RunSpec::ExecutorAction(action)) = spec {
            action.timeout = Some(
                clamp_host_item_timeout_ms(action.timeout).unwrap_or(MAX_HOST_ITEM_TIMEOUT_MS),
            );
        }
    }
}

#[derive(Clone)]
pub(crate) struct ResolvedRunBatch {
    pub request: McpCallbackRequest,
    pub run_context: Option<crate::mcp::handlers::RunContext>,
    pub resolved: Vec<(String, Result<RunSpec, String>)>,
    pub tool_use_id: String,
    pub stop_on_error: bool,
    pub originally_sequential: bool,
    /// The job's execution home, when this batch is an agent job's. The batch
    /// then runs as processes inside that lease rather than in a cell of its
    /// own, which is what puts it in the same place as the job's terminals.
    pub execution_residency: Option<cairn_common::executor_protocol::ResidencyFence>,
}

use crate::fleet::{CellOutcome, CellPriority, CellRequest, MutationPolicy};
use cairn_common::executor_protocol::{ExecutorSelector, RepositoryLocator};

use crate::mcp::handlers::tool_use_correlation::Claim;
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::responses::{Interrupter, Interruption, InvocationInterrupt};
use cairn_vcs::PublicationIdentity;
use uuid::Uuid;

/// Aborts a spawned task if dropped before it is awaited to completion.
///
/// A bare `tokio::spawn` handle detaches on drop, so a cancelled handler future
/// would leave parallel `run` items executing with nobody listening. Wrapping
/// each handle here propagates cancellation: dropping the guard aborts the task,
/// which drops the item's future and its kill-on-drop guard, reaping the tree.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn expand_procedure_target(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &mut RunPayload,
) -> Result<(), String> {
    let Some(raw_target) = payload
        .commands
        .first()
        .and_then(|item| item.target.as_deref())
    else {
        return Ok(());
    };
    let target =
        crate::resources::resolve_home_relative_resource_uri(&orch.db, request, raw_target).await?;
    let Some(parsed) = cairn_common::uri::parse_uri(&target) else {
        return Ok(());
    };
    // What a thread sub-path MEANS is decided in exactly one place, and this is
    // the same call read and write dispatch make. The common parser stops a
    // thread URI at `Thread { path }` and never names `procedures/<procedure>`,
    // so reading the canonical string a second time here saw every stored
    // procedure as a non-procedure and dropped it into the skill-script branch
    // to be refused as "not a skill script target" (CAIRN-4725).
    let resource = crate::resources::delegate_thread_descendant(parsed)?;
    let (project, name, procedure) = match resource {
        cairn_common::uri::CairnResource::ThreadProcedure {
            project,
            name,
            procedure,
        } => (project, name, procedure),
        // A target on the procedure surface that names no single procedure is
        // refused HERE, with its reason. Falling through hands it to the
        // skill-script resolver, whose "expected cairn://skills/<id>/scripts/<name>"
        // is a true sentence about a resource the caller never addressed.
        cairn_common::uri::CairnResource::ThreadProcedures { project, name } => {
            return Err(format!(
                "`{target}` addresses thread `{name}`'s procedure collection, not one procedure. Read it to see what is stored, then invoke a procedure by name: run({{commands:[{{target:\"cairn://p/{project}/{name}/procedures/<procedure>\"}}]}})."
            ))
        }
        cairn_common::uri::CairnResource::Thread {
            project,
            name,
            path,
        } if path.iter().any(|segment| segment == "procedures") => {
            return Err(format!(
                "`{target}` is not an invocable procedure. A procedure is addressed by name directly under its thread: cairn://p/{project}/{name}/procedures/<procedure>."
            ))
        }
        _ => return Ok(()),
    };

    if payload.commands.len() != 1 {
        return Err("A procedure target must be the only item in its run batch because the procedure carries its own batch-level settings.".to_string());
    }
    if payload.sequential.is_some()
        || payload.stop_on_error.is_some()
        || payload.commit_msg.is_some()
        || payload.branch.is_some()
        || payload.executor.is_some()
        || payload.conflict_markers_reason.is_some()
    {
        return Err("A procedure invocation cannot carry batch-level execution settings; those settings belong to the stored procedure batch.".to_string());
    }
    let invocation = &payload.commands[0];
    if invocation.command.is_some()
        || invocation.code.is_some()
        || invocation.interpreter.is_some()
        || invocation.repl.is_some()
        || invocation.wait_for.is_some()
        || invocation.background.is_some()
        || invocation.timeout.is_some()
    {
        return Err("A procedure invocation accepts only `target` and optional `payload.args`; execution settings belong to the stored procedure batch.".to_string());
    }
    let args = match invocation.payload.as_ref() {
        None => Vec::new(),
        Some(item_payload) if item_payload.args_json.is_none() => item_payload.args.clone(),
        Some(_) => return Err(
            "A procedure invocation accepts positional `payload.args`, not `payload.args_json`."
                .to_string(),
        ),
    };

    let (context, db) = super::run_context::lookup_run_routed(&orch.db, request).await?;
    let addressed_thread = crate::threads::thread_id_by_name(&db, &project, &name)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("Thread `{project}/{name}` was not found"))?;
    if context.thread_id.as_deref() != Some(addressed_thread.as_str()) {
        return Err(format!(
            "Procedure `{target}` belongs to another thread. Procedure bodies execute with the invoking thread's identity; read and copy it into this thread instead."
        ));
    }

    let stored = crate::threads::procedures::get(&addressed_thread, &procedure)
        .await?
        .ok_or_else(|| format!("Procedure `{procedure}` was not found in thread `{name}`"))?;
    crate::threads::procedures::validate_body(&stored.body)?;
    let mut expanded: RunPayload = serde_json::from_value(stored.body).map_err(|error| {
        format!("Stored procedure `{procedure}` is not a valid run batch: {error}")
    })?;
    for item in &mut expanded.commands {
        item.procedure_args = args.clone();
    }
    *payload = expanded;
    Ok(())
}

/// Handle run tool call - an ordered batch of synchronous shell commands and
/// skill-script invocations. Parallel by default; `sequential` runs in order.
pub async fn handle_run(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    // Asked of the raw call, before the lenient parse below drops what the
    // grammar does not define: a dropped `executor` or `commitMsg` is a batch
    // placed or committed other than its caller asked.
    if let Some(refusal) = cairn_common::run_grammar::run_grammar_refusal(&request.payload) {
        return run_input_refusal(refusal);
    }
    let mut payload: RunPayload = match super::parse_payload(request) {
        Ok(payload) => payload,
        Err(error) => return run_input_refusal(error),
    };

    if payload.commands.is_empty() {
        return run_input_refusal(
            "Invalid payload: `commands` must contain at least one item".to_string(),
        );
    }

    if let Err(error) = expand_procedure_target(orch, request, &mut payload).await {
        return run_envelope(error, Vec::new());
    }

    let commit_present = payload.commit_msg.is_some();

    // A thread writes no tracked files, and a thread runs ON the base branch: a
    // seal from this batch publishes to the project's default branch with no PR
    // behind it. Asked FIRST, ahead of every branch below that can act — the
    // waitFor arm's host control flow and, critically, the workflow arm, which
    // starts a workflow node and durably suspends the caller. A refusal placed
    // after item-specific dispatch is not fail-closed, however early it looks in
    // the executing path. Only a batch that actually carries a `commit_msg` pays
    // the lookup, and the gate itself is the one the `write` verb takes, so the
    // two commit-capable verbs answer to a single predicate.
    if commit_present {
        if let Some(refusal) = super::run_context::commit_posture_refusal(&orch.db, request).await {
            return run_envelope(refusal, Vec::new());
        }
    }

    if let Some(item) = payload.commands.iter().find(|item| item.wait_for.is_some()) {
        if payload.commands.len() != 1 {
            return run_envelope(
                "A waitFor item must be the only item in its run batch (it suspends the caller)."
                    .to_string(),
                Vec::new(),
            );
        }
        if payload.branch.is_some() || payload.commit_msg.is_some() {
            return run_envelope("A waitFor run cannot use branch or commit_msg; it is host control flow and does not execute in a worktree.".to_string(), Vec::new());
        }
        if payload.sequential.is_some() || payload.stop_on_error.is_some() {
            return run_envelope("A waitFor run cannot use sequential or stop_on_error; the batch contains exactly one control-flow item.".to_string(), Vec::new());
        }
        if item.command.is_some()
            || item.target.is_some()
            || item.code.is_some()
            || item.repl.is_some()
            || item.payload.is_some()
            || item.interpreter.is_some()
            || item.timeout.is_some()
            || item.background.is_some()
        {
            return run_envelope("A waitFor item cannot include command, target, code, repl, payload, interpreter, timeout, or background.".to_string(), Vec::new());
        }
        return run_envelope(
            crate::mcp::handlers::owned_wait::handle_owned_wait(
                orch,
                request,
                item.wait_for.as_ref().expect("checked waitFor"),
            )
            .await,
            Vec::new(),
        );
    }

    // A run item targeting a workflow URI is a DELEGATION, not a subprocess: it
    // starts a workflow node under the caller and durably suspends the caller
    // (reusing the call-packet suspend/resume tail), off the 600s run-item path.
    // It must be the sole item in its batch, since it suspends the whole call.
    if let Some((project, workflow_id)) =
        crate::mcp::handlers::workflows::detect_workflow_target(&payload.commands)
    {
        if payload.commands.len() != 1 {
            return run_envelope(
                "A workflow run target must be the only item in its batch (it suspends the caller)."
                    .to_string(),
                Vec::new(),
            );
        }
        let result = crate::mcp::handlers::workflows::invoke_workflow(
            orch,
            request,
            project,
            workflow_id,
            &payload.commands[0],
        )
        .await;
        return run_envelope(result, Vec::new());
    }

    let cwd = request.cwd.clone();
    let tool_use_id = request
        .tool_use_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    // Advisory nudge: if any shell item wraps an interpreter one-liner
    // (`python3 -c`, `bun -e`, a `python <<EOF` heredoc, …), surface a one-line
    // tip pointing at inline `{code, interpreter}`. Computed once here and
    // appended to the composed output below; never affects success/exit status.
    let interpreter_tip = tip::interpreter_tip(&payload.commands);

    // Resolve authenticated agent context once for the whole batch. A supplied
    // run ID is authoritative and must resolve; it never degrades into an ambient
    // cwd-scoped operation. Requests without a run ID are separately typed
    // ambient/user operations.
    let (run_context, run_db) = if request.run_id.is_some() {
        match super::run_context::lookup_run_routed(&orch.db, request).await {
            Ok((context, owning_db)) => (Some(context), Some(owning_db)),
            Err(error) => return run_envelope(error, Vec::new()),
        }
    } else {
        (None, None)
    };
    let transcript_event_uri = match (run_context.as_ref(), run_db.as_deref()) {
        (Some(context), Some(db)) => {
            run_transcript_event_uri(
                orch,
                db,
                context,
                request.tool_use_id.as_deref(),
                &request.payload,
            )
            .await
        }
        _ => None,
    };

    let branch_target = if let Some(branch) = payload.branch.as_deref() {
        if commit_present {
            return run_envelope(
                "A branch-scoped run is verdict-only and cannot commit. Remove commit_msg and retry."
                    .to_string(),
                Vec::new(),
            );
        }
        match crate::mcp::handlers::branch::resolve_for_run(orch, request, branch).await {
            Ok(resolution) => Some(resolution),
            Err(error) => return run_envelope(error.to_string(), Vec::new()),
        }
    } else {
        None
    };
    // Builders verify constants through `run branch:main` as often as through
    // `read ?branch=main`, so the result names the tree it ran against with the
    // same label a read header carries.
    let revision_label = match branch_target.as_ref() {
        Some(target) => Some(
            crate::orchestrator::base_branch_sync::revision_label(
                orch,
                &target.project_id,
                &target.rev,
                &target.default_branch,
                &target.revision_store,
                &target.commit_id,
            )
            .await,
        ),
        None => None,
    };

    // Resolve every item before any placement or managed-workspace preparation.
    // This is the dispatch seam where process-shaped searches can be served by
    // the in-process grep engine without consuming a build-slot admission.
    let mut resolved: Vec<(String, Result<RunSpec, String>)> =
        Vec::with_capacity(payload.commands.len());
    for item in &payload.commands {
        resolved.push(resolve_run_item(orch, request, run_context.as_ref(), item).await);
    }
    let sequential = payload.sequential.unwrap_or(false);
    // Settled once, before `payload` is consumed by either settlement site. An
    // all-whitespace reason is no reason at all: the escape is audited, so it
    // must actually say something.
    let marker_escape = payload
        .conflict_markers_reason
        .as_deref()
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
        .map(ToOwned::to_owned);
    let stop_on_error = payload.stop_on_error.unwrap_or(true);

    // Authenticated jobs resolve repository identity and the immutable batch base
    // solely from their durable run coordinate. Process cwd is scratch residence.
    let logical_resolution = if branch_target.is_none() && run_context.is_some() {
        match super::branch::resolve_current_for_read(orch, request).await {
            Ok(resolution) => Some(resolution),
            Err(error) => return run_envelope(error, Vec::new()),
        }
    } else {
        None
    };

    // Item timeouts settle before the dispatch seam so a served search and a
    // real execution work to the same budget. `buildSlots.defaultTimeoutSeconds`
    // deliberately does not appear here: it bounds slot lifetime and the
    // executor watchdog, never an agent's command.
    apply_run_item_timeouts(&mut resolved);

    // The dispatch seam. A search-shaped item is a read in run's clothing, so it
    // is served here: after the head coordinate is known, so it reads the same
    // content `read ?grep=` reads, and before any lease or slot admission
    // exists, so it never enters the scheduler. A branch-scoped run is a verdict
    // about another coordinate and executes for real.
    if branch_target.is_none() {
        if let Some(outcomes) = search::try_run_search_batch(
            orch,
            request,
            logical_resolution.as_ref(),
            &cwd,
            &resolved,
            sequential,
            stop_on_error,
        )
        .await
        {
            let text = compose_run_output(&outcomes);
            return run_envelope(text, collect_run_images(outcomes));
        }
    }
    let store_lock = logical_resolution
        .as_ref()
        .map(|resolution| resolution.repository_path.clone());

    // Ambient user operations retain a physical VCS observer only for the
    // explicitly supplied checkout. Authenticated jobs never select one by cwd.
    let vcs = if run_context.is_none() && branch_target.is_none() {
        Some(crate::mcp::vcs::resolve_worktree_vcs(
            orch,
            std::path::Path::new(&cwd),
        ))
    } else {
        None
    };
    let status_before = if payload.commit_msg.is_none() {
        vcs.as_ref()
            .and_then(|vcs| vcs.snapshot(std::path::Path::new(&cwd)).ok())
    } else {
        None
    };

    // The process residence is never compared with a project checkout.
    let repo_root: Option<String> = None;

    // Ambient operations may receive advisory notes for redundant checkout changes.
    // Authenticated jobs have no repository cwd, so no project path is supplied.
    let cd_advisory = check_cd_commands(
        resolved.iter().map(|(header, _)| header.as_str()),
        &cwd,
        repo_root.as_deref(),
    );

    if let Some((header, _)) = resolved.first() {
        let redacted = redact_command(header);
        log::info!(
            "run batch ({} item(s), sequential={}): {} (cwd={})",
            resolved.len(),
            payload.sequential.unwrap_or(false),
            &redacted[..redacted.len().min(100)],
            cwd
        );
    }

    // Executor containment and the logical fence adjudicate filesystem crossings.
    // Placement is a preflight batch invariant. A call may contain exactly one
    // execution class: tree-bound processes, host MCP gateway calls, or persistent
    // REPL sends. Splitting a mixed call here would violate batch ordering and the
    // single commit barrier, so reject before any item starts.
    let has_process = resolved
        .iter()
        .any(|(_, spec)| matches!(spec, Ok(RunSpec::Shell { .. } | RunSpec::Script { .. })));
    let has_host_rpc = resolved
        .iter()
        .any(|(_, spec)| matches!(spec, Ok(RunSpec::McpCall(_) | RunSpec::ExecutorAction(_))));
    let has_response = resolved.iter().any(|(_, spec)| {
        matches!(
            spec,
            Ok(RunSpec::ResponseCall { .. } | RunSpec::Decide { .. })
        )
    });
    let has_repl = resolved
        .iter()
        .any(|(_, spec)| matches!(spec, Ok(RunSpec::ReplSend { .. })));
    if usize::from(has_process) + usize::from(has_host_rpc || has_response) + usize::from(has_repl)
        > 1
    {
        return run_envelope(
            "A run batch may not mix tree-bound shell/script items with host RPC or REPL items. Split them into separate run calls.".to_string(),
            Vec::new(),
        );
    }
    if branch_target.is_some() && !has_process {
        return run_envelope(
            "The branch option applies only to tree-bound shell or script batches; gateway, executor-action, and REPL batches run on the host.".to_string(),
            Vec::new(),
        );
    }
    if payload
        .executor
        .as_ref()
        .is_some_and(|selector| !selector.is_empty())
        && !has_process
    {
        return run_envelope(
            "The executor option applies only to tree-bound shell or script batches; gateway, executor-action, and REPL batches run on the host. An executor-action target already names its machine; split the host-bound item into a run call without executor.".to_string(),
            Vec::new(),
        );
    }

    let batch_residence = match BatchResidence::for_job_batch(
        orch,
        run_db.as_deref(),
        logical_resolution.as_ref(),
        run_context.as_ref(),
        payload.executor.as_ref(),
        commit_present,
    )
    .await
    {
        Ok(residence) => residence,
        Err(refusal) => {
            return run_envelope(
                RunFailure::NotExecuted(format!("{refusal}.")).text(),
                Vec::new(),
            )
        }
    };
    let slot_target = if let Some(target) = branch_target.as_ref() {
        log::info!(
            "resolved branch run rev {} to commit {} in project {}",
            target.rev,
            target.commit_id,
            target.project_id
        );
        Some((
            RepositoryLocator::ColocatedPath {
                project_id: target.project_id.clone(),
                repository_id: target.project_id.clone(),
                absolute_path: target.repository_path.to_string_lossy().into_owned(),
            },
            target.commit_id.clone(),
            String::new(),
            MutationPolicy::PureVerdict,
        ))
    } else if let Some(resolution) = logical_resolution.as_ref() {
        if has_process {
            Some((
                RepositoryLocator::ColocatedPath {
                    project_id: resolution.project_id.clone(),
                    repository_id: resolution.project_id.clone(),
                    absolute_path: resolution
                        .object_repository_path
                        .to_string_lossy()
                        .into_owned(),
                },
                resolution.commit_id.clone(),
                String::new(),
                if batch_residence == BatchResidence::Detached && !commit_present {
                    MutationPolicy::PureVerdict
                } else {
                    MutationPolicy::AllowDelta
                },
            ))
        } else {
            None
        }
    } else if has_process {
        run_context.as_ref().and_then(|ctx| {
            let root = std::process::Command::new("git")
                .args(["rev-parse", "--show-toplevel"])
                .current_dir(&cwd)
                .output()
                .ok()?;
            let head = std::process::Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(&cwd)
                .output()
                .ok()?;
            if !root.status.success() || !head.status.success() {
                return None;
            }
            let root = String::from_utf8_lossy(&root.stdout).trim().to_string();
            let relative_cwd = std::path::Path::new(&cwd)
                .strip_prefix(&root)
                .ok()
                .map(|path| path.to_string_lossy().into_owned())
                .unwrap_or_default();
            Some((
                RepositoryLocator::ExistingCheckout {
                    project_id: ctx.project_id.clone(),
                    repository_id: ctx.project_id.clone(),
                    absolute_path: root,
                },
                String::from_utf8_lossy(&head.stdout).trim().to_string(),
                relative_cwd,
                MutationPolicy::PureVerdict,
            ))
        })
    } else {
        None
    };
    if has_process && slot_target.is_none() {
        return run_envelope(
            RunFailure::NotExecuted(
                "this batch's working directory could not be resolved.".to_string(),
            )
            .text(),
            Vec::new(),
        );
    }
    if has_process {
        if let Some((repository, base_commit, relative_cwd, mutation_policy)) = slot_target {
            let project_id = repository.project_id().to_string();
            let (command, command_class) = build_slot_command_parts(&resolved);
            let ceiling_horizon_unix_ms = batch_ceiling_horizon_unix_ms();
            let template = CellRequest {
                request_id: Uuid::new_v4().to_string(),
                // Minted per attempt. A batch that waits for room is presented
                // more than once, and those are attempts of one request: the
                // request id is what a cancellation and a suspension both name,
                // so it is the only identity fixed here.
                attempt_id: String::new(),
                project_id: project_id.clone(),
                repository,
                base_commit,
                command,
                command_class,
                placement_work_class:
                    cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
                owner: run_context.as_ref().map(|ctx| {
                    cairn_common::executor_protocol::CellOwnerRef {
                        project_id: ctx.project_id.clone(),
                        project_key: Some(ctx.project_key.clone()),
                        issue_number: ctx.issue_number,
                        job_id: Some(ctx.job_id.clone()),
                        execution_seq: ctx.exec_seq,
                        node_kind: ctx.job_name.clone(),
                    }
                }),
                cwd: relative_cwd,
                env: placed_process_env(
                    run_context.as_ref().map(|ctx| ctx.run_id.as_str()),
                    match (run_context.as_ref(), run_db.as_deref()) {
                        (Some(ctx), Some(db)) => {
                            crate::jobs::queries::home_uri_for_job(db, &ctx.job_id)
                                .await
                                .unwrap_or_else(|error| {
                                    log::warn!(
                                        "Could not resolve home URI for job {}: {error}",
                                        ctx.job_id
                                    );
                                    None
                                })
                        }
                        _ => None,
                    }
                    .as_deref(),
                    branch_target.as_ref().map(|target| target.rev.as_str()),
                    logical_resolution
                        .as_ref()
                        .map(|resolution| resolution.rev.as_str()),
                    payload.executor.as_ref(),
                )
                .into_iter()
                .chain(
                    transcript_event_uri
                        .clone()
                        .map(|uri| ("CAIRN_TRANSCRIPT_EVENT_URI".into(), uri)),
                )
                .collect(),
                priority: CellPriority::AgentInteractive,
                // The ceiling until the batch starts waiting for room, when
                // `BatchPlacement::begin_waiting_for_room` narrows it to the
                // batch's declared bound, once, for every later presentation.
                wait_horizon_unix_ms: ceiling_horizon_unix_ms,
                waiting_since_unix_ms: crate::fleet::unix_time_ms(),
                timeout_ms: batch_execution_budget_ms(&resolved, sequential).into(),
                mutation_policy,
                requesting_job_id: run_context.as_ref().map(|ctx| ctx.job_id.clone()),
                // Placement is settled by the lease when there is one; the
                // affinity key only steers cell selection for unbound batches.
                affinity_key: run_context.as_ref().map(|ctx| ctx.run_id.clone()),
                executor: payload.executor.clone(),
                pinned_executor_id: None,
                // An agent's batch is home-bound unless its own selector sent it
                // elsewhere: its working tree is a leased cell that a residency
                // already placed, and the pin each attempt sets states where. This
                // is the case the mobility fact exists for -- untargeted here
                // means "nothing further to say", not "put it anywhere".
                placement_mobility:
                    cairn_common::executor_protocol::PlacementMobility::PinnedOrColocated,
                // An agent's batch publishes output, not a verdict, so no
                // platform's answer counts more than another's here.
                verdict_platforms: Vec::new(),
                avoided_executor_ids: Vec::new(),
                project_requirements: Default::default(),
                command_resource_identity: None,
                resource_reservation: Default::default(),
                learned_estimate: None,
            };
            let placement = BatchPlacement {
                // An agent job executes everything in one home: this batch, the
                // job's terminals, its REPLs, and any of its runs that outlive
                // their timeout. Acquiring that home is part of placing the
                // batch, not a precondition checked above it, which is what lets
                // a contended acquisition wait like any other placement.
                environment: if batch_residence == BatchResidence::Home {
                    match (logical_resolution.as_ref(), run_context.as_ref(), run_db) {
                        (Some(resolution), Some(ctx), Some(db)) => Some(JobEnvironment {
                            db,
                            job_id: ctx.job_id.clone(),
                            base_commit: resolution.commit_id.clone(),
                        }),
                        _ => None,
                    }
                } else {
                    None
                },
                residence: batch_residence,
                commit_present,
                capacity_wait_budget: declared_capacity_wait_budget(&payload, sequential),
                ceiling_horizon_unix_ms,
                template,
            };
            let batch = ResolvedRunBatch {
                request: request.clone(),
                run_context: run_context.clone(),
                resolved,
                tool_use_id: tool_use_id.clone(),
                stop_on_error,
                originally_sequential: sequential,
                // Filled per attempt with the execution home that attempt got.
                execution_residency: None,
            };
            let settlement = RunBatchSettlement {
                request: request.clone(),
                run_context,
                commit_msg: payload.commit_msg,
                verdict_only: branch_target.is_some(),
                revision_label,
                logical_resolution,
                store_lock,
                // Publication reads repository identity off this, which every
                // attempt shares, so the template answers for all of them.
                routed_request: Some(placement.template.clone()),
                cwd,
                tool_use_id,
                cd_advisory,
                interpreter_tip,
                vcs,
                status_before,
                marker_escape: marker_escape.clone(),
            };
            return off_frame(|| run_routed_batch(orch, settlement, placement, batch)).await;
        }
    }

    // Everything below runs on the host: MCP gateway calls, REPL sends, model
    // calls, and items that failed to resolve. Tree-bound shell and script items
    // never reach here — a process batch that cannot be placed is refused above.
    let batch = HostBatch {
        request: request.clone(),
        cwd: cwd.clone(),
        tool_use_id: tool_use_id.clone(),
        run_context: run_context.clone(),
        resolved,
        sequential,
        stop_on_error,
    };
    let settlement = RunBatchSettlement {
        request: request.clone(),
        run_context,
        commit_msg: payload.commit_msg,
        verdict_only: branch_target.is_some(),
        revision_label,
        logical_resolution,
        store_lock,
        routed_request: None,
        cwd,
        tool_use_id,
        cd_advisory,
        interpreter_tip,
        vcs,
        status_before,
        marker_escape,
    };
    if batch.may_outlive_grace() {
        return off_frame(|| {
            run_suspendable_host_batch(orch, settlement, batch, run_grace_window())
        })
        .await;
    }
    let outcomes = off_frame(|| run_host_items(orch, batch, None)).await;
    let settled = off_frame(|| settle_host_batch(orch, &settlement, outcomes)).await;
    host_batch_envelope(settled)
}

/// A host-executed batch's items and what each needs to run, owned so the
/// batch can run on a task of its own when it may outlive its call.
struct HostBatch {
    request: McpCallbackRequest,
    cwd: String,
    tool_use_id: String,
    run_context: Option<crate::mcp::handlers::RunContext>,
    resolved: Vec<(String, Result<RunSpec, String>)>,
    sequential: bool,
    stop_on_error: bool,
}

impl HostBatch {
    /// Whether the batch holds an item allowed to run past the grace window. A
    /// response item is: it runs as long as its definition declares. So is an
    /// MCP-tool item: it runs to its declared `timeout`. Decide, REPL, and
    /// executor-action items are capped at the window and never need to
    /// suspend.
    fn may_outlive_grace(&self) -> bool {
        self.resolved
            .iter()
            .any(|(_, spec)| matches!(spec, Ok(RunSpec::ResponseCall { .. } | RunSpec::McpCall(_))))
    }
}

/// Run a host batch's items: in order with fail-fast, or each on its own task.
/// `interrupt` reaches every response and MCP-tool item, and once it fires a
/// sequential batch starts nothing further.
async fn run_host_items(
    orch: &Orchestrator,
    batch: HostBatch,
    interrupt: Option<&InvocationInterrupt>,
) -> Vec<ItemOutcome> {
    let HostBatch {
        request,
        cwd,
        tool_use_id,
        run_context,
        resolved,
        sequential,
        stop_on_error,
    } = batch;
    if sequential {
        let mut outcomes: Vec<ItemOutcome> = Vec::with_capacity(resolved.len());
        for (index, (header, spec)) in resolved.into_iter().enumerate() {
            if interrupt.is_some_and(|interrupt| interrupt.fired().is_some()) {
                break;
            }
            let stream_id = run_item_stream_id(&tool_use_id, index);
            let outcome = off_frame(|| {
                run_one(
                    orch,
                    &request,
                    &cwd,
                    &stream_id,
                    run_context.as_ref(),
                    header,
                    spec,
                    interrupt,
                )
            })
            .await;
            // A suspend stops the (sequential) batch: the whole call re-runs on
            // resume once the fence is answered.
            let stop = outcome.suspended || (!outcome.succeeded && stop_on_error);
            outcomes.push(outcome);
            if stop {
                break;
            }
        }
        return outcomes;
    }
    // Parallel: each item runs on its own task so one item's wait never stalls
    // the others. Each handle is wrapped in an abort-on-drop guard so dropping
    // this future (client disconnect / MCP cancel) aborts every in-flight item,
    // which drops each item's kill-on-drop guard and reaps its process group —
    // detached `tokio::spawn` tasks would otherwise outlive the cancelled request.
    let mut handles = Vec::with_capacity(resolved.len());
    for (index, (header, spec)) in resolved.into_iter().enumerate() {
        let orch = orch.clone();
        let cwd = cwd.clone();
        let stream_id = run_item_stream_id(&tool_use_id, index);
        let run_context = run_context.clone();
        let request = request.clone();
        let interrupt = interrupt.cloned();
        handles.push(AbortOnDrop(tokio::spawn(async move {
            run_one(
                &orch,
                &request,
                &cwd,
                &stream_id,
                run_context.as_ref(),
                header,
                spec,
                interrupt.as_ref(),
            )
            .await
        })));
    }
    let mut outcomes = Vec::with_capacity(handles.len());
    for handle in &mut handles {
        match (&mut handle.0).await {
            Ok(outcome) => outcomes.push(outcome),
            Err(e) => outcomes.push(ItemOutcome::failed(
                "<item>".to_string(),
                format!("Failed to join run task: {e}"),
            )),
        }
    }
    outcomes
}

/// The host batch tail: the same settlement every batch takes, from the home.
async fn settle_host_batch(
    orch: &Orchestrator,
    settlement: &RunBatchSettlement,
    outcomes: Vec<ItemOutcome>,
) -> SettledRunBatch {
    settle_run_batch(
        orch,
        settlement,
        BatchResidence::Home,
        outcomes,
        None,
        None,
        None,
        None,
        None,
    )
    .await
}

fn host_batch_envelope(settled: SettledRunBatch) -> String {
    if settled.suspended {
        return run_envelope(RUN_ITEM_SUSPENDED_MARKER.to_string(), Vec::new());
    }
    run_envelope(settled.text, settled.images)
}

fn joined_host_outcomes(
    joined: Result<Vec<ItemOutcome>, tokio::task::JoinError>,
) -> Vec<ItemOutcome> {
    joined.unwrap_or_else(|error| {
        vec![ItemOutcome::failed(
            "<batch>".to_string(),
            format!("Failed to join run task: {error}"),
        )]
    })
}

/// Interrupts a host batch's response and MCP-tool items when the call carrying
/// them is dropped before it hands them to a suspension. The host batch's
/// counterpart of [`AbortOnDrop`]: aborting would end response items without
/// their journal rows.
struct InterruptOnDrop(Option<Interrupter>);

impl InterruptOnDrop {
    fn interrupt(&self, interruption: Interruption) {
        if let Some(interrupter) = &self.0 {
            interrupter.interrupt(interruption);
        }
    }

    /// Hand the interrupter to whoever owns the batch from here on.
    fn disarm(mut self) -> Interrupter {
        self.0.take().expect("an armed interrupter")
    }
}

impl Drop for InterruptOnDrop {
    fn drop(&mut self) {
        if let Some(interrupter) = self.0.take() {
            interrupter.interrupt(Interruption::Cancelled(CALL_ABANDONED_REASON.into()));
        }
    }
}

/// Run a host batch holding a response or MCP-tool item, either of which may
/// legitimately take minutes, racing it against the grace window exactly as a
/// routed batch is raced ([`run_routed_batch`]).
///
/// Past grace the call parks on the same durable suspension a routed batch
/// takes, marked as holding no tree, and the batch keeps running on its own
/// task until it settles, is stopped, or reaches the batch ceiling.
///
/// A call with no `run` tool call of its own to park on (a `cairn.run` from
/// @cairn/sdk inside a shell or script item) cannot suspend, and nothing above
/// the host would still be listening past grace: the callback contract sizes
/// every wrapper just above it. So its response and MCP-tool items are ended at
/// grace with a bound failure that says why (a response journals it like any
/// other outcome, an MCP call is cancelled at its server) rather than left to
/// finish for a caller that has already gone.
async fn run_suspendable_host_batch(
    orch: &Orchestrator,
    settlement: RunBatchSettlement,
    batch: HostBatch,
    grace: std::time::Duration,
) -> String {
    let request_id = Uuid::new_v4().to_string();
    let (interrupter, interrupt) = crate::responses::interruption();
    let interrupter = InterruptOnDrop(Some(interrupter));
    let task_orch = orch.clone();
    let mut handle =
        tokio::spawn(async move { run_host_items(&task_orch, batch, Some(&interrupt)).await });
    let joined = match tokio::time::timeout(grace, &mut handle).await {
        Ok(joined) => joined,
        Err(_) => match begin_batch_suspension(orch, &settlement, &request_id, true).await {
            Some((record, db, handoff)) => {
                let interrupter = interrupter.disarm();
                let owned_orch = orch.clone();
                tokio::spawn(async move {
                    settle_suspended_host_batch(
                        &owned_orch,
                        settlement,
                        handle,
                        interrupter,
                        SuspendedCall {
                            record,
                            db,
                            handoff,
                        },
                        grace,
                    )
                    .await;
                });
                return run_envelope(RUN_BATCH_SUSPENDED_MARKER.to_string(), Vec::new());
            }
            None => {
                interrupter.interrupt(Interruption::Capped {
                    limit: grace,
                    name: "host-item cap",
                    why: UNPARKED_HOST_BATCH_WHY,
                });
                handle.await
            }
        },
    };
    let settled =
        off_frame(|| settle_host_batch(orch, &settlement, joined_host_outcomes(joined))).await;
    host_batch_envelope(settled)
}

/// The suspension a call parked on: its row, the database holding it, and the
/// handoff that says when its turn has actually parked.
struct SuspendedCall {
    record: crate::mcp::handlers::durable_suspend::Record,
    db: std::sync::Arc<crate::storage::LocalDb>,
    handoff: crate::mcp::handlers::durable_suspend::ParkHandoff,
}

/// Await a parked host batch off-call and answer its call with the result.
///
/// The batch ceiling is enforced here as it is for a routed batch. A stop is
/// enforced here too: it cancels the suspension row, which this task watches,
/// and the items still running are interrupted. A response item journals the
/// stop and releases its provider request to its own timeout; an MCP-tool item
/// reports itself stopped and its request is cancelled at the server. The stop
/// itself already answered the call and ended the turn, so nothing resolves.
async fn settle_suspended_host_batch(
    orch: &Orchestrator,
    settlement: RunBatchSettlement,
    mut handle: tokio::task::JoinHandle<Vec<ItemOutcome>>,
    interrupter: Interrupter,
    call: SuspendedCall,
    grace: std::time::Duration,
) {
    use crate::mcp::handlers::durable_suspend;

    let SuspendedCall {
        record,
        db,
        handoff,
    } = call;
    let ceiling = tokio::time::Instant::now() + RUN_BATCH_CEILING.saturating_sub(grace);
    let joined = loop {
        tokio::select! {
            joined = &mut handle => break joined,
            () = tokio::time::sleep_until(ceiling) => {
                interrupter.interrupt(Interruption::Capped {
                    limit: RUN_BATCH_CEILING,
                    name: "run batch ceiling",
                    why: RUN_ITEM_CEILING_WHY,
                });
                break (&mut handle).await;
            }
            () = tokio::time::sleep(HOST_BATCH_STOP_POLL) => {
                if durable_suspend::was_cancelled(&db, &record.id).await {
                    interrupter.interrupt(Interruption::Cancelled(RUN_STOPPED_REASON.into()));
                    let _ = (&mut handle).await;
                    return;
                }
            }
        }
    };
    let settled =
        off_frame(|| settle_host_batch(orch, &settlement, joined_host_outcomes(joined))).await;
    // Nothing resumes the run until the predecessor turn is actually parked.
    if !handoff.parked().await {
        log::warn!(
            "host run batch suspension {} was never parked; leaving it for startup reconciliation",
            record.id
        );
        return;
    }
    // An item's own suspension (an MCP continuation) owns the call now.
    if settled.suspended {
        return;
    }
    durable_suspend::await_inner_wait_release(&db, &record).await;
    // A resumed call carries text only, so an MCP item's images in a batch that
    // also held a long response item cannot ride along; say so rather than
    // drop them silently.
    let mut text = settled.text;
    if !settled.images.is_empty() {
        text.push_str(&format!(
            "\n\n{} image block(s) from this batch could not be carried across its suspension; run the MCP item that produced them in a batch of its own to see them.",
            settled.images.len()
        ));
    }
    if let Err(error) = durable_suspend::resolve(orch, &db, &record, &text, false).await {
        log::warn!("host run batch suspension resolution failed: {error}");
    }
}

/// A `run` call returns synchronously when its batch settles inside this window.
/// Past it the call suspends durably and the agent resumes with the same
/// composed result — one call in, one final result out.
///
/// Grace governs the SHAPE of the call; an item's `timeout` governs the fate of
/// that item. They are orthogonal: a ten-minute `timeout` still kills at ten
/// minutes, it just does not hold a socket open for ten minutes.
const RUN_GRACE_WINDOW: std::time::Duration =
    std::time::Duration::from_millis(cairn_common::run_contract::RUN_GRACE_WINDOW_MS);

/// Absolute ceiling for one `run` batch. Far above any legitimate build or test
/// suite, and well below the transport ceilings above it, so a command that
/// never exits fails loudly instead of parking its agent forever.
const RUN_BATCH_CEILING: std::time::Duration =
    std::time::Duration::from_millis(cairn_common::run_contract::RUN_BATCH_CEILING_MS as u64);

/// Resolve the grace window, honoring the `CAIRN_RUN_GRACE_MS` dev/test override
/// in milliseconds (parsed, non-empty) and falling back to the constant —
/// mirroring the `CAIRN_SESSION_STALENESS_SECS` convention. Suspension is
/// otherwise observable only by waiting two real minutes, which no test can
/// afford. Shortening grace is safe to do process-wide: a batch that cannot be
/// parked simply keeps awaiting inline and returns the same envelope.
///
/// [`RUN_BATCH_CEILING`] deliberately has no such override. Shortening it would
/// CANCEL every concurrent batch in the process, which is a very different blast
/// radius; its composed failure is covered by unit tests instead.
fn run_grace_window() -> std::time::Duration {
    std::env::var("CAIRN_RUN_GRACE_MS")
        .ok()
        .and_then(|raw| {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                trimmed.parse::<u64>().ok()
            }
        })
        .map(std::time::Duration::from_millis)
        .unwrap_or(RUN_GRACE_WINDOW)
}

/// What a batch that hit [`RUN_BATCH_CEILING`] tells the agent. It names the
/// likely mistake, because the only command that runs this long is one that was
/// never going to exit.
const RUN_CEILING_DETAIL: &str = "it exceeded the 6-hour ceiling for a single `run` batch and was killed. A process meant to keep running belongs in a terminal: `write cairn:~/terminal/<slug>`.";

/// What the agent reads for a batch that outlived the grace window.
///
/// This is the whole agent-visible surface of a suspended batch, so it says what
/// actually happened -- the call is continuing -- and never implies anyone
/// declined it. `crate::mcp::handlers::suspension_markers` pins that property.
pub(crate) const RUN_BATCH_SUSPENDED_MARKER: &str =
    "Run handed off to durable suspend; still running. The same call resumes with this batch's completed result.";

/// What the agent reads for a batch one of whose ITEMS suspended durably -- an
/// external MCP call awaiting its continuation, or a worktree-fence approval.
/// The whole batch re-drives on resume, so this answers for the whole call.
///
/// Like [`RUN_BATCH_SUSPENDED_MARKER`] it names a call that is continuing and
/// never implies anyone declined it (`super::suspension_markers` pins that), and
/// the transcript reads it as "still in flight" rather than as a verdict
/// (`src/components/chat/suspensionHandoff.ts`).
pub(crate) const RUN_ITEM_SUSPENDED_MARKER: &str =
    "Run suspended pending an item's own suspension; resume will continue once it is answered.";

/// The typed failure a suspended batch resolves to when the host restarts under
/// it. The join handle and the executor result channel were in memory, so the
/// result is genuinely unrecoverable; saying so unblocks the agent, where
/// leaving the row pending would strand it.
pub(crate) fn run_batch_lost_to_restart_text(commits: bool) -> String {
    let commit_clause = if commits {
        " Its `commit_msg` never landed — nothing was committed."
    } else {
        " Nothing was committed."
    };
    RunFailure::NotPublished(format!(
        "this host restarted while the batch was still running, so its result was lost.{commit_clause} Re-run the batch if you still need it."
    ))
    .text()
}

/// Where a job's process batch obtains its checkout.
///
/// `Home` runs inside the job's execution environment, pinned to the machine
/// holding it. `Detached` runs on whatever machine the batch's own selector
/// names, from the transferable object repository at the branch head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BatchResidence {
    Home,
    Detached,
}

/// What a job's execution environment looks like to a batch that named a
/// machine of its own.
struct HomeFacts {
    /// The public name of the machine the environment is on.
    executor_name: String,
    /// Whether that machine satisfies the batch's selector. `None` when it is away
    /// and the selector asks for something only its live advertisement can say.
    satisfies_selector: Option<bool>,
    /// Live processes resident in the environment's working tree, named the way
    /// an agent addresses them (`terminal \`dev\``). Always empty for a guessed
    /// home: with no recorded environment there is no tree for a process to hold.
    resident_processes: Vec<String>,
    /// Set when placement found this machine can never hold the batch's
    /// measured memory while another machine can. Means exactly what a
    /// selector the home fails means: the batch leaves, unless it would strand
    /// a live process.
    cannot_hold: Option<CannotHold>,
}

/// A home that can never hold one batch, as placement reported it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct CannotHold {
    ceiling: cairn_common::executor_protocol::MemoryCeiling,
    /// The machine placement would choose with the pin lifted, which can.
    alternative_executor_id: String,
    alternative_executor_name: String,
}

impl CannotHold {
    fn from_outcome(outcome: &CellOutcome) -> Option<Self> {
        match outcome {
            CellOutcome::Unavailable {
                reason:
                    cairn_common::executor_protocol::CellUnavailableReason::HomeCannotHold {
                        ceiling,
                        alternative_executor_id,
                        alternative_executor_name,
                        ..
                    },
                ..
            } => Some(Self {
                ceiling: *ceiling,
                alternative_executor_id: alternative_executor_id.clone(),
                alternative_executor_name: alternative_executor_name.clone(),
            }),
            _ => None,
        }
    }

    /// Why a batch left its home for this reason. The machine it ran on is the
    /// result's first line; this says why it is not the home.
    fn ran_elsewhere_note(&self, home: &str) -> String {
        use cairn_common::executor_protocol::describe_gib;
        format!(
            "It ran on **{}** because this batch's measured memory (~{}) exceeds what {home} can ever hold ({}). Your execution environment there is unchanged.",
            self.alternative_executor_name,
            describe_gib(self.ceiling.estimate_bytes),
            describe_gib(self.ceiling.ceiling_bytes),
        )
    }
}

impl BatchResidence {
    /// The one rule for where a selector-bearing batch runs. It is applied twice
    /// with the same inputs' shape: once before the environment is acquired, from
    /// where it is believed to be, and again once acquisition has said where it
    /// actually is. The second answer is the one that executes, so a home that
    /// moved, reconnected, or was placed somewhere other than predicted can
    /// never pin a batch to a machine its selector refused.
    ///
    /// The only thing that binds a batch to a machine against its own selector is
    /// a live process sharing write authority over the tree, and only a
    /// committing batch shares that authority: a commit made from another
    /// checkout would leave the live process working on a stale tree. A batch
    /// without `commit_msg` discards its edits wherever it runs, so it is always
    /// free to go where it asked.
    ///
    /// `home` is `None` when no machine is known to hold the environment, which
    /// means there is nothing to share and nothing to protect.
    ///
    /// Uncertainty is never a mismatch. When the home's machine is away and the
    /// selector asks for something only it can confirm, a batch with nothing to
    /// protect still goes where it asked, but a committing batch beside a live
    /// process stays home: acquisition then establishes where the tree actually
    /// is, and the rule is applied again there rather than refusing on a guess.
    ///
    /// A home that placement found can never hold the batch's measured memory
    /// is refused the batch by the same rule: the batch leaves unless leaving
    /// would strand a live process, and then the refusal names the process.
    fn decide(
        selector: Option<&ExecutorSelector>,
        home: Option<&HomeFacts>,
        commit_present: bool,
    ) -> Result<Self, String> {
        let selector = selector.filter(|value| !value.is_empty());
        let Some(home) = home else {
            return Ok(match selector {
                Some(_) => Self::Detached,
                None => Self::Home,
            });
        };
        let protects_a_live_process = commit_present && !home.resident_processes.is_empty();
        let selector_refused = selector.filter(|_| home.satisfies_selector == Some(false));
        let uncertain = selector.is_some() && home.satisfies_selector.is_none();
        if let Some(selector) = selector_refused {
            return match protects_a_live_process {
                true => Err(split_tree_refusal(selector, home)),
                false => Ok(Self::Detached),
            };
        }
        if let Some(cannot_hold) = &home.cannot_hold {
            return match protects_a_live_process {
                true => Err(cannot_hold_refusal(cannot_hold, home)),
                false => Ok(Self::Detached),
            };
        }
        Ok(match uncertain && !protects_a_live_process {
            true => Self::Detached,
            false => Self::Home,
        })
    }

    /// The decision before the environment is acquired, from the machine its
    /// recorded route names, even while that machine is away. Reading a
    /// disconnected home as the colocated machine would make a selector for the
    /// colocated machine look like it matched, and wait on a remote it never
    /// asked for.
    async fn for_job_batch(
        orch: &Orchestrator,
        db: Option<&crate::storage::LocalDb>,
        logical_resolution: Option<&super::branch::BranchResolution>,
        run_context: Option<&crate::mcp::handlers::RunContext>,
        selector: Option<&ExecutorSelector>,
        commit_present: bool,
    ) -> Result<Self, String> {
        let (Some(_), Some(context), Some(selector)) = (
            logical_resolution,
            run_context,
            selector.filter(|value| !value.is_empty()),
        ) else {
            return Ok(Self::Home);
        };
        let holder = cairn_common::executor_protocol::ResidencyHolder::Job {
            job_id: context.job_id.clone(),
        };
        let predicted = orch.fleet.predicted_job_home(&holder);
        let home = home_facts(
            orch,
            db,
            &context.job_id,
            predicted
                .as_ref()
                .map(|home| (home.executor_id.as_str(), home.recorded)),
            Some(selector),
            commit_present,
            None,
        )
        .await;
        Self::decide(Some(selector), home.as_ref(), commit_present)
    }
}

/// The line that opens every result a machine produced: the machine that ran
/// the batch, its platform, and the job's environment machine when the batch
/// ran away from it. `None` when no machine can be named truthfully, which is
/// only an outcome nothing claimed.
///
/// Taken from the claim (the machine that actually took the batch), and from
/// the outcome's own metadata only when no claim was recorded. Never from the
/// selector: a selector says where the batch was asked to go, not where it ran.
fn ran_on_line(
    orch: &Orchestrator,
    claimed: Option<&crate::fleet::CellPlacement>,
    outcome: &CellOutcome,
    detached_from: Option<&str>,
) -> Option<String> {
    let (name, os) = match claimed {
        Some(placement) => (
            match placement.name.is_empty() {
                true => orch.fleet.executor_name_for_text(&placement.executor_id),
                false => placement.name.clone(),
            },
            placement
                .platform
                .as_ref()
                .map(|platform| platform.os.clone()),
        ),
        None => {
            let CellOutcome::Completed { metadata, .. } = outcome else {
                return None;
            };
            if metadata.executor_id.is_empty() {
                return None;
            }
            (
                orch.fleet.executor_name_for_text(&metadata.executor_id),
                orch.fleet
                    .executor_platform(&metadata.executor_id)
                    .map(|platform| platform.os),
            )
        }
    };
    Some(ran_on_text(&name, os.as_deref(), detached_from))
}

fn ran_on_text(name: &str, os: Option<&str>, detached_from: Option<&str>) -> String {
    let platform = os.map(|os| format!(" ({os})")).unwrap_or_default();
    match detached_from.filter(|home| *home != name) {
        Some(home) => format!(
            "Ran on **{name}**{platform}, detached from the job's environment on **{home}**."
        ),
        None => format!("Ran on **{name}**{platform}."),
    }
}

/// The facts [`BatchResidence::decide`] reads about the machine holding a job's
/// environment, given as `(executor id, recorded)`: whether a residency route
/// names it, or it is only the machine guessed for an owner with no environment.
/// Resident processes are looked up only when they can change the answer, a
/// committing batch that machine may refuse, and only for a recorded home.
/// With no selector there is nothing for the home to fail.
async fn home_facts(
    orch: &Orchestrator,
    db: Option<&crate::storage::LocalDb>,
    job_id: &str,
    home: Option<(&str, bool)>,
    selector: Option<&ExecutorSelector>,
    commit_present: bool,
    cannot_hold: Option<CannotHold>,
) -> Option<HomeFacts> {
    let (executor_id, recorded) = home?;
    let satisfies_selector = match selector {
        Some(selector) => orch
            .fleet
            .recorded_executor_satisfies_selector(executor_id, selector),
        None => Some(true),
    };
    let may_refuse = satisfies_selector != Some(true) || cannot_hold.is_some();
    let resident_processes = match db {
        Some(db) if recorded && commit_present && may_refuse => {
            resident_processes(orch, db, job_id).await
        }
        _ => Vec::new(),
    };
    Some(HomeFacts {
        executor_name: orch.fleet.executor_name_for_text(executor_id),
        satisfies_selector,
        resident_processes,
        cannot_hold,
    })
}

/// The live processes inside a job's execution environment: its running
/// terminals and live REPLs. Only an owner on a managed branch keeps them there;
/// a branchless owner's terminals and REPLs live in the project's own checkout
/// under a separate holder and share nothing with the job's run batches.
async fn resident_processes(
    orch: &Orchestrator,
    db: &crate::storage::LocalDb,
    job_id: &str,
) -> Vec<String> {
    crate::fleet::residency::job_live_processes(orch, db, job_id, true).await
}

/// Why a committing batch cannot go where it asked: the live process it would
/// strand, and the two ways out.
fn split_tree_refusal(selector: &ExecutorSelector, home: &HomeFacts) -> String {
    let destination = selector
        .name
        .clone()
        .unwrap_or_else(|| format!("an executor matching {}", selector.describe()));
    let processes = match home.resident_processes.as_slice() {
        [only] => format!("{only} is"),
        many => format!("{} are", many.join(", ")),
    };
    format!(
        "this batch commits, and {processes} running in this job's working tree on {machine}. A commit made on {destination} would leave that live process on a stale tree. Drop `commit_msg` to run it on {destination} as a verdict-only batch, or drop `executor` to run and commit it on {machine}",
        machine = home.executor_name,
    )
}

/// Why a committing batch its home can never hold cannot run elsewhere either:
/// the live process it would strand, the numbers, and the two ways out.
fn cannot_hold_refusal(cannot_hold: &CannotHold, home: &HomeFacts) -> String {
    use cairn_common::executor_protocol::describe_gib;
    let processes = match home.resident_processes.as_slice() {
        [only] => format!("{only} is"),
        many => format!("{} are", many.join(", ")),
    };
    format!(
        "this batch commits, and {processes} running in this job's working tree on {machine}, which cannot hold this batch's measured ~{demand} (the most it will ever reserve is {ceiling}). A commit made on {alternative}, which can, would leave that live process on a stale tree. Stop the process, or drop `commit_msg` to run it on {alternative} as a verdict-only batch",
        machine = home.executor_name,
        demand = describe_gib(cannot_hold.ceiling.estimate_bytes),
        ceiling = describe_gib(cannot_hold.ceiling.ceiling_bytes),
        alternative = cannot_hold.alternative_executor_name,
    )
}

/// Everything the batch tail needs, owned and `'static` so one settlement can
/// outlive the call that started it.
///
/// Both the synchronous path and the suspended path settle through the same
/// function with the same context. That is what makes publication
/// duration-invariant by construction rather than by two code paths agreeing.
struct RunBatchSettlement {
    request: McpCallbackRequest,
    run_context: Option<crate::mcp::handlers::RunContext>,
    commit_msg: Option<String>,
    /// A `branch:` run: a verdict about another revision, never published.
    verdict_only: bool,
    /// For a `branch:` run, the label naming the revision it ran against.
    revision_label: Option<String>,
    logical_resolution: Option<super::branch::BranchResolution>,
    store_lock: Option<std::path::PathBuf>,
    routed_request: Option<CellRequest>,
    cwd: String,
    tool_use_id: String,
    cd_advisory: String,
    interpreter_tip: Option<&'static str>,
    vcs: Option<Box<dyn crate::mcp::vcs::WorktreeVcs>>,
    status_before: Option<crate::mcp::vcs::VcsSnapshot>,
    /// A non-empty reason opting this batch out of the conflict-marker guard.
    marker_escape: Option<String>,
}

impl RunBatchSettlement {
    /// Whether the batch's tracked edits were undone rather than published.
    /// Decided by where the batch actually ran, which placement reports back:
    /// a batch can detach after its environment turns out to be elsewhere.
    fn discards_tracked_changes(&self, residence: BatchResidence) -> bool {
        self.verdict_only || (residence == BatchResidence::Detached && self.commit_msg.is_none())
    }
}

/// A settled batch: the composed agent-facing text plus any image blocks.
struct SettledRunBatch {
    text: String,
    images: Vec<cairn_common::read::ImageBlock>,
    suspended: bool,
}

impl SettledRunBatch {
    fn failure(text: String) -> Self {
        Self {
            text,
            images: Vec::new(),
            suspended: false,
        }
    }

    fn suspended() -> Self {
        Self {
            text: String::new(),
            images: Vec::new(),
            suspended: true,
        }
    }
}

/// The job's execution home, and what a placement attempt needs to acquire it.
struct JobEnvironment {
    db: std::sync::Arc<crate::storage::LocalDb>,
    job_id: String,
    base_commit: String,
}

/// Everything one attempt at placing this batch needs, kept so a second attempt
/// can be made from the same facts rather than re-derived from the request.
struct BatchPlacement {
    /// The request every attempt is a variation of. `attempt_id` is the only
    /// thing that belongs to an attempt; everything else, the wait horizon
    /// included, is fixed for the batch.
    template: CellRequest,
    /// The job whose one execution home this batch runs in, when it has one.
    environment: Option<JobEnvironment>,
    /// Where the batch was decided to run before its environment was acquired.
    /// An attempt that acquires the environment decides again from where it
    /// actually is.
    residence: BatchResidence,
    /// Whether the batch declared a `commit_msg`, which decides whether its
    /// edits are kept or undone inside a shared execution home.
    commit_present: bool,
    /// How long the agent itself allowed this batch to spend looking for room,
    /// counted from [`BatchPlacement::begin_waiting_for_room`].
    capacity_wait_budget: Option<std::time::Duration>,
    /// The batch ceiling as an instant, fixed when placement began. Acquiring
    /// the job's execution home waits on this rather than on the declared
    /// budget, and the declared horizon is capped by it.
    ceiling_horizon_unix_ms: u64,
}

impl BatchPlacement {
    /// Start the batch's own clock: from here its declared budget bounds how
    /// long it waits for room, both in the executor's queue (the horizon every
    /// later presentation carries) and across re-presentations (the returned
    /// wait). Called once, when the batch first has the home it runs in.
    fn begin_waiting_for_room(&mut self) -> CapacityWait {
        self.template.wait_horizon_unix_ms =
            room_wait_horizon_unix_ms(self.capacity_wait_budget, self.ceiling_horizon_unix_ms);
        CapacityWait::new(self.capacity_wait_budget)
    }

    /// One attempt at placing this batch: its own identity, and the execution
    /// home it just acquired.
    ///
    /// The wait horizon is deliberately NOT touched here. It is the batch's
    /// answer to when its result stops being wanted, and a second presentation
    /// does not renew that answer any more than it renews the batch.
    fn attempt(
        &self,
        home: Option<&HomeExecutor>,
        fenced: bool,
        residence: BatchResidence,
    ) -> CellRequest {
        let mut request = self.template.clone();
        request.attempt_id = Uuid::new_v4().to_string();
        // In a shared execution home the executor cannot restore the whole tree
        // after the fact, so "no commit_msg means the edits are discarded" has to
        // be decided before the batch runs: the executor then undoes exactly the
        // paths this batch changed. A detached batch has no tree to share and
        // discards the same way.
        if !self.commit_present && (fenced || residence == BatchResidence::Detached) {
            request.mutation_policy = MutationPolicy::PureVerdict;
        }
        request.pinned_executor_id = match residence {
            BatchResidence::Home => home_bound_pin(home),
            BatchResidence::Detached => None,
        };
        request
    }

    /// Where this attempt runs, now that the environment's machine is known.
    ///
    /// `cannot_hold` is what an earlier attempt learned from placement: the
    /// home can never hold this batch while another machine can.
    async fn residence_for(
        &self,
        orch: &Orchestrator,
        home: Option<&HomeExecutor>,
        cannot_hold: Option<&CannotHold>,
    ) -> Result<BatchResidence, String> {
        let selector = self
            .template
            .executor
            .as_ref()
            .filter(|selector| !selector.is_empty());
        let Some(environment) = self.environment.as_ref() else {
            return Ok(self.residence);
        };
        if selector.is_none() && cannot_hold.is_none() {
            return Ok(self.residence);
        }
        // The fence is the authority here: the home is where acquisition found it.
        let facts = home_facts(
            orch,
            Some(&environment.db),
            &environment.job_id,
            home.map(|home| (home.executor_id.as_str(), true)),
            selector,
            self.commit_present,
            cannot_hold.cloned(),
        )
        .await;
        BatchResidence::decide(selector, facts.as_ref(), self.commit_present)
    }
}

/// The machine a job's execution home is resident on, in both the identity that
/// pins placement and the name a refusal can print.
struct HomeExecutor {
    executor_id: String,
}

fn resolve_home_executor(
    orch: &Orchestrator,
    fence: Option<&cairn_common::executor_protocol::ResidencyFence>,
) -> Option<HomeExecutor> {
    let executor_id = orch.fleet.residency_route_executor(&fence?.holder)?;
    Some(HomeExecutor { executor_id })
}

/// Pin a home-bound batch to the executor holding its leased cell.
fn home_bound_pin(home: Option<&HomeExecutor>) -> Option<String> {
    home.map(|home| home.executor_id.clone())
}

/// What the placement task produced.
enum PlacedBatch {
    /// The batch reached an executor, which answered, from the residence it
    /// actually ran in, with any note about where placement put it. Boxed
    /// because a completed outcome carries the whole batch result and dwarfs
    /// the refusals beside it.
    Cell(Box<CellOutcome>, BatchResidence, PlacementNotes),
    /// The batch's own request contradicts a live process it would strand.
    Refused(String),
    /// The job's execution home could not be acquired, for a reason no amount of
    /// waiting changes.
    NoEnvironment(String),
    /// The machine stayed full for longer than the agent's own declared bound.
    NoRoom(String),
}

/// What placement has to say about where a batch went, kept apart by what
/// each claim needs in order to be true.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PlacementNotes {
    /// "Ran on **X** (os).", the machine that claimed the batch. True only of
    /// an outcome that executed there, which is the only kind that carries it.
    ran_on: Option<String>,
    /// Why a batch detached because its home cannot hold it ran where it did.
    /// True only of an outcome that executed there.
    ran_elsewhere: Option<String>,
    /// The batch was placed on a machine its measured memory exceeds. Set only
    /// once a machine claimed it, so it is true of every outcome after that,
    /// and it must survive a failure: that is the case that ends in an
    /// out-of-memory kill, and the one an operator most needs explained.
    above_ceiling: Option<String>,
}

impl PlacementNotes {
    /// Everything, for an outcome that executed.
    fn executed(&self) -> Option<String> {
        [self.ran_elsewhere.clone(), self.above_ceiling.clone()]
            .into_iter()
            .flatten()
            .reduce(|left, right| format!("{left}\n\n{right}"))
    }

    /// Only what a claim makes true, for an outcome that may not have run.
    fn claimed(&self) -> Option<String> {
        self.above_ceiling.clone()
    }
}

/// A settled failure's text with whatever placement note its outcome can
/// truthfully carry: "Ran on X" only after the batch executed, and the
/// above-ceiling note after any claim. The failure itself, and whether it may
/// say nothing ran, is [`failure_without_result`]'s to decide.
fn settled_failure_text(
    outcome: &CellOutcome,
    grant_without_result: Option<crate::fleet::GrantWithoutResult>,
    commits: bool,
    ceiling: Option<String>,
    notes: &PlacementNotes,
) -> Option<String> {
    let text = failure_without_result(outcome, grant_without_result, commits, ceiling)?.text();
    let note = match outcome {
        CellOutcome::FailedAfterExecution { .. } | CellOutcome::Completed { .. } => {
            [notes.ran_on.clone(), notes.executed()]
                .into_iter()
                .flatten()
                .reduce(|left, right| format!("{left}\n\n{right}"))
        }
        CellOutcome::Unavailable { .. }
        | CellOutcome::StorageFailure { .. }
        | CellOutcome::Cancelled { .. } => notes.claimed(),
    };
    Some(match note {
        Some(note) => format!("{text}\n\n{note}"),
        None => text,
    })
}

/// What the placement task produced, and whether any attempt along the way was
/// granted to a machine that never answered for it.
struct BatchAttempts {
    placed: PlacedBatch,
    /// The latest attempt granted to a machine that never answered for it. Kept
    /// across attempts: once one exists, no later refusal may say that nothing
    /// ran, because that earlier attempt may have.
    grant_without_result: Option<crate::fleet::GrantWithoutResult>,
}

/// Run a routed batch, racing it against the grace window. The batch runs on its
/// own task so it survives this handler returning — which is the entire point,
/// and why the task deliberately does NOT take the `AbortOnDrop` guard the
/// parallel host-item path uses.
///
/// Placement runs on that task too, rather than ahead of it. That is what makes
/// "no room right now" a property of how long the batch takes instead of whether
/// it works: a batch still looking for somewhere to run when grace expires
/// suspends exactly like one that is already running, and the agent is never
/// woken to be told a fact more tokens cannot act on (CAIRN-3258).
async fn run_routed_batch(
    orch: &Orchestrator,
    settlement: RunBatchSettlement,
    placement: BatchPlacement,
    batch: ResolvedRunBatch,
) -> String {
    let request_id = placement.template.request_id.clone();
    // The ceiling cancels a batch that never ends. Between two placement
    // attempts there is no cell request for the fleet-level cancel to reach, so
    // the loop reads this before presenting the batch again.
    let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let task_orch = orch.clone();
    let task_cancelled = cancelled.clone();
    let mut handle = tokio::spawn(async move {
        place_and_run_batch(&task_orch, placement, batch, task_cancelled).await
    });
    match tokio::time::timeout(run_grace_window(), &mut handle).await {
        Ok(joined) => {
            let settled = off_frame(|| settle_joined_batch(orch, &settlement, joined, None)).await;
            if settled.suspended {
                return run_envelope(RUN_ITEM_SUSPENDED_MARKER.to_string(), Vec::new());
            }
            run_envelope(settled.text, settled.images)
        }
        Err(_) => {
            off_frame(|| {
                suspend_until_batch_settles(orch, settlement, handle, request_id, cancelled)
            })
            .await
        }
    }
}

/// How long a batch pauses before presenting itself to the fleet again after
/// being told there is no room.
///
/// Deliberately short. A request that reaches the queue waits there for its whole
/// horizon and never comes back here, so this only paces the refusals that never
/// entered a queue at all — a full waiting room, a fleet with nothing free to
/// select. Keeping it small also keeps the batch visible as queued work: the
/// operator's Running panel renders the executor's queue, and a long pause
/// between attempts would blink the row out of it.
const CAPACITY_RETRY_INITIAL: std::time::Duration = std::time::Duration::from_secs(1);
const CAPACITY_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(5);

/// Spread a backoff over its own duration so simultaneous probers do not
/// re-arrive together. Deterministic per call site is not required here — the
/// point is precisely that two callers disagree — so this reads the clock rather
/// than pulling in a random-number generator.
fn jittered(backoff: std::time::Duration) -> std::time::Duration {
    let span = backoff.as_millis().max(1) as u64;
    let offset = crate::fleet::unix_time_ms() % span;
    backoff + std::time::Duration::from_millis(offset)
}

/// The pause between placement attempts, and the agent's own bound on how many
/// of them it is willing to sit through.
struct CapacityWait {
    started: std::time::Instant,
    backoff: std::time::Duration,
    budget: Option<std::time::Duration>,
}

impl CapacityWait {
    fn new(budget: Option<std::time::Duration>) -> Self {
        Self {
            started: std::time::Instant::now(),
            backoff: CAPACITY_RETRY_INITIAL,
            budget,
        }
    }

    /// Hold before the next attempt, or report that the declared budget is spent.
    ///
    /// The elapsed time measured here is wall time across the whole placement,
    /// not the sum of these pauses: nearly all of a congested batch's wait is
    /// spent queued inside an attempt, so counting only the pauses would make a
    /// declared bound mean almost nothing.
    async fn hold(&mut self, request_id: &str, diagnostic: &str) -> Result<(), String> {
        if let Some(budget) = self.budget {
            let elapsed = self.started.elapsed();
            if elapsed >= budget {
                return Err(format!(
                    "the machine had no room to run it, and the {}s this batch's own item timeouts allow for waiting elapsed while it queued ({diagnostic}).",
                    budget.as_secs()
                ));
            }
        }
        log::info!(
            "run batch {request_id} has no room yet, waiting rather than refusing: {diagnostic}"
        );
        // Jittered, because everything that reaches this path was refused by the
        // same full waiting room at the same instant. Unjittered backoff makes
        // those probers converge on one schedule and re-arrive together, so the
        // room is full again at the moment they all look.
        tokio::time::sleep(jittered(self.backoff)).await;
        self.backoff = (self.backoff * 2).min(CAPACITY_RETRY_MAX);
        Ok(())
    }
}

/// Acquire this batch's execution home, place it, and run it — waiting rather
/// than refusing whenever the only thing wrong is that the machine is full.
///
/// A capacity-shaped failure is not an answer, it is a "not yet": the batch is
/// presented again rather than refused, and the waiting happens here instead of
/// in an agent that would answer a refusal by retrying into the same congestion.
///
/// A re-presented request gets a fresh enqueue sequence, but it keeps the
/// requester's original waiting time. The executor ranks by aged priority, then
/// that waiting time, and uses the sequence only to break an otherwise exact
/// tie, so re-presentation preserves the seniority the batch already earned. A
/// `QueueFull` refusal never held a place to begin with.
///
/// A structural failure is returned at once,
/// because waiting on an unsatisfiable constraint or a missing executor strands
/// the agent in a queue it can never leave. [`crate::fleet::placement`] owns
/// which is which.
async fn place_and_run_batch(
    orch: &Orchestrator,
    placement: BatchPlacement,
    batch: ResolvedRunBatch,
    cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> BatchAttempts {
    let mut grant_without_result = None;
    let placed =
        place_and_run_attempts(orch, placement, batch, cancelled, &mut grant_without_result).await;
    BatchAttempts {
        placed,
        grant_without_result,
    }
}

/// The attempt loop behind [`place_and_run_batch`], recording into
/// `grant_without_result` every attempt a machine was granted and never answered.
async fn place_and_run_attempts(
    orch: &Orchestrator,
    mut placement: BatchPlacement,
    batch: ResolvedRunBatch,
    cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    grant_without_result: &mut Option<crate::fleet::GrantWithoutResult>,
) -> PlacedBatch {
    use std::sync::atomic::Ordering;

    let request_id = placement.template.request_id.clone();
    // Before the home is acquired, and therefore before anything can wait: a
    // batch whose declared executor contradicts where it must run is answered
    // now, with the contradiction, rather than after a queue it was never going
    // to leave.
    //
    // Until the home is in hand the batch is being provisioned, not waiting for
    // room, so a capacity refusal of the acquisition itself is paced but bounded
    // only by the ceiling. The batch's own wait begins with its first home.
    let mut provisioning = CapacityWait::new(None);
    let mut waiting_for_room: Option<CapacityWait> = None;
    // Learned from a home attempt placement refused because the home can never
    // hold this batch, and kept for every later attempt: a machine's size does
    // not change between them.
    let mut cannot_hold: Option<CannotHold> = None;
    loop {
        if cancelled.load(Ordering::SeqCst) {
            return PlacedBatch::Cell(
                Box::new(CellOutcome::Cancelled {
                    request_id: request_id.clone(),
                    attempt_id: String::new(),
                }),
                placement.residence,
                PlacementNotes::default(),
            );
        }
        let fence = match placement.environment.as_ref() {
            None => None,
            // The home is the job's, shared with its terminals, REPLs, and
            // later batches, so standing it up waits on the batch ceiling
            // rather than on this batch's declared budget. A cold cell that
            // legitimately takes minutes is then slow rather than refused, and
            // a job whose batches all declare short timeouts still gets a home:
            // an acquisition abandoned at a one-second bound is cancelled, and
            // the next batch would only start it over.
            Some(environment) => match crate::fleet::residency::acquire_job_residency(
                orch,
                &environment.db,
                &environment.job_id,
                &environment.base_commit,
                placement.ceiling_horizon_unix_ms,
                placement.template.waiting_since_unix_ms,
                None,
            )
            .await
            {
                Ok(fence) => Some(fence),
                Err(refusal) if refusal.verdict.is_capacity() => {
                    let wait = waiting_for_room.as_mut().unwrap_or(&mut provisioning);
                    match wait.hold(&request_id, &refusal.diagnostic).await {
                        Ok(()) => continue,
                        Err(spent) => return PlacedBatch::NoRoom(spent),
                    }
                }
                Err(refusal) => return PlacedBatch::NoEnvironment(refusal.diagnostic),
            },
        };
        if waiting_for_room.is_none() {
            waiting_for_room = Some(placement.begin_waiting_for_room());
        }
        let home = resolve_home_executor(orch, fence.as_ref());
        let residence = match placement
            .residence_for(orch, home.as_ref(), cannot_hold.as_ref())
            .await
        {
            Ok(residence) => residence,
            Err(refusal) => return PlacedBatch::Refused(refusal),
        };
        let mut request = placement.attempt(home.as_ref(), fence.is_some(), residence);
        // A batch detached because its home cannot hold it goes where placement
        // said it can be held, by pin. A pin rather than spill mobility keeps a
        // committing batch from ever being replayed on a third machine when the
        // one running it disappears.
        let detached_for_memory = cannot_hold
            .as_ref()
            .filter(|_| residence == BatchResidence::Detached);
        if let Some(cannot_hold) = detached_for_memory {
            request.pinned_executor_id = Some(cannot_hold.alternative_executor_id.clone());
        }
        let home_name = home
            .as_ref()
            .map(|home| orch.fleet.executor_name_for_text(&home.executor_id));
        let mut attempt = batch.clone();
        attempt.execution_residency = match residence {
            BatchResidence::Home => fence,
            BatchResidence::Detached => None,
        };
        let executed = orch.fleet.submit_run_batch(orch, request, attempt).await;
        if let Some(grant) = executed.grant_without_result {
            *grant_without_result = Some(grant);
        }
        let above_ceiling = executed.above_ceiling;
        let claimed = executed.account.placement;
        let outcome = executed.outcome;
        if residence == BatchResidence::Home && cannot_hold.is_none() {
            if let Some(learned) = CannotHold::from_outcome(&outcome) {
                cannot_hold = Some(learned);
                continue;
            }
        }
        if !crate::fleet::placement::classify_cell_outcome(&outcome, orch.fleet.link_restoration())
            .is_capacity()
        {
            let detached_from = match residence {
                BatchResidence::Detached => home_name.as_deref(),
                BatchResidence::Home => None,
            };
            let notes = PlacementNotes {
                ran_on: ran_on_line(orch, claimed.as_ref(), &outcome, detached_from),
                ran_elsewhere: detached_for_memory.map(|cannot_hold| {
                    cannot_hold.ran_elsewhere_note(home_name.as_deref().unwrap_or("its home"))
                }),
                above_ceiling: above_ceiling.map(|above| above.note()),
            };
            return PlacedBatch::Cell(Box::new(outcome), residence, notes);
        }
        let diagnostic = match &outcome {
            CellOutcome::Unavailable { diagnostic, .. } => diagnostic.clone(),
            _ => String::new(),
        };
        let wait = waiting_for_room
            .as_mut()
            .expect("the batch began waiting for room before its first presentation");
        match wait.hold(&request_id, &diagnostic).await {
            Ok(()) => {}
            Err(spent) => return PlacedBatch::NoRoom(spent),
        }
    }
}

/// Park the turn on the durable suspend core and settle the batch off-call.
///
/// The suspension row is established BEFORE the resolver is armed; see
/// [`crate::mcp::handlers::durable_suspend`] for why that order is load-bearing.
async fn suspend_until_batch_settles(
    orch: &Orchestrator,
    settlement: RunBatchSettlement,
    mut handle: tokio::task::JoinHandle<BatchAttempts>,
    request_id: String,
    cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> String {
    use crate::mcp::handlers::durable_suspend;

    let suspended = begin_batch_suspension(orch, &settlement, &request_id, false).await;
    let Some((record, db, handoff)) = suspended else {
        // Nothing to park (no correlatable call, no active turn): hold the call
        // open rather than abandoning a running batch.
        let joined = handle.await;
        let settled = off_frame(|| settle_joined_batch(orch, &settlement, joined, None)).await;
        if settled.suspended {
            return run_envelope(RUN_ITEM_SUSPENDED_MARKER.to_string(), Vec::new());
        }
        return run_envelope(settled.text, settled.images);
    };

    let owned_orch = orch.clone();
    tokio::spawn(async move {
        // The ceiling is enforced here, on the suspended path, because that is
        // the only path a never-exiting command can reach. Cancelling routes the
        // batch through the ordinary cancelled-outcome tail, so it inherits
        // exactly the publication semantics any other failure has.
        let remaining = RUN_BATCH_CEILING.saturating_sub(run_grace_window());
        let (joined, ceiling) = match tokio::time::timeout(remaining, &mut handle).await {
            Ok(joined) => (joined, None),
            Err(_) => {
                // Both halves are needed: the flag stops a batch that is between
                // placement attempts, and the fleet cancel stops one that is
                // queued or running inside an attempt.
                cancelled.store(true, std::sync::atomic::Ordering::SeqCst);
                owned_orch.cancel_cell_request(&request_id);
                (handle.await, Some(RUN_CEILING_DETAIL.to_string()))
            }
        };
        let settled =
            off_frame(|| settle_joined_batch(&owned_orch, &settlement, joined, ceiling)).await;
        // Nothing resumes the run until the predecessor turn is actually parked.
        if !handoff.parked().await {
            log::warn!(
                "run batch suspension {} was never parked; leaving it for startup reconciliation",
                record.id
            );
            return;
        }
        // The item's permission request owns the continuation. Never resolve the
        // outer batch wait with a marker that could become a model-facing result.
        if settled.suspended {
            // Permission insertion atomically retires this outer row, while outer
            // admission refuses to race ahead of an existing permission row.
            return;
        }
        // A permission that took this call over but did not suspend the item is
        // being withdrawn, and its withdrawal hands this row back.
        durable_suspend::await_inner_wait_release(&db, &record).await;
        // A suspended batch cannot carry images: only an MCP gateway call
        // produces them, and a batch may not mix gateway items with the process
        // items that are the only thing routed here.
        debug_assert!(
            settled.images.is_empty(),
            "a routed process batch produced image blocks a suspended resume cannot carry"
        );
        if let Err(error) =
            durable_suspend::resolve(&owned_orch, &db, &record, &settled.text, false).await
        {
            log::warn!("run batch suspension resolution failed: {error}");
        }
    });
    run_envelope(RUN_BATCH_SUSPENDED_MARKER.to_string(), Vec::new())
}

/// Correlate this batch's callback with the `run` tool call that issued it, by
/// finding that call in the current turn's transcript. Two callers share it: a
/// batch establishing a durable suspension, and a workflow run target binding
/// its delegated packet (`mcp::handlers::workflows`).
///
/// This is the identity for every MCP-hosted agent, not a fallback. The MCP
/// `tools/call` transport carries no provider tool-use id, so `cairn-cmd` sends
/// `tool_use_id: None` for every tool; only the Cairn-native tool loop
/// (`backends::http_loop`), which dispatches tools itself, populates it.
/// Requiring the transport's id is what made the grace contract unreachable in
/// production: no batch could bind a suspension to a call, every long batch fell
/// back to awaiting inline, and the cairn-cmd socket then discarded it whole at
/// grace + 60s (CAIRN-3229). `waitFor` and blocking task/question appends
/// correlate the same way.
///
/// The batch is matched semantically: both sides are parsed into [`RunPayload`]
/// and re-serialized, so a match does not rest on incidental encoding — an
/// omitted optional against an explicit null, a key order, a field added later.
/// Comparing raw JSON is what made no terminal exit wait correlate in CAIRN-3115.
///
/// Matching contents is not by itself an identity, though, and this returns a
/// [`Claim`] rather than an id for that reason. A provider can emit several `run`
/// tool uses in one assistant event — 1.8% of recorded events on this machine
/// carry two to five — and two of them may be byte-identical. Claiming the newest
/// match would then answer one call with the other's result and leave a call
/// answered twice. So the claim is exclusive (unanswered, unclaimed invocations
/// only) and a tie refuses to claim at all, which costs that batch its suspension
/// and costs no call a wrong answer.
pub(super) async fn claim_batch_tool_use_id(
    db: &crate::storage::LocalDb,
    run_id: &str,
    turn_id: &str,
    payload: &serde_json::Value,
) -> Claim {
    let Some(expected) = batch_identity(payload) else {
        return Claim::None;
    };
    super::tool_use_correlation::claim_tool_use_id(db, run_id, turn_id, |name, input| {
        is_this_batchs_call(name, input, &expected)
    })
    .await
}

/// Whether a recorded tool invocation is the `run` call this batch came from.
fn is_this_batchs_call(
    name: &str,
    input: &serde_json::Value,
    expected: &serde_json::Value,
) -> bool {
    (name == "run" || name.ends_with("__run"))
        && batch_identity(input).is_some_and(|identity| &identity == expected)
}

/// A batch's identity for correlation: its payload normalized through the run
/// schema, so two spellings of the same batch compare equal and any key the
/// schema does not model cannot make them differ.
fn batch_identity(input: &serde_json::Value) -> Option<serde_json::Value> {
    let payload = serde_json::from_value::<RunPayload>(input.clone()).ok()?;
    serde_json::to_value(payload).ok()
}

/// How much of a batch label survives: long enough to name a real command,
/// short enough that a section header in a resume prompt stays a header.
const BATCH_LABEL_MAX_CHARS: usize = 60;

/// A short name for this batch, carried on its suspension so a turn resuming
/// with several parked calls can say which result answers which call.
///
/// The batch's first item is the best identity available. `description` is the
/// model's own name for the work and is preferred wherever it wrote one; the
/// rest are what the item actually is. `None` is fine — the composed prompt
/// falls back to naming it a run batch and leans on the provider call id, which
/// is unique regardless.
fn batch_label(payload: &serde_json::Value) -> Option<String> {
    let parsed = serde_json::from_value::<RunPayload>(payload.clone()).ok()?;
    let item = parsed.commands.first()?;
    let raw = item
        .description
        .as_deref()
        .or(item.command.as_deref())
        .or(item.target.as_deref())
        .or(item.interpreter.as_deref())?;
    let first_line = raw.lines().next().unwrap_or(raw).trim();
    if first_line.is_empty() {
        return None;
    }
    if first_line.chars().count() <= BATCH_LABEL_MAX_CHARS {
        return Some(first_line.to_string());
    }
    let head: String = first_line
        .chars()
        .take(BATCH_LABEL_MAX_CHARS - 1)
        .collect::<String>()
        .trim_end()
        .to_string();
    Some(format!("{head}\u{2026}"))
}

/// Persist the suspension row for a batch and park its run. `None` means this
/// call cannot be resumed durably (no correlatable tool call, no run context,
/// or no active turn). A routed batch then keeps awaiting inline; a host batch
/// ends its response items at grace ([`run_suspendable_host_batch`]). `host`
/// marks a batch that executes on the host and so holds no working tree.
async fn begin_batch_suspension(
    orch: &Orchestrator,
    settlement: &RunBatchSettlement,
    request_id: &str,
    host: bool,
) -> Option<(
    crate::mcp::handlers::durable_suspend::Record,
    std::sync::Arc<crate::storage::LocalDb>,
    crate::mcp::handlers::durable_suspend::ParkHandoff,
)> {
    use crate::mcp::handlers::durable_suspend::{self, Condition, Record};

    let (ctx, db) = super::run_context::lookup_run_routed(&orch.db, &settlement.request)
        .await
        .ok()?;
    // The turn this batch came from, which may already be parked on a sibling
    // call of the same turn.
    let turn_id = durable_suspend::suspending_turn_id(orch, &db, &ctx.run_id).await?;
    // The call this suspension will answer: the transport's own id when there is
    // one, and otherwise the transcript claim, which is the only identity an
    // MCP-hosted agent has. Either refusal costs this batch its suspension and
    // nothing else — it keeps awaiting inline — so both are logged rather than
    // passed over in silence.
    let tool_use_id = match settlement.request.tool_use_id.clone() {
        Some(id) => id,
        None => {
            match claim_batch_tool_use_id(&db, &ctx.run_id, &turn_id, &settlement.request.payload)
                .await
            {
                Claim::One(id) => id,
                Claim::None => {
                    log::warn!(
                        "run batch for run {} found no unanswered tool call of its own to suspend on; it will keep awaiting inline",
                        ctx.run_id
                    );
                    return None;
                }
                Claim::Ambiguous(count) => {
                    log::warn!(
                        "run batch for run {} matches {count} indistinguishable open tool calls, so it cannot claim one without risking another call's answer; it will keep awaiting inline",
                        ctx.run_id
                    );
                    return None;
                }
            }
        }
    };
    let session_id = durable_suspend::run_session(&db, &ctx.run_id)
        .await
        .ok()
        .flatten()?;
    let record = Record {
        id: cairn_common::ids::mint_child(&ctx.run_id),
        job_id: ctx.job_id.clone(),
        run_id: ctx.run_id.clone(),
        session_id,
        turn_id,
        tool_use_id,
        condition: Condition::RunBatch {
            request_id: request_id.to_string(),
            commits: settlement.commit_msg.is_some(),
            label: batch_label(&settlement.request.payload),
            host,
        },
        deadline: None,
        created: chrono::Utc::now().timestamp_millis(),
    };
    // Admission to the durable run-batch bracket and base reconciliation share
    // the jj store lock. Reconcile revalidates the row while holding this same
    // guard immediately before moving the bookmark, so exactly one side wins.
    let _bracket_guard = match acquire_store_lock(
        orch,
        settlement.store_lock.as_deref(),
        crate::orchestrator::StoreLockClass::Foreground,
        "run batch suspension admission",
        STORE_LOCK_TIMEOUT,
    )
    .await
    {
        Ok(guard) => guard,
        Err(error) => {
            log::warn!(
                "failed to enter run batch suspension bracket for run {}: {error}",
                ctx.run_id
            );
            return None;
        }
    };
    match durable_suspend::suspend(orch, &db, &record).await {
        Ok(handoff) => Some((record, db, handoff)),
        Err(error) => {
            log::warn!(
                "failed to suspend run batch for run {}: {error}",
                ctx.run_id
            );
            None
        }
    }
}

async fn settle_joined_batch(
    orch: &Orchestrator,
    settlement: &RunBatchSettlement,
    joined: Result<BatchAttempts, tokio::task::JoinError>,
    ceiling: Option<String>,
) -> SettledRunBatch {
    let BatchAttempts {
        placed,
        grant_without_result,
    } = match joined {
        Ok(attempts) => attempts,
        Err(error) => return SettledRunBatch::failure(placement_task_failure(&error).text()),
    };
    let commits = settlement.commit_msg.is_some();
    let refused = |detail: String| {
        SettledRunBatch::failure(
            unexecuted_failure(detail, grant_without_result.clone(), commits).text(),
        )
    };
    match placed {
        PlacedBatch::Cell(outcome, residence, notes) => {
            off_frame(|| {
                settle_routed_run_batch(
                    orch,
                    settlement,
                    residence,
                    *outcome,
                    grant_without_result.clone(),
                    ceiling,
                    notes,
                )
            })
            .await
        }
        PlacedBatch::Refused(refusal) => refused(format!("{refusal}.")),
        PlacedBatch::NoEnvironment(diagnostic) => refused(no_environment_detail(&diagnostic)),
        PlacedBatch::NoRoom(detail) => refused(detail),
    }
}

/// The sentence for a batch whose environment could not be acquired. A project
/// config the build cannot load is not a machine fault, so that refusal is
/// stated as itself, without the stage labels in front of it.
fn no_environment_detail(diagnostic: &str) -> String {
    match diagnostic.find(crate::fleet::PROJECT_CONFIG_REFUSAL) {
        Some(start) => format!("{}.", &diagnostic[start..]),
        None => format!("this job's environment could not be reached ({diagnostic})."),
    }
}

#[cfg(test)]
mod no_environment_detail_tests {
    use super::no_environment_detail;

    #[test]
    fn an_unloadable_project_config_is_named_as_the_project_wide_refusal() {
        let diagnostic = format!(
            "prepare execution environment placement: {}",
            crate::fleet::project_config_refusal(
                "failed to parse project config at /repo/.cairn/config.yaml: \
                 did not find expected ',' or ']' at line 1 column 28"
            )
        );
        let detail = no_environment_detail(&diagnostic);
        assert_eq!(
            detail,
            "this project's .cairn/config.yaml refuses every execution in the project until it \
             loads, because failed to parse project config at /repo/.cairn/config.yaml: did not \
             find expected ',' or ']' at line 1 column 28. Fix or revert the file, or run a \
             Cairn build that understands it."
        );
        assert!(!detail.contains("could not be reached"));
    }

    #[test]
    fn a_machine_fault_keeps_its_environment_framing() {
        assert_eq!(
            no_environment_detail("select execution environment executor: none attached"),
            "this job's environment could not be reached (select execution environment \
             executor: none attached)."
        );
    }
}

/// Convert a routed cell outcome into item outcomes and settle it. Every routed
/// batch — fast or suspended — lands here, so there is exactly one publication
/// path and no second one to drift from it.
async fn settle_routed_run_batch(
    orch: &Orchestrator,
    settlement: &RunBatchSettlement,
    residence: BatchResidence,
    outcome: CellOutcome,
    grant_without_result: Option<crate::fleet::GrantWithoutResult>,
    ceiling: Option<String>,
    notes: PlacementNotes,
) -> SettledRunBatch {
    if let CellOutcome::Unavailable { reason, diagnostic } = &outcome {
        // Only a structural refusal reaches here: a capacity-shaped one is a
        // wait, and `place_and_run_batch` presents the batch again rather than
        // settling it. Composing "this run could not execute" for a machine
        // that was merely busy is the defect CAIRN-3258 removed.
        debug_assert!(
            !crate::fleet::placement::classify_unavailable(reason, orch.fleet.link_restoration())
                .is_capacity(),
            "a capacity-shaped placement failure was settled as a run failure: {diagnostic}"
        );
    }
    if let Some(text) = settled_failure_text(
        &outcome,
        grant_without_result,
        settlement.commit_msg.is_some(),
        ceiling,
        &notes,
    ) {
        return SettledRunBatch::failure(text);
    }
    let CellOutcome::Completed {
        attempt_id,
        output,
        mutation_delta,
        tracked_modifications,
        metadata,
        ..
    } = outcome
    else {
        unreachable!("failure_without_result answers every outcome but a completion");
    };
    let mut publication_request = settlement.routed_request.clone();
    if let Some(request) = publication_request.as_mut() {
        request.attempt_id = attempt_id;
    }
    let (outcomes, routed_delta, routed_tracked_modifications) = match serde_json::from_str::<
        Vec<cairn_common::executor_protocol::ProcessBatchItemOutcome>,
    >(&output)
    {
        Ok(outcomes) => (
            outcomes.into_iter().map(ItemOutcome::from).collect(),
            mutation_delta,
            tracked_modifications,
        ),
        Err(error) => {
            // The commands ran; only their result is unreadable, so this is a
            // publication failure and must classify as one.
            return SettledRunBatch::failure(
                RunFailure::NotPublished(format!("their result could not be read back ({error})."))
                    .text(),
            );
        }
    };
    off_frame(|| {
        settle_run_batch(
            orch,
            settlement,
            residence,
            outcomes,
            routed_delta,
            routed_tracked_modifications,
            publication_request.as_ref(),
            Some(&notes),
            metadata.cell_origin.as_ref(),
        )
    })
    .await
}

/// The first line of a verdict-only result: the revision it ran against and,
/// when the executor said, where the cell came from, so a cold run reads as
/// cold rather than as slow.
fn ran_against_line(
    label: &str,
    cell_origin: Option<&cairn_common::executor_protocol::CellOrigin>,
) -> String {
    match cell_origin {
        Some(origin) => format!("ran against {label}, {}", origin.describe()),
        None => format!("ran against {label}"),
    }
}

/// What the agent reads when its execution environment was rebuilt under it.
///
/// An environment is a lease against a live executor session, and losing one
/// between batches is legal and ordinary. What is not ordinary is the machine
/// itself going away: the rebuilt environment is a fresh checkout of the branch,
/// so committed work is intact and everything only the old one held is gone.
/// Saying so is the whole point — an agent that finds its scratch file missing
/// and is told nothing debugs the wrong thing, and both nodes that hit this
/// discovered the workaround by themselves (CAIRN-4332).
fn environment_rebuild_note(orch: &Orchestrator, s: &RunBatchSettlement) -> Option<String> {
    let context = s.run_context.as_ref()?;
    let holder = cairn_common::executor_protocol::ResidencyHolder::Job {
        job_id: context.job_id.clone(),
    };
    let eviction = orch.fleet.take_environment_eviction(&holder)?;
    Some(environment_rebuild_text(&eviction))
}

fn environment_rebuild_text(eviction: &crate::fleet::EnvironmentEviction) -> String {
    let why = match &eviction.cause {
        crate::fleet::EvictionCause::Disconnected { absent_ms } => format!(
            "disconnected {} ago and did not come back",
            crate::fleet::describe_quiet_ms(*absent_ms)
        ),
        crate::fleet::EvictionCause::Silent { silent_ms, .. } => format!(
            "stopped answering for {} while still connected",
            crate::fleet::describe_quiet_ms(*silent_ms)
        ),
        crate::fleet::EvictionCause::CannotPrepareProject { project_id, detail } => {
            format!("cannot prepare cells for {project_id} ({detail})")
        }
    };
    let processes = match eviction.lost_processes.as_slice() {
        [] => "terminals or REPLs from earlier batches".to_string(),
        processes => processes.join(", "),
    };
    format!(
        "Note: this job's execution environment was rebuilt. The machine holding the previous one (**{}**) {why}, so a fresh checkout of your branch was materialized. Committed work is intact; uncommitted files, installed packages, and {processes} did not survive.",
        eviction.executor
    )
}

/// The batch tail: the verdict-only modification report, the commit barrier and
/// logical-head publication, the synchronous when:write checks, and the composed
/// output with its advisories.
#[allow(clippy::too_many_arguments)]
async fn settle_run_batch(
    orch: &Orchestrator,
    s: &RunBatchSettlement,
    residence: BatchResidence,
    mut outcomes: Vec<ItemOutcome>,
    routed_delta: Option<Box<crate::fleet::MutationDelta>>,
    routed_tracked_modifications: Option<
        cairn_common::executor_protocol::TrackedModificationEvidence,
    >,
    publication_request: Option<&CellRequest>,
    notes: Option<&PlacementNotes>,
    cell_origin: Option<&cairn_common::executor_protocol::CellOrigin>,
) -> SettledRunBatch {
    // Suspension is backend control flow, not item output. This guard is the
    // common settlement boundary for host-local and routed batches and must run
    // before image promotion, composition, publication, or commit handling.
    if batch_is_suspended(&outcomes) {
        return SettledRunBatch::suspended();
    }
    for outcome in &mut outcomes {
        crate::mcp::handlers::durable_images::promote_images(
            orch,
            &s.request,
            &mut outcome.images,
            &mut outcome.body,
        )
        .await;
    }
    let mut result = compose_run_output(&outcomes);
    if let Some(label) = &s.revision_label {
        result = format!("{}\n\n{result}", ran_against_line(label, cell_origin));
    }
    // Every batch a machine ran opens by naming it, selector or not, so a
    // batch is never somewhere its caller cannot see. Host items (MCP tools,
    // decide, REPL sends) run on the host and carry no line.
    if let Some(line) = notes.and_then(|notes| notes.ran_on.as_deref()) {
        let body = match result.is_empty() {
            true => "(no output)",
            false => result.as_str(),
        };
        result = format!("{line}\n\n{body}");
    }
    let residence_note = notes.and_then(PlacementNotes::executed);

    if s.discards_tracked_changes(residence) {
        let mut modified_paths = std::collections::BTreeSet::new();
        if let Some(evidence) = routed_tracked_modifications {
            modified_paths.extend(evidence.paths);
        }
        for evidence in outcomes
            .iter()
            .filter_map(|outcome| outcome.tracked_modifications.as_ref())
        {
            modified_paths.extend(evidence.paths.iter().cloned());
        }
        if !modified_paths.is_empty() {
            if !result.is_empty() {
                result.push_str("\n\n");
            }
            result.push_str(&format!(
                "Verdict-only run modified {} tracked path(s); the mutation was discarded: {}",
                modified_paths.len(),
                modified_paths.into_iter().collect::<Vec<_>>().join(", ")
            ));
        }
        if let Some(note) = environment_rebuild_note(orch, s) {
            if !result.is_empty() {
                result.push_str("\n\n");
            }
            result.push_str(&note);
        }
        if let Some(note) = &residence_note {
            if !result.is_empty() {
                result.push_str("\n\n");
            }
            result.push_str(note);
        }
        if !s.cd_advisory.is_empty() {
            if !result.is_empty() {
                result.push_str("\n\n");
            }
            result.push_str(&s.cd_advisory);
        }
        if let Some(tip) = s.interpreter_tip {
            if !result.is_empty() {
                result.push_str("\n\n");
            }
            result.push_str(tip);
        }
        let text = if result.is_empty() {
            "(no output)".to_string()
        } else {
            result
        };
        return SettledRunBatch {
            text,
            images: collect_run_images(outcomes),
            suspended: false,
        };
    }

    // Publish an authenticated cell delta through the runner-owned logical-head
    // transaction. Ambient user operations retain their physical hygiene gate.
    let all_ok = outcomes.iter().all(|o| o.succeeded);
    let checkout_path = std::path::Path::new(&s.cwd);
    let author = match s.commit_msg.as_deref() {
        Some(_) => s
            .run_context
            .as_ref()
            .and_then(|ctx| orch.resolve_git_identity_for_project(Some(&ctx.project_id)))
            .map(|(name, email)| PublicationIdentity::new(name, email)),
        None => None,
    };
    // Serialize the seal/discard inside the barrier on the per-store jj lock that
    // base-advance reconcile and merge-fold also hold, so a run-path seal never
    // forks the shared store's operation log against a concurrent reconcile/fold.
    // The guard scopes ONLY the barrier's store mutation — the pre-batch snapshot
    // and per-item command execution above stay outside it (per-workspace reads /
    // FS work, not shared-store rebase/import). `None` for a non-worktree cwd.
    // `store_lock` is the same handle resolved for the pre-flight reconcile above.
    // Held deltas this barrier republished. Settled once the store lock is
    // released, because their remaining rungs include an origin push.
    let mut drained = Vec::new();
    // A routed delta is installed, pinned, and integrated against the head it
    // finds now, before the lock (I3, I4). Under the lock the plan lands only if
    // the bookmark still holds that head — a held delta the drain below lands
    // first moves it, and then the integration is simply done again there.
    let prepared_routed = match (
        s.commit_msg.as_deref(),
        routed_delta.as_ref(),
        publication_request.or(s.routed_request.as_ref()),
        s.logical_resolution.as_ref(),
    ) {
        (Some(message), Some(delta), Some(request), Some(resolution)) => {
            Some(match s.store_lock.as_deref() {
                Some(_store) => match resolve_runner_publication_target(resolution, request) {
                    Ok(target) => match prepare_slot_publication(
                        orch,
                        &target,
                        request,
                        delta,
                        message,
                        author.as_ref(),
                    )
                    .await
                    {
                        Ok(prepared) => Ok((prepared, target, message, delta, request)),
                        Err(error) => Err((error, Some(target))),
                    },
                    Err(error) => Err((
                        SlotPublicationError::NotInStore {
                            delta: delta.delta_commit.clone(),
                            error,
                        },
                        None,
                    )),
                },
                None => Err((
                    SlotPublicationError::NotInStore {
                        delta: delta.delta_commit.clone(),
                        error: "build-slot publication has no resolved shared-store lock path"
                            .to_string(),
                    },
                    None,
                )),
            })
        }
        _ => None,
    };
    let barrier = match acquire_store_lock(
        orch,
        s.store_lock.as_deref(), crate::orchestrator::StoreLockClass::Foreground,
        "run commit barrier publication",
        STORE_LOCK_TIMEOUT,
    )
    .await
    {
        Ok(_store_guard) => {
            let barrier_trace = _store_guard.as_ref().map(|guard| guard.trace());
            // A commit_msg makes this batch a commit barrier on its branch. A
            // delta an earlier batch sealed but could not publish lands first,
            // ahead of this batch's own changes, and whether or not this batch
            // changed anything: a trivial batch with a commit_msg is the lever
            // an agent is given for publishing a held commit now.
            if s.commit_msg.is_some() {
                if let Some(resolution) = s.logical_resolution.as_ref() {
                    drained = held::drain_held_publications(
                        orch,
                        &resolution.project_id,
                        &resolution.rev,
                        barrier_trace.as_ref(),
                    )
                    .await;
                }
            }
            let routed = match prepared_routed {
                Some(Ok((prepared, target, message, delta, request))) => {
                    Some(match land_prepared_slot_delta(
                        orch,
                        &target,
                        request,
                        delta,
                        message,
                        author.as_ref(),
                        barrier_trace.as_ref(),
                        prepared,
                    )
                    .await
                    {
                        Ok(publication) => Ok((publication, target)),
                        Err(error) => Err((error, Some(target))),
                    })
                }
                Some(Err(error)) => Some(Err(error)),
                None => None,
            };
            // A routed publication's store work ends with its landing, or with
            // the hold or strand that replaced it. Everything its arms do next —
            // receipt finalization, the patch capture, the origin push — is
            // network I/O or a read of immutable commits (I6), so the lock is
            // released here for every routed outcome. Only the ambient barrier
            // below still needs it.
            let _store_guard = if routed.is_some() {
                drop(_store_guard);
                None
            } else {
                _store_guard
            };
            match routed {
                None if s.logical_resolution.is_some() => CommitBarrierOutcome {
                    // A bound batch reports discarded work as tracked
                    // modifications (it already undid them); an unbound one
                    // reports a sealed delta the runner declines to publish.
                    message: if (routed_delta.is_some()
                        || routed_tracked_modifications.is_some())
                        && s.commit_msg.is_none()
                    {
                        "Executor changes were discarded because this run batch had no commit_msg".to_string()
                    } else {
                        String::new()
                    },
                    worktree_changed: false,
                    committed: false,
                },
                None => run_commit_barrier(
                    s.vcs
                        .as_ref()
                        .expect("ambient run resolves a physical VCS")
                        .as_ref(),
                    checkout_path,
                    s.commit_msg.as_deref(),
                    all_ok,
                    s.status_before.as_ref(),
                    author.as_ref(),
                    s.marker_escape.as_deref(),
                ),
                Some(Ok((publication, target))) => {
                    let SlotPublication {
                        consume_receipt,
                        mut outcome,
                    } = publication;
                    // The lock was released above; the patch capture reads
                    // immutable commits.
                    outcome.capture_patch(&target.repository);
                    // The upload was validated and installed before either
                    // outcome was reached, so it is finalized on both.
                    let mut notes = String::new();
                    if consume_receipt {
                        if let Some(delta) = routed_delta.as_ref() {
                            if let Err(error) = finalize_delta_receipt(orch, &target, delta).await {
                                notes.push_str(&format!(
                                    " — ⚠️ the delta upload was not finalized: {error}"
                                ));
                            }
                        }
                    }
                    let routed = super::run_context::lookup_run_routed(&orch.db, &s.request)
                        .await
                        .ok();
                    match outcome {
                        SlotPublicationOutcome::AlreadyLanded { head } => CommitBarrierOutcome {
                            message: format!(
                                "The branch already carries this batch's changes at {head}, so nothing new was committed.{notes}"
                            ),
                            worktree_changed: false,
                            committed: false,
                        },
                        SlotPublicationOutcome::Published {
                            landed,
                            export,
                            patch,
                            patch_unavailable,
                            integration,
                            ..
                        } => {
                            let resolution = s
                                .logical_resolution
                                .as_ref()
                                .expect("a routed publication has a logical resolution");
                            let rungs = climb_landing_rungs(
                                orch,
                                &LandingRungs {
                                    project_id: &resolution.project_id,
                                    object_repository: &resolution.object_repository_path,
                                    store_dir: &target.store_dir,
                                    branch: &target.branch,
                                    routed: routed.as_ref().map(|(run, db)| RoutedLanding {
                                        job_id: &run.job_id,
                                        project_id: &run.project_id,
                                        db,
                                    }),
                                },
                                &landed.head,
                                export,
                            )
                            .await;
                            let mut message = match rungs.published {
                                Err(error) => format!(
                                    "⚠️ {}",
                                    super::unpublished_commit_message(&landed.head, &error)
                                ),
                                Ok(()) => {
                                    let mut message =
                                        format!("✓ Committed changes ({})", landed.head);
                                    let (additions, deletions) = crate::jj::parse_git_patch(&patch)
                                        .iter()
                                        .fold((0, 0), |(add, del), change| {
                                            (add + change.additions, del + change.deletions)
                                        });
                                    if additions > 0 || deletions > 0 {
                                        message.push_str(&format!(" +{additions}/-{deletions}"));
                                    }
                                    if let Some(error) = patch_unavailable.as_deref() {
                                        message.push_str(&format!(
                                            " — line counts unavailable: {error}"
                                        ));
                                    }
                                    if let Some(note) = landed.amend_note.as_deref() {
                                        message.push_str(&format!(" — {note}"));
                                    }
                                    if let Some(note) = integration.as_ref() {
                                        message.push_str(&format!(
                                            " — the branch moved to {} while this batch ran; these changes were merged onto it",
                                            note.head
                                        ));
                                        if note.amend_converted {
                                            message.push_str(
                                                ", and the amend was recorded as a new commit because the \
                                                 branch's own commit is no longer the one this batch built on",
                                            );
                                        }
                                    }
                                    message
                                }
                            };
                            message.push_str(&rungs.warnings);
                            message.push_str(&notes);
                            CommitBarrierOutcome {
                                message,
                                worktree_changed: false,
                                committed: true,
                            }
                        }
                    }
                }
                Some(Err((error, target))) => {
                    let mut message = format!("⚠️ {error}");
                    // A held delta was installed like a published one, so its
                    // upload is spent the same way.
                    if let (
                        SlotPublicationError::Held {
                            consume_receipt: true,
                            ..
                        },
                        Some(target),
                        Some(delta),
                    ) = (&error, target.as_ref(), routed_delta.as_ref())
                    {
                        if let Err(error) = finalize_delta_receipt(orch, target, delta).await {
                            message.push_str(&format!(
                                " — ⚠️ the delta upload was not finalized: {error}"
                            ));
                        }
                    }
                    CommitBarrierOutcome {
                        message,
                        worktree_changed: false,
                        committed: false,
                    }
                }
            }
        }
        Err(error) => CommitBarrierOutcome {
            message: format!("⚠️ {error} Nothing was committed and the working copy was PRESERVED exactly. Retry with a trivial `run` carrying the same `commit_msg`; the commit barrier will seal any remaining dirty worktree."),
            worktree_changed: false,
            committed: false,
        },
    };
    if barrier.worktree_changed {
        let _ = orch.services.emitter.emit(
            "worktree-changed",
            serde_json::json!({"checkout_path": s.cwd.clone()}),
        );
    }
    // A republished held commit advanced the branch just as this batch's own
    // commit would, so it owes the same checks.
    let committed = barrier.committed || held::any_landed(&drained);
    // Ahead of the batch's own outcome: the held commits landed first.
    for note in held::settle_drained(orch, drained).await {
        if !result.is_empty() {
            result.push_str("\n\n");
        }
        result.push_str(&note);
    }
    if !barrier.message.is_empty() {
        if !result.is_empty() {
            result.push_str("\n\n");
        }
        result.push_str(&barrier.message);
    }
    // when:write check runner: a sealed source-touching commit fires the affected
    // when:write checks against that commit, streams their output live into this
    // tool's transcript, and appends a compact pass/fail line — or a pending line
    // once the bounded inline wait elapses, with the wave finishing in the
    // background. Gated on an actually-landed commit (`committed` is true only
    // with commit_msg + a successful seal or republication).
    if committed {
        // A commit just sealed → the branch advanced. Cancel any in-flight
        // when:review suite for this job so its heavy concurrent compiles stop
        // starving this commit's own when:write checks (below) and the agent's
        // next manual check run; the review cadence relaunches fresh at the next
        // turn-end. See cancel_stale_review_on_branch_advance for the rationale
        // and the deliberate job-id scoping.
        if let Some(ctx) = s.run_context.as_ref() {
            crate::execution::checks::cancel_stale_review_on_branch_advance(
                orch,
                &ctx.job_id,
                crate::execution::checks::BranchAdvance::Commit,
            )
            .await;
        }
        if let Some(summary) = crate::execution::checks::run_write_checks_after_seal(
            orch,
            s.run_context.as_ref(),
            &s.cwd,
            &s.tool_use_id,
            crate::execution::checks::WriteCheckTarget::CurrentHead,
        )
        .await
        {
            if !result.is_empty() {
                result.push_str("\n\n");
            }
            result.push_str(&summary);
        }
    }
    // Separately typed ambient commands cannot publish a logical agent branch.
    if s.run_context.is_none() && s.commit_msg.is_some() && !crate::jj::is_jj_dir(checkout_path) {
        let note = "Note: this checkout is not yours to commit to, so the commands ran but nothing was committed.";
        if !result.is_empty() {
            result.push_str("\n\n");
        }
        result.push_str(note);
    }

    if let Some(note) = environment_rebuild_note(orch, s) {
        if !result.is_empty() {
            result.push_str("\n\n");
        }
        result.push_str(&note);
    }
    if let Some(note) = &residence_note {
        if !result.is_empty() {
            result.push_str("\n\n");
        }
        result.push_str(note);
    }
    if !s.cd_advisory.is_empty() {
        if !result.is_empty() {
            result.push_str("\n\n");
        }
        result.push_str(&s.cd_advisory);
    }
    if let Some(tip) = s.interpreter_tip {
        if !result.is_empty() {
            result.push_str("\n\n");
        }
        result.push_str(tip);
    }

    let text = if result.is_empty() {
        "(no output)".to_string()
    } else {
        result
    };
    SettledRunBatch {
        text,
        images: collect_run_images(outcomes),
        suspended: false,
    }
}

/// The Cairn environment a placed process carries to whatever machine runs it.
///
/// A run batch is dispatched to a build cell, and that cell may be on this
/// machine or on an enrolled remote, so only facts that stay true wherever it
/// lands belong here. A resident terminal is placed the same way and states the
/// same identity through this function (see the terminal handler's
/// `terminal_process_env`); everything about the machine — `HOME`, `USER`,
/// `PATH`, `CAIRN_HOME`, XDG and toolchain variables — is the executor's own
/// environment, which it layers this vector over.
///
/// `CAIRN_RUN_ID` is the run whose authority the batch's commands act with. An
/// agent shell is expected to reach `cairn read|write|check run …` with no
/// setup, and the runner authenticates those calls against this run; without it
/// every in-batch CLI invocation is anonymous and every run-scoped tool refuses
/// it. [`process::build_agent_spawn_config`] states the same fact for a batch
/// the host spawns itself — placement must not be what decides whether an agent
/// shell knows who it is.
///
/// `CAIRN_HOME_URI` is the informational canonical job home, resolved by the
/// server. Clients still forward `cairn:~` unchanged.
///
/// `CAIRN_WORKTREE_BRANCH` is the branch a detached cell checkout cannot name
/// for itself, which branch-keyed tooling (`scripts/resolve-branch.ts`) reads.
///
/// `CAIRN_BATCH_EXECUTOR` is the batch's own placement pin, its machine name or
/// platform as JSON, so that a `cairn check run` (or an SDK `checkRun`) inside a
/// batch pinned `executor:{os:"linux"}` asks for a Linux verdict without being
/// told twice. Only `name` and `os` are inherited: what a check needs from a
/// machine is its configuration's to declare, never the batch's. A batch the
/// host spawns itself never carries a pin (the run tool places every pinned
/// process batch), so [`process::build_agent_spawn_config`] has nothing to
/// state here.
///
/// Transport is deliberately absent. The callback URL and the MCP secret
/// describe the machine the runner runs on, not the batch, and the CLI resolves
/// both from the environment it actually executes in. On a colocated executor
/// that resolves to this runner and the shell authenticates with nothing
/// plumbed through; on a remote executor nothing resolves and the CLI reports an
/// unreachable runner, which is true, rather than a loopback address naming a
/// different machine's runner.
pub(crate) fn placed_process_env(
    run_id: Option<&str>,
    home_uri: Option<&str>,
    branch_target_rev: Option<&str>,
    managed_workspace_branch: Option<&str>,
    batch_executor: Option<&cairn_common::executor_protocol::ExecutorSelector>,
) -> Vec<(String, String)> {
    let mut env = Vec::new();
    if let Some(run_id) = run_id.filter(|run_id| !run_id.is_empty()) {
        env.push(("CAIRN_RUN_ID".to_string(), run_id.to_string()));
        if let Some(home_uri) = home_uri {
            env.push(("CAIRN_HOME_URI".to_string(), home_uri.to_string()));
        }
    }
    if let Some(branch) = branch_target_rev.or(managed_workspace_branch) {
        env.push(("CAIRN_WORKTREE_BRANCH".to_string(), branch.to_string()));
    }
    if let Some(pin) = batch_executor.and_then(|selector| selector.placement_pin()) {
        env.push((
            cairn_common::executor_protocol::BATCH_EXECUTOR_ENV.to_string(),
            serde_json::to_string(&pin).expect("an executor selector serializes"),
        ));
    }
    env
}

/// The canonical home URI of a run's job, resolved through the database that
/// run routes to.
///
/// It is informational wherever it is stated (`CAIRN_HOME_URI` on a batch, a
/// terminal, or a REPL), so a failed lookup is logged and yields `None` rather
/// than refusing the spawn.
pub(crate) async fn job_home_uri(
    orch: &Orchestrator,
    run_id: &str,
    job_id: &str,
) -> Option<String> {
    let resolved = async {
        let db = crate::execution::routing::routing_db_for_id(&orch.db, run_id)
            .await
            .map_err(|error| error.to_string())?;
        crate::jobs::queries::home_uri_for_job(&db, job_id)
            .await
            .map_err(|error| error.to_string())
    }
    .await;
    resolved.unwrap_or_else(|error: String| {
        tracing::warn!(%job_id, %error, "could not resolve a job's home URI");
        None
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::CellCommandClass;

    #[test]
    fn a_suspended_item_controls_the_whole_batch_before_ordinary_tail_results() {
        let ordinary = ItemOutcome::failed("first".to_string(), "ordinary failure");
        let suspended = ItemOutcome {
            header: "permission".to_string(),
            body: "executor transport text must not escape".to_string(),
            succeeded: false,
            suspended: true,
            images: Vec::new(),
            tracked_modifications: None,
        };
        let tail = ItemOutcome::failed("tail".to_string(), "later failure");

        assert!(batch_is_suspended(&[ordinary, suspended, tail]));
    }

    fn local_home() -> HomeExecutor {
        HomeExecutor {
            executor_id: "colocated".into(),
        }
    }

    #[test]
    fn only_home_batches_receive_the_home_pin() {
        assert_eq!(home_bound_pin(None), None);
        assert_eq!(
            home_bound_pin(Some(&local_home())),
            Some("colocated".to_string())
        );
    }

    /// The note says which way the machine was lost, because the two send an
    /// agent looking in different places, and it names what was running there
    /// rather than a generic "terminals or REPLs".
    #[test]
    fn the_rebuild_note_names_the_cause_and_the_processes_lost() {
        let silent = crate::fleet::EnvironmentEviction {
            executor: "bbs".into(),
            cause: crate::fleet::EvictionCause::Silent {
                silent_ms: 303_000,
                generation: 1,
            },
            lost_processes: vec!["terminal `dev`".into()],
        };
        let note = environment_rebuild_text(&silent);
        assert!(
            note.contains("(**bbs**) stopped answering for 5m03s while still connected"),
            "{note}"
        );
        assert!(
            note.contains("and terminal `dev` did not survive"),
            "{note}"
        );

        let gone = crate::fleet::EnvironmentEviction {
            executor: "bbs".into(),
            cause: crate::fleet::EvictionCause::Disconnected { absent_ms: 300_000 },
            lost_processes: Vec::new(),
        };
        let note = environment_rebuild_text(&gone);
        assert!(note.contains("disconnected 5m00s ago"), "{note}");
        assert!(
            note.contains("terminals or REPLs from earlier batches did not survive"),
            "{note}"
        );
    }

    /// Every executed batch names its machine, selector or not; a detached one
    /// also names the environment it left, and a home it never left is not
    /// called a detachment.
    #[test]
    fn the_ran_on_line_names_the_machine_and_any_environment_left() {
        assert_eq!(
            ran_on_text("bgsrv-ub", Some("linux"), None),
            "Ran on **bgsrv-ub** (linux)."
        );
        assert_eq!(
            ran_on_text("bglab-ub", Some("linux"), Some("bgsrv-mac")),
            "Ran on **bglab-ub** (linux), detached from the job's environment on **bgsrv-mac**."
        );
        assert_eq!(
            ran_on_text("bgsrv-mac", None, Some("bgsrv-mac")),
            "Ran on **bgsrv-mac**."
        );
    }

    /// A batch that executed and then failed still says where; one that never
    /// ran says nothing ran and names no machine as though it had.
    #[test]
    fn only_an_executed_outcome_names_the_machine_it_ran_on() {
        let notes = PlacementNotes {
            ran_on: Some("Ran on **bgsrv-ub** (linux).".into()),
            ..PlacementNotes::default()
        };
        let failed = settled_failure_text(
            &CellOutcome::FailedAfterExecution {
                request_id: "r".into(),
                attempt_id: "a".into(),
                diagnostic: "executor process was killed".into(),
            },
            None,
            false,
            None,
            &notes,
        )
        .unwrap();
        assert!(failed.contains("Ran on **bgsrv-ub** (linux)."), "{failed}");
        let refused = settled_failure_text(
            &CellOutcome::Unavailable {
                reason: cairn_common::executor_protocol::CellUnavailableReason::NoMatchingExecutor,
                diagnostic: "no executor named fedora is enrolled".into(),
            },
            None,
            false,
            None,
            &notes,
        )
        .unwrap();
        assert!(refused.contains("No commands ran."), "{refused}");
        assert!(!refused.contains("Ran on"), "{refused}");
    }

    fn named(name: &str) -> ExecutorSelector {
        ExecutorSelector {
            name: Some(name.into()),
            ..ExecutorSelector::default()
        }
    }

    /// The environment on `bglab-mac`, which a selector for `local` does not
    /// match, with whatever is running in its tree.
    fn home_elsewhere(resident_processes: &[&str]) -> HomeFacts {
        HomeFacts {
            executor_name: "bglab-mac".into(),
            satisfies_selector: Some(false),
            resident_processes: resident_processes.iter().map(|p| p.to_string()).collect(),
            cannot_hold: None,
        }
    }

    /// Review case 1: terminal `dev` is on bglab-mac, whose link has dropped with
    /// the process alive. The prediction is the recorded machine (see
    /// `fleet::tests::an_absent_home_is_predicted_where_its_route_says_not_on_the_colocated_machine`),
    /// which a selector naming bglab-mac satisfies by its enrollment, so a
    /// committing batch asking for the terminal's own machine stays home and
    /// waits for the link instead of being refused as if the terminal were on
    /// `local`.
    #[test]
    fn a_committing_batch_naming_the_terminals_absent_machine_is_not_refused() {
        let home = HomeFacts {
            satisfies_selector: Some(true),
            ..home_elsewhere(&["terminal `dev`"])
        };
        assert_eq!(
            BatchResidence::decide(Some(&named("bglab-mac")), Some(&home), true),
            Ok(BatchResidence::Home)
        );
    }

    /// Review case 2: the same absent home, and a verdict batch for `local`. The
    /// recorded machine's enrollment refuses the selector, so the batch is
    /// Detached before placement, which gives it no environment to acquire: it
    /// never waits on bglab-mac's reconnect grace.
    #[test]
    fn a_verdict_batch_for_local_never_waits_on_an_absent_remote_home() {
        let home = home_elsewhere(&["terminal `dev`"]);
        assert_eq!(
            BatchResidence::decide(Some(&named("local")), Some(&home), false),
            Ok(BatchResidence::Detached)
        );
    }

    /// An absent home asked for something only its live advertisement can say:
    /// a verdict batch goes where it asked, and a committing batch beside a live
    /// process stays home so acquisition can answer, never a refusal on a guess.
    #[test]
    fn uncertainty_about_an_absent_home_is_never_a_refusal() {
        let unknown = HomeFacts {
            satisfies_selector: None,
            ..home_elsewhere(&["terminal `dev`"])
        };
        let toolchains = ExecutorSelector {
            required_toolchains: vec!["rust".into()],
            ..ExecutorSelector::default()
        };
        assert_eq!(
            BatchResidence::decide(Some(&toolchains), Some(&unknown), false),
            Ok(BatchResidence::Detached)
        );
        assert_eq!(
            BatchResidence::decide(Some(&toolchains), Some(&unknown), true),
            Ok(BatchResidence::Home)
        );
    }

    /// cairn/4712 (a): a live terminal on the home's machine does not bind a
    /// batch that discards its edits. The cairn/4687 builder sat blocked four
    /// hours on exactly this after opening a node terminal.
    #[test]
    fn a_verdict_only_batch_goes_where_it_asked_despite_a_live_terminal() {
        let home = home_elsewhere(&["terminal `dev`"]);
        assert_eq!(
            BatchResidence::decide(Some(&named("local")), Some(&home), false),
            Ok(BatchResidence::Detached)
        );
    }

    /// cairn/4712 (b): a committing batch shares write authority with the live
    /// process, so it is refused, and the refusal names the process, both
    /// machines, and both ways out -- never an "execution home".
    #[test]
    fn a_committing_batch_is_refused_naming_the_live_process_and_the_hatch() {
        let home = home_elsewhere(&["terminal `dev`"]);
        let refusal = BatchResidence::decide(Some(&named("local")), Some(&home), true)
            .expect_err("a commit elsewhere would strand the terminal's tree");
        assert!(refusal.contains("terminal `dev` is running"), "{refusal}");
        assert!(refusal.contains("bglab-mac"), "{refusal}");
        assert!(refusal.contains("local"), "{refusal}");
        assert!(refusal.contains("Drop `commit_msg`"), "{refusal}");
        assert!(refusal.contains("drop `executor`"), "{refusal}");
        assert!(!refusal.contains("execution home"), "{refusal}");

        let both = home_elsewhere(&["terminal `dev`", "REPL `py`"]);
        let refusal = BatchResidence::decide(Some(&named("local")), Some(&both), true).unwrap_err();
        assert!(
            refusal.contains("terminal `dev`, REPL `py` are running"),
            "{refusal}"
        );
    }

    /// 3538's committing detached path survives: with nothing live in the tree
    /// there is nothing to strand, so an explicit destination is honored.
    #[test]
    fn a_committing_batch_detaches_when_nothing_lives_in_the_tree() {
        assert_eq!(
            BatchResidence::decide(Some(&named("local")), Some(&home_elsewhere(&[])), true),
            Ok(BatchResidence::Detached)
        );
    }

    /// cairn/4712 (c) and the thread specimen: the decision made before
    /// acquisition predicted a home the selector matched, and acquisition then
    /// rejoined the environment on another machine that had just reconnected.
    /// Deciding again from the acquired machine sends the batch where it asked,
    /// where the old code pinned it to the acquired machine and refused.
    #[test]
    fn a_home_that_moved_after_the_prediction_cannot_capture_the_batch() {
        let selector = named("local");
        let predicted = HomeFacts {
            executor_name: "local".into(),
            satisfies_selector: Some(true),
            resident_processes: Vec::new(),
            cannot_hold: None,
        };
        assert_eq!(
            BatchResidence::decide(Some(&selector), Some(&predicted), false),
            Ok(BatchResidence::Home)
        );
        let acquired = home_elsewhere(&[]);
        let residence = BatchResidence::decide(Some(&selector), Some(&acquired), false).unwrap();
        assert_eq!(residence, BatchResidence::Detached);

        let placement = test_placement(false);
        let home = HomeExecutor {
            executor_id: "bglab-mac".into(),
        };
        let request = placement.attempt(Some(&home), true, residence);
        assert_eq!(
            request.pinned_executor_id, None,
            "a detached batch is never pinned"
        );
        assert_eq!(request.mutation_policy, MutationPolicy::PureVerdict);
    }

    #[test]
    fn no_selector_or_a_matching_home_keeps_the_batch_home() {
        let home = home_elsewhere(&["terminal `dev`"]);
        assert_eq!(
            BatchResidence::decide(None, Some(&home), true),
            Ok(BatchResidence::Home)
        );
        assert_eq!(
            BatchResidence::decide(Some(&ExecutorSelector::default()), Some(&home), true),
            Ok(BatchResidence::Home)
        );
        let matching = HomeFacts {
            satisfies_selector: Some(true),
            ..home_elsewhere(&["terminal `dev`"])
        };
        assert_eq!(
            BatchResidence::decide(Some(&named("bglab-mac")), Some(&matching), true),
            Ok(BatchResidence::Home)
        );
        // No machine known to hold the environment: nothing to share.
        assert_eq!(
            BatchResidence::decide(Some(&named("local")), None, true),
            Ok(BatchResidence::Detached)
        );
    }

    /// cairn/4931 specimen B: bgsrv-ub, which holds the environment, can never
    /// hold this cargo test, and bglab-mac can. No selector is involved.
    fn home_too_small(resident_processes: &[&str]) -> HomeFacts {
        HomeFacts {
            executor_name: "bgsrv-ub".into(),
            satisfies_selector: Some(true),
            resident_processes: resident_processes.iter().map(|p| p.to_string()).collect(),
            cannot_hold: Some(CannotHold {
                ceiling: cairn_common::executor_protocol::MemoryCeiling {
                    estimate_bytes: 10_307_921_510,
                    ceiling_bytes: 8_282_086_656,
                },
                alternative_executor_id: "exec-bglab-mac".into(),
                alternative_executor_name: "bglab-mac".into(),
            }),
        }
    }

    /// A home that cannot hold the batch is a reason to leave exactly as a
    /// selector it fails is: verdict batches and committing batches with
    /// nothing live in the tree both run where placement says they fit.
    #[test]
    fn a_batch_its_home_cannot_hold_detaches_unless_it_would_strand_a_live_process() {
        assert_eq!(
            BatchResidence::decide(None, Some(&home_too_small(&["terminal `dev`"])), false),
            Ok(BatchResidence::Detached)
        );
        assert_eq!(
            BatchResidence::decide(None, Some(&home_too_small(&[])), true),
            Ok(BatchResidence::Detached)
        );
    }

    /// The one binding constraint is a live process sharing the tree, and the
    /// refusal names it and the numbers, never an "execution home".
    #[test]
    fn a_committing_batch_its_home_cannot_hold_beside_a_live_process_is_refused_by_name() {
        let refusal =
            BatchResidence::decide(None, Some(&home_too_small(&["terminal `dev`"])), true)
                .expect_err("a commit elsewhere would strand the terminal's tree");
        assert!(refusal.contains("terminal `dev` is running"), "{refusal}");
        assert!(refusal.contains("bgsrv-ub"), "{refusal}");
        assert!(refusal.contains("9.6 GiB"), "{refusal}");
        assert!(refusal.contains("7.7 GiB"), "{refusal}");
        assert!(refusal.contains("bglab-mac"), "{refusal}");
        assert!(refusal.contains("drop `commit_msg`"), "{refusal}");
        assert!(!refusal.contains("execution home"), "{refusal}");
    }

    fn oversized_notes(detached: bool) -> PlacementNotes {
        PlacementNotes {
            ran_on: Some("Ran on **bglab-mac** (macos).".to_string()),
            ran_elsewhere: detached.then(|| "It ran on **bglab-mac** because this batch's measured memory (~9.6 GiB) exceeds what bgsrv-ub can ever hold (7.7 GiB). Your execution environment there is unchanged.".to_string()),
            above_ceiling: Some("Note: no executor can hold this batch's measured ~9.6 GiB (largest ceiling 7.7 GiB on bgsrv-ub); it was placed on **bgsrv-ub** at the clamp.".into()),
        }
    }

    /// Running above the ceiling is never silent, least of all when the batch
    /// then dies: that is the out-of-memory path the note exists to explain.
    #[test]
    fn a_claimed_oversized_batch_that_fails_after_execution_still_says_it_was_oversized() {
        let text = settled_failure_text(
            &CellOutcome::FailedAfterExecution {
                request_id: "r".into(),
                attempt_id: "a".into(),
                diagnostic: "executor process was killed".into(),
            },
            None,
            false,
            None,
            &oversized_notes(false),
        )
        .unwrap();
        assert!(text.contains("executor process was killed"), "{text}");
        assert!(
            text.contains("no executor can hold this batch's measured ~9.6 GiB"),
            "{text}"
        );
    }

    /// A cancelled batch was placed and claimed, so the ceiling note is still
    /// true of it; whether it ran is not known, so "Ran on" is not said.
    #[test]
    fn a_cancelled_oversized_batch_keeps_the_ceiling_note_but_never_claims_it_ran() {
        let text = settled_failure_text(
            &CellOutcome::Cancelled {
                request_id: "r".into(),
                attempt_id: "a".into(),
            },
            None,
            false,
            None,
            &oversized_notes(true),
        )
        .unwrap();
        assert!(
            text.contains("placed on **bgsrv-ub** at the clamp"),
            "{text}"
        );
        assert!(!text.contains("Ran on"), "{text}");

        let storage = settled_failure_text(
            &CellOutcome::StorageFailure {
                request_id: "r".into(),
                attempt_id: "a".into(),
                stage: cairn_common::executor_protocol::StorageFailureStage::ProvisioningCheckout,
                kind: cairn_common::executor_protocol::StorageFailureKind::NoSpace,
                diagnostic: "checkout failed".into(),
                slot_retired: false,
            },
            None,
            false,
            None,
            &oversized_notes(true),
        )
        .unwrap();
        assert!(!storage.contains("Ran on"), "{storage}");
        assert!(storage.contains("at the clamp"), "{storage}");
    }

    /// A completed batch settles through publication with both notes.
    #[test]
    fn a_completed_batch_carries_every_placement_note() {
        let executed = oversized_notes(true).executed().unwrap();
        assert!(
            executed.starts_with("It ran on **bglab-mac** because"),
            "{executed}"
        );
        assert!(executed.contains("at the clamp"), "{executed}");
        assert_eq!(PlacementNotes::default().executed(), None);
    }

    #[test]
    fn a_batch_that_ran_elsewhere_for_memory_says_why_and_that_the_home_is_intact() {
        let note = home_too_small(&[])
            .cannot_hold
            .unwrap()
            .ran_elsewhere_note("bgsrv-ub");
        assert_eq!(
            note,
            "It ran on **bglab-mac** because this batch's measured memory (~9.6 GiB) exceeds what bgsrv-ub can ever hold (7.7 GiB). Your execution environment there is unchanged."
        );
    }

    #[test]
    fn a_home_attempt_is_pinned_and_keeps_its_commit() {
        let home = HomeExecutor {
            executor_id: "bglab-mac".into(),
        };
        let committing = test_placement(true);
        let request = committing.attempt(Some(&home), true, BatchResidence::Home);
        assert_eq!(request.pinned_executor_id.as_deref(), Some("bglab-mac"));
        assert_eq!(request.mutation_policy, MutationPolicy::AllowDelta);
        let detached = committing.attempt(Some(&home), true, BatchResidence::Detached);
        assert_eq!(detached.pinned_executor_id, None);
        assert_eq!(detached.mutation_policy, MutationPolicy::AllowDelta);
    }

    /// cairn/5360's acceptance, as the run path decides it: the job's
    /// environment is on bginf-halo and a batch names fedora. It goes to fedora
    /// (unpinned and carrying fedora's selector, so placement can choose nothing
    /// else), or it is refused naming bginf-halo and the live process. It is
    /// never pinned to bginf-halo while asking for fedora, except in the one
    /// uncertain case, where it still carries the selector and placement refuses
    /// the mismatch (`fleet::tests::a_batch_naming_another_machine_than_its_environment_runs_there_or_nowhere`).
    #[test]
    fn a_batch_pinning_b_beside_an_environment_on_a_runs_on_b_or_is_refused_naming_a() {
        let fedora = named("fedora");
        let on_halo = |live: &[&str]| HomeFacts {
            executor_name: "bginf-halo".into(),
            satisfies_selector: Some(false),
            resident_processes: live.iter().map(|p| p.to_string()).collect(),
            cannot_hold: None,
        };
        let halo = HomeExecutor {
            executor_id: "bginf-halo".into(),
        };
        let placement = |commit_present: bool| {
            let mut placement = test_placement(commit_present);
            placement.template.executor = Some(fedora.clone());
            placement
        };

        // A verdict-only batch goes where it asked, even beside a live terminal.
        let residence =
            BatchResidence::decide(Some(&fedora), Some(&on_halo(&["terminal `lab`"])), false)
                .unwrap();
        assert_eq!(residence, BatchResidence::Detached);
        let request = placement(false).attempt(Some(&halo), true, residence);
        assert_eq!(request.pinned_executor_id, None);
        assert_eq!(request.executor.as_ref(), Some(&fedora));
        assert_eq!(request.mutation_policy, MutationPolicy::PureVerdict);

        // A committing batch with nothing live in the environment goes too.
        let residence = BatchResidence::decide(Some(&fedora), Some(&on_halo(&[])), true).unwrap();
        assert_eq!(residence, BatchResidence::Detached);
        let request = placement(true).attempt(Some(&halo), true, residence);
        assert_eq!(request.pinned_executor_id, None);
        assert_eq!(request.executor.as_ref(), Some(&fedora));

        // A committing batch beside a live terminal is refused naming both.
        let refusal =
            BatchResidence::decide(Some(&fedora), Some(&on_halo(&["terminal `lab`"])), true)
                .unwrap_err();
        assert!(
            refusal.contains("terminal `lab` is running in this job's working tree on bginf-halo"),
            "{refusal}"
        );
        assert!(refusal.contains("fedora"), "{refusal}");

        // Uncertain: the batch stays home, but its attempt keeps the selector.
        let unknown = HomeFacts {
            satisfies_selector: None,
            ..on_halo(&["terminal `lab`"])
        };
        let residence = BatchResidence::decide(Some(&fedora), Some(&unknown), true).unwrap();
        assert_eq!(residence, BatchResidence::Home);
        let request = placement(true).attempt(Some(&halo), true, residence);
        assert_eq!(request.pinned_executor_id.as_deref(), Some("bginf-halo"));
        assert_eq!(request.executor.as_ref(), Some(&fedora));
    }

    fn test_placement(commit_present: bool) -> BatchPlacement {
        let mut template = identity_request("project", "project");
        template.mutation_policy = MutationPolicy::AllowDelta;
        template.executor = Some(named("local"));
        BatchPlacement {
            template,
            environment: None,
            residence: BatchResidence::Home,
            commit_present,
            capacity_wait_budget: None,
            ceiling_horizon_unix_ms: u64::MAX,
        }
    }

    fn shell(timeout: Option<u32>) -> (String, Result<RunSpec, String>) {
        (
            "shell".into(),
            Ok(RunSpec::Shell {
                command: "true".into(),
                args: Vec::new(),
                timeout,
            }),
        )
    }

    fn script(timeout: Option<u32>) -> (String, Result<RunSpec, String>) {
        (
            "script".into(),
            Ok(RunSpec::Script {
                program: "true".into(),
                args: Vec::new(),
                timeout,
                stdin: None,
            }),
        )
    }

    fn process_timeout(spec: &(String, Result<RunSpec, String>)) -> Option<u32> {
        match &spec.1 {
            Ok(RunSpec::Shell { timeout, .. } | RunSpec::Script { timeout, .. }) => *timeout,
            _ => None,
        }
    }

    /// The single clamp, stated as the contract it enforces: an omitted bound
    /// means "run to completion", an explicit bound is honored, and only the
    /// batch ceiling shortens anything. A second, smaller bound underneath the
    /// ceiling is exactly the defect this replaces.
    #[test]
    fn an_omitted_timeout_runs_to_completion_and_only_the_ceiling_clamps() {
        assert_eq!(clamp_run_item_timeout_ms(None), MAX_RUN_ITEM_TIMEOUT_MS);
        // The reported failure: a suite asking for an hour must get an hour.
        assert_eq!(clamp_run_item_timeout_ms(Some(3_600_000)), 3_600_000);
        assert_eq!(clamp_run_item_timeout_ms(Some(1_800)), 1_800);
        assert_eq!(
            clamp_run_item_timeout_ms(Some(u32::MAX)),
            MAX_RUN_ITEM_TIMEOUT_MS
        );
        // The item bound and the batch ceiling are one number, so no layer can
        // kill an item before the batch's own loud guard would.
        assert_eq!(
            std::time::Duration::from_millis(u64::from(MAX_RUN_ITEM_TIMEOUT_MS)),
            RUN_BATCH_CEILING
        );
    }

    /// Every process item leaves the resolver carrying a settled bound, so no
    /// layer below re-derives one. An omitted bound becomes the ceiling; a
    /// fleet-configured default never appears.
    #[test]
    fn run_item_timeouts_settle_once_and_never_undercut_an_explicit_bound() {
        let mut resolved = vec![
            shell(None),
            script(None),
            shell(Some(1_800)),
            shell(Some(3_600_000)),
        ];

        apply_run_item_timeouts(&mut resolved);

        assert_eq!(process_timeout(&resolved[0]), Some(MAX_RUN_ITEM_TIMEOUT_MS));
        assert_eq!(process_timeout(&resolved[1]), Some(MAX_RUN_ITEM_TIMEOUT_MS));
        assert_eq!(process_timeout(&resolved[2]), Some(1_800));
        assert_eq!(process_timeout(&resolved[3]), Some(3_600_000));
    }

    /// A host-executed item (MCP tool call, REPL send) never enters the routed,
    /// suspendable path, so the ceiling above it is the grace window rather than
    /// the batch ceiling. Advertising the batch ceiling over it would promise a
    /// bound the transport underneath cannot honor: the call would still be
    /// attached when the socket gave up, and the whole batch's output would be
    /// discarded — the original defect, reached by a different door.
    #[test]
    fn a_host_item_is_bounded_by_what_its_transport_can_actually_honor() {
        assert_eq!(
            std::time::Duration::from_millis(u64::from(MAX_HOST_ITEM_TIMEOUT_MS)),
            RUN_GRACE_WINDOW
        );

        // An explicit bound is clamped to what the host path can deliver, so an
        // over-long ask becomes an honest item timeout instead of a transport
        // death that discards every other item's output too.
        assert_eq!(
            clamp_host_item_timeout_ms(Some(u32::MAX)),
            Some(MAX_HOST_ITEM_TIMEOUT_MS)
        );
        assert_eq!(
            clamp_host_item_timeout_ms(Some(3_600_000)),
            Some(MAX_HOST_ITEM_TIMEOUT_MS)
        );
        assert_eq!(clamp_host_item_timeout_ms(Some(5_000)), Some(5_000));

        // `None` is preserved, not filled: "runs to completion" is a promise only
        // the suspendable path can keep, and these items have their own defaults.
        assert_eq!(clamp_host_item_timeout_ms(None), None);

        let mut executor_action = vec![(
            "executor action".into(),
            Ok(RunSpec::ExecutorAction(Box::new(
                super::types::ExecutorActionSpec {
                    executor_name: "local".into(),
                    action: "look".into(),
                    args: serde_json::json!({}),
                    timeout: Some(u32::MAX),
                },
            ))),
        )];
        apply_run_item_timeouts(&mut executor_action);
        assert!(matches!(
            &executor_action[0].1,
            Ok(RunSpec::ExecutorAction(spec))
                if spec.timeout == Some(MAX_HOST_ITEM_TIMEOUT_MS)
        ));

        // A response or decide item keeps its ask through the seam, so its
        // timeout can name both the ask and the cap that overrode it.
        let mut resolved = vec![(
            "response".into(),
            Ok(RunSpec::ResponseCall {
                response_id: "test".into(),
                project: None,
                args: serde_json::json!({}),
                timeout: Some(310_000),
            }),
        )];
        apply_run_item_timeouts(&mut resolved);
        assert!(matches!(
            &resolved[0].1,
            Ok(RunSpec::ResponseCall {
                timeout: Some(310_000),
                ..
            })
        ));
    }

    /// A model call's bound is the cap whenever the cap is what holds it, and
    /// then carries the ask it overrode; an ask under the cap is its own bound.
    #[test]
    fn a_model_items_bound_records_whether_the_cap_or_the_caller_set_it() {
        use std::time::Duration;
        let cap = Duration::from_millis(u64::from(MAX_HOST_ITEM_TIMEOUT_MS));

        let over = host_item_bound(Some(310_000));
        assert_eq!(over.limit, cap);
        let capped = over.cap.as_ref().expect("the cap decided it");
        assert_eq!(capped.requested, Some(Duration::from_secs(310)));
        assert_eq!(
            over.describe(Some(Duration::from_secs(300))),
            format!(
                "timed out at the 120 s host-item cap (requested 310 s; the definition declares 300 s). {HOST_ITEM_CAP_WHY}"
            )
        );
        assert!(HOST_ITEM_CAP_WHY.contains("@cairn/sdk"));

        let unasked = host_item_bound(None);
        assert_eq!(unasked.limit, cap);
        assert!(unasked
            .describe(None)
            .starts_with("timed out at the 120 s host-item cap (no timeout was requested)."));

        let under = host_item_bound(Some(300));
        assert_eq!(under.limit, Duration::from_millis(300));
        assert_eq!(under.cap, None);
        assert_eq!(
            under.describe(Some(Duration::from_secs(15))),
            "timed out at the 0.3 s bound its caller requested (the definition declares 15 s)."
        );
    }

    /// `CellRequest.timeout_ms` is the request's own bound. Sequential items add
    /// up, parallel items overlap, and neither may exceed the ceiling the host
    /// enforces — a request claiming more time than the batch can legally take
    /// is the same class of disagreement that lost the original suite's output.
    fn payload(value: serde_json::Value) -> RunPayload {
        serde_json::from_value(value).expect("a run payload")
    }

    #[test]
    fn a_declared_budget_counts_from_when_the_batch_starts_waiting_for_room() {
        let mut placement = test_placement(false);
        placement.capacity_wait_budget = Some(std::time::Duration::from_secs(1));
        placement.ceiling_horizon_unix_ms = batch_ceiling_horizon_unix_ms();
        placement.template.wait_horizon_unix_ms = placement.ceiling_horizon_unix_ms;
        // Provisioning time passes before the home is in hand. None of it may
        // come out of the declared second.
        std::thread::sleep(std::time::Duration::from_millis(50));
        let before = crate::fleet::unix_time_ms();
        let wait = placement.begin_waiting_for_room();
        let after = crate::fleet::unix_time_ms();
        let horizon = placement.template.wait_horizon_unix_ms;
        assert!(
            (before + 1_000..=after + 1_000).contains(&horizon),
            "horizon {horizon} not one second from [{before}, {after}]"
        );
        assert_eq!(wait.budget, Some(std::time::Duration::from_secs(1)));
        // Every later presentation carries the same number.
        let home = HomeExecutor {
            executor_id: "local".into(),
        };
        let attempt = placement.attempt(Some(&home), true, BatchResidence::Home);
        assert_eq!(attempt.wait_horizon_unix_ms, horizon);
    }

    #[test]
    fn no_horizon_derived_for_a_batch_reaches_past_its_ceiling() {
        let now = crate::fleet::unix_time_ms();
        let ceiling = now + 500;
        assert_eq!(
            room_wait_horizon_unix_ms(Some(std::time::Duration::from_secs(60)), ceiling),
            ceiling
        );
        assert_eq!(room_wait_horizon_unix_ms(None, ceiling), ceiling);
        let declared =
            room_wait_horizon_unix_ms(Some(std::time::Duration::from_millis(1)), now + 60_000);
        assert!(declared < now + 60_000, "{declared}");
    }

    /// Waiting for room is bounded by what the agent itself declared, and only
    /// by that. "Run to completion" is a willingness to wait out a busy machine,
    /// so a batch that bounded nothing is held to the ceiling alone; a batch
    /// that bounded every item said what it is worth, and queueing past that
    /// would keep the letter of the bound and none of its meaning.
    #[test]
    fn only_a_fully_declared_batch_bounds_its_own_wait_for_room() {
        assert_eq!(
            declared_capacity_wait_budget(
                &payload(serde_json::json!({"commands":[{"command":"bun test"}]})),
                false
            ),
            None
        );
        // One unbounded item makes the batch unbounded.
        assert_eq!(
            declared_capacity_wait_budget(
                &payload(serde_json::json!({"commands":[
                    {"command":"a","timeout":1000},
                    {"command":"b"}
                ]})),
                false
            ),
            None
        );
        // Parallel items overlap, so the batch is worth the longest of them.
        assert_eq!(
            declared_capacity_wait_budget(
                &payload(serde_json::json!({"commands":[
                    {"command":"a","timeout":1000},
                    {"command":"b","timeout":4000}
                ]})),
                false
            ),
            Some(std::time::Duration::from_millis(4000))
        );
        // Sequential items run one after another, so the batch is worth both.
        assert_eq!(
            declared_capacity_wait_budget(
                &payload(serde_json::json!({"commands":[
                    {"command":"a","timeout":1000},
                    {"command":"b","timeout":4000}
                ]})),
                true
            ),
            Some(std::time::Duration::from_millis(5000))
        );
    }

    #[test]
    fn the_cell_request_budget_reflects_the_batch_not_a_fleet_default() {
        assert_eq!(
            batch_execution_budget_ms(&[shell(Some(3_000)), shell(Some(4_000))], true),
            7_000
        );
        assert_eq!(
            batch_execution_budget_ms(&[shell(Some(3_000)), shell(Some(4_000))], false),
            4_000
        );
        assert_eq!(
            batch_execution_budget_ms(&[shell(None)], false),
            MAX_RUN_ITEM_TIMEOUT_MS
        );
        // Three no-timeout items sequentially would be eighteen hours; the host
        // kills the batch at six, so the request may not claim more.
        assert_eq!(
            batch_execution_budget_ms(&[shell(None), shell(None), shell(None)], true),
            MAX_RUN_ITEM_TIMEOUT_MS
        );
    }

    /// The ceiling exists so a command that never exits fails loudly instead of
    /// parking its agent forever. Its failure must classify as a run failure (an
    /// action run reads outcomes by text) and must name the remedy, since the
    /// only batch that reaches six hours is one that should have been a terminal.
    #[test]
    fn the_batch_ceiling_fails_loudly_and_names_the_remedy() {
        let text = RunFailure::Cancelled(Some(RUN_CEILING_DETAIL.to_string())).text();

        assert!(envelope_reports_run_failure(&text), "{text}");
        assert!(text.contains("6-hour"), "{text}");
        assert!(text.contains("cairn:~/terminal/"), "{text}");
        // Far above any legitimate suite, and strictly below the transport
        // ceilings above it (a 6-day callback, a 7-day agent tool timeout), so a
        // wedged batch is always the layer that gives up first.
        assert_eq!(RUN_BATCH_CEILING, std::time::Duration::from_secs(21_600));
        assert!(RUN_BATCH_CEILING > RUN_GRACE_WINDOW);
        assert!(RUN_BATCH_CEILING < std::time::Duration::from_secs(6 * 24 * 60 * 60));
    }

    /// A suspended batch whose host restarts under it cannot be re-driven, so it
    /// resolves to a failure that says so rather than leaving the agent parked
    /// forever on a result that will never arrive.
    #[test]
    fn a_batch_lost_to_a_restart_says_so_and_says_nothing_landed() {
        for commits in [true, false] {
            let text = run_batch_lost_to_restart_text(commits);
            assert!(envelope_reports_run_failure(&text), "{text}");
            assert!(text.contains("restarted"), "{text}");
            assert!(
                text.to_lowercase().contains("nothing was committed"),
                "{text}"
            );
            crate::system_prompt::assert_no_substrate_vocabulary("host-restart failure", &text);
        }
    }

    /// The conflict-marker escape is normalized at the REQUEST boundary, not in
    /// the barrier, so the barrier only ever sees an authorization that carries
    /// a real reason. An empty or whitespace-only string is no reason at all: a
    /// silent default-deny bypass with an empty audit trail is exactly the
    /// boolean-in-string's-clothing the design ruled out.
    #[test]
    fn a_blank_marker_reason_never_becomes_an_authorization() {
        let normalize = |payload: serde_json::Value| -> Option<String> {
            let payload: RunPayload =
                serde_json::from_value(payload).expect("payload deserializes");
            payload
                .conflict_markers_reason
                .as_deref()
                .map(str::trim)
                .filter(|reason| !reason.is_empty())
                .map(ToOwned::to_owned)
        };
        let batch = |reason: serde_json::Value| {
            serde_json::json!({
                "commands": [{ "command": "true" }],
                "commit_msg": "land it",
                "conflict_markers_reason": reason
            })
        };

        for blank in ["", "   ", "\t\n "] {
            assert_eq!(
                normalize(batch(serde_json::json!(blank))),
                None,
                "a blank reason must not authorize a bypass: {blank:?}"
            );
        }
        assert_eq!(normalize(batch(serde_json::Value::Null)), None);
        assert_eq!(
            normalize(batch(serde_json::json!("  documenting marker syntax  "))),
            Some("documenting marker syntax".to_string()),
            "a real reason is accepted and trimmed for the audit line"
        );
    }

    /// The grace window decides the shape of the call and nothing else, so a
    /// dev/test override must move it without touching any item's kill bound.
    /// The two sides of correlation never share a spelling: the transcript holds
    /// the JSON the model emitted, with every optional it did not use simply
    /// absent, while the callback payload arrives re-serialized by cairn-cmd,
    /// which writes those optionals out as explicit nulls. Identity has to be the
    /// batch itself, or a suspension would find no call and the whole grace
    /// contract would silently fall back to the inline await it replaced.
    #[test]
    fn a_batch_correlates_with_its_call_across_encodings() {
        let model_emitted = serde_json::json!({
            "commands": [{ "command": "bun run test:rust", "description": "the suite" }],
            "commit_msg": "land it"
        });
        let re_serialized = serde_json::json!({
            "commands": [{
                "command": "bun run test:rust",
                "description": "the suite",
                "timeout": null,
                "target": null,
                "payload": null,
                "code": null,
                "background": null,
                "interpreter": null,
                "repl": null,
                "waitFor": null
            }],
            "sequential": null,
            "stop_on_error": null,
            "commit_msg": "land it",
            "branch": null
        });
        let expected = batch_identity(&re_serialized).expect("the callback payload is a batch");
        for name in ["run", "mcp__cairn__run"] {
            assert!(
                is_this_batchs_call(name, &model_emitted, &expected),
                "{name} carrying this batch must correlate whatever the encoding"
            );
        }
    }

    /// Correlation stays an identity, not a category. Another `run` call in the
    /// same turn is a different call, and answering it would deliver this
    /// batch's result to the wrong tool use.
    #[test]
    fn a_different_batch_in_the_same_turn_is_not_this_call() {
        let expected = batch_identity(&serde_json::json!({
            "commands": [{ "command": "bun run test:rust" }]
        }))
        .expect("the callback payload is a batch");
        for (label, other) in [
            (
                "a different command",
                serde_json::json!({ "commands": [{ "command": "bun run check:rust" }] }),
            ),
            (
                "the same command under a different batch option",
                serde_json::json!({
                    "commands": [{ "command": "bun run test:rust" }],
                    "branch": "main"
                }),
            ),
            (
                "the same command with a second item",
                serde_json::json!({
                    "commands": [
                        { "command": "bun run test:rust" },
                        { "command": "bun run check:rust" }
                    ]
                }),
            ),
            (
                "not a batch at all",
                serde_json::json!({ "paths": ["file:x"] }),
            ),
        ] {
            assert!(
                !is_this_batchs_call("run", &other, &expected),
                "{label} must not correlate"
            );
        }
        assert!(
            !is_this_batchs_call(
                "read",
                &serde_json::json!({ "commands": [{ "command": "bun run test:rust" }] }),
                &expected
            ),
            "only a run call can be a run batch's origin"
        );
    }

    #[test]
    fn the_grace_override_moves_only_the_call_shape() {
        assert_eq!(run_grace_window(), RUN_GRACE_WINDOW);
        std::env::set_var("CAIRN_RUN_GRACE_MS", "25");
        assert_eq!(run_grace_window(), std::time::Duration::from_millis(25));
        std::env::set_var("CAIRN_RUN_GRACE_MS", "  ");
        assert_eq!(run_grace_window(), RUN_GRACE_WINDOW);
        std::env::set_var("CAIRN_RUN_GRACE_MS", "not-a-number");
        assert_eq!(run_grace_window(), RUN_GRACE_WINDOW);
        std::env::remove_var("CAIRN_RUN_GRACE_MS");

        // An item's kill bound is the batch ceiling, never grace: moving the
        // call's shape must not move when any item dies.
        assert_eq!(clamp_run_item_timeout_ms(None), MAX_RUN_ITEM_TIMEOUT_MS);
    }

    #[test]
    fn placed_process_env_covers_managed_and_explicit_branch_routes() {
        assert_eq!(
            placed_process_env(None, None, None, Some("agent/cairn-2929-builder-0"), None),
            [(
                "CAIRN_WORKTREE_BRANCH".into(),
                "agent/cairn-2929-builder-0".into()
            )]
        );
        assert_eq!(
            placed_process_env(None, None, Some("feature/dev-instance"), None, None),
            [(
                "CAIRN_WORKTREE_BRANCH".into(),
                "feature/dev-instance".into()
            )]
        );
    }

    /// An authenticated batch states its run wherever it is placed.
    ///
    /// A `cairn` invocation inside a batch shell authenticates against this and
    /// nothing else, so dropping it does not degrade the shell — it anonymizes
    /// it, and every run-scoped tool refuses an anonymous caller. That is how
    /// `cairn check run <suite>` came to answer "authenticated check_run request
    /// is missing its run ID" from a shell whose run was never in doubt
    /// (CAIRN-3381). The branch route travelling while the identity did not is
    /// the exact shape of the regression, so assert both together.
    #[test]
    fn a_placed_batch_carries_the_run_its_commands_act_with() {
        assert_eq!(
            placed_process_env(
                Some("run-7"),
                Some("cairn://p/cairn/thread"),
                None,
                Some("agent/cairn-3381-builder-0"),
                None
            ),
            [
                ("CAIRN_RUN_ID".into(), "run-7".into()),
                ("CAIRN_HOME_URI".into(), "cairn://p/cairn/thread".into()),
                (
                    "CAIRN_WORKTREE_BRANCH".into(),
                    "agent/cairn-3381-builder-0".into()
                )
            ]
        );
        // An attribution run (`cairn check run rust-tests --branch main`) reads another
        // revision but is still the caller's own run acting on it.
        assert_eq!(
            placed_process_env(
                Some("run-7"),
                Some("cairn://p/cairn/thread"),
                Some("main"),
                None,
                None
            ),
            [
                ("CAIRN_RUN_ID".into(), "run-7".into()),
                ("CAIRN_HOME_URI".into(), "cairn://p/cairn/thread".into()),
                ("CAIRN_WORKTREE_BRANCH".into(), "main".into())
            ]
        );
    }

    /// An unauthenticated batch claims no run rather than an empty one.
    ///
    /// `CAIRN_RUN_ID=""` would authenticate as far as the variable's presence
    /// and then fail deeper, where the diagnosis is worse; the CLI's own
    /// `run_id` is `Option`, so absence is representable and is the honest
    /// answer for a batch nobody owns.
    #[test]
    fn an_unowned_batch_states_no_run_at_all() {
        assert!(placed_process_env(None, None, None, None, None).is_empty());
        assert!(placed_process_env(Some(""), None, None, None, None).is_empty());
    }

    /// A batch pinned to a machine or a platform hands that pin to its shells,
    /// so a check started inside it asks for the same placement. Capability is
    /// never inherited, and a selector naming neither carries nothing.
    #[test]
    fn a_pinned_batch_hands_its_shells_its_name_or_platform_only() {
        use cairn_common::executor_protocol::ExecutorSelector;
        let pinned = |selector: ExecutorSelector| {
            placed_process_env(None, None, None, None, Some(&selector))
                .into_iter()
                .find(|(key, _)| key == "CAIRN_BATCH_EXECUTOR")
                .map(|(_, value)| value)
        };
        assert_eq!(
            pinned(ExecutorSelector {
                os: Some("linux".into()),
                required_toolchains: vec!["rust".into()],
                ..ExecutorSelector::default()
            })
            .as_deref(),
            Some(r#"{"os":"linux"}"#)
        );
        assert_eq!(
            pinned(ExecutorSelector {
                name: Some("fedora".into()),
                ..ExecutorSelector::default()
            })
            .as_deref(),
            Some(r#"{"name":"fedora"}"#)
        );
        assert_eq!(
            pinned(ExecutorSelector {
                required_toolchains: vec!["rust".into()],
                ..ExecutorSelector::default()
            }),
            None
        );
    }

    fn identity_request(project_id: &str, repository_id: &str) -> CellRequest {
        CellRequest {
            request_id: "request".into(),
            attempt_id: "attempt".into(),
            project_id: project_id.into(),
            repository: cairn_common::executor_protocol::RepositoryLocator::ColocatedPath {
                project_id: project_id.into(),
                repository_id: repository_id.into(),
                absolute_path: "/repository".into(),
            },
            base_commit: "base".into(),
            command: "true".into(),
            command_class: CellCommandClass::Other,
            placement_work_class:
                cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::AgentInteractive,
            wait_horizon_unix_ms: 1,
            waiting_since_unix_ms: 0,
            timeout_ms: 1,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            executor: None,
            pinned_executor_id: None,
            placement_mobility: Default::default(),
            verdict_platforms: Vec::new(),
            avoided_executor_ids: Vec::new(),
            project_requirements: Default::default(),
            command_resource_identity: None,
            resource_reservation: Default::default(),
            learned_estimate: None,
        }
    }

    #[test]
    fn publication_identity_keeps_project_and_repository_ids_distinct() {
        let request = identity_request("project", "repository");
        assert!(validate_publication_identity("project", &request).is_ok());
        assert!(validate_publication_identity("other-project", &request).is_err());
    }

    #[test]
    fn described_recognized_check_keeps_display_and_executable_classification_separate() {
        let resolved = vec![(
            "Run Rust suite".to_string(),
            Ok(RunSpec::Shell {
                command: "cargo test --workspace".to_string(),
                args: Vec::new(),
                timeout: None,
            }),
        )];

        let (command, command_class) = build_slot_command_parts(&resolved);

        assert_eq!(command, "Run Rust suite");
        assert_eq!(command_class, CellCommandClass::CargoTest);
    }
}

#[cfg(test)]
mod barrier_lock_scope_tests {
    /// The commit barrier releases the store lock as soon as a routed
    /// publication has landed, been held, or been stranded, and before any arm
    /// finalizes a receipt or pushes: those are network I/O (I6). Pinned in
    /// source order because the barrier lives inside the run handler, which has
    /// no harness of its own.
    #[test]
    fn the_barrier_releases_the_store_lock_before_any_routed_arm_finalizes() {
        let source = include_str!("mod.rs");
        let lock = source
            .find("\"run commit barrier publication\",")
            .expect("the barrier acquires the store lock");
        let landing = lock
            + source[lock..]
                .find("land_prepared_slot_delta(")
                .expect("the barrier lands a prepared delta");
        let release = landing
            + source[landing..]
                .find("let _store_guard = if routed.is_some()")
                .expect("the barrier releases the lock for routed outcomes");
        let arms = release
            + source[release..]
                .find("match routed {")
                .expect("the routed arms follow the release");
        let finalize = landing
            + source[landing..]
                .find("finalize_delta_receipt(")
                .expect("routed arms finalize receipts");
        assert!(lock < landing && landing < release && release < arms && arms < finalize);
    }
}

/// `run cairn://models/decide` (and the response door beside it): resolution,
/// the batch it may sit in, the body it returns, and how a timeout releases the
/// caller and names what bounded it.
#[cfg(test)]
mod decide_door_tests {
    use super::resolve::resolve_run_item;
    use super::types::{RunItem, RunSpec};
    use crate::credentials::CustomProtocol;
    use crate::mcp::types::McpCallbackRequest;
    use crate::orchestrator::decide_fixtures::{
        choosing_b, with_logprobs, workspace_with_server, DecideOutcome,
    };
    use serde_json::{json, Value};
    use std::time::{Duration, Instant};

    fn request(payload: Value) -> McpCallbackRequest {
        McpCallbackRequest {
            thread_id: None,
            cwd: std::env::temp_dir().to_string_lossy().into_owned(),
            run_id: None,
            tool: "run".to_string(),
            tool_use_id: None,
            payload,
        }
    }

    fn decide_item(timeout: Option<u32>) -> Value {
        json!({
            "target": "cairn://models/decide",
            "payload": {"args_json": {
                "model": "qwen3",
                "input": "The checkout page times out for every customer.",
                "questions": [{"name": "scale", "type": "choice", "instructions": "How large is this issue?",
                               "options": [{"value": "small"}, {"value": "large"}]}]
            }},
            "timeout": timeout,
        })
    }

    /// The run result's text, out of its envelope.
    async fn run(orch: &crate::orchestrator::Orchestrator, commands: Value) -> String {
        let envelope = super::handle_run(orch, &request(json!({ "commands": commands }))).await;
        serde_json::from_str::<Value>(&envelope)
            .ok()
            .and_then(|value| value["text"].as_str().map(str::to_string))
            .unwrap_or(envelope)
    }

    /// The rs/778 call exactly: the selector sat inside the item, the lenient
    /// parse dropped it, and the batch ran on the job's residency machine. The
    /// SDK and CLI reach `handle_run` without the MCP schema, so core refuses it
    /// itself, typed as invalid input, before anything runs.
    #[tokio::test(flavor = "multi_thread")]
    async fn keys_outside_the_run_grammar_are_refused_before_anything_runs() {
        let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        for (payload, expected) in [
            (
                json!({"commands": [{"command": "hostname", "executor": {"name": "fedora"}}]}),
                "`commands[0].executor` is a call-level option: one batch runs on one machine. Move it beside `commands`",
            ),
            (
                json!({"commands": [{"command": "true"}], "commitMsg": "m"}),
                "unknown key `commitMsg`; did you mean `commit_msg`?",
            ),
            (
                json!({"commands": [{"command": "true", "timout": 5, "zzqq": 1}]}),
                "2 keys are outside the run grammar",
            ),
        ] {
            let envelope = super::handle_run(&orch, &request(payload.clone())).await;
            let value: Value = serde_json::from_str(&envelope).expect("a run envelope");
            assert_eq!(value["invalid_input"], json!(true), "{payload}: {envelope}");
            let text = value["text"].as_str().expect("envelope text");
            assert!(text.contains(expected), "{payload}: {text}");
            assert!(text.ends_with("Nothing ran."), "{payload}: {text}");
        }
    }

    /// Every advertised run target reaches its own dispatch arm, never the
    /// skill-script fallback: the table and the resolver cannot drift apart.
    #[tokio::test(flavor = "multi_thread")]
    async fn every_run_target_resolves_to_its_own_spec() {
        let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        for target in cairn_common::contract::RUN_TARGETS {
            let sample = cairn_common::uri::samples::every_resource_variant()
                .into_iter()
                .find(|sample| sample.resource.kind() == target.kind)
                .expect("every run target has a URI");
            let item: RunItem = serde_json::from_value(json!({
                "target": sample.resource.to_uri(),
                "payload": {"args_json": {}},
                "timeout": 5000,
            }))
            .unwrap();
            let (_, spec) = resolve_run_item(&orch, &request(json!({})), None, &item).await;
            match spec {
                Ok(RunSpec::Decide { args, timeout }) => {
                    assert_eq!(args, json!({}));
                    assert_eq!(timeout, Some(5000));
                }
                Ok(_) => panic!("{:?} resolved to another spec", target.kind),
                Err(error) => panic!("{:?} did not resolve: {error}", target.kind),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_decide_item_returns_the_decision_and_stays_out_of_tree_bound_batches() {
        let (orch, server, key) = workspace_with_server(
            CustomProtocol::OpenAiChatCompletions,
            None,
            "qwen3",
            vec![with_logprobs(), choosing_b()],
        )
        .await;

        let text = run(&orch, json!([decide_item(None)])).await;
        let outcome: DecideOutcome = serde_json::from_str(&text)
            .unwrap_or_else(|error| panic!("the item body is a decision ({error}):\n{text}"));
        assert_eq!(outcome.offer.provider, key);
        assert_eq!(outcome.decision.answers[0].pick(), Some("large"));
        assert_eq!(server.calls(), 2);

        let mixed = run(&orch, json!([decide_item(None), {"command": "true"}])).await;
        assert!(mixed.contains("may not mix"), "{mixed}");
        assert_eq!(server.calls(), 2, "a refused batch asks nothing");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_bad_argument_shape_fails_the_item_naming_the_shape() {
        let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        let text = run(
            &orch,
            json!([{"target": "cairn://models/decide",
                    "payload": {"args_json": {"model": "qwen3", "question": []}}}]),
        )
        .await;
        assert!(
            text.contains("{model, input, questions, provider?}"),
            "{text}"
        );
    }

    /// The facet's decide cannot be cancelled mid-request, so the item's
    /// timeout is what releases the caller; the abandoned call ends on its own.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_timeout_releases_the_caller_while_the_provider_hangs() {
        let (orch, server, _) = workspace_with_server(
            CustomProtocol::OpenAiChatCompletions,
            None,
            "qwen3",
            vec![with_logprobs(), choosing_b()],
        )
        .await;
        server.hold_each_reply(Duration::from_secs(3));

        let started = Instant::now();
        let text = run(&orch, json!([decide_item(Some(300))])).await;
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "released after {:?}",
            started.elapsed()
        );
        assert!(
            text.contains("Decision timed out at the 0.3 s bound its caller requested."),
            "the failure names the bound and that the caller set it: {text}"
        );
    }

    /// A response item cut off by its bound says which bound and what the
    /// definition declared, and the call is still journaled: the history is the
    /// one place a later reader looks for a model call's fate.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_response_timeout_names_its_bound_and_is_journaled() {
        let (orch, server, key) = workspace_with_server(
            CustomProtocol::OpenAiChatCompletions,
            None,
            "qwen3",
            vec![json!({"model": "qwen3", "choices": [{"message": {"role": "assistant", "content": "late"}}]})],
        )
        .await;
        server.hold_each_reply(Duration::from_secs(3));
        let responses = orch.config_dir.join("responses");
        std::fs::create_dir_all(&responses).unwrap();
        std::fs::write(
            responses.join("slow.md"),
            format!("---\nname: Slow\ndescription: a reasoning call\nmodel: qwen3\nbackend: {key}\ntimeout: 300s\n---\nLabel this."),
        )
        .unwrap();

        let started = Instant::now();
        let text = run(
            &orch,
            json!([{"target": "cairn://responses/slow", "payload": {"args_json": {}}, "timeout": 300}]),
        )
        .await;
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "released after {:?}",
            started.elapsed()
        );
        let named = "response timed out at the 0.3 s bound its caller requested (the definition declares 300 s).";
        assert!(text.contains(named), "{text}");
        assert_eq!(server.calls(), 1, "the provider was reached");

        let journal =
            crate::storage::list_response_invocations(&orch.db.local, "workspace", "slow", 10)
                .await
                .unwrap();
        assert_eq!(journal.len(), 1, "the timed-out call has its row");
        assert_eq!(journal[0].status, "failed");
        assert_eq!(journal[0].error.as_deref(), Some(named));
        assert_eq!(journal[0].model.as_deref(), Some("qwen3"));
        assert_eq!(journal[0].backend.as_deref(), Some(key.as_str()));
        assert_eq!(journal[0].rendered_prompt, "Label this.");
    }
}

/// A batch holding a response item takes the durable-suspend path a long shell
/// batch takes, driven here directly with a short grace window and a stand-in
/// provider, against a seeded run whose turn can park.
#[cfg(test)]
mod host_batch_tests {
    use super::types::RunSpec;
    use super::*;
    use crate::backends::{CompletionOutcome, CompletionRequest};
    use crate::db::DbState;
    use crate::responses::ResponseError;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    /// One live run in a turn that can park, and a workspace response `slow`
    /// declaring 300 s.
    async fn env() -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let local = LocalDb::open_migrated_for_test(root.join("test.db"))
            .await
            .unwrap();
        for sql in [
            "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1)",
            "INSERT INTO issues (id,project_id,number,title,status,created_at,updated_at) VALUES ('i','p',1,'T','active',1,1)",
            "INSERT INTO executions (id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id,execution_id,issue_id,project_id,node_name,status,created_at,updated_at,uri_segment) VALUES ('job-1','e','i','p','Builder','running',1,1,'builder')",
            "INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-1','job-1','open','handle-1',1,1)",
            "INSERT INTO runs (id,issue_id,project_id,job_id,status,session_id,created_at,updated_at) VALUES ('run-1','i','p','job-1','live','session-1',1,1)",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('pred-turn','session-1','run-1','job-1',1,'running','initial',1,1)",
            "UPDATE jobs SET current_session_id='session-1', current_turn_id='pred-turn' WHERE id='job-1'",
        ] {
            local.execute(sql, ()).await.unwrap();
        }
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            root.join("config"),
        )
        .build();
        let mut handle = crate::agent_process::process::RunHandle::new(
            Arc::new(std::sync::Mutex::new(None)),
            Arc::new(std::sync::Mutex::new(None)),
            Some("session-1".to_string()),
            Some("job-1".to_string()),
        );
        handle.backend = Some("codex".to_string());
        handle.begin_turn("pred-turn");
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register("run-1".to_string(), handle);
        let responses = orch.config_dir.join("responses");
        std::fs::create_dir_all(&responses).unwrap();
        std::fs::write(
            responses.join("slow.md"),
            "---\nname: Slow\ndescription: a reasoning call\nmodel: test-model\nbackend: openrouter\ntimeout: 300s\n---\nLabel this.",
        )
        .unwrap();
        orch
    }

    /// A provider that takes `delay` (or the request's own timeout, if that is
    /// shorter, as a real one would) and records the timeout it was sent.
    fn provider(orch: &Orchestrator, delay: Duration) -> Arc<Mutex<Option<Duration>>> {
        let sent = Arc::new(Mutex::new(None));
        let recorded = sent.clone();
        crate::responses::test_completion::install(
            &orch.config_dir,
            Arc::new(move |_, request: CompletionRequest, _| {
                *recorded.lock().unwrap() = Some(request.timeout);
                if request.timeout < delay {
                    std::thread::sleep(request.timeout);
                    return Err(ResponseError::Timeout.into());
                }
                std::thread::sleep(delay);
                Ok(CompletionOutcome {
                    text: "the long answer".into(),
                    parsed: None,
                    model: "test-model".into(),
                    usage: Default::default(),
                    latency_ms: delay.as_millis() as u64,
                    account_id: None,
                })
            }),
        );
        sent
    }

    /// One response item, from a call that can park when `parkable` (it names
    /// its tool call) and from a nested `cairn.run` that cannot otherwise.
    async fn batch(
        orch: &Orchestrator,
        timeout: Option<u32>,
        parkable: bool,
    ) -> (RunBatchSettlement, HostBatch) {
        let item = (
            "cairn://responses/slow".to_string(),
            RunSpec::ResponseCall {
                response_id: "slow".into(),
                project: None,
                args: json!({}),
                timeout,
            },
        );
        batch_of(orch, item, parkable).await
    }

    /// One item of any host kind, parkable as [`batch`] describes.
    async fn batch_of(
        orch: &Orchestrator,
        (header, spec): (String, RunSpec),
        parkable: bool,
    ) -> (RunBatchSettlement, HostBatch) {
        let request = McpCallbackRequest {
            thread_id: None,
            cwd: std::env::temp_dir().to_string_lossy().into_owned(),
            run_id: Some("run-1".into()),
            tool: "run".into(),
            tool_use_id: parkable.then(|| "tool-1".to_string()),
            payload: json!({"commands": [{"target": "cairn://responses/slow"}]}),
        };
        let context = crate::mcp::handlers::run_context::lookup_run_by_id(&orch.db.local, "run-1")
            .await
            .unwrap();
        // A live job's batch settles against its branch resolution, which needs
        // a repository this fixture does not have. Verdict-only settles through
        // the same publication-free tail a host batch's result reaches anyway.
        let settlement = RunBatchSettlement {
            request: request.clone(),
            run_context: Some(context.clone()),
            commit_msg: None,
            verdict_only: true,
            revision_label: None,
            logical_resolution: None,
            store_lock: None,
            routed_request: None,
            cwd: request.cwd.clone(),
            tool_use_id: "tool-1".into(),
            cd_advisory: String::new(),
            interpreter_tip: None,
            vcs: None,
            status_before: None,
            marker_escape: None,
        };
        let batch = HostBatch {
            request: request.clone(),
            cwd: request.cwd.clone(),
            tool_use_id: "tool-1".into(),
            run_context: Some(context),
            resolved: vec![(header, Ok(spec))],
            sequential: false,
            stop_on_error: true,
        };
        (settlement, batch)
    }

    /// A stand-in external server whose one tool answers after `delay`. It
    /// records the bound the gateway was handed and whether a call was
    /// abandoned (its future dropped unanswered), which is what cancels the
    /// request at a real server.
    struct SlowTool {
        delay: Duration,
        sent: Mutex<Option<Option<u32>>>,
        abandoned: Arc<std::sync::atomic::AtomicBool>,
    }

    struct MarkAbandoned(Arc<std::sync::atomic::AtomicBool>, bool);

    impl Drop for MarkAbandoned {
        fn drop(&mut self) {
            if !self.1 {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::mcp::gateway::McpGateway for SlowTool {
        async fn list_tools(
            &self,
            _: &str,
            _: &str,
            _: &crate::security::BrokeredMcpConfig,
        ) -> Result<crate::mcp::gateway::McpToolCatalog, String> {
            Ok(Default::default())
        }
        async fn list_resources(
            &self,
            _: &str,
            _: &str,
            _: &crate::security::BrokeredMcpConfig,
        ) -> Result<Vec<crate::mcp::gateway::McpResourceDef>, String> {
            Ok(Vec::new())
        }
        async fn read_resource(
            &self,
            _: &str,
            _: &str,
            _: &crate::security::BrokeredMcpConfig,
            _: &str,
        ) -> Result<String, String> {
            Ok(String::new())
        }
        async fn call_tool_once(
            &self,
            _: &str,
            _: &str,
            _: &crate::security::BrokeredMcpConfig,
            _: &str,
            _: serde_json::Value,
            _: Option<serde_json::Value>,
            _: Option<String>,
            timeout_ms: Option<u32>,
            _: Option<&str>,
        ) -> Result<crate::mcp::gateway::McpCallOutcome, String> {
            *self.sent.lock().unwrap() = Some(timeout_ms);
            let mut pending = MarkAbandoned(self.abandoned.clone(), false);
            tokio::time::sleep(self.delay).await;
            pending.1 = true;
            Ok(crate::mcp::gateway::McpCallOutcome::Complete(
                crate::mcp::gateway::McpToolCallResult::from_text("the tool answered"),
            ))
        }
        async fn close_session(&self, _: &str) {}
    }

    /// Install a [`SlowTool`] and hand back its records.
    fn slow_tool(
        orch: &Orchestrator,
        delay: Duration,
    ) -> (
        Arc<std::sync::atomic::AtomicBool>,
        impl Fn() -> Option<Option<u32>>,
    ) {
        let abandoned = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let tool = Arc::new(SlowTool {
            delay,
            sent: Mutex::new(None),
            abandoned: abandoned.clone(),
        });
        assert!(orch.set_mcp_gateway(tool.clone()).is_ok());
        (abandoned, move || *tool.sent.lock().unwrap())
    }

    fn mcp_item(timeout: Option<u32>) -> (String, RunSpec) {
        (
            "cairn://mcp/fake/slow".to_string(),
            RunSpec::McpCall(Box::new(super::types::McpCallSpec {
                server: "fake".into(),
                credential_key: "fake".into(),
                tool: "slow".into(),
                args: json!({}),
                config: serde_json::from_value(json!({})).unwrap(),
                timeout,
            })),
        )
    }

    fn was(abandoned: &std::sync::atomic::AtomicBool) -> bool {
        abandoned.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The contract this change exists for: an MCP call declared longer than
    /// the synchronous window suspends its batch and resumes with the tool's
    /// result, rather than being cut at the window.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_mcp_call_that_outlives_grace_suspends_and_resumes_with_its_result() {
        let orch = env().await;
        let (abandoned, sent) = slow_tool(&orch, Duration::from_millis(800));
        let (settlement, batch) = batch_of(&orch, mcp_item(Some(5_000)), true).await;

        let text =
            run_suspendable_host_batch(&orch, settlement, batch, Duration::from_millis(100)).await;
        assert!(text.contains(RUN_BATCH_SUSPENDED_MARKER), "{text}");
        let (state, condition) = wait_row(&orch).await;
        assert_eq!(state, "pending");
        assert!(condition.contains("\"host\":true"), "{condition}");

        let result = eventually(Duration::from_secs(10), || {
            tool_result_containing(&orch, "the tool answered")
        })
        .await
        .expect("the parked call resumes with the tool's result");
        let resumed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(resumed["toolUseId"], "tool-1");
        assert_eq!(resumed["toolResult"], "the tool answered");
        assert_eq!(resumed["isError"], false);
        assert_eq!(
            sent(),
            Some(Some(5_000)),
            "the gateway is held to the declared bound, not the host-item cap"
        );
        assert!(!was(&abandoned));
    }

    /// An omitted `timeout` is not "run to completion": an external server is
    /// arbitrary code, so the call keeps the gateway's 30 s default, and its
    /// failure names that bound and how to raise it. Time is paused so the
    /// thirty seconds pass at once.
    #[tokio::test]
    async fn an_mcp_call_with_no_timeout_fails_at_the_gateway_default_and_says_so() {
        let orch = env().await;
        let (abandoned, sent) = slow_tool(&orch, Duration::from_secs(600));
        let (_, mut batch) = batch_of(&orch, mcp_item(None), true).await;
        // No run context: the item touches no database, so paused time can
        // advance freely.
        batch.run_context = None;

        tokio::time::pause();
        let started = tokio::time::Instant::now();
        let outcomes = run_host_items(&orch, batch, None).await;
        // Paused time advances to the timer wheel's next tick, a hair past.
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_secs(30) && elapsed < Duration::from_secs(31),
            "{elapsed:?}"
        );
        assert_eq!(outcomes.len(), 1);
        assert!(!outcomes[0].succeeded);
        assert_eq!(
            outcomes[0].body,
            format!(
                "MCP call 'fake/slow' timed out at the 30 s MCP gateway default (no timeout was requested). {MCP_GATEWAY_DEFAULT_WHY}"
            )
        );
        assert!(MCP_GATEWAY_DEFAULT_WHY.contains("declare `timeout` on the item"));
        assert_eq!(sent(), Some(Some(30_000)));
        assert!(
            was(&abandoned),
            "the timed-out call was dropped, so cancelled"
        );
    }

    /// A stop cancels the suspension row and nothing else. The batch's task
    /// notices, interrupts the MCP item, and the gateway call is dropped,
    /// which cancels the request at the server. The stop already answered the
    /// call, so nothing resumes.
    #[tokio::test(flavor = "multi_thread")]
    async fn stopping_the_run_cancels_a_suspended_mcp_call() {
        let orch = env().await;
        let (abandoned, _) = slow_tool(&orch, Duration::from_secs(30));
        let (settlement, batch) = batch_of(&orch, mcp_item(Some(60_000)), true).await;

        let text =
            run_suspendable_host_batch(&orch, settlement, batch, Duration::from_millis(100)).await;
        assert!(text.contains(RUN_BATCH_SUSPENDED_MARKER), "{text}");
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='cancelled' WHERE run_id='run-1' AND state='pending'",
                (),
            )
            .await
            .unwrap();
        let stopped = Instant::now();

        eventually(Duration::from_secs(5), || async {
            was(&abandoned).then_some(())
        })
        .await
        .expect("the in-flight MCP request is cancelled");
        assert!(
            stopped.elapsed() < Duration::from_millis(2_500),
            "cancelled {:?} after the stop",
            stopped.elapsed()
        );
        assert!(tool_result_containing(&orch, "tool-1").await.is_none());
    }

    /// What the interrupted MCP item itself reports when the run stops.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stopped_mcp_item_reports_itself_stopped() {
        let orch = env().await;
        let (abandoned, _) = slow_tool(&orch, Duration::from_secs(30));
        let (_, batch) = batch_of(&orch, mcp_item(Some(60_000)), true).await;
        let (interrupter, interrupt) = crate::responses::interruption();

        let (outcomes, ()) = tokio::join!(run_host_items(&orch, batch, Some(&interrupt)), async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            interrupter.interrupt(Interruption::Cancelled(RUN_STOPPED_REASON.into()));
        });
        assert!(!outcomes[0].succeeded);
        assert_eq!(
            outcomes[0].body,
            "MCP call 'fake/slow' stopped: the run that called it was stopped before it answered. Its request was cancelled at the server."
        );
        assert!(was(&abandoned));
    }

    /// A nested `cairn.run` has nothing to park on, so an MCP call it makes
    /// ends at the window with the same why a response gets, and is cancelled.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_call_that_cannot_park_ends_its_mcp_call_at_the_window_and_says_why() {
        let orch = env().await;
        let (abandoned, _) = slow_tool(&orch, Duration::from_secs(6));
        let (settlement, batch) = batch_of(&orch, mcp_item(Some(5_000)), false).await;

        let started = Instant::now();
        let text =
            run_suspendable_host_batch(&orch, settlement, batch, Duration::from_millis(200)).await;
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "{:?}",
            started.elapsed()
        );
        let named = format!(
            "MCP call 'fake/slow' timed out at the 0.2 s host-item cap (requested 5 s). {UNPARKED_HOST_BATCH_WHY}"
        );
        assert!(text.contains(&named), "{text}");
        assert!(!UNPARKED_HOST_BATCH_WHY.contains("response"));
        assert!(was(&abandoned));
        let parked: i64 = orch
            .db
            .local
            .query_one("SELECT COUNT(*) FROM agent_waits", (), |row| row.i64(0))
            .await
            .unwrap();
        assert_eq!(parked, 0, "nothing was parked");
    }

    async fn journal(orch: &Orchestrator) -> Vec<cairn_db::storage::ResponseInvocationRecord> {
        crate::storage::list_response_invocations(&orch.db.local, "workspace", "slow", 10)
            .await
            .unwrap()
    }

    /// Poll for up to `limit` until `found` yields something.
    async fn eventually<T, F, Fut>(limit: Duration, mut found: F) -> Option<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Option<T>>,
    {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(value) = found().await {
                return Some(value);
            }
            if Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn wait_row(orch: &Orchestrator) -> (String, String) {
        orch.db
            .local
            .query_one(
                "SELECT state, condition_json FROM agent_waits WHERE run_id='run-1'",
                (),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .unwrap()
    }

    async fn tool_result_containing(orch: &Orchestrator, needle: &str) -> Option<String> {
        let needle = format!("%{needle}%");
        orch.db
            .local
            .query_text(
                "SELECT data FROM events WHERE event_type='tool_result' AND data LIKE ?1 LIMIT 1",
                (needle,),
            )
            .await
            .ok()
            .flatten()
    }

    /// The ruling's contract: a response runs past the synchronous window by
    /// suspending, keeps running while the turn is parked, and the resumed
    /// call carries its answer, journaled like any other.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_response_that_outlives_grace_suspends_and_resumes_with_its_answer() {
        let orch = env().await;
        let sent = provider(&orch, Duration::from_millis(800));
        let (settlement, batch) = batch(&orch, None, true).await;

        let text =
            run_suspendable_host_batch(&orch, settlement, batch, Duration::from_millis(100)).await;
        assert!(text.contains(RUN_BATCH_SUSPENDED_MARKER), "{text}");
        let (state, condition) = wait_row(&orch).await;
        assert_eq!(state, "pending");
        assert!(
            condition.contains("\"host\":true"),
            "a host batch holds no tree: {condition}"
        );
        assert!(journal(&orch).await.is_empty(), "still running when parked");

        let result = eventually(Duration::from_secs(10), || {
            tool_result_containing(&orch, "the long answer")
        })
        .await
        .expect("the parked call resumes with the response");
        // A one-item batch composes as the item's body alone.
        let resumed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(
            resumed["toolUseId"], "tool-1",
            "it answers the call that parked"
        );
        assert_eq!(resumed["toolResult"], "the long answer");
        assert_eq!(resumed["isError"], false);

        let rows = journal(&orch).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, "ok");
        assert_eq!(rows[0].output_text.as_deref(), Some("the long answer"));
        assert_eq!(
            *sent.lock().unwrap(),
            Some(Duration::from_secs(300)),
            "with no item timeout, the definition's own timeout governs the request"
        );
    }

    /// The item's own bound still ends it, in 5250's words, with its row.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_response_items_own_timeout_names_its_bound_and_is_journaled() {
        let orch = env().await;
        provider(&orch, Duration::from_secs(5));
        let (settlement, batch) = batch(&orch, Some(200), true).await;

        let started = Instant::now();
        let text =
            run_suspendable_host_batch(&orch, settlement, batch, Duration::from_secs(5)).await;
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{:?}",
            started.elapsed()
        );
        let named = "response timed out at the 0.2 s bound its caller requested (the definition declares 300 s).";
        assert!(text.contains(named), "{text}");
        let rows = journal(&orch).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, "failed");
        assert_eq!(rows[0].error.as_deref(), Some(named));
    }

    /// A stop cancels the suspension row and nothing else. The batch notices,
    /// interrupts its response item at once, and the item journals the stop;
    /// the abandoned request was held to the item's bound, so it ends on its
    /// own within it. The stop already answered the call, so nothing resumes.
    #[tokio::test(flavor = "multi_thread")]
    async fn stopping_the_run_interrupts_a_suspended_response_and_journals_it() {
        let orch = env().await;
        let sent = provider(&orch, Duration::from_secs(30));
        let (settlement, batch) = batch(&orch, Some(4_000), true).await;

        let text =
            run_suspendable_host_batch(&orch, settlement, batch, Duration::from_millis(100)).await;
        assert!(text.contains(RUN_BATCH_SUSPENDED_MARKER), "{text}");
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='cancelled' WHERE run_id='run-1' AND state='pending'",
                (),
            )
            .await
            .unwrap();
        let stopped = Instant::now();

        let rows = eventually(Duration::from_secs(5), || async {
            let rows = journal(&orch).await;
            (!rows.is_empty()).then_some(rows)
        })
        .await
        .expect("the interrupted call is journaled");
        assert!(
            stopped.elapsed() < Duration::from_millis(2_500),
            "released {:?} after the stop, while the provider still had seconds to run",
            stopped.elapsed()
        );
        assert_eq!(rows[0].status, "failed");
        assert_eq!(
            rows[0].error.as_deref(),
            Some("response cancelled: the run that called it was stopped before it answered")
        );
        let sent = sent.lock().unwrap().expect("the request was sent");
        assert!(
            sent <= Duration::from_secs(4),
            "the abandoned request is bounded by the item's bound: {sent:?}"
        );
        assert!(tool_result_containing(&orch, "slow").await.is_none());
    }

    /// A nested `cairn.run` has no tool call of its own to park on, and nothing
    /// above the host listens past grace. Its response is ended at the window
    /// with a failure saying why and how to get the full timeout, and journaled.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_call_that_cannot_park_ends_its_response_at_the_window_and_says_why() {
        let orch = env().await;
        provider(&orch, Duration::from_secs(6));
        let (settlement, batch) = batch(&orch, None, false).await;

        let started = Instant::now();
        let text =
            run_suspendable_host_batch(&orch, settlement, batch, Duration::from_millis(200)).await;
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "{:?}",
            started.elapsed()
        );
        let named = format!(
            "response timed out at the 0.2 s host-item cap (no timeout was requested; the definition declares 300 s). {UNPARKED_HOST_BATCH_WHY}"
        );
        assert!(text.contains(&named), "{text}");
        assert!(UNPARKED_HOST_BATCH_WHY.contains("@cairn/sdk"));
        let rows = journal(&orch).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].error.as_deref(), Some(named.as_str()));
        let parked: i64 = orch
            .db
            .local
            .query_one("SELECT COUNT(*) FROM agent_waits", (), |row| row.i64(0))
            .await
            .unwrap();
        assert_eq!(parked, 0, "nothing was parked");
    }

    /// Responses take the run-item ceiling; decide keeps the 120 s host-item
    /// cap and never makes its batch suspendable.
    #[test]
    fn a_response_is_bounded_by_the_ceiling_and_decide_by_the_host_item_cap() {
        let ceiling = Duration::from_millis(u64::from(MAX_RUN_ITEM_TIMEOUT_MS));
        let asked = response_item_bound(Some(310_000));
        assert_eq!(asked.limit, Duration::from_secs(310));
        assert_eq!(asked.cap, None, "an ask under the ceiling is its own bound");
        let unasked = response_item_bound(None);
        assert_eq!(unasked.limit, ceiling);
        assert!(unasked
            .describe(None)
            .starts_with("timed out at the 21600 s run-item ceiling (no timeout was requested)."));
        assert_eq!(response_item_bound(Some(u32::MAX)).limit, ceiling);
        assert!(RUN_ITEM_CEILING_WHY.contains(&format!(
            "{}-hour",
            cairn_common::run_contract::RUN_BATCH_CEILING_HOURS
        )));

        let decide = host_item_bound(Some(310_000));
        assert_eq!(decide.limit, Duration::from_secs(120));
        assert_eq!(
            decide.cap.as_ref().map(|cap| cap.name),
            Some("host-item cap")
        );
        assert!(!HOST_ITEM_CAP_WHY.contains("Response"));

        let host = |spec: RunSpec| HostBatch {
            request: McpCallbackRequest {
                thread_id: None,
                cwd: String::new(),
                run_id: None,
                tool: "run".into(),
                tool_use_id: None,
                payload: json!({}),
            },
            cwd: String::new(),
            tool_use_id: String::new(),
            run_context: None,
            resolved: vec![(String::new(), Ok(spec))],
            sequential: false,
            stop_on_error: true,
        };
        assert!(!host(RunSpec::Decide {
            args: json!({}),
            timeout: None
        })
        .may_outlive_grace());
        assert!(host(RunSpec::ResponseCall {
            response_id: "slow".into(),
            project: None,
            args: json!({}),
            timeout: None,
        })
        .may_outlive_grace());
        assert!(host(mcp_item(None).1).may_outlive_grace());
    }

    /// MCP-tool items take the run-item ceiling when they declare a bound and
    /// the gateway's default when they declare none; never the host-item cap.
    #[test]
    fn an_mcp_item_is_bounded_by_its_ask_or_the_gateway_default() {
        let ceiling = Duration::from_millis(u64::from(MAX_RUN_ITEM_TIMEOUT_MS));
        let asked = mcp_item_bound(Some(240_000));
        assert_eq!(asked.limit, Duration::from_secs(240));
        assert_eq!(
            asked.describe(None),
            "timed out at the 240 s bound its caller requested."
        );
        assert_eq!(mcp_item_bound(Some(u32::MAX)).limit, ceiling);
        let unasked = mcp_item_bound(None);
        assert_eq!(unasked.limit, crate::mcp::gateway::DEFAULT_MCP_TIMEOUT);
        assert_eq!(unasked.limit, Duration::from_secs(30));
        assert!(MCP_GATEWAY_DEFAULT_WHY.contains(&format!(
            "{}-hour",
            cairn_common::run_contract::RUN_BATCH_CEILING_HOURS
        )));
    }
}

#[cfg(test)]
mod ran_against_line_tests {
    use super::ran_against_line;
    use cairn_common::cache_distance::CacheDistance;
    use cairn_common::executor_protocol::{CellOrigin, CellProvenance, CellSeed};

    fn origin(cell: &str, provenance: CellProvenance) -> CellOrigin {
        CellOrigin {
            cell_id: cell.into(),
            provenance,
            warmth: None,
        }
    }

    #[test]
    fn names_how_the_cell_was_seeded_or_reused() {
        let cloned = origin(
            "slot-7",
            CellProvenance::Seeded {
                seed: CellSeed::Clone {
                    source_cell_id: "slot-3".into(),
                    source_holder: Some("cairn/5105".into()),
                    source_commit: "659e638aabbccdd".into(),
                    distance: Some(CacheDistance {
                        files: 12,
                        crates: 3,
                        ..Default::default()
                    }),
                    cloned_at_unix_ms: 1,
                    clone_ms: 900,
                },
            },
        );
        let catalog = origin(
            "slot-7",
            CellProvenance::Seeded {
                seed: CellSeed::Catalog {
                    generation: "g1".into(),
                },
            },
        );
        let cold = origin(
            "slot-7",
            CellProvenance::Seeded {
                seed: CellSeed::Cold,
            },
        );
        let reused = origin(
            "slot-3",
            CellProvenance::Reused {
                distance: Some(CacheDistance::default()),
            },
        );
        assert_eq!(
            ran_against_line("main", Some(&cloned)),
            "ran against main, cell 7 cloned from cell 3 (held by cairn/5105; warm: 12 files / 3 crates from 659e638)"
        );
        assert_eq!(
            ran_against_line("main", Some(&catalog)),
            "ran against main, cell 7 seeded from the dependency catalog"
        );
        assert_eq!(
            ran_against_line("main", Some(&cold)),
            "ran against main, cell 7 (cold: no clone source)"
        );
        assert_eq!(
            ran_against_line("main", Some(&reused)),
            "ran against main, cell 3 (warm: 0 files / 0 crates)"
        );
        assert_eq!(ran_against_line("main", None), "ran against main");
    }
}
