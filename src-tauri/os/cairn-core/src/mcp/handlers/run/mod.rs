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
pub(crate) use sandbox_policy::{build_run_sandbox_policy, RunCheckout};
pub use types::{
    CheckStatusEntry, CheckStatusPayload, ChecksWaitEvent, ChecksWaitKind, RunCompletePayload,
    RunItem, RunItemPayload, RunOutputPayload, RunPayload, TerminalWaitEvent, TerminalWaitKind,
    WaitDuration, WaitFor,
};

use crate::mcp::vcs::{acquire_store_lock, STORE_LOCK_TIMEOUT};
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
    let _guard = acquire_store_lock(
        orch,
        Some(store_dir),
        "build-slot delta publication and seal",
        STORE_LOCK_TIMEOUT,
    )
    .await?;
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
    let publication = publish_visible_slot_delta(orch, &target, request, delta, message, author)
        .await
        .map_err(|error| error.to_string())?;
    // The upload was validated and installed before either outcome was reached,
    // so it is finalized on both.
    if publication.consume_receipt {
        if let Err(error) = finalize_delta_receipt(orch, &target, delta).await {
            log::warn!("system-fix delta upload was not finalized: {error}");
        }
    }
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
    cairn_codec::transfer::verify_commit_closure(&objects_dir, &[], &delta.delta_commit)
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
        patch: String,
        /// Present when the branch moved while the batch ran and its changes
        /// were merged onto the head the branch actually held.
        integration: Option<IntegrationNote>,
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

/// Why a routed batch could not be published.
///
/// [`SlotPublicationError::Straddled`] is separated from everything else
/// because the two need opposite things said about them: an ordinary
/// publication failure leaves the batch unrun, while a straddle that cannot be
/// merged leaves real work sitting in the working directory with a route back
/// into the branch that has to be spelled out.
#[derive(Debug)]
pub(crate) enum SlotPublicationError {
    Straddled(String),
    Other(String),
}

impl From<String> for SlotPublicationError {
    fn from(error: String) -> Self {
        Self::Other(error)
    }
}

impl std::fmt::Display for SlotPublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Straddled(text) | Self::Other(text) => f.write_str(text),
        }
    }
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

// Caller holds the canonical per-store lock for the full visibility window.
async fn publish_visible_slot_delta(
    orch: &Orchestrator,
    target: &RunnerPublicationTarget,
    request: &CellRequest,
    delta: &crate::fleet::MutationDelta,
    message: &str,
    author: Option<&PublicationIdentity>,
) -> Result<SlotPublication, SlotPublicationError> {
    debug_assert_eq!(target.project_id, request.project_id);
    debug_assert_eq!(target.repository_identity, request.repository.identity());
    debug_assert!(target.git_common_dir.is_absolute());
    let repository = &target.repository;
    let (consume_receipt, installed_pack) =
        make_delta_objects_available(orch, repository, request, delta)?;
    let _object_pin = crate::jj::pin_validated_delta(
        repository,
        &delta.base_commit,
        &delta.delta_commit,
        installed_pack.as_deref(),
    )?;
    verify_available_delta(repository, delta)?;

    // A delta declares as its parent the commit the batch was routed against,
    // and the logical-head transaction refuses a parent the bookmark no longer
    // holds. Read the bookmark here — under the same store lock the transaction
    // will re-check it under, through the same jj view, so the two cannot
    // disagree — and merge the batch's changes onto it when it moved. The
    // executor cannot do this: it cannot tell its own head having been refreshed
    // to a new tip from its head being a descendant carrying work the runner has
    // not published back yet.
    let head = cairn_vcs::resolve_coordinate(&target.store_dir, &target.branch)
        .await
        .map_err(|error| format!("resolve `{}` for delta publication: {error}", target.branch))?;
    let integration = crate::jj::integrate_delta_onto_head(
        repository,
        &delta.base_commit,
        &delta.delta_commit,
        &head,
    )?;
    // Held for the same window as `_object_pin`: the merged commit, its tree,
    // and the blobs the merge wrote are ref-less until the transaction commits.
    let mut _integrated_pin = None;
    let (expected, proposed, integrated) = match integration {
        crate::jj::Integration::Unmoved => {
            (delta.base_commit.clone(), delta.delta_commit.clone(), false)
        }
        crate::jj::Integration::AlreadyLanded => {
            return Ok(SlotPublication {
                consume_receipt,
                outcome: SlotPublicationOutcome::AlreadyLanded { head },
            })
        }
        crate::jj::Integration::Commit(commit) => {
            _integrated_pin = Some(crate::jj::pin_validated_delta(
                repository, &head, &commit, None,
            )?);
            (head.clone(), commit, true)
        }
        crate::jj::Integration::Conflicted { paths } => {
            return Err(SlotPublicationError::Straddled(strand_conflicted_delta(
                repository, request, delta, &head, &paths,
            )))
        }
    };

    let head_description = if integrated && message == "^" {
        Some(git_output(
            repository,
            &["log", "-1", "--format=%B", &expected],
            "read the logical head description for a converted amend",
        )?)
    } else {
        None
    };
    let (mode, amend_converted) = publication_mode(message, head_description.as_deref());
    // Carried for BOTH modes: an amend re-stamps the committer even though it
    // preserves the author, so withholding the identity here leaves the
    // amended commit unpushable.
    let identity = author.cloned();
    let published_onto = expected.clone();
    let crate::jj::PublishedLogicalHead { landed, export } =
        crate::jj::publish_logical_head_exported(
            &crate::jj::environment(&orch.jj_binary_path, &orch.config_dir),
            &target.store_dir,
            &target.branch,
            &expected,
            crate::jj::ProposedPublication::DeltaCommit(proposed),
            identity,
            mode,
        )
        .await?;
    if let Some(job_id) = request
        .owner
        .as_ref()
        .and_then(|owner| owner.job_id.as_deref())
    {
        orch.invalidate_node_check_status(job_id, "logical-head-tree-or-contract-change");
    }
    // Against the head actually published onto, never the routed base: after an
    // integration those differ, and diffing from the routed base would attribute
    // every line the advance carried to this batch.
    let patch = git_output(
        repository,
        &[
            "diff",
            "--no-ext-diff",
            "--binary",
            &published_onto,
            &landed.head,
        ],
        "capture logical-head delta patch",
    )?;
    Ok(SlotPublication {
        consume_receipt,
        outcome: SlotPublicationOutcome::Published {
            landed,
            export,
            patch,
            integration: integrated.then_some(IntegrationNote {
                head: published_onto,
                amend_converted,
            }),
        },
    })
}

/// Whether this run's commit must reach `origin` now or may wait for the next
/// explicit publication barrier.
///
/// The same question the write verb asks, asked the same way, so the two verbs
/// cannot drift into different publication semantics for the same branch. An
/// unresolvable routed run cannot PROVE that no open PR depends on this branch,
/// so it fails closed exactly as the write path does.
async fn publication_requirement_for_run(
    routed: Option<&(
        crate::mcp::handlers::RunContext,
        std::sync::Arc<crate::storage::LocalDb>,
    )>,
    branch: &str,
) -> crate::merge_requests::queries::PublicationRequirement {
    match routed {
        Some((run, db)) => {
            crate::merge_requests::queries::publication_requirement_for_managed_branch(
                db,
                &run.job_id,
                &run.project_id,
                branch,
            )
            .await
        }
        None => crate::merge_requests::queries::PublicationRequirement::RequiredForOpenPr,
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
    let reference = stranded_ref_name(request);
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
fn stranded_ref_name(request: &CellRequest) -> String {
    let job = request
        .owner
        .as_ref()
        .and_then(|owner| owner.job_id.as_deref())
        .filter(|job| !job.is_empty() && !job.contains('/'));
    match job {
        Some(job) => format!("refs/cairn/stranded/{job}/{}", request.attempt_id),
        None => format!("refs/cairn/stranded/{}", request.attempt_id),
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
/// The generic publication failure says the batch was not rerun locally, which
/// is true of every other failure and false of this one: real work was sealed
/// and nothing has been lost. This names where the branch went, what collided,
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
        )
        .await
        .expect("a straddle over disjoint paths publishes");
        let SlotPublicationOutcome::Published {
            landed,
            export,
            patch,
            integration,
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
        )
        .await
        .expect("an unmoved bookmark publishes");
        let SlotPublicationOutcome::Published {
            landed,
            export,
            patch,
            integration,
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
            publish_visible_slot_delta(&orch, &fx.target, &fx.request, &fx.delta, "^", None)
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
use output::{collect_run_images, compose_run_output, run_envelope};
use process::run_one;
pub(crate) use process::READ_ONLY_CHECKOUT_DENIAL;
use resolve::resolve_run_item;
use types::ItemOutcome;
pub(crate) use types::RunSpec;

fn build_slot_command_parts(
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

/// The largest kill bound an item that executes ON THE HOST may carry — an MCP
/// gateway call or a REPL send.
///
/// These items never enter [`run_routed_batch`], so the batch cannot suspend for
/// them and the call stays attached to its socket. The host answers within the
/// grace window, so a host item that outlives grace cannot deliver its result at
/// all: the transport gives up first and the whole batch is discarded with no
/// output. Capping the bound at grace converts that silent transport death into
/// an ordinary item timeout with a real result block — the same trade the batch
/// ceiling makes for routed items, sized to the ceiling that actually sits above
/// this path.
///
/// This is deliberately NOT [`MAX_RUN_ITEM_TIMEOUT_MS`]. Letting a host item ask
/// for six hours would advertise a bound the transport underneath it cannot
/// honor, which is the exact class of defect this file exists to remove.
const MAX_HOST_ITEM_TIMEOUT_MS: u32 = cairn_common::run_contract::RUN_GRACE_WINDOW_MS as u32;

/// The ordering between the two item bounds is the whole point, so it is checked
/// at compile time rather than by a test that could be deleted.
const _: () = assert!(MAX_HOST_ITEM_TIMEOUT_MS < MAX_RUN_ITEM_TIMEOUT_MS);

/// The clamp for a host-executed item's kill bound. `None` is preserved rather
/// than filled, because these items have their own meaningful defaults below
/// this bound (the MCP gateway's own call timeout); "run to completion" is a
/// promise only the suspendable routed path can keep.
pub(crate) fn clamp_host_item_timeout_ms(requested: Option<u32>) -> Option<u32> {
    requested.map(|ms| ms.min(MAX_HOST_ITEM_TIMEOUT_MS))
}

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

/// The instant past which nobody is waiting for this batch.
///
/// The one number both sides honour: the executor holds this batch's queue entry
/// until here, and the runner waits for it until here. It is the batch's own
/// declared bound when it made one, and the batch ceiling otherwise — the same
/// authority that already cancels a batch which never ends, so a batch is never
/// held for longer than something above it would allow anyway.
///
/// Computed once, at the top of placement, and never refreshed. A refreshed
/// horizon measures the attempt rather than the batch, which is exactly how a
/// twenty-second queue budget came to stand in for an agent's real patience.
fn batch_wait_horizon_unix_ms(payload: &RunPayload, sequential: bool) -> u64 {
    let budget = declared_capacity_wait_budget(payload, sequential).unwrap_or(RUN_BATCH_CEILING);
    crate::fleet::unix_time_ms().saturating_add(budget.as_millis() as u64)
}

/// Settle every process item's kill bound once, before the dispatch seam, so no
/// layer below has to re-derive it.
fn apply_run_item_timeouts(resolved: &mut [(String, Result<RunSpec, String>)]) {
    for (_, spec) in resolved {
        if let Ok(RunSpec::Shell { timeout, .. } | RunSpec::Script { timeout, .. }) = spec {
            *timeout = Some(clamp_run_item_timeout_ms(*timeout));
        } else if let Ok(RunSpec::ResponseCall { timeout, .. }) = spec {
            *timeout =
                Some(clamp_host_item_timeout_ms(*timeout).unwrap_or(MAX_HOST_ITEM_TIMEOUT_MS));
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
    let mut payload: RunPayload = match super::parse_payload(request) {
        Ok(payload) => payload,
        Err(error) => return run_envelope(error, Vec::new()),
    };

    if payload.commands.is_empty() {
        return run_envelope(
            "Invalid payload: `commands` must contain at least one item".to_string(),
            Vec::new(),
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
            crate::orchestrator::default_branch_sync::revision_label(
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
    let has_response = resolved
        .iter()
        .any(|(_, spec)| matches!(spec, Ok(RunSpec::ResponseCall { .. })));
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
                )
                .into_iter()
                .chain(
                    transcript_event_uri
                        .clone()
                        .map(|uri| ("CAIRN_TRANSCRIPT_EVENT_URI".into(), uri)),
                )
                .collect(),
                priority: CellPriority::AgentInteractive,
                // Computed once, below, and carried unchanged by every
                // presentation. Refreshing it per attempt would make the number
                // measure the attempt instead of the batch, which is the drift
                // the horizon exists to remove.
                wait_horizon_unix_ms: batch_wait_horizon_unix_ms(&payload, sequential),
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
                selector: payload.executor.clone(),
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
            return run_routed_batch(orch, settlement, placement, batch).await;
        }
    }

    // Everything below runs on the host: MCP gateway calls, REPL sends, and
    // items that failed to resolve. Tree-bound shell and script items never
    // reach here — a process batch that cannot be placed is refused above.
    let outcomes = if sequential {
        let mut outcomes: Vec<ItemOutcome> = Vec::with_capacity(resolved.len());
        for (index, (header, spec)) in resolved.into_iter().enumerate() {
            let stream_id = run_item_stream_id(&tool_use_id, index);
            let outcome = run_one(
                orch,
                request,
                &cwd,
                &stream_id,
                run_context.as_ref(),
                header,
                spec,
            )
            .await;
            // A suspend stops the (sequential) batch: the whole call re-runs on
            // resume once the fence is answered.
            let stop = outcome.suspended || (!outcome.succeeded && stop_on_error);
            outcomes.push(outcome);
            if stop {
                break;
            }
        }
        outcomes
    } else {
        // Parallel: each item runs on its own task so one item's wait never stalls
        // the others. Each handle is wrapped in an abort-on-drop guard so dropping
        // this handler future (client disconnect / MCP cancel) aborts every
        // in-flight item, which drops each item's kill-on-drop guard and reaps its
        // process group — detached `tokio::spawn` tasks would otherwise outlive
        // the cancelled request.
        let mut handles = Vec::with_capacity(resolved.len());
        for (index, (header, spec)) in resolved.into_iter().enumerate() {
            let orch = orch.clone();
            let cwd = cwd.clone();
            let stream_id = run_item_stream_id(&tool_use_id, index);
            let run_context = run_context.clone();
            let request = request.clone();
            handles.push(AbortOnDrop(tokio::spawn(async move {
                run_one(
                    &orch,
                    &request,
                    &cwd,
                    &stream_id,
                    run_context.as_ref(),
                    header,
                    spec,
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
    };

    let settlement = RunBatchSettlement {
        request: request.clone(),
        run_context,
        commit_msg: payload.commit_msg,
        verdict_only: branch_target.is_some(),
        revision_label,
        selector: None,
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
    let settled = settle_run_batch(
        orch,
        &settlement,
        BatchResidence::Home,
        outcomes,
        None,
        None,
        None,
        None,
    )
    .await;
    if settled.suspended {
        return run_envelope(RUN_ITEM_SUSPENDED_MARKER.to_string(), Vec::new());
    }
    run_envelope(settled.text, settled.images)
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

    /// The note a batch that ran elsewhere for this reason carries.
    fn ran_elsewhere_note(&self, home: &str) -> String {
        use cairn_common::executor_protocol::describe_gib;
        format!(
            "Ran on **{}**: this batch's measured memory (~{}) exceeds what {home} can ever hold ({}). Your execution environment there is unchanged.",
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

    fn note(self, selector: Option<&ExecutorSelector>) -> Option<String> {
        if self != Self::Detached {
            return None;
        }
        let selector = selector?;
        let destination = selector
            .name
            .as_deref()
            .map(|name| format!("**{name}**"))
            .unwrap_or_else(|| format!("an executor matching **{}**", selector.describe()));
        Some(format!("Ran on {destination}"))
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
    let owned_job_id = job_id.to_string();
    let terminals = db
        .read(move |conn| {
            let job_id = owned_job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT t.slug FROM job_terminals t
                         JOIN jobs j ON j.id = t.job_id
                         WHERE t.job_id = ?1
                           AND t.status = 'running'
                           AND COALESCE(j.branch, '') != ''
                         ORDER BY t.slug",
                        (job_id.as_str(),),
                    )
                    .await?;
                let mut slugs = Vec::new();
                while let Some(row) = rows.next().await? {
                    slugs.push(row.text(0)?);
                }
                Ok(slugs)
            })
        })
        .await
        .unwrap_or_else(|error| {
            log::warn!("Could not list running terminals for job {job_id}: {error}");
            Vec::new()
        });
    terminals
        .into_iter()
        .map(|slug| format!("terminal `{slug}`"))
        .chain(
            orch.repl_state
                .resident_slugs(job_id)
                .into_iter()
                .map(|slug| format!("REPL `{slug}`")),
        )
        .collect()
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
    /// The caller's own executor selector, for the note a detached batch earns.
    selector: Option<ExecutorSelector>,
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
    /// How long the agent itself allowed this batch to spend looking for room.
    capacity_wait_budget: Option<std::time::Duration>,
}

impl BatchPlacement {
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
    /// "Ran on X: ..." for a batch detached because its home cannot hold it.
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
            notes.executed()
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
            let settled = settle_joined_batch(orch, &settlement, joined, None).await;
            if settled.suspended {
                return run_envelope(RUN_ITEM_SUSPENDED_MARKER.to_string(), Vec::new());
            }
            run_envelope(settled.text, settled.images)
        }
        Err(_) => {
            suspend_until_batch_settles(orch, settlement, handle, request_id, cancelled).await
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
    placement: BatchPlacement,
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
    let mut wait = CapacityWait::new(placement.capacity_wait_budget);
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
            // Acquiring the home is part of placing this batch, so it waits on
            // the batch's own horizon. A cold cell that legitimately takes
            // minutes is then slow rather than refused.
            Some(environment) => match crate::fleet::residency::acquire_job_residency(
                orch,
                &environment.db,
                &environment.job_id,
                &environment.base_commit,
                placement.template.wait_horizon_unix_ms,
                placement.template.waiting_since_unix_ms,
            )
            .await
            {
                Ok(fence) => Some(fence),
                Err(refusal) if refusal.verdict.is_capacity() => {
                    match wait.hold(&request_id, &refusal.diagnostic).await {
                        Ok(()) => continue,
                        Err(spent) => return PlacedBatch::NoRoom(spent),
                    }
                }
                Err(refusal) => return PlacedBatch::NoEnvironment(refusal.diagnostic),
            },
        };
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
            let notes = PlacementNotes {
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

    let suspended = begin_batch_suspension(orch, &settlement, &request_id).await;
    let Some((record, db, handoff)) = suspended else {
        // Nothing to park (no correlatable call, no active turn): hold the call
        // open rather than abandoning a running batch.
        let joined = handle.await;
        let settled = settle_joined_batch(orch, &settlement, joined, None).await;
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
        let settled = settle_joined_batch(&owned_orch, &settlement, joined, ceiling).await;
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

/// Persist the suspension row for a routed batch and park its run. `None` means
/// this call cannot be resumed durably (no correlatable tool call, no run
/// context, or no active turn), so the caller must keep awaiting inline.
async fn begin_batch_suspension(
    orch: &Orchestrator,
    settlement: &RunBatchSettlement,
    request_id: &str,
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
            settle_routed_run_batch(
                orch,
                settlement,
                residence,
                *outcome,
                grant_without_result.clone(),
                ceiling,
                notes,
            )
            .await
        }
        PlacedBatch::Refused(refusal) => refused(format!("{refusal}.")),
        PlacedBatch::NoEnvironment(diagnostic) => refused(format!(
            "this job's environment could not be reached ({diagnostic})."
        )),
        PlacedBatch::NoRoom(detail) => refused(detail),
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
    settle_run_batch(
        orch,
        settlement,
        residence,
        outcomes,
        routed_delta,
        routed_tracked_modifications,
        publication_request.as_ref(),
        notes.executed(),
    )
    .await
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
    placement_note: Option<String>,
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
        result = format!("ran against {label}\n\n{result}");
    }
    let residence_note = [residence.note(s.selector.as_ref()), placement_note]
        .into_iter()
        .flatten()
        .reduce(|left, right| format!("{left}\n\n{right}"));

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
    let barrier = match acquire_store_lock(
        orch,
        s.store_lock.as_deref(),
        "run commit barrier publication",
        STORE_LOCK_TIMEOUT,
    )
    .await
    {
        Ok(_store_guard) => {
            let routed = match (
                s.commit_msg.as_deref(),
                routed_delta.as_ref(),
                publication_request.or(s.routed_request.as_ref()),
                s.logical_resolution.as_ref(),
            ) {
                (Some(message), Some(delta), Some(request), Some(resolution)) => Some(match s.store_lock.as_deref() {
                    Some(_store) => match resolve_runner_publication_target(
                        resolution, request,
                    ) {
                        Ok(target) => publish_visible_slot_delta(
                            orch,
                            &target,
                            request,
                            delta,
                            message,
                            author.as_ref(),
                        )
                        .await
                        .map(|publication| (publication, target)),
                        Err(error) => Err(SlotPublicationError::Other(error)),
                    },
                    None => Err(SlotPublicationError::Other(
                        "build-slot publication has no resolved shared-store lock path".to_string(),
                    )),
                }),
                _ => None,
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
                        outcome,
                    } = publication;
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
                    // Everything below is post-transaction: the origin push is
                    // network I/O and must not hold the store lock.
                    drop(_store_guard);
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
                            integration,
                        } => {
                            // The publication ladder, identical to the write
                            // verb's: export always, push when a remote PR is
                            // open on this branch. `run` had neither leg, which
                            // is why a coordinator's integration commits never
                            // moved their branch ref and a builder's
                            // run-committed review fixes never reached its PR.
                            let published = match export {
                                Err(error) => Err(error),
                                Ok(()) => match publication_requirement_for_run(
                                    routed.as_ref(),
                                    &target.branch,
                                )
                                .await
                                {
                                    crate::merge_requests::queries::PublicationRequirement::RequiredForOpenPr => {
                                        crate::orchestrator::base_advance::publish_managed_branch(
                                            orch,
                                            &target.store_dir,
                                            &target.branch,
                                        )
                                        .await
                                    }
                                    crate::merge_requests::queries::PublicationRequirement::DeferredUntilPublication => Ok(()),
                                },
                            };
                            let mut message = match published {
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
                            if let Some(resolution) = s.logical_resolution.as_ref() {
                                let sync_failures =
                                    crate::dev_instances::sync_live_branch_instances(
                                        orch,
                                        &resolution.project_id,
                                        &resolution.rev,
                                        &landed.head,
                                    )
                                    .await;
                                if !sync_failures.is_empty() {
                                    message.push_str(&format!(
                                        " — ⚠️ live dev instance sync blocked ({} instance(s)); the owner received a blocking notice",
                                        sync_failures.len()
                                    ));
                                }
                            }
                            message.push_str(&notes);
                            if let (Some(resolution), Some((_run, db))) =
                                (s.logical_resolution.as_ref(), routed.as_ref())
                            {
                                if let Err(error) = crate::mcp::vcs::publish_sealed_commit_pack(
                                    db,
                                    &resolution.project_id,
                                    &resolution.object_repository_path,
                                    &landed.head,
                                )
                                .await
                                {
                                    message.push_str(&format!(
                                        " — ⚠️ sealed commit cloud publication failed: {error}"
                                    ));
                                }
                            }
                            CommitBarrierOutcome {
                                message,
                                worktree_changed: false,
                                committed: true,
                            }
                        }
                    }
                }
                Some(Err(error)) => CommitBarrierOutcome {
                    message: match error {
                        // A straddle that could not be merged left real work
                        // behind and carries its own route back into the branch;
                        // the generic tail would be false about both.
                        SlotPublicationError::Straddled(text) => format!("⚠️ {text}"),
                        SlotPublicationError::Other(text) => {
                            format!("⚠️ {text}. The routed batch was not rerun locally.")
                        }
                    },
                    worktree_changed: false,
                    committed: false,
                },
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
    // with commit_msg + a successful seal).
    if barrier.committed {
        // A commit just sealed → the branch advanced. Cancel any in-flight
        // when:review suite for this job so its heavy concurrent compiles stop
        // starving this commit's own when:write checks (below) and the agent's
        // next manual check run; the review cadence relaunches fresh at the next
        // turn-end. See cancel_stale_review_on_branch_advance for the rationale
        // and the deliberate job-id scoping.
        if let Some(ctx) = s.run_context.as_ref() {
            crate::execution::checks::cancel_stale_review_on_branch_advance(orch, &ctx.job_id)
                .await;
        }
        if let Some(summary) = crate::execution::checks::run_write_checks_after_seal(
            orch,
            s.run_context.as_ref(),
            &s.cwd,
            &s.tool_use_id,
            None,
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

    #[test]
    fn detached_residence_note_is_compact_and_home_is_silent() {
        let selector = named("bglab-ub");
        let note = BatchResidence::Detached.note(Some(&selector)).unwrap();
        assert_eq!(note, "Ran on **bglab-ub**");
        assert!(BatchResidence::Home.note(Some(&selector)).is_none());
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
            ran_elsewhere: detached.then(|| "Ran on **bglab-mac**: this batch's measured memory (~9.6 GiB) exceeds what bgsrv-ub can ever hold (7.7 GiB). Your execution environment there is unchanged.".to_string()),
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
        assert!(executed.starts_with("Ran on **bglab-mac**"), "{executed}");
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
            "Ran on **bglab-mac**: this batch's measured memory (~9.6 GiB) exceeds what bgsrv-ub can ever hold (7.7 GiB). Your execution environment there is unchanged."
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

        let mut resolved = vec![(
            "response".into(),
            Ok(RunSpec::ResponseCall {
                response_id: "test".into(),
                project: None,
                args: serde_json::json!({}),
                timeout: None,
            }),
        )];
        apply_run_item_timeouts(&mut resolved);
        assert!(matches!(
            &resolved[0].1,
            Ok(RunSpec::ResponseCall { timeout: Some(timeout), .. })
                if *timeout == MAX_HOST_ITEM_TIMEOUT_MS
        ));
    }

    /// `CellRequest.timeout_ms` is the request's own bound. Sequential items add
    /// up, parallel items overlap, and neither may exceed the ceiling the host
    /// enforces — a request claiming more time than the batch can legally take
    /// is the same class of disagreement that lost the original suite's output.
    fn payload(value: serde_json::Value) -> RunPayload {
        serde_json::from_value(value).expect("a run payload")
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
            placed_process_env(None, None, None, Some("agent/cairn-2929-builder-0")),
            [(
                "CAIRN_WORKTREE_BRANCH".into(),
                "agent/cairn-2929-builder-0".into()
            )]
        );
        assert_eq!(
            placed_process_env(None, None, Some("feature/dev-instance"), None),
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
                Some("agent/cairn-3381-builder-0")
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
        assert!(placed_process_env(None, None, None, None).is_empty());
        assert!(placed_process_env(Some(""), None, None, None).is_empty());
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
