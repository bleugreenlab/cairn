//! Write-verb MCP handler.

pub(crate) mod file_mutations;
mod home_targets;
mod preview;
#[cfg_attr(feature = "test-utils", doc(hidden))]
pub mod replay;
mod types;

use self::file_mutations::{
    apply_logical_file_batch, apply_prepared_logical, finalize_file_commit,
    record_agent_file_change, record_agent_file_change_once, reverted_file_changes, CommitOutcome,
    FileBatchSuccess,
};
pub(crate) use self::preview::build_current_event_uri;
use self::preview::{handle_apply_change, preview_change};
use self::types::{
    build_failure, empty_change_report, mode_name, resource_failure, AppliedChange, BranchReport,
    ChangeFailure, CloudPublicationInput, CommitReport, IndexedChange, IndexedFailure,
    MintedPostSealState, PersistedFileChange,
};
use super::target::{target_family, TargetFamily};
use crate::mcp::types::{ChangeMode, ChangePayload, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::resources::mutations::{
    blocking_append_kind, dispatch_resource_change, project_write_crossing_path,
    run_blocking_group, validate_blocking_group, PromotedMemoryRef,
};
use cairn_common::authorization::AuthorityRequest;

/// A single-failure change report for an item refused before anything applied.
fn deny_change(changes: &[crate::mcp::types::ChangeItem], index: usize, message: String) -> String {
    let IndexedFailure { failure, .. } = *build_failure(index, &changes[index], message);
    serde_json::to_string(&empty_change_report(
        Vec::new(),
        vec![failure],
        None,
        false,
        false,
    ))
    .unwrap_or_else(|e| format!("Failed to serialize change report: {e}"))
}

async fn rollback_unsealed_mint(
    orch: &Orchestrator,
    minted: Option<&super::branch::MintedThreadBranch>,
) {
    if let Some(minted) = minted {
        if let Err(error) = super::branch::rollback_thread_branch_mint(orch, minted).await {
            log::error!(
                "failed to compensate unsealed thread branch {}: {error}",
                minted.git_ref
            );
        }
    }
}

async fn complete_minted_branch_pr(
    orch: &Orchestrator,
    payload: &ChangePayload,
    index: usize,
    job_id: &str,
    resolution: &super::branch::BranchResolution,
    git_ref: &str,
    number: i64,
) -> Result<String, String> {
    let pr = payload.changes[index]
        .payload
        .as_ref()
        .and_then(|value| value.get("pr"))
        .ok_or("mint payload has no PR request")?;
    let title = payload.changes[index]
        .payload
        .as_ref()
        .and_then(|value| value.get("title"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| format!("Thread branch {number}"));
    let body = pr.get("body").and_then(serde_json::Value::as_str);
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id).await?;
    let url = crate::execution::actions::open_pr_for_job(
        orch,
        &db,
        job_id,
        &resolution.project_id,
        None,
        git_ref,
        None,
        &title,
        body,
    )
    .await?;
    crate::mcp::handlers::comments_artifacts::store_create_pr_artifact_for_job(
        orch, job_id, &title, body,
    )
    .await?;
    db.execute(
        "UPDATE thread_branches SET stage = 'pr-open', moved_at = unixepoch() WHERE job_id = ?1",
        (job_id.to_string(),),
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(url)
}

async fn checkpoint_minted_post_seal(
    db: &crate::storage::LocalDb,
    call: &replay::WriteCall,
    owner: &replay::ReplayOwner,
    report: &self::types::ChangeReport,
    phase: &str,
) -> Result<(), String> {
    let checkpoint = serde_json::to_string(report)
        .map_err(|error| format!("failed to serialize {phase} checkpoint: {error}"))?;
    if replay::checkpoint_owned_report(db, call, owner, &checkpoint).await {
        Ok(())
    } else {
        Err(format!("lost replay ownership while checkpointing {phase}"))
    }
}

#[allow(clippy::too_many_arguments)]
async fn finish_minted_post_seal(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &ChangePayload,
    index: usize,
    resolution: &super::branch::BranchResolution,
    job_id: &str,
    call: &replay::WriteCall,
    owner: &replay::ReplayOwner,
    mut report: self::types::ChangeReport,
) -> Result<String, String> {
    let state = report.post_seal.as_mut().ok_or_else(|| {
        "the sealed minted replay has no persisted post-seal continuation inputs".to_string()
    })?;

    if !state.ledger_recorded {
        for (change_index, change) in state.file_changes.iter().enumerate() {
            record_agent_file_change_once(
                orch,
                request,
                &call.file_change_effect_id(change_index),
                &change.path,
                &change.status,
                change.additions,
                change.deletions,
            )
            .await
            .map_err(|error| format!("failed to record agent file change: {error}"))?;
        }
        state.ledger_recorded = true;
        checkpoint_minted_post_seal(&orch.db.local, call, owner, &report, "file-change ledger")
            .await?;
    }

    crate::orchestrator::base_advance::publish_managed_branch(
        orch,
        &resolution.repository_path,
        &resolution.rev,
    )
    .await
    .map_err(|error| {
        format!(
            "failed to publish sealed branch: {error}. The commit is sealed in Cairn's store; the \
             open pull request still shows the previous head. {}",
            super::PUBLISH_ACTION_REMEDY
        )
    })?;

    let state = report.post_seal.as_mut().expect("post-seal state");
    if !state.cloud_published {
        if let Some(publication) = state.cloud.as_ref() {
            let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id).await?;
            if let Err(error) = crate::mcp::vcs::publish_sealed_commit_pack(
                &db,
                &publication.project_id,
                &publication.repository,
                report
                    .commit
                    .as_ref()
                    .and_then(|commit| commit.sha.as_deref())
                    .ok_or("post-seal publication has no sealed sha")?,
            )
            .await
            {
                let warning = format!("sealed commit cloud publication failed: {error}");
                if let Some(commit) = report.commit.as_mut() {
                    commit.message = Some(match commit.message.take() {
                        Some(message) => format!("{message}; {warning}"),
                        None => warning,
                    });
                }
            }
        }
        report
            .post_seal
            .as_mut()
            .expect("post-seal state")
            .cloud_published = true;
        checkpoint_minted_post_seal(&orch.db.local, call, owner, &report, "cloud publication")
            .await?;
    }

    if report
        .branch
        .as_ref()
        .is_some_and(|branch| branch.stage != "pr-open")
    {
        let branch = report.branch.as_ref().expect("minted branch report");
        let url = complete_minted_branch_pr(
            orch,
            payload,
            index,
            job_id,
            resolution,
            &branch.git_ref,
            branch.number,
        )
        .await
        .map_err(|error| format!("failed to complete pull request: {error}"))?;
        let branch = report.branch.as_mut().expect("minted branch report");
        branch.stage = "pr-open".to_string();
        branch.pr_url = Some(url);
        checkpoint_minted_post_seal(&orch.db.local, call, owner, &report, "PR completion").await?;
    }

    let new_tip = report.commit.as_ref().and_then(|report| report.sha.clone());
    if let (Some(commit), Some(new_tip)) = (report.commit.as_mut(), new_tip) {
        let sync_failures = crate::dev_instances::sync_live_branch_instances(
            orch,
            &resolution.project_id,
            &resolution.rev,
            &new_tip,
        )
        .await;
        if !sync_failures.is_empty() {
            let warning = format!(
                "live dev instance sync blocked ({} instance(s)); the owner received a blocking notice",
                sync_failures.len()
            );
            commit.message = Some(match commit.message.take() {
                Some(message) => format!("{message}; {warning}"),
                None => warning,
            });
        }
    }

    let run_context = check_run_context_for_destination(
        crate::mcp::handlers::run_context::lookup_run(&orch.db.local, request)
            .await
            .ok(),
        Some(job_id),
    );
    if let Some(context) = run_context.as_ref() {
        crate::execution::checks::cancel_stale_review_on_branch_advance(orch, &context.job_id)
            .await;
    }
    let check_summary = crate::execution::checks::run_write_checks_after_seal(
        orch,
        run_context.as_ref(),
        &request.cwd,
        call.tool_use_id(),
        Some(resolution),
    )
    .await;
    report.post_seal = None;
    Ok(append_check_summary(
        serde_json::to_string(&report)
            .unwrap_or_else(|error| format!("Failed to serialize change report: {error}")),
        &check_summary,
    ))
}

/// Build a single-failure change report carrying the rendered multi-error
/// validation message. The validator reports every problem at once; this maps
/// that into the report's `failures` shape, keyed to the first offending item so
/// the index/target/mode fields point somewhere useful.
fn validation_failure_report(
    request: &McpCallbackRequest,
    errors: &[cairn_common::change_validation::ChangeValidationError],
) -> String {
    let message = cairn_common::change_validation::render_validation_errors(errors);
    let first_index = errors.iter().find_map(|e| e.index);
    let (target, mode) = first_index
        .and_then(|i| {
            request
                .payload
                .get("changes")
                .and_then(|c| c.as_array())
                .and_then(|a| a.get(i))
        })
        .map(|item| {
            (
                item.get("target")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                item.get("mode")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
            )
        })
        .unwrap_or_default();

    let failure = ChangeFailure {
        index: first_index.unwrap_or(0),
        target,
        mode,
        kind: "validation".to_string(),
        error: message,
    };
    serde_json::to_string(&empty_change_report(
        Vec::new(),
        vec![failure],
        None,
        false,
        false,
    ))
    .unwrap_or_else(|e| format!("Failed to serialize change report: {e}"))
}

fn change_report_json(
    applied: Vec<AppliedChange>,
    failures: Vec<ChangeFailure>,
    commit: Option<CommitReport>,
    transactional: bool,
) -> String {
    let partial_success = !applied.is_empty() && !failures.is_empty();
    serde_json::to_string(&empty_change_report(
        applied,
        failures,
        commit,
        partial_success,
        transactional,
    ))
    .unwrap_or_else(|e| format!("Failed to serialize change report: {e}"))
}

/// Append the synchronous when:write check-runner summary (when present) to a
/// tool result, separated by a blank line. A no-op when no checks applied.
fn append_check_summary(text: String, summary: &Option<String>) -> String {
    match summary {
        Some(s) => format!("{text}\n\n{s}"),
        None => text,
    }
}

/// A thread session may advance a numbered branch, but the branch's backing job
/// owns the resulting check wave. Preserve the authenticated run as the stream
/// destination while replacing only the durable job coordinate used for check
/// baselines, observations, cancellation, and `/checks` reads.
fn check_run_context_for_destination(
    mut context: Option<super::RunContext>,
    destination_job_id: Option<&str>,
) -> Option<super::RunContext> {
    if let (Some(context), Some(destination_job_id)) = (context.as_mut(), destination_job_id) {
        context.job_id = destination_job_id.to_string();
    }
    context
}

async fn rollback_promoted_memory_decisions(
    orch: &Orchestrator,
    promoted_memories: &[(usize, String, String, PromotedMemoryRef)],
) {
    let ids: Vec<String> = promoted_memories
        .iter()
        .map(|(_, _, _, promoted)| promoted.memory_id.clone())
        .collect();
    if let Err(error) = crate::memories::db::clear_triage_decisions(&orch.db.local, &ids).await {
        log::warn!("Failed to roll back rejected memory promote decisions: {error}");
    }
}

fn apply_file_changes<'a>(
    request: &McpCallbackRequest,
    batch: &'a [IndexedChange<'a>],
    allow_escape: bool,
    atomic: bool,
    logical_snapshot: &mut std::collections::HashMap<String, Option<String>>,
) -> Vec<Result<FileBatchSuccess, Box<IndexedFailure>>> {
    if atomic {
        let result = apply_logical_file_batch(request, batch, allow_escape, logical_snapshot);
        if let Ok(success) = &result {
            apply_snapshot_mutations(logical_snapshot, &success.logical_mutations);
        }
        return vec![result];
    }
    let mut results = Vec::with_capacity(batch.len());
    for change in batch {
        let result = apply_logical_file_batch(
            request,
            &[IndexedChange {
                index: change.index,
                item: change.item,
            }],
            allow_escape,
            logical_snapshot,
        );
        if let Ok(success) = &result {
            apply_snapshot_mutations(logical_snapshot, &success.logical_mutations);
        }
        results.push(result);
    }
    results
}

fn apply_snapshot_mutations(
    snapshot: &mut std::collections::HashMap<String, Option<String>>,
    mutations: &[cairn_vcs::LogicalTreeMutation],
) {
    for mutation in mutations {
        snapshot.insert(
            format!("file:{}", mutation.path),
            mutation
                .content
                .as_ref()
                .map(|bytes| String::from_utf8_lossy(bytes).into_owned()),
        );
    }
}

pub async fn handle_write(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let mut drained = Vec::new();
    let report = handle_write_under_barrier(orch, request, &mut drained).await;
    if drained.is_empty() {
        return report;
    }
    // The store lock was released when the write returned, so the held
    // commits' remaining rungs (the origin push among them) can run now.
    attach_held_notes(
        report,
        super::run::held::settle_drained(orch, drained).await,
    )
}

/// Carry what the write's commit barrier republished in its change report.
fn attach_held_notes(report: String, notes: Vec<String>) -> String {
    match serde_json::from_str::<self::types::ChangeReport>(&report) {
        Ok(mut parsed) => {
            parsed.held_publications = notes;
            serde_json::to_string(&parsed).unwrap_or(report)
        }
        Err(_) => format!("{}\n\n{report}", notes.join("\n")),
    }
}

/// A parking point an integration test can put right after an optimistic
/// write's unlocked replay-ledger lookup, to make a second delivery of the
/// same call seal in exactly that window.
#[cfg(feature = "test-utils")]
pub mod test_pause {
    use std::collections::HashMap;
    use std::sync::{Arc, LazyLock, Mutex};
    use tokio::sync::Notify;

    type Pause = (Arc<Notify>, Arc<Notify>);
    static PAUSES: LazyLock<Mutex<HashMap<String, Pause>>> = LazyLock::new(Default::default);

    /// Park the next optimistic delivery of `tool_use_id` after its replay
    /// lookup. Returns `(reached, resume)`: `reached` is notified when it
    /// parks, and notifying `resume` lets it continue.
    pub fn after_replay_lookup(tool_use_id: &str) -> Pause {
        let pause = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        PAUSES
            .lock()
            .unwrap()
            .insert(tool_use_id.to_string(), pause.clone());
        pause
    }

    pub(super) async fn reached(tool_use_id: Option<&str>) {
        let Some(tool_use_id) = tool_use_id else {
            return;
        };
        let pause = PAUSES.lock().unwrap().remove(tool_use_id);
        if let Some((reached, resume)) = pause {
            reached.notify_one();
            resume.notified().await;
        }
    }
}

/// How a file write serializes against other writers of its store.
#[derive(Clone, Copy, PartialEq, Eq)]
enum WriteLocking {
    /// Prepare against the head read without the lock, and take the lock only
    /// to seal, with that head as the expected one. Used only for batches
    /// whose preparation has no side effects, so a seal that finds the head
    /// moved can discard everything and run again locked.
    Optimistic,
    /// Hold the lock from head resolution through the seal.
    Locked,
}

/// Whether an optimistic attempt's outcome has been checked against the
/// replay ledger under the store lock (I5).
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplayEpoch {
    /// The attempt was not optimistic, so it held the lock throughout.
    Locked,
    /// Optimistic, and it ended before any locked re-check: a preparation
    /// failure, or nothing applied. Another delivery of the same call may have
    /// sealed meanwhile, so its report is not yet the answer.
    Unchecked,
    /// Optimistic, and its outcome is settled: it replayed a recorded report,
    /// re-entered locked, or sealed after its locked re-checks passed.
    Checked,
}

/// The write itself. A file write is a commit barrier on its branch: the
/// branch's held deltas are republished into `drained` under the same store
/// lock, before the head this write builds on is read.
async fn handle_write_under_barrier(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    drained: &mut Vec<super::run::held::Drained>,
) -> String {
    let mut epoch = ReplayEpoch::Locked;
    // Boxed: the attempt future carries every preparation and sealing state of
    // the write, and awaiting it inline puts all of that on the caller's
    // stack, which is how write-driven tests overflowed a worker stack.
    let report = Box::pin(handle_write_attempt(
        orch,
        request,
        drained,
        WriteLocking::Optimistic,
        &mut epoch,
    ))
    .await;
    if epoch != ReplayEpoch::Unchecked {
        return report;
    }
    // The optimistic attempt read the replay ledger without the lock and then
    // returned before sealing. A second delivery of a call whose first
    // delivery sealed in between would otherwise report what it found on the
    // moved head — an anchor that no longer matches — instead of the recorded
    // success. Re-read the ledger under the lock; if a record appeared, the
    // locked attempt replays it.
    let Some(call) = replay::invoking_write_call(orch, request).await else {
        return report;
    };
    let store = crate::mcp::vcs::resolve_store_lock(orch, request).await;
    let guard = crate::mcp::vcs::acquire_store_lock(
        orch,
        store.as_deref(),
        crate::orchestrator::StoreLockClass::Foreground,
        "logical write transaction",
        crate::mcp::vcs::STORE_LOCK_TIMEOUT,
    )
    .await;
    let replayed = match guard {
        Ok(_) => replay::recorded_report(&orch.db.local, &call)
            .await
            .is_some(),
        // The epoch could not be checked; the locked attempt waits for the
        // lock itself and reports a busy store if it cannot get it either.
        Err(_) => true,
    };
    drop(guard);
    if !replayed {
        return report;
    }
    Box::pin(handle_write_attempt(
        orch,
        request,
        drained,
        WriteLocking::Locked,
        &mut epoch,
    ))
    .await
}

/// One attempt at the write. An optimistic attempt that meets a moved head,
/// held work, or a replay record at its seal re-enters here locked: nothing it
/// did before that point is visible, so the locked attempt produces exactly
/// the report a fully locked write would have.
async fn handle_write_attempt(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    drained: &mut Vec<super::run::held::Drained>,
    locking: WriteLocking,
    epoch: &mut ReplayEpoch,
) -> String {
    // Authoritative validation gate, shared with cairn-cmd's pre-flight check so
    // the contract and error text are identical everywhere. Collects every
    // blocking problem (structural shape, target/mode, and the commit_msg
    // requirement) in one pass before any typed deserialize or side effect.
    // committing is non-optional for file-target changes: file edits live only in
    // the worktree until committed, so a missing commit_msg is rejected here too.
    let validation_errors =
        cairn_common::change_validation::validate_change_value(&request.payload);
    if !validation_errors.is_empty() {
        return validation_failure_report(request, &validation_errors);
    }

    // After validation passes this deserialize is an invariant, not the
    // user-facing gate; the `Invalid payload` branch is a defensive
    // internal-error path.
    let mut payload: ChangePayload = match crate::mcp::handlers::parse_payload(request) {
        Ok(payload) => payload,
        Err(error) => return error,
    };

    // Resolve home-relative (`cairn:~/...`) targets to canonical up front, so
    // blocking-append classification — which runs on the raw target before the
    // dispatch that would otherwise resolve it — sees the real resource. All
    // clients forward `cairn:~/` raw, so the run's home is looked up here and
    // carried across the batch: an item that renames this run's own thread moves
    // the home the items after it name, and the record this returns is what lets
    // a failure be reported in the shorthand the caller wrote. See
    // [`home_targets`].
    let home_targets = home_targets::resolve(orch, request, &mut payload.changes).await;

    let mint_indices = payload
        .changes
        .iter()
        .enumerate()
        .filter_map(|(index, item)| {
            if item.mode != ChangeMode::Append {
                return None;
            }
            // The common parser stops a thread URI at `Thread { path }`; the
            // resolver is the single reader that knows `branches` names the
            // mint destination. Same call dispatch makes, one rung below.
            let parsed = cairn_common::uri::parse_uri(&item.target)?;
            match crate::resources::delegate_thread_descendant(parsed) {
                Ok(cairn_common::uri::CairnResource::ThreadBranches { .. }) => Some(index),
                _ => None,
            }
        })
        .collect::<Vec<_>>();
    let has_file_targets = payload
        .changes
        .iter()
        .any(|item| matches!(target_family(&item.target), Ok(TargetFamily::File)));
    if mint_indices.len() > 1 && has_file_targets {
        return deny_change(&payload.changes, mint_indices[1],
            "Refusing thread branch mint: this batch names more than one branch destination, so its file commit would be ambiguous. Keep exactly one append to the thread's branches collection.".to_string());
    }
    if !mint_indices.is_empty() && payload.branch.is_some() {
        return deny_change(&payload.changes, mint_indices[0],
            "Refusing thread branch write: the branches append and batch-level branch: name two destinations. Remove branch: to mint, or remove the append to commit onto the existing branch.".to_string());
    }
    let mint_index = mint_indices.first().copied();
    if let Some(index) = mint_index {
        let has_pr = payload.changes[index]
            .payload
            .as_ref()
            .and_then(|value| value.get("pr"))
            .and_then(serde_json::Value::as_object)
            .is_some();
        if !has_pr {
            return deny_change(&payload.changes, index,
                "Refusing an accumulating branch birth: this bottom rung requires payload.pr to be an object and opens the branch through PR review. Bare accumulating branches belong to the sibling workflow.".to_string());
        }
        if has_pr && !has_file_targets {
            return deny_change(&payload.changes, index,
                "Refusing to open a thread branch PR without file edits: a PR needs a commit. Include the file changes and commit_msg in this batch.".to_string());
        }
        if let Some(blocking_index) = payload
            .changes
            .iter()
            .position(|item| blocking_append_kind(item).is_some())
        {
            return deny_change(
                &payload.changes,
                blocking_index,
                "Refusing a blocking task or question in a minted-branch batch: crash recovery cannot safely repeat an unanswered blocking append after the commit is sealed. Mint the branch first, then send the blocking append in a separate write.".to_string(),
            );
        }
        if let Some(memory_index) = payload.changes.iter().position(|item| {
            item.payload
                .as_ref()
                .and_then(|value| value.get("action"))
                .and_then(serde_json::Value::as_str)
                == Some("promote")
        }) {
            return deny_change(
                &payload.changes,
                memory_index,
                "Refusing memory promotion in a minted-branch batch: crash recovery cannot reconstruct the promotion bookkeeping after the commit is sealed. Promote the memory in a separate write; ordinary resource mutations remain supported alongside minting.".to_string(),
            );
        }
    }

    // Direct-to-base remains forbidden. A single mint append licenses the
    // commit because its destination is allocated after preview and before the
    // logical head is resolved.
    if payload.commit_msg.is_some() && payload.branch.is_none() && mint_index.is_none() {
        if let Some(refusal) =
            crate::mcp::handlers::run_context::commit_posture_refusal(&orch.db, request).await
        {
            return refusal;
        }
    }

    if let Some(result) = handle_apply_change(orch, request, &payload).await {
        return result;
    }

    // Rename items default to the preview path: a bare `mode:"rename"` call
    // computes the edit set and returns it (plus apply_uri) WITHOUT mutating, so
    // the agent never has to pass preview:true. The `mode:"apply"` round-trip
    // re-runs the stored call with preview=Some(false), which falls through to
    // the mutating path below. An explicit `preview:false` is the one-shot-apply
    // escape hatch.
    if payload.preview.is_none()
        && payload
            .changes
            .iter()
            .any(|item| item.mode == ChangeMode::Rename)
    {
        return preview_change(orch, request, &payload).await;
    }

    if payload.preview.unwrap_or(false) {
        return preview_change(orch, request, &payload).await;
    }

    log::info!(
        "change for cwd={}, {} changes",
        request.cwd,
        payload.changes.len()
    );

    // Resolve what this batch would actually DO, before any of it happens.
    //
    // Ahead of the fence on purpose. Whether a change may cross a filesystem
    // boundary and whether it may rewrite this workspace's capability set are
    // different questions with different answers, and asking them in this order
    // means an agent under `fence: ask` is refused for the real reason instead
    // of first being prompted about a host path it was never going to be
    // allowed to write. It also means `fence: allow` skips nothing: containment
    // policy relaxes containment, never authority.
    let prepared =
        crate::authorization::prepare::prepare_batch(orch, request, &payload.changes).await;
    for (index, outcome) in &prepared {
        if let crate::authorization::prepare::Prepared::Refused(message) = outcome {
            return deny_change(&payload.changes, *index, message.clone());
        }
    }

    // Logical namespace fence (writes). Repository-relative names are always
    // handled by the runner logical-head transaction and never joined to cwd.
    // Detect explicit host paths up front
    // and gate it BEFORE applying anything, so a suspend leaves no partial side
    // effects and the slow-path resume can safely re-drive the whole batch.
    // `allow_escape` then permits the approved escaping write(s) on the apply
    // path. Resolution is once per call:
    //   - no active run -> host-path writes refused
    //   - fence allow -> writes anywhere, no fence
    //   - fence ask/deny -> fence each escaping target; allow_escape iff any
    //     escaping target was approved.
    let allow_escape = {
        use crate::mcp::handlers::fence;
        use crate::models::Fence;
        // Resource-only changes (cairn:// mutations, todos, return, messages)
        // never reach the filesystem, so they never touch the fence path.
        let has_file_target = payload
            .changes
            .iter()
            .any(|item| matches!(target_family(&item.target), Ok(TargetFamily::File)));
        if !has_file_target {
            false
        } else {
            match fence::resolve_run_fence(orch, request).await {
                fence::RunFenceResolution::Ambient => false,
                fence::RunFenceResolution::Unavailable(error) => return error,
                fence::RunFenceResolution::Resolved(_, Fence::Allow) => true,
                fence::RunFenceResolution::Resolved(
                    run_id,
                    fence_mode @ (Fence::Ask | Fence::Deny),
                ) => {
                    let residence = std::path::Path::new(&request.cwd);
                    let mut any_escape = false;
                    for (index, item) in payload.changes.iter().enumerate() {
                        if !matches!(target_family(&item.target), Ok(TargetFamily::File)) {
                            continue;
                        }
                        if crate::mcp::file_targets::resolve_logical_file_target(&item.target)
                            .is_ok()
                        {
                            continue;
                        }
                        let Ok(normalized) =
                            crate::mcp::file_targets::normalize_change_target(&item.target, true)
                        else {
                            continue; // invalid target — the apply path reports it
                        };
                        let Ok(full) = crate::mcp::file_targets::resolve_change_target(
                            residence,
                            &normalized,
                            true,
                        ) else {
                            continue;
                        };
                        // Anything not classified as a logical target must be an
                        // explicit absolute host path. Traversal and malformed
                        // targets fail later without receiving a capability.
                        if !full.is_absolute() {
                            continue;
                        }
                        // Temp dirs + toolchain caches are in the sandbox
                        // writable set, so a structured write there is
                        // in-sandbox (parity with a shell write under `run`) and
                        // takes no prompt. Mark the escape so the apply path
                        // permits the absolute target, but don't raise the fence.
                        if crate::mcp::file_targets::path_within_any(
                            &full,
                            &crate::services::sandbox::default_writable_extra(),
                        ) {
                            any_escape = true;
                            continue;
                        }
                        any_escape = true;
                        match fence::raise_fence(
                            orch,
                            &run_id,
                            fence_mode,
                            request,
                            fence::Crossing::write_outside(&full),
                        )
                        .await
                        {
                            fence::FenceDecision::Allow => {}
                            fence::FenceDecision::Deny(msg) => {
                                let IndexedFailure { failure, .. } =
                                    *build_failure(index, item, msg);
                                return serde_json::to_string(&empty_change_report(
                                    Vec::new(),
                                    vec![failure],
                                    None,
                                    false,
                                    false,
                                ))
                                .unwrap_or_else(|e| {
                                    format!("Failed to serialize change report: {e}")
                                });
                            }
                            fence::FenceDecision::Unavailable(msg) => {
                                let IndexedFailure { failure, .. } =
                                    *build_failure(index, item, msg);
                                return serde_json::to_string(&empty_change_report(
                                    Vec::new(),
                                    vec![failure],
                                    None,
                                    false,
                                    false,
                                ))
                                .unwrap_or_else(|e| {
                                    format!("Failed to serialize change report: {e}")
                                });
                            }
                            fence::FenceDecision::Suspended => {
                                return "Change suspended pending logical fence approval; resume \
                                    will continue once it is answered."
                                    .to_string();
                            }
                        }
                    }
                    any_escape
                }
            }
        }
    };

    // Named authority boundaries for workspace configuration.
    //
    // A workspace settings write or a workspace MCP mutation changes what every
    // FUTURE agent in this workspace can do, which is an authority question,
    // not a containment one. This used to be gated by raising the fence on
    // `~/.cairn/settings.yaml` as though it were an ordinary host path escape;
    // that prompt could only say "a file outside the project is being written",
    // when the thing the operator actually needed to decide was whether to hand
    // over a capability. The authorization service names the scope instead, and
    // the approval it produces is journaled, listable, and revocable.
    //
    // Raised before any change applies, so a suspend leaves no partial side
    // effects. Each authority-bearing section or server is adjudicated in turn;
    // a suspend re-drives the whole batch on resume, at which point the granted
    // one passes and the next unapproved one prompts.
    let authority_requests: Vec<(usize, Result<AuthorityRequest, String>)> = prepared
        .into_iter()
        .filter_map(|(index, outcome)| match outcome {
            crate::authorization::prepare::Prepared::Authority(request) => {
                Some((index, Ok(request)))
            }
            crate::authorization::prepare::Prepared::Invalid(error) => Some((index, Err(error))),
            // Already returned above; nothing reaches here.
            crate::authorization::prepare::Prepared::Refused(_) => None,
        })
        .collect();
    let had_authority_request = !authority_requests.is_empty();
    if !authority_requests.is_empty() {
        use crate::mcp::handlers::authority::{self, AuthorityGate};
        let deny =
            |index: usize, msg: String| -> String { deny_change(&payload.changes, index, msg) };
        // An authenticated principal is the one hard requirement: a grant has to
        // be attributable to someone, so a request with no resolvable run is
        // refused rather than written.
        let Some(actor) = crate::authorization::resolve_actor(orch, request).await else {
            let index = authority_requests[0].0;
            return deny(
                index,
                "Denied: changing workspace settings or workspace MCP servers is a named \
                 authority boundary and requires an authenticated run to adjudicate. Use \
                 scope:\"project\" for the project's own .cairn/config.yaml."
                    .to_string(),
            );
        };
        // The fence policy answers only "is there anyone to ask". An
        // unresolvable policy is not evidence that nobody is there — a run
        // without an execution snapshot still has an operator — so it defaults
        // to asking. Hard-denying here would make the boundary unreachable for
        // such a run even when it holds a standing grant, which is the opposite
        // of what a grant is for.
        let fence_mode = match crate::mcp::handlers::fence::resolve_run_fence(orch, request).await {
            crate::mcp::handlers::fence::RunFenceResolution::Resolved(_, mode) => mode,
            crate::mcp::handlers::fence::RunFenceResolution::Unavailable(error) => return error,
            crate::mcp::handlers::fence::RunFenceResolution::Ambient => {
                return "permission context unavailable: authority approval requires an authenticated run".to_string()
            }
        };
        for (index, authority_request) in authority_requests {
            let authority_request = match authority_request {
                Ok(request) => request,
                // A target with no nameable place cannot be approved at all.
                Err(error) => return deny(index, format!("Refused: {error}")),
            };
            match authority::raise_authority(
                orch,
                &actor,
                fence_mode,
                request,
                "write",
                &authority_request,
            )
            .await
            {
                AuthorityGate::Allow => {}
                AuthorityGate::Deny(msg) => return deny(index, msg),
                AuthorityGate::Unavailable(msg) => return deny(index, msg),
                AuthorityGate::Suspended => {
                    return "Change suspended pending authority approval; resume will continue \
                        once it is answered."
                        .to_string();
                }
            }
        }
    }

    // Logical namespace fence for explicit repository-host operations. Creating
    // a project initializes an arbitrary `repoPath`, and attach-remote runs git on
    // the project's live repository. They route through the same fence keyed on the target repo
    // path. Pure-DB project writes (rename/hide, or a create with an empty
    // repoPath) return no crossing and skip this. Raised before any change
    // applies. Resolving the crossing for attach-remote needs a DB lookup, so
    // detection is async.
    let mut project_crossing: Option<(usize, std::path::PathBuf)> = None;
    for (index, item) in payload.changes.iter().enumerate() {
        if let Some(path) = project_write_crossing_path(orch, item).await {
            project_crossing = Some((index, path));
            break;
        }
    }
    if let Some((index, repo_path)) = project_crossing {
        use crate::mcp::handlers::fence;
        use crate::models::Fence;
        let item = &payload.changes[index];
        let deny = |msg: String| -> String {
            let IndexedFailure { failure, .. } = *build_failure(index, item, msg);
            serde_json::to_string(&empty_change_report(
                Vec::new(),
                vec![failure],
                None,
                false,
                false,
            ))
            .unwrap_or_else(|e| format!("Failed to serialize change report: {e}"))
        };
        match fence::resolve_run_fence(orch, request).await {
            fence::RunFenceResolution::Resolved(_, Fence::Allow) => {}
            fence::RunFenceResolution::Resolved(run_id, fence_mode @ (Fence::Ask | Fence::Deny)) => {
                match fence::raise_fence(
                    orch,
                    &run_id,
                    fence_mode,
                    request,
                    fence::Crossing::write_outside(&repo_path),
                )
                .await
                {
                    fence::FenceDecision::Allow => {}
                    fence::FenceDecision::Deny(msg) => return deny(msg),
                    fence::FenceDecision::Unavailable(msg) => return deny(msg),
                    fence::FenceDecision::Suspended => {
                        return "Change suspended pending logical namespace approval; resume \
                            will continue once it is answered."
                            .to_string();
                    }
                }
            }
            fence::RunFenceResolution::Unavailable(error) => return error,
            fence::RunFenceResolution::Ambient => return "permission context unavailable: creating a project or attaching a remote requires an authenticated run".to_string(),
        }
    }

    let has_file_targets = payload
        .changes
        .iter()
        .any(|item| matches!(target_family(&item.target), Ok(TargetFamily::File)));

    // Resolve the provider call before either serialization boundary: file
    // batches use the store lock, while resource-only batches claim the replay
    // ledger itself before dispatching any effects.
    let write_call = replay::invoking_write_call(orch, request).await;

    // The logical coordinate and every anchor evaluation must belong to the
    // same writer epoch as publication. A locked write holds the store lock
    // from here to the seal. An optimistic one reads the head without it and
    // takes the lock only to seal, re-reading the head, the replay ledger, and
    // the branch's held work under it first (I3, I5): any of them changed, and
    // the write runs again locked. Optimistic only where nothing before the
    // seal has a side effect — every item a file edit, no branch mint (which
    // needs the lock from the start), no host-path escape or authority prompt
    // that a second run would raise again.
    let optimistic = locking == WriteLocking::Optimistic
        && has_file_targets
        && mint_index.is_none()
        && !allow_escape
        && !had_authority_request
        && payload
            .changes
            .iter()
            .all(|item| matches!(target_family(&item.target), Ok(TargetFamily::File)));
    if optimistic {
        *epoch = ReplayEpoch::Unchecked;
    }
    let write_store = if has_file_targets {
        crate::mcp::vcs::resolve_store_lock(orch, request).await
    } else {
        None
    };
    let mut write_store_guard = if has_file_targets && !optimistic {
        let store = write_store.clone();
        match crate::mcp::vcs::acquire_store_lock(
            orch,
            store.as_deref(),
            crate::orchestrator::StoreLockClass::Foreground,
            "logical write transaction",
            crate::mcp::vcs::STORE_LOCK_TIMEOUT,
        )
        .await
        {
            Ok(guard) => guard,
            Err(error) => {
                return serde_json::to_string(&empty_change_report(
                    Vec::new(),
                    vec![ChangeFailure {
                        index: 0,
                        target: payload
                            .changes
                            .first()
                            .map(|change| change.target.clone())
                            .unwrap_or_default(),
                        mode: payload
                            .changes
                            .first()
                            .map(|change| mode_name(change.mode))
                            .unwrap_or("patch")
                            .to_string(),
                        kind: "file".to_string(),
                        error,
                    }],
                    None,
                    payload.atomic.unwrap_or(false),
                    false,
                ))
                .unwrap_or_else(|serialize_error| serialize_error.to_string());
            }
        }
    } else {
        None
    };
    let mut store_trace = write_store_guard.as_ref().map(|guard| guard.trace());

    // The replay guard. The lookup belongs inside the store-lock epoch that the
    // matching record is written in: that is what serializes two deliveries of
    // one call against each other, so the second cannot read an empty ledger
    // while the first is still publishing.
    if has_file_targets {
        if let Some(call) = write_call.as_ref() {
            if replay::recorded_report(&orch.db.local, call)
                .await
                .is_some()
            {
                // The row itself now owns completion. Release the repository-wide
                // lock before waiting on or resuming remote work for this call.
                drop(write_store_guard);
                *epoch = ReplayEpoch::Checked;
                match replay::await_file_replay_or_takeover(&orch.db.local, call).await {
                    replay::FileReplayClaim::Replayed(report) => {
                        return replay::mark_as_replay(&report);
                    }
                    replay::FileReplayClaim::TakenOver { owner, report } => {
                        let prior = match serde_json::from_str::<self::types::ChangeReport>(&report)
                        {
                            Ok(prior) => prior,
                            Err(error) => {
                                log::warn!("write replay recovery remains pending: the sealed replay report is invalid: {error}");
                                return "A prior delivery sealed this write, but its publication recovery did not complete; the pending replay remains retryable.".to_string();
                            }
                        };
                        // Ordinary writes historically publish a finalized replay row
                        // as soon as the commit is sealed. A row without minted-branch
                        // continuation state can only be an interrupted row written by
                        // the short-lived pending implementation. Settle that durable
                        // report instead of trying to run mint-only recovery against it.
                        if prior.branch.is_none() {
                            if replay::finalize_owned_report(&orch.db.local, call, &owner, &report)
                                .await
                            {
                                return replay::mark_as_replay(&report);
                            }
                            return "A prior delivery of this write is still completing; replay ownership could not be acquired safely.".to_string();
                        }
                        // Keep the resumable post-seal continuation off this
                        // handler's poll stack. `handle_write` is also the hot
                        // path for ordinary resource-only writes; embedding the
                        // mint recovery future made those calls overflow a Tokio
                        // worker stack before dispatch.
                        let recovery = Box::pin(async {
                            let index = mint_index.ok_or_else(|| {
                                "the sealed write has no resumable minted-branch operation"
                                    .to_string()
                            })?;
                            let branch = prior.branch.as_ref().ok_or_else(|| {
                                "the sealed write does not describe a minted branch".to_string()
                            })?;
                            let branch_uri = branch.uri.clone();
                            let (resolution, job_id) = super::branch::resolve_explicit_for_write(
                                orch,
                                request,
                                &branch_uri,
                            )
                            .await
                            .map_err(|error| {
                                format!("failed to resolve the sealed branch: {error}")
                            })?;
                            let job_id = job_id.ok_or_else(|| {
                                "the sealed branch no longer has its publication job".to_string()
                            })?;
                            finish_minted_post_seal(
                                orch,
                                request,
                                &payload,
                                index,
                                &resolution,
                                &job_id,
                                call,
                                &owner,
                                prior,
                            )
                            .await
                        });
                        match replay::with_owned_report_heartbeat(
                            &orch.db.local,
                            call,
                            &owner,
                            recovery,
                        )
                        .await
                        {
                            Ok(final_report) => {
                                if replay::finalize_owned_report(
                                    &orch.db.local,
                                    call,
                                    &owner,
                                    &final_report,
                                )
                                .await
                                {
                                    return replay::mark_as_replay(&final_report);
                                }
                            }
                            Err(error) => {
                                log::warn!("write replay recovery remains pending: {error}");
                            }
                        }
                        return "A prior delivery sealed this write, but its publication recovery did not complete; the pending replay remains retryable.".to_string();
                    }
                    replay::FileReplayClaim::Stalled => {
                        return "A prior delivery of this write is still completing; replay ownership could not be acquired safely.".to_string();
                    }
                }
            }
        }
    }

    #[cfg(feature = "test-utils")]
    if optimistic {
        test_pause::reached(request.tool_use_id.as_deref()).await;
    }

    let revert_commit = payload.changes.first().and_then(|change| {
        (change.mode == ChangeMode::Revert).then(|| {
            change
                .payload
                .as_ref()
                .and_then(|payload| payload.get("commit"))
                .and_then(serde_json::Value::as_str)
                .expect("validated revert carries payload.commit")
                .to_string()
        })
    });

    let mut destination_job_id = None;
    let mut minted_branch: Option<super::branch::MintedThreadBranch> = None;
    let mut minted_pr_url: Option<String> = None;
    let mut logical_resolution = if has_file_targets {
        let resolved = if let Some(index) = mint_index {
            let title = payload.changes[index]
                .payload
                .as_ref()
                .and_then(|value| value.get("title"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            match super::branch::mint_thread_branch_for_write(orch, request, title).await {
                Ok(minted) => {
                    destination_job_id = Some(minted.job_id.clone());
                    let resolution = minted.resolution.clone();
                    minted_branch = Some(minted);
                    Ok(resolution)
                }
                Err(error) => Err(error),
            }
        } else {
            match payload.branch.as_deref() {
                Some(branch) => {
                    match super::branch::resolve_explicit_for_write(orch, request, branch).await {
                        Ok((resolution, job_id)) => {
                            destination_job_id = job_id;
                            Ok(resolution)
                        }
                        Err(error) => Err(error),
                    }
                }
                None => super::branch::resolve_current_for_read(orch, request).await,
            }
        };
        match resolved {
            Ok(resolution) => Some(resolution),
            Err(error) => {
                return serde_json::to_string(&empty_change_report(
                    Vec::new(),
                    vec![ChangeFailure {
                        index: 0,
                        target: payload
                            .changes
                            .first()
                            .map(|change| change.target.clone())
                            .unwrap_or_default(),
                        mode: payload
                            .changes
                            .first()
                            .map(|change| mode_name(change.mode))
                            .unwrap_or("patch")
                            .to_string(),
                        kind: "file".to_string(),
                        error: format!("Resolve authoritative logical head for write: {error}"),
                    }],
                    None,
                    payload.atomic.unwrap_or(false),
                    false,
                ))
                .unwrap_or_else(|serialize_error| {
                    format!(
                        "Failed to serialize logical-head resolution failure: {serialize_error}"
                    )
                });
            }
        }
    } else {
        None
    };
    // Held work lands ahead of this write, and landing it needs the lock.
    if optimistic {
        if let Some(resolution) = logical_resolution.as_ref() {
            let held = super::run::held::held_rows(
                &orch.db.local,
                &resolution.project_id,
                &resolution.rev,
            )
            .await;
            if !held.is_ok_and(|rows| rows.is_empty()) {
                *epoch = ReplayEpoch::Checked;
                return Box::pin(handle_write_attempt(
                    orch,
                    request,
                    drained,
                    WriteLocking::Locked,
                    epoch,
                ))
                .await;
            }
        }
    }
    // A freshly minted branch has nothing held on it yet.
    if minted_branch.is_none() && !optimistic {
        if let Some(resolution) = logical_resolution.as_mut() {
            *drained = super::run::held::drain_held_publications(
                orch,
                &resolution.project_id,
                &resolution.rev,
                store_trace.as_ref(),
            )
            .await;
            // The write's expected-head check must see the head the drain
            // left, or it would refuse its own branch as moved.
            if super::run::held::any_landed(drained) {
                match cairn_vcs::resolve_coordinate(&resolution.repository_path, &resolution.rev)
                    .await
                {
                    Ok(head) => resolution.commit_id = head,
                    Err(error) => {
                        return serde_json::to_string(&empty_change_report(
                            Vec::new(),
                            vec![ChangeFailure {
                                index: 0,
                                target: payload
                                    .changes
                                    .first()
                                    .map(|change| change.target.clone())
                                    .unwrap_or_default(),
                                mode: payload
                                    .changes
                                    .first()
                                    .map(|change| mode_name(change.mode))
                                    .unwrap_or("patch")
                                    .to_string(),
                                kind: "file".to_string(),
                                error: format!(
                                    "Re-resolve the logical head after publishing held commits: {error}"
                                ),
                            }],
                            None,
                            payload.atomic.unwrap_or(false),
                            false,
                        ))
                        .unwrap_or_else(|serialize_error| serialize_error.to_string());
                    }
                }
            }
        }
    }
    let mut logical_snapshot = std::collections::HashMap::new();
    if revert_commit.is_none() {
        if let Some(resolution) = logical_resolution.as_ref() {
            let has_rename = payload.changes.iter().any(|change| {
                change.mode == ChangeMode::Rename
                    && matches!(target_family(&change.target), Ok(TargetFamily::File))
            });
            type LogicalSnapshot = Vec<(String, Option<Vec<u8>>)>;
            let snapshot_result: Result<LogicalSnapshot, String> = if has_rename {
                super::read::files_at_commit(
                    resolution.object_repository_path.clone(),
                    resolution.commit_id.clone(),
                )
                .map(|files| {
                    files
                        .into_iter()
                        .map(|(path, bytes)| (path, Some(bytes)))
                        .collect()
                })
            } else {
                let indexed = payload
                    .changes
                    .iter()
                    .enumerate()
                    .filter(|(_, change)| {
                        matches!(target_family(&change.target), Ok(TargetFamily::File))
                    })
                    .map(|(index, item)| IndexedChange { index, item })
                    .collect::<Vec<_>>();
                file_mutations::logical_paths_for_changes(
                    std::path::Path::new(&request.cwd),
                    &indexed,
                    allow_escape,
                )
                .and_then(|paths| {
                    paths
                        .into_iter()
                        .map(|path| {
                            super::read::file_at_commit(
                                resolution.object_repository_path.clone(),
                                resolution.commit_id.clone(),
                                &path,
                            )
                            .map(|bytes| (path, bytes))
                        })
                        .collect()
                })
            };
            match snapshot_result {
                Ok(files) => {
                    for (path, bytes) in files {
                        let content = bytes
                            .map(String::from_utf8)
                            .transpose()
                            .map_err(|_| format!("File `{path}` is not valid UTF-8"));
                        match content {
                            Ok(content) => {
                                logical_snapshot.insert(format!("file:{path}"), content);
                            }
                            Err(error) => {
                                rollback_unsealed_mint(orch, minted_branch.as_ref()).await;
                                return serde_json::to_string(&empty_change_report(
                                Vec::new(),
                                vec![ChangeFailure {
                                    index: 0,
                                    target: format!("file:{path}"),
                                    mode: "write".to_string(),
                                    kind: "file".to_string(),
                                    error,
                                }],
                                None,
                                payload.atomic.unwrap_or(false),
                                false,
                            ))
                            .unwrap_or_else(|serialize_error| format!("Failed to serialize logical-head snapshot failure: {serialize_error}"));
                            }
                        }
                    }
                }
                Err(error) => {
                    rollback_unsealed_mint(orch, minted_branch.as_ref()).await;
                    return serde_json::to_string(&empty_change_report(
                        Vec::new(),
                        vec![ChangeFailure {
                            index: 0,
                            target: payload
                                .changes
                                .first()
                                .map(|change| change.target.clone())
                                .unwrap_or_default(),
                            mode: payload
                                .changes
                                .first()
                                .map(|change| mode_name(change.mode))
                                .unwrap_or("patch")
                                .to_string(),
                            kind: "file".to_string(),
                            error: format!("Read authoritative logical head for write: {error}"),
                        }],
                        None,
                        payload.atomic.unwrap_or(false),
                        false,
                    ))
                    .unwrap_or_else(|serialize_error| {
                        format!(
                            "Failed to serialize logical-head snapshot failure: {serialize_error}"
                        )
                    });
                }
            }
        }
    }

    // Blocking appends (task/question collections) are deferred to the end of the
    // call and run as a single group; everything else applies first in order.
    let mut blocking_indices: Vec<usize> = payload
        .changes
        .iter()
        .enumerate()
        .filter(|(_, item)| blocking_append_kind(item).is_some())
        .map(|(index, _)| index)
        .collect();
    let blocking_kind = match validate_blocking_group(&payload.changes, &blocking_indices) {
        Ok(kind) => kind,
        Err(error) => {
            let index = blocking_indices.first().copied().unwrap_or(0);
            rollback_unsealed_mint(orch, minted_branch.as_ref()).await;
            return serde_json::to_string(&empty_change_report(
                Vec::new(),
                vec![ChangeFailure {
                    index,
                    target: payload
                        .changes
                        .get(index)
                        .map(|item| item.target.clone())
                        .unwrap_or_default(),
                    mode: "append".to_string(),
                    kind: "resource".to_string(),
                    error,
                }],
                None,
                false,
                false,
            ))
            .unwrap_or_else(|e| format!("Failed to serialize change report: {e}"));
        }
    };

    let atomic = payload.atomic.unwrap_or(false);
    let guard_resource_report = !has_file_targets && write_call.is_some();
    if let Some(call) = write_call.as_ref().filter(|_| !has_file_targets) {
        match replay::claim_resource_call(&orch.db.local, call).await {
            replay::ResourceClaim::Claimed => {}
            replay::ResourceClaim::Replayed(report) => return replay::mark_as_replay(&report),
            replay::ResourceClaim::Stalled => {
                return change_report_json(
                    Vec::new(),
                    vec![ChangeFailure {
                        index: 0,
                        target: payload
                            .changes
                            .first()
                            .map(|change| change.target.clone())
                            .unwrap_or_default(),
                        mode: payload
                            .changes
                            .first()
                            .map(|change| mode_name(change.mode))
                            .unwrap_or("append")
                            .to_string(),
                        kind: "replay".to_string(),
                        error: "A previous delivery claimed this resource write but did not settle within 30 seconds. Nothing was applied again because the first delivery may already have produced side effects."
                            .to_string(),
                    }],
                    None,
                    atomic,
                );
            }
            replay::ResourceClaim::Unguarded => {}
        }
    }
    let mut applied: Vec<AppliedChange> = Vec::new();
    let mut failures: Vec<ChangeFailure> = Vec::new();
    let mut commit: Option<CommitReport> = None;
    let mut affected_paths: Vec<String> = Vec::new();
    let mut recorded_changes = Vec::new();
    let mut logical_mutations = Vec::new();
    let mut first_file_change: Option<IndexedChange<'_>> = None;
    let mut promoted_memories: Vec<(usize, String, String, PromotedMemoryRef)> = Vec::new();
    let mut index = 0;

    while index < payload.changes.len() {
        let item = &payload.changes[index];

        // An item whose `cairn:~` shorthand never reached a real resource — the
        // home lookup failed, or the rename it resolved through did not apply —
        // is refused here, in the vocabulary of the shorthand the caller wrote.
        // Dispatching it would instead fail downstream against a canonical URI
        // the caller never typed, in whatever terms that dispatcher happens to
        // use. A refused item also leaves the deferred blocking group, so a
        // target that resolves to nothing is not run at the end of the call.
        if let Some(error) = home_targets.blocked(index, &applied) {
            blocking_indices.retain(|blocked| *blocked != index);
            let IndexedFailure { failure, .. } = *build_failure(index, item, error);
            if atomic {
                let report = change_report_json(applied, vec![failure], commit, atomic);
                if guard_resource_report {
                    replay::finalize_report(
                        &orch.db.local,
                        write_call.as_ref().expect("guarded resource call"),
                        &report,
                    )
                    .await;
                }
                rollback_unsealed_mint(orch, minted_branch.as_ref()).await;
                return report;
            }
            failures.push(failure);
            index += 1;
            continue;
        }

        if item.mode == ChangeMode::Revert {
            first_file_change = Some(IndexedChange { index, item });
            applied.push(AppliedChange {
                index,
                target: item.target.clone(),
                mode: mode_name(item.mode).to_string(),
                kind: "file".to_string(),
                summary: format!(
                    "Revert {}",
                    revert_commit.as_deref().expect("revert item has commit")
                ),
                data: None,
            });
            index += 1;
            continue;
        }

        // Rename is its own branch: its target is a file URI (so the contiguous
        // File slice below would otherwise swallow it), but the edit set is
        // computed out of band by the structural engine, not the textual file path.
        // A `cairn://` resource target with mode=rename is not a structural rename;
        // let it fall through to the resource dispatch, which rejects it through the
        // contract gate as an unsupported mutation rather than a rename-payload error.
        if item.mode == ChangeMode::Rename
            && matches!(target_family(&item.target), Ok(TargetFamily::File))
        {
            let worktree = std::path::Path::new(&request.cwd);
            let rename_result = match file_mutations::parse_rename_spec(worktree, item) {
                Ok((_route_file, spec, new_name)) => {
                    let files = logical_snapshot
                        .iter()
                        .filter_map(|(target, content)| {
                            content.as_ref().map(|content| {
                                (
                                    worktree.join(target.strip_prefix("file:").unwrap_or(target)),
                                    content.clone(),
                                )
                            })
                        })
                        .collect::<Vec<_>>();
                    crate::symbols::rename::compute_plan_from_files(&files, spec, &new_name)
                }
                Err(error) => Err(error),
            };
            match rename_result {
                Ok(plan) => {
                    let (prepared, rename_applied, summaries) =
                        file_mutations::prepare_rename_changes(worktree, index, &plan);
                    let synthetic = [IndexedChange { index, item }];
                    match apply_prepared_logical(
                        &synthetic,
                        &prepared,
                        &summaries,
                        &logical_snapshot,
                    ) {
                        Ok(success) => {
                            if first_file_change.is_none() {
                                first_file_change = Some(IndexedChange { index, item });
                            }
                            applied.extend(rename_applied);
                            affected_paths.extend(success.affected_paths);
                            recorded_changes.extend(success.recorded_changes);
                            apply_snapshot_mutations(
                                &mut logical_snapshot,
                                &success.logical_mutations,
                            );
                            logical_mutations.extend(success.logical_mutations);
                        }
                        Err(failure) => {
                            let IndexedFailure {
                                failure,
                                commit: failure_commit,
                            } = *failure;
                            if atomic {
                                rollback_unsealed_mint(orch, minted_branch.as_ref()).await;
                                return change_report_json(
                                    applied,
                                    vec![failure],
                                    failure_commit.or(commit),
                                    atomic,
                                );
                            }
                            if let Some(failure_commit) = failure_commit {
                                commit = Some(failure_commit);
                            }
                            failures.push(failure);
                        }
                    }
                }
                Err(error) => {
                    let IndexedFailure {
                        failure,
                        commit: failure_commit,
                    } = *build_failure(index, item, error);
                    if atomic {
                        let report = change_report_json(
                            applied,
                            vec![failure],
                            failure_commit.or(commit),
                            atomic,
                        );
                        if guard_resource_report {
                            replay::finalize_report(
                                &orch.db.local,
                                write_call.as_ref().expect("guarded resource call"),
                                &report,
                            )
                            .await;
                        }
                        rollback_unsealed_mint(orch, minted_branch.as_ref()).await;
                        return report;
                    }
                    if let Some(failure_commit) = failure_commit {
                        commit = Some(failure_commit);
                    }
                    failures.push(failure);
                }
            }
            index += 1;
            continue;
        }

        let family = match target_family(&item.target) {
            Ok(family) => family,
            Err(error) => {
                let IndexedFailure {
                    failure,
                    commit: failure_commit,
                } = *build_failure(index, item, error);
                if atomic {
                    let report = change_report_json(
                        applied,
                        vec![failure],
                        failure_commit.or(commit),
                        atomic,
                    );
                    if guard_resource_report {
                        replay::finalize_report(
                            &orch.db.local,
                            write_call.as_ref().expect("guarded resource call"),
                            &report,
                        )
                        .await;
                    }
                    rollback_unsealed_mint(orch, minted_branch.as_ref()).await;
                    return report;
                }
                if let Some(failure_commit) = failure_commit {
                    commit = Some(failure_commit);
                }
                failures.push(failure);
                index += 1;
                continue;
            }
        };

        if family == TargetFamily::Resource {
            // The branch append is a batch-level destination declaration. Minting
            // already consumed it before file preparation, so generic resource
            // dispatch must not query for or mutate a second entity.
            if mint_index == Some(index) {
                if let Some(minted) = minted_branch.as_ref() {
                    applied.push(AppliedChange {
                        index,
                        target: item.target.clone(),
                        mode: mode_name(item.mode).to_string(),
                        kind: "resource".to_string(),
                        summary: format!("Minted {} ({})", minted.uri, minted.git_ref),
                        data: None,
                    });
                    index += 1;
                    continue;
                }
            }
            // Blocking appends run after all other changes, as one group.
            if blocking_append_kind(item).is_some() {
                index += 1;
                continue;
            }
            match dispatch_resource_change(orch, request, index, item, false).await {
                Ok(change) => {
                    if let Some(promoted) = change.promoted_memory.clone() {
                        promoted_memories.push((
                            change.index,
                            change.target.clone(),
                            change.mode.clone(),
                            promoted,
                        ));
                    }
                    applied.push(change.into());
                }
                Err(mut failure) => {
                    failure.error = home_targets.annotate(index, failure.error);
                    let IndexedFailure {
                        failure,
                        commit: failure_commit,
                    } = *resource_failure(failure);
                    if atomic {
                        let report = change_report_json(
                            applied,
                            vec![failure],
                            failure_commit.or(commit),
                            atomic,
                        );
                        if guard_resource_report {
                            replay::finalize_report(
                                &orch.db.local,
                                write_call.as_ref().expect("guarded resource call"),
                                &report,
                            )
                            .await;
                        }
                        return report;
                    }
                    if let Some(failure_commit) = failure_commit {
                        commit = Some(failure_commit);
                    }
                    failures.push(failure);
                }
            }
            index += 1;
            continue;
        }

        let start = index;
        while index < payload.changes.len()
            && matches!(
                target_family(&payload.changes[index].target),
                Ok(TargetFamily::File)
            )
            && payload.changes[index].mode != ChangeMode::Rename
        {
            index += 1;
        }
        let batch = payload.changes[start..index]
            .iter()
            .enumerate()
            .map(|(offset, item)| IndexedChange {
                index: start + offset,
                item,
            })
            .collect::<Vec<_>>();
        let batch_results =
            apply_file_changes(request, &batch, allow_escape, atomic, &mut logical_snapshot);
        for result in batch_results {
            match result {
                Ok(success) => {
                    if first_file_change.is_none() && !success.applied.is_empty() {
                        let first_index = success.applied[0].index;
                        if let Some(change) =
                            batch.iter().find(|change| change.index == first_index)
                        {
                            first_file_change = Some(IndexedChange {
                                index: change.index,
                                item: change.item,
                            });
                        }
                    }
                    applied.extend(success.applied);
                    affected_paths.extend(success.affected_paths);
                    recorded_changes.extend(success.recorded_changes);
                    logical_mutations.extend(success.logical_mutations);
                }
                Err(failure) => {
                    let IndexedFailure {
                        failure,
                        commit: failure_commit,
                    } = *failure;
                    if atomic {
                        let report = change_report_json(
                            applied,
                            vec![failure],
                            failure_commit.or(commit),
                            atomic,
                        );
                        if guard_resource_report {
                            replay::finalize_report(
                                &orch.db.local,
                                write_call.as_ref().expect("guarded resource call"),
                                &report,
                            )
                            .await;
                        }
                        rollback_unsealed_mint(orch, minted_branch.as_ref()).await;
                        return report;
                    }
                    if let Some(failure_commit) = failure_commit {
                        commit = Some(failure_commit);
                    }
                    failures.push(failure);
                }
            }
        }
    }

    let promoted_memory_uris: Vec<String> = promoted_memories
        .iter()
        .map(|(_, _, _, promoted)| promoted.memory_uri.clone())
        .collect();

    let first_file_change_index = first_file_change.as_ref().map(|change| change.index);
    let had_file_change = first_file_change.is_some();
    let mut publication_requirement =
        crate::merge_requests::queries::PublicationRequirement::DeferredUntilPublication;
    let mut post_seal_publication = None;
    // The publication ladder's second rung, carried out of the transaction:
    // `Err` means the commit reached the jj bookmark but not the branch ref.
    let mut export = Ok(());
    // Whether this delivery owns the replay-ledger row, and so may correct the
    // report it recorded once the ladder's remaining rungs have settled.
    let mut replay_owner: Option<replay::ReplayOwner> = None;
    let mut ordinary_ledger_claim = replay::LedgerClaim::NotClaimed;
    if optimistic && first_file_change.is_some() {
        let guard = match crate::mcp::vcs::acquire_store_lock(
            orch,
            write_store.as_deref(),
            crate::orchestrator::StoreLockClass::Foreground,
            "logical write transaction",
            crate::mcp::vcs::STORE_LOCK_TIMEOUT,
        )
        .await
        {
            Ok(guard) => guard,
            Err(error) => {
                return serde_json::to_string(&empty_change_report(
                    Vec::new(),
                    vec![ChangeFailure {
                        index: 0,
                        target: payload
                            .changes
                            .first()
                            .map(|change| change.target.clone())
                            .unwrap_or_default(),
                        mode: payload
                            .changes
                            .first()
                            .map(|change| mode_name(change.mode))
                            .unwrap_or("patch")
                            .to_string(),
                        kind: "file".to_string(),
                        error,
                    }],
                    None,
                    payload.atomic.unwrap_or(false),
                    false,
                ))
                .unwrap_or_else(|serialize_error| serialize_error.to_string());
            }
        };
        let resolution = logical_resolution
            .as_ref()
            .expect("file writes require a resolved logical head");
        let replayed = match write_call.as_ref() {
            Some(call) => replay::recorded_report(&orch.db.local, call)
                .await
                .is_some(),
            None => false,
        };
        let head = cairn_vcs::resolve_coordinate(&resolution.repository_path, &resolution.rev)
            .await
            .ok();
        let held =
            super::run::held::held_rows(&orch.db.local, &resolution.project_id, &resolution.rev)
                .await;
        let unchanged = !replayed
            && head.as_deref() == Some(resolution.commit_id.as_str())
            && held.is_ok_and(|rows| rows.is_empty());
        if !unchanged {
            log::info!(
                "logical write on `{}`: the head, held work, or replay ledger changed after \
                 preparation (prepared on {}, now {head:?}); writing again under the store lock",
                resolution.rev,
                resolution.commit_id
            );
            drop(guard);
            *epoch = ReplayEpoch::Checked;
            return Box::pin(handle_write_attempt(
                orch,
                request,
                drained,
                WriteLocking::Locked,
                epoch,
            ))
            .await;
        }
        *epoch = ReplayEpoch::Checked;
        store_trace = guard.as_ref().map(|guard| guard.trace());
        write_store_guard = guard;
    }
    if let Some(first_file_change) = first_file_change {
        let _seal_phase = write_store_guard
            .as_ref()
            .map(|guard| guard.phase("logical tree publication"));
        match finalize_file_commit(
            orch,
            request,
            payload.commit_msg.as_deref(),
            &affected_paths,
            &recorded_changes,
            &first_file_change,
            &promoted_memory_uris,
            logical_resolution
                .as_ref()
                .expect("file writes require a resolved logical head"),
            destination_job_id.as_deref(),
            match revert_commit.as_ref() {
                Some(commit) => crate::jj::ProposedPublication::Revert(commit.clone()),
                None => crate::jj::ProposedPublication::Mutations(logical_mutations.clone()),
            },
            payload
                .conflict_markers_reason
                .as_deref()
                .map(str::trim)
                .filter(|reason| !reason.is_empty()),
        )
        .await
        {
            Ok(CommitOutcome::Done(mut completed)) => {
                if revert_commit.is_some() {
                    affected_paths = completed.affected_paths.clone();
                    if let Some(report) = completed.report.as_ref() {
                        if let Some(sha) = report.sha.as_deref() {
                            match reverted_file_changes(
                                logical_resolution.as_ref().expect("revert has resolution"),
                                sha,
                                &affected_paths,
                            ) {
                                Ok(changes) => recorded_changes = changes,
                                Err(error) => {
                                    log::warn!("Failed to build revert file ledger: {error}")
                                }
                            }
                        }
                    }
                    if let Some(change) = applied.first_mut() {
                        change.summary = format!(
                            "Reverted {} ({} paths)",
                            revert_commit.as_deref().expect("revert has commit"),
                            affected_paths.len()
                        );
                    }
                }
                commit = completed.report.take();
                export = completed.export;
                publication_requirement = completed.publication_requirement;
                post_seal_publication = completed.publication;
            }
            Err(failure) => {
                let IndexedFailure {
                    failure,
                    commit: failure_commit,
                } = *failure;
                if revert_commit.is_some() {
                    applied.retain(|change| change.index != failure.index);
                }
                if atomic {
                    rollback_promoted_memory_decisions(orch, &promoted_memories).await;
                    rollback_unsealed_mint(orch, minted_branch.as_ref()).await;
                    return change_report_json(applied, vec![failure], failure_commit, atomic);
                }
                if revert_commit.is_some() {
                    applied.clear();
                } else if let Some(failure_commit) = failure_commit {
                    commit = Some(failure_commit);
                }
                failures.push(failure);
            }
        }

        if minted_branch.is_some()
            && commit
                .as_ref()
                .and_then(|report| report.sha.as_ref())
                .is_none()
        {
            rollback_unsealed_mint(orch, minted_branch.as_ref()).await;
            minted_branch = None;
            destination_job_id = None;
        }

        // Record the publication against the call that produced it, still inside
        // the epoch that published it, so a crash cannot leave a landed commit
        // with no ledger row for a later delivery to find. Only a batch that
        // actually sealed a commit records: a write that published nothing has no
        // application to suppress and should stay freely retryable.
        if let (Some(call), Some(_)) = (
            write_call.as_ref(),
            commit.as_ref().and_then(|report| report.sha.as_ref()),
        ) {
            let mut report = empty_change_report(
                applied.clone(),
                failures.clone(),
                commit.clone(),
                !applied.is_empty() && !failures.is_empty(),
                atomic,
            );
            report.branch = minted_branch.as_ref().map(|minted| BranchReport {
                number: minted.number,
                git_ref: minted.git_ref.clone(),
                uri: minted.uri.clone(),
                stage: "accumulating".to_string(),
                pr_url: None,
            });
            if minted_branch.is_some() {
                report.post_seal = Some(MintedPostSealState {
                    file_changes: recorded_changes
                        .iter()
                        .map(|change| PersistedFileChange {
                            path: change.path.clone(),
                            status: change.status.to_string(),
                            additions: change.additions,
                            deletions: change.deletions,
                        })
                        .collect(),
                    cloud: post_seal_publication.as_ref().map(|publication| {
                        CloudPublicationInput {
                            project_id: publication.project_id.clone(),
                            repository: publication.repository.clone(),
                        }
                    }),
                    ledger_recorded: false,
                    cloud_published: false,
                });
            }
            let report = serde_json::to_string(&report)
                .unwrap_or_else(|error| format!("Failed to serialize change report: {error}"));
            if minted_branch.is_some() {
                replay_owner =
                    replay::record_pending_file_report(&orch.db.local, call, &report).await;
            } else {
                ordinary_ledger_claim = replay::record_report(&orch.db.local, call, &report).await;
            }
        }
        drop(_seal_phase);
    }
    // Local publication and its crash-resumable replay claim are durable. Remote
    // origin, cloud, GitHub, artifact, and check work must not serialize every
    // repository writer behind this call.
    drop(write_store_guard);
    // The post-seal mint path is intentionally a separate durable continuation.
    // Box the completion state so an ordinary resource mutation does not inherit
    // the complete mint/recovery future on its poll stack.
    let completion = Box::pin(async {
        if minted_branch.is_none()
            && commit
                .as_ref()
                .and_then(|report| report.sha.as_ref())
                .is_some()
        {
            for change in &recorded_changes {
                if let Err(error) = record_agent_file_change(
                    orch,
                    request,
                    &change.path,
                    change.status,
                    change.additions,
                    change.deletions,
                    None,
                )
                .await
                {
                    log::warn!("Failed to record file change: {error}");
                }
            }
        }

        let append_commit_warning = |commit: &mut Option<CommitReport>, warning: String| {
            if let Some(report) = commit.as_mut() {
                report.message = Some(match report.message.take() {
                    Some(message) => format!("{message}; {warning}"),
                    None => warning,
                });
            }
        };
        // The publication ladder past the store transaction: the export already ran
        // under the store lock inside it, and the origin push runs here because it is
        // network I/O that must not hold the lock. Both legs report the same way,
        // because both leave the same state behind — a commit that exists only where
        // Cairn can see it.
        let published = match (export, publication_requirement) {
            (Err(error), _) => Err(error),
            (Ok(()), crate::merge_requests::queries::PublicationRequirement::RequiredForOpenPr)
                if minted_branch.is_some() =>
            {
                Ok(())
            }
            (Ok(()), crate::merge_requests::queries::PublicationRequirement::RequiredForOpenPr) => {
                let _phase = store_trace.as_ref().map(|trace| trace.phase("origin push"));
                match logical_resolution.as_ref() {
                    Some(resolution) => {
                        crate::orchestrator::base_advance::publish_managed_branch(
                            orch,
                            &resolution.repository_path,
                            &resolution.rev,
                        )
                        .await
                    }
                    // Only a file change reaches a required push, and a file change
                    // cannot be prepared without a resolved logical head.
                    None => Ok(()),
                }
            }
            (
                Ok(()),
                crate::merge_requests::queries::PublicationRequirement::DeferredUntilPublication,
            ) => {
                if had_file_change {
                    if let Some(trace) = &store_trace {
                        trace.deferred("origin push deferred");
                    }
                }
                Ok(())
            }
        };
        if let Err(error) = published {
            let index = first_file_change_index.unwrap_or(0);
            let sealed_sha = commit.as_ref().and_then(|report| report.sha.clone());
            let error = super::unpublished_commit_message(
                sealed_sha.as_deref().unwrap_or("(unknown)"),
                &error,
            );
            failures.push(ChangeFailure {
                index,
                target: payload
                    .changes
                    .get(index)
                    .map(|c| c.target.clone())
                    .unwrap_or_else(|| "file:".to_string()),
                mode: payload
                    .changes
                    .get(index)
                    .map(|c| mode_name(c.mode).to_string())
                    .unwrap_or_else(|| "patch".to_string()),
                kind: "publication".to_string(),
                error: error.clone(),
            });
            commit = Some(CommitReport {
                status: "sealed locally; unpublished".to_string(),
                sha: sealed_sha,
                pr_number: commit.as_ref().and_then(|report| report.pr_number),
                message: Some(error),
            });
            let report = change_report_json(applied, failures, commit, atomic);
            // The ledger recorded `committed` inside the store lock, before the
            // export and push could say otherwise. Correct it here or a redelivery of
            // this same call is answered, durably, with a success that did not happen.
            if let (Some(call), Some(owner)) = (write_call.as_ref(), replay_owner.as_ref()) {
                let _ = replay::finalize_owned_report(&orch.db.local, call, owner, &report).await;
            } else if let (Some(call), replay::LedgerClaim::Claimed) =
                (write_call.as_ref(), ordinary_ledger_claim)
            {
                replay::finalize_report(&orch.db.local, call, &report).await;
            }
            return report;
        }
        if minted_branch.is_none() {
            if let Some(ref publication) = post_seal_publication {
                let _phase = store_trace
                    .as_ref()
                    .map(|trace| trace.phase("sealed pack publication"));
                if let Err(error) = crate::mcp::vcs::publish_sealed_commit_pack(
                    publication.db.as_ref(),
                    &publication.project_id,
                    &publication.repository,
                    commit
                        .as_ref()
                        .and_then(|report| report.sha.as_deref())
                        .expect("post-seal publication requires a sealed sha"),
                )
                .await
                {
                    append_commit_warning(
                        &mut commit,
                        format!("sealed commit cloud publication failed: {error}"),
                    );
                }
            }
        }

        if let (Some(index), Some(minted), Some(call), Some(owner)) = (
            mint_index,
            minted_branch.as_ref(),
            write_call.as_ref(),
            replay_owner.as_ref(),
        ) {
            let mut report = empty_change_report(
                applied.clone(),
                failures.clone(),
                commit.clone(),
                !applied.is_empty() && !failures.is_empty(),
                atomic,
            );
            report.branch = Some(BranchReport {
                number: minted.number,
                git_ref: minted.git_ref.clone(),
                uri: minted.uri.clone(),
                stage: "accumulating".to_string(),
                pr_url: None,
            });
            report.post_seal = Some(MintedPostSealState {
                file_changes: recorded_changes
                    .iter()
                    .map(|change| PersistedFileChange {
                        path: change.path.clone(),
                        status: change.status.to_string(),
                        additions: change.additions,
                        deletions: change.deletions,
                    })
                    .collect(),
                cloud: post_seal_publication
                    .as_ref()
                    .map(|publication| CloudPublicationInput {
                        project_id: publication.project_id.clone(),
                        repository: publication.repository.clone(),
                    }),
                ledger_recorded: false,
                cloud_published: false,
            });
            return finish_minted_post_seal(
                orch,
                request,
                &payload,
                index,
                &minted.resolution,
                &minted.job_id,
                call,
                owner,
                report,
            )
            .await
            .unwrap_or_else(|error| {
                format!("The minted branch sealed, but post-seal recovery remains pending: {error}")
            });
        }

        if let (Some(index), Some(minted), Some(_)) = (
            mint_index,
            minted_branch.as_ref(),
            commit.as_ref().and_then(|report| report.sha.as_deref()),
        ) {
            if let Some(pr) = payload.changes[index]
                .payload
                .as_ref()
                .and_then(|value| value.get("pr"))
            {
                let _ = pr;
                let opened = complete_minted_branch_pr(
                    orch,
                    &payload,
                    index,
                    &minted.job_id,
                    &minted.resolution,
                    &minted.git_ref,
                    minted.number,
                )
                .await;
                match opened {
                Ok(url) => minted_pr_url = Some(url),
                Err(error) => failures.push(ChangeFailure {
                    index,
                    target: payload.changes[index].target.clone(),
                    mode: mode_name(payload.changes[index].mode).to_string(),
                    kind: "pull-request".to_string(),
                    error: format!("Commit sealed on {}, but opening its PR failed; the branch remains accumulating: {error}", minted.git_ref),
                }),
            }
            }
        }

        if let (Some(minted), Some(url)) = (minted_branch.as_ref(), minted_pr_url.as_ref()) {
            let mut report = empty_change_report(
                applied.clone(),
                failures.clone(),
                commit.clone(),
                !applied.is_empty() && !failures.is_empty(),
                atomic,
            );
            report.branch = Some(BranchReport {
                number: minted.number,
                git_ref: minted.git_ref.clone(),
                uri: minted.uri.clone(),
                stage: "pr-open".to_string(),
                pr_url: Some(url.clone()),
            });
            if let (Some(call), Some(owner)) = (write_call.as_ref(), replay_owner.as_ref()) {
                let checkpoint = serde_json::to_string(&report).unwrap_or_default();
                if !replay::checkpoint_owned_report(&orch.db.local, call, owner, &checkpoint).await
                {
                    return "The minted branch PR completed, but replay ownership was lost before its completion checkpoint; a later delivery will recover the final result.".to_string();
                }
            }
            return serde_json::to_string(&report)
                .unwrap_or_else(|error| format!("Failed to serialize change report: {error}"));
        }

        if let (Some(resolution), Some(new_tip)) = (
            logical_resolution.as_ref(),
            commit.as_ref().and_then(|report| report.sha.as_deref()),
        ) {
            let sync_failures = crate::dev_instances::sync_live_branch_instances(
                orch,
                &resolution.project_id,
                &resolution.rev,
                new_tip,
            )
            .await;
            if !sync_failures.is_empty() {
                append_commit_warning(
                &mut commit,
                format!(
                    "live dev instance sync blocked ({} instance(s)); the owner received a blocking notice",
                    sync_failures.len()
                ),
            );
            }
        }

        // when:write check runner: a write that sealed a source-touching commit
        // fires the affected when:write checks against that sealed commit, streams
        // their output live into this tool's transcript, and appends a compact
        // pass/fail line to the change report — or a pending line once the bounded
        // inline wait elapses, with the wave finishing in the background. Gated on
        // an actual file change that produced a real commit sha — a
        // cairn://-resource-only write has no file change and never triggers.
        let check_summary: Option<String> = if had_file_change
            && commit
                .as_ref()
                .and_then(|report| report.sha.clone())
                .is_some()
        {
            // An explicit thread-branch destination is backed by its own durable job.
            // Checks and cancellation belong to that job rather than to the thread
            // session that happened to advance it, otherwise the branch's `/checks`
            // resource remains hollow and review waves attach to the wrong owner.
            let run_context = check_run_context_for_destination(
                crate::mcp::handlers::run_context::lookup_run(&orch.db.local, request)
                    .await
                    .ok(),
                destination_job_id.as_deref(),
            );
            let tool_use_id = request
                .tool_use_id
                .clone()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            // This write sealed a source-touching commit → the branch advanced.
            // Cancel any in-flight when:review suite for this job so the write-cadence
            // checks below and the agent's next manual run inherit the freed CPU
            // rather than competing with a dying suite. See
            // cancel_stale_review_on_branch_advance for the rationale and job-id scoping.
            if let Some(ctx) = run_context.as_ref() {
                crate::execution::checks::cancel_stale_review_on_branch_advance(orch, &ctx.job_id)
                    .await;
            }
            crate::execution::checks::run_write_checks_after_seal(
                orch,
                run_context.as_ref(),
                &request.cwd,
                &tool_use_id,
                logical_resolution.as_ref(),
            )
            .await
        } else {
            None
        };

        if !promoted_memories.is_empty() {
            let commit_sha = commit.as_ref().and_then(|report| report.sha.clone());
            if let Some(sha) = commit_sha {
                let ids: Vec<String> = promoted_memories
                    .iter()
                    .map(|(_, _, _, promoted)| promoted.memory_id.clone())
                    .collect();
                if let Err(error) = crate::memories::db::set_memories_promoted_commit_sha(
                    &orch.db.local,
                    &ids,
                    &sha,
                )
                .await
                {
                    failures.push(ChangeFailure {
                        index: promoted_memories[0].0,
                        target: promoted_memories[0].1.clone(),
                        mode: promoted_memories[0].2.clone(),
                        kind: "resource".to_string(),
                        error: format!(
                        "Committed file changes but failed to record promoted memory SHA: {error}"
                    ),
                    });
                }
            } else {
                rollback_promoted_memory_decisions(orch, &promoted_memories).await;
                let promoted_indices: std::collections::HashSet<usize> = promoted_memories
                    .iter()
                    .map(|(index, _, _, _)| *index)
                    .collect();
                applied.retain(|change| !promoted_indices.contains(&change.index));
                for (index, target, mode, _) in &promoted_memories {
                    failures.push(ChangeFailure {
                        index: *index,
                        target: target.clone(),
                        mode: mode.clone(),
                        kind: "resource".to_string(),
                        error: "promote_memory requires a committed file change in the same write"
                            .to_string(),
                    });
                }
                if atomic {
                    return change_report_json(applied, failures, commit, atomic);
                }
            }
        }

        let mut report = empty_change_report(applied, failures, commit, false, atomic);
        report.partial_success = !report.applied.is_empty() && !report.failures.is_empty();
        report.branch = minted_branch.as_ref().map(|minted| BranchReport {
            number: minted.number,
            git_ref: minted.git_ref.clone(),
            uri: minted.uri.clone(),
            stage: if minted_pr_url.is_some() {
                "pr-open"
            } else {
                "accumulating"
            }
            .to_string(),
            pr_url: minted_pr_url.clone(),
        });
        let mut blocking_checkpointed = false;
        if let Some(kind) = blocking_kind.filter(|_| !blocking_indices.is_empty()) {
            let mut blocking_request = request.clone();
            let checkpoint = if let Some(call) = write_call.as_ref() {
                blocking_request.tool_use_id = Some(call.tool_use_id().to_string());
                let outer = append_check_summary(
                    serde_json::to_string(&report).expect("write report serializes"),
                    &check_summary,
                );
                replay::checkpoint_blocking_report(
                    &orch.db.local,
                    call,
                    replay_owner.as_ref(),
                    ordinary_ledger_claim,
                    &outer,
                )
                .await
            } else {
                Ok(())
            };
            match checkpoint {
                Ok(()) => {
                    blocking_checkpointed = write_call.is_some();
                    report.blocking = Some(
                        run_blocking_group(
                            orch,
                            &blocking_request,
                            &payload.changes,
                            &blocking_indices,
                            kind,
                        )
                        .await,
                    );
                }
                Err(error) => {
                    for &index in &blocking_indices {
                        let IndexedFailure { failure, .. } =
                            *build_failure(index, &payload.changes[index], error.clone());
                        report.failures.push(failure);
                    }
                    report.partial_success =
                        !report.applied.is_empty() && !report.failures.is_empty();
                }
            }
        }
        let result = append_check_summary(
            serde_json::to_string(&report)
                .unwrap_or_else(|e| format!("Failed to serialize change report: {e}")),
            &check_summary,
        );
        if replay_owner.is_none()
            && (guard_resource_report
                || ordinary_ledger_claim == replay::LedgerClaim::Claimed
                || blocking_checkpointed)
        {
            replay::finalize_report(
                &orch.db.local,
                write_call.as_ref().expect("guarded write call"),
                &result,
            )
            .await;
        }
        result
    });
    let result = match (write_call.as_ref(), replay_owner.as_ref()) {
        (Some(call), Some(owner)) => {
            replay::with_owned_report_heartbeat(&orch.db.local, call, owner, completion).await
        }
        _ => completion.await,
    };
    if let (Some(call), Some(owner)) = (write_call.as_ref(), replay_owner.as_ref()) {
        let _ = replay::finalize_owned_report(&orch.db.local, call, owner, &result).await;
    }
    result
}

#[cfg(test)]
mod change_preview_tests {
    use super::file_mutations::{
        hash_file_target, literal_not_found_diagnostic, prepare_file_changes, sha256_hex,
        PreparedChange,
    };
    use super::preview::{find_change_preview_in_assistant_rows, is_change_tool_name};
    use super::*;
    use crate::mcp::types::{ChangeItem, ChangeMode};

    #[test]
    fn blocking_report_keeps_body_opaque_and_spawn_metadata_structured() {
        let body = "{\"failures\":[{\"error\":\"task output, not a write failure\"}]}\nsecond line";
        let uri = "cairn://p/test/1/1/builder/call/one/artifact";
        let response = cairn_common::protocol::CallbackResponse {
            result: body.to_string(),
            artifact_uri: Some(uri.to_string()),
            ..Default::default()
        };
        let mut report = empty_change_report(Vec::new(), Vec::new(), None, false, false);
        report.blocking = Some(response.into());
        let encoded = serde_json::to_string(&report).unwrap();
        let wire: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(wire["blocking"]["text"], body);
        assert_eq!(wire["blocking"]["artifact_uri"], uri);
        assert!(wire.get("failures").is_none());
        let decoded: types::ChangeReport = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.blocking.unwrap().text, body);
        let replayed: serde_json::Value =
            serde_json::from_str(&replay::mark_as_replay(&encoded)).unwrap();
        assert_eq!(replayed["blocking"], wire["blocking"]);
    }

    #[test]
    fn blocking_report_retains_prior_failures_and_commit() {
        let mut report = empty_change_report(
            Vec::new(),
            vec![ChangeFailure {
                index: 0,
                target: "cairn://p/missing/messages".to_string(),
                mode: "append".to_string(),
                kind: "resource".to_string(),
                error: "missing project".to_string(),
            }],
            Some(CommitReport {
                status: "committed".to_string(),
                sha: Some("abc123".to_string()),
                pr_number: None,
                message: None,
            }),
            false,
            false,
        );
        report.blocking = Some("question answer".to_string().into());
        let wire = serde_json::to_value(&report).unwrap();
        assert_eq!(wire["failures"][0]["index"], 0);
        assert_eq!(wire["commit"]["sha"], "abc123");
        assert_eq!(wire["blocking"]["text"], "question answer");
        assert!(wire["blocking"].get("artifact_uri").is_none());
    }

    #[test]
    fn minted_post_seal_inputs_round_trip_without_guessing() {
        let mut report = empty_change_report(Vec::new(), Vec::new(), None, false, false);
        report.post_seal = Some(MintedPostSealState {
            file_changes: vec![PersistedFileChange {
                path: "src/lib.rs".to_string(),
                status: "modified".to_string(),
                additions: 3,
                deletions: 1,
            }],
            cloud: Some(CloudPublicationInput {
                project_id: "cairn".to_string(),
                repository: std::path::PathBuf::from("/objects/repository.git"),
            }),
            ledger_recorded: false,
            cloud_published: false,
        });

        let encoded = serde_json::to_string(&report).unwrap();
        let decoded: self::types::ChangeReport = serde_json::from_str(&encoded).unwrap();
        let state = decoded.post_seal.unwrap();
        assert_eq!(state.file_changes[0].path, "src/lib.rs");
        assert_eq!(state.file_changes[0].additions, 3);
        assert_eq!(
            state.cloud.unwrap().repository,
            std::path::PathBuf::from("/objects/repository.git")
        );
    }

    fn patch_item(target: &str, old: &str, new: &str) -> ChangeItem {
        ChangeItem {
            target: target.to_string(),
            mode: ChangeMode::Patch,
            payload: Some(serde_json::json!({
                "old_string": old,
                "new_string": new,
            })),
        }
    }

    #[test]
    fn prepare_file_changes_computes_patch_without_writing() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("example.txt");
        std::fs::write(&file, "hello old world\n").unwrap();
        let item = patch_item("file:example.txt", "old", "new");
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let (prepared, summaries) = prepare_file_changes(temp.path(), &changes, false).unwrap();

        assert_eq!(summaries, vec!["~file:example.txt".to_string()]);
        match &prepared[0] {
            PreparedChange::Write {
                content, is_new, ..
            } => {
                assert_eq!(content, "hello new world\n");
                assert!(!is_new);
            }
            _ => panic!("expected write"),
        }
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "hello old world\n");
    }

    #[test]
    fn prepare_file_changes_applies_wildcard_span_specimen() {
        // End-to-end through the change patch dispatch: the motivating specimen
        // (head ends in `{`, tail starts with a non-closer) resolves as a span
        // delete-and-replace, where the old global-balance net produced an
        // unsatisfiable brace constraint.
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("specimen.rs");
        let original = "fn lookup_project_by_cwd() {\n    old_impl();\n}\n\npub async fn handle_read_resource() {\n    body();\n}\n";
        std::fs::write(&file, original).unwrap();
        let item = patch_item(
            "file:specimen.rs",
            "fn lookup_project_by_cwd() {~~*~~pub async fn handle_read_resource(",
            "fn lookup_project_by_cwd() {\n    new_impl();\n}\n\npub async fn handle_read_resource(",
        );
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let (prepared, _) = prepare_file_changes(temp.path(), &changes, false).unwrap();
        match &prepared[0] {
            PreparedChange::Write { content, .. } => {
                assert_eq!(
                    content,
                    "fn lookup_project_by_cwd() {\n    new_impl();\n}\n\npub async fn handle_read_resource() {\n    body();\n}\n"
                );
            }
            _ => panic!("expected write"),
        }
    }

    #[test]
    fn prepare_file_changes_applies_wildcard_balanced() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("balanced.rs");
        std::fs::write(&file, "fn main() {\n    if x {\n        y;\n    }\n}\n").unwrap();
        let item = patch_item(
            "file:balanced.rs",
            "fn main() {~~*~~}",
            "fn main() {\n    z;\n}",
        );
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let (prepared, _) = prepare_file_changes(temp.path(), &changes, false).unwrap();
        match &prepared[0] {
            PreparedChange::Write { content, .. } => {
                assert_eq!(content, "fn main() {\n    z;\n}\n");
            }
            _ => panic!("expected write"),
        }
    }

    #[test]
    fn prepare_file_changes_treats_escaped_marker_as_literal() {
        // A single escaped marker is not a wildcard edit — it is a literal
        // find/replace of the text `~~*~~`.
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("escaped.txt");
        std::fs::write(&file, "keep ~~*~~ here\n").unwrap();
        let item = patch_item("file:escaped.txt", "\\~~*~~", "REPLACED");
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let (prepared, _) = prepare_file_changes(temp.path(), &changes, false).unwrap();
        match &prepared[0] {
            PreparedChange::Write { content, .. } => {
                assert_eq!(content, "keep REPLACED here\n");
            }
            _ => panic!("expected write"),
        }
    }

    #[test]
    fn literal_not_found_diagnostic_flags_malformed_marker() {
        let msg = literal_not_found_diagnostic("foo ~~ bar", "foo baz");
        assert!(msg.contains("~~*~~"), "got: {msg}");
    }

    #[test]
    fn literal_not_found_diagnostic_suggests_wildcard_for_middle_edit() {
        let msg = literal_not_found_diagnostic(
            "function validate(token) {\n  old_body();\n}",
            "function validate(token) {\n  new_body();\n}",
        );
        assert!(msg.contains("~~*~~"), "got: {msg}");
        assert!(msg.contains("collapse"), "got: {msg}");
    }

    #[test]
    fn literal_not_found_diagnostic_plain_when_no_hint() {
        let msg = literal_not_found_diagnostic("abc", "xyz");
        assert!(msg.starts_with("old_string not found"), "got: {msg}");
        assert!(!msg.contains("~~*~~"), "got: {msg}");
    }

    #[test]
    fn hash_file_target_captures_existing_and_missing() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("existing.txt"), "contents").unwrap();
        let existing = patch_item("file:existing.txt", "contents", "changed");
        let missing = ChangeItem {
            target: "file:missing.txt".to_string(),
            mode: ChangeMode::Create,
            payload: Some(serde_json::json!({ "content": "new" })),
        };

        let existing_hash = hash_file_target(temp.path(), &existing).unwrap();
        let missing_hash = hash_file_target(temp.path(), &missing).unwrap();

        assert!(existing_hash.exists);
        assert_eq!(existing_hash.kind, "file");
        assert_eq!(existing_hash.hash, sha256_hex(b"contents"));
        assert!(!missing_hash.exists);
        assert_eq!(missing_hash.hash, "missing");
    }

    fn create_item(target: &str, payload: serde_json::Value) -> ChangeItem {
        ChangeItem {
            target: target.to_string(),
            mode: ChangeMode::Create,
            payload: Some(payload),
        }
    }

    fn content_item(target: &str, mode: ChangeMode, content: &str) -> ChangeItem {
        ChangeItem {
            target: target.to_string(),
            mode,
            payload: Some(serde_json::json!({ "content": content })),
        }
    }

    #[test]
    fn prepare_file_changes_create_via_payload() {
        let temp = tempfile::tempdir().unwrap();
        let item = create_item(
            "file:created.txt",
            serde_json::json!({ "content": "hello\nworld\n" }),
        );
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let (prepared, summaries) = prepare_file_changes(temp.path(), &changes, false).unwrap();
        assert_eq!(summaries, vec!["+file:created.txt".to_string()]);
        match &prepared[0] {
            PreparedChange::Write {
                content, is_new, ..
            } => {
                assert_eq!(content, "hello\nworld\n");
                assert!(is_new);
            }
            _ => panic!("expected write"),
        }
    }

    #[test]
    fn prepare_file_changes_replace_via_payload() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("replace.txt");
        std::fs::write(&file, "old\n").unwrap();
        let item = content_item("file:replace.txt", ChangeMode::Replace, "new\n");
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let (prepared, summaries) = prepare_file_changes(temp.path(), &changes, false).unwrap();
        assert_eq!(summaries, vec!["~file:replace.txt".to_string()]);
        match &prepared[0] {
            PreparedChange::Write {
                content, is_new, ..
            } => {
                assert_eq!(content, "new\n");
                assert!(!is_new);
            }
            _ => panic!("expected write"),
        }
    }

    #[test]
    fn prepare_file_changes_append_existing_via_payload() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("append.txt");
        std::fs::write(&file, "first\n").unwrap();
        let item = content_item("file:append.txt", ChangeMode::Append, "second\n");
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let (prepared, summaries) = prepare_file_changes(temp.path(), &changes, false).unwrap();
        assert_eq!(summaries, vec!["~file:append.txt".to_string()]);
        match &prepared[0] {
            PreparedChange::Write {
                content, is_new, ..
            } => {
                assert_eq!(content, "first\nsecond\n");
                assert!(!is_new);
            }
            _ => panic!("expected write"),
        }
    }

    #[test]
    fn prepare_file_changes_append_creates_missing_file() {
        let temp = tempfile::tempdir().unwrap();
        let item = content_item("file:new-append.txt", ChangeMode::Append, "created\n");
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let (prepared, summaries) = prepare_file_changes(temp.path(), &changes, false).unwrap();
        assert_eq!(summaries, vec!["~file:new-append.txt".to_string()]);
        match &prepared[0] {
            PreparedChange::Write {
                content, is_new, ..
            } => {
                assert_eq!(content, "created\n");
                assert!(is_new);
            }
            _ => panic!("expected write"),
        }
    }

    #[test]
    fn prepare_file_changes_append_uses_in_flight_content() {
        let temp = tempfile::tempdir().unwrap();
        let create = content_item("file:chain.txt", ChangeMode::Create, "one\n");
        let append = content_item("file:chain.txt", ChangeMode::Append, "two\n");
        let changes = vec![
            IndexedChange {
                index: 0,
                item: &create,
            },
            IndexedChange {
                index: 1,
                item: &append,
            },
        ];

        let (prepared, summaries) = prepare_file_changes(temp.path(), &changes, false).unwrap();
        assert_eq!(
            summaries,
            vec!["+file:chain.txt".to_string(), "~file:chain.txt".to_string()]
        );
        match &prepared[1] {
            PreparedChange::Write {
                content, is_new, ..
            } => {
                assert_eq!(content, "one\ntwo\n");
                assert!(!is_new);
            }
            _ => panic!("expected write"),
        }
    }

    #[test]
    fn prepare_file_changes_patch_via_diff_payload() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("api.txt");
        std::fs::write(&file, "line one\nline two\nline three\n").unwrap();
        let item = ChangeItem {
            target: "file:api.txt".to_string(),
            mode: ChangeMode::Patch,
            payload: Some(serde_json::json!({
                "diff": "--- a/api.txt\n+++ b/api.txt\n@@ -1,3 +1,3 @@\n line one\n-line two\n+line 2\n line three\n"
            })),
        };
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let (prepared, _) = prepare_file_changes(temp.path(), &changes, false).unwrap();
        match &prepared[0] {
            PreparedChange::Write { content, .. } => {
                assert_eq!(content, "line one\nline 2\nline three\n");
            }
            _ => panic!("expected write"),
        }
    }

    #[test]
    fn prepare_file_changes_patch_replace_all_via_payload() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("dup.txt");
        std::fs::write(&file, "x\nx\nx\n").unwrap();
        let item = ChangeItem {
            target: "file:dup.txt".to_string(),
            mode: ChangeMode::Patch,
            payload: Some(serde_json::json!({
                "old_string": "x",
                "new_string": "y",
                "replace_all": true,
            })),
        };
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let (prepared, _) = prepare_file_changes(temp.path(), &changes, false).unwrap();
        match &prepared[0] {
            PreparedChange::Write { content, .. } => {
                assert_eq!(content, "y\ny\ny\n");
            }
            _ => panic!("expected write"),
        }
    }

    #[test]
    fn prepare_file_changes_patch_non_unique_old_string_errors() {
        // A literal old_string matching more than one site with replace_all
        // unset is a footgun: silently editing the first match would let the
        // caller believe they edited the unique site they meant. It must error.
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("dup.txt");
        std::fs::write(&file, "x\nx\nx\n").unwrap();
        let item = patch_item("file:dup.txt", "x", "y");
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let err = match prepare_file_changes(temp.path(), &changes, false) {
            Ok(_) => panic!("expected failure for non-unique old_string"),
            Err(err) => err,
        };
        assert!(
            err.failure.error.contains("matched 3 sites"),
            "error should name the match count, got: {}",
            err.failure.error
        );
        assert!(
            err.failure.error.contains("replace_all"),
            "error should mention replace_all, got: {}",
            err.failure.error
        );
        assert!(
            err.failure.error.contains("Match 1 — dup.txt:1:1-1")
                && err.failure.error.contains("Match 2 — dup.txt:2:1-1")
                && err.failure.error.contains("Match 3 — dup.txt:3:1-1"),
            "error should list every matching location, got: {}",
            err.failure.error
        );
        assert!(
            err.failure.error.contains(">     1 | x"),
            "error should include line-numbered excerpts, got: {}",
            err.failure.error
        );
        // prepare_file_changes never writes; the on-disk file is untouched.
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "x\nx\nx\n");
    }

    #[test]
    fn prepare_file_changes_non_unique_excerpts_distinguish_similar_fixtures() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("equipment.txt");
        std::fs::write(
            &file,
            "equipment: pump-a\n  document: manual-a\n  enabled: true\n  mode: primary\n\n\
             equipment: pump-b\n  document: manual-b\n  enabled: true\n  mode: backup\n",
        )
        .unwrap();
        let item = patch_item("file:equipment.txt", "  enabled: true", "  enabled: false");
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let err = match prepare_file_changes(temp.path(), &changes, false) {
            Ok(_) => panic!("expected failure for non-unique fixture field"),
            Err(err) => err,
        };
        let diagnostic = err.failure.error;

        assert!(diagnostic.contains("Match 1 — equipment.txt:3:1-15"));
        assert!(diagnostic.contains("Match 2 — equipment.txt:8:1-15"));
        assert!(diagnostic.contains("1 | equipment: pump-a"));
        assert!(diagnostic.contains("2 |   document: manual-a"));
        assert!(diagnostic.contains("6 | equipment: pump-b"));
        assert!(diagnostic.contains("7 |   document: manual-b"));
    }

    #[test]
    fn prepare_file_changes_non_unique_reports_multiline_ranges() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("ranges.txt");
        std::fs::write(
            &file,
            "fixture a\nstart\nmiddle\nend\ntail a\nfixture b\nstart\nmiddle\nend\ntail b\n",
        )
        .unwrap();
        let item = patch_item("file:ranges.txt", "start\nmiddle", "replacement");
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let err = match prepare_file_changes(temp.path(), &changes, false) {
            Ok(_) => panic!("expected failure for non-unique multiline text"),
            Err(err) => err,
        };
        let diagnostic = err.failure.error;

        assert!(diagnostic.contains("Match 1 — ranges.txt:2:1-3:6"));
        assert!(diagnostic.contains("Match 2 — ranges.txt:7:1-8:6"));
        assert!(diagnostic.contains(">     2 | start\n>     3 | middle"));
        assert!(diagnostic.contains(">     7 | start\n>     8 | middle"));
    }

    #[test]
    fn prepare_file_changes_non_unique_distinguishes_matches_on_one_long_line() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("one-line.txt");
        let content = format!("{}needle{}needle\n", "a".repeat(220), "x".repeat(30));
        std::fs::write(&file, content).unwrap();
        let item = patch_item("file:one-line.txt", "needle", "changed");
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let err = match prepare_file_changes(temp.path(), &changes, false) {
            Ok(_) => panic!("expected failure for two matches on one line"),
            Err(err) => err,
        };
        let diagnostic = err.failure.error;

        assert!(diagnostic.contains("Match 1 — one-line.txt:1:221-226"));
        assert!(diagnostic.contains("Match 2 — one-line.txt:1:257-262"));
        let first_match = diagnostic
            .split("Match 1 — one-line.txt:1:221-226\n")
            .nth(1)
            .and_then(|section| section.split("\n\nMatch 2 —").next())
            .expect("first match excerpt");
        let second_match = diagnostic
            .split("Match 2 — one-line.txt:1:257-262\n")
            .nth(1)
            .expect("second match excerpt");
        assert!(
            second_match.contains("needle"),
            "the centered excerpt must retain a late-line match: {diagnostic}"
        );
        let first_marker = first_match
            .lines()
            .find(|line| line.starts_with("        |"))
            .expect("first focus marker");
        let second_marker = second_match
            .lines()
            .find(|line| line.starts_with("        |"))
            .expect("second focus marker");
        assert_ne!(
            first_marker, second_marker,
            "focus markers must distinguish occurrences on the same rendered line"
        );
    }

    #[test]
    fn prepare_file_changes_non_unique_bounds_large_multiline_excerpts() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("large-block.txt");
        let block = (1..=50)
            .map(|index| format!("block line {index:02}"))
            .collect::<Vec<_>>()
            .join("\n");
        let content = format!("fixture a\n{block}\ntail a\nfixture b\n{block}\ntail b\n");
        std::fs::write(&file, content).unwrap();
        let item = patch_item("file:large-block.txt", &block, "changed");
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let err = match prepare_file_changes(temp.path(), &changes, false) {
            Ok(_) => panic!("expected failure for repeated large multiline blocks"),
            Err(err) => err,
        };
        let diagnostic = err.failure.error;

        assert!(diagnostic.contains("Match 1 — large-block.txt:2:1-51:13"));
        assert!(diagnostic.contains("Match 2 — large-block.txt:54:1-103:13"));
        assert!(diagnostic.contains("lines omitted"));
        assert!(diagnostic.contains("block line 01"));
        assert!(diagnostic.contains("block line 50"));
        assert!(
            diagnostic.lines().count() < 30,
            "each site should have a bounded excerpt: {diagnostic}"
        );
    }

    #[test]
    fn prepare_file_changes_non_unique_diagnostic_caps_and_continues() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("many.txt");
        let content = (1..=12)
            .map(|index| format!("fixture {index}\nvalue: repeated\nend {index}\n"))
            .collect::<String>();
        std::fs::write(&file, content).unwrap();
        let item = patch_item("file:many.txt", "value: repeated", "value: changed");
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let err = match prepare_file_changes(temp.path(), &changes, false) {
            Ok(_) => panic!("expected failure for a large non-unique match set"),
            Err(err) => err,
        };
        let diagnostic = err.failure.error;

        assert!(diagnostic.contains("showing 10 of 12"));
        assert!(diagnostic.contains("Match 10 — many.txt:29:1-15"));
        assert!(!diagnostic.contains("Match 11 —"));
        assert!(diagnostic.contains("2 more matches omitted"));
        assert!(diagnostic.contains("`file:many.txt?offset=29&limit=40`"));
    }

    #[test]
    fn prepare_file_changes_in_flight_ambiguity_does_not_emit_stale_continuation_uri() {
        let temp = tempfile::tempdir().unwrap();
        let content = (1..=12)
            .map(|index| format!("fixture {index}\nvalue: repeated\nend {index}\n"))
            .collect::<String>();
        let create = content_item("file:created.txt", ChangeMode::Create, &content);
        let patch = patch_item("file:created.txt", "value: repeated", "value: changed");
        let changes = vec![
            IndexedChange {
                index: 0,
                item: &create,
            },
            IndexedChange {
                index: 1,
                item: &patch,
            },
        ];

        let err = match prepare_file_changes(temp.path(), &changes, false) {
            Ok(_) => panic!("expected failure for ambiguous in-flight content"),
            Err(err) => err,
        };
        let diagnostic = err.failure.error;

        assert_eq!(err.failure.index, 1);
        assert!(diagnostic.contains("showing 10 of 12"));
        assert!(diagnostic.contains("in-flight snapshot"));
        assert!(diagnostic.contains("Apply the preceding changes in a separate write"));
        assert!(!diagnostic.contains("file:created.txt?offset="));
        assert!(!temp.path().join("created.txt").exists());
    }

    #[test]
    fn prepare_file_changes_patch_single_match_applies() {
        // The unique-match case is unchanged: exactly one occurrence applies.
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("single.txt");
        std::fs::write(&file, "keep\nunique\nkeep\n").unwrap();
        let item = patch_item("file:single.txt", "unique", "changed");
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let (prepared, _) = prepare_file_changes(temp.path(), &changes, false).unwrap();
        match &prepared[0] {
            PreparedChange::Write { content, .. } => {
                assert_eq!(content, "keep\nchanged\nkeep\n");
            }
            _ => panic!("expected write"),
        }
    }

    #[test]
    fn prepare_file_changes_patch_non_unique_after_earlier_item_errors() {
        // Bug #81: a later item's old_string is unique on disk but becomes
        // non-unique against the in-flight content an earlier item produced.
        // The count runs against the working content the patch operates on, so
        // item B errors with a clear count message instead of failing-and-
        // reverting the whole batch with an empty error.
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("batch.txt");
        std::fs::write(&file, "alpha\nbeta\ngamma\n").unwrap();
        // Item A makes a second "beta" appear in the in-flight content.
        let item_a = patch_item("file:batch.txt", "gamma", "beta");
        // Item B's old_string is unique on disk but non-unique after A.
        let item_b = patch_item("file:batch.txt", "beta", "delta");
        let changes = vec![
            IndexedChange {
                index: 0,
                item: &item_a,
            },
            IndexedChange {
                index: 1,
                item: &item_b,
            },
        ];

        let err = match prepare_file_changes(temp.path(), &changes, false) {
            Ok(_) => panic!("expected failure for non-unique in-flight old_string"),
            Err(err) => err,
        };
        assert_eq!(err.failure.index, 1, "item B should be the failing item");
        assert!(
            err.failure.error.contains("matched 2 sites"),
            "error should name the in-flight match count, got: {}",
            err.failure.error
        );
    }

    #[test]
    fn prepare_file_changes_ignores_top_level_flat_keys() {
        // The flat file shape is gone. A top-level `content` with no `payload`
        // deserializes into an unknown (ignored) field, so create finds no
        // payload.content and fails exactly as an empty create would.
        let temp = tempfile::tempdir().unwrap();
        let item: ChangeItem = serde_json::from_value(serde_json::json!({
            "target": "file:created.txt",
            "mode": "create",
            "content": "ignored",
        }))
        .unwrap();
        assert!(item.payload.is_none());
        let changes = vec![IndexedChange {
            index: 0,
            item: &item,
        }];

        let err = match prepare_file_changes(temp.path(), &changes, false) {
            Ok(_) => panic!("expected failure for create without payload.content"),
            Err(err) => err,
        };
        assert!(
            err.failure.error.contains("mode=create requires content"),
            "got: {}",
            err.failure.error
        );
    }

    #[test]
    fn change_tool_name_accepts_mcp_names() {
        // Current verb name.
        assert!(is_change_tool_name("write"));
        assert!(is_change_tool_name("mcp__cairn__write"));
        // Legacy name kept for historical-event rendering.
        assert!(is_change_tool_name("change"));
        assert!(is_change_tool_name("mcp__cairn__change"));
        assert!(!is_change_tool_name("exchange"));
        assert!(!is_change_tool_name("mcp__cairn__change_extra"));
        assert!(!is_change_tool_name("mcp__cairn__write_extra"));
    }

    fn assistant_preview_event(
        sequence: i32,
        id: &str,
        name: &str,
    ) -> (i32, String, Option<String>) {
        (
            sequence,
            serde_json::json!({
                "toolUses": [{
                    "id": id,
                    "name": name,
                    "input": {"preview": true, "changes": []}
                }]
            })
            .to_string(),
            None,
        )
    }

    #[test]
    fn no_id_preview_lookup_does_not_attach_to_prior_event() {
        let rows = vec![assistant_preview_event(10, "old", "mcp__cairn__change")];
        let found = find_change_preview_in_assistant_rows("run", rows, Some(11), None).unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn no_id_preview_lookup_accepts_latest_visible_event() {
        let rows = vec![assistant_preview_event(11, "current", "mcp__cairn__change")];
        let found = find_change_preview_in_assistant_rows("run", rows, Some(11), None)
            .unwrap()
            .unwrap();
        assert_eq!(found.tool_use_id, "current");
        assert_eq!(found.sequence, 11);
    }

    #[test]
    fn no_id_preview_lookup_rejects_multiple_preview_calls_in_one_event() {
        let rows = vec![
            (
                11,
                serde_json::json!({
                    "toolUses": [
                        {"id": "one", "name": "mcp__cairn__change", "input": {"preview": true, "changes": []}},
                        {"id": "two", "name": "mcp__cairn__change", "input": {"preview": true, "changes": []}}
                    ]
                })
                .to_string(),
                None,
            ),
        ];
        let error = find_change_preview_in_assistant_rows("run", rows, Some(11), None).unwrap_err();
        assert!(error.contains("multiple preview change calls"));
    }

    fn assistant_rename_event(
        sequence: i32,
        id: &str,
        name: &str,
    ) -> (i32, String, Option<String>) {
        // A bare rename call: no `preview` field in the input, mode "rename".
        (
            sequence,
            serde_json::json!({
                "toolUses": [{
                    "id": id,
                    "name": name,
                    "input": {"changes": [{
                        "target": "file:a.rs",
                        "mode": "rename",
                        "payload": {"new_name": "X", "old_name": "Y"}
                    }]}
                }]
            })
            .to_string(),
            None,
        )
    }

    #[test]
    fn rename_call_without_explicit_preview_is_preview_shaped() {
        // Mirrors the runtime bug: a rename defaults to the preview path without
        // the agent passing preview:true, so the transcript matcher must still
        // recognize it as a preview event to mark pending and enable apply.
        let rows = vec![assistant_rename_event(11, "rename-1", "mcp__cairn__write")];
        let found = find_change_preview_in_assistant_rows("run", rows, Some(11), None)
            .unwrap()
            .unwrap();
        assert_eq!(found.tool_use_id, "rename-1");
    }

    #[test]
    fn input_is_preview_shaped_classifies_inputs() {
        use super::preview::input_is_preview_shaped;
        use serde_json::json;
        // An explicit `preview` boolean wins in either direction.
        assert!(input_is_preview_shaped(
            &json!({"preview": true, "changes": []})
        ));
        assert!(!input_is_preview_shaped(
            &json!({"preview": false, "changes": [{"mode": "rename"}]})
        ));
        // Absent `preview`: only a rename item makes the call preview-shaped.
        assert!(input_is_preview_shaped(
            &json!({"changes": [{"mode": "rename"}]})
        ));
        assert!(!input_is_preview_shaped(
            &json!({"changes": [{"mode": "patch"}]})
        ));
        assert!(!input_is_preview_shaped(&json!({"changes": []})));
    }

    #[test]
    fn clean_success_report_omits_null_and_false_fields() {
        let report = empty_change_report(
            vec![AppliedChange {
                index: 0,
                target: "file:src/lib.rs".to_string(),
                mode: "patch".to_string(),
                kind: "file".to_string(),
                summary: "~file:src/lib.rs".to_string(),
                data: None,
            }],
            Vec::new(),
            None,
            false,
            false,
        );
        let value = serde_json::to_value(&report).unwrap();
        let object = value.as_object().unwrap();
        // A clean success collapses to just `applied`; null/false fields drop out.
        assert!(object.contains_key("applied"));
        assert!(!object.contains_key("failures"));
        assert!(!object.contains_key("commit"));
        assert!(!object.contains_key("partial_success"));
        assert!(!object.contains_key("transactional"));
        assert!(!object.contains_key("preview"));
        // The structured `data` echo is absent for a file change, so it stays
        // off the wire — non-todos results are byte-identical to before.
        let applied = object["applied"].as_array().unwrap();
        assert!(!applied[0].as_object().unwrap().contains_key("data"));
    }

    #[test]
    fn applied_change_data_serializes_for_frontend_parser() {
        // Pins the wire shape the todos transcript renderer parses out of a
        // change result: applied[i].data is the post-mutation collection with
        // camelCase TodoItem fields. If this shape drifts, the UI parser breaks
        // silently, so this test is the contract.
        let change = AppliedChange {
            index: 0,
            target: "cairn:~/todos".to_string(),
            mode: "patch".to_string(),
            kind: "resource".to_string(),
            summary: "Patched 1 todos".to_string(),
            data: Some(serde_json::json!([
                { "id": "1", "content": "X", "status": "completed" }
            ])),
        };
        let value = serde_json::to_value(&change).unwrap();
        assert_eq!(value["data"][0]["id"], "1");
        assert_eq!(value["data"][0]["content"], "X");
        assert_eq!(value["data"][0]["status"], "completed");
    }

    #[test]
    fn report_serializes_only_signal_carrying_fields() {
        let report = empty_change_report(
            Vec::new(),
            vec![ChangeFailure {
                index: 1,
                target: "file:src/lib.rs".to_string(),
                mode: "patch".to_string(),
                kind: "file".to_string(),
                error: "old_string not found".to_string(),
            }],
            Some(CommitReport {
                status: "failed".to_string(),
                sha: None,
                pr_number: None,
                message: None,
            }),
            true,
            false,
        );
        let value = serde_json::to_value(&report).unwrap();
        let object = value.as_object().unwrap();
        assert!(object.contains_key("failures"));
        assert!(object.contains_key("commit"));
        assert_eq!(
            object.get("partial_success"),
            Some(&serde_json::json!(true))
        );
        // transactional stays false here, so it is still omitted.
        assert!(!object.contains_key("transactional"));
    }

    #[test]
    fn thread_branch_checks_keep_the_run_stream_but_use_the_branch_job() {
        let context = super::super::RunContext {
            run_id: "thread-run".into(),
            job_id: "thread-session-job".into(),
            exec_seq: None,
            issue_id: None,
            issue_number: None,
            thread_id: Some("thread-id".into()),
            project_id: "project-id".into(),
            project_key: "project".into(),
            job_name: Some("Thread".into()),
            agent_config_id: Some("thread".into()),
        };

        let routed = check_run_context_for_destination(Some(context), Some("thread-branch-job"))
            .expect("authenticated context remains available");

        assert_eq!(
            routed.run_id, "thread-run",
            "streaming stays on the caller run"
        );
        assert_eq!(
            routed.job_id, "thread-branch-job",
            "check ownership follows the branch"
        );
        assert_eq!(routed.thread_id.as_deref(), Some("thread-id"));
        assert_eq!(routed.project_id, "project-id");
    }

    /// The write's two serialization shapes, pinned in source order. A locked
    /// write takes the lock before it resolves the head. An optimistic one
    /// takes it after preparing and, under it, re-reads the head and the
    /// replay ledger before the seal — the compare-and-set that makes
    /// preparing outside the lock safe (I3, I5).
    #[test]
    fn logical_write_lock_precedes_head_resolution_and_publication() {
        let source = include_str!("mod.rs");
        let lock = source.find("\"logical write transaction\"").unwrap();
        let resolve = source[lock..]
            .find("resolve_current_for_read")
            .map(|offset| lock + offset)
            .unwrap();
        let prepare = source[resolve..]
            .find("apply_logical_file_batch")
            .map(|offset| resolve + offset)
            .unwrap();
        let publish = source[prepare..]
            .find("finalize_file_commit")
            .map(|offset| prepare + offset)
            .unwrap();
        assert!(lock < resolve && resolve < prepare && prepare < publish);

        let late_lock = source[lock + 1..]
            .find("\"logical write transaction\"")
            .map(|offset| lock + 1 + offset)
            .expect("an optimistic write takes the lock to seal");
        let seal = source[late_lock..]
            .find("finalize_file_commit(")
            .map(|offset| late_lock + offset)
            .unwrap();
        let replay_recheck = source[late_lock..seal]
            .find("replay::recorded_report")
            .expect("the replay ledger is re-read under the sealing lock");
        let head_recheck = source[late_lock..seal]
            .find("cairn_vcs::resolve_coordinate")
            .expect("the head is re-read under the sealing lock");
        assert!(replay_recheck > 0 && head_recheck > 0);
        assert!(!source.contains(&["recover_stale", "file_commit"].concat()));
        assert!(!source.contains(&["update", "_stale"].concat()));
    }
}
