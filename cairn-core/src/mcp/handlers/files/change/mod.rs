//! Change-tool MCP handler.

pub(crate) mod file_mutations;
pub(crate) mod host_edit;
mod preview;
mod types;

use self::file_mutations::{
    apply_file_batch, emit_worktree_changed, finalize_file_commit, CommitOutcome, FileBatchSuccess,
};
use self::preview::{handle_apply_change, preview_change};
use self::types::{
    build_failure, empty_change_report, mode_name, resource_failure, AppliedChange, ChangeFailure,
    CommitReport, IndexedChange, IndexedFailure,
};
use super::target::{target_family, TargetFamily};
use crate::mcp::types::{ChangeMode, ChangePayload, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::resources::mutations::{
    blocking_append_kind, dispatch_resource_change, is_workspace_mcp_mutation,
    is_workspace_settings_mutation, project_write_crossing_path, run_blocking_group,
    validate_blocking_group, PromotedMemoryRef,
};

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
) -> Vec<Result<FileBatchSuccess, Box<IndexedFailure>>> {
    if atomic {
        return vec![apply_file_batch(request, batch, allow_escape)];
    }

    batch
        .iter()
        .map(|change| {
            apply_file_batch(
                request,
                &[IndexedChange {
                    index: change.index,
                    item: change.item,
                }],
                allow_escape,
            )
        })
        .collect()
}

/// Persist a give-up discard's would-be-lost edits to the job scratch dir
/// (`$TMPDIR`, in the sandbox writable set per `docs/worktree-fence.md`) so the
/// agent can re-apply them after retrying — making the recovery invariant's
/// "preserved/recoverable" clause true from the agent's seat, not just from the
/// jj operation log an agent cannot realistically reach. `patch` is captured
/// BEFORE any `update-stale`/`discard`, so it reflects the agent's full intended
/// batch against the pre-advance base. Best-effort: a `None`/empty patch or a
/// write failure yields `None` and the error simply omits the path.
fn preserve_discarded_edits(patch: Option<&str>) -> Option<std::path::PathBuf> {
    let patch = patch?;
    if patch.trim().is_empty() {
        return None;
    }
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let path = std::env::temp_dir().join(format!("cairn-discarded-edits-{ts}.patch"));
    std::fs::write(&path, patch).ok()?;
    Some(path)
}

/// Build the give-up failure message: the seal failed, `detail` names the
/// recovery step that couldn't proceed, the worktree was restored to HEAD (or
/// that restore itself failed), and — when edits were preserved — the scratch
/// path the agent can re-apply from. Pure, so the give-up contract is unit-tested
/// without an Orchestrator or jj binary.
fn give_up_error_message(
    seal_error: &str,
    detail: &str,
    restore: &Result<(), String>,
    preserved: Option<&std::path::Path>,
) -> String {
    let recovery = match preserved {
        Some(path) => format!(
            " Your edits were saved to {} — re-apply them after retrying.",
            path.display()
        ),
        None => String::new(),
    };
    match restore {
        Ok(()) => format!(
            "Applied file changes but commit failed: {seal_error}; {detail}, so the worktree was restored to HEAD. Retry the write.{recovery}"
        ),
        Err(re) => format!(
            "Applied file changes but commit failed: {seal_error}; {detail}, and restoring the worktree to HEAD also failed: {re}.{recovery}"
        ),
    }
}

/// Build the typed failure for a write whose seal was refused because the branch
/// bookmark tip carries a recorded conflict and `@` diverged from it (a
/// deliberate resolve-at-base flatten). Unlike every other seal failure this does
/// NOT discard — the on-disk edits are PRESERVED, because `@` holds the agent's
/// resolved work a discard would destroy — so the message names that and points at
/// the pure-jj flatten procedure rather than the futile "retry" advice the stale
/// family gets. Pure, so the contract is unit-testable without a jj binary.
fn conflicted_branch_failure(
    first_file_change: &IndexedChange<'_>,
    seal_error: &str,
) -> Box<IndexedFailure> {
    let error = format!(
        "Applied file changes but the seal was refused: {seal_error}. This branch has conflicted \
         intermediate commits jj will not fold, so sealing `@` forward can't clear them. The \
         on-disk edits were PRESERVED (not discarded). To land a flattened resolution, run the \
         pure-jj resolve-at-base flatten with NO commit_msg (see the git-workflow skill); do not \
         retry the write with commit_msg."
    );
    Box::new(IndexedFailure {
        failure: ChangeFailure {
            index: first_file_change.index,
            target: first_file_change.item.target.clone(),
            mode: mode_name(first_file_change.item.mode).to_string(),
            kind: "file".to_string(),
            error: error.clone(),
        },
        commit: Some(CommitReport {
            status: "failed".to_string(),
            sha: None,
            pr_number: None,
            message: Some(error),
        }),
    })
}

/// Recover a write+commit_msg batch whose seal hit a STALE working copy
/// ([`CommitOutcome::StaleRetry`]). A sibling advanced `@` over the shared store
/// between apply and seal; the loose edits are still on disk. Clear the staleness
/// (which discards those edits and re-bases `@` onto the advanced tip), re-apply
/// the batch's file changes against that fresh base, and re-seal. Anchored edits
/// re-match the advanced base — preserving a sibling's edits elsewhere in a
/// touched file and failing cleanly when the sibling rewrote the anchored region
/// itself. Every failure mode falls back to a stale-resilient discard, so the
/// worktree==HEAD invariant holds even when recovery can't land the batch — and
/// each give-up first persists the agent's would-be-lost edits to scratch (see
/// [`preserve_discarded_edits`]) so "recoverable" is true from the agent's seat.
async fn recover_stale_file_commit(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &ChangePayload,
    allow_escape: bool,
    promoted_memory_uris: &[String],
    first_file_change: &IndexedChange<'_>,
    seal_error: &str,
) -> Result<Option<CommitReport>, Box<IndexedFailure>> {
    let cwd = std::path::Path::new(&request.cwd);
    let atomic = payload.atomic.unwrap_or(false);
    let vcs = crate::mcp::vcs::resolve_worktree_vcs(orch, cwd);

    // Capture the agent's loose edits NOW, before any update-stale/discard can
    // overwrite them, so a give-up can persist them to scratch (Fix B). This
    // reflects the full intended batch against the pre-advance base regardless of
    // which give-up branch fires later. Best-effort: `None` if jj can't diff.
    let captured_patch = vcs.capture_patch(cwd);

    // Giveup failure: revert via the stale-resilient discard and report that the
    // worktree was restored to HEAD (the Phase-1 invariant), with retry guidance.
    // `detail` names the recovery step that couldn't proceed; the captured patch
    // is persisted to scratch so the discarded edits are recoverable.
    let give_up = |vcs: &dyn crate::mcp::vcs::WorktreeVcs, detail: &str| -> Box<IndexedFailure> {
        let preserved = preserve_discarded_edits(captured_patch.as_deref());
        let restore = vcs.discard(cwd);
        let error = give_up_error_message(seal_error, detail, &restore, preserved.as_deref());
        Box::new(IndexedFailure {
            failure: ChangeFailure {
                index: first_file_change.index,
                target: first_file_change.item.target.clone(),
                mode: mode_name(first_file_change.item.mode).to_string(),
                kind: "file".to_string(),
                error: error.clone(),
            },
            commit: Some(CommitReport {
                status: "failed".to_string(),
                sha: None,
                pr_number: None,
                message: Some(error),
            }),
        })
    };

    // A rename in the batch can't be safely re-derived here (its edit set is a
    // structural plan that would have to be recomputed against the advanced
    // base), so re-applying only the non-rename items would seal an incomplete
    // batch. Revert cleanly instead.
    let has_rename = payload.changes.iter().any(|item| {
        item.mode == ChangeMode::Rename
            && matches!(target_family(&item.target), Ok(TargetFamily::File))
    });
    if has_rename {
        let failure = give_up(
            vcs.as_ref(),
            "the batch includes a structural rename that can't be re-derived against the advanced base",
        );
        emit_worktree_changed(orch, &request.cwd);
        return Err(failure);
    }

    // (a) Clear staleness: advances `@` onto the sibling's tip and discards the
    // loose edits, so the on-disk base is now the advanced sibling state.
    if let Err(e) = vcs.update_stale(cwd) {
        let failure = give_up(
            vcs.as_ref(),
            &format!("recovering the stale worktree failed ({e})"),
        );
        emit_worktree_changed(orch, &request.cwd);
        return Err(failure);
    }

    // (b) Re-apply the ordered non-rename file changes against the fresh base.
    let file_batch: Vec<IndexedChange> = payload
        .changes
        .iter()
        .enumerate()
        .filter(|(_, item)| {
            matches!(target_family(&item.target), Ok(TargetFamily::File))
                && item.mode != ChangeMode::Rename
        })
        .map(|(index, item)| IndexedChange { index, item })
        .collect();

    let mut affected_paths: Vec<String> = Vec::new();
    let mut recorded_changes = Vec::new();
    for result in apply_file_changes(request, &file_batch, allow_escape, atomic) {
        match result {
            Ok(success) => {
                affected_paths.extend(success.affected_paths);
                recorded_changes.extend(success.recorded_changes);
            }
            Err(_) => {
                let failure = give_up(
                    vcs.as_ref(),
                    "an anchored edit no longer matched the advanced base",
                );
                emit_worktree_changed(orch, &request.cwd);
                return Err(failure);
            }
        }
    }

    // (c) Re-seal against the advanced base.
    match finalize_file_commit(
        orch,
        request,
        payload.commit_msg.as_deref(),
        &affected_paths,
        &recorded_changes,
        first_file_change,
        promoted_memory_uris,
    )
    .await
    {
        Ok(CommitOutcome::Done(report)) => Ok(report),
        Ok(CommitOutcome::StaleRetry { seal_error: e2 }) => {
            let failure = give_up(
                vcs.as_ref(),
                &format!("the worktree advanced again during recovery ({e2})"),
            );
            emit_worktree_changed(orch, &request.cwd);
            Err(failure)
        }
        Ok(CommitOutcome::ConflictedBranch { seal_error: e2 }) => {
            // The just-advanced base itself presents a conflicted bookmark tip:
            // preserve the edits (no discard) and surface the flatten guidance,
            // same as the primary path. Reaching here from stale-recovery is rare
            // but must NOT fall through to the discarding give-up.
            emit_worktree_changed(orch, &request.cwd);
            Err(conflicted_branch_failure(first_file_change, &e2))
        }
        // A non-stale re-seal error already discarded inside finalize.
        Err(failure) => Err(failure),
    }
}

/// Handle canonical change tool call.
pub async fn handle_change(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    // Authoritative validation gate, shared with cairn-cli's pre-flight check so
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
    let payload: ChangePayload = match crate::mcp::handlers::parse_payload(request) {
        Ok(payload) => payload,
        Err(error) => return error,
    };

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

    // Changes can only happen in a worktree. A non-jj cwd is the project's live
    // checkout behind a long-lived manager / triage / read-only agent (project
    // chat included); reject any file-target edit BEFORE applying it so the
    // user's checkout is never written to and never reverted. Resource-only
    // writes (issues, messages, todos, tasks) are unaffected.
    if !crate::jj::is_jj_dir(std::path::Path::new(&request.cwd)) {
        if let Some((index, item)) = payload
            .changes
            .iter()
            .enumerate()
            .find(|(_, item)| matches!(target_family(&item.target), Ok(TargetFamily::File)))
        {
            let IndexedFailure { failure, .. } = *build_failure(
                index,
                item,
                file_mutations::NON_WORKTREE_CHANGE_ERROR.to_string(),
            );
            return change_report_json(Vec::new(), vec![failure], None, false);
        }
    }

    // Worktree fence (writes). Detect any out-of-worktree file target up front
    // and gate it BEFORE applying anything, so a suspend leaves no partial side
    // effects and the slow-path resume can safely re-drive the whole batch.
    // `allow_escape` then permits the approved escaping write(s) on the apply
    // path. Resolution is once per call:
    //   - no active run (project chat) -> worktree-jailed (today's behavior)
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
                None => false,
                Some((_, Fence::Allow)) => true,
                Some((run_id, fence_mode @ (Fence::Ask | Fence::Deny))) => {
                    let worktree = std::path::Path::new(&request.cwd);
                    let mut any_escape = false;
                    for (index, item) in payload.changes.iter().enumerate() {
                        if !matches!(target_family(&item.target), Ok(TargetFamily::File)) {
                            continue;
                        }
                        let Ok(normalized) =
                            crate::mcp::git::normalize_change_target(&item.target, true)
                        else {
                            continue; // invalid target — the apply path reports it
                        };
                        let Ok(full) =
                            crate::mcp::git::resolve_change_target(worktree, &normalized, true)
                        else {
                            continue;
                        };
                        if !crate::mcp::git::path_escapes_worktree(worktree, &full) {
                            continue;
                        }
                        // Temp dirs + toolchain caches are in the sandbox
                        // writable set, so a structured write there is
                        // in-sandbox (parity with a shell write under `run`) and
                        // takes no prompt. Mark the escape so the apply path
                        // permits the absolute target, but don't raise the fence.
                        if crate::mcp::git::path_within_any(
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
                            fence::FenceDecision::Suspended => {
                                return "Change suspended pending worktree fence approval; resume \
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

    // Worktree fence (out-of-worktree workspace config writes). A workspace-scope
    // `write cairn://mcp` create/patch/delete and any `write cairn://settings`
    // patch edit files under ~/.cairn (settings.yaml, keybinds.json, the identity
    // store) — the SAME out-of-worktree write that a direct `>> settings.yaml`
    // would be. They route through the identical fence permission flow rather
    // than a parallel gate. Raised once for the batch and BEFORE any change
    // applies, so a suspend leaves no partial side effects and resume re-drives
    // the whole batch. Project-scope MCP writes are in-worktree and skip this.
    if let Some((index, _)) =
        payload.changes.iter().enumerate().find(|(_, item)| {
            is_workspace_mcp_mutation(item) || is_workspace_settings_mutation(item)
        })
    {
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
            // Fence::Allow agents write anywhere — no fence.
            Some((_, Fence::Allow)) => {}
            Some((run_id, fence_mode @ (Fence::Ask | Fence::Deny))) => {
                let settings_path = crate::config::settings::get_settings_path(&orch.config_dir);
                match fence::raise_fence(
                    orch,
                    &run_id,
                    fence_mode,
                    request,
                    fence::Crossing::write_outside(&settings_path),
                )
                .await
                {
                    fence::FenceDecision::Allow => {}
                    fence::FenceDecision::Deny(msg) => return deny(msg),
                    fence::FenceDecision::Suspended => {
                        return "Change suspended pending worktree fence approval; resume \
                            will continue once it is answered."
                            .to_string();
                    }
                }
            }
            // No active run: there is no run to gate the out-of-worktree write
            // against, mirroring how a no-run file write to settings.yaml is
            // worktree-jailed. Deny rather than silently write.
            None => {
                return deny(
                    "Denied: a workspace-scope cairn://mcp write edits ~/.cairn/settings.yaml \
                     (outside the worktree) and requires an active run to gate via the worktree \
                     fence. Use scope:\"project\" to edit the project's .cairn/config.yaml instead."
                        .to_string(),
                );
            }
        }
    }

    // Worktree fence (out-of-worktree project repo writes). Creating a project
    // git-inits/commits at an arbitrary `repoPath`, and attach-remote runs git on
    // the project's repo — both outside the worktree, even more arbitrary than
    // settings.yaml. They route through the same fence keyed on the target repo
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
            Some((_, Fence::Allow)) => {}
            Some((run_id, fence_mode @ (Fence::Ask | Fence::Deny))) => {
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
                    fence::FenceDecision::Suspended => {
                        return "Change suspended pending worktree fence approval; resume \
                            will continue once it is answered."
                            .to_string();
                    }
                }
            }
            None => {
                return deny(
                    "Denied: creating a project or attaching a remote writes to a git repo \
                     outside the worktree and requires an active run to gate via the worktree \
                     fence."
                        .to_string(),
                );
            }
        }
    }

    // Blocking appends (task/question collections) are deferred to the end of the
    // call and run as a single group; everything else applies first in order.
    let blocking_indices: Vec<usize> = payload
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
    let mut applied: Vec<AppliedChange> = Vec::new();
    let mut failures: Vec<ChangeFailure> = Vec::new();
    let mut commit: Option<CommitReport> = None;
    let mut affected_paths: Vec<String> = Vec::new();
    let mut recorded_changes = Vec::new();
    let mut first_file_change: Option<IndexedChange<'_>> = None;
    let mut promoted_memories: Vec<(usize, String, String, PromotedMemoryRef)> = Vec::new();
    let mut index = 0;

    while index < payload.changes.len() {
        let item = &payload.changes[index];

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
                Ok((route_file, spec, new_name)) => {
                    crate::symbols::rename::compute_plan(worktree, &route_file, spec, &new_name)
                }
                Err(error) => Err(error),
            };
            match rename_result {
                Ok(plan) => {
                    let (prepared, rename_applied, summaries) =
                        file_mutations::prepare_rename_changes(worktree, index, &plan);
                    let synthetic = [IndexedChange { index, item }];
                    match file_mutations::apply_prepared(&synthetic, &prepared, &summaries) {
                        Ok(success) => {
                            if first_file_change.is_none() {
                                first_file_change = Some(IndexedChange { index, item });
                            }
                            applied.extend(rename_applied);
                            affected_paths.extend(success.affected_paths);
                            recorded_changes.extend(success.recorded_changes);
                        }
                        Err(failure) => {
                            if !affected_paths.is_empty() {
                                emit_worktree_changed(orch, &request.cwd);
                            }
                            let IndexedFailure {
                                failure,
                                commit: failure_commit,
                            } = *failure;
                            if atomic {
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
            index += 1;
            continue;
        }

        let family = match target_family(&item.target) {
            Ok(family) => family,
            Err(error) => {
                if !affected_paths.is_empty() {
                    emit_worktree_changed(orch, &request.cwd);
                }
                let IndexedFailure {
                    failure,
                    commit: failure_commit,
                } = *build_failure(index, item, error);
                if atomic {
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
                index += 1;
                continue;
            }
        };

        if family == TargetFamily::Resource {
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
                Err(failure) => {
                    if !affected_paths.is_empty() {
                        emit_worktree_changed(orch, &request.cwd);
                    }
                    let IndexedFailure {
                        failure,
                        commit: failure_commit,
                    } = *resource_failure(failure);
                    if atomic {
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
        let batch_results = apply_file_changes(request, &batch, allow_escape, atomic);
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
                }
                Err(failure) => {
                    if !affected_paths.is_empty() {
                        emit_worktree_changed(orch, &request.cwd);
                    }
                    let IndexedFailure {
                        failure,
                        commit: failure_commit,
                    } = *failure;
                    if atomic {
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
    }

    let promoted_memory_uris: Vec<String> = promoted_memories
        .iter()
        .map(|(_, _, _, promoted)| promoted.memory_uri.clone())
        .collect();

    if let Some(first_file_change) = first_file_change {
        // Serialize the seal/discard (and its stale-recovery: update-stale →
        // re-apply → re-seal → fallback discard) on the per-store jj lock that
        // base-advance reconcile and merge-fold hold, so a write-path seal never
        // forks the shared store's operation log against a concurrent
        // reconcile/fold. The guard must span the recovery too, so it is bound
        // here across the whole finalize+recover branch. Disk-apply of edits
        // happened earlier (pure FS writes, no shared-store mutation) and stays
        // outside the lock. `None` for a non-worktree cwd. The held guard crosses
        // `.await`s; tokio's MutexGuard is Send, so this is correct.
        let store_lock = crate::mcp::vcs::resolve_store_lock(orch, request).await;
        let _store_guard = match store_lock.as_ref() {
            Some(lock) => Some(lock.lock().await),
            None => None,
        };
        match finalize_file_commit(
            orch,
            request,
            payload.commit_msg.as_deref(),
            &affected_paths,
            &recorded_changes,
            &first_file_change,
            &promoted_memory_uris,
        )
        .await
        {
            Ok(CommitOutcome::Done(commit_report)) => commit = commit_report,
            Ok(CommitOutcome::ConflictedBranch { seal_error }) => {
                // The seal was refused because the branch bookmark tip carries a
                // recorded conflict and `@` diverged from it — a deliberate
                // resolve-at-base flatten. Preserve the on-disk edits (no discard,
                // no update-stale retry which can never converge) and surface a
                // typed error pointing at the flatten procedure. The worktree==HEAD
                // invariant is deliberately left broken here because the only safe
                // automatic action is to keep the agent's resolved state; the
                // flatten the message references converges it.
                let IndexedFailure {
                    failure,
                    commit: failure_commit,
                } = *conflicted_branch_failure(&first_file_change, &seal_error);
                if atomic {
                    rollback_promoted_memory_decisions(orch, &promoted_memories).await;
                    return change_report_json(applied, vec![failure], failure_commit, atomic);
                }
                if let Some(failure_commit) = failure_commit {
                    commit = Some(failure_commit);
                }
                failures.push(failure);
            }
            Ok(CommitOutcome::StaleRetry { seal_error }) => {
                // A sibling advanced `@` between apply and seal. Try to land the
                // batch on the advanced base rather than lose it; any failure
                // falls back to a stale-resilient revert inside the recovery.
                match recover_stale_file_commit(
                    orch,
                    request,
                    &payload,
                    allow_escape,
                    &promoted_memory_uris,
                    &first_file_change,
                    &seal_error,
                )
                .await
                {
                    Ok(commit_report) => commit = commit_report,
                    Err(failure) => {
                        let IndexedFailure {
                            failure,
                            commit: failure_commit,
                        } = *failure;
                        if atomic {
                            rollback_promoted_memory_decisions(orch, &promoted_memories).await;
                            return change_report_json(
                                applied,
                                vec![failure],
                                failure_commit,
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
            Err(failure) => {
                let IndexedFailure {
                    failure,
                    commit: failure_commit,
                } = *failure;
                if atomic {
                    rollback_promoted_memory_decisions(orch, &promoted_memories).await;
                    return change_report_json(applied, vec![failure], failure_commit, atomic);
                }
                if let Some(failure_commit) = failure_commit {
                    commit = Some(failure_commit);
                }
                failures.push(failure);
            }
        }
    }

    if !promoted_memories.is_empty() {
        let commit_sha = commit.as_ref().and_then(|report| report.sha.clone());
        if let Some(sha) = commit_sha {
            let ids: Vec<String> = promoted_memories
                .iter()
                .map(|(_, _, _, promoted)| promoted.memory_id.clone())
                .collect();
            if let Err(error) =
                crate::memories::db::set_memories_promoted_commit_sha(&orch.db.local, &ids, &sha)
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

    if let Some(kind) = blocking_kind {
        let blocking_result =
            run_blocking_group(orch, request, &payload.changes, &blocking_indices, kind).await;
        // The blocking result (task output / suspend marker / question answer) is
        // the meaningful tool result. If earlier non-atomic items failed, put the
        // structured change report first so callers still see the failed indices.
        if !failures.is_empty() {
            let report = change_report_json(applied, failures, commit, atomic);
            return format!("{report}\n\n{blocking_result}");
        }
        if applied.is_empty() {
            return blocking_result;
        }
        let summary = applied
            .iter()
            .map(|change| change.summary.clone())
            .collect::<Vec<_>>()
            .join("; ");
        return format!(
            "Applied {} change(s): {}\n\n{}",
            applied.len(),
            summary,
            blocking_result
        );
    }

    change_report_json(applied, failures, commit, atomic)
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
    fn conflicted_branch_failure_preserves_edits_and_points_at_flatten() {
        // The write-path contract for a seal refused because the branch bookmark
        // tip carries a recorded conflict (a deliberate resolve-at-base flatten):
        // the typed failure says the on-disk edits were PRESERVED (no discard),
        // points at the no-commit_msg flatten, and does NOT advise retrying the
        // write with commit_msg. The CommitReport is a non-sha "failed" record.
        let item = patch_item("file:src/lib.rs", "old", "new");
        let change = IndexedChange {
            index: 3,
            item: &item,
        };
        let IndexedFailure { failure, commit } =
            *conflicted_branch_failure(&change, crate::jj::CONFLICTED_BRANCH_SEAL_MSG);

        assert_eq!(failure.index, 3);
        assert_eq!(failure.target, "file:src/lib.rs");
        assert_eq!(failure.kind, "file");
        assert!(
            failure.error.contains("PRESERVED"),
            "names that the edits were preserved: {}",
            failure.error
        );
        assert!(
            failure.error.contains("NO commit_msg") && failure.error.contains("git-workflow"),
            "points at the no-commit_msg flatten procedure: {}",
            failure.error
        );
        assert!(
            failure
                .error
                .contains("do not retry the write with commit_msg"),
            "explicitly warns against retrying with commit_msg: {}",
            failure.error
        );
        let commit = commit.expect("carries a failed CommitReport");
        assert_eq!(commit.status, "failed");
        assert!(commit.sha.is_none(), "a refused seal has no sha");
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
        // prepare_file_changes never writes; the on-disk file is untouched.
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "x\nx\nx\n");
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

    // ---- Fix B: give-up edit preservation (the #158 residual recoverability) ----

    #[test]
    fn preserve_discarded_edits_writes_patch_to_scratch() {
        let patch = "diff --git a/x.rs b/x.rs\n@@ -1 +1 @@\n-old\n+new\n";
        let path = preserve_discarded_edits(Some(patch))
            .expect("a non-empty patch is persisted to scratch");
        assert!(path.exists(), "the patch file is written");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), patch);
        assert!(
            path.starts_with(std::env::temp_dir()),
            "the patch lands in the scratch/temp dir (in the sandbox writable set): {path:?}"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn preserve_discarded_edits_skips_empty_or_absent() {
        assert!(
            preserve_discarded_edits(None).is_none(),
            "nothing to preserve when no patch was captured"
        );
        assert!(
            preserve_discarded_edits(Some("   \n")).is_none(),
            "a whitespace-only patch is not worth a scratch file"
        );
    }

    #[test]
    fn give_up_error_message_cites_preserved_path_and_retry() {
        let preserved = std::path::PathBuf::from("/tmp/scratch/cairn-discarded-edits-1.patch");
        let msg = give_up_error_message(
            "seal captured no change",
            "an anchored edit no longer matched the advanced base",
            &Ok(()),
            Some(preserved.as_path()),
        );
        assert!(msg.contains("restored to HEAD"), "got: {msg}");
        assert!(msg.contains("Retry the write"), "got: {msg}");
        assert!(
            msg.contains("cairn-discarded-edits-1.patch"),
            "the scratch path is cited so the agent can re-apply: {msg}"
        );
        assert!(msg.contains("re-apply"), "got: {msg}");
    }

    #[test]
    fn give_up_error_message_omits_path_when_nothing_preserved() {
        let msg = give_up_error_message(
            "the working copy is stale",
            "recovering the stale worktree failed (x)",
            &Ok(()),
            None,
        );
        assert!(msg.contains("restored to HEAD"), "got: {msg}");
        assert!(
            !msg.contains("saved to"),
            "no recovery clause without a preserved patch: {msg}"
        );
    }

    #[test]
    fn give_up_error_message_surfaces_failed_restore() {
        let msg = give_up_error_message(
            "seal failed",
            "the batch includes a structural rename that can't be re-derived",
            &Err("restore blew up".to_string()),
            None,
        );
        assert!(
            msg.contains("restoring the worktree to HEAD also failed: restore blew up"),
            "got: {msg}"
        );
    }
}
