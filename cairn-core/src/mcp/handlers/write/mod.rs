//! Write-verb MCP handler.

pub(crate) mod file_mutations;
pub(crate) mod host_edit;
mod preview;
mod types;

use self::file_mutations::{
    apply_logical_file_batch, apply_prepared_logical, emit_worktree_changed, finalize_file_commit,
    record_file_change_async, CommitOutcome, FileBatchSuccess,
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

/// Append the synchronous when:write check-runner summary (when present) to a
/// tool result, separated by a blank line. A no-op when no checks applied.
fn append_check_summary(text: String, summary: &Option<String>) -> String {
    match summary {
        Some(s) => format!("{text}\n\n{s}"),
        None => text,
    }
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
    // dispatch that would otherwise resolve it — sees the real resource. SDK
    // writers (the workflow harness) send `cairn:~/` raw; `cairn-cmd` resolves it
    // client-side, so canonical targets pass through unchanged.
    for item in payload.changes.iter_mut() {
        if item.target.starts_with("cairn:~") {
            if let Ok(resolved) =
                crate::resources::mutations::resolve_change_target_uri(orch, request, &item.target)
                    .await
            {
                item.target = resolved;
            }
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

    // Changes can only happen in a worktree. A non-jj cwd is the project's live
    // checkout behind a long-lived triage / read-only agent or another
    // no-worktree run; reject any file-target edit BEFORE applying it so the
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
    //   - no active run -> worktree-jailed (today's behavior)
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
                            crate::mcp::file_targets::normalize_change_target(&item.target, true)
                        else {
                            continue; // invalid target — the apply path reports it
                        };
                        let Ok(full) = crate::mcp::file_targets::resolve_change_target(
                            worktree,
                            &normalized,
                            true,
                        ) else {
                            continue;
                        };
                        if !crate::mcp::file_targets::path_escapes_worktree(worktree, &full) {
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

    let mut bookmark_observation = None;
    let has_file_targets = payload
        .changes
        .iter()
        .any(|item| matches!(target_family(&item.target), Ok(TargetFamily::File)));
    // The logical coordinate and every anchor evaluation must belong to the
    // same serialized writer epoch as publication. Acquiring here prevents a
    // second runner writer from advancing the bookmark between preparation and
    // the expected-head CAS; no workspace stale/reapply path is involved.
    let write_store_guard = if has_file_targets {
        let store = crate::mcp::vcs::resolve_store_lock(orch, request).await;
        match crate::mcp::vcs::acquire_store_lock(
            orch,
            store.as_deref(),
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
    let store_trace = write_store_guard.as_ref().map(|guard| guard.trace());
    let logical_resolution = if has_file_targets {
        match super::branch::resolve_current_for_read(orch, request).await {
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
    if has_file_targets {
        if let Ok((run, db)) = super::run_context::lookup_run_routed(&orch.db, request).await {
            if let Ok(context) =
                crate::execution::jobs::workspace_identity::resolve_managed_workspace_context(
                    db, run.job_id,
                )
                .await
            {
                bookmark_observation =
                    crate::mcp::vcs::observe_managed_bookmark(orch, context.as_ref());
            }
        }
    }
    let mut logical_snapshot = std::collections::HashMap::new();
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
                    format!("Failed to serialize logical-head snapshot failure: {serialize_error}")
                });
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
    let mut logical_mutations = Vec::new();
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

    let first_file_change_index = first_file_change.as_ref().map(|change| change.index);
    let had_file_change = first_file_change.is_some();
    let mut publication_requirement =
        crate::merge_requests::queries::PublicationRequirement::DeferredUntilPublication;
    let mut post_seal_publication = None;
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
            &logical_mutations,
        )
        .await
        {
            Ok(CommitOutcome::Done(mut completed)) => {
                commit = completed.report.take();
                publication_requirement = completed.publication_requirement;
                post_seal_publication = completed.publication;
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
        drop(_seal_phase);
    }
    drop(write_store_guard);

    if commit
        .as_ref()
        .and_then(|report| report.sha.as_ref())
        .is_some()
    {
        for change in &recorded_changes {
            if let Err(error) = record_file_change_async(
                orch,
                &request.cwd,
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
    if publication_requirement
        == crate::merge_requests::queries::PublicationRequirement::RequiredForOpenPr
    {
        let _phase = store_trace.as_ref().map(|trace| trace.phase("origin push"));
        let vcs = crate::mcp::vcs::resolve_worktree_vcs(orch, std::path::Path::new(&request.cwd));
        if let Err(error) = crate::mcp::vcs::publish_required_origin(
            vcs.as_ref(),
            std::path::Path::new(&request.cwd),
        ) {
            let index = first_file_change_index.unwrap_or(0);
            let sealed_sha = commit.as_ref().and_then(|report| report.sha.clone());
            let error = format!(
                "Commit {} was sealed locally but remains unpublished because the required open-PR origin push failed: {error}. Retry a file-touching write to publish the current bookmark.",
                sealed_sha.as_deref().unwrap_or("(unknown)")
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
            return change_report_json(applied, failures, commit, atomic);
        }
    } else if had_file_change {
        if let Some(trace) = &store_trace {
            trace.deferred("origin push deferred");
        }
    }
    if let Some(publication) = post_seal_publication {
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

    // The file-seal guard has been released. Propagation takes the same store
    // mutex, so it must run here rather than inside finalize_file_commit.
    if let Err(error) =
        crate::mcp::vcs::acknowledge_logical_bookmark_advance(orch, bookmark_observation.as_ref())
    {
        let index = first_file_change_index.unwrap_or(0);
        failures.push(ChangeFailure {
            index,
            target: payload
                .changes
                .get(index)
                .map(|change| change.target.clone())
                .unwrap_or_else(|| "file:".to_string()),
            mode: payload
                .changes
                .get(index)
                .map(|change| mode_name(change.mode).to_string())
                .unwrap_or_else(|| "patch".to_string()),
            kind: "publication".to_string(),
            error: format!(
                "Managed branch advancement was committed, but logical bookmark validation failed: {error}"
            ),
        });
    }

    // Synchronous when:write check runner: a write that sealed a source-touching
    // commit fires the affected when:write checks against that sealed commit,
    // streams their output live into this tool's transcript, runs them to
    // completion, and returns a compact inline pass/fail line appended to the
    // change report. Gated on an actual file change that produced a real commit
    // sha — a cairn://-resource-only write has no file change and never triggers.
    let check_summary: Option<String> = if had_file_change
        && commit
            .as_ref()
            .and_then(|report| report.sha.clone())
            .is_some()
    {
        let run_context = crate::mcp::handlers::run_context::lookup_run(&orch.db.local, request)
            .await
            .ok();
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
            crate::execution::checks::cancel_stale_review_on_branch_advance(orch, &ctx.job_id);
        }
        crate::execution::checks::run_write_checks_after_seal(
            orch,
            run_context.as_ref(),
            &request.cwd,
            &tool_use_id,
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
            return append_check_summary(format!("{report}\n\n{blocking_result}"), &check_summary);
        }
        if applied.is_empty() {
            return append_check_summary(blocking_result, &check_summary);
        }
        let summary = applied
            .iter()
            .map(|change| change.summary.clone())
            .collect::<Vec<_>>()
            .join("; ");
        return append_check_summary(
            format!(
                "Applied {} change(s): {}\n\n{}",
                applied.len(),
                summary,
                blocking_result
            ),
            &check_summary,
        );
    }

    append_check_summary(
        change_report_json(applied, failures, commit, atomic),
        &check_summary,
    )
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
        assert!(!source.contains(&["recover_stale", "file_commit"].concat()));
        assert!(!source.contains(&["update", "_stale"].concat()));
    }
}
