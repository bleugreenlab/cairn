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
pub(crate) use process::{build_agent_spawn_config, cache_checkpoint_callback, MAX_BUFFER_SIZE};
pub(crate) use redact::redact_command;
pub(crate) use sandbox_policy::build_run_sandbox_policy;
pub use types::{
    CheckStatusEntry, CheckStatusPayload, RunCompletePayload, RunItem, RunItemPayload,
    RunOutputPayload, RunPayload, TerminalWaitEvent, TerminalWaitKind, WaitDuration, WaitFor,
};

use crate::mcp::vcs::{acquire_store_lock, STORE_LOCK_TIMEOUT};
use commit_barrier::{run_commit_barrier, CommitBarrierOutcome};
use std::path::Path;

#[derive(Debug, Clone)]
pub(crate) struct PublishedSlotDelta {
    pub commit: String,
    pub patch: String,
    pub paths: Vec<String>,
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
    author: Option<&GitAuthor>,
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
    let publication =
        publish_visible_slot_delta(orch, &target, request, delta, message, author).await?;
    if publication.consume_receipt {
        if let Err(error) = finalize_delta_receipt(orch, &target, delta).await {
            log::warn!("system-fix commit sealed but delta receipt remains unconsumed: {error}");
        }
    }
    let patch = publication.patch;
    let commit = publication.landed.head;
    let paths = crate::jj::parse_git_diff(&patch)
        .into_iter()
        .map(|change| change.path)
        .collect();
    Ok(PublishedSlotDelta {
        commit,
        patch,
        paths,
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

struct VisibleSlotPublication {
    consume_receipt: bool,
    landed: cairn_vcs::LogicalHeadPublication,
    patch: String,
}

fn resolve_runner_publication_target(
    config_dir: &Path,
    context: &ManagedWorkspaceContext,
    store_lock: &Path,
    request: &CellRequest,
) -> Result<RunnerPublicationTarget, String> {
    let target = crate::execution::jobs::workspace_identity::resolve_managed_workspace_git_target(
        config_dir,
        context,
        store_lock,
        "build-slot publication",
    )?;

    validate_publication_identity(&context.identity.project_id, request)?;
    let Some(request_repository) = request.repository.colocated_path() else {
        return Err(format!(
            "build-slot publication repository mismatch: runner workspace {} requires colocated runner repository {}, but request used managed objects",
            target.workspace.display(),
            target.repository.display()
        ));
    };
    let request_repository = std::fs::canonicalize(request_repository).map_err(|error| {
        format!("canonicalize build-slot request repository {request_repository}: {error}")
    })?;
    if request_repository != target.repository {
        return Err(format!(
            "build-slot publication repository mismatch: request resolves to {}, managed workspace resolves to {}",
            request_repository.display(),
            target.repository.display()
        ));
    }

    Ok(RunnerPublicationTarget {
        project_id: context.identity.project_id.clone(),
        repository_identity: request.repository.identity(),
        repository: target.repository,
        store_dir: target.store_dir,
        git_common_dir: target.git_common_dir,
        branch: context.identity.branch.clone(),
    })
}

// Caller holds the canonical per-store lock for the full visibility window.
async fn publish_visible_slot_delta(
    orch: &Orchestrator,
    target: &RunnerPublicationTarget,
    request: &CellRequest,
    delta: &crate::fleet::MutationDelta,
    message: &str,
    author: Option<&GitAuthor>,
) -> Result<VisibleSlotPublication, String> {
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
    let mode = if message == "^" {
        cairn_vcs::PublicationMode::Amend
    } else {
        cairn_vcs::PublicationMode::Child {
            description: message.to_string(),
            author: author.map(|author| cairn_vcs::PublicationAuthor {
                name: author.name.clone(),
                email: author.email.clone(),
            }),
        }
    };
    let store = target.store_dir.clone();
    let branch = target.branch.clone();
    let expected = delta.base_commit.clone();
    let proposed = delta.delta_commit.clone();
    let landed = tokio::task::spawn_blocking(move || {
        cairn_vcs::publish_logical_head(&store, &branch, &expected, &proposed, mode)
    })
    .await
    .map_err(|error| format!("join logical-head delta publication: {error}"))??;
    let patch = git_output(
        repository,
        &[
            "diff",
            "--no-ext-diff",
            "--binary",
            &delta.base_commit,
            &landed.head,
        ],
        "capture logical-head delta patch",
    )?;
    Ok(VisibleSlotPublication {
        consume_receipt,
        landed,
        patch,
    })
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

use hygiene::{check_cd_commands, checkout_has_tracked_changes};
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
            Ok(RunSpec::McpCall(_) | RunSpec::ReplSend { .. }) | Err(_) => None,
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

const MAX_RUN_ITEM_TIMEOUT_MS: u32 = 600_000;

fn configured_process_timeout_ms(default_timeout_seconds: u64) -> u32 {
    default_timeout_seconds
        .saturating_mul(1_000)
        .min(u64::from(MAX_RUN_ITEM_TIMEOUT_MS)) as u32
}

fn apply_default_process_timeout(
    resolved: &mut [(String, Result<RunSpec, String>)],
    default_timeout_ms: u32,
) {
    for (_, spec) in resolved {
        match spec {
            Ok(RunSpec::Shell { timeout, .. } | RunSpec::Script { timeout, .. })
                if timeout.is_none() =>
            {
                *timeout = Some(default_timeout_ms);
            }
            _ => {}
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
}

use crate::execution::jobs::workspace_identity::ManagedWorkspaceContext;
use crate::fleet::{CellOutcome, CellPriority, CellRequest, MutationPolicy};
use cairn_common::executor_protocol::RepositoryLocator;

use crate::mcp::git::GitAuthor;
use crate::mcp::types::McpCallbackRequest;
use crate::models::Fence;
use crate::orchestrator::Orchestrator;
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

/// Handle run tool call - an ordered batch of synchronous shell commands and
/// skill-script invocations. Parallel by default; `sequential` runs in order.
pub async fn handle_run(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: RunPayload = match super::parse_payload(request) {
        Ok(payload) => payload,
        Err(error) => return run_envelope(error, Vec::new()),
    };

    if payload.commands.is_empty() {
        return run_envelope(
            "Invalid payload: `commands` must contain at least one item".to_string(),
            Vec::new(),
        );
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
    let commit_present = payload.commit_msg.is_some();

    // Advisory nudge: if any shell item wraps an interpreter one-liner
    // (`python3 -c`, `bun -e`, a `python <<EOF` heredoc, …), surface a one-line
    // tip pointing at inline `{code, interpreter}`. Computed once here and
    // appended to the composed output below; never affects success/exit status.
    let interpreter_tip = tip::interpreter_tip(&payload.commands);

    // Look up streaming/run context once for the whole batch. Prefer the
    // callback's run_id when present: cwd lookups are only a fallback and can
    // miss or pick the wrong run when multiple runs share a repo/worktree path.
    // The live bash preview subscribes by this run id, so using the exact
    // request run is what wires emitted `run-output` chunks to the visible tool.
    let run_context = super::run_context::lookup_run(&orch.db.local, request)
        .await
        .ok();

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

    // Resolve every item before any placement or managed-workspace preparation.
    // This is the dispatch seam where process-shaped searches can be served by
    // the in-process grep engine without consuming a build-slot admission.
    let mut resolved: Vec<(String, Result<RunSpec, String>)> =
        Vec::with_capacity(payload.commands.len());
    for item in &payload.commands {
        resolved.push(resolve_run_item(orch, request, run_context.as_ref(), item).await);
    }
    let sequential = payload.sequential.unwrap_or(false);
    let stop_on_error = payload.stop_on_error.unwrap_or(true);
    {
        if let Some(outcomes) = search::try_run_search_batch(
            orch,
            run_context.as_ref(),
            &cwd,
            &resolved,
            branch_target.is_some(),
            sequential,
            stop_on_error,
        )
        .await
        {
            let text = compose_run_output(&outcomes);
            return run_envelope(text, collect_run_images(outcomes));
        }
    }

    // Changes can only happen in a worktree. A non-jj cwd is the project's live
    // checkout behind a long-lived triage / read-only agent or another
    // no-worktree run. A `commit_msg` means the caller intends to commit, which
    // requires a worktree; reject the whole batch BEFORE running any command, so
    // nothing executes against — or is left in — the user's live checkout. (The
    // commit barrier itself cannot help here: NonWorktreeVcs is a read-only
    // no-op that must never seal or revert the user's checkout.)
    if commit_present && !crate::jj::is_jj_dir(std::path::Path::new(&cwd)) {
        return run_envelope(
            "Commits require a worktree. This agent runs on the project's live checkout \
             (no worktree), so a run carrying commit_msg cannot commit and no commands were \
             executed. Changes can only be made in a worktree."
                .to_string(),
            Vec::new(),
        );
    }

    // Look up the project's primary checkout path for cd-command advisory notes.
    let repo_root = if let Some(ctx) = run_context.as_ref() {
        crate::config::get_project_path(&orch.db.local, &ctx.project_id)
            .await
            .ok()
            .and_then(|p| p.to_str().map(|s| s.to_string()))
    } else {
        None
    };
    let run_hygiene_applies = matches!(
        super::fence::resolve_run_fence(orch, request).await,
        Some((_run_id, Fence::Ask | Fence::Deny))
    );
    // Resolve the worktree's VCS backend once (jj for a worktree; the read-only
    // NonWorktreeVcs for the project's live checkout) and capture the pre-batch
    // snapshot through it.
    // Pre-flight staleness reconcile: heal a stale / behind-its-branch-tip working
    // copy BEFORE the batch runs, serialized on the same per-store jj lock the
    // base-advance reconcile and merge-fold hold, so it can never race a concurrent
    // rebase (the hazard a hand-run `jj workspace update-stale` hit). Resolved once
    // here and reused by the post-batch commit barrier below. Best-effort: a
    // failure leaves the seal-time stale arm as the mid-batch fallback.
    let store_lock = if branch_target.is_none() {
        crate::mcp::vcs::resolve_store_lock(orch, request).await
    } else {
        None
    };
    let managed_context = if branch_target.is_none() {
        let _guard = match acquire_store_lock(
            orch,
            store_lock.as_deref(),
            "run managed workspace preparation",
            STORE_LOCK_TIMEOUT,
        )
        .await
        {
            Ok(guard) => guard,
            Err(error) => return run_envelope(error, Vec::new()),
        };
        match crate::mcp::vcs::prepare_managed_workspace(orch, request).await {
            Ok(context) => context,
            Err(error) => return run_envelope(error, Vec::new()),
        }
    } else {
        None
    };
    let bookmark_observation =
        crate::mcp::vcs::observe_managed_bookmark(orch, managed_context.as_ref());
    let vcs = if branch_target.is_none() {
        let vcs = crate::mcp::vcs::resolve_managed_worktree_vcs(
            orch,
            std::path::Path::new(&cwd),
            managed_context.as_ref(),
        );
        if vcs
            .workspace_needs_reconcile(std::path::Path::new(&cwd))
            .unwrap_or(true)
        {
            let _guard = match acquire_store_lock(
                orch,
                store_lock.as_deref(),
                "run pre-flight workspace reconcile",
                STORE_LOCK_TIMEOUT,
            )
            .await
            {
                Ok(guard) => guard,
                Err(error) => return run_envelope(error, Vec::new()),
            };
            if let Err(e) = vcs.reconcile_workspace(std::path::Path::new(&cwd)) {
                if crate::mcp::vcs::is_workspace_lineage_mismatch(&e) {
                    return run_envelope(e, Vec::new());
                }
                log::warn!("pre-flight workspace reconcile failed: {e}");
            }
        }
        Some(vcs)
    } else {
        None
    };
    // Capture the pre-batch snapshot whenever a no-`commit_msg` run could leave
    // dirt the barrier must reconcile. For a worktree this is gated on the
    // hygiene fence (Ask/Deny). For the project's LIVE checkout we capture
    // regardless of fence; a request without an execution snapshot is
    // unconfined, yet a stray write there still violates the worktree boundary
    // and must be flagged (read-only detection; never reverted). The commit_msg
    // case on a non-worktree cwd already returned early above.
    let non_worktree_cwd = !crate::jj::is_jj_dir(std::path::Path::new(&cwd));
    let status_before = if branch_target.is_none()
        && payload.commit_msg.is_none()
        && (run_hygiene_applies || non_worktree_cwd)
    {
        vcs.as_ref()
            .and_then(|vcs| vcs.snapshot(std::path::Path::new(&cwd)).ok())
    } else {
        None
    };

    // Advisory notes for cd commands targeting the worktree (redundant) or the
    // project's primary checkout (should stay in the worktree).
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

    // Worktree fence: enforcement is OS-level now (each command Cairn spawns on
    // the agent's behalf runs under a kernel filesystem sandbox; see
    // `services::sandbox`). A blocked command surfaces as a denial that
    // `run_one` adjudicates through `fence::raise_fence` after execution — no
    // up-front command-string classification.
    // Placement is a preflight batch invariant. A call may contain exactly one
    // execution class: tree-bound processes, host MCP gateway calls, or persistent
    // REPL sends. Splitting a mixed call here would violate batch ordering and the
    // single commit barrier, so reject before any item starts.
    let has_process = resolved
        .iter()
        .any(|(_, spec)| matches!(spec, Ok(RunSpec::Shell { .. } | RunSpec::Script { .. })));
    let has_mcp = resolved
        .iter()
        .any(|(_, spec)| matches!(spec, Ok(RunSpec::McpCall(_))));
    let has_repl = resolved
        .iter()
        .any(|(_, spec)| matches!(spec, Ok(RunSpec::ReplSend { .. })));
    if usize::from(has_process) + usize::from(has_mcp) + usize::from(has_repl) > 1 {
        return run_envelope(
            "A run batch may not mix tree-bound shell/script items with MCP gateway or REPL items. Split them into separate run calls.".to_string(),
            Vec::new(),
        );
    }
    if branch_target.is_some() && !has_process {
        return run_envelope(
            "The branch option applies only to tree-bound shell or script batches; MCP gateway and REPL batches run on the host.".to_string(),
            Vec::new(),
        );
    }

    let mut routed_delta = None;
    let mut routed_tracked_modifications = None;
    let mut routed_request = None;
    let mut routed_outcomes = None;
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
    } else if let Some(context) = managed_context.as_ref() {
        if has_process {
            if checkout_has_tracked_changes(orch, std::path::Path::new(&cwd)).unwrap_or(true) {
                return run_envelope(
                    "A managed process batch requires a clean workspace before build-slot placement.".to_string(),
                    Vec::new(),
                );
            }
            let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
            let base_commit = match crate::jj::head_commit(&jj, std::path::Path::new(&cwd)) {
                Ok(commit) => commit,
                Err(error) => {
                    return run_envelope(
                        format!("Run infrastructure failure: could not resolve the immutable slot base: {error}"),
                        Vec::new(),
                    )
                }
            };
            let relative_cwd = std::path::Path::new(&cwd)
                .strip_prefix(&context.identity.worktree_path)
                .ok()
                .map(|path| path.to_string_lossy().into_owned())
                .unwrap_or_default();
            Some((
                RepositoryLocator::ColocatedPath {
                    project_id: context.identity.project_id.clone(),
                    repository_id: context.identity.project_id.clone(),
                    absolute_path: context.identity.project_root.to_string_lossy().into_owned(),
                },
                base_commit,
                relative_cwd,
                MutationPolicy::AllowDelta,
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
            "Run infrastructure failure: the tree-bound batch could not be resolved to an executor repository target. The batch was not executed locally."
                .to_string(),
            Vec::new(),
        );
    }
    if has_process {
        if let Some((repository, base_commit, relative_cwd, mutation_policy)) = slot_target {
            let project_id = repository.project_id().to_string();
            let fleet_config = crate::config::settings::load_fleet(&orch.config_dir);
            let default_timeout_ms =
                configured_process_timeout_ms(fleet_config.default_timeout_seconds);
            apply_default_process_timeout(&mut resolved, default_timeout_ms);
            let (command, command_class) = build_slot_command_parts(&resolved);
            let slot_request = CellRequest {
                request_id: Uuid::new_v4().to_string(),
                attempt_id: Uuid::new_v4().to_string(),
                project_id: project_id.clone(),
                repository,
                base_commit,
                command,
                command_class,
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
                env: detached_route_env(
                    branch_target.as_ref().map(|target| target.rev.as_str()),
                    managed_context
                        .as_ref()
                        .map(|context| context.identity.branch.as_str()),
                ),
                priority: CellPriority::AgentInteractive,
                deadline_unix_ms: crate::fleet::unix_time_ms()
                    + fleet_config
                        .acquisition_deadline_seconds
                        .saturating_mul(1_000),
                timeout_ms: default_timeout_ms,
                mutation_policy,
                requesting_job_id: run_context.as_ref().map(|ctx| ctx.job_id.clone()),
                affinity_key: run_context.as_ref().map(|ctx| ctx.run_id.clone()),
                constraints: payload.constraints.clone(),
                command_resource_identity: None,
                resource_reservation: Default::default(),
                learned_estimate: None,
            };
            routed_request = Some(slot_request.clone());
            let batch = ResolvedRunBatch {
                request: request.clone(),
                run_context: run_context.clone(),
                resolved: resolved.clone(),
                tool_use_id: tool_use_id.clone(),
                stop_on_error,
                originally_sequential: sequential,
            };
            match orch.fleet.submit_run_batch(orch, slot_request, batch).await {
                CellOutcome::Unavailable { reason, diagnostic } => {
                    return run_envelope(
                        format!("Run infrastructure failure ({reason:?}): {diagnostic}. The batch was not executed locally."),
                        Vec::new(),
                    );
                }
                CellOutcome::FailedAfterExecution { diagnostic, .. } => {
                    return run_envelope(
                        format!("Build-slot run executed but could not publish its result: {diagnostic}. The batch was not rerun locally."),
                        Vec::new(),
                    );
                }
                CellOutcome::StorageFailure {
                    stage,
                    kind,
                    diagnostic,
                    ..
                } => {
                    return run_envelope(
                        format!("Build-slot storage failure ({stage:?}/{kind:?}): {diagnostic}. The batch was not rerun locally."),
                        Vec::new(),
                    );
                }
                CellOutcome::Cancelled { .. } => {
                    return run_envelope(
                        "Run cancelled while waiting for or executing in a cell.".to_string(),
                        Vec::new(),
                    );
                }
                CellOutcome::Completed {
                    output,
                    mutation_delta,
                    tracked_modifications,
                    ..
                } => match serde_json::from_str::<Vec<ItemOutcome>>(&output) {
                    Ok(mut outcomes) => {
                        for outcome in &mut outcomes {
                            if let Some(promoted) = &outcome.promoted_terminal {
                                if !outcome.body.is_empty() {
                                    outcome.body.push_str("\n\n");
                                }
                                if promoted.wake_subscribed {
                                    outcome.body.push_str(&format!(
                                        "Command still running in {0}. It is readable and killable there; you will be notified when it exits.",
                                        promoted.uri
                                    ));
                                } else {
                                    outcome.body.push_str(&format!(
                                        "Command still running in {0}. It is readable and killable there; automatic exit notification could not be registered.",
                                        promoted.uri
                                    ));
                                }
                            }
                        }
                        routed_outcomes = Some(outcomes);
                        routed_delta = mutation_delta;
                        routed_tracked_modifications = tracked_modifications;
                    }
                    Err(error) => {
                        return run_envelope(
                                format!("Build-slot run completed but its result could not be decoded: {error}"),
                                Vec::new(),
                            );
                    }
                },
            }
        }
    }

    let outcomes = if let Some(outcomes) = routed_outcomes {
        outcomes
    } else if sequential {
        let mut outcomes: Vec<ItemOutcome> = Vec::with_capacity(resolved.len());
        for (index, (header, spec)) in resolved.into_iter().enumerate() {
            let stream_id = run_item_stream_id(&tool_use_id, index);
            let outcome = run_one(
                orch,
                request,
                &cwd,
                &stream_id,
                run_context.as_ref(),
                true,
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
                    true,
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

    // If any item durably suspended on a worktree-fence approval, return the
    // suspend marker for the whole call; the run re-drives the batch on resume.
    if outcomes.iter().any(|o| o.suspended) {
        return run_envelope(
            "Run suspended pending worktree fence approval; resume will continue once it is answered."
                .to_string(),
            Vec::new(),
        );
    }

    let mut result = compose_run_output(&outcomes);

    if branch_target.is_some() {
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
        if !cd_advisory.is_empty() {
            if !result.is_empty() {
                result.push_str("\n\n");
            }
            result.push_str(&cd_advisory);
        }
        if let Some(tip) = interpreter_tip {
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
        let images = collect_run_images(outcomes);
        return run_envelope(text, images);
    }

    // Top-level commit barrier / hygiene gate. The session-archival scheme
    // requires the worktree to exactly equal HEAD after every run; the barrier
    // either commits the worktree or restores it to HEAD. Author identity and
    // event emission stay here (they need the orchestrator); the git decision
    // lives in `run_commit_barrier` so it can be tested without one.
    let all_ok = outcomes.iter().all(|o| o.succeeded);
    let worktree_path = std::path::Path::new(&cwd);
    let author = match payload.commit_msg.as_deref() {
        Some(_) => run_context
            .as_ref()
            .and_then(|ctx| orch.resolve_git_identity_for_project(Some(&ctx.project_id)))
            .map(|(name, email)| GitAuthor::new(name, email)),
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
        store_lock.as_deref(),
        "run commit barrier publication",
        STORE_LOCK_TIMEOUT,
    )
    .await
    {
        Ok(_store_guard) => {
            let routed = match (
                payload.commit_msg.as_deref(),
                routed_delta.as_ref(),
                routed_request.as_ref(),
                managed_context.as_ref(),
            ) {
                (Some(message), Some(delta), Some(request), Some(context)) => Some(match store_lock.as_deref() {
                    Some(store) => match resolve_runner_publication_target(
                        &orch.config_dir, context, store, request,
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
                        Err(error) => Err(error),
                    },
                    None => Err(
                        "build-slot publication has no resolved shared-store lock path".to_string(),
                    ),
                }),
                _ => None,
            };
            match routed {
                None => run_commit_barrier(
                        vcs.as_ref().expect("ambient run always resolves a VCS").as_ref(),
                        worktree_path,
                        payload.commit_msg.as_deref(),
                        all_ok,
                        status_before.as_ref(),
                        author.as_ref(),
                    ),
                Some(Ok((publication, target))) => {
                    let mut message = format!("✓ Committed changes ({})", publication.landed.head);
                    let (additions, deletions) = crate::jj::parse_git_patch(&publication.patch)
                        .iter()
                        .fold((0, 0), |(add, del), change| {
                            (add + change.additions, del + change.deletions)
                        });
                    if additions > 0 || deletions > 0 {
                        message.push_str(&format!(" +{additions}/-{deletions}"));
                    }
                    if let Some(note) = publication.landed.amend_note.as_deref() {
                        message.push_str(&format!(" — {note}"));
                    }
                    if publication.consume_receipt {
                        if let Some(delta) = routed_delta.as_ref() {
                            if let Err(error) = finalize_delta_receipt(orch, &target, delta).await {
                                message.push_str(&format!(
                                    " — ⚠️ commit landed but delta receipt remains unconsumed: {error}"
                                ));
                            }
                        }
                    }
                    drop(_store_guard);
                    if let (Some(context), Ok((_run, db))) = (
                        managed_context.as_ref(),
                        super::run_context::lookup_run_routed(&orch.db, request).await,
                    ) {
                        if let Err(error) = crate::mcp::vcs::publish_sealed_commit_pack(
                            &db,
                            &context.identity.project_id,
                            &context.identity.project_root,
                            &publication.landed.head,
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
                        committed_patch: Some(publication.patch),
                    }
                }
                Some(Err(error)) => CommitBarrierOutcome {
                    message: format!("⚠️ {error}. The routed batch was not rerun locally."),
                    worktree_changed: false,
                    committed: false,
                    committed_patch: None,
                },
            }
        }
        Err(error) => CommitBarrierOutcome {
            message: format!("⚠️ {error} Nothing was committed and the working copy was PRESERVED exactly. Retry with a trivial `run` carrying the same `commit_msg`; the commit barrier will seal any remaining dirty worktree."),
            worktree_changed: false,
            committed: false,
            committed_patch: None,
        },
    };
    // The barrier guard is gone before propagation: the canonical reconciler
    // takes the same per-store mutex. Observe the actual bookmark, not just the
    // barrier's committed flag, so a plain `jj rebase` in a clean run batch also
    // propagates its new parent tip.
    if let Err(error) =
        crate::mcp::vcs::propagate_observed_bookmark_advance(orch, bookmark_observation.as_ref())
            .await
    {
        if !result.is_empty() {
            result.push_str("\n\n");
        }
        result.push_str(&format!(
            "⚠️ Your managed branch advanced and remains committed, but downstream workspace reconciliation failed: {error}"
        ));
    }

    // Record the sealed commit's file changes so a run-path commit populates the
    // same `file_changes` cache the write path does. This is what makes the
    // node's diff facet appear, keeps the per-node change summary correct after
    // worktree teardown, and feeds every other `file_changes` consumer (issue
    // `/changed` cache fallback, PR data, analytics). Best-effort per file,
    // mirroring the write path.
    //
    // This MUST run BEFORE the `worktree-changed` emit below: that event
    // invalidates the DB-driven node change summary, whose refetch reads
    // `file_changes`. The write path holds the same contract (record rows, then
    // emit) for exactly this reason — emitting first races the async inserts and
    // can cache an empty summary with no later invalidation to correct it.
    if barrier.committed {
        if let Some(patch) = barrier.committed_patch.as_deref() {
            for change in crate::jj::parse_git_patch(patch) {
                if let Err(e) = super::write::file_mutations::record_file_change_async(
                    orch,
                    &cwd,
                    &change.path,
                    &change.status,
                    change.additions,
                    change.deletions,
                    change.previous_path.as_deref(),
                )
                .await
                {
                    log::warn!("Failed to record run-path file change: {}", e);
                }
            }
        }
    }
    if barrier.worktree_changed {
        let _ = orch.services.emitter.emit(
            "worktree-changed",
            serde_json::json!({"worktree_path": cwd}),
        );
    }
    if !barrier.message.is_empty() {
        if !result.is_empty() {
            result.push_str("\n\n");
        }
        result.push_str(&barrier.message);
    }
    // Synchronous when:write check runner: a sealed source-touching commit fires
    // the affected when:write checks against that commit, streams their output
    // live into this tool's transcript, runs them to completion, and appends a
    // compact inline pass/fail line. Gated on an actually-landed commit
    // (`committed` is true only with commit_msg + a successful seal).
    if barrier.committed {
        // A commit just sealed → the branch advanced. Cancel any in-flight
        // when:review suite for this job so its heavy concurrent compiles stop
        // starving this commit's own when:write checks (below) and the agent's
        // next manual check run; the review cadence relaunches fresh at the next
        // turn-end. See cancel_stale_review_on_branch_advance for the rationale
        // and the deliberate job-id scoping.
        if let Some(ctx) = run_context.as_ref() {
            crate::execution::checks::cancel_stale_review_on_branch_advance(orch, &ctx.job_id);
        }
        if let Some(summary) = crate::execution::checks::run_write_checks_after_seal(
            orch,
            run_context.as_ref(),
            &cwd,
            &tool_use_id,
        )
        .await
        {
            if !result.is_empty() {
                result.push_str("\n\n");
            }
            result.push_str(&summary);
        }
    }
    // A commit_msg on a non-worktree cwd (the project's live checkout) cannot
    // commit: changes only happen in worktrees. The commands already ran, so
    // don't fail the run — just note that nothing was committed.
    if payload.commit_msg.is_some() && !crate::jj::is_jj_dir(worktree_path) {
        let note = "Note: commits require a worktree. This agent runs on the project's live \
                    checkout (no worktree), so the commands ran but nothing was committed.";
        if !result.is_empty() {
            result.push_str("\n\n");
        }
        result.push_str(note);
    }

    if !cd_advisory.is_empty() {
        if !result.is_empty() {
            result.push_str("\n\n");
        }
        result.push_str(&cd_advisory);
    }
    if let Some(tip) = interpreter_tip {
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
    let images = collect_run_images(outcomes);
    run_envelope(text, images)
}

fn detached_route_env(
    branch_target_rev: Option<&str>,
    managed_workspace_branch: Option<&str>,
) -> Vec<(String, String)> {
    branch_target_rev
        .or(managed_workspace_branch)
        .map(|branch| vec![("CAIRN_WORKTREE_BRANCH".to_string(), branch.to_string())])
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::CellCommandClass;

    fn shell(timeout: Option<u32>) -> (String, Result<RunSpec, String>) {
        (
            "shell".into(),
            Ok(RunSpec::Shell {
                command: "true".into(),
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

    #[test]
    fn configured_process_timeout_converts_seconds_and_caps_item_budget() {
        assert_eq!(configured_process_timeout_ms(1_800), 600_000);
        assert_eq!(configured_process_timeout_ms(u64::MAX), 600_000);
    }

    #[test]
    fn default_process_timeout_fills_omissions_without_overriding_explicit_ms() {
        let default_timeout_ms = configured_process_timeout_ms(30);
        let mut resolved = vec![shell(None), script(None), shell(Some(1_800))];

        apply_default_process_timeout(&mut resolved, default_timeout_ms);

        assert_eq!(process_timeout(&resolved[0]), Some(30_000));
        assert_eq!(process_timeout(&resolved[1]), Some(30_000));
        assert_eq!(process_timeout(&resolved[2]), Some(1_800));
    }

    #[test]
    fn detached_route_env_covers_managed_and_explicit_branch_routes() {
        assert_eq!(
            detached_route_env(None, Some("agent/CAIRN-2929-builder-0")),
            [(
                "CAIRN_WORKTREE_BRANCH".into(),
                "agent/CAIRN-2929-builder-0".into()
            )]
        );
        assert_eq!(
            detached_route_env(Some("feature/dev-instance"), None),
            [(
                "CAIRN_WORKTREE_BRANCH".into(),
                "feature/dev-instance".into()
            )]
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
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::AgentInteractive,
            deadline_unix_ms: 1,
            timeout_ms: 1,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            constraints: None,
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
                timeout: None,
            }),
        )];

        let (command, command_class) = build_slot_command_parts(&resolved);

        assert_eq!(command, "Run Rust suite");
        assert_eq!(command_class, CellCommandClass::CargoTest);
    }
}
