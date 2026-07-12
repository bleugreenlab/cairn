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
mod tip;
mod types;

pub(crate) use checks::{check_stream_id, run_check_command, run_item_stream_id, CheckExecResult};
pub(crate) use process::{
    apply_non_interactive_pager_env_to_pty, build_agent_spawn_config, cache_checkpoint_callback,
    scrub_dev_instance_routing_pty, MAX_BUFFER_SIZE,
};
pub use redact::redact_command;
pub(crate) use sandbox_policy::build_run_sandbox_policy;
pub(crate) use types::PromotedTerminal;
pub use types::{
    CheckStatusEntry, CheckStatusPayload, RunCompletePayload, RunItem, RunItemPayload,
    RunOutputPayload, RunPayload,
};

use commit_barrier::{run_commit_barrier, CommitBarrierOutcome};
use std::path::Path;
use std::time::Duration;

const STORE_LOCK_TIMEOUT: Duration = Duration::from_secs(600);
const STORE_BUSY_MESSAGE: &str =
    "The project's version-control store is busy behind a long-running operation; retry this run.";

async fn acquire_store_lock(
    orch: &Orchestrator,
    store: Option<&Path>,
    operation: &str,
    timeout: Duration,
) -> Result<Option<crate::orchestrator::JjStoreGuard>, ()> {
    match store {
        Some(store) => orch
            .acquire_jj_store_lock_with_timeout(store, operation, Some(timeout))
            .await
            .map(Some),
        None => Ok(None),
    }
}
fn make_delta_objects_available(
    orch: &Orchestrator,
    repository: &std::path::Path,
    request: &BuildSlotRequest,
    delta: &crate::build_slots::MutationDelta,
) -> Result<bool, String> {
    let Some(receipt) = delta.upload_receipt.as_ref() else {
        verify_available_delta(repository, delta)?;
        return Ok(false);
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
    cairn_codec::transfer::install_pack(&objects_dir, &validated)
        .map_err(|error| format!("install managed delta pack: {error}"))?;
    cairn_codec::transfer::verify_commit_closure(&objects_dir, &[], &delta.delta_commit)
        .map_err(|error| format!("verify imported managed delta closure: {error}"))?;
    verify_available_delta(repository, delta)?;
    Ok(true)
}

fn verify_available_delta(
    repository: &std::path::Path,
    delta: &crate::build_slots::MutationDelta,
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

fn temporary_delta_ref(
    request: &BuildSlotRequest,
    delta: &crate::build_slots::MutationDelta,
) -> String {
    fn safe(value: &str) -> String {
        value
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                    character
                } else {
                    '-'
                }
            })
            .collect()
    }
    let abbreviated = delta.delta_commit.get(..12).unwrap_or(&delta.delta_commit);
    format!(
        "refs/heads/cairn-build-delta-{}-{}-{abbreviated}",
        safe(&request.request_id),
        safe(&request.attempt_id),
    )
}

#[derive(Debug)]
struct RunnerPublicationTarget {
    project_id: String,
    repository_identity: cairn_common::executor_protocol::RepositoryIdentity,
    repository: std::path::PathBuf,
    git_common_dir: std::path::PathBuf,
    store_dir: std::path::PathBuf,
    workspace: std::path::PathBuf,
}

fn canonical_path(path: &Path, context: &str) -> Result<std::path::PathBuf, String> {
    std::fs::canonicalize(path).map_err(|error| format!("{context} {}: {error}", path.display()))
}

fn resolve_git_common_dir(repository: &Path) -> Result<std::path::PathBuf, String> {
    let common = std::path::PathBuf::from(git_output(
        repository,
        &["rev-parse", "--git-common-dir"],
        "resolve runner repository Git common directory",
    )?);
    canonical_path(
        &if common.is_absolute() {
            common
        } else {
            repository.join(common)
        },
        "canonicalize runner repository Git common directory",
    )
}

fn resolve_runner_publication_target(
    config_dir: &Path,
    context: &ManagedWorkspaceContext,
    store_lock: &Path,
    request: &BuildSlotRequest,
) -> Result<RunnerPublicationTarget, String> {
    let repository = canonical_path(
        &context.identity.project_root,
        "canonicalize managed project repository",
    )?;
    let workspace = canonical_path(
        &context.identity.worktree_path,
        "canonicalize managed workspace",
    )?;
    let store_dir = canonical_path(store_lock, "canonicalize managed shared store")?;
    let expected_store = canonical_path(
        &crate::jj::project_store_dir(config_dir, &context.identity.project_root),
        "canonicalize expected managed shared store",
    )?;
    if store_dir != expected_store {
        return Err(format!(
            "build-slot publication lock mismatch: locked shared store resolves to {}, managed project store resolves to {}",
            store_dir.display(),
            expected_store.display()
        ));
    }

    if request.project_id != context.identity.project_id
        || request.repository.project_id() != context.identity.project_id
        || request.repository.repository_id() != context.identity.project_id
    {
        return Err(format!(
            "build-slot publication identity mismatch: request project/repository {}/{} does not match managed project {} at {}",
            request.repository.project_id(),
            request.repository.repository_id(),
            context.identity.project_id,
            repository.display()
        ));
    }
    let Some(request_repository) = request.repository.colocated_path() else {
        return Err(format!(
            "build-slot publication repository mismatch: runner workspace {} requires colocated runner repository {}, but request used managed objects",
            workspace.display(),
            repository.display()
        ));
    };
    let request_repository = canonical_path(
        Path::new(request_repository),
        "canonicalize build-slot request repository",
    )?;
    if request_repository != repository {
        return Err(format!(
            "build-slot publication repository mismatch: request resolves to {}, managed workspace resolves to {}",
            request_repository.display(),
            repository.display()
        ));
    }

    let workspace_repo_pointer = workspace.join(".jj").join("repo");
    let workspace_repo = std::fs::read_to_string(&workspace_repo_pointer).map_err(|error| {
        format!(
            "read managed workspace repository pointer {}: {error}",
            workspace_repo_pointer.display()
        )
    })?;
    let workspace_repo = std::path::PathBuf::from(workspace_repo.trim());
    let workspace_repo = canonical_path(
        &if workspace_repo.is_absolute() {
            workspace_repo
        } else {
            workspace_repo_pointer
                .parent()
                .unwrap_or(&workspace)
                .join(workspace_repo)
        },
        "resolve managed workspace .jj/repo",
    )?;
    let store_repo = canonical_path(
        &store_dir.join(".jj").join("repo"),
        "resolve managed shared-store .jj/repo",
    )?;
    if workspace_repo != store_repo {
        return Err(format!(
            "build-slot publication store mismatch: workspace .jj/repo resolves to {}, locked shared store resolves to {}",
            workspace_repo.display(),
            store_repo.display()
        ));
    }

    let git_common_dir = resolve_git_common_dir(&repository)?;
    let git_target_file = store_repo.join("store").join("git_target");
    let git_target = std::fs::read_to_string(&git_target_file).map_err(|error| {
        format!(
            "read shared-store Git target {}: {error}",
            git_target_file.display()
        )
    })?;
    let git_target = std::path::PathBuf::from(git_target.trim());
    let git_target = canonical_path(
        &if git_target.is_absolute() {
            git_target
        } else {
            git_target_file
                .parent()
                .unwrap_or(&store_repo)
                .join(git_target)
        },
        "canonicalize shared-store Git target",
    )?;
    if git_target != git_common_dir {
        return Err(format!(
            "build-slot publication Git backend mismatch: shared store targets {}, runner repository common directory is {}",
            git_target.display(),
            git_common_dir.display()
        ));
    }

    Ok(RunnerPublicationTarget {
        project_id: context.identity.project_id.clone(),
        repository_identity: request.repository.identity(),
        repository,
        git_common_dir,
        store_dir,
        workspace,
    })
}

// Caller holds the canonical per-store lock for the full visibility window.
fn publish_visible_slot_delta(
    orch: &Orchestrator,
    target: &RunnerPublicationTarget,
    request: &BuildSlotRequest,
    delta: &crate::build_slots::MutationDelta,
) -> Result<(), String> {
    debug_assert_eq!(target.project_id, request.project_id);
    debug_assert_eq!(target.repository_identity, request.repository.identity());
    debug_assert!(target.git_common_dir.is_absolute());
    let repository = &target.repository;
    let consume_receipt = make_delta_objects_available(orch, repository, request, delta)?;
    let reference = temporary_delta_ref(request, delta);
    let existing = git_output(
        repository,
        &["rev-parse", "--verify", "--quiet", &reference],
        "inspect temporary build-delta reference",
    )
    .ok();
    match existing.as_deref() {
        Some(commit) if commit == delta.delta_commit => {}
        Some(commit) => {
            return Err(format!(
                "temporary build-delta ref collision: {reference} points to {commit}"
            ))
        }
        None => {
            let absent = "0".repeat(delta.delta_commit.len());
            git_output(
                repository,
                &["update-ref", &reference, &delta.delta_commit, &absent],
                "publish temporary build-delta reference",
            )?;
        }
    }

    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    // Executor slots may seal against a separate backend. Once their objects reach
    // the runner, however, the temporary ref and materializing workspace store
    // must address this one validated runner Git backend.
    let primary = (|| {
        crate::jj::import_git(&jj, &target.store_dir)
            .map_err(|error| format!("import build-slot delta reference: {error}"))?;
        jj.run(
            &target.store_dir,
            &[
                "log",
                "-r",
                &delta.delta_commit,
                "--no-graph",
                "-T",
                "commit_id",
            ],
            "verify build-slot delta visibility",
        )?;
        verify_available_delta(repository, delta)?;
        materialize_slot_delta(orch, target, delta)
    })();
    let cleanup = (|| {
        git_output(
            repository,
            &["update-ref", "-d", &reference, &delta.delta_commit],
            "delete temporary build-delta reference",
        )?;
        crate::jj::import_git(&jj, &target.store_dir)
            .map_err(|error| format!("import temporary build-delta reference deletion: {error}"))?;
        Ok::<(), String>(())
    })();
    match (primary, cleanup) {
        (Err(primary), Err(cleanup)) => Err(format!(
            "{primary}; temporary-ref cleanup also failed: {cleanup}"
        )),
        (Err(primary), _) => Err(primary),
        (Ok(()), Err(cleanup)) => Err(format!(
            "build-slot delta materialized but temporary-ref cleanup failed: {cleanup}"
        )),
        (Ok(()), Ok(())) => {
            if consume_receipt {
                let receipt = delta.upload_receipt.as_ref().ok_or_else(|| {
                    "managed delta receipt disappeared before consumption".to_string()
                })?;
                orch.object_plane.consume_staged_delta(receipt)?;
            }
            Ok(())
        }
    }
}

fn materialize_slot_delta(
    orch: &Orchestrator,
    target: &RunnerPublicationTarget,
    delta: &crate::build_slots::MutationDelta,
) -> Result<(), String> {
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let worktree = &target.workspace;
    let current = crate::jj::head_commit(&jj, worktree)?;
    if current != delta.base_commit {
        return Err(format!(
            "build-slot publication conflict: workspace base changed from {} to {}",
            delta.base_commit, current
        ));
    }
    if checkout_has_tracked_changes(orch, worktree)? {
        return Err(
            "build-slot publication conflict: agent workspace is no longer clean".to_string(),
        );
    }
    jj.run(
        worktree,
        &["restore", "--from", &delta.delta_commit, "--into", "@"],
        "materialize build-slot delta",
    )?;
    if crate::jj::head_commit(&jj, worktree)? != delta.base_commit {
        return Err(
            "build-slot publication conflict: materialization changed the sealed base".to_string(),
        );
    }
    Ok(())
}
use hygiene::{check_cd_commands, checkout_has_tracked_changes};
use output::{collect_run_images, compose_run_output, run_envelope};
use process::run_one;
use resolve::resolve_run_item;
use types::ItemOutcome;
pub(crate) use types::RunSpec;

fn apply_default_process_timeout(
    resolved: &mut [(String, Result<RunSpec, String>)],
    default_timeout_seconds: u32,
) {
    for (_, spec) in resolved {
        match spec {
            Ok(RunSpec::Shell { timeout, .. } | RunSpec::Script { timeout, .. })
                if timeout.is_none() =>
            {
                *timeout = Some(default_timeout_seconds);
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

use crate::build_slots::{BuildSlotOutcome, BuildSlotPriority, BuildSlotRequest, MutationPolicy};
use crate::execution::jobs::workspace_identity::ManagedWorkspaceContext;
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
            Err(()) => return run_envelope(STORE_BUSY_MESSAGE.to_string(), Vec::new()),
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
                Err(()) => return run_envelope(STORE_BUSY_MESSAGE.to_string(), Vec::new()),
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

    // Resolve every item up front (header + executable spec or a per-item error).
    let mut resolved: Vec<(String, Result<RunSpec, String>)> =
        Vec::with_capacity(payload.commands.len());
    for item in &payload.commands {
        resolved.push(resolve_run_item(orch, request, run_context.as_ref(), item).await);
    }

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
    let sequential = payload.sequential.unwrap_or(false);
    let stop_on_error = payload.stop_on_error.unwrap_or(true);

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
    let mut routed_request = None;
    let mut routed_outcomes = None;
    let mut executed_in_slot = false;
    let slot_target = if let Some(target) = branch_target.as_ref() {
        log::info!(
            "resolved branch run rev {} to commit {} in project {}",
            target.rev,
            target.commit_id,
            target.project_id
        );
        Some((
            target.project_id.clone(),
            target.repository_path.clone(),
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
                context.identity.project_id.clone(),
                context.identity.project_root.clone(),
                base_commit,
                relative_cwd,
                MutationPolicy::AllowDelta,
            ))
        } else {
            None
        }
    } else {
        None
    };
    if has_process {
        if let Some((project_id, project_root, base_commit, relative_cwd, mutation_policy)) =
            slot_target
        {
            let slot_config = crate::config::settings::load_build_slots(&orch.config_dir);
            apply_default_process_timeout(
                &mut resolved,
                slot_config.default_timeout_seconds.min(u32::MAX as u64) as u32,
            );
            let slot_request = BuildSlotRequest {
                request_id: Uuid::new_v4().to_string(),
                attempt_id: Uuid::new_v4().to_string(),
                project_id: project_id.clone(),
                repository: cairn_common::executor_protocol::RepositoryLocator::ColocatedPath {
                    project_id: project_id.clone(),
                    repository_id: project_id,
                    absolute_path: project_root.to_string_lossy().into_owned(),
                },
                base_commit,
                command: format!("run batch ({} items)", resolved.len()),
                cwd: relative_cwd,
                env: Vec::new(),
                priority: BuildSlotPriority::AgentInteractive,
                deadline_unix_ms: crate::build_slots::unix_time_ms()
                    + slot_config
                        .acquisition_deadline_seconds
                        .saturating_mul(1_000),
                timeout_ms: slot_config
                    .default_timeout_seconds
                    .saturating_mul(1_000)
                    .min(u32::MAX as u64) as u32,
                mutation_policy,
                requesting_job_id: run_context.as_ref().map(|ctx| ctx.job_id.clone()),
                affinity_key: run_context.as_ref().map(|ctx| ctx.run_id.clone()),
                constraints: payload.constraints.clone(),
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
            match orch
                .build_slots
                .submit_run_batch(orch, slot_request, batch)
                .await
            {
                BuildSlotOutcome::Unavailable { reason, diagnostic } => {
                    return run_envelope(
                        format!("Run infrastructure failure ({reason:?}): {diagnostic}. The batch was not executed locally."),
                        Vec::new(),
                    );
                }
                BuildSlotOutcome::FailedAfterExecution { diagnostic, .. } => {
                    return run_envelope(
                        format!("Build-slot run executed but could not publish its result: {diagnostic}. The batch was not rerun locally."),
                        Vec::new(),
                    );
                }
                BuildSlotOutcome::Cancelled { .. } => {
                    return run_envelope(
                        "Run cancelled while waiting for or executing in a build slot.".to_string(),
                        Vec::new(),
                    );
                }
                BuildSlotOutcome::Completed {
                    output,
                    mutation_delta,
                    ..
                } => match serde_json::from_str::<Vec<ItemOutcome>>(&output) {
                    Ok(outcomes) => {
                        routed_outcomes = Some(outcomes);
                        routed_delta = mutation_delta;
                        executed_in_slot = true;
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
            let publication = match (
                payload.commit_msg.as_deref(),
                routed_delta.as_ref(),
                routed_request.as_ref(),
                managed_context.as_ref(),
            ) {
                (Some(_), Some(delta), Some(request), Some(context)) => match store_lock.as_deref() {
                    Some(store) => resolve_runner_publication_target(
                        &orch.config_dir,
                        context,
                        store,
                        request,
                    )
                    .and_then(|target| publish_visible_slot_delta(orch, &target, request, delta)),
                    None => Err(
                        "build-slot publication has no resolved shared-store lock path".to_string(),
                    ),
                },
                _ => Ok(()),
            };
            match publication {
                Ok(()) => run_commit_barrier(
                    vcs.as_ref().expect("ambient run always resolves a VCS").as_ref(),
                    worktree_path,
                    payload.commit_msg.as_deref(),
                    all_ok,
                    status_before.as_ref(),
                    author.as_ref(),
                ),
                Err(error) => CommitBarrierOutcome {
                    message: format!("⚠️ {error}. The routed batch was not rerun locally."),
                    worktree_changed: false,
                    committed: false,
                    committed_patch: None,
                },
            }
        }
        Err(()) => CommitBarrierOutcome {
            message: "⚠️ The project's version-control store stayed busy behind a long-running operation. Nothing was committed and the working copy was PRESERVED exactly. Retry with a trivial `run` carrying the same `commit_msg`; the commit barrier will seal any remaining dirty worktree.".to_string(),
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

    if executed_in_slot {
        if !result.is_empty() {
            result.push_str("\n\n");
        }
        result.push_str("Executed in an isolated build slot; ignored and untracked outputs remain in that slot and are not available in this workspace.");
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
