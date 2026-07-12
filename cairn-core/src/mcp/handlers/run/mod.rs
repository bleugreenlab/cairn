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
    apply_non_interactive_pager_env_to_pty, build_agent_spawn_config,
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
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex as TokioMutex, MutexGuard};

const STORE_LOCK_TIMEOUT: Duration = Duration::from_secs(600);
const STORE_BUSY_MESSAGE: &str =
    "The project's version-control store is busy behind a long-running operation; retry this run.";

async fn acquire_store_lock<'a>(
    lock: Option<&'a Arc<TokioMutex<()>>>,
    timeout: Duration,
) -> Result<Option<MutexGuard<'a, ()>>, ()> {
    match lock {
        Some(lock) => tokio::time::timeout(timeout, lock.lock())
            .await
            .map(Some)
            .map_err(|_| ()),
        None => Ok(None),
    }
}

fn materialize_slot_delta(
    orch: &Orchestrator,
    worktree: &std::path::Path,
    delta: &crate::build_slots::MutationDelta,
) -> Result<(), String> {
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
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
use hygiene::{
    check_cd_commands, checkout_has_tracked_changes, verify_branch_checkout_clean_after_run,
};
use output::{collect_run_images, compose_run_output, run_envelope};
use process::run_one;
use resolve::resolve_run_item;
use types::{ItemOutcome, RunSpec};

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
    pub commit_present: bool,
}

/// Execute a routed batch in one leased working directory while preserving the
/// public scheduling contract: sequential payloads run in order and all other
/// payloads run concurrently. Outcomes remain ordered for stable presentation.
pub(crate) async fn execute_resolved_slot_batch(
    orch: &Orchestrator,
    cwd: &str,
    batch: ResolvedRunBatch,
) -> Vec<ItemOutcome> {
    if batch.originally_sequential {
        let mut outcomes = Vec::with_capacity(batch.resolved.len());
        for (index, (header, spec)) in batch.resolved.into_iter().enumerate() {
            let outcome = run_one(
                orch,
                &batch.request,
                cwd,
                &run_item_stream_id(&batch.tool_use_id, index),
                batch.run_context.as_ref(),
                batch.commit_present,
                false,
                false,
                header,
                spec,
            )
            .await;
            let stop = outcome.suspended || (!outcome.succeeded && batch.stop_on_error);
            outcomes.push(outcome);
            if stop {
                break;
            }
        }
        return outcomes;
    }

    let mut handles = Vec::with_capacity(batch.resolved.len());
    for (index, (header, spec)) in batch.resolved.into_iter().enumerate() {
        let orch = orch.clone();
        let cwd = cwd.to_string();
        let stream_id = run_item_stream_id(&batch.tool_use_id, index);
        let run_context = batch.run_context.clone();
        let request = batch.request.clone();
        let commit_present = batch.commit_present;
        handles.push(AbortOnDrop(tokio::spawn(async move {
            run_one(
                &orch,
                &request,
                &cwd,
                &stream_id,
                run_context.as_ref(),
                commit_present,
                false,
                false,
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
            Err(error) => outcomes.push(ItemOutcome::failed(
                "<item>".to_string(),
                format!("Failed to join run task: {error}"),
            )),
        }
    }
    outcomes
}

use crate::build_slots::{BuildSlotOutcome, BuildSlotPriority, BuildSlotRequest, MutationPolicy};
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

    let mut cwd = request.cwd.clone();
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

    let branch_checkout = if let Some(branch) = payload.branch.as_deref() {
        if commit_present {
            return run_envelope(
                "A branch-scoped run cannot commit; it runs against another checkout and refuses dirty tracked state. Remove commit_msg and retry."
                    .to_string(),
                Vec::new(),
            );
        }
        let resolution =
            match crate::mcp::handlers::branch::resolve_for_run(orch, request, branch).await {
                Ok(resolution) => resolution,
                Err(error) => return run_envelope(error, Vec::new()),
            };
        let checkout = resolution
            .checkout
            .expect("branch run resolution always carries checkout");
        match checkout_has_tracked_changes(orch, &checkout) {
            Ok(false) => {}
            Ok(true) => {
                return run_envelope(
                    format!(
                        "Branch-scoped run refused: target checkout has uncommitted tracked changes: {}",
                        checkout.display()
                    ),
                    Vec::new(),
                )
            }
            Err(error) => return run_envelope(error, Vec::new()),
        }
        cwd = checkout.to_string_lossy().into_owned();
        Some(checkout)
    } else {
        None
    };
    let branch_scoped_run = branch_checkout.is_some();

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
    let store_lock = crate::mcp::vcs::resolve_store_lock(orch, request).await;
    let managed_context = {
        let _guard = match acquire_store_lock(store_lock.as_ref(), STORE_LOCK_TIMEOUT).await {
            Ok(guard) => guard,
            Err(()) => return run_envelope(STORE_BUSY_MESSAGE.to_string(), Vec::new()),
        };
        match crate::mcp::vcs::prepare_managed_workspace(orch, request).await {
            Ok(context) => context,
            Err(error) => return run_envelope(error, Vec::new()),
        }
    };
    let bookmark_observation =
        crate::mcp::vcs::observe_managed_bookmark(orch, managed_context.as_ref());
    let vcs = crate::mcp::vcs::resolve_managed_worktree_vcs(
        orch,
        std::path::Path::new(&cwd),
        managed_context.as_ref(),
    );
    {
        let _guard = match acquire_store_lock(store_lock.as_ref(), STORE_LOCK_TIMEOUT).await {
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
    // Capture the pre-batch snapshot whenever a no-`commit_msg` run could leave
    // dirt the barrier must reconcile. For a worktree this is gated on the
    // hygiene fence (Ask/Deny). For the project's LIVE checkout we capture
    // regardless of fence; a request without an execution snapshot is
    // unconfined, yet a stray write there still violates the worktree boundary
    // and must be flagged (read-only detection; never reverted). The commit_msg
    // case on a non-worktree cwd already returned early above.
    let non_worktree_cwd = !crate::jj::is_jj_dir(std::path::Path::new(&cwd));
    let status_before = if payload.commit_msg.is_none() && (run_hygiene_applies || non_worktree_cwd)
    {
        vcs.snapshot(std::path::Path::new(&cwd)).ok()
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

    let mut routed_delta = None;
    let mut routed_outcomes = None;
    let mut executed_in_slot = false;
    if has_process && !branch_scoped_run {
        if let Some(context) = managed_context.as_ref() {
            if checkout_has_tracked_changes(orch, std::path::Path::new(&cwd)).unwrap_or(true) {
                return run_envelope(
                    "A managed process batch requires a clean workspace before build-slot placement.".to_string(),
                    Vec::new(),
                );
            }
            let slot_config = crate::config::settings::load_build_slots(&orch.config_dir);
            apply_default_process_timeout(
                &mut resolved,
                slot_config.default_timeout_seconds.min(u32::MAX as u64) as u32,
            );
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
            let slot_request = BuildSlotRequest {
                request_id: Uuid::new_v4().to_string(),
                attempt_id: Uuid::new_v4().to_string(),
                project_id: context.identity.project_id.clone(),
                repository: context.identity.project_root.to_string_lossy().into_owned(),
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
                mutation_policy: MutationPolicy::AllowDelta,
                requesting_job_id: run_context.as_ref().map(|ctx| ctx.job_id.clone()),
                affinity_key: run_context.as_ref().map(|ctx| ctx.run_id.clone()),
            };
            let batch = ResolvedRunBatch {
                request: request.clone(),
                run_context: run_context.clone(),
                resolved: resolved.clone(),
                tool_use_id: tool_use_id.clone(),
                stop_on_error,
                originally_sequential: sequential,
                commit_present,
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
                commit_present,
                branch_scoped_run,
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
                    commit_present,
                    branch_scoped_run,
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

    let branch_restore_message = if let Some(checkout) = branch_checkout.as_ref() {
        match verify_branch_checkout_clean_after_run(orch, checkout) {
            Ok(true) => Some(format!(
                "✓ Verified branch checkout {} has no tracked changes after run",
                checkout.display()
            )),
            Ok(false) => None,
            Err(error) => Some(format!(
                "⚠️ Branch checkout {} has tracked changes after run: {}",
                checkout.display(),
                error
            )),
        }
    } else {
        None
    };

    // If any item durably suspended on a worktree-fence approval, return the
    // suspend marker for the whole call; the run re-drives the batch on resume.
    if outcomes.iter().any(|o| o.suspended) {
        let mut text = "Run suspended pending worktree fence approval; resume will continue once it is answered."
            .to_string();
        if let Some(message) = branch_restore_message {
            text.push_str("\n\n");
            text.push_str(&message);
        }
        return run_envelope(text, Vec::new());
    }

    let mut result = compose_run_output(&outcomes);

    if branch_scoped_run {
        if let Some(message) = branch_restore_message {
            if !result.is_empty() {
                result.push_str("\n\n");
            }
            result.push_str(&message);
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
    let barrier = match acquire_store_lock(store_lock.as_ref(), STORE_LOCK_TIMEOUT).await {
        Ok(_store_guard) => {
            let publication = match (payload.commit_msg.as_deref(), routed_delta.as_ref()) {
                (Some(_), Some(delta)) => materialize_slot_delta(orch, worktree_path, delta),
                _ => Ok(()),
            };
            match publication {
                Ok(()) => run_commit_barrier(
                    vcs.as_ref(),
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

#[cfg(test)]
mod store_lock_timeout_tests {
    use super::*;

    #[tokio::test]
    async fn acquisition_returns_timeout_instead_of_waiting_forever() {
        let lock = Arc::new(TokioMutex::new(()));
        let _held = lock.lock().await;
        let result = acquire_store_lock(Some(&lock), Duration::from_millis(10)).await;
        assert!(result.is_err());
        assert!(STORE_BUSY_MESSAGE.contains("retry"));
    }
}
