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

pub(crate) use checks::{
    check_stream_id, run_check_command, run_check_command_unsandboxed, run_item_stream_id,
    CheckExecResult,
};
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

use commit_barrier::run_commit_barrier;
use hygiene::{
    check_cd_commands, checkout_has_tracked_changes, verify_branch_checkout_clean_after_run,
};
use output::{collect_run_images, compose_run_output, run_envelope};
use process::run_one;
use resolve::resolve_run_item;
use types::{ItemOutcome, RunSpec};

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
    let vcs = crate::mcp::vcs::resolve_worktree_vcs(orch, std::path::Path::new(&cwd));
    // Pre-flight staleness reconcile: heal a stale / behind-its-branch-tip working
    // copy BEFORE the batch runs, serialized on the same per-store jj lock the
    // base-advance reconcile and merge-fold hold, so it can never race a concurrent
    // rebase (the hazard a hand-run `jj workspace update-stale` hit). Resolved once
    // here and reused by the post-batch commit barrier below. Best-effort: a
    // failure leaves the seal-time stale arm as the mid-batch fallback.
    let store_lock = crate::mcp::vcs::resolve_store_lock(orch, request).await;
    {
        let _guard = match store_lock.as_ref() {
            Some(lock) => Some(lock.lock().await),
            None => None,
        };
        if let Err(e) = vcs.reconcile_workspace(std::path::Path::new(&cwd)) {
            log::warn!(
                "pre-flight workspace reconcile failed (continuing; seal-time stale arm remains the fallback): {e}"
            );
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
                commit_present,
                branch_scoped_run,
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
    let barrier = {
        let _store_guard = match store_lock.as_ref() {
            Some(lock) => Some(lock.lock().await),
            None => None,
        };
        run_commit_barrier(
            vcs.as_ref(),
            worktree_path,
            payload.commit_msg.as_deref(),
            all_ok,
            status_before.as_ref(),
            author.as_ref(),
        )
    };
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
