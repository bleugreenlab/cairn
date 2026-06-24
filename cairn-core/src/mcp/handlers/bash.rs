//! Bash command MCP handler.
//!
//! Routes synchronous shell commands through inline execution and exposes
//! terminal resource helpers for long-running PTY sessions.

use super::{normalize_command, unwrap_shell_launcher};
use crate::services::{
    ensure_submitted_line, get_default_shell, sandbox, submit_command_exiting_shell, PtySession,
    SpawnConfig, TerminalChild,
};
use crate::storage::{DbResult, LocalDb, RowExt};
use portable_pty::{CommandBuilder, PtySize};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::{Duration, SystemTime};
use turso::params;
use uuid::Uuid;

use crate::config::mcp_servers::McpServerConfig;
use crate::mcp::git::GitAuthor;
use crate::mcp::types::McpCallbackRequest;
use crate::models::Fence;
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;

/// PTY data event payload (mirrors Tauri-side PtyDataPayload)
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PtyDataPayload {
    pub session_id: String,
    pub data: String,
}

/// PTY exit event payload (mirrors Tauri-side PtyExitPayload)
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PtyExitPayload {
    pub session_id: String,
    pub exit_code: Option<i32>,
}

/// Payload for run tool from MCP server.
///
/// `run` is an ordered batch of invocations. Each item is either a shell
/// command (`command`) or a `cairn://` skill-script target (`target`).
/// Items run in parallel by default; `sequential` opts into ordered execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunPayload {
    pub commands: Vec<RunItem>,
    /// Run items in input order instead of concurrently. Default false.
    #[serde(default)]
    pub sequential: Option<bool>,
    /// In sequential mode, abort remaining items after a failure. Default true.
    #[serde(default)]
    pub stop_on_error: Option<bool>,
    /// Commit all worktree changes once after the whole batch succeeds. Required
    /// when a successful worktree-bound run dirties the worktree; `^` amends.
    /// Without it, a run that dirties the worktree is restored to HEAD (see
    /// `run_commit_barrier`).
    #[serde(default)]
    pub commit_msg: Option<String>,
}

/// A single run invocation: exactly one of `command` (shell) or `target`
/// (a `cairn://skills/<id>/scripts/<name>` URI).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunItem {
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub timeout: Option<u32>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub payload: Option<RunItemPayload>,
}

/// Structured args for a run item's target.
///
/// `args` carries positional arguments for a skill-script target. `args_json`
/// carries the named-argument object for an MCP tool call
/// (`cairn://mcp/<server>/<tool>`), passed through to the server's `tools/call`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunItemPayload {
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub args_json: Option<serde_json::Value>,
}

/// A resolved, ready-to-execute run item.
enum RunSpec {
    /// A shell command run through `bash -c` (or `cmd /c` on Windows).
    Shell {
        command: String,
        timeout: Option<u32>,
    },
    /// A resolved skill script: interpreter program + full argv (script path
    /// included), executed directly (not through a shell).
    Script {
        program: String,
        args: Vec<String>,
        timeout: Option<u32>,
    },
    /// A proxied external MCP `tools/call` (`cairn://mcp/<server>/<tool>`).
    /// Not a process exec — dispatched through the host `McpGateway`. Boxed
    /// because its config payload is far larger than the other variants.
    McpCall(Box<McpCallSpec>),
}

/// Resolved payload for a proxied MCP `tools/call`.
struct McpCallSpec {
    credential_key: String,
    tool: String,
    args: serde_json::Value,
    config: McpServerConfig,
    timeout: Option<u32>,
}

/// Per-item execution result used to compose the batch output.
struct ItemOutcome {
    /// Header shown above this item's output (`=== <header> ===`): the command
    /// or the skill-script target URI.
    header: String,
    body: String,
    succeeded: bool,
    /// The item durably suspended on a worktree-fence approval; the whole batch
    /// re-runs on resume. Carried so `handle_run` can surface the suspend marker.
    suspended: bool,
}

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

/// Raw process result before body formatting.
struct ExecOutput {
    stdout: String,
    stderr: String,
    exit_code: Option<i32>,
    timed_out: bool,
    /// A kernel sandbox denial detected after execution (drives the fence).
    denial: Option<sandbox::SandboxDenial>,
    /// Set when a timed-out item with a run context was detached to a terminal
    /// instead of killed. Carries the partial output (in `stdout`/`stderr`) plus
    /// where the still-running process can be reached.
    promoted: Option<PromotedTerminal>,
}

/// A timed-out `run` item that was promoted to a durable terminal session.
struct PromotedTerminal {
    /// The terminal's auto-allocated slug (`run-<n>`).
    #[allow(dead_code)]
    slug: String,
    /// Canonical terminal URI the agent can read and kill.
    uri: String,
}

/// Max chars in a composed batch result. Deliberately equal to the CLI's
/// `MAX_RESULT_CHARS`: `compose_run_output` keeps its result at or under this,
/// so the CLI's run-result guard passes a core-capped batch through untouched
/// (no double truncation, no second elision marker).
const MAX_RUN_RESULT_CHARS: usize = 45_000;

/// Event payload for real-time bash output streaming
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BashOutputPayload {
    pub run_id: String,
    pub tool_use_id: String,
    pub chunk: String,
    pub stream: String, // "stdout" or "stderr"
}

/// Event payload for bash command completion
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BashCompletePayload {
    pub run_id: String,
    pub tool_use_id: String,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
}

/// Event payload when agent spawns a background terminal
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentTerminalCreatedPayload {
    pub run_id: String,
    pub session_id: String,
    pub command: String,
    pub description: Option<String>,
}

/// Maximum output buffer size (64KB)
const MAX_BUFFER_SIZE: usize = 64 * 1024;

/// Default pager used for agent-launched commands.
///
/// Inline `run` commands are non-interactive: stdout/stderr are piped and there
/// is no terminal for a pager such as `less` to read from. Git falls back to the
/// configured pager even for common inspection commands (`git diff`, `git log`),
/// so force pager-aware tools to stream to stdout unless the command explicitly
/// overrides the env itself (for example `PAGER=less git log`).
const NON_INTERACTIVE_PAGER: &str = "cat";

fn apply_non_interactive_pager_env(mut config: SpawnConfig) -> SpawnConfig {
    config = config.env("GIT_PAGER", NON_INTERACTIVE_PAGER);
    config.env("PAGER", NON_INTERACTIVE_PAGER)
}

fn apply_non_interactive_pager_env_to_pty(cmd: &mut CommandBuilder) {
    cmd.env("GIT_PAGER", NON_INTERACTIVE_PAGER);
    cmd.env("PAGER", NON_INTERACTIVE_PAGER);
}

/// Redact common secret patterns from a command string for safe logging.
///
/// Replaces values matching bearer tokens, API keys, export statements with
/// secret-like variable names, and password flags with `[REDACTED]`.
///
/// This is intentionally conservative — it only redacts well-known patterns
/// to avoid false positives that would make logs unreadable.
pub fn redact_command(command: &str) -> String {
    use regex::Regex;
    use std::sync::LazyLock;

    static PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
        vec![
            // Bearer tokens: "Bearer sk-abc123..." or "Bearer eyJ..."
            // [^\s'"]+ avoids eating surrounding quotes
            Regex::new(r#"(?i)(Bearer\s+)[^\s'"]+"#).unwrap(),
            // API key patterns: sk-..., sk_live_..., etc.
            Regex::new(r"\bsk[-_][a-zA-Z0-9._-]{8,}\b").unwrap(),
            // export KEY=value, export SECRET=value, export TOKEN=value, export PASSWORD=value
            Regex::new(r"(?i)(export\s+[A-Z_]*(?:KEY|SECRET|TOKEN|PASSWORD)\s*=)\S+").unwrap(),
            // --password=value or --password value
            Regex::new(r"(?i)(--password[= ])\S+").unwrap(),
        ]
    });

    let mut result = command.to_string();
    for pattern in PATTERNS.iter() {
        result = pattern
            .replace_all(&result, |caps: &regex::Captures| {
                // Preserve the prefix (e.g., "Bearer ", "export API_KEY=", "--password=")
                if let Some(prefix) = caps.get(1) {
                    format!("{}[REDACTED]", prefix.as_str())
                } else {
                    "[REDACTED]".to_string()
                }
            })
            .to_string();
    }
    result
}

/// Handle run tool call - an ordered batch of synchronous shell commands and
/// skill-script invocations. Parallel by default; `sequential` runs in order.
pub async fn handle_run(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: RunPayload = match super::parse_payload(request) {
        Ok(payload) => payload,
        Err(error) => return error,
    };

    if payload.commands.is_empty() {
        return "Invalid payload: `commands` must contain at least one item".to_string();
    }

    let cwd = request.cwd.clone();
    let tool_use_id = request
        .tool_use_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let commit_present = payload.commit_msg.is_some();

    // Changes can only happen in a worktree. A non-jj cwd is the project's live
    // checkout behind a long-lived manager / triage / read-only agent (project
    // chat included). A `commit_msg` means the caller intends to commit, which
    // requires a worktree; reject the whole batch BEFORE running any command, so
    // nothing executes against — or is left in — the user's live checkout. (The
    // commit barrier itself cannot help here: NonWorktreeVcs is a read-only
    // no-op that must never seal or revert the user's checkout.)
    if commit_present && !crate::jj::is_jj_dir(std::path::Path::new(&cwd)) {
        return "Commits require a worktree. This agent runs on the project's live checkout \
                (no worktree), so a run carrying commit_msg cannot commit and no commands were \
                executed. Changes can only be made in a worktree."
            .to_string();
    }

    // Look up streaming/run context once for the whole batch. Prefer the
    // callback's run_id when present: cwd lookups are only a fallback and can
    // miss or pick the wrong run when multiple runs share a repo/worktree path.
    // The live bash preview subscribes by this run id, so using the exact
    // request run is what wires emitted `bash-output` chunks to the visible tool.
    let run_context = super::run_context::lookup_run(&orch.db.local, request)
        .await
        .ok();
    let run_hygiene_applies = matches!(
        super::fence::resolve_run_fence(orch, request).await,
        Some((_run_id, Fence::Ask | Fence::Deny))
    );
    // Resolve the worktree's VCS backend once (jj for a worktree; the read-only
    // NonWorktreeVcs for the project's live checkout) and capture the pre-batch
    // snapshot through it.
    let vcs = crate::mcp::vcs::resolve_worktree_vcs(orch, std::path::Path::new(&cwd));
    // Capture the pre-batch snapshot whenever a no-`commit_msg` run could leave
    // dirt the barrier must reconcile. For a worktree this is gated on the
    // hygiene fence (Ask/Deny). For the project's LIVE checkout we capture
    // regardless of fence — project chat carries no execution snapshot and is
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
                Err(e) => outcomes.push(ItemOutcome {
                    header: "<item>".to_string(),
                    body: format!("Failed to join run task: {e}"),
                    succeeded: false,
                    suspended: false,
                }),
            }
        }
        outcomes
    };

    // If any item durably suspended on a worktree-fence approval, return the
    // suspend marker for the whole call; the run re-drives the batch on resume.
    if outcomes.iter().any(|o| o.suspended) {
        return "Run suspended pending worktree fence approval; resume will \
                continue once it is answered."
            .to_string();
    }

    let mut result = compose_run_output(&outcomes);

    // Top-level commit barrier / hygiene gate. The session-archival scheme
    // requires the worktree to exactly equal HEAD after every run; the barrier
    // either commits the worktree or restores it to HEAD. Author identity and
    // event emission stay here (they need the orchestrator); the git decision
    // lives in `run_commit_barrier` so it can be tested without one.
    let all_ok = outcomes.iter().all(|o| o.succeeded);
    let worktree_path = std::path::Path::new(&cwd);
    let author = match payload.commit_msg.as_deref() {
        Some(_) => run_context
            .and_then(|ctx| orch.resolve_git_identity_for_project(Some(&ctx.project_id)))
            .map(|(name, email)| GitAuthor::new(name, email)),
        None => None,
    };
    let barrier = run_commit_barrier(
        vcs.as_ref(),
        worktree_path,
        payload.commit_msg.as_deref(),
        all_ok,
        status_before.as_ref(),
        author.as_ref(),
    );
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

    if result.is_empty() {
        "(no output)".to_string()
    } else {
        result
    }
}

/// Outcome of the post-batch commit barrier.
pub(super) struct CommitBarrierOutcome {
    /// Text to append to the run output (may be empty).
    pub message: String,
    /// Whether the worktree was mutated (committed or restored) so the caller
    /// should emit a `worktree-changed` event.
    pub worktree_changed: bool,
    /// Whether a real commit (or amend) landed. True only when the seal
    /// succeeded — not on a restore, a clean no-op, or a missing commit_msg.
    /// Part of the barrier's result contract and asserted by the commit-hygiene
    /// tests; no production reader consumes it today.
    #[allow(dead_code)]
    pub committed: bool,
}

/// Enforce the worktree==HEAD invariant after a `run` batch.
///
/// The single decision point for what happens to the worktree once a batch
/// finishes: commit it, restore it to HEAD, or leave it alone. It touches git
/// only inside `worktree_path` and returns the user-facing message plus whether
/// the worktree changed, so it is testable without an `Orchestrator`.
///
/// - `Some(msg)`: commit the worktree if dirty, even on partial item failure;
///   a commit failure restores the worktree to HEAD.
/// - `None`: when the batch fully succeeded and changed the worktree, restore
///   it to HEAD — no commit_msg means the new dirt must not persist.
pub(super) fn run_commit_barrier(
    vcs: &dyn crate::mcp::vcs::WorktreeVcs,
    worktree_path: &std::path::Path,
    commit_msg: Option<&str>,
    all_ok: bool,
    before: Option<&crate::mcp::vcs::VcsSnapshot>,
    author: Option<&GitAuthor>,
) -> CommitBarrierOutcome {
    let mut message = String::new();
    let mut worktree_changed = false;
    let mut committed = false;

    match commit_msg {
        Some(commit_msg) => {
            // Commit the worktree even when some items failed: a partial-success
            // batch must not silently leave the successful items' dirt behind.
            if !matches!(vcs.is_dirty(worktree_path), Ok(false)) {
                match vcs.seal_all(worktree_path, commit_msg, author) {
                    Ok(commit_result) => {
                        worktree_changed = true;
                        committed = true;
                        let pr_suffix = commit_result
                            .pr_number
                            .map(|pr| format!(" updated PR#{}", pr))
                            .unwrap_or_default();
                        message.push_str(&format!(
                            "\u{2713} Committed changes ({}){}",
                            commit_result.sha, pr_suffix
                        ));
                    }
                    Err(e) if e.contains("nothing to commit") => {
                        // Tree was (or became) clean; already equals HEAD.
                        log::info!("run commit_msg given but nothing to commit: {}", e);
                    }
                    Err(e) => {
                        let restore = vcs.discard(worktree_path);
                        worktree_changed = true;
                        match restore {
                            Ok(()) => message.push_str(&format!(
                                "\u{26a0}\u{fe0f} Failed to commit: {}; the worktree was restored to HEAD.",
                                e
                            )),
                            Err(re) => message.push_str(&format!(
                                "\u{26a0}\u{fe0f} Failed to commit: {}; additionally failed to restore the worktree to HEAD: {}",
                                e, re
                            )),
                        }
                    }
                }
            }
        }
        None => {
            // No commit_msg: the run must not leave new dirt. When the whole
            // batch succeeded and changed the worktree, restore to HEAD. Gate on
            // `all_ok` — a failed batch's own error is the headline, and the
            // hygiene gate must not mask it.
            if all_ok {
                if let Some(before) = before {
                    if matches!(vcs.changed_since(worktree_path, before), Ok(true)) {
                        // A non-worktree backend (the project's live checkout)
                        // cannot revert: rolling it back would destroy the user's
                        // own uncommitted work. Changes can only happen in a
                        // worktree, so the stray dirt is left in place and the
                        // agent is warned loudly instead of (falsely) told it was
                        // reverted. See `docs/worktree-fence.md`.
                        if !vcs.can_revert() {
                            message.push_str(
                                "\u{26a0}\u{fe0f} This run wrote into the project's live checkout, but changes can only be made in a worktree. The changes were left in place — the live checkout is not reverted, because that could destroy your own uncommitted work — so review and clean them up (`git status` / `git restore`). To make changes that persist, run inside a worktree.",
                            );
                            return CommitBarrierOutcome {
                                message,
                                worktree_changed,
                                committed,
                            };
                        }
                        let reset_ok = vcs.discard(worktree_path);
                        worktree_changed = true;
                        if let Err(e) = reset_ok {
                            message.push_str(&format!(
                                "\u{26a0}\u{fe0f} Run changed the worktree but no commit_msg was given. Failed to restore the worktree to HEAD: {}. Run with commit_msg like `run({{commands:[…], commit_msg)`, then retry.",
                                e
                            ));
                        } else {
                            message.push_str(
                                "\u{26a0}\u{fe0f} Run reverted: it changed the worktree but no commit_msg was given. Run with commit_msg like `run({commands:[…], commit_msg)`, then retry.",
                            );
                        }
                    }
                }
            }
        }
    }

    CommitBarrierOutcome {
        message,
        worktree_changed,
        committed,
    }
}

/// Resolve a single run item into a header + executable spec, or a per-item
/// error message that will be reported inline (and counts as a failure).
async fn resolve_run_item(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    run_context: Option<&super::RunContext>,
    item: &RunItem,
) -> (String, Result<RunSpec, String>) {
    match (item.command.as_deref(), item.target.as_deref()) {
        (Some(_), Some(_)) => (
            "<invalid item>".to_string(),
            Err("Run item has both `command` and `target`; provide exactly one".to_string()),
        ),
        (None, None) => (
            "<invalid item>".to_string(),
            Err("Run item has neither `command` nor `target`; provide exactly one".to_string()),
        ),
        (Some(command), None) => {
            // Unwrap launcher forms (e.g. `bash -lc '...'`) like the old path did.
            let unwrapped = unwrap_shell_launcher(command);
            let command = if command.trim() != unwrapped {
                unwrapped
            } else {
                command.to_string()
            };
            let header = command.clone();
            (
                header,
                Ok(RunSpec::Shell {
                    command,
                    timeout: item.timeout,
                }),
            )
        }
        (None, Some(target)) => {
            let header = target.to_string();
            // Branch on the target URI family: MCP gateway tool calls are
            // proxied RPC; everything else is a skill-script process exec.
            match cairn_common::uri::parse_uri(target) {
                Some(CairnResource::Mcp { server, resource }) => {
                    let spec = resolve_mcp_call(orch, run_context, server, resource, item).await;
                    (header, spec)
                }
                _ => {
                    let spec = resolve_script_spec(orch, request, target, item).await;
                    (header, spec)
                }
            }
        }
    }
}

/// Resolve a `cairn://mcp/<server>/<tool>` target into an `McpCall` spec.
async fn resolve_mcp_call(
    orch: &Orchestrator,
    run_context: Option<&super::RunContext>,
    server: Option<String>,
    resource: Option<String>,
    item: &RunItem,
) -> Result<RunSpec, String> {
    let server = server.ok_or_else(|| {
        "MCP run target must name a server and tool: cairn://mcp/<server>/<tool>".to_string()
    })?;
    let tool = resource
        .ok_or_else(|| format!("MCP run target must name a tool: cairn://mcp/{server}/<tool>"))?;

    let (config, credential_key) = resolve_mcp_server_config(orch, run_context, &server).await?;

    // Named-argument object passed through to the server's tools/call. Default
    // to an empty object; reject non-object shapes with a clear message rather
    // than duplicating the server's own inputSchema validation.
    let args = match item.payload.as_ref().and_then(|p| p.args_json.clone()) {
        None | Some(serde_json::Value::Null) => serde_json::json!({}),
        Some(v @ serde_json::Value::Object(_)) => v,
        Some(_) => {
            return Err(format!(
                "MCP tool args (payload.args_json) for '{server}/{tool}' must be a JSON object of named arguments"
            ))
        }
    };

    Ok(RunSpec::McpCall(Box::new(McpCallSpec {
        credential_key,
        tool,
        args,
        config,
        timeout: item.timeout,
    })))
}

/// Resolve the env-expanded config for a named MCP server from workspace +
/// project settings (project overlays workspace).
async fn resolve_mcp_server_config(
    orch: &Orchestrator,
    run_context: Option<&super::RunContext>,
    server: &str,
) -> Result<(McpServerConfig, String), String> {
    // Resolve the run's project path for the project-level overlay; fall back to
    // workspace-only servers when there is no active run context.
    let project_path = match run_context {
        Some(ctx) => crate::config::get_project_path(&orch.db.local, &ctx.project_id)
            .await
            .ok(),
        None => None,
    };

    let project_servers = project_path
        .as_deref()
        .map(crate::config::mcp_servers::load_project_mcp_servers)
        .unwrap_or_default();
    let servers =
        crate::config::mcp_servers::resolve_mcp_servers(&orch.config_dir, project_path.as_deref());

    match servers.get(server) {
        Some(cfg) => {
            let project_scope = project_path
                .as_deref()
                .filter(|_| project_servers.contains_key(server));
            let credential_key = crate::config::secrets::credential_key(server, project_scope);
            Ok((cfg.expanded(&credential_key), credential_key))
        }
        None => {
            let mut names: Vec<&str> = servers.keys().map(|s| s.as_str()).collect();
            names.sort_unstable();
            let configured = if names.is_empty() {
                "(none configured)".to_string()
            } else {
                names.join(", ")
            };
            Err(format!(
                "Unknown MCP server '{server}'. Configured servers: {configured}. \
                 Add it under `mcpServers` in ~/.cairn/settings.yaml or the project's .cairn/config.yaml."
            ))
        }
    }
}

/// Resolve a `cairn://skills/<id>/scripts/<name>` target to a Script spec.
async fn resolve_script_spec(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    target: &str,
    item: &RunItem,
) -> Result<RunSpec, String> {
    let resource = cairn_common::uri::parse_uri(target)
        .ok_or_else(|| format!("Invalid run target URI: {target}"))?;
    let script_path =
        super::skills_resources::resolve_skill_script_path(orch, request, &resource).await?;
    let (program, mut args) = resolve_interpreter(&script_path)?;
    args.push(script_path.to_string_lossy().to_string());
    if let Some(payload) = &item.payload {
        args.extend(payload.args.iter().cloned());
    }
    Ok(RunSpec::Script {
        program,
        args,
        timeout: item.timeout,
    })
}

/// Determine the interpreter for a script: shebang first, then extension.
/// Returns (program, prefix_args) where the script path is appended by caller.
fn resolve_interpreter(script_path: &std::path::Path) -> Result<(String, Vec<String>), String> {
    if let Ok(content) = std::fs::read_to_string(script_path) {
        if let Some(first) = content.lines().next() {
            if let Some(rest) = first.strip_prefix("#!") {
                let toks: Vec<&str> = rest.split_whitespace().collect();
                if let Some((&head, tail)) = toks.split_first() {
                    // `/usr/bin/env python3` -> program is the next token.
                    if head.ends_with("env") {
                        if let Some((&prog, env_tail)) = tail.split_first() {
                            return Ok((
                                prog.to_string(),
                                env_tail.iter().map(|s| s.to_string()).collect(),
                            ));
                        }
                    } else {
                        return Ok((
                            head.to_string(),
                            tail.iter().map(|s| s.to_string()).collect(),
                        ));
                    }
                }
            }
        }
    }

    let ext = script_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    let program = match ext {
        "sh" => "bash",
        "py" => "python3",
        "js" => "node",
        "ts" => "bun",
        "rb" => "ruby",
        _ => {
            return Err(format!(
            "Cannot determine interpreter for script '{}': no shebang and unrecognized extension",
            script_path.display()
        ))
        }
    };
    Ok((program.to_string(), Vec::new()))
}

fn run_item_stream_id(tool_use_id: &str, index: usize) -> String {
    format!("{tool_use_id}:{index}")
}

/// Percent of an item's char budget reserved for the head (leading context: the
/// command echo and opening lines). The remainder holds the tail. Command output
/// is read end-first — error summaries, test failures, the last `&&` segment, and
/// exit codes all live at the end — so the per-item cap is tail-biased.
const ITEM_HEAD_PERCENT: usize = 15;

/// Bytes reserved inside a per-item budget for the elision marker, so a capped
/// body (head + marker + tail) never exceeds its budget. This is what keeps the
/// composed batch within `MAX_RUN_RESULT_CHARS`, so the CLI passes it untouched.
const ELISION_MARKER_RESERVE: usize = 96;

/// Smallest per-item body budget worth showing. Below this an item is omitted
/// whole (the last-resort path) rather than reduced to a useless sliver. Sized
/// well above any trailing detached-terminal note, so the tail cap of a
/// non-omitted item never elides that note.
const MIN_ITEM_BODY_BUDGET: usize = 512;

/// Headroom held back from the batch cap for the trailing omission note and
/// inter-segment separators, so the composed result stays under the cap.
const OMISSION_NOTE_RESERVE: usize = 256;

/// Largest byte index `<= idx` that is a UTF-8 char boundary.
fn floor_char_boundary(s: &str, mut idx: usize) -> usize {
    if idx >= s.len() {
        return s.len();
    }
    while idx > 0 && !s.is_char_boundary(idx) {
        idx -= 1;
    }
    idx
}

/// Smallest byte index `>= idx` that is a UTF-8 char boundary.
fn ceil_char_boundary(s: &str, mut idx: usize) -> usize {
    while idx < s.len() && !s.is_char_boundary(idx) {
        idx += 1;
    }
    idx
}

/// Tail-biased head+tail cap for one command body. Keeps a small head (~15%, the
/// command's opening context) and a large tail (~85%, the signal), with an
/// elision marker between stating how many lines and chars were dropped. The tail
/// extends to the byte-exact end of the body, so a trailing note — e.g. the
/// detached-terminal pointer a promoted item appends — is always retained as long
/// as the item clears `MIN_ITEM_BODY_BUDGET`. Its start is snapped forward to the
/// next line boundary so the displayed tail begins on a whole line rather than a
/// mid-line fragment (which reads as corrupted output); the skipped partial line
/// folds into the elision count. The result never exceeds `budget`.
fn cap_item_body_tail_biased(body: &str, budget: usize) -> String {
    if body.len() <= budget {
        return body.to_string();
    }
    let content_budget = budget.saturating_sub(ELISION_MARKER_RESERVE);
    let head_budget = content_budget * ITEM_HEAD_PERCENT / 100;
    let tail_budget = content_budget.saturating_sub(head_budget);

    // Head: snap down to a line boundary for a clean cut (only ever shrinks it).
    let head_end = floor_char_boundary(body, head_budget);
    let head_end = body[..head_end]
        .rfind('\n')
        .map(|i| i + 1)
        .unwrap_or(head_end);

    // Tail: the last `tail_budget` bytes, snapped up to a char boundary. The end
    // of the body is always retained, so a trailing note survives.
    let raw_tail_start = body.len().saturating_sub(tail_budget);
    let mut tail_start = ceil_char_boundary(body, raw_tail_start).max(head_end);

    // Advance the tail start to the next line boundary so the displayed tail
    // begins on a whole line, not a mid-line fragment. Only advance when a
    // newline leaves real content after it — otherwise the byte-exact end (and
    // any trailing note riding it) would be elided. The skipped partial line
    // folds into the elided span below, keeping the marker count accurate.
    if tail_start > head_end {
        if let Some(rel) = body[tail_start..].find('\n') {
            let line_start = tail_start + rel + 1;
            if line_start < body.len() {
                tail_start = line_start;
            }
        }
    }

    let elided = &body[head_end..tail_start];
    let elided_chars = elided.len();
    let elided_lines = elided.matches('\n').count();
    let head = &body[..head_end];
    let tail = &body[tail_start..];
    let head_sep = if head.is_empty() || head.ends_with('\n') {
        ""
    } else {
        "\n"
    };
    format!(
        "{head}{head_sep}--- elided {elided_lines} lines / {elided_chars} chars; tail kept below ---\n{tail}"
    )
}

/// Render one item's body block (no header): tail-capped to `body_budget`, with
/// an empty body shown as `(no output)`.
fn render_item_body(outcome: &ItemOutcome, body_budget: usize) -> String {
    if outcome.body.is_empty() {
        "(no output)".to_string()
    } else {
        cap_item_body_tail_biased(&outcome.body, body_budget)
    }
}

/// Divide `total` across items by their `natural` (uncapped) sizes: items smaller
/// than their equal share take their full size and donate the surplus to larger
/// items (classic water-filling). Each returned allocation is at least the equal
/// division of whatever budget remained when that item was filled.
fn water_fill_budgets(natural: &[usize], total: usize) -> Vec<usize> {
    let n = natural.len();
    let mut alloc = vec![0usize; n];
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by_key(|&i| natural[i]);
    let mut remaining = total;
    let mut left = n;
    for &i in &order {
        let share = remaining / left.max(1);
        let give = natural[i].min(share);
        alloc[i] = give;
        remaining = remaining.saturating_sub(give);
        left -= 1;
    }
    alloc
}

/// Compose per-item outcomes into a single result string, bounded by
/// `MAX_RUN_RESULT_CHARS`. A single item returns its (tail-capped) body directly
/// with no header. Multiple items are labeled `=== <header> ===` in input order;
/// the batch cap is divided fairly across them (water-filling) and each item's
/// body is tail-capped within its share, so every item surfaces meaningful
/// output. Whole-item omission, with a named note, remains only as a last resort
/// for pathological item counts where the per-item share falls below a usable
/// floor. Because each item's body is capped (head + marker + tail) within its
/// allocation, the composed result stays under `MAX_RUN_RESULT_CHARS` — the CLI's
/// equal cap then passes it through untouched (no double truncation).
fn compose_run_output(outcomes: &[ItemOutcome]) -> String {
    if outcomes.len() == 1 {
        return render_item_body(&outcomes[0], MAX_RUN_RESULT_CHARS);
    }

    let headers: Vec<String> = outcomes
        .iter()
        .map(|o| format!("=== {} ===\n", o.header))
        .collect();
    // Fixed per-item cost that must always be present: the header line plus one
    // inter-segment separator.
    let overhead: Vec<usize> = headers.iter().map(|h| h.len() + 1).collect();
    let natural: Vec<usize> = outcomes
        .iter()
        .enumerate()
        .map(|(i, o)| {
            let body_len = if o.body.is_empty() {
                "(no output)".len()
            } else {
                o.body.len()
            };
            overhead[i] + body_len
        })
        .collect();

    let total_budget = MAX_RUN_RESULT_CHARS.saturating_sub(OMISSION_NOTE_RESERVE);
    let alloc = water_fill_budgets(&natural, total_budget);

    let mut segments: Vec<String> = Vec::with_capacity(outcomes.len());
    let mut omitted: Vec<String> = Vec::new();
    for (i, o) in outcomes.iter().enumerate() {
        let body_budget = alloc[i].saturating_sub(overhead[i]);
        let natural_body = natural[i] - overhead[i];
        // Last resort: an item that does not fit and whose share is below the
        // usable floor is omitted whole rather than shown as a sliver.
        if natural_body > body_budget && body_budget < MIN_ITEM_BODY_BUDGET {
            omitted.push(o.header.clone());
            continue;
        }
        segments.push(format!(
            "{}{}",
            headers[i],
            render_item_body(o, body_budget)
        ));
    }

    let mut text = segments.join("\n");
    if !omitted.is_empty() {
        if !text.is_empty() {
            text.push_str("\n\n");
        }
        text.push_str(&format!(
            "--- {} of {} items omitted (batch {}K char cap); re-run each scoped (grep/tail the command) or open a terminal resource: {} ---",
            omitted.len(),
            outcomes.len(),
            MAX_RUN_RESULT_CHARS / 1000,
            omitted.join(", "),
        ));
    }

    text
}

#[derive(Clone)]
struct TerminalResourceTarget {
    slug: String,
    job_id: Option<String>,
    project_id: String,
    run_id: Option<String>,
    cwd: String,
}

async fn resolve_terminal_resource_target(
    db: &LocalDb,
    resource: &CairnResource,
) -> DbResult<TerminalResourceTarget> {
    let resource = resource.clone();
    db.read(|conn| {
        Box::pin(async move {
            match resource {
                CairnResource::NodeTerminal {
                    project,
                    number,
                    exec_seq,
                    node_id,
                    slug,
                } => {
                    let job_id = find_terminal_target_job_id(conn, &project, number, exec_seq, &node_id)
                        .await?
                        .ok_or_else(|| {
                            crate::storage::DbError::Row(format!(
                                "No node job found for terminal target {project}-{number}/{exec_seq}/{node_id}"
                            ))
                        })?;
                    let mut rows = conn
                        .query(
                            "
                            SELECT j.project_id, COALESCE(j.worktree_path, p.repo_path), r.id
                            FROM jobs j
                            JOIN projects p ON j.project_id = p.id
                            LEFT JOIN runs r ON r.job_id = j.id
                            WHERE j.id = ?1
                            ORDER BY r.created_at DESC
                            LIMIT 1
                            ",
                            (job_id.as_str(),),
                        )
                        .await?;
                    let row = rows.next().await?.ok_or_else(|| {
                        crate::storage::DbError::Row(format!("No job found for id {job_id}"))
                    })?;
                    Ok(TerminalResourceTarget {
                        slug,
                        job_id: Some(job_id),
                        project_id: row.text(0)?,
                        cwd: row.text(1)?,
                        run_id: row.opt_text(2)?,
                    })
                }
                CairnResource::TaskTerminal {
                    project,
                    number,
                    exec_seq,
                    node_id,
                    task_name,
                    slug,
                } => {
                    let job_id = find_task_terminal_target_job_id(
                        conn,
                        &project,
                        number,
                        exec_seq,
                        &node_id,
                        &task_name,
                    )
                    .await?
                    .ok_or_else(|| {
                        crate::storage::DbError::Row(format!(
                            "No task job found for terminal target {project}-{number}/{exec_seq}/{node_id}/task/{task_name}"
                        ))
                    })?;
                    let mut rows = conn
                        .query(
                            "
                            SELECT j.project_id, COALESCE(j.worktree_path, p.repo_path), r.id
                            FROM jobs j
                            JOIN projects p ON j.project_id = p.id
                            LEFT JOIN runs r ON r.job_id = j.id
                            WHERE j.id = ?1
                            ORDER BY r.created_at DESC
                            LIMIT 1
                            ",
                            (job_id.as_str(),),
                        )
                        .await?;
                    let row = rows.next().await?.ok_or_else(|| {
                        crate::storage::DbError::Row(format!("No job found for id {job_id}"))
                    })?;
                    Ok(TerminalResourceTarget {
                        slug,
                        job_id: Some(job_id),
                        project_id: row.text(0)?,
                        cwd: row.text(1)?,
                        run_id: row.opt_text(2)?,
                    })
                }
                CairnResource::ProjectTerminal { project, slug } => {
                    let lookup_key = project.to_uppercase();
                    let mut rows = conn
                        .query(
                            "SELECT id, repo_path FROM projects WHERE key = ?1 LIMIT 1",
                            (lookup_key.as_str(),),
                        )
                        .await?;
                    let row = rows.next().await?.ok_or_else(|| {
                        crate::storage::DbError::Row(format!("No project found for key {project}"))
                    })?;
                    Ok(TerminalResourceTarget {
                        slug,
                        job_id: None,
                        project_id: row.text(0)?,
                        cwd: row.text(1)?,
                        run_id: None,
                    })
                }
                _ => Err(crate::storage::DbError::Row(
                    "Resource is not a terminal URI".to_string(),
                )),
            }
        })
    })
    .await
}

async fn ensure_terminal_slug_available(
    db: &LocalDb,
    target: &TerminalResourceTarget,
) -> DbResult<()> {
    let job_id = target.job_id.clone();
    let project_id = target.project_id.clone();
    let slug = target.slug.clone();
    db.read(|conn| {
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        let slug = slug.clone();
        Box::pin(async move {
            // Only a *running* terminal blocks the slug. Exited rows linger (for
            // post-exit reads and already-exited wake subscribes) and are
            // reclaimed on insert, so they must not block re-creating the slug.
            let exists = match job_id.as_deref() {
                Some(job_id) => terminal_slug_running_for_job(conn, job_id, &slug).await?,
                None => terminal_slug_running_for_project(conn, &project_id, &slug).await?,
            };
            if exists {
                return Err(crate::storage::DbError::Row(format!(
                    "A running terminal already exists in this scope: {slug}"
                )));
            }
            Ok(())
        })
    })
    .await
}

async fn insert_terminal_resource(
    db: &LocalDb,
    target: &TerminalResourceTarget,
    session_id: &str,
    command: &str,
    description: Option<&str>,
) -> DbResult<()> {
    let job_id = target.job_id.clone();
    let project_id = target.project_id.clone();
    let run_id = target.run_id.clone();
    let slug = target.slug.clone();
    let session_id = session_id.to_string();
    let command = command.to_string();
    let description = description.map(ToOwned::to_owned);

    db.write(|conn| {
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        let run_id = run_id.clone();
        let slug = slug.clone();
        let session_id = session_id.clone();
        let command = command.clone();
        let description = description.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp() as i32;
            let id = Uuid::new_v4().to_string();
            // Reclaim a lingering exited row for this (scope, slug). The
            // create-availability check only blocks running slugs, so an exited
            // row could still collide with the unique (scope, slug) index.
            match job_id.as_deref() {
                Some(job_id) => {
                    conn.execute(
                        "DELETE FROM job_terminals WHERE job_id = ?1 AND slug = ?2",
                        (job_id, slug.as_str()),
                    )
                    .await?;
                }
                None => {
                    conn.execute(
                        "DELETE FROM job_terminals WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2",
                        (project_id.as_str(), slug.as_str()),
                    )
                    .await?;
                }
            }
            conn.execute(
                "
                INSERT INTO job_terminals (
                    id, job_id, project_id, run_id, session_id, command, title,
                    description, status, exit_code, created_at, exited_at, slug
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, 'running', NULL, ?8, NULL, ?9)
                ",
                (
                    id.as_str(),
                    job_id.as_deref(),
                    if job_id.is_some() {
                        None
                    } else {
                        Some(project_id.as_str())
                    },
                    run_id.as_deref(),
                    session_id.as_str(),
                    command.as_str(),
                    description.as_deref(),
                    now,
                    slug.as_str(),
                ),
            )
            .await?;
            Ok(())
        })
    })
    .await
}

async fn update_terminal_resource_session(
    db: &LocalDb,
    target: &TerminalResourceTarget,
    session_id: &str,
) -> DbResult<()> {
    let job_id = target.job_id.clone();
    let project_id = target.project_id.clone();
    let slug = target.slug.clone();
    let session_id = session_id.to_string();
    db.write(|conn| {
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        let slug = slug.clone();
        let session_id = session_id.clone();
        Box::pin(async move {
            if let Some(job_id) = job_id {
                conn.execute(
                    "
                    UPDATE job_terminals
                    SET session_id = ?3, status = 'running', exit_code = NULL, exited_at = NULL
                    WHERE job_id = ?1 AND slug = ?2
                    ",
                    (job_id.as_str(), slug.as_str(), session_id.as_str()),
                )
                .await?;
            } else {
                conn.execute(
                    "
                    UPDATE job_terminals
                    SET session_id = ?3, status = 'running', exit_code = NULL, exited_at = NULL
                    WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2
                    ",
                    (project_id.as_str(), slug.as_str(), session_id.as_str()),
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
}

async fn find_terminal_target_job_id(
    conn: &turso::Connection,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_id: &str,
) -> DbResult<Option<String>> {
    let lookup_key = project_key.to_uppercase();
    let mut issue_rows = conn
        .query(
            "
            SELECT i.id
            FROM issues i
            JOIN projects p ON i.project_id = p.id
            WHERE p.key = ?1 AND i.number = ?2
            LIMIT 1
            ",
            (lookup_key.as_str(), issue_number),
        )
        .await?;

    let Some(issue_row) = issue_rows.next().await? else {
        return Ok(None);
    };
    let issue_id = issue_row.text(0)?;

    let mut exec_rows = conn
        .query(
            "
            SELECT id
            FROM executions
            WHERE issue_id = ?1 AND seq = ?2
            LIMIT 1
            ",
            (issue_id.as_str(), exec_seq),
        )
        .await?;

    let Some(exec_row) = exec_rows.next().await? else {
        return Ok(None);
    };
    let exec_id = exec_row.text(0)?;

    let mut exact_rows = conn
        .query(
            "
            SELECT id
            FROM jobs
            WHERE issue_id = ?1
              AND execution_id = ?2
              AND parent_job_id IS NULL
              AND uri_segment = ?3
            LIMIT 1
            ",
            (issue_id.as_str(), exec_id.as_str(), node_id),
        )
        .await?;

    if let Some(row) = exact_rows.next().await? {
        return Ok(Some(row.text(0)?));
    }

    Ok(None)
}

async fn find_task_terminal_target_job_id(
    conn: &turso::Connection,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    parent_node_id: &str,
    task_name: &str,
) -> DbResult<Option<String>> {
    let lookup_key = project_key.to_uppercase();
    let mut rows = conn
        .query(
            "
            SELECT child.id
            FROM jobs parent
            JOIN jobs child ON child.parent_job_id = parent.id
            JOIN issues i ON parent.issue_id = i.id
            JOIN projects p ON i.project_id = p.id
            JOIN executions e ON parent.execution_id = e.id
            WHERE p.key = ?1
              AND i.number = ?2
              AND e.seq = ?3
              AND parent.parent_job_id IS NULL
              AND parent.uri_segment = ?4
              AND child.uri_segment = ?5
            LIMIT 1
            ",
            (
                lookup_key.as_str(),
                issue_number,
                exec_seq,
                parent_node_id,
                task_name,
            ),
        )
        .await?;
    rows.next().await?.map(|row| row.text(0)).transpose()
}

async fn terminal_slug_exists_for_job(
    conn: &turso::Connection,
    job_id: &str,
    slug: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM job_terminals WHERE job_id = ?1 AND slug = ?2",
            (job_id, slug),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| crate::storage::DbError::Row("missing terminal count".to_string()))?;
    Ok(row.i64(0)? > 0)
}

async fn terminal_slug_exists_for_project(
    conn: &turso::Connection,
    project_id: &str,
    slug: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "
            SELECT COUNT(*)
            FROM job_terminals
            WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2
            ",
            (project_id, slug),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| crate::storage::DbError::Row("missing terminal count".to_string()))?;
    Ok(row.i64(0)? > 0)
}

/// Whether a *running* terminal with this slug exists in the job scope. Used by
/// create-availability so a lingering exited row does not block re-use.
async fn terminal_slug_running_for_job(
    conn: &turso::Connection,
    job_id: &str,
    slug: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM job_terminals
             WHERE job_id = ?1 AND slug = ?2 AND status = 'running'",
            (job_id, slug),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| crate::storage::DbError::Row("missing terminal count".to_string()))?;
    Ok(row.i64(0)? > 0)
}

async fn terminal_slug_running_for_project(
    conn: &turso::Connection,
    project_id: &str,
    slug: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM job_terminals
             WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2 AND status = 'running'",
            (project_id, slug),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| crate::storage::DbError::Row("missing terminal count".to_string()))?;
    Ok(row.i64(0)? > 0)
}

/// A job-owned terminal row, loaded for terminal-exit wake subscribe validation
/// and the already-exited immediate-fire path.
pub(crate) struct TerminalWakeRow {
    pub status: String,
    pub exit_code: Option<i32>,
    pub created_at: i64,
    pub exited_at: Option<i64>,
    pub output_tail: Option<String>,
    /// The live PTY session id, used to reach the session's in-memory output
    /// buffer and phrase-watcher registry when subscribing an output wake.
    pub session_id: Option<String>,
}

/// Look up a terminal by job scope + slug for wake subscription.
pub(crate) async fn lookup_terminal_for_wake(
    db: &LocalDb,
    job_id: &str,
    slug: &str,
) -> DbResult<Option<TerminalWakeRow>> {
    let job_id = job_id.to_string();
    let slug = slug.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        let slug = slug.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status, exit_code, created_at, exited_at, output_tail, session_id
                     FROM job_terminals WHERE job_id = ?1 AND slug = ?2 LIMIT 1",
                    params![job_id.as_str(), slug.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            Ok(Some(TerminalWakeRow {
                status: row.text(0)?,
                exit_code: row.opt_i64(1)?.map(|code| code as i32),
                created_at: row.i64(2)?,
                exited_at: row.opt_i64(3)?,
                output_tail: row.opt_text(4)?,
                session_id: row.opt_text(5)?,
            }))
        })
    })
    .await
}

/// List the terminal slugs owned by a job, for a precise unknown-slug error.
pub(crate) async fn list_job_terminal_slugs(db: &LocalDb, job_id: &str) -> DbResult<Vec<String>> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT slug FROM job_terminals
                     WHERE job_id = ?1 AND slug IS NOT NULL ORDER BY slug",
                    params![job_id.as_str()],
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
}

/// Whether a terminal with this slug exists in any state (running or exited).
/// The promote `run-N` auto-allocation scan uses this so exited run slugs stay
/// taken and run numbers remain monotonic.
async fn terminal_slug_taken_any(db: &LocalDb, target: &TerminalResourceTarget) -> DbResult<bool> {
    let job_id = target.job_id.clone();
    let project_id = target.project_id.clone();
    let slug = target.slug.clone();
    db.read(|conn| {
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        let slug = slug.clone();
        Box::pin(async move {
            match job_id.as_deref() {
                Some(job_id) => terminal_slug_exists_for_job(conn, job_id, &slug).await,
                None => terminal_slug_exists_for_project(conn, &project_id, &slug).await,
            }
        })
    })
    .await
}

fn block_on_background_db<T>(fut: impl std::future::Future<Output = DbResult<T>>) -> DbResult<T> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(crate::storage::DbError::from)?
        .block_on(fut)
}

/// Context loaded while marking a terminal exited: the row's `created_at` (for
/// runtime) and, for a nonzero exit on a job-owned terminal, the injection
/// target (the owning job's current session + the exit code).
struct TerminalExitContext {
    created_at: Option<i64>,
    injection: Option<(String, i32)>,
}

/// Mark a terminal row exited and retain its output tail (always), and load the
/// context the exit path needs. Supersedes the old delete-on-exit: the row now
/// lingers as `status='exited'` so post-exit reads and already-exited wake
/// subscribes can still see it. Mark-exited is decoupled from the nonzero-only
/// injection-target lookup so the two concerns are independent.
///
/// The UPDATE is gated on `status='running'`: it is the single idempotency guard
/// for finalize. Returns `Ok(None)` when no running row matched (already exited
/// or gone), so every caller — the reader EOF, the promoted watcher, and the
/// external kill entry point — converges here without ever finalizing twice with
/// different codes.
async fn mark_terminal_exited_and_load_context(
    db: Arc<LocalDb>,
    terminal_session_id: String,
    job_id: Option<String>,
    exit_code: Option<i32>,
    output_tail: Option<String>,
) -> DbResult<Option<TerminalExitContext>> {
    db.write(|conn| {
        let terminal_session_id = terminal_session_id.clone();
        let job_id = job_id.clone();
        let output_tail = output_tail.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();
            let transitioned = conn
                .execute(
                    "UPDATE job_terminals
                 SET status = 'exited', exit_code = ?2, exited_at = ?3, output_tail = ?4
                 WHERE session_id = ?1 AND status = 'running'",
                    params![
                        terminal_session_id.as_str(),
                        exit_code.map(|code| code as i64),
                        now,
                        output_tail.as_deref()
                    ],
                )
                .await?;
            if transitioned == 0 {
                return Ok(None);
            }

            let mut rows = conn
                .query(
                    "SELECT created_at FROM job_terminals WHERE session_id = ?1 LIMIT 1",
                    params![terminal_session_id.as_str()],
                )
                .await?;
            let created_at = rows.next().await?.map(|row| row.i64(0)).transpose()?;
            drop(rows);

            // Nonzero-exit injection target: the owning job's current session.
            let injection = match (job_id, exit_code.filter(|code| *code != 0)) {
                (Some(job_id), Some(code)) => {
                    let mut rows = conn
                        .query(
                            "SELECT current_session_id FROM jobs WHERE id = ?1 LIMIT 1",
                            params![job_id.as_str()],
                        )
                        .await?;
                    rows.next()
                        .await?
                        .map(|row| row.opt_text(0))
                        .transpose()?
                        .flatten()
                        .map(|session_id| (session_id, code))
                }
                _ => None,
            };

            Ok(Some(TerminalExitContext {
                created_at,
                injection,
            }))
        })
    })
    .await
}

/// Capture the last ~2000 chars of a terminal's buffer for the retained
/// `output_tail` and the wake message.
fn capture_output_tail(buffer: &Arc<Mutex<VecDeque<u8>>>) -> Option<String> {
    let bytes: Vec<u8> = buffer
        .lock()
        .map(|b| b.iter().copied().collect())
        .unwrap_or_default();
    if bytes.is_empty() {
        return None;
    }
    let text = String::from_utf8_lossy(&bytes);
    let chars: Vec<char> = text.chars().collect();
    let start = chars.len().saturating_sub(2000);
    let tail: String = chars[start..].iter().collect();
    if tail.trim().is_empty() {
        None
    } else {
        Some(tail)
    }
}

/// Outcome of registering an output-phrase watcher on a live terminal session.
pub(crate) enum OutputWatchRegistration {
    /// The phrase is already present in the terminal's current buffer; the
    /// caller should fire the wake immediately rather than register a watcher.
    AlreadyPresent { excerpt: Option<String> },
    /// A live watcher was registered; it fires when the phrase next appears.
    Registered,
    /// No live agent PTY session backs this terminal, so its output cannot be
    /// watched (e.g. a promoted-run or already-finalized session).
    NotLive,
}

/// Register a phrase watcher on a live terminal session, after first scanning
/// the session's current output buffer so an already-printed phrase fires
/// immediately instead of waiting for new output.
pub(crate) fn register_terminal_output_watcher(
    orch: &Orchestrator,
    session_id: &str,
    subscription_id: &str,
    job_id: &str,
    phrase: &str,
    terminal_uri: &str,
) -> OutputWatchRegistration {
    let session_arc = {
        let Ok(sessions) = orch.pty_state.sessions.lock() else {
            return OutputWatchRegistration::NotLive;
        };
        match sessions.get(session_id) {
            Some(arc) => arc.clone(),
            None => return OutputWatchRegistration::NotLive,
        }
    };
    let (watchers, buffer) = {
        let Ok(session) = session_arc.lock() else {
            return OutputWatchRegistration::NotLive;
        };
        let Some(watchers) = session.output_watchers.clone() else {
            return OutputWatchRegistration::NotLive;
        };
        (watchers, session.output_buffer.clone())
    };
    if let Some(buffer) = buffer {
        let bytes: Vec<u8> = buffer
            .lock()
            .map(|b| b.iter().copied().collect())
            .unwrap_or_default();
        let text = String::from_utf8_lossy(&bytes);
        let scan = crate::services::scan_for_phrase("", &text, phrase);
        if scan.matched_excerpt.is_some() {
            return OutputWatchRegistration::AlreadyPresent {
                excerpt: scan.matched_excerpt,
            };
        }
    }
    // Bind to a local so the lock-guard temporary drops here, before the
    // function's `watchers`/`buffer` locals (a tail-position match would keep
    // the guard's borrow alive past their drop — E0597).
    let outcome = match watchers.lock() {
        Ok(mut guard) => {
            // Replace any existing watcher for this subscription (a re-subscribe
            // may change the phrase) so the session never carries a stale one,
            // and a subscribe that lands on a session already hydrated for this
            // same row does not double-register it.
            guard.retain(|w| w.subscription_id != subscription_id);
            guard.push(crate::services::TerminalOutputWatcher {
                subscription_id: subscription_id.to_string(),
                job_id: job_id.to_string(),
                phrase: phrase.to_string(),
                carry: String::new(),
                terminal_uri: terminal_uri.to_string(),
            });
            OutputWatchRegistration::Registered
        }
        Err(_) => OutputWatchRegistration::NotLive,
    };
    outcome
}

async fn lookup_terminal_session_id_for_target(
    db: &LocalDb,
    target: &TerminalResourceTarget,
) -> DbResult<Option<String>> {
    let job_id = target.job_id.clone();
    let project_id = target.project_id.clone();
    let slug = target.slug.clone();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = if let Some(job_id) = job_id {
                conn.query(
                    "
                    SELECT session_id
                    FROM job_terminals
                    WHERE job_id = ?1 AND slug = ?2
                    LIMIT 1
                    ",
                    (job_id.as_str(), slug.as_str()),
                )
                .await?
            } else {
                conn.query(
                    "
                    SELECT session_id
                    FROM job_terminals
                    WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2
                    LIMIT 1
                    ",
                    (project_id.as_str(), slug.as_str()),
                )
                .await?
            };
            crate::storage::next_text(&mut rows, 0).await
        })
    })
    .await
}

/// Run a single resolved item: execute it, format its body, and (for shell
/// items only) cache checkpoint results and auto-push on `git commit`.
#[allow(clippy::too_many_arguments)]
async fn run_one(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    cwd: &str,
    tool_use_id: &str,
    run_context: Option<&super::RunContext>,
    _commit_present: bool,
    header: String,
    spec: Result<RunSpec, String>,
) -> ItemOutcome {
    let spec = match spec {
        Ok(spec) => spec,
        Err(e) => {
            return ItemOutcome {
                header,
                body: e,
                succeeded: false,
                suspended: false,
            }
        }
    };

    let (shell, flag) = if cfg!(windows) {
        ("cmd.exe", "/c")
    } else {
        ("bash", "-c")
    };

    let (program, args, timeout, shell_command): (
        String,
        Vec<String>,
        Option<u32>,
        Option<String>,
    ) = match spec {
        RunSpec::Shell { command, timeout } => (
            shell.to_string(),
            vec![flag.to_string(), command.clone()],
            timeout,
            Some(command),
        ),
        RunSpec::Script {
            program,
            args,
            timeout,
        } => (program, args, timeout, None),
        // MCP calls are proxied RPC through the host gateway, not process
        // exec — handle and return here.
        RunSpec::McpCall(spec) => {
            return run_mcp_call(orch, run_context, cwd, header, *spec).await;
        }
    };

    let timeout_ms = timeout.unwrap_or(120_000).min(600_000);

    let mut exec = match execute_process(
        orch,
        cwd,
        tool_use_id,
        run_context,
        &program,
        &args,
        timeout_ms,
        shell_command.as_deref(),
        true,
    )
    .await
    {
        Ok(exec) => exec,
        Err(e) => {
            return ItemOutcome {
                header,
                body: format!("Failed to spawn command: {e}"),
                succeeded: false,
                suspended: false,
            }
        }
    };

    // Denial-driven worktree fence: if the kernel sandbox blocked this command,
    // adjudicate the crossing under the agent's escape policy. On allow we
    // re-execute escalated (sandbox off) so the now-granted crossing proceeds.
    if let Some(denial) = exec.denial.take() {
        use crate::mcp::handlers::fence;
        // The live checkout is read-only and non-negotiable: a non-worktree cwd
        // routes a denial to a clear explanation, never a fence prompt. There is
        // no run context to adjudicate and nothing the user can grant — changes
        // can only be made in a worktree. This replaces the raw EPERM project
        // chat would otherwise surface.
        if !crate::jj::is_jj_dir(std::path::Path::new(cwd)) {
            let _ = denial;
            return ItemOutcome {
                header,
                body: "This command tried to write the project's live checkout, which is read-only for agents — the write was blocked by the kernel sandbox. Changes can only be made in a worktree; nothing was left in the checkout. Re-run inside a worktree to make changes that persist."
                    .to_string(),
                succeeded: false,
                suspended: false,
            };
        }
        if let Some((run_id, fence_mode)) = fence::resolve_run_fence(orch, request).await {
            let crossing = match &denial {
                sandbox::SandboxDenial::Path(p) => {
                    fence::Crossing::shell_path(p.as_path(), &p.display().to_string())
                }
                sandbox::SandboxDenial::Command => fence::Crossing::shell_command(
                    format!("command blocked by the worktree sandbox: {header}"),
                    shell_command.as_deref().unwrap_or(&program),
                ),
            };
            match fence::raise_fence(orch, &run_id, fence_mode, request, crossing).await {
                fence::FenceDecision::Allow => {
                    match execute_process(
                        orch,
                        cwd,
                        tool_use_id,
                        run_context,
                        &program,
                        &args,
                        timeout_ms,
                        shell_command.as_deref(),
                        false,
                    )
                    .await
                    {
                        Ok(e) => exec = e,
                        Err(e) => {
                            return ItemOutcome {
                                header,
                                body: format!("Failed to spawn command: {e}"),
                                succeeded: false,
                                suspended: false,
                            }
                        }
                    }
                }
                fence::FenceDecision::Deny(msg) => {
                    return ItemOutcome {
                        header,
                        body: msg,
                        succeeded: false,
                        suspended: false,
                    }
                }
                fence::FenceDecision::Suspended => {
                    return ItemOutcome {
                        header,
                        body: "Run suspended pending worktree fence approval; resume will \
                               continue once it is answered."
                            .to_string(),
                        succeeded: false,
                        suspended: true,
                    }
                }
            }
        }
    }

    // A timed-out item with a run context was detached to a terminal rather than
    // killed. Its outcome is unknown, so it does NOT count as succeeded (a
    // sequential batch with stop_on_error halts after it), and the shell-keyed
    // side effects below (checkpoint cache, git auto-push) are skipped entirely.
    if let Some(promoted) = exec.promoted.take() {
        let mut body = String::new();
        if !exec.stdout.is_empty() {
            body.push_str(&exec.stdout);
        }
        if !exec.stderr.is_empty() {
            if !body.is_empty() {
                body.push_str("\n\n");
            }
            body.push_str("stderr:\n");
            body.push_str(&exec.stderr);
        }
        if !body.is_empty() {
            body.push_str("\n\n");
        }
        body.push_str(&format!(
            "Command still running; detached to {0} — readable and killable there. \
             To end your turn and resume when it exits, subscribe via cairn:~/wakes: \
             {{subscribe:{{kind:\"terminal\", ref:\"{0}\", on:\"exit\"}}}}.",
            promoted.uri
        ));
        return ItemOutcome {
            header,
            body,
            succeeded: false,
            suspended: false,
        };
    }

    let succeeded = !exec.timed_out && exec.exit_code.is_none_or(|code| code == 0);

    // Shell-only side effects keyed on the command string.
    if let Some(ref command) = shell_command {
        if !exec.timed_out {
            if let Some(ctx) = run_context {
                cache_checkpoint_result(orch, &ctx.job_id, command, cwd, exec.exit_code).await;
            }
        }
    }

    let body = format_exec_body(&exec, timeout_ms);
    ItemOutcome {
        header,
        body,
        succeeded,
        suspended: false,
    }
}

/// Execute a proxied MCP `tools/call` through the host gateway. A missing
/// gateway or a down/erroring server fails this item only — never the batch.
async fn run_mcp_call(
    orch: &Orchestrator,
    run_context: Option<&super::RunContext>,
    cwd: &str,
    header: String,
    spec: McpCallSpec,
) -> ItemOutcome {
    let McpCallSpec {
        credential_key,
        tool,
        args,
        config,
        timeout,
    } = spec;
    let Some(gateway) = orch.mcp_gateway() else {
        return ItemOutcome {
            header,
            body: "MCP gateway is not available in this host".to_string(),
            succeeded: false,
            suspended: false,
        };
    };

    // Per-session connection pooling keys on the run's job id, isolating
    // concurrent agents. Fall back to the cwd when there is no run context.
    let session_key = run_context
        .map(|c| c.job_id.clone())
        .unwrap_or_else(|| cwd.to_string());

    match gateway
        .call_tool(&session_key, &credential_key, &config, &tool, args, timeout)
        .await
    {
        Ok(body) => ItemOutcome {
            header,
            body,
            succeeded: true,
            suspended: false,
        },
        Err(e) => ItemOutcome {
            header,
            body: e,
            succeeded: false,
            suspended: false,
        },
    }
}

/// Format process output into the human-readable body shown for an item.
fn format_exec_body(exec: &ExecOutput, timeout_ms: u32) -> String {
    let mut result = String::new();
    if exec.timed_out {
        result.push_str(&format!("Command timed out after {}ms\n\n", timeout_ms));
    }
    if !exec.stdout.is_empty() {
        result.push_str(&exec.stdout);
    }
    if !exec.stderr.is_empty() {
        if !result.is_empty() {
            result.push_str("\n\n");
        }
        result.push_str("stderr:\n");
        result.push_str(&exec.stderr);
    }
    if let Some(code) = exec.exit_code {
        if code != 0 {
            if !result.is_empty() {
                result.push_str("\n\n");
            }
            result.push_str(&format!("Exit code: {}", code));
        }
    }
    result
}

/// Spawn a process, stream its stdout/stderr, and wait with a timeout.
#[allow(clippy::too_many_arguments)]
async fn execute_process(
    orch: &Orchestrator,
    cwd: &str,
    tool_use_id: &str,
    run_context: Option<&super::RunContext>,
    program: &str,
    args: &[String],
    timeout_ms: u32,
    shell_command: Option<&str>,
    sandbox_enabled: bool,
) -> Result<ExecOutput, String> {
    let services = &orch.services;
    let tool_use_id = tool_use_id.to_string();
    let inline_command_id = Uuid::new_v4().to_string();

    // Build the OS filesystem sandbox for this spawn (None for fence allow, no
    // run context, an already-granted command, or platforms without a sandbox
    // primitive). `sandbox_enabled` is false on an escalated
    // re-execution after a fence grant.
    let sandbox = if sandbox_enabled {
        // Grant key: the shell command for shell items, else the program for
        // skill scripts — so a command-scoped session grant generalizes to
        // scripts too (the only crossing kind Linux produces).
        build_run_sandbox_policy(
            orch,
            cwd,
            run_context.map(|c| c.run_id.as_str()),
            run_context.map(|c| c.project_id.as_str()),
            shell_command.or(Some(program)),
        )
        .await
    } else {
        None
    };
    // Ask agents get a synthetic command-scoped macOS fallback when path recovery
    // misses; Deny agents preserve their raw fail-fast output. Non-worktree (live
    // checkout) runs ALSO enable fallback detection regardless of fence: the
    // checkout is read-only, so a kernel block must be recognized even when macOS
    // log recovery misses or a shell masks the exit (`... || true`). run_one
    // routes such a denial to the hard read-only-checkout message, never a fence
    // prompt — non-worktree is non-grantable, so enabling detection cannot
    // synthesize a grant.
    let non_worktree = !crate::jj::is_jj_dir(std::path::Path::new(cwd));
    let command_scoped_fallback =
        non_worktree || matches!(sandbox.as_ref().map(|(_, fence)| *fence), Some(Fence::Ask));
    let sandbox_policy = sandbox.map(|(policy, _)| policy);
    let sandboxed = sandbox_policy.is_some();
    let spawn_started = std::time::SystemTime::now();

    // Inject the MCP callback env so an in-run `cairn read|change ...` shell
    // invocation can authenticate and forward to this same app (the basis for
    // composability like `cairn read <uri> | rg ...`). The CLI is a thin client;
    // it never opens the DB, it forwards over this callback.
    let mut spawn_config = SpawnConfig::new(program);
    for arg in args {
        spawn_config = spawn_config.arg(arg);
    }
    let mut spawn_config = apply_non_interactive_pager_env(
        spawn_config
            .cwd(cwd)
            // Explicit worktree anchor so an agent that `cd`s into a subdir can still
            // address the worktree root. Shell `~`/`$HOME` are intentionally left
            // untouched (gh/cargo/ssh/npm configs live there).
            .env("CAIRN_WORKTREE", cwd)
            .env(
                "CAIRN_CALLBACK_URL",
                &format!("http://127.0.0.1:{}/api/mcp", orch.mcp_callback_port),
            )
            .sandbox(sandbox_policy),
    );
    if let Ok(secret) = orch.mcp_auth.get_secret_for_mcp() {
        spawn_config = spawn_config.env("CAIRN_MCP_SECRET", &secret);
    }
    // Managed Build Services: when this spawn is fenced, inject each enabled
    // service's client env so the agent's tooling connects to the Cairn-owned
    // daemon (e.g. the shared sccache server) rather than auto-starting its own
    // confined one (the cross-worktree EPERM bug). `CAIRN_SANDBOXED` is set by
    // the process spawner. Inert for projects the env doesn't apply to.
    if sandboxed {
        for (k, v) in orch.build_service_client_env(Some(std::path::Path::new(cwd))) {
            spawn_config = spawn_config.env(&k, &v);
        }
    }
    if let Some(ctx) = run_context {
        spawn_config = spawn_config.env("CAIRN_RUN_ID", &ctx.run_id);
        if let Some((name, email)) = orch.resolve_git_identity_for_project(Some(&ctx.project_id)) {
            spawn_config = spawn_config
                .env("GIT_AUTHOR_NAME", &name)
                .env("GIT_AUTHOR_EMAIL", &email)
                .env("GIT_COMMITTER_NAME", &name)
                .env("GIT_COMMITTER_EMAIL", &email);
        }
        // Point the command's temp-file handling at a per-job scratch dir so
        // default tooling (mktemp, cargo, compilers, harness logs) writes
        // scratch there with no agent awareness. The dir is a subpath of the
        // system temp root, already in the sandbox writable set, so this takes
        // no fence prompt — it just namespaces scratch per job (no cross-job
        // collisions) and lets teardown reclaim it. TMP/TEMP cover the few
        // tools that read those instead of TMPDIR.
        let scratch = crate::scratch::ensure_job_scratch_dir(&ctx.job_id);
        let scratch = scratch.to_string_lossy().to_string();
        spawn_config = spawn_config
            .env("TMPDIR", &scratch)
            .env("TMP", &scratch)
            .env("TEMP", &scratch);
        // `cairn:~` shorthand resolution needs the job's canonical home URI.
        // For a top-level node that's `.../{seq}/{segment}`; for a sub-task it
        // nests under its parent as `.../{seq}/{parent}/task/{segment}`. The
        // previous build_node_uri call passed ctx.job_name (the human-readable
        // name, not the URI segment) and ignored parent_job_id, so a sub-task's
        // spawned shell got the broken top-level shape as its home — every
        // `cairn:~/<name>` resolution from that shell pointed at the wrong
        // resource namespace.
        if let (Some(num), Some(seq)) = (ctx.issue_number, ctx.exec_seq) {
            if let Some(segment) =
                crate::jobs::queries::node_uri_segment_for_job(&orch.db.local, &ctx.job_id).await
            {
                let parent_segment =
                    crate::jobs::queries::parent_uri_segment_for_job(&orch.db.local, &ctx.job_id)
                        .await;
                let home_uri = cairn_common::uri::build_job_base_uri(
                    &ctx.project_key,
                    num,
                    seq,
                    &segment,
                    parent_segment.as_deref(),
                );
                spawn_config = spawn_config.env("CAIRN_HOME_URI", &home_uri);
            }
        }
    }

    let child = match services.process.spawn(spawn_config) {
        Ok(child) => Arc::new(Mutex::new(child)),
        Err(e) => return Err(format!("Failed to spawn command: {}", e)),
    };

    let (stdout, stderr, child_pid) = {
        let mut guard = match child.lock() {
            Ok(guard) => guard,
            Err(e) => return Err(format!("Failed to access command process: {}", e)),
        };
        (guard.take_stdout(), guard.take_stderr(), Some(guard.id()))
    };

    if let Some(ctx) = run_context {
        orch.pty_state.register_inline_command(
            ctx.run_id.clone(),
            inline_command_id.clone(),
            child.clone(),
        );
    }

    // Collect output (we'll stream it if we have a run context)
    let stdout_content = Arc::new(Mutex::new(String::new()));
    let stderr_content = Arc::new(Mutex::new(String::new()));

    // A combined byte buffer, created only for promotion-eligible items (those
    // with a run context). On promotion this same buffer becomes the terminal's
    // output buffer — already seeded with everything captured so far, and the
    // still-running reader threads keep appending so terminal reads show live
    // output with no copy and no race window.
    let combined_buffer: Option<Arc<Mutex<VecDeque<u8>>>> =
        run_context.map(|_| Arc::new(Mutex::new(VecDeque::new())));
    // Tracks the wall-clock time of the most recent output chunk. On promotion it
    // becomes the terminal's `last_output_at`, feeding the "last output Ns ago"
    // status banner the resource read path renders.
    let last_output_at: Option<Arc<Mutex<SystemTime>>> =
        run_context.map(|_| Arc::new(Mutex::new(SystemTime::now())));

    // Read stdout in a thread
    let stdout_handle = {
        let content = stdout_content.clone();
        let combined = combined_buffer.clone();
        let last_ts = last_output_at.clone();
        let emitter = services.emitter.clone();
        let run_id = run_context.map(|r| r.run_id.clone());
        let tool_id = tool_use_id.clone();

        thread::spawn(move || {
            if let Some(stdout) = stdout {
                let reader = BufReader::new(stdout);
                for line in reader.lines().map_while(Result::ok) {
                    {
                        let mut c = content.lock().unwrap();
                        if !c.is_empty() {
                            c.push('\n');
                        }
                        c.push_str(&line);
                    }
                    record_combined_output(&combined, &last_ts, &line);

                    // Stream to frontend if we have run context
                    if let Some(ref rid) = run_id {
                        let _ = emitter.emit(
                            "bash-output",
                            serde_json::to_value(BashOutputPayload {
                                run_id: rid.clone(),
                                tool_use_id: tool_id.clone(),
                                chunk: format!("{}\n", line),
                                stream: "stdout".to_string(),
                            })
                            .unwrap_or_default(),
                        );
                    }
                }
            }
        })
    };

    // Read stderr in a thread
    let stderr_handle = {
        let content = stderr_content.clone();
        let combined = combined_buffer.clone();
        let last_ts = last_output_at.clone();
        let emitter = services.emitter.clone();
        let run_id = run_context.map(|r| r.run_id.clone());
        let tool_id = tool_use_id.clone();

        thread::spawn(move || {
            if let Some(stderr) = stderr {
                let reader = BufReader::new(stderr);
                for line in reader.lines().map_while(Result::ok) {
                    {
                        let mut c = content.lock().unwrap();
                        if !c.is_empty() {
                            c.push('\n');
                        }
                        c.push_str(&line);
                    }
                    record_combined_output(&combined, &last_ts, &line);

                    // Stream to frontend if we have run context
                    if let Some(ref rid) = run_id {
                        let _ = emitter.emit(
                            "bash-output",
                            serde_json::to_value(BashOutputPayload {
                                run_id: rid.clone(),
                                tool_use_id: tool_id.clone(),
                                chunk: format!("{}\n", line),
                                stream: "stderr".to_string(),
                            })
                            .unwrap_or_default(),
                        );
                    }
                }
            }
        })
    };

    // Wait for the process without pinning a tokio worker: yield between polls.
    // A `KillOnDrop` guard ties process lifetime to this future, so an abandoned
    // request (client disconnect, MCP cancel, handler abort) reaps the whole
    // process group on drop. It is disarmed before every normal return below.
    let guard = crate::services::KillOnDrop::new(child.clone());
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms as u64);
    let mut timed_out = false;
    let mut exit_code = None;

    loop {
        let wait_result = {
            let mut g = match child.lock() {
                Ok(g) => g,
                Err(e) => return Err(format!("Failed to access command process: {}", e)),
            };
            g.try_wait()
        };

        match wait_result {
            Ok(Some(status)) => {
                exit_code = status.code();
                break;
            }
            Ok(None) => {
                if std::time::Instant::now() >= deadline {
                    timed_out = true;
                    break;
                }
                // Yields the worker instead of pinning it for the command's
                // whole duration (the old thread::sleep starved the MCP host).
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => return Err(format!("Error waiting for process: {}", e)),
        }
    }

    if timed_out {
        // Timed out with the process still alive. With a run context and no
        // sandbox denial we PROMOTE it to a durable terminal instead of killing
        // it; otherwise we kill the group. (A promoted command may keep running
        // and dirty the worktree after the batch's commit barrier evaluates —
        // same as any pre-existing terminal; out of scope here, see the issue.)
        let mut promoted: Option<PromotedTerminal> = None;
        let mut denial: Option<sandbox::SandboxDenial> = None;

        if let Some(ctx) = run_context {
            // Denial-before-promote: a command blocked by the kernel sandbox must
            // be adjudicated through the worktree fence, not detached. Detect on
            // the partial output captured so far.
            if sandboxed {
                let partial = {
                    let o = stdout_content.lock().unwrap().clone();
                    let e = stderr_content.lock().unwrap().clone();
                    match (o.is_empty(), e.is_empty()) {
                        (false, false) => format!("{o}\n{e}"),
                        (false, true) => o,
                        (true, false) => e,
                        (true, true) => String::new(),
                    }
                };
                denial = sandbox::detect_denial(
                    None,
                    &partial,
                    child_pid,
                    spawn_started,
                    command_scoped_fallback,
                );
            }

            if denial.is_none() {
                let output_buffer = combined_buffer
                    .clone()
                    .expect("combined buffer is created whenever run_context is set");
                let display_cmd = shell_command.unwrap_or(program);
                match promote_to_terminal(
                    orch,
                    ctx,
                    cwd,
                    display_cmd,
                    child.clone(),
                    output_buffer,
                    last_output_at.clone(),
                    &inline_command_id,
                )
                .await
                {
                    Ok(p) => promoted = Some(p),
                    Err(e) => {
                        log::warn!("run promotion to terminal failed; killing instead: {e}")
                    }
                }
            }
        }

        if promoted.is_some() {
            // Ownership has moved to the terminal session (which already
            // unregistered the inline command and owns the later reaping path).
            // The reader threads keep appending to the now-terminal buffer, so we
            // deliberately do NOT join them here.
            guard.disarm();
        } else {
            // Kill fallback: SIGKILL the group, then bound the reader reaping so a
            // setsid escapee holding a pipe open cannot hang the call forever.
            if let Ok(mut g) = child.lock() {
                let _ = g.kill();
            }
            guard.disarm();
            if let Some(ctx) = run_context {
                orch.pty_state
                    .unregister_inline_command(&ctx.run_id, &inline_command_id);
            }
            reap_readers_bounded(stdout_handle, stderr_handle).await;
        }

        if let Some(ctx) = run_context {
            let _ = orch.services.emitter.emit(
                "bash-complete",
                serde_json::to_value(BashCompletePayload {
                    run_id: ctx.run_id.clone(),
                    tool_use_id: tool_use_id.clone(),
                    exit_code: None,
                    timed_out: true,
                })
                .unwrap_or_default(),
            );
        }

        let stdout_str = stdout_content.lock().unwrap().clone();
        let stderr_str = stderr_content.lock().unwrap().clone();
        return Ok(ExecOutput {
            stdout: stdout_str,
            stderr: stderr_str,
            exit_code: None,
            timed_out: true,
            denial,
            promoted,
        });
    }

    // Normal completion: the process exited on its own.
    guard.disarm();
    if let Some(ctx) = run_context {
        orch.pty_state
            .unregister_inline_command(&ctx.run_id, &inline_command_id);
    }

    // Bound the reader reaping even on clean exit (cheap once the pipes hit EOF;
    // protects against a lingering escapee that kept a pipe write end open).
    reap_readers_bounded(stdout_handle, stderr_handle).await;

    if let Some(ctx) = run_context {
        let _ = orch.services.emitter.emit(
            "bash-complete",
            serde_json::to_value(BashCompletePayload {
                run_id: ctx.run_id.clone(),
                tool_use_id: tool_use_id.clone(),
                exit_code,
                timed_out: false,
            })
            .unwrap_or_default(),
        );
    }

    let stdout_str = stdout_content.lock().unwrap().clone();
    let stderr_str = stderr_content.lock().unwrap().clone();

    // Detect a kernel sandbox denial so the caller can drive the worktree fence.
    let denial = if sandboxed {
        let combined = match (stdout_str.is_empty(), stderr_str.is_empty()) {
            (false, false) => format!("{stdout_str}\n{stderr_str}"),
            (false, true) => stdout_str.clone(),
            (true, false) => stderr_str.clone(),
            (true, true) => String::new(),
        };
        sandbox::detect_denial(
            exit_code,
            &combined,
            child_pid,
            spawn_started,
            command_scoped_fallback,
        )
    } else {
        None
    };

    Ok(ExecOutput {
        stdout: stdout_str,
        stderr: stderr_str,
        exit_code,
        timed_out: false,
        denial,
        promoted: None,
    })
}

/// Append a captured line (plus newline) to the optional combined byte buffer,
/// keeping it bounded. Both reader threads call this so a promoted terminal's
/// buffer reflects interleaved stdout/stderr.
fn record_combined_output(
    combined: &Option<Arc<Mutex<VecDeque<u8>>>>,
    last_output_at: &Option<Arc<Mutex<SystemTime>>>,
    line: &str,
) {
    if let Some(cb) = combined {
        if let Ok(mut b) = cb.lock() {
            b.extend(line.as_bytes());
            b.push_back(b'\n');
            while b.len() > MAX_BUFFER_SIZE {
                b.pop_front();
            }
        }
    }
    if let Some(ts) = last_output_at {
        if let Ok(mut t) = ts.lock() {
            *t = SystemTime::now();
        }
    }
}

/// Join the stdout/stderr reader threads with a hard 2s ceiling, off the tokio
/// runtime. A `setsid` escapee that holds a pipe write end open keeps a reader
/// blocked on EOF forever; we abandon the threads after the grace period and
/// return whatever the shared capture buffers already hold rather than hang the
/// tool call.
async fn reap_readers_bounded(
    stdout_handle: thread::JoinHandle<()>,
    stderr_handle: thread::JoinHandle<()>,
) {
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::task::spawn_blocking(move || {
            let _ = stdout_handle.join();
            let _ = stderr_handle.join();
        }),
    )
    .await;
}

/// Promote a still-running, timed-out inline command to a durable terminal.
///
/// The inline child becomes the terminal's `TerminalChild`; the combined capture
/// buffer (already seeded and still fed by the reader threads) becomes the
/// terminal's output buffer. Inline-command bookkeeping is handed off here; the
/// caller must disarm the kill-on-drop guard. A background watcher thread
/// finalizes the terminal when the process eventually exits.
#[allow(clippy::too_many_arguments)]
async fn promote_to_terminal(
    orch: &Orchestrator,
    ctx: &super::RunContext,
    cwd: &str,
    command: &str,
    child: Arc<Mutex<Box<dyn crate::services::ChildProcess>>>,
    output_buffer: Arc<Mutex<VecDeque<u8>>>,
    last_output_at: Option<Arc<Mutex<SystemTime>>>,
    inline_command_id: &str,
) -> Result<PromotedTerminal, String> {
    let mut target = TerminalResourceTarget {
        slug: String::new(),
        job_id: Some(ctx.job_id.clone()),
        project_id: ctx.project_id.clone(),
        run_id: Some(ctx.run_id.clone()),
        cwd: cwd.to_string(),
    };

    // Auto slug: run-1, run-2, ... until one is free in this job's scope.
    let mut slug = String::new();
    for n in 1..=10_000 {
        let candidate = format!("run-{n}");
        target.slug = candidate.clone();
        // Any-state check: an exited run slug stays taken so run numbers remain
        // monotonic across promotions within a job.
        if !terminal_slug_taken_any(&orch.db.local, &target)
            .await
            .unwrap_or(true)
        {
            slug = candidate;
            break;
        }
    }
    if slug.is_empty() {
        return Err("could not allocate a terminal slug".to_string());
    }

    let session_id = Uuid::new_v4().to_string();
    insert_terminal_resource(&orch.db.local, &target, &session_id, command, None)
        .await
        .map_err(|e| format!("Database error: {e}"))?;

    // A pipe-backed session: no PTY master/writer (input/resize unavailable),
    // while exit tracking and buffer reads work the same as any terminal.
    let session = PtySession {
        master: None,
        writer: None,
        child: Box::new(crate::services::InlineTerminalChild::new(child.clone())),
        output_buffer: Some(output_buffer.clone()),
        is_agent_spawned: true,
        last_output_at,
        // Agent/promoted sessions are non-interactive: no prompt markers, no busy signal.
        command_state: None,
        // Promoted runs have no chunked read loop to scan; output phrase wakes
        // are an agent-PTY-terminal feature only.
        output_watchers: None,
    };
    {
        let mut sessions = orch
            .pty_state
            .sessions
            .lock()
            .map_err(|e| format!("Failed to store session: {e}"))?;
        sessions.insert(session_id.clone(), Arc::new(Mutex::new(session)));
    }

    // Ownership of the process has moved to the terminal; stop the inline
    // interrupt bookkeeping for it.
    orch.pty_state
        .unregister_inline_command(&ctx.run_id, inline_command_id);

    // Round-trips for a top-level node; a sub-task job still gets a readable,
    // killable terminal via the UI and job teardown.
    let uri = build_promoted_terminal_uri(orch, ctx, &slug).await;

    // Promoted-process exit watcher: block on the inline child, then run the same
    // finalization a PTY terminal does on EOF. Plain OS thread, so the blocking
    // wait and `block_on_background_db` inside finalize are safe.
    {
        let orch_t = orch.clone();
        let job_id = ctx.job_id.clone();
        let command_t = command.to_string();
        let buffer_t = output_buffer.clone();
        let watch_child = child.clone();
        let sid = session_id.clone();
        let process_ref = slug.clone();
        let detail_uri = uri.clone();
        thread::spawn(move || {
            let mut tc = crate::services::InlineTerminalChild::new(watch_child);
            let _ = tc.wait();
            let exit_code = tc.try_wait_exit();
            finalize_terminal_session(
                &orch_t,
                &sid,
                exit_code,
                &buffer_t,
                &command_t,
                Some(job_id),
                &process_ref,
                &detail_uri,
            );
        });
    }

    // Surface the new terminal to the UI.
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_terminals", "action": "update"}),
    );
    let _ = orch.services.emitter.emit(
        "agent-terminal-created",
        serde_json::to_value(AgentTerminalCreatedPayload {
            run_id: ctx.run_id.clone(),
            session_id,
            command: command.to_string(),
            description: None,
        })
        .unwrap_or_default(),
    );

    Ok(PromotedTerminal { slug, uri })
}

/// Build the canonical readable/killable URI for a promoted terminal.
async fn build_promoted_terminal_uri(
    orch: &Orchestrator,
    ctx: &super::RunContext,
    slug: &str,
) -> String {
    if let (Some(num), Some(seq)) = (ctx.issue_number, ctx.exec_seq) {
        if let Some(parent_segment) =
            crate::jobs::queries::parent_uri_segment_for_job(&orch.db.local, &ctx.job_id).await
        {
            if let Some(task_segment) =
                crate::jobs::queries::task_uri_segment_for_job(&orch.db.local, &ctx.job_id).await
            {
                return cairn_common::uri::build_task_terminal_uri(
                    &ctx.project_key,
                    num,
                    seq,
                    &parent_segment,
                    &task_segment,
                    slug,
                );
            }
        }
        if let Some(segment) =
            crate::jobs::queries::node_uri_segment_for_job(&orch.db.local, &ctx.job_id).await
        {
            return cairn_common::uri::build_node_terminal_uri(
                &ctx.project_key,
                num,
                seq,
                &segment,
                slug,
            );
        }
    }
    cairn_common::uri::build_project_terminal_uri(&ctx.project_key, slug)
}

/// Remove a dead terminal session from the live PTY map without killing (the
/// child has already exited). Idempotent: removing an absent session no-ops.
fn remove_pty_session(pty_state: &crate::services::PtyState, session_id: &str) {
    if let Ok(mut sessions) = pty_state.sessions.lock() {
        sessions.remove(session_id);
    }
}

/// The single terminal end-of-life sink. Attempts the conditional mark-exited
/// first; on a real `running`→`exited` transition it emits `pty-exit` +
/// `db-change`, routes the rich `terminal_exit` wake, runs nonzero-exit
/// injection, and finally drops the dead session from the live map. When no row
/// transitioned (already exited, or gone) it is a pure no-op beyond dropping the
/// session — no second `pty-exit`, wake, or injection. This makes finalize
/// idempotent across every caller (reader EOF, promoted watcher, external kill,
/// fence-deny, reader-error) at the DB boundary.
#[allow(clippy::too_many_arguments)]
fn finalize_terminal_session(
    orch: &Orchestrator,
    session_id: &str,
    exit_code: Option<i32>,
    buffer: &Arc<Mutex<VecDeque<u8>>>,
    command: &str,
    job_id: Option<String>,
    process_ref: &str,
    detail_uri: &str,
) {
    let output_tail = capture_output_tail(buffer);

    let context = match block_on_background_db(mark_terminal_exited_and_load_context(
        orch.db.local.clone(),
        session_id.to_string(),
        job_id,
        exit_code,
        output_tail.clone(),
    )) {
        Ok(Some(context)) => context,
        // No running row matched: another path already finalized (or the row is
        // gone). Stay idempotent — drop the dead session and return without a
        // second pty-exit/wake/injection.
        Ok(None) => {
            remove_pty_session(&orch.pty_state, session_id);
            return;
        }
        Err(error) => {
            log::warn!("failed to mark terminal exited for {process_ref}: {error}");
            remove_pty_session(&orch.pty_state, session_id);
            return;
        }
    };

    let _ = orch.services.emitter.emit(
        "pty-exit",
        serde_json::to_value(PtyExitPayload {
            session_id: session_id.to_string(),
            exit_code,
        })
        .unwrap_or_default(),
    );

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_terminals", "action": "update"}),
    );

    let runtime_secs = context
        .created_at
        .map(|created| (chrono::Utc::now().timestamp() - created).max(0));

    if let Err(error) = crate::orchestrator::wakes::route_terminal_exit(
        orch,
        process_ref,
        detail_uri,
        exit_code,
        runtime_secs,
        output_tail.as_deref(),
    ) {
        log::warn!("failed to route terminal-exit wake for {process_ref}: {error}");
    }

    if let Some((injection_session_id, code)) = context.injection {
        send_terminal_exit_context(
            orch,
            &injection_session_id,
            session_id,
            code,
            buffer,
            command,
        );
    }

    // The process is dead and finalize has captured everything it needs from the
    // session (buffer/exit were passed in). Drop it from the live map.
    remove_pty_session(&orch.pty_state, session_id);
}

/// Exit code recorded for a terminal we intentionally killed: the SIGKILL
/// convention (128 + 9). A killed-mid-run command must never read as success.
const KILLED_EXIT_CODE: i32 = 137;

/// The fields finalize-by-session needs from a `job_terminals` row.
struct TerminalFinalizeRow {
    status: String,
    job_id: Option<String>,
    project_id: Option<String>,
    slug: Option<String>,
    command: String,
}

async fn load_terminal_finalize_row(
    db: &LocalDb,
    session_id: &str,
) -> DbResult<Option<TerminalFinalizeRow>> {
    let session_id = session_id.to_string();
    db.query_opt(
        "SELECT status, job_id, project_id, slug, command
         FROM job_terminals WHERE session_id = ?1 LIMIT 1",
        params![session_id.as_str()],
        |row| {
            Ok(TerminalFinalizeRow {
                status: row.text(0)?,
                job_id: row.opt_text(1)?,
                project_id: row.opt_text(2)?,
                slug: row.opt_text(3)?,
                command: row.text(4)?,
            })
        },
    )
    .await
}

/// Reconstruct a terminal's canonical detail URI from its row. Mirrors
/// `build_promoted_terminal_uri` and the subscribe path (`dispatch.rs`) so the
/// `terminal_exit` wake's match key matches what a subscriber stored, without a
/// stored-URI column: job-owned with a resolvable issue/exec/node → node URI;
/// otherwise the project URI.
async fn build_terminal_detail_uri(
    orch: &Orchestrator,
    job_id: Option<&str>,
    project_id: Option<&str>,
    slug: &str,
) -> String {
    if let Some(job_id) = job_id {
        if let Some((project_key, number, exec_seq)) =
            load_job_uri_parts(&orch.db.local, job_id).await
        {
            if let Some(parent_segment) =
                crate::jobs::queries::parent_uri_segment_for_job(&orch.db.local, job_id).await
            {
                if let Some(task_segment) =
                    crate::jobs::queries::task_uri_segment_for_job(&orch.db.local, job_id).await
                {
                    return cairn_common::uri::build_task_terminal_uri(
                        &project_key,
                        number,
                        exec_seq,
                        &parent_segment,
                        &task_segment,
                        slug,
                    );
                }
            }
            if let Some(segment) =
                crate::jobs::queries::node_uri_segment_for_job(&orch.db.local, job_id).await
            {
                return cairn_common::uri::build_node_terminal_uri(
                    &project_key,
                    number,
                    exec_seq,
                    &segment,
                    slug,
                );
            }
            return cairn_common::uri::build_project_terminal_uri(&project_key, slug);
        }
    }
    if let Some(project_id) = project_id {
        if let Some(project_key) = load_project_key(&orch.db.local, project_id).await {
            return cairn_common::uri::build_project_terminal_uri(&project_key, slug);
        }
    }
    cairn_common::uri::build_project_terminal_uri(project_id.unwrap_or_default(), slug)
}

async fn load_job_uri_parts(db: &LocalDb, job_id: &str) -> Option<(String, i32, i32)> {
    let job_id = job_id.to_string();
    db.query_opt(
        "SELECT p.key, i.number, e.seq
         FROM jobs j
         JOIN issues i ON i.id = j.issue_id
         JOIN projects p ON p.id = i.project_id
         LEFT JOIN executions e ON e.id = j.execution_id
         WHERE j.id = ?1 LIMIT 1",
        params![job_id.as_str()],
        |row| Ok((row.text(0)?, row.i64(1)? as i32, row.opt_i64(2)?)),
    )
    .await
    .ok()
    .flatten()
    .and_then(|(key, number, seq)| seq.map(|seq| (key, number, seq as i32)))
}

async fn load_project_key(db: &LocalDb, project_id: &str) -> Option<String> {
    db.query_opt_text(
        "SELECT key FROM projects WHERE id = ?1 LIMIT 1",
        params![project_id.to_string()],
    )
    .await
    .ok()
    .flatten()
}

/// Take a live session out of the PTY map, capture its buffer, kill the child,
/// and derive an honest non-success exit code. `try_wait_exit` can report `0`
/// (or `None`) for a signalled child — portable_pty drops the signal and std's
/// `ExitStatus::code()` is `None` on signal death — so any zero/unknown is
/// normalized to the SIGKILL convention. Neither the row's `exit_code` nor the
/// wake message can then read as success for a killed terminal.
fn take_and_kill_session(
    pty_state: &crate::services::PtyState,
    session_id: &str,
) -> (Arc<Mutex<VecDeque<u8>>>, Option<i32>) {
    let session_arc = pty_state
        .sessions
        .lock()
        .ok()
        .and_then(|mut sessions| sessions.remove(session_id));
    let empty = || Arc::new(Mutex::new(VecDeque::new()));
    match session_arc {
        Some(arc) => match arc.lock() {
            Ok(mut session) => {
                let buffer = session.output_buffer.clone().unwrap_or_else(empty);
                let _ = session.child.kill();
                let _ = session.child.wait();
                let exit_code = session
                    .child
                    .try_wait_exit()
                    .filter(|code| *code != 0)
                    .or(Some(KILLED_EXIT_CODE));
                (buffer, exit_code)
            }
            Err(_) => (empty(), Some(KILLED_EXIT_CODE)),
        },
        None => (empty(), Some(KILLED_EXIT_CODE)),
    }
}

/// Converge an externally-triggered terminal end-of-life (UI tab close, resource
/// "stop", session hard kill) on the single finalize sink: kill the live child,
/// mark the row exited with an honest non-success code, route the exit wake, and
/// retain the row. No-ops when the row is missing or already exited — the reader
/// EOF that follows the kill hits the conditional UPDATE (0 rows) and no-ops too.
/// Deletion is reserved for job teardown.
///
/// The work runs on a dedicated plain thread because finalize blocks on a fresh
/// current-thread runtime, which panics on any thread that carries a tokio
/// context (a `spawn_blocking` worker or a runtime thread). Callers come from
/// both sync and async contexts, so this isolates uniformly. Async callers
/// should still wrap the call in `spawn_blocking` so the join does not stall an
/// executor worker.
pub fn finalize_terminal_by_session_id(
    orch: &Orchestrator,
    session_id: &str,
) -> Result<(), String> {
    let orch = orch.clone();
    let session_id = session_id.to_string();
    std::thread::spawn(move || finalize_terminal_by_session_id_inner(&orch, &session_id))
        .join()
        .map_err(|_| "terminal finalize thread panicked".to_string())?
}

fn finalize_terminal_by_session_id_inner(
    orch: &Orchestrator,
    session_id: &str,
) -> Result<(), String> {
    let loaded = block_on_background_db(async {
        let Some(row) = load_terminal_finalize_row(&orch.db.local, session_id).await? else {
            return Ok(None);
        };
        if row.status != "running" {
            return Ok(None);
        }
        let slug = row.slug.clone().unwrap_or_default();
        let detail_uri = build_terminal_detail_uri(
            orch,
            row.job_id.as_deref(),
            row.project_id.as_deref(),
            &slug,
        )
        .await;
        Ok(Some((row, detail_uri)))
    })
    .map_err(|e: crate::storage::DbError| e.to_string())?;

    let Some((row, detail_uri)) = loaded else {
        return Ok(());
    };

    let (buffer, exit_code) = take_and_kill_session(&orch.pty_state, session_id);
    let process_ref = row.slug.as_deref().unwrap_or("terminal");
    finalize_terminal_session(
        orch,
        session_id,
        exit_code,
        &buffer,
        &row.command,
        row.job_id.clone(),
        process_ref,
        &detail_uri,
    );
    Ok(())
}

/// Build the OS sandbox policy for a run, or `None` when no confinement applies.
///
/// Two regimes:
/// - **Worktree cwd**: gated on the fence — `allow` (or no run context) runs
///   unconfined; `ask`/`deny` confine writes to the worktree.
/// - **Non-worktree cwd** (the project's live checkout — project chat / manager /
///   triage): a read-only-checkout policy applies STRUCTURALLY, regardless of
///   fence, so a stray write into the live checkout is kernel-denied while the
///   checkout stays readable.
///
/// The unconfined escape hatches — an already-granted command and an accepted
/// dev-command carveout with no declared scopes — are WORKTREE-ONLY: a
/// non-worktree run never returns `None` for them, because the live checkout is
/// read-only and non-grantable. The only `None` for a non-worktree cwd is a host
/// with no sandbox primitive, where the restored dirt-detection warning covers
/// the gap. Carveout write-globs still apply on a non-worktree cwd, but any glob
/// that would write the checkout itself is dropped.
///
/// Whether a writable carveout scope `glob` would permit a write into `checkout`
/// (the project's live checkout). Used to refuse a dev-command scope that would
/// defeat the read-only-checkout guarantee on a non-worktree run.
///
/// A scope can only ever write at or below its **non-wildcard literal prefix**
/// (everything from the first `*` on merely widens the match within that prefix).
/// So the scope re-opens checkout writes iff that prefix sits inside the checkout
/// — including a nested subtree like `{checkout}/target/**`, which a root/child
/// regex probe would miss — or is an ancestor whose subpath grant would include
/// the checkout. Concrete (wildcard-free) scopes fall out of the same check with
/// the whole scope as the prefix. Fail-closed: an empty prefix (a leading-`*`
/// glob) counts as covering.
fn glob_covers_checkout(glob: &str, checkout: &std::path::Path) -> bool {
    let checkout = checkout
        .canonicalize()
        .unwrap_or_else(|_| checkout.to_path_buf());
    let literal_prefix = match glob.find('*') {
        Some(i) => &glob[..i],
        None => glob,
    };
    let prefix = std::path::Path::new(literal_prefix);
    let prefix = prefix
        .canonicalize()
        .unwrap_or_else(|_| prefix.to_path_buf());
    prefix.starts_with(&checkout) || checkout.starts_with(&prefix)
}

/// Build the OS sandbox policy for a run, or `None` when no confinement applies.
async fn build_run_sandbox_policy(
    orch: &Orchestrator,
    cwd: &str,
    run_id: Option<&str>,
    project_id: Option<&str>,
    command_for_grant: Option<&str>,
) -> Option<(sandbox::SandboxPolicy, Fence)> {
    use crate::mcp::handlers::permission::resolve_fence_policy;

    // The project's live checkout (a non-jj cwd: project chat / manager / triage)
    // is read-only for agents, non-negotiable, so the read-only-checkout sandbox
    // applies STRUCTURALLY regardless of fence — project chat carries no
    // execution snapshot and would otherwise be fully unconfined. A real worktree
    // keeps the fence gate (ask/deny confine; allow runs free).
    let non_worktree = !crate::jj::is_jj_dir(std::path::Path::new(cwd));
    let fence = if non_worktree {
        // Read-only and non-grantable: `Deny` makes run_one never route a denial
        // through the fence (no prompt, no session grant). Detection of the block
        // itself is enabled separately in execute_process so the agent still gets
        // the clear read-only-checkout message.
        Fence::Deny
    } else {
        let fence = resolve_fence_policy(orch, run_id).await?;
        if !sandbox::sandbox_applies(fence) {
            return None;
        }
        fence
    };

    if !sandbox::is_available() {
        log::warn!("OS sandbox unavailable on this host; running command unconfined (cwd={cwd})");
        return None;
    }

    let granted: Vec<String> = orch
        .session_allowed_crossings
        .lock()
        .ok()
        .map(|s| s.iter().cloned().collect())
        .unwrap_or_default();

    // A command-scoped session grant escalates: skip the sandbox so the approved
    // command (shell command, or a skill script's program) runs with full reach
    // without re-tripping the fence. Keyed identically to the crossing descriptor
    // raised in `run_one`. WORKTREE-ONLY: the project's live checkout is read-only
    // and non-negotiable, so a command grant (earned in some worktree run, since
    // the session set is shared) must never re-open the checkout to writes.
    if !non_worktree {
        if let Some(cmd) = command_for_grant {
            if granted.contains(&normalize_command(cmd)) {
                return None;
            }
        }
    }

    let deny_read = orch.sandbox_deny_read();

    // Durable dev-command fence carveouts: a project terminal command the user
    // has accepted as a fence-crosser pre-ordains the crossings it needs (e.g.
    // Cairn's `bun run dev:instance` writing its per-instance home outside the
    // worktree), so an approved launch does not park on a fence prompt. The
    // declaration lives in repo config but only takes effect once the user has
    // accepted that command (stored per project in workspace settings), so a
    // cloned repo can declare a fence-crosser but cannot grant itself the
    // crossing. See `crate::config::dev_commands` and `docs/worktree-fence.md`.
    let carveouts = match (command_for_grant, project_id) {
        (Some(cmd), Some(pid)) => {
            let accepted = crate::config::settings::load_accepted_fence_commands(&orch.config_dir)
                .remove(pid)
                .unwrap_or_default();
            if accepted.is_empty() {
                Default::default()
            } else {
                let project_terminals = crate::config::project_settings::load_terminal_commands(
                    std::path::Path::new(cwd),
                );
                let templates = crate::config::settings::build_service_templates(
                    &orch.config_dir,
                    Some(std::path::PathBuf::from(cwd)),
                );
                crate::config::dev_commands::resolve_carveouts(
                    cmd,
                    &project_terminals,
                    &accepted,
                    &deny_read,
                    &templates,
                )
            }
        }
        _ => Default::default(),
    };

    // An accepted command with no declared scopes crosses fully: skip the sandbox
    // like a command-scoped session grant does. WORKTREE-ONLY for the same reason
    // — on the live checkout an unconfined carveout still resolves to a read-only
    // checkout policy; nothing re-opens the checkout to writes.
    if carveouts.unconfined && !non_worktree {
        log::info!("dev-command carveout: running accepted command unconfined (cwd={cwd})");
        return None;
    }
    if !carveouts.dropped_sensitive.is_empty() {
        log::warn!(
            "dev-command carveout dropped {} declared scope(s) that touch the read denylist \
             (a repo-committed terminal command cannot grant a secret-store crossing): {:?}",
            carveouts.dropped_sensitive.len(),
            carveouts.dropped_sensitive,
        );
    }

    // Non-worktree cwd: the live checkout is read-only (dropped from the writable
    // set) but readable; a worktree cwd keeps the worktree writable. Session path
    // grants flow into either policy, but `for_readonly_checkout` drops any grant
    // that lies within (or contains) the checkout, so a grant can never re-open it.
    let checkout = std::path::Path::new(cwd);
    let mut policy = if non_worktree {
        sandbox::SandboxPolicy::for_readonly_checkout(checkout, &granted, deny_read)
    } else {
        sandbox::SandboxPolicy::for_run(checkout, &granted, deny_read)
    };
    // Writable carveout scopes are globs: realize them as macOS SBPL regex grants
    // (matching the build-service mechanism), and additionally as a concrete
    // writable subpath for any wildcard-free scope so the Linux landlock path
    // (which does not translate regex grants) still honors simple carveouts. On a
    // non-worktree cwd, refuse any scope that would write the read-only live
    // checkout — the read-only guarantee outranks a dev-command's declared scope.
    for glob in &carveouts.write_globs {
        if non_worktree && glob_covers_checkout(glob, checkout) {
            log::warn!(
                "dev-command carveout scope {glob:?} would write the read-only live checkout; \
                 dropping it (cwd={cwd})"
            );
            continue;
        }
        policy.writable_regex.push(sandbox::glob_to_regex(glob));
        if !glob.contains('*') {
            policy.writable_extra.push(std::path::PathBuf::from(glob));
        }
    }

    Some((policy, fence))
}

#[derive(Clone, Copy)]
enum TerminalSpawnMode {
    Create,
    Respawn,
}

pub async fn create_terminal_from_resource(
    orch: &Orchestrator,
    resource: &CairnResource,
    command: &str,
    description: Option<&str>,
) -> Result<String, String> {
    let target = resolve_terminal_resource_target(&orch.db.local, resource)
        .await
        .map_err(|e| e.to_string())?;
    ensure_terminal_slug_available(&orch.db.local, &target)
        .await
        .map_err(|e| e.to_string())?;
    spawn_terminal_session(
        orch,
        resource.clone(),
        target,
        command.to_string(),
        description.map(ToOwned::to_owned),
        TerminalSpawnMode::Create,
    )
    .await?;
    Ok(format!("Started terminal {}", resource_slug(resource)))
}

fn resource_slug(resource: &CairnResource) -> String {
    match resource {
        CairnResource::NodeTerminal { slug, .. }
        | CairnResource::ProjectTerminal { slug, .. }
        | CairnResource::TaskTerminal { slug, .. } => slug.clone(),
        _ => "terminal".to_string(),
    }
}

#[allow(clippy::too_many_lines)]
async fn spawn_terminal_session(
    orch: &Orchestrator,
    resource: CairnResource,
    target: TerminalResourceTarget,
    command: String,
    description: Option<String>,
    mode: TerminalSpawnMode,
) -> Result<String, String> {
    let services = &orch.services;
    let pty_state = &orch.pty_state;

    let pair = services
        .pty_factory
        .create_pty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .map_err(|e| format!("Failed to create PTY: {e}"))?;

    let shell_path = get_default_shell();

    // Sandbox the interactive shell for worktree agents. On macOS the shell runs
    // under `sandbox-exec`; on Linux Landlock needs a `pre_exec` hook and the PTY
    // spawning backend does not expose one, so terminal async fence detection is
    // macOS-only until PTY spawning can apply Landlock before exec.
    let sandbox = build_run_sandbox_policy(
        orch,
        &target.cwd,
        target.run_id.as_deref(),
        Some(target.project_id.as_str()),
        Some(&command),
    )
    .await;
    let fence_mode = sandbox.as_ref().map(|(_, fence)| *fence);
    let sandbox_applied = sandbox.is_some() && cfg!(target_os = "macos");
    let sandbox_policy = sandbox.map(|(policy, _)| policy);
    let (shell_program, shell_args): (String, Vec<String>) = match &sandbox_policy {
        Some(policy) if cfg!(target_os = "macos") => sandbox::wrap_argv(&shell_path, &[], policy),
        Some(_) => {
            log::warn!(
                "PTY terminal not OS-sandboxed on this platform (cwd={}); inline run is confined",
                target.cwd
            );
            (shell_path.clone(), Vec::new())
        }
        None => (shell_path.clone(), Vec::new()),
    };

    let spawn_started = SystemTime::now();
    let mut cmd = CommandBuilder::new(&shell_program);
    for arg in &shell_args {
        cmd.arg(arg);
    }
    cmd.cwd(&target.cwd);
    for (key, value) in std::env::vars() {
        cmd.env(key, value);
    }
    cmd.env("PATH", crate::env::get_user_path());
    apply_non_interactive_pager_env_to_pty(&mut cmd);
    cmd.env("CAIRN_WORKTREE", &target.cwd);
    cmd.env(
        "CAIRN_CALLBACK_URL",
        format!("http://127.0.0.1:{}/api/mcp", orch.mcp_callback_port),
    );
    if let Ok(secret) = orch.mcp_auth.get_secret_for_mcp() {
        cmd.env("CAIRN_MCP_SECRET", secret);
    }
    if let Some(run_id) = target.run_id.as_deref() {
        cmd.env("CAIRN_RUN_ID", run_id);
    }
    // Managed Build Services: the interactive PTY shell (e.g. the Dev terminal
    // running `bun run build:mcp`) is fenced on macOS, so inject the build-
    // service client env + CAIRN_SANDBOXED here too. The PTY path uses
    // CommandBuilder, not the process spawner, so CAIRN_SANDBOXED is set
    // explicitly rather than by `build_command`.
    if sandbox_applied {
        cmd.env("CAIRN_SANDBOXED", "1");
        for (k, v) in orch.build_service_client_env(Some(std::path::Path::new(&target.cwd))) {
            cmd.env(k, v);
        }
    }

    let components = pair
        .spawn_and_split(cmd)
        .map_err(|e| format!("Failed to spawn shell: {e}"))?;
    let mut reader = components.reader;
    let child_pid = components.child.process_id();
    let session_id = Uuid::new_v4().to_string();
    let output_buffer: Arc<Mutex<VecDeque<u8>>> = Arc::new(Mutex::new(VecDeque::new()));
    let last_output_at: Arc<Mutex<SystemTime>> = Arc::new(Mutex::new(SystemTime::now()));
    // Shared phrase-watcher registry: the read loop below scans output against it,
    // and the wake-subscribe path registers watchers into the same `Arc` while the
    // terminal runs.
    let output_watchers: Arc<Mutex<Vec<crate::services::TerminalOutputWatcher>>> =
        Arc::new(Mutex::new(Vec::new()));
    let session = PtySession {
        master: Some(components.master),
        writer: Some(components.writer),
        child: Box::new(crate::services::PortableTerminalChild::new(
            components.child,
        )),
        output_buffer: Some(output_buffer.clone()),
        is_agent_spawned: true,
        last_output_at: Some(last_output_at.clone()),
        // Agent terminals are non-interactive: no prompt markers, no busy signal.
        command_state: None,
        output_watchers: Some(output_watchers.clone()),
    };
    let session_arc = Arc::new(Mutex::new(session));

    {
        let mut sessions = pty_state
            .sessions
            .lock()
            .map_err(|e| format!("Failed to store session: {e}"))?;
        sessions.insert(session_id.clone(), session_arc.clone());
    }

    // Hydrate persisted output-phrase watchers for this terminal so an output
    // wake is durable across sessions: a respawn (e.g. after a worktree-fence
    // approval restarts the terminal) and a subscribe made while no session was
    // live both re-attach their watchers here. The wake_subscriptions row is the
    // source of truth; this in-memory list is only the per-session cache the
    // read loop scans.
    {
        let detail_uri = resource.to_uri();
        match crate::orchestrator::wakes::list_terminal_output_watchers(&orch.db.local, &detail_uri)
            .await
        {
            Ok(persisted) if !persisted.is_empty() => {
                if let Ok(mut guard) = output_watchers.lock() {
                    for (subscription_id, watcher_job_id, phrase, terminal_uri) in persisted {
                        guard.push(crate::services::TerminalOutputWatcher {
                            subscription_id,
                            job_id: watcher_job_id,
                            phrase,
                            carry: String::new(),
                            terminal_uri,
                        });
                    }
                }
            }
            Ok(_) => {}
            Err(error) => {
                log::warn!("failed to hydrate terminal output watchers: {error}")
            }
        }
    }

    let db_result = match mode {
        TerminalSpawnMode::Create => {
            insert_terminal_resource(
                &orch.db.local,
                &target,
                &session_id,
                &command,
                description.as_deref(),
            )
            .await
        }
        TerminalSpawnMode::Respawn => {
            update_terminal_resource_session(&orch.db.local, &target, &session_id).await
        }
    };
    if let Err(e) = db_result {
        if let Ok(mut sessions) = pty_state.sessions.lock() {
            sessions.remove(&session_id);
        }
        if let Ok(mut session) = session_arc.lock() {
            let _ = session.child.kill();
            let _ = session.child.wait();
        }
        return Err(format!("Database error: {e}"));
    }

    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_terminals", "action": "update"}),
    );

    let sid = session_id.clone();
    let orch_t = orch.clone();
    let emitter = services.emitter.clone();
    let buffer = output_buffer.clone();
    let last_output_thread = last_output_at.clone();
    let job_id = target.job_id.clone();
    let command_for_thread = command.clone();
    let target_for_thread = target.clone();
    let resource_for_thread = resource.clone();
    let description_for_thread = description.clone();
    let watchers_for_thread = output_watchers.clone();
    let fence_handled = Arc::new(AtomicBool::new(false));
    let suppress_cleanup = Arc::new(AtomicBool::new(false));
    let fence_handled_thread = fence_handled.clone();
    let suppress_cleanup_thread = suppress_cleanup.clone();

    thread::spawn(move || {
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => {
                    if suppress_cleanup_thread.load(Ordering::SeqCst) {
                        break;
                    }
                    let exit_code = get_exit_code_from_session(&orch_t.pty_state, &sid);
                    let process_ref = resource_slug(&resource_for_thread);
                    let detail_uri = resource_for_thread.to_uri();
                    finalize_terminal_session(
                        &orch_t,
                        &sid,
                        exit_code,
                        &buffer,
                        &command_for_thread,
                        job_id.clone(),
                        &process_ref,
                        &detail_uri,
                    );
                    break;
                }
                Ok(n) => {
                    let data = String::from_utf8_lossy(&buf[..n]).to_string();
                    emit_terminal_data(&*emitter, &sid, &data, &buffer);
                    if let Ok(mut last) = last_output_thread.lock() {
                        *last = SystemTime::now();
                    }
                    crate::orchestrator::wakes::scan_and_route_terminal_output(
                        &orch_t,
                        &watchers_for_thread,
                        &data,
                    );
                    if should_handle_terminal_denial(sandbox_applied, fence_mode, &data)
                        && !fence_handled_thread.swap(true, Ordering::SeqCst)
                    {
                        let crossing =
                            terminal_denial_crossing(child_pid, spawn_started, &command_for_thread);
                        match fence_mode {
                            Some(Fence::Deny) => {
                                emit_terminal_data(
                                    &*emitter,
                                    &sid,
                                    &format!(
                                        "\r\nDenied by agent fence policy (fence: deny): {}\r\n",
                                        crossing.summary
                                    ),
                                    &buffer,
                                );
                                suppress_cleanup_thread.store(true, Ordering::SeqCst);
                                // Kill the denied process, then converge on
                                // finalize so the row is marked exited (denied →
                                // exit code unknown) and retained, not deleted.
                                // The buffer is an independent Arc, so the tail
                                // survives the session removal terminate performs.
                                let process_ref = resource_slug(&resource_for_thread);
                                let detail_uri = resource_for_thread.to_uri();
                                terminate_pty_session(&orch_t, &sid);
                                finalize_terminal_session(
                                    &orch_t,
                                    &sid,
                                    None,
                                    &buffer,
                                    &command_for_thread,
                                    job_id.clone(),
                                    &process_ref,
                                    &detail_uri,
                                );
                                break;
                            }
                            Some(Fence::Ask) => {
                                emit_terminal_data(
                                    &*emitter,
                                    &sid,
                                    "\r\nTerminal is waiting for worktree-fence approval. The command will restart if allowed for the session.\r\n",
                                    &buffer,
                                );
                                suppress_cleanup_thread.store(true, Ordering::SeqCst);
                                let orch_bg = orch_t.clone();
                                let sid_bg = sid.clone();
                                let buffer_bg = buffer.clone();
                                let target_bg = target_for_thread.clone();
                                let resource_bg = resource_for_thread.clone();
                                let command_bg = command_for_thread.clone();
                                let description_bg = description_for_thread.clone();
                                thread::spawn(move || {
                                    handle_terminal_fence_prompt(
                                        orch_bg,
                                        sid_bg,
                                        buffer_bg,
                                        target_bg,
                                        resource_bg,
                                        command_bg,
                                        description_bg,
                                        crossing,
                                    );
                                });
                                terminate_pty_session(&orch_t, &sid);
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    if suppress_cleanup_thread.load(Ordering::SeqCst) {
                        break;
                    }
                    log::error!("Agent PTY read error: {e}");
                    // Converge on finalize (emits pty-exit, marks exited with
                    // exit code unknown, routes the wake, drops the session)
                    // rather than a bare delete.
                    let process_ref = resource_slug(&resource_for_thread);
                    let detail_uri = resource_for_thread.to_uri();
                    finalize_terminal_session(
                        &orch_t,
                        &sid,
                        None,
                        &buffer,
                        &command_for_thread,
                        job_id.clone(),
                        &process_ref,
                        &detail_uri,
                    );
                    break;
                }
            }
        }
    });

    // Wrap so the shell exits with the command: EOF (which drives
    // `finalize_terminal_session`) then coincides with command completion and the
    // shell's exit code equals the command's. Respawn-after-fence-approval routes
    // through this same site, so it stays consistent.
    write_terminal_input_by_session(orch, &session_id, &submit_command_exiting_shell(&command))
        .await?;

    if let Some(run_id) = target.run_id.clone() {
        let _ = services.emitter.emit(
            "agent-terminal-created",
            serde_json::to_value(AgentTerminalCreatedPayload {
                run_id,
                session_id: session_id.clone(),
                command: command.to_string(),
                description,
            })
            .unwrap_or_default(),
        );
    }

    Ok(session_id)
}

fn emit_terminal_data(
    emitter: &dyn crate::services::EventEmitter,
    session_id: &str,
    data: &str,
    buffer: &Arc<Mutex<VecDeque<u8>>>,
) {
    let _ = emitter.emit(
        "pty-data",
        serde_json::to_value(PtyDataPayload {
            session_id: session_id.to_string(),
            data: data.to_string(),
        })
        .unwrap_or_default(),
    );
    if let Ok(mut buf_guard) = buffer.lock() {
        buf_guard.extend(data.as_bytes());
        while buf_guard.len() > MAX_BUFFER_SIZE {
            buf_guard.pop_front();
        }
    }
}

fn should_handle_terminal_denial(sandbox_applied: bool, fence: Option<Fence>, data: &str) -> bool {
    cfg!(target_os = "macos")
        && sandbox_applied
        && matches!(fence, Some(Fence::Ask | Fence::Deny))
        && sandbox::has_denial_signature(data)
}

fn terminal_denial_crossing(
    child_pid: Option<u32>,
    spawned_at: SystemTime,
    command: &str,
) -> crate::mcp::handlers::fence::Crossing {
    use crate::mcp::handlers::fence::Crossing;
    #[cfg(target_os = "macos")]
    if let Some(path) = child_pid.and_then(|pid| sandbox::macos::detect_violation(pid, spawned_at))
    {
        return Crossing::shell_path(path.as_path(), &path.display().to_string());
    }
    let _ = (child_pid, spawned_at);
    Crossing::shell_command(
        format!("terminal command blocked by the worktree sandbox: {command}"),
        command,
    )
}

fn terminate_pty_session(orch: &Orchestrator, session_id: &str) {
    let session_arc = orch
        .pty_state
        .sessions
        .lock()
        .ok()
        .and_then(|mut sessions| sessions.remove(session_id));
    if let Some(session_arc) = session_arc {
        if let Ok(mut session) = session_arc.lock() {
            let _ = session.child.kill();
            let _ = session.child.wait();
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_terminal_fence_prompt(
    orch: Orchestrator,
    old_session_id: String,
    buffer: Arc<Mutex<VecDeque<u8>>>,
    target: TerminalResourceTarget,
    resource: CairnResource,
    command: String,
    description: Option<String>,
    crossing: crate::mcp::handlers::fence::Crossing,
) {
    let Some(run_id) = target.run_id.clone() else {
        return;
    };
    let request_id = match block_on_background_db(async {
        let request = McpCallbackRequest {
            cwd: target.cwd.clone(),
            run_id: Some(run_id.clone()),
            tool: "run".to_string(),
            payload: serde_json::Value::Null,
            tool_use_id: Some(format!("terminal-{}", old_session_id)),
        };
        let tool_input = serde_json::json!({
            "kind": crossing.kind.tag(),
            "verb": crossing.verb,
            "descriptor": crossing.descriptor.clone(),
            "summary": crossing.summary.clone(),
            "request": request,
            "origin": "terminal",
        });
        crate::mcp::handlers::permission::create_background_permission_request(
            &orch,
            &run_id,
            &format!("terminal-{}", old_session_id),
            crossing.verb,
            &tool_input,
        )
        .await
        .map_err(crate::storage::DbError::Row)
    }) {
        Ok(id) => id,
        Err(e) => {
            emit_terminal_data(
                &*orch.services.emitter,
                &old_session_id,
                &format!("\r\nFailed to create worktree-fence permission request: {e}\r\n"),
                &buffer,
            );
            return;
        }
    };

    let mut rx = orch.permission_responses.subscribe();
    let response = match block_on_background_db(async move {
        loop {
            match rx.recv().await {
                Ok((resp_request_id, response_json)) if resp_request_id == request_id => {
                    return Ok(response_json)
                }
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err(crate::storage::DbError::Row(
                        "permission response channel closed".to_string(),
                    ))
                }
            }
        }
    }) {
        Ok(response) => response,
        Err(e) => {
            emit_terminal_data(
                &*orch.services.emitter,
                &old_session_id,
                &format!("\r\nWorktree-fence permission wait failed: {e}\r\n"),
                &buffer,
            );
            return;
        }
    };

    let allowed = serde_json::from_str::<serde_json::Value>(&response)
        .ok()
        .and_then(|v| {
            v.get("behavior")
                .and_then(|b| b.as_str())
                .map(str::to_string)
        })
        .as_deref()
        == Some("allow");

    if !allowed {
        emit_terminal_data(
            &*orch.services.emitter,
            &old_session_id,
            &format!("\r\nDenied by worktree fence: {}\r\n", crossing.summary),
            &buffer,
        );
        // Converge on finalize: mark the row exited (denied → exit code unknown)
        // and route the wake, retaining the row. The session was already removed
        // by the reader thread's terminate before this prompt handler ran, so
        // there is no child to kill here.
        let process_ref = resource_slug(&resource);
        let detail_uri = resource.to_uri();
        finalize_terminal_session(
            &orch,
            &old_session_id,
            None,
            &buffer,
            &command,
            target.job_id.clone(),
            &process_ref,
            &detail_uri,
        );
        return;
    }

    let session_granted = orch
        .session_allowed_crossings
        .lock()
        .ok()
        .is_some_and(|allowed| allowed.contains(&crossing.descriptor));
    if !session_granted {
        emit_terminal_data(
            &*orch.services.emitter,
            &old_session_id,
            "\r\nTerminal worktree-fence approvals must be allowed for the session; not restarting.\r\n",
            &buffer,
        );
        // Converge on finalize: mark the row exited (denied → exit code unknown)
        // and route the wake, retaining the row. The session was already removed
        // by the reader thread's terminate before this prompt handler ran, so
        // there is no child to kill here.
        let process_ref = resource_slug(&resource);
        let detail_uri = resource.to_uri();
        finalize_terminal_session(
            &orch,
            &old_session_id,
            None,
            &buffer,
            &command,
            target.job_id.clone(),
            &process_ref,
            &detail_uri,
        );
        return;
    }

    emit_terminal_data(
        &*orch.services.emitter,
        &old_session_id,
        "\r\nWorktree-fence approval granted for this session; restarting terminal.\r\n",
        &buffer,
    );
    let respawn = block_on_background_db(async {
        spawn_terminal_session(
            &orch,
            resource,
            target,
            command,
            description,
            TerminalSpawnMode::Respawn,
        )
        .await
        .map_err(crate::storage::DbError::Row)
    });
    if let Err(e) = respawn {
        emit_terminal_data(
            &*orch.services.emitter,
            &old_session_id,
            &format!("\r\nFailed to restart terminal after worktree-fence approval: {e}\r\n"),
            &buffer,
        );
    }
}

async fn write_terminal_input_by_session(
    orch: &Orchestrator,
    session_id: &str,
    content: &str,
) -> Result<(), String> {
    let sessions = orch
        .pty_state
        .sessions
        .lock()
        .map_err(|e| format!("Failed to access sessions: {e}"))?;
    let session = sessions
        .get(session_id)
        .ok_or_else(|| format!("Terminal session not running: {session_id}"))?;
    let mut session = session
        .lock()
        .map_err(|e| format!("Failed to lock terminal session: {e}"))?;
    let writer = session
        .writer
        .as_mut()
        .ok_or_else(|| "This terminal does not accept input".to_string())?;
    writer
        .write_all(content.as_bytes())
        .map_err(|e| format!("Failed to write terminal input: {e}"))?;
    writer
        .flush()
        .map_err(|e| format!("Failed to flush terminal input: {e}"))?;
    Ok(())
}

pub async fn append_terminal_input(
    orch: &Orchestrator,
    resource: &CairnResource,
    content: &str,
    submit: bool,
) -> Result<String, String> {
    let target = resolve_terminal_resource_target(&orch.db.local, resource)
        .await
        .map_err(|e| e.to_string())?;
    let session_id = lookup_terminal_session_id_for_target(&orch.db.local, &target)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("Terminal not found: {}", target.slug))?;

    let sessions = orch
        .pty_state
        .sessions
        .lock()
        .map_err(|e| format!("Failed to access sessions: {e}"))?;
    let session = sessions
        .get(&session_id)
        .ok_or_else(|| format!("Terminal session not running: {}", target.slug))?;
    let mut session = session
        .lock()
        .map_err(|e| format!("Failed to lock terminal session: {e}"))?;
    let to_write = if submit {
        ensure_submitted_line(content)
    } else {
        std::borrow::Cow::Borrowed(content)
    };
    let writer = session
        .writer
        .as_mut()
        .ok_or_else(|| "This terminal does not accept input".to_string())?;
    writer
        .write_all(to_write.as_bytes())
        .map_err(|e| format!("Failed to write terminal input: {e}"))?;
    writer
        .flush()
        .map_err(|e| format!("Failed to flush terminal input: {e}"))?;

    Ok(format!(
        "Sent {} chars to terminal {}",
        to_write.len(),
        target.slug
    ))
}

pub async fn delete_terminal_by_resource(
    orch: &Orchestrator,
    resource: &CairnResource,
) -> Result<String, String> {
    let target = resolve_terminal_resource_target(&orch.db.local, resource)
        .await
        .map_err(|e| e.to_string())?;
    let session_id = lookup_terminal_session_id_for_target(&orch.db.local, &target)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("Terminal not found: {}", target.slug))?;

    // "Stop" converges on the single finalize sink: kill the child, mark the row
    // exited (honest non-success code) + route the wake, and retain the row.
    // Deletion is reserved for job teardown. Run on a blocking worker so the
    // finalize thread's join does not stall the async executor.
    let orch_for_finalize = orch.clone();
    let session_for_finalize = session_id.clone();
    tokio::task::spawn_blocking(move || {
        finalize_terminal_by_session_id(&orch_for_finalize, &session_for_finalize)
    })
    .await
    .map_err(|e| format!("terminal finalize task failed: {e}"))??;

    Ok(format!("Stopped terminal {}", target.slug))
}

/// Get exit code from PTY session using non-blocking try_wait.
fn get_exit_code_from_session(
    pty_state: &crate::services::PtyState,
    session_id: &str,
) -> Option<i32> {
    let sessions = pty_state.sessions.lock().ok()?;
    let session_arc = sessions.get(session_id)?;
    let mut session = session_arc.lock().ok()?;

    // Use try_wait to avoid blocking - process should have exited if we got EOF
    session.child.try_wait_exit()
}

/// Send terminal context to Claude via stdin when a background process exits with error.
fn send_terminal_exit_context(
    orch: &Orchestrator,
    session_id: &str,
    _terminal_session_id: &str,
    exit_code: i32,
    buffer: &Arc<Mutex<VecDeque<u8>>>,
    command: &str,
) {
    // Get recent output from buffer
    let output = {
        let buf_guard = buffer.lock().unwrap();
        let bytes: Vec<u8> = buf_guard.iter().copied().collect();
        String::from_utf8_lossy(&bytes).to_string()
    };

    // Truncate for display
    let cmd_display: String = command.chars().take(100).collect();
    let truncated_output: String = output.chars().take(2000).collect();

    let content = format!(
        "[Terminal Update] Background command '{}' exited with code {}:\n```\n{}\n```",
        cmd_display, exit_code, truncated_output
    );

    // Find the process and send via stdin
    let process_state = orch.process_state.clone();

    let run_id = match process_state.find_process_by_session(session_id) {
        Some(rid) => rid,
        None => {
            log::info!(
                "No active process for session {}, skipping terminal context",
                &session_id[..8.min(session_id.len())]
            );
            return;
        }
    };

    match crate::backends::stdin::send_user_message(
        &process_state,
        &run_id,
        &content,
        session_id,
        None,
        None,
    ) {
        Ok(()) => {
            log::info!(
                "Sent terminal context to session {}: command='{}' exit_code={}",
                &session_id[..8.min(session_id.len())],
                cmd_display,
                exit_code
            );
        }
        Err(e) => {
            log::warn!("Failed to send terminal context: {}", e);
        }
    }
}

/// Cache a checkpoint command result if the executed command matches the job's checkpoint command.
/// This enables the checkpoint lookback optimization: when the programmatic checkpoint runs,
/// it can use this cached result instead of re-running the command.
async fn cache_checkpoint_result(
    orch: &Orchestrator,
    job_id: &str,
    command: &str,
    cwd: &str,
    exit_code: Option<i32>,
) {
    // Check if this command matches the job's checkpoint command
    let checkpoint_cmd = match get_job_checkpoint_command(&orch.db.local, job_id).await {
        Some(cmd) => cmd,
        None => return, // No checkpoint command configured for this job
    };

    let normalized_checkpoint = normalize_command(&checkpoint_cmd);
    let normalized_executed = normalize_command(command);

    if normalized_checkpoint != normalized_executed {
        return; // Command doesn't match checkpoint command
    }

    // Capture jj worktree state for cache validity checking.
    let commit_sha = match crate::execution::cache::get_current_head_sha(orch, cwd) {
        Ok(sha) => sha,
        Err(_) => return,
    };
    let is_dirty = crate::execution::cache::is_worktree_dirty(orch, cwd).unwrap_or(true);
    let now = chrono::Utc::now().timestamp() as i32;

    let cache_id = Uuid::new_v4().to_string();
    if let Err(e) = orch
        .db
        .local
        .write(|conn| {
            let cache_id = cache_id.clone();
            let job_id = job_id.to_string();
            let command = command.to_string();
            let normalized_executed = normalized_executed.clone();
            let commit_sha = commit_sha.clone();
            Box::pin(async move {
                conn.execute(
                    "DELETE FROM checkpoint_command_cache WHERE job_id = ?1",
                    (job_id.as_str(),),
                )
                .await?;
                conn.execute(
                    "
                    INSERT INTO checkpoint_command_cache (
                        id, job_id, command, normalized_command, exit_code,
                        commit_sha, is_dirty, ran_at, created_at
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                    ",
                    (
                        cache_id.as_str(),
                        job_id.as_str(),
                        command.as_str(),
                        normalized_executed.as_str(),
                        exit_code.unwrap_or(-1),
                        commit_sha.as_str(),
                        if is_dirty { 1 } else { 0 },
                        now,
                        now,
                    ),
                )
                .await?;
                Ok(())
            })
        })
        .await
    {
        log::warn!("Failed to cache checkpoint result: {}", e);
        return;
    }

    log::info!(
        "Cached checkpoint command result for job {}: exit={}, sha={}, dirty={}",
        &job_id[..8.min(job_id.len())],
        exit_code.unwrap_or(-1),
        &commit_sha[..7.min(commit_sha.len())],
        is_dirty
    );
}

async fn get_job_checkpoint_command(db: &LocalDb, job_id: &str) -> Option<String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "
                    SELECT j.recipe_node_id, e.snapshot
                    FROM jobs j
                    LEFT JOIN executions e ON j.execution_id = e.id
                    WHERE j.id = ?1
                    LIMIT 1
                    ",
                    (job_id.as_str(),),
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let Some(agent_node_id) = row.opt_text(0)? else {
                return Ok(None);
            };
            let Some(snapshot_json) = row.opt_text(1)? else {
                return Ok(None);
            };
            let snapshot: crate::models::ExecutionSnapshot =
                serde_json::from_str(&snapshot_json)
                    .map_err(|e| crate::storage::DbError::Row(e.to_string()))?;

            // Find a standalone checkpoint node docked to this agent (parent_id)
            // and return its command for the CI lookback cache. Embedded
            // checkpoints no longer exist; only standalone command checkpoints.
            let checkpoint_node = snapshot.recipe.nodes.iter().find(|node| {
                node.parent_id.as_deref() == Some(agent_node_id.as_str())
                    && node.node_type.to_string() == "checkpoint"
            });
            Ok(checkpoint_node
                .and_then(|node| node.checkpoint_config.as_ref())
                .and_then(|config| config.command.clone()))
        })
    })
    .await
    .ok()
    .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_covers_checkout_drops_checkout_scopes_keeps_external() {
        // A non-worktree run must drop any carveout scope that could write the
        // read-only live checkout, while keeping scopes safely outside it.
        let checkout = std::path::Path::new("/project/live");
        // Wildcard scopes that reach into the checkout are covered.
        assert!(glob_covers_checkout("/project/live/**", checkout));
        // A broader ancestor wildcard that also matches the checkout.
        assert!(glob_covers_checkout("/project/**", checkout));
        // A nested subtree wildcard INSIDE the checkout (the case a root/child
        // regex probe missed): `target/`, `node_modules/`, etc.
        assert!(glob_covers_checkout("/project/live/target/**", checkout));
        assert!(glob_covers_checkout(
            "/project/live/node_modules/**",
            checkout
        ));
        // Concrete scope inside the checkout.
        assert!(glob_covers_checkout("/project/live/target", checkout));
        // Concrete ancestor whose subpath grant would include the checkout.
        assert!(glob_covers_checkout("/project", checkout));
        // The checkout itself.
        assert!(glob_covers_checkout("/project/live", checkout));
        // Safely-outside scopes are not covered (they stay writable).
        assert!(!glob_covers_checkout("/other/**", checkout));
        assert!(!glob_covers_checkout("/home/u/.cairn-dev/**", checkout));
        assert!(!glob_covers_checkout("/scratch/ok", checkout));
    }

    // ── redact_command tests ───────────────────────────────────────────────

    #[test]
    fn test_redact_bearer_token() {
        assert_eq!(
            redact_command(r#"curl -H "Authorization: Bearer sk-abc123xyz""#),
            r#"curl -H "Authorization: Bearer [REDACTED]""#
        );
    }

    #[test]
    fn test_redact_bearer_case_insensitive() {
        assert_eq!(
            redact_command("curl -H 'bearer eyJhbGciOiJI.long.token'"),
            "curl -H 'bearer [REDACTED]'"
        );
    }

    #[test]
    fn test_redact_api_key_pattern() {
        assert_eq!(
            redact_command("echo sk-proj-abcdefghij123456"),
            "echo [REDACTED]"
        );
        assert_eq!(
            redact_command("echo sk_live_abcdefghij123456"),
            "echo [REDACTED]"
        );
    }

    #[test]
    fn test_redact_export_secret_vars() {
        assert_eq!(
            redact_command("export API_KEY=supersecret123"),
            "export API_KEY=[REDACTED]"
        );
        assert_eq!(
            redact_command("export OPENAI_SECRET=sk-abc123456789"),
            "export OPENAI_SECRET=[REDACTED]"
        );
        assert_eq!(
            redact_command("export AUTH_TOKEN=eyJhbGciOiJI"),
            "export AUTH_TOKEN=[REDACTED]"
        );
        assert_eq!(
            redact_command("export DB_PASSWORD=hunter2"),
            "export DB_PASSWORD=[REDACTED]"
        );
    }

    #[test]
    fn test_redact_password_flag() {
        assert_eq!(
            redact_command("mysql --password=secret123 -u root"),
            "mysql --password=[REDACTED] -u root"
        );
        assert_eq!(
            redact_command("mysql --password secret123 -u root"),
            "mysql --password [REDACTED] -u root"
        );
    }

    #[test]
    fn test_redact_normal_commands_unchanged() {
        let normal = "git status --short";
        assert_eq!(redact_command(normal), normal);

        let normal2 = "ls -la /tmp";
        assert_eq!(redact_command(normal2), normal2);

        let normal3 = "cargo build --release";
        assert_eq!(redact_command(normal3), normal3);
    }

    #[test]
    fn test_redact_multiple_secrets_in_one_command() {
        let cmd = "export API_KEY=secret1 && curl -H 'Bearer sk-abc123456789'";
        let redacted = redact_command(cmd);
        assert!(!redacted.contains("secret1"));
        assert!(!redacted.contains("sk-abc123456789"));
        assert!(redacted.contains("[REDACTED]"));
    }

    #[test]
    fn test_redact_short_sk_not_matched() {
        // Short sk- patterns (less than 8 chars after prefix) should NOT be redacted
        assert_eq!(redact_command("echo sk-short"), "echo sk-short");
    }

    // ── existing tests ─────────────────────────────────────────────────────

    #[test]
    fn non_interactive_pager_env_defaults_to_cat() {
        let config = apply_non_interactive_pager_env(SpawnConfig::new("bash"));

        assert_eq!(config.env.get("GIT_PAGER").map(String::as_str), Some("cat"));
        assert_eq!(config.env.get("PAGER").map(String::as_str), Some("cat"));
    }

    #[test]
    fn non_interactive_pager_env_overrides_inherited_defaults() {
        let config = apply_non_interactive_pager_env(
            SpawnConfig::new("bash")
                .env("GIT_PAGER", "less")
                .env("PAGER", "more"),
        );

        assert_eq!(config.env.get("GIT_PAGER").map(String::as_str), Some("cat"));
        assert_eq!(config.env.get("PAGER").map(String::as_str), Some("cat"));
    }

    #[test]
    fn test_handle_bash_command_unwrap_uses_semantic_inner_command() {
        assert_eq!(
            unwrap_shell_launcher(r#"/bin/zsh -lc "sed -n '120,520p' src/app.tsx""#),
            "sed -n '120,520p' src/app.tsx"
        );
        assert_eq!(
            unwrap_shell_launcher("bash -lc 'git status --short'"),
            "git status --short"
        );
    }

    // ── batch run composition + interpreter resolution ─────────────────────

    fn outcome(header: &str, body: &str, succeeded: bool) -> ItemOutcome {
        ItemOutcome {
            header: header.to_string(),
            body: body.to_string(),
            succeeded,
            suspended: false,
        }
    }

    #[test]
    fn compose_single_returns_body_without_header() {
        let outcomes = vec![outcome("echo hi", "hi", true)];
        assert_eq!(compose_run_output(&outcomes), "hi");
    }

    #[test]
    fn compose_single_empty_body_is_no_output() {
        let outcomes = vec![outcome("true", "", true)];
        assert_eq!(compose_run_output(&outcomes), "(no output)");
    }

    #[test]
    fn compose_multi_labels_items_in_input_order() {
        let outcomes = vec![
            outcome("cmd a", "alpha", true),
            outcome("cmd b", "beta", true),
        ];
        let text = compose_run_output(&outcomes);
        let ia = text.find("=== cmd a ===").unwrap();
        let ialpha = text.find("alpha").unwrap();
        let ib = text.find("=== cmd b ===").unwrap();
        let ibeta = text.find("beta").unwrap();
        assert!(ia < ialpha, "header a precedes its body");
        assert!(ialpha < ib, "item a precedes item b");
        assert!(ib < ibeta, "header b precedes its body");
    }

    #[test]
    fn compose_multi_inlines_empty_body_as_no_output() {
        let outcomes = vec![outcome("a", "", false), outcome("b", "out", true)];
        let text = compose_run_output(&outcomes);
        assert!(text.contains("=== a ===\n(no output)"));
        assert!(text.contains("=== b ===\nout"));
    }

    // Regression for the original repro: a small first segment followed by a huge
    // second segment. Head-biased capping dropped the second entirely; fair
    // budgets must surface both, with the huge one tail-biased.
    #[test]
    fn compose_multi_fair_budget_surfaces_both_segments() {
        let huge = format!("HEADLINE-OF-DIFF\n{}\nTAIL-OF-DIFF", "d".repeat(100_000));
        let outcomes = vec![
            outcome("git diff --stat", "1 file changed", true),
            outcome("git diff", &huge, true),
        ];
        let text = compose_run_output(&outcomes);
        // First (small) item surfaces whole.
        assert!(text.contains("=== git diff --stat ===\n1 file changed"));
        // Second (huge) item surfaces, tail-biased with an elision marker, and its
        // tail (the signal) is retained.
        assert!(text.contains("=== git diff ==="));
        assert!(text.contains("--- elided"));
        assert!(text.contains("TAIL-OF-DIFF"));
        // Nothing is omitted, and the result stays within the cap.
        assert!(!text.contains("items omitted"));
        assert!(text.len() <= MAX_RUN_RESULT_CHARS);
    }

    // A 3-item batch where item 1 floods the output still shows items 2 and 3.
    #[test]
    fn compose_multi_starved_item_does_not_omit_later_items() {
        let flood = "f".repeat(100_000);
        let outcomes = vec![
            outcome("flood", &flood, true),
            outcome("item-two", "SECOND-OUTPUT", true),
            outcome("item-three", "THIRD-OUTPUT", true),
        ];
        let text = compose_run_output(&outcomes);
        assert!(text.contains("=== item-two ===\nSECOND-OUTPUT"));
        assert!(text.contains("=== item-three ===\nTHIRD-OUTPUT"));
        assert!(text.contains("--- elided"));
        assert!(!text.contains("items omitted"));
        assert!(text.len() <= MAX_RUN_RESULT_CHARS);
    }

    // Single-item path is tail-biased: a body that exceeds the cap keeps a head
    // and the byte-exact tail, with an elision marker between.
    #[test]
    fn compose_single_tail_biased_keeps_head_and_tail() {
        let body = format!(
            "HEAD-SENTINEL\n{}\nTAIL-SENTINEL",
            "m".repeat(MAX_RUN_RESULT_CHARS * 2)
        );
        let text = compose_run_output(&[outcome("big", &body, true)]);
        assert!(text.starts_with("HEAD-SENTINEL"));
        assert!(text.ends_with("TAIL-SENTINEL"));
        assert!(text.contains("--- elided"));
        assert!(text.len() <= MAX_RUN_RESULT_CHARS);
    }

    // A trailing detached-terminal note rides in the tail and is never elided.
    #[test]
    fn cap_item_body_preserves_trailing_promoted_note() {
        let note = "Command still running; detached to \
            cairn://p/CAIRN/1632/1/builder/terminal/run-1 — readable and killable there.";
        let body = format!("{}\n\n{}", "p".repeat(50_000), note);
        let capped = cap_item_body_tail_biased(&body, 4_000);
        assert!(capped.len() <= 4_000);
        assert!(capped.ends_with(note), "promoted note must survive the cap");
        assert!(capped.contains("--- elided"));
    }

    // The elision marker reports the actual dropped span.
    #[test]
    fn cap_item_body_elision_counts_are_accurate() {
        let body = (0..1_000)
            .map(|i| format!("line-{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let budget = 2_000;
        let capped = cap_item_body_tail_biased(&body, budget);
        // Parse "--- elided N lines / M chars; ..."
        let marker = capped
            .lines()
            .find(|l| l.starts_with("--- elided"))
            .expect("marker present");
        let chars: usize = marker
            .split("/ ")
            .nth(1)
            .and_then(|s| s.split(' ').next())
            .and_then(|s| s.parse().ok())
            .expect("char count parses");
        // Reconstructed length: head + tail + elided chars == original length.
        let head_tail: usize = capped
            .lines()
            .filter(|l| !l.starts_with("--- elided"))
            .map(|l| l.len() + 1)
            .sum::<usize>()
            .saturating_sub(1);
        // Within a few bytes of the original (newline accounting around the marker).
        let reconstructed = head_tail + chars;
        assert!(
            reconstructed.abs_diff(body.len()) <= 4,
            "reconstructed {reconstructed} vs original {}",
            body.len()
        );
    }

    // The retained tail begins on a whole source line, never a mid-line
    // fragment that would read as corrupted output (e.g. `94548` for `194548`).
    #[test]
    fn cap_item_body_first_tail_line_is_complete() {
        // Variable-width lines: a byte-exact tail start would land mid-line and
        // surface a partial number; the snap-to-line fix must avoid that.
        let lines: Vec<String> = (0..4_000)
            .map(|i| format!("{}-entry", 90_000 + i * 7))
            .collect();
        let body = lines.join("\n");
        let capped = cap_item_body_tail_biased(&body, 2_000);
        let tail = capped
            .split_once("tail kept below ---\n")
            .map(|(_, t)| t)
            .expect("elision marker present");
        let first_tail_line = tail.lines().next().expect("a tail line");
        assert!(
            lines.iter().any(|l| l == first_tail_line),
            "first tail line {first_tail_line:?} is not a complete source line"
        );
    }

    // Multibyte content is sliced on char boundaries (no panic).
    #[test]
    fn cap_item_body_multibyte_boundary_safe() {
        let body = "é".repeat(40_000); // 2 bytes each → 80_000 bytes
        let capped = cap_item_body_tail_biased(&body, 5_000);
        assert!(capped.len() <= 5_000);
        assert!(capped.contains("--- elided"));
        // Round-trips as valid UTF-8 by construction (String), nothing to assert
        // beyond the absence of a panic above.
    }

    // Pathological item counts fall back to whole-item omission, with advice to
    // re-run scoped or use a terminal — never an offset-continuation footer.
    #[test]
    fn compose_multi_omits_only_as_last_resort() {
        let big = "x".repeat(2_000);
        let outcomes: Vec<ItemOutcome> = (0..400)
            .map(|i| outcome(&format!("cmd-{i}"), &big, true))
            .collect();
        let text = compose_run_output(&outcomes);
        assert!(text.contains("items omitted"));
        assert!(text.contains("re-run each scoped"));
        assert!(!text.contains("Call again with offset"));
        assert!(text.len() <= MAX_RUN_RESULT_CHARS);
    }

    // The composed result never exceeds the batch cap, even when every item floods.
    #[test]
    fn compose_multi_never_exceeds_cap() {
        let flood = "z".repeat(60_000);
        let outcomes: Vec<ItemOutcome> = (0..8)
            .map(|i| outcome(&format!("c{i}"), &flood, true))
            .collect();
        let text = compose_run_output(&outcomes);
        assert!(text.len() <= MAX_RUN_RESULT_CHARS);
    }

    // Water-filling: small items keep their full size; the large item absorbs the
    // donated surplus.
    #[test]
    fn water_fill_donates_surplus_to_large_item() {
        let natural = vec![100, 100, 100_000];
        let alloc = water_fill_budgets(&natural, 45_000);
        assert_eq!(alloc[0], 100, "small item keeps full size");
        assert_eq!(alloc[1], 100, "small item keeps full size");
        assert!(
            alloc[2] >= 44_000,
            "large item absorbs the surplus: {}",
            alloc[2]
        );
        assert!(alloc.iter().sum::<usize>() <= 45_000);
    }

    #[test]
    fn resolve_interpreter_prefers_env_shebang() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("s");
        std::fs::write(&p, "#!/usr/bin/env python3\nprint(1)\n").unwrap();
        let (prog, args) = resolve_interpreter(&p).unwrap();
        assert_eq!(prog, "python3");
        assert!(args.is_empty());
    }

    #[test]
    fn resolve_interpreter_uses_absolute_shebang_with_args() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("s");
        std::fs::write(&p, "#!/bin/bash -e\necho hi\n").unwrap();
        let (prog, args) = resolve_interpreter(&p).unwrap();
        assert_eq!(prog, "/bin/bash");
        assert_eq!(args, vec!["-e".to_string()]);
    }

    #[test]
    fn resolve_interpreter_extension_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("script.py");
        std::fs::write(&p, "print(1)\n").unwrap();
        let (prog, _args) = resolve_interpreter(&p).unwrap();
        assert_eq!(prog, "python3");
    }

    #[test]
    fn resolve_interpreter_unknown_extension_errors() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("script.xyz");
        std::fs::write(&p, "data\n").unwrap();
        assert!(resolve_interpreter(&p).is_err());
    }

    #[test]
    fn terminal_denial_detection_requires_macos_sandbox_and_policy() {
        let denied = "bash: /Users/me/probe: Operation not permitted";
        assert_eq!(
            should_handle_terminal_denial(true, Some(Fence::Ask), denied),
            cfg!(target_os = "macos")
        );
        assert_eq!(
            should_handle_terminal_denial(true, Some(Fence::Deny), denied),
            cfg!(target_os = "macos")
        );
        assert!(!should_handle_terminal_denial(
            false,
            Some(Fence::Ask),
            denied
        ));
        assert!(!should_handle_terminal_denial(
            true,
            Some(Fence::Allow),
            denied
        ));
        assert!(!should_handle_terminal_denial(
            true,
            Some(Fence::Ask),
            "ordinary failure"
        ));
    }

    #[test]
    fn terminal_denial_crossing_falls_back_to_command_scope() {
        let crossing = terminal_denial_crossing(None, SystemTime::now(), "echo hi > $HOME/probe");
        assert_eq!(crossing.verb, "run");
        assert!(crossing.summary.contains("terminal command blocked"));
        assert_eq!(
            crossing.descriptor,
            normalize_command("echo hi > $HOME/probe")
        );
    }

    #[test]
    fn run_payload_deserializes_batch_shape() {
        let payload: RunPayload = serde_json::from_value(serde_json::json!({
            "commands": [
                { "command": "echo hi", "description": "greet" },
                { "target": "cairn://skills/ui/scripts/check.sh", "payload": { "args": ["--fast"] } }
            ],
            "sequential": true,
            "commit_msg": "done"
        }))
        .unwrap();
        assert_eq!(payload.commands.len(), 2);
        assert_eq!(payload.sequential, Some(true));
        assert_eq!(payload.commit_msg.as_deref(), Some("done"));
        assert_eq!(payload.commands[0].command.as_deref(), Some("echo hi"));
        assert_eq!(
            payload.commands[1].target.as_deref(),
            Some("cairn://skills/ui/scripts/check.sh")
        );
        assert_eq!(
            payload.commands[1].payload.as_ref().unwrap().args,
            vec!["--fast".to_string()]
        );
    }
}

#[cfg(test)]
mod commit_barrier_tests {
    use super::*;
    use crate::mcp::vcs::{FakeVcs, VcsSnapshot};
    use std::path::Path;

    // The barrier touches the VCS only through the `WorktreeVcs` seam, so a
    // FakeVcs double covers its commit/restore/no-op control flow deterministically
    // and without a VCS binary. The worktree path is never dereferenced.
    fn wt() -> &'static Path {
        Path::new("/tmp/fake-worktree")
    }

    #[test]
    fn commit_msg_commits_dirty_worktree_even_on_partial_failure() {
        // Some(msg) + dirty: a partial-success batch (all_ok=false) still seals
        // its dirt rather than stranding the successful items' changes.
        let vcs = FakeVcs::new().dirty(Ok(true));
        let out = run_commit_barrier(&vcs, wt(), Some("add file"), false, None, None);
        assert_eq!(vcs.seals(), 1, "a dirty worktree must be sealed");
        assert_eq!(vcs.discards(), 0);
        assert!(out.worktree_changed);
        assert!(out.committed, "a real commit must set committed");
        assert!(out.message.contains("Committed"), "got: {}", out.message);
    }

    #[test]
    fn commit_msg_with_clean_worktree_is_noop() {
        let vcs = FakeVcs::new().dirty(Ok(false));
        let out = run_commit_barrier(&vcs, wt(), Some("nothing"), true, None, None);
        assert_eq!(vcs.seals(), 0, "a clean worktree is not sealed");
        assert_eq!(vcs.discards(), 0);
        assert!(!out.worktree_changed);
        assert!(!out.committed, "a clean no-op must not set committed");
        assert!(out.message.is_empty());
    }

    #[test]
    fn commit_failure_restores_worktree_to_head() {
        let vcs = FakeVcs::new()
            .dirty(Ok(true))
            .seal(Err("pre-commit hook failed".to_string()));
        let out = run_commit_barrier(&vcs, wt(), Some("will fail"), true, None, None);
        assert_eq!(vcs.seals(), 1);
        assert_eq!(
            vcs.discards(),
            1,
            "a failed seal restores the worktree to HEAD"
        );
        assert!(!out.committed, "a failed commit must not set committed");
        assert!(
            out.message.contains("Failed to commit"),
            "got: {}",
            out.message
        );
        assert!(
            out.message.contains("restored to HEAD"),
            "got: {}",
            out.message
        );
    }

    #[test]
    fn commit_msg_nothing_to_commit_is_clean_noop() {
        // The seal reports "nothing to commit" when the tree became clean; that is
        // already==HEAD, not a failure — no restore, no message.
        let vcs = FakeVcs::new()
            .dirty(Ok(true))
            .seal(Err("nothing to commit, working tree clean".to_string()));
        let out = run_commit_barrier(&vcs, wt(), Some("noop"), true, None, None);
        assert_eq!(vcs.discards(), 0, "nothing-to-commit must not restore");
        assert!(!out.committed);
        assert!(out.message.is_empty(), "got: {}", out.message);
    }

    #[test]
    fn none_commit_msg_reverts_changed_worktree() {
        // No commit_msg + a fully-successful batch that changed the worktree must
        // restore to HEAD: new dirt must not persist across calls.
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new().changed(Ok(true));
        let out = run_commit_barrier(&vcs, wt(), None, true, Some(&before), None);
        assert_eq!(vcs.seals(), 0);
        assert_eq!(vcs.discards(), 1, "new dirt without commit_msg is reverted");
        assert!(out.worktree_changed);
        assert!(!out.committed, "a restore must not set committed");
        assert!(out.message.contains("reverted"), "got: {}", out.message);
        assert!(
            out.message
                .contains("Run with commit_msg like `run({commands:[…], commit_msg)`"),
            "got: {}",
            out.message
        );
    }

    #[test]
    fn none_commit_msg_leaves_unchanged_worktree_alone() {
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new().changed(Ok(false));
        let out = run_commit_barrier(&vcs, wt(), None, true, Some(&before), None);
        assert_eq!(vcs.discards(), 0);
        assert!(!out.worktree_changed);
        assert!(out.message.is_empty());
    }

    #[test]
    fn none_commit_msg_warns_without_reverting_when_backend_cannot_revert() {
        // A backend that cannot revert (the project's live checkout) must NOT
        // discard on a no-commit_msg run that left dirt: reverting the checkout
        // would destroy the user's own uncommitted work. It warns instead.
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new().changed(Ok(true)).can_revert(false);
        let out = run_commit_barrier(&vcs, wt(), None, true, Some(&before), None);
        assert_eq!(vcs.discards(), 0, "the live checkout is never reverted");
        assert_eq!(vcs.seals(), 0);
        assert!(!out.worktree_changed, "Cairn mutated nothing");
        assert!(!out.committed);
        assert!(
            out.message.contains("live checkout") && out.message.contains("worktree"),
            "the agent is warned it crossed the worktree boundary: {}",
            out.message
        );
    }

    #[test]
    fn none_commit_msg_leaves_failed_batch_dirt_for_inspection() {
        // Deliberate boundary: a failed batch (all_ok=false) with no commit_msg
        // keeps the hygiene gate out of the way so the failure's side effects stay
        // visible. The `all_ok` guard lives only here.
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new().changed(Ok(true));
        let out = run_commit_barrier(&vcs, wt(), None, false, Some(&before), None);
        assert_eq!(vcs.discards(), 0, "a failed batch's dirt is not reverted");
        assert!(!out.worktree_changed);
        assert!(out.message.is_empty());
    }

    /// Over the read-only non-worktree sentinel the barrier is a clean no-op in
    /// both directions: with commit_msg it never seals (is_dirty=false), and
    /// without commit_msg it never discards (changed_since=false) — so an agent
    /// on the project's live checkout never has its working copy sealed or
    /// reverted by Cairn.
    #[test]
    fn non_worktree_barrier_is_a_safe_noop_both_directions() {
        use crate::mcp::vcs::NonWorktreeVcs;
        let before = VcsSnapshot(String::new());

        let with_msg = run_commit_barrier(&NonWorktreeVcs, wt(), Some("work"), true, None, None);
        assert!(!with_msg.worktree_changed);
        assert!(!with_msg.committed);
        assert!(with_msg.message.is_empty());

        let no_msg = run_commit_barrier(&NonWorktreeVcs, wt(), None, true, Some(&before), None);
        assert!(!no_msg.worktree_changed);
        assert!(no_msg.message.is_empty());
    }
}

#[cfg(test)]
mod terminal_finalize_tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use tempfile::tempdir;

    /// Run an async block on a throwaway current-thread runtime. Each call
    /// creates and drops its own runtime, so the test thread carries no ambient
    /// runtime between calls — finalize's internal `block_on` then runs safely.
    fn block_on<T>(fut: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fut)
    }

    fn test_orchestrator(db: LocalDb) -> Orchestrator {
        let temp = tempdir().unwrap();
        let config_dir = temp.keep();
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search));
        let services = Arc::new(TestServicesBuilder::new().build());
        Orchestrator::builder(db_state, services, config_dir).build()
    }

    async fn seed(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p','w','P','P','/tmp',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES('i','p',7,'I','active','active','none',1,1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES('e','rec','i','p','running',1,2);
            INSERT INTO jobs(id, project_id, issue_id, execution_id, uri_segment, status, created_at, updated_at) VALUES('j','p','i','e','builder','running',1,1);
            INSERT INTO jobs(id, project_id, issue_id, execution_id, parent_job_id, uri_segment, status, created_at, updated_at) VALUES('task-j','p','i','e','j','Explore','running',1,1);
            INSERT INTO job_terminals(id, job_id, session_id, command, status, created_at, slug) VALUES('t','j','s1','sleep 100','running',1,'run-1');
            INSERT INTO job_terminals(id, job_id, session_id, command, status, created_at, slug) VALUES('task-t','task-j','task-s1','sleep 100','running',1,'run-1');
            ",
        )
        .await
        .unwrap();
    }

    async fn read_terminal(
        db: &LocalDb,
        session_id: &str,
    ) -> (String, Option<i32>, Option<String>) {
        let session_id = session_id.to_string();
        db.query_opt(
            "SELECT status, exit_code, output_tail FROM job_terminals WHERE session_id = ?1",
            params![session_id.as_str()],
            |row| {
                Ok((
                    row.text(0)?,
                    row.opt_i64(1)?.map(|v| v as i32),
                    row.opt_text(2)?,
                ))
            },
        )
        .await
        .unwrap()
        .expect("terminal row present")
    }

    fn node_uri() -> String {
        cairn_common::uri::build_node_terminal_uri("P", 7, 2, "builder", "run-1")
    }

    fn task_uri() -> String {
        cairn_common::uri::build_task_terminal_uri("P", 7, 2, "builder", "Explore", "run-1")
    }

    #[test]
    fn finalize_marks_exited_retains_tail_and_is_idempotent() {
        let db = block_on(crate::storage::migrated_test_db("term_finalize_a.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));

        let buffer: Arc<Mutex<VecDeque<u8>>> =
            Arc::new(Mutex::new(b"all done\n".iter().copied().collect()));
        let uri = node_uri();
        finalize_terminal_session(
            &orch,
            "s1",
            Some(0),
            &buffer,
            "sleep 100",
            Some("j".to_string()),
            "run-1",
            &uri,
        );

        let (status, code, tail) = block_on(read_terminal(&orch.db.local, "s1"));
        assert_eq!(status, "exited");
        assert_eq!(code, Some(0));
        assert!(tail.unwrap_or_default().contains("all done"));

        // A second finalize with a different code must not re-transition the row.
        finalize_terminal_session(
            &orch,
            "s1",
            Some(99),
            &buffer,
            "sleep 100",
            Some("j".to_string()),
            "run-1",
            &uri,
        );
        let (status2, code2, _) = block_on(read_terminal(&orch.db.local, "s1"));
        assert_eq!(status2, "exited");
        assert_eq!(
            code2,
            Some(0),
            "idempotent finalize must not overwrite the recorded exit code"
        );
    }

    #[test]
    fn mark_terminal_exited_is_conditional_on_running() {
        let db = block_on(crate::storage::migrated_test_db("term_finalize_b.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));

        let first = block_on(mark_terminal_exited_and_load_context(
            orch.db.local.clone(),
            "s1".to_string(),
            Some("j".to_string()),
            Some(0),
            Some("tail".to_string()),
        ))
        .unwrap();
        assert!(first.is_some(), "first mark transitions a running row");

        let second = block_on(mark_terminal_exited_and_load_context(
            orch.db.local.clone(),
            "s1".to_string(),
            Some("j".to_string()),
            Some(5),
            Some("tail".to_string()),
        ))
        .unwrap();
        assert!(
            second.is_none(),
            "an already-exited row must not transition again"
        );
    }

    #[test]
    fn build_terminal_detail_uri_matches_node_builder() {
        let db = block_on(crate::storage::migrated_test_db("term_finalize_c.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));

        let uri = block_on(build_terminal_detail_uri(&orch, Some("j"), None, "run-1"));
        assert_eq!(uri, node_uri());
    }

    #[test]
    fn build_terminal_detail_uri_matches_task_builder() {
        let db = block_on(crate::storage::migrated_test_db("term_finalize_task.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));

        let uri = block_on(build_terminal_detail_uri(
            &orch,
            Some("task-j"),
            None,
            "run-1",
        ));
        assert_eq!(uri, task_uri());
    }

    #[test]
    fn resolve_task_terminal_targets_child_job_and_scopes_slug_collisions() {
        let db = block_on(crate::storage::migrated_test_db("term_resolve_task.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));
        let resource = CairnResource::TaskTerminal {
            project: "P".to_string(),
            number: 7,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
            slug: "run-1".to_string(),
        };

        let target = block_on(resolve_terminal_resource_target(&orch.db.local, &resource)).unwrap();
        assert_eq!(target.job_id.as_deref(), Some("task-j"));
        assert_eq!(target.slug, "run-1");
        assert!(block_on(ensure_terminal_slug_available(&orch.db.local, &target)).is_err());

        let parent_resource = CairnResource::NodeTerminal {
            project: "P".to_string(),
            number: 7,
            exec_seq: 2,
            node_id: "builder".to_string(),
            slug: "task-only".to_string(),
        };
        let parent_target = block_on(resolve_terminal_resource_target(
            &orch.db.local,
            &parent_resource,
        ))
        .unwrap();
        assert!(block_on(ensure_terminal_slug_available(
            &orch.db.local,
            &parent_target
        ))
        .is_ok());
    }

    #[test]
    fn finalize_by_session_records_honest_kill_code_and_retains_row() {
        let db = block_on(crate::storage::migrated_test_db("term_finalize_d.db"));
        let orch = test_orchestrator(db);
        block_on(seed(&orch.db.local));

        // No live session in the PTY map: finalize-by-session records the SIGKILL
        // convention (never success) and retains the row, not deletes it.
        finalize_terminal_by_session_id(&orch, "s1").unwrap();

        let (status, code, _) = block_on(read_terminal(&orch.db.local, "s1"));
        assert_eq!(status, "exited");
        assert_eq!(code, Some(KILLED_EXIT_CODE));

        // Idempotent: a second stop no-ops because the row is no longer running.
        finalize_terminal_by_session_id(&orch, "s1").unwrap();
        let (status2, code2, _) = block_on(read_terminal(&orch.db.local, "s1"));
        assert_eq!(status2, "exited");
        assert_eq!(code2, Some(KILLED_EXIT_CODE));
    }
}
