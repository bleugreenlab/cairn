//! Process execution: spawn, stream, timeout, promote-to-terminal, MCP-call
//! proxying and checkpoint-result caching.

use super::output::{strip_streamable_tail, OutputTail};
use super::sandbox_policy::build_run_sandbox_policy;
use super::types::{ItemOutcome, McpCallSpec, RunCompletePayload, RunOutputPayload, RunSpec};
use crate::mcp::gateway::McpCallOutcome;
use crate::mcp::handlers::{normalize_command, RunContext};
use crate::mcp::types::McpCallbackRequest;
use crate::models::Fence;
use crate::orchestrator::Orchestrator;
use crate::services::{sandbox, SpawnConfig};
use crate::storage::{LocalDb, RowExt};
use std::collections::VecDeque;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
use uuid::Uuid;

/// Raw process result before body formatting.
struct ExecOutput {
    stdout: String,
    stderr: String,
    exit_code: Option<i32>,
    timed_out: bool,
    /// A kernel sandbox denial detected after execution (drives the fence).
    denial: Option<sandbox::SandboxDenial>,
}

/// Maximum output buffer size (64KB)
pub(crate) const MAX_BUFFER_SIZE: usize = 64 * 1024;

/// Default pager used for agent-launched commands.
///
/// Inline `run` commands are non-interactive: stdout/stderr are piped and there
/// is no terminal for a pager such as `less` to read from. Git falls back to the
/// configured pager even for common inspection commands (`git diff`, `git log`),
/// so force pager-aware tools to stream to stdout unless the command explicitly
/// overrides the env itself (for example `PAGER=less git log`).
const NON_INTERACTIVE_PAGER: &str = "cat";

pub(crate) const READ_ONLY_CHECKOUT_DENIAL: &str = "This command tried to write the project's own checkout on this machine, which is read-only for agents — the write was blocked by the kernel sandbox, and nothing was left behind. Make the change in your own working directory instead: `write` for file edits, or a `run` batch carrying a `commit_msg`.";

fn apply_non_interactive_pager_env(mut config: SpawnConfig) -> SpawnConfig {
    config = config.env("GIT_PAGER", NON_INTERACTIVE_PAGER);
    config.env("PAGER", NON_INTERACTIVE_PAGER)
}

/// Strip the dev-instance build-target routing env from a worktree command's
/// spawn config (see [`crate::env::DEV_INSTANCE_ROUTING_ENV`]).
///
/// A host orchestrator launched by `bun dev:instance` carries `CAIRN_INSTANCE=1`
/// plus a derived `CARGO_TARGET_DIR` pointing at the single shared dev target
/// dir. Command spawns inherit that env wholesale, so without this strip every
/// worktree's cargo checks route into the one shared dir and concurrent
/// worktrees corrupt each other's build-script `OUT_DIR` (CAIRN-2533). Removing
/// both keys restores the design's per-worktree `src-tauri/target`. This one
/// seam covers both check cadences (sandboxed when:write and turn-end
/// when:review) and the agent's own `run` cargo, since all route through
/// `execute_process`.
fn strip_dev_instance_routing_env(mut config: SpawnConfig) -> SpawnConfig {
    for key in crate::env::DEV_INSTANCE_ROUTING_ENV {
        config = config.env_remove(key);
    }
    config
}

/// Run a single resolved item: execute it, format its body, and (for shell
/// items only) cache checkpoint results and auto-push on `git commit`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_one(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    cwd: &str,
    tool_use_id: &str,
    run_context: Option<&RunContext>,
    header: String,
    spec: Result<RunSpec, String>,
) -> ItemOutcome {
    let spec = match spec {
        Ok(spec) => spec,
        Err(e) => return ItemOutcome::failed(header, e),
    };

    let (shell, flag) = if cfg!(windows) {
        ("cmd.exe", "/c")
    } else {
        ("bash", "-c")
    };

    // A trailing `| tail -N` on a shell item buffers all upstream output until
    // EOF, so the live preview stays blank for the whole run. When there is a run
    // context (a live preview worth restoring), strip that tail for execution and
    // re-apply the line limit to the captured stdout after the process exits (see
    // below). `shell_command` stays the ORIGINAL string — grant key, checkpoint
    // cache, and the displayed header all key on exactly what the agent wrote.
    let mut tail_transform: Option<OutputTail> = None;

    let (program, args, timeout, shell_command, stdin): (
        String,
        Vec<String>,
        Option<u32>,
        Option<String>,
        Option<String>,
    ) = match spec {
        RunSpec::Shell { command, timeout } => {
            // Only strip when a live preview exists to protect (a run context);
            // headless/CLI invocations keep exact pipeline semantics.
            let exec_command = match run_context.and_then(|_| strip_streamable_tail(&command)) {
                Some((stripped, tail)) => {
                    tail_transform = Some(tail);
                    stripped
                }
                None => command.clone(),
            };
            (
                shell.to_string(),
                vec![flag.to_string(), exec_command],
                timeout,
                Some(command),
                None,
            )
        }
        RunSpec::Script {
            program,
            args,
            timeout,
            stdin,
        } => (program, args, timeout, None, stdin),
        // MCP calls are proxied RPC through the host gateway, not process
        // exec — handle and return here.
        RunSpec::McpCall(spec) => {
            return run_mcp_call(orch, request, run_context, cwd, header, *spec).await;
        }
        // A REPL send writes to a live eval-server's persistent stdin and awaits
        // one framed response — not a process exec.
        RunSpec::ReplSend {
            slug,
            code,
            timeout,
            lang,
        } => {
            return run_repl_send(orch, run_context, header, slug, code, timeout, lang).await;
        }
    };

    let timeout_ms = super::clamp_run_item_timeout_ms(timeout);

    let mut exec = match execute_process(
        orch,
        cwd,
        tool_use_id,
        run_context,
        &program,
        &args,
        timeout_ms,
        shell_command.as_deref(),
        stdin.as_deref(),
        true,
    )
    .await
    {
        Ok(exec) => exec,
        Err(e) => return ItemOutcome::failed(header, format!("Failed to spawn command: {e}")),
    };

    // Denial-driven namespace fence: if the kernel sandbox blocked this command,
    // adjudicate the crossing under the agent's escape policy. On allow we
    // re-execute escalated (sandbox off) so the now-granted crossing proceeds.
    if let Some(denial) = exec.denial.take() {
        use crate::mcp::handlers::fence;
        // The live checkout is read-only and non-negotiable: an ambient checkout cwd
        // routes a denial to a clear explanation, never a fence prompt. There is
        // no run context to adjudicate and nothing the user can grant — changes
        // can only be published through an executor cell. This replaces the raw EPERM
        // chat would otherwise surface.
        if !crate::jj::is_jj_dir(std::path::Path::new(cwd)) {
            let _ = denial;
            return ItemOutcome::failed(header, READ_ONLY_CHECKOUT_DENIAL);
        }
        if let Some((run_id, fence_mode)) = fence::resolve_run_fence(orch, request).await {
            let crossing = match &denial {
                sandbox::SandboxDenial::Path { path, .. } => {
                    fence::Crossing::shell_path(path.as_path(), &path.display().to_string())
                }
                sandbox::SandboxDenial::Command => fence::Crossing::shell_command(
                    format!("command blocked by the executor sandbox: {header}"),
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
                        stdin.as_deref(),
                        false,
                    )
                    .await
                    {
                        Ok(e) => exec = e,
                        Err(e) => {
                            return ItemOutcome::failed(
                                header,
                                format!("Failed to spawn command: {e}"),
                            )
                        }
                    }
                }
                fence::FenceDecision::Deny(msg) => return ItemOutcome::failed(header, msg),
                fence::FenceDecision::Suspended => {
                    return ItemOutcome {
                        header,
                        body: "Run suspended pending worktree fence approval; resume will \
                               continue once it is answered."
                            .to_string(),
                        succeeded: false,
                        suspended: true,
                        images: Vec::new(),
                        tracked_modifications: None,
                    }
                }
            }
        }
    }

    // The stripped `| tail -N` re-applied (see resolve above): the command ran
    // bare so its full stdout streamed to the live preview; trim the captured
    // stdout back to the last N lines and mask the exit code to tail's success
    // (0), so the recorded result is what `cmd | tail -N` produces — only the live
    // preview changed. Skipped on timeout: the command never reached EOF (tail
    // would still be buffering), and the promoted-terminal path owns that
    // partial-output case.
    if let Some(tail) = &tail_transform {
        if !exec.timed_out {
            exec.stdout = tail.apply(&exec.stdout);
            exec.exit_code = Some(0);
        }
    }

    let succeeded = !exec.timed_out && exec.exit_code.is_none_or(|code| code == 0);

    // Shell-only side effects keyed on the command string.
    if let Some(ref command) = shell_command {
        if !exec.timed_out {
            if let Some(ctx) = run_context {
                cache_checkpoint_result(orch, &ctx.job_id, command, exec.exit_code).await;
            }
        }
    }

    let body = format_exec_body(&exec, timeout_ms);
    ItemOutcome {
        header,
        body,
        succeeded,
        suspended: false,
        images: Vec::new(),
        tracked_modifications: None,
    }
}

/// Execute a proxied MCP `tools/call` through the host gateway. A missing
/// gateway or a down/erroring server fails this item only — never the batch.
async fn run_mcp_call(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    run_context: Option<&RunContext>,
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
        return ItemOutcome::failed(header, "MCP gateway is not available in this host");
    };

    // Per-session connection pooling keys on the run's job id, isolating
    // concurrent agents. Fall back to the cwd when there is no run context.
    let session_key = run_context
        .map(|c| c.job_id.clone())
        .unwrap_or_else(|| cwd.to_string());

    // A gateway call is host work on an attached socket, so its bound is the
    // host-item bound, not the batch ceiling. `None` stays `None`: the gateway's
    // own default call timeout is the meaningful floor here.
    let timeout = super::clamp_host_item_timeout_ms(timeout);
    match gateway
        .call_tool_once(
            &session_key,
            &credential_key,
            &config,
            &tool,
            args.clone(),
            None,
            None,
            timeout,
            None,
        )
        .await
    {
        Ok(McpCallOutcome::Complete(result)) => ItemOutcome {
            header,
            body: result.text,
            succeeded: true,
            suspended: false,
            images: result.images,
            tracked_modifications: None,
        },
        Ok(outcome) => {
            suspend_mcp_call(
                orch,
                request,
                run_context,
                header,
                session_key,
                credential_key,
                tool,
                args,
                config,
                timeout,
                outcome,
            )
            .await
        }
        Err(e) => ItemOutcome::failed(header, e),
    }
}

#[allow(clippy::too_many_arguments)]
async fn suspend_mcp_call(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    run_context: Option<&RunContext>,
    header: String,
    session_key: String,
    server: String,
    tool: String,
    arguments: serde_json::Value,
    config: crate::config::mcp_servers::McpServerConfig,
    timeout_ms: Option<u32>,
    outcome: McpCallOutcome,
) -> ItemOutcome {
    use crate::mcp::handlers::durable_suspend::{self, Condition, Record};
    use crate::mcp::handlers::mcp_continuation::{self, McpContinuationState};
    use crate::mcp::handlers::tool_use_correlation::Claim;
    let Some(ctx) = run_context else {
        return ItemOutcome::failed(header, "MCP continuation requires an active run");
    };
    let Ok((_, db)) = super::super::run_context::lookup_run_routed(&orch.db, request).await else {
        return ItemOutcome::failed(header, "MCP continuation database is unavailable");
    };
    let Some(turn_id) = durable_suspend::suspending_turn_id(orch, &db, &ctx.run_id).await else {
        return ItemOutcome::failed(header, "MCP continuation requires an active turn");
    };
    let tool_use_id = match request.tool_use_id.clone() {
        Some(id) => id,
        None => match super::claim_batch_tool_use_id(&db, &ctx.run_id, &turn_id, &request.payload)
            .await
        {
            Claim::One(id) => id,
            Claim::None => {
                return ItemOutcome::failed(
                    header,
                    "Could not correlate MCP continuation with its original run call",
                )
            }
            Claim::Ambiguous(_) => {
                return ItemOutcome::failed(
                    header,
                    "MCP continuation matched several original run calls ambiguously",
                )
            }
        },
    };
    let Ok(Some(session_id)) = durable_suspend::run_session(&db, &ctx.run_id).await else {
        return ItemOutcome::failed(header, "MCP continuation requires an active session");
    };
    let mut state = McpContinuationState {
        server,
        session_key,
        config,
        tool,
        arguments,
        request_state: None,
        timeout_ms,
        pending_operation: None,
        pending_input_requests: None,
        task_input_pending: false,
        mrtr_round: 0,
        pending_prompt_id: None,
        task: None,
        next_poll_at_ms: None,
        deadline_ms: Some(
            chrono::Utc::now()
                .timestamp_millis()
                .saturating_add(mcp_continuation::DEFAULT_CONTINUATION_TTL_MS),
        ),
    };
    match outcome {
        McpCallOutcome::InputRequired {
            input_requests,
            request_state,
        } => {
            state.pending_input_requests = Some(input_requests);
            state.request_state = request_state;
        }
        McpCallOutcome::Task {
            task_id,
            poll_interval_ms,
            ttl_ms,
        } => {
            mcp_continuation::set_task(
                &mut state,
                task_id,
                poll_interval_ms.unwrap_or(500),
                chrono::Utc::now().timestamp_millis(),
                ttl_ms,
            );
        }
        McpCallOutcome::Complete(_) => unreachable!(),
    }
    let deadline = state.deadline_ms;
    let record = Record {
        id: cairn_common::ids::mint_child(&ctx.run_id),
        job_id: ctx.job_id.clone(),
        run_id: ctx.run_id.clone(),
        session_id,
        turn_id,
        tool_use_id,
        condition: Condition::McpContinuation { state },
        deadline,
        created: chrono::Utc::now().timestamp_millis(),
    };
    if let Err(error) = durable_suspend::suspend(orch, &db, &record).await {
        return ItemOutcome::failed(header, error);
    }
    // The global durable scheduler owns the continuation from here. Its
    // persisted handoff delay prevents a first transition until this suspension
    // marker has reached the provider, without retaining a per-call task or
    // receiver in memory.
    ItemOutcome { header, body: "MCP call suspended; the same run call remains pending and will resume when the external operation completes.".into(), succeeded: true, suspended: true, images: Vec::new(), tracked_modifications: None }
}

/// Send inline code to a live REPL session and compose its outcome. Fails
/// closed on every missing precondition (no run context, unknown slug,
/// dead/timed-out session, language mismatch) rather than silently spawning a
/// fresh process — the whole value of the REPL is that state persists.
async fn run_repl_send(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    header: String,
    slug: String,
    code: String,
    timeout: Option<u32>,
    lang: crate::mcp::handlers::repl::ReplLang,
) -> ItemOutcome {
    use crate::mcp::handlers::repl::{self, ReplExchangeStatus, ReplOrigin};

    let Some(ctx) = run_context else {
        return ItemOutcome::failed(
            header,
            "A REPL send needs a node context and cannot run without an execution run context.",
        );
    };

    // A REPL send awaits a live eval server over an attached socket, so it takes
    // the host-item bound. Preserving the previous 120s default keeps an omitted
    // bound exactly where it was; only the unreachable ceiling above it moves.
    let timeout_ms =
        super::clamp_host_item_timeout_ms(timeout).unwrap_or(super::MAX_HOST_ITEM_TIMEOUT_MS);
    match repl::send_recorded(
        orch,
        &ctx.job_id,
        &slug,
        &code,
        Duration::from_millis(timeout_ms as u64),
        ReplOrigin::Agent,
        Some(lang),
    )
    .await
    {
        // Fail-closed precondition (unknown slug, language mismatch) that predates
        // any recorded exchange.
        Err(message) => ItemOutcome::failed(header, message),
        Ok(exchange) => match exchange.status {
            ReplExchangeStatus::Success | ReplExchangeStatus::Error => {
                let mut body = String::new();
                let mut push = |section: &str| {
                    if section.is_empty() {
                        return;
                    }
                    if !body.is_empty() {
                        body.push('\n');
                    }
                    body.push_str(section);
                };
                if let Some(value) = exchange.value.as_deref() {
                    push(value);
                }
                if let Some(stdout) = exchange.stdout.as_deref() {
                    push(stdout);
                }
                if let Some(stderr) = exchange.stderr.as_deref() {
                    push(&format!("stderr:\n{stderr}"));
                }
                if let Some(note) = exchange.note.as_deref() {
                    push(&format!("note: {note}"));
                }
                if let Some(error) = exchange.error.as_deref() {
                    push(error);
                }
                ItemOutcome {
                    header,
                    body,
                    succeeded: matches!(exchange.status, ReplExchangeStatus::Success),
                    suspended: false,
                    images: Vec::new(),
                    tracked_modifications: None,
                }
            }
            // Died / Timeout / Protocol (and the impossible lingering Pending) all
            // carry their agent-facing hint in `error`; the funnel already killed
            // and unregistered a session-ending outcome.
            _ => ItemOutcome::failed(
                header,
                exchange
                    .error
                    .unwrap_or_else(|| format!("REPL '{slug}' send failed.")),
            ),
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

/// Build the agent-facing spawn config shared by inline `run` items and the
/// stateful REPL eval-server: identical env injection (MCP callback, PATH shim,
/// uv cache, worktree VCS env, git identity, per-job scratch `TMPDIR`, the
/// `cairn:~` home URI) plus the optional OS sandbox. The one canonical
/// env-injection site for work the host spawns itself, so those two paths
/// cannot drift.
///
/// A batch placed onto a build cell does NOT pass through here: the executor
/// composes that machine's own PATH, scratch, and toolchain env, and cairn-core
/// states only what stays true wherever the batch lands. That seam is
/// [`super::placed_batch_env`], and the run identity below is the one thing both
/// paths must agree on — an agent shell knows who it is regardless of which
/// machine it got.
///
/// Callers layer only their own extras on top: `execute_process` adds `.stdin`
/// for the `uv run -` payload; the REPL spawner always captures stdin (its
/// request protocol) and takes stdout for its reader thread.
pub(crate) async fn build_agent_spawn_config(
    orch: &Orchestrator,
    cwd: &str,
    run_context: Option<&RunContext>,
    program: &str,
    args: &[String],
    sandbox_policy: Option<sandbox::SandboxPolicy>,
) -> SpawnConfig {
    // Inject the MCP callback env so an in-run `cairn read|change ...` shell
    // invocation can authenticate and forward to this same app (the basis for
    // composability like `cairn read <uri> | rg ...`). The CLI is a thin client;
    // it never opens the DB, it forwards over this callback.
    // Point uv at the shared, per-home Cairn package cache (`<cairn_home>/uv-cache`)
    // so every agent shares warm caches and uv never writes to `~/.cache/uv`,
    // which is outside the fence's writable set. The dir is in the sandbox
    // writable set (`services::sandbox::default_writable_extra`) and created at
    // host startup (`env::ensure_uv_cache_dir`).
    let uv_cache_dir = crate::env::uv_cache_dir().to_string_lossy().into_owned();
    let mut spawn_config = SpawnConfig::new(program);
    for arg in args {
        spawn_config = spawn_config.arg(arg);
    }
    let mut spawn_config = strip_dev_instance_routing_env(apply_non_interactive_pager_env(
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
            // Put the host-owned `cairn` shim dir ahead of the resolved user
            // PATH so an in-run `cairn read|write|watch …` resolves the CLI.
            .env("PATH", &crate::env::agent_shell_path())
            // Shared per-home uv package cache; see the binding above.
            .env("UV_CACHE_DIR", &uv_cache_dir)
            // The Cairn home this host resolved, stated rather than inherited.
            // It is the root every Cairn-owned path hangs off, so a spawn that
            // has it can compose one it was never handed — a node's scratch dir
            // and its terminal logs at `$CAIRN_HOME/scratch/<node URI tail>`.
            // Inheritance alone would leave that true only when whoever
            // launched the host happened to export the variable, while
            // `cairn_home()` resolved a default regardless; naming it here makes
            // the child agree with the host by construction.
            .env(
                "CAIRN_HOME",
                &cairn_common::paths::cairn_home().to_string_lossy(),
            )
            .sandbox(sandbox_policy),
    ));
    // Make bare `git`/`jj` behave correctly inside a jj-only worktree: managed
    // `JJ_CONFIG`/editor so a bare `jj` commit is pushable, and a
    // `GIT_CEILING_DIRECTORIES` ceiling so a bare `git` fails loudly instead of
    // resolving up to the `~/.cairn` HOME repo. Empty (no-op) for a non-worktree
    // (live-checkout) cwd, so that path is untouched.
    for (k, v) in crate::mcp::vcs::worktree_shell_vcs_env(orch, std::path::Path::new(cwd)) {
        spawn_config = spawn_config.env(&k, &v);
    }
    if let Ok(secret) = orch.mcp_auth.get_secret_for_mcp() {
        spawn_config = spawn_config.env("CAIRN_MCP_SECRET", &secret);
    }
    // Managed Build Services: inject each enabled service's client env so the
    // agent's tooling connects to the Cairn-owned daemon (e.g. the shared sccache
    // server) rather than auto-starting its own (the cross-worktree EPERM bug).
    // Empty unless this spawn builds inside a managed build root — the daemon
    // runs each cache-miss compile itself, so a spawn whose `target/` its sandbox
    // does not cover would fail to compile rather than merely miss the cache.
    // Deliberately independent of the fence: supervision and compile caching are
    // unrelated policies, and gating on the fence switched the cache off entirely
    // wherever the dial is `allow`.
    for (k, v) in orch.build_service_client_env(std::path::Path::new(cwd)) {
        spawn_config = spawn_config.env(&k, &v);
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
        // `cairn:~` shorthand resolution and the readable per-node scratch name
        // both use the job's canonical home URI.
        let home_uri = job_home_uri(orch, ctx).await;
        // Point the command's temp-file handling at a per-job scratch dir so
        // default tooling (mktemp, cargo, compilers, harness logs) writes there
        // with no agent awareness. TMP/TEMP cover tools that ignore TMPDIR.
        let scratch = crate::scratch::ensure_job_scratch_dir(&ctx.job_id, home_uri.as_deref());
        // Give a helper script written there the dependency resolution a script
        // inside the checkout gets for free.
        cairn_common::scratch::link_scratch_dependency_resolution(
            &scratch,
            std::path::Path::new(cwd),
        );
        let scratch = scratch.to_string_lossy().to_string();
        spawn_config = spawn_config
            .env("TMPDIR", &scratch)
            .env("TMP", &scratch)
            .env("TEMP", &scratch);
        if let Some(home_uri) = home_uri {
            spawn_config = spawn_config.env("CAIRN_HOME_URI", &home_uri);
        }
    }
    spawn_config
}

/// The job's canonical home URI (e.g. `cairn://p/CAIRN/1/1/builder`; a sub-task
/// nests under its parent as `.../{seq}/{parent}/task/{segment}`), used both for
/// `cairn:~` shorthand and the readable per-node scratch dir name. `None` when
/// the run lacks issue/exec coordinates or the node segment can't resolve.
pub(crate) async fn job_home_uri(orch: &Orchestrator, ctx: &RunContext) -> Option<String> {
    let (num, seq) = (ctx.issue_number?, ctx.exec_seq?);
    let segment =
        crate::jobs::queries::node_uri_segment_for_job(&orch.db.local, &ctx.job_id).await?;
    let parent_segment =
        crate::jobs::queries::parent_uri_segment_for_job(&orch.db.local, &ctx.job_id).await;
    Some(cairn_common::uri::build_job_base_uri(
        &ctx.project_key,
        num,
        seq,
        &segment,
        parent_segment.as_deref(),
    ))
}
/// Spawn a process, stream its stdout/stderr, and wait with a timeout.
#[allow(clippy::too_many_arguments)]
async fn execute_process(
    orch: &Orchestrator,
    cwd: &str,
    tool_use_id: &str,
    run_context: Option<&RunContext>,
    program: &str,
    args: &[String],
    timeout_ms: u32,
    shell_command: Option<&str>,
    stdin: Option<&str>,
    sandbox_enabled: bool,
) -> Result<ExecOutput, String> {
    let services = &orch.services;
    let tool_use_id = tool_use_id.to_string();
    let inline_command_id = Uuid::new_v4().to_string();

    // Build the OS filesystem sandbox for this spawn (None for fence allow, no
    // run context, an already-granted command, or platforms without a sandbox
    // primitive). `sandbox_enabled` is false on an escalated
    // re-execution after a fence grant.
    // Host execution only ever runs in the agent's jj residence or the project's
    // live checkout, so the path tells the truth here.
    let checkout_kind = super::sandbox_policy::RunCheckout::infer(cwd);
    let sandbox = if sandbox_enabled {
        // Grant key: the shell command for shell items, else the program for
        // skill scripts — so a command-scoped session grant generalizes to
        // scripts too (the only crossing kind Linux produces).
        build_run_sandbox_policy(
            orch,
            cwd,
            checkout_kind,
            run_context.map(|c| c.run_id.as_str()),
            run_context.map(|c| c.project_id.as_str()),
            shell_command.or(Some(program)),
        )
        .await
    } else {
        None
    };
    // Ask agents get a synthetic command-scoped macOS fallback when path recovery
    // misses; Deny agents preserve their raw fail-fast output. Ambient live-checkout
    // runs also enable fallback detection regardless of fence: the
    // checkout is read-only, so a kernel block must be recognized even when macOS
    // log recovery misses or a shell masks the exit (`... || true`). run_one
    // routes such a denial to the hard read-only-checkout message, never a fence
    // prompt — a live checkout is non-grantable, so enabling detection cannot
    // synthesize a grant.
    let command_scoped_fallback = checkout_kind.is_project_live()
        || matches!(sandbox.as_ref().map(|(_, fence)| *fence), Some(Fence::Ask));
    let sandbox_policy = sandbox.map(|(policy, _)| policy);
    let sandboxed = sandbox_policy.is_some();
    let spawn_started = std::time::SystemTime::now();

    let mut spawn_config =
        build_agent_spawn_config(orch, cwd, run_context, program, args, sandbox_policy).await;

    // Capture stdin only when the spec carries a payload to feed (today only
    // `uv run -`, which has no inline-source flag, so its script arrives on
    // stdin). Every other item leaves stdin inherited, unchanged.
    if stdin.is_some() {
        spawn_config = spawn_config.stdin(true);
    }

    let child = match services.process.spawn(spawn_config) {
        Ok(child) => Arc::new(Mutex::new(child)),
        Err(e) => return Err(format!("Failed to spawn command: {}", e)),
    };

    let (stdout, stderr, stdin_writer, child_pid) = {
        let mut guard = match child.lock() {
            Ok(guard) => guard,
            Err(e) => return Err(format!("Failed to access command process: {}", e)),
        };
        (
            guard.take_stdout(),
            guard.take_stderr(),
            guard.take_stdin(),
            Some(guard.id()),
        )
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
                            "run-output",
                            serde_json::to_value(RunOutputPayload {
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
                            "run-output",
                            serde_json::to_value(RunOutputPayload {
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

    // Feed the child's stdin from a dedicated thread when the spec carries a
    // payload (only `uv run -`): write the whole script, flush, then drop the
    // handle to close the pipe (EOF) so uv stops reading and executes. A
    // dedicated thread — not an inline write — keeps this robust to large code
    // and unusual buffering: if the child emitted enough stderr to fill its pipe
    // before draining stdin, an inline write could deadlock, but the reader
    // threads above keep draining while this thread keeps writing.
    let stdin_writer_handle: Option<thread::JoinHandle<()>> = stdin_writer.map(|mut writer| {
        let payload = stdin.unwrap_or("").to_string();
        thread::spawn(move || {
            use std::io::Write as _;
            let _ = writer.write_all(payload.as_bytes());
            let _ = writer.flush();
            // `writer` drops here → stdin EOF.
        })
    });

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
        // Host-routed invocations have no tree-bound promotion authority. All
        // promotable run batches are executor-routed; checks and the remaining
        // host-only classes retain the killed-at-budget contract.
        let denial = if let Some(_ctx) = run_context {
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
                sandbox::detect_denial(
                    None,
                    &partial,
                    child_pid,
                    spawn_started,
                    command_scoped_fallback,
                )
            } else {
                None
            }
        } else {
            None
        };
        if let Ok(mut child) = child.lock() {
            let _ = child.kill();
        }
        guard.disarm();
        if let Some(ctx) = run_context {
            orch.pty_state
                .unregister_inline_command(&ctx.run_id, &inline_command_id);
        }
        reap_readers_bounded(stdout_handle, stderr_handle, stdin_writer_handle).await;

        if let Some(ctx) = run_context {
            let _ = orch.services.emitter.emit(
                "run-complete",
                serde_json::to_value(RunCompletePayload {
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
    reap_readers_bounded(stdout_handle, stderr_handle, stdin_writer_handle).await;

    if let Some(ctx) = run_context {
        let _ = orch.services.emitter.emit(
            "run-complete",
            serde_json::to_value(RunCompletePayload {
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
        let mut combined = match (stdout_str.is_empty(), stderr_str.is_empty()) {
            (false, false) => format!("{stdout_str}\n{stderr_str}"),
            (false, true) => stdout_str.clone(),
            (true, false) => stderr_str.clone(),
            (true, true) => String::new(),
        };
        let ssh_crossing = command_scoped_fallback
            && (shell_command.is_some_and(|command| command.trim_start().starts_with("ssh "))
                || std::path::Path::new(program)
                    .file_name()
                    .is_some_and(|name| name == "ssh"));
        if ssh_crossing && exit_code != Some(0) {
            combined.push_str("\nPermission denied while reading SSH credentials");
        }
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
    })
}

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
    stdin_handle: Option<thread::JoinHandle<()>>,
) {
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::task::spawn_blocking(move || {
            let _ = stdout_handle.join();
            let _ = stderr_handle.join();
            // The stdin writer (only present for `uv run -`) has normally long
            // since written its payload and closed the pipe; join it under the
            // same ceiling so it is never left dangling.
            if let Some(h) = stdin_handle {
                let _ = h.join();
            }
        }),
    )
    .await;
}

/// Cache a checkpoint command result if the executed command matches the job's checkpoint command.
/// This enables the checkpoint lookback optimization: when the programmatic checkpoint runs,
/// it can use this cached result instead of re-running the command.
pub(crate) async fn cache_checkpoint_callback(
    orch: &Orchestrator,
    job_id: &str,
    command: &str,
    _cwd: &str,
    exit_code: Option<i32>,
) {
    cache_checkpoint_result(orch, job_id, command, exit_code).await;
}

async fn cache_checkpoint_result(
    orch: &Orchestrator,
    job_id: &str,
    command: &str,
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

    let commit_sha = match crate::execution::cache::resolve_job_logical_head(orch, job_id).await {
        Ok(sha) => sha,
        Err(_) => return,
    };
    let is_dirty = false;
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
    fn strip_dev_instance_routing_env_marks_both_keys_for_removal() {
        let config = strip_dev_instance_routing_env(SpawnConfig::new("bash"));
        for key in crate::env::DEV_INSTANCE_ROUTING_ENV {
            assert!(
                config.env_remove.iter().any(|k| k == key),
                "{key} must be stripped so a worktree command builds into its own target dir"
            );
        }
    }

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
}
