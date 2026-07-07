//! Process execution: spawn, stream, timeout, promote-to-terminal, MCP-call
//! proxying, warm-search fast path, and checkpoint-result caching.

use super::hygiene::expand_tilde;
use super::output::{strip_streamable_tail, OutputTail};
use super::sandbox_policy::build_run_sandbox_policy;
use super::types::{
    ItemOutcome, McpCallSpec, PromotedTerminal, RunCompletePayload, RunOutputPayload, RunSpec,
};
use crate::mcp::handlers::search_translate::{translate_search_command, TranslatedSearch};
use crate::mcp::handlers::{normalize_command, search, terminal, RunContext};
use crate::mcp::types::McpCallbackRequest;
use crate::models::Fence;
use crate::orchestrator::Orchestrator;
use crate::services::{sandbox, SpawnConfig};
use crate::storage::{LocalDb, RowExt};
use portable_pty::CommandBuilder;
use std::collections::VecDeque;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
use uuid::Uuid;

/// Raw process result before body formatting.
pub(super) struct ExecOutput {
    pub(super) stdout: String,
    pub(super) stderr: String,
    pub(super) exit_code: Option<i32>,
    pub(super) timed_out: bool,
    /// A kernel sandbox denial detected after execution (drives the fence).
    pub(super) denial: Option<sandbox::SandboxDenial>,
    /// Set when a timed-out item with a run context was detached to a terminal
    /// instead of killed. Carries the partial output (in `stdout`/`stderr`) plus
    /// where the still-running process can be reached.
    pub(super) promoted: Option<PromotedTerminal>,
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

fn apply_non_interactive_pager_env(mut config: SpawnConfig) -> SpawnConfig {
    config = config.env("GIT_PAGER", NON_INTERACTIVE_PAGER);
    config.env("PAGER", NON_INTERACTIVE_PAGER)
}

pub(crate) fn apply_non_interactive_pager_env_to_pty(cmd: &mut CommandBuilder) {
    cmd.env("GIT_PAGER", NON_INTERACTIVE_PAGER);
    cmd.env("PAGER", NON_INTERACTIVE_PAGER);
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

/// PTY-spawn counterpart of [`strip_dev_instance_routing_env`]: removes the same
/// dev-instance build-target routing keys from a `CommandBuilder` (whose base
/// env is seeded from the host process) so a worktree terminal builds into its
/// own target dir (CAIRN-2533).
pub(crate) fn scrub_dev_instance_routing_pty(cmd: &mut CommandBuilder) {
    for key in crate::env::DEV_INSTANCE_ROUTING_ENV {
        cmd.env_remove(key);
    }
}

/// Run a single resolved item: execute it, format its body, and (for shell
/// items only) cache checkpoint results and auto-push on `git commit`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_one(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    cwd: &str,
    tool_use_id: &str,
    run_context: Option<&RunContext>,
    _commit_present: bool,
    branch_scoped_run: bool,
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

    let (program, args, timeout, shell_command): (
        String,
        Vec<String>,
        Option<u32>,
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
            )
        }
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

    if let Some(command) = shell_command.as_deref() {
        if !branch_scoped_run {
            if let Some(outcome) =
                try_run_warm_search(orch, run_context, cwd, header.clone(), command).await
            {
                return outcome;
            }
        }
    }

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
        branch_scoped_run,
    )
    .await
    {
        Ok(exec) => exec,
        Err(e) => return ItemOutcome::failed(header, format!("Failed to spawn command: {e}")),
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
        if !branch_scoped_run && !crate::jj::is_jj_dir(std::path::Path::new(cwd)) {
            let _ = denial;
            return ItemOutcome::failed(
                header,
                "This command tried to write the project's live checkout, which is read-only for agents — the write was blocked by the kernel sandbox. Changes can only be made in a worktree; nothing was left in the checkout. Re-run inside a worktree to make changes that persist.",
            );
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
                        branch_scoped_run,
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
        if promoted.wake_subscribed {
            body.push_str(&format!(
                "Command still running; detached to {0} — readable and killable there. \
                 You will be notified when the command exits.",
                promoted.uri
            ));
        } else {
            body.push_str(&format!(
                "Command still running; detached to {0} — readable and killable there. \
                 Automatic exit notification could not be registered; if you need to wait for it, \
                 subscribe via cairn:~/wakes: \
                 {{subscribe:{{kind:\"terminal\", ref:\"{0}\", on:\"exit\"}}}}.",
                promoted.uri
            ));
        }
        return ItemOutcome::failed(header, body);
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
        images: Vec::new(),
    }
}

async fn try_run_warm_search(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    cwd: &str,
    header: String,
    command: &str,
) -> Option<ItemOutcome> {
    let translated = translate_search_command(command)?;
    let worktree_raw = PathBuf::from(run_context?.worktree_path.as_ref()?);
    let deny_read = orch.sandbox_deny_read();
    match translated {
        TranslatedSearch::Grep {
            pattern,
            globs,
            output_mode,
            case_insensitive,
            before_context,
            after_context,
            show_line_numbers,
            max_per_file,
            path,
        } => {
            let search_root = resolve_run_search_root(cwd, path.as_deref());
            let scope = search::resolve_worktree_scope(orch, &search_root, &worktree_raw)?;
            let params = crate::worktree_search::WorktreeGrepParams {
                pattern: pattern.clone(),
                subdir: scope.subdir,
                globs,
                max_per_file,
                output_mode,
                case_insensitive,
                before_context,
                after_context,
                show_line_numbers,
            };
            let body =
                tokio::task::spawn_blocking(move || scope.search.try_grep(&params, &deny_read))
                    .await
                    .ok()??;
            let body = search::finalize_grep_output(body, &pattern, 0, None);
            Some(warm_search_outcome(header, body))
        }
        TranslatedSearch::Files { globs, path } => {
            let pattern = match globs.as_slice() {
                [] => "**".to_string(),
                [glob] if !glob.starts_with('!') => glob.clone(),
                _ => return None,
            };
            let search_root = resolve_run_search_root(cwd, path.as_deref());
            let scope = search::resolve_worktree_scope(orch, &search_root, &worktree_raw)?;
            let subdir = scope.subdir.clone();
            let paths = tokio::task::spawn_blocking(move || {
                scope
                    .search
                    .try_glob(&pattern, subdir.as_deref(), &deny_read)
            })
            .await
            .ok()??;
            let body = paths
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join("\n");
            Some(warm_search_outcome(header, body))
        }
    }
}

fn resolve_run_search_root(cwd: &str, path: Option<&str>) -> PathBuf {
    match path {
        Some(path) => {
            let expanded = expand_tilde(path);
            let path = Path::new(&expanded);
            if path.is_absolute() {
                path.to_path_buf()
            } else {
                Path::new(cwd).join(path)
            }
        }
        None => PathBuf::from(cwd),
    }
}

fn warm_search_outcome(header: String, mut body: String) -> ItemOutcome {
    if !body.is_empty() {
        body.push('\n');
    }
    body.push_str("[served from Cairn's warm search index, not a spawned process]");
    ItemOutcome {
        header: format!("{header} [warm index]"),
        body,
        succeeded: true,
        suspended: false,
        images: Vec::new(),
    }
}

/// Execute a proxied MCP `tools/call` through the host gateway. A missing
/// gateway or a down/erroring server fails this item only — never the batch.
async fn run_mcp_call(
    orch: &Orchestrator,
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

    match gateway
        .call_tool(&session_key, &credential_key, &config, &tool, args, timeout)
        .await
    {
        Ok(result) => ItemOutcome {
            header,
            body: result.text,
            succeeded: true,
            suspended: false,
            images: result.images,
        },
        Err(e) => ItemOutcome::failed(header, e),
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
pub(super) async fn execute_process(
    orch: &Orchestrator,
    cwd: &str,
    tool_use_id: &str,
    run_context: Option<&RunContext>,
    program: &str,
    args: &[String],
    timeout_ms: u32,
    shell_command: Option<&str>,
    sandbox_enabled: bool,
    branch_scoped_run: bool,
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
            branch_scoped_run,
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
    let readonly_non_worktree =
        !branch_scoped_run && !crate::jj::is_jj_dir(std::path::Path::new(cwd));
    let command_scoped_fallback = readonly_non_worktree
        || matches!(sandbox.as_ref().map(|(_, fence)| *fence), Some(Fence::Ask));
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
                match terminal::promote_to_terminal(
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
    fn warm_search_outcome_marks_header_and_body() {
        let outcome = warm_search_outcome("rg needle".to_string(), "src/lib.rs:needle".to_string());
        assert_eq!(outcome.header, "rg needle [warm index]");
        assert!(outcome.succeeded);
        assert_eq!(
            outcome.body,
            "src/lib.rs:needle\n[served from Cairn's warm search index, not a spawned process]"
        );
    }

    #[test]
    fn warm_search_outcome_trailer_is_present_without_result_lines() {
        let outcome = warm_search_outcome("rg absent".to_string(), String::new());
        assert_eq!(
            outcome.body,
            "[served from Cairn's warm search index, not a spawned process]"
        );
    }
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
    fn scrub_dev_instance_routing_pty_removes_both_keys() {
        let mut cmd = CommandBuilder::new("bash");
        cmd.env("CAIRN_INSTANCE", "1");
        cmd.env("CARGO_TARGET_DIR", "/shared/target");
        scrub_dev_instance_routing_pty(&mut cmd);
        assert!(cmd.get_env("CAIRN_INSTANCE").is_none());
        assert!(cmd.get_env("CARGO_TARGET_DIR").is_none());
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
    #[test]
    fn terminal_denial_detection_requires_macos_sandbox_and_policy() {
        let denied = "bash: /Users/me/probe: Operation not permitted";
        assert_eq!(
            terminal::should_handle_terminal_denial(true, Some(Fence::Ask), denied),
            cfg!(target_os = "macos")
        );
        assert_eq!(
            terminal::should_handle_terminal_denial(true, Some(Fence::Deny), denied),
            cfg!(target_os = "macos")
        );
        assert!(!terminal::should_handle_terminal_denial(
            false,
            Some(Fence::Ask),
            denied
        ));
        assert!(!terminal::should_handle_terminal_denial(
            true,
            Some(Fence::Allow),
            denied
        ));
        assert!(!terminal::should_handle_terminal_denial(
            true,
            Some(Fence::Ask),
            "ordinary failure"
        ));
    }

    #[test]
    fn terminal_denial_crossing_falls_back_to_command_scope() {
        let crossing =
            terminal::terminal_denial_crossing(None, SystemTime::now(), "echo hi > $HOME/probe");
        assert_eq!(crossing.verb, "run");
        assert!(crossing.summary.contains("terminal command blocked"));
        assert_eq!(
            crossing.descriptor,
            normalize_command("echo hi > $HOME/probe")
        );
    }
}
