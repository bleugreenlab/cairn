//! Process execution: spawn, stream, timeout, promote-to-terminal, MCP-call
//! proxying and checkpoint-result caching.

use super::output::{strip_streamable_tail, OutputTail};
use super::sandbox_policy::{resolve_run_confinement, RunConfinement};
use super::types::{ItemOutcome, McpCallSpec, RunCompletePayload, RunOutputPayload, RunSpec};
use crate::config::mcp_servers::McpServerConfigExt;
use crate::mcp::gateway::{McpCallOutcome, McpResourceDef};
use crate::mcp::handlers::{normalize_command, RunContext};
use crate::mcp::types::McpCallbackRequest;
use crate::models::Fence;
use crate::orchestrator::Orchestrator;
use crate::services::{sandbox, SpawnConfig};
use crate::storage::{LocalDb, RowExt};
use cairn_common::uri::CairnResource;
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
    /// Whether the operator credential boundary refused the command, which is
    /// answered with its own refusal and never adjudicated.
    credential_refused: bool,
    /// Why the spawn's macOS profile refused it `ps` or `top`, appended after
    /// its output (see `sandbox::setuid_refusal_note`).
    setuid_refusal: Option<String>,
}

/// The verbs a cached probe positively proves a machine has, if it proves any.
///
/// A failed probe and a machine nobody has probed yet both leave an empty
/// catalog, and an empty catalog is not evidence of absence. Refusing a verb on
/// it would tell an agent the machine cannot do something when the truth is that
/// nobody could ask — which is exactly what a dropped link produces. Only a
/// probe that came back with a real catalog can refute a verb.
fn advertised_verbs(cached: &cairn_db::storage::ExecutorDesktopAutomation) -> Option<Vec<String>> {
    if cached.probe_error.is_some() {
        return None;
    }
    let tools =
        serde_json::from_str::<Vec<crate::mcp::gateway::McpToolDef>>(&cached.verbs_json).ok()?;
    if tools.is_empty() {
        return None;
    }
    Some(tools.into_iter().map(|tool| tool.name).collect())
}

async fn run_executor_action(
    orch: &Orchestrator,
    header: String,
    spec: super::types::ExecutorActionSpec,
) -> ItemOutcome {
    let Some(gateway) = orch.mcp_gateway() else {
        return ItemOutcome::failed(header, "MCP gateway is not available in this host");
    };
    let config = crate::config::settings::load_fleet(&orch.config_dir);
    let os = orch
        .fleet
        .inspect_executors(crate::fleet::unix_time_ms())
        .into_iter()
        .find(|executor| executor.name == spec.executor_name)
        .map(|executor| executor.health.advertisement.capabilities.os)
        .unwrap_or_default();
    let desktop = config.desktop_automation.resolve(&spec.executor_name, &os);
    if !desktop.enabled {
        return ItemOutcome::failed(
            header,
            format!("Desktop automation is off for {}", spec.executor_name),
        );
    }
    if let Ok(Some(cached)) =
        cairn_db::storage::get_executor_desktop_automation(&orch.db.local, &spec.executor_name)
            .await
    {
        if let Some(verbs) = advertised_verbs(&cached) {
            if !verbs.iter().any(|verb| verb == &spec.action) {
                return ItemOutcome::failed(header, format!(
                    "{} does not advertise `{}`. Its desktop verbs are: {}. Read cairn://executors/{}.",
                    spec.executor_name, spec.action, verbs.join(", "), spec.executor_name
                ));
            }
        }
    }
    let facade =
        crate::fleet::desktop::placed_desktop_facade(orch, &spec.executor_name, desktop.binary);
    let timeout = super::clamp_host_item_timeout_ms(spec.timeout);
    let outcome = gateway
        .call_placed_tool_once(facade.clone(), &spec.action, spec.args, timeout)
        .await;
    if let Ok(catalog) = gateway.list_placed_tools(facade).await {
        let state = cairn_db::storage::ExecutorDesktopAutomation {
            executor_id: spec.executor_name.clone(),
            probed_at: chrono::Utc::now().timestamp_millis(),
            health_json: None,
            verbs_json: serde_json::to_string(&catalog.tools).unwrap_or_else(|_| "[]".into()),
            probe_error: None,
        };
        if let Err(error) =
            cairn_db::storage::upsert_executor_desktop_automation(&orch.db.local, &state).await
        {
            tracing::warn!(machine = %spec.executor_name, %error, "could not cache desktop verb catalog");
        }
    }
    match outcome {
        Ok(McpCallOutcome::Complete(result)) => ItemOutcome {
            header,
            body: result.text,
            succeeded: true,
            suspended: false,
            images: result.images,
            tracked_modifications: None,
        },
        Ok(_) => ItemOutcome::failed(header, "Desktop verbs cannot suspend for follow-up input"),
        Err(error) => ItemOutcome::failed(header, format!("{}: {error}", spec.executor_name)),
    }
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

/// Answer a `cairn://models/decide` item in the caller's project scope (the
/// workspace's, outside a run), bounded by the item's timeout under the
/// host-item cap.
///
/// The facet's decide takes no cancel token, so a timeout releases the
/// caller at once and the abandoned provider call ends within its own bounds.
/// Decisions keep no invocation journal, so there is no row to write for one.
pub(super) async fn run_decide(
    orch: &Orchestrator,
    run_context: Option<&RunContext>,
    header: String,
    args: serde_json::Value,
    timeout: Option<u32>,
) -> ItemOutcome {
    let request: crate::orchestrator::DecideRequest = match serde_json::from_value(args) {
        Ok(request) => request,
        Err(error) => {
            return ItemOutcome::failed(
                header,
                format!(
                    "cairn://models/decide takes {{model, input, questions, provider?}} in payload.args_json: {error}. Read cairn://models/decide for the shape."
                ),
            )
        }
    };
    let bound = super::host_item_bound(timeout);
    let project_id = run_context.map(|context| context.project_id.as_str());
    match tokio::time::timeout(bound.limit, orch.decide_named(project_id, request)).await {
        Err(_) => ItemOutcome::failed(
            header,
            format!(
                "Decision {} The provider call it abandoned ends on its own within 30 s per request (15 s for a first-time probe).",
                bound.describe(None)
            ),
        ),
        Ok(Err(failure)) => ItemOutcome::failed(header, failure.to_string()),
        Ok(Ok(outcome)) => ItemOutcome {
            header: format!(
                "cairn://models/decide [{} · {} · {:.1}s]",
                outcome.offer.provider,
                outcome.offer.model,
                outcome.latency_ms as f64 / 1000.0
            ),
            body: serde_json::to_string_pretty(&outcome)
                .unwrap_or_else(|error| format!("the decision could not be rendered: {error}")),
            succeeded: true,
            suspended: false,
            images: Vec::new(),
            tracked_modifications: None,
        },
    }
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
    interrupt: Option<&crate::responses::InvocationInterrupt>,
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
        RunSpec::Shell {
            command,
            timeout,
            args,
        } => {
            // Only strip when a live preview exists to protect (a run context);
            // headless/CLI invocations keep exact pipeline semantics.
            let exec_command = match run_context.and_then(|_| strip_streamable_tail(&command)) {
                Some((stripped, tail)) => {
                    tail_transform = Some(tail);
                    stripped
                }
                None => command.clone(),
            };
            let mut shell_args = vec![flag.to_string(), exec_command];
            if !args.is_empty() {
                // bash -c assigns the word after the command to $0; reserve it so
                // invocation arguments naturally begin at $1 and populate "$@".
                shell_args.push("cairn-procedure".to_string());
                shell_args.extend(args);
            }
            (shell.to_string(), shell_args, timeout, Some(command), None)
        }
        RunSpec::ExecutorAction(spec) => {
            return run_executor_action(orch, header, *spec).await;
        }
        RunSpec::Decide { args, timeout } => {
            return run_decide(orch, run_context, header, args, timeout).await;
        }
        RunSpec::ResponseCall {
            response_id,
            project,
            args,
            timeout,
        } => {
            let explicit_project_id = if let Some(project) = project.as_deref() {
                orch.db
                    .local
                    .query_text(
                        "SELECT id FROM projects WHERE UPPER(key) = UPPER(?1) LIMIT 1",
                        (project.to_string(),),
                    )
                    .await
                    .ok()
                    .flatten()
            } else {
                None
            };
            let project_path = if let Some(project) = project.as_deref() {
                crate::mcp::handlers::skills_resources::project_path_by_key(orch, project)
                    .await
                    .ok()
            } else if let Some(context) = run_context {
                crate::mcp::handlers::run_context::project_path(&orch.db.local, &context.project_id)
                    .await
                    .ok()
                    .flatten()
                    .map(std::path::PathBuf::from)
            } else {
                None
            };
            let project_key = project
                .clone()
                .or_else(|| run_context.map(|context| context.project_key.clone()));
            let caller = run_context.map_or_else(
                || {
                    explicit_project_id.map_or_else(
                        || crate::responses::ResponseCaller::Internal {
                            label: "run".into(),
                        },
                        |project_id| crate::responses::ResponseCaller::Agent {
                            label: Some("run".into()),
                            run_id: request
                                .run_id
                                .clone()
                                .unwrap_or_else(|| "ambient-run".into()),
                            project_id: Some(project_id),
                            project_path: project_path.clone(),
                        },
                    )
                },
                |context| {
                    if context.agent_config_id.as_deref() == Some("workflow") {
                        crate::responses::ResponseCaller::Workflow {
                            label: context.job_name.clone(),
                            run_id: context.run_id.clone(),
                            project_id: Some(context.project_id.clone()),
                            project_path: project_path.clone(),
                        }
                    } else {
                        crate::responses::ResponseCaller::Agent {
                            label: context.job_name.clone(),
                            run_id: context.run_id.clone(),
                            project_id: Some(context.project_id.clone()),
                            project_path: project_path.clone(),
                        }
                    }
                },
            );
            // The bound and any interruption are enforced inside the
            // invocation, never around it: a call cut off from outside would
            // skip its journal row.
            let bound = super::response_item_bound(timeout);
            return match crate::responses::invoke(
                orch,
                &response_id,
                &args,
                caller,
                Some(&bound),
                interrupt,
            )
            .await
            {
                Ok(outcome) => {
                    let invocation = project_key.as_deref().map_or_else(
                        || format!("cairn://responses/{response_id}/history/{}", outcome.seq),
                        |project| {
                            format!(
                                "cairn://p/{project}/responses/{response_id}/history/{}",
                                outcome.seq
                            )
                        },
                    );
                    let cost = outcome
                        .cost
                        .map(|cost| format!(" · ${cost:.4}"))
                        .unwrap_or_default();
                    ItemOutcome {
                        header: format!(
                            "{invocation} [{} · {} · {:.1}s{}]",
                            outcome.backend,
                            outcome.model,
                            outcome.latency_ms as f64 / 1000.0,
                            cost
                        ),
                        body: outcome
                            .parsed
                            .map_or(outcome.text, |value| value.to_string()),
                        succeeded: true,
                        suspended: false,
                        images: Vec::new(),
                        tracked_modifications: None,
                    }
                }
                Err(error) => ItemOutcome::failed(header, error.to_string()),
            };
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
    if exec.credential_refused {
        return ItemOutcome::failed(header, sandbox::OPERATOR_CREDENTIAL_REFUSAL);
    }
    if let Some(denial) = exec.denial.take() {
        use crate::mcp::handlers::fence;
        // The operator credential boundary is answered before anything else: it
        // is never a fence crossing, and in the live checkout it is not the
        // read-only-checkout refusal either.
        if let sandbox::SandboxDenial::Path { path, .. } = &denial {
            if let Some(refusal) =
                crate::authorization::protected::read_refusal(&orch.config_dir, path)
            {
                return ItemOutcome::failed(header, refusal);
            }
        }
        // The live checkout is read-only and non-negotiable: an ambient checkout cwd
        // routes a denial to a clear explanation, never a fence prompt. There is
        // no run context to adjudicate and nothing the user can grant — changes
        // can only be published through an executor cell. This replaces the raw EPERM
        // chat would otherwise surface.
        if !crate::jj::is_jj_dir(std::path::Path::new(cwd)) {
            let _ = denial;
            return ItemOutcome::failed(header, READ_ONLY_CHECKOUT_DENIAL);
        }
        // A blocked touch of the workspace configuration document or the desktop
        // operator credential is not an approvable crossing: allowing one
        // re-executes this command with the sandbox switched off, which would
        // let the access actually land on the strength of a prompt that only
        // ever said "a file outside the project is being touched". That refusal
        // now lives in `fence::raise_fence`, which both this site and the
        // executor-relayed one in `fleet/mod.rs` reach, rather than here where
        // only one of them would get it.
        match fence::resolve_run_fence(orch, request).await {
            fence::RunFenceResolution::Resolved(run_id, fence_mode) => {
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
                    fence::FenceDecision::Unavailable(msg) => {
                        return ItemOutcome::failed(header, msg)
                    }
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
            fence::RunFenceResolution::Unavailable(error) => {
                return ItemOutcome::failed(header, error);
            }
            fence::RunFenceResolution::Ambient => {}
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
        server,
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
            &config.brokered(&credential_key, "mcp tool call"),
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
            body: append_link_affordance(result.text, &result.links, &server),
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

/// How many linked-resource addresses one tool result advertises before the
/// list is summarized. The server chooses how many links it returns; the
/// affordance stays a fixed-size footer either way.
const MAX_LINK_ADDRESSES: usize = 20;

/// Append the retrieval affordance for the resources a tool named by reference.
///
/// The gateway preserved each link in place with its metadata. What it could not
/// supply is the Cairn address to retrieve one, because only this layer knows
/// the server's configured name — the gateway is handed a credential key, which
/// a project-scoped server prefixes with its project path. Retrieval stays an
/// explicit later act: this is an address, not a fetch.
fn append_link_affordance(mut body: String, links: &[McpResourceDef], server: &str) -> String {
    if links.is_empty() {
        return body;
    }
    // A reference whose URI was too long to carry has no address to advertise.
    // Composing one from its truncated prefix would publish a read that is
    // guaranteed to fail, which is worse than saying it cannot be retrieved.
    let addressable: Vec<&McpResourceDef> =
        links.iter().filter(|link| link.is_retrievable()).collect();
    let withheld = links.len() - addressable.len();
    if !body.is_empty() {
        body.push('\n');
    }
    body.push_str("\nLinked resources (references only — nothing was fetched):");
    for link in addressable.iter().take(MAX_LINK_ADDRESSES) {
        let uri = CairnResource::Mcp {
            server: Some(server.to_string()),
            resource: Some(link.uri.clone()),
        }
        .to_uri();
        body.push_str(&format!("\n  - read: {uri}"));
    }
    if let Some(rest) = addressable
        .len()
        .checked_sub(MAX_LINK_ADDRESSES)
        .filter(|n| *n > 0)
    {
        body.push_str(&format!(
            "\n  … and {rest} more named above, addressed the same way under cairn://mcp/{server}/"
        ));
    }
    if withheld > 0 {
        body.push_str(&format!(
            "\n  {withheld} named above had an address too long to carry and cannot be retrieved."
        ));
    }
    body
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
    if let Some(note) = &exec.setuid_refusal {
        result.push_str("\n\n");
        result.push_str(note);
    }
    result
}

/// Build the agent-facing spawn config for work the host spawns itself: env
/// injection (MCP callback, PATH shim, uv cache, worktree VCS env, git
/// identity, per-job scratch `TMPDIR`, the `cairn:~` home URI) plus the
/// spawn's confinement. Every value here may name a path on this machine,
/// which is exactly why nothing placed on a cell may use it.
///
/// A batch, terminal, or REPL placed onto a cell does NOT pass through here:
/// the executor composes that machine's own PATH, scratch, and toolchain env,
/// and cairn-core states only what stays true wherever the process lands. That
/// seam is [`super::placed_process_env`], and the run identity below is the one
/// thing both paths must agree on — an agent shell knows who it is regardless
/// of which machine it got.
///
/// Callers layer only their own extras on top: `execute_process` adds `.stdin`
/// for the `uv run -` payload.
pub(crate) async fn build_agent_spawn_config(
    orch: &Orchestrator,
    cwd: &str,
    run_context: Option<&RunContext>,
    program: &str,
    args: &[String],
    confinement: sandbox::Confinement,
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
    // A host-spawned bun gets the same runner-provided SDK a placed batch gets,
    // installed in the Cairn-owned directory above the checkout. Best-effort on
    // purpose: most bun invocations never import a Cairn package, so a failure
    // here must not take one of those down. The cost is that an invocation that
    // does import one fails with the runtime's ordinary missing-module error,
    // with this log line as the only record of why the package was absent.
    if program == "bun" {
        if let Err(error) = crate::runtime::install_sdk_for_checkout(std::path::Path::new(cwd)) {
            log::warn!("could not install the Cairn runtime SDK for {cwd}: {error}");
        }
    }
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
            .confinement(confinement),
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
        let home_uri = super::job_home_uri(orch, &ctx.run_id, &ctx.job_id).await;
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

    // Decide this spawn's confinement. `sandbox_enabled` is false on an
    // escalated re-execution after a fence grant: the grant lifted the fence,
    // and the spawn keeps the operator credential boundary all the same.
    // Host execution only ever runs in the agent's jj residence or the project's
    // live checkout, so the path tells the truth here.
    let checkout_kind = super::sandbox_policy::RunCheckout::infer(cwd);
    let run_confinement = if sandbox_enabled {
        // Grant key: the shell command for shell items, else the program for
        // skill scripts — so a command-scoped session grant generalizes to
        // scripts too (the only crossing kind Linux produces).
        resolve_run_confinement(
            orch,
            cwd,
            checkout_kind,
            run_context.map(|c| c.run_id.as_str()),
            run_context.map(|c| c.project_id.as_str()),
            shell_command.or(Some(program)),
        )
        .await
    } else {
        RunConfinement::CredentialFloor
    };
    let fence_mode = run_confinement.fenced().map(|(_, fence)| fence);
    // Ask agents get a synthetic command-scoped macOS fallback when path recovery
    // misses; Deny agents preserve their raw fail-fast output. Ambient live-checkout
    // runs also enable fallback detection regardless of fence: the
    // checkout is read-only, so a kernel block must be recognized even when macOS
    // log recovery misses or a shell masks the exit (`... || true`). run_one
    // routes such a denial to the hard read-only-checkout message, never a fence
    // prompt — a live checkout is non-grantable, so enabling detection cannot
    // synthesize a grant.
    let command_scoped_fallback =
        checkout_kind.is_project_live() || matches!(fence_mode, Some(Fence::Ask));
    let sandboxed = fence_mode.is_some();
    let confinement = run_confinement.into_confinement();
    let floored = matches!(confinement, sandbox::Confinement::CredentialFloor);
    let spawn_started = std::time::SystemTime::now();
    let applied_confinement = confinement.clone();

    let mut spawn_config =
        build_agent_spawn_config(orch, cwd, run_context, program, args, confinement).await;

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

    let stdout_handle = drain_host_pipe(
        stdout,
        "stdout",
        HostPipeSinks {
            content: stdout_content.clone(),
            combined: combined_buffer.clone(),
            last_output_at: last_output_at.clone(),
            emitter: services.emitter.clone(),
            run_id: run_context.map(|r| r.run_id.clone()),
            tool_use_id: tool_use_id.clone(),
        },
    );
    let stderr_handle = drain_host_pipe(
        stderr,
        "stderr",
        HostPipeSinks {
            content: stderr_content.clone(),
            combined: combined_buffer.clone(),
            last_output_at: last_output_at.clone(),
            emitter: services.emitter.clone(),
            run_id: run_context.map(|r| r.run_id.clone()),
            tool_use_id: tool_use_id.clone(),
        },
    );

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
            if sandboxed || floored {
                let partial = {
                    let o = captured(&stdout_content);
                    let e = captured(&stderr_content);
                    match (o.is_empty(), e.is_empty()) {
                        (false, false) => format!("{o}\n{e}"),
                        (false, true) => o,
                        (true, false) => e,
                        (true, true) => String::new(),
                    }
                };
                classify_host_denial(
                    sandboxed,
                    None,
                    &partial,
                    child_pid,
                    spawn_started,
                    command_scoped_fallback,
                    cwd,
                )
            } else {
                None
            }
        } else {
            None
        };
        let (denial, credential_refused) = split_host_denial(denial);
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

        let stdout_str = captured(&stdout_content);
        let stderr_str = captured(&stderr_content);
        return Ok(ExecOutput {
            stdout: stdout_str,
            stderr: stderr_str,
            exit_code: None,
            timed_out: true,
            denial,
            credential_refused,
            setuid_refusal: None,
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

    let stdout_str = captured(&stdout_content);
    let stderr_str = captured(&stderr_content);

    // Detect a kernel sandbox denial so the caller can drive the worktree fence.
    let denial = if sandboxed || floored {
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
        classify_host_denial(
            sandboxed,
            exit_code,
            &combined,
            child_pid,
            spawn_started,
            command_scoped_fallback,
            cwd,
        )
    } else {
        None
    };
    let (denial, credential_refused) = split_host_denial(denial);
    let setuid_refusal = sandbox::setuid_refusal_note(
        &applied_confinement,
        exit_code,
        &format!("{stdout_str}\n{stderr_str}"),
    );

    Ok(ExecOutput {
        stdout: stdout_str,
        stderr: stderr_str,
        exit_code,
        timed_out: false,
        denial,
        credential_refused,
        setuid_refusal,
    })
}

/// What refused a host spawn: the operator credential boundary, or an
/// ordinary fence denial.
enum HostDenial {
    OperatorCredential,
    Fence(sandbox::SandboxDenial),
}

/// Classify a host spawn's denial by what it was refused.
///
/// A credential read is recognized from the attempted path (the kernel log's,
/// or any path the output names, resolved through symlinks), so it is the
/// boundary's refusal even when the log lookup misses. A spawn carrying only
/// the floor reports nothing else: the floor refuses nothing else.
fn classify_host_denial(
    fenced: bool,
    exit_code: Option<i32>,
    output: &str,
    pid: Option<u32>,
    since: std::time::SystemTime,
    command_scoped_fallback: bool,
    cwd: &str,
) -> Option<HostDenial> {
    let cwd = Some(std::path::Path::new(cwd));
    if !fenced {
        return sandbox::detect_credential_floor_denial(exit_code, output, pid, since, cwd)
            .map(|_| HostDenial::OperatorCredential);
    }
    let detected = sandbox::detect_denial(exit_code, output, pid, since, command_scoped_fallback)?;
    if sandbox::credential_denial(output, cwd, Some(&detected)).is_some() {
        return Some(HostDenial::OperatorCredential);
    }
    Some(HostDenial::Fence(detected))
}

fn split_host_denial(denial: Option<HostDenial>) -> (Option<sandbox::SandboxDenial>, bool) {
    match denial {
        Some(HostDenial::OperatorCredential) => (None, true),
        Some(HostDenial::Fence(denial)) => (Some(denial), false),
        None => (None, false),
    }
}

/// Read back what a pipe reader captured.
///
/// The reader appends newline-terminated text, so the buffer carries a trailing
/// terminator that the captured value never had; one is stripped here rather
/// than by the reader, because the reader is mid-stream and cannot know which
/// newline is the last one.
fn captured(buffer: &Arc<Mutex<String>>) -> String {
    let mut text = buffer.lock().unwrap().clone();
    if text.ends_with('\n') {
        text.pop();
    }
    text
}

/// Everything one host pipe's output has to reach.
struct HostPipeSinks {
    content: Arc<Mutex<String>>,
    combined: Option<Arc<Mutex<VecDeque<u8>>>>,
    last_output_at: Option<Arc<Mutex<SystemTime>>>,
    emitter: Arc<dyn crate::services::EventEmitter>,
    run_id: Option<String>,
    tool_use_id: String,
}

/// Drain one host process pipe into every sink that observes it.
///
/// The sinks are the point: the captured content becomes the item's result text,
/// the combined buffer is what a promoted terminal adopts as its scrollback, and
/// the live payload is what the UI renders as the command runs. They all read
/// the *same* scrubbed text here, so they cannot fork into a raw and a redacted
/// version of one command's output.
///
/// Reading is line-oriented because that is the shape both the capture and the
/// live payload want, but scrubbing is not: one streaming scrubber spans the
/// whole pipe, because a registered value containing a newline would otherwise
/// straddle two lines and be invisible to a per-line pass. The scrubber releases
/// on any byte outside the registered alphabet, so a newline-terminated line
/// still goes out the moment it arrives.
fn drain_host_pipe<R: std::io::Read + Send + 'static>(
    pipe: Option<R>,
    stream: &'static str,
    sinks: HostPipeSinks,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let Some(pipe) = pipe else { return };
        let mut scrubber = crate::security::StreamingScrubber::new();
        let publish = |settled: Vec<u8>| {
            if settled.is_empty() {
                return;
            }
            let text = String::from_utf8_lossy(&settled).into_owned();
            sinks.content.lock().unwrap().push_str(&text);
            record_combined_output(&sinks.combined, &sinks.last_output_at, &text);
            if let Some(run_id) = sinks.run_id.as_ref() {
                let _ = sinks.emitter.emit(
                    "run-output",
                    serde_json::to_value(RunOutputPayload {
                        run_id: run_id.clone(),
                        tool_use_id: sinks.tool_use_id.clone(),
                        chunk: text,
                        stream: stream.to_string(),
                    })
                    .unwrap_or_default(),
                );
            }
        };
        let reader = BufReader::new(pipe);
        for line in reader.lines().map_while(Result::ok) {
            // `lines()` strips the terminator, so it goes back on: the scrubber
            // is scrubbing a stream, not a list of lines, and the separator is
            // part of it.
            publish(scrubber.push(format!("{line}\n").as_bytes()));
        }
        // End of pipe. A reader that returns without this drops whatever the
        // scrubber was still withholding, which is the tail of the command's
        // output.
        publish(scrubber.flush());
    })
}

fn record_combined_output(
    combined: &Option<Arc<Mutex<VecDeque<u8>>>>,
    last_output_at: &Option<Arc<Mutex<SystemTime>>>,
    text: &str,
) {
    if let Some(cb) = combined {
        if let Ok(mut b) = cb.lock() {
            b.extend(text.as_bytes());
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

    /// The floor's setuid explanation is appended, never substituted: the
    /// command's stderr and its exit status reach the agent exactly as before.
    #[test]
    fn a_setuid_refusal_note_follows_the_untouched_output_and_exit_code() {
        let exec = ExecOutput {
            stdout: String::new(),
            stderr: "/bin/bash: /bin/ps: Operation not permitted".into(),
            exit_code: Some(126),
            timed_out: false,
            denial: None,
            credential_refused: false,
            setuid_refusal: Some("Cairn: the floor refused `/bin/ps`.".into()),
        };
        assert_eq!(
            format_exec_body(&exec, 1_000),
            "stderr:\n/bin/bash: /bin/ps: Operation not permitted\n\nExit code: 126\n\nCairn: the floor refused `/bin/ps`."
        );
    }

    #[tokio::test]
    async fn host_spawn_home_uses_the_team_run_database() {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::{MigrationRunner, SearchIndex, TURSO_MIGRATIONS};

        let temp = tempfile::tempdir().unwrap();
        let private = Arc::new(LocalDb::open(temp.path().join("private.db")).await.unwrap());
        let team = Arc::new(LocalDb::open(temp.path().join("team.db")).await.unwrap());
        for db in [&private, &team] {
            MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
                .run(db)
                .await
                .unwrap();
        }
        let dbs = Arc::new(DbState::new(
            private.clone(),
            Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap()),
        ));
        dbs.insert_team_db_for_test("team1", team.clone()).await;
        for sql in [
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','default','P','team-home','/tmp',1,1)",
            "INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at) VALUES ('t','p','design','active','none',1,1)",
            "INSERT INTO jobs(id, thread_id, project_id, node_name, status, uri_segment, created_at, updated_at) VALUES ('j','t','p','Thread','running','thread',1,1)",
            "INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at) VALUES ('team1~00000000-0000-4000-8000-000000000001','p','j','live',1,1)",
        ] {
            team.execute(sql, ()).await.unwrap();
        }
        assert!(crate::jobs::queries::home_uri_for_job(&private, "j")
            .await
            .unwrap()
            .is_none());
        let request = crate::mcp::types::McpCallbackRequest {
            run_id: Some("team1~00000000-0000-4000-8000-000000000001".into()),
            ..Default::default()
        };
        let (ctx, _) = crate::mcp::handlers::run_context::lookup_run_routed(&dbs, &request)
            .await
            .unwrap();
        let orch = Orchestrator::builder(
            dbs,
            Arc::new(TestServicesBuilder::new().build()),
            temp.path().join("config"),
        )
        .build();
        let config = build_agent_spawn_config(
            &orch,
            temp.path().to_str().unwrap(),
            Some(&ctx),
            "bash",
            &[],
            sandbox::Confinement::CredentialFloor,
        )
        .await;
        assert_eq!(
            config.env.get("CAIRN_HOME_URI").map(String::as_str),
            Some("cairn://p/team-home/design")
        );
    }

    fn link(uri: &str) -> McpResourceDef {
        McpResourceDef {
            uri: uri.to_string(),
            ..McpResourceDef::default()
        }
    }

    #[test]
    fn link_affordance_addresses_each_reference_through_the_gateway() {
        let body = append_link_affordance(
            "two artifacts".to_string(),
            &[
                link("https://files.example/report.pdf"),
                link("file:///srv/out/trace.bin"),
            ],
            "bmd",
        );
        assert!(body.starts_with("two artifacts"), "{body}");
        assert!(
            body.contains("  - read: cairn://mcp/bmd/https://files.example/report.pdf"),
            "{body}"
        );
        assert!(
            body.contains("  - read: cairn://mcp/bmd/file:///srv/out/trace.bin"),
            "{body}"
        );
    }

    #[test]
    fn a_reference_with_no_usable_address_is_never_advertised_as_readable() {
        // Composing an address out of a truncated prefix would publish a read
        // that asks the server for a URI it never named.
        let withheld = McpResourceDef {
            uri: "data:application/pdf;base64,QUJD".to_string(),
            uri_truncated: true,
            ..McpResourceDef::default()
        };
        let body = append_link_affordance(
            "one of each".to_string(),
            &[link("https://files.example/ok.pdf"), withheld],
            "bmd",
        );
        assert!(
            body.contains("  - read: cairn://mcp/bmd/https://files.example/ok.pdf"),
            "{body}"
        );
        assert!(!body.contains("data:application/pdf"), "{body}");
        assert!(
            body.contains("1 named above had an address too long to carry"),
            "{body}"
        );
    }

    #[test]
    fn link_affordance_is_a_fixed_size_footer_and_absent_without_links() {
        let untouched = append_link_affordance("plain text".to_string(), &[], "bmd");
        assert_eq!(untouched, "plain text");

        // A server chooses how many links it returns; the footer does not grow
        // without bound because of that choice.
        let many: Vec<McpResourceDef> = (0..500)
            .map(|n| link(&format!("https://files.example/{n}.bin")))
            .collect();
        let body = append_link_affordance(String::new(), &many, "bmd");
        assert_eq!(body.matches("  - read: ").count(), MAX_LINK_ADDRESSES);
        assert!(
            body.contains(
                "… and 480 more named above, addressed the same way under cairn://mcp/bmd/"
            ),
            "{body}"
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

    fn cached_desktop(
        verbs_json: &str,
        probe_error: Option<&str>,
    ) -> cairn_db::storage::ExecutorDesktopAutomation {
        cairn_db::storage::ExecutorDesktopAutomation {
            executor_id: "bglab-win".into(),
            probed_at: 0,
            health_json: None,
            verbs_json: verbs_json.into(),
            probe_error: probe_error.map(str::to_string),
        }
    }

    /// A real catalog is the only thing that may refute a verb. The other two
    /// shapes are a machine whose probe failed and one nobody has probed, and
    /// treating either as "advertises nothing" is how a dropped link turns into
    /// a refusal claiming the verb does not exist.
    #[test]
    fn only_a_probed_catalog_can_refute_a_verb() {
        let probed = cached_desktop(r#"[{"name":"look","input_schema":{}}]"#, None);
        assert_eq!(
            advertised_verbs(&probed),
            Some(vec!["look".to_string()]),
            "a probed catalog is evidence"
        );

        assert_eq!(
            advertised_verbs(&cached_desktop("[]", Some("Transport closed"))),
            None,
            "a failed probe proves nothing about the machine's verbs"
        );
        assert_eq!(
            advertised_verbs(&cached_desktop("[]", None)),
            None,
            "an empty catalog is not evidence of absence"
        );
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
