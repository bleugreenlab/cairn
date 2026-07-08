//! The stdio MCP server: the `CairnCmd` service, its three `#[tool]` verbs
//! (`write`/`read`/`run`), the HTTP callback plumbing, and the `ServerHandler`
//! implementation (tool listing, resource reads).
use rmcp::{
    handler::server::tool::{Parameters, ToolCallContext, ToolRouter},
    model::{
        CallToolRequestParam, CallToolResult, Content, Implementation, ListToolsResult,
        PaginatedRequestParam, ProtocolVersion, ReadResourceRequestParam, ReadResourceResult,
        ResourceContents, ServerCapabilities, ServerInfo,
    },
    service::RequestContext,
    tool, tool_router, RoleServer, ServerHandler,
};
use serde::Deserialize;
use std::future::Future;
use std::sync::{Arc, Mutex};

use cairn_common::protocol::{CallbackRequest, CallbackResponse};
use cairn_common::read::{ReadBatchEnvelope, RunBatchEnvelope};
use cairn_common::uri::{parse_uri as parse_cairn_uri, CairnResource};

use crate::output::{
    assemble_reminders, cap_run_result, cap_text_result, change_callback_result,
    http_status_error_message, redact_command, truncate_chars, CallbackOutcome,
};
use crate::schemas::{validate_run_input, AgentInfo, ChangeInput, ReadFileInput, RunInput};
use crate::timeouts::callback_timeout;

/// Cairn MCP Server - tools for Claude to interact with Cairn during planning
#[derive(Clone)]
pub(crate) struct CairnCmd {
    callback_url: Arc<String>,
    /// Current working directory - used by backend to identify the active run
    pub(crate) cwd: Arc<String>,
    /// Run ID - preferred method to identify the active run (avoids cwd ambiguity)
    pub(crate) run_id: Option<Arc<String>>,
    /// Shared secret (base64-encoded string from env var, sent directly as bearer token)
    mcp_secret: Option<Arc<String>>,
    /// Stable shorthand root for `cairn:~/...` resolution.
    pub(crate) home_uri: Option<Arc<String>>,
    /// Last successful resource read URI (navigation context).
    pub(crate) base_uri: Arc<Mutex<Option<String>>>,
    tool_router: ToolRouter<Self>,
    /// Available agents for task tool description
    available_agents: Vec<AgentInfo>,
}

#[tool_router]
impl CairnCmd {
    #[cfg(test)]
    fn new(
        callback_url: String,
        cwd: String,
        run_id: Option<String>,
        mcp_secret: Option<String>,
        available_agents: Vec<AgentInfo>,
    ) -> Self {
        Self::new_with_home_uri(
            callback_url,
            cwd,
            run_id,
            mcp_secret,
            available_agents,
            None,
        )
    }

    pub(crate) fn new_with_home_uri(
        callback_url: String,
        cwd: String,
        run_id: Option<String>,
        mcp_secret: Option<String>,
        available_agents: Vec<AgentInfo>,
        home_uri: Option<String>,
    ) -> Self {
        let home_uri = home_uri.map(Arc::new);
        let base_uri = Arc::new(Mutex::new(home_uri.as_ref().map(|uri| uri.to_string())));

        Self {
            callback_url: Arc::new(callback_url),
            cwd: Arc::new(cwd),
            run_id: run_id.map(Arc::new),
            mcp_secret: mcp_secret.map(Arc::new),
            home_uri,
            base_uri,
            tool_router: Self::tool_router(),
            available_agents,
        }
    }
    /// Apply ordered file and resource mutations through the canonical change carrier.
    #[tool(
        description = r#"Apply ordered file and resource mutations through one carrier. Items in `changes` apply in input order.

Targets:
- File: `file:path/to/file` (worktree-relative; bare `file:` is the worktree root, `file:/abs` is absolute). Every item carries its keys under `payload`: create/replace/append take `payload:{content}`; patch takes `payload:{diff}` OR `payload:{old_string, new_string}` (optional `replace_all`); unified_patch takes `payload:{patch}` containing a native `*** Begin Patch` envelope with add/update/delete sections; delete needs no payload; `rename` takes `payload:{new_name, and exactly one of old_name | symbol_at}` and performs an ast-grep-backed structural rename of an identifier across the worktree, applying every edit site (and any module file move) as one commit. A bare `rename` returns a preview by default; land it with the `apply` round-trip. For a structural gap in a patch, put `~~*~~` in `old_string` between a head and tail anchor to replace everything in between — span by default, balanced only with the contiguous delimiter-pair token (`{~~*~~}`, delimiters immediately adjacent to the marker; the own-line `{\n~~*~~\n}` form spans). Multiple `~~*~~` markers span non-contiguous regions; escape a literal marker with a leading backslash. Example: `payload:{old_string: "fn validate(t) {~~*~~}", new_string: "fn validate(t) { verify(t); }"}`.
- Resource: canonical `cairn://p/PROJECT/...` or home-relative `cairn:~/...`. Modes: create, append, patch, replace, delete.

Don't guess a resource's payload: `read` the target URI first — its affordance block lists the exact actions (mode + required/optional payload keys + a copy-paste example) and read filters. If a mutation is unsupported or missing a required key, the rejection enumerates what the resource accepts.

Notes: `atomic` defaults to false: matching items apply, failed items are reported in `failures`, and `commit_msg` commits only files that applied; set `atomic:true` for fail-fast apply behavior. `cairn:~/...` resolves against your running node; `preview:true` returns an `apply_uri` to re-submit with `mode=apply`; `commit_msg` is REQUIRED whenever the batch touches a file target (`\"^\"` amends the previous commit; without a commit_msg the worktree is restored to HEAD) — uncommitted worktree edits are lost if the worktree is cleaned up; task/question appends to `cairn:~/tasks` and `cairn:~/questions` block until results return. Change reports list per-item `applied` and `failures`."#
    )]
    pub(crate) async fn write(
        &self,
        params: Parameters<ChangeInput>,
        meta: rmcp::model::Meta,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        // Pooled Codex call (CAIRN-2549): Codex injects the originating thread as
        // `_meta.threadId`. When present, forward it as `thread_id` and forward
        // `cairn:~/` targets RAW (the host expands them from the thread-resolved
        // run). Absent — every non-pooled caller — behaviour is unchanged.
        let thread_id = Self::thread_id_from_meta(&meta);
        let pooled = thread_id.is_some();

        // Validate the raw input ourselves, in one pass, before any rewrite or
        // forward. This owns the error text the model sees (the rmcp-facing
        // struct is lenient precisely so control reaches here) and returns every
        // problem at once with no server round-trip.
        let raw = serde_json::to_value(&input).unwrap_or(serde_json::Value::Null);
        let payload_bytes = serde_json::to_vec(&input).map(|v| v.len()).unwrap_or(0);
        let change_count = input.changes.as_ref().map(|c| c.len()).unwrap_or(0);
        let changes_present = input.changes.is_some();
        tracing::info!(
            "write called: {} changes, changes_present={}, payload {} bytes",
            change_count,
            changes_present,
            payload_bytes
        );

        let validation_errors = cairn_common::change_validation::validate_change_value(&raw);
        if !validation_errors.is_empty() {
            let text =
                cairn_common::change_validation::render_validation_errors(&validation_errors);
            return Ok(CallToolResult::success(vec![Content::text(text)]));
        }

        let rewritten = match self.rewrite_change_targets_with(&input, pooled) {
            Ok(rewritten) => rewritten,
            Err(message) => return Ok(CallToolResult::success(vec![Content::text(message)])),
        };

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "write".to_string(),
            payload: serde_json::to_value(&rewritten).unwrap_or_default(),
            tool_use_id: None,
            thread_id,
        };

        let outcome = self.call_tauri_full(&request).await;
        Ok(change_callback_result(
            outcome,
            self.callback_url.as_str(),
            "write",
        ))
    }

    /// Read one or more files, directories, or Cairn resources in a single call.
    #[tool(
        description = r#"Read one or more files, directories, or Cairn resources in a single call. `paths` is an ordered, non-empty array of target URIs; results return in order, each under a `=== <uri> [suffix] ===` header (the optional bracketed suffix is a terse count drawn from a small closed vocabulary: `lines A–B of T`, `N matches[ in M files]`, `M files`, or `truncated`).

Targets (mix freely within one call):
- File: `file:` (worktree root), `file:src/lib.rs` (worktree-relative), `file:/abs/path` (absolute / global)
- Resource: canonical `cairn://p/PROJECT[/NUMBER[/EXEC/NODE[/sub]]]` plus collections `/issues`, `/messages`, `/changed`, `/references`, and `/references/NAME`. `cairn:~/...` resolves against the run home.
- Web/PDF: `http(s)://...` URLs and local `.pdf` paths return markdown via the active web-fetch provider (raw capped markdown; no extraction prompt). The default built-in provider is a plain HTTP fetch (HTML→markdown); PDF extraction needs a configured provider such as `bmd`. Providers are configured in Settings → Web Services.
- Web search: `cairn://websearch?q=QUERY` runs the query through the active web-search provider (configured in Settings → Web Services) and returns ranked results as markdown; everything after `q=` is the literal query, so spaces are fine.

Per-target scoping rides in each URI's query string — append `?key=value&...`:
- Files: `offset=N` skips N leading lines (0-based — line N is at `offset N−1`); `limit=N` returns N lines; `offset=-N` returns the last N lines (tail). `branch=REF` reads file content from a jj-resolved bookmark, commit/change id, or node URI without checking out that branch; it is per-target only and applies only to `file:` targets. `glob=PATTERN` selects matched files (`output_mode=files_with_matches|content|count`; a directory grep defaults to `files_with_matches`, a single-file grep to `content`). `issue_history=true|verbose` appends issues that touched the file.
- Grep is universal: `grep=REGEX` matches over ANY target's rendered text. A file tree greps with ripgrep and labels each line `path:N:text`; a single file or any rendered resource/web body greps in memory and drops the path prefix (`N:text`). Modifiers: `-i` (case-insensitive), `-A=N`/`-B=N`/`-C=N`/`context=N` (context lines), `head_limit=N` to cap matches (`limit=N` aliases it under grep). `offset` is NOT allowed with grep — paginate matches with `head_limit`. The header suffix counts real matches: `[N matches]` for one body, `[N matches in M files]` for a tree.
- Structural code: `ast=PATTERN` runs an ast-grep pattern over any file/dir target and renders the same `path:N:line` rows as grep (composes with `glob`). A pattern is real code with metavariables — `$VAR` matches one node, `$$$` a run — e.g. `ast=fn $NAME($$$) { $$$ }` (Rust) or `ast=console.log($$$)` (TS); it is NOT a tree-sitter node-kind name like `function_declaration`. `outline` (bare flag) renders a file/dir signature skeleton. Symbol navigation lives on the `symbols` resource: `cairn://p/PROJECT/NUMBER/EXEC/NODE/symbols/{name}?op=definition|references|callers|implementations` (node-scoped) or `cairn://p/PROJECT/symbols/{name}` (project-checkout fallback); absent `op` is an overview (definition site + signature + reference count), `in=GLOB` scopes it.
- Escaping: `&` and `+` are literal inside a value (so `grep=&mut self` and `grep=\d+` work as written); use `%26` for a literal `&` that immediately precedes a recognized key token
- Resources: `offset=N` skips rendered resource lines client-side; `limit=N` is resource-specific unless reading a single transcript event, where it is a client-side line count
- `/issues`: `status=backlog,active,...` (comma-separated), `limit=N`, `sort=updated_desc|created_asc|...`, `ready=true|false`
- `/messages`: `before=`, `after=`, `since=EPOCH`, `limit=N`
- Project search: `cairn://p/PROJECT?search=QUERY&limit=N&since=EPOCH`

Partial failures never abort: a target that errors shows its message inline as that target's block, and every requested target still contributes a block. A multi-target read shares a single ~45k-char total budget across targets (water-filled so small targets render whole and large ones fair-share); every requested target is included, and a windowed or truncated segment carries an always-valid `continue:` footer — `[lines A–B of T — output truncated to fit budget; continue: ...]`, advancing `offset`/`head_limit`, or a `char_offset=` resume when a single line is itself larger than the budget."#
    )]
    pub(crate) async fn read(
        &self,
        params: Parameters<ReadFileInput>,
        meta: rmcp::model::Meta,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        let thread_id = Self::thread_id_from_meta(&meta);
        let pooled = thread_id.is_some();
        if input.paths.is_empty() {
            return Ok(CallToolResult::error(vec![Content::text(
                "read requires a non-empty `paths` array (one or more target URIs).".to_string(),
            )]));
        }
        tracing::info!("read called: {} paths", input.paths.len());

        // Resolve each target client-side (home-URI + base-URI shorthand). Web
        // URLs pass through unresolved — the backend classifies and fetches them.
        // A target that fails resolution is forwarded as-is so the backend emits
        // it as that target's inline error block (partial failure never aborts).
        let resolved: Vec<String> = input
            .paths
            .iter()
            .map(|path| {
                if path.starts_with("http://") || path.starts_with("https://") {
                    path.clone()
                } else {
                    self.resolve_read_target_with(path, pooled)
                        .unwrap_or_else(|_| path.clone())
                }
            })
            .collect();

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "read_batch".to_string(),
            payload: serde_json::json!({ "paths": resolved }),
            tool_use_id: None,
            thread_id,
        };

        let outcome = self.call_tauri_full(&request).await;

        // The handler content is the bare envelope JSON — augmentation reminders
        // ride separately, so this parses cleanly with no trailing-text split.
        let envelope = match serde_json::from_str::<ReadBatchEnvelope>(&outcome.result) {
            Ok(envelope) => envelope,
            Err(_) => {
                // Transport/parse failure: surface the raw result as text.
                return Ok(CallToolResult::success(vec![Content::text(
                    self.relativize_cairn_uris_in_text(&outcome.result),
                )]));
            }
        };
        let text = self.relativize_cairn_uris_in_text(&envelope.text);
        let text = assemble_reminders(text, &outcome.reminders);

        let mut blocks: Vec<Content> = Vec::with_capacity(1 + envelope.images.len());
        blocks.push(Content::text(text));
        for image in envelope.images {
            blocks.push(Content::image(image.data, image.mime_type));
        }
        Ok(CallToolResult::success(blocks))
    }

    /// Execute an ordered batch of shell commands, inline code, and skill-script
    /// invocations, synchronously. Parallel by default; `sequential: true` runs
    /// in order. Long-running terminals are managed by `write` on terminals.
    #[tool(
        description = "Execute an ordered batch of synchronous invocations. `commands` is a non-empty array; each item is exactly one of: a shell `command`; a `target` skill-script URI (cairn://skills/<id>/scripts/<name>) with optional `payload.args`; a `target` external MCP tool (cairn://mcp/<server>/<tool>) with its named arguments in `payload.args_json` (e.g. `{target:\"cairn://mcp/axon/look\", payload:{args_json:{app:\"Finder\"}}}` — read cairn://mcp/<server> for each tool's arg shape); or inline `code` with a required `interpreter` (e.g. `{code:\"console.log(1)\", interpreter:\"typescript\"}`). Inline `code` is the default way to run code that isn't a CLI invocation: the interpreter execs the source directly, so there is no shell and no quoting. `typescript`/`ts` and `javascript`/`js` run via bun with the worktree `node_modules` and zero-config `@cairn/sdk` importable; `python`/`py` runs through the bundled `uv`, so a PEP 723 `# /// script` dependency block resolves into a cached environment and a worktree `pyproject.toml`/`uv.lock` project env is picked up automatically (falling back to plain `python3` when uv is absent). Add `repl:<slug>` to an inline `code` item to evaluate it in a stateful REPL session — create it first with `write cairn:~/repl/<slug> {interpreter:python}` — so variables, imports, and defs persist across `run` calls (its state is lost if the REPL dies). Prefer inline code over wrapping a one-liner in `sh -c` / `python3 -c` / `bun -e`. Keep inline code synchronous and run-to-completion; long-running or background code belongs to terminal resources (and durable workflow scripts). Items run in PARALLEL by default; set `sequential: true` for ordered execution (fail-fast unless `stop_on_error: false`). Output is composed under `=== <header> ===` headers in input order. If a successful worktree-bound batch dirties the tree, `commit_msg` is required and commits all worktree changes ONCE after the batch succeeds; `^` amends. Without a commit_msg, a batch that dirties the worktree is restored to HEAD. `branch` runs the whole batch in the live checkout holding that branch/ref (for example `main` or `agent/CAIRN-123-builder-0`); it rejects `commit_msg`, refuses to run if the target checkout has uncommitted tracked changes, leaves untracked build output as warm cache, warns and leaves tracked changes in place if any appear during the run so concurrent edits are not discarded, and errors if no live checkout exists. Not for long-lived/background processes — use a terminal resource via `write` for those. Each item's `timeout` (ms, default 120000, max 600000) is honored by the host: when an item exceeds it, that item is terminated and its result block reports the timeout with whatever output it produced so far — the batch is never aborted with no output. Outer layers (the callback transport and the agent's own tool timeout) are sized strictly above the host budget, so a legal batch always runs to completion: sequential batches get the sum of per-item timeouts, parallel batches the max."
    )]
    async fn run(
        &self,
        params: Parameters<RunInput>,
        meta: rmcp::model::Meta,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        let thread_id = Self::thread_id_from_meta(&meta);

        if let Err(msg) = validate_run_input(&input) {
            return Ok(CallToolResult::error(vec![Content::text(msg)]));
        }

        let first = input
            .commands
            .first()
            .and_then(|item| item.command.clone().or_else(|| item.target.clone()))
            .unwrap_or_default();
        let redacted = redact_command(&first);
        tracing::info!(
            "run called: {} item(s), first={}",
            input.commands.len(),
            &redacted[..redacted.len().min(100)]
        );

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "run".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
            thread_id,
        };

        let outcome = self.call_tauri_full(&request).await;
        // The run handler returns a RunBatchEnvelope (composed text + image
        // content blocks) like read_batch; parse it and lift each image into its
        // own content block after the text so an image-bearing MCP tool result
        // (e.g. an Axon look screenshot) reaches the agent. A transport/parse
        // failure falls back to the raw text.
        let envelope =
            serde_json::from_str::<RunBatchEnvelope>(&outcome.result).unwrap_or_else(|_| {
                RunBatchEnvelope {
                    text: outcome.result.clone(),
                    images: Vec::new(),
                }
            });
        let text = assemble_reminders(cap_run_result(&envelope.text), &outcome.reminders);
        let mut blocks: Vec<Content> = Vec::with_capacity(1 + envelope.images.len());
        blocks.push(Content::text(text));
        for image in envelope.images {
            blocks.push(Content::image(image.data, image.mime_type));
        }
        Ok(CallToolResult::success(blocks))
    }
}

impl CairnCmd {
    /// Extract Codex's per-call thread id from a `tools/call` request's `_meta`
    /// (CAIRN-2549). Codex injects it under the `"threadId"` key for every tool
    /// call from a pooled app-server thread; other callers send no such meta and
    /// this returns `None`.
    fn thread_id_from_meta(meta: &rmcp::model::Meta) -> Option<String> {
        meta.get("threadId")
            .and_then(|v| v.as_str())
            .map(str::to_string)
    }

    /// Call the Tauri callback server and return the full outcome (handler
    /// result plus augmentation reminders). Verbs assemble reminders into the
    /// model-visible text at the edge, after parsing any structured result.
    pub(crate) async fn call_tauri_full(&self, request: &CallbackRequest) -> CallbackOutcome {
        let client = match reqwest::Client::builder()
            .timeout(callback_timeout(request))
            .build()
        {
            Ok(client) => client,
            Err(e) => {
                tracing::error!("Failed to build HTTP client: {}", e);
                return CallbackOutcome {
                    result: format!("Error building HTTP client: {}", e),
                    ..Default::default()
                };
            }
        };
        let mut req = client.post(self.callback_url.as_str()).json(request);

        if let Some(secret) = &self.mcp_secret {
            req = req.header("Authorization", format!("Bearer {}", secret));
        }

        let request_bytes = serde_json::to_vec(request).map(|v| v.len()).unwrap_or(0);
        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                let http_ok = status.is_success();
                match resp.text().await {
                    // A non-2xx callback (e.g. HTTP 413 when the body exceeds the
                    // callback limit) is not a `CallbackResponse`; surface the
                    // status and request size explicitly instead of failing to
                    // parse the error body into an opaque message.
                    Ok(text) if !http_ok => {
                        tracing::error!(
                            "MCP callback returned HTTP {} for tool {}: {}",
                            status,
                            request.tool,
                            truncate_chars(&text, 500)
                        );
                        CallbackOutcome {
                            result: http_status_error_message(
                                status.as_u16(),
                                status.canonical_reason().unwrap_or("error"),
                                request_bytes,
                            ),
                            ..Default::default()
                        }
                    }
                    Ok(text) => match serde_json::from_str::<CallbackResponse>(&text) {
                        Ok(r) => CallbackOutcome {
                            result: r.result,
                            reminders: r.reminders,
                            transport_ok: http_ok,
                        },
                        Err(e) => {
                            tracing::error!(
                                "Failed to parse response (status {}): {} - body: {}",
                                status,
                                e,
                                truncate_chars(&text, 500)
                            );
                            CallbackOutcome {
                                result: format!(
                                    "Error parsing response: {} (body: {})",
                                    e,
                                    truncate_chars(&text, 200)
                                ),
                                ..Default::default()
                            }
                        }
                    },
                    Err(e) => CallbackOutcome {
                        result: format!("Error reading response body: {}", e),
                        ..Default::default()
                    },
                }
            }
            Err(e) => CallbackOutcome {
                result: format!("Error calling Tauri: {}", e),
                ..Default::default()
            },
        }
    }
}

impl ServerHandler for CairnCmd {
    fn get_info(&self) -> ServerInfo {
        let mut instructions = "Cairn MCP server for agent orchestration.\n\n\
             Issue tools:\n\
             - read: Read issue and node resources (cairn://p/PROJECT/NUMBER, cairn://p/PROJECT/NUMBER/EXEC/NODE/chat)\n\n\
             Implementation tools:\n\
             - read: Read file contents, directory listings, canonical cairn://p/... resources, and read-query projections such as `src?glob=**/*.rs`, `src?grep=foo&glob=*.ts`, or `cairn://p/CAIRN?search=uri&limit=5`\n\\

             - write: Apply ordered file and cairn:// resource mutations — files, terminals, delegated tasks (append to a node's tasks collection), ephemeral agent calls (append to a node's calls collection — one prompt in, schema-validated JSON out), user questions (append to a node's questions collection), and output artifacts (write/patch your node's artifact via cairn:~/<name>)\n\
             - run: Execute an ordered batch of synchronous shell commands, inline code (`{code, interpreter}` — typescript/javascript via bun with zero-config `@cairn/sdk`, or python via bundled uv with PEP 723 deps and project-env pickup), and skill-script targets (parallel by default; `sequential` for ordered). Add `repl:<slug>` to an inline code item to persist state across calls in a stateful REPL (create via `write cairn:~/repl/<slug>`). If a successful worktree-bound run dirties the tree, pass `commit_msg` to commit it (`^` amends). Without a commit_msg, a run that dirties the worktree is restored to HEAD.\n\n\
             Output artifact: when your node declares an output schema, write your result with `write` to `cairn:~/<name>` (mode create, then mode patch to revise). The payload is validated against the schema server-side. Do this as the last action of your turn; the write pauses the run for user review, and the session resumes on their reply."
            .to_string();

        // Add available agents to instructions
        if !self.available_agents.is_empty() {
            instructions.push_str(
                "\n\nAvailable agents (use as subagentType when appending to a node's tasks collection):\n",
            );
            for agent in &self.available_agents {
                instructions.push_str(&format!("- {}: {}\n", agent.name, agent.description));
            }
        }

        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
            server_info: Implementation {
                name: "cairn-cmd".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            instructions: Some(instructions),
        }
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        // Get static tools from the router
        let mut tools = self.tool_router.list_all();

        // Append available agents to the write tool description so task
        // appends know which subagentType values are valid.
        if !self.available_agents.is_empty() {
            for tool in &mut tools {
                if tool.name == "write" {
                    let mut desc = tool
                        .description
                        .as_ref()
                        .map(|d| d.to_string())
                        .unwrap_or_default();
                    desc.push_str("\n\nAvailable agents for task appends (subagentType):\n");
                    for agent in &self.available_agents {
                        desc.push_str(&format!("- {}: {}\n", agent.name, agent.description));
                    }
                    tool.description = Some(std::borrow::Cow::Owned(desc));
                    break;
                }
            }
        }

        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParam,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        // Delegate to router for static tools
        let tcc = ToolCallContext::new(self, request, context);
        self.tool_router.call(tcc).await
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParam,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, rmcp::ErrorData> {
        let uri = &request.uri;
        let thread_id = Self::thread_id_from_meta(&context.meta);
        tracing::info!("read_resource called: uri={}", uri);

        // Determine which callback to use based on URI scheme
        let tool_name = if uri.starts_with("cairn://") {
            // Parse cairn:// URI to determine resource type
            match parse_cairn_uri(uri) {
                Some(resource) => {
                    match resource {
                        // Terminal resources use read_resource
                        CairnResource::NodeTerminal { .. }
                        | CairnResource::ProjectTerminal { .. } => "read_resource",
                        // All other resources use read_issue_resource
                        _ => "read_issue_resource",
                    }
                }
                None => {
                    return Err(rmcp::ErrorData::invalid_request(
                        format!("Invalid cairn resource URI: {}", uri),
                        None,
                    ));
                }
            }
        } else {
            return Err(rmcp::ErrorData::invalid_request(
                format!("Unknown resource scheme: {}", uri),
                None,
            ));
        };

        // Call Tauri callback
        let callback_request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: tool_name.to_string(),
            payload: serde_json::json!({ "uri": uri }),
            tool_use_id: None,
            thread_id,
        };

        let response = self.call_tauri_full(&callback_request).await;
        if self.should_update_base_uri_after_read(tool_name, &response) {
            self.note_successful_resource_read(uri);
        }

        // For terminal resources, parse the structured response
        if tool_name == "read_resource" {
            match serde_json::from_str::<TerminalReadResult>(&response.result) {
                Ok(terminal_result) => {
                    let rendered = self.relativize_cairn_uris_in_text(&terminal_result.output);
                    let rendered = assemble_reminders(rendered, &response.reminders);
                    let contents = vec![ResourceContents::text(cap_text_result(&rendered, 0), uri)];
                    return Ok(ReadResourceResult { contents });
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to parse terminal read result: {} - response: {}",
                        e,
                        response.result
                    );
                }
            }
        }

        // For issue resources (or fallback), return the result directly
        // The backend returns canonical content; display rendering can relativize URIs.
        let rendered = self.relativize_cairn_uris_in_text(&response.result);
        let rendered = assemble_reminders(rendered, &response.reminders);
        let contents = vec![ResourceContents::text(cap_text_result(&rendered, 0), uri)];
        Ok(ReadResourceResult { contents })
    }
}

/// Terminal read result returned from Tauri
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct TerminalReadResult {
    output: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{create_test_mcp_with_home_uri, get_text};

    #[test]
    fn test_available_agents_stored_for_change_description() {
        let agents = vec![
            AgentInfo {
                name: "Explore".to_string(),
                description: "Search and explore the codebase".to_string(),
            },
            AgentInfo {
                name: "Research".to_string(),
                description: "Research a topic in depth".to_string(),
            },
        ];

        let mcp = CairnCmd::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            agents,
        );

        let tools = mcp.tool_router.list_all();
        assert!(
            tools.iter().any(|t| t.name == "write"),
            "write tool should exist"
        );

        // Agent-list injection into the change description happens in list_tools();
        // here we verify the agents are stored for that step.
        assert_eq!(mcp.available_agents.len(), 2);
        assert_eq!(mcp.available_agents[0].name, "Explore");
        assert_eq!(mcp.available_agents[1].name, "Research");
    }

    #[test]
    fn test_server_info_includes_agents_in_instructions() {
        let agents = vec![AgentInfo {
            name: "Explore".to_string(),
            description: "Search the codebase".to_string(),
        }];

        let mcp = CairnCmd::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            agents,
        );

        let info = mcp.get_info();
        let instructions = info.instructions.unwrap();

        assert!(
            instructions.contains("Available agents"),
            "Instructions should mention available agents"
        );
        assert!(
            instructions.contains("Explore"),
            "Instructions should include agent name"
        );
        assert!(
            instructions.contains("Search the codebase"),
            "Instructions should include agent description"
        );
    }

    #[test]
    fn test_server_info_excludes_agents_when_empty() {
        let mcp = CairnCmd::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None,   // run_id
            None,   // mcp_secret
            vec![], // No agents
        );

        let info = mcp.get_info();
        let instructions = info.instructions.unwrap();

        assert!(
            !instructions.contains("Available agents"),
            "Instructions should not mention agents when none available"
        );
    }
    #[test]
    fn test_unified_edit_tool_visible() {
        let mcp = CairnCmd::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None,
            None,
            vec![],
        );

        let all_tools = mcp.tool_router.list_all();
        let all_names: Vec<&str> = all_tools.iter().map(|t| t.name.as_ref()).collect();

        assert!(
            all_names.contains(&"write"),
            "write tool should be in tool router"
        );
        assert!(
            !all_names.contains(&"edit"),
            "edit tool should not exist after replacement"
        );
        assert!(
            !all_names.contains(&"message"),
            "message tool should not exist after replacement"
        );
        assert!(
            !all_names.contains(&"add_comment"),
            "add_comment tool should not exist after replacement"
        );
        assert!(
            !all_names.contains(&"update_issue"),
            "update_issue tool should not exist after replacement"
        );
    }
    #[test]
    fn thread_id_from_meta_reads_thread_id_key() {
        // Codex injects `_meta.threadId` on every pooled tool call (CAIRN-2549);
        // other callers send no such key.
        let mut meta = rmcp::model::Meta::default();
        assert_eq!(CairnCmd::thread_id_from_meta(&meta), None);
        meta.insert(
            "threadId".to_string(),
            serde_json::Value::String("thread-xyz".to_string()),
        );
        assert_eq!(
            CairnCmd::thread_id_from_meta(&meta).as_deref(),
            Some("thread-xyz")
        );
    }

    #[tokio::test]
    async fn read_rejects_empty_paths() {
        let mcp = create_test_mcp_with_home_uri(None);
        let result = mcp
            .read(
                Parameters(ReadFileInput { paths: vec![] }),
                rmcp::model::Meta::default(),
            )
            .await
            .unwrap();
        assert!(result.is_error.unwrap_or(false));
        assert!(get_text(&result).contains("non-empty"));
    }
}
