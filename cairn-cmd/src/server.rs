//! The stdio MCP server: the `CairnCmd` service, its three `#[tool]` verbs
//! (`write`/`read`/`run`), the HTTP callback plumbing, and the `ServerHandler`
//! implementation (tool listing, resource reads).
use rmcp::{
    handler::server::{
        tool::{ToolCallContext, ToolRouter},
        wrapper::Parameters,
    },
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Implementation,
        ListToolsResult, PaginatedRequestParams, ReadResourceRequestParams, ReadResourceResponse,
        ReadResourceResult, ResourceContents, ServerCapabilities, ServerInfo,
    },
    service::RequestContext,
    tool, tool_router, RoleServer, ServerHandler,
};
use serde::Deserialize;
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
    /// Process residence forwarded for diagnostics and explicit host operations.
    pub(crate) cwd: Arc<String>,
    /// Authenticated run identity for project-scoped agent operations.
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
        meta: rmcp::model::RequestMetaObject,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        self.write_with_origin(params, meta, false).await
    }

    pub(crate) async fn write_cli(
        &self,
        params: Parameters<ChangeInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        self.write_with_origin(params, rmcp::model::RequestMetaObject::default(), true)
            .await
    }

    async fn write_with_origin(
        &self,
        params: Parameters<ChangeInput>,
        meta: rmcp::model::RequestMetaObject,
        standalone_cli: bool,
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
            return Ok(CallToolResult::success(vec![ContentBlock::text(text)]));
        }

        let rewritten = match self.rewrite_change_targets_with(&input, pooled) {
            Ok(rewritten) => rewritten,
            Err(message) => return Ok(CallToolResult::success(vec![ContentBlock::text(message)])),
        };

        let mut payload = serde_json::to_value(&rewritten).unwrap_or_default();
        if standalone_cli {
            payload["_cairn_origin"] = serde_json::Value::String("cli".into());
        }
        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "write".to_string(),
            payload,
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
        description = r#"Read one or more files, directories, or Cairn resources in a single call. `paths` is an ordered, non-empty array of target URIs; results return in order, each under a `=== <uri> [suffix] ===` header.

Targets (mix freely within one call):
- File: `file:` (worktree root), `file:src/lib.rs` (worktree-relative), `file:/abs/path` (absolute / global)
- Resource: canonical `cairn://p/PROJECT[/NUMBER[/EXEC/NODE[/sub]]]` plus collections `/issues`, `/messages`, issue `/changed`, node `/diff`, `/references`, and `/references/NAME`. `cairn:~/...` resolves against the run home.
- Web/PDF: `http(s)://...` URLs and local `.pdf` paths return markdown via the active web-fetch provider (the built-in default is a plain HTTP fetch; PDF extraction needs a configured provider such as `bmd`).
- Web search: `cairn://websearch?q=QUERY` runs the query through the active web-search provider and returns ranked results as markdown; everything after `q=` is the literal query, so spaces are fine.
- Fleet: `cairn://executors` lists every machine enrolled with this runner by the public name that addresses it, and `cairn://executors/{name}` shows compact machine status — platform, toolchains, link and build state, timestamped telemetry, admission and queues, and occupancy. Drill into placement history with `?view=placements`, then read one complete decision (including passed-over candidate predictions) with `?view=placement&request=<request-id>`. These names are exactly what the run tool's `executor` selector accepts, so what you can read is what you can target. Served from cached fleet state; a read never probes a machine.
- Images: reading an image (an image file, `cairn:~/browser?screenshot`, a stored image URI) shows it to BOTH of you in one step — you see the image, and a durable `![label](cairn://p/PROJECT/ISSUE/images/N)` reference renders in the transcript; paste that reference into a message, issue, or artifact to carry the image forward. Image addresses are ordinal and scoped, so `images/4` addresses a sibling directly and reading `cairn://p/PROJECT/ISSUE/images` lists an issue's images.

Per-target scoping rides in each URI's query string — append `?key=value&...`:
- Files: `offset=N` skips N leading lines (0-based — line N is at `offset N−1`); `limit=N` returns N lines; `offset=-N` returns the last N lines (tail). `branch=REF` reads file content from a jj-resolved bookmark, commit/change id, or node URI without checking out that branch; it is per-target only and applies only to `file:` targets. `glob=PATTERN` selects matched files (`output_mode=files_with_matches|content|count`; a directory grep defaults to `files_with_matches`, a single-file grep to `content`). `issue_history=true|verbose` appends issues that touched the file.
- Grep is universal: `grep=REGEX` matches over ANY target's rendered text. A file tree greps with ripgrep and labels each line `path:N:text`; a single file or any rendered resource/web body greps in memory and drops the path prefix (`N:text`). Modifiers: `-i` (case-insensitive), `-A=N`/`-B=N`/`-C=N`/`context=N` (context lines), `head_limit=N` to cap matches (`limit=N` aliases it under grep). `offset` is NOT allowed with grep — paginate matches with `head_limit`.
- Structural code: `ast=PATTERN` runs an ast-grep pattern over any file/dir target and renders the same `path:N:line` rows as grep (composes with `glob`). A pattern is real code with metavariables — `$VAR` matches one node, `$$$` a run — e.g. `ast=fn $NAME($$$) { $$$ }` (Rust) or `ast=console.log($$$)` (TS); it is NOT a tree-sitter node-kind name like `function_declaration`. `outline` (bare flag) renders a file/dir signature skeleton. Symbol navigation lives on the `symbols` resource: `cairn://p/PROJECT/NUMBER/EXEC/NODE/symbols/{name}?op=definition|references|callers|implementations` (node-scoped) or `cairn://p/PROJECT/symbols/{name}` (project-checkout fallback); absent `op` is an overview (definition site + signature + reference count), `in=GLOB` scopes it.
- Escaping: `&` and `+` are literal inside a value (so `grep=&mut self` and `grep=\d+` work as written); use `%26` for a literal `&` that immediately precedes a recognized key token
- Resources: `offset=N` skips rendered resource lines client-side; `limit=N` is resource-specific unless reading a single transcript event, where it is a client-side line count
- Raw transcripts: append `format=json` to node or task `/chat/raw` for JSONL with one canonically reconstructed event per line (`runId`, `sequence`, `turnId`, `eventType`, `createdAt`, `payload`); `offset`/`limit` then page events, and an oversized event resumes through the emitted cumulative `char_offset` continuation
- `/issues`: `status=backlog,active,...` (comma-separated), `limit=N`, `sort=updated_desc|created_asc|...`, `ready=true|false`
- `/messages`: `before=`, `after=`, `since=EPOCH`, `limit=N`
- Project search: `cairn://p/PROJECT?search=QUERY&limit=N&since=EPOCH`

Partial failures never abort: a target that errors shows its message inline as that target's block, and every requested target still contributes a block. A multi-target read shares a single ~45k-char total budget across targets (water-filled so small targets render whole and large ones fair-share); every requested target is included, and a windowed or truncated segment carries an always-valid `continue:` footer — `[lines A–B of T — output truncated to fit budget; continue: ...]`, advancing `offset`/`head_limit`, or a `char_offset=` resume when a single line is itself larger than the budget."#
    )]
    pub(crate) async fn read(
        &self,
        params: Parameters<ReadFileInput>,
        meta: rmcp::model::RequestMetaObject,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        self.read_with_origin(params.0, meta, false).await
    }

    pub(crate) async fn read_cli(
        &self,
        input: ReadFileInput,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        self.read_with_origin(input, rmcp::model::RequestMetaObject::default(), true)
            .await
    }

    async fn read_with_origin(
        &self,
        input: ReadFileInput,
        meta: rmcp::model::RequestMetaObject,
        standalone_cli: bool,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let thread_id = Self::thread_id_from_meta(&meta);
        let pooled = thread_id.is_some();
        if input.paths.is_empty() {
            return Ok(CallToolResult::error(vec![ContentBlock::text(
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

        let mut payload = serde_json::json!({ "paths": resolved });
        if standalone_cli {
            payload["_cairn_origin"] = serde_json::Value::String("cli".to_string());
        }
        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "read_batch".to_string(),
            payload,
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
                return Ok(CallToolResult::success(vec![ContentBlock::text(
                    self.relativize_cairn_uris_in_text(&outcome.result),
                )]));
            }
        };
        let text = self.relativize_cairn_uris_in_text(&envelope.text);
        let text = assemble_reminders(text, &outcome.reminders);

        let mut blocks: Vec<ContentBlock> = Vec::with_capacity(1 + envelope.images.len());
        blocks.push(ContentBlock::text(text));
        for image in envelope.images {
            blocks.push(ContentBlock::image(image.data, image.mime_type));
        }
        Ok(CallToolResult::success(blocks))
    }

    /// Execute an ordered batch of shell commands, inline code, and skill-script
    /// invocations, synchronously. Parallel by default; `sequential: true` runs
    /// in order. Long-running terminals are managed by `write` on terminals.
    #[tool(
        description = "Suspend this turn without polling with a sole wait item: `{waitFor:{duration:\"3m\"}}`, `{waitFor:{kind:\"terminal\",ref:\"cairn:~/terminal/tests\",on:\"exit\"}}`, `{waitFor:{kind:\"terminal\",ref:\"cairn:~/terminal/dev\",on:\"output\",phrase:\"ready\"}}`, or `{waitFor:{kind:\"checks\",ref:\"cairn://p/CAIRN/3427/1/builder/checks\",on:\"settled\"}}` (add `on:\"verdict\",suite:\"rust-tests\"` for one suite) — the checks wait is how you learn that ANOTHER node's project check lanes have stopped moving, e.g. before merging a child's PR, instead of sleeping a blind duration and re-reading. It resumes with each lane's verdict and a one-word `verdict` of passed/failed/incomplete, where incomplete means a lane stopped without ever producing one. You cannot wait on your OWN lanes (the turn-end wave is armed by this turn ENDING); for that, end your turn on `write cairn:~/wakes {subscribe:{kind:\"checks\"}}`. The pending run call resumes when the condition fires. Otherwise, execute an ordered batch of synchronous invocations. `commands` is a non-empty array; each item is exactly one of: a shell `command`; a `target` skill-script URI (cairn://skills/<id>/scripts/<name>) with optional `payload.args`; a `target` external MCP tool (cairn://mcp/<server>/<tool>) with its named arguments in `payload.args_json` (e.g. `{target:\"cairn://mcp/axon/look\", payload:{args_json:{app:\"Finder\"}}}` — read cairn://mcp/<server> for each tool's arg shape); or inline `code` with a required `interpreter` (e.g. `{code:\"console.log(1)\", interpreter:\"typescript\"}`). Inline `code` is the default way to run code that isn't a CLI invocation: the interpreter execs the source directly, so there is no shell and no quoting. `typescript`/`ts` and `javascript`/`js` run via bun with the worktree `node_modules` and zero-config `@cairn/sdk` importable; `python`/`py` runs through the bundled `uv` (a PEP 723 `# /// script` dependency block resolves into a cached environment, and a worktree `pyproject.toml`/`uv.lock` project env is picked up automatically); `matlab` runs via `matlab -batch`. Add `repl:<slug>` to an inline `code` item to evaluate it in a stateful REPL session — create it first with `write cairn:~/repl/<slug> {interpreter:\"python\", deps:[\"pandas\"]}` — so variables, imports, and defs persist across `run` calls (its state is lost if the REPL dies). Prefer inline code over wrapping a one-liner in `sh -c` / `python3 -c` / `bun -e`. Keep inline code synchronous and run-to-completion; long-running or background code belongs to terminal resources (and durable workflow scripts). Items run in PARALLEL by default; set `sequential: true` for ordered execution (fail-fast unless `stop_on_error: false`). Output is composed under `=== <header> ===` headers in input order. If a successful worktree-bound batch dirties the tree, `commit_msg` is required and commits all worktree changes ONCE after the batch succeeds; `^` amends. Without a commit_msg, a batch that dirties the worktree is restored to HEAD. `branch` runs the batch against another revision — a branch name, a commit, or a node URI — instead of your own, which is how you tell a real regression from a failure already on the base (`run({commands:[{command:\"bun run test\"}], branch:\"main\"})`); it is verdict-only, so tracked writes are discarded, and `commit_msg`, MCP, and REPL items are rejected. `executor` states which machine runs the batch — `{name:\"bglab-ub\"}` for one machine by its public name or `{os:\"linux\"}` for any machine on that platform, either optionally refined by `requiredToolchains`; `name` and `os` are mutually exclusive, and read `cairn://executors` for the names, platforms, and toolchains available. In an agent job, an incompatible explicit selector runs in a fresh checkout at the branch head rather than the job's warm working tree. Omit it and the batch runs in its normal execution home. Not for long-lived/background processes — use a terminal resource via `write` for those. One call in, one final result out: a batch runs to completion, and if it takes longer than 120 seconds this call suspends and resumes with the finished result rather than reporting progress. An item's `timeout` (ms) is a kill bound, not a call bound: the item terminates at the bound and its result block reports the timeout with whatever output it produced — the batch is never aborted with no output. Omitting it lets a shell, code, or skill-script item run to completion, capped only by the 6-hour ceiling on a batch (a command meant to keep running belongs in a terminal); an MCP-tool or REPL item is capped at the 120-second synchronous window."
    )]
    async fn run(
        &self,
        params: Parameters<RunInput>,
        meta: rmcp::model::RequestMetaObject,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        let thread_id = Self::thread_id_from_meta(&meta);

        if let Err(msg) = validate_run_input(&input) {
            return Ok(CallToolResult::error(vec![ContentBlock::text(msg)]));
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
        let mut blocks: Vec<ContentBlock> = Vec::with_capacity(1 + envelope.images.len());
        blocks.push(ContentBlock::text(text));
        for image in envelope.images {
            blocks.push(ContentBlock::image(image.data, image.mime_type));
        }
        Ok(CallToolResult::success(blocks))
    }
}

impl CairnCmd {
    /// Extract Codex's per-call thread id from a `tools/call` request's `_meta`
    /// (CAIRN-2549). Codex injects it under the `"threadId"` key for every tool
    /// call from a pooled app-server thread; other callers send no such meta and
    /// this returns `None`.
    fn thread_id_from_meta(meta: &rmcp::model::RequestMetaObject) -> Option<String> {
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
        let mut instructions = "Cairn MCP server for agent orchestration. Three batch verbs: \
             `read` (files, directories, cairn:// resources, and web targets, with per-target \
             `?query` scoping), `write` (ordered file and cairn:// resource mutations; \
             `commit_msg` commits file edits), and `run` (shell commands, inline code, and \
             skill scripts). Each tool's description carries its full contract; reading a \
             cairn:// resource returns its affordances inline."
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

        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder()
            .enable_tools()
            .enable_resources()
            .build();
        info.server_info = Implementation::new("cairn-cmd", env!("CARGO_PKG_VERSION"));
        info.instructions = Some(instructions);
        info
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
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
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, rmcp::ErrorData> {
        // Delegate to router for static tools
        let tcc = ToolCallContext::new(self, request, context);
        self.tool_router.call(tcc).await
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, rmcp::ErrorData> {
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
                    return Ok(ReadResourceResult::new(contents).into());
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
        Ok(ReadResourceResult::new(contents).into())
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
    /// The `run` tool's advertised timeout contract is prose in two places — the
    /// tool description and the `RunItemInput.timeout` schema description — while
    /// the bound is enforced in cairn-core. They drifted once already: both
    /// surfaces promised a 600,000 ms maximum that no layer enforced, so an agent
    /// asking for an hour had no way to know whether it would get one. Bind the
    /// advertised text to the shared constant so it cannot silently rot again.
    #[test]
    fn the_advertised_run_timeout_contract_matches_the_enforced_ceiling() {
        use cairn_common::run_contract::{RUN_BATCH_CEILING_HOURS, RUN_GRACE_WINDOW_MS};

        let mcp = CairnCmd::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None,
            None,
            vec![],
        );
        let run = mcp
            .tool_router
            .list_all()
            .into_iter()
            .find(|tool| tool.name == "run")
            .expect("run tool should exist");

        let ceiling = format!("{RUN_BATCH_CEILING_HOURS}-hour");
        // Both surfaces must name the grace window, but prose reads it either as
        // "120 seconds" or "120-second" depending on position. Normalize the
        // hyphen so the assertion pins the number and unit, not the grammar.
        let grace = format!("{} second", RUN_GRACE_WINDOW_MS / 1_000);
        let normalize = |text: &str| text.replace('-', " ");
        let description = run
            .description
            .as_ref()
            .expect("run tool should be described")
            .to_string();
        assert!(
            description.contains(&ceiling),
            "the run description must name the enforced ceiling ({ceiling}): {description}"
        );
        assert!(
            normalize(&description).contains(&grace),
            "the run description must name the grace window ({grace}): {description}"
        );

        let schema = serde_json::Value::Object((*run.input_schema).clone());
        let timeout = run_item_timeout_description(&schema);
        assert!(
            timeout.contains(&ceiling),
            "RunItemInput.timeout must name the enforced ceiling ({ceiling}): {timeout}"
        );
        assert!(
            timeout.to_lowercase().contains("omit"),
            "RunItemInput.timeout must say what omitting it means: {timeout}"
        );

        // Host-executed items (MCP tool calls, REPL sends) never enter the
        // suspendable routed path, so the ceiling above them is the grace window,
        // not the batch ceiling. Advertising one generic bound over both classes
        // is what made the contract describe states the system cannot reach.
        for surface in [&description, &timeout] {
            assert!(
                normalize(surface).contains(&grace),
                "the host-item bound ({grace}) must be advertised alongside the batch ceiling: {surface}"
            );
        }

        // The retired maximum. Any layer re-advertising it is advertising a bound
        // nothing enforces, which is the exact defect being fixed here.
        for surface in [&description, &timeout] {
            assert!(
                !surface.contains("600000") && !surface.contains("600,000"),
                "the retired 600000 ms maximum must not be advertised: {surface}"
            );
        }
    }

    /// Pull `RunItemInput.timeout`'s description out of the generated schema,
    /// tolerating either schemars definition key. A panic here means the schema
    /// shape moved and the assertion above stopped covering anything.
    fn run_item_timeout_description(schema: &serde_json::Value) -> String {
        for defs in ["definitions", "$defs"] {
            if let Some(description) = schema
                .pointer(&format!(
                    "/{defs}/RunItemInput/properties/timeout/description"
                ))
                .and_then(|value| value.as_str())
            {
                return description.to_string();
            }
        }
        panic!("RunItemInput.timeout description missing from the run tool schema: {schema}");
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
        let mut meta = rmcp::model::RequestMetaObject::default();
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
                rmcp::model::RequestMetaObject::default(),
            )
            .await
            .unwrap();
        assert!(result.is_error.unwrap_or(false));
        assert!(get_text(&result).contains("non-empty"));
    }
}
