//! The Cairn MCP server: the `CairnMcpServer` service, its three `#[tool]`
//! verbs (`write`/`read`/`run`), and the `ServerHandler` implementation (tool
//! listing, resource reads).
//!
//! One implementation serves every transport. `cairn-cmd mcp` serves it over
//! stdio and forwards each call to the runner's HTTP callback; the orchestrator
//! serves it over streamable HTTP at `/mcp` and dispatches in process. The two
//! differ only in the [`CallbackSink`] that executes a call and in where the
//! run identity comes from ([`Caller`]), so tool descriptions, validation,
//! target resolution, reminder assembly, and output caps cannot drift apart.
use rmcp::{
    handler::server::{
        tool::{ToolCallContext, ToolRouter},
        wrapper::Parameters,
    },
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, DiscoverResult,
        ErrorCode, ErrorData, Extensions, Implementation, ListToolsResult, PaginatedRequestParams,
        ReadResourceRequestParams, ReadResourceResponse, ReadResourceResult, ResourceContents,
        ServerCapabilities, ServerInfo,
    },
    service::{MaybeSendFuture, RequestContext},
    tool, tool_router, RoleServer, ServerHandler,
};
use serde::Deserialize;
use std::{borrow::Cow, future::Future, sync::Arc};

use cairn_common::protocol::CallbackRequest;
use cairn_common::read::{ReadBatchEnvelope, RunBatchEnvelope};
use cairn_common::uri::{parse_uri as parse_cairn_uri, CairnResource};

use crate::output::{
    assemble_reminders, cap_run_result, cap_text_result, change_callback_result, redact_command,
    CallbackOutcome,
};
use crate::schemas::{validate_run_input, ChangeInput, ReadFileInput, RunInput};
use crate::sink::{CallbackSink, RunPrincipal};

/// Where a server learns which run each tool call belongs to.
#[derive(Clone, Debug)]
pub enum Caller {
    /// The process speaks for one run for its whole life. The stdio server and
    /// the `cairn` CLI read the run id and residence from their environment. A
    /// pooled Codex app-server may still name a different thread per call via
    /// `_meta.threadId`, which the host maps back to that thread's run.
    Fixed { run_id: Option<String>, cwd: String },
    /// Every request names its own run through the [`RunPrincipal`] the HTTP
    /// layer attached after verifying that request's bearer. The server holds
    /// no run of its own, and a request without a principal is refused.
    /// `_meta.threadId` is ignored: honoring a client-chosen thread would let
    /// one run's bearer act as another run.
    PerRequest,
}

/// Cairn's MCP server. See the module docs for how transports share it.
#[derive(Clone)]
pub struct CairnMcpServer {
    sink: Arc<dyn CallbackSink>,
    per_request: bool,
    /// Process residence forwarded for diagnostics and explicit host operations.
    cwd: Arc<String>,
    /// Authenticated run identity for project-scoped agent operations.
    run_id: Option<Arc<String>>,
    tool_router: Arc<ToolRouter<Self>>,
}

#[tool_router]
impl CairnMcpServer {
    pub fn new(sink: Arc<dyn CallbackSink>, caller: Caller) -> Self {
        let (per_request, run_id, cwd) = match caller {
            Caller::Fixed { run_id, cwd } => (false, run_id, cwd),
            // Process location is not callback authority, and a remote caller's
            // would name a directory on another machine: report none.
            Caller::PerRequest => (true, None, String::new()),
        };
        Self {
            sink,
            per_request,
            cwd: Arc::new(cwd),
            run_id: run_id.map(Arc::new),
            tool_router: Arc::new(Self::tool_router()),
        }
    }
    /// Apply ordered file and resource mutations through the canonical change carrier.
    #[tool(
        description = r#"Apply ordered file and resource mutations through one carrier. Items in `changes` apply in input order.

Targets:
- File: `file:path/to/file` (worktree-relative; bare `file:` is the worktree root, `file:/abs` is absolute). Every item carries its keys under `payload`: create/replace/append take `payload:{content}`; patch takes `payload:{diff}` OR `payload:{old_string, new_string}` (optional `replace_all`); unified_patch takes `payload:{patch}` containing a native `*** Begin Patch` envelope with add/update/delete sections; delete needs no payload; `rename` takes `payload:{new_name, and exactly one of old_name | symbol_at}` and performs an ast-grep-backed structural rename of an identifier across the worktree, applying every edit site (and any module file move) as one commit. A bare `rename` returns a preview by default; land it with the `apply` round-trip. For a structural gap in a patch, put `~~*~~` in `old_string` between a head and tail anchor to replace everything in between — span by default, balanced only with the contiguous delimiter-pair token (`{~~*~~}`, delimiters immediately adjacent to the marker; the own-line `{\n~~*~~\n}` form spans). Multiple `~~*~~` markers span non-contiguous regions; escape a literal marker with a leading backslash. Example: `payload:{old_string: "fn validate(t) {~~*~~}", new_string: "fn validate(t) { verify(t); }"}`.
- Resource: canonical `cairn://p/PROJECT/...`, node-relative `cairn:~/...`, or project-relative `cairn:@/...`. Modes: create, append, patch, replace, delete.

Don't guess a resource's payload: `read` the target URI first — its affordance block lists the exact actions (mode + required/optional payload keys + a copy-paste example) and read filters. If a mutation is unsupported or missing a required key, the rejection enumerates what the resource accepts.

Notes: `atomic` defaults to false: matching items apply, failed items are reported in `failures`, and `commit_msg` commits only files that applied; set `atomic:true` for fail-fast apply behavior. `cairn:~/...` resolves against your running node and `cairn:@/...` against its project; `preview:true` returns an `apply_uri` to re-submit with `mode=apply`; `commit_msg` is REQUIRED whenever the batch touches a file target (`\"^\"` commits with the previous commit's description; on an agent branch this always creates a new commit and never rewrites the previous commit in place; without a commit_msg the worktree is restored to HEAD) — uncommitted worktree edits are lost if the worktree is cleaned up; task/question appends to `cairn:~/tasks` and `cairn:~/questions` block until results return. Change reports list per-item `applied` and `failures`."#
    )]
    pub(crate) async fn write(
        &self,
        params: Parameters<ChangeInput>,
        meta: rmcp::model::RequestMetaObject,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        self.write_with_origin(params, meta, false).await
    }

    pub async fn write_cli(
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
        // Pooled Codex calls inject the originating thread as `_meta.threadId` so
        // the host can select the run. Deictic targets are always forwarded raw
        // and the host expands them from that run.
        let thread_id = self.thread_id(&meta);

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

        let rewritten = match self.rewrite_change_targets(&input) {
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

        let outcome = self.callback(&request).await;
        Ok(change_callback_result(
            outcome,
            self.sink.describe(),
            "write",
        ))
    }

    /// Read one or more files, directories, or Cairn resources in a single call.
    #[tool(
        description = r#"Read one or more files, directories, or Cairn resources in a single call. `paths` is an ordered, non-empty array of target URIs; results return in order, each under a `=== <uri> [suffix] ===` header.

Targets (mix freely within one call):
- File: `file:` (worktree root), `file:src/lib.rs` (worktree-relative), `file:/abs/path` (absolute / global)
- Resource: canonical `cairn://p/PROJECT[/NUMBER[/EXEC/NODE[/sub]]]` and the other `cairn://` families (posts, feed, executors, channels, models, inference, help); `cairn:~/...` resolves against the run home. Source-control questions need no checkout: `cairn:@/vcs/diff`, `/vcs/ancestry` (fork point, ahead/behind, is X in main), `/vcs/log` and `/vcs/blame` answer from the store and take the same refs `branch=` does. Every resource read returns an affordance block listing its filters, links, and actions.
- Web: `http(s)://...` URLs and local `.pdf` paths return markdown; `cairn://websearch?q=QUERY` runs a web search (everything after `q=` is the literal query, so spaces are fine).
- Images: reading an image (an image file, `cairn:~/browser?screenshot`, a stored image URI) shows it to BOTH you and the user, and a durable `![label](cairn://p/PROJECT/ISSUE/images/N)` reference renders in the transcript to carry the image forward.

Per-target scoping rides in each URI's query string — append `?key=value&...`:
- Files: `offset=N` skips N leading lines; `limit=N` returns N lines; `offset=-N` tails. `branch=REF` reads content as of another revision (a branch name, commit, or node URI) without switching branches. `glob=PATTERN` selects matched files. `issue_history=true|verbose` appends issues that touched the file.
- Grep is universal: `grep=REGEX` matches over ANY target's rendered text. Results group by file the way `rg --heading` does: the path on its own line, then its matches beneath it as `N:text` (context lines as `N-text`), a blank line before the next file. Modifiers: `-i`, `-A=N`/`-B=N`/`-C=N`, `output_mode=files_with_matches|content|count`, `head_limit=N` to cap matches (matches, not rendered lines — context does not count against it). A capped grep reports the FULL result's counts and marks itself partial (`[23 matches in 6 files; 8 of 23 matches shown]`) with a `continue:` footer that widens the cap, so a windowed enumeration is never mistaken for a complete one. `offset` is not allowed with grep — page with `head_limit`. `&` and `+` are literal inside a value; use `%26` for a literal `&` that immediately precedes a recognized key token.
- Structural code: `ast=PATTERN` runs an ast-grep pattern (real code with `$VAR`/`$$$` metavariables, e.g. `ast=console.log($$$)`) over any file/dir target; `outline` (bare flag) renders a signature skeleton. Symbol navigation lives on `cairn:~/symbols/{name}?op=definition|references|callers|implementations` (`in=GLOB` scopes it).
- Resources: `offset`/`limit` page rendered lines, and each collection takes its own keys (e.g. `/issues?status=active&limit=20`, `/messages?since=EPOCH`, project search `cairn://p/PROJECT?search=QUERY`); the affordance block lists them. Raw transcripts (`/chat/raw?format=json`) return JSONL, one event per line.

Partial failures never abort: a failed target stays in place under an `[error]` header, and an unchanged unwindowed repeat is suppressed as `[duplicate]` (explicit windows are deliberate navigation and never suppressed). A multi-target read shares a single ~45k-char budget, water-filled so small targets render whole; every truncated or windowed segment ends with a valid `continue:` footer — follow it rather than re-reading."#
    )]
    pub(crate) async fn read(
        &self,
        params: Parameters<ReadFileInput>,
        meta: rmcp::model::RequestMetaObject,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        self.read_with_origin(params.0, meta, false).await
    }

    pub async fn read_cli(&self, input: ReadFileInput) -> Result<CallToolResult, rmcp::ErrorData> {
        self.read_with_origin(input, rmcp::model::RequestMetaObject::default(), true)
            .await
    }

    async fn read_with_origin(
        &self,
        input: ReadFileInput,
        meta: rmcp::model::RequestMetaObject,
        standalone_cli: bool,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let thread_id = self.thread_id(&meta);
        if input.paths.is_empty() {
            return Ok(CallToolResult::error(vec![ContentBlock::text(
                "read requires a non-empty `paths` array (one or more target URIs).".to_string(),
            )]));
        }
        tracing::info!("read called: {} paths", input.paths.len());

        let outcome = self.read_batch(&input, thread_id, standalone_cli).await;

        // The handler content is the bare envelope JSON — augmentation reminders
        // ride separately, so this parses cleanly with no trailing-text split.
        let envelope = match serde_json::from_str::<ReadBatchEnvelope>(&outcome.result) {
            Ok(envelope) => envelope,
            Err(_) => {
                // Transport/parse failure: surface the raw result as text.
                return Ok(CallToolResult::success(vec![ContentBlock::text(
                    outcome.result,
                )]));
            }
        };
        let text = assemble_reminders(envelope.text, &outcome.reminders);

        let mut blocks: Vec<ContentBlock> = Vec::with_capacity(1 + envelope.images.len());
        blocks.push(ContentBlock::text(text));
        for image in envelope.images {
            blocks.push(ContentBlock::image(image.data, image.mime_type));
        }
        Ok(CallToolResult::success(blocks))
    }

    /// Issue one `read_batch` callback for `input`.
    ///
    /// The single request builder behind every read: the MCP tool, the `cairn
    /// read` subcommand, and the shell all reach the backend through here, so
    /// they cannot resolve targets or frame the call differently from one
    /// another.
    async fn read_batch(
        &self,
        input: &ReadFileInput,
        thread_id: Option<String>,
        standalone_cli: bool,
    ) -> CallbackOutcome {
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
                    self.resolve_read_target(path)
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
        self.callback(&request).await
    }

    /// Read one target and hand back the envelope itself.
    ///
    /// The shell needs both halves of a read: the composed text to print, which
    /// is byte-identical to what `cairn read` prints, and the structured
    /// affordances, which are its command set at that location. A tool result
    /// carries only the first, so the shell takes the envelope directly rather
    /// than parsing the rendered markdown back apart.
    pub async fn read_envelope(&self, target: &str) -> Result<ReadBatchEnvelope, String> {
        let input = ReadFileInput {
            paths: vec![target.to_string()],
        };
        let outcome = self.read_batch(&input, None, true).await;
        match serde_json::from_str::<ReadBatchEnvelope>(&outcome.result) {
            Ok(envelope) => Ok(envelope),
            Err(_) => Err(outcome.result),
        }
    }

    /// Execute an ordered batch of shell commands, inline code, and skill-script
    /// invocations, synchronously. Parallel by default; `sequential: true` runs
    /// in order. Long-running terminals are managed by `write` on terminals.
    #[tool(
        description = r#"Execute an ordered batch of synchronous invocations, or suspend this turn on a sole `waitFor` item. Each item in `commands` is exactly one of: a shell `command`; a `target` skill-script URI (cairn://skills/<id>/scripts/<name>) with optional `payload.args`; a `target` external MCP tool (cairn://mcp/<server>/<tool>) with named arguments in `payload.args_json` (read cairn://mcp/<server> for each tool's arg shape); or inline `code` with a required `interpreter` (`typescript`/`javascript` via bun, `python` via uv, `matlab`). Inline code is the default way to run code that isn't a CLI invocation — the interpreter execs the source directly, so there is no shell and no quoting; keep it synchronous and run-to-completion, and put long-running or background processes in a terminal resource via `write`. Add `repl:<slug>` to an inline item to evaluate it in a stateful REPL session created at `cairn:~/repl/<slug>`. A batch may not mix tree-bound items (shell, inline code, skill scripts) with MCP or REPL items.

Items run in PARALLEL by default; `sequential:true` runs in order (fail-fast unless `stop_on_error:false`). Output composes under `=== <header> ===` headers in input order. One call in, one final result out: a batch that outlives the 120-second synchronous window suspends and resumes with the finished result. An item's `timeout` (ms) kills that item at the bound and reports whatever output it produced rather than terminating the call. For a tree-bound batch in which every item declares a timeout, those bounds also limit how long the batch waits for a busy machine (the parallel maximum or sequential sum, capped at the 6-hour batch ceiling) and tell a remote executor how long to hold its queue place; if any item omits one, the batch may wait up to that ceiling. An omitted timeout also lets a tree-bound item run to completion under the ceiling. A response item (`cairn://responses/...`) runs to its `timeout`, or without one to its definition's declared timeout, under the same ceiling, and its batch suspends past the window like a long shell batch; a `cairn.run` of one from @cairn/sdk inside a shell item has no call of its own to suspend on and ends at 120 seconds. MCP-tool, REPL, and decide (`cairn://models/decide`) items execute on the host inside the 120-second window, and a longer `timeout` is cut to 120 seconds.

If a successful batch dirties the tree, `commit_msg` is required and commits all worktree changes once after success (`^` amends); without it the worktree is restored to HEAD. `branch` runs the batch against another revision — a branch name, a commit, or a node URI — to tell a regression from a failure already on the base (read `cairn:@/check-results/<base>?suite=S&environment=current` first: the ledger may already hold that verdict, and diffs and ancestry are `cairn:@/vcs/...` reads, not branch runs); it is verdict-only: tracked writes are discarded, and commit_msg, MCP, and REPL items are rejected. `executor` places the batch on a specific machine (`{name:"..."}`) or platform (`{os:"linux"}`), optionally refined by `requiredToolchains`; read cairn://executors for what is available, and omit it otherwise.

A sole `{waitFor:...}` item suspends until a duration elapses (`{duration:"3m"}`), a terminal exits or prints a phrase (`{kind:"terminal",ref:"cairn:~/terminal/dev",on:"output",phrase:"ready"}`), or ANOTHER node's project check lanes settle (`{kind:"checks",ref:"cairn://p/CAIRN/123/1/builder/checks",on:"settled"}`, or `on:"verdict",suite:"..."` for one suite), resuming with the lanes' verdicts. You cannot wait on your own lanes; for those, end the turn on `write cairn:~/wakes {subscribe:{kind:"checks"}}` instead."#
    )]
    async fn run(
        &self,
        params: Parameters<RunInput>,
        meta: rmcp::model::RequestMetaObject,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        let thread_id = self.thread_id(&meta);

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

        let outcome = self.callback(&request).await;
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

impl CairnMcpServer {
    /// Process residence reported with each callback.
    pub fn cwd(&self) -> &str {
        &self.cwd
    }

    /// The run this server speaks for, when it has a fixed one.
    pub fn run_id(&self) -> Option<&str> {
        self.run_id.as_deref().map(String::as_str)
    }

    /// Extract Codex's per-call thread id from a `tools/call` request's `_meta`
    /// (CAIRN-2549). Codex injects it under the `"threadId"` key for every tool
    /// call from a pooled app-server thread; other callers send no such meta and
    /// this returns `None`. A per-request server never honors it (see
    /// [`Caller::PerRequest`]).
    fn thread_id(&self, meta: &rmcp::model::RequestMetaObject) -> Option<String> {
        if self.per_request {
            return None;
        }
        meta.get("threadId")
            .and_then(|v| v.as_str())
            .map(str::to_string)
    }

    /// Execute one callback through this server's sink and return the full
    /// outcome (handler result plus augmentation reminders). Verbs assemble
    /// reminders into the model-visible text at the edge, after parsing any
    /// structured result.
    pub async fn callback(&self, request: &CallbackRequest) -> CallbackOutcome {
        self.sink.call(request).await
    }

    /// The server that answers one request. A fixed server answers as itself. A
    /// per-request server answers as the run the HTTP layer authenticated for
    /// this request, and refuses a request that carries no such run.
    fn for_request(&self, extensions: &Extensions) -> Result<Cow<'_, Self>, ErrorData> {
        if !self.per_request {
            return Ok(Cow::Borrowed(self));
        }
        let principal = extensions
            .get::<http::request::Parts>()
            .and_then(|parts| parts.extensions.get::<RunPrincipal>())
            .ok_or_else(|| {
                ErrorData::invalid_request(
                    "this request carries no authenticated Cairn run; present the run's bearer token",
                    None,
                )
            })?;
        let mut server = self.clone();
        server.run_id = Some(Arc::new(principal.run_id.clone()));
        Ok(Cow::Owned(server))
    }
}

impl ServerHandler for CairnMcpServer {
    fn discover(
        &self,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<DiscoverResult, ErrorData>> + MaybeSendFuture + '_ {
        // Cairn selects classic initialization. HTTP discovery is stateless;
        // stdio must decline it in classic_stdio_transport before rmcp locks
        // the connection into requiring inline metadata.
        std::future::ready(Err(ErrorData::new(
            ErrorCode::METHOD_NOT_FOUND,
            "Method not found",
            None,
        )))
    }

    fn get_info(&self) -> ServerInfo {
        let instructions = "Cairn MCP server for agent orchestration. Three batch verbs: \
             `read` (files, directories, cairn:// resources, and web targets, with per-target \
             `?query` scoping), `write` (ordered file and cairn:// resource mutations; \
             `commit_msg` commits file edits), and `run` (shell commands, inline code, and \
             skill scripts). Each tool's description carries its full contract; reading a \
             cairn:// resource returns its affordances inline."
            .to_string();

        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder()
            .enable_tools()
            .enable_resources()
            .build();
        info.server_info = Implementation::new("cairn", env!("CARGO_PKG_VERSION"));
        info.instructions = Some(instructions);
        info
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        Ok(ListToolsResult::with_all_items(self.tool_router.list_all()))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, rmcp::ErrorData> {
        let server = self.for_request(&context.extensions)?;
        let tcc = ToolCallContext::new(server.as_ref(), request, context);
        server.tool_router.call(tcc).await
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, rmcp::ErrorData> {
        let server = self.for_request(&context.extensions)?;
        let uri = &request.uri;
        let thread_id = server.thread_id(&context.meta);
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
            cwd: server.cwd.to_string(),
            run_id: server.run_id.as_ref().map(|r| r.to_string()),
            tool: tool_name.to_string(),
            payload: serde_json::json!({ "uri": uri }),
            tool_use_id: None,
            thread_id,
        };

        let response = server.callback(&callback_request).await;
        // For terminal resources, parse the structured response
        if tool_name == "read_resource" {
            match serde_json::from_str::<TerminalReadResult>(&response.result) {
                Ok(terminal_result) => {
                    let rendered = assemble_reminders(terminal_result.output, &response.reminders);
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

        // For issue resources (or fallback), return the backend result directly.
        let rendered = assemble_reminders(response.result, &response.reminders);
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
    use crate::test_support::{get_text, test_server, RecordingSink};
    use rmcp::{
        model::ClientInfo, ClientHandler, ClientLifecycleMode, ClientServiceExt, ServiceExt,
    };

    #[derive(Clone)]
    struct LifecycleClient;

    impl ClientHandler for LifecycleClient {
        fn get_info(&self) -> ClientInfo {
            let mut info = ClientInfo::default();
            info.protocol_version = rmcp::model::ProtocolVersion::V_2025_11_25;
            info
        }
    }

    #[tokio::test]
    async fn discovery_declines_negotiated_lifecycle_and_classic_init_lists_core_tools() {
        let mcp = test_server();
        let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(async move {
            let _ = mcp
                .serve(crate::classic_stdio_transport(server_transport))
                .await
                .expect("server should start")
                .waiting()
                .await;
        });
        let client = LifecycleClient
            .serve_with_lifecycle(
                client_transport,
                ClientLifecycleMode::Auto {
                    preferred_versions: vec![rmcp::model::ProtocolVersion::V_2026_07_28],
                    legacy_version: Some(rmcp::model::ProtocolVersion::V_2025_11_25),
                },
            )
            .await
            .expect("auto client should fall back to classic initialization");
        assert_eq!(
            client
                .peer_info()
                .expect("initialized server info")
                .protocol_version,
            rmcp::model::ProtocolVersion::V_2025_11_25
        );
        let mut names = client
            .list_tools(None)
            .await
            .expect("tools/list after same-connection fallback")
            .tools
            .into_iter()
            .map(|tool| tool.name.to_string())
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, ["read", "run", "write"]);

        client.cancel().await.expect("classic client should cancel");
        server.await.expect("server task");
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

        let mcp = test_server();
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
        let mcp = test_server();

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
    fn only_a_fixed_server_honors_the_pooled_thread_id() {
        // Codex injects `_meta.threadId` on every pooled tool call (CAIRN-2549);
        // other callers send no such key.
        let mut meta = rmcp::model::RequestMetaObject::default();
        assert_eq!(test_server().thread_id(&meta), None);
        meta.insert(
            "threadId".to_string(),
            serde_json::Value::String("thread-xyz".to_string()),
        );
        assert_eq!(
            test_server().thread_id(&meta).as_deref(),
            Some("thread-xyz")
        );

        // A per-request server answers only as its authenticated run, so a
        // client-chosen thread never reaches the host.
        let per_request = CairnMcpServer::new(
            RecordingSink::answering(CallbackOutcome::default()),
            Caller::PerRequest,
        );
        assert_eq!(per_request.thread_id(&meta), None);
    }

    #[tokio::test]
    async fn read_rejects_empty_paths() {
        let mcp = test_server();
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

    #[tokio::test]
    async fn read_returns_callback_text_byte_exact() {
        let canonical = "cairn://p/CAIRN/4297/1/builder";
        let envelope = serde_json::json!({
            "text": canonical,
            "images": [],
            "segments": []
        })
        .to_string();
        let sink = RecordingSink::answering(CallbackOutcome {
            result: envelope,
            reminders: Vec::new(),
            transport_ok: true,
        });
        let mcp = CairnMcpServer::new(
            sink.clone(),
            Caller::Fixed {
                run_id: None,
                cwd: "/test/path".to_string(),
            },
        );
        let result = mcp
            .read(
                Parameters(ReadFileInput {
                    paths: vec!["cairn:~/arc?format=json".to_string()],
                }),
                rmcp::model::RequestMetaObject::default(),
            )
            .await
            .expect("read result");

        assert_eq!(get_text(&result), canonical);
        // The home shorthand is forwarded raw; the host resolves it from the run.
        assert_eq!(
            sink.requests()[0].payload["paths"][0],
            "cairn:~/arc?format=json"
        );
    }
}
