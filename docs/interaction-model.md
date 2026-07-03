# Interaction Model

> Historical note: parts of this document describe an older session model. The current `cairn-core` implementation uses `idle` runs plus warm-process retention rather than the older `paused`/kill-first framing. For the current code shape, see `docs/cairn-core-shape-map.md`, `docs/state-machines.md`, and `docs/execution-lifecycle-map.md`.

This document describes how cairn-core, Claude CLI, and cairn-cmd work together — the session lifecycle, callback architecture, tool resolution, and process management.

## Architecture

```
┌──────────────────┐
│   Host           │  Owns DB, services, process state
│   (Orchestrator) │
│                  │
│   port 3847      │◄─── HTTP callbacks ────┐
└────────┬─────────┘                        │
         │                                  │
    spawns subprocess                       │
         │                                  │
         ▼                                  │
┌──────────────────┐                 ┌──────┴───────────┐
│   Claude CLI     │── stdio MCP ──►│   cairn-cmd       │
│   (subprocess)   │                │   (MCP server)    │
│                  │◄── stdio MCP ──│   stateless proxy │
└──────────────────┘                └───────────────────┘
```

The host process builds an `Orchestrator` and owns all state: the database, file system, git operations, and active processes. cairn-cmd is a stateless proxy — it receives tool calls from Claude via stdio-based MCP, translates them into HTTP callbacks to the host, and returns the results.

## Session Startup

`start_agent_session()` is the entry point. It takes a run ID, prompt, working directory, agent config, and optional resume session ID. The startup sequence:

1. **Resolve output schema** — if the agent has a structured output requirement, resolve the schema reference, write it to a temp file, and extract the tool name and description.

2. **Generate MCP config** — `ensure_mcp_config()` serializes available agents and tools to JSON arguments, resolves the cairn-cmd binary path, and writes an `mcp-config.json` that tells Claude CLI how to start the MCP server (skills are read on demand via `cairn://skills` resources):

   ```json
   {
     "mcpServers": {
       "cairn": {
         "type": "stdio",
         "command": "/path/to/cairn-cmd",
         "args": ["--agents", "[...]", "--tools", "[...]"],
         "env": {
           "CAIRN_CALLBACK_URL": "http://127.0.0.1:3847/api/mcp",
           "CAIRN_MCP_SECRET": "<base64-encoded-secret>"
         }
       }
     }
   }
   ```

3. **Build CLI arguments** — `build_claude_args()` constructs the full argument list: `--output-format stream-json`, `--mcp-config`, `--model`, `--session-id` or `--resume`, `--append-system-prompt-file` (agent instructions), tool allow/disallow lists, permission mode, and thinking token budget.

4. **Spawn the subprocess** — Claude CLI starts with the constructed arguments. The host captures stdout for event streaming and stores the process handle in `ClaudeProcessState`, keyed by run ID.

5. **Stream events** — a reader thread parses `stream-json` output line by line. Each event is inserted into the `events` table with run ID, session ID, and sequence number, and emitted via the `EventEmitter` service.

For bidirectional mode (`--input-format stream-json`), the prompt is written to stdin after spawning rather than passed as a CLI argument.

## The Callback Loop

When Claude calls a tool, the request flows through three layers:

1. **Claude → cairn-cmd** (stdio MCP): Claude CLI invokes a tool like `mcp__cairn__read`. cairn-cmd's handler constructs a `CallbackRequest`:

   ```rust
   CallbackRequest {
       cwd: String,              // working directory (fallback identifier)
       run_id: Option<String>,   // primary identifier for matching to execution context
       tool: String,             // "read", "write", "bash", "task", etc.
       payload: Value,           // tool-specific input
       tool_use_id: Option<String>,  // links child tasks to parent (batch_tasks)
   }
   ```

2. **cairn-cmd → host** (HTTP POST): the request is sent to the callback URL with bearer token authentication. The host validates the token, extracts the run ID and tool name, and dispatches to the appropriate handler.

3. **Host → cairn-cmd** (HTTP response): the handler executes (DB query, file I/O, process spawn, git operation) using the `Orchestrator`'s services, and returns a `CallbackResponse { result: String }`. cairn-cmd parses the result and returns it to Claude as the tool output.

This design means cairn-cmd never touches the database or filesystem directly. All state mutations go through the host, which maintains consistency and emits events for downstream consumers.

## Authentication

The host generates a 32-byte random secret at startup, base64-encodes it, and stores it in `McpAuthState`. The same value is passed to cairn-cmd via the `CAIRN_MCP_SECRET` environment variable in the MCP config.

On every callback, cairn-cmd sends the secret as a bearer token in the `Authorization` header. The host validates it against the stored value. Failed authentication returns 401.

## Tool Resolution

Native provider tools are fully off. Cairn exposes exactly three working verbs — `mcp__cairn__read`, `mcp__cairn__write`, `mcp__cairn__run` — plus the corpus tools (`create_pr`, `return`, read-only issue/plan access; memories are managed via `write` on node-scoped memory URIs). All operations route through the callback loop, which gives the host control over file I/O, git commits, and process management.

`resolve_tools()` is a small alias map:

- Friendly verbs map to their canonical Cairn verb: `Read → mcp__cairn__read`, `Write`/`Edit` → `mcp__cairn__write`, `Bash → mcp__cairn__run`.
- Every native provider tool (`Task`, `Glob`, `Grep`, `WebFetch`, `WebSearch`, `LSP`, `Skill`, `NotebookEdit`, …) and every dead Cairn name (`mcp__cairn__task`, `batch_tasks`, `ask_user`, `web_fetch`, `web_search`) is dropped from the allow-list.
- For Claude, the disallow list is the union of all native tools, `EnterPlanMode`/`ExitPlanMode`/`TodoWrite`, and the agent's own disallowed list. Codex ignores disallow lists and enforces native-off by simply not allowing native tools.

Needs that native tools used to cover are served by Cairn: sub-agents and user questions go through `write` appends (`…/tasks`, `…/questions`); skills arrive via `cairn://skills` + the slash-command hook; web and local-PDF reads route through `read` → the `bmd` CLI.

## Process Lifecycle

Claude processes have three states:

**Active** — executing a turn. The host is streaming events from stdout, and the process may make tool calls via cairn-cmd callbacks.

**Warm** — idle after completing a turn. The process is still alive with stdin open and the conversation cache preserved in memory. This avoids the cost of re-establishing context when a follow-up message arrives. Warm processes can be resumed by writing a new prompt to stdin, which transitions them back to Active.

**Killed** — process terminated, either by explicit stop or GC eviction.

### Warm Process Management

When a turn completes, `transition_to_warm_state()` marks the process as Warm and updates the run status to completed. The process handle, stdin pipe, and session ID are preserved — MCP auth is not revoked, temp files are not cleaned up.

When the same job or session needs to continue, `find_warm_by_job()` or `find_warm_by_session()` locates the warm process. `transition_to_active()` marks it Active again, and a new message is written to stdin.

`WarmProcessGC` manages eviction. When too many warm processes accumulate, the GC scores them by recency and evicts the least-recently-active ones. Eviction calls `graceful_stop()`, which sends SIGTERM, waits briefly, then SIGKILL if needed.

### Stopping a Process

`kill_session()` handles explicit stops:
- If **Active**: sends an interrupt via stdin, then kills the OS process. The run is finalized as failed.
- If **Warm**: the run is already marked completed, so just the process handle is cleaned up.

## Service Traits

The `Orchestrator` bundles all external dependencies behind trait interfaces in the `Services` struct:

- **ProcessSpawner** — spawn child processes, capture output
- **GitClient** — commit, checkout, push, branch operations
- **HttpClient** — HTTP requests to external APIs
- **EventEmitter** — emit app events (`db-change`, `run-completed`, `dag-advance`)
- **Clock** — current time (injectable for deterministic tests)
- **FileSystem** — file read/write (injectable for tests)
- **PtyFactory** — create PTY sessions for interactive terminal output

All traits are `Arc<dyn Trait>`, and `TestServicesBuilder` provides a builder pattern for injecting mocks in tests. Host runtimes provide production implementations, then construct the runtime with `Orchestrator::builder(...)` (via `cairn_core::internal` when using the unstable host-facing API) so wiring stays explicit and centralized in one place.
