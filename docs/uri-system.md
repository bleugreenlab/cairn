# URI System

Cairn uses `cairn://` URIs to address every readable resource in the system — projects, issues, execution nodes, transcripts, artifacts, and terminals. URIs are the primary API surface that agents use to navigate execution history.

## Hierarchy

Resources are organized in a strict containment hierarchy:

```
cairn://PROJECT
├── /messages
├── /chat/NAME
├── /terminal/SLUG
│
└── /NUMBER                              (issue)
    ├── /files
    ├── /messages
    │
    └── /EXEC/NODE                       (execution sequence + node name)
        ├── /chat
        ├── /chat/full
        ├── /chat/RUN_SEQ/EVENT_SEQ
        ├── /artifact
        ├── /files
        ├── /terminal/SLUG
        │
        └── /task/NAME                   (sub-agent task)
            ├── /chat
            ├── /chat/full
            ├── /chat/RUN_SEQ/EVENT_SEQ
            └── /artifact
```

**EXEC** is a 1-based execution sequence number. The same issue can be executed multiple times (retries, re-runs), and each execution gets its own sequence number. Every node-scoped and task-scoped URI requires an execution sequence to be unambiguous — `cairn://CAIRN/123/planner-1/chat` is rejected because it doesn't specify which execution's planner.

**NODE** is the human-readable node name from the execution snapshot (e.g., `Planner`, `builder-1`). These names come from recipe node configuration and are unique within an execution.

**NAME** for tasks is the sub-agent task name. Duplicate names within a node get a `-N` suffix (`Explore`, `Explore-2`, `Explore-3`).

## The CairnResource Enum

All URI variants are defined in a single Rust enum (`cairn-common/src/uri.rs`):

**Project-level** (no issue context):
- `Project` — project overview
- `ProjectMessages` — cross-agent message channel
- `ProjectTerminal` — background terminal output
- `ProjectChat` — project chat session transcript

**Issue-level** (project + issue number):
- `Issue` — issue overview with execution history
- `IssueMessages` — issue-scoped message channel
- `Files` — files changed across all executions

**Node-level** (project + number + exec_seq + node_id):
- `Node` — node summary with metadata
- `NodeChat` / `NodeChatFull` — transcript (compact vs full)
- `NodeChatEvent` — single event by run_seq + event_seq
- `NodeArtifact` — structured output
- `NodeFiles` — files changed by this node
- `NodeTerminal` — node-scoped terminal output

**Task-level** (all node fields + task_name):
- `TaskChat` / `TaskChatFull` — sub-task transcript (compact vs full)
- `TaskChatEvent` — single sub-task event
- `TaskArtifact` — sub-task output

`/chat` responses keep tool calls/results concise and include event URI pointers for deep inspection. Use `/chat/full` or `.../chat/{run_seq}/{event_seq}` when full tool payload bodies are needed.

Each variant carries exactly the fields needed to resolve it — no optional fields, no overloading.

## Parsing

URI parsing uses a two-stage dispatch pattern:

**Stage 1: `parse_uri()`** strips the `cairn://` prefix, splits on `/`, and dispatches based on segment count. One-segment paths resolve to `Project`, two-segment to `Issue`, three-segment to project-level resources (`terminal/SLUG`, `chat/NAME`) or issue-level resources (`files`, `messages`). Anything deeper delegates to stage 2.

**Stage 2: `parse_issue_scoped()`** handles everything after `PROJECT/NUMBER`. The first segment must parse to a positive integer (the execution sequence). This is the gate that rejects old-format URIs without an execution sequence — if the segment isn't a valid `i32 > 0`, the parse returns `None`. After extracting exec_seq and node_id, remaining segments are matched as keywords: `chat`, `chat/full`, `artifact`, `files`, `terminal/SLUG`, or `task/NAME/...`.

Both stages are pure functions with no database access. They return `Option<CairnResource>` — invalid URIs produce `None`, not errors.

## Serialization

`CairnResource` supports two representations:

- **`to_uri()`** — canonical `cairn://PROJECT/...` format, used in agent-facing contexts
- **`to_route()`** — path format `/p/project/i/123/...` with lowercase project key, suitable for URL routing

## Dispatch

When an agent calls the `read_uri` MCP tool, the URI flows through three layers:

1. **cairn-mcp** parses the URI with `parse_uri()` and decides the dispatch target. Terminal URIs (`NodeTerminal`, `ProjectTerminal`) route to a `read_resource` callback that reads terminal output. All other URIs route to `read_issue_resource`.

2. **The host's callback server** receives the HTTP request and passes it to cairn-core.

3. **`handle_read_issue_resource()`** in cairn-core pattern-matches on the `CairnResource` variant and executes the appropriate database query: look up the project by key, find the issue by number, select the execution by sequence, find the job by node name, then fetch the requested sub-resource (transcript events, artifact data, file changes, etc.).

The separation between terminal and non-terminal dispatch exists because terminal output lives in the process management layer (active PTY sessions or saved output), not in the database.
