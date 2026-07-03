# URI System

Cairn uses `cairn://` URIs to address every readable resource in the system: projects, issues, execution nodes, transcripts, artifacts, and terminals. URIs are the primary API surface that agents use to navigate execution history.

## Hierarchy

Resources are organized in a strict containment hierarchy under explicit project scope:

```
cairn://p/PROJECT
├── /issues
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
        ├── /chat/raw
        ├── /chat/turn/N
        ├── /chat/RUN_SEQ/EVENT_SEQ
        ├── /artifact
        ├── /files
        ├── /terminal/SLUG
        │
        └── /task/NAME                   (sub-agent task)
            ├── /chat
            ├── /chat/raw
            ├── /chat/turn/N
            ├── /chat/RUN_SEQ/EVENT_SEQ
            └── /artifact
```

**`p`** is the explicit project-scope namespace. Legacy root-as-project forms such as `cairn://CAIRN/123` are rejected.

**EXEC** is a 1-based execution sequence number. The same issue can be executed multiple times (retries, re-runs), and each execution gets its own sequence number. Every node-scoped and task-scoped URI requires an execution sequence to be unambiguous: `cairn://p/CAIRN/123/planner-1/chat` is rejected because it does not specify which execution's planner.

**NODE** is the node job URI segment stored on `jobs.uri_segment` (for example, `planner` or `builder-1`). For older rows that predate segment storage, readers may fall back to legacy derivation.

**NAME** for tasks is the child task job segment stored on `jobs.uri_segment`. New task segments are allocated at creation time and are unique among siblings; collisions append `-N` (`explore`, `explore-2`, `explore-3`).

For project chat resources (`cairn://p/PROJECT/chat/NAME`), `NAME` is the stored `chats.uri_segment` value.

## The CairnResource Enum

All URI variants are defined in a single Rust enum (`cairn-common/src/uri.rs`).

Project-level:
- `Project`
- `ProjectIssues`
- `ProjectMessages`
- `ProjectTerminal`
- `ProjectChat`

Issue-level:
- `Issue`
- `IssueMessages`
- `Files`

Node-level:
- `Node`
- `NodeChat` / `NodeChatFull`
- `NodeChatTurn`
- `NodeChatEvent`
- `NodeArtifact`
- `NodeFiles`
- `NodeTerminal`

Task-level:
- `TaskChat` / `TaskChatFull`
- `TaskChatTurn`
- `TaskChatEvent`
- `TaskArtifact`

`/chat` is a turn-structured digest: per-turn sections with one row per tool-call target and a right-pinned count figure (`?latest=true` orders newest-first). Use `/chat/turn/N` for a full turn, `/chat/raw` for the unsummarized stream, or `.../chat/RUN_SEQ/EVENT_SEQ` for a single event's full payload.

Each variant carries exactly the fields needed to resolve it.

## Parsing

URI parsing uses a two-stage dispatch pattern:

**Stage 1: `parse_uri()`** strips the `cairn://` prefix, strips query fragments for identity, then validates scope. The first segment must be `p`, followed by a project key. Project, issue, and project-scoped resources (`issues`, `messages`, `chat/NAME`, `terminal/SLUG`) are resolved here; deeper paths delegate to stage 2.

**Stage 2: `parse_issue_scoped()`** handles everything after `p/PROJECT/NUMBER`. The first remaining segment must be a positive integer execution sequence. This rejects legacy forms without explicit execution sequence. After extracting `exec_seq` and `node_id`, the parser matches `chat`, `chat/raw`, `chat/turn/N`, `chat/RUN_SEQ/EVENT_SEQ`, `artifact`, `files`, `terminal/SLUG`, and `task/NAME/...`.

Both stages are pure functions with no database access. Invalid URIs return `None`.

Database-backed handlers then resolve node/task/project-chat resources by exact stored segment identity. This prevents route drift when visible labels or naming heuristics change.

## Serialization

`CairnResource` supports two representations:

- `to_uri()` - canonical `cairn://p/PROJECT/...` format, used in agent-facing contexts
- `to_route()` - path format `/p/project/i/123/...` with lowercase project key, suitable for URL routing where a UI route exists (`ProjectIssues` intentionally returns `None`)

## Dispatch

When an agent calls the `read` MCP tool with a `cairn://` URI, the URI flows through three layers:

1. `cairn-cmd` parses the URI with `parse_uri()` and decides dispatch target. Terminal URIs (`NodeTerminal`, `ProjectTerminal`) route to a callback that reads terminal output. All other URIs route to issue-resource handlers.
2. The host callback server receives the request and forwards it to cairn-core.
3. `handle_read_issue_resource()` in cairn-core pattern-matches on the `CairnResource` variant and executes the appropriate database query.

Terminal and non-terminal dispatch stay separate because terminal output lives in process/session state, not relational issue tables.

## Segment Persistence And Backfill

URI segments are first-class persisted identity fields:

- `jobs.uri_segment` for node and task resources
- `chats.uri_segment` for project chat resources

Migration backfill deterministically allocates missing legacy segments and resolves natural collisions by adding numeric suffixes in scope, so each segment is unique where it is queried:

- `(issue_id, execution_id, uri_segment)` for top-level execution nodes
- `(parent_job_id, uri_segment)` for child tasks
- `(project_id, uri_segment)` for project chats
