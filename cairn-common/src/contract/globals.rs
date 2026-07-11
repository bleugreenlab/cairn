//! Global resource contracts.
//!
//! Verbatim `ResourceContract` table entries, assembled into
//! `RESOURCE_CONTRACTS` by the module facade in table order.

use super::specs::*;
use super::types::*;

pub(crate) const DB_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Db,
        uri_template: "cairn://db",
        name: "Live database SQL projection",
        description: "Read-only SQL against the running app's existing local database connection. Requires ?sql=... and supports offset/limit row windows. EXPLAIN and EXPLAIN QUERY PLAN are permitted for inspecting query plans. Write SQL verbatim: the sql value is taken literally, so a `%` (a LIKE wildcard) and spaces need no URL-encoding.",
        read_projections: &[
            ProjectionSpec { key: "sql", values: "read-only SELECT/WITH, EXPLAIN [QUERY PLAN], or schema PRAGMA (verbatim; a literal % LIKE wildcard needs no encoding)" },
            ProjectionSpec { key: "offset", values: "N rows to skip (default 0)" },
            ProjectionSpec { key: "limit", values: "N rows (default 100, max 1000)" },
        ],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    };

pub(crate) const DEV_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Dev,
        uri_template: "cairn://dev",
        name: "Dev instance introspection",
        description: "Process-introspection tools for a running `bun run dev:instance` (the per-branch dev build you launched). read cairn://dev lists running instances and the available sub-tools: cairn://dev/db (read-only SQL against the instance's database) and cairn://dev/pid (the instance's OS process id, e.g. to target it with Axon accessibility).",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    };

pub(crate) const DEV_DB_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::DevDb,
        uri_template: "cairn://dev/db",
        name: "Dev instance database SQL projection",
        description: "Read-only SQL against a running `bun run dev:instance` database (the per-branch dev build you launched), not the host app's own DB. The instance holds a process lock on its database file, so this queries the instance's own MCP callback server, which means the instance must be running. Same statement policy as cairn://db (SELECT, read-only WITH, EXPLAIN [QUERY PLAN], schema PRAGMAs) with offset/limit row windows. read cairn://dev/db with no ?sql lists registered instances and their running state; ?at=<branch-or-key> selects one (optional when exactly one is registered, or exactly one is running).",
        read_projections: &[
            ProjectionSpec { key: "sql", values: "read-only SELECT/WITH, EXPLAIN [QUERY PLAN], or schema PRAGMA (verbatim; a literal % LIKE wildcard needs no encoding); omit to list dev instances" },
            ProjectionSpec { key: "at", values: "branch name or slug key of the dev instance to query" },
            ProjectionSpec { key: "offset", values: "N rows to skip (default 0)" },
            ProjectionSpec { key: "limit", values: "N rows (default 100, max 1000)" },
        ],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    };

pub(crate) const DEV_PID_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::DevPid,
        uri_template: "cairn://dev/pid",
        name: "Dev instance process id",
        description: "The OS process id(s) of the desktop GUI attached to running `bun run dev:instance`(s). Each desktop registers its own std::process::id() with its runner over WebSocket (authoritative, no lsof), so a caller can target the window with external tools such as Axon accessibility without shelling out. A windowless runner is reported as unavailable, never as the GUI pid. read cairn://dev/pid lists every running instance; ?at=<branch-or-key> selects one.",
        read_projections: &[
            ProjectionSpec { key: "at", values: "branch name or slug key of the dev instance to target" },
        ],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    };

pub(crate) const LOGS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Logs,
        uri_template: "cairn://logs",
        name: "App logs",
        description: "Read-only projection of the running app's JSONL log entries — the same files behind Settings \u{2192} Logs. Selects one daily file by ?process= (and optional ?date=) and renders recent entries as plain greppable lines, most recent last. Filter by level/target/text with universal grep (e.g. ?grep=ERROR); window with offset/limit (negative offset tails the most recent N).",
        read_projections: &[
            ProjectionSpec { key: "process", values: "app (default) | mcp | server — which log file family" },
            ProjectionSpec { key: "date", values: "YYYY-MM-DD; default is the newest available file for the process" },
            ProjectionSpec { key: "offset", values: "N lines to skip (negative tails the most recent N)" },
            ProjectionSpec { key: "limit", values: "N lines to return" },
        ],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    };

pub(crate) const MCP_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Mcp,
        uri_template: "cairn://mcp/{server}/{tool-or-resource}",
        name: "External MCP gateway",
        description: "Configured external MCP servers reached through Cairn as a client. read cairn://mcp lists servers; read cairn://mcp/<server> shows tool inputSchemas + resources; read cairn://mcp/<server>/<resource-uri> proxies resources/read. Invoke a tool with run {target:\"cairn://mcp/<server>/<tool>\", payload:{args_json:{...}}} (every tools/call goes through run, never write). write cairn://mcp manages the server registry: create a new server, patch or delete one by name. A workspace-scope write edits ~/.cairn/settings.yaml and is gated by the same worktree fence as any out-of-worktree write; a project-scope write edits the run's .cairn/config.yaml in place.",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[MCP_NAME],
                optional: &[
                    MCP_TYPE,
                    MCP_COMMAND,
                    MCP_ARGS,
                    MCP_ENV,
                    MCP_URL,
                    MCP_HEADERS,
                    MCP_ENABLED,
                    MCP_SCOPE,
                ],
                label: "add MCP server",
                example: "write({changes:[{target:\"cairn://mcp\",mode:\"create\",payload:{name:\"playwright\",command:\"npx\",args:[\"@playwright/mcp@latest\"]}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    MCP_TYPE,
                    MCP_COMMAND,
                    MCP_ARGS,
                    MCP_ENV,
                    MCP_URL,
                    MCP_HEADERS,
                    MCP_ENABLED,
                    MCP_SCOPE,
                ],
                label: "edit MCP server",
                example: "write({changes:[{target:\"cairn://mcp/playwright\",mode:\"patch\",payload:{enabled:false}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[MCP_SCOPE],
                label: "remove MCP server",
                example: "write({changes:[{target:\"cairn://mcp/playwright\",mode:\"delete\"}]})",
            },
        ],
    };

pub(crate) const HELP_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Help,
        uri_template: "cairn://help",
        name: "Help",
        description: "Complete on-demand reference: URI grammar, the read catalog, and the full (resource, mode) mutation matrix",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    };

pub(crate) const WEB_SEARCH_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::WebSearch,
        uri_template: "cairn://websearch?q={query}",
        name: "Web search",
        description: "Run a web search through the active typed web-search provider (Settings → Web Services) and get back a normalized ranked list of title · url · snippet results to read and then fetch. The query rides in ?q= as literal text — spaces are fine, no manual URL-encoding. Web search is opt-in: with no provider configured the read returns a clear setup message.",
        read_projections: &[ProjectionSpec {
            key: "q",
            values: "the search query (literal text; spaces and punctuation need no encoding)",
        }],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    };
