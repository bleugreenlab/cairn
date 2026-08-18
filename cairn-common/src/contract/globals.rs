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

pub(crate) const CHANNELS_CONVERSATIONS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ChannelsConversations,
    uri_template: "cairn://channels/conversations",
    name: "Channel conversations",
    description: "Every active binding on an enabled external-channel provider, with its canonical conversation address, target, outbound message classes, transport capabilities, and current deliverability. Compatibility rows also contain label, provider, and optional last_error.",
    read_projections: &[
        ProjectionSpec { key: "provider", values: "imessage | telegram | discord" },
        ProjectionSpec { key: "deliverability", values: "ready | degraded | stopped" },
    ],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const EXECUTOR_ACTION_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ExecutorAction,
    uri_template: "cairn://executors/{name}/{action}",
    name: "Executor action",
    description: "One machine-local action from an executor's cached tool catalog. Read the URI for its description and input schema; invoke it through run with payload.args_json containing an object of named arguments.",
    read_projections: NO_PROJECTIONS,
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

const GRANT_REVOKE: KeySpec = KeySpec::new(
    "revoke",
    KeyType::Bool,
    "must be true; takes effect on the next authorization check",
);

const GRANT_REVOKED_BY: KeySpec = KeySpec::new(
    "revokedBy",
    KeyType::Str,
    "who revoked it, recorded on the grant",
);

pub(crate) const GRANTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Grants,
    uri_template: "cairn://grants",
    name: "Authority grants",
    description: "Every journaled authority grant in this workspace: the normalized scope it covers (place plus action, e.g. workspace/default/tool/mcp/linear:write), its constraints, who it was issued to, its lifetime and status, and where it came from. A grant is what an operator's approval of a named authority boundary produces — installing or reconfiguring a workspace MCP server, or writing a capability-bearing workspace settings section. Ordinary project work needs no grant and appears nowhere here. Read cairn://grants/<id> for one grant and every decision that cited it; ?view=decisions lists the recent authorization journal instead of the grants, and ?view=leases lists the credential leases currently out — what a grant authorized, actually in someone's hands, with the destination it is bound to and when it expires. This is authorization, not containment: the logical namespace fence still owns host-path and sandbox crossings, and no grant here relaxes one.",
    read_projections: &[ProjectionSpec {
        key: "view",
        values: "grants (default) | decisions — the authorization journal | leases — live credential leases",
    }],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const GRANT_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Grant,
    uri_template: "cairn://grants/{id}",
    name: "Authority grant",
    description: "One journaled authority grant: its normalized scope, typed constraints, principal, audience, lifetime and status, provenance, and every authorization decision that cited it. Patch with revoke:true to revoke it; revocation is recorded with the revoker and takes effect on the very next authorization check, so a standing grant stops authorizing immediately rather than at the end of some window. A grant is never deleted — the journal that cites it has to stay readable.",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[GRANT_REVOKE],
        optional: &[GRANT_REVOKED_BY],
        label: "revoke a grant",
        example: r#"write({changes:[{target:"cairn://grants/GRANT_ID",mode:"patch",payload:{revoke:true}}]})"#,
    }],
};

pub(crate) const EXECUTORS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Executors,
        uri_template: "cairn://executors",
        name: "Fleet",
        description: "Every machine enrolled with this runner, listed by the public name that addresses it. One row per executor: name, platform, toolchains, link state, and how loaded it is right now. These names are exactly what a placement request accepts — run({executor:{name:\"bglab-ub\"}}) or a check's `executor:` block — so what you can target is what you can read here. Read cairn://executors/<name> for one machine in full. This serves the runner's cached fleet state and never provokes a fresh probe. write cairn://executors enrolls a machine over SSH: host and sshUser are the only required keys and everything else is derived. The write returns an enrollment operation id immediately instead of blocking on the SSH bootstrap; the operation's real phases are listed here and on the machine's own URI until it reports Ready.",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[EXECUTOR_HOST, EXECUTOR_SSH_USER],
                optional: &[
                    EXECUTOR_PROJECT_KEYS,
                    EXECUTOR_BINARY_PATH,
                    EXECUTOR_CAIRN_HOME,
                    EXECUTOR_ID,
                    EXECUTOR_DEVICE_ID,
                    EXECUTOR_DISPLAY_NAME,
                    EXECUTOR_TUNNEL_PORT,
                    EXECUTOR_EXTRA_SSH_ARGS,
                ],
                label: "enroll a machine",
                example: r#"write({changes:[{target:"cairn://executors",mode:"create",payload:{host:"bglab-ub.local",sshUser:"mitch"}}]})"#,
            },
        ],
    };

pub(crate) const EXECUTOR_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Executor,
        uri_template: "cairn://executors/{name}",
        name: "Executor",
        description: "Compact operational status for one enrolled machine: identity and platform, link/build/protocol state, timestamped placement telemetry, admission and queue state, and occupancy. Diagnostic attach-session output is drillable with ?view=attach-log rather than dumped into the default status. Placement history is drillable rather than inline: ?view=placements lists informative decisions and collapses routine forced placements; ?view=placement&request=<request-id> renders one complete decision including every passed-over candidate prediction. write patches this machine: newName moves its public address (configuration, enrollment claim, and supervision together), runtimePolicy and draining are live generation-fenced controls that need the expectedGeneration you read here. Draining is what disabling a machine means — it refuses new admissions and leaves resident work alone. delete removes the machine and revokes its enrollment; it is refused while any work or residency remains, so drain first and remove once the counts reach zero. A machine that is enrolled but not attached holds nothing, so its removal completes from registry state even when its host cannot be reached, and says so.",
        read_projections: &[
            ProjectionSpec { key: "view", values: "placements (compact decision list) | placement (one full decision; requires request) | attach-log (full latest attach-session diagnostic)" },
            ProjectionSpec { key: "request", values: "request id from view=placements; valid with view=placement" },
        ],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    EXECUTOR_NEW_NAME,
                    EXECUTOR_RUNTIME_POLICY,
                    EXECUTOR_DRAINING,
                    EXECUTOR_DESKTOP_AUTOMATION,
                    EXECUTOR_EXPECTED_GENERATION,
                ],
                label: "configure an enrolled machine",
                example: r#"write({changes:[{target:"cairn://executors/bglab-ub",mode:"patch",payload:{draining:true,expectedGeneration:7}}]})"#,
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "remove a machine and revoke its enrollment",
                example: r#"write({changes:[{target:"cairn://executors/bglab-ub",mode:"delete"}]})"#,
            },
        ],
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
                    MCP_OAUTH,
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
                    MCP_OAUTH,
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
