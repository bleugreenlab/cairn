//! Global resource contracts.
//!
//! Verbatim `ResourceContract` table entries, assembled into
//! `RESOURCE_CONTRACTS` by the module facade in table order.

use super::specs::*;
use super::types::*;
use cairn_uri::ResourceKind;

pub(crate) const DB_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Db,
        uri_template: "cairn://db",
        name: "Live database SQL projection",
        description: "Read-only access to the running app's existing local database connection. Read it bare for a compact schema overview (tables, columns with types, each table's max rowid as a size that costs a seek rather than a count, index counts), or use ?sql= with offset/limit row windows. A ?sql= read is cut after 60 s (product reads keep a 10-minute bound), because a long read holds a snapshot of the live database open, keeping every later row version in memory and blocking the log fold; a cut read is refused naming its elapsed time and statement. Size a table from the overview's max rowid, read ?table= for its shape, and run EXPLAIN QUERY PLAN first on anything that might scan a large table. EXPLAIN and EXPLAIN QUERY PLAN are permitted for inspecting query plans. Write SQL verbatim: the sql value is taken literally, so a `%` (a LIKE wildcard) and spaces need no URL-encoding.",
        read_projections: &[
            ProjectionSpec { key: "sql", values: "read-only SELECT/WITH, EXPLAIN [QUERY PLAN], or schema PRAGMA (verbatim; a literal % LIKE wildcard needs no encoding); cut after 60 s" },
            ProjectionSpec { key: "table", values: "NAME — one table's full column list and every index on it (columns or expressions, unique/partial marks, a partial index's WHERE predicate), no elision (overview only)" },
            ProjectionSpec { key: "offset", values: "N rows to skip (default 0)" },
            ProjectionSpec { key: "limit", values: "N rows (default 100, max 1000)" },
            ProjectionSpec { key: "format", values: "json: with sql=, JSONL rows; without it, the overview as one document (with table=, every column's type, nullability, and key); omit for text" },
        ],
        related: &[RelatedSpec {
            label: "one table's rows, each addressed by primary key",
            kind: ResourceKind::DbTable,
            actions: false,
        }],
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    };

pub(crate) const DB_TABLE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::DbTable,
    uri_template: "cairn://db/{table}",
    name: "Live database table",
    description: "The rows of one table in the running app's database, with the same read grammar as a thread database table. Structurally read-only: every mutation mode is refused. The affordance block lists the live columns, types, nullability, and primary key. The segment `schema` is reserved.",
    read_projections: super::issues::DB_TABLE_PROJECTIONS,
    related: &[RelatedSpec {
        label: "database overview and SQL",
        kind: ResourceKind::Db,
        actions: false,
    }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const DB_ROW_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::DbRow,
    uri_template: "cairn://db/{table}/{pk}",
    name: "Live database row",
    description: "One row of an application database table, by single-column primary key. Structurally read-only: every mutation mode is refused.",
    read_projections: super::issues::DB_ROW_PROJECTIONS,
    related: &[RelatedSpec {
        label: "database overview and SQL",
        kind: ResourceKind::Db,
        actions: false,
    }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const CHANNELS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Channels,
    uri_template: "cairn://channels",
    name: "Channels",
    description: "The channel providers, one row each: the in-app channel (`app`, whose conversations are the operator's own messages at cairn://operator) and the external transports (Discord, iMessage, Telegram), each with whether it is enabled, its runtime health and detail (Discord's card-pin degradation is reported here, once), the operator's purpose line for it, operator presence (for iMessage, the presence its local probe reads), when the operator last wrote on it, and a link to its conversations at cairn://channels/{provider}. The purpose is the line an agent reads to decide which provider a message belongs on. Only the verified desktop operator may patch {provider, purpose}; an agent's or any run-less caller's patch is refused.",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[CHANNEL_PROVIDER, CHANNEL_PURPOSE],
        optional: &[],
        label: "set a provider's purpose (desktop operator only)",
        example: "write({changes:[{target:\"cairn://channels\",mode:\"patch\",payload:{provider:\"telegram\",purpose:\"urgent: reaches the operator's phone\"}}]})",
    }],
};

pub(crate) const CHANNELS_PROVIDER_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ChannelsProvider,
    uri_template: "cairn://channels/{provider}",
    name: "Channel conversations",
    description: "The conversations on one provider that the reader can reach, newest activity first. A thread reaches its own channel, its project's ambient channel, and any conversation bound to it with agent text; an issue node reaches its issue's channel and the project's ambient channel; the verified desktop operator reaches every active binding. A request that speaks for no run and carries no operator credential reaches none. Each row is the conversation's resource URI, its label (`#channels · cairn · Discord`), its provider address, the binding kind, the classes the reader may send (`agent_text` only while a send would be admitted), its last activity, and a copy-paste send.",
    read_projections: &[
        ProjectionSpec { key: "limit", values: "N rows (default 100, max 1000)" },
        ProjectionSpec { key: "project", values: "project key; only conversations whose binding targets it (the operator's view narrowed to one project)" },
    ],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const CHANNELS_CONVERSATION_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ChannelsConversation,
    uri_template: "cairn://channels/{provider}/{conversation}",
    name: "Channel conversation",
    description: "One conversation the reader can reach: a header (label, provider address, binding, target, provider health, the send affordance) and then its messages oldest first, inbound and outbound interleaved, each with its Cairn id. An inbound message shows its sender, text, the address it opened with, and what the router did: delivered to a target, logged for routes (with each route firing, its outcome, and the acknowledgement it sent), or rejected with a reason. An outbound message shows its sender (a thread, a route, or Cairn), kind, text, and delivery state: pending, deferred for operator presence, sent, failed with its attempt count and next retry, resolved, or expired. A Discord conversation is cairn://channels/discord/PROJECT (the project's #ambient) or cairn://channels/discord/PROJECT/THREAD_OR_ISSUE; Telegram's is cairn://channels/telegram/CHAT_ID and iMessage's cairn://channels/imessage/HANDLE. Append {content} to send plain text under the sender's attribution; a reply to it routes back to the sending thread, or to the sending node's issue.",
    read_projections: &[
        ProjectionSpec { key: "since", values: "epoch seconds; messages at or after" },
        ProjectionSpec { key: "limit", values: "N newest messages (default 50, max 500)" },
        ProjectionSpec { key: "direction", values: "in | out | all (default all)" },
    ],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[CHANNEL_MESSAGE_CONTENT],
        optional: &[],
        label: "send a message to this conversation",
        example: "write({changes:[{target:\"cairn://channels/discord/PROJECT/THREAD\",mode:\"append\",payload:{content:\"...\"}}]})",
    }],
};

pub(crate) const CHANNELS_AMBIENT_FEEDBACK_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ChannelsAmbientFeedback,
    uri_template: "cairn://channels/ambient/feedback",
    name: "Ambient routing feedback",
    description: "The ambient Discord route's label set: one row per routing outcome, swept from the event log every five minutes. An admitted message has one row per thread it was delivered to; any other outcome (below_threshold, refused, unanswerable, failed) has one refused-side row, and an event the sweep reached after the route's firing journal evicted it reads `unrecorded`. Each row snapshots the pick, probability, method, charter revision and admitted picks, beside the label and the passive engagement the sweep records 48 hours after the event (engaged when the recipient cited the event or sent to its conversation; on a refused-side row, engaged_by names the first thread that cited it). Append {event, verdict} to label a delivery: the labeller is the writing thread, never the payload, and only a chartered thread or the operator may write. `not_mine` disowns a delivery on the writer's own row. `reroute` with `to` names the thread that owns it: on the recipient's own row it also delivers the original message once to that thread's messages; from anyone else it records where the event ended up on the refused-side row and delivers nothing. Nothing is sent to Discord: an ambient conversation grants no agent free text by default (tenet 1031, cairn/4884).",
    read_projections: &[
        ProjectionSpec { key: "since", values: "epoch seconds; events at or after" },
        ProjectionSpec { key: "limit", values: "N rows (default 50, max 500)" },
        ProjectionSpec { key: "verdict", values: "not_mine | reroute" },
        ProjectionSpec { key: "outcome", values: "admitted | below_threshold | refused | unanswerable | failed | unrecorded" },
        ProjectionSpec { key: "thread", values: "thread URI; rows delivered or re-routed to it" },
        ProjectionSpec { key: "engaged", values: "true | false | unchecked" },
    ],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[AMBIENT_FEEDBACK_EVENT, AMBIENT_FEEDBACK_VERDICT],
        optional: &[AMBIENT_FEEDBACK_TO, AMBIENT_FEEDBACK_NOTE],
        label: "label an ambient routing",
        example: "write({changes:[{target:\"cairn://channels/ambient/feedback\",mode:\"append\",payload:{event:\"cairn://p/PROJECT/events/ID\",verdict:\"reroute\",to:\"cairn://p/PROJECT/THREAD\"}}]})",
    }],
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

pub(crate) const DEV_LAUNCH_TICKET_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::DevLaunchTicket,
        uri_template: "cairn://dev/launch-ticket",
        name: "Dev instance launch ticket",
        description: "Issues the calling run a one-time ticket that admits one connection to the runner's dev-instance launch route within a minute. `bun dev:instance` asks for one itself: an agent shell has no credential it could otherwise present to that route, because the route is a WebSocket on the runner and an agent shell reaches the runner only through its executor's request relay.",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[],
            optional: &[],
            label: "issue a one-time launch ticket (a run only)",
            example: "write({changes:[{target:\"cairn://dev/launch-ticket\",mode:\"append\",payload:{}}]})",
        }],
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
    description: "One journaled authority grant: its normalized scope, typed constraints, principal, audience, lifetime and status, provenance, and every authorization decision that cited it. Patch with revoke:true to revoke a grant you hold: a once, turn or session grant minted for your own run, a standing grant bound to you, or an execution-queue grant bound to the thread your run acts for (a thread holds its queue grants whatever their lifetime). Anyone else's grant, and every standing grant with no principal (those are workspace-wide), is the operator's to revoke at Settings → Grants in the Cairn desktop app, and a patch here is refused naming whose grant it is. Revocation records the revoker the runner resolved, never a name the caller gives, and takes effect on the very next authorization check, so a standing grant stops authorizing immediately rather than at the end of some window. A grant is never deleted — the journal that cites it has to stay readable.",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[GRANT_REVOKE],
        optional: &[],
        label: "revoke a grant you hold (the operator revokes any grant at Settings → Grants)",
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
                    EXECUTOR_EXTRA_SSH_ARGS,
                    EXECUTOR_ROLE,
                ],
                label: "enroll a machine",
                example: r#"write({changes:[{target:"cairn://executors",mode:"create",payload:{host:"bglab-ub.local",sshUser:"mitch"}}]})"#,
            },
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    KeySpec::new("regenerateExecutorListenerCertificate", KeyType::Bool, "stage a new certificate and wait for every enrolled machine"),
                    KeySpec::new("forceListenerRotationCutover", KeyType::Bool, "promote despite unacknowledged machines"),
                    KeySpec::new("confirm", KeyType::Bool, "required for force cutover when machines would be stranded"),
                ],
                label: "rotate the executor listener certificate",
                example: r#"regenerate: write({changes:[{target:"cairn://executors",mode:"patch",payload:{regenerateExecutorListenerCertificate:true}}]}); force after naming casualties: write({changes:[{target:"cairn://executors",mode:"patch",payload:{forceListenerRotationCutover:true,confirm:true}}]})"#,
            },
        ],
    };

pub(crate) const EXECUTOR_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Executor,
        uri_template: "cairn://executors/{name}",
        name: "Executor",
        description: "Compact operational status for one enrolled machine: identity and platform, link/build/protocol state, timestamped placement telemetry, admission and queue state, and occupancy. Diagnostic attach-session output is drillable with ?view=attach-log rather than dumped into the default status. Placement history is drillable rather than inline: ?view=placements lists informative decisions and collapses routine forced placements; ?view=placement&request=<request-id> renders one complete decision including every passed-over candidate prediction. write patches this machine: newName moves its public address (configuration, enrollment claim, and supervision together), runtimePolicy is a live generation-fenced control that needs the expectedGeneration you read here. Drains are independent runner-owned operator, thread, runtime or imported legacy records. Reads show every owner, reason, record generation and provenance on attached or detached machines, effective refusal and separately labeled removal fences. draining:true with optional reason imposes the authenticated owning thread's record; omission stores exactly no reason supplied. draining:false clears only the acting thread's entry and imported legacy state, never other owners. Ordinary operator clearing requires an accepted lift_drain request. Imposition needs no live connection generation and persists across reconnects and restarts. Any remaining record or fence continues to refuse new admission, including pinned batches; existing work continues. role (buildsOnly, general, inferenceOnly, servicesOnly) says what the machine is for, in the same durable unfenced shape: placement never offers it a class its role refuses, and an explicit pin by name for such a class is refused naming the machine, its role, and the class. A role change refuses new admissions and stops nothing: work already running there finishes. buildRoot (local only) moves the runner's own executor's cells, managed objects, dependency catalog and build caches to a directory on another volume from the next runner start; while that volume is unplugged the machine stays attached, reads buildVolumeAbsent(<path>), and declines work that needs a cell, and its cells are adopted again when it returns. delete requires a caller-retained operationId and removes the machine and revokes its enrollment; retry an ambiguous failure with the same operationId and frozen stable executor ID to recover the receipt without repeating effects; it is refused while any work or residency remains, so drain first and remove once the counts reach zero, or pass confirm:true to acknowledge stranded occupancy. Confirmation never bypasses drain ownership: removal can clear only a credential-proven acting operator's own record and imported legacy state, and refuses another principal's operator record, thread/runtime drains and pre-existing fences. A machine that is enrolled but not attached holds nothing, so its removal completes from registry state even when its host cannot be reached, and says so.",
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
                    EXECUTOR_DRAIN_REASON,
                    EXECUTOR_ROLE,
                    EXECUTOR_MACHINE_SERVICES,
                    EXECUTOR_REPAIRED,
                    EXECUTOR_BUILD_ROOT,
                    EXECUTOR_MEMORY_DIAGNOSTIC,
                    EXECUTOR_EXPECTED_GENERATION,
                ],
                label: "configure an enrolled machine",
                example: r#"write({changes:[{target:"cairn://executors/bglab-ub",mode:"patch",payload:{draining:true}}]})"#,
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[EXECUTOR_REMOVAL_OPERATION_ID],
                optional: &[EXECUTOR_REMOVAL_CONFIRM],
                label: "remove a machine and revoke its enrollment",
                example: r#"write({changes:[{target:"cairn://executors/bglab-ub",mode:"delete",payload:{operationId:"unique-removal-id"}}]})"#,
            },
        ],
    };

pub(crate) const MCP_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Mcp,
        uri_template: "cairn://mcp/{server}/{tool-or-resource}",
        name: "External MCP gateway",
        description: "Configured external MCP servers reached through Cairn as a client. read cairn://mcp lists servers; read cairn://mcp/<server> shows the calling run's connection to it (connected, not connected, or closed, plus how its last server process ended and that process's last stderr lines), tool inputSchemas, and resources; read cairn://mcp/<server>/<resource-uri> proxies resources/read. Invoke a tool with run {target:\"cairn://mcp/<server>/<tool>\", payload:{args_json:{...}}} (every tools/call goes through run, never write). A machine-service server (such as axon, desktop automation) runs on many machines, each with its own catalog: read cairn://mcp/<server> renders one section per machine and its state, never merged, and run's call-level executor:{name} or {os} chooses which machine serves the batch's calls (required when several serve it; the node default never chooses it). write cairn://mcp manages the server registry: create a new server, patch or delete one by name. A patch with action:restart instead drops the calling run's pooled connection to that server so its next call reconnects; a call already finds and replaces a closed connection on its own, so restart is for a server that is alive but wedged. A workspace-scope write edits ~/.cairn/settings.yaml and is gated by the same worktree fence as any out-of-worktree write; a project-scope write edits the run's .cairn/config.yaml in place.",
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
                mode: ChangeMode::Patch,
                required: &[MCP_ACTION],
                optional: &[],
                label: "restart this run's connection to an MCP server",
                example: "write({changes:[{target:\"cairn://mcp/playwright\",mode:\"patch\",payload:{action:\"restart\"}}]})",
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
        description: "Run a web search through the active web-search provider (selected in the Library) and get back a normalized ranked list of title · url · snippet results to read and then fetch. The query rides in ?q= as literal text — spaces are fine, no manual URL-encoding. Web search is opt-in: with no provider configured the read returns a clear setup message.",
        read_projections: &[ProjectionSpec {
            key: "q",
            values: "the search query (literal text; spaces and punctuation need no encoding)",
        }],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    };
