//! Reusable `KeySpec`/projection/related/cross-action constants shared by the
//! `RESOURCE_CONTRACTS` table entries.

use super::types::*;

// Reusable key specs. Notes carry value guidance (enumerations/defaults) only
// where the key name + type + example don't already make the value obvious; an
// empty note renders as just `key(type)`.
pub(crate) const CONTENT: KeySpec = KeySpec::new("content", KeyType::Str, "");
/// Node/task direct messages only. A project or issue channel append ignores
/// this key, so declaring it there would advertise a promise that surface does
/// not keep.
pub(crate) const ESCALATE: KeySpec = KeySpec::new(
    "escalate",
    KeyType::Bool,
    "interrupt the recipient's active turn instead of queueing for its next one",
);
pub(crate) const PROGRESS_KIND: KeySpec = KeySpec::new("kind", KeyType::Str, "phase | log");
pub(crate) const PROGRESS_TEXT: KeySpec = KeySpec::new(
    "text",
    KeyType::Str,
    "phase name (kind=phase) or log message (kind=log)",
);
pub(crate) const EXECUTOR_DESKTOP_AUTOMATION: KeySpec = KeySpec::with_aliases(
    "desktopAutomation", &["desktop_automation"], KeyType::Bool,
    "whether desktop automation is enabled for this machine; stored fleet configuration, not an authorization gate",
);
pub(crate) const REBASE_RESOLUTION: KeySpec = KeySpec::new(
    "resolution",
    KeyType::Str,
    "take-committed-tip — explicitly use the branch's committed tip content for the session's conflicting paths",
);
pub(crate) const REBASE_DROP_INCOMING_REASON: KeySpec = KeySpec::new(
    "drop_incoming_reason",
    KeyType::Str,
    "why discarding the incoming hunks a take-committed-tip would drop is correct; only needed when the replay is refused for dropping them",
);
pub(crate) const OLD_STRING: KeySpec = KeySpec::new(
    "old_string",
    KeyType::Str,
    "text replacement operation key; not stored as artifact metadata",
);
pub(crate) const NEW_STRING: KeySpec = KeySpec::new(
    "new_string",
    KeyType::Str,
    "text replacement operation key; not stored as artifact metadata",
);
pub(crate) const REPLACE_ALL: KeySpec = KeySpec::new(
    "replace_all",
    KeyType::Bool,
    "replace all old_string matches; default false errors if old_string is non-unique",
);
pub(crate) const SUBMIT: KeySpec = KeySpec::new(
    "submit",
    KeyType::Bool,
    "send as a command line (append newline if missing); set false to send bytes verbatim. default true",
);
pub(crate) const FIELD: KeySpec = KeySpec::new(
    "field",
    KeyType::Str,
    "top-level string artifact field to edit; defaults to content then body",
);
pub(crate) const COMMAND: KeySpec = KeySpec::new("command", KeyType::Str, "");
pub(crate) const REPL_INTERPRETER: KeySpec = KeySpec::new(
    "interpreter",
    KeyType::Str,
    "python (py) | typescript (ts) — the language this REPL evaluates",
);
pub(crate) const REPL_DEPS: KeySpec = KeySpec::new(
    "deps",
    KeyType::Array,
    "python-only: package names preloaded via uv (uv run --with)",
);
pub(crate) const WAKE: KeySpec = KeySpec::new(
    "wake",
    KeyType::Str,
    "\"exit\" to resume when the command finishes, or a literal output phrase to resume when it prints (also fires on exit)",
);
pub(crate) const BROWSER_URL: KeySpec = KeySpec::with_aliases(
    "url",
    &["navigate"],
    KeyType::Str,
    "navigate the browser to this URL",
);
/// The full browser patch action vocabulary. Advertised verbatim in
/// [`BROWSER_ACTION`] and pinned against the set `apply_browser_action` actually
/// handles by a cairn-core test, so the structured affordance can't silently
/// under-advertise an action the dispatch really accepts.
pub const BROWSER_ACTIONS: &[&str] = &[
    "back",
    "forward",
    "reload",
    "click",
    "type",
    "select",
    "drag",
    "scroll",
    "waitFor",
    "waitForNavigation",
    "waitForLoad",
    "clearData",
];
pub(crate) const BROWSER_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "back|forward|reload (history); click (needs selector|text|handle); type (needs value + selector|text|handle); select (picks a <select> option: needs value + selector|text|handle); drag (needs a source selector|text|handle + a destination toSelector|toText|toHandle); scroll (needs selector|text|handle|to|by); waitFor (needs selector); waitForNavigation|waitForLoad (await the next navigation/page-load, optional timeoutMs); clearData (clears website data — default cookies+cache, or kinds). Interaction args below.",
);
pub(crate) const BROWSER_SELECTOR: KeySpec = KeySpec::new(
    "selector",
    KeyType::Str,
    "CSS selector target for click/type/scroll/waitFor",
);
pub(crate) const BROWSER_TEXT: KeySpec = KeySpec::new(
    "text",
    KeyType::Str,
    "visible-text target (alternative to selector) for click/type/scroll",
);
pub(crate) const BROWSER_VALUE: KeySpec = KeySpec::new(
    "value",
    KeyType::Str,
    "text to type (type; may be empty to clear the field), or the option label/value to pick (select); required by both",
);
pub(crate) const BROWSER_TO_SELECTOR: KeySpec = KeySpec::new(
    "toSelector",
    KeyType::Str,
    "CSS selector for a drag destination",
);
pub(crate) const BROWSER_TO_TEXT: KeySpec = KeySpec::new(
    "toText",
    KeyType::Str,
    "visible-text drag destination (alternative to toSelector)",
);
pub(crate) const BROWSER_TO_HANDLE: KeySpec = KeySpec::with_aliases(
    "toHandle",
    &["toRef"],
    KeyType::Str,
    "element handle (ref e1..eN) for a drag destination",
);
pub(crate) const BROWSER_MODE: KeySpec = KeySpec::new(
    "mode",
    KeyType::Str,
    "drag event family: auto (default; html5 when the source is draggable, else pointer) | pointer | html5",
);
pub(crate) const BROWSER_STEPS: KeySpec = KeySpec::new(
    "steps",
    KeyType::Int,
    "interpolated drag moves (default 10, max 60)",
);
pub(crate) const BROWSER_DELAY_MS: KeySpec = KeySpec::with_aliases(
    "delayMs",
    &["delay_ms"],
    KeyType::Int,
    "pause between drag moves in ms (default 16, max 200)",
);
pub(crate) const BROWSER_SUBMIT: KeySpec =
    KeySpec::new("submit", KeyType::Bool, "press Enter after typing (type)");
pub(crate) const BROWSER_TO: KeySpec =
    KeySpec::new("to", KeyType::Str, "scroll target top|bottom (scroll)");
pub(crate) const BROWSER_BY: KeySpec =
    KeySpec::new("by", KeyType::Int, "scroll delta in pixels (scroll)");
pub(crate) const BROWSER_TIMEOUT_MS: KeySpec = KeySpec::with_aliases(
    "timeoutMs",
    &["timeout_ms"],
    KeyType::Int,
    "poll/await budget in ms (waitFor, waitForNavigation, waitForLoad)",
);
pub(crate) const BROWSER_HANDLE: KeySpec = KeySpec::with_aliases(
    "handle",
    &["ref"],
    KeyType::Str,
    "element handle (ref e1..eN) from the last ?interactive read; a click/type/scroll locator resolved via the durable element anchor",
);
pub(crate) const BROWSER_KINDS: KeySpec = KeySpec::new(
    "kinds",
    KeyType::Array,
    "data buckets for clearData: cookies|cache|storage (default cookies+cache); clears the live webview's persistent website data",
);
pub(crate) const DESCRIPTION: KeySpec = KeySpec::new("description", KeyType::Str, "");
pub(crate) const REFERENCE_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "reference identifier used in the URI and project config",
);
pub(crate) const REFERENCE_GIT: KeySpec = KeySpec::new(
    "git",
    KeyType::Str,
    "git remote URL; use exactly one of git or path",
);
pub(crate) const REFERENCE_PATH: KeySpec = KeySpec::new(
    "path",
    KeyType::Str,
    "local directory path; use exactly one of git or path",
);
pub(crate) const REFERENCE_BRANCH: KeySpec = KeySpec::new(
    "branch",
    KeyType::Str,
    "optional git branch; send null in patch to clear",
);
pub(crate) const REFERENCE_REFRESH: KeySpec = KeySpec::new(
    "refresh",
    KeyType::Bool,
    "when true, refresh the git reference after patching",
);
pub(crate) const TITLE: KeySpec = KeySpec::new("title", KeyType::Str, "");
pub(crate) const EXECUTION: KeySpec = KeySpec::new(
    "execution",
    KeyType::Object,
    "{recipe, backend?, overrides?} to also start an execution once the issue is created (recipe required); omit to create only. `overrides` takes the same launch deltas the executions collection does, so a child can be created with review already off in one call",
);
/// Ad-hoc launch deltas, shared verbatim by the executions-collection append and
/// the issue-create `execution` block — one grammar, described once.
pub(crate) const LAUNCH_OVERRIDES: KeySpec = KeySpec::new(
    "overrides",
    KeyType::Object,
    "ad-hoc adjustments to THIS launch only, never to the recipe others run. `without:[node,...]` removes nodes and splices their edges through, e.g. {without:[\"review\"]} to skip review on a tiny fix; `nodes:{node:{agent}}` rebinds one node to a different agent and re-resolves its snapshot from that agent's config, e.g. {nodes:{\"builder\":{\"agent\":\"coordinator\"}}}; `agents:{agentId:{...}}` merges agent-snapshot fields (prompt, tier, backend, selection, tools, disallowedTools, skills, extras, description) using the same grammar as the post-create snapshot patch. Address a node by its name or by the agent id it runs — recipe-file node ids are not stable and a token matching no node is refused, never skipped. Removing the trigger, the last agent node, or anything that would strand a node from the trigger is refused; a fence is not settable here",
);
pub(crate) const ISSUE_KIND: KeySpec = KeySpec::new(
    "kind",
    KeyType::Str,
    "issue (default) | thread; a thread is a durable session anchor that owns no branch and never merges",
);
pub(crate) const PARENT: KeySpec = KeySpec::new(
    "parent",
    KeyType::Str,
    "canonical issue URI (cairn://p/PROJECT/N) or thread URI (cairn://p/PROJECT/thread-name) of the parent: an issue parent also confers branch ancestry (the child branches from / PRs into it), a thread parent routes attention only; defaults to the thread the creating agent is acting for, and to unparented outside a thread",
);
pub(crate) const TODOS: KeySpec = KeySpec::new("todos", KeyType::Array, "");
pub(crate) const CONFIRMED: KeySpec = KeySpec::new(
    "confirmed",
    KeyType::Bool,
    "set true to confirm a gated artifact and advance the DAG; omit to edit data",
);
pub(crate) const PR_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "merge|close|refresh — operate on the PR a PR artifact produced (mutually exclusive with confirmed)",
);
pub(crate) const PR_METHOD: KeySpec = KeySpec::new(
    "method",
    KeyType::Str,
    "merge method for action:merge (default squash)",
);
pub(crate) const NODE_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "stop|merge|close|refresh — stop interrupts the node's active turn and parks the session warm (resumable, not a kill; cascades to child runs); merge|close|refresh operate on the PR a `pr` action node produced (mutually exclusive with confirmed)",
);
pub(crate) const REBASE_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "replay — ask the store to replay this branch onto the base named by the session",
);
pub(crate) const REBASE_FINGERPRINT: KeySpec = KeySpec::new(
    "fingerprint",
    KeyType::Str,
    "the session fingerprint you read; a request naming a stale one is refused with a fresh diagnostic",
);
pub(crate) const UPDATES: KeySpec = KeySpec::new("updates", KeyType::Array, "");
pub(crate) const SKILL_NAME: KeySpec = KeySpec::new("name", KeyType::Str, "");

/// Dedicated to the pack member: `install` materializes an available pack,
/// `update` re-syncs an installed one. Deliberately not shared with
/// `NODE_ACTION`/`PR_ACTION`, which no sibling contract can perform.
pub(crate) const PACK_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "install | update | restore | export (requires path)",
);
pub(crate) const PACK_PATH: KeySpec =
    KeySpec::new("path", KeyType::Str, "local Agent Plugin directory");
pub(crate) const PACK_ITEM_KIND: KeySpec = KeySpec::new(
    "kind",
    KeyType::Str,
    "required for reset-item: which kind of packed item to restore",
);
pub(crate) const PACK_ITEM_ID: KeySpec = KeySpec::new(
    "itemId",
    KeyType::Str,
    "required for reset-item: the packed item to restore to its shipped form",
);
pub(crate) const SKILL_PROMPT: KeySpec = KeySpec::new("prompt", KeyType::Str, "SKILL.md body");
pub(crate) const SKILL_ALLOWED_TOOLS: KeySpec = KeySpec::new(
    "allowedTools",
    KeyType::Array,
    "tool names this skill is allowed to use; empty clears the restriction",
);
pub(crate) const SKILL_SOURCE_ISSUE: KeySpec = KeySpec::new(
    "sourceIssue",
    KeyType::Str,
    "issue this skill came from; defaults to the writing run's issue",
);
pub(crate) const SKILL_APPEND_TO_PROMPT: KeySpec = KeySpec::with_aliases(
    "appendToPrompt",
    &["append_to_prompt"],
    KeyType::Str,
    "append to SKILL.md instead of replacing it",
);
pub(crate) const SKILL_REPLACE_SECTION: KeySpec = KeySpec::with_aliases(
    "replaceSection",
    &["replace_section"],
    KeyType::Object,
    "{heading, content} — replace one SKILL.md section in place",
);
pub(crate) const MEMORY_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "short display handle; not used for identity",
);
pub(crate) const MEMORY_SCOPE: KeySpec = KeySpec::new(
    "scope",
    KeyType::Str,
    "project | role | workspace; backend resolves scope_value",
);
pub(crate) const MEMORY_STATUS: KeySpec = KeySpec::new(
    "status",
    KeyType::Str,
    "draft | pending | claimed | promoted | discarded | deferred",
);
pub(crate) const MEMORY_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "promote | discard | defer — reasoned triage decision for claimed memories",
);
pub(crate) const MEMORY_REASON: KeySpec = KeySpec::new(
    "reason",
    KeyType::Str,
    "why this triage decision is correct",
);
pub(crate) const MEMORY_NEW_SCOPE: KeySpec = KeySpec::with_aliases(
    "newScope",
    &["new_scope"],
    KeyType::Object,
    "optional for defer: {scope,value} (project key or role name); re-pools as pending",
);
pub(crate) const LABEL_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "display name; slugified into the label id",
);
pub(crate) const LABEL_COLOR: KeySpec = KeySpec::new(
    "color",
    KeyType::Str,
    "#RRGGBB; deterministic palette color when omitted",
);
pub(crate) const LABELS: KeySpec = KeySpec::new(
    "labels",
    KeyType::Array,
    "full replacement label refs by name or slug; a name the workspace vocabulary lacks is created",
);
pub(crate) const SUBAGENT_TYPE: KeySpec = KeySpec::with_aliases(
    "subagentType",
    &["subagent_type"],
    KeyType::Str,
    "one of the Available Agents listed above",
);
pub(crate) const TASK_DESCRIPTION: KeySpec = KeySpec::new(
    "description",
    KeyType::Str,
    "short title for what this task is",
);
pub(crate) const QUESTIONS: KeySpec = KeySpec::new("questions", KeyType::Array, "");
pub(crate) const ANSWER: KeySpec =
    KeySpec::new("answer", KeyType::Str, "single-question shorthand answer");
pub(crate) const PERMISSION_DECISION: KeySpec =
    KeySpec::new("decision", KeyType::Str, "allow|deny");
pub(crate) const PERMISSION_SCOPE: KeySpec =
    KeySpec::new("scope", KeyType::Str, "once|session (default once)");
pub(crate) const PERMISSION_LIFETIME: KeySpec = KeySpec::new(
    "lifetime",
    KeyType::Str,
    "how long an allow decision grants authority for, beyond this one answer",
);
pub(crate) const PERMISSION_EXPIRES_IN_SECONDS: KeySpec = KeySpec::new(
    "expiresInSeconds",
    KeyType::Int,
    "explicit expiry for an allow decision, in seconds from now",
);
pub(crate) const ANSWERS: KeySpec = KeySpec::new(
    "answers",
    KeyType::Array,
    "indexed answers for one or more questions; each item is {index(int), and exactly one of selection(str option label) | selections(array of labels, for multiSelect) | text(str, free-form/'Other')}; a bare string item is shorthand for {index:<position>, text:<string>}",
);
pub(crate) const BUG_CATEGORY: KeySpec = KeySpec::new(
    "category",
    KeyType::Str,
    "tool_bug|prompt_issue|harness_friction|suggestion",
);
pub(crate) const RECIPE_CONTENT: KeySpec = KeySpec::new(
    "content",
    KeyType::Str,
    "recipe YAML body (cairnVersion, name, trigger, nodes, edges); validated like the file loader",
);
pub(crate) const RECIPE_ID: KeySpec = KeySpec::new(
    "id",
    KeyType::Str,
    "filename id; defaults to slugify(name from the YAML)",
);
pub(crate) const RECIPE_OLD_STRING: KeySpec = KeySpec::new(
    "old_string",
    KeyType::Str,
    "exact text in the recipe YAML source to replace (targeted edit; pair with new_string)",
);
pub(crate) const RECIPE_NEW_STRING: KeySpec = KeySpec::new(
    "new_string",
    KeyType::Str,
    "replacement for old_string; the resulting YAML is re-validated like the file loader",
);
pub(crate) const RECIPE_REPLACE_ALL: KeySpec = KeySpec::new(
    "replace_all",
    KeyType::Bool,
    "replace every occurrence of old_string instead of requiring a unique match",
);
pub(crate) const DELETE_REASON: KeySpec =
    KeySpec::new("reason", KeyType::Str, "why it was removed");
pub(crate) const AGENT_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "display name; slugified into the agent id",
);
pub(crate) const AGENT_PROMPT: KeySpec = KeySpec::new(
    "prompt",
    KeyType::Str,
    "agent system prompt (markdown body)",
);
pub(crate) const AGENT_TOOLS: KeySpec =
    KeySpec::new("tools", KeyType::Array, "tool names; at least one required");
pub(crate) const AGENT_TIER: KeySpec = KeySpec::with_aliases(
    "tier",
    &["model"],
    KeyType::Str,
    "sm|md|lg preset or a model name",
);
pub(crate) const AGENT_BACKEND: KeySpec = KeySpec::new("backend", KeyType::Str, "claude|codex");
pub(crate) const AGENT_FENCE: KeySpec =
    KeySpec::new("fence", KeyType::Str, "deny | ask (default) | allow");
pub(crate) const AGENT_DISALLOWED: KeySpec = KeySpec::with_aliases(
    "disallowedTools",
    &["disallowed_tools"],
    KeyType::Array,
    "tools to block",
);
pub(crate) const AGENT_SKILLS: KeySpec =
    KeySpec::new("skills", KeyType::Array, "skill ids to inject");
pub(crate) const AGENT_ICON: KeySpec =
    KeySpec::new("icon", KeyType::Str, "icon name shown beside the agent");
pub(crate) const AGENT_HOOKS: KeySpec = KeySpec::new(
    "hooks",
    KeyType::Any,
    "lifecycle hook configuration, stored verbatim",
);
/// Superseded by `fence`, still read so existing agent definitions keep working.
pub(crate) const AGENT_SANDBOX: KeySpec = KeySpec::new(
    "sandbox",
    KeyType::Str,
    "legacy, use fence: workspace-write | read-only | danger-full-access",
);
/// Superseded by `fence`, still read so existing agent definitions keep working.
pub(crate) const AGENT_ON_ESCAPE: KeySpec = KeySpec::with_aliases(
    "onEscape",
    &["on_escape"],
    KeyType::Str,
    "legacy, use fence: deny | ask | allow",
);
pub(crate) const ACTION_NAME: KeySpec = KeySpec::new("name", KeyType::Str, "");
pub(crate) const ACTION_COMMAND: KeySpec = KeySpec::with_aliases(
    "commandTemplate",
    &["command_template"],
    KeyType::Str,
    "shell template with {{var:type}} placeholders",
);
pub(crate) const ACTION_INPUT_SCHEMA: KeySpec = KeySpec::with_aliases(
    "inputSchema",
    &["input_schema"],
    KeyType::Object,
    "JSON Schema; derived from the template when omitted",
);
pub(crate) const ACTION_OUTPUT_SCHEMA: KeySpec = KeySpec::with_aliases(
    "outputSchema",
    &["output_schema"],
    KeyType::Object,
    "JSON Schema for the action output",
);

// --- external MCP server registry (cairn://mcp write CRUD) ---
pub(crate) const MCP_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "server key under mcpServers (the <server> segment in cairn://mcp/<server>)",
);
pub(crate) const MCP_TYPE: KeySpec =
    KeySpec::new("type", KeyType::Str, "stdio (default) | http | sse");
pub(crate) const MCP_COMMAND: KeySpec =
    KeySpec::new("command", KeyType::Str, "stdio: program to spawn");
pub(crate) const MCP_ARGS: KeySpec =
    KeySpec::new("args", KeyType::Array, "stdio: command arguments");
pub(crate) const MCP_ENV: KeySpec = KeySpec::new(
    "env",
    KeyType::Object,
    "stdio: environment variables; values may use ${VAR} references (no plaintext secrets)",
);
pub(crate) const MCP_URL: KeySpec = KeySpec::new("url", KeyType::Str, "http/sse: server URL");
pub(crate) const MCP_HEADERS: KeySpec = KeySpec::new(
    "headers",
    KeyType::Object,
    "http/sse: per-request headers; values may use ${VAR} references (no plaintext secrets)",
);
pub(crate) const MCP_ENABLED: KeySpec =
    KeySpec::new("enabled", KeyType::Bool, "expose to agents (default true)");
pub(crate) const MCP_OAUTH: KeySpec = KeySpec::new(
    "oauth",
    KeyType::Object,
    "non-secret OAuth block only: {clientId?, scopes?}; tokens and client secrets live in the keychain and are never set here",
);
pub(crate) const MCP_SCOPE: KeySpec = KeySpec::new(
    "scope",
    KeyType::Str,
    "workspace (default; ~/.cairn/settings.yaml, gated by the worktree fence) | project (the run's .cairn/config.yaml)",
);

// --- fleet management (cairn://executors write) ---
pub(crate) const EXECUTOR_HOST: KeySpec = KeySpec::new(
    "host",
    KeyType::Str,
    "hostname or address ssh reaches the machine at",
);
pub(crate) const EXECUTOR_SSH_USER: KeySpec = KeySpec::with_aliases(
    "sshUser",
    &["ssh_user"],
    KeyType::Str,
    "the user ssh authenticates as",
);
pub(crate) const EXECUTOR_PROJECT_KEYS: KeySpec = KeySpec::with_aliases(
    "projectKeys",
    &["project_keys"],
    KeyType::Array,
    "project keys this machine serves; omitted or empty means every project",
);
pub(crate) const EXECUTOR_BINARY_PATH: KeySpec = KeySpec::with_aliases(
    "binaryPath",
    &["binary_path"],
    KeyType::Str,
    "absolute remote path for the executor binary; derived per platform when omitted",
);
pub(crate) const EXECUTOR_CAIRN_HOME: KeySpec = KeySpec::with_aliases(
    "cairnHome",
    &["cairn_home"],
    KeyType::Str,
    "absolute remote CAIRN_HOME; derived per platform when omitted",
);
pub(crate) const EXECUTOR_ID: KeySpec = KeySpec::with_aliases(
    "executorId",
    &["executor_id"],
    KeyType::Str,
    "stable internal identity; derived from the host when omitted",
);
pub(crate) const EXECUTOR_DEVICE_ID: KeySpec = KeySpec::with_aliases(
    "deviceId",
    &["device_id"],
    KeyType::Str,
    "enrollment device identity; derived from the executor identity when omitted",
);
pub(crate) const EXECUTOR_DISPLAY_NAME: KeySpec = KeySpec::with_aliases(
    "displayName",
    &["display_name"],
    KeyType::Str,
    "the public name placement requests address this machine by; derived from the host when omitted",
);
pub(crate) const EXECUTOR_TUNNEL_PORT: KeySpec = KeySpec::with_aliases(
    "tunnelPort",
    &["tunnel_port"],
    KeyType::Int,
    "loopback port for the reverse tunnel; first unused port from 43849 when omitted",
);
pub(crate) const EXECUTOR_EXTRA_SSH_ARGS: KeySpec = KeySpec::with_aliases(
    "extraSshArgs",
    &["extra_ssh_args"],
    KeyType::Array,
    "transport-only ssh arguments; -4 or -6",
);
pub(crate) const EXECUTOR_NEW_NAME: KeySpec = KeySpec::with_aliases(
    "newName",
    &["new_name"],
    KeyType::Str,
    "new public name; configuration, enrollment claim, and supervision move together",
);
pub(crate) const EXECUTOR_RUNTIME_POLICY: KeySpec = KeySpec::with_aliases(
    "runtimePolicy",
    &["runtime_policy"],
    KeyType::Object,
    "{memoryBudgetBytes, diskGrowthBudgetBytes, freeDiskWatermarkBytes, concurrencyUnits, maximumQueueDepth}",
);
pub(crate) const EXECUTOR_DRAINING: KeySpec = KeySpec::new(
    "draining",
    KeyType::Bool,
    "true refuses new admissions without terminating resident work; live and lost on reconnect",
);
pub(crate) const EXECUTOR_EXPECTED_GENERATION: KeySpec = KeySpec::with_aliases(
    "expectedGeneration",
    &["expected_generation"],
    KeyType::Int,
    "the connection generation you read; required with runtimePolicy or draining",
);

// Empty mutation set, named for readability.
pub(crate) const NO_MUTATIONS: &[MutationSpec] = &[];
pub(crate) const NO_PROJECTIONS: &[ProjectionSpec] = &[];

/// Browser reads accept a content format, a native screenshot, the page's
/// captured runtime buffers (console/network), or its actionable elements
/// (interactive). The screenshot/console/network/interactive facets are
/// mutually exclusive; the screenshot is a host-native capture returned as an
/// image block (works even on about:blank). Content reads page like any
/// resource via ?offset/?limit.
pub(crate) const BROWSER_READ_PROJECTIONS: &[ProjectionSpec] = &[
    ProjectionSpec {
        key: "format",
        values: "markdown (default) | text — live page content",
    },
    ProjectionSpec {
        key: "screenshot",
        values: "(no value) — a PNG screenshot of the rendered page, returned as an image",
    },
    ProjectionSpec {
        key: "console",
        values: "(no value) — the page's captured console output + uncaught errors; optional &limit=N",
    },
    ProjectionSpec {
        key: "network",
        values: "(no value) — the page's captured fetch/XHR request summaries; optional &limit=N",
    },
    ProjectionSpec {
        key: "interactive",
        values: "(no value) — actionable elements as durable handles (e1..eN) with descriptor + selector, for click/type/scroll by handle; optional &limit=N",
    },
    ProjectionSpec {
        key: "offset",
        values: "N — line window into a long content read (follows the continue: footer)",
    },
    ProjectionSpec {
        key: "limit",
        values: "N — line-window size for content (a buffer/element cap for ?console/?network/?interactive)",
    },
];
pub(crate) const NO_RELATED: &[RelatedSpec] = &[];
pub(crate) const NO_CROSS_ACTIONS: &[CrossActionSpec] = &[];
// Shared read-query projections for the symbol resources (node- and project-scoped).
pub(crate) const SYMBOLS_PROJECTIONS: &[ProjectionSpec] = &[
    ProjectionSpec {
        key: "op",
        values: "definition|references|callers|implementations (absent = overview: definition site + signature + reference count)",
    },
    ProjectionSpec {
        key: "in",
        values: "GLOB — scope navigation to a path subtree",
    },
];
// A recipe is the `{recipe}` input to starting an execution; that `append`
// mutation lives on the executions resource, so surface it as a cross-action.
pub(crate) const RECIPE_CROSS_ACTIONS: &[CrossActionSpec] = &[CrossActionSpec {
    kind: ResourceKind::IssueExecutions,
    mode: ChangeMode::Append,
    label: "start an execution with this recipe",
}];
pub(crate) const PROJECT_RELATED: &[RelatedSpec] = &[
    RelatedSpec {
        label: "issues",
        kind: ResourceKind::ProjectIssues,
        actions: true,
    },
    RelatedSpec {
        label: "messages",
        kind: ResourceKind::ProjectMessages,
        actions: false,
    },
    RelatedSpec {
        label: "labels",
        kind: ResourceKind::Labels,
        actions: true,
    },
];
pub(crate) const PROJECT_CHILD_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "up",
    kind: ResourceKind::Project,
    actions: false,
}];
pub(crate) const ISSUE_RELATED: &[RelatedSpec] = &[
    RelatedSpec {
        label: "messages",
        kind: ResourceKind::IssueMessages,
        actions: true,
    },
    RelatedSpec {
        label: "comments",
        kind: ResourceKind::IssueComments,
        actions: true,
    },
    RelatedSpec {
        label: "changed",
        kind: ResourceKind::Changed,
        actions: false,
    },
];
pub(crate) const ISSUE_COMMENTS_RELATED: &[RelatedSpec] = &[
    RelatedSpec {
        label: "up",
        kind: ResourceKind::Issue,
        actions: false,
    },
    // Surface the member's edit/delete in the collection's affordance block so a
    // reader of /comments discovers how to act on a specific comment.
    RelatedSpec {
        label: "comment",
        kind: ResourceKind::IssueComment,
        actions: true,
    },
];
pub(crate) const ISSUE_COMMENT_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "up",
    kind: ResourceKind::IssueComments,
    actions: false,
}];
pub(crate) const ISSUE_MESSAGES_RELATED: &[RelatedSpec] = &[
    RelatedSpec {
        label: "up",
        kind: ResourceKind::Issue,
        actions: false,
    },
    RelatedSpec {
        label: "changed",
        kind: ResourceKind::Changed,
        actions: false,
    },
];
pub(crate) const NODE_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "messages",
    kind: ResourceKind::NodeMessages,
    actions: true,
}];
pub(crate) const NODE_MESSAGES_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "up",
    kind: ResourceKind::Node,
    actions: true,
}];
pub(crate) const NODE_PROGRESS_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "up",
    kind: ResourceKind::Node,
    actions: true,
}];
pub(crate) const TASK_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "messages",
    kind: ResourceKind::TaskMessages,
    actions: true,
}];
pub(crate) const TASK_MESSAGES_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "up",
    kind: ResourceKind::Task,
    actions: true,
}];

// --- workspace settings (cairn://settings patch) ---
pub(crate) const SETTINGS_TIER_DEFAULTS: KeySpec = KeySpec::new(
    "tierDefaults",
    KeyType::Object,
    "tier -> default backend, e.g. {lg: claude, sm: codex}",
);
pub(crate) const SETTINGS_TIERS: KeySpec = KeySpec::new("tiers", KeyType::Array, "tier ordering");
pub(crate) const SETTINGS_BACKENDS: KeySpec =
    KeySpec::new("backends", KeyType::Object, "backend -> tier -> preset map");
pub(crate) const SETTINGS_ENABLED_PROVIDERS: KeySpec = KeySpec::new(
    "enabledProviders",
    KeyType::Array,
    "providers installed in this workspace, e.g. [claude, openrouter]; refused when it would strand a tier default or call routing",
);
pub(crate) const SETTINGS_MERGE_TYPE: KeySpec =
    KeySpec::new("mergeType", KeyType::Str, "squash|merge|rebase");
pub(crate) const SETTINGS_MEMORY_REVIEW_ENABLED: KeySpec = KeySpec::new(
    "memoryReviewEnabled",
    KeyType::Bool,
    "enable memory review prompts and automatic triage",
);
pub(crate) const SETTINGS_GIT_IDENTITIES: KeySpec = KeySpec::new(
    "gitIdentities",
    KeyType::Object,
    "{add[{label,name,email}], update[{id,label?,name?,email?}], remove[ids], order[ids]}",
);
pub(crate) const SETTINGS_ACCOUNTS: KeySpec = KeySpec::new(
    "accounts",
    KeyType::Object,
    "{add[{provider,label,authType,authValue?}], update[{id,label}], remove[ids], order{provider,ids}} (api_key|oauth_token|base_url only; Claude sign-in stays UI-only)",
);
pub(crate) const SETTINGS_KEYBINDS: KeySpec = KeySpec::new(
    "keybinds",
    KeyType::Object,
    "{set[{action,key,modifiers}], reset[actions], resetAll?}",
);
pub(crate) const SETTINGS_BUILD_SERVICES: KeySpec = KeySpec::new(
    "buildServices",
    KeyType::Object,
    "{upsert[{name,config}], setEnabled[{name,enabled}], remove[names]}",
);
pub(crate) const SETTINGS_MAX_THINKING_TOKENS: KeySpec = KeySpec::new(
    "maxThinkingTokens",
    KeyType::Int,
    "thinking budget per turn; null restores the backend default",
);
pub(crate) const SETTINGS_ORPHAN_CLEANUP_DAYS: KeySpec = KeySpec::new(
    "orphanCleanupDays",
    KeyType::Int,
    "days before orphaned worktrees are swept",
);
pub(crate) const SETTINGS_REPO_TARGET_SWEEP_DAYS: KeySpec = KeySpec::new(
    "repoTargetSweepDays",
    KeyType::Int,
    "days before stale build artifacts in project checkouts are swept; 0 disables",
);
pub(crate) const SETTINGS_BUG_REPORTS: KeySpec = KeySpec::new(
    "bugReports",
    KeyType::Bool,
    "whether agents may file bug reports",
);
pub(crate) const SETTINGS_THINKING_DISPLAY_MODE: KeySpec = KeySpec::new(
    "thinkingDisplayMode",
    KeyType::Str,
    "how thinking blocks render in transcripts",
);
pub(crate) const SETTINGS_TRANSCRIPT_TEXT_SIZE: KeySpec = KeySpec::new(
    "transcriptTextSize",
    KeyType::Str,
    "base text scale for transcript markdown",
);
pub(crate) const SETTINGS_TRANSCRIPT_DENSITY: KeySpec = KeySpec::new(
    "transcriptDensity",
    KeyType::Str,
    "vertical rhythm preset for transcript markdown",
);
pub(crate) const SETTINGS_LOG_LEVEL: KeySpec =
    KeySpec::new("logLevel", KeyType::Str, "file-log verbosity");
pub(crate) const SETTINGS_LOG_RETENTION_DAYS: KeySpec = KeySpec::new(
    "logRetentionDays",
    KeyType::Int,
    "days of daily JSONL logs to retain",
);
pub(crate) const SETTINGS_MEMORY_TRIAGE_ENABLED: KeySpec = KeySpec::new(
    "memoryTriageEnabled",
    KeyType::Bool,
    "whether memory-triage issues are created automatically",
);
pub(crate) const SETTINGS_MAX_OPEN_TRIAGE_ISSUES: KeySpec = KeySpec::new(
    "maxOpenTriageIssuesPerScope",
    KeyType::Int,
    "cap on simultaneously open memory-triage issues for one exact scope",
);
pub(crate) const SETTINGS_PENDING_MEMORY_THRESHOLD: KeySpec = KeySpec::new(
    "pendingMemoryThreshold",
    KeyType::Int,
    "pending memories in one scope that trigger a triage issue",
);
pub(crate) const SETTINGS_THREAD_COMPACT_THRESHOLD: KeySpec = KeySpec::new(
    "threadCompactThreshold",
    KeyType::Float,
    "fraction of the context window at which a warm thread session rebuilds",
);
pub(crate) const SETTINGS_EXTERNAL_REPLIES: KeySpec = KeySpec::new(
    "externalReplies",
    KeyType::Str,
    "policy for replies addressed to an external correspondent",
);
pub(crate) const SETTINGS_SUBSCRIPTION_FEES: KeySpec = KeySpec::new(
    "subscriptionFees",
    KeyType::Object,
    "backend -> flat monthly fee in USD; replaces the whole map",
);
pub(crate) const SETTINGS_OPENROUTER_ROUTING: KeySpec = KeySpec::new(
    "openrouterRouting",
    KeyType::Object,
    "OpenRouter provider-routing controls; replaces the whole object",
);
pub(crate) const SETTINGS_ROUTE_CALLS_VIA_OPENROUTER: KeySpec = KeySpec::new(
    "routeCallsViaOpenRouter",
    KeyType::Bool,
    "route tier-based ephemeral calls through OpenRouter instead of the native backend",
);
pub(crate) const SETTINGS_CHANNELS: KeySpec = KeySpec::new(
    "channels",
    KeyType::Object,
    "workspace external delivery policy; replaces the whole object",
);

// --- projects collection + project lifecycle ---
pub(crate) const PROJECT_KEY: KeySpec =
    KeySpec::new("key", KeyType::Str, "uppercase project key (issue prefix)");
pub(crate) const PROJECT_CREATE_NAME: KeySpec = KeySpec::new("name", KeyType::Str, "display name");
pub(crate) const PROJECT_REPO_PATH: KeySpec = KeySpec::with_aliases(
    "repoPath",
    &["repo_path"],
    KeyType::Str,
    "absolute path to the local git repo",
);
pub(crate) const PROJECT_DEFAULT_BRANCH: KeySpec = KeySpec::with_aliases(
    "defaultBranch",
    &["default_branch"],
    KeyType::Str,
    "default branch (default main)",
);
pub(crate) const PROJECT_TEAM_ID: KeySpec = KeySpec::with_aliases(
    "teamId",
    &["team_id"],
    KeyType::Str,
    "route this project to a team's shared database (default: local/private)",
);
pub(crate) const PROJECT_HIDDEN: KeySpec =
    KeySpec::new("hidden", KeyType::Bool, "hide/unhide the project");
pub(crate) const PROJECT_REMOTE_URL: KeySpec = KeySpec::with_aliases(
    "remoteUrl",
    &["remote_url"],
    KeyType::Str,
    "attach this git remote as origin",
);

// --- project settings ---
pub(crate) const PS_SETUP_COMMANDS: KeySpec =
    KeySpec::with_aliases("setupCommands", &["setup_commands"], KeyType::Array, "");
pub(crate) const PS_TERMINAL_COMMANDS: KeySpec = KeySpec::with_aliases(
    "terminalCommands",
    &["terminal_commands"],
    KeyType::Array,
    "[{name,command}]",
);
pub(crate) const PS_MATERIALIZATION_POPULATE: KeySpec = KeySpec::with_aliases(
    "materializationPopulate",
    &["materialization_populate"],
    KeyType::Object,
    "{copy[],symlink[]} gitignored-path populate rules",
);
pub(crate) const PS_ACCOUNT_OVERRIDES: KeySpec = KeySpec::with_aliases(
    "accountOverrides",
    &["account_overrides"],
    KeyType::Object,
    "per-project identity/account overrides; null clears",
);
pub(crate) const PS_REFERENCES: KeySpec = KeySpec::new(
    "references",
    KeyType::Object,
    "{add[{name, git|path, description?, branch?}], remove[names], refresh[names]}",
);
pub(crate) const PS_CHECKS: KeySpec = KeySpec::new(
    "checks",
    KeyType::Object,
    "{name: {full, select?{mode,command,targetsFrom?}, impact?[], parse?, policy?, when?, deterministic?}}; empty object clears all",
);

// --- posts ---
pub(crate) const POST_CONTENT: KeySpec =
    KeySpec::new("content", KeyType::Str, "non-empty Markdown");
pub(crate) const POST_TITLE: KeySpec =
    KeySpec::new("title", KeyType::Str, "optional non-empty title");
pub(crate) const POST_SCOPE: KeySpec = KeySpec::new(
    "scope",
    KeyType::Str,
    "project or the authenticated caller's own project key",
);
pub(crate) const FEED_ACK: KeySpec = KeySpec::new(
    "ack",
    KeyType::Str,
    "the acknowledgement token this home's last feed read returned; a position cannot be named",
);
