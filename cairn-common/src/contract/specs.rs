//! Reusable `KeySpec`/projection/related/cross-action constants shared by the
//! `RESOURCE_CONTRACTS` table entries.

use super::types::*;

// Reusable key specs. Notes carry value guidance (enumerations/defaults) only
// where the key name + type + example don't already make the value obvious; an
// empty note renders as just `key(type)`.
pub(crate) const CONTENT: KeySpec = KeySpec::new("content", KeyType::Str, "");
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
    "scroll",
    "waitFor",
    "waitForNavigation",
    "waitForLoad",
    "clearData",
];
pub(crate) const BROWSER_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "back|forward|reload (history); click (needs selector|text|handle); type (needs value + selector|text|handle); scroll (needs selector|text|handle|to|by); waitFor (needs selector); waitForNavigation|waitForLoad (await the next navigation/page-load, optional timeoutMs); clearData (clears website data — default cookies+cache, or kinds). Interaction args below.",
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
    "text to type; required by type (may be empty to clear the field)",
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
    "{recipe, backend?} to also start an execution once the issue is created (recipe required); omit to create only",
);
pub(crate) const PARENT: KeySpec = KeySpec::new(
    "parent",
    KeyType::Str,
    "issue URI (cairn://p/PROJECT/N) of the parent; child branches from / PRs into the parent's branch and wakes it on attention",
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
pub(crate) const UPDATES: KeySpec = KeySpec::new("updates", KeyType::Array, "");
pub(crate) const SKILL_NAME: KeySpec = KeySpec::new("name", KeyType::Str, "");
pub(crate) const SKILL_PROMPT: KeySpec = KeySpec::new("prompt", KeyType::Str, "SKILL.md body");
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
pub(crate) const MEMORY_NEW_SCOPE: KeySpec = KeySpec::new(
    "newScope",
    KeyType::Object,
    "optional for defer: {scope,value}; re-pools as pending in corrected scope",
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
    "full replacement label refs by name or slug",
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
pub(crate) const MCP_SCOPE: KeySpec = KeySpec::new(
    "scope",
    KeyType::Str,
    "workspace (default; ~/.cairn/settings.yaml, gated by the worktree fence) | project (the run's .cairn/config.yaml)",
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
pub(crate) const SETTINGS_ACTIVE_BACKEND: KeySpec =
    KeySpec::new("activeBackend", KeyType::Str, "claude|codex");
pub(crate) const SETTINGS_TIERS: KeySpec = KeySpec::new("tiers", KeyType::Array, "tier ordering");
pub(crate) const SETTINGS_BACKENDS: KeySpec =
    KeySpec::new("backends", KeyType::Object, "backend -> tier -> preset map");
pub(crate) const SETTINGS_BRANCH_PREFIX: KeySpec = KeySpec::new("branchPrefix", KeyType::Str, "");
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
    "{add[{provider,label,authType,authValue?}], update[{id,label}], remove[ids], order{provider,ids}} (api_key|oauth_token|local_cli only; OAuth browser add stays UI-only)",
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
pub(crate) const PS_WORKTREE_POPULATE: KeySpec = KeySpec::with_aliases(
    "worktreePopulate",
    &["worktree_populate"],
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
