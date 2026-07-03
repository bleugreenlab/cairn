//! Single-source resource contract table.
//!
//! Every Cairn resource is described once here: its URI template, read-side
//! description and query projections, and its supported `write` mutations
//! (mode + required/optional payload keys + a copy-paste example). Every other
//! surface — the MCP `write` dispatcher's gate, the affordance blocks rendered
//! on read, and the advertised resource templates — is a projection of this
//! table.
//!
//! Adding a mutable resource is two edits: a table entry here and a dispatch
//! arm in `cairn-core`. The `contract_mutations_all_have_dispatch_arms` parity
//! test in `cairn-core` (`resources/mutations/dispatch.rs`) pins the two
//! together so neither can drift without failing CI — it must live there, not
//! here, because `cairn-common` cannot see `CairnResource` or the dispatcher.

/// Supported operations for the canonical `write` carrier.
///
/// Lives here (not in `cairn-core`) so the contract table and the core
/// dispatcher share one definition. `cairn-core` re-exports it.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ChangeMode {
    Create,
    Append,
    Patch,
    #[serde(rename = "unified_patch")]
    UnifiedPatch,
    Replace,
    Delete,
    Rename,
    Apply,
}

impl ChangeMode {
    /// Every mode, for exhaustive enumeration (parity test).
    pub const ALL: &'static [ChangeMode] = &[
        ChangeMode::Create,
        ChangeMode::Append,
        ChangeMode::Patch,
        ChangeMode::UnifiedPatch,
        ChangeMode::Replace,
        ChangeMode::Delete,
        ChangeMode::Rename,
        ChangeMode::Apply,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ChangeMode::Create => "create",
            ChangeMode::Append => "append",
            ChangeMode::Patch => "patch",
            ChangeMode::UnifiedPatch => "unified_patch",
            ChangeMode::Replace => "replace",
            ChangeMode::Delete => "delete",
            ChangeMode::Rename => "rename",
            ChangeMode::Apply => "apply",
        }
    }
}

/// Data-free mirror of `CairnResource` used to key the table and to enumerate
/// resources for the parity test. `CairnResource::kind()` maps each variant
/// here 1:1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceKind {
    Project,
    ProjectIssues,
    ProjectMessages,
    ProjectTerminal,
    ProjectBrowser,
    Issue,
    Changed,
    IssueExecutions,
    IssueExecution,
    IssueMessages,
    IssueComments,
    IssueComment,
    Node,
    NodeChat,
    NodeChatRaw,
    NodeChatTurn,
    NodeChatEvent,
    NodeArtifact,
    NodeChanged,
    NodeTerminal,
    NodeBrowser,
    NodeMessages,
    TaskTerminal,
    TaskBrowser,
    Task,
    TaskChat,
    TaskChatRaw,
    TaskChatTurn,
    TaskChatEvent,
    TaskArtifact,
    TaskMessages,
    JobTodos,
    NodeTasks,
    NodeWakes,
    NodeChecks,
    NodeQuestions,
    NodeQuestion,
    NodePermissions,
    NodePermission,
    TaskPermissions,
    TaskPermission,
    Db,
    Dev,
    DevDb,
    DevPid,
    Logs,
    Bug,
    Skills,
    Skill,
    ProjectSkills,
    ProjectSkill,
    ProjectReferences,
    ProjectReference,
    Labels,
    Label,
    NodeMemories,
    NodeMemory,
    Recipes,
    Recipe,
    ProjectRecipes,
    ProjectRecipe,
    Agents,
    Agent,
    ProjectAgents,
    ProjectAgent,
    Actions,
    Action,
    ProjectActions,
    ProjectAction,
    NodeSymbols,
    ProjectSymbols,
    Help,
    WebSearch,
    Mcp,
    Settings,
    Projects,
    ProjectSettings,
}

impl ResourceKind {
    /// Every kind, for exhaustive enumeration (parity test + advertising).
    pub const ALL: &'static [ResourceKind] = &[
        ResourceKind::Project,
        ResourceKind::ProjectIssues,
        ResourceKind::ProjectMessages,
        ResourceKind::ProjectTerminal,
        ResourceKind::ProjectBrowser,
        ResourceKind::Issue,
        ResourceKind::Changed,
        ResourceKind::IssueExecutions,
        ResourceKind::IssueExecution,
        ResourceKind::IssueMessages,
        ResourceKind::IssueComments,
        ResourceKind::IssueComment,
        ResourceKind::Node,
        ResourceKind::NodeChat,
        ResourceKind::NodeChatRaw,
        ResourceKind::NodeChatTurn,
        ResourceKind::NodeChatEvent,
        ResourceKind::NodeArtifact,
        ResourceKind::NodeChanged,
        ResourceKind::NodeTerminal,
        ResourceKind::NodeBrowser,
        ResourceKind::NodeMessages,
        ResourceKind::TaskTerminal,
        ResourceKind::TaskBrowser,
        ResourceKind::Task,
        ResourceKind::TaskChat,
        ResourceKind::TaskChatRaw,
        ResourceKind::TaskChatTurn,
        ResourceKind::TaskChatEvent,
        ResourceKind::TaskArtifact,
        ResourceKind::TaskMessages,
        ResourceKind::JobTodos,
        ResourceKind::NodeTasks,
        ResourceKind::NodeWakes,
        ResourceKind::NodeChecks,
        ResourceKind::NodeQuestions,
        ResourceKind::NodeQuestion,
        ResourceKind::NodePermissions,
        ResourceKind::NodePermission,
        ResourceKind::TaskPermissions,
        ResourceKind::TaskPermission,
        ResourceKind::Db,
        ResourceKind::Dev,
        ResourceKind::DevDb,
        ResourceKind::DevPid,
        ResourceKind::Logs,
        ResourceKind::Bug,
        ResourceKind::Skills,
        ResourceKind::Skill,
        ResourceKind::ProjectSkills,
        ResourceKind::ProjectSkill,
        ResourceKind::ProjectReferences,
        ResourceKind::ProjectReference,
        ResourceKind::Labels,
        ResourceKind::Label,
        ResourceKind::NodeMemories,
        ResourceKind::NodeMemory,
        ResourceKind::Recipes,
        ResourceKind::Recipe,
        ResourceKind::ProjectRecipes,
        ResourceKind::ProjectRecipe,
        ResourceKind::Agents,
        ResourceKind::Agent,
        ResourceKind::ProjectAgents,
        ResourceKind::ProjectAgent,
        ResourceKind::Actions,
        ResourceKind::Action,
        ResourceKind::ProjectActions,
        ResourceKind::ProjectAction,
        ResourceKind::NodeSymbols,
        ResourceKind::ProjectSymbols,
        ResourceKind::Help,
        ResourceKind::WebSearch,
        ResourceKind::Mcp,
        ResourceKind::Settings,
        ResourceKind::Projects,
        ResourceKind::ProjectSettings,
    ];
}

/// Rough JSON type of a payload key, for documentation in rejections/affordances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    Str,
    Bool,
    Int,
    Array,
    Object,
}

impl KeyType {
    pub fn as_str(self) -> &'static str {
        match self {
            KeyType::Str => "str",
            KeyType::Bool => "bool",
            KeyType::Int => "int",
            KeyType::Array => "array",
            KeyType::Object => "object",
        }
    }
}

/// One payload key a mutation reads.
#[derive(Debug, Clone, Copy)]
pub struct KeySpec {
    /// Canonical key name (advertised in examples).
    pub key: &'static str,
    /// Accepted alternate spellings (e.g. snake_case for a camelCase key).
    ///
    /// Every alias advertised here must be honored by the downstream handler or
    /// deserializer, not just by the gate: `satisfied_by` lets an alias clear the
    /// required-key gate, but a handler that only reads the canonical spelling
    /// would then silently drop the field. The dispatch-level
    /// `advertised_aliases_are_honored_by_dispatch` test pins the gate-observable
    /// (required-key) cases; struct-deserialized optional aliases are pinned
    /// individually (see `agent_frontmatter_honors_model_alias_for_tier`).
    pub aliases: &'static [&'static str],
    pub ty: KeyType,
    pub note: &'static str,
}

impl KeySpec {
    const fn new(key: &'static str, ty: KeyType, note: &'static str) -> Self {
        Self {
            key,
            aliases: &[],
            ty,
            note,
        }
    }

    const fn with_aliases(
        key: &'static str,
        aliases: &'static [&'static str],
        ty: KeyType,
        note: &'static str,
    ) -> Self {
        Self {
            key,
            aliases,
            ty,
            note,
        }
    }

    /// True when `key` or any alias is present among the supplied payload keys.
    pub fn satisfied_by<'a>(&self, mut keys: impl Iterator<Item = &'a str>) -> bool {
        keys.any(|present| present == self.key || self.aliases.contains(&present))
    }

    /// `key(type)` for self-evident keys, `key(type, note)` when the note carries
    /// value guidance (enumerations, defaults). Shared by the affordance blocks
    /// and the system-prompt mutation reference so they never diverge.
    pub fn display(&self) -> String {
        if self.note.is_empty() {
            format!("{}({})", self.key, self.ty.as_str())
        } else {
            format!("{}({}, {})", self.key, self.ty.as_str(), self.note)
        }
    }
}

/// One supported mutation for a resource.
///
/// Editing a `label` or `example` here also changes the affordance blocks
/// rendered on read, which the `affordance_for_*_kind_*` assertion tests in
/// `cairn-core/src/resources/common.rs` consume verbatim. Those tests surface
/// only at a full `bun run test:rust`, never at `bun run check:rust`, so update
/// them in the same change.
#[derive(Debug, Clone, Copy)]
pub struct MutationSpec {
    pub mode: ChangeMode,
    pub required: &'static [KeySpec],
    pub optional: &'static [KeySpec],
    /// Short human label ("create issue", "start terminal").
    pub label: &'static str,
    /// One-line, ready-to-copy `write` call.
    pub example: &'static str,
}

/// One read-side query projection (`?key=values`).
#[derive(Debug, Clone, Copy)]
pub struct ProjectionSpec {
    pub key: &'static str,
    pub values: &'static str,
}

/// A related resource surfaced from a resource's affordance block.
#[derive(Debug, Clone, Copy)]
pub struct RelatedSpec {
    pub label: &'static str,
    pub kind: ResourceKind,
    pub actions: bool,
}

/// A mutation that lives on ANOTHER resource but takes THIS resource as input.
///
/// Some workflows can't be expressed by a resource's own mutation set: a recipe
/// is the `{recipe}` input to starting an execution, but that `append` mutation
/// lives on the executions resource, not the recipe. A cross-action references
/// the owning `(kind, mode)` so the affordance can render "this resource feeds
/// that mutation over there" using the target's `uri_template` and example —
/// restoring a deliberate hint the contract-derived affordances otherwise drop.
#[derive(Debug, Clone, Copy)]
pub struct CrossActionSpec {
    /// The resource kind that owns the mutation.
    pub kind: ResourceKind,
    /// Which mutation on that kind this resource feeds.
    pub mode: ChangeMode,
    /// Action label framed from this resource's perspective.
    pub label: &'static str,
}

/// The full contract for one resource kind.
#[derive(Debug, Clone, Copy)]
pub struct ResourceContract {
    pub kind: ResourceKind,
    pub uri_template: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub read_projections: &'static [ProjectionSpec],
    pub related: &'static [RelatedSpec],
    /// Mutations on OTHER resources that take this resource as input. Rendered in
    /// this kind's actions section from the target's `uri_template` + example.
    pub cross_actions: &'static [CrossActionSpec],
    pub mutations: &'static [MutationSpec],
}

impl ResourceContract {
    /// Find the spec for a mode, if this resource supports it.
    pub fn mutation(&self, mode: ChangeMode) -> Option<&'static MutationSpec> {
        self.mutations.iter().find(|spec| spec.mode == mode)
    }
}

/// Look up the contract for a kind.
pub fn contract_for(kind: ResourceKind) -> Option<&'static ResourceContract> {
    RESOURCE_CONTRACTS
        .iter()
        .find(|contract| contract.kind == kind)
}

/// Look up the mutation spec for a `(kind, mode)` pair. `None` means the
/// dispatcher must reject the mutation.
pub fn mutation_spec(kind: ResourceKind, mode: ChangeMode) -> Option<&'static MutationSpec> {
    contract_for(kind).and_then(|contract| contract.mutation(mode))
}

/// Cross-cutting flows a per-resource schema can't express. Surfaced by the
/// trimmed `write` tool description and on-read help.
pub const GLOBAL_CONTRACT_NOTES: &[(&str, &str)] = &[
    (
        "cairn:~ expansion",
        "Home-relative URIs (cairn:~/...) resolve against the running job's node.",
    ),
    (
        "preview -> apply",
        "preview:true (and a bare mode=rename) writes nothing and needs no commit_msg; it returns an apply_uri. Land it by re-submitting one item with mode=apply, that URI, and the commit_msg that commits the edits.",
    ),
    (
        "commit_msg amend",
        "commit_msg:\"^\" amends the previous commit; applies to the file-target subset only.",
    ),
    (
        "task session",
        "tasks accept session:new|fork and are created only under your own node (cairn:~/tasks).",
    ),
];

// ============================================================================
// The table
// ============================================================================

// Reusable key specs. Notes carry value guidance (enumerations/defaults) only
// where the key name + type + example don't already make the value obvious; an
// empty note renders as just `key(type)`.
const CONTENT: KeySpec = KeySpec::new("content", KeyType::Str, "");
const OLD_STRING: KeySpec = KeySpec::new(
    "old_string",
    KeyType::Str,
    "text replacement operation key; not stored as artifact metadata",
);
const NEW_STRING: KeySpec = KeySpec::new(
    "new_string",
    KeyType::Str,
    "text replacement operation key; not stored as artifact metadata",
);
const REPLACE_ALL: KeySpec = KeySpec::new(
    "replace_all",
    KeyType::Bool,
    "replace all old_string matches; default false errors if old_string is non-unique",
);
const SUBMIT: KeySpec = KeySpec::new(
    "submit",
    KeyType::Bool,
    "send as a command line (append newline if missing); set false to send bytes verbatim. default true",
);
const FIELD: KeySpec = KeySpec::new(
    "field",
    KeyType::Str,
    "top-level string artifact field to edit; defaults to content then body",
);
const COMMAND: KeySpec = KeySpec::new("command", KeyType::Str, "");
const WAKE: KeySpec = KeySpec::new(
    "wake",
    KeyType::Str,
    "\"exit\" to resume when the command finishes, or a literal output phrase to resume when it prints (also fires on exit)",
);
const BROWSER_URL: KeySpec = KeySpec::with_aliases(
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
const BROWSER_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "back|forward|reload (history); click (needs selector|text|handle); type (needs value + selector|text|handle); scroll (needs selector|text|handle|to|by); waitFor (needs selector); waitForNavigation|waitForLoad (await the next navigation/page-load, optional timeoutMs); clearData (clears website data — default cookies+cache, or kinds). Interaction args below.",
);
const BROWSER_SELECTOR: KeySpec = KeySpec::new(
    "selector",
    KeyType::Str,
    "CSS selector target for click/type/scroll/waitFor",
);
const BROWSER_TEXT: KeySpec = KeySpec::new(
    "text",
    KeyType::Str,
    "visible-text target (alternative to selector) for click/type/scroll",
);
const BROWSER_VALUE: KeySpec = KeySpec::new(
    "value",
    KeyType::Str,
    "text to type; required by type (may be empty to clear the field)",
);
const BROWSER_SUBMIT: KeySpec =
    KeySpec::new("submit", KeyType::Bool, "press Enter after typing (type)");
const BROWSER_TO: KeySpec = KeySpec::new("to", KeyType::Str, "scroll target top|bottom (scroll)");
const BROWSER_BY: KeySpec = KeySpec::new("by", KeyType::Int, "scroll delta in pixels (scroll)");
const BROWSER_TIMEOUT_MS: KeySpec = KeySpec::with_aliases(
    "timeoutMs",
    &["timeout_ms"],
    KeyType::Int,
    "poll/await budget in ms (waitFor, waitForNavigation, waitForLoad)",
);
const BROWSER_HANDLE: KeySpec = KeySpec::with_aliases(
    "handle",
    &["ref"],
    KeyType::Str,
    "element handle (ref e1..eN) from the last ?interactive read; a click/type/scroll locator resolved via the durable element anchor",
);
const BROWSER_KINDS: KeySpec = KeySpec::new(
    "kinds",
    KeyType::Array,
    "data buckets for clearData: cookies|cache|storage (default cookies+cache); clears the live webview's persistent website data",
);
const DESCRIPTION: KeySpec = KeySpec::new("description", KeyType::Str, "");
const REFERENCE_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "reference identifier used in the URI and project config",
);
const REFERENCE_GIT: KeySpec = KeySpec::new(
    "git",
    KeyType::Str,
    "git remote URL; use exactly one of git or path",
);
const REFERENCE_PATH: KeySpec = KeySpec::new(
    "path",
    KeyType::Str,
    "local directory path; use exactly one of git or path",
);
const REFERENCE_BRANCH: KeySpec = KeySpec::new(
    "branch",
    KeyType::Str,
    "optional git branch; send null in patch to clear",
);
const REFERENCE_REFRESH: KeySpec = KeySpec::new(
    "refresh",
    KeyType::Bool,
    "when true, refresh the git reference after patching",
);
const TITLE: KeySpec = KeySpec::new("title", KeyType::Str, "");
const EXECUTION: KeySpec = KeySpec::new(
    "execution",
    KeyType::Object,
    "{recipe, backend?} to also start an execution once the issue is created (recipe required); omit to create only",
);
const PARENT: KeySpec = KeySpec::new(
    "parent",
    KeyType::Str,
    "issue URI (cairn://p/PROJECT/N) of the parent; child branches from / PRs into the parent's branch and wakes it on attention",
);
const TODOS: KeySpec = KeySpec::new("todos", KeyType::Array, "");
const CONFIRMED: KeySpec = KeySpec::new(
    "confirmed",
    KeyType::Bool,
    "set true to confirm a gated artifact and advance the DAG; omit to edit data",
);
const PR_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "merge|close|refresh — operate on the PR a PR artifact produced (mutually exclusive with confirmed)",
);
const PR_METHOD: KeySpec = KeySpec::new(
    "method",
    KeyType::Str,
    "merge method for action:merge (default squash)",
);
const NODE_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "stop|merge|close|refresh — stop interrupts the node's active turn and parks the session warm (resumable, not a kill; cascades to child runs); merge|close|refresh operate on the PR a `pr` action node produced (mutually exclusive with confirmed)",
);
const UPDATES: KeySpec = KeySpec::new("updates", KeyType::Array, "");
const SKILL_NAME: KeySpec = KeySpec::new("name", KeyType::Str, "");
const SKILL_PROMPT: KeySpec = KeySpec::new("prompt", KeyType::Str, "SKILL.md body");
const MEMORY_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "short display handle; not used for identity",
);
const MEMORY_SCOPE: KeySpec = KeySpec::new(
    "scope",
    KeyType::Str,
    "project | role | workspace; backend resolves scope_value",
);
const MEMORY_STATUS: KeySpec = KeySpec::new(
    "status",
    KeyType::Str,
    "draft | pending | claimed | promoted | discarded | deferred",
);
const MEMORY_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "promote | discard | defer — reasoned triage decision for claimed memories",
);
const MEMORY_REASON: KeySpec = KeySpec::new(
    "reason",
    KeyType::Str,
    "why this triage decision is correct",
);
const MEMORY_NEW_SCOPE: KeySpec = KeySpec::new(
    "newScope",
    KeyType::Object,
    "optional for defer: {scope,value}; re-pools as pending in corrected scope",
);
const LABEL_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "display name; slugified into the label id",
);
const LABEL_COLOR: KeySpec = KeySpec::new(
    "color",
    KeyType::Str,
    "#RRGGBB; deterministic palette color when omitted",
);
const LABELS: KeySpec = KeySpec::new(
    "labels",
    KeyType::Array,
    "full replacement label refs by name or slug",
);
const SUBAGENT_TYPE: KeySpec = KeySpec::with_aliases(
    "subagentType",
    &["subagent_type"],
    KeyType::Str,
    "one of the Available Agents listed above",
);
const TASK_DESCRIPTION: KeySpec = KeySpec::new(
    "description",
    KeyType::Str,
    "short title for what this task is",
);
const QUESTIONS: KeySpec = KeySpec::new("questions", KeyType::Array, "");
const ANSWER: KeySpec = KeySpec::new("answer", KeyType::Str, "single-question shorthand answer");
const PERMISSION_DECISION: KeySpec = KeySpec::new("decision", KeyType::Str, "allow|deny");
const PERMISSION_SCOPE: KeySpec =
    KeySpec::new("scope", KeyType::Str, "once|session (default once)");
const ANSWERS: KeySpec = KeySpec::new(
    "answers",
    KeyType::Array,
    "indexed answers for one or more questions; each item is {index(int), and exactly one of selection(str option label) | selections(array of labels, for multiSelect) | text(str, free-form/'Other')}; a bare string item is shorthand for {index:<position>, text:<string>}",
);
const BUG_CATEGORY: KeySpec = KeySpec::new(
    "category",
    KeyType::Str,
    "tool_bug|prompt_issue|harness_friction|suggestion",
);
const RECIPE_CONTENT: KeySpec = KeySpec::new(
    "content",
    KeyType::Str,
    "recipe YAML body (cairnVersion, name, trigger, nodes, edges); validated like the file loader",
);
const RECIPE_ID: KeySpec = KeySpec::new(
    "id",
    KeyType::Str,
    "filename id; defaults to slugify(name from the YAML)",
);
const RECIPE_OLD_STRING: KeySpec = KeySpec::new(
    "old_string",
    KeyType::Str,
    "exact text in the recipe YAML source to replace (targeted edit; pair with new_string)",
);
const RECIPE_NEW_STRING: KeySpec = KeySpec::new(
    "new_string",
    KeyType::Str,
    "replacement for old_string; the resulting YAML is re-validated like the file loader",
);
const RECIPE_REPLACE_ALL: KeySpec = KeySpec::new(
    "replace_all",
    KeyType::Bool,
    "replace every occurrence of old_string instead of requiring a unique match",
);
const DELETE_REASON: KeySpec = KeySpec::new("reason", KeyType::Str, "why it was removed");
const AGENT_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "display name; slugified into the agent id",
);
const AGENT_PROMPT: KeySpec = KeySpec::new(
    "prompt",
    KeyType::Str,
    "agent system prompt (markdown body)",
);
const AGENT_TOOLS: KeySpec =
    KeySpec::new("tools", KeyType::Array, "tool names; at least one required");
const AGENT_TIER: KeySpec = KeySpec::with_aliases(
    "tier",
    &["model"],
    KeyType::Str,
    "sm|md|lg preset or a model name",
);
const AGENT_BACKEND: KeySpec = KeySpec::new("backend", KeyType::Str, "claude|codex");
const AGENT_FENCE: KeySpec = KeySpec::new("fence", KeyType::Str, "deny | ask (default) | allow");
const AGENT_DISALLOWED: KeySpec = KeySpec::with_aliases(
    "disallowedTools",
    &["disallowed_tools"],
    KeyType::Array,
    "tools to block",
);
const AGENT_SKILLS: KeySpec = KeySpec::new("skills", KeyType::Array, "skill ids to inject");
const ACTION_NAME: KeySpec = KeySpec::new("name", KeyType::Str, "");
const ACTION_COMMAND: KeySpec = KeySpec::with_aliases(
    "commandTemplate",
    &["command_template"],
    KeyType::Str,
    "shell template with {{var:type}} placeholders",
);
const ACTION_INPUT_SCHEMA: KeySpec = KeySpec::with_aliases(
    "inputSchema",
    &["input_schema"],
    KeyType::Object,
    "JSON Schema; derived from the template when omitted",
);
const ACTION_OUTPUT_SCHEMA: KeySpec = KeySpec::with_aliases(
    "outputSchema",
    &["output_schema"],
    KeyType::Object,
    "JSON Schema for the action output",
);

// --- external MCP server registry (cairn://mcp write CRUD) ---
const MCP_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "server key under mcpServers (the <server> segment in cairn://mcp/<server>)",
);
const MCP_TYPE: KeySpec = KeySpec::new("type", KeyType::Str, "stdio (default) | http | sse");
const MCP_COMMAND: KeySpec = KeySpec::new("command", KeyType::Str, "stdio: program to spawn");
const MCP_ARGS: KeySpec = KeySpec::new("args", KeyType::Array, "stdio: command arguments");
const MCP_ENV: KeySpec = KeySpec::new(
    "env",
    KeyType::Object,
    "stdio: environment variables; values may use ${VAR} references (no plaintext secrets)",
);
const MCP_URL: KeySpec = KeySpec::new("url", KeyType::Str, "http/sse: server URL");
const MCP_HEADERS: KeySpec = KeySpec::new(
    "headers",
    KeyType::Object,
    "http/sse: per-request headers; values may use ${VAR} references (no plaintext secrets)",
);
const MCP_ENABLED: KeySpec =
    KeySpec::new("enabled", KeyType::Bool, "expose to agents (default true)");
const MCP_SCOPE: KeySpec = KeySpec::new(
    "scope",
    KeyType::Str,
    "workspace (default; ~/.cairn/settings.yaml, gated by the worktree fence) | project (the run's .cairn/config.yaml)",
);

// Empty mutation set, named for readability.
const NO_MUTATIONS: &[MutationSpec] = &[];
const NO_PROJECTIONS: &[ProjectionSpec] = &[];

/// Browser reads accept a content format, a native screenshot, the page's
/// captured runtime buffers (console/network), or its actionable elements
/// (interactive). The screenshot/console/network/interactive facets are
/// mutually exclusive; the screenshot is a host-native capture returned as an
/// image block (works even on about:blank). Content reads page like any
/// resource via ?offset/?limit.
const BROWSER_READ_PROJECTIONS: &[ProjectionSpec] = &[
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
const NO_RELATED: &[RelatedSpec] = &[];
const NO_CROSS_ACTIONS: &[CrossActionSpec] = &[];
// Shared read-query projections for the symbol resources (node- and project-scoped).
const SYMBOLS_PROJECTIONS: &[ProjectionSpec] = &[
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
const RECIPE_CROSS_ACTIONS: &[CrossActionSpec] = &[CrossActionSpec {
    kind: ResourceKind::IssueExecutions,
    mode: ChangeMode::Append,
    label: "start an execution with this recipe",
}];
const PROJECT_RELATED: &[RelatedSpec] = &[
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
const PROJECT_CHILD_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "up",
    kind: ResourceKind::Project,
    actions: false,
}];
const ISSUE_RELATED: &[RelatedSpec] = &[
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
const ISSUE_COMMENTS_RELATED: &[RelatedSpec] = &[
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
const ISSUE_COMMENT_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "up",
    kind: ResourceKind::IssueComments,
    actions: false,
}];
const ISSUE_MESSAGES_RELATED: &[RelatedSpec] = &[
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
const NODE_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "messages",
    kind: ResourceKind::NodeMessages,
    actions: true,
}];
const NODE_MESSAGES_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "up",
    kind: ResourceKind::Node,
    actions: true,
}];
const TASK_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "messages",
    kind: ResourceKind::TaskMessages,
    actions: true,
}];
const TASK_MESSAGES_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "up",
    kind: ResourceKind::Task,
    actions: true,
}];

// --- workspace settings (cairn://settings patch) ---
const SETTINGS_ACTIVE_BACKEND: KeySpec =
    KeySpec::new("activeBackend", KeyType::Str, "claude|codex");
const SETTINGS_TIERS: KeySpec = KeySpec::new("tiers", KeyType::Array, "tier ordering");
const SETTINGS_BACKENDS: KeySpec =
    KeySpec::new("backends", KeyType::Object, "backend -> tier -> preset map");
const SETTINGS_BRANCH_PREFIX: KeySpec = KeySpec::new("branchPrefix", KeyType::Str, "");
const SETTINGS_MERGE_TYPE: KeySpec = KeySpec::new("mergeType", KeyType::Str, "squash|merge|rebase");
const SETTINGS_GIT_IDENTITIES: KeySpec = KeySpec::new(
    "gitIdentities",
    KeyType::Object,
    "{add[{label,name,email}], update[{id,label?,name?,email?}], remove[ids], order[ids]}",
);
const SETTINGS_ACCOUNTS: KeySpec = KeySpec::new(
    "accounts",
    KeyType::Object,
    "{add[{provider,label,authType,authValue?}], update[{id,label}], remove[ids], order{provider,ids}} (api_key|oauth_token|local_cli only; OAuth browser add stays UI-only)",
);
const SETTINGS_KEYBINDS: KeySpec = KeySpec::new(
    "keybinds",
    KeyType::Object,
    "{set[{action,key,modifiers}], reset[actions], resetAll?}",
);
const SETTINGS_BUILD_SERVICES: KeySpec = KeySpec::new(
    "buildServices",
    KeyType::Object,
    "{upsert[{name,config}], setEnabled[{name,enabled}], remove[names]}",
);

// --- projects collection + project lifecycle ---
const PROJECT_KEY: KeySpec =
    KeySpec::new("key", KeyType::Str, "uppercase project key (issue prefix)");
const PROJECT_CREATE_NAME: KeySpec = KeySpec::new("name", KeyType::Str, "display name");
const PROJECT_REPO_PATH: KeySpec = KeySpec::with_aliases(
    "repoPath",
    &["repo_path"],
    KeyType::Str,
    "absolute path to the local git repo",
);
const PROJECT_DEFAULT_BRANCH: KeySpec = KeySpec::with_aliases(
    "defaultBranch",
    &["default_branch"],
    KeyType::Str,
    "default branch (default main)",
);
const PROJECT_TEAM_ID: KeySpec = KeySpec::with_aliases(
    "teamId",
    &["team_id"],
    KeyType::Str,
    "route this project to a team's shared database (default: local/private)",
);
const PROJECT_HIDDEN: KeySpec = KeySpec::new("hidden", KeyType::Bool, "hide/unhide the project");
const PROJECT_REMOTE_URL: KeySpec = KeySpec::with_aliases(
    "remoteUrl",
    &["remote_url"],
    KeyType::Str,
    "attach this git remote as origin",
);

// --- project settings ---
const PS_SETUP_COMMANDS: KeySpec =
    KeySpec::with_aliases("setupCommands", &["setup_commands"], KeyType::Array, "");
const PS_TERMINAL_COMMANDS: KeySpec = KeySpec::with_aliases(
    "terminalCommands",
    &["terminal_commands"],
    KeyType::Array,
    "[{name,command}]",
);
const PS_WORKTREE_POPULATE: KeySpec = KeySpec::with_aliases(
    "worktreePopulate",
    &["worktree_populate"],
    KeyType::Object,
    "{copy[],symlink[]} gitignored-path populate rules",
);
const PS_ACCOUNT_OVERRIDES: KeySpec = KeySpec::with_aliases(
    "accountOverrides",
    &["account_overrides"],
    KeyType::Object,
    "per-project identity/account overrides; null clears",
);
const PS_REFERENCES: KeySpec = KeySpec::new(
    "references",
    KeyType::Object,
    "{add[{name, git|path, description?, branch?}], remove[names], refresh[names]}",
);
const PS_CHECKS: KeySpec = KeySpec::new(
    "checks",
    KeyType::Object,
    "{name: {full, select?{mode,command,targetsFrom?}, impact?[], parse?, policy?, when?, deterministic?}}; empty object clears all",
);

pub const RESOURCE_CONTRACTS: &[ResourceContract] = &[
    ResourceContract {
        kind: ResourceKind::Db,
        uri_template: "cairn://db",
        name: "Live database SQL projection",
        description: "Read-only SQL against the running app's existing local database connection. Requires ?sql=... and supports offset/limit row windows. EXPLAIN and EXPLAIN QUERY PLAN are permitted for inspecting query plans.",
        read_projections: &[
            ProjectionSpec { key: "sql", values: "read-only SELECT/WITH, EXPLAIN [QUERY PLAN], or schema PRAGMA" },
            ProjectionSpec { key: "offset", values: "N rows to skip (default 0)" },
            ProjectionSpec { key: "limit", values: "N rows (default 100, max 1000)" },
        ],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::Dev,
        uri_template: "cairn://dev",
        name: "Dev instance introspection",
        description: "Process-introspection tools for a running `bun run dev:instance` (the per-branch dev build you launched). read cairn://dev lists running instances and the available sub-tools: cairn://dev/db (read-only SQL against the instance's database) and cairn://dev/pid (the instance's OS process id, e.g. to target it with Axon accessibility).",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::DevDb,
        uri_template: "cairn://dev/db",
        name: "Dev instance database SQL projection",
        description: "Read-only SQL against a running `bun run dev:instance` database (the per-branch dev build you launched), not the host app's own DB. The instance holds a process lock on its database file, so this queries the instance's own MCP callback server, which means the instance must be running. Same statement policy as cairn://db (SELECT, read-only WITH, EXPLAIN [QUERY PLAN], schema PRAGMAs) with offset/limit row windows. read cairn://dev/db with no ?sql lists registered instances and their running state; ?at=<branch-or-key> selects one (optional when exactly one is registered, or exactly one is running).",
        read_projections: &[
            ProjectionSpec { key: "sql", values: "read-only SELECT/WITH, EXPLAIN [QUERY PLAN], or schema PRAGMA; omit to list dev instances" },
            ProjectionSpec { key: "at", values: "branch name or slug key of the dev instance to query" },
            ProjectionSpec { key: "offset", values: "N rows to skip (default 0)" },
            ProjectionSpec { key: "limit", values: "N rows (default 100, max 1000)" },
        ],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::DevPid,
        uri_template: "cairn://dev/pid",
        name: "Dev instance process id",
        description: "The OS process id(s) of running `bun run dev:instance`(s). Each instance reports its own std::process::id() over its MCP callback server (authoritative, no lsof), so a caller can target the process with external tools such as Axon accessibility without shelling out. read cairn://dev/pid lists every running instance's pid; ?at=<branch-or-key> selects one.",
        read_projections: &[
            ProjectionSpec { key: "at", values: "branch name or slug key of the dev instance to target" },
        ],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
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
    },
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
    },
    ResourceContract {
        kind: ResourceKind::Help,
        uri_template: "cairn://help",
        name: "Help",
        description: "Complete on-demand reference: URI grammar, the read catalog, and the full (resource, mode) mutation matrix",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
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
    },
    ResourceContract {
        kind: ResourceKind::Project,
        uri_template: "cairn://p/{project}",
        name: "Project overview",
        description: "Project overview with recent issues and status",
        read_projections: &[
            ProjectionSpec {
                key: "search",
                values: "QUERY (full-text across the project)",
            },
            ProjectionSpec {
                key: "limit",
                values: "N",
            },
            ProjectionSpec {
                key: "since",
                values: "EPOCH",
            },
        ],
        related: PROJECT_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[PROJECT_CREATE_NAME, PROJECT_HIDDEN, PROJECT_REMOTE_URL],
            label: "patch project (rename / hide / attach remote)",
            example: "write({changes:[{target:\"cairn://p/PROJECT\",mode:\"patch\",payload:{hidden:true}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::Settings,
        uri_template: "cairn://settings",
        name: "Workspace settings",
        description: "The workspace-global settings document with every section: app prefs (branchPrefix, maxThinkingTokens, mergeType, pullOnMerge, orphanCleanupDays, bugReports, thinkingDisplayMode, pendingMemoryThreshold, externalReplies), backends (activeBackend/tiers/backends plus a read-only model catalog and usage), git identities, provider accounts, keybinds, build services, and read-only GitHub status. patch routes each present key to its existing store; GitHub is read-only and OAuth account-add stays UI-only.",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[
                SETTINGS_BRANCH_PREFIX,
                SETTINGS_MERGE_TYPE,
                SETTINGS_ACTIVE_BACKEND,
                SETTINGS_TIERS,
                SETTINGS_BACKENDS,
                SETTINGS_GIT_IDENTITIES,
                SETTINGS_ACCOUNTS,
                SETTINGS_KEYBINDS,
                SETTINGS_BUILD_SERVICES,
            ],
            label: "patch workspace settings",
            example: "write({changes:[{target:\"cairn://settings\",mode:\"patch\",payload:{branchPrefix:\"agent\",keybinds:{set:[{action:\"issue.create\",key:\"n\",modifiers:[\"meta\"]}]}}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::Projects,
        uri_template: "cairn://projects",
        name: "Projects",
        description: "All projects with canonical project URIs; create registers a new project from a local git repo path",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[PROJECT_KEY, PROJECT_CREATE_NAME, PROJECT_REPO_PATH],
            optional: &[PROJECT_DEFAULT_BRANCH, PROJECT_TEAM_ID],
            label: "create project",
            example: "write({changes:[{target:\"cairn://projects\",mode:\"create\",payload:{key:\"DEMO\",name:\"Demo\",repoPath:\"/abs/path/to/repo\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::ProjectSettings,
        uri_template: "cairn://p/{project}/settings",
        name: "Project settings",
        description: "Project-scoped configuration: setup/terminal commands, worktree populate rules, default branch, per-project identity overrides, external references, and background-testing checks. patch routes each present key to its store (project-settings.yaml, the projects DB row, or the global references store).",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[
                PS_SETUP_COMMANDS,
                PS_TERMINAL_COMMANDS,
                PS_WORKTREE_POPULATE,
                PROJECT_DEFAULT_BRANCH,
                PS_ACCOUNT_OVERRIDES,
                PS_REFERENCES,
                PS_CHECKS,
            ],
            label: "patch project settings",
            example: "write({changes:[{target:\"cairn://p/PROJECT/settings\",mode:\"patch\",payload:{setupCommands:[\"bun install\"]}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::ProjectIssues,
        uri_template: "cairn://p/{project}/issues",
        name: "Project issues",
        description: "Project issue collection with canonical issue URIs",
        read_projections: &[
            ProjectionSpec {
                key: "status",
                values: "backlog,active",
            },
            ProjectionSpec {
                key: "limit",
                values: "N issues (default 20)",
            },
            ProjectionSpec {
                key: "offset",
                values: "N issues to skip (paging)",
            },
            ProjectionSpec {
                key: "sort",
                values: "updated_desc|created_asc|created_desc|updated_asc",
            },
            ProjectionSpec {
                key: "ready",
                values: "true|false",
            },
            ProjectionSpec {
                key: "label",
                values: "<name|slug>",
            },
            ProjectionSpec {
                key: "labels",
                values: "a,b (AND)",
            },
        ],
        related: PROJECT_CHILD_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[TITLE],
            optional: &[DESCRIPTION, EXECUTION, PARENT, LABELS],
            label: "create issue",
            example: "write({changes:[{target:\"cairn://p/PROJECT/issues\",mode:\"append\",payload:{title:\"...\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::ProjectMessages,
        uri_template: "cairn://p/{project}/messages",
        name: "Project messages",
        description: "Project-wide messages between agents",
        read_projections: &[
            ProjectionSpec {
                key: "before",
                values: "CURSOR",
            },
            ProjectionSpec {
                key: "after",
                values: "CURSOR",
            },
            ProjectionSpec {
                key: "since",
                values: "EPOCH",
            },
            ProjectionSpec {
                key: "limit",
                values: "N",
            },
        ],
        related: PROJECT_CHILD_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[CONTENT],
            optional: &[],
            label: "append message",
            example: "write({changes:[{target:\"cairn://p/PROJECT/messages\",mode:\"append\",payload:{content:\"...\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::ProjectTerminal,
        uri_template: "cairn://p/{project}/terminal/{slug}",
        name: "Project terminal",
        description: "Project-scoped terminal output",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[COMMAND],
                optional: &[DESCRIPTION, WAKE],
                label: "start terminal",
                example: "write({changes:[{target:\"cairn://p/PROJECT/terminal/SLUG\",mode:\"create\",payload:{command:\"...\",wake:\"exit\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Append,
                required: &[CONTENT],
                optional: &[SUBMIT],
                label: "send to terminal",
                example: "write({changes:[{target:\"cairn://p/PROJECT/terminal/SLUG\",mode:\"append\",payload:{content:\"rs\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "stop terminal",
                example: "write({changes:[{target:\"cairn://p/PROJECT/terminal/SLUG\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::ProjectBrowser,
        uri_template: "cairn://p/{project}/browser/{slug}",
        name: "Project browser",
        description: "Project-scoped shared browser pane (native webview). replace = go to a URL (sets the page; url required). create = open/ensure the pane (url optional). patch = drive it: navigate (url/navigate) or history/interaction (action: back|forward|reload|click|type|scroll|waitFor|waitForNavigation|waitForLoad; click/type/scroll take a selector, visible text, or a ?interactive handle). delete = close it. create/replace/patch are an idempotent ensure — they reuse the open pane (reopening it if closed) and never error on an existing slug. Read returns the live url/title/status plus current page content (paged via ?offset/?limit); ?screenshot for a native PNG, ?console/?network for captured runtime buffers, ?interactive for actionable elements as durable handles (e1..eN). Add ?return_content=true to a write to get the post-action page inline. The pane self-heals across an app restart.",
        read_projections: BROWSER_READ_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[],
                optional: &[BROWSER_URL],
                label: "open/ensure browser",
                example: "write({changes:[{target:\"cairn://p/PROJECT/browser\",mode:\"create\",payload:{url:\"https://example.com\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Replace,
                required: &[BROWSER_URL],
                optional: &[],
                label: "go to a URL (set the page)",
                example: "write({changes:[{target:\"cairn://p/PROJECT/browser\",mode:\"replace\",payload:{url:\"https://example.com\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    BROWSER_URL,
                    BROWSER_ACTION,
                    BROWSER_SELECTOR,
                    BROWSER_TEXT,
                    BROWSER_HANDLE,
                    BROWSER_VALUE,
                    BROWSER_SUBMIT,
                    BROWSER_TO,
                    BROWSER_BY,
                    BROWSER_TIMEOUT_MS,
                    BROWSER_KINDS,
                ],
                label: "navigate / drive browser",
                example: "write({changes:[{target:\"cairn://p/PROJECT/browser\",mode:\"patch\",payload:{action:\"click\",text:\"Sign in\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "close browser",
                example: "write({changes:[{target:\"cairn://p/PROJECT/browser\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::Issue,
        uri_template: "cairn://p/{project}/{number}",
        name: "Issue details",
        description: "Issue overview with comments, PR data, and execution history",
        read_projections: NO_PROJECTIONS,
        related: ISSUE_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    TITLE,
                    DESCRIPTION,
                    KeySpec::with_aliases(
                        "depends_on",
                        &["dependsOn"],
                        KeyType::Array,
                        "full replacement array of issue URIs",
                    ),
                    KeySpec::new(
                        "labels",
                        KeyType::Array,
                        "full replacement label refs by name or slug",
                    ),
                    KeySpec::new(
                        "status",
                        KeyType::Str,
                        "record a resolution (merged | closed); to MERGE a PR, patch its create-pr artifact with action:\"merge\" instead — status:merged with an open PR is refused",
                    ),
                    KeySpec::new(
                        "parent",
                        KeyType::Str,
                        "canonical issue URI to adopt under (future executions branch from / merge to the parent's branch); null to orphan back to the base branch",
                    ),
                ],
                label: "patch issue",
                example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER\",mode:\"patch\",payload:{status:\"closed\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Append,
                required: &[CONTENT],
                optional: &[],
                label: "append comment",
                example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER\",mode:\"append\",payload:{content:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "delete issue",
                example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::Changed,
        uri_template: "cairn://p/{project}/{number}/changed",
        name: "Issue changed files",
        description: "All files changed across executions for an issue",
        read_projections: &[
            ProjectionSpec {
                key: "glob",
                values: "PATTERN",
            },
            ProjectionSpec {
                key: "output_mode",
                values: "files_with_matches|content|count",
            },
        ],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::IssueExecutions,
        uri_template: "cairn://p/{project}/{number}/executions",
        name: "Issue executions",
        description: "Executions for an issue. Append {recipe, backend?} to start a new execution programmatically.",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[KeySpec::new(
                "recipe",
                KeyType::Str,
                "recipe id to run; discover ids via cairn://recipes",
            )],
            optional: &[KeySpec::new(
                "backend",
                KeyType::Str,
                "claude|codex; defaults to the recipe/agent default",
            )],
            label: "start execution",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/executions\",mode:\"append\",payload:{recipe:\"build\",backend:\"claude\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::IssueExecution,
        uri_template: "cairn://p/{project}/{number}/executions/{exec_seq}",
        name: "Execution snapshot",
        description: "A single execution's frozen snapshot: the recipe (nodes/edges/trigger), every agent snapshot (prompt, tools, model selection, fence, skills), and skills. Read renders it. Patch {agent, snapshot} merges the given snapshot fields over one agent snapshot (send only what changes; a full snapshot replaces every field), mirroring the UI snapshot editor (fence reaches a live session immediately, model on the next turn, prompt on the next session). An agent cannot edit its own snapshot, nor change the fence of any agent in its own execution.",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Patch,
            required: &[
                KeySpec::new(
                    "agent",
                    KeyType::Str,
                    "agentConfigId key in the snapshot's agents map",
                ),
                KeySpec::new(
                    "snapshot",
                    KeyType::Object,
                    "agent-snapshot fields to merge over the current snapshot (camelCase; send only what changes, or a full AgentSnapshot to replace every field)",
                ),
            ],
            optional: &[],
            label: "edit agent snapshot",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/executions/2\",mode:\"patch\",payload:{agent:\"builder\",snapshot:{fence:\"deny\"}}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::IssueMessages,
        uri_template: "cairn://p/{project}/{number}/messages",
        name: "Issue messages",
        description: "Messages between agents working on an issue",
        read_projections: &[
            ProjectionSpec {
                key: "before",
                values: "CURSOR",
            },
            ProjectionSpec {
                key: "after",
                values: "CURSOR",
            },
            ProjectionSpec {
                key: "since",
                values: "EPOCH",
            },
            ProjectionSpec {
                key: "limit",
                values: "N",
            },
        ],
        related: ISSUE_MESSAGES_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[CONTENT],
            optional: &[],
            label: "append message",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/messages\",mode:\"append\",payload:{content:\"...\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::IssueComments,
        uri_template: "cairn://p/{project}/{number}/comments",
        name: "Issue comments",
        description: "Stored comments on an issue, each with its stable id, source (user or agent), and timestamp. Read-only here: post a new comment by appending to the issue URI (cairn://p/PROJECT/NUMBER); edit or delete an existing one through its cairn://p/PROJECT/NUMBER/comments/{id} member URI.",
        read_projections: NO_PROJECTIONS,
        related: ISSUE_COMMENTS_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::IssueComment,
        uri_template: "cairn://p/{project}/{number}/comments/{comment_seq}",
        name: "Issue comment",
        description: "A single issue comment addressed by its stable, 1-based per-issue sequence (the N in /comments/N, shown as [#N] in the issue's comment list); patch edits its content, delete removes it.",
        read_projections: NO_PROJECTIONS,
        related: ISSUE_COMMENT_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[CONTENT],
                optional: &[],
                label: "edit comment",
                example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/comments/N\",mode:\"patch\",payload:{content:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "delete comment",
                example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/comments/N\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::Node,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}",
        name: "Node summary",
        description: "Execution node summary with status and metadata. A patch with action:stop interrupts a running node's active turn and parks its session warm (resumable, not a kill). A `pr` action node also reads back its live GitHub state and accepts merge/close/refresh actions on this bare URI.",
        read_projections: &[ProjectionSpec {
            key: "diff",
            values: "full (inline the live PR patch text on a pr action node)",
        }],
        related: NODE_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Patch,
            required: &[NODE_ACTION],
            optional: &[PR_METHOD],
            label: "stop a running node or act on a pr action node",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/EXEC/NODE\",mode:\"patch\",payload:{action:\"stop\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::NodeMessages,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/messages",
        name: "Node messages",
        description: "Direct messages to and from a node agent. Canonical messaging target for a node, symmetric with project/issue /messages: append delivers a direct message (queued and steered if the recipient is mid-turn), read returns the node's direct-message stream.",
        read_projections: &[
            ProjectionSpec {
                key: "since",
                values: "EPOCH",
            },
            ProjectionSpec {
                key: "limit",
                values: "N",
            },
        ],
        related: NODE_MESSAGES_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[CONTENT],
            optional: &[],
            label: "send direct message",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/EXEC/NODE/messages\",mode:\"append\",payload:{content:\"...\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::NodeChat,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/chat",
        name: "Node transcript",
        description: "Turn-structured digest of the agent conversation",
        read_projections: &[ProjectionSpec {
            key: "latest",
            values: "true|false (newest turn first; events within a turn stay chronological)",
        }],
        related: &[
            RelatedSpec {
                label: "full turn",
                kind: ResourceKind::NodeChatTurn,
                actions: false,
            },
            RelatedSpec {
                label: "raw stream",
                kind: ResourceKind::NodeChatRaw,
                actions: false,
            },
        ],
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::NodeChatRaw,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/chat/raw",
        name: "Raw transcript",
        description: "Full unsummarized transcript stream; the digest's programmatic and grep fallback",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::NodeChatTurn,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/chat/turn/{turn}",
        name: "Transcript turn",
        description: "Turn-scoped transcript slice",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::NodeChatEvent,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/chat/{run_seq}/{event_seq}",
        name: "Transcript event",
        description: "Single event from a node transcript",
        read_projections: &[
            ProjectionSpec {
                key: "offset",
                values: "N",
            },
            ProjectionSpec {
                key: "limit",
                values: "N",
            },
        ],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::NodeArtifact,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/{name}",
        name: "Node artifact",
        description: "Agent output artifact (plan, PR, etc.). Write it via change to cairn:~/<name>; the payload is validated against the node's declared schema. Patch accepts either artifact-field merge payloads (for example {content}) or text replacement operations ({old_string,new_string,field?}); text replacement helper keys are operations and are not stored as artifact metadata. A PR artifact reads back its live GitHub state and accepts merge/close/refresh actions.",
        read_projections: &[ProjectionSpec {
            key: "diff",
            values: "full (inline the live PR patch text on a PR artifact)",
        }],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[],
                optional: &[],
                label: "write artifact",
                example: "write({changes:[{target:\"cairn:~/plan\",mode:\"create\",payload:{title:\"...\",content:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    CONTENT,
                    OLD_STRING,
                    NEW_STRING,
                    FIELD,
                    REPLACE_ALL,
                    CONFIRMED,
                    PR_ACTION,
                    PR_METHOD,
                ],
                label: "edit, confirm, or act on a PR artifact",
                example: "field merge: write({changes:[{target:\"cairn:~/plan\",mode:\"patch\",payload:{content:\"...\"}}]}) | text replacement: write({changes:[{target:\"cairn:~/plan\",mode:\"patch\",payload:{old_string:\"old\",new_string:\"new\",field:\"content\"}}]}) | confirm a gated artifact: write({changes:[{target:\"cairn:~/plan\",mode:\"patch\",payload:{confirmed:true}}]}) | merge a PR artifact: write({changes:[{target:\"cairn:~/pr\",mode:\"patch\",payload:{action:\"merge\",method:\"squash\"}}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::NodeChanged,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/changed",
        name: "Node changed files",
        description: "Files changed by a specific execution node",
        read_projections: &[
            ProjectionSpec {
                key: "glob",
                values: "PATTERN",
            },
            ProjectionSpec {
                key: "output_mode",
                values: "files_with_matches|content|count",
            },
        ],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::NodeTerminal,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/terminal/{slug}",
        name: "Node terminal",
        description: "Execution node terminal output",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[COMMAND],
                optional: &[DESCRIPTION, WAKE],
                label: "start terminal",
                example: "write({changes:[{target:\"cairn:~/terminal/SLUG\",mode:\"create\",payload:{command:\"...\",wake:\"ready\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Append,
                required: &[CONTENT],
                optional: &[SUBMIT],
                label: "send to terminal",
                example: "write({changes:[{target:\"cairn:~/terminal/SLUG\",mode:\"append\",payload:{content:\"rs\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "stop terminal",
                example: "write({changes:[{target:\"cairn:~/terminal/SLUG\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::NodeBrowser,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/browser/{slug}",
        name: "Node browser",
        description: "Execution node shared browser pane (native webview). cairn:~/browser is the default shared session (slug optional; add /SLUG for additional browsers). replace = go to a URL (sets the page; url required). create = open/ensure the pane (url optional). patch = drive it: navigate (url/navigate) or history/interaction (action: back|forward|reload|click|type|scroll|waitFor|waitForNavigation|waitForLoad; click/type/scroll take a selector, visible text, or a ?interactive handle). delete = close it. create/replace/patch are an idempotent ensure — they reuse the open pane (reopening it if closed) and never error on an existing slug. Read returns the live url/title/status plus current page content (paged via ?offset/?limit); ?screenshot for a native PNG, ?console/?network for captured runtime buffers, ?interactive for actionable elements as durable handles (e1..eN). Add ?return_content=true to a write to get the post-action page inline. The user sees and can drive the same session, which self-heals across an app restart.",
        read_projections: BROWSER_READ_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[],
                optional: &[BROWSER_URL],
                label: "open/ensure browser",
                example: "write({changes:[{target:\"cairn:~/browser\",mode:\"create\",payload:{url:\"https://example.com\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Replace,
                required: &[BROWSER_URL],
                optional: &[],
                label: "go to a URL (set the page)",
                example: "write({changes:[{target:\"cairn:~/browser\",mode:\"replace\",payload:{url:\"https://example.com\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    BROWSER_URL,
                    BROWSER_ACTION,
                    BROWSER_SELECTOR,
                    BROWSER_TEXT,
                    BROWSER_HANDLE,
                    BROWSER_VALUE,
                    BROWSER_SUBMIT,
                    BROWSER_TO,
                    BROWSER_BY,
                    BROWSER_TIMEOUT_MS,
                    BROWSER_KINDS,
                ],
                label: "navigate / drive browser",
                example: "write({changes:[{target:\"cairn:~/browser\",mode:\"patch\",payload:{action:\"click\",text:\"Sign in\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "close browser",
                example: "write({changes:[{target:\"cairn:~/browser\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::TaskTerminal,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{task}/terminal/{slug}",
        name: "Task terminal",
        description: "Sub-agent task terminal output scoped to the task job",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[COMMAND],
                optional: &[DESCRIPTION, WAKE],
                label: "start terminal",
                example: "write({changes:[{target:\"cairn:~/terminal/SLUG\",mode:\"create\",payload:{command:\"...\",wake:\"exit\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Append,
                required: &[CONTENT],
                optional: &[SUBMIT],
                label: "send to terminal",
                example: "write({changes:[{target:\"cairn:~/terminal/SLUG\",mode:\"append\",payload:{content:\"rs\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "stop terminal",
                example: "write({changes:[{target:\"cairn:~/terminal/SLUG\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::TaskBrowser,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{task}/browser/{slug}",
        name: "Task browser",
        description: "Sub-agent task shared browser pane (native webview) scoped to the task job. replace = go to a URL (sets the page; url required). create = open/ensure the pane (url optional). patch = drive it: navigate (url/navigate) or history/interaction (action: back|forward|reload|click|type|scroll|waitFor|waitForNavigation|waitForLoad; click/type/scroll take a selector, visible text, or a ?interactive handle). delete = close it. create/replace/patch are an idempotent ensure — they reuse the open pane (reopening it if closed) and never error on an existing slug. Read returns the live url/title/status plus current page content (paged via ?offset/?limit); ?screenshot for a native PNG, ?console/?network for captured runtime buffers, ?interactive for actionable elements as durable handles (e1..eN). Add ?return_content=true to a write to get the post-action page inline. Self-heals across an app restart.",
        read_projections: BROWSER_READ_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[],
                optional: &[BROWSER_URL],
                label: "open/ensure browser",
                example: "write({changes:[{target:\"cairn:~/browser\",mode:\"create\",payload:{url:\"https://example.com\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Replace,
                required: &[BROWSER_URL],
                optional: &[],
                label: "go to a URL (set the page)",
                example: "write({changes:[{target:\"cairn:~/browser\",mode:\"replace\",payload:{url:\"https://example.com\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    BROWSER_URL,
                    BROWSER_ACTION,
                    BROWSER_SELECTOR,
                    BROWSER_TEXT,
                    BROWSER_HANDLE,
                    BROWSER_VALUE,
                    BROWSER_SUBMIT,
                    BROWSER_TO,
                    BROWSER_BY,
                    BROWSER_TIMEOUT_MS,
                    BROWSER_KINDS,
                ],
                label: "navigate / drive browser",
                example: "write({changes:[{target:\"cairn:~/browser\",mode:\"patch\",payload:{action:\"click\",text:\"Sign in\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "close browser",
                example: "write({changes:[{target:\"cairn:~/browser\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::Task,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{name}",
        name: "Task summary",
        description: "Sub-agent task job summary with status and metadata",
        read_projections: NO_PROJECTIONS,
        related: TASK_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::TaskMessages,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{name}/messages",
        name: "Task messages",
        description: "Direct messages to and from a sub-agent task. The task analogue of node /messages: append delivers a direct message, read returns the task's direct-message stream.",
        read_projections: &[
            ProjectionSpec {
                key: "since",
                values: "EPOCH",
            },
            ProjectionSpec {
                key: "limit",
                values: "N",
            },
        ],
        related: TASK_MESSAGES_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[CONTENT],
            optional: &[],
            label: "send direct message",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/EXEC/NODE/task/NAME/messages\",mode:\"append\",payload:{content:\"...\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::TaskChat,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{name}/chat",
        name: "Task transcript",
        description: "Turn-structured digest of the sub-task conversation",
        read_projections: &[ProjectionSpec {
            key: "latest",
            values: "true|false (newest turn first; events within a turn stay chronological)",
        }],
        related: &[
            RelatedSpec {
                label: "full turn",
                kind: ResourceKind::TaskChatTurn,
                actions: false,
            },
            RelatedSpec {
                label: "raw stream",
                kind: ResourceKind::TaskChatRaw,
                actions: false,
            },
        ],
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::TaskChatRaw,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{name}/chat/raw",
        name: "Raw task transcript",
        description: "Full unsummarized sub-task transcript stream; the digest's programmatic and grep fallback",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::TaskChatTurn,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{name}/chat/turn/{turn}",
        name: "Task transcript turn",
        description: "Turn-scoped task transcript slice",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::TaskChatEvent,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{name}/chat/{run_seq}/{event_seq}",
        name: "Task transcript event",
        description: "Single event from a task transcript",
        read_projections: &[
            ProjectionSpec {
                key: "offset",
                values: "N",
            },
            ProjectionSpec {
                key: "limit",
                values: "N",
            },
        ],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::TaskArtifact,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{task}/{name}",
        name: "Task artifact",
        description: "Sub-task output artifact. Write it via change to cairn:~/<name>; the payload is validated against the task's declared schema. Patch accepts either artifact-field merge payloads (for example {content}) or text replacement operations ({old_string,new_string,field?}); text replacement helper keys are operations and are not stored as artifact metadata.",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[],
                optional: &[],
                label: "write artifact",
                example: "write({changes:[{target:\"cairn:~/result\",mode:\"create\",payload:{content:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[CONTENT, OLD_STRING, NEW_STRING, FIELD, REPLACE_ALL],
                label: "edit artifact",
                example: "field merge: write({changes:[{target:\"cairn:~/result\",mode:\"patch\",payload:{content:\"...\"}}]}) | text replacement: write({changes:[{target:\"cairn:~/result\",mode:\"patch\",payload:{old_string:\"old\",new_string:\"new\",field:\"content\"}}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::JobTodos,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/todos",
        name: "Node todos",
        description: "Todo list owned by a node job (read, replace/append/patch via change)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Replace,
                required: &[TODOS],
                optional: &[],
                label: "replace todos",
                example: "write({changes:[{target:\"cairn:~/todos\",mode:\"replace\",payload:{todos:[{content:\"...\",status:\"pending\"}]}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Append,
                required: &[TODOS],
                optional: &[],
                label: "append todos",
                example: "write({changes:[{target:\"cairn:~/todos\",mode:\"append\",payload:{todos:[{content:\"...\",status:\"pending\"}]}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[UPDATES],
                optional: &[],
                label: "patch todos",
                example: "write({changes:[{target:\"cairn:~/todos\",mode:\"patch\",payload:{updates:[{id:\"...\",status:\"completed\"}]}}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::NodeChecks,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/checks",
        name: "Node checks",
        description: "Turn-end project check results for a node job — running: live log tail; done: cached pass/fail verdicts (read-only)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::NodeWakes,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/wakes",
        name: "Node wakes",
        description: "Wake subscriptions owned by a node job (read; subscribe/mute/unmute/unsubscribe via write)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Append,
                required: &[],
                optional: &[KeySpec::new("subscribe", KeyType::Object, "source filter; kind:\"terminal\" resumes the node when a terminal exits"), KeySpec::new("mute", KeyType::Object, "source filter"), KeySpec::new("until", KeyType::Object, "source filter that lifts the mute")],
                label: "subscribe or mute wakes",
                example: "write({changes:[{target:\"cairn:~/wakes\",mode:\"append\",payload:{subscribe:{kind:\"terminal\",ref:\"cairn:~/terminal/<slug>\",on:\"exit\"}}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[KeySpec::new("unmute", KeyType::Object, "source filter")],
                optional: &[],
                label: "unmute wakes",
                example: "write({changes:[{target:\"cairn:~/wakes\",mode:\"patch\",payload:{unmute:{kind:\"issue\",ref:\"cairn://p/CAIRN/1\"}}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[KeySpec::new("unsubscribe", KeyType::Object, "source filter")],
                optional: &[],
                label: "unsubscribe wakes",
                example: "write({changes:[{target:\"cairn:~/wakes\",mode:\"delete\",payload:{unsubscribe:{kind:\"issue\",ref:\"cairn://p/CAIRN/1\"}}}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::NodeTasks,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/tasks",
        name: "Node tasks",
        description: "Delegated sub-agent tasks owned by a node job (read; spawn via change append)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[SUBAGENT_TYPE, TASK_DESCRIPTION],
            optional: &[
                KeySpec::new("prompt", KeyType::Str, ""),
                KeySpec::new(
                    "tier",
                    KeyType::Str,
                    "sm|md|lg preset, or a model name; defaults to the parent's",
                ),
                KeySpec::new(
                    "backend",
                    KeyType::Str,
                    "claude|codex; defaults to the parent's",
                ),
                KeySpec::new(
                    "session",
                    KeyType::Str,
                    "new (fresh context) | fork (copy parent's); default new",
                ),
                KeySpec::new(
                    "background",
                    KeyType::Bool,
                    "fire-and-forget; returns task URIs without waiting",
                ),
            ],
            label: "spawn task",
            example: "write({changes:[{target:\"cairn:~/tasks\",mode:\"append\",payload:{subagentType:\"Explore\",description:\"map parser flow\",prompt:\"...\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::NodeQuestions,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/questions",
        name: "Node questions",
        description: "User questions asked by a node job (read; ask via change append)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[QUESTIONS],
            optional: &[KeySpec::new(
                "background",
                KeyType::Bool,
                "fire-and-forget; returns without waiting for the answer",
            )],
            label: "ask question",
            example: "write({changes:[{target:\"cairn:~/questions\",mode:\"append\",payload:{questions:[{question:\"...\",options:[{label:\"...\",description:\"...\"}],multiSelect:false}]}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::NodeQuestion,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/questions/{segment}",
        name: "Node question",
        description: "A single user question and its response",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[ANSWER, ANSWERS],
                label: "answer question",
                example: "write({changes:[{target:\"cairn://p/PROJ/1/1/planner/questions/q-1\",mode:\"patch\",payload:{answers:[{index:0,selection:\"Option A\"},{index:1,text:\"free-form answer\"}]}}]})", // single-question shorthand: payload:{answer:\"Option 1\"}
            },
            MutationSpec {
                mode: ChangeMode::Append,
                required: &[],
                optional: &[ANSWER, ANSWERS],
                label: "answer question (compat)",
                example: "write({changes:[{target:\"cairn://p/PROJ/1/1/planner/questions/q-1\",mode:\"append\",payload:{answer:\"Option 1\"}}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::NodePermissions,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/permissions",
        name: "Node permissions",
        description: "Permission requests raised by a node job: pending worktree-fence crossings and tool prompts plus their resolutions (read; answer one via its segment)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::NodePermission,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/permissions/{segment}",
        name: "Node permission",
        description: "A single permission request and its resolution; patch {decision, scope} to answer (allow/deny, once/session)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[PERMISSION_DECISION, PERMISSION_SCOPE],
            label: "answer permission",
            example: "write({changes:[{target:\"cairn://p/PROJ/1/1/builder/permissions/perm-1\",mode:\"patch\",payload:{decision:\"allow\",scope:\"once\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::TaskPermissions,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{task}/permissions",
        name: "Task permissions",
        description: "Permission requests raised by a sub-agent task job: pending worktree-fence crossings and tool prompts plus their resolutions (read; answer one via its segment)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::TaskPermission,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{task}/permissions/{segment}",
        name: "Task permission",
        description: "A single permission request raised by a sub-agent task job and its resolution; patch {decision, scope} to answer (allow/deny, once/session)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[PERMISSION_DECISION, PERMISSION_SCOPE],
            label: "answer permission",
            example: "write({changes:[{target:\"cairn://p/PROJ/1/1/builder/task/review/permissions/perm-1\",mode:\"patch\",payload:{decision:\"allow\",scope:\"once\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::Bug,
        uri_template: "cairn://bug",
        name: "Bug report",
        description: "Global bug report sink — append a report with payload.category, payload.title, payload.description, and optional payload.toolName",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[BUG_CATEGORY, TITLE, DESCRIPTION],
            optional: &[KeySpec::new("toolName", KeyType::Str, "related tool")],
            label: "submit bug report",
            example: "write({changes:[{target:\"cairn://bug\",mode:\"append\",payload:{category:\"...\",title:\"...\",description:\"...\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::Skills,
        uri_template: "cairn://skills",
        name: "Skills",
        description: "Workspace and current-project skills with canonical URI links",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[SKILL_NAME, DESCRIPTION, SKILL_PROMPT],
            optional: &[],
            label: "create skill",
            example: "write({changes:[{target:\"cairn://skills\",mode:\"create\",payload:{name:\"...\",description:\"...\",prompt:\"...\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::Skill,
        uri_template: "cairn://skills/{skill_id}",
        name: "Skill",
        description: "Skill SKILL.md content, plus references/scripts/assets package links",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[SKILL_NAME, DESCRIPTION, SKILL_PROMPT],
                label: "patch skill",
                example: "write({changes:[{target:\"cairn://skills/ID\",mode:\"patch\",payload:{description:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[KeySpec::new("reason", KeyType::Str, "why it was removed")],
                label: "delete skill",
                example: "write({changes:[{target:\"cairn://skills/ID\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::ProjectSkills,
        uri_template: "cairn://p/{project}/skills",
        name: "Project skills",
        description: "Skills available in a project's context (workspace + project)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[SKILL_NAME, DESCRIPTION, SKILL_PROMPT],
            optional: &[],
            label: "create skill",
            example: "write({changes:[{target:\"cairn://p/PROJECT/skills\",mode:\"create\",payload:{name:\"...\",description:\"...\",prompt:\"...\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::ProjectSkill,
        uri_template: "cairn://p/{project}/skills/{skill_id}",
        name: "Project skill",
        description: "Project-scoped skill SKILL.md content and package files",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[SKILL_NAME, DESCRIPTION, SKILL_PROMPT],
                label: "patch skill",
                example: "write({changes:[{target:\"cairn://p/PROJECT/skills/ID\",mode:\"patch\",payload:{description:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[KeySpec::new("reason", KeyType::Str, "why it was removed")],
                label: "delete skill",
                example: "write({changes:[{target:\"cairn://p/PROJECT/skills/ID\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::ProjectReferences,
        uri_template: "cairn://p/{project}/references",
        name: "Project references",
        description: "Project-scoped external git repos and local directories configured in .cairn/config.yaml, with resolved status and member URI links",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[REFERENCE_NAME],
            optional: &[REFERENCE_GIT, REFERENCE_PATH, DESCRIPTION, REFERENCE_BRANCH],
            label: "create project reference",
            example: "write({changes:[{target:\"cairn://p/PROJECT/references\",mode:\"create\",payload:{name:\"openpnp\",git:\"https://github.com/openpnp/openpnp.git\",description:\"OpenPnP source\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::ProjectReference,
        uri_template: "cairn://p/{project}/references/{name}",
        name: "Project reference",
        description: "A single project reference; patch editable source fields or delete it from project config without deleting the global clone. When switching between path and git sources, send null for the old source field so exactly one of git/path remains set.",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[REFERENCE_GIT, REFERENCE_PATH, DESCRIPTION, REFERENCE_BRANCH, REFERENCE_REFRESH],
                label: "patch project reference",
                example: "write({changes:[{target:\"cairn://p/PROJECT/references/openpnp\",mode:\"patch\",payload:{description:\"Updated description\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "delete project reference",
                example: "write({changes:[{target:\"cairn://p/PROJECT/references/openpnp\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::Labels,
        uri_template: "cairn://labels",
        name: "Labels",
        description: "Workspace label vocabulary with canonical URI links and issue usage counts",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[LABEL_NAME],
            optional: &[LABEL_COLOR],
            label: "create label",
            example: "write({changes:[{target:\"cairn://labels\",mode:\"create\",payload:{name:\"Needs Review\",color:\"#6B8F71\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::Label,
        uri_template: "cairn://labels/{label_id}",
        name: "Label",
        description: "A single workspace label; deleting it detaches it from all issues",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[LABEL_NAME, LABEL_COLOR],
                label: "patch label",
                example: "write({changes:[{target:\"cairn://labels/needs-review\",mode:\"patch\",payload:{name:\"Reviewed\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "delete label",
                example: "write({changes:[{target:\"cairn://labels/needs-review\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::NodeSymbols,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/symbols",
        name: "Node Symbols",
        description: "Structural code navigation over this node's worktree via the in-process ast-grep engine. Append a symbol (`/build_widget`) and pick an op with `?op=` (definition|references|callers|implementations); an absent op returns an overview (definition site, signature, and reference count). Scope with `?in=<glob>`. No language server, no index — files are parsed on demand.",
        read_projections: SYMBOLS_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::ProjectSymbols,
        uri_template: "cairn://p/{project}/symbols",
        name: "Project Symbols",
        description: "Structural code navigation over the project's main checkout — the node-less fallback to the node-scoped symbols resource. Same ops and projections; append a symbol and pick an op with `?op=`.",
        read_projections: SYMBOLS_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    },
    ResourceContract {
        kind: ResourceKind::NodeMemories,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/memories",
        name: "Node memories",
        description: "Draft intake ledger entries captured by a node; append creates a draft",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[CONTENT],
            optional: &[MEMORY_NAME, MEMORY_SCOPE],
            label: "append node memory",
            example: "write({changes:[{target:\"cairn:~/memories\",mode:\"append\",payload:{content:\"...\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::NodeMemory,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/memories/{memory_seq}",
        name: "Node memory",
        description: "A single node-captured draft/pending intake ledger entry",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[CONTENT, MEMORY_STATUS],
                label: "patch node memory",
                example: r#"write({changes:[{target:"cairn:~/memories/1",mode:"patch",payload:{content:"...",status:"draft"}}]})"#,
            },
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[MEMORY_ACTION, MEMORY_REASON],
                optional: &[MEMORY_NEW_SCOPE],
                label: "triage node memory",
                example: r#"write({changes:[{target:"cairn:~/memories/1",mode:"patch",payload:{action:"promote",reason:"..."}}]})"#,
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "delete node memory",
                example: r#"write({changes:[{target:"cairn:~/memories/1",mode:"delete"}]})"#,
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::Recipes,
        uri_template: "cairn://recipes",
        name: "Recipes",
        description: "Workspace and current-project recipes with valid ids for starting executions",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[RECIPE_CONTENT],
            optional: &[RECIPE_ID],
            label: "create recipe",
            example: "write({changes:[{target:\"cairn://recipes\",mode:\"create\",payload:{content:\"cairnVersion: 1\\nname: ...\\ntrigger: manual\\nnodes: []\\nedges: []\\n\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::Recipe,
        uri_template: "cairn://recipes/{recipe_id}",
        name: "Recipe",
        description: "A single recipe rendered as its full editable YAML source (cairnVersion, name, trigger, nodes, edges); patch it with a full content replace or a targeted old_string/new_string edit",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: RECIPE_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    RECIPE_CONTENT,
                    RECIPE_OLD_STRING,
                    RECIPE_NEW_STRING,
                    RECIPE_REPLACE_ALL,
                ],
                label: "edit recipe (full content replace or targeted text replacement)",
                example: "write({changes:[{target:\"cairn://recipes/ID\",mode:\"patch\",payload:{old_string:\"name: Old Name\",new_string:\"name: New Name\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[DELETE_REASON],
                label: "delete recipe",
                example: "write({changes:[{target:\"cairn://recipes/ID\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::ProjectRecipes,
        uri_template: "cairn://p/{project}/recipes",
        name: "Project recipes",
        description: "Recipes available in a project's context (workspace + project)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[RECIPE_CONTENT],
            optional: &[RECIPE_ID],
            label: "create recipe",
            example: "write({changes:[{target:\"cairn://p/PROJECT/recipes\",mode:\"create\",payload:{content:\"cairnVersion: 1\\nname: ...\\n\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::ProjectRecipe,
        uri_template: "cairn://p/{project}/recipes/{recipe_id}",
        name: "Project recipe",
        description: "A single project-scoped recipe rendered as its full editable YAML source; patch it with a full content replace or a targeted old_string/new_string edit",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: RECIPE_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    RECIPE_CONTENT,
                    RECIPE_OLD_STRING,
                    RECIPE_NEW_STRING,
                    RECIPE_REPLACE_ALL,
                ],
                label: "edit recipe (full content replace or targeted text replacement)",
                example: "write({changes:[{target:\"cairn://p/PROJECT/recipes/ID\",mode:\"patch\",payload:{old_string:\"name: Old Name\",new_string:\"name: New Name\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[DELETE_REASON],
                label: "delete recipe",
                example: "write({changes:[{target:\"cairn://p/PROJECT/recipes/ID\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::Agents,
        uri_template: "cairn://agents",
        name: "Agents",
        description: "Workspace and current-project agents with canonical URI links",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[AGENT_NAME, DESCRIPTION, AGENT_PROMPT, AGENT_TOOLS],
            optional: &[
                AGENT_TIER,
                AGENT_BACKEND,
                AGENT_FENCE,
                AGENT_DISALLOWED,
                AGENT_SKILLS,
            ],
            label: "create agent",
            example: "write({changes:[{target:\"cairn://agents\",mode:\"create\",payload:{name:\"...\",description:\"...\",prompt:\"...\",tools:[\"Read\"]}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::Agent,
        uri_template: "cairn://agents/{agent_id}",
        name: "Agent",
        description: "A single agent: prompt, tools, tier, and behavior settings",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    AGENT_NAME,
                    DESCRIPTION,
                    AGENT_PROMPT,
                    AGENT_TOOLS,
                    AGENT_TIER,
                    AGENT_BACKEND,
                    AGENT_FENCE,
                    AGENT_DISALLOWED,
                    AGENT_SKILLS,
                ],
                label: "patch agent",
                example: "write({changes:[{target:\"cairn://agents/ID\",mode:\"patch\",payload:{prompt:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[DELETE_REASON],
                label: "delete agent",
                example: "write({changes:[{target:\"cairn://agents/ID\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::ProjectAgents,
        uri_template: "cairn://p/{project}/agents",
        name: "Project agents",
        description: "Agents available in a project's context (workspace + project)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[AGENT_NAME, DESCRIPTION, AGENT_PROMPT, AGENT_TOOLS],
            optional: &[
                AGENT_TIER,
                AGENT_BACKEND,
                AGENT_FENCE,
                AGENT_DISALLOWED,
                AGENT_SKILLS,
            ],
            label: "create agent",
            example: "write({changes:[{target:\"cairn://p/PROJECT/agents\",mode:\"create\",payload:{name:\"...\",description:\"...\",prompt:\"...\",tools:[\"Read\"]}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::ProjectAgent,
        uri_template: "cairn://p/{project}/agents/{agent_id}",
        name: "Project agent",
        description: "A single project-scoped agent: prompt, tools, tier, and behavior settings",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    AGENT_NAME,
                    DESCRIPTION,
                    AGENT_PROMPT,
                    AGENT_TOOLS,
                    AGENT_TIER,
                    AGENT_BACKEND,
                    AGENT_FENCE,
                    AGENT_DISALLOWED,
                    AGENT_SKILLS,
                ],
                label: "patch agent",
                example: "write({changes:[{target:\"cairn://p/PROJECT/agents/ID\",mode:\"patch\",payload:{prompt:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[DELETE_REASON],
                label: "delete agent",
                example: "write({changes:[{target:\"cairn://p/PROJECT/agents/ID\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::Actions,
        uri_template: "cairn://actions",
        name: "Actions",
        description: "Workspace and current-project action definitions with canonical URI links",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[ACTION_NAME, ACTION_COMMAND],
            optional: &[DESCRIPTION, ACTION_INPUT_SCHEMA, ACTION_OUTPUT_SCHEMA],
            label: "create action",
            example: "write({changes:[{target:\"cairn://actions\",mode:\"create\",payload:{name:\"...\",commandTemplate:\"gh pr create --title {{title:string}}\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::Action,
        uri_template: "cairn://actions/{action_id}",
        name: "Action",
        description: "A single action definition: command template and input/output schema (built-ins are read-only)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    ACTION_NAME,
                    DESCRIPTION,
                    ACTION_COMMAND,
                    ACTION_INPUT_SCHEMA,
                    ACTION_OUTPUT_SCHEMA,
                ],
                label: "patch action",
                example: "write({changes:[{target:\"cairn://actions/ID\",mode:\"patch\",payload:{commandTemplate:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[DELETE_REASON],
                label: "delete action",
                example: "write({changes:[{target:\"cairn://actions/ID\",mode:\"delete\"}]})",
            },
        ],
    },
    ResourceContract {
        kind: ResourceKind::ProjectActions,
        uri_template: "cairn://p/{project}/actions",
        name: "Project actions",
        description: "Action definitions available in a project's context (workspace + project)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[ACTION_NAME, ACTION_COMMAND],
            optional: &[DESCRIPTION, ACTION_INPUT_SCHEMA, ACTION_OUTPUT_SCHEMA],
            label: "create action",
            example: "write({changes:[{target:\"cairn://p/PROJECT/actions\",mode:\"create\",payload:{name:\"...\",commandTemplate:\"...\"}}]})",
        }],
    },
    ResourceContract {
        kind: ResourceKind::ProjectAction,
        uri_template: "cairn://p/{project}/actions/{action_id}",
        name: "Project action",
        description: "A single project-scoped action definition (built-ins are read-only)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    ACTION_NAME,
                    DESCRIPTION,
                    ACTION_COMMAND,
                    ACTION_INPUT_SCHEMA,
                    ACTION_OUTPUT_SCHEMA,
                ],
                label: "patch action",
                example: "write({changes:[{target:\"cairn://p/PROJECT/actions/ID\",mode:\"patch\",payload:{commandTemplate:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[DELETE_REASON],
                label: "delete action",
                example: "write({changes:[{target:\"cairn://p/PROJECT/actions/ID\",mode:\"delete\"}]})",
            },
        ],
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_has_exactly_one_contract() {
        for kind in ResourceKind::ALL {
            let matches = RESOURCE_CONTRACTS
                .iter()
                .filter(|c| c.kind == *kind)
                .count();
            assert_eq!(matches, 1, "kind {:?} must have exactly one contract", kind);
        }
        assert_eq!(RESOURCE_CONTRACTS.len(), ResourceKind::ALL.len());
    }

    /// Every required key a mutation advertises must appear (by canonical name or
    /// an alias) in its copy-paste `example`, so copying the example verbatim
    /// produces a write that clears the required-key gate. Guards against the
    /// affordance example drifting from the schema it documents (CAIRN #170).
    #[test]
    fn every_mutation_example_names_its_required_keys() {
        for contract in RESOURCE_CONTRACTS {
            for spec in contract.mutations {
                for req in spec.required {
                    let present = spec.example.contains(req.key)
                        || req.aliases.iter().any(|a| spec.example.contains(a));
                    assert!(
                        present,
                        "{:?} {:?} example must name required key `{}`: {}",
                        contract.kind, spec.mode, req.key, spec.example
                    );
                }
            }
        }
    }

    /// Spot-check the inverse for a static-schema resource: every payload key the
    /// label-create example uses is a declared key. This is the property the
    /// schema-aware artifact affordance enforces dynamically (CAIRN #170); here
    /// it is pinned for a representative contract whose keys are fully static.
    #[test]
    fn label_create_example_keys_are_all_declared() {
        let spec = mutation_spec(ResourceKind::Labels, ChangeMode::Create)
            .expect("labels must support create");
        let declared: Vec<&str> = spec
            .required
            .iter()
            .chain(spec.optional)
            .flat_map(|k| std::iter::once(k.key).chain(k.aliases.iter().copied()))
            .collect();
        // Pull the keys out of the single `payload:{...}` object in the example.
        let payload = spec
            .example
            .split_once("payload:{")
            .and_then(|(_, rest)| rest.split_once('}'))
            .map(|(inner, _)| inner)
            .expect("label create example must contain a payload object");
        for field in payload.split(',') {
            let key = field.split(':').next().unwrap_or("").trim();
            assert!(
                declared.contains(&key),
                "label create example key `{key}` is not a declared key: {}",
                spec.example
            );
        }
    }

    #[test]
    fn mutation_spec_lookup_matches_table() {
        assert!(mutation_spec(ResourceKind::ProjectIssues, ChangeMode::Append).is_some());
        assert!(mutation_spec(ResourceKind::ProjectIssues, ChangeMode::Create).is_none());
        assert!(mutation_spec(ResourceKind::Issue, ChangeMode::Patch).is_some());
        assert!(mutation_spec(ResourceKind::Issue, ChangeMode::Delete).is_some());
        assert!(mutation_spec(ResourceKind::Node, ChangeMode::Append).is_none());
        assert!(mutation_spec(ResourceKind::NodeMessages, ChangeMode::Append).is_some());
        assert!(mutation_spec(ResourceKind::NodeChat, ChangeMode::Append).is_none());
        assert!(mutation_spec(ResourceKind::Task, ChangeMode::Append).is_none());
        assert!(mutation_spec(ResourceKind::TaskMessages, ChangeMode::Append).is_some());
        assert!(mutation_spec(ResourceKind::JobTodos, ChangeMode::Replace).is_some());
        assert!(mutation_spec(ResourceKind::IssueExecutions, ChangeMode::Append).is_some());
        assert!(mutation_spec(ResourceKind::IssueExecutions, ChangeMode::Delete).is_none());
        assert!(mutation_spec(ResourceKind::IssueExecution, ChangeMode::Patch).is_some());
        assert!(mutation_spec(ResourceKind::IssueExecution, ChangeMode::Delete).is_none());
        assert!(mutation_spec(ResourceKind::IssueExecution, ChangeMode::Append).is_none());
    }

    #[test]
    fn settings_family_mutation_matrix() {
        // Settings is patch-only (read-only sections live inside it).
        assert!(mutation_spec(ResourceKind::Settings, ChangeMode::Patch).is_some());
        assert!(mutation_spec(ResourceKind::Settings, ChangeMode::Create).is_none());
        assert!(mutation_spec(ResourceKind::Settings, ChangeMode::Delete).is_none());
        // Projects collection creates; the single Project gains patch (no delete).
        assert!(mutation_spec(ResourceKind::Projects, ChangeMode::Create).is_some());
        assert!(mutation_spec(ResourceKind::Project, ChangeMode::Patch).is_some());
        assert!(mutation_spec(ResourceKind::Project, ChangeMode::Delete).is_none());
        // Project settings is patch-only.
        assert!(mutation_spec(ResourceKind::ProjectSettings, ChangeMode::Patch).is_some());
        assert!(mutation_spec(ResourceKind::ProjectSettings, ChangeMode::Create).is_none());
    }

    #[test]
    fn key_spec_satisfied_by_alias() {
        assert!(SUBAGENT_TYPE.satisfied_by(["subagent_type"].into_iter()));
        assert!(SUBAGENT_TYPE.satisfied_by(["subagentType"].into_iter()));
        assert!(!SUBAGENT_TYPE.satisfied_by(["prompt"].into_iter()));
    }
}
