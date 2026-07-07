//! The URI data model: scheme constants, the `CairnResource` enum and its
//! `CairnResourceUri` wrapper, and the data-free `kind()` discriminant.

use crate::contract::ResourceKind;
use crate::query::{encode_query_params, QueryParam};

pub const PROJECT_SCOPE: &str = "p";

/// Default browser slug when a browser URI omits an explicit `/SLUG` (e.g. the
/// agent's `cairn:~/browser`). Keeps the shared browser a single canonical
/// session; explicit slugs address additional browsers.
pub const DEFAULT_BROWSER_SLUG: &str = "default";

/// Reserved trailing segments under a node (or task) that name a specific
/// resource rather than an artifact type. A trailing segment NOT in this set is
/// interpreted as a type-named artifact (`.../{node}/plan`). This is the single
/// source of truth shared by the URI parser and cairn-cmd's `cairn:~/<name>`
/// resolution so e.g. `cairn:~/chat` can never be misread as an artifact write.
pub const RESERVED_NODE_SEGMENTS: &[&str] = &[
    "chat",
    "artifact",
    "changed",
    "todos",
    "memories",
    "tasks",
    "wakes",
    "checks",
    "questions",
    "question",
    "terminal",
    "browser",
    "task",
    "messages",
    "progress",
    "annotations",
    "symbols",
];

/// True when `segment` is a reserved node/task sub-resource keyword (see
/// [`RESERVED_NODE_SEGMENTS`]) and therefore not a valid artifact type-name.
pub fn is_reserved_node_segment(segment: &str) -> bool {
    RESERVED_NODE_SEGMENTS.contains(&segment)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CairnResourceUri {
    pub resource: CairnResource,
    pub params: Vec<QueryParam>,
}

impl CairnResourceUri {
    pub fn to_uri(&self) -> String {
        let mut uri = self.resource.to_uri();
        if !self.params.is_empty() {
            uri.push('?');
            uri.push_str(&encode_query_params(&self.params));
        }
        uri
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CairnResource {
    Project {
        project: String,
    },
    ProjectIssues {
        project: String,
    },
    Issue {
        project: String,
        number: i32,
    },
    Node {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },
    NodeChat {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },
    NodeChatRaw {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },
    NodeChatTurn {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        turn_seq: i32,
    },
    NodeChatEvent {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        run_seq: i32,
        event_seq: i32,
    },
    NodeArtifact {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        /// The artifact's type-name segment (`.../{node}/plan`). `None` is the
        /// generic `.../{node}/artifact` alias, kept for back-compat reads.
        name: Option<String>,
    },
    NodeSymbols {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        symbol: Option<String>,
    },
    ProjectSymbols {
        project: String,
        symbol: Option<String>,
    },
    NodeTerminal {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        slug: String,
    },
    TaskTerminal {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        task_name: String,
        slug: String,
    },
    NodeBrowser {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        slug: String,
    },
    TaskBrowser {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        task_name: String,
        slug: String,
    },
    /// A sub-agent task job's base (`.../{node}/task/{name}`). The task analogue
    /// of `Node` — a job is a job. `node_id` is the parent node; `task_name` is
    /// the task's own segment.
    Task {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        task_name: String,
    },
    TaskChat {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        task_name: String,
    },
    TaskChatRaw {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        task_name: String,
    },
    TaskChatTurn {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        task_name: String,
        turn_seq: i32,
    },
    TaskChatEvent {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        task_name: String,
        run_seq: i32,
        event_seq: i32,
    },
    TaskArtifact {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        task_name: String,
        /// The artifact's type-name segment. `None` is the generic
        /// `.../task/{name}/artifact` alias, kept for back-compat reads.
        name: Option<String>,
    },
    /// Todos owned by a job (node or sub-agent task).
    ///
    /// `task_name: None` addresses a node job's todos; `Some(name)` addresses a
    /// sub-agent task job's todos. A job is a job — both URI shapes resolve here.
    JobTodos {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        task_name: Option<String>,
    },
    /// Delegated sub-agent tasks owned by a node job (collection).
    NodeTasks {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },
    /// Ephemeral agent calls owned by a node job (collection, CAIRN-2481).
    NodeCalls {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },
    /// Wake subscriptions owned by a node job (collection).
    NodeWakes {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },
    /// Turn-end project check results owned by a node job (collection).
    NodeChecks {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },
    /// User questions asked by a node job (collection).
    NodeQuestions {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },
    /// A single user question addressed by its stored segment (e.g. `q-1`).
    NodeQuestion {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        segment: String,
    },
    /// Permission requests raised by a node job (collection): pending fence
    /// crossings and tool prompts plus their resolutions.
    NodePermissions {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },
    /// A single permission request addressed by its stored segment (e.g.
    /// `perm-1`); answerable with `{decision, scope}`.
    NodePermission {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        segment: String,
    },
    /// Permission requests raised by a sub-agent task job (collection): the task
    /// analogue of `NodePermissions`. `node_id` is the parent node, `task_name`
    /// the task's own segment.
    TaskPermissions {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        task_name: String,
    },
    /// A single permission request raised by a sub-agent task job, addressed by
    /// its stored segment (e.g. `perm-1`); answerable with `{decision, scope}`.
    /// The task analogue of `NodePermission`.
    TaskPermission {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        task_name: String,
        segment: String,
    },
    /// Messages addressed to a node job (collection). The canonical messaging
    /// target for a node, symmetric with project/issue `/messages`: append
    /// delivers a direct message to the node agent; read returns the node's
    /// direct-message stream. The bare-node append (`Node` + append) is kept
    /// as a backward-compatible alias.
    NodeMessages {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },
    /// Durable phase/log progress timeline for a workflow node (collection).
    /// Append records a typed entry (`kind` = `phase` | `log`, `text` = phase
    /// name or log message); read returns the chronological timeline. The
    /// harness `phase()`/`log()` verbs write here and the workflow monitoring
    /// panel renders it. Resolves from a workflow node's `cairn:~/progress`.
    NodeProgress {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },
    /// Messages addressed to a sub-agent task job (collection). The task
    /// analogue of `NodeMessages`; `node_id` is the parent node.
    TaskMessages {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        task_name: String,
    },
    ProjectMessages {
        project: String,
    },
    IssueMessages {
        project: String,
        number: i32,
    },
    Changed {
        project: String,
        number: i32,
    },
    /// Collection of executions for an issue. Appending starts a new execution.
    IssueExecutions {
        project: String,
        number: i32,
    },
    /// Collection of stored comments for an issue. Read-only here: posting a new
    /// comment stays on the issue-URI append (`cairn://p/PROJECT/NUMBER`). Each
    /// member is individually addressable for edit/delete via `IssueComment`.
    IssueComments {
        project: String,
        number: i32,
    },
    /// A single issue comment addressed by its stable, 1-based per-issue
    /// sequence (`/comments/N`). Supports `patch` (edit content) and `delete`.
    IssueComment {
        project: String,
        number: i32,
        comment_seq: i32,
    },
    /// A single execution's frozen snapshot (recipe + agent snapshots + skills).
    /// Read renders it; patch edits a named agent snapshot.
    IssueExecution {
        project: String,
        number: i32,
        exec_seq: i32,
    },
    NodeChanged {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },
    ProjectTerminal {
        project: String,
        slug: String,
    },
    ProjectBrowser {
        project: String,
        slug: String,
    },
    /// Contextual skills collection (workspace + current project).
    Skills,
    /// Contextual skill package (resolves project-first, then workspace).
    Skill {
        skill_id: String,
        /// Remaining path segments under the skill package (e.g. ["SKILL.md"]).
        path: Vec<String>,
    },
    /// Explicit project skills collection.
    ProjectSkills {
        project: String,
    },
    /// Explicit project-scoped skill package.
    ProjectSkill {
        project: String,
        skill_id: String,
        path: Vec<String>,
    },
    /// Project references collection.
    ProjectReferences {
        project: String,
    },
    /// A single project reference addressed by name.
    ProjectReference {
        project: String,
        name: String,
    },
    /// Workspace labels collection.
    Labels,
    /// A single workspace label addressed by id.
    Label {
        label_id: String,
    },
    /// Memories captured by a specific node job (collection).
    NodeMemories {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
    },
    /// A single node-captured memory addressed by node-local sequence.
    NodeMemory {
        project: String,
        number: i32,
        exec_seq: i32,
        node_id: String,
        memory_seq: i32,
    },
    /// Contextual recipes collection (workspace + current project).
    Recipes,
    /// A single recipe addressed by id (resolves project-first, then workspace).
    Recipe {
        recipe_id: String,
    },
    /// Explicit project recipes collection.
    ProjectRecipes {
        project: String,
    },
    /// Explicit project-scoped recipe.
    ProjectRecipe {
        project: String,
        recipe_id: String,
    },
    /// Contextual workflows collection (workspace + current project).
    Workflows,
    /// A single workflow package addressed by id (resolves project-first, then
    /// workspace).
    Workflow {
        workflow_id: String,
    },
    /// Explicit project workflows collection.
    ProjectWorkflows {
        project: String,
    },
    /// Explicit project-scoped workflow package.
    ProjectWorkflow {
        project: String,
        workflow_id: String,
    },
    /// Contextual agents collection (workspace + current project).
    Agents,
    /// A single agent addressed by id (resolves project-first, then workspace).
    Agent {
        agent_id: String,
    },
    /// Explicit project agents collection.
    ProjectAgents {
        project: String,
    },
    /// Explicit project-scoped agent.
    ProjectAgent {
        project: String,
        agent_id: String,
    },
    /// Contextual actions collection (workspace + current project).
    Actions,
    /// A single action addressed by id (resolves project-first, then workspace).
    Action {
        action_id: String,
    },
    /// Explicit project actions collection.
    ProjectActions {
        project: String,
    },
    /// Explicit project-scoped action.
    ProjectAction {
        project: String,
        action_id: String,
    },
    /// Workspace-global settings document. One resource with every section:
    /// app prefs, backends (+ read-only catalog/usage), git identities, provider
    /// accounts, keybinds, build services, and read-only GitHub status. Patch
    /// routes each present section key to its existing store.
    Settings,
    /// Projects collection: list all projects (read) and create one (write).
    Projects,
    /// Project-scoped configuration (commands, worktree populate, default
    /// branch, identity overrides, references).
    ProjectSettings {
        project: String,
    },
    /// Live local database read projection.
    Db,
    /// `cairn://dev` collection entrypoint: process-introspection tools for a
    /// running `dev:instance` you launched (lists running instances + sub-tools).
    Dev,
    /// `cairn://dev/db` — read-only SQL projection into a running `dev:instance`
    /// database, queried over that instance's own MCP callback server (it holds a
    /// process lock on its database file, so the host cannot open it directly).
    DevDb,
    /// `cairn://dev/pid` — the OS process id(s) of running `dev:instance`(s), each
    /// reported by the instance over its MCP callback server so a caller can
    /// target the process (e.g. with Axon) without shelling out.
    DevPid,
    /// Read-only projection of the running app's JSONL log entries
    /// (URI parity with the in-app Logs viewer).
    Logs,
    /// Global bug report sink.
    Bug,
    /// Self-describing help page: URI grammar + read catalog + mutation matrix.
    Help,
    /// Web search via the active typed web-search provider. The query rides in
    /// the `?q=` projection (`cairn://websearch?q=...`); the read returns a
    /// normalized ranked list of title · url · snippet rows.
    WebSearch,
    /// External MCP gateway family.
    ///
    /// - `cairn://mcp` → `{ server: None, resource: None }` (list servers)
    /// - `cairn://mcp/<server>` → `{ server: Some, resource: None }` (list tools/resources)
    /// - `cairn://mcp/<server>/<tool-or-resource>` → `{ server: Some, resource: Some }`
    ///
    /// `resource` is the raw tail (a tool name for `run`, or an external
    /// resource URI for `read`) kept intact — it may contain `/` and `://`.
    Mcp {
        server: Option<String>,
        resource: Option<String>,
    },
}

impl CairnResource {
    /// Data-free discriminant used to key the resource contract table
    /// (gate dispatch + affordance rendering).
    pub fn kind(&self) -> ResourceKind {
        match self {
            Self::Settings => ResourceKind::Settings,
            Self::Projects => ResourceKind::Projects,
            Self::ProjectSettings { .. } => ResourceKind::ProjectSettings,
            Self::Project { .. } => ResourceKind::Project,
            Self::ProjectIssues { .. } => ResourceKind::ProjectIssues,
            Self::ProjectMessages { .. } => ResourceKind::ProjectMessages,
            Self::ProjectTerminal { .. } => ResourceKind::ProjectTerminal,
            Self::ProjectBrowser { .. } => ResourceKind::ProjectBrowser,
            Self::Issue { .. } => ResourceKind::Issue,
            Self::Changed { .. } => ResourceKind::Changed,
            Self::IssueExecutions { .. } => ResourceKind::IssueExecutions,
            Self::IssueExecution { .. } => ResourceKind::IssueExecution,
            Self::IssueMessages { .. } => ResourceKind::IssueMessages,
            Self::IssueComments { .. } => ResourceKind::IssueComments,
            Self::IssueComment { .. } => ResourceKind::IssueComment,
            Self::NodeMessages { .. } => ResourceKind::NodeMessages,
            Self::NodeProgress { .. } => ResourceKind::NodeProgress,
            Self::TaskMessages { .. } => ResourceKind::TaskMessages,
            Self::Node { .. } => ResourceKind::Node,
            Self::NodeChat { .. } => ResourceKind::NodeChat,
            Self::NodeChatRaw { .. } => ResourceKind::NodeChatRaw,
            Self::NodeChatTurn { .. } => ResourceKind::NodeChatTurn,
            Self::NodeChatEvent { .. } => ResourceKind::NodeChatEvent,
            Self::NodeArtifact { .. } => ResourceKind::NodeArtifact,
            Self::NodeChanged { .. } => ResourceKind::NodeChanged,
            Self::NodeTerminal { .. } => ResourceKind::NodeTerminal,
            Self::TaskTerminal { .. } => ResourceKind::TaskTerminal,
            Self::NodeBrowser { .. } => ResourceKind::NodeBrowser,
            Self::TaskBrowser { .. } => ResourceKind::TaskBrowser,
            Self::Task { .. } => ResourceKind::Task,
            Self::TaskChat { .. } => ResourceKind::TaskChat,
            Self::TaskChatRaw { .. } => ResourceKind::TaskChatRaw,
            Self::TaskChatTurn { .. } => ResourceKind::TaskChatTurn,
            Self::TaskChatEvent { .. } => ResourceKind::TaskChatEvent,
            Self::TaskArtifact { .. } => ResourceKind::TaskArtifact,
            Self::JobTodos { .. } => ResourceKind::JobTodos,
            Self::NodeTasks { .. } => ResourceKind::NodeTasks,
            Self::NodeCalls { .. } => ResourceKind::NodeCalls,
            Self::NodeWakes { .. } => ResourceKind::NodeWakes,
            Self::NodeChecks { .. } => ResourceKind::NodeChecks,
            Self::NodeQuestions { .. } => ResourceKind::NodeQuestions,
            Self::NodeQuestion { .. } => ResourceKind::NodeQuestion,
            Self::NodePermissions { .. } => ResourceKind::NodePermissions,
            Self::NodePermission { .. } => ResourceKind::NodePermission,
            Self::TaskPermissions { .. } => ResourceKind::TaskPermissions,
            Self::TaskPermission { .. } => ResourceKind::TaskPermission,
            Self::Db => ResourceKind::Db,
            Self::Dev => ResourceKind::Dev,
            Self::DevDb => ResourceKind::DevDb,
            Self::DevPid => ResourceKind::DevPid,
            Self::Logs => ResourceKind::Logs,
            Self::Bug => ResourceKind::Bug,
            Self::Help => ResourceKind::Help,
            Self::WebSearch => ResourceKind::WebSearch,
            Self::Mcp { .. } => ResourceKind::Mcp,
            Self::Skills => ResourceKind::Skills,
            Self::Skill { .. } => ResourceKind::Skill,
            Self::ProjectSkills { .. } => ResourceKind::ProjectSkills,
            Self::ProjectSkill { .. } => ResourceKind::ProjectSkill,
            Self::ProjectReferences { .. } => ResourceKind::ProjectReferences,
            Self::ProjectReference { .. } => ResourceKind::ProjectReference,
            Self::Labels => ResourceKind::Labels,
            Self::Label { .. } => ResourceKind::Label,
            Self::NodeMemories { .. } => ResourceKind::NodeMemories,
            Self::NodeMemory { .. } => ResourceKind::NodeMemory,
            Self::Recipes => ResourceKind::Recipes,
            Self::Recipe { .. } => ResourceKind::Recipe,
            Self::ProjectRecipes { .. } => ResourceKind::ProjectRecipes,
            Self::ProjectRecipe { .. } => ResourceKind::ProjectRecipe,
            Self::Workflows => ResourceKind::Workflows,
            Self::Workflow { .. } => ResourceKind::Workflow,
            Self::ProjectWorkflows { .. } => ResourceKind::ProjectWorkflows,
            Self::ProjectWorkflow { .. } => ResourceKind::ProjectWorkflow,
            Self::Agents => ResourceKind::Agents,
            Self::Agent { .. } => ResourceKind::Agent,
            Self::ProjectAgents { .. } => ResourceKind::ProjectAgents,
            Self::ProjectAgent { .. } => ResourceKind::ProjectAgent,
            Self::Actions => ResourceKind::Actions,
            Self::Action { .. } => ResourceKind::Action,
            Self::ProjectActions { .. } => ResourceKind::ProjectActions,
            Self::ProjectAction { .. } => ResourceKind::ProjectAction,
            Self::NodeSymbols { .. } => ResourceKind::NodeSymbols,
            Self::ProjectSymbols { .. } => ResourceKind::ProjectSymbols,
        }
    }
}
