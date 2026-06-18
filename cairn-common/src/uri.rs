//! Unified cairn:// URI scheme parser.
//!
//! Canonical project-scoped URIs use an explicit namespace token:
//! `cairn://p/PROJECT/...`

use crate::contract::ResourceKind;
use crate::query::{encode_query_params, parse_query_params, QueryParam};

pub const PROJECT_SCOPE: &str = "p";

/// Reserved trailing segments under a node (or task) that name a specific
/// resource rather than an artifact type. A trailing segment NOT in this set is
/// interpreted as a type-named artifact (`.../{node}/plan`). This is the single
/// source of truth shared by the URI parser and cairn-cli's `cairn:~/<name>`
/// resolution so e.g. `cairn:~/chat` can never be misread as an artifact write.
pub const RESERVED_NODE_SEGMENTS: &[&str] = &[
    "chat",
    "artifact",
    "changed",
    "todos",
    "memories",
    "tasks",
    "wakes",
    "questions",
    "question",
    "terminal",
    "task",
    "messages",
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
    /// Wake subscriptions owned by a node job (collection).
    NodeWakes {
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

fn canonical_project(project: &str) -> String {
    project.to_uppercase()
}

fn parse_positive_i32(value: &str) -> Option<i32> {
    value.parse::<i32>().ok().filter(|value| *value > 0)
}

fn parse_non_negative_i32(value: &str) -> Option<i32> {
    value.parse::<i32>().ok().filter(|value| *value >= 0)
}

pub fn build_project_uri(project: &str) -> String {
    format!("cairn://{}/{}", PROJECT_SCOPE, canonical_project(project))
}

pub fn build_project_issues_uri(project: &str) -> String {
    format!("{}/issues", build_project_uri(project))
}

pub fn build_issue_uri(project: &str, number: i32) -> String {
    format!("{}/{}", build_project_uri(project), number)
}

pub fn build_project_messages_uri(project: &str) -> String {
    format!("{}/messages", build_project_uri(project))
}

pub fn build_issue_messages_uri(project: &str, number: i32) -> String {
    format!("{}/messages", build_issue_uri(project, number))
}

pub fn build_issue_comments_uri(project: &str, number: i32) -> String {
    format!("{}/comments", build_issue_uri(project, number))
}

pub fn build_issue_comment_uri(project: &str, number: i32, comment_seq: i32) -> String {
    format!(
        "{}/{}",
        build_issue_comments_uri(project, number),
        comment_seq
    )
}

pub fn build_node_messages_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "messages")
}

pub fn build_task_messages_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: &str,
) -> String {
    build_task_subresource_uri(project, number, exec_seq, node_id, task_name, "messages")
}

pub fn build_issue_changed_uri(project: &str, number: i32) -> String {
    format!("{}/changed", build_issue_uri(project, number))
}

pub fn build_issue_executions_uri(project: &str, number: i32) -> String {
    format!("{}/executions", build_issue_uri(project, number))
}

pub fn build_issue_execution_uri(project: &str, number: i32, exec_seq: i32) -> String {
    format!(
        "{}/{}",
        build_issue_executions_uri(project, number),
        exec_seq
    )
}

pub fn build_node_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    format!(
        "{}/{}/{}",
        build_issue_uri(project, number),
        exec_seq,
        node_id
    )
}

fn build_node_subresource_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    resource: &str,
) -> String {
    format!(
        "{}/{}",
        build_node_uri(project, number, exec_seq, node_id),
        resource
    )
}

fn build_node_segmented_resource_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    resource: &str,
    segment: &str,
) -> String {
    format!(
        "{}/{}",
        build_node_subresource_uri(project, number, exec_seq, node_id, resource),
        segment
    )
}

fn build_task_subresource_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: &str,
    resource: &str,
) -> String {
    format!(
        "{}/task/{}/{}",
        build_node_uri(project, number, exec_seq, node_id),
        task_name,
        resource
    )
}

/// Canonical base URI for a job, used as its run home (`cairn:~`).
///
/// A top-level node job is `.../{seq}/{segment}`. A sub-agent task job nests
/// under its parent node as `.../{seq}/{parent}/task/{segment}` — matching the
/// shape every task sub-resource builder uses (artifact/chat/todos). Pass the
/// task's own `uri_segment` as `segment` and the parent node's `uri_segment` as
/// `parent_segment`; `None` parent means a top-level node.
pub fn build_job_base_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    segment: &str,
    parent_segment: Option<&str>,
) -> String {
    match parent_segment {
        Some(parent) => format!(
            "{}/task/{}",
            build_node_uri(project, number, exec_seq, parent),
            segment
        ),
        None => build_node_uri(project, number, exec_seq, segment),
    }
}

pub fn build_node_chat_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "chat")
}

pub fn build_node_artifact_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_artifact_uri_named(project, number, exec_seq, node_id, None)
}

/// Build a node artifact URI. `name: Some("plan")` emits `.../{node}/plan`;
/// `None` emits the generic `.../{node}/artifact` alias.
pub fn build_node_artifact_uri_named(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    name: Option<&str>,
) -> String {
    build_node_subresource_uri(
        project,
        number,
        exec_seq,
        node_id,
        name.unwrap_or("artifact"),
    )
}

pub fn build_node_changed_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "changed")
}

pub fn build_node_terminal_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    slug: &str,
) -> String {
    build_node_segmented_resource_uri(project, number, exec_seq, node_id, "terminal", slug)
}

pub fn build_task_terminal_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: &str,
    slug: &str,
) -> String {
    format!(
        "{}/{}",
        build_task_subresource_uri(project, number, exec_seq, node_id, task_name, "terminal"),
        slug
    )
}

pub fn build_task_chat_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: &str,
) -> String {
    build_task_subresource_uri(project, number, exec_seq, node_id, task_name, "chat")
}

pub fn build_task_artifact_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: &str,
) -> String {
    build_task_artifact_uri_named(project, number, exec_seq, node_id, task_name, None)
}

/// Build a task artifact URI. `name: Some("plan")` emits
/// `.../task/{task}/plan`; `None` emits the generic `.../task/{task}/artifact`.
pub fn build_task_artifact_uri_named(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: &str,
    name: Option<&str>,
) -> String {
    build_task_subresource_uri(
        project,
        number,
        exec_seq,
        node_id,
        task_name,
        name.unwrap_or("artifact"),
    )
}

pub fn build_job_todos_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: Option<&str>,
) -> String {
    match task_name {
        Some(task_name) => {
            build_task_subresource_uri(project, number, exec_seq, node_id, task_name, "todos")
        }
        None => build_node_subresource_uri(project, number, exec_seq, node_id, "todos"),
    }
}

pub fn build_node_tasks_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "tasks")
}

pub fn build_node_wakes_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "wakes")
}

pub fn build_node_questions_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "questions")
}

pub fn build_node_question_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    segment: &str,
) -> String {
    build_node_segmented_resource_uri(project, number, exec_seq, node_id, "questions", segment)
}

pub fn build_node_permissions_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "permissions")
}

pub fn build_node_permission_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    segment: &str,
) -> String {
    build_node_segmented_resource_uri(project, number, exec_seq, node_id, "permissions", segment)
}

pub fn build_project_terminal_uri(project: &str, slug: &str) -> String {
    format!("{}/terminal/{}", build_project_uri(project), slug)
}

fn append_path(base: String, path: &[String]) -> String {
    if path.is_empty() {
        base
    } else {
        format!("{}/{}", base, path.join("/"))
    }
}

pub fn build_bug_uri() -> String {
    "cairn://bug".to_string()
}

pub fn build_skills_uri() -> String {
    "cairn://skills".to_string()
}

pub fn build_skill_uri(skill_id: &str, path: &[String]) -> String {
    append_path(format!("cairn://skills/{}", skill_id), path)
}

pub fn build_project_skills_uri(project: &str) -> String {
    format!("{}/skills", build_project_uri(project))
}

pub fn build_project_skill_uri(project: &str, skill_id: &str, path: &[String]) -> String {
    append_path(
        format!("{}/skills/{}", build_project_uri(project), skill_id),
        path,
    )
}

pub fn build_labels_uri() -> String {
    "cairn://labels".to_string()
}

pub fn build_label_uri(label_id: &str) -> String {
    format!("cairn://labels/{}", label_id)
}

pub fn build_node_memories_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "memories")
}

pub fn build_node_symbols_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    symbol: Option<&str>,
) -> String {
    let base = build_node_subresource_uri(project, number, exec_seq, node_id, "symbols");
    match symbol {
        Some(symbol) => format!("{base}/{symbol}"),
        None => base,
    }
}

pub fn build_project_symbols_uri(project: &str, symbol: Option<&str>) -> String {
    let base = format!("{}/symbols", build_project_uri(project));
    match symbol {
        Some(symbol) => format!("{base}/{symbol}"),
        None => base,
    }
}

pub fn build_node_memory_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    memory_seq: i32,
) -> String {
    format!(
        "{}/{}",
        build_node_memories_uri(project, number, exec_seq, node_id),
        memory_seq
    )
}

pub fn build_recipes_uri() -> String {
    "cairn://recipes".to_string()
}

pub fn build_recipe_uri(recipe_id: &str) -> String {
    format!("cairn://recipes/{}", recipe_id)
}

pub fn build_project_recipes_uri(project: &str) -> String {
    format!("{}/recipes", build_project_uri(project))
}

pub fn build_project_recipe_uri(project: &str, recipe_id: &str) -> String {
    format!("{}/recipes/{}", build_project_uri(project), recipe_id)
}

pub fn build_agents_uri() -> String {
    "cairn://agents".to_string()
}

pub fn build_agent_uri(agent_id: &str) -> String {
    format!("cairn://agents/{}", agent_id)
}

pub fn build_project_agents_uri(project: &str) -> String {
    format!("{}/agents", build_project_uri(project))
}

pub fn build_project_agent_uri(project: &str, agent_id: &str) -> String {
    format!("{}/agents/{}", build_project_uri(project), agent_id)
}

pub fn build_actions_uri() -> String {
    "cairn://actions".to_string()
}

pub fn build_action_uri(action_id: &str) -> String {
    format!("cairn://actions/{}", action_id)
}

pub fn build_project_actions_uri(project: &str) -> String {
    format!("{}/actions", build_project_uri(project))
}

pub fn build_project_action_uri(project: &str, action_id: &str) -> String {
    format!("{}/actions/{}", build_project_uri(project), action_id)
}

pub fn build_settings_uri() -> String {
    "cairn://settings".to_string()
}

pub fn build_projects_uri() -> String {
    "cairn://projects".to_string()
}

pub fn build_project_settings_uri(project: &str) -> String {
    format!("{}/settings", build_project_uri(project))
}

impl CairnResource {
    pub fn to_uri(&self) -> String {
        match self {
            Self::Project { project } => build_project_uri(project),
            Self::ProjectIssues { project } => build_project_issues_uri(project),
            Self::Issue { project, number } => build_issue_uri(project, *number),
            Self::Node {
                project,
                number,
                exec_seq,
                node_id,
            } => build_node_uri(project, *number, *exec_seq, node_id),
            Self::NodeChat {
                project,
                number,
                exec_seq,
                node_id,
            } => build_node_chat_uri(project, *number, *exec_seq, node_id),
            Self::NodeChatRaw {
                project,
                number,
                exec_seq,
                node_id,
            } => format!(
                "{}/raw",
                build_node_chat_uri(project, *number, *exec_seq, node_id)
            ),
            Self::NodeChatTurn {
                project,
                number,
                exec_seq,
                node_id,
                turn_seq,
            } => format!(
                "{}/turn/{}",
                build_node_chat_uri(project, *number, *exec_seq, node_id),
                turn_seq
            ),
            Self::NodeChatEvent {
                project,
                number,
                exec_seq,
                node_id,
                run_seq,
                event_seq,
            } => format!(
                "{}/{}/{}",
                build_node_chat_uri(project, *number, *exec_seq, node_id),
                run_seq,
                event_seq
            ),
            Self::NodeArtifact {
                project,
                number,
                exec_seq,
                node_id,
                name,
            } => {
                build_node_artifact_uri_named(project, *number, *exec_seq, node_id, name.as_deref())
            }
            Self::NodeTerminal {
                project,
                number,
                exec_seq,
                node_id,
                slug,
            } => build_node_terminal_uri(project, *number, *exec_seq, node_id, slug),
            Self::TaskTerminal {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                slug,
            } => build_task_terminal_uri(project, *number, *exec_seq, node_id, task_name, slug),
            Self::Task {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => build_job_base_uri(project, *number, *exec_seq, task_name, Some(node_id)),
            Self::TaskChat {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => build_task_chat_uri(project, *number, *exec_seq, node_id, task_name),
            Self::TaskChatRaw {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => format!(
                "{}/raw",
                build_task_chat_uri(project, *number, *exec_seq, node_id, task_name)
            ),
            Self::TaskChatTurn {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                turn_seq,
            } => format!(
                "{}/turn/{}",
                build_task_chat_uri(project, *number, *exec_seq, node_id, task_name),
                turn_seq
            ),
            Self::TaskChatEvent {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                run_seq,
                event_seq,
            } => format!(
                "{}/{}/{}",
                build_task_chat_uri(project, *number, *exec_seq, node_id, task_name),
                run_seq,
                event_seq
            ),
            Self::TaskArtifact {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                name,
            } => build_task_artifact_uri_named(
                project,
                *number,
                *exec_seq,
                node_id,
                task_name,
                name.as_deref(),
            ),
            Self::JobTodos {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => build_job_todos_uri(project, *number, *exec_seq, node_id, task_name.as_deref()),
            Self::NodeTasks {
                project,
                number,
                exec_seq,
                node_id,
            } => build_node_tasks_uri(project, *number, *exec_seq, node_id),
            Self::NodeWakes {
                project,
                number,
                exec_seq,
                node_id,
            } => build_node_wakes_uri(project, *number, *exec_seq, node_id),
            Self::NodeQuestions {
                project,
                number,
                exec_seq,
                node_id,
            } => build_node_questions_uri(project, *number, *exec_seq, node_id),
            Self::NodeQuestion {
                project,
                number,
                exec_seq,
                node_id,
                segment,
            } => build_node_question_uri(project, *number, *exec_seq, node_id, segment),
            Self::NodePermissions {
                project,
                number,
                exec_seq,
                node_id,
            } => build_node_permissions_uri(project, *number, *exec_seq, node_id),
            Self::NodePermission {
                project,
                number,
                exec_seq,
                node_id,
                segment,
            } => build_node_permission_uri(project, *number, *exec_seq, node_id, segment),
            Self::NodeMessages {
                project,
                number,
                exec_seq,
                node_id,
            } => build_node_messages_uri(project, *number, *exec_seq, node_id),
            Self::TaskMessages {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => build_task_messages_uri(project, *number, *exec_seq, node_id, task_name),
            Self::ProjectMessages { project } => build_project_messages_uri(project),
            Self::IssueMessages { project, number } => build_issue_messages_uri(project, *number),
            Self::Changed { project, number } => build_issue_changed_uri(project, *number),
            Self::IssueExecutions { project, number } => {
                build_issue_executions_uri(project, *number)
            }
            Self::IssueComments { project, number } => build_issue_comments_uri(project, *number),
            Self::IssueComment {
                project,
                number,
                comment_seq,
            } => build_issue_comment_uri(project, *number, *comment_seq),
            Self::IssueExecution {
                project,
                number,
                exec_seq,
            } => build_issue_execution_uri(project, *number, *exec_seq),
            Self::NodeChanged {
                project,
                number,
                exec_seq,
                node_id,
            } => build_node_changed_uri(project, *number, *exec_seq, node_id),
            Self::ProjectTerminal { project, slug } => build_project_terminal_uri(project, slug),
            Self::NodeSymbols {
                project,
                number,
                exec_seq,
                node_id,
                symbol,
            } => build_node_symbols_uri(project, *number, *exec_seq, node_id, symbol.as_deref()),
            Self::ProjectSymbols { project, symbol } => {
                build_project_symbols_uri(project, symbol.as_deref())
            }
            Self::Db => "cairn://db".to_string(),
            Self::Logs => "cairn://logs".to_string(),
            Self::Bug => "cairn://bug".to_string(),
            Self::Help => "cairn://help".to_string(),
            Self::WebSearch => "cairn://websearch".to_string(),
            Self::Mcp { server, resource } => {
                let mut s = "cairn://mcp".to_string();
                if let Some(server) = server {
                    s.push('/');
                    s.push_str(server);
                    if let Some(resource) = resource {
                        s.push('/');
                        s.push_str(resource);
                    }
                }
                s
            }
            Self::Skills => build_skills_uri(),
            Self::Skill { skill_id, path } => build_skill_uri(skill_id, path),
            Self::ProjectSkills { project } => build_project_skills_uri(project),
            Self::ProjectSkill {
                project,
                skill_id,
                path,
            } => build_project_skill_uri(project, skill_id, path),
            Self::Labels => build_labels_uri(),
            Self::Label { label_id } => build_label_uri(label_id),
            Self::NodeMemories {
                project,
                number,
                exec_seq,
                node_id,
            } => build_node_memories_uri(project, *number, *exec_seq, node_id),
            Self::NodeMemory {
                project,
                number,
                exec_seq,
                node_id,
                memory_seq,
            } => build_node_memory_uri(project, *number, *exec_seq, node_id, *memory_seq),
            Self::Recipes => build_recipes_uri(),
            Self::Recipe { recipe_id } => build_recipe_uri(recipe_id),
            Self::ProjectRecipes { project } => build_project_recipes_uri(project),
            Self::ProjectRecipe { project, recipe_id } => {
                build_project_recipe_uri(project, recipe_id)
            }
            Self::Agents => build_agents_uri(),
            Self::Agent { agent_id } => build_agent_uri(agent_id),
            Self::ProjectAgents { project } => build_project_agents_uri(project),
            Self::ProjectAgent { project, agent_id } => build_project_agent_uri(project, agent_id),
            Self::Actions => build_actions_uri(),
            Self::Action { action_id } => build_action_uri(action_id),
            Self::ProjectActions { project } => build_project_actions_uri(project),
            Self::ProjectAction { project, action_id } => {
                build_project_action_uri(project, action_id)
            }
            Self::Settings => build_settings_uri(),
            Self::Projects => build_projects_uri(),
            Self::ProjectSettings { project } => build_project_settings_uri(project),
        }
    }

    pub fn to_route(&self) -> Option<String> {
        // Settings/Projects have no project; ProjectSettings has no dedicated UI
        // route. None for all three (the `?` below short-circuits the first two).
        if matches!(
            self,
            Self::Settings | Self::Projects | Self::ProjectSettings { .. }
        ) {
            return None;
        }
        let project = self.project()?.to_lowercase();
        match self {
            Self::Settings | Self::Projects | Self::ProjectSettings { .. } => None,
            Self::Project { .. } => Some(format!("/p/{}/issues", project)),
            Self::Issue { number, .. } => Some(format!("/p/{}/i/{}", project, number)),
            Self::Node {
                number,
                exec_seq,
                node_id,
                ..
            }
            | Self::NodeChat {
                number,
                exec_seq,
                node_id,
                ..
            } => Some(format!(
                "/p/{}/i/{}/{}/{}/chat",
                project, number, exec_seq, node_id
            )),
            Self::NodeArtifact {
                number,
                exec_seq,
                node_id,
                ..
            } => Some(format!(
                "/p/{}/i/{}/{}/{}/artifact",
                project, number, exec_seq, node_id
            )),
            Self::NodeTerminal {
                number,
                exec_seq,
                node_id,
                slug,
                ..
            } => Some(format!(
                "/p/{}/i/{}/{}/{}?terminalId={}",
                project, number, exec_seq, node_id, slug
            )),
            Self::TaskTerminal {
                number,
                exec_seq,
                node_id,
                task_name,
                slug,
                ..
            } => Some(format!(
                "/p/{}/i/{}/{}/{}/task/{}?terminalId={}",
                project, number, exec_seq, node_id, task_name, slug
            )),
            Self::NodeMemories {
                number,
                exec_seq,
                node_id,
                ..
            } => Some(format!(
                "/p/{}/i/{}/{}/{}/memories",
                project, number, exec_seq, node_id
            )),
            Self::NodeMemory {
                number,
                exec_seq,
                node_id,
                memory_seq,
                ..
            } => Some(format!(
                "/p/{}/i/{}/{}/{}/memories/{}",
                project, number, exec_seq, node_id, memory_seq
            )),
            Self::Task {
                number,
                exec_seq,
                node_id,
                task_name,
                ..
            }
            | Self::TaskChat {
                number,
                exec_seq,
                node_id,
                task_name,
                ..
            } => Some(format!(
                "/p/{}/i/{}/{}/{}/task/{}/chat",
                project, number, exec_seq, node_id, task_name
            )),
            Self::ProjectTerminal { slug, .. } => {
                Some(format!("/p/{}/terminal?terminalId={}", project, slug))
            }
            Self::NodeChatRaw { .. }
            | Self::NodeChatTurn { .. }
            | Self::NodeChatEvent { .. }
            | Self::TaskChatRaw { .. }
            | Self::TaskChatTurn { .. }
            | Self::TaskChatEvent { .. }
            | Self::TaskArtifact { .. }
            | Self::JobTodos { .. }
            | Self::NodeTasks { .. }
            | Self::NodeWakes { .. }
            | Self::NodeQuestions { .. }
            | Self::NodeQuestion { .. }
            | Self::NodePermissions { .. }
            | Self::NodePermission { .. }
            | Self::NodeMessages { .. }
            | Self::TaskMessages { .. }
            | Self::ProjectIssues { .. }
            | Self::ProjectMessages { .. }
            | Self::IssueMessages { .. }
            | Self::Changed { .. }
            | Self::IssueExecutions { .. }
            | Self::IssueComments { .. }
            | Self::IssueComment { .. }
            | Self::IssueExecution { .. }
            | Self::NodeChanged { .. }
            | Self::Skills
            | Self::Skill { .. }
            | Self::ProjectSkills { .. }
            | Self::ProjectSkill { .. }
            | Self::Labels
            | Self::Label { .. }
            | Self::Recipes
            | Self::Recipe { .. }
            | Self::ProjectRecipes { .. }
            | Self::ProjectRecipe { .. }
            | Self::Agents
            | Self::Agent { .. }
            | Self::ProjectAgents { .. }
            | Self::ProjectAgent { .. }
            | Self::Actions
            | Self::Action { .. }
            | Self::ProjectActions { .. }
            | Self::ProjectAction { .. }
            | Self::NodeSymbols { .. }
            | Self::ProjectSymbols { .. }
            | Self::Db
            | Self::Logs
            | Self::Bug
            | Self::Help
            | Self::WebSearch
            | Self::Mcp { .. } => None,
        }
    }

    pub fn project(&self) -> Option<&str> {
        match self {
            Self::ProjectSettings { project } => Some(project),
            Self::Settings | Self::Projects => None,
            Self::Project { project }
            | Self::ProjectIssues { project }
            | Self::Issue { project, .. }
            | Self::Node { project, .. }
            | Self::NodeChat { project, .. }
            | Self::NodeChatRaw { project, .. }
            | Self::NodeChatTurn { project, .. }
            | Self::NodeChatEvent { project, .. }
            | Self::NodeArtifact { project, .. }
            | Self::NodeTerminal { project, .. }
            | Self::TaskTerminal { project, .. }
            | Self::Task { project, .. }
            | Self::TaskChat { project, .. }
            | Self::TaskChatRaw { project, .. }
            | Self::TaskChatTurn { project, .. }
            | Self::TaskChatEvent { project, .. }
            | Self::TaskArtifact { project, .. }
            | Self::JobTodos { project, .. }
            | Self::NodeTasks { project, .. }
            | Self::NodeWakes { project, .. }
            | Self::NodeQuestions { project, .. }
            | Self::NodeQuestion { project, .. }
            | Self::NodePermissions { project, .. }
            | Self::NodePermission { project, .. }
            | Self::NodeMessages { project, .. }
            | Self::TaskMessages { project, .. }
            | Self::ProjectMessages { project }
            | Self::IssueMessages { project, .. }
            | Self::IssueComments { project, .. }
            | Self::IssueComment { project, .. }
            | Self::Changed { project, .. }
            | Self::IssueExecutions { project, .. }
            | Self::IssueExecution { project, .. }
            | Self::NodeChanged { project, .. }
            | Self::ProjectTerminal { project, .. }
            | Self::ProjectSkills { project }
            | Self::ProjectSkill { project, .. }
            | Self::NodeMemories { project, .. }
            | Self::NodeMemory { project, .. }
            | Self::ProjectRecipes { project }
            | Self::ProjectRecipe { project, .. }
            | Self::ProjectAgents { project }
            | Self::ProjectAgent { project, .. }
            | Self::ProjectActions { project }
            | Self::ProjectAction { project, .. }
            | Self::NodeSymbols { project, .. }
            | Self::ProjectSymbols { project, .. } => Some(project),
            Self::Skills
            | Self::Skill { .. }
            | Self::Labels
            | Self::Label { .. }
            | Self::Recipes
            | Self::Recipe { .. }
            | Self::Agents
            | Self::Agent { .. }
            | Self::Actions
            | Self::Action { .. }
            | Self::Db
            | Self::Logs
            | Self::Bug
            | Self::Help
            | Self::WebSearch
            | Self::Mcp { .. } => None,
        }
    }

    pub fn issue_number(&self) -> Option<i32> {
        match self {
            Self::Settings | Self::Projects | Self::ProjectSettings { .. } => None,
            Self::Issue { number, .. }
            | Self::Node { number, .. }
            | Self::NodeChat { number, .. }
            | Self::NodeChatRaw { number, .. }
            | Self::NodeChatTurn { number, .. }
            | Self::NodeChatEvent { number, .. }
            | Self::NodeArtifact { number, .. }
            | Self::NodeTerminal { number, .. }
            | Self::TaskTerminal { number, .. }
            | Self::Task { number, .. }
            | Self::TaskChat { number, .. }
            | Self::TaskChatRaw { number, .. }
            | Self::TaskChatTurn { number, .. }
            | Self::TaskChatEvent { number, .. }
            | Self::TaskArtifact { number, .. }
            | Self::JobTodos { number, .. }
            | Self::NodeTasks { number, .. }
            | Self::NodeWakes { number, .. }
            | Self::NodeQuestions { number, .. }
            | Self::NodeQuestion { number, .. }
            | Self::NodePermissions { number, .. }
            | Self::NodePermission { number, .. }
            | Self::NodeMessages { number, .. }
            | Self::TaskMessages { number, .. }
            | Self::IssueMessages { number, .. }
            | Self::IssueComments { number, .. }
            | Self::IssueComment { number, .. }
            | Self::Changed { number, .. }
            | Self::IssueExecutions { number, .. }
            | Self::IssueExecution { number, .. }
            | Self::NodeChanged { number, .. }
            | Self::NodeMemories { number, .. }
            | Self::NodeMemory { number, .. }
            | Self::NodeSymbols { number, .. } => Some(*number),
            Self::Project { .. }
            | Self::ProjectIssues { .. }
            | Self::ProjectMessages { .. }
            | Self::ProjectTerminal { .. }
            | Self::Skills
            | Self::Skill { .. }
            | Self::ProjectSkills { .. }
            | Self::ProjectSkill { .. }
            | Self::Labels
            | Self::Label { .. }
            | Self::Recipes
            | Self::Recipe { .. }
            | Self::ProjectRecipes { .. }
            | Self::ProjectRecipe { .. }
            | Self::Agents
            | Self::Agent { .. }
            | Self::ProjectAgents { .. }
            | Self::ProjectAgent { .. }
            | Self::Actions
            | Self::Action { .. }
            | Self::ProjectActions { .. }
            | Self::ProjectAction { .. }
            | Self::ProjectSymbols { .. }
            | Self::Db
            | Self::Logs
            | Self::Bug
            | Self::Help
            | Self::WebSearch
            | Self::Mcp { .. } => None,
        }
    }

    pub fn node_id(&self) -> Option<&str> {
        match self {
            Self::Node { node_id, .. }
            | Self::NodeChat { node_id, .. }
            | Self::NodeChatRaw { node_id, .. }
            | Self::NodeChatTurn { node_id, .. }
            | Self::NodeChatEvent { node_id, .. }
            | Self::NodeArtifact { node_id, .. }
            | Self::NodeTerminal { node_id, .. }
            | Self::TaskTerminal { node_id, .. }
            | Self::Task { node_id, .. }
            | Self::TaskChat { node_id, .. }
            | Self::TaskChatRaw { node_id, .. }
            | Self::TaskChatTurn { node_id, .. }
            | Self::TaskChatEvent { node_id, .. }
            | Self::TaskArtifact { node_id, .. }
            | Self::JobTodos { node_id, .. }
            | Self::NodeTasks { node_id, .. }
            | Self::NodeQuestions { node_id, .. }
            | Self::NodeQuestion { node_id, .. }
            | Self::NodePermissions { node_id, .. }
            | Self::NodePermission { node_id, .. }
            | Self::NodeMessages { node_id, .. }
            | Self::TaskMessages { node_id, .. }
            | Self::NodeChanged { node_id, .. }
            | Self::NodeMemories { node_id, .. }
            | Self::NodeMemory { node_id, .. }
            | Self::NodeSymbols { node_id, .. } => Some(node_id),
            _ => None,
        }
    }

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
            Self::Issue { .. } => ResourceKind::Issue,
            Self::Changed { .. } => ResourceKind::Changed,
            Self::IssueExecutions { .. } => ResourceKind::IssueExecutions,
            Self::IssueExecution { .. } => ResourceKind::IssueExecution,
            Self::IssueMessages { .. } => ResourceKind::IssueMessages,
            Self::IssueComments { .. } => ResourceKind::IssueComments,
            Self::IssueComment { .. } => ResourceKind::IssueComment,
            Self::NodeMessages { .. } => ResourceKind::NodeMessages,
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
            Self::Task { .. } => ResourceKind::Task,
            Self::TaskChat { .. } => ResourceKind::TaskChat,
            Self::TaskChatRaw { .. } => ResourceKind::TaskChatRaw,
            Self::TaskChatTurn { .. } => ResourceKind::TaskChatTurn,
            Self::TaskChatEvent { .. } => ResourceKind::TaskChatEvent,
            Self::TaskArtifact { .. } => ResourceKind::TaskArtifact,
            Self::JobTodos { .. } => ResourceKind::JobTodos,
            Self::NodeTasks { .. } => ResourceKind::NodeTasks,
            Self::NodeWakes { .. } => ResourceKind::NodeWakes,
            Self::NodeQuestions { .. } => ResourceKind::NodeQuestions,
            Self::NodeQuestion { .. } => ResourceKind::NodeQuestion,
            Self::NodePermissions { .. } => ResourceKind::NodePermissions,
            Self::NodePermission { .. } => ResourceKind::NodePermission,
            Self::Db => ResourceKind::Db,
            Self::Logs => ResourceKind::Logs,
            Self::Bug => ResourceKind::Bug,
            Self::Help => ResourceKind::Help,
            Self::WebSearch => ResourceKind::WebSearch,
            Self::Mcp { .. } => ResourceKind::Mcp,
            Self::Skills => ResourceKind::Skills,
            Self::Skill { .. } => ResourceKind::Skill,
            Self::ProjectSkills { .. } => ResourceKind::ProjectSkills,
            Self::ProjectSkill { .. } => ResourceKind::ProjectSkill,
            Self::Labels => ResourceKind::Labels,
            Self::Label { .. } => ResourceKind::Label,
            Self::NodeMemories { .. } => ResourceKind::NodeMemories,
            Self::NodeMemory { .. } => ResourceKind::NodeMemory,
            Self::Recipes => ResourceKind::Recipes,
            Self::Recipe { .. } => ResourceKind::Recipe,
            Self::ProjectRecipes { .. } => ResourceKind::ProjectRecipes,
            Self::ProjectRecipe { .. } => ResourceKind::ProjectRecipe,
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

pub fn parse_resource_uri(uri: &str) -> Result<Option<CairnResourceUri>, String> {
    let (identity, raw_query) = match uri.split_once('?') {
        Some((identity, query)) => (identity, Some(query)),
        None => (uri, None),
    };
    let Some(resource) = parse_uri(identity) else {
        return Ok(None);
    };
    let params = raw_query
        .map(parse_query_params)
        .transpose()?
        .unwrap_or_default();
    Ok(Some(CairnResourceUri { resource, params }))
}

pub fn parse_uri(uri: &str) -> Option<CairnResource> {
    let uri = uri.strip_prefix("cairn://")?;

    // External MCP gateway family. Handled before the '?'-split and the generic
    // '/'-split so the external resource tail survives intact — it may contain
    // '/', '://', AND its own '?query', none of which are Cairn-side syntax
    // (the family advertises no read projections). A server name itself never
    // contains '?' or '/'.
    if uri == "mcp" {
        return Some(CairnResource::Mcp {
            server: None,
            resource: None,
        });
    }
    if let Some(rest) = uri.strip_prefix("mcp/") {
        let mut segs = rest.splitn(2, '/');
        let server = segs.next()?;
        if server.is_empty() || server.contains('?') {
            return None;
        }
        let resource = segs.next().filter(|s| !s.is_empty()).map(|s| s.to_string());
        return Some(CairnResource::Mcp {
            server: Some(server.to_string()),
            resource,
        });
    }

    let path = uri.split('?').next()?;
    if path.is_empty() {
        return None;
    }
    let parts: Vec<&str> = path.split('/').collect();
    if parts.iter().any(|part| part.is_empty()) {
        return None;
    }

    match parts.as_slice() {
        ["db"] => Some(CairnResource::Db),
        ["logs"] => Some(CairnResource::Logs),
        ["bug"] => Some(CairnResource::Bug),
        ["help"] => Some(CairnResource::Help),
        ["websearch"] => Some(CairnResource::WebSearch),
        ["skills"] => Some(CairnResource::Skills),
        ["skills", skill_id, rest @ ..] => Some(CairnResource::Skill {
            skill_id: (*skill_id).to_string(),
            path: rest.iter().map(|segment| (*segment).to_string()).collect(),
        }),
        [PROJECT_SCOPE, project, "skills"] => Some(CairnResource::ProjectSkills {
            project: canonical_project(project),
        }),
        [PROJECT_SCOPE, project, "skills", skill_id, rest @ ..] => {
            Some(CairnResource::ProjectSkill {
                project: canonical_project(project),
                skill_id: (*skill_id).to_string(),
                path: rest.iter().map(|segment| (*segment).to_string()).collect(),
            })
        }
        ["settings"] => Some(CairnResource::Settings),
        ["projects"] => Some(CairnResource::Projects),
        ["labels"] => Some(CairnResource::Labels),
        ["labels", label_id] => Some(CairnResource::Label {
            label_id: (*label_id).to_string(),
        }),
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "memories"] => {
            Some(CairnResource::NodeMemories {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "memories", memory_seq] => {
            Some(CairnResource::NodeMemory {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                memory_seq: parse_positive_i32(memory_seq)?,
            })
        }
        ["recipes"] => Some(CairnResource::Recipes),
        ["recipes", recipe_id] => Some(CairnResource::Recipe {
            recipe_id: (*recipe_id).to_string(),
        }),
        [PROJECT_SCOPE, project, "recipes"] => Some(CairnResource::ProjectRecipes {
            project: canonical_project(project),
        }),
        [PROJECT_SCOPE, project, "recipes", recipe_id] => Some(CairnResource::ProjectRecipe {
            project: canonical_project(project),
            recipe_id: (*recipe_id).to_string(),
        }),
        ["agents"] => Some(CairnResource::Agents),
        ["agents", agent_id] => Some(CairnResource::Agent {
            agent_id: (*agent_id).to_string(),
        }),
        [PROJECT_SCOPE, project, "agents"] => Some(CairnResource::ProjectAgents {
            project: canonical_project(project),
        }),
        [PROJECT_SCOPE, project, "agents", agent_id] => Some(CairnResource::ProjectAgent {
            project: canonical_project(project),
            agent_id: (*agent_id).to_string(),
        }),
        ["actions"] => Some(CairnResource::Actions),
        ["actions", action_id] => Some(CairnResource::Action {
            action_id: (*action_id).to_string(),
        }),
        [PROJECT_SCOPE, project, "actions"] => Some(CairnResource::ProjectActions {
            project: canonical_project(project),
        }),
        [PROJECT_SCOPE, project, "actions", action_id] => Some(CairnResource::ProjectAction {
            project: canonical_project(project),
            action_id: (*action_id).to_string(),
        }),
        // Literal `symbols` segment(s); must precede the numeric issue arm below so a
        // project-scoped symbols URI is never misread as an issue id.
        [PROJECT_SCOPE, project, "symbols"] => Some(CairnResource::ProjectSymbols {
            project: canonical_project(project),
            symbol: None,
        }),
        [PROJECT_SCOPE, project, "symbols", symbol] => Some(CairnResource::ProjectSymbols {
            project: canonical_project(project),
            symbol: Some((*symbol).to_string()),
        }),
        [PROJECT_SCOPE, project] => Some(CairnResource::Project {
            project: canonical_project(project),
        }),
        [PROJECT_SCOPE, project, "issues"] => Some(CairnResource::ProjectIssues {
            project: canonical_project(project),
        }),
        [PROJECT_SCOPE, project, "messages"] => Some(CairnResource::ProjectMessages {
            project: canonical_project(project),
        }),
        // Must precede the `[PROJECT_SCOPE, project, number]` issue arm: a
        // literal `settings` segment is not a numeric issue id.
        [PROJECT_SCOPE, project, "settings"] => Some(CairnResource::ProjectSettings {
            project: canonical_project(project),
        }),
        [PROJECT_SCOPE, project, "terminal", slug] => Some(CairnResource::ProjectTerminal {
            project: canonical_project(project),
            slug: (*slug).to_string(),
        }),
        [PROJECT_SCOPE, project, number] => Some(CairnResource::Issue {
            project: canonical_project(project),
            number: parse_positive_i32(number)?,
        }),
        [PROJECT_SCOPE, project, number, "changed"] => Some(CairnResource::Changed {
            project: canonical_project(project),
            number: parse_positive_i32(number)?,
        }),
        [PROJECT_SCOPE, project, number, "messages"] => Some(CairnResource::IssueMessages {
            project: canonical_project(project),
            number: parse_positive_i32(number)?,
        }),
        [PROJECT_SCOPE, project, number, "executions"] => Some(CairnResource::IssueExecutions {
            project: canonical_project(project),
            number: parse_positive_i32(number)?,
        }),
        [PROJECT_SCOPE, project, number, "comments"] => Some(CairnResource::IssueComments {
            project: canonical_project(project),
            number: parse_positive_i32(number)?,
        }),
        // Must precede the `[PROJECT_SCOPE, project, number, exec_seq, node_id]`
        // node arm: that arm binds `exec_seq`/`node_id` to any segments, so a
        // literal `comments` member URI would otherwise be misread as a node.
        [PROJECT_SCOPE, project, number, "comments", comment_seq] => {
            Some(CairnResource::IssueComment {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                comment_seq: parse_positive_i32(comment_seq)?,
            })
        }
        // MUST precede the Node arm: both are 5-segment shapes, but a literal
        // `executions` in the 4th position names a single execution snapshot,
        // not a node whose exec_seq happens to parse here.
        [PROJECT_SCOPE, project, number, "executions", exec_seq] => {
            Some(CairnResource::IssueExecution {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id] => Some(CairnResource::Node {
            project: canonical_project(project),
            number: parse_positive_i32(number)?,
            exec_seq: parse_positive_i32(exec_seq)?,
            node_id: (*node_id).to_string(),
        }),
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "chat"] => {
            Some(CairnResource::NodeChat {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "chat", "raw"] => {
            Some(CairnResource::NodeChatRaw {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "chat", "turn", turn_seq] => {
            Some(CairnResource::NodeChatTurn {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                turn_seq: parse_non_negative_i32(turn_seq)?,
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "chat", run_seq, event_seq] => {
            Some(CairnResource::NodeChatEvent {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                run_seq: parse_positive_i32(run_seq)?,
                event_seq: parse_non_negative_i32(event_seq)?,
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "artifact"] => {
            Some(CairnResource::NodeArtifact {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                name: None,
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "changed"] => {
            Some(CairnResource::NodeChanged {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "symbols"] => {
            Some(CairnResource::NodeSymbols {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                symbol: None,
            })
        }
        // A `::`/`.`-qualified symbol stays one path segment, so a
        // container-qualified name (`Foo::bar`) passes through intact.
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "symbols", symbol] => {
            Some(CairnResource::NodeSymbols {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                symbol: Some((*symbol).to_string()),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "todos"] => {
            Some(CairnResource::JobTodos {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: None,
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "tasks"] => {
            Some(CairnResource::NodeTasks {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "wakes"] => {
            Some(CairnResource::NodeWakes {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "questions"] => {
            Some(CairnResource::NodeQuestions {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "questions", segment] => {
            Some(CairnResource::NodeQuestion {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                segment: (*segment).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "permissions"] => {
            Some(CairnResource::NodePermissions {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "permissions", segment] => {
            Some(CairnResource::NodePermission {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                segment: (*segment).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "messages"] => {
            Some(CairnResource::NodeMessages {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "todos"] => {
            Some(CairnResource::JobTodos {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: Some((*task_name).to_string()),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "terminal", slug] => {
            Some(CairnResource::NodeTerminal {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                slug: (*slug).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "terminal", slug] => {
            Some(CairnResource::TaskTerminal {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
                slug: (*slug).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name] => {
            Some(CairnResource::Task {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "chat"] => {
            Some(CairnResource::TaskChat {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "chat", "raw"] => {
            Some(CairnResource::TaskChatRaw {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "chat", "turn", turn_seq] => {
            Some(CairnResource::TaskChatTurn {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
                turn_seq: parse_non_negative_i32(turn_seq)?,
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "chat", run_seq, event_seq] => {
            Some(CairnResource::TaskChatEvent {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
                run_seq: parse_positive_i32(run_seq)?,
                event_seq: parse_non_negative_i32(event_seq)?,
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "artifact"] => {
            Some(CairnResource::TaskArtifact {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
                name: None,
            })
        }
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, "messages"] => {
            Some(CairnResource::TaskMessages {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
            })
        }
        // Type-named artifact under a task: a trailing non-reserved segment.
        [PROJECT_SCOPE, project, number, exec_seq, node_id, "task", task_name, name]
            if !is_reserved_node_segment(name) =>
        {
            Some(CairnResource::TaskArtifact {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                task_name: (*task_name).to_string(),
                name: Some((*name).to_string()),
            })
        }
        // Type-named artifact under a node: a trailing non-reserved segment
        // (`.../{node}/plan`). Reserved keywords are handled by the arms above.
        [PROJECT_SCOPE, project, number, exec_seq, node_id, name]
            if !is_reserved_node_segment(name) =>
        {
            Some(CairnResource::NodeArtifact {
                project: canonical_project(project),
                number: parse_positive_i32(number)?,
                exec_seq: parse_positive_i32(exec_seq)?,
                node_id: (*node_id).to_string(),
                name: Some((*name).to_string()),
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_symbol_resources_all_forms() {
        assert_eq!(
            parse_uri("cairn://p/cairn/12/1/builder/symbols"),
            Some(CairnResource::NodeSymbols {
                project: "CAIRN".to_string(),
                number: 12,
                exec_seq: 1,
                node_id: "builder".to_string(),
                symbol: None,
            })
        );
        assert_eq!(
            parse_uri("cairn://p/cairn/12/1/builder/symbols/build_widget"),
            Some(CairnResource::NodeSymbols {
                project: "CAIRN".to_string(),
                number: 12,
                exec_seq: 1,
                node_id: "builder".to_string(),
                symbol: Some("build_widget".to_string()),
            })
        );
        // A `::`-qualified symbol survives as one segment.
        assert_eq!(
            parse_uri("cairn://p/cairn/12/1/builder/symbols/Foo::bar"),
            Some(CairnResource::NodeSymbols {
                project: "CAIRN".to_string(),
                number: 12,
                exec_seq: 1,
                node_id: "builder".to_string(),
                symbol: Some("Foo::bar".to_string()),
            })
        );
        assert_eq!(
            parse_uri("cairn://p/cairn/symbols"),
            Some(CairnResource::ProjectSymbols {
                project: "CAIRN".to_string(),
                symbol: None,
            })
        );
        assert_eq!(
            parse_uri("cairn://p/cairn/symbols/build_widget"),
            Some(CairnResource::ProjectSymbols {
                project: "CAIRN".to_string(),
                symbol: Some("build_widget".to_string()),
            })
        );
    }

    #[test]
    fn symbols_segment_is_reserved_not_an_artifact() {
        assert!(is_reserved_node_segment("symbols"));
        // `.../node/symbols` must parse as NodeSymbols, never a NodeArtifact named "symbols".
        assert!(matches!(
            parse_uri("cairn://p/cairn/12/1/builder/symbols"),
            Some(CairnResource::NodeSymbols { .. })
        ));
    }

    #[test]
    fn symbol_uris_round_trip() {
        for uri in [
            "cairn://p/CAIRN/12/1/builder/symbols",
            "cairn://p/CAIRN/12/1/builder/symbols/build_widget",
            "cairn://p/CAIRN/symbols",
            "cairn://p/CAIRN/symbols/build_widget",
        ] {
            assert_eq!(parse_uri(uri).unwrap().to_uri(), uri, "round-trip {uri}");
        }
    }

    #[test]
    fn parses_canonical_project_resources() {
        assert_eq!(
            parse_uri("cairn://p/cairn/123/changed"),
            Some(CairnResource::Changed {
                project: "CAIRN".to_string(),
                number: 123,
            })
        );
        assert_eq!(
            parse_uri("cairn://p/CAIRN/issues"),
            Some(CairnResource::ProjectIssues {
                project: "CAIRN".to_string(),
            })
        );
        assert_eq!(
            parse_uri("cairn://p/CAIRN/messages"),
            Some(CairnResource::ProjectMessages {
                project: "CAIRN".to_string(),
            })
        );
    }

    #[test]
    fn parses_and_roundtrips_node_and_task_messages() {
        // Node `/messages` is the canonical node messaging target.
        let node = parse_uri("cairn://p/cairn/42/2/builder/messages");
        assert_eq!(
            node,
            Some(CairnResource::NodeMessages {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
            })
        );
        assert_eq!(
            node.unwrap().to_uri(),
            "cairn://p/CAIRN/42/2/builder/messages"
        );

        // Task `/messages` is the sub-agent analogue.
        let task = parse_uri("cairn://p/cairn/42/2/builder/task/review/messages");
        assert_eq!(
            task,
            Some(CairnResource::TaskMessages {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: "review".to_string(),
            })
        );
        assert_eq!(
            task.unwrap().to_uri(),
            "cairn://p/CAIRN/42/2/builder/task/review/messages"
        );

        // `/messages` is not mistaken for a type-named artifact.
        assert_eq!(
            parse_uri("cairn://p/CAIRN/42/2/builder/messages").map(|r| r.kind()),
            Some(ResourceKind::NodeMessages)
        );
    }

    #[test]
    fn parses_and_roundtrips_settings_family() {
        assert_eq!(parse_uri("cairn://settings"), Some(CairnResource::Settings));
        assert_eq!(CairnResource::Settings.to_uri(), "cairn://settings");
        assert_eq!(CairnResource::Settings.kind(), ResourceKind::Settings);
        assert_eq!(CairnResource::Settings.project(), None);
        assert_eq!(CairnResource::Settings.to_route(), None);

        assert_eq!(parse_uri("cairn://projects"), Some(CairnResource::Projects));
        assert_eq!(CairnResource::Projects.to_uri(), "cairn://projects");
        assert_eq!(CairnResource::Projects.kind(), ResourceKind::Projects);

        let ps = parse_uri("cairn://p/cairn/settings");
        assert_eq!(
            ps,
            Some(CairnResource::ProjectSettings {
                project: "CAIRN".to_string(),
            })
        );
        let ps = ps.unwrap();
        assert_eq!(ps.to_uri(), "cairn://p/CAIRN/settings");
        assert_eq!(ps.kind(), ResourceKind::ProjectSettings);
        assert_eq!(ps.project(), Some("CAIRN"));
        assert_eq!(ps.issue_number(), None);
        assert_eq!(ps.to_route(), None);

        // `settings` is not parsed as an issue number.
        assert_eq!(
            parse_uri("cairn://p/CAIRN/settings").map(|r| r.kind()),
            Some(ResourceKind::ProjectSettings)
        );
    }

    #[test]
    fn parses_and_roundtrips_websearch() {
        assert_eq!(parse_uri("cairn://websearch"), Some(CairnResource::WebSearch));
        assert_eq!(CairnResource::WebSearch.to_uri(), "cairn://websearch");
        assert_eq!(CairnResource::WebSearch.kind(), ResourceKind::WebSearch);
        assert_eq!(CairnResource::WebSearch.project(), None);
        assert_eq!(CairnResource::WebSearch.issue_number(), None);
        // The query rides in ?q=; parse_uri ignores the query like every resource.
        assert_eq!(
            parse_uri("cairn://websearch?q=rust async"),
            Some(CairnResource::WebSearch)
        );
    }

    #[test]
    fn parses_and_roundtrips_help() {
        assert_eq!(parse_uri("cairn://help"), Some(CairnResource::Help));
        assert_eq!(CairnResource::Help.to_uri(), "cairn://help");
        assert_eq!(CairnResource::Help.kind(), ResourceKind::Help);
        assert_eq!(CairnResource::Help.project(), None);
    }

    #[test]
    fn parses_and_roundtrips_logs() {
        assert_eq!(parse_uri("cairn://logs"), Some(CairnResource::Logs));
        assert_eq!(CairnResource::Logs.to_uri(), "cairn://logs");
        assert_eq!(CairnResource::Logs.kind(), ResourceKind::Logs);
        assert_eq!(CairnResource::Logs.project(), None);
        // Read-only logical resource: not a UI deep-link.
        assert_eq!(CairnResource::Logs.to_route(), None);
        // Query strings do not affect resource identity.
        assert_eq!(
            parse_uri("cairn://logs?process=mcp&grep=ERROR"),
            Some(CairnResource::Logs)
        );
    }

    #[test]
    fn parses_type_named_node_artifact() {
        // A trailing non-reserved segment is a type-named artifact.
        assert_eq!(
            parse_uri("cairn://p/CAIRN/42/2/builder/plan"),
            Some(CairnResource::NodeArtifact {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
                name: Some("plan".to_string()),
            })
        );
        // Round-trips back to the same type-named URI.
        assert_eq!(
            CairnResource::NodeArtifact {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
                name: Some("plan".to_string()),
            }
            .to_uri(),
            "cairn://p/CAIRN/42/2/builder/plan"
        );
        // The literal `artifact` keyword is the generic (name: None) alias.
        assert_eq!(
            parse_uri("cairn://p/CAIRN/42/2/builder/artifact"),
            Some(CairnResource::NodeArtifact {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
                name: None,
            })
        );
    }

    #[test]
    fn reserved_segments_are_not_artifacts() {
        // `chat` is reserved and must parse as NodeChat, never an artifact named "chat".
        assert_eq!(
            parse_uri("cairn://p/CAIRN/42/2/builder/chat"),
            Some(CairnResource::NodeChat {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
            })
        );
        assert!(is_reserved_node_segment("chat"));
        assert!(is_reserved_node_segment("todos"));
        assert!(!is_reserved_node_segment("plan"));
        assert!(!is_reserved_node_segment("pr"));
    }

    #[test]
    fn parses_type_named_task_artifact() {
        assert_eq!(
            parse_uri("cairn://p/CAIRN/42/2/builder/task/Explore/result"),
            Some(CairnResource::TaskArtifact {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: "Explore".to_string(),
                name: Some("result".to_string()),
            })
        );
    }

    #[test]
    fn parses_and_roundtrips_task_base() {
        // The task job base — the analogue of a node base. Regression: this used
        // to fall through to `None`, so a sub-agent's home URI was rejected and
        // `cairn:~/...` shorthand could not resolve.
        let uri = "cairn://p/CAIRN/1174/1/planner/task/cairn-1171";
        let parsed = parse_uri(uri);
        assert_eq!(
            parsed,
            Some(CairnResource::Task {
                project: "CAIRN".to_string(),
                number: 1174,
                exec_seq: 1,
                node_id: "planner".to_string(),
                task_name: "cairn-1171".to_string(),
            })
        );
        // Round-trips back to the same string (distinct from the artifact form).
        assert_eq!(parsed.unwrap().to_uri(), uri);
    }

    #[test]
    fn task_base_built_by_build_job_base_uri_parses() {
        // The exact path the orchestrator uses to stamp CAIRN_HOME_URI for a task.
        let built = build_job_base_uri("CAIRN", 1174, 1, "cairn-1171", Some("planner"));
        assert!(
            parse_uri(&built).is_some(),
            "task home URI must parse: {built}"
        );
    }

    #[test]
    fn chat_full_uri_no_longer_parses() {
        // The `full` segment was renamed to `raw`; the old spelling must 404 so
        // the removal is deliberate rather than a silent dual-name.
        assert!(parse_uri("cairn://p/CAIRN/42/2/builder/chat/full").is_none());
        assert!(parse_uri("cairn://p/CAIRN/42/2/builder/task/Explore/chat/full").is_none());
    }

    #[test]
    fn parses_node_and_task_resources() {
        assert_eq!(
            parse_uri("cairn://p/CAIRN/42/2/builder/chat/raw"),
            Some(CairnResource::NodeChatRaw {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
            })
        );
        assert_eq!(
            parse_uri("cairn://p/CAIRN/42/2/builder/task/Explore/chat/turn/3"),
            Some(CairnResource::TaskChatTurn {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: "Explore".to_string(),
                turn_seq: 3,
            })
        );
        assert_eq!(
            parse_uri("cairn://p/CAIRN/42/2/builder/chat/1/0"),
            Some(CairnResource::NodeChatEvent {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
                run_seq: 1,
                event_seq: 0,
            })
        );
        assert_eq!(
            parse_uri("cairn://p/CAIRN/42/2/builder/task/Explore/chat/1/0"),
            Some(CairnResource::TaskChatEvent {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: "Explore".to_string(),
                run_seq: 1,
                event_seq: 0,
            })
        );
    }

    #[test]
    fn parses_and_roundtrips_node_tasks_and_questions() {
        let cases = [
            (
                "cairn://p/CAIRN/42/2/builder/tasks",
                CairnResource::NodeTasks {
                    project: "CAIRN".to_string(),
                    number: 42,
                    exec_seq: 2,
                    node_id: "builder".to_string(),
                },
            ),
            (
                "cairn://p/CAIRN/42/2/builder/questions",
                CairnResource::NodeQuestions {
                    project: "CAIRN".to_string(),
                    number: 42,
                    exec_seq: 2,
                    node_id: "builder".to_string(),
                },
            ),
            (
                "cairn://p/CAIRN/42/2/builder/questions/q-1",
                CairnResource::NodeQuestion {
                    project: "CAIRN".to_string(),
                    number: 42,
                    exec_seq: 2,
                    node_id: "builder".to_string(),
                    segment: "q-1".to_string(),
                },
            ),
            (
                "cairn://p/CAIRN/42/2/builder/permissions",
                CairnResource::NodePermissions {
                    project: "CAIRN".to_string(),
                    number: 42,
                    exec_seq: 2,
                    node_id: "builder".to_string(),
                },
            ),
            (
                "cairn://p/CAIRN/42/2/builder/permissions/perm-1",
                CairnResource::NodePermission {
                    project: "CAIRN".to_string(),
                    number: 42,
                    exec_seq: 2,
                    node_id: "builder".to_string(),
                    segment: "perm-1".to_string(),
                },
            ),
        ];
        for (uri, expected) in cases {
            assert_eq!(parse_uri(uri), Some(expected.clone()));
            assert_eq!(expected.to_uri(), uri);
        }
    }

    #[test]
    fn parse_uri_keeps_path_only_compatibility_with_queries() {
        assert_eq!(
            parse_uri("cairn://p/CAIRN/42/2/builder/terminal/dev?full=true"),
            Some(CairnResource::NodeTerminal {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
                slug: "dev".to_string(),
            })
        );
        assert_eq!(
            parse_uri("cairn://p/CAIRN/42/2/builder/task/Explore/terminal/ci?new=true"),
            Some(CairnResource::TaskTerminal {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: "Explore".to_string(),
                slug: "ci".to_string(),
            })
        );
    }

    #[test]
    fn parses_and_roundtrips_task_terminal() {
        let uri = "cairn://p/CAIRN/42/2/builder/task/Explore/terminal/ci";
        let parsed = parse_uri(uri);
        assert_eq!(
            parsed,
            Some(CairnResource::TaskTerminal {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: "Explore".to_string(),
                slug: "ci".to_string(),
            })
        );
        let resource = parsed.unwrap();
        assert_eq!(resource.to_uri(), uri);
        assert_eq!(resource.kind(), ResourceKind::TaskTerminal);
        assert_eq!(resource.project(), Some("CAIRN"));
        assert_eq!(
            resource.to_route(),
            Some("/p/cairn/i/42/2/builder/task/Explore?terminalId=ci".to_string())
        );
    }

    #[test]
    fn parse_resource_uri_preserves_ordered_query_params() {
        let parsed = parse_resource_uri("cairn://p/cairn/issues?limit=10&status=backlog").unwrap();
        assert_eq!(
            parsed,
            Some(CairnResourceUri {
                resource: CairnResource::ProjectIssues {
                    project: "CAIRN".to_string(),
                },
                params: vec![
                    QueryParam {
                        key: "limit".to_string(),
                        value: "10".to_string(),
                    },
                    QueryParam {
                        key: "status".to_string(),
                        value: "backlog".to_string(),
                    },
                ],
            })
        );
        assert_eq!(
            parsed.unwrap().to_uri(),
            "cairn://p/CAIRN/issues?limit=10&status=backlog"
        );
    }

    #[test]
    fn parse_resource_uri_encodes_canonical_query_params() {
        // `+` is literal in a value (not form-decoded to a space), so it
        // canonicalizes to `%2B`; a space encodes as `%20`. `&status=` still
        // splits because `status` is a recognized key.
        let parsed =
            parse_resource_uri("cairn://p/cairn/issues?label=needs+review&status=backlog%2Cactive")
                .unwrap()
                .unwrap();
        assert_eq!(
            parsed.to_uri(),
            "cairn://p/CAIRN/issues?label=needs%2Breview&status=backlog%2Cactive"
        );
    }

    #[test]
    fn parse_resource_uri_rejects_invalid_query_encoding() {
        let err = parse_resource_uri("cairn://p/CAIRN/issues?status=%ZZ").unwrap_err();
        assert!(err.contains("Invalid percent escape"));
    }

    #[test]
    fn rejects_legacy_roots_and_invalid_paths() {
        assert!(parse_uri("cairn://CAIRN/42").is_none());
        assert!(parse_uri("cairn://ws/skills").is_none());
        assert!(parse_uri("cairn://p").is_none());
        assert!(parse_uri("cairn://p/CAIRN/comments").is_none());
        assert!(parse_uri("cairn://p/CAIRN/42/pr").is_none());
        // Note: a trailing non-reserved segment like `.../builder/diff` is now a
        // valid type-named artifact (see parses_type_named_node_artifact), not an error.
        assert!(parse_uri("cairn://p/CAIRN/42/0/builder").is_none());
        assert!(parse_uri("cairn://").is_none());
    }

    #[test]
    fn serializes_canonical_uris() {
        assert_eq!(build_project_uri("cairn"), "cairn://p/CAIRN");
        assert_eq!(build_project_issues_uri("cairn"), "cairn://p/CAIRN/issues");
        assert_eq!(build_issue_uri("cairn", 42), "cairn://p/CAIRN/42");
        assert_eq!(
            build_node_terminal_uri("cairn", 42, 2, "builder", "dev"),
            "cairn://p/CAIRN/42/2/builder/terminal/dev"
        );
        assert_eq!(
            build_task_terminal_uri("cairn", 42, 2, "builder", "Explore", "ci"),
            "cairn://p/CAIRN/42/2/builder/task/Explore/terminal/ci"
        );
        assert_eq!(
            build_task_artifact_uri("cairn", 42, 2, "builder", "Explore"),
            "cairn://p/CAIRN/42/2/builder/task/Explore/artifact"
        );
    }

    #[test]
    fn parses_issue_executions_collection() {
        assert_eq!(
            parse_uri("cairn://p/cairn/42/executions"),
            Some(CairnResource::IssueExecutions {
                project: "CAIRN".to_string(),
                number: 42,
            })
        );
        assert_eq!(
            build_issue_executions_uri("CAIRN", 42),
            "cairn://p/CAIRN/42/executions"
        );
    }

    #[test]
    fn parses_issue_comments_collection_and_member() {
        assert_eq!(
            parse_uri("cairn://p/cairn/12/comments"),
            Some(CairnResource::IssueComments {
                project: "CAIRN".to_string(),
                number: 12,
            })
        );
        assert_eq!(
            build_issue_comments_uri("CAIRN", 12),
            "cairn://p/CAIRN/12/comments"
        );

        let member = parse_uri("cairn://p/cairn/12/comments/3").unwrap();
        assert_eq!(
            member,
            CairnResource::IssueComment {
                project: "CAIRN".to_string(),
                number: 12,
                comment_seq: 3,
            }
        );
        assert_eq!(member.kind(), ResourceKind::IssueComment);
        assert_eq!(member.project(), Some("CAIRN"));
        assert_eq!(member.issue_number(), Some(12));
        assert_eq!(member.to_uri(), "cairn://p/CAIRN/12/comments/3");
        // A non-numeric comment tail is not a valid member URI.
        assert_eq!(parse_uri("cairn://p/CAIRN/12/comments/not-a-number"), None);

        let collection = parse_uri("cairn://p/CAIRN/12/comments").unwrap();
        assert_eq!(collection.kind(), ResourceKind::IssueComments);
        assert_eq!(collection.issue_number(), Some(12));
    }

    #[test]
    fn parses_single_execution_snapshot() {
        let resource = parse_uri("cairn://p/cairn/42/executions/2").unwrap();
        assert_eq!(
            resource,
            CairnResource::IssueExecution {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
            }
        );
        assert_eq!(resource.kind(), ResourceKind::IssueExecution);
        assert_eq!(resource.project(), Some("CAIRN"));
        assert_eq!(resource.issue_number(), Some(42));
        assert_eq!(resource.to_route(), None);
        assert_eq!(
            build_issue_execution_uri("CAIRN", 42, 2),
            "cairn://p/CAIRN/42/executions/2"
        );
        assert_eq!(resource.to_uri(), "cairn://p/CAIRN/42/executions/2");
    }

    /// `.../42/executions/2` and the node shape `.../42/2/builder` are both
    /// 5-segment URIs; the literal `executions` in the 4th slot must resolve to
    /// a single execution, never a node whose exec_seq parsed there.
    #[test]
    fn single_execution_does_not_shadow_node() {
        assert_eq!(
            parse_uri("cairn://p/cairn/42/executions/2"),
            Some(CairnResource::IssueExecution {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
            })
        );
        assert_eq!(
            parse_uri("cairn://p/cairn/42/2/builder"),
            Some(CairnResource::Node {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
            })
        );
        // A non-numeric exec_seq under `executions` is malformed, not a node.
        assert_eq!(parse_uri("cairn://p/cairn/42/executions/abc"), None);
    }

    #[test]
    fn round_trips_every_resource_family() {
        let resources = vec![
            CairnResource::Project {
                project: "CAIRN".to_string(),
            },
            CairnResource::ProjectIssues {
                project: "CAIRN".to_string(),
            },
            CairnResource::Issue {
                project: "CAIRN".to_string(),
                number: 1,
            },
            CairnResource::ProjectMessages {
                project: "CAIRN".to_string(),
            },
            CairnResource::ProjectTerminal {
                project: "CAIRN".to_string(),
                slug: "build".to_string(),
            },
            CairnResource::IssueMessages {
                project: "CAIRN".to_string(),
                number: 1,
            },
            CairnResource::Changed {
                project: "CAIRN".to_string(),
                number: 1,
            },
            CairnResource::IssueExecutions {
                project: "CAIRN".to_string(),
                number: 1,
            },
            CairnResource::IssueComments {
                project: "CAIRN".to_string(),
                number: 1,
            },
            CairnResource::IssueComment {
                project: "CAIRN".to_string(),
                number: 1,
                comment_seq: 1,
            },
            CairnResource::IssueExecution {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
            },
            CairnResource::Node {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
            },
            CairnResource::NodeChat {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
            },
            CairnResource::NodeChatRaw {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
            },
            CairnResource::NodeChatTurn {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
                turn_seq: 0,
            },
            CairnResource::NodeChatEvent {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
                run_seq: 1,
                event_seq: 5,
            },
            CairnResource::NodeArtifact {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
                name: None,
            },
            CairnResource::NodeChanged {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
            },
            CairnResource::NodeTerminal {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
                slug: "dev".to_string(),
            },
            CairnResource::TaskTerminal {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: "Explore".to_string(),
                slug: "ci".to_string(),
            },
            CairnResource::TaskChat {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: "Explore".to_string(),
            },
            CairnResource::TaskChatRaw {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: "Explore".to_string(),
            },
            CairnResource::TaskChatTurn {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: "Explore".to_string(),
                turn_seq: 2,
            },
            CairnResource::TaskChatEvent {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: "Explore".to_string(),
                run_seq: 1,
                event_seq: 3,
            },
            CairnResource::TaskArtifact {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: "Explore".to_string(),
                name: None,
            },
            CairnResource::JobTodos {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: None,
            },
            CairnResource::JobTodos {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: Some("Explore".to_string()),
            },
            CairnResource::Bug,
        ];

        for resource in resources {
            assert_eq!(parse_uri(&resource.to_uri()), Some(resource.clone()));
        }
    }

    #[test]
    fn parses_job_todos_node_and_task_forms() {
        assert_eq!(
            parse_uri("cairn://p/cairn/42/2/builder/todos"),
            Some(CairnResource::JobTodos {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: None,
            })
        );
        assert_eq!(
            parse_uri("cairn://p/CAIRN/42/2/builder/task/Explore/todos"),
            Some(CairnResource::JobTodos {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: Some("Explore".to_string()),
            })
        );
        assert_eq!(
            build_job_todos_uri("cairn", 42, 2, "builder", None),
            "cairn://p/CAIRN/42/2/builder/todos"
        );
        assert_eq!(
            build_job_todos_uri("cairn", 42, 2, "builder", Some("Explore")),
            "cairn://p/CAIRN/42/2/builder/task/Explore/todos"
        );
    }

    #[test]
    fn job_todos_uri_keeps_path_only_compatibility_with_queries() {
        // parse_uri strips the query; query rejection is enforced at the handler.
        assert_eq!(
            parse_uri("cairn://p/CAIRN/42/2/builder/todos?limit=3"),
            Some(CairnResource::JobTodos {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
                task_name: None,
            })
        );
    }

    #[test]
    fn parses_bug_resource() {
        assert_eq!(parse_uri("cairn://bug"), Some(CairnResource::Bug));
        assert_eq!(build_bug_uri(), "cairn://bug");
        assert_eq!(CairnResource::Bug.project(), None);
        assert_eq!(CairnResource::Bug.to_route(), None);
    }

    #[test]
    fn parses_and_round_trips_mcp_resources() {
        // Top-level: list servers.
        assert_eq!(
            parse_uri("cairn://mcp"),
            Some(CairnResource::Mcp {
                server: None,
                resource: None,
            })
        );
        // Server scope: list tools/resources.
        assert_eq!(
            parse_uri("cairn://mcp/playwright"),
            Some(CairnResource::Mcp {
                server: Some("playwright".to_string()),
                resource: None,
            })
        );
        // Tool target (for run).
        assert_eq!(
            parse_uri("cairn://mcp/playwright/browser_navigate"),
            Some(CairnResource::Mcp {
                server: Some("playwright".to_string()),
                resource: Some("browser_navigate".to_string()),
            })
        );
        // External resource URI tail kept intact, including '/' and '://'.
        let r = parse_uri("cairn://mcp/linear/issue://ABC-1/sub").unwrap();
        assert_eq!(
            r,
            CairnResource::Mcp {
                server: Some("linear".to_string()),
                resource: Some("issue://ABC-1/sub".to_string()),
            }
        );
        assert_eq!(r.to_uri(), "cairn://mcp/linear/issue://ABC-1/sub");
        // The external resource tail may carry its own '?query', which must NOT
        // be consumed as Cairn-side query params.
        let q = parse_uri("cairn://mcp/linear/https://api.example.com/items?limit=10").unwrap();
        assert_eq!(
            q,
            CairnResource::Mcp {
                server: Some("linear".to_string()),
                resource: Some("https://api.example.com/items?limit=10".to_string()),
            }
        );
        assert_eq!(
            q.to_uri(),
            "cairn://mcp/linear/https://api.example.com/items?limit=10"
        );
        // Round-trip the simpler forms.
        for uri in [
            "cairn://mcp",
            "cairn://mcp/playwright",
            "cairn://mcp/playwright/browser_navigate",
        ] {
            assert_eq!(parse_uri(uri).unwrap().to_uri(), uri);
        }
        assert_eq!(parse_uri("cairn://mcp").unwrap().project(), None);
        assert_eq!(parse_uri("cairn://mcp").unwrap().kind(), ResourceKind::Mcp);
    }

    #[test]
    fn parses_skill_resources() {
        assert_eq!(parse_uri("cairn://skills"), Some(CairnResource::Skills));
        assert_eq!(
            parse_uri("cairn://skills/ui"),
            Some(CairnResource::Skill {
                skill_id: "ui".to_string(),
                path: vec![],
            })
        );
        assert_eq!(
            parse_uri("cairn://skills/ui/SKILL.md"),
            Some(CairnResource::Skill {
                skill_id: "ui".to_string(),
                path: vec!["SKILL.md".to_string()],
            })
        );
        assert_eq!(
            parse_uri("cairn://skills/ui/references/a/b.md"),
            Some(CairnResource::Skill {
                skill_id: "ui".to_string(),
                path: vec![
                    "references".to_string(),
                    "a".to_string(),
                    "b.md".to_string()
                ],
            })
        );
        assert_eq!(
            parse_uri("cairn://p/cairn/skills"),
            Some(CairnResource::ProjectSkills {
                project: "CAIRN".to_string(),
            })
        );
        assert_eq!(
            parse_uri("cairn://p/cairn/skills/ui/scripts/run.sh"),
            Some(CairnResource::ProjectSkill {
                project: "CAIRN".to_string(),
                skill_id: "ui".to_string(),
                path: vec!["scripts".to_string(), "run.sh".to_string()],
            })
        );
    }

    #[test]
    fn round_trips_skill_resources() {
        let resources = vec![
            CairnResource::Skills,
            CairnResource::Skill {
                skill_id: "ui".to_string(),
                path: vec![],
            },
            CairnResource::Skill {
                skill_id: "ui".to_string(),
                path: vec!["SKILL.md".to_string()],
            },
            CairnResource::Skill {
                skill_id: "ui".to_string(),
                path: vec![
                    "references".to_string(),
                    "a".to_string(),
                    "b.md".to_string(),
                ],
            },
            CairnResource::ProjectSkills {
                project: "CAIRN".to_string(),
            },
            CairnResource::ProjectSkill {
                project: "CAIRN".to_string(),
                skill_id: "ui".to_string(),
                path: vec!["scripts".to_string(), "run.sh".to_string()],
            },
        ];
        for resource in resources {
            assert_eq!(parse_uri(&resource.to_uri()), Some(resource.clone()));
        }
    }

    #[test]
    fn parses_only_node_memory_resources() {
        assert_eq!(parse_uri("cairn://memories"), None);
        assert_eq!(parse_uri("cairn://memories/abc-123"), None);
        assert_eq!(parse_uri("cairn://p/CAIRN/memories"), None);
        assert_eq!(parse_uri("cairn://p/CAIRN/memories/abc-123"), None);

        let resource = CairnResource::NodeMemory {
            project: "CAIRN".to_string(),
            number: 1498,
            exec_seq: 1,
            node_id: "builder".to_string(),
            memory_seq: 2,
        };
        let uri = "cairn://p/CAIRN/1498/1/builder/memories/2";
        assert_eq!(parse_uri(uri), Some(resource.clone()));
        assert_eq!(resource.to_uri(), uri);
        assert_eq!(resource.project(), Some("CAIRN"));
        assert_eq!(
            resource.to_route(),
            Some("/p/cairn/i/1498/1/builder/memories/2".to_string())
        );
    }

    #[test]
    fn parses_and_round_trips_recipe_resources() {
        let cases = [
            ("cairn://recipes", CairnResource::Recipes),
            (
                "cairn://recipes/default-flow",
                CairnResource::Recipe {
                    recipe_id: "default-flow".to_string(),
                },
            ),
            (
                "cairn://p/CAIRN/recipes",
                CairnResource::ProjectRecipes {
                    project: "CAIRN".to_string(),
                },
            ),
            (
                "cairn://p/CAIRN/recipes/default-flow",
                CairnResource::ProjectRecipe {
                    project: "CAIRN".to_string(),
                    recipe_id: "default-flow".to_string(),
                },
            ),
        ];
        for (uri, expected) in cases {
            assert_eq!(parse_uri(uri), Some(expected.clone()));
            assert_eq!(expected.to_uri(), uri);
            assert_eq!(expected.kind(), expected.clone().kind());
        }
        // Project canonicalization on parse.
        assert_eq!(
            parse_uri("cairn://p/cairn/recipes"),
            Some(CairnResource::ProjectRecipes {
                project: "CAIRN".to_string(),
            })
        );
        assert_eq!(
            parse_uri("cairn://p/cairn/recipes/default-flow"),
            Some(CairnResource::ProjectRecipe {
                project: "CAIRN".to_string(),
                recipe_id: "default-flow".to_string(),
            })
        );
        assert_eq!(CairnResource::Recipes.project(), None);
        assert_eq!(CairnResource::Recipes.issue_number(), None);
        assert_eq!(
            CairnResource::ProjectRecipe {
                project: "CAIRN".to_string(),
                recipe_id: "x".to_string(),
            }
            .project(),
            Some("CAIRN")
        );
        assert_eq!(CairnResource::Recipes.to_route(), None);
        assert_eq!(CairnResource::Recipes.kind(), ResourceKind::Recipes);
    }

    #[test]
    fn skill_resources_have_no_project_or_route() {
        assert_eq!(CairnResource::Skills.project(), None);
        assert_eq!(CairnResource::Skills.to_route(), None);
        assert_eq!(
            CairnResource::ProjectSkills {
                project: "CAIRN".to_string(),
            }
            .project(),
            Some("CAIRN")
        );
        assert_eq!(
            CairnResource::ProjectSkill {
                project: "CAIRN".to_string(),
                skill_id: "ui".to_string(),
                path: vec![],
            }
            .to_route(),
            None
        );
    }

    #[test]
    fn resource_contracts_include_project_issue_collection() {
        use crate::contract::RESOURCE_CONTRACTS;
        assert!(RESOURCE_CONTRACTS
            .iter()
            .any(|contract| contract.uri_template == "cairn://p/{project}/issues"));
    }

    #[test]
    fn every_resource_kind_round_trips_through_kind() {
        use crate::contract::ResourceKind;
        // kind() must agree with the table: every kind a resource reports has a contract.
        for resource in [
            CairnResource::Project {
                project: "CAIRN".to_string(),
            },
            CairnResource::Bug,
            CairnResource::Skills,
        ] {
            assert!(crate::contract::contract_for(resource.kind()).is_some());
        }
        assert_eq!(
            CairnResource::Issue {
                project: "CAIRN".to_string(),
                number: 1,
            }
            .kind(),
            ResourceKind::Issue
        );
    }

    #[test]
    fn routes_only_navigate_supported_resources() {
        assert_eq!(
            CairnResource::Project {
                project: "CAIRN".to_string(),
            }
            .to_route(),
            Some("/p/cairn/issues".to_string())
        );
        assert_eq!(
            CairnResource::ProjectIssues {
                project: "CAIRN".to_string(),
            }
            .to_route(),
            None
        );
        assert_eq!(
            CairnResource::ProjectTerminal {
                project: "CAIRN".to_string(),
                slug: "build".to_string(),
            }
            .to_route(),
            Some("/p/cairn/terminal?terminalId=build".to_string())
        );
        assert_eq!(
            CairnResource::NodeChatRaw {
                project: "CAIRN".to_string(),
                number: 1,
                exec_seq: 2,
                node_id: "builder".to_string(),
            }
            .to_route(),
            None
        );
    }
}
