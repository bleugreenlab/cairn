//! `build_*` URI constructors and the `CairnResource::to_uri` serializer.

use super::types::{CairnResource, PROJECT_SCOPE};

pub(super) fn canonical_project(project: &str) -> String {
    project.to_uppercase()
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

pub fn build_node_progress_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "progress")
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

pub fn build_node_calls_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "calls")
}

pub fn build_node_wakes_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "wakes")
}

pub fn build_node_checks_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "checks")
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

pub fn build_task_permissions_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: &str,
) -> String {
    build_task_subresource_uri(project, number, exec_seq, node_id, task_name, "permissions")
}

pub fn build_task_permission_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: &str,
    segment: &str,
) -> String {
    format!(
        "{}/{}",
        build_task_subresource_uri(project, number, exec_seq, node_id, task_name, "permissions"),
        segment
    )
}

pub fn build_project_terminal_uri(project: &str, slug: &str) -> String {
    format!("{}/terminal/{}", build_project_uri(project), slug)
}

pub fn build_node_browser_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    slug: &str,
) -> String {
    build_node_segmented_resource_uri(project, number, exec_seq, node_id, "browser", slug)
}

pub fn build_task_browser_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: &str,
    slug: &str,
) -> String {
    format!(
        "{}/{}",
        build_task_subresource_uri(project, number, exec_seq, node_id, task_name, "browser"),
        slug
    )
}

pub fn build_project_browser_uri(project: &str, slug: &str) -> String {
    format!("{}/browser/{}", build_project_uri(project), slug)
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

pub fn build_project_references_uri(project: &str) -> String {
    format!("{}/references", build_project_uri(project))
}

pub fn build_project_reference_uri(project: &str, name: &str) -> String {
    format!("{}/references/{}", build_project_uri(project), name)
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

pub fn build_workflows_uri() -> String {
    "cairn://workflows".to_string()
}

pub fn build_workflow_uri(workflow_id: &str) -> String {
    format!("cairn://workflows/{}", workflow_id)
}

pub fn build_project_workflows_uri(project: &str) -> String {
    format!("{}/workflows", build_project_uri(project))
}

pub fn build_project_workflow_uri(project: &str, workflow_id: &str) -> String {
    format!("{}/workflows/{}", build_project_uri(project), workflow_id)
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
            Self::NodeBrowser {
                project,
                number,
                exec_seq,
                node_id,
                slug,
            } => build_node_browser_uri(project, *number, *exec_seq, node_id, slug),
            Self::TaskBrowser {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                slug,
            } => build_task_browser_uri(project, *number, *exec_seq, node_id, task_name, slug),
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
            Self::NodeCalls {
                project,
                number,
                exec_seq,
                node_id,
            } => build_node_calls_uri(project, *number, *exec_seq, node_id),
            Self::NodeWakes {
                project,
                number,
                exec_seq,
                node_id,
            } => build_node_wakes_uri(project, *number, *exec_seq, node_id),
            Self::NodeChecks {
                project,
                number,
                exec_seq,
                node_id,
            } => build_node_checks_uri(project, *number, *exec_seq, node_id),
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
            Self::TaskPermissions {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => build_task_permissions_uri(project, *number, *exec_seq, node_id, task_name),
            Self::TaskPermission {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                segment,
            } => {
                build_task_permission_uri(project, *number, *exec_seq, node_id, task_name, segment)
            }
            Self::NodeMessages {
                project,
                number,
                exec_seq,
                node_id,
            } => build_node_messages_uri(project, *number, *exec_seq, node_id),
            Self::NodeProgress {
                project,
                number,
                exec_seq,
                node_id,
            } => build_node_progress_uri(project, *number, *exec_seq, node_id),
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
            Self::ProjectBrowser { project, slug } => build_project_browser_uri(project, slug),
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
            Self::Dev => "cairn://dev".to_string(),
            Self::DevDb => "cairn://dev/db".to_string(),
            Self::DevPid => "cairn://dev/pid".to_string(),
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
            Self::ProjectReferences { project } => build_project_references_uri(project),
            Self::ProjectReference { project, name } => build_project_reference_uri(project, name),
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
            Self::Workflows => build_workflows_uri(),
            Self::Workflow { workflow_id } => build_workflow_uri(workflow_id),
            Self::ProjectWorkflows { project } => build_project_workflows_uri(project),
            Self::ProjectWorkflow {
                project,
                workflow_id,
            } => build_project_workflow_uri(project, workflow_id),
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
}
