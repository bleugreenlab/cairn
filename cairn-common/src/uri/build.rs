//! `build_*` URI constructors and the `CairnResource::to_uri` serializer.

use super::types::{CairnResource, ImageRef, PROJECT_SCOPE};

pub fn canonical_project(project: impl AsRef<str>) -> String {
    project.as_ref().to_lowercase()
}

pub fn build_project_check_observation_uri(project: &str, handle: &str) -> String {
    format!("{}/check-observations/{handle}", build_project_uri(project))
}

pub fn build_project_uri(project: &str) -> String {
    format!("cairn://{}/{}", PROJECT_SCOPE, canonical_project(project))
}

/// The public URI for a stored image, in whichever form addresses it.
///
/// The friendly forms carry their own scope, so they render as themselves
/// everywhere; only the legacy hash form needs a display substitute.
pub fn build_project_image_uri(project: &str, reference: &ImageRef) -> String {
    let base = build_project_uri(project);
    match reference {
        ImageRef::Issue { number, ordinal } => format!("{base}/{number}/images/{ordinal}"),
        ImageRef::Project { ordinal } => format!("{base}/images/{ordinal}"),
        ImageRef::Hash(hash) => format!("{base}/images/{hash}"),
    }
}

/// The collection of images minted in one scope. Its members are exactly
/// `‹this›/1..N`, so an agent can construct a member address from the collection
/// and vice versa without a lookup.
pub fn build_project_images_uri(project: &str, issue: Option<i32>) -> String {
    let base = build_project_uri(project);
    match issue {
        Some(number) => format!("{base}/{number}/images"),
        None => format!("{base}/images"),
    }
}

/// The friendly URI for an image minted inside an issue's world.
pub fn build_issue_image_uri(project: &str, number: i32, ordinal: i32) -> String {
    build_project_image_uri(project, &ImageRef::Issue { number, ordinal })
}

/// The friendly URI for an image minted with no issue context.
pub fn build_project_image_ordinal_uri(project: &str, ordinal: i32) -> String {
    build_project_image_uri(project, &ImageRef::Project { ordinal })
}

/// The permalink URI for a stored image addressed by its content hash. Only
/// immutable history writes this form; nothing mints it.
pub fn build_project_image_hash_uri(project: &str, hash: &str) -> String {
    build_project_image_uri(project, &ImageRef::Hash(hash.to_string()))
}

pub fn build_project_check_results_uri(project: &str, revision: &str) -> String {
    format!("{}/check-results/{}", build_project_uri(project), revision)
}

pub fn build_project_threads_uri(project: &str) -> String {
    format!("{}/threads", build_project_uri(project))
}

pub fn build_thread_uri(project: &str, name: &str) -> String {
    format!("{}/{}", build_project_uri(project), name)
}

/// The canonical home URI for a sub-agent task a thread's session spawned.
///
/// A thread addresses its descendants the way an issue node addresses its own:
/// the task hangs beneath its parent as `/task/{segment}`, on the thread address
/// rather than an issue coordinate. This is the form the thread read surface
/// already resolves, by the task's `parent_job_id` and `uri_segment`.
pub fn build_thread_task_uri(project: &str, name: &str, task_segment: &str) -> String {
    format!("{}/task/{}", build_thread_uri(project, name), task_segment)
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

pub(crate) fn build_issue_comments_uri(project: &str, number: i32) -> String {
    format!("{}/comments", build_issue_uri(project, number))
}

pub fn build_issue_comment_uri(project: &str, number: i32, comment_seq: i32) -> String {
    format!(
        "{}/{}",
        build_issue_comments_uri(project, number),
        comment_seq
    )
}

fn build_node_messages_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "messages")
}

fn build_node_progress_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "progress")
}

fn build_task_messages_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: &str,
) -> String {
    build_task_subresource_uri(project, number, exec_seq, node_id, task_name, "messages")
}

fn build_issue_changed_uri(project: &str, number: i32) -> String {
    format!("{}/changed", build_issue_uri(project, number))
}

pub fn build_issue_executions_uri(project: &str, number: i32) -> String {
    format!("{}/executions", build_issue_uri(project, number))
}

pub(crate) fn build_issue_execution_uri(project: &str, number: i32, exec_seq: i32) -> String {
    format!(
        "{}/{}",
        build_issue_executions_uri(project, number),
        exec_seq
    )
}

/// Who owns the node coordinate a URI is being built from.
///
/// An execution node is addressed by `{issue}/{exec_seq}/{segment}`. A thread's
/// session job has neither an issue nor an execution, yet it owns the same
/// job-scoped resource families a node does — todos, tasks, terminals, wakes,
/// artifacts. Those families therefore carry the reserved `(0, 0, thread-name)`
/// coordinate, which [`NodeAddress::new`] is the one place that recognizes.
///
/// The sentinel is safe because it is unspellable: every numeric segment of a
/// parsed node URI goes through `parse_positive_i32`, so no URI a caller can
/// write ever produces `(0, 0)`. `thread_coordinate_is_unspellable` pins that.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeAddress<'a> {
    Node {
        number: i32,
        exec_seq: i32,
        node_id: &'a str,
    },
    Thread {
        name: &'a str,
    },
}

impl<'a> NodeAddress<'a> {
    /// Read a raw node coordinate as the address it actually names.
    ///
    /// This is the single interpretation of the reserved coordinate. Every node
    /// and task sub-resource URI composes from [`build_node_uri`], which routes
    /// through here, so a thread-owned resource renders at its thread address
    /// rather than leaking `cairn://p/PROJECT/0/0/thread-name/...`.
    pub fn new(number: i32, exec_seq: i32, node_id: &'a str) -> Self {
        if number == 0 && exec_seq == 0 {
            return Self::Thread { name: node_id };
        }
        Self::Node {
            number,
            exec_seq,
            node_id,
        }
    }

    /// The canonical base URI this address renders as.
    pub fn render(&self, project: &str) -> String {
        match self {
            Self::Node {
                number,
                exec_seq,
                node_id,
            } => format!(
                "{}/{}/{}",
                build_issue_uri(project, *number),
                exec_seq,
                node_id
            ),
            Self::Thread { name } => build_thread_uri(project, name),
        }
    }
}

pub fn build_node_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    NodeAddress::new(number, exec_seq, node_id).render(project)
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

/// Build a node transcript TURN URI (`.../{node}/chat/turn/{n}`). `turn_seq` is
/// `turns.sequence` within the node's primary session — the only session the
/// turn coordinate addresses.
pub fn build_node_chat_turn_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    turn_seq: i32,
) -> String {
    format!(
        "{}/turn/{}",
        build_node_chat_uri(project, number, exec_seq, node_id),
        turn_seq
    )
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

fn build_node_diff_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "diff")
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

fn build_node_repl_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    slug: &str,
) -> String {
    build_node_segmented_resource_uri(project, number, exec_seq, node_id, "repl", slug)
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

/// Build a task transcript TURN URI (`.../task/{task}/chat/turn/{n}`), the task
/// sibling of [`build_node_chat_turn_uri`].
pub fn build_task_chat_turn_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: &str,
    turn_seq: i32,
) -> String {
    format!(
        "{}/turn/{}",
        build_task_chat_uri(project, number, exec_seq, node_id, task_name),
        turn_seq
    )
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

fn build_node_tasks_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "tasks")
}

fn build_node_calls_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "calls")
}

pub fn build_node_wakes_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "wakes")
}

pub fn build_node_checks_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "checks")
}

pub fn build_task_checks_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: &str,
) -> String {
    build_task_subresource_uri(project, number, exec_seq, node_id, task_name, "checks")
}

fn build_node_questions_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
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

fn build_node_permissions_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
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

fn build_task_permissions_uri(
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

fn build_project_browser_uri(project: &str, slug: &str) -> String {
    format!("{}/browser/{}", build_project_uri(project), slug)
}

pub fn build_project_browser_network_request_uri(
    project: &str,
    slug: &str,
    request_id: &str,
) -> String {
    format!(
        "{}/network/{}",
        build_project_browser_uri(project, slug),
        request_id
    )
}

pub fn build_node_browser_network_request_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    slug: &str,
    request_id: &str,
) -> String {
    format!(
        "{}/network/{}",
        build_node_browser_uri(project, number, exec_seq, node_id, slug),
        request_id
    )
}

pub fn build_task_browser_network_request_uri(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: &str,
    slug: &str,
    request_id: &str,
) -> String {
    format!(
        "{}/network/{}",
        build_task_browser_uri(project, number, exec_seq, node_id, task_name, slug),
        request_id
    )
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

pub fn build_packs_uri() -> String {
    "cairn://packs".to_string()
}

pub fn build_pack_uri(pack_id: &str) -> String {
    format!("cairn://packs/{pack_id}")
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

fn build_labels_uri() -> String {
    "cairn://labels".to_string()
}

pub fn build_label_uri(label_id: &str) -> String {
    format!("cairn://labels/{}", label_id)
}

fn build_node_memories_uri(project: &str, number: i32, exec_seq: i32, node_id: &str) -> String {
    build_node_subresource_uri(project, number, exec_seq, node_id, "memories")
}

fn build_node_symbols_uri(
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

fn build_project_symbols_uri(project: &str, symbol: Option<&str>) -> String {
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

fn build_recipes_uri() -> String {
    "cairn://recipes".to_string()
}

pub fn build_recipe_uri(recipe_id: &str) -> String {
    format!("cairn://recipes/{}", recipe_id)
}

fn build_project_recipes_uri(project: &str) -> String {
    format!("{}/recipes", build_project_uri(project))
}

pub fn build_project_recipe_uri(project: &str, recipe_id: &str) -> String {
    format!("{}/recipes/{}", build_project_uri(project), recipe_id)
}

fn build_workflows_uri() -> String {
    "cairn://workflows".to_string()
}

pub fn build_workflow_uri(workflow_id: &str) -> String {
    format!("cairn://workflows/{}", workflow_id)
}

fn build_project_workflows_uri(project: &str) -> String {
    format!("{}/workflows", build_project_uri(project))
}

pub fn build_project_workflow_uri(project: &str, workflow_id: &str) -> String {
    format!("{}/workflows/{}", build_project_uri(project), workflow_id)
}

pub fn build_routes_uri() -> String {
    "cairn://routes".into()
}
pub fn build_route_uri(id: &str) -> String {
    format!("cairn://routes/{id}")
}
pub fn build_route_history_uri(id: &str) -> String {
    format!("{}/history", build_route_uri(id))
}
pub fn build_route_history_entry_uri(id: &str, seq: i64) -> String {
    format!("{}/{seq}", build_route_history_uri(id))
}
pub fn build_project_routes_uri(project: &str) -> String {
    format!("{}/routes", build_project_uri(project))
}
pub fn build_project_route_uri(project: &str, id: &str) -> String {
    format!("{}/{id}", build_project_routes_uri(project))
}
pub fn build_project_route_history_uri(project: &str, id: &str) -> String {
    format!("{}/history", build_project_route_uri(project, id))
}
pub fn build_project_route_history_entry_uri(project: &str, id: &str, seq: i64) -> String {
    format!("{}/{seq}", build_project_route_history_uri(project, id))
}

pub fn build_responses_uri() -> String {
    "cairn://responses".to_string()
}
pub fn build_response_uri(response_id: &str) -> String {
    format!("cairn://responses/{response_id}")
}
pub fn build_response_history_uri(response_id: &str) -> String {
    format!("{}/history", build_response_uri(response_id))
}
pub fn build_response_history_entry_uri(response_id: &str, seq: i64) -> String {
    format!("{}/{seq}", build_response_history_uri(response_id))
}
pub fn build_project_responses_uri(project: &str) -> String {
    format!("{}/responses", build_project_uri(project))
}
pub fn build_project_response_uri(project: &str, response_id: &str) -> String {
    format!("{}/{response_id}", build_project_responses_uri(project))
}
pub fn build_project_response_history_uri(project: &str, response_id: &str) -> String {
    format!(
        "{}/history",
        build_project_response_uri(project, response_id)
    )
}
pub fn build_project_response_history_entry_uri(
    project: &str,
    response_id: &str,
    seq: i64,
) -> String {
    format!(
        "{}/{seq}",
        build_project_response_history_uri(project, response_id)
    )
}

fn build_agents_uri() -> String {
    "cairn://agents".to_string()
}

pub fn build_agent_uri(agent_id: &str) -> String {
    format!("cairn://agents/{}", agent_id)
}

fn build_project_agents_uri(project: &str) -> String {
    format!("{}/agents", build_project_uri(project))
}

pub fn build_project_agent_uri(project: &str, agent_id: &str) -> String {
    format!("{}/agents/{}", build_project_uri(project), agent_id)
}

fn build_actions_uri() -> String {
    "cairn://actions".to_string()
}

pub fn build_action_uri(action_id: &str) -> String {
    format!("cairn://actions/{}", action_id)
}

fn build_project_actions_uri(project: &str) -> String {
    format!("{}/actions", build_project_uri(project))
}

pub fn build_project_action_uri(project: &str, action_id: &str) -> String {
    format!("{}/actions/{}", build_project_uri(project), action_id)
}

fn build_settings_uri() -> String {
    "cairn://settings".to_string()
}

fn build_projects_uri() -> String {
    "cairn://projects".to_string()
}

fn build_project_settings_uri(project: &str) -> String {
    format!("{}/settings", build_project_uri(project))
}

impl CairnResource {
    pub fn to_uri(&self) -> String {
        match self {
            Self::Project { project } => build_project_uri(project),
            Self::ProjectIssues { project } => build_project_issues_uri(project),
            Self::ProjectCheckResults { project, revision } => {
                build_project_check_results_uri(project, revision)
            }
            Self::ProjectCheckObservation { project, handle } => {
                build_project_check_observation_uri(project, handle)
            }
            Self::ProjectImages { project, issue } => build_project_images_uri(project, *issue),
            Self::ProjectImage { project, reference } => {
                build_project_image_uri(project, reference)
            }
            Self::Issue { project, number } => build_issue_uri(project, *number),
            Self::ProjectThreads { project } => build_project_threads_uri(project),
            Self::Thread {
                project,
                name,
                path,
            } => {
                let mut uri = build_thread_uri(project, name);
                for segment in path {
                    uri.push('/');
                    uri.push_str(segment);
                }
                uri
            }
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
            } => build_node_chat_turn_uri(project, *number, *exec_seq, node_id, *turn_seq),
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
            Self::NodeRepl {
                project,
                number,
                exec_seq,
                node_id,
                slug,
            } => build_node_repl_uri(project, *number, *exec_seq, node_id, slug),
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
            Self::NodeBrowserNetworkRequest {
                project,
                number,
                exec_seq,
                node_id,
                slug,
                request_id,
            } => build_node_browser_network_request_uri(
                project, *number, *exec_seq, node_id, slug, request_id,
            ),
            Self::TaskBrowser {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                slug,
            } => build_task_browser_uri(project, *number, *exec_seq, node_id, task_name, slug),
            Self::TaskBrowserNetworkRequest {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                slug,
                request_id,
            } => build_task_browser_network_request_uri(
                project, *number, *exec_seq, node_id, task_name, slug, request_id,
            ),
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
            } => {
                build_task_chat_turn_uri(project, *number, *exec_seq, node_id, task_name, *turn_seq)
            }
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
            Self::TaskChecks {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => build_task_checks_uri(project, *number, *exec_seq, node_id, task_name),
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
            Self::NodeDiff {
                project,
                number,
                exec_seq,
                node_id,
            } => build_node_diff_uri(project, *number, *exec_seq, node_id),
            Self::NodeRebase {
                project,
                number,
                exec_seq,
                node_id,
            } => build_node_subresource_uri(project, *number, *exec_seq, node_id, "rebase"),
            Self::ProjectTerminal { project, slug } => build_project_terminal_uri(project, slug),
            Self::ProjectBrowser { project, slug } => build_project_browser_uri(project, slug),
            Self::ProjectBrowserNetworkRequest {
                project,
                slug,
                request_id,
            } => build_project_browser_network_request_uri(project, slug, request_id),
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
            Self::Executors => "cairn://executors".to_string(),
            Self::Grants => "cairn://grants".to_string(),
            Self::Grant { id } => format!("cairn://grants/{id}"),
            Self::Executor { name } => format!("cairn://executors/{name}"),
            Self::ExecutorAction { name, action } => {
                format!("cairn://executors/{name}/{action}")
            }
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
            Self::Packs => build_packs_uri(),
            Self::Pack { pack_id } => build_pack_uri(pack_id),
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
            Self::Routes => build_routes_uri(),
            Self::Route { route_id } => build_route_uri(route_id),
            Self::RouteHistory { route_id } => build_route_history_uri(route_id),
            Self::RouteHistoryEntry { route_id, seq } => {
                build_route_history_entry_uri(route_id, *seq)
            }
            Self::ProjectRoutes { project } => build_project_routes_uri(project),
            Self::ProjectRoute { project, route_id } => build_project_route_uri(project, route_id),
            Self::ProjectRouteHistory { project, route_id } => {
                build_project_route_history_uri(project, route_id)
            }
            Self::ProjectRouteHistoryEntry {
                project,
                route_id,
                seq,
            } => build_project_route_history_entry_uri(project, route_id, *seq),
            Self::Responses => build_responses_uri(),
            Self::Response { response_id } => build_response_uri(response_id),
            Self::ResponseHistory { response_id } => build_response_history_uri(response_id),
            Self::ResponseHistoryEntry { response_id, seq } => {
                build_response_history_entry_uri(response_id, *seq)
            }
            Self::ProjectResponses { project } => build_project_responses_uri(project),
            Self::ProjectResponse {
                project,
                response_id,
            } => build_project_response_uri(project, response_id),
            Self::ProjectResponseHistory {
                project,
                response_id,
            } => build_project_response_history_uri(project, response_id),
            Self::ProjectResponseHistoryEntry {
                project,
                response_id,
                seq,
            } => build_project_response_history_entry_uri(project, response_id, *seq),
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
