//! The condensed label an address renders as wherever it is cited: a chip, a
//! reference heading, a tool row.
//!
//! [`CairnResource::label`] is one exhaustive `match`, so a variant added to the
//! enum without a label fails to compile here. The frontend renders every chip
//! through this function (compiled to WebAssembly), and the kinds it labelled
//! before it adopted the Rust grammar keep exactly the text they had;
//! `tests::the_frontend_labels_are_reproduced` pins them.
//!
//! The style, extended to every other kind:
//!
//! - A project-scoped address leads with its lowercased project key, an issue
//!   with `project/number`, and each narrower part follows after ` / `
//!   (`cairn / tenets`, `cairn/42 / comment 3`).
//! - A node-family address is `project/number / node`, then the task, then the
//!   leaf the address names (`cairn/42 / builder / questions`). Its chat, and a
//!   task's chat, name no leaf: the job is what a reader recognizes.
//! - Anything owned by a thread is `project/thread / leaf`, where the leaf is
//!   the address's last path segment (`cairn/uri-parity / questions`).
//! - A workspace-level address names no project to set the case, so it leads
//!   with a capitalized word (`Posts`, `Executor mac-mini`, `Models catalog`).

use std::fmt::Display;

use crate::percent::decode_component;
use crate::{build_thread_uri, AttachmentRef, AttachmentScopeRef, CairnResource, NodeAddress};

/// Display names a caller already knows for the job an address names, used in
/// place of the raw URI segments. The fields mirror `ResourceLabelOptions` in
/// `packages/ui/src/uri.ts`, and with the `serde` feature the JSON form is
/// `{"nodeLabel": .., "taskLabel": ..}`, either key optional.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(rename_all = "camelCase", default, deny_unknown_fields)
)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LabelContext {
    /// Replaces the node segment (`builder`) of a node-family address.
    pub node_label: Option<String>,
    /// Replaces the task segment of a task-family address.
    pub task_label: Option<String>,
}

/// The job a node-family address hangs beneath, as raw coordinates.
struct Job<'a> {
    project: &'a str,
    number: i32,
    exec_seq: i32,
    node_id: &'a str,
    task: Option<&'a str>,
}

impl<'a> Job<'a> {
    fn node(project: &'a str, number: i32, exec_seq: i32, node_id: &'a str) -> Self {
        Self {
            project,
            number,
            exec_seq,
            node_id,
            task: None,
        }
    }

    fn task(project: &'a str, number: i32, exec_seq: i32, node_id: &'a str, task: &'a str) -> Self {
        Self {
            task: Some(task),
            ..Self::node(project, number, exec_seq, node_id)
        }
    }

    fn maybe_task(
        project: &'a str,
        number: i32,
        exec_seq: i32,
        node_id: &'a str,
        task: &'a Option<String>,
    ) -> Self {
        Self {
            task: task.as_deref(),
            ..Self::node(project, number, exec_seq, node_id)
        }
    }
}

fn key(project: &str) -> String {
    project.to_lowercase()
}

/// `cairn / leaf`.
fn in_project(project: &str, leaf: impl Display) -> String {
    format!("{} / {leaf}", key(project))
}

/// `cairn/42`.
fn issue(project: &str, number: i32) -> String {
    format!("{}/{number}", key(project))
}

/// `cairn/42 / leaf`.
fn in_issue(project: &str, number: i32, leaf: impl Display) -> String {
    format!("{} / {leaf}", issue(project, number))
}

/// The owner an attachment ordinal is scoped to: `cairn/42`, `cairn/ui`, or
/// the project itself.
fn scope_base(project: &str, scope: &AttachmentScopeRef) -> String {
    match scope {
        AttachmentScopeRef::Issue(number) => issue(project, *number),
        AttachmentScopeRef::Thread(name) => format!("{}/{name}", key(project)),
        AttachmentScopeRef::Project => key(project),
    }
}

/// One stored object. The permalink form names only the project: its own
/// text is a content hash, which says nothing to a reader.
fn attachment(project: &str, reference: &AttachmentRef, noun: &str) -> String {
    match reference {
        AttachmentRef::Scoped { scope, ordinal } => {
            format!("{} / {noun} {ordinal}", scope_base(project, scope))
        }
        AttachmentRef::Hash(_) => in_project(project, noun),
    }
}

/// `cairn/thread`, then ` / ` and the last path segment of `uri` beneath the
/// thread, when there is one.
fn thread(project: &str, name: &str, uri: &str) -> String {
    let base = format!("{}/{name}", key(project));
    let path = uri.split('?').next().unwrap_or(uri);
    let below = path
        .strip_prefix(build_thread_uri(project, name).as_str())
        .unwrap_or_default();
    match below.rsplit('/').next().filter(|leaf| !leaf.is_empty()) {
        Some(leaf) => format!(
            "{base} / {}",
            decode_component(leaf).unwrap_or_else(|_| leaf.to_string())
        ),
        None => base,
    }
}

/// `Word`, or `Word id` when the address names one member.
fn workspace(word: &str, id: Option<&str>) -> String {
    match id {
        Some(id) => format!("{word} {id}"),
        None => word.to_string(),
    }
}

fn with_path(label: String, path: &[String]) -> String {
    if path.is_empty() {
        label
    } else {
        format!("{label} / {}", path.join("/"))
    }
}

fn symbol_leaf(symbol: &Option<String>) -> String {
    match symbol {
        Some(symbol) => format!("symbol {symbol}"),
        None => "symbols".to_string(),
    }
}

impl CairnResource {
    /// The condensed label for this address. See the module docs for the style;
    /// `ctx` supplies display names for the job a node-family address names.
    pub fn label(&self, ctx: &LabelContext) -> String {
        match self {
            // ---- Workspace ----
            Self::Posts => "Posts".to_string(),
            Self::Post { id } => format!("Post {id}"),
            Self::PostComment {
                post_id,
                comment_id,
            } => format!("Post {post_id} / comment {comment_id}"),
            Self::Settings => "Settings".to_string(),
            Self::Projects => "Projects".to_string(),
            Self::Packs => "Packs".to_string(),
            Self::Pack { pack_id } => workspace("Pack", Some(pack_id)),
            Self::Skills => "Skills".to_string(),
            Self::Skill { skill_id, path } => with_path(workspace("Skill", Some(skill_id)), path),
            Self::Labels => "Labels".to_string(),
            Self::Label { label_id } => workspace("Label", Some(label_id)),
            Self::Recipes => "Recipes".to_string(),
            Self::Recipe { recipe_id } => workspace("Recipe", Some(recipe_id)),
            Self::Workflows => "Workflows".to_string(),
            Self::Workflow { workflow_id } => workspace("Workflow", Some(workflow_id)),
            Self::Routes => "Routes".to_string(),
            Self::Route { route_id } => workspace("Route", Some(route_id)),
            Self::RouteHistory { route_id } => format!("Route {route_id} / history"),
            Self::RouteHistoryEntry { route_id, seq } => {
                format!("Route {route_id} / history {seq}")
            }
            Self::WorkspaceEvents => "Events".to_string(),
            Self::WorkspaceEvent { id } => format!("Event {id}"),
            Self::Responses => "Responses".to_string(),
            Self::Response { response_id } => workspace("Response", Some(response_id)),
            Self::ResponseHistory { response_id } => format!("Response {response_id} / history"),
            Self::ResponseHistoryEntry { response_id, seq } => {
                format!("Response {response_id} / history {seq}")
            }
            Self::Agents => "Agents".to_string(),
            Self::Agent { agent_id } => workspace("Agent", Some(agent_id)),
            Self::Actions => "Actions".to_string(),
            Self::Action { action_id } => workspace("Action", Some(action_id)),
            Self::Db => "Database".to_string(),
            Self::DbTable { table } => workspace("Database", Some(table)),
            Self::DbRow { table, pk } => format!("Database {table} / {pk}"),
            Self::Dev => "Dev".to_string(),
            Self::DevDb => "Dev db".to_string(),
            Self::DevPid => "Dev pid".to_string(),
            Self::DevLaunchTicket => "Dev launch ticket".to_string(),
            Self::Logs => "Logs".to_string(),
            Self::ChannelsConversations => "Channel conversations".to_string(),
            Self::ChannelsAmbientFeedback => "Channel ambient feedback".to_string(),
            Self::Executors => "Executors".to_string(),
            Self::Executor { name } => workspace("Executor", Some(name)),
            Self::Requests => "Requests".to_string(),
            Self::Request { id } => workspace("Request", Some(id)),
            Self::Grants => "Grants".to_string(),
            Self::Grant { id } => workspace("Grant", Some(id)),
            Self::Models => "Models".to_string(),
            Self::ModelsCatalog => "Models catalog".to_string(),
            Self::ModelsDecide => "Models decide".to_string(),
            Self::ModelsEmbed => "Models embed".to_string(),
            Self::ModelsComplete => "Models complete".to_string(),
            Self::ModelsImage => "Models image".to_string(),
            Self::ModelsSpeak => "Models speak".to_string(),
            Self::ModelsAccounts => "Models accounts".to_string(),
            Self::ModelsAccount { id } => workspace("Models account", Some(id)),
            Self::ModelsUsage => "Models usage".to_string(),
            Self::ModelsSpend => "Models spend".to_string(),
            Self::ModelsLadder => "Models ladder".to_string(),
            Self::ModelsSelection => "Models selection".to_string(),
            Self::ModelsEmbeddings => "Models embeddings".to_string(),
            Self::ModelsSessions => "Models sessions".to_string(),
            Self::ModelsSession { id } => workspace("Models session", Some(id)),
            Self::ModelsClients => "Models clients".to_string(),
            Self::ModelsClient { id } => workspace("Models client", Some(id)),
            Self::Inference => "Inference".to_string(),
            Self::InferenceProfiles => "Inference profiles".to_string(),
            Self::InferenceProfile { id } => workspace("Inference profile", Some(id)),
            Self::InferenceDeployments => "Inference deployments".to_string(),
            Self::InferenceDeployment { id } => workspace("Inference deployment", Some(id)),
            Self::InferencePools => "Inference pools".to_string(),
            Self::InferencePoolHardware { pool } => format!("Inference pool {pool} / hardware"),
            Self::InferenceRequests => "Inference requests".to_string(),
            Self::InferenceModels => "Inference models".to_string(),
            Self::InferenceModel { name } => workspace("Inference model", Some(name)),
            Self::InferenceModelFiles { name } => format!("Inference model {name} / files"),
            Self::InferenceVerifications => "Inference verifications".to_string(),
            Self::InferenceVerification { id } => workspace("Inference verification", Some(id)),
            Self::InferenceFit => "Inference fit".to_string(),
            Self::InferenceTemplates => "Inference templates".to_string(),
            Self::InferenceTemplate { id } => workspace("Inference template", Some(id)),
            Self::InferenceGenerations => "Inference generations".to_string(),
            Self::InferenceGeneration { id } => workspace("Inference generation", Some(id)),
            Self::Bug => "Bug".to_string(),
            Self::Rebuilds => "Rebuild ledger".to_string(),
            Self::Rebuild { id } => workspace("Rebuild", Some(id)),
            Self::Help => "Help".to_string(),
            Self::WebSearch => "Web search".to_string(),
            Self::Mcp { server, resource } => match (server, resource) {
                (Some(server), Some(resource)) => format!("MCP {server} / {resource}"),
                (Some(server), None) => format!("MCP {server}"),
                (None, Some(resource)) => format!("MCP / {resource}"),
                (None, None) => "MCP".to_string(),
            },

            // ---- Project ----
            // The bare project and its message stream are the project itself.
            Self::Project { project } | Self::ProjectMessages { project } => key(project),
            Self::ProjectMessage { project, id } => in_project(project, format!("message {id}")),
            Self::ProjectSettings { project } => in_project(project, "settings"),
            Self::ProjectIssues { project } => in_project(project, "issues"),
            Self::ProjectThreads { project } => in_project(project, "threads"),
            Self::ProjectPosts { project } => in_project(project, "posts"),
            Self::ProjectTenets { project } => in_project(project, "tenets"),
            // A number reads as a tenet's number; a migrated slug as written.
            Self::ProjectTenet { project, key } => {
                if !key.is_empty() && key.bytes().all(|byte| byte.is_ascii_digit()) {
                    in_project(project, format!("tenet #{key}"))
                } else {
                    in_project(project, format!("tenet {key}"))
                }
            }
            Self::ProjectCamps { project } => in_project(project, "camps"),
            // The project's standing camp reads as a phrase, not as a slug.
            Self::ProjectCamp { project, slug } if slug == "base" => {
                in_project(project, "base camp")
            }
            Self::ProjectCamp { project, slug } => in_project(project, format!("camp {slug}")),
            Self::ProjectCampParticipants { project, slug } => {
                in_project(project, format!("camp {slug} / participants"))
            }
            Self::ProjectCampPosts { project, slug } if slug == "base" => {
                in_project(project, "base camp posts")
            }
            Self::ProjectCampPosts { project, slug } => {
                in_project(project, format!("{slug} camp posts"))
            }
            Self::ProjectHome { project } => in_project(project, "home"),
            Self::ProjectHeadlines { project } => in_project(project, "headlines"),
            Self::ProjectPages { project } => in_project(project, "pages"),
            Self::ProjectCodemap { project } => in_project(project, "codemap"),
            Self::ProjectMap { project } => in_project(project, "map"),
            Self::ProjectQueue { project } => in_project(project, "queue"),
            Self::ProjectCheckResults { project, revision } => {
                in_project(project, format!("checks {revision}"))
            }
            Self::ProjectCheckObservation { project, handle } => {
                in_project(project, format!("check {handle}"))
            }
            Self::ProjectVcsDiff { project } => in_project(project, "vcs diff"),
            Self::ProjectVcsAncestry { project } => in_project(project, "vcs ancestry"),
            Self::ProjectVcsLog { project } => in_project(project, "vcs log"),
            Self::ProjectVcsBlame { project } => in_project(project, "vcs blame"),
            // A project terminal is named by its slug alone.
            Self::ProjectTerminal { project, slug } => in_project(project, slug),
            Self::ProjectBrowser { project, slug } => {
                in_project(project, format!("browser {slug}"))
            }
            Self::ProjectBrowserNetworkRequest {
                project,
                slug,
                request_id,
            } => in_project(project, format!("browser {slug} / request {request_id}")),
            Self::ProjectSymbols { project, symbol } => in_project(project, symbol_leaf(symbol)),
            Self::ProjectReferences { project } => in_project(project, "references"),
            Self::ProjectReference { project, name } => {
                in_project(project, format!("reference {name}"))
            }
            Self::ProjectSkills { project } => in_project(project, "skills"),
            Self::ProjectSkill {
                project,
                skill_id,
                path,
            } => with_path(in_project(project, format!("skill {skill_id}")), path),
            Self::ProjectRecipes { project } => in_project(project, "recipes"),
            Self::ProjectRecipe { project, recipe_id } => {
                in_project(project, format!("recipe {recipe_id}"))
            }
            Self::ProjectWorkflows { project } => in_project(project, "workflows"),
            Self::ProjectWorkflow {
                project,
                workflow_id,
            } => in_project(project, format!("workflow {workflow_id}")),
            Self::ProjectServices { project } => in_project(project, "services"),
            Self::ProjectService { project, service } => {
                in_project(project, format!("service {service}"))
            }
            Self::ProjectServiceStatus { project, service } => {
                in_project(project, format!("service {service} / status"))
            }
            Self::ProjectServiceLog { project, service } => {
                in_project(project, format!("service {service} / log"))
            }
            Self::ProjectPublish { project } => in_project(project, "publications"),
            Self::ProjectPublication { project, id } => {
                in_project(project, format!("publication {id}"))
            }
            Self::ProjectPublicationLegLog {
                project,
                id,
                target,
                leg,
            } => in_project(project, format!("publication {id} / {target} {leg} log")),
            Self::ProjectRoutes { project } => in_project(project, "routes"),
            Self::ProjectRoute { project, route_id } => {
                in_project(project, format!("route {route_id}"))
            }
            Self::ProjectRouteHistory { project, route_id } => {
                in_project(project, format!("route {route_id} / history"))
            }
            Self::ProjectRouteHistoryEntry {
                project,
                route_id,
                seq,
            } => in_project(project, format!("route {route_id} / history {seq}")),
            Self::ProjectEvents { project } => in_project(project, "events"),
            Self::ProjectEvent { project, id } => in_project(project, format!("event {id}")),
            Self::ProjectEventEndpoints { project } => in_project(project, "event endpoints"),
            Self::ProjectEventEndpoint { project, name } => {
                in_project(project, format!("event endpoint {name}"))
            }
            Self::ProjectEventDelivery { project, name, id } => {
                in_project(project, format!("event endpoint {name} / delivery {id}"))
            }
            Self::ProjectResponses { project } => in_project(project, "responses"),
            Self::ProjectResponse {
                project,
                response_id,
            } => in_project(project, format!("response {response_id}")),
            Self::ProjectResponseHistory {
                project,
                response_id,
            } => in_project(project, format!("response {response_id} / history")),
            Self::ProjectResponseHistoryEntry {
                project,
                response_id,
                seq,
            } => in_project(project, format!("response {response_id} / history {seq}")),
            Self::ProjectAgents { project } => in_project(project, "agents"),
            Self::ProjectAgent { project, agent_id } => {
                in_project(project, format!("agent {agent_id}"))
            }
            Self::ProjectActions { project } => in_project(project, "actions"),
            Self::ProjectAction { project, action_id } => {
                in_project(project, format!("action {action_id}"))
            }

            // ---- Attachments ----
            Self::ProjectImages { project, scope } => {
                format!("{} / images", scope_base(project, scope))
            }
            Self::ProjectImage { project, reference } => attachment(project, reference, "image"),
            Self::ProjectAttachments { project, scope } => {
                format!("{} / attachments", scope_base(project, scope))
            }
            Self::ProjectAttachment { project, reference } => {
                attachment(project, reference, "attachment")
            }

            // ---- Issues ----
            // An issue's message stream and its changed files are the issue.
            Self::Issue { project, number }
            | Self::IssueMessages { project, number }
            | Self::Changed { project, number } => issue(project, *number),
            Self::IssueComments { project, number } => in_issue(project, *number, "comments"),
            Self::IssueComment {
                project,
                number,
                comment_seq,
            } => in_issue(project, *number, format!("comment {comment_seq}")),
            Self::IssueExecutions { project, number } => in_issue(project, *number, "executions"),
            Self::IssueExecution {
                project,
                number,
                exec_seq,
            } => in_issue(project, *number, format!("execution {exec_seq}")),

            // ---- Threads ----
            Self::Thread { project, name, .. }
            | Self::ThreadDb { project, name }
            | Self::ThreadDbSchema { project, name }
            | Self::ThreadDbTable { project, name, .. }
            | Self::ThreadDbRow { project, name, .. }
            | Self::ThreadServices { project, name }
            | Self::ThreadService { project, name, .. }
            | Self::ThreadServiceStatus { project, name, .. }
            | Self::ThreadServiceLog { project, name, .. }
            | Self::ThreadQueue {
                project,
                thread: name,
            } => thread(project, name, &self.to_uri()),

            // ---- Node and task jobs ----
            // A job's chat names no leaf: the job is what a reader recognizes.
            Self::Node {
                project,
                number,
                exec_seq,
                node_id,
            }
            | Self::NodeChat {
                project,
                number,
                exec_seq,
                node_id,
            }
            | Self::NodeChatRaw {
                project,
                number,
                exec_seq,
                node_id,
            }
            | Self::NodeChatTurn {
                project,
                number,
                exec_seq,
                node_id,
                ..
            }
            | Self::NodeChatEvent {
                project,
                number,
                exec_seq,
                node_id,
                ..
            } => self.job(ctx, Job::node(project, *number, *exec_seq, node_id), None),
            Self::NodeArtifact {
                project,
                number,
                exec_seq,
                node_id,
                name,
            } => self.job(
                ctx,
                Job::node(project, *number, *exec_seq, node_id),
                Some(name.as_deref().unwrap_or("artifact").to_string()),
            ),
            Self::NodeSymbols {
                project,
                number,
                exec_seq,
                node_id,
                symbol,
            } => self.job(
                ctx,
                Job::node(project, *number, *exec_seq, node_id),
                Some(symbol_leaf(symbol)),
            ),
            // A terminal is named by its slug alone.
            Self::NodeTerminal {
                project,
                number,
                exec_seq,
                node_id,
                slug,
            } => self.job(
                ctx,
                Job::node(project, *number, *exec_seq, node_id),
                Some(slug.clone()),
            ),
            Self::NodeRepl {
                project,
                number,
                exec_seq,
                node_id,
                slug,
            } => self.job(
                ctx,
                Job::node(project, *number, *exec_seq, node_id),
                Some(format!("repl {slug}")),
            ),
            Self::NodeBrowser {
                project,
                number,
                exec_seq,
                node_id,
                slug,
            } => self.job(
                ctx,
                Job::node(project, *number, *exec_seq, node_id),
                Some(format!("browser {slug}")),
            ),
            Self::NodeBrowserNetworkRequest {
                project,
                number,
                exec_seq,
                node_id,
                slug,
                request_id,
            } => self.job(
                ctx,
                Job::node(project, *number, *exec_seq, node_id),
                Some(format!("browser {slug} / request {request_id}")),
            ),
            Self::NodeDiff {
                project,
                number,
                exec_seq,
                node_id,
            } => self.node_leaf(ctx, project, *number, *exec_seq, node_id, "diff"),
            Self::NodeRebase {
                project,
                number,
                exec_seq,
                node_id,
            } => self.node_leaf(ctx, project, *number, *exec_seq, node_id, "rebase"),
            Self::NodeTasks {
                project,
                number,
                exec_seq,
                node_id,
            } => self.node_leaf(ctx, project, *number, *exec_seq, node_id, "tasks"),
            Self::NodeCalls {
                project,
                number,
                exec_seq,
                node_id,
            } => self.node_leaf(ctx, project, *number, *exec_seq, node_id, "calls"),
            Self::NodeWakes {
                project,
                number,
                exec_seq,
                node_id,
            } => self.node_leaf(ctx, project, *number, *exec_seq, node_id, "wakes"),
            Self::NodeChecks {
                project,
                number,
                exec_seq,
                node_id,
            } => self.node_leaf(ctx, project, *number, *exec_seq, node_id, "checks"),
            Self::NodeBatches {
                project,
                number,
                exec_seq,
                node_id,
            } => self.node_leaf(ctx, project, *number, *exec_seq, node_id, "batches"),
            Self::NodeBatch {
                project,
                number,
                exec_seq,
                node_id,
                request_id,
            } => self.job(
                ctx,
                Job::node(project, *number, *exec_seq, node_id),
                Some(format!("batch {request_id}")),
            ),
            Self::NodeQuestions {
                project,
                number,
                exec_seq,
                node_id,
            } => self.node_leaf(ctx, project, *number, *exec_seq, node_id, "questions"),
            // One question or permission request reads as the kind, not its
            // stored segment (`q-1`), which says nothing to a reader.
            Self::NodeQuestion {
                project,
                number,
                exec_seq,
                node_id,
                ..
            } => self.node_leaf(ctx, project, *number, *exec_seq, node_id, "question"),
            Self::NodePermissions {
                project,
                number,
                exec_seq,
                node_id,
            } => self.node_leaf(ctx, project, *number, *exec_seq, node_id, "permissions"),
            Self::NodePermission {
                project,
                number,
                exec_seq,
                node_id,
                ..
            } => self.node_leaf(ctx, project, *number, *exec_seq, node_id, "permission"),
            Self::NodeMessages {
                project,
                number,
                exec_seq,
                node_id,
            } => self.node_leaf(ctx, project, *number, *exec_seq, node_id, "messages"),
            Self::NodeProgress {
                project,
                number,
                exec_seq,
                node_id,
            } => self.node_leaf(ctx, project, *number, *exec_seq, node_id, "progress"),
            Self::NodeMemories {
                project,
                number,
                exec_seq,
                node_id,
            } => self.node_leaf(ctx, project, *number, *exec_seq, node_id, "memories"),
            Self::NodeMemory {
                project,
                number,
                exec_seq,
                node_id,
                memory_seq,
            } => self.job(
                ctx,
                Job::node(project, *number, *exec_seq, node_id),
                Some(format!("memory {memory_seq}")),
            ),
            Self::NodeEras {
                project,
                number,
                exec_seq,
                node_id,
            } => self.node_leaf(ctx, project, *number, *exec_seq, node_id, "eras"),
            Self::NodeEra {
                project,
                number,
                exec_seq,
                node_id,
                era_seq,
            } => self.job(
                ctx,
                Job::node(project, *number, *exec_seq, node_id),
                Some(format!("era {era_seq}")),
            ),
            Self::NodePages {
                project,
                number,
                exec_seq,
                node_id,
            } => self.node_leaf(ctx, project, *number, *exec_seq, node_id, "pages"),
            Self::NodePage {
                project,
                number,
                exec_seq,
                node_id,
                name,
            } => self.job(
                ctx,
                Job::node(project, *number, *exec_seq, node_id),
                Some(format!("page {name}")),
            ),
            Self::NodePageLog {
                project,
                number,
                exec_seq,
                node_id,
                name,
            } => self.job(
                ctx,
                Job::node(project, *number, *exec_seq, node_id),
                Some(format!("page {name} / log")),
            ),
            Self::Task {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            }
            | Self::TaskChat {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            }
            | Self::TaskChatRaw {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            }
            | Self::TaskChatTurn {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                ..
            }
            | Self::TaskChatEvent {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                ..
            } => self.job(
                ctx,
                Job::task(project, *number, *exec_seq, node_id, task_name),
                None,
            ),
            Self::TaskArtifact {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                name,
            } => self.job(
                ctx,
                Job::task(project, *number, *exec_seq, node_id, task_name),
                Some(name.as_deref().unwrap_or("artifact").to_string()),
            ),
            Self::TaskTerminal {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                slug,
            } => self.job(
                ctx,
                Job::task(project, *number, *exec_seq, node_id, task_name),
                Some(slug.clone()),
            ),
            Self::TaskBrowser {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                slug,
            } => self.job(
                ctx,
                Job::task(project, *number, *exec_seq, node_id, task_name),
                Some(format!("browser {slug}")),
            ),
            Self::TaskBrowserNetworkRequest {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                slug,
                request_id,
            } => self.job(
                ctx,
                Job::task(project, *number, *exec_seq, node_id, task_name),
                Some(format!("browser {slug} / request {request_id}")),
            ),
            Self::TaskBatches {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => self.job(
                ctx,
                Job::task(project, *number, *exec_seq, node_id, task_name),
                Some("batches".to_string()),
            ),
            Self::TaskBatch {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                request_id,
            } => self.job(
                ctx,
                Job::task(project, *number, *exec_seq, node_id, task_name),
                Some(format!("batch {request_id}")),
            ),
            Self::TaskChecks {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => self.job(
                ctx,
                Job::task(project, *number, *exec_seq, node_id, task_name),
                Some("checks".to_string()),
            ),
            Self::TaskPermissions {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => self.job(
                ctx,
                Job::task(project, *number, *exec_seq, node_id, task_name),
                Some("permissions".to_string()),
            ),
            Self::TaskPermission {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                ..
            } => self.job(
                ctx,
                Job::task(project, *number, *exec_seq, node_id, task_name),
                Some("permission".to_string()),
            ),
            Self::TaskMessages {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => self.job(
                ctx,
                Job::task(project, *number, *exec_seq, node_id, task_name),
                Some("messages".to_string()),
            ),
            Self::JobTodos {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => self.job(
                ctx,
                Job::maybe_task(project, *number, *exec_seq, node_id, task_name),
                Some("todos".to_string()),
            ),
            Self::HomeFeed {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => self.job(
                ctx,
                Job::maybe_task(project, *number, *exec_seq, node_id, task_name),
                Some("feed".to_string()),
            ),
        }
    }

    fn node_leaf(
        &self,
        ctx: &LabelContext,
        project: &str,
        number: i32,
        exec_seq: i32,
        node_id: &str,
        leaf: &str,
    ) -> String {
        self.job(
            ctx,
            Job::node(project, number, exec_seq, node_id),
            Some(leaf.to_string()),
        )
    }

    /// A node-family label: `cairn/42 / node [/ task] [/ leaf]`, or, when the
    /// coordinate is a thread's, the thread and the address's last segment.
    fn job(&self, ctx: &LabelContext, job: Job<'_>, leaf: Option<String>) -> String {
        match NodeAddress::new(job.number, job.exec_seq, job.node_id) {
            NodeAddress::Thread { name } => thread(job.project, name, &self.to_uri()),
            NodeAddress::Node {
                number, node_id, ..
            } => {
                let mut label = in_issue(
                    job.project,
                    number,
                    ctx.node_label.as_deref().unwrap_or(node_id),
                );
                if let Some(task) = job.task {
                    label.push_str(" / ");
                    label.push_str(ctx.task_label.as_deref().unwrap_or(task));
                }
                if let Some(leaf) = leaf {
                    label.push_str(" / ");
                    label.push_str(&leaf);
                }
                label
            }
        }
    }
}

#[cfg(test)]
mod tests;
