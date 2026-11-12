//! What kind of view an address opens in the app.
//!
//! [`CairnResource::surface`] is one exhaustive `match`, so a variant added to
//! the enum without a surface fails to compile here. A [`Surface`] names a view
//! and the fields it needs, never a router path: the frontend owns its router
//! and turns each surface into a route there.
//!
//! A kind with no dedicated view is [`Surface::Read`], a generic pane showing
//! the address's read output. The mapping follows what the frontend's
//! navigation already does for the kinds it models; where that navigation sent
//! an address to a nearby view that does not show it (a question to its node's
//! chat), the surface is `Read` instead.

use crate::{CairnResource, NodeAddress};

/// A view an address opens. With the `serde` feature this is the object
/// `{"surface": <kebab-case variant>, ...fields}`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize),
    serde(tag = "surface", rename_all = "kebab-case")
)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum Surface {
    /// The project's issue list.
    ProjectIssues {
        project: String,
    },
    /// The project's thread list.
    ProjectThreads {
        project: String,
    },
    /// The project's merged home conversation.
    ProjectHome {
        project: String,
    },
    /// The project's Tenets tab, which renders every tenet in full.
    ProjectTenets {
        project: String,
    },
    /// The project's promoted pages.
    ProjectPages {
        project: String,
    },
    /// One camp's page: its census, issue roll-call, posts and history. Base
    /// camp's page is the project's generated roadmap.
    Camp {
        project: String,
        slug: String,
    },
    ProjectTerminal {
        project: String,
        slug: String,
    },
    ProjectBrowser {
        project: String,
        slug: String,
    },
    /// The posts timeline, focused on `post` when one is named. `project` is
    /// the timeline's project when the address names one; otherwise the
    /// frontend resolves it (the reader's project, or the post's own).
    Posts {
        project: Option<String>,
        post: Option<i64>,
    },
    IssueOverview {
        project: String,
        number: i32,
    },
    NodeChat {
        project: String,
        number: i32,
        exec: i32,
        node: String,
    },
    NodeDiff {
        project: String,
        number: i32,
        exec: i32,
        node: String,
    },
    /// A node's output; `name: None` is its latest.
    NodeArtifact {
        project: String,
        number: i32,
        exec: i32,
        node: String,
        name: Option<String>,
    },
    NodeTerminal {
        project: String,
        number: i32,
        exec: i32,
        node: String,
        slug: String,
    },
    NodeBrowser {
        project: String,
        number: i32,
        exec: i32,
        node: String,
        slug: String,
    },
    TaskChat {
        project: String,
        number: i32,
        exec: i32,
        node: String,
        task: String,
    },
    ThreadChat {
        project: String,
        name: String,
    },
    /// A thread on its Tenets facet, where its proposals are answered.
    ThreadTenets {
        project: String,
        name: String,
    },
    /// One thread-owned page, rendered as a facet of the thread.
    ThreadPage {
        project: String,
        name: String,
        page: String,
    },
    /// Settings, scoped to `project` when the address names one.
    Settings {
        project: Option<String>,
    },
    /// The notification center's operator requests, focused on `request` when
    /// one is named.
    Requests {
        request: Option<String>,
    },
    /// The build center beside the notification bell: builds of Cairn itself,
    /// opened onto `rebuild` when one is named.
    Builds {
        rebuild: Option<String>,
    },
    /// Settings > Library, focused on `pack` when one is named.
    Packs {
        pack: Option<String>,
    },
    /// The fleet pane, focused on `machine` when one is named.
    Fleet {
        machine: Option<String>,
    },
    /// Settings > Inference.
    Inference,
    Logs,
    /// The application database, browsed read-only.
    Database,
    /// A stored image renders where it is cited and has no view of its own.
    Inline,
    /// No dedicated view: a pane showing what reading `uri` returns.
    Read {
        uri: String,
    },
}

/// The segment a thread's node-family artifact uses for its Tenets facet:
/// `cairn://p/{project}/{thread}/tenets` resolves to the thread session's
/// artifact of that name, and the app opens the facet for it.
const THREAD_TENETS: &str = "tenets";

impl CairnResource {
    /// The view this address opens. See the module docs.
    pub fn surface(&self) -> Surface {
        match self {
            // ---- Workspace ----
            Self::Posts => Surface::Posts {
                project: None,
                post: None,
            },
            Self::Post { id } => Surface::Posts {
                project: None,
                post: Some(*id),
            },
            Self::PostComment { post_id, .. } => Surface::Posts {
                project: None,
                post: Some(*post_id),
            },
            Self::Settings => Surface::Settings { project: None },
            Self::Packs => Surface::Packs { pack: None },
            Self::Pack { pack_id } => Surface::Packs {
                pack: Some(pack_id.clone()),
            },
            Self::Requests => Surface::Requests { request: None },
            Self::Request { id } => Surface::Requests {
                request: Some(id.clone()),
            },
            Self::Rebuilds => Surface::Builds { rebuild: None },
            Self::Rebuild { id } => Surface::Builds {
                rebuild: Some(id.clone()),
            },
            Self::Executors => Surface::Fleet { machine: None },
            Self::Executor { name } => Surface::Fleet {
                machine: Some(name.clone()),
            },
            Self::Inference => Surface::Inference,
            Self::Logs => Surface::Logs,
            Self::Db => Surface::Database,
            Self::Projects
            | Self::Skills
            | Self::Skill { .. }
            | Self::Labels
            | Self::Label { .. }
            | Self::Recipes
            | Self::Recipe { .. }
            | Self::Workflows
            | Self::Workflow { .. }
            | Self::Routes
            | Self::Route { .. }
            | Self::RouteHistory { .. }
            | Self::RouteHistoryEntry { .. }
            | Self::WorkspaceEvents
            | Self::WorkspaceEvent { .. }
            | Self::Responses
            | Self::Response { .. }
            | Self::ResponseHistory { .. }
            | Self::ResponseHistoryEntry { .. }
            | Self::Agents
            | Self::Agent { .. }
            | Self::Actions
            | Self::Action { .. }
            | Self::DbTable { .. }
            | Self::DbRow { .. }
            | Self::Dev
            | Self::DevDb
            | Self::DevPid
            | Self::DevLaunchTicket
            | Self::ChannelsConversations
            | Self::ChannelsAmbientFeedback
            | Self::Grants
            | Self::Grant { .. }
            | Self::Models
            | Self::ModelsCatalog
            | Self::ModelsDecide
            | Self::ModelsEmbed
            | Self::ModelsComplete
            | Self::ModelsImage
            | Self::ModelsSpeak
            | Self::ModelsAccounts
            | Self::ModelsAccount { .. }
            | Self::ModelsUsage
            | Self::ModelsSpend
            | Self::ModelsLadder
            | Self::ModelsSelection
            | Self::ModelsEmbeddings
            | Self::ModelsSessions
            | Self::ModelsSession { .. }
            | Self::ModelsClients
            | Self::ModelsClient { .. }
            | Self::InferenceProfiles
            | Self::InferenceProfile { .. }
            | Self::InferenceDeployments
            | Self::InferenceDeployment { .. }
            | Self::InferencePools
            | Self::InferencePoolHardware { .. }
            | Self::InferenceRequests
            | Self::InferenceModels
            | Self::InferenceModel { .. }
            | Self::InferenceModelFiles { .. }
            | Self::InferenceVerifications
            | Self::InferenceVerification { .. }
            | Self::InferenceFit
            | Self::InferenceTemplates
            | Self::InferenceTemplate { .. }
            | Self::InferenceGenerations
            | Self::InferenceGeneration { .. }
            | Self::Bug
            | Self::Help
            | Self::WebSearch
            | Self::Mcp { .. } => self.read(),

            // ---- Project ----
            // The bare project and its message stream open the issue list.
            Self::Project { project }
            | Self::ProjectMessages { project }
            | Self::ProjectIssues { project } => Surface::ProjectIssues {
                project: project.clone(),
            },
            Self::ProjectThreads { project } => Surface::ProjectThreads {
                project: project.clone(),
            },
            // Home is where the operator reads the merged headlines.
            Self::ProjectHome { project } | Self::ProjectHeadlines { project } => {
                Surface::ProjectHome {
                    project: project.clone(),
                }
            }
            // One tenet opens the tab, which renders every tenet in full.
            Self::ProjectTenets { project } | Self::ProjectTenet { project, .. } => {
                Surface::ProjectTenets {
                    project: project.clone(),
                }
            }
            Self::ProjectPages { project } => Surface::ProjectPages {
                project: project.clone(),
            },
            Self::ProjectPosts { project } => Surface::Posts {
                project: Some(project.clone()),
                post: None,
            },
            Self::ProjectSettings { project } => Surface::Settings {
                project: Some(project.clone()),
            },
            Self::ProjectTerminal { project, slug } => Surface::ProjectTerminal {
                project: project.clone(),
                slug: slug.clone(),
            },
            Self::ProjectBrowser { project, slug } => Surface::ProjectBrowser {
                project: project.clone(),
                slug: slug.clone(),
            },
            Self::ProjectImage { .. } => Surface::Inline,
            // A camp, its census and its posts are sections of one page; the
            // camps collection opens base camp, whose page lists every camp.
            Self::ProjectCamp { project, slug }
            | Self::ProjectCampParticipants { project, slug }
            | Self::ProjectCampPosts { project, slug } => Surface::Camp {
                project: project.clone(),
                slug: slug.clone(),
            },
            Self::ProjectCamps { project } => Surface::Camp {
                project: project.clone(),
                slug: "base".to_string(),
            },
            Self::ProjectCodemap { .. }
            | Self::ProjectMap { .. }
            | Self::ProjectQueue { .. }
            | Self::ProjectCheckResults { .. }
            | Self::ProjectCheckObservation { .. }
            | Self::ProjectVcsDiff { .. }
            | Self::ProjectVcsAncestry { .. }
            | Self::ProjectVcsLog { .. }
            | Self::ProjectVcsBlame { .. }
            | Self::ProjectBrowserNetworkRequest { .. }
            | Self::ProjectSymbols { .. }
            | Self::ProjectReferences { .. }
            | Self::ProjectReference { .. }
            | Self::ProjectSkills { .. }
            | Self::ProjectSkill { .. }
            | Self::ProjectRecipes { .. }
            | Self::ProjectRecipe { .. }
            | Self::ProjectWorkflows { .. }
            | Self::ProjectWorkflow { .. }
            | Self::ProjectServices { .. }
            | Self::ProjectService { .. }
            | Self::ProjectServiceStatus { .. }
            | Self::ProjectServiceLog { .. }
            // The ledger has no dedicated pane; `Builds` renders rebuilds.
            | Self::ProjectPublish { .. }
            | Self::ProjectPublication { .. }
            | Self::ProjectPublicationLegLog { .. }
            | Self::ProjectRoutes { .. }
            | Self::ProjectRoute { .. }
            | Self::ProjectRouteHistory { .. }
            | Self::ProjectRouteHistoryEntry { .. }
            | Self::ProjectEvents { .. }
            | Self::ProjectEvent { .. }
            | Self::ProjectEventEndpoints { .. }
            | Self::ProjectEventEndpoint { .. }
            | Self::ProjectEventDelivery { .. }
            | Self::ProjectResponses { .. }
            | Self::ProjectResponse { .. }
            | Self::ProjectResponseHistory { .. }
            | Self::ProjectResponseHistoryEntry { .. }
            | Self::ProjectAgents { .. }
            | Self::ProjectAgent { .. }
            | Self::ProjectActions { .. }
            | Self::ProjectAction { .. }
            | Self::ProjectImages { .. }
            | Self::ProjectAttachments { .. }
            | Self::ProjectAttachment { .. } => self.read(),

            // ---- Issues ----
            // The overview shows the issue's messages and changed files.
            Self::Issue { project, number }
            | Self::IssueMessages { project, number }
            | Self::Changed { project, number } => Surface::IssueOverview {
                project: project.clone(),
                number: *number,
            },
            Self::IssueComments { .. }
            | Self::IssueComment { .. }
            | Self::IssueExecutions { .. }
            | Self::IssueExecution { .. } => self.read(),

            // ---- Threads ----
            // A thread path the resolver leaves undelegated is the thread.
            Self::Thread { project, name, .. } => Surface::ThreadChat {
                project: project.clone(),
                name: name.clone(),
            },
            Self::ThreadQueue { .. }
            | Self::ThreadDb { .. }
            | Self::ThreadDbSchema { .. }
            | Self::ThreadDbTable { .. }
            | Self::ThreadDbRow { .. }
            | Self::ThreadServices { .. }
            | Self::ThreadService { .. }
            | Self::ThreadServiceStatus { .. }
            | Self::ThreadServiceLog { .. } => self.read(),

            // ---- Node and task jobs ----
            // A job's chat, at any depth, opens the chat.
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
            } => match NodeAddress::new(*number, *exec_seq, node_id) {
                NodeAddress::Thread { name } => thread_chat(project, name),
                NodeAddress::Node { .. } => Surface::NodeChat {
                    project: project.clone(),
                    number: *number,
                    exec: *exec_seq,
                    node: node_id.clone(),
                },
            },
            Self::NodeDiff {
                project,
                number,
                exec_seq,
                node_id,
            } => match NodeAddress::new(*number, *exec_seq, node_id) {
                NodeAddress::Thread { .. } => self.read(),
                NodeAddress::Node { .. } => Surface::NodeDiff {
                    project: project.clone(),
                    number: *number,
                    exec: *exec_seq,
                    node: node_id.clone(),
                },
            },
            Self::NodeArtifact {
                project,
                number,
                exec_seq,
                node_id,
                name,
            } => match NodeAddress::new(*number, *exec_seq, node_id) {
                NodeAddress::Thread { name: thread } if name.as_deref() == Some(THREAD_TENETS) => {
                    Surface::ThreadTenets {
                        project: project.clone(),
                        name: thread.to_string(),
                    }
                }
                NodeAddress::Thread { .. } => self.read(),
                NodeAddress::Node { .. } => Surface::NodeArtifact {
                    project: project.clone(),
                    number: *number,
                    exec: *exec_seq,
                    node: node_id.clone(),
                    name: name.clone(),
                },
            },
            Self::NodeTerminal {
                project,
                number,
                exec_seq,
                node_id,
                slug,
            } => match NodeAddress::new(*number, *exec_seq, node_id) {
                NodeAddress::Thread { .. } => self.read(),
                NodeAddress::Node { .. } => Surface::NodeTerminal {
                    project: project.clone(),
                    number: *number,
                    exec: *exec_seq,
                    node: node_id.clone(),
                    slug: slug.clone(),
                },
            },
            Self::NodeBrowser {
                project,
                number,
                exec_seq,
                node_id,
                slug,
            } => match NodeAddress::new(*number, *exec_seq, node_id) {
                NodeAddress::Thread { .. } => self.read(),
                NodeAddress::Node { .. } => Surface::NodeBrowser {
                    project: project.clone(),
                    number: *number,
                    exec: *exec_seq,
                    node: node_id.clone(),
                    slug: slug.clone(),
                },
            },
            Self::NodePage {
                project,
                number,
                exec_seq,
                node_id,
                name,
            } => match NodeAddress::new(*number, *exec_seq, node_id) {
                NodeAddress::Thread { name: thread } => Surface::ThreadPage {
                    project: project.clone(),
                    name: thread.to_string(),
                    page: name.clone(),
                },
                // A page on an issue node is a draft with no view of its own.
                NodeAddress::Node { .. } => self.read(),
            },
            // A task's artifact shows in the task's chat, as its return.
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
            }
            | Self::TaskArtifact {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                ..
            } => match NodeAddress::new(*number, *exec_seq, node_id) {
                // A thread's task has no chat view of its own; its thread's
                // conversation is where it was spawned and where it returns.
                NodeAddress::Thread { name } => thread_chat(project, name),
                NodeAddress::Node { .. } => Surface::TaskChat {
                    project: project.clone(),
                    number: *number,
                    exec: *exec_seq,
                    node: node_id.clone(),
                    task: task_name.clone(),
                },
            },
            // Questions and permission requests render inside the chat that
            // raised them but have no view that shows one by itself.
            Self::NodeQuestions { .. }
            | Self::NodeQuestion { .. }
            | Self::NodePermissions { .. }
            | Self::NodePermission { .. }
            | Self::TaskPermissions { .. }
            | Self::TaskPermission { .. }
            | Self::NodeSymbols { .. }
            | Self::NodeRepl { .. }
            | Self::NodeBrowserNetworkRequest { .. }
            | Self::NodeRebase { .. }
            | Self::NodeTasks { .. }
            | Self::NodeCalls { .. }
            | Self::NodeWakes { .. }
            | Self::NodeChecks { .. }
            | Self::NodeBatches { .. }
            | Self::NodeBatch { .. }
            | Self::NodeMessages { .. }
            | Self::NodeProgress { .. }
            | Self::NodeMemories { .. }
            | Self::NodeMemory { .. }
            | Self::NodeEras { .. }
            | Self::NodeEra { .. }
            | Self::NodePages { .. }
            | Self::NodePageLog { .. }
            | Self::TaskTerminal { .. }
            | Self::TaskBrowser { .. }
            | Self::TaskBrowserNetworkRequest { .. }
            | Self::TaskBatches { .. }
            | Self::TaskBatch { .. }
            | Self::TaskChecks { .. }
            | Self::TaskMessages { .. }
            | Self::JobTodos { .. }
            | Self::HomeFeed { .. } => self.read(),
        }
    }

    fn read(&self) -> Surface {
        Surface::Read { uri: self.to_uri() }
    }
}

fn thread_chat(project: &str, name: &str) -> Surface {
    Surface::ThreadChat {
        project: project.to_string(),
        name: name.to_string(),
    }
}

#[cfg(test)]
mod tests;
