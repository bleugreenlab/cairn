//! Field-projection query methods over `CairnResource`: the project key, issue
//! number, and node id.

use super::types::CairnResource;

impl CairnResource {
    /// The project key this resource is scoped to, if any.
    ///
    /// Project-scoped resources route to the database that owns their project
    /// (see `DbState::for_project`); global/workspace resources (skills, labels,
    /// settings, the projects collection, dev/db/logs, etc.) return `None` and
    /// stay on the private database. The rebuild ledger is install-local even
    /// when a journal describes work requested by a project.
    pub fn project_key(&self) -> Option<&str> {
        use CairnResource::*;
        match self {
            Project { project, .. }
            | ProjectImage { project, .. }
            | ProjectImages { project, .. }
            | ProjectAttachment { project, .. }
            | ProjectAttachments { project, .. }
            | ProjectIssues { project, .. }
            | ProjectPosts { project, .. }
            | ProjectTenets { project, .. }
            | ProjectTenet { project, .. }
            | ProjectCamps { project, .. }
            | ProjectCamp { project, .. }
            | ProjectCampParticipants { project, .. }
            | ProjectCampPosts { project, .. }
            | ProjectPages { project, .. }
            | ProjectCodemap { project, .. }
            | ProjectQueue { project, .. }
            | ProjectMap { project, .. }
            | ProjectHome { project, .. }
            | ProjectHeadlines { project, .. }
            | ProjectCheckResults { project, .. }
            | ProjectVcsDiff { project }
            | ProjectVcsAncestry { project }
            | ProjectVcsLog { project }
            | ProjectVcsBlame { project }
            | Issue { project, .. }
            | ProjectThreads { project, .. }
            | Thread { project, .. }
            | Node { project, .. }
            | NodeChat { project, .. }
            | NodeChatRaw { project, .. }
            | NodeChatTurn { project, .. }
            | NodeChatEvent { project, .. }
            | NodeArtifact { project, .. }
            | NodeSymbols { project, .. }
            | ProjectSymbols { project, .. }
            | NodeTerminal { project, .. }
            | NodeRepl { project, .. }
            | TaskTerminal { project, .. }
            | NodeBrowser { project, .. }
            | TaskBrowser { project, .. }
            | Task { project, .. }
            | TaskChat { project, .. }
            | TaskChatRaw { project, .. }
            | TaskChatTurn { project, .. }
            | TaskChatEvent { project, .. }
            | TaskArtifact { project, .. }
            | JobTodos { project, .. }
            | HomeFeed { project, .. }
            | NodeTasks { project, .. }
            | NodeCalls { project, .. }
            | NodeWakes { project, .. }
            | NodeChecks { project, .. }
            | NodeBatches { project, .. }
            | NodeBatch { project, .. }
            | TaskBatches { project, .. }
            | TaskBatch { project, .. }
            | TaskChecks { project, .. }
            | NodeQuestions { project, .. }
            | NodeQuestion { project, .. }
            | NodePermissions { project, .. }
            | NodePermission { project, .. }
            | TaskPermissions { project, .. }
            | TaskPermission { project, .. }
            | NodeMessages { project, .. }
            | NodeProgress { project, .. }
            | TaskMessages { project, .. }
            | ProjectMessages { project, .. }
            | IssueMessages { project, .. }
            | Changed { project, .. }
            | IssueExecutions { project, .. }
            | IssueComments { project, .. }
            | IssueComment { project, .. }
            | IssueExecution { project, .. }
            | NodeDiff { project, .. }
            | NodeRebase { project, .. }
            | ProjectTerminal { project, .. }
            | ProjectBrowser { project, .. }
            | ProjectSkills { project, .. }
            | ProjectSkill { project, .. }
            | NodeMemories { project, .. }
            | NodeMemory { project, .. }
            | NodeEras { project, .. }
            | NodeEra { project, .. }
            | NodePages { project, .. }
            | NodePage { project, .. }
            | NodePageLog { project, .. }
            | ProjectRecipes { project, .. }
            | ProjectRecipe { project, .. }
            | ProjectServices { project, .. }
            | ProjectService { project, .. }
            | ProjectServiceStatus { project, .. }
            | ProjectServiceLog { project, .. }
            | ProjectPublish { project, .. }
            | ProjectPublication { project, .. }
            | ProjectPublicationLegLog { project, .. }
            | ProjectRoutes { project, .. }
            | ProjectRoute { project, .. }
            | ProjectRouteHistory { project, .. }
            | ProjectRouteHistoryEntry { project, .. }
            | ProjectEvents { project, .. }
            | ProjectEvent { project, .. }
            | ProjectEventEndpoints { project, .. }
            | ProjectEventEndpoint { project, .. }
            | ProjectEventDelivery { project, .. }
            | ProjectResponses { project, .. }
            | ProjectResponse { project, .. }
            | ProjectResponseHistory { project, .. }
            | ProjectResponseHistoryEntry { project, .. }
            | ProjectAgents { project, .. }
            | ProjectAgent { project, .. }
            | ProjectActions { project, .. }
            | ProjectAction { project, .. }
            | ProjectSettings { project, .. }
            | ProjectBrowserNetworkRequest { project, .. }
            | NodeBrowserNetworkRequest { project, .. }
            | TaskBrowserNetworkRequest { project, .. } => Some(project.as_str()),
            _ => None,
        }
    }

    pub fn project(&self) -> Option<&str> {
        match self {
            Self::ProjectSettings { project } => Some(project),
            Self::Settings | Self::Projects => None,
            Self::Project { project }
            | Self::ProjectImage { project, .. }
            | Self::ProjectImages { project, .. }
            | Self::ProjectAttachment { project, .. }
            | Self::ProjectAttachments { project, .. }
            | Self::ProjectIssues { project }
            | Self::ProjectPosts { project }
            | Self::ProjectTenets { project }
            | Self::ProjectTenet { project, .. }
            | Self::ProjectCamps { project }
            | Self::ProjectCamp { project, .. }
            | Self::ProjectCampParticipants { project, .. }
            | Self::ProjectCampPosts { project, .. }
            | Self::ProjectPages { project }
            | Self::ProjectCodemap { project }
            | Self::ProjectQueue { project }
            | Self::ProjectMap { project }
            | Self::ProjectHome { project }
            | Self::ProjectHeadlines { project }
            | Self::ProjectCheckResults { project, .. }
            | Self::ProjectCheckObservation { project, .. }
            | Self::ProjectVcsDiff { project }
            | Self::ProjectVcsAncestry { project }
            | Self::ProjectVcsLog { project }
            | Self::ProjectVcsBlame { project }
            | Self::Issue { project, .. }
            | Self::ProjectThreads { project, .. }
            | Self::Thread { project, .. }
            | Self::ThreadQueue { project, .. }
            | Self::ThreadDb { project, .. }
            | Self::ThreadDbSchema { project, .. }
            | Self::ThreadDbTable { project, .. }
            | Self::ThreadDbRow { project, .. }
            | Self::ThreadServices { project, .. }
            | Self::ThreadService { project, .. }
            | Self::ThreadServiceStatus { project, .. }
            | Self::ThreadServiceLog { project, .. }
            | Self::Node { project, .. }
            | Self::NodeChat { project, .. }
            | Self::NodeChatRaw { project, .. }
            | Self::NodeChatTurn { project, .. }
            | Self::NodeChatEvent { project, .. }
            | Self::NodeArtifact { project, .. }
            | Self::NodeTerminal { project, .. }
            | Self::NodeRepl { project, .. }
            | Self::TaskTerminal { project, .. }
            | Self::NodeBrowser { project, .. }
            | Self::NodeBrowserNetworkRequest { project, .. }
            | Self::TaskBrowser { project, .. }
            | Self::TaskBrowserNetworkRequest { project, .. }
            | Self::Task { project, .. }
            | Self::TaskChat { project, .. }
            | Self::TaskChatRaw { project, .. }
            | Self::TaskChatTurn { project, .. }
            | Self::TaskChatEvent { project, .. }
            | Self::TaskArtifact { project, .. }
            | Self::JobTodos { project, .. }
            | Self::HomeFeed { project, .. }
            | Self::NodeTasks { project, .. }
            | Self::NodeCalls { project, .. }
            | Self::NodeWakes { project, .. }
            | Self::NodeChecks { project, .. }
            | Self::NodeBatches { project, .. }
            | Self::NodeBatch { project, .. }
            | Self::TaskBatches { project, .. }
            | Self::TaskBatch { project, .. }
            | Self::TaskChecks { project, .. }
            | Self::NodeQuestions { project, .. }
            | Self::NodeQuestion { project, .. }
            | Self::NodePermissions { project, .. }
            | Self::NodePermission { project, .. }
            | Self::TaskPermissions { project, .. }
            | Self::TaskPermission { project, .. }
            | Self::NodeMessages { project, .. }
            | Self::NodeProgress { project, .. }
            | Self::TaskMessages { project, .. }
            | Self::ProjectMessages { project }
            | Self::IssueMessages { project, .. }
            | Self::IssueComments { project, .. }
            | Self::IssueComment { project, .. }
            | Self::Changed { project, .. }
            | Self::IssueExecutions { project, .. }
            | Self::IssueExecution { project, .. }
            | Self::NodeDiff { project, .. }
            | Self::NodeRebase { project, .. }
            | Self::ProjectTerminal { project, .. }
            | Self::ProjectBrowser { project, .. }
            | Self::ProjectBrowserNetworkRequest { project, .. }
            | Self::ProjectSkills { project }
            | Self::ProjectSkill { project, .. }
            | Self::ProjectReferences { project }
            | Self::ProjectReference { project, .. }
            | Self::NodeMemories { project, .. }
            | Self::NodeMemory { project, .. }
            | Self::NodeEras { project, .. }
            | Self::NodeEra { project, .. }
            | Self::NodePages { project, .. }
            | Self::NodePage { project, .. }
            | Self::NodePageLog { project, .. }
            | Self::ProjectRecipes { project }
            | Self::ProjectRecipe { project, .. }
            | Self::ProjectWorkflows { project }
            | Self::ProjectWorkflow { project, .. }
            | Self::ProjectServices { project }
            | Self::ProjectService { project, .. }
            | Self::ProjectServiceStatus { project, .. }
            | Self::ProjectServiceLog { project, .. }
            | Self::ProjectPublish { project }
            | Self::ProjectPublication { project, .. }
            | Self::ProjectPublicationLegLog { project, .. }
            | Self::ProjectRoutes { project }
            | Self::ProjectRoute { project, .. }
            | Self::ProjectRouteHistory { project, .. }
            | Self::ProjectRouteHistoryEntry { project, .. }
            | Self::ProjectEvents { project }
            | Self::ProjectEvent { project, .. }
            | Self::ProjectEventEndpoints { project }
            | Self::ProjectEventEndpoint { project, .. }
            | Self::ProjectEventDelivery { project, .. }
            | Self::ProjectResponses { project }
            | Self::ProjectResponse { project, .. }
            | Self::ProjectResponseHistory { project, .. }
            | Self::ProjectResponseHistoryEntry { project, .. }
            | Self::ProjectAgents { project }
            | Self::ProjectAgent { project, .. }
            | Self::ProjectActions { project }
            | Self::ProjectAction { project, .. }
            | Self::NodeSymbols { project, .. }
            | Self::ProjectSymbols { project, .. } => Some(project),
            Self::Posts
            | Self::Post { .. }
            | Self::PostComment { .. }
            | Self::Routes
            | Self::Route { .. }
            | Self::RouteHistory { .. }
            | Self::RouteHistoryEntry { .. }
            | Self::WorkspaceEvents
            | Self::WorkspaceEvent { .. }
            | Self::Packs
            | Self::Pack { .. }
            | Self::Skills
            | Self::Skill { .. }
            | Self::Labels
            | Self::Label { .. }
            | Self::Recipes
            | Self::Recipe { .. }
            | Self::Workflows
            | Self::Workflow { .. }
            | Self::Responses
            | Self::Response { .. }
            | Self::ResponseHistory { .. }
            | Self::ResponseHistoryEntry { .. }
            | Self::Agents
            | Self::Agent { .. }
            | Self::Actions
            | Self::Action { .. }
            | Self::Db
            | Self::DbTable { .. }
            | Self::DbRow { .. }
            | Self::Dev
            | Self::DevDb
            | Self::DevPid
            | Self::DevLaunchTicket
            | Self::Logs
            | Self::ChannelsConversations
            | Self::ChannelsAmbientFeedback
            | Self::Executors
            | Self::Executor { .. }
            | Self::Requests
            | Self::Request { .. }
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
            | Self::Inference
            | Self::InferenceProfiles
            | Self::InferenceDeployments
            | Self::InferenceDeployment { .. }
            | Self::InferencePools
            | Self::InferenceRequests
            | Self::InferenceModels
            | Self::InferenceModel { .. }
            | Self::InferenceProfile { .. }
            | Self::InferenceVerifications
            | Self::InferenceVerification { .. }
            | Self::InferenceModelFiles { .. }
            | Self::InferencePoolHardware { .. }
            | Self::InferenceFit
            | Self::InferenceTemplates
            | Self::InferenceTemplate { .. }
            | Self::InferenceGenerations
            | Self::InferenceGeneration { .. }
            | Self::Bug
            | Self::Rebuilds
            | Self::Rebuild { .. }
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
            | Self::NodeRepl { number, .. }
            | Self::TaskTerminal { number, .. }
            | Self::NodeBrowser { number, .. }
            | Self::NodeBrowserNetworkRequest { number, .. }
            | Self::TaskBrowser { number, .. }
            | Self::TaskBrowserNetworkRequest { number, .. }
            | Self::Task { number, .. }
            | Self::TaskChat { number, .. }
            | Self::TaskChatRaw { number, .. }
            | Self::TaskChatTurn { number, .. }
            | Self::TaskChatEvent { number, .. }
            | Self::TaskArtifact { number, .. }
            | Self::JobTodos { number, .. }
            | Self::HomeFeed { number, .. }
            | Self::NodeTasks { number, .. }
            | Self::NodeCalls { number, .. }
            | Self::NodeWakes { number, .. }
            | Self::NodeChecks { number, .. }
            | Self::NodeBatches { number, .. }
            | Self::NodeBatch { number, .. }
            | Self::TaskBatches { number, .. }
            | Self::TaskBatch { number, .. }
            | Self::TaskChecks { number, .. }
            | Self::NodeQuestions { number, .. }
            | Self::NodeQuestion { number, .. }
            | Self::NodePermissions { number, .. }
            | Self::NodePermission { number, .. }
            | Self::TaskPermissions { number, .. }
            | Self::TaskPermission { number, .. }
            | Self::NodeMessages { number, .. }
            | Self::NodeProgress { number, .. }
            | Self::TaskMessages { number, .. }
            | Self::IssueMessages { number, .. }
            | Self::IssueComments { number, .. }
            | Self::IssueComment { number, .. }
            | Self::Changed { number, .. }
            | Self::IssueExecutions { number, .. }
            | Self::IssueExecution { number, .. }
            | Self::NodeDiff { number, .. }
            | Self::NodeRebase { number, .. }
            | Self::NodeMemories { number, .. }
            | Self::NodeMemory { number, .. }
            | Self::NodeEras { number, .. }
            | Self::NodeEra { number, .. }
            | Self::NodePages { number, .. }
            | Self::NodePage { number, .. }
            | Self::NodePageLog { number, .. }
            | Self::NodeSymbols { number, .. } => Some(*number),
            Self::ProjectThreads { .. }
            | Self::Thread { .. }
            | Self::ThreadQueue { .. }
            | Self::ThreadDb { .. }
            | Self::ThreadDbSchema { .. }
            | Self::ThreadDbTable { .. }
            | Self::ThreadDbRow { .. }
            | Self::ThreadServices { .. }
            | Self::ThreadService { .. }
            | Self::ThreadServiceStatus { .. }
            | Self::ThreadServiceLog { .. }
            | Self::Project { .. }
            | Self::ProjectIssues { .. }
            | Self::Posts
            | Self::Post { .. }
            | Self::PostComment { .. }
            | Self::ProjectPosts { .. }
            | Self::ProjectTenets { .. }
            | Self::ProjectTenet { .. }
            | Self::ProjectCamps { .. }
            | Self::ProjectCamp { .. }
            | Self::ProjectCampParticipants { .. }
            | Self::ProjectCampPosts { .. }
            | Self::ProjectPages { .. }
            | Self::ProjectCodemap { .. }
            | Self::ProjectQueue { .. }
            | Self::ProjectMap { .. }
            | Self::ProjectHome { .. }
            | Self::ProjectHeadlines { .. }
            | Self::ProjectCheckResults { .. }
            | Self::ProjectVcsDiff { .. }
            | Self::ProjectVcsAncestry { .. }
            | Self::ProjectVcsLog { .. }
            | Self::ProjectVcsBlame { .. }
            | Self::ProjectMessages { .. }
            | Self::ProjectTerminal { .. }
            | Self::ProjectBrowser { .. }
            | Self::ProjectBrowserNetworkRequest { .. }
            | Self::Packs
            | Self::Pack { .. }
            | Self::Skills
            | Self::Skill { .. }
            | Self::ProjectSkills { .. }
            | Self::ProjectSkill { .. }
            | Self::ProjectReferences { .. }
            | Self::ProjectReference { .. }
            | Self::Labels
            | Self::Label { .. }
            | Self::Recipes
            | Self::Recipe { .. }
            | Self::ProjectRecipes { .. }
            | Self::ProjectRecipe { .. }
            | Self::Routes
            | Self::Route { .. }
            | Self::RouteHistory { .. }
            | Self::RouteHistoryEntry { .. }
            | Self::ProjectRoutes { .. }
            | Self::ProjectRoute { .. }
            | Self::ProjectRouteHistory { .. }
            | Self::ProjectRouteHistoryEntry { .. }
            | Self::ProjectEvents { .. }
            | Self::ProjectEvent { .. }
            | Self::ProjectEventEndpoints { .. }
            | Self::ProjectEventEndpoint { .. }
            | Self::ProjectEventDelivery { .. }
            | Self::WorkspaceEvents
            | Self::WorkspaceEvent { .. }
            | Self::Responses
            | Self::Response { .. }
            | Self::ResponseHistory { .. }
            | Self::ResponseHistoryEntry { .. }
            | Self::ProjectResponses { .. }
            | Self::ProjectResponse { .. }
            | Self::ProjectResponseHistory { .. }
            | Self::ProjectResponseHistoryEntry { .. }
            | Self::Agents
            | Self::Agent { .. }
            | Self::ProjectAgents { .. }
            | Self::ProjectAgent { .. }
            | Self::Actions
            | Self::Action { .. }
            | Self::ProjectActions { .. }
            | Self::ProjectAction { .. }
            | Self::Workflows
            | Self::Workflow { .. }
            | Self::ProjectWorkflows { .. }
            | Self::ProjectWorkflow { .. }
            | Self::ProjectServices { .. }
            | Self::ProjectService { .. }
            | Self::ProjectServiceStatus { .. }
            | Self::ProjectServiceLog { .. }
            | Self::ProjectPublish { .. }
            | Self::ProjectPublication { .. }
            | Self::ProjectPublicationLegLog { .. }
            | Self::ProjectSymbols { .. }
            | Self::Db
            | Self::DbTable { .. }
            | Self::DbRow { .. }
            | Self::Dev
            | Self::DevDb
            | Self::DevPid
            | Self::DevLaunchTicket
            | Self::Logs
            | Self::ChannelsConversations
            | Self::ChannelsAmbientFeedback
            | Self::Executors
            | Self::Executor { .. }
            | Self::Requests
            | Self::Request { .. }
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
            | Self::Inference
            | Self::InferenceProfiles
            | Self::InferenceDeployments
            | Self::InferenceDeployment { .. }
            | Self::InferencePools
            | Self::InferenceRequests
            | Self::InferenceModels
            | Self::InferenceModel { .. }
            | Self::InferenceProfile { .. }
            | Self::InferenceVerifications
            | Self::InferenceVerification { .. }
            | Self::InferenceModelFiles { .. }
            | Self::InferencePoolHardware { .. }
            | Self::InferenceFit
            | Self::InferenceTemplates
            | Self::InferenceTemplate { .. }
            | Self::InferenceGenerations
            | Self::InferenceGeneration { .. }
            | Self::Bug
            | Self::Rebuilds
            | Self::Rebuild { .. }
            | Self::Help
            | Self::WebSearch
            | Self::Mcp { .. } => None,
            Self::ProjectImage { .. }
            | Self::ProjectImages { .. }
            | Self::ProjectAttachment { .. }
            | Self::ProjectAttachments { .. }
            | Self::ProjectCheckObservation { .. } => None,
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
            | Self::NodeRepl { node_id, .. }
            | Self::TaskTerminal { node_id, .. }
            | Self::NodeBrowser { node_id, .. }
            | Self::NodeBrowserNetworkRequest { node_id, .. }
            | Self::TaskBrowser { node_id, .. }
            | Self::TaskBrowserNetworkRequest { node_id, .. }
            | Self::Task { node_id, .. }
            | Self::TaskChat { node_id, .. }
            | Self::TaskChatRaw { node_id, .. }
            | Self::TaskChatTurn { node_id, .. }
            | Self::TaskChatEvent { node_id, .. }
            | Self::TaskArtifact { node_id, .. }
            | Self::JobTodos { node_id, .. }
            | Self::HomeFeed { node_id, .. }
            | Self::NodeTasks { node_id, .. }
            | Self::NodeCalls { node_id, .. }
            | Self::NodeQuestions { node_id, .. }
            | Self::NodeQuestion { node_id, .. }
            | Self::NodePermissions { node_id, .. }
            | Self::NodePermission { node_id, .. }
            | Self::TaskPermissions { node_id, .. }
            | Self::TaskPermission { node_id, .. }
            | Self::NodeMessages { node_id, .. }
            | Self::NodeProgress { node_id, .. }
            | Self::TaskMessages { node_id, .. }
            | Self::NodeDiff { node_id, .. }
            | Self::NodeRebase { node_id, .. }
            | Self::NodeMemories { node_id, .. }
            | Self::NodeMemory { node_id, .. }
            | Self::NodeEras { node_id, .. }
            | Self::NodeEra { node_id, .. }
            | Self::NodePages { node_id, .. }
            | Self::NodePage { node_id, .. }
            | Self::NodePageLog { node_id, .. }
            | Self::NodeSymbols { node_id, .. } => Some(node_id),
            _ => None,
        }
    }
}
