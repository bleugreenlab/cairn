//! Field-projection query methods over `CairnResource`: the project key, issue
//! number, node id, and the optional UI navigation route.

use super::types::CairnResource;

impl CairnResource {
    /// The project key this resource is scoped to, if any.
    ///
    /// Project-scoped resources route to the database that owns their project
    /// (see `DbState::for_project`); global/workspace resources (skills, labels,
    /// settings, the projects collection, dev/db/logs, etc.) return `None` and
    /// stay on the private database.
    pub fn project_key(&self) -> Option<&str> {
        use CairnResource::*;
        match self {
            Project { project, .. }
            | ProjectIssues { project, .. }
            | Issue { project, .. }
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
            | NodeTasks { project, .. }
            | NodeCalls { project, .. }
            | NodeWakes { project, .. }
            | NodeChecks { project, .. }
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
            | ProjectTerminal { project, .. }
            | ProjectBrowser { project, .. }
            | ProjectSkills { project, .. }
            | ProjectSkill { project, .. }
            | NodeMemories { project, .. }
            | NodeMemory { project, .. }
            | ProjectRecipes { project, .. }
            | ProjectRecipe { project, .. }
            | ProjectAgents { project, .. }
            | ProjectAgent { project, .. }
            | ProjectActions { project, .. }
            | ProjectAction { project, .. }
            | ProjectSettings { project, .. } => Some(project.as_str()),
            _ => None,
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
            // A REPL is agent-only — no dedicated frontend route.
            Self::NodeRepl { .. } => None,
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
            Self::NodeBrowser {
                number,
                exec_seq,
                node_id,
                slug,
                ..
            } => Some(format!(
                "/p/{}/i/{}/{}/{}?browserId={}",
                project, number, exec_seq, node_id, slug
            )),
            Self::TaskBrowser {
                number,
                exec_seq,
                node_id,
                task_name,
                slug,
                ..
            } => Some(format!(
                "/p/{}/i/{}/{}/{}/task/{}?browserId={}",
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
            Self::ProjectBrowser { slug, .. } => {
                Some(format!("/p/{}/browser?browserId={}", project, slug))
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
            | Self::NodeCalls { .. }
            | Self::NodeWakes { .. }
            | Self::NodeChecks { .. }
            | Self::NodeQuestions { .. }
            | Self::NodeQuestion { .. }
            | Self::NodePermissions { .. }
            | Self::NodePermission { .. }
            | Self::TaskPermissions { .. }
            | Self::TaskPermission { .. }
            | Self::NodeMessages { .. }
            | Self::NodeProgress { .. }
            | Self::TaskMessages { .. }
            | Self::ProjectIssues { .. }
            | Self::ProjectMessages { .. }
            | Self::IssueMessages { .. }
            | Self::Changed { .. }
            | Self::IssueExecutions { .. }
            | Self::IssueComments { .. }
            | Self::IssueComment { .. }
            | Self::IssueExecution { .. }
            | Self::NodeDiff { .. }
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
            | Self::Dev
            | Self::DevDb
            | Self::DevPid
            | Self::Logs
            | Self::Bug
            | Self::Help
            | Self::WebSearch
            | Self::Workflows
            | Self::Workflow { .. }
            | Self::ProjectWorkflows { .. }
            | Self::ProjectWorkflow { .. }
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
            | Self::NodeRepl { project, .. }
            | Self::TaskTerminal { project, .. }
            | Self::NodeBrowser { project, .. }
            | Self::TaskBrowser { project, .. }
            | Self::Task { project, .. }
            | Self::TaskChat { project, .. }
            | Self::TaskChatRaw { project, .. }
            | Self::TaskChatTurn { project, .. }
            | Self::TaskChatEvent { project, .. }
            | Self::TaskArtifact { project, .. }
            | Self::JobTodos { project, .. }
            | Self::NodeTasks { project, .. }
            | Self::NodeCalls { project, .. }
            | Self::NodeWakes { project, .. }
            | Self::NodeChecks { project, .. }
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
            | Self::ProjectTerminal { project, .. }
            | Self::ProjectBrowser { project, .. }
            | Self::ProjectSkills { project }
            | Self::ProjectSkill { project, .. }
            | Self::ProjectReferences { project }
            | Self::ProjectReference { project, .. }
            | Self::NodeMemories { project, .. }
            | Self::NodeMemory { project, .. }
            | Self::ProjectRecipes { project }
            | Self::ProjectRecipe { project, .. }
            | Self::ProjectWorkflows { project }
            | Self::ProjectWorkflow { project, .. }
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
            | Self::Workflows
            | Self::Workflow { .. }
            | Self::Agents
            | Self::Agent { .. }
            | Self::Actions
            | Self::Action { .. }
            | Self::Db
            | Self::Dev
            | Self::DevDb
            | Self::DevPid
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
            | Self::NodeRepl { number, .. }
            | Self::TaskTerminal { number, .. }
            | Self::NodeBrowser { number, .. }
            | Self::TaskBrowser { number, .. }
            | Self::Task { number, .. }
            | Self::TaskChat { number, .. }
            | Self::TaskChatRaw { number, .. }
            | Self::TaskChatTurn { number, .. }
            | Self::TaskChatEvent { number, .. }
            | Self::TaskArtifact { number, .. }
            | Self::JobTodos { number, .. }
            | Self::NodeTasks { number, .. }
            | Self::NodeCalls { number, .. }
            | Self::NodeWakes { number, .. }
            | Self::NodeChecks { number, .. }
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
            | Self::NodeMemories { number, .. }
            | Self::NodeMemory { number, .. }
            | Self::NodeSymbols { number, .. } => Some(*number),
            Self::Project { .. }
            | Self::ProjectIssues { .. }
            | Self::ProjectMessages { .. }
            | Self::ProjectTerminal { .. }
            | Self::ProjectBrowser { .. }
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
            | Self::ProjectSymbols { .. }
            | Self::Db
            | Self::Dev
            | Self::DevDb
            | Self::DevPid
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
            | Self::NodeRepl { node_id, .. }
            | Self::TaskTerminal { node_id, .. }
            | Self::NodeBrowser { node_id, .. }
            | Self::TaskBrowser { node_id, .. }
            | Self::Task { node_id, .. }
            | Self::TaskChat { node_id, .. }
            | Self::TaskChatRaw { node_id, .. }
            | Self::TaskChatTurn { node_id, .. }
            | Self::TaskChatEvent { node_id, .. }
            | Self::TaskArtifact { node_id, .. }
            | Self::JobTodos { node_id, .. }
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
            | Self::NodeMemories { node_id, .. }
            | Self::NodeMemory { node_id, .. }
            | Self::NodeSymbols { node_id, .. } => Some(node_id),
            _ => None,
        }
    }
}
