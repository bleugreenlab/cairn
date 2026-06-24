//! Orchestrator agent configuration operations.
//!
//! Consolidates scope resolution logic for agent configs:
//! workspace-scoped (`~/.cairn/agents/`) vs project-scoped (`[project]/.cairn/agents/`).

use std::path::{Path, PathBuf};

use crate::config::{agents as config_agents, ConfigResult};
use crate::models::{AgentConfig, CreateAgentConfig, UpdateAgentConfig};

use super::config_resource::{self, merge_optional, ConfigResource};
use super::Orchestrator;

/// Convert a file agent to the API model.
fn file_agent_to_config(
    agent: config_agents::FileAgent,
    workspace_id: Option<String>,
    project_id: Option<String>,
    now: i64,
) -> AgentConfig {
    let now = now as i32;
    AgentConfig {
        id: agent.id,
        name: agent.name,
        description: agent.description,
        prompt: agent.prompt,
        tools: agent.tools,
        tier: agent.tier,
        workspace_id,
        project_id,
        created_at: now,
        updated_at: now,
        disallowed_tools: agent.disallowed_tools,
        skills: agent.skills,
        fence: agent.fence,
        backend_preference: agent.backend_preference,
        // Authoring/display config: no resolved selection until a launch resolves it.
        selection: None,
        extras: None,
    }
}

pub(crate) struct AgentResource;

impl ConfigResource for AgentResource {
    type Config = AgentConfig;
    type File = config_agents::FileAgent;
    type CreateInput = CreateAgentConfig;
    type UpdateInput = UpdateAgentConfig;

    const ENTITY_TYPE: &'static str = "agent";
    const TABLE: &'static str = "agent_configs";
    const SUBDIR: &'static str = "agents";

    fn list_files(
        config_dir: &Path,
        project_path: Option<&Path>,
    ) -> Result<Vec<ConfigResult<Self::File>>, String> {
        config_agents::list_agents(config_dir, project_path)
    }

    fn get_file(
        config_dir: &Path,
        id: &str,
        project_path: Option<&Path>,
    ) -> Result<Option<Self::File>, String> {
        config_agents::get_agent(config_dir, id, project_path)
    }

    fn save_file(
        config_dir: &Path,
        file: &Self::File,
        project_path: Option<&Path>,
    ) -> Result<PathBuf, String> {
        config_agents::save_agent(config_dir, file, project_path)
    }

    fn delete_file(config_dir: &Path, id: &str, project_path: Option<&Path>) -> Result<(), String> {
        config_agents::delete_agent(config_dir, id, project_path)
    }

    fn file_is_project_scoped(file: &Self::File) -> bool {
        file.is_project_scoped
    }

    fn to_config(
        file: Self::File,
        workspace_id: Option<String>,
        project_id: Option<String>,
        now: i64,
    ) -> Self::Config {
        file_agent_to_config(file, workspace_id, project_id, now)
    }

    fn config_name(cfg: &Self::Config) -> &str {
        &cfg.name
    }

    fn config_id(cfg: &Self::Config) -> String {
        cfg.id.clone()
    }

    fn create_id(input: &Self::CreateInput) -> String {
        input.id.clone()
    }

    fn create_input_scopes(input: &Self::CreateInput) -> (Option<String>, Option<String>) {
        (input.workspace_id.clone(), input.project_id.clone())
    }

    fn build_create_file(
        input: Self::CreateInput,
        id: String,
        is_project_scoped: bool,
        _now: i64,
    ) -> Self::File {
        config_agents::FileAgent {
            id,
            name: input.name,
            description: input.description,
            prompt: input.prompt,
            tools: input.tools,
            tier: input.tier,
            fence: input.fence.or_else(|| {
                Some(crate::models::Fence::from_legacy(
                    input.sandbox,
                    input.on_escape,
                ))
            }),
            backend_preference: input.backend_preference,
            disallowed_tools: input.disallowed_tools,
            skills: input.skills,
            hooks: None,
            is_project_scoped,
            file_path: PathBuf::new(),
        }
    }

    fn update_scope(
        existing: &Self::File,
        input: &Self::UpdateInput,
        _target_project_id: Option<&str>,
    ) -> (bool, Option<String>) {
        let is_project_scoped = match (&input.workspace_id, &input.project_id) {
            (Some(Some(_)), _) => false,
            (_, Some(Some(_))) => true,
            _ => existing.is_project_scoped,
        };
        let project_id = match &input.project_id {
            Some(Some(pid)) => Some(pid.clone()),
            _ => None,
        };
        (is_project_scoped, project_id)
    }

    fn build_update_file(
        existing: &Self::File,
        input: Self::UpdateInput,
        id: &str,
        new_is_project_scoped: bool,
        scope_changing: bool,
        _now: i64,
    ) -> Self::File {
        config_agents::FileAgent {
            id: id.to_string(),
            name: input.name.unwrap_or_else(|| existing.name.clone()),
            description: input
                .description
                .unwrap_or_else(|| existing.description.clone()),
            prompt: input.prompt.unwrap_or_else(|| existing.prompt.clone()),
            tools: input.tools.unwrap_or_else(|| existing.tools.clone()),
            tier: merge_optional(&existing.tier, &input.tier),
            fence: merge_optional(&existing.fence, &input.fence),
            backend_preference: merge_optional(
                &existing.backend_preference,
                &input.backend_preference,
            ),
            disallowed_tools: merge_optional(&existing.disallowed_tools, &input.disallowed_tools),
            skills: merge_optional(&existing.skills, &input.skills),
            hooks: existing.hooks.clone(),
            is_project_scoped: new_is_project_scoped,
            file_path: if scope_changing {
                PathBuf::new()
            } else {
                existing.file_path.clone()
            },
        }
    }

    fn cleanup_after_scope_change(existing: &Self::File, _dest_path: &Path) {
        if let Err(e) = std::fs::remove_file(&existing.file_path) {
            log::warn!(
                "Failed to delete old agent file {:?}: {}",
                existing.file_path,
                e
            );
        }
    }

    fn target_exists(scope_root: &Path, id: &str, is_project_scoped: bool) -> bool {
        let root = if is_project_scoped {
            scope_root.join(".cairn")
        } else {
            scope_root.to_path_buf()
        };
        root.join(Self::SUBDIR).join(format!("{}.md", id)).exists()
    }

    fn build_scoped_copy(
        source: &Self::File,
        id: &str,
        is_project_scoped: bool,
        _workspace_id: Option<String>,
        _project_id: Option<String>,
        _now: i64,
    ) -> Self::File {
        config_agents::FileAgent {
            id: id.to_string(),
            name: source.name.clone(),
            description: source.description.clone(),
            prompt: source.prompt.clone(),
            tools: source.tools.clone(),
            tier: source.tier.clone(),
            fence: source.fence,
            backend_preference: source.backend_preference.clone(),
            disallowed_tools: source.disallowed_tools.clone(),
            skills: source.skills.clone(),
            hooks: source.hooks.clone(),
            is_project_scoped,
            file_path: PathBuf::new(),
        }
    }

    fn post_save_copy(_source: &Self::File, _dest_path: &Path) -> Result<(), String> {
        Ok(())
    }

    fn primary_path(file: &Self::File) -> PathBuf {
        file.file_path.clone()
    }
}

impl Orchestrator {
    /// List agent configurations with optional scope filters.
    ///
    /// - Both `None`: load from ALL scopes (workspace + all projects)
    /// - `workspace_id` set: workspace agents only
    /// - `project_id` set: project agents only
    pub fn list_agent_configs(
        &self,
        workspace_id: Option<&str>,
        project_id: Option<&str>,
    ) -> Result<Vec<AgentConfig>, String> {
        config_resource::list_configs::<AgentResource>(self, workspace_id, project_id)
    }

    /// Get a single agent configuration by ID.
    pub fn get_agent_config(
        &self,
        id: &str,
        project_id: Option<&str>,
    ) -> Result<Option<AgentConfig>, String> {
        config_resource::get_config::<AgentResource>(self, id, project_id)
    }

    /// Create a new agent configuration.
    pub fn create_agent_config(&self, input: CreateAgentConfig) -> Result<AgentConfig, String> {
        config_resource::create_config::<AgentResource>(self, input)
    }

    /// Update an existing agent configuration.
    pub fn update_agent_config(
        &self,
        id: &str,
        input: UpdateAgentConfig,
        target_project_id: Option<&str>,
    ) -> Result<AgentConfig, String> {
        config_resource::update_config::<AgentResource>(self, id, input, target_project_id)
    }

    /// Delete an agent configuration.
    pub fn delete_agent_config(&self, id: &str, project_id: Option<&str>) -> Result<(), String> {
        config_resource::delete_config::<AgentResource>(self, id, project_id)
    }

    /// List agents for a project context with workspace→project shadowing.
    pub fn list_agents_for_context(&self, project_id: &str) -> Result<Vec<AgentConfig>, String> {
        config_resource::list_for_context::<AgentResource>(self, project_id)
    }

    /// Copy an agent from any scope into a target scope under a chosen id.
    pub fn copy_agent_config(
        &self,
        source_id: &str,
        source_project_id: Option<&str>,
        target_id: &str,
        target_project_id: Option<&str>,
    ) -> Result<AgentConfig, String> {
        config_resource::copy_config::<AgentResource>(
            self,
            source_id,
            source_project_id,
            target_id,
            target_project_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::db::DbState;
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use std::sync::Arc;

    async fn build_orch(config_dir: std::path::PathBuf) -> Orchestrator {
        let local = LocalDb::open(tempfile::tempdir().unwrap().keep().join("orch.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(Arc::new(local), search));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db, services, config_dir).build()
    }

    /// A config file the loader rejects must not vanish: it stays out of the
    /// valid list but surfaces through `list_invalid_configs` with its error.
    #[tokio::test]
    async fn invalid_agent_file_surfaces_through_list_path() {
        let config_dir = tempfile::tempdir().unwrap();
        let agents_dir = config_dir.path().join("agents");
        std::fs::create_dir_all(&agents_dir).unwrap();
        std::fs::write(
            agents_dir.join("good.md"),
            "---\nname: Good\ndescription: ok\ntools: Read\n---\n\nprompt",
        )
        .unwrap();
        std::fs::write(agents_dir.join("broken.md"), "no frontmatter here").unwrap();

        let orch = build_orch(config_dir.path().to_path_buf()).await;

        let valid = orch.list_agent_configs(None, None).unwrap();
        assert_eq!(valid.len(), 1, "only the valid agent should list");
        assert_eq!(valid[0].id, "good");

        let invalid = orch.list_invalid_configs(None, None).unwrap();
        assert_eq!(invalid.len(), 1, "the broken agent must surface as invalid");
        assert_eq!(invalid[0].id, "broken");
        assert_eq!(invalid[0].entity_type, "agent");
        assert!(invalid[0].project_id.is_none());
        assert!(
            !invalid[0].error.is_empty(),
            "invalid entry must carry the parse error"
        );
    }

    /// An agent the Settings form writes (empty tools) round-trips: it lists
    /// normally and never appears as invalid.
    #[tokio::test]
    async fn ui_created_empty_tools_agent_lists_without_invalid() {
        use crate::models::CreateAgentConfig;

        let config_dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(config_dir.path().join("agents")).unwrap();
        let orch = build_orch(config_dir.path().to_path_buf()).await;

        orch.create_agent_config(CreateAgentConfig {
            id: "my-agent".to_string(),
            name: "My Agent".to_string(),
            description: "desc".to_string(),
            prompt: "prompt".to_string(),
            tools: vec![],
            tier: Some(crate::models::Model::new("xl")),
            workspace_id: Some("default".to_string()),
            project_id: None,
            disallowed_tools: None,
            skills: None,
            fence: None,
            on_escape: None,
            sandbox: None,
            backend_preference: None,
        })
        .unwrap();

        let valid = orch.list_agent_configs(None, None).unwrap();
        assert_eq!(valid.len(), 1);
        assert_eq!(valid[0].id, "my-agent");
        assert!(valid[0].tools.is_empty());

        let invalid = orch.list_invalid_configs(None, None).unwrap();
        assert!(invalid.is_empty(), "UI-created agent must not be invalid");
    }

    /// An inherited workspace agent disabled for one project drops out of that
    /// project's effective set without redefinition, and re-enabling restores it.
    #[tokio::test]
    async fn disabled_inherited_agent_drops_from_context_and_returns_on_enable() {
        let config_dir = tempfile::tempdir().unwrap();
        let agents_dir = config_dir.path().join("agents");
        std::fs::create_dir_all(&agents_dir).unwrap();
        std::fs::write(
            agents_dir.join("planner.md"),
            "---\nname: Planner\ndescription: plans\ntools: Read\n---\n\nprompt",
        )
        .unwrap();

        let orch = build_orch(config_dir.path().to_path_buf()).await;

        // A non-workspace project whose repo path resolves for list_for_context.
        let project_dir = tempfile::tempdir().unwrap();
        let repo_path = project_dir.path().to_string_lossy().to_string();
        orch.db
            .local
            .write(|conn| {
                let repo_path = repo_path.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                         VALUES ('p1', 'default', 'Proj', 'PROJ', ?1, 1, 1, 0)",
                        (repo_path.as_str(),),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        assert!(orch
            .list_agents_for_context("p1")
            .unwrap()
            .iter()
            .any(|a| a.id == "planner"));

        crate::config_disables::disable_config(&orch.db.local, "p1", "agent", "planner")
            .await
            .unwrap();
        assert!(!orch
            .list_agents_for_context("p1")
            .unwrap()
            .iter()
            .any(|a| a.id == "planner"));

        crate::config_disables::enable_config(&orch.db.local, "p1", "agent", "planner")
            .await
            .unwrap();
        assert!(orch
            .list_agents_for_context("p1")
            .unwrap()
            .iter()
            .any(|a| a.id == "planner"));
    }
}
