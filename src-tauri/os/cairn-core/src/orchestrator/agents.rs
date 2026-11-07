//! Orchestrator agent configuration operations.
//!
//! Consolidates scope resolution logic for agent configs:
//! workspace-scoped (`~/.cairn/agents/`) vs project-scoped (`[project]/.cairn/agents/`).

use cairn_config::library::{LibraryRoots, Origin};
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
        models: agent.models,
        workspace_id,
        project_id,
        created_at: now,
        updated_at: now,
        disallowed_tools: agent.disallowed_tools,
        skills: agent.skills,
        fence: agent.fence,
        icon: agent.icon,
        // Authoring/display config: no model is chosen until a job launches.
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
        config_agents::list_agents(&LibraryRoots::at(config_dir, project_path))
    }

    fn get_file(
        config_dir: &Path,
        id: &str,
        project_path: Option<&Path>,
    ) -> Result<Option<Self::File>, String> {
        config_agents::get_agent(&LibraryRoots::at(config_dir, project_path), id)
    }

    fn save_file(
        config_dir: &Path,
        file: &Self::File,
        project_path: Option<&Path>,
        layer: Option<&Origin>,
    ) -> Result<PathBuf, String> {
        config_agents::save_agent(&LibraryRoots::at(config_dir, project_path), file, layer)
    }

    fn delete_file(
        config_dir: &Path,
        id: &str,
        project_path: Option<&Path>,
    ) -> Result<Option<PathBuf>, String> {
        config_agents::delete_agent(&LibraryRoots::at(config_dir, project_path), id, None)
    }

    fn file_origin(file: &Self::File) -> Origin {
        file.origin.clone()
    }

    fn file_is_project_scoped(file: &Self::File) -> bool {
        file.is_project_scoped()
    }

    fn file_bundles(file: &Self::File) -> &[String] {
        &file.bundles
    }

    fn package_kind() -> crate::config::contextual_packages::ContextualPackageKind {
        crate::config::contextual_packages::ContextualPackageKind::Agent
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
        origin: Origin,
        _now: i64,
    ) -> Self::File {
        config_agents::FileAgent {
            id,
            name: input.name,
            description: input.description,
            prompt: input.prompt,
            tools: input.tools,
            models: input.models,
            fence: input.fence.or_else(|| {
                Some(crate::models::Fence::from_legacy(
                    input.sandbox,
                    input.on_escape,
                ))
            }),
            disallowed_tools: input.disallowed_tools,
            skills: input.skills,
            hooks: None,
            icon: input.icon,
            bundles: Vec::new(),
            origin,
            file_path: PathBuf::new(),
            seed_budget_tokens: None,
            seed_eras: None,
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
            _ => existing.is_project_scoped(),
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
        new_origin: Origin,
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
            models: input.models.unwrap_or_else(|| existing.models.clone()),
            fence: merge_optional(&existing.fence, &input.fence),
            disallowed_tools: merge_optional(&existing.disallowed_tools, &input.disallowed_tools),
            skills: merge_optional(&existing.skills, &input.skills),
            hooks: existing.hooks.clone(),
            icon: merge_optional(&existing.icon, &input.icon),
            bundles: existing.bundles.clone(),
            origin: new_origin,
            file_path: if scope_changing {
                PathBuf::new()
            } else {
                existing.file_path.clone()
            },
            // No editor exposes these; an update keeps what the file authored.
            seed_budget_tokens: existing.seed_budget_tokens,
            seed_eras: existing.seed_eras,
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
        origin: Origin,
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
            models: source.models.clone(),
            fence: source.fence,
            disallowed_tools: source.disallowed_tools.clone(),
            skills: source.skills.clone(),
            hooks: source.hooks.clone(),
            icon: source.icon.clone(),
            bundles: source.bundles.clone(),
            origin,
            file_path: PathBuf::new(),
            seed_budget_tokens: None,
            seed_eras: None,
        }
    }

    fn post_save_copy(_source: &Self::File, _dest_path: &Path) -> Result<(), String> {
        Ok(())
    }

    fn primary_path(file: &Self::File) -> PathBuf {
        file.file_path.clone()
    }

    fn stage_paths(write_root: &Path, primary_path: &Path) -> Vec<PathBuf> {
        config_agents::agent_stage_paths(&LibraryRoots::workspace(write_root), primary_path)
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
        workspace_id: Option<&str>,
    ) -> Result<AgentConfig, String> {
        self.update_agent_config_in_layer(id, input, target_project_id, workspace_id, None)
    }

    /// Update an agent, saving an edit to a pack's agent as the user's own
    /// definition in `target_layer` (`workspace` or `project`).
    pub fn update_agent_config_in_layer(
        &self,
        id: &str,
        input: UpdateAgentConfig,
        target_project_id: Option<&str>,
        workspace_id: Option<&str>,
        target_layer: Option<&str>,
    ) -> Result<AgentConfig, String> {
        config_resource::update_config::<AgentResource>(
            self,
            id,
            input,
            target_project_id,
            workspace_id,
            target_layer,
        )
    }

    /// Delete an agent configuration.
    pub fn delete_agent_config(
        &self,
        id: &str,
        project_id: Option<&str>,
        workspace_id: Option<&str>,
    ) -> Result<(), String> {
        config_resource::delete_config::<AgentResource>(self, id, project_id, workspace_id)
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
        target_workspace_id: Option<&str>,
    ) -> Result<AgentConfig, String> {
        config_resource::copy_config::<AgentResource>(
            self,
            source_id,
            source_project_id,
            target_id,
            target_project_id,
            target_workspace_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::db::DbState;
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use std::sync::Arc;

    async fn build_orch(config_dir: std::path::PathBuf) -> Orchestrator {
        let local =
            LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("orch.db"))
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

        let invalid = orch.list_invalid_configs(None, None, None).unwrap();
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
            models: vec![cairn_config::allotment::AllotmentEntry::Pool("xl".into())],
            workspace_id: Some("default".to_string()),
            project_id: None,
            disallowed_tools: None,
            skills: None,
            fence: None,
            on_escape: None,
            sandbox: None,
            icon: None,
        })
        .unwrap();

        let valid = orch.list_agent_configs(None, None).unwrap();
        assert_eq!(valid.len(), 1);
        assert_eq!(valid[0].id, "my-agent");
        assert!(valid[0].tools.is_empty());

        let invalid = orch.list_invalid_configs(None, None, None).unwrap();
        assert!(invalid.is_empty(), "UI-created agent must not be invalid");
    }

    /// The editor's exact save payload for a pin-only change: every field the
    /// form round-trips (trimmed prompt, explicit default fence, unchanged
    /// tools) plus the one key the user touched.
    fn editor_pin_update(
        agent: &crate::models::AgentConfig,
        models: &str,
        icon: Option<&str>,
    ) -> crate::models::UpdateAgentConfig {
        crate::models::UpdateAgentConfig {
            name: Some(agent.name.clone()),
            description: Some(agent.description.trim().to_string()),
            prompt: Some(agent.prompt.trim().to_string()),
            tools: Some(agent.tools.clone()),
            models: Some(vec![
                cairn_config::allotment::AllotmentEntry::parse(models).unwrap()
            ]),
            disallowed_tools: None,
            skills: None,
            workspace_id: Some(Some("default".to_string())),
            project_id: Some(None),
            fence: Some(Some(agent.fence.unwrap_or_default())),
            icon: Some(icon.map(str::to_string)),
        }
    }

    fn named(agent: &crate::models::AgentConfig) -> Vec<String> {
        agent.models.iter().map(ToString::to_string).collect()
    }

    /// Acceptance for cairn/4993, read in place: pinning `models: [claude/opus]`
    /// / `icon: hammer` on the shipped Build agent through the editor leaves the
    /// pack's `agents/build.md` byte-identical with the pin in effect, and the
    /// next pack update advances the prompt while the pin holds. A body edit is
    /// refused unless it names a layer to define the agent in front of the
    /// pack's, and pins apply over that definition too.
    #[tokio::test]
    async fn a_pinned_allotment_keeps_a_shipped_agent_updating_in_place() {
        use crate::services::{RealFileSystem, RealGitClient};
        use crate::workspace::bundle::sync_workspace_packs;

        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let resources = temp.path().join("resources");
        let core = resources.join("packs/core");
        let ship = |prompt: &str| {
            std::fs::create_dir_all(core.join("agents")).unwrap();
            std::fs::write(
                core.join("cairn-pack.yaml"),
                "id: core\nname: Core\nversion: 1.0.0\ndefault: true\n",
            )
            .unwrap();
            std::fs::write(
                core.join("agents/build.md"),
                format!(
                    "---\nname: Build\nbundles: [coding]\ndescription: Implements features and fixes\ntools: mcp__cairn__read, mcp__cairn__write, mcp__cairn__run\nmodels: [lg]\n---\n\n{prompt}\n"
                ),
            )
            .unwrap();
        };
        let sync = || {
            sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &home, "1.0.0")
                .unwrap()
        };
        let file = || std::fs::read_to_string(home.join("packs/core/agents/build.md")).unwrap();

        ship("# Implementation Mode\n\nPrompt v1.");
        sync();
        let shipped_v1 = file();
        let orch = build_orch(home.clone()).await;

        let build = orch.get_agent_config("build", None).unwrap().unwrap();
        orch.update_agent_config(
            "build",
            editor_pin_update(&build, "claude/opus", Some("hammer")),
            None,
            Some("default"),
        )
        .unwrap();

        assert_eq!(file(), shipped_v1, "a pin must not touch the pack's file");
        assert!(
            !home.join("agents/build.md").exists(),
            "nor create a definition"
        );
        let pinned = orch.get_agent_config("build", None).unwrap().unwrap();
        assert_eq!(named(&pinned), ["claude/opus"]);
        assert_eq!(pinned.icon.as_deref(), Some("hammer"));

        ship("# Implementation Mode\n\nPrompt v2.");
        sync();
        let updated = orch.get_agent_config("build", None).unwrap().unwrap();
        assert!(
            updated.prompt.contains("Prompt v2."),
            "the pack update lands"
        );
        assert_eq!(named(&updated), ["claude/opus"]);

        // A body edit of the pack's agent is refused without a layer.
        let mut edit = editor_pin_update(&updated, "claude/opus", Some("hammer"));
        edit.prompt = Some("My own prompt.".to_string());
        let refusal = orch
            .update_agent_config("build", edit, None, Some("default"))
            .unwrap_err();
        assert!(refusal.contains("pack items are never edited"), "{refusal}");
        assert!(file().contains("Prompt v2."));

        // Choosing the file's own value again un-pins.
        orch.update_agent_config(
            "build",
            editor_pin_update(&updated, "lg", None),
            None,
            Some("default"),
        )
        .unwrap();
        let unpinned = orch.get_agent_config("build", None).unwrap().unwrap();
        assert_eq!(named(&unpinned), ["lg"]);
        assert!(cairn_config::agent_pins::load_pins(&home)
            .get("build")
            .is_none());

        // Saving the edit as the workspace's own definition puts a file in
        // front of the pack's, which stays as the pack shipped it.
        let mut edit = editor_pin_update(&unpinned, "lg", None);
        edit.prompt = Some("My own prompt.".to_string());
        let mine = orch
            .update_agent_config_in_layer("build", edit, None, Some("default"), Some("workspace"))
            .unwrap();
        assert!(mine.prompt.contains("My own prompt."));
        assert!(std::fs::read_to_string(home.join("agents/build.md"))
            .unwrap()
            .contains("My own prompt."));
        assert!(
            file().contains("Prompt v2."),
            "the pack's file is untouched"
        );
        let resolved = orch.get_agent_config("build", None).unwrap().unwrap();
        assert!(resolved.prompt.contains("My own prompt."));

        // A layer is chosen only over a pack's item.
        let again = orch
            .update_agent_config_in_layer(
                "build",
                editor_pin_update(&resolved, "lg", None),
                None,
                Some("default"),
                Some("workspace"),
            )
            .unwrap_err();
        assert!(again.contains("already your"), "{again}");
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
                         VALUES ('p1', 'default', 'Proj', 'proj', ?1, 1, 1, 0)",
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

        let package_ref = crate::config::contextual_packages::ContextualPackageRef {
            kind: crate::config::contextual_packages::ContextualPackageKind::Agent,
            id: "planner".to_string(),
        };
        let mut settings =
            crate::config::project_settings::load_project_settings_read_only(project_dir.path());
        settings
            .contextual_packages
            .get_or_insert_default()
            .disable(package_ref.clone());
        crate::config::project_settings::save_project_settings(project_dir.path(), &settings)
            .unwrap();
        assert!(!orch
            .list_agents_for_context("p1")
            .unwrap()
            .iter()
            .any(|a| a.id == "planner"));

        let mut settings =
            crate::config::project_settings::load_project_settings_read_only(project_dir.path());
        settings
            .contextual_packages
            .get_or_insert_default()
            .remove_disabled(&package_ref);
        crate::config::project_settings::save_project_settings(project_dir.path(), &settings)
            .unwrap();
        assert!(orch
            .list_agents_for_context("p1")
            .unwrap()
            .iter()
            .any(|a| a.id == "planner"));
    }
}
