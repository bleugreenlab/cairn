//! Orchestrator skill configuration operations.
//!
//! Mirrors the agent pattern for skills — workspace vs project scope resolution.

use std::path::{Path, PathBuf};

use crate::config::{skills as config_skills, slugify, ConfigResult};
use crate::models::{CreateSkillConfig, SkillConfig, UpdateSkillConfig};

use super::config_resource::{self, merge_optional, ConfigResource};
use super::Orchestrator;

/// Convert a file skill to the API model.
fn file_skill_to_config(
    skill: config_skills::FileSkill,
    workspace_id: Option<String>,
    project_id: Option<String>,
    now: i64,
) -> SkillConfig {
    let now = now as i32;
    SkillConfig {
        id: skill.id,
        name: skill.name,
        description: skill.description,
        prompt: skill.prompt,
        allowed_tools: skill.allowed_tools,
        workspace_id,
        project_id,
        created_at: now,
        updated_at: now,
    }
}

pub(crate) struct SkillResource;

impl ConfigResource for SkillResource {
    type Config = SkillConfig;
    type File = config_skills::FileSkill;
    type CreateInput = CreateSkillConfig;
    type UpdateInput = UpdateSkillConfig;

    const ENTITY_TYPE: &'static str = "skill";
    const TABLE: &'static str = "skill_configs";
    const SUBDIR: &'static str = "skills";

    fn list_files(
        config_dir: &Path,
        project_path: Option<&Path>,
    ) -> Result<Vec<ConfigResult<Self::File>>, String> {
        config_skills::list_skills(config_dir, project_path)
    }

    fn get_file(
        config_dir: &Path,
        id: &str,
        project_path: Option<&Path>,
    ) -> Result<Option<Self::File>, String> {
        config_skills::get_skill(config_dir, id, project_path)
    }

    fn save_file(
        config_dir: &Path,
        file: &Self::File,
        project_path: Option<&Path>,
    ) -> Result<PathBuf, String> {
        config_skills::save_skill(config_dir, file, project_path, None)
    }

    fn delete_file(config_dir: &Path, id: &str, project_path: Option<&Path>) -> Result<(), String> {
        config_skills::delete_skill(config_dir, id, project_path)
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
        file_skill_to_config(file, workspace_id, project_id, now)
    }

    fn config_name(cfg: &Self::Config) -> &str {
        &cfg.name
    }

    fn config_id(cfg: &Self::Config) -> String {
        cfg.id.clone()
    }

    fn create_id(input: &Self::CreateInput) -> String {
        input.id.clone().unwrap_or_else(|| slugify(&input.name))
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
        config_skills::FileSkill {
            id,
            name: input.name,
            description: input.description,
            prompt: input.prompt,
            allowed_tools: input.allowed_tools,
            is_project_scoped,
            file_path: PathBuf::new(),
            dir_path: PathBuf::new(),
            meta: None,
            has_references: false,
            has_scripts: false,
            has_assets: false,
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
        config_skills::FileSkill {
            id: id.to_string(),
            name: input.name.unwrap_or_else(|| existing.name.clone()),
            description: input
                .description
                .unwrap_or_else(|| existing.description.clone()),
            prompt: input.prompt.unwrap_or_else(|| existing.prompt.clone()),
            allowed_tools: merge_optional(&existing.allowed_tools, &input.allowed_tools),
            is_project_scoped: new_is_project_scoped,
            file_path: if scope_changing {
                PathBuf::new()
            } else {
                existing.file_path.clone()
            },
            dir_path: if scope_changing {
                PathBuf::new()
            } else {
                existing.dir_path.clone()
            },
            meta: existing.meta.clone(),
            has_references: existing.has_references,
            has_scripts: existing.has_scripts,
            has_assets: existing.has_assets,
        }
    }

    fn cleanup_after_scope_change(existing: &Self::File, dest_path: &Path) {
        if let Some(dest_dir) = dest_path.parent() {
            if let Err(e) = config_skills::copy_skill_package(&existing.dir_path, dest_dir) {
                log::warn!("Failed to copy skill package contents: {}", e);
            }
        }
        if existing.dir_path.exists() {
            if let Err(e) = std::fs::remove_dir_all(&existing.dir_path) {
                log::warn!(
                    "Failed to delete old skill directory {:?}: {}",
                    existing.dir_path,
                    e
                );
            }
        }
    }

    fn target_exists(scope_root: &Path, id: &str, is_project_scoped: bool) -> bool {
        let root = if is_project_scoped {
            scope_root.join(".cairn")
        } else {
            scope_root.to_path_buf()
        };
        root.join(Self::SUBDIR).join(id).join("SKILL.md").exists()
    }

    fn build_scoped_copy(
        source: &Self::File,
        id: &str,
        is_project_scoped: bool,
        _workspace_id: Option<String>,
        _project_id: Option<String>,
        _now: i64,
    ) -> Self::File {
        config_skills::FileSkill {
            id: id.to_string(),
            name: source.name.clone(),
            description: source.description.clone(),
            prompt: source.prompt.clone(),
            allowed_tools: source.allowed_tools.clone(),
            is_project_scoped,
            file_path: PathBuf::new(),
            dir_path: PathBuf::new(),
            meta: None,
            has_references: false,
            has_scripts: false,
            has_assets: false,
        }
    }

    fn post_save_copy(source: &Self::File, dest_path: &Path) -> Result<(), String> {
        if let Some(dest_dir) = dest_path.parent() {
            config_skills::copy_skill_package(&source.dir_path, dest_dir)?;
        }
        Ok(())
    }

    fn primary_path(file: &Self::File) -> PathBuf {
        file.file_path.clone()
    }

    fn stage_paths(primary_path: &Path) -> Vec<PathBuf> {
        // primary_path is the skill's SKILL.md; stage the enclosing directory so
        // .meta.json and package subdirs (references/scripts/assets) commit too.
        vec![primary_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| primary_path.to_path_buf())]
    }
}

impl Orchestrator {
    /// List skill configurations with optional scope filters.
    pub fn list_skill_configs(
        &self,
        workspace_id: Option<&str>,
        project_id: Option<&str>,
    ) -> Result<Vec<SkillConfig>, String> {
        config_resource::list_configs::<SkillResource>(self, workspace_id, project_id)
    }

    /// Get a single skill configuration by ID.
    pub fn get_skill_config(
        &self,
        id: &str,
        project_id: Option<&str>,
    ) -> Result<Option<SkillConfig>, String> {
        config_resource::get_config::<SkillResource>(self, id, project_id)
    }

    /// Create a new skill configuration.
    pub fn create_skill_config(&self, input: CreateSkillConfig) -> Result<SkillConfig, String> {
        config_resource::create_config::<SkillResource>(self, input)
    }

    /// Update an existing skill configuration.
    pub fn update_skill_config(
        &self,
        id: &str,
        input: UpdateSkillConfig,
        target_project_id: Option<&str>,
    ) -> Result<SkillConfig, String> {
        config_resource::update_config::<SkillResource>(self, id, input, target_project_id)
    }

    /// Delete a skill configuration.
    pub fn delete_skill_config(&self, id: &str, project_id: Option<&str>) -> Result<(), String> {
        config_resource::delete_config::<SkillResource>(self, id, project_id)
    }

    /// Fetch a skill from a URL and return a preview for the UI.
    pub fn fetch_skill_preview(
        &self,
        url: &str,
    ) -> Result<crate::config::skill_fetch::FetchedSkill, String> {
        let source = crate::config::skill_fetch::parse_skill_url(url)?;
        crate::config::skill_fetch::fetch_skill(&source, url)
    }

    /// Install a previously fetched skill to the target scope.
    pub fn install_skill_from_url(
        &self,
        url: &str,
        project_id: Option<&str>,
    ) -> Result<SkillConfig, String> {
        let source = crate::config::skill_fetch::parse_skill_url(url)?;
        let fetched = crate::config::skill_fetch::fetch_skill(&source, url)?;

        let project_path: Option<std::path::PathBuf> = if let Some(pid) = project_id {
            Some(self.project_path(pid)?)
        } else {
            None
        };

        let installed_dir = crate::config::skill_fetch::install_fetched_skill(
            &fetched,
            &self.config_dir,
            project_path.as_deref(),
        )?;

        // Derive the final skill ID from the installed directory name
        let final_id = installed_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&fetched.skill_id)
            .to_string();

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "skill", "action": "created", "id": final_id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "skill_configs", "action": "insert"}),
        );

        let now = chrono::Utc::now().timestamp() as i32;
        Ok(SkillConfig {
            id: final_id,
            name: fetched.name,
            description: fetched.description,
            prompt: fetched.prompt,
            allowed_tools: fetched.allowed_tools,
            workspace_id: if project_id.is_none() {
                Some("default".to_string())
            } else {
                None
            },
            project_id: project_id.map(|s| s.to_string()),
            created_at: now,
            updated_at: now,
        })
    }

    /// List skills for a project context with workspace→project shadowing.
    /// Copy a skill from any scope into a target scope under a chosen id.
    pub fn copy_skill_config(
        &self,
        source_id: &str,
        source_project_id: Option<&str>,
        target_id: &str,
        target_project_id: Option<&str>,
    ) -> Result<SkillConfig, String> {
        config_resource::copy_config::<SkillResource>(
            self,
            source_id,
            source_project_id,
            target_id,
            target_project_id,
        )
    }

    pub fn list_skills_for_context(&self, project_id: &str) -> Result<Vec<SkillConfig>, String> {
        config_resource::list_for_context::<SkillResource>(self, project_id)
    }
}
