//! Orchestrator skill configuration operations.
//!
//! Mirrors the agent pattern for skills — workspace vs project scope resolution.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::config::{skills as config_skills, slugify, ConfigResult};
use crate::models::{CreateSkillConfig, SkillConfig, UpdateSkillConfig};

use super::Orchestrator;

/// Convert a file skill to the API model.
fn file_skill_to_config(
    skill: config_skills::FileSkill,
    workspace_id: Option<String>,
    project_id: Option<String>,
) -> SkillConfig {
    let now = chrono::Utc::now().timestamp() as i32;
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

impl Orchestrator {
    /// List skill configurations with optional scope filters.
    pub fn list_skill_configs(
        &self,
        workspace_id: Option<&str>,
        project_id: Option<&str>,
    ) -> Result<Vec<SkillConfig>, String> {
        let mut all_skills = Vec::new();

        if workspace_id.is_none() && project_id.is_none() {
            // Load workspace skills
            let ws_results = config_skills::list_skills(&self.config_dir, None)?;
            for result in ws_results {
                if let ConfigResult::Ok(skill) = result {
                    if !skill.is_project_scoped {
                        all_skills.push(file_skill_to_config(
                            skill,
                            Some("default".to_string()),
                            None,
                        ));
                    }
                }
            }

            // Load skills from all projects
            let projects = self.all_project_paths()?;
            for (pid, project_path) in projects {
                let proj_results =
                    config_skills::list_skills(&self.config_dir, Some(&project_path))?;
                for result in proj_results {
                    if let ConfigResult::Ok(skill) = result {
                        if skill.is_project_scoped {
                            all_skills.push(file_skill_to_config(skill, None, Some(pid.clone())));
                        }
                    }
                }
            }
        } else {
            let project_path: Option<PathBuf> = if let Some(pid) = project_id {
                Some(self.project_path(pid)?)
            } else {
                None
            };

            let results = config_skills::list_skills(&self.config_dir, project_path.as_deref())?;

            for result in results {
                if let ConfigResult::Ok(skill) = result {
                    let include = match (workspace_id, project_id) {
                        (Some(_), None) => !skill.is_project_scoped,
                        (None, Some(_)) => skill.is_project_scoped,
                        _ => true,
                    };

                    if include {
                        let (ws_id, proj_id) = if skill.is_project_scoped {
                            (None, project_id.map(|s| s.to_string()))
                        } else {
                            (workspace_id.map(|s| s.to_string()), None)
                        };
                        all_skills.push(file_skill_to_config(skill, ws_id, proj_id));
                    }
                }
            }
        }

        all_skills.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(all_skills)
    }

    /// Get a single skill configuration by ID.
    pub fn get_skill_config(
        &self,
        id: &str,
        project_id: Option<&str>,
    ) -> Result<Option<SkillConfig>, String> {
        if let Some(pid) = project_id {
            let project_path = self.project_path(pid)?;
            let skill = config_skills::get_skill(&self.config_dir, id, Some(&project_path))?;
            return Ok(skill.map(|s| {
                let (ws_id, proj_id) = if s.is_project_scoped {
                    (None, Some(pid.to_string()))
                } else {
                    (Some("default".to_string()), None)
                };
                file_skill_to_config(s, ws_id, proj_id)
            }));
        }

        // Workspace first
        if let Some(skill) = config_skills::get_skill(&self.config_dir, id, None)? {
            if !skill.is_project_scoped {
                return Ok(Some(file_skill_to_config(
                    skill,
                    Some("default".to_string()),
                    None,
                )));
            }
        }

        // Search all projects
        let projects = self.all_project_paths()?;
        for (pid, project_path) in projects {
            if let Some(skill) =
                config_skills::get_skill(&self.config_dir, id, Some(&project_path))?
            {
                if skill.is_project_scoped {
                    return Ok(Some(file_skill_to_config(skill, None, Some(pid))));
                }
            }
        }

        Ok(None)
    }

    /// Create a new skill configuration.
    pub fn create_skill_config(&self, input: CreateSkillConfig) -> Result<SkillConfig, String> {
        if input.workspace_id.is_none() == input.project_id.is_none() {
            return Err("Exactly one of workspace_id or project_id must be set".to_string());
        }

        let project_path: Option<PathBuf> = if let Some(ref pid) = input.project_id {
            Some(self.project_path(pid)?)
        } else {
            None
        };

        let is_project_scoped = project_path.is_some();
        let id = input.id.unwrap_or_else(|| slugify(&input.name));

        let file_skill = config_skills::FileSkill {
            id: id.clone(),
            name: input.name.clone(),
            description: input.description.clone(),
            prompt: input.prompt.clone(),
            allowed_tools: input.allowed_tools.clone(),
            is_project_scoped,
            file_path: PathBuf::new(),
            dir_path: PathBuf::new(),
            meta: None,
            has_references: false,
            has_scripts: false,
            has_assets: false,
        };

        config_skills::save_skill(&self.config_dir, &file_skill, project_path.as_deref(), None)?;

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "skill", "action": "created", "id": id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "skill_configs", "action": "insert"}),
        );

        let now = chrono::Utc::now().timestamp() as i32;
        Ok(SkillConfig {
            id,
            name: input.name,
            description: input.description,
            prompt: input.prompt,
            allowed_tools: input.allowed_tools,
            workspace_id: input.workspace_id,
            project_id: input.project_id,
            created_at: now,
            updated_at: now,
        })
    }

    /// Update an existing skill configuration.
    pub fn update_skill_config(
        &self,
        id: &str,
        input: UpdateSkillConfig,
        target_project_id: Option<&str>,
    ) -> Result<SkillConfig, String> {
        // Find existing skill in the targeted scope
        let existing = {
            if let Some(pid) = target_project_id {
                let project_path = self.project_path(pid)?;
                config_skills::get_skill(&self.config_dir, id, Some(&project_path))?
                    .ok_or_else(|| format!("Skill not found: {}", id))?
            } else if let Some(skill) = config_skills::get_skill(&self.config_dir, id, None)? {
                skill
            } else {
                let projects = self.all_project_paths()?;
                let mut found: Option<config_skills::FileSkill> = None;
                for (_pid, project_path) in projects {
                    if let Some(skill) =
                        config_skills::get_skill(&self.config_dir, id, Some(&project_path))?
                    {
                        found = Some(skill);
                        break;
                    }
                }
                found.ok_or_else(|| format!("Skill not found: {}", id))?
            }
        };

        let name = input.name.unwrap_or(existing.name);
        let description = input.description.unwrap_or(existing.description);
        let prompt = input.prompt.unwrap_or(existing.prompt);
        let allowed_tools = match input.allowed_tools {
            None => existing.allowed_tools,
            Some(None) => None,
            Some(Some(v)) => Some(v),
        };

        let new_is_project_scoped = match (&input.workspace_id, &input.project_id) {
            (Some(Some(_)), _) => false,
            (_, Some(Some(_))) => true,
            _ => existing.is_project_scoped,
        };

        let new_project_id: Option<String> = match &input.project_id {
            Some(Some(pid)) => Some(pid.clone()),
            _ => None,
        };

        let scope_changing = new_is_project_scoped != existing.is_project_scoped;

        let new_project_path: Option<PathBuf> = if new_is_project_scoped {
            if let Some(ref pid) = new_project_id {
                Some(self.project_path(pid)?)
            } else if scope_changing {
                return Err("Project ID required when changing to project scope".to_string());
            } else {
                None
            }
        } else {
            None
        };

        let file_skill = config_skills::FileSkill {
            id: id.to_string(),
            name: name.clone(),
            description: description.clone(),
            prompt: prompt.clone(),
            allowed_tools: allowed_tools.clone(),
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
        };

        let dest_path = config_skills::save_skill(
            &self.config_dir,
            &file_skill,
            new_project_path.as_deref(),
            None,
        )?;

        if scope_changing {
            // Copy package subdirectories to new location
            if let Some(dest_dir) = dest_path.parent() {
                if let Err(e) = config_skills::copy_skill_package(&existing.dir_path, dest_dir) {
                    log::warn!("Failed to copy skill package contents: {}", e);
                }
            }
            // Remove old skill directory
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

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "skill", "action": "modified", "id": id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "skill_configs", "action": "update"}),
        );

        let now = chrono::Utc::now().timestamp() as i32;
        Ok(SkillConfig {
            id: id.to_string(),
            name,
            description,
            prompt,
            allowed_tools,
            workspace_id: if !new_is_project_scoped {
                Some("default".to_string())
            } else {
                None
            },
            project_id: new_project_id,
            created_at: now,
            updated_at: now,
        })
    }

    /// Delete a skill configuration.
    pub fn delete_skill_config(&self, id: &str, project_id: Option<&str>) -> Result<(), String> {
        let project_path: Option<PathBuf> = if let Some(pid) = project_id {
            Some(self.project_path(pid)?)
        } else {
            None
        };

        config_skills::delete_skill(&self.config_dir, id, project_path.as_deref())?;

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "skill", "action": "removed", "id": id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "skill_configs", "action": "delete"}),
        );

        Ok(())
    }

    /// Create a project override of a skill (from any scope).
    pub fn create_skill_override(&self, id: &str, project_id: &str) -> Result<SkillConfig, String> {
        let project_path = self.project_path(project_id)?;

        let source = config_skills::get_skill(&self.config_dir, id, Some(&project_path))?
            .ok_or_else(|| format!("Skill not found: {}", id))?;

        let target_dir = project_path.join(".cairn").join("skills").join(id);
        if target_dir.join("SKILL.md").exists() {
            return Err(format!("Skill '{}' already exists in this project", id));
        }

        let override_skill = config_skills::FileSkill {
            id: id.to_string(),
            name: source.name.clone(),
            description: source.description.clone(),
            prompt: source.prompt.clone(),
            allowed_tools: source.allowed_tools.clone(),
            is_project_scoped: true,
            file_path: PathBuf::new(),
            dir_path: PathBuf::new(),
            meta: None,
            has_references: false,
            has_scripts: false,
            has_assets: false,
        };

        let dest_path = config_skills::save_skill(
            &self.config_dir,
            &override_skill,
            Some(&project_path),
            None,
        )?;

        // Copy package subdirectories from source
        if let Some(dest_dir) = dest_path.parent() {
            config_skills::copy_skill_package(&source.dir_path, dest_dir)?;
        }

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "skill", "action": "created", "id": id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "skill_configs", "action": "insert"}),
        );

        let now = chrono::Utc::now().timestamp() as i32;
        Ok(SkillConfig {
            id: id.to_string(),
            name: source.name,
            description: source.description,
            prompt: source.prompt,
            allowed_tools: source.allowed_tools,
            workspace_id: None,
            project_id: Some(project_id.to_string()),
            created_at: now,
            updated_at: now,
        })
    }

    /// Promote a project-only skill to workspace scope.
    pub fn promote_skill_to_workspace(
        &self,
        id: &str,
        source_project_id: &str,
    ) -> Result<SkillConfig, String> {
        let project_path = self.project_path(source_project_id)?;

        let source = config_skills::get_skill(&self.config_dir, id, Some(&project_path))?
            .ok_or_else(|| format!("Skill not found in project: {}", id))?;

        if !source.is_project_scoped {
            return Err("Skill is not project-scoped".to_string());
        }

        let ws_dir = self.config_dir.join("skills").join(id);
        if ws_dir.join("SKILL.md").exists() {
            return Err(format!("Skill '{}' already exists at workspace scope", id));
        }

        let ws_skill = config_skills::FileSkill {
            id: id.to_string(),
            name: source.name.clone(),
            description: source.description.clone(),
            prompt: source.prompt.clone(),
            allowed_tools: source.allowed_tools.clone(),
            is_project_scoped: false,
            file_path: PathBuf::new(),
            dir_path: PathBuf::new(),
            meta: None,
            has_references: false,
            has_scripts: false,
            has_assets: false,
        };

        let dest_path = config_skills::save_skill(&self.config_dir, &ws_skill, None, None)?;

        // Copy package subdirectories from source
        if let Some(dest_dir) = dest_path.parent() {
            config_skills::copy_skill_package(&source.dir_path, dest_dir)?;
        }

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "skill", "action": "created", "id": id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "skill_configs", "action": "insert"}),
        );

        let now = chrono::Utc::now().timestamp() as i32;
        Ok(SkillConfig {
            id: id.to_string(),
            name: source.name,
            description: source.description,
            prompt: source.prompt,
            allowed_tools: source.allowed_tools,
            workspace_id: Some("default".to_string()),
            project_id: None,
            created_at: now,
            updated_at: now,
        })
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
    pub fn list_skills_for_context(&self, project_id: &str) -> Result<Vec<SkillConfig>, String> {
        let project_path = self.project_path(project_id)?;
        let mut skills_map: HashMap<String, SkillConfig> = HashMap::new();

        let ws_results = config_skills::list_skills(&self.config_dir, None)?;
        for result in ws_results {
            if let ConfigResult::Ok(skill) = result {
                if !skill.is_project_scoped {
                    let config = file_skill_to_config(skill, Some("default".to_string()), None);
                    skills_map.insert(config.id.clone(), config);
                }
            }
        }

        let proj_results = config_skills::list_skills(&self.config_dir, Some(&project_path))?;
        for result in proj_results {
            if let ConfigResult::Ok(skill) = result {
                if skill.is_project_scoped {
                    let config = file_skill_to_config(skill, None, Some(project_id.to_string()));
                    skills_map.insert(config.id.clone(), config);
                }
            }
        }

        let mut skills: Vec<SkillConfig> = skills_map.into_values().collect();
        skills.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(skills)
    }
}
