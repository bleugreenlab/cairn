//! Orchestrator agent configuration operations.
//!
//! Consolidates scope resolution logic for agent configs:
//! workspace-scoped (`~/.cairn/agents/`) vs project-scoped (`[project]/.cairn/agents/`).

use std::collections::HashMap;
use std::path::PathBuf;

use crate::config::{agents as config_agents, get_project_path, ConfigResult};
use crate::models::{AgentConfig, CreateAgentConfig, UpdateAgentConfig};
use crate::projects;

use super::Orchestrator;

/// Convert a file agent to the API model.
fn file_agent_to_config(
    agent: config_agents::FileAgent,
    workspace_id: Option<String>,
    project_id: Option<String>,
) -> AgentConfig {
    let now = chrono::Utc::now().timestamp() as i32;
    AgentConfig {
        id: agent.id,
        name: agent.name,
        description: agent.description,
        prompt: agent.prompt,
        tools: agent.tools,
        model: agent.model,
        workspace_id,
        project_id,
        created_at: now,
        updated_at: now,
        disallowed_tools: agent.disallowed_tools,
        skills: agent.skills,
        permission_mode: agent.permission_mode,
    }
}

impl Orchestrator {
    /// List all project (id, repo_path) pairs from DB.
    pub(crate) fn all_project_paths(&self) -> Result<Vec<(String, PathBuf)>, String> {
        let mut conn = self.db.conn.lock().map_err(|e| e.to_string())?;
        let db_projects = projects::crud::list_db(&mut conn)?;
        Ok(db_projects
            .into_iter()
            .map(|p| (p.id, PathBuf::from(p.repo_path)))
            .collect())
    }

    /// Resolve project_id → repo path from DB.
    pub(crate) fn project_path(&self, project_id: &str) -> Result<PathBuf, String> {
        let mut conn = self.db.conn.lock().map_err(|e| e.to_string())?;
        get_project_path(&mut conn, project_id)
    }

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
        let mut all_agents = Vec::new();

        if workspace_id.is_none() && project_id.is_none() {
            // Load workspace agents
            let ws_results = config_agents::list_agents(&self.config_dir, None)?;
            for result in ws_results {
                if let ConfigResult::Ok(agent) = result {
                    if !agent.is_project_scoped {
                        all_agents.push(file_agent_to_config(
                            agent,
                            Some("default".to_string()),
                            None,
                        ));
                    }
                }
            }

            // Load agents from all projects
            let projects = self.all_project_paths()?;
            for (pid, project_path) in projects {
                let proj_results =
                    config_agents::list_agents(&self.config_dir, Some(&project_path))?;
                for result in proj_results {
                    if let ConfigResult::Ok(agent) = result {
                        if agent.is_project_scoped {
                            all_agents.push(file_agent_to_config(agent, None, Some(pid.clone())));
                        }
                    }
                }
            }
        } else {
            // Load agents for a specific scope
            let project_path: Option<PathBuf> = if let Some(pid) = project_id {
                Some(self.project_path(pid)?)
            } else {
                None
            };

            let results = config_agents::list_agents(&self.config_dir, project_path.as_deref())?;

            for result in results {
                if let ConfigResult::Ok(agent) = result {
                    let include = match (workspace_id, project_id) {
                        (Some(_), None) => !agent.is_project_scoped,
                        (None, Some(_)) => agent.is_project_scoped,
                        _ => true,
                    };

                    if include {
                        let (ws_id, proj_id) = if agent.is_project_scoped {
                            (None, project_id.map(|s| s.to_string()))
                        } else {
                            (workspace_id.map(|s| s.to_string()), None)
                        };
                        all_agents.push(file_agent_to_config(agent, ws_id, proj_id));
                    }
                }
            }
        }

        all_agents.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(all_agents)
    }

    /// Get a single agent configuration by ID.
    pub fn get_agent_config(
        &self,
        id: &str,
        project_id: Option<&str>,
    ) -> Result<Option<AgentConfig>, String> {
        // If project_id specified, look there first
        if let Some(pid) = project_id {
            let project_path = self.project_path(pid)?;
            let agent = config_agents::get_agent(&self.config_dir, id, Some(&project_path))?;
            return Ok(agent.map(|a| {
                let (ws_id, proj_id) = if a.is_project_scoped {
                    (None, Some(pid.to_string()))
                } else {
                    (Some("default".to_string()), None)
                };
                file_agent_to_config(a, ws_id, proj_id)
            }));
        }

        // No project_id — try workspace first
        if let Some(agent) = config_agents::get_agent(&self.config_dir, id, None)? {
            if !agent.is_project_scoped {
                return Ok(Some(file_agent_to_config(
                    agent,
                    Some("default".to_string()),
                    None,
                )));
            }
        }

        // Search all projects
        let projects = self.all_project_paths()?;
        for (pid, project_path) in projects {
            if let Some(agent) =
                config_agents::get_agent(&self.config_dir, id, Some(&project_path))?
            {
                if agent.is_project_scoped {
                    return Ok(Some(file_agent_to_config(agent, None, Some(pid))));
                }
            }
        }

        Ok(None)
    }

    /// Create a new agent configuration.
    pub fn create_agent_config(&self, input: CreateAgentConfig) -> Result<AgentConfig, String> {
        if input.workspace_id.is_none() == input.project_id.is_none() {
            return Err("Exactly one of workspace_id or project_id must be set".to_string());
        }

        let project_path: Option<PathBuf> = if let Some(ref pid) = input.project_id {
            Some(self.project_path(pid)?)
        } else {
            None
        };

        let is_project_scoped = project_path.is_some();

        let file_agent = config_agents::FileAgent {
            id: input.id.clone(),
            name: input.name.clone(),
            description: input.description.clone(),
            prompt: input.prompt.clone(),
            tools: input.tools.clone(),
            model: input.model.clone(),
            permission_mode: input.permission_mode.clone(),
            disallowed_tools: input.disallowed_tools.clone(),
            skills: input.skills.clone(),
            hooks: None,
            is_project_scoped,
            file_path: PathBuf::new(),
        };

        config_agents::save_agent(&self.config_dir, &file_agent, project_path.as_deref())?;

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "agent", "action": "created", "id": input.id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "agent_configs", "action": "insert"}),
        );

        let now = chrono::Utc::now().timestamp() as i32;
        Ok(AgentConfig {
            id: input.id,
            name: input.name,
            description: input.description,
            prompt: input.prompt,
            tools: input.tools,
            model: input.model,
            workspace_id: input.workspace_id,
            project_id: input.project_id,
            created_at: now,
            updated_at: now,
            disallowed_tools: input.disallowed_tools,
            skills: input.skills,
            permission_mode: input.permission_mode,
        })
    }

    /// Update an existing agent configuration.
    pub fn update_agent_config(
        &self,
        id: &str,
        input: UpdateAgentConfig,
    ) -> Result<AgentConfig, String> {
        // Find existing agent
        let existing = {
            if let Some(agent) = config_agents::get_agent(&self.config_dir, id, None)? {
                agent
            } else {
                let projects = self.all_project_paths()?;
                let mut found: Option<config_agents::FileAgent> = None;
                for (_pid, project_path) in projects {
                    if let Some(agent) =
                        config_agents::get_agent(&self.config_dir, id, Some(&project_path))?
                    {
                        found = Some(agent);
                        break;
                    }
                }
                found.ok_or_else(|| format!("Agent not found: {}", id))?
            }
        };

        // Merge updates
        let name = input.name.unwrap_or(existing.name);
        let description = input.description.unwrap_or(existing.description);
        let prompt = input.prompt.unwrap_or(existing.prompt);
        let tools = input.tools.unwrap_or(existing.tools);
        let model = match input.model {
            None => existing.model,
            Some(None) => None,
            Some(Some(m)) => Some(m),
        };
        let disallowed_tools = match input.disallowed_tools {
            None => existing.disallowed_tools,
            Some(None) => None,
            Some(Some(v)) => Some(v),
        };
        let skills = match input.skills {
            None => existing.skills,
            Some(None) => None,
            Some(Some(v)) => Some(v),
        };
        let permission_mode = match input.permission_mode {
            None => existing.permission_mode.clone(),
            Some(None) => None,
            Some(Some(m)) => Some(m),
        };

        // Determine new scope
        let new_is_project_scoped = match (&input.workspace_id, &input.project_id) {
            (Some(Some(_)), _) => false,
            (_, Some(Some(_))) => true,
            _ => existing.is_project_scoped,
        };

        let new_project_id: Option<String> = match &input.project_id {
            Some(Some(pid)) => Some(pid.clone()),
            Some(None) => None,
            None => None,
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

        let file_agent = config_agents::FileAgent {
            id: id.to_string(),
            name: name.clone(),
            description: description.clone(),
            prompt: prompt.clone(),
            tools: tools.clone(),
            model: model.clone(),
            permission_mode: permission_mode.clone(),
            disallowed_tools: disallowed_tools.clone(),
            skills: skills.clone(),
            hooks: existing.hooks.clone(),
            is_project_scoped: new_is_project_scoped,
            file_path: if scope_changing {
                PathBuf::new()
            } else {
                existing.file_path.clone()
            },
        };

        config_agents::save_agent(&self.config_dir, &file_agent, new_project_path.as_deref())?;

        // Delete old file if scope changed
        if scope_changing {
            if let Err(e) = std::fs::remove_file(&existing.file_path) {
                log::warn!(
                    "Failed to delete old agent file {:?}: {}",
                    existing.file_path,
                    e
                );
            }
        }

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "agent", "action": "modified", "id": id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "agent_configs", "action": "update"}),
        );

        let now = chrono::Utc::now().timestamp() as i32;
        Ok(AgentConfig {
            id: id.to_string(),
            name,
            description,
            prompt,
            tools,
            model,
            workspace_id: if !new_is_project_scoped {
                Some("default".to_string())
            } else {
                None
            },
            project_id: new_project_id,
            created_at: now,
            updated_at: now,
            disallowed_tools,
            skills,
            permission_mode,
        })
    }

    /// Delete an agent configuration.
    pub fn delete_agent_config(&self, id: &str, project_id: Option<&str>) -> Result<(), String> {
        let project_path: Option<PathBuf> = if let Some(pid) = project_id {
            Some(self.project_path(pid)?)
        } else {
            None
        };

        config_agents::delete_agent(&self.config_dir, id, project_path.as_deref())?;

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "agent", "action": "removed", "id": id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "agent_configs", "action": "delete"}),
        );

        Ok(())
    }

    /// Fork a workspace agent to a project scope.
    pub fn fork_agent_config(&self, id: &str, project_id: &str) -> Result<AgentConfig, String> {
        let project_path = self.project_path(project_id)?;

        let source = config_agents::get_agent(&self.config_dir, id, None)?
            .ok_or_else(|| format!("Agent not found in workspace: {}", id))?;

        if source.is_project_scoped {
            return Err("Cannot fork a project-scoped agent".to_string());
        }

        let target_path = project_path
            .join(".cairn")
            .join("agents")
            .join(format!("{}.md", id));
        if target_path.exists() {
            return Err(format!("Agent '{}' already exists in this project", id));
        }

        let forked = config_agents::FileAgent {
            id: id.to_string(),
            name: source.name.clone(),
            description: source.description.clone(),
            prompt: source.prompt.clone(),
            tools: source.tools.clone(),
            model: source.model.clone(),
            permission_mode: source.permission_mode.clone(),
            disallowed_tools: source.disallowed_tools.clone(),
            skills: source.skills.clone(),
            hooks: source.hooks.clone(),
            is_project_scoped: true,
            file_path: PathBuf::new(),
        };

        config_agents::save_agent(&self.config_dir, &forked, Some(&project_path))?;

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "agent", "action": "created", "id": id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "agent_configs", "action": "insert"}),
        );

        let now = chrono::Utc::now().timestamp() as i32;
        Ok(AgentConfig {
            id: id.to_string(),
            name: source.name,
            description: source.description,
            prompt: source.prompt,
            tools: source.tools,
            model: source.model,
            workspace_id: None,
            project_id: Some(project_id.to_string()),
            created_at: now,
            updated_at: now,
            disallowed_tools: source.disallowed_tools,
            skills: source.skills,
            permission_mode: source.permission_mode,
        })
    }

    /// List agents for a project context with workspace→project shadowing.
    pub fn list_agents_for_context(&self, project_id: &str) -> Result<Vec<AgentConfig>, String> {
        let project_path = self.project_path(project_id)?;
        let mut agents_map: HashMap<String, AgentConfig> = HashMap::new();

        // Workspace agents first
        let ws_results = config_agents::list_agents(&self.config_dir, None)?;
        for result in ws_results {
            if let ConfigResult::Ok(agent) = result {
                if !agent.is_project_scoped {
                    let config = file_agent_to_config(agent, Some("default".to_string()), None);
                    agents_map.insert(config.id.clone(), config);
                }
            }
        }

        // Project agents shadow workspace agents with same ID
        let proj_results = config_agents::list_agents(&self.config_dir, Some(&project_path))?;
        for result in proj_results {
            if let ConfigResult::Ok(agent) = result {
                if agent.is_project_scoped {
                    let config = file_agent_to_config(agent, None, Some(project_id.to_string()));
                    agents_map.insert(config.id.clone(), config);
                }
            }
        }

        let mut agents: Vec<AgentConfig> = agents_map.into_values().collect();
        agents.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(agents)
    }
}
