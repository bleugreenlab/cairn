//! Agent configuration file reading.
//!
//! Agents are stored as markdown files with YAML frontmatter.
//! - Workspace-scoped: `~/.cairn/agents/`
//! - Project-scoped: `[project-dir]/.cairn/agents/`

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::agents::{agent_to_markdown, parse_agent_markdown, AgentExportData};
use crate::models::{ApprovalPolicy, FilesystemScope, Model};

use super::{id_from_path, ConfigResult};

/// Agent loaded from a file
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileAgent {
    pub id: String,
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub tools: Vec<String>,
    #[serde(alias = "model")]
    pub tier: Option<Model>,
    pub approval_policy: Option<ApprovalPolicy>,
    pub filesystem_scope: Option<FilesystemScope>,
    pub disallowed_tools: Option<Vec<String>>,
    pub skills: Option<Vec<String>>,
    pub hooks: Option<serde_json::Value>,
    /// Preferred backend when multiple providers are available.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "backend", alias = "backendPreference")]
    pub backend_preference: Option<String>,
    /// Whether this agent is project-scoped (vs workspace-scoped)
    pub is_project_scoped: bool,
    /// Path to the source file
    pub file_path: PathBuf,
}

/// List all agents from files
///
/// Reads workspace-scoped agents from `~/.cairn/agents/`.
/// If project_path is Some, also includes agents from `[project_path]/.cairn/agents/`.
pub fn list_agents(
    config_dir: &Path,
    project_path: Option<&Path>,
) -> Result<Vec<ConfigResult<FileAgent>>, String> {
    let mut results = vec![];

    // Read workspace-scoped agents
    let ws_dir = config_dir.join("agents");
    if ws_dir.exists() {
        for entry in std::fs::read_dir(&ws_dir)
            .map_err(|e| format!("Failed to read agents directory: {}", e))?
        {
            let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
            let path = entry.path();

            // Skip directories and non-.md files
            if path.is_dir() || path.extension().and_then(|e| e.to_str()) != Some("md") {
                continue;
            }

            results.push(load_agent_file(&path, false));
        }
    }

    // Read project-scoped agents if project path specified
    if let Some(proj_path) = project_path {
        let proj_dir = proj_path.join(".cairn").join("agents");
        if proj_dir.exists() && proj_dir.is_dir() {
            for entry in std::fs::read_dir(&proj_dir)
                .map_err(|e| format!("Failed to read project agents directory: {}", e))?
            {
                let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
                let path = entry.path();

                if path.extension().and_then(|e| e.to_str()) != Some("md") {
                    continue;
                }

                results.push(load_agent_file(&path, true));
            }
        }
    }

    Ok(results)
}

/// Get a specific agent by ID
///
/// Checks project-scoped first (if project_path provided), then workspace-scoped.
pub fn get_agent(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<Option<FileAgent>, String> {
    // Try project-scoped first if project specified
    if let Some(proj_path) = project_path {
        let path = proj_path
            .join(".cairn")
            .join("agents")
            .join(format!("{}.md", id));
        if path.exists() {
            return match load_agent_file(&path, true) {
                ConfigResult::Ok(agent) => Ok(Some(agent)),
                ConfigResult::Err { error, .. } => Err(error),
            };
        }
    }

    // Try workspace-scoped
    let path = config_dir.join("agents").join(format!("{}.md", id));
    if path.exists() {
        return match load_agent_file(&path, false) {
            ConfigResult::Ok(agent) => Ok(Some(agent)),
            ConfigResult::Err { error, .. } => Err(error),
        };
    }

    Ok(None)
}

/// Save an agent to a file
///
/// Uses the agent's file_path if set, otherwise determines path from is_project_scoped.
/// For project-scoped agents without file_path, project_path must be provided.
pub fn save_agent(
    config_dir: &Path,
    agent: &FileAgent,
    project_path: Option<&Path>,
) -> Result<PathBuf, String> {
    // Determine target path
    let path = if agent.file_path.as_os_str().is_empty() {
        // No existing path - determine from scope
        if agent.is_project_scoped {
            let proj_path = project_path.ok_or("Project path required for project-scoped agent")?;
            let dir = proj_path.join(".cairn").join("agents");
            std::fs::create_dir_all(&dir)
                .map_err(|e| format!("Failed to create project agents directory: {}", e))?;
            dir.join(format!("{}.md", agent.id))
        } else {
            let dir = config_dir.join("agents");
            std::fs::create_dir_all(&dir)
                .map_err(|e| format!("Failed to create agents directory: {}", e))?;
            dir.join(format!("{}.md", agent.id))
        }
    } else {
        // Use existing path, ensure parent directory exists
        if let Some(parent) = agent.file_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create directory: {}", e))?;
        }
        agent.file_path.clone()
    };

    // Generate markdown content
    let markdown = agent_to_markdown(AgentExportData {
        id: &agent.id,
        name: &agent.name,
        description: &agent.description,
        tools: &agent.tools,
        tier: agent.tier.as_ref().map(|m| m.to_string()).as_deref(),
        prompt: &agent.prompt,
        approval_policy: agent.approval_policy,
        filesystem_scope: agent.filesystem_scope,
        disallowed_tools: agent.disallowed_tools.as_deref(),
        skills: agent.skills.as_deref(),
        hooks: agent.hooks.as_ref(),
        backend_preference: agent.backend_preference.as_deref(),
    });

    // Write file
    std::fs::write(&path, markdown).map_err(|e| format!("Failed to write agent file: {}", e))?;

    Ok(path)
}

/// Delete an agent file
pub fn delete_agent(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<(), String> {
    // Try project-scoped first if project specified
    if let Some(proj_path) = project_path {
        let path = proj_path
            .join(".cairn")
            .join("agents")
            .join(format!("{}.md", id));
        if path.exists() {
            std::fs::remove_file(&path)
                .map_err(|e| format!("Failed to delete agent file: {}", e))?;
            return Ok(());
        }
    }

    // Try workspace-scoped
    let path = config_dir.join("agents").join(format!("{}.md", id));
    if path.exists() {
        std::fs::remove_file(&path).map_err(|e| format!("Failed to delete agent file: {}", e))?;
    }

    Ok(())
}

/// List agents from Claude Code directories (for import)
///
/// Reads user-level agents from `~/.claude/agents/`.
/// If project_path is Some, also includes agents from `[project_path]/.claude/agents/`.
#[allow(dead_code)]
pub fn list_claude_agents(
    project_path: Option<&Path>,
) -> Result<Vec<ConfigResult<FileAgent>>, String> {
    let mut results = vec![];

    // User-level: ~/.claude/agents/
    if let Some(home) = dirs::home_dir() {
        let user_dir = home.join(".claude").join("agents");
        if user_dir.exists() && user_dir.is_dir() {
            for entry in std::fs::read_dir(&user_dir)
                .map_err(|e| format!("Failed to read ~/.claude/agents: {}", e))?
            {
                let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
                let path = entry.path();

                // Skip directories and non-.md files
                if path.is_dir() || path.extension().and_then(|e| e.to_str()) != Some("md") {
                    continue;
                }

                results.push(load_agent_file(&path, false));
            }
        }
    }

    // Project-level: [project]/.claude/agents/
    if let Some(proj_path) = project_path {
        let proj_dir = proj_path.join(".claude").join("agents");
        if proj_dir.exists() && proj_dir.is_dir() {
            for entry in std::fs::read_dir(&proj_dir)
                .map_err(|e| format!("Failed to read project .claude/agents: {}", e))?
            {
                let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
                let path = entry.path();

                if path.extension().and_then(|e| e.to_str()) != Some("md") {
                    continue;
                }

                results.push(load_agent_file(&path, true));
            }
        }
    }

    Ok(results)
}

/// Load a single agent file
fn load_agent_file(path: &Path, is_project_scoped: bool) -> ConfigResult<FileAgent> {
    let id = match id_from_path(path) {
        Some(id) => id,
        None => {
            return ConfigResult::Err {
                path: path.to_path_buf(),
                error: "Could not determine agent ID from filename".to_string(),
            }
        }
    };

    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => {
            return ConfigResult::Err {
                path: path.to_path_buf(),
                error: format!("Failed to read file: {}", e),
            }
        }
    };

    match parse_agent_markdown(&content) {
        Ok(parsed) => {
            let tier: Option<Model> = parsed.tier.and_then(|m| m.parse().ok());

            ConfigResult::Ok(FileAgent {
                id,
                name: parsed.name,
                description: parsed.description,
                prompt: parsed.prompt,
                tools: parsed.tools,
                tier,
                approval_policy: parsed.approval_policy,
                filesystem_scope: parsed.filesystem_scope,
                disallowed_tools: parsed.disallowed_tools,
                skills: parsed.skills,
                hooks: parsed.hooks,
                backend_preference: parsed.backend_preference,
                is_project_scoped,
                file_path: path.to_path_buf(),
            })
        }
        Err(e) => ConfigResult::Err {
            path: path.to_path_buf(),
            error: e,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_load_agent_roundtrip() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("test-agent.md");

        let content = r#"---
name: Test Agent
description: A test agent
tools:
  - Read
  - Write
tier: sonnet
---

You are a test agent.
"#;

        std::fs::write(&path, content).unwrap();

        let result = load_agent_file(&path, false);
        match result {
            ConfigResult::Ok(agent) => {
                assert_eq!(agent.id, "test-agent");
                assert_eq!(agent.name, "Test Agent");
                assert_eq!(agent.description, "A test agent");
                assert_eq!(agent.tools, vec!["Read", "Write"]);
                assert_eq!(agent.tier, Some(Model::new(Model::SONNET)));
                assert!(!agent.is_project_scoped);
                assert!(agent.prompt.contains("test agent"));
            }
            ConfigResult::Err { error, .. } => panic!("Failed to load: {}", error),
        }
    }
}
