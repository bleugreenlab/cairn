//! Skill configuration file reading.
//!
//! Skills are stored as markdown files with YAML frontmatter.
//! - Workspace-scoped: `~/.cairn/skills/`
//! - Project-scoped: `[project-dir]/.cairn/skills/`

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::models::Model;
use crate::skills::{parse_skill_markdown, skill_to_markdown, SkillExportData};

use super::{id_from_path, ConfigResult};

/// Skill loaded from a file
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSkill {
    pub id: String,
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub allowed_tools: Option<Vec<String>>,
    pub model: Option<Model>,
    /// Whether this skill is project-scoped (vs workspace-scoped)
    pub is_project_scoped: bool,
    /// Path to the source file
    pub file_path: PathBuf,
}

/// List all skills from files
///
/// Reads workspace-scoped skills from `~/.cairn/skills/`.
/// If project_path is Some, also includes skills from `[project_path]/.cairn/skills/`.
pub fn list_skills(
    config_dir: &Path,
    project_path: Option<&Path>,
) -> Result<Vec<ConfigResult<FileSkill>>, String> {
    let mut results = vec![];

    // Read workspace-scoped skills
    let ws_dir = config_dir.join("skills");
    if ws_dir.exists() {
        for entry in std::fs::read_dir(&ws_dir)
            .map_err(|e| format!("Failed to read skills directory: {}", e))?
        {
            let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
            let path = entry.path();

            // Skip directories and non-.md files
            if path.is_dir() || path.extension().and_then(|e| e.to_str()) != Some("md") {
                continue;
            }

            results.push(load_skill_file(&path, false));
        }
    }

    // Read project-scoped skills if project path specified
    if let Some(proj_path) = project_path {
        let proj_dir = proj_path.join(".cairn").join("skills");
        if proj_dir.exists() && proj_dir.is_dir() {
            for entry in std::fs::read_dir(&proj_dir)
                .map_err(|e| format!("Failed to read project skills directory: {}", e))?
            {
                let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
                let path = entry.path();

                if path.extension().and_then(|e| e.to_str()) != Some("md") {
                    continue;
                }

                results.push(load_skill_file(&path, true));
            }
        }
    }

    Ok(results)
}

/// Get a specific skill by ID
///
/// Checks project-scoped first (if project_path provided), then workspace-scoped.
pub fn get_skill(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<Option<FileSkill>, String> {
    // Try project-scoped first if project specified
    if let Some(proj_path) = project_path {
        let path = proj_path
            .join(".cairn")
            .join("skills")
            .join(format!("{}.md", id));
        if path.exists() {
            return match load_skill_file(&path, true) {
                ConfigResult::Ok(skill) => Ok(Some(skill)),
                ConfigResult::Err { error, .. } => Err(error),
            };
        }
    }

    // Try workspace-scoped
    let path = config_dir.join("skills").join(format!("{}.md", id));
    if path.exists() {
        return match load_skill_file(&path, false) {
            ConfigResult::Ok(skill) => Ok(Some(skill)),
            ConfigResult::Err { error, .. } => Err(error),
        };
    }

    Ok(None)
}

/// Save a skill to a file
pub fn save_skill(
    config_dir: &Path,
    skill: &FileSkill,
    project_path: Option<&Path>,
) -> Result<PathBuf, String> {
    // Determine target path
    let path = if skill.file_path.as_os_str().is_empty() {
        // No existing path - determine from scope
        if skill.is_project_scoped {
            let proj_path = project_path.ok_or("Project path required for project-scoped skill")?;
            let dir = proj_path.join(".cairn").join("skills");
            std::fs::create_dir_all(&dir)
                .map_err(|e| format!("Failed to create project skills directory: {}", e))?;
            dir.join(format!("{}.md", skill.id))
        } else {
            let dir = config_dir.join("skills");
            std::fs::create_dir_all(&dir)
                .map_err(|e| format!("Failed to create skills directory: {}", e))?;
            dir.join(format!("{}.md", skill.id))
        }
    } else {
        // Use existing path, ensure parent directory exists
        if let Some(parent) = skill.file_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create directory: {}", e))?;
        }
        skill.file_path.clone()
    };

    // Generate markdown content
    let markdown = skill_to_markdown(SkillExportData {
        name: &skill.name,
        description: &skill.description,
        allowed_tools: skill.allowed_tools.as_deref(),
        model: skill.model.as_ref().map(|m| m.to_string()).as_deref(),
        prompt: &skill.prompt,
    });

    // Write file
    std::fs::write(&path, markdown).map_err(|e| format!("Failed to write skill file: {}", e))?;

    Ok(path)
}

/// Delete a skill file
pub fn delete_skill(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<(), String> {
    // Try project-scoped first if project specified
    if let Some(proj_path) = project_path {
        let path = proj_path
            .join(".cairn")
            .join("skills")
            .join(format!("{}.md", id));
        if path.exists() {
            std::fs::remove_file(&path)
                .map_err(|e| format!("Failed to delete skill file: {}", e))?;
            return Ok(());
        }
    }

    // Try workspace-scoped
    let path = config_dir.join("skills").join(format!("{}.md", id));
    if path.exists() {
        std::fs::remove_file(&path).map_err(|e| format!("Failed to delete skill file: {}", e))?;
    }

    Ok(())
}

/// Load a single skill file
fn load_skill_file(path: &Path, is_project_scoped: bool) -> ConfigResult<FileSkill> {
    let id = match id_from_path(path) {
        Some(id) => id,
        None => {
            return ConfigResult::Err {
                path: path.to_path_buf(),
                error: "Could not determine skill ID from filename".to_string(),
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

    match parse_skill_markdown(&content) {
        Ok(parsed) => {
            let model: Option<Model> = parsed.model.and_then(|m| m.parse().ok());

            ConfigResult::Ok(FileSkill {
                id,
                name: parsed.name,
                description: parsed.description,
                prompt: parsed.prompt,
                allowed_tools: parsed.allowed_tools,
                model,
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
    fn test_load_skill_roundtrip() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("test-skill.md");

        let content = r#"---
name: Test Skill
description: A test skill
allowedTools:
  - Read
  - Grep
---

You are a test skill providing expertise.
"#;

        std::fs::write(&path, content).unwrap();

        let result = load_skill_file(&path, false);
        match result {
            ConfigResult::Ok(skill) => {
                assert_eq!(skill.id, "test-skill");
                assert_eq!(skill.name, "Test Skill");
                assert_eq!(skill.description, "A test skill");
                assert_eq!(
                    skill.allowed_tools,
                    Some(vec!["Read".to_string(), "Grep".to_string()])
                );
                assert!(!skill.is_project_scoped);
                assert!(skill.prompt.contains("test skill"));
            }
            ConfigResult::Err { error, .. } => panic!("Failed to load: {}", error),
        }
    }
}
