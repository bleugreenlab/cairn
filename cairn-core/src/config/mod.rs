//! File-based configuration reading.
//!
//! This module reads recipes, agents, and skills directly from files.
//! Files are the source of truth - the database only stores runtime state (executions, jobs, etc.).
//!
//! ## File Structure
//!
//! Workspace-level configs (shared across all projects):
//! ```text
//! ~/.cairn/
//! ├── recipes/
//! │   └── my-recipe.yaml
//! ├── agents/
//! │   └── my-agent.md
//! └── skills/
//!     └── my-skill.md
//! ```
//!
//! Project-level configs (stored in project directory, can be version-controlled):
//! ```text
//! [project-dir]/.cairn/
//! ├── recipes/
//! │   └── project-recipe.yaml
//! ├── agents/
//! │   └── project-agent.md
//! └── skills/
//!     └── project-skill.md
//! ```
//!
//! ## ID = Filename
//!
//! - `explore.md` → ID is `explore`
//! - Config in project's `.cairn/agents/custom.md` → ID is `custom` (project-scoped)

pub mod agents;
pub mod keybinds;
pub mod mcp_setup;
pub mod project_settings;
pub mod recipes;
pub mod settings;
pub mod skills;
pub mod tools;

use diesel::prelude::*;
use diesel::SqliteConnection;
use std::path::{Path, PathBuf};

/// Get the base config directory (~/.cairn/ for prod, ~/.cairn-dev/ for dev)
pub fn get_config_dir() -> Result<PathBuf, String> {
    let config_name = if cfg!(debug_assertions) {
        ".cairn-dev"
    } else {
        ".cairn"
    };

    dirs::home_dir()
        .map(|h| h.join(config_name))
        .ok_or_else(|| "Could not determine home directory".to_string())
}

/// Get project repo path from project ID
pub fn get_project_path(conn: &mut SqliteConnection, project_id: &str) -> Result<PathBuf, String> {
    use crate::schema::projects;

    let repo_path: String = projects::table
        .find(project_id)
        .select(projects::repo_path)
        .first(conn)
        .map_err(|e| format!("Project not found: {}", e))?;

    Ok(PathBuf::from(repo_path))
}

/// Ensure config directories exist
pub fn ensure_config_dirs(config_dir: &Path) -> Result<(), String> {
    for dir in ["recipes", "agents", "skills", "tools"] {
        let path = config_dir.join(dir);
        std::fs::create_dir_all(&path)
            .map_err(|e| format!("Failed to create directory {:?}: {}", path, e))?;
    }

    Ok(())
}

/// Result type for config file loading - includes both successful loads and parse errors
#[derive(Debug, Clone)]
pub enum ConfigResult<T> {
    /// Successfully parsed config
    Ok(T),
    /// Failed to parse - shows error in UI
    Err { path: PathBuf, error: String },
}

/// Extract ID from a file path (filename without extension)
pub fn id_from_path(path: &std::path::Path) -> Option<String> {
    path.file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
}

/// Generate a slug from a name (lowercase, hyphens, no special chars)
pub fn slugify(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

/// Copy files from source to dest directory, skipping files that already exist.
pub fn copy_dir_contents(
    source_dir: &Path,
    dest_dir: &Path,
    extension: &str,
) -> Result<(), String> {
    let entries = std::fs::read_dir(source_dir)
        .map_err(|e| format!("Failed to read directory {:?}: {}", source_dir, e))?;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
        let path = entry.path();

        // Skip directories and files with wrong extension
        if path.is_dir() {
            continue;
        }

        let file_ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if file_ext != extension && !(extension == "yaml" && file_ext == "yml") {
            continue;
        }

        // Get filename and determine destination
        let filename = match path.file_name() {
            Some(name) => name,
            None => continue,
        };

        let dest_path = dest_dir.join(filename);

        // Only copy if destination doesn't exist
        if !dest_path.exists() {
            std::fs::copy(&path, &dest_path)
                .map_err(|e| format!("Failed to copy {:?} to {:?}: {}", path, dest_path, e))?;
            log::info!("Copied bundled default: {:?}", dest_path);
        }
    }

    Ok(())
}

/// Copy files from source to dest directory, always overwriting.
/// Collects restored filenames into the provided list.
pub fn force_copy_dir_contents(
    source_dir: &Path,
    dest_dir: &Path,
    extension: &str,
    restored: &mut Vec<String>,
) -> Result<(), String> {
    let entries = std::fs::read_dir(source_dir)
        .map_err(|e| format!("Failed to read directory {:?}: {}", source_dir, e))?;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
        let path = entry.path();

        // Skip directories and files with wrong extension
        if path.is_dir() {
            continue;
        }

        let file_ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if file_ext != extension && !(extension == "yaml" && file_ext == "yml") {
            continue;
        }

        // Get filename and determine destination
        let filename = match path.file_name() {
            Some(name) => name,
            None => continue,
        };

        let dest_path = dest_dir.join(filename);

        // Always copy (overwrite if exists)
        std::fs::copy(&path, &dest_path)
            .map_err(|e| format!("Failed to copy {:?} to {:?}: {}", path, dest_path, e))?;

        // Track the restored file (without extension)
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            restored.push(stem.to_string());
        }

        log::info!("Restored bundled default: {:?}", dest_path);
    }

    Ok(())
}

/// Result of restoring bundled defaults
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreResult {
    pub recipes_restored: Vec<String>,
    pub agents_restored: Vec<String>,
    pub skills_restored: Vec<String>,
}

/// Get a recipe by ID from files (for use by execution and snapshot builders).
///
/// This loads recipes from YAML files rather than the database, since files
/// are the source of truth for recipe configuration.
pub fn get_recipe_from_files(
    config_dir: &std::path::Path,
    project_path: Option<&std::path::Path>,
    recipe_id: &str,
) -> Result<crate::models::Recipe, String> {
    log::info!(
        "get_recipe_from_files: loading recipe_id={} from project_path={:?}",
        recipe_id,
        project_path
    );

    let file_recipe = recipes::get_recipe(config_dir, recipe_id, project_path)?
        .ok_or_else(|| format!("Recipe not found: {}", recipe_id))?;

    log::info!(
        "get_recipe_from_files: loaded file_recipe with {} nodes, {} edges, file_path={:?}",
        file_recipe.recipe.nodes.len(),
        file_recipe.recipe.edges.len(),
        file_recipe.file_path
    );

    // Determine workspace_id and project_id based on scope
    let (ws_id, proj_id) = if file_recipe.is_project_scoped {
        (None, None) // Project ID will be set by caller if needed
    } else {
        (Some("default".to_string()), None)
    };

    let recipe = file_recipe_to_recipe(file_recipe, ws_id, proj_id);
    log::info!(
        "get_recipe_from_files: after conversion, recipe has {} nodes, {} edges",
        recipe.nodes.len(),
        recipe.edges.len()
    );

    Ok(recipe)
}

/// Convert a FileRecipe to a Recipe with scope metadata
pub fn file_recipe_to_recipe(
    file_recipe: recipes::FileRecipe,
    workspace_id: Option<String>,
    project_id: Option<String>,
) -> crate::models::Recipe {
    let mut recipe = file_recipe.recipe;
    // Set workspace_id to "default" for workspace-scoped recipes if not provided
    recipe.workspace_id = if !file_recipe.is_project_scoped {
        workspace_id.or_else(|| Some("default".to_string()))
    } else {
        None
    };
    recipe.project_id = project_id;
    recipe
}

/// Get a recipe as a snapshot format, along with all agents referenced by the recipe.
///
/// This loads a recipe from files and converts it to RecipeSnapshot format.
/// Used by the SnapshotRecipeEditorDialog for customizing recipes before execution,
/// and by cairn-server for the same purpose.
pub fn get_recipe_as_snapshot(
    conn: &mut SqliteConnection,
    config_dir: &Path,
    recipe_id: &str,
    project_id: &str,
    default_model: &crate::models::Model,
) -> Result<
    (
        crate::models::RecipeSnapshot,
        std::collections::HashMap<String, crate::models::AgentSnapshot>,
    ),
    String,
> {
    use crate::models::{AgentSnapshot, RecipeNodeType, RecipeSnapshot};
    use std::collections::HashMap;

    // Get project path for file-based config lookup
    let project_path = get_project_path(conn, project_id)?;

    // Load the recipe from files
    let recipe = get_recipe_from_files(config_dir, Some(&project_path), recipe_id)?;

    // Build recipe snapshot
    let recipe_snapshot = RecipeSnapshot {
        id: recipe.id.clone(),
        name: recipe.name,
        description: recipe.description,
        trigger: recipe.trigger,
        context: recipe.context,
        nodes: recipe.nodes.clone(),
        edges: recipe.edges,
    };

    // Find all agent IDs referenced by agent nodes
    let agent_ids: Vec<String> = recipe
        .nodes
        .iter()
        .filter(|n| n.node_type == RecipeNodeType::Agent)
        .filter_map(|n| {
            n.agent_config
                .as_ref()
                .and_then(|cfg| cfg.agent_config_id.clone())
        })
        .collect();

    // Load agents
    let mut agents: HashMap<String, AgentSnapshot> = HashMap::new();
    for agent_id in agent_ids {
        if agents.contains_key(&agent_id) {
            continue;
        }

        if let Ok(Some(file_agent)) = agents::get_agent(config_dir, &agent_id, Some(&project_path))
        {
            let resolved_model = file_agent
                .model
                .clone()
                .unwrap_or_else(|| default_model.clone());

            agents.insert(
                agent_id.clone(),
                AgentSnapshot {
                    id: file_agent.id,
                    name: file_agent.name,
                    description: file_agent.description,
                    prompt: file_agent.prompt,
                    tools: file_agent.tools,
                    model: Some(resolved_model),
                    disallowed_tools: file_agent.disallowed_tools,
                    skills: file_agent.skills,
                    permission_mode: file_agent.permission_mode,
                },
            );
        }
    }

    Ok((recipe_snapshot, agents))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slugify() {
        assert_eq!(slugify("My Agent"), "my-agent");
        assert_eq!(slugify("test--skill"), "test-skill");
        assert_eq!(slugify("Recipe Name!"), "recipe-name");
    }

    #[test]
    fn test_id_from_path() {
        let path = PathBuf::from("/home/user/.cairn/agents/my-agent.md");
        assert_eq!(id_from_path(&path), Some("my-agent".to_string()));

        let path = PathBuf::from("/home/user/.cairn/recipes/test-recipe.yaml");
        assert_eq!(id_from_path(&path), Some("test-recipe".to_string()));
    }
}
