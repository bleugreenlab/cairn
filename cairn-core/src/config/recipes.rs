//! Recipe configuration file reading.
//!
//! Recipes are stored as YAML files.
//! - Workspace-scoped: `~/.cairn/recipes/`
//! - Project-scoped: `[project-dir]/.cairn/recipes/`

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::models::{Recipe, RecipeFile};

use super::{id_from_path, ConfigResult};

/// Recipe loaded from a file (uses internal Recipe format)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileRecipe {
    /// The loaded recipe in internal format
    #[serde(flatten)]
    pub recipe: Recipe,
    /// Whether this recipe is project-scoped (vs workspace-scoped)
    pub is_project_scoped: bool,
    /// Path to the source file
    pub file_path: PathBuf,
}

/// List all recipes from files
///
/// Reads workspace-scoped recipes from `~/.cairn/recipes/`.
/// If project_path is Some, also includes recipes from `[project_path]/.cairn/recipes/`.
pub fn list_recipes(
    config_dir: &Path,
    project_path: Option<&Path>,
) -> Result<Vec<ConfigResult<FileRecipe>>, String> {
    let mut results = vec![];

    // Read workspace-scoped recipes
    let ws_dir = config_dir.join("recipes");
    if ws_dir.exists() {
        for entry in std::fs::read_dir(&ws_dir)
            .map_err(|e| format!("Failed to read recipes directory: {}", e))?
        {
            let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
            let path = entry.path();

            // Skip directories
            if path.is_dir() {
                continue;
            }

            // Accept both .yaml and .yml
            let ext = path.extension().and_then(|e| e.to_str());
            if ext != Some("yaml") && ext != Some("yml") {
                continue;
            }

            results.push(load_recipe_file(&path, false));
        }
    }

    // Read project-scoped recipes if project path specified
    if let Some(proj_path) = project_path {
        let proj_dir = proj_path.join(".cairn").join("recipes");
        if proj_dir.exists() && proj_dir.is_dir() {
            for entry in std::fs::read_dir(&proj_dir)
                .map_err(|e| format!("Failed to read project recipes directory: {}", e))?
            {
                let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
                let path = entry.path();

                let ext = path.extension().and_then(|e| e.to_str());
                if ext != Some("yaml") && ext != Some("yml") {
                    continue;
                }

                results.push(load_recipe_file(&path, true));
            }
        }
    }

    Ok(results)
}

/// Get a specific recipe by ID (filename without extension)
///
/// Checks project-scoped first (if project_path provided), then workspace-scoped.
pub fn get_recipe(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<Option<FileRecipe>, String> {
    // Try project-scoped first if project specified
    if let Some(proj_path) = project_path {
        for ext in ["yaml", "yml"] {
            let path = proj_path
                .join(".cairn")
                .join("recipes")
                .join(format!("{}.{}", id, ext));
            if path.exists() {
                return match load_recipe_file(&path, true) {
                    ConfigResult::Ok(recipe) => Ok(Some(recipe)),
                    ConfigResult::Err { error, .. } => Err(error),
                };
            }
        }
    }

    // Try workspace-scoped
    let ws_dir = config_dir.join("recipes");
    for ext in ["yaml", "yml"] {
        let path = ws_dir.join(format!("{}.{}", id, ext));
        if path.exists() {
            return match load_recipe_file(&path, false) {
                ConfigResult::Ok(recipe) => Ok(Some(recipe)),
                ConfigResult::Err { error, .. } => Err(error),
            };
        }
    }

    Ok(None)
}

/// Save a recipe to a file
pub fn save_recipe(
    config_dir: &Path,
    file_recipe: &FileRecipe,
    project_path: Option<&Path>,
) -> Result<PathBuf, String> {
    // Determine target path
    let path = if file_recipe.file_path.as_os_str().is_empty() {
        // No existing path - determine from scope
        let filename = format!("{}.yaml", crate::config::slugify(&file_recipe.recipe.name));
        if file_recipe.is_project_scoped {
            let proj_path =
                project_path.ok_or("Project path required for project-scoped recipe")?;
            let dir = proj_path.join(".cairn").join("recipes");
            std::fs::create_dir_all(&dir)
                .map_err(|e| format!("Failed to create project recipes directory: {}", e))?;
            dir.join(&filename)
        } else {
            let dir = config_dir.join("recipes");
            std::fs::create_dir_all(&dir)
                .map_err(|e| format!("Failed to create recipes directory: {}", e))?;
            dir.join(&filename)
        }
    } else {
        // Use existing path, ensure parent directory exists
        if let Some(parent) = file_recipe.file_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create directory: {}", e))?;
        }
        file_recipe.file_path.clone()
    };

    // Convert to RecipeFile for serialization
    let recipe_file: RecipeFile = file_recipe.recipe.clone().into();
    let yaml = recipe_file.to_yaml()?;

    // Write file
    std::fs::write(&path, yaml).map_err(|e| format!("Failed to write recipe file: {}", e))?;

    Ok(path)
}

/// Delete a recipe file
pub fn delete_recipe(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<(), String> {
    // Try project-scoped first if project specified
    if let Some(proj_path) = project_path {
        for ext in ["yaml", "yml"] {
            let path = proj_path
                .join(".cairn")
                .join("recipes")
                .join(format!("{}.{}", id, ext));
            if path.exists() {
                std::fs::remove_file(&path)
                    .map_err(|e| format!("Failed to delete recipe file: {}", e))?;
                return Ok(());
            }
        }
    }

    // Try workspace-scoped
    let ws_dir = config_dir.join("recipes");
    for ext in ["yaml", "yml"] {
        let path = ws_dir.join(format!("{}.{}", id, ext));
        if path.exists() {
            std::fs::remove_file(&path)
                .map_err(|e| format!("Failed to delete recipe file: {}", e))?;
            return Ok(());
        }
    }

    Ok(())
}

/// Load a single recipe file
fn load_recipe_file(path: &Path, is_project_scoped: bool) -> ConfigResult<FileRecipe> {
    let id = match id_from_path(path) {
        Some(id) => id,
        None => {
            return ConfigResult::Err {
                path: path.to_path_buf(),
                error: "Could not determine recipe ID from filename".to_string(),
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

    match RecipeFile::from_yaml(&content) {
        Ok(recipe_file) => {
            // Validate the recipe
            let validation = recipe_file.validate();
            if !validation.valid {
                return ConfigResult::Err {
                    path: path.to_path_buf(),
                    error: format!("Invalid recipe: {}", validation.errors.join(", ")),
                };
            }

            // Convert to internal Recipe format
            let mut recipe = recipe_file.into_recipe(None, None);
            // Override the generated ID with the filename-based ID
            recipe.id = id;

            ConfigResult::Ok(FileRecipe {
                recipe,
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
    fn test_load_recipe_roundtrip() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("test-recipe.yaml");

        let content = r#"cairnVersion: 1
name: Test Recipe
description: A test recipe
trigger: issue
context: issue

nodes:
  - id: trigger-1
    type: trigger
    name: Trigger
    position: 0@0

edges: []
"#;

        std::fs::write(&path, content).unwrap();

        let result = load_recipe_file(&path, false);
        match result {
            ConfigResult::Ok(file_recipe) => {
                assert_eq!(file_recipe.recipe.id, "test-recipe");
                assert_eq!(file_recipe.recipe.name, "Test Recipe");
                assert_eq!(
                    file_recipe.recipe.description,
                    Some("A test recipe".to_string())
                );
                assert!(!file_recipe.is_project_scoped);
                assert_eq!(file_recipe.recipe.nodes.len(), 1);
            }
            ConfigResult::Err { error, .. } => panic!("Failed to load: {}", error),
        }
    }
}
