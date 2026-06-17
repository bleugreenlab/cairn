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

    for (dir, is_project_scoped) in super::config_root_subdirs(config_dir, project_path, "recipes")
    {
        if !dir.exists() || !dir.is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(&dir)
            .map_err(|e| format!("Failed to read recipes directory: {}", e))?
        {
            let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
            let path = entry.path();
            if path.is_dir() {
                continue;
            }
            let ext = path.extension().and_then(|e| e.to_str());
            if ext != Some("yaml") && ext != Some("yml") {
                continue;
            }
            results.push(load_recipe_file(&path, is_project_scoped));
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
    for (dir, is_project_scoped) in super::config_root_subdirs(config_dir, project_path, "recipes")
    {
        for ext in ["yaml", "yml"] {
            let path = dir.join(format!("{}.{}", id, ext));
            if path.exists() {
                return match load_recipe_file(&path, is_project_scoped) {
                    ConfigResult::Ok(recipe) => Ok(Some(recipe)),
                    ConfigResult::Err { error, .. } => Err(error),
                };
            }
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
        let filename = format!("{}.yaml", &file_recipe.recipe.id);
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

    fn minimal_recipe(id: &str, name: &str) -> Recipe {
        use crate::models::{NodePosition, RecipeNode, RecipeNodeType, RecipeTrigger};
        Recipe {
            id: id.to_string(),
            name: name.to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            workspace_id: Some("default".to_string()),
            project_id: None,
            is_system: false,
            version: 1,
            parent_recipe_id: None,
            child_recipe_id: None,
            nodes: vec![RecipeNode {
                id: "t1".to_string(),
                node_type: RecipeNodeType::Trigger,
                name: "Trigger".to_string(),
                position: NodePosition { x: 0.0, y: 0.0 },
                parent_id: None,
                trigger_config: None,
                agent_config: None,
                action_config: None,
                checkpoint_config: None,
                artifact_config: None,
                condition_config: None,
                context_config: None,
            }],
            edges: vec![],
            created_at: 0,
            updated_at: 0,
        }
    }

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

    /// save_recipe with empty file_path should use recipe.id for the filename,
    /// NOT slugify(recipe.name). This was a bug where a recipe named
    /// "My Recipe" would be saved as "my-recipe.yaml" instead of "{id}.yaml",
    /// causing get_recipe (which looks up by id) to fail to find it.
    #[test]
    fn save_recipe_uses_id_for_filename() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path();

        let recipe = minimal_recipe("custom-id", "My Fancy Recipe Name");
        let file_recipe = FileRecipe {
            recipe,
            is_project_scoped: false,
            file_path: PathBuf::new(), // empty → should derive from id
        };

        let saved_path = save_recipe(config_dir, &file_recipe, None).unwrap();

        // The filename must be "{id}.yaml", not "my-fancy-recipe-name.yaml"
        assert_eq!(
            saved_path.file_name().unwrap().to_str().unwrap(),
            "custom-id.yaml"
        );

        // Round-trip: get_recipe should find it by id
        let loaded = get_recipe(config_dir, "custom-id", None)
            .unwrap()
            .expect("should find recipe by id");
        assert_eq!(loaded.recipe.name, "My Fancy Recipe Name");
    }

    /// save_recipe with an explicit file_path should use that path, not derive one.
    #[test]
    fn save_recipe_respects_existing_file_path() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path();
        let explicit_path = config_dir.join("recipes").join("explicit-name.yaml");

        let recipe = minimal_recipe("some-id", "Some Recipe");
        let file_recipe = FileRecipe {
            recipe,
            is_project_scoped: false,
            file_path: explicit_path.clone(),
        };

        let saved_path = save_recipe(config_dir, &file_recipe, None).unwrap();
        assert_eq!(saved_path, explicit_path);
    }

    /// save_recipe for a project-scoped recipe with empty file_path should
    /// place the file under the project's .cairn/recipes/ directory using the id.
    #[test]
    fn save_recipe_project_scoped_uses_id() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        let project_dir = temp.path().join("project");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::create_dir_all(&project_dir).unwrap();

        let mut recipe = minimal_recipe("proj-recipe", "Project Recipe");
        recipe.workspace_id = None;
        recipe.project_id = Some("proj-1".to_string());

        let file_recipe = FileRecipe {
            recipe,
            is_project_scoped: true,
            file_path: PathBuf::new(),
        };

        let saved_path = save_recipe(&config_dir, &file_recipe, Some(&project_dir)).unwrap();

        assert_eq!(
            saved_path,
            project_dir
                .join(".cairn")
                .join("recipes")
                .join("proj-recipe.yaml")
        );

        // Verify it can be loaded back from the project scope
        let loaded = get_recipe(&config_dir, "proj-recipe", Some(&project_dir))
            .unwrap()
            .expect("should find project-scoped recipe");
        assert!(loaded.is_project_scoped);
    }

    /// get_recipe should prefer project scope over workspace when both exist.
    #[test]
    fn get_recipe_project_shadows_workspace() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        let project_dir = temp.path().join("project");

        // Create workspace recipe
        let ws_recipe = minimal_recipe("shared", "Workspace Version");
        let ws_file = FileRecipe {
            recipe: ws_recipe,
            is_project_scoped: false,
            file_path: PathBuf::new(),
        };
        save_recipe(&config_dir, &ws_file, None).unwrap();

        // Create project recipe with same id
        let mut proj_recipe = minimal_recipe("shared", "Project Version");
        proj_recipe.workspace_id = None;
        proj_recipe.project_id = Some("p1".to_string());
        let proj_file = FileRecipe {
            recipe: proj_recipe,
            is_project_scoped: true,
            file_path: PathBuf::new(),
        };
        save_recipe(&config_dir, &proj_file, Some(&project_dir)).unwrap();

        // With project context, project version wins
        let loaded = get_recipe(&config_dir, "shared", Some(&project_dir))
            .unwrap()
            .expect("should find recipe");
        assert_eq!(loaded.recipe.name, "Project Version");
        assert!(loaded.is_project_scoped);

        // Without project context, workspace version is returned
        let loaded_ws = get_recipe(&config_dir, "shared", None)
            .unwrap()
            .expect("should find workspace recipe");
        assert_eq!(loaded_ws.recipe.name, "Workspace Version");
        assert!(!loaded_ws.is_project_scoped);
    }

    /// A `system: true` recipe file loads with is_system set; the picker filter
    /// (`list_recipes_for_context`) keys off this flag.
    #[test]
    fn load_recipe_reads_system_flag() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("system-recipe.yaml");
        let content = r#"cairnVersion: 1
name: System Recipe
trigger: manual
system: true
nodes:
  - id: trigger-1
    type: trigger
    name: Trigger
    position: 0@0
edges: []
"#;
        std::fs::write(&path, content).unwrap();
        match load_recipe_file(&path, false) {
            ConfigResult::Ok(fr) => assert!(fr.recipe.is_system),
            ConfigResult::Err { error, .. } => panic!("Failed to load: {}", error),
        }
    }

    /// A recipe file with no `system:` key defaults to is_system == false, so
    /// ordinary recipes keep showing in the issue-create picker.
    #[test]
    fn load_recipe_without_system_key_defaults_false() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("plain-recipe.yaml");
        let content = r#"cairnVersion: 1
name: Plain Recipe
trigger: manual
nodes:
  - id: trigger-1
    type: trigger
    name: Trigger
    position: 0@0
edges: []
"#;
        std::fs::write(&path, content).unwrap();
        match load_recipe_file(&path, false) {
            ConfigResult::Ok(fr) => assert!(!fr.recipe.is_system),
            ConfigResult::Err { error, .. } => panic!("Failed to load: {}", error),
        }
    }
}
