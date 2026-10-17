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
    #[serde(default)]
    pub bundles: Vec<String>,
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

/// What a recipe id is, for refusals that name the rule.
const RECIPE_ID_SHAPE: &str = "a recipe id is a plain name of letters, digits, `-` and `_` (the recipe file's name without its extension), never a path or a file name";

/// Whether `id` is a recipe id: a plain name of letters, digits, `-` and `_`.
///
/// A recipe's identity is the stem of its file, and policy that names a
/// recipe (the tenet-gated list, contextual-package selection) compares ids,
/// so an id must not be able to reach a file by another spelling: no path
/// separators, no `.` (so no `..`, no extension, no hidden name).
pub fn is_recipe_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
}

/// The refusal for a string that is not a recipe id, naming the recipe it
/// most plausibly meant when one resolves (`./planbuild` and
/// `recipes/planbuild.yaml` both suggest `planbuild`).
fn recipe_id_refusal(config_dir: &Path, id: &str, project_path: Option<&Path>) -> String {
    let last = id.rsplit(['/', '\\']).next().unwrap_or(id).trim();
    let stem = last
        .strip_suffix(".yaml")
        .or_else(|| last.strip_suffix(".yml"))
        .unwrap_or(last)
        .trim_start_matches('.');
    let canonical = is_recipe_id(stem)
        .then(|| {
            find_recipe_file(config_dir, stem, project_path)
                .ok()
                .flatten()
        })
        .flatten()
        .and_then(|(path, _)| id_from_path(&path));
    match canonical {
        Some(canonical) => {
            format!("`{id}` is not a recipe id: {RECIPE_ID_SHAPE}. Use `{canonical}`.")
        }
        None => format!("`{id}` is not a recipe id: {RECIPE_ID_SHAPE}, like `build`."),
    }
}

/// The file recipe `id` names, and whether it is project-scoped.
///
/// Project scope shadows workspace scope. Within a directory the file whose
/// stem is exactly `id` wins, else the one whose stem matches it ignoring
/// ASCII case, `.yaml` before `.yml`. Matching is done here against the
/// directory's entries rather than by joining `id` into a path, so the
/// filesystem's own case folding never decides which file an id names, and
/// the resolved file's stem is the recipe's canonical id on every platform.
fn find_recipe_file(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<Option<(PathBuf, bool)>, String> {
    for (dir, is_project_scoped) in super::config_root_subdirs(config_dir, project_path, "recipes")
    {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        // (exact, extension rank, stem, path): sorting puts the best first.
        let mut candidates: Vec<(bool, u8, String, PathBuf)> = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            let Some((stem, ext)) = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.rsplit_once('.'))
            else {
                continue;
            };
            let rank = match ext {
                "yaml" => 0,
                "yml" => 1,
                _ => continue,
            };
            if !stem.eq_ignore_ascii_case(id) || path.is_dir() {
                continue;
            }
            candidates.push((stem != id, rank, stem.to_string(), path));
        }
        candidates.sort();
        let Some((inexact, _, stem, path)) = candidates.first() else {
            continue;
        };
        if *inexact {
            if let Some((_, _, other, _)) = candidates.iter().find(|c| &c.2 != stem) {
                return Err(format!(
                    "recipe id `{id}` matches both `{stem}` and `{other}` in {}; name one exactly",
                    dir.display()
                ));
            }
        }
        return Ok(Some((path.clone(), is_project_scoped)));
    }
    Ok(None)
}

/// Get a specific recipe by ID (filename without extension)
///
/// Checks project-scoped first (if project_path provided), then workspace-scoped.
/// The loaded recipe's id is its file's stem, which may differ from `id` in
/// ASCII case only. A string that is not a recipe id ([`is_recipe_id`]) is
/// refused rather than looked up.
pub fn get_recipe(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<Option<FileRecipe>, String> {
    if !is_recipe_id(id) {
        return Err(recipe_id_refusal(config_dir, id, project_path));
    }
    match find_recipe_file(config_dir, id, project_path)? {
        Some((path, is_project_scoped)) => match load_recipe_file(&path, is_project_scoped) {
            ConfigResult::Ok(recipe) => Ok(Some(recipe)),
            ConfigResult::Err { error, .. } => Err(error),
        },
        None => Ok(None),
    }
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
        if !is_recipe_id(&file_recipe.recipe.id) {
            return Err(format!(
                "cannot save recipe `{}`: {RECIPE_ID_SHAPE}",
                file_recipe.recipe.id
            ));
        }
        let filename = format!("{}.yaml", file_recipe.recipe.id);
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
    let mut recipe_file: RecipeFile = file_recipe.recipe.clone().into();
    recipe_file.bundles = file_recipe.bundles.clone();
    let yaml = recipe_file.to_yaml()?;

    // Write file
    std::fs::write(&path, yaml).map_err(|e| format!("Failed to write recipe file: {}", e))?;

    Ok(path)
}

/// Delete the recipe file `id` names, the same file [`get_recipe`] loads for
/// it. Deleting a recipe that does not exist is not an error.
pub fn delete_recipe(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<(), String> {
    if !is_recipe_id(id) {
        return Err(recipe_id_refusal(config_dir, id, project_path));
    }
    let Some((path, is_project_scoped)) = find_recipe_file(config_dir, id, project_path)? else {
        return Ok(());
    };
    std::fs::remove_file(&path).map_err(|e| format!("Failed to delete recipe file: {}", e))?;
    if !is_project_scoped {
        let canonical = id_from_path(&path).unwrap_or_else(|| id.to_string());
        super::pack::note_removed_item(config_dir, super::pack::PackItemKind::Recipe, &canonical);
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
    if !is_recipe_id(&id) {
        return ConfigResult::Err {
            path: path.to_path_buf(),
            error: format!(
                "the recipe file name `{id}` is not a recipe id: {RECIPE_ID_SHAPE}. Rename the file."
            ),
        };
    }

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
        Ok(mut recipe_file) => {
            if let Err(error) =
                super::contextual_packages::normalize_bundles(&mut recipe_file.bundles)
            {
                return ConfigResult::Err {
                    path: path.to_path_buf(),
                    error,
                };
            }
            // Validate the recipe
            let validation = recipe_file.validate();
            if !validation.valid {
                return ConfigResult::Err {
                    path: path.to_path_buf(),
                    error: format!("Invalid recipe: {}", validation.errors.join(", ")),
                };
            }

            // Convert to internal Recipe format
            let bundles = recipe_file.bundles.clone();
            let mut recipe = recipe_file.into_recipe(None, None);
            // Override the generated ID with the filename-based ID
            recipe.id = id;

            ConfigResult::Ok(FileRecipe {
                recipe,
                bundles,
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
            branch_targets: cairn_db::models::default_branch_targets(),
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
                decision_config: None,
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
            bundles: Vec::new(),
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
            bundles: Vec::new(),
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
            bundles: Vec::new(),
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
            bundles: Vec::new(),
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
            bundles: Vec::new(),
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

    /// A path, a file name, or a dotted name is not a recipe id: every caller
    /// is refused, with the canonical id named when one resolves, and no file
    /// outside a recipes directory is ever loaded, saved, or deleted.
    #[test]
    fn noncanonical_recipe_ids_are_refused_for_every_operation() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        save_recipe(
            &config_dir,
            &FileRecipe {
                recipe: minimal_recipe("planbuild", "Plan Build"),
                bundles: Vec::new(),
                is_project_scoped: false,
                file_path: PathBuf::new(),
            },
            None,
        )
        .unwrap();
        let absolute = config_dir.join("recipes/planbuild").display().to_string();
        for spelling in [
            "./planbuild",
            "planbuild.yaml",
            "planbuild.yml",
            ".planbuild",
            "../recipes/planbuild",
            "recipes\\planbuild",
            " planbuild",
            absolute.as_str(),
        ] {
            let refusal = get_recipe(&config_dir, spelling, None).unwrap_err();
            assert!(
                refusal.contains("is not a recipe id") && refusal.ends_with("Use `planbuild`."),
                "{spelling}: {refusal}"
            );
            assert!(delete_recipe(&config_dir, spelling, None).is_err());
        }
        let unknown = get_recipe(&config_dir, "../nowhere", None).unwrap_err();
        assert!(unknown.ends_with("like `build`."), "{unknown}");
        assert!(get_recipe(&config_dir, "", None).is_err());

        let escaping = FileRecipe {
            recipe: minimal_recipe("../escaped", "Escaped"),
            bundles: Vec::new(),
            is_project_scoped: false,
            file_path: PathBuf::new(),
        };
        assert!(save_recipe(&config_dir, &escaping, None).is_err());
        assert!(!config_dir.join("escaped.yaml").exists());
        assert!(config_dir.join("recipes/planbuild.yaml").exists());
    }

    /// An id resolves to its file ignoring ASCII case on every platform, and
    /// the loaded recipe carries the file's own stem as its id; an exact match
    /// wins over a case variant.
    #[test]
    fn a_case_variant_resolves_to_the_files_own_id() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        let save = |id: &str| {
            save_recipe(
                &config_dir,
                &FileRecipe {
                    recipe: minimal_recipe(id, id),
                    bundles: Vec::new(),
                    is_project_scoped: false,
                    file_path: PathBuf::new(),
                },
                None,
            )
            .unwrap()
        };
        save("PlanCoord");
        let loaded = get_recipe(&config_dir, "plancoord", None)
            .unwrap()
            .expect("a case variant resolves");
        assert_eq!(loaded.recipe.id, "PlanCoord");
        assert_eq!(
            get_recipe(&config_dir, "PlanCoord", None)
                .unwrap()
                .unwrap()
                .recipe
                .id,
            "PlanCoord"
        );
        delete_recipe(&config_dir, "PLANCOORD", None).unwrap();
        assert!(get_recipe(&config_dir, "PlanCoord", None)
            .unwrap()
            .is_none());
    }

    /// A file whose name is not a recipe id lists as an error naming the fix,
    /// rather than as a recipe no id can reach.
    #[test]
    fn a_recipe_file_with_a_noncanonical_name_lists_as_an_error() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("v1.2.yaml");
        std::fs::write(
            &path,
            "cairnVersion: 1\nname: Dotted\ntrigger: manual\nnodes: []\nedges: []\n",
        )
        .unwrap();
        match load_recipe_file(&path, false) {
            ConfigResult::Err { error, .. } => {
                assert!(error.contains("Rename the file"), "{error}")
            }
            ConfigResult::Ok(_) => panic!("a dotted file name is not a recipe id"),
        }
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
