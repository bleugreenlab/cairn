//! Orchestrator recipe operations.
//!
//! File-based recipe management: workspace-scoped (`~/.cairn/recipes/`) vs
//! project-scoped (`[project]/.cairn/recipes/`).

use std::path::{Path, PathBuf};

use crate::config::{recipes as config_recipes, slugify, ConfigResult};
use crate::models::{
    CreateRecipe, Recipe, RecipeFile, RecipeFileValidation, RecipeTrigger, RecipeVersionInfo,
    UpdateRecipe,
};

use super::config_resource::{self, merge_optional, ConfigResource};
use super::Orchestrator;

/// Convert a file recipe to the API model.
fn file_recipe_to_model(
    file_recipe: config_recipes::FileRecipe,
    workspace_id: Option<String>,
    project_id: Option<String>,
    _now: i64,
) -> Recipe {
    let mut recipe = file_recipe.recipe;
    recipe.workspace_id = workspace_id;
    recipe.project_id = project_id;
    recipe
}

pub(crate) struct RecipeResource;

impl ConfigResource for RecipeResource {
    type Config = Recipe;
    type File = config_recipes::FileRecipe;
    type CreateInput = CreateRecipe;
    type UpdateInput = UpdateRecipe;

    const ENTITY_TYPE: &'static str = "recipe";
    const TABLE: &'static str = "recipes";
    const SUBDIR: &'static str = "recipes";
    const GET_SEARCHES_ALL_PROJECTS: bool = false;
    const UPDATE_SEARCHES_ALL_PROJECTS: bool = false;

    fn list_files(
        config_dir: &Path,
        project_path: Option<&Path>,
    ) -> Result<Vec<ConfigResult<Self::File>>, String> {
        config_recipes::list_recipes(config_dir, project_path)
    }

    fn get_file(
        config_dir: &Path,
        id: &str,
        project_path: Option<&Path>,
    ) -> Result<Option<Self::File>, String> {
        config_recipes::get_recipe(config_dir, id, project_path)
    }

    fn save_file(
        config_dir: &Path,
        file: &Self::File,
        project_path: Option<&Path>,
    ) -> Result<PathBuf, String> {
        config_recipes::save_recipe(config_dir, file, project_path)
    }

    fn delete_file(config_dir: &Path, id: &str, project_path: Option<&Path>) -> Result<(), String> {
        config_recipes::delete_recipe(config_dir, id, project_path)
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
        file_recipe_to_model(file, workspace_id, project_id, now)
    }

    fn config_name(cfg: &Self::Config) -> &str {
        &cfg.name
    }

    fn config_id(cfg: &Self::Config) -> String {
        cfg.id.clone()
    }

    fn create_id(input: &Self::CreateInput) -> String {
        slugify(&input.name)
    }

    fn create_input_scopes(input: &Self::CreateInput) -> (Option<String>, Option<String>) {
        (input.workspace_id.clone(), input.project_id.clone())
    }

    fn build_create_file(
        input: Self::CreateInput,
        id: String,
        is_project_scoped: bool,
        now: i64,
    ) -> Self::File {
        let recipe = Recipe {
            id,
            name: input.name,
            description: input.description,
            trigger: input.trigger.unwrap_or_default(),
            workspace_id: input.workspace_id,
            project_id: input.project_id,
            is_default: false,
            version: 1,
            parent_recipe_id: None,
            child_recipe_id: None,
            nodes: input.nodes.unwrap_or_default(),
            edges: input.edges.unwrap_or_default(),
            created_at: now,
            updated_at: now,
        };
        config_recipes::FileRecipe {
            recipe,
            is_project_scoped,
            file_path: PathBuf::new(),
        }
    }

    fn update_scope(
        _existing: &Self::File,
        input: &Self::UpdateInput,
        target_project_id: Option<&str>,
    ) -> (bool, Option<String>) {
        let project_id = input.project_id.as_ref().and_then(Clone::clone);
        let scope_is_changing = project_id != target_project_id.map(|s| s.to_string());
        if scope_is_changing {
            (project_id.is_some(), project_id)
        } else {
            (target_project_id.is_some(), project_id)
        }
    }

    fn build_update_file(
        existing: &Self::File,
        input: Self::UpdateInput,
        _id: &str,
        new_is_project_scoped: bool,
        scope_changing: bool,
        now: i64,
    ) -> Self::File {
        let mut recipe = existing.recipe.clone();
        if let Some(name) = input.name {
            recipe.name = name;
        }
        recipe.description = merge_optional(&recipe.description, &input.description);
        if let Some(trigger) = input.trigger {
            recipe.trigger = trigger;
        }
        if let Some(nodes) = input.nodes {
            recipe.nodes = nodes;
        }
        if let Some(edges) = input.edges {
            recipe.edges = edges;
        }
        recipe.workspace_id = if new_is_project_scoped {
            None
        } else {
            Some("default".to_string())
        };
        recipe.project_id = input.project_id.and_then(|p| p);
        recipe.updated_at = now;

        config_recipes::FileRecipe {
            recipe,
            is_project_scoped: new_is_project_scoped,
            file_path: if scope_changing {
                PathBuf::new()
            } else {
                existing.file_path.clone()
            },
        }
    }

    fn cleanup_after_scope_change(existing: &Self::File, _dest_path: &Path) {
        if let Err(e) = std::fs::remove_file(&existing.file_path) {
            log::warn!(
                "Failed to delete old recipe file {:?}: {}",
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
        let dir = root.join(Self::SUBDIR);
        ["yaml", "yml"]
            .into_iter()
            .any(|ext| dir.join(format!("{}.{}", id, ext)).exists())
    }

    fn build_scoped_copy(
        source: &Self::File,
        id: &str,
        is_project_scoped: bool,
        workspace_id: Option<String>,
        project_id: Option<String>,
        now: i64,
    ) -> Self::File {
        let mut recipe = source.recipe.clone();
        recipe.id = id.to_string();
        recipe.workspace_id = workspace_id;
        recipe.project_id = project_id;
        if is_project_scoped {
            recipe.is_default = false;
        }
        recipe.created_at = now;
        recipe.updated_at = now;

        config_recipes::FileRecipe {
            recipe,
            is_project_scoped,
            file_path: PathBuf::new(),
        }
    }

    fn primary_path(file: &Self::File) -> PathBuf {
        file.file_path.clone()
    }

    fn post_save_copy(_source: &Self::File, _dest_path: &Path) -> Result<(), String> {
        Ok(())
    }
}

struct RecipeChangeEvent<'a> {
    event_id: &'a str,
    config_action: &'a str,
    db_action: &'a str,
}

impl Orchestrator {
    fn save_recipe_change(
        &self,
        recipe: Recipe,
        is_project_scoped: bool,
        file_path: PathBuf,
        project_path: Option<&Path>,
        event: RecipeChangeEvent<'_>,
    ) -> Result<Recipe, String> {
        let file_recipe = config_recipes::FileRecipe {
            recipe: recipe.clone(),
            is_project_scoped,
            file_path,
        };
        let saved_path = config_recipes::save_recipe(&self.config_dir, &file_recipe, project_path)?;
        let action = if event.config_action == "created" {
            "create"
        } else {
            "update"
        };
        crate::config::commit_config_paths(
            std::slice::from_ref(&saved_path),
            &format!("cairn: {action} recipe {}", event.event_id),
        );

        config_resource::emit_config_change::<RecipeResource>(
            self,
            event.config_action,
            event.event_id,
        );
        config_resource::emit_db_change::<RecipeResource>(self, event.db_action);

        Ok(recipe)
    }

    /// List recipe configurations with optional scope filters.
    pub fn list_recipes(
        &self,
        workspace_id: Option<&str>,
        project_id: Option<&str>,
    ) -> Result<Vec<Recipe>, String> {
        config_resource::list_configs::<RecipeResource>(self, workspace_id, project_id)
    }

    /// Get a single recipe by ID.
    pub fn get_recipe(
        &self,
        recipe_id: &str,
        project_id: Option<&str>,
    ) -> Result<Option<Recipe>, String> {
        config_resource::get_config::<RecipeResource>(self, recipe_id, project_id)
    }

    /// Create a new recipe.
    pub fn create_recipe(&self, input: CreateRecipe) -> Result<Recipe, String> {
        config_resource::create_config::<RecipeResource>(self, input)
    }

    /// Update an existing recipe.
    pub fn update_recipe(
        &self,
        recipe_id: &str,
        input: UpdateRecipe,
        project_id: Option<&str>,
    ) -> Result<Recipe, String> {
        config_resource::update_config::<RecipeResource>(self, recipe_id, input, project_id)
    }

    /// Delete a recipe.
    pub fn delete_recipe(&self, recipe_id: &str, project_id: Option<&str>) -> Result<(), String> {
        config_resource::delete_config::<RecipeResource>(self, recipe_id, project_id)
    }

    /// List recipes for a project context with workspace→project shadowing.
    pub fn list_recipes_for_context(&self, project_id: &str) -> Result<Vec<Recipe>, String> {
        config_resource::list_for_context::<RecipeResource>(self, project_id)
    }

    /// Copy a recipe from any scope into a target scope under a chosen id.
    pub fn copy_recipe_config(
        &self,
        source_id: &str,
        source_project_id: Option<&str>,
        target_id: &str,
        target_project_id: Option<&str>,
    ) -> Result<Recipe, String> {
        config_resource::copy_config::<RecipeResource>(
            self,
            source_id,
            source_project_id,
            target_id,
            target_project_id,
        )
    }

    /// Archive a recipe (in file-only mode, same as delete).
    pub fn archive_recipe(&self, recipe_id: &str, project_id: Option<&str>) -> Result<(), String> {
        self.delete_recipe(recipe_id, project_id)
    }

    /// Duplicate a recipe with a new name.
    pub fn duplicate_recipe(
        &self,
        recipe_id: &str,
        new_name: &str,
        project_id: Option<&str>,
    ) -> Result<Recipe, String> {
        let project_path: Option<PathBuf> = if let Some(pid) = project_id {
            Some(self.project_path(pid)?)
        } else {
            None
        };

        let existing =
            config_recipes::get_recipe(&self.config_dir, recipe_id, project_path.as_deref())?
                .ok_or_else(|| format!("Recipe not found: {}", recipe_id))?;

        let new_id = slugify(new_name);
        let now = chrono::Utc::now().timestamp();

        let mut new_recipe = existing.recipe.clone();
        new_recipe.id = new_id.clone();
        new_recipe.name = new_name.to_string();
        new_recipe.is_default = false;
        new_recipe.version = 1;
        new_recipe.parent_recipe_id = None;
        new_recipe.child_recipe_id = None;
        new_recipe.created_at = now;
        new_recipe.updated_at = now;

        let file_path = existing
            .file_path
            .parent()
            .map(|p| p.join(format!("{}.yaml", new_id)))
            .ok_or_else(|| "Invalid source recipe path".to_string())?;

        self.save_recipe_change(
            new_recipe,
            existing.is_project_scoped,
            file_path,
            project_path.as_deref(),
            RecipeChangeEvent {
                event_id: &new_id,
                config_action: "created",
                db_action: "insert",
            },
        )
    }

    /// Get the default recipe for an issue (project first, then workspace).
    pub fn get_default_recipe_for_issue(&self, project_id: &str) -> Result<Option<Recipe>, String> {
        let project_path = self.project_path(project_id)?;

        // Project default first
        let project_recipes = config_recipes::list_recipes(&self.config_dir, Some(&project_path))?;
        for result in project_recipes {
            if let ConfigResult::Ok(file_recipe) = result {
                if file_recipe.recipe.is_default
                    && file_recipe.recipe.trigger == RecipeTrigger::Manual
                    && file_recipe.is_project_scoped
                {
                    return Ok(Some(file_recipe_to_model(
                        file_recipe,
                        None,
                        Some(project_id.to_string()),
                        chrono::Utc::now().timestamp(),
                    )));
                }
            }
        }

        // Workspace default fallback
        let workspace_recipes = config_recipes::list_recipes(&self.config_dir, None)?;
        for result in workspace_recipes {
            if let ConfigResult::Ok(file_recipe) = result {
                if file_recipe.recipe.is_default
                    && file_recipe.recipe.trigger == RecipeTrigger::Manual
                    && !file_recipe.is_project_scoped
                {
                    return Ok(Some(file_recipe_to_model(
                        file_recipe,
                        Some("default".to_string()),
                        None,
                        chrono::Utc::now().timestamp(),
                    )));
                }
            }
        }

        Ok(None)
    }

    /// Set a recipe as the default for its trigger type.
    pub fn set_default_recipe(
        &self,
        recipe_id: &str,
        project_id: Option<&str>,
    ) -> Result<Recipe, String> {
        let project_path: Option<PathBuf> = if let Some(pid) = project_id {
            Some(self.project_path(pid)?)
        } else {
            None
        };

        let existing =
            config_recipes::get_recipe(&self.config_dir, recipe_id, project_path.as_deref())?
                .ok_or_else(|| format!("Recipe not found: {}", recipe_id))?;

        let trigger = existing.recipe.trigger.clone();

        // Unset any other defaults with the same trigger
        let all_recipes = config_recipes::list_recipes(&self.config_dir, project_path.as_deref())?;
        for result in all_recipes {
            if let ConfigResult::Ok(file_recipe) = result {
                if file_recipe.recipe.is_default
                    && file_recipe.recipe.trigger == trigger
                    && file_recipe.recipe.id != recipe_id
                    && file_recipe.is_project_scoped == existing.is_project_scoped
                {
                    let mut updated = file_recipe.recipe.clone();
                    updated.is_default = false;
                    updated.updated_at = chrono::Utc::now().timestamp();

                    let updated_file = config_recipes::FileRecipe {
                        recipe: updated,
                        is_project_scoped: file_recipe.is_project_scoped,
                        file_path: file_recipe.file_path,
                    };
                    config_recipes::save_recipe(
                        &self.config_dir,
                        &updated_file,
                        project_path.as_deref(),
                    )?;
                }
            }
        }

        let mut recipe = existing.recipe.clone();
        recipe.is_default = true;
        recipe.updated_at = chrono::Utc::now().timestamp();

        self.save_recipe_change(
            recipe,
            existing.is_project_scoped,
            existing.file_path,
            project_path.as_deref(),
            RecipeChangeEvent {
                event_id: recipe_id,
                config_action: "modified",
                db_action: "update",
            },
        )
    }

    /// Unset a recipe as the default.
    pub fn unset_default_recipe(
        &self,
        recipe_id: &str,
        project_id: Option<&str>,
    ) -> Result<Recipe, String> {
        let project_path: Option<PathBuf> = if let Some(pid) = project_id {
            Some(self.project_path(pid)?)
        } else {
            None
        };

        let existing =
            config_recipes::get_recipe(&self.config_dir, recipe_id, project_path.as_deref())?
                .ok_or_else(|| format!("Recipe not found: {}", recipe_id))?;

        let mut recipe = existing.recipe.clone();
        recipe.is_default = false;
        recipe.updated_at = chrono::Utc::now().timestamp();

        self.save_recipe_change(
            recipe,
            existing.is_project_scoped,
            existing.file_path,
            project_path.as_deref(),
            RecipeChangeEvent {
                event_id: recipe_id,
                config_action: "modified",
                db_action: "update",
            },
        )
    }

    /// Get recipe version history (stub — file-based recipes have no version history).
    pub fn get_recipe_versions(&self, recipe_id: &str) -> Vec<RecipeVersionInfo> {
        vec![RecipeVersionInfo {
            id: recipe_id.to_string(),
            version: 1,
            created_at: chrono::Utc::now().timestamp(),
            is_current: true,
        }]
    }

    /// Export a recipe to YAML or JSON.
    pub fn export_recipe(&self, recipe_id: &str, format: &str) -> Result<String, String> {
        let file_recipe = config_recipes::get_recipe(&self.config_dir, recipe_id, None)?
            .ok_or_else(|| format!("Recipe '{}' not found", recipe_id))?;

        let file: RecipeFile = file_recipe.recipe.into();
        match format.to_lowercase().as_str() {
            "yaml" => file.to_yaml(),
            "json" => file.to_json(),
            _ => Err(format!("Invalid format '{}'. Use 'yaml' or 'json'", format)),
        }
    }

    /// Import a recipe from YAML or JSON content.
    pub fn import_recipe(
        &self,
        content: &str,
        format: &str,
        workspace_id: Option<String>,
        project_id: Option<String>,
    ) -> Result<Recipe, String> {
        let recipe_file: RecipeFile = match format.to_lowercase().as_str() {
            "yaml" | "yml" => RecipeFile::from_yaml(content)?,
            "json" => RecipeFile::from_json(content)?,
            _ => return Err(format!("Unsupported format: {}", format)),
        };

        let validation = recipe_file.validate();
        if !validation.valid {
            return Err(format!("Invalid recipe: {}", validation.errors.join(", ")));
        }

        let project_path: Option<PathBuf> = if let Some(ref pid) = project_id {
            Some(self.project_path(pid)?)
        } else {
            None
        };

        let mut recipe = recipe_file.into_recipe(workspace_id.clone(), project_id.clone());
        let id = slugify(&recipe.name);
        recipe.id = id.clone();

        let is_project_scoped = project_path.is_some();
        let file_path = if let Some(ref proj_path) = project_path {
            let recipes_dir = proj_path.join(".cairn").join("recipes");
            std::fs::create_dir_all(&recipes_dir)
                .map_err(|e| format!("Failed to create project recipes directory: {}", e))?;
            recipes_dir.join(format!("{}.yaml", id))
        } else {
            let recipes_dir = self.config_dir.join("recipes");
            std::fs::create_dir_all(&recipes_dir)
                .map_err(|e| format!("Failed to create recipes directory: {}", e))?;
            recipes_dir.join(format!("{}.yaml", id))
        };

        self.save_recipe_change(
            recipe,
            is_project_scoped,
            file_path,
            project_path.as_deref(),
            RecipeChangeEvent {
                event_id: &id,
                config_action: "created",
                db_action: "insert",
            },
        )
    }

    /// Validate recipe file content without saving.
    pub fn validate_recipe_file(
        &self,
        content: &str,
        format: &str,
    ) -> Result<RecipeFileValidation, String> {
        let file: RecipeFile = match format.to_lowercase().as_str() {
            "yaml" => RecipeFile::from_yaml(content)?,
            "json" => RecipeFile::from_json(content)?,
            "auto" => RecipeFile::parse(content)?,
            _ => {
                return Err(format!(
                    "Invalid format '{}'. Use 'yaml', 'json', or 'auto'",
                    format
                ))
            }
        };
        Ok(file.validate())
    }
}
