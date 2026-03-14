//! Orchestrator recipe configuration operations.
//!
//! File-based recipe management: workspace-scoped (`~/.cairn/recipes/`) vs
//! project-scoped (`[project]/.cairn/recipes/`).

use std::collections::HashMap;
use std::path::PathBuf;

use crate::config::{recipes as config_recipes, slugify, ConfigResult};
use crate::models::{
    CreateRecipe, Recipe, RecipeFile, RecipeFileValidation, RecipeTrigger, RecipeVersionInfo,
    UpdateRecipe,
};

use super::Orchestrator;

/// Convert a file recipe to the API model.
fn file_recipe_to_model(
    file_recipe: config_recipes::FileRecipe,
    workspace_id: Option<String>,
    project_id: Option<String>,
) -> Recipe {
    let mut recipe = file_recipe.recipe;
    recipe.workspace_id = workspace_id;
    recipe.project_id = project_id;
    recipe
}

impl Orchestrator {
    /// List recipe configurations with optional scope filters.
    pub fn list_recipes(
        &self,
        workspace_id: Option<&str>,
        project_id: Option<&str>,
    ) -> Result<Vec<Recipe>, String> {
        let mut all_recipes = Vec::new();

        if workspace_id.is_none() && project_id.is_none() {
            // Load workspace recipes
            let ws_results = config_recipes::list_recipes(&self.config_dir, None)?;
            for result in ws_results {
                if let ConfigResult::Ok(file_recipe) = result {
                    if !file_recipe.is_project_scoped {
                        all_recipes.push(file_recipe_to_model(
                            file_recipe,
                            Some("default".to_string()),
                            None,
                        ));
                    }
                }
            }

            // Load recipes from all projects
            let projects = self.all_project_paths()?;
            for (pid, project_path) in projects {
                let proj_results =
                    config_recipes::list_recipes(&self.config_dir, Some(&project_path))?;
                for result in proj_results {
                    if let ConfigResult::Ok(file_recipe) = result {
                        if file_recipe.is_project_scoped {
                            all_recipes.push(file_recipe_to_model(
                                file_recipe,
                                None,
                                Some(pid.clone()),
                            ));
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

            let results = config_recipes::list_recipes(&self.config_dir, project_path.as_deref())?;

            for result in results {
                if let ConfigResult::Ok(file_recipe) = result {
                    let include = match (workspace_id, project_id) {
                        (Some(_), None) => !file_recipe.is_project_scoped,
                        (None, Some(_)) => file_recipe.is_project_scoped,
                        _ => true,
                    };

                    if include {
                        let (ws_id, proj_id) = if file_recipe.is_project_scoped {
                            (None, project_id.map(|s| s.to_string()))
                        } else {
                            (workspace_id.map(|s| s.to_string()), None)
                        };
                        all_recipes.push(file_recipe_to_model(file_recipe, ws_id, proj_id));
                    }
                }
            }
        }

        all_recipes.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(all_recipes)
    }

    /// Get a single recipe by ID.
    pub fn get_recipe(
        &self,
        recipe_id: &str,
        project_id: Option<&str>,
    ) -> Result<Option<Recipe>, String> {
        let project_path: Option<PathBuf> = if let Some(pid) = project_id {
            Some(self.project_path(pid)?)
        } else {
            None
        };

        let file_recipe =
            config_recipes::get_recipe(&self.config_dir, recipe_id, project_path.as_deref())?;

        Ok(file_recipe.map(|fr| {
            let (ws_id, proj_id) = if fr.is_project_scoped {
                (None, project_id.map(|s| s.to_string()))
            } else {
                (Some("default".to_string()), None)
            };
            file_recipe_to_model(fr, ws_id, proj_id)
        }))
    }

    /// Create a new recipe.
    pub fn create_recipe(&self, input: CreateRecipe) -> Result<Recipe, String> {
        if input.workspace_id.is_none() == input.project_id.is_none() {
            return Err("Exactly one of workspace_id or project_id must be set".to_string());
        }

        let project_path: Option<PathBuf> = if let Some(ref pid) = input.project_id {
            Some(self.project_path(pid)?)
        } else {
            None
        };

        let is_project_scoped = project_path.is_some();
        let id = slugify(&input.name);

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

        let now = chrono::Utc::now().timestamp();
        let recipe = Recipe {
            id: id.clone(),
            name: input.name.clone(),
            description: input.description.clone(),
            trigger: input.trigger.unwrap_or_default(),
            context: input.context.unwrap_or_default(),
            workspace_id: input.workspace_id.clone(),
            project_id: input.project_id.clone(),
            is_default: false,
            version: 1,
            parent_recipe_id: None,
            child_recipe_id: None,
            nodes: input.nodes.unwrap_or_default(),
            edges: input.edges.unwrap_or_default(),
            created_at: now,
            updated_at: now,
        };

        let file_recipe = config_recipes::FileRecipe {
            recipe: recipe.clone(),
            is_project_scoped,
            file_path,
        };

        config_recipes::save_recipe(&self.config_dir, &file_recipe, project_path.as_deref())?;

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "recipe", "action": "created", "id": id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "recipes", "action": "insert"}),
        );

        Ok(recipe)
    }

    /// Update an existing recipe.
    pub fn update_recipe(
        &self,
        recipe_id: &str,
        input: UpdateRecipe,
        project_id: Option<&str>,
    ) -> Result<Recipe, String> {
        let original_project_path: Option<PathBuf> = if let Some(pid) = project_id {
            Some(self.project_path(pid)?)
        } else {
            None
        };

        let existing = config_recipes::get_recipe(
            &self.config_dir,
            recipe_id,
            original_project_path.as_deref(),
        )?
        .ok_or_else(|| format!("Recipe not found: {}", recipe_id))?;

        let mut recipe = existing.recipe.clone();

        if let Some(name) = input.name {
            recipe.name = name;
        }
        if let Some(description) = input.description {
            recipe.description = description;
        }
        if let Some(trigger) = input.trigger {
            recipe.trigger = trigger;
        }
        if let Some(context) = input.context {
            recipe.context = context;
        }
        if let Some(nodes) = input.nodes {
            recipe.nodes = nodes;
        }
        if let Some(edges) = input.edges {
            recipe.edges = edges;
        }

        let input_project_id = input.project_id.as_ref().and_then(|p| p.clone());
        let scope_is_changing = input_project_id != project_id.map(|s| s.to_string());

        if scope_is_changing {
            let new_is_project_scoped = input_project_id.is_some();

            recipe.workspace_id = if new_is_project_scoped {
                None
            } else {
                Some("default".to_string())
            };
            recipe.project_id = input_project_id.clone();

            let new_project_path: Option<PathBuf> = if let Some(ref pid) = input_project_id {
                Some(self.project_path(pid)?)
            } else {
                None
            };

            config_recipes::delete_recipe(
                &self.config_dir,
                recipe_id,
                original_project_path.as_deref(),
            )?;

            let file_recipe = config_recipes::FileRecipe {
                recipe: recipe.clone(),
                is_project_scoped: new_is_project_scoped,
                file_path: PathBuf::new(),
            };
            recipe.updated_at = chrono::Utc::now().timestamp();
            config_recipes::save_recipe(
                &self.config_dir,
                &file_recipe,
                new_project_path.as_deref(),
            )?;
        } else {
            recipe.updated_at = chrono::Utc::now().timestamp();

            let recipe_file: RecipeFile = recipe.clone().into();
            let yaml = recipe_file.to_yaml()?;
            std::fs::write(&existing.file_path, yaml)
                .map_err(|e| format!("Failed to write recipe file: {}", e))?;
        }

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "recipe", "action": "modified", "id": recipe_id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "recipes", "action": "update"}),
        );

        Ok(recipe)
    }

    /// Delete a recipe.
    pub fn delete_recipe(&self, recipe_id: &str, project_id: Option<&str>) -> Result<(), String> {
        let project_path: Option<PathBuf> = if let Some(pid) = project_id {
            Some(self.project_path(pid)?)
        } else {
            None
        };

        config_recipes::delete_recipe(&self.config_dir, recipe_id, project_path.as_deref())?;

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "recipe", "action": "removed", "id": recipe_id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "recipes", "action": "delete"}),
        );

        Ok(())
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

        let file_recipe = config_recipes::FileRecipe {
            recipe: new_recipe.clone(),
            is_project_scoped: existing.is_project_scoped,
            file_path,
        };
        config_recipes::save_recipe(&self.config_dir, &file_recipe, project_path.as_deref())?;

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "recipe", "action": "created", "id": new_id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "recipes", "action": "insert"}),
        );

        Ok(new_recipe)
    }

    /// Fork a workspace recipe to a project scope.
    pub fn fork_recipe(&self, recipe_id: &str, project_id: &str) -> Result<Recipe, String> {
        let project_path = self.project_path(project_id)?;

        let source = config_recipes::get_recipe(&self.config_dir, recipe_id, None)?
            .ok_or_else(|| format!("Recipe not found in workspace: {}", recipe_id))?;

        if source.is_project_scoped {
            return Err("Cannot fork a project-scoped recipe".to_string());
        }

        let target_path = project_path
            .join(".cairn")
            .join("recipes")
            .join(format!("{}.yaml", recipe_id));
        if target_path.exists() {
            return Err(format!(
                "Recipe '{}' already exists in this project",
                recipe_id
            ));
        }

        let now = chrono::Utc::now().timestamp();
        let mut forked_recipe = source.recipe.clone();
        forked_recipe.workspace_id = None;
        forked_recipe.project_id = Some(project_id.to_string());
        forked_recipe.is_default = false;
        forked_recipe.created_at = now;
        forked_recipe.updated_at = now;

        let file_recipe = config_recipes::FileRecipe {
            recipe: forked_recipe.clone(),
            is_project_scoped: true,
            file_path: PathBuf::new(),
        };

        config_recipes::save_recipe(&self.config_dir, &file_recipe, Some(&project_path))?;

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "recipe", "action": "created", "id": recipe_id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "recipes", "action": "insert"}),
        );

        Ok(forked_recipe)
    }

    /// List recipes for a project context with workspace→project shadowing.
    pub fn list_recipes_for_context(&self, project_id: &str) -> Result<Vec<Recipe>, String> {
        let project_path = self.project_path(project_id)?;
        let mut recipes_map: HashMap<String, Recipe> = HashMap::new();

        let ws_results = config_recipes::list_recipes(&self.config_dir, None)?;
        for result in ws_results {
            if let ConfigResult::Ok(file_recipe) = result {
                if !file_recipe.is_project_scoped {
                    let recipe =
                        file_recipe_to_model(file_recipe, Some("default".to_string()), None);
                    recipes_map.insert(recipe.id.clone(), recipe);
                }
            }
        }

        let proj_results = config_recipes::list_recipes(&self.config_dir, Some(&project_path))?;
        for result in proj_results {
            if let ConfigResult::Ok(file_recipe) = result {
                if file_recipe.is_project_scoped {
                    let recipe =
                        file_recipe_to_model(file_recipe, None, Some(project_id.to_string()));
                    recipes_map.insert(recipe.id.clone(), recipe);
                }
            }
        }

        let mut recipes: Vec<Recipe> = recipes_map.into_values().collect();
        recipes.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(recipes)
    }

    /// Get the default recipe for an issue (project first, then workspace).
    pub fn get_default_recipe_for_issue(&self, project_id: &str) -> Result<Option<Recipe>, String> {
        let project_path = self.project_path(project_id)?;

        // Project default first
        let project_recipes = config_recipes::list_recipes(&self.config_dir, Some(&project_path))?;
        for result in project_recipes {
            if let ConfigResult::Ok(file_recipe) = result {
                if file_recipe.recipe.is_default
                    && file_recipe.recipe.trigger == RecipeTrigger::Issue
                    && file_recipe.is_project_scoped
                {
                    return Ok(Some(file_recipe_to_model(
                        file_recipe,
                        None,
                        Some(project_id.to_string()),
                    )));
                }
            }
        }

        // Workspace default fallback
        let workspace_recipes = config_recipes::list_recipes(&self.config_dir, None)?;
        for result in workspace_recipes {
            if let ConfigResult::Ok(file_recipe) = result {
                if file_recipe.recipe.is_default
                    && file_recipe.recipe.trigger == RecipeTrigger::Issue
                    && !file_recipe.is_project_scoped
                {
                    return Ok(Some(file_recipe_to_model(
                        file_recipe,
                        Some("default".to_string()),
                        None,
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

        let file_recipe = config_recipes::FileRecipe {
            recipe: recipe.clone(),
            is_project_scoped: existing.is_project_scoped,
            file_path: existing.file_path,
        };
        config_recipes::save_recipe(&self.config_dir, &file_recipe, project_path.as_deref())?;

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "recipe", "action": "modified", "id": recipe_id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "recipes", "action": "update"}),
        );

        Ok(recipe)
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

        let file_recipe = config_recipes::FileRecipe {
            recipe: recipe.clone(),
            is_project_scoped: existing.is_project_scoped,
            file_path: existing.file_path,
        };
        config_recipes::save_recipe(&self.config_dir, &file_recipe, project_path.as_deref())?;

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "recipe", "action": "modified", "id": recipe_id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "recipes", "action": "update"}),
        );

        Ok(recipe)
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

        let file_recipe = config_recipes::FileRecipe {
            recipe: recipe.clone(),
            is_project_scoped,
            file_path,
        };
        config_recipes::save_recipe(&self.config_dir, &file_recipe, project_path.as_deref())?;

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "recipe", "action": "created", "id": id}),
        );
        let _ = self.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "recipes", "action": "insert"}),
        );

        Ok(recipe)
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
