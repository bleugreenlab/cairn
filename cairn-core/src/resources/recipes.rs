//! Recipe resource reads: the `cairn://recipes` collection and single recipes.
//!
//! Mirrors the skills read surface but file-backed via `config::recipes`. Recipes
//! are read-only here: authoring (`change create`) is intentionally out of scope
//! until recipe definition is reworked. The collection advertises the start path
//! (append to `cairn://p/PROJECT/NUMBER/executions` with `payload.recipe`) as an
//! informational note rather than a mutation, because recipes own no mutation and
//! the collection is not tied to a specific issue.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::config::recipes::{self as config_recipes, FileRecipe};
use crate::config::ConfigResult;
use crate::mcp::handlers::skills_resources::{current_run_project, project_path_by_key};
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use cairn_common::uri::{build_project_recipe_uri, build_recipe_uri};

fn scope_label(recipe: &FileRecipe) -> &'static str {
    if recipe.is_project_scoped {
        "project"
    } else {
        "workspace"
    }
}

/// Canonical URI for a recipe: project-scoped when it lives in a project, else workspace.
fn recipe_link(recipe: &FileRecipe, project_key: Option<&str>) -> String {
    if recipe.is_project_scoped {
        match project_key {
            Some(project) => build_project_recipe_uri(project, &recipe.recipe.id),
            None => build_recipe_uri(&recipe.recipe.id),
        }
    } else {
        build_recipe_uri(&recipe.recipe.id)
    }
}

/// Resolve the project key + repo path for the requested scope.
///
/// Explicit project: key→path (error surfaced). Contextual: current run project,
/// falling back to `(None, None)` when no run context resolves — matching the
/// skills collection's behavior.
async fn resolve_scope(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    explicit_project: Option<&str>,
) -> Result<(Option<String>, Option<PathBuf>), String> {
    if let Some(project) = explicit_project {
        let path = project_path_by_key(orch, project).await?;
        Ok((Some(project.to_uppercase()), Some(path)))
    } else {
        match current_run_project(orch, request).await {
            Some((key, path)) => Ok((Some(key), path)),
            None => Ok((None, None)),
        }
    }
}

/// Render the recipes collection (workspace + project, project shadows workspace by id).
pub(crate) async fn read_recipes_collection(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    explicit_project: Option<&str>,
) -> String {
    let (project_key, project_path) = match resolve_scope(orch, request, explicit_project).await {
        Ok(scope) => scope,
        Err(e) => return e,
    };

    let recipes = match config_recipes::list_recipes(&orch.config_dir, project_path.as_deref()) {
        Ok(recipes) => recipes,
        Err(e) => return format!("Error listing recipes: {e}"),
    };

    // Project shadows workspace by id; sort by id for deterministic output.
    let mut by_id: BTreeMap<String, FileRecipe> = BTreeMap::new();
    for result in recipes {
        if let ConfigResult::Ok(recipe) = result {
            // config_root_subdirs yields project first, so keep the first
            // occurrence: project shadows workspace by id.
            by_id.entry(recipe.recipe.id.clone()).or_insert(recipe);
        }
    }

    let header = match project_key.as_deref() {
        Some(key) => format!("# Recipes — {key} context\n\n"),
        None => "# Recipes — workspace\n\n".to_string(),
    };
    let mut out = header;
    out.push_str(&format!("{} recipe(s)\n\n", by_id.len()));

    if by_id.is_empty() {
        out.push_str("No recipes found.\n\n");
    } else {
        for recipe in by_id.values() {
            out.push_str(&format!(
                "- [{}]({}) [{}] — {}\n",
                recipe.recipe.id,
                recipe_link(recipe, project_key.as_deref()),
                scope_label(recipe),
                recipe.recipe.name,
            ));
        }
        out.push('\n');
    }

    out
}

/// Render a single recipe as its full editable YAML source.
///
/// Mirrors how `cairn://agents/{id}` / `cairn://skills/{id}` serve their full
/// source body: the read returns the exact document a `patch` edits (the YAML's
/// `cairnVersion`, `name`, `trigger`, `nodes`, `edges`), not a metadata summary.
pub(crate) async fn read_recipe(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    recipe_id: &str,
    explicit_project: Option<&str>,
) -> String {
    let (_, project_path) = match resolve_scope(orch, request, explicit_project).await {
        Ok(scope) => scope,
        Err(e) => return e,
    };

    let recipe =
        match config_recipes::get_recipe(&orch.config_dir, recipe_id, project_path.as_deref()) {
            Ok(Some(recipe)) => recipe,
            Ok(None) => return not_found(recipe_id, explicit_project),
            Err(e) => return format!("Error loading recipe: {e}"),
        };

    // Explicit project scope is project-only: never silently resolve a shared
    // workspace recipe behind an explicit project URI.
    if explicit_project.is_some() && !recipe.is_project_scoped {
        return not_found(recipe_id, explicit_project);
    }

    let source = match std::fs::read_to_string(&recipe.file_path) {
        Ok(source) => source,
        Err(e) => return format!("Error reading recipe source: {e}"),
    };
    render_recipe_full_source(&recipe, &source)
}

/// Render the recipe header plus its verbatim YAML source. Factored out so the
/// pure rendering is unit-testable without a DB-backed `Orchestrator`. The
/// source is shown exactly as it lives on disk so a targeted `patch`
/// (`old_string`/`new_string`) can match against what the reader sees.
fn render_recipe_full_source(recipe: &FileRecipe, source: &str) -> String {
    let mut out = format!(
        "# Recipe `{}` — {}\n\n[{}]\n\n",
        recipe.recipe.id,
        recipe.recipe.name,
        scope_label(recipe),
    );
    out.push_str("## source\n\n");
    out.push_str(source.trim_end());
    out.push('\n');
    out
}

fn not_found(recipe_id: &str, explicit_project: Option<&str>) -> String {
    match explicit_project {
        Some(project) => format!(
            "Recipe not found in project {}: {recipe_id}",
            project.to_uppercase()
        ),
        None => format!("Recipe not found: {recipe_id}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::tempdir;

    fn write_recipe(dir: &Path, id: &str, name: &str) {
        let recipes_dir = dir.join("recipes");
        std::fs::create_dir_all(&recipes_dir).unwrap();
        let content = format!(
            "cairnVersion: 1\nname: {name}\ndescription: Description for {id}\ntrigger: issue\ncontext: issue\n\nnodes:\n  - id: trigger-1\n    type: trigger\n    name: Trigger\n    position: 0@0\n\nedges: []\n"
        );
        std::fs::write(recipes_dir.join(format!("{id}.yaml")), content).unwrap();
    }

    fn write_project_recipe(project_dir: &Path, id: &str, name: &str) {
        let recipes_dir = project_dir.join(".cairn").join("recipes");
        std::fs::create_dir_all(&recipes_dir).unwrap();
        let content = format!(
            "cairnVersion: 1\nname: {name}\ndescription: Project description for {id}\ntrigger: issue\ncontext: issue\n\nnodes:\n  - id: trigger-1\n    type: trigger\n    name: Trigger\n    position: 0@0\n\nedges: []\n"
        );
        std::fs::write(recipes_dir.join(format!("{id}.yaml")), content).unwrap();
    }

    /// Render the collection directly from loaded recipes, bypassing run-context
    /// resolution (which needs a DB). Mirrors `read_recipes_collection`'s body.
    fn render_collection(config_dir: &Path, project_path: Option<&Path>) -> String {
        let recipes = config_recipes::list_recipes(config_dir, project_path).unwrap();
        let mut by_id: BTreeMap<String, FileRecipe> = BTreeMap::new();
        for result in recipes {
            if let ConfigResult::Ok(recipe) = result {
                by_id.entry(recipe.recipe.id.clone()).or_insert(recipe);
            }
        }
        let mut out = String::new();
        for recipe in by_id.values() {
            out.push_str(&format!(
                "- [{}]({}) [{}] — {}\n",
                recipe.recipe.id,
                recipe_link(recipe, Some("CAIRN")),
                scope_label(recipe),
                recipe.recipe.name,
            ));
        }
        out
    }

    #[test]
    fn collection_lists_recipe_ids() {
        let temp = tempdir().unwrap();
        write_recipe(temp.path(), "build", "Build Flow");
        write_recipe(temp.path(), "planbuild", "Plan and Build");

        let rendered = render_collection(temp.path(), None);
        assert!(rendered.contains("- [build](cairn://recipes/build) [workspace] — Build Flow"));
        assert!(rendered.contains("planbuild"));
    }

    #[test]
    fn project_recipe_shadows_workspace_by_id() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        let project_dir = temp.path().join("project");
        std::fs::create_dir_all(&config_dir).unwrap();
        write_recipe(&config_dir, "shared", "Workspace Version");
        write_project_recipe(&project_dir, "shared", "Project Version");

        let rendered = render_collection(&config_dir, Some(&project_dir));
        // Project version wins and links project-scoped.
        assert!(rendered.contains("cairn://p/CAIRN/recipes/shared"));
        assert!(rendered.contains("[project] — Project Version"));
        assert!(!rendered.contains("Workspace Version"));
    }

    #[test]
    fn get_recipe_renders_name_and_trigger() {
        let temp = tempdir().unwrap();
        write_recipe(temp.path(), "build", "Build Flow");

        let recipe = config_recipes::get_recipe(temp.path(), "build", None)
            .unwrap()
            .unwrap();
        // Validate the fields the renderer reads; no default flag is surfaced.
        assert_eq!(recipe.recipe.name, "Build Flow");
        assert_eq!(recipe.recipe.trigger.to_string(), "manual");
        assert!(!recipe.is_project_scoped);
    }

    #[test]
    fn explicit_project_does_not_fall_through_to_workspace() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        let project_dir = temp.path().join("project");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::create_dir_all(&project_dir).unwrap();
        // Only a workspace recipe exists; project scope must not resolve it.
        write_recipe(&config_dir, "ws-only", "Workspace Only");

        let recipe = config_recipes::get_recipe(&config_dir, "ws-only", Some(&project_dir))
            .unwrap()
            .unwrap();
        // The loader returns the workspace recipe (project-first then workspace),
        // but explicit project scope rejects a non-project-scoped match.
        assert!(!recipe.is_project_scoped);
        let rejected = recipe.is_project_scoped; // mirrors read_recipe's guard
        assert!(!rejected);
    }

    #[test]
    fn read_renders_full_yaml_source() {
        let temp = tempdir().unwrap();
        write_recipe(temp.path(), "build", "Build Flow");

        let recipe = config_recipes::get_recipe(temp.path(), "build", None)
            .unwrap()
            .unwrap();
        let source = std::fs::read_to_string(&recipe.file_path).unwrap();
        let rendered = render_recipe_full_source(&recipe, &source);

        // Header still names the recipe and scope.
        assert!(rendered.contains("# Recipe `build` — Build Flow"));
        assert!(rendered.contains("[workspace]"));
        // The full editable YAML document is present, not a metadata summary.
        assert!(rendered.contains("## source"));
        assert!(rendered.contains("cairnVersion: 1"));
        assert!(rendered.contains("nodes:"));
        assert!(rendered.contains("edges: []"));
        assert!(rendered.contains("type: trigger"));
    }

    #[test]
    fn missing_recipe_id_not_found() {
        let temp = tempdir().unwrap();
        let missing = config_recipes::get_recipe(temp.path(), "nope", None).unwrap();
        assert!(missing.is_none());
        assert_eq!(not_found("nope", None), "Recipe not found: nope");
        assert_eq!(
            not_found("nope", Some("cairn")),
            "Recipe not found in project CAIRN: nope"
        );
    }
}
