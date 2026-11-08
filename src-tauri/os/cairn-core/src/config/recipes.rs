//! Recipe configuration file reading.
//!
//! Recipes are YAML files, one per layer directory (`recipes/<id>.yaml` in a
//! project's `.cairn/`, the workspace, or an installed pack), resolved
//! nearest-first through [`cairn_config::library`].

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::path::{Path, PathBuf};

use cairn_config::library::{
    self, LibraryItem, LibraryKind, LibraryRef, LibraryRoots, Listed, Origin, ResolveError,
    Resolved,
};

use crate::models::{Recipe, RecipeFile, RecipeNodeType};

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
    /// The layer this recipe was read from, or the layer a new one is for.
    pub origin: Origin,
    /// Path to the source file
    pub file_path: PathBuf,
}

impl FileRecipe {
    pub fn is_project_scoped(&self) -> bool {
        self.origin.is_project()
    }
}

/// A recipe's authority: what fires it, which branches it may land on, the
/// graph and the conditions that decide which gates run, and per node what that
/// node runs, with which agent, models and placement constraints, and whether a
/// person must confirm its output.
///
/// Every field of every recipe type is named below, taken or set aside with
/// its reason, so a field added to any of them does not compile until it is
/// classified here: nothing new defaults to prose.
impl LibraryItem for FileRecipe {
    fn library_id(&self) -> &str {
        &self.recipe.id
    }

    fn authority(&self) -> Map<String, Value> {
        use crate::models::decision::DecisionNode;
        use crate::models::{
            ActionNodeConfig, AgentNodeConfig, ArtifactNodeConfig, CheckpointNodeConfig,
            ConditionNodeConfig, ContextNodeConfig, RecipeEdge, RecipeNode, SchemaConfig,
        };

        let FileRecipe {
            recipe,
            // Which bundled sets list the recipe: catalog metadata.
            bundles: _,
            origin: _,
            file_path: _,
        } = self;
        let Recipe {
            // The name the layer stores it under, which resolution already keys.
            id: _,
            // Prose.
            name: _,
            description: _,
            trigger,
            // Where a stored copy was filed; a file definition's scope is its
            // layer.
            workspace_id: _,
            project_id: _,
            // Whether the issue-create picker lists it; it stays runnable.
            is_system: _,
            // Revision bookkeeping.
            version: _,
            parent_recipe_id: _,
            child_recipe_id: _,
            created_at: _,
            updated_at: _,
            nodes,
            edges,
            branch_targets,
        } = recipe;

        // Loading a recipe file mints fresh node ids, so nodes are named by
        // their place in the file, which is what a confirmation can bind.
        let label: std::collections::HashMap<&str, String> = nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (node.id.as_str(), format!("n{index}")))
            .collect();
        let named = |id: &str| label.get(id).cloned().unwrap_or_else(|| id.to_string());

        let mut fields = Map::new();
        fields.insert("trigger".into(), json!(trigger));
        fields.insert("branchTargets".into(), json!(branch_targets));
        let mut graph: Vec<String> = edges
            .iter()
            .map(|edge| {
                let RecipeEdge {
                    // An edge's own id names nothing it does.
                    id: _,
                    edge_type,
                    source_node_id,
                    source_handle,
                    target_node_id,
                    target_handle,
                } = edge;
                format!(
                    "{}:{source_handle} -> {}:{target_handle} ({})",
                    named(source_node_id),
                    named(target_node_id),
                    serde_json::to_value(edge_type)
                        .ok()
                        .and_then(|v| v.as_str().map(str::to_string))
                        .unwrap_or_default()
                )
            })
            .collect();
        graph.sort();
        if !graph.is_empty() {
            fields.insert("edges".into(), json!(graph));
        }

        // Only the gate a schema carries is authority: its name, field shape,
        // tool name and description shape what is written, not what is allowed.
        let gate = |schema: &SchemaConfig| {
            let SchemaConfig {
                name: _,
                schema: _,
                confirm_policy,
                tool_name: _,
                description: _,
            } = schema;
            json!(confirm_policy)
        };

        for node in nodes {
            let RecipeNode {
                id,
                node_type,
                // Prose and layout.
                name: _,
                position: _,
                parent_id: _,
                trigger_config,
                agent_config,
                action_config,
                checkpoint_config,
                artifact_config,
                condition_config,
                decision_config,
                context_config,
            } = node;
            let mut node_fields = Map::new();
            node_fields.insert("type".into(), json!(node_type));
            if let Some(trigger) = trigger_config {
                node_fields.insert("trigger".into(), json!(trigger));
            }
            if let Some(agent) = agent_config {
                let AgentNodeConfig {
                    agent_config_id,
                    models,
                    constraints,
                    output_schema,
                    git_config,
                } = agent;
                node_fields.insert("agentConfigId".into(), json!(agent_config_id));
                if let Some(models) = models {
                    node_fields.insert("models".into(), json!(models));
                }
                if !constraints.is_empty() {
                    node_fields.insert("constraints".into(), json!(constraints));
                }
                if let Some(git) = git_config {
                    node_fields.insert("gitConfig".into(), json!(git));
                }
                if let Some(schema) = output_schema {
                    node_fields.insert("confirmPolicy".into(), gate(schema));
                }
            }
            if let Some(action) = action_config {
                let ActionNodeConfig {
                    action_config_id,
                    action,
                    action_params,
                    // What the action is handed is checked by the action; its
                    // shape grants nothing.
                    input_schema: _,
                    output_schema,
                } = action;
                node_fields.insert("actionConfigId".into(), json!(action_config_id));
                node_fields.insert("action".into(), json!(action));
                node_fields.insert("actionParams".into(), action_params.clone());
                if let Some(schema) = output_schema {
                    node_fields.insert("confirmPolicy".into(), gate(schema));
                }
            }
            if let Some(checkpoint) = checkpoint_config {
                let CheckpointNodeConfig {
                    command,
                    // Shown to the user.
                    prompt: _,
                } = checkpoint;
                if let Some(command) = command {
                    node_fields.insert("command".into(), json!(command));
                }
            }
            if let Some(artifact) = artifact_config {
                let ArtifactNodeConfig {
                    // The document's name, shape and literal content are what
                    // is written.
                    name: _,
                    schema: _,
                    confirm_policy,
                    content: _,
                } = artifact;
                if *node_type == RecipeNodeType::Artifact {
                    node_fields.insert("confirmPolicy".into(), json!(confirm_policy));
                }
            }
            if let Some(condition) = condition_config {
                // A condition picks which branch of the graph runs, so it is
                // authority like the edges it routes between.
                let ConditionNodeConfig {
                    expression,
                    ports,
                    on_error,
                    default_port,
                } = condition;
                node_fields.insert(
                    "condition".into(),
                    json!({
                        "expression": expression,
                        "ports": ports,
                        "onError": on_error,
                        "defaultPort": default_port,
                    }),
                );
            }
            if let Some(decision) = decision_config {
                let DecisionNode {
                    // The questions are prose put to a model; the ports they
                    // route to are the graph's.
                    questions: _,
                    models,
                    thresholds,
                } = decision;
                if let Some(models) = models {
                    node_fields.insert("models".into(), json!(models));
                }
                if !thresholds.is_empty() {
                    node_fields.insert("thresholds".into(), json!(thresholds));
                }
            }
            if let Some(context) = context_config {
                // Static text handed to downstream nodes.
                let ContextNodeConfig { content: _ } = context;
            }
            fields.insert(format!("node:{}", named(id)), Value::Object(node_fields));
        }
        fields
    }
}

/// The recipe file `id` names in one layer directory.
///
/// The file whose stem is exactly `id` wins, else the one whose stem matches
/// it ignoring ASCII case, `.yaml` before `.yml`. Matching is done here
/// against the directory's entries rather than by joining `id` into a path, so
/// the filesystem's own case folding never decides which file an id names, and
/// the resolved file's stem is the recipe's canonical id on every platform.
fn recipe_file_in(dir: &Path, id: &str) -> Result<Option<PathBuf>, String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Ok(None);
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
        return Ok(None);
    };
    if *inexact {
        if let Some((_, _, other, _)) = candidates.iter().find(|c| &c.2 != stem) {
            return Err(format!(
                "recipe id `{id}` matches both `{stem}` and `{other}` in {}; name one exactly",
                dir.display()
            ));
        }
    }
    Ok(Some(path.clone()))
}

fn load_recipe_in(dir: &Path, id: &str, origin: &Origin) -> Option<ConfigResult<FileRecipe>> {
    match recipe_file_in(dir, id) {
        Ok(Some(path)) => Some(load_recipe_file(&path, origin.clone())),
        Ok(None) => None,
        Err(error) => Some(ConfigResult::Err {
            path: dir.to_path_buf(),
            error,
        }),
    }
}

fn enumerate_recipes(
    dir: &Path,
    origin: &Origin,
) -> Result<Vec<(String, ConfigResult<FileRecipe>)>, String> {
    let mut results = Vec::new();
    for entry in
        std::fs::read_dir(dir).map_err(|e| format!("Failed to read recipes directory: {e}"))?
    {
        let path = entry
            .map_err(|e| format!("Failed to read directory entry: {e}"))?
            .path();
        if path.is_dir() {
            continue;
        }
        let ext = path.extension().and_then(|e| e.to_str());
        if ext != Some("yaml") && ext != Some("yml") {
            continue;
        }
        if let Some(id) = id_from_path(&path) {
            results.push((id, load_recipe_file(&path, origin.clone())));
        }
    }
    Ok(results)
}

/// Every recipe definition across the layers, with its origin and what it
/// shadows.
pub fn list_recipe_rows(roots: &LibraryRoots) -> Result<Vec<Listed<FileRecipe>>, String> {
    roots.list(LibraryKind::Recipe, enumerate_recipes)
}

/// The recipes in force, one per name.
pub fn list_recipes(roots: &LibraryRoots) -> Result<Vec<ConfigResult<FileRecipe>>, String> {
    Ok(library::active(list_recipe_rows(roots)?))
}

/// What a recipe id is, for refusals that name the rule.
const RECIPE_ID_SHAPE: &str = "a recipe id is a plain name of letters, digits, `-` and `_` (the recipe file's name without its extension), never a path or a file name";

pub use crate::models::is_recipe_id;

/// The refusal for a string that is not a recipe id, naming the recipe it
/// most plausibly meant when one resolves (`./planbuild` and
/// `recipes/planbuild.yaml` both suggest `planbuild`).
fn recipe_id_refusal(roots: &LibraryRoots, id: &str) -> String {
    let last = id.rsplit(['/', '\\']).next().unwrap_or(id).trim();
    let stem = last
        .strip_suffix(".yaml")
        .or_else(|| last.strip_suffix(".yml"))
        .unwrap_or(last)
        .trim_start_matches('.');
    let canonical = is_recipe_id(stem)
        .then(|| {
            roots
                .resolve(
                    LibraryKind::Recipe,
                    &LibraryRef::Bare(stem.to_string()),
                    load_recipe_in,
                )
                .ok()
        })
        .flatten()
        .map(|resolved| resolved.item.recipe.id);
    match canonical {
        Some(canonical) => {
            format!("`{id}` is not a recipe id: {RECIPE_ID_SHAPE}. Use `{canonical}`.")
        }
        None => format!("`{id}` is not a recipe id: {RECIPE_ID_SHAPE}, like `build`."),
    }
}

/// A recipe reference, refused unless its name is a recipe id.
fn recipe_reference(roots: &LibraryRoots, reference: &str) -> Result<LibraryRef, ResolveError> {
    let invalid = |error: String| ResolveError::Invalid {
        path: PathBuf::new(),
        error,
        origin: None,
    };
    let parsed = LibraryRef::parse(LibraryKind::Recipe, reference).map_err(invalid)?;
    if !is_recipe_id(parsed.name()) {
        return Err(invalid(recipe_id_refusal(roots, reference)));
    }
    Ok(parsed)
}

/// Resolve a recipe reference (a recipe id or
/// `cairn://packs/<pack>/recipes/<id>`) nearest-first. The loaded recipe's id
/// is its file's stem, which may differ from the reference in ASCII case only.
pub fn resolve_recipe(
    roots: &LibraryRoots,
    reference: &str,
) -> Result<Resolved<FileRecipe>, ResolveError> {
    let reference = recipe_reference(roots, reference)?;
    roots.resolve(LibraryKind::Recipe, &reference, load_recipe_in)
}

/// Resolve recipe `id` in one layer only (see [`LibraryRoots::resolve_in`]).
pub fn resolve_recipe_in(
    roots: &LibraryRoots,
    layer: &Origin,
    id: &str,
) -> Result<Resolved<FileRecipe>, ResolveError> {
    if !is_recipe_id(id) {
        return Err(ResolveError::Invalid {
            path: PathBuf::new(),
            error: recipe_id_refusal(roots, id),
            origin: None,
        });
    }
    roots.resolve_in(LibraryKind::Recipe, layer, id, load_recipe_in)
}

/// Recipe `id` in one layer as it stands on disk, in force or not: what a
/// write addressed to that layer edits (see [`LibraryRoots::find_in`]).
pub fn find_recipe_in(
    roots: &LibraryRoots,
    layer: &Origin,
    id: &str,
) -> Result<Resolved<FileRecipe>, ResolveError> {
    if !is_recipe_id(id) {
        return Err(ResolveError::Invalid {
            path: PathBuf::new(),
            error: recipe_id_refusal(roots, id),
            origin: None,
        });
    }
    roots.find_in(LibraryKind::Recipe, layer, id, load_recipe_in)
}

/// The recipe a reference resolves to, `None` when nothing defines it. A
/// string that is not a recipe id ([`is_recipe_id`]) is refused rather than
/// looked up, as is a name that does not resolve for any other reason.
pub fn get_recipe(roots: &LibraryRoots, id: &str) -> Result<Option<FileRecipe>, String> {
    match resolve_recipe(roots, id) {
        Ok(resolved) => Ok(Some(resolved.item)),
        Err(error) if error.is_not_found() => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

/// Save a recipe into a definite layer.
///
/// `layer` names where to write; `None` writes back where it was read from
/// (`file_recipe.origin`), to the same file. A pack's own recipe is never
/// written: saving one into the workspace or a project defines it there, in
/// front of the pack's.
pub fn save_recipe(
    roots: &LibraryRoots,
    file_recipe: &FileRecipe,
    layer: Option<&Origin>,
) -> Result<PathBuf, String> {
    let target = layer.unwrap_or(&file_recipe.origin);
    if let Origin::Pack { id: pack } = target {
        return Err(library::pack_edit_refusal(
            LibraryKind::Recipe,
            &file_recipe.recipe.id,
            pack,
        ));
    }
    let path = if layer.is_none() && !file_recipe.file_path.as_os_str().is_empty() {
        if let Some(parent) = file_recipe.file_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create directory: {}", e))?;
        }
        file_recipe.file_path.clone()
    } else {
        if !is_recipe_id(&file_recipe.recipe.id) {
            return Err(format!(
                "cannot save recipe `{}`: {RECIPE_ID_SHAPE}",
                file_recipe.recipe.id
            ));
        }
        let dir = roots
            .layer_dir(LibraryKind::Recipe, target)
            .ok_or("Project path required for project-scoped recipe")?;
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("Failed to create recipes directory: {}", e))?;
        dir.join(format!("{}.yaml", file_recipe.recipe.id))
    };

    // Convert to RecipeFile for serialization
    let mut recipe_file: RecipeFile = file_recipe.recipe.clone().into();
    recipe_file.bundles = file_recipe.bundles.clone();
    let yaml = recipe_file.to_yaml()?;

    std::fs::write(&path, yaml).map_err(|e| format!("Failed to write recipe file: {}", e))?;

    Ok(path)
}

/// Delete the recipe `id` resolves to, or the one in `layer` when named. A
/// workspace or project definition is deleted outright; a pack's own recipe is
/// recorded as removed from its pack. Returns the path that changed (`None`
/// when nothing defined it).
pub fn delete_recipe(
    roots: &LibraryRoots,
    id: &str,
    layer: Option<&Origin>,
) -> Result<Option<PathBuf>, String> {
    let resolved = match layer {
        Some(layer) => match layer {
            Origin::Project { .. } | Origin::Workspace => {
                if !is_recipe_id(id) {
                    return Err(recipe_id_refusal(roots, id));
                }
                let dir = roots
                    .layer_dir(LibraryKind::Recipe, layer)
                    .ok_or_else(|| format!("No {layer} layer to delete recipe `{id}` from"))?;
                match recipe_file_in(&dir, id)? {
                    Some(path) => (layer.clone(), path),
                    None => return Ok(None),
                }
            }
            Origin::Pack { .. } => match resolve_recipe_in(roots, layer, id) {
                Ok(resolved) => (resolved.origin, resolved.item.file_path),
                Err(ResolveError::Invalid {
                    path,
                    origin: Some(origin),
                    ..
                }) => (origin, path),
                Err(error) if error.is_not_found() => return Ok(None),
                Err(error) => return Err(error.to_string()),
            },
        },
        None => match resolve_recipe(roots, id) {
            Ok(resolved) => (resolved.origin, resolved.item.file_path),
            Err(ResolveError::Invalid {
                path,
                origin: Some(origin),
                ..
            }) => (origin, path),
            Err(error) if error.is_not_found() => return Ok(None),
            Err(error) => return Err(error.to_string()),
        },
    };
    let (origin, path) = resolved;
    let canonical = id_from_path(&path).unwrap_or_else(|| id.to_string());
    library::remove_definition(roots, LibraryKind::Recipe, &canonical, &origin, &path).map(Some)
}

/// Load a single recipe file
fn load_recipe_file(path: &Path, origin: Origin) -> ConfigResult<FileRecipe> {
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
                origin,
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
    #[allow(unused_imports)]
    use cairn_config::library::{LibraryRoots, Origin};
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

        let result = load_recipe_file(&path, Origin::Workspace);
        match result {
            ConfigResult::Ok(file_recipe) => {
                assert_eq!(file_recipe.recipe.id, "test-recipe");
                assert_eq!(file_recipe.recipe.name, "Test Recipe");
                assert_eq!(
                    file_recipe.recipe.description,
                    Some("A test recipe".to_string())
                );
                assert!(!file_recipe.is_project_scoped());
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
            origin: Origin::Workspace,
            file_path: PathBuf::new(), // empty → should derive from id
        };

        let saved_path =
            save_recipe(&LibraryRoots::workspace(config_dir), &file_recipe, None).unwrap();

        // The filename must be "{id}.yaml", not "my-fancy-recipe-name.yaml"
        assert_eq!(
            saved_path.file_name().unwrap().to_str().unwrap(),
            "custom-id.yaml"
        );

        // Round-trip: get_recipe should find it by id
        let loaded = get_recipe(&LibraryRoots::workspace(config_dir), "custom-id")
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
            origin: Origin::Workspace,
            file_path: explicit_path.clone(),
        };

        let saved_path =
            save_recipe(&LibraryRoots::workspace(config_dir), &file_recipe, None).unwrap();
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
            origin: Origin::Project { key: "demo".into() },
            file_path: PathBuf::new(),
        };

        let saved_path = save_recipe(
            &crate::config::trusted_roots(&config_dir, &project_dir),
            &file_recipe,
            None,
        )
        .unwrap();

        assert_eq!(
            saved_path,
            project_dir
                .join(".cairn")
                .join("recipes")
                .join("proj-recipe.yaml")
        );

        // Verify it can be loaded back from the project scope
        let loaded = get_recipe(
            &crate::config::trusted_roots(&config_dir, &project_dir),
            "proj-recipe",
        )
        .unwrap()
        .expect("should find project-scoped recipe");
        assert!(loaded.is_project_scoped());
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
            origin: Origin::Workspace,
            file_path: PathBuf::new(),
        };
        save_recipe(&LibraryRoots::workspace(&config_dir), &ws_file, None).unwrap();

        // Create project recipe with same id
        let mut proj_recipe = minimal_recipe("shared", "Project Version");
        proj_recipe.workspace_id = None;
        proj_recipe.project_id = Some("p1".to_string());
        let proj_file = FileRecipe {
            recipe: proj_recipe,
            bundles: Vec::new(),
            origin: Origin::Project { key: "demo".into() },
            file_path: PathBuf::new(),
        };
        save_recipe(
            &crate::config::trusted_roots(&config_dir, &project_dir),
            &proj_file,
            None,
        )
        .unwrap();

        // With project context, project version wins
        let loaded = get_recipe(
            &crate::config::trusted_roots(&config_dir, &project_dir),
            "shared",
        )
        .unwrap()
        .expect("should find recipe");
        assert_eq!(loaded.recipe.name, "Project Version");
        assert!(loaded.is_project_scoped());

        // Without project context, workspace version is returned
        let loaded_ws = get_recipe(&LibraryRoots::workspace(&config_dir), "shared")
            .unwrap()
            .expect("should find workspace recipe");
        assert_eq!(loaded_ws.recipe.name, "Workspace Version");
        assert!(!loaded_ws.is_project_scoped());
    }

    /// A path, a file name, or a dotted name is not a recipe id: every caller
    /// is refused, with the canonical id named when one resolves, and no file
    /// outside a recipes directory is ever loaded, saved, or deleted.
    #[test]
    fn noncanonical_recipe_ids_are_refused_for_every_operation() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        save_recipe(
            &LibraryRoots::workspace(&config_dir),
            &FileRecipe {
                recipe: minimal_recipe("planbuild", "Plan Build"),
                bundles: Vec::new(),
                origin: Origin::Workspace,
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
            let refusal = get_recipe(&LibraryRoots::workspace(&config_dir), spelling).unwrap_err();
            assert!(
                refusal.contains("is not a recipe id") && refusal.ends_with("Use `planbuild`."),
                "{spelling}: {refusal}"
            );
            assert!(delete_recipe(&LibraryRoots::workspace(&config_dir), spelling, None).is_err());
        }
        let unknown = get_recipe(&LibraryRoots::workspace(&config_dir), "../nowhere").unwrap_err();
        assert!(unknown.ends_with("like `build`."), "{unknown}");
        assert!(get_recipe(&LibraryRoots::workspace(&config_dir), "").is_err());

        let escaping = FileRecipe {
            recipe: minimal_recipe("../escaped", "Escaped"),
            bundles: Vec::new(),
            origin: Origin::Workspace,
            file_path: PathBuf::new(),
        };
        assert!(save_recipe(&LibraryRoots::workspace(&config_dir), &escaping, None).is_err());
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
                &LibraryRoots::workspace(&config_dir),
                &FileRecipe {
                    recipe: minimal_recipe(id, id),
                    bundles: Vec::new(),
                    origin: Origin::Workspace,
                    file_path: PathBuf::new(),
                },
                None,
            )
            .unwrap()
        };
        save("PlanCoord");
        let loaded = get_recipe(&LibraryRoots::workspace(&config_dir), "plancoord")
            .unwrap()
            .expect("a case variant resolves");
        assert_eq!(loaded.recipe.id, "PlanCoord");
        assert_eq!(
            get_recipe(&LibraryRoots::workspace(&config_dir), "PlanCoord")
                .unwrap()
                .unwrap()
                .recipe
                .id,
            "PlanCoord"
        );
        delete_recipe(&LibraryRoots::workspace(&config_dir), "PLANCOORD", None).unwrap();
        assert!(
            get_recipe(&LibraryRoots::workspace(&config_dir), "PlanCoord")
                .unwrap()
                .is_none()
        );
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
        match load_recipe_file(&path, Origin::Workspace) {
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
        match load_recipe_file(&path, Origin::Workspace) {
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
        match load_recipe_file(&path, Origin::Workspace) {
            ConfigResult::Ok(fr) => assert!(!fr.recipe.is_system),
            ConfigResult::Err { error, .. } => panic!("Failed to load: {}", error),
        }
    }
}
