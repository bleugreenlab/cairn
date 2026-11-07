//! Recipe resource mutations (create / patch / delete via `write`).
//!
//! File-backed via `config::recipes`. Payloads carry the recipe YAML body under
//! `content`, validated through `RecipeFile::from_yaml` + `validate()` — the same
//! path the file loader and GUI importer use — so malformed recipes are rejected
//! rather than written. The id is taken from `payload.id` or slugified from the
//! recipe name, matching the filename = id convention.

use cairn_config::library::{self, LibraryKind, LibraryRoots, Origin};
use std::path::PathBuf;

use cairn_uri::{build_project_recipe_uri, build_recipe_uri};

use crate::config::recipes::{self as config_recipes, FileRecipe};
use crate::config::slugify;
use crate::mcp::types::McpCallbackRequest;
use crate::models::RecipeFile;
use crate::orchestrator::Orchestrator;

fn recipe_file_exists(roots: &LibraryRoots, id: &str, layer: &Origin) -> bool {
    roots
        .layer_dir(LibraryKind::Recipe, layer)
        .is_some_and(|dir| {
            ["yaml", "yml"]
                .iter()
                .any(|ext| dir.join(format!("{id}.{ext}")).exists())
        })
}

/// The recipe a write addresses: the project's own for an explicit project,
/// else whatever the id resolves to.
fn addressed_recipe(
    roots: &LibraryRoots,
    recipe_id: &str,
    explicit_project: Option<&str>,
) -> Result<FileRecipe, String> {
    let resolved = match (explicit_project, roots.project_origin()) {
        (Some(_), Some(layer)) => config_recipes::resolve_recipe_in(roots, &layer, recipe_id),
        _ => config_recipes::resolve_recipe(roots, recipe_id),
    };
    match resolved {
        Ok(resolved) => Ok(resolved.item),
        Err(error) if error.is_not_found() => Err(not_found(recipe_id, explicit_project)),
        Err(error) => Err(error.to_string()),
    }
}

fn parse_and_validate(content: &str) -> Result<RecipeFile, String> {
    let recipe_file = RecipeFile::from_yaml(content)?;
    let validation = recipe_file.validate();
    if !validation.valid {
        return Err(format!("Invalid recipe: {}", validation.errors.join(", ")));
    }
    Ok(recipe_file)
}

fn build_file_recipe(recipe_file: RecipeFile, id: &str, origin: Origin) -> FileRecipe {
    let (ws_id, proj_id) = if origin.is_project() {
        (None, None)
    } else {
        (Some("default".to_string()), None)
    };
    let bundles = recipe_file.bundles.clone();
    let mut recipe = recipe_file.into_recipe(ws_id, proj_id);
    recipe.id = id.to_string();
    FileRecipe {
        recipe,
        bundles,
        origin,
        file_path: PathBuf::new(),
    }
}

pub(super) async fn apply_recipe_create(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &serde_json::Value,
    explicit_project: Option<&str>,
) -> Result<String, String> {
    let content = super::payload_str(payload, "content", &[])
        .filter(|value| !value.trim().is_empty())
        .ok_or("payload.content is required and must be the recipe YAML body")?;
    let recipe_file = parse_and_validate(content)?;

    let id = super::payload_trimmed_non_empty_str(payload, "id", &[])
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| slugify(&recipe_file.name));
    if id.is_empty() {
        return Err("Could not derive recipe id from payload.id or the recipe name".to_string());
    }

    let project_path = super::scope_project_path(orch, request, explicit_project).await?;
    let roots = LibraryRoots::at(&orch.config_dir, project_path.as_deref());
    let layer = super::write_layer(payload, &roots, explicit_project)?.unwrap_or(Origin::Workspace);
    if recipe_file_exists(&roots, &id, &layer) {
        return Err(format!("Recipe already exists: {id}"));
    }

    let file_recipe = build_file_recipe(recipe_file, &id, layer);
    let path = config_recipes::save_recipe(&roots, &file_recipe, None)?;
    crate::config::commit_config_paths(
        std::slice::from_ref(&path),
        &format!("cairn: create recipe {id}"),
    );
    let recipe_uri = match explicit_project {
        Some(project) => build_project_recipe_uri(project, &id),
        None => build_recipe_uri(&id),
    };
    Ok(super::created_resource_ack("recipe", &id, &recipe_uri))
}

/// Patch a recipe. Two forms are accepted:
///
/// - Full-content replace: `payload.content` carries the whole recipe YAML body,
///   re-serialized through the loader (the original, backward-compatible path).
/// - Targeted text replacement: `payload.old_string`/`new_string` (with optional
///   `replace_all`) edit the YAML source in place, mirroring the artifact patch
///   model. The result is re-validated through the SAME loader/validator the
///   full-content path uses, so an edit that yields invalid YAML or an invalid
///   recipe is rejected rather than saved.
pub(super) async fn apply_recipe_patch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &serde_json::Value,
    recipe_id: &str,
    explicit_project: Option<&str>,
) -> Result<String, String> {
    let project_path = super::scope_project_path(orch, request, explicit_project).await?;
    let roots = LibraryRoots::at(&orch.config_dir, project_path.as_deref());
    // Confirm the recipe exists in the addressed scope before overwriting.
    let existing = addressed_recipe(&roots, recipe_id, explicit_project)?;
    let target = super::write_layer(payload, &roots, explicit_project)?
        .unwrap_or_else(|| existing.origin.clone());
    if let Origin::Pack { id: pack } = &target {
        return Err(library::pack_edit_refusal(
            LibraryKind::Recipe,
            &existing.recipe.id,
            pack,
        ));
    }
    // Same layer: the file it was read from. Another layer: a new definition
    // there, in front of the one it was read from.
    let write_path = if target == existing.origin {
        existing.file_path.clone()
    } else {
        roots
            .layer_dir(LibraryKind::Recipe, &target)
            .ok_or("Project path required for a project-scoped recipe")?
            .join(format!("{}.yaml", existing.recipe.id))
    };

    let has_text_replacement =
        payload.get("old_string").is_some() || payload.get("new_string").is_some();
    let has_content = super::payload_str(payload, "content", &[])
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);

    if has_text_replacement {
        if has_content {
            return Err(
                "payload.content cannot be combined with old_string/new_string; \
                 use one full-content replace OR one targeted text replacement"
                    .to_string(),
            );
        }
        let updated = apply_recipe_text_replacement(payload, &existing)?;
        if let Some(dir) = write_path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| format!("Failed to create directory: {e}"))?;
        }
        std::fs::write(&write_path, &updated)
            .map_err(|e| format!("Failed to write recipe file: {e}"))?;
        crate::config::commit_config_paths(
            std::slice::from_ref(&write_path),
            &format!("cairn: edit recipe {recipe_id}"),
        );
        return Ok(format!("Edited recipe '{recipe_id}'"));
    }

    let content = super::payload_str(payload, "content", &[])
        .filter(|value| !value.trim().is_empty())
        .ok_or(
            "payload.content (full recipe YAML body) or old_string/new_string (targeted edit) is required",
        )?;
    let recipe_file = parse_and_validate(content)?;

    let mut file_recipe = build_file_recipe(recipe_file, &existing.recipe.id, target);
    file_recipe.file_path = write_path;
    let path = config_recipes::save_recipe(&roots, &file_recipe, None)?;
    crate::config::commit_config_paths(
        std::slice::from_ref(&path),
        &format!("cairn: update recipe {recipe_id}"),
    );
    Ok(format!("Updated recipe '{recipe_id}'"))
}

/// Apply a targeted text replacement to a recipe's on-disk YAML source and
/// return the validated result. Reads the existing source verbatim, reuses the
/// artifact text-replacement primitive (wildcard + literal matching, multi-match
/// guard), then re-validates the whole document through `parse_and_validate`.
fn apply_recipe_text_replacement(
    payload: &serde_json::Value,
    existing: &FileRecipe,
) -> Result<String, String> {
    let old = payload
        .get("old_string")
        .and_then(|value| value.as_str())
        .ok_or("payload.old_string is required for a targeted recipe edit")?;
    let new = payload
        .get("new_string")
        .and_then(|value| value.as_str())
        .ok_or("payload.new_string is required for a targeted recipe edit")?;
    let replace_all = payload
        .get("replace_all")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);

    let source = std::fs::read_to_string(&existing.file_path)
        .map_err(|e| format!("Failed to read recipe source: {e}"))?;
    recipe_text_replacement(&source, old, new, replace_all)
}

/// Pure text-replacement + revalidation core, factored out for unit testing.
fn recipe_text_replacement(
    source: &str,
    old: &str,
    new: &str,
    replace_all: bool,
) -> Result<String, String> {
    let updated = crate::mcp::handlers::comments_artifacts::replace_artifact_text(
        source,
        old,
        new,
        replace_all,
    )?;
    // Re-validate through the same loader/validator the full-content path uses.
    parse_and_validate(&updated)?;
    Ok(updated)
}

pub(super) async fn apply_recipe_delete(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    recipe_id: &str,
    explicit_project: Option<&str>,
) -> Result<String, String> {
    let project_path = super::scope_project_path(orch, request, explicit_project).await?;
    let roots = LibraryRoots::at(&orch.config_dir, project_path.as_deref());
    let existing = addressed_recipe(&roots, recipe_id, explicit_project)?;
    let changed = config_recipes::delete_recipe(&roots, recipe_id, Some(&existing.origin))?
        .ok_or_else(|| not_found(recipe_id, explicit_project))?;
    crate::config::commit_config_paths(
        std::slice::from_ref(&changed),
        &format!("cairn: delete recipe {recipe_id}"),
    );
    Ok(match existing.origin.pack_id() {
        Some(pack) => format!("Removed recipe '{recipe_id}' from pack {pack}"),
        None => format!("Deleted recipe '{recipe_id}'"),
    })
}

fn not_found(recipe_id: &str, explicit_project: Option<&str>) -> String {
    match explicit_project {
        Some(project) => format!(
            "Recipe not found in project {}: {recipe_id}",
            cairn_uri::canonical_project(project)
        ),
        None => format!("Recipe not found: {recipe_id}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_SOURCE: &str = "cairnVersion: 1\nname: Build Flow\ndescription: A flow\ntrigger: issue\ncontext: issue\n\nnodes:\n  - id: trigger-1\n    type: trigger\n    name: Trigger\n    position: 0@0\n\nedges: []\n";

    #[test]
    fn text_replacement_edits_one_field_and_round_trips() {
        let updated = recipe_text_replacement(
            VALID_SOURCE,
            "name: Build Flow",
            "name: Renamed Flow",
            false,
        )
        .expect("valid edit should succeed");
        // The targeted field changed.
        assert!(updated.contains("name: Renamed Flow"));
        assert!(!updated.contains("name: Build Flow"));
        // The rest of the document is untouched (formatting preserved).
        assert!(updated.contains("cairnVersion: 1"));
        assert!(updated.contains("type: trigger"));
        assert!(updated.contains("edges: []"));
        // And it still parses + validates as a real recipe.
        let recipe_file = parse_and_validate(&updated).expect("result is a valid recipe");
        assert_eq!(recipe_file.name, "Renamed Flow");
    }

    #[test]
    fn text_replacement_yielding_invalid_yaml_is_rejected() {
        // Turn `name: Build Flow` into an unterminated flow sequence: still a
        // text edit, but the resulting document no longer parses as YAML.
        let result =
            recipe_text_replacement(VALID_SOURCE, "name: Build Flow", "name: [unclosed", false);
        assert!(
            result.is_err(),
            "invalid YAML must be rejected, got {result:?}"
        );
    }

    #[test]
    fn text_replacement_yielding_invalid_recipe_is_rejected() {
        // Valid YAML, but removing the only node leaves a recipe with no
        // trigger node — the loader's validator must reject it.
        let result = recipe_text_replacement(
            VALID_SOURCE,
            "nodes:\n  - id: trigger-1\n    type: trigger\n    name: Trigger\n    position: 0@0\n",
            "nodes: []\n",
            false,
        );
        assert!(
            result.is_err(),
            "a recipe with no nodes must be rejected, got {result:?}"
        );
    }

    #[test]
    fn text_replacement_missing_anchor_errors() {
        let result = recipe_text_replacement(VALID_SOURCE, "name: Nonexistent", "name: X", false);
        assert!(result.is_err(), "a non-matching old_string must error");
    }

    #[test]
    fn full_content_replace_still_validates() {
        // The backward-compatible full-content path runs the same validator.
        assert!(parse_and_validate(VALID_SOURCE).is_ok());
        assert!(parse_and_validate("cairnVersion: 1\nname: Broken\n").is_err());
    }
}
