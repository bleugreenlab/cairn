//! `write` over project lifecycle and project settings.
//!
//! - `cairn://projects` create registers a new project from a local git repo.
//! - `cairn://p/PROJECT` patch renames, hides/unhides, or attaches a remote.
//! - `cairn://p/PROJECT/settings` patch routes each field to its store:
//!   `project-settings.yaml` (setup/terminal commands, worktree populate,
//!   default branch, references), the projects DB row (default branch, name,
//!   hidden), and the per-project identity overrides store.

use serde_json::{json, Value};
use std::path::{Path, PathBuf};

use crate::config::project_settings::{
    load_project_settings, save_project_settings, PopulateConfig, ProjectSettingsFile,
};
use crate::identity::AccountOverrides;
use crate::mcp::types::{ChangeItem, ChangeMode};
use crate::models::{CreateProject, TerminalCommand};
use crate::orchestrator::Orchestrator;
use crate::projects::{crud, remote};
use crate::references::ProjectReference;
use crate::services::RealClock;
use crate::storage::RowExt;
use cairn_common::uri::{parse_uri, CairnResource};

fn emit_db_change(orch: &Orchestrator, action: &str) {
    if let Err(error) = orch
        .services
        .emitter
        .emit("db-change", json!({"table": "projects", "action": action}))
    {
        log::error!("Failed to emit db-change event: {error}");
    }
}

fn first_value<'a>(payload: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|key| payload.get(*key))
}

fn req_str(payload: &Value, keys: &[&str], label: &str) -> Result<String, String> {
    first_value(payload, keys)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("'{label}' is required and must be a non-empty string"))
}

fn opt_trimmed(payload: &Value, keys: &[&str]) -> Option<String> {
    first_value(payload, keys)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn array<'a>(value: &'a Value, key: &str) -> Result<Vec<&'a Value>, String> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => Ok(items.iter().collect()),
        Some(_) => Err(format!("'{key}' must be an array")),
    }
}

fn string_array(value: &Value, key: &str) -> Result<Vec<String>, String> {
    array(value, key)?
        .into_iter()
        .map(|item| {
            item.as_str()
                .map(str::to_string)
                .ok_or_else(|| format!("'{key}' entries must be strings"))
        })
        .collect()
}

/// The non-empty `repoPath` a `cairn://projects` create would init/git outside
/// the worktree, if any. An empty/absent path means a pure DB insert (no fs
/// write), so no fence crossing.
fn project_create_repo_path(payload: Option<&Value>) -> Option<PathBuf> {
    payload
        .and_then(|p| first_value(p, &["repoPath", "repo_path"]))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// True when a `cairn://p/PROJECT` patch carries a non-empty `remoteUrl` — the
/// only project-patch field that runs git on the repo (rename/hide are pure DB).
fn project_patch_has_remote(payload: Option<&Value>) -> bool {
    payload
        .and_then(|p| first_value(p, &["remoteUrl", "remote_url"]))
        .and_then(Value::as_str)
        .map(str::trim)
        .map(|value| !value.is_empty())
        .unwrap_or(false)
}

/// If `item` is a project write that creates/inits or runs git on a repo outside
/// the worktree, return that repo path so the change handler can gate it through
/// the same worktree fence as workspace settings/MCP writes. Pure-DB writes
/// (rename/hide, or a create with an empty repoPath) return `None`.
pub(crate) async fn project_write_crossing_path(
    orch: &Orchestrator,
    item: &ChangeItem,
) -> Option<PathBuf> {
    match parse_uri(&item.target) {
        Some(CairnResource::Projects) if item.mode == ChangeMode::Create => {
            project_create_repo_path(item.payload.as_ref())
        }
        Some(CairnResource::Project { project }) if item.mode == ChangeMode::Patch => {
            if !project_patch_has_remote(item.payload.as_ref()) {
                return None;
            }
            resolve_project(orch, &project)
                .await
                .ok()
                .map(|(_, repo_path, _)| PathBuf::from(repo_path))
        }
        _ => None,
    }
}

/// Resolve a project by key to `(id, repo_path, default_branch)`.
async fn resolve_project(
    orch: &Orchestrator,
    project_key: &str,
) -> Result<(String, String, Option<String>), String> {
    let lookup = project_key.to_uppercase();
    let row = orch
        .db
        .local
        .read(move |conn| {
            let lookup = lookup.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, repo_path, default_branch FROM projects WHERE key = ?1 LIMIT 1",
                        (lookup.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((row.text(0)?, row.text(1)?, row.opt_text(2)?))),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|error| format!("Failed to load project: {error}"))?;
    row.ok_or_else(|| format!("Project '{project_key}' not found"))
}

pub(super) async fn apply_projects_create(
    orch: &Orchestrator,
    payload: &Value,
    dry_run: bool,
) -> Result<String, String> {
    let key = req_str(payload, &["key"], "key")?;
    let name = req_str(payload, &["name"], "name")?;
    let repo_path = req_str(payload, &["repoPath", "repo_path"], "repoPath")?;
    let default_branch = opt_trimmed(payload, &["defaultBranch", "default_branch"]);

    if dry_run {
        return Ok(format!("Would create project {key} at {repo_path}"));
    }

    let input = CreateProject {
        id: None,
        name,
        key: key.clone(),
        repo_path,
        remote_url: None,
        server_id: None,
    };
    let project = crud::create(&orch.db.local, &RealClock, input, None)
        .await
        .map_err(|error| error.to_string())?;

    if let Some(branch) = default_branch {
        crud::set_default_branch_db(&orch.db.local, &project.id, &branch)
            .await
            .map_err(|error| error.to_string())?;
    }

    emit_db_change(orch, "insert");
    Ok(format!("Created project {key}"))
}

pub(super) async fn apply_project_patch(
    orch: &Orchestrator,
    project_key: &str,
    payload: &Value,
    dry_run: bool,
) -> Result<String, String> {
    let (id, repo_path, default_branch) = resolve_project(orch, project_key).await?;
    let mut ops: Vec<&str> = Vec::new();

    if let Some(name) = opt_trimmed(payload, &["name"]) {
        if !dry_run {
            crud::set_name_db(&orch.db.local, &id, &name)
                .await
                .map_err(|error| error.to_string())?;
        }
        ops.push("rename");
    }
    if let Some(hidden) = payload.get("hidden") {
        let hidden = hidden.as_bool().ok_or("payload.hidden must be a boolean")?;
        if !dry_run {
            crud::set_hidden_db(&orch.db.local, &id, hidden)
                .await
                .map_err(|error| error.to_string())?;
        }
        ops.push("hidden");
    }
    if let Some(remote_url) = opt_trimmed(payload, &["remoteUrl", "remote_url"]) {
        let branch = default_branch.clone().unwrap_or_else(|| "main".to_string());
        if !dry_run {
            remote::attach_remote(
                orch.services.git.as_ref(),
                Path::new(&repo_path),
                &branch,
                &remote_url,
            )?;
        }
        ops.push("attach remote");
    }

    if ops.is_empty() {
        return Err("payload must set at least one of: name, hidden, remoteUrl".to_string());
    }
    if !dry_run {
        emit_db_change(orch, "update");
    }
    let verb = if dry_run { "Would patch" } else { "Patched" };
    Ok(format!("{verb} project {project_key}: {}", ops.join(", ")))
}

struct ReferenceOps {
    changed: bool,
    count: usize,
}

fn apply_references(
    orch: &Orchestrator,
    config: &mut ProjectSettingsFile,
    refs: &Value,
    dry_run: bool,
) -> Result<ReferenceOps, String> {
    let mut changed = false;
    let mut count = 0;

    for item in array(refs, "add")? {
        let reference: ProjectReference = serde_json::from_value(item.clone())
            .map_err(|error| format!("invalid reference: {error}"))?;
        if let Some(existing) = &config.references {
            if existing.iter().any(|r| r.name == reference.name) {
                return Err(format!("reference '{}' already exists", reference.name));
            }
        }
        if !dry_run {
            if reference.git.is_some() {
                crate::references::clone_reference(&orch.config_dir, &reference)?;
            }
            if let Some(description) = &reference.description {
                let _ = crate::references::save_reference_description(
                    &orch.config_dir,
                    &reference.name,
                    description,
                );
            }
        }
        config
            .references
            .get_or_insert_with(Vec::new)
            .push(reference);
        changed = true;
        count += 1;
    }

    for name in string_array(refs, "remove")? {
        if let Some(existing) = &mut config.references {
            let before = existing.len();
            existing.retain(|r| r.name != name);
            if existing.len() != before {
                changed = true;
            }
            if existing.is_empty() {
                config.references = None;
            }
        }
        count += 1;
    }

    for name in string_array(refs, "refresh")? {
        let reference = config
            .references
            .as_ref()
            .and_then(|refs| refs.iter().find(|r| r.name == name))
            .cloned()
            .ok_or_else(|| format!("reference '{name}' not found"))?;
        if !dry_run {
            crate::references::refresh_reference(&orch.config_dir, &reference)?;
        }
        count += 1;
    }

    Ok(ReferenceOps { changed, count })
}

pub(super) async fn apply_project_settings_patch(
    orch: &Orchestrator,
    project_key: &str,
    payload: &Value,
    dry_run: bool,
) -> Result<String, String> {
    let (id, repo_path, _db_default_branch) = resolve_project(orch, project_key).await?;
    let repo = Path::new(&repo_path);
    let mut config = load_project_settings(repo);
    let mut file_changed = false;
    let mut ops: Vec<String> = Vec::new();

    if let Some(commands) = first_value(payload, &["setupCommands", "setup_commands"]) {
        let commands: Vec<String> = serde_json::from_value(commands.clone())
            .map_err(|error| format!("invalid setupCommands: {error}"))?;
        config.setup_commands = Some(commands);
        file_changed = true;
        ops.push("setupCommands".to_string());
    }
    if let Some(commands) = first_value(payload, &["terminalCommands", "terminal_commands"]) {
        let commands: Vec<TerminalCommand> = serde_json::from_value(commands.clone())
            .map_err(|error| format!("invalid terminalCommands: {error}"))?;
        config.terminal_commands = Some(commands);
        file_changed = true;
        ops.push("terminalCommands".to_string());
    }
    if let Some(populate) = first_value(payload, &["worktreePopulate", "worktree_populate"]) {
        let populate: PopulateConfig = serde_json::from_value(populate.clone())
            .map_err(|error| format!("invalid worktreePopulate: {error}"))?;
        config
            .worktree
            .get_or_insert_with(Default::default)
            .populate = populate;
        file_changed = true;
        ops.push("worktreePopulate".to_string());
    }
    if let Some(branch) = opt_trimmed(payload, &["defaultBranch", "default_branch"]) {
        config.default_branch = Some(branch.clone());
        file_changed = true;
        if !dry_run {
            crud::set_default_branch_db(&orch.db.local, &id, &branch)
                .await
                .map_err(|error| error.to_string())?;
        }
        ops.push("defaultBranch".to_string());
    }
    if let Some(overrides_value) = first_value(payload, &["accountOverrides", "account_overrides"])
    {
        let overrides: Option<AccountOverrides> = if overrides_value.is_null() {
            None
        } else {
            Some(
                serde_json::from_value(overrides_value.clone())
                    .map_err(|error| format!("invalid accountOverrides: {error}"))?,
            )
        };
        if !dry_run {
            orch.set_project_overrides(&id, overrides)?;
        }
        ops.push("accountOverrides".to_string());
    }
    if let Some(refs) = payload.get("references") {
        let result = apply_references(orch, &mut config, refs, dry_run)?;
        if result.changed {
            file_changed = true;
        }
        ops.push(format!("references ({} op(s))", result.count));
    }

    if ops.is_empty() {
        return Err(
            "payload must set at least one of: setupCommands, terminalCommands, worktreePopulate, defaultBranch, accountOverrides, references"
                .to_string(),
        );
    }

    if file_changed && !dry_run {
        save_project_settings(repo, &config)?;
        crate::config::commit_project_config_change(
            repo,
            &format!("cairn: update project settings for {project_key}"),
        );
    }
    if !dry_run {
        let _ = orch
            .services
            .emitter
            .emit("config-changed", json!({"entity_type": "project_settings"}));
        emit_db_change(orch, "update");
    }

    let verb = if dry_run { "Would patch" } else { "Patched" };
    Ok(format!(
        "{verb} project settings for {project_key}: {}",
        ops.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_crossing_path_only_for_nonempty_repo_path() {
        let with_path = json!({"key": "DEMO", "name": "Demo", "repoPath": "/abs/repo"});
        assert_eq!(
            project_create_repo_path(Some(&with_path)),
            Some(PathBuf::from("/abs/repo"))
        );
        // snake_case alias is accepted.
        let snake = json!({"repo_path": "/abs/repo2"});
        assert_eq!(
            project_create_repo_path(Some(&snake)),
            Some(PathBuf::from("/abs/repo2"))
        );
        // Empty or absent path -> pure DB insert, no fence crossing.
        assert_eq!(
            project_create_repo_path(Some(&json!({"key": "DEMO", "repoPath": "  "}))),
            None
        );
        assert_eq!(
            project_create_repo_path(Some(&json!({"key": "DEMO"}))),
            None
        );
        assert_eq!(project_create_repo_path(None), None);
    }

    #[test]
    fn patch_needs_fence_only_when_attaching_remote() {
        assert!(project_patch_has_remote(Some(&json!({
            "remoteUrl": "git@github.com:o/r.git"
        }))));
        // rename/hide are pure DB -> no git, no fence.
        assert!(!project_patch_has_remote(Some(&json!({"name": "Renamed"}))));
        assert!(!project_patch_has_remote(Some(&json!({"hidden": true}))));
        assert!(!project_patch_has_remote(Some(&json!({"remoteUrl": "  "}))));
        assert!(!project_patch_has_remote(None));
    }
}
