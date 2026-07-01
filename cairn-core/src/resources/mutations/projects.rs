//! `write` over project lifecycle and project settings.
//!
//! - `cairn://projects` create registers a new project from a local git repo.
//! - `cairn://p/PROJECT` patch renames, hides/unhides, or attaches a remote.
//! - `cairn://p/PROJECT/settings` patch routes each field to its store:
//!   `project-settings.yaml` (setup/terminal commands, worktree populate,
//!   default branch, references, background-testing checks), the projects DB row
//!   (default branch, name, hidden), and the per-project identity overrides store.

use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::config::project_settings::{
    load_project_settings, save_project_settings, CheckCommand, PopulateConfig, ProjectSettingsFile,
};
use crate::identity::AccountOverrides;
use crate::mcp::types::{ChangeItem, ChangeMode};
use crate::models::{CreateProject, TerminalCommand};
use crate::orchestrator::Orchestrator;
use crate::projects::{crud, remote};
use crate::references::ProjectReference;
use crate::services::RealClock;
use crate::storage::{LocalDb, RowExt};
use cairn_common::uri::{parse_uri, CairnResource};
use std::collections::HashMap;

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
                .map(|(_, _, repo_path, _)| PathBuf::from(repo_path))
        }
        _ => None,
    }
}

/// Resolve a project by key to `(owning_db, id, repo_path, default_branch)`.
///
/// Routes through `for_project(key)` so a team-only project (whose `projects` row
/// lives only in its team replica) resolves to and is read from that replica; a
/// local project resolves to the private database (a strict no-op). The returned
/// handle is the one every project-patch seam must mutate, so a routed patch lands
/// in the same database the row was read from.
async fn resolve_project(
    orch: &Orchestrator,
    project_key: &str,
) -> Result<(Arc<LocalDb>, String, String, Option<String>), String> {
    let db = orch.db.for_project(project_key).await;
    let lookup = project_key.to_uppercase();
    let row = db
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
    row.map(|(id, repo_path, default_branch)| (db, id, repo_path, default_branch))
        .ok_or_else(|| format!("Project '{project_key}' not found"))
}

/// The optional team id a `cairn://projects` create routes to, parsed from the
/// payload (`teamId`/`team_id`). `None` lands the project's row in the private
/// database (a local project); `Some(team)` routes it to that team's replica.
fn project_create_team_id(payload: &Value) -> Option<String> {
    opt_trimmed(payload, &["teamId", "team_id"])
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
    // Route to a team's shared database when the payload carries a team id;
    // absent, the row lands in the private database exactly as before.
    let team_id = project_create_team_id(payload);

    if dry_run {
        return Ok(format!("Would create project {key} at {repo_path}"));
    }

    let input = CreateProject {
        id: None,
        name,
        key: key.clone(),
        repo_path,
        team_id,
    };
    let (project, target_db) = crud::create_routed(&orch.db, &RealClock, input, None)
        .await
        .map_err(|error| error.to_string())?;

    if let Some(branch) = default_branch {
        crud::set_default_branch_db(&target_db, &project.id, &branch)
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
    let (db, id, repo_path, default_branch) = resolve_project(orch, project_key).await?;
    let mut ops: Vec<&str> = Vec::new();

    if let Some(name) = opt_trimmed(payload, &["name"]) {
        if !dry_run {
            crud::set_name_db(&db, &id, &name)
                .await
                .map_err(|error| error.to_string())?;
        }
        ops.push("rename");
    }
    if let Some(hidden) = payload.get("hidden") {
        let hidden = hidden.as_bool().ok_or("payload.hidden must be a boolean")?;
        if !dry_run {
            crud::set_hidden_db(&db, &id, hidden)
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

fn nullable_trimmed(payload: &Value, key: &str) -> Result<Option<Option<String>>, String> {
    match payload.get(key) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::String(value)) => Ok(Some({
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })),
        Some(_) => Err(format!("'{key}' must be a string or null")),
    }
}

fn payload_bool_value(payload: &Value, key: &str) -> Result<Option<bool>, String> {
    match payload.get(key) {
        None => Ok(None),
        Some(value) => value
            .as_bool()
            .map(Some)
            .ok_or_else(|| format!("'{key}' must be a boolean")),
    }
}

fn validate_reference_source(reference: &ProjectReference) -> Result<(), String> {
    match (reference.git.as_ref(), reference.path.as_ref()) {
        (Some(_), None) => Ok(()),
        (None, Some(_)) if reference.branch.is_none() => Ok(()),
        (None, Some(_)) => Err("'branch' is only valid for git references".to_string()),
        (Some(_), Some(_)) => Err("reference must set exactly one of 'git' or 'path'".to_string()),
        (None, None) => Err("reference must set exactly one of 'git' or 'path'".to_string()),
    }
}

fn reference_from_create_payload(payload: &Value) -> Result<ProjectReference, String> {
    let reference = ProjectReference {
        name: req_str(payload, &["name"], "name")?,
        git: opt_trimmed(payload, &["git"]),
        path: opt_trimmed(payload, &["path"]),
        description: nullable_trimmed(payload, "description")?.flatten(),
        branch: nullable_trimmed(payload, "branch")?.flatten(),
    };
    validate_reference_source(&reference)?;
    Ok(reference)
}

fn clone_and_save_reference_metadata(
    orch: &Orchestrator,
    reference: &ProjectReference,
) -> Result<(), String> {
    if reference.git.is_some() {
        crate::references::clone_reference(&orch.config_dir, reference)?;
    }
    if let Some(description) = &reference.description {
        if !description.is_empty() {
            crate::references::save_reference_description(
                &orch.config_dir,
                &reference.name,
                description,
            )?;
        }
    }
    Ok(())
}

fn add_reference_to_config(
    config: &mut ProjectSettingsFile,
    payload: &Value,
) -> Result<ProjectReference, String> {
    let reference = reference_from_create_payload(payload)?;
    if let Some(existing) = &config.references {
        if existing.iter().any(|r| r.name == reference.name) {
            return Err(format!("reference '{}' already exists", reference.name));
        }
    }
    config
        .references
        .get_or_insert_with(Vec::new)
        .push(reference.clone());
    Ok(reference)
}

fn create_reference_in_config(
    orch: &Orchestrator,
    config: &mut ProjectSettingsFile,
    payload: &Value,
    dry_run: bool,
) -> Result<ProjectReference, String> {
    let reference = add_reference_to_config(config, payload)?;
    if !dry_run {
        clone_and_save_reference_metadata(orch, &reference)?;
    }
    Ok(reference)
}

fn delete_reference_in_config(config: &mut ProjectSettingsFile, name: &str) -> bool {
    let mut changed = false;
    if let Some(existing) = &mut config.references {
        let before = existing.len();
        existing.retain(|r| r.name != name);
        changed = existing.len() != before;
        if existing.is_empty() {
            config.references = None;
        }
    }
    changed
}

#[derive(Debug)]
struct ReferencePatchPlan {
    reference: ProjectReference,
    changed: bool,
    source_changed: bool,
    description_present: bool,
    branch_changed: bool,
    refresh: bool,
}

fn plan_reference_patch(
    reference: &ProjectReference,
    payload: &Value,
) -> Result<ReferencePatchPlan, String> {
    let mut patched = reference.clone();
    let mut changed = false;
    let mut source_changed = false;
    let mut description_present = false;
    let mut branch_changed = false;

    if let Some(value) = nullable_trimmed(payload, "git")? {
        if patched.git != value {
            patched.git = value;
            changed = true;
            source_changed = true;
        }
    }
    if let Some(value) = nullable_trimmed(payload, "path")? {
        if patched.path != value {
            patched.path = value;
            changed = true;
            source_changed = true;
        }
    }
    if let Some(value) = nullable_trimmed(payload, "description")? {
        description_present = true;
        if patched.description != value {
            patched.description = value;
            changed = true;
        }
    }
    if let Some(value) = nullable_trimmed(payload, "branch")? {
        if patched.branch != value {
            patched.branch = value;
            changed = true;
            branch_changed = true;
        }
    }
    let refresh = payload_bool_value(payload, "refresh")?.unwrap_or(false);

    if !changed && !refresh {
        return Err(
            "payload must set at least one of: git, path, description, branch, refresh".to_string(),
        );
    }
    validate_reference_source(&patched)?;

    Ok(ReferencePatchPlan {
        reference: patched,
        changed,
        source_changed,
        description_present,
        branch_changed,
        refresh,
    })
}

fn patch_reference_in_config_pure(
    config: &mut ProjectSettingsFile,
    name: &str,
    payload: &Value,
) -> Result<ReferencePatchPlan, String> {
    let references = config
        .references
        .as_mut()
        .ok_or_else(|| format!("reference '{name}' not found"))?;
    let reference = references
        .iter_mut()
        .find(|r| r.name == name)
        .ok_or_else(|| format!("reference '{name}' not found"))?;
    let plan = plan_reference_patch(reference, payload)?;
    *reference = plan.reference.clone();
    Ok(plan)
}

fn patch_reference_in_config(
    orch: &Orchestrator,
    config: &mut ProjectSettingsFile,
    name: &str,
    payload: &Value,
    dry_run: bool,
) -> Result<bool, String> {
    let plan = patch_reference_in_config_pure(config, name, payload)?;

    if !dry_run {
        if (plan.source_changed || plan.branch_changed) && plan.reference.git.is_some() {
            crate::references::clone_reference(&orch.config_dir, &plan.reference)?;
        }
        if plan.description_present {
            crate::references::save_reference_description(
                &orch.config_dir,
                &plan.reference.name,
                plan.reference.description.as_deref().unwrap_or(""),
            )?;
        } else if let Some(description) = &plan.reference.description {
            if !description.is_empty() {
                crate::references::save_reference_description(
                    &orch.config_dir,
                    &plan.reference.name,
                    description,
                )?;
            }
        }
        if plan.refresh {
            crate::references::refresh_reference(&orch.config_dir, &plan.reference)?;
        }
    }
    Ok(plan.changed)
}

fn refresh_reference_in_config(
    orch: &Orchestrator,
    config: &ProjectSettingsFile,
    name: &str,
    dry_run: bool,
) -> Result<(), String> {
    let reference = config
        .references
        .as_ref()
        .and_then(|refs| refs.iter().find(|r| r.name == name))
        .cloned()
        .ok_or_else(|| format!("reference '{name}' not found"))?;
    if !dry_run {
        crate::references::refresh_reference(&orch.config_dir, &reference)?;
    }
    Ok(())
}

async fn load_project_settings_for_mutation(
    orch: &Orchestrator,
    project_key: &str,
) -> Result<(String, PathBuf, ProjectSettingsFile), String> {
    // resolve_project returns (owning_db, id, repo_path, default_branch) since
    // CAIRN-2136. Reference handlers operate on the repo's on-disk settings file
    // (filesystem), not the database, so the owning_db is intentionally ignored;
    // repo_path still comes from the routed lookup so a team project resolves.
    let (_owning_db, _id, repo_path, _db_default_branch) =
        resolve_project(orch, project_key).await?;
    let repo = PathBuf::from(repo_path);
    let config = load_project_settings(&repo);
    Ok((project_key.to_uppercase(), repo, config))
}

fn save_project_settings_after_reference_change(
    orch: &Orchestrator,
    repo: &Path,
    config: &ProjectSettingsFile,
    changed: bool,
    dry_run: bool,
) -> Result<(), String> {
    if changed && !dry_run {
        save_project_settings(repo, config)?;
    }
    if !dry_run {
        let _ = orch
            .services
            .emitter
            .emit("config-changed", json!({"entity_type": "project_settings"}));
        emit_db_change(orch, "update");
    }
    Ok(())
}

pub(super) async fn apply_project_reference_create(
    orch: &Orchestrator,
    project_key: &str,
    payload: &Value,
    dry_run: bool,
) -> Result<String, String> {
    let (project, repo, mut config) = load_project_settings_for_mutation(orch, project_key).await?;
    let reference = create_reference_in_config(orch, &mut config, payload, dry_run)?;
    save_project_settings_after_reference_change(orch, &repo, &config, true, dry_run)?;
    let verb = if dry_run { "Would create" } else { "Created" };
    Ok(format!(
        "{verb} project reference '{project}/{}'",
        reference.name
    ))
}

pub(super) async fn apply_project_reference_patch(
    orch: &Orchestrator,
    project_key: &str,
    name: &str,
    payload: &Value,
    dry_run: bool,
) -> Result<String, String> {
    let (project, repo, mut config) = load_project_settings_for_mutation(orch, project_key).await?;
    let changed = patch_reference_in_config(orch, &mut config, name, payload, dry_run)?;
    save_project_settings_after_reference_change(orch, &repo, &config, changed, dry_run)?;
    let verb = if dry_run { "Would patch" } else { "Patched" };
    Ok(format!("{verb} project reference '{project}/{name}'"))
}

pub(super) async fn apply_project_reference_delete(
    orch: &Orchestrator,
    project_key: &str,
    name: &str,
    dry_run: bool,
) -> Result<String, String> {
    let (project, repo, mut config) = load_project_settings_for_mutation(orch, project_key).await?;
    let changed = delete_reference_in_config(&mut config, name);
    save_project_settings_after_reference_change(orch, &repo, &config, changed, dry_run)?;
    let verb = if dry_run { "Would delete" } else { "Deleted" };
    Ok(format!("{verb} project reference '{project}/{name}'"))
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
        create_reference_in_config(orch, config, item, dry_run)?;
        changed = true;
        count += 1;
    }

    for name in string_array(refs, "remove")? {
        if delete_reference_in_config(config, &name) {
            changed = true;
        }
        count += 1;
    }

    for name in string_array(refs, "refresh")? {
        refresh_reference_in_config(orch, config, &name, dry_run)?;
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
    let (db, id, repo_path, _db_default_branch) = resolve_project(orch, project_key).await?;
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
    if let Some(checks) = payload.get("checks") {
        let checks: HashMap<String, CheckCommand> = serde_json::from_value(checks.clone())
            .map_err(|error| format!("invalid checks: {error}"))?;
        // An empty map clears checks so the key drops out of the YAML entirely,
        // matching the `update_project` Tauri command's behavior.
        config.checks = if checks.is_empty() {
            None
        } else {
            Some(checks)
        };
        file_changed = true;
        ops.push("checks".to_string());
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
            crud::set_default_branch_db(&db, &id, &branch)
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
            "payload must set at least one of: setupCommands, terminalCommands, checks, worktreePopulate, defaultBranch, accountOverrides, references"
                .to_string(),
        );
    }

    if file_changed && !dry_run {
        // `save_project_settings` commits the `config.yaml` rewrite, scoped.
        save_project_settings(repo, &config)?;
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
    fn reference_create_rejects_duplicate_names() {
        let mut config = ProjectSettingsFile {
            references: Some(vec![ProjectReference {
                name: "docs".to_string(),
                git: None,
                path: Some("/tmp/docs".to_string()),
                description: None,
                branch: None,
            }]),
            ..Default::default()
        };
        let payload = json!({"name": "docs", "path": "/tmp/other"});

        let error = add_reference_to_config(&mut config, &payload).unwrap_err();
        assert_eq!(error, "reference 'docs' already exists");
        assert_eq!(config.references.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn reference_create_stores_local_path_without_git_fields() {
        let mut config = ProjectSettingsFile::default();
        let payload = json!({
            "name": "docs",
            "path": "/tmp/docs",
            "description": "Project docs"
        });

        let reference = add_reference_to_config(&mut config, &payload).unwrap();

        assert_eq!(reference.name, "docs");
        assert_eq!(reference.path.as_deref(), Some("/tmp/docs"));
        assert_eq!(reference.git, None);
        assert_eq!(reference.description.as_deref(), Some("Project docs"));
        assert_eq!(config.references.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn reference_source_validation_requires_exactly_one_source() {
        let mut reference = ProjectReference {
            name: "bad".to_string(),
            git: None,
            path: None,
            description: None,
            branch: None,
        };
        assert!(validate_reference_source(&reference).is_err());

        reference.git = Some("https://example.com/repo.git".to_string());
        reference.path = Some("/tmp/repo".to_string());
        assert!(validate_reference_source(&reference).is_err());

        reference.git = None;
        reference.branch = Some("main".to_string());
        assert!(validate_reference_source(&reference).is_err());

        reference.branch = None;
        assert!(validate_reference_source(&reference).is_ok());
    }

    #[test]
    fn reference_patch_updates_present_fields_only_and_clears_nullable_fields() {
        let reference = ProjectReference {
            name: "docs".to_string(),
            git: Some("https://example.com/docs.git".to_string()),
            path: None,
            description: Some("Old docs".to_string()),
            branch: Some("main".to_string()),
        };
        let payload = json!({"description": null, "branch": null});

        let plan = plan_reference_patch(&reference, &payload).unwrap();

        assert!(plan.changed);
        assert!(!plan.source_changed);
        assert!(plan.description_present);
        assert!(plan.branch_changed);
        assert_eq!(
            plan.reference.git.as_deref(),
            Some("https://example.com/docs.git")
        );
        assert_eq!(plan.reference.path, None);
        assert_eq!(plan.reference.description, None);
        assert_eq!(plan.reference.branch, None);
    }

    #[test]
    fn reference_patch_rejects_empty_payload_and_missing_member() {
        let reference = ProjectReference {
            name: "docs".to_string(),
            git: None,
            path: Some("/tmp/docs".to_string()),
            description: None,
            branch: None,
        };
        let error = plan_reference_patch(&reference, &json!({})).unwrap_err();
        assert_eq!(
            error,
            "payload must set at least one of: git, path, description, branch, refresh"
        );

        let mut config = ProjectSettingsFile {
            references: Some(vec![reference]),
            ..Default::default()
        };
        let error = patch_reference_in_config_pure(
            &mut config,
            "missing",
            &json!({"description": "Updated"}),
        )
        .unwrap_err();
        assert_eq!(error, "reference 'missing' not found");
    }

    #[test]
    fn reference_patch_source_switch_requires_clearing_old_source() {
        let reference = ProjectReference {
            name: "docs".to_string(),
            git: None,
            path: Some("/tmp/docs".to_string()),
            description: None,
            branch: None,
        };

        let error =
            plan_reference_patch(&reference, &json!({"git": "https://example.com/docs.git"}))
                .unwrap_err();
        assert_eq!(error, "reference must set exactly one of 'git' or 'path'");

        let plan = plan_reference_patch(
            &reference,
            &json!({"git": "https://example.com/docs.git", "path": null, "branch": "main"}),
        )
        .unwrap();
        assert!(plan.changed);
        assert!(plan.source_changed);
        assert!(plan.branch_changed);
        assert_eq!(
            plan.reference.git.as_deref(),
            Some("https://example.com/docs.git")
        );
        assert_eq!(plan.reference.path, None);
        assert_eq!(plan.reference.branch.as_deref(), Some("main"));
    }

    #[test]
    fn reference_delete_removes_config_entry_without_clone_cleanup() {
        let mut config = ProjectSettingsFile {
            references: Some(vec![ProjectReference {
                name: "docs".to_string(),
                git: None,
                path: Some("/tmp/docs".to_string()),
                description: None,
                branch: None,
            }]),
            ..Default::default()
        };

        assert!(delete_reference_in_config(&mut config, "docs"));
        assert!(config.references.is_none());
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

    #[test]
    fn create_parses_optional_team_id_from_either_case() {
        // camelCase and snake_case both resolve, trimmed.
        assert_eq!(
            project_create_team_id(&json!({"key": "DEMO", "teamId": "team-1"})).as_deref(),
            Some("team-1")
        );
        assert_eq!(
            project_create_team_id(&json!({"team_id": "  team-2  "})).as_deref(),
            Some("team-2")
        );
        // Absent or blank -> a local project (private database).
        assert_eq!(project_create_team_id(&json!({"key": "DEMO"})), None);
        assert_eq!(project_create_team_id(&json!({"teamId": "   "})), None);
    }
}
