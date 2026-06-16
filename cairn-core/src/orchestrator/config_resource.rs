//! Shared orchestration for workspace/project-scoped config resources.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::config::ConfigResult;
use crate::storage::{run_db_blocking, RowExt};

use super::Orchestrator;

async fn all_project_paths_from_db(
    db: std::sync::Arc<crate::storage::LocalDb>,
) -> Result<Vec<(String, PathBuf)>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, repo_path
                     FROM projects
                     WHERE COALESCE(is_workspace, 0) = 0
                     ORDER BY name ASC",
                    (),
                )
                .await?;
            let mut projects = Vec::new();
            while let Some(row) = rows.next().await? {
                projects.push((row.text(0)?, PathBuf::from(row.text(1)?)));
            }
            Ok(projects)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

impl Orchestrator {
    /// List all project (id, repo_path) pairs from DB.
    pub(crate) fn all_project_paths(&self) -> Result<Vec<(String, PathBuf)>, String> {
        let db = self.db.local.clone();
        run_db_blocking(move || all_project_paths_from_db(db))
    }

    /// Resolve project_id → repo path from DB.
    pub(crate) fn project_path(&self, project_id: &str) -> Result<PathBuf, String> {
        let db = self.db.local.clone();
        let project_id = project_id.to_string();
        let missing_id = project_id.clone();
        let path = run_db_blocking(move || async move {
            db.read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT repo_path
                             FROM projects
                             WHERE id = ?1",
                            (project_id.as_str(),),
                        )
                        .await?;
                    crate::storage::next_text(&mut rows, 0).await
                })
            })
            .await
            .map_err(|e| e.to_string())
        })?;

        path.map(PathBuf::from)
            .ok_or_else(|| format!("Project not found: {}", missing_id))
    }

    /// List every config file that failed to parse, across agents, recipes, and
    /// skills. Powers the Settings "invalid config" rows so a file the loader
    /// rejected is visible with its error instead of silently missing.
    ///
    /// Scope follows the same rules as the per-entity list methods: both `None`
    /// aggregates workspace plus every project.
    pub fn list_invalid_configs(
        &self,
        workspace_id: Option<&str>,
        project_id: Option<&str>,
    ) -> Result<Vec<InvalidConfig>, String> {
        let mut invalid = Vec::new();
        invalid.extend(
            list_configs_with_invalid::<super::agents::AgentResource>(
                self,
                workspace_id,
                project_id,
            )?
            .1,
        );
        invalid.extend(
            list_configs_with_invalid::<super::recipes::RecipeResource>(
                self,
                workspace_id,
                project_id,
            )?
            .1,
        );
        invalid.extend(
            list_configs_with_invalid::<super::skills::SkillResource>(
                self,
                workspace_id,
                project_id,
            )?
            .1,
        );
        Ok(invalid)
    }
}

pub(crate) fn merge_optional<T: Clone>(
    existing: &Option<T>,
    update: &Option<Option<T>>,
) -> Option<T> {
    match update {
        None => existing.clone(),
        Some(None) => None,
        Some(Some(value)) => Some(value.clone()),
    }
}

pub(crate) trait ConfigResource {
    type Config;
    type File;
    type CreateInput;
    type UpdateInput;

    const ENTITY_TYPE: &'static str;
    const TABLE: &'static str;
    const SUBDIR: &'static str;
    const GET_SEARCHES_ALL_PROJECTS: bool = true;
    const UPDATE_SEARCHES_ALL_PROJECTS: bool = true;

    fn list_files(
        config_dir: &Path,
        project_path: Option<&Path>,
    ) -> Result<Vec<ConfigResult<Self::File>>, String>;
    fn get_file(
        config_dir: &Path,
        id: &str,
        project_path: Option<&Path>,
    ) -> Result<Option<Self::File>, String>;
    fn save_file(
        config_dir: &Path,
        file: &Self::File,
        project_path: Option<&Path>,
    ) -> Result<PathBuf, String>;
    fn delete_file(config_dir: &Path, id: &str, project_path: Option<&Path>) -> Result<(), String>;

    fn file_is_project_scoped(file: &Self::File) -> bool;
    fn to_config(
        file: Self::File,
        workspace_id: Option<String>,
        project_id: Option<String>,
        now: i64,
    ) -> Self::Config;
    fn config_name(cfg: &Self::Config) -> &str;
    fn config_id(cfg: &Self::Config) -> String;

    fn create_id(input: &Self::CreateInput) -> String;
    fn create_input_scopes(input: &Self::CreateInput) -> (Option<String>, Option<String>);
    fn build_create_file(
        input: Self::CreateInput,
        id: String,
        is_project_scoped: bool,
        now: i64,
    ) -> Self::File;

    fn update_scope(
        existing: &Self::File,
        input: &Self::UpdateInput,
        target_project_id: Option<&str>,
    ) -> (bool, Option<String>);
    fn build_update_file(
        existing: &Self::File,
        input: Self::UpdateInput,
        id: &str,
        new_is_project_scoped: bool,
        scope_changing: bool,
        now: i64,
    ) -> Self::File;
    fn cleanup_after_scope_change(existing: &Self::File, dest_path: &Path);

    fn target_exists(scope_root: &Path, id: &str, is_project_scoped: bool) -> bool;
    fn build_scoped_copy(
        source: &Self::File,
        id: &str,
        is_project_scoped: bool,
        workspace_id: Option<String>,
        project_id: Option<String>,
        now: i64,
    ) -> Self::File;
    fn post_save_copy(source: &Self::File, dest_path: &Path) -> Result<(), String>;

    /// The on-disk path that identifies `file` (an agent/recipe file, or a
    /// skill's SKILL.md). Used to stage a delete or an old-scope cleanup.
    fn primary_path(file: &Self::File) -> PathBuf;

    /// Map a primary path (as returned by `save_file` or `primary_path`) to the
    /// path(s) git should stage for this entity. Single-file resources stage the
    /// file itself; directory-backed resources (skills) override to stage the
    /// enclosing directory so sidecar files like `.meta.json` are captured.
    fn stage_paths(primary_path: &Path) -> Vec<PathBuf> {
        vec![primary_path.to_path_buf()]
    }
}

/// A config file the loader could not parse.
///
/// Config files (agents, recipes, skills) are the source of truth, but a file
/// that fails to parse used to be dropped from list results with no log and no
/// UI surface — so a broken agent simply vanished. These entries make the
/// exclusion loud: every dropped file is both logged and carried out of the
/// list path so the Settings UI can render it as a visible "invalid config" row
/// with its parse error.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvalidConfig {
    /// ID derived from the filename (the file stem).
    pub id: String,
    /// One of `agent`, `recipe`, `skill`.
    pub entity_type: String,
    /// Absolute path to the offending file.
    pub path: String,
    /// Project this file belongs to, or `None` for a workspace-scoped file.
    pub project_id: Option<String>,
    /// The parse error, verbatim.
    pub error: String,
}

/// Log a dropped config file and record it as an `InvalidConfig`.
fn record_invalid<R: ConfigResource>(
    path: &Path,
    error: String,
    project_id: Option<String>,
    out: &mut Vec<InvalidConfig>,
) {
    log::warn!(
        "Skipping invalid {} config at {:?}: {}",
        R::ENTITY_TYPE,
        path,
        error
    );
    out.push(InvalidConfig {
        id: crate::config::id_from_path(path).unwrap_or_default(),
        entity_type: R::ENTITY_TYPE.to_string(),
        path: path.to_string_lossy().into_owned(),
        project_id,
        error,
    });
}

pub(crate) fn list_configs<R: ConfigResource>(
    orch: &Orchestrator,
    workspace_id: Option<&str>,
    project_id: Option<&str>,
) -> Result<Vec<R::Config>, String> {
    Ok(list_configs_with_invalid::<R>(orch, workspace_id, project_id)?.0)
}

/// Like [`list_configs`], but also returns every config file that failed to
/// parse. Both arms share one implementation so agents, recipes, and skills
/// surface broken files identically. Dropped files are always logged via
/// [`record_invalid`].
pub(crate) fn list_configs_with_invalid<R: ConfigResource>(
    orch: &Orchestrator,
    workspace_id: Option<&str>,
    project_id: Option<&str>,
) -> Result<(Vec<R::Config>, Vec<InvalidConfig>), String> {
    let mut all_configs = Vec::new();
    let mut invalid = Vec::new();
    let now = now_timestamp();

    if workspace_id.is_none() && project_id.is_none() {
        for result in R::list_files(&orch.config_dir, None)? {
            match result {
                ConfigResult::Ok(file) => {
                    if !R::file_is_project_scoped(&file) {
                        all_configs.push(R::to_config(
                            file,
                            Some("default".to_string()),
                            None,
                            now,
                        ));
                    }
                }
                ConfigResult::Err { path, error } => {
                    record_invalid::<R>(&path, error, None, &mut invalid)
                }
            }
        }

        for (pid, project_path) in orch.all_project_paths()? {
            for result in R::list_files(&orch.config_dir, Some(&project_path))? {
                match result {
                    ConfigResult::Ok(file) => {
                        if R::file_is_project_scoped(&file) {
                            all_configs.push(R::to_config(file, None, Some(pid.clone()), now));
                        }
                    }
                    ConfigResult::Err { path, error } => {
                        // `list_files` returns the project dir AND the workspace
                        // dir, so only record errors actually under this project
                        // to avoid re-recording workspace files once per project.
                        if path.starts_with(&project_path) {
                            record_invalid::<R>(&path, error, Some(pid.clone()), &mut invalid);
                        }
                    }
                }
            }
        }
    } else {
        let project_path: Option<PathBuf> =
            project_id.map(|pid| orch.project_path(pid)).transpose()?;
        for result in R::list_files(&orch.config_dir, project_path.as_deref())? {
            match result {
                ConfigResult::Ok(file) => {
                    let include = match (workspace_id, project_id) {
                        (Some(_), None) => !R::file_is_project_scoped(&file),
                        (None, Some(_)) => R::file_is_project_scoped(&file),
                        _ => true,
                    };

                    if include {
                        let (ws_id, proj_id) = if R::file_is_project_scoped(&file) {
                            (None, project_id.map(|s| s.to_string()))
                        } else {
                            (workspace_id.map(|s| s.to_string()), None)
                        };
                        all_configs.push(R::to_config(file, ws_id, proj_id, now));
                    }
                }
                ConfigResult::Err { path, error } => {
                    // Mirror the Ok scope filter: a file under the project dir is
                    // project-scoped, otherwise it is the workspace copy.
                    let is_project_err = project_path
                        .as_deref()
                        .map(|pp| path.starts_with(pp))
                        .unwrap_or(false);
                    let include = match (workspace_id, project_id) {
                        (Some(_), None) => !is_project_err,
                        (None, Some(_)) => is_project_err,
                        _ => true,
                    };
                    if include {
                        let pid = is_project_err
                            .then(|| project_id.map(|s| s.to_string()))
                            .flatten();
                        record_invalid::<R>(&path, error, pid, &mut invalid);
                    }
                }
            }
        }
    }

    sort_configs::<R>(&mut all_configs);
    invalid.sort_by(|a, b| a.id.cmp(&b.id));
    Ok((all_configs, invalid))
}

pub(crate) fn get_config<R: ConfigResource>(
    orch: &Orchestrator,
    id: &str,
    project_id: Option<&str>,
) -> Result<Option<R::Config>, String> {
    let now = now_timestamp();
    if let Some(pid) = project_id {
        let project_path = orch.project_path(pid)?;
        let file = R::get_file(&orch.config_dir, id, Some(&project_path))?;
        return Ok(file.map(|f| {
            let (ws_id, proj_id) = if R::file_is_project_scoped(&f) {
                (None, Some(pid.to_string()))
            } else {
                (Some("default".to_string()), None)
            };
            R::to_config(f, ws_id, proj_id, now)
        }));
    }

    if let Some(file) = R::get_file(&orch.config_dir, id, None)? {
        if !R::file_is_project_scoped(&file) {
            return Ok(Some(R::to_config(
                file,
                Some("default".to_string()),
                None,
                now,
            )));
        }
    }

    if R::GET_SEARCHES_ALL_PROJECTS {
        for (pid, project_path) in orch.all_project_paths()? {
            if let Some(file) = R::get_file(&orch.config_dir, id, Some(&project_path))? {
                if R::file_is_project_scoped(&file) {
                    return Ok(Some(R::to_config(file, None, Some(pid), now)));
                }
            }
        }
    }

    Ok(None)
}

pub(crate) fn create_config<R: ConfigResource>(
    orch: &Orchestrator,
    input: R::CreateInput,
) -> Result<R::Config, String> {
    let (workspace_id, project_id) = R::create_input_scopes(&input);
    if workspace_id.is_none() == project_id.is_none() {
        return Err("Exactly one of workspace_id or project_id must be set".to_string());
    }

    let project_path: Option<PathBuf> = project_id
        .as_deref()
        .map(|pid| orch.project_path(pid))
        .transpose()?;
    let is_project_scoped = project_path.is_some();
    let id = R::create_id(&input);
    let now = now_timestamp();
    let file = R::build_create_file(input, id.clone(), is_project_scoped, now);

    let dest_path = R::save_file(&orch.config_dir, &file, project_path.as_deref())?;
    crate::config::commit_config_paths(
        &R::stage_paths(&dest_path),
        &config_commit_message::<R>("create", &id),
    );

    emit_config_change::<R>(orch, "created", &id);
    emit_db_change::<R>(orch, "insert");

    Ok(R::to_config(file, workspace_id, project_id, now))
}

pub(crate) fn update_config<R: ConfigResource>(
    orch: &Orchestrator,
    id: &str,
    input: R::UpdateInput,
    target_project_id: Option<&str>,
) -> Result<R::Config, String> {
    let existing = find_for_update::<R>(orch, id, target_project_id)?;
    let (new_is_project_scoped, new_project_id) =
        R::update_scope(&existing, &input, target_project_id);
    let scope_changing = new_is_project_scoped != R::file_is_project_scoped(&existing);

    let new_project_path: Option<PathBuf> = if new_is_project_scoped {
        if let Some(ref pid) = new_project_id {
            Some(orch.project_path(pid)?)
        } else if scope_changing {
            return Err("Project ID required when changing to project scope".to_string());
        } else {
            None
        }
    } else {
        None
    };

    let now = now_timestamp();
    let file = R::build_update_file(
        &existing,
        input,
        id,
        new_is_project_scoped,
        scope_changing,
        now,
    );
    let dest_path = R::save_file(&orch.config_dir, &file, new_project_path.as_deref())?;

    let mut commit_targets = R::stage_paths(&dest_path);
    if scope_changing {
        R::cleanup_after_scope_change(&existing, &dest_path);
        // The old-scope file was just removed; stage that path too so its
        // deletion commits in whichever repo (workspace or project) held it.
        commit_targets.extend(R::stage_paths(&R::primary_path(&existing)));
    }
    crate::config::commit_config_paths(&commit_targets, &config_commit_message::<R>("update", id));

    emit_config_change::<R>(orch, "modified", id);
    emit_db_change::<R>(orch, "update");

    let workspace_id = if new_is_project_scoped {
        None
    } else {
        Some("default".to_string())
    };
    Ok(R::to_config(file, workspace_id, new_project_id, now))
}

pub(crate) fn delete_config<R: ConfigResource>(
    orch: &Orchestrator,
    id: &str,
    project_id: Option<&str>,
) -> Result<(), String> {
    let project_path: Option<PathBuf> = project_id.map(|pid| orch.project_path(pid)).transpose()?;

    // Capture the on-disk path before deleting so we can stage exactly it.
    let existing = R::get_file(&orch.config_dir, id, project_path.as_deref())?;
    R::delete_file(&orch.config_dir, id, project_path.as_deref())?;
    if let Some(file) = &existing {
        crate::config::commit_config_paths(
            &R::stage_paths(&R::primary_path(file)),
            &config_commit_message::<R>("delete", id),
        );
    }

    emit_config_change::<R>(orch, "removed", id);
    emit_db_change::<R>(orch, "delete");

    Ok(())
}

pub(crate) fn create_override<R: ConfigResource>(
    orch: &Orchestrator,
    id: &str,
    project_id: &str,
) -> Result<R::Config, String> {
    copy_to_scope::<R>(orch, id, Some(project_id), "this project")
}

pub(crate) fn promote_to_workspace<R: ConfigResource>(
    orch: &Orchestrator,
    id: &str,
    source_project_id: &str,
) -> Result<R::Config, String> {
    let source_project_path = orch.project_path(source_project_id)?;
    let source =
        R::get_file(&orch.config_dir, id, Some(&source_project_path))?.ok_or_else(|| {
            format!(
                "{} not found in project: {}",
                title_case(R::ENTITY_TYPE),
                id
            )
        })?;

    if !R::file_is_project_scoped(&source) {
        return Err(format!(
            "{} is not project-scoped",
            title_case(R::ENTITY_TYPE)
        ));
    }

    copy_source_to_scope::<R>(orch, source, id, None, "workspace scope")
}

pub(crate) fn list_for_context<R: ConfigResource>(
    orch: &Orchestrator,
    project_id: &str,
) -> Result<Vec<R::Config>, String> {
    let project_path = orch.project_path(project_id)?;
    let mut configs_map: HashMap<String, R::Config> = HashMap::new();
    let now = now_timestamp();

    for result in R::list_files(&orch.config_dir, None)? {
        match result {
            ConfigResult::Ok(file) => {
                if !R::file_is_project_scoped(&file) {
                    let config = R::to_config(file, Some("default".to_string()), None, now);
                    configs_map.insert(R::config_id(&config), config);
                }
            }
            ConfigResult::Err { path, error } => {
                log::warn!(
                    "Skipping invalid {} config at {:?}: {}",
                    R::ENTITY_TYPE,
                    path,
                    error
                );
            }
        }
    }

    for result in R::list_files(&orch.config_dir, Some(&project_path))? {
        match result {
            ConfigResult::Ok(file) => {
                if R::file_is_project_scoped(&file) {
                    let config = R::to_config(file, None, Some(project_id.to_string()), now);
                    configs_map.insert(R::config_id(&config), config);
                }
            }
            ConfigResult::Err { path, error } => {
                if path.starts_with(&project_path) {
                    log::warn!(
                        "Skipping invalid {} config at {:?}: {}",
                        R::ENTITY_TYPE,
                        path,
                        error
                    );
                }
            }
        }
    }

    let mut configs: Vec<R::Config> = configs_map.into_values().collect();
    sort_configs::<R>(&mut configs);
    Ok(configs)
}

fn find_for_update<R: ConfigResource>(
    orch: &Orchestrator,
    id: &str,
    target_project_id: Option<&str>,
) -> Result<R::File, String> {
    if let Some(pid) = target_project_id {
        let project_path = orch.project_path(pid)?;
        return R::get_file(&orch.config_dir, id, Some(&project_path))?
            .ok_or_else(|| format!("{} not found: {}", title_case(R::ENTITY_TYPE), id));
    }

    if let Some(file) = R::get_file(&orch.config_dir, id, None)? {
        return Ok(file);
    }

    if R::UPDATE_SEARCHES_ALL_PROJECTS {
        for (_pid, project_path) in orch.all_project_paths()? {
            if let Some(file) = R::get_file(&orch.config_dir, id, Some(&project_path))? {
                return Ok(file);
            }
        }
    }

    Err(format!("{} not found: {}", title_case(R::ENTITY_TYPE), id))
}

fn copy_to_scope<R: ConfigResource>(
    orch: &Orchestrator,
    id: &str,
    project_id: Option<&str>,
    duplicate_scope_label: &str,
) -> Result<R::Config, String> {
    let source_project_path = project_id.map(|pid| orch.project_path(pid)).transpose()?;
    let source = R::get_file(&orch.config_dir, id, source_project_path.as_deref())?
        .ok_or_else(|| format!("{} not found: {}", title_case(R::ENTITY_TYPE), id))?;
    copy_source_to_scope::<R>(orch, source, id, project_id, duplicate_scope_label)
}

fn copy_source_to_scope<R: ConfigResource>(
    orch: &Orchestrator,
    source: R::File,
    id: &str,
    project_id: Option<&str>,
    duplicate_scope_label: &str,
) -> Result<R::Config, String> {
    let (target_root, workspace_id, project_id_string, is_project_scoped) = match project_id {
        Some(pid) => (orch.project_path(pid)?, None, Some(pid.to_string()), true),
        None => (
            orch.config_dir.clone(),
            Some("default".to_string()),
            None,
            false,
        ),
    };

    if R::target_exists(&target_root, id, is_project_scoped) {
        return Err(format!(
            "{} '{}' already exists at {}",
            title_case(R::ENTITY_TYPE),
            id,
            duplicate_scope_label
        ));
    }

    let now = now_timestamp();
    let file = R::build_scoped_copy(
        &source,
        id,
        is_project_scoped,
        workspace_id.clone(),
        project_id_string.clone(),
        now,
    );
    let project_path = is_project_scoped.then_some(target_root.as_path());
    let dest_path = R::save_file(&orch.config_dir, &file, project_path)?;
    R::post_save_copy(&source, &dest_path)?;
    crate::config::commit_config_paths(
        &R::stage_paths(&dest_path),
        &config_commit_message::<R>("create", id),
    );

    emit_config_change::<R>(orch, "created", id);
    emit_db_change::<R>(orch, "insert");

    Ok(R::to_config(file, workspace_id, project_id_string, now))
}

pub(crate) fn emit_config_change<R: ConfigResource>(orch: &Orchestrator, action: &str, id: &str) {
    let _ = orch.services.emitter.emit(
        "config-changed",
        serde_json::json!({"entity_type": R::ENTITY_TYPE, "action": action, "id": id}),
    );
}

pub(crate) fn emit_db_change<R: ConfigResource>(orch: &Orchestrator, action: &str) {
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": R::TABLE, "action": action}),
    );
}

fn now_timestamp() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Build a config commit subject like `cairn: update agent planner`.
fn config_commit_message<R: ConfigResource>(action: &str, id: &str) -> String {
    format!("cairn: {action} {} {}", R::ENTITY_TYPE, id)
}

fn sort_configs<R: ConfigResource>(configs: &mut [R::Config]) {
    configs.sort_by(|a, b| R::config_name(a).cmp(R::config_name(b)));
}

fn title_case(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::all_project_paths_from_db;
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn all_project_paths_excludes_workspace_project() {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("cairn.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES ('workspace', 'default', 'Workspace', 'CANON', '/tmp/cairn', 1, 1, 1)",
                    (),
                ).await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES ('project', 'default', 'Project', 'PROJ', '/tmp/project', 1, 1, 0)",
                    (),
                ).await?;
                Ok(())
            })
        }).await.unwrap();

        let paths = all_project_paths_from_db(Arc::new(db)).await.unwrap();
        assert_eq!(
            paths,
            vec![(
                "project".to_string(),
                std::path::PathBuf::from("/tmp/project")
            )]
        );
    }

    /// End-to-end through the public Settings-UI path: a workspace agent
    /// create/update/delete each commit to the workspace repo, staging only the
    /// agent file and leaving unrelated dirty state untouched.
    #[tokio::test]
    async fn workspace_agent_lifecycle_commits_only_touched_file() {
        use crate::db::DbState;
        use crate::models::{CreateAgentConfig, UpdateAgentConfig};
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
        use std::path::Path;
        use std::sync::Arc;

        fn git(args: &[&str], dir: &Path) -> std::process::Output {
            crate::env::git()
                .args(args)
                .current_dir(dir)
                .output()
                .unwrap()
        }
        fn head_subject(dir: &Path) -> String {
            String::from_utf8_lossy(&git(&["log", "-1", "--pretty=%s"], dir).stdout)
                .trim()
                .to_string()
        }
        fn porcelain(dir: &Path) -> String {
            String::from_utf8_lossy(&git(&["status", "--porcelain"], dir).stdout)
                .trim()
                .to_string()
        }

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        // The workspace config dir is a git repo, as CAIRN-1468 makes ~/.cairn.
        assert!(git(&["init", "-q"], &config_dir).status.success());
        // Unrelated dirty state in the same repo must survive every commit.
        std::fs::write(config_dir.join("settings.yaml"), "activeBackend: x\n").unwrap();

        let db = LocalDb::open(root.join("test.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(db_state, services, config_dir.clone()).build();

        orch.create_agent_config(CreateAgentConfig {
            id: "planner".to_string(),
            name: "Planner".to_string(),
            description: "Plans things".to_string(),
            prompt: "Plan.".to_string(),
            tools: vec!["Read".to_string(), "Grep".to_string()],
            tier: None,
            workspace_id: Some("default".to_string()),
            project_id: None,
            disallowed_tools: None,
            skills: None,
            fence: None,
            sandbox: None,
            on_escape: None,
            backend_preference: None,
        })
        .unwrap();
        assert_eq!(head_subject(&config_dir), "cairn: create agent planner");
        let status = porcelain(&config_dir);
        assert!(
            status.contains("settings.yaml"),
            "unrelated settings.yaml must stay dirty: {status:?}"
        );
        assert!(
            !status.contains("planner.md"),
            "the new agent file must be committed: {status:?}"
        );

        orch.update_agent_config(
            "planner",
            UpdateAgentConfig {
                description: Some("Plans better".to_string()),
                ..Default::default()
            },
            None,
        )
        .unwrap();
        assert_eq!(head_subject(&config_dir), "cairn: update agent planner");
        assert!(porcelain(&config_dir).contains("settings.yaml"));

        orch.delete_agent_config("planner", None).unwrap();
        assert_eq!(head_subject(&config_dir), "cairn: delete agent planner");
        let status = porcelain(&config_dir);
        assert!(
            status.contains("settings.yaml") && !status.contains("planner.md"),
            "delete should commit only the agent file: {status:?}"
        );
    }
}
