//! Shared orchestration for workspace/project-scoped config resources.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::config::ConfigResult;
use crate::storage::{run_db_blocking, RowExt};

use super::Orchestrator;

async fn project_paths_from_db(
    db: std::sync::Arc<crate::storage::LocalDb>,
) -> Result<Vec<(String, String, PathBuf)>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, name, repo_path
                     FROM projects
                     WHERE COALESCE(is_workspace, 0) = 0
                     ORDER BY name ASC",
                    (),
                )
                .await?;
            let mut projects = Vec::new();
            while let Some(row) = rows.next().await? {
                projects.push((row.text(0)?, row.text(1)?, PathBuf::from(row.text(2)?)));
            }
            Ok(projects)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

impl Orchestrator {
    /// List all project (id, repo_path) pairs across every open database — the
    /// private DB plus each open team replica — so a team project's recipes,
    /// agents, and skills are listed. A team project's `projects` row lives in
    /// its team replica, not the private DB (CAIRN-2126/2129 routing), so a
    /// private-only read silently dropped team projects (CAIRN-2194). Deduped by
    /// id and name-sorted to match the project-list ordering.
    pub(crate) fn all_project_paths(&self) -> Result<Vec<(String, PathBuf)>, String> {
        let dbs = self.db.clone();
        run_db_blocking(move || async move {
            let mut seen = HashSet::new();
            let mut rows: Vec<(String, String, PathBuf)> = Vec::new();
            for db in dbs.all_dbs().await {
                for (id, name, path) in project_paths_from_db(db).await? {
                    if seen.insert(id.clone()) {
                        rows.push((id, name, path));
                    }
                }
            }
            rows.sort_by(|a, b| a.1.cmp(&b.1));
            Ok(rows
                .into_iter()
                .map(|(id, _name, path)| (id, path))
                .collect())
        })
    }

    /// Resolve project_id → repo path across every open database. A team
    /// project's `projects` row lives in its team replica, not the private DB,
    /// so this routes through `owning_db` to find whichever open database holds
    /// the row (CAIRN-2194); a private-only read returned `Project not found`
    /// for a team project, which emptied the recipe/agent/skill pickers.
    pub(crate) fn project_path(&self, project_id: &str) -> Result<PathBuf, String> {
        let dbs = self.db.clone();
        let project_id = project_id.to_string();
        let missing_id = project_id.clone();
        let path = run_db_blocking(move || async move {
            let db = crate::projects::crud::owning_db(&dbs, &project_id)
                .await
                .map_err(|e| e.to_string())?;
            crate::projects::crud::repo_path(&db, &project_id)
                .await
                .map_err(|e| e.to_string())
        })?;

        path.map(PathBuf::from)
            .ok_or_else(|| format!("Project not found: {}", missing_id))
    }

    /// Resolve the machine-local clone of a team project's WORKSPACE config home,
    /// the middle layer between the project and the personal workspace
    /// (`project > team workspace > personal`). Returns `(workspace_project_id,
    /// clone_path)` when `project_id` belongs to a team whose workspace row is
    /// seeded AND materialized on this machine.
    ///
    /// Graceful degrade is the contract: a local project, an unparse-able id, a
    /// team whose replica is not open, a team with no seeded workspace row, or a
    /// workspace not yet cloned on this machine all yield `None` — never an error.
    /// One `?` here must not empty every picker (the CAIRN-2194 failure mode), so
    /// the middle layer is simply absent until it exists. This is READ-path
    /// resolution; the WRITE path resolves its root fail-closed instead.
    fn team_workspace_path(&self, project_id: &str) -> Result<Option<(String, PathBuf)>, String> {
        let dbs = self.db.clone();
        let project_id = project_id.to_string();
        run_db_blocking(move || async move {
            let team = match cairn_common::ids::parse_route_scope(&project_id) {
                Ok(cairn_common::ids::RouteScope::Team(team)) => team,
                // Local projects and any malformed id get no middle layer.
                _ => return Ok(None),
            };
            let Some(team_db) = dbs.team_db(&team).await else {
                return Ok(None); // replica not open -> graceful degrade
            };
            let Some(project) = crate::projects::crud::team_workspace_project(&team_db)
                .await
                .map_err(|e| e.to_string())?
            else {
                return Ok(None); // team has no workspace row yet
            };
            let local = crate::projects::crud::resolve_local_repo_path(
                &dbs,
                &project.id,
                &project.key,
                &project.repo_path,
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok(local
                .filter(|p| !p.is_empty())
                .map(|p| (project.id, PathBuf::from(p))))
        })
    }

    /// Materialize this team's WORKSPACE config home on THIS machine and record
    /// its clone path so [`Orchestrator::team_workspace_path`] resolves it.
    ///
    /// The activation counterpart to the CAIRN-2579 read-path foundation:
    /// `reconcile_team_routes` seeds the team's `is_workspace` `projects` row and
    /// a path-LESS `project_routes` stub, so until this runs the middle config
    /// layer is always absent (the route has no `local_repo_path`). This seeds a
    /// remoteless git repo at `<config_dir>/teams/<team>/workspace` — an exact
    /// twin of `~/.cairn` via [`sync_workspace_bundle`] — then fills that route's
    /// clone path under the team-scoped [`team_workspace_key`]. Idempotent: a
    /// re-run only re-syncs the bundle and re-asserts the same route.
    ///
    /// `resource_dir` is the app bundle's resource directory, the same bundle
    /// source the personal `sync_workspace_bundle` copies from. The DB-owning
    /// runner has no `resource_dir` of its own, so the desktop — which alone
    /// resolves it — supplies it (a real local path both same-machine processes
    /// read). Requires the team replica to be open (the `is_workspace` row lives
    /// there); returns `Err` otherwise, unlike the graceful-degrade read path.
    fn ensure_team_workspace(&self, resource_dir: &Path, team_id: &str) -> Result<PathBuf, String> {
        let ws_path = self
            .config_dir
            .join("teams")
            .join(team_id)
            .join("workspace");
        let key = crate::projects::crud::team_workspace_key(team_id);

        // Seed the remoteless bundle twin BEFORE recording the route, so a
        // resolved clone path always points at an initialized repo.
        crate::workspace::bundle::sync_workspace_bundle(
            &crate::services::RealGitClient,
            &crate::services::RealFileSystem,
            resource_dir,
            &ws_path,
            env!("CARGO_PKG_VERSION"),
        )?;

        let dbs = self.db.clone();
        let team_id = team_id.to_string();
        let key_upper = key.to_uppercase();
        let path_str = ws_path.to_string_lossy().to_string();
        run_db_blocking(move || async move {
            let Some(team_db) = dbs.team_db(&team_id).await else {
                return Err(format!("team `{team_id}` replica is not open"));
            };
            // The `is_workspace` row is seeded by `reconcile_team_routes` (which
            // runs on every team open/pull); materialization only records the
            // machine-local clone path against it. Require it to exist rather
            // than re-seeding here, keeping seeding single-owner in reconcile.
            if crate::projects::crud::team_workspace_project(&team_db)
                .await
                .map_err(|e| e.to_string())?
                .is_none()
            {
                return Err(format!(
                    "team `{team_id}` has no seeded workspace row yet (reconcile pending)"
                ));
            }
            // Fill the clone path on the route stub `reconcile_team_routes`
            // already created for this workspace key. Fail loudly when the stub
            // is absent (reconcile has not populated routes yet) rather than
            // leaving the middle layer silently absent after a "successful" run.
            let affected = dbs
                .local
                .execute(
                    "UPDATE project_routes SET local_repo_path = ?1 WHERE project_key = ?2",
                    (path_str.clone(), key_upper.clone()),
                )
                .await
                .map_err(|e| e.to_string())?;
            if affected == 0 {
                return Err(format!(
                    "no project_routes stub for team workspace `{key_upper}` (reconcile pending)"
                ));
            }
            Ok(())
        })?;

        Ok(ws_path)
    }

    /// Materialize the team workspace for EVERY currently-open team, best-effort.
    ///
    /// The startup driver behind [`Orchestrator::ensure_team_workspace`]: the
    /// DB-owning runner has no `resource_dir`, so the desktop (which alone
    /// resolves it) calls this once its side has resolved the app bundle path.
    /// Idempotent and non-fatal per team — a team whose replica just closed, or
    /// whose reconcile has not yet seeded the workspace row, is logged and
    /// skipped rather than failing the whole pass. Returns each team's outcome so
    /// the caller can log a summary.
    pub fn materialize_open_team_workspaces(
        &self,
        resource_dir: &Path,
    ) -> Vec<(String, Result<PathBuf, String>)> {
        let dbs = self.db.clone();
        let team_ids =
            run_db_blocking(move || async move { Ok::<_, String>(dbs.open_team_ids().await) })
                .unwrap_or_default();
        team_ids
            .into_iter()
            .map(|team_id| {
                let outcome = self.ensure_team_workspace(resource_dir, &team_id);
                if let Err(error) = &outcome {
                    log::warn!("team-workspace materialization for `{team_id}` skipped: {error}");
                }
                (team_id, outcome)
            })
            .collect()
    }

    /// Resolve the on-disk root a workspace-scoped config WRITE targets.
    ///
    /// `"default"` (the personal workspace) resolves to `config_dir` (`~/.cairn`).
    /// A team-workspace project id resolves to that team's machine-local clone,
    /// FAIL-CLOSED: unlike the graceful-degrade read path
    /// ([`Orchestrator::team_workspace_path`]), an unparse-able id, an unopened
    /// replica, a non-workspace project, or a workspace not yet cloned all error
    /// rather than silently falling back to `config_dir` — a write must never leak
    /// team config into the personal home or vice versa.
    pub(crate) fn workspace_write_root(&self, workspace_id: &str) -> Result<PathBuf, String> {
        if workspace_id == "default" {
            return Ok(self.config_dir.clone());
        }
        let dbs = self.db.clone();
        let workspace_id = workspace_id.to_string();
        run_db_blocking(move || async move {
            let team = match cairn_common::ids::parse_route_scope(&workspace_id) {
                Ok(cairn_common::ids::RouteScope::Team(team)) => team,
                _ => {
                    return Err(format!(
                    "not a team-workspace id (expected \"default\" or a team id): {workspace_id}"
                ))
                }
            };
            let team_db = dbs
                .team_db(&team)
                .await
                .ok_or_else(|| format!("team `{team}` replica is not open"))?;
            let project = crate::projects::crud::get_db(&team_db, &workspace_id)
                .await
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("workspace project not found: {workspace_id}"))?;
            if project.is_workspace == 0 {
                return Err(format!("project {workspace_id} is not a team workspace"));
            }
            let local = crate::projects::crud::resolve_local_repo_path(
                &dbs,
                &project.id,
                &project.key,
                &project.repo_path,
            )
            .await
            .map_err(|e| e.to_string())?
            .filter(|p| !p.is_empty())
            .ok_or_else(|| format!("team workspace not cloned on this machine: {workspace_id}"))?;
            Ok(PathBuf::from(local))
        })
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
        entity_type: Option<&str>,
    ) -> Result<Vec<InvalidConfig>, String> {
        let mut invalid = Vec::new();
        if entity_type.is_none() || entity_type == Some("agent") {
            invalid.extend(
                list_configs_with_invalid::<super::agents::AgentResource>(
                    self,
                    workspace_id,
                    project_id,
                )?
                .1,
            );
        }
        if entity_type.is_none() || entity_type == Some("recipe") {
            invalid.extend(
                list_configs_with_invalid::<super::recipes::RecipeResource>(
                    self,
                    workspace_id,
                    project_id,
                )?
                .1,
            );
        }
        if entity_type.is_none() || entity_type == Some("skill") {
            invalid.extend(
                list_configs_with_invalid::<super::skills::SkillResource>(
                    self,
                    workspace_id,
                    project_id,
                )?
                .1,
            );
        }
        if entity_type.is_none() || entity_type == Some("workflow") {
            invalid.extend(self.list_invalid_workflow_configs(workspace_id, project_id)?);
        }
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
    fn file_bundles(file: &Self::File) -> &[String];
    fn package_kind() -> crate::config::contextual_packages::ContextualPackageKind;
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
    pub(crate) id: String,
    /// One of `agent`, `recipe`, `skill`.
    pub(crate) entity_type: String,
    /// Absolute path to the offending file.
    pub(crate) path: String,
    /// Project this file belongs to, or `None` for a workspace-scoped file.
    pub(crate) project_id: Option<String>,
    /// The parse error, verbatim.
    pub(crate) error: String,
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
fn list_configs_with_invalid<R: ConfigResource>(
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
        // Resolve the workspace list base with the same fail-closed semantics as
        // the write path: a team-workspace `workspace_id` lists from that team's
        // machine-local clone, not the personal `~/.cairn`, so team-authored
        // configs surface under the Team Workspace scope and personal configs are
        // never mislabeled as team-scoped. `"default"` and the project-only
        // filter keep `config_dir` (workspace_write_root("default") == config_dir).
        let list_root = match (workspace_id, project_id) {
            (Some(ws), None) => orch.workspace_write_root(ws)?,
            _ => orch.config_dir.clone(),
        };
        for result in R::list_files(&list_root, project_path.as_deref())? {
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
        // Resolve project > team workspace > personal, first hit wins. `get_file`
        // with a project path already walks project-then-personal, so a
        // project-scoped hit returns outright; otherwise the team-workspace layer
        // is consulted before falling back to the personal copy.
        if let Some(f) = R::get_file(&orch.config_dir, id, Some(&project_path))? {
            if R::file_is_project_scoped(&f) {
                return Ok(Some(R::to_config(f, None, Some(pid.to_string()), now)));
            }
        }
        if let Some((ws_id, ws_root)) = &orch.team_workspace_path(pid)? {
            if let Some(f) = R::get_file(ws_root, id, None)? {
                if !R::file_is_project_scoped(&f) {
                    return Ok(Some(R::to_config(f, Some(ws_id.clone()), None, now)));
                }
            }
        }
        if let Some(f) = R::get_file(&orch.config_dir, id, None)? {
            if !R::file_is_project_scoped(&f) {
                return Ok(Some(R::to_config(
                    f,
                    Some("default".to_string()),
                    None,
                    now,
                )));
            }
        }
        return Ok(None);
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
    // The workspace root a workspace-scoped create writes into: personal
    // `~/.cairn` for `"default"`, or a team's machine-local clone for a
    // team-workspace id (fail-closed). Project-scoped creates keep `config_dir`
    // as the base — `project_path` drives their on-disk location — so the
    // resolved root only diverges for a team-workspace author.
    let write_root: PathBuf = match &workspace_id {
        Some(ws) => orch.workspace_write_root(ws)?,
        None => orch.config_dir.clone(),
    };
    let id = R::create_id(&input);
    let now = now_timestamp();
    let file = R::build_create_file(input, id.clone(), is_project_scoped, now);

    let dest_path = R::save_file(&write_root, &file, project_path.as_deref())?;
    // Offer the resolved write root as the push target; commit_and_maybe_push
    // only pushes when that repo actually received one of these commits AND has a
    // remote, so a project-scoped create and a personal-workspace create stay
    // commit-only while a team-workspace create pushes the shared config repo.
    crate::config::commit_and_maybe_push(
        &R::stage_paths(&dest_path),
        &config_commit_message::<R>("create", &id),
        Some(write_root.as_path()),
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
    workspace_id: Option<&str>,
) -> Result<R::Config, String> {
    let existing = find_for_update::<R>(orch, id, target_project_id, workspace_id)?;
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

    // The workspace root a workspace-scoped update writes into. Project-scoped
    // updates keep `config_dir` as the base (`new_project_path` drives their
    // location); a team-workspace update resolves the team clone, fail-closed.
    let write_base: PathBuf = if new_is_project_scoped {
        orch.config_dir.clone()
    } else {
        match workspace_id {
            Some(ws) => orch.workspace_write_root(ws)?,
            None => orch.config_dir.clone(),
        }
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
    let dest_path = R::save_file(&write_base, &file, new_project_path.as_deref())?;

    let mut commit_targets = R::stage_paths(&dest_path);
    if scope_changing {
        R::cleanup_after_scope_change(&existing, &dest_path);
        // The old-scope file was just removed; stage that path too so its
        // deletion commits in whichever repo (workspace or project) held it.
        commit_targets.extend(R::stage_paths(&R::primary_path(&existing)));
    }
    // Offer the resolved write root as the push target: a team-workspace update
    // pushes the shared config repo, while personal-workspace and project updates
    // stay commit-only. commit_and_maybe_push pushes only if that repo actually
    // committed AND has a remote.
    crate::config::commit_and_maybe_push(
        &commit_targets,
        &config_commit_message::<R>("update", id),
        Some(write_base.as_path()),
    );

    emit_config_change::<R>(orch, "modified", id);
    emit_db_change::<R>(orch, "update");

    let result_workspace_id = if new_is_project_scoped {
        None
    } else {
        Some(workspace_id.unwrap_or("default").to_string())
    };
    Ok(R::to_config(file, result_workspace_id, new_project_id, now))
}

pub(crate) fn delete_config<R: ConfigResource>(
    orch: &Orchestrator,
    id: &str,
    project_id: Option<&str>,
    workspace_id: Option<&str>,
) -> Result<(), String> {
    let project_path: Option<PathBuf> = project_id.map(|pid| orch.project_path(pid)).transpose()?;

    // Capture the on-disk path before deleting so we can stage exactly it. A
    // project-scoped delete reads/writes under `config_dir` + `project_path`; a
    // workspace-scoped delete targets the resolved workspace root (personal
    // `~/.cairn` or a team clone), which is also its commit/push root.
    let (delete_root, delete_project) = match &project_path {
        Some(pp) => (orch.config_dir.clone(), Some(pp.as_path())),
        None => {
            let ws_root = match workspace_id {
                Some(ws) => orch.workspace_write_root(ws)?,
                None => orch.config_dir.clone(),
            };
            (ws_root, None)
        }
    };
    let existing = R::get_file(&delete_root, id, delete_project)?;
    R::delete_file(&delete_root, id, delete_project)?;
    if let Some(file) = &existing {
        crate::config::commit_and_maybe_push(
            &R::stage_paths(&R::primary_path(file)),
            &config_commit_message::<R>("delete", id),
            Some(delete_root.as_path()),
        );
    }

    emit_config_change::<R>(orch, "removed", id);
    emit_db_change::<R>(orch, "delete");

    Ok(())
}

pub(crate) fn migrate_contextual_disables_blocking(
    orch: &Orchestrator,
    project_id: &str,
    project_path: &Path,
) -> Result<usize, String> {
    let db = orch.db.local.clone();
    let project_id = project_id.to_string();
    let project_path = project_path.to_path_buf();
    run_db_blocking(move || async move {
        crate::config_disables::migrate_contextual_disables(&db, &project_id, &project_path).await
    })
}

pub(crate) fn list_for_context<R: ConfigResource>(
    orch: &Orchestrator,
    project_id: &str,
) -> Result<Vec<R::Config>, String> {
    let project_path = orch.project_path(project_id)?;
    migrate_contextual_disables_blocking(orch, project_id, &project_path)?;
    let team_workspace = orch.team_workspace_path(project_id)?;
    let mut configs_map: HashMap<String, (R::Config, Vec<String>)> = HashMap::new();
    let now = now_timestamp();

    for result in R::list_files(&orch.config_dir, None)? {
        match result {
            ConfigResult::Ok(file) => {
                if !R::file_is_project_scoped(&file) {
                    let bundles = R::file_bundles(&file).to_vec();
                    let config = R::to_config(file, Some("default".to_string()), None, now);
                    configs_map.insert(R::config_id(&config), (config, bundles));
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

    // Middle layer: the team workspace shadows personal and is shadowed by the
    // project below (last-wins by config id yields project > team ws > personal).
    if let Some((ws_id, ws_root)) = &team_workspace {
        for result in R::list_files(ws_root, None)? {
            match result {
                ConfigResult::Ok(file) => {
                    if !R::file_is_project_scoped(&file) {
                        let bundles = R::file_bundles(&file).to_vec();
                        let config = R::to_config(file, Some(ws_id.clone()), None, now);
                        configs_map.insert(R::config_id(&config), (config, bundles));
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
    }

    for result in R::list_files(&orch.config_dir, Some(&project_path))? {
        match result {
            ConfigResult::Ok(file) => {
                if R::file_is_project_scoped(&file) {
                    let bundles = R::file_bundles(&file).to_vec();
                    let config = R::to_config(file, None, Some(project_id.to_string()), now);
                    configs_map.insert(R::config_id(&config), (config, bundles));
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

    let policy = crate::config::contextual_packages::load_contextual_packages(Some(&project_path));
    let mut configs: Vec<R::Config> = configs_map
        .into_values()
        .filter_map(|(config, bundles)| {
            policy
                .is_selected(R::package_kind(), &R::config_id(&config), &bundles)
                .then_some(config)
        })
        .collect();
    sort_configs::<R>(&mut configs);
    Ok(configs)
}

fn find_for_update<R: ConfigResource>(
    orch: &Orchestrator,
    id: &str,
    target_project_id: Option<&str>,
    workspace_id: Option<&str>,
) -> Result<R::File, String> {
    if let Some(pid) = target_project_id {
        let project_path = orch.project_path(pid)?;
        return R::get_file(&orch.config_dir, id, Some(&project_path))?
            .ok_or_else(|| format!("{} not found: {}", title_case(R::ENTITY_TYPE), id));
    }

    // Workspace-scoped: read from the resolved workspace root — personal
    // `~/.cairn` for `"default"`/`None`, or a team's machine-local clone for a
    // team-workspace id — so a team-authored config is found for editing.
    let ws_root = match workspace_id {
        Some(ws) => orch.workspace_write_root(ws)?,
        None => orch.config_dir.clone(),
    };
    if let Some(file) = R::get_file(&ws_root, id, None)? {
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

/// Copy any source artifact (from any scope) into a target scope under a chosen
/// id. Same id as an inherited workspace artifact + project target shadows it;
/// a new id is additive. Hard copy: no parent link, no propagation.
///
/// This is the single canonical scope-mutation primitive: a same-id copy into a
/// project shadows the inherited workspace artifact, a same-id copy into the
/// workspace promotes a project-only artifact, and a new id is an additive copy.
pub(crate) fn copy_config<R: ConfigResource>(
    orch: &Orchestrator,
    source_id: &str,
    source_project_id: Option<&str>,
    target_id: &str,
    target_project_id: Option<&str>,
    target_workspace_id: Option<&str>,
) -> Result<R::Config, String> {
    let source_project_path: Option<PathBuf> = source_project_id
        .map(|pid| orch.project_path(pid))
        .transpose()?;
    // get_file resolves project-first-then-workspace, so an inherited workspace
    // source is found when the source project hasn't redefined it.
    let source = R::get_file(&orch.config_dir, source_id, source_project_path.as_deref())?
        .ok_or_else(|| format!("{} not found: {}", title_case(R::ENTITY_TYPE), source_id))?;

    // Resolve the target's on-disk base and scope. A project target writes under
    // `config_dir` + the project path; a workspace target writes into the
    // resolved workspace root — personal `~/.cairn` for `"default"`/`None` or a
    // team's machine-local clone (fail-closed) for a team-workspace id.
    let (save_base, target_project_path, workspace_id, project_id_string, is_project_scoped) =
        match target_project_id {
            Some(pid) => (
                orch.config_dir.clone(),
                Some(orch.project_path(pid)?),
                None,
                Some(pid.to_string()),
                true,
            ),
            None => {
                let ws = target_workspace_id.unwrap_or("default");
                (
                    orch.workspace_write_root(ws)?,
                    None,
                    Some(ws.to_string()),
                    None,
                    false,
                )
            }
        };

    // The existence check root: the project repo for a project target, else the
    // workspace root itself.
    let exists_root = target_project_path.as_deref().unwrap_or(&save_base);
    if R::target_exists(exists_root, target_id, is_project_scoped) {
        return Err(format!(
            "{} '{}' already exists at {}",
            title_case(R::ENTITY_TYPE),
            target_id,
            if is_project_scoped {
                "this project"
            } else {
                "workspace scope"
            }
        ));
    }

    let now = now_timestamp();
    let file = R::build_scoped_copy(
        &source,
        target_id,
        is_project_scoped,
        workspace_id.clone(),
        project_id_string.clone(),
        now,
    );
    let dest_path = R::save_file(&save_base, &file, target_project_path.as_deref())?;
    R::post_save_copy(&source, &dest_path)?;
    crate::config::commit_and_maybe_push(
        &R::stage_paths(&dest_path),
        &config_commit_message::<R>("create", target_id),
        Some(save_base.as_path()),
    );

    emit_config_change::<R>(orch, "created", target_id);
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
    use super::project_paths_from_db;
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn project_paths_from_db_excludes_workspace_project() {
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

        let paths: Vec<(String, std::path::PathBuf)> = project_paths_from_db(Arc::new(db))
            .await
            .unwrap()
            .into_iter()
            .map(|(id, _name, path)| (id, path))
            .collect();
        assert_eq!(
            paths,
            vec![(
                "project".to_string(),
                std::path::PathBuf::from("/tmp/project")
            )]
        );
    }

    /// CAIRN-2194: a team project's `projects` row lives only in its team
    /// replica, never the private DB. `project_path` must route through every
    /// open database (via `owning_db`) to resolve it, and `all_project_paths`
    /// must union team projects into its result — otherwise the recipe, agent,
    /// and skill pickers come up empty on a team-project issue.
    #[tokio::test(flavor = "current_thread")]
    async fn project_path_resolves_team_project_in_open_replica() {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();

        let local = Arc::new(crate::storage::migrated_test_db("cfg-local.db").await);
        let team = Arc::new(crate::storage::migrated_test_db("cfg-team.db").await);
        // Seed the team project ONLY in the team replica, exactly as routed
        // creation does — the private DB never carries this row.
        team.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES ('team1~00000000-0000-4000-8000-000000000001', 'default', 'Team Project', 'TEAM', '/tmp/team-repo', 1, 1, 0)",
                    (),
                ).await?;
                Ok(())
            })
        }).await.unwrap();

        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let dbs = Arc::new(DbState::new(local, index));
        dbs.insert_team_db_for_test("team1", team.clone()).await;

        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(dbs, services, config_dir).build();

        // Resolves through the open replica rather than erroring "not found".
        assert_eq!(
            orch.project_path("team1~00000000-0000-4000-8000-000000000001")
                .unwrap(),
            std::path::PathBuf::from("/tmp/team-repo")
        );
        // And the union list includes the team project.
        let all = orch.all_project_paths().unwrap();
        assert!(
            all.contains(&(
                "team1~00000000-0000-4000-8000-000000000001".to_string(),
                std::path::PathBuf::from("/tmp/team-repo")
            )),
            "all_project_paths should union team projects: {all:?}"
        );
    }

    /// The picker chain end-to-end: `list_recipes_for_context` for a team
    /// project must return the inherited workspace recipes instead of bailing
    /// with the propagated "Project not found" that emptied the picker.
    #[tokio::test(flavor = "current_thread")]
    async fn list_recipes_for_context_resolves_team_project() {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        // A workspace recipe that every project context inherits.
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        std::fs::write(
            config_dir.join("recipes").join("build.yaml"),
            "cairnVersion: 1\nname: Build\ntrigger: issue\ncontext: issue\n\nnodes:\n  - id: trigger-1\n    type: trigger\n    name: Trigger\n    position: 0@0\n\nedges: []\n",
        )
        .unwrap();

        let local = Arc::new(crate::storage::migrated_test_db("recipe-local.db").await);
        let team = Arc::new(crate::storage::migrated_test_db("recipe-team.db").await);
        let team_repo = temp.path().join("team-repo");
        std::fs::create_dir_all(&team_repo).unwrap();
        let repo_path = team_repo.to_str().unwrap().to_string();
        team.write(move |conn| {
            let repo_path = repo_path.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES ('team1~00000000-0000-4000-8000-000000000001', 'default', 'Team Project', 'TEAM', ?1, 1, 1, 0)",
                    (repo_path.as_str(),),
                ).await?;
                Ok(())
            })
        }).await.unwrap();

        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let dbs = Arc::new(DbState::new(local, index));
        dbs.insert_team_db_for_test("team1", team.clone()).await;

        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(dbs, services, config_dir).build();

        let recipes = orch
            .list_recipes_for_context("team1~00000000-0000-4000-8000-000000000001")
            .unwrap();
        assert!(
            recipes.iter().any(|r| r.id == "build"),
            "team-project context should inherit the workspace 'build' recipe: {:?}",
            recipes.iter().map(|r| r.id.clone()).collect::<Vec<_>>()
        );
    }

    /// The resolution-layering acceptance core (CAIRN-2579): a same-named
    /// agent, recipe, AND skill defined at personal, team-workspace, and project
    /// scope resolves to the project copy; removing the project copy falls to the
    /// team workspace; removing that falls to personal. Proves the ordered
    /// three-layer passes (`project > team workspace > personal`) for every
    /// entity type through the public `list_*_for_context` path.
    #[tokio::test(flavor = "current_thread")]
    async fn team_workspace_layering_shadows_all_entity_types() {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("personal"); // ~/.cairn twin
        let team_ws = temp.path().join("team-ws"); // team config-home clone
        let project_dir = temp.path().join("project-repo");

        // `marker` distinguishes which scope a same-id config came from.
        let write_scope = |root: &std::path::Path, project_scoped: bool, marker: &str| {
            let base = if project_scoped {
                root.join(".cairn")
            } else {
                root.to_path_buf()
            };
            std::fs::create_dir_all(base.join("recipes")).unwrap();
            std::fs::create_dir_all(base.join("agents")).unwrap();
            std::fs::create_dir_all(base.join("skills").join("shared")).unwrap();
            std::fs::write(
                base.join("recipes").join("shared.yaml"),
                format!("cairnVersion: 1\nname: {marker}\ntrigger: issue\ncontext: issue\n\nnodes:\n  - id: trigger-1\n    type: trigger\n    name: Trigger\n    position: 0@0\n\nedges: []\n"),
            )
            .unwrap();
            std::fs::write(
                base.join("agents").join("shared.md"),
                format!(
                    "---\nname: shared\ndescription: {marker}\ntools:\n  - Read\n---\n\nPrompt.\n"
                ),
            )
            .unwrap();
            std::fs::write(
                base.join("skills").join("shared").join("SKILL.md"),
                format!("---\nname: shared\ndescription: {marker}\n---\n\nSkill body.\n"),
            )
            .unwrap();
        };
        write_scope(&config_dir, false, "Personal");
        write_scope(&team_ws, false, "TeamWs");
        write_scope(&project_dir, true, "Project");

        let local = Arc::new(crate::storage::migrated_test_db("tws-layer-local.db").await);
        let team = Arc::new(crate::storage::migrated_test_db("tws-layer-team.db").await);
        let team_id = "team1";
        let team_project_id = "team1~00000000-0000-4000-8000-000000000001";
        let ws_project_id = "team1~00000000-0000-4000-8000-0000000000ff";

        let project_repo = project_dir.to_str().unwrap().to_string();
        team.write(move |conn| {
            let project_repo = project_repo.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES ('team1~00000000-0000-4000-8000-000000000001', 'default', 'Team Project', 'TEAM', ?1, 1, 1, 0)",
                    (project_repo.as_str(),),
                ).await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES ('team1~00000000-0000-4000-8000-0000000000ff', 'default', 'Team Workspace', 'WORKSPACE-team1', '', 1, 1, 1)",
                    (),
                ).await?;
                Ok(())
            })
        }).await.unwrap();

        // Private route: the workspace project's (team-scoped) key -> its
        // machine-local clone. Routes are stored upper-cased, as reconcile and
        // `resolve_local_repo_path` both normalize the key. A NULL route team_id
        // is fine here (the clone lookup keys on `project_key` only) and sidesteps
        // the private `teams` registry FK.
        let team_ws_str = team_ws.to_str().unwrap().to_string();
        local
            .execute(
                "INSERT INTO project_routes(project_key, team_id, local_repo_path, created_at)
                 VALUES ('WORKSPACE-TEAM1', NULL, ?1, 1)",
                (team_ws_str.as_str(),),
            )
            .await
            .unwrap();

        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let dbs = Arc::new(DbState::new(local, index));
        dbs.insert_team_db_for_test(team_id, team.clone()).await;
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(dbs, services, config_dir.clone()).build();

        // The resolver finds the workspace clone for the team project.
        assert_eq!(
            orch.team_workspace_path(team_project_id).unwrap(),
            Some((ws_project_id.to_string(), team_ws.clone()))
        );

        let shared_markers = |orch: &super::Orchestrator| -> (String, String, String) {
            let recipe = orch
                .list_recipes_for_context(team_project_id)
                .unwrap()
                .into_iter()
                .find(|r| r.id == "shared")
                .expect("shared recipe present");
            let agent = orch
                .list_agents_for_context(team_project_id)
                .unwrap()
                .into_iter()
                .find(|a| a.id == "shared")
                .expect("shared agent present");
            let skill = orch
                .list_skills_for_context(team_project_id)
                .unwrap()
                .into_iter()
                .find(|s| s.id == "shared")
                .expect("shared skill present");
            (recipe.name, agent.description, skill.description)
        };

        // All three scopes present -> project wins.
        assert_eq!(
            shared_markers(&orch),
            (
                "Project".to_string(),
                "Project".to_string(),
                "Project".to_string()
            )
        );

        // Remove the project copies -> the team workspace wins.
        std::fs::remove_file(project_dir.join(".cairn/recipes/shared.yaml")).unwrap();
        std::fs::remove_file(project_dir.join(".cairn/agents/shared.md")).unwrap();
        std::fs::remove_dir_all(project_dir.join(".cairn/skills/shared")).unwrap();
        assert_eq!(
            shared_markers(&orch),
            (
                "TeamWs".to_string(),
                "TeamWs".to_string(),
                "TeamWs".to_string()
            )
        );

        // Remove the team-workspace copies -> personal wins.
        std::fs::remove_file(team_ws.join("recipes/shared.yaml")).unwrap();
        std::fs::remove_file(team_ws.join("agents/shared.md")).unwrap();
        std::fs::remove_dir_all(team_ws.join("skills/shared")).unwrap();
        assert_eq!(
            shared_markers(&orch),
            (
                "Personal".to_string(),
                "Personal".to_string(),
                "Personal".to_string()
            )
        );
    }

    /// Graceful degrade (guards the CAIRN-2194 empty-picker regression): a local
    /// project id gets no middle layer, and a team whose workspace row exists but
    /// is not cloned on this machine resolves to `None` rather than erroring.
    #[tokio::test(flavor = "current_thread")]
    async fn team_workspace_path_degrades_gracefully() {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("personal");
        std::fs::create_dir_all(&config_dir).unwrap();

        let local = Arc::new(crate::storage::migrated_test_db("tws-degrade-local.db").await);
        let team = Arc::new(crate::storage::migrated_test_db("tws-degrade-team.db").await);
        // Workspace row seeded, but NO project_routes clone path -> not cloned.
        team.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES ('team1~00000000-0000-4000-8000-0000000000ff', 'default', 'Team Workspace', 'WORKSPACE-team1', '', 1, 1, 1)",
                    (),
                ).await?;
                Ok(())
            })
        }).await.unwrap();

        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let dbs = Arc::new(DbState::new(local, index));
        dbs.insert_team_db_for_test("team1", team.clone()).await;
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(dbs, services, config_dir).build();

        // Local project id -> no middle layer, no error.
        assert_eq!(
            orch.team_workspace_path("00000000-0000-4000-8000-000000000abc")
                .unwrap(),
            None
        );
        // Team project whose workspace is seeded-but-not-cloned -> None, no error.
        assert_eq!(
            orch.team_workspace_path("team1~00000000-0000-4000-8000-000000000001")
                .unwrap(),
            None
        );
        // A team with no open replica at all -> None, no error.
        assert_eq!(
            orch.team_workspace_path("unopened~00000000-0000-4000-8000-000000000001")
                .unwrap(),
            None
        );
    }

    /// Phase 2 activation, end-to-end: `ensure_team_workspace` materializes the
    /// remoteless bundle twin, records the clone path under the team-scoped
    /// route key, and only THEN does the read-path `team_workspace_path` resolve
    /// (it was `None` before) and the fail-closed `workspace_write_root` return
    /// the clone. Proves the middle layer flips from absent to live purely by
    /// materializing — the exact gap the foundation left open in production.
    #[tokio::test(flavor = "current_thread")]
    async fn ensure_team_workspace_materializes_and_activates_resolution() {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("cairn-home"); // <- data root / ~/.cairn
        std::fs::create_dir_all(&config_dir).unwrap();

        // A minimal app-bundle resource dir with one bundled agent to seed.
        let resource_dir = temp.path().join("resources");
        std::fs::create_dir_all(resource_dir.join("agents")).unwrap();
        std::fs::write(
            resource_dir.join("agents").join("example.md"),
            "---\nname: example\ndescription: Bundled\ntools:\n  - Read\n---\n\nPrompt.\n",
        )
        .unwrap();

        let local = Arc::new(crate::storage::migrated_test_db("tws-ensure-local.db").await);
        let team = Arc::new(crate::storage::migrated_test_db("tws-ensure-team.db").await);
        let team_id = "team1";
        let team_project_id = "team1~00000000-0000-4000-8000-000000000001";
        let ws_project_id = "team1~00000000-0000-4000-8000-0000000000ff";

        // Seed exactly what `reconcile_team_routes` leaves behind: the team
        // project, the `is_workspace` row, and a path-LESS route stub. No clone
        // path yet -> the middle layer is absent, as it is in production.
        team.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES ('team1~00000000-0000-4000-8000-000000000001', 'default', 'Team Project', 'TEAM', '', 1, 1, 0)",
                    (),
                ).await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES ('team1~00000000-0000-4000-8000-0000000000ff', 'default', 'Team Workspace', 'WORKSPACE-TEAM1', '', 1, 1, 1)",
                    (),
                ).await?;
                Ok(())
            })
        }).await.unwrap();

        // The path-LESS route stub reconcile_team_routes leaves behind (team_id
        // NULL sidesteps the private `teams` registry FK, per the sibling tests);
        // materialization fills its `local_repo_path`.
        local
            .execute(
                "INSERT INTO project_routes(project_key, team_id, created_at)
                 VALUES ('WORKSPACE-TEAM1', NULL, 1)",
                (),
            )
            .await
            .unwrap();

        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let dbs = Arc::new(DbState::new(local, index));
        dbs.insert_team_db_for_test(team_id, team.clone()).await;
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(dbs, services, config_dir.clone()).build();

        let expected_ws = config_dir.join("teams").join("team1").join("workspace");

        // Before materialization: no middle layer, and a write would fail closed.
        assert_eq!(orch.team_workspace_path(team_project_id).unwrap(), None);
        assert!(orch.workspace_write_root(ws_project_id).is_err());

        // Materialize.
        let got = orch.ensure_team_workspace(&resource_dir, team_id).unwrap();
        assert_eq!(got, expected_ws);
        // The bundle twin exists on disk as a git repo carrying the seeded agent.
        assert!(
            expected_ws.join(".git").exists(),
            "workspace repo initialized"
        );
        assert!(
            expected_ws.join("agents").join("example.md").exists(),
            "bundled agent seeded into the team workspace clone"
        );

        // After materialization: the read path resolves the clone, and the
        // fail-closed write path returns it.
        assert_eq!(
            orch.team_workspace_path(team_project_id).unwrap(),
            Some((ws_project_id.to_string(), expected_ws.clone()))
        );
        assert_eq!(
            orch.workspace_write_root(ws_project_id).unwrap(),
            expected_ws
        );
        // "default" always maps to the personal home; a bad id fails closed.
        assert_eq!(orch.workspace_write_root("default").unwrap(), config_dir);
        assert!(orch.workspace_write_root("not-a-real-id").is_err());

        // Idempotent: a second pass is a clean no-op that keeps resolution live.
        let again = orch.ensure_team_workspace(&resource_dir, team_id).unwrap();
        assert_eq!(again, expected_ws);
        assert_eq!(
            orch.team_workspace_path(team_project_id).unwrap(),
            Some((ws_project_id.to_string(), expected_ws))
        );
    }

    /// Phase 3 authoring, end-to-end: with a materialized team workspace, a
    /// create/update/delete scoped to the team-workspace id writes into the team
    /// clone — NOT the personal `~/.cairn` — and the personal home never gains
    /// the file. Proves the fail-closed write root (`workspace_write_root`)
    /// threaded through create/update/delete actually routes team authoring.
    #[tokio::test(flavor = "current_thread")]
    async fn team_workspace_scoped_agent_writes_into_team_clone() {
        use crate::db::DbState;
        use crate::models::{CreateAgentConfig, UpdateAgentConfig};
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("cairn-home");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        let resource_dir = temp.path().join("resources");
        std::fs::create_dir_all(resource_dir.join("agents")).unwrap();

        let local = Arc::new(crate::storage::migrated_test_db("tws-author-local.db").await);
        let team = Arc::new(crate::storage::migrated_test_db("tws-author-team.db").await);
        let team_id = "team1";
        let ws_project_id = "team1~00000000-0000-4000-8000-0000000000ff";

        team.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES ('team1~00000000-0000-4000-8000-0000000000ff', 'default', 'Team Workspace', 'WORKSPACE-TEAM1', '', 1, 1, 1)",
                    (),
                ).await?;
                Ok(())
            })
        }).await.unwrap();
        local
            .execute(
                "INSERT INTO project_routes(project_key, team_id, created_at)
                 VALUES ('WORKSPACE-TEAM1', NULL, 1)",
                (),
            )
            .await
            .unwrap();

        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let dbs = Arc::new(DbState::new(local, index));
        dbs.insert_team_db_for_test(team_id, team.clone()).await;
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(dbs, services, config_dir.clone()).build();

        let clone = orch.ensure_team_workspace(&resource_dir, team_id).unwrap();
        let team_agent = clone.join("agents").join("teammate.md");
        let personal_agent = config_dir.join("agents").join("teammate.md");

        // Create, scoped to the team workspace.
        let created = orch
            .create_agent_config(CreateAgentConfig {
                id: "teammate".to_string(),
                name: "Teammate".to_string(),
                description: "v1".to_string(),
                prompt: "Help.".to_string(),
                tools: vec!["Read".to_string()],
                tier: None,
                workspace_id: Some(ws_project_id.to_string()),
                project_id: None,
                disallowed_tools: None,
                skills: None,
                fence: None,
                sandbox: None,
                on_escape: None,
                backend_preference: None,
                icon: None,
            })
            .unwrap();
        assert_eq!(created.workspace_id.as_deref(), Some(ws_project_id));
        assert!(team_agent.exists(), "agent written into the team clone");
        assert!(
            !personal_agent.exists(),
            "team agent must NOT leak into the personal ~/.cairn"
        );
        assert!(
            std::fs::read_to_string(&team_agent).unwrap().contains("v1"),
            "team clone holds the authored content"
        );

        // A personal (default) agent, to prove the two scopes never cross-list.
        orch.create_agent_config(CreateAgentConfig {
            id: "solo".to_string(),
            name: "Solo".to_string(),
            description: "personal".to_string(),
            prompt: "Solo.".to_string(),
            tools: vec!["Read".to_string()],
            tier: None,
            workspace_id: Some("default".to_string()),
            project_id: None,
            disallowed_tools: None,
            skills: None,
            fence: None,
            sandbox: None,
            on_escape: None,
            backend_preference: None,
            icon: None,
        })
        .unwrap();

        // The scoped LIST reads the team clone for a team-workspace id: the team
        // agent appears (labeled with the team-workspace id), the personal one
        // does not. This is the read contract the pickers/Team Settings depend on.
        let team_list = orch.list_agent_configs(Some(ws_project_id), None).unwrap();
        assert!(
            team_list
                .iter()
                .any(|a| a.id == "teammate" && a.workspace_id.as_deref() == Some(ws_project_id)),
            "team agent must list under the team-workspace scope: {:?}",
            team_list.iter().map(|a| a.id.clone()).collect::<Vec<_>>()
        );
        assert!(
            !team_list.iter().any(|a| a.id == "solo"),
            "personal agent must NOT appear in the team-workspace list"
        );

        // The personal (default) list reads `~/.cairn`: only the personal agent.
        let personal_list = orch.list_agent_configs(Some("default"), None).unwrap();
        assert!(
            personal_list
                .iter()
                .any(|a| a.id == "solo" && a.workspace_id.as_deref() == Some("default")),
            "personal agent must list under the default scope"
        );
        assert!(
            !personal_list.iter().any(|a| a.id == "teammate"),
            "team agent must NOT appear in the personal list (no mislabeling)"
        );

        // Update, scoped to the team workspace: edits the clone copy in place.
        orch.update_agent_config(
            "teammate",
            UpdateAgentConfig {
                description: Some("v2".to_string()),
                workspace_id: Some(Some(ws_project_id.to_string())),
                ..Default::default()
            },
            None,
            Some(ws_project_id),
        )
        .unwrap();
        assert!(
            std::fs::read_to_string(&team_agent).unwrap().contains("v2"),
            "update edits the team clone copy"
        );
        assert!(!personal_agent.exists());

        // Delete, scoped to the team workspace: removes the clone copy.
        orch.delete_agent_config("teammate", None, Some(ws_project_id))
            .unwrap();
        assert!(!team_agent.exists(), "delete removes the team clone copy");
    }

    /// CAIRN-2597: the recipe verbs that previously assumed the personal home
    /// (`duplicate_recipe`, `archive_recipe`) now thread `workspace_id`, so a
    /// team-workspace recipe is duplicated INTO and archived FROM that team's
    /// clone rather than misrouting to `~/.cairn`.
    #[tokio::test(flavor = "current_thread")]
    async fn team_workspace_scoped_recipe_duplicate_and_archive_target_team_clone() {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("cairn-home");
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        let resource_dir = temp.path().join("resources");
        std::fs::create_dir_all(resource_dir.join("recipes")).unwrap();

        let local = Arc::new(crate::storage::migrated_test_db("tws-recipe-local.db").await);
        let team = Arc::new(crate::storage::migrated_test_db("tws-recipe-team.db").await);
        let team_id = "team1";
        let ws_project_id = "team1~00000000-0000-4000-8000-0000000000ff";

        team.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES ('team1~00000000-0000-4000-8000-0000000000ff', 'default', 'Team Workspace', 'WORKSPACE-TEAM1', '', 1, 1, 1)",
                    (),
                ).await?;
                Ok(())
            })
        }).await.unwrap();
        local
            .execute(
                "INSERT INTO project_routes(project_key, team_id, created_at)
                 VALUES ('WORKSPACE-TEAM1', NULL, 1)",
                (),
            )
            .await
            .unwrap();

        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let dbs = Arc::new(DbState::new(local, index));
        dbs.insert_team_db_for_test(team_id, team.clone()).await;
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(dbs, services, config_dir.clone()).build();

        let clone = orch.ensure_team_workspace(&resource_dir, team_id).unwrap();
        let recipes_dir = clone.join("recipes");

        // Seed a team-workspace recipe directly into the clone (create's own
        // path is covered elsewhere; this test targets duplicate/archive).
        std::fs::write(
            recipes_dir.join("deploy.yaml"),
            "cairnVersion: 1\nname: Deploy\ntrigger: manual\ncontext: issue\n\nnodes:\n  - id: trigger-1\n    type: trigger\n    name: Trigger\n    position: 0@0\n\nedges: []\n",
        )
        .unwrap();
        // A same-id personal recipe, to prove duplicate/archive never touch it.
        std::fs::write(
            config_dir.join("recipes").join("deploy.yaml"),
            "cairnVersion: 1\nname: Deploy\ntrigger: manual\ncontext: issue\n\nnodes:\n  - id: trigger-1\n    type: trigger\n    name: Trigger\n    position: 0@0\n\nedges: []\n",
        )
        .unwrap();

        // Duplicate, scoped to the team workspace: the copy lands in the clone.
        orch.duplicate_recipe("deploy", "Deploy Copy", None, Some(ws_project_id))
            .unwrap();
        assert!(
            recipes_dir.join("deploy-copy.yaml").exists(),
            "duplicate written into the team clone, not the personal home"
        );
        assert!(
            !config_dir.join("recipes").join("deploy-copy.yaml").exists(),
            "duplicate must NOT misroute to the personal ~/.cairn"
        );

        // Archive (delete) the original, scoped to the team workspace.
        orch.archive_recipe("deploy", None, Some(ws_project_id))
            .unwrap();
        assert!(
            !recipes_dir.join("deploy.yaml").exists(),
            "archive removes the team clone copy"
        );
        assert!(
            recipes_dir.join("deploy-copy.yaml").exists(),
            "the duplicate survives the original's archive"
        );
        // The identically-named personal recipe is untouched by both verbs.
        assert!(
            config_dir.join("recipes").join("deploy.yaml").exists(),
            "the personal ~/.cairn recipe must be untouched by team-scoped archive"
        );
        assert!(
            !config_dir.join("recipes").join("deploy-copy.yaml").exists(),
            "the duplicate must not have leaked into the personal ~/.cairn"
        );
    }

    /// Multi-team regression (the reserved-key collision): two registered teams
    /// each have their OWN workspace clone. Because the machine-local
    /// `project_routes` catalog is keyed by `project_key` alone, a constant
    /// `WORKSPACE` key would collide across teams; the team-scoped key keeps each
    /// team's route and clone path distinct, so team B never resolves to team A's
    /// workspace.
    #[tokio::test(flavor = "current_thread")]
    async fn team_workspace_paths_are_team_scoped_across_multiple_teams() {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("personal");
        std::fs::create_dir_all(&config_dir).unwrap();
        let recipe = |name: &str| {
            format!("cairnVersion: 1\nname: {name}\ntrigger: issue\ncontext: issue\n\nnodes:\n  - id: trigger-1\n    type: trigger\n    name: Trigger\n    position: 0@0\n\nedges: []\n")
        };

        // Two distinct workspace clones, each with a same-id 'shared' recipe, and
        // an (empty) project repo per team.
        let ws_a = temp.path().join("ws-a");
        let ws_b = temp.path().join("ws-b");
        std::fs::create_dir_all(ws_a.join("recipes")).unwrap();
        std::fs::create_dir_all(ws_b.join("recipes")).unwrap();
        std::fs::write(ws_a.join("recipes/shared.yaml"), recipe("TeamA")).unwrap();
        std::fs::write(ws_b.join("recipes/shared.yaml"), recipe("TeamB")).unwrap();
        let proj_a = temp.path().join("proj-a");
        let proj_b = temp.path().join("proj-b");
        std::fs::create_dir_all(&proj_a).unwrap();
        std::fs::create_dir_all(&proj_b).unwrap();

        let local = Arc::new(crate::storage::migrated_test_db("tws-multi-local.db").await);
        let team_a = Arc::new(crate::storage::migrated_test_db("tws-multi-a.db").await);
        let team_b = Arc::new(crate::storage::migrated_test_db("tws-multi-b.db").await);

        let proj_a_str = proj_a.to_str().unwrap().to_string();
        team_a.write(move |conn| {
            let proj_a_str = proj_a_str.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES ('teamaaa~00000000-0000-4000-8000-000000000001', 'default', 'A', 'PROJA', ?1, 1, 1, 0)",
                    (proj_a_str.as_str(),),
                ).await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES ('teamaaa~00000000-0000-4000-8000-0000000000ff', 'default', 'WS', 'WORKSPACE-teamaaa', '', 1, 1, 1)",
                    (),
                ).await?;
                Ok(())
            })
        }).await.unwrap();
        let proj_b_str = proj_b.to_str().unwrap().to_string();
        team_b.write(move |conn| {
            let proj_b_str = proj_b_str.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES ('teambbb~00000000-0000-4000-8000-000000000001', 'default', 'B', 'PROJB', ?1, 1, 1, 0)",
                    (proj_b_str.as_str(),),
                ).await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES ('teambbb~00000000-0000-4000-8000-0000000000ff', 'default', 'WS', 'WORKSPACE-teambbb', '', 1, 1, 1)",
                    (),
                ).await?;
                Ok(())
            })
        }).await.unwrap();

        // Distinct, non-colliding routes (upper-cased team-scoped keys).
        local
            .execute(
                "INSERT INTO project_routes(project_key, team_id, local_repo_path, created_at)
                 VALUES ('WORKSPACE-TEAMAAA', NULL, ?1, 1)",
                (ws_a.to_str().unwrap(),),
            )
            .await
            .unwrap();
        local
            .execute(
                "INSERT INTO project_routes(project_key, team_id, local_repo_path, created_at)
                 VALUES ('WORKSPACE-TEAMBBB', NULL, ?1, 1)",
                (ws_b.to_str().unwrap(),),
            )
            .await
            .unwrap();

        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let dbs = Arc::new(DbState::new(local, index));
        dbs.insert_team_db_for_test("teamaaa", team_a.clone()).await;
        dbs.insert_team_db_for_test("teambbb", team_b.clone()).await;
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(dbs, services, config_dir).build();

        // Each team's project resolves to ITS OWN workspace clone — no collision.
        assert_eq!(
            orch.team_workspace_path("teamaaa~00000000-0000-4000-8000-000000000001")
                .unwrap(),
            Some((
                "teamaaa~00000000-0000-4000-8000-0000000000ff".to_string(),
                ws_a.clone()
            ))
        );
        assert_eq!(
            orch.team_workspace_path("teambbb~00000000-0000-4000-8000-000000000001")
                .unwrap(),
            Some((
                "teambbb~00000000-0000-4000-8000-0000000000ff".to_string(),
                ws_b.clone()
            ))
        );
        // And the layered 'shared' recipe is each team's own.
        let shared_name = |pid: &str| {
            orch.list_recipes_for_context(pid)
                .unwrap()
                .into_iter()
                .find(|r| r.id == "shared")
                .expect("shared recipe")
                .name
        };
        assert_eq!(
            shared_name("teamaaa~00000000-0000-4000-8000-000000000001"),
            "TeamA"
        );
        assert_eq!(
            shared_name("teambbb~00000000-0000-4000-8000-000000000001"),
            "TeamB"
        );
    }

    /// Local-only no-op invariant: with NO team registered, a local project's
    /// context is byte-for-byte the pre-feature behavior — the team-workspace pass
    /// contributes nothing and never errors. Asserted at the `list_*_for_context`
    /// level, not merely by the resolver returning `None`.
    #[tokio::test(flavor = "current_thread")]
    async fn local_project_context_has_no_team_workspace_layer() {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("personal");
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        std::fs::write(
            config_dir.join("recipes/build.yaml"),
            "cairnVersion: 1\nname: Build\ntrigger: issue\ncontext: issue\n\nnodes:\n  - id: trigger-1\n    type: trigger\n    name: Trigger\n    position: 0@0\n\nedges: []\n",
        )
        .unwrap();

        let project_dir = temp.path().join("local-repo");
        std::fs::create_dir_all(&project_dir).unwrap();
        let local = Arc::new(crate::storage::migrated_test_db("tws-localnoop.db").await);
        let project_repo = project_dir.to_str().unwrap().to_string();
        // A LOCAL project (bare id) in the private DB; no team registered at all.
        local
            .write(move |conn| {
                let project_repo = project_repo.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                         VALUES ('local-1', 'default', 'Local', 'LOCAL', ?1, 1, 1, 0)",
                        (project_repo.as_str(),),
                    ).await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let dbs = Arc::new(DbState::new(local, index));
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(dbs, services, config_dir).build();

        // A bare (Local) id gets no middle layer, and never errors.
        assert_eq!(orch.team_workspace_path("local-1").unwrap(), None);
        // Resolution returns exactly the personal recipe — unchanged from before.
        let ids: Vec<String> = orch
            .list_recipes_for_context("local-1")
            .unwrap()
            .into_iter()
            .map(|r| r.id)
            .collect();
        assert_eq!(ids, vec!["build".to_string()]);
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
            icon: None,
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
            None,
        )
        .unwrap();
        assert_eq!(head_subject(&config_dir), "cairn: update agent planner");
        assert!(porcelain(&config_dir).contains("settings.yaml"));

        orch.delete_agent_config("planner", None, None).unwrap();
        assert_eq!(head_subject(&config_dir), "cairn: delete agent planner");
        let status = porcelain(&config_dir);
        assert!(
            status.contains("settings.yaml") && !status.contains("planner.md"),
            "delete should commit only the agent file: {status:?}"
        );
    }

    /// Regression: moving a workspace-scoped config into a project commits the
    /// workspace-side deletion into `~/.cairn` AND pushes it to the workspace
    /// remote, while the project repo's origin is never touched. Guards the
    /// asymmetry where keying push_root on the *new* (project) scope dropped the
    /// push of the workspace deletion.
    #[tokio::test]
    async fn workspace_to_project_move_pushes_workspace_deletion_only() {
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
        fn rev(dir: &Path, r: &str) -> String {
            String::from_utf8_lossy(&git(&["rev-parse", r], dir).stdout)
                .trim()
                .to_string()
        }
        fn branch(dir: &Path) -> String {
            String::from_utf8_lossy(&git(&["rev-parse", "--abbrev-ref", "HEAD"], dir).stdout)
                .trim()
                .to_string()
        }
        fn head_subject(dir: &Path) -> String {
            String::from_utf8_lossy(&git(&["log", "-1", "--pretty=%s"], dir).stdout)
                .trim()
                .to_string()
        }
        fn origin_ref(origin: &Path, branch: &str) -> Option<String> {
            let out = git(
                &[
                    "rev-parse",
                    "--verify",
                    "--quiet",
                    &format!("refs/heads/{branch}"),
                ],
                origin,
            );
            out.status
                .success()
                .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
        }
        fn init_repo(dir: &Path) {
            assert!(git(&["init", "-q"], dir).status.success());
        }
        fn init_bare(dir: &Path) {
            std::fs::create_dir_all(dir).unwrap();
            assert!(git(&["init", "--bare", "-q"], dir).status.success());
        }
        fn set_origin(repo: &Path, origin: &Path) {
            assert!(
                git(&["remote", "add", "origin", origin.to_str().unwrap()], repo)
                    .status
                    .success()
            );
        }

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();

        // Workspace repo (~/.cairn) with a bare origin.
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        init_repo(&config_dir);
        let ws_origin = root.join("ws-origin.git");
        init_bare(&ws_origin);
        set_origin(&config_dir, &ws_origin);

        // Project repo with its own bare origin, to prove it is never pushed.
        let project_repo = root.join("project");
        std::fs::create_dir_all(&project_repo).unwrap();
        init_repo(&project_repo);
        let proj_origin = root.join("proj-origin.git");
        init_bare(&proj_origin);
        set_origin(&project_repo, &proj_origin);

        let db = LocalDb::open(root.join("test.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        // Register the project so orch.project_path("project") resolves to its repo.
        let repo_path = project_repo.to_str().unwrap().to_string();
        db.write(move |conn| {
            let repo_path = repo_path.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                     VALUES ('project', 'default', 'Project', 'PROJ', ?1, 1, 1, 0)",
                    (repo_path.as_str(),),
                ).await?;
                Ok(())
            })
        }).await.unwrap();

        let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(db_state, services, config_dir.clone()).build();

        orch.create_agent_config(CreateAgentConfig {
            id: "planner".to_string(),
            name: "Planner".to_string(),
            description: "Plans things".to_string(),
            prompt: "Plan.".to_string(),
            tools: vec!["Read".to_string()],
            tier: None,
            workspace_id: Some("default".to_string()),
            project_id: None,
            disallowed_tools: None,
            skills: None,
            fence: None,
            sandbox: None,
            on_escape: None,
            backend_preference: None,
            icon: None,
        })
        .unwrap();

        // Move it into the project: the workspace file is deleted+committed in
        // `~/.cairn`, and the new project file is committed in the project repo.
        orch.update_agent_config(
            "planner",
            UpdateAgentConfig {
                project_id: Some(Some("project".to_string())),
                ..Default::default()
            },
            Some("project"),
            None,
        )
        .unwrap();

        let ws_branch = branch(&config_dir);
        let ws_head = rev(&config_dir, "HEAD");
        assert_eq!(
            head_subject(&config_dir),
            "cairn: update agent planner",
            "workspace repo should hold the scope-change deletion commit"
        );
        assert_eq!(
            head_subject(&project_repo),
            "cairn: update agent planner",
            "project repo should hold the new project-scoped file"
        );

        // The workspace deletion must reach the workspace origin. The push is a
        // detached best-effort thread, so poll briefly for it to land.
        let mut pushed = false;
        for _ in 0..120 {
            if origin_ref(&ws_origin, &ws_branch).as_deref() == Some(ws_head.as_str()) {
                pushed = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        assert!(
            pushed,
            "workspace origin should advance to the deletion commit {ws_head}"
        );

        // The project origin must never receive a push: project scope is commit-only.
        let proj_branch = branch(&project_repo);
        assert!(
            origin_ref(&proj_origin, &proj_branch).is_none(),
            "project origin must not be pushed on a config scope change"
        );
    }
}
