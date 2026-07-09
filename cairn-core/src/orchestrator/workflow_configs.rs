//! Orchestrator workflow-configuration operations (settings surface).
//!
//! Workflows are file-backed packages (`config::workflows`), listed and edited by
//! the Settings Workflows page. Unlike agents/skills/recipes they have NO
//! team-workspace authoring home, so scope resolution here is the simpler two-way
//! split of personal workspace (`config_dir`) vs a real project's `.cairn/`.
//!
//! The heavy generic scope machinery in [`super::config_resource`] is built
//! around the DB-shadow + team-workspace + copy model and a single `File` type;
//! a workflow's editor payload also carries the SCRIPT text (separate from the
//! manifest), so these methods are dedicated rather than routed through that
//! trait. They reuse the `config::workflows` loader for every read and its
//! validated `save_workflow` for every write, so a save round-trips through the
//! same load/validation path the loader uses.

use std::path::{Path, PathBuf};

use crate::config::workflows::{self as config_workflows, FileWorkflow, WorkflowWorktreeMode};
use crate::config::{slugify, ConfigResult};
use crate::models::{SaveWorkflowConfig, WorkflowConfig, WorkflowConfigDetail};

use super::config_resource::InvalidConfig;
use super::Orchestrator;

/// Map a loaded [`FileWorkflow`] to the API [`WorkflowConfig`] with the resolved
/// scope ids.
fn to_workflow_config(
    w: FileWorkflow,
    workspace_id: Option<String>,
    project_id: Option<String>,
    now: i32,
) -> WorkflowConfig {
    WorkflowConfig {
        id: w.id,
        name: w.name,
        description: w.description,
        script: w.script,
        worktree: w.worktree.as_str().to_string(),
        args_schema: w.args_schema,
        output: w.output,
        workspace_id,
        project_id,
        created_at: now,
        updated_at: now,
    }
}

/// Derive the visible id for an invalid package from its manifest error path
/// (`.../<id>/workflow.yaml` → `<id>`).
fn invalid_id(path: &Path) -> String {
    path.parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .map(str::to_string)
        .unwrap_or_default()
}

fn now_ts() -> i32 {
    chrono::Utc::now().timestamp() as i32
}

impl Orchestrator {
    fn emit_workflow_change(&self, action: &str, id: &str) {
        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({ "entity_type": "workflow", "action": action, "id": id }),
        );
    }

    /// List workflow packages with optional scope filters, mirroring the
    /// skills/agents list semantics: both `None` aggregates the personal
    /// workspace plus every project; a `workspace_id` ("default") lists
    /// workspace-only; a `project_id` lists that project's own packages.
    pub fn list_workflow_configs(
        &self,
        workspace_id: Option<&str>,
        project_id: Option<&str>,
    ) -> Result<Vec<WorkflowConfig>, String> {
        let now = now_ts();
        let mut out = Vec::new();

        if workspace_id.is_none() && project_id.is_none() {
            for result in config_workflows::list_workflows(&self.config_dir, None)? {
                if let ConfigResult::Ok(w) = result {
                    if !w.is_project_scoped {
                        out.push(to_workflow_config(
                            w,
                            Some("default".to_string()),
                            None,
                            now,
                        ));
                    }
                }
            }
            for (pid, project_path) in self.all_project_paths()? {
                for result in
                    config_workflows::list_workflows(&self.config_dir, Some(&project_path))?
                {
                    if let ConfigResult::Ok(w) = result {
                        if w.is_project_scoped {
                            out.push(to_workflow_config(w, None, Some(pid.clone()), now));
                        }
                    }
                }
            }
        } else {
            let project_path: Option<PathBuf> =
                project_id.map(|pid| self.project_path(pid)).transpose()?;
            for result in
                config_workflows::list_workflows(&self.config_dir, project_path.as_deref())?
            {
                if let ConfigResult::Ok(w) = result {
                    let include = match (workspace_id, project_id) {
                        (Some(_), None) => !w.is_project_scoped,
                        (None, Some(_)) => w.is_project_scoped,
                        _ => true,
                    };
                    if include {
                        let (ws, pj) = if w.is_project_scoped {
                            (None, project_id.map(str::to_string))
                        } else {
                            (workspace_id.map(str::to_string), None)
                        };
                        out.push(to_workflow_config(w, ws, pj, now));
                    }
                }
            }
        }

        out.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
        Ok(out)
    }

    /// Load one workflow's manifest AND script text for the editor. Resolves
    /// project-then-workspace (project shadows workspace by id).
    pub fn get_workflow_config_detail(
        &self,
        id: &str,
        project_id: Option<&str>,
    ) -> Result<Option<WorkflowConfigDetail>, String> {
        let now = now_ts();
        let project_path: Option<PathBuf> =
            project_id.map(|pid| self.project_path(pid)).transpose()?;
        let Some(w) =
            config_workflows::get_workflow(&self.config_dir, id, project_path.as_deref())?
        else {
            return Ok(None);
        };
        let script_content = config_workflows::read_workflow_script(&w)?;
        let (ws, pj) = if w.is_project_scoped {
            (None, project_id.map(str::to_string))
        } else {
            (Some("default".to_string()), None)
        };
        Ok(Some(WorkflowConfigDetail {
            config: to_workflow_config(w, ws, pj, now),
            script_content,
        }))
    }

    /// Create or update a workflow package (`workflow.yaml` + script). Exactly one
    /// of `workspace_id` / `project_id` selects the write scope. The manifest is
    /// validated through the loader's path before anything is written.
    pub fn save_workflow_config(
        &self,
        input: SaveWorkflowConfig,
    ) -> Result<WorkflowConfig, String> {
        if input.workspace_id.is_some() == input.project_id.is_some() {
            return Err("Exactly one of workspaceId or projectId must be set".to_string());
        }
        let is_project = input.project_id.is_some();
        let project_path: Option<PathBuf> = input
            .project_id
            .as_deref()
            .map(|pid| self.project_path(pid))
            .transpose()?;

        let id = input
            .id
            .clone()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| slugify(&input.name));
        if id.trim().is_empty() {
            return Err("Workflow name must produce a non-empty id".to_string());
        }

        let worktree = match input.worktree.as_deref() {
            None | Some("") => WorkflowWorktreeMode::None,
            Some(v) => WorkflowWorktreeMode::parse(v)
                .ok_or_else(|| format!("Invalid worktree mode: {v}"))?,
        };
        let script = input
            .script
            .clone()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| config_workflows::DEFAULT_SCRIPT.to_string());

        let spec = config_workflows::SaveWorkflowSpec {
            id: &id,
            name: &input.name,
            description: &input.description,
            script: &script,
            worktree,
            args_schema: input.args_schema.as_ref(),
            output: input.output.as_ref(),
            script_content: &input.script_content,
            is_project_scoped: is_project,
        };
        let dest =
            config_workflows::save_workflow(&self.config_dir, &spec, project_path.as_deref())?;

        let commit_root: &Path = match &project_path {
            Some(p) => p.as_path(),
            None => self.config_dir.as_path(),
        };
        crate::config::commit_and_maybe_push(
            &[dest],
            &format!("cairn: save workflow {id}"),
            Some(commit_root),
        );
        self.emit_workflow_change("saved", &id);

        let now = now_ts();
        Ok(WorkflowConfig {
            id,
            name: input.name,
            description: input.description,
            script,
            worktree: worktree.as_str().to_string(),
            args_schema: input.args_schema,
            output: input.output,
            workspace_id: if is_project {
                None
            } else {
                Some(input.workspace_id.unwrap_or_else(|| "default".to_string()))
            },
            project_id: input.project_id,
            created_at: now,
            updated_at: now,
        })
    }

    /// Delete a workflow package. `workspace_id` is accepted for parity with the
    /// other config deletes; workflows have no team-workspace home, so it is only
    /// ever `"default"`/`None` and does not reroute the write.
    pub fn delete_workflow_config(
        &self,
        id: &str,
        project_id: Option<&str>,
        _workspace_id: Option<&str>,
    ) -> Result<(), String> {
        let project_path: Option<PathBuf> =
            project_id.map(|pid| self.project_path(pid)).transpose()?;
        let existing =
            config_workflows::get_workflow(&self.config_dir, id, project_path.as_deref())?;
        config_workflows::delete_workflow(&self.config_dir, id, project_path.as_deref())?;
        if let Some(w) = &existing {
            let root: &Path = match &project_path {
                Some(p) => p.as_path(),
                None => self.config_dir.as_path(),
            };
            crate::config::commit_and_maybe_push(
                std::slice::from_ref(&w.dir_path),
                &format!("cairn: delete workflow {id}"),
                Some(root),
            );
        }
        self.emit_workflow_change("removed", id);
        Ok(())
    }

    /// Copy a workflow package (from any scope) into a target scope under a chosen
    /// id. A same-id copy into a project shadows the inherited workspace package;
    /// a new id is additive. Hard copy: the whole package directory is duplicated.
    pub fn copy_workflow_config(
        &self,
        source_id: &str,
        source_project_id: Option<&str>,
        target_id: &str,
        target_project_id: Option<&str>,
        target_workspace_id: Option<&str>,
    ) -> Result<WorkflowConfig, String> {
        // Preserve the target id byte-for-byte. A same-id copy must shadow the
        // source package by its EXACT directory id: ids come straight from the
        // package directory name and are not required to be `slugify` output, so
        // normalizing here would change a legitimate id like `RepoAudit` (breaking
        // the shadow) or collapse a non-ASCII id to the empty string (targeting the
        // workflows root). The UI passes the source id for copy-to-edit;
        // display-name defense lives at that call site, not here.
        if target_id.trim().is_empty() {
            return Err("Copy target id must not be empty".to_string());
        }
        let source_project_path: Option<PathBuf> = source_project_id
            .map(|pid| self.project_path(pid))
            .transpose()?;
        let source = config_workflows::get_workflow(
            &self.config_dir,
            source_id,
            source_project_path.as_deref(),
        )?
        .ok_or_else(|| format!("Workflow not found: {source_id}"))?;

        let is_project = target_project_id.is_some();
        let target_project_path: Option<PathBuf> = target_project_id
            .map(|pid| self.project_path(pid))
            .transpose()?;
        let dest_workflows_dir = match &target_project_path {
            Some(p) => p.join(".cairn").join("workflows"),
            None => self.config_dir.join("workflows"),
        };
        let dest_dir = dest_workflows_dir.join(target_id);
        if dest_dir.join(config_workflows::MANIFEST_FILE).exists() {
            return Err(format!(
                "Workflow '{target_id}' already exists in {}",
                if is_project {
                    "this project"
                } else {
                    "workspace scope"
                }
            ));
        }

        config_workflows::copy_workflow_package(&source.dir_path, &dest_dir)?;
        let root: &Path = match &target_project_path {
            Some(p) => p.as_path(),
            None => self.config_dir.as_path(),
        };
        crate::config::commit_and_maybe_push(
            &[dest_dir],
            &format!("cairn: copy workflow {target_id}"),
            Some(root),
        );
        self.emit_workflow_change("created", target_id);

        let copied = config_workflows::get_workflow(
            &self.config_dir,
            target_id,
            target_project_path.as_deref(),
        )?
        .ok_or_else(|| "Copied workflow failed to load".to_string())?;
        let now = now_ts();
        let (ws, pj) = if is_project {
            (None, target_project_id.map(str::to_string))
        } else {
            (
                Some(target_workspace_id.unwrap_or("default").to_string()),
                None,
            )
        };
        Ok(to_workflow_config(copied, ws, pj, now))
    }

    /// Every workflow package that failed to load, for the Settings invalid-config
    /// surface. Scope arms mirror [`Orchestrator::list_workflow_configs`].
    pub(crate) fn list_invalid_workflow_configs(
        &self,
        workspace_id: Option<&str>,
        project_id: Option<&str>,
    ) -> Result<Vec<InvalidConfig>, String> {
        let mut invalid = Vec::new();
        let mut push = |path: &Path, error: String, pid: Option<String>| {
            invalid.push(InvalidConfig {
                id: invalid_id(path),
                entity_type: "workflow".to_string(),
                path: path.to_string_lossy().into_owned(),
                project_id: pid,
                error,
            });
        };

        if workspace_id.is_none() && project_id.is_none() {
            for result in config_workflows::list_workflows(&self.config_dir, None)? {
                if let ConfigResult::Err { path, error } = result {
                    push(&path, error, None);
                }
            }
            for (pid, project_path) in self.all_project_paths()? {
                for result in
                    config_workflows::list_workflows(&self.config_dir, Some(&project_path))?
                {
                    if let ConfigResult::Err { path, error } = result {
                        if path.starts_with(&project_path) {
                            push(&path, error, Some(pid.clone()));
                        }
                    }
                }
            }
        } else {
            let project_path: Option<PathBuf> =
                project_id.map(|pid| self.project_path(pid)).transpose()?;
            for result in
                config_workflows::list_workflows(&self.config_dir, project_path.as_deref())?
            {
                if let ConfigResult::Err { path, error } = result {
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
                            .then(|| project_id.map(str::to_string))
                            .flatten();
                        push(&path, error, pid);
                    }
                }
            }
        }

        invalid.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(invalid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn write_workflow(dir: &Path, manifest: &str) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("workflow.yaml"), manifest).unwrap();
        std::fs::write(dir.join("main.ts"), "// script\n").unwrap();
    }

    async fn orch_with_project(config_dir: &Path, project_dir: &Path) -> Orchestrator {
        let local = Arc::new(crate::storage::migrated_test_db("wf-cfg-local.db").await);
        let repo = project_dir.to_string_lossy().to_string();
        local
            .execute(
                "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                 VALUES ('proj1', 'default', 'Proj One', 'PROJ', ?1, 1, 1, 0)",
                (repo,),
            )
            .await
            .unwrap();
        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let dbs = Arc::new(DbState::new(local, index));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(dbs, services, config_dir.to_path_buf()).build()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn lists_both_scopes_and_reads_detail() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("cairn-home");
        std::fs::create_dir_all(&config_dir).unwrap();
        let project_dir = temp.path().join("proj");
        std::fs::create_dir_all(&project_dir).unwrap();

        write_workflow(
            &config_dir.join("workflows").join("ws-flow"),
            "name: WS Flow\ndescription: workspace\n",
        );
        write_workflow(
            &project_dir
                .join(".cairn")
                .join("workflows")
                .join("proj-flow"),
            "name: Proj Flow\ndescription: project\nworktree: inherit\n",
        );

        let orch = orch_with_project(&config_dir, &project_dir).await;

        // Both scopes aggregate.
        let all = orch.list_workflow_configs(None, None).unwrap();
        assert_eq!(all.len(), 2);
        let ws = all.iter().find(|w| w.id == "ws-flow").unwrap();
        assert_eq!(ws.workspace_id.as_deref(), Some("default"));
        assert!(ws.project_id.is_none());
        let proj = all.iter().find(|w| w.id == "proj-flow").unwrap();
        assert_eq!(proj.project_id.as_deref(), Some("proj1"));
        assert_eq!(proj.worktree, "inherit");

        // Workspace-only filter drops the project package.
        let ws_only = orch.list_workflow_configs(Some("default"), None).unwrap();
        assert_eq!(ws_only.len(), 1);
        assert_eq!(ws_only[0].id, "ws-flow");

        // Project-only filter drops the workspace package.
        let proj_only = orch.list_workflow_configs(None, Some("proj1")).unwrap();
        assert_eq!(proj_only.len(), 1);
        assert_eq!(proj_only[0].id, "proj-flow");

        // Detail carries the script text.
        let detail = orch
            .get_workflow_config_detail("ws-flow", None)
            .unwrap()
            .unwrap();
        assert_eq!(detail.config.name, "WS Flow");
        assert_eq!(detail.script_content, "// script\n");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn save_create_update_and_delete_roundtrip() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("cairn-home");
        std::fs::create_dir_all(&config_dir).unwrap();
        let project_dir = temp.path().join("proj");
        std::fs::create_dir_all(&project_dir).unwrap();
        let orch = orch_with_project(&config_dir, &project_dir).await;

        // Create at workspace scope; id derives from the name.
        let created = orch
            .save_workflow_config(SaveWorkflowConfig {
                id: None,
                name: "My Flow".to_string(),
                description: "desc".to_string(),
                script: None,
                worktree: Some("inherit".to_string()),
                args_schema: Some(serde_json::json!({
                    "type": "object",
                    "properties": { "topic": { "type": "string" } },
                    "required": ["topic"],
                })),
                output: None,
                script_content: "export {};\n".to_string(),
                workspace_id: Some("default".to_string()),
                project_id: None,
            })
            .unwrap();
        assert_eq!(created.id, "my-flow");
        assert_eq!(created.worktree, "inherit");

        // Re-read round-trips manifest + script.
        let detail = orch
            .get_workflow_config_detail("my-flow", None)
            .unwrap()
            .unwrap();
        assert_eq!(detail.config.description, "desc");
        assert_eq!(
            detail.config.args_schema.as_ref().unwrap()["required"][0],
            "topic"
        );
        assert_eq!(detail.script_content, "export {};\n");

        // Update changes the manifest.
        orch.save_workflow_config(SaveWorkflowConfig {
            id: Some("my-flow".to_string()),
            name: "My Flow".to_string(),
            description: "updated".to_string(),
            script: None,
            worktree: Some("none".to_string()),
            args_schema: None,
            output: None,
            script_content: "export {};\n// v2\n".to_string(),
            workspace_id: Some("default".to_string()),
            project_id: None,
        })
        .unwrap();
        let after = orch
            .get_workflow_config_detail("my-flow", None)
            .unwrap()
            .unwrap();
        assert_eq!(after.config.description, "updated");
        assert_eq!(after.config.worktree, "none");
        assert!(after.config.args_schema.is_none());
        assert_eq!(after.script_content, "export {};\n// v2\n");

        // An invalid args schema is rejected as a save error.
        let err = orch.save_workflow_config(SaveWorkflowConfig {
            id: Some("my-flow".to_string()),
            name: "My Flow".to_string(),
            description: "x".to_string(),
            script: None,
            worktree: None,
            args_schema: Some(serde_json::json!({ "type": "string", "pattern": "(" })),
            output: None,
            script_content: "x".to_string(),
            workspace_id: Some("default".to_string()),
            project_id: None,
        });
        assert!(err.is_err());

        // Delete removes it.
        orch.delete_workflow_config("my-flow", None, Some("default"))
            .unwrap();
        assert!(orch
            .get_workflow_config_detail("my-flow", None)
            .unwrap()
            .is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn copy_workspace_into_project_shadows_by_id() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("cairn-home");
        std::fs::create_dir_all(&config_dir).unwrap();
        let project_dir = temp.path().join("proj");
        std::fs::create_dir_all(&project_dir).unwrap();
        write_workflow(
            &config_dir.join("workflows").join("shared"),
            "name: Shared\ndescription: ws\n",
        );
        let orch = orch_with_project(&config_dir, &project_dir).await;

        let copied = orch
            .copy_workflow_config("shared", None, "shared", Some("proj1"), None)
            .unwrap();
        assert_eq!(copied.project_id.as_deref(), Some("proj1"));
        assert!(project_dir
            .join(".cairn")
            .join("workflows")
            .join("shared")
            .join("main.ts")
            .exists());

        // A second copy onto the existing project package is rejected.
        assert!(orch
            .copy_workflow_config("shared", None, "shared", Some("proj1"), None)
            .is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn copy_preserves_exact_source_id() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("cairn-home");
        std::fs::create_dir_all(&config_dir).unwrap();
        let project_dir = temp.path().join("proj");
        std::fs::create_dir_all(&project_dir).unwrap();
        // A manually-authored workspace workflow whose id is NOT a `slugify`
        // output (`slugify("RepoAudit") == "repoaudit"`).
        write_workflow(
            &config_dir.join("workflows").join("RepoAudit"),
            "name: Repo Audit\ndescription: ws\n",
        );
        let orch = orch_with_project(&config_dir, &project_dir).await;

        // Copy-to-edit passes the exact source id; the project copy shadows it
        // byte-for-byte — no normalization that would change `RepoAudit`.
        let copied = orch
            .copy_workflow_config("RepoAudit", None, "RepoAudit", Some("proj1"), None)
            .unwrap();
        assert_eq!(copied.id, "RepoAudit");
        assert_eq!(copied.project_id.as_deref(), Some("proj1"));
        // The destination directory is named with the EXACT source id (not a
        // slug). Checked via the directory entry name rather than path existence,
        // since a case-insensitive filesystem would resolve a `repoaudit` path to
        // the same directory and hide a normalization bug.
        let entries: Vec<String> = std::fs::read_dir(project_dir.join(".cairn").join("workflows"))
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries, vec!["RepoAudit".to_string()]);
        assert!(project_dir
            .join(".cairn")
            .join("workflows")
            .join("RepoAudit")
            .join("main.ts")
            .exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn invalid_package_surfaces_in_invalid_list() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("cairn-home");
        let dir = config_dir.join("workflows").join("broken");
        std::fs::create_dir_all(&dir).unwrap();
        // Manifest present but no script entry -> invalid on load.
        std::fs::write(dir.join("workflow.yaml"), "name: Broken\n").unwrap();
        let project_dir = temp.path().join("proj");
        std::fs::create_dir_all(&project_dir).unwrap();
        let orch = orch_with_project(&config_dir, &project_dir).await;

        // It does not appear in the valid list...
        assert!(orch.list_workflow_configs(None, None).unwrap().is_empty());
        // ...but does appear in the invalid list, keyed by its dir id.
        let invalid = orch.list_invalid_workflow_configs(None, None).unwrap();
        assert_eq!(invalid.len(), 1);
        assert_eq!(invalid[0].id, "broken");
        assert_eq!(invalid[0].entity_type, "workflow");
        assert!(invalid[0].error.contains("Script entry not found"));
    }
}
