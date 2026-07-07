//! Workflow package loading (CAIRN-2487).
//!
//! A workflow is a reusable Bun script that orchestrates ephemeral agent calls
//! and deterministic code. Each workflow is a directory with a `workflow.yaml`
//! manifest and a script entry (default `main.ts`):
//!
//! ```text
//! workflows/{id}/
//!   workflow.yaml     <- required manifest (name, description, args, output, script)
//!   main.ts           <- default script entry (overridable via `script:`)
//! ```
//!
//! - Workspace-scoped: `~/.cairn/workflows/`
//! - Project-scoped: `[project-dir]/.cairn/workflows/` (shadows workspace)
//!
//! This is the minimal package surface for the execution-side build: manifest +
//! script entry, with the args JSON Schema validated at load. The fuller package
//! surface (listing affordances, `whenToUse` surfacing, recipe embedding) is a
//! P5 follow-up.
//!
//! The loader is consumed by the workflow backend (which spawns the resolved
//! `script_path`) and by workflow read dispatch; both land in later stages, so
//! the module is marked dead-code-allowed until then.
#![allow(dead_code)]

use serde::Deserialize;
use std::path::{Path, PathBuf};

use cairn_db::models::OutputSchema;

use super::ConfigResult;

/// The default script entry filename when a manifest omits `script:`.
pub const DEFAULT_SCRIPT: &str = "main.ts";

/// The manifest filename inside a workflow package directory.
pub const MANIFEST_FILE: &str = "workflow.yaml";

/// Whether a workflow binds to the project tree. Declared in `workflow.yaml` as
/// `worktree: none|inherit`; defaults to `none`.
///
/// The worktree decision belongs to the workflow, not the caller: a workflow
/// script knows whether it (and the agent calls it fans out, which inherit its
/// cwd) needs to read the repo. `none` runs the script in a scratch dir; the
/// invocation surface maps this onto `CallWorktree` (see `mcp/handlers/workflows`),
/// where `inherit` shares a worktree-backed caller's tree or mints an ephemeral
/// one under an ambient (no-worktree) caller.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WorkflowWorktreeMode {
    /// Scratch dir, no project-tree binding — the default; right for research /
    /// orchestration workflows that never read the repo.
    #[default]
    None,
    /// Give the workflow the project worktree.
    Inherit,
}

/// Raw `workflow.yaml` manifest, deserialized from YAML.
#[derive(Debug, Clone, Deserialize)]
struct WorkflowManifest {
    name: String,
    #[serde(default)]
    description: String,
    /// Script entry filename, relative to the package dir. Defaults to `main.ts`.
    #[serde(default)]
    script: Option<String>,
    /// JSON Schema (an object) validating the named `args_json` at invocation.
    /// `None` means the workflow takes no declared args.
    #[serde(default)]
    args: Option<serde_json::Value>,
    /// Output schema: a preset name (string) or an inline JSON Schema (mapping),
    /// deserialized into the untagged [`OutputSchema`]. `None` falls back to the
    /// default `return` contract at invocation.
    #[serde(default)]
    output: Option<OutputSchema>,
    /// Whether the workflow binds to the project tree. Defaults to `none`.
    #[serde(default)]
    worktree: WorkflowWorktreeMode,
}

/// A workflow package loaded from a directory.
#[derive(Debug, Clone)]
pub struct FileWorkflow {
    pub id: String,
    pub name: String,
    pub description: String,
    /// The script entry filename (resolved: `main.ts` when the manifest omits it).
    pub script: String,
    /// JSON Schema validating the invocation args, if the workflow declares any.
    pub args_schema: Option<serde_json::Value>,
    /// The workflow's output schema, if declared.
    pub output: Option<OutputSchema>,
    /// Whether the workflow binds to the project tree (default `none`/scratch).
    pub worktree: WorkflowWorktreeMode,
    pub is_project_scoped: bool,
    /// Path to the package directory.
    pub dir_path: PathBuf,
    /// Absolute path to the resolved script entry (`dir_path/script`).
    pub script_path: PathBuf,
}

/// List all workflow packages from the workspace and project config dirs.
///
/// A directory qualifies as a workflow package if it contains a `workflow.yaml`.
/// Project-scoped packages are listed before workspace ones (project shadows
/// workspace, matching skills/agents/recipes).
pub fn list_workflows(
    config_dir: &Path,
    project_path: Option<&Path>,
) -> Result<Vec<ConfigResult<FileWorkflow>>, String> {
    let mut results = vec![];
    for (dir, is_project_scoped) in
        super::config_root_subdirs(config_dir, project_path, "workflows")
    {
        if dir.exists() && dir.is_dir() {
            scan_workflows_dir(&dir, is_project_scoped, &mut results)?;
        }
    }
    Ok(results)
}

fn scan_workflows_dir(
    dir: &Path,
    is_project_scoped: bool,
    results: &mut Vec<ConfigResult<FileWorkflow>>,
) -> Result<(), String> {
    let entries: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| format!("Failed to read workflows directory: {}", e))?
        .filter_map(|e| e.ok())
        .collect();

    for entry in &entries {
        let path = entry.path();
        if path.is_dir() && path.join(MANIFEST_FILE).exists() {
            results.push(load_workflow_dir(&path, is_project_scoped));
        }
    }
    Ok(())
}

/// Get a specific workflow package by id. Project scope shadows workspace: the
/// first matching package (project, then workspace) wins.
pub fn get_workflow(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<Option<FileWorkflow>, String> {
    for (workflows_dir, is_project_scoped) in
        super::config_root_subdirs(config_dir, project_path, "workflows")
    {
        let dir_path = workflows_dir.join(id);
        if dir_path.join(MANIFEST_FILE).exists() {
            return match load_workflow_dir(&dir_path, is_project_scoped) {
                ConfigResult::Ok(workflow) => Ok(Some(workflow)),
                ConfigResult::Err { error, .. } => Err(error),
            };
        }
    }
    Ok(None)
}

fn load_workflow_dir(dir_path: &Path, is_project_scoped: bool) -> ConfigResult<FileWorkflow> {
    let id = match dir_path.file_name().and_then(|n| n.to_str()) {
        Some(name) => name.to_string(),
        None => {
            return ConfigResult::Err {
                path: dir_path.to_path_buf(),
                error: "Could not determine workflow id from directory name".to_string(),
            }
        }
    };

    let manifest_path = dir_path.join(MANIFEST_FILE);
    let content = match std::fs::read_to_string(&manifest_path) {
        Ok(c) => c,
        Err(e) => {
            return ConfigResult::Err {
                path: manifest_path,
                error: format!("Failed to read {MANIFEST_FILE}: {e}"),
            }
        }
    };

    let manifest: WorkflowManifest = match serde_yaml::from_str(&content) {
        Ok(m) => m,
        Err(e) => {
            return ConfigResult::Err {
                path: manifest_path,
                error: format!("Invalid {MANIFEST_FILE}: {e}"),
            }
        }
    };

    // Reject a malformed args JSON Schema at load, so a broken package surfaces
    // on discovery rather than at invocation time.
    if let Some(schema) = &manifest.args {
        if let Err(e) = jsonschema::validator_for(schema) {
            return ConfigResult::Err {
                path: manifest_path,
                error: format!("Invalid args schema (not a valid JSON Schema): {e}"),
            };
        }
    }

    let script = manifest
        .script
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_SCRIPT.to_string());
    let script_path = dir_path.join(&script);

    ConfigResult::Ok(FileWorkflow {
        id,
        name: manifest.name,
        description: manifest.description,
        script,
        args_schema: manifest.args,
        output: manifest.output,
        worktree: manifest.worktree,
        is_project_scoped,
        dir_path: dir_path.to_path_buf(),
        script_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn write_workflow(dir: &Path, manifest: &str) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join(MANIFEST_FILE), manifest).unwrap();
    }

    #[test]
    fn loads_a_basic_workflow_with_defaults() {
        let temp = tempdir().unwrap();
        let dir = temp.path().join("workflows").join("deep-research");
        write_workflow(
            &dir,
            "name: Deep Research\ndescription: A quorum of researchers.\n",
        );
        match load_workflow_dir(&dir, false) {
            ConfigResult::Ok(w) => {
                assert_eq!(w.id, "deep-research");
                assert_eq!(w.name, "Deep Research");
                assert_eq!(w.script, DEFAULT_SCRIPT);
                assert_eq!(w.script_path, dir.join("main.ts"));
                assert!(w.args_schema.is_none());
                assert!(w.output.is_none());
            }
            ConfigResult::Err { error, .. } => panic!("{error}"),
        }
    }

    #[test]
    fn worktree_defaults_to_none_and_parses_inherit() {
        let temp = tempdir().unwrap();
        // Default when omitted.
        let none_dir = temp.path().join("workflows").join("research");
        write_workflow(&none_dir, "name: Research\ndescription: d\n");
        match load_workflow_dir(&none_dir, false) {
            ConfigResult::Ok(w) => assert_eq!(w.worktree, WorkflowWorktreeMode::None),
            ConfigResult::Err { error, .. } => panic!("{error}"),
        }
        // Explicit inherit.
        let inherit_dir = temp.path().join("workflows").join("repo-reader");
        write_workflow(
            &inherit_dir,
            "name: Repo Reader\ndescription: d\nworktree: inherit\n",
        );
        match load_workflow_dir(&inherit_dir, false) {
            ConfigResult::Ok(w) => assert_eq!(w.worktree, WorkflowWorktreeMode::Inherit),
            ConfigResult::Err { error, .. } => panic!("{error}"),
        }
    }

    #[test]
    fn parses_args_output_preset_and_custom_script() {
        let temp = tempdir().unwrap();
        let dir = temp.path().join("workflows").join("wf");
        write_workflow(
            &dir,
            "name: WF\ndescription: d\nscript: run.ts\nargs:\n  type: object\n  properties:\n    topic:\n      type: string\n  required: [topic]\noutput: review\n",
        );
        match load_workflow_dir(&dir, false) {
            ConfigResult::Ok(w) => {
                assert_eq!(w.script, "run.ts");
                let schema = w.args_schema.unwrap();
                assert_eq!(schema["type"], "object");
                assert_eq!(schema["required"][0], "topic");
                assert_eq!(w.output, Some(OutputSchema::Preset("review".to_string())));
            }
            ConfigResult::Err { error, .. } => panic!("{error}"),
        }
    }

    #[test]
    fn inline_output_schema_deserializes_as_custom() {
        let temp = tempdir().unwrap();
        let dir = temp.path().join("workflows").join("wf");
        write_workflow(
            &dir,
            "name: WF\ndescription: d\noutput:\n  type: object\n  properties:\n    score:\n      type: number\n  required: [score]\n",
        );
        match load_workflow_dir(&dir, false) {
            ConfigResult::Ok(w) => match w.output {
                Some(OutputSchema::Custom(v)) => assert_eq!(v["type"], "object"),
                other => panic!("expected custom output schema, got {other:?}"),
            },
            ConfigResult::Err { error, .. } => panic!("{error}"),
        }
    }

    #[test]
    fn malformed_args_schema_is_rejected() {
        let temp = tempdir().unwrap();
        let dir = temp.path().join("workflows").join("wf");
        // A `pattern` with an unclosed regex group is an invalid JSON Schema.
        write_workflow(
            &dir,
            "name: WF\ndescription: d\nargs:\n  type: string\n  pattern: \"(\"\n",
        );
        assert!(matches!(
            load_workflow_dir(&dir, false),
            ConfigResult::Err { .. }
        ));
    }

    #[test]
    fn list_skips_dirs_without_a_manifest() {
        let temp = tempdir().unwrap();
        let wd = temp.path().join("workflows");
        write_workflow(&wd.join("valid"), "name: Valid\ndescription: d\n");
        std::fs::create_dir_all(wd.join("empty-dir")).unwrap();

        let results = list_workflows(temp.path(), None).unwrap();
        assert_eq!(results.len(), 1);
        match &results[0] {
            ConfigResult::Ok(w) => assert_eq!(w.id, "valid"),
            ConfigResult::Err { error, .. } => panic!("{error}"),
        }
    }

    #[test]
    fn get_workflow_project_shadows_workspace() {
        let temp = tempdir().unwrap();
        let cfg = temp.path();
        write_workflow(
            &cfg.join("workflows/wf"),
            "name: WF\ndescription: Workspace version\n",
        );
        let proj = temp.path().join("project");
        write_workflow(
            &proj.join(".cairn/workflows/wf"),
            "name: WF\ndescription: Project version\n",
        );

        let w = get_workflow(cfg, "wf", Some(&proj)).unwrap().unwrap();
        assert_eq!(w.description, "Project version");
        assert!(w.is_project_scoped);
    }

    #[test]
    fn get_workflow_missing_is_none() {
        let temp = tempdir().unwrap();
        assert!(get_workflow(temp.path(), "nope", None).unwrap().is_none());
    }
}
