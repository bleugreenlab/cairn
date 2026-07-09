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

use serde::{Deserialize, Serialize};
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
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WorkflowWorktreeMode {
    /// Scratch dir, no project-tree binding — the default; right for research /
    /// orchestration workflows that never read the repo.
    #[default]
    None,
    /// Give the workflow the project worktree.
    Inherit,
}

impl WorkflowWorktreeMode {
    /// The manifest string for this mode (`"none"` | `"inherit"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkflowWorktreeMode::None => "none",
            WorkflowWorktreeMode::Inherit => "inherit",
        }
    }

    /// Parse a manifest worktree string; unknown values yield `None`.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(WorkflowWorktreeMode::None),
            "inherit" => Some(WorkflowWorktreeMode::Inherit),
            _ => None,
        }
    }
}

/// Serializable `workflow.yaml` manifest, for writing a package from the editor.
/// The inverse of [`WorkflowManifest`]: only the fields the settings surface
/// authors, always emitting `script` and `worktree` so the effective shape is
/// visible on disk.
#[derive(Debug, Serialize)]
struct WorkflowManifestOut<'a> {
    name: &'a str,
    description: &'a str,
    script: &'a str,
    worktree: WorkflowWorktreeMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    args: Option<&'a serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output: Option<&'a OutputSchema>,
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

/// Parse and validate a `workflow.yaml` manifest string. This is the single
/// validation path shared by the loader ([`load_workflow_dir`]) and the settings
/// save path ([`save_workflow`]): an invalid manifest (malformed YAML or an
/// invalid args JSON Schema) is rejected identically whether the package is
/// discovered on disk or submitted from the editor, so a bad save never writes a
/// package the loader would later reject.
fn parse_workflow_manifest(content: &str) -> Result<WorkflowManifest, String> {
    let manifest: WorkflowManifest =
        serde_yaml::from_str(content).map_err(|e| format!("Invalid {MANIFEST_FILE}: {e}"))?;
    if let Some(schema) = &manifest.args {
        if let Err(e) = jsonschema::validator_for(schema) {
            return Err(format!(
                "Invalid args schema (not a valid JSON Schema): {e}"
            ));
        }
    }
    Ok(manifest)
}

/// The fields needed to author a workflow package from the settings editor.
pub struct SaveWorkflowSpec<'a> {
    pub id: &'a str,
    pub name: &'a str,
    pub description: &'a str,
    /// Script entry filename (a plain filename, no path separators).
    pub script: &'a str,
    pub worktree: WorkflowWorktreeMode,
    pub args_schema: Option<&'a serde_json::Value>,
    pub output: Option<&'a OutputSchema>,
    pub script_content: &'a str,
    pub is_project_scoped: bool,
}

/// Write a workflow package (`workflow.yaml` + the script entry) to the resolved
/// scope root. The manifest is validated through [`parse_workflow_manifest`]
/// BEFORE anything is written, so an invalid args schema is a save error rather
/// than a corrupt package on disk (a re-save of an existing package is never left
/// half-written). Returns the package directory path.
pub fn save_workflow(
    config_dir: &Path,
    spec: &SaveWorkflowSpec,
    project_path: Option<&Path>,
) -> Result<PathBuf, String> {
    if spec.id.trim().is_empty() {
        return Err("Workflow id must not be empty".to_string());
    }
    // The script must live inside the package dir: reject path separators and the
    // parent-dir escape so a save can never write outside the package.
    let script = if spec.script.trim().is_empty() {
        DEFAULT_SCRIPT
    } else {
        spec.script.trim()
    };
    if script.contains('/') || script.contains('\\') || script.contains("..") {
        return Err(format!("Script entry must be a plain filename: {script}"));
    }

    let workflows_dir = if spec.is_project_scoped {
        let proj = project_path.ok_or("Project path required for project-scoped workflow")?;
        proj.join(".cairn").join("workflows")
    } else {
        config_dir.join("workflows")
    };
    let dir = workflows_dir.join(spec.id);

    let manifest_out = WorkflowManifestOut {
        name: spec.name,
        description: spec.description,
        script,
        worktree: spec.worktree,
        args: spec.args_schema,
        output: spec.output,
    };
    let yaml = serde_yaml::to_string(&manifest_out)
        .map_err(|e| format!("Failed to serialize {MANIFEST_FILE}: {e}"))?;

    // Round-trip through the shared load/validation path before writing.
    parse_workflow_manifest(&yaml)?;

    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("Failed to create workflow directory: {e}"))?;
    std::fs::write(dir.join(MANIFEST_FILE), yaml)
        .map_err(|e| format!("Failed to write {MANIFEST_FILE}: {e}"))?;
    std::fs::write(dir.join(script), spec.script_content)
        .map_err(|e| format!("Failed to write script {script}: {e}"))?;

    Ok(dir)
}

/// Read the full text of a workflow's resolved script entry.
pub fn read_workflow_script(workflow: &FileWorkflow) -> Result<String, String> {
    std::fs::read_to_string(&workflow.script_path).map_err(|e| {
        format!(
            "Failed to read script {}: {e}",
            workflow.script_path.display()
        )
    })
}

/// Delete a workflow package directory. Project scope is removed first when a
/// project path is given (mirrors skills), else the workspace copy.
pub fn delete_workflow(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<(), String> {
    if let Some(proj) = project_path {
        let dir = proj.join(".cairn").join("workflows").join(id);
        if dir.is_dir() {
            return std::fs::remove_dir_all(&dir)
                .map_err(|e| format!("Failed to delete workflow directory: {e}"));
        }
    }
    let dir = config_dir.join("workflows").join(id);
    if dir.is_dir() {
        std::fs::remove_dir_all(&dir)
            .map_err(|e| format!("Failed to delete workflow directory: {e}"))?;
    }
    Ok(())
}

/// Recursively copy a workflow package directory (manifest, script, and any
/// supporting files) to a new location. Used to shadow/promote a package across
/// scopes.
pub fn copy_workflow_package(source_dir: &Path, dest_dir: &Path) -> Result<(), String> {
    copy_dir_recursive(source_dir, dest_dir)
        .map_err(|e| format!("Failed to copy workflow package: {e}"))
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
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

    let manifest = match parse_workflow_manifest(&content) {
        Ok(m) => m,
        Err(error) => {
            return ConfigResult::Err {
                path: manifest_path,
                error,
            }
        }
    };

    let script = manifest
        .script
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_SCRIPT.to_string());
    let script_path = dir_path.join(&script);

    // A manifest that names (or defaults to) a script entry that does not exist
    // lists as valid but dies at runtime with module-not-found, after a workflow
    // node was already created. Reject it on discovery under the same principle
    // as the args-schema check — a broken package surfaces here, not at run time.
    if !script_path.exists() {
        return ConfigResult::Err {
            path: manifest_path,
            error: format!("Script entry not found: {}", script_path.display()),
        };
    }

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
        // The loader now requires the resolved script entry to exist, so seed the
        // default one; tests using a custom `script:` write that file too.
        std::fs::write(dir.join(DEFAULT_SCRIPT), "// test script\n").unwrap();
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
        std::fs::write(dir.join("run.ts"), "// run\n").unwrap();
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
    /// The built-in `fan-out` workflow shipped in-repo under `src-tauri/workflows/`
    /// is seeded into every workspace by the bundle sync (`BUNDLE_RESOURCE_DIRS`
    /// includes `workflows`). Guard that the SHIPPED package actually loads: its
    /// manifest parses, its args JSON Schema validates, and it declares a
    /// description. Verifying the shipped file directly beats dogfooding through
    /// the running host, which serves a stale binary and a stale synced copy.
    #[test]
    fn bundled_fan_out_workflow_loads() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../workflows/fan-out")
            .canonicalize()
            .expect("bundled fan-out workflow dir exists at src-tauri/workflows/fan-out");
        match load_workflow_dir(&dir, false) {
            ConfigResult::Ok(w) => {
                assert_eq!(w.id, "fan-out");
                assert!(!w.description.trim().is_empty());
                // The orchestrator only dispatches, so the manifest binds no tree.
                assert_eq!(w.worktree, WorkflowWorktreeMode::None);
                // The args schema is present and validated as a JSON Schema at load.
                let schema = w.args_schema.expect("fan-out declares an args schema");
                assert_eq!(schema["required"][0], "prompt");
                assert_eq!(schema["required"][1], "items");
                // The `agent` arg carries the `format: agent` UI hint that steers
                // the Run dialog to a picker; lock the convention here.
                assert_eq!(schema["properties"]["agent"]["format"], "agent");
            }
            ConfigResult::Err { error, .. } => panic!("bundled fan-out failed to load: {error}"),
        }
    }

    #[test]
    fn missing_script_entry_is_rejected() {
        let temp = tempdir().unwrap();
        let dir = temp.path().join("workflows").join("no-script");
        std::fs::create_dir_all(&dir).unwrap();
        // Manifest only, no main.ts — the loader must reject this on discovery.
        std::fs::write(dir.join(MANIFEST_FILE), "name: No Script\ndescription: d\n").unwrap();
        match load_workflow_dir(&dir, false) {
            ConfigResult::Err { error, .. } => {
                assert!(error.contains("Script entry not found"), "{error}")
            }
            ConfigResult::Ok(_) => panic!("expected missing-script rejection"),
        }
    }

    #[test]
    fn save_workflow_roundtrips_manifest_and_script() {
        let temp = tempdir().unwrap();
        let cfg = temp.path();
        let args = serde_json::json!({
            "type": "object",
            "properties": { "topic": { "type": "string" } },
            "required": ["topic"],
        });
        let spec = SaveWorkflowSpec {
            id: "greet",
            name: "Greeter",
            description: "Says hi",
            script: DEFAULT_SCRIPT,
            worktree: WorkflowWorktreeMode::Inherit,
            args_schema: Some(&args),
            output: None,
            script_content: "export {};\n// hello\n",
            is_project_scoped: false,
        };
        let dir = save_workflow(cfg, &spec, None).unwrap();
        assert!(dir.join(MANIFEST_FILE).exists());
        assert!(dir.join(DEFAULT_SCRIPT).exists());

        let loaded = get_workflow(cfg, "greet", None).unwrap().unwrap();
        assert_eq!(loaded.name, "Greeter");
        assert_eq!(loaded.description, "Says hi");
        assert_eq!(loaded.worktree, WorkflowWorktreeMode::Inherit);
        assert_eq!(loaded.args_schema.as_ref().unwrap()["required"][0], "topic");
        assert_eq!(
            read_workflow_script(&loaded).unwrap(),
            "export {};\n// hello\n"
        );
    }

    #[test]
    fn save_workflow_rejects_invalid_args_schema_without_writing() {
        let temp = tempdir().unwrap();
        let cfg = temp.path();
        // `pattern` with an unclosed group is an invalid JSON Schema.
        let bad = serde_json::json!({ "type": "string", "pattern": "(" });
        let spec = SaveWorkflowSpec {
            id: "bad",
            name: "Bad",
            description: "",
            script: DEFAULT_SCRIPT,
            worktree: WorkflowWorktreeMode::None,
            args_schema: Some(&bad),
            output: None,
            script_content: "x",
            is_project_scoped: false,
        };
        assert!(save_workflow(cfg, &spec, None).is_err());
        // Validation runs before any write, so nothing lands on disk.
        assert!(!cfg.join("workflows").join("bad").exists());
    }

    #[test]
    fn save_workflow_rejects_script_with_path_separator() {
        let temp = tempdir().unwrap();
        let spec = SaveWorkflowSpec {
            id: "x",
            name: "X",
            description: "",
            script: "../evil.ts",
            worktree: WorkflowWorktreeMode::None,
            args_schema: None,
            output: None,
            script_content: "x",
            is_project_scoped: false,
        };
        assert!(save_workflow(temp.path(), &spec, None).is_err());
    }

    #[test]
    fn save_then_delete_removes_package() {
        let temp = tempdir().unwrap();
        let cfg = temp.path();
        let spec = SaveWorkflowSpec {
            id: "gone",
            name: "Gone",
            description: "",
            script: DEFAULT_SCRIPT,
            worktree: WorkflowWorktreeMode::None,
            args_schema: None,
            output: None,
            script_content: "x",
            is_project_scoped: false,
        };
        save_workflow(cfg, &spec, None).unwrap();
        assert!(cfg.join("workflows").join("gone").exists());
        delete_workflow(cfg, "gone", None).unwrap();
        assert!(!cfg.join("workflows").join("gone").exists());
    }

    #[test]
    fn copy_workflow_package_copies_all_files() {
        let temp = tempdir().unwrap();
        let src = temp.path().join("src");
        std::fs::create_dir_all(src.join("lib")).unwrap();
        std::fs::write(src.join(MANIFEST_FILE), "name: S\n").unwrap();
        std::fs::write(src.join("main.ts"), "a").unwrap();
        std::fs::write(src.join("lib/util.ts"), "u").unwrap();
        let dst = temp.path().join("dst");
        copy_workflow_package(&src, &dst).unwrap();
        assert!(dst.join(MANIFEST_FILE).exists());
        assert!(dst.join("main.ts").exists());
        assert!(dst.join("lib/util.ts").exists());
    }
}
