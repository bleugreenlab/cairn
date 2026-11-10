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
//! One package per layer (`workflows/<id>/` in a project's `.cairn/`, the
//! workspace, or an installed pack), resolved nearest-first through
//! [`cairn_config::library`].
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
use serde_json::{json, Map, Value};
use std::path::{Path, PathBuf};

use cairn_config::library::{
    self, LibraryItem, LibraryKind, LibraryRef, LibraryRoots, Listed, Origin, ResolveError,
    Resolved,
};
use cairn_db::models::OutputSchema;

use super::ConfigResult;

/// The default script entry filename when a manifest omits `script:`.
pub const DEFAULT_SCRIPT: &str = "main.ts";

/// The manifest filename inside a workflow package directory.
pub const MANIFEST_FILE: &str = "workflow.yaml";

/// Whether a workflow inherits the caller's durable branch coordinate. Declared
/// in `workflow.yaml` as `branch: none|inherit`; defaults to `none`.
///
/// The branch decision belongs to the workflow, not the caller. `none` runs the
/// script as repository-independent orchestration. `inherit` gives the workflow
/// and its delegated calls the authenticated caller's branch and logical head;
/// neither mode creates or binds a dedicated checkout.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WorkflowBranchMode {
    /// Scratch dir, no project-tree binding — the default; right for research /
    /// orchestration workflows that never read the repo.
    #[default]
    None,
    /// Inherit the caller's durable branch and logical head.
    Inherit,
}

impl WorkflowBranchMode {
    /// The manifest string for this mode (`"none"` | `"inherit"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkflowBranchMode::None => "none",
            WorkflowBranchMode::Inherit => "inherit",
        }
    }

    /// Parse a manifest branch string; unknown values yield `None`.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(WorkflowBranchMode::None),
            "inherit" => Some(WorkflowBranchMode::Inherit),
            _ => None,
        }
    }
}

/// Serializable `workflow.yaml` manifest, for writing a package from the editor.
/// The inverse of [`WorkflowManifest`]: only the fields the settings surface
/// authors, always emitting `script` and `branch` so the effective shape is
/// visible on disk.
#[derive(Debug, Serialize)]
struct WorkflowManifestOut<'a> {
    name: &'a str,
    description: &'a str,
    script: &'a str,
    branch: WorkflowBranchMode,
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
    /// Whether the workflow inherits the caller's branch. Defaults to `none`.
    #[serde(default)]
    branch: WorkflowBranchMode,
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
    /// Whether the workflow inherits the caller's branch (default `none`).
    pub branch: WorkflowBranchMode,
    /// The layer this workflow was read from, or the layer a new one is for.
    pub origin: Origin,
    /// Path to the package directory.
    pub dir_path: PathBuf,
    /// Absolute path to the resolved script entry (`dir_path/script`).
    pub script_path: PathBuf,
}

impl FileWorkflow {
    pub fn is_project_scoped(&self) -> bool {
        self.origin.is_project()
    }
}

/// A workflow's authority is the code it runs on this machine and whether
/// that code gets the caller's branch. The manifest's name, description and
/// schemas are prose and shape, so the package is hashed without it.
impl LibraryItem for FileWorkflow {
    fn library_id(&self) -> &str {
        &self.id
    }

    /// Every field is named below, so one added to `FileWorkflow` does not
    /// compile until it is classified.
    fn authority(&self) -> Map<String, Value> {
        let FileWorkflow {
            id: _,
            // Prose.
            name: _,
            description: _,
            script,
            // The shape of what it is handed and what it returns.
            args_schema: _,
            output: _,
            branch,
            origin: _,
            // The package whose code is hashed below.
            dir_path,
            // Derived from `dir_path` and `script`.
            script_path: _,
        } = self;
        let mut fields = Map::new();
        fields.insert("script".into(), json!(script));
        fields.insert("code".into(), json!(package_code_hash(dir_path)));
        fields.insert("branch".into(), json!(branch.as_str()));
        fields
    }
}

/// The hash of every file in a workflow package but its manifest.
fn package_code_hash(dir: &Path) -> String {
    let mut entries: Vec<(String, String)> = Vec::new();
    let files = match cairn_platform::services::guarded_tree_files(dir) {
        Ok(files) => files,
        // An unreadable package never matches a confirmation.
        Err(error) => return format!("unreadable: {error}"),
    };
    for file in files {
        let relative = file.relative_path.to_string_lossy().replace('\\', "/");
        if relative == MANIFEST_FILE || cairn_packs::is_os_metadata_path(&file.relative_path) {
            continue;
        }
        let hash = std::fs::read(&file.resolved_path)
            .map(|bytes| cairn_packs::sha256_bytes(&bytes))
            .unwrap_or_else(|error| format!("unreadable: {error}"));
        entries.push((relative, hash));
    }
    entries.sort();
    cairn_packs::sha256_bytes(
        entries
            .iter()
            .map(|(path, hash)| format!("{path}\0{hash}\n"))
            .collect::<String>()
            .as_bytes(),
    )
}

fn load_workflow_in(dir: &Path, id: &str, origin: &Origin) -> Option<ConfigResult<FileWorkflow>> {
    let dir_path = dir.join(id);
    dir_path
        .join(MANIFEST_FILE)
        .exists()
        .then(|| load_workflow_dir(&dir_path, origin.clone()))
}

fn enumerate_workflows(
    dir: &Path,
    origin: &Origin,
) -> Result<Vec<(String, ConfigResult<FileWorkflow>)>, String> {
    let entries: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| format!("Failed to read workflows directory: {}", e))?
        .filter_map(|e| e.ok())
        .collect();
    Ok(entries
        .iter()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir() && path.join(MANIFEST_FILE).exists())
        .filter_map(|path| {
            let id = path.file_name()?.to_str()?.to_string();
            Some((id, load_workflow_dir(&path, origin.clone())))
        })
        .collect())
}

/// Every workflow package across the layers, with its origin and what it
/// shadows. A directory qualifies if it contains a `workflow.yaml`.
pub fn list_workflow_rows(roots: &LibraryRoots) -> Result<Vec<Listed<FileWorkflow>>, String> {
    roots.list(LibraryKind::Workflow, enumerate_workflows)
}

/// The workflows in force, one per name.
pub fn list_workflows(roots: &LibraryRoots) -> Result<Vec<ConfigResult<FileWorkflow>>, String> {
    Ok(library::active(list_workflow_rows(roots)?))
}

/// Resolve a workflow reference (an id or
/// `cairn://packs/<pack>/workflows/<id>`) nearest-first.
pub fn resolve_workflow(
    roots: &LibraryRoots,
    reference: &str,
) -> Result<Resolved<FileWorkflow>, ResolveError> {
    let parsed = LibraryRef::parse(LibraryKind::Workflow, reference).map_err(|error| {
        ResolveError::Invalid {
            path: PathBuf::new(),
            error,
            origin: None,
        }
    })?;
    roots.resolve(LibraryKind::Workflow, &parsed, load_workflow_in)
}

/// Resolve workflow `id` in one layer only (see [`LibraryRoots::resolve_in`]).
pub fn resolve_workflow_in(
    roots: &LibraryRoots,
    layer: &Origin,
    id: &str,
) -> Result<Resolved<FileWorkflow>, ResolveError> {
    roots.resolve_in(LibraryKind::Workflow, layer, id, load_workflow_in)
}

/// The workflow a reference resolves to, `None` when nothing defines it.
pub fn get_workflow(roots: &LibraryRoots, id: &str) -> Result<Option<FileWorkflow>, String> {
    match resolve_workflow(roots, id) {
        Ok(resolved) => Ok(Some(resolved.item)),
        Err(error) if error.is_not_found() => Ok(None),
        Err(error) => Err(error.to_string()),
    }
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
    pub branch: WorkflowBranchMode,
    pub args_schema: Option<&'a serde_json::Value>,
    pub output: Option<&'a OutputSchema>,
    pub script_content: &'a str,
    /// The layer to write the package into: the workspace or a project.
    pub layer: &'a Origin,
}

/// Write a workflow package (`workflow.yaml` + the script entry) to the resolved
/// scope root. The manifest is validated through [`parse_workflow_manifest`]
/// BEFORE anything is written, so an invalid args schema is a save error rather
/// than a corrupt package on disk (a re-save of an existing package is never left
/// half-written). Returns the package directory path.
pub fn save_workflow(roots: &LibraryRoots, spec: &SaveWorkflowSpec) -> Result<PathBuf, String> {
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

    if let Origin::Pack { id: pack } = spec.layer {
        return Err(library::pack_edit_refusal(
            LibraryKind::Workflow,
            spec.id,
            pack,
        ));
    }
    let workflows_dir = roots
        .layer_dir(LibraryKind::Workflow, spec.layer)
        .ok_or("Project path required for project-scoped workflow")?;
    let dir = workflows_dir.join(spec.id);

    let manifest_out = WorkflowManifestOut {
        name: spec.name,
        description: spec.description,
        script,
        branch: spec.branch,
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

/// Delete the workflow `id` resolves to, or the one in `layer` when named. A
/// workspace or project package is deleted outright; a pack's own workflow is
/// recorded as removed from its pack. Returns the path that changed (`None`
/// when nothing defined it).
pub fn delete_workflow(
    roots: &LibraryRoots,
    id: &str,
    layer: Option<&Origin>,
) -> Result<Option<PathBuf>, String> {
    let (origin, dir) = match layer {
        Some(origin @ (Origin::Project { .. } | Origin::Workspace)) => {
            let dir = roots
                .layer_dir(LibraryKind::Workflow, origin)
                .ok_or_else(|| format!("No {origin} layer to delete workflow `{id}` from"))?
                .join(id);
            if !dir.is_dir() {
                return Ok(None);
            }
            (origin.clone(), dir)
        }
        Some(origin) => match resolve_workflow_in(roots, origin, id) {
            Ok(resolved) => (resolved.origin, resolved.item.dir_path),
            Err(ResolveError::Invalid {
                path,
                origin: Some(origin),
                ..
            }) => (origin, path.parent().map(Path::to_path_buf).unwrap_or(path)),
            Err(error) if error.is_not_found() => return Ok(None),
            Err(error) => return Err(error.to_string()),
        },
        None => match resolve_workflow(roots, id) {
            Ok(resolved) => (resolved.origin, resolved.item.dir_path),
            Err(ResolveError::Invalid {
                path,
                origin: Some(origin),
                ..
            }) => (origin, path.parent().map(Path::to_path_buf).unwrap_or(path)),
            Err(error) if error.is_not_found() => return Ok(None),
            Err(error) => return Err(error.to_string()),
        },
    };
    library::remove_definition(roots, LibraryKind::Workflow, id, &origin, &dir).map(Some)
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

fn load_workflow_dir(dir_path: &Path, origin: Origin) -> ConfigResult<FileWorkflow> {
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
    // The entry must be a file of this package. The confirmed authority of a
    // workflow is the hash of its package directory, and execution uploads the
    // entry's directory, so an entry outside the package (an absolute path, a
    // `..` escape, or a symlink out) would run bytes no confirmation covered.
    if script.contains('/') || script.contains('\\') || script.contains("..") {
        return ConfigResult::Err {
            path: manifest_path,
            error: format!("Script entry must be a plain filename inside the package: {script}"),
        };
    }
    let script_path = dir_path.join(&script);
    if let (Ok(entry), Ok(package)) = (script_path.canonicalize(), dir_path.canonicalize()) {
        if entry.parent() != Some(package.as_path()) {
            return ConfigResult::Err {
                path: manifest_path,
                error: format!(
                    "Script entry {} resolves outside the package to {}",
                    script,
                    entry.display()
                ),
            };
        }
    }

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
        branch: manifest.branch,
        origin,
        dir_path: dir_path.to_path_buf(),
        script_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use cairn_config::library::{LibraryRoots, Origin};
    use tempfile::tempdir;

    /// An entry outside the package would run bytes the confirmed package hash
    /// never covered, so the loader refuses it however it is spelled.
    #[test]
    fn a_script_entry_outside_the_package_is_refused() {
        let temp = tempdir().unwrap();
        let outside = temp.path().join("outside.ts");
        std::fs::write(&outside, "// elsewhere\n").unwrap();
        for script in [
            "../outside.ts".to_string(),
            outside.to_string_lossy().to_string(),
            "sub/main.ts".to_string(),
        ] {
            let dir = temp.path().join("workflows").join("w");
            let _ = std::fs::remove_dir_all(&dir);
            write_workflow(&dir, &format!("name: W\nscript: {script}\n"));
            assert!(
                matches!(
                    load_workflow_dir(&dir, Origin::Workspace),
                    ConfigResult::Err { .. }
                ),
                "{script} must be refused"
            );
        }
        #[cfg(unix)]
        {
            let dir = temp.path().join("workflows").join("linked");
            write_workflow(&dir, "name: L\nscript: entry.ts\n");
            std::os::unix::fs::symlink(&outside, dir.join("entry.ts")).unwrap();
            assert!(matches!(
                load_workflow_dir(&dir, Origin::Workspace),
                ConfigResult::Err { .. }
            ));
        }
    }

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
        match load_workflow_dir(&dir, Origin::Workspace) {
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
    fn branch_defaults_to_none_and_parses_inherit() {
        let temp = tempdir().unwrap();
        // Default when omitted.
        let none_dir = temp.path().join("workflows").join("research");
        write_workflow(&none_dir, "name: Research\ndescription: d\n");
        match load_workflow_dir(&none_dir, Origin::Workspace) {
            ConfigResult::Ok(w) => assert_eq!(w.branch, WorkflowBranchMode::None),
            ConfigResult::Err { error, .. } => panic!("{error}"),
        }
        // Explicit inherit.
        let inherit_dir = temp.path().join("workflows").join("repo-reader");
        write_workflow(
            &inherit_dir,
            "name: Repo Reader\ndescription: d\nbranch: inherit\n",
        );
        match load_workflow_dir(&inherit_dir, Origin::Workspace) {
            ConfigResult::Ok(w) => assert_eq!(w.branch, WorkflowBranchMode::Inherit),
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
        match load_workflow_dir(&dir, Origin::Workspace) {
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
        match load_workflow_dir(&dir, Origin::Workspace) {
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
            load_workflow_dir(&dir, Origin::Workspace),
            ConfigResult::Err { .. }
        ));
    }

    #[test]
    fn list_skips_dirs_without_a_manifest() {
        let temp = tempdir().unwrap();
        let wd = temp.path().join("workflows");
        write_workflow(&wd.join("valid"), "name: Valid\ndescription: d\n");
        std::fs::create_dir_all(wd.join("empty-dir")).unwrap();

        let results = list_workflows(&LibraryRoots::workspace(temp.path())).unwrap();
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

        let w = get_workflow(&crate::config::trusted_roots(cfg, &proj), "wf")
            .unwrap()
            .unwrap();
        assert_eq!(w.description, "Project version");
        assert!(w.is_project_scoped());
    }

    #[test]
    fn get_workflow_missing_is_none() {
        let temp = tempdir().unwrap();
        assert!(get_workflow(&LibraryRoots::workspace(temp.path()), "nope")
            .unwrap()
            .is_none());
    }
    /// The built-in `fan-out` workflow shipped in-repo under the `core` pack is
    /// seeded into every workspace when that pack installs (which it does on
    /// every fresh run). Guard that the SHIPPED package actually loads: its
    /// manifest parses, its args JSON Schema validates, and it declares a
    /// description. Verifying the shipped file directly beats dogfooding through
    /// the running host, which serves a stale binary and a stale synced copy.
    #[test]
    fn bundled_fan_out_workflow_loads() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../packs/core/workflows/fan-out")
            .canonicalize()
            .expect("fan-out workflow ships in the core pack");
        match load_workflow_dir(&dir, Origin::Workspace) {
            ConfigResult::Ok(w) => {
                assert_eq!(w.id, "fan-out");
                assert!(!w.description.trim().is_empty());
                // The orchestrator only dispatches, so the manifest binds no tree.
                assert_eq!(w.branch, WorkflowBranchMode::None);
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
        match load_workflow_dir(&dir, Origin::Workspace) {
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
            branch: WorkflowBranchMode::Inherit,
            args_schema: Some(&args),
            output: None,
            script_content: "export {};\n// hello\n",
            layer: &Origin::Workspace,
        };
        let dir = save_workflow(&LibraryRoots::workspace(cfg), &spec).unwrap();
        assert!(dir.join(MANIFEST_FILE).exists());
        assert!(dir.join(DEFAULT_SCRIPT).exists());

        let loaded = get_workflow(&LibraryRoots::workspace(cfg), "greet")
            .unwrap()
            .unwrap();
        assert_eq!(loaded.name, "Greeter");
        assert_eq!(loaded.description, "Says hi");
        assert_eq!(loaded.branch, WorkflowBranchMode::Inherit);
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
            branch: WorkflowBranchMode::None,
            args_schema: Some(&bad),
            output: None,
            script_content: "x",
            layer: &Origin::Workspace,
        };
        assert!(save_workflow(&LibraryRoots::workspace(cfg), &spec).is_err());
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
            branch: WorkflowBranchMode::None,
            args_schema: None,
            output: None,
            script_content: "x",
            layer: &Origin::Workspace,
        };
        assert!(save_workflow(&LibraryRoots::workspace(temp.path()), &spec).is_err());
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
            branch: WorkflowBranchMode::None,
            args_schema: None,
            output: None,
            script_content: "x",
            layer: &Origin::Workspace,
        };
        save_workflow(&LibraryRoots::workspace(cfg), &spec).unwrap();
        assert!(cfg.join("workflows").join("gone").exists());
        delete_workflow(&LibraryRoots::workspace(cfg), "gone", None).unwrap();
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
