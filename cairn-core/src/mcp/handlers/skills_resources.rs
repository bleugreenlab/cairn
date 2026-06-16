//! Skill resources exposed through the canonical cairn:// URI graph.
//!
//! Skills are readable as `cairn://skills` (contextual collection) and
//! `cairn://skills/{id}` (the skill's SKILL.md content, served inline with
//! provenance and links to the `references/`, `scripts/`, and `assets/`
//! package directories). SKILL.md lives at the skill root URI — there is no
//! separate `.../SKILL.md` sub-resource. The explicit project forms
//! `cairn://p/PROJECT/skills[/...]` resolve against a named project's context.
//!
//! This module also exposes the small scope-resolution helpers used by the
//! `write` tool to route skill mutations (create/patch/delete).

use std::path::{Path, PathBuf};

use crate::config::skills::FileSkill;
use crate::config::{skills as config_skills, ConfigResult};
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use cairn_common::uri::{build_project_skill_uri, build_skill_uri, CairnResource};

/// Whether a skill subtree is addressed contextually or under an explicit project.
#[derive(Debug, Clone)]
pub(crate) enum SkillScopeUri {
    Contextual,
    Project(String),
}

impl SkillScopeUri {
    fn skill_uri(&self, id: &str, path: &[String]) -> String {
        match self {
            SkillScopeUri::Contextual => build_skill_uri(id, path),
            SkillScopeUri::Project(project) => build_project_skill_uri(project, id, path),
        }
    }
}

/// The project key + repo path of the current run, when one can be resolved.
pub(crate) async fn current_run_project(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Option<(String, Option<PathBuf>)> {
    let ctx = super::run_context::lookup_run(&orch.db.local, request)
        .await
        .ok()?;
    let path = super::run_context::project_path(&orch.db.local, &ctx.project_id)
        .await
        .ok()
        .flatten()
        .map(PathBuf::from);
    Some((ctx.project_key, path))
}

/// Resolve a project's repo path by key (for explicit `cairn://p/PROJECT/skills`).
pub(crate) async fn project_path_by_key(
    orch: &Orchestrator,
    project_key: &str,
) -> Result<PathBuf, String> {
    let key = project_key.to_uppercase();
    let lookup_key = key.clone();
    let id = orch
        .db
        .local
        .query_text(
            "SELECT id FROM projects WHERE key = ?1 LIMIT 1",
            turso::params![lookup_key.as_str()],
        )
        .await
        .map_err(|e| format!("Failed to load project: {e}"))?
        .ok_or_else(|| format!("No project found with key '{key}'"))?;
    super::run_context::project_path(&orch.db.local, &id)
        .await?
        .map(PathBuf::from)
        .ok_or_else(|| format!("Project '{key}' has no repository path"))
}

/// Resolve a skill for a given scope.
///
/// Contextual scope (no explicit project) resolves project-first then workspace,
/// mirroring `get_skill`. Explicit project scope is **project-only**: it must not
/// fall back to a workspace skill, because the caller asserted *where* the skill
/// lives — silently editing/deleting the shared workspace skill instead would have
/// a blast radius across every project.
pub(crate) fn resolve_skill_for_scope(
    config_dir: &Path,
    skill_id: &str,
    explicit_project: bool,
    project_path: Option<&Path>,
) -> Result<Option<FileSkill>, String> {
    let skill = config_skills::get_skill(config_dir, skill_id, project_path)?;
    if explicit_project {
        // Reject the workspace fallthrough: only a project-scoped match counts.
        Ok(skill.filter(|skill| skill.is_project_scoped))
    } else {
        Ok(skill)
    }
}

/// Resolve a skill-script URI (`cairn://skills/<id>/scripts/<name>` or the
/// project-scoped `cairn://p/<PROJECT>/skills/<id>/scripts/<name>`) to its
/// on-disk path. Reuses skill scope resolution and the package-relative safe
/// path join, so traversal is rejected exactly as for readable skill subpaths.
/// Only paths under `scripts/` are allowed.
pub(crate) async fn resolve_skill_script_path(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    resource: &CairnResource,
) -> Result<PathBuf, String> {
    let (skill_id, path, explicit_project) = match resource {
        CairnResource::Skill { skill_id, path } => (skill_id.clone(), path.clone(), None),
        CairnResource::ProjectSkill {
            project,
            skill_id,
            path,
        } => (skill_id.clone(), path.clone(), Some(project.clone())),
        other => {
            return Err(format!(
                "Not a skill script target: {} (expected cairn://skills/<id>/scripts/<name>)",
                other.to_uri()
            ))
        }
    };

    if path.first().map(String::as_str) != Some("scripts") || path.len() < 2 {
        return Err(format!(
            "Skill script target must address scripts/<name>, got '{}'",
            path.join("/")
        ));
    }

    let project_path: Option<PathBuf> = if let Some(project) = &explicit_project {
        Some(project_path_by_key(orch, project).await?)
    } else {
        current_run_project(orch, request)
            .await
            .and_then(|(_, path)| path)
    };

    let skill = resolve_skill_for_scope(
        &orch.config_dir,
        &skill_id,
        explicit_project.is_some(),
        project_path.as_deref(),
    )?
    .ok_or_else(|| format!("Skill not found: {skill_id}"))?;

    resolve_safe_path(&skill.dir_path, &path)
}

fn scope_label(skill: &FileSkill) -> &'static str {
    if skill.is_project_scoped {
        "project"
    } else {
        "workspace"
    }
}

fn skill_link(skill: &FileSkill, project_key: Option<&str>, path: &[String]) -> String {
    if skill.is_project_scoped {
        match project_key {
            Some(project) => build_project_skill_uri(project, &skill.id, path),
            None => build_skill_uri(&skill.id, path),
        }
    } else {
        build_skill_uri(&skill.id, path)
    }
}

/// Render the skills collection (workspace + project, project shadows workspace by id).
pub(crate) async fn read_skills_collection(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    explicit_project: Option<&str>,
) -> String {
    // Resolve project context: explicit project key + id + path, or current run.
    let (project_id, project_key, project_path): (Option<String>, Option<String>, Option<PathBuf>) =
        if let Some(project) = explicit_project {
            match project_path_by_key(orch, project).await {
                Ok(path) => {
                    let id = super::run_context::project_id_by_key(&orch.db.local, project)
                        .await
                        .ok();
                    (id, Some(project.to_uppercase()), Some(path))
                }
                Err(e) => return e,
            }
        } else {
            match super::run_context::lookup_run(&orch.db.local, request).await {
                Ok(ctx) => {
                    let path = super::run_context::project_path(&orch.db.local, &ctx.project_id)
                        .await
                        .ok()
                        .flatten()
                        .map(PathBuf::from);
                    (Some(ctx.project_id), Some(ctx.project_key), path)
                }
                Err(_) => (None, None, None),
            }
        };

    let skills = match config_skills::list_skills(&orch.config_dir, project_path.as_deref()) {
        Ok(skills) => skills,
        Err(e) => return format!("Error listing skills: {e}"),
    };

    // Project shadows workspace by id; sort by id for deterministic output.
    let mut by_id: std::collections::BTreeMap<String, FileSkill> =
        std::collections::BTreeMap::new();
    for result in skills {
        if let ConfigResult::Ok(skill) = result {
            // config_root_subdirs yields project first, so keep the first
            // occurrence for each id to let project skills shadow workspace.
            by_id.entry(skill.id.clone()).or_insert(skill);
        }
    }

    // Drop inherited workspace skills the project has disabled. A project-scoped
    // skill of the same id has already shadowed it above, so only an inherited
    // (workspace) skill remains to hide.
    if let Some(pid) = project_id.as_deref() {
        if let Ok(disabled) =
            crate::config_disables::list_disabled_keys(&orch.db.local, pid, "skill").await
        {
            by_id.retain(|_, skill| {
                skill.is_project_scoped || !disabled.contains(&skill.id)
            });
        }
    }

    let header = match project_key.as_deref() {
        Some(key) => format!("# Skills — {key} context\n\n"),
        None => "# Skills — workspace\n\n".to_string(),
    };
    let mut out = header;
    out.push_str(&format!("{} skill(s)\n\n", by_id.len()));

    if by_id.is_empty() {
        out.push_str("No skills found.\n\n");
    } else {
        for skill in by_id.values() {
            out.push_str(&format!(
                "- [{}]({}) [{}] — {}\n",
                skill.id,
                skill_link(skill, project_key.as_deref(), &[]),
                scope_label(skill),
                skill.description,
            ));
        }
        out.push('\n');
    }

    let create_uri = match explicit_project {
        Some(project) => cairn_common::uri::build_project_skills_uri(project),
        None => cairn_common::uri::build_skills_uri(),
    };
    out.push_str("## actions\n");
    out.push_str(&format!(
        "- [create skill]({create_uri}): required `payload.name`, `payload.description`, `payload.prompt`; optional `payload.allowedTools`, `payload.model`, `payload.sourceIssue`\n",
    ));
    out
}

/// Resolve and render a skill package given a scope URI context.
pub(crate) async fn read_skill(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    skill_id: &str,
    path: &[String],
    explicit_project: Option<&str>,
) -> String {
    let (project_id, project_path): (Option<String>, Option<PathBuf>) =
        if let Some(project) = explicit_project {
            match project_path_by_key(orch, project).await {
                Ok(path) => {
                    let id = super::run_context::project_id_by_key(&orch.db.local, project)
                        .await
                        .ok();
                    (id, Some(path))
                }
                Err(e) => return e,
            }
        } else {
            match super::run_context::lookup_run(&orch.db.local, request).await {
                Ok(ctx) => {
                    let path = super::run_context::project_path(&orch.db.local, &ctx.project_id)
                        .await
                        .ok()
                        .flatten()
                        .map(PathBuf::from);
                    (Some(ctx.project_id), path)
                }
                Err(_) => (None, None),
            }
        };

    let skill = match resolve_skill_for_scope(
        &orch.config_dir,
        skill_id,
        explicit_project.is_some(),
        project_path.as_deref(),
    ) {
        Ok(Some(skill)) => skill,
        Ok(None) => {
            return match explicit_project {
                Some(project) => {
                    format!(
                        "Skill not found in project {}: {skill_id}",
                        project.to_uppercase()
                    )
                }
                None => format!("Skill not found: {skill_id}"),
            }
        }
        Err(e) => return format!("Error loading skill: {e}"),
    };

    // An inherited workspace skill disabled for this project reads as not-found.
    // A project-scoped skill is the project's own and is never hidden.
    if !skill.is_project_scoped {
        if let Some(pid) = project_id.as_deref() {
            if let Ok(disabled) =
                crate::config_disables::list_disabled_keys(&orch.db.local, pid, "skill").await
            {
                if disabled.contains(&skill.id) {
                    return match explicit_project {
                        Some(project) => format!(
                            "Skill not found in project {}: {skill_id}",
                            project.to_uppercase()
                        ),
                        None => format!("Skill not found: {skill_id}"),
                    };
                }
            }
        }
    }

    let scope = match explicit_project {
        Some(project) => SkillScopeUri::Project(project.to_uppercase()),
        None => SkillScopeUri::Contextual,
    };

    if path.is_empty() {
        return render_skill(&skill, &scope);
    }

    // SKILL.md content is served at the skill root, not as a sub-resource.
    if path.len() == 1 && path[0].eq_ignore_ascii_case("SKILL.md") {
        return format!(
            "SKILL.md content is served at the skill root: {}",
            scope.skill_uri(&skill.id, &[])
        );
    }

    read_skill_subpath(&skill, path, &scope)
}

/// Render a skill at its root URI: the SKILL.md instruction body inline, plus
/// provenance, links to supporting package directories, and actions. SKILL.md
/// content lives here — there is no separate `.../SKILL.md` sub-resource.
fn render_skill(skill: &FileSkill, scope: &SkillScopeUri) -> String {
    let mut out = format!("# Skill: {} (`{}`)\n\n", skill.name, skill.id);
    out.push_str(&format!("[{}]\n\n", scope_label(skill)));
    out.push_str(&format!("{}\n\n", skill.description));

    // The SKILL.md instruction body is the skill's content; serve it inline.
    out.push_str(skill.prompt.trim_end());
    out.push_str("\n\n");

    if let Some(meta) = &skill.meta {
        let mut provenance = Vec::new();
        if let Some(updated) = &meta.updated_at {
            provenance.push(format!("- updated: {}", &updated[..10.min(updated.len())]));
        }
        if let Some(by) = &meta.updated_by {
            provenance.push(format!("- updated by: {by}"));
        }
        if let Some(issue) = &meta.source_issue {
            provenance.push(format!("- source issue: {issue}"));
        }
        if !provenance.is_empty() {
            out.push_str("## provenance\n");
            out.push_str(&provenance.join("\n"));
            out.push_str("\n\n");
        }
    }

    // Supporting package directories (SKILL.md itself is served at this URI).
    let mut package: Vec<(&str, String)> = Vec::new();
    if skill.has_references {
        package.push((
            "references",
            scope.skill_uri(&skill.id, &["references".to_string()]),
        ));
    }
    if skill.has_scripts {
        package.push((
            "scripts",
            scope.skill_uri(&skill.id, &["scripts".to_string()]),
        ));
    }
    if skill.has_assets {
        package.push((
            "assets",
            scope.skill_uri(&skill.id, &["assets".to_string()]),
        ));
    }
    if !package.is_empty() {
        out.push_str("## package\n");
        for (label, uri) in package {
            out.push_str(&format!("- [{label}]({uri})\n"));
        }
        out.push('\n');
    }

    let skill_uri = scope.skill_uri(&skill.id, &[]);
    out.push_str("## actions\n");
    out.push_str(&format!(
        "- [patch skill]({skill_uri}): optional `payload.description`, one of `payload.prompt` | `payload.appendToPrompt` | `payload.replaceSection`, `payload.allowedTools`, `payload.model`\n",
    ));
    out.push_str(&format!(
        "- [delete skill]({skill_uri}): optional `payload.reason`\n",
    ));
    out
}

fn read_skill_subpath(skill: &FileSkill, path: &[String], scope: &SkillScopeUri) -> String {
    let resolved = match resolve_safe_path(&skill.dir_path, path) {
        Ok(resolved) => resolved,
        Err(e) => return e,
    };

    if resolved.is_dir() {
        render_directory(skill, path, &resolved, scope)
    } else {
        match std::fs::read_to_string(&resolved) {
            Ok(content) => content,
            Err(e) => format!("Failed to read {}: {e}", path.join("/")),
        }
    }
}

fn render_directory(
    skill: &FileSkill,
    path: &[String],
    resolved: &Path,
    scope: &SkillScopeUri,
) -> String {
    let rel = path.join("/");
    let mut out = format!("# {}/{}\n\n", skill.id, rel);

    let mut entries: Vec<(String, bool)> = match std::fs::read_dir(resolved) {
        Ok(reader) => reader
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let name = entry.file_name().to_str()?.to_string();
                let is_dir = entry.path().is_dir();
                Some((name, is_dir))
            })
            .collect(),
        Err(e) => return format!("Failed to read directory {rel}: {e}"),
    };
    entries.sort();

    if entries.is_empty() {
        out.push_str("(empty)\n");
        return out;
    }

    for (name, is_dir) in entries {
        let mut child = path.to_vec();
        child.push(name.clone());
        let label = if is_dir { format!("{name}/") } else { name };
        out.push_str(&format!(
            "- [{}]({})\n",
            label,
            scope.skill_uri(&skill.id, &child)
        ));
    }
    out
}

/// Join package-relative segments under `dir_path`, rejecting traversal/escape.
fn resolve_safe_path(dir_path: &Path, segments: &[String]) -> Result<PathBuf, String> {
    for segment in segments {
        if segment.is_empty()
            || segment == "."
            || segment == ".."
            || segment.contains('/')
            || segment.contains('\\')
        {
            return Err(format!("Invalid path segment: {segment:?}"));
        }
    }

    let mut joined = dir_path.to_path_buf();
    for segment in segments {
        joined.push(segment);
    }

    let canonical_dir = dir_path
        .canonicalize()
        .map_err(|e| format!("Failed to resolve skill directory: {e}"))?;
    let canonical = joined
        .canonicalize()
        .map_err(|_| format!("Path not found: {}", segments.join("/")))?;
    if !canonical.starts_with(&canonical_dir) {
        return Err("Path escapes skill directory".to_string());
    }
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn write_skill(dir: &Path, id: &str, description: &str) -> FileSkill {
        let skill_dir = dir.join(id);
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(
            skill_dir.join("SKILL.md"),
            format!("---\nname: {id}\ndescription: {description}\n---\n\nBody for {id}.\n"),
        )
        .unwrap();
        match config_skills::get_skill(dir.parent().unwrap(), id, None) {
            Ok(Some(skill)) => skill,
            _ => panic!("failed to load fixture skill"),
        }
    }

    fn write_project_skill(project_dir: &Path, id: &str, description: &str) {
        let skill_dir = project_dir.join(".cairn").join("skills").join(id);
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(
            skill_dir.join("SKILL.md"),
            format!("---\nname: {id}\ndescription: {description}\n---\n\nProject body for {id}.\n"),
        )
        .unwrap();
    }

    /// Render the collection directly from loaded skills, bypassing run-context
    /// resolution (which needs a DB). Mirrors `read_skills_collection`'s body.
    fn render_collection(config_dir: &Path, project_path: Option<&Path>) -> String {
        let skills = config_skills::list_skills(config_dir, project_path).unwrap();
        let mut by_id: std::collections::BTreeMap<String, FileSkill> =
            std::collections::BTreeMap::new();
        for result in skills {
            if let ConfigResult::Ok(skill) = result {
                by_id.entry(skill.id.clone()).or_insert(skill);
            }
        }

        let mut out = String::new();
        for skill in by_id.values() {
            out.push_str(&format!(
                "- [{}]({}) [{}] — {}\n",
                skill.id,
                skill_link(skill, Some("CAIRN"), &[]),
                scope_label(skill),
                skill.description,
            ));
        }
        out
    }

    #[test]
    fn project_skill_shadows_workspace_by_id() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        let project_dir = temp.path().join("project");
        std::fs::create_dir_all(config_dir.join("skills")).unwrap();
        write_skill(&config_dir.join("skills"), "shared", "Workspace Version");
        write_project_skill(&project_dir, "shared", "Project Version");

        let rendered = render_collection(&config_dir, Some(&project_dir));
        // Project version wins and links project-scoped.
        assert!(rendered.contains("cairn://p/CAIRN/skills/shared"));
        assert!(rendered.contains("[project] — Project Version"));
        assert!(!rendered.contains("Workspace Version"));
    }

    #[test]
    fn resolve_safe_path_rejects_traversal() {
        let temp = tempdir().unwrap();
        let dir = temp.path().join("skills").join("ui");
        std::fs::create_dir_all(dir.join("references")).unwrap();
        std::fs::write(dir.join("references/a.md"), "hello").unwrap();

        assert!(resolve_safe_path(&dir, &["..".to_string()]).is_err());
        assert!(resolve_safe_path(&dir, &["references".to_string(), "..".to_string()]).is_err());
        assert!(resolve_safe_path(&dir, &["references".to_string(), "a.md".to_string()]).is_ok());
    }

    #[test]
    fn render_skill_serves_body_and_package_links() {
        let temp = tempdir().unwrap();
        let skills = temp.path().join("skills");
        std::fs::create_dir_all(&skills).unwrap();
        let skill = write_skill(&skills, "ui", "UI work");
        let rendered = render_skill(&skill, &SkillScopeUri::Contextual);
        // The SKILL.md body is served inline; there is no SKILL.md sub-resource link.
        assert!(rendered.contains("Body for ui."));
        assert!(!rendered.contains("cairn://skills/ui/SKILL.md"));
        assert!(!rendered.contains("cairn://skills/ui/references"));

        // Add a references dir and reload.
        std::fs::create_dir_all(skills.join("ui/references")).unwrap();
        let skill = write_skill(&skills, "ui", "UI work");
        let rendered = render_skill(&skill, &SkillScopeUri::Project("CAIRN".to_string()));
        assert!(rendered.contains("cairn://p/CAIRN/skills/ui/references"));
        assert!(!rendered.contains("cairn://p/CAIRN/skills/ui/SKILL.md"));
        assert!(rendered.contains("Body for ui."));
    }

    #[test]
    fn read_subpath_returns_file_and_lists_dir() {
        let temp = tempdir().unwrap();
        let skills = temp.path().join("skills");
        std::fs::create_dir_all(skills.join("ui/references")).unwrap();
        std::fs::write(skills.join("ui/references/a.md"), "alpha").unwrap();
        let skill = write_skill(&skills, "ui", "UI work");

        let listing = read_skill_subpath(
            &skill,
            &["references".to_string()],
            &SkillScopeUri::Contextual,
        );
        assert!(listing.contains("a.md"));
        assert!(listing.contains("cairn://skills/ui/references/a.md"));

        let content = read_skill_subpath(
            &skill,
            &["references".to_string(), "a.md".to_string()],
            &SkillScopeUri::Contextual,
        );
        assert_eq!(content, "alpha");
    }
}
