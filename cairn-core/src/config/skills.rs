//! Skill configuration file reading.
//!
//! Skills are directories with `SKILL.md` and optional supporting files.
//! Follows the Agent Skills spec (agentskills.io).
//!
//! ```text
//! skills/{name}/
//!   SKILL.md          <- required (spec-compliant frontmatter)
//!   scripts/          <- optional executable code
//!   references/       <- optional documentation
//!   assets/           <- optional static resources
//!   .meta.json        <- Cairn-internal provenance
//! ```
//!
//! - Workspace-scoped: `~/.cairn/skills/`
//! - Project-scoped: `[project-dir]/.cairn/skills/`

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::skills::{parse_skill_markdown, skill_to_markdown_spec, SkillExportDataSpec};

use super::ConfigResult;

/// Cairn-internal provenance metadata stored in `.meta.json`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SkillMeta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_issue: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_url: Option<String>,
}

/// Update parameters for `.meta.json` provenance.
#[derive(Debug, Clone, Default)]
pub struct SkillMetaUpdate {
    pub updated_by: Option<String>,
    pub source_issue: Option<String>,
    pub source_run_id: Option<String>,
}

/// Skill loaded from a directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSkill {
    pub id: String,
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub allowed_tools: Option<Vec<String>>,
    pub is_project_scoped: bool,
    /// Path to SKILL.md
    pub file_path: PathBuf,
    /// Path to the skill directory
    pub dir_path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<SkillMeta>,
    #[serde(default)]
    pub has_references: bool,
    #[serde(default)]
    pub has_scripts: bool,
    #[serde(default)]
    pub has_assets: bool,
}

/// List all skills from directories.
pub fn list_skills(
    config_dir: &Path,
    project_path: Option<&Path>,
) -> Result<Vec<ConfigResult<FileSkill>>, String> {
    let mut results = vec![];
    let mut project_migrated = false;

    for (dir, is_project_scoped) in super::config_root_subdirs(config_dir, project_path, "skills") {
        if dir.exists() && dir.is_dir() {
            let mut migrated = false;
            scan_skills_dir(&dir, is_project_scoped, &mut results, &mut migrated)?;
            if migrated && is_project_scoped {
                project_migrated = true;
            }
        }
    }

    // A project-scoped legacy `.md` skill was rewritten to directory format in
    // the canonical checkout; commit it so it does not float as dirty state.
    if project_migrated {
        if let Some(project_path) = project_path {
            super::commit_project_config_change(project_path, "cairn: migrate skills");
        }
    }

    Ok(results)
}

fn scan_skills_dir(
    dir: &Path,
    is_project_scoped: bool,
    results: &mut Vec<ConfigResult<FileSkill>>,
    migrated: &mut bool,
) -> Result<(), String> {
    let entries: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| format!("Failed to read skills directory: {}", e))?
        .filter_map(|e| e.ok())
        .collect();

    // Collect directory skill IDs first
    let mut dir_ids = std::collections::HashSet::new();
    for entry in &entries {
        let path = entry.path();
        if path.is_dir() && path.join("SKILL.md").exists() {
            if let Some(id) = path.file_name().and_then(|n| n.to_str()) {
                dir_ids.insert(id.to_string());
            }
            results.push(load_skill_dir(&path, is_project_scoped));
        }
    }

    // Auto-migrate legacy .md files that don't already have a directory
    for entry in &entries {
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|e| e.to_str()) == Some("md") {
            if let Some(id) = super::id_from_path(&path) {
                if !dir_ids.contains(&id) {
                    match migrate_legacy_skill(dir, &id) {
                        Ok(()) => {
                            *migrated = true;
                            log::info!("Auto-migrated legacy skill: {}", id);
                            let migrated_dir = dir.join(&id);
                            results.push(load_skill_dir(&migrated_dir, is_project_scoped));
                        }
                        Err(e) => {
                            log::warn!("Failed to auto-migrate skill {}: {}", id, e);
                            results.push(ConfigResult::Err {
                                path: path.clone(),
                                error: format!("Legacy skill needs migration: {}", e),
                            });
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// Get a specific skill by ID.
///
/// Auto-migrates legacy `.md` files to directory format on access.
pub fn get_skill(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<Option<FileSkill>, String> {
    for (skills_dir, is_project_scoped) in
        super::config_root_subdirs(config_dir, project_path, "skills")
    {
        let mut migrated = false;
        let skill = get_skill_from_dir(&skills_dir, id, is_project_scoped, &mut migrated)?;
        // A project-scoped legacy `.md` skill was rewritten to directory format in
        // the canonical checkout; commit it so it does not float as dirty state.
        if migrated && is_project_scoped {
            if let Some(project_path) = project_path {
                super::commit_project_config_change(
                    project_path,
                    &format!("cairn: migrate skill {id}"),
                );
            }
        }
        if let Some(skill) = skill {
            return Ok(Some(skill));
        }
    }
    Ok(None)
}

/// Try to load a skill from a skills directory, auto-migrating legacy files.
fn get_skill_from_dir(
    skills_dir: &Path,
    id: &str,
    is_project_scoped: bool,
    migrated: &mut bool,
) -> Result<Option<FileSkill>, String> {
    let dir_path = skills_dir.join(id);
    if dir_path.join("SKILL.md").exists() {
        return match load_skill_dir(&dir_path, is_project_scoped) {
            ConfigResult::Ok(skill) => Ok(Some(skill)),
            ConfigResult::Err { error, .. } => Err(error),
        };
    }

    // Auto-migrate legacy file if present
    let legacy_path = skills_dir.join(format!("{}.md", id));
    if legacy_path.exists() {
        migrate_legacy_skill(skills_dir, id)?;
        *migrated = true;
        return match load_skill_dir(&dir_path, is_project_scoped) {
            ConfigResult::Ok(skill) => Ok(Some(skill)),
            ConfigResult::Err { error, .. } => Err(error),
        };
    }

    Ok(None)
}

/// Save a skill directory with spec-compliant SKILL.md.
pub fn save_skill(
    config_dir: &Path,
    skill: &FileSkill,
    project_path: Option<&Path>,
    meta_update: Option<SkillMetaUpdate>,
) -> Result<PathBuf, String> {
    let skills_dir = if skill.is_project_scoped {
        let proj_path = project_path.ok_or("Project path required for project-scoped skill")?;
        proj_path.join(".cairn").join("skills")
    } else {
        config_dir.join("skills")
    };

    let skill_dir = skills_dir.join(&skill.id);
    std::fs::create_dir_all(&skill_dir)
        .map_err(|e| format!("Failed to create skill directory: {}", e))?;

    let markdown = skill_to_markdown_spec(SkillExportDataSpec {
        slug: &skill.id,
        display_name: &skill.name,
        description: &skill.description,
        allowed_tools: skill.allowed_tools.as_deref(),
        prompt: &skill.prompt,
    });

    let skill_md_path = skill_dir.join("SKILL.md");
    std::fs::write(&skill_md_path, &markdown)
        .map_err(|e| format!("Failed to write SKILL.md: {}", e))?;

    if let Some(update) = meta_update {
        write_meta(&skill_dir, update)?;
    }

    Ok(skill_md_path)
}

fn write_meta(skill_dir: &Path, update: SkillMetaUpdate) -> Result<(), String> {
    let now = chrono::Utc::now().to_rfc3339();
    let meta_path = skill_dir.join(".meta.json");

    let mut meta = if meta_path.exists() {
        std::fs::read_to_string(&meta_path)
            .ok()
            .and_then(|s| serde_json::from_str::<SkillMeta>(&s).ok())
            .unwrap_or_default()
    } else {
        SkillMeta {
            created_at: Some(now.clone()),
            created_by: update.updated_by.clone(),
            ..Default::default()
        }
    };

    meta.updated_at = Some(now.clone());
    if let Some(by) = update.updated_by {
        meta.updated_by = Some(by);
        if meta.created_by.is_none() {
            meta.created_by = meta.updated_by.clone();
        }
    }
    if meta.created_at.is_none() {
        meta.created_at = Some(now);
    }
    if let Some(issue) = update.source_issue {
        meta.source_issue = Some(issue);
    }
    if let Some(run_id) = update.source_run_id {
        meta.source_run_id = Some(run_id);
    }

    let meta_json = serde_json::to_string_pretty(&meta)
        .map_err(|e| format!("Failed to serialize .meta.json: {}", e))?;
    std::fs::write(&meta_path, meta_json)
        .map_err(|e| format!("Failed to write .meta.json: {}", e))?;
    Ok(())
}

/// Copy package subdirectories (scripts/, references/, assets/) from source to dest skill dir.
/// Skips directories that don't exist in source. Overwrites files in dest.
pub fn copy_skill_package(source_dir: &Path, dest_dir: &Path) -> Result<(), String> {
    for subdir in &["scripts", "references", "assets"] {
        let src = source_dir.join(subdir);
        if src.is_dir() {
            let dst = dest_dir.join(subdir);
            copy_dir_recursive(&src, &dst)
                .map_err(|e| format!("Failed to copy {}: {}", subdir, e))?;
        }
    }
    Ok(())
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

/// Delete a skill directory.
pub fn delete_skill(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<(), String> {
    if let Some(proj_path) = project_path {
        let dir_path = proj_path.join(".cairn").join("skills").join(id);
        if dir_path.exists() && dir_path.is_dir() {
            return std::fs::remove_dir_all(&dir_path)
                .map_err(|e| format!("Failed to delete skill directory: {}", e));
        }
    }

    let dir_path = config_dir.join("skills").join(id);
    if dir_path.exists() && dir_path.is_dir() {
        std::fs::remove_dir_all(&dir_path)
            .map_err(|e| format!("Failed to delete skill directory: {}", e))?;
    }
    Ok(())
}

/// Migrate a single legacy skill file to directory format.
pub fn migrate_legacy_skill(skills_dir: &Path, id: &str) -> Result<(), String> {
    let legacy_path = skills_dir.join(format!("{}.md", id));
    if !legacy_path.exists() {
        return Err(format!("Legacy skill file not found: {}", id));
    }

    let content = std::fs::read_to_string(&legacy_path)
        .map_err(|e| format!("Failed to read legacy skill: {}", e))?;
    let parsed =
        parse_skill_markdown(&content).map_err(|e| format!("Parse error for {}: {}", id, e))?;

    let skill_dir = skills_dir.join(id);
    std::fs::create_dir_all(&skill_dir)
        .map_err(|e| format!("Failed to create skill directory: {}", e))?;

    let markdown = skill_to_markdown_spec(SkillExportDataSpec {
        slug: id,
        display_name: &parsed.name,
        description: &parsed.description,
        allowed_tools: parsed.allowed_tools.as_deref(),
        prompt: &parsed.prompt,
    });
    std::fs::write(skill_dir.join("SKILL.md"), &markdown)
        .map_err(|e| format!("Failed to write SKILL.md: {}", e))?;

    let now = chrono::Utc::now().to_rfc3339();
    let meta = SkillMeta {
        created_at: Some(now.clone()),
        updated_at: Some(now),
        ..Default::default()
    };
    std::fs::write(
        skill_dir.join(".meta.json"),
        serde_json::to_string_pretty(&meta).map_err(|e| format!("Failed: {}", e))?,
    )
    .map_err(|e| format!("Failed to write .meta.json: {}", e))?;

    std::fs::remove_file(&legacy_path)
        .map_err(|e| format!("Failed to remove legacy file: {}", e))?;
    Ok(())
}

/// Migrate all legacy skills in workspace and project dirs.
pub fn migrate_all_legacy_skills(config_dir: &Path, project_path: Option<&Path>) -> Vec<String> {
    let mut migrated = vec![];
    let mut dirs = vec![config_dir.join("skills")];
    if let Some(proj) = project_path {
        dirs.push(proj.join(".cairn").join("skills"));
    }

    for skills_dir in dirs {
        if !skills_dir.exists() {
            continue;
        }
        if let Ok(entries) = std::fs::read_dir(&skills_dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|e| e.to_str()) == Some("md") {
                    if let Some(id) = super::id_from_path(&path) {
                        if !skills_dir.join(&id).exists() {
                            match migrate_legacy_skill(&skills_dir, &id) {
                                Ok(()) => {
                                    log::info!("Migrated legacy skill: {}", id);
                                    migrated.push(id);
                                }
                                Err(e) => log::warn!("Failed to migrate skill {}: {}", id, e),
                            }
                        }
                    }
                }
            }
        }
    }
    migrated
}

fn load_skill_dir(dir_path: &Path, is_project_scoped: bool) -> ConfigResult<FileSkill> {
    let id = match dir_path.file_name().and_then(|n| n.to_str()) {
        Some(name) => name.to_string(),
        None => {
            return ConfigResult::Err {
                path: dir_path.to_path_buf(),
                error: "Could not determine skill ID from directory name".to_string(),
            }
        }
    };

    let skill_md_path = dir_path.join("SKILL.md");
    let content = match std::fs::read_to_string(&skill_md_path) {
        Ok(c) => c,
        Err(e) => {
            return ConfigResult::Err {
                path: skill_md_path,
                error: format!("Failed to read SKILL.md: {}", e),
            }
        }
    };

    match parse_skill_markdown(&content) {
        Ok(parsed) => ConfigResult::Ok(FileSkill {
            id,
            name: parsed.name,
            description: parsed.description,
            prompt: parsed.prompt,
            allowed_tools: parsed.allowed_tools,
            is_project_scoped,
            file_path: skill_md_path,
            dir_path: dir_path.to_path_buf(),
            meta: load_meta(dir_path),
            has_references: dir_path.join("references").is_dir(),
            has_scripts: dir_path.join("scripts").is_dir(),
            has_assets: dir_path.join("assets").is_dir(),
        }),
        Err(e) => ConfigResult::Err {
            path: skill_md_path,
            error: e,
        },
    }
}

fn load_meta(dir_path: &Path) -> Option<SkillMeta> {
    std::fs::read_to_string(dir_path.join(".meta.json"))
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn write_skill_md(dir: &std::path::Path, markdown: &str) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("SKILL.md"), markdown).unwrap();
    }

    #[test]
    fn test_load_skill_dir_basic() {
        let temp = tempdir().unwrap();
        let skill_dir = temp.path().join("skills").join("testing");
        write_skill_md(&skill_dir, "---\nname: testing\ndescription: Test skill\nallowed-tools: Read Grep\nmetadata:\n  model: sonnet\n  display-name: Testing\n---\n\nRun the tests.\n");

        match load_skill_dir(&skill_dir, false) {
            ConfigResult::Ok(s) => {
                assert_eq!(s.id, "testing");
                assert_eq!(s.name, "Testing");
                assert_eq!(
                    s.allowed_tools,
                    Some(vec!["Read".to_string(), "Grep".to_string()])
                );
            }
            ConfigResult::Err { error, .. } => panic!("{}", error),
        }
    }

    #[test]
    fn test_load_skill_dir_with_meta() {
        let temp = tempdir().unwrap();
        let d = temp.path().join("skills").join("testing");
        write_skill_md(&d, "---\nname: testing\ndescription: Test\n---\n\nPrompt.");
        std::fs::write(
            d.join(".meta.json"),
            r#"{"created_by":"Reviewer","source_issue":"CAIRN-949"}"#,
        )
        .unwrap();

        match load_skill_dir(&d, false) {
            ConfigResult::Ok(s) => {
                let m = s.meta.unwrap();
                assert_eq!(m.created_by, Some("Reviewer".to_string()));
                assert_eq!(m.source_issue, Some("CAIRN-949".to_string()));
            }
            ConfigResult::Err { error, .. } => panic!("{}", error),
        }
    }

    #[test]
    fn test_load_skill_dir_supporting_files() {
        let temp = tempdir().unwrap();
        let d = temp.path().join("skills").join("testing");
        std::fs::create_dir_all(d.join("references")).unwrap();
        std::fs::create_dir_all(d.join("scripts")).unwrap();
        std::fs::create_dir_all(d.join("assets")).unwrap();
        write_skill_md(&d, "---\nname: testing\ndescription: Test\n---\n\nPrompt.");

        match load_skill_dir(&d, false) {
            ConfigResult::Ok(s) => {
                assert!(s.has_references && s.has_scripts && s.has_assets);
            }
            ConfigResult::Err { error, .. } => panic!("{}", error),
        }
    }

    #[test]
    fn test_load_skill_dir_missing_skill_md() {
        let temp = tempdir().unwrap();
        let d = temp.path().join("skills").join("testing");
        std::fs::create_dir_all(&d).unwrap();
        assert!(matches!(
            load_skill_dir(&d, false),
            ConfigResult::Err { .. }
        ));
    }

    #[test]
    fn test_save_and_load_roundtrip() {
        let temp = tempdir().unwrap();
        let cfg = temp.path();
        std::fs::create_dir_all(cfg.join("skills")).unwrap();

        let skill = FileSkill {
            id: "testing".into(),
            name: "Testing".into(),
            description: "A test skill".into(),
            prompt: "Do things.".into(),
            allowed_tools: Some(vec!["Read".into(), "Grep".into()]),
            is_project_scoped: false,
            file_path: PathBuf::new(),
            dir_path: PathBuf::new(),
            meta: None,
            has_references: false,
            has_scripts: false,
            has_assets: false,
        };

        save_skill(
            cfg,
            &skill,
            None,
            Some(SkillMetaUpdate {
                updated_by: Some("test".into()),
                source_issue: Some("CAIRN-999".into()),
                ..Default::default()
            }),
        )
        .unwrap();

        let loaded = get_skill(cfg, "testing", None).unwrap().unwrap();
        assert_eq!(loaded.id, "testing");
        assert_eq!(
            loaded.allowed_tools,
            Some(vec!["Read".into(), "Grep".into()])
        );
        assert!(loaded.meta.unwrap().source_issue == Some("CAIRN-999".into()));
    }

    #[test]
    fn test_save_preserves_created_at() {
        let temp = tempdir().unwrap();
        let cfg = temp.path();
        let d = cfg.join("skills/testing");
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(
            d.join(".meta.json"),
            r#"{"created_at":"2026-01-01T00:00:00Z","created_by":"original"}"#,
        )
        .unwrap();
        write_skill_md(&d, "---\nname: testing\ndescription: Test\n---\n\nPrompt.");

        let skill = FileSkill {
            id: "testing".into(),
            name: "Testing".into(),
            description: "Updated".into(),
            prompt: "Updated.".into(),
            allowed_tools: None,
            is_project_scoped: false,
            file_path: PathBuf::new(),
            dir_path: PathBuf::new(),
            meta: None,
            has_references: false,
            has_scripts: false,
            has_assets: false,
        };
        save_skill(
            cfg,
            &skill,
            None,
            Some(SkillMetaUpdate {
                updated_by: Some("updater".into()),
                ..Default::default()
            }),
        )
        .unwrap();

        let m: SkillMeta =
            serde_json::from_str(&std::fs::read_to_string(d.join(".meta.json")).unwrap()).unwrap();
        assert_eq!(m.created_at, Some("2026-01-01T00:00:00Z".into()));
        assert_eq!(m.created_by, Some("original".into()));
        assert_eq!(m.updated_by, Some("updater".into()));
    }

    #[test]
    fn test_delete_skill() {
        let temp = tempdir().unwrap();
        let cfg = temp.path();
        let d = cfg.join("skills/testing");
        write_skill_md(&d, "---\nname: testing\ndescription: Test\n---\n\nPrompt.");

        delete_skill(cfg, "testing", None).unwrap();
        assert!(!d.exists());
    }

    #[test]
    fn test_migrate_legacy_skill() {
        let temp = tempdir().unwrap();
        let sd = temp.path().join("skills");
        std::fs::create_dir_all(&sd).unwrap();
        std::fs::write(sd.join("testing.md"), "---\nname: Testing\ndescription: Test patterns\nallowedTools: Read, Grep, Glob\nmodel: sonnet\n---\n\nHow to test.\n").unwrap();

        migrate_legacy_skill(&sd, "testing").unwrap();
        assert!(!sd.join("testing.md").exists());
        let content = std::fs::read_to_string(sd.join("testing/SKILL.md")).unwrap();
        assert!(content.contains("name: testing"));
        assert!(content.contains("allowed-tools: Read Grep Glob"));
        assert!(sd.join("testing/.meta.json").exists());
    }

    #[test]
    fn test_migrate_bad_parse_preserves_file() {
        let temp = tempdir().unwrap();
        let sd = temp.path().join("skills");
        std::fs::create_dir_all(&sd).unwrap();
        std::fs::write(sd.join("broken.md"), "not valid frontmatter").unwrap();

        assert!(migrate_legacy_skill(&sd, "broken").is_err());
        assert!(sd.join("broken.md").exists());
        assert!(!sd.join("broken").exists());
    }

    #[test]
    fn test_list_skills_auto_migrates_legacy_and_skips_empty_dirs() {
        let temp = tempdir().unwrap();
        let sd = temp.path().join("skills");
        std::fs::create_dir_all(&sd).unwrap();

        // A legacy .md file should be auto-migrated
        std::fs::write(
            sd.join("legacy.md"),
            "---\nname: Legacy\ndescription: Old\n---\n\nPrompt.",
        )
        .unwrap();

        // A directory without SKILL.md should be skipped
        std::fs::create_dir_all(sd.join("empty-dir")).unwrap();

        // A valid skill directory
        let valid = sd.join("valid-skill");
        write_skill_md(
            &valid,
            "---\nname: valid-skill\ndescription: Valid\n---\n\nPrompt.",
        );

        let results = list_skills(temp.path(), None).unwrap();
        // Should have 2: the valid dir + the auto-migrated legacy
        assert_eq!(results.len(), 2);
        let mut ids: Vec<String> = results
            .iter()
            .filter_map(|r| match r {
                ConfigResult::Ok(s) => Some(s.id.clone()),
                _ => None,
            })
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["legacy", "valid-skill"]);

        // legacy.md should be gone, replaced by legacy/SKILL.md
        assert!(!sd.join("legacy.md").exists());
        assert!(sd.join("legacy/SKILL.md").exists());
    }

    #[test]
    fn test_migrate_all_legacy_skills_batch() {
        let temp = tempdir().unwrap();
        let cfg = temp.path();
        let sd = cfg.join("skills");
        std::fs::create_dir_all(&sd).unwrap();

        // Two legacy skills
        std::fs::write(
            sd.join("alpha.md"),
            "---\nname: Alpha\ndescription: A skill\n---\n\nAlpha prompt.",
        )
        .unwrap();
        std::fs::write(
            sd.join("beta.md"),
            "---\nname: Beta\ndescription: B skill\n---\n\nBeta prompt.",
        )
        .unwrap();

        // One already-migrated (directory exists)
        let existing = sd.join("alpha");
        write_skill_md(
            &existing,
            "---\nname: alpha\ndescription: Already\n---\n\nExisting.",
        );

        let migrated = migrate_all_legacy_skills(cfg, None);
        // Only beta should be migrated (alpha dir already exists)
        assert_eq!(migrated, vec!["beta".to_string()]);
        assert!(sd.join("beta/SKILL.md").exists());
        assert!(!sd.join("beta.md").exists());
        // alpha.md should still exist (skipped because dir exists)
        assert!(sd.join("alpha.md").exists());
    }

    #[test]
    fn test_delete_skill_project_scoped_leaves_workspace() {
        let temp = tempdir().unwrap();
        let cfg = temp.path();

        // Create workspace skill
        let ws_dir = cfg.join("skills/my-skill");
        write_skill_md(
            &ws_dir,
            "---\nname: my-skill\ndescription: WS\n---\n\nWS prompt.",
        );

        // Create project skill with same ID
        let proj = temp.path().join("project");
        let proj_dir = proj.join(".cairn/skills/my-skill");
        write_skill_md(
            &proj_dir,
            "---\nname: my-skill\ndescription: Proj\n---\n\nProj prompt.",
        );

        // Delete with project_path — should remove project version
        delete_skill(cfg, "my-skill", Some(&proj)).unwrap();

        // Project version gone, workspace version intact
        assert!(!proj_dir.exists());
        assert!(ws_dir.exists());
    }

    #[test]
    fn test_get_skill_project_shadows_workspace() {
        let temp = tempdir().unwrap();
        let cfg = temp.path();

        // Create workspace skill
        let ws_dir = cfg.join("skills/my-skill");
        write_skill_md(
            &ws_dir,
            "---\nname: my-skill\ndescription: Workspace version\n---\n\nWS.",
        );

        // Create project skill
        let proj = temp.path().join("project");
        let proj_dir = proj.join(".cairn/skills/my-skill");
        write_skill_md(
            &proj_dir,
            "---\nname: my-skill\ndescription: Project version\n---\n\nProj.",
        );

        let skill = get_skill(cfg, "my-skill", Some(&proj)).unwrap().unwrap();
        assert_eq!(skill.description, "Project version");
        assert!(skill.is_project_scoped);
    }

    #[test]
    fn test_write_meta_sets_created_by_from_updated_by_on_new() {
        let temp = tempdir().unwrap();
        let d = temp.path().join("skill");
        std::fs::create_dir_all(&d).unwrap();

        write_meta(
            &d,
            SkillMetaUpdate {
                updated_by: Some("builder-1".into()),
                source_issue: None,
                source_run_id: None,
            },
        )
        .unwrap();

        let m: SkillMeta =
            serde_json::from_str(&std::fs::read_to_string(d.join(".meta.json")).unwrap()).unwrap();
        // On new meta, created_by should be set from updated_by
        assert_eq!(m.created_by, Some("builder-1".into()));
        assert_eq!(m.updated_by, Some("builder-1".into()));
        assert!(m.created_at.is_some());
    }

    fn init_git_repo(path: &Path) {
        assert!(crate::env::git()
            .args(["init", "-q"])
            .current_dir(path)
            .status()
            .unwrap()
            .success());
    }

    fn commit_all(repo: &Path, msg: &str) {
        crate::env::git()
            .args(["add", "-A"])
            .current_dir(repo)
            .status()
            .unwrap();
        crate::env::git()
            .args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@local.invalid",
                "commit",
                "-q",
                "-m",
                msg,
            ])
            .current_dir(repo)
            .status()
            .unwrap();
    }

    fn git_status(path: &Path) -> String {
        let out = crate::env::git()
            .args(["status", "--porcelain"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn git_head_subject(path: &Path) -> String {
        let out = crate::env::git()
            .args(["log", "-1", "--pretty=%s"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    #[test]
    fn legacy_project_skill_migration_commits_in_canonical_repo() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let project = temp.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        init_git_repo(&project);

        let skills_dir = project.join(".cairn").join("skills");
        std::fs::create_dir_all(&skills_dir).unwrap();
        std::fs::write(
            skills_dir.join("legacy.md"),
            "---\nname: Legacy\ndescription: Old skill\n---\n\nBody.\n",
        )
        .unwrap();
        commit_all(&project, "seed legacy skill");

        // Resolving the skill migrates legacy.md -> legacy/SKILL.md and commits it.
        let skill = get_skill(&config_dir, "legacy", Some(&project))
            .unwrap()
            .unwrap();
        assert!(skill.dir_path.join("SKILL.md").exists());
        assert!(!skills_dir.join("legacy.md").exists());

        assert!(
            git_status(&project).is_empty(),
            "migration left the canonical repo dirty"
        );
        assert_eq!(git_head_subject(&project), "cairn: migrate skill legacy");
    }
}
