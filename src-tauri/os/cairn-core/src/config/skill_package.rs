//! A skill package read from a local `.skill` archive, previewed, then
//! installed into a scope.
//!
//! Two-phase flow:
//! 1. Read the package files → return a preview (`build_fetched_skill`)
//! 2. Install the files to the target scope directory (`install_fetched_skill`)
//!
//! Installing a skill from an arbitrary URL is not offered: a remote skill is
//! third-party content, which reaches a workspace only through the Library's
//! sources (tenet 1370).

use std::path::{Path, PathBuf};

use crate::skills::parse_skill_markdown;

use super::skills::SkillMeta;

/// A single file of a skill package.
#[derive(Debug, Clone)]
pub struct FetchedFile {
    /// Relative path within the skill directory (e.g. "SKILL.md", "scripts/run.py")
    pub relative_path: String,
    pub content: Vec<u8>,
    pub is_binary: bool,
}

/// A complete skill package ready for preview/install.
#[derive(Debug, Clone)]
pub struct FetchedSkill {
    /// The local filename the package was read from, recorded as provenance.
    pub source_file: String,
    pub skill_id: String,
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub allowed_tools: Option<Vec<String>>,
    pub files: Vec<FetchedFile>,
    pub has_scripts: bool,
}

pub(crate) fn build_fetched_skill(
    files: Vec<FetchedFile>,
    source_file: String,
) -> Result<FetchedSkill, String> {
    let skill_md = files
        .iter()
        .find(|f| f.relative_path == "SKILL.md")
        .ok_or_else(|| "No SKILL.md found in the package".to_string())?;

    let content_str = String::from_utf8(skill_md.content.clone())
        .map_err(|_| "SKILL.md is not valid UTF-8".to_string())?;

    let parsed = parse_skill_markdown(&content_str)?;

    let has_scripts = files
        .iter()
        .any(|f| f.relative_path.starts_with("scripts/"));

    Ok(FetchedSkill {
        source_file,
        skill_id: parsed.id,
        name: parsed.name,
        description: parsed.description,
        prompt: parsed.prompt,
        allowed_tools: parsed.allowed_tools,
        files,
        has_scripts,
    })
}

/// Install a skill package's files to the target directory.
///
/// Returns the path to the installed skill directory.
pub(crate) fn install_fetched_skill(
    skill: &FetchedSkill,
    config_dir: &Path,
    project_path: Option<&Path>,
) -> Result<PathBuf, String> {
    let skills_dir = if let Some(proj) = project_path {
        proj.join(".cairn").join("skills")
    } else {
        config_dir.join("skills")
    };

    // Auto-increment suffix if skill ID already exists
    let mut final_id = skill.skill_id.clone();
    let mut counter = 1;
    while skills_dir.join(&final_id).join("SKILL.md").exists() {
        final_id = format!("{}-{}", skill.skill_id, counter);
        counter += 1;
    }

    // Validate the complete package before creating or writing anything.
    for file in &skill.files {
        super::skill_archive::validate_portable_relative_path(&file.relative_path)?;
    }

    let target_dir = skills_dir.join(&final_id);
    std::fs::create_dir_all(&target_dir)
        .map_err(|e| format!("Failed to create skill directory: {}", e))?;

    // Write all files preserving directory structure.
    for file in &skill.files {
        let dest = target_dir.join(&file.relative_path);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create directory: {}", e))?;
        }
        std::fs::write(&dest, &file.content)
            .map_err(|e| format!("Failed to write {}: {}", file.relative_path, e))?;
    }

    let now = chrono::Utc::now().to_rfc3339();
    let meta = SkillMeta {
        created_at: Some(now.clone()),
        updated_at: Some(now),
        source_file: Some(skill.source_file.clone()),
        ..Default::default()
    };
    let meta_json = serde_json::to_string_pretty(&meta)
        .map_err(|e| format!("Failed to serialize .meta.json: {}", e))?;
    std::fs::write(target_dir.join(".meta.json"), meta_json)
        .map_err(|e| format!("Failed to write .meta.json: {}", e))?;

    Ok(target_dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_skill_md(dir: &std::path::Path, markdown: &str) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("SKILL.md"), markdown).unwrap();
    }

    fn fetched_file(relative_path: &str, content: impl AsRef<[u8]>) -> FetchedFile {
        FetchedFile {
            relative_path: relative_path.to_string(),
            content: content.as_ref().to_vec(),
            is_binary: false,
        }
    }

    fn skill_md_content(skill_id: &str, description: &str, prompt: &str) -> Vec<u8> {
        format!("---\nname: {skill_id}\ndescription: {description}\n---\n\n{prompt}\n").into_bytes()
    }

    fn fetched_skill(skill_id: &str, description: &str, files: Vec<FetchedFile>) -> FetchedSkill {
        let has_scripts = files
            .iter()
            .any(|file| file.relative_path.starts_with("scripts/"));
        FetchedSkill {
            source_file: format!("{skill_id}.skill"),
            skill_id: skill_id.to_string(),
            name: skill_id.to_string(),
            description: description.to_string(),
            prompt: "Do stuff.".to_string(),
            allowed_tools: None,
            files,
            has_scripts,
        }
    }

    fn markdown_skill(skill_id: &str, description: &str) -> FetchedSkill {
        fetched_skill(
            skill_id,
            description,
            vec![fetched_file(
                "SKILL.md",
                skill_md_content(skill_id, description, "Do stuff."),
            )],
        )
    }

    #[test]
    fn installing_writes_every_file_and_records_the_local_source() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path();
        std::fs::create_dir_all(config_dir.join("skills")).unwrap();

        let skill = fetched_skill(
            "my-skill",
            "A test skill",
            vec![
                fetched_file(
                    "SKILL.md",
                    skill_md_content("my-skill", "A test skill", "Do stuff."),
                ),
                fetched_file("scripts/run.py", b"print('hello')"),
                fetched_file("references/guide.md", b"# Guide\nSome docs."),
                fetched_file("scripts/tools/deep/run.py", b"print('deep')"),
            ],
        );

        let result = install_fetched_skill(&skill, config_dir, None).unwrap();
        assert!(result.join("SKILL.md").exists());
        assert!(result.join("scripts/run.py").exists());
        assert!(result.join("references/guide.md").exists());
        assert!(result.join("scripts/tools/deep/run.py").exists());
        let meta: SkillMeta =
            serde_json::from_str(&std::fs::read_to_string(result.join(".meta.json")).unwrap())
                .unwrap();
        assert_eq!(meta.source_file.as_deref(), Some("my-skill.skill"));
        assert_eq!(meta.source_url, None);
    }

    #[test]
    fn a_taken_id_increments_until_free() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path();
        for name in ["my-skill", "my-skill-1"] {
            write_skill_md(
                &config_dir.join("skills").join(name),
                "---\nname: my-skill\ndescription: Existing\n---\n\nOld.",
            );
        }
        let result =
            install_fetched_skill(&markdown_skill("my-skill", "New"), config_dir, None).unwrap();
        assert!(result.ends_with("my-skill-2"));
        assert!(config_dir.join("skills/my-skill/SKILL.md").exists());
    }

    #[test]
    fn a_project_scope_installs_under_the_project() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join("config");
        let project_path = temp.path().join("project");
        let result = install_fetched_skill(
            &markdown_skill("test-skill", "Test"),
            &config_dir,
            Some(&project_path),
        )
        .unwrap();
        assert!(result.starts_with(project_path.join(".cairn/skills")));
        assert!(result.join("SKILL.md").exists());
    }

    #[test]
    fn a_package_needs_a_utf8_skill_md_and_scripts_are_detected() {
        let err = build_fetched_skill(vec![fetched_file("README.md", b"# Hello")], "x".into())
            .unwrap_err();
        assert!(err.contains("No SKILL.md found"));
        let err = build_fetched_skill(
            vec![fetched_file("SKILL.md", vec![0xFF, 0xFE, 0x00, 0x01])],
            "x".into(),
        )
        .unwrap_err();
        assert!(err.contains("not valid UTF-8"));

        let skill_md = b"---\nname: test\ndescription: A test\n---\n\nDo things.\n".to_vec();
        let plain =
            build_fetched_skill(vec![fetched_file("SKILL.md", skill_md.clone())], "x".into())
                .unwrap();
        assert!(!plain.has_scripts);
        let scripted = build_fetched_skill(
            vec![
                fetched_file("SKILL.md", skill_md),
                fetched_file("scripts/run.sh", b"#!/bin/bash"),
            ],
            "x".into(),
        )
        .unwrap();
        assert!(scripted.has_scripts);
    }
}
