//! Portable `.skill` ZIP transport for directory-based Agent Skills.

use std::collections::HashSet;
use std::io::{Cursor, Read, Write};
use std::path::{Component, Path};

use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

use super::skill_fetch::{build_fetched_skill, FetchedFile, FetchedSkill, FetchedSkillSource};

pub(crate) const MAX_COMPRESSED_BYTES: u64 = 50 * 1024 * 1024;
const MAX_UNCOMPRESSED_BYTES: u64 = 25 * 1024 * 1024;
const MAX_FILE_BYTES: u64 = 10 * 1024 * 1024;
const MAX_FILES: usize = 500;

/// Validate a portable package-relative path shared by imports and installation.
pub(crate) fn validate_portable_relative_path(path: &str) -> Result<(), String> {
    if path.is_empty() || path.contains('\0') {
        return Err("Package path is empty or contains a NUL byte".into());
    }
    if path.contains('\\') {
        return Err(format!(
            "Unsafe package path '{path}': backslashes are not portable"
        ));
    }
    if path.starts_with('/') || path.starts_with("//") {
        return Err(format!(
            "Unsafe package path '{path}': absolute paths are not allowed"
        ));
    }
    let bytes = path.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        return Err(format!(
            "Unsafe package path '{path}': Windows path prefixes are not allowed"
        ));
    }
    for component in Path::new(path).components() {
        match component {
            Component::Normal(part) if !part.is_empty() => {}
            _ => {
                return Err(format!(
                    "Unsafe package path '{path}': traversal is not allowed"
                ))
            }
        }
    }
    Ok(())
}

fn ignored_metadata_path(path: &str) -> bool {
    path == ".DS_Store"
        || path.ends_with("/.DS_Store")
        || path == "__MACOSX"
        || path.starts_with("__MACOSX/")
}

/// Parse a bounded `.skill` ZIP payload without extracting it to disk.
pub(crate) fn parse_skill_archive(bytes: &[u8], filename: &str) -> Result<FetchedSkill, String> {
    if bytes.len() as u64 > MAX_COMPRESSED_BYTES {
        return Err(format!(
            "Skill archive exceeds the {} MB compressed limit",
            MAX_COMPRESSED_BYTES / 1024 / 1024
        ));
    }
    let mut archive = ZipArchive::new(Cursor::new(bytes))
        .map_err(|e| format!("Invalid or malformed .skill ZIP archive: {e}"))?;
    if archive.len() > MAX_FILES + 32 {
        return Err(format!("Skill archive exceeds the {MAX_FILES} file limit"));
    }

    let mut root: Option<String> = None;
    let mut files = Vec::new();
    let mut normalized = HashSet::new();
    let mut total = 0u64;
    let mut skill_md_count = 0usize;

    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|e| format!("Invalid ZIP entry #{index}: {e}"))?;
        let raw = entry.name().trim_end_matches('/').to_string();
        if raw.is_empty() || ignored_metadata_path(&raw) {
            continue;
        }
        validate_portable_relative_path(&raw)?;
        let mode = entry.unix_mode().unwrap_or(0);
        let kind = mode & 0o170000;
        if kind == 0o120000 {
            return Err(format!(
                "Unsafe package entry '{raw}': symbolic links are not allowed"
            ));
        }
        if kind != 0 && kind != 0o100000 && kind != 0o040000 {
            return Err(format!(
                "Unsafe package entry '{raw}': special files are not allowed"
            ));
        }
        if entry.is_dir() {
            continue;
        }
        if !matches!(
            entry.compression(),
            CompressionMethod::Stored | CompressionMethod::Deflated
        ) {
            return Err(format!("Unsupported compression for package entry '{raw}'"));
        }
        if entry.size() > MAX_FILE_BYTES {
            return Err(format!(
                "Package entry '{raw}' exceeds the {} MB per-file limit",
                MAX_FILE_BYTES / 1024 / 1024
            ));
        }
        total = total
            .checked_add(entry.size())
            .ok_or_else(|| "Skill archive size overflow".to_string())?;
        if total > MAX_UNCOMPRESSED_BYTES {
            return Err(format!(
                "Skill archive exceeds the {} MB uncompressed limit",
                MAX_UNCOMPRESSED_BYTES / 1024 / 1024
            ));
        }

        let mut parts = raw.split('/');
        let top = parts.next().unwrap();
        let relative = parts.collect::<Vec<_>>().join("/");
        if relative.is_empty() {
            return Err(format!("Unexpected top-level file '{raw}'; .skill archives require one top-level skill directory"));
        }
        match &root {
            Some(existing) if existing != top => {
                return Err(
                    "Skill archive must contain exactly one top-level skill directory".into(),
                )
            }
            None => root = Some(top.to_string()),
            _ => {}
        }
        let canonical = if relative.eq_ignore_ascii_case("SKILL.md") {
            skill_md_count += 1;
            "SKILL.md".to_string()
        } else {
            relative
        };
        validate_portable_relative_path(&canonical)?;
        if !normalized.insert(canonical.to_lowercase()) {
            return Err(format!("Duplicate normalized package path '{canonical}'"));
        }
        if files.len() >= MAX_FILES {
            return Err(format!("Skill archive exceeds the {MAX_FILES} file limit"));
        }
        let mut content = Vec::with_capacity(entry.size().min(MAX_FILE_BYTES) as usize);
        entry
            .by_ref()
            .take(MAX_FILE_BYTES + 1)
            .read_to_end(&mut content)
            .map_err(|e| format!("Failed to decompress package entry '{raw}': {e}"))?;
        if content.len() as u64 > MAX_FILE_BYTES || content.len() as u64 != entry.size() {
            return Err(format!(
                "Package entry '{raw}' has a misleading or oversized uncompressed size"
            ));
        }
        files.push(FetchedFile {
            relative_path: canonical,
            is_binary: content.iter().take(8192).any(|byte| *byte == 0),
            content,
        });
    }

    let root = root.ok_or_else(|| "Skill archive contains no package files".to_string())?;
    if skill_md_count != 1 {
        return Err("Skill archive must contain exactly one case-insensitive SKILL.md".into());
    }
    let fetched = build_fetched_skill(files, FetchedSkillSource::LocalFile(filename.to_string()))?;
    if fetched.skill_id != root {
        return Err(format!(
            "SKILL.md name '{}' must match top-level directory '{root}'",
            fetched.skill_id
        ));
    }
    Ok(fetched)
}

fn excluded_export_path(path: &str) -> bool {
    path == ".meta.json"
        || ignored_metadata_path(path)
        || path.ends_with('~')
        || path.ends_with(".tmp")
        || path.ends_with(".swp")
}

/// Build a deterministic portable archive from an installed skill directory.
pub(crate) fn build_skill_archive(skill_id: &str, skill_dir: &Path) -> Result<Vec<u8>, String> {
    validate_portable_relative_path(skill_id)?;
    if skill_id.contains('/') {
        return Err("Skill id must be a single portable directory name".into());
    }
    let mut package_files = Vec::new();
    collect_export_files(skill_dir, skill_dir, &mut package_files)?;
    package_files.sort_by(|a, b| {
        let a_skill = a.0 != "SKILL.md";
        let b_skill = b.0 != "SKILL.md";
        a_skill.cmp(&b_skill).then_with(|| a.0.cmp(&b.0))
    });
    if !package_files.iter().any(|(path, _)| path == "SKILL.md") {
        return Err("Installed skill has no SKILL.md".into());
    }
    let cursor = Cursor::new(Vec::new());
    let mut writer = ZipWriter::new(cursor);
    let options = SimpleFileOptions::default()
        .compression_method(CompressionMethod::Deflated)
        .last_modified_time(zip::DateTime::default())
        .unix_permissions(0o644);
    for (relative, content) in package_files {
        writer
            .start_file(format!("{skill_id}/{relative}"), options)
            .map_err(|e| format!("Failed to create .skill entry: {e}"))?;
        writer
            .write_all(&content)
            .map_err(|e| format!("Failed to write .skill entry: {e}"))?;
    }
    writer
        .finish()
        .map(|cursor| cursor.into_inner())
        .map_err(|e| format!("Failed to finish .skill archive: {e}"))
}

fn collect_export_files(
    dir: &Path,
    base: &Path,
    output: &mut Vec<(String, Vec<u8>)>,
) -> Result<(), String> {
    for entry in std::fs::read_dir(dir).map_err(|e| format!("Failed to read skill package: {e}"))? {
        let entry = entry.map_err(|e| format!("Failed to read skill package entry: {e}"))?;
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|e| format!("Failed to inspect skill package entry: {e}"))?;
        if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
            return Err(format!(
                "Skill package contains unsupported entry '{}'",
                path.display()
            ));
        }
        if metadata.is_dir() {
            collect_export_files(&path, base, output)?;
            continue;
        }
        let relative = path
            .strip_prefix(base)
            .map_err(|_| "Invalid skill package path".to_string())?
            .to_string_lossy()
            .replace('\\', "/");
        if excluded_export_path(&relative) {
            continue;
        }
        validate_portable_relative_path(&relative)?;
        let content =
            std::fs::read(&path).map_err(|e| format!("Failed to read '{relative}': {e}"))?;
        output.push((relative, content));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn archive(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        for (path, content) in entries {
            writer
                .start_file(*path, SimpleFileOptions::default())
                .unwrap();
            writer.write_all(content).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    #[test]
    fn imports_portable_package_and_normalizes_skill_markdown() {
        let bytes = archive(&[
            (
                "demo/skill.md",
                b"---\nname: demo\ndescription: Demo\n---\nPrompt",
            ),
            ("demo/agents/openai.yaml", b"model: gpt-5"),
            ("demo/scripts/run.sh", b"echo ok"),
        ]);
        let skill = parse_skill_archive(&bytes, "demo.skill").unwrap();
        assert!(skill.files.iter().any(|f| f.relative_path == "SKILL.md"));
        assert!(skill
            .files
            .iter()
            .any(|f| f.relative_path == "agents/openai.yaml"));
        assert!(skill.has_scripts);
    }

    #[test]
    fn rejects_roots_names_and_traversal() {
        let mismatch = archive(&[(
            "other/SKILL.md",
            b"---\nname: demo\ndescription: Demo\n---\nPrompt",
        )]);
        assert!(parse_skill_archive(&mismatch, "x.skill")
            .unwrap_err()
            .contains("must match"));
        let roots = archive(&[
            (
                "one/SKILL.md",
                b"---\nname: one\ndescription: One\n---\nPrompt",
            ),
            ("two/file.txt", b"x"),
        ]);
        assert!(parse_skill_archive(&roots, "x.skill").is_err());
        for path in ["../x", "/x", "C:/x", "a\\..\\x", "a/../x"] {
            assert!(validate_portable_relative_path(path).is_err(), "{path}");
        }
    }

    #[test]
    fn rejects_duplicate_normalized_paths_and_malformed_zip() {
        let duplicate = archive(&[
            (
                "demo/SKILL.md",
                b"---\nname: demo\ndescription: Demo\n---\nPrompt",
            ),
            ("demo/readme", b"a"),
            ("demo/README", b"b"),
        ]);
        assert!(parse_skill_archive(&duplicate, "x.skill")
            .unwrap_err()
            .contains("Duplicate"));
        assert!(parse_skill_archive(b"not a zip", "x.skill")
            .unwrap_err()
            .contains("malformed"));
    }

    #[test]
    fn export_is_deterministic_and_round_trips_without_meta() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(
            temp.path().join("SKILL.md"),
            "---\nname: demo\ndescription: Demo\n---\nPrompt",
        )
        .unwrap();
        std::fs::create_dir(temp.path().join("agents")).unwrap();
        std::fs::write(temp.path().join("agents/openai.yaml"), "model: gpt-5").unwrap();
        std::fs::write(temp.path().join(".meta.json"), "secret").unwrap();
        let first = build_skill_archive("demo", temp.path()).unwrap();
        let second = build_skill_archive("demo", temp.path()).unwrap();
        assert_eq!(first, second);
        let imported = parse_skill_archive(&first, "demo.skill").unwrap();
        assert!(!imported
            .files
            .iter()
            .any(|f| f.relative_path == ".meta.json"));
    }
}
