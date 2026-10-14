use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::process::Command;

/// Which Git-ignored paths of the source checkout are copied into a new cell.
/// Paths no `copy` pattern matches are skipped and left to setup commands.
///
/// Cairn creates no symlinks inside build cells, so the retired `symlink`
/// strategy is rejected when a config still names it rather than ignored: a
/// silently dropped rule would leave the project believing its cells are warm.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", try_from = "PopulateConfigInput")]
pub struct PopulateConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub copy: Vec<String>,
}
impl PopulateConfig {
    pub fn is_empty(&self) -> bool {
        self.copy.is_empty()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PopulateConfigInput {
    #[serde(default)]
    copy: Vec<String>,
    #[serde(default)]
    symlink: Option<Vec<String>>,
}

impl TryFrom<PopulateConfigInput> for PopulateConfig {
    type Error = String;

    fn try_from(input: PopulateConfigInput) -> Result<Self, Self::Error> {
        match input.symlink {
            Some(patterns) if !patterns.is_empty() => Err(format!(
                "materialization.populate.symlink is no longer supported (patterns: {}): \
                 Cairn does not create symlinks inside build cells. Move any pattern a cell \
                 needs to materialization.populate.copy, or delete it; ignored paths that \
                 no copy pattern matches are skipped and left to setupCommands.",
                patterns.join(", ")
            )),
            _ => Ok(Self { copy: input.copy }),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PopulateResult {
    pub copied: usize,
    pub skipped: usize,
    pub failed: usize,
    pub paths: Vec<String>,
}

pub trait PopulateBackend {
    fn ignored_paths(&self, source_root: &Path) -> Result<Vec<String>, String>;
    fn exists(&self, path: &Path) -> bool;
    fn is_symlink(&self, path: &Path) -> bool;
    fn create_dir_all(&self, path: &Path) -> Result<(), String>;
    fn copy_file(&self, source: &Path, destination: &Path) -> Result<(), String>;
    fn copy_dir_recursive(&self, source: &Path, destination: &Path) -> Result<(), String>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RealPopulateBackend;
impl PopulateBackend for RealPopulateBackend {
    fn ignored_paths(&self, source_root: &Path) -> Result<Vec<String>, String> {
        let output = Command::new("git")
            .args([
                "ls-files",
                "--others",
                "--ignored",
                "--exclude-standard",
                "--directory",
            ])
            .current_dir(source_root)
            .output()
            .map_err(|error| format!("run git ls-files: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "git ls-files failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect())
    }
    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }
    fn is_symlink(&self, path: &Path) -> bool {
        fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink())
    }
    fn create_dir_all(&self, path: &Path) -> Result<(), String> {
        fs::create_dir_all(path).map_err(|e| e.to_string())
    }
    fn copy_file(&self, source: &Path, destination: &Path) -> Result<(), String> {
        fs::copy(source, destination)
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    fn copy_dir_recursive(&self, source: &Path, destination: &Path) -> Result<(), String> {
        copy_dir_recursive(source, destination)
    }
}

fn copy_dir_recursive(source: &Path, destination: &Path) -> Result<(), String> {
    fs::create_dir_all(destination).map_err(|e| e.to_string())?;
    for entry in fs::read_dir(source).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let source = entry.path();
        let destination = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(&source).map_err(|e| e.to_string())?;
        if metadata.is_dir() {
            copy_dir_recursive(&source, &destination)?;
        } else if metadata.file_type().is_symlink() {
            // A link already inside a copied tree (`node_modules/.bin`) is
            // reproduced as the same link, never followed: following could copy
            // content from outside the tree or loop.
            let target = fs::read_link(&source).map_err(|e| e.to_string())?;
            symlink_path(&target, &destination).map_err(|e| e.to_string())?;
        } else {
            fs::copy(&source, &destination).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}
#[cfg(unix)]
fn symlink_path(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(source, destination)
}
#[cfg(windows)]
fn symlink_path(source: &Path, destination: &Path) -> std::io::Result<()> {
    if source.is_dir() {
        std::os::windows::fs::symlink_dir(source, destination)
    } else {
        std::os::windows::fs::symlink_file(source, destination)
    }
}

fn build_glob_set(patterns: &[String]) -> Result<GlobSet, String> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(
            GlobBuilder::new(pattern)
                .literal_separator(false)
                .build()
                .map_err(|error| format!("Invalid glob pattern '{pattern}': {error}"))?,
        );
    }
    builder
        .build()
        .map_err(|error| format!("Failed to build glob set: {error}"))
}

/// Populate a destination checkout from ignored paths discovered in a source checkout.
pub fn populate<B: PopulateBackend>(
    backend: &B,
    source_root: &Path,
    destination_root: &Path,
    config: &PopulateConfig,
) -> Result<PopulateResult, String> {
    if config.is_empty() {
        return Ok(PopulateResult::default());
    }
    let copy = build_glob_set(&config.copy)?;
    let mut result = PopulateResult::default();
    for raw_entry in backend.ignored_paths(source_root)? {
        let entry = raw_entry.replace('\\', "/");
        if entry == ".cairn/" || entry.starts_with(".cairn/") {
            result.skipped += 1;
            continue;
        }
        let relative = entry.trim_end_matches('/');
        if !copy.is_match(&entry) && !copy.is_match(relative) {
            result.skipped += 1;
            continue;
        }
        let source = source_root.join(relative);
        let destination = destination_root.join(relative);
        if !backend.exists(&source)
            || backend.exists(&destination)
            || backend.is_symlink(&destination)
        {
            result.skipped += 1;
            continue;
        }
        if let Some(parent) = destination.parent() {
            if let Err(error) = backend.create_dir_all(parent) {
                tracing::warn!(path = %entry, %error, "failed to create populate destination parent");
                result.failed += 1;
                continue;
            }
        }
        let applied = if entry.ends_with('/') {
            backend.copy_dir_recursive(&source, &destination)
        } else {
            backend.copy_file(&source, &destination)
        };
        match applied {
            Ok(()) => {
                result.copied += 1;
                result.paths.push(relative.to_string());
            }
            Err(error) => {
                tracing::warn!(path = %entry, %error, "failed to populate ignored path");
                result.failed += 1;
            }
        }
    }
    Ok(result)
}

pub fn populate_real(
    source_root: &Path,
    destination_root: &Path,
    config: &PopulateConfig,
) -> Result<PopulateResult, String> {
    populate(&RealPopulateBackend, source_root, destination_root, config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::path::PathBuf;

    #[derive(Default)]
    struct Fake {
        ignored: Vec<String>,
        existing: HashSet<PathBuf>,
        symlinks: HashSet<PathBuf>,
        fail_copy: HashSet<PathBuf>,
    }
    impl PopulateBackend for Fake {
        fn ignored_paths(&self, _: &Path) -> Result<Vec<String>, String> {
            Ok(self.ignored.clone())
        }
        fn exists(&self, path: &Path) -> bool {
            self.existing.contains(path)
        }
        fn is_symlink(&self, path: &Path) -> bool {
            self.symlinks.contains(path)
        }
        fn create_dir_all(&self, _: &Path) -> Result<(), String> {
            Ok(())
        }
        fn copy_file(&self, source: &Path, _: &Path) -> Result<(), String> {
            if self.fail_copy.contains(source) {
                Err("copy failed".into())
            } else {
                Ok(())
            }
        }
        fn copy_dir_recursive(&self, source: &Path, _: &Path) -> Result<(), String> {
            if self.fail_copy.contains(source) {
                Err("copy failed".into())
            } else {
                Ok(())
            }
        }
    }
    fn source(path: &str) -> PathBuf {
        Path::new("/source").join(path)
    }

    #[test]
    fn copy_matches_unmatched_skip_and_cairn_is_excluded() {
        let mut fake = Fake {
            ignored: vec![".env".into(), ".cairn/".into(), "target/".into()],
            ..Default::default()
        };
        fake.existing
            .extend([source(".env"), source("target"), source(".cairn")]);
        let result = populate(
            &fake,
            Path::new("/source"),
            Path::new("/dest"),
            &PopulateConfig {
                copy: vec![".env".into(), ".cairn/".into()],
            },
        )
        .unwrap();
        assert_eq!(
            result,
            PopulateResult {
                copied: 1,
                skipped: 2,
                paths: vec![".env".into()],
                ..Default::default()
            }
        );
    }
    #[cfg(unix)]
    #[test]
    fn real_populate_creates_no_symlinks_for_copied_paths() {
        let temp = tempfile::tempdir().unwrap();
        let source_root = temp.path().join("source");
        let destination_root = temp.path().join("dest");
        fs::create_dir_all(source_root.join("cache")).unwrap();
        fs::write(source_root.join("cache/data"), b"warm").unwrap();
        fs::write(source_root.join(".gitignore"), "cache/\n").unwrap();
        fs::create_dir_all(&destination_root).unwrap();
        let git = |args: &[&str]| {
            assert!(Command::new("git")
                .args(args)
                .current_dir(&source_root)
                .output()
                .unwrap()
                .status
                .success());
        };
        git(&["init", "-q"]);

        let result = populate_real(
            &source_root,
            &destination_root,
            &PopulateConfig {
                copy: vec!["cache/".into()],
            },
        )
        .unwrap();

        assert_eq!(result.copied, 1);
        let copied = destination_root.join("cache");
        assert!(!fs::symlink_metadata(&copied).unwrap().is_symlink());
        assert_eq!(fs::read(copied.join("data")).unwrap(), b"warm");
    }

    #[test]
    fn preserves_existing_and_dangling_symlink_destinations() {
        let mut fake = Fake {
            ignored: vec!["one".into(), "two".into()],
            ..Default::default()
        };
        fake.existing
            .extend([source("one"), source("two"), PathBuf::from("/dest/one")]);
        fake.symlinks.insert(PathBuf::from("/dest/two"));
        let result = populate(
            &fake,
            Path::new("/source"),
            Path::new("/dest"),
            &PopulateConfig {
                copy: vec!["*".into()],
            },
        )
        .unwrap();
        assert_eq!(result.skipped, 2);
    }
    #[test]
    fn directory_copy_and_entry_failures_are_best_effort() {
        let mut fake = Fake {
            ignored: vec!["cache/".into(), ".env".into()],
            ..Default::default()
        };
        fake.existing.extend([source("cache"), source(".env")]);
        fake.fail_copy.insert(source("cache"));
        let result = populate(
            &fake,
            Path::new("/source"),
            Path::new("/dest"),
            &PopulateConfig {
                copy: vec!["*".into()],
            },
        )
        .unwrap();
        assert_eq!((result.failed, result.copied), (1, 1));
    }
    #[test]
    fn invalid_glob_is_fatal() {
        let error = populate(
            &Fake::default(),
            Path::new("/source"),
            Path::new("/dest"),
            &PopulateConfig {
                copy: vec!["[bad".into()],
            },
        )
        .unwrap_err();
        assert!(error.contains("Invalid glob pattern"));
    }
    #[test]
    fn normalizes_windows_separators() {
        let mut fake = Fake {
            ignored: vec![r"cache\\data/".into()],
            ..Default::default()
        };
        fake.existing.insert(source("cache/data"));
        let result = populate(
            &fake,
            Path::new("/source"),
            Path::new("/dest"),
            &PopulateConfig {
                copy: vec!["cache/**".into()],
            },
        )
        .unwrap();
        assert_eq!(result.copied, 1);
    }
}
