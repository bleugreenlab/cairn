use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::process::Command;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PopulateConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub copy: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub symlink: Vec<String>,
}
impl PopulateConfig {
    pub fn is_empty(&self) -> bool {
        self.copy.is_empty() && self.symlink.is_empty()
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PopulateResult {
    pub copied: usize,
    pub symlinked: usize,
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
    fn symlink(&self, source: &Path, destination: &Path) -> Result<(), String>;
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
    fn symlink(&self, source: &Path, destination: &Path) -> Result<(), String> {
        symlink_path(source, destination).map_err(|e| e.to_string())
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
    let symlink = build_glob_set(&config.symlink)?;
    let mut result = PopulateResult::default();
    for raw_entry in backend.ignored_paths(source_root)? {
        let entry = raw_entry.replace('\\', "/");
        if entry == ".cairn/" || entry.starts_with(".cairn/") {
            result.skipped += 1;
            continue;
        }
        let relative = entry.trim_end_matches('/');
        let strategy = if copy.is_match(&entry) || copy.is_match(relative) {
            Some(true)
        } else if symlink.is_match(&entry) || symlink.is_match(relative) {
            Some(false)
        } else {
            None
        };
        let Some(copy_strategy) = strategy else {
            result.skipped += 1;
            continue;
        };
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
        let applied = if copy_strategy {
            if entry.ends_with('/') {
                backend.copy_dir_recursive(&source, &destination)
            } else {
                backend.copy_file(&source, &destination)
            }
        } else {
            backend.symlink(&source, &destination)
        };
        match applied {
            Ok(()) if copy_strategy => {
                result.copied += 1;
                result.paths.push(relative.to_string());
            }
            Ok(()) => {
                result.symlinked += 1;
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
        fn symlink(&self, _: &Path, _: &Path) -> Result<(), String> {
            Ok(())
        }
    }
    fn source(path: &str) -> PathBuf {
        Path::new("/source").join(path)
    }

    #[test]
    fn copy_precedes_symlink_and_cairn_is_excluded() {
        let mut fake = Fake {
            ignored: vec![".env".into(), ".cairn/".into()],
            ..Default::default()
        };
        fake.existing.insert(source(".env"));
        let result = populate(
            &fake,
            Path::new("/source"),
            Path::new("/dest"),
            &PopulateConfig {
                copy: vec![".env".into(), ".cairn/".into()],
                symlink: vec![".env".into(), ".cairn/".into()],
            },
        )
        .unwrap();
        assert_eq!(
            result,
            PopulateResult {
                copied: 1,
                skipped: 1,
                paths: vec![".env".into()],
                ..Default::default()
            }
        );
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
                symlink: vec![],
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
                symlink: vec![],
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
                symlink: vec![],
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
                symlink: vec![],
            },
        )
        .unwrap();
        assert_eq!(result.copied, 1);
    }
}
