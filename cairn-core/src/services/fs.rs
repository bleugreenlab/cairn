//! File system service for I/O operations.
//!
//! Abstracts filesystem access to enable testing without real files.

use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};

pub const GUARDED_TREE_MAX_FILES: usize = 500;
pub const GUARDED_TREE_MAX_FILE_BYTES: u64 = 10 * 1024 * 1024;
pub const GUARDED_TREE_MAX_TOTAL_BYTES: u64 = 25 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct GuardedTreeFile {
    pub relative_path: PathBuf,
    pub resolved_path: PathBuf,
    pub size: u64,
}

#[cfg(target_os = "macos")]
fn rename_exclusive(from: &Path, to: &Path) -> Result<(), String> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    const RENAME_EXCL: u32 = 0x0000_0004;

    #[link(name = "System")]
    unsafe extern "C" {
        fn renamex_np(
            from: *const std::os::raw::c_char,
            to: *const std::os::raw::c_char,
            flags: u32,
        ) -> i32;
    }

    let from = CString::new(from.as_os_str().as_bytes())
        .map_err(|_| format!("Source path contains an interior NUL byte: {from:?}"))?;
    let to = CString::new(to.as_os_str().as_bytes())
        .map_err(|_| format!("Destination path contains an interior NUL byte: {to:?}"))?;

    if unsafe { renamex_np(from.as_ptr(), to.as_ptr(), RENAME_EXCL) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().to_string())
    }
}

fn remove_staged_clone(path: &Path, operation_error: String) -> String {
    match std::fs::remove_dir_all(path) {
        Ok(()) => operation_error,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => operation_error,
        Err(error) => format!(
            "{operation_error}; additionally failed to clean staged clone {path:?}: {error}"
        ),
    }
}

pub fn guarded_resolve_file(root: &Path, path: &Path) -> Result<PathBuf, String> {
    let canonical_root = root
        .canonicalize()
        .map_err(|e| format!("Failed to resolve guarded tree root {root:?}: {e}"))?;
    let relative = path
        .strip_prefix(root)
        .map_err(|_| format!("Guarded file {path:?} is outside root {root:?}"))?;
    for component in relative.components() {
        match component {
            Component::Normal(name) => validate_tree_component(name)?,
            _ => return Err(format!("Unsafe guarded file path: {path:?}")),
        }
    }
    let resolved = path
        .canonicalize()
        .map_err(|e| format!("Dangling or unreadable guarded file {path:?}: {e}"))?;
    if !resolved.starts_with(&canonical_root) {
        return Err(format!("Guarded tree path escapes its root: {path:?}"));
    }
    let metadata = std::fs::metadata(&resolved)
        .map_err(|e| format!("Failed to inspect guarded file {path:?}: {e}"))?;
    if !metadata.is_file() {
        return Err(format!("Guarded path is not a regular file: {path:?}"));
    }
    if metadata.len() > GUARDED_TREE_MAX_FILE_BYTES {
        return Err(format!(
            "Guarded tree file {path:?} exceeds the per-file size limit"
        ));
    }
    Ok(resolved)
}

pub fn guarded_copy_file(root: &Path, source: &Path, destination: &Path) -> Result<(), String> {
    let resolved = guarded_resolve_file(root, source)?;
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create guarded destination {parent:?}: {e}"))?;
    }
    std::fs::copy(&resolved, destination)
        .map_err(|e| format!("Failed to copy guarded file {resolved:?}: {e}"))?;
    Ok(())
}

/// Enumerate regular files beneath 'root'. Links are allowed only when their
/// resolved targets remain within the canonical root.
pub fn guarded_tree_files(root: &Path) -> Result<Vec<GuardedTreeFile>, String> {
    let canonical_root = root
        .canonicalize()
        .map_err(|e| format!("Failed to resolve guarded tree root {root:?}: {e}"))?;
    if !canonical_root.is_dir() {
        return Err(format!("Guarded tree root is not a directory: {root:?}"));
    }
    let mut files = Vec::new();
    let mut visited = HashSet::new();
    let mut total = 0u64;
    walk_guarded_dir(
        &canonical_root,
        &canonical_root,
        Path::new(""),
        &mut visited,
        &mut files,
        &mut total,
    )?;
    files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    Ok(files)
}

fn walk_guarded_dir(
    canonical_root: &Path,
    dir: &Path,
    relative_dir: &Path,
    visited: &mut HashSet<PathBuf>,
    files: &mut Vec<GuardedTreeFile>,
    total: &mut u64,
) -> Result<(), String> {
    let resolved_dir = dir
        .canonicalize()
        .map_err(|e| format!("Failed to resolve guarded directory {dir:?}: {e}"))?;
    if !resolved_dir.starts_with(canonical_root) {
        return Err(format!("Guarded tree path escapes its root: {dir:?}"));
    }
    if !visited.insert(resolved_dir.clone()) {
        return Err(format!(
            "Guarded tree contains a directory link cycle at {dir:?}"
        ));
    }
    let mut children = std::fs::read_dir(&resolved_dir)
        .map_err(|e| format!("Failed to read guarded directory {dir:?}: {e}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to read guarded directory entry: {e}"))?;
    children.sort_by_key(|entry| entry.file_name());
    for child in children {
        let name = child.file_name();
        validate_tree_component(&name)?;
        let relative_path = relative_dir.join(&name);
        let path = child.path();
        let link_metadata = std::fs::symlink_metadata(&path)
            .map_err(|e| format!("Failed to inspect guarded path {path:?}: {e}"))?;
        let resolved = path
            .canonicalize()
            .map_err(|e| format!("Dangling or unreadable guarded path {path:?}: {e}"))?;
        if !resolved.starts_with(canonical_root) {
            return Err(format!("Guarded tree path escapes its root: {path:?}"));
        }
        let metadata = std::fs::metadata(&path)
            .map_err(|e| format!("Failed to inspect guarded target {path:?}: {e}"))?;
        if metadata.is_dir() {
            walk_guarded_dir(
                canonical_root,
                &resolved,
                &relative_path,
                visited,
                files,
                total,
            )?;
        } else if metadata.is_file() {
            let size = metadata.len();
            if size > GUARDED_TREE_MAX_FILE_BYTES {
                return Err(format!(
                    "Guarded tree file {path:?} exceeds the per-file size limit"
                ));
            }
            *total = total
                .checked_add(size)
                .ok_or_else(|| "Guarded tree total size overflowed".to_string())?;
            if *total > GUARDED_TREE_MAX_TOTAL_BYTES {
                return Err("Guarded tree exceeds the total size limit".into());
            }
            files.push(GuardedTreeFile {
                relative_path,
                resolved_path: resolved,
                size,
            });
            if files.len() > GUARDED_TREE_MAX_FILES {
                return Err("Guarded tree exceeds the file count limit".into());
            }
        } else {
            let kind = if link_metadata.file_type().is_symlink() {
                "symbolic-link target"
            } else {
                "special file"
            };
            return Err(format!(
                "Guarded tree contains unsupported {kind}: {path:?}"
            ));
        }
    }
    visited.remove(&resolved_dir);
    Ok(())
}

fn validate_tree_component(name: &std::ffi::OsStr) -> Result<(), String> {
    let text = name
        .to_str()
        .ok_or_else(|| format!("Guarded tree path is not valid UTF-8: {name:?}"))?;
    if text.is_empty()
        || text.contains('\0')
        || text.contains('\\')
        || matches!(text, "." | "..")
        || (text.len() >= 2
            && text.as_bytes()[0].is_ascii_alphabetic()
            && text.as_bytes()[1] == b':')
    {
        return Err(format!("Unsafe guarded tree path component: {text:?}"));
    }
    if Path::new(text)
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!("Unsafe guarded tree path component: {text:?}"));
    }
    Ok(())
}

pub fn guarded_copy_tree(root: &Path, destination: &Path) -> Result<(), String> {
    let files = guarded_tree_files(root)?;
    std::fs::create_dir_all(destination)
        .map_err(|e| format!("Failed to create guarded destination {destination:?}: {e}"))?;
    for file in files {
        let target = destination.join(&file.relative_path);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create guarded destination {parent:?}: {e}"))?;
        }
        std::fs::copy(&file.resolved_path, &target)
            .map_err(|e| format!("Failed to copy guarded file {:?}: {e}", file.resolved_path))?;
    }
    Ok(())
}

#[cfg(any(test, feature = "test-utils"))]
use mockall::automock;

/// Trait for file system operations.
///
/// This abstraction allows tests to mock file operations
/// without touching the real filesystem.
#[cfg_attr(any(test, feature = "test-utils"), automock)]
pub trait FileSystem: Send + Sync {
    /// Check if a path exists.
    fn exists(&self, path: &Path) -> bool;

    /// Create a directory and all parent directories.
    fn create_dir_all(&self, path: &Path) -> Result<(), String>;

    /// Read file contents as bytes.
    fn read(&self, path: &Path) -> Result<Vec<u8>, String>;

    /// Read file contents as string.
    fn read_to_string(&self, path: &Path) -> Result<String, String>;

    /// Write bytes to a file, creating it if needed.
    fn write(&self, path: &Path, contents: &[u8]) -> Result<(), String>;

    /// Write string to a file, creating it if needed.
    fn write_str(&self, path: &Path, contents: &str) -> Result<(), String>;

    /// Remove a file.
    fn remove_file(&self, path: &Path) -> Result<(), String>;

    /// Remove a directory and all its contents.
    fn remove_dir_all(&self, path: &Path) -> Result<(), String>;

    /// Copy a file from one location to another.
    /// Creates parent directories of the destination if they don't exist.
    fn copy_file(&self, from: &Path, to: &Path) -> Result<(), String>;

    /// Copy-on-write clone a file when the filesystem supports it, falling back
    /// to a regular byte copy otherwise. Creates parent directories of the
    /// destination if they don't exist.
    fn reflink_file(&self, from: &Path, to: &Path) -> Result<(), String>;

    /// STRICTLY copy-on-write clone a directory tree, with NO byte-copy fallback.
    /// Returns `Err` whenever a cheap COW clone is unavailable (a non-APFS volume,
    /// a cross-volume destination, or a clone failure) so the caller can route to a
    /// non-cloning path instead of silently deep-copying a multi-GB build tree
    /// (`src-tauri/target`, `node_modules`). The destination must not already
    /// exist. macOS-only for now; other platforms always `Err` (a Linux
    /// reflink-per-file variant can come later — the sequential fallback covers
    /// those hosts).
    fn try_clone_dir_cow(&self, from: &Path, to: &Path) -> Result<(), String>;

    /// Create a symbolic link at `link` pointing to `target`.
    /// On Windows, uses a directory junction for directories.
    fn symlink(&self, target: &Path, link: &Path) -> Result<(), String>;

    /// Check if a path is a symbolic link (or junction on Windows).
    fn is_symlink(&self, path: &Path) -> bool;

    /// Recursively copy a directory from one location to another.
    /// Creates the destination directory and copies all contents.
    fn copy_dir_recursive(&self, from: &Path, to: &Path) -> Result<(), String>;
}

/// Production filesystem implementation using std::fs.
pub struct RealFileSystem;

#[cfg(target_os = "macos")]
fn clonefile_dir(from: &Path, to: &Path) -> Result<(), String> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    #[link(name = "System")]
    unsafe extern "C" {
        fn clonefile(
            src: *const std::os::raw::c_char,
            dst: *const std::os::raw::c_char,
            flags: u32,
        ) -> i32;
    }

    let src = CString::new(from.as_os_str().as_bytes())
        .map_err(|_| format!("Source path contains an interior NUL byte: {:?}", from))?;
    let dst = CString::new(to.as_os_str().as_bytes())
        .map_err(|_| format!("Destination path contains an interior NUL byte: {:?}", to))?;

    let result = unsafe { clonefile(src.as_ptr(), dst.as_ptr(), 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().to_string())
    }
}

impl FileSystem for RealFileSystem {
    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn create_dir_all(&self, path: &Path) -> Result<(), String> {
        std::fs::create_dir_all(path).map_err(|e| format!("Failed to create directory: {}", e))
    }

    fn read(&self, path: &Path) -> Result<Vec<u8>, String> {
        std::fs::read(path).map_err(|e| format!("Failed to read file: {}", e))
    }

    fn read_to_string(&self, path: &Path) -> Result<String, String> {
        std::fs::read_to_string(path).map_err(|e| format!("Failed to read file: {}", e))
    }

    fn write(&self, path: &Path, contents: &[u8]) -> Result<(), String> {
        std::fs::write(path, contents).map_err(|e| format!("Failed to write file: {}", e))
    }

    fn write_str(&self, path: &Path, contents: &str) -> Result<(), String> {
        std::fs::write(path, contents).map_err(|e| format!("Failed to write file: {}", e))
    }

    fn remove_file(&self, path: &Path) -> Result<(), String> {
        std::fs::remove_file(path).map_err(|e| format!("Failed to remove file: {}", e))
    }

    fn remove_dir_all(&self, path: &Path) -> Result<(), String> {
        std::fs::remove_dir_all(path).map_err(|e| format!("Failed to remove directory: {}", e))
    }

    fn copy_file(&self, from: &Path, to: &Path) -> Result<(), String> {
        // Create parent directories if needed
        if let Some(parent) = to.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create parent directory: {}", e))?;
        }
        std::fs::copy(from, to).map_err(|e| format!("Failed to copy file: {}", e))?;
        Ok(())
    }

    fn reflink_file(&self, from: &Path, to: &Path) -> Result<(), String> {
        if let Some(parent) = to.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create parent directory: {}", e))?;
        }
        if to.exists() || self.is_symlink(to) {
            std::fs::remove_file(to)
                .map_err(|e| format!("Failed to replace existing destination {:?}: {}", to, e))?;
        }
        match reflink_copy::reflink_or_copy(from, to) {
            Ok(None) => Ok(()),
            Ok(Some(_bytes)) => Ok(()),
            Err(e) => Err(format!(
                "Failed to reflink or copy {:?} to {:?}: {}",
                from, to, e
            )),
        }
    }

    fn try_clone_dir_cow(&self, from: &Path, to: &Path) -> Result<(), String> {
        if to.exists() || self.is_symlink(to) {
            return Err(format!(
                "Destination already exists and cannot be COW-cloned: {:?}",
                to
            ));
        }
        if let Some(parent) = to.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create parent directory: {}", e))?;
        }

        #[cfg(target_os = "macos")]
        {
            let parent = to
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let destination_name = to
                .file_name()
                .ok_or_else(|| format!("COW clone destination has no file name: {to:?}"))?
                .to_string_lossy();
            let staging = parent.join(format!(
                ".{destination_name}.cairn-clone-{}",
                uuid::Uuid::new_v4()
            ));

            if let Err(error) = clonefile_dir(from, &staging) {
                return Err(remove_staged_clone(
                    &staging,
                    format!("Failed to COW-clone directory {from:?}: {error}"),
                ));
            }
            if let Err(error) = rename_exclusive(&staging, to) {
                return Err(remove_staged_clone(
                    &staging,
                    format!("Failed to atomically publish COW clone at {to:?}: {error}"),
                ));
            }
            Ok(())
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = from;
            Err("COW directory clone not supported on this platform".to_string())
        }
    }

    fn symlink(&self, target: &Path, link: &Path) -> Result<(), String> {
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(target, link)
                .map_err(|e| format!("Failed to create symlink: {}", e))
        }
        #[cfg(windows)]
        {
            // Use junction for directories (no admin required), symlink for files
            if target.is_dir() {
                std::os::windows::fs::symlink_dir(target, link)
                    .or_else(|_| {
                        // Fall back to junction if symlink fails (requires privileges)
                        std::process::Command::new("cmd")
                            .args(["/c", "mklink", "/J"])
                            .arg(link.as_os_str())
                            .arg(target.as_os_str())
                            .output()
                            .map_err(|e| format!("Failed to create junction: {}", e))
                            .and_then(|o| {
                                if o.status.success() {
                                    Ok(())
                                } else {
                                    Err(format!(
                                        "mklink /J failed: {}",
                                        String::from_utf8_lossy(&o.stderr)
                                    ))
                                }
                            })
                    })
                    .map_err(|e| format!("Failed to create directory link: {}", e))
            } else {
                std::os::windows::fs::symlink_file(target, link)
                    .map_err(|e| format!("Failed to create file symlink: {}", e))
            }
        }
    }

    fn is_symlink(&self, path: &Path) -> bool {
        path.symlink_metadata()
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false)
    }

    fn copy_dir_recursive(&self, from: &Path, to: &Path) -> Result<(), String> {
        std::fs::create_dir_all(to)
            .map_err(|e| format!("Failed to create directory {:?}: {}", to, e))?;
        let entries = std::fs::read_dir(from)
            .map_err(|e| format!("Failed to read directory {:?}: {}", from, e))?;
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
            let src_path = entry.path();
            let dst_path = to.join(entry.file_name());
            if src_path.is_dir() {
                self.copy_dir_recursive(&src_path, &dst_path)?;
            } else {
                self.reflink_file(&src_path, &dst_path)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn real_fs_fixture() -> (TempDir, RealFileSystem) {
        (TempDir::new().unwrap(), RealFileSystem)
    }

    #[cfg(unix)]
    #[test]
    fn guarded_tree_allows_contained_file_links_and_copies_bytes() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("tree");
        let destination = temp.path().join("copy");
        std::fs::create_dir_all(root.join("content")).unwrap();
        std::fs::write(root.join("content/value.txt"), "safe").unwrap();
        std::os::unix::fs::symlink("content/value.txt", root.join("linked.txt")).unwrap();

        let files = guarded_tree_files(&root).unwrap();
        assert_eq!(files.len(), 2);
        guarded_copy_tree(&root, &destination).unwrap();
        assert_eq!(
            std::fs::read_to_string(destination.join("linked.txt")).unwrap(),
            "safe"
        );
        assert!(!destination.join("linked.txt").is_symlink());
    }

    #[cfg(unix)]
    #[test]
    fn guarded_tree_rejects_escape_and_dangling_links() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("tree");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(temp.path().join("secret"), "outside").unwrap();
        std::os::unix::fs::symlink("../secret", root.join("escape")).unwrap();
        assert!(guarded_tree_files(&root)
            .unwrap_err()
            .contains("escapes its root"));

        std::fs::remove_file(root.join("escape")).unwrap();
        std::os::unix::fs::symlink("missing", root.join("dangling")).unwrap();
        assert!(guarded_tree_files(&root).unwrap_err().contains("Dangling"));
    }

    #[test]
    fn guarded_tree_enforces_per_file_size_limit() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("tree");
        std::fs::create_dir_all(&root).unwrap();
        let file = std::fs::File::create(root.join("large")).unwrap();
        file.set_len(GUARDED_TREE_MAX_FILE_BYTES + 1).unwrap();
        assert!(guarded_tree_files(&root)
            .unwrap_err()
            .contains("per-file size limit"));
    }

    #[test]
    fn real_fs_create_and_check_exists() {
        let (temp, fs) = real_fs_fixture();

        let new_dir = temp.path().join("subdir");
        assert!(!fs.exists(&new_dir));

        fs.create_dir_all(&new_dir).unwrap();
        assert!(fs.exists(&new_dir));
    }

    #[test]
    fn real_fs_write_and_read() {
        let (temp, fs) = real_fs_fixture();

        let file = temp.path().join("test.txt");
        fs.write(&file, b"hello").unwrap();

        let contents = fs.read(&file).unwrap();
        assert_eq!(contents, b"hello");
    }

    #[test]
    fn real_fs_write_str_and_read_to_string() {
        let (temp, fs) = real_fs_fixture();

        let file = temp.path().join("test.txt");
        fs.write_str(&file, "hello world").unwrap();

        let contents = fs.read_to_string(&file).unwrap();
        assert_eq!(contents, "hello world");
    }

    #[test]
    fn real_fs_remove_file() {
        let (temp, fs) = real_fs_fixture();

        let file = temp.path().join("test.txt");
        fs.write_str(&file, "content").unwrap();
        assert!(fs.exists(&file));

        fs.remove_file(&file).unwrap();
        assert!(!fs.exists(&file));
    }

    #[test]
    fn real_fs_remove_dir_all() {
        let (temp, fs) = real_fs_fixture();

        let dir = temp.path().join("mydir");
        fs.create_dir_all(&dir.join("subdir")).unwrap();
        fs.write_str(&dir.join("file.txt"), "content").unwrap();
        assert!(fs.exists(&dir));

        fs.remove_dir_all(&dir).unwrap();
        assert!(!fs.exists(&dir));
    }

    #[test]
    fn mock_fs_returns_configured_values() {
        let mut mock = MockFileSystem::new();
        mock.expect_exists().returning(|_| true);
        mock.expect_read_to_string()
            .returning(|_| Ok("mocked content".to_string()));

        assert!(mock.exists(Path::new("/any/path")));
        assert_eq!(
            mock.read_to_string(Path::new("/file")).unwrap(),
            "mocked content"
        );
    }

    #[test]
    fn real_fs_copy_file() {
        let (temp, fs) = real_fs_fixture();

        // Create source file
        let src = temp.path().join("source.txt");
        fs.write_str(&src, "hello world").unwrap();

        // Copy to destination
        let dst = temp.path().join("dest.txt");
        fs.copy_file(&src, &dst).unwrap();

        // Verify contents
        let contents = fs.read_to_string(&dst).unwrap();
        assert_eq!(contents, "hello world");
    }

    #[test]
    fn real_fs_copy_file_creates_parent_dirs() {
        let (temp, fs) = real_fs_fixture();

        // Create source file
        let src = temp.path().join("source.txt");
        fs.write_str(&src, "content").unwrap();

        // Copy to nested destination
        let dst = temp.path().join("nested").join("subdir").join("dest.txt");
        fs.copy_file(&src, &dst).unwrap();

        // Verify file exists and has correct content
        assert!(fs.exists(&dst));
        let contents = fs.read_to_string(&dst).unwrap();
        assert_eq!(contents, "content");
    }

    #[test]
    fn real_fs_reflink_file() {
        let (temp, fs) = real_fs_fixture();

        let src = temp.path().join("source.txt");
        fs.write_str(&src, "hello reflink").unwrap();

        let dst = temp.path().join("nested").join("dest.txt");
        fs.reflink_file(&src, &dst).unwrap();

        assert!(fs.exists(&dst));
        assert_eq!(fs.read_to_string(&dst).unwrap(), "hello reflink");
    }

    #[test]
    fn real_fs_reflink_file_overwrites_existing() {
        let (temp, fs) = real_fs_fixture();

        let src = temp.path().join("source.txt");
        fs.write_str(&src, "new content").unwrap();

        let dst = temp.path().join("dest.txt");
        fs.write_str(&dst, "old content").unwrap();
        fs.reflink_file(&src, &dst).unwrap();

        assert_eq!(fs.read_to_string(&dst).unwrap(), "new content");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn real_fs_clone_dir_cow_publishes_complete_independent_tree() {
        let (temp, fs) = real_fs_fixture();
        let source = temp.path().join("catalog");
        let destination = temp.path().join("slot-target");
        fs.create_dir_all(&source.join("debug/deps")).unwrap();
        fs.write_str(&source.join("debug/deps/library.rlib"), "artifact")
            .unwrap();

        fs.try_clone_dir_cow(&source, &destination).unwrap();
        assert_eq!(
            fs.read_to_string(&destination.join("debug/deps/library.rlib"))
                .unwrap(),
            "artifact"
        );

        fs.remove_dir_all(&source).unwrap();
        assert_eq!(
            fs.read_to_string(&destination.join("debug/deps/library.rlib"))
                .unwrap(),
            "artifact"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn real_fs_clone_dir_cow_refuses_existing_destination_without_modifying_it() {
        let (temp, fs) = real_fs_fixture();
        let source = temp.path().join("catalog");
        let destination = temp.path().join("slot-target");
        fs.create_dir_all(&source).unwrap();
        fs.write_str(&source.join("new"), "new").unwrap();
        fs.create_dir_all(&destination).unwrap();
        fs.write_str(&destination.join("existing"), "existing")
            .unwrap();

        let error = fs.try_clone_dir_cow(&source, &destination).unwrap_err();

        assert!(error.contains("Destination already exists"));
        assert_eq!(
            fs.read_to_string(&destination.join("existing")).unwrap(),
            "existing"
        );
        assert!(!destination.join("new").exists());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn real_fs_clone_dir_cow_cleans_staging_after_clone_failure() {
        let (temp, fs) = real_fs_fixture();
        let source = temp.path().join("missing-catalog");
        let destination = temp.path().join("slot-target");

        assert!(fs.try_clone_dir_cow(&source, &destination).is_err());
        assert!(!destination.exists());
        let names = std::fs::read_dir(temp.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert!(names.is_empty(), "staging residue remained: {names:?}");
    }

    #[test]
    fn real_fs_symlink_and_is_symlink() {
        let (temp, fs) = real_fs_fixture();

        // Create a target directory
        let target = temp.path().join("target_dir");
        fs.create_dir_all(&target).unwrap();
        fs.write_str(&target.join("file.txt"), "hello").unwrap();

        // Create symlink
        let link = temp.path().join("link_dir");
        fs.symlink(&target, &link).unwrap();

        // Verify it's a symlink
        assert!(fs.is_symlink(&link));
        assert!(fs.exists(&link));

        // Verify content is accessible through symlink
        let content = fs.read_to_string(&link.join("file.txt")).unwrap();
        assert_eq!(content, "hello");

        // Non-symlink path should return false
        assert!(!fs.is_symlink(&target));
    }

    #[test]
    fn real_fs_symlink_file() {
        let (temp, fs) = real_fs_fixture();

        let target = temp.path().join("target.txt");
        fs.write_str(&target, "content").unwrap();

        let link = temp.path().join("link.txt");
        fs.symlink(&target, &link).unwrap();

        assert!(fs.is_symlink(&link));
        assert_eq!(fs.read_to_string(&link).unwrap(), "content");
    }

    #[test]
    fn real_fs_copy_file_overwrites_existing() {
        let (temp, fs) = real_fs_fixture();

        // Create source and existing destination
        let src = temp.path().join("source.txt");
        fs.write_str(&src, "new content").unwrap();

        let dst = temp.path().join("dest.txt");
        fs.write_str(&dst, "old content").unwrap();

        // Copy should overwrite
        fs.copy_file(&src, &dst).unwrap();

        let contents = fs.read_to_string(&dst).unwrap();
        assert_eq!(contents, "new content");
    }

    #[test]
    fn real_fs_copy_dir_recursive() {
        let (temp, fs) = real_fs_fixture();

        // Create a source directory with nested structure
        let src = temp.path().join("source_dir");
        fs.create_dir_all(&src.join("sub")).unwrap();
        fs.write_str(&src.join("root.txt"), "root file").unwrap();
        fs.write_str(&src.join("sub").join("nested.txt"), "nested file")
            .unwrap();

        // Copy to destination
        let dst = temp.path().join("dest_dir");
        fs.copy_dir_recursive(&src, &dst).unwrap();

        // Verify structure was reproduced
        assert!(fs.exists(&dst));
        assert!(fs.exists(&dst.join("sub")));
        assert_eq!(
            fs.read_to_string(&dst.join("root.txt")).unwrap(),
            "root file"
        );
        assert_eq!(
            fs.read_to_string(&dst.join("sub").join("nested.txt"))
                .unwrap(),
            "nested file"
        );
    }

    #[test]
    fn real_fs_copy_dir_recursive_nonexistent_source() {
        let (temp, fs) = real_fs_fixture();

        let src = temp.path().join("does_not_exist");
        let dst = temp.path().join("dest");

        let result = fs.copy_dir_recursive(&src, &dst);
        assert!(result.is_err());
    }
}
