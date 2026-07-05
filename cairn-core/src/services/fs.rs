//! File system service for I/O operations.
//!
//! Abstracts filesystem access to enable testing without real files.

use std::path::Path;

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

    /// Copy-on-write clone a directory tree when the filesystem supports it,
    /// falling back to recursive file reflinks/copies otherwise. The destination
    /// must not already exist.
    fn reflink_dir(&self, from: &Path, to: &Path) -> Result<(), String>;

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

    fn reflink_dir(&self, from: &Path, to: &Path) -> Result<(), String> {
        if to.exists() || self.is_symlink(to) {
            return Err(format!(
                "Destination already exists and cannot be directory-cloned: {:?}",
                to
            ));
        }
        if let Some(parent) = to.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create parent directory: {}", e))?;
        }

        #[cfg(target_os = "macos")]
        {
            if from.is_dir() {
                match clonefile_dir(from, to) {
                    Ok(()) => return Ok(()),
                    Err(error) => {
                        log::debug!(
                            "clonefile failed for {} to {}; falling back to recursive reflink/copy: {}",
                            from.display(),
                            to.display(),
                            error
                        );
                    }
                }
            }
        }

        self.copy_dir_recursive(from, to)
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
