use super::*;
use cairn_worktree::{PopulateBackend, PopulateConfig};

struct CorePopulateBackend<'a> {
    git: &'a dyn GitClient,
    fs: &'a dyn FileSystem,
}
impl PopulateBackend for CorePopulateBackend<'_> {
    fn ignored_paths(&self, source_root: &Path) -> Result<Vec<String>, String> {
        let output = self.git.run(
            source_root,
            vec![
                "ls-files".into(),
                "--others".into(),
                "--ignored".into(),
                "--exclude-standard".into(),
                "--directory".into(),
            ],
        )?;
        if !output.success {
            return Err(format!("git ls-files failed: {}", output.stderr.trim()));
        }
        Ok(output
            .stdout
            .lines()
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect())
    }
    fn exists(&self, path: &Path) -> bool {
        self.fs.exists(path)
    }
    fn is_symlink(&self, path: &Path) -> bool {
        self.fs.is_symlink(path)
    }
    fn create_dir_all(&self, path: &Path) -> Result<(), String> {
        self.fs.create_dir_all(path)
    }
    fn copy_file(&self, source: &Path, destination: &Path) -> Result<(), String> {
        self.fs.copy_file(source, destination)
    }
    fn copy_dir_recursive(&self, source: &Path, destination: &Path) -> Result<(), String> {
        self.fs.copy_dir_recursive(source, destination)
    }
    fn symlink(&self, source: &Path, destination: &Path) -> Result<(), String> {
        self.fs.symlink(source, destination)
    }
}

pub use cairn_worktree::PopulateResult;

pub fn populate_worktree(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    repo_path: &Path,
    worktree_path: &Path,
    config: &PopulateConfig,
) -> Result<PopulateResult, String> {
    cairn_worktree::populate(
        &CorePopulateBackend { git, fs },
        repo_path,
        worktree_path,
        config,
    )
}
