pub use cairn_packs::instructions::seed_default_workspace_instructions;
/// Best-effort read of the workspace-level instruction file (`~/.cairn/AGENTS.md`).
///
/// Returns `None` when the file is absent, empty, or unreadable — never errors.
pub(crate) fn read_workspace_instructions() -> Option<String> {
    let path = cairn_common::paths::cairn_home().join("AGENTS.md");
    let content = std::fs::read_to_string(&path).ok()?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Read instruction-file bytes for the Settings editor. Missing files and
/// whitespace-only files are represented by an empty document.
pub fn read_instructions_for_editor(root: &std::path::Path) -> Result<String, String> {
    let path = root.join("AGENTS.md");
    match std::fs::read_to_string(&path) {
        Ok(content) if content.trim().is_empty() => Ok(String::new()),
        Ok(content) => Ok(content),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(format!("failed to read {}: {error}", path.display())),
    }
}

/// Persist an instruction document and commit only `AGENTS.md` in its owning
/// repository. Exact-content saves are semantic no-ops.
pub fn save_instructions(
    root: &std::path::Path,
    content: &str,
    push_workspace: bool,
) -> Result<bool, String> {
    let path = root.join("AGENTS.md");
    match std::fs::read_to_string(&path) {
        Ok(existing) if existing == content => return Ok(false),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && content.is_empty() => {
            return Ok(false);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("failed to read {}: {error}", path.display())),
    }

    std::fs::create_dir_all(root)
        .map_err(|error| format!("failed to create {}: {error}", root.display()))?;
    std::fs::write(&path, content)
        .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    crate::config::commit_and_maybe_push(
        std::slice::from_ref(&path),
        "cairn: update instructions",
        push_workspace.then_some(root),
    );
    Ok(true)
}

/// Best-effort read of a project-level instruction file from an explicitly
/// materialized repository projection. Agent session assembly uses the store-
/// native logical view instead of process cwd. Returns `None` when the file is
/// absent, empty, or unreadable.
pub(crate) fn read_project_instructions(repo_root: &std::path::Path) -> Option<String> {
    let content = std::fs::read_to_string(repo_root.join("AGENTS.md")).ok()?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn clear_env() {
        std::env::remove_var("CAIRN_HOME");
    }

    fn git(repo: &std::path::Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .args(args)
            .current_dir(repo)
            .env("GIT_AUTHOR_NAME", "Test")
            .env("GIT_AUTHOR_EMAIL", "test@example.com")
            .env("GIT_COMMITTER_NAME", "Test")
            .env("GIT_COMMITTER_EMAIL", "test@example.com")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    fn init_repo(repo: &std::path::Path) {
        git(repo, &["init", "-b", "main"]);
        git(repo, &["commit", "--allow-empty", "-m", "initial"]);
    }

    #[test]
    fn reads_present_workspace_instructions_from_cairn_home() {
        let _guard = env_lock();
        clear_env();
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("CAIRN_HOME", dir.path());
        std::fs::write(dir.path().join("AGENTS.md"), "\n  workspace doctrine\n\n").unwrap();

        assert_eq!(
            read_workspace_instructions(),
            Some("workspace doctrine".to_string())
        );

        clear_env();
    }

    #[test]
    fn absent_workspace_instructions_are_noop() {
        let _guard = env_lock();
        clear_env();
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("CAIRN_HOME", dir.path());

        assert_eq!(read_workspace_instructions(), None);

        clear_env();
    }

    #[test]
    fn empty_workspace_instructions_are_noop() {
        let _guard = env_lock();
        clear_env();
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("CAIRN_HOME", dir.path());
        std::fs::write(dir.path().join("AGENTS.md"), "  \n\t\n").unwrap();

        assert_eq!(read_workspace_instructions(), None);

        clear_env();
    }

    #[test]
    fn reads_present_project_instructions_from_repo_root() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "\n  project doctrine\n\n").unwrap();

        assert_eq!(
            read_project_instructions(dir.path()),
            Some("project doctrine".to_string())
        );
    }

    #[test]
    fn absent_or_empty_project_instructions_are_noop() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_project_instructions(dir.path()), None);
        std::fs::write(dir.path().join("AGENTS.md"), "   \n\t\n").unwrap();
        assert_eq!(read_project_instructions(dir.path()), None);
    }

    #[test]
    fn editor_read_treats_missing_and_whitespace_only_as_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_instructions_for_editor(dir.path()).unwrap(), "");
        std::fs::write(dir.path().join("AGENTS.md"), " \n\t").unwrap();
        assert_eq!(read_instructions_for_editor(dir.path()).unwrap(), "");
    }

    #[test]
    fn project_save_commits_only_agents_file_once_and_unchanged_save_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        init_repo(dir.path());
        assert!(!save_instructions(dir.path(), "", false).unwrap());
        assert!(!dir.path().join("AGENTS.md").exists());
        assert_eq!(git(dir.path(), &["rev-list", "--count", "HEAD"]), "1");

        std::fs::write(dir.path().join("unrelated.txt"), "staged").unwrap();
        git(dir.path(), &["add", "unrelated.txt"]);

        assert!(save_instructions(dir.path(), "# Project\n", false).unwrap());
        assert_eq!(git(dir.path(), &["rev-list", "--count", "HEAD"]), "2");
        assert_eq!(
            git(
                dir.path(),
                &["diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD"]
            ),
            "AGENTS.md"
        );
        assert!(git(dir.path(), &["diff", "--cached", "--name-only"]).contains("unrelated.txt"));

        assert!(!save_instructions(dir.path(), "# Project\n", false).unwrap());
        assert_eq!(git(dir.path(), &["rev-list", "--count", "HEAD"]), "2");
    }

    #[test]
    fn workspace_save_commits_only_agents_file() {
        let dir = tempfile::tempdir().unwrap();
        init_repo(dir.path());
        assert!(save_instructions(dir.path(), "# Workspace\n", true).unwrap());
        assert_eq!(git(dir.path(), &["rev-list", "--count", "HEAD"]), "2");
        assert_eq!(
            git(
                dir.path(),
                &["diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD"]
            ),
            "AGENTS.md"
        );
    }
}
