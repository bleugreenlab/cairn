/// Best-effort read of the workspace-level instruction file (`~/.cairn/AGENTS.md`).
///
/// Returns `None` when the file is absent, empty, or unreadable — never errors.
pub fn read_workspace_instructions() -> Option<String> {
    let path = cairn_common::paths::cairn_home().join("AGENTS.md");
    let content = std::fs::read_to_string(&path).ok()?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Best-effort read of a run's project-level instruction file
/// (`<repo_root>/AGENTS.md`). `repo_root` is the agent's worktree cwd — the
/// exact checkout the agent operates in — so this reflects branch-specific
/// `AGENTS.md` edits. Returns `None` when the file is absent, empty, or
/// unreadable — never errors.
pub fn read_project_instructions(repo_root: &std::path::Path) -> Option<String> {
    let content = std::fs::read_to_string(repo_root.join("AGENTS.md")).ok()?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Seed the default workspace character prompt to `<config_dir>/AGENTS.md`, but
/// only when that file does not already exist. An existing workspace file is the
/// user's and is never clobbered. Returns whether the seed was written.
pub fn seed_default_workspace_instructions(config_dir: &std::path::Path) -> std::io::Result<bool> {
    let path = config_dir.join("AGENTS.md");
    if path.exists() {
        return Ok(false);
    }
    std::fs::create_dir_all(config_dir)?;
    std::fs::write(&path, crate::system_prompt::DEFAULT_WORKSPACE_PROMPT)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn clear_env() {
        std::env::remove_var("CAIRN_HOME");
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
    fn seed_writes_default_when_absent_and_never_clobbers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("AGENTS.md");

        // Absent: seed writes the compiled-in default and reports it wrote.
        assert!(seed_default_workspace_instructions(dir.path()).unwrap());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            crate::system_prompt::DEFAULT_WORKSPACE_PROMPT
        );

        // Present: seed leaves the user's file untouched and reports no write.
        std::fs::write(&path, "user doctrine\n").unwrap();
        assert!(!seed_default_workspace_instructions(dir.path()).unwrap());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "user doctrine\n");
    }
}
