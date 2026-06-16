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
}
