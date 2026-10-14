pub const DEFAULT_WORKSPACE_PROMPT: &str = include_str!("default_workspace_prompt.md");
/// Seed the default workspace character prompt to `<config_dir>/AGENTS.md`, but
/// only when that file does not already exist. An existing workspace file is the
/// user's and is never clobbered. Returns whether the seed was written.
pub fn seed_default_workspace_instructions(config_dir: &std::path::Path) -> std::io::Result<bool> {
    let path = config_dir.join("AGENTS.md");
    if path.exists() {
        return Ok(false);
    }
    std::fs::create_dir_all(config_dir)?;
    std::fs::write(&path, DEFAULT_WORKSPACE_PROMPT)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn seed_writes_default_when_absent_and_never_clobbers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("AGENTS.md");

        assert!(seed_default_workspace_instructions(dir.path()).unwrap());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            DEFAULT_WORKSPACE_PROMPT
        );

        std::fs::write(&path, "user doctrine\n").unwrap();
        assert!(!seed_default_workspace_instructions(dir.path()).unwrap());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "user doctrine\n");
    }
}
