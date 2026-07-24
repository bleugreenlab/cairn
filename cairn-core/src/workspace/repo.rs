use std::path::Path;

use crate::services::{FileSystem, GitClient};

pub(crate) const WORKSPACE_GITIGNORE: &str = "# Ignore everything by default; only the curated config below is tracked.\n/*\n!/agents/\n!/skills/\n!/recipes/\n!/workflows/\n!/AGENTS.md\n!/settings.yaml\n!/.gitignore\n.DS_Store\n";

pub(crate) fn ensure_workspace_repo(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    config_dir: &Path,
    default_branch: &str,
) -> Result<(), String> {
    fs.create_dir_all(config_dir)?;

    let initialized = if git.is_repo(config_dir)? {
        false
    } else {
        git.init_repo(config_dir, default_branch)?;
        true
    };

    ensure_gitignore(fs, config_dir)?;

    if initialized {
        git.add_all(config_dir)?;
        git.commit(config_dir, "Initialize Cairn workspace config")?;
    }

    Ok(())
}

fn ensure_gitignore(fs: &dyn FileSystem, config_dir: &Path) -> Result<(), String> {
    let gitignore_path = config_dir.join(".gitignore");
    let should_write = if fs.exists(&gitignore_path) {
        fs.read_to_string(&gitignore_path)? != WORKSPACE_GITIGNORE
    } else {
        true
    };

    if should_write {
        fs.write_str(&gitignore_path, WORKSPACE_GITIGNORE)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::{MockFileSystem, MockGitClient};
    use mockall::predicate::eq;
    use std::path::Path;

    #[test]
    fn not_a_repo_initializes_gitignore_and_commits_once() {
        let repo = Path::new("/home/user/.cairn");
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all()
            .with(eq(repo))
            .returning(|_| Ok(()));
        git.expect_is_repo().with(eq(repo)).returning(|_| Ok(false));
        git.expect_init_repo()
            .withf(|path, branch| path == Path::new("/home/user/.cairn") && branch == "main")
            .times(1)
            .returning(|_, _| Ok(()));
        fs.expect_exists()
            .with(eq(repo.join(".gitignore")))
            .returning(|_| false);
        fs.expect_write_str()
            .withf(|path, contents| {
                path == Path::new("/home/user/.cairn/.gitignore") && contents == WORKSPACE_GITIGNORE
            })
            .times(1)
            .returning(|_, _| Ok(()));
        git.expect_add_all()
            .with(eq(repo))
            .times(1)
            .returning(|_| Ok(()));
        git.expect_commit()
            .withf(|path, msg| {
                path == Path::new("/home/user/.cairn") && msg == "Initialize Cairn workspace config"
            })
            .times(1)
            .returning(|_, _| Ok(()));

        ensure_workspace_repo(&git, &fs, repo, "main").unwrap();
    }

    #[test]
    fn existing_repo_ensures_gitignore_without_commit() {
        let repo = Path::new("/home/user/.cairn");
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        git.expect_is_repo().returning(|_| Ok(true));
        git.expect_init_repo().times(0);
        fs.expect_exists().returning(|_| true);
        fs.expect_read_to_string()
            .returning(|_| Ok("old\n".to_string()));
        fs.expect_write_str()
            .withf(|path, contents| path.ends_with(".gitignore") && contents == WORKSPACE_GITIGNORE)
            .times(1)
            .returning(|_, _| Ok(()));
        git.expect_add_all().times(0);
        git.expect_commit().times(0);

        ensure_workspace_repo(&git, &fs, repo, "main").unwrap();
    }

    #[test]
    fn gitignore_is_allowlist_for_config_only() {
        assert!(WORKSPACE_GITIGNORE.contains("/*"));
        assert!(WORKSPACE_GITIGNORE.contains("!/agents/"));
        assert!(WORKSPACE_GITIGNORE.contains("!/skills/"));
        assert!(WORKSPACE_GITIGNORE.contains("!/recipes/"));
        assert!(!WORKSPACE_GITIGNORE.contains("!worktrees"));
        assert!(!WORKSPACE_GITIGNORE.contains("!logs"));
        assert!(!WORKSPACE_GITIGNORE.contains("!mcp_auth_secret"));
    }
}
