//! Shared test helpers for archival tests: building disposable git repositories
//! in temporary directories. Only compiled under `cfg(test)`.

use std::path::Path;
use std::process::Command;

/// Run a git command in `dir`, isolated from the developer's global/system
/// config and with a fixed identity so commits succeed anywhere.
pub fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(dir)
        .env("GIT_AUTHOR_NAME", "Archival Test")
        .env("GIT_AUTHOR_EMAIL", "archival@example.com")
        .env("GIT_COMMITTER_NAME", "Archival Test")
        .env("GIT_COMMITTER_EMAIL", "archival@example.com")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .args(args)
        .output()
        .expect("spawn git");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// Initialize a repository with `main` as the default branch.
pub fn init_repo(dir: &Path) {
    git(dir, &["init", "-q", "-b", "main"]);
}

/// Write a file (creating parent directories) relative to the repo root.
pub fn write_file(dir: &Path, rel: &str, content: &[u8]) {
    let path = dir.join(rel);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, content).unwrap();
}

/// Stage all changes, commit, and return the new commit's 40-hex sha.
pub fn commit_all(dir: &Path, message: &str) -> String {
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", message]);
    git(dir, &["rev-parse", "HEAD"]).trim().to_string()
}
