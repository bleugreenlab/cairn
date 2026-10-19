use super::*;

/// Resolves recorded (possibly short) commit ids to full commit ids in the
/// project repository, caching each answer: an execution reports the same
/// commit many times.
pub(super) struct CommitResolver<'a> {
    git_dir: &'a Path,
    full: HashMap<String, Option<String>>,
}

impl<'a> CommitResolver<'a> {
    pub(super) fn new(git_dir: &'a Path) -> Self {
        Self {
            git_dir,
            full: HashMap::new(),
        }
    }

    /// Expand a recorded sha to its full commit id, peeling tags. `None` when
    /// the repository does not know it.
    pub(super) fn resolve_full(&mut self, sha: &str) -> Option<String> {
        if let Some(full) = self.full.get(sha) {
            return full.clone();
        }
        let resolved = git_stdout(
            self.git_dir,
            &[
                "rev-parse",
                "--verify",
                "--quiet",
                &format!("{sha}^{{commit}}"),
            ],
        )
        .filter(|full| !full.is_empty());
        self.full.insert(sha.to_string(), resolved.clone());
        resolved
    }
}

fn git_stdout(git_dir: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .current_dir(git_dir)
        .args(args)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}
