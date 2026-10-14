//! Real repository fixtures shared with application integration tests.
use super::*;
use std::path::Path;

/// The jj binary a fixture test may drive, or `None` when jj is not resolvable
/// on this machine. The single definition for every crate-internal suite that
/// builds a real store.
///
/// A bare `jj` on PATH is very often Cairn's OWN shim: `<cairn_home>/bin` leads
/// PATH in every agent shell and, once installed, in the operator's. That shim
/// intercepts `jj workspace update-stale`, so a suite resolving it would drive
/// an unconditional no-op while believing it was driving jj — which is exactly
/// the confusion this harness must never reproduce, since an evening went to
/// diagnosing a stale store against that no-op. A resolved shim is unwrapped to
/// the binary it forwards to, read out of the `exec` line Cairn generated.
pub fn jj_bin() -> Option<String> {
    let bin = std::env::var("CAIRN_JJ_BIN")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "jj".to_string());
    let bin = cairn_platform::env::real_jj_behind_shim(&bin).unwrap_or(bin);
    cairn_platform::env::command(&bin)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
        .then_some(bin)
}

pub fn git(repo: &Path, args: &[&str]) {
    assert!(
        cairn_platform::env::git()
            .args(args)
            .current_dir(repo)
            .status()
            .unwrap()
            .success(),
        "git {args:?} failed"
    );
}

pub fn init_project(repo: &Path) {
    git(repo, &["init", "-q", "-b", "main"]);
    git(repo, &["config", "user.email", "p@e.com"]);
    git(repo, &["config", "user.name", "P"]);
    std::fs::write(repo.join("shared.rs"), "base\n").unwrap();
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-q", "-m", "base"]);
}

/// Capture trimmed stdout of a git command (test helper).
pub fn git_stdout(repo: &Path, args: &[&str]) -> String {
    let out = cairn_platform::env::git()
        .args(args)
        .current_dir(repo)
        .output()
        .unwrap();
    assert!(out.status.success(), "git {args:?} failed");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Rebase a branch WITHOUT the conflict guard, leaving the recorded conflict in
/// place.
///
/// [`rebase_branch_onto`] deliberately refuses to do this — it rolls a
/// conflicting rebase back so nothing conflict-flagged can reach git — so it can
/// no longer be used to BUILD a conflicted store. Fixtures that need that shape
/// construct it here: a branch carrying conflict-flagged commits still arrives
/// from stores that predate the guard and from `jj` run outside Cairn, and
/// flatten recovery exists precisely to clear it.
pub fn rebase_recording_conflict(jj: &JjEnv, store: &Path, branch: &str, dest: &str) {
    jj.run(
        store,
        &["rebase", "-b", branch, "-o", dest, "--ignore-working-copy"],
        "test fixture: unguarded rebase",
    )
    .unwrap();
}
