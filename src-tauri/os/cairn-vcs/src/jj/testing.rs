//! Real repository fixtures shared with application integration tests.
use super::*;
use std::path::Path;

/// The jj artifact `scripts/ensure-sidecar.ts` provisions for this host under
/// `src-tauri/binaries`, when it is present.
///
/// This is the pinned binary the app itself ships, and the one resolution that
/// does not depend on what the test process inherits: lanes run under a
/// hermetic environment whose PATH may carry no `jj` at all, while an agent
/// shell's PATH leads with Cairn's own shim.
pub fn provisioned_jj() -> Option<std::path::PathBuf> {
    let triple = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => "aarch64-apple-darwin",
        ("macos", "x86_64") => "x86_64-apple-darwin",
        ("linux", "aarch64") => "aarch64-unknown-linux-gnu",
        ("linux", "x86_64") => "x86_64-unknown-linux-gnu",
        ("windows", "aarch64") => "aarch64-pc-windows-msvc",
        ("windows", "x86_64") => "x86_64-pc-windows-msvc",
        _ => return None,
    };
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    let bundled = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../binaries")
        .join(format!("jj-{triple}{suffix}"));
    bundled.is_file().then_some(bundled)
}

/// The skip reason a jj-gated test records when jj cannot be resolved.
///
/// Deliberately never a `src-tauri/skip-manifest.toml` entry: jj is something a
/// lane is expected to have (`bun run test:rust` provisions it), so a missing jj
/// is an undeclared skip that fails the run. An operator on a machine that
/// genuinely cannot get jj declares it for ONE run with
/// [`JJ_TESTS_OPTIONAL_ENV`], which the runner honours and the lane capability
/// test respects.
pub const JJ_UNAVAILABLE_SKIP_REASON: &str = "jj-unavailable";

/// Set (to anything) to declare, for this run only, that jj-gated tests may skip.
pub const JJ_TESTS_OPTIONAL_ENV: &str = "CAIRN_JJ_TESTS_OPTIONAL";

/// The jj binary a fixture test may drive, or `None` once the calling test has
/// been recorded as a [`JJ_UNAVAILABLE_SKIP_REASON`] skip.
///
/// This is the guard behind every `let Some(bin) = jj_bin() else { return }`.
/// The early return is a pass to libtest, so the record is what keeps the skip
/// from being counted as coverage. A caller that is not a skip guard, and would
/// otherwise misattribute a skip to whatever test reached it first, uses
/// [`resolve_jj`] instead.
pub fn jj_bin() -> Option<String> {
    skip_unless_jj(resolve_jj(), cairn_common::skip_ledger::record_skip)
}

/// Pass `resolved` through, first recording the calling test as a
/// [`JJ_UNAVAILABLE_SKIP_REASON`] skip via `record` when it is `None`. Shared by
/// both `jj_bin` guards (this one and the cairn-core integration harness's), so
/// the jj skip has one shape whichever resolution order found nothing.
pub fn skip_unless_jj<T>(resolved: Option<T>, record: impl FnOnce(&str, &str)) -> Option<T> {
    if resolved.is_none() {
        let test = cairn_common::skip_ledger::current_test_name();
        eprintln!("skipping {test}: jj not resolvable");
        record(&test, JJ_UNAVAILABLE_SKIP_REASON);
    }
    resolved
}

/// The jj binary on this machine, or `None` when it is not resolvable; no skip
/// is recorded. The single resolution for every suite that builds a real store:
/// the [`provisioned_jj`] artifact first, then `CAIRN_JJ_BIN`, then PATH.
///
/// A bare `jj` on PATH is very often Cairn's OWN shim: `<cairn_home>/bin` leads
/// PATH in every agent shell and, once installed, in the operator's. That shim
/// intercepts `jj workspace update-stale`, so a suite resolving it would drive
/// an unconditional no-op while believing it was driving jj — which is exactly
/// the confusion this harness must never reproduce, since an evening went to
/// diagnosing a stale store against that no-op. A resolved shim is unwrapped to
/// the binary it forwards to, read out of the `exec` line Cairn generated.
pub fn resolve_jj() -> Option<String> {
    let bin = provisioned_jj()
        .map(|path| path.to_string_lossy().into_owned())
        .or_else(|| {
            std::env::var("CAIRN_JJ_BIN")
                .ok()
                .filter(|s| !s.trim().is_empty())
        })
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
    // Any `git commit` may start a detached `git gc --auto` that repacks the
    // store behind a test's back, deleting packs the test is about to inspect
    // and writing a cruft pack of its own.
    git(repo, &["config", "gc.auto", "0"]);
    git(repo, &["config", "maintenance.auto", "false"]);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unresolvable_jj_records_the_calling_test_as_a_jj_skip() {
        let mut recorded = Vec::new();
        let bin: Option<String> = skip_unless_jj(None, |test, reason| {
            recorded.push((test.to_string(), reason.to_string()))
        });
        assert_eq!(bin, None);
        assert_eq!(
            recorded,
            vec![(
                "an_unresolvable_jj_records_the_calling_test_as_a_jj_skip".to_string(),
                JJ_UNAVAILABLE_SKIP_REASON.to_string()
            )]
        );
    }

    #[test]
    fn a_resolved_jj_records_nothing() {
        let bin = skip_unless_jj(Some("/opt/jj".to_string()), |test, reason| {
            panic!("recorded {test} as skipped ({reason}) although jj resolved")
        });
        assert_eq!(bin.as_deref(), Some("/opt/jj"));
    }
}
