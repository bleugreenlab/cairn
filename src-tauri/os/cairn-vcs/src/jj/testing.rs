//! Real repository fixtures shared with application integration tests.
use super::*;
use std::path::Path;

/// Hold the real maintenance exclusion while an application fixture tries to
/// read store metadata. Dropping this guard releases maintenance admission.
pub struct StoreReclamationGuard {
    _guard: super::object_lease::Reclamation,
}

pub fn hold_store_reclamation(store: &Path) -> Result<StoreReclamationGuard, String> {
    Ok(StoreReclamationGuard {
        _guard: super::object_lease::Reclamation::for_store(store)?,
    })
}

/// Hold every publication to `store` after its store transaction lands and
/// before its export leg runs, until [`PublicationPause::release`] or drop.
///
/// The publication blocks the thread it runs on, inside its caller's poll.
/// That is how a test cancels a caller in the middle of a real publication and
/// observes what its store guard does meanwhile.
pub struct PublicationPause {
    gate: std::sync::Arc<PauseGate>,
}

#[derive(Default)]
struct PauseGate {
    state: std::sync::Mutex<PauseState>,
    changed: std::sync::Condvar,
}

#[derive(Default)]
struct PauseState {
    reached: bool,
    released: bool,
}

type PauseRegistry = std::sync::Mutex<Vec<(std::path::PathBuf, std::sync::Arc<PauseGate>)>>;

/// Process-wide because the paused publication runs on whichever runtime
/// thread polled its caller, which a thread-local could not follow.
static PUBLICATION_PAUSES: PauseRegistry = std::sync::Mutex::new(Vec::new());

pub fn pause_publication_after_landing(store: &Path) -> PublicationPause {
    let gate = std::sync::Arc::new(PauseGate::default());
    PUBLICATION_PAUSES
        .lock()
        .unwrap()
        .push((store.to_path_buf(), gate.clone()));
    PublicationPause { gate }
}

impl PublicationPause {
    /// Wait until a publication is held at the pause. `false` on timeout.
    pub fn wait_until_paused(&self, timeout: std::time::Duration) -> bool {
        let state = self.gate.state.lock().unwrap();
        let (state, _) = self
            .gate
            .changed
            .wait_timeout_while(state, timeout, |state| !state.reached)
            .unwrap();
        state.reached
    }

    /// Let the held publication run its export leg.
    pub fn release(&self) {
        self.gate.state.lock().unwrap().released = true;
        self.gate.changed.notify_all();
    }
}

impl Drop for PublicationPause {
    fn drop(&mut self) {
        self.release();
        PUBLICATION_PAUSES
            .lock()
            .unwrap()
            .retain(|(_, gate)| !std::sync::Arc::ptr_eq(gate, &self.gate));
    }
}

/// The publication's landed-but-not-exported point. Holds there while a
/// [`PublicationPause`] for `store` is armed and unreleased.
pub(crate) fn publication_landed(store: &Path) {
    let gate = PUBLICATION_PAUSES
        .lock()
        .unwrap()
        .iter()
        .find(|(paused, _)| paused == store)
        .map(|(_, gate)| gate.clone());
    let Some(gate) = gate else { return };
    let mut state = gate.state.lock().unwrap();
    state.reached = true;
    gate.changed.notify_all();
    let _released = gate
        .changed
        .wait_while(state, |state| !state.released)
        .unwrap();
}

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
    // Found at run time, not baked in with `env!`: integration tests in other
    // crates link this helper, and a baked path would follow the artifact to
    // whichever build cell reuses it.
    let bundled = |src_tauri: &Path| {
        src_tauri
            .join("binaries")
            .join(format!("jj-{triple}{suffix}"))
    };
    cairn_common::paths::development_checkout_ancestor(|directory| bundled(directory).is_file())
        .map(|src_tauri| bundled(&src_tauri))
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
///
/// Each call commits at a time no earlier call in this process used. A fixture
/// that rolls the store back and repeats the same rebase would otherwise, within
/// one second, mint the commit id of the rebase it undid (git commit times have
/// one-second resolution), and jj refuses a newly created commit it already
/// indexed.
pub fn rebase_recording_conflict(jj: &JjEnv, store: &Path, branch: &str, dest: &str) {
    let timestamp = format!("debug.commit-timestamp={}", unique_commit_timestamp());
    jj.run(
        store,
        &[
            "rebase",
            "-b",
            branch,
            "-o",
            dest,
            "--ignore-working-copy",
            "--config",
            &timestamp,
        ],
        "test fixture: unguarded rebase",
    )
    .unwrap();
}

/// An RFC 3339 UTC time, at or after now, strictly later than any earlier
/// call in this process returned.
fn unique_commit_timestamp() -> String {
    use std::sync::atomic::{AtomicI64, Ordering};
    static LAST: AtomicI64 = AtomicI64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let next = |last: i64| last.max(now - 1) + 1;
    let secs = next(
        LAST.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |last| Some(next(last)))
            .unwrap(),
    );
    // Civil date from days since the epoch (Howard Hinnant's algorithm).
    let (days, of_day) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        of_day / 3_600,
        of_day % 3_600 / 60,
        of_day % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_commit_timestamps_are_rfc3339_and_strictly_increasing() {
        let first = unique_commit_timestamp();
        let second = unique_commit_timestamp();
        assert!(second > first, "{first} then {second}");
        let bytes = first.as_bytes();
        assert_eq!(bytes.len(), 20, "{first}");
        assert_eq!(
            (bytes[4], bytes[7], bytes[10], bytes[19]),
            (b'-', b'-', b'T', b'Z'),
            "{first}"
        );
    }

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
