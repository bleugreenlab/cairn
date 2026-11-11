//! The Rust half of the self-skip ledger: how a test that returns early instead
//! of running says so.
//!
//! libtest counts an early return as a pass and swallows its message, so a
//! self-skip is otherwise indistinguishable from real coverage (#157,
//! CAIRN-3164). Every skip helper in the workspace therefore appends one
//! `name<TAB>reason` line to `$CAIRN_SKIP_LOG`, which the Rust test runner
//! (`scripts/test-rust-nextest.ts`) creates per run. The runner moves each logged
//! test out of `passed` and into `skipped`, and fails the run on any skip that
//! neither `src-tauri/skip-manifest.toml` nor a per-run environment declaration
//! permits (`scripts/lib/skip-ledger.ts`, the TypeScript half).
//!
//! `name` is the bare libtest test name with no crate or module prefix, which is
//! what manifest entries are keyed by. `reason` is the manifest's closed
//! vocabulary; it is what lets the runner tell a declared capability gap from an
//! inherited habit.
//!
//! Outside the runner (a bare `cargo test` or `cargo nextest run`) the variable is
//! unset and recording is a no-op: there is no ledger to report to, and the skip
//! note on stderr is all that survives.

use std::path::Path;

/// The per-run skip log the Rust test runner hands every test process.
pub const SKIP_LOG_ENV: &str = "CAIRN_SKIP_LOG";

/// The first field of a skip-log line that records an observation about the
/// run rather than a skip. The ledger reports these by kind and never counts
/// one as a skip.
pub const OBSERVATION_MARKER: &str = "#observation";

/// Record that `test` skipped for `reason`, best-effort, when a skip log exists.
pub fn record_skip(test: &str, reason: &str) {
    if let Some(log) = std::env::var_os(SKIP_LOG_ENV) {
        append_line(Path::new(&log), &format!("{test}\t{reason}\n"));
    }
}

/// Record an observation of kind `kind` beside the run's skips, best-effort,
/// when a skip log exists: something the run's owner should see in the skip
/// report, such as two measurements of one capability disagreeing.
pub fn record_observation(kind: &str, detail: &str) {
    if let Some(log) = std::env::var_os(SKIP_LOG_ENV) {
        append_line(Path::new(&log), &observation_line(kind, detail));
    }
}

fn observation_line(kind: &str, detail: &str) -> String {
    // One line, three fields: a tab or newline in the detail would split it.
    let detail = detail
        .chars()
        .map(|c| {
            if c == '\t' || c == '\n' || c == '\r' {
                ' '
            } else {
                c
            }
        })
        .collect::<String>();
    format!("{OBSERVATION_MARKER}\t{kind}\t{detail}\n")
}

fn append_line(log: &Path, line: &str) {
    use std::io::Write as _;
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
    {
        // One write_all (vs writeln!'s multiple syscalls) keeps the append atomic
        // when parallel test threads all skip at once.
        let _ = file.write_all(line.as_bytes());
    }
}

/// The bare name of the test running on this thread, for a skip helper that is
/// called from inside a test without being told which one.
///
/// libtest runs every test on its own thread named after the test's full path
/// (`module::tests::name`), under both `cargo test` and nextest, so the name is
/// recoverable without threading it through every call site. A helper called
/// from a thread the test spawned (a tokio worker, `spawn_blocking`) sees that
/// thread's name instead; it is still recorded, so the skip stays counted and
/// visibly unattributable rather than silently becoming a pass.
pub fn current_test_name() -> String {
    let thread = std::thread::current();
    match thread.name() {
        Some(name) => bare_test_name(name).to_string(),
        None => "unnamed-thread".to_string(),
    }
}

fn bare_test_name(path: &str) -> &str {
    path.rsplit("::").next().unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_test_helper_learns_which_test_called_it_from_the_libtest_thread() {
        assert_eq!(
            current_test_name(),
            "a_test_helper_learns_which_test_called_it_from_the_libtest_thread"
        );
    }

    #[test]
    fn a_bare_test_name_drops_the_module_path() {
        assert_eq!(
            bare_test_name("jj::tests::seal_round_trips"),
            "seal_round_trips"
        );
        assert_eq!(bare_test_name("top_level"), "top_level");
    }

    #[test]
    fn each_skip_appends_one_tab_separated_record() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("skips.log");
        append_line(&log, "first\tworktree-fence\n");
        append_line(&log, "second\tjj-unavailable\n");
        assert_eq!(
            std::fs::read_to_string(&log).unwrap(),
            "first\tworktree-fence\nsecond\tjj-unavailable\n"
        );
    }

    #[test]
    fn an_observation_is_one_marked_line_whatever_its_detail_holds() {
        assert_eq!(
            observation_line("nested-sandbox-cross-check-disagreed", "in-process\tAVAILABLE\nrecord UNAVAILABLE"),
            "#observation\tnested-sandbox-cross-check-disagreed\tin-process AVAILABLE record UNAVAILABLE\n"
        );
    }
}
