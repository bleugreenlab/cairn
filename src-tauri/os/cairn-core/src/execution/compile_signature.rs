//! Reading a cargo build failure out of a check's combined output.
//!
//! A check's stored excerpt is a bounded TAIL, and cargo prints its errors
//! first and its `could not compile` summaries last, so the tail of a failure
//! with more than a handful of errors keeps the summaries and loses the errors
//! that caused them. This parse runs over the WHOLE output while it is still in
//! hand, and keeps every error rustc reported with the location it named.
//!
//! Two consumers read the result. The landing compile gate renders every error
//! in its refusal, and the compile fact (`compile_facts`) stores the ordered
//! `(code, path, line)` signature that attribution matches on.
//!
//! The parse is of cargo's human format, which is what a check command prints
//! when it is not asked for another. A failure is recognised only by cargo's
//! own `error: could not compile` summary, so an `error` line in a test's
//! captured stdout never makes a test failure read as a build failure.

use serde::{Deserialize, Serialize};

/// One `error` diagnostic rustc emitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RustcError {
    /// The error code, such as `E0425`. Absent for an uncoded error (a parse
    /// error, a macro's `compile_error!`).
    pub code: Option<String>,
    pub message: String,
    /// The primary span's path, exactly as cargo printed it: relative to the
    /// workspace root the command ran in.
    pub path: Option<String>,
    pub line: Option<u32>,
    pub column: Option<u32>,
}

/// One `error: could not compile `<crate>` (<target>)` summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FailedTarget {
    #[serde(rename = "crate")]
    pub krate: String,
    /// The target kind cargo named in parentheses (`lib`, `lib test`,
    /// `test "main"`, `bin "cairn"`), or empty when it named none.
    pub target: String,
}

/// A cargo build failure: every rustc error in output order, and every crate
/// target cargo reported it could not compile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompileFailure {
    pub errors: Vec<RustcError>,
    pub failed: Vec<FailedTarget>,
}

/// How many errors a rendering lists before it counts the rest. A merge that
/// breaks a widely used signature can produce hundreds of copies of one error;
/// the refusal needs every distinct site an agent must visit, not a wall.
const RENDERED_ERRORS: usize = 200;

impl CompileFailure {
    /// The first crate target cargo could not compile, the one a reader starts
    /// from. Cargo reports a dependency's failure before its dependents'.
    pub fn first_failed(&self) -> Option<&FailedTarget> {
        self.failed.first()
    }

    /// The signature attribution matches on: each error's code and path, in
    /// order, without line numbers, so a branch whose unrelated edit moved the
    /// site by a line still matches a base's recorded break (cairn/5236).
    pub fn attribution_key(&self) -> Vec<(Option<&str>, Option<&str>)> {
        self.errors
            .iter()
            .map(|error| (error.code.as_deref(), error.path.as_deref()))
            .collect()
    }

    /// One line per error, `path:line:column: error[CODE]: message`, then the
    /// crates that did not compile.
    pub fn render(&self) -> String {
        let mut lines: Vec<String> = self
            .errors
            .iter()
            .take(RENDERED_ERRORS)
            .map(|error| format!("- {}", render_error(error)))
            .collect();
        if self.errors.len() > RENDERED_ERRORS {
            lines.push(format!(
                "- … and {} more errors",
                self.errors.len() - RENDERED_ERRORS
            ));
        }
        if !self.failed.is_empty() {
            lines.push(format!(
                "did not compile: {}",
                self.failed
                    .iter()
                    .map(|failed| if failed.target.is_empty() {
                        format!("`{}`", failed.krate)
                    } else {
                        format!("`{}` ({})", failed.krate, failed.target)
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        lines.join("\n")
    }
}

fn render_error(error: &RustcError) -> String {
    let head = match &error.code {
        Some(code) => format!("error[{code}]: {}", error.message),
        None => format!("error: {}", error.message),
    };
    match (&error.path, error.line, error.column) {
        (Some(path), Some(line), Some(column)) => format!("{path}:{line}:{column}: {head}"),
        (Some(path), Some(line), None) => format!("{path}:{line}: {head}"),
        (Some(path), None, _) => format!("{path}: {head}"),
        (None, ..) => head,
    }
}

/// The build failure in a stored output TAIL, which is the trailing slice of
/// a longer output cut at a character count. Its first line may begin
/// mid-line, so it is never read as a diagnostic: a fragment that happens to
/// start with `error` would otherwise be a site rustc never reported.
pub fn parse_compile_failure_from_tail(tail: &str) -> Option<CompileFailure> {
    let rest = tail.split_once('\n').map_or("", |(_, rest)| rest);
    parse_compile_failure(rest)
}

/// The build failure in `output`, or `None` when cargo did not report one.
pub fn parse_compile_failure(output: &str) -> Option<CompileFailure> {
    let lines: Vec<String> = output.lines().map(strip_ansi).collect();
    let mut errors: Vec<RustcError> = Vec::new();
    let mut failed: Vec<FailedTarget> = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if let Some(target) = failed_target(line) {
            if !failed.contains(&target) {
                failed.push(target);
            }
            continue;
        }
        let Some((code, message)) = error_header(line) else {
            continue;
        };
        let location = lines.get(index + 1).and_then(|next| primary_location(next));
        let (path, line_number, column) = match location {
            Some((path, line, column)) => (Some(path), line, column),
            None => (None, None, None),
        };
        let error = RustcError {
            code,
            message,
            path,
            line: line_number,
            column,
        };
        // Cargo prints an error once per target that hits it (the lib and its
        // test harness compile the same source), so an identical diagnostic is
        // one site, not two.
        if !errors.contains(&error) {
            errors.push(error);
        }
    }
    if failed.is_empty() {
        return None;
    }
    Some(CompileFailure { errors, failed })
}

/// `error[E0425]: message` or `error: message`, excluding cargo's own summary
/// and progress lines, which describe the failure rather than cause it.
fn error_header(line: &str) -> Option<(Option<String>, String)> {
    let rest = line.strip_prefix("error")?;
    let (code, message) = if let Some(coded) = rest.strip_prefix('[') {
        let (code, after) = coded.split_once("]: ")?;
        if code.is_empty() || !code.chars().all(|c| c.is_ascii_alphanumeric()) {
            return None;
        }
        (Some(code.to_string()), after)
    } else {
        (None, rest.strip_prefix(": ")?)
    };
    let message = message.trim();
    if code.is_none() && is_cargo_summary(message) {
        return None;
    }
    Some((code, message.to_string()))
}

fn is_cargo_summary(message: &str) -> bool {
    message.starts_with("could not compile")
        || message.starts_with("aborting due to")
        || message.starts_with("build failed")
        // A wrapper reporting that the cargo (or bun) process it ran failed:
        // `command `cargo test ...` exited with code 101`,
        // `script "test:rust:full" exited with code 101`.
        || ((message.starts_with("command `") || message.starts_with("script \""))
            && message.contains("exited with code"))
}

/// `   --> path:line:column`, the primary span rustc prints under a header.
fn primary_location(line: &str) -> Option<(String, Option<u32>, Option<u32>)> {
    let location = line.trim_start().strip_prefix("--> ")?.trim();
    let mut parts = location.rsplitn(3, ':');
    let last = parts.next()?;
    let middle = parts.next();
    let first = parts.next();
    match (first, middle, last.parse::<u32>().ok()) {
        (Some(path), Some(line), Some(column)) => match line.parse::<u32>() {
            Ok(line) => Some((path.to_string(), Some(line), Some(column))),
            Err(_) => Some((location.to_string(), None, None)),
        },
        _ => Some((location.to_string(), None, None)),
    }
}

/// ``error: could not compile `crate` (target) due to N previous errors``.
fn failed_target(line: &str) -> Option<FailedTarget> {
    let rest = line.strip_prefix("error: could not compile `")?;
    let (krate, after) = rest.split_once('`')?;
    let after = after.trim_start();
    let target = after
        .strip_prefix('(')
        .and_then(|inner| inner.split_once(')'))
        .map(|(target, _)| target.to_string())
        .unwrap_or_default();
    Some(FailedTarget {
        krate: krate.to_string(),
        target,
    })
}

/// Drop terminal colour escapes, which a command forced to colour leaves in.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for next in chars.by_ref() {
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cairn/5290 stitch's shape: casualties in two crates neither PR
    /// touched, more errors than a tail keeps, and the lib and lib-test targets
    /// both reporting the same site.
    const STITCH: &str = "    Checking cairn-exec v0.12.34 (/w/src-tauri/cairn-exec)
warning: unused import: `Foo`
 --> cairn-exec/src/lib.rs:3:5
  |
error[E0063]: missing field `decision` in initializer of `Verdict`
   --> cairn-exec/src/placement.rs:412:9
    |
412 |         Verdict {
    |         ^^^^^^^ missing `decision`

error[E0063]: missing field `decision` in initializer of `Verdict`
   --> cairn-exec/src/placement.rs:518:13

error: could not compile `cairn-exec` (lib) due to 2 previous errors
warning: build failed, waiting for other jobs to finish...
error[E0063]: missing field `decision` in initializer of `Verdict`
   --> cairn-exec/src/placement.rs:412:9
error[E0308]: mismatched types
   --> os/cairn-core/src/execution/impact_slice.rs:437:21
error: could not compile `cairn-exec` (lib test) due to 1 previous error
error: could not compile `cairn-core` (lib) due to 1 previous error
";

    #[test]
    fn every_error_is_kept_in_order_with_its_location() {
        let failure = parse_compile_failure(STITCH).expect("a build failure");
        let sites: Vec<_> = failure
            .errors
            .iter()
            .map(|e| (e.code.as_deref(), e.path.as_deref(), e.line))
            .collect();
        assert_eq!(
            sites,
            vec![
                (
                    Some("E0063"),
                    Some("cairn-exec/src/placement.rs"),
                    Some(412)
                ),
                (
                    Some("E0063"),
                    Some("cairn-exec/src/placement.rs"),
                    Some(518)
                ),
                (
                    Some("E0308"),
                    Some("os/cairn-core/src/execution/impact_slice.rs"),
                    Some(437)
                ),
            ],
            "the lib-test repeat of 412 is the same site, and warnings are not errors"
        );
        assert_eq!(
            failure.first_failed(),
            Some(&FailedTarget {
                krate: "cairn-exec".into(),
                target: "lib".into()
            })
        );
        assert_eq!(failure.failed.len(), 3);
    }

    #[test]
    fn the_rendering_names_every_site_and_every_crate() {
        let rendered = parse_compile_failure(STITCH).unwrap().render();
        assert!(rendered.contains(
            "- cairn-exec/src/placement.rs:518:13: error[E0063]: missing field `decision` in initializer of `Verdict`"
        ));
        assert!(rendered.contains(
            "- os/cairn-core/src/execution/impact_slice.rs:437:21: error[E0308]: mismatched types"
        ));
        assert!(rendered.contains(
            "did not compile: `cairn-exec` (lib), `cairn-exec` (lib test), `cairn-core` (lib)"
        ));
        assert!(!rendered.contains("warning"));
    }

    /// Captured verbatim from the check ledger (rust-tests on fedora, 10-20
    /// 12:19 PDT, observation 7f799e2618ee4dd6b9ee9c01): a 4k tail with
    /// interleaved `Compiling` lines, cut mid-line at its start, closed by the
    /// test runner's own `error: command ...` and `error: script ...` lines.
    const LEDGER_E0425: &str =
        include_str!("../../tests/fixtures/rustc/e0425_dependency_catalog.txt");
    /// Captured verbatim (rust-tests main cadence on fedora, 10-20 13:23 PDT,
    /// observation 6637f7745091424caf7ff106): one E0560 between an
    /// unused-import warning and a dead-code warning, in a lib-test target.
    const LEDGER_E0560: &str =
        include_str!("../../tests/fixtures/rustc/e0560_runtime_tests.txt");

    fn sites(failure: &CompileFailure) -> Vec<(Option<&str>, Option<&str>, Option<u32>)> {
        failure
            .errors
            .iter()
            .map(|e| (e.code.as_deref(), e.path.as_deref(), e.line))
            .collect()
    }

    #[test]
    fn a_ledger_tail_with_a_runner_wrapper_yields_only_rustcs_sites() {
        let failure = parse_compile_failure_from_tail(LEDGER_E0425).expect("a build failure");
        assert_eq!(
            sites(&failure),
            vec![
                (Some("E0425"), Some("cairn-exec/src/dependency_catalog.rs"), Some(236)),
                (Some("E0425"), Some("cairn-exec/src/dependency_catalog.rs"), Some(256)),
            ],
            "the runner's `error: command` / `error: script` lines are not sites"
        );
        assert_eq!(
            failure.failed,
            vec![FailedTarget {
                krate: "cairn-exec".into(),
                target: "lib".into()
            }]
        );
        assert_eq!(failure.errors[0].message, "cannot find value `cairn_home` in this scope");
    }

    #[test]
    fn a_ledger_tail_with_interleaved_warnings_yields_the_one_error() {
        let failure = parse_compile_failure_from_tail(LEDGER_E0560).expect("a build failure");
        assert_eq!(
            sites(&failure),
            vec![(Some("E0560"), Some("os/cairn-workflow/src/runtime_tests.rs"), Some(312))]
        );
        assert_eq!(
            failure.first_failed(),
            Some(&FailedTarget {
                krate: "cairn-workflow".into(),
                target: "lib test".into()
            })
        );
    }

    /// Attribution matches on (code, path): a one-line edit above the site
    /// moves the line and must still match the base's recorded break.
    #[test]
    fn the_attribution_key_ignores_line_numbers() {
        let base = parse_compile_failure_from_tail(LEDGER_E0425).unwrap();
        let moved = parse_compile_failure_from_tail(
            &LEDGER_E0425
                .replace("dependency_catalog.rs:236:24", "dependency_catalog.rs:237:24")
                .replace("dependency_catalog.rs:256:16", "dependency_catalog.rs:257:16"),
        )
        .unwrap();
        assert_ne!(sites(&base), sites(&moved));
        assert_eq!(base.attribution_key(), moved.attribution_key());
        let other = parse_compile_failure_from_tail(LEDGER_E0560).unwrap();
        assert_ne!(base.attribution_key(), other.attribution_key());
    }

    /// A stored tail is the last 4k of a longer build. When the cut lands
    /// inside a line, the fragment it leaves is not a site, even when it reads
    /// like an error header.
    #[test]
    fn a_tail_cut_mid_line_does_not_invent_a_site() {
        let cut = format!(
            "error[E0599]: no method named `x` found (cut from a `= note:` line)\n{}",
            LEDGER_E0560
        );
        let failure = parse_compile_failure_from_tail(&cut).unwrap();
        assert_eq!(failure.errors.len(), 1, "{:?}", failure.errors);
        assert_eq!(failure.errors[0].code.as_deref(), Some("E0560"));
    }

    #[test]
    fn an_error_in_a_tests_output_is_not_a_build_failure() {
        let output = "test parses ... FAILED\nerror: expected value at line 1\ntest result: FAILED";
        assert_eq!(parse_compile_failure(output), None);
    }

    #[test]
    fn an_uncoded_error_and_a_cargo_refusal_are_kept() {
        let output = "\u{1b}[1m\u{1b}[31merror\u{1b}[0m: expected one of `,` or `}`, found `fn`\n  --> os/cairn-db/src/storage/migrations.rs:90:1\nerror: could not compile `cairn-db` (lib) due to 1 previous error\n";
        let failure = parse_compile_failure(output).unwrap();
        assert_eq!(failure.errors.len(), 1);
        assert_eq!(failure.errors[0].code, None);
        assert_eq!(
            failure.errors[0].path.as_deref(),
            Some("os/cairn-db/src/storage/migrations.rs")
        );
        assert_eq!(failure.errors[0].line, Some(90));
    }
}
