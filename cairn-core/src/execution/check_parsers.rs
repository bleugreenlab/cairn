//! Structured per-test parsing of project-check output.
//!
//! A project check's verdict is its EXIT CODE — a check passes iff its command
//! exits `0`, and that decision is made by the runners in
//! [`crate::execution::checks`] / [`crate::execution::checks_turn_end`]. This
//! module is pure ENRICHMENT layered on top of that verdict: given a check's
//! command and its captured combined output, it extracts the failing test
//! identifiers, the pass/fail/skip counts, and (where the runner exposes one) a
//! short per-failure message. The agent-facing surfaces use that to say WHAT
//! failed instead of only `exit 101`, so the agent does not have to re-run the
//! whole suite to learn which tests broke.
//!
//! ## Fail-closed
//!
//! Parsing NEVER changes a verdict. The exit code stays the sole authority for
//! pass/fail; a parse only adds detail. A command with no recognized runner, a
//! missing reporter, or output a parser cannot make sense of yields `None` (or a
//! zero-failure result), and the surfaces degrade to the raw output tail —
//! today's behavior — rather than breaking or flipping a failing exit to a pass.
//!
//! ## Parser selection
//!
//! The runner family is detected from the COMMAND STRING, not a config field, so
//! the `checks` schema stays unextended and no new field can drift across its
//! four consumers (`config/project_settings.rs`, the TS `CheckCommand`, the
//! editor, and docs). The repo's Rust entry points (`test:rust`, `nextest`, and
//! `cargo test`) route to the rust parser, `vitest` to the vitest parser, and
//! `tsc` to the tsc parser.
//!
//! ## The persisted shape (`target_results_json`)
//!
//! [`ParsedCheckResult`] is what serializes into the `check_result_cache`
//! `target_results_json` column. It is deliberately the SUBSTRATE future
//! baseline/delta work consumes: `failures[].name` is a stable per-test
//! identifier that can be compared across two sealed trees to attribute a break
//! to a diff. The full shape is documented in `docs/checks.md`.

use std::sync::LazyLock;

use regex::Regex;

/// Chars of failure detail inlined per failing check into either surface (the
/// write-cadence inline summary and the turn-end `### Systematic checks`
/// section). One constant for both surfaces, sized at the turn-end section's
/// historical 1500-char cap.
const FAILURE_EXCERPT_CHARS: usize = 1_500;

/// Failing test names listed inline before collapsing the rest into `+N more`.
pub(crate) const MAX_FAILURE_NAMES: usize = 5;

/// Cap on a single per-failure message excerpt, keeping the stored JSON and the
/// rendered lines bounded regardless of a runner's stack-trace verbosity.
const MESSAGE_CHARS: usize = 240;

/// Structured result of parsing one check's output. Serializes (camelCase) into
/// the `check_result_cache.target_results_json` column. Enrichment only — the
/// authoritative pass/fail verdict is the exit code, stored separately.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParsedCheckResult {
    /// Which runner produced this: `"nextest"`, `"vitest"`, or `"tsc"`.
    pub(crate) parser: String,
    /// Passing test count. `0` for tsc, which has no test concept.
    pub(crate) passed: usize,
    /// Failing test / error count (the runner's own tally when available).
    pub(crate) failed: usize,
    /// Skipped / ignored / todo test count.
    pub(crate) skipped: usize,
    /// The failing tests, in report order. This is the substrate future
    /// baseline/delta work compares across trees, so `name` is a stable
    /// identifier.
    pub(crate) failures: Vec<CheckFailure>,
}

impl ParsedCheckResult {
    /// Whether this parse came from a TEST RUNNER (`nextest`/`vitest`), whose
    /// pass/fail counts denote real tests, rather than `tsc`, whose `failed` is a
    /// type-error tally with no "test count" meaning. Verdict surfaces gate the
    /// `N tests` / `no tests matched` rendering on this so a passing typecheck is
    /// never labelled with a bogus test count.
    pub(crate) fn is_test_runner(&self) -> bool {
        self.parser == "nextest" || self.parser == "vitest"
    }

    /// Tests the runner actually executed: passed + failed. Skipped/ignored tests
    /// did not run, so they are excluded from the "was anything validated?" tally
    /// that distinguishes a real green from a zero-selection green.
    pub(crate) fn tests_run(&self) -> usize {
        self.passed + self.failed
    }
}

/// One failing test / error site.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckFailure {
    /// Stable identifier for the failing test / error site: a nextest
    /// `<crate> <test::path>`, a vitest full test title, or a tsc
    /// `file(line,col)`.
    pub(crate) name: String,
    /// A short, single-line excerpt of the failure message when the runner
    /// exposes one cleanly (vitest assertion text, tsc error text). `None` for
    /// nextest, whose per-test panic text is not reliably attributable from its
    /// human output; callers fall back to the raw output tail there.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) message: Option<String>,
}

/// Runner family a check's output should be parsed as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParserKind {
    /// `cargo nextest` (primary) or `cargo test` (fallback) output.
    Rust,
    Vitest,
    Tsc,
}

/// Detect the runner family from the command string. Returns `None` for a
/// command with no recognized runner, so the check degrades to exit-code +
/// raw-tail behavior.
fn detect_parser(command: &str) -> Option<ParserKind> {
    if command.contains("nextest")
        || command.contains("cargo test")
        || command.contains("test:rust")
    {
        return Some(ParserKind::Rust);
    }
    if command.contains("vitest") {
        return Some(ParserKind::Vitest);
    }
    if command.contains("tsc") {
        return Some(ParserKind::Tsc);
    }
    None
}

/// Parse one check's combined output into a [`ParsedCheckResult`], selecting the
/// parser from `command`. Returns `None` when the command has no recognized
/// runner or the output could not be parsed at all — the caller then degrades to
/// the raw output tail. A recognized runner that simply found no failures still
/// returns `Some` (a zero-failure result), which is a valid "green at this tree"
/// record for future baseline work.
pub(crate) fn parse_check_output(command: &str, output: &str) -> Option<ParsedCheckResult> {
    let kind = detect_parser(command)?;
    let clean = strip_ansi(output);
    match kind {
        ParserKind::Rust => parse_rust(&clean),
        ParserKind::Vitest => parse_vitest(&clean),
        ParserKind::Tsc => parse_tsc(&clean),
    }
}

// ---------------------------------------------------------------------------
// tsc
// ---------------------------------------------------------------------------

static TSC_ERROR: LazyLock<Regex> = LazyLock::new(|| {
    // `path(line,col): error TS2322: message` — tsc's line-structured output,
    // stable across pretty/plain modes once ANSI is stripped (plain when piped,
    // which is how the check runner captures it).
    Regex::new(r"(?m)^\s*(\S.*?)\((\d+),(\d+)\): error (TS\d+): (.+)$").unwrap()
});

fn parse_tsc(clean: &str) -> Option<ParsedCheckResult> {
    let failures: Vec<CheckFailure> = TSC_ERROR
        .captures_iter(clean)
        .map(|cap| CheckFailure {
            name: format!("{}({},{})", &cap[1], &cap[2], &cap[3]),
            message: Some(cap_chars(
                &format!("{}: {}", &cap[4], cap[5].trim()),
                MESSAGE_CHARS,
            )),
        })
        .collect();
    // Always `Some`: an empty match set is a coherent zero-error verdict (a
    // passing typecheck), and a failing exit with no matched errors still
    // records structured emptiness so callers fall back to the raw tail.
    Some(ParsedCheckResult {
        parser: "tsc".to_string(),
        passed: 0,
        failed: failures.len(),
        skipped: 0,
        failures,
    })
}

// ---------------------------------------------------------------------------
// vitest (JSON reporter)
// ---------------------------------------------------------------------------

fn parse_vitest(clean: &str) -> Option<ParsedCheckResult> {
    // The JSON reporter emits one JSON object identified by its leading key. With
    // the dual `--reporter=default --reporter=json` the object trails the human
    // output, and combined capture may append stderr after it — so locate the
    // object by its key and parse the FIRST JSON value from there, letting serde
    // stop at the value's end and ignore any trailing text.
    let idx = clean.find("{\"numTotalTestSuites\"")?;
    let value: serde_json::Value = serde_json::Deserializer::from_str(&clean[idx..])
        .into_iter::<serde_json::Value>()
        .next()?
        .ok()?;

    let count = |key: &str| value.get(key).and_then(|v| v.as_u64()).unwrap_or(0);
    let passed = count("numPassedTests") as usize;
    let failed = count("numFailedTests") as usize;
    let skipped = (count("numPendingTests") + count("numTodoTests")) as usize;

    let mut failures = Vec::new();
    if let Some(files) = value.get("testResults").and_then(|v| v.as_array()) {
        for file in files {
            let Some(assertions) = file.get("assertionResults").and_then(|v| v.as_array()) else {
                continue;
            };
            for a in assertions {
                if a.get("status").and_then(|v| v.as_str()) != Some("failed") {
                    continue;
                }
                let name = a
                    .get("fullName")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .or_else(|| a.get("title").and_then(|v| v.as_str()))
                    .unwrap_or("<unknown test>")
                    .to_string();
                let message = a
                    .get("failureMessages")
                    .and_then(|v| v.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|m| m.as_str())
                    .map(|m| cap_chars(first_line(m), MESSAGE_CHARS));
                failures.push(CheckFailure { name, message });
            }
        }
    }

    Some(ParsedCheckResult {
        parser: "vitest".to_string(),
        passed,
        failed,
        skipped,
        failures,
    })
}

// ---------------------------------------------------------------------------
// rust: cargo-nextest (primary) with a cargo test fallback
// ---------------------------------------------------------------------------

fn parse_rust(clean: &str) -> Option<ParsedCheckResult> {
    // nextest is the configured runner; cargo test is the wrapper's degraded
    // fallback when nextest is not installed. Recognize whichever produced the
    // output so both machines get structured results.
    parse_nextest(clean).or_else(|| parse_cargo_test(clean))
}

static NEXTEST_FAIL: LazyLock<Regex> = LazyLock::new(|| {
    // `FAIL [   0.088s] (3/4) <crate> <test::path>` — the optional `(i/N)`
    // progress counter appears during the run. Signal/timeout statuses are
    // failures too; SLOW/PASS lines are deliberately excluded.
    Regex::new(
        r"(?m)^\s*(?:FAIL|TIMEOUT|ABORT|SIGSEGV)\s+\[[^\]]*\]\s+(?:\(\d+/\d+\)\s+)?(\S+)\s+(\S+)\s*$",
    )
    .unwrap()
});

static NEXTEST_SLOW: LazyLock<Regex> = LazyLock::new(|| {
    // `SLOW [>  60.000s] <crate> <test::path>` — nextest's marker for a test
    // still running past its slow-timeout threshold. When a suite is KILLED at
    // its budget these name the tests that were mid-flight at the kill.
    Regex::new(r"(?m)^\s*SLOW\s+\[[^\]]*\]\s+(?:\(\d+/\d+\)\s+)?(\S+)\s+(\S+)\s*$").unwrap()
});

/// Extract the nextest tests still running when a suite was killed — its
/// `SLOW […]` lines — from a check's captured output. Deduped, first-seen order.
/// Empty for any other runner or output with no SLOW lines. Enrichment only:
/// surfaced beside a `timed_out` verdict so the first agent question ("what was
/// it doing when it died?") is answerable without re-running the suite.
pub(crate) fn extract_running_tests(output: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for cap in NEXTEST_SLOW.captures_iter(output) {
        let name = format!("{} {}", &cap[1], &cap[2]);
        if seen.insert(name.clone()) {
            out.push(name);
        }
    }
    out
}

static COUNT_PASSED: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(\d+)\s+passed").unwrap());
static COUNT_FAILED: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(\d+)\s+failed").unwrap());
static COUNT_SKIPPED: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(\d+)\s+skipped").unwrap());

fn parse_nextest(clean: &str) -> Option<ParsedCheckResult> {
    // `Summary [   0.093s] 4 tests run: 2 passed, 2 failed, 1 skipped` is the
    // authoritative tally and the marker that this IS nextest output. No Summary
    // line ⇒ not nextest; let the cargo-test fallback try.
    let summary = clean.lines().find(|l| {
        let t = l.trim_start();
        t.starts_with("Summary [") && t.contains("tests run:")
    })?;
    let grab = |re: &Regex| -> usize {
        re.captures(summary)
            .and_then(|c| c.get(1))
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(0)
    };

    // FAIL lines repeat (once during the run, once in the final summary list), so
    // dedupe by identifier while preserving first-seen order.
    let mut failures = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for cap in NEXTEST_FAIL.captures_iter(clean) {
        let name = format!("{} {}", &cap[1], &cap[2]);
        if seen.insert(name.clone()) {
            failures.push(CheckFailure {
                name,
                message: None,
            });
        }
    }

    Some(ParsedCheckResult {
        parser: "nextest".to_string(),
        passed: grab(&COUNT_PASSED),
        failed: grab(&COUNT_FAILED),
        skipped: grab(&COUNT_SKIPPED),
        failures,
    })
}

static CARGO_RESULT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?m)^\s*test result:\s*\w+\.\s*(\d+) passed;\s*(\d+) failed;\s*(\d+) ignored")
        .unwrap()
});
static CARGO_FAILED_LINE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?m)^test (\S+) \.\.\. FAILED\s*$").unwrap());

fn parse_cargo_test(clean: &str) -> Option<ParsedCheckResult> {
    // Sum the per-binary `test result:` lines; their absence means this is not
    // libtest output at all, so there is nothing to enrich.
    let mut passed = 0;
    let mut failed = 0;
    let mut ignored = 0;
    let mut any = false;
    for cap in CARGO_RESULT.captures_iter(clean) {
        any = true;
        passed += cap[1].parse::<usize>().unwrap_or(0);
        failed += cap[2].parse::<usize>().unwrap_or(0);
        ignored += cap[3].parse::<usize>().unwrap_or(0);
    }
    if !any {
        return None;
    }

    let mut failures = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for cap in CARGO_FAILED_LINE.captures_iter(clean) {
        let name = cap[1].to_string();
        if seen.insert(name.clone()) {
            failures.push(CheckFailure {
                name,
                message: None,
            });
        }
    }

    // Libtest repeats the authoritative failing names in a `failures:` summary
    // immediately before each binary's `test result:` line. Collect those too:
    // inline `... FAILED` status lines can be absent or interleaved in captured
    // output, while `--no-fail-fast` emits one stable summary per binary.
    // Candidates are committed only when the block reaches `test result:` so the
    // earlier `failures:` detail section and its indented panic output are ignored.
    let mut in_failure_summary = false;
    let mut summary_names: Vec<String> = Vec::new();
    for line in clean.lines() {
        if line.trim() == "failures:" {
            in_failure_summary = true;
            summary_names.clear();
            continue;
        }
        if !in_failure_summary {
            continue;
        }
        if line.trim_start().starts_with("test result:") {
            for name in summary_names.drain(..) {
                if seen.insert(name.clone()) {
                    failures.push(CheckFailure {
                        name,
                        message: None,
                    });
                }
            }
            in_failure_summary = false;
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }
        if let Some(name) = line.strip_prefix("    ").map(str::trim) {
            if !name.is_empty() {
                summary_names.push(name.to_string());
                continue;
            }
        }
        in_failure_summary = false;
        summary_names.clear();
    }

    Some(ParsedCheckResult {
        // `nextest` is the persisted label for the whole Rust test-runner family,
        // including the wrapper's cargo-test fallback.
        parser: "nextest".to_string(),
        passed,
        failed,
        skipped: ignored,
        failures,
    })
}

// ---------------------------------------------------------------------------
// Shared rendering helpers (pure) used by both surfaces.
// ---------------------------------------------------------------------------

/// Render the one-line "what failed" fragment for a failing check:
/// `3 failed: a, b, c +N more`. `None` when the parse carries no failing names
/// (a degraded parse), so callers fall back to the exit code alone.
pub(crate) fn format_failure_names(parsed: &ParsedCheckResult) -> Option<String> {
    if parsed.failures.is_empty() {
        return None;
    }
    let shown: Vec<&str> = parsed
        .failures
        .iter()
        .take(MAX_FAILURE_NAMES)
        .map(|f| f.name.as_str())
        .collect();
    let more = parsed.failures.len().saturating_sub(shown.len());
    let names = if more > 0 {
        format!("{}, +{more} more", shown.join(", "))
    } else {
        shown.join(", ")
    };
    let count = parsed.failed.max(parsed.failures.len());
    Some(format!("{count} failed: {names}"))
}

/// Build the bounded failure-detail excerpt for a failing check. When the parse
/// carries per-failure messages (vitest / tsc), compose `name: message` lines
/// from them; otherwise fall back to the raw output tail (nextest, or a degraded
/// parse). Always bounded to [`FAILURE_EXCERPT_CHARS`].
pub(crate) fn format_failure_excerpt(parsed: Option<&ParsedCheckResult>, raw_tail: &str) -> String {
    if let Some(p) = parsed {
        let composed: Vec<String> = p
            .failures
            .iter()
            .filter_map(|f| f.message.as_ref().map(|m| format!("{}: {m}", f.name)))
            .collect();
        if !composed.is_empty() {
            return head_chars(&composed.join("\n"), FAILURE_EXCERPT_CHARS);
        }
    }
    tail_chars(raw_tail, FAILURE_EXCERPT_CHARS)
}

// ---------------------------------------------------------------------------
// Small string utilities.
// ---------------------------------------------------------------------------

static ANSI: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\x1b\[[0-9;]*[A-Za-z]").unwrap());

/// Strip ANSI SGR/cursor escape sequences so parsers see plain text regardless of
/// a runner's color output.
fn strip_ansi(s: &str) -> String {
    ANSI.replace_all(s, "").into_owned()
}

/// First non-empty line of `s`, trimmed.
fn first_line(s: &str) -> &str {
    s.lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("")
}

/// Keep the leading `max` chars (char-boundary safe).
fn head_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    s.chars().take(max).collect()
}

/// Keep the trailing `max` chars (char-boundary safe).
fn tail_chars(s: &str, max: usize) -> String {
    let n = s.chars().count();
    if n <= max {
        return s.to_string();
    }
    s.chars().skip(n - max).collect()
}

/// `head_chars` with a distinct name for message-length capping intent.
fn cap_chars(s: &str, max: usize) -> String {
    head_chars(s, max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_running_tests_names_nextest_slow_lines() {
        // A killed-at-budget nextest run leaves `SLOW [...]` lines for the tests
        // still in flight; those are exactly what to surface beside a timeout.
        let output = "\
   Compiling cairn-core v0.1.0
        SLOW [>  60.000s] cairn-core jj::tests::big_clone
        SLOW [> 120.000s] (2/9) cairn-core sync::tests::slow_roundtrip
        SLOW [>  60.000s] cairn-core jj::tests::big_clone
running 1900 tests";
        let running = extract_running_tests(output);
        assert_eq!(
            running,
            vec![
                "cairn-core jj::tests::big_clone".to_string(),
                "cairn-core sync::tests::slow_roundtrip".to_string(),
            ],
            "SLOW lines dedupe by identifier, keeping first-seen order"
        );
    }

    #[test]
    fn extract_running_tests_empty_without_slow_lines() {
        assert!(extract_running_tests("all quiet; no slow lines here").is_empty());
    }

    // Fixtures harvested from real deliberately-failing runs (see CAIRN-2282).

    const TSC_FIXTURE: &str = "\
bad.ts(1,7): error TS2322: Type 'string' is not assignable to type 'number'.
bad.ts(2,40): error TS2322: Type 'number' is not assignable to type 'string'.
bad.ts(3,24): error TS2322: Type 'number' is not assignable to type 'string'.";

    // A faithful subset of the vitest JSON reporter object, prefixed with human
    // output (dual reporter) and suffixed with stderr noise, to exercise the
    // locate-and-parse-first-value extraction.
    const VITEST_FIXTURE: &str = concat!(
        " FAIL  sample.test.ts > math suite > fails subtraction\n",
        "{\"numTotalTestSuites\":2,\"numPassedTests\":2,\"numFailedTests\":2,",
        "\"numPendingTests\":0,\"numTodoTests\":0,\"testResults\":[{\"assertionResults\":[",
        "{\"fullName\":\"math suite adds\",\"status\":\"passed\",\"failureMessages\":[]},",
        "{\"fullName\":\"math suite fails subtraction\",\"status\":\"failed\",",
        "\"failureMessages\":[\"AssertionError: expected 3 to be 4 // Object.is equality\\n    at foo\"]},",
        "{\"fullName\":\"top level fail\",\"status\":\"failed\",",
        "\"failureMessages\":[\"AssertionError: expected true to be false\"]},",
        "{\"fullName\":\"passes alone\",\"status\":\"passed\",\"failureMessages\":[]}",
        "]}]}\n",
        "some trailing stderr line\n"
    );

    const NEXTEST_FIXTURE: &str = "\
    FAIL [   0.088s] (3/4) fixcrate tests::fails_panic
        FAIL [   0.088s] (4/4) fixcrate tests::fails_math
     Summary [   0.093s] 4 tests run: 2 passed, 2 failed, 1 skipped
        FAIL [   0.088s] (3/4) fixcrate tests::fails_panic
        FAIL [   0.088s] (4/4) fixcrate tests::fails_math
error: test run failed";

    const CARGO_TEST_FIXTURE: &str = "\
running 5 tests
test tests::ignored_one ... ignored
test tests::passes_one ... ok
test tests::fails_panic ... FAILED
test tests::fails_math ... FAILED
test tests::passes_two ... ok

failures:
    tests::fails_math
    tests::fails_panic

test result: FAILED. 2 passed; 2 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s";

    #[test]
    fn detects_each_runner_from_the_command() {
        for command in [
            "bun run test:rust",
            "bun run test:rust:nextest",
            "cargo nextest run",
            "cargo test --workspace",
        ] {
            assert_eq!(detect_parser(command), Some(ParserKind::Rust), "{command}");
        }
        assert_eq!(
            detect_parser("bunx vitest related --reporter=json a.ts"),
            Some(ParserKind::Vitest)
        );
        assert_eq!(detect_parser("bunx tsc --noEmit"), Some(ParserKind::Tsc));
        assert_eq!(detect_parser("bun run test:api"), None);
        assert_eq!(detect_parser("bun run check:web"), None);
    }

    #[test]
    fn parses_tsc_errors() {
        let r = parse_check_output("bunx tsc --noEmit", TSC_FIXTURE).unwrap();
        assert_eq!(r.parser, "tsc");
        assert_eq!(r.failed, 3);
        assert_eq!(r.passed, 0);
        assert_eq!(r.failures[0].name, "bad.ts(1,7)");
        assert_eq!(
            r.failures[0].message.as_deref(),
            Some("TS2322: Type 'string' is not assignable to type 'number'.")
        );
    }

    #[test]
    fn tsc_clean_run_is_zero_failures() {
        let r = parse_check_output("bunx tsc --noEmit", "").unwrap();
        assert_eq!(r.parser, "tsc");
        assert_eq!(r.failed, 0);
        assert!(r.failures.is_empty());
    }

    #[test]
    fn parses_vitest_json_from_mixed_output() {
        let r = parse_check_output("bunx vitest run --reporter=json", VITEST_FIXTURE).unwrap();
        assert_eq!(r.parser, "vitest");
        assert_eq!(r.passed, 2);
        assert_eq!(r.failed, 2);
        assert_eq!(r.skipped, 0);
        assert_eq!(r.failures.len(), 2);
        assert_eq!(r.failures[0].name, "math suite fails subtraction");
        assert_eq!(
            r.failures[0].message.as_deref(),
            Some("AssertionError: expected 3 to be 4 // Object.is equality")
        );
        assert_eq!(r.failures[1].name, "top level fail");
    }

    #[test]
    fn vitest_without_json_blob_degrades_to_none() {
        // A vitest command whose output carries no JSON object (reporter absent)
        // parses to nothing — the caller degrades to the raw tail, never a pass.
        assert!(parse_check_output("bunx vitest run", " FAIL  a.test.ts\n").is_none());
    }

    #[test]
    fn parses_nextest_summary_and_dedupes_fail_lines() {
        let r = parse_check_output("bun run test:rust:nextest", NEXTEST_FIXTURE).unwrap();
        assert_eq!(r.parser, "nextest");
        assert_eq!(r.passed, 2);
        assert_eq!(r.failed, 2);
        assert_eq!(r.skipped, 1);
        // The two FAIL lines appear twice each in the output but dedupe by name.
        assert_eq!(r.failures.len(), 2);
        assert_eq!(r.failures[0].name, "fixcrate tests::fails_panic");
        assert_eq!(r.failures[1].name, "fixcrate tests::fails_math");
        assert!(r.failures[0].message.is_none());
    }

    #[test]
    fn rust_parser_falls_back_to_cargo_test() {
        // No nextest Summary line ⇒ the cargo-test fallback parses libtest output.
        let r = parse_check_output("bun run test:rust:nextest", CARGO_TEST_FIXTURE).unwrap();
        assert_eq!(r.passed, 2);
        assert_eq!(r.failed, 2);
        assert_eq!(r.skipped, 1);
        let names: Vec<&str> = r.failures.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, vec!["tests::fails_panic", "tests::fails_math"]);
    }

    #[test]
    fn cargo_test_collects_failure_summaries_across_binaries_and_dedupes() {
        let output = "\
running 1 test

failures:

---- alpha::breaks stdout ----
    panic output that is not a test name

failures:
    alpha::breaks

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out

running 2 tests
test alpha::breaks ... FAILED
test beta::also_breaks ... FAILED

failures:
    alpha::breaks
    beta::also_breaks

test result: FAILED. 0 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out

running 1 test

failures:
    src/lib.rs - module::item (line 42)

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out";
        let parsed = parse_check_output("cargo test --workspace", output).unwrap();
        let names: Vec<&str> = parsed.failures.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "alpha::breaks",
                "beta::also_breaks",
                "src/lib.rs - module::item (line 42)",
            ]
        );
        assert_eq!(parsed.failed, 4);
    }

    #[test]
    fn unrecognized_rust_output_degrades_to_none() {
        assert!(
            parse_check_output("bun run test:rust:nextest", "error: could not compile").is_none()
        );
    }

    #[test]
    fn format_failure_names_caps_and_counts() {
        let parsed = ParsedCheckResult {
            parser: "tsc".to_string(),
            passed: 0,
            failed: 8,
            skipped: 0,
            failures: (0..8)
                .map(|i| CheckFailure {
                    name: format!("f{i}"),
                    message: None,
                })
                .collect(),
        };
        let s = format_failure_names(&parsed).unwrap();
        assert!(s.starts_with("8 failed: f0, f1, f2, f3, f4"));
        assert!(s.contains("+3 more"));
        assert!(!s.contains("f5"));
    }

    #[test]
    fn format_failure_names_empty_is_none() {
        let parsed = ParsedCheckResult {
            parser: "nextest".to_string(),
            passed: 3,
            failed: 0,
            skipped: 0,
            failures: vec![],
        };
        assert!(format_failure_names(&parsed).is_none());
    }

    #[test]
    fn excerpt_prefers_messages_then_raw_tail() {
        // vitest/tsc: composed name: message lines.
        let with_msgs = ParsedCheckResult {
            parser: "vitest".to_string(),
            passed: 0,
            failed: 1,
            skipped: 0,
            failures: vec![CheckFailure {
                name: "suite test".to_string(),
                message: Some("AssertionError: boom".to_string()),
            }],
        };
        let e = format_failure_excerpt(Some(&with_msgs), "RAW TAIL");
        assert_eq!(e, "suite test: AssertionError: boom");

        // nextest (no messages): raw tail.
        let no_msgs = ParsedCheckResult {
            parser: "nextest".to_string(),
            passed: 0,
            failed: 1,
            skipped: 0,
            failures: vec![CheckFailure {
                name: "crate test::x".to_string(),
                message: None,
            }],
        };
        assert_eq!(
            format_failure_excerpt(Some(&no_msgs), "panic at src/lib.rs"),
            "panic at src/lib.rs"
        );

        // degraded parse (None): raw tail.
        assert_eq!(
            format_failure_excerpt(None, "exit 101 tail"),
            "exit 101 tail"
        );
    }

    #[test]
    fn parsed_result_json_roundtrips() {
        let parsed = parse_check_output("bunx tsc --noEmit", TSC_FIXTURE).unwrap();
        let json = serde_json::to_string(&parsed).unwrap();
        let back: ParsedCheckResult = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, back);
        // camelCase field name is the persisted contract.
        assert!(json.contains("\"parser\":\"tsc\""));
    }
}
