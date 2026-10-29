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
//! A structured result file (`CAIRN_CHECK_RESULT_JSON=`, folded into the output
//! by the executor) names its own parser, so it wins for ANY command: a
//! per-test result from any test runner in [`TEST_RUNNER_PARSERS`]
//! (scripts/test-rust-nextest.ts writes `nextest`, scripts/test-frontend.ts
//! writes `vitest`) or a `rust-lint` verdict (scripts/check-rust.ts). Without
//! one, the runner family is detected from the COMMAND STRING, not a config
//! field, so the `checks` schema stays unextended and no new field can drift
//! across its four consumers (`config/project_settings.rs`, the TS
//! `CheckCommand`, the editor, and docs). The repo's Rust entry points
//! (`test:rust`, `nextest`, and `cargo test`) route to the rust parser,
//! `vitest` to the vitest parser, `bun test` to the bun parser, and `tsc` to the
//! tsc parser. A command that names none of them (`bun run test:api`) is still
//! read when its output carries a runner's own closing summary: bun's
//! `Ran N tests across M files.` or vitest's `Tests  … (N)`.
//!
//! ## The persisted shape (`target_results_json`)
//!
//! [`ParsedCheckResult`] is what serializes into the `check_result_cache`
//! `target_results_json` column. It is deliberately the SUBSTRATE future
//! baseline/delta work consumes: `failures[].name` is a stable per-test
//! identifier that can be compared across two sealed trees to attribute a break
//! to a diff. The full shape is documented in `docs/checks.md`.

use std::borrow::Cow;
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

/// Cap on a structured result's `report`. Mirrors `RUST_LINT_REPORT_CHARS` in
/// scripts/lib/rust-lint-result.ts; re-applied here because the file is input.
const REPORT_CHARS: usize = 1_500;

/// The `parser` a `scripts/check-rust.ts` result file names: a static lint
/// verdict, not a test enumeration.
pub(crate) const RUST_LINT_PARSER: &str = "rust-lint";

/// The `parser` a `bun test` run is recorded under.
pub(crate) const BUN_TEST_PARSER: &str = "bun-test";

/// Every `parser` whose counts are real tests, as opposed to `tsc`'s type-error
/// tally or `rust-lint`'s gating sites. The one list every verdict, reuse, and
/// rendering decision consults, and the set of result-file parsers accepted as
/// per-test results.
const TEST_RUNNER_PARSERS: [&str; 3] = ["nextest", "vitest", BUN_TEST_PARSER];

/// Whether `parser` names a test runner (see [`TEST_RUNNER_PARSERS`]).
pub(crate) fn is_test_runner_parser(parser: &str) -> bool {
    TEST_RUNNER_PARSERS.contains(&parser)
}

/// Structured result of parsing one check's output. Serializes (camelCase) into
/// the `check_result_cache.target_results_json` column. Enrichment only — the
/// authoritative pass/fail verdict is the exit code, stored separately.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParsedCheckResult {
    /// Version of the canonical per-test JSON contract.
    #[serde(default = "current_result_schema_version")]
    pub(crate) schema_version: u32,
    /// Whether the producer enumerated the complete selected test manifest.
    #[serde(default)]
    pub(crate) complete: bool,
    /// `full`, `filtered`, `empty`, or `unknown`.
    #[serde(default = "unknown_selection")]
    pub(crate) selection: String,
    /// Final result for every selected test when `complete` is true.
    #[serde(default)]
    pub(crate) tests: Vec<CheckTestResult>,
    /// Self-skips lacking manifest or environment authority.
    #[serde(default)]
    pub(crate) undeclared_skips: usize,
    /// Which runner produced this: a [`TEST_RUNNER_PARSERS`] entry, `"tsc"`,
    /// or [`RUST_LINT_PARSER`].
    pub(crate) parser: String,
    /// Passing test count. `0` for tsc, which has no test concept.
    pub(crate) passed: usize,
    /// Failing test / error count (the runner's own tally when available).
    pub(crate) failed: usize,
    /// Suites that failed as a whole without any test failing: a vitest file
    /// that could not be COLLECTED (unresolved import, `vi.mock` hoisting error,
    /// throwing top-level). Such a file runs no test, so it contributes nothing
    /// to `failed` even though it fails the run — leaving a red check with
    /// nothing named. Zero for runners that have no separate collection phase.
    #[serde(default)]
    pub(crate) suite_failures: usize,
    /// Skipped / ignored / todo test count.
    pub(crate) skipped: usize,
    /// Tests the rust-tests fast tier terminated at its per-test budget:
    /// started, never finished. Neither passed nor failed, so they are outside
    /// `tests_run()` and can never be counted as a pass. Nonzero only under
    /// `selection: "fast"`.
    #[serde(default)]
    pub(crate) deferred: usize,
    /// The failing tests, in report order. This is the substrate future
    /// baseline/delta work compares across trees, so `name` is a stable
    /// identifier.
    pub(crate) failures: Vec<CheckFailure>,
    /// A compact, summary-first rendering of the verdict written by the check
    /// itself (`rust-lint` today). The stored output excerpt leads with it, so a
    /// red's cause survives however much chatter the run printed after it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) report: Option<String>,
    /// Where the lane's wall clock went, when its runner measured it (the
    /// rust-tests runner does; see `scripts/lib/lane-phases.ts`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) phases: Option<CheckPhases>,
    /// The run's own statement that its exit vouches for nothing, and why: the
    /// producer judged the environment it ran in unfit to answer for the change
    /// (`runtime bun 1.3.13 ≠ pinned 1.4.4`). Taken from the structured result
    /// whichever parser produced the rest, and a green carrying it is
    /// unverified however complete the rest of the result is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) unverified: Option<String>,
}

/// A lane's duration divided into phases on one timeline, so they sum to the
/// command's wall clock: `setup` (the command's prelude, probes, planning, and
/// the fast tier's fixture run), `compile` (each cargo pass up to its `Finished`
/// line), `test` (each pass after it), `doctest`, and `publish` (result
/// assembly between and after passes).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CheckPhases {
    /// `executor` when the timeline starts at the executor's spawn stamp,
    /// `process` when it starts at the runner script, missing the prelude.
    pub(crate) origin: String,
    pub(crate) setup_ms: u64,
    pub(crate) compile_ms: u64,
    pub(crate) test_ms: u64,
    #[serde(default)]
    pub(crate) doctest_ms: u64,
    pub(crate) publish_ms: u64,
    /// Crates cargo compiled across every pass. Zero means nothing was stale.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) compiled_units: Option<u64>,
    #[serde(default)]
    pub(crate) passes: Vec<CheckPhasePass>,
}

/// One cargo invocation of a lane: `full`, `related`, `targeted`, the fast
/// tier's `local`, `warmup`, and `global`, or `doctest`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CheckPhasePass {
    pub(crate) name: String,
    pub(crate) wall_ms: u64,
    pub(crate) compile_ms: u64,
    pub(crate) test_ms: u64,
    #[serde(default)]
    pub(crate) compiled_units: u64,
    /// nextest's own `Summary [ Xs ]` for the pass, when it printed one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) summary_ms: Option<u64>,
}

/// More passes than any lane runs; a result claiming more is not a lane's.
const MAX_PHASE_PASSES: usize = 16;

impl CheckPhases {
    /// The `phases` a result file carries, when it is one this core can store.
    /// The file is the runner's input, so a malformed or implausible object is
    /// dropped rather than trusted; the rest of the result still parses.
    fn from_artifact(value: &serde_json::Value) -> Option<Self> {
        let phases: Self = serde_json::from_value(value.get("phases")?.clone()).ok()?;
        let representable = [
            phases.setup_ms,
            phases.compile_ms,
            phases.test_ms,
            phases.doctest_ms,
            phases.publish_ms,
        ]
        .into_iter()
        .chain(phases.compiled_units)
        .chain(phases.passes.iter().flat_map(|pass| {
            [
                pass.wall_ms,
                pass.compile_ms,
                pass.test_ms,
                pass.compiled_units,
            ]
        }))
        .all(|value| i64::try_from(value).is_ok());
        (matches!(phases.origin.as_str(), "executor" | "process")
            && phases.passes.len() <= MAX_PHASE_PASSES
            && representable)
            .then_some(phases)
    }
}

fn current_result_schema_version() -> u32 {
    1
}

fn unknown_selection() -> String {
    "unknown".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CheckTestResult {
    pub(crate) id: String,
    pub(crate) status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) duration_ms: Option<u64>,
    #[serde(default = "one_attempt")]
    pub(crate) attempts: u32,
    #[serde(default)]
    pub(crate) retried: bool,
    #[serde(default)]
    pub(crate) flaky: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) failure_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) skip_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) skip_declaration: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) skip_declaration_source: Option<String>,
}

fn one_attempt() -> u32 {
    1
}

impl ParsedCheckResult {
    /// Whether this parse came from a TEST RUNNER (`nextest`/`vitest`), whose
    /// pass/fail counts denote real tests, rather than `tsc`, whose `failed` is a
    /// type-error tally with no "test count" meaning. Verdict surfaces gate the
    /// `N tests` / `no tests matched` rendering on this so a passing typecheck is
    /// never labelled with a bogus test count.
    pub(crate) fn is_test_runner(&self) -> bool {
        self.verdict_facts().is_test_runner()
    }

    /// See [`VerdictFacts::is_incomplete_lint`].
    pub(crate) fn is_incomplete_lint(&self) -> bool {
        self.verdict_facts().is_incomplete_lint()
    }

    /// See [`VerdictFacts::tests_run`].
    pub(crate) fn tests_run(&self) -> usize {
        self.verdict_facts().tests_run()
    }

    /// The fields of this result that decide whether its green is verified.
    pub(crate) fn verdict_facts(&self) -> VerdictFacts<'_> {
        VerdictFacts {
            parser: Cow::Borrowed(&self.parser),
            selection: Cow::Borrowed(&self.selection),
            complete: self.complete,
            passed: self.passed,
            failed: self.failed,
            skipped: self.skipped,
            unverified: self.unverified.as_deref().map(Cow::Borrowed),
        }
    }

    /// Whether this is a fast-tier lane result: the change's related selection
    /// run in full, then the rest of the workspace under a per-test budget. Its
    /// verdict never speaks for the whole suite.
    pub(crate) fn is_fast_tier(&self) -> bool {
        self.selection == FAST_TIER_SELECTION
    }

    /// The one rendering of a fast-tier population, shared by every verdict
    /// surface so a fast green can never read as a bare one: `fast tier: 8501
    /// ran, 3596 deferred` (plus `, N skipped` when any were). `None` for any
    /// other selection.
    pub(crate) fn fast_tier_label(&self) -> Option<String> {
        self.is_fast_tier()
            .then(|| fast_tier_label(self.tests_run(), self.deferred, self.skipped))
    }
}

/// Everything of a [`ParsedCheckResult`] that
/// [`crate::execution::checks_status::verdict_evidence`] reads, and nothing else.
///
/// The per-test array is the bulk of a stored result (megabytes for a full
/// rust-tests run) and has no bearing on the verdict, so a reader that needs
/// only the verdict projects these fields out in SQL with
/// [`VERDICT_FACTS_SQL`] instead of copying and parsing the whole document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerdictFacts<'a> {
    pub(crate) parser: Cow<'a, str>,
    pub(crate) selection: Cow<'a, str>,
    pub(crate) complete: bool,
    pub(crate) passed: usize,
    pub(crate) failed: usize,
    pub(crate) skipped: usize,
    /// See [`ParsedCheckResult::unverified`].
    pub(crate) unverified: Option<Cow<'a, str>>,
}

/// Projects `target_results_json` onto the [`VerdictFacts`] fields as one small
/// JSON array, parsed once inside the database. Malformed JSON projects to
/// NULL, which reads as no structured result exactly as a failed full parse
/// does, instead of failing the whole query.
pub(crate) const VERDICT_FACTS_SQL: &str = "CASE WHEN json_valid(target_results_json) \
     THEN json_extract(target_results_json,'$.parser','$.selection','$.complete',\
     '$.passed','$.failed','$.skipped','$.unverified') END";

impl VerdictFacts<'_> {
    /// Whether these facts came from a TEST RUNNER ([`TEST_RUNNER_PARSERS`]),
    /// whose pass/fail counts denote real tests, rather than `tsc`, whose
    /// `failed` is a type-error tally with no "test count" meaning.
    pub(crate) fn is_test_runner(&self) -> bool {
        is_test_runner_parser(&self.parser)
    }

    /// A `rust-lint` result whose lint did not run to completion: cargo was not
    /// runnable, no crate was selected, or clippy died without a verdict. Such a
    /// result measured nothing, so an exit 0 beside it is not a verified pass.
    ///
    /// Deliberately not every non-test parse with `complete == false`: tsc sets
    /// `complete: false` to mean "not a test enumeration", not "never measured".
    pub(crate) fn is_incomplete_lint(&self) -> bool {
        self.parser == RUST_LINT_PARSER && !self.complete
    }

    /// Tests the runner actually executed: passed + failed. Skipped/ignored tests
    /// did not run, and deferred tests never reached a verdict, so both are
    /// excluded from the "was anything validated?" tally that distinguishes a
    /// real green from a zero-selection green.
    pub(crate) fn tests_run(&self) -> usize {
        self.passed + self.failed
    }
}

impl VerdictFacts<'static> {
    /// Decodes a [`VERDICT_FACTS_SQL`] projection under the same rules
    /// [`ParsedCheckResult`] deserializes with: `parser` and the three counts
    /// are required (a document missing one has no structured result),
    /// `selection` defaults to `unknown`, and `complete` to false.
    pub(crate) fn from_projection(projection: &str) -> Option<Self> {
        type Projection = (
            Option<String>,
            Option<String>,
            Option<bool>,
            Option<usize>,
            Option<usize>,
            Option<usize>,
            Option<String>,
        );
        let (parser, selection, complete, passed, failed, skipped, unverified): Projection =
            serde_json::from_str(projection).ok()?;
        Some(Self {
            parser: Cow::Owned(parser?),
            selection: Cow::Owned(selection.unwrap_or_else(unknown_selection)),
            complete: complete.unwrap_or(false),
            passed: passed?,
            failed: failed?,
            skipped: skipped?,
            unverified: unverified.map(Cow::Owned),
        })
    }
}

/// The `selection` a fast-tier lane result declares.
pub(crate) const FAST_TIER_SELECTION: &str = "fast";

/// `fast tier: N ran, D deferred[, S skipped]` — the label every surface uses.
pub(crate) fn fast_tier_label(ran: usize, deferred: usize, skipped: usize) -> String {
    let mut label = format!("fast tier: {ran} ran, {deferred} deferred");
    if skipped > 0 {
        label.push_str(&format!(", {skipped} skipped"));
    }
    label
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
    BunTest,
    Tsc,
}

/// Whether `command` names Vitest itself.
pub(crate) fn is_vitest_command(command: &str) -> bool {
    detect_parser(command) == Some(ParserKind::Vitest)
}

/// Whether `command` names a test runner, so a run of it that produced no
/// structured result measured nothing a verdict can rest on.
pub(crate) fn is_test_runner_command(command: &str) -> bool {
    matches!(
        detect_parser(command),
        Some(ParserKind::Rust | ParserKind::Vitest | ParserKind::BunTest)
    )
}

/// Detect the runner family from the command string. Returns `None` for a
/// command with no recognized runner; [`parse_check_output`] then looks for a
/// runner's closing summary in the output before degrading to exit code + raw
/// tail.
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
    if command.contains("bun test") {
        return Some(ParserKind::BunTest);
    }
    if command.contains("tsc") {
        return Some(ParserKind::Tsc);
    }
    None
}

/// Parse one check's combined output into a [`ParsedCheckResult`]. A structured
/// result file wins regardless of the command; otherwise the parser is selected
/// from `command`, and for a command that names no runner (a package script such
/// as `bun run test:api`), from the runner summary its output closes with.
/// Returns `None` when none of these yields anything — the caller then degrades
/// to the raw output tail. A recognized runner that simply found no failures
/// still returns `Some` (a zero-failure result), which is a valid "green at this
/// tree" record for future baseline work.
///
/// A structured result's `unverified` declaration rides on whichever parse wins,
/// and stands alone (a result with no counts) for a runner Cairn has no parser
/// for, so a lane can disown its green without first growing a parser.
pub(crate) fn parse_check_output(command: &str, output: &str) -> Option<ParsedCheckResult> {
    let clean = strip_ansi(output);
    let parsed = parse_structured_artifact(&clean).or_else(|| match detect_parser(command) {
        Some(ParserKind::Rust) => parse_rust(&clean),
        Some(ParserKind::Vitest) => parse_vitest(&clean),
        Some(ParserKind::BunTest) => parse_bun_test(&clean),
        Some(ParserKind::Tsc) => parse_tsc(&clean),
        None => parse_bun_test(&clean).or_else(|| parse_vitest_summary(&clean)),
    });
    let Some((declaring_parser, reason)) = declared_unverified(&clean) else {
        return parsed;
    };
    Some(match parsed {
        Some(parsed) => ParsedCheckResult {
            unverified: Some(reason),
            ..parsed
        },
        None => ParsedCheckResult {
            schema_version: 1,
            complete: false,
            selection: unknown_selection(),
            tests: vec![],
            undeclared_skips: 0,
            parser: declaring_parser,
            passed: 0,
            failed: 0,
            suite_failures: 0,
            skipped: 0,
            deferred: 0,
            failures: vec![],
            report: None,
            phases: None,
            unverified: Some(reason),
        },
    })
}

/// The runner a structured result names and its `unverified` declaration, when
/// it makes one: a non-blank string, capped like any other excerpt because the
/// file is the command's input.
fn declared_unverified(clean: &str) -> Option<(String, String)> {
    let value = structured_artifact(clean)?;
    let reason = value.get("unverified")?.as_str()?.trim();
    if reason.is_empty() {
        return None;
    }
    let parser = value
        .get("parser")
        .and_then(|parser| parser.as_str())
        .unwrap_or("unknown");
    Some((parser.to_string(), cap_chars(reason, MESSAGE_CHARS)))
}

/// The line prefix the executor writes a check's result file under.
pub(crate) const RESULT_ARTIFACT_JSON_MARKER: &str = "CAIRN_CHECK_RESULT_JSON=";

/// The structured result a runner script handed back through the executor, as
/// the last `CAIRN_CHECK_RESULT_JSON=` line of the output, when it decodes to a
/// schema this coordinator understands. The executor appends that line itself,
/// after the child exits, so the last one is the executor's and an earlier one
/// is something the command printed.
fn structured_artifact(output: &str) -> Option<serde_json::Value> {
    let json = output
        .lines()
        .rev()
        .find_map(|line| line.trim().strip_prefix(RESULT_ARTIFACT_JSON_MARKER))?;
    let value: serde_json::Value = serde_json::from_str(json).ok()?;
    (value.get("schemaVersion")?.as_u64()? == 1).then_some(value)
}

/// Whether the runner declared that this run selected nothing to check.
///
/// True only for a structured result whose `selection` is `"empty"`, whichever
/// runner wrote it. A missing or malformed result is not a declaration, so it
/// answers false: absence of evidence about the selection is not evidence that
/// nothing ran.
pub(crate) fn declared_empty_selection(output: &str) -> bool {
    structured_artifact(output)
        .and_then(|value| {
            value
                .get("selection")
                .and_then(|selection| selection.as_str())
                .map(|selection| selection == "empty")
        })
        .unwrap_or(false)
}

/// Parse the result file. Only parsers this core understands are accepted: a
/// per-test result from any [`TEST_RUNNER_PARSERS`] runner, or a `rust-lint`
/// verdict. Anything else (an unknown parser, malformed JSON) returns `None` so
/// command detection still gets its turn.
fn parse_structured_artifact(clean: &str) -> Option<ParsedCheckResult> {
    let value = structured_artifact(clean)?;
    match value.get("parser")?.as_str()? {
        RUST_LINT_PARSER => parse_rust_lint_artifact(&value),
        parser if is_test_runner_parser(parser) => parse_test_artifact(&value, parser),
        _ => None,
    }
}

/// A canonical per-test result. Every count is derived from the rows, so a
/// writer cannot claim a tally its rows do not carry.
fn parse_test_artifact(value: &serde_json::Value, parser: &str) -> Option<ParsedCheckResult> {
    let tests: Vec<CheckTestResult> = serde_json::from_value(value.get("tests")?.clone()).ok()?;
    let passed = tests.iter().filter(|test| test.status == "passed").count();
    let failed = tests.iter().filter(|test| test.status == "failed").count();
    let skipped = tests.iter().filter(|test| test.status == "skipped").count();
    // A deferred row is counted only as deferred. It is never folded into
    // `passed` (it did not pass) nor `failed` (no assertion failed).
    let deferred = tests
        .iter()
        .filter(|test| test.status == "deferred")
        .count();
    let failures = tests
        .iter()
        .filter(|test| test.status == "failed")
        .map(|test| CheckFailure {
            name: test.id.clone(),
            message: test
                .failure_message
                .clone()
                .map(|message| cap_chars(first_line(&message), MESSAGE_CHARS)),
        })
        .collect();
    Some(ParsedCheckResult {
        schema_version: 1,
        complete: value
            .get("complete")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        selection: value
            .get("selection")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string(),
        tests,
        undeclared_skips: value
            .get("undeclaredSkips")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize,
        parser: parser.to_string(),
        passed,
        failed,
        skipped,
        suite_failures: 0,
        deferred,
        report: None,
        phases: CheckPhases::from_artifact(value),
        failures,
        unverified: None,
    })
}

/// A `scripts/check-rust.ts` lint verdict. `complete` is taken from the file and
/// read as `false` when missing, so a result that cannot vouch for a full lint
/// never becomes a clean measurement. No test counts are invented: `failed` is
/// the gating-site tally, as it is for tsc.
fn parse_rust_lint_artifact(value: &serde_json::Value) -> Option<ParsedCheckResult> {
    #[derive(serde::Deserialize)]
    struct LintFailure {
        name: String,
        #[serde(default)]
        message: Option<String>,
    }
    let failures: Vec<LintFailure> = serde_json::from_value(value.get("failures")?.clone()).ok()?;
    let failures: Vec<CheckFailure> = failures
        .into_iter()
        .map(|failure| CheckFailure {
            name: failure.name,
            message: failure
                .message
                .map(|message| cap_chars(first_line(&message), MESSAGE_CHARS))
                .filter(|message| !message.is_empty()),
        })
        .collect();
    let report = value
        .get("report")
        .and_then(|v| v.as_str())
        .map(|report| head_chars(report.trim_end(), REPORT_CHARS))
        .filter(|report| !report.trim().is_empty());
    Some(ParsedCheckResult {
        schema_version: 1,
        complete: value
            .get("complete")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        selection: "unknown".to_string(),
        tests: vec![],
        undeclared_skips: 0,
        parser: RUST_LINT_PARSER.to_string(),
        passed: 0,
        failed: failures.len(),
        skipped: 0,
        suite_failures: 0,
        phases: None,
        deferred: 0,
        failures,
        report,
        unverified: None,
    })
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
        schema_version: 1,
        complete: false,
        selection: "unknown".to_string(),
        tests: vec![],
        undeclared_skips: 0,
        parser: "tsc".to_string(),
        passed: 0,
        failed: failures.len(),
        skipped: 0,
        suite_failures: 0,
        phases: None,
        deferred: 0,
        report: None,
        failures,
        unverified: None,
    })
}

// ---------------------------------------------------------------------------
// vitest (JSON reporter)
// ---------------------------------------------------------------------------

fn parse_vitest(clean: &str) -> Option<ParsedCheckResult> {
    parse_vitest_json(clean).or_else(|| parse_vitest_summary(clean))
}

fn parse_vitest_json(clean: &str) -> Option<ParsedCheckResult> {
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

    let empty = Vec::new();
    let files = value
        .get("testResults")
        .and_then(|v| v.as_array())
        .unwrap_or(&empty);

    // Vitest reports each file by absolute path, which on a check run is rooted
    // in an ephemeral build slot. Strip the deepest directory every reported file
    // shares so a suite name is legible and identical across trees.
    let root = common_dir_prefix(files.iter().filter_map(|f| f.get("name")?.as_str()));

    let mut failures = Vec::new();
    let mut tests = Vec::new();
    let mut suite_failures = 0usize;
    for file in files {
        let mut assertion_failed = false;
        for a in file
            .get("assertionResults")
            .and_then(|v| v.as_array())
            .unwrap_or(&empty)
        {
            let status = a
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
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
            let canonical_status = match status {
                "passed" => "passed",
                "failed" => "failed",
                "pending" | "todo" | "skipped" => "skipped",
                _ => continue,
            };
            tests.push(CheckTestResult {
                id: name.clone(),
                status: canonical_status.to_string(),
                duration_ms: a.get("duration").and_then(|v| v.as_u64()),
                attempts: 1,
                retried: false,
                flaky: false,
                failure_message: message.clone(),
                skip_reason: None,
                skip_declaration: None,
                skip_declaration_source: None,
            });
            if status == "failed" {
                assertion_failed = true;
                failures.push(CheckFailure { name, message });
            }
        }

        // A file that failed while no assertion in it did never got as far as
        // running a test: collection threw. Vitest tallies that only in
        // `numFailedTestSuites` and puts the reason in the file's own `message`,
        // so reading `numFailedTests` alone yields a red check with zero named
        // failures. Record the file as the failure site it is.
        if !assertion_failed && file.get("status").and_then(|v| v.as_str()) == Some("failed") {
            suite_failures += 1;
            let name = file
                .get("name")
                .and_then(|v| v.as_str())
                .map(|n| strip_root(n, &root))
                .unwrap_or_else(|| "<unknown suite>".to_string());
            let message = file
                .get("message")
                .and_then(|v| v.as_str())
                .map(|m| strip_root(m, &root))
                .filter(|m| !m.trim().is_empty())
                .map(|m| cap_chars(first_line(&m), MESSAGE_CHARS));
            failures.push(CheckFailure { name, message });
        }
    }

    Some(ParsedCheckResult {
        schema_version: 1,
        complete: tests.len() == passed + failed + skipped,
        selection: "unknown".to_string(),
        tests,
        undeclared_skips: 0,
        parser: "vitest".to_string(),
        passed,
        failed,
        skipped,
        suite_failures,
        deferred: 0,
        report: None,
        phases: None,
        failures,
        unverified: None,
    })
}

// ---------------------------------------------------------------------------
// vitest (printed report, when no JSON reporter ran)
// ---------------------------------------------------------------------------

static VITEST_ROW: LazyLock<Regex> = LazyLock::new(|| {
    // ` ✓ src/a.test.ts > suite > name 3ms`. Only a TEST row carries ` > `: the
    // default reporter's per-file line (` ✓ src/a.test.ts (11 tests) 5ms`) does
    // not, so it is never counted as a test.
    Regex::new(r"^\s*([✓×↓])\s+(.+? > .+?)(?:\s+(\d+(?:\.\d+)?)ms)?$").unwrap()
});

static VITEST_TESTS_SUMMARY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s*Tests\s+(.+?)\s*\((\d+)\)$").unwrap());

static VITEST_TALLY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s*(\d+)\s+(passed|failed|skipped|todo)\s*$").unwrap());

/// Vitest's printed report: the `Tests  1 failed | 10 passed (11)` summary each
/// vitest process closes with (summed, so a sharded run reads as one), and the
/// verbose reporter's per-test `✓`/`×`/`↓` rows. Complete only when those rows
/// account for every test the summaries count; the default reporter prints no
/// passing rows, so it yields counts and an incomplete enumeration.
fn parse_vitest_summary(clean: &str) -> Option<ParsedCheckResult> {
    let (mut total, mut passed, mut failed, mut skipped) = (0usize, 0usize, 0usize, 0usize);
    let mut summaries = 0usize;
    let mut tests: Vec<CheckTestResult> = Vec::new();
    let mut failures: Vec<CheckFailure> = Vec::new();
    let mut awaiting_message = false;
    for line in clean.lines() {
        let line = line.trim_end();
        if let Some(cap) = VITEST_TESTS_SUMMARY.captures(line) {
            summaries += 1;
            total += cap[2].parse::<usize>().unwrap_or(0);
            for part in cap[1].split('|') {
                let Some(tally) = VITEST_TALLY.captures(part) else {
                    continue;
                };
                let n = tally[1].parse::<usize>().unwrap_or(0);
                match &tally[2] {
                    "passed" => passed += n,
                    "failed" => failed += n,
                    _ => skipped += n,
                }
            }
            awaiting_message = false;
            continue;
        }
        if let Some(cap) = VITEST_ROW.captures(line) {
            let status = match &cap[1] {
                "✓" => "passed",
                "×" => "failed",
                _ => "skipped",
            };
            let id = cap[2].to_string();
            if status == "failed" {
                failures.push(CheckFailure {
                    name: id.clone(),
                    message: None,
                });
            }
            awaiting_message = status == "failed";
            tests.push(test_row(id, status, millis(cap.get(3), false)));
            continue;
        }
        // The verbose reporter prints a failed row's assertion on the next line
        // as `   → expected 1 to be 2`.
        if awaiting_message {
            awaiting_message = false;
            if let Some(message) = line.trim_start().strip_prefix("→ ") {
                let message = cap_chars(message.trim(), MESSAGE_CHARS);
                if let Some(test) = tests.last_mut() {
                    test.failure_message = Some(message.clone());
                }
                if let Some(failure) = failures.last_mut() {
                    failure.message = Some(message);
                }
            }
        }
    }
    if summaries == 0 {
        return None;
    }
    let complete = rows_account_for(&tests, total, passed, failed, skipped);
    Some(ParsedCheckResult {
        schema_version: 1,
        complete,
        selection: if total == 0 { "empty" } else { "unknown" }.to_string(),
        tests: if complete { tests } else { vec![] },
        undeclared_skips: 0,
        parser: "vitest".to_string(),
        passed,
        failed,
        skipped,
        suite_failures: 0,
        deferred: 0,
        report: None,
        phases: None,
        failures,
        unverified: None,
    })
}

// ---------------------------------------------------------------------------
// bun test
// ---------------------------------------------------------------------------

static BUN_RAN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^Ran (\d+) tests? across (\d+) files?\.").unwrap());

static BUN_TALLY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s*(\d+) (pass|fail|skip|todo|errors?)$").unwrap());

static BUN_ROW: LazyLock<Regex> = LazyLock::new(|| {
    // `(pass) suite > name [0.48ms]` when piped; the glyph form when bun was
    // told to color its output.
    Regex::new(
        r"^\s*(?:\((pass|fail|skip|todo)\)|([✓✗»✎]))\s+(.+?)(?:\s+\[(\d+(?:\.\d+)?)(ms|s)\])?$",
    )
    .unwrap()
});

static BUN_FILE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(\S.*\.[cm]?[jt]sx?):$").unwrap());

static BUN_RECAP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d+ tests? (?:skipped|failed|todo):$").unwrap());

/// `bun test`'s report. Bun prints a `path/to/file.test.ts:` header before each
/// file's rows, one `(pass)`/`(fail)`/`(skip)`/`(todo)` row per test, then a
/// recap that repeats the skipped and failed rows without their file, then the
/// tallies (` 799 pass`, ` 24 skip`, ` 0 fail`, ` 10 errors`) and
/// `Ran 823 tests across 58 files.`, the line that identifies bun output.
///
/// Row ids are `<file> > <test>`, so the recap is skipped rather than deduped:
/// its rows have no file. The run is complete when its rows account for every
/// test the tallies count. `errors` are failures bun could not attribute to a
/// test (an error thrown between tests), so they count as suite failures.
fn parse_bun_test(clean: &str) -> Option<ParsedCheckResult> {
    #[derive(Default)]
    struct Tally {
        pass: usize,
        fail: usize,
        skip: usize,
        errors: usize,
    }
    let mut totals = Tally::default();
    let mut pending = Tally::default();
    let mut ran = 0usize;
    let mut summaries = 0usize;
    let mut file: Option<String> = None;
    let mut in_recap = false;
    let mut last_error: Option<String> = None;
    let mut tests: Vec<CheckTestResult> = Vec::new();
    let mut failures: Vec<CheckFailure> = Vec::new();
    for line in clean.lines() {
        let line = line.trim_end();
        if let Some(cap) = BUN_RAN.captures(line) {
            ran += cap[1].parse::<usize>().unwrap_or(0);
            summaries += 1;
            totals.pass += pending.pass;
            totals.fail += pending.fail;
            totals.skip += pending.skip;
            totals.errors += pending.errors;
            pending = Tally::default();
            in_recap = false;
            file = None;
            continue;
        }
        if let Some(cap) = BUN_TALLY.captures(line) {
            let n = cap[1].parse::<usize>().unwrap_or(0);
            match &cap[2] {
                "pass" => pending.pass += n,
                "fail" => pending.fail += n,
                "skip" | "todo" => pending.skip += n,
                _ => pending.errors += n,
            }
            continue;
        }
        if line.trim().is_empty() || line.ends_with("expect() calls") || line.ends_with("snapshots")
        {
            continue;
        }
        // Tallies are only the lines directly above `Ran …`.
        pending = Tally::default();
        if BUN_RECAP.is_match(line) {
            in_recap = true;
            continue;
        }
        if in_recap {
            continue;
        }
        if let Some(cap) = BUN_ROW.captures(line) {
            let status = match cap.get(1).or(cap.get(2)).map(|m| m.as_str()) {
                Some("pass" | "✓") => "passed",
                Some("fail" | "✗") => "failed",
                _ => "skipped",
            };
            let id = match &file {
                Some(file) => format!("{file} > {}", &cap[3]),
                None => cap[3].to_string(),
            };
            let duration = millis(
                cap.get(4),
                cap.get(5).is_some_and(|unit| unit.as_str() == "s"),
            );
            let mut row = test_row(id.clone(), status, duration);
            if status == "failed" {
                // Bun prints a failing test's error ABOVE its row.
                let message = last_error.take();
                row.failure_message = message.clone();
                failures.push(CheckFailure { name: id, message });
            }
            last_error = None;
            tests.push(row);
            continue;
        }
        if let Some(cap) = BUN_FILE.captures(line) {
            file = Some(cap[1].to_string());
            last_error = None;
            continue;
        }
        if last_error.is_none() {
            if let Some(error) = line.trim_start().strip_prefix("error:") {
                last_error = Some(cap_chars(error.trim(), MESSAGE_CHARS));
            }
        }
    }
    if summaries == 0 {
        return None;
    }
    let complete = rows_account_for(&tests, ran, totals.pass, totals.fail, totals.skip);
    Some(ParsedCheckResult {
        schema_version: 1,
        complete,
        selection: if ran == 0 { "empty" } else { "unknown" }.to_string(),
        tests: if complete { tests } else { vec![] },
        undeclared_skips: 0,
        parser: BUN_TEST_PARSER.to_string(),
        passed: totals.pass,
        failed: totals.fail,
        skipped: totals.skip,
        suite_failures: totals.errors,
        deferred: 0,
        report: None,
        phases: None,
        failures,
        unverified: None,
    })
}

/// Whether per-test rows read from printed output account for exactly the tests
/// the runner's own summary counts, status by status. Only then are the rows an
/// enumeration of the selection rather than whatever survived the capture.
fn rows_account_for(
    tests: &[CheckTestResult],
    total: usize,
    passed: usize,
    failed: usize,
    skipped: usize,
) -> bool {
    let count = |status: &str| tests.iter().filter(|test| test.status == status).count();
    total == passed + failed + skipped
        && tests.len() == total
        && count("passed") == passed
        && count("failed") == failed
        && count("skipped") == skipped
}

/// A printed duration in milliseconds (`seconds` when the unit was `s`).
fn millis(value: Option<regex::Match<'_>>, seconds: bool) -> Option<u64> {
    let value: f64 = value?.as_str().parse().ok()?;
    Some((if seconds { value * 1000.0 } else { value }).round() as u64)
}

fn test_row(id: String, status: &str, duration_ms: Option<u64>) -> CheckTestResult {
    CheckTestResult {
        id,
        status: status.to_string(),
        duration_ms,
        attempts: 1,
        retried: false,
        flaky: false,
        failure_message: None,
        skip_reason: None,
        skip_declaration: None,
        skip_declaration_source: None,
    }
}

/// The deepest directory shared by every path in `paths`, without a trailing
/// slash, or `""` when they share none. Each path's own file name is excluded, so
/// a single path yields its parent directory and reduces to a bare basename.
fn common_dir_prefix<'a>(paths: impl Iterator<Item = &'a str>) -> String {
    let mut common: Option<Vec<&str>> = None;
    for path in paths {
        let mut segments: Vec<&str> = path.split('/').collect();
        segments.pop();
        common = Some(match common {
            None => segments,
            Some(previous) => previous
                .into_iter()
                .zip(segments)
                .take_while(|(a, b)| a == b)
                .map(|(a, _)| a)
                .collect(),
        });
    }
    common
        .map(|segments| segments.join("/"))
        .unwrap_or_default()
}

/// Rewrite every occurrence of `root/` in `s` away, so both a suite's path and
/// the paths quoted inside its error message read relative to the run root.
fn strip_root(s: &str, root: &str) -> String {
    if root.is_empty() {
        return s.to_string();
    }
    s.replace(&format!("{root}/"), "")
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
        schema_version: 1,
        complete: false,
        selection: "unknown".to_string(),
        tests: vec![],
        undeclared_skips: 0,
        parser: "nextest".to_string(),
        passed: grab(&COUNT_PASSED),
        failed: grab(&COUNT_FAILED),
        skipped: grab(&COUNT_SKIPPED),
        suite_failures: 0,
        phases: None,
        deferred: 0,
        report: None,
        failures,
        unverified: None,
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
        schema_version: 1,
        complete: false,
        selection: "unknown".to_string(),
        tests: vec![],
        undeclared_skips: 0,
        // `nextest` is the persisted label for the whole Rust test-runner family,
        // including the wrapper's cargo-test fallback.
        parser: "nextest".to_string(),
        passed,
        failed,
        skipped: ignored,
        suite_failures: 0,
        phases: None,
        deferred: 0,
        report: None,
        failures,
        unverified: None,
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
        // A check's own report already orders its verdicts ahead of their sites;
        // re-deriving one from failure messages would lose its headers and its
        // pre-existing count, and would list sites in whatever order they came.
        if let Some(report) = p.report.as_deref().filter(|r| !r.trim().is_empty()) {
            return report_excerpt(report, raw_tail, FAILURE_EXCERPT_CHARS);
        }
        let composed: Vec<String> = p
            .failures
            .iter()
            .filter_map(|f| f.message.as_ref().map(|m| format!("{}: {m}", f.name)))
            .collect();
        if !composed.is_empty() {
            return head_chars(&composed.join("\n"), FAILURE_EXCERPT_CHARS);
        }
    }
    anchored_tail(raw_tail, FAILURE_EXCERPT_CHARS)
}

/// A check's `report` as a renderer shows it: led by whatever the stored
/// excerpt carries ahead of it (a failure classification and its evidence) and
/// cut before the `Final output:` chatter that follows it. The report leads
/// with its verdicts, so its head is what survives `max`. Falls back to the
/// report alone when the stored excerpt does not carry it (a row written by
/// another path).
pub(crate) fn report_excerpt(report: &str, stored_tail: &str, max: usize) -> String {
    let report = report.trim_end();
    let lead = stored_tail
        .find(report)
        .map_or(report, |at| &stored_tail[..at + report.len()]);
    head_chars(lead.trim_end(), max)
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
pub(crate) fn tail_chars(s: &str, max: usize) -> String {
    let n = s.chars().count();
    if n <= max {
        return s.to_string();
    }
    s.chars().skip(n - max).collect()
}

/// What stands between an anchored error and the tail it leads.
const ANCHOR_ELISION: &str = "\n…\n";

/// The trailing `max` chars of `s`, led by the lines a reader needs most when
/// the cut would drop them: the first build error and nextest's `Summary`
/// lines.
///
/// A build prints its error once and then however many warnings and footers
/// follow, so an end-anchored window alone can keep `could not compile … due
/// to 1 previous error` while losing the one error it counts, and a reader
/// takes the last warning for the cause. A fast-tier pass prints its Summary
/// and then one `TIMEOUT` line per deferred test, thousands of them, so the
/// window keeps the deferrals and loses the one line saying how many tests ran
/// and for how long.
///
/// Each anchor the window misses (the first `error[`/`error:` line with its
/// `-->` location, and every `Summary [` line) leads verbatim in output order,
/// within half of `max`, then an elision mark, then the end-anchored remainder.
pub(crate) fn anchored_tail(s: &str, max: usize) -> String {
    let total = s.chars().count();
    if total <= max {
        return s.to_string();
    }
    let window = s
        .char_indices()
        .nth(total - max)
        .map_or(s.len(), |(at, _)| at);
    let mut anchors: Vec<(usize, usize)> = first_error_span(s)
        .into_iter()
        .chain(summary_spans(s))
        .filter(|(start, _)| *start < window)
        .collect();
    if anchors.is_empty() {
        return tail_chars(s, max);
    }
    anchors.sort_unstable();
    let lead_budget = max / 2;
    let mut lead = String::new();
    let mut lead_end = 0;
    for (start, end) in anchors {
        if start < lead_end {
            continue;
        }
        let piece = s[start..end].trim_end();
        if lead.is_empty() {
            lead = head_chars(piece, lead_budget);
        } else {
            let joint = if start == lead_end {
                "\n"
            } else {
                ANCHOR_ELISION
            };
            if lead.chars().count() + joint.chars().count() + piece.chars().count() > lead_budget {
                break;
            }
            lead.push_str(joint);
            lead.push_str(piece);
        }
        lead_end = end;
    }
    let budget = max.saturating_sub(lead.chars().count() + ANCHOR_ELISION.chars().count());
    format!(
        "{lead}{ANCHOR_ELISION}{}",
        tail_chars(&s[lead_end..], budget)
    )
}

static NEXTEST_SUMMARY_LINE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s*Summary\s+\[\s*[\d.]+s\]").unwrap());

/// The byte spans of every nextest `Summary [ Xs ]` line in `s`.
fn summary_spans(s: &str) -> Vec<(usize, usize)> {
    let mut offset = 0;
    s.split_inclusive('\n')
        .filter_map(|line| {
            let at = offset;
            offset += line.len();
            NEXTEST_SUMMARY_LINE
                .is_match(&strip_ansi(line))
                .then_some((at, at + line.len()))
        })
        .collect()
}

/// The byte span of `s`'s first error line, through its `-->` location when
/// one follows within two lines.
fn first_error_span(s: &str) -> Option<(usize, usize)> {
    let mut offset = 0;
    let mut lines = s.split_inclusive('\n').map(|line| {
        let at = offset;
        offset += line.len();
        (at, line)
    });
    let (start, first) = lines.find(|(_, line)| {
        let line = strip_ansi(line);
        let line = line.trim_start();
        line.starts_with("error[") || line.starts_with("error:")
    })?;
    let mut end = start + first.len();
    for (at, line) in lines.take(2) {
        if strip_ansi(line).trim_start().starts_with("-->") {
            end = at + line.len();
            break;
        }
    }
    Some((start, end))
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
        assert_eq!(
            detect_parser("cd api && bun test --isolate"),
            Some(ParserKind::BunTest)
        );
        assert_eq!(detect_parser("bun run test:frontend:scoped"), None);
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

    /// A faithful reduction of the JSON Vitest emits when one file fails to
    /// COLLECT: `numFailedTestSuites` counts it, `numFailedTests` does not, and
    /// the reason lives on the file entry rather than on any assertion. Paths are
    /// absolute and rooted in the ephemeral build slot the check ran in.
    const VITEST_SUITE_FAILURE_FIXTURE: &str = concat!(
        "{\"numTotalTestSuites\":2,\"numFailedTestSuites\":1,\"numPassedTests\":881,",
        "\"numFailedTests\":0,\"numPendingTests\":0,\"numTodoTests\":0,\"success\":false,",
        "\"testResults\":[",
        "{\"name\":\"/slots/slot-178/src/components/FileTabView.test.tsx\",",
        "\"status\":\"failed\",\"assertionResults\":[],",
        "\"message\":\"Error: Cannot find module '../../packages/ui/src/lib/readableMarkdown' ",
        "imported from '/slots/slot-178/src/components/FileTabView.test.tsx'\\n  at load\"},",
        "{\"name\":\"/slots/slot-178/packages/ui/src/types.test.ts\",",
        "\"status\":\"passed\",\"assertionResults\":[",
        "{\"status\":\"passed\",\"fullName\":\"STATUS_COLORS maps complete\"}]}]}"
    );

    #[test]
    fn vitest_counts_a_suite_that_failed_to_collect() {
        let r = parse_check_output(
            "bunx vitest run --reporter=json",
            VITEST_SUITE_FAILURE_FIXTURE,
        )
        .unwrap();
        // The run failed, but not one assertion did: without the suite tally this
        // parse is indistinguishable from a clean pass.
        assert_eq!(r.failed, 0);
        assert_eq!(r.suite_failures, 1);
        assert_eq!(r.failures.len(), 1);
        // The build-slot root is stripped, so the name is the repo-relative path
        // rather than a path into a directory that no longer exists.
        assert_eq!(r.failures[0].name, "src/components/FileTabView.test.tsx");
        let message = r.failures[0].message.as_deref().unwrap();
        assert!(
            message.contains("Cannot find module '../../packages/ui/src/lib/readableMarkdown'"),
            "the collection error is the whole explanation: {message}"
        );
        assert!(
            message.contains("imported from 'src/components/FileTabView.test.tsx'"),
            "paths quoted inside the message are stripped too: {message}"
        );
    }

    #[test]
    fn vitest_leaves_suite_failures_at_zero_when_every_file_collected() {
        let r = parse_check_output("bunx vitest run --reporter=json", VITEST_FIXTURE).unwrap();
        assert_eq!(r.suite_failures, 0);
    }

    #[test]
    fn common_dir_prefix_reduces_a_lone_path_to_its_basename() {
        // A narrow `vitest related` selection can report a single file, where the
        // deepest shared directory is that file's own parent.
        assert_eq!(
            common_dir_prefix(["/slots/slot-1/src/a.test.ts"].into_iter()),
            "/slots/slot-1/src"
        );
        assert_eq!(
            common_dir_prefix(
                [
                    "/slots/slot-1/src/a.test.ts",
                    "/slots/slot-1/packages/b.test.ts"
                ]
                .into_iter()
            ),
            "/slots/slot-1"
        );
        assert_eq!(common_dir_prefix([].into_iter()), "");
        // Nothing shared: leave the paths alone rather than mangling them.
        assert_eq!(
            common_dir_prefix(["/a/x.test.ts", "/b/y.test.ts"].into_iter()),
            ""
        );
    }

    #[test]
    fn vitest_without_json_blob_degrades_to_none() {
        // A vitest command whose output carries no JSON object (reporter absent)
        // parses to nothing — the caller degrades to the raw tail, never a pass.
        assert!(parse_check_output("bunx vitest run", " FAIL  a.test.ts\n").is_none());
    }

    /// The bytes `scripts/test-api.ts` writes for an off-pin green, pinned by
    /// `scripts/lib/bun-runtime-pin.test.ts` on the other side.
    const OFF_PIN_DECLARATION: &str = r#"{"schemaVersion":1,"parser":"bun-test","unverified":"runtime bun 1.3.13 ≠ pinned 1.4.4"}"#;

    #[test]
    fn a_declared_unverified_result_stands_alone_for_an_unparsed_runner() {
        let json =
            r#"{"schemaVersion":1,"parser":"custom-runner","unverified":"ran on the wrong host"}"#;
        let output = format!("custom runner finished\n{RESULT_ARTIFACT_JSON_MARKER}{json}\n");
        let parsed = parse_check_output("bun run check:custom", &output).unwrap();
        assert_eq!(parsed.parser, "custom-runner");
        assert_eq!(parsed.unverified.as_deref(), Some("ran on the wrong host"));
        assert!(!parsed.is_test_runner(), "no test counts are invented");
        assert_eq!((parsed.passed, parsed.failed, parsed.skipped), (0, 0, 0));
        let stored = serde_json::to_string(&parsed).unwrap();
        let restored: ParsedCheckResult = serde_json::from_str(&stored).unwrap();
        assert_eq!(restored, parsed, "the declaration survives storage");
    }

    #[test]
    fn a_declared_unverified_result_keeps_the_api_lanes_bun_counts() {
        // The tail of an off-pin `bun run test:api` green: bun's own summary,
        // then the declaration the wrapper wrote, which has no per-test rows.
        let output = format!(
            " 801 pass\n 24 skip\n 0 fail\n 2408 expect() calls\nRan 825 tests across 58 files. [5.99s]\n{RESULT_ARTIFACT_JSON_MARKER}{OFF_PIN_DECLARATION}\n"
        );
        let parsed = parse_check_output("bun run test:api", &output).unwrap();
        assert_eq!(
            parsed.parser, BUN_TEST_PARSER,
            "bun's summary is still read"
        );
        assert_eq!((parsed.passed, parsed.skipped), (801, 24));
        assert_eq!(
            parsed.unverified.as_deref(),
            Some("runtime bun 1.3.13 ≠ pinned 1.4.4")
        );
    }

    #[test]
    fn a_declared_unverified_result_rides_on_the_winning_parse() {
        let json = r#"{"schemaVersion":1,"parser":"nextest","complete":true,"selection":"full","undeclaredSkips":0,"tests":[{"id":"c t::ok","status":"passed","attempts":1,"retried":false,"flaky":false}],"unverified":"runtime differs"}"#;
        let output = format!("{RESULT_ARTIFACT_JSON_MARKER}{json}\n");
        let parsed = parse_check_output("bun run test:rust", &output).unwrap();
        assert_eq!((parsed.parser.as_str(), parsed.passed), ("nextest", 1));
        assert_eq!(parsed.unverified.as_deref(), Some("runtime differs"));
    }

    #[test]
    fn a_blank_or_absent_declaration_is_no_declaration() {
        for json in [
            r#"{"schemaVersion":1,"parser":"bun-test","unverified":"  "}"#,
            r#"{"schemaVersion":1,"parser":"bun-test"}"#,
        ] {
            let output = format!("{RESULT_ARTIFACT_JSON_MARKER}{json}\n");
            assert_eq!(parse_check_output("bun run test:api", &output), None);
        }
    }

    #[test]
    fn parses_versioned_nextest_artifact_as_canonical_per_test_result() {
        let json = r#"{"schemaVersion":1,"parser":"nextest","complete":true,"selection":"full","undeclaredSkips":1,"tests":[{"id":"crate test::passes","status":"passed","durationMs":12,"attempts":1,"retried":false,"flaky":false},{"id":"crate test::flaky","status":"passed","attempts":2,"retried":true,"flaky":true},{"id":"crate test::fails","status":"failed","attempts":1,"retried":false,"flaky":false,"failureMessage":"panic: expected true"},{"id":"crate test::skip","status":"skipped","attempts":1,"retried":false,"flaky":false,"skipReason":"worktree-fence","skipDeclaration":"undeclared"}]}"#;
        let output = format!("human output\n{RESULT_ARTIFACT_JSON_MARKER}{json}\n");
        let parsed = parse_check_output("bun run test:rust", &output).unwrap();
        assert_eq!(parsed.schema_version, 1);
        assert!(parsed.complete);
        assert_eq!(parsed.selection, "full");
        assert_eq!((parsed.passed, parsed.failed, parsed.skipped), (2, 1, 1));
        assert_eq!(parsed.undeclared_skips, 1);
        assert_eq!(parsed.tests.len(), 4);
        assert!(parsed.tests[1].flaky);
        assert_eq!(parsed.failures[0].name, "crate test::fails");
        assert_eq!(
            parsed.failures[0].message.as_deref(),
            Some("panic: expected true")
        );
    }

    /// A fast-tier artifact, shaped as `scripts/test-rust-nextest.ts tiered`
    /// writes it: one related test run in full, one budget-sized pass, one
    /// genuine failure that happened before the budget, and two deferrals.
    const FAST_TIER_ARTIFACT: &str = r#"{"schemaVersion":1,"parser":"nextest","complete":true,"selection":"fast","undeclaredSkips":0,"tests":[{"id":"c::c$changed::slow","status":"passed","durationMs":5200,"attempts":1,"retried":false,"flaky":false},{"id":"c::c$other::quick","status":"passed","durationMs":3,"attempts":1,"retried":false,"flaky":false},{"id":"c::c$other::breaks","status":"failed","durationMs":4,"attempts":1,"retried":false,"flaky":false,"failureMessage":"boom"},{"id":"c::c$other::slow_a","status":"deferred","durationMs":1001,"attempts":1,"retried":false,"flaky":false},{"id":"c::c$other::slow_b","status":"deferred","durationMs":1002,"attempts":1,"retried":false,"flaky":false}]}"#;

    #[test]
    fn a_deferred_test_is_never_counted_as_passed() {
        let output = format!("{RESULT_ARTIFACT_JSON_MARKER}{FAST_TIER_ARTIFACT}\n");
        let parsed = parse_check_output("bun run test:rust:tiered", &output).unwrap();
        assert!(parsed.is_fast_tier());
        assert!(
            parsed.complete,
            "a deferral is a terminal row; the stream was whole"
        );
        assert_eq!(
            (
                parsed.passed,
                parsed.failed,
                parsed.skipped,
                parsed.deferred
            ),
            (2, 1, 0, 2)
        );
        assert_eq!(
            parsed.tests_run(),
            3,
            "deferred tests never reached a verdict"
        );
        // A genuine failure before the budget is still the failure it was, and
        // a deferral is never named as one.
        let failing: Vec<_> = parsed.failures.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(failing, vec!["c::c$other::breaks"]);
        assert_eq!(
            parsed.fast_tier_label().as_deref(),
            Some("fast tier: 3 ran, 2 deferred")
        );
    }

    #[test]
    fn only_a_fast_selection_carries_a_tier_label() {
        for selection in ["full", "filtered", "empty", "unknown"] {
            let json = FAST_TIER_ARTIFACT.replace(
                "\"selection\":\"fast\"",
                &format!("\"selection\":\"{selection}\""),
            );
            let output = format!("{RESULT_ARTIFACT_JSON_MARKER}{json}\n");
            let parsed = parse_check_output("bun run test:rust", &output).unwrap();
            assert_eq!(parsed.fast_tier_label(), None, "{selection}");
        }
        assert_eq!(
            fast_tier_label(10, 0, 2),
            "fast tier: 10 ran, 0 deferred, 2 skipped"
        );
    }

    #[test]
    fn parses_transferred_nextest_artifact_without_executor_filesystem_access() {
        let json = r#"{"schemaVersion":1,"parser":"nextest","complete":true,"selection":"filtered","undeclaredSkips":0,"tests":[{"id":"crate test::remote","status":"passed","attempts":1,"retried":false,"flaky":false}]}"#;
        let output = format!("{RESULT_ARTIFACT_JSON_MARKER}{json}\n");
        let parsed = parse_check_output("bun run test:rust", &output).unwrap();
        assert!(parsed.complete);
        assert_eq!(parsed.selection, "filtered");
        assert_eq!(parsed.tests[0].id, "crate test::remote");
        assert_eq!(parsed.passed, 1);
    }

    #[test]
    fn malformed_transferred_artifact_fails_closed_without_reading_path() {
        let output = format!(
            "CAIRN_CHECK_RESULT_ARTIFACT=/etc/hosts\n{RESULT_ARTIFACT_JSON_MARKER}{{not-json}}\n"
        );
        assert!(parse_structured_artifact(&output).is_none());
        assert!(!declared_empty_selection(&output));
    }

    #[test]
    fn an_empty_selection_is_declared_by_any_runner_that_writes_the_marker() {
        for parser in ["nextest", "vitest"] {
            let json = format!(
                r#"{{"schemaVersion":1,"parser":"{parser}","complete":true,"selection":"empty","tests":[],"undeclaredSkips":0}}"#
            );
            let output = format!(
                "ℹ️  nothing to run.\n{RESULT_ARTIFACT_JSON_MARKER}{json}\n\nstderr:\nnoise"
            );
            assert!(declared_empty_selection(&output), "{parser}");
        }
    }

    #[test]
    fn only_an_empty_selection_declares_one() {
        for selection in ["full", "filtered", "unknown"] {
            let json = format!(
                r#"{{"schemaVersion":1,"parser":"nextest","complete":true,"selection":"{selection}","tests":[],"undeclaredSkips":0}}"#
            );
            let output = format!("{RESULT_ARTIFACT_JSON_MARKER}{json}\n");
            assert!(!declared_empty_selection(&output), "{selection}");
        }
        // No marker, a different schema, or an unparseable body: none of them is
        // a statement that nothing ran.
        assert!(!declared_empty_selection("ℹ️  nothing to run.\n"));
        assert!(!declared_empty_selection(&format!(
            r#"{RESULT_ARTIFACT_JSON_MARKER}{{"schemaVersion":2,"selection":"empty"}}"#
        )));
        assert!(!declared_empty_selection(&format!(
            "{RESULT_ARTIFACT_JSON_MARKER}{{\"selection\":\"empty\""
        )));
    }

    /// The executor appends its marker after the child exits, so a command that
    /// prints an empty-selection line of its own earlier in its output does not
    /// speak for the run.
    #[test]
    fn the_executors_trailing_marker_is_the_one_that_counts() {
        let output = format!(
            r#"{RESULT_ARTIFACT_JSON_MARKER}{{"schemaVersion":1,"selection":"empty"}}
real work
{RESULT_ARTIFACT_JSON_MARKER}{{"schemaVersion":1,"selection":"full"}}"#
        );
        assert!(!declared_empty_selection(&output));
    }

    /// The builder in scripts/lib/rust-lint-result.ts must produce exactly this
    /// file for its specimen inputs (its test asserts so), and this side parses
    /// it, so neither half of the contract can drift alone.
    const RUST_LINT_FIXTURE: &str = include_str!("fixtures/rust_lint_result.json");

    fn result_line(json: &str) -> String {
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        format!("{RESULT_ARTIFACT_JSON_MARKER}{value}")
    }

    #[test]
    fn a_rust_lint_result_parses_whatever_the_command_is_spelled_like() {
        assert_eq!(detect_parser("bun run check:rust"), None);
        let output = format!(
            "    Checking cairn-core\nExit code: 1\n{}\n",
            result_line(RUST_LINT_FIXTURE)
        );
        let parsed = parse_check_output("bun run check:rust", &output).unwrap();
        assert_eq!(parsed.parser, RUST_LINT_PARSER);
        assert!(parsed.complete);
        assert!(!parsed.is_test_runner());
        assert!(!parsed.is_incomplete_lint());
        assert_eq!((parsed.passed, parsed.failed, parsed.skipped), (0, 2, 0));
        let names: Vec<&str> = parsed.failures.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "src-tauri/os/cairn-core/src/execution/checks_turn_end.rs:2811 clippy::cloned_ref_to_slice_refs",
                "src-tauri/os/cairn-core/src/execution/checks_turn_end.rs:2805 rustfmt",
            ]
        );
        assert_eq!(
            parsed.failures[0].message.as_deref(),
            Some("this call to `clone` can be replaced with `std::slice::from_ref`")
        );
        let report = parsed.report.as_deref().unwrap();
        assert!(report.starts_with("❌ clippy: 1 new finding(s) in your diff"));
        assert!(report.contains("❌ rustfmt: formatting drift in 1 file(s)"));
        assert!(report.contains("15 pre-existing finding(s)"));
    }

    #[test]
    fn a_report_excerpt_keeps_its_lead_and_drops_the_chatter_after_it() {
        let report = "❌ clippy: 1 new finding(s) in your diff\n  b.rs:2  clippy::x";
        let stored = format!("infra reason\nEvidence:\nline\n\n{report}\n\nFinal output:\nchatter");
        assert_eq!(
            report_excerpt(report, &stored, 1_500),
            format!("infra reason\nEvidence:\nline\n\n{report}")
        );
        assert_eq!(report_excerpt(report, "unrelated tail", 1_500), report);
        assert_eq!(report_excerpt(report, &stored, 12), "infra reason");

        let mut parsed =
            parse_check_output("bun run check:rust", &result_line(RUST_LINT_FIXTURE)).unwrap();
        let excerpt = format_failure_excerpt(Some(&parsed), "chatter only");
        assert!(
            excerpt.starts_with("❌ clippy: 1 new finding(s) in your diff"),
            "{excerpt}"
        );
        assert!(excerpt.contains("15 pre-existing"), "{excerpt}");
        parsed.report = None;
        assert!(
            format_failure_excerpt(Some(&parsed), "chatter only").starts_with(
                "src-tauri/os/cairn-core/src/execution/checks_turn_end.rs:2811 clippy::"
            ),
            "without a report, the failure messages are the excerpt"
        );
    }

    #[test]
    fn a_rust_lint_result_that_cannot_vouch_for_completeness_is_incomplete() {
        for json in [
            r#"{"schemaVersion":1,"parser":"rust-lint","failures":[]}"#,
            r#"{"schemaVersion":1,"parser":"rust-lint","complete":"yes","failures":[]}"#,
            r#"{"schemaVersion":1,"parser":"rust-lint","complete":false,"failures":[]}"#,
        ] {
            let parsed = parse_check_output("bun run check:rust", &result_line(json)).unwrap();
            assert!(!parsed.complete, "{json}");
            assert!(parsed.is_incomplete_lint(), "{json}");
            assert!(parsed.report.is_none());
        }
    }

    #[test]
    fn rust_lint_messages_and_report_are_capped() {
        let json = serde_json::json!({
            "schemaVersion": 1,
            "parser": "rust-lint",
            "complete": true,
            "failures": [{ "name": "a.rs:1 clippy::x", "message": format!("{}\nsecond line", "m".repeat(500)) }],
            "report": "r".repeat(3_000),
        })
        .to_string();
        let parsed = parse_check_output("bun run check:rust", &result_line(&json)).unwrap();
        let message = parsed.failures[0].message.as_deref().unwrap();
        assert_eq!(message.chars().count(), MESSAGE_CHARS);
        assert!(!message.contains("second line"));
        assert_eq!(
            parsed.report.as_deref().unwrap().chars().count(),
            REPORT_CHARS
        );
    }

    #[test]
    fn an_unrecognized_result_file_falls_through_to_command_detection() {
        let jest = r#"{"schemaVersion":1,"parser":"jest","complete":true,"tests":[]}"#;
        let output = format!("{TSC_FIXTURE}\n{}", result_line(jest));
        let parsed = parse_check_output("bunx tsc --noEmit", &output).unwrap();
        assert_eq!(
            parsed.parser, "tsc",
            "a result from a parser this core does not know is not consulted"
        );
        assert_eq!(parsed.failed, 3);

        let unknown = r#"{"schemaVersion":1,"parser":"eslint","failures":[]}"#;
        assert!(parse_check_output("bun run check:rust", &result_line(unknown)).is_none());
        let future = r#"{"schemaVersion":2,"parser":"rust-lint","failures":[]}"#;
        assert!(parse_check_output("bun run check:rust", &result_line(future)).is_none());
        let malformed = format!("{RESULT_ARTIFACT_JSON_MARKER}{{not-json");
        assert!(parse_check_output("bun run check:rust", &malformed).is_none());
    }

    #[test]
    fn a_row_stored_before_reports_existed_still_deserializes() {
        let stored = r#"{"parser":"tsc","passed":0,"failed":0,"skipped":0,"failures":[]}"#;
        let parsed: ParsedCheckResult = serde_json::from_str(stored).unwrap();
        assert_eq!(parsed.report, None);
        assert!(
            !serde_json::to_string(&parsed).unwrap().contains("report"),
            "an absent report is not written back as null"
        );
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
            schema_version: 1,
            complete: false,
            selection: "unknown".to_string(),
            tests: vec![],
            undeclared_skips: 0,
            parser: "tsc".to_string(),
            passed: 0,
            failed: 8,
            skipped: 0,
            suite_failures: 0,
            phases: None,
            deferred: 0,
            report: None,
            unverified: None,
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
            schema_version: 1,
            complete: false,
            selection: "unknown".to_string(),
            tests: vec![],
            undeclared_skips: 0,
            parser: "nextest".to_string(),
            passed: 3,
            failed: 0,
            skipped: 0,
            suite_failures: 0,
            phases: None,
            deferred: 0,
            report: None,
            failures: vec![],
            unverified: None,
        };
        assert!(format_failure_names(&parsed).is_none());
    }

    /// Specimen 59dbccd5: cairn-core emitted one `error[E0425]` and 32
    /// warnings; the end-anchored tail kept the warnings and the `could not
    /// compile … due to 1 previous error` footer but not the error, and a
    /// builder read the last warning as the cause.
    #[test]
    fn a_truncated_tail_keeps_the_first_error_and_its_location() {
        let mut output = String::from(
            "   Compiling cairn-core v0.1.0\nerror[E0425]: cannot find value `turn_id` in this scope\n   --> os/cairn-core/src/execution/checks_main.rs:612:9\n    |\n612 |         turn_id,\n    |         ^^^^^^^ not found in this scope\n\n",
        );
        for n in 0..32 {
            output.push_str(&format!(
                "warning: unused variable: `scratch_{n}`\n   --> os/cairn-core/src/lib.rs:{n}:9\n    |\n    = note: `#[warn(unused_variables)]` on by default\n\n"
            ));
        }
        output.push_str(
            "error: could not compile `cairn-core` (lib) due to 1 previous error; 32 warnings emitted\n",
        );
        assert!(
            !tail_chars(&output, 1_500).contains("error[E0425]"),
            "the plain tail loses the error, which is the defect"
        );
        let excerpt = anchored_tail(&output, 1_500);
        assert!(excerpt.chars().count() <= 1_500, "{excerpt}");
        assert!(
            excerpt.starts_with(
                "error[E0425]: cannot find value `turn_id` in this scope\n   --> os/cairn-core/src/execution/checks_main.rs:612:9\n…\n"
            ),
            "{excerpt}"
        );
        assert!(
            excerpt.ends_with("due to 1 previous error; 32 warnings emitted\n"),
            "{excerpt}"
        );

        // An untruncated output, a window that already holds the error, and an
        // output with no error are plain tails.
        assert_eq!(anchored_tail(&output, 100_000), output);
        let late = format!("{}error[E0308]: mismatched types\n", "noise\n".repeat(400));
        assert_eq!(anchored_tail(&late, 300), tail_chars(&late, 300));
        let quiet = "warning: unused\n".repeat(400);
        assert_eq!(anchored_tail(&quiet, 300), tail_chars(&quiet, 300));
    }

    /// One TIMEOUT line per deferral, as nextest reprints them after a fast-tier
    /// pass's Summary (specimen c505b3fe deferred 1,029).
    fn timeout_lines(count: usize) -> String {
        (0..count)
            .map(|n| {
                format!(
                    "     TIMEOUT [   1.002s] ({n:>5}/13327) cairn-vcs jj::tests::deferred_{n}\n"
                )
            })
            .collect()
    }

    /// Every rust-tests fast-tier row on the ledger lost nextest's Summary:
    /// the global pass's deferral list fills the window behind it.
    #[test]
    fn a_deferral_heavy_tail_keeps_every_nextest_summary() {
        let output = format!(
            "ℹ️  fast tier, local pass (no budget): cargo nextest run -p cairn-core\n\nstderr:\n\
             \x1b[1;32m     Summary\x1b[0m [  63.900s] 412 tests run: 412 passed, 0 skipped\n\
             {}     Summary [   5.400s] 3 tests run: 3 passed, 0 skipped\n\
             {}     Summary [ 312.004s] 9811 tests run: 8782 passed, 1029 timed out, 41 skipped\n\
             {}error: test run failed\nerror: script \"test:rust:tiered\" exited with code 100\n\n\nExit code: 100",
            "        PASS [   0.004s] crate::passes\n".repeat(200),
            "        PASS [   0.004s] crate::passes\n".repeat(200),
            timeout_lines(1_029),
        );
        assert!(
            !tail_chars(&output, 4_000).contains("Summary"),
            "the plain tail loses every Summary, which is the defect"
        );
        let excerpt = anchored_tail(&output, 4_000);
        assert!(excerpt.chars().count() <= 4_000, "{excerpt}");
        assert!(
            excerpt.starts_with(
                "\x1b[1;32m     Summary\x1b[0m [  63.900s] 412 tests run: 412 passed, 0 skipped\n…\n     Summary [   5.400s] 3 tests run"
            ),
            "{excerpt}"
        );
        assert!(
            excerpt.contains("     Summary [ 312.004s] 9811 tests run: 8782 passed, 1029 timed out, 41 skipped\n…\n"),
            "{excerpt}"
        );
        assert!(excerpt.ends_with("Exit code: 100"), "{excerpt}");

        // A build error and a Summary both above the window lead in output order.
        let both = format!(
            "error[E0425]: cannot find value `x`\n   --> os/cairn-core/src/lib.rs:1:1\n{}     Summary [   1.000s] 1 test run: 1 passed\n{}",
            "noise\n".repeat(400),
            timeout_lines(100),
        );
        let excerpt = anchored_tail(&both, 2_000);
        assert!(
            excerpt.starts_with(
                "error[E0425]: cannot find value `x`\n   --> os/cairn-core/src/lib.rs:1:1\n…\n     Summary [   1.000s] 1 test run: 1 passed\n…\n"
            ),
            "{excerpt}"
        );
        assert!(excerpt.chars().count() <= 2_000);
    }

    /// c47874ea's Summary sits inside the window already, behind a short
    /// failure list, so its tail is the plain one.
    #[test]
    fn a_summary_inside_the_window_is_not_repeated() {
        let output = format!(
            "{}     Summary [ 144.635s] 13117 tests run: 13116 passed (2 slow), 1 failed, 46 skipped\n        FAIL [   2.148s] ( 4648/13117) cairn-core mcp::auth::tests::the_operator_credential_has_no_unexpected_exposure_sites\nerror: test run failed\nerror: script \"test:rust:full\" exited with code 100\n\n\nExit code: 100",
            "        PASS [   0.081s] (13090/13117) cairn-workflow transitions::outcome::tests::idle\n".repeat(300),
        );
        assert_eq!(anchored_tail(&output, 4_000), tail_chars(&output, 4_000));
    }

    #[test]
    fn lane_phases_parse_from_the_result_file_and_implausible_ones_are_dropped() {
        let with = |phases: &str| {
            format!(
                r#"{RESULT_ARTIFACT_JSON_MARKER}{{"schemaVersion":1,"parser":"nextest","complete":true,"selection":"full","undeclaredSkips":0,"tests":[],"phases":{phases}}}"#
            )
        };
        let phases = r#"{"origin":"executor","setupMs":12000,"compileMs":408000,"testMs":149735,"doctestMs":0,"publishMs":1479,"compiledUnits":1,"passes":[{"name":"full","wallMs":557735,"compileMs":408000,"testMs":149735,"compiledUnits":1,"summaryMs":144635}]}"#;
        let parsed = parse_check_output("bun run test:rust:full", &with(phases)).unwrap();
        let measured = parsed
            .phases
            .as_ref()
            .expect("the runner's phases are kept");
        assert_eq!(
            (
                measured.setup_ms,
                measured.compile_ms,
                measured.test_ms,
                measured.publish_ms
            ),
            (12_000, 408_000, 149_735, 1_479)
        );
        assert_eq!(measured.passes[0].summary_ms, Some(144_635));
        // They ride into the stored result and back.
        let stored = serde_json::to_string(&parsed).unwrap();
        let back: ParsedCheckResult = serde_json::from_str(&stored).unwrap();
        assert_eq!(back.phases, parsed.phases);

        for bad in [
            r#"{"origin":"guess","setupMs":1,"compileMs":1,"testMs":1,"publishMs":1}"#,
            r#"{"origin":"process","setupMs":-1,"compileMs":1,"testMs":1,"publishMs":1}"#,
            r#"{"origin":"process","setupMs":18446744073709551615,"compileMs":1,"testMs":1,"publishMs":1}"#,
            r#""twelve minutes""#,
        ] {
            let parsed = parse_check_output("bun run test:rust", &with(bad)).unwrap();
            assert_eq!(parsed.phases, None, "{bad}");
            assert!(parsed.complete, "the rest of the result still parses");
        }
    }

    #[test]
    fn excerpt_prefers_messages_then_raw_tail() {
        // vitest/tsc: composed name: message lines.
        let with_msgs = ParsedCheckResult {
            schema_version: 1,
            complete: false,
            selection: "unknown".to_string(),
            tests: vec![],
            undeclared_skips: 0,
            parser: "vitest".to_string(),
            passed: 0,
            failed: 1,
            skipped: 0,
            suite_failures: 0,
            phases: None,
            deferred: 0,
            report: None,
            failures: vec![CheckFailure {
                name: "suite test".to_string(),
                message: Some("AssertionError: boom".to_string()),
            }],
            unverified: None,
        };
        let e = format_failure_excerpt(Some(&with_msgs), "RAW TAIL");
        assert_eq!(e, "suite test: AssertionError: boom");

        // nextest (no messages): raw tail.
        let no_msgs = ParsedCheckResult {
            schema_version: 1,
            complete: false,
            selection: "unknown".to_string(),
            tests: vec![],
            undeclared_skips: 0,
            parser: "nextest".to_string(),
            passed: 0,
            failed: 1,
            skipped: 0,
            suite_failures: 0,
            phases: None,
            deferred: 0,
            report: None,
            failures: vec![CheckFailure {
                name: "crate test::x".to_string(),
                message: None,
            }],
            unverified: None,
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

    // `bun run test:api` as the executor composes it: bun writes its whole
    // report to stderr. Rows, a recap that repeats the skip without its file,
    // then the tallies and the `Ran` line.
    const BUN_PASS_FIXTURE: &str = "\
stderr:
$ cd api && bun test --isolate
bun test v1.3.13 (bf2e2cec)

src/routes/sync.test.ts:
{\"msg\":\"sync-enable seed failed\",\"teamId\":\"org1\",\"error\":\"Error: db down\"}
(pass) POST /sync-enable > seed failure (DB error) → 500 [1.00ms]
(pass) POST /object-grants > non-member is denied before minting [1.00ms]
(skip) teams sync broker ↔ real tursodb (/v2/pipeline) > serves a read-only SELECT

src/routes/relay-query.test.ts:
(pass) relay payload byte measurement compiles for PostgreSQL [84.00ms]

1 tests skipped:
(skip) teams sync broker ↔ real tursodb (/v2/pipeline) > serves a read-only SELECT

 3 pass
 1 skip
 0 fail
 9 expect() calls
Ran 4 tests across 2 files. [6.53s]
";

    // A red: the failing test's error precedes its row, an error thrown between
    // tests is tallied as `errors`, and the failed recap repeats the row.
    const BUN_RED_FIXTURE: &str = "\
stderr:
src/auth.test.ts:
(pass) bearer > accepts its own token [0.20ms]
1 | test(\"refuses another token\", () => {
2 |   expect(verify(other)).toBe(false);
                            ^
error: expect(received).toBe(expected)

Expected: false
Received: true

      at <anonymous> (/slot/api/src/auth.test.ts:2:25)
(fail) bearer > refuses another token [0.48ms]

# Unhandled error between tests
-------------------------------
error: socket closed
-------------------------------


1 tests failed:
(fail) bearer > refuses another token [0.48ms]

 1 pass
 1 fail
 1 error
 3 expect() calls
Ran 2 tests across 1 file. [1.20s]
error: script \"test:api\" exited with code 1

Exit code: 1";

    #[test]
    fn a_bun_test_run_is_a_complete_per_test_enumeration() {
        let parsed = parse_check_output("bun run test:api", BUN_PASS_FIXTURE).unwrap();
        assert_eq!(parsed.parser, BUN_TEST_PARSER);
        assert!(parsed.is_test_runner());
        assert!(parsed.complete, "every tallied test has its row");
        assert_eq!((parsed.passed, parsed.failed, parsed.skipped), (3, 0, 1));
        assert_eq!(parsed.suite_failures, 0);
        let ids: Vec<&str> = parsed.tests.iter().map(|test| test.id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "src/routes/sync.test.ts > POST /sync-enable > seed failure (DB error) → 500",
                "src/routes/sync.test.ts > POST /object-grants > non-member is denied before minting",
                "src/routes/sync.test.ts > teams sync broker ↔ real tursodb (/v2/pipeline) > serves a read-only SELECT",
                "src/routes/relay-query.test.ts > relay payload byte measurement compiles for PostgreSQL",
            ],
            "the recap's file-less repeat is not a second row"
        );
        assert_eq!(parsed.tests[3].duration_ms, Some(84));
        assert_eq!(
            crate::execution::checks_status::verdict_evidence(true, Some(&parsed)),
            crate::execution::checks_status::VerdictEvidence::Verified
        );
        // The command names `bun test` itself: the same parse.
        assert_eq!(
            parse_check_output("cd api && bun test --isolate", BUN_PASS_FIXTURE),
            Some(parsed)
        );
    }

    #[test]
    fn a_red_bun_run_names_its_failure_with_the_error_above_it() {
        let parsed = parse_check_output("bun run test:api", BUN_RED_FIXTURE).unwrap();
        assert!(parsed.complete);
        assert_eq!((parsed.passed, parsed.failed, parsed.skipped), (1, 1, 0));
        assert_eq!(parsed.suite_failures, 1, "an error between tests");
        assert_eq!(
            parsed.failures,
            vec![CheckFailure {
                name: "src/auth.test.ts > bearer > refuses another token".to_string(),
                message: Some("expect(received).toBe(expected)".to_string()),
            }]
        );
        assert_eq!(
            parsed.tests[1].failure_message.as_deref(),
            Some("expect(received).toBe(expected)")
        );
    }

    /// The stored tail of ledger observation 948e2911 (an `api` red): the
    /// tallies survived the capture but most rows did not, so the counts are
    /// read and the enumeration is not claimed.
    #[test]
    fn a_bun_tail_that_lost_its_rows_keeps_the_counts_but_not_completeness() {
        let tail = "rejects when membership is revoked [0.28ms]
(pass) POST /refresh > rejects missing Authorization header [0.07ms]
(pass) DELETE /device/:deviceId > deletes device and returns deactivated id [0.14ms]

17 tests skipped:
(skip) teams sync broker ↔ real tursodb (/v2/pipeline) > serves an arbitrary read-only SELECT through the broker
(skip) approval state machine on migrated PostgreSQL > concurrent claims consume one approval exactly once


8 tests failed:
(fail) bearer > refuses another token [0.48ms]

 575 pass
 17 skip
 8 fail
 10 errors
 1326 expect() calls
Ran 600 tests across 58 files. [5.72s]
error: script \"test:api\" exited with code 1


Exit code: 1";
        let parsed = parse_check_output("bun run test:api", tail).unwrap();
        assert_eq!(parsed.parser, BUN_TEST_PARSER);
        assert_eq!((parsed.passed, parsed.failed, parsed.skipped), (575, 8, 17));
        assert_eq!(parsed.suite_failures, 10);
        assert!(!parsed.complete);
        assert!(parsed.tests.is_empty(), "no partial enumeration is stored");
    }

    #[test]
    fn output_without_a_runner_summary_is_still_unparsed() {
        assert!(parse_check_output("bun run check:dead", "no dead code\n").is_none());
        assert!(parse_check_output("bun run test:api", " 3 pass\n 0 fail\n").is_none());
    }

    #[test]
    fn a_vitest_result_file_is_a_per_test_result_for_any_command() {
        let json = r#"{"schemaVersion":1,"parser":"vitest","complete":true,"selection":"filtered","undeclaredSkips":0,"tests":[{"id":"src/a.test.ts > a > works","status":"passed","durationMs":3,"attempts":1,"retried":false,"flaky":false},{"id":"src/a.test.ts > a > breaks","status":"failed","attempts":1,"retried":false,"flaky":false,"failureMessage":"AssertionError: expected 1 to be 2\n    at a.test.ts:4"}]}"#;
        let output = format!("ok\n\nstderr:\nnoise\n{}", result_line(json));
        let parsed = parse_check_output("bun run test:frontend:scoped", &output).unwrap();
        assert_eq!(parsed.parser, "vitest");
        assert!(parsed.complete);
        assert_eq!(parsed.selection, "filtered");
        assert_eq!((parsed.passed, parsed.failed), (1, 1));
        assert_eq!(
            parsed.failures,
            vec![CheckFailure {
                name: "src/a.test.ts > a > breaks".to_string(),
                message: Some("AssertionError: expected 1 to be 2".to_string()),
            }]
        );
    }

    // Two vitest processes' verbose reports, as a sharded run prints them.
    const VITEST_VERBOSE_FIXTURE: &str = "\
 RUN  v4.0.17 /slot

 ✓ scripts/lib/a.test.ts > scan > reads an uncolored FAIL line 0ms
 × scripts/lib/a.test.ts > scan > breaks 2ms
   → expected 1 to be 2
 ↓ scripts/lib/a.test.ts > scan > later

 Test Files  1 failed (1)
      Tests  1 failed | 1 passed | 1 skipped (3)
   Start at  14:20:34
   Duration  569ms (transform 35ms, setup 167ms)

 ✓ src/b.test.ts > b > works 1ms

 Test Files  1 passed (1)
      Tests  1 passed (1)
";

    #[test]
    fn a_verbose_vitest_report_without_json_is_a_complete_enumeration() {
        let parsed = parse_check_output("bunx vitest run", VITEST_VERBOSE_FIXTURE).unwrap();
        assert_eq!(parsed.parser, "vitest");
        assert!(parsed.complete);
        assert_eq!((parsed.passed, parsed.failed, parsed.skipped), (2, 1, 1));
        assert_eq!(parsed.tests.len(), 4);
        assert_eq!(
            parsed.failures,
            vec![CheckFailure {
                name: "scripts/lib/a.test.ts > scan > breaks".to_string(),
                message: Some("expected 1 to be 2".to_string()),
            }]
        );
        // A package script that runs vitest is read from the same summary.
        assert_eq!(
            parse_check_output("npm test", VITEST_VERBOSE_FIXTURE),
            Some(parsed)
        );
    }

    #[test]
    fn the_default_vitest_reporter_yields_counts_but_no_enumeration() {
        let output = " ✓ src/b.test.ts (11 tests) 5ms\n\n Test Files  1 passed (1)\n      Tests  11 passed (11)\n";
        let parsed = parse_check_output("bunx vitest run", output).unwrap();
        assert_eq!(parsed.passed, 11);
        assert!(!parsed.complete, "a per-file line is not a test row");
        assert!(parsed.tests.is_empty());
    }

    #[test]
    fn every_test_runner_parser_is_one_list() {
        for parser in ["nextest", "vitest", BUN_TEST_PARSER] {
            assert!(is_test_runner_parser(parser), "{parser}");
        }
        for parser in ["tsc", RUST_LINT_PARSER, "jest"] {
            assert!(!is_test_runner_parser(parser), "{parser}");
        }
        assert!(is_test_runner_command("cd api && bun test"));
        assert!(is_test_runner_command("bun run test:rust"));
        assert!(!is_test_runner_command("bunx tsc --noEmit"));
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
