//! Evidence accounting: whether a test lane's recorded result accounts for
//! everything its runner set out to run, link by link, from the expected
//! inventory and the runner's own tally to the rows the store holds.
//!
//! The producer (`scripts/lib/structured-test-result.ts`) writes what only it
//! can see: started identities, terminal events, the runner's independent
//! tally, and every named transfer between them. Ingestion
//! ([`crate::execution::check_parsers`]) adds what it received and kept. The
//! store supplies what it holds. [`reconcile_evidence`] is the one
//! classification of those facts; every surface that shows it (the
//! observation read, a lane's `NodeCheckStatus`) renders this result instead
//! of re-deriving its own. See docs/checks.md, "Evidence accounting".

use serde::{Deserialize, Serialize};

use crate::execution::cache::ObservationTestCounts;
use crate::execution::check_parsers::ParsedCheckResult;
use crate::execution::checks::CheckFailureKind;

/// Identities named per population. The count beside them is always whole.
pub(crate) const NAMED_IDS: usize = 10;
/// A named identity or label is the command's input; it is capped like any excerpt.
const NAME_CHARS: usize = 300;

/// Rows by final status.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct StatusCounts {
    pub(crate) passed: usize,
    pub(crate) failed: usize,
    pub(crate) skipped: usize,
    pub(crate) deferred: usize,
}

impl StatusCounts {
    pub(crate) fn total(&self) -> usize {
        self.passed + self.failed + self.skipped + self.deferred
    }
}

/// The runner's own tally, read by the producer from a reporter independent of
/// the per-test events (nextest's `Summary` line, vitest's report totals).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct RunnerTotals {
    pub(crate) source: String,
    pub(crate) total: usize,
    pub(crate) passed: usize,
    pub(crate) failed: usize,
    pub(crate) skipped: usize,
    pub(crate) timed_out: usize,
}

/// One runner pass of a lane. Identities, not attempts, are counted.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PassAccounting {
    pub(crate) pass: String,
    pub(crate) processes: usize,
    /// `None` when the runner announces no starts (vitest's JSON report).
    pub(crate) started: Option<usize>,
    pub(crate) enumerated: StatusCounts,
    pub(crate) unresolved: usize,
    pub(crate) unresolved_ids: Vec<String>,
    pub(crate) runner: Option<RunnerTotals>,
    pub(crate) runner_reconciled: Option<bool>,
    pub(crate) filtered_skips: usize,
    pub(crate) skip_witnesses: usize,
    pub(crate) terminal_without_start: usize,
    pub(crate) retried: usize,
    pub(crate) collection_failures: usize,
    pub(crate) stderr_only_failures: usize,
}

/// The population expected before the runner ran.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub(crate) enum Inventory {
    /// No list was taken before the run, and why.
    Unavailable { reason: String },
    /// A list was taken: `count` units of `unit`, `unreported` of which no
    /// runner report mentions. A listed count of zero is an empty inventory,
    /// which is a different fact from an unavailable one.
    Listed {
        unit: String,
        count: usize,
        #[serde(default)]
        unreported: usize,
        #[serde(default, rename = "unreportedIds")]
        unreported_ids: Vec<String>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct Transfers {
    pub(crate) self_skips: usize,
    pub(crate) ledger_only_skips: usize,
    pub(crate) tier_overlap: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct FinalCounts {
    #[serde(flatten)]
    pub(crate) counts: StatusCounts,
    pub(crate) rows: usize,
    pub(crate) duplicate_ids: usize,
}

/// What the producer says its result accounts for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EvidenceAccounting {
    pub(crate) version: u32,
    pub(crate) inventory: Inventory,
    #[serde(default)]
    pub(crate) passes: Vec<PassAccounting>,
    #[serde(default)]
    pub(crate) transfers: Transfers,
    #[serde(default, rename = "final")]
    pub(crate) final_rows: FinalCounts,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) blocked_by: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) uninstrumented: Vec<String>,
}

/// The accounting contract this core reads.
pub(crate) const ACCOUNTING_VERSION: u32 = 1;

fn cap(name: &str) -> String {
    name.chars().take(NAME_CHARS).collect()
}

fn cap_ids(ids: &[String]) -> Vec<String> {
    ids.iter().take(NAMED_IDS).map(|id| cap(id)).collect()
}

impl EvidenceAccounting {
    /// The producer's block, when it is one this core reads. An unreadable or
    /// newer block is no accounting (the result reads as uninstrumented), never
    /// a reason to drop the rows it rode with. Names are bounded on the way in.
    pub(crate) fn from_artifact(value: &serde_json::Value) -> Option<Self> {
        let mut accounting: Self = serde_json::from_value(value.get("accounting")?.clone()).ok()?;
        if accounting.version != ACCOUNTING_VERSION {
            return None;
        }
        for pass in &mut accounting.passes {
            pass.pass = cap(&pass.pass);
            pass.unresolved_ids = cap_ids(&pass.unresolved_ids);
            if let Some(runner) = &mut pass.runner {
                runner.source = cap(&runner.source);
            }
        }
        if let Inventory::Listed { unreported_ids, .. } = &mut accounting.inventory {
            *unreported_ids = cap_ids(unreported_ids);
        }
        match &mut accounting.inventory {
            Inventory::Unavailable { reason } => *reason = cap(reason),
            Inventory::Listed { unit, .. } => *unit = cap(unit),
        }
        accounting.blocked_by = accounting.blocked_by.as_deref().map(cap);
        accounting.uninstrumented = cap_ids(&accounting.uninstrumented);
        Some(accounting)
    }
}

/// What ingestion received from the producer and kept: the link between the
/// rows a producer handed off and the rows the store was asked to hold.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct Ingestion {
    pub(crate) received: usize,
    pub(crate) kept: usize,
    /// Rows dropped because an earlier row already held their identity.
    pub(crate) duplicate_rows: usize,
    pub(crate) duplicate_ids: Vec<String>,
    /// Rows refused for an empty identity or a status that is not terminal.
    pub(crate) invalid_rows: usize,
}

/// The one classification of a lane result's evidence. Precedence runs from
/// what never ran to what ran and was lost; a class names the most
/// consequential gap, and `findings` names every one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum EvidenceClass {
    /// Every started test has a terminal row, the runner's tally reconciles
    /// under named transfers, and every ingested row is stored.
    Accounted,
    /// A static check or script: its exit code is its verdict, and no test
    /// enumeration is expected of it.
    NoEnumerationExpected,
    /// A lane expected to enumerate tests produced no structured result. Its
    /// exit status (a status-only completion) is not per-test authority.
    NoResultAuthority,
    /// No structured result, read where the lane's definition is not known
    /// (an observation alone): the exit status is all there is, and whether a
    /// test enumeration was owed is the lane's definition to say.
    ExitStatusOnly,
    /// The suite never ran: it was never placed, a prerequisite failed, the
    /// substrate failed, or it was cancelled.
    NotRun,
    /// The build before the tests failed, so no test started.
    CompileBlocked,
    /// The selection held no test.
    ZeroSelected,
    /// Tests started that never reached a terminal outcome.
    Interrupted,
    /// The enumeration does not account for itself: a runner tally that does
    /// not reconcile, a missing tally, unreported planned units, duplicate or
    /// invalid rows, or a producer that declared itself incomplete.
    Incomplete,
    /// The store holds fewer rows than ingestion kept.
    LostEvidence,
    /// A result recorded without accounting (before it existed, or by a
    /// runner without it). Its counts stand; what it started is unknown.
    Uninstrumented,
    /// This build cannot run the lane at all.
    Unsupported,
}

impl EvidenceClass {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Accounted => "accounted",
            Self::NoEnumerationExpected => "no enumeration expected",
            Self::NoResultAuthority => "no result authority",
            Self::ExitStatusOnly => "exit status only",
            Self::NotRun => "not run",
            Self::CompileBlocked => "compile-blocked",
            Self::ZeroSelected => "zero selected",
            Self::Interrupted => "interrupted",
            Self::Incomplete => "incomplete enumeration",
            Self::LostEvidence => "lost evidence",
            Self::Uninstrumented => "uninstrumented",
            Self::Unsupported => "unsupported",
        }
    }

    /// Whether a gap of this class overrides a recorded `complete`/`reusable`
    /// claim when it is shown: the rows cannot stand for what ran.
    pub(crate) fn revokes_authority(self) -> bool {
        matches!(
            self,
            Self::Interrupted | Self::Incomplete | Self::LostEvidence | Self::CompileBlocked
        )
    }
}

/// A lane result's evidence, link by link. Populations the surface could not
/// read are `None`, never zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EvidenceReconciliation {
    pub(crate) class: EvidenceClass,
    /// False for a verdict reused from an earlier execution: its populations
    /// are that execution's, cited by `reused_from`, and nothing here ran.
    pub(crate) fresh: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reused_from: Option<String>,
    pub(crate) inventory: String,
    pub(crate) started: Option<usize>,
    pub(crate) runner_total: Option<usize>,
    pub(crate) enumerated: Option<usize>,
    pub(crate) unresolved: usize,
    pub(crate) handed_off: Option<usize>,
    pub(crate) ingested: Option<usize>,
    pub(crate) persisted: Option<usize>,
    /// Every broken link, named. Empty only for a class with no gap.
    pub(crate) findings: Vec<String>,
    /// Named semantics that explain a count difference without being a gap
    /// (retries, ignored tests without a start, skip transfers, deferrals).
    pub(crate) notes: Vec<String>,
    /// Phases whose outcome is in the exit code but which produced no rows.
    pub(crate) uninstrumented: Vec<String>,
}

/// The facts one surface has about a lane result.
pub(crate) struct EvidenceInputs<'a> {
    /// The stored structured result; `None` when the run produced none.
    pub(crate) parsed: Option<&'a ParsedCheckResult>,
    pub(crate) failure_kind: Option<&'a str>,
    /// Whether the lane's command is a test runner's, when the surface knows
    /// the command; `None` when it does not.
    pub(crate) expects_enumeration: Option<bool>,
    /// The rows the store holds, when the surface read them.
    pub(crate) persisted: Option<&'a ObservationTestCounts>,
    /// The original evidence a reused verdict came from.
    pub(crate) reused_from: Option<String>,
    /// For a result recorded without accounting: the runner tally retained in
    /// its output disagrees with the stored rows, as the reader that compared
    /// them worded it.
    pub(crate) retained_tally_mismatch: Option<String>,
}

fn named(ids: &[String], count: usize) -> String {
    let more = count.saturating_sub(ids.len());
    let mut text = ids.join(", ");
    if more > 0 {
        text.push_str(&format!(", +{more} more"));
    }
    text
}

fn counts_text(counts: &StatusCounts) -> String {
    format!(
        "{} passed, {} failed, {} skipped, {} deferred",
        counts.passed, counts.failed, counts.skipped, counts.deferred
    )
}

fn sum_options(values: impl Iterator<Item = Option<usize>>) -> Option<usize> {
    let mut any = false;
    let mut sum = 0usize;
    for value in values {
        sum = sum.checked_add(value?)?;
        any = true;
    }
    any.then_some(sum)
}

/// The classification for a lane this build cannot run.
pub(crate) fn unsupported_evidence() -> EvidenceReconciliation {
    EvidenceReconciliation {
        class: EvidenceClass::Unsupported,
        fresh: true,
        reused_from: None,
        inventory: "not taken: this build cannot run the lane".into(),
        started: None,
        runner_total: None,
        enumerated: None,
        unresolved: 0,
        handed_off: None,
        ingested: None,
        persisted: None,
        findings: Vec::new(),
        notes: Vec::new(),
        uninstrumented: Vec::new(),
    }
}

/// Classify one lane result's evidence. See [`EvidenceClass`] for the order.
pub(crate) fn reconcile_evidence(inputs: EvidenceInputs<'_>) -> EvidenceReconciliation {
    let EvidenceInputs {
        parsed,
        failure_kind,
        expects_enumeration,
        persisted,
        reused_from,
        retained_tally_mismatch,
    } = inputs;
    let kind = failure_kind.and_then(CheckFailureKind::from_stored);
    let mut findings = Vec::new();
    let mut notes = Vec::new();
    if let Some(source) = &reused_from {
        notes.push(format!(
            "reused: these populations are the original execution's ({source}); nothing ran for this request"
        ));
    }
    // A substrate death, cancellation, prerequisite failure or unplaced lane:
    // the suite's own verdict was never produced.
    let never_ran = kind.is_some_and(|kind| {
        kind == CheckFailureKind::Prerequisite || !kind.is_verdict_about_the_code()
    });
    if let Some(kind) = failure_kind {
        notes.push(format!("the run ended with failure kind `{kind}`"));
    }
    let mut result = EvidenceReconciliation {
        class: EvidenceClass::Accounted,
        fresh: reused_from.is_none(),
        reused_from,
        inventory: "not recorded".into(),
        started: None,
        runner_total: None,
        enumerated: None,
        unresolved: 0,
        handed_off: None,
        ingested: None,
        persisted: persisted.map(ObservationTestCounts::total),
        findings: Vec::new(),
        notes: Vec::new(),
        uninstrumented: Vec::new(),
    };
    // Stored rows without a compact result: a row recorded before that column
    // existed. Its rows are an enumeration whose producer facts are gone.
    if parsed.is_none() && result.persisted.is_some_and(|stored| stored > 0) {
        notes.push(
            "no compact result recorded beside the stored rows (an older row); producer and ingestion facts are unavailable"
                .into(),
        );
        findings.extend(retained_tally_mismatch);
        result.class = if !findings.is_empty() {
            EvidenceClass::Incomplete
        } else if never_ran {
            EvidenceClass::NotRun
        } else {
            EvidenceClass::Uninstrumented
        };
        result.findings = findings;
        result.notes = notes;
        return result;
    }
    let Some(parsed) = parsed.filter(|parsed| parsed.is_test_runner()) else {
        result.class = if kind == Some(CheckFailureKind::Selection) {
            EvidenceClass::ZeroSelected
        } else if never_ran {
            EvidenceClass::NotRun
        } else if parsed.is_some() || expects_enumeration == Some(false) {
            EvidenceClass::NoEnumerationExpected
        } else if expects_enumeration.is_none() {
            notes.push(
                "no structured result; whether this lane owed a test enumeration is its definition's to say"
                    .into(),
            );
            EvidenceClass::ExitStatusOnly
        } else {
            findings.push(
                "no structured result: the exit status alone (a status-only completion) is not per-test authority"
                    .into(),
            );
            EvidenceClass::NoResultAuthority
        };
        result.inventory = "not applicable".into();
        result.findings = findings;
        result.notes = notes;
        return result;
    };

    let ingested = parsed
        .ingestion
        .as_ref()
        .map(|ingestion| ingestion.kept)
        .unwrap_or(parsed.passed + parsed.failed + parsed.skipped + parsed.deferred);
    result.ingested = Some(ingested);
    // What the core kept, by status. Every count below compares against this.
    let ingested_counts = StatusCounts {
        passed: parsed.passed,
        failed: parsed.failed,
        skipped: parsed.skipped,
        deferred: parsed.deferred,
    };

    if let Some(accounting) = &parsed.accounting {
        result.inventory = match &accounting.inventory {
            Inventory::Unavailable { reason } => format!("unavailable: {reason}"),
            Inventory::Listed { unit, count: 0, .. } => format!("listed: empty (0 {unit}s)"),
            Inventory::Listed {
                unit,
                count,
                unreported,
                unreported_ids,
            } => {
                if *unreported > 0 {
                    findings.push(format!(
                        "{unreported} of {count} planned {unit}s have no runner report: {}",
                        named(unreported_ids, *unreported)
                    ));
                }
                format!("listed: {count} {unit}s, {unreported} unreported")
            }
        };
        result.started = sum_options(accounting.passes.iter().map(|pass| pass.started));
        result.runner_total = sum_options(
            accounting
                .passes
                .iter()
                .map(|pass| pass.runner.as_ref().map(|runner| runner.total)),
        );
        let mut enumerated = StatusCounts::default();
        let (mut collection, mut stderr_only) = (0, 0);
        for pass in &accounting.passes {
            enumerated.passed += pass.enumerated.passed;
            enumerated.failed += pass.enumerated.failed;
            enumerated.skipped += pass.enumerated.skipped;
            enumerated.deferred += pass.enumerated.deferred;
            collection += pass.collection_failures;
            stderr_only += pass.stderr_only_failures;
            result.unresolved += pass.unresolved;
            if pass.unresolved > 0 {
                findings.push(format!(
                    "pass `{}`: {} started tests have no terminal outcome and no row (none invented): {}",
                    pass.pass,
                    pass.unresolved,
                    named(&pass.unresolved_ids, pass.unresolved)
                ));
            }
            match (&pass.runner, pass.runner_reconciled) {
                (None, _) if pass.started != Some(0) || pass.enumerated.total() > 0 => {
                    findings.push(format!(
                        "pass `{}`: the runner left no tally of its own to reconcile against",
                        pass.pass
                    ));
                }
                (Some(runner), Some(false)) => findings.push(format!(
                    "pass `{}`: runner tally ({}: {} total; {} passed, {} failed, {} skipped, {} timed out) does not reconcile with its terminal rows ({})",
                    pass.pass,
                    runner.source,
                    runner.total,
                    runner.passed,
                    runner.failed,
                    runner.skipped,
                    runner.timed_out,
                    counts_text(&pass.enumerated)
                )),
                _ => {}
            }
            for (count, what) in [
                (pass.retried, "identities took more than one attempt (counted once, by final attempt)"),
                (pass.terminal_without_start, "terminal rows arrived without a start event (nextest reports ignored tests so)"),
                (pass.skip_witnesses, "skips were witnessed only by nextest's human SKIP line"),
                (pass.filtered_skips, "runner skips were outside the selection (never started, never rows)"),
                (pass.collection_failures, "failed rows are files that never collected (outside the runner's test tally)"),
                (pass.stderr_only_failures, "failed rows were named only on stderr (outside the runner's report)"),
            ] {
                if count > 0 {
                    notes.push(format!("pass `{}`: {count} {what}", pass.pass));
                }
            }
        }
        result.enumerated = Some(enumerated.total());
        let transfers = &accounting.transfers;
        let handed = &accounting.final_rows;
        result.handed_off = Some(handed.rows);
        for (count, what) in [
            (
                transfers.self_skips,
                "runner passes reclassified as declared self-skips",
            ),
            (
                transfers.ledger_only_skips,
                "skipped rows come from the skip log alone",
            ),
            (
                transfers.tier_overlap,
                "global-pass rows were dropped for an identity the local pass holds",
            ),
        ] {
            if count > 0 {
                notes.push(format!("{count} {what}"));
            }
        }
        // Runner terminals -> handed-off rows, under the named transfers.
        let expected_rows =
            (enumerated.total() + transfers.ledger_only_skips + collection + stderr_only)
                .checked_sub(transfers.tier_overlap);
        let expected_status = (transfers.tier_overlap == 0).then(|| StatusCounts {
            passed: enumerated.passed.saturating_sub(transfers.self_skips),
            failed: enumerated.failed + collection + stderr_only,
            skipped: enumerated.skipped + transfers.self_skips + transfers.ledger_only_skips,
            deferred: enumerated.deferred,
        });
        if expected_rows != Some(handed.rows)
            || expected_status
                .as_ref()
                .is_some_and(|status| *status != handed.counts)
        {
            findings.push(format!(
                "handed-off rows ({}, {} rows) do not follow from the runner's terminal rows ({}) under the named transfers",
                counts_text(&handed.counts),
                handed.rows,
                counts_text(&enumerated)
            ));
        }
        if handed.duplicate_ids > 0 {
            findings.push(format!(
                "{} identities are carried by more than one handed-off row",
                handed.duplicate_ids
            ));
        }
        if accounting.blocked_by.is_some() && result.started.unwrap_or(0) == 0 {
            result.class = EvidenceClass::CompileBlocked;
        }
        result.uninstrumented = accounting.uninstrumented.clone();
        if let Some(ingestion) = &parsed.ingestion {
            if ingestion.received != handed.rows {
                findings.push(format!(
                    "the core received {} rows; the producer handed off {}",
                    ingestion.received, handed.rows
                ));
            } else if ingestion.duplicate_rows == 0
                && ingestion.invalid_rows == 0
                && ingested_counts != handed.counts
            {
                // Same row count, different statuses: a row changed outcome
                // between the producer's handoff and what the core kept.
                // With duplicate or invalid rows the kept statuses differ by
                // construction, and those findings already name why.
                findings.push(format!(
                    "ingested rows ({}) do not match the handed-off rows ({}) status by status",
                    counts_text(&ingested_counts),
                    counts_text(&handed.counts)
                ));
            }
        }
    } else {
        notes.push(
            "no accounting: recorded before evidence accounting or by a producer without it; what started is unknown"
                .into(),
        );
        findings.extend(retained_tally_mismatch);
    }
    if let Some(ingestion) = &parsed.ingestion {
        if ingestion.duplicate_rows > 0 {
            findings.push(format!(
                "ingestion dropped {} rows whose identity an earlier row held: {}",
                ingestion.duplicate_rows,
                named(&ingestion.duplicate_ids, ingestion.duplicate_rows)
            ));
        }
        if ingestion.invalid_rows > 0 {
            findings.push(format!(
                "ingestion refused {} rows with an empty identity or a non-terminal status",
                ingestion.invalid_rows
            ));
        }
    }
    if parsed.deferred > 0 {
        notes.push(format!(
            "{} deferred: started and terminated at the per-test budget, never judged",
            parsed.deferred
        ));
    }
    let lost = result.persisted.is_some_and(|stored| stored < ingested);
    if let Some(persisted) = persisted {
        let stored = persisted.total();
        let stored_counts = StatusCounts {
            passed: persisted.passed,
            failed: persisted.failed,
            skipped: persisted.skipped,
            deferred: persisted.deferred,
        };
        if stored != ingested {
            findings.push(format!(
                "the store holds {stored} rows; ingestion kept {ingested}"
            ));
        } else if stored_counts != ingested_counts {
            findings.push(format!(
                "the stored rows ({}) do not match the ingested rows ({}) status by status",
                counts_text(&stored_counts),
                counts_text(&ingested_counts)
            ));
        }
    }
    if !parsed.complete && findings.is_empty() && result.class != EvidenceClass::CompileBlocked {
        let declared = parsed
            .report
            .as_deref()
            .and_then(|report| report.lines().find(|line| !line.trim().is_empty()))
            .map(|line| format!(": {}", line.trim()))
            .unwrap_or_default();
        findings.push(format!(
            "the producer declared its enumeration incomplete{declared}"
        ));
    }

    if result.class != EvidenceClass::CompileBlocked {
        result.class = if parsed.selection == "empty" || kind == Some(CheckFailureKind::Selection) {
            EvidenceClass::ZeroSelected
        } else if lost {
            EvidenceClass::LostEvidence
        } else if result.unresolved > 0 {
            EvidenceClass::Interrupted
        } else if !findings.is_empty() {
            EvidenceClass::Incomplete
        } else if never_ran {
            EvidenceClass::NotRun
        } else if parsed.accounting.is_none() {
            EvidenceClass::Uninstrumented
        } else {
            EvidenceClass::Accounted
        };
    }
    result.findings = findings;
    result.notes = notes;
    result
}

impl EvidenceReconciliation {
    /// Whether this reconciliation is about a test enumeration at all: a class
    /// that never reached one has no populations to show.
    pub(crate) fn has_populations(&self) -> bool {
        self.ingested.is_some() || self.persisted.is_some_and(|stored| stored > 0)
    }

    /// The reconciliation as observation-page lines.
    pub(crate) fn lines(&self) -> Vec<String> {
        let population = |value: Option<usize>| {
            value.map_or_else(|| "not recorded".to_string(), |value| value.to_string())
        };
        let mut lines = vec![
            format!("- Class: {}", self.class.label()),
            format!(
                "- Execution: {}",
                if self.fresh {
                    "fresh".to_string()
                } else {
                    format!(
                        "reused from {} (not counted as a fresh execution)",
                        self.reused_from
                            .as_deref()
                            .unwrap_or("an earlier execution")
                    )
                }
            ),
        ];
        if !self.has_populations() {
            lines.extend(
                self.findings
                    .iter()
                    .map(|finding| format!("- Gap: {finding}")),
            );
            lines.extend(self.notes.iter().map(|note| format!("- Note: {note}")));
            return lines;
        }
        lines.extend([
            format!("- Expected inventory: {}", self.inventory),
            format!("- Started: {}", population(self.started)),
            format!("- Runner tally: {}", population(self.runner_total)),
            format!("- Runner terminal rows: {}", population(self.enumerated)),
            format!("- Unresolved starts: {}", self.unresolved),
            format!("- Handed off: {}", population(self.handed_off)),
            format!("- Ingested: {}", population(self.ingested)),
            format!("- Persisted: {}", population(self.persisted)),
        ]);
        lines.extend(
            self.findings
                .iter()
                .map(|finding| format!("- Gap: {finding}")),
        );
        lines.extend(self.notes.iter().map(|note| format!("- Note: {note}")));
        if !self.uninstrumented.is_empty() {
            lines.push(format!(
                "- Uninstrumented phases (in the exit code, no rows): {}",
                self.uninstrumented.join(", ")
            ));
        }
        lines
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::check_parsers::{parse_check_output, RESULT_ARTIFACT_JSON_MARKER};
    use serde_json::json;

    const COMMAND: &str = "bun run test:rust:tiered {changedFiles}";

    fn rows(statuses: &[(&str, &str)]) -> serde_json::Value {
        statuses
            .iter()
            .map(|(id, status)| {
                json!({"id": id, "status": status, "attempts": 1, "retried": false, "flaky": false})
            })
            .collect()
    }

    /// A pass as the producer writes it: `started` identities, terminal rows by
    /// status, the unresolved names, and the runner's tally when it has one.
    fn pass(
        started: Option<usize>,
        enumerated: [usize; 4],
        unresolved: &[&str],
        runner: Option<[usize; 5]>,
    ) -> serde_json::Value {
        let [passed, failed, skipped, deferred] = enumerated;
        let reconciled = runner.map(|[total, rp, rf, rs, rt]| {
            total == passed + failed + skipped + deferred
                && rp == passed
                && rf + rt == failed + deferred
                && rs == skipped
                && unresolved.is_empty()
        });
        json!({
            "pass": "full", "processes": 1, "started": started,
            "enumerated": {"passed": passed, "failed": failed, "skipped": skipped, "deferred": deferred},
            "unresolved": unresolved.len(), "unresolvedIds": unresolved,
            "runner": runner.map(|[total, passed, failed, skipped, timed_out]| json!({
                "source": "nextest-summary", "total": total, "passed": passed,
                "failed": failed, "skipped": skipped, "timedOut": timed_out,
            })),
            "runnerReconciled": reconciled,
            "filteredSkips": 0, "skipWitnesses": 0, "terminalWithoutStart": 0, "retried": 0,
            "collectionFailures": 0, "stderrOnlyFailures": 0,
        })
    }

    fn accounting(
        passes: Vec<serde_json::Value>,
        transfers: [usize; 2],
        final_rows: [usize; 5],
    ) -> serde_json::Value {
        let [passed, failed, skipped, deferred, rows] = final_rows;
        json!({
            "version": 1,
            "inventory": {"state": "unavailable", "reason": "nextest enumerates at run time"},
            "passes": passes,
            "transfers": {"selfSkips": transfers[0], "ledgerOnlySkips": transfers[1], "tierOverlap": 0},
            "final": {"passed": passed, "failed": failed, "skipped": skipped, "deferred": deferred, "rows": rows, "duplicateIds": 0},
        })
    }

    /// The result the core ingests from a producer document, through the same
    /// `CAIRN_CHECK_RESULT_JSON=` line the executor appends.
    fn ingest(document: serde_json::Value) -> ParsedCheckResult {
        parse_check_output(COMMAND, &format!("{RESULT_ARTIFACT_JSON_MARKER}{document}"))
            .expect("a test-runner artifact parses")
    }

    fn document(
        selection: &str,
        complete: bool,
        tests: serde_json::Value,
        accounting: Option<serde_json::Value>,
    ) -> serde_json::Value {
        let mut document = json!({
            "schemaVersion": 1, "parser": "nextest", "complete": complete,
            "selection": selection, "tests": tests, "undeclaredSkips": 0,
        });
        if let Some(accounting) = accounting {
            document["accounting"] = accounting;
        }
        document
    }

    fn ordinary() -> ParsedCheckResult {
        ingest(document(
            "full",
            true,
            rows(&[
                ("c::c$a", "passed"),
                ("c::c$b", "failed"),
                ("c::c$c", "skipped"),
            ]),
            Some(accounting(
                vec![pass(Some(3), [1, 1, 1, 0], &[], Some([3, 1, 1, 1, 0]))],
                [0, 0],
                [1, 1, 1, 0, 3],
            )),
        ))
    }

    fn stored(
        passed: usize,
        failed: usize,
        skipped: usize,
        deferred: usize,
    ) -> ObservationTestCounts {
        ObservationTestCounts {
            passed,
            failed,
            skipped,
            deferred,
            self_skipped: 0,
        }
    }

    fn reconcile(
        parsed: Option<&ParsedCheckResult>,
        persisted: Option<&ObservationTestCounts>,
    ) -> EvidenceReconciliation {
        reconcile_evidence(EvidenceInputs {
            parsed,
            failure_kind: None,
            expects_enumeration: Some(true),
            persisted,
            reused_from: None,
            retained_tally_mismatch: None,
        })
    }

    #[test]
    fn an_ordinary_run_is_accounted_link_by_link() {
        let parsed = ordinary();
        let evidence = reconcile(Some(&parsed), Some(&stored(1, 1, 1, 0)));
        assert_eq!(evidence.class, EvidenceClass::Accounted, "{evidence:#?}");
        assert!(evidence.findings.is_empty());
        assert!(evidence.fresh);
        assert_eq!(
            (
                evidence.started,
                evidence.runner_total,
                evidence.enumerated,
                evidence.handed_off,
                evidence.ingested,
                evidence.persisted
            ),
            (Some(3), Some(3), Some(3), Some(3), Some(3), Some(3))
        );
    }

    /// The negative control for the whole chain: one terminal event dropped
    /// between the runner and the producer's rows.
    #[test]
    fn a_dropped_terminal_event_is_an_interruption_and_no_outcome_is_invented() {
        let parsed = ingest(document(
            "full",
            false,
            rows(&[("c::c$a", "passed"), ("c::c$c", "skipped")]),
            Some(accounting(
                vec![pass(
                    Some(3),
                    [1, 0, 1, 0],
                    &["c::c$b"],
                    Some([3, 1, 1, 1, 0]),
                )],
                [0, 0],
                [1, 0, 1, 0, 2],
            )),
        ));
        let evidence = reconcile(Some(&parsed), Some(&stored(1, 0, 1, 0)));
        assert_eq!(evidence.class, EvidenceClass::Interrupted);
        assert_eq!(evidence.unresolved, 1);
        assert!(
            evidence
                .findings
                .iter()
                .any(|finding| finding.contains("c::c$b")),
            "{evidence:#?}"
        );
        assert!(evidence
            .findings
            .iter()
            .any(|finding| finding.contains("does not reconcile")));
        assert!(evidence.class.revokes_authority());
    }

    #[test]
    fn a_stored_row_short_of_what_ingestion_kept_is_lost_evidence() {
        let parsed = ordinary();
        let evidence = reconcile(Some(&parsed), Some(&stored(1, 0, 1, 0)));
        assert_eq!(evidence.class, EvidenceClass::LostEvidence);
        assert!(evidence
            .findings
            .iter()
            .any(|finding| finding.contains("the store holds 2 rows; ingestion kept 3")));
        assert!(evidence.class.revokes_authority());
    }

    /// Owner-review counterexample 1: the producer's accounting says it handed
    /// off 1 passed / 1 failed / 1 skipped, but the rows the core received
    /// carry the failure as a pass. Same row count, so only a status-by-status
    /// comparison sees it.
    #[test]
    fn a_status_changed_between_handoff_and_ingestion_is_not_accounted() {
        let parsed = ingest(document(
            "full",
            true,
            rows(&[
                ("c::c$a", "passed"),
                ("c::c$b", "passed"),
                ("c::c$c", "skipped"),
            ]),
            Some(accounting(
                vec![pass(Some(3), [1, 1, 1, 0], &[], Some([3, 1, 1, 1, 0]))],
                [0, 0],
                [1, 1, 1, 0, 3],
            )),
        ));
        let evidence = reconcile(Some(&parsed), Some(&stored(2, 0, 1, 0)));
        assert_eq!(evidence.class, EvidenceClass::Incomplete, "{evidence:#?}");
        assert!(evidence.class.revokes_authority());
        assert!(
            evidence
                .findings
                .iter()
                .any(|finding| finding.contains("do not match the handed-off rows")),
            "{evidence:#?}"
        );
    }

    /// Owner-review counterexample 2: what ingestion kept is ordinary, but the
    /// store holds the same number of rows with a failure turned into a pass.
    #[test]
    fn a_stored_status_that_differs_from_ingestion_at_the_same_total_is_not_accounted() {
        let parsed = ordinary();
        let evidence = reconcile(Some(&parsed), Some(&stored(2, 0, 1, 0)));
        assert_eq!(evidence.class, EvidenceClass::Incomplete, "{evidence:#?}");
        assert!(evidence.class.revokes_authority());
        assert!(evidence
            .findings
            .iter()
            .any(|finding| finding.contains("the stored rows (2 passed, 0 failed, 1 skipped, 0 deferred) do not match the ingested rows (1 passed, 1 failed, 1 skipped, 0 deferred)")));
        // The matching store stays accounted: the check is not vacuous.
        assert_eq!(
            reconcile(Some(&parsed), Some(&stored(1, 1, 1, 0))).class,
            EvidenceClass::Accounted
        );
    }

    #[test]
    fn duplicate_rows_are_named_and_never_accounted() {
        let parsed = ingest(document(
            "full",
            true,
            rows(&[("c::c$a", "passed"), ("c::c$a", "failed")]),
            Some(accounting(
                vec![pass(Some(1), [1, 1, 0, 0], &[], Some([1, 1, 0, 0, 0]))],
                [0, 0],
                [1, 1, 0, 0, 2],
            )),
        ));
        let evidence = reconcile(Some(&parsed), Some(&stored(0, 1, 0, 0)));
        assert_eq!(evidence.class, EvidenceClass::Incomplete, "{evidence:#?}");
        assert!(evidence
            .findings
            .iter()
            .any(|finding| finding.contains("ingestion dropped 1 rows")
                && finding.contains("c::c$a")));
        assert_eq!(evidence.ingested, Some(1));
        // The duplicate is the diagnosis; it is not restated as a status
        // contradiction, since dropping a row changes the kept statuses.
        assert!(
            !evidence
                .findings
                .iter()
                .any(|finding| finding.contains("status by status")),
            "{evidence:#?}"
        );
    }

    #[test]
    fn a_cancelled_run_is_not_run_without_rows_and_interrupted_with_its_starts() {
        let cancelled = reconcile_evidence(EvidenceInputs {
            parsed: None,
            failure_kind: Some("cancelled"),
            expects_enumeration: Some(true),
            persisted: None,
            reused_from: None,
            retained_tally_mismatch: None,
        });
        assert_eq!(cancelled.class, EvidenceClass::NotRun);

        let partial = ingest(document(
            "full",
            false,
            rows(&[("c::c$a", "passed")]),
            Some(accounting(
                vec![pass(Some(3), [1, 0, 0, 0], &["c::c$b", "c::c$c"], None)],
                [0, 0],
                [1, 0, 0, 0, 1],
            )),
        ));
        let evidence = reconcile_evidence(EvidenceInputs {
            parsed: Some(&partial),
            failure_kind: Some("killed"),
            expects_enumeration: Some(true),
            persisted: None,
            reused_from: None,
            retained_tally_mismatch: None,
        });
        assert_eq!(evidence.class, EvidenceClass::Interrupted);
        assert!(evidence
            .findings
            .iter()
            .any(|finding| finding.contains("no tally")));
    }

    #[test]
    fn a_truncated_artifact_is_no_result_and_the_stored_compact_result_is_truncation_proof() {
        let full = format!(
            "{RESULT_ARTIFACT_JSON_MARKER}{}",
            document(
                "full",
                true,
                rows(&[
                    ("c::c$a", "passed"),
                    ("c::c$b", "failed"),
                    ("c::c$c", "skipped")
                ]),
                Some(accounting(
                    vec![pass(Some(3), [1, 1, 1, 0], &[], Some([3, 1, 1, 1, 0]))],
                    [0, 0],
                    [1, 1, 1, 0, 3],
                )),
            )
        );
        // An artifact cut mid-document is no structured result, never a
        // smaller complete one.
        let cut = &full[..full.len() / 2];
        assert!(parse_check_output(COMMAND, cut).is_none());
        let evidence = reconcile(None, None);
        assert_eq!(evidence.class, EvidenceClass::NoResultAuthority);

        // The compact column keeps the accounting whole, so a read that never
        // sees the (bounded) output tail classifies exactly as ingestion did.
        let parsed = parse_check_output(COMMAND, &full).unwrap();
        let compact = crate::execution::cache::compact_verdict_result_for_test(
            serde_json::to_string(&parsed).unwrap().as_str(),
        )
        .unwrap();
        let compact: ParsedCheckResult = serde_json::from_str(&compact).unwrap();
        assert!(
            compact.tests.is_empty(),
            "the compact result carries no rows"
        );
        let persisted = stored(1, 1, 1, 0);
        assert_eq!(
            reconcile(Some(&compact), Some(&persisted)),
            reconcile(Some(&parsed), Some(&persisted))
        );
    }

    #[test]
    fn skip_transfers_and_deferrals_reconcile_under_their_named_semantics() {
        // Runner: 3 passed, 1 timed out under the budget (deferred). The skip
        // ledger moved one pass to a declared self-skip and added one skip the
        // runner never finished.
        let parsed = ingest(document(
            "fast",
            true,
            rows(&[
                ("c::c$a", "passed"),
                ("c::c$b", "passed"),
                ("c::c$self", "skipped"),
                ("ledger_only", "skipped"),
                ("c::c$slow", "deferred"),
            ]),
            Some(accounting(
                vec![pass(Some(4), [3, 0, 0, 1], &[], Some([4, 3, 0, 0, 1]))],
                [1, 1],
                [2, 0, 2, 1, 5],
            )),
        ));
        let evidence = reconcile(Some(&parsed), Some(&stored(2, 0, 2, 1)));
        assert_eq!(evidence.class, EvidenceClass::Accounted, "{evidence:#?}");
        for expected in [
            "1 runner passes reclassified",
            "1 skipped rows come from the skip log alone",
            "1 deferred",
        ] {
            assert!(
                evidence.notes.iter().any(|note| note.contains(expected)),
                "{expected}: {evidence:#?}"
            );
        }
        // The same rows without the transfers named do not follow.
        let unexplained = ingest(document(
            "fast",
            true,
            rows(&[("c::c$a", "passed"), ("c::c$self", "skipped")]),
            Some(accounting(
                vec![pass(Some(2), [2, 0, 0, 0], &[], Some([2, 2, 0, 0, 0]))],
                [0, 0],
                [1, 0, 1, 0, 2],
            )),
        ));
        assert_eq!(
            reconcile(Some(&unexplained), Some(&stored(1, 0, 1, 0))).class,
            EvidenceClass::Incomplete
        );
    }

    #[test]
    fn a_reused_verdict_cites_its_source_and_is_never_fresh() {
        let parsed = ordinary();
        let evidence = reconcile_evidence(EvidenceInputs {
            parsed: Some(&parsed),
            failure_kind: None,
            expects_enumeration: Some(true),
            persisted: Some(&stored(1, 1, 1, 0)),
            reused_from: Some("the execution at abc123 (cairn://p/x/check-observations/h)".into()),
            retained_tally_mismatch: None,
        });
        assert!(!evidence.fresh);
        assert_eq!(evidence.class, EvidenceClass::Accounted);
        assert!(evidence
            .lines()
            .iter()
            .any(|line| line.contains("reused from the execution at abc123")
                && line.contains("not counted as a fresh execution")));
    }

    #[test]
    fn compile_blocked_zero_selected_and_uninstrumented_stay_distinct() {
        let blocked = ingest(document(
            "full",
            false,
            rows(&[]),
            Some({
                let mut accounting = accounting(
                    vec![pass(Some(0), [0, 0, 0, 0], &[], None)],
                    [0, 0],
                    [0, 0, 0, 0, 0],
                );
                accounting["blockedBy"] = json!("compile");
                accounting
            }),
        ));
        let empty = ingest(document(
            "empty",
            true,
            rows(&[]),
            Some(accounting(vec![], [0, 0], [0, 0, 0, 0, 0])),
        ));
        let legacy = ingest(document("full", true, rows(&[("c::c$a", "passed")]), None));
        let newer = ingest(document(
            "full",
            true,
            rows(&[("c::c$a", "passed")]),
            Some(json!({"version": 2, "inventory": {"state": "unavailable", "reason": "x"}})),
        ));
        let classes =
            [&blocked, &empty, &legacy, &newer].map(|parsed| reconcile(Some(parsed), None).class);
        assert_eq!(
            classes,
            [
                EvidenceClass::CompileBlocked,
                EvidenceClass::ZeroSelected,
                EvidenceClass::Uninstrumented,
                // A block this core does not read is no accounting, not a gap.
                EvidenceClass::Uninstrumented,
            ]
        );
        assert!(EvidenceClass::CompileBlocked.revokes_authority());
        assert!(!EvidenceClass::ZeroSelected.revokes_authority());
    }

    /// A status-only completion carries an exit and no rows: it is never test
    /// authority, whatever the exit said.
    #[test]
    fn a_status_only_result_is_not_test_authority() {
        let classify = |expects| {
            reconcile_evidence(EvidenceInputs {
                parsed: None,
                failure_kind: None,
                expects_enumeration: expects,
                persisted: None,
                reused_from: None,
                retained_tally_mismatch: None,
            })
            .class
        };
        assert_eq!(classify(Some(true)), EvidenceClass::NoResultAuthority);
        assert_eq!(classify(None), EvidenceClass::ExitStatusOnly);
        assert_eq!(classify(Some(false)), EvidenceClass::NoEnumerationExpected);
        assert_eq!(unsupported_evidence().class, EvidenceClass::Unsupported);
    }

    #[test]
    fn rows_the_core_did_not_receive_are_a_handoff_gap() {
        let parsed = ingest(document(
            "full",
            true,
            rows(&[("c::c$a", "passed")]),
            Some(accounting(
                vec![pass(Some(2), [2, 0, 0, 0], &[], Some([2, 2, 0, 0, 0]))],
                [0, 0],
                [2, 0, 0, 0, 2],
            )),
        ));
        let evidence = reconcile(Some(&parsed), None);
        assert_eq!(evidence.class, EvidenceClass::Incomplete);
        assert!(
            evidence
                .findings
                .iter()
                .any(|finding| finding
                    .contains("the core received 1 rows; the producer handed off 2"))
        );
    }

    #[test]
    fn named_identities_are_bounded_on_the_way_in() {
        let many: Vec<String> = (0..50)
            .map(|index| format!("c::c${}", "x".repeat(1_000) + &index.to_string()))
            .collect();
        let ids: Vec<&str> = many.iter().map(String::as_str).collect();
        let parsed = ingest(document(
            "full",
            false,
            rows(&[]),
            Some(accounting(
                vec![pass(Some(50), [0, 0, 0, 0], &ids, None)],
                [0, 0],
                [0, 0, 0, 0, 0],
            )),
        ));
        let pass = &parsed.accounting.as_ref().unwrap().passes[0];
        assert_eq!(pass.unresolved, 50);
        assert_eq!(pass.unresolved_ids.len(), NAMED_IDS);
        assert!(pass
            .unresolved_ids
            .iter()
            .all(|id| id.chars().count() <= NAME_CHARS));
    }
}
