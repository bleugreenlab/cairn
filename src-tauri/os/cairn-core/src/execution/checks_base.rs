//! Where a branch lane's failing tests stand on main at the branch's base.
//!
//! A branch's test lane runs on a tree that carries main's history up to the
//! branch's base, so a test main already fails at that base fails on the branch
//! too, whatever the branch changed. The per-test ledger already answers "does
//! this test fail on main": every whole-suite row on main's first-parent chain
//! has its tests in `check_test_results`, and the main cadence diffs its rows by
//! that ledger (see [`crate::execution::checks_main`]). This module asks the
//! same ledger on the branch's behalf, and reports what it observed, never what
//! caused it.
//!
//! Each failure a branch's test lane names is read against the nearest complete
//! whole-suite row at or before the base on the lane's own verdict platform.
//! Only a row whose own commit IS the base speaks for the base. A row on an
//! older commit observed a tree the base has since changed, and a cached alias
//! at the base names a reusable observation of another tree under a key that
//! does not yet fold every input its command reads (cairn/5883), so neither
//! answers for the base. Each failure lands in exactly one bucket:
//!
//! - **red at base**: the exact base's row failed it too.
//! - **failing differently**: the exact base's row failed it, with a different
//!   failure signature than the branch's.
//! - **intermittent on main**: the exact base's row passed it, but another of
//!   main's recent rows at or before the base failed it.
//! - **passed at base**: the exact base's row and every recent main row that ran
//!   it passed it. This is an observed difference between the base and the
//!   branch, not proof the branch caused it: the test may be flaky off main, or
//!   depend on the host.
//! - **base unobserved**: the nearest row is older than the base. What it said,
//!   passed or red, is kept, but nothing is charged on its word.
//! - **uncovered**: the row did not observe the test (it is new, or main
//!   skipped it), or no row exists at or before the base.
//!
//! Two reads qualify the buckets without moving a failure between them. An
//! exact-base row on another platform that failed an unanswered test is cited
//! as corroboration from that platform, never as this platform's verdict, and
//! its green is never reused. A test that passed at base and failed on other
//! jobs' recent rows of the same suite is named with those rows, so a reader can
//! tell a test that fails elsewhere from one that fails only here. A read that
//! fails leaves its answer unknown, and says so; it never reads as absence.
//!
//! A lane is attributed wherever its red is reported: from its stored row on
//! the wake, `/checks` and the settle resume, and from the outcome a write
//! wave just recorded on the inline summary of the committing tool result.
//! Every surface renders [`BaseAttribution::headline`] and
//! [`BaseAttribution::lines`], and the serialized form carries both.
//!
//! Only a branch cut from the project's default branch is attributed, and only
//! a test runner's lane (`nextest`, `vitest`, `bun-test`): a lint already
//! subtracts its own base, and a branch stacked on another branch has no main
//! row at its base.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use crate::execution::cache::{CheckResultCacheEntry, OBSERVED_INFRA_FAILURE_BOUND};
use crate::execution::check_parsers::{
    shown_failure_names, ParsedCheckResult, PLAN_AUDIT_FAILURE_PREFIX,
};
use crate::execution::checks::CheckOutcome;
use crate::execution::checks_main::{
    failure_signature, first_parent_ancestry, nearest_row_on_chain, rates_on_chain, CadenceRow,
    ExecutorNames, MainChain, TestOutcome, TestRate, PER_TEST_WHOLE_SUITE,
};
use crate::execution::checks_status::{row_existence, LaneExistence};
use crate::execution::checks_turn_end::{JobCoords, WavePlan};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use cairn_db::storage::RowExt;

/// One failure a branch lane named, with the signature of what its runner said.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BranchFailure {
    pub(crate) name: String,
    /// [`failure_signature`] of the runner's message, when it gave one.
    pub(crate) signature: Option<String>,
}

/// One branch lane's failing tests, partitioned against main at its base.
///
/// Every name the lane failed is in exactly one of `red_at_base`,
/// `failing_differently`, `green_at_base`, `intermittent_on_main`,
/// `base_unobserved` and `uncovered`. The rest qualifies those buckets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BaseAttribution {
    /// The branch's fork point on main.
    pub(crate) base: String,
    /// The lane's verdict platform, which every same-platform bucket is about.
    pub(crate) platform: String,
    /// The nearest complete whole-suite row at or before `base` on `platform`.
    /// `None` when main has none, and every failure is then uncovered.
    pub(crate) main_row: Option<MainRow>,
    /// Failed on the exact base's row too.
    pub(crate) red_at_base: Vec<String>,
    /// Failed on the exact base's row, with another signature than here.
    pub(crate) failing_differently: Vec<FailingDifferently>,
    /// Passed on the exact base's row and on every recent main row that ran
    /// it: red here, green there, which shows where the red appeared and not
    /// what caused it.
    pub(crate) green_at_base: Vec<String>,
    /// Passed on the exact base's row but failed on another of main's recent
    /// rows at or before the base.
    pub(crate) intermittent_on_main: Vec<RateOnMain>,
    /// Why main's recent rows could not be read for `green_at_base`: their
    /// intermittence is unknown.
    pub(crate) main_rate_unread: Option<String>,
    /// Observed only by a row older than the base: the base itself is
    /// unobserved on `platform`.
    pub(crate) base_unobserved: Vec<OlderStanding>,
    /// Not observed by `main_row`, or there is no `main_row`.
    pub(crate) uncovered: Vec<String>,
    /// Exact-base rows on other platforms that failed one of the tests this
    /// platform's row could not answer for the base.
    pub(crate) red_at_base_elsewhere: Vec<ElsewhereAtBase>,
    /// Why those rows could not be read.
    pub(crate) elsewhere_unread: Option<String>,
    /// Other jobs' recent rows of this suite that failed a `green_at_base`
    /// test, newest first.
    pub(crate) other_jobs: Vec<OtherJobFailure>,
    /// Why those rows could not be read.
    pub(crate) other_jobs_unread: Option<String>,
}

/// One test's rate of failure on main's recent rows at or before the base.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RateOnMain {
    pub(crate) name: String,
    /// Main rows in the window that failed it.
    pub(crate) red: usize,
    /// Main rows in the window that ran it.
    pub(crate) ran: usize,
}

/// The main row a [`BaseAttribution`] was read against.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MainRow {
    /// The commit the row's observation itself recorded: its actual source,
    /// whatever revision a reader asked for.
    pub(crate) commit: String,
    /// The row's canonical `check-observations` URI.
    pub(crate) observation: String,
    /// Whether `commit` is the base, the only row that speaks for it.
    pub(crate) at_base: bool,
    /// How many first-parent commits `commit` sits behind the base: 0 at the
    /// base. `None` when the base's chain does not place it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) commits_before_base: Option<usize>,
}

/// A test red at the exact base that fails another way here.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FailingDifferently {
    pub(crate) name: String,
    pub(crate) here: String,
    pub(crate) at_base: String,
}

/// What a row older than the base recorded for a test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum OlderOutcome {
    Passed,
    Failed,
}

/// A failure the base is unobserved for, with what the older row said.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OlderStanding {
    pub(crate) name: String,
    pub(crate) older_row: OlderOutcome,
}

/// The exact base's row on another platform, and which unanswered tests it
/// failed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ElsewhereAtBase {
    pub(crate) platform: String,
    pub(crate) observation: String,
    pub(crate) red: Vec<String>,
}

/// Another job's row of the same suite that failed a test that passed at base.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OtherJobFailure {
    pub(crate) name: String,
    pub(crate) observation: String,
    pub(crate) commit: String,
    pub(crate) platform: String,
    /// How the row was armed (`review`, `write`, `manual`, `main`).
    pub(crate) cadence: String,
    pub(crate) signature: String,
    /// Whether `signature` is the branch's own, when the branch named one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) same_failure: Option<bool>,
    /// The row's job, which only tells rows of one job from another's.
    #[serde(skip)]
    pub(crate) job: String,
}

/// What a line of [`BaseAttribution::lines`] is about, so a surface can weigh
/// it without parsing its text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum LineKind {
    GreenAtBase,
    FailingDifferently,
    BaseUnobserved,
    Uncovered,
    RedAtBase,
    Intermittent,
    Elsewhere,
    Unread,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AttributionLine {
    pub(crate) kind: LineKind,
    pub(crate) text: String,
}

impl serde::Serialize for BaseAttribution {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(serde::Serialize)]
        #[serde(rename_all = "camelCase")]
        struct View<'a> {
            base: &'a str,
            platform: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            main_row: Option<&'a MainRow>,
            red_at_base: &'a [String],
            failing_differently: &'a [FailingDifferently],
            green_at_base: &'a [String],
            intermittent_on_main: &'a [RateOnMain],
            #[serde(skip_serializing_if = "Option::is_none")]
            main_rate_unread: Option<&'a str>,
            base_unobserved: &'a [OlderStanding],
            uncovered: &'a [String],
            red_at_base_elsewhere: &'a [ElsewhereAtBase],
            #[serde(skip_serializing_if = "Option::is_none")]
            elsewhere_unread: Option<&'a str>,
            other_jobs: &'a [OtherJobFailure],
            #[serde(skip_serializing_if = "Option::is_none")]
            other_jobs_unread: Option<&'a str>,
            accounted_by_base: bool,
            headline: String,
            lines: Vec<AttributionLine>,
        }
        View {
            base: &self.base,
            platform: &self.platform,
            main_row: self.main_row.as_ref(),
            red_at_base: &self.red_at_base,
            failing_differently: &self.failing_differently,
            green_at_base: &self.green_at_base,
            intermittent_on_main: &self.intermittent_on_main,
            main_rate_unread: self.main_rate_unread.as_deref(),
            base_unobserved: &self.base_unobserved,
            uncovered: &self.uncovered,
            red_at_base_elsewhere: &self.red_at_base_elsewhere,
            elsewhere_unread: self.elsewhere_unread.as_deref(),
            other_jobs: &self.other_jobs,
            other_jobs_unread: self.other_jobs_unread.as_deref(),
            accounted_by_base: self.accounted_by_base(),
            headline: self.headline(),
            lines: self.lines(),
        }
        .serialize(serializer)
    }
}

/// What main's row recorded for one failure a branch names.
enum Observed {
    Red(Option<String>),
    Green,
}

/// Sort `failures` by what `main_row` recorded for each.
///
/// `commits_before_base` places `main_row` on the base's first-parent chain,
/// for display. Whether the row speaks for the base is not read from it: only
/// a row whose own commit is `base` does.
pub(crate) fn partition(
    base: &str,
    platform: &str,
    failures: &[BranchFailure],
    main_row: Option<&CadenceRow>,
    commits_before_base: Option<usize>,
) -> BaseAttribution {
    let at_base = main_row.is_some_and(|row| row.commit == base);
    let mut attribution = BaseAttribution {
        base: base.to_string(),
        platform: platform.to_string(),
        main_row: main_row.map(|row| MainRow {
            commit: row.commit.clone(),
            observation: row.reference.clone(),
            at_base,
            commits_before_base: if at_base {
                Some(0)
            } else {
                commits_before_base
            },
        }),
        red_at_base: Vec::new(),
        failing_differently: Vec::new(),
        green_at_base: Vec::new(),
        intermittent_on_main: Vec::new(),
        main_rate_unread: None,
        base_unobserved: Vec::new(),
        uncovered: Vec::new(),
        red_at_base_elsewhere: Vec::new(),
        elsewhere_unread: None,
        other_jobs: Vec::new(),
        other_jobs_unread: None,
    };
    for failure in failures {
        let name = failure.name.clone();
        let observed = main_row.and_then(|row| observed(row, &name));
        match (at_base, observed) {
            (_, None) => attribution.uncovered.push(name),
            (true, Some(Observed::Green)) => attribution.green_at_base.push(name),
            (true, Some(Observed::Red(at_base_signature))) => {
                match differing(failure.signature.as_deref(), at_base_signature.as_deref()) {
                    Some((here, there)) => {
                        attribution.failing_differently.push(FailingDifferently {
                            name,
                            here: here.to_string(),
                            at_base: there.to_string(),
                        })
                    }
                    None => attribution.red_at_base.push(name),
                }
            }
            (false, Some(observed)) => attribution.base_unobserved.push(OlderStanding {
                name,
                older_row: match observed {
                    Observed::Green => OlderOutcome::Passed,
                    Observed::Red(_) => OlderOutcome::Failed,
                },
            }),
        }
    }
    attribution
}

/// `None`: the row did not observe `name`. A test main skipped was not
/// observed there, any more than a test main does not have.
fn observed(row: &CadenceRow, name: &str) -> Option<Observed> {
    match name.strip_prefix(PLAN_AUDIT_FAILURE_PREFIX) {
        Some(finding) => audit_standing(row, finding),
        None => match row.tests.get(name)? {
            TestOutcome::Failed(failure) => {
                Some(Observed::Red(known_signature(&failure.signature)))
            }
            TestOutcome::Passed => Some(Observed::Green),
            TestOutcome::Skipped => None,
        },
    }
}

/// A plan-audit finding's standing on main's row. A row that evaluated its audit
/// observed every statement its run explained, so a finding it does not hold in
/// red is green there, whether absent or allowlisted. A row that did not
/// evaluate one observed nothing.
fn audit_standing(row: &CadenceRow, finding: &str) -> Option<Observed> {
    let audit = row.plan_audit.as_ref().filter(|audit| audit.evaluated)?;
    Some(if audit.gates(finding) {
        Observed::Red(None)
    } else {
        Observed::Green
    })
}

const NO_EXCERPT: &str = "(no failure excerpt)";

fn known_signature(signature: &str) -> Option<String> {
    (!signature.is_empty() && signature != NO_EXCERPT).then(|| signature.to_string())
}

/// The two signatures, when both are known and they differ.
fn differing<'a>(here: Option<&'a str>, there: Option<&'a str>) -> Option<(&'a str, &'a str)> {
    match (here, there) {
        (Some(here), Some(there)) if here != there => Some((here, there)),
        _ => None,
    }
}

fn short(commit: &str) -> &str {
    commit.get(..12).unwrap_or(commit)
}

fn plural(count: usize, one: &'static str, many: &'static str) -> &'static str {
    if count == 1 {
        one
    } else {
        many
    }
}

impl BaseAttribution {
    /// Move each test that passed at base but that main's recent rows failed
    /// (by `rates`, read over the base's chain) into `intermittent_on_main`.
    /// A rate that could not be read leaves every one where it is and the
    /// intermittence unknown, which the headline and lines say.
    pub(crate) fn with_main_rates(
        mut self,
        rates: Result<HashMap<String, TestRate>, String>,
    ) -> Self {
        let rates = match rates {
            Ok(rates) => rates,
            Err(error) => {
                if !self.green_at_base.is_empty() {
                    self.main_rate_unread = Some(error);
                }
                return self;
            }
        };
        let (intermittent, green): (Vec<String>, Vec<String>) =
            std::mem::take(&mut self.green_at_base)
                .into_iter()
                .partition(|name| rates.get(name).is_some_and(|rate| rate.red > 0));
        self.green_at_base = green;
        self.intermittent_on_main = intermittent
            .into_iter()
            .map(|name| {
                let rate = rates[&name];
                RateOnMain {
                    name,
                    red: rate.red,
                    ran: rate.ran,
                }
            })
            .collect();
        self
    }

    /// Cite exact-base reds on other platforms, or why they were not read.
    pub(crate) fn with_elsewhere(
        mut self,
        elsewhere: Result<Vec<ElsewhereAtBase>, String>,
    ) -> Self {
        match elsewhere {
            Ok(rows) => self.red_at_base_elsewhere = rows,
            Err(error) => self.elsewhere_unread = Some(error),
        }
        self
    }

    /// Cite other jobs' failures of the tests that passed at base, or why
    /// they were not read.
    pub(crate) fn with_other_jobs(mut self, other: Result<Vec<OtherJobFailure>, String>) -> Self {
        match other {
            Ok(rows) => self.other_jobs = rows,
            Err(error) => self.other_jobs_unread = Some(error),
        }
        self
    }

    /// The failures this platform's row could not answer for the base itself.
    pub(crate) fn unanswered_at_base(&self) -> Vec<String> {
        self.base_unobserved
            .iter()
            .map(|standing| standing.name.clone())
            .chain(self.uncovered.iter().cloned())
            .collect()
    }

    /// Whether the exact base's row accounts for every failure: each was red
    /// there too, or is intermittent on main. A lane accounted for by its base
    /// is still red; a surface just does not count it as failing the branch.
    pub(crate) fn accounted_by_base(&self) -> bool {
        self.green_at_base.is_empty()
            && self.base_unobserved.is_empty()
            && self.uncovered.is_empty()
    }

    /// What the comparison found, as a clause that follows the lane's own
    /// tally (`3 of 900 failed; 1 passed at base and failed here, 2 red at base
    /// too`). Every bucket that holds a failure is counted, so a count that was
    /// not checked is never folded into one that was.
    pub(crate) fn headline(&self) -> String {
        let mut parts = Vec::new();
        let count = |parts: &mut Vec<String>, n: usize, text: String| {
            if n > 0 {
                parts.push(format!("{n} {text}"));
            }
        };
        count(
            &mut parts,
            self.green_at_base.len(),
            match self.main_rate_unread {
                Some(_) => "passed at base and failed here (main's recent rate unread)".to_string(),
                None => "passed at base and failed here".to_string(),
            },
        );
        count(
            &mut parts,
            self.failing_differently.len(),
            "red at base but failing differently".to_string(),
        );
        count(
            &mut parts,
            self.base_unobserved.len(),
            format!("unattributed (base unobserved on {})", self.platform),
        );
        count(
            &mut parts,
            self.uncovered.len(),
            "not observed on main".to_string(),
        );
        count(
            &mut parts,
            self.red_at_base.len(),
            "red at base too".to_string(),
        );
        count(
            &mut parts,
            self.intermittent_on_main.len(),
            "intermittent on main".to_string(),
        );
        parts.join(", ")
    }

    /// Where the row sits, said once per line.
    fn row_at(&self, row: &MainRow) -> String {
        if row.at_base {
            format!(
                "base `{}` on {} ({})",
                short(&self.base),
                self.platform,
                row.observation
            )
        } else {
            format!(
                "`{}` on {} ({})",
                short(&row.commit),
                self.platform,
                row.observation
            )
        }
    }

    /// One line per non-empty bucket and per qualifying read, each naming its
    /// tests, then each read that failed.
    ///
    /// Every name the reader may have to act on is listed. Only the base's
    /// reds are capped, and their remainder points at the main row, which
    /// holds every one of them by name. A capped list is never an unmarked
    /// prefix.
    pub(crate) fn lines(&self) -> Vec<AttributionLine> {
        let mut lines = Vec::new();
        let mut push = |kind, text| lines.push(AttributionLine { kind, text });
        let names = |tests: &[String]| tests.join(", ");
        match &self.main_row {
            Some(row) if row.at_base => {
                let at = self.row_at(row);
                if !self.green_at_base.is_empty() {
                    let named: Vec<String> = self
                        .green_at_base
                        .iter()
                        .map(|name| self.with_other_jobs_named(name))
                        .collect();
                    push(
                        LineKind::GreenAtBase,
                        format!(
                            "{} passed at {at} and failed here, an observed difference and not \
                             proof of cause: {}",
                            self.green_at_base.len(),
                            named.join(", ")
                        ),
                    );
                }
                if !self.failing_differently.is_empty() {
                    let named: Vec<String> = self
                        .failing_differently
                        .iter()
                        .map(|test| {
                            format!(
                                "{} (here: `{}`; at base: `{}`)",
                                test.name, test.here, test.at_base
                            )
                        })
                        .collect();
                    push(
                        LineKind::FailingDifferently,
                        format!(
                            "{} red at {at} but failing differently here: {}",
                            self.failing_differently.len(),
                            named.join(", ")
                        ),
                    );
                }
                if !self.red_at_base.is_empty() {
                    let (shown, capped) = shown_failure_names(&self.red_at_base);
                    let more = match capped {
                        0 => String::new(),
                        more => format!(", +{more} more, every one red on {}", row.observation),
                    };
                    push(
                        LineKind::RedAtBase,
                        format!(
                            "{} also red at {at}: {}{more}",
                            self.red_at_base.len(),
                            shown.join(", ")
                        ),
                    );
                }
                if !self.intermittent_on_main.is_empty() {
                    let rated: Vec<String> = self
                        .intermittent_on_main
                        .iter()
                        .map(|rate| {
                            format!(
                                "{} (red on {} of the last {} main rows that ran it)",
                                rate.name, rate.red, rate.ran
                            )
                        })
                        .collect();
                    push(
                        LineKind::Intermittent,
                        format!(
                            "{} passed at {at} but red on recent main rows (intermittent on \
                             main): {}",
                            self.intermittent_on_main.len(),
                            rated.join(", ")
                        ),
                    );
                }
                if !self.uncovered.is_empty() {
                    push(
                        LineKind::Uncovered,
                        format!(
                            "{} not observed by the row at {at} (new, or skipped there): {}",
                            self.uncovered.len(),
                            names(&self.uncovered)
                        ),
                    );
                }
            }
            Some(row) => {
                if !self.base_unobserved.is_empty() {
                    let distance = match row.commits_before_base {
                        Some(1) => "1 commit older".to_string(),
                        Some(commits) => format!("{commits} commits older"),
                        None => "older".to_string(),
                    };
                    let said = |outcome: OlderOutcome| -> Vec<String> {
                        self.base_unobserved
                            .iter()
                            .filter(|standing| standing.older_row == outcome)
                            .map(|standing| standing.name.clone())
                            .collect()
                    };
                    let mut there = Vec::new();
                    for (outcome, label) in [
                        (OlderOutcome::Passed, "passed there"),
                        (OlderOutcome::Failed, "red there"),
                    ] {
                        let tests = said(outcome);
                        if !tests.is_empty() {
                            there.push(format!("{label}: {}", names(&tests)));
                        }
                    }
                    push(
                        LineKind::BaseUnobserved,
                        format!(
                            "{} unattributed: no complete row observed base `{}` on {}; the \
                             nearest, {}, is {distance} and says nothing about the changes \
                             since; {}",
                            self.base_unobserved.len(),
                            short(&self.base),
                            self.platform,
                            self.row_at(row),
                            there.join("; ")
                        ),
                    );
                }
                if !self.uncovered.is_empty() {
                    push(
                        LineKind::Uncovered,
                        format!(
                            "{} not observed by the nearest row, {} (new, or skipped there): {}",
                            self.uncovered.len(),
                            self.row_at(row),
                            names(&self.uncovered)
                        ),
                    );
                }
            }
            None => {
                if !self.uncovered.is_empty() {
                    push(
                        LineKind::Uncovered,
                        format!(
                            "{} with no main row at or before base `{}` on {}: {}",
                            self.uncovered.len(),
                            short(&self.base),
                            self.platform,
                            names(&self.uncovered)
                        ),
                    );
                }
            }
        }
        for elsewhere in &self.red_at_base_elsewhere {
            push(
                LineKind::Elsewhere,
                format!(
                    "{} red at exact base `{}` on {} ({}), another platform's row and not a {} \
                     verdict: {}",
                    elsewhere.red.len(),
                    short(&self.base),
                    elsewhere.platform,
                    elsewhere.observation,
                    self.platform,
                    names(&elsewhere.red)
                ),
            );
        }
        if let Some(error) = &self.main_rate_unread {
            push(
                LineKind::Unread,
                format!(
                    "main's recent rows could not be read for the {} that passed at base, so \
                     whether main fails them now and then is unknown: {error}",
                    self.green_at_base.len()
                ),
            );
        }
        if let Some(error) = &self.elsewhere_unread {
            push(
                LineKind::Unread,
                format!("exact-base rows on other platforms could not be read: {error}"),
            );
        }
        if let Some(error) = &self.other_jobs_unread {
            push(
                LineKind::Unread,
                format!(
                    "other jobs' recent rows could not be read, so whether the tests that passed \
                     at base fail elsewhere is unknown: {error}"
                ),
            );
        }
        lines
    }

    /// `name`, followed by the other jobs' rows that failed it, at most
    /// [`OTHER_JOBS_SHOWN`] of them and the rest counted.
    fn with_other_jobs_named(&self, name: &str) -> String {
        let rows: Vec<&OtherJobFailure> = self
            .other_jobs
            .iter()
            .filter(|row| row.name == name)
            .collect();
        if rows.is_empty() {
            return name.to_string();
        }
        let jobs = rows
            .iter()
            .map(|row| row.job.as_str())
            .collect::<HashSet<_>>()
            .len();
        let cited: Vec<String> = rows
            .iter()
            .take(OTHER_JOBS_SHOWN)
            .map(|row| {
                let failure = match row.same_failure {
                    Some(true) => "the same failure".to_string(),
                    _ => format!("`{}`", row.signature),
                };
                format!(
                    "{} at `{}` on {}, a {} row, {failure}",
                    row.observation,
                    short(&row.commit),
                    row.platform,
                    row.cadence
                )
            })
            .collect();
        let more = match rows.len().saturating_sub(OTHER_JOBS_SHOWN) {
            0 => String::new(),
            more => format!("; +{more} more"),
        };
        format!(
            "{name} (also red on {} {} of {jobs} other {}: {}{more})",
            rows.len(),
            plural(rows.len(), "row", "rows"),
            plural(jobs, "job", "jobs"),
            cited.join("; ")
        )
    }

    /// [`Self::lines`] as text, for a surface that renders no kinds.
    pub(crate) fn line_texts(&self) -> Vec<String> {
        self.lines().into_iter().map(|line| line.text).collect()
    }
}

/// The failing tests a lane's row names, when its row is a test runner's
/// complete red: the only rows a per-test ledger can answer.
pub(crate) fn failing_tests(row: &CheckResultCacheEntry) -> Option<Vec<BranchFailure>> {
    let parsed: Option<ParsedCheckResult> = row
        .target_results_json
        .as_deref()
        .and_then(|json| serde_json::from_str(json).ok());
    complete_red_failures(
        row.passed,
        row.failure_kind.is_some(),
        (row.infra_failure_streak >= OBSERVED_INFRA_FAILURE_BOUND)
            .then_some(row.infra_failure_streak),
        parsed.as_ref(),
    )
}

/// [`failing_tests`] for the outcome a write wave just evaluated.
pub(crate) fn failing_outcome_tests(outcome: &CheckOutcome) -> Option<Vec<BranchFailure>> {
    complete_red_failures(
        outcome.passed,
        outcome.failure_kind.is_some(),
        outcome.suppressed_after,
        outcome.parsed.as_ref(),
    )
}

fn complete_red_failures(
    passed: bool,
    died: bool,
    breaker_after: Option<i64>,
    parsed: Option<&ParsedCheckResult>,
) -> Option<Vec<BranchFailure>> {
    if passed || died {
        return None;
    }
    let parsed = parsed?;
    let existence = row_existence(false, None, breaker_after, Some(&parsed.verdict_facts()));
    if !matches!(existence, LaneExistence::Complete)
        || !parsed.is_test_runner()
        || parsed.failures.is_empty()
    {
        return None;
    }
    // The per-test message is what the ledger stores as a row's excerpt, so
    // it is what a main row's signature is read from too.
    let messages: HashMap<&str, &str> = parsed
        .tests
        .iter()
        .filter(|test| test.status == "failed")
        .filter_map(|test| Some((test.id.as_str(), test.failure_message.as_deref()?)))
        .collect();
    Some(
        parsed
            .failures
            .iter()
            .map(|failure| BranchFailure {
                name: failure.name.clone(),
                signature: messages
                    .get(failure.name.as_str())
                    .copied()
                    .or(failure.message.as_deref())
                    .and_then(|message| known_signature(&failure_signature(message))),
            })
            .collect(),
    )
}

/// The base the job's last review wave planned against, for a surface that
/// reads verdicts without resolving the branch.
pub(crate) async fn recorded_base(db: &LocalDb, job_id: &str) -> Option<String> {
    let plan = db
        .query_opt_text(
            "SELECT plan_json FROM turn_end_check_attempts
              WHERE job_id = ?1 AND plan_json IS NOT NULL
              ORDER BY created_at DESC, id DESC LIMIT 1",
            (job_id.to_string(),),
        )
        .await
        .map_err(|error| log::debug!("node {job_id}: recorded wave plan unreadable: {error}"))
        .ok()
        .flatten()?;
    serde_json::from_str::<WavePlan>(&plan)
        .ok()
        .map(|plan| plan.base)
        .filter(|base| !base.is_empty())
}

/// How long an attribution is reused. A main row recorded since can change the
/// answer, and a minute is well inside how often main records one; inside it,
/// a live `/checks` poll reads no ledger.
const ATTRIBUTION_TTL: Duration = Duration::from_secs(60);
const MEMO_LIMIT: usize = 128;

/// Project, check, platform, base, job, and the failing tests.
type AttributionKey = (String, String, String, String, String, Vec<BranchFailure>);
type AttributionMemo = Mutex<HashMap<AttributionKey, (Instant, Option<BaseAttribution>)>>;

static ATTRIBUTIONS: LazyLock<AttributionMemo> = LazyLock::new(Default::default);

/// Repository path and base commit.
type ChainKey = (String, String);
type ChainMemo = Mutex<HashMap<ChainKey, Arc<MainChain>>>;

/// A commit's first-parent ancestry never changes, so a base's chain is read
/// once.
static CHAINS: LazyLock<ChainMemo> = LazyLock::new(Default::default);

fn remember<K: std::hash::Hash + Eq, V>(memo: &Mutex<HashMap<K, V>>, key: K, value: V) {
    let mut memo = memo.lock().unwrap();
    if memo.len() >= MEMO_LIMIT {
        memo.clear();
    }
    memo.insert(key, value);
}

/// Every failing test lane in `rows`, attributed against main at `base`, by
/// check name. A lane missing from the answer is shown as it always was: one
/// that is not a test runner's red, a branch not cut from the default branch,
/// a base outside main's recent history, or a ledger that could not be read.
pub(crate) async fn attribute_lanes<'a>(
    orch: &Orchestrator,
    db: &LocalDb,
    coords: &JobCoords,
    job_id: &str,
    base: &str,
    rows: impl IntoIterator<Item = &'a CheckResultCacheEntry>,
) -> HashMap<String, BaseAttribution> {
    let failing = rows
        .into_iter()
        .filter_map(|row| {
            failing_tests(row).map(|failures| FailingLane {
                check: &row.check_name,
                failures,
                verdict: VerdictSource::Row(row),
            })
        })
        .collect();
    Box::pin(attribute_failing(orch, db, coords, job_id, base, failing)).await
}

/// [`attribute_lanes`] for the outcomes a write wave just evaluated. An
/// outcome that recorded no observation has no verdict platform to read, and
/// is left unattributed.
pub(crate) async fn attribute_outcomes(
    orch: &Orchestrator,
    db: &LocalDb,
    coords: &JobCoords,
    job_id: &str,
    base: &str,
    outcomes: &[CheckOutcome],
) -> HashMap<String, BaseAttribution> {
    let failing = outcomes
        .iter()
        .filter_map(|outcome| {
            let recorded = outcome.recorded.as_ref()?;
            failing_outcome_tests(outcome).map(|failures| FailingLane {
                check: &outcome.name,
                failures,
                verdict: VerdictSource::Observation(&recorded.id),
            })
        })
        .collect();
    Box::pin(attribute_failing(orch, db, coords, job_id, base, failing)).await
}

/// One red test lane to attribute.
struct FailingLane<'a> {
    check: &'a str,
    failures: Vec<BranchFailure>,
    verdict: VerdictSource<'a>,
}

/// What names the observation a lane's verdict platform is read from.
enum VerdictSource<'a> {
    /// A stored lane row, by the inputs it was recorded for.
    Row(&'a CheckResultCacheEntry),
    /// The observation a fresh evaluation recorded.
    Observation(&'a str),
}

/// Called boxed, so a caller's future does not embed this chain's layout: the
/// `/checks` read reaches here from the page read path, whose future already
/// sits at rustc's query depth limit, and this chain inline overflows it.
async fn attribute_failing(
    orch: &Orchestrator,
    db: &LocalDb,
    coords: &JobCoords,
    job_id: &str,
    base: &str,
    failing: Vec<FailingLane<'_>>,
) -> HashMap<String, BaseAttribution> {
    let mut attributions = HashMap::new();
    if failing.is_empty() || base.is_empty() {
        return attributions;
    }
    match cut_from_default_branch(db, coords).await {
        Ok(true) => {}
        Ok(false) => return attributions,
        Err(error) => {
            log::debug!("base attribution: default branch unreadable: {error}");
            return attributions;
        }
    }
    let chain = match base_chain(orch, &coords.repository_path, base).await {
        Ok(chain) if !chain.is_empty() => chain,
        Ok(_) => return attributions,
        Err(error) => {
            log::debug!("base attribution: main's chain at {base} unreadable: {error}");
            return attributions;
        }
    };
    let names = |executor: &str| orch.fleet.executor_name_for_text(executor);
    for lane in failing {
        let check = lane.check.to_string();
        match attribute_lane(db, coords, job_id, base, &chain, lane, &names).await {
            Ok(Some(attribution)) => {
                attributions.insert(check, attribution);
            }
            Ok(None) => {}
            Err(error) => log::debug!("base attribution for {check} at {base}: {error}"),
        }
    }
    attributions
}

async fn attribute_lane(
    db: &LocalDb,
    coords: &JobCoords,
    job_id: &str,
    base: &str,
    chain: &MainChain,
    lane: FailingLane<'_>,
    names: ExecutorNames<'_>,
) -> Result<Option<BaseAttribution>, String> {
    let Some(platform) =
        verdict_platform(db, &coords.project_id, lane.check, &lane.verdict).await?
    else {
        return Ok(None);
    };
    let key = (
        coords.project_id.clone(),
        lane.check.to_string(),
        platform.clone(),
        base.to_string(),
        job_id.to_string(),
        lane.failures,
    );
    if let Some((at, attribution)) = ATTRIBUTIONS.lock().unwrap().get(&key) {
        if at.elapsed() < ATTRIBUTION_TTL {
            return Ok(attribution.clone());
        }
    }
    let attribution = attribute_on_chain(
        db,
        Ledger {
            project_id: &coords.project_id,
            project_key: &coords.project_key,
            job_id,
            check: &key.1,
            platform: &platform,
        },
        base,
        chain,
        &key.5,
        names,
    )
    .await?;
    let attribution = Some(attribution);
    remember(&ATTRIBUTIONS, key, (Instant::now(), attribution.clone()));
    Ok(attribution)
}

/// Whose ledger one lane is read from.
#[derive(Clone, Copy)]
struct Ledger<'a> {
    project_id: &'a str,
    project_key: &'a str,
    /// The branch's own job, whose rows are never another job's corroboration.
    job_id: &'a str,
    check: &'a str,
    platform: &'a str,
}

/// Every read one lane's attribution makes, each bounded: the nearest row
/// (whose candidate read is capped), the rate window, the newest exact-base row
/// per other platform, and other jobs' newest failed rows, each probed by
/// `(observation_id, test_id)` for the named tests only. The nearest row's
/// read failing leaves the lane unattributed; any later read failing leaves
/// its own answer unknown.
async fn attribute_on_chain(
    db: &LocalDb,
    ledger: Ledger<'_>,
    base: &str,
    chain: &MainChain,
    failures: &[BranchFailure],
    names: ExecutorNames<'_>,
) -> Result<BaseAttribution, String> {
    let main_row = nearest_row_on_chain(
        db,
        ledger.project_id,
        ledger.project_key,
        ledger.check,
        ledger.platform,
        chain,
        names,
    )
    .await?;
    let depth = main_row.as_ref().and_then(|row| chain.depth(&row.commit));
    let mut attribution = partition(base, ledger.platform, failures, main_row.as_ref(), depth);
    if !attribution.green_at_base.is_empty() {
        let rates = rates_on_chain(
            db,
            ledger.project_id,
            ledger.check,
            ledger.platform,
            chain,
            &attribution.green_at_base,
        )
        .await;
        attribution = attribution.with_main_rates(rates);
    }
    let unanswered = attribution.unanswered_at_base();
    if !unanswered.is_empty() {
        let elsewhere = red_at_base_elsewhere(db, ledger, base, &unanswered).await;
        attribution = attribution.with_elsewhere(elsewhere);
    }
    if !attribution.green_at_base.is_empty() {
        let signatures: HashMap<&str, &str> = failures
            .iter()
            .filter_map(|failure| Some((failure.name.as_str(), failure.signature.as_deref()?)))
            .collect();
        let other =
            failures_on_other_jobs(db, ledger, chain, &attribution.green_at_base, &signatures)
                .await;
        attribution = attribution.with_other_jobs(other);
    }
    Ok(attribution)
}

/// How many exact-base rows on other platforms are read, newest first; the
/// newest of each platform answers for it.
const ELSEWHERE_SCAN: i64 = 16;

/// How many of other jobs' failed rows of a suite are read, newest first.
const OTHER_JOBS_SCAN: i64 = 32;

/// How many of those rows a test's line cites before counting the rest.
const OTHER_JOBS_SHOWN: usize = 3;

fn observation_uri(project_key: &str, handle: &str) -> String {
    format!("cairn://p/{project_key}/check-observations/{handle}")
}

/// The newest complete whole-suite row at exactly `base` on each platform
/// other than the lane's, and which of `tests` each failed. Seeks the
/// `(project_id, commit_sha, check_name)` index; a platform whose row failed
/// none of them is not cited, and its greens are never read.
async fn red_at_base_elsewhere(
    db: &LocalDb,
    ledger: Ledger<'_>,
    base: &str,
    tests: &[String],
) -> Result<Vec<ElsewhereAtBase>, String> {
    let rows = db
        .query_all(
            format!(
                "SELECT id, COALESCE(public_handle, id), LOWER(TRIM(verdict_platform))
                   FROM check_result_observations
                  WHERE project_id = ?1 AND commit_sha = ?2 AND check_name = ?3
                    AND complete = 1 AND failure_kind IS NULL AND {PER_TEST_WHOLE_SUITE}
                    AND COALESCE(verdict_platform, '') != ''
                    AND LOWER(TRIM(verdict_platform)) != ?4
                  ORDER BY ran_at DESC LIMIT {ELSEWHERE_SCAN}"
            ),
            (
                ledger.project_id.to_string(),
                base.to_string(),
                ledger.check.to_string(),
                ledger.platform.to_string(),
            ),
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut platforms = HashSet::new();
    let mut elsewhere = Vec::new();
    for (id, handle, platform) in rows {
        if !platforms.insert(platform.clone()) {
            continue;
        }
        let red: Vec<String> = failed_among(db, std::slice::from_ref(&id), tests)
            .await?
            .into_iter()
            .map(|failed| failed.test)
            .collect();
        if !red.is_empty() {
            elsewhere.push(ElsewhereAtBase {
                platform,
                observation: observation_uri(ledger.project_key, &handle),
                red,
            });
        }
    }
    Ok(elsewhere)
}

/// Other jobs' newest failed rows of the lane's suite, on any platform and
/// tree, that failed one of `tests`, newest first. A row on the base's chain
/// is main's, already read by the rate window; the branch's own rows are never
/// another job's.
async fn failures_on_other_jobs(
    db: &LocalDb,
    ledger: Ledger<'_>,
    chain: &MainChain,
    tests: &[String],
    signatures: &HashMap<&str, &str>,
) -> Result<Vec<OtherJobFailure>, String> {
    struct Candidate {
        handle: String,
        commit: String,
        platform: String,
        cadence: String,
        job: String,
    }
    let rows = db
        .query_all(
            format!(
                "SELECT id, COALESCE(public_handle, id), commit_sha,
                        LOWER(TRIM(COALESCE(verdict_platform, ''))), cadence, COALESCE(job_id, '')
                   FROM check_result_observations
                  WHERE project_id = ?1 AND check_name = ?2 AND verdict = 'failed'
                    AND complete = 1 AND failure_kind IS NULL AND COALESCE(job_id, '') != ?3
                  ORDER BY ran_at DESC LIMIT {OTHER_JOBS_SCAN}"
            ),
            (
                ledger.project_id.to_string(),
                ledger.check.to_string(),
                ledger.job_id.to_string(),
            ),
            |row| {
                Ok((
                    row.text(0)?,
                    Candidate {
                        handle: row.text(1)?,
                        commit: row.text(2)?,
                        platform: row.text(3)?,
                        cadence: row.text(4)?,
                        job: row.text(5)?,
                    },
                ))
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    let candidates: Vec<(String, Candidate)> = rows
        .into_iter()
        .filter(|(_, candidate)| chain.depth(&candidate.commit).is_none())
        .collect();
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<String> = candidates.iter().map(|(id, _)| id.clone()).collect();
    let mut failed: HashMap<String, Vec<Failed>> = HashMap::new();
    for row in failed_among(db, &ids, tests).await? {
        failed.entry(row.observation.clone()).or_default().push(row);
    }
    let mut found = Vec::new();
    for (id, candidate) in &candidates {
        let Some(rows) = failed.remove(id) else {
            continue;
        };
        // In `tests` order, so one row's failures read as the lane named them.
        let by_test: BTreeMap<usize, Failed> = rows
            .into_iter()
            .filter_map(|row| Some((tests.iter().position(|test| *test == row.test)?, row)))
            .collect();
        for row in by_test.into_values() {
            let signature = row
                .excerpt
                .as_deref()
                .map(failure_signature)
                .unwrap_or_else(|| NO_EXCERPT.to_string());
            let same_failure = known_signature(&signature).and_then(|signature| {
                signatures
                    .get(row.test.as_str())
                    .map(|here| *here == signature)
            });
            found.push(OtherJobFailure {
                name: row.test,
                observation: observation_uri(ledger.project_key, &candidate.handle),
                commit: candidate.commit.clone(),
                platform: candidate.platform.clone(),
                cadence: candidate.cadence.clone(),
                signature,
                same_failure,
                job: candidate.job.clone(),
            });
        }
    }
    Ok(found)
}

/// One failed per-test row.
struct Failed {
    observation: String,
    test: String,
    excerpt: Option<String>,
}

/// Which of `tests` each of `observations` failed, probed by the
/// `(observation_id, test_id)` key.
async fn failed_among(
    db: &LocalDb,
    observations: &[String],
    tests: &[String],
) -> Result<Vec<Failed>, String> {
    use cairn_db::sql::{placeholders, IN_LIST_CHUNK};
    use cairn_db::turso::Value as SqlValue;

    let mut failed = Vec::new();
    if observations.is_empty() || tests.is_empty() {
        return Ok(failed);
    }
    for chunk in tests.chunks(IN_LIST_CHUNK - observations.len()) {
        let params: Vec<SqlValue> = observations
            .iter()
            .chain(chunk)
            .cloned()
            .map(SqlValue::Text)
            .collect();
        let rows = db
            .query_all(
                format!(
                    "SELECT observation_id, test_id, failure_excerpt FROM check_test_results
                      WHERE observation_id IN ({}) AND test_id IN ({}) AND status = 'failed'",
                    placeholders(observations.len()),
                    placeholders(chunk.len())
                ),
                params,
                |row| {
                    Ok(Failed {
                        observation: row.text(0)?,
                        test: row.text(1)?,
                        excerpt: row.opt_text(2)?,
                    })
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        failed.extend(rows);
    }
    Ok(failed)
}

/// The platform a lane's verdict is about, read from the observation that
/// produced it. `None` for an observation that recorded none.
async fn verdict_platform(
    db: &LocalDb,
    project_id: &str,
    check: &str,
    source: &VerdictSource<'_>,
) -> Result<Option<String>, String> {
    let platform = match source {
        VerdictSource::Row(row) => {
            db.query_opt_text(
                "SELECT verdict_platform FROM check_result_observations
                  WHERE project_id = ?1 AND check_name = ?2 AND input_hash = ?3
                    AND tree_hash = ?4 AND COALESCE(verdict_platform, '') != ''
                  ORDER BY ran_at DESC LIMIT 1",
                (
                    project_id.to_string(),
                    check.to_string(),
                    row.input_hash.clone(),
                    row.tree_hash.clone(),
                ),
            )
            .await
        }
        VerdictSource::Observation(id) => {
            db.query_opt_text(
                "SELECT verdict_platform FROM check_result_observations
                  WHERE id = ?1 AND COALESCE(verdict_platform, '') != ''",
                (id.to_string(),),
            )
            .await
        }
    };
    platform
        .map(|platform| platform.map(|platform| platform.trim().to_ascii_lowercase()))
        .map_err(|error| error.to_string())
}

/// Whether the job's base branch is the project's default branch, the only
/// branch the main ledger observes.
async fn cut_from_default_branch(db: &LocalDb, coords: &JobCoords) -> Result<bool, String> {
    let default_branch = db
        .query_opt_text(
            "SELECT COALESCE(default_branch, '') FROM projects WHERE id = ?1",
            (coords.project_id.clone(),),
        )
        .await
        .map_err(|error| error.to_string())?
        .filter(|branch| !branch.is_empty());
    Ok(match (default_branch, coords.base_branch.as_deref()) {
        (None, _) => false,
        (Some(_), None) => true,
        (Some(default_branch), Some(base_branch)) => default_branch == base_branch,
    })
}

async fn base_chain(
    orch: &Orchestrator,
    repository_path: &str,
    base: &str,
) -> Result<Arc<MainChain>, String> {
    let key = (repository_path.to_string(), base.to_string());
    if let Some(chain) = CHAINS.lock().unwrap().get(&key) {
        return Ok(chain.clone());
    }
    let jj_binary_path = orch.jj_binary_path.clone();
    let config_dir = orch.config_dir.clone();
    let (project_repo, commit) = key.clone();
    let chain = tokio::task::spawn_blocking(move || {
        let jj = crate::jj::environment(&jj_binary_path, &config_dir);
        let repository =
            crate::jj::coordinate_repository(&jj, &config_dir, Path::new(&project_repo))?;
        first_parent_ancestry(&jj, &repository, &commit)
    })
    .await
    .map_err(|error| format!("first-parent walk task failed: {error}"))??;
    let chain = Arc::new(chain);
    remember(&CHAINS, key, chain.clone());
    Ok(chain)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::execution::checks_main::Failure;

    pub(crate) fn main_row(commit: &str, tests: &[(&str, &str)]) -> CadenceRow {
        CadenceRow {
            id: "obs-main".to_string(),
            reference: "cairn://p/cairn/check-observations/e884003b".to_string(),
            commit: commit.to_string(),
            cadence: "main".to_string(),
            platform: "linux".to_string(),
            executor_id: None,
            executor: None,
            ran_at_ms: 1,
            verdict_passed: false,
            exit_code: 100,
            complete: true,
            selection: Some("full".to_string()),
            tests: tests
                .iter()
                .map(|(test, status)| {
                    let outcome = match *status {
                        "passed" => TestOutcome::Passed,
                        "skipped" => TestOutcome::Skipped,
                        _ => TestOutcome::Failed(Failure {
                            signature: "boom".to_string(),
                            host_condition: None,
                            ending: None,
                            span: None,
                        }),
                    };
                    (test.to_string(), outcome)
                })
                .collect::<BTreeMap<_, _>>(),
            output_tail: String::new(),
            lane_started_at_ms: None,
            plan_audit: None,
        }
    }

    /// Failures with no runner message, as nextest names them.
    pub(crate) fn failures(tests: &[&str]) -> Vec<BranchFailure> {
        tests
            .iter()
            .map(|test| BranchFailure {
                name: test.to_string(),
                signature: None,
            })
            .collect()
    }

    fn names(tests: &[&str]) -> Vec<String> {
        tests.iter().map(|test| test.to_string()).collect()
    }

    fn texts(attribution: &BaseAttribution) -> Vec<String> {
        attribution.line_texts()
    }

    const BASE: &str = "4a52d52e0000";

    /// At the exact base each failure lands by what the base's row recorded:
    /// red there is red at base, green there passed at base, and a test the
    /// row skipped or never had is uncovered. Nothing is "the branch's".
    #[test]
    fn at_the_exact_base_each_failure_lands_by_what_the_row_recorded() {
        let row = main_row(
            BASE,
            &[
                ("core red", "failed"),
                ("core b", "passed"),
                ("core skipped", "skipped"),
            ],
        );
        let attribution = partition(
            BASE,
            "linux",
            &failures(&["core red", "core b", "core skipped", "core new"]),
            Some(&row),
            Some(0),
        );
        assert_eq!(attribution.red_at_base, names(&["core red"]));
        assert_eq!(attribution.green_at_base, names(&["core b"]));
        assert_eq!(attribution.uncovered, names(&["core skipped", "core new"]));
        assert!(attribution.base_unobserved.is_empty());
        assert!(!attribution.accounted_by_base());
        assert_eq!(
            attribution.headline(),
            "1 passed at base and failed here, 2 not observed on main, 1 red at base too"
        );
        let at = "base `4a52d52e0000` on linux (cairn://p/cairn/check-observations/e884003b)";
        assert_eq!(
            texts(&attribution),
            vec![
                format!(
                    "1 passed at {at} and failed here, an observed difference and not proof of \
                     cause: core b"
                ),
                format!("1 also red at {at}: core red"),
                format!("2 not observed by the row at {at} (new, or skipped there): core skipped, core new"),
            ]
        );
        for line in texts(&attribution) {
            assert!(!line.contains("this branch's"), "{line}");
        }

        let only_reds = partition(BASE, "linux", &failures(&["core red"]), Some(&row), Some(0));
        assert!(only_reds.accounted_by_base());
        assert_eq!(only_reds.headline(), "1 red at base too");
    }

    /// cairn/6168: a row 256 commits before the base passed (or failed) a test.
    /// That says nothing about the commits since, so neither result is charged
    /// to anyone: each is unattributed, the base named as unobserved on this
    /// platform, with the older row, its commit and the gap, and what it said.
    #[test]
    fn a_row_older_than_the_base_charges_nothing_and_names_the_gap() {
        let row = main_row(
            "79401a191cd5aaaa",
            &[("core passed", "passed"), ("core red", "failed")],
        );
        let attribution = partition(
            "7b8e6c41f471bbbb",
            "macos",
            &failures(&["core passed", "core red", "core new"]),
            Some(&row),
            Some(256),
        );
        assert!(attribution.green_at_base.is_empty());
        assert!(attribution.red_at_base.is_empty());
        assert!(attribution.intermittent_on_main.is_empty());
        assert_eq!(
            attribution.base_unobserved,
            vec![
                OlderStanding {
                    name: "core passed".to_string(),
                    older_row: OlderOutcome::Passed
                },
                OlderStanding {
                    name: "core red".to_string(),
                    older_row: OlderOutcome::Failed
                },
            ]
        );
        let row = attribution.main_row.as_ref().unwrap();
        assert!(!row.at_base);
        assert_eq!(row.commits_before_base, Some(256));
        assert!(!attribution.accounted_by_base());
        assert_eq!(
            attribution.headline(),
            "2 unattributed (base unobserved on macos), 1 not observed on main"
        );
        assert_eq!(
            texts(&attribution),
            vec![
                "2 unattributed: no complete row observed base `7b8e6c41f471` on macos; the \
                 nearest, `79401a191cd5` on macos (cairn://p/cairn/check-observations/e884003b), \
                 is 256 commits older and says nothing about the changes since; passed there: \
                 core passed; red there: core red"
                    .to_string(),
                "1 not observed by the nearest row, `79401a191cd5` on macos \
                 (cairn://p/cairn/check-observations/e884003b) (new, or skipped there): core new"
                    .to_string(),
            ]
        );
        assert_eq!(
            attribution.unanswered_at_base(),
            names(&["core passed", "core red", "core new"])
        );
    }

    /// With no main row every failure is uncovered, and the line names the
    /// base it looked behind and the platform.
    #[test]
    fn without_a_main_row_every_failure_is_uncovered() {
        let attribution = partition(BASE, "linux", &failures(&["core a", "core b"]), None, None);
        assert!(attribution.red_at_base.is_empty() && attribution.green_at_base.is_empty());
        assert_eq!(attribution.headline(), "2 not observed on main");
        assert_eq!(
            texts(&attribution),
            vec![
                "2 with no main row at or before base `4a52d52e0000` on linux: core a, core b"
                    .to_string()
            ]
        );
    }

    /// A test main fails now and then can pass on the base's row by chance:
    /// red on 3 of main's last 10 rows, it is intermittent on main. A test no
    /// recent row failed, or none in the window ran, stays passed at base.
    #[test]
    fn a_main_intermittent_green_at_the_base_is_named_with_its_rate() {
        let row = main_row(
            BASE,
            &[
                ("core flaky", "passed"),
                ("core mine", "passed"),
                ("core unrated", "passed"),
            ],
        );
        let rates = HashMap::from([
            ("core flaky".to_string(), TestRate { red: 3, ran: 10 }),
            ("core mine".to_string(), TestRate { red: 0, ran: 10 }),
        ]);
        let attribution = partition(
            BASE,
            "linux",
            &failures(&["core flaky", "core mine", "core unrated"]),
            Some(&row),
            Some(0),
        )
        .with_main_rates(Ok(rates.clone()));

        assert_eq!(
            attribution.green_at_base,
            names(&["core mine", "core unrated"])
        );
        assert_eq!(
            attribution.intermittent_on_main,
            vec![RateOnMain {
                name: "core flaky".to_string(),
                red: 3,
                ran: 10
            }]
        );
        assert!(attribution.main_rate_unread.is_none());
        assert!(texts(&attribution).iter().any(|line| line.ends_with(
            "but red on recent main rows (intermittent on main): core flaky (red on 3 of the \
             last 10 main rows that ran it)"
        )));

        let all_intermittent = partition(
            BASE,
            "linux",
            &failures(&["core flaky"]),
            Some(&row),
            Some(0),
        )
        .with_main_rates(Ok(rates));
        assert!(all_intermittent.accounted_by_base());
        assert_eq!(all_intermittent.headline(), "1 intermittent on main");
    }

    /// A rate that could not be read is unknown, never "main never fails it".
    #[test]
    fn an_unread_rate_leaves_intermittence_unknown() {
        let row = main_row(BASE, &[("core a", "passed")]);
        let attribution = partition(BASE, "linux", &failures(&["core a"]), Some(&row), Some(0))
            .with_main_rates(Err("database is locked".to_string()));
        assert_eq!(attribution.green_at_base, names(&["core a"]));
        assert_eq!(
            attribution.main_rate_unread.as_deref(),
            Some("database is locked")
        );
        assert_eq!(
            attribution.headline(),
            "1 passed at base and failed here (main's recent rate unread)"
        );
        assert!(texts(&attribution).contains(
            &"main's recent rows could not be read for the 1 that passed at base, so whether \
              main fails them now and then is unknown: database is locked"
                .to_string()
        ));

        // With nothing passed at base there is no rate to be unknown.
        let red = main_row(BASE, &[("core a", "failed")]);
        let attribution = partition(BASE, "linux", &failures(&["core a"]), Some(&red), Some(0))
            .with_main_rates(Err("database is locked".to_string()));
        assert!(attribution.main_rate_unread.is_none());
    }

    /// A test red at the exact base that fails another way here is kept
    /// apart, both signatures named. One whose branch signature is unknown, or
    /// matches, is red at base.
    #[test]
    fn a_different_failure_signature_at_the_base_is_kept_apart() {
        let row = main_row(
            BASE,
            &[
                ("core a", "failed"),
                ("core b", "failed"),
                ("core c", "failed"),
            ],
        );
        let lane = vec![
            BranchFailure {
                name: "core a".to_string(),
                signature: Some("left: 2 right: 1".to_string()),
            },
            BranchFailure {
                name: "core b".to_string(),
                signature: Some("boom".to_string()),
            },
            BranchFailure {
                name: "core c".to_string(),
                signature: None,
            },
        ];
        let attribution = partition(BASE, "linux", &lane, Some(&row), Some(0));
        assert_eq!(
            attribution.failing_differently,
            vec![FailingDifferently {
                name: "core a".to_string(),
                here: "left: 2 right: 1".to_string(),
                at_base: "boom".to_string(),
            }]
        );
        assert_eq!(attribution.red_at_base, names(&["core b", "core c"]));
        assert_eq!(
            attribution.headline(),
            "1 red at base but failing differently, 2 red at base too"
        );
        assert!(texts(&attribution)[0].ends_with(
            "but failing differently here: core a (here: `left: 2 right: 1`; at base: `boom`)"
        ));
    }

    /// A finding `failed` on the base's row is red at base, one absent from it
    /// or merely allowlisted there passed at base, and a row that did not judge
    /// its audit observed none of them.
    #[test]
    fn a_plan_audit_failure_is_read_against_the_bases_audit() {
        use crate::execution::checks_main::{AuditFinding, RowAudit};
        let audited = |evaluated: bool| {
            let mut row = main_row(BASE, &[("t", "passed")]);
            row.plan_audit = Some(RowAudit {
                evaluated,
                note: None,
                findings: [
                    ("red", "failed"),
                    ("expired", "stale"),
                    ("known", "allowlisted"),
                ]
                .into_iter()
                .map(|(id, status)| {
                    (
                        id.to_string(),
                        AuditFinding {
                            status: status.to_string(),
                            issue: None,
                            excerpt: None,
                        },
                    )
                })
                .collect(),
            });
            row
        };
        let lane = failures(&[
            "plan-audit red",
            "plan-audit expired",
            "plan-audit known",
            "plan-audit new",
            "t",
        ]);

        let attribution = partition(BASE, "linux", &lane, Some(&audited(true)), Some(0));
        assert_eq!(
            attribution.red_at_base,
            names(&["plan-audit red", "plan-audit expired"])
        );
        assert_eq!(
            attribution.green_at_base,
            names(&["plan-audit known", "plan-audit new", "t"])
        );
        assert!(attribution.uncovered.is_empty());

        for row in [audited(false), main_row(BASE, &[("t", "passed")])] {
            let attribution = partition(BASE, "linux", &lane, Some(&row), Some(0));
            assert!(attribution.red_at_base.is_empty());
            assert_eq!(attribution.green_at_base, names(&["t"]));
            assert_eq!(attribution.uncovered.len(), 4);
        }
    }

    /// A gating audit on a tree whose tests all passed is a complete red with
    /// something to attribute, and a failure carries its runner's signature.
    #[test]
    fn a_complete_red_names_its_failures_with_their_signatures() {
        let parsed: ParsedCheckResult = serde_json::from_value(serde_json::json!({
            "schemaVersion": 1, "complete": true, "selection": "full",
            "parser": "nextest", "passed": 5, "failed": 1, "skipped": 0,
            "tests": [{"id": "core a", "status": "failed",
                       "failureMessage": "thread 'a' (7) panicked at a.rs:1:\nleft: 2 right: 1"}],
            "failures": [{"name": "core a"}, {"name": "plan-audit a.rs|wide-scan|events|aa"}],
        }))
        .unwrap();
        assert_eq!(
            complete_red_failures(false, false, None, Some(&parsed)),
            Some(vec![
                BranchFailure {
                    name: "core a".to_string(),
                    signature: Some("left: 2 right: 1".to_string()),
                },
                BranchFailure {
                    name: "plan-audit a.rs|wide-scan|events|aa".to_string(),
                    signature: None,
                },
            ])
        );
    }

    /// Only the base's reds are capped, an audit finding is named past that
    /// cap, and the remainder points at the row that names every one.
    #[test]
    fn the_bases_capped_reds_still_name_every_audit_finding() {
        use crate::execution::checks_main::{AuditFinding, RowAudit};
        let reds: Vec<String> = (0..8).map(|i| format!("t{i}")).collect();
        let tests: Vec<(&str, &str)> = reds.iter().map(|t| (t.as_str(), "failed")).collect();
        let mut row = main_row(BASE, &tests);
        row.plan_audit = Some(RowAudit {
            evaluated: true,
            note: None,
            findings: [(
                "late".to_string(),
                AuditFinding {
                    status: "failed".to_string(),
                    issue: None,
                    excerpt: None,
                },
            )]
            .into_iter()
            .collect(),
        });
        let mut lane: Vec<&str> = reds.iter().map(String::as_str).collect();
        lane.push("plan-audit late");
        let lines = texts(&partition(
            BASE,
            "linux",
            &failures(&lane),
            Some(&row),
            Some(0),
        ));
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].ends_with(
                "plan-audit late, t0, t1, t2, t3, t4, +3 more, every one red on \
                 cairn://p/cairn/check-observations/e884003b"
            ),
            "{lines:?}"
        );
    }

    /// Every name passed at base is listed however many there are.
    #[test]
    fn the_tests_that_passed_at_base_are_all_named() {
        let mine: Vec<String> = (0..7).map(|i| format!("core mine{i}")).collect();
        let tests: Vec<(&str, &str)> = mine.iter().map(|t| (t.as_str(), "passed")).collect();
        let row = main_row(BASE, &tests);
        let lane: Vec<&str> = mine.iter().map(String::as_str).collect();
        let lines = texts(&partition(
            BASE,
            "linux",
            &failures(&lane),
            Some(&row),
            Some(0),
        ));
        assert!(lines[0].ends_with(&mine.join(", ")), "{lines:?}");
    }

    /// The structured result carries the buckets, the row's provenance, the
    /// qualifying reads, and the same headline and lines every text surface
    /// renders; a job id stays internal.
    #[test]
    fn the_structured_result_carries_the_rendered_headline_and_lines() {
        let row = main_row(BASE, &[("core a", "passed")]);
        let attribution = partition(BASE, "linux", &failures(&["core a"]), Some(&row), Some(0))
            .with_other_jobs(Ok(vec![OtherJobFailure {
                name: "core a".to_string(),
                observation: "cairn://p/cairn/check-observations/other".to_string(),
                commit: "6bfe8619aaaabbbb".to_string(),
                platform: "linux".to_string(),
                cadence: "review".to_string(),
                signature: "left: 2 right: 1".to_string(),
                same_failure: None,
                job: "job-secret".to_string(),
            }]))
            .with_elsewhere(Err("no such table".to_string()));
        let json = serde_json::to_value(&attribution).unwrap();
        assert_eq!(json["greenAtBase"], serde_json::json!(["core a"]));
        assert_eq!(json["mainRow"]["atBase"], true);
        assert_eq!(json["mainRow"]["commitsBeforeBase"], 0);
        assert_eq!(json["accountedByBase"], false);
        assert_eq!(json["headline"], "1 passed at base and failed here");
        assert_eq!(json["elsewhereUnread"], "no such table");
        assert_eq!(json["lines"][0]["kind"], "greenAtBase");
        assert_eq!(json["lines"][1]["kind"], "unread");
        assert_eq!(
            json["lines"][0]["text"],
            serde_json::Value::String(texts(&attribution)[0].clone())
        );
        assert!(texts(&attribution)[0].ends_with(
            "core a (also red on 1 row of 1 other job: cairn://p/cairn/check-observations/other \
             at `6bfe8619aaaa` on linux, a review row, `left: 2 right: 1`)"
        ));
        assert!(!json.to_string().contains("job-secret"));
    }

    #[test]
    fn only_a_test_runners_complete_red_is_attributed() {
        let row = |parser: &str, passed: bool, kind: Option<&str>| CheckResultCacheEntry {
            source_observation_id: None,
            project_id: "p".to_string(),
            tree_hash: "t".to_string(),
            input_hash: "i".to_string(),
            check_name: "rust-tests".to_string(),
            exit_code: if passed { 0 } else { 100 },
            passed,
            output_tail: String::new(),
            duration_ms: 1,
            ran_at: 1,
            target_results_json: Some(
                serde_json::json!({
                    "schemaVersion": 1, "complete": true, "selection": "full",
                    "parser": parser, "passed": 5, "failed": 1, "skipped": 0,
                    "failures": [{"name": "core a"}],
                })
                .to_string(),
            ),
            job_id: Some("job".to_string()),
            cached: Some(false),
            failure_kind: kind.map(str::to_string),
            infra_failure_streak: 0,
            executor_id: None,
            executor_device_id: None,
            executor_connection_generation: None,
            executor_cell_id: None,
            executor_lease_epoch: None,
            executor_started_at_unix_ms: None,
            executor_finished_at_unix_ms: None,
            toolchain_fingerprint: None,
            defined_by_commit_sha: None,
        };
        assert_eq!(
            failing_tests(&row("nextest", false, None)),
            Some(failures(&["core a"]))
        );
        assert_eq!(failing_tests(&row("nextest", true, None)), None);
        assert_eq!(
            failing_tests(&row("rust-lint", false, None)),
            None,
            "a lint subtracts its own base"
        );
        assert_eq!(
            failing_tests(&row("nextest", false, Some("timed_out"))),
            None,
            "a death names no failure main could hold"
        );
    }

    // --- The ledger reads, against a migrated database. ---

    struct Observation<'a> {
        id: &'a str,
        commit: &'a str,
        platform: &'a str,
        job: &'a str,
        cadence: &'a str,
        ran_at: i64,
        /// (test, status, excerpt)
        tests: &'a [(&'a str, &'a str, &'a str)],
    }

    async fn ledger_db(temp: &tempfile::TempDir) -> LocalDb {
        let db = LocalDb::open_migrated_for_test(temp.path().join("attribution.db"))
            .await
            .unwrap();
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','w','Project','cairn','/tmp/repo',1,1);",
        )
        .await
        .unwrap();
        db
    }

    async fn observe(db: &LocalDb, row: Observation<'_>) {
        let Observation {
            id,
            commit,
            platform,
            job,
            cadence,
            ran_at,
            tests,
        } = row;
        let (exit_code, verdict) = if tests.iter().any(|(_, status, _)| *status == "failed") {
            (100, "failed")
        } else {
            (0, "passed")
        };
        let mut script = format!(
            "INSERT INTO check_result_observations
               (id, project_id, commit_sha, tree_hash, check_name, input_hash,
                environment_fingerprint, exit_code, verdict, complete, reusable,
                parser_version, result_schema_version, ran_at, duration_ms, job_id,
                cadence, output_tail, executor_id, verdict_platform, public_handle, selection)
             VALUES('{id}','p','{commit}','t-{id}','rust-tests','i-{id}','env',{exit_code},
                    '{verdict}',1,1,1,1,{ran_at},5,'{job}','{cadence}','','host','{platform}',
                    'h-{id}','full');"
        );
        for (test, status, excerpt) in tests {
            script.push_str(&format!(
                "INSERT INTO check_test_results (observation_id, test_id, status, failure_excerpt)
                 VALUES('{id}','{test}','{status}','{excerpt}');"
            ));
        }
        db.execute_script(script).await.unwrap();
    }

    fn ledger<'a>(platform: &'a str) -> Ledger<'a> {
        Ledger {
            project_id: "p",
            project_key: "cairn",
            job_id: "job-branch",
            check: "rust-tests",
            platform,
        }
    }

    /// `base`, then `depth` first-parent ancestors `c1`..`c{depth}`.
    fn chain(depth: usize) -> MainChain {
        let commits: Vec<String> = std::iter::once("base".to_string())
            .chain((1..=depth).map(|depth| format!("c{depth}")))
            .collect();
        MainChain::from_log(&commits.join("\n"))
    }

    async fn attribute(
        db: &LocalDb,
        platform: &str,
        chain: &MainChain,
        lane: &[BranchFailure],
    ) -> BaseAttribution {
        let names = |executor: &str| executor.to_string();
        attribute_on_chain(db, ledger(platform), "base", chain, lane, &names)
            .await
            .unwrap()
    }

    /// The cairn/6168 specimen: macOS's newest row is 256 commits before the
    /// base and passed both tests; Linux observed the exact base and failed
    /// one. Nothing is charged: both are unattributed on macOS, and Linux's
    /// exact-base red is cited as another platform's row, its green unused.
    #[tokio::test]
    async fn a_stale_pass_is_unattributed_and_an_exact_base_red_elsewhere_corroborates() {
        let temp = tempfile::tempdir().unwrap();
        let db = ledger_db(&temp).await;
        let both_green: &[(&str, &str, &str)] = &[("a", "passed", ""), ("b", "passed", "")];
        observe(
            &db,
            Observation {
                id: "mac-old",
                commit: "c256",
                platform: "macos",
                job: "main:p",
                cadence: "main",
                ran_at: 1,
                tests: both_green,
            },
        )
        .await;
        observe(
            &db,
            Observation {
                id: "linux-base",
                commit: "base",
                platform: "linux",
                job: "main:p",
                cadence: "main",
                ran_at: 2,
                tests: &[("a", "failed", "boom"), ("b", "passed", "")],
            },
        )
        .await;

        let attribution = attribute(&db, "macos", &chain(256), &failures(&["a", "b"])).await;

        assert!(attribution.green_at_base.is_empty() && attribution.red_at_base.is_empty());
        assert_eq!(attribution.base_unobserved.len(), 2);
        let row = attribution.main_row.as_ref().unwrap();
        assert_eq!((row.commit.as_str(), row.at_base), ("c256", false));
        assert_eq!(row.commits_before_base, Some(256));
        assert_eq!(
            row.observation,
            "cairn://p/cairn/check-observations/h-mac-old"
        );
        assert_eq!(
            attribution.red_at_base_elsewhere,
            vec![ElsewhereAtBase {
                platform: "linux".to_string(),
                observation: "cairn://p/cairn/check-observations/h-linux-base".to_string(),
                red: names(&["a"]),
            }]
        );
        assert_eq!(
            attribution.headline(),
            "2 unattributed (base unobserved on macos)"
        );
        let lines = texts(&attribution);
        assert!(lines[0].contains("is 256 commits older"), "{lines:?}");
        assert_eq!(
            lines[1],
            "1 red at exact base `base` on linux \
             (cairn://p/cairn/check-observations/h-linux-base), another platform's row and not a \
             macos verdict: a"
        );
        assert!(attribution.elsewhere_unread.is_none());
    }

    /// A stale pass with no other platform's row at the base is unattributed,
    /// and the empty corroboration is a finding, not an unread one.
    #[tokio::test]
    async fn a_stale_pass_with_no_other_row_is_only_unattributed() {
        let temp = tempfile::tempdir().unwrap();
        let db = ledger_db(&temp).await;
        observe(
            &db,
            Observation {
                id: "mac-old",
                commit: "c3",
                platform: "macos",
                job: "main:p",
                cadence: "main",
                ran_at: 1,
                tests: &[("a", "passed", "")],
            },
        )
        .await;
        let attribution = attribute(&db, "macos", &chain(3), &failures(&["a"])).await;
        assert_eq!(
            attribution.headline(),
            "1 unattributed (base unobserved on macos)"
        );
        assert!(attribution.red_at_base_elsewhere.is_empty());
        assert!(attribution.elsewhere_unread.is_none());
        assert!(
            attribution.other_jobs.is_empty(),
            "nothing passed at base to corroborate"
        );
    }

    /// A red on a row older than the base is history, not a waiver for the
    /// base: it is unattributed, never "red at base too".
    #[tokio::test]
    async fn a_stale_red_is_history_and_never_charged_to_the_base() {
        let temp = tempfile::tempdir().unwrap();
        let db = ledger_db(&temp).await;
        observe(
            &db,
            Observation {
                id: "linux-old",
                commit: "c7",
                platform: "linux",
                job: "main:p",
                cadence: "main",
                ran_at: 1,
                tests: &[("a", "failed", "boom")],
            },
        )
        .await;
        let attribution = attribute(&db, "linux", &chain(7), &failures(&["a"])).await;
        assert!(attribution.red_at_base.is_empty());
        assert_eq!(
            attribution.base_unobserved,
            vec![OlderStanding {
                name: "a".to_string(),
                older_row: OlderOutcome::Failed
            }]
        );
        assert!(!attribution.accounted_by_base());
        assert!(texts(&attribution)[0].contains("is 7 commits older"));
    }

    /// The exact base observed on the lane's own platform answers by itself,
    /// green and red, and an exact base's row on another platform is not read
    /// for a test the platform's own row answered.
    #[tokio::test]
    async fn the_exact_base_on_this_platform_answers_green_and_red() {
        let temp = tempfile::tempdir().unwrap();
        let db = ledger_db(&temp).await;
        observe(
            &db,
            Observation {
                id: "linux-base",
                commit: "base",
                platform: "linux",
                job: "main:p",
                cadence: "main",
                ran_at: 2,
                tests: &[("red", "failed", "boom"), ("green", "passed", "")],
            },
        )
        .await;
        observe(
            &db,
            Observation {
                id: "mac-base",
                commit: "base",
                platform: "macos",
                job: "main:p",
                cadence: "main",
                ran_at: 3,
                tests: &[("green", "failed", "boom")],
            },
        )
        .await;
        let attribution = attribute(&db, "linux", &chain(2), &failures(&["red", "green"])).await;
        assert_eq!(attribution.red_at_base, names(&["red"]));
        assert_eq!(attribution.green_at_base, names(&["green"]));
        assert!(attribution.main_row.as_ref().unwrap().at_base);
        assert!(attribution.red_at_base_elsewhere.is_empty());
        assert!(attribution.main_rate_unread.is_none());
    }

    /// A cached alias at the base names an observation of an older commit
    /// under a key that does not fold every input (cairn/5883). The row the
    /// base is read against is that observation's own commit, so the base
    /// stays unobserved and the row names its actual source.
    #[tokio::test]
    async fn an_alias_at_the_base_does_not_make_an_older_row_the_base() {
        let temp = tempfile::tempdir().unwrap();
        let db = ledger_db(&temp).await;
        observe(
            &db,
            Observation {
                id: "source",
                commit: "c1",
                platform: "linux",
                job: "main:p",
                cadence: "manual",
                ran_at: 1,
                tests: &[("a", "passed", "")],
            },
        )
        .await;
        db.execute_script(
            "INSERT INTO check_result_commit_aliases (
                project_id, commit_sha, check_name, environment_fingerprint,
                result_schema_version, source_observation_id, tree_hash, input_hash,
                disposition, evaluated_at)
             VALUES ('p', 'base', 'rust-tests', 'env', 1, 'source', 't-base', 'i-source',
                     'cached', 2);",
        )
        .await
        .unwrap();
        let attribution = attribute(&db, "linux", &chain(1), &failures(&["a"])).await;
        let row = attribution.main_row.as_ref().unwrap();
        assert_eq!(
            row.commit, "c1",
            "the actual source, not the requested revision"
        );
        assert!(!row.at_base);
        assert!(attribution.green_at_base.is_empty());
        assert_eq!(
            attribution.headline(),
            "1 unattributed (base unobserved on linux)"
        );
    }

    /// Main green at the exact base beside recent failures on other jobs: the
    /// test stays passed at base (another job's red is no exemption), and each
    /// other job's row is cited with its tree, platform, cadence and whether
    /// it failed the same way. The branch's own rows are never cited.
    #[tokio::test]
    async fn other_jobs_failures_corroborate_without_exempting() {
        let temp = tempfile::tempdir().unwrap();
        let db = ledger_db(&temp).await;
        let rows: [(&str, &str, &str, &str, &str, i64, &str); 4] = [
            ("base", "base", "linux", "main:p", "main", 1, "passed"),
            ("same", "x1", "linux", "job-other", "review", 5, "failed"),
            ("differ", "x2", "macos", "job-third", "write", 4, "failed"),
            ("mine", "x3", "linux", "job-branch", "review", 6, "failed"),
        ];
        for (id, commit, platform, job, cadence, ran_at, status) in rows {
            let excerpt = if id == "differ" {
                "other failure"
            } else {
                "left: 2 right: 1"
            };
            observe(
                &db,
                Observation {
                    id,
                    commit,
                    platform,
                    job,
                    cadence,
                    ran_at,
                    tests: &[("a", status, excerpt), ("z", "passed", "")],
                },
            )
            .await;
        }
        let lane = vec![BranchFailure {
            name: "a".to_string(),
            signature: Some("left: 2 right: 1".to_string()),
        }];
        let attribution = attribute(&db, "linux", &chain(1), &lane).await;

        assert_eq!(attribution.green_at_base, names(&["a"]));
        assert!(
            attribution.intermittent_on_main.is_empty(),
            "{attribution:?}"
        );
        let cited: Vec<(&str, Option<bool>)> = attribution
            .other_jobs
            .iter()
            .map(|row| (row.observation.as_str(), row.same_failure))
            .collect();
        assert_eq!(
            cited,
            vec![
                ("cairn://p/cairn/check-observations/h-same", Some(true)),
                ("cairn://p/cairn/check-observations/h-differ", Some(false)),
            ]
        );
        let line = &texts(&attribution)[0];
        assert!(
            line.contains("a (also red on 2 rows of 2 other jobs: "),
            "{line}"
        );
        assert!(
            line.contains("at `x1` on linux, a review row, the same failure"),
            "{line}"
        );
        assert!(
            line.contains("at `x2` on macos, a write row, `other failure`"),
            "{line}"
        );
        assert_eq!(attribution.headline(), "1 passed at base and failed here");
    }
}
