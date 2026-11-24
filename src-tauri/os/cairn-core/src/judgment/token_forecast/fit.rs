//! The refit: cairn/6271's validation-then-test protocol
//! (`bench/token-forecast/score.py::run_protocol`) re-run on a rolling
//! window, from resolved issues and their stored labels.
//!
//! At `as_of` T:
//!
//! - **Shift stage.** Fit score bins on eligible issues resolved before
//!   T - 21 days; predict eligible issues created in [T - 21 days, T); take
//!   the p80 and p90 split-conformal shifts and each bin's shifted p90
//!   coverage on those issues. This is the bench's validation window shape,
//!   so a replay at T = 2026-10-19 is 6271's test protocol.
//! - **Table stage.** Refit the bins on every eligible issue and apply the
//!   shift stage's shifts.
//!
//! A bin's p90 is soft when it is the top score bin, or its held-out coverage
//! fell below [`SOFT_COVERAGE`], or (for every bin) the largest fifth of the
//! shift window by actual tokens fell below it: the stratum 6271 found weak
//! (0.77) cannot be told apart at filing, and the replay showed the score
//! bins do not isolate it. The flag is decided by data at fit time and known
//! at filing.
//!
//! Everything here is pure: the population and labels come in, a
//! [`Refit`] comes out, and the same inputs give the same version.

use std::collections::{BTreeMap, HashMap};

use super::applicability::{ANTHROPIC, OPENAI};
use super::calibration::{
    conformal_shift, decide_table, quantile, shift, Calibration, CalibrationBin, BINS,
};
use super::contract::SoftBasis;
use super::question::sha256_hex;

/// 2026-02-01 00:00 America/Los_Angeles: token rollups are partial before.
pub const FLOOR: i64 = 1_769_932_800;
/// cairn/6271's test cut, 2026-10-19 00:00 America/Los_Angeles.
pub const BENCH_CUT: i64 = 1_792_393_200;
/// The end of 6271's test window, 2026-11-02 00:00 America/Los_Angeles.
pub const BENCH_TEST_TO: i64 = 1_793_606_400;
/// The shift window: 6271's validation span, and judgment's rolling
/// recalibration (11-06).
pub const SHIFT_WINDOW_SECS: i64 = 21 * 86_400;
/// An open execution counts as live work only if the issue spent tokens this
/// close to the snapshot; older open rows are stale status.
pub const IN_FLIGHT_WINDOW_SECS: i64 = 48 * 3_600;
/// The fewest issues the shift stage may fit on.
pub const MIN_FIT: usize = 200;
/// The fewest shift-window issues the shifts may come from.
pub const MIN_SHIFT: usize = 50;
/// A bin whose held-out shifted p90 coverage is below this is soft.
pub const SOFT_COVERAGE: f64 = 0.85;
/// Bumped whenever the refit's rules change, so a version names its code.
pub const FIT_CODE_VERSION: &str = "refit.v2";

/// One issue as the population reads it.
#[derive(Debug, Clone, PartialEq)]
pub struct PopulationIssue {
    pub issue_id: String,
    pub project_key: Option<String>,
    pub number: i64,
    pub created_at: i64,
    /// When it was merged or closed; `None` while unresolved.
    pub resolved_at: Option<i64>,
    pub open_executions: i64,
    /// The newest token rollup bucket attributed to it.
    pub last_bucket: Option<i64>,
    /// Billable tokens of its primary (most-spending) maker.
    pub primary_billable: i64,
    pub primary_maker: Option<String>,
}

impl PopulationIssue {
    pub fn in_flight(&self, snapshot: i64) -> bool {
        self.open_executions > 0
            && self.last_bucket.unwrap_or(0) >= snapshot - IN_FLIGHT_WINDOW_SECS
    }

    /// The target: ln of the primary maker's billable tokens.
    pub fn y(&self) -> Option<f64> {
        (self.primary_billable > 0).then(|| (self.primary_billable as f64).ln())
    }

    /// `project/number`, the bench's item id.
    pub fn item_id(&self) -> String {
        format!(
            "{}/{}",
            self.project_key.as_deref().unwrap_or(""),
            self.number
        )
    }
}

/// A stored label as the refit reads it.
#[derive(Debug, Clone, PartialEq)]
pub struct FitLabel {
    pub score: f64,
    pub filing_maker: Option<String>,
    pub text_source: Option<String>,
}

fn group_of(maker: Option<&str>) -> &'static str {
    match maker {
        Some(ANTHROPIC) => ANTHROPIC,
        Some(OPENAI) => OPENAI,
        Some("unknown") | None => "unknown",
        Some(_) => "other",
    }
}

/// A refit that was not activated, with why.
#[derive(Debug, Clone, PartialEq)]
pub struct RejectedRefit {
    pub version: String,
    pub answerer_version: String,
    pub question_version: String,
    pub as_of: i64,
    pub table_window: [i64; 2],
    pub shift_window: [i64; 2],
    pub fit_n: u64,
    pub shift_n: u64,
    pub reason: String,
}

/// What a refit came to, with the population it counted.
#[derive(Debug, Clone, PartialEq)]
pub enum RefitVerdict {
    Accepted(Calibration),
    Rejected(RejectedRefit),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Refit {
    pub verdict: RefitVerdict,
    /// Issues left out, by reason.
    pub exclusions: BTreeMap<String, u64>,
}

struct Eligible<'a> {
    issue: &'a PopulationIssue,
    score: f64,
    y: f64,
}

/// Why an issue is out of the population at `as_of`, or its score and target.
fn eligibility<'a>(
    issue: &'a PopulationIssue,
    labels: &HashMap<String, FitLabel>,
    as_of: i64,
    snapshot: i64,
) -> Result<Eligible<'a>, String> {
    if issue.created_at < FLOOR {
        return Err("before_floor".into());
    }
    if issue.created_at >= as_of {
        return Err("created_after_as_of".into());
    }
    match issue.resolved_at {
        None => return Err("unresolved".into()),
        Some(at) if at >= as_of => return Err("resolved_after_as_of".into()),
        Some(_) => {}
    }
    if issue.in_flight(snapshot) {
        return Err("in_flight".into());
    }
    let Some(y) = issue.y() else {
        return Err("no_tokens".into());
    };
    let Some(label) = labels.get(&issue.issue_id) else {
        return Err("unlabelled".into());
    };
    let group = group_of(label.filing_maker.as_deref());
    if group != ANTHROPIC {
        return Err(format!("filing_maker_{group}"));
    }
    Ok(Eligible {
        issue,
        score: label.score,
        y,
    })
}

/// The deterministic name of a refit: what it was asked and every issue it
/// read, with the stage each was used in.
fn version_digest(
    answerer_version: &str,
    question_version: &str,
    as_of: i64,
    members: &[(&Eligible<'_>, &str)],
) -> String {
    let mut lines: Vec<String> = members
        .iter()
        .map(|(e, stage)| format!("{}\t{:?}\t{:?}\t{stage}", e.issue.issue_id, e.score, e.y))
        .collect();
    lines.sort();
    let head = format!("{FIT_CODE_VERSION}\t{answerer_version}\t{question_version}\t{as_of}");
    let digest = sha256_hex(&format!("{head}\n{}", lines.join("\n")));
    format!("tfc-{}", &digest[..16])
}

/// Refit at `as_of`, reading in-flight status against `snapshot` (the time
/// the population was read).
pub fn refit(
    issues: &[PopulationIssue],
    labels: &HashMap<String, FitLabel>,
    as_of: i64,
    snapshot: i64,
    answerer_version: &str,
    question_version: &str,
) -> Refit {
    let mut exclusions: BTreeMap<String, u64> = BTreeMap::new();
    let mut eligible = Vec::new();
    for issue in issues {
        match eligibility(issue, labels, as_of, snapshot) {
            Ok(e) => eligible.push(e),
            Err(reason) => *exclusions.entry(reason).or_default() += 1,
        }
    }
    let shift_from = as_of - SHIFT_WINDOW_SECS;
    let stage_a: Vec<&Eligible<'_>> = eligible
        .iter()
        .filter(|e| e.issue.resolved_at.is_some_and(|at| at < shift_from))
        .collect();
    let shift_set: Vec<&Eligible<'_>> = eligible
        .iter()
        .filter(|e| e.issue.created_at >= shift_from)
        .collect();
    let mut members: Vec<(&Eligible<'_>, &str)> = eligible.iter().map(|e| (e, "table")).collect();
    members.extend(stage_a.iter().map(|e| (*e, "shift_fit")));
    members.extend(shift_set.iter().map(|e| (*e, "shift")));
    let version = version_digest(answerer_version, question_version, as_of, &members);
    let table_window = [FLOOR, as_of];
    let shift_window = [shift_from, as_of];
    let reject = |reason: String, exclusions| Refit {
        verdict: RefitVerdict::Rejected(RejectedRefit {
            version: version.clone(),
            answerer_version: answerer_version.to_string(),
            question_version: question_version.to_string(),
            as_of,
            table_window,
            shift_window,
            fit_n: eligible.len() as u64,
            shift_n: shift_set.len() as u64,
            reason,
        }),
        exclusions,
    };
    if stage_a.len() < MIN_FIT || shift_set.len() < MIN_SHIFT {
        return reject(
            format!(
                "too few issues: {} to fit the shift stage (need {MIN_FIT}), {} in the shift window (need {MIN_SHIFT})",
                stage_a.len(),
                shift_set.len()
            ),
            exclusions,
        );
    }

    let pairs = |set: &[&Eligible<'_>]| set.iter().map(|e| (e.score, e.y)).collect::<Vec<_>>();
    let Some(table_a) = decide_table(&pairs(&stage_a)) else {
        return reject("the shift stage fit no table".into(), exclusions);
    };
    let all: Vec<&Eligible<'_>> = eligible.iter().collect();
    let Some(table) = decide_table(&pairs(&all)) else {
        return reject("the table stage fit no table".into(), exclusions);
    };
    // Each shift-window issue is predicted by the shift-stage table, which
    // never saw it, and counted in the final table's bin for its score, so a
    // bin's held-out coverage describes the interval forecasts are made in.
    let predicted: Vec<(usize, [f64; 3], f64)> = shift_set
        .iter()
        .map(|e| {
            (
                table.bin_of(e.score),
                table_a.bins[table_a.bin_of(e.score)],
                e.y,
            )
        })
        .collect();
    let residuals = |j: usize| {
        predicted
            .iter()
            .map(|(_, p, y)| y - p[j])
            .collect::<Vec<_>>()
    };
    let (Some(s80), Some(s90)) = (
        conformal_shift(&residuals(1), 0.8),
        conformal_shift(&residuals(2), 0.9),
    ) else {
        return reject("no shift-window residuals".into(), exclusions);
    };
    let shifts = [s80, s90];
    let mut covered = [0u64; BINS];
    let mut counted = [0u64; BINS];
    for (bin, p, y) in &predicted {
        counted[*bin] += 1;
        if *y <= shift(*p, shifts)[2] {
            covered[*bin] += 1;
        }
    }

    // The stratum cairn/6271 found weakest (its largest fifth by actuals),
    // measured the same held-out way. Size is unknowable at filing, so a weak
    // reading softens every p90 of this calibration rather than one bin's.
    let shift_ys: Vec<f64> = predicted.iter().map(|(_, _, y)| *y).collect();
    let large_actuals_coverage = quantile(&shift_ys, 0.8).and_then(|edge| {
        let large: Vec<_> = predicted.iter().filter(|(_, _, y)| *y > edge).collect();
        (!large.is_empty()).then(|| {
            large
                .iter()
                .filter(|(_, p, y)| *y <= shift(*p, shifts)[2])
                .count() as f64
                / large.len() as f64
        })
    });
    let large_actuals_weak = large_actuals_coverage.is_some_and(|c| c < SOFT_COVERAGE);
    let bins = table
        .bins
        .iter()
        .enumerate()
        .map(|(bin, quantiles)| {
            let heldout_coverage =
                (counted[bin] > 0).then(|| covered[bin] as f64 / counted[bin] as f64);
            // The precedence `SoftBasis` documents: calibration-wide first.
            let soft_basis = if large_actuals_weak {
                Some(SoftBasis::LargestActualsHeldoutCoverageBelow085)
            } else if bin == BINS - 1 {
                Some(SoftBasis::TopScoreBin)
            } else if heldout_coverage.is_some_and(|c| c < SOFT_COVERAGE) {
                Some(SoftBasis::BinHeldoutCoverageBelow085)
            } else {
                None
            };
            CalibrationBin {
                quantiles: *quantiles,
                soft_basis,
                heldout_coverage,
                heldout_n: counted[bin],
            }
        })
        .collect();
    Refit {
        verdict: RefitVerdict::Accepted(Calibration {
            version,
            answerer_version: answerer_version.to_string(),
            question_version: question_version.to_string(),
            as_of,
            table_window,
            shift_window,
            fit_n: eligible.len() as u64,
            shift_n: shift_set.len() as u64,
            edges: table.edges,
            bins,
            shifts,
            group: ANTHROPIC.to_string(),
            large_actuals_coverage,
        }),
        exclusions,
    }
}

/// Error and coverage over one set of scored issues.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct Cut {
    pub n: u64,
    pub mdale: Option<f64>,
    pub p90_coverage: Option<f64>,
}

/// A calibration scored on a later window, the way 6271's test was scored.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct ReplayReport {
    /// Resolved, settled issues scored.
    pub n: u64,
    /// In-flight or unresolved issues, scored only as lower bounds.
    pub censored_n: u64,
    /// Censored issues whose tokens so far already exceed the p90.
    pub censored_misses: u64,
    pub mdale: Option<f64>,
    pub p50_coverage: Option<f64>,
    pub p80_coverage: Option<f64>,
    pub p90_coverage: Option<f64>,
    /// Resolved hits over resolved issues plus censored misses: the bar's count.
    pub bar_p90_coverage: Option<f64>,
    /// Median p90 - p50 (ln).
    pub width: Option<f64>,
    pub largest_fifth_by_actuals: Cut,
    pub p90_soft: Cut,
    pub p90_unflagged: Cut,
    pub event_text: Cut,
    /// Issues in the window left out, by reason.
    pub exclusions: BTreeMap<String, u64>,
}

struct Scored {
    y: f64,
    p: [f64; 3],
    soft: bool,
    event_text: bool,
}

fn cut<'a>(items: impl Iterator<Item = &'a Scored>) -> Cut {
    let items: Vec<&Scored> = items.collect();
    if items.is_empty() {
        return Cut::default();
    }
    let errors: Vec<f64> = items.iter().map(|s| (s.y - s.p[0]).abs()).collect();
    let hits = items.iter().filter(|s| s.y <= s.p[2]).count();
    Cut {
        n: items.len() as u64,
        mdale: quantile(&errors, 0.5),
        p90_coverage: Some(hits as f64 / items.len() as f64),
    }
}

/// Score `calibration` on Anthropic-filed, labelled issues created in
/// `[from, to)`, reading in-flight status against `snapshot`.
pub fn replay(
    calibration: &Calibration,
    issues: &[PopulationIssue],
    labels: &HashMap<String, FitLabel>,
    from: i64,
    to: i64,
    snapshot: i64,
) -> ReplayReport {
    let mut report = ReplayReport::default();
    let mut scored = Vec::new();
    let mut censored = Vec::new();
    let skip = |report: &mut ReplayReport, reason: String| {
        *report.exclusions.entry(reason).or_default() += 1;
    };
    for issue in issues
        .iter()
        .filter(|i| i.created_at >= from && i.created_at < to)
    {
        let Some(label) = labels.get(&issue.issue_id) else {
            skip(&mut report, "unlabelled".into());
            continue;
        };
        let group = group_of(label.filing_maker.as_deref());
        if group != ANTHROPIC {
            skip(&mut report, format!("filing_maker_{group}"));
            continue;
        }
        let Some(looked) = calibration.lookup(label.score) else {
            skip(&mut report, "no_bin".into());
            continue;
        };
        let settled = issue.resolved_at.is_some() && !issue.in_flight(snapshot);
        if !settled {
            censored.push((issue.y(), looked.quantiles[2]));
            continue;
        }
        let Some(y) = issue.y() else {
            skip(&mut report, "no_tokens".into());
            continue;
        };
        scored.push(Scored {
            y,
            p: looked.quantiles,
            soft: looked.soft_basis.is_some(),
            event_text: label.text_source.as_deref() == Some("event"),
        });
    }
    report.n = scored.len() as u64;
    report.censored_n = censored.len() as u64;
    report.censored_misses = censored
        .iter()
        .filter(|(lower, p90)| lower.is_some_and(|lower| lower > *p90))
        .count() as u64;
    if scored.is_empty() {
        return report;
    }
    let n = scored.len() as f64;
    let coverage = |j: usize| scored.iter().filter(|s| s.y <= s.p[j]).count() as f64 / n;
    let errors: Vec<f64> = scored.iter().map(|s| (s.y - s.p[0]).abs()).collect();
    let widths: Vec<f64> = scored.iter().map(|s| s.p[2] - s.p[0]).collect();
    let hits = scored.iter().filter(|s| s.y <= s.p[2]).count() as f64;
    report.mdale = quantile(&errors, 0.5);
    report.p50_coverage = Some(coverage(0));
    report.p80_coverage = Some(coverage(1));
    report.p90_coverage = Some(coverage(2));
    report.bar_p90_coverage = Some(hits / (n + report.censored_misses as f64));
    report.width = quantile(&widths, 0.5);
    let ys: Vec<f64> = scored.iter().map(|s| s.y).collect();
    if let Some(top_edge) = quantile(&ys, 0.8) {
        report.largest_fifth_by_actuals = cut(scored.iter().filter(|s| s.y > top_edge));
    }
    report.p90_soft = cut(scored.iter().filter(|s| s.soft));
    report.p90_unflagged = cut(scored.iter().filter(|s| !s.soft));
    report.event_text = cut(scored.iter().filter(|s| s.event_text));
    report
}
