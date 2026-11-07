//! Where a branch lane's failing tests stand on main at the branch's base.
//!
//! A branch's test lane runs on a tree that carries main's history up to the
//! branch's base, so a test main already fails at that base fails on the branch
//! too, whatever the branch changed. The per-test ledger already answers "does
//! this test fail on main": every whole-suite row on main's first-parent chain
//! has its tests in `check_test_results`, and the main cadence diffs its rows by
//! that ledger (see [`crate::execution::checks_main`]). This module asks the
//! same ledger on the branch's behalf. Each failure a branch's test lane names
//! is read against the nearest complete whole-suite row at or before the base
//! on the lane's own verdict platform, and lands in exactly one bucket:
//!
//! - **red on main**: the test failed on that row. It is charged to the base
//!   and not counted against the branch.
//! - **intermittent on main**: the test passed on that row but failed on
//!   another of main's recent rows at or before the base. One green row is a
//!   sample, not an answer, for a test main fails now and then; it is charged
//!   to neither the base nor the branch, and named with its rate.
//! - **this branch's**: the test passed on that row and on every recent main
//!   row that ran it, so the branch moved it.
//! - **uncovered**: the row did not observe the test (it is new, or main
//!   skipped it), or no main row exists at or before the base. A test no row
//!   observed is never folded into either of the others.
//!
//! It is the main cadence's newly-red computation with the roles swapped: main
//! asks whether the tree moved between two of its rows; a branch asks whether
//! it moved a test off its base's row. Nothing new is measured: the rows are
//! the ones the main cadence and `cairn check run --branch main` record.
//!
//! A lane is attributed wherever its red is reported: from its stored row on
//! the wake, `/checks` and the settle resume, and from the outcome a write
//! wave just recorded on the inline summary of the committing tool result.
//!
//! Only a branch cut from the project's default branch is attributed, and only
//! a test runner's lane (`nextest`, `vitest`, `bun-test`): a lint already
//! subtracts its own base, and a branch stacked on another branch has no main
//! row at its base.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use crate::execution::cache::{CheckResultCacheEntry, OBSERVED_INFRA_FAILURE_BOUND};
use crate::execution::check_parsers::{
    shown_failure_names, ParsedCheckResult, PLAN_AUDIT_FAILURE_PREFIX,
};
use crate::execution::checks::CheckOutcome;
use crate::execution::checks_main::{
    first_parent_ancestry, nearest_row_on_chain, rates_on_chain, CadenceRow, ExecutorNames,
    MainChain, TestOutcome, TestRate,
};
use crate::execution::checks_status::{row_existence, LaneExistence};
use crate::execution::checks_turn_end::{JobCoords, WavePlan};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;

/// One branch lane's failing tests, partitioned against main at its base.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BaseAttribution {
    /// The branch's fork point on main.
    pub(crate) base: String,
    /// The row the failures were read against: the nearest complete
    /// whole-suite row at or before `base` on the lane's platform. `None` when
    /// main has none, and every failure is then uncovered.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) main_row: Option<MainRow>,
    /// Failed on `main_row` too: the base's.
    pub(crate) red_on_main: Vec<String>,
    /// Passed on `main_row` but failed on another of main's recent rows at or
    /// before `base`: charged to neither.
    pub(crate) intermittent_on_main: Vec<RateOnMain>,
    /// Passed on `main_row` and on every recent main row that ran it: the
    /// branch's.
    pub(crate) branch: Vec<String>,
    /// Not observed by `main_row`, or there is no `main_row`.
    pub(crate) uncovered: Vec<String>,
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
    /// The main commit the row observed.
    pub(crate) commit: String,
    /// The row's canonical `check-observations` URI.
    pub(crate) observation: String,
}

/// Sort `failures` by what `main_row` recorded for each.
pub(crate) fn partition(
    base: &str,
    failures: &[String],
    main_row: Option<&CadenceRow>,
) -> BaseAttribution {
    let mut attribution = BaseAttribution {
        base: base.to_string(),
        main_row: main_row.map(|row| MainRow {
            commit: row.commit.clone(),
            observation: row.reference.clone(),
        }),
        red_on_main: Vec::new(),
        intermittent_on_main: Vec::new(),
        branch: Vec::new(),
        uncovered: Vec::new(),
    };
    for name in failures {
        let standing = match name.strip_prefix(PLAN_AUDIT_FAILURE_PREFIX) {
            Some(finding) => main_row.and_then(|row| audit_standing(row, finding)),
            None => main_row
                .and_then(|row| row.tests.get(name))
                .and_then(|outcome| {
                    match outcome {
                        TestOutcome::Failed(_) => Some(MainStanding::Red),
                        TestOutcome::Passed => Some(MainStanding::NotRed),
                        // A test main skipped was not observed there, any more than
                        // a test main does not have.
                        TestOutcome::Skipped => None,
                    }
                }),
        };
        let bucket = match standing {
            Some(MainStanding::Red) => &mut attribution.red_on_main,
            Some(MainStanding::NotRed) => &mut attribution.branch,
            None => &mut attribution.uncovered,
        };
        bucket.push(name.clone());
    }
    attribution
}

/// What main's row says of one failure a branch names. `None`: it did not
/// observe it.
enum MainStanding {
    Red,
    NotRed,
}

/// A plan-audit finding's standing on main's row. A row that evaluated its audit
/// observed every statement its run explained, so a finding it does not hold in
/// red is the branch's, whether absent or allowlisted there. A row that did not
/// evaluate one observed nothing.
fn audit_standing(row: &CadenceRow, finding: &str) -> Option<MainStanding> {
    let audit = row.plan_audit.as_ref().filter(|audit| audit.evaluated)?;
    Some(if audit.gates(finding) {
        MainStanding::Red
    } else {
        MainStanding::NotRed
    })
}

fn short(commit: &str) -> &str {
    commit.get(..12).unwrap_or(commit)
}

impl BaseAttribution {
    /// Move each of the branch's failures that main's recent rows failed (by
    /// `rates`, read over the same chain as `main_row`) into
    /// `intermittent_on_main`. A failure stays the branch's only when no row in
    /// the window failed it.
    pub(crate) fn with_main_rates(mut self, rates: &HashMap<String, TestRate>) -> Self {
        let (intermittent, branch): (Vec<String>, Vec<String>) = std::mem::take(&mut self.branch)
            .into_iter()
            .partition(|name| rates.get(name).is_some_and(|rate| rate.red > 0));
        self.branch = branch;
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

    /// The failures charged to the branch: the only ones a headline counts.
    pub(crate) fn charged(&self) -> usize {
        self.branch.len()
    }

    /// How many of the lane's failures are the branch's, as a clause that
    /// follows the lane's own tally (`3 of 900 failed; 1 is this branch's`).
    ///
    /// A failure no main row observed may well be the branch's own new test, so
    /// "none is this branch's" is said only when nothing went unchecked.
    pub(crate) fn charge_clause(&self) -> String {
        match (self.branch.len(), self.uncovered.len()) {
            (0, 0) => "none is this branch's".to_string(),
            (0, unchecked) => {
                format!("none charged to this branch, {unchecked} not checked against main")
            }
            (1, _) => "1 is this branch's".to_string(),
            (charged, _) => format!("{charged} are this branch's"),
        }
    }

    /// Where the main row sits, said once per line: its commit and URI, and
    /// when it is not the base itself, that it is the nearest row before it.
    fn row_at(&self, row: &MainRow) -> String {
        if row.commit == self.base {
            format!("`{}` ({})", short(&row.commit), row.observation)
        } else {
            format!(
                "`{}` ({}, the nearest main row at or before base `{}`)",
                short(&row.commit),
                row.observation,
                short(&self.base)
            )
        }
    }

    /// One line per non-empty bucket, each naming its tests: the base's reds,
    /// the branch's, main's intermittents with their rates, and the uncovered.
    ///
    /// Every name the reader may have to act on is listed: the branch's, the
    /// intermittents and the uncovered, which no other line names. Only the
    /// base's reds are capped, and their remainder points at the main row,
    /// which holds every one of them by name. A capped list is never an
    /// unmarked prefix.
    pub(crate) fn bucket_lines(&self) -> Vec<String> {
        let names = |tests: &[String]| tests.join(", ");
        let mut lines = Vec::new();
        if let Some(row) = &self.main_row {
            if !self.red_on_main.is_empty() {
                let (shown, capped) = shown_failure_names(&self.red_on_main);
                let shown = shown.join(", ");
                let more = match capped {
                    0 => String::new(),
                    more => format!(", +{more} more, every one red on {}", row.observation),
                };
                lines.push(format!(
                    "{} red on main at {}, charged to the base: {shown}{more}",
                    self.red_on_main.len(),
                    self.row_at(row),
                ));
            }
            if !self.branch.is_empty() {
                lines.push(format!(
                    "{} not red on main at {}, this branch's: {}",
                    self.branch.len(),
                    self.row_at(row),
                    names(&self.branch)
                ));
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
                lines.push(format!(
                    "{} not red on main at {} but red on recent main rows (intermittent on main), \
                     charged to neither: {}",
                    self.intermittent_on_main.len(),
                    self.row_at(row),
                    names(&rated)
                ));
            }
            if !self.uncovered.is_empty() {
                lines.push(format!(
                    "{} not observed by main's row at {} (new, or skipped there): {}",
                    self.uncovered.len(),
                    self.row_at(row),
                    names(&self.uncovered)
                ));
            }
        } else if !self.uncovered.is_empty() {
            lines.push(format!(
                "{} with no main row at or before base `{}` on this platform: {}",
                self.uncovered.len(),
                short(&self.base),
                names(&self.uncovered)
            ));
        }
        lines
    }
}

/// The failing tests a lane's row names, when its row is a test runner's
/// complete red: the only rows a per-test ledger can answer.
pub(crate) fn failing_tests(row: &CheckResultCacheEntry) -> Option<Vec<String>> {
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
pub(crate) fn failing_outcome_tests(outcome: &CheckOutcome) -> Option<Vec<String>> {
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
) -> Option<Vec<String>> {
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
    Some(
        parsed
            .failures
            .iter()
            .map(|failure| failure.name.clone())
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

/// Project, check, platform, base, and the failing tests.
type AttributionKey = (String, String, String, String, Vec<String>);
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
    Box::pin(attribute_failing(orch, db, coords, base, failing)).await
}

/// [`attribute_lanes`] for the outcomes a write wave just evaluated. An
/// outcome that recorded no observation has no verdict platform to read, and
/// is left unattributed.
pub(crate) async fn attribute_outcomes(
    orch: &Orchestrator,
    db: &LocalDb,
    coords: &JobCoords,
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
    Box::pin(attribute_failing(orch, db, coords, base, failing)).await
}

/// One red test lane to attribute.
struct FailingLane<'a> {
    check: &'a str,
    failures: Vec<String>,
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
        match attribute_lane(db, coords, base, &chain, lane, &names).await {
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
        lane.failures,
    );
    if let Some((at, attribution)) = ATTRIBUTIONS.lock().unwrap().get(&key) {
        if at.elapsed() < ATTRIBUTION_TTL {
            return Ok(attribution.clone());
        }
    }
    let main_row = nearest_row_on_chain(
        db,
        &coords.project_id,
        &coords.project_key,
        &key.1,
        &platform,
        chain,
        names,
    )
    .await?;
    let mut attribution = partition(base, &key.4, main_row.as_ref());
    if !attribution.branch.is_empty() {
        // Without the rate the three buckets still stand; an intermittent is
        // then charged to the branch, as before the rate was read.
        match rates_on_chain(
            db,
            &coords.project_id,
            &key.1,
            &platform,
            chain,
            &attribution.branch,
        )
        .await
        {
            Ok(rates) => attribution = attribution.with_main_rates(&rates),
            Err(error) => log::debug!("main rate for {} at {base}: {error}", key.1),
        }
    }
    let attribution = Some(attribution);
    remember(&ATTRIBUTIONS, key, (Instant::now(), attribution.clone()));
    Ok(attribution)
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
    use std::collections::BTreeMap;

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

    /// A finding `failed` on main's row is the base's, one absent from it or
    /// merely allowlisted there is the branch's, and a row that did not judge its
    /// audit observed none of them.
    #[test]
    fn a_plan_audit_failure_is_read_against_mains_audit() {
        use crate::execution::checks_main::{AuditFinding, RowAudit};
        let audited = |evaluated: bool| {
            let mut row = main_row("4a52d52e0000", &[("t", "passed")]);
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
        let failures = names(&[
            "plan-audit red",
            "plan-audit expired",
            "plan-audit known",
            "plan-audit new",
            "t",
        ]);

        let attribution = partition("4a52d52e0000", &failures, Some(&audited(true)));
        assert_eq!(
            attribution.red_on_main,
            names(&["plan-audit red", "plan-audit expired"])
        );
        assert_eq!(
            attribution.branch,
            names(&["plan-audit known", "plan-audit new", "t"])
        );
        assert!(attribution.uncovered.is_empty());

        for row in [audited(false), main_row("4a52d52e0000", &[("t", "passed")])] {
            let attribution = partition("4a52d52e0000", &failures, Some(&row));
            assert!(attribution.red_on_main.is_empty());
            assert_eq!(attribution.branch, names(&["t"]));
            assert_eq!(attribution.uncovered.len(), 4);
        }
    }

    /// A gating audit on a tree whose tests all passed is a complete red with
    /// something to attribute, on the stored row and on a fresh outcome alike.
    #[test]
    fn a_gating_audit_on_an_all_passed_tree_is_attributed() {
        let parsed: ParsedCheckResult = serde_json::from_value(serde_json::json!({
            "schemaVersion": 1, "complete": true, "selection": "full",
            "parser": "nextest", "passed": 5, "failed": 0, "skipped": 0,
            "failures": [{"name": "plan-audit a.rs|wide-scan|events|aa"}],
        }))
        .unwrap();
        assert_eq!(
            complete_red_failures(false, false, None, Some(&parsed)),
            Some(vec!["plan-audit a.rs|wide-scan|events|aa".to_string()])
        );
    }

    /// Only the base's reds are capped, and an audit finding is named past that
    /// cap though it follows the tests in the list.
    #[test]
    fn the_bases_capped_reds_still_name_every_audit_finding() {
        use crate::execution::checks_main::{AuditFinding, RowAudit};

        let mut row = main_row(
            "4a52d52e0000",
            &[
                ("t0", "failed"),
                ("t1", "failed"),
                ("t2", "failed"),
                ("t3", "failed"),
                ("t4", "failed"),
                ("t5", "failed"),
                ("t6", "failed"),
                ("t7", "failed"),
            ],
        );
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
        let failures = names(&[
            "t0",
            "t1",
            "t2",
            "t3",
            "t4",
            "t5",
            "t6",
            "t7",
            "plan-audit late",
        ]);
        let lines = partition("4a52d52e0000", &failures, Some(&row)).bucket_lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].contains("plan-audit late, t0, t1, t2, t3, t4, +3 more, every one red on"),
            "{lines:?}"
        );
    }

    fn names(tests: &[&str]) -> Vec<String> {
        tests.iter().map(|test| test.to_string()).collect()
    }

    /// Each failure lands in exactly one bucket by what main's row recorded:
    /// red there is the base's, green there is the branch's, and a test the
    /// row skipped or never had is uncovered.
    #[test]
    fn each_failure_lands_in_the_bucket_main_recorded() {
        let row = main_row(
            "4a52d52e0000",
            &[
                ("core decide::a", "failed"),
                ("core b", "passed"),
                ("core skipped", "skipped"),
            ],
        );
        let attribution = partition(
            "4a52d52e0000",
            &names(&["core decide::a", "core b", "core skipped", "core new"]),
            Some(&row),
        );
        assert_eq!(attribution.red_on_main, names(&["core decide::a"]));
        assert_eq!(attribution.branch, names(&["core b"]));
        assert_eq!(attribution.uncovered, names(&["core skipped", "core new"]));
        assert_eq!(attribution.charged(), 1);
        assert_eq!(attribution.charge_clause(), "1 is this branch's");
    }

    /// With no main row every failure is uncovered, never charged to the base
    /// nor to the branch, and the line names the base it looked behind.
    #[test]
    fn without_a_main_row_every_failure_is_uncovered() {
        let attribution = partition("4a52d52e0000", &names(&["core a", "core b"]), None);
        assert!(attribution.red_on_main.is_empty() && attribution.branch.is_empty());
        assert_eq!(
            attribution.charge_clause(),
            "none charged to this branch, 2 not checked against main"
        );
        assert_eq!(
            attribution.bucket_lines(),
            vec![
                "2 with no main row at or before base `4a52d52e0000` on this platform: core a, core b"
                    .to_string()
            ]
        );
    }

    /// A row behind the base says it is the nearest one rather than claiming
    /// to be the base.
    #[test]
    fn a_row_before_the_base_says_it_is_the_nearest() {
        let row = main_row("1111aaaa2222bbbb", &[("core a", "failed")]);
        let attribution = partition("3333cccc4444dddd", &names(&["core a"]), Some(&row));
        assert_eq!(
            attribution.bucket_lines(),
            vec![
                "1 red on main at `1111aaaa2222` (cairn://p/cairn/check-observations/e884003b, \
                 the nearest main row at or before base `3333cccc4444`), charged to the base: core a"
                    .to_string()
            ]
        );
    }

    /// A test main fails now and then can pass on the base's one row by
    /// chance. Red on 3 of main's last 10 rows, it is intermittent on main:
    /// named with its rate, charged to neither, and not counted as the
    /// branch's. A test no recent row failed stays the branch's.
    #[test]
    fn a_main_intermittent_green_on_the_base_row_is_charged_to_neither() {
        let row = main_row(
            "4a52d52e0000",
            &[("core flaky", "passed"), ("core mine", "passed")],
        );
        let rates = HashMap::from([
            ("core flaky".to_string(), TestRate { red: 3, ran: 10 }),
            ("core mine".to_string(), TestRate { red: 0, ran: 10 }),
        ]);
        let attribution = partition(
            "4a52d52e0000",
            &names(&["core flaky", "core mine"]),
            Some(&row),
        )
        .with_main_rates(&rates);

        assert_eq!(attribution.branch, names(&["core mine"]));
        assert_eq!(
            attribution.intermittent_on_main,
            vec![RateOnMain {
                name: "core flaky".to_string(),
                red: 3,
                ran: 10
            }]
        );
        assert_eq!(attribution.charged(), 1);
        assert_eq!(
            attribution.bucket_lines(),
            vec![
                "1 not red on main at `4a52d52e0000` (cairn://p/cairn/check-observations/e884003b), \
                 this branch's: core mine"
                    .to_string(),
                "1 not red on main at `4a52d52e0000` (cairn://p/cairn/check-observations/e884003b) \
                 but red on recent main rows (intermittent on main), charged to neither: \
                 core flaky (red on 3 of the last 10 main rows that ran it)"
                    .to_string(),
            ]
        );

        let all_intermittent =
            partition("4a52d52e0000", &names(&["core flaky"]), Some(&row)).with_main_rates(&rates);
        assert_eq!(all_intermittent.charge_clause(), "none is this branch's");
    }

    /// The branch's failures are all named however many there are; the
    /// base's are capped, and the remainder is counted and pointed at the main
    /// row that names them.
    #[test]
    fn the_branchs_failures_are_all_named_and_the_bases_cap_points_at_main() {
        let mine: Vec<String> = (0..7).map(|i| format!("core mine{i}")).collect();
        let mains: Vec<String> = (0..7).map(|i| format!("core main{i}")).collect();
        let mut tests: Vec<(&str, &str)> = mine.iter().map(|t| (t.as_str(), "passed")).collect();
        tests.extend(mains.iter().map(|t| (t.as_str(), "failed")));
        let row = main_row("4a52d52e0000", &tests);
        let failures: Vec<String> = mine.iter().chain(&mains).cloned().collect();

        let lines = partition("4a52d52e0000", &failures, Some(&row)).bucket_lines();

        assert!(
            lines[0].ends_with(
                "charged to the base: core main0, core main1, core main2, core main3, \
                 core main4, +2 more, every one red on cairn://p/cairn/check-observations/e884003b"
            ),
            "{lines:?}"
        );
        assert!(
            lines[1].ends_with(&format!("this branch's: {}", mine.join(", "))),
            "{lines:?}"
        );
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
            Some(names(&["core a"]))
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
}
