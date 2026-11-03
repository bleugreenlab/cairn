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
//! - **this branch's**: the test passed on that row, so the branch moved it.
//! - **uncovered**: the row did not observe the test (it is new, or main
//!   skipped it), or no main row exists at or before the base. A test no row
//!   observed is never folded into either of the others.
//!
//! It is the main cadence's newly-red computation with the roles swapped: main
//! asks whether the tree moved between two of its rows; a branch asks whether
//! it moved a test off its base's row. Nothing new is measured: the rows are
//! the ones the main cadence and `cairn check run --branch main` record.
//!
//! Only a branch cut from the project's default branch is attributed, and only
//! a test runner's lane (`nextest`, `vitest`, `bun-test`): a lint already
//! subtracts its own base, and a branch stacked on another branch has no main
//! row at its base.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use crate::execution::cache::CheckResultCacheEntry;
use crate::execution::check_parsers::{ParsedCheckResult, MAX_FAILURE_NAMES};
use crate::execution::checks_main::{
    first_parent_ancestry, nearest_row_on_chain, CadenceRow, ExecutorNames, MainChain, TestOutcome,
};
use crate::execution::checks_status::{entry_existence, join_capped, LaneExistence};
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
    /// Passed on `main_row`: the branch's.
    pub(crate) branch: Vec<String>,
    /// Not observed by `main_row`, or there is no `main_row`.
    pub(crate) uncovered: Vec<String>,
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
        branch: Vec::new(),
        uncovered: Vec::new(),
    };
    for name in failures {
        let bucket = match main_row.and_then(|row| row.tests.get(name)) {
            Some(TestOutcome::Failed(_)) => &mut attribution.red_on_main,
            Some(TestOutcome::Passed) => &mut attribution.branch,
            // A test main skipped was not observed there, any more than a
            // test main does not have.
            Some(TestOutcome::Skipped) | None => &mut attribution.uncovered,
        };
        bucket.push(name.clone());
    }
    attribution
}

fn short(commit: &str) -> &str {
    commit.get(..12).unwrap_or(commit)
}

impl BaseAttribution {
    /// The failures charged to the branch: the only ones a headline counts.
    pub(crate) fn charged(&self) -> usize {
        self.branch.len()
    }

    /// How many of the lane's failures are the branch's, as a clause that
    /// follows the lane's own tally (`3 of 900 failed; 1 is this branch's`).
    pub(crate) fn charge_clause(&self) -> String {
        match self.branch.len() {
            0 => "none is this branch's".to_string(),
            1 => "1 is this branch's".to_string(),
            charged => format!("{charged} are this branch's"),
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
    /// the branch's, and the uncovered.
    pub(crate) fn bucket_lines(&self) -> Vec<String> {
        let names = |tests: &[String]| join_capped(tests, MAX_FAILURE_NAMES);
        let mut lines = Vec::new();
        if let Some(row) = &self.main_row {
            if !self.red_on_main.is_empty() {
                lines.push(format!(
                    "{} red on main at {}, charged to the base: {}",
                    self.red_on_main.len(),
                    self.row_at(row),
                    names(&self.red_on_main)
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
    if row.passed
        || row.failure_kind.is_some()
        || !matches!(entry_existence(row), LaneExistence::Complete)
    {
        return None;
    }
    let parsed: ParsedCheckResult =
        serde_json::from_str(row.target_results_json.as_deref()?).ok()?;
    if !parsed.is_test_runner() || parsed.failures.is_empty() {
        return None;
    }
    Some(
        parsed
            .failures
            .into_iter()
            .map(|failure| failure.name)
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
    let failing: Vec<(&CheckResultCacheEntry, Vec<String>)> = rows
        .into_iter()
        .filter_map(|row| failing_tests(row).map(|tests| (row, tests)))
        .collect();
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
    for (row, failures) in failing {
        match attribute_row(db, coords, base, &chain, row, failures, &names).await {
            Ok(Some(attribution)) => {
                attributions.insert(row.check_name.clone(), attribution);
            }
            Ok(None) => {}
            Err(error) => log::debug!("base attribution for {} at {base}: {error}", row.check_name),
        }
    }
    attributions
}

async fn attribute_row(
    db: &LocalDb,
    coords: &JobCoords,
    base: &str,
    chain: &MainChain,
    row: &CheckResultCacheEntry,
    failures: Vec<String>,
    names: ExecutorNames<'_>,
) -> Result<Option<BaseAttribution>, String> {
    let Some(platform) = verdict_platform(db, row).await? else {
        return Ok(None);
    };
    let key = (
        coords.project_id.clone(),
        row.check_name.clone(),
        platform.clone(),
        base.to_string(),
        failures,
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
        &row.check_name,
        &platform,
        chain,
        names,
    )
    .await?;
    let attribution = Some(partition(base, &key.4, main_row.as_ref()));
    remember(&ATTRIBUTIONS, key, (Instant::now(), attribution.clone()));
    Ok(attribution)
}

/// The platform the row's verdict is about, read from the observation that
/// produced it. `None` for a row whose observation recorded none.
async fn verdict_platform(
    db: &LocalDb,
    row: &CheckResultCacheEntry,
) -> Result<Option<String>, String> {
    db.query_opt_text(
        "SELECT verdict_platform FROM check_result_observations
          WHERE project_id = ?1 AND check_name = ?2 AND input_hash = ?3 AND tree_hash = ?4
            AND COALESCE(verdict_platform, '') != ''
          ORDER BY ran_at DESC LIMIT 1",
        (
            row.project_id.clone(),
            row.check_name.clone(),
            row.input_hash.clone(),
            row.tree_hash.clone(),
        ),
    )
    .await
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
        }
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
        assert_eq!(attribution.charge_clause(), "none is this branch's");
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

    #[test]
    fn only_a_test_runners_complete_red_is_attributed() {
        let row = |parser: &str, passed: bool, kind: Option<&str>| CheckResultCacheEntry {
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
