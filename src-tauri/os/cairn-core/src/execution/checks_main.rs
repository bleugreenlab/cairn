//! The main cadence: a whole-suite verdict on the default branch after every
//! merge, announced to a project thread.
//!
//! A check opts in with `mainCadence: { platforms, notify }` in the
//! `.cairn/config.yaml` of the tip being attested. Each declared platform is one
//! **lane**. After every observed advance of the default branch, every lane runs
//! its check's whole-suite command on the new tip through the same producer as
//! `cairn check run`, and the result is posted to the `notify` thread: totals,
//! the tests that turned red or green since the lane's previous complete row,
//! and the merges between the two rows.
//!
//! Scheduling is coalesced per project and never preemptive. A whole-suite run
//! takes most of an hour while merges land every few minutes, so a newer tip
//! never cancels the run in flight; it queues at most one more pass, which
//! attests whatever the tip is when that pass starts. A heartbeat re-arms every
//! project on a fixed interval, so a tip that never got a complete row is
//! retried and a lane that has gone quiet says so.
//!
//! A lane that does not produce a complete per-test verdict is announced louder
//! than any red, with its cause class: **death** (the lane died: placement,
//! capacity, infrastructure, a stalled or killed run) or **no-verdict** (the lane
//! ran to an exit but its result is not a per-test verdict: a test target that
//! did not compile, zero tests, an incomplete enumeration). Under a post-merge
//! detection regime, a silent detector is the dangerous case.
//!
//! Store identity: cadence observations record `cadence = 'main'` and
//! `job_id = 'main:{project_id}'`. A row is measured against the newest such
//! observation for the same check and `verdict_platform` that classifies as
//! complete and ran on the **same executor**: two hosts of one platform differ
//! by provisioning and load as well as by tree, so a diff across hosts reports
//! the hosts as movement. Only when the executor has no complete row does the
//! platform's newest row stand in, and the message says so (see [`Comparison`]).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cairn_common::executor_protocol::{CheckSelectorSource, ExecutorSelector};

use crate::config::project_settings::CheckCommand;
use crate::execution::check_placement::CheckPlacementOverride;
use crate::execution::checks::{
    execute_check_contract, load_checks_contract_at_commit, resolve_check_contract_at_commit,
    CheckRunProducer, CheckRunTarget, MAIN_CADENCE_STREAM_PREFIX,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// A burst of merges settles into one pass.
const SETTLE_WINDOW: Duration = Duration::from_secs(10);
/// How often every project is re-armed without a merge.
pub(crate) const HEARTBEAT: Duration = Duration::from_secs(6 * 60 * 60);
/// The name cadence messages are sent under.
const SENDER: &str = "main cadence";
/// How far back a bracket walk reads before it calls itself truncated.
const BRACKET_SCAN: usize = 400;
const MAX_GROUPS: usize = 15;
const MAX_IDS_PER_GROUP: usize = 10;
const MAX_GREEN: usize = 40;
const MAX_BRACKET: usize = 60;
const MAX_CAUSE_CHARS: usize = 1_500;

/// The `job_id` every cadence observation records.
pub(crate) fn cadence_owner(project_id: &str) -> String {
    format!("main:{project_id}")
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

// ---------------------------------------------------------------------------
// Scheduling
// ---------------------------------------------------------------------------

/// Why a pass was armed. Ordered by strength: a queued pass keeps the
/// strongest reason any of the requests it absorbed carried.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Trigger {
    /// The fixed-interval re-arm. Only this trigger announces a quiet lane.
    Heartbeat,
    /// The launch-time repair edge.
    Startup,
    /// An observed advance of the default branch.
    Merge,
}

struct CadencePass {
    branch: String,
    armed_at_unix_ms: i64,
    queued: Option<Trigger>,
}

/// The per-project cadence slot: one pass in flight, at most one queued.
#[derive(Default)]
pub(crate) struct CadenceSlots(HashMap<String, CadencePass>);

impl CadenceSlots {
    /// Ask for a pass. Returns `true` when the caller must start one; otherwise
    /// the request was folded into the single queued successor.
    fn request(&mut self, project_id: &str, branch: &str, trigger: Trigger, now: i64) -> bool {
        match self.0.get_mut(project_id) {
            Some(pass) => {
                pass.queued = Some(pass.queued.map_or(trigger, |queued| queued.max(trigger)));
                false
            }
            None => {
                self.0.insert(
                    project_id.to_string(),
                    CadencePass {
                        branch: branch.to_string(),
                        armed_at_unix_ms: now,
                        queued: None,
                    },
                );
                true
            }
        }
    }

    /// A pass ended. Returns the queued successor's trigger, keeping the slot
    /// held for it, or releases the slot when nothing queued.
    fn finish(&mut self, project_id: &str, now: i64) -> Option<Trigger> {
        let pass = self.0.get_mut(project_id)?;
        match pass.queued.take() {
            Some(next) => {
                pass.armed_at_unix_ms = now;
                Some(next)
            }
            None => {
                self.0.remove(project_id);
                None
            }
        }
    }

    fn in_flight(&self, project_id: &str) -> Option<(String, i64)> {
        self.0
            .get(project_id)
            .map(|pass| (pass.branch.clone(), pass.armed_at_unix_ms))
    }
}

/// The cadence pass running for `project_id` right now, if any: the branch it
/// attests and when it was armed. While it runs the store holds no row for the
/// new tip, so this is what tells "pending" from "never ran" (CAIRN-3823).
pub(crate) fn in_flight(orch: &Orchestrator, project_id: &str) -> Option<(String, i64)> {
    orch.main_cadence.lock().unwrap().in_flight(project_id)
}

/// Arm a cadence pass for the project's default branch. Calling this on every
/// observed advance is intended: requests coalesce into the running pass's one
/// queued successor.
pub(crate) fn spawn(
    orch: &Orchestrator,
    project_id: String,
    repo_path: String,
    default_branch: String,
    trigger: Trigger,
) {
    let start =
        orch.main_cadence
            .lock()
            .unwrap()
            .request(&project_id, &default_branch, trigger, now_ms());
    if !start {
        return;
    }
    let orch = orch.clone();
    tokio::spawn(async move {
        let mut trigger = trigger;
        loop {
            tokio::time::sleep(SETTLE_WINDOW).await;
            // Supervised, so a panicking pass still releases the slot and is
            // announced rather than holding the project's cadence forever.
            let pass =
                tokio::spawn({
                    let orch = orch.clone();
                    let project_id = project_id.clone();
                    let repo_path = repo_path.clone();
                    let default_branch = default_branch.clone();
                    async move {
                        run_pass(&orch, &project_id, &repo_path, &default_branch, trigger).await
                    }
                })
                .await;
            let failure = match pass {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(join) => Some(format!("the cadence pass panicked: {join}")),
            };
            if let Some(cause) = failure {
                log::error!("main cadence pass for project {project_id} failed: {cause}");
                announce_pass_failure(&orch, &project_id, &repo_path, &cause).await;
            }
            let next = orch
                .main_cadence
                .lock()
                .unwrap()
                .finish(&project_id, now_ms());
            match next {
                Some(next) => trigger = next,
                None => break,
            }
        }
    });
}

/// Arm every local project's cadence: the launch-time repair edge, and each
/// heartbeat tick.
pub(crate) async fn spawn_all_current(orch: &Orchestrator, trigger: Trigger) {
    for db in orch.db.all_dbs().await {
        let projects = db
            .query_all(
                "SELECT id, repo_path, default_branch FROM projects WHERE repo_path != '' AND default_branch IS NOT NULL",
                (),
                |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
            )
            .await;
        match projects {
            Ok(projects) => {
                for (project_id, repo_path, default_branch) in projects {
                    spawn(orch, project_id, repo_path, default_branch, trigger);
                }
            }
            Err(error) => log::warn!("main cadence project scan failed: {error}"),
        }
    }
}

/// Re-arm every project every [`HEARTBEAT`]. The first tick waits a full
/// interval: launch already armed every project.
pub(crate) fn spawn_heartbeat(orch: &Orchestrator) {
    let orch = orch.clone();
    tokio::spawn(async move {
        let mut ticks =
            tokio::time::interval_at(tokio::time::Instant::now() + HEARTBEAT, HEARTBEAT);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            spawn_all_current(&orch, Trigger::Heartbeat).await;
        }
    });
}

// ---------------------------------------------------------------------------
// Lanes
// ---------------------------------------------------------------------------

/// One check on one platform.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CadenceLane {
    pub(crate) check: String,
    pub(crate) platform: String,
    pub(crate) notify: String,
}

/// Every lane a checks contract declares, in a stable order.
fn cadence_lanes(checks: &HashMap<String, CheckCommand>) -> Vec<CadenceLane> {
    let mut lanes: Vec<CadenceLane> = checks
        .iter()
        .filter_map(|(name, check)| check.main_cadence.as_ref().map(|cadence| (name, cadence)))
        .flat_map(|(name, cadence)| {
            cadence.platforms.iter().map(move |platform| CadenceLane {
                check: name.clone(),
                platform: platform.trim().to_ascii_lowercase(),
                notify: cadence.notify.clone(),
            })
        })
        .collect();
    lanes.sort_by(|a, b| (&a.check, &a.platform).cmp(&(&b.check, &b.platform)));
    lanes.dedup_by(|a, b| a.check == b.check && a.platform == b.platform);
    lanes
}

struct PassContext<'a> {
    project_id: &'a str,
    project_key: &'a str,
    repo_path: &'a str,
    default_branch: &'a str,
    repository: &'a Path,
    commit: &'a str,
    trigger: Trigger,
}

async fn run_pass(
    orch: &Orchestrator,
    project_id: &str,
    repo_path: &str,
    default_branch: &str,
    trigger: Trigger,
) -> Result<(), String> {
    let db = crate::execution::routing::owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| error.to_string())?;
    let repo_root = PathBuf::from(repo_path);
    // A stale store attests a stale tip, which its row names honestly; a pass
    // that gave up here would attest nothing at all.
    if let Err(error) = refresh_default_branch(orch, project_id, &repo_root, default_branch).await {
        log::warn!(
            "main cadence for project {project_id}: {error}; attesting the store's current {default_branch}"
        );
    }
    let repository = {
        let jj_binary_path = orch.jj_binary_path.clone();
        let config_dir = orch.config_dir.clone();
        let project_repo = repo_root.clone();
        tokio::task::spawn_blocking(move || {
            let jj = crate::jj::environment(&jj_binary_path, &config_dir);
            crate::jj::coordinate_repository(&jj, &config_dir, &project_repo)
        })
        .await
        .map_err(|error| format!("coordinate repository task failed: {error}"))??
    };
    let commit = cairn_vcs::resolve_coordinate(&repository, default_branch)
        .await
        .map_err(|error| format!("default head '{default_branch}' is unresolvable: {error}"))?;
    let contract = load_checks_contract_at_commit(&repo_root, &commit)
        .await
        .ok_or_else(|| format!("the checks contract at {commit} could not be read"))?;
    let lanes = cadence_lanes(&contract.contract.checks);
    if lanes.is_empty() {
        return Ok(());
    }
    let project_key = project_key(&db, project_id).await?;
    let context = PassContext {
        project_id,
        project_key: &project_key,
        repo_path,
        default_branch,
        repository: &repository,
        commit: &commit,
        trigger,
    };
    futures_util::future::join_all(
        lanes
            .iter()
            .map(|lane| run_lane(orch, db.clone(), &context, lane)),
    )
    .await;
    Ok(())
}

/// Refresh origin and reconcile the canonical bookmark, so the pass attests
/// the real tip rather than whatever the store last saw.
async fn refresh_default_branch(
    orch: &Orchestrator,
    project_id: &str,
    repo_root: &Path,
    default_branch: &str,
) -> Result<(), String> {
    if orch.services.git.remote_get_url(repo_root).is_err() {
        return Ok(());
    }
    crate::orchestrator::base_branch_sync::fetch_origin_serialized(
        orch,
        repo_root,
        default_branch,
        Some(crate::orchestrator::base_branch_sync::FetchRecord {
            project_id,
            source: crate::orchestrator::base_branch_sync::Source::Fetch,
        }),
    )
    .await
    .map_err(|error| format!("fetch failed: {error}"))?;
    let store = crate::jj::project_store_dir(&orch.config_dir, repo_root);
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let guard = orch
        .acquire_jj_store_lock(
            &store,
            crate::orchestrator::StoreLockClass::Background,
            "main cadence canonical refresh",
        )
        .await;
    let _phase = guard.phase("main cadence canonical refresh");
    crate::jj::ensure_store_initialized(&jj, &store, repo_root)
        .map_err(|error| format!("store init failed: {error}"))?;
    crate::jj::reconcile_tracked_bookmark(&jj, &store, default_branch)
        .map_err(|error| format!("bookmark reconcile failed: {error}"))?;
    Ok(())
}

async fn project_key(db: &LocalDb, project_id: &str) -> Result<String, String> {
    Ok(db
        .query_opt_text(
            "SELECT key FROM projects WHERE id = ?1",
            (project_id.to_string(),),
        )
        .await
        .map_err(|error| error.to_string())?
        .unwrap_or_else(|| project_id.to_string()))
}

enum LaneAttempt {
    /// A complete cadence row already answers this lane for these inputs.
    Covered(CadenceRow),
    Verdict(CadenceVerdict),
}

async fn run_lane(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    context: &PassContext<'_>,
    lane: &CadenceLane,
) {
    match attempt_lane(orch, db.clone(), context, lane).await {
        LaneAttempt::Covered(row) => {
            if context.trigger == Trigger::Heartbeat {
                announce_quiet_lane(orch, &db, context, lane, &row).await;
            }
        }
        LaneAttempt::Verdict(verdict) => {
            report_lane(
                orch,
                &db,
                context.project_id,
                context.project_key,
                Some(context.repository),
                lane,
                Some(context.commit),
                verdict,
            )
            .await;
        }
    }
}

async fn attempt_lane(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    context: &PassContext<'_>,
    lane: &CadenceLane,
) -> LaneAttempt {
    let death = |cause: String| LaneAttempt::Verdict(CadenceVerdict::Death { cause });
    let placement = match CheckPlacementOverride::new(
        ExecutorSelector {
            os: Some(lane.platform.clone()),
            ..Default::default()
        },
        CheckSelectorSource::Explicit,
    ) {
        Ok(placement) => placement,
        Err(error) => {
            return death(format!(
                "mainCadence platform {:?} is not a placeable platform: {error}",
                lane.platform
            ))
        }
    };
    let contract = match resolve_check_contract_at_commit(
        orch,
        db.clone(),
        CheckRunTarget {
            job_id: cadence_owner(context.project_id),
            project_id: context.project_id.to_string(),
            project_key: context.project_key.to_string(),
            repository_path: context.repo_path.to_string(),
            // The fork point of the tip with its own branch is the tip, so a
            // whole-suite run carries no changed-file attribution.
            attribution_target: context.default_branch.to_string(),
        },
        context.commit.to_string(),
        &lane.check,
        Some(&placement),
    )
    .await
    {
        Ok(contract) => contract,
        Err(error) => return death(error),
    };
    let names = |executor: &str| orch.fleet.executor_name_for_text(executor);
    // A complete row on any executor of the platform covers these inputs: the
    // question is whether a verdict is owed, not what to diff against.
    match latest_complete_row(
        &db,
        context.project_id,
        context.project_key,
        lane,
        RowFilter {
            input_hash: Some(&contract.context.input_hash),
            ..RowFilter::default()
        },
        &names,
    )
    .await
    {
        Ok(Some(row)) => return LaneAttempt::Covered(row),
        Ok(None) => {}
        Err(error) => log::warn!(
            "main cadence {} on {}: coverage lookup failed, running anyway: {error}",
            lane.check,
            lane.platform
        ),
    }
    let short = short_commit(context.commit);
    let result = match execute_check_contract(
        orch,
        db.clone(),
        contract,
        CheckRunProducer {
            tool_use_id: format!(
                "{MAIN_CADENCE_STREAM_PREFIX}{}:{}:{}",
                context.project_id, lane.check, lane.platform
            ),
            node_kind: format!("main-cadence {} @ {short}", lane.platform),
            owner_job_id: None,
            purpose: Some(crate::fleet::purpose::PurposeKey::main_cadence(
                context.project_id,
                &lane.check,
                &lane.platform,
            )),
        },
        // A detector never answers with a verdict another run produced.
        true,
        Arc::new(|_| {}),
    )
    .await
    {
        Ok(result) => result,
        Err(error) => return death(error),
    };
    if let Some(no_verdict) = result.no_verdict.as_ref() {
        return death(format!("{}: {}", no_verdict.kind, no_verdict.cause));
    }
    let Some(observation_id) = result.observation_id.as_deref() else {
        return LaneAttempt::Verdict(CadenceVerdict::NoVerdict {
            row: None,
            cause: format!(
                "the run exited {} but recorded no observation: {}",
                result
                    .exit_code
                    .map_or_else(|| "without a code".to_string(), |code| code.to_string()),
                result
                    .not_recorded
                    .as_deref()
                    .unwrap_or("no cause reported")
            ),
        });
    };
    match load_row(&db, context.project_key, observation_id, &names).await {
        Ok(row) => LaneAttempt::Verdict(classify_row(row)),
        Err(error) => LaneAttempt::Verdict(CadenceVerdict::NoVerdict {
            row: None,
            cause: format!("observation {observation_id} could not be read back: {error}"),
        }),
    }
}

#[allow(clippy::too_many_arguments)]
async fn report_lane(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    repository: Option<&Path>,
    lane: &CadenceLane,
    commit: Option<&str>,
    verdict: CadenceVerdict,
) {
    let names = |executor: &str| orch.fleet.executor_name_for_text(executor);
    let comparison = comparison_for(db, project_id, project_key, lane, verdict.row(), &names)
        .await
        .unwrap_or_else(|error| {
            log::warn!("main cadence previous-row lookup failed: {error}");
            Comparison::Baseline
        });
    let bracket = match (comparison.previous(), repository, commit) {
        (Some(previous), Some(repository), Some(commit)) => {
            merge_bracket(
                orch,
                db,
                project_id,
                project_key,
                repository,
                &previous.commit,
                commit,
            )
            .await
        }
        _ => Bracket::default(),
    };
    let content = render_report(&CadenceReport {
        check: &lane.check,
        platform: &lane.platform,
        commit,
        verdict: &verdict,
        comparison: &comparison,
        bracket: &bracket,
        now_ms: now_ms(),
    });
    if verdict.is_complete() {
        log::info!(
            "main cadence {} on {}: row recorded",
            lane.check,
            lane.platform
        );
    } else {
        log::error!("{content}");
    }
    deliver(orch, db, project_id, &lane.notify, &content).await;
}

async fn announce_quiet_lane(
    orch: &Orchestrator,
    db: &LocalDb,
    context: &PassContext<'_>,
    lane: &CadenceLane,
    covered: &CadenceRow,
) {
    let latest = db
        .query_opt(
            "SELECT MAX(ran_at) FROM check_result_observations
              WHERE project_id = ?1 AND check_name = ?2 AND cadence = 'main' AND verdict_platform = ?3",
            (
                context.project_id.to_string(),
                lane.check.clone(),
                lane.platform.clone(),
            ),
            |row| row.opt_i64(0),
        )
        .await
        .ok()
        .flatten()
        .flatten()
        .unwrap_or(covered.ran_at_ms);
    let now = now_ms();
    if now - latest < HEARTBEAT.as_millis() as i64 {
        return;
    }
    let content = render_heartbeat(&lane.check, &lane.platform, context.commit, covered, now);
    deliver(orch, db, context.project_id, &lane.notify, &content).await;
}

/// A pass that failed before reaching its lanes still owes each lane its
/// loudest message. The tip's contract was unreadable, so the lanes come from
/// the project checkout's configuration.
async fn announce_pass_failure(
    orch: &Orchestrator,
    project_id: &str,
    repo_path: &str,
    cause: &str,
) {
    let Some(checks) = crate::config::project_settings::load_checks(Path::new(repo_path)) else {
        return;
    };
    let lanes = cadence_lanes(&checks);
    if lanes.is_empty() {
        return;
    }
    let Ok(db) = crate::execution::routing::owning_db_for_project(&orch.db, project_id).await
    else {
        return;
    };
    let Ok(project_key) = project_key(&db, project_id).await else {
        return;
    };
    for lane in lanes {
        report_lane(
            orch,
            &db,
            project_id,
            &project_key,
            None,
            &lane,
            None,
            CadenceVerdict::Death {
                cause: format!("the cadence pass failed before this lane could run: {cause}"),
            },
        )
        .await;
    }
}

async fn deliver(orch: &Orchestrator, db: &LocalDb, project_id: &str, thread: &str, content: &str) {
    let thread_id = match db
        .query_opt_text(
            "SELECT id FROM threads WHERE project_id = ?1 AND name = ?2 LIMIT 1",
            (project_id.to_string(), thread.to_string()),
        )
        .await
    {
        Ok(Some(thread_id)) => thread_id,
        Ok(None) => {
            log::error!(
                "main cadence notify thread {thread:?} does not exist; undelivered:\n{content}"
            );
            return;
        }
        Err(error) => {
            log::error!(
                "main cadence notify thread lookup failed: {error}; undelivered:\n{content}"
            );
            return;
        }
    };
    if let Err(error) = crate::messages::delivery::append_thread_message(
        orch, db, &thread_id, None, SENDER, content,
    )
    .await
    {
        log::error!(
            "main cadence delivery to thread {thread:?} failed: {error}; undelivered:\n{content}"
        );
    }
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TestOutcome {
    Passed,
    Skipped,
    /// The failure's signature: see [`failure_signature`].
    Failed(String),
}

/// One cadence observation, read back from the store.
#[derive(Clone, Debug)]
pub(crate) struct CadenceRow {
    pub(crate) id: String,
    /// Canonical citation: `cairn://p/{key}/check-observations/{handle}`.
    pub(crate) reference: String,
    pub(crate) commit: String,
    pub(crate) platform: String,
    /// The executor identity the row recorded: what previous rows are keyed by.
    pub(crate) executor_id: Option<String>,
    /// The executor's public name, for display.
    pub(crate) executor: Option<String>,
    pub(crate) ran_at_ms: i64,
    pub(crate) verdict_passed: bool,
    pub(crate) exit_code: i64,
    pub(crate) complete: bool,
    pub(crate) tests: BTreeMap<String, TestOutcome>,
    pub(crate) output_tail: String,
}

impl CadenceRow {
    fn count(&self, wanted: fn(&TestOutcome) -> bool) -> usize {
        self.tests
            .values()
            .filter(|outcome| wanted(outcome))
            .count()
    }
    fn passed(&self) -> usize {
        self.count(|outcome| *outcome == TestOutcome::Passed)
    }
    fn skipped(&self) -> usize {
        self.count(|outcome| *outcome == TestOutcome::Skipped)
    }
    fn failed(&self) -> usize {
        self.count(|outcome| matches!(outcome, TestOutcome::Failed(_)))
    }
    fn ran(&self) -> usize {
        self.passed() + self.failed()
    }
    fn failure(&self, test: &str) -> Option<&str> {
        match self.tests.get(test) {
            Some(TestOutcome::Failed(signature)) => Some(signature),
            _ => None,
        }
    }
}

/// What a lane's attempt amounts to.
#[derive(Clone, Debug)]
pub(crate) enum CadenceVerdict {
    /// A per-test verdict on the whole suite.
    Complete(CadenceRow),
    /// The lane ran to an exit, but its result is not a per-test verdict.
    NoVerdict {
        row: Option<CadenceRow>,
        cause: String,
    },
    /// The lane died before producing a result.
    Death { cause: String },
}

impl CadenceVerdict {
    fn row(&self) -> Option<&CadenceRow> {
        match self {
            CadenceVerdict::Complete(row) => Some(row),
            CadenceVerdict::NoVerdict { row, .. } => row.as_ref(),
            CadenceVerdict::Death { .. } => None,
        }
    }
    fn is_complete(&self) -> bool {
        matches!(self, CadenceVerdict::Complete(_))
    }
}

/// Whether a stored row is a per-test verdict on the whole suite. A row with
/// zero tests is never one, whatever its exit code (rulings 695/706): an empty
/// or unknown selection is the absence of a verdict, not a green.
fn classify_row(row: CadenceRow) -> CadenceVerdict {
    let cause = if row.ran() == 0 {
        Some(format!(
            "no test produced a result (exit {}): a test target that did not compile, or a runner that stopped before running any test",
            row.exit_code
        ))
    } else if !row.complete {
        Some(
            "the per-test enumeration is incomplete, so tests that did not report are unobserved"
                .to_string(),
        )
    } else if !row.verdict_passed && row.failed() == 0 {
        Some(format!(
            "the suite exited {} with no failing test: the runner failed outside any test (a test target that did not compile, a crash, or a hang), so the {} tests that did report are not the whole suite",
            row.exit_code,
            row.ran()
        ))
    } else {
        None
    };
    match cause {
        Some(cause) => CadenceVerdict::NoVerdict {
            row: Some(row),
            cause,
        },
        None => CadenceVerdict::Complete(row),
    }
}

/// Resolves a recorded executor identity to its public name.
type ExecutorNames<'a> = &'a (dyn Fn(&str) -> String + Sync);

async fn load_row(
    db: &LocalDb,
    project_key: &str,
    observation_id: &str,
    names: ExecutorNames<'_>,
) -> Result<CadenceRow, String> {
    let head = db
        .query_opt(
            "SELECT id, COALESCE(public_handle, id), commit_sha, COALESCE(verdict_platform, ''),
                    executor_id, ran_at, verdict, exit_code, complete, output_tail
               FROM check_result_observations WHERE id = ?1",
            (observation_id.to_string(),),
            |row| {
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.text(2)?,
                    row.text(3)?,
                    row.opt_text(4)?,
                    row.i64(5)?,
                    row.text(6)?,
                    row.i64(7)?,
                    row.i64(8)?,
                    row.text(9)?,
                ))
            },
        )
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("observation {observation_id} does not exist"))?;
    let (id, handle, commit, platform, executor_id, ran_at, verdict, exit_code, complete, tail) =
        head;
    let tests = db
        .query_all(
            "SELECT test_id, status, failure_excerpt FROM check_test_results WHERE observation_id = ?1",
            (id.clone(),),
            |row| Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?)),
        )
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|(test, status, excerpt)| {
            let outcome = match status.as_str() {
                "passed" => TestOutcome::Passed,
                "skipped" => TestOutcome::Skipped,
                _ => TestOutcome::Failed(failure_signature(excerpt.as_deref().unwrap_or(""))),
            };
            (test, outcome)
        })
        .collect();
    Ok(CadenceRow {
        reference: format!("cairn://p/{project_key}/check-observations/{handle}"),
        id,
        commit,
        platform,
        executor: executor_id.as_deref().map(names),
        executor_id,
        ran_at_ms: ran_at,
        verdict_passed: verdict == "passed",
        exit_code,
        complete: complete != 0,
        tests,
        output_tail: tail,
    })
}

/// Narrows [`latest_complete_row`]'s candidates; every field is optional.
#[derive(Clone, Copy, Default)]
struct RowFilter<'a> {
    /// Only rows recorded for this input key.
    input_hash: Option<&'a str>,
    /// Only rows recorded on this executor.
    executor_id: Option<&'a str>,
    /// Never this row.
    excluding: Option<&'a str>,
}

/// The newest cadence row for `lane` that classifies as complete and passes
/// `filter`.
async fn latest_complete_row(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    lane: &CadenceLane,
    filter: RowFilter<'_>,
    names: ExecutorNames<'_>,
) -> Result<Option<CadenceRow>, String> {
    let candidates = db
        .query_all(
            "SELECT id FROM check_result_observations
              WHERE project_id = ?1 AND check_name = ?2 AND cadence = 'main'
                AND verdict_platform = ?3 AND complete = 1 AND id != ?4
                AND (?5 = '' OR input_hash = ?5)
                AND (?6 = '' OR executor_id = ?6)
              ORDER BY ran_at DESC LIMIT 8",
            (
                project_id.to_string(),
                lane.check.clone(),
                lane.platform.clone(),
                filter.excluding.unwrap_or("").to_string(),
                filter.input_hash.unwrap_or("").to_string(),
                filter.executor_id.unwrap_or("").to_string(),
            ),
            |row| row.text(0),
        )
        .await
        .map_err(|error| error.to_string())?;
    for id in candidates {
        if let CadenceVerdict::Complete(row) =
            classify_row(load_row(db, project_key, &id, names).await?)
        {
            return Ok(Some(row));
        }
    }
    Ok(None)
}

/// What a cadence attempt is measured against. Two hosts of one platform
/// differ by provisioning and load as well as by tree, so the previous row is
/// chosen by executor first, and a comparison that crosses hosts says so.
#[derive(Clone, Debug, Default)]
pub(crate) enum Comparison {
    /// No complete row exists for the lane on any executor.
    #[default]
    Baseline,
    /// The previous complete row on the attempt's own executor: the diff is
    /// tree movement. `neighbour` is another executor's complete row that ran
    /// after `previous`, when one did: the other host's view of the same
    /// window, which separates the host census from movement.
    SameExecutor {
        previous: CadenceRow,
        neighbour: Option<Box<CadenceRow>>,
    },
    /// The attempt's executor has no earlier complete row, so the platform's
    /// newest (another executor's) stands in, and host differences cannot be
    /// told from movement.
    OtherExecutor { previous: CadenceRow },
    /// The attempt names no executor (a death), so the platform's newest
    /// complete row on any executor stands in.
    AnyExecutor { previous: CadenceRow },
}

impl Comparison {
    fn previous(&self) -> Option<&CadenceRow> {
        match self {
            Comparison::Baseline => None,
            Comparison::SameExecutor { previous, .. }
            | Comparison::OtherExecutor { previous }
            | Comparison::AnyExecutor { previous } => Some(previous),
        }
    }
}

/// Choose the comparison for an attempt on `executor_id` from that executor's
/// newest complete row and the platform's newest complete row on any executor.
fn select_comparison(
    executor_id: Option<&str>,
    same_executor: Option<CadenceRow>,
    newest: Option<CadenceRow>,
) -> Comparison {
    let Some(executor_id) = executor_id else {
        return newest.map_or(Comparison::Baseline, |previous| Comparison::AnyExecutor {
            previous,
        });
    };
    match same_executor {
        Some(previous) => {
            let neighbour = newest.filter(|row| {
                row.executor_id.as_deref() != Some(executor_id)
                    && row.ran_at_ms > previous.ran_at_ms
            });
            Comparison::SameExecutor {
                previous,
                neighbour: neighbour.map(Box::new),
            }
        }
        None => newest.map_or(Comparison::Baseline, |previous| Comparison::OtherExecutor {
            previous,
        }),
    }
}

/// What `current` (the attempt's own row, when it recorded one) is measured
/// against.
async fn comparison_for(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    lane: &CadenceLane,
    current: Option<&CadenceRow>,
    names: ExecutorNames<'_>,
) -> Result<Comparison, String> {
    let excluding = current.map(|row| row.id.as_str());
    let executor_id = current.and_then(|row| row.executor_id.as_deref());
    let same_executor = match executor_id {
        Some(executor_id) => {
            latest_complete_row(
                db,
                project_id,
                project_key,
                lane,
                RowFilter {
                    executor_id: Some(executor_id),
                    excluding,
                    ..RowFilter::default()
                },
                names,
            )
            .await?
        }
        None => None,
    };
    let newest = latest_complete_row(
        db,
        project_id,
        project_key,
        lane,
        RowFilter {
            excluding,
            ..RowFilter::default()
        },
        names,
    )
    .await?;
    Ok(select_comparison(executor_id, same_executor, newest))
}

/// The line of a failure excerpt that says what went wrong. A Rust panic opens
/// with `thread '<test>' (<tid>) panicked at <file>:<line>:` — unique to every
/// test — and states the failure on the next line, so that line is the
/// signature. Tests that fail the same way share a signature, which is what
/// lets one intermittent race that flips a dozen tests read as one group.
fn failure_signature(excerpt: &str) -> String {
    let mut lines = excerpt
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty());
    let Some(first) = lines.next() else {
        return "(no failure excerpt)".to_string();
    };
    let line = if first.starts_with("thread '") && first.contains("panicked at") {
        lines.next().unwrap_or(first)
    } else {
        first
    };
    truncate(line, 200)
}

// ---------------------------------------------------------------------------
// Bracket
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BracketMerge {
    pub(crate) commit: String,
    pub(crate) subject: String,
    pub(crate) pr: Option<i64>,
    pub(crate) issue: Option<String>,
}

/// The first-parent commits between two rows' commits, oldest first.
#[derive(Clone, Debug, Default)]
pub(crate) struct Bracket {
    pub(crate) merges: Vec<BracketMerge>,
    /// The walk stopped before reaching the earlier row's commit.
    pub(crate) truncated: bool,
    /// Why the bracket could not be read at all.
    pub(crate) unavailable: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LogEntry {
    commit: String,
    parents: Vec<String>,
    subject: String,
}

const BRACKET_TEMPLATE: &str = r#"commit_id ++ "\t" ++ parents.map(|c| c.commit_id()).join(",") ++ "\t" ++ description.first_line() ++ "\n""#;

fn parse_log(output: &str) -> Vec<LogEntry> {
    output
        .lines()
        .filter_map(|line| {
            let mut fields = line.splitn(3, '\t');
            let commit = fields.next()?.trim().to_string();
            let parents = fields
                .next()?
                .split(',')
                .map(str::trim)
                .filter(|parent| !parent.is_empty())
                .map(str::to_string)
                .collect();
            let subject = fields.next().unwrap_or("").trim().to_string();
            (!commit.is_empty()).then_some(LogEntry {
                commit,
                parents,
                subject,
            })
        })
        .collect()
}

/// Walk `tip`'s first-parent chain back to `base` (exclusive) through the
/// commits `entries` holds. Second-parent history is never entered: on main,
/// each first-parent commit is one landing. Returns the chain oldest first and
/// whether `base` was reached.
fn first_parent_chain<'a>(
    entries: &'a [LogEntry],
    base: &str,
    tip: &str,
) -> (Vec<&'a LogEntry>, bool) {
    let by_commit: HashMap<&str, &LogEntry> = entries
        .iter()
        .map(|entry| (entry.commit.as_str(), entry))
        .collect();
    let mut chain = Vec::new();
    let mut current = tip;
    let reached = loop {
        if current == base {
            break true;
        }
        let Some(entry) = by_commit.get(current) else {
            break false;
        };
        chain.push(*entry);
        let Some(parent) = entry.parents.first() else {
            break false;
        };
        current = parent;
    };
    chain.reverse();
    (chain, reached)
}

/// The pull request a squash-merge subject names with a trailing `(#N)`.
fn subject_pr(subject: &str) -> Option<i64> {
    let start = subject.rfind("(#")?;
    let rest = &subject[start + 2..];
    let end = rest.find(')')?;
    rest[..end].parse().ok()
}

/// The first `{key}/N` issue mention in a subject, rendered as `key/N`.
fn subject_issue(subject: &str, project_key: &str) -> Option<String> {
    let key = project_key.to_ascii_lowercase();
    let lower = subject.to_ascii_lowercase();
    let needle = format!("{key}/");
    let mut from = 0;
    while let Some(found) = lower[from..].find(&needle) {
        let at = from + found;
        let boundary = lower[..at]
            .chars()
            .next_back()
            .is_none_or(|before| !before.is_ascii_alphanumeric());
        let digits: String = lower[at + needle.len()..]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        if boundary && !digits.is_empty() {
            return Some(format!("{key}/{digits}"));
        }
        from = at + needle.len();
    }
    None
}

async fn merge_bracket(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    repository: &Path,
    base: &str,
    tip: &str,
) -> Bracket {
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let revset = format!("{base}..{tip}");
    let limit = BRACKET_SCAN.to_string();
    let output = match jj.run(
        repository,
        &[
            "log",
            "--ignore-working-copy",
            "--no-graph",
            "--limit",
            &limit,
            "-r",
            &revset,
            "-T",
            BRACKET_TEMPLATE,
        ],
        "jj log (main cadence bracket)",
    ) {
        Ok(output) => output,
        Err(error) => {
            return Bracket {
                unavailable: Some(error),
                ..Bracket::default()
            }
        }
    };
    let entries = parse_log(&output);
    let (chain, reached) = first_parent_chain(&entries, base, tip);
    let key = project_key.to_ascii_lowercase();
    let mut merges = Vec::with_capacity(chain.len());
    for entry in chain {
        let pr = subject_pr(&entry.subject);
        let resolved = match pr {
            Some(pr) => db
                .query_opt(
                    "SELECT i.number FROM merge_requests mr JOIN issues i ON i.id = mr.issue_id
                      WHERE mr.project_id = ?1 AND mr.github_pr_number = ?2 LIMIT 1",
                    (project_id.to_string(), pr),
                    |row| row.i64(0),
                )
                .await
                .ok()
                .flatten()
                .map(|number| format!("{key}/{number}")),
            None => None,
        };
        merges.push(BracketMerge {
            commit: entry.commit.clone(),
            subject: entry.subject.clone(),
            pr,
            issue: resolved.or_else(|| subject_issue(&entry.subject, project_key)),
        });
    }
    Bracket {
        merges,
        truncated: !reached,
        unavailable: None,
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

pub(crate) struct CadenceReport<'a> {
    pub(crate) check: &'a str,
    pub(crate) platform: &'a str,
    /// The tip the lane attempted, when it was resolved.
    pub(crate) commit: Option<&'a str>,
    pub(crate) verdict: &'a CadenceVerdict,
    /// What the attempt is measured against.
    pub(crate) comparison: &'a Comparison,
    pub(crate) bracket: &'a Bracket,
    pub(crate) now_ms: i64,
}

fn short_commit(commit: &str) -> &str {
    commit.get(..12).unwrap_or(commit)
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.to_string()
    } else {
        format!("{}…", text.chars().take(max).collect::<String>())
    }
}

fn thousands(value: usize) -> String {
    let digits = value.to_string();
    let mut out = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

fn plural(count: usize) -> &'static str {
    if count == 1 {
        ""
    } else {
        "s"
    }
}

fn host(row: &CadenceRow) -> &str {
    row.executor.as_deref().unwrap_or("unknown")
}

fn age(now_ms: i64, then_ms: i64) -> String {
    crate::clock::format_elapsed((now_ms - then_ms).max(0) / 1000)
}

fn totals(row: &CadenceRow) -> String {
    format!(
        "{} ran · {} passed · {} failed · {} skipped",
        thousands(row.ran()),
        thousands(row.passed()),
        thousands(row.failed()),
        thousands(row.skipped())
    )
}

fn row_identity(out: &mut String, row: &CadenceRow) {
    out.push_str(&format!(
        "- Tree: commit `{}` · platform `{}` · executor `{}`\n- Observation: {}\n- Totals: {}\n",
        row.commit,
        row.platform,
        row.executor.as_deref().unwrap_or("unknown"),
        row.reference,
        totals(row)
    ));
}

fn render_bracket(out: &mut String, heading: &str, bracket: &Bracket) {
    if let Some(error) = bracket.unavailable.as_deref() {
        out.push_str(&format!(
            "\n**{heading}:** could not be read ({}).\n",
            truncate(error, 300)
        ));
        return;
    }
    out.push_str(&format!(
        "\n**{heading} ({}{})**\n",
        bracket.merges.len(),
        if bracket.truncated {
            "+, walk truncated"
        } else {
            ""
        }
    ));
    if bracket.merges.is_empty() {
        out.push_str("- none\n");
    }
    for merge in bracket.merges.iter().rev().take(MAX_BRACKET) {
        let mut refs = Vec::new();
        if let Some(pr) = merge.pr {
            refs.push(format!("#{pr}"));
        }
        if let Some(issue) = merge.issue.as_deref() {
            refs.push(issue.to_string());
        }
        let refs = if refs.is_empty() {
            String::new()
        } else {
            format!(" {}", refs.join(" · "))
        };
        out.push_str(&format!(
            "- `{}`{refs} — {}\n",
            short_commit(&merge.commit),
            truncate(&merge.subject, 140)
        ));
    }
    if bracket.merges.len() > MAX_BRACKET {
        out.push_str(&format!(
            "- … and {} earlier\n",
            bracket.merges.len() - MAX_BRACKET
        ));
    }
}

/// Newly-red tests grouped by failure signature, largest group first.
fn newly_red_groups<'a>(
    current: &'a CadenceRow,
    previous: Option<&CadenceRow>,
) -> Vec<(&'a str, Vec<&'a str>)> {
    let mut groups: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (test, outcome) in &current.tests {
        let TestOutcome::Failed(signature) = outcome else {
            continue;
        };
        if previous.is_some_and(|previous| previous.failure(test).is_some()) {
            continue;
        }
        groups
            .entry(signature.as_str())
            .or_default()
            .push(test.as_str());
    }
    let mut groups: Vec<_> = groups.into_iter().collect();
    groups.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then(a.0.cmp(b.0)));
    groups
}

/// Pass/fail of one test in a row; `None` when it was skipped or absent.
fn passed_in(row: &CadenceRow, test: &str) -> Option<bool> {
    match row.tests.get(test)? {
        TestOutcome::Passed => Some(true),
        TestOutcome::Failed(_) => Some(false),
        TestOutcome::Skipped => None,
    }
}

/// Tests that differ from another host's row while agreeing with this host's
/// previous row: a difference between the hosts, not movement in the tree.
#[derive(Debug, Default)]
struct HostCensus<'a> {
    /// Red here, passed on the other host.
    red_here: Vec<&'a str>,
    /// Passed here, red on the other host.
    green_here: Vec<&'a str>,
}

fn host_census<'a>(
    current: &'a CadenceRow,
    previous: &CadenceRow,
    neighbour: &CadenceRow,
) -> HostCensus<'a> {
    let mut census = HostCensus::default();
    for test in current.tests.keys() {
        let Some(here) = passed_in(current, test) else {
            continue;
        };
        if passed_in(neighbour, test).is_none_or(|there| there == here)
            || passed_in(previous, test) != Some(here)
        {
            continue;
        }
        if here {
            census.green_here.push(test);
        } else {
            census.red_here.push(test);
        }
    }
    census
}

/// The one-host tag on a newly-red group: how many of its tests passed on the
/// other host's row in the same window.
fn one_host(tests: &[&str], neighbour: Option<&CadenceRow>) -> String {
    let Some(neighbour) = neighbour else {
        return String::new();
    };
    let passed = tests
        .iter()
        .filter(|test| passed_in(neighbour, test) == Some(true))
        .count();
    if passed == 0 {
        String::new()
    } else if passed == tests.len() {
        format!(" · **one host**: passed on `{}`", host(neighbour))
    } else {
        format!(
            " · **one host** for {passed} of {}: passed on `{}`",
            tests.len(),
            host(neighbour)
        )
    }
}

fn render_tests(out: &mut String, indent: &str, tests: &[&str]) {
    for test in tests.iter().take(MAX_GREEN) {
        out.push_str(&format!("{indent}- `{test}`\n"));
    }
    if tests.len() > MAX_GREEN {
        out.push_str(&format!(
            "{indent}- … and {} more\n",
            tests.len() - MAX_GREEN
        ));
    }
}

fn render_groups(out: &mut String, groups: &[(&str, Vec<&str>)], neighbour: Option<&CadenceRow>) {
    for (signature, tests) in groups.iter().take(MAX_GROUPS) {
        out.push_str(&format!(
            "- **{} test{}**{} — `{}`\n",
            tests.len(),
            plural(tests.len()),
            one_host(tests, neighbour),
            signature.replace('`', "'")
        ));
        for test in tests.iter().take(MAX_IDS_PER_GROUP) {
            out.push_str(&format!("  - `{test}`\n"));
        }
        if tests.len() > MAX_IDS_PER_GROUP {
            out.push_str(&format!(
                "  - … and {} more\n",
                tests.len() - MAX_IDS_PER_GROUP
            ));
        }
    }
    if groups.len() > MAX_GROUPS {
        let rest: usize = groups[MAX_GROUPS..]
            .iter()
            .map(|(_, tests)| tests.len())
            .sum();
        out.push_str(&format!(
            "- … and {} more group{} ({rest} tests)\n",
            groups.len() - MAX_GROUPS,
            plural(groups.len() - MAX_GROUPS)
        ));
    }
}

/// The "Compared with" line: which row the diff is against, and on which host.
fn render_basis(out: &mut String, report: &CadenceReport<'_>, row: &CadenceRow) {
    let line = match report.comparison {
        Comparison::Baseline => format!(
            "nothing. No earlier complete cadence row exists for {} on {}, so this row is the baseline and every failure is listed as newly red.",
            report.check, report.platform
        ),
        Comparison::SameExecutor { previous, .. } => format!(
            "{}, the previous row on `{}` (commit `{}`, ran {} earlier)",
            previous.reference,
            host(previous),
            short_commit(&previous.commit),
            age(row.ran_at_ms, previous.ran_at_ms)
        ),
        Comparison::OtherExecutor { previous } => format!(
            "{} on `{}` (commit `{}`, ran {} earlier). No earlier complete row exists on `{}`, so this is a **cross-host comparison**: what differs below may be the two hosts (provisioning, load, toolchain) rather than the tree, and none of it is reported as newly red or green.",
            previous.reference,
            host(previous),
            short_commit(&previous.commit),
            age(row.ran_at_ms, previous.ran_at_ms),
            host(row)
        ),
        Comparison::AnyExecutor { previous } => format!(
            "{} on `{}` (commit `{}`, ran {} earlier). This row names no executor, so the comparison may cross hosts.",
            previous.reference,
            host(previous),
            short_commit(&previous.commit),
            age(row.ran_at_ms, previous.ran_at_ms)
        ),
    };
    out.push_str(&format!("- Compared with: {line}\n"));
}

fn render_host_census(
    out: &mut String,
    row: &CadenceRow,
    previous: &CadenceRow,
    neighbour: &CadenceRow,
) {
    let census = host_census(row, previous, neighbour);
    let total = census.red_here.len() + census.green_here.len();
    if total == 0 {
        return;
    }
    let (here, there) = (host(row), host(neighbour));
    out.push_str(&format!(
        "\n**HOST CENSUS against `{there}`: {total}** — these differ from {} (`{there}`, commit `{}`, ran {} earlier) but match this host's previous row, so they are a difference between the hosts, not movement in the tree.\n",
        neighbour.reference,
        short_commit(&neighbour.commit),
        age(row.ran_at_ms, neighbour.ran_at_ms)
    ));
    if !census.red_here.is_empty() {
        out.push_str(&format!(
            "- Red on `{here}`, passed on `{there}`: {}\n",
            census.red_here.len()
        ));
        render_tests(out, "  ", &census.red_here);
    }
    if !census.green_here.is_empty() {
        out.push_str(&format!(
            "- Passed on `{here}`, red on `{there}`: {}\n",
            census.green_here.len()
        ));
        render_tests(out, "  ", &census.green_here);
    }
}

fn render_complete(out: &mut String, report: &CadenceReport<'_>, row: &CadenceRow) {
    let previous = report.comparison.previous();
    let (neighbour, cross_host) = match report.comparison {
        Comparison::SameExecutor { neighbour, .. } => (neighbour.as_deref(), None),
        Comparison::OtherExecutor { previous } => (None, Some(host(previous))),
        Comparison::Baseline | Comparison::AnyExecutor { .. } => (None, None),
    };
    let groups = newly_red_groups(row, previous);
    let newly_red: usize = groups.iter().map(|(_, tests)| tests.len()).sum();
    let (newly_green, unreported): (Vec<&str>, Vec<&str>) = previous
        .map(|previous| {
            previous
                .tests
                .iter()
                .filter(|(test, outcome)| {
                    matches!(outcome, TestOutcome::Failed(_)) && row.failure(test).is_none()
                })
                .map(|(test, _)| test.as_str())
                .partition(|test| row.tests.get(*test) == Some(&TestOutcome::Passed))
        })
        .unwrap_or_default();
    let still_red = row.failed() - newly_red;
    let headline = if row.failed() == 0 {
        format!("all {} passed", thousands(row.ran()))
    } else {
        format!("{} failed", thousands(row.failed()))
    };
    let movement = match cross_host {
        Some(other) => format!(
            "cross-host against `{other}`: {newly_red} red here only, {} green here only",
            newly_green.len()
        ),
        None => format!("{newly_red} newly red, {} newly green", newly_green.len()),
    };
    out.push_str(&format!(
        "**Main cadence · {} · {} on `{}` · `{}`: {headline} — {movement}**\n\n",
        report.check,
        report.platform,
        host(row),
        short_commit(&row.commit)
    ));
    row_identity(out, row);
    render_basis(out, report, row);
    if newly_red > 0 {
        let title = match cross_host {
            Some(other) => format!("RED HERE, NOT RED ON `{other}`"),
            None => "NEWLY RED".to_string(),
        };
        out.push_str(&format!(
            "\n**{title}: {newly_red} test{} in {} failure group{}**\n",
            plural(newly_red),
            groups.len(),
            plural(groups.len())
        ));
        render_groups(out, &groups, neighbour);
    }
    if !newly_green.is_empty() {
        let title = match cross_host {
            Some(other) => format!("GREEN HERE, RED ON `{other}`"),
            None => "NEWLY GREEN".to_string(),
        };
        out.push_str(&format!("\n**{title}: {}**\n", newly_green.len()));
        render_tests(out, "", &newly_green);
    }
    if !unreported.is_empty() {
        out.push_str(&format!(
            "\n{} previously red test{} reported no result in this row (renamed, removed, or skipped), so {} not counted as green.\n",
            unreported.len(),
            if unreported.len() == 1 { "" } else { "s" },
            if unreported.len() == 1 { "it is" } else { "they are" }
        ));
    }
    if previous.is_some() {
        let label = if cross_host.is_some() {
            "Red on both hosts"
        } else {
            "Still red"
        };
        out.push_str(&format!("\n**{label}:** {still_red} (not listed)\n"));
    }
    if let Comparison::SameExecutor {
        previous,
        neighbour: Some(neighbour),
    } = report.comparison
    {
        render_host_census(out, row, previous, neighbour);
    }
    if previous.is_some() {
        render_bracket(out, "Merges since the compared row", report.bracket);
    }
}

fn render_incomplete(
    out: &mut String,
    report: &CadenceReport<'_>,
    row: Option<&CadenceRow>,
    cause: &str,
    death: bool,
) {
    let tip = report
        .commit
        .or(row.map(|row| row.commit.as_str()))
        .map(|commit| format!(" · `{}`", short_commit(commit)))
        .unwrap_or_default();
    out.push_str(&format!(
        "🚨 **MAIN CADENCE DID NOT COMPLETE · {} · {}{tip}** — main is unobserved for {} on {} until a cadence row completes.\n\n",
        report.check, report.platform, report.check, report.platform
    ));
    if death {
        out.push_str("- Cause class: **death** — the lane died before it produced a result (the execution side of the seam: no machine, capacity, infrastructure, a stalled or killed run).\n");
    } else {
        out.push_str("- Cause class: **no-verdict** — the lane ran to an exit, but its result is not a per-test verdict (the testing side of the seam: a test target that did not compile, zero tests, an incomplete enumeration). It is not green.\n");
    }
    out.push_str(&format!("- Cause: {cause}\n"));
    if let Some(row) = row {
        row_identity(out, row);
        let tail = row.output_tail.trim();
        if !tail.is_empty() {
            let tail: String = tail
                .lines()
                .rev()
                .take(25)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\n");
            out.push_str(&format!(
                "\nOutput tail:\n```\n{}\n```\n",
                truncate(&tail, MAX_CAUSE_CHARS).replace("```", "'''")
            ));
        }
    }
    let since = |previous: &CadenceRow| {
        format!(
            "commit `{}`, ran {} ago",
            short_commit(&previous.commit),
            age(report.now_ms, previous.ran_at_ms)
        )
    };
    match report.comparison {
        Comparison::Baseline => out.push_str(&format!(
            "- Last complete row: none. No complete cadence row has ever been recorded for {} on {}.\n",
            report.check, report.platform
        )),
        Comparison::SameExecutor { previous, .. } => out.push_str(&format!(
            "- Last complete row: {}, the previous row on `{}`, at {}.\n",
            previous.reference,
            host(previous),
            since(previous)
        )),
        Comparison::OtherExecutor { previous } => out.push_str(&format!(
            "- Last complete row: none on `{}`. The newest complete row on {} is {} on `{}`, at {}.\n",
            row.map_or("this executor", host),
            report.platform,
            previous.reference,
            host(previous),
            since(previous)
        )),
        Comparison::AnyExecutor { previous } => out.push_str(&format!(
            "- Last complete row: {} on `{}` (the newest on {}, any executor), at {}.\n",
            previous.reference,
            host(previous),
            report.platform,
            since(previous)
        )),
    }
    if report.comparison.previous().is_some() && report.commit.is_some() {
        render_bracket(out, "Merges not covered by a complete row", report.bracket);
    }
}

/// The message one cadence attempt posts to its lane's thread.
pub(crate) fn render_report(report: &CadenceReport<'_>) -> String {
    let mut out = String::new();
    match report.verdict {
        CadenceVerdict::Complete(row) => render_complete(&mut out, report, row),
        CadenceVerdict::NoVerdict { row, cause } => render_incomplete(
            &mut out,
            report,
            row.as_ref(),
            &truncate(cause, MAX_CAUSE_CHARS),
            false,
        ),
        CadenceVerdict::Death { cause } => render_incomplete(
            &mut out,
            report,
            None,
            &truncate(cause, MAX_CAUSE_CHARS),
            true,
        ),
    }
    out
}

/// The heartbeat's note for a lane whose current inputs a complete row already
/// answers but which has posted nothing for a whole heartbeat interval.
pub(crate) fn render_heartbeat(
    check: &str,
    platform: &str,
    tip: &str,
    covered: &CadenceRow,
    now_ms: i64,
) -> String {
    format!(
        "**Main cadence heartbeat · {check} · {platform}** — the cadence is alive. Main is at `{}`, and no merge since {} (commit `{}`, ran {} ago: {}) has changed this suite's inputs, so no new verdict is owed.\n",
        short_commit(tip),
        covered.reference,
        short_commit(&covered.commit),
        age(now_ms, covered.ran_at_ms),
        totals(covered)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, commit: &str, ran_at_ms: i64, tests: &[(&str, TestOutcome)]) -> CadenceRow {
        CadenceRow {
            id: id.to_string(),
            reference: format!("cairn://p/cairn/check-observations/{id}"),
            commit: commit.to_string(),
            platform: "linux".to_string(),
            executor_id: Some("bbs".to_string()),
            executor: Some("bbs".to_string()),
            ran_at_ms,
            verdict_passed: !tests
                .iter()
                .any(|(_, outcome)| matches!(outcome, TestOutcome::Failed(_))),
            exit_code: if tests
                .iter()
                .any(|(_, outcome)| matches!(outcome, TestOutcome::Failed(_)))
            {
                100
            } else {
                0
            },
            complete: true,
            tests: tests
                .iter()
                .map(|(test, outcome)| (test.to_string(), outcome.clone()))
                .collect(),
            output_tail: String::new(),
        }
    }

    fn failed(signature: &str) -> TestOutcome {
        TestOutcome::Failed(signature.to_string())
    }

    /// `row` as recorded on another executor.
    fn on(executor: &str, mut row: CadenceRow) -> CadenceRow {
        row.executor_id = Some(executor.to_string());
        row.executor = Some(executor.to_string());
        row
    }

    fn same(previous: &CadenceRow) -> Comparison {
        Comparison::SameExecutor {
            previous: previous.clone(),
            neighbour: None,
        }
    }

    const HOUR: i64 = 60 * 60 * 1000;

    fn report(verdict: &CadenceVerdict, comparison: &Comparison, bracket: &Bracket) -> String {
        render_report(&CadenceReport {
            check: "rust-tests",
            platform: "linux",
            commit: Some("bbbbbbbbbbbbbbbbbbbb"),
            verdict,
            comparison,
            bracket,
            now_ms: 10 * HOUR,
        })
    }

    #[test]
    fn requests_during_a_pass_coalesce_into_one_queued_successor() {
        let mut slots = CadenceSlots::default();
        assert!(slots.request("p", "main", Trigger::Startup, 1));
        assert_eq!(slots.in_flight("p"), Some(("main".to_string(), 1)));
        // Three merges and a heartbeat while the first pass runs: no second
        // pass starts, and exactly one successor is owed.
        assert!(!slots.request("p", "main", Trigger::Heartbeat, 2));
        assert!(!slots.request("p", "main", Trigger::Merge, 3));
        assert!(!slots.request("p", "main", Trigger::Merge, 4));
        assert!(!slots.request("p", "main", Trigger::Heartbeat, 5));
        assert_eq!(
            slots.finish("p", 6),
            Some(Trigger::Merge),
            "the strongest reason is kept"
        );
        assert_eq!(slots.in_flight("p"), Some(("main".to_string(), 6)));
        assert_eq!(
            slots.finish("p", 7),
            None,
            "one successor, never one per merge"
        );
        assert_eq!(slots.in_flight("p"), None);
        assert!(
            slots.request("p", "main", Trigger::Merge, 8),
            "a released slot starts afresh"
        );
        assert!(
            slots.request("q", "trunk", Trigger::Merge, 8),
            "projects are independent"
        );
    }

    #[test]
    fn lanes_come_only_from_checks_that_declare_a_main_cadence() {
        let checks: HashMap<String, CheckCommand> = serde_yaml::from_str(
            "rust-tests:\n  command: bun run test:rust {changedFiles}\n  fullCommand: bun run test:rust:full\n  mainCadence:\n    platforms: [macos, Linux, linux]\n    notify: testing\nlint:\n  command: bunx eslint src/\n",
        )
        .unwrap();
        let lanes = cadence_lanes(&checks);
        assert_eq!(
            lanes,
            vec![
                CadenceLane {
                    check: "rust-tests".into(),
                    platform: "linux".into(),
                    notify: "testing".into()
                },
                CadenceLane {
                    check: "rust-tests".into(),
                    platform: "macos".into(),
                    notify: "testing".into()
                },
            ]
        );
    }

    #[test]
    fn a_row_with_zero_tests_is_never_a_verdict() {
        let mut empty = row("o1", "c1", 0, &[]);
        empty.exit_code = 0;
        empty.verdict_passed = true;
        assert!(matches!(
            classify_row(empty),
            CadenceVerdict::NoVerdict { .. }
        ));

        let mut compile_failure = row("o2", "c1", 0, &[]);
        compile_failure.exit_code = 101;
        compile_failure.verdict_passed = false;
        assert!(matches!(
            classify_row(compile_failure),
            CadenceVerdict::NoVerdict { .. }
        ));

        let mut partial = row("o3", "c1", 0, &[("a", TestOutcome::Passed)]);
        partial.complete = false;
        assert!(matches!(
            classify_row(partial),
            CadenceVerdict::NoVerdict { .. }
        ));

        let mut runner_died = row("o4", "c1", 0, &[("a", TestOutcome::Passed)]);
        runner_died.verdict_passed = false;
        runner_died.exit_code = 101;
        assert!(matches!(
            classify_row(runner_died),
            CadenceVerdict::NoVerdict { .. }
        ));

        assert!(classify_row(row("o5", "c1", 0, &[("a", TestOutcome::Passed)])).is_complete());
        assert!(classify_row(row("o6", "c1", 0, &[("a", failed("boom"))])).is_complete());
    }

    #[test]
    fn the_diff_lists_newly_red_grouped_by_failure_and_newly_green() {
        let previous = row(
            "prev",
            "aaaaaaaaaaaaaaaaaaaa",
            2 * HOUR,
            &[
                ("core::old_red", failed("assertion failed: stale")),
                ("core::fixed", failed("assertion failed: fixed later")),
                ("core::removed", failed("assertion failed: gone")),
                ("core::race_a", TestOutcome::Passed),
                ("core::race_b", TestOutcome::Passed),
                ("core::race_c", TestOutcome::Passed),
                ("core::lone", TestOutcome::Passed),
            ],
        );
        let race = "called `Result::unwrap()` on an `Err` value: UNIQUE constraint failed";
        let current = row(
            "cur",
            "bbbbbbbbbbbbbbbbbbbb",
            5 * HOUR,
            &[
                ("core::old_red", failed("assertion failed: stale")),
                ("core::fixed", TestOutcome::Passed),
                ("core::race_a", failed(race)),
                ("core::race_b", failed(race)),
                ("core::race_c", failed(race)),
                ("core::lone", failed("assertion `left == right` failed")),
                ("core::skipped", TestOutcome::Skipped),
            ],
        );
        let verdict = CadenceVerdict::Complete(current);
        let text = report(&verdict, &same(&previous), &Bracket::default());

        assert!(text.contains("4 newly red, 1 newly green"), "{text}");
        assert!(
            text.contains(
                "Compared with: cairn://p/cairn/check-observations/prev, the previous row on `bbs`"
            ),
            "the comparison names its host: {text}"
        );
        assert!(
            !text.contains("HOST CENSUS")
                && !text.contains("one host")
                && !text.contains("cross-host"),
            "no other host ran in the window: {text}"
        );
        assert!(
            text.contains("cairn://p/cairn/check-observations/cur"),
            "{text}"
        );
        assert!(
            text.contains("cairn://p/cairn/check-observations/prev"),
            "{text}"
        );
        assert!(
            text.contains("6 ran · 1 passed · 5 failed · 1 skipped"),
            "{text}"
        );
        assert!(
            text.contains("platform `linux`") && text.contains("executor `bbs`"),
            "{text}"
        );
        assert!(
            text.contains("NEWLY RED: 4 tests in 2 failure groups"),
            "{text}"
        );
        // The race's three tests are one group, listed first, with its count.
        let race_group = text.find("**3 tests**").expect("grouped by excerpt");
        assert!(race_group < text.find("**1 test**").unwrap(), "{text}");
        assert!(
            text.contains("- `core::race_a`") && text.contains("- `core::lone`"),
            "{text}"
        );
        assert!(
            text.contains("NEWLY GREEN: 1") && text.contains("- `core::fixed`"),
            "{text}"
        );
        assert!(
            text.contains("1 previously red test reported no result"),
            "{text}"
        );
        assert!(
            !text.contains("`core::removed`"),
            "a vanished test is not green: {text}"
        );
        assert!(text.contains("**Still red:** 1 (not listed)"), "{text}");
        assert!(
            !text.contains("- `core::old_red`"),
            "still-red tests are counted, not listed"
        );
        assert!(text.contains("ran 3h"), "{text}");
    }

    #[test]
    fn the_first_row_is_a_baseline_with_every_failure_newly_red() {
        let current = row(
            "cur",
            "bbbbbbbbbbbb",
            HOUR,
            &[("a", failed("x")), ("b", TestOutcome::Passed)],
        );
        let verdict = CadenceVerdict::Complete(current);
        let text = report(&verdict, &Comparison::Baseline, &Bracket::default());
        assert!(text.contains("this row is the baseline"), "{text}");
        assert!(
            text.contains("NEWLY RED: 1 test in 1 failure group"),
            "{text}"
        );
        assert!(
            !text.contains("Merges since"),
            "no earlier row, no bracket: {text}"
        );
    }

    #[test]
    fn the_bracket_names_each_landing_with_its_pr_and_issue() {
        let previous = row(
            "prev",
            "aaaaaaaaaaaaaaaa",
            HOUR,
            &[("a", TestOutcome::Passed)],
        );
        let current = row(
            "cur",
            "bbbbbbbbbbbbbbbb",
            2 * HOUR,
            &[("a", TestOutcome::Passed)],
        );
        let bracket = Bracket {
            merges: vec![
                BracketMerge {
                    commit: "1111111111111111".into(),
                    subject: "Fix the index (cairn/5119) (#4115)".into(),
                    pr: Some(4115),
                    issue: Some("cairn/5119".into()),
                },
                BracketMerge {
                    commit: "2222222222222222".into(),
                    subject: "Direct push without a PR".into(),
                    pr: None,
                    issue: None,
                },
            ],
            truncated: false,
            unavailable: None,
        };
        let verdict = CadenceVerdict::Complete(current);
        let text = report(&verdict, &same(&previous), &bracket);
        assert!(text.contains("all 1 passed"), "{text}");
        assert!(
            text.contains("**Merges since the compared row (2)**"),
            "{text}"
        );
        assert!(
            text.contains(
                "- `111111111111` #4115 · cairn/5119 — Fix the index (cairn/5119) (#4115)"
            ),
            "{text}"
        );
        assert!(
            text.contains("- `222222222222` — Direct push without a PR"),
            "{text}"
        );
        // Newest landing first.
        assert!(text.find("222222222222").unwrap() < text.find("111111111111").unwrap());
    }

    #[test]
    fn a_death_is_the_loudest_message_and_names_its_class_and_last_row_age() {
        let previous = row(
            "prev",
            "aaaaaaaaaaaaaaaa",
            3 * HOUR,
            &[("a", TestOutcome::Passed)],
        );
        let verdict = CadenceVerdict::Death {
            cause: "infrastructure: the command stopped making progress".into(),
        };
        let comparison = Comparison::AnyExecutor { previous };
        let text = report(&verdict, &comparison, &Bracket::default());
        assert!(
            text.starts_with("🚨 **MAIN CADENCE DID NOT COMPLETE"),
            "{text}"
        );
        assert!(text.contains("Cause class: **death**"), "{text}");
        assert!(text.contains("stopped making progress"), "{text}");
        assert!(
            text.contains("Last complete row: cairn://p/cairn/check-observations/prev"),
            "{text}"
        );
        assert!(
            text.contains("ran 7h"),
            "age of the last complete row: {text}"
        );
        assert!(
            text.contains("Merges not covered by a complete row"),
            "{text}"
        );
    }

    #[test]
    fn a_zero_row_observation_renders_as_no_verdict_never_green() {
        let mut empty = row("cur", "bbbbbbbbbbbb", HOUR, &[]);
        empty.exit_code = 101;
        empty.verdict_passed = false;
        empty.output_tail = "error[E0599]: no method named `turn_id`".into();
        let verdict = classify_row(empty);
        let text = report(&verdict, &Comparison::Baseline, &Bracket::default());
        assert!(
            text.starts_with("🚨 **MAIN CADENCE DID NOT COMPLETE"),
            "{text}"
        );
        assert!(text.contains("Cause class: **no-verdict**"), "{text}");
        assert!(text.contains("It is not green"), "{text}");
        assert!(
            text.contains("cairn://p/cairn/check-observations/cur"),
            "{text}"
        );
        assert!(
            text.contains("error[E0599]"),
            "the compile error is quoted: {text}"
        );
        assert!(
            text.contains("No complete cadence row has ever been recorded"),
            "{text}"
        );
        assert!(
            !text.contains("passed**") && !text.contains("all 0 passed"),
            "{text}"
        );

        let mut green_but_empty = row("cur2", "bbbbbbbbbbbb", HOUR, &[]);
        green_but_empty.verdict_passed = true;
        green_but_empty.exit_code = 0;
        let text = report(
            &classify_row(green_but_empty),
            &Comparison::Baseline,
            &Bracket::default(),
        );
        assert!(text.contains("Cause class: **no-verdict**"), "{text}");
    }

    #[test]
    fn a_cross_host_fallback_names_both_hosts_and_claims_no_movement() {
        let on_bbs = row(
            "prev",
            "aaaaaaaaaaaaaaaa",
            2 * HOUR,
            &[
                ("core::lock_wait", failed("Blocking waiting for file lock")),
                ("core::load", failed("timed out")),
                ("core::both", failed("assertion failed")),
                ("core::here_only", TestOutcome::Passed),
            ],
        );
        let on_fedora = on(
            "fedora",
            row(
                "cur",
                "bbbbbbbbbbbbbbbb",
                3 * HOUR,
                &[
                    ("core::lock_wait", TestOutcome::Passed),
                    ("core::load", TestOutcome::Passed),
                    ("core::both", failed("assertion failed")),
                    ("core::here_only", failed("left == right")),
                ],
            ),
        );
        let comparison = Comparison::OtherExecutor { previous: on_bbs };
        let text = report(
            &CadenceVerdict::Complete(on_fedora),
            &comparison,
            &Bracket::default(),
        );
        assert!(text.contains("linux on `fedora`"), "{text}");
        assert!(
            text.contains("cross-host against `bbs`: 1 red here only, 2 green here only"),
            "{text}"
        );
        assert!(
            text.contains("check-observations/prev on `bbs`")
                && text.contains("No earlier complete row exists on `fedora`"),
            "the basis names both hosts: {text}"
        );
        assert!(
            text.contains("RED HERE, NOT RED ON `bbs`: 1 test")
                && text.contains("GREEN HERE, RED ON `bbs`: 2")
                && text.contains("**Red on both hosts:** 1"),
            "{text}"
        );
        assert!(
            !text.contains("NEWLY")
                && !text.contains("newly red,")
                && !text.contains("newly green**"),
            "a cross-host difference is never reported as movement: {text}"
        );
        assert!(text.contains("Merges since the compared row"), "{text}");
    }

    #[test]
    fn another_hosts_row_in_the_window_splits_the_host_census_from_movement() {
        let previous = on(
            "fedora",
            row(
                "prev",
                "aaaaaaaaaaaaaaaa",
                HOUR,
                &[
                    ("core::lock_a", TestOutcome::Passed),
                    ("core::lock_b", TestOutcome::Passed),
                    ("core::fedora_only", failed("refresh computed twice")),
                    ("core::moved", TestOutcome::Passed),
                    ("core::moved_everywhere", TestOutcome::Passed),
                    ("core::fixed", failed("stale")),
                ],
            ),
        );
        let neighbour = row(
            "other",
            "cccccccccccccccc",
            2 * HOUR,
            &[
                ("core::lock_a", failed("Blocking waiting for file lock")),
                ("core::lock_b", failed("Blocking waiting for file lock")),
                ("core::fedora_only", TestOutcome::Passed),
                ("core::moved", TestOutcome::Passed),
                ("core::moved_everywhere", failed("index out of bounds")),
                ("core::fixed", failed("stale")),
            ],
        );
        let current = on(
            "fedora",
            row(
                "cur",
                "bbbbbbbbbbbbbbbb",
                3 * HOUR,
                &[
                    ("core::lock_a", TestOutcome::Passed),
                    ("core::lock_b", TestOutcome::Passed),
                    ("core::fedora_only", failed("refresh computed twice")),
                    ("core::moved", failed("left == right")),
                    ("core::moved_everywhere", failed("index out of bounds")),
                    ("core::fixed", TestOutcome::Passed),
                ],
            ),
        );
        let comparison = Comparison::SameExecutor {
            previous,
            neighbour: Some(Box::new(neighbour)),
        };
        let text = report(
            &CadenceVerdict::Complete(current),
            &comparison,
            &Bracket::default(),
        );
        // Movement is measured on fedora's own history only.
        assert!(text.contains("2 newly red, 1 newly green"), "{text}");
        assert!(text.contains("the previous row on `fedora`"), "{text}");
        assert!(
            text.contains("**1 test** · **one host**: passed on `bbs` — `left == right`"),
            "a red the other host's newer row passed is flagged: {text}"
        );
        assert!(
            text.contains("**1 test** — `index out of bounds`"),
            "a red on both hosts is not one-host: {text}"
        );
        assert!(
            text.contains("**HOST CENSUS against `bbs`: 3**")
                && text.contains("check-observations/other"),
            "{text}"
        );
        assert!(
            text.contains("- Red on `fedora`, passed on `bbs`: 1\n  - `core::fedora_only`"),
            "{text}"
        );
        assert!(
            text.contains(
                "- Passed on `fedora`, red on `bbs`: 2\n  - `core::lock_a`\n  - `core::lock_b`"
            ),
            "{text}"
        );
        let green = text.find("NEWLY GREEN").unwrap();
        let census = text.find("HOST CENSUS").unwrap();
        assert!(
            !text[green..census].contains("lock_"),
            "the host census is not reported as newly green: {text}"
        );
        assert!(text[green..census].contains("`core::fixed`"), "{text}");
    }

    #[test]
    fn a_no_verdict_on_a_host_without_history_names_the_other_hosts_row() {
        let previous = row(
            "prev",
            "aaaaaaaaaaaaaaaa",
            HOUR,
            &[("a", TestOutcome::Passed)],
        );
        let mut partial = on(
            "fedora",
            row(
                "cur",
                "bbbbbbbbbbbb",
                2 * HOUR,
                &[("a", TestOutcome::Passed)],
            ),
        );
        partial.complete = false;
        let comparison = Comparison::OtherExecutor { previous };
        let text = report(&classify_row(partial), &comparison, &Bracket::default());
        assert!(
            text.contains("Last complete row: none on `fedora`. The newest complete row on linux is cairn://p/cairn/check-observations/prev on `bbs`"),
            "{text}"
        );
        assert!(
            !text.contains("has ever been recorded"),
            "a complete row exists on the platform: {text}"
        );
    }

    async fn insert_observation(
        db: &LocalDb,
        id: &str,
        executor: &str,
        ran_at: i64,
        cadence: &str,
        tests: &[(&str, &str)],
    ) {
        // A failing exit with no failing test is not a verdict, so a row's
        // verdict follows its tests; a row with no tests is a compile failure.
        let (exit_code, verdict) = if tests.is_empty() {
            (101, "failed")
        } else if tests.iter().any(|(_, status)| *status == "failed") {
            (100, "failed")
        } else {
            (0, "passed")
        };
        let mut script = format!(
            "INSERT INTO check_result_observations
               (id, project_id, commit_sha, tree_hash, check_name, input_hash,
                environment_fingerprint, exit_code, verdict, complete, reusable,
                parser_version, result_schema_version, ran_at, duration_ms, job_id,
                cadence, output_tail, executor_id, verdict_platform, public_handle)
             VALUES('{id}','p','c-{id}','t','rust-tests','i-{id}','env',{exit_code},'{verdict}',1,0,
                    1,1,{ran_at},5,'main:p','{cadence}','','{executor}','linux','h-{id}');"
        );
        for (test, status) in tests {
            script.push_str(&format!(
                "INSERT INTO check_test_results (observation_id, test_id, status, failure_excerpt)
                 VALUES('{id}','{test}','{status}','boom');"
            ));
        }
        db.execute_script(script).await.unwrap();
    }

    #[tokio::test]
    async fn the_previous_row_is_the_same_executors_with_a_platform_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.path().join("cadence.db"))
            .await
            .unwrap();
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','w','Project','cairn','/tmp/repo',1,1);",
        )
        .await
        .unwrap();
        let lane = CadenceLane {
            check: "rust-tests".into(),
            platform: "linux".into(),
            notify: "testing".into(),
        };
        let names = |executor: &str| format!("{executor}-host");
        let comparison = |current: Option<CadenceRow>| {
            let db = &db;
            let lane = &lane;
            async move {
                comparison_for(db, "p", "cairn", lane, current.as_ref(), &names)
                    .await
                    .unwrap()
            }
        };
        let load = |id: &'static str| {
            let db = &db;
            async move { load_row(db, "cairn", id, &names).await.unwrap() }
        };
        let id_of = |row: Option<&CadenceRow>| row.map(|row| row.id.clone());

        // Interleaved hosts, plus rows the lookup must never pick: a complete
        // flag with no tests (a compile failure), a manual run, another platform.
        insert_observation(&db, "b1", "bbs", 1, "main", &[("a", "failed")]).await;
        insert_observation(&db, "f1", "fedora", 2, "main", &[("a", "passed")]).await;
        insert_observation(&db, "b2", "bbs", 3, "main", &[("a", "failed")]).await;
        insert_observation(&db, "fz", "fedora", 4, "main", &[]).await;
        insert_observation(&db, "fm", "fedora", 5, "manual", &[("a", "passed")]).await;

        // bbs's newest is diffed against bbs's previous; fedora's f1 ran in
        // between, so it is the window's neighbour.
        let b2 = load("b2").await;
        assert_eq!(b2.executor.as_deref(), Some("bbs-host"));
        match comparison(Some(b2)).await {
            Comparison::SameExecutor {
                previous,
                neighbour,
            } => {
                assert_eq!(previous.id, "b1");
                assert_eq!(id_of(neighbour.as_deref()).as_deref(), Some("f1"));
            }
            other => panic!("expected a same-executor comparison, got {other:?}"),
        }

        // fedora's next row skips the newer bbs row and the unusable fedora
        // rows, and keeps b2 as the neighbour.
        insert_observation(&db, "f2", "fedora", 6, "main", &[("a", "passed")]).await;
        match comparison(Some(load("f2").await)).await {
            Comparison::SameExecutor {
                previous,
                neighbour,
            } => {
                assert_eq!(previous.id, "f1");
                assert_eq!(id_of(neighbour.as_deref()).as_deref(), Some("b2"));
            }
            other => panic!("expected a same-executor comparison, got {other:?}"),
        }

        // Consecutive rows on one host: no neighbour, the pre-existing shape.
        insert_observation(&db, "f3", "fedora", 7, "main", &[("a", "passed")]).await;
        match comparison(Some(load("f3").await)).await {
            Comparison::SameExecutor {
                previous,
                neighbour,
            } => {
                assert_eq!(previous.id, "f2");
                assert!(neighbour.is_none(), "{neighbour:?}");
            }
            other => panic!("expected a same-executor comparison, got {other:?}"),
        }

        // A host with no history falls back to the platform's newest row.
        insert_observation(&db, "n1", "nuc", 8, "main", &[("a", "passed")]).await;
        match comparison(Some(load("n1").await)).await {
            Comparison::OtherExecutor { previous } => assert_eq!(previous.id, "f3"),
            other => panic!("expected a cross-host fallback, got {other:?}"),
        }

        // A death names no executor: the platform's newest on any executor.
        match comparison(None).await {
            Comparison::AnyExecutor { previous } => assert_eq!(previous.id, "n1"),
            other => panic!("expected the platform's newest, got {other:?}"),
        }
    }

    #[test]
    fn the_bracket_walks_the_first_parent_chain_only() {
        let log = "t\tm2\tLanding three (#3)\nm2\tm1,side\tLanding two (#2)\nside\tbase\tA branch commit\nm1\tbase\tLanding one (#1)\n";
        let entries = parse_log(log);
        let (chain, reached) = first_parent_chain(&entries, "base", "t");
        assert!(reached);
        assert_eq!(
            chain
                .iter()
                .map(|entry| entry.commit.as_str())
                .collect::<Vec<_>>(),
            vec!["m1", "m2", "t"]
        );
        let (chain, reached) = first_parent_chain(&entries, "elsewhere", "t");
        assert!(
            !reached,
            "a walk that never meets the base reports truncation"
        );
        assert_eq!(chain.len(), 3);
    }

    #[test]
    fn subjects_yield_their_pr_and_issue_numbers() {
        let subject = "[Build] base_advance: index the probe (cairn/5119) (#4115)";
        assert_eq!(subject_pr(subject), Some(4115));
        assert_eq!(
            subject_issue(subject, "cairn"),
            Some("cairn/5119".to_string())
        );
        assert_eq!(subject_pr("No number here"), None);
        assert_eq!(subject_issue("notcairn/12 only", "cairn"), None);
        assert_eq!(
            subject_issue("CAIRN/12 shouted", "cairn"),
            Some("cairn/12".to_string())
        );
    }

    #[test]
    fn a_panic_excerpt_signs_with_its_message_not_its_thread() {
        let excerpt = "thread 'mcp::tests::a' (1387401) panicked at os/x.rs:700:10:\ncalled `Result::unwrap()` on an `Err` value: UNIQUE\nnote: run with `RUST_BACKTRACE=1`";
        let other = "thread 'mcp::tests::b' (1387434) panicked at os/x.rs:700:10:\ncalled `Result::unwrap()` on an `Err` value: UNIQUE\n";
        assert_eq!(failure_signature(excerpt), failure_signature(other));
        assert_eq!(
            failure_signature(excerpt),
            "called `Result::unwrap()` on an `Err` value: UNIQUE"
        );
        assert_eq!(failure_signature(""), "(no failure excerpt)");
        assert_eq!(failure_signature("plain failure\nmore"), "plain failure");
    }

    #[test]
    fn the_heartbeat_note_cites_the_covering_row() {
        let covered = row(
            "prev",
            "aaaaaaaaaaaaaaaa",
            HOUR,
            &[("a", TestOutcome::Passed)],
        );
        let text = render_heartbeat(
            "rust-tests",
            "linux",
            "cccccccccccccccc",
            &covered,
            8 * HOUR,
        );
        assert!(text.contains("cadence is alive"), "{text}");
        assert!(
            text.contains("cairn://p/cairn/check-observations/prev"),
            "{text}"
        );
        assert!(text.contains("ran 7h"), "{text}");
    }

    #[test]
    fn cadence_stream_ids_record_the_main_cadence() {
        assert_eq!(cadence_owner("p1"), "main:p1");
        assert!(
            format!("{MAIN_CADENCE_STREAM_PREFIX}p1:rust-tests:linux").starts_with("main-cadence:")
        );
    }
}
