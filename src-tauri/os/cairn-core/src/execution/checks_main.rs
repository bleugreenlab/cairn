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
//! attests whatever the tip is when that pass starts. The runner arms every
//! project at boot ([`arm_at_boot`]), and a heartbeat re-arms every project on
//! a fixed interval, so a tip that never got a complete row is retried and a
//! lane that has gone quiet says so.
//!
//! A pass the runner's own restart cuts short is neither a death nor a failure:
//! it is announced as **cancelled by restart**, recorded, and cited by the pass
//! the next boot arms, so a restart never leaves main with no pass at all.
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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cairn_common::executor_protocol::{CheckSelectorSource, ExecutorSelector};

use crate::config::project_settings::CheckCommand;
use crate::execution::check_placement::CheckPlacementOverride;
use crate::execution::checks::{
    execute_check_contract, load_checks_contract_at_commit_detailed,
    resolve_check_contract_at_commit, CheckRunProducer, CheckRunTarget, CommitChecksContract,
    CommitChecksContractError, MAIN_CADENCE_STREAM_PREFIX,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// A burst of merges settles into one pass.
const SETTLE_WINDOW: Duration = Duration::from_secs(10);
/// How long a boot pass waits before its first lane: the runner's boot grace.
/// A restart closes every executor link, and placement refuses a lane outright
/// when no machine for its platform is attached and none is known to be
/// restoring, so a pass that started sooner would announce a death for a
/// machine that was only reconnecting.
const BOOT_SETTLE_WINDOW: Duration = Duration::from_secs(60);
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
    /// A lane refused because its question owed a displaced answer's death,
    /// and that debt has now settled. Without this a settled debt would leave
    /// main unobserved until the next merge or heartbeat.
    DebtSettled,
    /// An observed advance of the default branch.
    Merge,
}

struct CadencePass {
    branch: String,
    armed_at_unix_ms: i64,
    queued: Option<Trigger>,
}

/// A lane pass the runner's own restart cancelled: recorded as the runner
/// stops, read back at boot, and cited by the pass that re-arms the lane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Interruption {
    pub(crate) check: String,
    pub(crate) platform: String,
    /// The tip the cancelled pass was attesting.
    pub(crate) commit: String,
    pub(crate) stopped_at_unix_ms: i64,
}

impl Interruption {
    /// The lane and what happened to it, as a clause.
    pub(crate) fn describe(&self) -> String {
        format!(
            "{} on {}, whose pass on `{}` the runner restart at {} cancelled",
            self.check,
            self.platform,
            short_commit(&self.commit),
            stamp(self.stopped_at_unix_ms)
        )
    }
}

/// The cadence pass armed for a project right now.
pub(crate) struct InFlight {
    pub(crate) branch: String,
    pub(crate) armed_at_unix_ms: i64,
    /// The lanes whose passes the last restart cancelled and which this pass
    /// re-arms, until each lane reports.
    pub(crate) rearming: Vec<Interruption>,
}

/// The per-project cadence slot: one pass in flight, at most one queued.
#[derive(Default)]
pub(crate) struct CadenceSlots {
    passes: HashMap<String, CadencePass>,
    /// Questions a refused lane is waiting on to settle, so a lane refused by
    /// every pass against one debt gets one waiter, not one per pass.
    debts: HashSet<crate::fleet::purpose::PurposeKey>,
    /// Per project, the tip whose checks contract could not be read: one
    /// outcome per tip, announced once and not read again until the tip moves.
    unreadable_contracts: HashMap<String, String>,
    /// Per project, the lanes the last restart cancelled that no pass has
    /// reported on since; see [`arm_at_boot`].
    interrupted: HashMap<String, Vec<Interruption>>,
    /// Whether the startup default-advance catch-up has observed origin. Until
    /// it has, a pass must not fetch; see [`run_pass`].
    origin_observed: bool,
}

impl CadenceSlots {
    /// Claim the one waiter for `key`'s settlement. `false` when one is
    /// already waiting.
    fn watch_debt(&mut self, key: &crate::fleet::purpose::PurposeKey) -> bool {
        self.debts.insert(key.clone())
    }

    fn debt_settled(&mut self, key: &crate::fleet::purpose::PurposeKey) {
        self.debts.remove(key);
    }

    /// Whether `commit`'s contract is still to be read: `false` once that
    /// exact tip's was found unreadable.
    fn contract_owed(&self, project_id: &str, commit: &str) -> bool {
        self.unreadable_contracts
            .get(project_id)
            .is_none_or(|tip| tip != commit)
    }

    fn contract_read(&mut self, project_id: &str, commit: &str, readable: bool) {
        if readable {
            self.unreadable_contracts.remove(project_id);
        } else {
            self.unreadable_contracts
                .insert(project_id.to_string(), commit.to_string());
        }
    }

    /// Hold `interruption` for the next pass to cite. A lane holds one: a later
    /// cancel replaces an earlier one nothing cited.
    fn interrupted(&mut self, project_id: &str, interruption: Interruption) {
        let lanes = self.interrupted.entry(project_id.to_string()).or_default();
        lanes.retain(|held| {
            held.check != interruption.check || held.platform != interruption.platform
        });
        lanes.push(interruption);
    }

    /// Take the restart citation `lane` holds, once.
    fn take_interruption(&mut self, project_id: &str, lane: &CadenceLane) -> Option<Interruption> {
        let lanes = self.interrupted.get_mut(project_id)?;
        let index = lanes
            .iter()
            .position(|held| held.check == lane.check && held.platform == lane.platform)?;
        let taken = lanes.remove(index);
        if lanes.is_empty() {
            self.interrupted.remove(project_id);
        }
        Some(taken)
    }

    /// Ask for a pass. Returns `true` when the caller must start one; otherwise
    /// the request was folded into the single queued successor.
    fn request(&mut self, project_id: &str, branch: &str, trigger: Trigger, now: i64) -> bool {
        match self.passes.get_mut(project_id) {
            Some(pass) => {
                pass.queued = Some(pass.queued.map_or(trigger, |queued| queued.max(trigger)));
                false
            }
            None => {
                self.passes.insert(
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
        let pass = self.passes.get_mut(project_id)?;
        match pass.queued.take() {
            Some(next) => {
                pass.armed_at_unix_ms = now;
                Some(next)
            }
            None => {
                self.passes.remove(project_id);
                // Every lane the pass ran has reported or was covered, so a
                // citation still held names a lane the tip no longer declares.
                self.interrupted.remove(project_id);
                None
            }
        }
    }

    fn in_flight(&self, project_id: &str) -> Option<InFlight> {
        self.passes.get(project_id).map(|pass| InFlight {
            branch: pass.branch.clone(),
            armed_at_unix_ms: pass.armed_at_unix_ms,
            rearming: self
                .interrupted
                .get(project_id)
                .cloned()
                .unwrap_or_default(),
        })
    }
}

/// The cadence pass armed for `project_id` right now, if any: the branch it
/// attests, when it was armed, and the restart-cancelled passes it re-arms.
/// While it runs the store holds no row for the new tip, so this is what tells
/// "pending" from "never ran" (CAIRN-3823).
pub(crate) fn in_flight(orch: &Orchestrator, project_id: &str) -> Option<InFlight> {
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
            let settle = if trigger == Trigger::Startup {
                BOOT_SETTLE_WINDOW
            } else {
                SETTLE_WINDOW
            };
            tokio::time::sleep(settle).await;
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

/// The boot repair edge: arm every project at once and start the heartbeat.
///
/// Runs as the runner comes up, not behind its gated maintenance. Behind it,
/// the arm waited on the startup default-advance catch-up, which rebases every
/// in-flight branch and took 11.6 minutes after the 10-28 restart: for that
/// whole window main had no pass in flight or queued, and the pass the restart
/// cancelled had no successor (cairn/5441). Arming is idempotent: requests
/// during a pass fold into its one queued successor, and a lane whose inputs a
/// complete row already covers is not re-run. A hand baseline already running
/// on the same inputs is joined by the fleet's coalescing, not run twice.
pub(crate) async fn arm_at_boot(orch: &Orchestrator) {
    match take_interruptions(&orch.db.local).await {
        Ok(interrupted) => {
            let mut slots = orch.main_cadence.lock().unwrap();
            for (project_id, interruption) in interrupted {
                log::info!(
                    "main cadence for project {project_id}: re-arming {}",
                    interruption.describe()
                );
                slots.interrupted(&project_id, interruption);
            }
        }
        Err(error) => log::warn!(
            "main cadence: could not read back the passes the last restart cancelled, so the boot pass will not cite them: {error}"
        ),
    }
    spawn_all_current(orch, Trigger::Startup).await;
    spawn_heartbeat(orch);
}

/// The startup default-advance catch-up has observed origin and reconciled any
/// advance that landed while the runner was down. From here a pass may fetch,
/// and every project is armed again for the tip the catch-up saw: a pass
/// already armed takes it as its queued successor, which finds its lanes
/// covered unless the tip moved.
pub(crate) async fn origin_observed(orch: &Orchestrator) {
    orch.main_cadence.lock().unwrap().origin_observed = true;
    spawn_all_current(orch, Trigger::Startup).await;
}

/// Re-arm every project every [`HEARTBEAT`]. The first tick waits a full
/// interval: boot already armed every project.
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
    // Until the startup catch-up has observed origin, a fetch here would move
    // `<branch>@origin` under it. The catch-up reads that ref before and after
    // its own fetch to find an advance that landed while the runner was down,
    // and would read the moved ref as no advance, leaving every in-flight
    // branch on the old base. So an early pass attests the store's current
    // head, and the catch-up re-arms the cadence once it has looked.
    let may_fetch = orch.main_cadence.lock().unwrap().origin_observed;
    // A stale store attests a stale tip, which its row names honestly; a pass
    // that gave up here would attest nothing at all.
    if !may_fetch {
        log::info!(
            "main cadence for project {project_id}: the startup catch-up has not observed origin yet; attesting the store's current {default_branch} without fetching"
        );
    } else if let Err(error) =
        refresh_default_branch(orch, project_id, &repo_root, default_branch).await
    {
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
    let lanes = tip_lanes(&orch.main_cadence, project_id, &commit, || {
        load_checks_contract_at_commit_detailed(&repo_root, &commit)
    })
    .await?;
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

/// The lanes `commit`'s checks contract declares, read through `load`.
///
/// A commit with no `.cairn/config.yaml`, or one that declares no checks, has
/// no lanes: that is an answer, and it is silent. A contract that cannot be
/// read is one outcome per tip: the first pass on the tip fails with the
/// diagnostic, which is announced, and every later pass on that tip skips the
/// read until the tip moves, rather than failing again on every observed
/// advance.
async fn tip_lanes<F, Fut>(
    slots: &std::sync::Mutex<CadenceSlots>,
    project_id: &str,
    commit: &str,
    load: F,
) -> Result<Vec<CadenceLane>, String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<CommitChecksContract, CommitChecksContractError>>,
{
    let owed = slots.lock().unwrap().contract_owed(project_id, commit);
    if !owed {
        log::debug!(
            "main cadence for project {project_id}: the checks contract at {commit} was already found unreadable; waiting for the tip to move"
        );
        return Ok(Vec::new());
    }
    let loaded = load().await;
    let readable = matches!(
        loaded,
        Ok(_) | Err(CommitChecksContractError::Missing | CommitChecksContractError::NoChecks)
    );
    slots
        .lock()
        .unwrap()
        .contract_read(project_id, commit, readable);
    match loaded {
        Ok(contract) => Ok(cadence_lanes(&contract.contract.checks)),
        Err(CommitChecksContractError::Missing | CommitChecksContractError::NoChecks) => {
            Ok(Vec::new())
        }
        Err(error) => Err(format!(
            "{}; the cadence will not read it again until the tip moves",
            error.manual_diagnostic(commit)
        )),
    }
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
            let rearmed = orch
                .main_cadence
                .lock()
                .unwrap()
                .take_interruption(context.project_id, lane);
            if let Some(interruption) = rearmed {
                log::info!(
                    "main cadence: {} is answered by {}, which already covers `{}`",
                    interruption.describe(),
                    row.reference,
                    short_commit(context.commit)
                );
            }
            if context.trigger == Trigger::Heartbeat {
                announce_quiet_lane(orch, &db, context, lane, &row).await;
            }
        }
        LaneAttempt::Verdict(verdict) => {
            let verdict = cancelled_by_restart(verdict, orch.fleet.runner_stopping_since());
            if matches!(verdict, CadenceVerdict::Death { .. }) {
                rearm_on_debt_settlement(orch, context, lane);
            }
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

/// If this lane died because its question still owes a displaced answer's
/// death, arm the project again the moment that debt settles.
///
/// The wait has no bound of its own: it lasts as long as the evidence takes,
/// and settling is the only thing that ends it. One waiter per question, so a
/// lane refused by many passes against one debt is re-armed once.
fn rearm_on_debt_settlement(orch: &Orchestrator, context: &PassContext<'_>, lane: &CadenceLane) {
    let key = crate::fleet::purpose::PurposeKey::main_cadence(
        context.project_id,
        &lane.check,
        &lane.platform,
    );
    if !orch.fleet.purpose_owes(&key) || !orch.main_cadence.lock().unwrap().watch_debt(&key) {
        return;
    }
    let orch = orch.clone();
    let project_id = context.project_id.to_string();
    let repo_path = context.repo_path.to_string();
    let default_branch = context.default_branch.to_string();
    tokio::spawn(async move {
        orch.fleet.purpose_debt_cleared(&key).await;
        orch.main_cadence.lock().unwrap().debt_settled(&key);
        log::info!(
            "purpose {key} owes nothing now; re-arming the main cadence for project {project_id}"
        );
        spawn(
            &orch,
            project_id,
            repo_path,
            default_branch,
            Trigger::DebtSettled,
        );
    });
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
    // Without the chain (a pass that failed before resolving the tip, or an
    // unreadable log), only the cadence's own rows can be placed on main.
    let chain = match (repository, commit) {
        (Some(repository), Some(commit)) => main_chain(orch, repository, commit)
            .map_err(|error| {
                log::warn!(
                    "main cadence first-parent walk failed; comparing cadence rows only: {error}"
                )
            })
            .ok(),
        _ => None,
    };
    let comparison = comparison_for(
        db,
        project_id,
        project_key,
        lane,
        verdict.row(),
        chain.as_ref(),
        &names,
    )
    .await
    .unwrap_or_else(|error| {
        log::warn!("main cadence previous-row lookup failed: {error}");
        Comparison::Baseline
    });
    let bracket = match (bracket_base(&verdict, &comparison), repository, commit) {
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
    let rearmed = orch
        .main_cadence
        .lock()
        .unwrap()
        .take_interruption(project_id, lane);
    if let (
        CadenceVerdict::Cancelled {
            stopped_at_unix_ms, ..
        },
        Some(commit),
    ) = (&verdict, commit)
    {
        let interruption = Interruption {
            check: lane.check.clone(),
            platform: lane.platform.clone(),
            commit: commit.to_string(),
            stopped_at_unix_ms: *stopped_at_unix_ms,
        };
        if let Err(error) = record_interruption(&orch.db.local, project_id, &interruption).await {
            log::warn!(
                "main cadence {} on {}: could not record that the restart cancelled this lane, so the boot pass will not cite it: {error}",
                lane.check,
                lane.platform
            );
        }
    }
    let content = render_report(&CadenceReport {
        check: &lane.check,
        platform: &lane.platform,
        commit,
        verdict: &verdict,
        comparison: &comparison,
        bracket: &bracket,
        now_ms: now_ms(),
        rearmed: rearmed.as_ref(),
    });
    match &verdict {
        CadenceVerdict::Complete(_) => log::info!(
            "main cadence {} on {}: row recorded",
            lane.check,
            lane.platform
        ),
        CadenceVerdict::Cancelled { .. } => log::info!("{content}"),
        CadenceVerdict::NoVerdict { .. } | CadenceVerdict::Death { .. } => {
            log::error!("{content}")
        }
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
            cancelled_by_restart(
                CadenceVerdict::Death {
                    cause: format!("the cadence pass failed before this lane could run: {cause}"),
                },
                orch.fleet.runner_stopping_since(),
            ),
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
    Failed(Failure),
}

/// What a failed test's excerpt says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Failure {
    /// See [`failure_signature`].
    pub(crate) signature: String,
    /// The machine condition the excerpt names, if any: see [`host_condition`].
    pub(crate) host_condition: Option<&'static str>,
}

impl Failure {
    fn from_excerpt(excerpt: &str) -> Self {
        Self {
            signature: failure_signature(excerpt),
            host_condition: host_condition(excerpt),
        }
    }
}

/// One whole-suite observation, read back from the store: the cadence's own
/// attempt, or a row of any cadence it is compared with or covered by.
#[derive(Clone, Debug)]
pub(crate) struct CadenceRow {
    pub(crate) id: String,
    /// Canonical citation: `cairn://p/{key}/check-observations/{handle}`.
    pub(crate) reference: String,
    pub(crate) commit: String,
    /// How the row was armed: `main` for the cadence's own, otherwise the
    /// producer's cadence (`manual`, `review`, `write`).
    pub(crate) cadence: String,
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
    /// The runner began stopping while the lane ran, and the lane ended
    /// without a result: see [`cancelled_by_restart`].
    Cancelled {
        cause: String,
        stopped_at_unix_ms: i64,
    },
}

impl CadenceVerdict {
    fn row(&self) -> Option<&CadenceRow> {
        match self {
            CadenceVerdict::Complete(row) => Some(row),
            CadenceVerdict::NoVerdict { row, .. } => row.as_ref(),
            CadenceVerdict::Death { .. } | CadenceVerdict::Cancelled { .. } => None,
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

/// A lane that ended without a result while this runner was stopping was
/// ended by the restart, whatever shape the ending took on the way out: the
/// fleet answers a claim the stop interrupted as cancelled, and a pass cut
/// short fails before its lanes. Neither judged the tree or a machine, and
/// announcing one as a death sent readers looking for a machine fault that was
/// a deploy. A result with a row is the tree's answer, stop or no stop.
fn cancelled_by_restart(verdict: CadenceVerdict, stopping_since: Option<u64>) -> CadenceVerdict {
    let Some(stopped_at) = stopping_since else {
        return verdict;
    };
    match verdict {
        CadenceVerdict::Death { cause } | CadenceVerdict::NoVerdict { row: None, cause } => {
            CadenceVerdict::Cancelled {
                cause,
                stopped_at_unix_ms: i64::try_from(stopped_at).unwrap_or(i64::MAX),
            }
        }
        other => other,
    }
}

/// Record a lane pass the restart cancelled, for the next boot to cite.
async fn record_interruption(
    db: &LocalDb,
    project_id: &str,
    interruption: &Interruption,
) -> Result<(), String> {
    db.execute(
        "INSERT OR REPLACE INTO main_cadence_interruptions
           (project_id, check_name, platform, commit_sha, stopped_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        (
            project_id,
            interruption.check.as_str(),
            interruption.platform.as_str(),
            interruption.commit.as_str(),
            interruption.stopped_at_unix_ms,
        ),
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

/// Every recorded restart cancel, removed as it is read: one boot cites each.
async fn take_interruptions(db: &LocalDb) -> Result<Vec<(String, Interruption)>, String> {
    let interrupted = db
        .query_all(
            "SELECT project_id, check_name, platform, commit_sha, stopped_at
               FROM main_cadence_interruptions",
            (),
            |row| {
                Ok((
                    row.text(0)?,
                    Interruption {
                        check: row.text(1)?,
                        platform: row.text(2)?,
                        commit: row.text(3)?,
                        stopped_at_unix_ms: row.i64(4)?,
                    },
                ))
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    if !interrupted.is_empty() {
        db.execute("DELETE FROM main_cadence_interruptions", ())
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(interrupted)
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
                    executor_id, ran_at, verdict, exit_code, complete, output_tail, cadence
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
                    row.text(10)?,
                ))
            },
        )
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("observation {observation_id} does not exist"))?;
    let (
        id,
        handle,
        commit,
        platform,
        executor_id,
        ran_at,
        verdict,
        exit_code,
        complete,
        tail,
        cadence,
    ) = head;
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
                _ => TestOutcome::Failed(Failure::from_excerpt(excerpt.as_deref().unwrap_or(""))),
            };
            (test, outcome)
        })
        .collect();
    Ok(CadenceRow {
        reference: format!("cairn://p/{project_key}/check-observations/{handle}"),
        id,
        commit,
        cadence,
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

/// The default head's first-parent chain, tip first: the trees main has held,
/// back [`BRACKET_SCAN`] landings. A whole-suite row on one of these commits
/// observed a tree main held, whoever armed it (tenet 687: a measurement is
/// keyed to the commit it measured, not to how it was armed).
#[derive(Debug, Default)]
pub(crate) struct MainChain {
    depth: HashMap<String, usize>,
}

impl MainChain {
    /// One commit per line, tip first.
    fn from_log(output: &str) -> Self {
        Self {
            depth: output
                .lines()
                .map(str::trim)
                .filter(|commit| !commit.is_empty())
                .enumerate()
                .map(|(depth, commit)| (commit.to_string(), depth))
                .collect(),
        }
    }

    /// How many landings behind the tip `commit` sits; `None` off the chain.
    fn depth(&self, commit: &str) -> Option<usize> {
        self.depth.get(commit).copied()
    }
}

fn main_chain(orch: &Orchestrator, repository: &Path, tip: &str) -> Result<MainChain, String> {
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let revset = format!("first_ancestors({tip}, {BRACKET_SCAN})");
    jj.run(
        repository,
        &[
            "log",
            "--ignore-working-copy",
            "--no-graph",
            "-r",
            &revset,
            "-T",
            r#"commit_id ++ "\n""#,
        ],
        "jj log (main cadence first-parent chain)",
    )
    .map(|output| MainChain::from_log(&output))
}

/// Where a row sits relative to main's tip, nearest first: the tree it
/// observed, then how recently it finished. A row's `ran_at` is its finish, so
/// a long run armed on an older tip can finish after a short one on a newer
/// tip; the tree decides. Without a chain, or off it, only the finish orders.
fn tree_order(chain: Option<&MainChain>, row: &CadenceRow) -> (usize, std::cmp::Reverse<i64>) {
    (
        chain
            .and_then(|chain| chain.depth(&row.commit))
            .unwrap_or(usize::MAX),
        std::cmp::Reverse(row.ran_at_ms),
    )
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
    /// Main's first-parent chain. A row of any cadence on it qualifies, and
    /// the nearest tree wins. Without it only the cadence's own rows qualify,
    /// newest finish first.
    main_chain: Option<&'a MainChain>,
}

/// How many recorded rows [`latest_complete_row`] reads before placing them on
/// main's chain: rows on agent branches and narrowed selections share the
/// table, so the newest few by finish time are not enough.
const CANDIDATE_SCAN: i64 = 64;

/// The newest whole-suite row for `lane` that classifies as complete and
/// passes `filter`.
///
/// A row qualifies when it observed the whole suite: `selection = 'full'` (a
/// fast, filtered, or empty selection is not the suite, tenet 695), nothing
/// deferred (a tiered run's deferred tests are unobserved), and no failure kind
/// (an infrastructure or killed run observed nothing). Its cadence is how it
/// was armed, not what it observed, so with a chain any cadence's row on main's
/// first-parent chain qualifies; the cadence's own rows qualify regardless,
/// having been armed on main's tip.
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
            "SELECT id, commit_sha, cadence FROM check_result_observations
              WHERE project_id = ?1 AND check_name = ?2 AND verdict_platform = ?3
                AND complete = 1 AND failure_kind IS NULL
                AND selection = 'full' AND deferred_count = 0 AND id != ?4
                AND (?5 = '' OR input_hash = ?5)
                AND (?6 = '' OR executor_id = ?6)
                AND (?7 = 1 OR cadence = 'main')
              ORDER BY ran_at DESC LIMIT ?8",
            (
                project_id.to_string(),
                lane.check.clone(),
                lane.platform.clone(),
                filter.excluding.unwrap_or("").to_string(),
                filter.input_hash.unwrap_or("").to_string(),
                filter.executor_id.unwrap_or("").to_string(),
                i64::from(filter.main_chain.is_some()),
                CANDIDATE_SCAN,
            ),
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut candidates: Vec<(String, usize)> = candidates
        .into_iter()
        .filter_map(|(id, commit, cadence)| {
            let depth = filter.main_chain.and_then(|chain| chain.depth(&commit));
            if depth.is_none() && cadence != "main" {
                return None;
            }
            Some((id, depth.unwrap_or(usize::MAX)))
        })
        .collect();
    // Stable, so rows on one tree keep the query's newest-finish-first order.
    candidates.sort_by_key(|(_, depth)| *depth);
    for (id, _) in candidates {
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

    /// The newest complete row for the lane on any executor. A merge is
    /// covered when any host's complete row sits on a tree that includes it,
    /// so coverage ends here, not at the attempt's own host's previous row.
    fn coverage(&self) -> Option<&CadenceRow> {
        match self {
            Comparison::Baseline => None,
            // `neighbour` is set exactly when another executor's row is newer.
            Comparison::SameExecutor {
                previous,
                neighbour,
            } => Some(neighbour.as_deref().unwrap_or(previous)),
            Comparison::OtherExecutor { previous } | Comparison::AnyExecutor { previous } => {
                Some(previous)
            }
        }
    }
}

/// Where an attempt's merge list starts. A complete row's list spans the
/// window its per-host diff measures; an incomplete attempt's lists what no
/// host's complete row covers yet.
fn bracket_base<'a>(
    verdict: &CadenceVerdict,
    comparison: &'a Comparison,
) -> Option<&'a CadenceRow> {
    if verdict.is_complete() {
        comparison.previous()
    } else {
        comparison.coverage()
    }
}

/// Choose the comparison for an attempt on `executor_id` from that executor's
/// newest complete row and the platform's newest complete row on any executor,
/// "newest" in [`tree_order`].
fn select_comparison(
    executor_id: Option<&str>,
    same_executor: Option<CadenceRow>,
    newest: Option<CadenceRow>,
    chain: Option<&MainChain>,
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
                    && tree_order(chain, row) < tree_order(chain, &previous)
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
/// against: whole-suite rows of any cadence on `chain`, when it is known.
async fn comparison_for(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    lane: &CadenceLane,
    current: Option<&CadenceRow>,
    chain: Option<&MainChain>,
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
                    main_chain: chain,
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
            main_chain: chain,
            ..RowFilter::default()
        },
        names,
    )
    .await?;
    Ok(select_comparison(executor_id, same_executor, newest, chain))
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

/// OS errors that describe the machine a test ran on rather than the tree it
/// tested: the face a census names, and the spellings that carry it (Rust's
/// `Display` and `Debug` of the error, SQLite's message). They are searched
/// for over the whole excerpt, not the signature: a failure that wraps the
/// error (`Custom { kind: StorageFull, error: PathError { path:
/// "/tmp/cairn-change-project-2jgZmE", .. } }`) carries a per-test temp path
/// on its signature line, so no two of them share a signature.
const HOST_CONDITIONS: &[(&str, &[&str])] = &[
    (
        "No space left on device",
        &["no space left on device", "storagefull", "(os error 28)"],
    ),
    (
        "Text file busy",
        &["text file busy", "executablefilebusy", "(os error 26)"],
    ),
    ("database is locked", &["database is locked"]),
];

/// How many failures in one row must share a host condition before they are
/// read as a census of the host rather than as tests. One test that hit a full
/// disk is still a red to look at; a run of them is the machine.
const HOST_CONDITION_MIN: usize = 2;

/// The host condition a failure excerpt names: see [`HOST_CONDITIONS`].
fn host_condition(excerpt: &str) -> Option<&'static str> {
    let excerpt = excerpt.to_ascii_lowercase();
    HOST_CONDITIONS
        .iter()
        .find(|(_, spellings)| spellings.iter().any(|spelling| excerpt.contains(spelling)))
        .map(|(face, _)| *face)
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
    /// The pass the runner's restart cancelled on this lane, when this one is
    /// the boot re-arm that succeeds it.
    pub(crate) rearmed: Option<&'a Interruption>,
}

fn short_commit(commit: &str) -> &str {
    commit.get(..12).unwrap_or(commit)
}

fn stamp(unix_ms: i64) -> String {
    crate::fleet::purpose::stamp(u64::try_from(unix_ms).unwrap_or(0))
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

/// A compared or covering row as the message names it: how it was armed and
/// where it ran ("the manual row on `bbs`"), so a hand-armed baseline reads
/// apart from a cadence pass.
fn armed(row: &CadenceRow) -> String {
    let cadence = if row.cadence == "main" {
        "cadence"
    } else {
        row.cadence.as_str()
    };
    format!("{cadence} row on `{}`", host(row))
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

/// Tests grouped by failure signature, largest group first.
type FailureGroups<'a> = Vec<(&'a str, Vec<&'a str>)>;

fn sorted_groups<'a>(groups: BTreeMap<&'a str, Vec<&'a str>>) -> FailureGroups<'a> {
    let mut groups: Vec<_> = groups.into_iter().collect();
    groups.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then(a.0.cmp(b.0)));
    groups
}

fn group_size(groups: &[(&str, Vec<&str>)]) -> usize {
    groups.iter().map(|(_, tests)| tests.len()).sum()
}

/// Failures in one row that share one OS error: the host's census, not tests.
#[derive(Debug)]
struct HostConditionGroup<'a> {
    face: &'static str,
    tests: Vec<&'a str>,
}

/// How a complete row differs from the row it is compared with. A test moved
/// only when both rows observed it: a test with no row in the compared
/// observation (born after that row's tree, or renamed since) has no earlier
/// state to have moved from.
#[derive(Debug, Default)]
struct RowDiff<'a> {
    /// At least [`HOST_CONDITION_MIN`] failures sharing one host condition,
    /// largest first. They are counted in no other field.
    host_conditions: Vec<HostConditionGroup<'a>>,
    /// Red here; passed or skipped in the compared row. Against no row at all
    /// (a baseline), every failure.
    newly_red: FailureGroups<'a>,
    /// Passed here, red in the compared row.
    newly_green: Vec<&'a str>,
    /// Red here, with no row in the compared observation.
    first_red: FailureGroups<'a>,
    /// Passed here, with no row in the compared observation.
    first_green: Vec<&'a str>,
    /// A row in the compared observation, none in this one.
    removed: Vec<&'a str>,
    /// Red in the compared row, skipped in this one.
    red_now_skipped: usize,
    /// Red in both rows.
    still_red: usize,
}

fn row_diff<'a>(current: &'a CadenceRow, previous: Option<&'a CadenceRow>) -> RowDiff<'a> {
    let mut diff = RowDiff::default();
    let mut faces: BTreeMap<&'static str, Vec<&str>> = BTreeMap::new();
    for (test, outcome) in &current.tests {
        if let TestOutcome::Failed(Failure {
            host_condition: Some(face),
            ..
        }) = outcome
        {
            faces.entry(*face).or_default().push(test.as_str());
        }
    }
    faces.retain(|_, tests| tests.len() >= HOST_CONDITION_MIN);
    let on_host: HashSet<&str> = faces.values().flatten().copied().collect();
    diff.host_conditions = faces
        .into_iter()
        .map(|(face, tests)| HostConditionGroup { face, tests })
        .collect();
    diff.host_conditions
        .sort_by(|a, b| b.tests.len().cmp(&a.tests.len()).then(a.face.cmp(b.face)));
    let mut newly_red: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    let mut first_red: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (test, outcome) in &current.tests {
        let test = test.as_str();
        // `None`: nothing to compare with. `Some(None)`: the compared row has
        // no row for this test.
        let before = previous.map(|previous| previous.tests.get(test));
        let was_red = matches!(before, Some(Some(TestOutcome::Failed(_))));
        match outcome {
            TestOutcome::Failed(_) if on_host.contains(test) => {}
            TestOutcome::Failed(failure) => {
                let signature = failure.signature.as_str();
                match before {
                    Some(None) => first_red.entry(signature).or_default().push(test),
                    _ if was_red => diff.still_red += 1,
                    _ => newly_red.entry(signature).or_default().push(test),
                }
            }
            TestOutcome::Passed => match before {
                Some(None) => diff.first_green.push(test),
                _ if was_red => diff.newly_green.push(test),
                _ => {}
            },
            TestOutcome::Skipped => {
                if was_red {
                    diff.red_now_skipped += 1;
                }
            }
        }
    }
    if let Some(previous) = previous {
        diff.removed = previous
            .tests
            .keys()
            .filter(|test| !current.tests.contains_key(*test))
            .map(String::as_str)
            .collect();
    }
    diff.newly_red = sorted_groups(newly_red);
    diff.first_red = sorted_groups(first_red);
    diff
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

/// Each host-condition group: the error and the host, the discriminator, and
/// the tests it holds.
fn render_host_conditions(
    out: &mut String,
    row: &CadenceRow,
    groups: &[HostConditionGroup<'_>],
    previous: Option<&CadenceRow>,
    neighbour: Option<&CadenceRow>,
) {
    for group in groups {
        let count = group.tests.len();
        let passed = |other: &CadenceRow| {
            group
                .tests
                .iter()
                .filter(|test| passed_in(other, test) == Some(true))
                .count()
        };
        out.push_str(&format!(
            "\n**HOST CONDITION on `{}`: {count} tests failed on `{}`** — one OS error across {count} tests is a condition of this machine, not of the tree, so none of them is counted as newly red or still red.\n",
            host(row),
            group.face
        ));
        let mut discriminator = match previous {
            Some(previous) => format!(
                "- Discriminator: {} of {count} passed on `{}` at the compared row ({}, commit `{}`)",
                passed(previous),
                host(previous),
                previous.reference,
                short_commit(&previous.commit)
            ),
            None => "- Discriminator: there is no earlier complete row to compare with".to_string(),
        };
        if let Some(neighbour) = neighbour {
            discriminator.push_str(&format!(
                "; {} of {count} passed on `{}` in the same window ({})",
                passed(neighbour),
                host(neighbour),
                neighbour.reference
            ));
        }
        discriminator.push_str(". The same test passing on the same host one tip earlier separates the host from the tree; a rerun here after the condition clears confirms it.\n");
        out.push_str(&discriminator);
        for test in group.tests.iter().take(MAX_IDS_PER_GROUP) {
            out.push_str(&format!("  - `{test}`\n"));
        }
        if count > MAX_IDS_PER_GROUP {
            out.push_str(&format!("  - … and {} more\n", count - MAX_IDS_PER_GROUP));
        }
    }
}

/// The "Compared with" line: which row the diff is against, and on which host.
fn render_basis(out: &mut String, report: &CadenceReport<'_>, row: &CadenceRow) {
    let line = match report.comparison {
        Comparison::Baseline => format!(
            "nothing. No earlier complete whole-suite row on main exists for {} on {}, so this row is the baseline and every failure is listed as newly red.",
            report.check, report.platform
        ),
        Comparison::SameExecutor { previous, .. } => format!(
            "{}, the previous {} (commit `{}`, ran {} earlier)",
            previous.reference,
            armed(previous),
            short_commit(&previous.commit),
            age(row.ran_at_ms, previous.ran_at_ms)
        ),
        Comparison::OtherExecutor { previous } => format!(
            "{}, the {} (commit `{}`, ran {} earlier). No earlier complete row exists on `{}`, so this is a **cross-host comparison**: what differs below may be the two hosts (provisioning, load, toolchain) rather than the tree, and none of it is reported as newly red or green.",
            previous.reference,
            armed(previous),
            short_commit(&previous.commit),
            age(row.ran_at_ms, previous.ran_at_ms),
            host(row)
        ),
        Comparison::AnyExecutor { previous } => format!(
            "{}, the {} (commit `{}`, ran {} earlier). This row names no executor, so the comparison may cross hosts.",
            previous.reference,
            armed(previous),
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
    let diff = row_diff(row, previous);
    let newly_red = group_size(&diff.newly_red);
    let first_red = group_size(&diff.first_red);
    let mut headline = if row.failed() == 0 {
        format!("all {} passed", thousands(row.ran()))
    } else {
        format!("{} failed", thousands(row.failed()))
    };
    if !diff.host_conditions.is_empty() {
        let conditions: Vec<String> = diff
            .host_conditions
            .iter()
            .map(|group| {
                format!(
                    "{} tests failed on one host condition: {}",
                    group.tests.len(),
                    group.face
                )
            })
            .collect();
        headline.push_str(&format!(" — {}", conditions.join("; ")));
    }
    let mut movement = match cross_host {
        Some(other) => format!(
            "cross-host against `{other}`: {newly_red} red here only, {} green here only",
            diff.newly_green.len()
        ),
        None => format!(
            "{newly_red} newly red, {} newly green",
            diff.newly_green.len()
        ),
    };
    if first_red + diff.first_green.len() > 0 {
        movement.push_str(&format!(
            " · first observed here: {first_red} red, {} green",
            diff.first_green.len()
        ));
    }
    if !diff.removed.is_empty() {
        movement.push_str(&format!(" · {} removed", diff.removed.len()));
    }
    out.push_str(&format!(
        "**Main cadence · {} · {} on `{}` · `{}`: {headline} — {movement}**\n\n",
        report.check,
        report.platform,
        host(row),
        short_commit(&row.commit)
    ));
    row_identity(out, row);
    render_basis(out, report, row);
    render_host_conditions(out, row, &diff.host_conditions, previous, neighbour);
    if newly_red > 0 {
        let title = match cross_host {
            Some(other) => format!("RED HERE, NOT RED ON `{other}`"),
            None => "NEWLY RED".to_string(),
        };
        out.push_str(&format!(
            "\n**{title}: {newly_red} test{} in {} failure group{}**\n",
            plural(newly_red),
            diff.newly_red.len(),
            plural(diff.newly_red.len())
        ));
        render_groups(out, &diff.newly_red, neighbour);
    }
    if first_red > 0 {
        out.push_str(&format!(
            "\n**FIRST OBSERVATION ON THIS HOST: RED · {first_red} test{} in {} failure group{}** — no row in the compared observation, so there is no earlier state to have moved from. A merge listed under **Merges since the compared row** added or renamed {}.\n",
            plural(first_red),
            diff.first_red.len(),
            plural(diff.first_red.len()),
            if first_red == 1 { "it" } else { "them" }
        ));
        render_groups(out, &diff.first_red, neighbour);
    }
    if !diff.newly_green.is_empty() {
        let title = match cross_host {
            Some(other) => format!("GREEN HERE, RED ON `{other}`"),
            None => "NEWLY GREEN".to_string(),
        };
        out.push_str(&format!("\n**{title}: {}**\n", diff.newly_green.len()));
        render_tests(out, "", &diff.newly_green);
    }
    if !diff.first_green.is_empty() {
        out.push_str(&format!(
            "\n**FIRST OBSERVATION ON THIS HOST: GREEN · {}** — no row in the compared observation.\n",
            diff.first_green.len()
        ));
        render_tests(out, "", &diff.first_green);
    }
    if !diff.removed.is_empty() {
        out.push_str(&format!(
            "\n**REMOVED SINCE THE COMPARED ROW: {}** — a row in the compared observation and none in this one (removed or renamed).\n",
            diff.removed.len()
        ));
        render_tests(out, "", &diff.removed);
    }
    if diff.red_now_skipped > 0 {
        let one = diff.red_now_skipped == 1;
        out.push_str(&format!(
            "\n{} previously red test{} skipped in this row, so {} not counted as green.\n",
            diff.red_now_skipped,
            if one { " was" } else { "s were" },
            if one { "it is" } else { "they are" }
        ));
    }
    if previous.is_some() {
        let label = if cross_host.is_some() {
            "Red on both hosts"
        } else {
            "Still red"
        };
        out.push_str(&format!("\n**{label}:** {} (not listed)\n", diff.still_red));
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

/// Why a lane posted no complete row.
#[derive(Clone, Copy)]
enum Unobserved {
    Death,
    NoVerdict,
    Restart { stopped_at_unix_ms: i64 },
}

fn render_incomplete(
    out: &mut String,
    report: &CadenceReport<'_>,
    row: Option<&CadenceRow>,
    cause: &str,
    class: Unobserved,
) {
    let tip = report
        .commit
        .or(row.map(|row| row.commit.as_str()))
        .map(|commit| format!(" · `{}`", short_commit(commit)))
        .unwrap_or_default();
    match class {
        Unobserved::Restart { stopped_at_unix_ms } => {
            out.push_str(&format!(
                "⏸️ **MAIN CADENCE PASS CANCELLED BY RESTART · {} · {}{tip}** — the runner began stopping at {} while this lane ran. Main stays unobserved for {} on {} until the pass the runner arms at boot completes.\n\n",
                report.check,
                report.platform,
                stamp(stopped_at_unix_ms),
                report.check,
                report.platform
            ));
            out.push_str("- Cause class: **cancelled by restart** — Cairn's own runner stopped mid-pass. Nothing died and nothing failed: neither the tree nor a machine was judged, and no row was recorded.\n");
            out.push_str("- Re-arm: at boot the runner arms this lane again for whatever main is then, and that pass's report cites this one.\n");
        }
        Unobserved::Death | Unobserved::NoVerdict => {
            out.push_str(&format!(
                "🚨 **MAIN CADENCE DID NOT COMPLETE · {} · {}{tip}** — main is unobserved for {} on {} until a cadence row completes.\n\n",
                report.check, report.platform, report.check, report.platform
            ));
            if matches!(class, Unobserved::Death) {
                out.push_str("- Cause class: **death** — the lane died before it produced a result (the execution side of the seam: no machine, capacity, infrastructure, a stalled or killed run).\n");
            } else {
                out.push_str("- Cause class: **no-verdict** — the lane ran to an exit, but its result is not a per-test verdict (the testing side of the seam: a test target that did not compile, zero tests, an incomplete enumeration). It is not green.\n");
            }
        }
    }
    out.push_str(&format!("- Cause: {cause}\n"));
    if let Some(row) = row {
        row_identity(out, row);
        let tail = row.output_tail.trim();
        if !tail.is_empty() {
            out.push_str(&format!(
                "\nOutput tail:\n```\n{}\n```\n",
                crate::execution::check_parsers::error_anchored_tail(tail, MAX_CAUSE_CHARS)
                    .replace("```", "'''")
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
            "- Last complete row: none. No complete whole-suite row on main has ever been recorded for {} on {}.\n",
            report.check, report.platform
        )),
        Comparison::SameExecutor { previous, .. } => out.push_str(&format!(
            "- Last complete row: {}, the previous {}, at {}.\n",
            previous.reference,
            armed(previous),
            since(previous)
        )),
        Comparison::OtherExecutor { previous } => out.push_str(&format!(
            "- Last complete row: none on `{}`. The newest complete row on {} is {}, the {}, at {}.\n",
            row.map_or("this executor", host),
            report.platform,
            previous.reference,
            armed(previous),
            since(previous)
        )),
        Comparison::AnyExecutor { previous } => out.push_str(&format!(
            "- Last complete row: {}, the {} (the newest on {}, any executor), at {}.\n",
            previous.reference,
            armed(previous),
            report.platform,
            since(previous)
        )),
    }
    if let (Some(previous), Some(covering)) =
        (report.comparison.previous(), report.comparison.coverage())
    {
        if covering.id != previous.id {
            out.push_str(&format!(
                "- Covered through: {}, the {}, at {}, the newest complete row on {} on any executor. A merge is covered when any host's complete whole-suite row includes it, whoever armed the run, so the list below starts there.\n",
                covering.reference,
                armed(covering),
                since(covering),
                report.platform
            ));
        }
    }
    if report.comparison.previous().is_some() && report.commit.is_some() {
        render_bracket(
            out,
            "Merges not covered by a complete row on any executor",
            report.bracket,
        );
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
            Unobserved::NoVerdict,
        ),
        CadenceVerdict::Death { cause } => render_incomplete(
            &mut out,
            report,
            None,
            &truncate(cause, MAX_CAUSE_CHARS),
            Unobserved::Death,
        ),
        CadenceVerdict::Cancelled {
            cause,
            stopped_at_unix_ms,
        } => render_incomplete(
            &mut out,
            report,
            None,
            &truncate(cause, MAX_CAUSE_CHARS),
            Unobserved::Restart {
                stopped_at_unix_ms: *stopped_at_unix_ms,
            },
        ),
    }
    if let Some(interruption) = report.rearmed {
        out.push_str(&format!(
            "\n- Re-armed at boot: this pass succeeds {}.\n",
            interruption.describe()
        ));
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
            cadence: "main".to_string(),
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

    fn failed(excerpt: &str) -> TestOutcome {
        TestOutcome::Failed(Failure::from_excerpt(excerpt))
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
            rearmed: None,
        })
    }

    /// The armed pass's branch and arming instant.
    fn armed(slots: &CadenceSlots, project_id: &str) -> Option<(String, i64)> {
        slots
            .in_flight(project_id)
            .map(|pass| (pass.branch, pass.armed_at_unix_ms))
    }

    #[test]
    fn requests_during_a_pass_coalesce_into_one_queued_successor() {
        let mut slots = CadenceSlots::default();
        assert!(slots.request("p", "main", Trigger::Startup, 1));
        assert_eq!(armed(&slots, "p"), Some(("main".to_string(), 1)));
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
        assert_eq!(armed(&slots, "p"), Some(("main".to_string(), 6)));
        assert_eq!(
            slots.finish("p", 7),
            None,
            "one successor, never one per merge"
        );
        assert_eq!(armed(&slots, "p"), None);
        assert!(
            slots.request("p", "main", Trigger::Merge, 8),
            "a released slot starts afresh"
        );
        assert!(
            slots.request("q", "trunk", Trigger::Merge, 8),
            "projects are independent"
        );
    }

    /// A settled debt outranks the routine re-arms, so a pass queued behind it
    /// is not reported as a heartbeat, and yields only to a merge.
    #[test]
    fn a_settled_debt_ranks_above_routine_rearms_and_below_a_merge() {
        assert!(Trigger::DebtSettled > Trigger::Heartbeat);
        assert!(Trigger::DebtSettled > Trigger::Startup);
        assert!(Trigger::DebtSettled < Trigger::Merge);
        let mut slots = CadenceSlots::default();
        assert!(slots.request("p", "main", Trigger::Startup, 1));
        assert!(!slots.request("p", "main", Trigger::Heartbeat, 2));
        assert!(!slots.request("p", "main", Trigger::DebtSettled, 3));
        assert_eq!(slots.finish("p", 4), Some(Trigger::DebtSettled));
    }

    /// One debt, one waiter, however many passes it refuses.
    #[test]
    fn a_blocked_question_gets_exactly_one_waiter() {
        let key = crate::fleet::purpose::PurposeKey::main_cadence("p", "rust-tests", "linux");
        let mut slots = CadenceSlots::default();
        assert!(slots.watch_debt(&key));
        assert!(!slots.watch_debt(&key));
        slots.debt_settled(&key);
        assert!(slots.watch_debt(&key), "a later debt gets its own waiter");
    }

    /// The waiter wakes when the question's last debt settles, and not before.
    #[tokio::test]
    async fn the_debt_waiter_wakes_only_when_the_question_owes_nothing() {
        let fleet = crate::fleet::Fleet::default();
        let key = crate::fleet::purpose::PurposeKey::main_cadence("p", "rust-tests", "linux");
        let old = ("old".to_string(), "old-attempt".to_string());
        fleet.admit_purpose_for_test(&key, &old, 10);
        fleet.admit_purpose_for_test(&key, &("new".into(), "new-attempt".into()), 20);
        assert!(fleet.purpose_owes(&key));
        let mut cleared = {
            let fleet = fleet.clone();
            let key = key.clone();
            tokio::spawn(async move { fleet.purpose_debt_cleared(&key).await })
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut cleared)
                .await
                .is_err(),
            "an owed question does not wake its waiter"
        );
        fleet.finish_purpose_for_test(&old);
        tokio::time::timeout(Duration::from_secs(5), cleared)
            .await
            .expect("settlement wakes the waiter")
            .unwrap();
    }

    /// Performance's specimen: project `rs` logged "the checks contract at
    /// <sha> could not be read" about every 30 minutes on an unchanged tip,
    /// 48 lines on 10-21. Its config declares MCP servers and references and
    /// no checks, which is no lanes, not a failure.
    #[tokio::test]
    async fn an_unreadable_contract_is_one_outcome_per_tip_and_no_checks_is_no_lanes() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let slots = std::sync::Mutex::new(CadenceSlots::default());
        let reads = AtomicUsize::new(0);
        let unreadable = || {
            reads.fetch_add(1, Ordering::SeqCst);
            async {
                Err::<CommitChecksContract, _>(CommitChecksContractError::Parse(
                    "mapping values are not allowed here".into(),
                ))
            }
        };
        let first = tip_lanes(&slots, "p", "tip-1", unreadable).await;
        assert!(
            first
                .as_ref()
                .is_err_and(|error| error.contains("failed to parse") && error.contains("tip-1")),
            "{first:?}"
        );
        assert_eq!(
            tip_lanes(&slots, "p", "tip-1", unreadable).await,
            Ok(Vec::new()),
            "one recorded outcome per tip"
        );
        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "an unchanged tip is not read again"
        );
        assert!(
            tip_lanes(&slots, "q", "tip-1", unreadable).await.is_err(),
            "projects are independent"
        );
        assert!(
            tip_lanes(&slots, "p", "tip-2", unreadable).await.is_err(),
            "a moved tip is read again"
        );
        assert_eq!(reads.load(Ordering::SeqCst), 3);

        let no_checks =
            || async { Err::<CommitChecksContract, _>(CommitChecksContractError::NoChecks) };
        let no_config =
            || async { Err::<CommitChecksContract, _>(CommitChecksContractError::Missing) };
        assert_eq!(
            tip_lanes(&slots, "rs", "tip-1", no_checks).await,
            Ok(Vec::new())
        );
        assert_eq!(
            tip_lanes(&slots, "rs", "tip-1", no_config).await,
            Ok(Vec::new())
        );
    }

    /// Testing's specimen (cairn/5441): the 15:48 rebuild cancelled the pass on
    /// 5027f547 while merges had moved main to 7e1b2a9a, and after boot nothing
    /// was armed. A pass binds no tip and resolves main only when it starts, so
    /// the boot pass attests wherever main moved to. What must hold is that the
    /// restart leaves exactly one armed pass, citing the cancelled one, however
    /// many arms the boot, its catch-up, and the merges around them make.
    #[tokio::test]
    async fn a_pass_the_restart_cancelled_is_rearmed_exactly_once_at_boot_and_cited() {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.path().join("cadence.db"))
            .await
            .unwrap();
        let lane = CadenceLane {
            check: "rust-tests".into(),
            platform: "linux".into(),
            notify: "testing".into(),
        };

        // Before the restart: a pass on the old tip, a merge queued behind it,
        // and the stop ending its lane.
        let mut before = CadenceSlots::default();
        assert!(before.request("p", "main", Trigger::Merge, 1));
        assert!(!before.request("p", "main", Trigger::Merge, 2));
        let stopped_at = 3 * HOUR;
        let ended = cancelled_by_restart(
            CadenceVerdict::Death {
                cause: "infrastructure: Cairn cancelled this check before it produced a verdict: Cairn's runner is stopping.".into(),
            },
            Some(stopped_at as u64),
        );
        assert!(
            matches!(ended, CadenceVerdict::Cancelled { stopped_at_unix_ms, .. } if stopped_at_unix_ms == stopped_at),
            "{ended:?}"
        );
        let cancelled = Interruption {
            check: lane.check.clone(),
            platform: lane.platform.clone(),
            commit: "5027f5473a57a1b2".into(),
            stopped_at_unix_ms: stopped_at,
        };
        record_interruption(&db, "p", &cancelled).await.unwrap();
        // The process ends, and its slots, queued successor included, with it.
        drop(before);

        // Boot: a fresh process reads back what the restart cancelled, and arms.
        let mut slots = CadenceSlots::default();
        assert!(
            slots.in_flight("p").is_none(),
            "no pass survives the restart in memory"
        );
        for (project_id, interruption) in take_interruptions(&db).await.unwrap() {
            slots.interrupted(&project_id, interruption);
        }
        assert!(
            slots.request("p", "main", Trigger::Startup, 4),
            "the boot arm starts a pass"
        );
        // The catch-up's re-arm and a merge seen during the boot pass are one
        // queued successor, not more passes.
        assert!(!slots.request("p", "main", Trigger::Startup, 5));
        assert!(!slots.request("p", "main", Trigger::Merge, 6));
        let armed = slots
            .in_flight("p")
            .expect("main has a pass armed after boot");
        assert_eq!(armed.armed_at_unix_ms, 4);
        assert_eq!(
            armed.rearming,
            vec![cancelled.clone()],
            "the armed pass names the pass it re-arms"
        );
        assert!(
            take_interruptions(&db).await.unwrap().is_empty(),
            "one boot cites a cancel, not every boot"
        );

        // The lane's report on the moved tip cites the cancelled pass, once.
        let cited = slots
            .take_interruption("p", &lane)
            .expect("the lane's first report takes the citation");
        assert!(slots.take_interruption("p", &lane).is_none());
        let moved = row(
            "r",
            "7e1b2a9a1d6bc3d4",
            5 * HOUR,
            &[("t", TestOutcome::Passed)],
        );
        let message = render_report(&CadenceReport {
            check: "rust-tests",
            platform: "linux",
            commit: Some("7e1b2a9a1d6bc3d4"),
            verdict: &CadenceVerdict::Complete(moved),
            comparison: &Comparison::Baseline,
            bracket: &Bracket::default(),
            now_ms: 10 * HOUR,
            rearmed: Some(&cited),
        });
        assert!(
            message.contains("Re-armed at boot") && message.contains("`5027f5473a57`"),
            "{message}"
        );

        assert_eq!(slots.finish("p", 7), Some(Trigger::Merge));
        assert_eq!(slots.finish("p", 8), None);
        assert!(slots.in_flight("p").is_none());
    }

    #[test]
    fn a_lane_ended_by_the_restart_reads_as_cancelled_never_as_a_death() {
        let death = CadenceVerdict::Death {
            cause: "infrastructure: Cairn cancelled this check before it produced a verdict: Cairn's runner is stopping.".into(),
        };
        assert!(
            matches!(
                cancelled_by_restart(death.clone(), None),
                CadenceVerdict::Death { .. }
            ),
            "without a stop, a death is a death"
        );
        assert!(matches!(
            cancelled_by_restart(
                CadenceVerdict::NoVerdict {
                    row: None,
                    cause: "lost".into()
                },
                Some(1)
            ),
            CadenceVerdict::Cancelled { .. }
        ));
        let complete = CadenceVerdict::Complete(row("r", "c", HOUR, &[("t", TestOutcome::Passed)]));
        assert!(
            matches!(
                cancelled_by_restart(complete, Some(1)),
                CadenceVerdict::Complete(_)
            ),
            "a row is the tree's answer, stop or no stop"
        );

        let cancelled = cancelled_by_restart(death, Some(9 * HOUR as u64));
        let message = report(&cancelled, &Comparison::Baseline, &Bracket::default());
        assert!(message.contains("CANCELLED BY RESTART"), "{message}");
        assert!(message.contains("**cancelled by restart**"), "{message}");
        assert!(
            !message.contains("DID NOT COMPLETE") && !message.contains("**death**"),
            "{message}"
        );
    }

    #[test]
    fn a_citation_no_lane_took_ends_with_its_pass() {
        let mut slots = CadenceSlots::default();
        slots.interrupted(
            "p",
            Interruption {
                check: "retired-check".into(),
                platform: "linux".into(),
                commit: "c".into(),
                stopped_at_unix_ms: 1,
            },
        );
        assert!(slots.request("p", "main", Trigger::Startup, 2));
        assert_eq!(slots.in_flight("p").unwrap().rearming.len(), 1);
        assert_eq!(slots.finish("p", 3), None);
        assert!(slots.request("p", "main", Trigger::Merge, 4));
        assert!(
            slots.in_flight("p").unwrap().rearming.is_empty(),
            "a lane the tip no longer declares is not re-armed forever"
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
                "Compared with: cairn://p/cairn/check-observations/prev, the previous cadence row on `bbs`"
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
            text.contains("4 newly red, 1 newly green · 1 removed"),
            "{text}"
        );
        let removed = text
            .find("REMOVED SINCE THE COMPARED ROW: 1**")
            .expect("a vanished test is listed as removed");
        assert!(text[removed..].contains("- `core::removed`"), "{text}");
        let green = text.find("NEWLY GREEN").unwrap();
        assert!(
            !text[green..removed].contains("core::removed"),
            "a vanished test is not green: {text}"
        );
        assert!(text.contains("**Still red:** 1 (not listed)"), "{text}");
        assert!(
            !text.contains("- `core::old_red`"),
            "still-red tests are counted, not listed"
        );
        assert!(text.contains("ran 3h"), "{text}");
    }

    /// Specimen 203c35ce: two tests born in a merge after the compared row's
    /// tree were rendered newly red against a row that could not have had them.
    #[test]
    fn a_test_with_no_row_in_the_compared_observation_is_a_first_observation() {
        let previous = row(
            "prev",
            "aaaaaaaaaaaaaaaa",
            HOUR,
            &[
                ("core::steady", TestOutcome::Passed),
                ("core::steady_red", failed("stale")),
                ("core::dropped_a", TestOutcome::Passed),
                ("core::dropped_b", failed("gone")),
            ],
        );
        let seeding = "assertion `left == right` failed: seeded clone";
        let current = row(
            "cur",
            "bbbbbbbbbbbbbbbb",
            2 * HOUR,
            &[
                ("core::steady", TestOutcome::Passed),
                ("core::steady_red", failed("stale")),
                ("clone_seeding::seeds_the_clone", failed(seeding)),
                ("clone_seeding::seeds_the_index", failed(seeding)),
                ("clone_seeding::reuses_the_seed", TestOutcome::Passed),
            ],
        );
        let bracket = Bracket {
            merges: vec![BracketMerge {
                commit: "4227422742274227".into(),
                subject: "Seed clones from the store (cairn/5100) (#4227)".into(),
                pr: Some(4227),
                issue: Some("cairn/5100".into()),
            }],
            ..Bracket::default()
        };
        let text = report(
            &CadenceVerdict::Complete(current),
            &same(&previous),
            &bracket,
        );
        assert!(
            text.contains("3 failed — 0 newly red, 0 newly green · first observed here: 2 red, 1 green · 2 removed"),
            "{text}"
        );
        assert!(
            !text.contains("NEWLY RED") && !text.contains("NEWLY GREEN"),
            "a test the compared row never observed cannot have moved: {text}"
        );
        let first_red = text
            .find("**FIRST OBSERVATION ON THIS HOST: RED · 2 tests in 1 failure group**")
            .expect("first-observation reds are their own section");
        assert!(
            text[first_red..].contains("- `clone_seeding::seeds_the_clone`")
                && text[first_red..].contains("- `clone_seeding::seeds_the_index`"),
            "{text}"
        );
        assert!(
            text.contains("**FIRST OBSERVATION ON THIS HOST: GREEN · 1**")
                && text.contains("- `clone_seeding::reuses_the_seed`"),
            "{text}"
        );
        assert!(
            text.contains("**REMOVED SINCE THE COMPARED ROW: 2**")
                && text.contains("- `core::dropped_a`")
                && text.contains("- `core::dropped_b`"),
            "a removed test is said, not silent: {text}"
        );
        assert!(text.contains("**Still red:** 1 (not listed)"), "{text}");
        assert!(
            text.contains("- `422742274227` #4227 · cairn/5100"),
            "the bracket names the landing that added them: {text}"
        );
    }

    /// Specimen 5715631a: fedora's tmpfs `/tmp` ran out of inodes and 48
    /// tests failed with `StorageFull`, each under its own temp path, so each
    /// had its own signature and all of them rendered newly red.
    #[test]
    fn a_shared_os_error_across_many_failures_is_one_host_condition_never_newly_red() {
        let names: Vec<String> = (0..50)
            .map(|n| format!("mcp_file_changes::case_{n:02}"))
            .collect();
        let enospc = |n: usize| {
            format!(
                "thread 'mcp_file_changes::case_{n:02}' (23610{n}) panicked at os/cairn-core/tests/main/mcp_file_changes.rs:979:14:\ncalled `Result::unwrap()` on an `Err` value: Custom {{ kind: StorageFull, error: PathError {{ path: \"/tmp/cairn-change-project-{n:06x}\", err: Os {{ code: 28, kind: StorageFull, message: \"No space left on device\" }} }} }}\nnote: run with `RUST_BACKTRACE=1`"
            )
        };
        let passing: Vec<(&str, TestOutcome)> = names
            .iter()
            .map(|name| (name.as_str(), TestOutcome::Passed))
            .collect();
        let full: Vec<(&str, TestOutcome)> = names
            .iter()
            .enumerate()
            .map(|(n, name)| (name.as_str(), failed(&enospc(n))))
            .collect();
        let signatures: HashSet<String> = full
            .iter()
            .filter_map(|(_, outcome)| match outcome {
                TestOutcome::Failed(failure) => Some(failure.signature.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            signatures.len(),
            50,
            "every failure has its own signature, as in the specimen"
        );

        let previous = on("fedora", row("prev", "aaaaaaaaaaaaaaaa", HOUR, &passing));
        let current = on("fedora", row("cur", "bbbbbbbbbbbbbbbb", 5 * HOUR, &full));
        let text = report(
            &CadenceVerdict::Complete(current),
            &same(&previous),
            &Bracket::default(),
        );
        assert!(
            text.contains("50 failed — 50 tests failed on one host condition: No space left on device — 0 newly red, 0 newly green"),
            "{text}"
        );
        assert_eq!(
            text.matches("**HOST CONDITION").count(),
            1,
            "one census group: {text}"
        );
        assert!(
            text.contains(
                "**HOST CONDITION on `fedora`: 50 tests failed on `No space left on device`**"
            ),
            "{text}"
        );
        assert!(
            text.contains("Discriminator: 50 of 50 passed on `fedora` at the compared row (cairn://p/cairn/check-observations/prev"),
            "{text}"
        );
        assert!(text.contains("- … and 40 more"), "{text}");
        assert!(!text.contains("NEWLY RED"), "{text}");
        assert!(text.contains("**Still red:** 0"), "{text}");

        // One test that hit the condition is still a red to look at.
        let mut lone = passing.clone();
        lone[0].1 = failed("called `Result::unwrap()` on an `Err` value: database is locked");
        let current = on("fedora", row("cur", "bbbbbbbbbbbbbbbb", 5 * HOUR, &lone));
        let text = report(
            &CadenceVerdict::Complete(current),
            &same(&previous),
            &Bracket::default(),
        );
        assert!(text.contains("1 failed — 1 newly red"), "{text}");
        assert!(!text.contains("HOST CONDITION"), "{text}");
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
            text.contains("No complete whole-suite row on main has ever been recorded"),
            "{text}"
        );
        assert!(
            !text.contains("passed**") && !text.contains("all 0 passed"),
            "{text}"
        );

        // The only error, above more warnings than the message quotes, still
        // leads the quoted tail.
        let mut buried = row("cur3", "bbbbbbbbbbbb", HOUR, &[]);
        buried.exit_code = 101;
        buried.verdict_passed = false;
        buried.output_tail = format!(
            "error[E0425]: cannot find value `turn_id` in this scope\n   --> os/cairn-core/src/lib.rs:7:9\n{}error: could not compile `cairn-core` (lib) due to 1 previous error",
            "warning: unused variable: `scratch`\n".repeat(80)
        );
        let text = report(
            &classify_row(buried),
            &Comparison::Baseline,
            &Bracket::default(),
        );
        assert!(
            text.contains("Output tail:\n```\nerror[E0425]: cannot find value `turn_id` in this scope\n   --> os/cairn-core/src/lib.rs:7:9\n…\n"),
            "{text}"
        );
        assert!(text.contains("due to 1 previous error\n```"), "{text}");

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
            text.contains("check-observations/prev, the cadence row on `bbs`")
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
        assert!(
            text.contains("the previous cadence row on `fedora`"),
            "{text}"
        );
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

    /// Specimen 03d65964: bbs's no-verdict wake listed five merges as
    /// uncovered that fedora's complete row e0f326a1 had covered 17 minutes
    /// earlier.
    #[test]
    fn merges_not_covered_are_keyed_by_tree_across_executors() {
        let previous = row(
            "prev",
            "aaaaaaaaaaaaaaaa",
            HOUR,
            &[("a", TestOutcome::Passed)],
        );
        let fedora = on(
            "fedora",
            row(
                "other",
                "cccccccccccccccc",
                2 * HOUR,
                &[("a", TestOutcome::Passed)],
            ),
        );
        let mut partial = row(
            "cur",
            "bbbbbbbbbbbbbbbb",
            3 * HOUR,
            &[("a", TestOutcome::Passed)],
        );
        partial.complete = false;
        let no_verdict = classify_row(partial.clone());
        let comparison = Comparison::SameExecutor {
            previous: previous.clone(),
            neighbour: Some(Box::new(fedora)),
        };
        assert_eq!(
            bracket_base(&no_verdict, &comparison).map(|row| row.id.as_str()),
            Some("other"),
            "an incomplete attempt's merge list starts at the newest complete row on any host"
        );
        partial.complete = true;
        assert_eq!(
            bracket_base(&classify_row(partial), &comparison).map(|row| row.id.as_str()),
            Some("prev"),
            "a complete row's merge list stays the window of its per-host diff"
        );
        let death = CadenceVerdict::Death {
            cause: "no machine".into(),
        };
        assert_eq!(
            bracket_base(&death, &same(&previous)).map(|row| row.id.as_str()),
            Some("prev")
        );

        let text = report(&no_verdict, &comparison, &Bracket::default());
        assert!(
            text.contains("Last complete row: cairn://p/cairn/check-observations/prev, the previous cadence row on `bbs`"),
            "{text}"
        );
        assert!(
            text.contains("Covered through: cairn://p/cairn/check-observations/other, the cadence row on `fedora`"),
            "{text}"
        );
        assert!(
            text.contains("**Merges not covered by a complete row on any executor (0)**"),
            "{text}"
        );
        let text = report(&no_verdict, &same(&previous), &Bracket::default());
        assert!(
            !text.contains("Covered through"),
            "this host's own row is the newest: {text}"
        );
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
            text.contains("Last complete row: none on `fedora`. The newest complete row on linux is cairn://p/cairn/check-observations/prev, the cadence row on `bbs`"),
            "{text}"
        );
        assert!(
            !text.contains("has ever been recorded"),
            "a complete row exists on the platform: {text}"
        );
    }

    /// One observation to store; [`insert_observation`] fills the usual shape.
    struct Stored<'a> {
        id: &'a str,
        executor: &'a str,
        ran_at: i64,
        cadence: &'a str,
        commit: &'a str,
        selection: &'a str,
        failure_kind: Option<&'a str>,
        tests: &'a [(&'a str, &'a str)],
    }

    /// A complete whole-suite row on commit `c-{id}`.
    async fn insert_observation(
        db: &LocalDb,
        id: &str,
        executor: &str,
        ran_at: i64,
        cadence: &str,
        tests: &[(&str, &str)],
    ) {
        let commit = format!("c-{id}");
        store(
            db,
            Stored {
                id,
                executor,
                ran_at,
                cadence,
                commit: &commit,
                selection: "full",
                failure_kind: None,
                tests,
            },
        )
        .await;
    }

    async fn store(db: &LocalDb, row: Stored<'_>) {
        let Stored {
            id,
            executor,
            ran_at,
            cadence,
            commit,
            selection,
            failure_kind,
            tests,
        } = row;
        let failure_kind = failure_kind.map_or("NULL".to_string(), |kind| format!("'{kind}'"));
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
                cadence, output_tail, executor_id, verdict_platform, public_handle,
                selection, failure_kind)
             VALUES('{id}','p','{commit}','t','rust-tests','i-{id}','env',{exit_code},'{verdict}',1,0,
                    1,1,{ran_at},5,'main:p','{cadence}','','{executor}','linux','h-{id}',
                    '{selection}',{failure_kind});"
        );
        for (test, status) in tests {
            script.push_str(&format!(
                "INSERT INTO check_test_results (observation_id, test_id, status, failure_excerpt)
                 VALUES('{id}','{test}','{status}','boom');"
            ));
        }
        db.execute_script(script).await.unwrap();
    }

    async fn cadence_db(temp: &tempfile::TempDir) -> LocalDb {
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
        db
    }

    fn linux_lane() -> CadenceLane {
        CadenceLane {
            check: "rust-tests".into(),
            platform: "linux".into(),
            notify: "testing".into(),
        }
    }

    #[tokio::test]
    async fn the_previous_row_is_the_same_executors_with_a_platform_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let db = cadence_db(&temp).await;
        let lane = linux_lane();
        let names = |executor: &str| format!("{executor}-host");
        // No first-parent chain is known here, so only cadence rows qualify.
        let comparison = |current: Option<CadenceRow>| {
            let db = &db;
            let lane = &lane;
            async move {
                comparison_for(db, "p", "cairn", lane, current.as_ref(), None, &names)
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
        // flag with no tests (a compile failure), and a manual run that no
        // chain places on main.
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

    /// cairn/5431: a hand-armed whole-suite row on main observed main's tree,
    /// so it covers the merges up to it and is a nearer comparison than an
    /// older cadence row. Narrowed selections, failed runs, rows off main's
    /// first-parent chain, and a longer run that finished later on an older
    /// tree never take its place.
    #[tokio::test]
    async fn a_whole_suite_row_of_any_cadence_on_main_covers_and_compares() {
        let temp = tempfile::tempdir().unwrap();
        let db = cadence_db(&temp).await;
        let lane = linux_lane();
        let names = |executor: &str| executor.to_string();
        // Main's first-parent chain, tip first.
        let chain = MainChain::from_log("tip\nx\nb\nq\na\n");
        let comparison = |current: Option<CadenceRow>, with_chain: bool| {
            let db = &db;
            let lane = &lane;
            let chain = with_chain.then_some(&chain);
            async move {
                comparison_for(db, "p", "cairn", lane, current.as_ref(), chain, &names)
                    .await
                    .unwrap()
            }
        };
        let passed: &[(&str, &str)] = &[("a", "passed")];
        let shaped = |id, executor, ran_at, cadence, commit, selection, failure_kind| Stored {
            id,
            executor,
            ran_at,
            cadence,
            commit,
            selection,
            failure_kind,
            tests: passed,
        };
        for row in [
            // The cadence's last row, on A.
            shaped("cad-a", "fedora", 1, "main", "a", "full", None),
            // `cairn check run rust-tests --branch main` on B.
            shaped("man-b", "bbs", 2, "manual", "b", "full", None),
            // Nearer the tip than B, but none of them observed the suite.
            shaped("fast-x", "bbs", 4, "manual", "x", "fast", None),
            shaped("filtered-x", "bbs", 5, "review", "x", "filtered", None),
            shaped(
                "infra-x",
                "bbs",
                6,
                "manual",
                "x",
                "full",
                Some("infrastructure"),
            ),
            // A whole-suite row on an agent branch never observed main.
            shaped("side", "bbs", 7, "review", "side", "full", None),
            // A long run armed on an older tip, finishing after B's.
            shaped("man-q", "bbs", 8, "manual", "q", "full", None),
        ] {
            store(&db, row).await;
        }

        // The cadence dies on the tip: B's manual row is the newest tree any
        // complete whole-suite row observed, so the uncovered list starts at B.
        let death = CadenceVerdict::Death {
            cause: "no machine".into(),
        };
        let covered = comparison(None, true).await;
        match &covered {
            Comparison::AnyExecutor { previous } => {
                assert_eq!(previous.id, "man-b");
                assert_eq!(previous.cadence, "manual");
            }
            other => panic!("expected the platform's nearest row, got {other:?}"),
        }
        assert_eq!(
            bracket_base(&death, &covered).map(|row| row.commit.as_str()),
            Some("b"),
            "merges up to and including B are covered"
        );
        let text = report(&death, &covered, &Bracket::default());
        assert!(
            text.contains("Last complete row: cairn://p/cairn/check-observations/h-man-b, the manual row on `bbs`"),
            "{text}"
        );

        // Without the chain nothing places a manual row on main: the
        // cadence's own row is all that qualifies.
        match comparison(None, false).await {
            Comparison::AnyExecutor { previous } => assert_eq!(previous.id, "cad-a"),
            other => panic!("expected the cadence's own row, got {other:?}"),
        }

        // A cadence row on the tip compares with its own host's nearest row
        // of any cadence, and the other host's nearer row is the neighbour
        // and the coverage.
        store(
            &db,
            shaped("man-f", "fedora", 3, "manual", "q", "full", None),
        )
        .await;
        store(
            &db,
            shaped("cad-tip", "fedora", 10, "main", "tip", "full", None),
        )
        .await;
        let tip = load_row(&db, "cairn", "cad-tip", &names).await.unwrap();
        let compared = comparison(Some(tip.clone()), true).await;
        match &compared {
            Comparison::SameExecutor {
                previous,
                neighbour,
            } => {
                assert_eq!(previous.id, "man-f", "q is nearer the tip than A");
                assert_eq!(
                    neighbour.as_deref().map(|row| row.id.as_str()),
                    Some("man-b")
                );
            }
            other => panic!("expected a same-executor comparison, got {other:?}"),
        }
        assert_eq!(
            compared.coverage().map(|row| row.id.as_str()),
            Some("man-b")
        );
        let text = report(
            &CadenceVerdict::Complete(tip),
            &compared,
            &Bracket::default(),
        );
        assert!(
            text.contains("Compared with: cairn://p/cairn/check-observations/h-man-f, the previous manual row on `fedora`"),
            "{text}"
        );
    }

    #[test]
    fn the_first_parent_chain_reads_tip_first() {
        let chain = MainChain::from_log("tip\n  mid \n\nroot\n");
        assert_eq!(chain.depth("tip"), Some(0));
        assert_eq!(chain.depth("mid"), Some(1));
        assert_eq!(chain.depth("root"), Some(2));
        assert_eq!(chain.depth("side"), None);
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
