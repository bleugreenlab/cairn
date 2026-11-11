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
//! What a lane's verdict is made of follows from the command it runs
//! ([`LaneVerdict`]). A test runner's verdict is its per-test enumeration. Any
//! other command (a typecheck, a build) has its exit as its verdict, and its
//! message names the move between the two rows' verdicts and quotes a red
//! one's output. Such a lane runs in a minute or two after nearly every merge,
//! so a green that follows a green is recorded but wakes no one.
//!
//! Scheduling is coalesced per lane and never preemptive. A whole-suite run
//! takes most of an hour while merges land every few minutes, so a newer tip
//! never cancels the run in flight; it queues at most one more run of that
//! lane, on the newest tip an arm resolved. Lanes never wait for each other: a
//! lane parked for hours on one platform does not hold back another platform's
//! next row, and a lane's own re-arm runs only that lane. The runner arms every
//! project at boot ([`arm_at_boot`]), and a heartbeat re-arms every project on
//! a fixed interval, so a tip that never got a complete row is retried and a
//! lane that has gone quiet says so.
//!
//! Remote main-cadence lanes survive a restart through durable grant claims.
//! Boot reserves each old lane before arming its single successor, rebuilds the
//! original contract, and binds the old attempt to the existing recorder.
//! Colocated lanes still read **cancelled by restart** and are re-armed at boot.
//!
//! A lane that does not produce a complete verdict is announced louder than any
//! red, with its cause class: **death** (the lane died: placement, capacity,
//! infrastructure, a stalled or killed run, a compile the host ended by signal
//! or by a storage error) or **no-verdict** (the lane ran to an exit but its result
//! is not a verdict: a test target that did not compile, zero tests, an
//! incomplete enumeration, a run that selected nothing). Under a post-merge
//! detection regime, a silent detector is the dangerous case.
//!
//! Every attempt is recorded (its row, its overview line); only news wakes the
//! `notify` thread ([`observe`], [`CadenceSlots::wake`]). A complete row wakes
//! when it differs from its own host's previous row, is the first on its host,
//! or ends a silence: [`SILENCE_BOUND_MS`] since that row, or the first row
//! after a non-verdict. A non-verdict wakes when it is not the one the lane last
//! announced. An unchanged one (a hold waiting on the same debt, a suppression
//! of the same inputs, a retried refusal whose only change is how stale its
//! evidence is) wakes no one, and a standing hold re-announces once per bound as
//! still held. Each wake costs the thread a turn, and a channel that repeats
//! itself trains its reader to skim the notice that differs.
//!
//! Store identity: cadence observations record `cadence = 'main'` and
//! `job_id = 'main:{project_id}'`. A row is measured against the newest such
//! observation for the same check and `verdict_platform` that classifies as
//! complete and ran on the **same executor**: two hosts of one platform differ
//! by provisioning and load as well as by tree, so a diff across hosts reports
//! the hosts as movement. Only when the executor has no complete row does the
//! platform's newest row stand in, and the message says so (see [`Comparison`]).
//! A lookup that fails, or that reads a full window of candidates without
//! finding one, leaves the history unknown, not absent: the message says the
//! comparison could not be read, keeps the row's measured verdict, and claims
//! no movement and no merge list.

mod corroboration;
mod incident_sources;
mod incidents;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use cairn_common::executor_protocol::{CheckSelectorSource, ExecutorSelector};
use regex::Regex;

use crate::config::project_settings::CheckCommand;
use crate::execution::check_parsers::{
    is_test_runner_command, plan_audit_status_gates, PLAN_AUDIT_EXIT_CODE,
};
use crate::execution::check_placement::CheckPlacementOverride;
use crate::execution::checks::{
    execute_adopted_check_item, execute_check_contract, host_ended_compile,
    load_checks_contract_at_commit_detailed, resolve_check_contract_at_commit,
    CheckContractSnapshot, CheckRunProducer, CheckRunTarget, CommitChecksContract,
    CommitChecksContractError, HostEnding, MAIN_CADENCE_STREAM_PREFIX, NO_VERDICT_SUPPRESSED,
};
use crate::fleet::PURPOSE_OWES_DEATH;
use crate::orchestrator::Orchestrator;
use crate::storage::{
    check_lane_adoptions::{self, Claim, Waiter},
    LocalDb, RowExt,
};

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
/// How long after its predecessor a row ends a silence, waking its thread even
/// when it says nothing new; and how often a standing hold re-announces itself
/// as still held.
const SILENCE_BOUND_MS: i64 = 60 * 60 * 1000;
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

/// A lane within a project: its check and platform.
type LaneKey = (String, String);

/// Which of a tip's lanes an arm runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LaneScope {
    /// Every lane the tip declares: a merge, a launch, the heartbeat.
    All,
    /// Only these lanes. A lane's own re-arm names only itself, so one
    /// platform's settled debt never re-runs another platform's lane.
    Only(BTreeSet<LaneKey>),
}

impl LaneScope {
    fn lane(lane: &CadenceLane) -> Self {
        LaneScope::Only(BTreeSet::from([lane.key()]))
    }

    /// The scope of one arm that absorbed both requests.
    fn union(self, other: LaneScope) -> LaneScope {
        match (self, other) {
            (LaneScope::Only(mut lanes), LaneScope::Only(more)) => {
                lanes.extend(more);
                LaneScope::Only(lanes)
            }
            _ => LaneScope::All,
        }
    }

    fn includes(&self, lane: &CadenceLane) -> bool {
        match self {
            LaneScope::All => true,
            LaneScope::Only(lanes) => lanes.contains(&lane.key()),
        }
    }
}

/// The tip an arm resolved, which its lanes attest. Plain data, so a busy
/// lane's queued successor carries the newest tip any arm resolved for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Tip {
    project_id: String,
    project_key: String,
    repo_path: String,
    default_branch: String,
    repository: PathBuf,
    commit: String,
}

impl Tip {
    fn context(&self, trigger: Trigger) -> PassContext<'_> {
        PassContext {
            project_id: &self.project_id,
            project_key: &self.project_key,
            repo_path: &self.repo_path,
            default_branch: &self.default_branch,
            repository: &self.repository,
            commit: &self.commit,
            trigger,
        }
    }
}

/// A project's arm: settling, refreshing origin, and resolving the tip and
/// its lanes. It ends as soon as the lanes are handed to their slots.
struct Arming {
    branch: String,
    armed_at_unix_ms: i64,
    queued: Option<(Trigger, LaneScope)>,
}

/// One lane's run in flight, and at most one successor.
struct LaneRun {
    adopted: Option<AdoptedLane>,
    commit: String,
    armed_at_unix_ms: i64,
    queued: Option<(Trigger, Tip)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AdoptedLane {
    pub(crate) request_id: String,
    pub(crate) machine: String,
    pub(crate) adopted_at_unix_ms: i64,
    pub(crate) awaiting_report: bool,
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

/// The cadence work armed for a project right now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InFlight {
    /// The arm still resolving the tip, if one is: its branch and when it was
    /// armed.
    pub(crate) resolving: Option<(String, i64)>,
    /// Each lane's run, in lane order.
    pub(crate) lanes: Vec<LaneInFlight>,
    /// The lanes whose passes the last restart cancelled and which the armed
    /// work re-arms, until each lane reports.
    pub(crate) rearming: Vec<Interruption>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LaneInFlight {
    pub(crate) check: String,
    pub(crate) platform: String,
    /// The tip the running attempt attests.
    pub(crate) commit: String,
    pub(crate) armed_at_unix_ms: i64,
    /// Whether a successor on a newer tip is owed when it ends.
    pub(crate) queued: bool,
    pub(crate) adopted: Option<AdoptedLane>,
}

/// What a lane last reported, kept so the project overview names every lane's
/// standing answer after its run has ended. A short lane (a typecheck) is in
/// flight for a minute or two after each merge and is otherwise only this.
/// Held in memory: the boot pass reports every lane again within minutes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LaneOutcome {
    pub(crate) check: String,
    pub(crate) platform: String,
    /// The tip the attempt attested, when the pass resolved one.
    pub(crate) tip: Option<String>,
    /// The verdict as a clause: `passed`, `failed (exit 2)`, `78 failed`,
    /// `did not complete (death)`.
    pub(crate) summary: String,
    /// The row that answers, when there is one: its citation and commit, which
    /// differs from the tip when an earlier row already covers the tip's inputs.
    pub(crate) row: Option<(String, String)>,
    pub(crate) at_unix_ms: i64,
}

/// A lane's announced non-verdict, held so that an unchanged repeat (a retry of
/// the same refusal, a re-resolution of the same suppression) is recorded
/// without waking the lane's thread again.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Standing {
    /// See [`observe`].
    identity: String,
    /// Whether it re-announces once per [`SILENCE_BOUND_MS`] while it stands. A
    /// suppression does not: it re-reports only when it lifts or its inputs
    /// change.
    reminds: bool,
    /// When it was first announced.
    since_ms: i64,
    /// When it last woke the thread.
    woke_ms: i64,
}

/// What an attempt was attempting, beyond its verdict: the inputs it would
/// have answered, once the contract resolved, and whether Cairn declined to
/// run them because the lane is suppressed.
#[derive(Clone, Debug)]
struct Attempted {
    inputs: Option<String>,
    suppressed: bool,
    event_id: String,
    /// The live subscriber remains owned through reporting and durable delivery.
    consumer_guard: Option<Arc<crate::fleet::CheckConsumerGuard>>,
}

impl Default for Attempted {
    fn default() -> Self {
        Self {
            inputs: None,
            suppressed: false,
            event_id: uuid::Uuid::new_v4().to_string(),
            consumer_guard: None,
        }
    }
}

/// An attempt as the announce step weighs it.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Observed {
    /// A complete row, and whether it is news: see [`row_is_news`].
    Row { news: bool },
    /// An attempt without a verdict, by identity: see [`observe`].
    Unobserved { identity: String, reminds: bool },
}

/// What the announce step does with an attempt's report. The row and the
/// overview line are recorded whichever it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Wake {
    /// Post the report: it carries a change or ends a silence.
    Announce,
    /// Post that the non-verdict first announced at `since_ms` still stands.
    StillHeld { since_ms: i64 },
    /// Post nothing: the attempt is identical to the one before it.
    Quiet,
}

/// One line of the project overview's Main cadence section.
pub(crate) fn render_outcome_line(outcome: &LaneOutcome, now_ms: i64) -> String {
    let tip = outcome.tip.as_deref().map_or_else(
        || "an unresolved tip".to_string(),
        |tip| format!("`{}`", short_commit(tip)),
    );
    let mut line = format!(
        "- {} on {}: {} at {tip}, {} ago",
        outcome.check,
        outcome.platform,
        outcome.summary,
        age(now_ms, outcome.at_unix_ms)
    );
    if let Some((reference, commit)) = &outcome.row {
        line.push_str(&format!(" — {reference}"));
        if outcome.tip.as_deref() != Some(commit.as_str()) {
            line.push_str(&format!(
                " (the row on `{}`, whose inputs this tip shares)",
                short_commit(commit)
            ));
        }
    }
    line.push_str(".\n");
    line
}

/// The cadence slots: per project, one arm in flight and at most one queued;
/// per lane, one run in flight and at most one queued.
#[derive(Default)]
pub(crate) struct CadenceSlots {
    arming: HashMap<String, Arming>,
    lanes: HashMap<(String, LaneKey), LaneRun>,
    /// Questions a refused lane is waiting on to settle, so a lane refused by
    /// every pass against one debt gets one waiter, not one per pass.
    debts: HashSet<crate::fleet::purpose::PurposeKey>,
    /// Per project, the tip whose checks contract could not be read: one
    /// outcome per tip, announced once and not read again until the tip moves.
    unreadable_contracts: HashMap<String, String>,
    /// Per project, the lanes the last restart cancelled that no pass has
    /// reported on since; see [`arm_at_boot`].
    interrupted: HashMap<String, Vec<Interruption>>,
    /// Each lane's last report, for the project overview.
    outcomes: HashMap<(String, LaneKey), LaneOutcome>,
    /// Each lane's standing non-verdict: the last it announced, until a
    /// complete row ends it. In memory like `outcomes`, so after a restart a
    /// lane's first non-verdict announces again.
    standing: HashMap<(String, LaneKey), Standing>,
    /// Extra filing notices share a project bound across its hosts and lanes.
    filing_notices: HashMap<String, i64>,
    /// Whether the startup default-advance catch-up has observed origin. Until
    /// it has, a pass must not fetch; see [`run_pass`].
    origin_observed: bool,
}

impl CadenceSlots {
    /// Reserve the old attempt before startup can offer this lane a new tip.
    fn adopt(&mut self, claim: &Claim, item_index: usize, boot_at: i64) -> bool {
        if cadence_claim_check(claim, item_index).is_err() {
            return false;
        }
        let key = (
            claim.project_id.clone(),
            (
                claim.check_names[item_index].clone(),
                claim.platform.clone(),
            ),
        );
        if let Some(run) = self.lanes.get(&key) {
            return run
                .adopted
                .as_ref()
                .is_some_and(|adopted| adopted.request_id == claim.request_id);
        }
        self.lanes.insert(
            key,
            LaneRun {
                adopted: Some(AdoptedLane {
                    request_id: claim.request_id.clone(),
                    machine: claim.machine.clone(),
                    adopted_at_unix_ms: boot_at,
                    awaiting_report: true,
                }),
                commit: claim.commit_sha.clone(),
                armed_at_unix_ms: claim.granted_at_ms,
                queued: None,
            },
        );
        true
    }

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

    /// Drop the restart citations of lanes the tip no longer declares: no lane
    /// will report on them, so they would otherwise be re-armed forever.
    fn forget_undeclared_interruptions(&mut self, project_id: &str, declared: &[CadenceLane]) {
        let Some(lanes) = self.interrupted.get_mut(project_id) else {
            return;
        };
        lanes.retain(|held| {
            declared
                .iter()
                .any(|lane| lane.check == held.check && lane.platform == held.platform)
        });
        if lanes.is_empty() {
            self.interrupted.remove(project_id);
        }
    }

    /// Record what a lane last reported, replacing its earlier report.
    fn settled(&mut self, project_id: &str, outcome: LaneOutcome) {
        let key = (outcome.check.clone(), outcome.platform.clone());
        self.outcomes.insert((project_id.to_string(), key), outcome);
    }

    /// Drop the last reports of lanes the tip no longer declares, so the
    /// overview never lists a lane that will not report again.
    fn forget_undeclared_outcomes(&mut self, project_id: &str, declared: &[CadenceLane]) {
        let kept = |(project, (check, platform)): &(String, LaneKey)| {
            project != project_id
                || declared
                    .iter()
                    .any(|lane| &lane.check == check && &lane.platform == platform)
        };
        self.outcomes.retain(|key, _| kept(key));
        self.standing.retain(|key, _| kept(key));
    }

    /// What the announce step does with `observed` on `lane`. A complete row
    /// wakes when it is news, and when it is the first row after a standing
    /// non-verdict, which it ends. A non-verdict wakes when it is not the one
    /// standing, and becomes the one standing; while it stands unchanged it
    /// wakes again only once per [`SILENCE_BOUND_MS`], and only if it reminds.
    fn wake(&mut self, project_id: &str, lane: &CadenceLane, observed: Observed, now: i64) -> Wake {
        let key = (project_id.to_string(), lane.key());
        match observed {
            Observed::Row { news } => {
                let ended = self.standing.remove(&key).is_some();
                if news || ended {
                    Wake::Announce
                } else {
                    Wake::Quiet
                }
            }
            Observed::Unobserved { identity, reminds } => match self.standing.get_mut(&key) {
                Some(standing) if standing.identity == identity => {
                    if standing.reminds && now - standing.woke_ms >= SILENCE_BOUND_MS {
                        standing.woke_ms = now;
                        Wake::StillHeld {
                            since_ms: standing.since_ms,
                        }
                    } else {
                        Wake::Quiet
                    }
                }
                _ => {
                    self.standing.insert(
                        key,
                        Standing {
                            identity,
                            reminds,
                            since_ms: now,
                            woke_ms: now,
                        },
                    );
                    Wake::Announce
                }
            },
        }
    }

    /// A complete row already answers `lane`'s inputs, so whatever non-verdict
    /// stood on it no longer does.
    fn answered(&mut self, project_id: &str, lane: &CadenceLane) {
        self.standing.remove(&(project_id.to_string(), lane.key()));
    }

    fn outcomes(&self, project_id: &str) -> Vec<LaneOutcome> {
        let mut outcomes: Vec<LaneOutcome> = self
            .outcomes
            .iter()
            .filter(|((project, _), _)| project == project_id)
            .map(|(_, outcome)| outcome.clone())
            .collect();
        outcomes.sort_by(|a, b| (&a.check, &a.platform).cmp(&(&b.check, &b.platform)));
        outcomes
    }

    /// Ask for an arm. Returns `true` when the caller must start one;
    /// otherwise the request was folded into the single queued successor,
    /// which runs every lane either request named.
    fn request(
        &mut self,
        project_id: &str,
        branch: &str,
        trigger: Trigger,
        scope: LaneScope,
        now: i64,
    ) -> bool {
        match self.arming.get_mut(project_id) {
            Some(arming) => {
                arming.queued = Some(match arming.queued.take() {
                    Some((queued, queued_scope)) => {
                        (queued.max(trigger), queued_scope.union(scope))
                    }
                    None => (trigger, scope),
                });
                false
            }
            None => {
                self.arming.insert(
                    project_id.to_string(),
                    Arming {
                        branch: branch.to_string(),
                        armed_at_unix_ms: now,
                        queued: None,
                    },
                );
                true
            }
        }
    }

    /// An arm ended. Returns the queued successor's trigger and scope, keeping
    /// the slot held for it, or releases the slot when nothing queued.
    fn finish(&mut self, project_id: &str, now: i64) -> Option<(Trigger, LaneScope)> {
        let arming = self.arming.get_mut(project_id)?;
        match arming.queued.take() {
            Some(next) => {
                arming.armed_at_unix_ms = now;
                Some(next)
            }
            None => {
                self.arming.remove(project_id);
                None
            }
        }
    }

    /// Offer `tip` to `lane`. Returns `true` when the caller must start the
    /// lane's run; otherwise the lane is busy and `tip` becomes its one queued
    /// successor, replacing an older queued tip. Another lane's run never
    /// enters into it.
    fn offer(&mut self, lane: &CadenceLane, tip: &Tip, trigger: Trigger, now: i64) -> bool {
        let key = (tip.project_id.clone(), lane.key());
        match self.lanes.get_mut(&key) {
            Some(run) => {
                let trigger = run
                    .queued
                    .as_ref()
                    .map_or(trigger, |(queued, _)| (*queued).max(trigger));
                run.queued = Some((trigger, tip.clone()));
                false
            }
            None => {
                self.lanes.insert(
                    key,
                    LaneRun {
                        adopted: None,
                        commit: tip.commit.clone(),
                        armed_at_unix_ms: now,
                        queued: None,
                    },
                );
                true
            }
        }
    }

    /// A lane's run ended. Returns its queued successor, keeping the slot held
    /// for it, or releases the slot when nothing queued.
    fn lane_finished(
        &mut self,
        project_id: &str,
        lane: &CadenceLane,
        now: i64,
    ) -> Option<(Trigger, Tip)> {
        let key = (project_id.to_string(), lane.key());
        let run = self.lanes.get_mut(&key)?;
        match run.queued.take() {
            Some((trigger, tip)) => {
                run.adopted = None;
                run.commit = tip.commit.clone();
                run.armed_at_unix_ms = now;
                Some((trigger, tip))
            }
            None => {
                self.lanes.remove(&key);
                None
            }
        }
    }

    fn in_flight(&self, project_id: &str) -> Option<InFlight> {
        let resolving = self
            .arming
            .get(project_id)
            .map(|arming| (arming.branch.clone(), arming.armed_at_unix_ms));
        let mut lanes: Vec<LaneInFlight> = self
            .lanes
            .iter()
            .filter(|((project, _), _)| project == project_id)
            .map(|((_, (check, platform)), run)| LaneInFlight {
                check: check.clone(),
                platform: platform.clone(),
                commit: run.commit.clone(),
                armed_at_unix_ms: run.armed_at_unix_ms,
                queued: run.queued.is_some(),
                adopted: run.adopted.clone(),
            })
            .collect();
        lanes.sort_by(|a, b| (&a.check, &a.platform).cmp(&(&b.check, &b.platform)));
        (resolving.is_some() || !lanes.is_empty()).then(|| InFlight {
            resolving,
            lanes,
            rearming: self
                .interrupted
                .get(project_id)
                .cloned()
                .unwrap_or_default(),
        })
    }
}

/// The cadence work armed for `project_id` right now, if any: an arm resolving
/// the tip, each lane's run, and the restart-cancelled passes they re-arm.
/// While a lane runs the store holds no row of its for the new tip, so this is
/// what tells "pending" from "never ran" (CAIRN-3823).
pub(crate) fn in_flight(orch: &Orchestrator, project_id: &str) -> Option<InFlight> {
    let mut flight = orch.main_cadence.lock().unwrap().in_flight(project_id)?;
    for lane in &mut flight.lanes {
        if let Some(adopted) = &mut lane.adopted {
            adopted.awaiting_report =
                orch.fleet
                    .purpose_owes(&crate::fleet::purpose::PurposeKey::main_cadence(
                        project_id,
                        &lane.check,
                        &lane.platform,
                    ));
        }
    }
    Some(flight)
}

/// The same debt clause appears on the project overview and the machine page.
pub(crate) fn render_adopted_lane(lane: &LaneInFlight) -> Option<String> {
    let adopted = lane.adopted.as_ref()?;
    let status = if adopted.awaiting_report {
        format!(
            "awaiting adoption on {} since {}; the lane re-arms when the machine reports or is removed; the operator may authorize the overlap (authorize_overlap)",
            adopted.machine, stamp(lane.armed_at_unix_ms)
        )
    } else {
        format!(
            "running on {} since {}",
            adopted.machine,
            stamp(lane.armed_at_unix_ms)
        )
    };
    Some(format!(
        "- {} on {} at `{}`: {status}; adopted from the previous runner at {} (request {}). Live output streaming is not reattached; the output tail arrives with the result.{}\n",
        lane.check, lane.platform, short_commit(&lane.commit), stamp(adopted.adopted_at_unix_ms), adopted.request_id,
        if lane.queued { " One successor is queued for the current tip." } else { "" },
    ))
}

pub(crate) fn render_machine_adoptions(orch: &Orchestrator, machine: &str) -> String {
    let projects: BTreeSet<_> = orch
        .main_cadence
        .lock()
        .unwrap()
        .lanes
        .keys()
        .map(|(project, _)| project.clone())
        .collect();
    let mut lines = String::new();
    for project in projects {
        if let Some(flight) = in_flight(orch, &project) {
            for lane in flight.lanes {
                if lane
                    .adopted
                    .as_ref()
                    .is_some_and(|adopted| adopted.machine == machine)
                {
                    if let Some(line) = render_adopted_lane(&lane) {
                        lines.push_str(&line);
                    }
                }
            }
        }
    }
    if lines.is_empty() {
        lines
    } else {
        format!("## Adopted main cadence lanes\n\n{lines}\n")
    }
}

/// Each lane's last report for `project_id` since this runner started, in lane
/// order.
pub(crate) fn last_outcomes(orch: &Orchestrator, project_id: &str) -> Vec<LaneOutcome> {
    orch.main_cadence.lock().unwrap().outcomes(project_id)
}

/// Arm every lane for the project's default branch. Calling this on every
/// observed advance is intended: requests coalesce into the running arm's one
/// queued successor, and each lane folds the tip into its own.
pub(crate) fn spawn(
    orch: &Orchestrator,
    project_id: String,
    repo_path: String,
    default_branch: String,
    trigger: Trigger,
) {
    arm(
        orch,
        project_id,
        repo_path,
        default_branch,
        trigger,
        LaneScope::All,
    );
}

fn arm(
    orch: &Orchestrator,
    project_id: String,
    repo_path: String,
    default_branch: String,
    trigger: Trigger,
    scope: LaneScope,
) {
    let start = orch.main_cadence.lock().unwrap().request(
        &project_id,
        &default_branch,
        trigger,
        scope.clone(),
        now_ms(),
    );
    if !start {
        return;
    }
    let orch = orch.clone();
    tokio::spawn(async move {
        let (mut trigger, mut scope) = (trigger, scope);
        loop {
            let settle = if trigger == Trigger::Startup {
                BOOT_SETTLE_WINDOW
            } else {
                SETTLE_WINDOW
            };
            tokio::time::sleep(settle).await;
            // Supervised, so a panicking arm still releases the slot and is
            // announced rather than holding the project's cadence forever.
            let pass = tokio::spawn({
                let orch = orch.clone();
                let project_id = project_id.clone();
                let repo_path = repo_path.clone();
                let default_branch = default_branch.clone();
                let scope = scope.clone();
                async move {
                    run_pass(
                        &orch,
                        &project_id,
                        &repo_path,
                        &default_branch,
                        trigger,
                        &scope,
                    )
                    .await
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
                announce_pass_failure(&orch, &project_id, &repo_path, &scope, &cause).await;
            }
            let next = orch
                .main_cadence
                .lock()
                .unwrap()
                .finish(&project_id, now_ms());
            match next {
                Some((next, next_scope)) => (trigger, scope) = (next, next_scope),
                None => break,
            }
        }
    });
}

/// Run `lane` on `tip`, then on each successor queued for it while it ran.
fn start_lane(orch: &Orchestrator, tip: Tip, lane: CadenceLane, trigger: Trigger) {
    let orch = orch.clone();
    tokio::spawn(async move {
        let (mut tip, mut trigger) = (tip, trigger);
        loop {
            // Supervised, so a panicking run still releases the lane's slot
            // and is announced rather than holding the lane forever.
            let run = tokio::spawn({
                let orch = orch.clone();
                let tip = tip.clone();
                let lane = lane.clone();
                async move { run_lane(&orch, &tip, &lane, trigger).await }
            })
            .await;
            if let Err(join) = run {
                let cause = format!("the cadence lane panicked: {join}");
                log::error!(
                    "main cadence {} on {} for project {}: {cause}",
                    lane.check,
                    lane.platform,
                    tip.project_id
                );
                announce_lane_death(&orch, &tip, &lane, cause).await;
            }
            if orch.fleet.runner_stopping_since().is_some() {
                break;
            }
            let next =
                orch.main_cadence
                    .lock()
                    .unwrap()
                    .lane_finished(&tip.project_id, &lane, now_ms());
            match next {
                Some((next, next_tip)) => (trigger, tip) = (next, next_tip),
                None => break,
            }
        }
    });
}

/// Arm every local project's cadence: the launch-time repair edge, and each
/// heartbeat tick.
pub(crate) async fn spawn_all_current(orch: &Orchestrator, trigger: Trigger) {
    for db in orch.db.all_dbs().await {
        incident_sources::retry(orch, &db).await;
        retry_incident_intents(orch, &db).await;
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

fn cadence_claim_check(claim: &Claim, index: usize) -> Result<&str, String> {
    if claim.check_names.len() != claim.input_hashes.len() || index >= claim.check_names.len() {
        return Err(format!(
            "cadence adoption {} has invalid item {index}: {} checks and {} hashes",
            claim.request_id,
            claim.check_names.len(),
            claim.input_hashes.len()
        ));
    }
    Ok(&claim.check_names[index])
}

fn verify_adoption_inputs(claim: &Claim, index: usize, rebuilt: &str) -> Result<(), String> {
    cadence_claim_check(claim, index)?;
    if claim.input_hashes[index] == rebuilt {
        Ok(())
    } else {
        Err(format!(
            "cannot adopt request {} at {}: granted input hash {} differs from rebuilt input hash {rebuilt}",
            claim.request_id, claim.commit_sha, claim.input_hashes[index]
        ))
    }
}

/// The decision must commit before any Cancel or held-result acknowledgement.
/// Store failures leave the debt intact and retry; they are not evidence of loss.
async fn lose_adoption(orch: &Orchestrator, claim: &Claim, checks: &[String], cause: &str) {
    loop {
        match try_lose_adoption(orch, claim, checks, cause).await {
            Ok(()) => return,
            Err(error) => {
                log::error!("main cadence adoption {} cannot durably record loss: {error}; retaining the debt and retrying", claim.request_id);
                tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
            }
        }
    }
}

fn adoption_interruptions(claim: &Claim, checks: &[String]) -> Vec<Interruption> {
    checks
        .iter()
        .map(|check| Interruption {
            check: check.clone(),
            platform: claim.platform.clone(),
            commit: claim.commit_sha.clone(),
            // The durable timestamp is stable across abrupt-stop resends.
            stopped_at_unix_ms: claim.stopped_at_ms.unwrap_or(claim.updated_ms),
        })
        .collect()
}

/// Persist cadence repair before fleet loss makes the claim terminal or releases
/// its binding. Boot must find the repair even if its open-claim scan runs later.
pub(crate) async fn record_adoption_interruptions(
    db: &LocalDb,
    claim: &Claim,
    checks: &[String],
) -> Result<(), String> {
    for interruption in adoption_interruptions(claim, checks) {
        record_interruption(db, &claim.project_id, &interruption).await?;
    }
    Ok(())
}

/// One settlement attempt, also used by the serial unbound-result dispatcher.
/// A store failure must yield to its retry queue, not block unrelated results.
pub(crate) async fn try_lose_adoption(
    orch: &Orchestrator,
    claim: &Claim,
    checks: &[String],
    cause: &str,
) -> Result<(), String> {
    let current =
        check_lane_adoptions::lookup(&orch.db.local, &claim.request_id, &claim.attempt_id)
            .await
            .map_err(|error| error.to_string())?;
    if current
        .as_ref()
        .is_some_and(|current| current.state == "recorded")
    {
        return Ok(());
    }
    let already_lost = current
        .as_ref()
        .is_some_and(|current| current.state == "lost");
    record_adoption_interruptions(&orch.db.local, claim, checks).await?;
    if !already_lost {
        check_lane_adoptions::settle_failure(
            &orch.db.local,
            &claim.request_id,
            &claim.attempt_id,
            "lost",
            cause,
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    for interruption in adoption_interruptions(claim, checks) {
        orch.main_cadence
            .lock()
            .unwrap()
            .interrupted(&claim.project_id, interruption);
    }
    if !already_lost {
        orch.fleet
            .cancel_adopted(&claim.request_id, &claim.attempt_id, &claim.executor_id);
    }
    let cause = current
        .as_ref()
        .filter(|current| current.state == "lost")
        .and_then(|current| current.evidence.as_deref())
        .unwrap_or(cause);
    log::warn!(
        "main cadence adoption {} on {} lost: {cause}",
        claim.request_id,
        claim.machine
    );
    Ok(())
}

async fn adoption_tip(
    orch: &Orchestrator,
    claim: &Claim,
    index: usize,
) -> Result<(Tip, CadenceLane), String> {
    let check = cadence_claim_check(claim, index)?;
    let db = crate::execution::routing::owning_db_for_project(&orch.db, &claim.project_id)
        .await
        .map_err(|e| e.to_string())?;
    let (repo_path, default_branch) = db
        .query_all(
            "SELECT repo_path, default_branch FROM projects WHERE id = ?1",
            (claim.project_id.clone(),),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .next()
        .ok_or_else(|| format!("adoption project {} no longer exists", claim.project_id))?;
    let declared = tip_lanes(
        &orch.main_cadence,
        &claim.project_id,
        &claim.commit_sha,
        || load_checks_contract_at_commit_detailed(Path::new(&repo_path), &claim.commit_sha),
    )
    .await?;
    let lane = declared
        .into_iter()
        .find(|lane| lane.check == check && lane.platform == claim.platform)
        .ok_or_else(|| {
            format!(
                "{} on {} is not declared at {}",
                check, claim.platform, claim.commit_sha
            )
        })?;
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let repository =
        crate::jj::coordinate_repository(&jj, &orch.config_dir, Path::new(&repo_path))?;
    Ok((
        Tip {
            project_id: claim.project_id.clone(),
            project_key: project_key(&db, &claim.project_id).await?,
            repo_path,
            default_branch,
            repository,
            commit: claim.commit_sha.clone(),
        },
        lane,
    ))
}

fn cadence_waiter_indices(claim: &Claim, waiters: &[Waiter]) -> Result<Vec<usize>, String> {
    let mut indices = BTreeSet::new();
    for waiter in waiters.iter().filter(|waiter| waiter.is_cadence_adopter()) {
        let check = cadence_claim_check(claim, waiter.item_index)?;
        if waiter.request_id != claim.request_id
            || waiter.attempt_id != claim.attempt_id
            || waiter.check_name != check
        {
            return Err(format!(
                "cadence waiter for {} does not match physical request {} item {}",
                waiter.check_name, claim.request_id, waiter.item_index
            ));
        }
        indices.insert(waiter.item_index);
    }
    Ok(indices.into_iter().collect())
}

fn reserve_adoption_slots(
    orch: &Orchestrator,
    claim: &Claim,
    indices: &[usize],
) -> Result<(), String> {
    let mut slots = orch.main_cadence.lock().unwrap();
    for index in indices {
        if !slots.adopt(claim, *index, now_ms()) {
            return Err(format!(
                "another physical request owns {} on {}",
                claim.check_names[*index], claim.platform
            ));
        }
    }
    Ok(())
}

fn finish_adoption_slots(orch: &Orchestrator, claim: &Claim, indices: &[usize]) {
    if orch.fleet.runner_stopping_since().is_some() {
        return;
    }
    for index in indices {
        let key = (
            claim.project_id.clone(),
            (claim.check_names[*index].clone(), claim.platform.clone()),
        );
        let queued = {
            let mut slots = orch.main_cadence.lock().unwrap();
            if !slots
                .lanes
                .get(&key)
                .and_then(|run| run.adopted.as_ref())
                .is_some_and(|adopted| adopted.request_id == claim.request_id)
            {
                continue;
            }
            slots.lanes.remove(&key).and_then(|run| run.queued)
        };
        if let Some((trigger, tip)) = queued {
            arm(
                orch,
                tip.project_id,
                tip.repo_path,
                tip.default_branch,
                trigger,
                LaneScope::Only(BTreeSet::from([key.1])),
            );
        }
    }
}

/// One physical recorder per item, then fan out the resulting observations to
/// cadence consumers. Manual/review siblings retain their original sealed plans.
pub(crate) async fn adopt_check_batch(orch: &Orchestrator, claim: &Claim, waiters: Vec<Waiter>) {
    let indices = match cadence_waiter_indices(claim, &waiters) {
        Ok(indices) => indices,
        Err(error) => {
            let checks: Vec<_> = waiters
                .iter()
                .filter(|w| w.consumer == "main_cadence")
                .map(|w| w.check_name.clone())
                .collect();
            lose_adoption(orch, claim, &checks, &error).await;
            return;
        }
    };
    if indices.is_empty() {
        return;
    }
    let checks: Vec<_> = indices
        .iter()
        .map(|i| claim.check_names[*i].clone())
        .collect();
    if let Err(error) = reserve_adoption_slots(orch, claim, &indices) {
        lose_adoption(orch, claim, &checks, &error).await;
        finish_adoption_slots(orch, claim, &indices);
        return;
    }
    let work = tokio::spawn({
        let orch = orch.clone();
        let claim = claim.clone();
        let indices = indices.clone();
        async move { record_adopted_batch(&orch, &claim, &indices).await }
    })
    .await;
    let error = match work {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error),
        Err(error) => Some(format!("cadence batch adopter panicked: {error}")),
    };
    if let Some(error) = error {
        lose_adoption(orch, claim, &checks, &error).await;
    }
    finish_adoption_slots(orch, claim, &indices);
}

async fn current_cadence_item_waiters(
    orch: &Orchestrator,
    claim: &Claim,
    index: usize,
) -> Vec<Waiter> {
    loop {
        match check_lane_adoptions::waiters(&orch.db.local, &claim.request_id, &claim.attempt_id)
            .await
        {
            Ok(waiters) => {
                return waiters
                    .into_iter()
                    .filter(|waiter| waiter.item_index == index && waiter.is_cadence_adopter())
                    .collect()
            }
            Err(error) => {
                log::error!("cannot read current cadence consumers for {}: {error}; retaining the result and retrying", claim.request_id);
                tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
            }
        }
    }
}

async fn record_adopted_batch(
    orch: &Orchestrator,
    claim: &Claim,
    indices: &[usize],
) -> Result<(), String> {
    let db = crate::execution::routing::owning_db_for_project(&orch.db, &claim.project_id)
        .await
        .map_err(|e| e.to_string())?;
    let mut cadence = Vec::new();
    for index in indices {
        let (tip, lane) = adoption_tip(orch, claim, *index).await?;
        let contract =
            resolve_lane_contract(orch, db.clone(), &tip.context(Trigger::Startup), &lane).await?;
        verify_adoption_inputs(claim, *index, &contract.context.input_hash)?;
        cadence.push((*index, tip, lane));
    }
    log::info!("main cadence adopting physical request {} on {} at original commit {} ({} suites, {} cadence lanes)",
        claim.request_id, claim.machine, claim.commit_sha, claim.check_names.len(), cadence.len());
    let mut observations = Vec::new();
    for index in 0..claim.check_names.len() {
        let result = execute_adopted_check_item(orch, db.clone(), claim, index).await?;
        if let Some(left) = result.left_running {
            for (_, tip, lane) in &cadence {
                report_lane(
                    orch,
                    &db,
                    &tip.project_id,
                    &tip.project_key,
                    Some(&tip.repository),
                    lane,
                    Some(&tip.commit),
                    CadenceVerdict::AwaitingAdoption {
                        machine: left.machine.clone(),
                        request_id: left.request_id.clone(),
                        stopped_at_unix_ms: i64::try_from(left.stopped_at_ms).unwrap_or(i64::MAX),
                    },
                    &Attempted::default(),
                )
                .await;
            }
            return Ok(());
        }
        let id = result.observation_id.ok_or_else(|| {
            format!(
                "adopted request {} suite {} recorded no observation: {:?}",
                claim.request_id, claim.check_names[index], result.no_verdict
            )
        })?;
        observations.push(id);
    }
    let names = |executor: &str| orch.fleet.executor_name_for_text(executor);
    'cadence: for (index, tip, lane) in cadence {
        let row = loop {
            if current_cadence_item_waiters(orch, claim, index)
                .await
                .is_empty()
            {
                continue 'cadence;
            }
            match load_row(&db, &tip.project_key, &observations[index], &names).await {
                Ok(row) => break row,
                Err(error) => {
                    log::error!("adopted observation {} could not be read: {error}; retaining the debt and retrying before delivery", observations[index]);
                    tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
                }
            }
        };
        let verdict = lane.verdict.classify(row);
        let attempted = Attempted {
            inputs: Some(claim.input_hashes[index].clone()),
            ..Default::default()
        };
        loop {
            if current_cadence_item_waiters(orch, claim, index)
                .await
                .is_empty()
            {
                continue 'cadence;
            }
            match try_report_lane(
                orch,
                &db,
                &tip.project_id,
                &tip.project_key,
                Some(&tip.repository),
                &lane,
                Some(&tip.commit),
                verdict.clone(),
                &attempted,
            )
            .await
            {
                Ok(()) => break,
                Err(error) => {
                    log::error!("adopted cadence report failed: {error}; retaining the waiter and retrying before acknowledgement");
                    tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
                }
            }
        }
        let mut confirmed = HashSet::new();
        loop {
            let pending = current_cadence_item_waiters(orch, claim, index)
                .await
                .into_iter()
                .find(|waiter| !confirmed.contains(&waiter.consumer_ref));
            let Some(waiter) = pending else { break };
            // Re-read after every failed delivery: a concurrent withdrawal is
            // terminal evidence, not a reason to retry an obsolete receipt.
            match orch
                .fleet
                .deliver_check_waiter(&waiter, &observations[index])
                .await
            {
                Ok(()) => {
                    confirmed.insert(waiter.consumer_ref);
                }
                Err(error) => {
                    // The receipt may already be delivered while the physical
                    // claim write failed. Only successful API completion proves
                    // its finalization edge ran; retry delivered receipts too.
                    log::error!("could not durably deliver adopted cadence waiter {}: {error}; retaining the debt and retrying", waiter.consumer_ref);
                    #[cfg(test)]
                    crate::execution::checks::notify_adoption_retry(&observations[index], &error);
                    tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
                }
            }
        }
    }
    Ok(())
}

async fn adopt_at_boot(orch: &Orchestrator) {
    orch.fleet.await_restart_adoptions_loaded().await;
    let claims = match check_lane_adoptions::open(&orch.db.local).await {
        Ok(claims) => claims,
        Err(error) => {
            log::error!("main cadence cannot load check_lane_adoptions at boot: {error}; remote lanes cannot be adopted");
            return;
        }
    };
    if let Err(error) = check_lane_adoptions::prune(&orch.db.local).await {
        log::warn!("check_lane_adoptions terminal-row pruning failed: {error}");
    }
    let mut batches = Vec::new();
    for claim in claims {
        let waiters = loop {
            match check_lane_adoptions::waiters(
                &orch.db.local,
                &claim.request_id,
                &claim.attempt_id,
            )
            .await
            {
                Ok(waiters) => break waiters,
                Err(error) => {
                    log::error!("main cadence cannot read check adoption waiters for {}: {error}; retaining the debt and retrying", claim.request_id);
                    tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
                }
            }
        };
        match cadence_waiter_indices(&claim, &waiters) {
            Ok(indices) if !indices.is_empty() => {
                let identity = (claim.request_id.clone(), claim.attempt_id.clone());
                if !orch.fleet.is_check_adoption_reloaded(&identity) {
                    let checks: Vec<_> = indices
                        .iter()
                        .map(|index| claim.check_names[*index].clone())
                        .collect();
                    // A recovered store read cannot undo the fleet's boot Cancel
                    // decision or manufacture ownership of that physical grant.
                    lose_adoption(
                        orch,
                        &claim,
                        &checks,
                        "boot preload failed; the fleet cancelled it",
                    )
                    .await;
                    continue;
                }
                if let Err(error) = reserve_adoption_slots(orch, &claim, &indices) {
                    log::error!("main cadence cannot reserve adopted lane: {error}");
                }
                batches.push((claim, waiters));
            }
            Ok(_) => {}
            Err(error) => {
                let checks: Vec<_> = waiters
                    .iter()
                    .filter(|w| w.consumer == "main_cadence")
                    .map(|w| w.check_name.clone())
                    .collect();
                lose_adoption(orch, &claim, &checks, &error).await;
            }
        }
    }
    // Reserve every old lane before any batch or startup arm can race it.
    for (claim, waiters) in batches {
        let orch = orch.clone();
        tokio::spawn(async move {
            adopt_check_batch(&orch, &claim, waiters).await;
        });
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
    adopt_at_boot(orch).await;
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

/// What a lane's verdict is made of, decided by the whole-suite command it
/// runs: the same test-runner rule the observation store applies when it
/// records whether a row is complete.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LaneVerdict {
    /// A test runner. The per-test enumeration is the verdict, and a run that
    /// reports no test is not one (rulings 695/706).
    PerTest,
    /// Any other command: a typecheck, a build, a compile. Its exit is the
    /// verdict, once its result vouches for having measured something.
    Exit,
}

impl LaneVerdict {
    fn of(check: &CheckCommand) -> Self {
        let command = check.full_command.as_deref().unwrap_or(&check.command);
        if is_test_runner_command(command) {
            LaneVerdict::PerTest
        } else {
            LaneVerdict::Exit
        }
    }

    fn classify(self, row: CadenceRow) -> CadenceVerdict {
        match self {
            LaneVerdict::PerTest => classify_row(row),
            LaneVerdict::Exit => classify_exit_row(row),
        }
    }
}

/// One check on one platform.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CadenceLane {
    pub(crate) check: String,
    pub(crate) platform: String,
    pub(crate) notify: String,
    pub(crate) verdict: LaneVerdict,
}

impl CadenceLane {
    fn key(&self) -> LaneKey {
        (self.check.clone(), self.platform.clone())
    }
}

/// Every lane a checks contract declares, in a stable order.
fn cadence_lanes(checks: &HashMap<String, CheckCommand>) -> Vec<CadenceLane> {
    let mut lanes: Vec<CadenceLane> = checks
        .iter()
        .filter_map(|(name, check)| {
            check
                .main_cadence
                .as_ref()
                .map(|cadence| (name, LaneVerdict::of(check), cadence))
        })
        .flat_map(|(name, verdict, cadence)| {
            cadence.platforms.iter().map(move |platform| CadenceLane {
                check: name.clone(),
                platform: platform.trim().to_ascii_lowercase(),
                notify: cadence.notify.clone(),
                verdict,
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
    scope: &LaneScope,
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
    // Refresh above is the guarded writer. Capturing an existing coordinate
    // below must neither create a missing store nor reconcile a split log.
    let repository = crate::jj::project_store_dir(&orch.config_dir, &repo_root);
    let commit = cairn_vcs::resolve_coordinate(&repository, default_branch)
        .await
        .map_err(|error| format!("default head '{default_branch}' is unresolvable: {error}"))?;
    let declared = tip_lanes(&orch.main_cadence, project_id, &commit, || {
        load_checks_contract_at_commit_detailed(&repo_root, &commit)
    })
    .await?;
    if declared.is_empty() {
        return Ok(());
    }
    {
        let mut slots = orch.main_cadence.lock().unwrap();
        slots.forget_undeclared_interruptions(project_id, &declared);
        slots.forget_undeclared_outcomes(project_id, &declared);
    }
    let lanes: Vec<CadenceLane> = declared
        .into_iter()
        .filter(|lane| scope.includes(lane))
        .collect();
    if lanes.is_empty() {
        return Ok(());
    }
    let tip = Tip {
        project_id: project_id.to_string(),
        project_key: project_key(&db, project_id).await?,
        repo_path: repo_path.to_string(),
        default_branch: default_branch.to_string(),
        repository,
        commit,
    };
    // Each lane runs on its own slot, so the arm ends here: a lane still
    // running from an earlier tip takes this one as its queued successor and
    // holds back no other lane.
    let now = now_ms();
    for lane in lanes {
        let start = orch
            .main_cadence
            .lock()
            .unwrap()
            .offer(&lane, &tip, trigger, now);
        if start {
            start_lane(orch, tip.clone(), lane, trigger);
        }
    }
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
    let db = crate::execution::routing::owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| error.to_string())?;
    let reservation = orch
        .reserve_branches(
            &store,
            &[default_branch],
            "main cadence canonical refresh",
            crate::mcp::vcs::STORE_LOCK_TIMEOUT,
        )
        .await?;
    let guard = orch
        .acquire_jj_store_lock(
            &store,
            crate::orchestrator::StoreLockClass::Background,
            "main cadence canonical refresh",
        )
        .await;
    let _phase = guard.phase("main cadence canonical refresh");
    orch.refuse_foreign_reservations(&guard, Some(&reservation))?;
    let preserved = crate::orchestrator::branch_recovery::preserved_import_bookmarks(
        orch,
        &store,
        &[default_branch],
        &format!("Refreshing `{default_branch}` from origin for the main cadence"),
    )
    .await?;
    let jj = jj.preserving_import_bookmarks(preserved);
    crate::jj::ensure_store_initialized(&jj, &store, repo_root)
        .map_err(|error| format!("store init failed: {error}"))?;
    // The reconcile imports (which may move any bookmark) and then moves the
    // default: admit its writer, and bracket the import for every other.
    guard
        .branch_write_permit(
            &db,
            &reservation,
            project_id,
            default_branch,
            crate::orchestrator::BatchAdmission::None,
        )
        .await?;
    let (reconcile_store, reconcile_branch) = (store.clone(), default_branch.to_string());
    orch.bracket_store_mutation(guard, Some(&reservation), db, project_id, move || {
        crate::jj::reconcile_tracked_bookmark(&jj, &reconcile_store, &reconcile_branch)
            .map(|_| ())
            .map_err(|error| format!("bookmark reconcile failed: {error}"))
    })
    .await?;
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
    Verdict(CadenceVerdict, Attempted),
}

async fn run_lane(orch: &Orchestrator, tip: &Tip, lane: &CadenceLane, trigger: Trigger) {
    let db = match crate::execution::routing::owning_db_for_project(&orch.db, &tip.project_id).await
    {
        Ok(db) => db,
        Err(error) => {
            log::error!(
                "main cadence {} on {} for project {}: the project's database is unreachable, so this lane can neither run nor report: {error}",
                lane.check,
                lane.platform,
                tip.project_id
            );
            return;
        }
    };
    let context = &tip.context(trigger);
    match attempt_lane(orch, db.clone(), context, lane).await {
        LaneAttempt::Covered(row) => {
            // The observation may have committed immediately before a crash or
            // a failed issue write. Coverage must not skip that filing retry.
            replay_covered_incidents(orch, &db, context, lane, &row).await;
            let rearmed = {
                let mut slots = orch.main_cadence.lock().unwrap();
                slots.settled(
                    context.project_id,
                    LaneOutcome {
                        check: lane.check.clone(),
                        platform: lane.platform.clone(),
                        tip: Some(context.commit.to_string()),
                        summary: headline_clause(&row, lane.verdict),
                        row: Some((row.reference.clone(), row.commit.clone())),
                        at_unix_ms: now_ms(),
                    },
                );
                slots.answered(context.project_id, lane);
                slots.take_interruption(context.project_id, lane)
            };
            if let Some(interruption) = rearmed {
                forget_interruption(&orch.db.local, context.project_id, &interruption).await;
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
        LaneAttempt::Verdict(verdict, attempted) => {
            let verdict = cancelled_by_restart(verdict, orch.fleet.runner_stopping_since());
            if matches!(verdict, CadenceVerdict::Death { .. }) {
                rearm_on_debt_settlement(orch, context, lane);
            }
            report_live_lane(orch, &db, context, lane, verdict, &attempted).await;
            drop(attempted.consumer_guard);
        }
    }
}

async fn main_cadence_delivery_requests(
    db: &LocalDb,
    project: &str,
    lane: &CadenceLane,
    observation: &str,
) -> Result<Vec<(String, String)>, String> {
    let purpose =
        crate::fleet::purpose::PurposeKey::main_cadence(project, &lane.check, &lane.platform)
            .to_string();
    db.query_all(
        "SELECT w.request_id,w.attempt_id FROM check_lane_adoption_waiters w
           JOIN check_lane_adoptions c ON c.request_id=w.request_id AND c.attempt_id=w.attempt_id
          WHERE c.project_id=?1 AND c.platform=?2 AND w.check_name=?3
            AND w.consumer='main_cadence' AND w.consumer_ref=?4 AND w.observation_id=?5
            AND w.state IN ('recorded','delivered')",
        (
            project.to_string(),
            lane.platform.clone(),
            lane.check.clone(),
            purpose,
            observation.to_string(),
        ),
        |row| Ok((row.text(0)?, row.text(1)?)),
    )
    .await
    .map_err(|error| error.to_string())
}

async fn report_live_lane(
    orch: &Orchestrator,
    db: &LocalDb,
    context: &PassContext<'_>,
    lane: &CadenceLane,
    verdict: CadenceVerdict,
    attempted: &Attempted,
) {
    let observation = verdict.row().map(|row| row.id.clone());
    let Some(observation) = observation else {
        report_lane(
            orch,
            db,
            context.project_id,
            context.project_key,
            Some(context.repository),
            lane,
            Some(context.commit),
            verdict,
            attempted,
        )
        .await;
        return;
    };
    loop {
        match try_report_lane(
            orch,
            db,
            context.project_id,
            context.project_key,
            Some(context.repository),
            lane,
            Some(context.commit),
            verdict.clone(),
            attempted,
        )
        .await
        {
            Ok(()) => break,
            Err(error) => {
                log::error!(
                    "live cadence report failed: {error}; retaining its consumer until delivery"
                );
                tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
            }
        }
    }
    let purpose = crate::fleet::purpose::PurposeKey::main_cadence(
        context.project_id,
        &lane.check,
        &lane.platform,
    )
    .to_string();
    loop {
        let delivered = async {
            if let Some(guard) = &attempted.consumer_guard {
                return guard.deliver(&observation).await;
            }
            for identity in main_cadence_delivery_requests(
                &orch.db.local,
                context.project_id,
                lane,
                &observation,
            )
            .await?
            {
                orch.fleet
                    .deliver_check_consumer(
                        &identity,
                        &lane.check,
                        "main_cadence",
                        &purpose,
                        &observation,
                    )
                    .await?;
            }
            Ok::<_, String>(())
        }
        .await;
        match delivered {
            Ok(()) => break,
            Err(error) => {
                log::error!("could not confirm live cadence delivery: {error}; retaining its consumer and retrying");
                tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
            }
        }
    }
}

/// If this lane died because its question still owes a displaced answer's
/// death, arm this lane again the moment that debt settles. Only this lane:
/// another platform's lane is not re-run because this one's debt settled.
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
    let scope = LaneScope::lane(lane);
    tokio::spawn(async move {
        orch.fleet.purpose_debt_cleared(&key).await;
        orch.main_cadence.lock().unwrap().debt_settled(&key);
        log::info!(
            "purpose {key} owes nothing now; re-arming its main cadence lane for project {project_id}"
        );
        arm(
            &orch,
            project_id,
            repo_path,
            default_branch,
            Trigger::DebtSettled,
            scope,
        );
    });
}

async fn resolve_lane_contract(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    context: &PassContext<'_>,
    lane: &CadenceLane,
) -> Result<CheckContractSnapshot, String> {
    let placement = CheckPlacementOverride::new(
        ExecutorSelector {
            os: Some(lane.platform.clone()),
            ..Default::default()
        },
        CheckSelectorSource::Explicit,
    )
    .map_err(|error| {
        format!(
            "mainCadence platform {:?} is not placeable: {error}",
            lane.platform
        )
    })?;
    resolve_check_contract_at_commit(
        orch,
        db,
        CheckRunTarget {
            job_id: cadence_owner(context.project_id),
            project_id: context.project_id.to_string(),
            project_key: context.project_key.to_string(),
            repository_path: context.repo_path.to_string(),
            attribution_target: context.default_branch.to_string(),
            attribution_commit: None,
        },
        context.commit.to_string(),
        &lane.check,
        Some(&placement),
    )
    .await
}

async fn accepted_cadence_observations(
    orch: &Orchestrator,
    project: &str,
    lane: &CadenceLane,
) -> Result<Vec<String>, String> {
    orch.db
        .local
        .query_all(
            "SELECT DISTINCT w.observation_id FROM check_lane_adoption_waiters w
           JOIN check_lane_adoptions c ON c.request_id=w.request_id AND c.attempt_id=w.attempt_id
          WHERE c.project_id=?1 AND c.platform=?2 AND w.check_name=?3
            AND w.consumer='main_cadence' AND w.observation_id IS NOT NULL
            AND w.state IN ('recorded','delivered')
          ORDER BY w.updated_ms DESC LIMIT 64",
            (
                project.to_string(),
                lane.platform.clone(),
                lane.check.clone(),
            ),
            |row| row.text(0),
        )
        .await
        .map_err(|error| error.to_string())
}

async fn attempt_lane(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    context: &PassContext<'_>,
    lane: &CadenceLane,
) -> LaneAttempt {
    let death = |cause: String| {
        LaneAttempt::Verdict(
            CadenceVerdict::Death { row: None, cause },
            Attempted::default(),
        )
    };
    let contract = match resolve_lane_contract(orch, db.clone(), context, lane).await {
        Ok(contract) => contract,
        Err(error) => return death(error),
    };
    let attempted = Attempted {
        inputs: Some(contract.context.input_hash.clone()),
        suppressed: false,
        ..Default::default()
    };
    let names = |executor: &str| orch.fleet.executor_name_for_text(executor);
    // A complete row on any executor of the platform covers these inputs:
    // physical execution can have served a manual/review and cadence waiter.
    let adopted = accepted_cadence_observations(orch, context.project_id, lane).await.unwrap_or_else(|error| {
        log::warn!("could not read cadence waiter coverage: {error}; using ordinary whole-suite rows only");
        Vec::new()
    });
    let chain = main_chain(orch, context.repository, context.commit).unwrap_or_else(|error| {
        log::warn!(
            "cadence coverage ancestry unavailable: {error}; checking the resolved tip only"
        );
        MainChain::from_log(context.commit)
    });
    {
        match latest_complete_row(
            &db,
            context.project_id,
            context.project_key,
            lane,
            RowFilter {
                input_hash: Some(&contract.context.input_hash),
                main_chain: Some(&chain),
                adopted_observations: Some(&adopted),
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
            adopt: None,
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
        Err(error) => {
            return LaneAttempt::Verdict(
                CadenceVerdict::Death {
                    row: None,
                    cause: error,
                },
                attempted,
            )
        }
    };
    let attempted = Attempted {
        consumer_guard: result.consumer_guard.clone(),
        ..attempted
    };
    let attempt = |verdict: CadenceVerdict, suppressed: bool| {
        LaneAttempt::Verdict(
            verdict,
            Attempted {
                suppressed,
                ..attempted.clone()
            },
        )
    };
    if let Some(left) = result.left_running.as_ref() {
        return attempt(
            CadenceVerdict::AwaitingAdoption {
                machine: left.machine.clone(),
                request_id: left.request_id.clone(),
                stopped_at_unix_ms: i64::try_from(left.stopped_at_ms).unwrap_or(i64::MAX),
            },
            false,
        );
    }
    if let Some(no_verdict) = result.no_verdict.as_ref() {
        return attempt(
            CadenceVerdict::Death {
                row: None,
                cause: format!("{}: {}", no_verdict.kind, no_verdict.cause),
            },
            no_verdict.kind == NO_VERDICT_SUPPRESSED,
        );
    }
    let Some(observation_id) = result.observation_id.as_deref() else {
        return attempt(
            CadenceVerdict::NoVerdict {
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
            },
            false,
        );
    };
    loop {
        match load_row(&db, context.project_key, observation_id, &names).await {
            Ok(row) => return attempt(lane.verdict.classify(row), false),
            Err(error) if attempted.consumer_guard.is_some() => {
                log::error!("live cadence observation {observation_id} could not be read: {error}; retaining the consumer and retrying before reporting");
                tokio::time::sleep(crate::fleet::RETAINED_RESULT_RETRY).await;
            }
            Err(error) => {
                return attempt(
                    CadenceVerdict::NoVerdict {
                        row: None,
                        cause: format!(
                            "observation {observation_id} could not be read back: {error}"
                        ),
                    },
                    false,
                )
            }
        }
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
    attempted: &Attempted,
) {
    if let Err(error) = try_report_lane(
        orch,
        db,
        project_id,
        project_key,
        repository,
        lane,
        commit,
        verdict,
        attempted,
    )
    .await
    {
        log::error!("main cadence report failed: {error}");
    }
}

#[allow(clippy::too_many_arguments)]
async fn try_report_lane(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    repository: Option<&Path>,
    lane: &CadenceLane,
    commit: Option<&str>,
    verdict: CadenceVerdict,
    attempted: &Attempted,
) -> Result<(), String> {
    let captured_at = now_ms();
    let source = incident_sources::capture(
        db,
        project_id,
        lane,
        commit,
        &verdict,
        attempted,
        captured_at,
    )
    .await;
    if let Err(error) = &source {
        return Err(format!(
            "main incident source capture failed: {error}; outcome: {verdict:?}"
        ));
    }
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
    .await;
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
    let now = now_ms();
    let report = CadenceReport {
        check: &lane.check,
        platform: &lane.platform,
        kind: lane.verdict,
        commit,
        verdict: &verdict,
        comparison: &comparison,
        bracket: &bracket,
        now_ms: now,
        rearmed: rearmed.as_ref(),
    };
    let event_key = source.as_ref().ok().and_then(|s| s.as_deref());
    let (mut filing_note, _) = incident_note(
        orch,
        db,
        project_id,
        project_key,
        &lane.notify,
        &report,
        event_key.map(|key| (key, captured_at)),
    )
    .await;
    // This row may be the second host or the newer tree that decides reds an
    // earlier row on the lane holds for corroboration.
    filing_note
        .push_str(&incident_sources::revisit_held(orch, db, project_id, lane, event_key).await);
    let content = format!("{}{filing_note}", render_report(&report));
    orch.main_cadence.lock().unwrap().settled(
        project_id,
        LaneOutcome {
            check: lane.check.clone(),
            platform: lane.platform.clone(),
            tip: commit
                .or(verdict.row().map(|row| row.commit.as_str()))
                .map(str::to_string),
            summary: outcome_summary(lane.verdict, &verdict),
            row: verdict
                .row()
                .map(|row| (row.reference.clone(), row.commit.clone())),
            at_unix_ms: now,
        },
    );
    match &verdict {
        CadenceVerdict::Complete(_) => log::info!(
            "main cadence {} on {}: row recorded",
            lane.check,
            lane.platform
        ),
        CadenceVerdict::Cancelled { .. } | CadenceVerdict::AwaitingAdoption { .. } => {
            log::info!("{content}")
        }
        CadenceVerdict::NoVerdict { .. } | CadenceVerdict::Death { .. } => {
            log::error!("{content}")
        }
    }
    let observed = observe(lane.verdict, &verdict, &comparison, commit, attempted);
    let lane_key = (project_id.to_string(), lane.key());
    let (previous_standing, previous_filing_notice) = {
        let slots = orch.main_cadence.lock().unwrap();
        (
            slots.standing.get(&lane_key).cloned(),
            slots.filing_notices.get(project_id).copied(),
        )
    };
    let wake = orch
        .main_cadence
        .lock()
        .unwrap()
        .wake(project_id, lane, observed, now);
    // A boot re-arm cites the pass the restart cancelled, which only its own
    // report names.
    let filing_news = filing_notice_due(orch, project_id, &filing_note, now);
    let wake = if rearmed.is_some() || filing_news {
        Wake::Announce
    } else {
        wake
    };
    let delivered = match wake {
        Wake::Announce => try_deliver(orch, db, project_id, &lane.notify, &content).await,
        Wake::StillHeld { since_ms } => {
            let still = render_still_held(&report, since_ms);
            try_deliver(orch, db, project_id, &lane.notify, &still).await
        }
        Wake::Quiet => {
            log::info!(
                "main cadence {} on {}: identical to its predecessor, so recorded and not posted",
                lane.check,
                lane.platform
            );
            Ok(())
        }
    };
    if let Err(error) = delivered {
        // An attempted send is not an announcement. A retry must not turn a
        // failed Announce into Quiet, or discard the interruption it should cite.
        let mut slots = orch.main_cadence.lock().unwrap();
        match previous_standing {
            Some(standing) => {
                slots.standing.insert(lane_key, standing);
            }
            None => {
                slots.standing.remove(&lane_key);
            }
        }
        match previous_filing_notice {
            Some(at) => {
                slots.filing_notices.insert(project_id.to_string(), at);
            }
            None => {
                slots.filing_notices.remove(project_id);
            }
        }
        if let Some(interruption) = rearmed {
            slots.interrupted(project_id, interruption);
        }
        return Err(error);
    }
    if let Some(interruption) = &rearmed {
        forget_interruption(&orch.db.local, project_id, interruption).await;
    }
    Ok(())
}

fn filing_notice_due(orch: &Orchestrator, project_id: &str, note: &str, now: i64) -> bool {
    if note.is_empty() {
        return false;
    }
    let mut slots = orch.main_cadence.lock().unwrap();
    if slots
        .filing_notices
        .get(project_id)
        .is_some_and(|last| now - last < SILENCE_BOUND_MS)
    {
        return false;
    }
    slots.filing_notices.insert(project_id.to_string(), now);
    true
}

async fn retry_incident_intents(orch: &Orchestrator, db: &LocalDb) {
    match crate::execution::main_incidents_store::drain_pending(db, now_ms() / 1000).await {
        Ok(links) => {
            let mut notices: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
            for link in links.into_iter().filter(|link| link.created) {
                let Some((project, number)) = link
                    .issue_uri
                    .strip_prefix("cairn://p/")
                    .and_then(|uri| uri.split_once('/'))
                else {
                    continue;
                };
                let Ok(number) = number.parse::<i64>() else {
                    continue;
                };
                match db.query_opt(
                    "SELECT p.id,t.name FROM issues i JOIN projects p ON p.id=i.project_id
                     JOIN threads t ON t.id=i.parent_thread_id WHERE p.key=?1 AND i.number=?2",
                    (project.to_string(), number), |row| Ok((row.text(0)?,row.text(1)?)),
                ).await {
                    Ok(Some(owner)) => notices.entry(owner).or_default().push(link.issue_uri),
                    other => log::warn!("main incident retry filed an issue but could not resolve its owner: {other:?}"),
                }
            }
            for ((project, thread), links) in notices {
                let content = format!(
                    "Durable main incident filing resumed (no executions started):\n{}",
                    links
                        .iter()
                        .map(|uri| format!("- {uri}"))
                        .collect::<Vec<_>>()
                        .join("\n")
                );
                if filing_notice_due(orch, &project, &content, now_ms()) {
                    deliver(orch, db, &project, &thread, &content).await;
                }
            }
        }
        Err(error) => log::error!(
            "main incident startup/heartbeat retry failed; pending evidence retained: {error}"
        ),
    }
}

/// Filing's note for a cadence notice, and whether the report's observation
/// still holds reds for corroboration.
async fn incident_note(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    notify: &str,
    report: &CadenceReport<'_>,
    delivery: Option<(&str, i64)>,
) -> (String, bool) {
    use crate::execution::main_incidents_store as store;

    let names = |executor: &str| orch.fleet.executor_name_for_text(executor);
    let result = match crate::issues::crud::installation_machine_authorship(
        orch.anon_device_manager.device_id(),
        orch.services.clock.now(),
    ) {
        Ok(authorship) => {
            incidents::record(
                db,
                project_id,
                project_key,
                notify,
                report,
                authorship,
                delivery,
                &names,
            )
            .await
        }
        Err(error) => Err(error),
    };
    let held_until_ms = result
        .as_ref()
        .ok()
        .and_then(|recorded| recorded.held_until_ms);
    if let Some((event_key, _)) = delivery {
        let acknowledged = match held_until_ms {
            // Left unacknowledged on purpose: the source keeps the observation
            // and comes back when a new row lands on the lane or the wait ends.
            Some(until_ms) => {
                store::hold_source(db, project_id, event_key, (until_ms + 999) / 1000).await
            }
            None => {
                store::finish_source(
                    db,
                    project_id,
                    event_key,
                    result.as_ref().err().map(ToString::to_string),
                    now_ms() / 1000,
                )
                .await
            }
        };
        if let Err(error) = acknowledged {
            return (
                format!("\nIncident source acknowledgement failed: {error}; retained for retry.\n"),
                held_until_ms.is_some(),
            );
        }
    }
    let mut note = match result {
        Ok(recorded) => recorded.note,
        Err(error) => {
            log::error!(
                "main incident filing failed; durable source retained for cadence retry: {error}"
            );
            format!("\n**Incident filing failed:** {error}. The durable source remains available for the existing cadence to retry.\n")
        }
    };
    match crate::execution::main_incidents_store::source_health(
        db,
        Some(project_id),
        now_ms() / 1000,
    )
    .await
    {
        Ok(health) if health.pending > 0 => note.push_str(&format!(
            "\n{health}. The existing cadence retries these sources.\n"
        )),
        Err(error) => note.push_str(&format!(
            "\nIncident source backlog could not be read: {error}.\n"
        )),
        _ => {}
    }
    (note, held_until_ms.is_some())
}

async fn replay_covered_incidents(
    orch: &Orchestrator,
    db: &LocalDb,
    context: &PassContext<'_>,
    lane: &CadenceLane,
    row: &CadenceRow,
) {
    let names = |executor: &str| orch.fleet.executor_name_for_text(executor);
    let chain = main_chain(orch, context.repository, &row.commit).ok();
    let comparison = comparison_for(
        db,
        context.project_id,
        context.project_key,
        lane,
        Some(row),
        chain.as_ref(),
        &names,
    )
    .await;
    let bracket = match comparison.previous() {
        Some(previous) => {
            merge_bracket(
                orch,
                db,
                context.project_id,
                context.project_key,
                context.repository,
                &previous.commit,
                &row.commit,
            )
            .await
        }
        None => Bracket::default(),
    };
    let verdict = CadenceVerdict::Complete(row.clone());
    let report = CadenceReport {
        check: &lane.check,
        platform: &lane.platform,
        kind: lane.verdict,
        commit: Some(&row.commit),
        verdict: &verdict,
        comparison: &comparison,
        bracket: &bracket,
        now_ms: now_ms(),
        rearmed: None,
    };
    let (note, _) = incident_note(
        orch,
        db,
        context.project_id,
        context.project_key,
        &lane.notify,
        &report,
        Some((&format!("observation:{}", row.id), row.ran_at_ms)),
    )
    .await;
    if filing_notice_due(orch, context.project_id, &note, report.now_ms) {
        deliver(orch, db, context.project_id, &lane.notify, &note).await;
    }
}

/// An elapsed duration as the fleet prints one (`808815ms`, `3m34s`, `2h 5m`).
static ELAPSED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(?:\d+(?:\.\d+)?(?:ms|[smhd]))+\b").unwrap());
/// One outstanding debt's ids, as `Fleet::describe_purpose_debt` opens it.
static DEBT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"request (\S+) attempt (\S+?),").unwrap());

/// The announce step's one predicate, before the lane's standing non-verdict
/// is consulted ([`CadenceSlots::wake`]): a complete row and whether it is news
/// ([`row_is_news`]), or a non-verdict and what makes it the same as another.
///
/// A **hold** (the fleet refused to dispatch because the lane's question still
/// owes a displaced answer's confirmed death) is the debt it waits on: its
/// reason and each outstanding request and attempt, whatever the tip, since
/// only settling the debt ends it. A **suppression** is its inputs and its
/// reason: it lifts when they change. Any other non-verdict is its inputs (the
/// tip, when it died before resolving them) and its cause with every elapsed
/// duration elided, since a retry of an unchanged refusal differs only in how
/// long ago its evidence was measured. A restart cancellation is its restart.
fn observe(
    kind: LaneVerdict,
    verdict: &CadenceVerdict,
    comparison: &Comparison,
    commit: Option<&str>,
    attempted: &Attempted,
) -> Observed {
    let (class, cause) = match verdict {
        CadenceVerdict::Complete(row) => {
            return Observed::Row {
                news: row_is_news(kind, row, comparison),
            }
        }
        CadenceVerdict::NoVerdict { cause, .. } => ("no-verdict", cause),
        CadenceVerdict::Death { cause, .. } => ("death", cause),
        CadenceVerdict::AwaitingAdoption { request_id, .. } => {
            return Observed::Unobserved {
                identity: format!("awaiting-adoption\n{request_id}"),
                reminds: false,
            };
        }
        CadenceVerdict::Cancelled {
            stopped_at_unix_ms, ..
        } => {
            return Observed::Unobserved {
                identity: format!("cancelled\n{stopped_at_unix_ms}"),
                reminds: false,
            }
        }
    };
    let identity = match hold_debts(cause) {
        Some((reason, debts)) => format!("hold\n{class}\n{reason}\n{debts}"),
        None => {
            let anchor = attempted
                .inputs
                .as_deref()
                .or(verdict.row().map(|row| row.commit.as_str()))
                .or(commit)
                .unwrap_or_default();
            let class = if attempted.suppressed {
                NO_VERDICT_SUPPRESSED
            } else {
                class
            };
            format!("{class}\n{anchor}\n{}", ELAPSED.replace_all(cause, "…"))
        }
    };
    Observed::Unobserved {
        identity,
        reminds: !attempted.suppressed,
    }
}

/// The debt a hold waits on, when `cause` is the fleet's refusal to dispatch a
/// second answer to a question that still owes a confirmed death: the reason
/// it was refused under, and each outstanding request and attempt. Nothing else
/// in the refusal identifies the hold; the rest says how long ago each piece of
/// its evidence was measured.
fn hold_debts(cause: &str) -> Option<(&str, String)> {
    let at = cause.find(PURPOSE_OWES_DEATH)?;
    let head = &cause[..at];
    let reason = head
        .rsplit_once("purpose ")
        .map_or(head, |(reason, _)| reason)
        .trim();
    let mut debts: Vec<String> = DEBT
        .captures_iter(&cause[at..])
        .map(|ids| format!("{} {}", &ids[1], &ids[2]))
        .collect();
    debts.sort();
    debts.dedup();
    Some((reason, debts.join(" | ")))
}

/// Whether a complete row is news: whether it differs from its predecessor on
/// its own host, or ends a silence there. A baseline is news, and so is a row
/// [`SILENCE_BOUND_MS`] or more after its predecessor. An exit lane's row is
/// news unless it is a green after a green: its verdict is its exit, so two
/// greens say the same thing whichever hosts ran them. A per-test row is news
/// unless it was measured against its own host's previous row and turned
/// nothing red or green, observed no red test for the first time, lost no test,
/// skipped no test that was red, and found no new failure the host explains:
/// then its still-red set is exactly the previous row's. A row measured against
/// another host's is the first on its host, which is news.
fn row_is_news(kind: LaneVerdict, row: &CadenceRow, comparison: &Comparison) -> bool {
    let Some(previous) = comparison.previous() else {
        return true;
    };
    if row.ran_at_ms - previous.ran_at_ms >= SILENCE_BOUND_MS {
        return true;
    }
    match kind {
        LaneVerdict::Exit => !(row.verdict_passed && previous.verdict_passed),
        LaneVerdict::PerTest => {
            if !matches!(comparison, Comparison::SameExecutor { .. }) {
                return true;
            }
            let diff = row_diff(row, comparison);
            !(diff
                .host_conditions
                .iter()
                .all(|group| group.tests.is_empty())
                && diff.newly_red.is_empty()
                && diff.newly_green.is_empty()
                && diff.first_red.is_empty()
                && diff.removed.is_empty()
                && diff.red_now_skipped == 0
                && diff.audit_newly_red.is_empty()
                && diff.audit_newly_green.is_empty()
                // An audit that stopped or started judging moved no finding, but
                // a row saying nothing about it would read as clean.
                && audit_state(row) == audit_state(previous))
        }
    }
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
    let content = render_heartbeat(
        &lane.check,
        &lane.platform,
        lane.verdict,
        context.commit,
        covered,
        now,
    );
    deliver(orch, db, context.project_id, &lane.notify, &content).await;
}

/// A lane run that ended without reaching its report (it panicked) still owes
/// its lane the loudest message.
async fn announce_lane_death(orch: &Orchestrator, tip: &Tip, lane: &CadenceLane, cause: String) {
    let Ok(db) = crate::execution::routing::owning_db_for_project(&orch.db, &tip.project_id).await
    else {
        return;
    };
    report_lane(
        orch,
        &db,
        &tip.project_id,
        &tip.project_key,
        Some(&tip.repository),
        lane,
        Some(&tip.commit),
        cancelled_by_restart(
            CadenceVerdict::Death { row: None, cause },
            orch.fleet.runner_stopping_since(),
        ),
        &Attempted::default(),
    )
    .await;
}

/// A pass that failed before reaching its lanes still owes each lane in its
/// scope its loudest message. The tip's contract was unreadable, so the lanes
/// come from the project checkout's configuration.
async fn announce_pass_failure(
    orch: &Orchestrator,
    project_id: &str,
    repo_path: &str,
    scope: &LaneScope,
    cause: &str,
) {
    let Some(checks) = crate::config::project_settings::load_checks(Path::new(repo_path)) else {
        return;
    };
    let lanes: Vec<CadenceLane> = cadence_lanes(&checks)
        .into_iter()
        .filter(|lane| scope.includes(lane))
        .collect();
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
                    row: None,
                    cause: format!("the cadence pass failed before this lane could run: {cause}"),
                },
                orch.fleet.runner_stopping_since(),
            ),
            &Attempted::default(),
        )
        .await;
    }
}

async fn deliver(orch: &Orchestrator, db: &LocalDb, project_id: &str, thread: &str, content: &str) {
    if let Err(error) = try_deliver(orch, db, project_id, thread, content).await {
        log::error!("{error}; undelivered:\n{content}");
    }
}

async fn try_deliver(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    thread: &str,
    content: &str,
) -> Result<(), String> {
    let thread_id = db
        .query_opt_text(
            "SELECT id FROM threads WHERE project_id = ?1 AND name = ?2 LIMIT 1",
            (project_id.to_string(), thread.to_string()),
        )
        .await
        .map_err(|error| format!("main cadence notify thread lookup failed: {error}"))?
        .ok_or_else(|| format!("main cadence notify thread {thread:?} does not exist"))?;
    crate::messages::delivery::append_thread_message(orch, db, &thread_id, None, SENDER, content)
        .await
        .map(|_| ())
        .map_err(|error| format!("main cadence delivery to thread {thread:?} failed: {error}"))
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

/// What a failed test's excerpt says, and when the test ran.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Failure {
    /// See [`failure_signature`].
    pub(crate) signature: String,
    /// The machine condition the excerpt names, if any: see [`host_condition`].
    pub(crate) host_condition: Option<&'static HostCondition>,
    /// The bound the excerpt says ended the test, if one did: see
    /// [`BOUND_ENDINGS`].
    pub(crate) ending: Option<BoundEnding>,
    /// Where the test ran on its lane's timeline, when the row recorded its
    /// start.
    pub(crate) span: Option<Span>,
}

impl Failure {
    fn from_excerpt(excerpt: &str) -> Self {
        Self {
            signature: failure_signature(excerpt),
            host_condition: host_condition(excerpt),
            ending: bound_ending(excerpt),
            span: None,
        }
    }

    fn ran(self, span: Option<Span>) -> Self {
        Self { span, ..self }
    }
}

/// Where a test ran on its lane's timeline: milliseconds from the lane's
/// origin to its start and to its end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Span {
    start_ms: i64,
    end_ms: i64,
}

impl Span {
    /// The span a per-test row's start offset and duration describe; `None`
    /// unless it recorded both.
    fn recorded(started_offset_ms: Option<i64>, duration_ms: Option<i64>) -> Option<Self> {
        let start_ms = started_offset_ms.filter(|start| *start >= 0)?;
        let duration_ms = duration_ms.filter(|duration| *duration >= 0)?;
        Some(Self {
            start_ms,
            end_ms: start_ms.saturating_add(duration_ms),
        })
    }

    fn overlaps(&self, from_ms: i64, to_ms: i64) -> bool {
        self.start_ms <= to_ms && from_ms <= self.end_ms
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
    /// The parsed result's selection (`full`, `empty`, `unknown`, ...), when it
    /// recorded one.
    pub(crate) selection: Option<String>,
    pub(crate) tests: BTreeMap<String, TestOutcome>,
    pub(crate) output_tail: String,
    /// The wall-clock instant a [`Span`] counts from: the executor's spawn
    /// stamp, when the lane's timeline started there (`phase_origin =
    /// 'executor'`). `None` when the timeline started anywhere else.
    pub(crate) lane_started_at_ms: Option<i64>,
    /// `None` for a lane that carries no audit, such as a row recorded before
    /// the audit existed.
    pub(crate) plan_audit: Option<RowAudit>,
}

/// A row's plan audit as stored: its state and its findings by identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RowAudit {
    pub(crate) evaluated: bool,
    /// Why it was not evaluated.
    pub(crate) note: Option<String>,
    pub(crate) findings: BTreeMap<String, AuditFinding>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuditFinding {
    pub(crate) status: String,
    pub(crate) issue: Option<String>,
    pub(crate) excerpt: Option<String>,
}

impl RowAudit {
    /// Whether this finding holds this row red.
    pub(crate) fn gates(&self, finding: &str) -> bool {
        self.findings
            .get(finding)
            .is_some_and(|finding| plan_audit_status_gates(&finding.status))
    }

    fn gating(&self) -> impl Iterator<Item = (&String, &AuditFinding)> {
        self.findings
            .iter()
            .filter(|(_, finding)| plan_audit_status_gates(&finding.status))
    }
}

impl CadenceRow {
    fn audit_gating(&self) -> usize {
        self.plan_audit
            .as_ref()
            .map_or(0, |audit| audit.gating().count())
    }

    /// Whether this row's nonzero exit is the plan audit's own, on a run that was
    /// otherwise clean: a named red, not a runner that failed outside any test.
    fn exit_is_plan_audits(&self) -> bool {
        self.exit_code == i64::from(PLAN_AUDIT_EXIT_CODE) && self.audit_gating() > 0
    }

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
    /// The lane died before producing a verdict. `row` is the observation it
    /// left when the death shows only in that row's output (a compile the host
    /// ended: see [`host_ended_compile`]); a lane that died before recording an
    /// observation has none.
    Death {
        row: Option<CadenceRow>,
        cause: String,
    },
    /// A remote lane survived shutdown and owes its answer to the next runner.
    AwaitingAdoption {
        machine: String,
        request_id: String,
        stopped_at_unix_ms: i64,
    },
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
            CadenceVerdict::NoVerdict { row, .. } | CadenceVerdict::Death { row, .. } => {
                row.as_ref()
            }
            CadenceVerdict::Cancelled { .. } | CadenceVerdict::AwaitingAdoption { .. } => None,
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
    if let Some(cause) = host_ended_compile_cause(&row).or_else(|| credential_fetch_cause(&row)) {
        return CadenceVerdict::Death {
            row: Some(row),
            cause,
        };
    }
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
    } else if !row.verdict_passed && row.failed() == 0 && !row.exit_is_plan_audits() {
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

/// Whether a stored row of an exit lane ([`LaneVerdict::Exit`]) is a verdict.
/// Its exit is the verdict once the run's own result vouches for having
/// measured something. A lint that selected nothing, or a run that stopped
/// before it finished measuring, is stored as incomplete, and a green beside it
/// is not one.
fn classify_exit_row(row: CadenceRow) -> CadenceVerdict {
    if let Some(cause) = host_ended_compile_cause(&row).or_else(|| credential_fetch_cause(&row)) {
        return CadenceVerdict::Death {
            row: Some(row),
            cause,
        };
    }
    let cause = if !row.complete {
        Some(format!(
            "the run exited {} but its own result says it did not run to completion, so it measured nothing a verdict can rest on",
            row.exit_code
        ))
    } else if row.selection.as_deref() == Some("empty") {
        Some(format!(
            "the run exited {} having selected nothing to check, so it measured nothing",
            row.exit_code
        ))
    } else if matches!(row.selection.as_deref(), Some("partial" | "filtered")) {
        Some(
            "the run measured a selected slice, not the whole command this cadence lane observes"
                .to_string(),
        )
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

/// The cause a row reports when its compile was ended by the host (see
/// [`host_ended_compile`], the predicate the store's classifier also uses):
/// a death on the execution side of the seam, with no rustc signature to
/// match against a build break on main. Read from the stored tail rather than
/// the stored `failure_kind` so a row stored before the classifier knew this
/// shape still reads as the death it was.
fn host_ended_compile_cause(row: &CadenceRow) -> Option<String> {
    if row.verdict_passed || row.failed() > 0 || row.exit_is_plan_audits() {
        return None;
    }
    let ended = host_ended_compile(&row.output_tail)?;
    let krate = ended
        .krate
        .map(|krate| format!(" while compiling `{krate}`"))
        .unwrap_or_default();
    let exit = row.exit_code;
    Some(match ended.by {
        HostEnding::Signal(signal) => format!(
            "infrastructure: a compiler process in the lane was killed ({signal}){krate}, so the run exited {exit} without a verdict on the tree. No compiler error about the code was printed, so the tree is not implicated: the signal came from outside the compile (a stall stop, a pressure reclaim, a cell operation), and there is no compile error to match against a break on main"
        ),
        HostEnding::Storage(error) => format!(
            "infrastructure: the host's storage failed ({error}){krate}, so the run exited {exit} without a verdict on the tree. The error is the operating system's, not a compiler error about the code, so the tree is not implicated and there is no compile error to match against a break on main"
        ),
        HostEnding::WriteDenied(error) => format!(
            "infrastructure: the operating system refused the compiler's write of its build output ({error}){krate}, so the run exited {exit} without a verdict on the tree. The refusal is the machine's write access to its build directory and recurs until that access changes; it is not a compiler error about the code, so the tree is not implicated and there is no compile error to match against a break on main"
        ),
    })
}

/// A stored row whose run died fetching a git dependency it had no credential
/// for, read from its tail with the same matcher the store's classifier and
/// the executor's GitHub probe use. Such a run never reached the tree.
fn credential_fetch_cause(row: &CadenceRow) -> Option<String> {
    if row.verdict_passed || row.failed() > 0 || row.exit_is_plan_audits() {
        return None;
    }
    let failure = cairn_common::git_credential::credential_failure(&row.output_tail)?;
    let what = failure
        .repository
        .as_ref()
        .map(|repository| format!("git dependency `{repository}`"))
        .unwrap_or_else(|| "a git dependency".to_string());
    let host = failure.host.as_deref().unwrap_or("its host");
    Some(format!(
        "infrastructure: fetching {what} failed for want of a credential for {host}, so the run exited {} without a verdict on the tree. The machine has no usable credential; the tree is not implicated and there is no error to match against a break on main",
        row.exit_code
    ))
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
        CadenceVerdict::Death { row: None, cause }
        | CadenceVerdict::NoVerdict { row: None, cause } => CadenceVerdict::Cancelled {
            cause,
            stopped_at_unix_ms: i64::try_from(stopped_at).unwrap_or(i64::MAX),
        },
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

async fn forget_interruption(db: &LocalDb, project_id: &str, interruption: &Interruption) {
    if let Err(error) = db.execute(
        "DELETE FROM main_cadence_interruptions WHERE project_id=?1 AND check_name=?2 AND platform=?3 AND commit_sha=?4 AND stopped_at=?5",
        (project_id, interruption.check.as_str(), interruption.platform.as_str(), interruption.commit.as_str(), interruption.stopped_at_unix_ms),
    ).await {
        log::warn!("could not retire cited cadence interruption: {error}");
    }
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
pub(crate) type ExecutorNames<'a> = &'a (dyn Fn(&str) -> String + Sync);

async fn load_row(
    db: &LocalDb,
    project_key: &str,
    observation_id: &str,
    names: ExecutorNames<'_>,
) -> Result<CadenceRow, String> {
    let head = db
        .query_opt(
            "SELECT id, COALESCE(public_handle, id), commit_sha, COALESCE(verdict_platform, ''),
                    executor_id, ran_at, verdict, exit_code, complete, output_tail, cadence,
                    selection,
                    CASE WHEN phase_origin = 'executor' THEN executor_started_at_unix_ms END,
                    plan_audit_state, plan_audit_note
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
                    row.opt_text(11)?,
                    row.opt_i64(12)?,
                    row.opt_text(13)?,
                    row.opt_text(14)?,
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
        selection,
        lane_started_at_ms,
        plan_audit_state,
        plan_audit_note,
    ) = head;
    let plan_audit = match plan_audit_state {
        None => None,
        Some(state) => Some(RowAudit {
            evaluated: state == "evaluated",
            note: plan_audit_note,
            findings: db
                .query_all(
                    "SELECT finding_id, status, issue, excerpt
                       FROM check_audit_findings WHERE observation_id = ?1",
                    (id.clone(),),
                    |row| {
                        Ok((
                            row.text(0)?,
                            AuditFinding {
                                status: row.text(1)?,
                                issue: row.opt_text(2)?,
                                excerpt: row.opt_text(3)?,
                            },
                        ))
                    },
                )
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .collect(),
        }),
    };
    let tests = db
        .query_all(
            "SELECT test_id, status, failure_excerpt, started_offset_ms, duration_ms
               FROM check_test_results WHERE observation_id = ?1",
            (id.clone(),),
            |row| {
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.opt_text(2)?,
                    Span::recorded(row.opt_i64(3)?, row.opt_i64(4)?),
                ))
            },
        )
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|(test, status, excerpt, span)| {
            let outcome = match status.as_str() {
                "passed" => TestOutcome::Passed,
                "skipped" => TestOutcome::Skipped,
                _ => TestOutcome::Failed(
                    Failure::from_excerpt(excerpt.as_deref().unwrap_or("")).ran(span),
                ),
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
        selection,
        tests,
        output_tail: tail,
        lane_started_at_ms,
        plan_audit,
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
    pub(crate) fn from_log(output: &str) -> Self {
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
    pub(crate) fn depth(&self, commit: &str) -> Option<usize> {
        self.depth.get(commit).copied()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.depth.is_empty()
    }

    /// Every commit on the chain, in no particular order.
    fn commits(&self) -> impl Iterator<Item = &str> {
        self.depth.keys().map(String::as_str)
    }
}

fn main_chain(orch: &Orchestrator, repository: &Path, tip: &str) -> Result<MainChain, String> {
    first_parent_ancestry(
        &crate::jj::environment(&orch.jj_binary_path, &orch.config_dir),
        repository,
        tip,
    )
}

/// `tip` and its first-parent ancestors, back [`BRACKET_SCAN`] landings, tip
/// first. Read from main's tip it is the trees main has held; read from a
/// branch's base on main it is the trees main held at or before that base.
pub(crate) fn first_parent_ancestry(
    jj: &crate::jj::JjEnv,
    repository: &Path,
    tip: &str,
) -> Result<MainChain, String> {
    let jj = jj.pin_immutable_read(repository, &[tip])?;
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
    /// Only rows that observed this commit.
    commit: Option<&'a str>,
    /// Only rows on a named executor other than this one.
    other_than_executor: Option<&'a str>,
    /// Main's first-parent chain. A row of any cadence on it qualifies, and
    /// the nearest tree wins. Without it only the cadence's own rows qualify,
    /// newest finish first.
    main_chain: Option<&'a MainChain>,
    /// Rows a durable cadence waiter accepted from a shared physical grant.
    /// The claims live in the runner store, which may differ from this row's DB.
    adopted_observations: Option<&'a [String]>,
    /// Only rows on `main_chain`, the cadence's own included. Set when the
    /// chain was read from a base rather than main's tip: a cadence row off it
    /// observed a tree main held after the base.
    chain_only: bool,
}

/// How many rows each of [`latest_complete_row`]'s reads returns, newest
/// finish first. A read that returns this many may have left older candidates
/// unread.
const CANDIDATE_SCAN: i64 = 64;

/// [`CANDIDATE_SCAN`] for a chain read from a branch's base: every main row
/// recorded since the base is newer than the rows that can answer it.
const BASE_CANDIDATE_SCAN: i64 = 256;

/// A per-test row observed the whole suite: a fast, filtered, or empty
/// selection is not the suite (tenet 695), and a tiered run's deferred tests
/// are unobserved.
pub(crate) const PER_TEST_WHOLE_SUITE: &str = "selection = 'full' AND deferred_count = 0";

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
///
/// An exit lane's rows carry no per-test selection to prove the whole command
/// ran, so its rows qualify by how they were armed instead: the cadence's own
/// and manual runs, which always execute the whole-suite command, and no
/// review or write row, which may have run a narrowed one.
///
/// Candidates are read by tree, not by recency: the cadence's own rows, and
/// every row on one of the chain's commits. Agent branches' write and review
/// rows share the table and outnumber main's by hundreds to one, so the newest
/// rows by finish time can all be off the chain while main's own sit just
/// behind them (cairn/6244). `Ok(None)` is a finding that no row qualifies.
/// When a read came back full and nothing qualified, older rows went unread and
/// nothing was found: that is an error, never `Ok(None)`.
async fn latest_complete_row(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    lane: &CadenceLane,
    filter: RowFilter<'_>,
    names: ExecutorNames<'_>,
) -> Result<Option<CadenceRow>, String> {
    use cairn_db::sql::{placeholders, IN_LIST_CHUNK};
    use cairn_db::turso::Value as SqlValue;

    let adopted = filter.adopted_observations.unwrap_or_default();
    let whole_suite = match lane.verdict {
        LaneVerdict::PerTest => PER_TEST_WHOLE_SUITE.to_string(),
        LaneVerdict::Exit => {
            let authority = if adopted.is_empty() {
                "cadence IN ('main', 'manual')".to_string()
            } else {
                format!(
                    "(cadence IN ('main', 'manual') OR id IN ({}))",
                    placeholders(adopted.len())
                )
            };
            format!("COALESCE(selection, '') NOT IN ('empty', 'partial', 'filtered') AND COALESCE(deferred_count, 0) = 0 AND {authority}")
        }
    };
    let mut narrowing = format!(
        "project_id = ? AND check_name = ? AND verdict_platform = ?
           AND complete = 1 AND failure_kind IS NULL AND {whole_suite}"
    );
    let mut params = vec![
        SqlValue::Text(project_id.to_string()),
        SqlValue::Text(lane.check.clone()),
        SqlValue::Text(lane.platform.clone()),
    ];
    if lane.verdict == LaneVerdict::Exit {
        params.extend(adopted.iter().cloned().map(SqlValue::Text));
    }
    for (clause, value) in [
        (" AND id != ?", filter.excluding),
        (" AND input_hash = ?", filter.input_hash),
        (" AND executor_id = ?", filter.executor_id),
        (" AND commit_sha = ?", filter.commit),
        (
            " AND executor_id IS NOT NULL AND executor_id != ?",
            filter.other_than_executor,
        ),
    ] {
        if let Some(value) = value {
            narrowing.push_str(clause);
            params.push(SqlValue::Text(value.to_string()));
        }
    }
    let scan = if filter.chain_only {
        BASE_CANDIDATE_SCAN
    } else {
        CANDIDATE_SCAN
    };
    let read = |predicate: String, bound: Vec<SqlValue>| {
        let sql = format!(
            "SELECT id, commit_sha, cadence, ran_at FROM check_result_observations
              WHERE {narrowing} AND {predicate}
              ORDER BY ran_at DESC LIMIT {scan}"
        );
        async move {
            db.query_all(sql, bound, |row| {
                Ok((row.text(0)?, row.text(1)?, row.text(2)?, row.i64(3)?))
            })
            .await
            .map_err(|error| error.to_string())
        }
    };
    let mut found = Vec::new();
    let mut saturated = false;
    if !filter.chain_only {
        let rows = read("cadence = 'main'".to_string(), params.clone()).await?;
        saturated |= rows.len() as i64 >= scan;
        found.extend(rows);
    }
    if let Some(chain) = filter.main_chain {
        let commits: Vec<&str> = chain.commits().collect();
        for chunk in commits.chunks(IN_LIST_CHUNK - params.len()) {
            let mut bound = params.clone();
            bound.extend(
                chunk
                    .iter()
                    .map(|commit| SqlValue::Text((*commit).to_string())),
            );
            let predicate = format!("commit_sha IN ({})", placeholders(chunk.len()));
            let rows = read(predicate, bound).await?;
            saturated |= rows.len() as i64 >= scan;
            found.extend(rows);
        }
    }
    let mut seen = HashSet::new();
    let mut candidates: Vec<(String, (usize, std::cmp::Reverse<i64>))> = found
        .into_iter()
        .filter(|(id, ..)| seen.insert(id.clone()))
        .filter_map(|(id, commit, cadence, ran_at)| {
            let depth = filter.main_chain.and_then(|chain| chain.depth(&commit));
            if depth.is_none() && (filter.chain_only || cadence != "main") {
                return None;
            }
            Some((id, (depth.unwrap_or(usize::MAX), std::cmp::Reverse(ran_at))))
        })
        .collect();
    candidates.sort_by_key(|(_, order)| *order);
    for (id, _) in candidates {
        let row = load_row(db, project_key, &id, names)
            .await
            .map_err(|error| format!("candidate {id} could not be read: {error}"))?;
        if let CadenceVerdict::Complete(row) = lane.verdict.classify(row) {
            return Ok(Some(row));
        }
    }
    if saturated {
        return Err(format!(
            "a read of the {scan} newest candidates held no complete whole-suite row, and older rows were not read"
        ));
    }
    Ok(None)
}

/// The nearest complete whole-suite row of the per-test `check` on `platform`
/// whose tree is on `chain`, in [`tree_order`]. With `chain` read from a
/// branch's base, this is the main row the base is judged by: the nearest at or
/// before it, whoever armed it, and never one recorded on a later main tree.
pub(crate) async fn nearest_row_on_chain(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    check: &str,
    platform: &str,
    chain: &MainChain,
    names: ExecutorNames<'_>,
) -> Result<Option<CadenceRow>, String> {
    let lane = CadenceLane {
        check: check.to_string(),
        platform: platform.to_string(),
        notify: String::new(),
        verdict: LaneVerdict::PerTest,
    };
    latest_complete_row(
        db,
        project_id,
        project_key,
        &lane,
        RowFilter {
            main_chain: Some(chain),
            chain_only: true,
            ..RowFilter::default()
        },
        names,
    )
    .await
}

/// How many of main's rows at or before a branch's base a failure's rate on
/// main is read over: enough that a test failing a quarter of main's rows
/// almost surely shows, few enough to stay inside main's recent history.
pub(crate) const MAIN_RATE_WINDOW: usize = 10;

/// How often one test failed across a window of main's rows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TestRate {
    /// Rows in the window that failed it.
    pub(crate) red: usize,
    /// Rows in the window that ran it (passed or failed).
    pub(crate) ran: usize,
}

/// How often each of `tests` failed on the newest [`MAIN_RATE_WINDOW`]
/// complete whole-suite rows of the per-test `check` on `platform` whose tree
/// is on `chain`, nearest first in [`tree_order`]. With `chain` read from a
/// branch's base, this is main's rate for a test the base's nearest row passed:
/// a test main fails now and then can pass on that one row by chance (tenet
/// 703's "red on K of the last N main rows"). A row recorded on a later main
/// tree is never in the window: it observed a tree the branch does not carry.
///
/// A row qualifies as [`latest_complete_row`] admits one and is a verdict as
/// [`classify_row`] reads one: a green row ran a passing test, a red row ran a
/// failing one. Both are checked in the query rather than by loading each row's
/// every test, and only `tests` are read from the rows chosen. A test absent
/// from the answer was run by no row in the window.
pub(crate) async fn rates_on_chain(
    db: &LocalDb,
    project_id: &str,
    check: &str,
    platform: &str,
    chain: &MainChain,
    tests: &[String],
) -> Result<HashMap<String, TestRate>, String> {
    use cairn_db::sql::{placeholders, IN_LIST_CHUNK};
    use cairn_db::turso::Value as SqlValue;

    let mut rates: HashMap<String, TestRate> = HashMap::new();
    if tests.is_empty() {
        return Ok(rates);
    }
    let candidates = db
        .query_all(
            format!(
                "SELECT o.id, o.commit_sha FROM check_result_observations o
                  WHERE o.project_id = ?1 AND o.check_name = ?2 AND o.verdict_platform = ?3
                    AND o.complete = 1 AND o.failure_kind IS NULL AND {PER_TEST_WHOLE_SUITE}
                    AND EXISTS (
                        SELECT 1 FROM check_test_results t
                         WHERE t.observation_id = o.id
                           AND t.status = CASE WHEN o.verdict = 'passed'
                                               THEN 'passed' ELSE 'failed' END)
                  ORDER BY o.ran_at DESC LIMIT ?4"
            ),
            (
                project_id.to_string(),
                check.to_string(),
                platform.to_string(),
                BASE_CANDIDATE_SCAN,
            ),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut window: Vec<(String, usize)> = candidates
        .into_iter()
        .filter_map(|(id, commit)| chain.depth(&commit).map(|depth| (id, depth)))
        .collect();
    // Stable, so rows on one tree keep the query's newest-finish-first order.
    window.sort_by_key(|(_, depth)| *depth);
    let window: Vec<String> = window
        .into_iter()
        .take(MAIN_RATE_WINDOW)
        .map(|(id, _)| id)
        .collect();
    if window.is_empty() {
        return Ok(rates);
    }
    for chunk in tests.chunks(IN_LIST_CHUNK - MAIN_RATE_WINDOW) {
        let params: Vec<SqlValue> = window
            .iter()
            .chain(chunk)
            .cloned()
            .map(SqlValue::Text)
            .collect();
        let outcomes = db
            .query_all(
                format!(
                    "SELECT test_id, status FROM check_test_results
                      WHERE observation_id IN ({}) AND test_id IN ({})
                        AND status IN ('passed', 'failed')",
                    placeholders(window.len()),
                    placeholders(chunk.len())
                ),
                params,
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .map_err(|error| error.to_string())?;
        for (test, status) in outcomes {
            let rate = rates.entry(test).or_default();
            rate.ran += 1;
            if status == "failed" {
                rate.red += 1;
            }
        }
    }
    Ok(rates)
}

/// What a cadence attempt is measured against. Two hosts of one platform
/// differ by provisioning and load as well as by tree, so the previous row is
/// chosen by executor first, and a comparison that crosses hosts says so.
///
/// History that could not be read is never history that does not exist: only
/// lookups that succeeded and found nothing make a [`Comparison::Baseline`].
#[derive(Clone, Debug, Default)]
pub(crate) enum Comparison {
    /// Every lookup succeeded, and no complete row exists for the lane on any
    /// executor.
    #[default]
    Baseline,
    /// The previous complete row on the attempt's own executor: the diff is
    /// tree movement. `neighbour` is another executor's complete row that ran
    /// after `previous`, when one did: the other host's view of the window,
    /// on whatever tree it ran. `peer` is another executor's complete row on
    /// the attempt's own commit, the only row a test is said to differ by
    /// host against: across trees, a merge in between explains a difference
    /// as well as the host does (tenet 772). `unread` names the lookups for
    /// `neighbour` and `peer` that failed: either one being unknown means
    /// nothing separates this host from the others.
    SameExecutor {
        previous: CadenceRow,
        neighbour: Option<Box<CadenceRow>>,
        peer: Option<Box<CadenceRow>>,
        unread: Unread,
    },
    /// The attempt's executor has no earlier complete row, so the platform's
    /// newest (another executor's) stands in, and host differences cannot be
    /// told from movement.
    OtherExecutor { previous: CadenceRow },
    /// The attempt names no executor (a death), so the platform's newest
    /// complete row on any executor stands in.
    AnyExecutor { previous: CadenceRow },
    /// The lookup that chooses the compared row failed, so the lane's history
    /// is unknown. Nothing is measured against it: no movement, no merge list,
    /// no coverage. `known` is the platform's newest complete row, when only
    /// this host's own lookup failed. It is cited as evidence that history
    /// exists and is never compared with.
    Unavailable {
        cause: String,
        known: Option<Box<CadenceRow>>,
    },
}

/// The optional lookups of a same-executor comparison that failed, each with
/// its bounded cause.
#[derive(Clone, Debug, Default)]
pub(crate) struct Unread {
    /// The platform's newest row on any executor: the window's neighbour, and
    /// how far another host's row covers main.
    pub(crate) neighbour: Option<String>,
    /// Another executor's row on the attempt's own commit.
    pub(crate) peer: Option<String>,
}

impl Unread {
    fn any(&self) -> bool {
        self.neighbour.is_some() || self.peer.is_some()
    }
}

const SAME_HOST_LOOKUP: &str = "lookup of this host's previous complete row";
const NEWEST_LOOKUP: &str = "lookup of the platform's newest complete row";
const PEER_LOOKUP: &str = "lookup of another host's row on this commit";

/// A failed lookup as a report states it: one line, bounded, so an error that
/// carries a statement or a row's contents cannot flood the thread.
fn lookup_failure(lookup: &str, error: &str) -> String {
    let error = error.split_whitespace().collect::<Vec<_>>().join(" ");
    format!("The {lookup} failed ({})", truncate(&error, 300))
}

impl Comparison {
    fn previous(&self) -> Option<&CadenceRow> {
        match self {
            Comparison::Baseline | Comparison::Unavailable { .. } => None,
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
            Comparison::Baseline | Comparison::Unavailable { .. } => None,
            // Another host's newer row may exist and was not read.
            Comparison::SameExecutor {
                unread: Unread {
                    neighbour: Some(_), ..
                },
                ..
            } => None,
            // `neighbour` is set exactly when another executor's row is newer.
            Comparison::SameExecutor {
                previous,
                neighbour,
                ..
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
/// newest complete row, the platform's newest complete row on any executor,
/// "newest" in [`tree_order`], and another executor's row on the attempt's
/// own commit.
fn select_comparison(
    executor_id: Option<&str>,
    same_executor: Option<CadenceRow>,
    newest: Option<CadenceRow>,
    peer: Option<CadenceRow>,
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
                peer: peer.map(Box::new),
                unread: Unread::default(),
            }
        }
        None => newest.map_or(Comparison::Baseline, |previous| Comparison::OtherExecutor {
            previous,
        }),
    }
}

/// [`select_comparison`] over lookups that may have failed. A failed lookup
/// is unknown history, never absent history. When the lookup that chooses the
/// compared row fails, the comparison is [`Comparison::Unavailable`]: this
/// host's own when the attempt names an executor, else the platform's newest.
/// When only an optional lookup fails after this host's previous row was
/// found, that row is still compared, and the lookups that failed are named in
/// [`Unread`].
fn resolve_comparison(
    executor_id: Option<&str>,
    same_executor: Result<Option<CadenceRow>, String>,
    newest: Result<Option<CadenceRow>, String>,
    peer: Result<Option<CadenceRow>, String>,
    chain: Option<&MainChain>,
) -> Comparison {
    let unavailable =
        |lookup: &str, error: &str, known: Option<CadenceRow>| Comparison::Unavailable {
            cause: lookup_failure(lookup, error),
            known: known.map(Box::new),
        };
    let Some(executor_id) = executor_id else {
        return match newest {
            Ok(newest) => select_comparison(None, None, newest, None, chain),
            Err(error) => unavailable(NEWEST_LOOKUP, &error, None),
        };
    };
    match same_executor {
        Err(error) => unavailable(SAME_HOST_LOOKUP, &error, newest.ok().flatten()),
        Ok(None) => match newest {
            Ok(newest) => select_comparison(Some(executor_id), None, newest, None, chain),
            Err(error) => unavailable(NEWEST_LOOKUP, &error, None),
        },
        Ok(Some(previous)) => {
            let unread = Unread {
                neighbour: newest
                    .as_ref()
                    .err()
                    .map(|error| lookup_failure(NEWEST_LOOKUP, error)),
                peer: peer
                    .as_ref()
                    .err()
                    .map(|error| lookup_failure(PEER_LOOKUP, error)),
            };
            let mut comparison = select_comparison(
                Some(executor_id),
                Some(previous),
                newest.ok().flatten(),
                peer.ok().flatten(),
                chain,
            );
            if let Comparison::SameExecutor { unread: slot, .. } = &mut comparison {
                *slot = unread;
            }
            comparison
        }
    }
}

/// What `current` (the attempt's own row, when it recorded one) is measured
/// against: whole-suite rows of any cadence on `chain`, when it is known. A
/// lookup that fails is logged and carried into the comparison
/// ([`resolve_comparison`]); it never reads as a lane with no history.
async fn comparison_for(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    lane: &CadenceLane,
    current: Option<&CadenceRow>,
    chain: Option<&MainChain>,
    names: ExecutorNames<'_>,
) -> Comparison {
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
            .await
        }
        None => Ok(None),
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
    .await;
    let peer = match (executor_id, current) {
        (Some(executor_id), Some(current)) => {
            latest_complete_row(
                db,
                project_id,
                project_key,
                lane,
                RowFilter {
                    excluding,
                    commit: Some(&current.commit),
                    other_than_executor: Some(executor_id),
                    main_chain: chain,
                    ..RowFilter::default()
                },
                names,
            )
            .await
        }
        _ => Ok(None),
    };
    for (lookup, result) in [
        (SAME_HOST_LOOKUP, &same_executor),
        (NEWEST_LOOKUP, &newest),
        (PEER_LOOKUP, &peer),
    ] {
        if let Err(error) = result {
            log::warn!(
                "main cadence {} on {}: {}; the report says its history is unknown rather than absent",
                lane.check,
                lane.platform,
                lookup_failure(lookup, error)
            );
        }
    }
    resolve_comparison(executor_id, same_executor, newest, peer, chain)
}

/// The line of a failure excerpt that says what went wrong. A Rust panic opens
/// with `thread '<test>' (<tid>) panicked at <file>:<line>:` — unique to every
/// test — and states the failure on the next line, so that line is the
/// signature. Tests that fail the same way share a signature, which is what
/// lets one intermittent race that flips a dozen tests read as one group.
pub(crate) fn failure_signature(excerpt: &str) -> String {
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

/// A failure that describes the machine a test ran on rather than the tree it
/// tested.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct HostCondition {
    /// The name a census gives it.
    face: &'static str,
    /// Lowercase spellings that carry it, searched for over the whole excerpt.
    spellings: &'static [&'static str],
    /// What the failures sharing it must span to read as the host.
    spread: Spread,
}

/// What [`HOST_CONDITION_MIN`] counts for a [`HostCondition`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Spread {
    /// Tests. An OS error names the machine wherever it lands, so two tests
    /// in one file hitting a full disk are already the disk.
    Tests,
    /// Test files. A time budget expiring is the machine's load only when it
    /// expires in unrelated places: several timeouts in one file may be that
    /// file's own hang, which is a red to look at.
    Files,
}

/// The host conditions a census recognizes. The OS errors carry Rust's
/// `Display` and `Debug` of the error and SQLite's message. They are searched
/// for over the whole excerpt, not the signature: a failure that wraps the
/// error (`Custom { kind: StorageFull, error: PathError { path:
/// "/tmp/cairn-change-project-2jgZmE", .. } }`) carries a per-test temp path
/// on its signature line, so no two of them share a signature.
///
/// The timeout is vitest's (`Error: Test timed out in 5000ms.`). On a loaded
/// host it lands in a rotating set of I/O-bound files, main's included, so the
/// same files pass on the same host one tip earlier. The frontend runner
/// rewrites every vitest timeout row to open with its `VITEST_TIMEOUT_EXCERPT`
/// (`scripts/lib/structured-test-result.ts`), because vitest 4's JSON report
/// carries a timeout only as `Error: STACK_TRACE_ERROR`; that excerpt is the
/// second spelling, and the two must change together.
const HOST_CONDITIONS: &[HostCondition] = &[
    HostCondition {
        face: "No space left on device",
        spellings: &["no space left on device", "storagefull", "(os error 28)"],
        spread: Spread::Tests,
    },
    HostCondition {
        face: "Text file busy",
        spellings: &["text file busy", "executablefilebusy", "(os error 26)"],
        spread: Spread::Tests,
    },
    HostCondition {
        face: "database is locked",
        spellings: &["database is locked"],
        spread: Spread::Tests,
    },
    HostCondition {
        face: "Test timed out",
        spellings: &["test timed out in ", "timeout: vitest terminated this test"],
        spread: Spread::Files,
    },
];

/// How many failures (or, for a [`Spread::Files`] condition, test files) in
/// one row must share a host condition before they are read as a census of
/// the host rather than as tests. One test that hit a full disk is still a
/// red to look at; a run of them is the machine.
const HOST_CONDITION_MIN: usize = 2;

/// The host condition a failure excerpt names: see [`HOST_CONDITIONS`].
fn host_condition(excerpt: &str) -> Option<&'static HostCondition> {
    let excerpt = excerpt.to_ascii_lowercase();
    HOST_CONDITIONS.iter().find(|condition| {
        condition
            .spellings
            .iter()
            .any(|spelling| excerpt.contains(spelling))
    })
}

impl HostCondition {
    /// How many of the condition's [`Spread`] `tests` hold.
    fn breadth(&self, tests: &[&str]) -> usize {
        match self.spread {
            Spread::Tests => tests.len(),
            Spread::Files => files(tests),
        }
    }
}

/// How a bound ended a test rather than what it asserted: see
/// [`BOUND_ENDINGS`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BoundEnding {
    /// A time bound expired while the test ran.
    Timeout,
    /// A batch the test ran was refused because no executor was there to run
    /// it, so the test failed on the refusal in the reply it asserted on.
    Refused,
}

/// The fleet's refusal when the runner has no colocated executor to place a
/// batch on (`fleet::NO_COLOCATED_EXECUTOR`), as a test that asserted on the
/// batch's reply carries it.
const REFUSED_NO_COLOCATED_EXECUTOR: &str =
    "no colocated executor is configured, enrolled, or being supervised";

/// Lowercase spellings of a test ended by a bound rather than by what it
/// asserted, each with the kind of ending it is.
///
/// Time bounds: nextest's slow-timeout termination as the rust-tests runner
/// spells it (`NEXTEST_TIMEOUT_EXCERPT` in scripts/lib/structured-test-result.ts,
/// which must change with it), vitest's timeout and the frontend runner's
/// rewrite of it, and tokio's `Elapsed` (its `Debug`, then its `Display`) from
/// a test's own bounded wait.
///
/// Refusals: [`REFUSED_NO_COLOCATED_EXECUTOR`], the fleet's placement refusal
/// when no colocated executor exists. The ledger keeps every row's excerpt as
/// it was recorded, so when the fleet's text changes its new spelling joins
/// this list beside the old one rather than replacing it; a unit test fails
/// until it does.
///
/// A host that stops scheduling ends every test in flight at whichever time
/// bound each carries, and a host whose executor is gone refuses every batch
/// in flight, so these are what [`stall_windows`] dates.
const BOUND_ENDINGS: &[(&str, BoundEnding)] = &[
    (
        "nextest terminated this test at its slow-timeout bound",
        BoundEnding::Timeout,
    ),
    ("test timed out in ", BoundEnding::Timeout),
    ("timeout: vitest terminated this test", BoundEnding::Timeout),
    ("elapsed(())", BoundEnding::Timeout),
    ("deadline has elapsed", BoundEnding::Timeout),
    ("no progress within ", BoundEnding::Timeout),
    (REFUSED_NO_COLOCATED_EXECUTOR, BoundEnding::Refused),
];

/// The bound a failure excerpt says ended the test, if one did: see
/// [`BOUND_ENDINGS`].
fn bound_ending(excerpt: &str) -> Option<BoundEnding> {
    let excerpt = excerpt.to_ascii_lowercase();
    BOUND_ENDINGS
        .iter()
        .find(|(spelling, _)| excerpt.contains(spelling))
        .map(|(_, ending)| *ending)
}

/// The test file a per-test id names. A vitest id is `<file> > <full name>`
/// (and a file that failed to collect is the bare file). A nextest id is
/// `<crate>::<binary>$<module path>::<name>`, and its module stands for its
/// file (`cairn-core::main$turso_sync_roundtrip`): a crate's integration-test
/// files all build into one `main` binary, so the binary would make them one
/// file. Any other id is its own file.
fn test_file(test: &str) -> &str {
    if let Some((file, _)) = test.split_once(" > ") {
        return file;
    }
    match test.split_once('$') {
        Some((binary, path)) => match path.rfind("::") {
            Some(at) => &test[..binary.len() + 1 + at],
            None => binary,
        },
        None => test,
    }
}

/// How many test files `tests` span: see [`test_file`].
fn files(tests: &[&str]) -> usize {
    tests
        .iter()
        .map(|test| test_file(test))
        .collect::<HashSet<_>>()
        .len()
}

// ---------------------------------------------------------------------------
// Bracket
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BracketMerge {
    pub(crate) commit: String,
    pub(crate) subject: String,
    pub(crate) pr: Option<i64>,
    /// Canonical issue URI shared by report rendering and owner resolution.
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

/// The first `{key}/N` issue mention in a subject, rendered as a canonical issue URI.
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
            return Some(format!("cairn://p/{key}/{digits}"));
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
    let jj = match crate::jj::environment(&orch.jj_binary_path, &orch.config_dir)
        .pin_immutable_read(repository, &[base, tip])
    {
        Ok(jj) => jj,
        Err(error) => {
            return Bracket {
                unavailable: Some(error),
                ..Bracket::default()
            }
        }
    };
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
    bracket_from_log(db, project_id, project_key, &output, base, tip).await
}

async fn bracket_from_log(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    output: &str,
    base: &str,
    tip: &str,
) -> Bracket {
    let entries = parse_log(output);
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
                .map(|number| format!("cairn://p/{key}/{number}")),
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
    /// What the lane's verdict is made of, which decides how a row reads.
    pub(crate) kind: LaneVerdict,
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

/// What a row measured: a per-test lane's totals, or an exit lane's verdict and
/// exit code.
fn totals(row: &CadenceRow, kind: LaneVerdict) -> String {
    match kind {
        LaneVerdict::PerTest => format!(
            "{} ran · {} passed · {} failed · {} skipped",
            thousands(row.ran()),
            thousands(row.passed()),
            thousands(row.failed()),
            thousands(row.skipped())
        ),
        LaneVerdict::Exit => format!(
            "{} (exit {})",
            if row.verdict_passed {
                "passed"
            } else {
                "failed"
            },
            row.exit_code
        ),
    }
}

/// A complete row's verdict as a headline clause.
fn verdict_clause(row: &CadenceRow, kind: LaneVerdict) -> String {
    match kind {
        LaneVerdict::PerTest if row.failed() == 0 => format!("all {} passed", thousands(row.ran())),
        LaneVerdict::PerTest => format!("{} failed", thousands(row.failed())),
        LaneVerdict::Exit if row.verdict_passed => "passed".to_string(),
        LaneVerdict::Exit => format!("failed (exit {})", row.exit_code),
    }
}

/// A complete row's headline: the plan audit leads a red it gates, so a green
/// test tally never fronts one, and follows the tests when it could not judge
/// them, because their verdict stands without it.
fn headline_clause(row: &CadenceRow, kind: LaneVerdict) -> String {
    let verdict = verdict_clause(row, kind);
    match &row.plan_audit {
        Some(audit) if !audit.evaluated => format!(
            "{verdict}; plan audit: not evaluated ({})",
            audit.note.as_deref().unwrap_or("no reason recorded")
        ),
        Some(_) if row.audit_gating() > 0 => {
            format!("plan audit: {} gating; {verdict}", row.audit_gating())
        }
        _ => verdict,
    }
}

/// An attempt's outcome as the project overview's clause.
fn outcome_summary(kind: LaneVerdict, verdict: &CadenceVerdict) -> String {
    match verdict {
        CadenceVerdict::Complete(row) => headline_clause(row, kind),
        CadenceVerdict::NoVerdict { .. } => "did not complete (no-verdict)".to_string(),
        CadenceVerdict::Death { .. } => "did not complete (death)".to_string(),
        CadenceVerdict::Cancelled { .. } => "cancelled by restart".to_string(),
        CadenceVerdict::AwaitingAdoption { machine, request_id, stopped_at_unix_ms } => format!(
            "the runner stopped at {} while this lane ran on {machine} (request {request_id}); awaiting adoption",
            stamp(*stopped_at_unix_ms)
        ),
    }
}

fn row_identity(out: &mut String, row: &CadenceRow, kind: LaneVerdict) {
    let label = match kind {
        LaneVerdict::PerTest => "Totals",
        LaneVerdict::Exit => "Verdict",
    };
    out.push_str(&format!(
        "- Tree: commit `{}` · platform `{}` · executor `{}`\n- Observation: {}\n- {label}: {}\n",
        row.commit,
        row.platform,
        row.executor.as_deref().unwrap_or("unknown"),
        row.reference,
        totals(row, kind)
    ));
}

fn render_output_tail(out: &mut String, row: &CadenceRow) {
    let tail = row.output_tail.trim();
    if !tail.is_empty() {
        out.push_str(&format!(
            "\nOutput tail:\n```\n{}\n```\n",
            crate::execution::check_parsers::anchored_tail(tail, MAX_CAUSE_CHARS)
                .replace("```", "'''")
        ));
    }
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

/// Failures in one row that share one host condition: the host's census, not
/// tests.
#[derive(Debug)]
struct HostConditionGroup<'a> {
    cause: HostCause,
    tests: Vec<&'a str>,
}

/// What puts a group's failures on the machine rather than the tree.
#[derive(Debug)]
enum HostCause {
    /// One error naming the machine, in every excerpt.
    Error(&'static HostCondition),
    /// Runs that overlapped and ended at their bounds: see [`stall_windows`].
    Stall(StallWindow),
}

/// Where on the lane's timeline a stall group's tests ran.
#[derive(Debug)]
struct StallWindow {
    /// The earliest start and latest end of the tests that ended at a bound,
    /// as lane offsets.
    from_ms: i64,
    to_ms: i64,
    /// The span every one of them was in flight through, when they shared one.
    together: Option<(i64, i64)>,
    /// The group's first `bounded` tests ended at a bound; the rest failed some
    /// other way while running through the window.
    bounded: usize,
    /// How many of the `bounded` were refused rather than timed out.
    refused: usize,
}

impl HostConditionGroup<'_> {
    /// The clause the report's headline gives the group.
    fn headline(&self) -> String {
        let count = self.tests.len();
        match &self.cause {
            HostCause::Error(condition) => {
                format!(
                    "{count} tests failed on one host condition: {}",
                    condition.face
                )
            }
            HostCause::Stall(window) => format!(
                "{count} tests failed inside one stall window, {}",
                lane_span(window.from_ms, window.to_ms, None)
            ),
        }
    }

    /// What the group's section is titled, on `row`'s timeline.
    fn title(&self, row: &CadenceRow) -> String {
        let count = self.tests.len();
        match &self.cause {
            HostCause::Error(condition) => {
                format!("{count} tests failed on `{}`", condition.face)
            }
            HostCause::Stall(window) => format!(
                "{count} tests failed inside one stall window, {}",
                lane_span(window.from_ms, window.to_ms, row.lane_started_at_ms)
            ),
        }
    }

    /// Why the group is the machine rather than the tree.
    fn reading(&self) -> String {
        let count = self.tests.len();
        match &self.cause {
            HostCause::Error(condition) => match condition.spread {
                Spread::Tests => format!(
                    "one OS error across {count} tests is a condition of this machine, not of the tree"
                ),
                Spread::Files => format!(
                    "one time budget expiring across {count} tests in {} unrelated files is this machine's load, not the tree",
                    files(&self.tests)
                ),
            },
            HostCause::Stall(window) => {
                let (ended, machine) = match window.refused {
                    0 => ("ran into a time bound".to_string(), "stalling"),
                    refused if refused == window.bounded => (
                        format!("were refused with `{REFUSED_NO_COLOCATED_EXECUTOR}`"),
                        "losing its executor",
                    ),
                    refused => (
                        format!(
                            "ran into a time bound or were refused with `{REFUSED_NO_COLOCATED_EXECUTOR}` ({refused} refused)"
                        ),
                        "stalling",
                    ),
                };
                let mut reading = format!(
                    "{} tests in {} unrelated files {ended} while their runs overlapped, which is this machine {machine}, not the tree",
                    window.bounded,
                    files(&self.tests[..window.bounded])
                );
                let others = count - window.bounded;
                if others > 0 {
                    reading.push_str(&format!(
                        "; {others} more failed while running through the same window"
                    ));
                }
                reading
            }
        }
    }

    /// Largest first, then error conditions by name before stalls by start.
    fn order(&self) -> (std::cmp::Reverse<usize>, bool, &'static str, i64) {
        let size = std::cmp::Reverse(self.tests.len());
        match &self.cause {
            HostCause::Error(condition) => (size, false, condition.face, 0),
            HostCause::Stall(window) => (size, true, "", window.from_ms),
        }
    }
}

/// The stall windows of a row: failures that ended at a bound, a time bound
/// or a refusal ([`BOUND_ENDINGS`]), whose runs overlapped one another, in at least
/// [`HOST_CONDITION_MIN`] test files, so the host stopped running them rather
/// than one file hanging on itself. Each window then also holds every other
/// failure whose run overlapped it: a test that failed its assertion after the
/// machine froze around it carries no error naming the freeze, and its start is
/// the only evidence that ties it to one. `taken` are the failures an error
/// condition already holds.
fn stall_windows<'a>(row: &'a CadenceRow, taken: &HashSet<&str>) -> Vec<HostConditionGroup<'a>> {
    let timed: Vec<(&str, &Failure, Span)> = row
        .tests
        .iter()
        .filter_map(|(test, outcome)| match outcome {
            TestOutcome::Failed(failure) if !taken.contains(test.as_str()) => {
                failure.span.map(|span| (test.as_str(), failure, span))
            }
            _ => None,
        })
        .collect();
    let mut bounded: Vec<(&str, Span, BoundEnding)> = timed
        .iter()
        .filter_map(|(test, failure, span)| failure.ending.map(|ending| (*test, *span, ending)))
        .collect();
    bounded.sort_by_key(|(test, span, _)| (span.start_ms, *test));
    // Maximal runs of transitively overlapping spans, so two windows never
    // overlap each other.
    let mut clusters: Vec<Vec<(&str, Span, BoundEnding)>> = Vec::new();
    let mut reach = i64::MIN;
    for (test, span, ending) in bounded {
        match clusters.last_mut() {
            Some(cluster) if span.start_ms <= reach => cluster.push((test, span, ending)),
            _ => clusters.push(vec![(test, span, ending)]),
        }
        reach = reach.max(span.end_ms);
    }
    let mut joined: HashSet<&str> = HashSet::new();
    let mut windows = Vec::new();
    for cluster in clusters {
        let mut tests: Vec<&str> = cluster.iter().map(|(test, _, _)| *test).collect();
        if files(&tests) < HOST_CONDITION_MIN {
            continue;
        }
        let refused = cluster
            .iter()
            .filter(|(_, _, ending)| *ending == BoundEnding::Refused)
            .count();
        let starts = cluster.iter().map(|(_, span, _)| span.start_ms);
        let ends = cluster.iter().map(|(_, span, _)| span.end_ms);
        let (from_ms, latest_start) = (starts.clone().min(), starts.max());
        let (earliest_end, to_ms) = (ends.clone().min(), ends.max());
        let (Some(from_ms), Some(latest_start), Some(earliest_end), Some(to_ms)) =
            (from_ms, latest_start, earliest_end, to_ms)
        else {
            continue;
        };
        let bounded = tests.len();
        for &(test, failure, span) in &timed {
            if failure.ending.is_none() && span.overlaps(from_ms, to_ms) && joined.insert(test) {
                tests.push(test);
            }
        }
        windows.push(HostConditionGroup {
            cause: HostCause::Stall(StallWindow {
                from_ms,
                to_ms,
                together: (latest_start <= earliest_end).then_some((latest_start, earliest_end)),
                bounded,
                refused,
            }),
            tests,
        });
    }
    windows
}

/// A lane offset as `1h 02m 05s`, `12m 05s`, or `45s`.
fn lane_offset(ms: i64) -> String {
    let seconds = (ms.max(0) + 500) / 1_000;
    let (hours, minutes, seconds) = (seconds / 3_600, seconds / 60 % 60, seconds % 60);
    if hours > 0 {
        format!("{hours}h {minutes:02}m {seconds:02}s")
    } else if minutes > 0 {
        format!("{minutes}m {seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

/// `52m 10s to 1h 03m 14s into the lane`, and the wall clock when the lane's
/// origin is known.
fn lane_span(from_ms: i64, to_ms: i64, origin_ms: Option<i64>) -> String {
    let mut text = format!(
        "{} to {} into the lane",
        lane_offset(from_ms),
        lane_offset(to_ms)
    );
    if let Some(origin) = origin_ms {
        text.push_str(&format!(
            " ({} to {})",
            stamp(origin.saturating_add(from_ms)),
            stamp(origin.saturating_add(to_ms))
        ));
    }
    text
}

/// ` · started 52m 10s, ran 11m 04s` for a failure whose row dated it.
fn ran_at(row: &CadenceRow, test: &str) -> String {
    match row.tests.get(test) {
        Some(TestOutcome::Failed(Failure {
            span: Some(span), ..
        })) => format!(
            " · started {}, ran {}",
            lane_offset(span.start_ms),
            lane_offset(span.end_ms - span.start_ms)
        ),
        _ => String::new(),
    }
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
    /// The still-red failures of each group sharing a host condition's error
    /// that did not read as the host. Counted in `still_red` too; these are
    /// the ones it lists.
    shared_still_red: Vec<SharedRed<'a>>,
    /// Plan-audit findings by the same three questions, over `failed` and
    /// `stale` rows. Empty unless both rows evaluated their audit: a row that
    /// judged nothing cannot have moved a finding either way.
    audit_newly_red: Vec<&'a str>,
    audit_newly_green: Vec<&'a str>,
    audit_still_red: usize,
}

/// What a row said of its audit: `None` for no audit, else whether it judged.
fn audit_state(row: &CadenceRow) -> Option<bool> {
    row.plan_audit.as_ref().map(|audit| audit.evaluated)
}

/// Fill `diff`'s audit fields. Against no row at all (a baseline) every gating
/// finding is newly red, as every failure is.
fn diff_audit<'a>(
    diff: &mut RowDiff<'a>,
    current: &'a CadenceRow,
    previous: Option<&'a CadenceRow>,
) {
    let Some(audit) = current.plan_audit.as_ref().filter(|audit| audit.evaluated) else {
        return;
    };
    let before = match previous {
        None => None,
        Some(previous) => match previous.plan_audit.as_ref().filter(|audit| audit.evaluated) {
            Some(before) => Some(before),
            None => return,
        },
    };
    for (finding, _) in audit.gating() {
        if before.is_some_and(|before| before.gates(finding)) {
            diff.audit_still_red += 1;
        } else {
            diff.audit_newly_red.push(finding);
        }
    }
    if let Some(before) = before {
        diff.audit_newly_green = before
            .gating()
            .map(|(finding, _)| finding.as_str())
            .filter(|finding| !audit.gates(finding))
            .collect();
    }
}

/// Failures that share one host condition's error, red at the compared row
/// too, whose group the discriminator refused: an intermittent in the tree,
/// listed under the error they share.
#[derive(Debug)]
struct SharedRed<'a> {
    condition: &'static HostCondition,
    tests: Vec<&'a str>,
    /// The window's other host, when it failed the same tests on the same error.
    elsewhere: Option<&'a CadenceRow>,
}

/// Whether `row` failed `test` on `condition`'s error.
fn failed_on(row: &CadenceRow, test: &str, condition: &HostCondition) -> bool {
    matches!(
        row.tests.get(test),
        Some(TestOutcome::Failed(Failure {
            host_condition: Some(other),
            ..
        })) if other.face == condition.face
    )
}

/// The diff of `current` against the comparison's row, with the failures the
/// host explains held apart.
///
/// Failures sharing a host condition's error read as the host only when the
/// discriminator holds: at least [`HOST_CONDITION_MIN`] of them (by the
/// condition's [`Spread`]) passed on this host at the compared row, and the
/// window's other host failed none of them on the same error. Otherwise the
/// error travels with the tree, not the machine (cairn/5888: four `database is
/// locked` reds on fedora were red there one tip earlier and on bgsrv-mac in
/// between), so the group's failures are ordinary reds signed by the error they
/// share. Without a row on this host there is nothing to separate the host from
/// the tree, and no group reads as the host; nor when the lookup for the
/// window's other host or the same tree's peer failed, since the other host
/// may have failed them too. Either way a failure already red at the compared
/// row is never held under a host label: it is still red.
fn row_diff<'a>(current: &'a CadenceRow, comparison: &'a Comparison) -> RowDiff<'a> {
    let previous = comparison.previous();
    let (same_host, neighbour, peer, others_read) = match comparison {
        Comparison::SameExecutor {
            previous,
            neighbour,
            peer,
            unread,
        } => (
            Some(previous),
            neighbour.as_deref(),
            peer.as_deref(),
            !unread.any(),
        ),
        _ => (None, None, None, false),
    };
    let red_before =
        |test: &str| previous.is_some_and(|previous| passed_in(previous, test) == Some(false));
    let mut diff = RowDiff::default();
    let mut faces: BTreeMap<&'static str, (&'static HostCondition, Vec<&str>)> = BTreeMap::new();
    for (test, outcome) in &current.tests {
        if let TestOutcome::Failed(Failure {
            host_condition: Some(condition),
            ..
        }) = outcome
        {
            faces
                .entry(condition.face)
                .or_insert_with(|| (condition, Vec::new()))
                .1
                .push(test.as_str());
        }
    }
    faces.retain(|_, (condition, tests)| condition.breadth(tests) >= HOST_CONDITION_MIN);
    // Failures signed by the error they share rather than by their own line.
    let mut shared: HashMap<&str, &'static str> = HashMap::new();
    for (condition, tests) in faces.into_values() {
        let passed_here_before: Vec<&str> = tests
            .iter()
            .copied()
            .filter(|test| same_host.is_some_and(|row| passed_in(row, test) == Some(true)))
            .collect();
        let elsewhere = [peer, neighbour]
            .into_iter()
            .flatten()
            .find(|row| tests.iter().any(|test| failed_on(row, test, condition)));
        if others_read
            && elsewhere.is_none()
            && condition.breadth(&passed_here_before) >= HOST_CONDITION_MIN
        {
            diff.host_conditions.push(HostConditionGroup {
                cause: HostCause::Error(condition),
                tests: tests.into_iter().filter(|test| !red_before(test)).collect(),
            });
            continue;
        }
        shared.extend(tests.iter().map(|test| (*test, condition.face)));
        let still: Vec<&str> = tests.into_iter().filter(|test| red_before(test)).collect();
        if !still.is_empty() {
            diff.shared_still_red.push(SharedRed {
                condition,
                tests: still,
                elsewhere,
            });
        }
    }
    if same_host.is_some() {
        let mut taken: HashSet<&str> = diff
            .host_conditions
            .iter()
            .flat_map(|group| group.tests.iter().copied())
            .chain(shared.keys().copied())
            .collect();
        taken.extend(
            current
                .tests
                .keys()
                .map(String::as_str)
                .filter(|test| red_before(test)),
        );
        diff.host_conditions.extend(stall_windows(current, &taken));
    }
    let on_host: HashSet<&str> = diff
        .host_conditions
        .iter()
        .flat_map(|group| group.tests.iter().copied())
        .collect();
    diff.host_conditions
        .sort_by(|a, b| a.order().cmp(&b.order()));
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
                let signature = shared
                    .get(test)
                    .copied()
                    .unwrap_or(failure.signature.as_str());
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
    diff_audit(&mut diff, current, previous);
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

/// Tests that differ from another host's row on the same tree while agreeing
/// with this host's previous row: a difference between the hosts, not
/// movement in the tree.
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
    peer: &CadenceRow,
) -> HostCensus<'a> {
    let mut census = HostCensus::default();
    for test in current.tests.keys() {
        let Some(here) = passed_in(current, test) else {
            continue;
        };
        if passed_in(peer, test).is_none_or(|there| there == here)
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

/// The one-host tag on a red group: how many of its tests passed on another
/// host's row on the same tree.
fn one_host(tests: &[&str], peer: Option<&CadenceRow>) -> String {
    let Some(peer) = peer else {
        return String::new();
    };
    let passed = tests
        .iter()
        .filter(|test| passed_in(peer, test) == Some(true))
        .count();
    if passed == 0 {
        String::new()
    } else if passed == tests.len() {
        format!(" · **one host**: passed on `{}`", host(peer))
    } else {
        format!(
            " · **one host** for {passed} of {}: passed on `{}`",
            tests.len(),
            host(peer)
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

fn render_groups(out: &mut String, groups: &[(&str, Vec<&str>)], peer: Option<&CadenceRow>) {
    for (signature, tests) in groups.iter().take(MAX_GROUPS) {
        out.push_str(&format!(
            "- **{} test{}**{} — `{}`\n",
            tests.len(),
            plural(tests.len()),
            one_host(tests, peer),
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
            "\n**HOST CONDITION on `{}`: {}** — {}, so none of them is counted as newly red or still red.\n",
            host(row),
            group.title(row),
            group.reading()
        ));
        if let HostCause::Stall(StallWindow {
            together: Some((from, to)),
            bounded,
            ..
        }) = &group.cause
        {
            out.push_str(&format!(
                "- All {bounded} that ended at a bound were in flight together from {}.\n",
                lane_span(*from, *to, row.lane_started_at_ms)
            ));
        }
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
            out.push_str(&format!("  - `{test}`{}\n", ran_at(row, test)));
        }
        if count > MAX_IDS_PER_GROUP {
            out.push_str(&format!("  - … and {} more\n", count - MAX_IDS_PER_GROUP));
        }
    }
}

/// Each still-red group sharing a host condition's error that the
/// discriminator refused: the error, what put it on the tree, and its tests.
fn render_shared_reds(
    out: &mut String,
    row: &CadenceRow,
    groups: &[SharedRed<'_>],
    previous: &CadenceRow,
) {
    for group in groups {
        let count = group.tests.len();
        let mut evidence = format!("red on `{}` at the compared row too", host(previous));
        if let Some(elsewhere) = group.elsewhere {
            evidence.push_str(&format!(
                " and on `{}` in the same window ({})",
                host(elsewhere),
                elsewhere.reference
            ));
        }
        out.push_str(&format!(
            "- **{count} test{}** — `{}` · {evidence}, so it is an intermittent in the tree, not a host condition on `{}`\n",
            plural(count),
            group.condition.face,
            host(row)
        ));
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
    let (baseline, cross_host) = match report.kind {
        LaneVerdict::PerTest => (
            "so this row is the baseline and every failure is listed as newly red",
            "what differs below may be the two hosts (provisioning, load, toolchain) rather than the tree, and none of it is reported as newly red or green",
        ),
        LaneVerdict::Exit => (
            "so this row is the baseline",
            "a verdict that differs may be the two hosts (provisioning, load, toolchain) rather than the tree",
        ),
    };
    let line = match report.comparison {
        Comparison::Baseline => format!(
            "nothing. No earlier complete whole-suite row on main exists for {} on {}, {baseline}.",
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
            "{}, the {} (commit `{}`, ran {} earlier). No earlier complete row exists on `{}`, so this is a **cross-host comparison**: {cross_host}.",
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
        Comparison::Unavailable { cause, known } => {
            let mut line = format!(
                "unknown. {cause}, so this row is not a baseline: nothing below is reported as moved, and no merges are listed since a compared row."
            );
            if let Some(known) = known {
                line.push_str(&format!(
                    " History exists: the newest complete row on {} is {}, the {} (commit `{}`, ran {} earlier), not compared with because this host's own previous row could not be read.",
                    report.platform,
                    known.reference,
                    armed(known),
                    short_commit(&known.commit),
                    age(row.ran_at_ms, known.ran_at_ms)
                ));
            }
            line
        }
    };
    out.push_str(&format!("- Compared with: {line}\n"));
}

/// The "Same tree on another host" line: the row host claims compare with,
/// or, when no other host has run this commit, that none can be made, with
/// the nearest other host's row placed by its commit and the merges between.
fn render_peer(out: &mut String, report: &CadenceReport<'_>, row: &CadenceRow, reds: usize) {
    let Comparison::SameExecutor {
        previous,
        neighbour,
        peer,
        unread,
    } = report.comparison
    else {
        return;
    };
    if let Some(cause) = &unread.peer {
        out.push_str(&format!(
            "- Same tree on another host: unknown. {cause}, so no red below is attributed to a host.\n"
        ));
    }
    if let Some(cause) = &unread.neighbour {
        out.push_str(&format!(
            "- Other hosts in the window: unknown. {cause}, so no red below is attributed to a host.\n"
        ));
    }
    if unread.any() {
        return;
    }
    if let Some(peer) = peer {
        out.push_str(&format!(
            "- Same tree on another host: {}, the {} (ran {} earlier). Host differences below are measured against it.\n",
            peer.reference,
            armed(peer),
            age(row.ran_at_ms, peer.ran_at_ms)
        ));
        return;
    }
    if reds == 0 {
        return;
    }
    let mut line = format!(
        "- Same tree on another host: none. No other host has run commit `{}` on {}, so no red below is attributed to a host.",
        short_commit(&row.commit),
        report.platform
    );
    if let Some(neighbour) = neighbour {
        let gap = if neighbour.commit == previous.commit {
            Some(report.bracket.merges.len())
        } else {
            report
                .bracket
                .merges
                .iter()
                .position(|merge| merge.commit == neighbour.commit)
                .map(|at| report.bracket.merges.len() - 1 - at)
        };
        let gap = match gap {
            Some(count) => format!(
                "{count} merge{} earlier (listed under **Merges since the compared row**)",
                plural(count)
            ),
            None => "a different tree".to_string(),
        };
        line.push_str(&format!(
            " The nearest other host's row, {} on `{}`, ran commit `{}`, {gap}; a test it passed may have been broken by a merge in between, not by this host.",
            neighbour.reference,
            host(neighbour),
            short_commit(&neighbour.commit)
        ));
    }
    out.push_str(&line);
    out.push('\n');
}

fn render_host_census(
    out: &mut String,
    row: &CadenceRow,
    previous: &CadenceRow,
    peer: &CadenceRow,
) {
    let census = host_census(row, previous, peer);
    let total = census.red_here.len() + census.green_here.len();
    if total == 0 {
        return;
    }
    let (here, there) = (host(row), host(peer));
    out.push_str(&format!(
        "\n**HOST CENSUS against `{there}`: {total}** — these differ from {} (`{there}`, the same commit `{}`, ran {} earlier) but match this host's previous row, so they are a difference between the hosts, not movement in the tree.\n",
        peer.reference,
        short_commit(&peer.commit),
        age(row.ran_at_ms, peer.ran_at_ms)
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

/// An exit lane's complete row: its verdict, how it moved from the compared
/// row's, the output a red one printed, and the merges between the two rows.
fn render_exit_complete(out: &mut String, report: &CadenceReport<'_>, row: &CadenceRow) {
    let previous = report.comparison.previous();
    let movement = match previous {
        None if matches!(report.comparison, Comparison::Unavailable { .. }) => {
            "movement unknown: the compared row could not be read".to_string()
        }
        None => "baseline".to_string(),
        Some(previous) => {
            let moved = match (previous.verdict_passed, row.verdict_passed) {
                (true, false) => "newly red",
                (false, false) => "still red",
                (false, true) => "newly green",
                (true, true) => "still green",
            };
            match report.comparison {
                Comparison::OtherExecutor { previous } => {
                    format!("{moved}, cross-host against `{}`", host(previous))
                }
                _ => moved.to_string(),
            }
        }
    };
    out.push_str(&format!(
        "**Main cadence · {} · {} on `{}` · `{}`: {} — {movement}**\n\n",
        report.check,
        report.platform,
        host(row),
        short_commit(&row.commit),
        verdict_clause(row, LaneVerdict::Exit)
    ));
    row_identity(out, row, LaneVerdict::Exit);
    render_basis(out, report, row);
    if !row.verdict_passed {
        render_output_tail(out, row);
    }
    if previous.is_some() {
        render_bracket(out, "Merges since the compared row", report.bracket);
    }
}

fn render_complete(out: &mut String, report: &CadenceReport<'_>, row: &CadenceRow) {
    if report.kind == LaneVerdict::Exit {
        return render_exit_complete(out, report, row);
    }
    if let Comparison::Unavailable { .. } = report.comparison {
        return render_uncompared(out, report, row);
    }
    let previous = report.comparison.previous();
    let (neighbour, peer, cross_host) = match report.comparison {
        Comparison::SameExecutor {
            neighbour, peer, ..
        } => (neighbour.as_deref(), peer.as_deref(), None),
        Comparison::OtherExecutor { previous } => (None, None, Some(host(previous))),
        Comparison::Baseline | Comparison::AnyExecutor { .. } | Comparison::Unavailable { .. } => {
            (None, None, None)
        }
    };
    let diff = row_diff(row, report.comparison);
    let newly_red = group_size(&diff.newly_red);
    let first_red = group_size(&diff.first_red);
    let mut headline = headline_clause(row, LaneVerdict::PerTest);
    if !diff.host_conditions.is_empty() {
        let conditions: Vec<String> = diff
            .host_conditions
            .iter()
            .map(HostConditionGroup::headline)
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
    if diff.audit_newly_red.len() + diff.audit_newly_green.len() > 0 {
        movement.push_str(&format!(
            " · plan audit: {} newly red, {} newly green",
            diff.audit_newly_red.len(),
            diff.audit_newly_green.len()
        ));
    }
    out.push_str(&format!(
        "**Main cadence · {} · {} on `{}` · `{}`: {headline} — {movement}**\n\n",
        report.check,
        report.platform,
        host(row),
        short_commit(&row.commit)
    ));
    row_identity(out, row, LaneVerdict::PerTest);
    render_basis(out, report, row);
    render_peer(out, report, row, newly_red + first_red);
    render_host_conditions(out, row, &diff.host_conditions, previous, neighbour);
    render_plan_audit(out, row, Some(&diff));
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
        render_groups(out, &diff.newly_red, peer);
    }
    if first_red > 0 {
        out.push_str(&format!(
            "\n**FIRST OBSERVATION ON THIS HOST: RED · {first_red} test{} in {} failure group{}** — no row in the compared observation, so there is no earlier state to have moved from. A merge listed under **Merges since the compared row** added or renamed {}.\n",
            plural(first_red),
            diff.first_red.len(),
            plural(diff.first_red.len()),
            if first_red == 1 { "it" } else { "them" }
        ));
        render_groups(out, &diff.first_red, peer);
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
    if let Some(previous) = previous {
        let label = if cross_host.is_some() {
            "Red on both hosts"
        } else {
            "Still red"
        };
        let listed: usize = diff
            .shared_still_red
            .iter()
            .map(|group| group.tests.len())
            .sum();
        let listing = match listed {
            0 => "not listed".to_string(),
            all if all == diff.still_red => "listed below".to_string(),
            some => format!("{some} listed below"),
        };
        out.push_str(&format!("\n**{label}:** {} ({listing})\n", diff.still_red));
        render_shared_reds(out, row, &diff.shared_still_red, previous);
    }
    if let Comparison::SameExecutor {
        previous,
        peer: Some(peer),
        ..
    } = report.comparison
    {
        render_host_census(out, row, previous, peer);
    }
    if previous.is_some() {
        render_bracket(out, "Merges since the compared row", report.bracket);
    }
}

/// A per-test row whose compared row could not be looked up: its verdict and
/// its failures as measured, with no claim about how either moved.
fn render_uncompared(out: &mut String, report: &CadenceReport<'_>, row: &CadenceRow) {
    // Measured against no row, every failure lands in `newly_red`: here that
    // is only the row's failures, grouped by signature, and none is called new.
    let nothing = Comparison::Baseline;
    let reds = row_diff(row, &nothing).newly_red;
    out.push_str(&format!(
        "**Main cadence · {} · {} on `{}` · `{}`: {} — movement unknown: the compared row could not be read**\n\n",
        report.check,
        report.platform,
        host(row),
        short_commit(&row.commit),
        headline_clause(row, LaneVerdict::PerTest)
    ));
    row_identity(out, row, LaneVerdict::PerTest);
    render_basis(out, report, row);
    render_plan_audit(out, row, None);
    let count = group_size(&reds);
    if count > 0 {
        out.push_str(&format!(
            "\n**RED IN THIS ROW: {count} test{} in {} failure group{}** — whether each is new or was already red is unknown without the compared row.\n",
            plural(count),
            reds.len(),
            plural(reds.len())
        ));
        render_groups(out, &reds, None);
    }
}

/// The audit's section of a report: every gating finding by name, however many,
/// because the identity is what a reader acts on. Without `diff` (no compared
/// row was read) the findings are listed with no movement.
fn render_plan_audit(out: &mut String, row: &CadenceRow, diff: Option<&RowDiff<'_>>) {
    let Some(audit) = &row.plan_audit else {
        return;
    };
    if !audit.evaluated {
        out.push_str(&format!(
            "\n**Plan audit: not evaluated** ({}). The tests' verdict stands without it. This row is not a clean audit, and it moved no finding either way.\n",
            audit.note.as_deref().unwrap_or("no reason recorded")
        ));
        return;
    }
    let gating = row.audit_gating();
    if gating > 0 {
        let movement = diff.map_or_else(
            || "movement unknown".to_string(),
            |diff| {
                format!(
                    "{} newly red, {} still red",
                    diff.audit_newly_red.len(),
                    diff.audit_still_red
                )
            },
        );
        out.push_str(&format!(
            "\n**PLAN AUDIT: {gating} gating finding{}** ({movement}). A finding is a statement that walks a wide row; its identity is file, rule, table and statement hash.\n",
            plural(gating)
        ));
        for (finding, detail) in audit.gating() {
            out.push_str(&format!("- `{finding}` ({})", detail.status));
            if let Some(issue) = &detail.issue {
                out.push_str(&format!(", {issue}"));
            }
            out.push('\n');
            if let Some(excerpt) = &detail.excerpt {
                out.push_str(&format!("```\n{excerpt}\n```\n"));
            }
        }
    }
    if let Some(diff) = diff.filter(|diff| !diff.audit_newly_green.is_empty()) {
        out.push_str(&format!(
            "\n**PLAN AUDIT NEWLY GREEN: {}**\n",
            diff.audit_newly_green.len()
        ));
        render_tests(out, "", &diff.audit_newly_green);
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
                out.push_str("- Cause class: **death** — the lane died before it produced a verdict (the execution side of the seam: no machine, capacity, infrastructure, a stalled or killed run or compile).\n");
            } else if report.kind == LaneVerdict::Exit {
                out.push_str("- Cause class: **no-verdict** — the lane ran to an exit, but its result does not vouch for having measured anything (the testing side of the seam: it selected nothing, or stopped before it finished). It is not green.\n");
            } else {
                out.push_str("- Cause class: **no-verdict** — the lane ran to an exit, but its result is not a per-test verdict (the testing side of the seam: a test target that did not compile, zero tests, an incomplete enumeration). It is not green.\n");
            }
        }
    }
    out.push_str(&format!("- Cause: {cause}\n"));
    if let Some(row) = row {
        row_identity(out, row, report.kind);
        render_output_tail(out, row);
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
        Comparison::Unavailable { cause, known } => {
            out.push_str(&format!(
                "- Last complete row: unknown. {cause}, which does not mean none exists, so no merge list is given.\n"
            ));
            if let Some(known) = known {
                out.push_str(&format!(
                    "- Newest complete row on {}: {}, the {}, at {}. It shows history exists; it is not the compared row.\n",
                    report.platform,
                    known.reference,
                    armed(known),
                    since(known)
                ));
            }
        }
    }
    if let Comparison::SameExecutor {
        unread: Unread {
            neighbour: Some(cause),
            ..
        },
        ..
    } = report.comparison
    {
        out.push_str(&format!(
            "- Covered through: unknown. {cause}, so how far another host's row covers main is not known, and no merge list is given.\n"
        ));
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
    if report.comparison.coverage().is_some() && report.commit.is_some() {
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
        CadenceVerdict::AwaitingAdoption { .. } => {
            out.push_str(&format!(
                "**MAIN CADENCE · {} · {}** — {}.\n",
                report.check,
                report.platform,
                outcome_summary(report.kind, report.verdict)
            ));
        }
        CadenceVerdict::NoVerdict { row, cause } => render_incomplete(
            &mut out,
            report,
            row.as_ref(),
            &truncate(cause, MAX_CAUSE_CHARS),
            Unobserved::NoVerdict,
        ),
        CadenceVerdict::Death { row, cause } => render_incomplete(
            &mut out,
            report,
            row.as_ref(),
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

/// What an unchanged non-verdict posts once per [`SILENCE_BOUND_MS`] while it
/// stands: how long, since when, and its cause as it reads now. The first
/// notice carried everything else, and the eventual complete row's bracket
/// lists every merge that landed meanwhile.
fn render_still_held(report: &CadenceReport<'_>, since_ms: i64) -> String {
    let (class, cause) = match report.verdict {
        CadenceVerdict::NoVerdict { cause, .. } => ("no-verdict", cause),
        CadenceVerdict::Death { cause, .. } => ("death", cause),
        CadenceVerdict::Complete(_)
        | CadenceVerdict::Cancelled { .. }
        | CadenceVerdict::AwaitingAdoption { .. } => return render_report(report),
    };
    let tip = report
        .commit
        .map(|commit| format!(" · `{}`", short_commit(commit)))
        .unwrap_or_default();
    format!(
        "🚨 **MAIN CADENCE STILL HELD · {check} · {platform}{tip}** — still held for {held}: the {class} first announced at {since} has not changed, and main is unobserved for {check} on {platform} until a cadence row completes.\n\n- Cause: {cause}\n- Repeats: every attempt since {since} is on the project overview and was not posted. The next notice is a changed cause, the first complete row, or this line again in {bound}.\n",
        check = report.check,
        platform = report.platform,
        held = age(report.now_ms, since_ms),
        since = stamp(since_ms),
        cause = truncate(cause, MAX_CAUSE_CHARS),
        bound = age(SILENCE_BOUND_MS, 0),
    )
}

/// The heartbeat's note for a lane whose current inputs a complete row already
/// answers but which has posted nothing for a whole heartbeat interval.
pub(crate) fn render_heartbeat(
    check: &str,
    platform: &str,
    kind: LaneVerdict,
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
        totals(covered, kind)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::checks::host_ended_compile_specimens::{
        COMPILE_ERROR_TAIL, DISK_FULL_TAIL, KILLED_RMCP_TAIL, MIRROR_WRITE_DENIED_TAIL,
    };

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
            selection: Some("full".to_string()),
            tests: tests
                .iter()
                .map(|(test, outcome)| (test.to_string(), outcome.clone()))
                .collect(),
            output_tail: String::new(),
            lane_started_at_ms: None,
            plan_audit: None,
        }
    }

    /// `row` carrying an evaluated audit with these findings, as `(id, status)`.
    /// A gating finding on an otherwise clean row exits as the runner would.
    fn audited(mut row: CadenceRow, findings: &[(&str, &str)]) -> CadenceRow {
        row.plan_audit = Some(RowAudit {
            evaluated: true,
            note: None,
            findings: findings
                .iter()
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
        if row.verdict_passed && row.audit_gating() > 0 {
            row.verdict_passed = false;
            row.exit_code = i64::from(PLAN_AUDIT_EXIT_CODE);
        }
        row
    }

    fn not_evaluated(mut row: CadenceRow) -> CadenceRow {
        row.plan_audit = Some(RowAudit {
            evaluated: false,
            note: Some("no test explained a statement".to_string()),
            findings: BTreeMap::new(),
        });
        row
    }

    fn failed(excerpt: &str) -> TestOutcome {
        TestOutcome::Failed(Failure::from_excerpt(excerpt))
    }

    /// A failure the row dated: started `start_ms` into the lane, ran
    /// `duration_ms`.
    fn failed_at(excerpt: &str, start_ms: i64, duration_ms: i64) -> TestOutcome {
        TestOutcome::Failed(
            Failure::from_excerpt(excerpt).ran(Span::recorded(Some(start_ms), Some(duration_ms))),
        )
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
            peer: None,
            unread: Unread::default(),
        }
    }

    const HOUR: i64 = 60 * 60 * 1000;

    fn report(verdict: &CadenceVerdict, comparison: &Comparison, bracket: &Bracket) -> String {
        render_report(&CadenceReport {
            check: "rust-tests",
            platform: "linux",
            kind: LaneVerdict::PerTest,
            commit: Some("bbbbbbbbbbbbbbbbbbbb"),
            verdict,
            comparison,
            bracket,
            now_ms: 10 * HOUR,
            rearmed: None,
        })
    }

    const WIDE: &str = "os/x/src/a.rs|wide-scan|events|0123456789abcdef";
    const OTHER_WIDE: &str = "os/x/src/b.rs|wide-scan|events|fedcba9876543210";

    fn all_passed(id: &str, ran_at_ms: i64) -> CadenceRow {
        row(
            id,
            "bbbbbbbbbbbbbbbbbbbb",
            ran_at_ms,
            &[("t", TestOutcome::Passed)],
        )
    }

    /// An audit red on a tree whose tests all passed is a complete red, through
    /// every reader that would otherwise take "nonzero with no failing test" for
    /// a runner that failed outside any test or a host that ended the compile.
    #[test]
    fn a_gating_audit_on_a_green_tree_is_a_complete_red() {
        let mut gating = audited(all_passed("a", HOUR), &[(WIDE, "failed")]);
        gating.output_tail = KILLED_RMCP_TAIL.to_string();
        assert!(host_ended_compile_cause(&gating).is_none());
        assert!(credential_fetch_cause(&gating).is_none());
        assert!(matches!(classify_row(gating), CadenceVerdict::Complete(_)));

        let stale = audited(all_passed("a", HOUR), &[(WIDE, "stale")]);
        assert!(matches!(classify_row(stale), CadenceVerdict::Complete(_)));

        // Allowlisted and report-only rows do not gate, so they explain no red.
        let mut quiet = audited(all_passed("a", HOUR), &[(WIDE, "allowlisted")]);
        quiet.verdict_passed = false;
        quiet.exit_code = 1;
        assert!(matches!(
            classify_row(quiet),
            CadenceVerdict::NoVerdict { .. }
        ));

        // The audit explains only its own exit: a run that died some other way
        // beside it is still a runner failure.
        let mut died = audited(all_passed("a", HOUR), &[(WIDE, "failed")]);
        died.exit_code = 101;
        assert!(matches!(
            classify_row(died),
            CadenceVerdict::NoVerdict { .. }
        ));
    }

    #[test]
    fn the_headline_leads_with_a_gating_audit() {
        let previous = audited(all_passed("a", HOUR), &[]);
        let current = audited(
            all_passed("b", 2 * HOUR),
            &[(WIDE, "failed"), (OTHER_WIDE, "stale")],
        );
        let verdict = CadenceVerdict::Complete(current);
        let bracket = Bracket::default();
        let text = report(&verdict, &same(&previous), &bracket);
        assert!(
            text.contains(": plan audit: 2 gating; all 1 passed — 0 newly red, 0 newly green · plan audit: 2 newly red, 0 newly green**"),
            "{text}"
        );
        assert!(text.contains(&format!("`{WIDE}` (failed)")), "{text}");
        assert!(text.contains(&format!("`{OTHER_WIDE}` (stale)")), "{text}");
    }

    /// The identities are the body, with no cap on how many.
    #[test]
    fn every_gating_finding_is_named() {
        let ids: Vec<String> = (0..40)
            .map(|n| format!("file|wide-scan|t|{n:016x}"))
            .collect();
        let findings: Vec<(&str, &str)> = ids.iter().map(|id| (id.as_str(), "failed")).collect();
        let verdict = CadenceVerdict::Complete(audited(all_passed("b", 2 * HOUR), &findings));
        let text = report(&verdict, &Comparison::Baseline, &Bracket::default());
        for id in &ids {
            assert!(text.contains(id.as_str()), "{id} missing from {text}");
        }
    }

    #[test]
    fn a_not_evaluated_audit_follows_the_tests_and_is_never_clean() {
        let verdict = CadenceVerdict::Complete(not_evaluated(all_passed("b", 2 * HOUR)));
        let text = report(&verdict, &Comparison::Baseline, &Bracket::default());
        assert!(
            text.contains(
                ": all 1 passed; plan audit: not evaluated (no test explained a statement) —"
            ),
            "{text}"
        );
        assert!(text.contains("**Plan audit: not evaluated**"), "{text}");
        let CadenceVerdict::Complete(row) = &verdict else {
            unreachable!()
        };
        assert!(matches!(
            classify_row(row.clone()),
            CadenceVerdict::Complete(_)
        ));
    }

    #[test]
    fn findings_diff_by_identity_between_two_evaluated_rows() {
        let previous = audited(all_passed("a", HOUR), &[(WIDE, "failed")]);
        let moved = audited(
            all_passed("b", 2 * HOUR),
            &[(WIDE, "failed"), (OTHER_WIDE, "failed")],
        );
        let comparison = same(&previous);
        let diff = row_diff(&moved, &comparison);
        assert_eq!(diff.audit_newly_red, [OTHER_WIDE]);
        assert_eq!(diff.audit_still_red, 1);
        assert!(diff.audit_newly_green.is_empty());

        let cleared = audited(all_passed("b", 2 * HOUR), &[(WIDE, "allowlisted")]);
        let diff = row_diff(&cleared, &comparison);
        assert_eq!(diff.audit_newly_green, [WIDE]);
        assert!(diff.audit_newly_red.is_empty());
        assert!(row_is_news(LaneVerdict::PerTest, &cleared, &comparison));

        // Inside the silence bound, which would make any later row news.
        let unmoved = audited(all_passed("b", HOUR + 1), &[(WIDE, "failed")]);
        assert!(!row_is_news(LaneVerdict::PerTest, &unmoved, &comparison));
    }

    /// A row that judged nothing moved nothing: not green from a clean row's red,
    /// not red for what the next row finds. Each side is still news once, so the
    /// gap is never silent.
    #[test]
    fn a_not_evaluated_row_produces_no_movement_on_either_side() {
        let red = audited(all_passed("a", HOUR), &[(WIDE, "failed")]);
        let unjudged = not_evaluated(all_passed("b", 2 * HOUR));
        let comparison = same(&red);
        let diff = row_diff(&unjudged, &comparison);
        assert!(diff.audit_newly_green.is_empty() && diff.audit_newly_red.is_empty());
        assert!(row_is_news(LaneVerdict::PerTest, &unjudged, &comparison));

        let comparison = same(&unjudged);
        let diff = row_diff(&red, &comparison);
        assert!(diff.audit_newly_green.is_empty() && diff.audit_newly_red.is_empty());
        assert_eq!(diff.audit_still_red, 0);
        assert!(row_is_news(LaneVerdict::PerTest, &red, &comparison));

        let clean = audited(all_passed("c", 3 * HOUR), &[]);
        let diff = row_diff(&clean, &comparison);
        assert!(diff.audit_newly_green.is_empty() && diff.audit_newly_red.is_empty());
    }

    /// A test red beside an audit red counts its own tests, and neither hides the
    /// other.
    #[test]
    fn a_test_red_beside_an_audit_red_counts_its_tests() {
        let previous = audited(all_passed("a", HOUR), &[]);
        let current = audited(
            row(
                "b",
                "bbbbbbbbbbbbbbbbbbbb",
                2 * HOUR,
                &[("t", failed("boom")), ("u", TestOutcome::Passed)],
            ),
            &[(WIDE, "failed")],
        );
        assert_eq!(current.exit_code, 100, "the tests' own exit stands");
        let verdict = classify_row(current);
        assert!(matches!(verdict, CadenceVerdict::Complete(_)));
        let text = report(&verdict, &same(&previous), &Bracket::default());
        assert!(
            text.contains(": plan audit: 1 gating; 1 failed —"),
            "{text}"
        );
        assert!(
            text.contains("Totals") && text.contains("2 ran · 1 passed · 1 failed"),
            "{text}"
        );
    }

    fn macos_lane() -> CadenceLane {
        CadenceLane {
            platform: "macos".into(),
            ..linux_lane()
        }
    }

    fn tip(project_id: &str, commit: &str) -> Tip {
        Tip {
            project_id: project_id.into(),
            project_key: "cairn".into(),
            repo_path: "/tmp/repo".into(),
            default_branch: "main".into(),
            repository: PathBuf::from("/tmp/store"),
            commit: commit.into(),
        }
    }

    /// The arm resolving the tip: its branch and arming instant.
    fn resolving(slots: &CadenceSlots, project_id: &str) -> Option<(String, i64)> {
        slots
            .in_flight(project_id)
            .and_then(|in_flight| in_flight.resolving)
    }

    #[test]
    fn requests_during_an_arm_coalesce_into_one_queued_successor() {
        let all = || LaneScope::All;
        let mut slots = CadenceSlots::default();
        assert!(slots.request("p", "main", Trigger::Startup, all(), 1));
        assert_eq!(resolving(&slots, "p"), Some(("main".to_string(), 1)));
        // Three merges and a heartbeat while the first arm runs: no second
        // arm starts, and exactly one successor is owed.
        assert!(!slots.request("p", "main", Trigger::Heartbeat, all(), 2));
        assert!(!slots.request("p", "main", Trigger::Merge, all(), 3));
        assert!(!slots.request("p", "main", Trigger::Merge, all(), 4));
        assert!(!slots.request("p", "main", Trigger::Heartbeat, all(), 5));
        assert_eq!(
            slots.finish("p", 6),
            Some((Trigger::Merge, LaneScope::All)),
            "the strongest reason is kept"
        );
        assert_eq!(resolving(&slots, "p"), Some(("main".to_string(), 6)));
        assert_eq!(
            slots.finish("p", 7),
            None,
            "one successor, never one per merge"
        );
        assert_eq!(slots.in_flight("p"), None);
        assert!(
            slots.request("p", "main", Trigger::Merge, all(), 8),
            "a released slot starts afresh"
        );
        assert!(
            slots.request("q", "trunk", Trigger::Merge, all(), 8),
            "projects are independent"
        );
    }

    /// cairn/5270: every lane of a pass used to be joined, so the next pass
    /// (and the Linux row it would record) waited for the slowest lane, and a
    /// macOS lane parked for its whole four-hour placement bound would have
    /// held Linux for four hours. Each lane now holds only its own slot.
    #[test]
    fn a_busy_lane_never_delays_another_lanes_successor() {
        let (linux, macos) = (linux_lane(), macos_lane());
        let mut slots = CadenceSlots::default();
        assert!(slots.offer(&linux, &tip("p", "t1"), Trigger::Merge, 1));
        assert!(slots.offer(&macos, &tip("p", "t1"), Trigger::Merge, 1));
        // Two more tips while both run: each lane owes one successor, on the
        // newest tip, with the strongest reason.
        assert!(!slots.offer(&linux, &tip("p", "t2"), Trigger::Merge, 2));
        assert!(!slots.offer(&macos, &tip("p", "t2"), Trigger::Merge, 2));
        assert!(!slots.offer(&linux, &tip("p", "t3"), Trigger::Heartbeat, 3));
        assert!(!slots.offer(&macos, &tip("p", "t3"), Trigger::Heartbeat, 3));

        // Linux finishes while macOS is still parked, and starts its successor
        // at once.
        assert_eq!(
            slots.lane_finished("p", &linux, 4),
            Some((Trigger::Merge, tip("p", "t3")))
        );
        assert_eq!(slots.lane_finished("p", &linux, 5), None);
        // The next merge, macOS still parked: idle Linux starts, macOS folds.
        assert!(slots.offer(&linux, &tip("p", "t4"), Trigger::Merge, 6));
        assert!(!slots.offer(&macos, &tip("p", "t4"), Trigger::Merge, 6));
        assert_eq!(
            slots.in_flight("p"),
            Some(InFlight {
                resolving: None,
                lanes: vec![
                    LaneInFlight {
                        check: "rust-tests".into(),
                        platform: "linux".into(),
                        commit: "t4".into(),
                        armed_at_unix_ms: 6,
                        queued: false,
                        adopted: None,
                    },
                    LaneInFlight {
                        check: "rust-tests".into(),
                        platform: "macos".into(),
                        commit: "t1".into(),
                        armed_at_unix_ms: 1,
                        queued: true,
                        adopted: None,
                    },
                ],
                rearming: Vec::new(),
            })
        );

        // macOS owes exactly one successor, on the newest tip.
        assert_eq!(
            slots.lane_finished("p", &macos, 7),
            Some((Trigger::Merge, tip("p", "t4")))
        );
        assert_eq!(slots.lane_finished("p", &macos, 8), None);
        assert!(
            slots.offer(&macos, &tip("q", "t1"), Trigger::Merge, 8),
            "projects are independent"
        );
    }

    /// A lane's own re-arm (its settled debt) names only that lane, so the
    /// macOS lane's debt never re-runs the Linux lane. A merge folded into the
    /// same arm is what widens it to every lane.
    #[test]
    fn a_lane_rearm_runs_only_that_lane_unless_a_merge_widens_it() {
        let (linux, macos) = (linux_lane(), macos_lane());
        let only_macos = LaneScope::lane(&macos);
        assert!(only_macos.includes(&macos));
        assert!(!only_macos.includes(&linux));
        assert!(LaneScope::All.includes(&linux));

        let mut slots = CadenceSlots::default();
        assert!(slots.request("p", "main", Trigger::Merge, LaneScope::All, 1));
        assert!(!slots.request("p", "main", Trigger::DebtSettled, only_macos.clone(), 2));
        assert_eq!(
            slots.finish("p", 3),
            Some((Trigger::DebtSettled, only_macos.clone())),
            "a queued lane re-arm stays that lane's"
        );
        assert!(!slots.request(
            "p",
            "main",
            Trigger::DebtSettled,
            LaneScope::lane(&linux),
            4
        ));
        assert!(!slots.request("p", "main", Trigger::DebtSettled, only_macos.clone(), 5));
        assert_eq!(
            slots.finish("p", 6),
            Some((
                Trigger::DebtSettled,
                LaneScope::Only(BTreeSet::from([linux.key(), macos.key()]))
            )),
            "two lanes' re-arms are one arm for both"
        );
        assert!(!slots.request("p", "main", Trigger::Merge, LaneScope::All, 7));
        assert!(!slots.request("p", "main", Trigger::DebtSettled, only_macos, 8));
        assert_eq!(slots.finish("p", 9), Some((Trigger::Merge, LaneScope::All)));
        assert_eq!(slots.finish("p", 10), None);
    }

    /// A settled debt outranks the routine re-arms, so a pass queued behind it
    /// is not reported as a heartbeat, and yields only to a merge.
    #[test]
    fn a_settled_debt_ranks_above_routine_rearms_and_below_a_merge() {
        assert!(Trigger::DebtSettled > Trigger::Heartbeat);
        assert!(Trigger::DebtSettled > Trigger::Startup);
        assert!(Trigger::DebtSettled < Trigger::Merge);
        let mut slots = CadenceSlots::default();
        assert!(slots.request("p", "main", Trigger::Startup, LaneScope::All, 1));
        assert!(!slots.request("p", "main", Trigger::Heartbeat, LaneScope::All, 2));
        assert!(!slots.request("p", "main", Trigger::DebtSettled, LaneScope::All, 3));
        assert_eq!(
            slots.finish("p", 4),
            Some((Trigger::DebtSettled, LaneScope::All))
        );
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
        let lane = linux_lane();

        // Before the restart: a pass on the old tip, a merge queued behind it,
        // and the stop ending its lane.
        let mut before = CadenceSlots::default();
        assert!(before.offer(&lane, &tip("p", "5027f5473a57a1b2"), Trigger::Merge, 1));
        assert!(!before.offer(&lane, &tip("p", "later"), Trigger::Merge, 2));
        let stopped_at = 3 * HOUR;
        let ended = cancelled_by_restart(
            CadenceVerdict::Death {
                row: None,
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
            slots.request("p", "main", Trigger::Startup, LaneScope::All, 4),
            "the boot arm starts a pass"
        );
        // The catch-up's re-arm and a merge seen during the boot pass are one
        // queued successor, not more passes.
        assert!(!slots.request("p", "main", Trigger::Startup, LaneScope::All, 5));
        assert!(!slots.request("p", "main", Trigger::Merge, LaneScope::All, 6));
        let armed = slots
            .in_flight("p")
            .expect("main has a pass armed after boot");
        assert_eq!(armed.resolving, Some(("main".to_string(), 4)));
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
            kind: LaneVerdict::PerTest,
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

        assert_eq!(slots.finish("p", 7), Some((Trigger::Merge, LaneScope::All)));
        assert_eq!(slots.finish("p", 8), None);
        assert!(slots.in_flight("p").is_none());
    }

    fn adoption_claim(state: &str) -> Claim {
        Claim {
            request_id: "remote-request".into(),
            attempt_id: "attempt".into(),
            project_id: "p".into(),
            check_names: vec!["rust-tests".into()],
            input_hashes: vec!["original-inputs".into()],
            recording_plans: Vec::new(),
            platform: "linux".into(),
            commit_sha: "original-commit".into(),
            consumer: "main_cadence".into(),
            consumer_ref: "main:p:rust-tests:linux".into(),
            executor_id: "remote".into(),
            machine: "fedora".into(),
            granted_at_ms: HOUR,
            state: state.into(),
            stopped_at_ms: (state != "running").then_some(2 * HOUR),
            evidence: None,
            observation_id: None,
            updated_ms: 2 * HOUR,
        }
    }

    #[test]
    fn cadence_adoption_reserves_running_awaiting_and_joined_claims_before_startup() {
        let lane = linux_lane();
        for state in ["running", "awaiting_adoption", "joined"] {
            let claim = adoption_claim(state);
            let mut slots = CadenceSlots::default();
            assert!(slots.adopt(&claim, 0, 3 * HOUR));
            assert!(
                slots.adopt(&claim, 0, 3 * HOUR),
                "reserving the same identity is idempotent"
            );
            assert!(!slots.offer(&lane, &tip("p", "startup-tip"), Trigger::Startup, 4 * HOUR));
            assert!(!slots.offer(&lane, &tip("p", "newest-tip"), Trigger::Merge, 5 * HOUR));
            let flight = slots.in_flight("p").unwrap();
            assert_eq!(flight.lanes.len(), 1);
            assert_eq!(flight.lanes[0].commit, "original-commit");
            assert_eq!(flight.lanes[0].armed_at_unix_ms, HOUR);
            assert!(flight.lanes[0].queued);
            let rendered = render_adopted_lane(&flight.lanes[0]).unwrap();
            for text in [
                "awaiting adoption on fedora",
                "authorize_overlap",
                "the machine reports or is removed",
                "Live output streaming is not reattached",
            ] {
                assert!(rendered.contains(text), "{rendered}");
            }
            assert_eq!(
                slots.lane_finished("p", &lane, 6 * HOUR),
                Some((Trigger::Merge, tip("p", "newest-tip")))
            );
            assert!(slots.in_flight("p").unwrap().lanes[0].adopted.is_none());
            assert_eq!(slots.lane_finished("p", &lane, 7 * HOUR), None);
        }
    }

    #[tokio::test]
    async fn cadence_adoption_failed_thread_write_remains_retryable_not_quiet() {
        let root = tempfile::tempdir().unwrap();
        let db = Arc::new(cadence_db(&root).await);
        let search = Arc::new(
            crate::storage::SearchIndex::open_or_create(root.path().join("search")).unwrap(),
        );
        let orch = Orchestrator::builder(
            Arc::new(crate::db::DbState::new(db.clone(), search)),
            Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            root.path().to_path_buf(),
        )
        .build();
        let lane = linux_lane();
        let verdict = CadenceVerdict::AwaitingAdoption {
            machine: "fedora".into(),
            request_id: "retained".into(),
            stopped_at_unix_ms: HOUR,
        };
        let attempted = Attempted::default();
        let missing = try_report_lane(
            &orch,
            &db,
            "p",
            "cairn",
            None,
            &lane,
            Some("commit"),
            verdict.clone(),
            &attempted,
        )
        .await
        .unwrap_err();
        assert!(missing.contains("does not exist"), "{missing}");
        db.execute_script(
            "INSERT INTO threads(id,project_id,name,status,attention,created_at,updated_at)
               VALUES('testing','p','testing','active','none',1,1);
             INSERT INTO jobs(id,thread_id,project_id,node_name,uri_segment,status,created_at,updated_at)
               VALUES('thread-job','testing','p','thread','thread','idle',1,1);
             CREATE TRIGGER reject_cadence_delivery BEFORE INSERT ON messages
               WHEN NEW.sender_name='main cadence'
               BEGIN SELECT RAISE(FAIL,'injected cadence delivery rejection'); END;"
        ).await.unwrap();
        for _ in 0..2 {
            let error = try_report_lane(
                &orch,
                &db,
                "p",
                "cairn",
                None,
                &lane,
                Some("commit"),
                verdict.clone(),
                &attempted,
            )
            .await
            .unwrap_err();
            assert!(
                error.contains("injected cadence delivery rejection"),
                "{error}"
            );
        }
        db.execute_script("DROP TRIGGER reject_cadence_delivery;")
            .await
            .unwrap();
        try_report_lane(
            &orch,
            &db,
            "p",
            "cairn",
            None,
            &lane,
            Some("commit"),
            verdict.clone(),
            &attempted,
        )
        .await
        .unwrap();
        let count = db.query_all(
            "SELECT COUNT(*) FROM messages WHERE channel_id='testing' AND sender_name='main cadence'",
            (), |row| row.i64(0),
        ).await.unwrap()[0];
        assert_eq!(
            count, 1,
            "failed sends are retried rather than mistaken for prior delivery"
        );
        // A genuine Quiet decision is successful and attempts no second write.
        db.execute_script(
            "CREATE TRIGGER reject_duplicate_delivery BEFORE INSERT ON messages
             WHEN NEW.sender_name='main cadence'
             BEGIN SELECT RAISE(FAIL,'quiet must not send'); END;",
        )
        .await
        .unwrap();
        try_report_lane(
            &orch,
            &db,
            "p",
            "cairn",
            None,
            &lane,
            Some("commit"),
            verdict,
            &attempted,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn cadence_adoption_live_confirmation_is_fenced_to_purpose_and_observation() {
        let db = crate::storage::migrated_test_db("cadence-live-confirmation").await;
        let lane = linux_lane();
        let mut claim = adoption_claim("running");
        claim.consumer = "manual".into();
        let purpose =
            crate::fleet::purpose::PurposeKey::main_cadence("p", &lane.check, &lane.platform)
                .to_string();
        check_lane_adoptions::insert(&db, &claim).await.unwrap();
        let exact = Waiter {
            request_id: claim.request_id.clone(),
            attempt_id: claim.attempt_id.clone(),
            check_name: lane.check.clone(),
            item_index: 0,
            consumer: "main_cadence".into(),
            consumer_ref: purpose.clone(),
            state: "recorded".into(),
            observation_id: Some("observation".into()),
            evidence: None,
            updated_ms: HOUR,
        };
        for waiter in [
            exact.clone(),
            Waiter {
                consumer_ref: "another-purpose".into(),
                ..exact.clone()
            },
            Waiter {
                consumer: "manual".into(),
                ..exact.clone()
            },
        ] {
            check_lane_adoptions::insert_waiter(&db, &waiter)
                .await
                .unwrap();
        }
        let expected = vec![(claim.request_id.clone(), claim.attempt_id.clone())];
        assert_eq!(
            main_cadence_delivery_requests(&db, "p", &lane, "observation")
                .await
                .unwrap(),
            expected
        );
        assert!(
            main_cadence_delivery_requests(&db, "p", &lane, "wrong-observation")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            main_cadence_delivery_requests(&db, "other-project", &lane, "observation")
                .await
                .unwrap()
                .is_empty()
        );
        let mut other_platform = lane.clone();
        other_platform.platform = "macos".into();
        assert!(
            main_cadence_delivery_requests(&db, "p", &other_platform, "observation")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn cadence_adoption_joined_waiter_selects_its_item_not_the_grants_initiator() {
        let mut claim = adoption_claim("running");
        claim.consumer = "manual".into();
        claim.check_names = vec!["lint".into(), "rust-tests".into(), "typecheck".into()];
        claim.input_hashes = vec![
            "lint-inputs".into(),
            "original-inputs".into(),
            "type-inputs".into(),
        ];
        let mut waiter = Waiter {
            request_id: claim.request_id.clone(),
            attempt_id: claim.attempt_id.clone(),
            check_name: "rust-tests".into(),
            item_index: 1,
            consumer: "main_cadence".into(),
            consumer_ref: "main:p:rust-tests:linux".into(),
            state: "joined".into(),
            observation_id: None,
            evidence: None,
            updated_ms: 2 * HOUR,
        };
        let indices = cadence_waiter_indices(&claim, &[waiter.clone()]).unwrap();
        assert_eq!(indices, vec![1]);
        assert!(verify_adoption_inputs(&claim, indices[0], "original-inputs").is_ok());
        let mut slots = CadenceSlots::default();
        assert!(slots.adopt(&claim, indices[0], 3 * HOUR));
        assert_eq!(slots.in_flight("p").unwrap().lanes[0].check, "rust-tests");
        waiter.item_index = 0;
        assert!(cadence_waiter_indices(&claim, &[waiter]).is_err());
    }

    #[test]
    fn cadence_adoption_left_running_is_neither_cancelled_nor_a_death() {
        let verdict = cancelled_by_restart(
            CadenceVerdict::AwaitingAdoption {
                machine: "fedora".into(),
                request_id: "remote-request".into(),
                stopped_at_unix_ms: 2 * HOUR,
            },
            Some((2 * HOUR) as u64),
        );
        assert!(matches!(verdict, CadenceVerdict::AwaitingAdoption { .. }));
        assert!(verdict.row().is_none());
        let report = render_report(&CadenceReport {
            check: "rust-tests",
            platform: "linux",
            kind: LaneVerdict::PerTest,
            commit: Some("original-commit"),
            verdict: &verdict,
            comparison: &Comparison::Baseline,
            bracket: &Bracket::default(),
            now_ms: 3 * HOUR,
            rearmed: None,
        });
        assert!(
            report.contains(
                "while this lane ran on fedora (request remote-request); awaiting adoption"
            ),
            "{report}"
        );
        assert!(!report.contains("cancelled by restart"), "{report}");
        assert!(matches!(
            observe(
                LaneVerdict::PerTest,
                &verdict,
                &Comparison::Baseline,
                None,
                &Attempted::default()
            ),
            Observed::Unobserved { reminds: false, .. }
        ));
    }

    #[test]
    fn cadence_adoption_validates_each_multicheck_item_and_rejects_changed_hashes() {
        let mut claim = adoption_claim("running");
        assert!(verify_adoption_inputs(&claim, 0, "original-inputs").is_ok());
        let error = verify_adoption_inputs(&claim, 0, "changed-inputs").unwrap_err();
        for text in ["original-commit", "original-inputs", "changed-inputs"] {
            assert!(error.contains(text), "{error}");
        }
        claim.check_names.push("lint".into());
        claim.input_hashes.push("lint-inputs".into());
        assert_eq!(cadence_claim_check(&claim, 1).unwrap(), "lint");
        assert!(verify_adoption_inputs(&claim, 1, "lint-inputs").is_ok());
        let mut slots = CadenceSlots::default();
        assert!(slots.adopt(&claim, 1, 3 * HOUR));
        assert_eq!(slots.in_flight("p").unwrap().lanes[0].check, "lint");
        assert!(verify_adoption_inputs(&claim, 2, "absent").is_err());
        claim.check_names.clear();
        claim.input_hashes.clear();
        assert!(verify_adoption_inputs(&claim, 0, "anything").is_err());
    }

    #[tokio::test]
    async fn cadence_adoption_failed_preload_recovers_as_loss_not_infrastructure_verdict() {
        failed_preload_recovery(false, false).await;
    }

    #[tokio::test]
    async fn cadence_adoption_failed_preload_retries_rejected_loss_after_interruption() {
        failed_preload_recovery(false, true).await;
    }

    #[tokio::test]
    async fn cadence_adoption_result_first_failed_interruption_yields_to_unrelated_result() {
        failed_preload_recovery(true, false).await;
    }

    #[tokio::test]
    async fn cadence_adoption_result_first_failed_loss_yields_to_unrelated_result() {
        failed_preload_recovery(true, true).await;
    }

    async fn failed_preload_recovery(result_first: bool, reject_loss: bool) {
        use crate::mcp::handlers::run::adopted::{adopt, Adoption};
        use cairn_common::executor_protocol::{CellOutcome, ExecutorMessage};

        let root = tempfile::tempdir().unwrap();
        let db = Arc::new(crate::storage::migrated_test_db("cadence-preload-failure").await);
        let claim = adoption_claim("awaiting_adoption");
        let waiter = Waiter {
            request_id: claim.request_id.clone(),
            attempt_id: claim.attempt_id.clone(),
            check_name: "rust-tests".into(),
            item_index: 0,
            consumer: "main_cadence".into(),
            consumer_ref: claim.consumer_ref.clone(),
            state: "awaiting_adoption".into(),
            observation_id: None,
            evidence: None,
            updated_ms: 2 * HOUR,
        };
        check_lane_adoptions::insert_grant(&db, &claim, &[waiter])
            .await
            .unwrap();
        // Fail the real preload query, then recover before cadence's read.
        db.execute_script(
            "ALTER TABLE check_lane_adoptions RENAME TO unavailable_check_lane_adoptions;",
        )
        .await
        .unwrap();
        let search = Arc::new(
            crate::storage::SearchIndex::open_or_create(root.path().join("search")).unwrap(),
        );
        let orch = Orchestrator::builder(
            Arc::new(crate::db::DbState::new(db.clone(), search)),
            Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            root.path().to_path_buf(),
        )
        .build();
        orch.fleet.await_restart_adoptions_loaded().await;
        db.execute_script(
            "ALTER TABLE unavailable_check_lane_adoptions RENAME TO check_lane_adoptions;",
        )
        .await
        .unwrap();
        let trigger = if reject_loss {
            "CREATE TRIGGER reject_preload_recovery BEFORE UPDATE OF state ON check_lane_adoptions
             WHEN NEW.state='lost' AND NEW.request_id='remote-request'
             BEGIN SELECT RAISE(ABORT,'fixture rejects loss'); END;"
        } else {
            "CREATE TRIGGER reject_preload_recovery BEFORE INSERT ON main_cadence_interruptions
             WHEN NEW.project_id='p' AND NEW.check_name='rust-tests'
             BEGIN SELECT RAISE(ABORT,'fixture rejects interruption'); END;"
        };
        db.execute_script(trigger).await.unwrap();
        assert!(!orch
            .fleet
            .is_check_adoption_reloaded(&(claim.request_id.clone(), claim.attempt_id.clone(),)));
        let mut frames = orch
            .fleet
            .attach_unreported_machine_for_test(&claim.executor_id);
        let result = crate::fleet::UnboundResult {
            executor_id: claim.executor_id.clone(),
            machine: claim.machine.clone(),
            request_id: claim.request_id.clone(),
            attempt_id: claim.attempt_id.clone(),
            outcome: CellOutcome::Cancelled {
                request_id: claim.request_id.clone(),
                attempt_id: claim.attempt_id.clone(),
            },
        };
        let mut driver = None;
        if result_first {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), adopt(&orch, result.clone()))
                    .await
                    .expect("a failed write must yield, not block the serial dispatcher"),
                Adoption::Undecided
            );
            // The serial dispatcher can decide another identity immediately.
            let unrelated = crate::fleet::UnboundResult {
                request_id: "unrelated-result".into(),
                attempt_id: "unrelated-attempt".into(),
                outcome: CellOutcome::Cancelled {
                    request_id: "unrelated-result".into(),
                    attempt_id: "unrelated-attempt".into(),
                },
                ..result.clone()
            };
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), adopt(&orch, unrelated))
                    .await
                    .expect("unrelated result is not held behind the rejected write"),
                Adoption::Unclaimed
            );
            assert!(matches!(
                frames.try_recv().unwrap(),
                ExecutorMessage::ResultAck { request_id, .. } if request_id == "unrelated-result"
            ));
            // A resend of the still-open claim must also yield without ACK.
            assert_eq!(adopt(&orch, result.clone()).await, Adoption::Undecided);
        } else {
            let mut boot = tokio::spawn({
                let orch = orch.clone();
                async move { adopt_at_boot(&orch).await }
            });
            assert!(
                tokio::time::timeout(Duration::from_millis(200), &mut boot)
                    .await
                    .is_err(),
                "boot recovery must retain the debt while its durable write fails"
            );
            driver = Some(boot);
        }
        assert_eq!(
            check_lane_adoptions::lookup(&db, &claim.request_id, &claim.attempt_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            "awaiting_adoption"
        );
        assert!(
            frames.try_recv().is_err(),
            "no Cancel or ACK before both writes commit"
        );
        assert!(orch.main_cadence.lock().unwrap().in_flight("p").is_none());
        assert_eq!(
            db.query_all("SELECT COUNT(*) FROM main_cadence_interruptions", (), |r| r
                .i64(0))
                .await
                .unwrap(),
            vec![i64::from(reject_loss)],
            "interruption must precede the terminal loss write"
        );
        db.execute_script("DROP TRIGGER reject_preload_recovery;")
            .await
            .unwrap();
        if let Some(driver) = driver {
            tokio::time::timeout(Duration::from_secs(10), driver)
                .await
                .expect("boot retries the failed write")
                .unwrap();
        } else {
            assert_eq!(adopt(&orch, result).await, Adoption::Unclaimed);
            // The later boot read must not re-adopt or recreate the repair.
            adopt_at_boot(&orch).await;
        }
        let lost = check_lane_adoptions::lookup(&db, &claim.request_id, &claim.attempt_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lost.state, "lost");
        assert_eq!(
            lost.evidence.as_deref(),
            Some("boot preload failed; the fleet cancelled it")
        );
        assert_eq!(
            check_lane_adoptions::waiters(&db, &claim.request_id, &claim.attempt_id)
                .await
                .unwrap()[0]
                .state,
            "lost"
        );
        assert!(matches!(
            frames.try_recv().unwrap(),
            ExecutorMessage::Cancel { .. }
        ));
        if result_first {
            assert!(matches!(
                frames.try_recv().unwrap(),
                ExecutorMessage::ResultAck { request_id, .. } if request_id == claim.request_id
            ));
        }
        assert!(
            frames.try_recv().is_err(),
            "one final acknowledgement, no duplicate release"
        );
        assert_eq!(
            db.query_all("SELECT COUNT(*) FROM check_result_observations", (), |r| r
                .i64(0))
                .await
                .unwrap(),
            vec![0],
            "preload cancellation is not an infrastructure answer about the original commit"
        );
        let interrupted = take_interruptions(&db).await.unwrap();
        assert_eq!(interrupted.len(), 1);
        assert_eq!(interrupted[0].1.commit, claim.commit_sha);
        assert_eq!(
            interrupted[0].1.stopped_at_unix_ms,
            claim.stopped_at_ms.unwrap()
        );
        assert!(take_interruptions(&db).await.unwrap().is_empty());
        let lane = linux_lane();
        let mut slots = orch.main_cadence.lock().unwrap();
        assert!(slots.take_interruption("p", &lane).is_some());
        assert!(slots.take_interruption("p", &lane).is_none());
        assert!(slots.offer(&lane, &tip("p", "current-tip"), Trigger::Startup, 3 * HOUR));
        assert!(!slots.offer(&lane, &tip("p", "current-tip"), Trigger::Startup, 3 * HOUR));
        assert_eq!(slots.in_flight("p").unwrap().lanes.len(), 1);
    }

    #[tokio::test]
    async fn cadence_adoption_omitted_account_before_bind_records_loss_not_verdict() {
        fleet_loss_recovery(false, false, false).await;
    }

    #[tokio::test]
    async fn cadence_adoption_omitted_account_pending_bind_rearms_after_startup() {
        fleet_loss_recovery(true, false, false).await;
    }

    #[tokio::test]
    async fn cadence_adoption_removed_machine_pending_bind_rearms_after_startup() {
        fleet_loss_recovery(true, true, false).await;
    }

    #[tokio::test]
    async fn cadence_adoption_omitted_account_before_boot_scan_keeps_durable_repair() {
        fleet_loss_recovery(false, false, true).await;
    }

    async fn fleet_loss_recovery(pending_bind: bool, removed: bool, before_boot_scan: bool) {
        use crate::execution::checks::CheckAdoptionRecordingPlan;
        use crate::execution::selection::{CheckPlan, CheckScope};

        let root = tempfile::tempdir().unwrap();
        let db = Arc::new(crate::storage::migrated_test_db("cadence-fleet-loss").await);
        db.execute_script(
            "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES('p','default','P','P','/fixture',1,1);",
        )
        .await
        .unwrap();
        let mut claim = adoption_claim("awaiting_adoption");
        let plan = CheckPlan {
            name: "rust-tests".into(),
            applies: true,
            command: "echo passed".into(),
            scope: CheckScope::Full,
            resource_class: crate::config::project_settings::CheckResourceClass::Shared,
            verdict_environment_names: vec![],
            verdict_executor_ids: None,
            config_error: None,
            verdict_platforms: vec!["linux".into()],
            verdict_runtime: vec![],
        };
        let coordinate = crate::execution::cache::SealedCheckCoordinate::new(
            &claim.commit_sha,
            &claim.commit_sha,
            "original-tree",
            &claim.input_hashes[0],
        );
        claim.recording_plans = vec![serde_json::to_value(CheckAdoptionRecordingPlan::new(
            &plan,
            &coordinate,
            "main:p",
            "main-cadence:p:rust-tests:linux",
            &[],
        ))
        .unwrap()];
        let waiter = Waiter {
            request_id: claim.request_id.clone(),
            attempt_id: claim.attempt_id.clone(),
            check_name: plan.name.clone(),
            item_index: 0,
            consumer: "main_cadence".into(),
            consumer_ref: claim.consumer_ref.clone(),
            state: "awaiting_adoption".into(),
            observation_id: None,
            evidence: None,
            updated_ms: 2 * HOUR,
        };
        check_lane_adoptions::insert_grant(&db, &claim, &[waiter])
            .await
            .unwrap();
        let search = Arc::new(
            crate::storage::SearchIndex::open_or_create(root.path().join("search")).unwrap(),
        );
        let orch = Orchestrator::builder(
            Arc::new(crate::db::DbState::new(db.clone(), search)),
            Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            root.path().to_path_buf(),
        )
        .build();
        orch.fleet.await_restart_adoptions_loaded().await;
        let key = (claim.request_id.clone(), claim.attempt_id.clone());
        let mut frames = orch
            .fleet
            .attach_unreported_machine_for_test(&claim.executor_id);
        let lane = linux_lane();
        if !before_boot_scan {
            assert!(orch.main_cadence.lock().unwrap().adopt(&claim, 0, 3 * HOUR));
            assert!(
                take_interruptions(&db).await.unwrap().is_empty(),
                "startup has already drained interruptions before this late loss"
            );
        }
        let driver = if pending_bind {
            let driver = tokio::spawn({
                let orch = orch.clone();
                let db = db.clone();
                let claim = claim.clone();
                async move { execute_adopted_check_item(&orch, db, &claim, 0).await }
            });
            tokio::time::timeout(Duration::from_secs(10), async {
                while !orch.fleet.has_pending_adopted_binder(&key) {
                    assert!(
                        !driver.is_finished(),
                        "the recorder must reach the real pending binding"
                    );
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("adopted recorder registers its binding before the loss");
            Some(driver)
        } else {
            None
        };
        if removed {
            orch.fleet
                .remove_machine_for_check_adoption_test(&claim.executor_id, "enrollment removed");
        } else {
            orch.fleet.apply_executor_snapshot(
                &claim.executor_id,
                1,
                Default::default(),
                Default::default(),
            );
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let current = check_lane_adoptions::lookup(&db, &key.0, &key.1)
                    .await
                    .unwrap()
                    .unwrap();
                if current.state == "lost" && orch.fleet.check_claim(&key).is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the actual fleet loss commits before releasing the binding");
        let lost = check_lane_adoptions::lookup(&db, &key.0, &key.1)
            .await
            .unwrap()
            .unwrap();
        let evidence = lost.evidence.clone().unwrap();
        assert!(evidence.contains(&claim.machine), "{evidence}");
        assert!(
            evidence.contains(if removed {
                "was removed"
            } else {
                "generation 1"
            }),
            "{evidence}"
        );
        if !removed {
            assert!(evidence.contains("resent no result"), "{evidence}");
        }
        let error = if let Some(driver) = driver {
            tokio::time::timeout(Duration::from_secs(10), driver)
                .await
                .unwrap()
                .unwrap()
                .err()
                .unwrap()
        } else {
            execute_adopted_check_item(&orch, db.clone(), &claim, 0)
                .await
                .err()
                .unwrap()
        };
        assert_eq!(
            error, evidence,
            "fleet loss never becomes a synthetic infrastructure answer"
        );
        let mut cancels = 0;
        while let Ok(frame) = frames.try_recv() {
            match frame {
                cairn_common::executor_protocol::ExecutorMessage::Cancel { .. } => cancels += 1,
                cairn_common::executor_protocol::ExecutorMessage::ResultAck { .. } => {
                    panic!("no result was resent, so there is nothing to acknowledge")
                }
                _ => {}
            }
        }
        assert_eq!(cancels, 1);
        if before_boot_scan {
            adopt_at_boot(&orch).await;
            assert!(orch.main_cadence.lock().unwrap().in_flight("p").is_none());
            let repairs = take_interruptions(&db).await.unwrap();
            assert_eq!(
                repairs.len(),
                1,
                "fleet persists repair before the open scan can miss its lost claim"
            );
            for (project, interruption) in repairs {
                orch.main_cadence
                    .lock()
                    .unwrap()
                    .interrupted(&project, interruption);
            }
        } else {
            // The batch driver's error edge must recover an already-lost claim
            // into memory, without rewriting authoritative fleet evidence/release.
            db.execute_script(
                "CREATE TRIGGER reject_repeated_loss BEFORE UPDATE ON check_lane_adoptions
                 BEGIN SELECT RAISE(ABORT,'already-lost claims must not be rewritten'); END;",
            )
            .await
            .unwrap();
            try_lose_adoption(&orch, &claim, std::slice::from_ref(&lane.check), &error)
                .await
                .unwrap();
            assert!(
                frames.try_recv().is_err(),
                "already-lost recovery does not release twice"
            );
            assert_eq!(
                check_lane_adoptions::lookup(&db, &key.0, &key.1)
                    .await
                    .unwrap()
                    .unwrap()
                    .evidence,
                Some(evidence)
            );
        }
        assert_eq!(
            db.query_all("SELECT COUNT(*) FROM check_result_observations", (), |r| r
                .i64(0))
                .await
                .unwrap(),
            vec![0],
        );
        assert_eq!(
            db.query_all("SELECT COUNT(*) FROM check_result_cache", (), |r| r.i64(0))
                .await
                .unwrap(),
            vec![0],
            "neither verdict cache nor infrastructure-suppression projection is written",
        );
        let citation = {
            let mut slots = orch.main_cadence.lock().unwrap();
            if !before_boot_scan {
                assert!(!slots.offer(&lane, &tip("p", "successor"), Trigger::Startup, 4 * HOUR));
                assert_eq!(
                    slots.lane_finished("p", &lane, 5 * HOUR),
                    Some((Trigger::Startup, tip("p", "successor")))
                );
            } else {
                assert!(slots.offer(&lane, &tip("p", "successor"), Trigger::Startup, 4 * HOUR));
            }
            assert_eq!(slots.in_flight("p").unwrap().lanes.len(), 1);
            let citation = slots.take_interruption("p", &lane).unwrap();
            assert!(slots.take_interruption("p", &lane).is_none());
            citation
        };
        assert_eq!(citation.commit, claim.commit_sha);
        let rendered = render_report(&CadenceReport {
            check: &lane.check,
            platform: &lane.platform,
            kind: LaneVerdict::PerTest,
            commit: Some("successor"),
            verdict: &CadenceVerdict::NoVerdict {
                row: None,
                cause: "fixture successor".into(),
            },
            comparison: &Comparison::Baseline,
            bracket: &Bracket::default(),
            now_ms: 6 * HOUR,
            rearmed: Some(&citation),
        });
        assert!(rendered.contains(&citation.describe()), "{rendered}");
        forget_interruption(&db, "p", &citation).await;
        assert!(
            take_interruptions(&db).await.unwrap().is_empty(),
            "a subsequent boot must not discover a stale repair and phantom rearm"
        );
        adopt_at_boot(&orch).await;
        assert!(take_interruptions(&db).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn cadence_adoption_loss_is_durable_and_cited_only_once() {
        let root = tempfile::tempdir().unwrap();
        let db = Arc::new(crate::storage::migrated_test_db("cadence-adoption-loss").await);
        let search = Arc::new(
            crate::storage::SearchIndex::open_or_create(root.path().join("search")).unwrap(),
        );
        let orch = Orchestrator::builder(
            Arc::new(crate::db::DbState::new(db.clone(), search)),
            Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            root.path().to_path_buf(),
        )
        .build();
        let claim = adoption_claim("running");
        check_lane_adoptions::insert(&db, &claim).await.unwrap();
        let pending = Waiter {
            request_id: claim.request_id.clone(),
            attempt_id: claim.attempt_id.clone(),
            check_name: "rust-tests".into(),
            item_index: 0,
            consumer: "main_cadence".into(),
            consumer_ref: claim.consumer_ref.clone(),
            state: "recorded".into(),
            observation_id: Some("committed-before-loss".into()),
            evidence: None,
            updated_ms: 2 * HOUR,
        };
        let unresolved = Waiter {
            consumer: "manual".into(),
            consumer_ref: "handle".into(),
            state: "running".into(),
            observation_id: None,
            ..pending.clone()
        };
        check_lane_adoptions::insert_waiter(&db, &pending)
            .await
            .unwrap();
        check_lane_adoptions::insert_waiter(&db, &unresolved)
            .await
            .unwrap();
        assert!(orch.main_cadence.lock().unwrap().adopt(&claim, 0, 3 * HOUR));
        let lane = linux_lane();
        assert!(!orch.main_cadence.lock().unwrap().offer(
            &lane,
            &tip("p", "next"),
            Trigger::Startup,
            4 * HOUR
        ));
        let cause =
            "fedora's first report generation 9 omitted remote-request and resent no result";
        lose_adoption(&orch, &claim, std::slice::from_ref(&lane.check), cause).await;
        let lost = check_lane_adoptions::lookup(&db, &claim.request_id, &claim.attempt_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lost.state, "lost");
        assert_eq!(lost.evidence.as_deref(), Some(cause));
        let waiters = check_lane_adoptions::waiters(&db, &claim.request_id, &claim.attempt_id)
            .await
            .unwrap();
        let cadence = waiters
            .iter()
            .find(|w| w.consumer == "main_cadence")
            .unwrap();
        assert_eq!(cadence.state, "recorded_undelivered");
        assert_eq!(
            cadence.observation_id.as_deref(),
            Some("committed-before-loss")
        );
        assert_eq!(
            waiters
                .iter()
                .find(|w| w.consumer == "manual")
                .unwrap()
                .state,
            "lost"
        );
        assert_eq!(
            orch.main_cadence
                .lock()
                .unwrap()
                .lane_finished("p", &lane, 5 * HOUR),
            Some((Trigger::Startup, tip("p", "next")))
        );
        let interrupted = orch
            .main_cadence
            .lock()
            .unwrap()
            .take_interruption("p", &lane)
            .unwrap();
        assert!(orch
            .main_cadence
            .lock()
            .unwrap()
            .take_interruption("p", &lane)
            .is_none());
        forget_interruption(&db, "p", &interrupted).await;
        assert!(take_interruptions(&db).await.unwrap().is_empty());
    }

    #[test]
    fn a_lane_ended_by_the_restart_reads_as_cancelled_never_as_a_death() {
        let death = CadenceVerdict::Death {
            row: None,
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

    /// Lanes report on their own slots after the arm has ended, so a citation
    /// is dropped when the tip no longer declares its lane, not when an arm
    /// ends: a citation for a lane still running must survive the arm.
    #[test]
    fn a_citation_for_a_lane_the_tip_no_longer_declares_is_dropped() {
        let interruption = |check: &str| Interruption {
            check: check.into(),
            platform: "linux".into(),
            commit: "c".into(),
            stopped_at_unix_ms: 1,
        };
        let mut slots = CadenceSlots::default();
        slots.interrupted("p", interruption("retired-check"));
        slots.interrupted("p", interruption("rust-tests"));
        assert!(slots.request("p", "main", Trigger::Startup, LaneScope::All, 2));
        assert_eq!(slots.in_flight("p").unwrap().rearming.len(), 2);
        slots.forget_undeclared_interruptions("p", &[linux_lane(), macos_lane()]);
        assert_eq!(slots.finish("p", 3), None);
        assert!(slots.offer(&linux_lane(), &tip("p", "t"), Trigger::Startup, 3));
        assert_eq!(
            slots.in_flight("p").unwrap().rearming,
            vec![interruption("rust-tests")],
            "the running lane keeps its citation; the retired one is not re-armed forever"
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
                    notify: "testing".into(),
                    verdict: LaneVerdict::PerTest,
                },
                CadenceLane {
                    check: "rust-tests".into(),
                    platform: "macos".into(),
                    notify: "testing".into(),
                    verdict: LaneVerdict::PerTest,
                },
            ]
        );
    }

    /// The repository's own lanes are judged as their whole-suite commands
    /// warrant. `bun run test:frontend` names no runner, so frontend-tests is a
    /// per-test lane only because the parser knows the script; read as an exit
    /// lane it would compare no tests. And its whole-suite command must be the
    /// whole suite: the scoped form finds no changed files on main's tip and
    /// records an empty selection, which never qualifies as main's row.
    #[test]
    fn the_repository_lanes_are_judged_by_their_whole_suite_commands() {
        let config = include_str!("../../../../../.cairn/config.yaml");
        let (settings, _) = crate::config::project_settings::parse_project_settings(config)
            .expect("repository project config must parse");
        let contract = crate::config::project_settings::checks_contract_from(settings)
            .expect("repository must declare checks");
        let lanes = cadence_lanes(&contract.checks);
        let verdict = |check: &str| {
            lanes
                .iter()
                .find(|lane| lane.check == check && lane.platform == "linux")
                .map(|lane| lane.verdict)
        };
        for check in ["rust-tests", "frontend-tests"] {
            assert_eq!(verdict(check), Some(LaneVerdict::PerTest), "{check}");
        }
        for check in [
            "typecheck",
            "typecheck-ui",
            "frontend-build",
            "workspace-compile",
            "lint",
        ] {
            assert_eq!(verdict(check), Some(LaneVerdict::Exit), "{check}");
        }
        let frontend =
            crate::execution::selection::whole_suite_command(&contract.checks["frontend-tests"])
                .unwrap();
        assert_eq!(frontend, "bun run test:frontend");
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
                issue: Some("cairn://p/cairn/5100".into()),
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
            text.contains("- `422742274227` #4227 · cairn://p/cairn/5100"),
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

    /// Specimen c20b8a9a (cairn/5888): main's rust-tests for 872ff563 on fedora
    /// held four `mcp_attachment_resources` reds on `database is locked` as a
    /// host condition while its own discriminator read 0 of 4 passed on fedora
    /// at the compared row, and the same four were red on bgsrv-mac in the
    /// window (cairn/5348). An error that travels with the tree is an
    /// intermittent: still red and listed, never hidden under a host label.
    #[test]
    fn a_shared_error_the_discriminator_refuses_is_a_tree_intermittent_not_the_host() {
        let names = [
            "cairn-core::main$mcp_attachment_resources::reads_an_attachment",
            "cairn-core::main$mcp_attachment_resources::lists_attachments",
            "cairn-core::main$mcp_attachment_resources::reads_an_image",
            "cairn-core::main$mcp_attachment_resources::refuses_a_foreign_attachment",
        ];
        // Each under its own path, so only the shared error can group them.
        let locked = |n: usize| {
            failed(&format!(
                "called `Result::unwrap()` on an `Err` value: database is locked at /tmp/cairn-attachments-{n:04}/cairn.db"
            ))
        };
        let red: Vec<(&str, TestOutcome)> = names
            .iter()
            .enumerate()
            .map(|(n, name)| (*name, locked(n)))
            .collect();
        let passing: Vec<(&str, TestOutcome)> = names
            .iter()
            .map(|name| (*name, TestOutcome::Passed))
            .collect();
        let render = |previous: &[(&str, TestOutcome)],
                      neighbour: Option<&[(&str, TestOutcome)]>| {
            report(
                &CadenceVerdict::Complete(on(
                    "fedora",
                    row("cur", "872ff563872ff563", 5 * HOUR, &red),
                )),
                &Comparison::SameExecutor {
                    previous: on("fedora", row("prev", "aaaaaaaaaaaaaaaa", HOUR, previous)),
                    neighbour: neighbour.map(|tests| {
                        Box::new(on(
                            "bgsrv-mac",
                            row("mac", "cccccccccccccccc", 4 * HOUR, tests),
                        ))
                    }),
                    peer: None,
                    unread: Unread::default(),
                },
                &Bracket::default(),
            )
        };
        let listed = |text: &str| {
            for name in names {
                assert!(text.contains(&format!("  - `{name}`\n")), "{name}: {text}");
            }
        };

        // The specimen: red here one tip earlier, and red on the other host.
        let text = render(&red, Some(&red));
        assert!(!text.contains("HOST CONDITION"), "{text}");
        assert!(
            text.contains("4 failed — 0 newly red, 0 newly green"),
            "{text}"
        );
        assert!(text.contains("**Still red:** 4 (listed below)"), "{text}");
        assert!(
            text.contains("- **4 tests** — `database is locked` · red on `fedora` at the compared row too and on `bgsrv-mac` in the same window (cairn://p/cairn/check-observations/mac), so it is an intermittent in the tree, not a host condition on `fedora`"),
            "{text}"
        );
        listed(&text);

        // The discriminator failing is enough without another host.
        let text = render(&red, None);
        assert!(!text.contains("HOST CONDITION"), "{text}");
        assert!(
            text.contains("**Still red:** 4 (listed below)\n- **4 tests** — `database is locked` · red on `fedora` at the compared row too, so it is"),
            "{text}"
        );

        // Passing here one tip earlier is not enough when the other host
        // failed them on the same error: newly red, signed by that error.
        let text = render(&passing, Some(&red));
        assert!(!text.contains("HOST CONDITION"), "{text}");
        assert!(
            text.contains(
                "**NEWLY RED: 4 tests in 1 failure group**\n- **4 tests** — `database is locked`\n"
            ),
            "{text}"
        );
        listed(&text);

        // The other host failing them some other way says nothing of this error.
        let other_red: Vec<(&str, TestOutcome)> = names
            .iter()
            .map(|name| (*name, failed("assertion `left == right` failed")))
            .collect();
        let text = render(&passing, Some(&other_red));
        assert!(
            text.contains("**HOST CONDITION on `fedora`: 4 tests failed on `database is locked`**"),
            "{text}"
        );

        // A test already red here stays still red; the rest, which passed
        // here one tip earlier, are the host.
        let mut one_red = passing.clone();
        one_red[0].1 = locked(0);
        let text = render(&one_red, None);
        assert!(
            text.contains(
                "4 failed — 3 tests failed on one host condition: database is locked — 0 newly red"
            ),
            "{text}"
        );
        assert!(
            text.contains("Discriminator: 3 of 3 passed on `fedora`"),
            "{text}"
        );
        assert!(text.contains("**Still red:** 1 (not listed)"), "{text}");

        // With no row on this host, nothing separates the host from the tree.
        let text = report(
            &CadenceVerdict::Complete(on("fedora", row("cur", "872ff563872ff563", 5 * HOUR, &red))),
            &Comparison::Baseline,
            &Bracket::default(),
        );
        assert!(!text.contains("HOST CONDITION"), "{text}");
        assert!(
            text.contains(
                "**NEWLY RED: 4 tests in 1 failure group**\n- **4 tests** — `database is locked`\n"
            ),
            "{text}"
        );
    }

    /// Specimen cairn/5574: frontend-tests on a host at load 53-56 dropped
    /// cases in five unrelated `scripts/**` files, every one `Test timed out
    /// in 5000ms`, and main failed the same way on the same host.
    #[test]
    fn vitest_timeouts_across_files_are_one_host_condition_but_one_file_stays_red() {
        timeouts_group_only_across_files(
            "Error: Test timed out in 5000ms.\nIf this is a long-running test, pass a timeout value as the last argument or configure it globally with \"testTimeout\".\n    at scripts/lib/pinned-sources.test.ts:41:3",
        );
    }

    /// Specimen cairn/5583: vitest 4's JSON report carries a timeout only as
    /// `Error: STACK_TRACE_ERROR`, and the frontend runner rewrites that row to
    /// open with its `VITEST_TIMEOUT_EXCERPT`. It is the same host condition.
    #[test]
    fn the_frontend_runners_timeout_excerpt_is_the_same_host_condition() {
        timeouts_group_only_across_files(
            "TIMEOUT: vitest terminated this test at its timeout bound (testTimeout/hookTimeout in vitest.config.ts, or the test's own)\nran 14991 ms before termination\n    at /slot/src/components/chat/minimapCompositing.test.tsx:88:3",
        );
    }

    fn timeouts_group_only_across_files(excerpt: &str) {
        let timeout = || failed(excerpt);
        let names = [
            "scripts/check-executor-protocol.test.ts > protocol > matches the schema",
            "scripts/check-executor-protocol.test.ts > protocol > rejects drift",
            "scripts/release-workflows.test.ts > release > pins every action",
            "scripts/lib/skip-ledger.test.ts > ledger > reads the history",
            "src/components/Button.test.tsx > renders",
        ];
        let passing: Vec<(&str, TestOutcome)> = names
            .iter()
            .map(|name| (*name, TestOutcome::Passed))
            .collect();
        let previous = on("local", row("prev", "aaaaaaaaaaaaaaaa", HOUR, &passing));

        let mut loaded = passing.clone();
        for outcome in loaded.iter_mut().take(4) {
            outcome.1 = timeout();
        }
        let current = on("local", row("cur", "bbbbbbbbbbbbbbbb", 5 * HOUR, &loaded));
        let text = report(
            &CadenceVerdict::Complete(current),
            &same(&previous),
            &Bracket::default(),
        );
        assert!(
            text.contains("4 failed — 4 tests failed on one host condition: Test timed out — 0 newly red, 0 newly green"),
            "{text}"
        );
        assert_eq!(text.matches("**HOST CONDITION").count(), 1, "{text}");
        assert!(
            text.contains("**HOST CONDITION on `local`: 4 tests failed on `Test timed out`** — one time budget expiring across 4 tests in 3 unrelated files is this machine's load"),
            "{text}"
        );
        assert!(
            text.contains("Discriminator: 4 of 4 passed on `local` at the compared row"),
            "{text}"
        );
        assert!(!text.contains("NEWLY RED"), "{text}");

        // Timeouts confined to one file may be that file's own hang: two
        // cases there are still reds to look at, not the host.
        let mut one_file = passing.clone();
        one_file[0].1 = timeout();
        one_file[1].1 = timeout();
        let current = on("local", row("cur", "bbbbbbbbbbbbbbbb", 5 * HOUR, &one_file));
        let text = report(
            &CadenceVerdict::Complete(current),
            &same(&previous),
            &Bracket::default(),
        );
        assert!(text.contains("2 failed — 2 newly red"), "{text}");
        assert!(!text.contains("HOST CONDITION"), "{text}");

        let mut lone = passing.clone();
        lone[2].1 = timeout();
        let current = on("local", row("cur", "bbbbbbbbbbbbbbbb", 5 * HOUR, &lone));
        let text = report(
            &CadenceVerdict::Complete(current),
            &same(&previous),
            &Bracket::default(),
        );
        assert!(text.contains("1 failed — 1 newly red"), "{text}");
        assert!(!text.contains("HOST CONDITION"), "{text}");
    }

    /// Specimen cairn/5579 (54001404, fedora): 15 tests ended at nextest's
    /// slow-timeout bound in three modules of cairn-core's `main` binary, and
    /// other tests failed their assertions while the same freeze held them.
    /// Dated, the timeouts are one window and the failures inside it join it;
    /// a red outside the window stays a red.
    #[test]
    fn overlapping_timeouts_across_files_are_one_stall_window_with_its_bounds() {
        const TIMEOUT: &str = "TIMEOUT: nextest terminated this test at its slow-timeout bound (src-tauri/.config/nextest.toml)\nran 663s before termination";
        const MINUTE: i64 = 60_000;
        let names = [
            "cairn-core::main$turso_sync_roundtrip::case_0",
            "cairn-core::main$turso_sync_roundtrip::case_1",
            "cairn-core::main$turso_sync_roundtrip::case_2",
            "cairn-core::main$turso_sync_roundtrip::case_3",
            "cairn-core::main$pr_actions::two_next_branches_take_consecutive_numbers",
            "cairn-core::cairn_core$ingress::ws::tests::a_doorbell_poke_syncs_within_a_second",
            "cairn-exec::cairn_exec$tests::slot_adoption_holds_a_queued_request",
            "cairn-core::cairn_core$projects::export::tests::quick",
        ];
        let passing: Vec<(&str, TestOutcome)> = names
            .iter()
            .map(|name| (*name, TestOutcome::Passed))
            .collect();
        let previous = on("fedora", row("prev", "aaaaaaaaaaaaaaaa", HOUR, &passing));
        let render = |tests: &[(&str, TestOutcome)], lane_started_at_ms: Option<i64>| {
            let mut current = on("fedora", row("cur", "bbbbbbbbbbbbbbbb", 5 * HOUR, tests));
            current.lane_started_at_ms = lane_started_at_ms;
            report(
                &CadenceVerdict::Complete(current),
                &same(&previous),
                &Bracket::default(),
            )
        };

        let mut frozen = passing.clone();
        for (n, outcome) in frozen.iter_mut().take(4).enumerate() {
            outcome.1 = failed_at(TIMEOUT, 50 * MINUTE + n as i64 * 1_000, 663_000);
        }
        frozen[4].1 = failed_at(TIMEOUT, 52 * MINUTE, 600_000);
        frozen[5].1 = failed_at(
            "test ingress::ws::tests::a_doorbell_poke_syncs_within_a_second has been running for over 60 seconds\n\nthread 'ingress::ws::tests::a_doorbell_poke_syncs_within_a_second' panicked at os/cairn-core/src/ingress/ws.rs:88:9:\nno sync within a second",
            55 * MINUTE,
            108_000,
        );
        frozen[6].1 = failed_at("assertion `left == right` failed", 5 * MINUTE, 29);
        let origin = 1_793_346_970_320;
        let text = render(&frozen, Some(origin));
        assert!(
            text.contains("7 failed — 6 tests failed inside one stall window, 50m 00s to 1h 02m 00s into the lane — 1 newly red, 0 newly green"),
            "{text}"
        );
        assert_eq!(text.matches("**HOST CONDITION").count(), 1, "{text}");
        assert!(
            text.contains(&format!(
                "**HOST CONDITION on `fedora`: 6 tests failed inside one stall window, 50m 00s to 1h 02m 00s into the lane ({} to {})** — 5 tests in 2 unrelated files ran into a time bound while their runs overlapped, which is this machine stalling, not the tree; 1 more failed while running through the same window",
                stamp(origin + 50 * MINUTE),
                stamp(origin + 62 * MINUTE)
            )),
            "{text}"
        );
        assert!(
            text.contains("- All 5 that ended at a bound were in flight together from 52m 00s to 1h 01m 03s into the lane"),
            "{text}"
        );
        assert!(
            text.contains("Discriminator: 6 of 6 passed on `fedora` at the compared row"),
            "{text}"
        );
        assert!(
            text.contains("  - `cairn-core::main$turso_sync_roundtrip::case_0` · started 50m 00s, ran 11m 03s"),
            "{text}"
        );
        assert!(
            text.contains("  - `cairn-core::cairn_core$ingress::ws::tests::a_doorbell_poke_syncs_within_a_second` · started 55m 00s, ran 1m 48s"),
            "{text}"
        );
        // The red that ran an hour before the window is still the tree's.
        assert!(
            text.contains("**NEWLY RED: 1 test in 1 failure group**"),
            "{text}"
        );
        assert!(
            text.contains("`cairn-exec::cairn_exec$tests::slot_adoption_holds_a_queued_request`"),
            "{text}"
        );

        // Overlapping timeouts in one file may be that file's own hang.
        let mut one_file = passing.clone();
        for (n, outcome) in one_file.iter_mut().take(4).enumerate() {
            outcome.1 = failed_at(TIMEOUT, 50 * MINUTE + n as i64 * 1_000, 663_000);
        }
        let text = render(&one_file, None);
        assert!(text.contains("4 failed — 4 newly red"), "{text}");
        assert!(!text.contains("HOST CONDITION"), "{text}");

        // Timeouts in two files an hour apart are two reds, not one stall.
        let mut apart = passing.clone();
        apart[0].1 = failed_at(TIMEOUT, 5 * MINUTE, 600_000);
        apart[4].1 = failed_at(TIMEOUT, 80 * MINUTE, 600_000);
        let text = render(&apart, None);
        assert!(text.contains("2 failed — 2 newly red"), "{text}");
        assert!(!text.contains("HOST CONDITION"), "{text}");

        // A lone timeout is a red to look at.
        let mut lone = passing.clone();
        lone[4].1 = failed_at(TIMEOUT, 52 * MINUTE, 600_000);
        let text = render(&lone, None);
        assert!(text.contains("1 failed — 1 newly red"), "{text}");
        assert!(!text.contains("HOST CONDITION"), "{text}");

        // Undated, the same timeouts cannot be bracketed and stay reds.
        let mut undated = passing.clone();
        for outcome in undated.iter_mut().take(5) {
            outcome.1 = failed(TIMEOUT);
        }
        let text = render(&undated, None);
        assert!(text.contains("5 failed — 5 newly red"), "{text}");
        assert!(!text.contains("HOST CONDITION"), "{text}");
    }

    /// Specimen cairn/5859 (799ab564, bgsrv-mac): three tests in
    /// `mcp_run_execution` failed together when their own colocated executor
    /// fixture never came up. Two asserted on a batch reply carrying the
    /// fleet's refusal; the third failed its own assertion while they waited.
    /// In one module that is the file hanging on its own fixture and stays
    /// three reds; the same refusals in two modules are one window.
    #[test]
    fn overlapping_refusals_are_a_stall_window_only_across_files() {
        const SECOND: i64 = 1_000;
        const START: i64 = 40 * 60 * SECOND;
        const REFUSAL: &str = r#"{"text":"This run could not execute: this job's environment could not be reached (select execution environment executor: no colocated executor is configured, enrolled, or being supervised). No commands ran.","images":[]}"#;
        let resumes = "thread 'mcp_run_execution::the_turn_resumes_only_after_the_last_batch_settles' (189096284) panicked at os/cairn-core/tests/main/mcp_run_execution.rs:2232:5: fewer than 1 suspensions ever recorded a result; rows were [(\"toolu-quick\", \"pending\", None), (\"toolu-slow\", \"pending\", None)]".to_string();
        let partial = format!(
            "test mcp_run_execution::timed_out_code_item_is_killed_with_partial_output has been running for over 60 seconds  thread 'mcp_run_execution::timed_out_code_item_is_killed_with_partial_output' (189100590) panicked at os/cairn-core/tests/main/mcp_run_execution.rs:3070:5: missing partial output: {REFUSAL}"
        );
        let subtask = format!(
            "test mcp_run_execution::timed_out_item_in_subtask_job_creates_no_terminal has been running for over 60 seconds  thread 'mcp_run_execution::timed_out_item_in_subtask_job_creates_no_terminal' (189103165) panicked at os/cairn-core/tests/main/mcp_run_execution.rs:2552:5: expected timeout: {REFUSAL}"
        );
        assert_eq!(bound_ending(&resumes), None);
        assert_eq!(bound_ending(&partial), Some(BoundEnding::Refused));
        assert_eq!(bound_ending(&subtask), Some(BoundEnding::Refused));

        let render = |subtask_module: &str| {
            let names = [
                "cairn-core::main$mcp_run_execution::the_turn_resumes_only_after_the_last_batch_settles".to_string(),
                "cairn-core::main$mcp_run_execution::timed_out_code_item_is_killed_with_partial_output".to_string(),
                format!("cairn-core::main${subtask_module}::timed_out_item_in_subtask_job_creates_no_terminal"),
            ];
            let passing: Vec<(&str, TestOutcome)> = names
                .iter()
                .map(|name| (name.as_str(), TestOutcome::Passed))
                .collect();
            let previous = on("bgsrv-mac", row("prev", "aaaaaaaaaaaaaaaa", HOUR, &passing));
            let failing = [
                (names[0].as_str(), failed_at(&resumes, START, 33_877)),
                (
                    names[1].as_str(),
                    failed_at(&partial, START + 5 * SECOND, 75_413),
                ),
                (
                    names[2].as_str(),
                    failed_at(&subtask, START + 6 * SECOND, 75_565),
                ),
            ];
            report(
                &CadenceVerdict::Complete(on(
                    "bgsrv-mac",
                    row("cur", "bbbbbbbbbbbbbbbb", 5 * HOUR, &failing),
                )),
                &same(&previous),
                &Bracket::default(),
            )
        };

        // The row as it was: one module hanging on its own fixture.
        let text = render("mcp_run_execution");
        assert!(text.contains("3 failed — 3 newly red"), "{text}");
        assert!(!text.contains("HOST CONDITION"), "{text}");
        assert!(!text.contains("stall window"), "{text}");

        // The same refusals in two modules are the machine.
        let text = render("mcp_run_subtasks");
        assert!(
            text.contains("3 failed — 3 tests failed inside one stall window, 40m 05s to 41m 22s into the lane — 0 newly red"),
            "{text}"
        );
        assert_eq!(text.matches("**HOST CONDITION").count(), 1, "{text}");
        assert!(
            text.contains("— 2 tests in 2 unrelated files were refused with `no colocated executor is configured, enrolled, or being supervised` while their runs overlapped, which is this machine losing its executor, not the tree; 1 more failed while running through the same window"),
            "{text}"
        );
        assert!(
            text.contains("- All 2 that ended at a bound were in flight together from 40m 06s to 41m 20s into the lane"),
            "{text}"
        );
    }

    /// A refusal next to a timeout names both endings.
    #[test]
    fn a_window_of_timeouts_and_refusals_names_the_refusal() {
        const TIMEOUT: &str = "TIMEOUT: nextest terminated this test at its slow-timeout bound (src-tauri/.config/nextest.toml)";
        let refusal = format!("expected timeout: {}", crate::fleet::NO_COLOCATED_EXECUTOR);
        let previous = on(
            "fedora",
            row(
                "prev",
                "aaaaaaaaaaaaaaaa",
                HOUR,
                &[
                    ("c::m$a::x", TestOutcome::Passed),
                    ("c::m$b::y", TestOutcome::Passed),
                ],
            ),
        );
        let current = on(
            "fedora",
            row(
                "cur",
                "bbbbbbbbbbbbbbbb",
                5 * HOUR,
                &[
                    ("c::m$a::x", failed_at(TIMEOUT, 0, 600_000)),
                    ("c::m$b::y", failed_at(&refusal, 1_000, 75_000)),
                ],
            ),
        );
        let text = report(
            &CadenceVerdict::Complete(current),
            &same(&previous),
            &Bracket::default(),
        );
        assert!(
            text.contains("2 tests in 2 unrelated files ran into a time bound or were refused with `no colocated executor is configured, enrolled, or being supervised` (1 refused) while their runs overlapped, which is this machine stalling, not the tree"),
            "{text}"
        );
    }

    /// The grouper reads the refusal in the fleet's own words. When the fleet
    /// rewords it, the new spelling joins [`BOUND_ENDINGS`] beside the old one,
    /// which stored rows still carry.
    #[test]
    fn the_fleets_no_colocated_executor_refusal_is_a_bound_ending() {
        assert_eq!(
            bound_ending(crate::fleet::NO_COLOCATED_EXECUTOR),
            Some(BoundEnding::Refused)
        );
        assert_eq!(
            bound_ending(&crate::fleet::NO_COLOCATED_EXECUTOR.to_ascii_uppercase()),
            Some(BoundEnding::Refused)
        );
    }

    #[test]
    fn a_nextest_ids_file_is_its_module_and_a_vitest_ids_is_its_file() {
        assert_eq!(
            test_file("cairn-core::main$turso_sync_roundtrip::all_dbs_list"),
            "cairn-core::main$turso_sync_roundtrip"
        );
        assert_eq!(
            test_file("cairn-core::cairn_core$ingress::ws::tests::a_doorbell"),
            "cairn-core::cairn_core$ingress::ws::tests"
        );
        assert_eq!(test_file("cairn-cli::cli$top_level"), "cairn-cli::cli");
        assert_eq!(
            test_file("scripts/lib/a.test.ts > suite > case"),
            "scripts/lib/a.test.ts"
        );
        assert_eq!(test_file("src/b.test.ts"), "src/b.test.ts");
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
                    issue: Some("cairn://p/cairn/5119".into()),
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
                "- `111111111111` #4115 · cairn://p/cairn/5119 — Fix the index (cairn/5119) (#4115)"
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
            row: None,
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
    fn another_hosts_row_on_the_same_tree_splits_the_host_census_from_movement() {
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
        let peer = row(
            "other",
            "bbbbbbbbbbbbbbbb",
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
            neighbour: Some(Box::new(peer.clone())),
            peer: Some(Box::new(peer)),
            unread: Unread::default(),
        };
        let text = report(
            &CadenceVerdict::Complete(current),
            &comparison,
            &Bracket::default(),
        );
        assert!(
            text.contains("- Same tree on another host: cairn://p/cairn/check-observations/other, the cadence row on `bbs`"),
            "{text}"
        );
        // Movement is measured on fedora's own history only.
        assert!(text.contains("2 newly red, 1 newly green"), "{text}");
        assert!(
            text.contains("the previous cadence row on `fedora`"),
            "{text}"
        );
        assert!(
            text.contains("**1 test** · **one host**: passed on `bbs` — `left == right`"),
            "a red the other host passed on the same tree is flagged: {text}"
        );
        assert!(
            text.contains("**1 test** — `index out of bounds`"),
            "a red on both hosts is not one-host: {text}"
        );
        assert!(
            text.contains("**HOST CENSUS against `bbs`: 3**")
                && text.contains("(`bbs`, the same commit `bbbbbbbbbbbb`"),
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

    /// Specimen ac84ce84 (cairn/5975): fedora's rust-tests at 8a3cd116 tagged
    /// its new reds `one host: passed on bginf-sp2` against sp2's row at
    /// 2702605c, a tree from before the merges (#4717, #4721) that broke them;
    /// they were red on bgsrv-mac at 8a3cd116 too. A host is named only
    /// against another host's row on the same tree.
    #[test]
    fn another_hosts_row_on_an_older_tree_names_no_host() {
        const TIP: &str = "8a3cd1160000000000000000";
        const SP2_TREE: &str = "2702605c0000000000000000";
        let colocated = "cairn-exec::link_tests::a_colocated_ready_reprobes_the_managed_tools";
        let named = "cairn-core::resources::common::tests::artifact_affordance_falls_back_without_properties";
        let born = "cairn-core::resources::common::tests::a_test_born_in_the_bracket";
        let stuck = "cairn-core::stuck";
        let previous = on(
            "fedora",
            row(
                "prev",
                "aaaaaaaaaaaaaaaa",
                HOUR,
                &[
                    (colocated, TestOutcome::Passed),
                    (named, TestOutcome::Passed),
                    (stuck, failed("refresh computed twice")),
                ],
            ),
        );
        let sp2 = on(
            "bginf-sp2",
            row(
                "sp2",
                SP2_TREE,
                2 * HOUR,
                &[
                    (colocated, TestOutcome::Passed),
                    (named, TestOutcome::Passed),
                    (born, TestOutcome::Passed),
                    (stuck, TestOutcome::Passed),
                ],
            ),
        );
        let current = on(
            "fedora",
            row(
                "cur",
                TIP,
                3 * HOUR,
                &[
                    (colocated, failed("managed tools were not reprobed")),
                    (named, failed("assertion `left == right` failed")),
                    (born, failed("assertion failed: born red")),
                    (stuck, failed("refresh computed twice")),
                ],
            ),
        );
        let merge = |commit: &str, pr: i64| BracketMerge {
            commit: commit.to_string(),
            subject: format!("land (#{pr})"),
            pr: Some(pr),
            issue: None,
        };
        let bracket = Bracket {
            merges: vec![
                merge(SP2_TREE, 4715),
                merge("dde08b700000000000000000", 4717),
                merge("c1c1c1c10000000000000000", 4719),
                merge("c2c2c2c20000000000000000", 4720),
                merge(TIP, 4721),
            ],
            ..Bracket::default()
        };
        let render = |peer: Option<CadenceRow>| {
            report(
                &CadenceVerdict::Complete(current.clone()),
                &Comparison::SameExecutor {
                    previous: previous.clone(),
                    neighbour: Some(Box::new(sp2.clone())),
                    peer: peer.map(Box::new),
                    unread: Unread::default(),
                },
                &bracket,
            )
        };

        let text = render(None);
        assert!(
            text.contains("2 newly red, 0 newly green · first observed here: 1 red"),
            "{text}"
        );
        assert!(
            !text.contains("one host") && !text.contains("HOST CENSUS"),
            "an older tree's pass is not a host difference: {text}"
        );
        assert!(
            text.contains("- Same tree on another host: none. No other host has run commit `8a3cd1160000` on linux, so no red below is attributed to a host. The nearest other host's row, cairn://p/cairn/check-observations/sp2 on `bginf-sp2`, ran commit `2702605c0000`, 4 merges earlier (listed under **Merges since the compared row**)"),
            "the gap to the nearest other host's tree is named: {text}"
        );
        assert!(
            text.contains("#4717") && text.contains("#4721"),
            "the merges between are listed: {text}"
        );

        // A reference tree outside the bracket is still never a host claim.
        let mut elsewhere = sp2.clone();
        elsewhere.commit = "ffffffffffffffff".into();
        let text = report(
            &CadenceVerdict::Complete(current.clone()),
            &Comparison::SameExecutor {
                previous: previous.clone(),
                neighbour: Some(Box::new(elsewhere)),
                peer: None,
                unread: Unread::default(),
            },
            &bracket,
        );
        assert!(!text.contains("one host"), "{text}");
        assert!(
            text.contains("ran commit `ffffffffffff`, a different tree;"),
            "{text}"
        );

        // bgsrv-mac on the same tree, red the same way: the tree, no host.
        let mut mac = current.clone();
        mac.id = "mac".into();
        mac.reference = "cairn://p/cairn/check-observations/mac".into();
        let mac = on("bgsrv-mac", mac);
        let text = render(Some(mac.clone()));
        assert!(
            text.contains("- Same tree on another host: cairn://p/cairn/check-observations/mac, the cadence row on `bgsrv-mac`"),
            "{text}"
        );
        assert!(
            !text.contains("one host") && !text.contains("No other host"),
            "{text}"
        );

        // A same-tree host that passed them is the host difference, as before.
        let mut passing = mac;
        for outcome in passing.tests.values_mut() {
            *outcome = TestOutcome::Passed;
        }
        let text = render(Some(passing));
        assert!(
            text.contains("**1 test** · **one host**: passed on `bgsrv-mac`"),
            "{text}"
        );
        assert!(
            text.contains("- Red on `fedora`, passed on `bgsrv-mac`: 1\n  - `cairn-core::stuck`"),
            "{text}"
        );
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
            peer: None,
            unread: Unread::default(),
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
            row: None,
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
            verdict: LaneVerdict::PerTest,
        }
    }

    fn typecheck_lane() -> CadenceLane {
        CadenceLane {
            check: "typecheck".into(),
            platform: "linux".into(),
            notify: "testing".into(),
            verdict: LaneVerdict::Exit,
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
            async move { comparison_for(db, "p", "cairn", lane, current.as_ref(), None, &names).await }
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
                ..
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
                ..
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
                peer,
                ..
            } => {
                assert_eq!(previous.id, "f2");
                assert!(neighbour.is_none(), "{neighbour:?}");
                assert!(peer.is_none(), "no other host ran c-f3: {peer:?}");
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

        // Another host's row on f3's own commit is its peer; a second row on
        // that commit from f3's own host is not.
        let passed: &[(&str, &str)] = &[("a", "passed")];
        for (id, executor, ran_at) in [("b3", "bbs", 9), ("f3r", "fedora", 10)] {
            store(
                &db,
                Stored {
                    id,
                    executor,
                    ran_at,
                    cadence: "main",
                    commit: "c-f3",
                    selection: "full",
                    failure_kind: None,
                    tests: passed,
                },
            )
            .await;
        }
        match comparison(Some(load("f3").await)).await {
            Comparison::SameExecutor { previous, peer, .. } => {
                assert_eq!(previous.id, "f3r");
                assert_eq!(id_of(peer.as_deref()).as_deref(), Some("b3"));
            }
            other => panic!("expected a same-executor comparison, got {other:?}"),
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
            async move { comparison_for(db, "p", "cairn", lane, current.as_ref(), chain, &names).await }
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
            row: None,
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
                ..
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

    /// A base is judged by the nearest whole-suite row at or before it: a main
    /// cadence row on a later tip is off the base's chain and never answers,
    /// and a row on an agent branch never observed main.
    #[tokio::test]
    async fn a_base_is_judged_by_the_nearest_row_at_or_before_it() {
        let temp = tempfile::tempdir().unwrap();
        let db = cadence_db(&temp).await;
        let names = |executor: &str| executor.to_string();
        let red: &[(&str, &str)] = &[("a", "failed")];
        let green: &[(&str, &str)] = &[("a", "passed")];
        for (id, ran_at, cadence, commit, tests) in [
            ("cad-old", 1, "main", "old", red),
            ("man-q", 2, "manual", "q", red),
            // Main moved on and the test went green there after the base.
            ("cad-tip", 5, "main", "tip", green),
            ("side", 6, "review", "side", green),
        ] {
            store(
                &db,
                Stored {
                    id,
                    executor: "bbs",
                    ran_at,
                    cadence,
                    commit,
                    selection: "full",
                    failure_kind: None,
                    tests,
                },
            )
            .await;
        }
        // Main's first-parent chain read from the base, which no row observed.
        let chain = MainChain::from_log("base\nq\nold\n");
        let row = nearest_row_on_chain(&db, "p", "cairn", "rust-tests", "linux", &chain, &names)
            .await
            .unwrap()
            .expect("a row behind the base");
        assert_eq!(row.id, "man-q");
        assert!(matches!(row.tests.get("a"), Some(TestOutcome::Failed(_))));

        let none = nearest_row_on_chain(
            &db,
            "p",
            "cairn",
            "rust-tests",
            "linux",
            &MainChain::from_log("base\n"),
            &names,
        )
        .await
        .unwrap();
        assert!(none.is_none(), "no row at or before the base: {none:?}");
    }

    /// A test's rate on main is read over the newest ten whole-suite rows on
    /// the base's chain: a red row after the base, a narrowed selection, and a
    /// row older than the window are not counted.
    #[tokio::test]
    async fn a_tests_rate_on_main_reads_the_newest_rows_at_or_before_the_base() {
        let temp = tempfile::tempdir().unwrap();
        let db = cadence_db(&temp).await;
        // c0 is the newest commit at or before the base, c11 the oldest.
        let commits: Vec<String> = (0..12).map(|depth| format!("c{depth}")).collect();
        let red: &[(&str, &str)] = &[("a", "failed"), ("z", "passed")];
        let green: &[(&str, &str)] = &[("a", "passed"), ("z", "passed")];
        for (depth, commit) in commits.iter().enumerate() {
            let id = format!("row-{depth}");
            // Red at c1, c4 and c7 inside the window; c10 and c11 are outside it.
            let tests = if [1, 4, 7, 10, 11].contains(&depth) {
                red
            } else {
                green
            };
            store(
                &db,
                Stored {
                    id: &id,
                    executor: "bbs",
                    ran_at: 100 - depth as i64,
                    cadence: "main",
                    commit,
                    selection: "full",
                    failure_kind: None,
                    tests,
                },
            )
            .await;
        }
        for (id, commit, selection) in [("after-base", "tip", "full"), ("narrow", "c2", "filtered")]
        {
            store(
                &db,
                Stored {
                    id,
                    executor: "bbs",
                    ran_at: 200,
                    cadence: "main",
                    commit,
                    selection,
                    failure_kind: None,
                    tests: red,
                },
            )
            .await;
        }
        let chain = MainChain::from_log(&format!("base\n{}\n", commits.join("\n")));

        let rates = rates_on_chain(
            &db,
            "p",
            "rust-tests",
            "linux",
            &chain,
            &["a".to_string(), "never-ran".to_string()],
        )
        .await
        .unwrap();

        assert_eq!(rates.get("a"), Some(&TestRate { red: 3, ran: 10 }));
        assert_eq!(rates.get("never-ran"), None);
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
            Some("cairn://p/cairn/5119".to_string())
        );
        assert_eq!(subject_pr("No number here"), None);
        assert_eq!(subject_issue("notcairn/12 only", "cairn"), None);
        assert_eq!(
            subject_issue("CAIRN/12 shouted", "cairn"),
            Some("cairn://p/cairn/12".to_string())
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
            LaneVerdict::PerTest,
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

    /// A typecheck's row, as the store records one: no per-test rows, the
    /// result complete, the tsc parser's `unknown` selection.
    fn tsc_row(id: &str, commit: &str, ran_at_ms: i64, exit_code: i64) -> CadenceRow {
        let mut row = row(id, commit, ran_at_ms, &[]);
        row.exit_code = exit_code;
        row.verdict_passed = exit_code == 0;
        row.selection = Some("unknown".to_string());
        row
    }

    fn exit_report(verdict: &CadenceVerdict, comparison: &Comparison, bracket: &Bracket) -> String {
        render_report(&CadenceReport {
            check: "typecheck",
            platform: "linux",
            kind: LaneVerdict::Exit,
            commit: Some("bbbbbbbbbbbbbbbbbbbb"),
            verdict,
            comparison,
            bracket,
            now_ms: 10 * HOUR,
            rearmed: None,
        })
    }

    #[test]
    fn a_lane_takes_its_verdict_kind_from_the_command_it_runs() {
        let checks: HashMap<String, CheckCommand> = serde_yaml::from_str(
            "rust-tests:\n  command: bun run test:rust:tiered {changedFiles}\n  fullCommand: bun run test:rust:full\n  mainCadence:\n    platforms: [linux]\n    notify: testing\n\
             typecheck:\n  command: bunx --bun tsc --noEmit\n  mainCadence:\n    platforms: [linux]\n    notify: testing\n\
             frontend-build:\n  command: bunx --bun vite build\n  mainCadence:\n    platforms: [linux]\n    notify: testing\n",
        )
        .unwrap();
        let kinds: Vec<(String, LaneVerdict)> = cadence_lanes(&checks)
            .into_iter()
            .map(|lane| (lane.check, lane.verdict))
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("frontend-build".to_string(), LaneVerdict::Exit),
                ("rust-tests".to_string(), LaneVerdict::PerTest),
                ("typecheck".to_string(), LaneVerdict::Exit),
            ]
        );
    }

    /// cairn/5555: a typecheck reports no test, so judged as a test runner its
    /// every row was no verdict. Its exit is its verdict; a test runner's
    /// zero-test row is still not one.
    #[test]
    fn an_exit_lanes_row_is_judged_by_its_exit_and_a_test_runners_is_not() {
        assert!(matches!(
            LaneVerdict::Exit.classify(tsc_row("g", "c", 0, 0)),
            CadenceVerdict::Complete(row) if row.verdict_passed
        ));
        assert!(matches!(
            LaneVerdict::Exit.classify(tsc_row("r", "c", 0, 2)),
            CadenceVerdict::Complete(row) if !row.verdict_passed
        ));

        let mut unmeasured = tsc_row("u", "c", 0, 0);
        unmeasured.complete = false;
        assert!(matches!(
            LaneVerdict::Exit.classify(unmeasured),
            CadenceVerdict::NoVerdict { .. }
        ));
        let mut nothing_selected = tsc_row("e", "c", 0, 0);
        nothing_selected.selection = Some("empty".to_string());
        assert!(matches!(
            LaneVerdict::Exit.classify(nothing_selected),
            CadenceVerdict::NoVerdict { .. }
        ));

        assert!(matches!(
            LaneVerdict::PerTest.classify(tsc_row("g", "c", 0, 0)),
            CadenceVerdict::NoVerdict { .. }
        ));
    }

    /// The specimen: a merge that breaks `tsc --noEmit` after a green row
    /// posts the red, the error it printed, and the merge that broke it.
    #[test]
    fn a_newly_red_typecheck_names_its_error_and_the_merges_since_the_green() {
        let previous = tsc_row("green", "aaaaaaaaaaaaaaaaaaaa", 2 * HOUR, 0);
        let mut red = tsc_row("red", "bbbbbbbbbbbbbbbbbbbb", 3 * HOUR, 2);
        red.output_tail = "src/hooks/useBuildFabric.ts(749,7): error TS2322: Type '\"STRANDED\"' is not assignable to type 'BuildFabricDeviation'.\nFound 1 error.".to_string();
        let bracket = Bracket {
            merges: vec![BracketMerge {
                commit: "bbbbbbbbbbbbbbbbbbbb".into(),
                subject: "build fabric: strand detection (#4413)".into(),
                pr: Some(4413),
                issue: Some("cairn://p/cairn/5510".into()),
            }],
            ..Bracket::default()
        };
        let comparison = same(&previous);
        let verdict = LaneVerdict::Exit.classify(red);
        assert_eq!(
            observe(
                LaneVerdict::Exit,
                &verdict,
                &comparison,
                None,
                &Attempted::default()
            ),
            Observed::Row { news: true }
        );
        let text = exit_report(&verdict, &comparison, &bracket);
        assert!(
            text.starts_with(
                "**Main cadence · typecheck · linux on `bbs` · `bbbbbbbbbbbb`: failed (exit 2) — newly red**"
            ),
            "{text}"
        );
        assert!(text.contains("- Verdict: failed (exit 2)"), "{text}");
        assert!(!text.contains("Totals"), "{text}");
        assert!(text.contains("error TS2322"), "{text}");
        assert!(
            text.contains("cairn://p/cairn/check-observations/green, the previous cadence row"),
            "{text}"
        );
        assert!(text.contains("#4413 · cairn://p/cairn/5510"), "{text}");
        assert!(!text.contains("MAIN CADENCE DID NOT COMPLETE"), "{text}");
    }

    fn compile_row(id: &str, tail: &str) -> CadenceRow {
        let mut row = row(id, "cccccccccccccccccccc", HOUR, &[]);
        row.exit_code = 101;
        row.verdict_passed = false;
        row.output_tail = tail.to_string();
        row
    }

    /// Specimen a08e20f0: on a tree that compiled on the same host fifteen
    /// minutes earlier, a rust-tests row's compiler process was killed by
    /// SIGTERM. It is a death that names the signal and the crate and still
    /// cites its row and tail, never a testing-side "did not compile", and a
    /// compile error about the code stays the testing side's no-verdict.
    #[test]
    fn a_compile_killed_by_a_signal_is_a_death_and_a_compile_error_stays_a_no_verdict() {
        let verdict = classify_row(compile_row("killed", KILLED_RMCP_TAIL));
        let CadenceVerdict::Death {
            row: Some(row),
            cause,
        } = &verdict
        else {
            panic!("a killed compile is a death that keeps its row: {verdict:?}");
        };
        assert_eq!(row.id, "killed");
        assert!(
            cause.starts_with(
                "infrastructure: a compiler process in the lane was killed (SIGTERM) while compiling `rmcp`"
            ),
            "{cause}"
        );
        let text = report(&verdict, &Comparison::Baseline, &Bracket::default());
        assert!(
            text.starts_with("🚨 **MAIN CADENCE DID NOT COMPLETE"),
            "{text}"
        );
        assert!(text.contains("Cause class: **death**"), "{text}");
        assert!(text.contains("the tree is not implicated"), "{text}");
        assert!(!text.contains("no-verdict"), "{text}");
        assert!(
            !text.contains("a test target that did not compile"),
            "{text}"
        );
        assert!(
            text.contains("cairn://p/cairn/check-observations/killed"),
            "the row it left is cited: {text}"
        );
        assert!(
            text.contains("(signal: 15, SIGTERM: termination signal)"),
            "the tail is quoted: {text}"
        );
        assert!(
            matches!(
                cancelled_by_restart(verdict.clone(), Some(HOUR as u64)),
                CadenceVerdict::Death { row: Some(_), .. }
            ),
            "a death that left a row is not re-read as the restart's"
        );

        // An exit lane compiling the same tree (workspace-compile) is not red.
        let mut check = compile_row("check", KILLED_RMCP_TAIL);
        check.selection = Some("unknown".to_string());
        assert!(matches!(
            LaneVerdict::Exit.classify(check),
            CadenceVerdict::Death { row: Some(_), .. }
        ));

        let no_verdict = |tail: &str| {
            matches!(
                classify_row(compile_row("other", tail)),
                CadenceVerdict::NoVerdict { row: Some(_), .. }
            )
        };
        assert!(
            no_verdict(COMPILE_ERROR_TAIL),
            "a compile error is the tree's"
        );
        assert!(
            no_verdict(&format!("{COMPILE_ERROR_TAIL}\n{KILLED_RMCP_TAIL}")),
            "a diagnostic beside a killed compile is still the tree's"
        );
        let uncoded = "error: expected one of `,` or `}`, found `turn`\n   --> os/cairn-core/src/lib.rs:7:9\nerror: could not compile `cairn-core` (lib) due to 1 previous error";
        assert!(
            no_verdict(uncoded),
            "an uncoded diagnostic is the tree's too"
        );
        assert!(
            no_verdict(&format!("{uncoded}\n{DISK_FULL_TAIL}")),
            "a diagnostic beside a full disk is still the tree's"
        );
        assert!(
            no_verdict(&KILLED_RMCP_TAIL.replace(
                "signal: 15, SIGTERM: termination signal",
                "signal: 11, SIGSEGV: invalid memory reference"
            )),
            "a crash signal is the compiler's own outcome"
        );
        assert!(
            no_verdict(
                "error: test failed, to rerun pass `-p cairn-core --lib`\n\nCaused by:\n  process didn't exit successfully: `/target/debug/deps/cairn_core-0a1b` (signal: 9, SIGKILL: kill)"
            ),
            "a killed test binary is not a killed compile"
        );
        assert!(no_verdict(""), "a bare zero-test exit stays a no-verdict");
    }

    /// Specimen 6ebdcedb: a compile that failed because the host's disk was
    /// full is the host's death, though rustc counts the OS error as its one
    /// "previous error" and warnings beside it point at the source.
    #[test]
    fn a_compile_the_disk_refused_is_a_death_that_names_the_storage_error() {
        let verdict = classify_row(compile_row("disk", DISK_FULL_TAIL));
        let CadenceVerdict::Death {
            row: Some(_),
            cause,
        } = &verdict
        else {
            panic!("a full disk is a death that keeps its row: {verdict:?}");
        };
        assert!(
            cause.starts_with(
                "infrastructure: the host's storage failed (No space left on device) while compiling `cairn-core`"
            ),
            "{cause}"
        );
        let text = report(&verdict, &Comparison::Baseline, &Bracket::default());
        assert!(text.contains("Cause class: **death**"), "{text}");
        assert!(text.contains("the tree is not implicated"), "{text}");
        assert!(!text.contains("no-verdict"), "{text}");

        let io =
            DISK_FULL_TAIL.replace("No space left on device", "Input/output error (os error 5)");
        assert!(matches!(
            classify_row(compile_row("io", &io)),
            CadenceVerdict::Death { cause, .. } if cause.contains("(Input/output error)")
        ));
    }

    /// cairn/6279: a compile whose output writes the operating system refused
    /// (the external-host mirror's dep-info, outside any build slot) is a death
    /// naming the refusal through the store's predicate, and a compile error
    /// beside it stays the tree's.
    #[test]
    fn a_compile_whose_output_write_the_os_refused_is_a_death_that_names_the_refusal() {
        let verdict = classify_row(compile_row("denied", MIRROR_WRITE_DENIED_TAIL));
        let CadenceVerdict::Death {
            row: Some(_),
            cause,
        } = &verdict
        else {
            panic!("a refused output write is a death that keeps its row: {verdict:?}");
        };
        assert!(
            cause.starts_with(
                "infrastructure: the operating system refused the compiler's write of its build output (Operation not permitted) while compiling `cfg-if`"
            ),
            "{cause}"
        );
        assert!(
            cause.contains("recurs until that access changes"),
            "{cause}"
        );
        let text = report(&verdict, &Comparison::Baseline, &Bracket::default());
        assert!(text.contains("Cause class: **death**"), "{text}");
        assert!(text.contains("the tree is not implicated"), "{text}");
        assert!(matches!(
            classify_row(compile_row(
                "mixed",
                &format!("{COMPILE_ERROR_TAIL}\n{MIRROR_WRITE_DENIED_TAIL}")
            )),
            CadenceVerdict::NoVerdict { row: Some(_), .. }
        ));
    }

    /// cairn/5873: tm/230's lane, dead at cargo's fetch of a private git
    /// dependency on a machine with no credential, is reported as a death
    /// naming the repository, and a genuinely missing revision is not.
    #[test]
    fn a_dependency_fetch_without_a_credential_is_a_death_that_names_the_repository() {
        use crate::execution::checks::git_credential_fetch_specimens::{wrapped, TM_230_TAIL};
        let verdict = classify_row(compile_row("fetch", TM_230_TAIL));
        let CadenceVerdict::Death {
            row: Some(_),
            cause,
        } = &verdict
        else {
            panic!("a credential-less fetch is a death that keeps its row: {verdict:?}");
        };
        assert!(
            cause.starts_with(
                "infrastructure: fetching git dependency `https://github.com/bleugreen/tm-cards` failed for want of a credential for github.com"
            ),
            "{cause}"
        );
        assert!(!matches!(
            classify_row(compile_row("pin", &wrapped(""))),
            CadenceVerdict::Death { .. }
        ));
    }

    #[test]
    fn an_exit_lane_row_is_news_unless_a_green_follows_a_green() {
        let green = || tsc_row("g", "c", HOUR, 0);
        let red = || tsc_row("r", "c", HOUR, 2);
        let complete = |row| CadenceVerdict::Complete(row);
        let exit = |row: &CadenceRow, comparison: &Comparison| {
            row_is_news(LaneVerdict::Exit, row, comparison)
        };
        assert!(!exit(&green(), &same(&green())), "still green");
        assert!(
            !exit(&green(), &Comparison::OtherExecutor { previous: green() }),
            "still green across hosts"
        );
        assert!(exit(&green(), &same(&red())), "newly green");
        assert!(exit(&red(), &same(&red())), "still red");
        assert!(exit(&green(), &Comparison::Baseline), "baseline");
        let mut later = green();
        later.ran_at_ms += SILENCE_BOUND_MS;
        assert!(
            exit(&later, &same(&green())),
            "the first green after a silence"
        );

        let still_green = exit_report(&complete(green()), &same(&green()), &Bracket::default());
        assert!(
            still_green.contains(": passed — still green**"),
            "{still_green}"
        );
        assert!(!still_green.contains("Output tail"), "{still_green}");
    }

    fn hold(attempt: &str, retired: &str) -> CadenceVerdict {
        CadenceVerdict::Death {
            row: None,
            cause: format!(
                "infrastructure: ExecutorUnavailable: purpose main-cadence:00ace0d0:rust-tests:linux {PURPOSE_OWES_DEATH} after 120s; refusing to dispatch a second answer to one question. Outstanding: request 7ceec5e8 attempt {attempt}, retired {retired} ago while running, on fedora (connection generation 4); displaced by newer; awaiting its terminal outcome; settles when fedora reports it ended"
            ),
        }
    }

    fn inputs(hash: &str, suppressed: bool) -> Attempted {
        Attempted {
            inputs: Some(hash.into()),
            suppressed,
            ..Default::default()
        }
    }

    const MINUTE: i64 = 60 * 1000;

    /// cairn/5515: four wakes for one standing hold whose only change was the
    /// `retired Nm ago` counter and, once, the tip.
    #[test]
    fn an_unchanged_hold_wakes_once_and_a_changed_one_or_a_row_wakes_again() {
        let lane = linux_lane();
        let mut slots = CadenceSlots::default();
        let mut wake = |verdict: &CadenceVerdict, tip: &str, attempted: &Attempted, at: i64| {
            let observed = observe(
                LaneVerdict::PerTest,
                verdict,
                &Comparison::Baseline,
                Some(tip),
                attempted,
            );
            slots.wake("p", &lane, observed, at)
        };
        assert_eq!(
            wake(&hold("20e7c41c", "3m"), "t1", &inputs("i1", false), 0),
            Wake::Announce,
            "the first notice of a hold is unchanged"
        );
        assert_eq!(
            wake(
                &hold("20e7c41c", "12m"),
                "t1",
                &inputs("i1", false),
                12 * MINUTE
            ),
            Wake::Quiet
        );
        assert_eq!(
            wake(
                &hold("20e7c41c", "28m"),
                "t2",
                &inputs("i2", false),
                28 * MINUTE
            ),
            Wake::Quiet,
            "a new tip under the same hold folds into the eventual row's bracket"
        );
        assert_eq!(
            wake(
                &hold("20e7c41c", "1h 5m"),
                "t2",
                &inputs("i2", false),
                65 * MINUTE
            ),
            Wake::StillHeld { since_ms: 0 },
            "a standing hold re-announces once per bound"
        );
        assert_eq!(
            wake(
                &hold("20e7c41c", "1h 10m"),
                "t2",
                &inputs("i2", false),
                70 * MINUTE
            ),
            Wake::Quiet
        );
        assert_eq!(
            wake(
                &hold("750f243f", "1m"),
                "t2",
                &inputs("i2", false),
                71 * MINUTE
            ),
            Wake::Announce,
            "another attempt is another hold"
        );

        let green = row("g", "t3", 80 * MINUTE, &[("a", TestOutcome::Passed)]);
        let mut then = green.clone();
        then.ran_at_ms -= MINUTE;
        let row_wake = |slots: &mut CadenceSlots, comparison: &Comparison| {
            let observed = observe(
                LaneVerdict::PerTest,
                &CadenceVerdict::Complete(green.clone()),
                comparison,
                Some("t3"),
                &inputs("i3", false),
            );
            slots.wake("p", &lane, observed, 80 * MINUTE)
        };
        assert_eq!(
            row_wake(&mut slots, &same(&then)),
            Wake::Announce,
            "the first row after a hold ends it, however quiet"
        );
        assert_eq!(row_wake(&mut slots, &same(&then)), Wake::Quiet);

        let still = render_still_held(
            &CadenceReport {
                check: "rust-tests",
                platform: "linux",
                kind: LaneVerdict::PerTest,
                commit: Some("bbbbbbbbbbbbbbbbbbbb"),
                verdict: &hold("20e7c41c", "2h"),
                comparison: &Comparison::Baseline,
                bracket: &Bracket::default(),
                now_ms: 2 * HOUR + 5 * MINUTE,
                rearmed: None,
            },
            0,
        );
        assert!(
            still.starts_with(
                "🚨 **MAIN CADENCE STILL HELD · rust-tests · linux · `bbbbbbbbbbbb`** — still held for 2h 5m: the death first announced at "
            ),
            "{still}"
        );
        assert!(
            still.contains("request 7ceec5e8 attempt 20e7c41c"),
            "{still}"
        );
        assert!(still.contains("this line again in 1h."), "{still}");
    }

    /// cairn/5515: four suppressions re-announced verbatim when main's lanes
    /// were re-resolved, and a refusal re-delivered with only its staleness
    /// counters advanced.
    #[test]
    fn an_unchanged_suppression_or_refusal_wakes_no_one() {
        let lane = linux_lane();
        let mut slots = CadenceSlots::default();
        let mut wake = |cause: &str, attempted: &Attempted, at: i64| {
            let verdict = CadenceVerdict::Death {
                row: None,
                cause: cause.into(),
            };
            let observed = observe(
                LaneVerdict::PerTest,
                &verdict,
                &Comparison::Baseline,
                Some("8befeb6d"),
                attempted,
            );
            slots.wake("p", &lane, observed, at)
        };
        let suppressed = "suppressed: stopped after 3 infrastructure failures: bgsrv-ub (cpu was measured 573485ms ago), fedora (cpu was measured 143851ms ago)";
        assert_eq!(wake(suppressed, &inputs("i1", true), 0), Wake::Announce);
        assert_eq!(
            wake(suppressed, &inputs("i1", true), 3 * HOUR + 19 * MINUTE),
            Wake::Quiet,
            "a suppression re-reports only when it lifts or its inputs change"
        );
        assert_eq!(
            wake(suppressed, &inputs("i2", true), 3 * HOUR + 20 * MINUTE),
            Wake::Announce,
            "new inputs"
        );

        let refused = |ms: &str| {
            format!("NoMatchingExecutor: no linux executor can run it: fedora (cpu was measured {ms} ago)")
        };
        assert_eq!(
            wake(&refused("808815ms"), &inputs("i3", false), 4 * HOUR),
            Wake::Announce
        );
        assert_eq!(
            wake(&refused("809445ms"), &inputs("i3", false), 4 * HOUR + 1),
            Wake::Quiet,
            "only the staleness counter moved"
        );
        assert_eq!(
            wake(&refused("809445ms"), &inputs("i4", false), 4 * HOUR + 2),
            Wake::Announce,
            "the same refusal of other inputs is another question"
        );
    }

    /// cairn/5515: eight skip-census rows in fifty minutes, each `0 newly red,
    /// 0 newly green · still red: 0`.
    #[test]
    fn a_row_identical_to_its_predecessor_wakes_no_one_and_a_change_wakes() {
        let lane = linux_lane();
        let mut slots = CadenceSlots::default();
        let mut wake = |current: &CadenceRow, comparison: &Comparison| {
            let observed = observe(
                LaneVerdict::PerTest,
                &CadenceVerdict::Complete(current.clone()),
                comparison,
                Some(&current.commit),
                &inputs("i", false),
            );
            slots.wake("p", &lane, observed, current.ran_at_ms)
        };
        let green = [("a", TestOutcome::Passed), ("b", TestOutcome::Passed)];
        let b_red = [("a", TestOutcome::Passed), ("b", failed("boom"))];
        let b_skipped = [("a", TestOutcome::Passed), ("b", TestOutcome::Skipped)];
        let r1 = row("r1", "c1", 0, &green);
        let r2 = row("r2", "c2", 5 * MINUTE, &green);
        let r3 = row("r3", "c3", 10 * MINUTE, &b_red);
        let r4 = row("r4", "c4", 15 * MINUTE, &b_red);
        let r5 = row("r5", "c5", 20 * MINUTE, &b_skipped);
        assert_eq!(wake(&r1, &Comparison::Baseline), Wake::Announce, "baseline");
        assert_eq!(wake(&r2, &same(&r1)), Wake::Quiet, "identical green");
        assert_eq!(wake(&r3, &same(&r2)), Wake::Announce, "one newly red");
        assert_eq!(
            wake(&r4, &same(&r3)),
            Wake::Quiet,
            "an unchanged still-red set"
        );
        assert_eq!(
            wake(&r5, &same(&r4)),
            Wake::Announce,
            "a red test now skipped changes the still-red set"
        );
        let mut renamed = row("r6", "c6", 25 * MINUTE, &[("a", TestOutcome::Passed)]);
        assert_eq!(wake(&renamed, &same(&r2)), Wake::Announce, "a removed test");
        renamed.tests.insert("c".into(), failed("new"));
        assert_eq!(
            wake(
                &renamed,
                &same(&row("r0", "c0", 24 * MINUTE, &[("a", TestOutcome::Passed)]))
            ),
            Wake::Announce,
            "a first-observed red"
        );
    }

    #[test]
    fn the_first_row_on_a_host_or_after_the_silence_bound_wakes_even_when_identical() {
        let green = [("a", TestOutcome::Passed)];
        let before = row("r1", "c1", 0, &green);
        let news = |current: &CadenceRow, comparison: &Comparison| {
            row_is_news(LaneVerdict::PerTest, current, comparison)
        };
        assert!(!news(
            &row("r2", "c2", SILENCE_BOUND_MS - 1, &green),
            &same(&before)
        ));
        assert!(
            news(&row("r2", "c2", SILENCE_BOUND_MS, &green), &same(&before)),
            "a silence ended"
        );
        assert!(
            news(
                &on("fedora", row("r2", "c2", MINUTE, &green)),
                &Comparison::OtherExecutor {
                    previous: before.clone()
                }
            ),
            "the first row on a host"
        );
    }

    /// An exit lane's comparison rows are its own check's whole-command rows:
    /// the cadence's and manual runs. A review row may have run a narrowed
    /// command, a row that selected nothing measured nothing, and another
    /// check's rows are another question.
    #[tokio::test]
    async fn an_exit_lane_compares_with_whole_command_rows_of_its_own_check() {
        let temp = tempfile::tempdir().unwrap();
        let db = cadence_db(&temp).await;
        let names = |executor: &str| executor.to_string();
        let insert = |id: &str, check: &str, ran_at: i64, cadence: &str, selection: &str| {
            format!(
                "INSERT INTO check_result_observations
                   (id, project_id, commit_sha, tree_hash, check_name, input_hash,
                    environment_fingerprint, exit_code, verdict, complete, reusable,
                    parser_version, result_schema_version, ran_at, duration_ms, job_id,
                    cadence, output_tail, executor_id, verdict_platform, public_handle,
                    selection, deferred_count)
                 VALUES('{id}','p','c-{id}','t','{check}','i-{id}','env',0,'passed',1,0,
                        1,1,{ran_at},5,'main:p','{cadence}','','bbs','linux','h-{id}',
                        '{selection}',0);"
            )
        };
        let script = [
            insert("main-green", "typecheck", 1, "main", "unknown"),
            insert("manual-green", "typecheck", 2, "manual", "unknown"),
            insert("review-green", "typecheck", 3, "review", "unknown"),
            insert("empty", "typecheck", 4, "main", "empty"),
            insert("other-check", "typecheck-ui", 5, "main", "unknown"),
            insert("partial", "typecheck", 6, "review", "partial"),
        ]
        .concat();
        db.execute_script(script).await.unwrap();

        // Main's first-parent chain, tip first: the review and empty rows sit
        // nearest the tip, and neither may stand in.
        let chain = MainChain::from_log(
            "c-partial\nc-other-check\nc-empty\nc-review-green\nc-manual-green\nc-main-green\n",
        );
        let newest = latest_complete_row(
            &db,
            "p",
            "cairn",
            &typecheck_lane(),
            RowFilter {
                main_chain: Some(&chain),
                ..RowFilter::default()
            },
            &names,
        )
        .await
        .unwrap()
        .expect("a whole-command row on main qualifies");
        assert_eq!(newest.id, "manual-green");

        // This review execution also served a cadence waiter, whose durable
        // acceptance proves it answered the whole-command question.
        let accepted = vec!["review-green".to_string(), "partial".to_string()];
        let shared = latest_complete_row(
            &db,
            "p",
            "cairn",
            &typecheck_lane(),
            RowFilter {
                main_chain: Some(&chain),
                adopted_observations: Some(&accepted),
                ..RowFilter::default()
            },
            &names,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(shared.id, "review-green");
        assert_eq!(
            shared.cadence, "review",
            "the original producer is never relabelled"
        );

        // Without the chain only the cadence's own rows can be placed on main.
        let unchained = latest_complete_row(
            &db,
            "p",
            "cairn",
            &typecheck_lane(),
            RowFilter::default(),
            &names,
        )
        .await
        .unwrap()
        .expect("the cadence's own row qualifies");
        assert_eq!(unchained.id, "main-green");

        // Accepting only the partial row still cannot promote a selected slice
        // into a whole-command observation. All rows stay immutable.
        let accepted_partial = vec!["partial".to_string()];
        let partial = latest_complete_row(
            &db,
            "p",
            "cairn",
            &typecheck_lane(),
            RowFilter {
                main_chain: Some(&chain),
                adopted_observations: Some(&accepted_partial),
                ..RowFilter::default()
            },
            &names,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(partial.id, "manual-green");

        // None of these rows is a full per-test selection.
        let mut as_tests = typecheck_lane();
        as_tests.verdict = LaneVerdict::PerTest;
        assert!(latest_complete_row(
            &db,
            "p",
            "cairn",
            &as_tests,
            RowFilter {
                main_chain: Some(&chain),
                ..RowFilter::default()
            },
            &names
        )
        .await
        .unwrap()
        .is_none());
    }

    /// cairn/6244: a lookup that fails is unknown history. It never reads as a
    /// lane with no history, never invents movement, a merge list or host
    /// attribution, and never erases a predecessor another lookup established.
    #[test]
    fn a_failed_lookup_is_unknown_history_never_absent_history() {
        let broken =
            || Err::<Option<CadenceRow>, _>("database is locked\nat statement SELECT".to_string());
        let previous = on("sp2", all_passed("prev", HOUR));
        let elsewhere = on("sp1", all_passed("sp1", HOUR + 500));
        let current = on("sp2", all_passed("cur", HOUR + 1_000));
        let complete = CadenceVerdict::Complete(current.clone());
        let never_absent = |text: &str| {
            assert!(!text.contains("No earlier complete"), "{text}");
            assert!(!text.contains("has ever been recorded"), "{text}");
            assert!(!text.contains("is the baseline"), "{text}");
            assert!(
                !text.contains("locked\nat"),
                "the cause stays on one line: {text}"
            );
        };

        // The optional peer lookup fails after this host's previous row was
        // found: that row is still the comparison.
        let compared = resolve_comparison(
            Some("sp2"),
            Ok(Some(previous.clone())),
            Ok(Some(elsewhere.clone())),
            broken(),
            None,
        );
        match &compared {
            Comparison::SameExecutor {
                previous: kept,
                neighbour,
                peer,
                unread,
            } => {
                assert_eq!(kept.id, "prev", "the predecessor stands");
                assert_eq!(neighbour.as_deref().map(|row| row.id.as_str()), Some("sp1"));
                assert!(peer.is_none());
                assert!(
                    unread.peer.is_some() && unread.neighbour.is_none(),
                    "{unread:?}"
                );
            }
            other => panic!("expected the same-host comparison kept, got {other:?}"),
        }
        assert!(!row_is_news(LaneVerdict::PerTest, &current, &compared));
        let text = report(&complete, &compared, &Bracket::default());
        never_absent(&text);
        assert!(
            text.contains("all 1 passed — 0 newly red, 0 newly green"),
            "{text}"
        );
        assert!(
            text.contains("Compared with: cairn://p/cairn/check-observations/prev"),
            "{text}"
        );
        assert!(
            text.contains("- Same tree on another host: unknown. The lookup of another host's row on this commit failed (database is locked at statement SELECT), so no red below is attributed to a host."),
            "{text}"
        );
        assert!(text.contains("Merges since the compared row"), "{text}");

        // The neighbour lookup fails: the same-host diff stands, but how far
        // another host covers main is unknown, so no merge is listed uncovered.
        let compared = resolve_comparison(
            Some("sp2"),
            Ok(Some(previous.clone())),
            broken(),
            Ok(None),
            None,
        );
        let death = CadenceVerdict::Death {
            row: Some(current.clone()),
            cause: "stopped making progress".into(),
        };
        assert!(compared.coverage().is_none());
        assert!(bracket_base(&death, &compared).is_none());
        let text = report(&death, &compared, &Bracket::default());
        never_absent(&text);
        assert!(
            text.contains("- Last complete row: cairn://p/cairn/check-observations/prev"),
            "{text}"
        );
        assert!(
            text.contains("- Covered through: unknown. The lookup of the platform's newest complete row failed"),
            "{text}"
        );
        assert!(!text.contains("Merges not covered"), "{text}");

        // This host's own lookup fails: no comparison and the verdict as
        // measured, with the platform's newest cited as evidence of history.
        let compared = resolve_comparison(
            Some("sp2"),
            broken(),
            Ok(Some(elsewhere.clone())),
            Ok(None),
            None,
        );
        assert!(
            matches!(&compared, Comparison::Unavailable { known: Some(known), .. } if known.id == "sp1"),
            "{compared:?}"
        );
        assert!(bracket_base(&complete, &compared).is_none());
        assert!(row_is_news(LaneVerdict::PerTest, &current, &compared));
        let text = report(&complete, &compared, &Bracket::default());
        never_absent(&text);
        assert!(
            text.contains(
                ": all 1 passed — movement unknown: the compared row could not be read**"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "- Compared with: unknown. The lookup of this host's previous complete row failed"
            ),
            "{text}"
        );
        assert!(
            text.contains("History exists: the newest complete row on linux is cairn://p/cairn/check-observations/sp1"),
            "{text}"
        );
        assert!(!text.contains("newly"), "{text}");
        assert!(!text.contains("Merges"), "{text}");

        // A red row with no comparison lists its failures, none called new.
        let red = on(
            "sp2",
            row(
                "red",
                "bbbbbbbbbbbbbbbb",
                HOUR + 1_000,
                &[("t", failed("boom"))],
            ),
        );
        let text = report(
            &CadenceVerdict::Complete(red),
            &compared,
            &Bracket::default(),
        );
        assert!(
            text.contains("**RED IN THIS ROW: 1 test in 1 failure group**"),
            "{text}"
        );
        assert!(!text.to_lowercase().contains("newly red"), "{text}");

        // An exit lane's row moves nowhere either.
        let text = exit_report(&complete, &compared, &Bracket::default());
        never_absent(&text);
        assert!(
            text.contains("— movement unknown: the compared row could not be read**"),
            "{text}"
        );
        assert!(!text.contains("— baseline"), "{text}");

        // No row on this host, and the platform lookup fails: unknown, not a
        // cross-host comparison and not a baseline.
        let compared = resolve_comparison(Some("sp2"), Ok(None), broken(), Ok(None), None);
        assert!(
            matches!(&compared, Comparison::Unavailable { known: None, .. }),
            "{compared:?}"
        );

        // A death names no executor, so the platform lookup decides.
        let compared = resolve_comparison(None, Ok(None), broken(), Ok(None), None);
        let text = report(
            &CadenceVerdict::Death {
                row: None,
                cause: "no machine".into(),
            },
            &compared,
            &Bracket::default(),
        );
        never_absent(&text);
        assert!(
            text.contains("- Last complete row: unknown. The lookup of the platform's newest complete row failed (database is locked at statement SELECT), which does not mean none exists"),
            "{text}"
        );
        assert!(!text.contains("Merges not covered"), "{text}");

        // Controls: lookups that succeed and find nothing are a baseline, and a
        // prior complete row on another host is a cross-host comparison.
        let compared = resolve_comparison(Some("sp2"), Ok(None), Ok(None), Ok(None), None);
        assert!(matches!(compared, Comparison::Baseline), "{compared:?}");
        let text = report(&complete, &compared, &Bracket::default());
        assert!(
            text.contains(
                "No earlier complete whole-suite row on main exists for rust-tests on linux"
            ),
            "{text}"
        );
        let compared = resolve_comparison(
            Some("sp2"),
            Ok(None),
            Ok(Some(elsewhere.clone())),
            broken(),
            None,
        );
        assert!(
            matches!(&compared, Comparison::OtherExecutor { previous } if previous.id == "sp1"),
            "{compared:?}"
        );
    }

    /// cairn/6244: a candidate that cannot be read fails the lookups that reach
    /// it, and the comparison keeps what the other lookups found.
    #[tokio::test]
    async fn an_unreadable_candidate_leaves_history_unknown_and_keeps_the_predecessor() {
        let temp = tempfile::tempdir().unwrap();
        let db = cadence_db(&temp).await;
        let lane = linux_lane();
        let names = |executor: &str| executor.to_string();
        let passed: &[(&str, &str)] = &[("a", "passed")];
        let main_row = |id, executor, ran_at, commit| Stored {
            id,
            executor,
            ran_at,
            cadence: "main",
            commit,
            selection: "full",
            failure_kind: None,
            tests: passed,
        };
        store(&db, main_row("prev", "sp2", 1, "c-prev")).await;
        store(&db, main_row("peer", "sp1", 2, "c-cur")).await;
        store(&db, main_row("cur", "sp2", 3, "c-cur")).await;
        let current = load_row(&db, "cairn", "cur", &names).await.unwrap();
        let compare = || comparison_for(&db, "p", "cairn", &lane, Some(&current), None, &names);
        // A test id stored as a blob: the row is listed, then cannot be loaded.
        let unreadable = |id: &str| {
            format!(
                "INSERT INTO check_test_results (observation_id, test_id, status)
                 VALUES('{id}', X'00', 'passed');"
            )
        };

        match compare().await {
            Comparison::SameExecutor { previous, peer, .. } => {
                assert_eq!(previous.id, "prev");
                assert_eq!(peer.as_deref().map(|row| row.id.as_str()), Some("peer"));
            }
            other => panic!("expected a same-executor comparison, got {other:?}"),
        }

        // The other host's row cannot be read: the platform's newest and the
        // same-commit peer lookups fail, and this host's previous row stands.
        db.execute_script(unreadable("peer")).await.unwrap();
        let compared = compare().await;
        match &compared {
            Comparison::SameExecutor {
                previous, unread, ..
            } => {
                assert_eq!(previous.id, "prev");
                assert!(
                    unread
                        .neighbour
                        .as_deref()
                        .is_some_and(|cause| cause.contains("candidate peer could not be read")),
                    "{unread:?}"
                );
                assert!(unread.peer.is_some(), "{unread:?}");
            }
            other => panic!("expected the predecessor kept, got {other:?}"),
        }
        let text = report(
            &CadenceVerdict::Complete(current.clone()),
            &compared,
            &Bracket::default(),
        );
        assert!(
            text.contains("all 1 passed — 0 newly red, 0 newly green"),
            "{text}"
        );
        assert!(
            text.contains("- Same tree on another host: unknown."),
            "{text}"
        );
        assert!(!text.contains("No earlier complete"), "{text}");

        // This host's previous row cannot be read either: unknown history.
        db.execute_script(unreadable("prev")).await.unwrap();
        let compared = compare().await;
        assert!(
            matches!(&compared, Comparison::Unavailable { known: None, cause } if cause.contains("candidate prev could not be read")),
            "{compared:?}"
        );
        let text = report(
            &CadenceVerdict::Complete(current.clone()),
            &compared,
            &Bracket::default(),
        );
        assert!(text.contains("- Compared with: unknown."), "{text}");
        assert!(!text.contains("No earlier complete"), "{text}");
    }

    /// cairn/6244, the 2026-11-05 skip-census specimen: the 64 newest
    /// whole-suite rows before sp2's cadence row were all agent-branch write
    /// rows off main's chain, and main's own rows (sp1 three landings back, bbs
    /// seven, sp1 twelve) sat just behind them. Read by recency, every lookup
    /// found nothing and the report said no earlier row existed. Read by tree,
    /// they answer.
    #[tokio::test]
    async fn main_rows_behind_a_window_of_agent_write_rows_still_answer() {
        let temp = tempfile::tempdir().unwrap();
        let db = cadence_db(&temp).await;
        let lane = linux_lane();
        let names = |executor: &str| executor.to_string();
        let chain = MainChain::from_log(
            "tip\nm1\nm2\ne07\nm4\nm5\nm6\n5c7\nm8\nm9\nm10\nm11\nbaf\nm13\nm14\nold\n",
        );
        let passed: &[(&str, &str)] = &[("a", "passed")];
        let at = |id, executor, ran_at, cadence, commit| Stored {
            id,
            executor,
            ran_at,
            cadence,
            commit,
            selection: "full",
            failure_kind: None,
            tests: passed,
        };
        let tip_row = |executor: &str| {
            on(
                executor,
                row("cur", "tip", 10_000, &[("a", TestOutcome::Passed)]),
            )
        };
        let compare = |current: CadenceRow| {
            let (db, lane, chain) = (&db, &lane, &chain);
            async move {
                comparison_for(db, "p", "cairn", lane, Some(&current), Some(chain), &names).await
            }
        };

        // Control: lookups that succeed over an empty history are a baseline.
        assert!(matches!(
            compare(tip_row("sp2")).await,
            Comparison::Baseline
        ));

        for row in [
            at("sp2-old", "sp2", 100, "main", "old"),
            at("cf8", "sp1", 200, "main", "baf"),
            at("83e", "bbs", 300, "main", "5c7"),
            at("f26", "sp1", 400, "main", "e07"),
        ] {
            store(&db, row).await;
        }
        // More write rows than one read returns, every one newer than main's
        // rows and on an agent branch.
        for n in 0..CANDIDATE_SCAN + 6 {
            let (id, commit) = (format!("w{n}"), format!("agent-{n}"));
            let executor = if n % 2 == 0 { "sp1" } else { "sp2" };
            store(
                &db,
                Stored {
                    id: &id,
                    executor,
                    ran_at: 1_000 + n,
                    cadence: "write",
                    commit: &commit,
                    selection: "full",
                    failure_kind: None,
                    tests: passed,
                },
            )
            .await;
        }

        let current = tip_row("sp2");
        let compared = compare(current.clone()).await;
        match &compared {
            Comparison::SameExecutor {
                previous,
                neighbour,
                unread,
                ..
            } => {
                assert_eq!(previous.id, "sp2-old");
                assert_eq!(
                    neighbour.as_deref().map(|row| row.id.as_str()),
                    Some("f26"),
                    "three landings back is the nearest other host's tree"
                );
                assert!(!unread.any(), "{unread:?}");
            }
            other => panic!("expected sp2's own earlier row, got {other:?}"),
        }
        let text = report(
            &CadenceVerdict::Complete(current),
            &compared,
            &Bracket::default(),
        );
        assert!(
            text.contains("Compared with: cairn://p/cairn/check-observations/h-sp2-old"),
            "{text}"
        );
        assert!(!text.contains("No earlier complete"), "{text}");

        // Control: a host with no row of its own compares across hosts with
        // the nearest tree's prior complete main row.
        match compare(tip_row("sp3")).await {
            Comparison::OtherExecutor { previous } => assert_eq!(previous.id, "f26"),
            other => panic!("expected a cross-host comparison, got {other:?}"),
        }
    }

    /// A read that came back full with no complete row among it left what lies
    /// behind it unread: unknown, not absent.
    #[tokio::test]
    async fn a_full_window_without_a_complete_row_is_not_absence() {
        let temp = tempfile::tempdir().unwrap();
        let db = cadence_db(&temp).await;
        let lane = linux_lane();
        let names = |executor: &str| executor.to_string();
        insert_observation(&db, "good", "bbs", 1, "main", &[("a", "passed")]).await;
        // Complete-flagged rows with no tests: compile failures, never verdicts.
        for n in 0..CANDIDATE_SCAN {
            insert_observation(&db, &format!("z{n}"), "bbs", 2 + n, "main", &[]).await;
        }
        let error = latest_complete_row(&db, "p", "cairn", &lane, RowFilter::default(), &names)
            .await
            .unwrap_err();
        assert!(error.contains("older rows were not read"), "{error}");

        let compared = comparison_for(&db, "p", "cairn", &lane, None, None, &names).await;
        assert!(
            matches!(compared, Comparison::Unavailable { .. }),
            "{compared:?}"
        );
        let text = report(
            &CadenceVerdict::Death {
                row: None,
                cause: "no machine".into(),
            },
            &compared,
            &Bracket::default(),
        );
        assert!(text.contains("- Last complete row: unknown."), "{text}");
        assert!(!text.contains("has ever been recorded"), "{text}");
    }

    /// A typecheck lane is in flight for a minute or two, so the overview
    /// would list it beside rust-tests only by remembering what it reported.
    #[test]
    fn the_overview_keeps_each_declared_lanes_last_report() {
        let mut slots = CadenceSlots::default();
        let outcome = |check: &str, summary: &str, tip: &str, row_commit: &str| LaneOutcome {
            check: check.into(),
            platform: "linux".into(),
            tip: Some(tip.into()),
            summary: summary.into(),
            row: Some((
                format!("cairn://p/cairn/check-observations/{check}"),
                row_commit.into(),
            )),
            at_unix_ms: HOUR,
        };
        slots.settled("p", outcome("typecheck", "failed (exit 2)", "t1", "t1"));
        slots.settled("p", outcome("rust-tests", "78 failed", "t1", "t1"));
        slots.settled("p", outcome("typecheck", "passed", "t2", "t1"));
        slots.settled("q", outcome("typecheck", "passed", "t9", "t9"));

        let listed = slots.outcomes("p");
        assert_eq!(
            listed
                .iter()
                .map(|outcome| (outcome.check.as_str(), outcome.summary.as_str()))
                .collect::<Vec<_>>(),
            vec![("rust-tests", "78 failed"), ("typecheck", "passed")],
            "one line per lane, its latest report, in lane order"
        );
        assert_eq!(
            render_outcome_line(&listed[1], 3 * HOUR),
            "- typecheck on linux: passed at `t2`, 2h ago — cairn://p/cairn/check-observations/typecheck (the row on `t1`, whose inputs this tip shares).\n"
        );

        slots.forget_undeclared_outcomes("p", &[linux_lane()]);
        assert_eq!(
            slots.outcomes("p").len(),
            1,
            "typecheck is no longer declared"
        );
        assert_eq!(slots.outcomes("q").len(), 1, "projects are independent");
    }
}

#[cfg(test)]
mod pinned_check_read_tests {
    use super::*;

    #[test]
    #[serial_test::serial(jj)]
    fn immutable_main_ancestry_preserves_a_split_operation_log() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let home = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        crate::jj::tests::init_project(project.path());
        let jj = crate::jj::environment(&bin, home.path());
        let store = crate::jj::coordinate_repository(&jj, home.path(), project.path()).unwrap();
        let head = crate::jj::bookmark_commit(&jj, &store, "main").unwrap();
        let before = first_parent_ancestry(&jj, &store, &head).unwrap();
        let cairn_vcs::SettledOperation::Single(operation) =
            cairn_vcs::settled_operation(&store).unwrap()
        else {
            panic!("fixture is settled");
        };
        for name in ["left-read", "right-read"] {
            jj.run(
                &store,
                &[
                    "--at-op",
                    &operation,
                    "bookmark",
                    "create",
                    "-r",
                    &head,
                    name,
                    "--ignore-working-copy",
                ],
                "fork main ancestry fixture",
            )
            .unwrap();
        }
        assert!(jj.pin_settled_read(&store).is_err());
        let after = first_parent_ancestry(&jj, &store, &head).unwrap();
        assert_eq!(after.depth, before.depth);
        assert!(
            matches!(cairn_vcs::settled_operation(&store).unwrap(), cairn_vcs::SettledOperation::Split(heads) if heads.len() == 2)
        );
    }
}
