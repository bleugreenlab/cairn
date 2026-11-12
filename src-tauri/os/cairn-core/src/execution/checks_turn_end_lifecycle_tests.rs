//! Deterministic evidence-lifecycle schedules for one review wave's results.
//!
//! The production boundary under test is [`ReviewGroupRecorder`], the
//! `on_group_complete` callback the turn-end coordinator hands to placement. A
//! complete placement-group result enters it, real classification
//! (`run_planned_checks_selected`) decides each member's verdict, and three
//! separate named steps persist it: the immutable observation, the member's
//! log file, and any durability fault. Every schedule here drives that
//! recorder against a real migrated database file and reads every effect back
//! through a second database handle; a restart reopens the same file under a
//! new orchestrator and runs the startup reconciliation. The only test-owned
//! code in the path is the [`Boundary`] wrapped around the three steps, which
//! holds, fails, loses the response to, or crashes a step at an explicit event
//! the schedule releases. No step waits on elapsed time.
//!
//! Results arrive the way placement delivers them: each carries its fleet
//! request identity (`PublicationCoordination::for_request`), which is what
//! gives a re-delivered result the same observation id, and a member its
//! executor stopped arrives through the fleet's own conversion
//! (`check_result_from_cell_outcome`).
//!
//! The contract oracle ("Contract oracle" below) is written from the evidence
//! contract, not from the recorder: it reads schedule facts, the boundary's
//! record of what it injected, durable storage, log files, visible faults and
//! the acknowledgment the recorder publishes. Safety is checked after every
//! action; conditional progress only once every held write has been released.
//!
//! Out of reach of this seam: the coordinator's cancellation `select!` and
//! fleet transport, and a log write held inside the job's log-generation lock
//! (see [`Abstract::check`]). docs/formal-verification.md, "Generated
//! evidence-lifecycle schedules", describes the bounds, the campaign and how
//! to replay a counterexample.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use super::*;
use crate::execution::cache::{FreshCheckObservationWrite, InfraScopes, SealedCheckCoordinate};

const JOB: &str = "j-1";
const PROJECT: &str = "p";
const MEMBERS: [&str; 3] = ["alpha", "beta", "gamma"];
/// Two placement groups: one single-member group and one group of two, so a
/// group can be partly recorded when it stops.
const GROUPS: [&[usize]; 2] = [&[0], &[1, 2]];
const HEADS: [&str; 3] = ["head-0", "head-1", "head-2"];
const MAX_ACTIONS: usize = 12;
const MAX_RESTARTS: usize = 1;
const MAX_SUPERSEDES: usize = HEADS.len() - 1;
const MAX_HOLDS_PER_BOOT: usize = 2;
/// A deadlock guard, never an oracle: every wait is for an explicit event.
const SETTLE_DEADLINE: Duration = Duration::from_secs(60);
const PROCESS_STOPPED: &str = "the process stopped before this write";
const SHRINK_BUDGET: usize = 150;
/// Contract properties production is known to violate, each with the issue
/// that owns the repair. A hit is recorded and reported, never silently
/// relaxed; the fixed schedules assert each is still observed, so a repair
/// turns them red until its entry is removed here.
const KNOWN_OPEN: &[(&str, &str)] = &[("duplicate-idempotent-non-verdict", "cairn/6885")];

// ---- Schedules ----

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum Kind {
    Pass,
    Red,
    /// The executor stopped the command (a silent tree), whatever it exited with.
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum Step {
    Observation,
    Log,
}

impl Step {
    fn fault_name(self) -> &'static str {
        match self {
            Self::Observation => "observation",
            Self::Log => "log",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum Directive {
    Fail,
    Hold,
    /// The write commits and its caller is told it failed.
    LostResponse,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum Resolution {
    Pass,
    Fail,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "camelCase")]
enum Action {
    /// A complete result for one placement group of the live wave.
    Deliver {
        group: usize,
        kinds: Vec<Kind>,
    },
    /// The same result again, with the same request identity: the `of`-th
    /// delivery this wave received.
    Duplicate {
        of: usize,
    },
    /// The next write of this step for this member meets the directive.
    Arm {
        step: Step,
        member: usize,
        directive: Directive,
    },
    /// Release the oldest held write; nothing when nothing is held.
    Release {
        resolution: Resolution,
    },
    Cancel,
    /// Cancel the live wave, if any, and start the next, at a new head when
    /// `advance`.
    Supersede {
        advance: bool,
    },
    /// The process stops: every held write and every later write of this
    /// process has no effect. The next process reopens the same database.
    Restart,
}

/// A historical defect, injected through the persistence boundary, never
/// through a production switch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Defect {
    None,
    /// Before cairn/6083 the review log write discarded its error
    /// (`let _ = std::fs::write(..)`).
    SwallowLogErrors,
    /// The measurement is recorded against the branch's current head instead
    /// of the head its wave sealed (cairn/6091's wrong-head control).
    WrongHead,
}

#[derive(Clone, Default)]
struct AbsWave {
    live: bool,
    delivered: [bool; 2],
    deliveries: usize,
}

/// The schedule's own state, enough to decide which actions are valid. The
/// generator only proposes valid actions; [`validate`] refuses an invalid
/// schedule (a hand-edited replay, a shrink candidate) by name.
#[derive(Clone)]
struct Abstract {
    actions: usize,
    restarts: usize,
    supersedes: usize,
    holds_armed: usize,
    wave: Option<AbsWave>,
}

impl Abstract {
    fn initial() -> Self {
        Self {
            actions: 0,
            restarts: 0,
            supersedes: 0,
            holds_armed: 0,
            wave: Some(AbsWave {
                live: true,
                ..AbsWave::default()
            }),
        }
    }

    fn live_wave(&self) -> Result<&AbsWave, String> {
        self.wave.as_ref().filter(|wave| wave.live).ok_or_else(|| {
            "only a live wave receives results: this one was cancelled, or a restart left no \
             wave until the next is started"
                .to_string()
        })
    }

    fn check(&self, action: &Action) -> Result<(), String> {
        if self.actions >= MAX_ACTIONS {
            return Err(format!("a schedule is bounded at {MAX_ACTIONS} actions"));
        }
        match action {
            Action::Deliver { group, kinds } => {
                let wave = self.live_wave()?;
                let members = GROUPS
                    .get(*group)
                    .ok_or_else(|| format!("the plan has no placement group {group}"))?;
                if wave.delivered[*group] {
                    return Err(format!(
                        "group {group} was already delivered to this wave; the same result \
                         again is a Duplicate"
                    ));
                }
                if kinds.len() != members.len() {
                    return Err(format!(
                        "group {group} has {} members but the result names {}",
                        members.len(),
                        kinds.len()
                    ));
                }
            }
            Action::Duplicate { of } => {
                let wave = self.live_wave()?;
                if *of >= wave.deliveries {
                    return Err(format!(
                        "this wave received {} deliveries, so there is no delivery {of} to repeat",
                        wave.deliveries
                    ));
                }
            }
            Action::Arm {
                step,
                member,
                directive,
            } => {
                if *member >= MEMBERS.len() {
                    return Err(format!("the plan has no member {member}"));
                }
                match (step, directive) {
                    // The log write runs inside the job's log-generation lock
                    // (`with_current_review_log_generation`), so holding it
                    // would also hold a successor wave's registration.
                    (Step::Log, Directive::Hold) => {
                        return Err("a log write can fail but not be held: it runs inside the \
                                    job's log-generation lock"
                            .into())
                    }
                    (Step::Log, Directive::LostResponse) => {
                        return Err("a lost response belongs to the observation commit, not \
                                    to a file write"
                            .into())
                    }
                    (Step::Observation, Directive::Hold)
                        if self.holds_armed >= MAX_HOLDS_PER_BOOT =>
                    {
                        return Err(format!(
                            "a schedule holds at most {MAX_HOLDS_PER_BOOT} writes per process"
                        ))
                    }
                    _ => {}
                }
            }
            Action::Release { .. } => {}
            Action::Cancel => {
                self.live_wave()?;
            }
            Action::Supersede { .. } => {
                if self.supersedes >= MAX_SUPERSEDES {
                    return Err(format!(
                        "a schedule supersedes at most {MAX_SUPERSEDES} waves"
                    ));
                }
            }
            Action::Restart => {
                if self.restarts >= MAX_RESTARTS {
                    return Err(format!("a schedule restarts at most {MAX_RESTARTS} time"));
                }
            }
        }
        Ok(())
    }

    fn apply(&mut self, action: &Action) {
        self.actions += 1;
        match action {
            Action::Deliver { group, .. } => {
                let wave = self.wave.as_mut().expect("validated");
                wave.delivered[*group] = true;
                wave.deliveries += 1;
            }
            Action::Duplicate { .. } => self.wave.as_mut().expect("validated").deliveries += 1,
            Action::Arm {
                directive: Directive::Hold,
                ..
            } => self.holds_armed += 1,
            Action::Arm { .. } | Action::Release { .. } => {}
            Action::Cancel => self.wave.as_mut().expect("validated").live = false,
            Action::Supersede { .. } => {
                self.supersedes += 1;
                self.wave = Some(AbsWave {
                    live: true,
                    ..AbsWave::default()
                });
            }
            Action::Restart => {
                self.restarts += 1;
                self.holds_armed = 0;
                self.wave = None;
            }
        }
    }
}

fn validate(actions: &[Action]) -> Result<(), String> {
    let mut state = Abstract::initial();
    for (index, action) in actions.iter().enumerate() {
        state
            .check(action)
            .map_err(|error| format!("action {index} ({action:?}): {error}"))?;
        state.apply(action);
    }
    Ok(())
}

fn propose(rng: &mut StdRng, state: &Abstract, held_now: usize) -> Action {
    for _ in 0..1_000 {
        let action = match rng.gen_range(0..100) {
            0..=29 => {
                let group = rng.gen_range(0..GROUPS.len());
                let kinds = GROUPS[group]
                    .iter()
                    .map(|_| match rng.gen_range(0..100) {
                        0..=69 => Kind::Pass,
                        70..=84 => Kind::Red,
                        _ => Kind::Stopped,
                    })
                    .collect();
                Action::Deliver { group, kinds }
            }
            30..=37 => Action::Duplicate {
                of: rng.gen_range(0..3),
            },
            38..=57 => {
                let member = rng.gen_range(0..MEMBERS.len());
                if rng.gen_bool(0.3) {
                    Action::Arm {
                        step: Step::Log,
                        member,
                        directive: Directive::Fail,
                    }
                } else {
                    let directive = match rng.gen_range(0..10) {
                        0..=3 => Directive::Hold,
                        4..=6 => Directive::Fail,
                        _ => Directive::LostResponse,
                    };
                    Action::Arm {
                        step: Step::Observation,
                        member,
                        directive,
                    }
                }
            }
            58..=74 if held_now > 0 => Action::Release {
                resolution: if rng.gen_bool(0.8) {
                    Resolution::Pass
                } else {
                    Resolution::Fail
                },
            },
            58..=74 => continue,
            75..=82 => Action::Cancel,
            83..=94 => Action::Supersede {
                advance: rng.gen_bool(0.6),
            },
            _ => Action::Restart,
        };
        if state.check(&action).is_ok() {
            return action;
        }
    }
    unreachable!("an observation Fail is valid in every state")
}

// ---- Persistence boundary ----

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Effect {
    Wrote,
    Failed,
    LostResponse,
    Crashed,
}

/// What one write met, in the order writes happened.
#[derive(Clone, Debug)]
struct Applied {
    boot: usize,
    action: usize,
    step: Step,
    member: String,
    subject: String,
    effect: Effect,
    output: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Admission {
    Proceed,
    Fail,
    LostResponse,
    Crashed,
}

struct HeldStep {
    ticket: u64,
    step: Step,
    subject: String,
}

#[derive(Default)]
struct BoundaryState {
    armed: HashMap<(Step, String), Directive>,
    held: Vec<HeldStep>,
    released: HashMap<u64, Resolution>,
    crashed: bool,
    next_ticket: u64,
}

/// The reusable test seam: wraps [`ReviewPersistenceSteps`] so a schedule can
/// fail, hold, or lose the response to one named write, release a held write
/// as an explicit event, or stop the whole process at a step boundary. One per
/// process; writes of a stopped process have no effect.
struct Boundary {
    boot: usize,
    defect: Defect,
    state: Mutex<BoundaryState>,
    wake: Condvar,
    progress: tokio::sync::mpsc::UnboundedSender<()>,
    ledger: Arc<Mutex<Vec<Applied>>>,
    action: Arc<AtomicUsize>,
    branch_head: Arc<Mutex<String>>,
}

impl Boundary {
    fn arm(&self, step: Step, member: &str, directive: Directive) {
        self.lock()
            .armed
            .insert((step, member.to_string()), directive);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BoundaryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn held(&self) -> usize {
        self.lock().held.len()
    }

    fn crashed(&self) -> bool {
        self.lock().crashed
    }

    fn release_oldest(&self, resolution: Resolution) -> bool {
        let mut state = self.lock();
        if state.held.is_empty() {
            return false;
        }
        let held = state.held.remove(0);
        state.released.insert(held.ticket, resolution);
        drop(state);
        self.wake.notify_all();
        true
    }

    /// Stop the process. Returns the observation writes it held: results that
    /// had been received but had not committed.
    fn crash(&self) -> Vec<String> {
        let mut state = self.lock();
        state.crashed = true;
        let lost = state
            .held
            .drain(..)
            .filter(|held| held.step == Step::Observation)
            .map(|held| held.subject)
            .collect();
        drop(state);
        self.wake.notify_all();
        lost
    }

    fn admit(&self, step: Step, member: &str, subject: &str) -> Admission {
        let mut state = self.lock();
        if state.crashed {
            return Admission::Crashed;
        }
        match state.armed.remove(&(step, member.to_string())) {
            None => Admission::Proceed,
            Some(Directive::Fail) => Admission::Fail,
            Some(Directive::LostResponse) => Admission::LostResponse,
            Some(Directive::Hold) => {
                let ticket = state.next_ticket;
                state.next_ticket += 1;
                state.held.push(HeldStep {
                    ticket,
                    step,
                    subject: subject.to_string(),
                });
                let _ = self.progress.send(());
                drop(state);
                // The writers are synchronous and run on runtime workers. Like
                // `run_db_blocking`, a hold must hand this worker's queued
                // tasks to another thread first, or a held write strands
                // whatever was scheduled behind it.
                cairn_platform::runtime_regions::hand_off_and_block(|| {
                    self.wait_for_release(ticket)
                })
            }
        }
    }

    fn wait_for_release(&self, ticket: u64) -> Admission {
        let mut state = self.lock();
        loop {
            if let Some(resolution) = state.released.remove(&ticket) {
                return match resolution {
                    Resolution::Pass => Admission::Proceed,
                    Resolution::Fail => Admission::Fail,
                };
            }
            if state.crashed {
                return Admission::Crashed;
            }
            let (next, waited) = self
                .wake
                .wait_timeout(state, SETTLE_DEADLINE * 2)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next;
            if waited.timed_out() {
                // An abandoned schedule: stop rather than pin a thread.
                state.crashed = true;
                state.held.retain(|held| held.ticket != ticket);
            }
        }
    }

    fn record(
        &self,
        step: Step,
        member: &str,
        subject: &str,
        effect: Effect,
        output: Option<&str>,
    ) {
        self.ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Applied {
                boot: self.boot,
                action: self.action.load(Ordering::SeqCst),
                step,
                member: member.to_string(),
                subject: subject.to_string(),
                effect,
                output: output.map(str::to_string),
            });
    }

    fn misattribute(&self, observation: &mut FreshCheckObservationWrite) {
        if self.defect != Defect::WrongHead {
            return;
        }
        let head = self
            .branch_head
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        observation.commit_sha = head.clone();
        observation.defined_by_commit_sha = head.clone();
        observation.sealed_coordinate = SealedCheckCoordinate::new(
            head.clone(),
            head,
            observation.tree_hash.clone(),
            observation.input_hash.clone(),
        );
    }

    fn steps(self: &Arc<Self>) -> ReviewPersistenceSteps {
        let real = ReviewPersistenceSteps::default();

        let boundary = self.clone();
        let observation: ReviewObservationWriter = Arc::new(move |db, mut observation| {
            let member = observation.check_name.clone();
            let subject = observation.id.clone();
            let admission = boundary.admit(Step::Observation, &member, &subject);
            let record =
                |effect| boundary.record(Step::Observation, &member, &subject, effect, None);
            match admission {
                Admission::Crashed => {
                    record(Effect::Crashed);
                    Err(PROCESS_STOPPED.into())
                }
                Admission::Fail => {
                    record(Effect::Failed);
                    Err(format!("injected observation write failure for {member}"))
                }
                Admission::Proceed | Admission::LostResponse => {
                    boundary.misattribute(&mut observation);
                    match crate::execution::cache::record_fresh_check_observation(db, observation) {
                        Err(error) => {
                            record(Effect::Failed);
                            Err(error)
                        }
                        Ok(()) if admission == Admission::LostResponse => {
                            record(Effect::LostResponse);
                            Err(format!(
                                "injected lost response: {member}'s observation committed and \
                                 its writer reported failure"
                            ))
                        }
                        Ok(()) => {
                            record(Effect::Wrote);
                            Ok(())
                        }
                    }
                }
            }
        });

        let boundary = self.clone();
        let real_log = real.log.clone();
        let log: ReviewLogWriter = Arc::new(move |path, output| {
            let member = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or_default()
                .to_string();
            let subject = path.display().to_string();
            let record =
                |effect, output| boundary.record(Step::Log, &member, &subject, effect, output);
            match boundary.admit(Step::Log, &member, &subject) {
                Admission::Crashed => {
                    record(Effect::Crashed, None);
                    Err(PROCESS_STOPPED.into())
                }
                Admission::Fail => {
                    record(Effect::Failed, None);
                    if boundary.defect == Defect::SwallowLogErrors {
                        Ok(())
                    } else {
                        Err(format!("injected log write failure for {member}"))
                    }
                }
                Admission::Proceed | Admission::LostResponse => {
                    let written = real_log(path, output);
                    match &written {
                        Ok(()) => record(Effect::Wrote, Some(output)),
                        Err(_) => record(Effect::Failed, None),
                    }
                    written
                }
            }
        });

        let boundary = self.clone();
        let real_fact = real.fact.clone();
        let fact: ReviewFaultWriter = Arc::new(move |db, job, attempt, failure| {
            if boundary.crashed() {
                let stopped: futures_util::future::BoxFuture<'static, Result<(), String>> =
                    Box::pin(async { Err(PROCESS_STOPPED.to_string()) });
                return stopped;
            }
            real_fact(db, job, attempt, failure)
        });

        ReviewPersistenceSteps {
            observation,
            log,
            fact,
        }
    }
}

// ---- Driver over the production recorder ----

fn input_hash(member: &str, head: &str) -> String {
    format!("input-{member}@{head}")
}

fn tree_hash(head: &str) -> String {
    format!("tree-{head}")
}

fn plan(member: &str, head: &str) -> (CheckPlan, String) {
    let configured: crate::config::project_settings::CheckCommand =
        serde_yaml::from_str("command: 'true'").expect("check command parses");
    (
        CheckPlan {
            name: member.to_string(),
            applies: true,
            command: "true".into(),
            scope: crate::execution::selection::CheckScope::Full,
            resource_class: crate::config::project_settings::CheckResourceClass::Shared,
            verdict_environment_names: crate::execution::check_identity::verdict_environment_names(
                &configured,
            ),
            verdict_executor_ids: None,
            config_error: None,
            verdict_platforms: vec![crate::execution::check_identity::gating_platform().into()],
            verdict_runtime: crate::execution::check_identity::expected_verdict_runtime(
                &configured,
                crate::execution::checks::check_toolchain_identity(),
            ),
        },
        input_hash(member, head),
    )
}

fn group_results(
    group: usize,
    kinds: &[Kind],
    request: &(String, String),
) -> crate::execution::checks::CheckGroupResults {
    GROUPS[group]
        .iter()
        .zip(kinds)
        .map(|(&index, &kind)| {
            let name = MEMBERS[index];
            let publication = crate::fleet::PublicationCoordination::for_request(request.clone());
            let output = format!("{name}: {kind:?} result of {}\n", request.0);
            let result = match kind {
                Kind::Pass | Kind::Red => Ok(crate::mcp::handlers::run::CheckExecResult {
                    exit_code: Some(if kind == Kind::Pass { 0 } else { 1 }),
                    output,
                    timed_out: false,
                    duration_ms: Some(5),
                    provenance: None,
                    publication: Some(publication),
                }),
                Kind::Stopped => {
                    let mut metadata: cairn_common::executor_protocol::CellExecutionMeta =
                        serde_json::from_value(serde_json::json!({
                            "slotId": "slot", "cellEpoch": 1,
                            "startedAtUnixMs": 1, "finishedAtUnixMs": 2,
                        }))
                        .expect("execution metadata parses");
                    metadata.stop = Some(cairn_common::executor_protocol::CommandStop::Silent {
                        quiet_ms: 900_000,
                        last_output_unix_ms: None,
                        cpu_ms_in_window: None,
                        storage_io_bytes_in_window: None,
                        tree: Vec::new(),
                    });
                    crate::execution::checks::check_result_from_cell_outcome(
                        crate::fleet::CellOutcome::Completed {
                            request_id: request.0.clone(),
                            attempt_id: request.1.clone(),
                            exit_code: Some(0),
                            output,
                            timed_out: false,
                            metadata,
                            mutation_delta: None,
                            sandbox_denials: Vec::new(),
                            tracked_modifications: None,
                            node_scratch: None,
                        },
                        &crate::fleet::CellAccount::default(),
                        Some(publication),
                    )
                }
            };
            (index, result)
        })
        .collect()
}

/// Search plays no part in this lifecycle, and building a tantivy index is
/// the costliest part of an orchestrator fixture, so every schedule and every
/// restart in one test process shares one.
fn shared_search_index() -> Arc<crate::storage::SearchIndex> {
    static INDEX: std::sync::OnceLock<(tempfile::TempDir, Arc<crate::storage::SearchIndex>)> =
        std::sync::OnceLock::new();
    INDEX
        .get_or_init(|| {
            let dir = tempfile::tempdir().expect("search index dir");
            let index = crate::storage::SearchIndex::open_or_create(dir.path().join("search"))
                .expect("search index opens");
            (dir, Arc::new(index))
        })
        .1
        .clone()
}

fn build_orchestrator(db: Arc<LocalDb>, config_dir: &Path) -> Orchestrator {
    let db_state = Arc::new(crate::db::DbState::new(db, shared_search_index()));
    let services = Arc::new(crate::services::testing::TestServicesBuilder::new().build());
    crate::orchestrator::OrchestratorBuilder::new(db_state, services, config_dir.to_path_buf())
        .build()
}

struct WaveRun {
    attempt: String,
    head: String,
    boot: usize,
    recorder: Option<Arc<ReviewGroupRecorder>>,
    deliveries: Vec<usize>,
}

struct DeliveryRun {
    wave: usize,
    group: usize,
    request: (String, String),
    kinds: Vec<Kind>,
    done: Arc<AtomicBool>,
}

struct Process {
    index: usize,
    orch: Orchestrator,
    boundary: Arc<Boundary>,
}

struct Harness {
    defect: Defect,
    db_path: PathBuf,
    config_dir: PathBuf,
    observer: Arc<LocalDb>,
    process: Process,
    waves: Vec<WaveRun>,
    live: Option<usize>,
    deliveries: Vec<DeliveryRun>,
    head: usize,
    progress_tx: tokio::sync::mpsc::UnboundedSender<()>,
    progress_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    ledger: Arc<Mutex<Vec<Applied>>>,
    action: Arc<AtomicUsize>,
    branch_head: Arc<Mutex<String>>,
    seen: BTreeMap<String, Row>,
    restart_lost: Vec<String>,
    known_open: BTreeSet<String>,
    _dir: tempfile::TempDir,
}

impl Drop for Harness {
    fn drop(&mut self) {
        // An abandoned schedule must not pin a runtime thread in a held write.
        self.process.boundary.crash();
    }
}

impl Harness {
    async fn new(defect: Defect) -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let db_path = dir.path().join("review-lifecycle.db");
        let db = LocalDb::open_migrated_for_test(&db_path)
            .await
            .expect("migrated database");
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','proj','/tmp/repo',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
               VALUES('i','p',6117,'I','active','active','none',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
               VALUES('e','r','i','p','running',1,1);
             INSERT INTO jobs(id,project_id,issue_id,execution_id,status,node_name,uri_segment,created_at,updated_at)
               VALUES('j-1','p','i','e','running','builder','builder',1,1);",
        )
        .await
        .expect("seed rows");
        let config_dir = dir.path().join("home").join("config");
        std::fs::create_dir_all(config_dir.join("agents")).expect("config dir");
        let orch = build_orchestrator(Arc::new(db), &config_dir);
        let observer = Arc::new(LocalDb::open(&db_path).await.expect("observer handle"));
        let (progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel();
        let ledger = Arc::new(Mutex::new(Vec::new()));
        let action = Arc::new(AtomicUsize::new(0));
        let branch_head = Arc::new(Mutex::new(HEADS[0].to_string()));
        let boundary = Arc::new(Boundary {
            boot: 0,
            defect,
            state: Mutex::new(BoundaryState::default()),
            wake: Condvar::new(),
            progress: progress_tx.clone(),
            ledger: ledger.clone(),
            action: action.clone(),
            branch_head: branch_head.clone(),
        });
        let mut harness = Self {
            defect,
            db_path,
            config_dir,
            observer,
            process: Process {
                index: 0,
                orch,
                boundary,
            },
            waves: Vec::new(),
            live: None,
            deliveries: Vec::new(),
            head: 0,
            progress_tx,
            progress_rx,
            ledger,
            action,
            branch_head,
            seen: BTreeMap::new(),
            restart_lost: Vec::new(),
            known_open: BTreeSet::new(),
            _dir: dir,
        };
        harness.start_wave().await;
        harness
    }

    async fn start_wave(&mut self) {
        let index = self.waves.len();
        let attempt = format!("wave-{index}");
        let head = HEADS[self.head].to_string();
        let orch = &self.process.orch;
        let db = orch.db.local.clone();
        db.execute(
            "INSERT INTO turn_end_check_attempts (id,job_id,state,plan_json,created_at,updated_at)
             VALUES (?1,?2,'submitted','{}',?3,?3)",
            (attempt.as_str(), JOB, index as i64 + 1),
        )
        .await
        .expect("attempt row");
        db.execute(
            "UPDATE jobs SET turn_end_check_attempt_id=?1 WHERE id=?2",
            (attempt.as_str(), JOB),
        )
        .await
        .expect("current attempt");
        orch.register_review_log_generation(JOB, &attempt);
        let plans: Vec<_> = MEMBERS.iter().map(|member| plan(member, &head)).collect();
        let board = CheckStatusBoard::for_review_wave(&plans, || {});
        let recorder = ReviewGroupRecorder {
            orch: orch.clone(),
            db,
            project_id: PROJECT.into(),
            job_id: JOB.into(),
            attempt_id: attempt.clone(),
            evaluated: head.clone(),
            defined_by: head.clone(),
            tree_hash: tree_hash(&head),
            sealed_coordinates: plans
                .iter()
                .map(|(_, input)| {
                    SealedCheckCoordinate::new(&head, &head, tree_hash(&head), input.clone())
                })
                .collect(),
            infra_scopes: plans
                .iter()
                .map(|_| InfraScopes::head_only(JOB, &head))
                .collect(),
            plans,
            tool_use_id: format!("turn-checks:{JOB}"),
            board,
            log_dir: turn_end_log_dir(orch, JOB),
            steps: self.process.boundary.steps(),
            outcomes: Mutex::new(HashMap::new()),
        };
        self.waves.push(WaveRun {
            attempt,
            head,
            boot: self.process.index,
            recorder: Some(Arc::new(recorder)),
            deliveries: Vec::new(),
        });
        self.live = Some(index);
    }

    fn deliver(&mut self, group: usize, kinds: Vec<Kind>, request: Option<(String, String)>) {
        let wave = self.live.expect("validated: a live wave");
        let request =
            request.unwrap_or_else(|| (format!("req-w{wave}-g{group}"), "attempt-1".to_string()));
        let recorder = self.waves[wave]
            .recorder
            .clone()
            .expect("a live wave has its recorder");
        let results = group_results(group, &kinds, &request);
        let done = Arc::new(AtomicBool::new(false));
        let finished = done.clone();
        let progress = self.progress_tx.clone();
        tokio::spawn(async move {
            let callback = recorder.callback();
            callback(&results).await;
            finished.store(true, Ordering::SeqCst);
            let _ = progress.send(());
        });
        self.waves[wave].deliveries.push(self.deliveries.len());
        self.deliveries.push(DeliveryRun {
            wave,
            group,
            request,
            kinds,
            done,
        });
    }

    fn cancel(&mut self, reason: &str) {
        if let Some(wave) = self.live.take() {
            transition_turn_end_attempt(
                &self.process.orch,
                JOB,
                &self.waves[wave].attempt,
                "cancelled",
                Some(reason),
            )
            .expect("cancelled attempt");
        }
    }

    async fn apply(&mut self, index: usize, action: &Action) -> Result<(), Violation> {
        self.action.store(index, Ordering::SeqCst);
        match action {
            Action::Deliver { group, kinds } => self.deliver(*group, kinds.clone(), None),
            Action::Duplicate { of } => {
                let wave = self.live.expect("validated: a live wave");
                let original = &self.deliveries[self.waves[wave].deliveries[*of]];
                let (group, kinds, request) = (
                    original.group,
                    original.kinds.clone(),
                    original.request.clone(),
                );
                self.deliver(group, kinds, Some(request));
            }
            Action::Arm {
                step,
                member,
                directive,
            } => self
                .process
                .boundary
                .arm(*step, MEMBERS[*member], *directive),
            Action::Release { resolution } => {
                self.process.boundary.release_oldest(*resolution);
            }
            Action::Cancel => self.cancel("cancelled by the schedule"),
            Action::Supersede { advance } => {
                self.cancel("superseded by the next wave");
                if *advance {
                    self.head += 1;
                    *self
                        .branch_head
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        HEADS[self.head].to_string();
                }
                self.start_wave().await;
            }
            Action::Restart => return self.restart().await,
        }
        Ok(())
    }

    /// Wait until every delivery has either finished or is parked at a held
    /// write. Holds are the only way a delivery can stop moving, so this
    /// returns at an explicit event, never after an elapsed interval.
    async fn settle(&mut self) -> Result<(), Violation> {
        let deadline = Instant::now() + SETTLE_DEADLINE;
        loop {
            let moving = self
                .deliveries
                .iter()
                .filter(|delivery| !delivery.done.load(Ordering::SeqCst))
                .count();
            let held = self.process.boundary.held();
            if moving == held {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, self.progress_rx.recv()).await {
                Ok(_) => continue,
                Err(_) => {
                    return Err(Violation::new(
                        "settles-at-holds",
                        format!(
                            "{moving} deliveries unfinished with {held} writes held after \
                             {SETTLE_DEADLINE:?}"
                        ),
                    ))
                }
            }
        }
    }

    async fn restart(&mut self) -> Result<(), Violation> {
        let lost = self.process.boundary.crash();
        self.settle().await?;
        let interrupted = self
            .live
            .take()
            .map(|wave| self.waves[wave].attempt.clone());
        let stopped = self.process.index;
        for wave in &mut self.waves {
            if wave.boot == stopped {
                wave.recorder = None;
            }
        }
        let next = stopped + 1;
        let db = Arc::new(
            LocalDb::open(&self.db_path)
                .await
                .map_err(|error| Violation::new("restart-reopens-storage", error.to_string()))?,
        );
        let orch = build_orchestrator(db, &self.config_dir);
        reconcile_turn_end_attempts_on_startup(&orch).await;
        let boundary = Arc::new(Boundary {
            boot: next,
            defect: self.defect,
            state: Mutex::new(BoundaryState::default()),
            wake: Condvar::new(),
            progress: self.progress_tx.clone(),
            ledger: self.ledger.clone(),
            action: self.action.clone(),
            branch_head: self.branch_head.clone(),
        });
        self.process = Process {
            index: next,
            orch,
            boundary,
        };
        self.observer = Arc::new(
            LocalDb::open(&self.db_path)
                .await
                .map_err(|error| Violation::new("restart-reopens-storage", error.to_string()))?,
        );
        let rows = self.rows().await?;
        let ledger = self.ledger();
        for subject in &lost {
            let committed = ledger.iter().any(|applied| {
                &applied.subject == subject
                    && matches!(applied.effect, Effect::Wrote | Effect::LostResponse)
            });
            if !committed && rows.contains_key(subject) {
                return Err(Violation::new(
                    "restart-loses-uncommitted",
                    format!(
                        "{subject} was held before its commit when the process stopped, yet a \
                         row exists after the restart"
                    ),
                ));
            }
        }
        self.restart_lost.extend(lost);
        if let Some(attempt) = interrupted {
            let state = self
                .observer
                .query_opt_text(
                    "SELECT state || ':' || COALESCE(reason, '') FROM turn_end_check_attempts WHERE id=?1",
                    (attempt.clone(),),
                )
                .await
                .map_err(|error| Violation::new("restart-terminalizes-interrupted-wave", error.to_string()))?;
            let expected = format!("failed:{RESTART_INTERRUPTED_REASON}");
            if state.as_deref() != Some(expected.as_str()) {
                return Err(Violation::new(
                    "restart-terminalizes-interrupted-wave",
                    format!("wave {attempt} reads {state:?} after the restart, not {expected:?}"),
                ));
            }
        }
        Ok(())
    }

    async fn drain(&mut self) -> Result<(), Violation> {
        while self.process.boundary.release_oldest(Resolution::Pass) {
            self.settle().await?;
        }
        self.settle().await
    }

    fn ledger(&self) -> Vec<Applied> {
        self.ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    async fn rows(&self) -> Result<BTreeMap<String, Row>, Violation> {
        self.observer
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT id, commit_sha, check_name, input_hash, verdict, failure_kind
                             FROM check_result_observations ORDER BY id",
                            (),
                        )
                        .await?;
                    let mut found = BTreeMap::new();
                    while let Some(row) = rows.next().await? {
                        found.insert(
                            row.text(0)?,
                            Row {
                                commit: row.text(1)?,
                                check: row.text(2)?,
                                input: row.text(3)?,
                                verdict: row.text(4)?,
                                failure_kind: row.opt_text(5)?,
                            },
                        );
                    }
                    Ok(found)
                })
            })
            .await
            .map_err(|error| Violation::new("storage-readable", error.to_string()))
    }

    async fn snapshot(&self) -> Result<Snapshot, Violation> {
        let rows = self.rows().await?;
        let log_dir = turn_end_log_dir(&self.process.orch, JOB);
        let logs = MEMBERS
            .iter()
            .map(|member| {
                let path = log_dir.join(format!("{}.log", sanitize_log_name(member)));
                (member.to_string(), std::fs::read_to_string(path).ok())
            })
            .collect();
        let mut faults = BTreeSet::new();
        let mut acks = Vec::new();
        for (index, wave) in self.waves.iter().enumerate() {
            let Some(recorder) = &wave.recorder else {
                continue;
            };
            for failure in recorder.board.durability_errors() {
                faults.insert((failure.member, failure.step));
            }
            let outcomes = recorder
                .outcomes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (&member, outcome) in outcomes.iter() {
                acks.push(Ack {
                    wave: index,
                    member,
                    observation: outcome
                        .recorded
                        .as_ref()
                        .map(|recorded| recorded.id.clone()),
                });
            }
        }
        for failure in self.process.orch.review_durability_errors(JOB) {
            faults.insert((failure.member, failure.step));
        }
        let mut stored_faults = BTreeSet::new();
        for failure in turn_end_durability_errors(&self.observer, JOB)
            .await
            .map_err(|error| Violation::new("faults-readable", error))?
        {
            faults.insert((failure.member.clone(), failure.step.clone()));
            stored_faults.insert((failure.member, failure.step));
        }
        let mut reusable = BTreeMap::new();
        for head in &HEADS[..=self.head] {
            for member in MEMBERS {
                let (plan, input) = plan(member, head);
                if let Some(reused) = crate::execution::checks::reusable_verdict_for_plan(
                    self.observer.clone(),
                    PROJECT,
                    &plan,
                    &input,
                    TURN_END_EFFECTIVE_ENV,
                ) {
                    reusable.insert(
                        (member.to_string(), head.to_string()),
                        Reused {
                            passed: reused.entry.passed,
                            source: reused.source_observation_id,
                        },
                    );
                }
            }
        }
        Ok(Snapshot {
            rows,
            logs,
            faults,
            stored_faults,
            acks,
            reusable,
        })
    }

    fn facts(&self) -> Facts {
        Facts {
            process: self.process.index,
            waves: self
                .waves
                .iter()
                .map(|wave| WaveFact {
                    head: wave.head.clone(),
                    answering: wave.recorder.is_some(),
                })
                .collect(),
            deliveries: self
                .deliveries
                .iter()
                .map(|delivery| DeliveryFact {
                    wave: delivery.wave,
                    group: delivery.group,
                    request: delivery.request.clone(),
                    kinds: delivery.kinds.clone(),
                })
                .collect(),
            ledger: self.ledger(),
        }
    }

    async fn check_safety(&mut self) -> Result<Snapshot, Violation> {
        let snapshot = self.snapshot().await?;
        contract_safety(&self.facts(), &snapshot, &self.seen, &mut self.known_open)?;
        self.seen.extend(
            snapshot
                .rows
                .iter()
                .map(|(id, row)| (id.clone(), row.clone())),
        );
        Ok(snapshot)
    }

    async fn step(&mut self, index: usize, action: &Action) -> Result<(), Violation> {
        self.apply(index, action).await?;
        self.settle().await?;
        self.check_safety().await.map(|_| ())
    }

    /// Release every held write, then check safety and conditional progress.
    async fn finish(&mut self, actions: usize) -> Result<Snapshot, Violation> {
        self.action.store(actions, Ordering::SeqCst);
        self.drain().await?;
        let snapshot = self.check_safety().await?;
        contract_progress(&self.facts(), &snapshot)?;
        Ok(snapshot)
    }
}

// ---- Contract oracle ----
//
// Written from the evidence contract (cairn/6091's properties, cairn/6083
// requirement 7), over facts the schedule knows and effects read back from
// storage. It never consults the recorder's reasoning, only the
// acknowledgment the recorder publishes.

#[derive(Clone, Debug, PartialEq, Eq)]
struct Row {
    commit: String,
    check: String,
    input: String,
    verdict: String,
    failure_kind: Option<String>,
}

struct Ack {
    wave: usize,
    member: usize,
    observation: Option<String>,
}

struct Snapshot {
    rows: BTreeMap<String, Row>,
    logs: BTreeMap<String, Option<String>>,
    /// Visible to the running process: board, orchestrator memory or storage.
    faults: BTreeSet<(String, String)>,
    /// Durable on an attempt row, so they outlive the process.
    stored_faults: BTreeSet<(String, String)>,
    acks: Vec<Ack>,
    /// What the next wave at these inputs would reuse instead of running.
    reusable: BTreeMap<(String, String), Reused>,
}

/// A verdict reuse would answer with: a pass or a red, and its source row.
struct Reused {
    passed: bool,
    source: String,
}

struct WaveFact {
    head: String,
    /// Its recorder still answers: the wave belongs to the running process.
    answering: bool,
}

struct DeliveryFact {
    wave: usize,
    group: usize,
    request: (String, String),
    kinds: Vec<Kind>,
}

struct Facts {
    process: usize,
    waves: Vec<WaveFact>,
    deliveries: Vec<DeliveryFact>,
    ledger: Vec<Applied>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Violation {
    property: String,
    detail: String,
}

impl Violation {
    fn new(property: &str, detail: impl Into<String>) -> Self {
        Self {
            property: property.to_string(),
            detail: detail.into(),
        }
    }
}

/// A delivered result's stable identity: its fleet request and the member.
fn measurement_id(request: &(String, String), member: &str) -> String {
    format!("remote-check:{}:{}:{member}", request.0, request.1)
}

fn measured(input: &str) -> Option<(&str, &str)> {
    input.strip_prefix("input-")?.split_once('@')
}

fn delivered_kinds(facts: &Facts) -> BTreeMap<String, (usize, usize, Kind)> {
    let mut kinds = BTreeMap::new();
    for delivery in &facts.deliveries {
        for (&member, &kind) in GROUPS[delivery.group].iter().zip(&delivery.kinds) {
            kinds.insert(
                measurement_id(&delivery.request, MEMBERS[member]),
                (delivery.wave, member, kind),
            );
        }
    }
    kinds
}

fn contract_safety(
    facts: &Facts,
    snapshot: &Snapshot,
    seen: &BTreeMap<String, Row>,
    known_open: &mut BTreeSet<String>,
) -> Result<(), Violation> {
    let mut violated = |violation: Violation| -> Result<(), Violation> {
        if KNOWN_OPEN
            .iter()
            .any(|(property, _)| *property == violation.property)
        {
            known_open.insert(violation.property);
            Ok(())
        } else {
            Err(violation)
        }
    };
    // Durable evidence never disappears or changes: not on cancel, supersede
    // or restart.
    for (id, row) in seen {
        if snapshot.rows.get(id) != Some(row) {
            return Err(Violation::new(
                "retention",
                format!("{id} was {row:?} and is now {:?}", snapshot.rows.get(id)),
            ));
        }
    }
    // Every row stays attached to the head and input it measured.
    for (id, row) in &snapshot.rows {
        match measured(&row.input) {
            Some((member, head)) if row.check == member && row.commit == head => {}
            _ => {
                return Err(Violation::new(
                    "measured-identity",
                    format!(
                        "{id} for {} at {} carries input {}",
                        row.check, row.commit, row.input
                    ),
                ))
            }
        }
    }
    // Only a completed exit-zero measurement is a pass; a stopped one is a
    // non-verdict whatever it exited with.
    let kinds = delivered_kinds(facts);
    for (id, row) in &snapshot.rows {
        let passed = row.verdict == "passed" && row.failure_kind.is_none();
        let expected = match kinds.get(id) {
            Some((wave, member, kind)) => {
                if facts.waves[*wave].head != row.commit || MEMBERS[*member] != row.check {
                    return Err(Violation::new(
                        "measured-identity",
                        format!(
                            "{id} belongs to {} at {}, recorded as {} at {}",
                            MEMBERS[*member], facts.waves[*wave].head, row.check, row.commit
                        ),
                    ));
                }
                match kind {
                    Kind::Pass => passed,
                    Kind::Red => row.verdict == "failed" && row.failure_kind.is_none(),
                    Kind::Stopped => !passed && row.failure_kind.is_some(),
                }
            }
            None => {
                !passed
                    || facts.deliveries.iter().any(|delivery| {
                        facts.waves[delivery.wave].head == row.commit
                            && GROUPS[delivery.group]
                                .iter()
                                .zip(&delivery.kinds)
                                .any(|(&m, &k)| MEMBERS[m] == row.check && k == Kind::Pass)
                    })
            }
        };
        if !expected {
            return Err(Violation::new(
                "no-false-pass",
                format!(
                    "{id} for {} reads {} ({:?})",
                    row.check, row.verdict, row.failure_kind
                ),
            ));
        }
    }
    // Reuse answers only from a durable genuine verdict at the same inputs: a
    // reused pass from a delivered pass, a reused red from a delivered red,
    // and a stopped member never.
    for ((member, head), reused) in &snapshot.reusable {
        let source = snapshot.rows.get(&reused.source);
        let backed = source.is_some_and(|row| {
            &row.check == member && &row.commit == head && row.failure_kind.is_none()
        }) && match kinds.get(&reused.source) {
            Some((_, _, Kind::Pass)) => reused.passed,
            Some((_, _, Kind::Red)) => !reused.passed,
            Some((_, _, Kind::Stopped)) | None => false,
        };
        if !backed {
            return Err(Violation::new(
                "no-false-pass",
                format!(
                    "{member} at {head} reuses {} (passed: {}) and storage holds {source:?} for it",
                    reused.source, reused.passed
                ),
            ));
        }
    }
    // A repeated delivery of one measurement creates no second row.
    let mut requests: BTreeMap<(String, String), BTreeSet<&(String, String)>> = BTreeMap::new();
    for delivery in &facts.deliveries {
        for &member in GROUPS[delivery.group] {
            requests
                .entry((
                    MEMBERS[member].to_string(),
                    facts.waves[delivery.wave].head.clone(),
                ))
                .or_default()
                .insert(&delivery.request);
        }
    }
    // Verdicts and non-verdicts are counted apart: a stopped member's
    // duplicate is the known-open cairn/6885.
    let mut rows_per_measurement: BTreeMap<(String, String, bool), usize> = BTreeMap::new();
    for row in snapshot.rows.values() {
        *rows_per_measurement
            .entry((
                row.check.clone(),
                row.commit.clone(),
                row.failure_kind.is_none(),
            ))
            .or_default() += 1;
    }
    for ((member, head, verdict), count) in rows_per_measurement {
        let distinct = requests
            .get(&(member.clone(), head.clone()))
            .map_or(0, BTreeSet::len);
        if count > distinct {
            violated(Violation::new(
                if verdict {
                    "duplicate-idempotent"
                } else {
                    "duplicate-idempotent-non-verdict"
                },
                format!("{member} at {head} has {count} rows for {distinct} distinct deliveries"),
            ))?;
        }
    }
    // An acknowledgment that a result is recorded names a durable row at the
    // wave's own coordinates.
    for ack in &snapshot.acks {
        let Some(id) = &ack.observation else {
            continue;
        };
        let head = &facts.waves[ack.wave].head;
        let member = MEMBERS[ack.member];
        match snapshot.rows.get(id) {
            Some(row)
                if &row.commit == head
                    && row.check == member
                    && row.input == input_hash(member, head) => {}
            row => {
                return Err(Violation::new(
                    "acknowledged-is-durable",
                    format!(
                        "wave {} acknowledged {member} as {id}, storage holds {row:?}",
                        ack.wave
                    ),
                ))
            }
        }
    }
    // A write this process failed is a visible fact naming its member and step.
    for applied in &facts.ledger {
        if applied.boot == facts.process
            && matches!(applied.effect, Effect::Failed | Effect::LostResponse)
            && !snapshot.faults.contains(&(
                applied.member.clone(),
                applied.step.fault_name().to_string(),
            ))
        {
            return Err(Violation::new(
                "failure-visible",
                format!(
                    "{} {} write for {} failed at action {} with no visible fault",
                    applied.member,
                    applied.step.fault_name(),
                    applied.subject,
                    applied.action
                ),
            ));
        }
    }
    // A member's log is the last bytes written for it, never removed or
    // replaced by anything else.
    for (member, actual) in &snapshot.logs {
        let expected = facts
            .ledger
            .iter()
            .rev()
            .find(|applied| {
                applied.step == Step::Log
                    && applied.effect == Effect::Wrote
                    && &applied.member == member
            })
            .and_then(|applied| applied.output.clone());
        if actual != &expected {
            return Err(Violation::new(
                "log-content",
                format!("{member}'s log reads {actual:?}, the last write was {expected:?}"),
            ));
        }
    }
    Ok(())
}

/// Progress is conditional: only a write that met no injected failure and no
/// stop is promised to land. Under a permanent failure the contract is the
/// visible fault (safety), never eventual persistence.
fn contract_progress(facts: &Facts, snapshot: &Snapshot) -> Result<(), Violation> {
    for (index, wave) in facts.waves.iter().enumerate() {
        if !wave.answering {
            continue;
        }
        let mut last: BTreeMap<usize, (&DeliveryFact, Kind)> = BTreeMap::new();
        for delivery in facts
            .deliveries
            .iter()
            .filter(|delivery| delivery.wave == index)
        {
            for (&member, &kind) in GROUPS[delivery.group].iter().zip(&delivery.kinds) {
                last.insert(member, (delivery, kind));
            }
        }
        for (member, (delivery, kind)) in last {
            let ack = snapshot
                .acks
                .iter()
                .find(|ack| ack.wave == index && ack.member == member);
            let Some(ack) = ack else {
                return Err(Violation::new(
                    "received-result-answered",
                    format!(
                        "wave {index} received {} and never answered for it",
                        MEMBERS[member]
                    ),
                ));
            };
            if kind == Kind::Stopped {
                continue;
            }
            let id = measurement_id(&delivery.request, MEMBERS[member]);
            let last_write = facts.ledger.iter().rev().find(|applied| {
                applied.boot == facts.process
                    && applied.step == Step::Observation
                    && applied.subject == id
            });
            if !matches!(
                last_write.map(|applied| applied.effect),
                None | Some(Effect::Wrote)
            ) {
                continue;
            }
            if ack.observation.as_deref() != Some(id.as_str()) {
                return Err(Violation::new(
                    "recorded-when-storage-accepts",
                    format!(
                        "wave {index}'s {} met no failure, yet its acknowledgment is {:?}",
                        MEMBERS[member], ack.observation
                    ),
                ));
            }
            // Newer evidence at identical inputs replaces older, so the pass
            // is promised to reuse only while it is the latest genuine verdict
            // committed for this member at this head.
            let kinds = delivered_kinds(facts);
            let latest_genuine = facts
                .ledger
                .iter()
                .rev()
                .filter(|applied| {
                    applied.step == Step::Observation
                        && matches!(applied.effect, Effect::Wrote | Effect::LostResponse)
                })
                .find_map(|applied| {
                    let (written_wave, written_member, written_kind) =
                        kinds.get(&applied.subject)?;
                    (*written_member == member
                        && facts.waves[*written_wave].head == wave.head
                        && *written_kind != Kind::Stopped)
                        .then_some(applied.subject.as_str())
                });
            if kind == Kind::Pass
                && latest_genuine == Some(id.as_str())
                && !snapshot
                    .reusable
                    .get(&(MEMBERS[member].to_string(), wave.head.clone()))
                    .is_some_and(|reused| reused.passed && reused.source == id)
            {
                return Err(Violation::new(
                    "durable-pass-is-reusable",
                    format!(
                        "{} passed and was recorded at {} as {id}, yet the next wave would reuse \
                         {:?} (row {:?})",
                        MEMBERS[member],
                        wave.head,
                        snapshot
                            .reusable
                            .get(&(MEMBERS[member].to_string(), wave.head.clone()))
                            .map(|reused| (reused.passed, reused.source.clone())),
                        snapshot.rows.get(&id)
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Completion is reachable: some wave of the running process holds a durable,
/// acknowledged row for every member.
fn completed(facts: &Facts, snapshot: &Snapshot) -> bool {
    facts.waves.iter().enumerate().any(|(index, wave)| {
        wave.answering
            && (0..MEMBERS.len()).all(|member| {
                snapshot.acks.iter().any(|ack| {
                    ack.wave == index
                        && ack.member == member
                        && ack
                            .observation
                            .as_ref()
                            .is_some_and(|id| snapshot.rows.contains_key(id))
                })
            })
    })
}

// ---- Runs, shrinking, replay ----

struct Trial {
    actions: Vec<Action>,
    /// The action index the violation surfaced at; `actions.len()` when it
    /// surfaced only once every held write was released.
    violation: Option<(usize, Violation)>,
    completed: bool,
    restart_lost: usize,
    observation_writes: usize,
    known_open: Vec<String>,
}

async fn run_actions(
    defect: Defect,
    mut next: impl FnMut(&Abstract, usize) -> Option<Action>,
) -> (Trial, Option<Snapshot>, Facts) {
    let mut harness = Harness::new(defect).await;
    let mut state = Abstract::initial();
    let mut actions = Vec::new();
    let mut violation = None;
    while let Some(action) = next(&state, harness.process.boundary.held()) {
        state.apply(&action);
        actions.push(action.clone());
        if let Err(found) = harness.step(actions.len() - 1, &action).await {
            violation = Some((actions.len() - 1, found));
            break;
        }
    }
    let mut snapshot = None;
    if violation.is_none() {
        match harness.finish(actions.len()).await {
            Ok(found) => snapshot = Some(found),
            Err(found) => violation = Some((actions.len(), found)),
        }
    }
    let facts = harness.facts();
    let observation_writes = facts
        .ledger
        .iter()
        .filter(|applied| applied.step == Step::Observation)
        .count();
    let trial = Trial {
        completed: snapshot
            .as_ref()
            .is_some_and(|snapshot| completed(&facts, snapshot)),
        restart_lost: harness.restart_lost.len(),
        observation_writes,
        known_open: harness.known_open.iter().cloned().collect(),
        actions,
        violation,
    };
    (trial, snapshot, facts)
}

async fn run_schedule(actions: &[Action], defect: Defect) -> (Trial, Option<Snapshot>, Facts) {
    validate(actions).unwrap_or_else(|error| panic!("invalid schedule: {error}"));
    let mut remaining = actions.iter().cloned();
    run_actions(defect, move |_, _| remaining.next()).await
}

async fn run_generated(seed: u64, defect: Defect) -> Trial {
    let mut rng = StdRng::seed_from_u64(seed);
    let length = rng.gen_range(4..=MAX_ACTIONS);
    let mut proposed = 0;
    run_actions(defect, move |state, held| {
        (proposed < length).then(|| {
            proposed += 1;
            propose(&mut rng, state, held)
        })
    })
    .await
    .0
}

fn simpler(action: &Action) -> Option<Action> {
    match action {
        Action::Deliver { group, kinds } if kinds.iter().any(|kind| *kind != Kind::Pass) => {
            Some(Action::Deliver {
                group: *group,
                kinds: vec![Kind::Pass; kinds.len()],
            })
        }
        Action::Release {
            resolution: Resolution::Fail,
        } => Some(Action::Release {
            resolution: Resolution::Pass,
        }),
        _ => None,
    }
}

/// Delta-debugging: drop chunks, then simplify single actions, keeping a
/// candidate only while it is valid and still violates the same property.
async fn shrink(trial: &Trial, defect: Defect) -> (Vec<Action>, Violation, usize) {
    let (at, violation) = trial.violation.clone().expect("a failing trial");
    let property = violation.property.clone();
    let mut best: Vec<Action> = trial.actions[..(at + 1).min(trial.actions.len())].to_vec();
    let mut best_violation = violation;
    let mut runs = 0;
    let reproduces = |candidate: &[Action]| {
        let candidate = candidate.to_vec();
        let property = property.clone();
        async move {
            if validate(&candidate).is_err() {
                return None;
            }
            run_schedule(&candidate, defect)
                .await
                .0
                .violation
                .map(|(_, found)| found)
                .filter(|found| found.property == property)
        }
    };
    let mut chunk = (best.len() / 2).max(1);
    loop {
        let mut index = 0;
        let mut removed = false;
        while index < best.len() && runs < SHRINK_BUDGET {
            let mut candidate = best.clone();
            candidate.drain(index..(index + chunk).min(best.len()));
            runs += 1;
            if let Some(found) = reproduces(&candidate).await {
                best = candidate;
                best_violation = found;
                removed = true;
            } else {
                index += chunk;
            }
        }
        if runs >= SHRINK_BUDGET || (!removed && chunk == 1) {
            break;
        }
        if !removed {
            chunk = (chunk / 2).max(1);
        }
    }
    for index in 0..best.len() {
        let Some(action) = simpler(&best[index]) else {
            continue;
        };
        if runs >= SHRINK_BUDGET {
            break;
        }
        let mut candidate = best.clone();
        candidate[index] = action;
        runs += 1;
        if let Some(found) = reproduces(&candidate).await {
            best = candidate;
            best_violation = found;
        }
    }
    (best, best_violation, runs)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Bounds {
    members: Vec<String>,
    groups: Vec<Vec<usize>>,
    heads: Vec<String>,
    max_actions: usize,
    max_restarts: usize,
    max_supersedes: usize,
    max_holds_per_process: usize,
}

fn bounds() -> Bounds {
    Bounds {
        members: MEMBERS.iter().map(|member| member.to_string()).collect(),
        groups: GROUPS.iter().map(|group| group.to_vec()).collect(),
        heads: HEADS.iter().map(|head| head.to_string()).collect(),
        max_actions: MAX_ACTIONS,
        max_restarts: MAX_RESTARTS,
        max_supersedes: MAX_SUPERSEDES,
        max_holds_per_process: MAX_HOLDS_PER_BOOT,
    }
}

/// Everything needed to replay a counterexample on another machine.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Counterexample {
    format: u32,
    seed: u64,
    defect: Defect,
    source_revision: String,
    harness_sha256: String,
    bounds: Bounds,
    property: String,
    detail: String,
    original: Vec<Action>,
    shrunk: Vec<Action>,
    shrink_runs: usize,
    replay: String,
}

const REPLAY_ENV: &str = "CAIRN_REVIEW_LIFECYCLE_REPLAY";
const ARTIFACT_ENV: &str = "CAIRN_REVIEW_LIFECYCLE_ARTIFACTS";

fn harness_sha256() -> String {
    format!(
        "{:x}",
        Sha256::digest(include_bytes!("checks_turn_end_lifecycle_tests.rs"))
    )
}

fn source_revision() -> String {
    std::env::var("CAIRN_SOURCE_REVISION")
        .ok()
        .or_else(|| {
            std::process::Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(env!("CARGO_MANIFEST_DIR"))
                .output()
                .ok()
                .filter(|output| output.status.success())
                .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        })
        .unwrap_or_else(|| "unrecorded".to_string())
}

async fn persist(seed: u64, defect: Defect, trial: &Trial) -> (PathBuf, Counterexample) {
    let (shrunk, violation, shrink_runs) = shrink(trial, defect).await;
    let dir = std::env::var_os(ARTIFACT_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("review-lifecycle-counterexamples"));
    std::fs::create_dir_all(&dir).expect("counterexample directory");
    let defect_name = serde_json::to_value(defect).expect("defect serializes");
    let path = dir.join(format!(
        "{}-seed-{seed}.json",
        defect_name.as_str().expect("defect is a string")
    ));
    let counterexample = Counterexample {
        format: 1,
        seed,
        defect,
        source_revision: source_revision(),
        harness_sha256: harness_sha256(),
        bounds: bounds(),
        property: violation.property,
        detail: violation.detail,
        original: trial.actions.clone(),
        shrunk,
        shrink_runs,
        replay: format!(
            "{REPLAY_ENV}={} bun run test:rust -p cairn-core --lib --run-ignored only \
             -E 'test(review_lifecycle_generated_campaign)'",
            path.display()
        ),
    };
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&counterexample).expect("counterexample serializes"),
    )
    .expect("counterexample written");
    (path, counterexample)
}

async fn replay(path: &Path) -> (Counterexample, Trial) {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("replay {}: {error}", path.display()));
    let counterexample: Counterexample = serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("replay {}: not a counterexample: {error}", path.display()));
    if let Err(error) = validate(&counterexample.shrunk) {
        panic!("replay {}: refused, {error}", path.display());
    }
    let trial = run_schedule(&counterexample.shrunk, counterexample.defect)
        .await
        .0;
    (counterexample, trial)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

// ---- Tests ----

enum Witness {
    Completed,
    RowAt(&'static str, &'static str),
    NoMeasurement(&'static str, &'static str),
    /// The next wave at these inputs reuses a pass.
    Reusable(&'static str, &'static str),
    /// The next wave at these inputs reuses a red.
    ReusedRed(&'static str, &'static str),
    /// The next wave at these inputs reuses nothing and runs the member.
    NotReusable(&'static str, &'static str),
    Fault(&'static str, &'static str),
    StoredFault(&'static str, &'static str),
    AckNone(usize, &'static str),
    AckSome(usize, &'static str),
    RestartLost(usize),
    /// A [`KNOWN_OPEN`] property was observed, so its repair is visible here.
    KnownOpen(&'static str),
}

fn witness_holds(witness: &Witness, trial: &Trial, facts: &Facts, snapshot: &Snapshot) -> bool {
    let row_at = |member: &str, head: &str| {
        snapshot
            .rows
            .values()
            .any(|row| row.check == member && row.commit == head)
    };
    let ack = |wave: usize, member: &str| {
        let index = MEMBERS
            .iter()
            .position(|name| *name == member)
            .expect("member");
        snapshot
            .acks
            .iter()
            .find(|ack| ack.wave == wave && ack.member == index)
            .map(|ack| ack.observation.clone())
    };
    match witness {
        Witness::Completed => completed(facts, snapshot),
        Witness::RowAt(member, head) => row_at(member, head),
        Witness::NoMeasurement(request, member) => !snapshot.rows.contains_key(&measurement_id(
            &(request.to_string(), "attempt-1".into()),
            member,
        )),
        Witness::Reusable(member, head) => snapshot
            .reusable
            .get(&(member.to_string(), head.to_string()))
            .is_some_and(|reused| reused.passed),
        Witness::ReusedRed(member, head) => snapshot
            .reusable
            .get(&(member.to_string(), head.to_string()))
            .is_some_and(|reused| !reused.passed),
        Witness::NotReusable(member, head) => !snapshot
            .reusable
            .contains_key(&(member.to_string(), head.to_string())),
        Witness::Fault(member, step) => snapshot
            .faults
            .contains(&(member.to_string(), step.to_string())),
        Witness::StoredFault(member, step) => snapshot
            .stored_faults
            .contains(&(member.to_string(), step.to_string())),
        Witness::AckNone(wave, member) => ack(*wave, member) == Some(None),
        Witness::AckSome(wave, member) => matches!(ack(*wave, member), Some(Some(_))),
        Witness::RestartLost(count) => trial.restart_lost == *count,
        Witness::KnownOpen(property) => trial.known_open.iter().any(|seen| seen == property),
    }
}

fn deliver(group: usize, kinds: &[Kind]) -> Action {
    Action::Deliver {
        group,
        kinds: kinds.to_vec(),
    }
}

fn arm(step: Step, member: usize, directive: Directive) -> Action {
    Action::Arm {
        step,
        member,
        directive,
    }
}

const PASS: Action = Action::Release {
    resolution: Resolution::Pass,
};
const ALPHA: usize = 0;
const BETA: usize = 1;
const GAMMA: usize = 2;

fn fixed_schedules() -> Vec<(&'static str, Vec<Action>, Vec<Witness>)> {
    use Kind::{Pass, Red, Stopped};
    use Witness::*;
    vec![
        (
            "both groups complete",
            vec![deliver(0, &[Pass]), deliver(1, &[Pass, Red])],
            vec![
                Completed,
                Reusable("alpha", "head-0"),
                RowAt("gamma", "head-0"),
                ReusedRed("gamma", "head-0"),
            ],
        ),
        (
            "result before cancel: a held sibling lands after the cancel, at the sealed head",
            vec![
                arm(Step::Observation, GAMMA, Directive::Hold),
                deliver(0, &[Pass]),
                deliver(1, &[Pass, Pass]),
                Action::Cancel,
                PASS,
            ],
            vec![
                RowAt("alpha", "head-0"),
                RowAt("beta", "head-0"),
                Reusable("gamma", "head-0"),
                AckSome(0, "gamma"),
            ],
        ),
        (
            "cancel before result: an undelivered group records nothing for its wave",
            vec![
                deliver(0, &[Pass]),
                Action::Cancel,
                Action::Supersede { advance: false },
                deliver(1, &[Pass, Pass]),
            ],
            vec![
                Reusable("alpha", "head-0"),
                NoMeasurement("req-w0-g1", "beta"),
                NoMeasurement("req-w0-g1", "gamma"),
                AckSome(1, "gamma"),
            ],
        ),
        (
            "duplicate delivery",
            vec![deliver(1, &[Pass, Stopped]), Action::Duplicate { of: 0 }],
            vec![
                Reusable("beta", "head-0"),
                NotReusable("gamma", "head-0"),
                KnownOpen("duplicate-idempotent-non-verdict"),
                AckSome(0, "beta"),
            ],
        ),
        (
            "observation and log fail independently, and the stored faults outlive the wave",
            vec![
                arm(Step::Observation, BETA, Directive::Fail),
                arm(Step::Log, ALPHA, Directive::Fail),
                deliver(0, &[Pass]),
                deliver(1, &[Pass, Pass]),
                Action::Supersede { advance: false },
            ],
            vec![
                StoredFault("beta", "observation"),
                StoredFault("alpha", "log"),
                Reusable("alpha", "head-0"),
                NoMeasurement("req-w0-g1", "beta"),
                AckNone(0, "beta"),
                Reusable("gamma", "head-0"),
            ],
        ),
        (
            "lost response after a durable commit, then re-delivery",
            vec![
                arm(Step::Observation, ALPHA, Directive::LostResponse),
                deliver(0, &[Pass]),
                Action::Duplicate { of: 0 },
            ],
            vec![
                Fault("alpha", "observation"),
                Reusable("alpha", "head-0"),
                AckSome(0, "alpha"),
            ],
        ),
        (
            "stale head: a superseded wave's late result keeps its sealed head",
            vec![
                arm(Step::Observation, ALPHA, Directive::Hold),
                deliver(0, &[Pass]),
                Action::Supersede { advance: true },
                PASS,
                deliver(0, &[Pass]),
            ],
            vec![
                Reusable("alpha", "head-0"),
                Reusable("alpha", "head-1"),
                AckSome(0, "alpha"),
                AckSome(1, "alpha"),
            ],
        ),
        (
            "restart before the durable receipt loses only the uncommitted member",
            vec![
                arm(Step::Observation, GAMMA, Directive::Hold),
                deliver(1, &[Pass, Pass]),
                Action::Restart,
                Action::Supersede { advance: false },
                deliver(1, &[Pass, Pass]),
            ],
            vec![
                RestartLost(1),
                NoMeasurement("req-w0-g1", "gamma"),
                RowAt("beta", "head-0"),
                AckSome(1, "gamma"),
            ],
        ),
    ]
}

/// The two historical defects, each with the shortest schedule that exposes
/// it, and the property that must name it.
fn defect_controls() -> Vec<(Defect, Vec<Action>, &'static str)> {
    vec![
        (
            Defect::SwallowLogErrors,
            vec![
                arm(Step::Log, ALPHA, Directive::Fail),
                deliver(0, &[Kind::Pass]),
            ],
            "failure-visible",
        ),
        (
            Defect::WrongHead,
            vec![
                arm(Step::Observation, ALPHA, Directive::Hold),
                deliver(0, &[Kind::Pass]),
                Action::Supersede { advance: true },
                PASS,
            ],
            "measured-identity",
        ),
    ]
}

/// Each schedule owns its database file and orchestrator, so they run side by
/// side; a held write pins one runtime thread, hence the worker count.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn review_lifecycle_fixed_schedules_hold_the_evidence_contract() {
    let started = Instant::now();
    // One process-wide setup (the migrated template, the shared search
    // index) is paid once, before the schedules are timed against each other.
    drop(Harness::new(Defect::None).await);
    let setup = started.elapsed();
    let mut runs = tokio::task::JoinSet::new();
    for (name, schedule, witnesses) in fixed_schedules() {
        runs.spawn(async move {
            let schedule_started = Instant::now();
            let (trial, snapshot, facts) = run_schedule(&schedule, Defect::None).await;
            let outcome = match (&trial.violation, snapshot) {
                (Some((at, violation)), _) => Err(format!(
                    "{name}: action {at} violates {}: {}",
                    violation.property, violation.detail
                )),
                (None, None) => Err(format!("{name}: no final snapshot")),
                (None, Some(snapshot)) => witnesses
                    .iter()
                    .position(|witness| !witness_holds(witness, &trial, &facts, &snapshot))
                    .map_or(Ok(()), |index| {
                        Err(format!(
                            "{name}: witness {index} does not hold, so the schedule never \
                             reached the state it names"
                        ))
                    }),
            };
            (
                format!("{}ms {name}", schedule_started.elapsed().as_millis()),
                outcome,
            )
        });
    }
    for (defect, schedule, property) in defect_controls() {
        runs.spawn(async move {
            let control_started = Instant::now();
            let (clean, _, _) = run_schedule(&schedule, Defect::None).await;
            let (trial, _, _) = run_schedule(&schedule, defect).await;
            let found = trial.violation.map(|(_, violation)| violation.property);
            let outcome = if let Some((_, violation)) = clean.violation {
                Err(format!(
                    "{defect:?} control is red without its defect: {}",
                    violation.detail
                ))
            } else if found.as_deref() != Some(property) {
                Err(format!(
                    "{defect:?} must violate {property}, found {found:?}"
                ))
            } else {
                Ok(())
            };
            (
                format!(
                    "{}ms {defect:?} control",
                    control_started.elapsed().as_millis()
                ),
                outcome,
            )
        });
    }
    let mut costs = Vec::new();
    while let Some(joined) = runs.join_next().await {
        let (cost, outcome) = joined.expect("schedule task");
        if let Err(error) = outcome {
            panic!("{error}");
        }
        costs.push(cost);
    }
    eprintln!(
        "review lifecycle fixed schedules: {} positive, {} defect controls; setup {:?}, total {:?} [{}]",
        fixed_schedules().len(),
        defect_controls().len(),
        setup,
        started.elapsed(),
        costs.join("; ")
    );
}

#[test]
fn review_lifecycle_schedules_refuse_invalid_actions() {
    use Kind::Pass;
    let cases: Vec<(Vec<Action>, &str)> = vec![
        (
            vec![Action::Cancel, deliver(0, &[Pass])],
            "only a live wave receives results",
        ),
        (
            vec![deliver(0, &[Pass]), deliver(0, &[Pass])],
            "already delivered",
        ),
        (vec![Action::Duplicate { of: 0 }], "no delivery 0"),
        (vec![deliver(1, &[Pass])], "has 2 members"),
        (vec![deliver(2, &[Pass])], "no placement group 2"),
        (
            vec![arm(Step::Log, ALPHA, Directive::Hold)],
            "log-generation lock",
        ),
        (
            vec![arm(Step::Log, ALPHA, Directive::LostResponse)],
            "observation commit",
        ),
        (
            vec![
                arm(Step::Observation, ALPHA, Directive::Hold),
                arm(Step::Observation, BETA, Directive::Hold),
                arm(Step::Observation, GAMMA, Directive::Hold),
            ],
            "holds at most 2 writes",
        ),
        (
            vec![arm(Step::Observation, 3, Directive::Fail)],
            "no member 3",
        ),
        (vec![Action::Restart, Action::Restart], "restarts at most"),
        (
            vec![Action::Restart, deliver(0, &[Pass])],
            "only a live wave receives results",
        ),
        (
            vec![Action::Supersede { advance: true }; 3],
            "supersedes at most",
        ),
        (vec![PASS; MAX_ACTIONS + 1], "bounded at"),
    ];
    for (schedule, reason) in cases {
        let error = validate(&schedule).expect_err(reason);
        assert!(error.contains(reason), "{schedule:?}: {error}");
    }
    // A release with nothing held is valid and does nothing; the generator
    // never proposes one, a shrunk schedule may keep one.
    assert!(validate(&[PASS]).is_ok());
    let replayable = serde_json::to_string(&defect_controls()[1].1).expect("serializes");
    let parsed: Vec<Action> = serde_json::from_str(&replayable).expect("parses");
    assert_eq!(parsed, defect_controls()[1].1);
}

/// Bounded seeded exploration. Not part of the default suite: run it
/// explicitly or on main (docs/formal-verification.md). Every generated
/// action is valid by construction; a failure is shrunk and persisted with
/// its seed, bounds, source revision and replay command.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "generated review-lifecycle campaign; run explicitly (docs/formal-verification.md)"]
async fn review_lifecycle_generated_campaign() {
    if let Some(path) = std::env::var_os(REPLAY_ENV) {
        let path = PathBuf::from(path);
        let (counterexample, trial) = replay(&path).await;
        let found = trial.violation.map(|(_, violation)| violation);
        eprintln!(
            "replayed {}: recorded {}, replay {found:?}",
            path.display(),
            counterexample.property
        );
        assert_eq!(
            found.map(|violation| violation.property).as_deref(),
            Some(counterexample.property.as_str()),
            "the replay must reproduce the recorded property"
        );
        return;
    }
    let base = env_u64("CAIRN_REVIEW_LIFECYCLE_SEED", 1);
    let count = env_u64("CAIRN_REVIEW_LIFECYCLE_SEEDS", 64);
    let started = Instant::now();
    let mut actions = 0;
    let mut completions = 0;
    let mut restarts_lost = 0;
    let mut observation_writes = 0;
    let mut known_open: BTreeMap<String, usize> = BTreeMap::new();
    let mut histogram: BTreeMap<String, usize> = BTreeMap::new();
    for seed in base..base + count {
        let trial = run_generated(seed, Defect::None).await;
        actions += trial.actions.len();
        for action in &trial.actions {
            let name = serde_json::to_value(action).expect("action serializes")["action"]
                .as_str()
                .expect("tagged")
                .to_string();
            *histogram.entry(name).or_default() += 1;
        }
        if trial.violation.is_some() {
            let (path, counterexample) = persist(seed, Defect::None, &trial).await;
            panic!(
                "seed {seed} violates {}: {}\nshrunk to {} actions: {}\n{}\nreplay: {}",
                counterexample.property,
                counterexample.detail,
                counterexample.shrunk.len(),
                path.display(),
                serde_json::to_string(&counterexample.shrunk).expect("schedule serializes"),
                counterexample.replay
            );
        }
        completions += usize::from(trial.completed);
        restarts_lost += trial.restart_lost;
        observation_writes += trial.observation_writes;
        for property in trial.known_open {
            *known_open.entry(property).or_default() += 1;
        }
    }
    let explored = started.elapsed();
    assert!(
        completions > 0,
        "completion was never reached in {count} sequences: the generator is over-constrained"
    );

    let mut controls = Vec::new();
    for (defect, _, property) in defect_controls() {
        let control_started = Instant::now();
        let mut caught = None;
        for seed in base..base + count {
            let trial = run_generated(seed, defect).await;
            if trial.violation.is_some() {
                caught = Some((seed, trial));
                break;
            }
        }
        let (seed, trial) =
            caught.unwrap_or_else(|| panic!("{defect:?} was not caught in {count} sequences"));
        let (path, counterexample) = persist(seed, defect, &trial).await;
        assert_eq!(
            counterexample.property, property,
            "{defect:?} surfaced as {}",
            counterexample.detail
        );
        let (_, replayed) = replay(&path).await;
        assert_eq!(
            replayed
                .violation
                .map(|(_, violation)| violation.property)
                .as_deref(),
            Some(property),
            "{defect:?}'s persisted counterexample must replay"
        );
        controls.push(serde_json::json!({
            "defect": defect, "seed": seed, "property": property,
            "originalActions": counterexample.original.len(),
            "shrunkActions": counterexample.shrunk.len(),
            "shrinkRuns": counterexample.shrink_runs,
            "artifact": path, "elapsedMs": control_started.elapsed().as_millis() as u64,
        }));
    }
    eprintln!(
        "{}",
        serde_json::json!({
            "campaign": "review-lifecycle",
            "seeds": [base, base + count - 1],
            "sequences": count,
            "actions": actions,
            "histogram": histogram,
            "completions": completions,
            "uncommittedLostAtRestart": restarts_lost,
            "observationWrites": observation_writes,
            "knownOpenSequences": known_open,
            "knownOpenOwners": KNOWN_OPEN,
            "exploreMs": explored.as_millis() as u64,
            "meanSequenceMs": explored.as_millis() as u64 / count.max(1),
            "defectControls": controls,
            "bounds": bounds(),
            "sourceRevision": source_revision(),
            "harnessSha256": harness_sha256(),
        })
    );
}
