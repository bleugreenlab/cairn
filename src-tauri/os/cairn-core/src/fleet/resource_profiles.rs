use super::{ResourceReservation, ResourceReservationSource};
use crate::storage::LocalDb;
use cairn_common::executor_protocol::{
    CellCommandClass, CellExecutionMeta, CommandResourceIdentity, CommandStop, ContentionEstimate,
    ContentionEvidence, ContentionFallback, CrowdedKills, DurationEstimate, DurationEvidence,
    DurationFallback, ExecutionLoadContext, ExecutionWarmth, ExecutorCapabilities,
    MeasurementQuality, MemoryCeiling, MemoryKill, ProcessDemandBasis, ReservationFallback,
    ReservationRationale, DURATION_PROFILE_STALE_AFTER_MS, DURATION_SAMPLE_WINDOW,
    MEASURED_RESERVATION_HEADROOM_PERCENT, MIN_CONFIDENT_DURATION_SAMPLES,
    MIN_CONFIDENT_RESERVATION_SAMPLES,
};
use cairn_db::turso::params;
use std::sync::Arc;

const MIN_CONFIDENT_SAMPLES: u64 = MIN_CONFIDENT_RESERVATION_SAMPLES;
const HEADROOM_PERCENT: u32 = MEASURED_RESERVATION_HEADROOM_PERCENT;
/// How far a lower peak pulls the memory high-water mark down: one
/// thirty-second of the gap, so one light run is never mistaken for the
/// command getting cheaper.
const PEAK_DECAY_DIVISOR: u64 = 32;
/// The same for a run whose build jobs this machine's memory bounded: half the
/// gap. Such a run measured the command at the width the machine grants, which
/// is the width the next run here gets too, so its peak is evidence about that
/// run rather than an outlier to discount. Without it a peak learned at full
/// width, before builds were memory-capped, takes some seventy capped runs to
/// fall, and while it stays above the machine's ceiling the runs that would
/// teach it seldom happen (cairn/5731).
const MEMORY_BOUND_PEAK_DECAY_DIVISOR: u64 = 2;
const MIN_CONTENTION_SAMPLES: usize = 3;
/// A command reservation must leave room for the operating system, executor,
/// and measurement noise. Peaks at or above this share describe saturation.
const MEMORY_QUARANTINE_NUMERATOR: u64 = 9;
const MEMORY_QUARANTINE_DENOMINATOR: u64 = 10;
const MEMORY_RESERVATION_CAP_NUMERATOR: u64 = 3;
const MEMORY_RESERVATION_CAP_DENOMINATOR: u64 = 4;

/// The most one request may reserve on a machine with this memory budget.
pub(super) fn memory_reservation_cap(memory_budget_bytes: u64) -> u64 {
    memory_budget_bytes.saturating_mul(MEMORY_RESERVATION_CAP_NUMERATOR)
        / MEMORY_RESERVATION_CAP_DENOMINATOR
}

/// Whether a peak is large enough against this budget that it describes the
/// machine's saturation rather than one command.
fn memory_peak_quarantined(peak_bytes: u64, memory_budget_bytes: Option<u64>) -> bool {
    memory_budget_bytes.is_some_and(|budget| {
        peak_bytes.saturating_mul(MEMORY_QUARANTINE_DENOMINATOR)
            >= budget.saturating_mul(MEMORY_QUARANTINE_NUMERATOR)
    })
}

/// Whether this machine can ever hold the work's measured memory demand.
///
/// Three answers, because two of them are evidence and one is not. A missing
/// measurement, or a machine that states no budget, is `Unknown`, and it is
/// never read as either of the others: treating an absent ceiling as proof a
/// machine can hold the work is how a smaller, colder machine would be named
/// the safe alternative to one measurably too small.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum MemoryFit {
    #[default]
    Unknown,
    /// A measured demand fits under this machine's reservation cap.
    Holds,
    /// A measured demand this machine's cap will never reach: the clamp as a
    /// fact.
    Exceeds(MemoryCeiling),
}

impl MemoryFit {
    pub(super) fn of(demand_bytes: Option<u64>, memory_budget_bytes: Option<u64>) -> Self {
        let (Some(estimate_bytes), Some(budget)) = (demand_bytes, memory_budget_bytes) else {
            return Self::Unknown;
        };
        let ceiling_bytes = memory_reservation_cap(budget);
        match estimate_bytes > ceiling_bytes {
            true => Self::Exceeds(MemoryCeiling {
                estimate_bytes,
                ceiling_bytes,
                killed_here: None,
            }),
            false => Self::Holds,
        }
    }

    pub(super) fn ceiling(self) -> Option<MemoryCeiling> {
        match self {
            Self::Exceeds(ceiling) => Some(ceiling),
            Self::Holds | Self::Unknown => None,
        }
    }

    /// The fit of a batch, which must hold its heaviest item: any item that
    /// exceeds decides it (the one measured furthest past the cap), and an item
    /// nothing is known about leaves the rest of the batch unknown.
    pub(super) fn of_batch(items: impl IntoIterator<Item = Self>) -> Self {
        items
            .into_iter()
            .reduce(|left, right| match (left, right) {
                (Self::Exceeds(held), Self::Exceeds(next)) => {
                    Self::Exceeds(match next.estimate_bytes > held.estimate_bytes {
                        true => next,
                        false => held,
                    })
                }
                (Self::Exceeds(held), _) | (_, Self::Exceeds(held)) => Self::Exceeds(held),
                (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
                (Self::Holds, Self::Holds) => Self::Holds,
            })
            .unwrap_or_default()
    }
}

#[derive(Clone, Debug)]
pub(super) struct ProfileContext {
    pub executor_class: String,
    pub os: String,
    pub arch: String,
    pub toolchain_fingerprint: String,
}

async fn load_contention_profile(
    db: Arc<LocalDb>,
    executor_class: &str,
    load: &ExecutionLoadContext,
) -> Result<Option<ContentionProfile>, String> {
    let executor_class = executor_class.to_string();
    let load = load.clone();
    db.read(|conn| Box::pin(async move {
        let mut rows = conn.query(
            "SELECT sample_count, updated_at_unix_ms, recent_multiplier_millis FROM command_contention_profiles WHERE executor_class=?1 AND compile_jobs=?2 AND light_jobs=?3",
            params![executor_class, load.co_resident_compile_jobs as i64, load.co_resident_light_jobs as i64],
        ).await?;
        match rows.next().await? {
            Some(row) => Ok(Some(ContentionProfile {
                sample_count: row.get::<i64>(0)? as u64,
                updated_at_unix_ms: row.get::<i64>(1)? as u64,
                recent_multiplier_millis: decode_window(&row.get::<String>(2)?),
            })),
            None => Ok(None),
        }
    })).await.map_err(|error| error.to_string())
}

async fn record_contention(
    db: Arc<LocalDb>,
    context: &ProfileContext,
    load: &ExecutionLoadContext,
    finished: u64,
    multiplier_millis: u64,
) -> Result<(), String> {
    for key in [context.executor_class.clone(), "*".into()] {
        let load = load.clone();
        db.write(move |conn| {
            let key = key.clone();
            let load = load.clone();
            Box::pin(async move {
            let mut rows = conn.query(
                "SELECT sample_count, recent_multiplier_millis FROM command_contention_profiles WHERE executor_class=?1 AND compile_jobs=?2 AND light_jobs=?3",
                params![key.clone(), load.co_resident_compile_jobs as i64, load.co_resident_light_jobs as i64],
            ).await?;
            let (count, mut window) = match rows.next().await? {
                Some(row) => (row.get::<i64>(0)? as u64, decode_window(&row.get::<String>(1)?)),
                None => (0, Vec::new()),
            };
            window.push(multiplier_millis);
            while window.len() > DURATION_SAMPLE_WINDOW { window.remove(0); }
            let encoded = serde_json::to_string(&window).unwrap_or_else(|_| "[]".into());
            conn.execute(
                "INSERT INTO command_contention_profiles (executor_class, compile_jobs, light_jobs, sample_count, updated_at_unix_ms, recent_multiplier_millis) VALUES (?1,?2,?3,?4,?5,?6) ON CONFLICT(executor_class, compile_jobs, light_jobs) DO UPDATE SET sample_count=excluded.sample_count, updated_at_unix_ms=excluded.updated_at_unix_ms, recent_multiplier_millis=excluded.recent_multiplier_millis",
                params![key, load.co_resident_compile_jobs as i64, load.co_resident_light_jobs as i64,
                    count.saturating_add(1).min(10_000) as i64, finished as i64, encoded],
            ).await?;
            Ok(())
            })
        }).await.map_err(|error| error.to_string())?;
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ContentionProfile {
    sample_count: u64,
    updated_at_unix_ms: u64,
    recent_multiplier_millis: Vec<u64>,
}

/// The largest stall share priced or divided out. The multiplier `1/(1-f)` has
/// no bound as the stall nears total, and a ranking needs a finite price; past
/// nine-tenths stalled a machine is already priced at ten times its run.
pub(super) const HOST_STALL_PRICED_CEILING_MILLIS: u32 = 900;

/// `t * f/(1-f)`: what a stall share `f` adds to a run that takes `t` on the
/// same machine unstalled. The one curve for host stall: placement adds it to
/// a prediction and [`destalled_ms`] takes it back out of an observation, so a
/// stall is charged once, from the reading current when the work is placed.
pub(super) fn stall_penalty_ms(run_ms: u64, full_millis: u32) -> u64 {
    let full_millis = full_millis.min(HOST_STALL_PRICED_CEILING_MILLIS);
    u64::try_from(u128::from(run_ms) * u128::from(full_millis) / u128::from(1_000 - full_millis))
        .unwrap_or(u64::MAX)
}

/// `d * (1-f)`: the unstalled duration of a run observed to take `d` under a
/// stall share `f`, the inverse of [`stall_penalty_ms`].
fn destalled_ms(observed_ms: u64, full_millis: u32) -> u64 {
    let full_millis = full_millis.min(HOST_STALL_PRICED_CEILING_MILLIS);
    observed_ms.saturating_mul(u64::from(1_000 - full_millis)) / 1_000
}

/// The host stall a run began under, as the learner reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartingStall {
    /// The machine has no stall reading: macOS, a kernel without PSI, an
    /// executor too old to record one. The count-only model takes the run as
    /// it came, as it always has; unmeasured is never priced as unstalled.
    Unmeasured,
    /// The machine measures stall but its reading was too old to be the
    /// machine at the start, so the run's own speed cannot be separated out.
    Stale,
    /// Thousandths of wall time fully stalled, the worse of memory and
    /// storage, as placement prices it.
    Measured(u32),
}

impl StartingStall {
    fn of(load: &ExecutionLoadContext) -> Self {
        if load.host_stall_stale {
            return Self::Stale;
        }
        match (
            load.io_pressure_full_millis,
            load.memory_pressure_full_millis,
        ) {
            (None, None) => Self::Unmeasured,
            (io, memory) => Self::Measured(io.unwrap_or(0).max(memory.unwrap_or(0))),
        }
    }

    /// The observation with the stall taken out, or `None` when it cannot be.
    fn destall(self, observed_ms: u64) -> Option<u64> {
        match self {
            Self::Unmeasured => Some(observed_ms),
            Self::Stale => None,
            Self::Measured(full_millis) => Some(destalled_ms(observed_ms, full_millis)),
        }
    }
}

/// Whether a successful run that begins under `load` becomes a sample of this
/// machine's own speed for a command it has no confident duration for. The
/// rule [`observe_completed`] applies, and the one placement asks before it
/// spends a lane exploring a machine: a run beside other work measures the
/// baseline times a slowdown nothing has separated yet, and a run whose stall
/// reading was stale cannot have the stall divided out.
pub(super) fn teaches_baseline(load: &ExecutionLoadContext) -> bool {
    load.co_resident_compile_jobs == 0
        && load.co_resident_light_jobs == 0
        && StartingStall::of(load) != StartingStall::Stale
}

pub(super) fn contention_prior(load: &ExecutionLoadContext) -> ContentionEstimate {
    ContentionEstimate {
        co_resident_compile_jobs: load.co_resident_compile_jobs,
        co_resident_light_jobs: load.co_resident_light_jobs,
        multiplier_millis: 1_000_u32
            .saturating_add(load.co_resident_compile_jobs.saturating_mul(600))
            .saturating_add(load.co_resident_light_jobs.saturating_mul(100)),
        source: ContentionEvidence::Prior,
        sample_count: 0,
        fallback: Some(ContentionFallback::NoGlobalCurve),
    }
}

pub(super) async fn resolve_contention(
    db: Arc<LocalDb>,
    context: &ProfileContext,
    load: &ExecutionLoadContext,
    now_unix_ms: u64,
) -> ContentionEstimate {
    let mut lookup_failed = false;
    for (key, source, fallback) in [
        (
            context.executor_class.as_str(),
            ContentionEvidence::Machine,
            None,
        ),
        (
            "*",
            ContentionEvidence::Global,
            Some(ContentionFallback::NoMachineCurve),
        ),
    ] {
        match load_contention_profile(db.clone(), key, load).await {
            Ok(Some(profile))
                if profile.recent_multiplier_millis.len() >= MIN_CONTENTION_SAMPLES
                    && now_unix_ms.saturating_sub(profile.updated_at_unix_ms)
                        <= DURATION_PROFILE_STALE_AFTER_MS =>
            {
                return ContentionEstimate {
                    co_resident_compile_jobs: load.co_resident_compile_jobs,
                    co_resident_light_jobs: load.co_resident_light_jobs,
                    multiplier_millis: upper_median(&profile.recent_multiplier_millis)
                        .clamp(1_000, u32::MAX as u64)
                        as u32,
                    source,
                    sample_count: profile.sample_count,
                    fallback,
                };
            }
            Ok(_) => {}
            Err(_) => lookup_failed = true,
        }
    }
    let mut prior = contention_prior(load);
    if lookup_failed {
        prior.fallback = Some(ContentionFallback::ProfileLookupFailed);
    }
    prior
}

impl ProfileContext {
    /// The context key in one readable line, for the rationale a placement
    /// decision carries. A profile learned on one platform never speaks for
    /// another, and this is what makes that visible without a second lookup.
    pub fn describe(&self) -> String {
        let toolchains = self.toolchain_fingerprint.replace('\u{1f}', ", ");
        format!(
            "{} on {}/{} with toolchains [{toolchains}]",
            self.executor_class, self.os, self.arch
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResolvedResourceProfile {
    pub reservation: ResourceReservation,
    pub learned_estimate: Option<cairn_common::executor_protocol::LearnedResourceEstimate>,
    /// How this number came to be this number.
    pub rationale: ReservationRationale,
    /// How long this work is predicted to run on this machine at the warmth it
    /// was resolved for. Read-only with respect to admission: nothing reserves
    /// against a duration, and it exists so placement can rank machines by when
    /// they would answer rather than by how unloaded they look.
    pub duration: DurationEstimate,
    /// Per-item durations when this profile represents a process batch.
    pub item_durations: Vec<DurationEstimate>,
    /// Whether this machine can ever hold the measured memory demand. When it
    /// cannot, `reservation` holds the clamp rather than the demand. Admission
    /// is unchanged by it; placement reads it to prefer a machine that can
    /// hold the work, and to say so when none can.
    pub memory_fit: MemoryFit,
    /// The largest single process this work is known to hold, which placement
    /// judges against what each machine can give one process, and any larger
    /// peak that names no process.
    pub process_evidence: ProcessEvidence,
}

impl ResolvedResourceProfile {
    /// Stamp the reservation with the most this work has been measured to
    /// hold, which admission charges a running command until its tree grows
    /// into it: the larger of the tree peak this machine measured (set by
    /// [`resolve_reservation`]) and the largest single process measured of this
    /// work on any platform, with headroom.
    ///
    /// Measured, never declared (tenet 946). A class prior is the project's
    /// largest rustc, not a measurement of this work, so it does not count; nor
    /// does a borrowed tree peak, which scales with the width of the machine
    /// that measured it rather than with this one.
    pub(super) fn charge_measured_peak(&mut self) {
        self.reservation.measured_peak_bytes =
            measured_peak_bytes(self.reservation.measured_peak_bytes, &self.process_evidence);
    }
}

/// The larger of this machine's own tree peak and the measured largest
/// process, or `None` when neither was measured.
fn measured_peak_bytes(own_tree_peak: Option<u64>, evidence: &ProcessEvidence) -> Option<u64> {
    let process = evidence
        .demand
        .as_ref()
        .filter(|demand| !matches!(demand.basis, ProcessDemandBasis::ClassPrior { .. }))
        .map(ProcessDemand::demand_bytes);
    match (own_tree_peak, process) {
        (Some(tree), Some(process)) => Some(tree.max(process)),
        (tree, process) => tree.or(process),
    }
}

/// The largest single process a request's work is known to hold, and where
/// that was learned.
///
/// Unlike a process tree's total this does not grow with build width, so a
/// measurement from any machine of the same platform is as true on this one,
/// and the largest is taken rather than a median: nothing beside one process
/// can inflate its own working set, so it needs no saturation quarantine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProcessDemand {
    /// The largest single process measured, or an out-of-memory victim's size,
    /// before headroom.
    pub peak_bytes: u64,
    pub name: Option<String>,
    /// The suite whose evidence set the number, when its did rather than the
    /// identity's own, as a reader names it (`rust-tests`, `class:cargoTest`).
    pub suite: Option<String>,
    /// The machine context (`device:executor`) the deciding number came from.
    pub measured_on: Option<String>,
    /// Kills of this suite in this machine context.
    pub kills_here: Option<SuiteKills>,
    /// Whether the number was measured on this platform, borrowed from
    /// another, or is the project's class prior.
    pub basis: ProcessDemandBasis,
}

impl ProcessDemand {
    /// What one process of the work is charged: the measurement with the
    /// measured-reservation headroom.
    pub(super) fn demand_bytes(&self) -> u64 {
        with_headroom(self.peak_bytes)
    }

    /// The larger of two demands, keeping the kill count of whichever has
    /// one: a batch must fit its heaviest item, and a suite killed here is
    /// killed here whichever item it was.
    pub(super) fn max(left: Option<Self>, right: Option<Self>) -> Option<Self> {
        match (left, right) {
            (None, other) | (other, None) => other,
            (Some(left), Some(right)) => {
                let kills = match (left.kills_here.clone(), right.kills_here.clone()) {
                    (Some(a), Some(b)) => Some(if a.alone >= b.alone { a } else { b }),
                    (a, b) => a.or(b),
                };
                let mut larger = if right.peak_bytes > left.peak_bytes {
                    right
                } else {
                    left
                };
                larger.kills_here = kills;
                Some(larger)
            }
        }
    }
}

/// A suite's out-of-memory kills in one machine context, split by what each
/// says about one process on this machine.
///
/// A kill while the victim ran alone is the machine's answer that one process
/// of the suite does not fit it. A kill while other commands shared the
/// machine is admission's over-commitment: evidence for the owed accounting
/// and the memory brake, never for the single-process fit. Counting the second
/// kind as the first excluded bginf-sp1 (121.7 GiB) from every Rust test lane
/// for a 32.5 GiB rustc after three kills under nineteen stacked lanes and no
/// memory brake (cairn/6496).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct SuiteKills {
    /// Kills while the victim ran alone, at `machine_total_bytes`, under
    /// `memory_brake`. A kill at another total, or under another brake state,
    /// started the count again.
    pub alone: u32,
    pub machine_total_bytes: u64,
    /// Whether the machine's memory brake existed at the alone kills. `None`
    /// for kills the ledger counted before it recorded their admission, all
    /// of which predate the brake.
    pub memory_brake: Option<bool>,
    pub crowded: Option<CrowdedKills>,
}

impl SuiteKills {
    /// These kills as the machine's admission now reads them. An alone kill
    /// is read from the load at its command's start; with no brake, work
    /// admitted after that start could crowd it unseen, so an alone kill under
    /// another brake state than the machine's current one has lost its basis
    /// and no longer counts (tenet 297).
    pub(super) fn under_brake(mut self, memory_brake_now: bool) -> Self {
        if self.memory_brake != Some(memory_brake_now) {
            self.alone = 0;
        }
        self
    }

    /// Fold one kill in.
    fn fold(mut self, kill: &SuiteKill, killed_at_unix_ms: u64) -> Self {
        match (kill.admission.alone(), kill.machine_total_bytes) {
            (true, Some(total)) => {
                let same_regime = self.alone > 0
                    && same_machine_total(self.machine_total_bytes, total)
                    && self.memory_brake == Some(kill.admission.memory_brake);
                self.alone = if same_regime { self.alone + 1 } else { 1 };
                self.machine_total_bytes = total;
                self.memory_brake = Some(kill.admission.memory_brake);
            }
            // Alone, but the machine's size was unread: nothing to count it
            // against, as before.
            (true, None) => {}
            (false, _) => {
                let prior = self.crowded.take();
                let most_co_resident = match (
                    prior.as_ref().and_then(|prior| prior.most_co_resident),
                    kill.admission.co_resident_commands,
                ) {
                    (Some(held), Some(now)) => Some(held.max(now)),
                    (held, now) => held.or(now),
                };
                self.crowded = Some(CrowdedKills {
                    count: prior.as_ref().map_or(0, |prior| prior.count) + 1,
                    most_co_resident,
                    memory_brake: kill.admission.memory_brake,
                    last_killed_at_unix_ms: prior
                        .map_or(0, |prior| prior.last_killed_at_unix_ms)
                        .max(killed_at_unix_ms),
                });
            }
        }
        self
    }
}

/// What admission held on the machine when one of a suite's processes was
/// killed there.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct KillAdmission {
    /// Commands that shared the machine with the victim when its command
    /// started, batch siblings included. `None` where no load was stated,
    /// which is never read as alone.
    pub co_resident_commands: Option<u32>,
    /// Whether the machine's memory brake existed: its stall instrument had
    /// measured, so it could hold new work.
    pub memory_brake: bool,
}

impl KillAdmission {
    /// The admission one completion ran under.
    pub(super) fn of(metadata: &CellExecutionMeta, machine: MachineAdmission) -> Self {
        Self {
            co_resident_commands: co_resident_commands(metadata, machine.batch_siblings),
            memory_brake: machine.memory_brake,
        }
    }

    fn alone(&self) -> bool {
        self.co_resident_commands == Some(0)
    }
}

/// What the runner knows of a machine's admission beside one completion that
/// the completion's own metadata does not say.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct MachineAdmission {
    pub memory_brake: bool,
    /// Items of the same parallel batch that ran beside this one.
    pub batch_siblings: u32,
}

/// The commands that shared the machine with `metadata`'s when it started,
/// from the load the executor stated plus `batch_siblings`, which the load
/// cannot see. Work admitted after the start is invisible here, which is why
/// an alone reading is only as good as the brake that kept such work off.
fn co_resident_commands(metadata: &CellExecutionMeta, batch_siblings: u32) -> Option<u32> {
    metadata.load_context.as_ref().map(|load| {
        load.co_resident_compile_jobs
            .saturating_add(load.co_resident_light_jobs)
            .saturating_add(batch_siblings)
    })
}

/// One out-of-memory kill as the suite ledger folds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SuiteKill {
    pub machine_total_bytes: Option<u64>,
    pub admission: KillAdmission,
}

/// What is known about one process of a request's work, split by whether it
/// can be judged as one process.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct ProcessEvidence {
    /// The largest single process measured: a peak the sampler named, or an
    /// out-of-memory victim, which is one process by construction. Carries
    /// this machine's kills of the suite.
    pub demand: Option<ProcessDemand>,
    /// The largest peak recorded with no process named. The macOS sampler
    /// attributes no peak to one process, so what it records is the process
    /// tree's, which grows with build width; it may inform a reservation but
    /// never the single-process fit. Kept so placement can say what it
    /// declined to judge.
    pub unattributed: Option<ProcessDemand>,
    /// The work compiles the project's Rust and no single process of it, of
    /// its suite, or of any of the project's Rust compiles was measured on
    /// any platform: placement offers it the largest machines first.
    pub unmeasured_compile: bool,
}

impl ProcessEvidence {
    /// A batch's evidence: the heaviest item on each side.
    pub(super) fn max(left: Self, right: Self) -> Self {
        Self {
            demand: ProcessDemand::max(left.demand, right.demand),
            unattributed: ProcessDemand::max(left.unattributed, right.unattributed),
            unmeasured_compile: left.unmeasured_compile || right.unmeasured_compile,
        }
    }

    fn is_empty(&self) -> bool {
        self.demand.is_none() && self.unattributed.is_none()
    }
}

/// The key a suite's evidence is learned under: a configured check's name,
/// else the command class, within the project. `None` for an unclassified
/// command, which is exactly the case nothing is known about: learning one
/// interactive shell's rustc under `other` would refuse every unclassified
/// command on a small machine.
pub(super) fn suite_key(
    project_id: &str,
    identity: Option<&CommandResourceIdentity>,
    class: CellCommandClass,
) -> Option<String> {
    match identity.and_then(|identity| identity.suite.as_deref()) {
        Some(suite) => Some(format!("{project_id}/{suite}")),
        None if class == CellCommandClass::Other => None,
        None => Some(format!(
            "{project_id}/{CLASS_SUITE_PREFIX}{}",
            class.as_str()
        )),
    }
}

/// What marks a suite keyed by command class rather than by a check's name.
const CLASS_SUITE_PREFIX: &str = "class:";

/// A suite key as a reader names it: without its project.
fn suite_display(suite_key: &str) -> String {
    suite_key
        .split_once('/')
        .map_or(suite_key, |(_, suite)| suite)
        .to_string()
}

/// The largest single process `identity` and its suite are known to hold on
/// this platform.
///
/// The identity's own number is the largest any context of the same os and
/// architecture measured, raised by any out-of-memory victim's size there. A
/// configured check's suite always joins it: a tiered lane is one suite whose
/// identity moves with its configuration, and any of its runs may compile the
/// whole closure. A command-class suite only stands in for an identity that
/// has measured nothing, since a class spans unrelated commands, and an
/// identity measured once on any machine then speaks for itself.
///
/// A peak with no process named is never a single-process demand: the macOS
/// sampler cannot attribute its peak to one process, so the number it records
/// is the tree's, and judging it as one process refused the only macOS machine
/// a suite it had just completed there (cairn/6310). Such a peak is returned
/// as `unattributed`, for the reservation and the record; pricing a borrowed
/// tree peak is cairn/6263's.
///
/// A platform that has measured nothing is not left blind (cairn/6312). A
/// cold identity on a platform whose sampler, executor build or machines had
/// never measured it was placed with no single-process demand at all, and
/// the measurement that would have refused it was the kernel's kill. So when
/// this platform has no named number, the identity's and its suite's largest
/// named number on any other platform stands in, marked with the platform it
/// came from; and when no platform has one, a Rust compile (`class`) stands
/// on the project's largest measured rustc across its suites, a class prior
/// that placement defers small machines on rather than refusing them. Only
/// when the project has measured no rustc anywhere is a Rust compile left
/// without a number, and then `unmeasured_compile` says so.
pub(super) async fn resolve_process_demand(
    db: Arc<LocalDb>,
    identity: Option<&CommandResourceIdentity>,
    suite_key: Option<&str>,
    class: CellCommandClass,
    context: &ProfileContext,
) -> ProcessEvidence {
    let (own, own_elsewhere) = match identity {
        Some(identity) => load_identity_process_peak(db.clone(), identity, context)
            .await
            .unwrap_or_default(),
        None => (ProcessEvidence::default(), None),
    };
    let suite = match suite_key {
        Some(key) => load_suite_evidence(db.clone(), key, context)
            .await
            .ok()
            .flatten(),
        None => None,
    };
    let class_suite =
        suite_key.is_some_and(|key| suite_display(key).starts_with(CLASS_SUITE_PREFIX));
    let suite_applies = !class_suite || own.is_empty();
    let measured_nowhere = own.is_empty() && own_elsewhere.is_none();
    let kills_here = suite.as_ref().and_then(|suite| suite.kills_here.clone());
    let suite_peak = suite
        .as_ref()
        .filter(|_| suite_applies)
        .and_then(|suite| suite.peak.clone())
        .map(|(bytes, name, measured_on)| ProcessDemand {
            peak_bytes: bytes,
            name,
            suite: suite_key.map(suite_display),
            measured_on: Some(measured_on),
            kills_here: None,
            basis: ProcessDemandBasis::ThisPlatform,
        });
    // The suite's row keeps one peak, a measurement or a victim, with its
    // name; unnamed, it cannot be told from a tree's total, so it is one.
    let (suite_named, suite_unattributed) = match suite_peak {
        Some(peak) if peak.name.is_some() => (Some(peak), None),
        unattributed => (None, unattributed),
    };
    let unattributed = ProcessDemand::max(own.unattributed, suite_unattributed);
    let mut demand = ProcessDemand::max(own.demand, suite_named);
    if demand.is_none() {
        let suite_elsewhere = suite
            .as_ref()
            .filter(|_| !class_suite || measured_nowhere)
            .and_then(|suite| suite.elsewhere.clone())
            .map(|(bytes, name, measured_on, platform)| ProcessDemand {
                peak_bytes: bytes,
                name: Some(name),
                suite: suite_key.map(suite_display),
                measured_on: Some(measured_on),
                kills_here: None,
                basis: ProcessDemandBasis::OtherPlatform { platform },
            });
        demand = ProcessDemand::max(own_elsewhere, suite_elsewhere);
    }
    if demand.is_none() && class.compiles_rust() {
        if let Some((project_id, _)) = suite_key.and_then(|key| key.split_once('/')) {
            demand = load_class_prior(db, project_id, context)
                .await
                .ok()
                .flatten();
        }
    }
    let unmeasured_compile = demand.is_none() && class.compiles_rust();
    match (&mut demand, kills_here) {
        (Some(demand), kills) => demand.kills_here = kills,
        (None, Some(kills)) => {
            demand = Some(ProcessDemand {
                peak_bytes: 0,
                name: None,
                suite: suite_key.map(suite_display),
                measured_on: Some(context.executor_class.clone()),
                kills_here: Some(kills),
                basis: ProcessDemandBasis::ThisPlatform,
            })
        }
        (None, None) => {}
    }
    ProcessEvidence {
        demand,
        unattributed,
        unmeasured_compile,
    }
}

/// The name the Linux sampler and the kernel give the Rust compiler, which
/// marks a peak as one of the project's Rust compiles whatever suite it ran
/// under.
const RUST_COMPILER_PROCESS: &str = "rustc";

/// Another platform as a reader of `context`'s machine names it: its
/// architecture, or its system and architecture when the system differs.
fn platform_label(os: &str, arch: &str, context: &ProfileContext) -> String {
    if os == context.os {
        arch.to_string()
    } else {
        format!("{os} {arch}")
    }
}

/// What one suite has measured: on this platform, its largest process with
/// its name and the context that measured it, and its kills in this
/// context; elsewhere, its largest named process with the context and the
/// platform that measured it.
struct SuiteEvidence {
    peak: Option<(u64, Option<String>, String)>,
    kills_here: Option<SuiteKills>,
    elsewhere: Option<(u64, String, String, String)>,
}

/// The identity's evidence on this platform, and its largest single process
/// measured on any other.
async fn load_identity_process_peak(
    db: Arc<LocalDb>,
    identity: &CommandResourceIdentity,
    context: &ProfileContext,
) -> Result<(ProcessEvidence, Option<ProcessDemand>), String> {
    let identity = identity.clone();
    let context = context.clone();
    db.read(|conn| {
        let identity = identity.clone();
        let context = context.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT executor_class, upper_process_peak_bytes, process_peak_name, oom_victim_rss_bytes, os, arch
                     FROM command_resource_profiles
                     WHERE identity_version=?1 AND command_identity=?2",
                    params![identity.version as i64, identity.key],
                )
                .await?;
            let mut evidence = ProcessEvidence::default();
            let mut elsewhere: Option<ProcessDemand> = None;
            while let Some(row) = rows.next().await? {
                let measured_on = row.get::<String>(0)?;
                let measured = row.get::<Option<i64>>(1)?.map(|value| value as u64);
                let name = row.get::<Option<String>>(2)?;
                let victim = row.get::<Option<i64>>(3)?.map(|value| value as u64);
                let os = row.get::<String>(4)?;
                let arch = row.get::<String>(5)?;
                let basis = if os == context.os && arch == context.arch {
                    ProcessDemandBasis::ThisPlatform
                } else {
                    ProcessDemandBasis::OtherPlatform {
                        platform: platform_label(&os, &arch, &context),
                    }
                };
                let at = |peak_bytes, name| {
                    Some(ProcessDemand {
                        peak_bytes,
                        name,
                        suite: None,
                        measured_on: Some(measured_on.clone()),
                        kills_here: None,
                        basis: basis.clone(),
                    })
                };
                // A victim is one process whether or not it was named; a
                // measured peak is one only when its process was.
                let (single, unattributed) = match name {
                    Some(_) => (measured.max(victim), None),
                    None => (victim, measured),
                };
                if !basis.is_this_platform() {
                    // Another platform lends only a single process: a tree's
                    // total there says even less here than it does there.
                    if let Some(peak) = single {
                        elsewhere = ProcessDemand::max(elsewhere, at(peak, name));
                    }
                    continue;
                }
                if let Some(peak) = single {
                    evidence.demand = ProcessDemand::max(evidence.demand, at(peak, name));
                }
                if let Some(peak) = unattributed {
                    evidence.unattributed =
                        ProcessDemand::max(evidence.unattributed, at(peak, None));
                }
            }
            Ok((evidence, elsewhere))
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_suite_evidence(
    db: Arc<LocalDb>,
    suite_key: &str,
    context: &ProfileContext,
) -> Result<Option<SuiteEvidence>, String> {
    let suite_key = suite_key.to_string();
    let context = context.clone();
    db.read(|conn| {
        let suite_key = suite_key.clone();
        let context = context.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT executor_class, upper_process_peak_bytes, process_peak_name,
                            oom_kill_count, oom_machine_total_bytes, os, arch,
                            oom_kill_memory_brake, oom_crowded_kill_count,
                            oom_crowded_most_co_resident, oom_crowded_memory_brake,
                            oom_crowded_killed_at_unix_ms
                     FROM suite_memory_evidence
                     WHERE suite_key=?1",
                    params![suite_key],
                )
                .await?;
            let mut evidence = SuiteEvidence {
                peak: None,
                kills_here: None,
                elsewhere: None,
            };
            let mut any = false;
            while let Some(row) = rows.next().await? {
                any = true;
                let executor_class = row.get::<String>(0)?;
                let os = row.get::<String>(5)?;
                let arch = row.get::<String>(6)?;
                if os != context.os || arch != context.arch {
                    let peak = row.get::<Option<i64>>(1)?.map(|value| value as u64);
                    if let (Some(peak), Some(name)) = (peak, row.get::<Option<String>>(2)?) {
                        if evidence
                            .elsewhere
                            .as_ref()
                            .is_none_or(|(held, ..)| peak > *held)
                        {
                            evidence.elsewhere = Some((
                                peak,
                                name,
                                executor_class,
                                platform_label(&os, &arch, &context),
                            ));
                        }
                    }
                    continue;
                }
                if let Some(peak) = row.get::<Option<i64>>(1)?.map(|value| value as u64) {
                    if evidence
                        .peak
                        .as_ref()
                        .is_none_or(|(held, _, _)| peak > *held)
                    {
                        evidence.peak =
                            Some((peak, row.get::<Option<String>>(2)?, executor_class.clone()));
                    }
                }
                if executor_class == context.executor_class {
                    let kills = suite_kills_from_row(&row, 3)?;
                    if kills.alone > 0 || kills.crowded.is_some() {
                        evidence.kills_here = Some(kills);
                    }
                }
            }
            Ok(any.then_some(evidence))
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The kill columns of a `suite_memory_evidence` row, from `first`:
/// `oom_kill_count, oom_machine_total_bytes`, then two columns this reader
/// skips (`os, arch`), then `oom_kill_memory_brake, oom_crowded_kill_count,
/// oom_crowded_most_co_resident, oom_crowded_memory_brake,
/// oom_crowded_killed_at_unix_ms`.
fn suite_kills_from_row(
    row: &cairn_db::turso::Row,
    first: usize,
) -> Result<SuiteKills, cairn_db::turso::Error> {
    let alone = row.get::<i64>(first)?.max(0) as u32;
    let total = row.get::<Option<i64>>(first + 1)?;
    let crowded_count = row.get::<i64>(first + 5)?.max(0) as u32;
    Ok(SuiteKills {
        alone: if total.is_some() { alone } else { 0 },
        machine_total_bytes: total.map_or(0, |total| total.max(0) as u64),
        memory_brake: row.get::<Option<i64>>(first + 4)?.map(|brake| brake != 0),
        crowded: (crowded_count > 0)
            .then(|| -> Result<_, cairn_db::turso::Error> {
                Ok(CrowdedKills {
                    count: crowded_count,
                    most_co_resident: row
                        .get::<Option<i64>>(first + 6)?
                        .map(|commands| commands.max(0) as u32),
                    memory_brake: row
                        .get::<Option<i64>>(first + 7)?
                        .is_some_and(|brake| brake != 0),
                    last_killed_at_unix_ms: row
                        .get::<Option<i64>>(first + 8)?
                        .map_or(0, |at| at.max(0) as u64),
                })
            })
            .transpose()?,
    })
}

/// The class prior for a Rust compile of `project_id` that nothing has
/// measured on any platform: the largest rustc any of the project's suites
/// measured, or lost to the kernel, on any machine. A largest rather than a
/// typical number, because what it guards against is a kill.
async fn load_class_prior(
    db: Arc<LocalDb>,
    project_id: &str,
    context: &ProfileContext,
) -> Result<Option<ProcessDemand>, String> {
    let prefix = format!("{project_id}/");
    let context = context.clone();
    db.read(|conn| {
        let prefix = prefix.clone();
        let context = context.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT suite_key, executor_class, os, arch, upper_process_peak_bytes
                     FROM suite_memory_evidence
                     WHERE substr(suite_key, 1, ?2) = ?1 AND process_peak_name = ?3
                       AND upper_process_peak_bytes IS NOT NULL
                     ORDER BY upper_process_peak_bytes DESC
                     LIMIT 1",
                    params![prefix.clone(), prefix.len() as i64, RUST_COMPILER_PROCESS],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let suite_key = row.get::<String>(0)?;
            let os = row.get::<String>(2)?;
            let arch = row.get::<String>(3)?;
            Ok(Some(ProcessDemand {
                peak_bytes: row.get::<i64>(4)?.max(0) as u64,
                name: Some(RUST_COMPILER_PROCESS.to_string()),
                suite: Some(suite_display(&suite_key)),
                measured_on: Some(row.get::<String>(1)?),
                kills_here: None,
                basis: ProcessDemandBasis::ClassPrior {
                    platform: (os != context.os || arch != context.arch)
                        .then(|| platform_label(&os, &arch, &context)),
                },
            }))
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// How far a machine's total memory must move from the total its kills
/// happened at before they stop counting against it: a sixteenth, as for the
/// solo-kill exclusion, so a kernel update that moves the total by a few
/// megabytes does not read as a new machine.
pub(super) const KILL_TOTAL_MARGIN_DIVISOR: u64 = 16;

/// Whether `total_bytes` is still the machine its kills happened on.
pub(super) fn same_machine_total(killed_at_total: u64, total_bytes: u64) -> bool {
    total_bytes.abs_diff(killed_at_total) <= killed_at_total / KILL_TOTAL_MARGIN_DIVISOR
}

/// Fold one completion into its suite's evidence in this machine context:
/// the largest single process it held, and, for an out-of-memory kill, the
/// victim's size and the kill, read alone or crowded by `machine`'s admission
/// ([`SuiteKills`]).
///
/// Every completion with a suite teaches this, measured identity or not: an
/// agent's batch carries no command identity, and its suite is the only
/// place its evidence can live. The peak is rise-only, with no decay: warm
/// runs say nothing about the cold compile, and one process's working set
/// depends on the code, not on how busy the machine was. The kills are not: a
/// successful run of the suite here is the machine's later answer that it can
/// hold it, and clears them.
pub(super) async fn observe_suite(
    db: Arc<LocalDb>,
    suite_key: Option<&str>,
    context: &ProfileContext,
    metadata: &CellExecutionMeta,
    kind: CompletionKind,
    machine: MachineAdmission,
) {
    let Some(suite_key) = suite_key else { return };
    if kind == CompletionKind::SelectedNothing {
        return;
    }
    let mut peak = metadata
        .peak_process
        .as_ref()
        .map(|peak| (peak.bytes, peak.name.clone()));
    let mut kills = KillsSeen::Neither;
    if let Some(CommandStop::OomKilled {
        rss_bytes,
        machine_total_bytes,
        process_name,
        ..
    }) = &metadata.stop
    {
        if let Some(victim) = rss_bytes {
            if peak.as_ref().is_none_or(|(held, _)| victim > held) {
                peak = Some((*victim, process_name.clone()));
            }
        }
        kills = KillsSeen::Killed(SuiteKill {
            machine_total_bytes: *machine_total_bytes,
            admission: KillAdmission::of(metadata, machine),
        });
    } else if kind == CompletionKind::Succeeded {
        kills = KillsSeen::CleanRun;
    }
    let _ = record_suite_evidence(
        db,
        suite_key,
        context,
        metadata.finished_at_unix_ms,
        peak,
        kills,
        None,
    )
    .await;
}

/// What one observation says about a suite's kills here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KillsSeen {
    Killed(SuiteKill),
    /// The suite ran to success here: whatever killed it before no longer
    /// does, and its kills here are forgotten.
    CleanRun,
    Neither,
}

/// Forget every suite's kills on `executor_id`, whatever machine context it
/// was in, and the solo-kill ceiling its command profiles hold: the
/// operator's `repaired`, the word for what no run of the machine has had the
/// chance to show. Returns the suites cleared.
pub(crate) async fn forget_kills_on(db: &LocalDb, executor_id: &str) -> Result<u64, String> {
    // A context is `device:executor`; the device can be unknown to a runner
    // the machine is not connected to, so the executor's half identifies it.
    let suffix = format!(":{executor_id}");
    let cleared = db
        .execute(
            format!(
                "UPDATE suite_memory_evidence SET {FORGET_SUITE_KILLS}
                 WHERE substr(executor_class, -?2) = ?1
                   AND (oom_kill_count > 0 OR oom_crowded_kill_count > 0)"
            ),
            (suffix.clone(), suffix.len() as i64),
        )
        .await
        .map_err(|error| format!("Failed to forget the machine's suite kills: {error}"))?;
    db.execute(
        "UPDATE command_resource_profiles SET oom_alone_machine_bytes = NULL
         WHERE substr(executor_class, -?2) = ?1 AND oom_alone_machine_bytes IS NOT NULL",
        (suffix.clone(), suffix.len() as i64),
    )
    .await
    .map_err(|error| format!("Failed to forget the machine's solo kills: {error}"))?;
    Ok(cleared)
}

/// The assignments that empty a suite row's kill ledger.
const FORGET_SUITE_KILLS: &str = "oom_kill_count = 0, oom_machine_total_bytes = NULL,
     oom_kill_memory_brake = NULL, oom_crowded_kill_count = 0,
     oom_crowded_most_co_resident = NULL, oom_crowded_memory_brake = NULL,
     oom_crowded_killed_at_unix_ms = NULL";

/// One out-of-memory kill of a suite's process outside any completion: a
/// resident's, which the executor reports on its health rather than on a
/// result. Its victim's size and name are learned when the executor read
/// them. A resident states no load at its start, so its kill is never read as
/// alone: it counts as crowded, with `memory_brake` the machine's brake state.
///
/// Idempotent by `(machine context, killed_at_unix_ms, pid)`: the machine
/// re-reports a kill on every snapshot for a day, and a runner that restarts
/// reads it again.
pub(super) async fn observe_suite_kill(
    db: Arc<LocalDb>,
    suite_key: &str,
    context: &ProfileContext,
    kill: &cairn_common::executor_protocol::OomKillReport,
    machine_total_bytes: Option<u64>,
    memory_brake: bool,
) {
    let victim = kill
        .footprint_bytes
        .map(|bytes| (bytes, kill.process_name.clone()));
    if victim.is_none() && machine_total_bytes.is_none() {
        return;
    }
    let _ = record_suite_evidence(
        db,
        suite_key,
        context,
        kill.killed_at_unix_ms,
        victim,
        KillsSeen::Killed(SuiteKill {
            machine_total_bytes,
            admission: KillAdmission {
                co_resident_commands: None,
                memory_brake,
            },
        }),
        Some(kill.pid),
    )
    .await;
}

/// How old a resident kill may be and still be learned. An executor keeps its
/// last kills by count, not age, so it may report one for days; the runner
/// learns only kills younger than this, measured against now.
pub(super) const RESIDENT_KILL_LEARNING_HORIZON_MS: u64 = 24 * 60 * 60 * 1_000;

/// How long a learned resident kill's identity is kept: twice the learning
/// horizon, so every kill still young enough to be learned keeps its identity
/// and is never counted again, and pruning removes only identities older than
/// anything the runner would replay.
const RESIDENT_KILL_IDENTITY_KEPT_MS: u64 = 2 * RESIDENT_KILL_LEARNING_HORIZON_MS;

async fn record_suite_evidence(
    db: Arc<LocalDb>,
    suite_key: &str,
    context: &ProfileContext,
    finished_at_unix_ms: u64,
    peak: Option<(u64, Option<String>)>,
    kills: KillsSeen,
    resident_kill: Option<u32>,
) -> Result<(), String> {
    if peak.is_none() && kills == KillsSeen::Neither {
        return Ok(());
    }
    let suite_key = suite_key.to_string();
    let context = context.clone();
    db.write(move |conn| {
        let suite_key = suite_key.clone();
        let context = context.clone();
        let peak = peak.clone();
        Box::pin(async move {
            // A resident kill is re-reported on every health snapshot until it
            // ages out of the machine's record; its stored identity is what
            // keeps it counted once, across runner restarts too.
            if let Some(pid) = resident_kill {
                let inserted = conn
                    .execute(
                        "INSERT INTO resident_kills_learned (executor_class, killed_at_unix_ms, pid)
                         VALUES (?1, ?2, ?3) ON CONFLICT DO NOTHING",
                        params![
                            context.executor_class.clone(),
                            finished_at_unix_ms as i64,
                            pid as i64
                        ],
                    )
                    .await?;
                if inserted == 0 {
                    return Ok(());
                }
                conn.execute(
                    "DELETE FROM resident_kills_learned WHERE executor_class=?1 AND killed_at_unix_ms < ?2",
                    params![
                        context.executor_class.clone(),
                        finished_at_unix_ms.saturating_sub(RESIDENT_KILL_IDENTITY_KEPT_MS) as i64
                    ],
                )
                .await?;
            }
            let key = params![
                suite_key.clone(),
                context.executor_class.clone(),
                context.os.clone(),
                context.arch.clone()
            ];
            if peak.is_none() && kills == KillsSeen::CleanRun {
                // Nothing to learn but that the kills are over: a row that
                // does not exist has none to forget.
                conn.execute(
                    &format!(
                        "UPDATE suite_memory_evidence SET {FORGET_SUITE_KILLS}
                         WHERE suite_key=?1 AND executor_class=?2 AND os=?3 AND arch=?4"
                    ),
                    key,
                )
                .await?;
                return Ok(());
            }
            let held = {
                let mut rows = conn
                    .query(
                        "SELECT oom_kill_count, oom_machine_total_bytes, 0, 0,
                                oom_kill_memory_brake, oom_crowded_kill_count,
                                oom_crowded_most_co_resident, oom_crowded_memory_brake,
                                oom_crowded_killed_at_unix_ms
                         FROM suite_memory_evidence
                         WHERE suite_key=?1 AND executor_class=?2 AND os=?3 AND arch=?4",
                        key,
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => suite_kills_from_row(&row, 0)?,
                    None => SuiteKills::default(),
                }
            };
            let ledger = match kills {
                KillsSeen::Killed(kill) => held.fold(&kill, finished_at_unix_ms),
                KillsSeen::CleanRun => SuiteKills::default(),
                KillsSeen::Neither => held,
            };
            let crowded = ledger.crowded.as_ref();
            let (peak_bytes, peak_name) = match &peak {
                Some((bytes, name)) => (Some(*bytes as i64), name.clone()),
                None => (None, None),
            };
            conn.execute(
                r#"INSERT INTO suite_memory_evidence (suite_key, executor_class, os, arch, upper_process_peak_bytes, process_peak_name, oom_kill_count, oom_machine_total_bytes, updated_at_unix_ms, oom_kill_memory_brake, oom_crowded_kill_count, oom_crowded_most_co_resident, oom_crowded_memory_brake, oom_crowded_killed_at_unix_ms)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
                   ON CONFLICT(suite_key, executor_class, os, arch) DO UPDATE SET
                     upper_process_peak_bytes=CASE WHEN ?5 IS NULL THEN suite_memory_evidence.upper_process_peak_bytes WHEN suite_memory_evidence.upper_process_peak_bytes IS NULL OR ?5 >= suite_memory_evidence.upper_process_peak_bytes THEN ?5 ELSE suite_memory_evidence.upper_process_peak_bytes END,
                     process_peak_name=CASE WHEN ?5 IS NOT NULL AND (suite_memory_evidence.upper_process_peak_bytes IS NULL OR ?5 >= suite_memory_evidence.upper_process_peak_bytes) THEN COALESCE(?6, suite_memory_evidence.process_peak_name) ELSE suite_memory_evidence.process_peak_name END,
                     oom_kill_count=?7,
                     oom_machine_total_bytes=?8,
                     updated_at_unix_ms=MAX(suite_memory_evidence.updated_at_unix_ms, ?9),
                     oom_kill_memory_brake=?10,
                     oom_crowded_kill_count=?11,
                     oom_crowded_most_co_resident=?12,
                     oom_crowded_memory_brake=?13,
                     oom_crowded_killed_at_unix_ms=?14"#,
                params![
                    suite_key.clone(),
                    context.executor_class.clone(),
                    context.os.clone(),
                    context.arch.clone(),
                    peak_bytes,
                    peak_name,
                    ledger.alone as i64,
                    (ledger.alone > 0).then_some(ledger.machine_total_bytes as i64),
                    finished_at_unix_ms as i64,
                    ledger.memory_brake.map(i64::from),
                    crowded.map_or(0, |crowded| crowded.count as i64),
                    crowded.and_then(|crowded| crowded.most_co_resident.map(i64::from)),
                    crowded.map(|crowded| i64::from(crowded.memory_brake)),
                    crowded.map(|crowded| crowded.last_killed_at_unix_ms as i64)
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The conservative safety prior for a work class that has never been measured
/// on this machine.
///
/// This is a floor chosen so that a cold start does not overcommit a host, and
/// it is marked [`ResourceReservationSource::Unmeasured`] precisely so nothing
/// downstream mistakes it for evidence. It replaces an anonymous 512 MiB that
/// was applied to a `cargo test` suite and a `tsc --noEmit` alike: one of those
/// numbers was a fiction and the other was merely wrong, and neither said which.
///
/// The classes are the ones the fleet already distinguishes
/// ([`CellCommandClass`]), so a prior is attached to a name the rest of the
/// system already uses rather than to a new taxonomy. Every value here is a
/// starting point that the learned profile displaces after
/// [`MIN_CONFIDENT_SAMPLES`] observations.
pub(super) fn cold_start_prior(
    class: CellCommandClass,
    capabilities: &ExecutorCapabilities,
) -> ResourceReservation {
    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;
    // A Rust compilation front is the memory-hungriest thing this fleet runs and
    // writes a target directory measured in gigabytes; a type-check holds a
    // program in memory but writes almost nothing; a browser-less unit suite is
    // modest on both. `Other` keeps the historical floor, because an unclassified
    // command is exactly the case nothing is known about.
    let (memory, disk) = match class {
        CellCommandClass::CargoTest
        | CellCommandClass::CargoClippy
        | CellCommandClass::CargoCheck => (2 * GIB, 4 * GIB),
        CellCommandClass::Build => (1536 * MIB, 2 * GIB),
        CellCommandClass::Typecheck => (1536 * MIB, 512 * MIB),
        CellCommandClass::Vitest => (GIB, GIB),
        CellCommandClass::Other => (512 * MIB, GIB),
    };
    // Memory always leaves the same host reserve as learned demand. A small
    // machine does not make the operating system and executor disappear.
    let memory = capabilities
        .memory_budget_bytes
        .map_or(memory, |budget| memory.min(memory_reservation_cap(budget)));
    let disk = capabilities
        .disk_budget_bytes
        .map_or(disk, |budget| disk.min(budget));
    ResourceReservation {
        memory_bytes: memory,
        disk_growth_bytes: disk,
        // One unit is the cold-start prior for every class. Deriving more from a
        // CPU percentage would turn an observation about how hard a machine was
        // pushed into a claim about how many lanes this work needs, which is a
        // different question with no evidence behind it yet.
        concurrency_units: 1,
        source: ResourceReservationSource::Unmeasured,
        measured_peak_bytes: None,
    }
}

/// What the rest of the fleet has measured one command doing, one high-water
/// mark per machine context that has seen it enough times to speak.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct FleetEvidence {
    /// Upper peak resident bytes, one entry per contributing context.
    peaks: Vec<u64>,
    /// Upper disk growth, one entry per contributing context.
    disks: Vec<u64>,
}

/// The prior for a command this machine has never measured, taken from the
/// machines that have.
///
/// The cold-start case is the permissive one, and on a large machine it is
/// permissive to the point of being no brake at all: a class prior cannot tell a
/// workspace build from a type-check, so eight unprofiled cargo builds offered
/// to a fresh 121 GiB machine each reserved two gigabytes, fit trivially, and
/// took the machine down (CAIRN-4365). The fleet already knew what those
/// commands cost — the same identities carried hundreds of samples between 11
/// and 15 gigabytes on three other machines — and nothing consulted them.
///
/// Memory and disk demand travel between machines; speed does not. A compiler
/// holds roughly the same working set wherever it runs, which is why this
/// borrows a reservation and [`resolve_duration`] deliberately borrows nothing:
/// how fast a machine is is exactly the thing another machine cannot tell you.
///
/// The statistic is the median of the per-context high-water marks, not their
/// maximum. Each contributing number is already an upper, so their median is the
/// typical worst case rather than a typical case — and it gives one poisoned
/// context a single vote, the same reason [`resolve_duration`] predicts from a
/// median. A maximum would let one observation taken on a saturated machine
/// (64 GiB for a command whose fleet-wide floor is 2.4) become every other
/// machine's opening reservation.
fn fleet_prior(
    evidence: &FleetEvidence,
    class_prior: &ResourceReservation,
    memory_budget_bytes: Option<u64>,
) -> Option<ResourceReservation> {
    let memory = fleet_memory_demand(evidence, memory_budget_bytes)?;
    let memory =
        memory_budget_bytes.map_or(memory, |budget| memory.min(memory_reservation_cap(budget)));
    // A prior is a floor. Borrowed evidence may raise the class prior and never
    // lower it, so a fleet that happens to have run this command on idle
    // machines cannot talk a host into admitting more than a cold start allows.
    let memory_bytes = memory.max(class_prior.memory_bytes);
    if memory_bytes <= class_prior.memory_bytes {
        return None;
    }
    // Same floor rule as memory above: borrowed disk evidence may raise the
    // class prior and never lower it, and no evidence at all leaves it standing.
    let disk_growth_bytes = if evidence.disks.is_empty() {
        class_prior.disk_growth_bytes
    } else {
        with_headroom(upper_median(&evidence.disks)).max(class_prior.disk_growth_bytes)
    };
    Some(ResourceReservation {
        memory_bytes,
        disk_growth_bytes,
        concurrency_units: class_prior.concurrency_units,
        // Borrowed, therefore still unmeasured HERE. This is what keeps the
        // executor's cold-work brake applying to it and keeps every downstream
        // reader from mistaking another machine's history for this one's.
        source: ResourceReservationSource::Unmeasured,
        measured_peak_bytes: None,
    })
}

/// What the rest of the fleet measured this command needing, with headroom and
/// before this machine's cap, or `None` when nothing was measured or the
/// borrowed peak is quarantined here.
fn fleet_memory_demand(evidence: &FleetEvidence, memory_budget_bytes: Option<u64>) -> Option<u64> {
    if evidence.peaks.is_empty() {
        return None;
    }
    let borrowed_peak = upper_median(&evidence.peaks);
    // The quarantine that guards a machine's own observations guards a borrowed
    // one for the same reason and with more force: a peak that would consume
    // this machine describes some other machine's saturation, or a machine this
    // command has no business running on. Either way it is not this machine's
    // prior, and letting it through would refuse the work here forever.
    if memory_peak_quarantined(borrowed_peak, memory_budget_bytes) {
        return None;
    }
    Some(with_headroom(borrowed_peak))
}

/// The conservative duration a work class is assumed to take on a machine that
/// has never run it.
///
/// A class prior, never a machine-speed constant. Every machine gets the same
/// number for the same class, which is precisely what makes it useless for
/// deciding between two unmeasured machines — and that is the point. Absent
/// evidence, placement must not manufacture a reason to prefer one host over
/// another; the queue forecast and the deterministic tiebreaks decide instead,
/// and the record says the run leg was [`DurationEvidence::Unmeasured`].
///
/// The values are ordered by what these classes actually are: a Rust
/// compilation front is minutes, a bundler or a type-check is under a minute, an
/// unclassified command is assumed short because nothing is known about it and
/// over-predicting would keep work off machines that have never been tried.
pub(super) fn duration_prior(class: CellCommandClass) -> u64 {
    const SECOND: u64 = 1_000;
    match class {
        CellCommandClass::CargoTest
        | CellCommandClass::CargoClippy
        | CellCommandClass::CargoCheck => 300 * SECOND,
        CellCommandClass::Build => 60 * SECOND,
        CellCommandClass::Typecheck | CellCommandClass::Vitest => 30 * SECOND,
        CellCommandClass::Other => 10 * SECOND,
    }
}

/// The labeled class prior, dressed as the estimate every caller reads.
pub(super) fn unmeasured_duration(
    class: CellCommandClass,
    context: &ProfileContext,
    identity: Option<&CommandResourceIdentity>,
    warmth: ExecutionWarmth,
    fallback: DurationFallback,
) -> DurationEstimate {
    DurationEstimate {
        predicted_ms: duration_prior(class),
        source: DurationEvidence::Unmeasured,
        sample_count: 0,
        profile_key: identity.map(|identity| identity.key.clone()),
        profile_context: context.describe(),
        warmth,
        updated_at_unix_ms: None,
        fallback: Some(fallback),
    }
}

/// How long this command is predicted to run on this machine at this warmth.
///
/// Predicts from the median of a bounded recent window rather than from a
/// high-water mark, because ranking and reserving want opposite things from the
/// same observations. A reservation must cover the worst case it has seen or it
/// stops being a safety margin; a prediction must describe the typical case or
/// one bad afternoon — a machine that swapped, a network mount that stalled —
/// becomes that machine's permanent answer and quietly removes it from the
/// fleet. The median gives the outlier one vote out of the window's size, and
/// the window's bound is what eventually retires it altogether.
pub(super) async fn resolve_duration(
    db: Arc<LocalDb>,
    identity: Option<&CommandResourceIdentity>,
    context: &ProfileContext,
    warmth: ExecutionWarmth,
    class: CellCommandClass,
    now_unix_ms: u64,
) -> DurationEstimate {
    let Some(identity) = identity else {
        return unmeasured_duration(
            class,
            context,
            None,
            warmth,
            DurationFallback::NoCommandIdentity,
        );
    };
    let prior = |fallback| unmeasured_duration(class, context, Some(identity), warmth, fallback);
    let profile = match load_duration_profile(db, identity, context, warmth).await {
        Ok(Some(profile)) => profile,
        Ok(None) => return prior(DurationFallback::NoProfileRecorded),
        Err(_) => return prior(DurationFallback::ProfileLookupFailed),
    };
    if profile.recent_ms.len() < MIN_CONFIDENT_DURATION_SAMPLES as usize {
        return prior(DurationFallback::BelowConfidenceFloor);
    }
    // Silence is not evidence of continuity. A profile whose newest observation
    // predates the age limit describes a machine that may have been reinstalled,
    // re-toolchained, or filled up since, so it stops speaking and says why.
    if now_unix_ms.saturating_sub(profile.updated_at_unix_ms) > DURATION_PROFILE_STALE_AFTER_MS {
        return prior(DurationFallback::ProfileTooOld);
    }
    DurationEstimate {
        predicted_ms: upper_median(&profile.recent_ms),
        source: DurationEvidence::Learned,
        sample_count: profile.sample_count,
        profile_key: Some(identity.key.clone()),
        profile_context: context.describe(),
        warmth,
        updated_at_unix_ms: Some(profile.updated_at_unix_ms),
        fallback: None,
    }
}

/// What this machine had learned to expect of one run of `identity` as the run
/// began: its learned speed alone here, times the learned slowdown for the
/// co-resident work it started beside, plus the stall it started under. These
/// are the terms placement predicts with, recomputed from the run's own
/// starting readings, so the run's result can say how far past its prediction
/// it went. `None` unless the speed is learned: a class prior is a starting
/// point, not an expectation a run can fall short of.
///
/// The caller reads it before the run is folded in, so a run is never measured
/// against a profile it has already moved.
pub(super) async fn learned_expectation(
    db: Arc<LocalDb>,
    identity: &CommandResourceIdentity,
    context: &ProfileContext,
    metadata: &CellExecutionMeta,
) -> Option<u64> {
    let warmth = metadata.warmth?;
    let started = metadata.started_at_unix_ms;
    let alone = resolve_duration(
        db.clone(),
        Some(identity),
        context,
        warmth,
        CellCommandClass::Other,
        started,
    )
    .await;
    if !alone.is_learned() || alone.predicted_ms == 0 {
        return None;
    }
    let load = metadata.load_context.clone().unwrap_or_default();
    let contention = resolve_contention(db, context, &load, started).await;
    let crowded = u64::try_from(
        u128::from(alone.predicted_ms) * u128::from(contention.multiplier_millis) / 1_000,
    )
    .unwrap_or(u64::MAX);
    let stall = match StartingStall::of(&load) {
        StartingStall::Measured(full_millis) => stall_penalty_ms(crowded, full_millis),
        StartingStall::Unmeasured | StartingStall::Stale => 0,
    };
    Some(crowded.saturating_add(stall))
}

/// The upper median of a non-empty window. Integer throughout: a prediction in
/// whole milliseconds is already finer than anything downstream compares on.
fn upper_median(samples: &[u64]) -> u64 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

/// The rationale for a reservation the caller stated itself.
pub(super) fn declared_rationale(
    context: &ProfileContext,
    prior: ResourceReservation,
) -> ReservationRationale {
    ReservationRationale {
        declared_concurrency_units: Some(prior.concurrency_units),
        profile_key: None,
        profile_context: context.describe(),
        sample_count: 0,
        upper_peak_rss_bytes: None,
        upper_disk_growth_bytes: None,
        upper_duration_ms: None,
        prior,
        headroom_percent: HEADROOM_PERCENT,
        fallback: Some(ReservationFallback::CallerDeclared),
        memory_kill: None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResourceProfile {
    sample_count: u64,
    upper_peak_rss_bytes: Option<u64>,
    upper_disk_delta_bytes: Option<u64>,
    upper_duration_ms: Option<u64>,
    /// The out-of-memory kills this context has recorded, kept apart from the
    /// peak because the saturation quarantine must never read them.
    memory_kill: Option<MemoryKill>,
}

/// The retained window a duration prediction is computed from, plus how much
/// history stands behind it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DurationProfile {
    sample_count: u64,
    updated_at_unix_ms: u64,
    /// Oldest first, bounded by [`DURATION_SAMPLE_WINDOW`].
    recent_ms: Vec<u64>,
}

/// What a duration lookup needs beyond the profile key: which class stands in
/// when nothing is learned, what state the machine is in, and the instant
/// staleness is judged against.
#[derive(Clone, Copy)]
pub(super) struct DurationContext {
    pub class: CellCommandClass,
    pub warmth: ExecutionWarmth,
    pub now_unix_ms: u64,
}

/// How one resolution came to its number, in the shape every caller reads.
fn profile_rationale(
    identity: Option<&CommandResourceIdentity>,
    context: &ProfileContext,
    prior: &ResourceReservation,
    profile: Option<&ResourceProfile>,
    fallback: Option<ReservationFallback>,
) -> ReservationRationale {
    ReservationRationale {
        // A learned profile speaks for memory, disk, and duration only. Any
        // declared concurrency is re-applied over this by the caller, which
        // is also what records it here.
        declared_concurrency_units: None,
        profile_key: identity.map(|identity| identity.key.clone()),
        profile_context: context.describe(),
        sample_count: profile.map_or(0, |profile| profile.sample_count),
        upper_peak_rss_bytes: profile.and_then(|profile| profile.upper_peak_rss_bytes),
        upper_disk_growth_bytes: profile.and_then(|profile| profile.upper_disk_delta_bytes),
        upper_duration_ms: profile.and_then(|profile| profile.upper_duration_ms),
        prior: prior.clone(),
        headroom_percent: HEADROOM_PERCENT,
        fallback,
        memory_kill: profile.and_then(|profile| profile.memory_kill),
    }
}

pub(super) async fn resolve_reservation(
    db: Arc<LocalDb>,
    identity: Option<&CommandResourceIdentity>,
    context: &ProfileContext,
    prior: ResourceReservation,
    memory_budget_bytes: Option<u64>,
    duration_context: DurationContext,
) -> ResolvedResourceProfile {
    let duration = resolve_duration(
        db.clone(),
        identity,
        context,
        duration_context.warmth,
        duration_context.class,
        duration_context.now_unix_ms,
    )
    .await;
    let Some(identity) = identity else {
        return ResolvedResourceProfile {
            reservation: prior.clone(),
            learned_estimate: None,
            rationale: profile_rationale(
                None,
                context,
                &prior,
                None,
                Some(ReservationFallback::NoCommandIdentity),
            ),
            duration,
            item_durations: Vec::new(),
            memory_fit: MemoryFit::Unknown,
            process_evidence: ProcessEvidence::default(),
        };
    };
    // A store that could not be read and a work class that has never run are
    // both "no learned number", and they call for different responses: one is a
    // cold start, the other is a fault worth seeing on the decision record.
    let local = load_profile(db.clone(), identity, context).await;
    // The fleet is consulted only where this machine cannot speak for its own
    // memory, which is the only place a borrowed number could change an
    // answer: it is cold, thinly evidenced, or its peak was quarantined as
    // saturation. The last is the machine too small for the command: its tree
    // grew until it filled the machine, so the quarantine rightly discards the
    // peak as demand, and without the fleet's measurement the work fell back to
    // its class prior and an unknown fit, which is how a whole-suite pass
    // measured at 20+ GiB elsewhere kept landing on a 12 GiB host. A machine
    // that has measured this command past the confidence floor, under its own
    // budget, speaks for itself and pays for no extra query.
    let cold_here = !matches!(
        &local,
        Ok(Some(profile)) if profile.sample_count >= MIN_CONFIDENT_SAMPLES
            && !profile
                .upper_peak_rss_bytes
                .is_some_and(|peak| memory_peak_quarantined(peak, memory_budget_bytes))
    );
    let fleet_evidence = match cold_here {
        false => None,
        true => load_fleet_evidence(db, identity, context).await.ok(),
    };
    let borrowed = fleet_evidence
        .as_ref()
        .and_then(|evidence| fleet_prior(evidence, &prior, memory_budget_bytes));
    // A borrowed process-tree peak prices the work here and never decides its
    // fit. A tree's total grows with the build width the machine that measured
    // it granted, so a peak taken on a 32-core machine lands at about 70% of
    // any large machine's size and refused both Sparks a lane that peaked at
    // 15 GiB (cairn/6089). Whether this machine can hold the work at all is
    // the largest single process's question, which placement asks of
    // `process_evidence`; the tree's fit here speaks only for a peak this
    // machine measured itself, at the width it grants.
    let fit_for = |demand: Option<u64>| MemoryFit::of(demand, memory_budget_bytes);
    // The prior is settled before anything explains itself, so every rationale
    // below reports the number this resolution actually stood on.
    let prior = borrowed.clone().unwrap_or(prior);
    let rationale = |fallback: Option<ReservationFallback>, profile: Option<&ResourceProfile>| {
        profile_rationale(Some(identity), context, &prior, profile, fallback)
    };
    // A borrowed prior explains the number whenever it is the number. Saying
    // only "no profile recorded" here would describe the absence and hide the
    // evidence that filled it.
    let cold_fallback = |otherwise| match borrowed.is_some() {
        true => ReservationFallback::FleetPrior,
        false => otherwise,
    };
    let profile = match local {
        Ok(Some(profile)) => profile,
        Ok(None) => {
            return ResolvedResourceProfile {
                reservation: prior.clone(),
                learned_estimate: None,
                rationale: rationale(
                    Some(cold_fallback(ReservationFallback::NoProfileRecorded)),
                    None,
                ),
                duration,
                item_durations: Vec::new(),
                memory_fit: MemoryFit::Unknown,
                process_evidence: ProcessEvidence::default(),
            }
        }
        Err(_) => {
            return ResolvedResourceProfile {
                reservation: prior.clone(),
                learned_estimate: None,
                rationale: rationale(Some(ReservationFallback::ProfileLookupFailed), None),
                duration,
                item_durations: Vec::new(),
                memory_fit: MemoryFit::Unknown,
                process_evidence: ProcessEvidence::default(),
            }
        }
    };
    let below_floor = profile.sample_count < MIN_CONFIDENT_SAMPLES;
    let memory_quarantined = profile
        .upper_peak_rss_bytes
        .is_some_and(|peak| memory_peak_quarantined(peak, memory_budget_bytes));
    let mut reservation = reservation_for_profile(
        &profile,
        prior.clone(),
        memory_budget_bytes,
        memory_quarantined,
    );
    // This machine's own tree peak, never a borrowed one and never one the
    // saturation quarantine discarded: what admission charges a running
    // command until its tree has grown into it.
    reservation.measured_peak_bytes = measured_memory_demand(&profile, memory_quarantined);
    let resolved = ResolvedResourceProfile {
        reservation: reservation.clone(),
        learned_estimate: Some(cairn_common::executor_protocol::LearnedResourceEstimate {
            sample_count: profile.sample_count,
            upper_duration_ms: profile.upper_duration_ms,
            upper_peak_rss_bytes: profile.upper_peak_rss_bytes,
            upper_disk_growth_bytes: profile.upper_disk_delta_bytes,
        }),
        rationale: rationale(
            if memory_quarantined {
                Some(ReservationFallback::MemoryObservationQuarantined)
            } else if below_floor {
                // Thin local evidence and a borrowed prior both reach this
                // branch, and which one set the number is the useful fact.
                Some(match reservation.source {
                    ResourceReservationSource::Learned => ReservationFallback::BelowConfidenceFloor,
                    _ => cold_fallback(ReservationFallback::BelowConfidenceFloor),
                })
            } else {
                None
            },
            Some(&profile),
        ),
        duration,
        item_durations: Vec::new(),
        // This machine's own measurement only. A quarantined own peak measured
        // this machine's saturation, so it says nothing about fit and leaves
        // it unknown.
        memory_fit: fit_for(measured_memory_demand(&profile, memory_quarantined)),
        process_evidence: ProcessEvidence::default(),
    };
    floored_by_kill(resolved, profile.memory_kill, memory_budget_bytes)
}

/// What a kill proved this command needs on this machine: the victim's
/// resident size at death, or the whole machine when the command was killed
/// while nothing else of Cairn's ran beside it.
fn kill_floor(kill: &MemoryKill) -> u64 {
    kill.victim_resident_bytes
        .max(kill.alone_on_machine_bytes.unwrap_or(0))
}

/// Raise a resolution to what an out-of-memory kill on this machine proved.
///
/// The commands that get killed are exactly the ones whose process-tree peak
/// nears the machine's budget, so the saturation quarantine discards that peak
/// and, without this, the work is charged its class prior: a 2 GiB claim for a
/// compile that has already died at 3.4 GiB fits a machine with 2.5 GiB free.
/// A victim's own resident size is one process's number, which nothing beside
/// it can inflate, so the floor is applied after the quarantine and never
/// through it.
///
/// A floor lowers nothing and never claims a fit: it is a lower bound, so one
/// under the cap leaves `memory_fit` as the rest of the evidence set it, and
/// one over the cap is a ceiling that names the kill. The reservation it sets
/// stays `Unmeasured`, because the killed run never reached its peak; that is
/// also what makes the executor hold the claim against its available memory
/// while the work is resident, which is the brake a second compile beside it
/// needs.
fn floored_by_kill(
    mut resolved: ResolvedResourceProfile,
    kill: Option<MemoryKill>,
    memory_budget_bytes: Option<u64>,
) -> ResolvedResourceProfile {
    let Some(kill) = kill else {
        return resolved;
    };
    let floor = kill_floor(&kill);
    let charged =
        memory_budget_bytes.map_or(floor, |budget| floor.min(memory_reservation_cap(budget)));
    if charged > resolved.reservation.memory_bytes {
        resolved.reservation.memory_bytes = charged;
        resolved.reservation.source = ResourceReservationSource::Unmeasured;
    }
    if let MemoryFit::Exceeds(ceiling) = MemoryFit::of(Some(floor), memory_budget_bytes) {
        let measured_past_it = matches!(
            resolved.memory_fit,
            MemoryFit::Exceeds(held) if held.estimate_bytes > floor
        );
        if !measured_past_it {
            resolved.memory_fit = MemoryFit::Exceeds(MemoryCeiling {
                killed_here: Some(kill),
                ..ceiling
            });
        }
    }
    resolved
}

/// What one completed execution may teach the learners.
///
/// A profile is keyed by the configured lane, not by what the lane did on a
/// given run, so the run's outcome is the only thing that can say whether its
/// measurements describe that work class. Deciding it once, here, is what keeps
/// every learner taking the same view of the same run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CompletionKind {
    /// Exited 0 and did the work its identity names: teaches everything.
    Succeeded,
    /// Non-zero exit, kill, or timeout. Its peak memory and disk growth were
    /// real demand, but its duration measures how fast this machine fails.
    Failed,
    /// Exited 0 having declared an empty selection: says nothing about the
    /// work class, including its memory.
    SelectedNothing,
}

impl CompletionKind {
    /// Classify a completion from whether it succeeded and what it printed.
    ///
    /// Failure wins over an empty selection: a nextest run that exits 4 because
    /// its filterset matched nothing still compiled what it selected, and that
    /// memory demand is worth keeping. The empty-selection signal is the
    /// runner's structured result and nothing else, so a run that merely prints
    /// "nothing to run" without declaring it still counts as work.
    pub(super) fn classify(succeeded: bool, output: &str) -> Self {
        if !succeeded {
            Self::Failed
        } else if crate::execution::check_parsers::declared_empty_selection(output) {
            Self::SelectedNothing
        } else {
            Self::Succeeded
        }
    }
}

/// Fold one real completion into what this machine has learned.
///
/// The only writer, and it guards against two kinds of contamination. A reused
/// check-result observation must never reach it, because its duration belongs to
/// the execution that originally produced it, on whatever machine that was;
/// recording it would teach the fleet that a cache hit is how fast that machine
/// runs the suite. And a run that failed or selected nothing must not teach the
/// predictor, because a lane that answered in 250 ms by testing nothing, or died
/// four seconds in, is not a sample of how long the lane takes. Mixing either in
/// gives a window two modes, and its median then lands in whichever mode happens
/// to hold the majority. The warmth rule below is the same principle applied to
/// the machine's state rather than to the run's outcome.
pub(super) async fn observe_completed(
    db: Arc<LocalDb>,
    identity: Option<&CommandResourceIdentity>,
    context: &ProfileContext,
    metadata: &CellExecutionMeta,
    kind: CompletionKind,
) {
    let Some(identity) = identity else { return };
    // A no-op is the most frequent lane on every machine. Recorded, each one
    // would also decay the memory high-water mark toward the footprint of a
    // script that only decided to do nothing.
    if kind == CompletionKind::SelectedNothing {
        return;
    }
    let Some(quality) = metadata.measurement_quality.as_ref() else {
        return;
    };
    let duration = (kind == CompletionKind::Succeeded
        && quality.duration != MeasurementQuality::Unavailable)
        .then_some(metadata.duration_ms)
        .flatten();
    let memory = (quality.memory != MeasurementQuality::Unavailable)
        .then_some(metadata.peak_rss_bytes)
        .flatten();
    let disk = (quality.disk != MeasurementQuality::Unavailable)
        .then_some(metadata.disk_delta_bytes)
        .flatten();
    if duration.is_none() && memory.is_none() && disk.is_none() {
        return;
    }
    // A duration only becomes evidence about speed once the executor says what
    // state the machine was in when it ran. An execution that makes no warmth
    // claim is still charged against the reservation profile — memory demand does
    // not depend on knowing why — but it teaches the predictor nothing, because
    // filing it under a guessed warmth is how warm and cold runs contaminate each
    // other.
    //
    // Speed is learned with the host's stall divided out, by the reading the
    // run began under: a baseline and a contention curve describe the machine
    // and its co-resident work, and the stall is priced separately from the
    // reading current when work is placed. Learned raw, a stalled run would be
    // charged twice, once inside the curve and again by the price.
    if let (Some(duration), Some(warmth), Some(load)) =
        (duration, metadata.warmth, metadata.load_context.as_ref())
    {
        if let Some(duration) = StartingStall::of(load).destall(duration) {
            learn_speed(
                db.clone(),
                identity,
                context,
                warmth,
                metadata,
                load,
                duration,
            )
            .await;
        }
    }
    let memory_bound = kind == CompletionKind::Succeeded
        && metadata
            .allocation
            .as_ref()
            .and_then(|allocation| allocation.build_jobs)
            .is_some_and(|jobs| jobs.memory_bound());
    let _ = update_profile_with(
        db,
        identity,
        context,
        metadata.finished_at_unix_ms,
        memory,
        disk,
        // The reservation's duration high-water mark stays the run as it
        // came: it covers the worst case seen, stall included.
        duration,
        // A success here is the machine holding the command, which is the
        // one thing that disproves "it cannot run here at all".
        kind == CompletionKind::Succeeded,
        match memory_bound {
            true => MEMORY_BOUND_PEAK_DECAY_DIVISOR,
            false => PEAK_DECAY_DIVISOR,
        },
        // The largest single process rises and never decays: a warm run
        // says nothing about the cold compile, and nothing beside a process
        // inflates its working set.
        metadata.peak_process.as_ref(),
    )
    .await;
}

/// Fold one de-stalled duration into the machine's baseline, when it ran
/// alone, or into its contention curve, when it ran beside work and the
/// baseline is already known.
async fn learn_speed(
    db: Arc<LocalDb>,
    identity: &CommandResourceIdentity,
    context: &ProfileContext,
    warmth: ExecutionWarmth,
    metadata: &CellExecutionMeta,
    load: &ExecutionLoadContext,
    duration: u64,
) {
    let finished = metadata.finished_at_unix_ms;
    if teaches_baseline(load) {
        let _ = record_duration(db.clone(), identity, context, warmth, finished, duration).await;
        let _ = record_contention(db, context, load, finished, 1_000).await;
        return;
    }
    let baseline = resolve_duration(
        db.clone(),
        Some(identity),
        context,
        warmth,
        CellCommandClass::Other,
        finished,
    )
    .await;
    if baseline.is_learned() && baseline.predicted_ms > 0 {
        let multiplier = duration.saturating_mul(1_000) / baseline.predicted_ms;
        let _ = record_contention(db, context, load, finished, multiplier.max(1_000)).await;
    }
}

/// Record an out-of-memory kill of `identity` on this machine context.
///
/// [`observe_completed`] still folds the killed run's process-tree peak into
/// the profile, and for a killed command that peak is the one the saturation
/// quarantine discards. The victim's own resident size is kept beside it as a
/// floor ([`floored_by_kill`]) so the kill reaches the next placement of the
/// same command here.
///
/// A kill while nothing else of Cairn's ran on the machine also records the
/// machine's size: it is the machine's answer that it cannot run the command,
/// not a crowding fact, and the floor it implies is a ceiling placement routes
/// around until a run succeeds here or the machine grows past it. The load
/// context is read at the command's start, so work admitted beside it later is
/// invisible here; `shared_with_siblings` is what the caller knows that the
/// load context does not, that a parallel batch's items ran beside each other.
pub(super) async fn observe_memory_kill(
    db: Arc<LocalDb>,
    identity: Option<&CommandResourceIdentity>,
    context: &ProfileContext,
    metadata: &CellExecutionMeta,
    shared_with_siblings: bool,
    memory_budget_bytes: Option<u64>,
) {
    let Some(identity) = identity else { return };
    let Some(CommandStop::OomKilled {
        rss_bytes: Some(victim_resident_bytes),
        machine_total_bytes,
        ..
    }) = &metadata.stop
    else {
        return;
    };
    let alone = !shared_with_siblings
        && metadata.load_context.as_ref().is_some_and(|load| {
            load.co_resident_compile_jobs == 0 && load.co_resident_light_jobs == 0
        });
    let alone_on_machine_bytes = alone
        .then_some(machine_total_bytes.or(memory_budget_bytes))
        .flatten();
    let _ = record_memory_kill(
        db,
        identity,
        context,
        MemoryKill {
            victim_resident_bytes: *victim_resident_bytes,
            killed_at_unix_ms: metadata.finished_at_unix_ms,
            alone_on_machine_bytes,
        },
    )
    .await;
}

/// Fold one kill into a context's record, creating the row if the context has
/// never completed the command. The victim floor only ever rises; a kill alone
/// overwrites the machine size, and a crowded one leaves an earlier solo kill
/// standing.
async fn record_memory_kill(
    db: Arc<LocalDb>,
    identity: &CommandResourceIdentity,
    context: &ProfileContext,
    kill: MemoryKill,
) -> Result<(), String> {
    let identity = identity.clone();
    let context = context.clone();
    db.write(move |conn| {
        let identity = identity.clone();
        let context = context.clone();
        Box::pin(async move {
            conn.execute(
                r#"INSERT INTO command_resource_profiles (identity_version, command_identity, executor_class, os, arch, toolchain_fingerprint, sample_count, updated_at_unix_ms, confidence_millis, oom_victim_rss_bytes, oom_killed_at_unix_ms, oom_alone_machine_bytes)
                   VALUES (?1,?2,?3,?4,?5,?6,0,?7,0,?8,?7,?9)
                   ON CONFLICT(identity_version, command_identity, executor_class, os, arch, toolchain_fingerprint) DO UPDATE SET
                     updated_at_unix_ms=MAX(command_resource_profiles.updated_at_unix_ms, excluded.updated_at_unix_ms),
                     oom_victim_rss_bytes=MAX(COALESCE(command_resource_profiles.oom_victim_rss_bytes, 0), excluded.oom_victim_rss_bytes),
                     oom_killed_at_unix_ms=MAX(COALESCE(command_resource_profiles.oom_killed_at_unix_ms, 0), excluded.oom_killed_at_unix_ms),
                     oom_alone_machine_bytes=COALESCE(excluded.oom_alone_machine_bytes, command_resource_profiles.oom_alone_machine_bytes)"#,
                params![
                    identity.version as i64,
                    identity.key,
                    context.executor_class,
                    context.os,
                    context.arch,
                    context.toolchain_fingerprint,
                    kill.killed_at_unix_ms as i64,
                    kill.victim_resident_bytes as i64,
                    kill.alone_on_machine_bytes.map(|bytes| bytes as i64)
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The memory demand this machine's own profile measured, as a reservation
/// charges it before the machine's cap: the raw high-water mark while evidence
/// is thin, with headroom once it is confident, and nothing at all for a
/// quarantined peak.
fn measured_memory_demand(profile: &ResourceProfile, memory_quarantined: bool) -> Option<u64> {
    if memory_quarantined {
        return None;
    }
    let peak = profile.upper_peak_rss_bytes?;
    Some(match profile.sample_count < MIN_CONFIDENT_SAMPLES {
        true => peak,
        false => with_headroom(peak),
    })
}

fn reservation_for_profile(
    profile: &ResourceProfile,
    prior: ResourceReservation,
    memory_budget_bytes: Option<u64>,
    memory_quarantined: bool,
) -> ResourceReservation {
    let measured_memory = measured_memory_demand(profile, memory_quarantined);
    let learned_disk = profile.upper_disk_delta_bytes.map(with_headroom);
    let (memory_bytes, disk_growth_bytes) = if profile.sample_count < MIN_CONFIDENT_SAMPLES {
        // Thin evidence cannot justify predictive headroom, but it is still a
        // measured high-water mark and admission must not ignore it. Preserve
        // the larger of the prior and the raw observation until enough samples
        // exist to apply the learned estimate with headroom.
        (
            measured_memory.map_or(prior.memory_bytes, |value| prior.memory_bytes.max(value)),
            profile
                .upper_disk_delta_bytes
                .map_or(prior.disk_growth_bytes, |value| {
                    prior.disk_growth_bytes.max(value)
                }),
        )
    } else {
        (
            measured_memory.unwrap_or(prior.memory_bytes),
            learned_disk.unwrap_or(prior.disk_growth_bytes),
        )
    };
    // The cap is what keeps work measured on a big host from claiming a small
    // one whole. It hides that it clamped, which `memory_fit` states.
    let memory_bytes = memory_budget_bytes.map_or(memory_bytes, |budget| {
        memory_bytes.min(memory_reservation_cap(budget))
    });
    ResourceReservation {
        memory_bytes,
        disk_growth_bytes,
        concurrency_units: prior.concurrency_units,
        // Measured only when a measurement is what set the memory number. Below
        // the confidence floor, and whenever a peak is quarantined, the prior
        // can be the larger of the two and hold the reservation on its own — and
        // a prior returned as `Learned` is a guess wearing evidence's clothes.
        // Downstream this is load-bearing rather than cosmetic: the executor
        // holds resident UNMEASURED claims against its available-memory floor,
        // so mislabeling turns that brake off for exactly the work it exists for.
        source: match memory_bytes > prior.memory_bytes {
            true => ResourceReservationSource::Learned,
            false => prior.source,
        },
        measured_peak_bytes: None,
    }
}

/// Seed one context's memory profile, for placement tests outside this module.
/// [`observe_completed`] stays the only production writer.
#[cfg(test)]
pub(super) async fn seed_memory_profile_for_test(
    db: Arc<LocalDb>,
    identity: &CommandResourceIdentity,
    context: &ProfileContext,
    peak_rss_bytes: u64,
) {
    for finished in 1..=MIN_CONFIDENT_SAMPLES {
        update_profile(
            db.clone(),
            identity,
            context,
            finished,
            Some(peak_rss_bytes),
            Some(1024 * 1024),
            Some(1_000),
        )
        .await
        .unwrap();
    }
}

/// A completion the kernel's out-of-memory killer ended, for tests outside this
/// module: `victim_resident_bytes` is the killed process's size, and
/// `co_resident_compile_jobs` is what else of Cairn's was compiling there when
/// it started.
#[cfg(test)]
pub(super) fn oom_killed_for_test(
    victim_resident_bytes: u64,
    machine_total_bytes: Option<u64>,
    co_resident_compile_jobs: u32,
    finished_at_unix_ms: u64,
) -> CellExecutionMeta {
    CellExecutionMeta {
        warmth: Some(ExecutionWarmth::PreparedWarmSlot),
        load_context: Some(ExecutionLoadContext {
            co_resident_compile_jobs,
            ..ExecutionLoadContext::default()
        }),
        environment_fingerprint: String::new(),
        executor_id: "executor".into(),
        executor_device_id: "device".into(),
        executor_connection_generation: 1,
        cell_id: "slot".into(),
        cell_epoch: 1,
        started_at_unix_ms: finished_at_unix_ms.saturating_sub(1_263_640),
        finished_at_unix_ms,
        duration_ms: Some(1_263_640),
        peak_rss_bytes: Some(victim_resident_bytes),
        peak_physical_footprint_bytes: None,
        peak_process: None,
        cpu: None,
        allocation: None,
        disk_delta_bytes: None,
        measurement_quality: Some(
            cairn_common::executor_protocol::ExecutionMeasurementQuality {
                duration: MeasurementQuality::Authoritative,
                memory: MeasurementQuality::Sampled,
                disk: MeasurementQuality::Unavailable,
                memory_platform: Some("linux".into()),
                disk_boundary: "cell".into(),
            },
        ),
        verdict_platform: None,
        verdict_arch: None,
        verdict_environment_hash: None,
        sandbox: None,
        toolchain_fingerprint: None,
        containment: None,
        stop: Some(CommandStop::OomKilled {
            pid: 1_940_747,
            rss_bytes: Some(victim_resident_bytes),
            machine_total_bytes,
            process_name: None,
        }),
        cell_origin: None,
    }
}

fn with_headroom(value: u64) -> u64 {
    cairn_common::executor_protocol::with_measured_headroom(value)
}

#[cfg(test)]
fn update_upper(previous: u64, sample: u64) -> u64 {
    if previous == 0 || sample >= previous {
        sample
    } else {
        // Slowly decay a high-water estimate while remaining biased upward.
        previous.saturating_sub((previous - sample) / PEAK_DECAY_DIVISOR)
    }
}

async fn load_profile(
    db: Arc<LocalDb>,
    identity: &CommandResourceIdentity,
    context: &ProfileContext,
) -> Result<Option<ResourceProfile>, String> {
    let identity = identity.clone();
    let context = context.clone();
    db.read(|conn| {
        let identity = identity.clone();
        let context = context.clone();
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT sample_count, upper_peak_rss_bytes, upper_disk_delta_bytes, upper_duration_ms,
                        oom_victim_rss_bytes, oom_killed_at_unix_ms, oom_alone_machine_bytes
                 FROM command_resource_profiles
                 WHERE identity_version=?1 AND command_identity=?2 AND executor_class=?3
                   AND os=?4 AND arch=?5 AND toolchain_fingerprint=?6",
                params![identity.version as i64, identity.key, context.executor_class,
                    context.os, context.arch, context.toolchain_fingerprint],
            ).await?;
            match rows.next().await? {
                Some(row) => Ok(Some(ResourceProfile {
                    sample_count: row.get::<i64>(0)? as u64,
                    upper_peak_rss_bytes: row.get::<Option<i64>>(1)?.map(|value| value as u64),
                    upper_disk_delta_bytes: row.get::<Option<i64>>(2)?.map(|value| value as u64),
                    upper_duration_ms: row.get::<Option<i64>>(3)?.map(|value| value as u64),
                    memory_kill: match (row.get::<Option<i64>>(4)?, row.get::<Option<i64>>(5)?) {
                        (Some(victim), Some(killed_at)) => Some(MemoryKill {
                            victim_resident_bytes: victim as u64,
                            killed_at_unix_ms: killed_at as u64,
                            alone_on_machine_bytes: row
                                .get::<Option<i64>>(6)?
                                .map(|value| value as u64),
                        }),
                        _ => None,
                    },
                })),
                None => Ok(None),
            }
        })
    }).await.map_err(|error| error.to_string())
}

/// Every OTHER machine context's high-water marks for one command identity.
///
/// The exclusion is the whole point: this answers "what does this command cost
/// elsewhere", and a machine's own row is not elsewhere. A context is excluded
/// only on an exact match of all four keys, so a machine that was re-toolchained
/// contributes its own pre-toolchain history — which is the strongest evidence
/// any prior could have.
///
/// Only contexts past the confidence floor contribute. A single observation
/// elsewhere is as thin as a single observation here, and borrowing it would
/// spread one machine's bad afternoon across the fleet.
async fn load_fleet_evidence(
    db: Arc<LocalDb>,
    identity: &CommandResourceIdentity,
    context: &ProfileContext,
) -> Result<FleetEvidence, String> {
    let identity = identity.clone();
    let context = context.clone();
    db.read(|conn| {
        let identity = identity.clone();
        let context = context.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT upper_peak_rss_bytes, upper_disk_delta_bytes
                 FROM command_resource_profiles
                 WHERE identity_version=?1 AND command_identity=?2 AND sample_count>=?3
                   AND NOT (executor_class=?4 AND os=?5 AND arch=?6 AND toolchain_fingerprint=?7)",
                    params![
                        identity.version as i64,
                        identity.key,
                        MIN_CONFIDENT_SAMPLES as i64,
                        context.executor_class,
                        context.os,
                        context.arch,
                        context.toolchain_fingerprint
                    ],
                )
                .await?;
            let mut evidence = FleetEvidence::default();
            while let Some(row) = rows.next().await? {
                if let Some(peak) = row.get::<Option<i64>>(0)? {
                    evidence.peaks.push(peak as u64);
                }
                if let Some(disk) = row.get::<Option<i64>>(1)? {
                    evidence.disks.push(disk as u64);
                }
            }
            Ok(evidence)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_duration_profile(
    db: Arc<LocalDb>,
    identity: &CommandResourceIdentity,
    context: &ProfileContext,
    warmth: ExecutionWarmth,
) -> Result<Option<DurationProfile>, String> {
    let identity = identity.clone();
    let context = context.clone();
    db.read(|conn| {
        let identity = identity.clone();
        let context = context.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT sample_count, updated_at_unix_ms, recent_duration_ms
                 FROM command_duration_profiles
                 WHERE identity_version=?1 AND command_identity=?2 AND executor_class=?3
                   AND os=?4 AND arch=?5 AND toolchain_fingerprint=?6 AND warmth=?7",
                    params![
                        identity.version as i64,
                        identity.key,
                        context.executor_class,
                        context.os,
                        context.arch,
                        context.toolchain_fingerprint,
                        warmth.as_str()
                    ],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(DurationProfile {
                    sample_count: row.get::<i64>(0)? as u64,
                    updated_at_unix_ms: row.get::<i64>(1)? as u64,
                    recent_ms: decode_window(&row.get::<String>(2)?),
                })),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// A window that cannot be decoded is an empty window, not a zero-length run.
/// The caller reads too few samples and falls back to the labeled prior, which
/// is the honest response to a store that has stopped making sense.
fn decode_window(encoded: &str) -> Vec<u64> {
    serde_json::from_str::<Vec<u64>>(encoded).unwrap_or_default()
}

/// Append one observation to the retained window, evicting the oldest.
///
/// Read-modify-write inside the transaction rather than in SQL: the window is a
/// bounded ordered list, and expressing "append, then drop the head if it grew
/// past the bound" in portable SQL costs more than it buys. `LocalDb::write`
/// re-runs this closure when a concurrent writer conflicts, so the read and the
/// write it derives always describe the same snapshot.
async fn record_duration(
    db: Arc<LocalDb>,
    identity: &CommandResourceIdentity,
    context: &ProfileContext,
    warmth: ExecutionWarmth,
    finished: u64,
    duration_ms: u64,
) -> Result<(), String> {
    let identity = identity.clone();
    let context = context.clone();
    db.write(move |conn| {
        let identity = identity.clone();
        let context = context.clone();
        Box::pin(async move {
            let key = params![identity.version as i64, identity.key.clone(), context.executor_class.clone(),
                context.os.clone(), context.arch.clone(), context.toolchain_fingerprint.clone(), warmth.as_str()];
            let mut rows = conn.query(
                "SELECT sample_count, updated_at_unix_ms, recent_duration_ms
                 FROM command_duration_profiles
                 WHERE identity_version=?1 AND command_identity=?2 AND executor_class=?3
                   AND os=?4 AND arch=?5 AND toolchain_fingerprint=?6 AND warmth=?7",
                key,
            ).await?;
            let existing = rows.next().await?;
            let (sample_count, newest, mut window) = match existing {
                Some(row) => (
                    row.get::<i64>(0)? as u64,
                    row.get::<i64>(1)? as u64,
                    decode_window(&row.get::<String>(2)?),
                ),
                None => (0, 0, Vec::new()),
            };
            window.push(duration_ms);
            while window.len() > DURATION_SAMPLE_WINDOW {
                window.remove(0);
            }
            let encoded = serde_json::to_string(&window).unwrap_or_else(|_| "[]".into());
            conn.execute(
                "INSERT INTO command_duration_profiles (identity_version, command_identity, executor_class, os, arch, toolchain_fingerprint, warmth, sample_count, updated_at_unix_ms, recent_duration_ms)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
                 ON CONFLICT(identity_version, command_identity, executor_class, os, arch, toolchain_fingerprint, warmth) DO UPDATE SET
                   sample_count=excluded.sample_count,
                   updated_at_unix_ms=excluded.updated_at_unix_ms,
                   recent_duration_ms=excluded.recent_duration_ms",
                params![identity.version as i64, identity.key.clone(), context.executor_class.clone(),
                    context.os.clone(), context.arch.clone(), context.toolchain_fingerprint.clone(), warmth.as_str(),
                    sample_count.saturating_add(1).min(10_000) as i64,
                    newest.max(finished) as i64,
                    encoded],
            ).await?;
            Ok(())
        })
    }).await.map_err(|error| error.to_string())
}

#[cfg(test)]
async fn update_profile(
    db: Arc<LocalDb>,
    identity: &CommandResourceIdentity,
    context: &ProfileContext,
    finished: u64,
    memory: Option<u64>,
    disk: Option<u64>,
    duration: Option<u64>,
) -> Result<(), String> {
    update_profile_with(
        db,
        identity,
        context,
        finished,
        memory,
        disk,
        duration,
        false,
        PEAK_DECAY_DIVISOR,
        None,
    )
    .await
}

/// [`update_profile`], also clearing a solo out-of-memory kill when this
/// completion `held` the command, that is, succeeded on this machine, and
/// pulling a higher peak down by `peak_decay_divisor` of the gap.
#[allow(clippy::too_many_arguments)]
async fn update_profile_with(
    db: Arc<LocalDb>,
    identity: &CommandResourceIdentity,
    context: &ProfileContext,
    finished: u64,
    memory: Option<u64>,
    disk: Option<u64>,
    duration: Option<u64>,
    held: bool,
    peak_decay_divisor: u64,
    peak_process: Option<&cairn_common::executor_protocol::PeakProcess>,
) -> Result<(), String> {
    let identity = identity.clone();
    let context = context.clone();
    let process_bytes = peak_process.map(|peak| peak.bytes as i64);
    let process_name = peak_process.and_then(|peak| peak.name.clone());
    db.write(|conn| {
        let identity = identity.clone();
        let context = context.clone();
        let process_name = process_name.clone();
        Box::pin(async move {
            conn.execute(
                r#"INSERT INTO command_resource_profiles (identity_version, command_identity, executor_class, os, arch, toolchain_fingerprint, sample_count, updated_at_unix_ms, upper_peak_rss_bytes, upper_disk_delta_bytes, upper_duration_ms, confidence_millis, upper_process_peak_bytes, process_peak_name)
                   VALUES (?1,?2,?3,?4,?5,?6,1,?7,?8,?9,?10,200,?13,?14)
                   ON CONFLICT(identity_version, command_identity, executor_class, os, arch, toolchain_fingerprint) DO UPDATE SET
                     sample_count=MIN(command_resource_profiles.sample_count + 1, 10000),
                     updated_at_unix_ms=MAX(command_resource_profiles.updated_at_unix_ms, excluded.updated_at_unix_ms),
                     upper_peak_rss_bytes=CASE WHEN excluded.upper_peak_rss_bytes IS NULL THEN command_resource_profiles.upper_peak_rss_bytes WHEN command_resource_profiles.upper_peak_rss_bytes IS NULL OR excluded.upper_peak_rss_bytes >= command_resource_profiles.upper_peak_rss_bytes THEN excluded.upper_peak_rss_bytes ELSE command_resource_profiles.upper_peak_rss_bytes - ((command_resource_profiles.upper_peak_rss_bytes - excluded.upper_peak_rss_bytes) / ?12) END,
                     upper_disk_delta_bytes=CASE WHEN excluded.upper_disk_delta_bytes IS NULL THEN command_resource_profiles.upper_disk_delta_bytes WHEN command_resource_profiles.upper_disk_delta_bytes IS NULL OR excluded.upper_disk_delta_bytes >= command_resource_profiles.upper_disk_delta_bytes THEN excluded.upper_disk_delta_bytes ELSE command_resource_profiles.upper_disk_delta_bytes - ((command_resource_profiles.upper_disk_delta_bytes - excluded.upper_disk_delta_bytes) / 32) END,
                     upper_duration_ms=CASE WHEN excluded.upper_duration_ms IS NULL THEN command_resource_profiles.upper_duration_ms WHEN command_resource_profiles.upper_duration_ms IS NULL OR excluded.upper_duration_ms >= command_resource_profiles.upper_duration_ms THEN excluded.upper_duration_ms ELSE command_resource_profiles.upper_duration_ms - ((command_resource_profiles.upper_duration_ms - excluded.upper_duration_ms) / 32) END,
                     confidence_millis=MIN(1000, (MIN(command_resource_profiles.sample_count + 1, 10000) * 1000) / 5),
                     oom_alone_machine_bytes=CASE WHEN ?11 = 1 THEN NULL ELSE command_resource_profiles.oom_alone_machine_bytes END,
                     upper_process_peak_bytes=CASE WHEN ?13 IS NULL THEN command_resource_profiles.upper_process_peak_bytes WHEN command_resource_profiles.upper_process_peak_bytes IS NULL OR ?13 >= command_resource_profiles.upper_process_peak_bytes THEN ?13 ELSE command_resource_profiles.upper_process_peak_bytes END,
                     process_peak_name=CASE WHEN ?13 IS NOT NULL AND (command_resource_profiles.upper_process_peak_bytes IS NULL OR ?13 >= command_resource_profiles.upper_process_peak_bytes) THEN COALESCE(?14, command_resource_profiles.process_peak_name) ELSE command_resource_profiles.process_peak_name END"#,
                params![identity.version as i64, identity.key, context.executor_class, context.os, context.arch, context.toolchain_fingerprint, finished as i64, memory.map(|value| value as i64), disk.map(|value| value as i64), duration.map(|value| value as i64), held as i64, peak_decay_divisor.max(1) as i64, process_bytes, process_name],
            ).await?;
            Ok(())
        })
    }).await.map_err(|error| error.to_string())
}

#[cfg(test)]
mod process_peak_tests {
    use super::*;
    use cairn_common::executor_protocol::PeakProcess;

    const GIB: u64 = 1 << 30;

    fn context(executor_class: &str) -> ProfileContext {
        ProfileContext {
            executor_class: executor_class.into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            toolchain_fingerprint: "rust".into(),
        }
    }

    fn identity(key: &str) -> CommandResourceIdentity {
        CommandResourceIdentity {
            version: 1,
            key: key.into(),
            suite: Some("rust-tests".into()),
        }
    }

    fn completed(peak: u64, finished_at_unix_ms: u64) -> CellExecutionMeta {
        CellExecutionMeta {
            stop: None,
            peak_process: Some(PeakProcess {
                bytes: peak,
                name: Some("rustc".into()),
            }),
            ..oom_killed_for_test(GIB, None, 0, finished_at_unix_ms)
        }
    }

    /// A completion teaches its identity and its suite the largest single
    /// process, which only rises: a warm run after the cold compile does not
    /// lower it.
    #[tokio::test]
    async fn the_largest_process_is_learned_and_only_rises() {
        let db = Arc::new(crate::storage::migrated_test_db("process-peak-rises.db").await);
        let fedora = context("fedora:fedora");
        let id = identity("rust-tests-a");
        let suite = suite_key("cairn", Some(&id), CellCommandClass::CargoTest);
        assert_eq!(suite.as_deref(), Some("cairn/rust-tests"));
        for (peak, at) in [(10 * GIB, 1), (4 * GIB, 2)] {
            let meta = completed(peak, at);
            observe_suite(
                db.clone(),
                suite.as_deref(),
                &fedora,
                &meta,
                CompletionKind::Succeeded,
                MachineAdmission::default(),
            )
            .await;
            observe_completed(
                db.clone(),
                Some(&id),
                &fedora,
                &meta,
                CompletionKind::Succeeded,
            )
            .await;
        }
        let demand = resolve_process_demand(
            db.clone(),
            Some(&id),
            suite.as_deref(),
            CellCommandClass::CargoTest,
            &fedora,
        )
        .await
        .demand
        .unwrap();
        assert_eq!(demand.peak_bytes, 10 * GIB);
        assert_eq!(demand.name.as_deref(), Some("rustc"));
        assert_eq!(demand.measured_on.as_deref(), Some("fedora:fedora"));
    }

    /// A tiered lane's never-seen identity is judged on its suite, measured
    /// on another machine of the same platform: the free kill every new
    /// identity used to get on each small machine.
    #[tokio::test]
    async fn a_never_seen_identity_inherits_its_suites_process() {
        let db = Arc::new(crate::storage::migrated_test_db("process-peak-suite.db").await);
        let fedora = context("fedora:fedora");
        let seen = identity("rust-tests-a");
        let suite = suite_key("cairn", Some(&seen), CellCommandClass::CargoTest);
        observe_suite(
            db.clone(),
            suite.as_deref(),
            &fedora,
            &completed(10 * GIB, 1),
            CompletionKind::Succeeded,
            MachineAdmission::default(),
        )
        .await;
        let unseen = identity("rust-tests-b");
        let demand = resolve_process_demand(
            db.clone(),
            Some(&unseen),
            suite.as_deref(),
            CellCommandClass::CargoTest,
            &context("bgsrv-ub-device:bgsrv-ub"),
        )
        .await
        .demand
        .unwrap();
        assert_eq!(demand.peak_bytes, 10 * GIB);
        assert_eq!(demand.suite.as_deref(), Some("rust-tests"));
        assert_eq!(demand.basis, ProcessDemandBasis::ThisPlatform);
        // cairn/6312: another platform's compiler is borrowed rather than
        // leaving this one blind, and says where it came from.
        let mac = ProfileContext {
            os: "macos".into(),
            arch: "aarch64".into(),
            ..fedora
        };
        let borrowed = resolve_process_demand(
            db,
            Some(&unseen),
            suite.as_deref(),
            CellCommandClass::CargoTest,
            &mac,
        )
        .await
        .demand
        .expect("another platform's compiler stands in");
        assert_eq!(borrowed.peak_bytes, 10 * GIB);
        assert_eq!(
            borrowed.basis,
            ProcessDemandBasis::OtherPlatform {
                platform: "linux x86_64".into()
            }
        );
    }

    /// cairn/6312's order of evidence for a platform that has measured
    /// nothing: the identity's largest named peak on any other platform,
    /// then the project's largest rustc as a class prior, and only then
    /// nothing, said as such, and only for a Rust compile.
    #[tokio::test]
    async fn an_unmeasured_platform_borrows_then_falls_to_the_class_prior() {
        let db = Arc::new(crate::storage::migrated_test_db("process-peak-borrow.db").await);
        let ub = context("bgsrv-ub-device:bgsrv-ub");
        let spark = ProfileContext {
            arch: "aarch64".into(),
            ..context("bginf-sp1-device:bginf-sp1")
        };
        let compile = CommandResourceIdentity {
            suite: Some("workspace-compile".into()),
            ..identity("workspace-compile-a")
        };
        let compile_suite = suite_key("cairn", Some(&compile), CellCommandClass::CargoCheck);
        let resolve = |identity: CommandResourceIdentity, class| {
            let db = db.clone();
            let suite = suite_key("cairn", Some(&identity), class);
            let ub = ub.clone();
            async move {
                resolve_process_demand(db, Some(&identity), suite.as_deref(), class, &ub).await
            }
        };

        let nothing = resolve(compile.clone(), CellCommandClass::CargoCheck).await;
        assert_eq!(nothing.demand, None);
        assert!(
            nothing.unmeasured_compile,
            "a Rust compile measured nowhere"
        );
        let vitest = CommandResourceIdentity {
            suite: None,
            ..identity("vitest")
        };
        assert!(
            !resolve(vitest.clone(), CellCommandClass::Vitest)
                .await
                .unmeasured_compile,
            "only a Rust compile is owed a prior"
        );

        // Another suite of the project measured a rustc: the class prior.
        observe_suite(
            db.clone(),
            Some("cairn/rust-tests"),
            &ub,
            &completed(9 * GIB, 1),
            CompletionKind::Succeeded,
            MachineAdmission::default(),
        )
        .await;
        let prior = resolve(compile.clone(), CellCommandClass::CargoCheck)
            .await
            .demand
            .expect("the class prior");
        assert_eq!(prior.peak_bytes, 9 * GIB);
        assert_eq!(prior.suite.as_deref(), Some("rust-tests"));
        assert_eq!(
            prior.basis,
            ProcessDemandBasis::ClassPrior { platform: None }
        );
        assert_eq!(
            resolve(vitest, CellCommandClass::Vitest).await.demand,
            None,
            "a class prior is for Rust compiles"
        );

        // The work itself measured on aarch64 outranks any prior.
        for (id, peak) in [(&compile, 12 * GIB)] {
            observe_completed(
                db.clone(),
                Some(id),
                &spark,
                &completed(peak, 2),
                CompletionKind::Succeeded,
            )
            .await;
        }
        observe_suite(
            db.clone(),
            compile_suite.as_deref(),
            &spark,
            &completed(10 * GIB, 2),
            CompletionKind::Succeeded,
            MachineAdmission::default(),
        )
        .await;
        let borrowed = resolve(compile.clone(), CellCommandClass::CargoCheck)
            .await
            .demand
            .expect("borrowed from aarch64");
        assert_eq!(borrowed.peak_bytes, 12 * GIB, "the largest on any platform");
        assert_eq!(
            borrowed.measured_on.as_deref(),
            Some("bginf-sp1-device:bginf-sp1")
        );
        assert_eq!(
            borrowed.basis,
            ProcessDemandBasis::OtherPlatform {
                platform: "aarch64".into()
            }
        );

        // And a measurement on this platform outranks a larger one elsewhere.
        observe_suite(
            db.clone(),
            compile_suite.as_deref(),
            &context("bbs-device:bbs"),
            &completed(8 * GIB, 3),
            CompletionKind::Succeeded,
            MachineAdmission::default(),
        )
        .await;
        let here = resolve(compile, CellCommandClass::CargoCheck)
            .await
            .demand
            .unwrap();
        assert_eq!(here.peak_bytes, 8 * GIB);
        assert_eq!(here.basis, ProcessDemandBasis::ThisPlatform);
    }

    /// A command class spans unrelated commands, so its suite only stands
    /// in for an identity that has measured nothing.
    #[tokio::test]
    async fn a_class_suite_stands_in_only_for_an_unmeasured_identity() {
        let db = Arc::new(crate::storage::migrated_test_db("process-peak-class.db").await);
        let fedora = context("fedora:fedora");
        let class = suite_key("cairn", None, CellCommandClass::CargoTest);
        assert_eq!(class.as_deref(), Some("cairn/class:cargoTest"));
        assert_eq!(suite_key("cairn", None, CellCommandClass::Other), None);
        observe_suite(
            db.clone(),
            class.as_deref(),
            &fedora,
            &completed(10 * GIB, 1),
            CompletionKind::Failed,
            MachineAdmission::default(),
        )
        .await;
        assert_eq!(
            resolve_process_demand(
                db.clone(),
                None,
                class.as_deref(),
                CellCommandClass::CargoTest,
                &fedora
            )
            .await
            .demand
            .map(|demand| demand.peak_bytes),
            Some(10 * GIB),
            "an agent's batch has no identity, and its class speaks for it"
        );
        let small = CommandResourceIdentity {
            suite: None,
            ..identity("cargo-test-p-small")
        };
        observe_completed(
            db.clone(),
            Some(&small),
            &fedora,
            &completed(GIB, 2),
            CompletionKind::Succeeded,
        )
        .await;
        assert_eq!(
            resolve_process_demand(
                db,
                Some(&small),
                class.as_deref(),
                CellCommandClass::CargoTest,
                &fedora
            )
            .await
            .demand
            .map(|demand| demand.peak_bytes),
            Some(GIB)
        );
    }

    /// A kill while the victim ran alone teaches the suite its victim's size
    /// and counts against this machine at its total; a kill at another total
    /// starts the count again.
    #[tokio::test]
    async fn kills_are_counted_per_machine_total() {
        let db = Arc::new(crate::storage::migrated_test_db("process-peak-kills.db").await);
        let ub = context("bgsrv-ub-device:bgsrv-ub");
        let id = identity("rust-tests-a");
        let suite = suite_key("cairn", Some(&id), CellCommandClass::CargoTest);
        let total = 13_000_000_000;
        let kill = |victim, total, at| {
            let mut meta = oom_killed_for_test(victim, Some(total), 0, at);
            if let Some(CommandStop::OomKilled { process_name, .. }) = &mut meta.stop {
                *process_name = Some("rustc".into());
            }
            meta
        };
        for at in [1, 2] {
            observe_suite(
                db.clone(),
                suite.as_deref(),
                &ub,
                &kill(3 * GIB, total, at),
                CompletionKind::Failed,
                MachineAdmission::default(),
            )
            .await;
        }
        let demand = resolve_process_demand(
            db.clone(),
            Some(&id),
            suite.as_deref(),
            CellCommandClass::CargoTest,
            &ub,
        )
        .await
        .demand
        .unwrap();
        let counted = |demand: &ProcessDemand| {
            demand
                .kills_here
                .as_ref()
                .map(|kills| (kills.alone, kills.machine_total_bytes))
        };
        assert_eq!(counted(&demand), Some((2, total)));
        assert_eq!(demand.peak_bytes, 3 * GIB);
        observe_suite(
            db.clone(),
            suite.as_deref(),
            &ub,
            &kill(GIB, 2 * total, 3),
            CompletionKind::Failed,
            MachineAdmission::default(),
        )
        .await;
        let demand = resolve_process_demand(
            db,
            Some(&id),
            suite.as_deref(),
            CellCommandClass::CargoTest,
            &ub,
        )
        .await
        .demand
        .unwrap();
        assert_eq!(counted(&demand), Some((1, 2 * total)));
        assert_eq!(demand.peak_bytes, 3 * GIB, "the largest victim stands");
    }

    /// A kill of `victim` bytes at `at`, with `co_resident` compile commands
    /// beside it at its start.
    fn rustc_killed(victim: u64, total: u64, co_resident: u32, at: u64) -> CellExecutionMeta {
        let mut meta = oom_killed_for_test(victim, Some(total), co_resident, at);
        if let Some(CommandStop::OomKilled { process_name, .. }) = &mut meta.stop {
            *process_name = Some("rustc".into());
        }
        meta
    }

    async fn kills_on(
        db: Arc<LocalDb>,
        suite: &str,
        context: &ProfileContext,
    ) -> Option<SuiteKills> {
        resolve_process_demand(db, None, Some(suite), CellCommandClass::CargoTest, context)
            .await
            .demand
            .and_then(|demand| demand.kills_here)
    }

    /// cairn/6496: a kill while other commands shared the machine is
    /// recorded apart from the alone kills, with the most commands beside a
    /// victim and the brake state at the latest; it never joins the count
    /// that ends fit. A later successful run of the suite here clears both.
    #[tokio::test]
    async fn crowded_kills_are_kept_apart_and_a_clean_run_clears_every_kill() {
        let db = Arc::new(crate::storage::migrated_test_db("suite-kills-crowded.db").await);
        let sp1 = context("bginf-sp1-device:bginf-sp1");
        let suite = "cairn/rust-tests";
        let total = 130_663_231_488;
        let observe = |meta: CellExecutionMeta, kind, memory_brake| {
            let db = db.clone();
            let sp1 = sp1.clone();
            async move {
                observe_suite(
                    db,
                    Some(suite),
                    &sp1,
                    &meta,
                    kind,
                    MachineAdmission {
                        memory_brake,
                        batch_siblings: 0,
                    },
                )
                .await
            }
        };
        for (co_resident, at) in [(18, 10), (6, 20)] {
            observe(
                rustc_killed(20 * GIB, total, co_resident, at),
                CompletionKind::Failed,
                false,
            )
            .await;
        }
        observe(
            rustc_killed(19 * GIB, total, 0, 30),
            CompletionKind::Failed,
            true,
        )
        .await;
        let kills = kills_on(db.clone(), suite, &sp1).await.expect("kills here");
        assert_eq!(kills.alone, 1, "only the alone kill counts");
        assert_eq!(kills.memory_brake, Some(true));
        assert_eq!(
            kills.crowded,
            Some(CrowdedKills {
                count: 2,
                most_co_resident: Some(18),
                memory_brake: false,
                last_killed_at_unix_ms: 20,
            })
        );

        observe(
            CellExecutionMeta {
                stop: None,
                peak_process: Some(cairn_common::executor_protocol::PeakProcess {
                    bytes: 21 * GIB,
                    name: Some("rustc".into()),
                }),
                ..oom_killed_for_test(GIB, None, 0, 40)
            },
            CompletionKind::Succeeded,
            true,
        )
        .await;
        assert_eq!(kills_on(db.clone(), suite, &sp1).await, None);
        let peak = resolve_process_demand(db, None, Some(suite), CellCommandClass::CargoTest, &sp1)
            .await
            .demand
            .map(|demand| demand.peak_bytes);
        assert_eq!(
            peak,
            Some(21 * GIB),
            "the peak is rise-only; only the kills clear"
        );
    }

    /// Batch siblings share the machine though the load at the start does
    /// not show them, so a kill beside them is crowded.
    #[tokio::test]
    async fn a_kill_beside_batch_siblings_is_crowded() {
        let db = Arc::new(crate::storage::migrated_test_db("suite-kills-siblings.db").await);
        let ub = context("bgsrv-ub-device:bgsrv-ub");
        observe_suite(
            db.clone(),
            Some("cairn/rust-tests"),
            &ub,
            &rustc_killed(3 * GIB, 13_000_000_000, 0, 1),
            CompletionKind::Failed,
            MachineAdmission {
                memory_brake: true,
                batch_siblings: 2,
            },
        )
        .await;
        let kills = kills_on(db, "cairn/rust-tests", &ub).await.unwrap();
        assert_eq!(kills.alone, 0);
        assert_eq!(kills.crowded.unwrap().most_co_resident, Some(2));
    }

    /// An alone kill under another brake state than the count's starts the
    /// count again, and a count read under another brake state than the
    /// machine's current one does not count, including the counts recorded
    /// before the ledger stamped a brake state at all.
    #[tokio::test]
    async fn alone_kills_count_only_under_the_brake_state_they_happened_in() {
        let db = Arc::new(crate::storage::migrated_test_db("suite-kills-brake.db").await);
        let ub = context("bgsrv-ub-device:bgsrv-ub");
        let total = 13_000_000_000;
        for (memory_brake, at) in [(false, 1), (false, 2), (true, 3)] {
            observe_suite(
                db.clone(),
                Some("cairn/rust-tests"),
                &ub,
                &rustc_killed(3 * GIB, total, 0, at),
                CompletionKind::Failed,
                MachineAdmission {
                    memory_brake,
                    batch_siblings: 0,
                },
            )
            .await;
        }
        let kills = kills_on(db.clone(), "cairn/rust-tests", &ub).await.unwrap();
        assert_eq!((kills.alone, kills.memory_brake), (1, Some(true)));
        assert_eq!(kills.clone().under_brake(true).alone, 1);
        assert_eq!(kills.under_brake(false).alone, 0);

        // A row the ledger counted before it recorded admission.
        db.execute(
            "INSERT INTO suite_memory_evidence (suite_key, executor_class, os, arch, oom_kill_count, oom_machine_total_bytes, updated_at_unix_ms)
             VALUES ('cairn/class:cargoTest', ?1, ?2, ?3, 3, ?4, 1)",
            (
                ub.executor_class.clone(),
                ub.os.clone(),
                ub.arch.clone(),
                total as i64,
            ),
        )
        .await
        .unwrap();
        let legacy = kills_on(db, "cairn/class:cargoTest", &ub).await.unwrap();
        assert_eq!((legacy.alone, legacy.memory_brake), (3, None));
        assert_eq!(legacy.clone().under_brake(true).alone, 0);
        assert_eq!(legacy.under_brake(false).alone, 0);
    }

    /// The operator's `repaired` forgets every kill the machine holds, in
    /// any device context, and no other machine's.
    #[tokio::test]
    async fn repaired_forgets_the_machines_kills_and_only_its() {
        let db = Arc::new(crate::storage::migrated_test_db("suite-kills-repaired.db").await);
        let sp1 = context("bginf-sp1-device:bginf-sp1");
        let sp2 = context("bginf-sp2-device:bginf-sp2");
        for machine in [&sp1, &sp2] {
            for at in [1, 2] {
                observe_suite(
                    db.clone(),
                    Some("cairn/rust-tests"),
                    machine,
                    &rustc_killed(3 * GIB, 130_000_000_000, 0, at),
                    CompletionKind::Failed,
                    MachineAdmission::default(),
                )
                .await;
            }
        }
        assert_eq!(forget_kills_on(&db, "bginf-sp1").await.unwrap(), 1);
        assert_eq!(kills_on(db.clone(), "cairn/rust-tests", &sp1).await, None);
        assert_eq!(
            kills_on(db, "cairn/rust-tests", &sp2)
                .await
                .map(|kills| kills.alone),
            Some(2)
        );
    }

    /// cairn/6310's specimen: the macOS sampler names no process, so the
    /// 15.6 GiB it recorded for a whole suite is the tree's, and it is never
    /// a single-process demand, from the identity's row or the suite's. The
    /// same holds for an unnamed peak on Linux: the rule is about the
    /// measurement, not the platform.
    #[tokio::test]
    async fn an_unnamed_peak_is_unattributed_never_a_single_process() {
        let db = Arc::new(crate::storage::migrated_test_db("process-peak-unnamed.db").await);
        let peak = 16_751_935_488;
        let unnamed = CellExecutionMeta {
            peak_process: Some(PeakProcess {
                bytes: peak,
                name: None,
            }),
            ..completed(peak, 1)
        };
        for host in [
            ProfileContext {
                os: "macos".into(),
                arch: "aarch64".into(),
                ..context("bgsrv-mac-device:bgsrv-mac")
            },
            context("fedora:fedora"),
        ] {
            let id = identity("rust-tests-whole");
            let suite = suite_key("cairn", Some(&id), CellCommandClass::CargoTest);
            observe_suite(
                db.clone(),
                suite.as_deref(),
                &host,
                &unnamed,
                CompletionKind::Succeeded,
                MachineAdmission::default(),
            )
            .await;
            observe_completed(
                db.clone(),
                Some(&id),
                &host,
                &unnamed,
                CompletionKind::Succeeded,
            )
            .await;
            let evidence = resolve_process_demand(
                db.clone(),
                Some(&id),
                suite.as_deref(),
                CellCommandClass::CargoTest,
                &host,
            )
            .await;
            assert_eq!(evidence.demand, None, "{}: no single process", host.os);
            let tree = evidence.unattributed.expect("the tree's peak is kept");
            assert_eq!(tree.peak_bytes, peak);
            assert_eq!(
                tree.measured_on.as_deref(),
                Some(host.executor_class.as_str())
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::ExecutionMeasurementQuality;

    /// A duration lookup the reservation tests do not care about the answer to.
    fn test_duration_context() -> DurationContext {
        DurationContext {
            class: CellCommandClass::Other,
            warmth: ExecutionWarmth::Cold,
            now_unix_ms: 0,
        }
    }

    fn capabilities() -> ExecutorCapabilities {
        ExecutorCapabilities {
            os: "linux".into(),
            arch: "x86_64".into(),
            logical_cores: 8,
            concurrency_capacity: None,
            toolchains: vec!["rust".into()],
            projects_served: Vec::new(),
            disk_budget_bytes: None,
            memory_budget_bytes: None,
            sandbox: None,
            toolchain_detection: None,
            build_prerequisites: None,
            containment: None,
            page_call_programs: None,
            github_credential: None,
            machine_services: Vec::new(),
        }
    }

    /// A cold start is a safety margin, not a measurement, and it has to say so
    /// -- and it has to differ by work class, because 512 MiB was a fiction for a
    /// Rust compilation front and merely wrong for a type-check.
    #[test]
    fn the_cold_start_prior_is_explicitly_unmeasured_and_per_work_class() {
        let rust = cold_start_prior(CellCommandClass::CargoTest, &capabilities());
        let unclassified = cold_start_prior(CellCommandClass::Other, &capabilities());
        assert_eq!(rust.source, ResourceReservationSource::Unmeasured);
        assert_eq!(unclassified.source, ResourceReservationSource::Unmeasured);
        assert!(
            rust.memory_bytes > unclassified.memory_bytes,
            "a compilation front does not cost what an unclassified command costs"
        );
        assert_eq!(
            rust.concurrency_units, 1,
            "one lane is the cold-start prior for every class; more needs evidence"
        );
    }

    /// An unschedulable safety margin is not a safety margin: a prior larger
    /// than the machine could never be admitted at all.
    #[test]
    fn a_prior_never_exceeds_what_the_machine_has() {
        let small = ExecutorCapabilities {
            memory_budget_bytes: Some(256 * 1024 * 1024),
            disk_budget_bytes: Some(512 * 1024 * 1024),
            ..capabilities()
        };
        let prior = cold_start_prior(CellCommandClass::CargoTest, &small);
        assert_eq!(prior.memory_bytes, 192 * 1024 * 1024);
        assert_eq!(prior.disk_growth_bytes, 512 * 1024 * 1024);
    }

    /// "Never run here" and "the store could not be read" are both "no learned
    /// number", and they call for different responses. The rationale keeps them
    /// apart on the decision record.
    #[tokio::test]
    async fn a_cold_start_names_which_kind_of_nothing_it_found() {
        let db = Arc::new(crate::storage::migrated_test_db("resource-profile-rationale.db").await);
        let context = ProfileContext {
            executor_class: "device:executor".into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            toolchain_fingerprint: "rust".into(),
        };
        let prior = cold_start_prior(CellCommandClass::CargoClippy, &capabilities());

        let anonymous = resolve_reservation(
            db.clone(),
            None,
            &context,
            prior.clone(),
            None,
            test_duration_context(),
        )
        .await;
        assert_eq!(
            anonymous.rationale.fallback,
            Some(ReservationFallback::NoCommandIdentity)
        );
        assert_eq!(anonymous.reservation, prior);

        let identity = CommandResourceIdentity {
            version: 1,
            key: "check:rust".into(),
            suite: None,
        };
        let unseen = resolve_reservation(
            db.clone(),
            Some(&identity),
            &context,
            prior.clone(),
            None,
            test_duration_context(),
        )
        .await;
        assert_eq!(
            unseen.rationale.fallback,
            Some(ReservationFallback::NoProfileRecorded)
        );
        assert_eq!(unseen.rationale.profile_key.as_deref(), Some("check:rust"));
        assert!(
            unseen.rationale.profile_context.contains("linux"),
            "the context is on the record, because a profile learned elsewhere does not speak here"
        );

        update_profile(
            db.clone(),
            &identity,
            &context,
            10,
            Some(100),
            Some(200),
            Some(30),
        )
        .await
        .unwrap();
        let thin = resolve_reservation(
            db,
            Some(&identity),
            &context,
            prior.clone(),
            None,
            test_duration_context(),
        )
        .await;
        assert_eq!(
            thin.rationale.fallback,
            Some(ReservationFallback::BelowConfidenceFloor),
            "one observation cannot displace the prior, and the record says why"
        );
        assert_eq!(thin.rationale.sample_count, 1);
        assert_eq!(thin.rationale.headroom_percent, HEADROOM_PERCENT);
        assert_eq!(thin.reservation.memory_bytes, prior.memory_bytes);
    }

    /// A profile is keyed by the machine context it was learned on, so one
    /// platform's observation is never READ AS this platform's measurement.
    ///
    /// That is a claim about evidence, not about ignorance, and the difference
    /// matters: the same observation may still stand in as a labeled PRIOR here
    /// (see `a_cold_machine_borrows_the_fleets_evidence_without_calling_it_measured`),
    /// because how much memory a compiler holds travels between machines even
    /// though how fast one runs does not. What must never happen is the borrowed
    /// number arriving as this machine's learned profile — so the keyed lookup
    /// below stays exact, and the borrowing goes through its own path.
    #[tokio::test]
    async fn a_profile_learned_on_one_platform_does_not_answer_for_another() {
        let db = Arc::new(crate::storage::migrated_test_db("resource-profile-context.db").await);
        let identity = CommandResourceIdentity {
            version: 1,
            key: "check:rust".into(),
            suite: None,
        };
        let linux = ProfileContext {
            executor_class: "device:executor".into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            toolchain_fingerprint: "rust".into(),
        };
        let macos = ProfileContext {
            os: "macos".into(),
            arch: "aarch64".into(),
            ..linux.clone()
        };
        for _ in 0..6 {
            update_profile(
                db.clone(),
                &identity,
                &linux,
                10,
                Some(100),
                Some(200),
                Some(30),
            )
            .await
            .unwrap();
        }
        assert!(load_profile(db.clone(), &identity, &linux)
            .await
            .unwrap()
            .is_some());
        assert!(
            load_profile(db, &identity, &macos).await.unwrap().is_none(),
            "an arm64 macOS host has learned nothing from an x86 Linux run"
        );
    }

    /// Concurrency is not a learnable quantity, at any sample count.
    ///
    /// An observation records how many cores a command used when nothing was in
    /// its way; it is not a statement about how many lanes the command requires.
    /// The learner must therefore be unable to move this number in either
    /// direction, however much evidence it accumulates — whoever declared it is
    /// the only party that can change it (CAIRN-3345).
    #[test]
    fn no_amount_of_evidence_can_make_concurrency_a_learned_number() {
        for concurrency_units in [1, 16] {
            let prior = ResourceReservation {
                memory_bytes: 100,
                disk_growth_bytes: 200,
                concurrency_units,
                source: ResourceReservationSource::Unmeasured,
                measured_peak_bytes: None,
            };
            for sample_count in [0, 1, 4, MIN_CONFIDENT_SAMPLES, 500] {
                let reservation = reservation_for_profile(
                    &ResourceProfile {
                        sample_count,
                        upper_peak_rss_bytes: Some(10_000),
                        upper_disk_delta_bytes: Some(20_000),
                        upper_duration_ms: Some(480_000),
                        memory_kill: None,
                    },
                    prior.clone(),
                    None,
                    false,
                );
                assert_eq!(
                    reservation.concurrency_units, concurrency_units,
                    "{sample_count} observations must not restate the caller's lane demand"
                );
            }
        }
    }

    /// Under the confidence floor an observation raises the prior to its raw
    /// high-water mark, but cannot add predictive headroom until enough samples
    /// support that extrapolation.
    #[test]
    fn a_thin_profile_uses_raw_high_water_without_headroom() {
        let prior = ResourceReservation {
            memory_bytes: 100,
            disk_growth_bytes: 200,
            concurrency_units: 1,
            source: ResourceReservationSource::Unmeasured,
            measured_peak_bytes: None,
        };
        for sample_count in 0..MIN_CONFIDENT_SAMPLES {
            let smaller = reservation_for_profile(
                &ResourceProfile {
                    sample_count,
                    upper_peak_rss_bytes: Some(1),
                    upper_disk_delta_bytes: Some(2),
                    upper_duration_ms: Some(3),
                    memory_kill: None,
                },
                prior.clone(),
                None,
                false,
            );
            assert_eq!(smaller.memory_bytes, prior.memory_bytes);
            assert_eq!(smaller.disk_growth_bytes, prior.disk_growth_bytes);

            let larger = reservation_for_profile(
                &ResourceProfile {
                    sample_count,
                    upper_peak_rss_bytes: Some(1_000),
                    upper_disk_delta_bytes: Some(2_000),
                    upper_duration_ms: Some(3),
                    memory_kill: None,
                },
                prior.clone(),
                None,
                false,
            );
            assert_eq!(larger.memory_bytes, 1_000);
            assert_eq!(larger.disk_growth_bytes, 2_000);
        }
    }

    #[test]
    fn low_confidence_profile_cannot_collapse_prior() {
        let reservation = reservation_for_profile(
            &ResourceProfile {
                sample_count: 1,
                upper_peak_rss_bytes: Some(10),
                upper_disk_delta_bytes: Some(20),
                upper_duration_ms: Some(30),
                memory_kill: None,
            },
            ResourceReservation {
                memory_bytes: 100,
                disk_growth_bytes: 200,
                concurrency_units: 1,
                source: ResourceReservationSource::Unmeasured,
                measured_peak_bytes: None,
            },
            None,
            false,
        );
        assert_eq!(reservation.memory_bytes, 100);
        assert_eq!(reservation.disk_growth_bytes, 200);
    }

    #[test]
    fn confident_profile_has_explicit_headroom() {
        let reservation = reservation_for_profile(
            &ResourceProfile {
                sample_count: 5,
                upper_peak_rss_bytes: Some(100),
                upper_disk_delta_bytes: Some(200),
                upper_duration_ms: Some(300),
                memory_kill: None,
            },
            ResourceReservation::default(),
            None,
            false,
        );
        assert_eq!(reservation.memory_bytes, 125);
        assert_eq!(reservation.disk_growth_bytes, 250);
        assert_eq!(reservation.source, ResourceReservationSource::Learned);
    }

    const GIB: u64 = 1024 * 1024 * 1024;

    fn class_prior() -> ResourceReservation {
        ResourceReservation {
            memory_bytes: 2 * GIB,
            disk_growth_bytes: 4 * GIB,
            concurrency_units: 1,
            source: ResourceReservationSource::Unmeasured,
            measured_peak_bytes: None,
        }
    }

    /// A borrowed prior is the median of what other machines measured, never
    /// their maximum. Each contributing number is already a high-water mark, so
    /// their median is a typical WORST case — which is what a reservation wants
    /// — and one context measured under saturation gets a single vote instead of
    /// setting every other machine's opening reservation.
    #[test]
    fn a_fleet_prior_takes_the_median_of_other_machines_high_water_marks() {
        let evidence = FleetEvidence {
            peaks: vec![11 * GIB, 13 * GIB, 15 * GIB, 15 * GIB, 64 * GIB],
            disks: vec![4 * GIB, 8 * GIB, 12 * GIB],
        };
        let borrowed = fleet_prior(&evidence, &class_prior(), Some(103 * GIB))
            .expect("three machines past the confidence floor is evidence");
        assert_eq!(borrowed.memory_bytes, with_headroom(15 * GIB));
        assert_eq!(borrowed.disk_growth_bytes, with_headroom(8 * GIB));
        assert_eq!(
            borrowed.source,
            ResourceReservationSource::Unmeasured,
            "borrowed from elsewhere is still unmeasured here, and the executor's \
             cold-work brake reads exactly this label"
        );
        assert_eq!(borrowed.concurrency_units, class_prior().concurrency_units);
    }

    /// A prior is a floor. Machines that all happened to run this command
    /// cheaply cannot talk a host into admitting more than a cold start allows.
    #[test]
    fn a_fleet_prior_never_lowers_the_class_floor() {
        let evidence = FleetEvidence {
            peaks: vec![100 * 1024 * 1024, 120 * 1024 * 1024],
            disks: Vec::new(),
        };
        assert_eq!(fleet_prior(&evidence, &class_prior(), None), None);
    }

    /// The quarantine that guards a machine's own observations guards a borrowed
    /// one with more force. A peak that would consume this machine describes
    /// somewhere else's saturation, or a machine this command has no business
    /// running on; adopting it as a prior would refuse the work here forever.
    #[test]
    fn a_borrowed_peak_that_would_consume_this_machine_is_refused() {
        let evidence = FleetEvidence {
            peaks: vec![60 * GIB],
            disks: Vec::new(),
        };
        let roomy = fleet_prior(&evidence, &class_prior(), Some(103 * GIB))
            .expect("a machine with room for it can hold this prior");
        assert_eq!(roomy.memory_bytes, with_headroom(60 * GIB));
        assert_eq!(
            fleet_prior(&evidence, &class_prior(), Some(64 * GIB)),
            None,
            "a prior that would take nine tenths of the host is not this host's prior"
        );
    }

    /// The bglab-sp1 case end to end: a machine with no history of its own, a
    /// fleet with plenty of it, and a reservation that reflects the second
    /// without ever claiming to be the first (CAIRN-4365).
    #[tokio::test]
    async fn a_cold_machine_borrows_the_fleets_evidence_without_calling_it_measured() {
        let db =
            Arc::new(crate::storage::migrated_test_db("resource-profile-fleet-prior.db").await);
        let identity = CommandResourceIdentity {
            version: 1,
            key: "test:rust".into(),
            suite: None,
        };
        let context = |executor_class: &str, os: &str, arch: &str| ProfileContext {
            executor_class: executor_class.into(),
            os: os.into(),
            arch: arch.into(),
            toolchain_fingerprint: "rust".into(),
        };
        // A brand-new linux/aarch64 machine, the first of its platform in the
        // fleet, so not one profile anywhere shares its context.
        let sp1 = context("sp1:sp1", "linux", "aarch64");
        for (elsewhere, peak) in [
            (context("local:colocated", "macos", "aarch64"), 11 * GIB),
            (context("bbs:bbs", "linux", "x86_64"), 15 * GIB),
            (context("fedora:fedora", "linux", "x86_64"), 13 * GIB),
        ] {
            for _ in 0..MIN_CONFIDENT_SAMPLES {
                update_profile(
                    db.clone(),
                    &identity,
                    &elsewhere,
                    10,
                    Some(peak),
                    Some(4 * GIB),
                    Some(300_000),
                )
                .await
                .unwrap();
            }
        }

        let prior = cold_start_prior(CellCommandClass::CargoTest, &capabilities());
        let resolved = resolve_reservation(
            db,
            Some(&identity),
            &sp1,
            prior.clone(),
            Some(103 * GIB),
            test_duration_context(),
        )
        .await;

        assert_eq!(
            resolved.rationale.fallback,
            Some(ReservationFallback::FleetPrior),
            "the record has to name where a number this large came from"
        );
        assert_eq!(
            resolved.reservation.memory_bytes,
            with_headroom(13 * GIB),
            "the median of what three other machines measured, with the usual headroom"
        );
        assert_eq!(
            resolved.reservation.source,
            ResourceReservationSource::Unmeasured,
            "another machine's history is not this machine's measurement"
        );
        assert!(
            resolved.learned_estimate.is_none(),
            "nothing was learned here, and the record must not imply otherwise"
        );
        assert!(
            resolved.reservation.memory_bytes > prior.memory_bytes * 5,
            "the class floor is what let eight of these onto one machine at once"
        );
        assert!(
            !resolved.duration.is_learned(),
            "memory demand travels between machines; speed does not, and this \
             machine still has no idea how fast it runs this command"
        );
    }

    /// A prior that holds the number is not a measurement, whatever raised it.
    ///
    /// The executor holds resident UNMEASURED claims against its available
    /// memory, so this label is load-bearing rather than cosmetic: a prior
    /// returned as `Learned` switches that brake off for exactly the work it
    /// exists to brake.
    #[test]
    fn a_prior_that_holds_the_number_is_not_reported_as_measured() {
        let prior = ResourceReservation {
            memory_bytes: 1_000,
            disk_growth_bytes: 200,
            concurrency_units: 1,
            source: ResourceReservationSource::Unmeasured,
            measured_peak_bytes: None,
        };
        let profile = |upper_peak_rss_bytes| ResourceProfile {
            sample_count: 50,
            upper_peak_rss_bytes,
            upper_disk_delta_bytes: Some(20),
            upper_duration_ms: Some(30),
            memory_kill: None,
        };

        let thin = reservation_for_profile(
            &ResourceProfile {
                sample_count: 1,
                ..profile(Some(10))
            },
            prior.clone(),
            None,
            false,
        );
        assert_eq!(thin.memory_bytes, prior.memory_bytes);
        assert_eq!(
            thin.source,
            ResourceReservationSource::Unmeasured,
            "one observation smaller than the prior did not set this number"
        );

        let quarantined =
            reservation_for_profile(&profile(Some(10_000)), prior.clone(), None, true);
        assert_eq!(
            quarantined.source,
            ResourceReservationSource::Unmeasured,
            "a quarantined peak leaves the prior standing, and a prior is not evidence"
        );

        let measured = reservation_for_profile(&profile(Some(10_000)), prior, None, false);
        assert_eq!(measured.source, ResourceReservationSource::Learned);
    }

    #[test]
    fn upper_estimate_rises_immediately_and_decays_slowly() {
        assert_eq!(update_upper(100, 200), 200);
        assert_eq!(update_upper(320, 0), 310);
    }

    /// cairn/5731: a whole-suite pass learned at 16.8 GiB from full-width runs,
    /// then run on a 24 GiB Mac whose memory held its build to four jobs. That
    /// run measured the command at the width this machine grants, so it moves
    /// the high-water mark halfway down; a run at the CPU width moves it the
    /// ordinary thirty-second, and a failed run, however bounded, the same.
    #[tokio::test]
    async fn a_memory_bound_run_pulls_the_learned_peak_toward_what_it_measured() {
        const GIB: u64 = 1024 * 1024 * 1024;
        let db = Arc::new(crate::storage::migrated_test_db("peak-memory-bound.db").await);
        let identity = duration_identity();
        let context = duration_context();
        let peak_after = |label: &'static str| {
            let db = db.clone();
            let identity = identity.clone();
            let context = context.clone();
            async move {
                load_profile(db, &identity, &context)
                    .await
                    .unwrap()
                    .and_then(|profile| profile.upper_peak_rss_bytes)
                    .unwrap_or_else(|| panic!("{label}: a peak was recorded"))
            }
        };
        let run = |finished: u64, peak: u64, memory_available: u64| {
            let mut meta = completion(None, finished, 60_000);
            meta.peak_rss_bytes = Some(peak);
            meta.allocation = Some(
                cairn_common::executor_protocol::ExecutionAllocation::uncapped(
                    10,
                    cairn_common::executor_protocol::NoCapReason::PlatformHasNoCap,
                    None,
                    60_000,
                )
                .with_build_jobs(cairn_common::executor_protocol::BuildJobs::new(
                    10,
                    Some(cairn_common::executor_protocol::MemoryJobBound::new(
                        memory_available,
                        finished,
                        5 * GIB / 2,
                        1,
                    )),
                )),
            );
            meta
        };

        observe_completed(
            db.clone(),
            Some(&identity),
            &context,
            &run(1, 16 * GIB, 100 * GIB),
            CompletionKind::Succeeded,
        )
        .await;
        assert_eq!(peak_after("full width").await, 16 * GIB);

        observe_completed(
            db.clone(),
            Some(&identity),
            &context,
            &run(2, 8 * GIB, 10 * GIB),
            CompletionKind::Failed,
        )
        .await;
        assert_eq!(
            peak_after("failed").await,
            16 * GIB - 8 * GIB / 32,
            "a failed run is not the pass at this width"
        );

        let before = peak_after("before").await;
        observe_completed(
            db.clone(),
            Some(&identity),
            &context,
            &run(3, 8 * GIB, 10 * GIB),
            CompletionKind::Succeeded,
        )
        .await;
        assert_eq!(
            peak_after("memory bound").await,
            before - (before - 8 * GIB) / 2
        );

        let before = peak_after("before").await;
        observe_completed(
            db.clone(),
            Some(&identity),
            &context,
            &run(4, 8 * GIB, 100 * GIB),
            CompletionKind::Succeeded,
        )
        .await;
        assert_eq!(
            peak_after("cpu width").await,
            before - (before - 8 * GIB) / 32,
            "a run its memory did not bound decays as it always has"
        );
    }

    #[tokio::test]
    async fn concurrent_observations_increment_without_lost_updates() {
        let db =
            Arc::new(crate::storage::migrated_test_db("resource-profile-concurrency.db").await);
        let identity = CommandResourceIdentity {
            version: 1,
            key: "identity".into(),
            suite: None,
        };
        let context = ProfileContext {
            executor_class: "device:executor".into(),
            os: "test-os".into(),
            arch: "test-arch".into(),
            toolchain_fingerprint: "toolchain".into(),
        };
        let metadata = CellExecutionMeta {
            warmth: None,
            load_context: None,
            executor_id: "executor".into(),
            executor_device_id: "device".into(),
            executor_connection_generation: 1,
            cell_id: "slot".into(),
            cell_epoch: 1,
            started_at_unix_ms: 1,
            finished_at_unix_ms: 2,
            duration_ms: Some(10),
            peak_rss_bytes: Some(100),
            peak_physical_footprint_bytes: None,
            peak_process: None,
            cpu: None,
            allocation: None,
            disk_delta_bytes: Some(20),
            measurement_quality: None,
            environment_fingerprint: String::new(),
            verdict_platform: None,
            verdict_arch: None,
            verdict_environment_hash: None,
            sandbox: None,
            toolchain_fingerprint: None,
            containment: None,
            stop: None,
            cell_origin: None,
        };
        let (left, right) = tokio::join!(
            update_profile(
                db.clone(),
                &identity,
                &context,
                metadata.finished_at_unix_ms,
                metadata.peak_rss_bytes,
                metadata.disk_delta_bytes,
                metadata.duration_ms
            ),
            update_profile(
                db.clone(),
                &identity,
                &context,
                metadata.finished_at_unix_ms,
                metadata.peak_rss_bytes,
                metadata.disk_delta_bytes,
                metadata.duration_ms
            ),
        );
        left.unwrap();
        right.unwrap();
        let profile = load_profile(db, &identity, &context)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(profile.sample_count, 2);
        assert_eq!(profile.upper_peak_rss_bytes, Some(100));
    }

    #[tokio::test]
    async fn unavailable_disk_still_learns_duration_and_memory() {
        let db = Arc::new(
            crate::storage::migrated_test_db("resource-profile-unavailable-disk.db").await,
        );
        let identity = CommandResourceIdentity {
            version: 1,
            key: "check-resource".into(),
            suite: None,
        };
        let context = ProfileContext {
            executor_class: "device:executor".into(),
            os: "test-os".into(),
            arch: "test-arch".into(),
            toolchain_fingerprint: "toolchain".into(),
        };
        let metadata = CellExecutionMeta {
            warmth: None,
            load_context: None,
            executor_id: "executor".into(),
            executor_device_id: "device".into(),
            executor_connection_generation: 1,
            cell_id: "slot".into(),
            cell_epoch: 1,
            started_at_unix_ms: 1,
            finished_at_unix_ms: 11,
            duration_ms: Some(10),
            peak_rss_bytes: Some(100),
            peak_physical_footprint_bytes: None,
            peak_process: None,
            cpu: None,
            allocation: None,
            disk_delta_bytes: None,
            measurement_quality: Some(ExecutionMeasurementQuality {
                duration: MeasurementQuality::Authoritative,
                memory: MeasurementQuality::Sampled,
                disk: MeasurementQuality::Unavailable,
                memory_platform: Some("test".into()),
                disk_boundary: "unavailable".into(),
            }),
            environment_fingerprint: String::new(),
            verdict_platform: None,
            verdict_arch: None,
            verdict_environment_hash: None,
            sandbox: None,
            toolchain_fingerprint: None,
            containment: None,
            stop: None,
            cell_origin: None,
        };

        observe_completed(
            db.clone(),
            Some(&identity),
            &context,
            &metadata,
            CompletionKind::Succeeded,
        )
        .await;
        observe_completed(
            db.clone(),
            Some(&identity),
            &context,
            &metadata,
            CompletionKind::Succeeded,
        )
        .await;
        let profile = load_profile(db.clone(), &identity, &context)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(profile.sample_count, 2);
        assert_eq!(profile.upper_duration_ms, Some(10));
        assert_eq!(profile.upper_peak_rss_bytes, Some(100));
        assert_eq!(profile.upper_disk_delta_bytes, None);

        let resolved = resolve_reservation(
            db,
            Some(&identity),
            &context,
            ResourceReservation {
                memory_bytes: 1_000,
                disk_growth_bytes: 2_000,
                concurrency_units: 1,
                source: ResourceReservationSource::Unmeasured,
                measured_peak_bytes: None,
            },
            None,
            test_duration_context(),
        )
        .await;
        assert_eq!(resolved.reservation.disk_growth_bytes, 2_000);
        let estimate = resolved.learned_estimate.unwrap();
        assert_eq!(estimate.upper_duration_ms, Some(10));
        assert_eq!(estimate.upper_disk_growth_bytes, None);
    }

    /// A peak that nearly equals the machine's entire budget describes host
    /// saturation, not one command in isolation. The raw evidence stays visible
    /// in the rationale while admission falls back to the platform prior.
    #[tokio::test]
    async fn a_near_machine_total_memory_peak_is_quarantined_with_a_labeled_prior() {
        const GIB: u64 = 1024 * 1024 * 1024;
        let db = Arc::new(
            crate::storage::migrated_test_db("resource-profile-memory-quarantine.db").await,
        );
        let identity = CommandResourceIdentity {
            version: 1,
            key: "check:rust".into(),
            suite: None,
        };
        let context = ProfileContext {
            executor_class: "device:executor".into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            toolchain_fingerprint: "rust".into(),
        };
        for finished in 1..=MIN_CONFIDENT_SAMPLES {
            update_profile(
                db.clone(),
                &identity,
                &context,
                finished,
                Some(99 * GIB),
                Some(GIB),
                Some(1_000),
            )
            .await
            .unwrap();
        }
        let prior = ResourceReservation {
            memory_bytes: 2 * GIB,
            disk_growth_bytes: 4 * GIB,
            concurrency_units: 1,
            source: ResourceReservationSource::Unmeasured,
            measured_peak_bytes: None,
        };
        let resolved = resolve_reservation(
            db,
            Some(&identity),
            &context,
            prior.clone(),
            Some(100 * GIB),
            test_duration_context(),
        )
        .await;

        assert_eq!(resolved.reservation.memory_bytes, prior.memory_bytes);
        assert_eq!(
            resolved.rationale.fallback,
            Some(ReservationFallback::MemoryObservationQuarantined)
        );
        assert_eq!(resolved.rationale.upper_peak_rss_bytes, Some(99 * GIB));
    }

    /// The budget of the ~12 GiB host in cairn/4931's specimen B. Three quarters
    /// of it is exactly the 8,282,086,656 bytes that host reported as a cargo
    /// test's "required" memory, which was the clamp and not the measurement.
    const SPECIMEN_BUDGET: u64 = 11_042_782_208;
    const SPECIMEN_CAP: u64 = 8_282_086_656;

    async fn ceiling_for_local_peak(
        name: &str,
        samples: u64,
        peak: u64,
    ) -> ResolvedResourceProfile {
        let db = Arc::new(crate::storage::migrated_test_db(name).await);
        let identity = CommandResourceIdentity {
            version: 1,
            key: "test:rust".into(),
            suite: None,
        };
        let context = ProfileContext {
            executor_class: "bgsrv-ub:bgsrv-ub".into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            toolchain_fingerprint: "rust".into(),
        };
        for finished in 1..=samples {
            update_profile(
                db.clone(),
                &identity,
                &context,
                finished,
                Some(peak),
                Some(GIB),
                Some(1_000),
            )
            .await
            .unwrap();
        }
        resolve_reservation(
            db,
            Some(&identity),
            &context,
            cold_start_prior(CellCommandClass::CargoTest, &capabilities()),
            Some(SPECIMEN_BUDGET),
            test_duration_context(),
        )
        .await
    }

    /// The clamp keeps the reservation admissible, and now also says it
    /// clamped: a measured demand above the cap is a ceiling this machine will
    /// never reach, which is what lets placement prefer one that can hold it.
    #[tokio::test]
    async fn a_measured_demand_above_the_cap_is_stated_and_the_reservation_stays_clamped() {
        let peak = 8 * GIB;
        let resolved = ceiling_for_local_peak(
            "resource-profile-ceiling-measured.db",
            MIN_CONFIDENT_SAMPLES,
            peak,
        )
        .await;
        assert_eq!(resolved.reservation.memory_bytes, SPECIMEN_CAP);
        assert_eq!(
            resolved.memory_fit,
            MemoryFit::Exceeds(MemoryCeiling {
                estimate_bytes: with_headroom(peak),
                ceiling_bytes: SPECIMEN_CAP,
                killed_here: None,
            })
        );
    }

    /// A peak near the whole budget measured the machine's saturation, not the
    /// command. Calling it a ceiling would refuse this machine the work forever
    /// (a solo user's laptop included), so it states nothing.
    #[tokio::test]
    async fn a_quarantined_peak_is_not_a_ceiling() {
        let resolved = ceiling_for_local_peak(
            "resource-profile-ceiling-quarantined.db",
            MIN_CONFIDENT_SAMPLES,
            SPECIMEN_BUDGET,
        )
        .await;
        assert_eq!(
            resolved.rationale.fallback,
            Some(ReservationFallback::MemoryObservationQuarantined)
        );
        assert_eq!(resolved.memory_fit, MemoryFit::Unknown);
    }

    /// A prior is not a measurement, whatever it holds, and one that fits under
    /// the cap by construction never says the machine is too small.
    #[tokio::test]
    async fn a_number_the_prior_set_is_not_a_ceiling() {
        let unmeasured = ceiling_for_local_peak("resource-profile-ceiling-prior.db", 0, GIB).await;
        assert_eq!(
            unmeasured.rationale.fallback,
            Some(ReservationFallback::NoProfileRecorded)
        );
        assert_eq!(unmeasured.memory_fit, MemoryFit::Unknown);

        let thin_and_small =
            ceiling_for_local_peak("resource-profile-ceiling-thin.db", 1, 512 * 1024 * 1024).await;
        assert_eq!(
            thin_and_small.reservation.memory_bytes,
            cold_start_prior(CellCommandClass::CargoTest, &capabilities()).memory_bytes
        );
        assert_eq!(
            thin_and_small.memory_fit,
            MemoryFit::Holds,
            "a measured peak under the cap is evidence the machine can hold it"
        );
    }

    /// A machine with no history of its own is charged what the fleet
    /// measured, clamped to its cap, and the borrowed process-tree total does
    /// not decide its fit: that total grew with the width the measuring
    /// machine granted, which is how both Sparks were passed over for a lane
    /// that peaked at 15 GiB. The largest single process decides fit.
    #[tokio::test]
    async fn a_borrowed_tree_peak_prices_the_work_and_never_decides_its_fit() {
        let db = Arc::new(
            crate::storage::migrated_test_db("resource-profile-ceiling-borrowed.db").await,
        );
        let identity = CommandResourceIdentity {
            version: 1,
            key: "test:rust".into(),
            suite: None,
        };
        let context = |executor_class: &str| ProfileContext {
            executor_class: executor_class.into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            toolchain_fingerprint: "rust".into(),
        };
        for elsewhere in ["bbs:bbs", "fedora:fedora"] {
            for finished in 1..=MIN_CONFIDENT_SAMPLES {
                update_profile(
                    db.clone(),
                    &identity,
                    &context(elsewhere),
                    finished,
                    Some(8 * GIB),
                    Some(GIB),
                    Some(1_000),
                )
                .await
                .unwrap();
            }
        }
        let resolved = resolve_reservation(
            db,
            Some(&identity),
            &context("bgsrv-ub:bgsrv-ub"),
            cold_start_prior(CellCommandClass::CargoTest, &capabilities()),
            Some(SPECIMEN_BUDGET),
            test_duration_context(),
        )
        .await;
        assert_eq!(resolved.reservation.memory_bytes, SPECIMEN_CAP);
        assert_eq!(resolved.memory_fit, MemoryFit::Unknown);
        assert_eq!(
            resolved.reservation.measured_peak_bytes, None,
            "a borrowed tree peak is another machine's width, not a measured peak here"
        );
    }

    fn process_demand(peak_bytes: u64, basis: ProcessDemandBasis) -> ProcessEvidence {
        ProcessEvidence {
            demand: Some(ProcessDemand {
                peak_bytes,
                name: Some("rustc".into()),
                suite: None,
                measured_on: None,
                kills_here: None,
                basis,
            }),
            ..ProcessEvidence::default()
        }
    }

    /// What admission charges a running command is measured, never declared:
    /// the larger of this machine's own tree peak and the largest process of
    /// this work measured on any platform; the largest process alone when the
    /// tree peak was quarantined or borrowed (and so is absent); never a class
    /// prior; and nothing when nothing was measured.
    #[test]
    fn the_measured_peak_is_the_larger_measurement_and_never_a_prior() {
        let here = process_demand(9 * GIB, ProcessDemandBasis::ThisPlatform);
        assert_eq!(
            measured_peak_bytes(Some(4 * GIB), &here),
            Some(with_headroom(9 * GIB))
        );
        assert_eq!(measured_peak_bytes(Some(20 * GIB), &here), Some(20 * GIB));
        assert_eq!(
            measured_peak_bytes(None, &here),
            Some(with_headroom(9 * GIB)),
            "a quarantined or borrowed tree peak leaves the largest process standing"
        );
        let elsewhere = process_demand(
            9 * GIB,
            ProcessDemandBasis::OtherPlatform {
                platform: "aarch64".into(),
            },
        );
        assert_eq!(
            measured_peak_bytes(None, &elsewhere),
            Some(with_headroom(9 * GIB))
        );
        let prior = process_demand(9 * GIB, ProcessDemandBasis::ClassPrior { platform: None });
        assert_eq!(measured_peak_bytes(None, &prior), None);
        assert_eq!(measured_peak_bytes(None, &ProcessEvidence::default()), None);
    }

    /// cairn/5472's first specimen: bgsrv-ub had run the whole suite past the
    /// confidence floor with every peak filling the machine, so its own profile
    /// was confident and quarantined at once, and the 20+ GiB the rest of the
    /// fleet measured for the same command was never read: the work was charged
    /// its class prior and its fit was unknown. A quarantined peak does not
    /// speak for this machine's memory, and neither does the fleet's tree
    /// total; the protection this machine needs comes from the largest single
    /// process, which placement judges (`process_fit_tests`).
    #[tokio::test]
    async fn a_quarantined_machine_is_not_judged_by_the_fleets_tree_total() {
        let db = Arc::new(
            crate::storage::migrated_test_db("resource-profile-quarantined-borrows.db").await,
        );
        let identity = CommandResourceIdentity {
            version: 1,
            key: "test:rust".into(),
            suite: None,
        };
        let context = |executor_class: &str| ProfileContext {
            executor_class: executor_class.into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            toolchain_fingerprint: "rust".into(),
        };
        for (executor_class, peak) in [
            ("bbs:bbs", 22 * GIB),
            ("fedora:fedora", 22 * GIB),
            ("bgsrv-ub:bgsrv-ub", SPECIMEN_BUDGET),
        ] {
            for finished in 1..=MIN_CONFIDENT_SAMPLES {
                update_profile(
                    db.clone(),
                    &identity,
                    &context(executor_class),
                    finished,
                    Some(peak),
                    Some(GIB),
                    Some(1_000),
                )
                .await
                .unwrap();
            }
        }
        let resolved = resolve_reservation(
            db,
            Some(&identity),
            &context("bgsrv-ub:bgsrv-ub"),
            cold_start_prior(CellCommandClass::CargoTest, &capabilities()),
            Some(SPECIMEN_BUDGET),
            test_duration_context(),
        )
        .await;
        assert_eq!(
            resolved.rationale.fallback,
            Some(ReservationFallback::MemoryObservationQuarantined),
            "its own peak is still not a demand"
        );
        assert_eq!(resolved.memory_fit, MemoryFit::Unknown);
    }

    /// bgsrv-ub, the 12.1 GiB host of cairn/5416's specimens.
    const SPECIMEN_TOTAL: u64 = 13_000_000_000;
    /// The `rustc` the kernel killed beside another compile: 3.4 GiB.
    const CROWDED_VICTIM: u64 = 3_650_722_201;

    fn killed_here() -> (CommandResourceIdentity, ProfileContext) {
        (
            CommandResourceIdentity {
                version: 1,
                key: "test:rust".into(),
                suite: None,
            },
            ProfileContext {
                executor_class: "bgsrv-ub:bgsrv-ub".into(),
                os: "linux".into(),
                arch: "x86_64".into(),
                toolchain_fingerprint: "rust".into(),
            },
        )
    }

    async fn resolve_killed(
        db: Arc<LocalDb>,
        identity: &CommandResourceIdentity,
        context: &ProfileContext,
    ) -> ResolvedResourceProfile {
        resolve_reservation(
            db,
            Some(identity),
            context,
            cold_start_prior(CellCommandClass::CargoTest, &capabilities()),
            Some(SPECIMEN_BUDGET),
            test_duration_context(),
        )
        .await
    }

    /// cairn/5416's first specimen. The tree's peak at the kill nears the whole
    /// budget, so the quarantine discards it and the work fell back to its 2
    /// GiB class prior, which fit the 2.5 GiB the machine had free. The
    /// victim's own size survives the quarantine and is what the next placement
    /// is charged.
    #[tokio::test]
    async fn a_kill_beside_other_work_floors_the_demand_the_quarantine_discarded() {
        let db =
            Arc::new(crate::storage::migrated_test_db("resource-profile-oom-crowded.db").await);
        let (identity, context) = killed_here();
        let mut killed = oom_killed_for_test(CROWDED_VICTIM, Some(SPECIMEN_TOTAL), 1, 10);
        killed.peak_rss_bytes = Some(SPECIMEN_BUDGET);
        observe_memory_kill(
            db.clone(),
            Some(&identity),
            &context,
            &killed,
            false,
            Some(SPECIMEN_BUDGET),
        )
        .await;
        observe_completed(
            db.clone(),
            Some(&identity),
            &context,
            &killed,
            CompletionKind::Failed,
        )
        .await;

        let kill = MemoryKill {
            victim_resident_bytes: CROWDED_VICTIM,
            killed_at_unix_ms: 10,
            alone_on_machine_bytes: None,
        };
        let profile = load_profile(db.clone(), &identity, &context)
            .await
            .unwrap()
            .expect("the kill wrote the demand row");
        assert_eq!(profile.memory_kill, Some(kill));
        assert_eq!(profile.sample_count, 1, "the kill is not a second sample");

        let resolved = resolve_killed(db, &identity, &context).await;
        assert_eq!(
            resolved.rationale.fallback,
            Some(ReservationFallback::MemoryObservationQuarantined),
            "the tree's peak is still quarantined"
        );
        assert_eq!(resolved.reservation.memory_bytes, CROWDED_VICTIM);
        assert_eq!(
            resolved.reservation.source,
            ResourceReservationSource::Unmeasured,
            "a run that died growing measured a floor, not an upper bound"
        );
        assert_eq!(resolved.rationale.memory_kill, Some(kill));
        assert_eq!(
            resolved.memory_fit,
            MemoryFit::Unknown,
            "a floor under the cap is not proof the machine holds the command"
        );
    }

    /// A kill with nothing else of Cairn's on the machine is the machine's
    /// answer that it cannot run the command at all. It stands as a ceiling
    /// naming the kill, survives a later crowded kill, and gives way only when
    /// the machine has held the command; the victim's floor outlives it.
    #[tokio::test]
    async fn a_kill_while_alone_is_a_ceiling_here_until_the_machine_holds_the_command() {
        let db = Arc::new(crate::storage::migrated_test_db("resource-profile-oom-alone.db").await);
        let (identity, context) = killed_here();
        let victim = 6_871_947_673;
        let kill = |db: Arc<LocalDb>, meta: CellExecutionMeta| {
            let identity = identity.clone();
            let context = context.clone();
            async move {
                observe_memory_kill(
                    db,
                    Some(&identity),
                    &context,
                    &meta,
                    false,
                    Some(SPECIMEN_BUDGET),
                )
                .await
            }
        };
        kill(
            db.clone(),
            oom_killed_for_test(victim, Some(SPECIMEN_TOTAL), 0, 10),
        )
        .await;

        let excluded = resolve_killed(db.clone(), &identity, &context).await;
        let MemoryFit::Exceeds(ceiling) = excluded.memory_fit else {
            panic!(
                "a 6.4 GiB victim fits the cap; the solo kill is what excludes: {:?}",
                excluded.memory_fit
            );
        };
        assert_eq!(ceiling.ceiling_bytes, SPECIMEN_CAP);
        assert_eq!(
            ceiling.killed_here,
            Some(MemoryKill {
                victim_resident_bytes: victim,
                killed_at_unix_ms: 10,
                alone_on_machine_bytes: Some(SPECIMEN_TOTAL),
            })
        );
        assert_eq!(excluded.reservation.memory_bytes, SPECIMEN_CAP);
        let said = ceiling.describe();
        assert!(
            said.contains("killed it here for memory at 6.4 GiB resident while it ran alone on this 12.1 GiB machine"),
            "{said}"
        );

        kill(
            db.clone(),
            oom_killed_for_test(victim / 2, Some(SPECIMEN_TOTAL), 2, 20),
        )
        .await;
        let still = resolve_killed(db.clone(), &identity, &context).await;
        let MemoryFit::Exceeds(ceiling) = still.memory_fit else {
            panic!("a crowded kill does not answer for a solo one");
        };
        let recorded = ceiling.killed_here.unwrap();
        assert_eq!(
            recorded.victim_resident_bytes, victim,
            "the floor only rises"
        );
        assert_eq!(recorded.killed_at_unix_ms, 20);

        let mut held = oom_killed_for_test(GIB, Some(SPECIMEN_TOTAL), 0, 30);
        held.stop = None;
        observe_completed(
            db.clone(),
            Some(&identity),
            &context,
            &held,
            CompletionKind::Succeeded,
        )
        .await;
        let after = resolve_killed(db, &identity, &context).await;
        assert!(
            !matches!(after.memory_fit, MemoryFit::Exceeds(_)),
            "the machine held it: {:?}",
            after.memory_fit
        );
        assert_eq!(after.reservation.memory_bytes, victim);
        assert_eq!(
            after
                .rationale
                .memory_kill
                .map(|kill| kill.alone_on_machine_bytes),
            Some(None)
        );
    }

    /// An item of a parallel batch ran beside its siblings, whatever the load
    /// context taken at the batch's start says; and a run the kernel did not
    /// kill records nothing.
    #[tokio::test]
    async fn only_an_out_of_memory_kill_with_nothing_beside_it_is_alone() {
        let db =
            Arc::new(crate::storage::migrated_test_db("resource-profile-oom-siblings.db").await);
        let (identity, context) = killed_here();
        let mut not_killed = oom_killed_for_test(CROWDED_VICTIM, Some(SPECIMEN_TOTAL), 0, 5);
        not_killed.stop = None;
        observe_memory_kill(
            db.clone(),
            Some(&identity),
            &context,
            &not_killed,
            false,
            Some(SPECIMEN_BUDGET),
        )
        .await;
        assert!(load_profile(db.clone(), &identity, &context)
            .await
            .unwrap()
            .is_none());

        observe_memory_kill(
            db.clone(),
            Some(&identity),
            &context,
            &oom_killed_for_test(CROWDED_VICTIM, Some(SPECIMEN_TOTAL), 0, 10),
            true,
            Some(SPECIMEN_BUDGET),
        )
        .await;
        let kill = load_profile(db, &identity, &context)
            .await
            .unwrap()
            .unwrap()
            .memory_kill
            .unwrap();
        assert_eq!(kill.victim_resident_bytes, CROWDED_VICTIM);
        assert_eq!(kill.alone_on_machine_bytes, None);
    }

    #[test]
    fn machine_memory_ceiling_never_yields_to_a_larger_prior() {
        let resolved = reservation_for_profile(
            &ResourceProfile {
                sample_count: MIN_CONFIDENT_SAMPLES,
                upper_peak_rss_bytes: Some(99),
                upper_disk_delta_bytes: None,
                upper_duration_ms: None,
                memory_kill: None,
            },
            ResourceReservation {
                memory_bytes: 100,
                disk_growth_bytes: 10,
                concurrency_units: 1,
                source: ResourceReservationSource::Unmeasured,
                measured_peak_bytes: None,
            },
            Some(100),
            true,
        );
        assert_eq!(resolved.memory_bytes, 75);
    }

    fn duration_context() -> ProfileContext {
        ProfileContext {
            executor_class: "device:executor".into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            toolchain_fingerprint: "rust".into(),
        }
    }

    fn duration_identity() -> CommandResourceIdentity {
        CommandResourceIdentity {
            version: 1,
            key: "check:rust".into(),
            suite: None,
        }
    }

    /// A completion the executor measured, at a stated warmth.
    fn completion(
        warmth: Option<ExecutionWarmth>,
        finished: u64,
        duration_ms: u64,
    ) -> CellExecutionMeta {
        CellExecutionMeta {
            warmth,
            load_context: Some(ExecutionLoadContext::default()),
            environment_fingerprint: String::new(),
            executor_id: "executor".into(),
            executor_device_id: "device".into(),
            executor_connection_generation: 1,
            cell_id: "slot".into(),
            cell_epoch: 1,
            started_at_unix_ms: finished.saturating_sub(duration_ms),
            finished_at_unix_ms: finished,
            duration_ms: Some(duration_ms),
            peak_rss_bytes: Some(100),
            peak_physical_footprint_bytes: None,
            peak_process: None,
            cpu: None,
            allocation: None,
            disk_delta_bytes: Some(20),
            measurement_quality: Some(ExecutionMeasurementQuality {
                duration: MeasurementQuality::Authoritative,
                memory: MeasurementQuality::Sampled,
                disk: MeasurementQuality::Sampled,
                memory_platform: Some("test".into()),
                disk_boundary: "cell".into(),
            }),
            verdict_platform: None,
            verdict_arch: None,
            verdict_environment_hash: None,
            sandbox: None,
            toolchain_fingerprint: None,
            containment: None,
            stop: None,
            cell_origin: None,
        }
    }

    /// The failure this whole stratification exists to prevent: an incremental
    /// compile against a populated target directory and a full one against an
    /// empty one are different work, and a single averaged number describes
    /// neither. A machine that has just built this tree must not be predicted at
    /// its cold-start speed, nor an empty one at its warm speed.
    #[tokio::test]
    async fn warm_and_cold_observations_of_one_command_never_mix() {
        let db = Arc::new(crate::storage::migrated_test_db("duration-warmth.db").await);
        let identity = duration_identity();
        let context = duration_context();

        for finished in 1..=3 {
            observe_completed(
                db.clone(),
                Some(&identity),
                &context,
                &completion(Some(ExecutionWarmth::Cold), finished, 300_000),
                CompletionKind::Succeeded,
            )
            .await;
            observe_completed(
                db.clone(),
                Some(&identity),
                &context,
                &completion(Some(ExecutionWarmth::PreparedWarmSlot), finished, 20_000),
                CompletionKind::Succeeded,
            )
            .await;
        }

        let cold = resolve_duration(
            db.clone(),
            Some(&identity),
            &context,
            ExecutionWarmth::Cold,
            CellCommandClass::CargoTest,
            10,
        )
        .await;
        let warm = resolve_duration(
            db.clone(),
            Some(&identity),
            &context,
            ExecutionWarmth::PreparedWarmSlot,
            CellCommandClass::CargoTest,
            10,
        )
        .await;
        assert_eq!(cold.predicted_ms, 300_000);
        assert_eq!(warm.predicted_ms, 20_000);
        assert!(cold.is_learned() && warm.is_learned());
        assert_eq!(cold.warmth, ExecutionWarmth::Cold);
        assert_eq!(warm.warmth, ExecutionWarmth::PreparedWarmSlot);

        // The reservation half saw every one of those six executions: memory
        // demand does not depend on what was already on disk, so splitting it by
        // warmth would only make the high-water mark cover less.
        let profile = load_profile(db, &identity, &context)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(profile.sample_count, 6);
    }

    /// A duration profile describes one machine on one platform. Reading a
    /// Linux observation as evidence about a macOS host is how a fleet predicts
    /// speeds nobody ever measured.
    #[tokio::test]
    async fn a_duration_learned_in_one_context_does_not_answer_for_another() {
        let db = Arc::new(crate::storage::migrated_test_db("duration-context.db").await);
        let identity = duration_identity();
        let here = duration_context();
        for finished in 1..=3 {
            observe_completed(
                db.clone(),
                Some(&identity),
                &here,
                &completion(Some(ExecutionWarmth::Cold), finished, 50_000),
                CompletionKind::Succeeded,
            )
            .await;
        }

        for elsewhere in [
            ProfileContext {
                os: "macos".into(),
                ..here.clone()
            },
            ProfileContext {
                arch: "aarch64".into(),
                ..here.clone()
            },
            ProfileContext {
                toolchain_fingerprint: "rust,node".into(),
                ..here.clone()
            },
            ProfileContext {
                executor_class: "other-device:executor".into(),
                ..here.clone()
            },
        ] {
            let estimate = resolve_duration(
                db.clone(),
                Some(&identity),
                &elsewhere,
                ExecutionWarmth::Cold,
                CellCommandClass::CargoTest,
                10,
            )
            .await;
            assert_eq!(
                estimate.source,
                DurationEvidence::Unmeasured,
                "{elsewhere:?} inherited a prediction it never earned"
            );
            assert_eq!(estimate.fallback, Some(DurationFallback::NoProfileRecorded));
        }
    }

    /// The specimen: one forty-minute run on a machine that was swapping must
    /// not become that machine's answer for the next hour. A median over a
    /// bounded window rejects it, and later observations push it out entirely.
    #[tokio::test]
    async fn one_extreme_observation_never_becomes_the_prediction() {
        let db = Arc::new(crate::storage::migrated_test_db("duration-outlier.db").await);
        let identity = duration_identity();
        let context = duration_context();
        let predict = |db: Arc<LocalDb>,
                       identity: CommandResourceIdentity,
                       context: ProfileContext| async move {
            resolve_duration(
                db,
                Some(&identity),
                &context,
                ExecutionWarmth::Cold,
                CellCommandClass::CargoTest,
                100,
            )
            .await
        };

        for finished in 1..=2 {
            observe_completed(
                db.clone(),
                Some(&identity),
                &context,
                &completion(Some(ExecutionWarmth::Cold), finished, 60_000),
                CompletionKind::Succeeded,
            )
            .await;
        }
        observe_completed(
            db.clone(),
            Some(&identity),
            &context,
            &completion(Some(ExecutionWarmth::Cold), 3, 2_400_000),
            CompletionKind::Succeeded,
        )
        .await;
        let with_outlier = predict(db.clone(), identity.clone(), context.clone()).await;
        assert_eq!(
            with_outlier.predicted_ms, 60_000,
            "the median holds the line at the two representative runs"
        );

        // Enough later observations to advance the window past the outlier. The
        // sample count keeps rising, so the profile does not forget that it has
        // history -- only that the outlier is no longer part of it.
        for finished in 4..=(4 + DURATION_SAMPLE_WINDOW as u64) {
            observe_completed(
                db.clone(),
                Some(&identity),
                &context,
                &completion(Some(ExecutionWarmth::Cold), finished, 55_000),
                CompletionKind::Succeeded,
            )
            .await;
        }
        let displaced = predict(db.clone(), identity.clone(), context.clone()).await;
        assert_eq!(displaced.predicted_ms, 55_000);
        assert!(
            displaced.sample_count > DURATION_SAMPLE_WINDOW as u64,
            "the window is bounded; the history behind it is not"
        );
        let stored = load_duration_profile(db, &identity, &context, ExecutionWarmth::Cold)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.recent_ms.len(), DURATION_SAMPLE_WINDOW);
        assert!(
            !stored.recent_ms.contains(&2_400_000),
            "the outlier aged out by sample count, not by clock"
        );
    }

    /// Every way a lookup can come back with nothing is a labeled prior that
    /// names which nothing it found. None of them is silence, and none of them
    /// is a number presented as a measurement.
    #[tokio::test]
    async fn every_absent_duration_is_a_named_prior() {
        let db = Arc::new(crate::storage::migrated_test_db("duration-absences.db").await);
        let identity = duration_identity();
        let context = duration_context();

        let anonymous = resolve_duration(
            db.clone(),
            None,
            &context,
            ExecutionWarmth::Cold,
            CellCommandClass::CargoTest,
            10,
        )
        .await;
        assert_eq!(
            anonymous.fallback,
            Some(DurationFallback::NoCommandIdentity)
        );
        assert_eq!(
            anonymous.predicted_ms,
            duration_prior(CellCommandClass::CargoTest)
        );
        assert_eq!(anonymous.sample_count, 0);

        let unseen = resolve_duration(
            db.clone(),
            Some(&identity),
            &context,
            ExecutionWarmth::Cold,
            CellCommandClass::Typecheck,
            10,
        )
        .await;
        assert_eq!(unseen.fallback, Some(DurationFallback::NoProfileRecorded));
        assert_eq!(
            unseen.predicted_ms,
            duration_prior(CellCommandClass::Typecheck),
            "the prior is a class prior, so it differs by class and not by machine"
        );

        for finished in 1..=(MIN_CONFIDENT_DURATION_SAMPLES - 1) {
            observe_completed(
                db.clone(),
                Some(&identity),
                &context,
                &completion(Some(ExecutionWarmth::Cold), finished, 1_000),
                CompletionKind::Succeeded,
            )
            .await;
        }
        let thin = resolve_duration(
            db.clone(),
            Some(&identity),
            &context,
            ExecutionWarmth::Cold,
            CellCommandClass::CargoTest,
            10,
        )
        .await;
        assert_eq!(thin.fallback, Some(DurationFallback::BelowConfidenceFloor));
        assert_eq!(thin.source, DurationEvidence::Unmeasured);
        assert_ne!(
            thin.predicted_ms, 1_000,
            "under the floor a median cannot reject an outlier, so it is not consulted"
        );

        observe_completed(
            db.clone(),
            Some(&identity),
            &context,
            &completion(
                Some(ExecutionWarmth::Cold),
                MIN_CONFIDENT_DURATION_SAMPLES,
                1_000,
            ),
            CompletionKind::Succeeded,
        )
        .await;
        let learned = resolve_duration(
            db.clone(),
            Some(&identity),
            &context,
            ExecutionWarmth::Cold,
            CellCommandClass::CargoTest,
            10,
        )
        .await;
        assert_eq!(learned.source, DurationEvidence::Learned);
        assert_eq!(learned.predicted_ms, 1_000);

        // Silence is not evidence of continuity: a machine's toolchain, caches,
        // and disk all move, and a prediction older than the age limit is
        // describing a machine that no longer exists.
        let stale = resolve_duration(
            db,
            Some(&identity),
            &context,
            ExecutionWarmth::Cold,
            CellCommandClass::CargoTest,
            MIN_CONFIDENT_DURATION_SAMPLES + DURATION_PROFILE_STALE_AFTER_MS + 1,
        )
        .await;
        assert_eq!(stale.fallback, Some(DurationFallback::ProfileTooOld));
        assert_eq!(stale.source, DurationEvidence::Unmeasured);
    }

    /// An execution that makes no warmth claim is charged against the
    /// reservation profile -- memory demand does not depend on knowing why --
    /// but teaches the predictor nothing. Filing it under a guessed warmth is
    /// exactly how warm and cold profiles contaminate each other.
    #[tokio::test]
    async fn an_execution_without_a_warmth_claim_teaches_the_predictor_nothing() {
        let db = Arc::new(crate::storage::migrated_test_db("duration-unclaimed.db").await);
        let identity = duration_identity();
        let context = duration_context();
        for finished in 1..=5 {
            observe_completed(
                db.clone(),
                Some(&identity),
                &context,
                &completion(None, finished, 42_000),
                CompletionKind::Succeeded,
            )
            .await;
        }

        assert_eq!(
            load_profile(db.clone(), &identity, &context)
                .await
                .unwrap()
                .unwrap()
                .sample_count,
            5
        );
        for warmth in [
            ExecutionWarmth::Cold,
            ExecutionWarmth::RepositoryOnly,
            ExecutionWarmth::PreparedWarmSlot,
        ] {
            assert!(
                load_duration_profile(db.clone(), &identity, &context, warmth)
                    .await
                    .unwrap()
                    .is_none(),
                "an unattributable duration must not land in any warmth's profile"
            );
        }
    }

    /// A window the store can no longer parse is an empty window, not a
    /// zero-length run. The caller then reads too few samples and falls back to
    /// the labeled prior, which is the honest answer to a store that has stopped
    /// making sense.
    #[test]
    fn an_undecodable_window_is_empty_rather_than_instantaneous() {
        assert!(decode_window("not json").is_empty());
        assert!(decode_window("").is_empty());
        assert_eq!(decode_window("[10,20,30]"), vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn contention_curve_falls_back_from_machine_to_global_to_labeled_prior() {
        let db = Arc::new(crate::storage::migrated_test_db("contention-fallback.db").await);
        let load = ExecutionLoadContext {
            co_resident_compile_jobs: 2,
            co_resident_light_jobs: 0,
            cpu_utilization_millis: Some(700),
            io_pressure_full_millis: None,
            memory_pressure_full_millis: None,
            host_stall_stale: false,
        };
        let target = duration_context();
        assert_eq!(
            resolve_contention(db.clone(), &target, &load, 10)
                .await
                .source,
            ContentionEvidence::Prior
        );

        let other = ProfileContext {
            executor_class: "other:executor".into(),
            ..target.clone()
        };
        for finished in 1..=MIN_CONTENTION_SAMPLES as u64 {
            record_contention(db.clone(), &other, &load, finished, 1_500)
                .await
                .unwrap();
        }
        let global = resolve_contention(db.clone(), &target, &load, 10).await;
        assert_eq!(
            (global.source, global.multiplier_millis),
            (ContentionEvidence::Global, 1_500)
        );

        for finished in 4..=(3 + MIN_CONTENTION_SAMPLES as u64) {
            record_contention(db.clone(), &target, &load, finished, 1_900)
                .await
                .unwrap();
        }
        let machine = resolve_contention(db, &target, &load, 10).await;
        assert_eq!(
            (machine.source, machine.multiplier_millis),
            (ContentionEvidence::Machine, 1_900)
        );
    }

    const MARKER: &str = "CAIRN_CHECK_RESULT_JSON=";

    /// What a runner script prints when it decides there is nothing to check.
    fn no_op_output() -> String {
        format!(
            "ℹ️  none of the 3 changed file(s) belong to a cargo crate — nothing to run.\n{MARKER}{}",
            r#"{"schemaVersion":1,"parser":"nextest","complete":true,"selection":"empty","tests":[],"undeclaredSkips":0}"#
        )
    }

    fn full_run_output() -> String {
        format!(
            "Summary [ 400.000s] 3227 tests run: 3227 passed\n{MARKER}{}",
            r#"{"schemaVersion":1,"parser":"nextest","complete":true,"selection":"full","tests":[],"undeclaredSkips":0}"#
        )
    }

    fn loaded(meta: CellExecutionMeta) -> CellExecutionMeta {
        CellExecutionMeta {
            load_context: Some(ExecutionLoadContext {
                co_resident_compile_jobs: 2,
                co_resident_light_jobs: 0,
                cpu_utilization_millis: None,
                io_pressure_full_millis: None,
                memory_pressure_full_millis: None,
                host_stall_stale: false,
            }),
            ..meta
        }
    }

    #[test]
    fn a_completion_is_classified_by_its_exit_first_and_its_declared_selection_second() {
        let filtered = full_run_output().replace("\"full\"", "\"filtered\"");
        let cases = [
            (true, "ok".to_string(), CompletionKind::Succeeded),
            (true, no_op_output(), CompletionKind::SelectedNothing),
            (true, full_run_output(), CompletionKind::Succeeded),
            (true, filtered, CompletionKind::Succeeded),
            // A nextest filterset that matched nothing exits 4 and still compiled
            // what it selected; its memory is worth keeping.
            (false, no_op_output(), CompletionKind::Failed),
            (
                true,
                format!("{MARKER}{{\"schemaVersion\":1,\"selection\":"),
                CompletionKind::Succeeded,
            ),
            // Only the structured declaration counts, never the prose.
            (
                true,
                "ℹ️  nothing to run.".to_string(),
                CompletionKind::Succeeded,
            ),
        ];
        for (succeeded, output, expected) in cases {
            assert_eq!(
                CompletionKind::classify(succeeded, &output),
                expected,
                "succeeded={succeeded} output={output:?}"
            );
        }
    }

    /// A build that died four seconds in measured how fast this machine fails,
    /// not how long the suite takes. Its memory peak was real demand, though,
    /// and the reservation keeps it.
    #[tokio::test]
    async fn a_failed_completion_never_teaches_the_predictor() {
        let db = Arc::new(crate::storage::migrated_test_db("duration-failed.db").await);
        let identity = duration_identity();
        let context = duration_context();
        let warmth = ExecutionWarmth::PreparedWarmSlot;
        for finished in 1..=3 {
            observe_completed(
                db.clone(),
                Some(&identity),
                &context,
                &completion(Some(warmth), finished, 400_000),
                CompletionKind::Succeeded,
            )
            .await;
        }
        let before = load_duration_profile(db.clone(), &identity, &context, warmth)
            .await
            .unwrap()
            .unwrap();

        for finished in 4..=8 {
            let mut failed = completion(Some(warmth), finished, 4_000);
            failed.peak_rss_bytes = Some(500);
            observe_completed(
                db.clone(),
                Some(&identity),
                &context,
                &failed,
                CompletionKind::Failed,
            )
            .await;
        }

        assert_eq!(
            load_duration_profile(db.clone(), &identity, &context, warmth)
                .await
                .unwrap()
                .unwrap(),
            before,
            "a failure must not enter the duration window"
        );
        let predicted = resolve_duration(
            db.clone(),
            Some(&identity),
            &context,
            warmth,
            CellCommandClass::CargoTest,
            10,
        )
        .await;
        assert_eq!(predicted.predicted_ms, 400_000);
        let profile = load_profile(db, &identity, &context)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(profile.sample_count, 8, "memory was learned from every run");
        assert_eq!(profile.upper_peak_rss_bytes, Some(500));
        assert_eq!(
            profile.upper_duration_ms,
            Some(400_000),
            "the duration high-water mark does not decay toward a failure"
        );
    }

    /// A lane that tested nothing is not a sample of anything about the work
    /// class, including its memory: recorded, each would decay the high-water
    /// mark toward the footprint of a script deciding to do nothing.
    #[tokio::test]
    async fn a_lane_that_selected_nothing_teaches_nothing() {
        let db = Arc::new(crate::storage::migrated_test_db("duration-no-op.db").await);
        let identity = duration_identity();
        let context = duration_context();
        let warmth = ExecutionWarmth::PreparedWarmSlot;
        let idle = ExecutionLoadContext::default();
        for finished in 1..=3 {
            observe_completed(
                db.clone(),
                Some(&identity),
                &context,
                &completion(Some(warmth), finished, 400_000),
                CompletionKind::Succeeded,
            )
            .await;
        }
        let snapshot = |db: Arc<LocalDb>| {
            let identity = identity.clone();
            let context = context.clone();
            let idle = idle.clone();
            async move {
                (
                    load_duration_profile(db.clone(), &identity, &context, warmth)
                        .await
                        .unwrap(),
                    load_profile(db.clone(), &identity, &context).await.unwrap(),
                    load_contention_profile(db.clone(), &context.executor_class, &idle)
                        .await
                        .unwrap(),
                    load_contention_profile(db, "*", &idle).await.unwrap(),
                )
            }
        };
        let before = snapshot(db.clone()).await;
        assert!(before.0.is_some() && before.1.is_some() && before.2.is_some());

        for finished in 4..=13 {
            let mut no_op = completion(Some(warmth), finished, 250);
            no_op.peak_rss_bytes = Some(1);
            observe_completed(
                db.clone(),
                Some(&identity),
                &context,
                &no_op,
                CompletionKind::SelectedNothing,
            )
            .await;
        }

        assert_eq!(snapshot(db).await, before);
    }

    /// The specimen: bbs's window for a full-suite lane, replayed in order.
    /// Every sub-second sample was a lane that selected nothing, and 418,205 ms
    /// was a run that failed. The old recorder kept all sixteen and predicted
    /// 251 ms; this one keeps only the runs that did the work.
    #[tokio::test]
    async fn the_recorder_never_builds_a_bimodal_window() {
        let db = Arc::new(crate::storage::migrated_test_db("duration-bimodal.db").await);
        let identity = duration_identity();
        let context = duration_context();
        let warmth = ExecutionWarmth::PreparedWarmSlot;
        const FAILED: u64 = 418_205;
        let observed = [
            52_834, 231, 218, 293, 251, 279_771, FAILED, 400_375, 141_267, 182_395, 305_829,
            294_437, 207, 202, 200, 198,
        ];
        for (finished, duration) in (1..).zip(observed) {
            let (succeeded, output) = match duration {
                FAILED => (
                    false,
                    "error: failed to run custom build command for `cmake`".into(),
                ),
                ms if ms < 1_000 => (true, no_op_output()),
                _ => (true, full_run_output()),
            };
            observe_completed(
                db.clone(),
                Some(&identity),
                &context,
                &completion(Some(warmth), finished, duration),
                CompletionKind::classify(succeeded, &output),
            )
            .await;
        }

        let stored = load_duration_profile(db.clone(), &identity, &context, warmth)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.recent_ms,
            vec![52_834, 279_771, 400_375, 141_267, 182_395, 305_829, 294_437]
        );
        let predicted = resolve_duration(
            db,
            Some(&identity),
            &context,
            warmth,
            CellCommandClass::CargoTest,
            20,
        )
        .await;
        assert!(predicted.is_learned());
        assert_eq!(
            predicted.predicted_ms, 279_771,
            "the real mode, not sub-second"
        );
    }

    /// Contention divides a loaded run's duration by the idle baseline, so a
    /// failure under load would record how much slower failing is than
    /// succeeding. It records nothing; a success under the same load does.
    #[tokio::test]
    async fn a_failed_run_under_load_records_no_contention_sample() {
        let db = Arc::new(crate::storage::migrated_test_db("contention-failed.db").await);
        let identity = duration_identity();
        let context = duration_context();
        let warmth = ExecutionWarmth::PreparedWarmSlot;
        for finished in 1..=3 {
            observe_completed(
                db.clone(),
                Some(&identity),
                &context,
                &completion(Some(warmth), finished, 400_000),
                CompletionKind::Succeeded,
            )
            .await;
        }
        let under_load = loaded(completion(Some(warmth), 4, 4_000));
        let load = under_load.load_context.clone().unwrap();
        observe_completed(
            db.clone(),
            Some(&identity),
            &context,
            &under_load,
            CompletionKind::Failed,
        )
        .await;
        for key in [context.executor_class.as_str(), "*"] {
            assert!(
                load_contention_profile(db.clone(), key, &load)
                    .await
                    .unwrap()
                    .is_none(),
                "{key}: a failure under load is not a slowdown sample"
            );
        }

        observe_completed(
            db.clone(),
            Some(&identity),
            &context,
            &loaded(completion(Some(warmth), 5, 800_000)),
            CompletionKind::Succeeded,
        )
        .await;
        let machine = load_contention_profile(db, &context.executor_class, &load)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(machine.recent_multiplier_millis, vec![2_000]);
    }

    fn under_stall(
        mut meta: CellExecutionMeta,
        co_resident_compile_jobs: u32,
        io_pressure_full_millis: Option<u32>,
    ) -> CellExecutionMeta {
        meta.load_context = Some(ExecutionLoadContext {
            co_resident_compile_jobs,
            io_pressure_full_millis,
            ..ExecutionLoadContext::default()
        });
        meta
    }

    /// The stall a run began under is divided out of what it teaches, by the
    /// curve placement prices stall with, so a machine is charged for it once:
    /// from the reading current when work is placed. At bbs's 10-15 reading
    /// (23% of the time fully stalled on I/O), three idle 10 s runs teach a
    /// 7.7 s baseline, which a placement at the same stall prices back to
    /// 10 s. The same runs on a machine with no stall reading teach 10 s: the
    /// count-only model, unchanged, and unmeasured never read as unstalled.
    #[tokio::test]
    async fn a_stalled_run_teaches_the_speed_the_machine_has_unstalled() {
        let db = Arc::new(crate::storage::migrated_test_db("duration-destalled.db").await);
        let identity = duration_identity();
        let warmth = ExecutionWarmth::PreparedWarmSlot;
        let measuring = duration_context();
        let unmeasuring = ProfileContext {
            executor_class: "mac-device:mac".into(),
            os: "macos".into(),
            ..duration_context()
        };
        for finished in 1..=3 {
            for (context, stall) in [(&measuring, Some(230)), (&unmeasuring, None)] {
                observe_completed(
                    db.clone(),
                    Some(&identity),
                    context,
                    &under_stall(completion(Some(warmth), finished, 10_000), 0, stall),
                    CompletionKind::Succeeded,
                )
                .await;
            }
        }
        let stalled = resolve_duration(
            db.clone(),
            Some(&identity),
            &measuring,
            warmth,
            CellCommandClass::CargoTest,
            10,
        )
        .await;
        let unmeasured = resolve_duration(
            db.clone(),
            Some(&identity),
            &unmeasuring,
            warmth,
            CellCommandClass::CargoTest,
            10,
        )
        .await;
        assert!(stalled.is_learned() && unmeasured.is_learned());
        assert_eq!(stalled.predicted_ms, 7_700);
        assert_eq!(unmeasured.predicted_ms, 10_000);
        assert_eq!(
            stalled.predicted_ms + stall_penalty_ms(stalled.predicted_ms, 230),
            10_000,
            "priced at the stall it was learned under, it is the run it was"
        );
        let profile = load_profile(db.clone(), &identity, &measuring)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            profile.upper_duration_ms,
            Some(10_000),
            "the reservation covers the run as it came, stall included"
        );

        // Beside two cargo lanes at the same stall, a 20 s run: 15.4 s of it
        // was the lanes, a 2x slowdown, and the rest was the disk.
        observe_completed(
            db.clone(),
            Some(&identity),
            &measuring,
            &under_stall(completion(Some(warmth), 4, 20_000), 2, Some(230)),
            CompletionKind::Succeeded,
        )
        .await;
        let load = ExecutionLoadContext {
            co_resident_compile_jobs: 2,
            ..ExecutionLoadContext::default()
        };
        let machine = load_contention_profile(db, &measuring.executor_class, &load)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(machine.recent_multiplier_millis, vec![2_000]);
    }

    /// A run that began on a stale stall reading cannot have the stall divided
    /// out, so it teaches no speed: neither a baseline nor a slowdown. Its
    /// memory was real demand, and the reservation keeps it.
    #[tokio::test]
    async fn a_run_begun_on_a_stale_stall_reading_teaches_no_speed() {
        let db = Arc::new(crate::storage::migrated_test_db("duration-stale-stall.db").await);
        let identity = duration_identity();
        let context = duration_context();
        let warmth = ExecutionWarmth::PreparedWarmSlot;
        for finished in 1..=3 {
            let mut meta = completion(Some(warmth), finished, 10_000);
            meta.load_context = Some(ExecutionLoadContext {
                host_stall_stale: true,
                ..ExecutionLoadContext::default()
            });
            observe_completed(
                db.clone(),
                Some(&identity),
                &context,
                &meta,
                CompletionKind::Succeeded,
            )
            .await;
        }
        assert!(
            load_duration_profile(db.clone(), &identity, &context, warmth)
                .await
                .unwrap()
                .is_none()
        );
        assert!(load_contention_profile(
            db.clone(),
            &context.executor_class,
            &ExecutionLoadContext::default()
        )
        .await
        .unwrap()
        .is_none());
        let profile = load_profile(db, &identity, &context)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(profile.sample_count, 3);
    }

    /// The rule exploration asks before spending a lane: only a run alone on
    /// the machine, on a current stall reading, becomes a baseline sample. A
    /// stall is divided out, so it does not disqualify the run.
    /// A run's expectation is the speed learned alone, priced for the work it
    /// began beside, and nothing at all before that speed is learned or when
    /// the run made no warmth claim.
    #[tokio::test]
    async fn a_runs_expectation_is_its_learned_speed_priced_for_its_start() {
        let db = Arc::new(crate::storage::migrated_test_db("duration-expectation.db").await);
        let identity = duration_identity();
        let context = duration_context();
        let warmth = ExecutionWarmth::PreparedWarmSlot;
        let starting = |load: ExecutionLoadContext| {
            let mut meta = completion(Some(warmth), 10, 1);
            meta.started_at_unix_ms = 10;
            meta.load_context = Some(load);
            meta
        };
        let alone = ExecutionLoadContext::default();
        assert_eq!(
            learned_expectation(db.clone(), &identity, &context, &starting(alone.clone())).await,
            None,
            "nothing learned yet"
        );
        for finished in 1..=3 {
            observe_completed(
                db.clone(),
                Some(&identity),
                &context,
                &completion(Some(warmth), finished, 30_000),
                CompletionKind::Succeeded,
            )
            .await;
        }
        assert_eq!(
            learned_expectation(db.clone(), &identity, &context, &starting(alone)).await,
            Some(30_000)
        );
        let beside_two_compiles = ExecutionLoadContext {
            co_resident_compile_jobs: 2,
            ..ExecutionLoadContext::default()
        };
        let prior = contention_prior(&beside_two_compiles).multiplier_millis;
        assert_eq!(
            learned_expectation(
                db.clone(),
                &identity,
                &context,
                &starting(beside_two_compiles)
            )
            .await,
            Some(30_000 * u64::from(prior) / 1_000),
            "priced by the slowdown placement prices that load with"
        );
        let mut unclaimed = starting(ExecutionLoadContext::default());
        unclaimed.warmth = None;
        assert_eq!(
            learned_expectation(db, &identity, &context, &unclaimed).await,
            None
        );
    }

    #[test]
    fn only_a_run_alone_on_a_current_reading_teaches_a_baseline() {
        assert!(teaches_baseline(&ExecutionLoadContext::default()));
        assert!(teaches_baseline(&ExecutionLoadContext {
            io_pressure_full_millis: Some(474),
            ..ExecutionLoadContext::default()
        }));
        assert!(!teaches_baseline(&ExecutionLoadContext {
            co_resident_compile_jobs: 15,
            co_resident_light_jobs: 1,
            io_pressure_full_millis: Some(474),
            ..ExecutionLoadContext::default()
        }));
        assert!(!teaches_baseline(&ExecutionLoadContext {
            host_stall_stale: true,
            ..ExecutionLoadContext::default()
        }));
    }
}
