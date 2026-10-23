use super::{ResourceReservation, ResourceReservationSource};
use crate::storage::LocalDb;
use cairn_common::executor_protocol::{
    CellCommandClass, CellExecutionMeta, CommandResourceIdentity, ContentionEstimate,
    ContentionEvidence, ContentionFallback, DurationEstimate, DurationEvidence, DurationFallback,
    ExecutionLoadContext, ExecutionWarmth, ExecutorCapabilities, MeasurementQuality, MemoryCeiling,
    ReservationFallback, ReservationRationale, DURATION_PROFILE_STALE_AFTER_MS,
    DURATION_SAMPLE_WINDOW, MIN_CONFIDENT_DURATION_SAMPLES, MIN_CONFIDENT_RESERVATION_SAMPLES,
};
use cairn_db::turso::params;
use std::sync::Arc;

const MIN_CONFIDENT_SAMPLES: u64 = MIN_CONFIDENT_RESERVATION_SAMPLES;
const HEADROOM_NUMERATOR: u64 = 5;
const HEADROOM_DENOMINATOR: u64 = 4;
const HEADROOM_PERCENT: u32 = 25;
const MIN_CONTENTION_SAMPLES: usize = 3;
/// A command reservation must leave room for the operating system, executor,
/// and measurement noise. Peaks at or above this share describe saturation.
const MEMORY_QUARANTINE_NUMERATOR: u64 = 9;
const MEMORY_QUARANTINE_DENOMINATOR: u64 = 10;
const MEMORY_RESERVATION_CAP_NUMERATOR: u64 = 3;
const MEMORY_RESERVATION_CAP_DENOMINATOR: u64 = 4;

/// The most one request may reserve on a machine with this memory budget.
fn memory_reservation_cap(memory_budget_bytes: u64) -> u64 {
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
    })
}

/// What the rest of the fleet measured this command needing, with headroom,
/// judged against nothing: the demand as the machines that ran it saw it. For
/// fit only; a reservation borrows through [`fleet_memory_demand`].
fn fleet_portable_demand(evidence: &FleetEvidence) -> Option<u64> {
    (!evidence.peaks.is_empty()).then(|| with_headroom(upper_median(&evidence.peaks)))
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
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResourceProfile {
    sample_count: u64,
    upper_peak_rss_bytes: Option<u64>,
    upper_disk_delta_bytes: Option<u64>,
    upper_duration_ms: Option<u64>,
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
        };
    };
    // A store that could not be read and a work class that has never run are
    // both "no learned number", and they call for different responses: one is a
    // cold start, the other is a fault worth seeing on the decision record.
    let local = load_profile(db.clone(), identity, context).await;
    // The fleet is consulted only where this machine is still cold or thinly
    // evidenced, which is the only place a borrowed number could change an
    // answer. A machine that has measured this command past the confidence floor
    // speaks for itself and pays for no extra query.
    let cold_here =
        !matches!(&local, Ok(Some(profile)) if profile.sample_count >= MIN_CONFIDENT_SAMPLES);
    let fleet_evidence = match cold_here {
        false => None,
        true => load_fleet_evidence(db, identity, context).await.ok(),
    };
    let borrowed = fleet_evidence
        .as_ref()
        .and_then(|evidence| fleet_prior(evidence, &prior, memory_budget_bytes));
    // Read beside the prior rather than from it, because the prior is already
    // clamped, may have been discarded as no larger than the class prior, and is
    // quarantined against this machine's budget: none of those says what the
    // fleet measured. The quarantine is right for a reservation and wrong for a
    // fit. A borrowed peak was measured on machines it did not saturate, so one
    // that would nearly consume this machine is evidence this machine is too
    // small, not that this machine was saturated.
    let borrowed_demand = fleet_evidence.as_ref().and_then(fleet_portable_demand);
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
                memory_fit: fit_for(borrowed_demand),
            }
        }
        Err(_) => {
            return ResolvedResourceProfile {
                reservation: prior.clone(),
                learned_estimate: None,
                rationale: rationale(Some(ReservationFallback::ProfileLookupFailed), None),
                duration,
                item_durations: Vec::new(),
                memory_fit: fit_for(borrowed_demand),
            }
        }
    };
    let below_floor = profile.sample_count < MIN_CONFIDENT_SAMPLES;
    let memory_quarantined = profile
        .upper_peak_rss_bytes
        .is_some_and(|peak| memory_peak_quarantined(peak, memory_budget_bytes));
    let reservation = reservation_for_profile(
        &profile,
        prior.clone(),
        memory_budget_bytes,
        memory_quarantined,
    );
    ResolvedResourceProfile {
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
        // Whichever measurement the reservation stood on: this machine's own,
        // or, while that is still thin, the fleet's.
        // A quarantined own peak measured this machine's saturation, so it says
        // nothing about fit and leaves it unknown unless the fleet says more.
        memory_fit: fit_for(
            measured_memory_demand(&profile, memory_quarantined).max(borrowed_demand),
        ),
    }
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
    if let (Some(duration), Some(warmth), Some(load)) =
        (duration, metadata.warmth, metadata.load_context.as_ref())
    {
        let idle = load.co_resident_compile_jobs == 0 && load.co_resident_light_jobs == 0;
        let baseline = resolve_duration(
            db.clone(),
            Some(identity),
            context,
            warmth,
            CellCommandClass::Other,
            metadata.finished_at_unix_ms,
        )
        .await;
        if idle {
            let _ = record_duration(
                db.clone(),
                identity,
                context,
                warmth,
                metadata.finished_at_unix_ms,
                duration,
            )
            .await;
            let _ = record_contention(
                db.clone(),
                context,
                load,
                metadata.finished_at_unix_ms,
                1_000,
            )
            .await;
        } else if baseline.is_learned() && baseline.predicted_ms > 0 {
            let multiplier = duration.saturating_mul(1_000) / baseline.predicted_ms;
            let _ = record_contention(
                db.clone(),
                context,
                load,
                metadata.finished_at_unix_ms,
                multiplier.max(1_000),
            )
            .await;
        }
    }
    let _ = update_profile(
        db,
        identity,
        context,
        metadata.finished_at_unix_ms,
        memory,
        disk,
        duration,
    )
    .await;
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

fn with_headroom(value: u64) -> u64 {
    value.saturating_mul(HEADROOM_NUMERATOR) / HEADROOM_DENOMINATOR
}

#[cfg(test)]
fn update_upper(previous: u64, sample: u64) -> u64 {
    if previous == 0 || sample >= previous {
        sample
    } else {
        // Slowly decay a high-water estimate while remaining biased upward.
        previous.saturating_sub((previous - sample) / 32)
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
                "SELECT sample_count, upper_peak_rss_bytes, upper_disk_delta_bytes, upper_duration_ms
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

async fn update_profile(
    db: Arc<LocalDb>,
    identity: &CommandResourceIdentity,
    context: &ProfileContext,
    finished: u64,
    memory: Option<u64>,
    disk: Option<u64>,
    duration: Option<u64>,
) -> Result<(), String> {
    let identity = identity.clone();
    let context = context.clone();
    db.write(|conn| {
        let identity = identity.clone();
        let context = context.clone();
        Box::pin(async move {
            conn.execute(
                r#"INSERT INTO command_resource_profiles (identity_version, command_identity, executor_class, os, arch, toolchain_fingerprint, sample_count, updated_at_unix_ms, upper_peak_rss_bytes, upper_disk_delta_bytes, upper_duration_ms, confidence_millis)
                   VALUES (?1,?2,?3,?4,?5,?6,1,?7,?8,?9,?10,200)
                   ON CONFLICT(identity_version, command_identity, executor_class, os, arch, toolchain_fingerprint) DO UPDATE SET
                     sample_count=MIN(command_resource_profiles.sample_count + 1, 10000),
                     updated_at_unix_ms=MAX(command_resource_profiles.updated_at_unix_ms, excluded.updated_at_unix_ms),
                     upper_peak_rss_bytes=CASE WHEN excluded.upper_peak_rss_bytes IS NULL THEN command_resource_profiles.upper_peak_rss_bytes WHEN command_resource_profiles.upper_peak_rss_bytes IS NULL OR excluded.upper_peak_rss_bytes >= command_resource_profiles.upper_peak_rss_bytes THEN excluded.upper_peak_rss_bytes ELSE command_resource_profiles.upper_peak_rss_bytes - ((command_resource_profiles.upper_peak_rss_bytes - excluded.upper_peak_rss_bytes) / 32) END,
                     upper_disk_delta_bytes=CASE WHEN excluded.upper_disk_delta_bytes IS NULL THEN command_resource_profiles.upper_disk_delta_bytes WHEN command_resource_profiles.upper_disk_delta_bytes IS NULL OR excluded.upper_disk_delta_bytes >= command_resource_profiles.upper_disk_delta_bytes THEN excluded.upper_disk_delta_bytes ELSE command_resource_profiles.upper_disk_delta_bytes - ((command_resource_profiles.upper_disk_delta_bytes - excluded.upper_disk_delta_bytes) / 32) END,
                     upper_duration_ms=CASE WHEN excluded.upper_duration_ms IS NULL THEN command_resource_profiles.upper_duration_ms WHEN command_resource_profiles.upper_duration_ms IS NULL OR excluded.upper_duration_ms >= command_resource_profiles.upper_duration_ms THEN excluded.upper_duration_ms ELSE command_resource_profiles.upper_duration_ms - ((command_resource_profiles.upper_duration_ms - excluded.upper_duration_ms) / 32) END,
                     confidence_millis=MIN(1000, (MIN(command_resource_profiles.sample_count + 1, 10000) * 1000) / 5)"#,
                params![identity.version as i64, identity.key, context.executor_class, context.os, context.arch, context.toolchain_fingerprint, finished as i64, memory.map(|value| value as i64), disk.map(|value| value as i64), duration.map(|value| value as i64)],
            ).await?;
            Ok(())
        })
    }).await.map_err(|error| error.to_string())
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
            };
            for sample_count in [0, 1, 4, MIN_CONFIDENT_SAMPLES, 500] {
                let reservation = reservation_for_profile(
                    &ResourceProfile {
                        sample_count,
                        upper_peak_rss_bytes: Some(10_000),
                        upper_disk_delta_bytes: Some(20_000),
                        upper_duration_ms: Some(480_000),
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
        };
        for sample_count in 0..MIN_CONFIDENT_SAMPLES {
            let smaller = reservation_for_profile(
                &ResourceProfile {
                    sample_count,
                    upper_peak_rss_bytes: Some(1),
                    upper_disk_delta_bytes: Some(2),
                    upper_duration_ms: Some(3),
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
            },
            ResourceReservation {
                memory_bytes: 100,
                disk_growth_bytes: 200,
                concurrency_units: 1,
                source: ResourceReservationSource::Unmeasured,
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
        };
        let profile = |upper_peak_rss_bytes| ResourceProfile {
            sample_count: 50,
            upper_peak_rss_bytes,
            upper_disk_delta_bytes: Some(20),
            upper_duration_ms: Some(30),
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

    #[tokio::test]
    async fn concurrent_observations_increment_without_lost_updates() {
        let db =
            Arc::new(crate::storage::migrated_test_db("resource-profile-concurrency.db").await);
        let identity = CommandResourceIdentity {
            version: 1,
            key: "identity".into(),
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

    /// A machine with no history of its own is judged on what the fleet
    /// measured, before this machine's cap discards the difference.
    #[tokio::test]
    async fn a_borrowed_demand_above_this_machines_cap_is_a_ceiling_here() {
        let db = Arc::new(
            crate::storage::migrated_test_db("resource-profile-ceiling-borrowed.db").await,
        );
        let identity = CommandResourceIdentity {
            version: 1,
            key: "test:rust".into(),
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
        assert_eq!(
            resolved.memory_fit,
            MemoryFit::Exceeds(MemoryCeiling {
                estimate_bytes: with_headroom(8 * GIB),
                ceiling_bytes: SPECIMEN_CAP,
            })
        );
    }

    #[test]
    fn machine_memory_ceiling_never_yields_to_a_larger_prior() {
        let resolved = reservation_for_profile(
            &ResourceProfile {
                sample_count: MIN_CONFIDENT_SAMPLES,
                upper_peak_rss_bytes: Some(99),
                upper_disk_delta_bytes: None,
                upper_duration_ms: None,
            },
            ResourceReservation {
                memory_bytes: 100,
                disk_growth_bytes: 10,
                concurrency_units: 1,
                source: ResourceReservationSource::Unmeasured,
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
}
