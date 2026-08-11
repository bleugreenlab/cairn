use super::{ResourceReservation, ResourceReservationSource};
use crate::storage::LocalDb;
use cairn_common::executor_protocol::{
    CellCommandClass, CellExecutionMeta, CommandResourceIdentity, ContentionEstimate,
    ContentionEvidence, ContentionFallback, DurationEstimate, DurationEvidence, DurationFallback,
    ExecutionLoadContext, ExecutionWarmth, ExecutorCapabilities, MeasurementQuality,
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
                    multiplier_millis: median_ms(&profile.recent_multiplier_millis)
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
    let memory = capabilities.memory_budget_bytes.map_or(memory, |budget| {
        let cap = budget.saturating_mul(MEMORY_RESERVATION_CAP_NUMERATOR)
            / MEMORY_RESERVATION_CAP_DENOMINATOR;
        memory.min(cap)
    });
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
        predicted_ms: median_ms(&profile.recent_ms),
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
fn median_ms(samples: &[u64]) -> u64 {
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
    let rationale = |fallback: Option<ReservationFallback>, profile: Option<&ResourceProfile>| {
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
    };
    let Some(identity) = identity else {
        return ResolvedResourceProfile {
            reservation: prior.clone(),
            learned_estimate: None,
            rationale: rationale(Some(ReservationFallback::NoCommandIdentity), None),
            duration,
            item_durations: Vec::new(),
        };
    };
    // A store that could not be read and a work class that has never run are
    // both "no learned number", and they call for different responses: one is a
    // cold start, the other is a fault worth seeing on the decision record.
    let profile = match load_profile(db, identity, context).await {
        Ok(Some(profile)) => profile,
        Ok(None) => {
            return ResolvedResourceProfile {
                reservation: prior.clone(),
                learned_estimate: None,
                rationale: rationale(Some(ReservationFallback::NoProfileRecorded), None),
                duration,
                item_durations: Vec::new(),
            }
        }
        Err(_) => {
            return ResolvedResourceProfile {
                reservation: prior.clone(),
                learned_estimate: None,
                rationale: rationale(Some(ReservationFallback::ProfileLookupFailed), None),
                duration,
                item_durations: Vec::new(),
            }
        }
    };
    let below_floor = profile.sample_count < MIN_CONFIDENT_SAMPLES;
    let memory_quarantined = memory_budget_bytes.is_some_and(|budget| {
        profile.upper_peak_rss_bytes.is_some_and(|peak| {
            peak.saturating_mul(MEMORY_QUARANTINE_DENOMINATOR)
                >= budget.saturating_mul(MEMORY_QUARANTINE_NUMERATOR)
        })
    });
    ResolvedResourceProfile {
        reservation: reservation_for_profile(
            &profile,
            prior.clone(),
            memory_budget_bytes,
            memory_quarantined,
        ),
        learned_estimate: Some(cairn_common::executor_protocol::LearnedResourceEstimate {
            sample_count: profile.sample_count,
            upper_duration_ms: profile.upper_duration_ms,
            upper_peak_rss_bytes: profile.upper_peak_rss_bytes,
            upper_disk_growth_bytes: profile.upper_disk_delta_bytes,
        }),
        rationale: rationale(
            if memory_quarantined {
                Some(ReservationFallback::MemoryObservationQuarantined)
            } else {
                below_floor.then_some(ReservationFallback::BelowConfidenceFloor)
            },
            Some(&profile),
        ),
        duration,
        item_durations: Vec::new(),
    }
}

/// Fold one real completion into what this machine has learned.
///
/// The only writer. Nothing else may put a number into these profiles — in
/// particular a reused check-result observation must not, because its duration
/// belongs to the execution that originally produced it, on whatever machine
/// that was, and recording it here would teach the fleet that a cache hit is how
/// fast that machine runs the suite.
pub(super) async fn observe_completed(
    db: Arc<LocalDb>,
    identity: Option<&CommandResourceIdentity>,
    context: &ProfileContext,
    metadata: &CellExecutionMeta,
) {
    let Some(identity) = identity else { return };
    let Some(quality) = metadata.measurement_quality.as_ref() else {
        return;
    };
    let duration = (quality.duration != MeasurementQuality::Unavailable)
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

fn reservation_for_profile(
    profile: &ResourceProfile,
    prior: ResourceReservation,
    memory_budget_bytes: Option<u64>,
    memory_quarantined: bool,
) -> ResourceReservation {
    let learned_memory = (!memory_quarantined)
        .then_some(profile.upper_peak_rss_bytes)
        .flatten()
        .map(with_headroom);
    let learned_disk = profile.upper_disk_delta_bytes.map(with_headroom);
    let (memory_bytes, disk_growth_bytes) = if profile.sample_count < MIN_CONFIDENT_SAMPLES {
        // Thin evidence cannot justify predictive headroom, but it is still a
        // measured high-water mark and admission must not ignore it. Preserve
        // the larger of the prior and the raw observation until enough samples
        // exist to apply the learned estimate with headroom.
        (
            if memory_quarantined {
                prior.memory_bytes
            } else {
                profile
                    .upper_peak_rss_bytes
                    .map_or(prior.memory_bytes, |value| prior.memory_bytes.max(value))
            },
            profile
                .upper_disk_delta_bytes
                .map_or(prior.disk_growth_bytes, |value| {
                    prior.disk_growth_bytes.max(value)
                }),
        )
    } else {
        (
            learned_memory.unwrap_or(prior.memory_bytes),
            learned_disk.unwrap_or(prior.disk_growth_bytes),
        )
    };
    let memory_bytes = memory_budget_bytes.map_or(memory_bytes, |budget| {
        let cap = budget.saturating_mul(MEMORY_RESERVATION_CAP_NUMERATOR)
            / MEMORY_RESERVATION_CAP_DENOMINATOR;
        memory_bytes.min(cap)
    });
    ResourceReservation {
        memory_bytes,
        disk_growth_bytes,
        concurrency_units: prior.concurrency_units,
        source: ResourceReservationSource::Learned,
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
            toolchains: vec!["rust".into()],
            projects_served: Vec::new(),
            disk_budget_bytes: None,
            memory_budget_bytes: None,
            toolchain_detection: None,
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

    /// A profile is keyed by the machine context it was learned on, so an
    /// observation from one platform can never be read as evidence about
    /// another.
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
            disk_delta_bytes: Some(20),
            measurement_quality: None,
            environment_fingerprint: String::new(),
            verdict_platform: None,
            verdict_arch: None,
            verdict_environment_hash: None,
            toolchain_fingerprint: None,
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
            toolchain_fingerprint: None,
        };

        observe_completed(db.clone(), Some(&identity), &context, &metadata).await;
        observe_completed(db.clone(), Some(&identity), &context, &metadata).await;
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
            toolchain_fingerprint: None,
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
            )
            .await;
            observe_completed(
                db.clone(),
                Some(&identity),
                &context,
                &completion(Some(ExecutionWarmth::PreparedWarmSlot), finished, 20_000),
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
            )
            .await;
        }
        observe_completed(
            db.clone(),
            Some(&identity),
            &context,
            &completion(Some(ExecutionWarmth::Cold), 3, 2_400_000),
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
}
