use super::{ResourceReservation, ResourceReservationSource};
use crate::storage::LocalDb;
use cairn_common::executor_protocol::{
    CellCommandClass, CellExecutionMeta, CommandResourceIdentity, ExecutorCapabilities,
    MeasurementQuality, ReservationFallback, ReservationRationale,
    MIN_CONFIDENT_RESERVATION_SAMPLES,
};
use cairn_db::turso::params;
use std::sync::Arc;

const MIN_CONFIDENT_SAMPLES: u64 = MIN_CONFIDENT_RESERVATION_SAMPLES;
const HEADROOM_NUMERATOR: u64 = 5;
const HEADROOM_DENOMINATOR: u64 = 4;
const HEADROOM_PERCENT: u32 = 25;

#[derive(Clone)]
pub(super) struct ProfileContext {
    pub executor_class: String,
    pub os: String,
    pub arch: String,
    pub toolchain_fingerprint: String,
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
    // A prior may never exceed what the machine has, or it could never be
    // admitted at all: an unschedulable safety margin is not a safety margin.
    let share = |budget: Option<u64>, floor: u64| budget.map_or(floor, |budget| floor.min(budget));
    ResourceReservation {
        memory_bytes: share(capabilities.memory_budget_bytes, memory),
        disk_growth_bytes: share(capabilities.disk_budget_bytes, disk),
        // One unit is the cold-start prior for every class. Deriving more from a
        // CPU percentage would turn an observation about how hard a machine was
        // pushed into a claim about how many lanes this work needs, which is a
        // different question with no evidence behind it yet.
        concurrency_units: 1,
        source: ResourceReservationSource::Unmeasured,
    }
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

pub(super) async fn resolve_reservation(
    db: Arc<LocalDb>,
    identity: Option<&CommandResourceIdentity>,
    context: &ProfileContext,
    prior: ResourceReservation,
) -> ResolvedResourceProfile {
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
            }
        }
        Err(_) => {
            return ResolvedResourceProfile {
                reservation: prior.clone(),
                learned_estimate: None,
                rationale: rationale(Some(ReservationFallback::ProfileLookupFailed), None),
            }
        }
    };
    let below_floor = profile.sample_count < MIN_CONFIDENT_SAMPLES;
    ResolvedResourceProfile {
        reservation: reservation_for_profile(&profile, prior.clone()),
        learned_estimate: Some(cairn_common::executor_protocol::LearnedResourceEstimate {
            sample_count: profile.sample_count,
            upper_duration_ms: profile.upper_duration_ms,
            upper_peak_rss_bytes: profile.upper_peak_rss_bytes,
            upper_disk_growth_bytes: profile.upper_disk_delta_bytes,
        }),
        rationale: rationale(
            below_floor.then_some(ReservationFallback::BelowConfidenceFloor),
            Some(&profile),
        ),
    }
}

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
) -> ResourceReservation {
    let learned_memory = profile.upper_peak_rss_bytes.map(with_headroom);
    let learned_disk = profile.upper_disk_delta_bytes.map(with_headroom);
    let (memory_bytes, disk_growth_bytes) = if profile.sample_count < MIN_CONFIDENT_SAMPLES {
        (
            learned_memory.map_or(prior.memory_bytes, |value| prior.memory_bytes.max(value)),
            learned_disk.map_or(prior.disk_growth_bytes, |value| {
                prior.disk_growth_bytes.max(value)
            }),
        )
    } else {
        (
            learned_memory.unwrap_or(prior.memory_bytes),
            learned_disk.unwrap_or(prior.disk_growth_bytes),
        )
    };
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
        assert_eq!(prior.memory_bytes, 256 * 1024 * 1024);
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

        let anonymous = resolve_reservation(db.clone(), None, &context, prior.clone()).await;
        assert_eq!(
            anonymous.rationale.fallback,
            Some(ReservationFallback::NoCommandIdentity)
        );
        assert_eq!(anonymous.reservation, prior);

        let identity = CommandResourceIdentity {
            version: 1,
            key: "check:rust".into(),
        };
        let unseen =
            resolve_reservation(db.clone(), Some(&identity), &context, prior.clone()).await;
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
        let thin = resolve_reservation(db, Some(&identity), &context, prior.clone()).await;
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
                );
                assert_eq!(
                    reservation.concurrency_units, concurrency_units,
                    "{sample_count} observations must not restate the caller's lane demand"
                );
            }
        }
    }

    /// Under the confidence floor a learned number may only RAISE the safety
    /// prior, never lower it: a thin profile makes admission more conservative,
    /// which is the opposite of what an under-evidenced estimate used to do.
    #[test]
    fn a_thin_profile_may_only_raise_the_prior() {
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
            );
            assert_eq!(larger.memory_bytes, with_headroom(1_000));
            assert_eq!(larger.disk_growth_bytes, with_headroom(2_000));
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
        )
        .await;
        assert_eq!(resolved.reservation.disk_growth_bytes, 2_000);
        let estimate = resolved.learned_estimate.unwrap();
        assert_eq!(estimate.upper_duration_ms, Some(10));
        assert_eq!(estimate.upper_disk_growth_bytes, None);
    }
}
