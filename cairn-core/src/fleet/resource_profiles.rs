use super::{ResourceReservation, ResourceReservationSource};
use crate::storage::LocalDb;
use cairn_common::executor_protocol::{
    CellExecutionMeta, CommandResourceIdentity, MeasurementQuality,
};
use cairn_db::turso::params;
use std::sync::Arc;

const MIN_CONFIDENT_SAMPLES: u64 = 5;
const HEADROOM_NUMERATOR: u64 = 5;
const HEADROOM_DENOMINATOR: u64 = 4;

#[derive(Clone)]
pub(super) struct ProfileContext {
    pub executor_class: String,
    pub os: String,
    pub arch: String,
    pub toolchain_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResolvedResourceProfile {
    pub reservation: ResourceReservation,
    pub learned_estimate: Option<cairn_common::executor_protocol::LearnedResourceEstimate>,
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
    let Some(identity) = identity else {
        return ResolvedResourceProfile {
            reservation: prior,
            learned_estimate: None,
        };
    };
    let Ok(Some(profile)) = load_profile(db, identity, context).await else {
        return ResolvedResourceProfile {
            reservation: prior,
            learned_estimate: None,
        };
    };
    ResolvedResourceProfile {
        reservation: reservation_for_profile(&profile, prior),
        learned_estimate: Some(cairn_common::executor_protocol::LearnedResourceEstimate {
            sample_count: profile.sample_count,
            upper_duration_ms: profile.upper_duration_ms,
            upper_peak_rss_bytes: profile.upper_peak_rss_bytes,
            upper_disk_growth_bytes: profile.upper_disk_delta_bytes,
        }),
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
                source: ResourceReservationSource::ZeroKnowledgePrior,
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
            lease_epoch: 1,
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
            lease_epoch: 1,
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
                source: ResourceReservationSource::ZeroKnowledgePrior,
            },
        )
        .await;
        assert_eq!(resolved.reservation.disk_growth_bytes, 2_000);
        let estimate = resolved.learned_estimate.unwrap();
        assert_eq!(estimate.upper_duration_ms, Some(10));
        assert_eq!(estimate.upper_disk_growth_bytes, None);
    }
}
