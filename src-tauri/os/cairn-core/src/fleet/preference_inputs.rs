//! Restart preference inputs, resolved only when an entry is figured.
use super::super::health::EXECUTOR_TELEMETRY_STALE_AFTER_MS;
use super::*;
use cairn_common::executor_protocol::{
    aged_priority, CellCommandClass, DurationEstimate, DurationFallback, ExecutionWarmth,
    QueueForecast, QueueUnknownReason,
};

pub(super) struct DurationOracle {
    pub(super) db: Option<Arc<cairn_db::storage::LocalDb>>,
}

impl DurationOracle {
    pub(super) async fn predict(
        &self,
        identity: Option<&CommandResourceIdentity>,
        context: &resource_profiles::ProfileContext,
        warmth: ExecutionWarmth,
        class: cairn_common::executor_protocol::CellCommandClass,
        now_unix_ms: u64,
    ) -> DurationEstimate {
        match &self.db {
            Some(db) => {
                resource_profiles::resolve_duration(
                    db.clone(),
                    identity,
                    context,
                    warmth,
                    class,
                    now_unix_ms,
                )
                .await
            }
            None => resource_profiles::unmeasured_duration(
                class,
                context,
                identity,
                warmth,
                DurationFallback::NoProfileStore,
            ),
        }
    }
}

/// Every distinct thing a forecast must price on one machine, so each is
/// resolved once however much work shares an identity.
pub(super) fn queue_price_keys(
    entry: &ExecutorConnectionState,
) -> Vec<(Option<CommandResourceIdentity>, CellCommandClass)> {
    let mut keys: Vec<(Option<CommandResourceIdentity>, CellCommandClass)> = Vec::new();
    let mut push = |identity: Option<CommandResourceIdentity>, class| {
        if !keys
            .iter()
            .any(|(known, known_class)| *known == identity && *known_class == class)
        {
            keys.push((identity, class));
        }
    };
    for queued in &entry.snapshot.queued_requests {
        push(
            queued.command_resource_identity.clone(),
            queued.command_class,
        );
    }
    for running in &entry.snapshot.executing_requests {
        push(
            running.command_resource_identity.clone(),
            running.command_class,
        );
    }
    keys
}

/// What each piece of work on a machine is predicted to take.
///
/// Resolving these is the only asynchronous part of a forecast, which is why it
/// is separated from the arithmetic: the ordering, the capacity model, and the
/// honesty rules are then a pure function that can be reasoned about and tested
/// without a database behind it.
#[derive(Default)]
pub(super) struct QueuePrices {
    pub(super) by_key: Vec<(
        (Option<CommandResourceIdentity>, CellCommandClass),
        DurationEstimate,
    )>,
}

impl QueuePrices {
    /// Queued work has not run, so nothing is known about the state it will
    /// find. Everything ahead is priced cold, which predicts the longer wait and
    /// therefore biases placement away from busy machines rather than toward
    /// them.
    pub(super) async fn resolve(
        oracle: &DurationOracle,
        entry: &ExecutorConnectionState,
        context: &resource_profiles::ProfileContext,
        now_unix_ms: u64,
    ) -> Self {
        let mut by_key = Vec::new();
        for (identity, class) in queue_price_keys(entry) {
            let estimate = oracle
                .predict(
                    identity.as_ref(),
                    context,
                    ExecutionWarmth::Cold,
                    class,
                    now_unix_ms,
                )
                .await;
            by_key.push(((identity, class), estimate));
        }
        Self { by_key }
    }

    pub(super) fn get(
        &self,
        identity: Option<&CommandResourceIdentity>,
        class: CellCommandClass,
    ) -> Option<&DurationEstimate> {
        self.by_key
            .iter()
            .find(|((known, known_class), _)| known.as_ref() == identity && *known_class == class)
            .map(|(_, estimate)| estimate)
    }
}

/// How long this request would wait on this machine before it starts.
///
/// `run_ms` is this request's own predicted run. A queue whose oldest entry
/// ahead has already waited longer than that has measurably not moved, and is
/// priced at least at that wait whatever its free units say.
pub(super) fn forecast_queue_wait(
    entry: &ExecutorConnectionState,
    prices: &QueuePrices,
    request: &CellRequest,
    run_ms: u64,
    now_unix_ms: u64,
) -> QueueForecast {
    let observed_at = entry
        .advertisement
        .liveness_observed_at_unix_ms
        .unwrap_or(entry.advertisement.observed_at_unix_ms);
    if now_unix_ms.saturating_sub(observed_at) > EXECUTOR_TELEMETRY_STALE_AFTER_MS {
        return QueueForecast::Unknown {
            reason: QueueUnknownReason::FactsStale,
        };
    }
    let capacity = entry
        .health
        .admission
        .concurrency_capacity
        .filter(|capacity| *capacity > 0);
    let Some(capacity) = capacity else {
        return QueueForecast::Unknown {
            reason: QueueUnknownReason::NoAdmissionCapacity,
        };
    };

    // Seniority is a property of the wait, not of the enqueue, so the
    // hypothetical request ranks by when its requester began waiting -- exactly
    // the key the executor's own admission uses.
    let waiting_since = if request.waiting_since_unix_ms == 0 {
        now_unix_ms
    } else {
        request.waiting_since_unix_ms
    };
    let ours = (
        aged_priority(request.priority, waiting_since, now_unix_ms),
        std::cmp::Reverse(waiting_since),
    );

    let mut demand_ahead = request.resource_reservation.concurrency_units;
    let mut requests_ahead = 0_usize;
    let mut oldest_ahead_ms = 0_u64;
    for queued in &entry.snapshot.queued_requests {
        let theirs = (
            aged_priority(queued.priority, queued.queued_at_unix_ms, now_unix_ms),
            std::cmp::Reverse(queued.queued_at_unix_ms),
        );
        if theirs <= ours {
            continue;
        }
        requests_ahead += 1;
        oldest_ahead_ms = oldest_ahead_ms.max(now_unix_ms.saturating_sub(queued.queued_at_unix_ms));
        let Some(estimate) = prices.get(
            queued.command_resource_identity.as_ref(),
            queued.command_class,
        ) else {
            // Something ahead could not be priced at all. Summing it as zero
            // would report an empty queue, so the whole forecast declines
            // instead.
            return QueueForecast::Unknown {
                reason: QueueUnknownReason::FactsStale,
            };
        };
        let _ = estimate;
        demand_ahead = demand_ahead.saturating_add(queued.resource_reservation.concurrency_units);
    }

    // Whatever the units say, entries ahead that have waited longer than this
    // request would take to run are a queue that is not moving, and that wait
    // is the measurement that prices it. Free units cannot read it as zero:
    // a machine holding work back for a reason it has not posted looks exactly
    // like one with room.
    let stalled_head_ms =
        (requests_ahead > 0 && oldest_ahead_ms > run_ms).then_some(oldest_ahead_ms);
    // The advertised capacity is already net of the interactive reserve (the
    // executor removes it when it sizes its budget, and admits against that
    // budget with nothing subtracted), so the reserve is not taken again here.
    let free = capacity.saturating_sub(entry.health.admission.active_reservation.concurrency_units);
    let running_ahead = entry.snapshot.executing_requests.len();

    // If measured capacity can admit this request and everything ranked ahead,
    // running durations do not enter the answer at all. This remains a measured
    // zero even when those durations only have priors.
    if demand_ahead <= free {
        return QueueForecast::Forecast {
            predicted_ms: stalled_head_ms.unwrap_or(0),
            requests_ahead,
            running_ahead,
            fully_measured: stalled_head_ms.is_none(),
            observed_at_unix_ms: observed_at,
            stalled_head_ms,
        };
    }

    let mut releases = Vec::with_capacity(running_ahead);
    for running in &entry.snapshot.executing_requests {
        let Some(estimate) = prices.get(
            running.command_resource_identity.as_ref(),
            running.command_class,
        ) else {
            return QueueForecast::Unknown {
                reason: QueueUnknownReason::FactsStale,
            };
        };
        let elapsed = now_unix_ms.saturating_sub(running.started_at_unix_ms);
        releases.push((
            estimate.predicted_ms.saturating_sub(elapsed),
            running.resource_reservation.concurrency_units,
            estimate.is_learned(),
        ));
    }
    releases.sort_by_key(|(remaining, _, _)| *remaining);

    let mut available = free;
    let mut predicted_ms = 0;
    let mut fully_measured = true;
    for (remaining, units, measured) in releases {
        available = available.saturating_add(units);
        predicted_ms = remaining;
        fully_measured &= measured && remaining > 0;
        if available >= demand_ahead {
            break;
        }
    }

    QueueForecast::Forecast {
        predicted_ms: stalled_head_ms.map_or(predicted_ms, |waited| waited.max(predicted_ms)),
        requests_ahead,
        running_ahead,
        fully_measured: fully_measured && stalled_head_ms.is_none(),
        observed_at_unix_ms: observed_at,
        stalled_head_ms,
    }
}

pub(super) fn expected_exposed_run<'a>(
    candidates: impl Iterator<Item = (bool, Option<&'a DurationEstimate>)>,
    bound_ms: u64,
) -> Option<u64> {
    let learned: Vec<_> = candidates
        .filter_map(|(local, duration)| {
            duration
                .filter(|duration| duration.is_learned())
                .map(|duration| (local, duration.predicted_ms))
        })
        .collect();
    let expected = learned
        .iter()
        .find(|(local, _)| *local)
        .map(|(_, run)| *run)
        .or_else(|| learned.iter().map(|(_, run)| *run).max())?;
    (expected > bound_ms).then_some(expected)
}

pub(super) fn restart_demoted(
    colocated: bool,
    queue_ms: Option<u64>,
    expected_run_ms: u64,
) -> bool {
    colocated || queue_ms.is_some_and(|wait| wait > expected_run_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::DurationEvidence;

    fn duration(ms: u64, learned: bool) -> DurationEstimate {
        DurationEstimate {
            predicted_ms: ms,
            source: if learned {
                DurationEvidence::Learned
            } else {
                DurationEvidence::Unmeasured
            },
            sample_count: u64::from(learned),
            profile_key: None,
            profile_context: String::new(),
            warmth: ExecutionWarmth::Cold,
            updated_at_unix_ms: None,
            fallback: None,
        }
    }

    #[test]
    fn restart_inputs_use_local_learning_before_the_longest_remote_and_never_priors() {
        let local = duration(100, true);
        let remote = duration(500, true);
        let prior = duration(1000, false);
        assert_eq!(
            expected_exposed_run(
                [(true, Some(&local)), (false, Some(&remote))].into_iter(),
                100
            ),
            None
        );
        assert_eq!(
            expected_exposed_run(
                [(true, Some(&local)), (false, Some(&remote))].into_iter(),
                99
            ),
            Some(100)
        );
        assert_eq!(
            expected_exposed_run(
                [
                    (true, Some(&prior)),
                    (false, Some(&local)),
                    (false, Some(&remote))
                ]
                .into_iter(),
                100
            ),
            Some(500)
        );
        assert_eq!(
            expected_exposed_run([(true, Some(&prior))].into_iter(), 1),
            None
        );
        assert_eq!(expected_exposed_run([(true, None)].into_iter(), 1), None);
    }

    #[test]
    fn restart_queue_guard_is_strict_and_unknown_is_not_demoted() {
        assert!(restart_demoted(true, None, 100));
        assert!(!restart_demoted(false, None, 100));
        assert!(!restart_demoted(false, Some(100), 100));
        assert!(restart_demoted(false, Some(101), 100));
    }
}

#[derive(Clone)]
pub(super) struct RestartFigures {
    pub(super) check: String,
    pub(super) platform: String,
    pub(super) expected_run_ms: u64,
    pub(super) bound_ms: u64,
    pub(super) colocated: String,
    pub(super) remotes: HashMap<String, (String, Option<u64>)>,
}

impl RestartFigures {
    pub(super) fn record(
        &self,
        local_won: bool,
        winner: &str,
        rejected: &[PlacementRejection],
    ) -> cairn_common::executor_protocol::RestartExposure {
        use cairn_common::executor_protocol::{RestartExposure, RestartExposureOutcome};
        let outcome = if local_won {
            let mut remotes: Vec<String> = self
                .remotes
                .iter()
                .map(|(id, (name, queue))| {
                    let why = if let Some(wait) = queue.filter(|wait| *wait > self.expected_run_ms)
                    {
                        format!(
                            "its queue is forecast at {} s, longer than the lane's run",
                            wait / 1000
                        )
                    } else {
                        rejected
                            .iter()
                            .find(|row| &row.executor_id == id)
                            .map(|row| row.reason.describe())
                            .unwrap_or_else(|| format!("outranked by {winner}"))
                    };
                    format!("{name}: {why}")
                })
                .collect();
            remotes.extend(
                rejected
                    .iter()
                    .filter(|row| {
                        row.executor_id != COLOCATED_EXECUTOR_ID
                            && !self.remotes.contains_key(&row.executor_id)
                            && !matches!(
                                row.reason,
                                PlacementRejectionReason::SelectorMismatch { .. }
                                    | PlacementRejectionReason::PinMismatch { .. }
                                    | PlacementRejectionReason::ProjectUnavailable { .. }
                                    | PlacementRejectionReason::UntrustedVerdictPlatform { .. }
                            )
                    })
                    .map(|row| format!("{}: {}", row.executor_name, row.reason.describe())),
            );
            remotes.sort();
            RestartExposureOutcome::NoAdoptableHost { remotes }
        } else {
            RestartExposureOutcome::ColocatedPassedOver {
                colocated: self.colocated.clone(),
            }
        };
        RestartExposure {
            check: self.check.clone(),
            platform: self.platform.clone(),
            expected_run_ms: self.expected_run_ms,
            restart_bound_ms: self.bound_ms,
            outcome,
        }
    }
}

pub(super) fn role_affinity(
    publication: bool,
    class: PlacementWorkClass,
    role: MachineRole,
) -> bool {
    if publication {
        role == MachineRole::PublishOnly
    } else {
        class == PlacementWorkClass::Services && role == MachineRole::ServicesOnly
    }
}

pub(super) fn cadence_lane(
    purposes: &purpose::PurposeRegistry,
    identity: &RequestIdentity,
) -> Option<(String, String)> {
    purposes
        .live_key_of(identity)
        .and_then(|key| key.main_cadence_lane())
        .map(|(check, platform)| (check.to_string(), platform.to_string()))
}

#[cfg(test)]
mod input_contract_tests {
    use super::*;
    #[test]
    fn publication_role_takes_precedence_over_services_class() {
        assert!(role_affinity(
            true,
            PlacementWorkClass::Services,
            MachineRole::PublishOnly
        ));
        assert!(!role_affinity(
            true,
            PlacementWorkClass::Services,
            MachineRole::ServicesOnly
        ));
        assert!(role_affinity(
            false,
            PlacementWorkClass::Services,
            MachineRole::ServicesOnly
        ));
        assert!(!role_affinity(
            false,
            PlacementWorkClass::Services,
            MachineRole::PublishOnly
        ));
    }

    #[test]
    fn only_the_live_main_cadence_holder_supplies_restart_inputs() {
        let mut purposes = purpose::PurposeRegistry::default();
        let main = ("main".to_string(), "attempt".to_string());
        let review = ("review".to_string(), "attempt".to_string());
        assert_eq!(cadence_lane(&purposes, &review), None);
        let key = purpose::PurposeKey::main_cadence("p", "rust-tests", "linux");
        purposes.admit(key.clone(), main.clone(), 0);
        assert_eq!(
            cadence_lane(&purposes, &main),
            Some(("rust-tests".into(), "linux".into()))
        );
        assert_eq!(cadence_lane(&purposes, &review), None);
        purposes.admit(key, review.clone(), 1);
        assert_eq!(
            cadence_lane(&purposes, &main),
            None,
            "a displaced holder is not a live cadence input"
        );
    }
}

/// The posted admission checks that turn a forecast into Unknown. Capacity
/// alone is deliberately absent: scheduled completions can relieve capacity,
/// but cannot predict relief from pressure, disk or foreign provisioning.
pub(super) struct QueueHoldWork<'a> {
    pub(super) project_id: &'a str,
    pub(super) request_id: &'a str,
    pub(super) entry_id: Option<&'a str>,
    pub(super) need: &'a Need,
    pub(super) class: CellCommandClass,
}

pub(super) fn posted_queue_hold(
    snapshot: &cairn_common::executor_protocol::FleetSnapshot,
    health: &cairn_common::executor_protocol::ExecutorSubstrateReport,
    work: &QueueHoldWork<'_>,
    now: u64,
) -> bool {
    use cairn_common::executor_protocol::{
        memory_short, MachineVolume, StallInstrument, StallInstrumentState,
    };
    let need = work.need;
    if health.disk.cleanup_blocked {
        return true;
    }
    if need.provisions.is_some()
        && snapshot.provisioning_holds.iter().any(|hold| {
            hold.project_id == work.project_id
                && hold.owner_request_id != work.request_id
                && !(work.entry_id.is_some() && hold.entry_id.as_deref() == work.entry_id)
        })
    {
        return true;
    }
    let build = health
        .machine
        .build_volume
        .as_ref()
        .and_then(|volume| volume.value());
    let volume = build.or_else(|| health.machine.volume.value());
    let floor = health
        .disk
        .watermark_bytes
        .saturating_add(if need.interactive {
            0
        } else {
            health.admission.active_reservation.disk_growth_bytes
        })
        .saturating_add(need.disk_bytes);
    if (health.disk.watermark_bytes > 0 && volume.is_some_and(|volume| volume.free_bytes < floor))
        || volume.and_then(MachineVolume::inodes_short).is_some()
        || build
            .and_then(|_| health.machine.volume.value())
            .and_then(MachineVolume::inodes_short)
            .is_some()
    {
        return true;
    }
    if need.interactive {
        return false;
    }
    let host = &health.host;
    let cpu = host.stall(StallInstrument::Cpu).state == StallInstrumentState::Pressured
        && host.cpu_arms().holding_arm(now).is_some();
    let memory = host.memory_admission.state == StallInstrumentState::Pressured
        && host.memory_admission.fresh(now);
    let disk = StallInstrument::DiskWrite.holds_class(work.class)
        && host.stall(StallInstrument::DiskWrite).state == StallInstrumentState::Pressured
        && host.stall(StallInstrument::DiskWrite).fresh(now);
    let owed = health.admission.owed_memory_bytes.unwrap_or_else(|| {
        snapshot
            .executing_requests
            .iter()
            .map(|running| {
                running
                    .resource_reservation
                    .memory_charge_bytes()
                    .saturating_sub(
                        running
                            .live_usage
                            .and_then(|usage| usage.resident_bytes)
                            .unwrap_or(0),
                    )
            })
            .fold(0, u64::saturating_add)
    });
    let short = health
        .machine
        .memory
        .value()
        .is_some_and(|memory| memory_short(memory.available_bytes, owed, need.memory_bytes));
    cpu || memory || disk || short
}

#[cfg(test)]
mod posted_hold_tests {
    use super::*;
    use cairn_common::executor_protocol::{
        ExecutorSubstrateReport, FleetSnapshot, ProvisioningHold,
    };
    #[test]
    fn foreign_provisioning_and_cleanup_make_wait_unknown_but_own_hold_and_capacity_do_not() {
        let need = Need::of(
            &ResourceReservation::default(),
            CellCommandClass::Other,
            false,
            cairn_common::pipe_health::PipeFloorWork::Process,
            true,
            Some("p"),
            CompileSlots::of(8),
        );
        let mut snapshot = FleetSnapshot::default();
        let mut health = ExecutorSubstrateReport::default();
        let mut work = QueueHoldWork {
            project_id: "p",
            request_id: "ours",
            entry_id: Some("entry"),
            need: &need,
            class: CellCommandClass::Other,
        };
        health.admission.concurrency_capacity = Some(0);
        assert!(!posted_queue_hold(&snapshot, &health, &work, 0));
        health.disk.cleanup_blocked = true;
        assert!(posted_queue_hold(&snapshot, &health, &work, 0));
        health.disk.cleanup_blocked = false;
        snapshot.provisioning_holds.push(ProvisioningHold {
            project_id: "p".into(),
            owner_request_id: "theirs".into(),
            entry_id: Some("foreign".into()),
            since_unix_ms: 0,
        });
        assert!(posted_queue_hold(&snapshot, &health, &work, 0));
        work.entry_id = Some("foreign");
        assert!(!posted_queue_hold(&snapshot, &health, &work, 0));
        work.entry_id = None;
        work.request_id = "theirs";
        assert!(!posted_queue_hold(&snapshot, &health, &work, 0));
    }
}
