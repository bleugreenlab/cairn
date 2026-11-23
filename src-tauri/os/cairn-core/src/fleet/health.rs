use super::node_scratch::NodeScratchEvent;
use super::requirements::project_eligibility;
use super::residency_routes::{
    resident_process_label, ResidencyRoute, GRANT_CONFIRMATION_GRACE_MS,
};
use super::selection::{missing_wait_reason_is_new, queued_placement_evidence, ReservationPlan};
use super::{
    executor_public_name, link_clock, load, rollout, rollout_candidate, service_placement,
    telemetry, unix_time_ms, warm_roots, ExecutorConnectionState, Fleet,
    PendingMemoryDiagnosticResult, PendingPolicyResult, RequestIdentity,
};
use cairn_common::executor_protocol::{
    CellRequest, ExecutorAdvertisement, ExecutorHealthSnapshot, ExecutorHealthStatus,
    ExecutorInspection, ExecutorMessage, ExecutorSubstrateReport, FleetSnapshot, MachineRole,
    MachineSample, ResidencyFence, ResidentProcessEvent, ResidentProcessEventKind, UpdateReport,
    EXECUTOR_HEARTBEAT_INTERVAL_MS,
};
use cairn_config::fleet::COLOCATED_EXECUTOR_ID;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

/// One executor link's snapshot count, read by [`Fleet::snapshot_receipts`].
/// Counts restart with each link generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotReceipt {
    pub generation: u64,
    pub received: u64,
}

/// The machine context a machine's learned memory evidence is keyed by, as
/// placement itself derives it, so a reader outside placement asks for exactly
/// the rows placement wrote: executor class, OS, architecture, and toolchain
/// fingerprint, in that order.
pub(crate) fn evidence_context(executor: &ExecutorInspection) -> [String; 4] {
    let context = ReservationPlan::context_for(
        &executor.health.identity.device_id,
        &executor.health.identity.executor_id,
        &executor.health.advertisement.capabilities,
    );
    [
        context.executor_class,
        context.os,
        context.arch,
        context.toolchain_fingerprint,
    ]
}

impl Fleet {
    pub(super) fn update_advertisement(
        &self,
        executor_id: &str,
        generation: u64,
        advertisement: ExecutorAdvertisement,
    ) -> bool {
        self.update_advertisement_with(executor_id, generation, advertisement, |_| {})
    }

    pub(super) fn update_advertisement_and_health(
        &self,
        executor_id: &str,
        generation: u64,
        advertisement: ExecutorAdvertisement,
        mut health: ExecutorSubstrateReport,
        last_update: Option<UpdateReport>,
        machine: Option<MachineSample>,
    ) -> bool {
        self.note_generation_process_telemetry(
            executor_id,
            generation,
            health.machine.process.clone(),
        );
        // The beat's own sample is newer than the pass its health came from
        // whenever that pass is waiting on pool state, which is exactly when
        // the difference matters.
        if let Some(sample) = &machine {
            health.adopt_newer_readings(sample);
        }
        self.update_advertisement_with(executor_id, generation, advertisement, |entry| {
            entry.publishes_machine_samples |= machine.is_some();
            if let Some(sample) = &machine {
                entry.live_usage.adopt(&sample.live_usage);
            }
            entry.health = Arc::new(entry.carry_sampled_readings(health));
            entry.last_update = last_update;
        })
    }

    /// Every machine's recent load, for a surface opening onto the fleet.
    pub fn fleet_load(&self) -> load::FleetLoad {
        self.load_rings
            .lock()
            .unwrap()
            .iter()
            .map(|(executor_id, ring)| (executor_id.clone(), ring.series()))
            .collect()
    }

    /// Take a machine sample from the executor's current link: adopt its
    /// readings, its work's live usage, and its point on the machine's ring,
    /// and answer the event that tells live surfaces. `None` for a frame from a
    /// link that has since been replaced, which describes nothing current.
    pub fn record_machine_sample(
        &self,
        executor_id: &str,
        generation: u64,
        sample: MachineSample,
    ) -> Option<load::FleetLoadSampleEvent> {
        let point = load::FleetLoadPoint::of_sample(&sample);
        let live_usage = sample.live_usage.clone();
        if !self.apply_machine_sample(executor_id, generation, sample) {
            return None;
        }
        let point = point.filter(|point| {
            self.load_rings
                .lock()
                .unwrap()
                .entry(executor_id.to_string())
                .or_default()
                .record(*point)
        });
        Some(load::FleetLoadSampleEvent {
            executor_id: executor_id.to_string(),
            point,
            live_usage,
        })
    }

    /// Hold the room an executor announced (cairn/7303): shown on its page,
    /// and handed to the dispatcher, which walks the entries that may run
    /// there. A frame from a replaced link, or one no newer than the room
    /// already held, changes nothing.
    pub fn record_room(
        &self,
        executor_id: &str,
        generation: u64,
        seq: u64,
        room: cairn_common::executor_protocol::AnnouncedRoom,
    ) {
        self.counters.announcement();
        let mut connections = self.lock_connections();
        let Some(entry) = connections
            .get_mut(executor_id)
            .filter(|entry| entry.generation == generation)
        else {
            return;
        };
        if entry.room.as_ref().is_some_and(|held| held.seq >= seq) {
            return;
        }
        entry.room = Some(Arc::new(
            cairn_common::executor_protocol::RoomAnnouncement {
                seq,
                received_at_unix_ms: unix_time_ms(),
                room: room.clone(),
            },
        ));
        self.dispatcher_room(&connections, executor_id, generation, seq, room);
    }

    /// Adopt a machine sample into this connection's posted health.
    ///
    /// Only the readings in it move. Cells, queues, reservations, and the rest
    /// of the posted health stay as the last snapshot or beat left them, and
    /// may be minutes older; the executor page already says so when they are.
    /// A sample never wakes placement: what a machine has room for arrives in
    /// its own announcement (cairn/7303), which its executor sends when the
    /// room moves.
    ///
    /// Link progress is deliberately not stamped. The beat stamps it, and what
    /// reads it judges whether the machine is accounting for its work, which a
    /// CPU reading says nothing about.
    ///
    /// Answers whether the sample came from the executor's current link.
    pub(super) fn apply_machine_sample(
        &self,
        executor_id: &str,
        generation: u64,
        sample: MachineSample,
    ) -> bool {
        {
            let mut connections = self.lock_connections();
            let Some(entry) = connections
                .get_mut(executor_id)
                .filter(|entry| entry.generation == generation)
            else {
                return false;
            };
            entry.publishes_machine_samples = true;
            entry.live_usage.adopt(&sample.live_usage);
            Arc::make_mut(&mut entry.health).adopt_newer_readings(&sample);
        }
        true
    }

    pub(super) fn update_advertisement_with(
        &self,
        executor_id: &str,
        generation: u64,
        advertisement: ExecutorAdvertisement,
        update: impl FnOnce(&mut ExecutorConnectionState),
    ) -> bool {
        let mut connections = self.lock_connections();
        let Some(entry) = connections.get_mut(executor_id) else {
            return false;
        };
        if entry.generation != generation || advertisement.identity != entry.identity {
            return false;
        }
        if advertisement.liveness_observed_at_unix_ms
            != entry.advertisement.liveness_observed_at_unix_ms
        {
            entry.link_clock.facts_recomputed();
        }
        let scratches_changed = advertisement.node_scratches != entry.advertisement.node_scratches;
        let replicas = scratches_changed.then(|| advertisement.node_scratches.clone());
        entry.advertisement = Arc::new(advertisement);
        entry.last_progress_unix_ms = unix_time_ms();
        update(entry);
        drop(connections);
        if let Some(replicas) = replicas {
            self.emit_node_scratch_event(NodeScratchEvent::Advertised {
                executor_id: executor_id.to_owned(),
                generation,
                replicas,
            });
        }
        self.machine_facts_posted(executor_id);
        true
    }

    /// Move a connection's warm roots by one change its executor sent.
    ///
    /// A delta against a base this runner does not hold leaves the set as it
    /// was. That costs placement precision and nothing else: the roots are
    /// hints, the executor re-verifies a commit before it trusts it, and a
    /// fetch negotiates its own haves. The runner asks for the whole
    /// inventory once, and not again until the answer arrives.
    pub(super) fn apply_warm_roots(
        &self,
        executor_id: &str,
        generation: u64,
        revision: u64,
        change: cairn_common::executor_protocol::WarmRootChange,
    ) {
        let mut connections = self.lock_connections();
        let Some(entry) = connections.get_mut(executor_id) else {
            return;
        };
        if entry.generation != generation {
            return;
        }
        let whole = matches!(
            change,
            cairn_common::executor_protocol::WarmRootChange::Whole { .. }
        );
        let mut moved = (*entry.warm_roots).clone();
        match moved.apply(revision, change) {
            warm_roots::WarmRootApply::Applied => {
                entry.warm_roots = Arc::new(moved);
                if whole {
                    entry.warm_roots_resync_requested = false;
                }
            }
            warm_roots::WarmRootApply::Stale => {
                log::debug!(
                    "executor_id={executor_id} generation={generation} sent warm-root revision {revision}, no newer than the {} this runner holds; discarded",
                    entry.warm_roots.revision()
                );
            }
            warm_roots::WarmRootApply::BaseMismatch { held, base } => {
                if entry.warm_roots_resync_requested {
                    return;
                }
                log::warn!(
                    "executor_id={executor_id} generation={generation} sent a warm-root delta against revision {base}, but this runner holds revision {held}; keeping revision {held} and asking for the whole inventory"
                );
                entry.warm_roots_resync_requested = entry
                    .sender
                    .send(ExecutorMessage::WarmRootsRequest {})
                    .is_ok();
            }
        }
    }

    /// Apply a snapshot from the current executor generation. `false` means the
    /// public cell snapshot did not change (or the generation is stale); health is
    /// still refreshed for a current connection.
    /// Ingest one snapshot a connected executor posted.
    ///
    /// The only way a snapshot enters the fleet from outside this crate, because
    /// a snapshot is more than state to store. The first one from a reattached
    /// generation is the machine's own account of what survived the gap, and
    /// held work it omits is settled here rather than left to the claimed-attempt
    /// silence bound. When the transport stored snapshots without this, an executor
    /// that died and came back without a command kept its submitter waiting for
    /// hours on a result no process would send.
    pub fn apply_executor_snapshot(
        &self,
        executor_id: &str,
        generation: u64,
        snapshot: FleetSnapshot,
        health: ExecutorSubstrateReport,
    ) -> bool {
        // Read before storing this account. A grant witnessed on this same
        // ordered stream proves the executor handled Granted, including taking
        // its hold before spawning work. An earlier empty snapshot can cross a
        // start still in flight and is not evidence of death.
        let Some((first_of_connection, witnessed)) =
            self.lock_connections()
                .get_mut(executor_id)
                .filter(|entry| entry.generation == generation)
                .map(|entry| {
                    entry.witnessed_grants.extend(
                        snapshot.granted_attempts.iter().map(|attempt| {
                            (attempt.request_id.clone(), attempt.attempt_id.clone())
                        }),
                    );
                    (
                        entry.snapshots_received == 0,
                        entry.witnessed_grants.clone(),
                    )
                })
        else {
            return false;
        };
        self.reconcile_held_executions(executor_id, generation, &snapshot);
        self.reconcile_witnessed_attempts(executor_id, generation, &witnessed, &snapshot);
        self.settle_purpose_debts_from_account(
            executor_id,
            generation,
            first_of_connection,
            &snapshot,
        );
        self.set_executor_snapshot(executor_id, generation, snapshot, health)
    }

    /// Store a snapshot as this connection's posted state, and nothing else.
    ///
    /// Crate-private so ingest cannot skip the accounting in
    /// [`Fleet::apply_executor_snapshot`]; tests use it to stage state.
    pub(crate) fn set_executor_snapshot(
        &self,
        executor_id: &str,
        generation: u64,
        mut snapshot: FleetSnapshot,
        health: ExecutorSubstrateReport,
    ) -> bool {
        // Before the connection lock, so this path never holds two locks at
        // once, and so a snapshot is as good a source of the final reading as a
        // heartbeat — whichever arrives last before the end is the one that
        // describes the end.
        self.note_generation_process_telemetry(
            executor_id,
            generation,
            health.machine.process.clone(),
        );
        let mut connections = self.lock_connections();
        let Some(entry) = connections.get_mut(executor_id) else {
            return false;
        };
        if entry.generation != generation {
            return false;
        }
        for cell in &mut snapshot.cells {
            cell.executor_id = executor_id.to_string();
            cell.executor_display_name = Some(entry.identity.display_name.clone());
            if let Some(active) = cell.occupancy.command.as_mut() {
                active.executor_id = executor_id.to_string();
            }
        }
        // Commands do not appear here any more. A machine answers an offer from
        // its own admission and either claims the work or declines it, so there
        // is no parked command to explain and nothing to join a placement survey
        // against after the fact. Residency acquisition also arrives already
        // claimed; neither operation parks in an executor admission queue.
        //
        // The evidence this used to journal has a better home: the entry's own
        // binding chain, which records the offer, the claim, and every decline
        // with its reason, and is addressable at
        // `cairn://executors?view=placement` instead of being grepped out of a
        // warning.
        for queued in &mut snapshot.queued_requests {
            queued.executor_id = executor_id.to_string();
            if queued.substrate_hold.is_none()
                && missing_wait_reason_is_new(&entry.snapshot, queued)
            {
                log::warn!(
                    "executor {executor_id} reported queued request {} attempt {} without a wait reason",
                    queued.request_id,
                    queued.attempt_id,
                );
            }
        }
        for execution in &mut snapshot.executing_requests {
            execution.executor_id = executor_id.to_string();
        }
        let snapshot_changed = *entry.snapshot != snapshot;
        entry.snapshots_received = entry.snapshots_received.saturating_add(1);
        let reconciled_process_events = snapshot
            .cells
            .iter()
            .filter_map(|cell| {
                let residency = cell.residency.as_ref()?;
                Some(
                    cell.occupancy
                        .processes
                        .iter()
                        .filter(move |(_, process)| {
                            matches!(
                                process.status,
                                cairn_common::executor_protocol::ResidentProcessStatus::Exited {
                                    executor_lost: true,
                                    ..
                                }
                            )
                        })
                        .map(move |(process_key, process)| ResidentProcessEvent {
                            holder: residency.holder.clone(),
                            incarnation_id: residency.incarnation_id.clone(),
                            cell_epoch: cell.cell_epoch,
                            process_key: process_key.clone(),
                            process_generation: process.generation,
                            event: ResidentProcessEventKind::State {
                                status: process.status.clone(),
                            },
                        }),
                )
            })
            .flatten()
            .collect::<Vec<_>>();
        let routes = snapshot
            .cells
            .iter()
            .filter_map(|cell| {
                cell.residency.as_ref().map(|residency| {
                    (
                        ResidencyRoute {
                            holder: residency.holder.clone(),
                            repository: residency.repository.clone(),
                            executor_id: executor_id.to_string(),
                            pending: false,
                        },
                        ResidencyFence {
                            holder: residency.holder.clone(),
                            incarnation_id: residency.incarnation_id.clone(),
                            cell_epoch: cell.cell_epoch,
                        },
                    )
                })
            })
            .collect::<Vec<_>>();
        // The replaced snapshot and health can run to hundreds of KB of nested
        // allocations. Freeing them is held until after `connections` is
        // released, because every MCP and UI read of the fleet takes it too.
        //
        // Progress and the snapshot it reports are written together, with
        // nothing that can fail between them, so no reader sees this link's
        // progress advanced past the snapshot that justified it.
        if snapshot_changed {
            entry.last_progress_unix_ms = unix_time_ms();
        }
        let health = entry.carry_sampled_readings(health);
        let replaced = (
            std::mem::replace(&mut entry.snapshot, Arc::new(snapshot)),
            std::mem::replace(&mut entry.health, Arc::new(health)),
        );
        let connected = connections.keys().cloned().collect::<HashSet<_>>();
        drop(connections);
        drop(replaced);
        let reported_holders = routes
            .iter()
            .map(|(route, _)| route.holder.storage_key())
            .collect::<HashSet<_>>();
        let now = unix_time_ms();
        let mut superseded = Vec::new();
        if let Err(error) = self.update_residency_routes(|known| {
            // A grant this report does not carry yet keeps its route: the
            // report may have been taken before the grant. It stops counting
            // once a report carries the cell (reports arrive in the order they
            // were taken, so none older follows), once a new link has opened,
            // or once reports have gone without it for the grace.
            let mut granted = self.granted_cells.lock().unwrap();
            granted.retain(|(granted_executor, holder_key), evidence| {
                granted_executor.as_str() != executor_id
                    || (!reported_holders.contains(holder_key)
                        && evidence.generation == generation
                        && now.saturating_sub(evidence.recorded_unix_ms)
                            < GRANT_CONFIRMATION_GRACE_MS)
            });
            known.retain(|key, route| {
                key.0.as_str() != executor_id || route.pending || granted.contains_key(key)
            });
            drop(granted);
            for (route, fence) in routes {
                // One owner, one environment. A machine coming back with a cell
                // for an owner that another live machine now hosts is reporting a
                // leftover from before it went away: recording it would leave the
                // owner's next acquisition facing two environments and unable to
                // choose, which is the ambiguity re-placement exists to avoid. The
                // leftover is released instead of recorded.
                if known.values().any(|held| {
                    !held.pending
                        && held.holder == route.holder
                        && held.executor_id != *executor_id
                        && connected.contains(&held.executor_id)
                }) {
                    superseded.push(fence);
                    continue;
                }
                known.insert(
                    (route.executor_id.clone(), route.holder.storage_key()),
                    route,
                );
            }
        }) {
            tracing::error!(%error, "persist executor residency route snapshot failed");
        }
        for fence in superseded {
            self.release_residency_on(executor_id, generation, fence);
        }
        for event in reconciled_process_events {
            self.end_relay_of_exited_process(executor_id, &event);
            self.publish_resident_process_event(event);
        }
        self.machine_facts_posted(executor_id);
        snapshot_changed
    }

    /// Jobs with a cell request no machine has claimed yet: the work the
    /// placement queue is still holding for a machine. Read by the execution
    /// queue to say how much of the fleet's in-progress work is still waiting
    /// for a machine; placement itself never reads the queue's caps.
    pub(crate) fn jobs_waiting_for_a_machine(&self) -> HashSet<String> {
        self.queue
            .waiting_rows()
            .into_iter()
            .filter_map(|row| row.requesting_job_id)
            .collect()
    }

    /// How many snapshots each connected executor's current link has delivered.
    pub(crate) fn snapshot_receipts(&self) -> HashMap<String, SnapshotReceipt> {
        self.lock_connections()
            .iter()
            .map(|(executor_id, connection)| {
                (
                    executor_id.clone(),
                    SnapshotReceipt {
                        generation: connection.generation,
                        received: connection.snapshots_received,
                    },
                )
            })
            .collect()
    }

    pub fn snapshot(&self) -> FleetSnapshot {
        let connections = self.lock_connections();
        let mut entries: Vec<_> = connections.iter().collect();
        entries.sort_by_key(|(id, _)| *id);
        let mut aggregate = FleetSnapshot::default();
        for (id, connection) in entries {
            let snapshot = &connection.snapshot;
            let mut live = FleetSnapshot {
                cells: snapshot.cells.clone(),
                executing_requests: snapshot.executing_requests.clone(),
                ..FleetSnapshot::default()
            };
            connection.live_usage.join_onto(&mut live);
            aggregate.cells.extend(live.cells);
            aggregate
                .queued_requests
                .extend(snapshot.queued_requests.clone());
            aggregate.executing_requests.extend(live.executing_requests);
            aggregate
                .cell_transitions
                .extend(snapshot.cell_transitions.clone());
            aggregate
                .recent_completions
                .extend(
                    snapshot
                        .recent_completions
                        .iter()
                        .cloned()
                        .map(|mut completion| {
                            completion.executor_id = id.clone();
                            completion
                        }),
                );
            if let Some(occupancy) = &snapshot.resident_occupancy {
                let aggregate_occupancy = aggregate
                    .resident_occupancy
                    .get_or_insert_with(Default::default);
                aggregate_occupancy.process_count += occupancy.process_count;
                aggregate_occupancy.reservation.memory_bytes = aggregate_occupancy
                    .reservation
                    .memory_bytes
                    .saturating_add(occupancy.reservation.memory_bytes);
                aggregate_occupancy.reservation.disk_growth_bytes = aggregate_occupancy
                    .reservation
                    .disk_growth_bytes
                    .saturating_add(occupancy.reservation.disk_growth_bytes);
                aggregate_occupancy.reservation.concurrency_units = aggregate_occupancy
                    .reservation
                    .concurrency_units
                    .saturating_add(occupancy.reservation.concurrency_units);
            }
        }
        // Work waiting for room waits in the runner's queue, not on a machine,
        // so the executors' reports hold only what they claimed. An identity a
        // machine already reports is left to that report.
        let reported: HashSet<(&str, &str)> =
            aggregate
                .queued_requests
                .iter()
                .map(|queued| (queued.request_id.as_str(), queued.attempt_id.as_str()))
                .chain(aggregate.executing_requests.iter().map(|executing| {
                    (executing.request_id.as_str(), executing.attempt_id.as_str())
                }))
                .collect();
        let waiting: Vec<_> = self
            .queue
            .waiting_rows()
            .into_iter()
            .filter(|row| !reported.contains(&(row.request_id.as_str(), row.attempt_id.as_str())))
            .collect();
        aggregate.queued_requests.extend(waiting);
        aggregate.recent_completions.extend(
            self.recent_cached_completions
                .lock()
                .unwrap()
                .iter()
                .cloned(),
        );
        aggregate
            .cells
            .sort_by(|a, b| (&a.executor_id, &a.cell_id).cmp(&(&b.executor_id, &b.cell_id)));
        aggregate
            .executing_requests
            .sort_by(|a, b| (&a.request_id, &a.attempt_id).cmp(&(&b.request_id, &b.attempt_id)));
        aggregate.recent_completions.sort_by(|a, b| {
            b.served_at_unix_ms
                .cmp(&a.served_at_unix_ms)
                .then_with(|| a.request_id.cmp(&b.request_id))
        });
        aggregate.recent_completions.truncate(32);
        aggregate.queued_requests.sort_by(|a, b| {
            (a.queued_at_unix_ms, &a.executor_id, &a.request_id).cmp(&(
                b.queued_at_unix_ms,
                &b.executor_id,
                &b.request_id,
            ))
        });
        let counts: HashMap<_, _> = self
            .in_flight
            .lock()
            .unwrap()
            .by_key
            .iter()
            .map(|(key, execution)| (key.leader.clone(), execution.subscribers.len()))
            .collect();
        for cell in &mut aggregate.cells {
            if let Some(active) = cell.occupancy.command.as_mut() {
                active.subscriber_count = counts
                    .get(&(active.request_id.clone(), active.attempt_id.clone()))
                    .copied()
                    .unwrap_or(1);
            }
        }
        let placements = self.recent_placements.lock().unwrap();
        for queued in &mut aggregate.queued_requests {
            queued.subscriber_count = counts
                .get(&(queued.request_id.clone(), queued.attempt_id.clone()))
                .copied()
                .unwrap_or(1);
            queued.placement = queued.placement.take().or_else(|| {
                placements
                    .iter()
                    .rev()
                    .find(|decision| {
                        decision.request_id == queued.request_id
                            && decision.attempt_id == queued.attempt_id
                    })
                    .map(queued_placement_evidence)
            });
        }
        for completion in &mut aggregate.recent_completions {
            completion.subscriber_count = counts
                .get(&(completion.request_id.clone(), completion.attempt_id.clone()))
                .copied()
                .unwrap_or(completion.subscriber_count.max(1));
        }
        if aggregate.substrate_state.is_none() {
            aggregate.substrate_state = self.colocated_substrate();
        }
        aggregate
    }

    pub fn executor_health(&self, captured_at_unix_ms: u64) -> Vec<ExecutorHealthSnapshot> {
        let connections = self.lock_connections();
        let expected_build_ids = self.expected_build_ids(&connections);
        let mut values: Vec<_> = connections
            .values()
            .map(|entry| executor_health_snapshot(entry, captured_at_unix_ms, &expected_build_ids))
            .collect();
        values.sort_by(|a, b| a.identity.executor_id.cmp(&b.identity.executor_id));
        values
    }

    /// The runner's original request, keyed by its physical attempt, not by
    /// a display label or an executor-local process.
    pub(crate) fn inspection_request(&self, identity: &RequestIdentity) -> Option<CellRequest> {
        self.queue
            .with_entry_by_identity(identity, |entry| entry.request.clone())
    }

    /// Every executor as an agent inspects it, addressed by public name.
    ///
    /// The whole projection is taken under ONE acquisition of the connections
    /// lock, so an executor's link state, its telemetry, and the work resident
    /// on it always come from the same connection generation. Reading them
    /// through separate calls would let a reconnect land in between and describe
    /// a machine that never existed — healthy link, previous incarnation's
    /// occupancy.
    ///
    /// Cached state only. This answers from what the runner already holds and
    /// never probes an executor or samples the fleet, so reading it costs a
    /// clone rather than a round trip to every machine.
    pub fn inspect_executors(&self, captured_at_unix_ms: u64) -> Vec<ExecutorInspection> {
        let known_projects = self.known_project_requirements.lock().unwrap().clone();
        let connections = self.lock_connections();
        let expected_build_ids = self.expected_build_ids(&connections);
        let artifacts = self.executor_artifact_builds.lock().unwrap().clone();
        let self_update = self.self_update.lock().unwrap();
        let enrolled_remotes = self.enrolled_remotes.lock().unwrap();
        let mut values: Vec<_> = connections
            .values()
            .map(|entry| ExecutorInspection {
                reach_history: self
                    .runner_events
                    .reach_history(&entry.identity.executor_id),
                drain_state: entry.drain_state.clone(),
                name: executor_public_name(entry),
                link_path: entry.link_path,
                recent_placements: self.placements_naming(&entry.identity.executor_id),
                colocated: entry.colocated,
                health: executor_health_snapshot(entry, captured_at_unix_ms, &expected_build_ids),
                executor_build_id: entry.executor_build_id.clone(),
                last_update: entry.last_update.clone(),
                self_update: if entry.colocated {
                    None
                } else {
                    self_update.status(&rollout_candidate(entry, captured_at_unix_ms), &artifacts)
                },
                occupancy: entry.occupancy(),
                live_usage_gap: entry.live_usage.gap(),
                slots: entry.health.slots.clone(),
                captured_at_unix_ms,
                connection_timeline: enrolled_remotes
                    .get(&entry.identity.executor_id)
                    .map(|record| record.connection_timeline.iter().rev().cloned().collect())
                    .unwrap_or_default(),
                project_eligibility: project_eligibility(entry, &known_projects),
                room: entry.room.as_deref().cloned(),
            })
            .collect();
        values.sort_by(|a, b| a.name.cmp(&b.name));
        values
    }

    /// The coarse frame a live surface subscribes to instead of polling
    /// `cairn://executors`.
    ///
    /// Each row goes through [`telemetry::ExecutorTelemetryRow::new`] from the
    /// same per-connection facts [`Self::inspect_executors`] projects (public
    /// name, link status, occupancy counts, machine readings), so the pushed
    /// frame and the read resource still cannot come to disagree. What it skips
    /// is the inspection's full clone of every executor's snapshot, which this
    /// frame never reads and which would otherwise be copied under
    /// `connections` on every fleet change.
    pub fn telemetry_frame(&self, captured_at_unix_ms: u64) -> telemetry::FleetTelemetryFrame {
        let connections = self.lock_connections();
        let mut executors: Vec<_> = connections
            .values()
            .map(|entry| {
                telemetry::ExecutorTelemetryRow::new(
                    executor_public_name(entry),
                    &entry.advertisement.capabilities.os,
                    &executor_link_status(entry, captured_at_unix_ms),
                    entry.snapshot.executing_requests.len(),
                    entry.snapshot.queued_requests.len(),
                    &entry.health.machine,
                )
            })
            .collect();
        drop(connections);
        executors.sort_by(|a, b| a.name.cmp(&b.name));
        telemetry::FleetTelemetryFrame {
            ts: captured_at_unix_ms,
            executors,
        }
    }

    /// [`Self::telemetry_frame`] stamped with the current instant.
    pub fn telemetry_frame_now(&self) -> telemetry::FleetTelemetryFrame {
        self.telemetry_frame(unix_time_ms())
    }

    /// Admit one fleet-state change and learn when its frame may go out.
    ///
    /// The clock stays inside the fleet rather than being passed in, so no
    /// caller can rate-limit against a different notion of now than the frame
    /// it ends up sending was stamped with.
    pub fn admit_telemetry(&self) -> telemetry::TelemetryEmit {
        self.telemetry_cadence.admit(unix_time_ms())
    }

    /// The deferred trailing frame a [`telemetry::TelemetryEmit::After`] asked
    /// for, with the cadence reopened before the fleet is read.
    ///
    /// Producing the frame and reopening the cadence are one operation because
    /// their order is load-bearing; see [`telemetry::TelemetryCadence::capture_trailing`].
    pub fn trailing_telemetry_frame(&self) -> telemetry::FleetTelemetryFrame {
        let now = unix_time_ms();
        self.telemetry_cadence
            .capture_trailing(now, || self.telemetry_frame(now))
    }

    pub(crate) async fn set_memory_diagnostic(
        &self,
        expected_generation: u64,
        attach: Option<cairn_common::executor_protocol::MemoryDiagnosticAttach>,
    ) -> Result<(), String> {
        if !cfg!(debug_assertions) {
            return Err("memory diagnostics are development-only".into());
        }
        if let Some(attach) = &attach {
            attach.validate()?;
        }
        let sender = self
            .lock_connections()
            .get(COLOCATED_EXECUTOR_ID)
            .filter(|entry| entry.colocated && entry.generation == expected_generation)
            .map(|entry| entry.sender.clone())
            .ok_or_else(|| "local executor connection generation is stale".to_string())?;
        let correlation_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending_memory_diagnostic.lock().unwrap().insert(
            correlation_id.clone(),
            PendingMemoryDiagnosticResult {
                generation: expected_generation,
                waiter: tx,
            },
        );
        if sender
            .send(ExecutorMessage::MemoryDiagnosticRequest {
                correlation_id: correlation_id.clone(),
                expected_generation,
                attach,
            })
            .is_err()
        {
            self.pending_memory_diagnostic
                .lock()
                .unwrap()
                .remove(&correlation_id);
            return Err("local executor disconnected while changing memory diagnostic".into());
        }
        let result = match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("local executor dropped the memory diagnostic response".into()),
            Err(_) => Err("memory diagnostic response timed out; an accepted watch still expires within 60 seconds".into()),
        };
        self.pending_memory_diagnostic
            .lock()
            .unwrap()
            .remove(&correlation_id);
        result
    }

    pub async fn set_executor_runtime_policy(
        &self,
        executor_id: &str,
        expected_generation: u64,
        policy: cairn_common::executor_protocol::ExecutorRuntimePolicy,
    ) -> Result<cairn_common::executor_protocol::ExecutorRuntimePolicy, String> {
        policy.validate()?;
        let sender = self
            .lock_connections()
            .get(executor_id)
            .filter(|entry| entry.generation == expected_generation)
            .map(|entry| entry.sender.clone())
            .ok_or_else(|| "executor connection generation is stale".to_string())?;
        let correlation_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending_policy.lock().unwrap().insert(
            correlation_id.clone(),
            PendingPolicyResult {
                executor_id: executor_id.to_string(),
                generation: expected_generation,
                waiter: tx,
            },
        );
        if sender
            .send(ExecutorMessage::RuntimePolicyRequest {
                correlation_id: correlation_id.clone(),
                policy,
            })
            .is_err()
        {
            self.pending_policy.lock().unwrap().remove(&correlation_id);
            return Err("executor disconnected while applying runtime policy".into());
        }
        match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("executor dropped the runtime-policy response".into()),
            Err(_) => {
                self.pending_policy.lock().unwrap().remove(&correlation_id);
                Err("executor runtime-policy update timed out".into())
            }
        }
    }

    /// The role stamped on a live connection, for tests that check the cache
    /// and the entry placement reads agree.
    #[cfg(test)]
    pub(crate) fn live_machine_role_for_test(&self, executor_id: &str) -> Option<MachineRole> {
        self.lock_connections()
            .get(executor_id)
            .map(|entry| entry.machine_role)
    }

    /// What this runner holds a machine to be for.
    pub fn machine_role(&self, executor_id: &str) -> MachineRole {
        self.machine_roles
            .lock()
            .unwrap()
            .get(executor_id)
            .copied()
            .unwrap_or_default()
    }

    /// Adopt the durable machine roles, replacing the whole cache and
    /// restamping every live connection.
    ///
    /// The only writer of [`Fleet::machine_roles`]. Callers pass
    /// [`FleetConfig::role_executor_ids`] of a config they have just loaded or
    /// just persisted, never a value of their own, so placement cannot enforce
    /// a role the settings file does not hold (the split cairn/4464 found in
    /// the drain stores). Restamping live entries makes the order against
    /// attach irrelevant, as it is for drains.
    pub fn hydrate_machine_roles(&self, roles: impl IntoIterator<Item = (String, MachineRole)>) {
        let adopted: HashMap<String, MachineRole> = roles
            .into_iter()
            .filter(|(_, role)| *role != MachineRole::General)
            .collect();
        // Map and entries change under one hold of the table lock, the lock
        // attach stamps under, so no attach can insert a role read from the
        // map this replaces. Callers serialize adoptions themselves by
        // publishing from inside `settings::mutate_fleet`.
        let mut connections = self.lock_connections();
        *self.machine_roles.lock().unwrap() = adopted.clone();
        for (executor_id, entry) in connections.iter_mut() {
            entry.machine_role = adopted.get(executor_id).copied().unwrap_or_default();
        }
    }

    pub fn set_desired_executor_runtime_policy(
        &self,
        executor_id: &str,
        policy: cairn_common::executor_protocol::ExecutorRuntimePolicy,
    ) {
        self.desired_runtime_policies
            .lock()
            .unwrap()
            .insert(executor_id.to_string(), policy);
    }
}

/// Three missed 30-second executor heartbeats make the live connection stale.
/// This bound describes the *link*, and nothing else: an executor beating on
/// time is reachable whatever the facts it carries look like.
pub(super) const EXECUTOR_LINK_STALE_AFTER_MS: u64 = 90_000;

/// An executor emits beats from one task and computes what they carry on
/// another, so a link that is demonstrably alive can still be shipping facts
/// that stopped moving. The payload refreshes on the same interval the beat
/// does, so the same three-cycle allowance applies: past it, the pressure, disk,
/// queue, and warm-root numbers on this snapshot are history, and presenting
/// them as current is the failure this bound exists to prevent.
///
/// It is reported separately from the link, because they call for different
/// responses and every surface has to say which one happened. Folding aged facts
/// into the connection status is how a healthy machine came to be labelled
/// "heartbeat stale" while its heartbeats were arriving on time. This is a
/// health verdict only — link stall remediation is silence, and silence alone.
pub(super) const EXECUTOR_TELEMETRY_STALE_AFTER_MS: u64 = EXECUTOR_LINK_STALE_AFTER_MS;

/// The minimum gap between two telemetry-stall bounces anywhere in the fleet.
///
/// Two heartbeat intervals: long enough for the machine that just went to
/// reattach and publish a beat or two, so the next candidate is judged against
/// a fleet that has already absorbed the previous recovery rather than against
/// the moment of maximum disruption.
pub(super) const TELEMETRY_STALL_BOUNCE_SPACING_MS: u64 = 2 * EXECUTOR_HEARTBEAT_INTERVAL_MS;

/// How long ago this connection's executor last recomputed the facts its
/// heartbeats carry, by the executor's own stamp.
///
/// `None` when the executor publishes no such stamp. The beat's own
/// `observed_at_unix_ms` is deliberately not substituted for it: that would
/// answer a freshness question about the facts with a timestamp taken when they
/// were shipped, which is precisely the conflation the split stamp exists to
/// prevent.
pub(super) fn published_facts_age_ms(
    entry: &ExecutorConnectionState,
    now_unix_ms: u64,
) -> Option<u64> {
    entry
        .advertisement
        .liveness_observed_at_unix_ms
        .map(|observed_at| now_unix_ms.saturating_sub(observed_at))
}

/// How long this connection has been publishing facts that stopped being
/// recomputed *and has not said why*.
///
/// Stall remediation reads this rather than the raw age, because the two
/// freezes are not the same fault. An executor whose refresh pass has overrun
/// its bound names the phase it is stuck in on every beat: that is a machine
/// reporting its own defect while still running its work and still answering
/// its link, and recycling that link answers the report by destroying the
/// reporter along with every batch it holds. A freeze with no explanation is
/// the case the watchdog was written for and reaches it unchanged — including
/// from an executor too old to explain itself, which claims nothing here and is
/// judged exactly as before.
///
/// The facts are equally stale either way, and nothing here makes them
/// trustworthy: placement, warmth, queue forecasting and `telemetry_stale` all
/// keep reading [`published_facts_age_ms`] and keep refusing them. What the
/// explanation buys is the difference between a degraded machine and a dead
/// link.
///
/// Nor is a freeze the runner cannot attribute to the executor: while a
/// heartbeat the reader has received is still waiting behind the link's ingest
/// (a slow snapshot apply, say), the advertisement on record is old because
/// the runner has not applied the newer one, not because the executor stopped
/// recomputing. Bouncing on that would be the runner blaming the peer for its
/// own backlog, which is the failure [`link_clock`] exists to prevent.
///
/// What it measures is how long the executor's recompute stamp has gone
/// unchanged on the runner's awake clock, not the stamp's wall-clock age. The
/// two agree on a host that stays awake. They part across a sleep: wall time
/// runs on, and an executor whose refresh interval is counted in its own awake
/// time correctly ships its pre-sleep facts in the first beats after the wake.
/// Read as wall age, a fifteen-minute sleep was a fifteen-minute freeze, and
/// the colocated executor was recycled on every wake. Placement keeps the wall
/// age, because facts measured before a sleep really are old.
pub(super) fn unexplained_facts_frozen_ms(
    entry: &ExecutorConnectionState,
    now_ns: u64,
) -> Option<u64> {
    if entry.advertisement.facts_stale_refresh_blocked.is_some()
        || entry.link_clock.heartbeat_pending()
    {
        return None;
    }
    entry.advertisement.liveness_observed_at_unix_ms?;
    entry.link_clock.facts_frozen_ms(now_ns)
}

/// The key a command holds a self-update drain under, shared by every witness
/// to it (the machine's executing, queued, and granted lists, and the runner's
/// queue binding), so one command is one piece of work however many report it.
pub(super) fn held_request_key(request_id: &str, attempt_id: &str) -> String {
    format!(
        "request:{}:{request_id}:{}:{attempt_id}",
        request_id.len(),
        attempt_id.len()
    )
}

/// A command named the way an operator reads it: the first line of what it
/// runs, and whose work it is.
pub(super) fn held_command_label(
    command: &str,
    owner: Option<&cairn_common::executor_protocol::CellOwnerRef>,
) -> String {
    let head: String = command
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .chars()
        .take(60)
        .collect();
    let head = if head.is_empty() {
        "a command".to_string()
    } else {
        format!("`{head}`")
    };
    match owner {
        Some(cairn_common::executor_protocol::CellOwnerRef {
            project_key: Some(project),
            issue_number: Some(issue),
            ..
        }) => format!("{head} for {project}/{issue}"),
        Some(cairn_common::executor_protocol::CellOwnerRef {
            job_id: Some(job_id),
            ..
        }) => format!("{head} for job {job_id}"),
        _ => head,
    }
}

/// ` of cairn/6613`, ` of job j-1`, or nothing: whose a resident is, the way
/// [`held_command_label`] names a command's owner.
pub(super) fn resident_owner_suffix(
    owner: Option<&cairn_common::executor_protocol::CellOwnerRef>,
) -> String {
    match owner {
        Some(cairn_common::executor_protocol::CellOwnerRef {
            project_key: Some(project),
            issue_number: Some(issue),
            ..
        }) => format!(" of {project}/{issue}"),
        Some(cairn_common::executor_protocol::CellOwnerRef {
            job_id: Some(job_id),
            ..
        }) => format!(" of job {job_id}"),
        _ => String::new(),
    }
}

/// What a restart of this machine would end, as its own snapshot reports it,
/// in three kinds: work that will finish on its own, one item per piece; the
/// long-lived service instances the restart cycles, which never finish and
/// are placed again by their owners; and the terminals, REPLs and dev
/// instances it ends, which never finish on their own and hold a turn only
/// for its notice period. A service's one-shot command is work.
pub(super) fn snapshot_work(
    entry: &ExecutorConnectionState,
) -> (Vec<rollout::HeldWork>, Vec<String>, Vec<rollout::Resident>) {
    use cairn_common::executor_protocol::{ResidentProcessKind, ResidentProcessStatus};

    let snapshot = &entry.snapshot;
    let learned = |estimate: Option<&cairn_common::executor_protocol::LearnedResourceEstimate>| {
        estimate.and_then(|estimate| estimate.upper_duration_ms)
    };
    // A granted attempt names no command; the cell it holds does.
    let active: HashMap<(&str, &str), &cairn_common::executor_protocol::ActiveCellRequest> =
        snapshot
            .cells
            .iter()
            .filter_map(|cell| cell.occupancy.command.as_ref())
            .map(|command| {
                (
                    (command.request_id.as_str(), command.attempt_id.as_str()),
                    command,
                )
            })
            .collect();
    // What the attempt's own stall stop holds about its tree, from the latest
    // machine sample. Absent whenever that stop cannot judge, which leaves the
    // drain to the bound alone.
    let progress = |request_id: &str, attempt_id: &str| {
        let subject = cairn_common::executor_protocol::LiveUsageSubject::Command {
            request_id: request_id.to_string(),
            attempt_id: attempt_id.to_string(),
        };
        let sample = entry.live_usage.get(&subject)?;
        let window = sample.quiet_window?;
        Some(rollout::Progress {
            quiet_ms: window.quiet_ms,
            silence_bound_ms: window.silence_bound_ms,
            measured_at_unix_ms: sample.measured_at_unix_ms,
        })
    };
    // Richest witness first, so the dedup below keeps its label and timing.
    let mut work: Vec<rollout::HeldWork> = Vec::new();
    for request in &snapshot.executing_requests {
        work.push(rollout::HeldWork {
            key: held_request_key(&request.request_id, &request.attempt_id),
            label: held_command_label(&request.command, request.owner.as_ref()),
            started_at_unix_ms: Some(request.started_at_unix_ms),
            expected_ms: learned(request.learned_estimate.as_ref()),
            progress: progress(&request.request_id, &request.attempt_id),
        });
    }
    for granted in &snapshot.granted_attempts {
        work.push(
            match active.get(&(granted.request_id.as_str(), granted.attempt_id.as_str())) {
                Some(command) => rollout::HeldWork {
                    key: held_request_key(&granted.request_id, &granted.attempt_id),
                    label: held_command_label(&command.command, command.owner.as_ref()),
                    started_at_unix_ms: command.started_at_unix_ms,
                    expected_ms: learned(command.learned_estimate.as_ref()),
                    progress: progress(&granted.request_id, &granted.attempt_id),
                },
                None => rollout::HeldWork {
                    key: held_request_key(&granted.request_id, &granted.attempt_id),
                    label: format!("request {} (preparing its cell)", granted.request_id),
                    started_at_unix_ms: None,
                    expected_ms: None,
                    progress: None,
                },
            },
        );
    }
    for queued in &snapshot.queued_requests {
        work.push(rollout::HeldWork {
            key: held_request_key(&queued.request_id, &queued.attempt_id),
            label: held_command_label(&queued.command, queued.owner.as_ref()),
            started_at_unix_ms: None,
            expected_ms: learned(queued.learned_estimate.as_ref()),
            progress: None,
        });
    }
    for request_id in snapshot.in_flight_residency_operations.keys() {
        work.push(rollout::HeldWork {
            key: format!("residency:{request_id}"),
            label: format!("an environment being acquired (request {request_id})"),
            started_at_unix_ms: None,
            expected_ms: None,
            progress: None,
        });
    }
    let mut services = Vec::new();
    let mut residents = Vec::new();
    for cell in &snapshot.cells {
        let residency = cell.residency.as_ref();
        let owner = residency.and_then(|residency| residency.owner_ref.as_ref());
        for (process_key, process) in &cell.occupancy.processes {
            let started_at_unix_ms = match &process.status {
                ResidentProcessStatus::Starting => None,
                ResidentProcessStatus::Running {
                    started_at_unix_ms, ..
                } => Some(*started_at_unix_ms),
                _ => continue,
            };
            let key = format!(
                "process:{}/{process_key}/{}",
                cell.cell_id, process.generation
            );
            match &process.kind {
                ResidentProcessKind::Service { role, .. }
                    if role != service_placement::ONE_SHOT_ROLE =>
                {
                    services.push(resident_process_label(&process.kind));
                }
                kind @ (ResidentProcessKind::Terminal { .. }
                | ResidentProcessKind::Repl { .. }
                | ResidentProcessKind::DevInstance { .. }) => {
                    residents.push(rollout::Resident {
                        key,
                        label: format!(
                            "{}{}",
                            resident_process_label(kind),
                            resident_owner_suffix(owner)
                        ),
                        address: rollout::ResidentAddress {
                            process_key: process_key.clone(),
                            kind: Some(kind.clone()),
                            holder: residency.map(|residency| residency.holder.clone()),
                            owner: owner.cloned(),
                            project_id: Some(cell.project_id.clone())
                                .filter(|project| !project.is_empty()),
                            selector: residency.and_then(|residency| residency.selector.clone()),
                        },
                    });
                }
                kind => work.push(rollout::HeldWork {
                    key,
                    label: resident_process_label(kind),
                    started_at_unix_ms,
                    expected_ms: None,
                    progress: None,
                }),
            }
        }
    }
    let mut keys = HashSet::new();
    work.retain(|item| keys.insert(item.key.clone()));
    services.sort();
    services.dedup();
    residents.sort_by(|a, b| a.label.cmp(&b.label).then_with(|| a.key.cmp(&b.key)));
    (work, services, residents)
}

/// The runner's own account joined to the machine's: work its queue binds to
/// the machine (offered or claimed) and residency acquisitions it is waiting
/// on there, either of which the machine's last snapshot may not show yet.
///
/// A binding is keyed by the request the machine reports it under, so the two
/// accounts union. A pending acquisition is keyed by the runner's own
/// correlation id, which the machine never sees, so only the excess over what
/// the machine reports is added: two witnesses to one acquisition count once.
pub(super) fn join_runner_work(
    work: &mut Vec<rollout::HeldWork>,
    bound: &[RequestIdentity],
    pending_residency: &[String],
) {
    for (request_id, attempt_id) in bound {
        work.push(rollout::HeldWork {
            key: held_request_key(request_id, attempt_id),
            label: format!("request {request_id} (bound here, not yet reported by the machine)"),
            started_at_unix_ms: None,
            expected_ms: None,
            progress: None,
        });
    }
    let reported = work
        .iter()
        .filter(|item| item.key.starts_with("residency:"))
        .count();
    let mut pending: Vec<&String> = pending_residency.iter().collect();
    pending.sort();
    for correlation_id in pending.into_iter().skip(reported) {
        work.push(rollout::HeldWork {
            key: format!("residency:{correlation_id}"),
            label: "an environment being acquired".to_string(),
            started_at_unix_ms: None,
            expected_ms: None,
            progress: None,
        });
    }
    let mut keys = HashSet::new();
    work.retain(|item| keys.insert(item.key.clone()));
}

pub(super) fn executor_health_snapshot(
    entry: &ExecutorConnectionState,
    captured_at_unix_ms: u64,
    expected_build_ids: &HashMap<String, String>,
) -> ExecutorHealthSnapshot {
    let heartbeat_age_ms =
        captured_at_unix_ms.saturating_sub(entry.advertisement.observed_at_unix_ms);
    let liveness_age_ms = published_facts_age_ms(entry, captured_at_unix_ms);
    let telemetry_stale =
        liveness_age_ms.is_some_and(|age| age > EXECUTOR_TELEMETRY_STALE_AFTER_MS);
    ExecutorHealthSnapshot {
        drain_state: entry.drain_state.clone(),
        identity: entry.identity.clone(),
        public_name: executor_public_name(entry),
        colocated: entry.colocated,
        status: executor_link_status(entry, captured_at_unix_ms),
        heartbeat_age_ms,
        liveness_age_ms,
        telemetry_stale,
        advertisement: (*entry.advertisement).clone(),
        admission: entry.health.admission.clone(),
        queues: entry.health.queues.clone(),
        host: entry.health.host.clone(),
        disk: entry.health.disk.clone(),
        machine: entry.health.machine.clone(),
        inventory: entry.health.inventory.clone(),
        connection_generation: entry.generation,
        applied_policy: entry.health.applied_policy.clone(),
        // One fact with two witnesses: the drain this runner is holding, and
        // the one the executor reports. A machine that has not yet been told
        // about a drain is still drained, so the surfaces read the union.
        drain_mode: entry.drained || entry.health.drain_mode,
        role: entry.machine_role,
        model_residents: entry.model_residents.clone(),
        resident_processes: entry.health.resident_processes.clone(),
        command_processes: entry.health.command_processes.clone(),
        evicted_cells: entry.health.evicted_cells.clone(),
        unadopted_cells: entry.health.unadopted_cells.clone(),
        busy_declined_cells: entry.health.busy_declined_cells.clone(),
        stopped_commands: entry.health.stopped_commands.clone(),
        memory_record: entry.health.memory_record.clone(),
        repository_faults: entry.health.repository_faults.clone(),
        removed_cairn_refs: entry.health.removed_cairn_refs.clone(),
        quarantines: entry.health.quarantines.clone(),
        build_volume: entry.health.build_volume.clone(),
        build_skew: expected_build_ids
            .get(&entry.identity.executor_id)
            .zip(entry.executor_build_id.as_ref())
            .filter(|(expected, running)| expected != running)
            .map(
                |(expected, running)| cairn_common::executor_protocol::BuildSkew {
                    runner_build_id: expected.clone(),
                    executor_build_id: running.clone(),
                },
            ),
    }
}

/// Link health from heartbeat age alone, the one rule both the health snapshot
/// and the telemetry frame report.
pub(super) fn executor_link_status(
    entry: &ExecutorConnectionState,
    captured_at_unix_ms: u64,
) -> ExecutorHealthStatus {
    // The same judgement placement makes. Its fallback evidence, for a link no
    // reader has stamped, is the age of the executor's own last beat, which is
    // what this status has always shown.
    match entry.link_clock.judge(
        link_clock::monotonic_now_ns(),
        captured_at_unix_ms.saturating_sub(entry.advertisement.observed_at_unix_ms),
        EXECUTOR_LINK_STALE_AFTER_MS,
    ) {
        link_clock::LinkJudgement::PeerSilent { .. } => ExecutorHealthStatus::Stale,
        link_clock::LinkJudgement::Fresh | link_clock::LinkJudgement::JudgeAbsent { .. } => {
            ExecutorHealthStatus::Online
        }
    }
}
