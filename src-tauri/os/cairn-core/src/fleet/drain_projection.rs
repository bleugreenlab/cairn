//! Ordered projection of committed drain records and independently owned fences.

use super::*;
use cairn_common::executor_protocol::{DrainBlocker, ExecutorDrainAdmissionEffect};

/// Exact capability for releasing one temporary admission fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionFenceToken {
    executor_id: String,
    incarnation: String,
}

#[derive(Debug)]
#[cfg_attr(not(test), derive(Default))]
pub(super) struct DrainProjection {
    pub(super) initialized: bool,
    pub(super) initialization_error: Option<String>,
    pub(super) records: HashMap<String, Vec<DrainBlocker>>,
    owned_records: HashMap<String, Vec<cairn_config::drain::DrainRecord>>,
    committed_drains: cairn_config::drain::DrainState,
    fences: HashMap<String, (AdmissionFenceToken, DrainBlocker)>,
}

#[cfg(test)]
impl Default for DrainProjection {
    fn default() -> Self {
        Self {
            // Unit fixtures intentionally start empty; production starts closed.
            initialized: true,
            initialization_error: None,
            records: HashMap::new(),
            owned_records: HashMap::new(),
            committed_drains: Default::default(),
            fences: HashMap::new(),
        }
    }
}

impl DrainProjection {
    pub(super) fn view(
        &self,
        executor_id: &str,
    ) -> cairn_common::executor_protocol::ExecutorDrainState {
        let records = self
            .owned_records
            .get(executor_id)
            .cloned()
            .unwrap_or_default();
        let records =
            serde_json::from_value(serde_json::to_value(records).expect("drain records serialize"))
                .expect("common drain record view matches config schema");
        let blockers = self.blockers(executor_id);
        cairn_common::executor_protocol::ExecutorDrainState {
            drain_key: FleetConfig::drain_key(executor_id).to_string(),
            machine_revision: self.committed_drains.machine_revision(executor_id),
            removal_fences: self
                .fences
                .values()
                .filter(|(token, _)| token.executor_id == executor_id)
                .map(|(_, blocker)| blocker.clone())
                .collect(),
            records,
            effective_refusal: !blockers.is_empty(),
            effective_effect: effective_effect(&blockers),
            blockers,
            startup_error: self.initialization_error.clone(),
        }
    }

    pub(super) fn blockers(&self, executor_id: &str) -> Vec<DrainBlocker> {
        let mut blockers = self.records.get(executor_id).cloned().unwrap_or_default();
        blockers.extend(
            self.fences
                .values()
                .filter(|(token, _)| token.executor_id == executor_id)
                .map(|(_, blocker)| blocker.clone()),
        );
        if !self.initialized {
            blockers.push(DrainBlocker {
                owner: "initialization".into(),
                reason: self
                    .initialization_error
                    .clone()
                    .unwrap_or_else(|| "authoritative fleet settings have not been loaded".into()),
                generation: 0,
                incarnation: None,
                estimate: None,
                admission_effect: ExecutorDrainAdmissionEffect::RefuseAllNewAdmission,
            });
        }
        blockers.sort_by(|a, b| {
            (&a.owner, a.generation, &a.incarnation).cmp(&(&b.owner, b.generation, &b.incarnation))
        });
        blockers
    }
}

/// The strictest effect among standing holds, `None` when nothing stands.
pub(super) fn effective_effect(blockers: &[DrainBlocker]) -> Option<ExecutorDrainAdmissionEffect> {
    if blockers.is_empty() {
        None
    } else if blockers
        .iter()
        .any(|row| row.admission_effect == ExecutorDrainAdmissionEffect::RefuseAllNewAdmission)
    {
        Some(ExecutorDrainAdmissionEffect::RefuseAllNewAdmission)
    } else {
        Some(ExecutorDrainAdmissionEffect::RefuseNewEnvironments)
    }
}

/// The executor's own `DrainModeRequest` boolean. Only a hold refusing all new
/// admission sets it: the executor cannot tell resident work from new, so a
/// self-update hold alone is enforced by runner placement and leaves it false.
pub(super) fn protocol_refusal(blockers: &[DrainBlocker]) -> bool {
    effective_effect(blockers) == Some(ExecutorDrainAdmissionEffect::RefuseAllNewAdmission)
}

/// Whether work placed on this machine runs in an environment already resident
/// there: a residency pin to it, for anything but a check lane. A check lane
/// builds its own cell, so it is a new environment even when pinned.
pub(super) fn resident_scope(pinned_here: bool, work_class: PlacementWorkClass) -> bool {
    pinned_here
        && !matches!(
            work_class,
            PlacementWorkClass::ReviewChecks | PlacementWorkClass::WriteChecks
        )
}

pub(super) fn resident_work(request: &CellRequest, executor_id: &str) -> bool {
    resident_scope(
        request.pinned_executor_id.as_deref() == Some(executor_id),
        request.placement_work_class,
    )
}

/// The standing holds that refuse this work. A `refuse_new_environments` hold
/// admits resident work; every other hold refuses everything new.
pub(super) fn refusing(blockers: &[DrainBlocker], resident: bool) -> Vec<DrainBlocker> {
    blockers
        .iter()
        .filter(|row| {
            !(resident
                && row.admission_effect == ExecutorDrainAdmissionEffect::RefuseNewEnvironments)
        })
        .cloned()
        .collect()
}

/// Whether this machine's holds refuse the work. A machine marked drained with
/// no recorded blocker refuses everything, as the strictest reading.
pub(super) fn entry_refuses(entry: &ExecutorConnectionState, resident: bool) -> bool {
    entry.drained
        && (entry.drain_blockers.is_empty()
            || !refusing(&entry.drain_blockers, resident).is_empty())
}

pub(super) fn rejection(entry: &ExecutorConnectionState) -> PlacementRejectionReason {
    use cairn_common::executor_protocol::{ExecutorDrainOwner, ExecutorRuntimeDrainKind};
    let records = &entry.drain_state.records;
    if !records.is_empty()
        && records.len() == entry.drain_blockers.len()
        && records.iter().all(|record| matches!(&record.owner,
            ExecutorDrainOwner::Runtime { context } if context.kind == ExecutorRuntimeDrainKind::SelfUpdate))
    {
        PlacementRejectionReason::UpdatePending
    } else {
        PlacementRejectionReason::Drained
    }
}

pub(super) struct AdmissionSendError {
    pub(super) diagnostic: String,
    pub(super) blockers: Vec<DrainBlocker>,
    pub(super) reason: PlacementRejectionReason,
}

impl Fleet {
    /// Publish the committed durable projection while the settings mutation lock
    /// is held. This performs no disk, network waits or acknowledgement handling.
    pub fn publish_drain_projection(&self, config: &FleetConfig) {
        use cairn_config::drain::{DrainOwner, RuntimeDrainKind};
        let owned_records: HashMap<_, _> =
            config.drain_records_executor_ids().into_iter().collect();
        let records = owned_records
            .clone()
            .into_iter()
            .map(|(id, records)| {
                let blockers = records
                    .into_iter()
                    .map(|record| {
                        let admission_effect = match record.admission_effect() {
                            cairn_config::drain::AdmissionEffect::RefuseAllNewAdmission => {
                                ExecutorDrainAdmissionEffect::RefuseAllNewAdmission
                            }
                            cairn_config::drain::AdmissionEffect::RefuseNewEnvironments => {
                                ExecutorDrainAdmissionEffect::RefuseNewEnvironments
                            }
                        };
                        let (owner, incarnation, estimate) = match &record.owner {
                            DrainOwner::Operator => ("operator".into(), None, None),
                            DrainOwner::Thread { thread_id } => {
                                (format!("thread:{thread_id}"), None, None)
                            }
                            DrainOwner::Legacy => ("legacy".into(), None, None),
                            DrainOwner::Runtime { context } => (
                                format!(
                                    "runtime:{:?}:{}:{}",
                                    context.kind, context.operation_id, context.turn_id
                                ),
                                Some(context.runner_boot_id.clone()),
                                (context.kind == RuntimeDrainKind::SelfUpdate)
                                    .then(|| "unknown".into()),
                            ),
                        };
                        DrainBlocker {
                            owner,
                            reason: record.reason,
                            generation: record.generation,
                            incarnation,
                            estimate,
                            admission_effect,
                        }
                    })
                    .collect();
                (id, blockers)
            })
            .collect();
        let mut projection = self.drain_projection.lock().unwrap();
        projection.records = records;
        projection.owned_records = owned_records;
        projection.committed_drains = config.drains.clone();
        projection.initialized = true;
        projection.initialization_error = None;
        self.stamp_drain_projection(&projection);
    }

    /// Publish an initialization failure without opening admission.
    pub fn set_drain_initialization_error(&self, error: String) {
        let mut projection = self.drain_projection.lock().unwrap();
        projection.initialized = false;
        projection.initialization_error = Some(error);
        self.stamp_drain_projection(&projection);
    }

    pub fn drain_initialization_error(&self) -> Option<String> {
        self.drain_projection
            .lock()
            .unwrap()
            .initialization_error
            .clone()
    }

    /// Whether an authoritative load has published, i.e. admission may be open.
    pub fn drain_projection_initialized(&self) -> bool {
        self.drain_projection.lock().unwrap().initialized
    }

    /// A fence is independent of durable records and every other fence.
    pub fn fence_admission(&self, executor_id: &str) -> AdmissionFenceToken {
        self.fence_admission_owned(
            executor_id,
            "removal".into(),
            "machine removal in progress".into(),
        )
    }

    pub fn fence_admission_owned(
        &self,
        executor_id: &str,
        owner: String,
        reason: String,
    ) -> AdmissionFenceToken {
        let token = AdmissionFenceToken {
            executor_id: executor_id.into(),
            incarnation: uuid::Uuid::new_v4().to_string(),
        };
        let mut projection = self.drain_projection.lock().unwrap();
        projection.fences.insert(
            token.incarnation.clone(),
            (
                token.clone(),
                DrainBlocker {
                    owner,
                    reason,
                    generation: 1,
                    incarnation: Some(token.incarnation.clone()),
                    estimate: None,
                    admission_effect: ExecutorDrainAdmissionEffect::RefuseAllNewAdmission,
                },
            ),
        );
        self.stamp_drain_projection(&projection);
        token
    }

    pub fn lift_admission_fence(&self, token: &AdmissionFenceToken) -> bool {
        let mut projection = self.drain_projection.lock().unwrap();
        if !projection
            .fences
            .get(&token.incarnation)
            .is_some_and(|(held, _)| held == token)
        {
            return false;
        }
        projection.fences.remove(&token.incarnation);
        self.stamp_drain_projection(&projection);
        true
    }

    pub fn owned_drain_records(&self, executor_id: &str) -> Vec<cairn_config::drain::DrainRecord> {
        self.drain_projection
            .lock()
            .unwrap()
            .owned_records
            .get(executor_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn admission_fences(&self, executor_id: &str) -> Vec<DrainBlocker> {
        let projection = self.drain_projection.lock().unwrap();
        projection
            .fences
            .values()
            .filter(|(token, _)| token.executor_id == executor_id)
            .map(|(_, blocker)| blocker.clone())
            .collect()
    }

    pub fn drain_blockers(&self, executor_id: &str) -> Vec<DrainBlocker> {
        self.drain_projection.lock().unwrap().blockers(executor_id)
    }

    /// Admission and its wire frame are linearized against drain publication.
    /// Already admitted execution recovery and cleanup do not call this gate.
    /// `resident` says the work runs in an environment already resident on the
    /// selected machine, which a `refuse_new_environments` hold admits.
    pub(super) fn send_new_admission(
        &self,
        selected: &SelectedExecutor,
        request: &CellRequest,
        resident: bool,
        message: ExecutorMessage,
    ) -> Result<(), Box<AdmissionSendError>> {
        let projection = self.drain_projection.lock().unwrap();
        let blockers = projection.blockers(&selected.executor_id);
        let refusing = refusing(&blockers, resident);
        if !refusing.is_empty() {
            // Only self-update holds refuse it: the work waits for the update.
            let reason = if refusing.iter().all(|row| {
                row.admission_effect == ExecutorDrainAdmissionEffect::RefuseNewEnvironments
            }) {
                PlacementRejectionReason::UpdatePending
            } else {
                PlacementRejectionReason::Drained
            };
            let diagnostic = format!(
                "{} refuses new admission: {}",
                selected.executor_id,
                refusing
                    .iter()
                    .map(|row| format!(
                        "{} generation {}: {}",
                        row.owner, row.generation, row.reason
                    ))
                    .collect::<Vec<_>>()
                    .join("; ")
            );
            drop(projection);
            let decision = placement_decision(
                request,
                unix_time_ms(),
                None,
                None,
                PlacementOutcome::Refused {
                    diagnostic: diagnostic.clone(),
                },
                vec![PlacementRejection {
                    executor_name: self
                        .executor_public_name(&selected.executor_id)
                        .unwrap_or_else(|| selected.executor_id.clone()),
                    executor_id: selected.executor_id.clone(),
                    reason: reason.clone(),
                    prediction: None,
                    drain_blockers: blockers.clone(),
                }],
            );
            self.queue.record_unqueued_refusal(request, &decision);
            self.record_placement_decision(decision);
            return Err(Box::new(AdmissionSendError {
                diagnostic,
                blockers,
                reason,
            }));
        }
        let connections = self.lock_connections();
        if !connections
            .get(&selected.executor_id)
            .is_some_and(|entry| entry.generation == selected.generation)
        {
            return Err(Box::new(AdmissionSendError {
                diagnostic: "selected executor connection generation changed before admission"
                    .into(),
                blockers: Vec::new(),
                reason: PlacementRejectionReason::Drained,
            }));
        }
        selected.sender.send(message).map_err(|_| {
            Box::new(AdmissionSendError {
                diagnostic: "executor link closed before admission".into(),
                blockers: Vec::new(),
                reason: PlacementRejectionReason::Drained,
            })
        })
    }

    pub fn is_drained(&self, executor_id: &str) -> bool {
        !self
            .drain_projection
            .lock()
            .unwrap()
            .blockers(executor_id)
            .is_empty()
    }

    /// The committed owned records, read under the projection lock.
    pub(super) fn with_committed_drain_records<R>(
        &self,
        read: impl FnOnce(&HashMap<String, Vec<cairn_config::drain::DrainRecord>>) -> R,
    ) -> R {
        read(&self.drain_projection.lock().unwrap().owned_records)
    }

    /// Caller holds projection before connections, including enqueue, on every path.
    ///
    /// Blockers, records and revisions restamp on every publication; the
    /// machine hears a frame only when the one boolean it enforces changes.
    /// The entry's blockers are what it was last told: attach enqueues the
    /// initial frame under these same locks, from the blockers it stamps.
    fn stamp_drain_projection(&self, projection: &DrainProjection) {
        let mut connections = self.lock_connections();
        for (executor_id, entry) in connections.iter_mut() {
            let told = protocol_refusal(&entry.drain_blockers);
            entry.drain_state = projection.view(executor_id);
            entry.drain_blockers = projection.blockers(executor_id);
            entry.drained = !entry.drain_blockers.is_empty();
            let refusal = protocol_refusal(&entry.drain_blockers);
            if refusal != told {
                let _ = entry.sender.send(ExecutorMessage::DrainModeRequest {
                    correlation_id: uuid::Uuid::new_v4().to_string(),
                    enabled: refusal,
                });
            }
        }
        self.connection_ready.notify_waiters();
    }

    #[cfg(test)]
    pub fn hydrate_drained(&self, executor_ids: impl IntoIterator<Item = String>) {
        let mut projection = self.drain_projection.lock().unwrap();
        projection.initialized = true;
        projection.records = executor_ids
            .into_iter()
            .map(|id| {
                (
                    id,
                    vec![DrainBlocker {
                        owner: "test".into(),
                        reason: "test drain".into(),
                        generation: 1,
                        incarnation: None,
                        estimate: None,
                        admission_effect: Default::default(),
                    }],
                )
            })
            .collect();
        self.stamp_drain_projection(&projection);
    }

    #[cfg(test)]
    pub async fn set_executor_drain_mode(
        &self,
        executor_id: &str,
        enabled: bool,
    ) -> DrainApplication {
        let mut projection = self.drain_projection.lock().unwrap();
        if enabled {
            projection.records.insert(
                executor_id.into(),
                vec![DrainBlocker {
                    owner: "test".into(),
                    reason: "test drain".into(),
                    generation: 1,
                    incarnation: None,
                    estimate: None,
                    admission_effect: Default::default(),
                }],
            );
        } else {
            projection.records.remove(executor_id);
        }
        self.stamp_drain_projection(&projection);
        DrainApplication::HeldWithoutMachine {
            reason: "test publication does not await acknowledgement".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn last_drain(inbox: &mut mpsc::UnboundedReceiver<ExecutorMessage>) -> Option<bool> {
        let mut last = None;
        while let Ok(message) = inbox.try_recv() {
            if let ExecutorMessage::DrainModeRequest { enabled, .. } = message {
                last = Some(enabled);
            }
        }
        last
    }

    #[test]
    fn early_attach_stays_closed_until_authoritative_publication() {
        let fleet = Fleet::default();
        fleet.drain_projection.lock().unwrap().initialized = false;
        let (sender, mut inbox) = mpsc::unbounded_channel();
        fleet.attach_executor(sender);
        assert_eq!(last_drain(&mut inbox), Some(true));
        assert!(fleet.is_drained(COLOCATED_EXECUTOR_ID));
        fleet.set_drain_initialization_error("invalid drain document".into());
        // Still refusing: the diagnostic restamps without a repeated frame.
        assert_eq!(last_drain(&mut inbox), None);
        assert_eq!(
            fleet.drain_blockers(COLOCATED_EXECUTOR_ID)[0].reason,
            "invalid drain document"
        );
        fleet.publish_drain_projection(&FleetConfig::default());
        assert_eq!(last_drain(&mut inbox), Some(false));
        assert!(!fleet.is_drained(COLOCATED_EXECUTOR_ID));
        assert!(fleet.drain_initialization_error().is_none());
    }

    #[test]
    fn a_delayed_clear_acknowledgement_cannot_replace_a_newer_fence() {
        let fleet = Fleet::default();
        let (sender, mut inbox) = mpsc::unbounded_channel();
        let generation = fleet.attach_executor(sender);
        let (waiter, response) = oneshot::channel();
        fleet.pending_drain.lock().unwrap().insert(
            "old-clear".into(),
            PendingDrainResult {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                generation,
                waiter,
            },
        );
        fleet.fence_admission(COLOCATED_EXECUTOR_ID);
        fleet.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::DrainModeResponse {
                correlation_id: "old-clear".into(),
                result: Ok(false),
            },
        );
        assert!(!response.blocking_recv().unwrap().unwrap());
        assert!(fleet.is_drained(COLOCATED_EXECUTOR_ID));
        assert_eq!(last_drain(&mut inbox), Some(true));
        assert!(
            fleet
                .lock_connections()
                .get(COLOCATED_EXECUTOR_ID)
                .unwrap()
                .drained
        );
    }

    #[test]
    fn hydration_cannot_erase_overlapping_fences_and_release_is_exact() {
        let fleet = Fleet::default();
        let (sender, mut inbox) = mpsc::unbounded_channel();
        fleet.attach_executor(sender);
        let first = fleet.fence_admission_owned(
            COLOCATED_EXECUTOR_ID,
            "remove-a".into(),
            "first removal".into(),
        );
        let second = fleet.fence_admission_owned(
            COLOCATED_EXECUTOR_ID,
            "remove-b".into(),
            "second removal".into(),
        );
        fleet.publish_drain_projection(&FleetConfig::default());
        assert_eq!(last_drain(&mut inbox), Some(true));
        let fences = fleet.admission_fences(COLOCATED_EXECUTOR_ID);
        assert_eq!(fences.len(), 2);
        assert_ne!(fences[0].incarnation, fences[1].incarnation);
        assert!(fleet.lift_admission_fence(&first));
        assert!(!fleet.lift_admission_fence(&first));
        assert!(fleet.is_drained(COLOCATED_EXECUTOR_ID));
        assert_eq!(last_drain(&mut inbox), None);
        assert!(fleet.lift_admission_fence(&second));
        assert_eq!(last_drain(&mut inbox), Some(false));
    }

    #[test]
    fn attach_publication_and_fence_interleavings_end_with_the_current_state() {
        for _ in 0..32 {
            let fleet = Fleet::default();
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let (sender, mut inbox) = mpsc::unbounded_channel();
            let attaching = {
                let fleet = fleet.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    fleet.attach_executor(sender);
                })
            };
            barrier.wait();
            let fence = fleet.fence_admission(COLOCATED_EXECUTOR_ID);
            attaching.join().unwrap();
            assert_eq!(last_drain(&mut inbox), Some(true));
            fleet.publish_drain_projection(&FleetConfig::default());
            assert_eq!(last_drain(&mut inbox), None);
            assert!(fleet.is_drained(COLOCATED_EXECUTOR_ID));
            fleet.lift_admission_fence(&fence);
            assert_eq!(last_drain(&mut inbox), Some(false));
        }
    }

    #[test]
    fn drain_wait_diagnostics_name_the_public_machine_not_its_opaque_identity() {
        let request = super::super::tests::targeted_request("macos");
        let decision = wait_decision(
            &request,
            &ActivePlacementPolicy::default_profile(),
            &[PlacementRejection {
                executor_id: "private-executor-id".into(),
                executor_name: "public-machine".into(),
                reason: PlacementRejectionReason::Drained,
                prediction: None,
                drain_blockers: vec![DrainBlocker {
                    owner: "operator".into(),
                    reason: "storage repair".into(),
                    generation: 3,
                    incarnation: None,
                    estimate: None,
                    admission_effect: Default::default(),
                }],
            }],
            &[],
            &[],
        )
        .unwrap();
        let PlacementOutcome::Waiting { diagnostic } = decision.outcome else {
            panic!("a drain records a waiting decision");
        };
        assert!(diagnostic.contains("public-machine"));
        assert!(diagnostic.contains("operator generation 3: storage repair"));
        assert!(!diagnostic.contains("private-executor-id"));
    }

    #[test]
    fn a_final_refusal_preserves_the_same_attempts_earlier_wait() {
        let fleet = Fleet::default();
        let request = super::super::tests::targeted_request("macos");
        let waiting = placement_decision(
            &request,
            42,
            None,
            None,
            PlacementOutcome::Waiting {
                diagnostic: "waiting for repair".into(),
            },
            vec![PlacementRejection {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                executor_name: "local".into(),
                reason: PlacementRejectionReason::Drained,
                prediction: None,
                drain_blockers: Vec::new(),
            }],
        );
        let mut refused = waiting.clone();
        refused.outcome = PlacementOutcome::Refused {
            diagnostic: "wait horizon reached".into(),
        };
        fleet.record_placement_decision(waiting.clone());
        fleet.record_placement_decision(refused.clone());
        let decisions = fleet.placements_naming(COLOCATED_EXECUTOR_ID);
        assert_eq!(decisions.len(), 2);
        assert!(decisions.contains(&waiting));
        assert!(decisions.contains(&refused));
    }

    #[test]
    fn forty_live_drain_waits_keep_batch_and_executor_evidence_after_history_eviction() {
        let fleet = Fleet::default();
        let (sender, _inbox) = mpsc::unbounded_channel();
        fleet.attach_executor(sender);
        let _fence = fleet.fence_admission_owned(
            COLOCATED_EXECUTOR_ID,
            "maintenance".into(),
            "storage repair".into(),
        );
        let mut waiters = Vec::new();
        for index in 0..40 {
            let mut request = super::super::tests::targeted_request("macos");
            request.request_id = format!("waiting-batch-{index}");
            let (_, waiter) = fleet
                .queue
                .enqueue(
                    queue::NewEntry {
                        request: request.clone(),
                        admission_kind: CellAdmissionKind::Command,
                        interactive: false,
                        execution_residency: None,
                        standing: false,
                    },
                    unix_time_ms(),
                )
                .unwrap();
            waiters.push(waiter);
            fleet.record_placement_decision(placement_decision(
                &request,
                unix_time_ms(),
                None,
                None,
                PlacementOutcome::Waiting {
                    diagnostic: "storage repair".into(),
                },
                vec![PlacementRejection {
                    executor_id: COLOCATED_EXECUTOR_ID.into(),
                    executor_name: "local".into(),
                    reason: PlacementRejectionReason::Drained,
                    prediction: None,
                    drain_blockers: fleet.drain_blockers(COLOCATED_EXECUTOR_ID),
                }],
            ));
        }
        assert_eq!(
            fleet.recent_placements.lock().unwrap().len(),
            RECENT_PLACEMENT_DECISIONS
        );
        let snapshot = fleet.snapshot();
        assert_eq!(snapshot.queued_requests.len(), 40);
        for request in &snapshot.queued_requests {
            let blockers = &request.placement.as_ref().unwrap().rejected[0].drain_blockers;
            assert_eq!(blockers[0].owner, "maintenance");
            assert_eq!(blockers[0].reason, "storage repair");
            assert_eq!(blockers[0].generation, 1);
        }
        let inspection = fleet.inspect_executors(unix_time_ms());
        assert_eq!(inspection[0].recent_placements.len(), 40);
        assert!(inspection[0]
            .recent_placements
            .iter()
            .any(|decision| decision.request_id == "waiting-batch-0"));
        for decision in &inspection[0].recent_placements {
            assert_eq!(
                decision.rejected[0].drain_blockers[0].reason,
                "storage repair"
            );
        }
    }

    #[test]
    fn a_new_grant_after_a_fence_is_refused_and_journals_every_blocker() {
        let fleet = Fleet::default();
        let (sender, mut inbox) = mpsc::unbounded_channel();
        fleet.attach_executor(sender);
        let selected =
            selected_executor(fleet.lock_connections().get(COLOCATED_EXECUTOR_ID).unwrap());
        let _first =
            fleet.fence_admission_owned(COLOCATED_EXECUTOR_ID, "a".into(), "reason a".into());
        let _second =
            fleet.fence_admission_owned(COLOCATED_EXECUTOR_ID, "b".into(), "reason b".into());
        let mut request = super::super::tests::targeted_request("macos");
        request.request_id = "stable-batch".into();
        request.attempt_id = "stable-attempt".into();
        request.pinned_executor_id = Some(COLOCATED_EXECUTOR_ID.into());
        let (journal, mut rows) = mpsc::unbounded_channel();
        fleet.queue.attach_journal(journal);
        let (_entry, _waiter) = fleet
            .queue
            .enqueue(
                queue::NewEntry {
                    request: request.clone(),
                    admission_kind: CellAdmissionKind::Command,
                    interactive: false,
                    execution_residency: None,
                    standing: false,
                },
                unix_time_ms(),
            )
            .unwrap();
        rows.try_recv().unwrap();
        assert!(fleet
            .send_new_admission(
                &selected,
                &request,
                true,
                ExecutorMessage::DrainModeRequest {
                    correlation_id: "must-not-send".into(),
                    enabled: false,
                }
            )
            .is_err());
        assert_eq!(last_drain(&mut inbox), Some(true));
        let placements = fleet.placements_naming(COLOCATED_EXECUTOR_ID);
        assert_eq!(placements[0].request_id, "stable-batch");
        assert_eq!(placements[0].attempt_id, "stable-attempt");
        assert_eq!(placements[0].rejected[0].drain_blockers.len(), 2);
        let journaled = rows.try_recv().unwrap();
        let json: serde_json::Value = serde_json::from_str(&journaled[0].scoring_json).unwrap();
        let decision: PlacementDecision =
            serde_json::from_value(json["placementDecision"].clone()).unwrap();
        assert_eq!(decision, placements[0]);
        assert_eq!(journaled[0].request_id, "stable-batch");
        fleet
            .queue
            .take(
                &(request.request_id.clone(), request.attempt_id.clone()),
                queue::TerminalState::Completed,
                unix_time_ms(),
            )
            .unwrap();
        rows.try_recv().unwrap();
        assert!(fleet
            .send_new_admission(
                &selected,
                &request,
                true,
                ExecutorMessage::DrainModeRequest {
                    correlation_id: "unqueued-must-not-send".into(),
                    enabled: false,
                }
            )
            .is_err());
        let unqueued = rows.try_recv().unwrap();
        assert_eq!(unqueued[0].state, "completed");
        assert_eq!(
            unqueued[0].executor_id.as_deref(),
            Some(COLOCATED_EXECUTOR_ID)
        );
        let json: serde_json::Value = serde_json::from_str(&unqueued[0].scoring_json).unwrap();
        let restored: PlacementDecision =
            serde_json::from_value(json["placementDecision"].clone()).unwrap();
        assert_eq!(restored.request_id, request.request_id);
        assert_eq!(restored.rejected[0].drain_blockers.len(), 2);
    }

    fn self_update_config(executor_id: &str) -> FleetConfig {
        use cairn_config::drain::{AdmissionEffect, RuntimeDrainContext, RuntimeDrainKind};
        let mut config = FleetConfig::default();
        config
            .drains
            .impose_runtime(
                executor_id,
                RuntimeDrainContext {
                    kind: RuntimeDrainKind::SelfUpdate,
                    operation_id: "turn".into(),
                    turn_id: "turn".into(),
                    runner_boot_id: "boot".into(),
                    admission_effect: AdmissionEffect::RefuseNewEnvironments,
                },
                &test_principal(),
                "self-update to build-b; completion estimate unknown",
                1,
            )
            .unwrap();
        config
    }

    fn test_principal() -> cairn_common::identity::PrincipalRef {
        cairn_common::identity::PrincipalRef::Human {
            issuer: "test".into(),
            subject: "operator".into(),
            organization: None,
        }
    }

    /// cairn/6382's hold never reached the executor; its owned record keeps it
    /// that way. Only a hold refusing everything new flips the boolean, so an
    /// overlapping operator drain or fence still closes the executor itself.
    #[test]
    fn the_executor_boolean_is_set_only_by_a_hold_refusing_all_new_admission() {
        let fleet = Fleet::default();
        let (sender, mut inbox) = mpsc::unbounded_channel();
        fleet.attach_executor(sender);
        let mut config = self_update_config(COLOCATED_EXECUTOR_ID);
        fleet.publish_drain_projection(&config);
        assert_eq!(last_drain(&mut inbox), Some(false));
        assert!(fleet.is_drained(COLOCATED_EXECUTOR_ID));
        let view = fleet
            .drain_projection
            .lock()
            .unwrap()
            .view(COLOCATED_EXECUTOR_ID);
        assert!(view.effective_refusal);
        assert_eq!(
            view.effective_effect,
            Some(ExecutorDrainAdmissionEffect::RefuseNewEnvironments)
        );
        assert_eq!(view.blockers[0].estimate.as_deref(), Some("unknown"));

        let fence = fleet.fence_admission(COLOCATED_EXECUTOR_ID);
        assert_eq!(last_drain(&mut inbox), Some(true));
        assert!(fleet.lift_admission_fence(&fence));
        assert_eq!(last_drain(&mut inbox), Some(false));

        config
            .drains
            .impose_operator(COLOCATED_EXECUTOR_ID, &test_principal(), "maintenance", 2)
            .unwrap();
        fleet.publish_drain_projection(&config);
        assert_eq!(last_drain(&mut inbox), Some(true));
        assert_eq!(
            fleet
                .drain_projection
                .lock()
                .unwrap()
                .view(COLOCATED_EXECUTOR_ID)
                .effective_effect,
            Some(ExecutorDrainAdmissionEffect::RefuseAllNewAdmission)
        );
        // A link attaching now reads the same derived boolean.
        let (sender, mut reattached) = mpsc::unbounded_channel();
        fleet.attach_executor(sender);
        assert_eq!(last_drain(&mut reattached), Some(true));
    }

    /// The admission gate applies the same effect placement does: a resident
    /// job's pinned batch is sent, while unpinned work and a pinned check lane
    /// are refused and journal the self-update blocker with its estimate.
    #[test]
    fn under_a_self_update_drain_only_resident_work_is_sent() {
        let fleet = Fleet::default();
        let (sender, mut inbox) = mpsc::unbounded_channel();
        fleet.attach_executor(sender);
        fleet.publish_drain_projection(&self_update_config(COLOCATED_EXECUTOR_ID));
        let selected =
            selected_executor(fleet.lock_connections().get(COLOCATED_EXECUTOR_ID).unwrap());
        let frame = |id: &str| ExecutorMessage::DrainModeRequest {
            correlation_id: id.into(),
            enabled: false,
        };
        let mut resident = super::super::tests::targeted_request("macos");
        resident.request_id = "resident-batch".into();
        resident.pinned_executor_id = Some(COLOCATED_EXECUTOR_ID.into());
        assert!(resident_work(&resident, COLOCATED_EXECUTOR_ID));
        assert!(fleet
            .send_new_admission(&selected, &resident, true, frame("resident"))
            .is_ok());

        let mut check = resident.clone();
        check.request_id = "pinned-check".into();
        check.placement_work_class = PlacementWorkClass::WriteChecks;
        assert!(!resident_work(&check, COLOCATED_EXECUTOR_ID));
        let mut fresh = resident.clone();
        fresh.request_id = "fresh-batch".into();
        fresh.pinned_executor_id = None;
        assert!(!resident_work(&fresh, COLOCATED_EXECUTOR_ID));
        for (request, id) in [(&check, "check"), (&fresh, "fresh")] {
            let error = fleet
                .send_new_admission(&selected, request, false, frame(id))
                .expect_err("new environments are refused");
            assert_eq!(error.reason, PlacementRejectionReason::UpdatePending);
        }
        let sent: Vec<String> = std::iter::from_fn(|| inbox.try_recv().ok())
            .filter_map(|message| match message {
                ExecutorMessage::DrainModeRequest { correlation_id, .. } => Some(correlation_id),
                _ => None,
            })
            .collect();
        assert!(sent.iter().any(|id| id == "resident"), "{sent:?}");
        assert!(
            !sent.iter().any(|id| id == "check" || id == "fresh"),
            "{sent:?}"
        );
        let placements = fleet.placements_naming(COLOCATED_EXECUTOR_ID);
        for id in ["pinned-check", "fresh-batch"] {
            let decision = placements
                .iter()
                .find(|decision| decision.request_id == id)
                .expect("every refusal is journaled");
            let blocker = &decision.rejected[0].drain_blockers[0];
            assert!(
                blocker.owner.starts_with("runtime:SelfUpdate"),
                "{blocker:?}"
            );
            assert_eq!(blocker.generation, 1);
            assert_eq!(blocker.estimate.as_deref(), Some("unknown"));
            assert!(blocker.reason.contains("self-update to build-b"));
        }
        assert!(!placements
            .iter()
            .any(|decision| decision.request_id == "resident-batch"));
    }
}

#[cfg(test)]
mod owned_tests {
    use super::*;
    use cairn_common::identity::PrincipalRef;

    #[test]
    fn publishing_owned_records_preserves_other_owners_and_wire_union() {
        let fleet = Fleet::default();
        let (sender, mut inbox) = mpsc::unbounded_channel();
        fleet.attach_executor(sender);
        let principal = PrincipalRef::Human {
            issuer: "test".into(),
            subject: "alice".into(),
            organization: None,
        };
        let mut config = FleetConfig::default();
        config
            .drains
            .impose_operator(COLOCATED_EXECUTOR_ID, &principal, "operator reason", 10)
            .unwrap();
        config
            .drains
            .impose_thread(
                COLOCATED_EXECUTOR_ID,
                "thread-id",
                &principal,
                "thread reason",
                11,
            )
            .unwrap();
        fleet.publish_drain_projection(&config);
        let view = fleet
            .lock_connections()
            .get(COLOCATED_EXECUTOR_ID)
            .unwrap()
            .drain_state
            .clone();
        assert_eq!(
            view.drain_key,
            FleetConfig::drain_key(COLOCATED_EXECUTOR_ID)
        );
        assert_eq!(view.machine_revision, 2);
        assert_eq!(view.records.len(), 2);
        assert_eq!(view.blockers.len(), 2);
        assert_eq!(view.blockers.iter().map(|r| r.generation).max(), Some(2));
        config
            .drains
            .clear_thread(COLOCATED_EXECUTOR_ID, "thread-id")
            .unwrap();
        fleet.publish_drain_projection(&config);
        assert_eq!(fleet.owned_drain_records(COLOCATED_EXECUTOR_ID).len(), 1);
        assert_eq!(
            fleet
                .drain_projection
                .lock()
                .unwrap()
                .view(COLOCATED_EXECUTOR_ID)
                .machine_revision,
            3
        );
        config
            .drains
            .impose_thread("remote", "thread-id", &principal, "temporary", 12)
            .unwrap();
        config.drains.clear_thread("remote", "thread-id").unwrap();
        fleet.publish_drain_projection(&config);
        let empty = fleet.drain_projection.lock().unwrap().view("remote");
        assert_eq!(empty.drain_key, "remote");
        assert_eq!(empty.machine_revision, 2);
        assert!(empty.records.is_empty());
        assert!(fleet.is_drained(COLOCATED_EXECUTOR_ID));
        let mut last = None;
        while let Ok(message) = inbox.try_recv() {
            if let ExecutorMessage::DrainModeRequest { enabled, .. } = message {
                last = Some(enabled);
            }
        }
        assert_eq!(last, Some(true));
    }

    /// A publication that leaves the executor's boolean alone sends nothing,
    /// yet still restamps the blockers and revision diagnostics read; a change
    /// of the boolean and a fresh attach are still told.
    #[test]
    fn only_a_changed_executor_boolean_is_broadcast() {
        use cairn_config::drain::{AdmissionEffect, RuntimeDrainContext, RuntimeDrainKind};
        let drain_frames = |inbox: &mut mpsc::UnboundedReceiver<ExecutorMessage>| {
            std::iter::from_fn(|| inbox.try_recv().ok())
                .filter_map(|message| match message {
                    ExecutorMessage::DrainModeRequest { enabled, .. } => Some(enabled),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        let fleet = Fleet::default();
        let (sender, mut inbox) = mpsc::unbounded_channel();
        fleet.attach_executor(sender);
        assert_eq!(drain_frames(&mut inbox), vec![false]);
        let principal = PrincipalRef::Human {
            issuer: "test".into(),
            subject: "alice".into(),
            organization: None,
        };
        let mut config = FleetConfig::default();
        config
            .drains
            .impose_thread(COLOCATED_EXECUTOR_ID, "thread", &principal, "repair", 1)
            .unwrap();
        fleet.publish_drain_projection(&config);
        assert_eq!(drain_frames(&mut inbox), vec![true]);

        // The same committed state again: nothing on the wire.
        fleet.publish_drain_projection(&config);
        assert!(drain_frames(&mut inbox).is_empty());

        // Owned facts change while the boolean holds: diagnostics restamp.
        config
            .drains
            .impose_runtime(
                COLOCATED_EXECUTOR_ID,
                RuntimeDrainContext {
                    kind: RuntimeDrainKind::SelfUpdate,
                    operation_id: "turn".into(),
                    turn_id: "turn".into(),
                    runner_boot_id: "boot".into(),
                    admission_effect: AdmissionEffect::RefuseNewEnvironments,
                },
                &principal,
                "self-update to build-a; completion estimate unknown",
                2,
            )
            .unwrap();
        fleet.publish_drain_projection(&config);
        assert!(drain_frames(&mut inbox).is_empty());
        {
            let connections = fleet.lock_connections();
            let entry = connections.get(COLOCATED_EXECUTOR_ID).unwrap();
            assert_eq!(entry.drain_blockers.len(), 2);
            assert_eq!(entry.drain_state.machine_revision, 2);
            assert_eq!(entry.drain_state.records.len(), 2);
        }

        // A fresh link is told its state at attach.
        let (sender, mut reattached) = mpsc::unbounded_channel();
        fleet.attach_executor(sender);
        assert_eq!(drain_frames(&mut reattached), vec![true]);

        // Clearing the only hold refusing everything flips the boolean.
        config
            .drains
            .clear_thread(COLOCATED_EXECUTOR_ID, "thread")
            .unwrap();
        fleet.publish_drain_projection(&config);
        assert_eq!(drain_frames(&mut reattached), vec![false]);
        assert!(fleet.is_drained(COLOCATED_EXECUTOR_ID));
    }
}
