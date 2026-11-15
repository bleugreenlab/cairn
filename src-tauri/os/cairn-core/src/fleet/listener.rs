use super::*;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerRotationPendingMachine {
    pub executor_id: String,
    pub name: String,
    pub presence_cause: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerRotationStatus {
    pub staged_fingerprint: String,
    pub started_at_unix_ms: u64,
    pub pending_executor_ids: Vec<String>,
}

pub trait ListenerRotationControl: Send + Sync {
    fn pending(&self) -> Result<Option<ListenerRotationStatus>, String>;
    fn regenerate(&self, started_at_unix_ms: u64) -> Result<ListenerRotationStatus, String>;
    fn force_cutover(&self) -> Result<Vec<String>, String>;
    fn update_endpoints(&self, endpoints: Vec<String>) -> Result<(), String>;
    /// Record, before the machine can be sent anything at all, that it is now
    /// eligible to receive a staged listener advertisement.
    ///
    /// This is the event boundary the cut-over gate rests on. A hold
    /// reconstructed afterwards from live connections cannot prove that a
    /// machine did not appear and vanish in between, and a machine that did
    /// would take an unacknowledged pin with it. Written here, the hold is
    /// durable from the moment the machine becomes reachable, so the link may
    /// drop at any point after without retiring it.
    fn hold_for_connection(&self, executor_id: &str, display_name: &str) -> Result<(), String>;
    /// The undeclared machines a pending rotation is holding for, by executor
    /// id, with the name each advertised. Kept so a status read can still
    /// describe a machine whose link is gone.
    fn undeclared_holds(&self) -> Result<std::collections::BTreeMap<String, String>, String>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorDisconnectOrigin {
    RunnerInitiated,
    PeerOrIo,
    /// The executor closed the link itself and said why: it is stopping or
    /// restarting. Whether its work survived is for its next opening snapshot
    /// to say, not for the close.
    ExecutorInitiated(String),
}

/// What a colocated link's observed silence says about whether it can be left
/// alone. `ConnectedStalled` is only a classification; this is the verdict the
/// supervisor acts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkRemediation {
    Healthy,
    Bounce {
        /// The connection the verdict was formed about. Carrying it fences the
        /// subsequent teardown against a reattach that lands in between.
        executor_id: String,
        generation: u64,
        cause: LinkStallCause,
    },
}

/// Why a link that never closed has stopped being usable.
///
/// Named rather than collapsed into one duration because the two failures look
/// nothing alike to whoever reads the log or the machine: one went quiet, the
/// other never missed a beat and has been republishing the same history for
/// minutes. An operator told "heartbeat lost" about a machine whose heartbeats
/// all arrived goes looking for the wrong failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkStallCause {
    /// Nothing at all has been recorded on this link.
    Silence {
        /// How long the runner has recorded no progress at all on this link.
        silence_ms: u64,
        /// How long since the socket reader's own timer last ran, or `None`
        /// when no reader has stamped this link. Fresh here while `silence_ms`
        /// is stale means the executor went quiet; stale in both means the
        /// runner's own reader is wedged. The reader stamps its timer apart
        /// from frame arrival, because a clock driven by inbound traffic reads
        /// identically for a silent executor and a wedged reader.
        pump_silence_ms: Option<u64>,
    },
    /// Beats keep arriving on time; what they carry stopped being recomputed.
    TelemetryFrozen {
        /// How long the executor's recompute stamp has gone unchanged, counted
        /// in time this runner was awake. Not the stamp's wall-clock age: wall
        /// time keeps running while the host sleeps, and a sleep is not a
        /// freeze.
        frozen_ms: u64,
    },
}

impl LinkStallCause {
    /// One line an operator can act on.
    pub fn describe(&self) -> String {
        match self {
            Self::Silence {
                silence_ms,
                pump_silence_ms,
            } => match pump_silence_ms {
                Some(pump_silence_ms) => format!(
                    "the link recorded nothing for {silence_ms}ms (socket reader last turned over {pump_silence_ms}ms ago)"
                ),
                None => format!("the link recorded nothing for {silence_ms}ms"),
            },
            Self::TelemetryFrozen { frozen_ms } => format!(
                "heartbeats kept arriving but the facts they carry have not been recomputed for {frozen_ms}ms of runner-awake time"
            ),
        }
    }

    /// The cause's measurements as fields, for a `runner_events` row: the
    /// numbers `describe` carries only as prose.
    pub fn detail(&self) -> serde_json::Value {
        match self {
            Self::Silence {
                silence_ms,
                pump_silence_ms,
            } => serde_json::json!({
                "stall_kind": "silence",
                "silence_ms": silence_ms,
                "pump_silence_ms": pump_silence_ms,
            }),
            Self::TelemetryFrozen { frozen_ms } => serde_json::json!({
                "stall_kind": "telemetry_frozen",
                "frozen_ms": frozen_ms,
            }),
        }
    }

    /// Whether this cause has to wait its turn behind another machine's
    /// recovery. Only frozen telemetry does: it is the failure that arrives on
    /// many machines at once.
    pub fn is_paced(&self) -> bool {
        matches!(self, Self::TelemetryFrozen { .. })
    }
}

/// The bounds a link's two stall verdicts are judged against.
///
/// Injected rather than read from the constants so a verdict is testable
/// without wall-clock sleeps, following [`deadline_evidence`]. They travel as
/// one value because they are judged together and in a fixed order, and because
/// two bare `u64` parameters are two parameters a caller can transpose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkStallBounds {
    /// How long total silence is tolerated before the link is bounced.
    pub silence_ms: u64,
    /// How long a link that keeps beating may carry facts that stopped being
    /// recomputed before it is bounced.
    pub telemetry_ms: u64,
}

/// What abandoning a stalled colocated link did, for the supervisor's reset
/// row and log line. All zero, with `disconnected` false, when the generation
/// no longer owned the link and nothing was touched.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StalledLinkAbandonment {
    pub disconnected: bool,
    pub abandoned_attempts: usize,
    pub abandoned_in_flight: usize,
    pub last_known_state: Option<ExecutorSubstrateState>,
}

impl Default for LinkStallBounds {
    fn default() -> Self {
        Self {
            silence_ms: EXECUTOR_LINK_STALL_REMEDIATION_MS,
            telemetry_ms: EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS,
        }
    }
}

/// What the runner knows about an enrolled machine independently of whether it
/// is attached: who it is, and the account of the last time the runner tried to
/// bring it up.
///
/// This outlives every connection to the machine on purpose. A record that only
/// existed while the link was up could not describe a link that is down, which
/// is the state the fleet most needs described.
#[derive(Debug, Clone)]
pub(super) struct EnrolledRemoteRecord {
    pub(super) name: String,
    pub(super) os: String,
    pub(super) arch: String,
    pub(super) link: RemoteLinkState,
    pub(super) last_attempt: Option<RemoteAttachAttempt>,
    /// See [`EnrolledRemote::retired`].
    pub(super) retired: Vec<String>,
    pub(super) last_seen_unix_ms: Option<u64>,
    pub(super) connection_timeline: VecDeque<RemoteConnectionTransition>,
    /// The most recent process telemetry any generation of this machine has
    /// reported. Held here rather than on the connection because a connection
    /// that has ended cannot describe how big it was when it ended, which is
    /// the one moment worth describing.
    pub(super) last_process_report: Option<GenerationProcessReport>,
    /// The run of protocol refusals this machine is in, if its last dial was
    /// refused on protocol. Cleared when it attaches.
    pub(super) protocol_refusal: Option<ProtocolRefusalRun>,
    /// The self-update this machine was directed to and did not return from
    /// (see [`unreturned`]). Cleared when it attaches.
    pub(super) unreturned_update: Option<cairn_common::executor_protocol::UnreturnedUpdate>,
}

#[derive(Debug)]
pub(super) struct ExecutorListenerPublication {
    pub(super) current: Mutex<Option<ExecutorListenerAdvertisement>>,
    pub(super) updates: tokio::sync::watch::Sender<Option<ExecutorListenerAdvertisement>>,
}

impl Default for ExecutorListenerPublication {
    fn default() -> Self {
        let (updates, _) = tokio::sync::watch::channel(None);
        Self {
            current: Mutex::new(None),
            updates,
        }
    }
}

impl Fleet {
    pub fn attach_executor(&self, sender: mpsc::UnboundedSender<ExecutorMessage>) -> u64 {
        self.attach_colocated_advertising(sender, Vec::new())
    }

    /// [`Fleet::attach_executor`], advertising `toolchains` as a real executor
    /// does from its first frame, for a test whose work requires one.
    #[cfg(test)]
    pub(crate) fn attach_executor_with_toolchains(
        &self,
        sender: mpsc::UnboundedSender<ExecutorMessage>,
        toolchains: &[&str],
    ) -> u64 {
        self.attach_colocated_advertising(
            sender,
            toolchains.iter().map(|name| name.to_string()).collect(),
        )
    }

    pub(super) fn attach_colocated_advertising(
        &self,
        sender: mpsc::UnboundedSender<ExecutorMessage>,
        toolchains: Vec<String>,
    ) -> u64 {
        let advertisement = ExecutorAdvertisement {
            identity: ExecutorIdentity {
                device_id: "local-device".into(),
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                display_name: "Local executor".into(),
            },
            capabilities: ExecutorCapabilities {
                os: std::env::consts::OS.into(),
                arch: std::env::consts::ARCH.into(),
                logical_cores: 1,
                concurrency_capacity: None,
                toolchains,
                projects_served: Vec::new(),
                disk_budget_bytes: None,
                memory_budget_bytes: None,
                sandbox: None,
                toolchain_detection: None,
                build_prerequisites: None,
                // A placeholder until the executor's own advertisement lands.
                // Claiming no containment matrix is the honest state here: this
                // process has measured nothing, and every envelope refuses
                // until the machine speaks for itself.
                containment: None,
                page_call_programs: None,
                github_credential: None,
                publish_isolation: None,
                machine_services: Vec::new(),
                credential_floor_service_residents: false,
                service_bounds: None,
            },
            current_load: 0,
            observed_at_unix_ms: unix_time_ms(),
            liveness_observed_at_unix_ms: None,
            facts_stale_refresh_blocked: None,
            last_refresh: None,
            node_scratches: Vec::new(),
        };
        // The colocated machine pins no listener certificate and places no
        // rotation hold, so the one failure a remote attach can report cannot
        // arise here.
        self.attach_advertised_executor(advertisement, Default::default(), sender, true, None, None)
            .expect("a colocated attach places no listener rotation hold and cannot fail")
    }

    /// Attach a machine with what its Hello said: its advertisement and every
    /// warm root it holds, inserted together so placement never reads a
    /// machine that has just attached as colder than it is.
    pub fn attach_advertised_executor(
        &self,
        advertisement: ExecutorAdvertisement,
        warm_roots: cairn_common::executor_protocol::WarmRootInventory,
        sender: mpsc::UnboundedSender<ExecutorMessage>,
        colocated: bool,
        executor_build_id: Option<String>,
        last_update: Option<UpdateReport>,
    ) -> Result<u64, String> {
        let executor_id = advertisement.identity.executor_id.clone();
        // The connection is addressed by the name this runner enrolled the
        // machine under, not by the label its own executor.json carries. The
        // advertisement keeps the machine's own words for diagnostics.
        let identity = self.addressed_identity(&advertisement.identity, colocated);
        if !executor_names_match(&identity.display_name, &advertisement.identity.display_name) {
            log::warn!(
                "executor_id={executor_id} advertises the name {:?} but is enrolled as {:?}; addressing it as {:?} (its executor.json predates a rename)",
                advertisement.identity.display_name,
                identity.display_name,
                identity.display_name
            );
        }
        // Before anything else this function does, and so before the caller can
        // send this machine a single frame. A non-colocated connection is about
        // to become a recipient of the staged listener advertisement: the
        // `Ready` carrying the current one and the watch subscription carrying
        // every later one are both established by the caller once this returns.
        //
        // Resolved here, before the advertisement is moved into the connection,
        // and the same way `executor_public_name` resolves it, so a hold names
        // the machine exactly as every other surface does.
        let public_name = normalize_executor_name(&identity.display_name)
            .or_else(|| normalize_executor_name(&executor_id))
            .unwrap_or_else(|| executor_id.clone());
        if let Some(policy) = self
            .desired_runtime_policies
            .lock()
            .unwrap()
            .get(&executor_id)
            .cloned()
        {
            let _ = sender.send(ExecutorMessage::RuntimePolicyRequest {
                correlation_id: uuid::Uuid::new_v4().to_string(),
                policy,
            });
        }
        // A machine whose turn it is stays held across a link bounce; the pass
        // that ends the turn is what releases it.
        let update_hold = self.self_update.lock().unwrap().holds(&executor_id);
        let generation = {
            let mut generations = self.connection_generations.lock().unwrap();
            let generation = generations
                .get(&executor_id)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                .expect("executor connection generation exhausted");
            generations.insert(executor_id.clone(), generation);
            generation
        };
        self.link_absence_since.lock().unwrap().remove(&executor_id);
        if executor_id == COLOCATED_EXECUTOR_ID {
            self.colocated_attached_once
                .store(true, std::sync::atomic::Ordering::Release);
        }
        let advertised_scratches = advertisement.node_scratches.clone();
        let mut connection = ExecutorConnectionState {
            identity,
            advertisement: Arc::new(advertisement),
            warm_roots: Arc::new(warm_roots::WarmRootSet::from_inventory(warm_roots)),
            warm_roots_resync_requested: false,
            generation,
            sender,
            snapshot: Arc::default(),
            snapshots_received: 0,
            witnessed_grants: HashSet::new(),
            last_progress_unix_ms: unix_time_ms(),
            health: Arc::default(),
            publishes_machine_samples: false,
            live_usage: load::LiveUsageIndex::default(),
            executor_build_id,
            last_update,
            colocated,
            drained: true,
            drain_blockers: Vec::new(),
            drain_state: Default::default(),
            update_hold,
            // Stamped below, under the table lock.
            machine_role: MachineRole::General,
            model_residents: Vec::new(),
            preparation_refusals: HashMap::new(),
            link_clock: Arc::default(),
            link_path: None,
        };
        // The facts a link opens with are judged from the moment it attached,
        // so a link whose facts never change still has a freeze to measure.
        connection.link_clock.facts_recomputed();
        // A refusal outlives the link only while the machine still advertises
        // the environment it was recorded against: a re-attach with the
        // toolchain present is the repair, identical evidence is not.
        let replaced_generation = {
            let projection = self.drain_projection.lock().unwrap();
            let mut connections = self.lock_connections();
            connection.drain_state = projection.view(&executor_id);
            connection.drain_blockers = projection.blockers(&executor_id);
            connection.drained = !connection.drain_blockers.is_empty();
            // Enqueue before releasing either boundary: a newer publication can
            // only enqueue after this frame and sees the inserted connection.
            let _ = connection.sender.send(ExecutorMessage::DrainModeRequest {
                correlation_id: uuid::Uuid::new_v4().to_string(),
                enabled: drain_projection::protocol_refusal(&connection.drain_blockers),
            });
            // A refusal outlives the link only while the machine still
            // advertises the environment it was recorded against: a re-attach
            // with the toolchain present is the repair, identical evidence is
            // not. Stamped under the table lock, so no transition interleaves.
            stamp_preparation_refusals(
                &mut self.preparation_refusals.lock().unwrap(),
                &mut connection,
            );
            // The role is read here, under the table lock that
            // `hydrate_machine_roles` also holds while it replaces the roles
            // and restamps entries, so an adoption is either seen by this
            // read or restamps this entry once it is inserted. Read before the
            // lock, an adoption could land in between and this insert would
            // restore the role it replaced. Unlike a drain, the machine is not
            // told: only this runner's placement reads it.
            connection.machine_role = self.machine_role(&executor_id);
            // Under the same lock the model pass takes first when it
            // publishes the residents, for the same reason as the role.
            connection.model_residents =
                self.model_residents_named(&executor_public_name(&connection));
            connections
                .insert(executor_id.clone(), connection)
                .map(|previous| previous.generation)
        };
        // A colocated process was spawned by this runner's supervisor with a
        // fresh nonce, so a replacement there is known to be a new process and
        // the old one's work is gone.
        //
        // A remote dialling in over a connection the runner still holds says
        // nothing of the kind. A runner that slept, or a path that went
        // half-open, leaves the old socket registered because nothing closed
        // it on this side, while the executor gave up on it, kept its cells
        // running, and redialled. That is a link gap whose close the runner
        // never saw, and it ends the old generation the way the close would
        // have: its work is held, and the rebinding below carries it onto this
        // connection for the first snapshot to account for. A restarted process
        // lists none of the old attempts there, so this needs no process
        // identity to tell the two apart.
        if let Some(replaced_generation) = replaced_generation {
            if colocated {
                self.fail_for_executor(
                    &executor_id,
                    "executor connection was replaced before returning a result",
                );
            } else {
                log::info!(
                    "executor_id={executor_id} generation {generation} replaced generation {replaced_generation} without a close; holding its work for reconciliation"
                );
                self.end_link_generation(
                    &executor_id,
                    replaced_generation,
                    &ExecutorDisconnectOrigin::PeerOrIo,
                );
            }
        }
        // Every attach is reconciled, even one advertising nothing: what a
        // machine holds is decided against the authority, not remembered.
        self.emit_node_scratch_event(NodeScratchEvent::Advertised {
            executor_id: executor_id.clone(),
            generation,
            replicas: advertised_scratches,
        });
        // After the insertion above and before this returns, which is the only
        // ordering that leaves no window. A rotation staged concurrently either
        // takes its snapshot after the insertion, in which case it sees this
        // machine, or before it, in which case the staging record already exists
        // by the time the hold below reads it. Those are exhaustive because
        // staging writes its record BEFORE it snapshots the fleet; taking the
        // hold first instead would let a rotation land in between and see
        // neither, and this machine would receive the overlap while nothing
        // owed it an acknowledgement.
        //
        // A hold that cannot be persisted rolls the attach back rather than
        // leaving a connection the rotation cannot account for. It is placed
        // ahead of the execution rebinding below so a refusal has no carried
        // work to lose, and the removal is generation-checked so a newer attach
        // that already replaced this one is left alone. Refusing costs a redial
        // and nothing else: the executor reconnects on its own, and a redial
        // landing once the disk is writable is a connection the runner can
        // actually account for.
        if !colocated {
            if let Err(error) =
                self.hold_listener_rotation_for_connection(&executor_id, &public_name)
            {
                let mut connections = self.lock_connections();
                if connections
                    .get(&executor_id)
                    .is_some_and(|entry| entry.generation == generation)
                {
                    connections.remove(&executor_id);
                }
                return Err(error);
            }
        }
        // Work this machine was running when its last link went away is still
        // its work. Carrying it onto this connection is what lets the result of
        // a command that outlived the gap be delivered to whoever asked for it.
        let carried = self.rebind_held_executions(&executor_id, generation);
        if !carried.is_empty() {
            log::info!(
                "executor_id={executor_id} reattached at generation {generation} carrying {} held execution(s)",
                carried.len()
            );
            self.unreconciled_executions
                .lock()
                .unwrap()
                .insert(executor_id.clone(), (generation, carried));
        }
        // The machine asserting its own presence. This is the only thing that
        // makes an enrolled machine connected — there is no runner-side attach
        // to succeed or fail.
        if !colocated {
            if let Some(record) = self.enrolled_remotes.lock().unwrap().get_mut(&executor_id) {
                record.link = RemoteLinkState::Connected {
                    generation,
                    since_unix_ms: unix_time_ms(),
                };
                record.protocol_refusal = None;
                record.unreturned_update = None;
            }
        }
        self.connection_ready.notify_waiters();
        Ok(generation)
    }

    /// Wait until an enrolled machine dials in, or until the wait is spent.
    ///
    /// Returns whether it arrived. Used by the bootstrap ceremony, which has
    /// nothing to attach and only wants to know whether the machine it just
    /// installed came to it.
    pub async fn wait_for_enrolled_remote(&self, executor_id: &str, within: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let notified = self.connection_ready.notified();
            if self
                .lock_connections()
                .get(executor_id)
                .is_some_and(|entry| !entry.sender.is_closed())
            {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return false;
            }
        }
    }

    pub async fn wait_for_named_executor(&self, executor_name: &str) {
        loop {
            let notified = self.connection_ready.notified();
            if self.named_executor_is_connected(executor_name) {
                return;
            }
            notified.await;
        }
    }

    /// Wait, within `bound`, for the colocated executor's first attach of this
    /// runner's life, and say whether it has happened.
    ///
    /// Placement ranks only attached machines, so at boot a remote that attaches
    /// first would win a choice the runner's own machine was about to contest.
    /// Only the first attach is waited for: later, an absent colocated executor
    /// is an ordinary absence for placement to route around.
    pub(crate) async fn wait_for_first_colocated_attach(&self, bound: std::time::Duration) -> bool {
        self.colocated_attached_once
            .load(std::sync::atomic::Ordering::Acquire)
            || tokio::time::timeout(bound, self.wait_for_named_executor(LOCAL_EXECUTOR_NAME))
                .await
                .is_ok()
    }

    pub(crate) fn named_executor_is_connected(&self, executor_name: &str) -> bool {
        self.lock_connections().values().any(|entry| {
            !entry.sender.is_closed()
                && executor_names_match(&executor_public_name(entry), executor_name)
        })
    }

    pub fn disconnect_advertised_executor(&self, executor_id: &str, generation: u64) -> bool {
        self.disconnect_advertised_executor_with_origin(
            executor_id,
            generation,
            ExecutorDisconnectOrigin::PeerOrIo,
        )
    }

    pub fn disconnect_advertised_executor_with_origin(
        &self,
        executor_id: &str,
        generation: u64,
        origin: ExecutorDisconnectOrigin,
    ) -> bool {
        let disconnected = {
            let mut connections = self.lock_connections();
            if connections
                .get(executor_id)
                .is_some_and(|entry| entry.generation == generation)
            {
                connections.remove(executor_id);
                true
            } else {
                false
            }
        };
        if disconnected {
            // The link going down is the last moment this machine was seen, and
            // the only moment the fact can be captured: once the connection is
            // gone there is nothing left holding its heartbeat.
            if let Some(record) = self.enrolled_remotes.lock().unwrap().get_mut(executor_id) {
                let last_seen = unix_time_ms();
                record.last_seen_unix_ms = Some(last_seen);
                // Absent, not failed. The machine is still running and still
                // reconnecting on its own; nothing here is waiting to be
                // repaired by the runner.
                record.link = RemoteLinkState::Absent {
                    last_beat_unix_ms: Some(last_seen),
                    last_reason: match &origin {
                        ExecutorDisconnectOrigin::RunnerInitiated => {
                            "the runner closed this link".to_string()
                        }
                        ExecutorDisconnectOrigin::PeerOrIo => {
                            "the link went away; the machine reconnects on its own".to_string()
                        }
                        ExecutorDisconnectOrigin::ExecutorInitiated(reason) => {
                            format!("the executor closed this link: {reason}")
                        }
                    },
                };
            }
            self.link_absence_since
                .lock()
                .unwrap()
                .entry(executor_id.to_string())
                .or_insert_with(link_clock::monotonic_now_ns);
            if executor_id == COLOCATED_EXECUTOR_ID {
                self.disconnect_origins
                    .lock()
                    .unwrap()
                    .insert((executor_id.to_string(), generation), origin.clone());
            }
            self.end_link_generation(executor_id, generation, &origin);
        }
        disconnected
    }

    /// End one connection generation's hold on the work bound to it, once that
    /// generation is no longer the machine's link.
    ///
    /// Separate from the disconnect because a generation also ends when a
    /// remote redials over a connection the runner never saw close. The machine
    /// is not absent then, so the absence bookkeeping a disconnect does first
    /// stays with the disconnect, and only what the work needs is done here.
    pub(super) fn end_link_generation(
        &self,
        executor_id: &str,
        generation: u64,
        origin: &ExecutorDisconnectOrigin,
    ) {
        // Who ended the link decides what it says about the work. A runner
        // that closed this connection is recycling the process behind it,
        // so the work really is gone; a link that simply went away says
        // nothing about a machine that is still running and still coming
        // back. An executor that closed its own link named why, but the
        // reconciliation against its next opening snapshot is still what
        // says which of its work survived, so its work is held the same way.
        //
        // Either way, an entry that was only ever OFFERED goes back to the
        // queue: nothing was running behind an offer, so nothing can be run
        // twice by re-offering it, and an offer outliving its link is the
        // stuck placement this whole design exists to make impossible.
        //
        // A runner closing links because it is itself stopping is a third
        // case, and the one most easily misread: the machine was not lost
        // and the work did not fail. Answering it as a closed connection is
        // what charged a runner restart to the substrate.
        let stopped_at = self.runner_stop.at();
        let diagnostic = match stopped_at {
            Some(at) => format!(
                "this runner began shutting down at {} before the request returned a result",
                describe_runner_stop(at)
            ),
            None => "executor connection closed before returning a result".to_string(),
        };
        let cause = match (origin, stopped_at) {
            (ExecutorDisconnectOrigin::RunnerInitiated, Some(stopped_at_unix_ms)) => {
                queue::GenerationEndCause::RunnerStopping { stopped_at_unix_ms }
            }
            (ExecutorDisconnectOrigin::RunnerInitiated, None) => {
                queue::GenerationEndCause::MachineGone
            }
            (
                ExecutorDisconnectOrigin::PeerOrIo | ExecutorDisconnectOrigin::ExecutorInitiated(_),
                _,
            ) => queue::GenerationEndCause::LinkGap,
        };
        let machine_gone = matches!(cause, queue::GenerationEndCause::MachineGone);
        let sweep =
            self.queue
                .end_generation(executor_id, generation, cause, &diagnostic, unix_time_ms());
        if !sweep.released_offers.is_empty() {
            log::info!(
                "returned {} offered entr(ies) to the queue for executor_id={executor_id}: nothing had been claimed on them",
                sweep.released_offers.len()
            );
        }
        match origin {
            ExecutorDisconnectOrigin::RunnerInitiated => {
                self.fail_calls_for_executor(executor_id, &diagnostic);
            }
            ExecutorDisconnectOrigin::PeerOrIo | ExecutorDisconnectOrigin::ExecutorInitiated(_) => {
                self.hold_executions_across_link_gap(executor_id, sweep.held_claims);
            }
        }
        // The runner ending this link on its own authority is the same
        // judgement the queue makes when it answers the link's claims as
        // `MachineGone`, and it covers the attempts nobody waits on any more.
        // A link that went away is not that judgement, and neither is this
        // runner shutting down: the machine keeps running through both.
        if machine_gone {
            self.settle_purpose_debts_for_lost_machine(
                executor_id,
                "the runner ended this machine's link on its own authority",
            );
        }
        self.binding_changed.notify_waiters();
        self.connection_ready.notify_waiters();
    }

    /// State where this runner's executor listener answers. Re-publishing sends
    /// a watch notification so connected executors can persist rotations and
    /// endpoint changes without polling.
    pub fn publish_executor_listener(&self, advertisement: ExecutorListenerAdvertisement) {
        let previous = self
            .executor_listener
            .current
            .lock()
            .unwrap()
            .replace(advertisement.clone());
        // A republication of the same advertisement is not a restage, and
        // every connected machine would otherwise read it as one.
        if previous.as_ref() != Some(&advertisement) {
            self.runner_events.record(
                runner_events::RunnerEvent::new(runner_events::RunnerEventKind::ListenerRestaged)
                    .detail(serde_json::json!({
                        "previous_cert_sha256": previous.map(|previous| previous.cert_sha256),
                        "cert_sha256": advertisement.cert_sha256,
                        "endpoints": advertisement.endpoints,
                    })),
            );
        }
        self.executor_listener
            .updates
            .send_replace(Some(advertisement));
    }

    /// Where this runner's executor listener answers, if it serves one.
    pub fn executor_listener(&self) -> Option<ExecutorListenerAdvertisement> {
        self.executor_listener.current.lock().unwrap().clone()
    }

    pub fn register_listener_rotation_control(&self, control: Arc<dyn ListenerRotationControl>) {
        *self.listener_rotation_control.lock().unwrap() = Some(control);
    }

    /// Tell a pending rotation that this machine can now receive the staged
    /// advertisement, so the cut-over hold it places outlives its link.
    ///
    /// A runner serving no listener has no control registered and nothing to
    /// hold, which is the only case that succeeds vacuously. A failure to
    /// persist is propagated rather than logged: this is the boundary the whole
    /// gate rests on, and a machine admitted without a hold is a machine a
    /// later acknowledgement can cut over past. Logging cannot keep an executor
    /// from being stranded.
    pub(super) fn hold_listener_rotation_for_connection(
        &self,
        executor_id: &str,
        display_name: &str,
    ) -> Result<(), String> {
        let control = self.listener_rotation_control.lock().unwrap().clone();
        let Some(control) = control else {
            return Ok(());
        };
        control.hold_for_connection(executor_id, display_name)
    }

    /// The undeclared machines a pending rotation is holding for, by executor
    /// id, with the name each advertised when it was last seen.
    pub(super) fn listener_rotation_undeclared_holds(
        &self,
    ) -> std::collections::BTreeMap<String, String> {
        let control = self.listener_rotation_control.lock().unwrap().clone();
        control
            .and_then(|control| control.undeclared_holds().ok())
            .unwrap_or_default()
    }

    pub fn listener_rotation_status(&self) -> Result<Option<ListenerRotationStatus>, String> {
        let control = self.listener_rotation_control.lock().unwrap().clone();
        control.map(|control| control.pending()).unwrap_or(Ok(None))
    }

    pub fn regenerate_listener_certificate(&self) -> Result<ListenerRotationStatus, String> {
        let control = self
            .listener_rotation_control
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| "the executor listener is not serving".to_string())?;
        control.regenerate(unix_time_ms())
    }

    pub fn force_listener_rotation_cutover(&self) -> Result<Vec<String>, String> {
        let control = self
            .listener_rotation_control
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| "the executor listener is not serving".to_string())?;
        control.force_cutover()
    }

    /// Whether an executor listener has registered its rotation authority, which
    /// it does only once it serves.
    pub fn serves_executor_listener(&self) -> bool {
        self.listener_rotation_control.lock().unwrap().is_some()
    }

    pub fn update_listener_endpoints(&self, endpoints: Vec<String>) -> Result<(), String> {
        let control = self
            .listener_rotation_control
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| "the executor listener is not serving".to_string())?;
        control.update_endpoints(endpoints)
    }

    pub fn listener_rotation_pending_machines(
        &self,
        ids: &[String],
    ) -> Vec<ListenerRotationPendingMachine> {
        // Taken and released before `enrolled_remotes`, keeping the fleet-wide
        // order in which those two locks are acquired.
        let connected: HashMap<String, String> = self
            .lock_connections()
            .values()
            .filter(|entry| !entry.colocated && !entry.sender.is_closed())
            .map(|entry| {
                (
                    entry.identity.executor_id.clone(),
                    executor_public_name(entry),
                )
            })
            .collect();
        let held = self.listener_rotation_undeclared_holds();
        let enrolled = self.enrolled_remotes.lock().unwrap();
        ids.iter()
            .map(|id| match enrolled.get(id) {
                Some(record) => ListenerRotationPendingMachine {
                    executor_id: id.clone(),
                    name: record.name.clone(),
                    presence_cause: match &record.link {
                        RemoteLinkState::Connected { .. } => {
                            "connected; acknowledgement has not arrived".into()
                        }
                        RemoteLinkState::NeverConnected { .. } | RemoteLinkState::Absent { .. } => {
                            presence_cause(&record.link)
                        }
                        RemoteLinkState::Bootstrapping { .. }
                        | RemoteLinkState::BootstrapFailed { .. }
                        | RemoteLinkState::BootstrapHeld { .. } => {
                            "bootstrap-only; excluded from the gate".into()
                        }
                    },
                },
                // A machine holding a live socket that no enrollment record
                // describes. Its own advertisement is the only account of it
                // there is, so it is named by that rather than by its identity
                // string.
                None => match connected.get(id) {
                    Some(name) => ListenerRotationPendingMachine {
                        executor_id: id.clone(),
                        name: name.clone(),
                        presence_cause: "connected; acknowledgement has not arrived".into(),
                    },
                    // Undeclared and no longer connected, but the rotation is
                    // deliberately still holding for it. Reporting that as a
                    // membership race would describe the opposite of what
                    // happened, so the name it advertised is carried in the
                    // rotation record for exactly this read.
                    None => match held.get(id) {
                        Some(name) => ListenerRotationPendingMachine {
                            executor_id: id.clone(),
                            name: name.clone(),
                            presence_cause:
                                "dialed in without an enrollment record; the link has since dropped"
                                    .into(),
                        },
                        None => ListenerRotationPendingMachine {
                            executor_id: id.clone(),
                            name: id.clone(),
                            presence_cause: "enrollment changed while status was read".into(),
                        },
                    },
                },
            })
            .collect()
    }

    /// Subscribe to listener advertisements. The receiver's initial value is
    /// the current advertisement, so reconnect and live-update paths use one
    /// source of truth.
    pub fn subscribe_executor_listener(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<ExecutorListenerAdvertisement>> {
        self.executor_listener.updates.subscribe()
    }

    /// Executor identities that must acknowledge a staged listener certificate
    /// before cut-over.
    ///
    /// Two populations, unioned, because a cut-over can strand a machine from
    /// either one. Every enrolled remote that is not ceremony-only: an absent
    /// one still holds the old pin it will dial back with, while bootstrap
    /// persists the overlap for a ceremony-only machine directly. And every
    /// connected non-colocated machine, declared or not: one holding a live
    /// socket has the current pins and is about to be sent `ListenerUpdated`,
    /// so pruning the old pin before it acknowledges strands it exactly as it
    /// would an absent enrolled machine.
    ///
    /// Whether a connected machine also appears in `enrolled_remotes` is
    /// bookkeeping — in production every resident executor is declared at
    /// runner start or at `add` — and the gate must not depend on that
    /// ordering, because a machine that dialed in before its declaration
    /// landed is no less strandable for it.
    ///
    /// The colocated executor is never gated: it is MCP-authenticated, pins no
    /// listener certificate, and refuses `ListenerUpdated` outright.
    pub fn listener_rotation_gate_executor_ids(&self) -> Vec<String> {
        // Taken and released before `enrolled_remotes`, keeping the fleet-wide
        // order in which those two locks are acquired.
        let mut ids: Vec<String> = self
            .lock_connections()
            .values()
            .filter(|entry| !entry.colocated && !entry.sender.is_closed())
            .map(|entry| entry.identity.executor_id.clone())
            .collect();
        ids.extend(
            self.enrolled_remotes
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, record)| {
                    !matches!(
                        record.link,
                        RemoteLinkState::Bootstrapping { .. }
                            | RemoteLinkState::BootstrapFailed { .. }
                            | RemoteLinkState::BootstrapHeld { .. }
                    )
                })
                .map(|(executor_id, _)| executor_id.clone()),
        );
        ids.sort();
        ids.dedup();
        ids
    }

    /// Connected non-colocated machines that no enrollment record describes.
    ///
    /// These are the gate members with no durable home. A declared machine is
    /// carried across a disconnect by `enrolled_remotes`, which is what keeps
    /// it gated while it is away; an undeclared one exists only for as long as
    /// its socket does, so a rotation that means to keep holding for it has to
    /// record it somewhere that outlives the link.
    pub fn undeclared_connected_executors(&self) -> Vec<(String, String)> {
        // Snapshotted and released before the enrollment lock is taken, keeping
        // the fleet-wide order in which the two are acquired.
        let connected: Vec<(String, String)> = self
            .lock_connections()
            .values()
            .filter(|entry| !entry.colocated && !entry.sender.is_closed())
            .map(|entry| {
                (
                    entry.identity.executor_id.clone(),
                    executor_public_name(entry),
                )
            })
            .collect();
        let enrolled = self.enrolled_remotes.lock().unwrap();
        let mut machines: Vec<(String, String)> = connected
            .into_iter()
            .filter(|(id, _)| !enrolled.contains_key(id))
            .collect();
        machines.sort();
        machines.dedup();
        machines
    }

    /// Whether an enrollment record describes this machine.
    pub fn is_enrolled_remote(&self, executor_id: &str) -> bool {
        self.enrolled_remotes
            .lock()
            .unwrap()
            .contains_key(executor_id)
    }

    /// Declare a machine this runner is enrolled with.
    ///
    /// Called for every configured remote as the runner starts and on every
    /// successful add — *before* any attempt is made on it. That ordering is the
    /// point: a machine becomes visible when it is enrolled, not when it first
    /// succeeds, so the state worth surfacing (nothing has worked yet) is not
    /// the one state that produces no row.
    ///
    /// Re-declaring refreshes the enrollment facts and keeps the attempt
    /// history, so a rename does not erase the explanation for a machine's
    /// current state.
    pub fn declare_enrolled_remote(&self, executor_id: &str, name: &str, os: &str, arch: &str) {
        let mut enrolled = self.enrolled_remotes.lock().unwrap();
        let record =
            enrolled
                .entry(executor_id.to_string())
                .or_insert_with(|| EnrolledRemoteRecord {
                    name: name.to_string(),
                    os: os.to_string(),
                    arch: arch.to_string(),
                    // Enrolled and not connected, with nothing yet said about
                    // why. The runner does not reach into this machine, so a
                    // freshly-started runner has no attempt to report and no
                    // business claiming one: the machine will say it is here by
                    // dialling in.
                    link: RemoteLinkState::Absent {
                        last_beat_unix_ms: None,
                        last_reason: "this runner has not seen it since starting".to_string(),
                    },
                    last_attempt: None,
                    retired: Vec::new(),
                    last_seen_unix_ms: None,
                    connection_timeline: VecDeque::new(),
                    last_process_report: None,
                    protocol_refusal: None,
                    unreturned_update: None,
                });
        record.name = name.to_string();
        record.os = os.to_string();
        record.arch = arch.to_string();
        drop(enrolled);
        if !self.lock_connections().contains_key(executor_id) {
            self.link_absence_since
                .lock()
                .unwrap()
                .entry(executor_id.to_string())
                .or_insert_with(link_clock::monotonic_now_ns);
        }
        // A rename re-declares while the machine stays connected, and nothing
        // reaches the machine, so the live connection takes the new name here.
        if let Some(entry) = self.lock_connections().get_mut(executor_id) {
            if !entry.colocated {
                entry.identity.display_name = name.to_string();
            }
        }
        self.enrollment_gate_changed.notify_waiters();
    }

    /// The name this runner enrolled `executor_id` under, when it is an
    /// enrolled remote.
    pub fn enrolled_remote_name(&self, executor_id: &str) -> Option<String> {
        self.enrolled_remotes
            .lock()
            .unwrap()
            .get(executor_id)
            .map(|record| record.name.clone())
    }

    /// `advertised` as this runner addresses it: an enrolled remote answers to
    /// the name it was enrolled under (and renamed to), whatever label its own
    /// executor.json carries, because a rename moves the runner's records and
    /// never reaches the machine. The colocated executor and a machine with no
    /// enrollment record keep their own label.
    pub fn addressed_identity(
        &self,
        advertised: &ExecutorIdentity,
        colocated: bool,
    ) -> ExecutorIdentity {
        let mut identity = advertised.clone();
        if colocated {
            return identity;
        }
        if let Some(enrolled) = self
            .enrolled_remote_name(&advertised.executor_id)
            .filter(|name| normalize_executor_name(name).is_some())
        {
            identity.display_name = enrolled;
        }
        identity
    }

    /// Record what the runner's most recent attempt on a machine did.
    ///
    /// The caller decides which state the attempt proved, because only the
    /// caller knows whether the host answered. Recording an attempt against a
    /// machine that is no longer enrolled is a no-op rather than a resurrection.
    /// Record where a machine stands without claiming anything was attempted
    /// on it. A runner-side hold is the case: nothing has reached the machine,
    /// so its last real attempt stays the one on record.
    pub fn record_remote_link(&self, executor_id: &str, link: RemoteLinkState) {
        let changed =
            if let Some(record) = self.enrolled_remotes.lock().unwrap().get_mut(executor_id) {
                record.link = link;
                true
            } else {
                false
            };
        if changed {
            self.enrollment_gate_changed.notify_waiters();
        }
    }

    /// Record what retiring a machine's pre-inversion executor found and did,
    /// without touching its link: the machine is usually attached when the
    /// runner's start pass retires it.
    ///
    /// A later retirement that found nothing does not erase an earlier one that
    /// found something, so the account of what was retired outlives the runs
    /// that confirm nothing is left.
    pub fn record_remote_retirement(&self, executor_id: &str, lines: Vec<String>, found: bool) {
        if let Some(record) = self.enrolled_remotes.lock().unwrap().get_mut(executor_id) {
            if found || record.retired.is_empty() {
                record.retired = lines;
            }
        }
    }

    /// The retirement account recorded for this machine, attached or not.
    pub fn remote_retirement(&self, executor_id: &str) -> Vec<String> {
        self.enrolled_remotes
            .lock()
            .unwrap()
            .get(executor_id)
            .map(|record| record.retired.clone())
            .unwrap_or_default()
    }

    pub fn record_remote_attach_attempt(
        &self,
        executor_id: &str,
        link: RemoteLinkState,
        attempt: RemoteAttachAttempt,
    ) {
        let changed =
            if let Some(record) = self.enrolled_remotes.lock().unwrap().get_mut(executor_id) {
                record.link = link;
                record.last_attempt = Some(attempt);
                true
            } else {
                false
            };
        if changed {
            self.enrollment_gate_changed.notify_waiters();
        }
    }

    /// Remember how big a machine's executor process was the last time the
    /// generation now reporting said anything.
    ///
    /// Kept on the enrollment record rather than the connection because the
    /// connection is exactly what a kill destroys. An executor's own memory
    /// readings live in its process and reach the runner on a heartbeat, so a
    /// process killed for its size takes the measurement of that size with it,
    /// and the machine that comes back reports a fresh process from nothing.
    /// The runner is a different process on a different machine: holding the
    /// last sample here costs the remote machine nothing, asks nothing of a
    /// dying process, and survives every way one can end.
    ///
    /// Generations only advance, so an older generation's late report is
    /// refused rather than allowed to overwrite a newer one's.
    pub(super) fn note_generation_process_telemetry(
        &self,
        executor_id: &str,
        generation: u64,
        process: ProcessTelemetry,
    ) {
        let mut enrolled = self.enrolled_remotes.lock().unwrap();
        let Some(record) = enrolled.get_mut(executor_id) else {
            return;
        };
        if record
            .last_process_report
            .as_ref()
            .is_some_and(|held| held.generation > generation)
        {
            return;
        }
        record.last_process_report = Some(GenerationProcessReport {
            generation,
            received_at_unix_ms: unix_time_ms(),
            process,
        });
    }

    /// Record the network path a connection took, as the runner's accepted
    /// socket saw it. Only the generation that took it is stamped.
    pub fn record_link_path(
        &self,
        executor_id: &str,
        generation: u64,
        path: cairn_common::executor_protocol::ExecutorLinkPath,
    ) {
        if let Some(entry) = self
            .lock_connections()
            .get_mut(executor_id)
            .filter(|entry| entry.generation == generation)
        {
            entry.link_path = Some(path);
        }
    }

    pub fn record_remote_connection_transition(
        &self,
        executor_id: &str,
        mut transition: RemoteConnectionTransition,
    ) {
        if let Some(record) = self.enrolled_remotes.lock().unwrap().get_mut(executor_id) {
            // A generation that ended carries the last size it reported. Only
            // that generation's own reading will do: pinning a newer
            // generation's number to an older generation's death would describe
            // a process that was already gone.
            if transition.phase.ends_generation() {
                if let Some(report) = record
                    .last_process_report
                    .as_ref()
                    .filter(|report| transition.generation == Some(report.generation))
                {
                    transition.memory_at_generation_end = Some(GenerationEndMemory {
                        received_at_unix_ms: report.received_at_unix_ms,
                        process: report.process.clone(),
                    });
                }
            }
            record.connection_timeline.push_back(transition);
            while record.connection_timeline.len() > REMOTE_CONNECTION_TIMELINE_LIMIT {
                record.connection_timeline.pop_front();
            }
        }
    }

    /// Drop an enrollment, so a removed machine stops being a fleet member
    /// rather than becoming a permanently failing one.
    /// Fleet management: the installed lifecycle implementation, enrollment
    /// operations in flight, and whether machine-local callers may manage it.
    pub fn management(&self) -> &management::ExecutorManagementState {
        &self.management
    }

    pub fn forget_enrolled_remote(&self, executor_id: &str) {
        if self
            .enrolled_remotes
            .lock()
            .unwrap()
            .remove(executor_id)
            .is_some()
        {
            self.enrollment_gate_changed.notify_waiters();
        }
    }

    pub fn enrollment_gate_changed(&self) -> Arc<tokio::sync::Notify> {
        self.enrollment_gate_changed.clone()
    }

    /// Every enrolled machine that is not attached right now, by name.
    ///
    /// An attached machine is deliberately absent: it is already described in
    /// full by the executor projections, and listing it twice would invite the
    /// two descriptions to disagree.
    pub fn unattached_enrolled_remotes(&self) -> Vec<EnrolledRemote> {
        // Snapshot the attached identities and release that lock before taking
        // the enrollment one, so this read never holds both at once.
        let attached: HashSet<String> = self.lock_connections().keys().cloned().collect();
        let enrolled = self.enrolled_remotes.lock().unwrap().clone();
        let roles = self.machine_roles.lock().unwrap().clone();
        let artifacts = self.executor_artifact_builds.lock().unwrap().clone();
        let returns_by: HashMap<_, _> = {
            let rollout = self.self_update.lock().unwrap();
            enrolled
                .keys()
                .filter_map(|id| rollout.returns_by(id).map(|at| (id.clone(), at)))
                .collect()
        };
        let now = unix_time_ms();
        let projection = self.drain_projection.lock().unwrap();
        let mut values: Vec<_> = enrolled
            .iter()
            .filter(|(executor_id, _)| !attached.contains(*executor_id))
            .map(|(executor_id, record)| EnrolledRemote {
                absence_since_unix_ms: Some(self.link_absent_since(executor_id, now)),
                update_returns_by_unix_ms: returns_by.get(executor_id).copied(),
                drain_state: projection.view(executor_id),
                stranded: record.protocol_refusal.and_then(|refusal| {
                    refusal.stranding(
                        enrolled_artifact_target(&record.os, &record.arch)?,
                        &artifacts,
                    )
                }),
                unreturned_update: record.unreturned_update.clone(),
                drained: !projection.blockers(executor_id).is_empty(),
                role: roles.get(executor_id).copied().unwrap_or_default(),
                executor_id: executor_id.clone(),
                name: record.name.clone(),
                os: record.os.clone(),
                arch: record.arch.clone(),
                link: record.link.clone(),
                last_attempt: record.last_attempt.clone(),
                last_seen_unix_ms: record.last_seen_unix_ms,
                retired: record.retired.clone(),
                connection_timeline: record.connection_timeline.iter().rev().cloned().collect(),
            })
            .collect();
        values.sort_by(|a, b| a.name.cmp(&b.name));
        values
    }

    pub fn take_disconnect_origin(
        &self,
        executor_id: &str,
        generation: u64,
    ) -> Option<ExecutorDisconnectOrigin> {
        self.disconnect_origins
            .lock()
            .unwrap()
            .remove(&(executor_id.to_string(), generation))
    }

    pub fn clear_disconnect_origins(&self, executor_id: &str) {
        self.disconnect_origins
            .lock()
            .unwrap()
            .retain(|(id, _), _| id != executor_id);
    }
}
