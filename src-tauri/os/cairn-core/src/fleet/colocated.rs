use super::*;

/// Why an untargeted request is refused at once when the runner has no
/// colocated executor and nothing to wait for. The main cadence's stall-window
/// grouper reads this text out of failing tests' excerpts (`BOUND_ENDINGS` in
/// `execution/checks_main.rs`), and a test there fails when the two part.
pub(crate) const NO_COLOCATED_EXECUTOR: &str =
    "no colocated executor is configured, enrolled, or being supervised";

/// The sentence a refused local placement carries while the executor
/// generation is held: who holds it, and what the runner is doing about it.
pub(super) fn generation_hold_diagnostic(
    hold: &cairn_common::executor_protocol::ColocatedGenerationHold,
    now_unix_ms: u64,
) -> String {
    use cairn_common::executor_protocol::GenerationHoldReason;
    let holder = match hold.holder_pid {
        Some(pid) => format!("pid {pid}"),
        None => "a process that left no record of itself".to_string(),
    };
    let recorded = hold
        .holder_acquired_at_unix_ms
        .map(|at| {
            format!(
                " (recorded {} ago)",
                compact_duration(now_unix_ms.saturating_sub(at))
            )
        })
        .unwrap_or_default();
    match hold.reason {
        GenerationHoldReason::Unidentified => format!(
            "the local executor's generation lock is held by {holder}{recorded}, which this runner did not start and will not signal; the local executor starts when that process exits"
        ),
        GenerationHoldReason::RetirementStuck { phase } => format!(
            "the outgoing local executor {holder}{recorded} is still alive after its retirement reached the {} phase; the runner keeps retrying and starts no successor while it holds the generation lock",
            phase.label()
        ),
    }
}

/// The sentence the colocated substrate carries while the supervisor waits on
/// the kernel: which process, at which stage, for how long, and what its
/// threads are waiting in.
pub(crate) fn kernel_wait_diagnostic(
    wait: &cairn_common::executor_protocol::ColocatedKernelWait,
    now_unix_ms: u64,
) -> String {
    use cairn_common::executor_protocol::KernelWaitStage;
    let waited = compact_duration(now_unix_ms.saturating_sub(wait.since_unix_ms));
    match wait.stage {
        KernelWaitStage::Starting => format!(
            "waiting on the kernel: the starting local executor pid {} has {} for {waited}; its startup budget is paused, and it is neither retired nor replaced until the wait returns",
            wait.pid, wait.kernel
        ),
        KernelWaitStage::Killed => format!(
            "killed, waiting on the kernel: the outgoing local executor pid {} was SIGKILLed and has {} for {waited}; the kernel ends it when that wait returns, and no successor starts until then",
            wait.pid, wait.kernel
        ),
    }
}

// Constructed only by the macOS adoption API; the response arm that answers it
// is platform-neutral, so the type exists everywhere.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub(super) struct PendingTreeAdoptionResult {
    pub(super) generation: u64,
    pub(super) waiter: oneshot::Sender<
        Result<cairn_common::executor_protocol::ProcessTreeAdoptionOutcome, String>,
    >,
}

/// How long the runner waits for its colocated executor to answer an adoption.
/// An executor older than adoption drops the request unanswered, so this bound
/// is also how long a reconciler tick pays to learn that once per link.
#[cfg(target_os = "macos")]
pub(super) const TREE_ADOPTION_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

impl Fleet {
    pub fn declare_colocated_substrate(&self, state: ExecutorSubstrateState) {
        let now = unix_time_ms();
        *self.colocated_substrate_state.lock().unwrap() =
            Some(ExecutorSubstrateEvidence::without_queue(state, now, now));
        self.connection_ready.notify_waiters();
    }

    pub fn declare_colocated_substrate_failure(&self, diagnostic: String) {
        self.declare_colocated_substrate_failure_state(
            ExecutorSubstrateState::SupervisorRespawning,
            diagnostic,
        );
    }

    pub fn declare_colocated_permanent_refusal(&self, diagnostic: String) {
        self.declare_colocated_substrate_failure_state(
            ExecutorSubstrateState::PermanentRefusal,
            diagnostic,
        );
    }

    pub(super) fn declare_colocated_substrate_failure_state(
        &self,
        state: ExecutorSubstrateState,
        diagnostic: String,
    ) {
        let now = unix_time_ms();
        let (failing_since, failed_attempts) = {
            let mut supervision = self.colocated_supervision.lock().unwrap();
            let failing_since = *supervision.failing_since_unix_ms.get_or_insert(now);
            supervision.failed_attempts = supervision.failed_attempts.saturating_add(1);
            supervision.last_cause = Some(diagnostic.clone());
            (failing_since, supervision.failed_attempts)
        };
        self.runner_events.record(
            runner_events::RunnerEvent::new(
                runner_events::RunnerEventKind::ExecutorSubstrateFailed,
            )
            .at(now)
            .executor(COLOCATED_EXECUTOR_ID)
            .cause(diagnostic.clone())
            .detail(serde_json::json!({
                "state": match state {
                    ExecutorSubstrateState::SupervisorRespawning => "supervisor_respawning".to_string(),
                    ExecutorSubstrateState::PermanentRefusal => "permanent_refusal".to_string(),
                    other => format!("{other:?}"),
                },
                "failed_attempts": failed_attempts,
                "failing_since_ms": failing_since,
            })),
        );
        let mut evidence = ExecutorSubstrateEvidence::without_queue(state, now, now);
        evidence.diagnostic = Some(diagnostic);
        *self.colocated_substrate_state.lock().unwrap() = Some(evidence);
        self.connection_ready.notify_waiters();
    }

    /// The colocated executor reached readiness: the declaration ends, and so
    /// does any failure streak or hold. The last retirement is kept, so a
    /// handoff that needed escalation stays visible after it succeeded.
    pub fn clear_colocated_substrate(&self) {
        self.colocated_substrate_state.lock().unwrap().take();
        {
            let mut supervision = self.colocated_supervision.lock().unwrap();
            supervision.failing_since_unix_ms = None;
            supervision.failed_attempts = 0;
            supervision.last_cause = None;
            supervision.generation_hold = None;
            supervision.kernel_wait = None;
        }
        self.connection_ready.notify_waiters();
    }

    pub fn colocated_substrate(&self) -> Option<ExecutorSubstrateEvidence> {
        self.colocated_substrate_state.lock().unwrap().clone()
    }

    pub fn colocated_supervision(&self) -> cairn_common::executor_protocol::ColocatedSupervision {
        self.colocated_supervision.lock().unwrap().clone()
    }

    /// Record the nonce the supervisor handed the colocated launch it is about
    /// to start. Replaces any earlier one: only the newest launch is this
    /// runner's executor.
    pub fn expect_colocated_supervision(&self, nonce: String) {
        *self.expected_colocated_nonce.lock().unwrap() = Some(nonce);
    }

    /// Whether a colocated Hello presenting `nonce` is the launch this runner
    /// is supervising. An empty nonce is never admitted: it is what an
    /// executor that predates supervision binding sends.
    pub fn admits_colocated_supervision(&self, nonce: &str) -> bool {
        !nonce.is_empty() && self.expected_colocated_nonce.lock().unwrap().as_deref() == Some(nonce)
    }

    /// Another live process holds the executor generation, so the supervisor
    /// is not starting a successor. Local placement is refused with this
    /// diagnostic rather than parked: the hold ends when that process exits,
    /// not when the supervisor makes progress.
    pub fn declare_colocated_generation_held(
        &self,
        hold: cairn_common::executor_protocol::ColocatedGenerationHold,
    ) {
        let now = unix_time_ms();
        let hold = {
            let mut supervision = self.colocated_supervision.lock().unwrap();
            // The same holder keeps the moment it was first found, so the
            // rendering reads how long it has been in the way.
            let hold = match &supervision.generation_hold {
                Some(prior) if prior.holder_pid == hold.holder_pid => {
                    cairn_common::executor_protocol::ColocatedGenerationHold {
                        observed_since_unix_ms: prior.observed_since_unix_ms,
                        ..hold
                    }
                }
                _ => hold,
            };
            supervision.generation_hold = Some(hold.clone());
            hold
        };
        let mut evidence = ExecutorSubstrateEvidence::without_queue(
            ExecutorSubstrateState::GenerationHeld,
            hold.observed_since_unix_ms,
            now,
        );
        evidence.diagnostic = Some(generation_hold_diagnostic(&hold, now));
        *self.colocated_substrate_state.lock().unwrap() = Some(evidence);
        self.connection_ready.notify_waiters();
    }

    /// The generation lock was found free; whatever held it is gone.
    pub fn release_colocated_generation_hold(&self) {
        self.colocated_supervision.lock().unwrap().generation_hold = None;
    }

    /// The supervisor read `pid`'s threads and found one in uninterruptible
    /// sleep, so it is waiting for the kernel rather than acting on the
    /// process. Declared again at every read: the same process at the same
    /// stage keeps the moment it was first found, so the rendering reads how
    /// long the wait has lasted, and each read refreshes the declaration's
    /// progress, because the reading is the supervisor's evidence that the wait
    /// is still the host's and not its own.
    pub fn declare_colocated_kernel_wait(
        &self,
        pid: u32,
        stage: cairn_common::executor_protocol::KernelWaitStage,
        kernel: cairn_common::process_identity::KernelThreadStates,
    ) {
        let now = unix_time_ms();
        let wait = {
            let mut supervision = self.colocated_supervision.lock().unwrap();
            let since_unix_ms = match &supervision.kernel_wait {
                Some(prior) if prior.pid == pid && prior.stage == stage => prior.since_unix_ms,
                _ => now,
            };
            let wait = cairn_common::executor_protocol::ColocatedKernelWait {
                pid,
                stage,
                since_unix_ms,
                kernel,
                last_read_unix_ms: now,
            };
            supervision.kernel_wait = Some(wait.clone());
            wait
        };
        let mut evidence = ExecutorSubstrateEvidence::without_queue(
            ExecutorSubstrateState::KernelWait,
            wait.since_unix_ms,
            now,
        );
        evidence.diagnostic = Some(kernel_wait_diagnostic(&wait, now));
        *self.colocated_substrate_state.lock().unwrap() = Some(evidence);
        self.connection_ready.notify_waiters();
    }

    /// No thread of the process the supervisor was waiting on is in the kernel
    /// any longer. The substrate declaration is the caller's to replace with
    /// whatever the supervisor does next.
    pub fn release_colocated_kernel_wait(&self) {
        self.colocated_supervision.lock().unwrap().kernel_wait = None;
    }

    pub fn record_colocated_retirement(
        &self,
        summary: cairn_common::executor_protocol::ExecutorRetirementSummary,
    ) {
        self.colocated_supervision.lock().unwrap().last_retirement = Some(summary);
    }

    /// Replace what the supervisor is still re-verifying of earlier
    /// retirements: surviving workload processes, and cells whose workload
    /// could not be read.
    pub fn set_colocated_workload_ledger(
        &self,
        survivors: Vec<cairn_common::executor_protocol::WorkloadSurvivor>,
        unresolved_cells: Vec<cairn_common::executor_protocol::UnresolvedCell>,
    ) {
        let mut supervision = self.colocated_supervision.lock().unwrap();
        supervision.workload_survivors = survivors;
        supervision.unresolved_cells = unresolved_cells;
    }

    /// Tell the colocated executor attached under `generation` to stop, and
    /// detach it as a runner-initiated close. `false` when that generation is
    /// not attached, which is not an error: it may never have reached Ready, or
    /// may already be gone.
    pub fn shut_down_colocated_generation(&self, generation: u64, reason: &str) -> bool {
        let target = self
            .lock_connections()
            .iter()
            .find(|(_, entry)| entry.colocated && entry.generation == generation)
            .map(|(executor_id, entry)| (executor_id.clone(), entry.sender.clone()));
        let Some((executor_id, sender)) = target else {
            return false;
        };
        let _ = sender.send(ExecutorMessage::Shutdown {
            reason: reason.into(),
        });
        self.disconnect_advertised_executor_with_origin(
            &executor_id,
            generation,
            ExecutorDisconnectOrigin::RunnerInitiated,
        )
    }

    /// Whether the runner is actively rebuilding the colocated environment.
    ///
    /// This is the fact [`placement::classify_unavailable`] cannot know: a lost
    /// link means "there is no machine" or "the machine is restarting" depending
    /// entirely on what the supervisor is doing about it, and only the runner is
    /// holding that.
    ///
    /// Read from the supervisor's own declaration, which is trustworthy in both
    /// directions because the supervisor clears it the moment a link attaches
    /// healthily. Three conditions have to hold, and each rules out a way this
    /// could park an agent on a machine that is never coming back: the state must
    /// be one of the recovery states, the declaration must carry no failure of its
    /// own (a recovery that is failing has a diagnostic, and that diagnostic is
    /// the actionable thing to tell the caller), and it must be fresh by the same
    /// progress rule every other substrate hold is judged by.
    ///
    /// A kernel wait is a recovery whose diagnostic is not a failure: it says
    /// what the host is doing, and it ends when the host's storage returns,
    /// with no further act of the runner's. The supervisor re-declares it at
    /// every read, so it stays fresh only while the supervisor is watching.
    pub(crate) fn link_restoration(&self) -> placement::LinkRestoration {
        let Some(evidence) = self.colocated_substrate() else {
            return placement::LinkRestoration::NotRestoring;
        };
        let recovering = match evidence.state {
            ExecutorSubstrateState::SupervisorSpawning
            | ExecutorSubstrateState::SupervisorRespawning
            | ExecutorSubstrateState::ProtocolAttaching => evidence.diagnostic.is_none(),
            ExecutorSubstrateState::KernelWait => true,
            _ => false,
        };
        let fresh = unix_time_ms().saturating_sub(evidence.last_progress_unix_ms)
            <= EXECUTOR_PROGRESS_FRESHNESS_MS;
        if recovering && fresh {
            placement::LinkRestoration::Restoring
        } else {
            placement::LinkRestoration::NotRestoring
        }
    }

    /// The connection generation one machine is attached under right now.
    ///
    /// A generation that has moved since a result was produced means the machine
    /// reattached and was re-measured, so whatever that result observed belongs
    /// to a machine state that no longer exists.
    pub(crate) fn live_generation(&self, executor_id: &str) -> Option<u64> {
        self.lock_connections()
            .values()
            .find(|entry| entry.identity.executor_id == executor_id && !entry.sender.is_closed())
            .map(|entry| entry.generation)
    }

    /// Whether the colocated machine's fresh-pipe floor refuses `work`.
    pub(super) fn colocated_pipe_floor_refuses(
        &self,
        work: PipeFloorWork,
        now_unix_ms: u64,
    ) -> bool {
        self.lock_connections()
            .values()
            .find(|entry| entry.colocated)
            .is_some_and(|entry| !pipe_health_assessment(entry, now_unix_ms).admits_work(work))
    }

    /// One attached machine's fresh-pipe floor, assessed now. `None` when no
    /// machine with that id is attached.
    pub fn executor_pipe_health(
        &self,
        executor_id: &str,
    ) -> Option<cairn_common::pipe_health::PipeHealthAssessment> {
        self.lock_connections()
            .get(executor_id)
            .map(|entry| pipe_health_assessment(entry, unix_time_ms()))
    }

    pub fn executor_generation(&self) -> Option<u64> {
        self.lock_connections()
            .values()
            .find(|entry| entry.colocated && !entry.sender.is_closed())
            .map(|entry| entry.generation)
    }

    /// The clock the transport's reader and ingest tasks stamp for this
    /// connection. Fetched once after attach and stamped lock-free, so the
    /// stamps never wait on the connection table they describe.
    /// Permission for `generation` to write this executor's synced registry
    /// row, held across the write. `None` once a newer generation has attached.
    ///
    /// A replaced connection's writes (its registry writer draining a last
    /// "online" upsert, its teardown's "offline" mark) can otherwise land after
    /// its replacement's and put the old generation back on the row. The check
    /// and the write happen under one per-executor lock, so a write already
    /// under way when the replacement attaches finishes before any of the
    /// replacement's begins, and every later one is refused. The newest
    /// generation is this process's own counter, so a runner restart starts
    /// it over and the new runner's first write is never fenced out by a row
    /// the previous one left.
    pub async fn registry_write_permit(
        &self,
        executor_id: &str,
        generation: u64,
    ) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        let fence = self
            .registry_write_fences
            .lock()
            .unwrap()
            .entry(executor_id.to_owned())
            .or_default()
            .clone();
        let permit = fence.lock_owned().await;
        let newest = self
            .connection_generations
            .lock()
            .unwrap()
            .get(executor_id)
            .copied();
        (newest == Some(generation)).then_some(permit)
    }

    /// The generation this executor's registered connection holds, if any.
    ///
    /// An attach replaces the entry and ends the replaced generation's work,
    /// but the replaced connection's task keeps running until its own socket
    /// fails. That task reads this to learn it is superseded and stop, rather
    /// than act for a connection the fleet has already moved past.
    pub fn connected_generation(&self, executor_id: &str) -> Option<u64> {
        self.lock_connections()
            .get(executor_id)
            .map(|entry| entry.generation)
    }

    pub fn link_clock(
        &self,
        executor_id: &str,
        generation: u64,
    ) -> Option<Arc<link_clock::LinkClock>> {
        self.lock_connections()
            .get(executor_id)
            .filter(|entry| entry.generation == generation)
            .map(|entry| entry.link_clock.clone())
    }

    /// This machine's silence past the placement liveness bound, by the one
    /// staleness rule, or `None` while it is fresh, absent, or unheard because
    /// the runner was not listening.
    pub fn peer_silence_ms(&self, executor_id: &str, now_unix_ms: u64) -> Option<u64> {
        self.lock_connections()
            .get(executor_id)
            .and_then(|entry| link_silence_ms(entry, now_unix_ms))
    }

    /// Runner-observed silence for one exact managed connection generation.
    pub fn managed_link_silence_ms(
        &self,
        executor_id: &str,
        generation: u64,
        now_unix_ms: u64,
    ) -> Option<u64> {
        self.lock_connections()
            .get(executor_id)
            .filter(|entry| entry.generation == generation)
            .map(|entry| now_unix_ms.saturating_sub(entry.last_progress_unix_ms))
    }

    /// Whether the colocated link has stopped being worth waiting on.
    ///
    /// `now_unix_ms` is a parameter rather than read from the clock so the
    /// decision is testable without wall-clock sleeps, following
    /// [`deadline_evidence`]. Returns [`LinkRemediation::Healthy`] when no
    /// colocated connection is attached: the supervisor's spawn and readiness
    /// paths already own that case, and a link that was never established is not
    /// a link to bounce.
    ///
    /// Neither criterion is duration. Any inbound message — heartbeat, snapshot,
    /// result — bumps `last_progress_unix_ms`, and every beat carries a fresh
    /// recompute stamp, so an executor grinding through an hour-long check keeps
    /// reporting `Healthy` throughout.
    ///
    /// Silence is judged first because it is the stronger statement: a link
    /// nobody has heard from cannot also be diagnosed as having a telemetry
    /// problem, and reporting it as one would send an operator after the wrong
    /// failure.
    ///
    /// A fact freeze the executor explains is not remediated here at all; see
    /// [`unexplained_facts_frozen_ms`].
    pub fn assess_colocated_link(
        &self,
        now_unix_ms: u64,
        bounds: LinkStallBounds,
    ) -> LinkRemediation {
        let observed = self
            .lock_connections()
            .values()
            .find(|entry| entry.colocated)
            .map(|entry| {
                (
                    entry.identity.executor_id.clone(),
                    entry.generation,
                    judge_link(entry, now_unix_ms, bounds.silence_ms),
                    entry
                        .link_clock
                        .reader_silence_ms(link_clock::monotonic_now_ns()),
                    unexplained_facts_frozen_ms(entry, link_clock::monotonic_now_ns()),
                )
            });
        let Some((executor_id, generation, judgement, pump_silence_ms, frozen_ms)) = observed
        else {
            return LinkRemediation::Healthy;
        };
        match judgement {
            link_clock::LinkJudgement::PeerSilent { silence_ms } => {
                return LinkRemediation::Bounce {
                    executor_id,
                    generation,
                    cause: LinkStallCause::Silence {
                        silence_ms,
                        pump_silence_ms,
                    },
                };
            }
            // Bouncing a healthy child because the runner was not listening
            // would turn the runner's stall into the executor's restart.
            link_clock::LinkJudgement::JudgeAbsent { .. } => return LinkRemediation::Healthy,
            link_clock::LinkJudgement::Fresh => {}
        }
        match frozen_ms {
            Some(frozen_ms) if frozen_ms > bounds.telemetry_ms => LinkRemediation::Bounce {
                executor_id,
                generation,
                cause: LinkStallCause::TelemetryFrozen { frozen_ms },
            },
            _ => LinkRemediation::Healthy,
        }
    }

    /// How long this exact connection generation has been publishing facts that
    /// stopped being recomputed, for a link that is otherwise beating normally.
    ///
    /// `None` covers four different "nothing to remediate" answers on purpose:
    /// the generation is not attached, the link is silent past its own bound so
    /// silence remediation owns it, the executor publishes no recompute stamp
    /// at all, or it publishes one that is frozen *and says why* — see
    /// [`unexplained_facts_frozen_ms`]. A clock that was never claimed is not a
    /// stalled clock, no machine may be bounced for a fact it never offered,
    /// and a machine reporting its own wedge has already done the thing the
    /// bounce exists to provoke.
    pub fn managed_telemetry_stall_ms(
        &self,
        executor_id: &str,
        generation: u64,
        now_unix_ms: u64,
    ) -> Option<u64> {
        let connections = self.lock_connections();
        let entry = connections
            .get(executor_id)
            .filter(|entry| entry.generation == generation)?;
        if !matches!(
            judge_link(entry, now_unix_ms, EXECUTOR_LINK_STALL_REMEDIATION_MS),
            link_clock::LinkJudgement::Fresh
        ) {
            return None;
        }
        unexplained_facts_frozen_ms(entry, link_clock::monotonic_now_ns())
    }

    /// Take the fleet's single telemetry-bounce turn, or decline.
    ///
    /// Callers must hold a turn before recycling a link for frozen telemetry;
    /// see [`Fleet::last_telemetry_bounce_unix_ms`] for why that failure is the
    /// one that must not be answered fleet-simultaneously. Declining is not an
    /// error: the next poll re-assesses, and a machine whose facts are still
    /// frozen a minute later takes the turn then.
    pub fn claim_telemetry_stall_bounce(&self, now_unix_ms: u64) -> bool {
        let mut last = self.last_telemetry_bounce_unix_ms.lock().unwrap();
        if *last != 0 && now_unix_ms.saturating_sub(*last) < TELEMETRY_STALL_BOUNCE_SPACING_MS {
            return false;
        }
        *last = now_unix_ms;
        true
    }

    /// Abandon a colocated link that has gone silent, so the supervisor can
    /// replace the process behind it.
    ///
    /// Retires the generation, which discards anything a wedged pump writes if it
    /// later resumes; resolves every attempt this connection owned to a typed
    /// retryable outcome; and parks the surface in `SupervisorRespawning` so
    /// subscribers that are still waiting pause their deadlines rather than
    /// expiring against an environment that is coming back.
    ///
    /// Returns an empty abandonment, without any side effect, when `generation`
    /// no longer owns the link — a reattach that landed between assessment and
    /// action owns it now, and bouncing that would strand the healthy
    /// connection that replaced the sick one.
    ///
    /// Records no runner event: the supervisor that decided the reset writes
    /// the one `executor_link_reset` row, because only it knows the pid, how
    /// many resets ran consecutively, and how the retirement went.
    pub fn abandon_stalled_colocated_link(
        &self,
        executor_id: &str,
        generation: u64,
        cause: LinkStallCause,
    ) -> StalledLinkAbandonment {
        if !self
            .lock_connections()
            .get(executor_id)
            .is_some_and(|entry| entry.generation == generation)
        {
            return StalledLinkAbandonment::default();
        }
        // Captured before the disconnect, which clears this link's entries on
        // its way through the generation-end sweep. These identities are how an
        // in-flight coalesced execution is attributed to this connection:
        // `InFlightExecution` itself carries no executor identity, and a remote
        // executor's leaders must survive a colocated bounce untouched.
        let abandoned_leaders: Vec<RequestIdentity> =
            self.queue.identities_on_generation(executor_id, generation);
        let last_known_state = self
            .colocated_substrate()
            .map(|evidence| evidence.state)
            .or_else(|| {
                self.lock_connections()
                    .get(executor_id)
                    .and_then(|entry| entry.snapshot.substrate_state.clone())
                    .map(|evidence| evidence.state)
            });
        // Declared first: a waiter that observes the teardown must already see a
        // recovering environment rather than expire against a failing one.
        self.declare_colocated_substrate(ExecutorSubstrateState::SupervisorRespawning);
        let disconnected = self.disconnect_advertised_executor_with_origin(
            executor_id,
            generation,
            ExecutorDisconnectOrigin::RunnerInitiated,
        );
        let abandoned_in_flight =
            self.abandon_in_flight_for_leaders(executor_id, generation, &abandoned_leaders);
        log::warn!(
            "abandoned stalled executor link executor_id={executor_id} generation={generation} \
             cause={cause} disconnected={disconnected} \
             abandoned_attempts={} abandoned_in_flight={abandoned_in_flight} \
             last_known_state={last_known_state:?}",
            abandoned_leaders.len(),
            cause = cause.describe(),
        );
        StalledLinkAbandonment {
            disconnected,
            abandoned_attempts: abandoned_leaders.len(),
            abandoned_in_flight,
            last_known_state,
        }
    }

    /// Resolve every coalesced execution led by one of `leaders` to a typed
    /// retryable outcome.
    ///
    /// The disconnect already resolved each leader's own waiter, and a leader
    /// that is still being polled will publish that outcome to its subscribers
    /// itself. This closes the gap by construction instead: a subscriber whose
    /// execution had already started has no deadline left to expire against
    /// (`await_coalesced` stops evaluating one once `ExecutionRunning` is
    /// observed), so leaving its resolution to a chain of collaborating drop
    /// guards is what turns a link reset into an indefinite hold.
    pub(super) fn abandon_in_flight_for_leaders(
        &self,
        executor_id: &str,
        generation: u64,
        leaders: &[RequestIdentity],
    ) -> usize {
        let settlement = LeaderSettlement::Settled {
            outcome: CellOutcome::Unavailable {
                reason: CellUnavailableReason::ExecutorUnavailable,
                diagnostic: format!(
                    "executor {executor_id} (generation {generation}): the build environment \
                     was reset while this attempt was in flight; it is being restored and a \
                     retry runs normally"
                ),
            },
            account: CellAccount {
                left_running: None,
                cancel_cause: None,
                placement: Some(CellPlacement {
                    executor_id: executor_id.to_string(),
                    generation,
                    name: self.executor_name_for_text(executor_id),
                    platform: self.executor_platform(executor_id),
                }),
            },
        };
        let mut resolved = 0;
        for leader in leaders {
            let led: Vec<CoalescingKey> = self
                .in_flight
                .lock()
                .unwrap()
                .by_key
                .keys()
                .filter(|key| &key.leader == leader)
                .cloned()
                .collect();
            for key in led {
                // Leader-fenced, so whichever of this and the leader's own
                // publication runs second is a no-op rather than a double send.
                if self.complete_coalesced_for_leader(&key, settlement.clone()) {
                    resolved += 1;
                }
            }
        }
        resolved
    }

    pub fn managed_generation(&self, executor_id: &str, device_id: &str) -> Option<u64> {
        self.lock_connections()
            .get(executor_id)
            .filter(|entry| {
                !entry.colocated
                    && entry.identity.device_id == device_id
                    && !entry.sender.is_closed()
            })
            .map(|entry| entry.generation)
    }

    /// End a managed executor's link deliberately, telling it why.
    ///
    /// The reason travels to the machine rather than staying in this runner's
    /// log. An executor whose link simply dies reads a socket reset and
    /// records that its runner dropped it; one that is told records the
    /// runner's own account of the decision, so the two logs of a single event
    /// agree instead of accusing each other.
    pub fn shutdown_advertised_executor(
        &self,
        executor_id: &str,
        device_id: &str,
        generation: u64,
        reason: impl Into<String>,
    ) -> bool {
        let sender = self
            .lock_connections()
            .get(executor_id)
            .filter(|entry| entry.generation == generation && entry.identity.device_id == device_id)
            .map(|entry| entry.sender.clone());
        let Some(sender) = sender else { return false };
        let _ = sender.send(ExecutorMessage::Shutdown {
            reason: reason.into(),
        });
        self.disconnect_advertised_executor_with_origin(
            executor_id,
            generation,
            ExecutorDisconnectOrigin::RunnerInitiated,
        );
        true
    }

    /// Record that this runner is shutting down, then stop the colocated
    /// executor before the runner begins waiting for transport connections to
    /// drain. Managed peers are not owned by this process and must survive a
    /// local daemon replacement.
    ///
    /// The record comes first, and it is what every waiter is answered from:
    /// each in-flight request withdraws itself naming the stop and the phase it
    /// was in, and the colocated link's own sweep answers a claim the same way,
    /// rather than as a connection that closed or a machine that does not exist.
    /// Returns whether a colocated executor was attached.
    pub fn begin_runner_shutdown(&self) -> bool {
        self.runner_stop.begin(unix_time_ms());
        let target = self
            .lock_connections()
            .iter()
            .find(|(_, entry)| entry.colocated)
            .map(|(executor_id, entry)| {
                (executor_id.clone(), entry.generation, entry.sender.clone())
            });
        let Some((executor_id, generation, sender)) = target else {
            return false;
        };
        let _ = sender.send(ExecutorMessage::Shutdown {
            reason: "this runner is shutting down".into(),
        });
        self.disconnect_advertised_executor_with_origin(
            &executor_id,
            generation,
            ExecutorDisconnectOrigin::RunnerInitiated,
        )
    }

    // The adoption API below serves the macOS rebuild consumer, the one
    // Cairn-owned tree outside every cell; it is compiled where that is.

    /// The colocated executor's link generation, while one is attached.
    #[cfg(target_os = "macos")]
    pub(crate) fn colocated_generation(&self) -> Option<u64> {
        self.lock_connections()
            .get(COLOCATED_EXECUTOR_ID)
            .filter(|entry| entry.colocated)
            .map(|entry| entry.generation)
    }

    /// The keys the colocated executor's latest snapshot reports adopted
    /// under `label`: its own account of what it measures, which outlives a
    /// link gap and includes an adoption accepted after its answer was lost.
    #[cfg(target_os = "macos")]
    pub(crate) fn colocated_adopted_keys(&self, label: &str) -> Vec<String> {
        self.lock_connections()
            .get(COLOCATED_EXECUTOR_ID)
            .filter(|entry| entry.colocated)
            .map(|entry| {
                entry
                    .snapshot
                    .executing_requests
                    .iter()
                    .filter(|request| {
                        request
                            .adopted
                            .as_ref()
                            .is_some_and(|tree| tree.label == label)
                    })
                    .map(|request| request.request_id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Ask the colocated executor, on exactly the link `expected_generation`
    /// names, to begin or end measuring a Cairn-owned tree outside every cell.
    /// Never addressed to a remote machine: such a tree runs on this host.
    #[cfg(target_os = "macos")]
    pub(crate) async fn change_process_tree_adoption(
        &self,
        expected_generation: u64,
        change: cairn_common::executor_protocol::ProcessTreeAdoptionChange,
    ) -> Result<cairn_common::executor_protocol::ProcessTreeAdoptionOutcome, String> {
        let sender = self
            .lock_connections()
            .get(COLOCATED_EXECUTOR_ID)
            .filter(|entry| entry.colocated && entry.generation == expected_generation)
            .map(|entry| entry.sender.clone())
            .ok_or_else(|| "local executor connection generation is stale".to_string())?;
        let correlation_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending_tree_adoption.lock().unwrap().insert(
            correlation_id.clone(),
            PendingTreeAdoptionResult {
                generation: expected_generation,
                waiter: tx,
            },
        );
        let sent = sender.send(ExecutorMessage::ProcessTreeAdoptionRequest {
            correlation_id: correlation_id.clone(),
            expected_generation,
            change,
        });
        let result = match sent {
            Err(_) => Err("local executor disconnected before the adoption was sent".into()),
            Ok(()) => match tokio::time::timeout(TREE_ADOPTION_RESPONSE_TIMEOUT, rx).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err("local executor dropped the adoption response".into()),
                Err(_) => Err("local executor did not answer the adoption; an executor older than adoption drops it".into()),
            },
        };
        self.pending_tree_adoption
            .lock()
            .unwrap()
            .remove(&correlation_id);
        result
    }

    /// The colocated machine's identity and capabilities as its profiles are
    /// keyed, while it is attached.
    #[cfg(target_os = "macos")]
    pub(super) fn colocated_profile(
        &self,
    ) -> Option<(SelectedExecutor, resource_profiles::ProfileContext)> {
        let selected = self
            .lock_connections()
            .get(COLOCATED_EXECUTOR_ID)
            .filter(|entry| entry.colocated)
            .map(selected_executor)?;
        let context = ReservationPlan::profile_context(&selected);
        Some((selected, context))
    }

    /// What a release build adopted on the colocated machine is charged: its
    /// class's learned memory profile there, or the class prior until one is
    /// learned, and never any concurrency. It is counted, not admitted.
    #[cfg(target_os = "macos")]
    pub(crate) async fn colocated_release_build_reservation(
        &self,
        db: Arc<cairn_db::storage::LocalDb>,
        identity: &CommandResourceIdentity,
    ) -> Option<cairn_common::executor_protocol::ResourceReservation> {
        let (selected, context) = self.colocated_profile()?;
        let class = cairn_common::executor_protocol::CellCommandClass::ReleaseBuild;
        let resolved = resource_profiles::resolve_reservation(
            db,
            Some(identity),
            &context,
            resource_profiles::cold_start_prior(class, &selected.capabilities),
            selected.capabilities.memory_budget_bytes,
            resource_profiles::DurationContext {
                class,
                warmth: cairn_common::executor_protocol::ExecutionWarmth::default(),
                now_unix_ms: unix_time_ms(),
            },
        )
        .await;
        Some(cairn_common::executor_protocol::ResourceReservation {
            concurrency_units: 0,
            ..resolved.reservation
        })
    }

    /// Teach the release build's profile what an adopted tree measured, through
    /// the one learner every command completes into. The measurement makes no
    /// warmth claim and carries no load context, so it teaches memory and never
    /// the duration predictor; a phase that failed teaches its peak as the
    /// failure kind, which keeps its duration out of the profile entirely.
    #[cfg(target_os = "macos")]
    pub(crate) async fn observe_adopted_release(
        &self,
        db: Arc<cairn_db::storage::LocalDb>,
        identity: &CommandResourceIdentity,
        measured: &cairn_common::executor_protocol::AdoptedTreeMeasurement,
        succeeded: bool,
    ) {
        let Some((selected, context)) = self.colocated_profile() else {
            return;
        };
        resource_profiles::observe_completed(
            db,
            Some(identity),
            &context,
            &adopted_release_metadata(&selected, measured),
            match succeeded {
                true => resource_profiles::CompletionKind::Succeeded,
                false => resource_profiles::CompletionKind::Failed,
            },
            resource_profiles::CompletedSource::default(),
        )
        .await;
    }
}

pub(super) fn require_colocated_population(
    request: &mut CellRequest,
    config: &ExecutorConfig,
) -> Result<(), String> {
    if config.populate.is_empty()
        || matches!(
            request.repository,
            RepositoryLocator::ExistingCheckout { .. } | RepositoryLocator::ScratchOnly { .. }
        )
    {
        return Ok(());
    }
    if request
        .executor
        .as_ref()
        .and_then(|selector| selector.name.as_deref())
        .is_some_and(|name| !executor_names_match(name, LOCAL_EXECUTOR_NAME))
    {
        return Err(
            "worktree population requires the local executor because ignored project content is available only in the runner's live primary checkout; remove the executor selector or run without worktree population"
                .into(),
        );
    }
    request.pinned_executor_id = Some(COLOCATED_EXECUTOR_ID.into());
    // The pin already settles placement, and stating the mobility alongside it
    // keeps the decision record from claiming this batch was free to move.
    request.placement_mobility = PlacementMobility::PinnedOrColocated;
    Ok(())
}

/// An adopted tree's measurement in the shape the profile learner reads, with
/// no warmth claim and no load context: what it says about speed is nothing.
// Compile the constructor everywhere so new metadata fields cannot break only
// the macOS adoption path, whose caller is platform-gated.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub(super) fn adopted_release_metadata(
    selected: &SelectedExecutor,
    measured: &cairn_common::executor_protocol::AdoptedTreeMeasurement,
) -> CellExecutionMeta {
    CellExecutionMeta {
        commands_exited_at_unix_ms: None,
        executor_id: selected.executor_id.clone(),
        executor_device_id: selected.device_id.clone(),
        executor_connection_generation: selected.generation,
        cell_id: String::new(),
        cell_epoch: 0,
        started_at_unix_ms: measured.started_at_unix_ms,
        finished_at_unix_ms: measured.finished_at_unix_ms,
        duration_ms: Some(measured.duration_ms),
        peak_rss_bytes: measured.peak_rss_bytes,
        peak_physical_footprint_bytes: None,
        peak_process: measured.peak_process.clone(),
        disk_delta_bytes: None,
        measurement_quality: Some(measured.measurement_quality.clone()),
        warmth: None,
        cell_origin: None,
        load_context: None,
        allocation: None,
        cpu: None,
        environment_fingerprint: String::new(),
        verdict_platform: None,
        verdict_arch: None,
        toolchain_fingerprint: None,
        verdict_environment_hash: None,
        sandbox: None,
        containment: None,
        stop: None,
        test_phase: None,
        build_job_processes: None,
    }
}
