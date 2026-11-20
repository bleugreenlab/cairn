use super::*;

pub(super) struct CoalescedLeaderCompletionGuard {
    pub(super) pool: Fleet,
    pub(super) leader: RequestIdentity,
    pub(super) work_class: PlacementWorkClass,
    pub(super) keys: Vec<CoalescingKey>,
    pub(super) runner_context_id: Option<String>,
    pub(super) armed: bool,
}

impl CoalescedLeaderCompletionGuard {
    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

/// Lock `mutex` even when a holder that panicked poisoned it.
///
/// For destructors that run during an unwind: a second panic there, on a lock
/// the unwinding code held, aborts the whole process.
pub(super) fn lock_through_poison<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The error line for a coalesced leader that ended without an outcome.
///
/// Two populations end this way and look nothing alike to the person reading
/// the log: a leader whose task panicked, and one whose task was dropped
/// unfinished, which is what a runtime shutting down does to every task on it.
pub(super) fn unfinished_leader_line(
    leader: &RequestIdentity,
    work_class: PlacementWorkClass,
    machine: Option<&str>,
    panicking: bool,
) -> String {
    let ending = if panicking {
        "its task panicked; the panic is logged just before this line"
    } else {
        "its task was dropped unfinished, as a runtime shutting down drops the tasks on it"
    };
    let machine = machine.map_or_else(
        || "no machine reported running it".to_string(),
        |machine| format!("last reported running on {machine}"),
    );
    format!(
        "coalesced {} leader {leader:?} ended without publishing a terminal outcome: {ending}; \
         {machine}; its subscribers are told it vanished",
        work_class.as_str()
    )
}

impl Drop for CoalescedLeaderCompletionGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        lock_through_poison(&self.pool.cancelled_leaders).remove(&self.leader);
        lock_through_poison(&self.pool.coalesced_leaders).remove(&self.leader);
        lock_through_poison(&self.pool.preparing_leaders).remove(&self.leader);
        if let Some(id) = &self.runner_context_id {
            self.pool.end_runner_context(id);
        }
        // The leader's queue entry is withdrawn by its own guard, which drops
        // first, so the binding is usually gone by now; the machine's last
        // snapshot still names the request until it next reports.
        let machine = self
            .pool
            .queue
            .binding_of(&self.leader)
            .and_then(|(executor_id, _)| self.pool.executor_public_name(&executor_id))
            .or_else(|| self.pool.machine_last_running(&self.leader));
        log::error!(
            "{}",
            unfinished_leader_line(
                &self.leader,
                self.work_class,
                machine.as_deref(),
                std::thread::panicking(),
            )
        );
        // A leader that ended without an outcome proves nothing about the
        // machine it may have been granted to. Before a grant its question
        // owes nothing; after one, the debt stays, unconfirmed, naming that
        // machine.
        if lock_through_poison(&self.pool.purposes).end_unconfirmed(
            &self.leader,
            purpose::UnconfirmedReason::LeaderVanished,
            unix_time_ms(),
        ) {
            self.pool.purpose_quiesced.notify_waiters();
        }
        let settlement = if !std::thread::panicking() {
            self.pool
                .left_running_attempt(&self.leader)
                .map(|left_running| LeaderSettlement::Settled {
                    outcome: self.pool.stopped_outcome(
                        left_running.stopped_at_ms,
                        &self.leader,
                        Some(&left_running.executor_id),
                    ),
                    account: CellAccount {
                        left_running: Some(left_running),
                        ..CellAccount::default()
                    },
                })
                .unwrap_or(LeaderSettlement::Vanished)
        } else {
            LeaderSettlement::Vanished
        };
        for key in &self.keys {
            self.pool
                .complete_coalesced_for_leader(key, settlement.clone());
        }
    }
}

/// Scrub the free-form text an executor reports with a terminal outcome.
///
/// This is the model-visible copy of a batch's output, and unlike the live
/// stream it arrives whole, so one exact pass covers it completely with no
/// chunk boundaries to reason about. The diagnostics are included because a
/// spawn failure, a preparation timeout, or a storage error quotes the command
/// and whatever it managed to print.
/// Where a pure-verdict check batch's finished items settle before the batch
/// returns.
///
/// Each item's stream names the coalesced execution it answers. What was
/// settled early is kept until the batch's own result arrives, which then
/// answers only the items no report reached and checks the rest against what
/// their subscribers were already given.
pub(super) struct ItemSettlements {
    pub(super) leader: RequestIdentity,
    pub(super) keys: HashMap<String, CoalescingKey>,
    pub(super) settled: Mutex<HashMap<CoalescingKey, CellOutcome>>,
}

impl ItemSettlements {
    /// The outcome `key` was settled with early, taken so the batch's end
    /// handles each key once.
    fn take(&self, key: &CoalescingKey) -> Option<CellOutcome> {
        lock_through_poison(&self.settled).remove(key)
    }
}

/// The verdict-bearing fields on which an item's early settlement and the
/// batch's own result for it disagree; empty when they agree.
///
/// Measurements are left out on purpose: an early report reads the command's
/// test phase as far as the batch had got, so its metadata can differ from
/// the batch's in what was measured, never in what was decided.
pub(crate) fn verdict_disagreement(early: &CellOutcome, last: &CellOutcome) -> Vec<&'static str> {
    let (
        CellOutcome::Completed {
            exit_code,
            output,
            timed_out,
            metadata,
            sandbox_denials,
            tracked_modifications,
            ..
        },
        CellOutcome::Completed {
            exit_code: last_exit_code,
            output: last_output,
            timed_out: last_timed_out,
            metadata: last_metadata,
            sandbox_denials: last_sandbox_denials,
            tracked_modifications: last_tracked_modifications,
            ..
        },
    ) = (early, last)
    else {
        return vec!["outcome"];
    };
    let mut differing = Vec::new();
    if exit_code != last_exit_code {
        differing.push("exit code");
    }
    if output != last_output {
        differing.push("output");
    }
    if timed_out != last_timed_out {
        differing.push("timed out");
    }
    if metadata.stop != last_metadata.stop {
        differing.push("stop");
    }
    if sandbox_denials != last_sandbox_denials {
        differing.push("sandbox denials");
    }
    if tracked_modifications != last_tracked_modifications {
        differing.push("tracked modifications");
    }
    differing
}

/// What a check request's subscriber identity adds to its request identity.
const CHECK_SUBSCRIBER_SUFFIX: &str = ":check";

pub(super) fn scrub_cell_outcome(outcome: &mut CellOutcome) {
    use crate::security::{Crossing, DetectionReport, Sanitizer};

    let mut sanitizer = Sanitizer::exact();
    if sanitizer.is_noop() {
        return;
    }
    match outcome {
        CellOutcome::Completed { output, .. } => sanitizer.text_in_place(output),
        CellOutcome::Unavailable { diagnostic, .. }
        | CellOutcome::FailedAfterExecution { diagnostic, .. }
        | CellOutcome::StorageFailure { diagnostic, .. } => sanitizer.text_in_place(diagnostic),
        CellOutcome::Cancelled { .. } => {}
    }
    for report in DetectionReport::from_detections(
        &sanitizer.into_detections(),
        Crossing::ProcessOutput,
        None,
        None,
    ) {
        report.log();
    }
}

/// Whether a submitted batch runs in the project's externally owned live
/// checkout, as opposed to a cell the executor materialized.
///
/// The locator states it. Nothing on disk does: a cell checkout is a plain
/// detached git checkout, indistinguishable from the user's own, so a
/// `.jj`-marker test answers "live checkout" for every cell in the fleet.
pub(super) fn runs_in_live_checkout(repository: &RepositoryLocator) -> bool {
    matches!(repository, RepositoryLocator::ExistingCheckout { .. })
}

pub(super) struct PreparedExecution {
    pub(super) executor_config: ExecutorConfig,
    pub(super) object_plane: Arc<crate::orchestrator::object_plane::ObjectPlaneState>,
    pub(super) db: Arc<cairn_db::storage::LocalDb>,
    /// This machine's build-service client env, or empty when this batch must
    /// not be pointed at the supervised daemon. Resolved here because the
    /// runner owns the service configuration; applied only for a colocated
    /// placement, because the daemon answers on loopback and is named by this
    /// machine's paths. See [`cell_build_service_env`].
    pub(super) cell_client_env: Vec<(String, String)>,
    pub(super) placement_policy: ActivePlacementPolicy,
    /// The runner's own store for the project, which dates the commit the
    /// work ran for the learned largest process
    /// ([`resource_profiles::source_committed_at`]). `None` for scratch work,
    /// which runs no project source.
    pub(super) source_store: Option<PathBuf>,
}

#[derive(Clone)]
pub(super) struct ActivePlacementPolicy {
    pub(super) name: String,
    pub(super) profile: PlacementProfile,
}

impl ActivePlacementPolicy {
    #[cfg(test)]
    pub(super) fn default_profile() -> Self {
        Self {
            name: DEFAULT_PLACEMENT_PROFILE.to_string(),
            profile: built_in_profile(DEFAULT_PLACEMENT_PROFILE).unwrap().clone(),
        }
    }
}

/// The build-service client env a cell batch should carry.
///
/// A cell the executor materializes builds inside `{cairnHome}/build-slots`,
/// which is a managed build root: the daemon's writable grant covers its
/// `target/` tree, so its compiles belong on the shared compile cache. A batch
/// running in the project's live checkout does not, and this is not a question
/// of losing a cache hit — the daemon runs each cache-miss compile itself, so a
/// build whose `target/` its sandbox does not cover fails outright with
/// `Operation not permitted`. That is the split the Cairn-specific sccache port
/// exists to keep: the developer's own checkout starts its own unconfined server
/// on sccache's default port instead.
pub(super) fn cell_build_service_env(
    orch: &Orchestrator,
    repository: &RepositoryLocator,
) -> Vec<(String, String)> {
    if runs_in_live_checkout(repository) {
        return Vec::new();
    }
    let mut env: Vec<(String, String)> = orch.cell_build_service_client_env().into_iter().collect();
    env.sort();
    env
}

/// Add the machine's build-service client env to every item of a batch bound for
/// a colocated cell. An item that already names a variable keeps its own value:
/// a caller that stated it meant it.
pub(super) fn with_cell_client_env(
    mut batch: ProcessBatch,
    env: &[(String, String)],
) -> ProcessBatch {
    for item in &mut batch.items {
        for (key, value) in env {
            if !item.env.iter().any(|(existing, _)| existing == key) {
                item.env.push((key.clone(), value.clone()));
            }
        }
    }
    batch
}

#[derive(Clone, Copy)]
pub(super) struct LeaderPreparation {
    pub(super) since_unix_ms: u64,
    pub(super) last_progress_unix_ms: u64,
}

/// What the machines report about one request; see [`Fleet::request_report`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RequestReport {
    /// A machine that could hold it has not reported since this runner
    /// started; `awaiting` names them, in the words an operator reads.
    Unknown { awaiting: Vec<String> },
    /// A machine still holds an attempt of it.
    Held { executor_id: String },
    /// A machine finished it and still lists the completion.
    Completed(Box<cairn_common::executor_protocol::CellCompletion>),
    /// Every machine that could hold it has reported, and none holds or
    /// finished it; or the machine that held it is no longer enrolled.
    Gone,
}

pub(super) struct CoalescedSubscriberDropGuard {
    pub(super) pool: Fleet,
    pub(super) identity: RequestIdentity,
    pub(super) armed: bool,
}

impl CoalescedSubscriberDropGuard {
    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CoalescedSubscriberDropGuard {
    fn drop(&mut self) {
        if self.armed {
            self.pool.detach_coalesced_subscriber(&self.identity, None);
        }
    }
}

/// What detaching one subscriber did to the execution it was waiting on.
pub(super) enum Detachment {
    /// The last subscriber let go, so the execution itself is retired.
    Retired { leader: RequestIdentity },
    /// Interest released; the execution continues for the subscribers left.
    Released { remaining: usize },
    /// No such subscriber.
    Unknown,
}

/// What a retraction did, in terms a surface can state without guessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "outcome")]
pub enum CancelOutcome {
    /// Nobody else wanted this answer, so the work has been told to stop.
    /// Ungranted work ends through the runner's cancellation tail; granted work
    /// still needs its machine's terminal report, not just a delivered Cancel.
    Stopping,
    /// This requester let go and the work continues for the ones that remain.
    Released { remaining: usize },
    /// Nothing live goes by that name: it finished, or was already retired,
    /// between the row rendering and the click landing.
    Unknown,
}

/// Cancels an entry whose caller went away before it settled.
///
/// What it sends is addressed from the entry's own binding rather than from a
/// machine captured when the guard was armed: an entry may be offered to
/// several machines in turn, and the one that must be told is whichever one
/// currently holds it. An entry that was only ever offered has nothing behind
/// it, so nothing is sent. A claim the grant reached may have work running, so
/// that machine is sent `Cancel`; a claim no grant reached holds only its
/// ticket, which `Cancel` does not release, so that machine is sent `Lost`.
///
/// The entry's terminal journal row names why it was withdrawn and where it
/// stood: a caller that went away, or this runner shutting down. A bare
/// `cancelled` row is what let a dropped caller read, after the fact, like work
/// nothing had ever ended.
pub(super) struct EntryDropGuard {
    pub(super) pool: Fleet,
    pub(super) request_id: String,
    pub(super) attempt_id: String,
    pub(super) entry_id: String,
    /// The machine a grant for this entry was delivered to, once one was.
    pub(super) granted_to: Option<String>,
    /// Why the runner itself is ending the entry, when it is. Absent, an
    /// armed drop means the caller stopped awaiting it.
    pub(super) cause: Option<WithdrawalCause>,
    pub(super) armed: bool,
}

/// The runner's own reasons for withdrawing an entry, as distinct from a
/// caller that stopped awaiting it.
pub(super) enum WithdrawalCause {
    RunnerStopped(u64),
    /// The machine holding a granted attempt stopped accounting for it, for the
    /// rendered duration.
    ClaimedAttemptSilent(String),
}

/// Tell the machine that claimed an entry the grant is never coming.
///
/// The claim charged its reservation, bound the request identity, and marked
/// the project provisioning; only `Lost` (or the link ending) gives those back.
/// Removing the entry here changes nothing on the machine, so every exit
/// between a claim and its grant sends this, or the project stays blocked
/// behind a ticket until the link drops.
pub(super) fn withdraw_claim(selected: &SelectedExecutor, entry_id: &str) {
    let _ = selected.sender.send(ExecutorMessage::Lost {
        entry_id: entry_id.to_owned(),
    });
}

impl EntryDropGuard {
    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for EntryDropGuard {
    fn drop(&mut self) {
        // Whatever ended this entry, its rejections are no longer news.
        let request_id = &self.request_id;
        self.pool
            .placement_rejection_log
            .lock()
            .unwrap()
            .forget(|(rejected, _)| rejected == request_id);
        if !self.armed {
            return;
        }
        let identity = (self.request_id.clone(), self.attempt_id.clone());
        let claimed_by = self.pool.queue.claimed_binding_of(&identity);
        let phase = self
            .pool
            .withdrawal_phase(&identity, self.granted_to.as_deref());
        let stopped_at = match &self.cause {
            Some(WithdrawalCause::RunnerStopped(at)) => Some(*at),
            Some(WithdrawalCause::ClaimedAttemptSilent(_)) => None,
            None => self.pool.runner_stop.at(),
        };
        let cause = match (&self.cause, stopped_at) {
            (Some(WithdrawalCause::ClaimedAttemptSilent(quiet)), _) => format!(
                "withdrawn: the machine holding the request gave no account of it for {quiet}, so the runner stopped waiting while it was {phase}"
            ),
            (_, Some(stopped_at)) => format!(
                "withdrawn: this runner began shutting down at {} while the request was {phase}",
                describe_runner_stop(stopped_at)
            ),
            (_, None) => format!(
                "withdrawn: the caller awaiting this request went away while it was {phase}"
            ),
        };
        self.pool.queue.withdraw(&identity, cause, unix_time_ms());
        if let Some(stopped_at) = stopped_at {
            if self.granted_to.is_some() && self.pool.check_shutdown(&identity, stopped_at) {
                return;
            }
        }
        if let Some((executor_id, generation)) = claimed_by {
            let message = if self.granted_to.as_deref() == Some(executor_id.as_str()) {
                ExecutorMessage::Cancel {
                    request_id: self.request_id.clone(),
                    attempt_id: self.attempt_id.clone(),
                }
            } else {
                ExecutorMessage::Lost {
                    entry_id: self.entry_id.clone(),
                }
            };
            let _ = self.pool.send_to(&executor_id, generation, message);
        }
    }
}

impl Fleet {
    pub fn handle_executor_message(
        &self,
        executor_id: &str,
        generation: u64,
        message: ExecutorMessage,
    ) -> bool {
        // Every frame on a retired link is superseded by what the current one
        // says, except a result: it is the only record that a command ran, and
        // the runner may already have read it off the old socket when a redial
        // replaced that link and carried the claim forward. The `Result` arm
        // judges it against the claim's binding instead.
        if !matches!(message, ExecutorMessage::Result { .. })
            && self
                .lock_connections()
                .get(executor_id)
                .is_none_or(|entry| entry.generation != generation)
        {
            return false;
        }
        match message {
            ExecutorMessage::Result {
                request_id,
                attempt_id,
                mut outcome,
            } => {
                if !outcome_matches(&outcome, &request_id, &attempt_id) {
                    return false;
                }
                let key = (request_id, attempt_id);
                if self.queue.binding_of(&key).is_none()
                    && self.is_direct_check_result(&key, executor_id)
                    && self
                        .link_generation(executor_id)
                        .is_some_and(|current| current >= generation)
                {
                    self.receive_direct_check_result(&key, executor_id);
                    return false;
                }
                if self.adoption_owner(&key).is_some()
                    && self
                        .check_claim(&key)
                        .is_some_and(|claim| claim.executor_id == executor_id)
                    && self.queue.binding_of(&key).is_none()
                {
                    scrub_cell_outcome(&mut outcome);
                    self.hold_check_adoption(UnboundResult {
                        executor_id: executor_id.to_owned(),
                        machine: self.executor_name_for_text(executor_id),
                        request_id: key.0,
                        attempt_id: key.1,
                        outcome,
                    });
                    return false;
                }
                // The result has to come from the machine this entry is bound to,
                // on the link it is bound on or one the binding was carried
                // forward to. A redial that replaces an unclosed link rebinds the
                // claim before the old link's ingest has drained, so a result the
                // runner already received there arrives stamped with the older
                // generation; what identifies the execution is the machine it ran
                // on, not the link that carried the order. Anything else is an
                // answer about a binding that no longer exists, such as a
                // colocated replacement whose work was already failed.
                let binding = self.queue.binding_of(&key);
                let bound_generation = match &binding {
                    Some((bound, bound_generation))
                        if bound == executor_id && *bound_generation >= generation =>
                    {
                        *bound_generation
                    }
                    _ => {
                        // Not this process's to settle: a runner that restarted
                        // holds no binding for work an earlier process granted,
                        // and the executor retains every result until one is
                        // acknowledged and sends it again on each new link.
                        // Adoption is driven only by what the machine declares
                        // like this; the link generation having changed is
                        // never read as the work's death (tenet 1239). Whether
                        // a durable row still claims the batch is decided off
                        // this frame, and nothing is acknowledged until it is.
                        let bound = match binding {
                            Some((bound, bound_generation)) => {
                                format!("it is bound to {bound} at generation {bound_generation}")
                            }
                            None => "no binding in this runner process holds it".to_string(),
                        };
                        log::info!(
                            "result of request {} attempt {} from executor_id={executor_id} generation {generation} is not this process's to settle ({bound}); offering it for adoption",
                            key.0,
                            key.1,
                        );
                        scrub_cell_outcome(&mut outcome);
                        let machine = self.executor_name_for_text(executor_id);
                        self.offer_unbound_result(UnboundResult {
                            executor_id: executor_id.to_string(),
                            machine,
                            request_id: key.0,
                            attempt_id: key.1,
                            outcome,
                        });
                        return false;
                    }
                };
                if bound_generation != generation {
                    log::info!(
                        "executor_id={executor_id} result of request {} attempt {} arrived on replaced generation {generation}; settling it against generation {bound_generation}, which carries the claim",
                        key.0,
                        key.1,
                    );
                }
                let _ = self.settle_runner_contexts_for_request(&key.0, &key.1);
                scrub_cell_outcome(&mut outcome);
                if let CellOutcome::Completed { metadata, .. } = &mut outcome {
                    let canonical = self
                        .lock_connections()
                        .get(executor_id)
                        .filter(|connection| connection.generation == bound_generation)
                        .map(|connection| {
                            (
                                connection.identity.executor_id.clone(),
                                connection.identity.device_id.clone(),
                            )
                        });
                    let Some((canonical_id, device_id)) = canonical else {
                        log::warn!(
                            "discarded the result of request {} attempt {} from executor_id={executor_id} generation {generation}: generation {bound_generation}, which carries the claim, is no longer that machine's link",
                            key.0,
                            key.1,
                        );
                        return false;
                    };
                    metadata.executor_id = canonical_id;
                    metadata.executor_device_id = device_id;
                    metadata.executor_connection_generation = bound_generation;
                }
                let direct = self.is_direct_check_result(&key, executor_id);
                if direct {
                    self.receive_direct_check_result(&key, executor_id);
                }
                self.queue.settle(
                    &key,
                    Some((executor_id, bound_generation)),
                    queue::TerminalState::Completed,
                    outcome,
                    unix_time_ms(),
                );
                if !direct {
                    self.acknowledge_settled_result(executor_id, &key);
                }
                false
            }
            ExecutorMessage::TransfersReauthorized {
                request_id,
                attempt_id,
                transfers,
            } => {
                let waiter = self
                    .reauthorizations
                    .lock()
                    .unwrap()
                    .remove(&(request_id.clone(), attempt_id.clone()));
                match waiter {
                    Some(waiter) => {
                        let _ = waiter.send(transfers);
                    }
                    None => log::warn!(
                        "executor_id={executor_id} reauthorized the transfers of request {request_id} attempt {attempt_id}, which nothing here is waiting for"
                    ),
                }
                false
            }
            // A machine takes responsibility for an entry it was offered. This
            // is the moment work binds; the placement pass waiting on the entry
            // sees the transition and sends the grant.
            // The dispatcher forgets a hold when it sends the release, so the
            // acknowledgement has nothing left to finish.
            ExecutorMessage::HeadroomReleased { .. } => false,
            ExecutorMessage::Claim { entry_id } => {
                match self
                    .queue
                    .claim(&entry_id, executor_id, generation, unix_time_ms())
                {
                    queue::ClaimVerdict::Granted => {
                        self.dispatcher_claimed(&entry_id, executor_id);
                    }
                    // The entry moved on while this claim was in flight. Say so,
                    // so the machine releases the ticket it is holding: nothing
                    // was granted, so there is nothing to cancel.
                    queue::ClaimVerdict::Lost => {
                        let _ = self.send_to(
                            executor_id,
                            generation,
                            ExecutorMessage::Lost { entry_id },
                        );
                    }
                }
                false
            }
            // A truthful refusal. The entry returns to the queue and routes to
            // the next candidate rather than waiting here for room that this
            // machine has just said it does not have.
            ExecutorMessage::Decline {
                entry_id,
                reason,
                detail,
                disk,
            } => {
                self.counters.refusal();
                // The id alone is not what an operator searches for: the
                // colocated machine's id is `colocated`, while every surface
                // calls it `local`. Both are named so either finds the line.
                let executor = self
                    .executor_public_name(executor_id)
                    .unwrap_or_else(|| executor_id.to_string());
                let line = format!(
                    "executor_id={executor_id} executor={executor} declined entry {entry_id} ({reason:?}): {}{}",
                    detail.as_deref().unwrap_or("no detail"),
                    disk.as_ref()
                        .map(|disk| format!("; {}", queue::disk_decline_summary(disk)))
                        .unwrap_or_default()
                );
                // A parked request is re-asked when the machine announces newer
                // room, so the same answer can come back for as long as a hold
                // lasts. The first answer of a kind is the news;
                // its repeats are folded into the entry's recorded decline and
                // logged quietly.
                let recorded = self.queue.decline(
                    &entry_id,
                    executor_id,
                    generation,
                    reason,
                    detail,
                    disk,
                    unix_time_ms(),
                );
                match recorded {
                    queue::DeclineRecord::Recorded { repeat: false } => log::info!("{line}"),
                    queue::DeclineRecord::Recorded { repeat: true } => log::debug!("{line}"),
                    queue::DeclineRecord::Stale => {
                        log::debug!("{line} (the entry had moved on; ignored)")
                    }
                }
                if matches!(recorded, queue::DeclineRecord::Recorded { .. }) {
                    let room_seq = self
                        .lock_connections()
                        .get(executor_id)
                        .filter(|entry| entry.generation == generation)
                        .and_then(|entry| entry.room.as_ref().map(|room| room.seq));
                    if let Some(room_seq) = room_seq {
                        self.queue
                            .note_decline_room(&entry_id, executor_id, room_seq);
                    }
                    self.dispatcher_declined(&entry_id, executor_id);
                }
                false
            }
            ExecutorMessage::ResidencyResponse {
                correlation_id,
                result,
            } => {
                let pending = self
                    .pending_residency
                    .lock()
                    .unwrap()
                    .remove(&correlation_id);
                if let Some(pending) = pending {
                    if pending.executor_id != executor_id || pending.generation != generation {
                        self.pending_residency
                            .lock()
                            .unwrap()
                            .insert(correlation_id, pending);
                        return false;
                    }
                    if pending.granted_entry_id.as_deref().is_some_and(|entry_id| {
                        !matches!(self.queue.state_of(entry_id), Some(queue::EntryState::Claimed {
                            executor_id: ref bound, generation: bound_generation, ..
                        }) if bound == executor_id && bound_generation == generation)
                    }) {
                        return false;
                    }
                    let _ = pending.waiter.send(result);
                }
                false
            }
            ExecutorMessage::MaterializationReadResponse {
                correlation_id,
                result,
            } => {
                let pending = self
                    .pending_materialization_reads
                    .lock()
                    .unwrap()
                    .remove(&correlation_id);
                if let Some(pending) = pending {
                    if pending.executor_id != executor_id || pending.generation != generation {
                        self.pending_materialization_reads
                            .lock()
                            .unwrap()
                            .insert(correlation_id, pending);
                        return false;
                    }
                    let _ = pending.waiter.send(result);
                }
                false
            }
            ExecutorMessage::ResidentProcessEvent { event } => {
                if self.resident_event_is_current(executor_id, generation, &event) {
                    self.end_relay_of_exited_process(executor_id, &event);
                    for event in self.settle_resident_event(executor_id, event) {
                        self.publish_resident_process_event(event);
                    }
                }
                false
            }
            ExecutorMessage::MemoryDiagnosticResponse {
                correlation_id,
                result,
            } => {
                let mut pending = self.pending_memory_diagnostic.lock().unwrap();
                if pending.get(&correlation_id).is_some_and(|request| {
                    executor_id == COLOCATED_EXECUTOR_ID && request.generation == generation
                }) {
                    if let Some(request) = pending.remove(&correlation_id) {
                        let _ = request.waiter.send(result);
                    }
                }
                false
            }
            ExecutorMessage::ProcessTreeAdoptionResponse {
                correlation_id,
                result,
            } => {
                let mut pending = self.pending_tree_adoption.lock().unwrap();
                if pending.get(&correlation_id).is_some_and(|request| {
                    executor_id == COLOCATED_EXECUTOR_ID && request.generation == generation
                }) {
                    if let Some(request) = pending.remove(&correlation_id) {
                        let _ = request.waiter.send(result);
                    }
                }
                false
            }
            ExecutorMessage::RuntimePolicyResponse {
                correlation_id,
                result,
            } => {
                let pending = self.pending_policy.lock().unwrap().remove(&correlation_id);
                if let Some(pending) = pending {
                    if pending.executor_id != executor_id || pending.generation != generation {
                        self.pending_policy
                            .lock()
                            .unwrap()
                            .insert(correlation_id, pending);
                        return false;
                    }
                    let _ = pending.waiter.send(result);
                }
                false
            }
            ExecutorMessage::DrainModeResponse {
                correlation_id,
                result,
            } => {
                let pending = self.pending_drain.lock().unwrap().remove(&correlation_id);
                if let Some(pending) = pending {
                    if pending.executor_id != executor_id || pending.generation != generation {
                        self.pending_drain
                            .lock()
                            .unwrap()
                            .insert(correlation_id, pending);
                        return false;
                    }
                    let _ = pending.waiter.send(result);
                }
                false
            }
            // Solicited responses are composed outside the serialized snapshot
            // publisher and can arrive after a newer account. They cannot
            // replace posted state or establish disappearance.
            ExecutorMessage::SnapshotResponse { .. } => false,
            ExecutorMessage::SnapshotUpdated { snapshot, health } => {
                self.apply_executor_snapshot(executor_id, generation, snapshot, health)
            }
            ExecutorMessage::Heartbeat {
                advertisement,
                health,
                last_update,
                machine,
            } => {
                // Answer every beat with who is still waiting. Pacing the report
                // off the executor's own beat rather than a runner-side timer is
                // what lets both sides size the reap window from one constant.
                self.report_waiting_requests(executor_id, generation);
                self.update_advertisement_and_health(
                    executor_id,
                    generation,
                    advertisement,
                    health,
                    last_update,
                    machine,
                )
            }
            ExecutorMessage::AdvertisementUpdated { advertisement } => {
                self.update_advertisement(executor_id, generation, advertisement)
            }
            // Placement reads the roots; the public fleet reading does not.
            ExecutorMessage::WarmRootsChanged { revision, change } => {
                self.apply_warm_roots(executor_id, generation, revision, change);
                false
            }
            // Health only: the public fleet reading did not change shape, as
            // the CPU snapshot this frame replaced almost never did either.
            ExecutorMessage::MachineSampled { sample } => {
                self.record_machine_sample(executor_id, generation, sample);
                false
            }
            // What the dispatcher places by (cairn/7303).
            ExecutorMessage::Room { seq, room } => {
                self.record_room(executor_id, generation, seq, room);
                false
            }
            ExecutorMessage::InfrastructureDiagnostic { diagnostic } => {
                self.fail_for_executor(executor_id, &diagnostic);
                false
            }
            ExecutorMessage::NodeScratchDeleted { scratch_id } => {
                self.emit_node_scratch_event(NodeScratchEvent::Deleted {
                    executor_id: executor_id.to_owned(),
                    scratch_id,
                });
                false
            }
            ExecutorMessage::DeploymentReport {
                command_id,
                deployment,
                phase,
            } => {
                self.deliver_deployment_report(executor_id, command_id, deployment, phase);
                false
            }
            _ => false,
        }
    }

    /// Every executor-side queue entry this runner still has a live waiter for.
    ///
    /// Liveness is read from the response channels, not inferred: a `oneshot`
    /// sender whose receiver has been dropped is exact evidence that the caller
    /// went away, and coalescing means one request keeps its place while ANY
    /// subscriber is still holding on. Residency acquisitions use the same
    /// queue ownership; a response correlation is not a second liveness source.
    /// The complete set is scoped to the bound executor and generation.
    pub(super) fn waiting_request_ids(&self, executor_id: &str, generation: u64) -> Vec<String> {
        let mut ids = self.queue.waiting_request_ids(executor_id, generation);
        ids.extend(self.adopting.lock().unwrap().iter().cloned());
        ids.extend(
            self.check_claims_for_machine(executor_id)
                .into_iter()
                .map(|(key, _)| key.0),
        );
        ids.sort();
        ids.dedup();
        ids
    }

    /// Tell one executor which of its queue entries still have a live waiter.
    ///
    /// Driven by that executor's own heartbeat and by nothing else. A newly
    /// attached executor needs no report before its first beat: its queue is
    /// reconciled against its first snapshot; claimed work survives a link gap
    /// and is rebound before that snapshot is accepted.
    pub(super) fn report_waiting_requests(&self, executor_id: &str, generation: u64) {
        let request_ids = self.waiting_request_ids(executor_id, generation);
        let _ = self.send_to(
            executor_id,
            generation,
            ExecutorMessage::WaitingRequests { request_ids },
        );
    }

    /// Bound a residency RPC response by its link, not by admission progress.
    pub(super) async fn await_link_bound_response<T>(
        &self,
        mut rx: oneshot::Receiver<T>,
        executor_id: &str,
        generation: u64,
    ) -> LinkBoundResponse<T> {
        let silence_budget = Duration::from_millis(RESIDENCY_RESPONSE_FLOOR_MS);
        let mut watchdog_deadline = Instant::now() + silence_budget;
        let mut last_observed_at = Instant::now();
        loop {
            let notified = self.connection_ready.notified();
            let now = Instant::now();
            if self.residency_link_is_fresh(executor_id, generation) {
                watchdog_deadline += now.saturating_duration_since(last_observed_at);
            }
            last_observed_at = now;
            let remaining = watchdog_deadline.saturating_duration_since(now);
            if remaining.is_zero() {
                return LinkBoundResponse::LinkSilent;
            }
            tokio::select! {
                result = &mut rx => return match result {
                    Ok(value) => LinkBoundResponse::Answered(value),
                    Err(_) => LinkBoundResponse::Dropped,
                },
                _ = tokio::time::sleep(remaining.min(Duration::from_millis(250))) => {}
                _ = notified => {}
            }
        }
    }

    /// Whether a residency operation on this link is still worth waiting for.
    /// A runner that was not listening has not heard the machine give up, so
    /// that is "not yet" rather than a silent link.
    pub(super) fn residency_link_is_fresh(&self, executor_id: &str, generation: u64) -> bool {
        self.lock_connections()
            .get(executor_id)
            .filter(|entry| entry.generation == generation)
            .is_some_and(|entry| link_silence_ms(entry, unix_time_ms()).is_none())
    }

    pub(super) fn request_substrate_hold(
        &self,
        executor_id: &str,
        generation: u64,
        request_id: &str,
        attempt_id: &str,
    ) -> Option<ExecutorSubstrateEvidence> {
        let connections = self.lock_connections();
        let entry = connections
            .get(executor_id)
            .filter(|entry| entry.generation == generation)?;
        if let Some(execution) = entry.snapshot.executing_requests.iter().find(|execution| {
            execution.request_id == request_id && execution.attempt_id == attempt_id
        }) {
            // A live child process is a kernel fact, not inferred progress. Once
            // execution begins, acquisition deadlines no longer govern the waiter.
            return Some(ExecutorSubstrateEvidence::without_queue(
                ExecutorSubstrateState::ExecutionRunning,
                execution.started_at_unix_ms,
                entry.last_progress_unix_ms,
            ));
        }
        if let Some(evidence) = entry
            .snapshot
            .in_flight_residency_operations
            .get(request_id)
        {
            return Some(evidence.clone());
        }
        if link_silence_ms(entry, unix_time_ms()).is_some() {
            return None;
        }
        // Executor substrate state is level-reported, so late and concurrent
        // waiters share the executor's epoch rather than inventing request epochs.
        entry.snapshot.substrate_state.clone().or_else(|| {
            entry
                .snapshot
                .queued_requests
                .iter()
                .find(|queued| queued.request_id == request_id)
                .map(|queued| {
                    queued.substrate_hold.clone().unwrap_or_else(|| {
                        let queue_position = entry
                            .snapshot
                            .queued_requests
                            .iter()
                            .position(|candidate| candidate.request_id == request_id)
                            .map(|position| position + 1);
                        ExecutorSubstrateEvidence {
                            state: ExecutorSubstrateState::CapacityBusy,
                            since_unix_ms: queued.queued_at_unix_ms,
                            last_progress_unix_ms: entry.last_progress_unix_ms,
                            diagnostic: Some(
                                "executor reported queue membership before its admission reason"
                                    .into(),
                            ),
                            queue_depth: Some(entry.snapshot.queued_requests.len()),
                            queue_position,
                            active_cell_count: Some(entry.snapshot.executing_requests.len()),
                            oldest_running_started_at_unix_ms: entry
                                .snapshot
                                .executing_requests
                                .iter()
                                .map(|request| request.started_at_unix_ms)
                                .min(),
                        }
                    })
                })
        })
    }

    pub(super) fn executor_deadline_evidence(
        &self,
        executor_id: &str,
        generation: u64,
    ) -> ExecutorSubstrateEvidence {
        let entry = self
            .lock_connections()
            .get(executor_id)
            .filter(|entry| entry.generation == generation)
            .map(|entry| {
                (
                    entry.snapshot.substrate_state.clone(),
                    entry.last_progress_unix_ms,
                )
            });
        let Some((reported, last_progress_unix_ms)) = entry else {
            let now = unix_time_ms();
            return ExecutorSubstrateEvidence::without_queue(
                ExecutorSubstrateState::ConnectedStalled,
                now,
                now,
            );
        };
        let evidence = reported.unwrap_or_else(|| {
            ExecutorSubstrateEvidence::without_queue(
                ExecutorSubstrateState::ConnectedStalled,
                last_progress_unix_ms,
                last_progress_unix_ms,
            )
        });
        deadline_evidence(unix_time_ms(), last_progress_unix_ms, evidence)
    }

    pub(super) fn coalesced_leader(&self, identity: &RequestIdentity) -> Option<RequestIdentity> {
        let registry = self.in_flight.lock().unwrap();
        let key = registry.subscriber_keys.get(identity)?;
        registry
            .by_key
            .contains_key(key)
            .then(|| key.leader.clone())
    }

    /// Where the execution behind one pure-verdict check subscriber stands:
    /// preparing, placing (with each machine's latest decline), or running on
    /// a named machine since its claim, and whose run it is when it coalesced
    /// onto another request's.
    ///
    /// `subscriber` is the identity [`Self::check_subscriber`] mints for a
    /// check request. `None` when no execution
    /// is known for it, or the one it had is between leaving the queue and
    /// settling; a caller treats that as nothing new to say.
    ///
    /// A pure read. The in-flight registry is released before the queue and
    /// the preparation table are read, so it never holds two of those locks.
    pub(crate) fn check_dispatch_phase(
        &self,
        subscriber: &RequestIdentity,
    ) -> Option<CheckDispatch> {
        let leader = self.coalesced_leader(subscriber)?;
        let own = subscriber
            .0
            .strip_suffix(CHECK_SUBSCRIBER_SUFFIX)
            .is_some_and(|request_id| request_id == leader.0);
        let (phase, owner) = self.request_dispatch_state(&leader)?;
        Some(CheckDispatch {
            phase,
            joined: (!own).then(|| owner.unwrap_or_else(|| "another request".to_string())),
        })
    }

    /// Where one request's own execution stands, read by the identity it was
    /// queued under: `(request_id, attempt_id)`. A write-check batch is never
    /// coalesced, so it is read here directly rather than through a subscriber.
    /// `None` while it is being prepared outside the preparation table, or once
    /// it has left the queue.
    pub(crate) fn request_dispatch_phase(
        &self,
        identity: &RequestIdentity,
    ) -> Option<CheckDispatchPhase> {
        self.request_dispatch_state(identity)
            .map(|(phase, _)| phase)
    }

    /// Why a request is still queued, from the queue entry that holds it
    /// (cairn/6499): each machine's latest refusal in its own words with the
    /// room it came under, what the latest placement pass did with each
    /// machine it named, and the turns it waits behind. Answers for any queued
    /// request, pinned or free to move. This is the one account of a queued
    /// request: the check board, and through it a check request's durable
    /// row, are written from this same phase. `None` once a machine has
    /// claimed it or it has left the queue.
    pub(crate) fn queued_request_account(&self, request_id: &str) -> Option<CheckDispatchPhase> {
        let identity = self.queue.attempt_identity_of(request_id)?;
        match self.request_dispatch_phase(&identity)? {
            phase @ CheckDispatchPhase::Placing { .. } => Some(phase),
            _ => None,
        }
    }

    /// The phase of the execution queued as `identity`, with the owner its
    /// request names.
    pub(super) fn request_dispatch_state(
        &self,
        identity: &RequestIdentity,
    ) -> Option<(CheckDispatchPhase, Option<String>)> {
        let queued = self.queue.with_entry_by_identity(identity, |entry| {
            (
                entry.state.clone(),
                entry.queued_at_unix_ms(),
                (entry.id.clone(), entry.declined.clone()),
                entry
                    .request
                    .owner
                    .as_ref()
                    .and_then(occupancy::describe_owner),
            )
        });
        let (phase, owner) = match queued {
            Some((
                queue::EntryState::Claimed {
                    executor_id,
                    at_unix_ms,
                    ..
                },
                _,
                _,
                owner,
            )) => (
                CheckDispatchPhase::Running {
                    executor_name: self.executor_name_for_text(&executor_id),
                    since_unix_ms: at_unix_ms,
                },
                owner,
            ),
            Some((state, since_unix_ms, (entry_id, raw_declines), owner)) => {
                let now = unix_time_ms();
                let placement =
                    self.placement_account(&entry_id, &raw_declines, state.executor_id(), now);
                let takes_room_on = self.room_taken_first_on(identity, now);
                let turn =
                    (!takes_room_on.is_empty()).then(|| describe_room_taken_first(&takes_room_on));
                let mut declines: Vec<_> = raw_declines
                    .into_iter()
                    .map(|(executor_id, decline)| {
                        CheckDispatchDecline::from_entry(
                            &self.executor_name_for_text(&executor_id),
                            &decline,
                        )
                    })
                    .collect();
                declines.sort_by(|a, b| {
                    b.at_unix_ms
                        .cmp(&a.at_unix_ms)
                        .then_with(|| a.executor_name.cmp(&b.executor_name))
                });
                (
                    CheckDispatchPhase::Placing {
                        since_unix_ms,
                        declines,
                        placement,
                        turn,
                    },
                    owner,
                )
            }
            None => {
                let preparing = self
                    .preparing_leaders
                    .lock()
                    .unwrap()
                    .get(identity)
                    .copied()?;
                (
                    CheckDispatchPhase::Preparing {
                        since_unix_ms: preparing.since_unix_ms,
                    },
                    None,
                )
            }
        };
        Some((phase, owner))
    }

    pub(super) fn leader_substrate_hold(
        &self,
        identity: &RequestIdentity,
    ) -> Option<ExecutorSubstrateEvidence> {
        let leader = self.coalesced_leader(identity)?;
        let owner = self.queue.binding_of(&leader);
        if let Some((executor_id, generation)) = owner {
            if let Some(hold) =
                self.request_substrate_hold(&executor_id, generation, &leader.0, &leader.1)
            {
                return Some(hold);
            }
        }
        if let Some(preparing) = self
            .preparing_leaders
            .lock()
            .unwrap()
            .get(&leader)
            .copied()
            .filter(|preparing| {
                unix_time_ms().saturating_sub(preparing.last_progress_unix_ms)
                    <= EXECUTOR_PROGRESS_FRESHNESS_MS
            })
        {
            return Some(ExecutorSubstrateEvidence::without_queue(
                ExecutorSubstrateState::DispatchPreparing,
                preparing.since_unix_ms,
                preparing.last_progress_unix_ms,
            ));
        }
        self.colocated_substrate().filter(|evidence| {
            unix_time_ms().saturating_sub(evidence.last_progress_unix_ms)
                <= EXECUTOR_PROGRESS_FRESHNESS_MS
        })
    }

    pub(super) fn leader_deadline_evidence(
        &self,
        identity: &RequestIdentity,
    ) -> ExecutorSubstrateEvidence {
        let Some(leader) = self.coalesced_leader(identity) else {
            let now = unix_time_ms();
            return ExecutorSubstrateEvidence::without_queue(
                ExecutorSubstrateState::ConnectedStalled,
                now,
                now,
            );
        };
        let owner = self.queue.binding_of(&leader);
        let Some((executor_id, generation)) = owner else {
            return self.colocated_substrate().map_or_else(
                || {
                    let now = unix_time_ms();
                    ExecutorSubstrateEvidence::without_queue(
                        ExecutorSubstrateState::ConnectedStalled,
                        now,
                        now,
                    )
                },
                |evidence| {
                    deadline_evidence(unix_time_ms(), evidence.last_progress_unix_ms, evidence)
                },
            );
        };
        let connections = self.lock_connections();
        let Some(entry) = connections
            .get(&executor_id)
            .filter(|entry| entry.generation == generation)
        else {
            drop(connections);
            return self.executor_deadline_evidence(&executor_id, generation);
        };
        let queued = entry
            .snapshot
            .queued_requests
            .iter()
            .find(|queued| queued.request_id == leader.0);
        let evidence = queued
            .and_then(|queued| queued.substrate_hold.clone())
            .unwrap_or_else(|| {
                let queue_position = entry
                    .snapshot
                    .queued_requests
                    .iter()
                    .position(|queued| queued.request_id == leader.0)
                    .map(|position| position + 1);
                let oldest_running_started_at_unix_ms = entry
                    .snapshot
                    .executing_requests
                    .iter()
                    .map(|request| request.started_at_unix_ms)
                    .min();
                ExecutorSubstrateEvidence {
                    state: ExecutorSubstrateState::CapacityBusy,
                    since_unix_ms: oldest_running_started_at_unix_ms
                        .or_else(|| queued.map(|queued| queued.queued_at_unix_ms))
                        .unwrap_or(entry.last_progress_unix_ms),
                    last_progress_unix_ms: entry.last_progress_unix_ms,
                    diagnostic: None,
                    queue_depth: Some(entry.snapshot.queued_requests.len()),
                    queue_position,
                    active_cell_count: Some(entry.snapshot.executing_requests.len()),
                    oldest_running_started_at_unix_ms,
                }
            });
        deadline_evidence(unix_time_ms(), entry.last_progress_unix_ms, evidence)
    }

    pub(crate) fn record_cached_completion(
        &self,
        project_id: &str,
        job_id: &str,
        executor_id: Option<&str>,
        command: &str,
        priority: CellPriority,
        passed: bool,
    ) {
        let served_at_unix_ms = unix_time_ms();
        let mut recent = self.recent_cached_completions.lock().unwrap();
        recent.push_front(cairn_common::executor_protocol::CellCompletion {
            executor_id: executor_id.unwrap_or("cache").to_string(),
            request_id: format!("cache:{}", uuid::Uuid::new_v4()),
            attempt_id: "cached".into(),
            owner: Some(cairn_common::executor_protocol::CellOwnerRef {
                project_id: project_id.to_string(),
                project_key: None,
                issue_number: None,
                job_id: Some(job_id.to_string()),
                execution_seq: None,
                node_kind: None,
            }),
            command_class: cairn_common::executor_protocol::CellCommandClass::classify(command),
            command: command.to_string(),
            priority,
            queued_at_unix_ms: served_at_unix_ms,
            started_at_unix_ms: Some(served_at_unix_ms),
            finished_at_unix_ms: served_at_unix_ms,
            duration_ms: 0,
            verdict: if passed {
                cairn_common::executor_protocol::CellCompletionVerdict::Succeeded
            } else {
                cairn_common::executor_protocol::CellCompletionVerdict::Failed
            },
            failure_output_tail: None,
            transcript_event_uri: None,
            resource_reservation: None,
            learned_estimate: None,
            actuals: None,
            cached: true,
            subscriber_count: 1,
            served_at_unix_ms,
            result_evicted: None,
        });
        recent.truncate(32);
    }

    /// Retract interest in one dispatched request, and report what that did.
    ///
    /// This is what the operator's ✗ reaches, and what it may honestly claim
    /// depends on a fact only the model holds: whether anyone else still wants
    /// the same answer. Check work coalesces, so one execution routinely serves
    /// several requesters, and a click on one of their rows releases that
    /// requester without stopping anything. Returning a bare `true` for that
    /// case reported success for an action that changed nothing an operator
    /// could see — the row stayed, fed by executor occupancy, because the work
    /// was still running for everyone else.
    /// What the machines' own reports say about a request this runner may no
    /// longer hold a subscriber for (one placed before a restart).
    ///
    /// Only a machine that has delivered at least one snapshot on its current
    /// link has said anything. Attachment alone proves nothing; the snapshot is
    /// the machine's account of the attempts it was granted and has not
    /// finished, and an attempt absent from it and from its recent completions
    /// did not survive the process that ran it (tenet 308). So a reporting
    /// machine that holds or lists the request decides it, whoever else is
    /// away.
    ///
    /// Absence is judged against `holder`, the machine durable evidence says
    /// claimed it, when one is known: that machine's report is the whole
    /// question, and another machine being away is irrelevant. A holder that
    /// is no longer enrolled can never report again, which is evidence too.
    /// With no known holder every machine that could hold it must have
    /// reported: every enrolled one attached, the colocated substrate not
    /// restoring, and every attached one past its first snapshot.
    pub(crate) fn request_report(&self, request_id: &str, holder: Option<&str>) -> RequestReport {
        let restoring = matches!(
            self.link_restoration(),
            placement::LinkRestoration::Restoring
        );
        let away: Vec<(String, String)> = self
            .unattached_enrolled_remotes()
            .into_iter()
            .map(|remote| (remote.executor_id, remote.name))
            .collect();
        let holder_enrolled = holder.is_some_and(|holder| {
            holder == COLOCATED_EXECUTOR_ID || self.is_enrolled_remote(holder)
        });
        let connections = self.lock_connections();
        let reported = |entry: &ExecutorConnectionState| {
            entry.snapshots_received >= 1 && !(entry.colocated && restoring)
        };
        let mut completed = None;
        for (executor_id, connection) in connections.iter().filter(|(_, entry)| reported(entry)) {
            let snapshot = &connection.snapshot;
            let held = snapshot
                .granted_attempts
                .iter()
                .any(|attempt| attempt.request_id == request_id)
                || snapshot
                    .executing_requests
                    .iter()
                    .any(|executing| executing.request_id == request_id)
                || snapshot
                    .queued_requests
                    .iter()
                    .any(|queued| queued.request_id == request_id)
                || snapshot.cells.iter().any(|cell| {
                    cell.occupancy
                        .command
                        .as_ref()
                        .is_some_and(|command| command.request_id == request_id)
                });
            if held {
                return RequestReport::Held {
                    executor_id: executor_id.clone(),
                };
            }
            if completed.is_none() {
                completed = snapshot
                    .recent_completions
                    .iter()
                    .find(|completion| completion.request_id == request_id)
                    .cloned();
            }
        }
        if let Some(completion) = completed {
            return RequestReport::Completed(Box::new(completion));
        }
        if let Some(holder) = holder {
            return match connections.get(holder) {
                Some(entry) if reported(entry) => RequestReport::Gone,
                Some(entry) => RequestReport::Unknown {
                    awaiting: vec![executor_public_name(entry)],
                },
                None if !holder_enrolled => RequestReport::Gone,
                None => RequestReport::Unknown {
                    awaiting: vec![away
                        .iter()
                        .find(|(executor_id, _)| executor_id == holder)
                        .map(|(_, name)| name.clone())
                        .unwrap_or_else(|| {
                            if holder == COLOCATED_EXECUTOR_ID {
                                LOCAL_EXECUTOR_NAME.to_string()
                            } else {
                                holder.to_string()
                            }
                        })],
                },
            };
        }
        let mut awaiting: Vec<String> = away.into_iter().map(|(_, name)| name).collect();
        if restoring && !connections.contains_key(COLOCATED_EXECUTOR_ID) {
            awaiting.push(LOCAL_EXECUTOR_NAME.to_string());
        }
        awaiting.extend(
            connections
                .values()
                .filter(|entry| !reported(entry))
                .map(executor_public_name),
        );
        // No machine attached at all is not every machine having reported;
        // it names no one because none has said anything.
        if awaiting.is_empty() && !connections.is_empty() {
            RequestReport::Gone
        } else {
            RequestReport::Unknown { awaiting }
        }
    }

    /// Tell every reporting machine whose snapshot lists a granted attempt of
    /// `request_id` to cancel it, for a request this runner holds no binding
    /// for (one an earlier runner placed). Best effort; how many were told.
    pub(crate) fn cancel_reported_attempts(&self, request_id: &str) -> usize {
        let granted: Vec<(String, u64, String)> = self
            .lock_connections()
            .iter()
            .flat_map(|(executor_id, entry)| {
                entry
                    .snapshot
                    .granted_attempts
                    .iter()
                    .filter(|attempt| attempt.request_id == request_id)
                    .map(|attempt| {
                        (
                            executor_id.clone(),
                            entry.generation,
                            attempt.attempt_id.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        granted
            .into_iter()
            .filter(|(executor_id, generation, attempt_id)| {
                self.send_to(
                    executor_id,
                    *generation,
                    ExecutorMessage::Cancel {
                        request_id: request_id.to_string(),
                        attempt_id: attempt_id.clone(),
                    },
                )
                .is_ok()
            })
            .count()
    }

    /// Whether this runner's registry holds a live subscriber for the request.
    ///
    /// A per-incarnation answer: right after a boot the registry is empty while
    /// remote batches are still running, so a miss is evidence of death only
    /// for a request this incarnation placed, or once its machine has
    /// reattached without rebinding it (tenets 308, 309 and 311).
    pub(crate) fn is_request_in_flight(&self, request_id: &str) -> bool {
        self.in_flight
            .lock()
            .unwrap()
            .subscriber_keys
            .keys()
            .any(|(id, _)| id == request_id)
    }

    /// Whether any attempt of `request_id` was retired (told to cancel).
    #[cfg(test)]
    pub(crate) fn was_retired_for_test(&self, request_id: &str) -> bool {
        self.cancelled_leaders
            .lock()
            .unwrap()
            .iter()
            .any(|(id, _)| id == request_id)
    }

    pub(crate) fn cancel_request(&self, request_id: &str) -> CancelOutcome {
        let subscriber = self
            .in_flight
            .lock()
            .unwrap()
            .subscriber_keys
            .keys()
            .find(|(id, _)| id == request_id)
            .cloned();
        if let Some(identity) = subscriber {
            return match self
                .detach_coalesced_subscriber(&identity, Some(CancelCause::OperatorRetracted))
            {
                Detachment::Retired { leader } => {
                    // Retiring, not retired: the key stays owed until this
                    // leader's terminal outcome lands, so a fresh answer to the
                    // same question waits for the death rather than starting
                    // beside it.
                    self.purposes
                        .lock()
                        .unwrap()
                        .retire(&leader, unix_time_ms());
                    CancelOutcome::Stopping
                }
                Detachment::Released { remaining } => CancelOutcome::Released { remaining },
                Detachment::Unknown => CancelOutcome::Unknown,
            };
        }
        let Some(identity) = self.queue.attempt_identity_of(request_id) else {
            return CancelOutcome::Unknown;
        };
        self.purposes
            .lock()
            .unwrap()
            .retire(&identity, unix_time_ms());
        // An executor built from this tree binds the request identity when it
        // CLAIMS an offer, on its message loop and before anything is spawned,
        // so a Cancel naming it is found whatever phase it has reached.
        //
        // The honest word is "stopping" whether or not the Cancel could be
        // delivered. A delivered instruction is not a stopped process, and an
        // undelivered one leaves the work's question owing a death this click
        // just recorded; either way the row stays until evidence says the work
        // ended. Reporting "unknown" over a debt the runner now holds would
        // hide the one thing the operator needs to know.
        self.retire_item(&identity, CancelCause::OperatorRetracted);
        CancelOutcome::Stopping
    }

    /// Deliver a leader's settlement to everyone waiting on the execution `key`
    /// names. The key carries the leader, so a completion can only ever settle
    /// the execution its own leader opened, never a later one for the same
    /// content.
    /// Run a coalesced leader to its terminal outcome on the runtime that hosts
    /// this fleet, so that it ends only by settling, never because the caller
    /// that opened it went away. See [`Fleet::host_runtime`].
    pub(super) fn spawn_leader(
        &self,
        leader: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        match &self.host_runtime {
            Some(runtime) => {
                runtime.spawn(leader);
            }
            None => {
                tokio::spawn(leader);
            }
        }
    }

    /// The public name of the machine whose last report showed `leader`
    /// executing, if any machine's does.
    pub(super) fn machine_last_running(&self, leader: &RequestIdentity) -> Option<String> {
        self.lock_connections()
            .values()
            .find(|entry| {
                entry.snapshot.executing_requests.iter().any(|execution| {
                    execution.request_id == leader.0 && execution.attempt_id == leader.1
                })
            })
            .map(executor_public_name)
    }

    /// Deliver `settlement` to every subscriber of `key`.
    ///
    /// Reached from the leader's completion guard during an unwind, so its locks
    /// recover from poison rather than panicking a second time.
    pub(super) fn complete_coalesced_for_leader(
        &self,
        key: &CoalescingKey,
        settlement: impl Into<LeaderSettlement>,
    ) -> bool {
        let settlement = settlement.into();
        let Some(execution) = self.take_coalesced(key) else {
            return false;
        };
        let drained = self.release_drained_leader(&key.leader);
        self.send_coalesced(execution, &settlement);
        if drained {
            self.leader_settled(&key.leader, &settlement);
        }
        true
    }

    /// Deliver `settlement` to every subscriber of `key`, and nothing else: the
    /// leader's own ending is [`Self::end_coalesced_leader_if_drained`]'s. False
    /// when the key was already delivered, so no subscriber is answered twice.
    fn deliver_coalesced(&self, key: &CoalescingKey, settlement: &LeaderSettlement) -> bool {
        let Some(execution) = self.take_coalesced(key) else {
            return false;
        };
        self.send_coalesced(execution, settlement);
        true
    }

    /// Take `key`'s execution out of the registry, so exactly one settlement
    /// ever answers its subscribers.
    fn take_coalesced(&self, key: &CoalescingKey) -> Option<InFlightExecution> {
        let mut registry = lock_through_poison(&self.in_flight);
        let execution = registry.by_key.remove(key)?;
        for identity in execution.subscribers.keys() {
            registry.subscriber_keys.remove(identity);
        }
        Some(execution)
    }

    fn send_coalesced(&self, execution: InFlightExecution, settlement: &LeaderSettlement) {
        for (identity, subscriber) in execution.subscribers {
            let delivery = match settlement {
                LeaderSettlement::Settled { outcome, account } => Ok(CoalescedCellOutcome {
                    outcome: restamp_outcome(outcome, &identity),
                    publication: PublicationCoordination {
                        state: execution.publication.state.clone(),
                        consumer: self.take_check_consumer_guard(&identity),
                    },
                    account: account.clone(),
                }),
                LeaderSettlement::Vanished => {
                    drop(self.take_check_consumer_guard(&identity));
                    Err(CoalescedWaitError::LeaderVanished)
                }
            };
            let _ = subscriber.waiter.send(delivery);
        }
    }

    /// Forget `leader` as a coalesced leader once none of its keys is left to
    /// deliver, and say whether that was so.
    fn release_drained_leader(&self, leader: &RequestIdentity) -> bool {
        let leader_still_active = lock_through_poison(&self.in_flight)
            .by_key
            .keys()
            .any(|candidate| candidate.leader == *leader);
        if !leader_still_active {
            lock_through_poison(&self.coalesced_leaders).remove(leader);
        }
        !leader_still_active
    }

    fn leader_settled(&self, leader: &RequestIdentity, settlement: &LeaderSettlement) {
        if matches!(settlement, LeaderSettlement::Settled { account, .. } if account.left_running.is_none())
        {
            self.check_leader_completed(leader);
        }
    }

    /// End `leader` once none of its keys is left to deliver, for a leader
    /// whose last keys were settled early ([`ItemSettlements`]) and so never
    /// reached [`Self::complete_coalesced_for_leader`]. Its durable check claim
    /// settles here, so this follows the leader's own terminal outcome.
    fn end_coalesced_leader_if_drained(
        &self,
        leader: &RequestIdentity,
        settlement: &LeaderSettlement,
    ) {
        if self.release_drained_leader(leader) {
            self.leader_settled(leader, settlement);
        }
    }

    /// Settle one finished item of a pure-verdict check batch from the
    /// executor's report of it, before the batch returns.
    ///
    /// The item is described exactly as the batch's result will describe it:
    /// the executor sends the metadata already narrowed to the item, and the
    /// machine's identity is stamped from the claim's link as the result path
    /// stamps it. A report the runner cannot place against a live claim is left
    /// for the batch's result, which settles every item no report reached.
    fn settle_item_early(
        &self,
        settlements: &ItemSettlements,
        stream_id: &str,
        result: cairn_common::executor_protocol::ProcessItemResult,
    ) {
        let Some(key) = settlements.keys.get(stream_id) else {
            return;
        };
        let leader = &settlements.leader;
        let Some((executor_id, generation)) = self.queue.binding_of(leader) else {
            return;
        };
        let Some((canonical_id, device_id)) = self
            .lock_connections()
            .get(&executor_id)
            .filter(|connection| connection.generation == generation)
            .map(|connection| {
                (
                    connection.identity.executor_id.clone(),
                    connection.identity.device_id.clone(),
                )
            })
        else {
            return;
        };
        let cairn_common::executor_protocol::ProcessItemResult {
            outcome: item,
            mut metadata,
        } = result;
        metadata.executor_id = canonical_id;
        metadata.executor_device_id = device_id;
        metadata.executor_connection_generation = generation;
        let mut outcome = CellOutcome::Completed {
            request_id: leader.0.clone(),
            attempt_id: leader.1.clone(),
            exit_code: item.exit_code,
            output: item.body,
            timed_out: item.timed_out,
            metadata,
            mutation_delta: None,
            sandbox_denials: item.sandbox_denials,
            tracked_modifications: item.tracked_modifications,
            node_scratch: None,
        };
        scrub_cell_outcome(&mut outcome);
        let account = CellAccount {
            cancel_cause: None,
            placement: Some(CellPlacement {
                name: self.executor_name_for_text(&executor_id),
                platform: self.executor_platform(&executor_id),
                executor_id,
                generation,
            }),
            left_running: None,
        };
        // Recorded before delivery, so the batch's result, however it races
        // this report, finds the key either still open or already answered.
        lock_through_poison(&settlements.settled).insert(key.clone(), outcome.clone());
        if !self.deliver_coalesced(key, &LeaderSettlement::Settled { outcome, account }) {
            lock_through_poison(&settlements.settled).remove(key);
        }
    }

    /// Release one subscriber's interest in a coalesced execution.
    ///
    /// `on_behalf` is the cause when someone other than the subscriber's own
    /// wait is ending it — a job stop, a session closing, an operator. That
    /// subscriber's waiter is still listening, so it is told why before its
    /// entry goes; a removed sender with nothing sent would read as the result
    /// channel failing. `None` is the waiter detaching itself, with nobody left
    /// to tell.
    pub(super) fn detach_coalesced_subscriber(
        &self,
        identity: &RequestIdentity,
        on_behalf: Option<CancelCause>,
    ) -> Detachment {
        drop(self.take_check_consumer_guard(identity));
        let remaining;
        let removed;
        let leader_to_cancel = {
            let mut registry = self.in_flight.lock().unwrap();
            let Some(key) = registry.subscriber_keys.remove(identity) else {
                return Detachment::Unknown;
            };
            let Some(execution) = registry.by_key.get_mut(&key) else {
                return Detachment::Unknown;
            };
            removed = execution.subscribers.remove(identity);
            remaining = execution.subscribers.len();
            if execution.subscribers.is_empty() {
                let leader = key.leader.clone();
                // Keep the empty execution as a tombstone until the leader publishes its
                // terminal outcome. Cancellation is asynchronous at the executor, so removing
                // the result key here would let an immediate cadence retry become a second
                // leader while the first command is still queued, running, or recovering.
                let group_still_consumed = registry.by_key.iter().any(|(candidate, execution)| {
                    candidate.leader == leader && !execution.subscribers.is_empty()
                });
                if group_still_consumed {
                    None
                } else {
                    self.cancelled_leaders
                        .lock()
                        .unwrap()
                        .insert(leader.clone());
                    Some(leader)
                }
            } else {
                None
            }
        };
        if let (Some(cause), Some(subscriber)) = (on_behalf.clone(), removed) {
            let _ = subscriber
                .waiter
                .send(Err(CoalescedWaitError::Cancelled(cause)));
        }
        let Some(leader) = leader_to_cancel else {
            return Detachment::Released { remaining };
        };
        if self.runner_stop.at().is_some() && self.adoption_owner(&leader).is_some() {
            self.cancelled_leaders.lock().unwrap().remove(&leader);
            return Detachment::Released { remaining };
        }
        // A leader nobody waits on any more ends now if it was never granted,
        // rather than whenever a machine happens to claim it.
        self.retirements.notify_waiters();
        // The execution stays coalescible until its terminal outcome, so a
        // follower can still join it after this. Recorded against the leader,
        // the cause reaches that follower with the cancelled outcome.
        if let Some(cause) = on_behalf {
            self.cancel_causes
                .lock()
                .unwrap()
                .record(leader.clone(), cause);
        }
        let owner = self
            .queue
            .binding_of(&leader)
            .map(|(executor_id, generation)| (executor_id, generation, leader.clone()));
        if let Some((executor_id, generation, (request_id, attempt_id))) = owner {
            let _ = self.send_to(
                &executor_id,
                generation,
                ExecutorMessage::Cancel {
                    request_id,
                    attempt_id,
                },
            );
        }
        Detachment::Retired { leader }
    }

    /// Cancel everything one job is waiting on that `cause` ends
    /// ([`CancelCause::ends_job_work_at`]).
    ///
    /// Job-wide on purpose: every consumer in the job the cause covers is
    /// ended, so only a caller ending the whole job (or, for a compaction, the
    /// whole session) may call it. A caller abandoning its own requests drops
    /// the futures awaiting them instead, and leaves the job's other consumers
    /// their answers.
    pub(crate) fn cancel_job_requests(&self, job_id: &str, cause: CancelCause) -> usize {
        let subscribers: Vec<_> = self
            .in_flight
            .lock()
            .unwrap()
            .by_key
            .values()
            .flat_map(|execution| execution.subscribers.iter())
            .filter(|(_, subscriber)| {
                subscriber.requesting_job_id.as_deref() == Some(job_id)
                    && cause.ends_job_work_at(subscriber.priority)
            })
            .map(|(identity, _)| identity.clone())
            .collect();
        let subscriber_count = subscribers.len();
        for identity in subscribers {
            self.detach_coalesced_subscriber(&identity, Some(cause.clone()));
        }

        let coalesced_leaders = self.coalesced_leaders.lock().unwrap().clone();
        let pending: Vec<_> = self
            .queue
            .bindings_for_job(job_id, |priority| cause.ends_job_work_at(priority))
            .into_iter()
            .filter(|(identity, _, _)| !coalesced_leaders.contains(identity))
            .map(|((request_id, attempt_id), executor_id, generation)| {
                (request_id, attempt_id, executor_id, generation)
            })
            .collect();
        subscriber_count
            + pending
                .into_iter()
                .filter(|(request_id, attempt_id, executor_id, generation)| {
                    self.cancel_causes
                        .lock()
                        .unwrap()
                        .record((request_id.clone(), attempt_id.clone()), cause.clone());
                    self.send_to(
                        executor_id,
                        *generation,
                        ExecutorMessage::Cancel {
                            request_id: request_id.clone(),
                            attempt_id: attempt_id.clone(),
                        },
                    )
                    .is_ok()
                })
                .count()
    }

    pub async fn submit(&self, orch: &Orchestrator, request: CellRequest) -> CellOutcome {
        self.submit_execution(orch, request, None).await
    }

    #[allow(clippy::result_large_err)]
    pub(crate) async fn submit_pure_verdict(
        &self,
        orch: &Orchestrator,
        result_identity: CheckResultIdentity,
        mut request: CellRequest,
    ) -> CoalescedDelivery {
        if request.mutation_policy != MutationPolicy::PureVerdict {
            return Err(CoalescedWaitError::refused(CellOutcome::Unavailable {
                reason: CellUnavailableReason::ExecutorUnavailable,
                diagnostic: "coalesced submission requires pure-verdict mutation policy".into(),
            }));
        }
        let prepared = self
            .prepare_execution(orch, &mut request)
            .await
            .map_err(CoalescedWaitError::refused)?;
        let public_identity = (request.request_id.clone(), request.attempt_id.clone());
        let (adoption_gate, metadata) = {
            let state = self.check_adoptions.lock().unwrap();
            (
                state.mutation.clone(),
                state.templates.get(&public_identity).cloned(),
            )
        };
        let adoption_admission = adoption_gate.lock().await;
        let check_name = result_identity.check_name.clone();
        let wait_horizon_unix_ms = request.wait_horizon_unix_ms;
        let (tx, rx) = oneshot::channel();
        let mut leader_request = None;
        let demand = PlacementDemand::of(&request);
        let admission_work_class = request.placement_work_class;
        let key = {
            let mut registry = self.in_flight.lock().unwrap();
            if registry.subscriber_keys.contains_key(&public_identity) {
                return Err(CoalescedWaitError::refused(executor_unavailable(
                    "duplicate coalesced subscriber identity".into(),
                )));
            }
            let (key, opened) = registry.admit(
                result_identity,
                &demand,
                &public_identity,
                public_identity.clone(),
                CoalescedSubscriber {
                    waiter: tx,
                    priority: request.priority,
                    requesting_job_id: request.requesting_job_id.clone(),
                },
            );
            if opened {
                self.coalesced_leaders
                    .lock()
                    .unwrap()
                    .insert(public_identity.clone());
                leader_request = Some(request);
            }
            key
        };
        let admission_subscriber = CoalescedSubscriberDropGuard {
            pool: self.clone(),
            identity: public_identity.clone(),
            armed: true,
        };
        let mut admission_leader = CoalescedLeaderCompletionGuard {
            pool: self.clone(),
            leader: public_identity.clone(),
            work_class: admission_work_class,
            keys: vec![key.clone()],
            runner_context_id: None,
            armed: leader_request.is_some(),
        };
        if let Some(metadata) = metadata {
            self.track_check_consumer(
                public_identity.clone(),
                key.leader.clone(),
                &check_name,
                &metadata,
            );
            if let Err(error) = self
                .register_check_join_locked(&key.leader, &check_name, &metadata)
                .await
            {
                self.detach_coalesced_subscriber(&public_identity, None);
                if leader_request.is_some() {
                    self.complete_coalesced_for_leader(&key, executor_unavailable(error.clone()));
                }
                self.check_adoptions
                    .lock()
                    .unwrap()
                    .templates
                    .remove(&public_identity);
                return Err(CoalescedWaitError::refused(executor_unavailable(format!(
                    "persist configured check consumer JOIN: {error}"
                ))));
            }
        }
        if leader_request.is_none() {
            self.check_adoptions
                .lock()
                .unwrap()
                .templates
                .remove(&public_identity);
        }
        drop(adoption_admission);
        if let Some(mut request) = leader_request {
            if let Some(priority) =
                self.in_flight
                    .lock()
                    .unwrap()
                    .by_key
                    .get(&key)
                    .and_then(|execution| {
                        execution
                            .subscribers
                            .values()
                            .map(|subscriber| subscriber.priority)
                            .max()
                    })
            {
                // Executor protocol has no queued priority update. Subscribers that arrive
                // after this send inherit the admitted priority until that protocol grows one.
                request.priority = priority;
            }
            let pool = self.clone();
            let completion_guard = CoalescedLeaderCompletionGuard {
                pool: pool.clone(),
                leader: (request.request_id.clone(), request.attempt_id.clone()),
                work_class: request.placement_work_class,
                keys: vec![key.clone()],
                runner_context_id: None,
                armed: true,
            };
            self.spawn_leader(async move {
                let mut completion_guard = completion_guard;
                if !pool.in_flight.lock().unwrap().by_key.contains_key(&key) {
                    let identity = (request.request_id.clone(), request.attempt_id.clone());
                    pool.cancelled_leaders.lock().unwrap().remove(&identity);
                    pool.coalesced_leaders.lock().unwrap().remove(&identity);
                    completion_guard.disarm();
                    return;
                }
                let leader = (request.request_id.clone(), request.attempt_id.clone());
                let executed = pool
                    .execute_prepared_accounted(request, None, prepared, None)
                    .await;
                pool.cancelled_leaders.lock().unwrap().remove(&leader);
                pool.coalesced_leaders.lock().unwrap().remove(&leader);
                // This leader names no question of its own, but a purpose item
                // that joined it may have left the question owing this
                // execution's death.
                pool.settle_leader_purpose(&leader, executed.abandoned_grant.clone());
                pool.complete_coalesced_for_leader(
                    &key,
                    LeaderSettlement::Settled {
                        outcome: executed.outcome,
                        account: executed.account,
                    },
                );
                completion_guard.disarm();
            });
        }
        admission_leader.disarm();
        let delivery = self
            .await_coalesced(public_identity, wait_horizon_unix_ms, rx)
            .await;
        drop(admission_subscriber);
        delivery
    }

    #[allow(clippy::result_large_err)]
    pub(super) async fn await_coalesced(
        &self,
        identity: RequestIdentity,
        wait_horizon_unix_ms: u64,
        mut rx: oneshot::Receiver<CoalescedDelivery>,
    ) -> CoalescedDelivery {
        let mut guard = CoalescedSubscriberDropGuard {
            pool: self.clone(),
            identity: identity.clone(),
            armed: true,
        };
        let mut execution_started = false;
        loop {
            let now = unix_time_ms();
            if self
                .leader_substrate_hold(&identity)
                .is_some_and(|hold| hold.state == ExecutorSubstrateState::ExecutionRunning)
            {
                execution_started = true;
            }
            let remaining = wait_horizon_unix_ms.saturating_sub(now);
            if !execution_started && remaining == 0 {
                let substrate = self.leader_deadline_evidence(&identity);
                self.detach_coalesced_subscriber(&identity, None);
                guard.disarm();
                return Err(CoalescedWaitError::refused(CellOutcome::Unavailable {
                    diagnostic: format!(
                        "cell subscriber deadline elapsed with {:?}; last progress at {}",
                        substrate.state, substrate.last_progress_unix_ms
                    ),
                    reason: CellUnavailableReason::Deadline {
                        host_pressure: None,
                        substrate: Some(substrate),
                    },
                }));
            }
            let wait = if execution_started {
                Duration::from_millis(250)
            } else {
                Duration::from_millis(remaining.clamp(1, 250))
            };
            match tokio::time::timeout(wait, &mut rx).await {
                Ok(Ok(delivery)) => {
                    guard.disarm();
                    return delivery;
                }
                Ok(Err(_)) => {
                    guard.disarm();
                    log::error!(
                        "coalesced result channel for {identity:?} closed with nothing sent; \
                         every path that removes a waiter answers it first, so this is a bug"
                    );
                    return Err(CoalescedWaitError::Orphaned);
                }
                Err(_) => {}
            }
        }
    }

    /// The single `ProcessBatch` shape both check cadences submit, so the
    /// confinement and ordering contract has one home rather than one copy per
    /// cadence. Checks run sequentially and every item runs even after a red, so
    /// one failing check never hides the verdicts behind it.
    /// The envelope a check batch was placed on, taken from the same merged
    /// selector placement evaluated.
    ///
    /// Checks are a primary batch producer, so dropping the envelope here would
    /// leave the admission-to-spawn window open for exactly the workloads the
    /// cadence runs most. A selector field whose main consumer discards its
    /// enforcement half is worse than no field at all.
    pub(super) fn check_batch_containment(
        request: &CellRequest,
    ) -> Option<cairn_common::executor_protocol::WorkloadEnvelope> {
        request
            .executor
            .as_ref()
            .and_then(|selector| selector.containment.clone())
    }

    pub(super) fn check_process_batch(
        mut items: Vec<ProcessBatchItem>,
        runner_context_id: Option<String>,
        containment: Option<cairn_common::executor_protocol::WorkloadEnvelope>,
        silence_bound_ms: u64,
    ) -> ProcessBatch {
        let mut files: Vec<ProcessBatchFile> = Vec::new();
        for item in &mut items {
            // Zero is the executor protocol's unbounded-process sentinel. Check
            // duration estimates inform placement, but elapsed time cannot turn a
            // live check into a failure.
            item.timeout_ms = 0;
            // What can: evidence that the check's tree has stopped making
            // progress. See `WRITE_CHECK_SILENCE_BOUND_MS`.
            item.silence_bound_ms = silence_bound_ms;
            for (content_env, file_env) in crate::execution::checks::CHECK_CONTENT_TRANSPORTS {
                if let Some(index) = item.env.iter().position(|(key, _)| key == content_env) {
                    let (_, contents) = item.env.remove(index);
                    if !files.iter().any(|file| file.env == file_env) {
                        files.push(ProcessBatchFile {
                            env: file_env.to_string(),
                            contents,
                        });
                    }
                }
            }
        }
        ProcessBatch {
            files,
            runtime_packages: Vec::new(),
            sequential: true,
            stop_on_error: false,
            containment,
            sandbox_mode: Self::CHECK_CADENCE_SANDBOX_MODE,
            timeout_bases: Vec::new(),
            items,
            runner_context_id,
            execution_residency: None,
            node_scratch: None,
        }
    }

    /// The identity a check request's verdict is delivered under. The request
    /// itself is the leader of any execution it opens; its subscriber is this,
    /// so [`Self::check_dispatch_phase`] can tell its own run from one it
    /// coalesced onto.
    pub(crate) fn check_subscriber(request: &CellRequest) -> RequestIdentity {
        (
            format!("{}{CHECK_SUBSCRIBER_SUFFIX}", request.request_id),
            format!("{}{CHECK_SUBSCRIBER_SUFFIX}", request.attempt_id),
        )
    }

    /// Ask for one check suite's verdict, coalescing onto an execution already
    /// answering the same question when one exists. A check request is always
    /// one suite: it is placed, priced and settled by itself.
    pub(crate) async fn submit_pure_verdict_check(
        &self,
        orch: &Orchestrator,
        request: CellRequest,
        item: PureVerdictBatchItem,
        run_context: Option<crate::mcp::handlers::RunContext>,
        check_status_board: Option<crate::execution::checks::CheckStatusBoard>,
        purpose: Option<purpose::PurposeKey>,
    ) -> CoalescedDelivery {
        if request.mutation_policy != MutationPolicy::PureVerdict {
            return Err(CoalescedWaitError::refused(executor_unavailable(
                "a check verdict requires the pure-verdict mutation policy".into(),
            )));
        }
        let leader = (request.request_id.clone(), request.attempt_id.clone());
        // The chokepoint. An item that names a purpose holds that purpose alone:
        // admitting it is, in the same locked transition, the retirement of
        // whatever older item was answering the same question, and no phase of
        // the older one is exempt from being displaced.
        //
        // Winning the key is only half of it. The successor then waits for the
        // predecessor's terminal outcome before dispatching, because retirement
        // is something the displaced work does rather than something the record
        // says about it.
        let mut admission_guard = None;
        if let Some(key) = &purpose {
            // Not before the debts a previous runner process left are back:
            // admitting first could dispatch beside one this process has not
            // read yet.
            self.await_purpose_debts_loaded().await;
            let admission =
                self.purposes
                    .lock()
                    .unwrap()
                    .admit(key.clone(), leader.clone(), unix_time_ms());
            match admission {
                purpose::Admission::Admitted => {}
                purpose::Admission::Replaced { retire, phase } => {
                    log::info!(
                        "purpose {key} moves to {leader:?}, retiring {retire:?} from {phase:?}"
                    );
                    self.retire_item(&retire, CancelCause::Superseded);
                }
                purpose::Admission::Superseded { holder } => {
                    // Newest-wins is decided here rather than by which item
                    // reaches an executor first, so a stale one never dispatches
                    // at all and never takes an occupancy row.
                    log::info!(
                        "purpose {key} is already held by {holder:?}; {leader:?} is stale and will not dispatch"
                    );
                    return Err(CoalescedWaitError::Cancelled(CancelCause::Superseded));
                }
            }
            admission_guard = Some(PurposeAdmissionGuard {
                pool: self.clone(),
                item: leader.clone(),
                armed: true,
            });
            // Nothing below this line may run while a displaced answer to the
            // same question is still alive somewhere in the fleet.
            match self.await_purpose_quiescent(key, &leader).await {
                PurposeGate::Clear => {}
                PurposeGate::Displaced => {
                    log::info!(
                        "purpose {key} moved on while {leader:?} waited to dispatch; it never ran"
                    );
                    return Err(CoalescedWaitError::Cancelled(CancelCause::Superseded));
                }
                PurposeGate::Owed { outstanding } => {
                    // Refusing is the whole point. Dispatching here would
                    // knowingly create the overlap this gate exists to prevent,
                    // and a log line is a record rather than a decision anyone
                    // made. A refused wave is visible, is retried by the next
                    // cadence, and leaves the operator a red they can act on;
                    // a silent second attestation leaves them the symptom that
                    // opened this issue.
                    let diagnostic = self.purpose_refusal_diagnostic(key, &outstanding);
                    log::warn!("{diagnostic}");
                    self.purposes.lock().unwrap().finish(&leader);
                    self.purpose_quiesced.notify_waiters();
                    return Err(CoalescedWaitError::refused(executor_unavailable(
                        diagnostic,
                    )));
                }
            }
        }
        let (adoption_gate, joining_metadata) = {
            let state = self.check_adoptions.lock().unwrap();
            (
                state.mutation.clone(),
                state.templates.get(&leader).cloned(),
            )
        };
        // The durable JOIN boundary and the recorder's final acknowledgement
        // share this gate; admitting a join cannot race ahead of its row.
        let adoption_admission = adoption_gate.lock().await;
        let wait_horizon_unix_ms = request.wait_horizon_unix_ms;
        let mut manual_publications = Vec::new();
        let mut keys = Vec::new();
        let demand = PlacementDemand::of(&request);
        let subscriber = Self::check_subscriber(&request);
        let manual_handle = crate::execution::checks::manual_check_handle(&item.process.stream_id)
            .map(str::to_owned);
        let check_name = item.result_identity.check_name.clone();
        let (tx, rx) = oneshot::channel();
        let (physical, newly_claimed) = {
            let mut registry = self.in_flight.lock().unwrap();
            let (key, opened) = registry.admit(
                item.result_identity.clone(),
                &demand,
                &leader,
                subscriber.clone(),
                CoalescedSubscriber {
                    waiter: tx,
                    priority: request.priority,
                    requesting_job_id: request.requesting_job_id.clone(),
                },
            );
            if let Some(handle) = &manual_handle {
                let publication = &registry.by_key[&key].publication;
                manual_publications.push((handle.clone(), publication.id().to_string()));
            }
            let physical = key.leader.clone();
            if opened {
                keys.push(key);
                (physical, Some(item))
            } else {
                if let Some(board) = &check_status_board {
                    // Answered by an execution another leader opened, whose
                    // item reports go to that leader. Say so, rather than
                    // leave the lane reading as queued until it returns.
                    if let Some(index) = check_index_from_stream_id(&item.process.stream_id) {
                        board.transition(index, "shared", None);
                    }
                }
                (physical, None)
            }
        };
        let admission_subscriber = CoalescedSubscriberDropGuard {
            pool: self.clone(),
            identity: subscriber.clone(),
            armed: true,
        };
        let mut admission_leader = CoalescedLeaderCompletionGuard {
            pool: self.clone(),
            leader: leader.clone(),
            work_class: request.placement_work_class,
            keys: keys.clone(),
            runner_context_id: None,
            armed: !keys.is_empty(),
        };
        if let Some(source) = &joining_metadata {
            let mut source = source.clone();
            if source.consumer == "manual" {
                if let Some(handle) = manual_handle {
                    source.consumer_ref = handle;
                }
            }
            self.track_check_consumer(subscriber.clone(), physical.clone(), &check_name, &source);
            if let Err(error) = self
                .register_check_join_locked(&physical, &check_name, &source)
                .await
            {
                let diagnostic = format!("persist configured check consumer JOIN: {error}");
                self.detach_coalesced_subscriber(&subscriber, None);
                for key in &keys {
                    self.complete_coalesced_for_leader(
                        key,
                        executor_unavailable(diagnostic.clone()),
                    );
                }
                self.check_adoptions
                    .lock()
                    .unwrap()
                    .templates
                    .remove(&leader);
                return Err(CoalescedWaitError::refused(executor_unavailable(
                    diagnostic,
                )));
            }
        }
        // Only a newly opened execution runs in this physical grant. A joined
        // check is recorded against its existing leader's claim instead.
        {
            let mut adoptions = self.check_adoptions.lock().unwrap();
            match &newly_claimed {
                None => {
                    adoptions.templates.remove(&leader);
                }
                Some(item) => {
                    if let Some(template) = adoptions.templates.get_mut(&leader) {
                        let position = template
                            .check_names
                            .iter()
                            .zip(&template.input_hashes)
                            .position(|(name, hash)| {
                                name == &item.result_identity.check_name
                                    && hash == &item.result_identity.input_hash
                            });
                        template.recording_plans = position
                            .and_then(|index| template.recording_plans.get(index).cloned())
                            .into_iter()
                            .collect();
                        template.check_names = vec![item.result_identity.check_name.clone()];
                        template.input_hashes = vec![item.result_identity.input_hash.clone()];
                    }
                }
            }
        }
        drop(adoption_admission);
        // Admission records the actual shared execution before this caller can
        // wait for it. The publisher may already be committing its source; recovery
        // joins this identity even if we never receive its direct observation ID.
        if !manual_publications.is_empty() {
            let mut subscriber_guard = CoalescedSubscriberDropGuard {
                pool: self.clone(),
                identity: subscriber.clone(),
                armed: true,
            };
            let mut leader_guard = CoalescedLeaderCompletionGuard {
                pool: self.clone(),
                leader: leader.clone(),
                work_class: request.placement_work_class,
                keys: keys.clone(),
                runner_context_id: None,
                armed: !keys.is_empty(),
            };
            let registration = async {
                let db =
                    crate::execution::routing::owning_db_for_project(&orch.db, &request.project_id)
                        .await
                        .map_err(|error| error.to_string())?;
                register_manual_check_publications(
                    &db,
                    request.project_id.clone(),
                    manual_publications,
                )
                .await
            }
            .await;
            subscriber_guard.disarm();
            leader_guard.disarm();
            if let Err(error) = registration {
                let diagnostic = format!("persist manual check publication: {error}");
                self.detach_coalesced_subscriber(&subscriber, None);
                for key in &keys {
                    self.complete_coalesced_for_leader(
                        key,
                        executor_unavailable(diagnostic.clone()),
                    );
                }
                if !keys.is_empty() {
                    self.cancelled_leaders.lock().unwrap().remove(&leader);
                }
                return Err(CoalescedWaitError::refused(executor_unavailable(
                    diagnostic,
                )));
            }
        }
        // A check that joined an execution another leader opened has no
        // leader of its own. Its subscription is its stake in the fleet, so
        // retiring it has to end it, and that ending is its end.
        if let (true, Some(key)) = (newly_claimed.is_none(), &purpose) {
            self.purpose_joiners
                .lock()
                .unwrap()
                .insert(leader.clone(), (key.clone(), vec![subscriber.clone()]));
            // Displaced between the gate and this registration: the
            // retirement found nothing to detach, so do it here.
            if !self.purposes.lock().unwrap().is_live_holder(key, &leader) {
                self.detach_purpose_joiner(&leader, CancelCause::Superseded);
            }
        }
        if let Some(item) = newly_claimed {
            self.coalesced_leaders
                .lock()
                .unwrap()
                .insert(leader.clone());
            let now = unix_time_ms();
            self.preparing_leaders.lock().unwrap().insert(
                leader.clone(),
                LeaderPreparation {
                    since_unix_ms: now,
                    last_progress_unix_ms: now,
                },
            );
            let coalesced_output = output_guard::RelayedOutput::shared();
            let flushed_context = run_context.clone();
            // The claimed check settles from the executor's report of it as
            // it finishes, so every check names a context to report to. The
            // executor sends its item reports only to a batch that does.
            let item_settlements = Arc::new(ItemSettlements {
                leader: leader.clone(),
                keys: keys
                    .iter()
                    .map(|key| (item.process.stream_id.clone(), key.clone()))
                    .collect(),
                settled: Mutex::new(HashMap::new()),
            });
            let runner_context_id = {
                let id = uuid::Uuid::new_v4().to_string();
                self.runner_contexts.lock().unwrap().insert(
                    id.clone(),
                    RunnerContextEntry::new(
                        &id,
                        &request.request_id,
                        RunnerCallbackContext {
                            request: None,
                            run_context,
                            check_status_board,
                            run_batch_request_id: None,
                            live_checkout: false,
                            executor_binding: None,
                            item_settlements: Some(item_settlements.clone()),
                            output: coalesced_output.clone(),
                        },
                    ),
                );
                Some(id)
            };
            let batch = Self::check_process_batch(
                vec![item.process],
                runner_context_id.clone(),
                Self::check_batch_containment(&request),
                Self::REVIEW_CHECK_SILENCE_BOUND_MS,
            );
            let pool = self.clone();
            let orch = orch.clone();
            let completion_guard = CoalescedLeaderCompletionGuard {
                pool: pool.clone(),
                leader: leader.clone(),
                work_class: request.placement_work_class,
                keys: keys.clone(),
                runner_context_id: runner_context_id.clone(),
                armed: true,
            };
            // From here the leader's own terminal path, or its completion
            // guard, answers for this item's question.
            if let Some(guard) = admission_guard.as_mut() {
                guard.armed = false;
            }
            self.spawn_leader(async move {
                let mut completion_guard = completion_guard;
                let mut request = request;
                let (outcome, account, abandoned_grant) =
                    match pool.prepare_execution(&orch, &mut request).await {
                        Ok(prepared) => {
                            if let Some(preparing) =
                                pool.preparing_leaders.lock().unwrap().get_mut(&leader)
                            {
                                preparing.last_progress_unix_ms = unix_time_ms();
                            }
                            let executed = pool
                                .execute_prepared_accounted(request, Some(batch), prepared, None)
                                .await;
                            (executed.outcome, executed.account, executed.abandoned_grant)
                        }
                        Err(outcome) => (outcome, CellAccount::default(), None),
                    };
                let settled = |outcome: CellOutcome| LeaderSettlement::Settled {
                    outcome,
                    account: account.clone(),
                };
                pool.preparing_leaders.lock().unwrap().remove(&leader);
                if let Some(id) = runner_context_id {
                    output_guard::flush_batch_output(
                        &orch,
                        &coalesced_output,
                        flushed_context.as_ref(),
                    );
                    pool.settle_runner_context(&id);
                }
                pool.cancelled_leaders.lock().unwrap().remove(&leader);
                pool.settle_leader_purpose(&leader, abandoned_grant);
                // An item the executor's report already settled is not answered
                // again: its subscribers have their outcome. The batch's own
                // result for it is only checked against what they were given.
                let mut settled_early = 0_usize;
                let settle_at_end = |key: &CoalescingKey, settlement: LeaderSettlement| {
                    let Some(early) = item_settlements.take(key) else {
                        pool.complete_coalesced_for_leader(key, settlement);
                        return 0;
                    };
                    match &settlement {
                        LeaderSettlement::Settled { outcome: last, .. } => {
                            let differing = verdict_disagreement(&early, last);
                            if !differing.is_empty() {
                                log::warn!(
                                    "check {} of request {} attempt {} was settled from its completion report, and the batch's result disagrees on {}; its subscribers keep the reported outcome",
                                    key.result.check_name,
                                    leader.0,
                                    leader.1,
                                    differing.join(", "),
                                );
                            }
                        }
                        LeaderSettlement::Vanished => {}
                    }
                    1
                };
                match &outcome {
                    CellOutcome::Completed {
                        output,
                        metadata,
                        mutation_delta: None,
                        tracked_modifications: None,
                        ..
                    } => match serde_json::from_str::<
                        Vec<cairn_common::executor_protocol::ProcessBatchItemOutcome>,
                    >(output)
                    {
                        Ok(results) if results.len() == keys.len() => {
                            for (key, result) in keys.iter().zip(results) {
                                let item_meta = metadata.for_batch_item(&result);
                                settled_early += settle_at_end(
                                    key,
                                    settled(CellOutcome::Completed {
                                        request_id: leader.0.clone(),
                                        attempt_id: leader.1.clone(),
                                        exit_code: result.exit_code,
                                        output: result.body,
                                        timed_out: result.timed_out,
                                        metadata: item_meta,
                                        mutation_delta: None,
                                        sandbox_denials: result.sandbox_denials,
                                        tracked_modifications: result.tracked_modifications,
                                        node_scratch: None,
                                    }),
                                );
                            }
                        }
                        Ok(results) => {
                            let failure = CellOutcome::FailedAfterExecution {
                                request_id: leader.0.clone(),
                                attempt_id: leader.1.clone(),
                                diagnostic: format!(
                                    "executor returned {} item outcomes for {} claimed checks",
                                    results.len(),
                                    keys.len()
                                ),
                            };
                            for key in &keys {
                                settled_early += settle_at_end(key, settled(failure.clone()));
                            }
                        }
                        Err(error) => {
                            let failure = CellOutcome::FailedAfterExecution {
                                request_id: leader.0.clone(),
                                attempt_id: leader.1.clone(),
                                diagnostic: format!("decode typed check batch outcomes: {error}"),
                            };
                            for key in &keys {
                                settled_early += settle_at_end(key, settled(failure.clone()));
                            }
                        }
                    },
                    CellOutcome::Completed {
                        tracked_modifications: Some(_),
                        ..
                    } => {
                        let failure = CellOutcome::FailedAfterExecution {
                            request_id: leader.0.clone(),
                            attempt_id: leader.1.clone(),
                            diagnostic:
                                "executor returned unattributed batch-level mutation evidence"
                                    .into(),
                        };
                        for key in &keys {
                            settled_early += settle_at_end(key, settled(failure.clone()));
                        }
                    }
                    _ => {
                        for key in &keys {
                            settled_early += settle_at_end(key, settled(outcome.clone()));
                        }
                    }
                }
                // Keys settled early never reach `complete_coalesced_for_leader`,
                // so when they were the last ones the leader ends here, on its
                // own terminal outcome.
                if settled_early > 0 {
                    pool.end_coalesced_leader_if_drained(&leader, &settled(outcome.clone()));
                }
                completion_guard.disarm();
            });
        }
        admission_leader.disarm();
        let delivery = self
            .await_coalesced(subscriber, wait_horizon_unix_ms, rx)
            .await;
        // Still armed only for a check that led nothing: its subscription has
        // ended, and with it its stake in the fleet.
        drop(admission_guard);
        drop(admission_subscriber);
        delivery
    }

    /// The confinement a batch runs under.
    ///
    /// The agent's fence dial is the only gate on whether a fence profile
    /// exists: an `allow` agent's batch has none wherever it runs, including in
    /// the project's live checkout, and neither does a batch with no resolvable
    /// run identity. Either way it is agent work and carries the operator
    /// credential boundary. The repository shape only picks which profile a
    /// *fenced* batch gets — an externally owned live checkout stays readable
    /// with its writes kernel-denied, a cell confines writes to itself.
    pub(super) fn batch_sandbox_mode(
        fence: Option<crate::models::Fence>,
        repository: &RepositoryLocator,
    ) -> ProcessSandboxMode {
        match fence {
            Some(fence) if crate::services::sandbox::sandbox_applies(fence) => {
                if runs_in_live_checkout(repository) {
                    ProcessSandboxMode::ReadOnlyCheckout
                } else {
                    ProcessSandboxMode::Confined
                }
            }
            _ => ProcessSandboxMode::CredentialFloor,
        }
    }

    pub(crate) async fn submit_run_batch(
        &self,
        orch: &Orchestrator,
        mut request: CellRequest,
        batch: ResolvedRunBatch,
        stop: &PlacementStop,
    ) -> ExecutedRequest {
        let identity = (request.request_id.clone(), request.attempt_id.clone());
        let cancelled = || CellOutcome::Cancelled {
            request_id: identity.0.clone(),
            attempt_id: identity.1.clone(),
        };
        let runner_context_id = uuid::Uuid::new_v4().to_string();
        let output = output_guard::RelayedOutput::shared();
        let run_context = batch.run_context.clone();
        self.runner_contexts.lock().unwrap().insert(
            runner_context_id.clone(),
            RunnerContextEntry::new(
                &runner_context_id,
                &format!("{}:{}", request.request_id, request.attempt_id),
                RunnerCallbackContext {
                    request: Some(batch.request.clone()),
                    run_context: batch.run_context.clone(),
                    check_status_board: None,
                    run_batch_request_id: Some(request.request_id.clone()),
                    live_checkout: runs_in_live_checkout(&request.repository),
                    executor_binding: None,
                    item_settlements: None,
                    output: output.clone(),
                },
            ),
        );
        // A page-call envelope decides the mode by itself: page-call work runs
        // only under the page-call policy, whatever a fence would say, and the
        // policy is the one its machine proved.
        let page_call = request
            .executor
            .as_ref()
            .and_then(|selector| selector.containment.as_ref())
            .is_some_and(cairn_common::executor_protocol::WorkloadEnvelope::is_page_call);
        let sandbox_mode = if page_call {
            ProcessSandboxMode::PageCall
        } else {
            let resolved = stop
                .until(crate::mcp::handlers::fence::resolve_run_fence(
                    orch,
                    &batch.request,
                ))
                .await;
            let Some(resolved) = resolved else {
                self.settle_runner_context(&runner_context_id);
                return cancelled().into();
            };
            let resolved_fence = match resolved {
                crate::mcp::handlers::fence::RunFenceResolution::Resolved(_, fence) => Some(fence),
                crate::mcp::handlers::fence::RunFenceResolution::Ambient => None,
                crate::mcp::handlers::fence::RunFenceResolution::Unavailable(diagnostic) => {
                    self.end_runner_context(&runner_context_id);
                    return CellOutcome::Unavailable {
                        reason: CellUnavailableReason::RunnerContext,
                        diagnostic,
                    }
                    .into();
                }
            };
            Self::batch_sandbox_mode(resolved_fence, &request.repository)
        };
        let node_scratch = batch.node_scratch.clone();
        let mut batch = match serialize_process_batch(
            batch,
            &request.env,
            runner_context_id.clone(),
            sandbox_mode,
            request
                .executor
                .as_ref()
                .and_then(|selector| selector.containment.clone()),
        ) {
            Ok(batch) => batch,
            Err(diagnostic) => {
                return CellOutcome::Unavailable {
                    reason: CellUnavailableReason::Spawn,
                    diagnostic,
                }
                .into()
            }
        };
        batch.node_scratch = node_scratch;
        // An agent's batch is the one submission whose requester reads the
        // outcome as a statement about whether its commands ran, so it is the
        // one that keeps the account of a grant without a result.
        let executed = match stop.until(self.prepare_execution(orch, &mut request)).await {
            Some(Ok(prepared)) => {
                self.execute_prepared_accounted(request, Some(batch), prepared, Some(stop))
                    .await
            }
            Some(Err(outcome)) => outcome.into(),
            None => cancelled().into(),
        };
        output_guard::flush_batch_output(orch, &output, run_context.as_ref());
        self.settle_runner_context(&runner_context_id);
        executed
    }

    /// Submit one cadence-coherent check batch without result coalescing.
    ///
    /// Mutating write checks cannot be coalesced per item: the executor returns a
    /// single end-of-batch delta and every item must observe earlier mutations.
    /// Review checks continue through the pure-verdict submission path.
    pub(crate) async fn submit_write_check_batch(
        &self,
        orch: &Orchestrator,
        request: CellRequest,
        items: Vec<ProcessBatchItem>,
        run_context: Option<crate::mcp::handlers::RunContext>,
        check_status_board: Option<crate::execution::checks::CheckStatusBoard>,
    ) -> ExecutedRequest {
        if request.mutation_policy != MutationPolicy::AllowDelta {
            return executor_unavailable(
                "write-check batch submission requires allow-delta mutation policy".into(),
            )
            .into();
        }
        let request_id = request.request_id.clone();
        let attempt_id = request.attempt_id.clone();
        self.register_direct_check((request_id.clone(), attempt_id.clone()));
        let runner_context_id = uuid::Uuid::new_v4().to_string();
        let output = output_guard::RelayedOutput::shared();
        let flushed_context = run_context.clone();
        self.runner_contexts.lock().unwrap().insert(
            runner_context_id.clone(),
            RunnerContextEntry::new(
                &runner_context_id,
                &format!("{}:{}", request.request_id, request.attempt_id),
                RunnerCallbackContext {
                    request: None,
                    run_context,
                    check_status_board,
                    run_batch_request_id: None,
                    live_checkout: false,
                    executor_binding: None,
                    item_settlements: None,
                    output: output.clone(),
                },
            ),
        );
        let batch = Self::check_process_batch(
            items,
            Some(runner_context_id.clone()),
            Self::check_batch_containment(&request),
            Self::WRITE_CHECK_SILENCE_BOUND_MS,
        );
        let mut request = request;
        let preparing = &mut request;
        let executed = match off_frame(move || self.prepare_execution(orch, preparing)).await {
            Ok(prepared) => {
                off_frame(|| self.execute_prepared_accounted(request, Some(batch), prepared, None))
                    .await
            }
            Err(outcome) => outcome.into(),
        };
        output_guard::flush_batch_output(orch, &output, flushed_context.as_ref());
        self.settle_runner_context(&runner_context_id);
        let identity = (request_id, attempt_id);
        if self.check_claim(&identity).is_none() {
            self.check_leader_completed(&identity);
        }
        executed
    }

    /// Publish process output without entering the serialized core task lane.
    /// A run request itself waits for the executor result on that lane, so routing
    /// output callbacks through it deadlocks the producer behind its own consumer.
    pub fn handle_process_output(
        &self,
        orch: &Orchestrator,
        context_id: &str,
        stream_id: &str,
        payload: String,
    ) -> RunnerCallbackResult {
        let Some(context) = self
            .runner_contexts
            .lock()
            .unwrap()
            .get(context_id)
            .map(|entry| entry.context.clone())
        else {
            return RunnerCallbackResult::Failed {
                diagnostic: self.describe_unheld_context(context_id),
            };
        };
        // Scrubbed before it is published, against this runner's registry rather
        // than the executor's. See `output_guard` for why both passes exist and
        // why neither subsumes the other. The withheld tail is released by
        // `flush_batch_output` when the batch settles.
        let payload = context
            .output
            .lock()
            .unwrap()
            .settle_text(stream_id, &payload);
        if !payload.is_empty() {
            if let Some(board) = &context.check_status_board {
                board.note_output(&payload);
            }
        }
        if let Some(run_context) = context.run_context {
            if let Some(board) = context.check_status_board {
                if let Some(index) = stream_id
                    .rsplit(":check-")
                    .next()
                    .and_then(|value| value.parse().ok())
                {
                    board.transition(index, "running", None);
                }
            }
            // A chunk that was entirely withheld is not a chunk yet. Publishing
            // an empty one would render as a flicker with nothing behind it.
            if !payload.is_empty() {
                let _ = orch.services.emitter.emit(
                    "run-output",
                    serde_json::json!({
                        "runId": run_context.run_id,
                        "toolUseId": stream_id,
                        "chunk": payload,
                        "stream": "stdout",
                    }),
                );
            }
        }
        RunnerCallbackResult::Completed
    }

    pub async fn handle_runner_callback(
        &self,
        orch: &Orchestrator,
        callback: RunnerCallback,
    ) -> RunnerCallbackResult {
        if let RunnerCallback::ProcessEvent {
            runner_context_id,
            stream_id,
            payload,
        } = callback
        {
            return self.handle_process_output(orch, &runner_context_id, &stream_id, payload);
        }
        let context_id = match &callback {
            RunnerCallback::SandboxDenied {
                runner_context_id, ..
            }
            | RunnerCallback::CacheCheckpoint {
                runner_context_id, ..
            }
            | RunnerCallback::ProcessItemStarted {
                runner_context_id, ..
            }
            | RunnerCallback::ProcessItemCompleted {
                runner_context_id, ..
            } => runner_context_id,
            RunnerCallback::ProcessEvent { .. } => unreachable!("handled above"),
        };
        // Subscribed under the same lock that finds the context, so a closure
        // recorded after this point cannot be missed.
        let Some((context, closed)) = self
            .runner_contexts
            .lock()
            .unwrap()
            .get(context_id)
            .map(|entry| (entry.context.clone(), entry.closed.subscribe()))
        else {
            return RunnerCallbackResult::Failed {
                diagnostic: self.describe_unheld_context(context_id),
            };
        };
        match callback {
            RunnerCallback::ProcessItemStarted { stream_id, .. } => {
                if let Some(board) = context.check_status_board {
                    if let Some(index) = check_index_from_stream_id(&stream_id) {
                        board.transition(index, "running", None);
                    }
                } else if let Some(request_id) = context.run_batch_request_id.as_deref() {
                    if let Some(index) = run_item_index_from_stream_id(&stream_id) {
                        crate::mcp::handlers::run::detached::note_item_started(request_id, index);
                    }
                }
                RunnerCallbackResult::Completed
            }
            RunnerCallback::ProcessItemCompleted {
                stream_id,
                succeeded,
                exit_code,
                timed_out,
                duration_ms,
                result,
                ..
            } => {
                if let (Some(result), Some(settlements)) =
                    (result, context.item_settlements.as_deref())
                {
                    self.settle_item_early(settlements, &stream_id, *result);
                }
                if let Some(board) = context
                    .check_status_board
                    .as_ref()
                    .filter(|board| board.engine_owns_verdicts())
                {
                    if let Some(index) = check_index_from_stream_id(&stream_id) {
                        let exit = match exit_code {
                            Some(code) => format!("exit {code}"),
                            None if timed_out => "timed out".into(),
                            None => "no exit code".into(),
                        };
                        board.transition(
                            index,
                            "finished",
                            Some(format!(
                                "finished in {} ({exit}); awaiting recording",
                                crate::execution::checks_status::human_duration(duration_ms)
                            )),
                        );
                    }
                } else if let Some(board) = context.check_status_board {
                    if let Some(index) = check_index_from_stream_id(&stream_id) {
                        let annotation = if succeeded {
                            Some(format_duration_annotation(duration_ms))
                        } else {
                            Some(match exit_code {
                                Some(code) => format!("exit {code}"),
                                None if timed_out => "timed out".into(),
                                None => "failed".into(),
                            })
                        };
                        board.transition(
                            index,
                            if succeeded { "passed" } else { "failed" },
                            annotation,
                        );
                    }
                } else if let Some(request_id) = context.run_batch_request_id.as_deref() {
                    if let Some(index) = run_item_index_from_stream_id(&stream_id) {
                        crate::mcp::handlers::run::detached::note_item_finished(request_id, index);
                    }
                }
                RunnerCallbackResult::Completed
            }
            RunnerCallback::SandboxDenied {
                command,
                denial,
                reported_denial,
                rerun,
                ..
            } => {
                // An executor refuses its own operator credential denials
                // without calling back; this catches a path only this runner
                // knows is a Cairn home, before any branch could prompt for it.
                if let cairn_common::executor_protocol::SandboxDenial::Path(path) = &denial {
                    if let Some(refusal) = crate::authorization::protected::read_refusal(
                        &orch.config_dir,
                        std::path::Path::new(path),
                    ) {
                        return RunnerCallbackResult::Rejected {
                            diagnostic: refusal.into(),
                        };
                    }
                }
                if context.live_checkout {
                    return RunnerCallbackResult::Rejected {
                        diagnostic: crate::mcp::handlers::run::READ_ONLY_CHECKOUT_DENIAL.into(),
                    };
                }
                use crate::mcp::handlers::fence::{self, FenceDecision};
                use crate::mcp::handlers::permission::Handoff;
                let Some(request) = context.request.as_ref() else {
                    return RunnerCallbackResult::Rejected {
                        diagnostic:
                            "check batch sandbox denial cannot be interactively adjudicated".into(),
                    };
                };
                let (run_id, mode) = match fence::resolve_run_fence(orch, request).await {
                    fence::RunFenceResolution::Resolved(run_id, mode) => (run_id, mode),
                    fence::RunFenceResolution::Unavailable(diagnostic) => return RunnerCallbackResult::Failed { diagnostic },
                    fence::RunFenceResolution::Ambient => return RunnerCallbackResult::Failed {
                        diagnostic: "permission context unavailable: sandbox denial has no authenticated originating run".into(),
                    },
                };
                let denied_path = match &denial {
                    cairn_common::executor_protocol::SandboxDenial::Path(path) => {
                        Some(std::path::Path::new(path))
                    }
                    cairn_common::executor_protocol::SandboxDenial::Command => None,
                };
                let mut crossing = fence::Crossing::for_denied_item(
                    denied_path,
                    rerun.as_ref(),
                    &command,
                    &command,
                    reported_denial.as_deref(),
                );
                // The binding names the executor that ran the item, so a
                // refusal can offer running it there once by request.
                if let Some(binding) = context.executor_binding.as_ref() {
                    let declared_check = match (denied_path, context.run_context.as_ref()) {
                        (None, Some(run_context)) => {
                            crate::mcp::handlers::run::is_declared_check(
                                orch,
                                &run_context.project_id,
                                &request.cwd,
                                &command,
                            )
                            .await
                        }
                        _ => false,
                    };
                    crossing = crossing.offering_request(&binding.executor_id, declared_check);
                }
                match fence::raise_fence_with_handoff(
                    orch,
                    &run_id,
                    mode,
                    request,
                    crossing,
                    Handoff::Watched(closed),
                )
                .await
                {
                    FenceDecision::Allow => RunnerCallbackResult::Allowed,
                    FenceDecision::Deny(diagnostic) => {
                        RunnerCallbackResult::Rejected { diagnostic }
                    }
                    FenceDecision::Unavailable(diagnostic) => {
                        RunnerCallbackResult::Failed { diagnostic }
                    }
                    FenceDecision::Suspended => RunnerCallbackResult::Suspended,
                }
            }
            RunnerCallback::CacheCheckpoint {
                command, exit_code, ..
            } => {
                if let Some(run_context) = context.run_context {
                    crate::mcp::handlers::run::record_cache_checkpoint(
                        orch,
                        &run_context.job_id,
                        &command,
                        exit_code,
                    )
                    .await;
                }
                RunnerCallbackResult::Completed
            }
            RunnerCallback::ProcessEvent { .. } => unreachable!("handled above"),
        }
    }

    pub(super) async fn submit_execution(
        &self,
        orch: &Orchestrator,
        mut request: CellRequest,
        batch: Option<ProcessBatch>,
    ) -> CellOutcome {
        let prepared = match self.prepare_execution(orch, &mut request).await {
            Ok(prepared) => prepared,
            Err(outcome) => return outcome,
        };
        self.execute_prepared(request, batch, prepared).await
    }

    /// Resolve what running `request` needs from this runner, and stamp the
    /// project's placement requirements onto it so every placement of it (and
    /// of every copy made from it) asks the same eligibility question.
    #[allow(clippy::result_large_err)]
    pub(super) async fn prepare_execution(
        &self,
        orch: &Orchestrator,
        request: &mut CellRequest,
    ) -> Result<PreparedExecution, CellOutcome> {
        let config =
            crate::config::settings::read_fleet_strict(&orch.config_dir).map_err(|error| {
                CellOutcome::Unavailable {
                    reason: CellUnavailableReason::ExecutorUnavailable,
                    diagnostic: format!("load cell settings: {error}"),
                }
            })?;
        if matches!(request.repository, RepositoryLocator::ScratchOnly { .. }) {
            return Ok(PreparedExecution {
                cell_client_env: Vec::new(),
                placement_policy: ActivePlacementPolicy {
                    name: config.active_placement_profile.clone(),
                    profile: config.active_placement_profile().clone(),
                },
                executor_config: ExecutorConfig {
                    project_id: request.project_id.clone(),
                    project_key: request.project_id.clone(),
                    default_timeout_seconds: config.default_timeout_seconds,
                    setup_commands: Vec::new(),
                    populate: cairn_cell::PopulateConfig::default(),
                    population_source_root: None,
                },
                object_plane: orch.object_plane.clone(),
                db: orch.db.local.clone(),
                source_store: None,
            });
        }
        let (local_repo_path, project_key) =
            crate::projects::crud::resolve_local_repo_path_and_key(&orch.db, &request.project_id)
                .await
                .map_err(|error| CellOutcome::Unavailable {
                    reason: CellUnavailableReason::ExecutorUnavailable,
                    diagnostic: format!("resolve cell project key: {error}"),
                })?;
        let project_path = local_repo_path.as_deref().or(match &request.repository {
            RepositoryLocator::ColocatedPath { absolute_path, .. }
            | RepositoryLocator::ExistingCheckout { absolute_path, .. } => {
                Some(absolute_path.as_str())
            }
            RepositoryLocator::ManagedObjects { .. } | RepositoryLocator::ScratchOnly { .. } => {
                None
            }
        });
        let project_path = project_path.ok_or_else(|| CellOutcome::Unavailable {
            reason: CellUnavailableReason::Preparation,
            diagnostic: "resolve canonical project setup: no local project checkout".into(),
        })?;
        // Owned, so it no longer borrows the request it may have come from.
        let project_path = project_path.to_string();
        let project_path = project_path.as_str();
        // Resolve setup from the primary checkout, from the canonical project configuration;
        // the requested commit may contain an older project configuration. This hot path is
        // deliberately fallible and side-effect free: it must neither migrate config nor run
        // a command after invalid setup policy was defaulted away.
        let project_policy =
            crate::config::project_settings::load_execution_project_policy(Path::new(project_path))
                .map_err(|error| CellOutcome::Unavailable {
                    reason: CellUnavailableReason::Preparation,
                    diagnostic: project_config_refusal(&error),
                })?;
        self.stamp_project_requirements(
            request,
            &project_policy.setup_commands,
            &project_policy.requires,
            project_path,
        );
        self.note_colocated_population(&request.project_id, !project_policy.populate.is_empty());
        Ok(PreparedExecution {
            cell_client_env: cell_build_service_env(orch, &request.repository),
            placement_policy: ActivePlacementPolicy {
                name: config.active_placement_profile.clone(),
                profile: config.active_placement_profile().clone(),
            },
            executor_config: ExecutorConfig {
                project_id: request.project_id.clone(),
                project_key,
                default_timeout_seconds: config.default_timeout_seconds,
                setup_commands: project_policy.setup_commands,
                populate: project_policy.populate,
                population_source_root: Some(project_path.to_string()),
            },
            object_plane: orch.object_plane.clone(),
            db: orch.db.local.clone(),
            source_store: Some(crate::jj::project_store_dir(
                &orch.config_dir,
                Path::new(project_path),
            )),
        })
    }

    /// Stamp what the project's preparation needs onto `request`, remember it,
    /// and forget refusals recorded under a setup the project no longer runs.
    ///
    /// Synchronous and kept out of line on purpose: in an async function every
    /// temporary claims space in the poll frame, and `prepare_execution` sits
    /// deep under residency operations whose stacks are already near the limit
    /// in debug builds.
    #[inline(never)]
    pub(super) fn stamp_project_requirements(
        &self,
        request: &mut CellRequest,
        setup_commands: &[String],
        declared: &[ProjectCapability],
        project_path: &str,
    ) {
        // Declared and learned capabilities hold whether or not setup runs:
        // a build fetches its dependencies in a live checkout too.
        let mut capabilities = declared.to_vec();
        capabilities.extend(self.learned_requirements.capabilities(&request.project_id));
        // A live checkout is never prepared, so it is held to nothing the
        // setup needs; what its checkout declares about building still holds.
        if runs_project_setup(&request.repository) {
            *request.project_requirements = project_placement_requirements(
                setup_commands,
                &capabilities,
                Path::new(project_path),
            );
            self.known_project_requirements.lock().unwrap().insert(
                request.project_id.clone(),
                (*request.project_requirements).clone(),
            );
            self.forget_superseded_setup_refusals(
                &request.project_id,
                &request.project_requirements.setup_identity,
            );
        } else {
            let declared =
                project_placement_requirements(&[], &capabilities, Path::new(project_path));
            *request.project_requirements = ProjectPlacementRequirements {
                prerequisite_scripts: declared.prerequisite_scripts,
                capabilities: declared.capabilities,
                ..ProjectPlacementRequirements::default()
            };
        }
    }

    pub(super) async fn execute_prepared(
        &self,
        request: CellRequest,
        batch: Option<ProcessBatch>,
        prepared: PreparedExecution,
    ) -> CellOutcome {
        self.execute_prepared_accounted(request, batch, prepared, None)
            .await
            .outcome
    }

    /// The answer for a request this runner withdraws because it is stopping,
    /// naming where the request stood.
    pub(super) fn stopped_outcome(
        &self,
        stopped_at: u64,
        identity: &RequestIdentity,
        granted_to: Option<&str>,
    ) -> CellOutcome {
        let phase = self.withdrawal_phase(identity, granted_to);
        runner_stopping(
            stopped_at,
            format!(
                "this runner began shutting down at {} while this request was {phase}; it was withdrawn, and neither the machine nor the work is implicated",
                describe_runner_stop(stopped_at)
            ),
        )
    }

    /// Where a live request stands, in the words a withdrawal names it by.
    /// `granted_to` is the machine a grant was delivered to, when the caller
    /// knows; a claim without one may not be running yet.
    pub(super) fn withdrawal_phase(
        &self,
        identity: &RequestIdentity,
        granted_to: Option<&str>,
    ) -> String {
        let state = self
            .queue
            .with_entry_by_identity(identity, |entry| entry.state.clone());
        match state {
            None => "not held in the queue".to_string(),
            Some(queue::EntryState::Queued) => "waiting to be placed".to_string(),
            Some(queue::EntryState::Offered { executor_id, .. }) => {
                format!("offered to {}", self.executor_name_for_text(&executor_id))
            }
            Some(queue::EntryState::Claimed { executor_id, .. }) => {
                let name = self.executor_name_for_text(&executor_id);
                if granted_to == Some(executor_id.as_str()) {
                    format!("running on {name}")
                } else {
                    format!("claimed by {name}")
                }
            }
        }
    }

    /// [`Self::execute_prepared`], also reporting a grant it delivered that
    /// produced no result, and why Cairn cancelled the request when it did.
    ///
    /// The cause is joined here, at the one exit every queued request passes,
    /// because the executor's `Cancelled` carries none: the runner recorded it
    /// when it sent `Cancel`, and taking it here also clears the record.
    pub(super) async fn execute_prepared_accounted(
        &self,
        request: CellRequest,
        batch: Option<ProcessBatch>,
        prepared: PreparedExecution,
        stop: Option<&PlacementStop>,
    ) -> ExecutedRequest {
        let key = (request.request_id.clone(), request.attempt_id.clone());
        let mut executed = off_frame(|| self.execute_entry(request, batch, prepared, stop)).await;
        if stop.is_some() {
            self.cancelled_leaders.lock().unwrap().remove(&key);
        }
        executed.account.cancel_cause = self.cancel_causes.lock().unwrap().take(&key);
        executed
    }

    /// Queue, place, grant, and await one request. Every path that returns
    /// before the grant converts its outcome with `into()`, which states that
    /// nothing was granted.
    ///
    /// The runner beginning to shut down is raced at each wait, and the order
    /// differs on either side of the grant. Before it, the stop is checked
    /// first: a request still being placed would otherwise see the colocated
    /// machine the shutdown removes and be told no machine could run it. After
    /// it, the result is checked first, so an answer the runner has already
    /// accepted is never replaced, and a stop that does win keeps the account
    /// of the grant, because the batch may already have run.
    pub(super) async fn execute_entry(
        &self,
        request: CellRequest,
        batch: Option<ProcessBatch>,
        prepared: PreparedExecution,
        stop: Option<&PlacementStop>,
    ) -> ExecutedRequest {
        let PreparedExecution {
            executor_config,
            object_plane,
            db,
            cell_client_env,
            placement_policy,
            source_store,
        } = prepared;
        let mut request = request;
        if let Err(diagnostic) = require_colocated_population(&mut request, &executor_config) {
            return CellOutcome::Unavailable {
                reason: CellUnavailableReason::NoMatchingExecutor {
                    rejected: Vec::new(),
                },
                diagnostic,
            }
            .into();
        }
        let plan = ReservationPlan::new(db.clone(), &request, batch.as_ref());
        let key = (request.request_id.clone(), request.attempt_id.clone());
        // Nothing queued after the stop could be waited on by anyone.
        if let Some(stopped_at) = self.runner_stop.at() {
            return self.stopped_outcome(stopped_at, &key, None).into();
        }
        // Retired while it was still being prepared: nothing is queued yet,
        // so ending it is the runner's own act and needs no machine.
        if self.cancelled_leaders.lock().unwrap().remove(&key)
            || stop.is_some_and(PlacementStop::is_stopped)
        {
            self.preparing_leaders.lock().unwrap().remove(&key);
            return CellOutcome::Cancelled {
                request_id: key.0,
                attempt_id: key.1,
            }
            .into();
        }
        let batch_profile_identities = plan.batch_identities.clone();
        let request_suite = plan.suite.clone();
        let batch_suites: Vec<Option<String>> = plan
            .batch_suites
            .iter()
            .map(|(_, suite, _)| suite.clone())
            .collect();
        let batch_classes: Vec<_> = plan
            .batch_suites
            .iter()
            .map(|(_, _, class)| *class)
            .collect();
        let batch_items_share_the_machine = plan.parallel_width > 1;
        let profile_identity = request.command_resource_identity.clone();
        let standing = self.purposes.lock().unwrap().live_key_of(&key).is_some();
        // One entry stands for this work from here until it settles. Placement
        // may offer it to several machines in turn; the entry, the waiter behind
        // it, and its binding chain outlive every one of those attempts.
        let Ok((entry_id, rx)) = self.queue.enqueue(
            queue::NewEntry {
                request: request.clone(),
                admission_kind: CellAdmissionKind::Command,
                interactive: request.priority == CellPriority::AgentInteractive,
                execution_residency: batch
                    .as_ref()
                    .and_then(|batch| batch.execution_residency.clone()),
                standing,
            },
            unix_time_ms(),
        ) else {
            return executor_unavailable("duplicate cell request identity".into()).into();
        };
        self.preparing_leaders.lock().unwrap().remove(&key);
        let mut guard = EntryDropGuard {
            pool: self.clone(),
            request_id: key.0.clone(),
            attempt_id: key.1.clone(),
            entry_id: entry_id.clone(),
            granted_to: None,
            cause: None,
            armed: true,
        };
        let plan = Arc::new(plan);
        let _joined = assignments::JoinedDispatcher {
            pool: self.clone(),
            entry_id: entry_id.clone(),
        };
        let mut deliveries = None;
        // Offer, and let the machine answer from its own live facts. A decline
        // is routing information rather than a failure: the entry comes back
        // around this loop to the next candidate, having never been parked.
        // The loop also yields whether the placement a machine claimed went
        // above that machine's memory ceiling, which the caller is owed
        // whatever the outcome.
        let (selected, request, batch, profile_context, claimed_above_ceiling) = loop {
            let Placement {
                selected,
                reservation,
                mut decision,
                above_ceiling,
                headroom,
            } = match tokio::select! {
                biased;
                stopped_at = self.runner_stop.stopped() => {
                    guard.cause = Some(WithdrawalCause::RunnerStopped(stopped_at));
                    return self.stopped_outcome(stopped_at, &key, None).into();
                }
                // Retired while waiting for a machine. Nothing was granted, so
                // the runner ends it here rather than waiting for a claim that
                // may never come; that `Cancelled` is the attempt's own end.
                () = self.retired_or_stopped(&key, stop) => {
                    self.queue.withdraw(
                        &key,
                        "withdrawn: retired before any machine claimed it".into(),
                        unix_time_ms(),
                    );
                    guard.disarm();
                    return CellOutcome::Cancelled {
                        request_id: key.0.clone(),
                        attempt_id: key.1.clone(),
                    }
                    .into();
                }
                selection = self.await_assignment(
                    &entry_id,
                    &request,
                    Some(&plan),
                    &placement_policy,
                    &mut deliveries,
                ) => selection,
            } {
                Ok(placement) => placement,
                Err(outcome) => {
                    self.queue
                        .take(&key, queue::TerminalState::Expired, unix_time_ms());
                    guard.disarm();
                    return outcome.into();
                }
            };
            // Only now is the machine known. The compile daemon is this
            // machine's, reachable on loopback and named by its paths, so a
            // batch placed on a remote executor keeps whatever cache that host
            // configured for itself.
            let attempt_batch = match batch.clone() {
                Some(batch) if selected.colocated && !cell_client_env.is_empty() => {
                    Some(with_cell_client_env(batch, &cell_client_env))
                }
                batch => batch,
            };
            let profile_context = ReservationPlan::profile_context(&selected);
            let mut attempt_request = request.clone();
            if let Some(resolved) = reservation {
                attempt_request.resource_reservation = resolved.reservation;
                attempt_request.learned_estimate = resolved.learned_estimate;
            }
            if !selected.colocated {
                let identity = attempt_request.repository.identity();
                attempt_request.repository = RepositoryLocator::ManagedObjects {
                    project_id: identity.project_id,
                    repository_id: identity.repository_id,
                    object_format: identity.object_format,
                };
                // The coordinate the objects travel under is only knowable once
                // the machine is chosen, and it is what makes a remote object
                // refusal actionable from the decision record alone.
                if let PlacementOutcome::Selected(selection) = &mut decision.outcome {
                    selection.object_transfer = Some(ObjectTransferCoordinate {
                        repository: attempt_request.repository.identity(),
                        request_id: attempt_request.request_id.clone(),
                        attempt_id: attempt_request.attempt_id.clone(),
                        executor_id: selected.executor_id.clone(),
                        connection_generation: selected.generation,
                    });
                }
            }
            let answer = tokio::select! {
                biased;
                stopped_at = self.runner_stop.stopped() => {
                    guard.cause = Some(WithdrawalCause::RunnerStopped(stopped_at));
                    return self.stopped_outcome(stopped_at, &key, None).into();
                }
                // Retired with an offer out. The machine may claim it as this
                // runs, so it is told the grant is never coming.
                () = self.retired_or_stopped(&key, stop) => {
                    self.queue.withdraw(
                        &key,
                        "withdrawn: retired while offered, before any grant".into(),
                        unix_time_ms(),
                    );
                    guard.disarm();
                    withdraw_claim(&selected, &entry_id);
                    return CellOutcome::Cancelled {
                        request_id: key.0.clone(),
                        attempt_id: key.1.clone(),
                    }
                    .into();
                }
                answer = self.offer_once(
                    &entry_id,
                    &attempt_request,
                    &selected,
                    &mut decision,
                    headroom,
                ) => answer,
            };
            match answer {
                OfferAnswer::Claimed => {
                    break (
                        selected,
                        attempt_request,
                        attempt_batch,
                        profile_context,
                        above_ceiling,
                    );
                }
                OfferAnswer::Requeued => continue,
                OfferAnswer::Expired => {
                    let outcome = self.refuse_at_horizon(
                        &request,
                        &placement_policy,
                        Some(&entry_id),
                        Vec::new(),
                    );
                    self.queue
                        .take(&key, queue::TerminalState::Expired, unix_time_ms());
                    guard.disarm();
                    return outcome.into();
                }
                OfferAnswer::Gone => {
                    guard.disarm();
                    // The entry left the queue while the offer was out, so
                    // the machine may already have claimed it. A `Lost` for a
                    // claim it never made is a no-op there.
                    withdraw_claim(&selected, &entry_id);
                    return CellOutcome::Cancelled {
                        request_id: key.0,
                        attempt_id: key.1,
                    }
                    .into();
                }
            }
        };
        // Only a claim puts the work on a machine, so only from here on can a
        // lost result be laid at a machine's door.
        let mut placement = CellPlacement {
            executor_id: selected.executor_id.clone(),
            generation: selected.generation,
            name: self.executor_name_for_text(&selected.executor_id),
            platform: Some(ExecutorPlatform::advertised(&selected.capabilities)),
        };
        // The claim is the binding, so it is the moment every grant is issued:
        // the runner context and the purpose registry's record that this item
        // has actually been dispatched. An offer conferred neither, which is
        // what let it be declined for free.
        if let Err(diagnostic) = self.bind_runner_context(
            batch.as_ref(),
            &request,
            &selected.executor_id,
            selected.generation,
        ) {
            self.queue
                .take(&key, queue::TerminalState::Cancelled, unix_time_ms());
            guard.disarm();
            withdraw_claim(&selected, &entry_id);
            return executor_unavailable(diagnostic).into();
        }
        let project_id = request.project_id.clone();
        let setup_identity = request.project_requirements.setup_identity.clone();
        let runs_in_home = request.pinned_executor_id.is_some();
        let granted_at_unix_ms = unix_time_ms();
        self.claim_check_at_grant(&key, &selected, granted_at_unix_ms)
            .await;
        // A retirement that landed while this item was being placed stops it
        // here. Granting anyway and chasing it with a Cancel is what made
        // retirement a race the work usually won: the granted item took an
        // occupancy row, and the cancel behind it could be handled before the
        // executor had bound the identity it named.
        if self.cancelled_leaders.lock().unwrap().remove(&key)
            || stop.is_some_and(PlacementStop::is_stopped)
        {
            self.settle_check_claim(
                &key,
                "cancelled",
                "stopped before the executor grant".into(),
                true,
            );
            self.queue
                .take(&key, queue::TerminalState::Cancelled, unix_time_ms());
            guard.disarm();
            withdraw_claim(&selected, &entry_id);
            return CellOutcome::Cancelled {
                request_id: key.0,
                attempt_id: key.1,
            }
            .into();
        }
        // The object plane opens last, in the same step as the grant and after
        // every fallible thing before it has succeeded. A machine that never
        // receives `Granted` has nothing to fetch and nothing to report, so
        // authority handed out earlier had no failure path that would take it
        // back: it survived until the connection generation ended. Authorizing
        // here means the only exit between the grant and its revocation is the
        // send below, which revokes on failure.
        if !selected.colocated {
            object_plane.authorize_request(&request, &selected.executor_id, selected.generation);
        }
        // A node scratch is reached over the object channel from every machine,
        // colocated or not, and only by the connection this batch was granted to.
        if let Some(grant) = batch.as_ref().and_then(|batch| batch.node_scratch.as_ref()) {
            object_plane.scratch.authorize(
                &selected.executor_id,
                selected.generation,
                &grant.scratch_id,
            );
        }
        let admission_request = request.clone();
        let admission = self.send_new_admission(
            &selected,
            &admission_request,
            drain_projection::resident_work(&admission_request, &selected.executor_id),
            ExecutorMessage::Granted {
                entry_id: entry_id.clone(),
                config: executor_config,
                request,
                batch,
            },
        );
        let sent = admission.is_ok();
        if sent {
            guard.granted_to = Some(selected.executor_id.clone());
            self.purposes.lock().unwrap().dispatched(
                &key,
                &selected.executor_id,
                selected.generation,
                granted_at_unix_ms,
            );
        }
        // Retirement can still land in the instant between the gate above and
        // this send, so the delivered form remains.
        let cancelled_before_correlation = self.cancelled_leaders.lock().unwrap().remove(&key)
            || stop.is_some_and(PlacementStop::is_stopped);
        if cancelled_before_correlation && sent {
            let _ = self.send_to(
                &selected.executor_id,
                selected.generation,
                ExecutorMessage::Cancel {
                    request_id: key.0.clone(),
                    attempt_id: key.1.clone(),
                },
            );
        }
        if !sent {
            let refusal = admission.expect_err("an unsent grant has an admission error");
            self.settle_check_claim(
                &key,
                "cancelled",
                "the executor grant was not delivered".into(),
                true,
            );
            withdraw_claim(&selected, &entry_id);
            self.queue
                .take(&key, queue::TerminalState::Cancelled, unix_time_ms());
            if !selected.colocated {
                object_plane.revoke_request(
                    &key.0,
                    &key.1,
                    &selected.executor_id,
                    selected.generation,
                );
            }
            guard.disarm();
            let mut executed: ExecutedRequest = executor_unavailable(refusal.diagnostic).into();
            executed.account.placement = Some(placement);
            return executed;
        }
        let waited = tokio::select! {
            biased;
            waited = self.await_claimed_result(&key, granted_at_unix_ms, rx) => Ok(waited),
            stopped_at = self.runner_stop.stopped() => Err(stopped_at),
        };
        // A claim can be carried across a reconnect onto a new generation, so
        // the link that answers for the work is the binding it held when it
        // was answered, or the one it holds now if the runner is ending it.
        if let Some((executor_id, generation)) = self
            .queue
            .take_settled_binding(&key)
            .or_else(|| self.queue.binding_of(&key))
        {
            if executor_id == placement.executor_id {
                placement.generation = generation;
            }
        }
        // Whether the runner, rather than the machine, ended this wait, and why.
        // The work may still be running when it did, so the item's question
        // keeps owing its death (see `Fleet::settle_leader_purpose`).
        let (outcome, ended_by_runner, abandoned_grant) = match waited {
            Ok(ClaimedWait::Answered(outcome)) => {
                // A runner shutting down answers its own claims, and that
                // answer says nothing about the machine, which keeps running.
                let abandoned = match &*outcome {
                    CellOutcome::Unavailable {
                        reason: CellUnavailableReason::RunnerStopping { stopped_at_unix_ms },
                        ..
                    } => Some(format!(
                        "this runner began shutting down at {}",
                        describe_runner_stop(*stopped_at_unix_ms)
                    )),
                    _ => None,
                };
                (*outcome, false, abandoned)
            }
            Ok(ClaimedWait::ChannelClosed) => (
                executor_unavailable(format!(
                    "executor {} (generation {}): result channel closed",
                    placement.executor_id, placement.generation
                )),
                false,
                Some("its result channel closed before the machine answered".to_string()),
            ),
            Err(stopped_at) => {
                guard.cause = Some(WithdrawalCause::RunnerStopped(stopped_at));
                (
                    self.stopped_outcome(stopped_at, &key, Some(&selected.executor_id)),
                    true,
                    Some(format!(
                        "this runner began shutting down at {}",
                        describe_runner_stop(stopped_at)
                    )),
                )
            }
            Ok(ClaimedWait::Silent(silence)) => {
                let executor_name = self.executor_name_for_text(&selected.executor_id);
                log::warn!(
                    "no account of request {} attempt {} from {executor_name} for {}ms ({:?}, last progress {:?}); cancelling it",
                    key.0,
                    key.1,
                    silence.quiet_ms,
                    silence.final_account,
                    silence.last_progress,
                );
                guard.cause = Some(WithdrawalCause::ClaimedAttemptSilent(compact_duration(
                    silence.quiet_ms,
                )));
                let abandoned = format!(
                    "{executor_name} gave no account of it for {}",
                    compact_duration(silence.quiet_ms)
                );
                (silence.outcome(&key, executor_name), true, Some(abandoned))
            }
        };
        if !selected.colocated {
            object_plane.revoke_request(&key.0, &key.1, &selected.executor_id, selected.generation);
        }
        // The executor names the job, request, and commit it could not
        // materialize; only the runner knows which machine it chose. Completing
        // the coordinate here is what makes a remote refusal actionable without
        // a second lookup: the operator reading it is looking at one failed
        // placement among several enrolled machines.
        let outcome = if selected.colocated {
            outcome
        } else {
            name_placement_in_object_refusal(outcome, &selected.executor_id, selected.generation)
        };
        // Every cell outcome passes here, so check lanes and agent batches
        // alike teach placement that this machine cannot prepare the project.
        let outcome = self.note_preparation_refusal(
            outcome,
            &selected.executor_id,
            &project_id,
            &setup_identity,
            runs_in_home,
        );
        // Read while the entry is still queued: its binding chain is the only
        // record of how often the claim was carried across a reconnect, and it
        // leaves with the entry.
        let grant_without_result =
            grant_left_without_result(&outcome).then(|| GrantWithoutResult {
                executor_name: self.executor_name_for_text(&selected.executor_id),
                granted_at_unix_ms,
                connection_drops: self
                    .queue
                    .with_entry(&entry_id, queue::QueueEntry::connection_drops_since_claim)
                    .flatten(),
            });
        // Left armed, the guard takes the entry out of the queue with the cause
        // set above and sends the machine `Cancel`, in case it is still running
        // work nobody waits for.
        if ended_by_runner {
            return ExecutedRequest {
                outcome,
                grant_without_result,
                abandoned_grant,
                above_ceiling: claimed_above_ceiling,
                account: CellAccount {
                    left_running: self.left_running_attempt(&key),
                    cancel_cause: None,
                    placement: Some(placement),
                },
                pace: None,
            };
        }
        let mut pace = None;
        if let CellOutcome::Completed { .. } = &outcome {
            self.learn_resident_kills(db.clone(), &selected.executor_id, &profile_context)
                .await;
        }
        if let CellOutcome::Completed {
            output,
            metadata,
            exit_code,
            timed_out,
            ..
        } = &outcome
        {
            // Dated once for every item: a batch runs one commit.
            let committed_at_unix_ms = Box::pin(resource_profiles::source_committed_at(
                source_store.as_deref(),
                &admission_request.base_commit,
            ))
            .await;
            let source = |began_on_the_cell| resource_profiles::CompletedSource {
                committed_at_unix_ms,
                began_on_the_cell,
            };
            // The batch's shape decides, not whether its items carry
            // identities: an agent's batch has none, and its items' outcomes
            // (their own stops included) live only in the serialized output.
            if batch_suites.is_empty() {
                if let Some(identity) = profile_identity.as_ref() {
                    record_command_outcome(
                        db.clone(),
                        identity,
                        &profile_context.os,
                        &selected.executor_id,
                        reliability::CommandOutcome::classify(
                            *exit_code,
                            *timed_out,
                            metadata.stop.as_ref(),
                            crate::execution::check_parsers::declared_empty_selection(output),
                        ),
                        metadata.duration_ms,
                        metadata.finished_at_unix_ms,
                    )
                    .await;
                }
                resource_profiles::observe_memory_kill(
                    db.clone(),
                    profile_identity.as_ref(),
                    &profile_context,
                    metadata,
                )
                .await;
                let kind = resource_profiles::CompletionKind::classify(
                    *exit_code == Some(0) && !*timed_out,
                    output,
                );
                resource_profiles::observe_suite(
                    db.clone(),
                    request_suite.as_deref(),
                    admission_request.command_class,
                    &profile_context,
                    metadata,
                    kind,
                    self.machine_admission(&selected.executor_id, 0),
                    source(true),
                )
                .await;
                resource_profiles::observe_completed(
                    db,
                    profile_identity.as_ref(),
                    &profile_context,
                    metadata,
                    kind,
                    source(true),
                )
                .await;
            } else if let Ok(items) = serde_json::from_str::<
                Vec<cairn_common::executor_protocol::ProcessBatchItemOutcome>,
            >(output)
            {
                // Read before any item is learned from, so no item is measured
                // against a profile its own run has already moved.
                let mut expected_ms = Vec::with_capacity(items.len());
                for (identity, item) in batch_profile_identities.iter().zip(&items) {
                    expected_ms.push(match identity {
                        Some(identity) => {
                            resource_profiles::learned_expectation(
                                db.clone(),
                                identity,
                                &profile_context,
                                &metadata.for_batch_item(item),
                            )
                            .await
                        }
                        None => None,
                    });
                }
                if batch_profile_identities.iter().any(Option::is_some) {
                    pace = Some(BatchPace {
                        lowered: selected.colocated,
                        expected_ms,
                    });
                }
                // Every item teaches its suite, with or without a command
                // identity: an agent's batch carries none, and its suite is
                // the only place its largest process can be learned.
                let machine = self.machine_admission(
                    &selected.executor_id,
                    if batch_items_share_the_machine {
                        items.len().saturating_sub(1) as u32
                    } else {
                        0
                    },
                );
                // Only the first item of a batch that runs its items in turn
                // began on the cell its warmth describes; any other began on
                // what the items before or beside it built.
                let began_on_the_cell = |index: usize| index == 0 && !batch_items_share_the_machine;
                for (index, item) in items.iter().enumerate() {
                    if item.suspended {
                        continue;
                    }
                    resource_profiles::observe_suite(
                        db.clone(),
                        batch_suites.get(index).and_then(|suite| suite.as_deref()),
                        batch_classes[index],
                        &profile_context,
                        &metadata.for_batch_item(item),
                        resource_profiles::CompletionKind::classify(
                            item.succeeded && !item.timed_out,
                            &item.body,
                        ),
                        machine,
                        source(began_on_the_cell(index)),
                    )
                    .await;
                }
                // Index-aligned with the items, so each item teaches only its
                // own identity; an item with none teaches only its suite.
                for (index, (identity, item)) in
                    batch_profile_identities.iter().zip(items).enumerate()
                {
                    let Some(identity) = identity else {
                        continue;
                    };
                    let item_meta = metadata.for_batch_item(&item);
                    // An item the batch never ran to an end (handed off as
                    // suspended) says nothing about how the machine ends work.
                    if !item.suspended {
                        record_command_outcome(
                            db.clone(),
                            identity,
                            &profile_context.os,
                            &selected.executor_id,
                            reliability::CommandOutcome::classify(
                                item.exit_code,
                                item.timed_out,
                                item.stop.as_ref(),
                                crate::execution::check_parsers::declared_empty_selection(
                                    &item.body,
                                ),
                            ),
                            Some(item.duration_ms),
                            item.finished_at_unix_ms,
                        )
                        .await;
                    }
                    let kind = resource_profiles::CompletionKind::classify(
                        item.succeeded && !item.timed_out && !item.suspended,
                        &item.body,
                    );
                    resource_profiles::observe_memory_kill(
                        db.clone(),
                        Some(identity),
                        &profile_context,
                        &item_meta,
                    )
                    .await;
                    resource_profiles::observe_completed(
                        db.clone(),
                        Some(identity),
                        &profile_context,
                        &item_meta,
                        kind,
                        source(began_on_the_cell(index)),
                    )
                    .await;
                }
            }
        }
        guard.disarm();
        ExecutedRequest {
            outcome,
            grant_without_result,
            abandoned_grant,
            above_ceiling: claimed_above_ceiling,
            account: CellAccount {
                left_running: None,
                cancel_cause: None,
                placement: Some(placement),
            },
            pace,
        }
    }

    /// What the machine holding `key`'s claim says about it now.
    ///
    /// `None` once the entry has left the queue: its outcome is on its way to
    /// the waiter, and that says nothing about the machine. A claim that went
    /// back to the queue after its grant has no machine answering for it, which
    /// reads the same as a link that is down.
    pub(super) fn claimed_attempt_account(
        &self,
        key: &RequestIdentity,
        now_unix_ms: u64,
    ) -> Option<ClaimedAttemptAccount> {
        let binding = self
            .queue
            .with_entry_by_identity(key, |entry| match &entry.state {
                queue::EntryState::Claimed {
                    executor_id,
                    generation,
                    ..
                } => Some((executor_id.clone(), *generation)),
                _ => None,
            })?;
        let Some((executor_id, generation)) = binding else {
            return Some(ClaimedAttemptAccount::LinkDown);
        };
        let connections = self.lock_connections();
        let Some(entry) = connections
            .get(&executor_id)
            .filter(|entry| entry.generation == generation)
        else {
            return Some(ClaimedAttemptAccount::LinkDown);
        };
        Some(account_of_claimed_attempt(
            &entry.snapshot,
            link_silence_ms(entry, now_unix_ms).is_some(),
            key,
        ))
    }

    /// Wait for a granted attempt's result for as long as its machine accounts
    /// for it.
    ///
    /// Progress is the machine's own account: connected, reporting, and listing
    /// the attempt as queued, held, or executing. That covers every phase a
    /// granted attempt passes through on the machine, including the ones with
    /// no process (cell acquisition, checkout materialization, the gap between
    /// batch items), because the executor holds a grant from the moment it
    /// handles it until its result is queued. A command that prints nothing for
    /// an hour is still progress: whether it is hung is the item's own timeout
    /// or an explicit cancel to decide, never this wait.
    pub(super) async fn await_claimed_result(
        &self,
        key: &RequestIdentity,
        granted_at_unix_ms: u64,
        mut rx: oneshot::Receiver<CellOutcome>,
    ) -> ClaimedWait {
        let mut watch = ClaimedAttemptWatch::granted_at(granted_at_unix_ms);
        loop {
            let bound_ms = self.claimed_attempt_silence.get();
            let now = unix_time_ms();
            let silence = self
                .claimed_attempt_account(key, now)
                .and_then(|account| watch.observe(account, now, bound_ms));
            if let Some(silence) = silence {
                // A result that landed between the read and the verdict wins.
                return match rx.try_recv() {
                    Ok(outcome) => ClaimedWait::Answered(Box::new(outcome)),
                    Err(oneshot::error::TryRecvError::Closed) => ClaimedWait::ChannelClosed,
                    Err(oneshot::error::TryRecvError::Empty) => ClaimedWait::Silent(silence),
                };
            }
            let poll = Duration::from_millis((bound_ms / 8).clamp(10, CLAIMED_ATTEMPT_POLL_MS));
            tokio::select! {
                biased;
                result = &mut rx => {
                    return match result {
                        Ok(outcome) => ClaimedWait::Answered(Box::new(outcome)),
                        Err(_) => ClaimedWait::ChannelClosed,
                    };
                }
                _ = tokio::time::sleep(poll) => {}
            }
        }
    }

    pub(crate) fn claimed_executor_of(&self, request_id: &str) -> Option<String> {
        let (attempt_id, _, _) = self.queue.attempt_binding_of(request_id)?;
        self.queue
            .claimed_binding_of(&(request_id.to_string(), attempt_id))
            .map(|(executor_id, _)| executor_id)
    }

    /// The generation of the link a machine is attached on now.
    pub(crate) fn link_generation(&self, executor_id: &str) -> Option<u64> {
        self.lock_connections()
            .get(executor_id)
            .map(|entry| entry.generation)
    }

    /// Tell a machine the runner holds one of its results, on whatever link it
    /// is attached on now. A machine not attached keeps the result and sends it
    /// again when it is, and the decision is simply made again.
    #[cfg(test)]
    pub(crate) fn set_result_ack_probe_for_test(&self, probe: ResultAckProbe) {
        *self.result_ack_probe.lock().unwrap() = Some(probe);
    }

    pub(crate) fn send_result_ack(&self, executor_id: &str, request_id: &str, attempt_id: &str) {
        let Some(generation) = self.link_generation(executor_id) else {
            return;
        };
        #[cfg(test)]
        if let Some(probe) = self.result_ack_probe.lock().unwrap().clone() {
            probe(request_id, attempt_id);
        }
        if let Err(error) = self.send_to(
            executor_id,
            generation,
            ExecutorMessage::ResultAck {
                request_id: request_id.to_string(),
                attempt_id: attempt_id.to_string(),
            },
        ) {
            log::info!("could not acknowledge request {request_id} attempt {attempt_id} to executor_id={executor_id}: {error}; it will resend");
        }
    }

    /// A result settled into its waiter in this process: acknowledge it now,
    /// unless it belongs to a run batch whose settlement acknowledges it.
    pub(super) fn acknowledge_settled_result(&self, executor_id: &str, key: &RequestIdentity) {
        {
            // Keep the exact claim and its ACK fence atomic with cancellation's
            // partition of request-scoped deferrals.
            let state = self.check_adoptions.lock().unwrap();
            let mut deferred = self.deferred_acks.lock().unwrap();
            if state.claims.contains_key(key) {
                deferred.entry(key.0.clone()).or_default();
            }
            if let Some(delivered) = deferred.get_mut(&key.0) {
                let attempt = (key.1.clone(), executor_id.to_string());
                if !delivered.contains(&attempt) {
                    delivered.push(attempt);
                }
                return;
            }
        }
        self.send_result_ack(executor_id, &key.0, &key.1);
    }

    /// Acknowledge this run batch's results only when the returned guard is
    /// dropped, rather than when they settle into its waiter.
    pub(crate) fn defer_result_ack(&self, request_id: &str) -> DeferredResultAck {
        self.deferred_acks
            .lock()
            .unwrap()
            .entry(request_id.to_string())
            .or_default();
        DeferredResultAck {
            fleet: self.clone(),
            request_id: request_id.to_string(),
            retain: false,
        }
    }

    /// Hand the orchestrator its adoption queue, with every result that
    /// arrived before it could decide anything.
    pub(crate) fn install_unbound_result_sink(&self, sink: mpsc::UnboundedSender<UnboundResult>) {
        let mut results = self.unbound_results.lock().unwrap();
        for result in std::mem::take(&mut results.pending) {
            let _ = sink.send(result);
        }
        results.sink = Some(sink);
    }

    pub(super) fn offer_unbound_result(&self, result: UnboundResult) {
        let mut results = self.unbound_results.lock().unwrap();
        let result = match results.sink.as_ref() {
            Some(sink) => match sink.send(result) {
                Ok(()) => return,
                Err(mpsc::error::SendError(result)) => result,
            },
            None => result,
        };
        results.sink = None;
        results.pending.push(result);
    }

    /// Ask a machine to re-offer an attempt's transfers on its current link.
    /// The caller authorizes them on that generation first.
    pub(crate) fn reauthorize_transfers(
        &self,
        executor_id: &str,
        generation: u64,
        request_id: &str,
        attempt_id: &str,
    ) -> Result<oneshot::Receiver<Vec<cairn_common::executor_protocol::ReauthorizedTransfer>>, String>
    {
        let key = (request_id.to_string(), attempt_id.to_string());
        let (tx, rx) = oneshot::channel();
        self.reauthorizations
            .lock()
            .unwrap()
            .insert(key.clone(), tx);
        if let Err(error) = self.send_to(
            executor_id,
            generation,
            ExecutorMessage::ReauthorizeTransfers {
                request_id: key.0.clone(),
                attempt_id: key.1.clone(),
            },
        ) {
            self.reauthorizations.lock().unwrap().remove(&key);
            return Err(error);
        }
        Ok(rx)
    }

    /// The requests earlier runner processes' durable rows still claim.
    pub(crate) fn set_adopting(&self, request_ids: HashSet<String>) {
        *self.adopting.lock().unwrap() = request_ids;
    }

    pub(super) fn send_to(
        &self,
        executor_id: &str,
        generation: u64,
        message: ExecutorMessage,
    ) -> Result<(), String> {
        let sender = self
            .lock_connections()
            .get(executor_id)
            .filter(|entry| entry.generation == generation)
            .map(|entry| entry.sender.clone())
            .ok_or_else(|| "executor is not connected at the selected generation".to_string())?;
        sender
            .send(message)
            .map_err(|_| "executor connection is closed".to_string())
    }

    /// Hold this executor's running work across a link gap.
    ///
    /// A link going away is a fact about a socket, not about the machine behind
    /// it. A `cairn-executor` keeps its cells running through a gap and dials
    /// back on its own, so the commands it is running are still running and
    /// their results are still coming. Resolving them the instant a transport
    /// blip lands is what made every blip cost whatever the machine was doing:
    /// one check lane died sixteen minutes into its run that way, and another
    /// died part-way through writing objects that a different node was then
    /// wedged without.
    ///
    /// Held work is bounded from three directions: the reattached connection
    /// accounts for it against its first snapshot
    /// ([`Fleet::reconcile_held_executions`]), the runner abandons it as soon as
    /// the machine's enrollment is gone ([`Fleet::abandon_held_executions`]),
    /// and each submitter's own silence watchdog remains underneath. Only the
    /// first two are evidence; the watchdog ends a submitter's wait, and the
    /// purpose debt behind it stays until evidence arrives.
    pub(super) fn hold_executions_across_link_gap(
        &self,
        executor_id: &str,
        held_claims: Vec<RequestIdentity>,
    ) -> usize {
        self.fail_link_calls_for_executor(
            executor_id,
            "executor connection closed before answering this call",
        );
        let held = held_claims.len();
        if held > 0 {
            log::info!(
                "holding {held} claimed execution(s) for executor_id={executor_id} across a link gap"
            );
        }
        held
    }

    /// Re-bind work held across a gap to the connection that came back.
    ///
    /// What identifies an execution is what it is on the machine, not the link
    /// that carried the order, so a result computed during the gap arrives on
    /// the new generation and has to be accepted there.
    pub(super) fn rebind_held_executions(
        &self,
        executor_id: &str,
        generation: u64,
    ) -> HashMap<RequestIdentity, queue::HeldClaim> {
        let rebound = self.queue.rebind(executor_id, generation);
        for pending in self.pending_residency.lock().unwrap().values_mut() {
            if pending.executor_id == executor_id && pending.granted_entry_id.as_deref().is_some_and(|entry_id|
                matches!(self.queue.state_of(entry_id), Some(queue::EntryState::Claimed { generation: bound, .. }) if bound == generation)) {
                pending.generation = generation;
            }
        }
        rebound
    }

    /// An ordered account after a witnessed grant can prove loss without a
    /// reconnect. The grant hold spans every start phase and retained result;
    /// serialized publication prevents a pre-start account overtaking it.
    pub(super) fn reconcile_witnessed_attempts(
        &self,
        executor_id: &str,
        generation: u64,
        witnessed: &HashSet<RequestIdentity>,
        snapshot: &FleetSnapshot,
    ) {
        let absent: HashSet<RequestIdentity> = witnessed
            .iter()
            .filter(|key| {
                !snapshot
                    .granted_attempts
                    .iter()
                    .any(|a| (&a.request_id, &a.attempt_id) == (&key.0, &key.1))
                    && !snapshot
                        .executing_requests
                        .iter()
                        .any(|a| (&a.request_id, &a.attempt_id) == (&key.0, &key.1))
                    && !snapshot
                        .queued_requests
                        .iter()
                        .any(|a| (&a.request_id, &a.attempt_id) == (&key.0, &key.1))
                    && !snapshot
                        .recent_completions
                        .iter()
                        .any(|a| (&a.request_id, &a.attempt_id) == (&key.0, &key.1))
            })
            .cloned()
            .collect();
        if absent.is_empty() {
            return;
        }
        let protected = self.protected_check_claims(executor_id);
        for key in &absent {
            self.account_witnessed_check_absence(executor_id, generation, key);
            self.queue.settle(
                key,
                Some((executor_id, generation)),
                queue::TerminalState::Completed,
                executor_unavailable(format!(
                    "executor {executor_id} no longer lists witnessed request {} attempt {} at generation {generation}; no result is coming",
                    key.0, key.1
                )),
                unix_time_ms(),
            );
        }
        let settled =
            self.purposes
                .lock()
                .unwrap()
                .settle_on_machine(executor_id, |key, placement| {
                    placement.generation == generation
                        && !placement.awaiting_post_boot_account
                        && absent.contains(key)
                        && !protected.contains(key)
                });
        self.note_purpose_settlements(
            &settled,
            "the machine's ordered same-generation report no longer lists its witnessed grant",
        );
        if let Some(entry) = self
            .lock_connections()
            .get_mut(executor_id)
            .filter(|entry| entry.generation == generation)
        {
            entry.witnessed_grants.retain(|key| !absent.contains(key));
        }
    }

    /// Settle work held across a gap against what the machine says it has.
    ///
    /// The first snapshot of the new connection is the machine's own account of
    /// what it is running. Anything held that does not appear in it did not
    /// survive — the process behind the link was replaced while the link was
    /// down — and is resolved now rather than left waiting for a result nobody
    /// is going to send.
    pub(super) fn reconcile_held_executions(
        &self,
        executor_id: &str,
        generation: u64,
        snapshot: &FleetSnapshot,
    ) {
        let held = {
            let mut carried = self.unreconciled_executions.lock().unwrap();
            match carried.get(executor_id) {
                Some((held_generation, _)) if *held_generation == generation => {
                    carried.remove(executor_id).map(|(_, keys)| keys)
                }
                _ => None,
            }
        };
        let Some(held) = held else { return };
        let identify =
            |request_id: &String, attempt_id: &String| (request_id.clone(), attempt_id.clone());
        let accounted: HashSet<RequestIdentity> = snapshot
            .granted_attempts
            .iter()
            .map(|attempt| identify(&attempt.request_id, &attempt.attempt_id))
            .chain(
                snapshot
                    .executing_requests
                    .iter()
                    .map(|request| identify(&request.request_id, &request.attempt_id)),
            )
            .chain(
                snapshot
                    .queued_requests
                    .iter()
                    .map(|request| identify(&request.request_id, &request.attempt_id)),
            )
            .chain(
                snapshot
                    .recent_completions
                    .iter()
                    .map(|completion| identify(&completion.request_id, &completion.attempt_id)),
            )
            .chain(
                snapshot
                    .in_flight_residency_operations
                    .keys()
                    .map(|request_id| {
                        (request_id.clone(), RESIDENCY_ACQUIRE_ATTEMPT_ID.to_owned())
                    }),
            )
            .chain(
                snapshot
                    .cells
                    .iter()
                    .filter_map(|cell| cell.residency.as_ref())
                    .map(|residency| {
                        (
                            residency_queue_entry_id(&residency.holder),
                            RESIDENCY_ACQUIRE_ATTEMPT_ID.to_owned(),
                        )
                    }),
            )
            .collect();
        let now = unix_time_ms();
        for (key, claim) in held.iter().filter(|(key, _)| !accounted.contains(*key)) {
            let settled = self.queue.settle(
                key,
                Some((executor_id, generation)),
                queue::TerminalState::Completed,
                executor_unavailable(format!(
                    "executor {executor_id}: reattached at generation {generation} without request {} attempt {}, \
                     which it claimed {}s earlier and was running at generation {}: it is no longer \
                     running or queued on that machine, so no result is coming",
                    key.0,
                    key.1,
                    now.saturating_sub(claim.claimed_at_unix_ms) / 1000,
                    claim.lost_generation,
                )),
                now,
            );
            if settled {
                log::warn!(
                    "executor_id={executor_id} reattached at generation {generation} without request {} attempt {} (running at generation {})",
                    key.0,
                    key.1,
                    claim.lost_generation,
                );
            }
        }
    }

    /// Give up on everything a machine holds, because its enrollment is gone.
    ///
    /// Called when an operator removes an enrolled remote or revokes its
    /// enrollment ([`management::enrollment_revoked`]), whether or not it is
    /// attached: a detached machine's held work has no live connection to
    /// end, and nothing else would ever answer it. Both revoke the machine's
    /// credential, so no result it might still produce can be accepted, and
    /// that is what makes its work gone by this runner's authority. Its
    /// purpose debts settle with it.
    pub fn abandon_held_executions(&self, executor_id: &str, diagnostic: &str) {
        self.unreconciled_executions
            .lock()
            .unwrap()
            .remove(executor_id);
        self.fail_executions_for_executor(executor_id, diagnostic);
    }

    /// Resolve everything this executor owed, because none of it is coming.
    pub(super) fn fail_for_executor(&self, executor_id: &str, diagnostic: &str) {
        self.fail_executions_for_executor(executor_id, diagnostic);
        self.fail_calls_for_executor(executor_id, diagnostic);
    }

    /// Resolve the cell executions this executor was running.
    ///
    /// Separate from the calls below because only this half has work behind it.
    /// An execution is a command that is running on a machine right now, and
    /// deciding it is lost is a decision about that machine's process — not one
    /// a socket is entitled to make on its own.
    pub(super) fn fail_executions_for_executor(&self, executor_id: &str, diagnostic: &str) {
        self.queue
            .fail_claims_for(executor_id, diagnostic, unix_time_ms());
        // The same decision settles the debts this machine holds, including
        // those whose attempts nothing waits on any more and the ones a
        // previous runner process left.
        self.settle_purpose_debts_for_lost_machine(executor_id, diagnostic);
    }

    /// Resolve the calls whose answers were in flight when the link went.
    ///
    /// A call has nothing running behind it worth preserving: it is a question
    /// that is cheap to ask again, and the answer it was waiting for described
    /// a moment that has already passed. These fail on any link loss.
    pub(super) fn fail_calls_for_executor(&self, executor_id: &str, diagnostic: &str) {
        self.fail_residency_calls(executor_id, diagnostic, false);
    }

    pub(super) fn fail_link_calls_for_executor(&self, executor_id: &str, diagnostic: &str) {
        self.fail_residency_calls(executor_id, diagnostic, true);
    }

    pub(super) fn fail_residency_calls(
        &self,
        executor_id: &str,
        diagnostic: &str,
        preserve_grants: bool,
    ) {
        let mut lifetime = self.pending_residency.lock().unwrap();
        let correlations: Vec<_> = lifetime
            .iter()
            .filter(|(_, entry)| {
                entry.executor_id == executor_id
                    && !(preserve_grants
                        && entry.granted_entry_id.as_deref().is_some_and(|entry_id| {
                            matches!(
                                self.queue.state_of(entry_id),
                                Some(queue::EntryState::Claimed { .. })
                            )
                        }))
            })
            .map(|(correlation, _)| correlation.clone())
            .collect();
        for correlation in correlations {
            if let Some(entry) = lifetime.remove(&correlation) {
                let _ = entry.waiter.send(residency_core_failure(
                    ResidencyFailureKind::Admission,
                    diagnostic.to_string(),
                    None,
                ));
            }
        }
        drop(lifetime);
        let mut reads = self.pending_materialization_reads.lock().unwrap();
        let correlations: Vec<_> = reads
            .iter()
            .filter(|(_, entry)| entry.executor_id == executor_id)
            .map(|(correlation, _)| correlation.clone())
            .collect();
        for correlation in correlations {
            if let Some(entry) = reads.remove(&correlation) {
                let _ = entry.waiter.send(MaterializationReadResult::Failed {
                    kind: MaterializationReadFailureKind::MaterializationUnavailable,
                    diagnostic: diagnostic.to_string(),
                });
            }
        }
    }
}

pub(super) fn restamp_outcome(outcome: &CellOutcome, identity: &RequestIdentity) -> CellOutcome {
    match outcome {
        CellOutcome::Completed {
            exit_code,
            output,
            timed_out,
            metadata,
            mutation_delta,
            sandbox_denials,
            tracked_modifications,
            ..
        } => CellOutcome::Completed {
            request_id: identity.0.clone(),
            attempt_id: identity.1.clone(),
            exit_code: *exit_code,
            output: output.clone(),
            timed_out: *timed_out,
            metadata: metadata.clone(),
            mutation_delta: mutation_delta.clone(),
            sandbox_denials: sandbox_denials.clone(),
            tracked_modifications: tracked_modifications.clone(),
            node_scratch: None,
        },
        CellOutcome::FailedAfterExecution { diagnostic, .. } => CellOutcome::FailedAfterExecution {
            request_id: identity.0.clone(),
            attempt_id: identity.1.clone(),
            diagnostic: diagnostic.clone(),
        },
        CellOutcome::StorageFailure {
            stage,
            kind,
            diagnostic,
            slot_retired,
            ..
        } => CellOutcome::StorageFailure {
            request_id: identity.0.clone(),
            attempt_id: identity.1.clone(),
            stage: *stage,
            kind: *kind,
            diagnostic: diagnostic.clone(),
            slot_retired: *slot_retired,
        },
        CellOutcome::Cancelled { .. } => CellOutcome::Cancelled {
            request_id: identity.0.clone(),
            attempt_id: identity.1.clone(),
        },
        CellOutcome::Unavailable { reason, diagnostic } => CellOutcome::Unavailable {
            reason: reason.clone(),
            diagnostic: diagnostic.clone(),
        },
    }
}

pub(super) fn selected_link_dispatch_error(
    connections: &HashMap<String, ExecutorConnectionState>,
    selected: &SelectedExecutor,
    now_unix_ms: u64,
) -> Option<String> {
    let Some(current) = connections.get(&selected.executor_id) else {
        return Some(format!(
            "selected executor {} disconnected before residency dispatch (selected connection generation {})",
            selected.executor_id, selected.generation
        ));
    };
    if current.generation != selected.generation {
        return Some(format!(
            "selected executor {} reconnected before residency dispatch (selected connection generation {}, current generation {}); the old generation is fenced",
            selected.executor_id, selected.generation, current.generation
        ));
    }
    if current.sender.is_closed() {
        return Some(format!(
            "selected executor {} closed connection generation {} before residency dispatch",
            selected.executor_id, selected.generation
        ));
    }
    link_silence_ms(current, now_unix_ms).map(|silence_ms| {
        format!(
            "selected executor {} connection generation {} became stale before residency dispatch: no progress for {}ms (liveness bound {}ms)",
            selected.executor_id,
            selected.generation,
            silence_ms,
            EXECUTOR_PROGRESS_FRESHNESS_MS
        )
    })
}

/// Whether this batch runs JavaScript, and so needs the Cairn packages to travel
/// with it.
///
/// A direct item names its program, which is how an inline TypeScript item
/// arrives. A shell item carries its whole command line in `args` instead, so
/// `bun script.ts` written as a shell command has exactly the same import to
/// resolve; answering only the inline form would leave the shell form as a
/// second, worse answer to one question. The scan is deliberately generous --
/// a command that merely mentions a runtime carries a few unread kilobytes,
/// while a missed one is the failure this whole mechanism exists to remove.
pub(super) fn batch_runs_javascript(items: &[ProcessBatchItem]) -> bool {
    const RUNTIMES: [&str; 4] = ["bun", "bunx", "node", "npx"];
    items.iter().any(|item| match item.execution {
        ProcessBatchExecution::NativeShell => item.args.iter().any(|command| {
            command
                .split(|c: char| !c.is_alphanumeric())
                .any(|word| RUNTIMES.contains(&word))
        }),
        _ => RUNTIMES.contains(&item.program.as_str()),
    })
}

pub(super) fn serialize_process_batch(
    batch: ResolvedRunBatch,
    env: &[(String, String)],
    runner_context_id: String,
    sandbox_mode: ProcessSandboxMode,
    containment: Option<cairn_common::executor_protocol::WorkloadEnvelope>,
) -> Result<ProcessBatch, String> {
    let mut items = Vec::with_capacity(batch.resolved.len());
    for (index, (header, spec)) in batch.resolved.into_iter().enumerate() {
        let spec = spec.map_err(|error| format!("resolve process item {header}: {error}"))?;
        let (execution, program, args, stdin, timeout) = match spec {
            RunSpec::Shell {
                command,
                timeout,
                args: shell_args,
            } => (
                ProcessBatchExecution::NativeShell,
                String::new(),
                std::iter::once(command).chain(shell_args).collect(),
                None,
                timeout,
            ),
            RunSpec::Script {
                program,
                args,
                timeout,
                stdin,
            } => (ProcessBatchExecution::Direct, program, args, stdin, timeout),
            RunSpec::McpCall(_)
            | RunSpec::ResponseCall { .. }
            | RunSpec::ModelDoor { .. }
            | RunSpec::ReplSend { .. } => {
                return Err(format!(
                    "{header} is not process-backed and cannot use a build cell"
                ))
            }
        };
        items.push(ProcessBatchItem {
            header,
            stream_id: format!("{}:{index}", batch.tool_use_id),
            execution,
            program,
            args,
            env: env.to_vec(),
            stdin,
            // The one clamp: an omitted bound runs to completion under the batch
            // ceiling, an explicit one is honored up to it. This layer used to
            // apply no bound of its own while the socket above it applied a
            // smaller one, which is how a suite's output was lost.
            timeout_ms: crate::mcp::handlers::run::clamp_run_item_timeout_ms(timeout).into(),
            // An agent's batch is stopped by cancellation alone: its owner is
            // present to end it, and a quiet interactive command is normal.
            silence_bound_ms: 0,
            // Minted, where the command earns one, by the reservation plan
            // that both places and learns from the batch
            // (`resource_profiles::ad_hoc_identity`), not sent from here.
            command_resource_identity: None,
            verdict_environment_names: Vec::new(),
        });
    }
    // A batch that runs bun carries the version-matched SDK with it, so inline
    // TypeScript resolves `@cairn/sdk` on whichever machine takes the batch --
    // including a project with no JavaScript toolchain of its own. Batches that
    // run no JavaScript carry nothing, so shell, Python, and MATLAB work is not
    // taxed with runtime bytes it will never read.
    //
    // A runner that cannot produce its own SDK fails the batch here, naming the
    // installation, rather than letting bun report a missing project dependency
    // and sending the reader to look for a mistake in their own project.
    // Placement applies the same relation the executor enforces, so a request
    // pairing an envelope with a contradicting sandbox mode fails here with the
    // reason rather than travelling to a machine that will refuse it.
    if let Some(envelope) = &containment {
        envelope
            .matches_policy_mode(cairn_common::executor_protocol::PolicyMode::of(
                sandbox_mode,
            ))
            .map_err(|mismatch| {
                format!("this batch's containment requirement cannot describe it: {mismatch}")
            })?;
    }
    let runtime_packages = if batch_runs_javascript(&items) {
        crate::runtime::sdk_packages()
            .map_err(|error| format!("Cairn cannot provide @cairn/sdk to this batch: {error}"))?
    } else {
        Vec::new()
    };
    Ok(ProcessBatch {
        files: Vec::new(),
        runtime_packages,
        sequential: batch.originally_sequential,
        stop_on_error: batch.stop_on_error,
        sandbox_mode,
        // The evidence this batch was placed on travels with it, so the
        // executor can re-check it at the spawn seam instead of trusting an
        // admission decision that may have gone stale in between.
        containment,
        timeout_bases: Vec::new(),
        items,
        runner_context_id: Some(runner_context_id),
        execution_residency: batch.execution_residency,
        node_scratch: None,
    })
}

pub(super) fn outcome_matches(outcome: &CellOutcome, request_id: &str, attempt_id: &str) -> bool {
    match outcome {
        CellOutcome::Completed {
            request_id: r,
            attempt_id: a,
            ..
        }
        | CellOutcome::FailedAfterExecution {
            request_id: r,
            attempt_id: a,
            ..
        }
        | CellOutcome::StorageFailure {
            request_id: r,
            attempt_id: a,
            ..
        }
        | CellOutcome::Cancelled {
            request_id: r,
            attempt_id: a,
        } => r == request_id && a == attempt_id,
        CellOutcome::Unavailable { .. } => true,
    }
}

/// How long a claimed attempt may go without its machine accounting for it
/// before the runner stops waiting for its result.
///
/// Not a budget on the work. A claimed attempt is waited on for as long as the
/// machine holding it is connected, reporting, and still lists it as queued,
/// held, or executing, however long that takes; only the absence of that
/// account is timed. Elapsed time since dispatch never ends a request: check
/// items carry the unbounded-process sentinel precisely so that it cannot, and
/// a wait summed from item timeouts and phase budgets reduced every check lane
/// to a fixed infrastructure allowance (cairn/4981).
///
/// Four times the link-stall remediation bound, so the mechanisms that know
/// more land first: a silent colocated link is recycled and its attempts
/// resolved, and a reattached machine's first snapshot settles what it lost.
/// This catches only what neither covers, such as a remote machine that never
/// returns, or a result frame lost with a dying socket while the completion is
/// still in the machine's history (cairn/4934).
pub(crate) const CLAIMED_ATTEMPT_SILENCE_BOUND_MS: u64 = 4 * EXECUTOR_LINK_STALL_REMEDIATION_MS;

/// The longest the claimed-attempt wait sleeps between reads of the machine's
/// account. A result wakes it at once; this paces only the silence check.
pub(super) const CLAIMED_ATTEMPT_POLL_MS: u64 = 5_000;

/// [`CLAIMED_ATTEMPT_SILENCE_BOUND_MS`] held as fleet state, so a test can
/// exercise the silence verdict without waiting minutes for it.
#[derive(Clone, Debug)]
pub(super) struct ClaimedAttemptSilenceBound(pub(super) Arc<AtomicU64>);

impl Default for ClaimedAttemptSilenceBound {
    fn default() -> Self {
        Self(Arc::new(AtomicU64::new(CLAIMED_ATTEMPT_SILENCE_BOUND_MS)))
    }
}

impl ClaimedAttemptSilenceBound {
    pub(super) fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// How a wait on a claimed attempt's result ended.
pub(super) enum ClaimedWait {
    Answered(Box<CellOutcome>),
    ChannelClosed,
    Silent(ClaimedAttemptSilence),
}

/// The runner's running read of one claimed attempt: when its machine last
/// accounted for it, and how.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ClaimedAttemptWatch {
    pub(super) last_progress_unix_ms: u64,
    pub(super) last_progress: Option<ClaimedAttemptAccount>,
}

/// A claimed attempt whose machine stopped accounting for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ClaimedAttemptSilence {
    pub(super) quiet_ms: u64,
    pub(super) last_progress: Option<ClaimedAttemptAccount>,
    pub(super) final_account: ClaimedAttemptAccount,
}

impl ClaimedAttemptWatch {
    /// The quiet clock starts at the grant: a machine that never lists what it
    /// was granted is as silent about it as one that stopped.
    pub(super) fn granted_at(granted_at_unix_ms: u64) -> Self {
        Self {
            last_progress_unix_ms: granted_at_unix_ms,
            last_progress: None,
        }
    }

    /// Fold in one reading of the machine's account, answering the silence
    /// once the account has been absent for `bound_ms`.
    pub(super) fn observe(
        &mut self,
        account: ClaimedAttemptAccount,
        now_unix_ms: u64,
        bound_ms: u64,
    ) -> Option<ClaimedAttemptSilence> {
        if account.is_progress() {
            self.last_progress_unix_ms = self.last_progress_unix_ms.max(now_unix_ms);
            self.last_progress = Some(account);
            return None;
        }
        let quiet_ms = now_unix_ms.saturating_sub(self.last_progress_unix_ms);
        (quiet_ms >= bound_ms).then_some(ClaimedAttemptSilence {
            quiet_ms,
            last_progress: self.last_progress,
            final_account: account,
        })
    }
}

impl ClaimedAttemptSilence {
    /// The outcome that ends the wait, saying why in the attempt's own terms:
    /// which machine, how long it was quiet, what it said last and what it says
    /// now. No substrate reading is taken at this instant, because the link's
    /// state at the moment of giving up is not what the wait was on.
    pub(super) fn outcome(&self, key: &RequestIdentity, executor_name: String) -> CellOutcome {
        let last = match self.last_progress {
            Some(account) => format!(
                "the last time it accounted for the attempt, it {}",
                account.describe()
            ),
            None => "it never listed the attempt after the grant".to_string(),
        };
        let diagnostic = format!(
            "{executor_name} gave no account of request {} attempt {} for {}: when the runner stopped waiting, the machine {}, and {last}. The runner waits on a claimed attempt for as long as its machine accounts for it, so this ended on that silence rather than on elapsed time; the attempt was cancelled and whether its commands ran is unknown",
            key.0,
            key.1,
            compact_duration(self.quiet_ms),
            self.final_account.describe(),
        );
        CellOutcome::Unavailable {
            reason: CellUnavailableReason::ClaimedAttemptSilent {
                executor_name,
                quiet_ms: self.quiet_ms,
                last_progress: self.last_progress,
                final_account: self.final_account,
            },
            diagnostic,
        }
    }
}

/// What one connection's latest posted state says about a claimed attempt.
///
/// `executingRequests` is checked first because it is the strongest statement,
/// then the executor's own queue, then `grantedAttempts`, which covers the
/// phases with no process: acquiring a cell, materializing a checkout, and the
/// gap between batch items. `recentCompletions` is not progress: the executor
/// writes a result ahead of any snapshot that drops the attempt from its
/// grants, so a completion the runner still has no result for is a result that
/// was lost.
pub(super) fn account_of_claimed_attempt(
    snapshot: &FleetSnapshot,
    link_silent: bool,
    key: &RequestIdentity,
) -> ClaimedAttemptAccount {
    if link_silent {
        return ClaimedAttemptAccount::LinkSilent;
    }
    let names = |request_id: &str, attempt_id: &str| request_id == key.0 && attempt_id == key.1;
    if snapshot
        .executing_requests
        .iter()
        .any(|request| names(&request.request_id, &request.attempt_id))
    {
        ClaimedAttemptAccount::Executing
    } else if snapshot
        .queued_requests
        .iter()
        .any(|request| names(&request.request_id, &request.attempt_id))
    {
        ClaimedAttemptAccount::Queued
    } else if snapshot
        .granted_attempts
        .iter()
        .any(|attempt| names(&attempt.request_id, &attempt.attempt_id))
    {
        ClaimedAttemptAccount::Held
    } else if snapshot
        .recent_completions
        .iter()
        .any(|completion| names(&completion.request_id, &completion.attempt_id))
    {
        ClaimedAttemptAccount::CompletedWithoutResult
    } else {
        ClaimedAttemptAccount::NotListed
    }
}

/// A request this runner granted to an executor that produced no result for it.
///
/// After a grant, an outcome without a result (a machine that stopped
/// accounting for the attempt, a result channel that closed, a machine declared
/// gone with the claim on it, or a refusal the executor sent) cannot say
/// nothing ran. This is what can be said in its place.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GrantWithoutResult {
    pub(crate) executor_name: String,
    pub(crate) granted_at_unix_ms: u64,
    /// How many times that machine's connection ended while it held the claim.
    /// `None` when the request had already left the queue, which is the only
    /// place that history lives.
    pub(crate) connection_drops: Option<usize>,
}

/// A request's outcome, and whether the runner rather than the executor
/// answered a grant it had delivered.
pub(crate) struct ExecutedRequest {
    pub(crate) outcome: CellOutcome,
    pub(crate) grant_without_result: Option<GrantWithoutResult>,
    /// Why the runner stopped waiting on a grant it had delivered without the
    /// machine answering: a silent machine, a closed result channel, or this
    /// runner shutting down. The work may still be running, so this is never
    /// evidence that it ended.
    pub(crate) abandoned_grant: Option<String>,
    /// Set when a machine claimed this work above its memory ceiling, because
    /// no usable machine could hold it.
    pub(crate) above_ceiling: Option<AboveMemoryCeiling>,
    /// What the runner knows about how the request ended beyond its outcome.
    pub(crate) account: CellAccount,
    /// For a completed batch, how its items stood against what their machine
    /// had learned to expect of them.
    pub(crate) pace: Option<BatchPace>,
}

/// What a machine had learned to expect of a completed batch's items, read
/// before the batch was learned from, so its result can say when an item ran
/// far past it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BatchPace {
    /// The machine ran the batch at lowered priority behind its own
    /// default-priority work, as a colocated executor runs everything it
    /// spawns for a request.
    pub lowered: bool,
    /// Index-aligned with the batch's items: the duration the machine's
    /// learned speed predicted for each, priced for the co-resident work and
    /// stall it began under (`resource_profiles::learned_expectation`), or
    /// `None` where nothing is learned.
    pub expected_ms: Vec<Option<u64>>,
}

/// An outcome reached before any grant was delivered.
impl From<CellOutcome> for ExecutedRequest {
    fn from(outcome: CellOutcome) -> Self {
        Self {
            outcome,
            grant_without_result: None,
            abandoned_grant: None,
            above_ceiling: None,
            account: CellAccount::default(),
            pace: None,
        }
    }
}

/// Whether a granted request ended without a result, leaving open whether any
/// of its commands ran.
///
/// Once a grant is delivered, only a completion, a cancellation, or a failure
/// the executor reports as after execution says what happened. Every other
/// outcome leaves it open, whoever composed it. The runner composes `Deadline`
/// and `ExecutorUnavailable` itself when it stops hearing about claimed work.
/// The executor's own refusals are not confined to before the first spawn
/// either: a sequential batch whose first item ran comes back as `Spawn` when
/// a later item, or the cache checkpoint after a finished item, fails, and a
/// storage failure can come from sealing a command or uploading its delta. So
/// all of them carry the grant. For a refusal that really was pre-spawn, that
/// costs the requester a needless look at its tree; the reverse would cost it a
/// false "nothing ran".
pub(super) fn grant_left_without_result(outcome: &CellOutcome) -> bool {
    matches!(
        outcome,
        CellOutcome::Unavailable { .. } | CellOutcome::StorageFailure { .. }
    )
}

pub(super) fn executor_unavailable(diagnostic: String) -> CellOutcome {
    CellOutcome::Unavailable {
        reason: CellUnavailableReason::ExecutorUnavailable,
        diagnostic,
    }
}

/// The answer a request gets when this runner withdraws it because the runner
/// itself is shutting down.
pub(super) fn runner_stopping(stopped_at_unix_ms: u64, diagnostic: String) -> CellOutcome {
    CellOutcome::Unavailable {
        reason: CellUnavailableReason::RunnerStopping { stopped_at_unix_ms },
        diagnostic,
    }
}

pub(super) fn describe_runner_stop(stopped_at_unix_ms: u64) -> String {
    crate::clock::stamp_millis_with_seconds(stopped_at_unix_ms as i64)
        .unwrap_or_else(|| format!("unix ms {stopped_at_unix_ms}"))
}

/// A batch's level-triggered stop, shared with preparation and placement.
/// Waiting after a stop observes it immediately; no wakeup can be missed.
pub(crate) struct PlacementStop {
    stopped: tokio::sync::watch::Sender<bool>,
}

impl Default for PlacementStop {
    fn default() -> Self {
        let (stopped, _) = tokio::sync::watch::channel(false);
        Self { stopped }
    }
}

impl PlacementStop {
    pub(crate) fn stop(&self) {
        self.stopped.send_replace(true);
    }

    pub(crate) fn is_stopped(&self) -> bool {
        *self.stopped.borrow()
    }

    pub(crate) fn stopped(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let mut observed = self.stopped.subscribe();
            let _ = observed.wait_for(|stopped| *stopped).await;
        })
    }

    /// Only for waits whose drop guards can end them. Dropping a residency
    /// acquisition releases its own queue and route guards, not another caller's.
    /// A granted command batch must instead await its machine's terminal report.
    pub(crate) async fn until<T>(&self, work: impl std::future::Future<Output = T>) -> Option<T> {
        tokio::select! {
            biased;
            () = self.stopped() => None,
            result = work => Some(result),
        }
    }
}

/// The moment this runner began shutting down, published once.
///
/// Level-triggered on purpose: a request that starts waiting after the stop
/// must see it at once, not wait for a wakeup that already happened.
pub(super) struct RunnerStop {
    pub(super) stopped_at: tokio::sync::watch::Sender<Option<u64>>,
}

impl Default for RunnerStop {
    fn default() -> Self {
        let (stopped_at, _) = tokio::sync::watch::channel(None);
        Self { stopped_at }
    }
}

impl RunnerStop {
    /// Record the stop. The first record stands; a second call changes nothing.
    pub(super) fn begin(&self, at_unix_ms: u64) {
        self.stopped_at.send_if_modified(|stopped_at| {
            if stopped_at.is_some() {
                return false;
            }
            *stopped_at = Some(at_unix_ms);
            true
        });
    }

    pub(super) fn at(&self) -> Option<u64> {
        *self.stopped_at.borrow()
    }

    /// Resolves with the stop's instant once the runner has begun stopping.
    pub(super) async fn stopped(&self) -> u64 {
        let mut observed = self.stopped_at.subscribe();
        let stopped_at = observed
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|at| *at);
        match stopped_at {
            Some(at) => at,
            // The sender lives as long as the fleet that owns it, so this is
            // unreachable while anything can still be waiting on it.
            None => std::future::pending().await,
        }
    }
}

/// Prefix a remote object-materialization refusal with the placement it came
/// from, leaving the executor's own coordinate and the low-level cause intact
/// as the tail. Every other outcome passes through untouched: this narrows a
/// diagnostic, it never reclassifies one.
pub(super) fn name_placement_in_object_refusal(
    outcome: CellOutcome,
    executor_id: &str,
    generation: u64,
) -> CellOutcome {
    match outcome {
        CellOutcome::Unavailable {
            reason: CellUnavailableReason::ObjectInfrastructure(stage),
            diagnostic,
        } => CellOutcome::Unavailable {
            reason: CellUnavailableReason::ObjectInfrastructure(stage),
            diagnostic: format!("on executor {executor_id} generation {generation}: {diagnostic}"),
        },
        other => other,
    }
}
