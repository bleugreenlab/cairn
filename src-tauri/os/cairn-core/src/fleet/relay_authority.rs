use super::*;

#[derive(Clone)]
pub(super) struct RunnerCallbackContext {
    pub(super) request: Option<crate::mcp::types::McpCallbackRequest>,
    pub(super) run_context: Option<crate::mcp::handlers::RunContext>,
    pub(super) check_status_board: Option<crate::execution::checks::CheckStatusBoard>,
    /// The request of the agent run batch this context serves, whose live
    /// control hears its per-item start and completion reports
    /// ([`crate::mcp::handlers::run::detached::note_item_started`]).
    pub(super) run_batch_request_id: Option<String>,
    /// Whether this batch runs in the project's externally owned live checkout.
    ///
    /// Stated from the submitted repository locator, never inferred from the
    /// filesystem: a cell checkout is a plain detached git checkout carrying no
    /// `.jj` marker, so a marker test calls every cell the live checkout and
    /// answers a cell's sandbox denial with a read-only-checkout explanation
    /// instead of the fence prompt the agent's dial asked for.
    pub(super) live_checkout: bool,
    /// The only executor session allowed to exercise this short-lived capability.
    /// Populated after placement and before the process batch is submitted.
    pub(super) executor_binding: Option<RunnerContextExecutorBinding>,
    /// Streaming scrubbers for this batch's relayed output, one per item stream.
    ///
    /// Shared with the submitting future, which drains them at the batch's
    /// terminal outcome. It has to be shared rather than owned here, because
    /// this entry is often revoked *before* that future returns — the result
    /// message revokes the context on its way in — and a scrubber dropped
    /// without a flush silently truncates the tail of the stream.
    pub(super) output: output_guard::SharedRelayedOutput,
}

/// One live runner callback context and the registration that hides its
/// capability.
///
/// The key of this map *is* the batch's relay capability: the executor injects
/// it into every item as `CAIRN_MCP_SECRET`, so a batch that dumps its own
/// environment echoes it straight back. Registering it inside the entry means
/// its scrub lifetime is exactly its authorization lifetime — batch completion,
/// `revoke_runner_contexts_for_request`, a reattach that does not announce it
/// and the coalesced-leader guard all remove from this map, so none of them can
/// forget to release it. That is the CAIRN-3385 property held by construction
/// rather than by four call sites agreeing. A link close only detaches it: the
/// batch is still running on its executor (cairn/5631).
pub(super) struct RunnerContextEntry {
    pub(super) context: RunnerCallbackContext,
    /// Why this context closed, for a fence wait still holding a callback that
    /// arrived under it. Every removal path records what it observed before
    /// the entry goes; `Drop` records [`HandoffClosed::ContextDropped`] for a
    /// path that recorded nothing, so no waiter is left hanging and none is
    /// told a cause nobody measured. A link close records
    /// [`HandoffClosed::ExecutorLinkDropped`] for the waits parked then, whose
    /// answers have nowhere to go, and installs a fresh watch so a wait raised
    /// after the reattach is not withdrawn by that past close.
    pub(super) closed: tokio::sync::watch::Sender<Option<HandoffClosed>>,
    /// Held, never read: dropping it is the whole contract.
    pub(super) _capability: Option<crate::security::SecretGuard<'static>>,
}

impl Drop for RunnerContextEntry {
    fn drop(&mut self) {
        self.close(HandoffClosed::ContextDropped);
    }
}

impl RunnerContextEntry {
    /// Record why the context closed. The first cause recorded stands.
    pub(super) fn close(&self, cause: HandoffClosed) {
        self.closed.send_if_modified(|closed| {
            if closed.is_some() {
                return false;
            }
            *closed = Some(cause);
            true
        });
    }

    /// The link carrying this context closed while its batch runs on: release
    /// the waits parked on it now, and keep the context open for the next.
    fn detach(&mut self, at_unix_ms: u64) {
        let Some(binding) = self.context.executor_binding.as_mut() else {
            return;
        };
        binding.link = BatchLink::Detached {
            generation: binding.link.generation(),
            at_unix_ms,
        };
        self.close(HandoffClosed::ExecutorLinkDropped);
        self.closed = tokio::sync::watch::channel(None).0;
    }

    pub(super) fn new(
        runner_context_id: &str,
        batch_label: &str,
        context: RunnerCallbackContext,
    ) -> Self {
        use crate::security::{registry, SecretCategory, SecretId, SecretMaterial};

        // Non-secret metadata: the id reaches logs and detection reports, so it
        // names the batch and never the capability. Deriving it from the value
        // would publish the capability to exactly the places the registry exists
        // to keep it out of.
        //
        // The counter is what makes it *unique*, and that is load-bearing rather
        // than tidy. Registering an id that already exists takes a second hold
        // and REPLACES the stored forms, so two live contexts spelling one id
        // would leave the first capability unregistered-for while it is still
        // minted, injected, and authorized. Batch identity alone does not
        // guarantee distinctness — the coalesced path names a request with no
        // attempt — so the id carries a process-local sequence instead of
        // relying on callers never colliding.
        static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = SecretId::new(format!("batch-capability:{batch_label}:{sequence}"));
        let capability = match registry().register(
            id.clone(),
            SecretCategory::BatchCapability,
            "per-batch executor relay capability",
            SecretMaterial::from_string(runner_context_id.to_string()),
        ) {
            Ok(guard) => Some(guard),
            Err(error) => {
                // Loud: the capability is minted and sent either way, so a
                // producer that believes it registered and did not believes in a
                // guarantee it does not have.
                log::warn!(
                    "per-batch relay capability {id} was not registered for scrubbing: {error}"
                );
                None
            }
        };
        Self {
            context,
            closed: tokio::sync::watch::channel(None).0,
            _capability: capability,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RunnerContextExecutorBinding {
    pub(super) executor_id: String,
    pub(super) link: BatchLink,
    pub(super) request_id: String,
    pub(super) attempt_id: String,
}

/// Which link of its executor a batch's relay context answers on.
///
/// A batch is bound to its own life and its executor's identity, never to one
/// link session: the executor keeps running it across a link gap and names it
/// again on its next link. So a link close only detaches the context, and it
/// ends when the batch does or when a reattach does not name it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BatchLink {
    /// Relayed calls are taken from this generation of the executor's link.
    Bound(u64),
    /// The link it was bound to closed at this moment, and no link has
    /// announced it since.
    Detached { generation: u64, at_unix_ms: u64 },
}

impl BatchLink {
    /// The link this context was last bound to.
    pub(super) fn generation(self) -> u64 {
        match self {
            Self::Bound(generation) | Self::Detached { generation, .. } => generation,
        }
    }
}

/// How many ended contexts the runner remembers the ending of, so a call on
/// one is refused with why it ended rather than as unknown.
pub(super) const CLOSED_RUNNER_CONTEXT_MEMORY: usize = 4_096;

/// Why a runner callback context ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ContextEnding {
    Settled,
    Cancelled,
    /// Its executor reattached on `generation` without announcing it.
    NotReannounced {
        executor: String,
        generation: u64,
    },
    /// It ended without its batch running or settling: the batch was never
    /// submitted, or its leader ended without an outcome.
    Ended,
}

/// The endings of the last [`CLOSED_RUNNER_CONTEXT_MEMORY`] contexts, oldest
/// forgotten first.
///
/// Keyed by the SHA-256 of the context id, never the id: the id is the batch's
/// capability, and its scrub registration ends with the context, so a raw id
/// kept here would outlive the guard that keeps it out of every output. Read
/// only to word a refusal; nothing is ever re-admitted from here.
#[derive(Default)]
pub(super) struct ClosedRunnerContexts {
    order: std::collections::VecDeque<[u8; 32]>,
    endings: HashMap<[u8; 32], (ContextEnding, u64)>,
}

impl ClosedRunnerContexts {
    pub(super) fn key(runner_context_id: &str) -> [u8; 32] {
        use sha2::Digest as _;
        sha2::Sha256::digest(runner_context_id.as_bytes()).into()
    }

    fn record(&mut self, runner_context_id: &str, ending: ContextEnding, at_unix_ms: u64) {
        let key = Self::key(runner_context_id);
        if self.endings.insert(key, (ending, at_unix_ms)).is_some() {
            return;
        }
        self.order.push_back(key);
        while self.order.len() > CLOSED_RUNNER_CONTEXT_MEMORY {
            if let Some(oldest) = self.order.pop_front() {
                self.endings.remove(&oldest);
            }
        }
    }

    fn ending(&self, runner_context_id: &str) -> Option<&(ContextEnding, u64)> {
        self.endings.get(&Self::key(runner_context_id))
    }

    #[cfg(test)]
    pub(super) fn keys(&self) -> impl Iterator<Item = &[u8; 32]> {
        self.endings.keys()
    }
}

fn rfc3339(unix_ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(unix_ms as i64).map_or_else(
        || format!("{unix_ms} ms"),
        |at| at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    )
}

impl Fleet {
    /// Authorize one MCP request relayed by an executor-local callback endpoint.
    ///
    /// The context token is a capability, but possession is not sufficient: it
    /// is bound to the executor session selected for the originating process
    /// batch, and the independently carried run identity must still match.
    pub fn authorize_mcp_relay(
        &self,
        executor_id: &str,
        generation: u64,
        runner_context_id: &str,
        request: &cairn_common::protocol::CallbackRequest,
    ) -> Result<(), String> {
        if request.thread_id.is_some() {
            return Err("relayed MCP requests cannot select a thread identity".into());
        }
        let contexts = self.runner_contexts.lock().unwrap();
        let Some(context) = contexts.get(runner_context_id).map(|entry| &entry.context) else {
            drop(contexts);
            return Err(self.describe_unheld_context(runner_context_id));
        };
        let binding = context.executor_binding.as_ref().ok_or_else(|| {
            "runner callback context is not bound to an active process batch".to_string()
        })?;
        if binding.executor_id != executor_id {
            return Err("runner callback context belongs to a different executor session".into());
        }
        match binding.link {
            BatchLink::Bound(bound) if bound == generation => {}
            BatchLink::Bound(_) => {
                return Err(
                    "runner callback context belongs to a different executor session".into(),
                )
            }
            BatchLink::Detached { at_unix_ms, .. } => {
                return Err(format!(
                    "relay detached by a link loss at {}; the executor has not reattached",
                    rfc3339(at_unix_ms)
                ))
            }
        }
        let expected_run_id = context
            .request
            .as_ref()
            .and_then(|request| request.run_id.as_deref())
            .or_else(|| {
                context
                    .run_context
                    .as_ref()
                    .map(|context| context.run_id.as_str())
            });
        if request.run_id.as_deref() != expected_run_id || expected_run_id.is_none() {
            return Err("relayed MCP request run identity does not match its process batch".into());
        }
        Ok(())
    }

    pub(super) fn bind_runner_context(
        &self,
        batch: Option<&ProcessBatch>,
        request: &CellRequest,
        executor_id: &str,
        generation: u64,
    ) -> Result<(), String> {
        let Some(context_id) = batch.and_then(|batch| batch.runner_context_id.as_deref()) else {
            return Ok(());
        };
        let mut contexts = self.runner_contexts.lock().unwrap();
        let context = contexts
            .get_mut(context_id)
            .map(|entry| &mut entry.context)
            .ok_or_else(|| "process batch names an unknown runner callback context".to_string())?;
        context.executor_binding = Some(RunnerContextExecutorBinding {
            executor_id: executor_id.to_string(),
            link: BatchLink::Bound(generation),
            request_id: request.request_id.clone(),
            attempt_id: request.attempt_id.clone(),
        });
        Ok(())
    }

    /// Why a relay or callback names a context this runner does not hold, as
    /// the refusal the caller reads.
    pub(super) fn describe_unheld_context(&self, runner_context_id: &str) -> String {
        let ledger = lock_through_poison(&self.closed_runner_contexts);
        match ledger.ending(runner_context_id) {
            Some((ContextEnding::Settled, at)) => format!(
                "the batch that held this relay settled at {}",
                rfc3339(*at)
            ),
            Some((ContextEnding::Cancelled, at)) => format!(
                "the batch that held this relay was cancelled at {}",
                rfc3339(*at)
            ),
            Some((
                ContextEnding::NotReannounced {
                    executor,
                    generation,
                },
                at,
            )) => format!(
                "relay revoked: executor {executor} reattached at {} (link {generation}) without announcing this batch",
                rfc3339(*at)
            ),
            Some((ContextEnding::Ended, at)) => format!(
                "the batch that held this relay ended at {} without settling",
                rfc3339(*at)
            ),
            None => format!(
                "unknown runner callback context: not issued by this runner process, or ended before the last {CLOSED_RUNNER_CONTEXT_MEMORY} closures it remembers"
            ),
        }
    }

    fn record_context_endings(&self, runner_context_ids: &[String], ending: &ContextEnding) {
        if runner_context_ids.is_empty() {
            return;
        }
        let at = unix_time_ms();
        let mut ledger = lock_through_poison(&self.closed_runner_contexts);
        for id in runner_context_ids {
            ledger.record(id, ending.clone(), at);
        }
    }

    /// Remove one context whose batch ended without running or settling. Safe
    /// in a destructor: it never panics on a poisoned lock.
    pub(super) fn end_runner_context(&self, runner_context_id: &str) {
        let removed = lock_through_poison(&self.runner_contexts).remove(runner_context_id);
        if removed.is_some() {
            self.record_context_endings(&[runner_context_id.to_string()], &ContextEnding::Ended);
        }
    }

    /// Admit a batch context bound to `generation` of `executor_id`, as a
    /// dispatched batch's would be, for tests that drive a link directly.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn admit_bound_batch_context_for_test(
        &self,
        runner_context_id: &str,
        executor_id: &str,
        generation: u64,
        request_id: &str,
        attempt_id: &str,
        run_id: &str,
    ) {
        self.runner_contexts.lock().unwrap().insert(
            runner_context_id.to_string(),
            RunnerContextEntry::new(
                runner_context_id,
                &format!("{request_id}:{attempt_id}"),
                RunnerCallbackContext {
                    request: Some(cairn_common::protocol::CallbackRequest {
                        run_id: Some(run_id.to_string()),
                        ..Default::default()
                    }),
                    run_context: None,
                    check_status_board: None,
                    run_batch_request_id: None,
                    live_checkout: false,
                    executor_binding: Some(RunnerContextExecutorBinding {
                        executor_id: executor_id.to_string(),
                        link: BatchLink::Bound(generation),
                        request_id: request_id.to_string(),
                        attempt_id: attempt_id.to_string(),
                    }),
                    output: output_guard::RelayedOutput::shared(),
                },
            ),
        );
    }

    /// Whether this runner still holds a callback context.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn holds_runner_context(&self, runner_context_id: &str) -> bool {
        self.runner_contexts
            .lock()
            .unwrap()
            .contains_key(runner_context_id)
    }

    /// Revoke the contexts of an attempt whose terminal result arrived.
    pub fn settle_runner_contexts_for_request(
        &self,
        request_id: &str,
        attempt_id: &str,
    ) -> Vec<String> {
        self.revoke_runner_contexts_for_request(request_id, attempt_id, HandoffClosed::BatchSettled)
    }

    /// Revoke the contexts of an attempt the runner is cancelling.
    pub fn cancel_runner_contexts_for_request(
        &self,
        request_id: &str,
        attempt_id: &str,
    ) -> Vec<String> {
        self.revoke_runner_contexts_for_request(
            request_id,
            attempt_id,
            HandoffClosed::BatchCancelled,
        )
    }

    pub(super) fn revoke_runner_contexts_for_request(
        &self,
        request_id: &str,
        attempt_id: &str,
        cause: HandoffClosed,
    ) -> Vec<String> {
        let mut revoked = Vec::new();
        self.runner_contexts.lock().unwrap().retain(|id, entry| {
            let matches = entry
                .context
                .executor_binding
                .as_ref()
                .is_some_and(|binding| {
                    binding.request_id == request_id && binding.attempt_id == attempt_id
                });
            if matches {
                entry.close(cause);
                revoked.push(id.clone());
            }
            !matches
        });
        let ending = match cause {
            HandoffClosed::BatchCancelled => ContextEnding::Cancelled,
            HandoffClosed::BatchSettled => ContextEnding::Settled,
            _ => ContextEnding::Ended,
        };
        self.record_context_endings(&revoked, &ending);
        revoked
    }

    /// Remove one context whose batch has settled.
    pub(super) fn settle_runner_context(&self, id: &str) {
        let removed = self.runner_contexts.lock().unwrap().remove(id);
        if let Some(entry) = removed {
            entry.close(HandoffClosed::BatchSettled);
            self.record_context_endings(&[id.to_string()], &ContextEnding::Settled);
        }
    }

    pub(crate) fn resident_relays(&self) -> MutexGuard<'_, resident_relay::ResidentRelays> {
        lock_through_poison(&self.resident_relays)
    }

    /// Link `generation` of `executor_id` closed. Every relay bound to it, a
    /// running batch's or a resident process's, waits for the executor to
    /// announce it on its next link: the work outlives the link.
    ///
    /// A context bound to a later link, an overlapping redial that reattached
    /// before this close was seen, is not touched.
    pub fn relay_link_closed(&self, executor_id: &str, generation: u64) {
        let at = unix_time_ms();
        for entry in self.runner_contexts.lock().unwrap().values_mut() {
            let bound_here = entry
                .context
                .executor_binding
                .as_ref()
                .is_some_and(|binding| {
                    binding.executor_id == executor_id
                        && binding.link == BatchLink::Bound(generation)
                });
            if bound_here {
                entry.detach(at);
            }
        }
        self.resident_relays().detach(executor_id, generation);
        // A link that dies mid-stream never delivers the exits that would have
        // drained its resident processes, so their scrubbers are dropped here
        // rather than left to accumulate.
        self.resident_output
            .lock()
            .unwrap()
            .forget_executor(executor_id);
    }

    /// The executor opened link `generation` naming the relays it still serves.
    ///
    /// One rule for both kinds. An announced relay this runner holds for that
    /// executor, for the same process or attempt, is bound to the new link,
    /// whether it was detached or still bound to an older link whose close
    /// has not been seen. A held relay for that executor that was not
    /// announced is gone from it and is revoked, unless it is already bound to
    /// this link: that one was dispatched after the executor composed the
    /// announcement. An announced relay this runner does not hold is ignored.
    ///
    /// Only the executor's current link may announce. A superseded link's
    /// connection task outlives an overlapping redial, so its announcement can
    /// arrive after the new link's; applied, it would bind live relays back to
    /// the dead link or revoke ones the new link has just started. It is
    /// dropped whole, and a batch's binding never moves to an older link even
    /// when no connection record says which link is current.
    pub fn reattach_relays(
        &self,
        executor_id: &str,
        generation: u64,
        relays: &[cairn_common::executor_protocol::RelayIdentity],
    ) {
        use cairn_common::executor_protocol::RelayIdentity;
        let current = self
            .lock_connections()
            .get(executor_id)
            .map(|connection| connection.generation);
        if current.is_some_and(|current| current != generation) {
            log::warn!(
                "ignored a relay announcement from superseded link {generation} of executor {executor_id}; its current link is {}",
                current.unwrap_or_default()
            );
            return;
        }
        self.resident_relays()
            .reattach(executor_id, generation, relays);
        let announced: HashMap<&str, (&str, &str)> = relays
            .iter()
            .filter_map(|relay| match relay {
                RelayIdentity::Batch {
                    runner_context_id,
                    request_id,
                    attempt_id,
                } => Some((
                    runner_context_id.as_str(),
                    (request_id.as_str(), attempt_id.as_str()),
                )),
                RelayIdentity::Resident { .. } => None,
            })
            .collect();
        let mut revoked = Vec::new();
        self.runner_contexts.lock().unwrap().retain(|id, entry| {
            let Some(binding) = entry.context.executor_binding.as_mut() else {
                return true;
            };
            if binding.executor_id != executor_id || binding.link.generation() > generation {
                return true;
            }
            let same_attempt =
                announced
                    .get(id.as_str())
                    .is_some_and(|(request_id, attempt_id)| {
                        binding.request_id == *request_id && binding.attempt_id == *attempt_id
                    });
            if same_attempt {
                binding.link = BatchLink::Bound(generation);
                return true;
            }
            if binding.link == BatchLink::Bound(generation) {
                return true;
            }
            entry.close(HandoffClosed::ExecutorDidNotReannounce);
            revoked.push(id.clone());
            false
        });
        if revoked.is_empty() {
            return;
        }
        let executor = self
            .executor_public_name(executor_id)
            .unwrap_or_else(|| executor_id.to_string());
        log::warn!(
            "executor {executor} reattached on link {generation} without announcing {} running batch relay(s); revoked",
            revoked.len()
        );
        self.record_context_endings(
            &revoked,
            &ContextEnding::NotReannounced {
                executor,
                generation,
            },
        );
    }

    #[cfg(test)]
    pub(super) fn closed_runner_context_keys(&self) -> Vec<[u8; 32]> {
        lock_through_poison(&self.closed_runner_contexts)
            .keys()
            .copied()
            .collect()
    }

    /// Scrub one resident-process event, and drain the process when it exits.
    ///
    /// Returns what should reach subscribers: usually the one event, scrubbed;
    /// on exit, the withheld tail of each of the process's streams *first*, so
    /// the last bytes land in the scrollback the exit is about to seal. An
    /// output event whose content was entirely withheld yields nothing at all,
    /// since a chunk holding no bytes is not yet a chunk.
    pub(super) fn settle_resident_event(
        &self,
        executor_id: &str,
        mut event: ResidentProcessEvent,
    ) -> Vec<ResidentProcessEvent> {
        let holder_key = event.holder.storage_key();
        match &mut event.event {
            ResidentProcessEventKind::Output { stream, data, .. } => {
                let settled = self.resident_output.lock().unwrap().settle_resident_bytes(
                    executor_id,
                    &holder_key,
                    &event.process_key,
                    event.process_generation,
                    *stream,
                    data,
                );
                if settled.is_empty() {
                    return Vec::new();
                }
                *data = settled;
                vec![event]
            }
            ResidentProcessEventKind::State {
                status: cairn_common::executor_protocol::ResidentProcessStatus::Exited { .. },
            } => {
                let tails = output_guard::finish_resident_process(
                    &self.resident_output,
                    executor_id,
                    &holder_key,
                    &event.process_key,
                    event.process_generation,
                );
                let mut sequence = u64::MAX - tails.len() as u64;
                let mut settled: Vec<ResidentProcessEvent> = tails
                    .into_iter()
                    .map(|(stream, data)| {
                        sequence += 1;
                        ResidentProcessEvent {
                            // The tail carries the exiting process's own
                            // identity, so a consumer keyed on the process does
                            // not have to special-case it.
                            event: ResidentProcessEventKind::Output {
                                sequence,
                                stream,
                                data,
                            },
                            ..event.clone()
                        }
                    })
                    .collect();
                settled.push(event);
                settled
            }
            ResidentProcessEventKind::State { .. } => vec![event],
        }
    }

    pub(super) fn end_relay_of_exited_process(
        &self,
        executor_id: &str,
        event: &ResidentProcessEvent,
    ) {
        if matches!(
            event.event,
            ResidentProcessEventKind::State {
                status: cairn_common::executor_protocol::ResidentProcessStatus::Exited { .. }
            }
        ) {
            self.resident_relays().process_exited(
                executor_id,
                &event.holder,
                &event.process_key,
                event.process_generation,
            );
        }
    }

    /// Whether a resident-process event still describes live work.
    ///
    /// The cached cell snapshot cannot answer that question by existence. A
    /// snapshot and a process event are two streams out of one executor with no
    /// ordering between them — the executor's protocol writer selects over its
    /// control channel and its event channel — so an event routinely reaches
    /// the runner before the snapshot that would vouch for it. Requiring the
    /// snapshot to already name the process at that generation made delivery a
    /// coin flip for processes whose whole life is shorter than that skew, and
    /// the event most often lost is the exit, which is the one a caller is
    /// parked on (CAIRN-3444).
    ///
    /// So the snapshot is consulted for what it can answer: contradiction, not
    /// existence. An event is refused when the runner knows something strictly
    /// newer at the same address — a later cell epoch for that holder, or a
    /// later generation at that process key — and admitted otherwise. Every
    /// subscriber matches the exact fence and generation it holds, so precision
    /// lives there; this gate exists to keep a superseded or foreign link's
    /// events from reaching any of them.
    pub(super) fn resident_event_is_current(
        &self,
        executor_id: &str,
        generation: u64,
        event: &ResidentProcessEvent,
    ) -> bool {
        let connections = self.lock_connections();
        let Some(connection) = connections
            .get(executor_id)
            .filter(|connection| connection.generation == generation)
        else {
            return false;
        };
        !connection.snapshot.cells.iter().any(|cell| {
            let holds_event = cell
                .residency
                .as_ref()
                .is_some_and(|residency| residency.holder == event.holder);
            if !holds_event {
                return false;
            }
            if cell.cell_epoch > event.cell_epoch {
                return true;
            }
            cell.cell_epoch == event.cell_epoch
                && cell
                    .occupancy
                    .processes
                    .get(&event.process_key)
                    .is_some_and(|process| process.generation > event.process_generation)
        })
    }

    pub(super) fn residency_silence_diagnostic(
        &self,
        executor_id: &str,
        generation: u64,
    ) -> String {
        let now = unix_time_ms();
        let connections = self.lock_connections();
        let Some(entry) = connections
            .get(executor_id)
            .filter(|entry| entry.generation == generation)
        else {
            return "residency operation lost its executor connection generation".into();
        };
        let report_silence_ms = now.saturating_sub(entry.last_progress_unix_ms);
        let now_ns = link_clock::monotonic_now_ns();
        if let Some(reader_silence_ms) = entry
            .link_clock
            .reader_silence_ms(now_ns)
            .filter(|silence| *silence > link_clock::READER_ABSENT_AFTER_MS)
        {
            let (phase, age_ms) = entry.link_clock.ingest_state(now_ns);
            format!(
                "runner socket reader stopped turning over ({reader_silence_ms}ms silent; ingest {} for {age_ms}ms)",
                phase.as_str()
            )
        } else if report_silence_ms > EXECUTOR_PROGRESS_FRESHNESS_MS {
            format!(
                "executor snapshot stream stopped reporting residency progress ({report_silence_ms}ms silent)"
            )
        } else {
            let executor = executor_public_name(entry);
            let phase = entry
                .snapshot
                .in_flight_residency_operations
                .values()
                .max_by_key(|evidence| evidence.last_progress_unix_ms)
                .map(|evidence| format!("{:?}", evidence.state))
                .unwrap_or_else(|| "no request-scoped phase".into());
            format!(
                "executor {executor} stopped publishing residency operation progress; last observed phase: {phase}"
            )
        }
    }

    pub(crate) fn subscribe_resident_process_events(
        &self,
        subscriber: impl Fn(ResidentProcessEvent) + Send + Sync + 'static,
    ) -> ResidentProcessSubscription {
        let mut subscribers = self.resident_process_subscribers.lock().unwrap();
        let id = subscribers.next_id;
        subscribers.next_id += 1;
        subscribers.entries.push((id, Arc::new(subscriber)));
        ResidentProcessSubscription {
            id,
            subscribers: Arc::downgrade(&self.resident_process_subscribers),
        }
    }

    /// Hand one event to every subscriber registered when it arrived.
    ///
    /// The registry is snapshotted and released before any subscriber runs, so a
    /// subscriber that drops a [`ResidentProcessSubscription`] cannot deadlock on
    /// it. Ordering per process is unchanged: one executor link ingests its
    /// messages one at a time.
    pub(super) fn publish_resident_process_event(&self, event: ResidentProcessEvent) {
        let subscribers: Vec<ResidentProcessSubscriber> = self
            .resident_process_subscribers
            .lock()
            .unwrap()
            .entries
            .iter()
            .map(|(_, subscriber)| subscriber.clone())
            .collect();
        for subscriber in subscribers {
            subscriber(event.clone());
        }
    }

    #[cfg(test)]
    pub(super) fn resident_process_subscriber_count(&self) -> usize {
        self.resident_process_subscribers
            .lock()
            .unwrap()
            .entries
            .len()
    }
}
