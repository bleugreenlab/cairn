//! Runner-local ownership of remote configured checks across a runner restart.
//! A terminal durable decision precedes cancellation or acknowledgement.
use super::*;
use crate::storage::{check_lane_adoptions as store, LocalDb};

/// How long a retained remote check result waits before retrying a durable
/// write the store refused. The result stays held (no acknowledgement, no
/// cancellation) until the same write commits. Test builds keep the interval
/// short so a recovered store is observed promptly rather than waited out.
#[cfg(not(test))]
pub(crate) const RETAINED_RESULT_RETRY: Duration = Duration::from_secs(5);
#[cfg(test)]
pub(crate) const RETAINED_RESULT_RETRY: Duration = Duration::from_millis(20);

#[derive(Default)]
pub(super) struct CheckAdoptions {
    pub(super) db: Option<Arc<LocalDb>>,
    pub(super) templates: HashMap<RequestIdentity, store::Claim>,
    pub(super) mutation: Arc<tokio::sync::Mutex<()>>,
    consumers: HashMap<RequestIdentity, Vec<store::Waiter>>,
    subscriber_consumers: HashMap<RequestIdentity, CheckConsumerIdentity>,
    consumer_guards: HashMap<CheckConsumerIdentity, std::sync::Weak<CheckConsumerGuard>>,
    pub(super) claims: HashMap<RequestIdentity, store::Claim>,
    held: HashMap<RequestIdentity, UnboundResult>,
    received: HashSet<RequestIdentity>,
    terminal: HashSet<RequestIdentity>,
    waiters: HashMap<RequestIdentity, Vec<oneshot::Sender<CellOutcome>>>,
    accounted: HashSet<RequestIdentity>,
    reloaded: HashSet<RequestIdentity>,
    losing: HashSet<RequestIdentity>,
    release_authorized: HashSet<RequestIdentity>,
    #[cfg(test)]
    settlement_retry_probe: Option<mpsc::UnboundedSender<(RequestIdentity, String)>>,
}

impl CheckAdoptions {
    fn forget_consumers(&mut self, identity: &RequestIdentity) {
        self.templates.remove(identity);
        self.consumers.remove(identity);
        self.consumer_guards
            .retain(|consumer, _| &consumer.physical != identity);
        self.subscriber_consumers
            .retain(|_, consumer| &consumer.physical != identity);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CheckConsumerIdentity {
    physical: RequestIdentity,
    check_name: String,
    consumer: String,
    consumer_ref: String,
}

/// Follows one live consumer from its result through projection and delivery.
/// The physical publication state is shared; this lifetime is not.
pub(crate) struct CheckConsumerGuard {
    fleet: Fleet,
    identity: CheckConsumerIdentity,
    settled: AtomicBool,
}

impl CheckConsumerGuard {
    pub(crate) async fn deliver(&self, observation_id: &str) -> Result<(), String> {
        self.fleet
            .deliver_check_consumer(
                &self.identity.physical,
                &self.identity.check_name,
                &self.identity.consumer,
                &self.identity.consumer_ref,
                observation_id,
            )
            .await?;
        self.settled.store(true, Ordering::Release);
        Ok(())
    }
}

impl std::fmt::Debug for CheckConsumerGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CheckConsumerGuard")
            .field(&self.identity)
            .finish()
    }
}

impl Drop for CheckConsumerGuard {
    fn drop(&mut self) {
        if self.settled.load(Ordering::Acquire) {
            return;
        }
        if lock_through_poison(&self.fleet.check_adoptions)
            .subscriber_consumers
            .values()
            .any(|identity| identity == &self.identity)
        {
            return;
        }
        if self.fleet.runner_stop.at().is_some()
            && self.fleet.adoption_owner(&self.identity.physical).is_some()
        {
            return;
        }
        let fleet = self.fleet.clone();
        let identity = self.identity.clone();
        let runtime = fleet
            .host_runtime
            .clone()
            .or_else(|| tokio::runtime::Handle::try_current().ok());
        crate::orchestrator::spawn_onto(
            runtime.as_ref(),
            async move {
                loop {
                    if fleet.runner_stop.at().is_some()
                        && fleet.adoption_owner(&identity.physical).is_some()
                    {
                        return;
                    }
                    match fleet.withdraw_check_consumer(&identity.physical, &identity.check_name, &identity.consumer,
                    &identity.consumer_ref, "live consumer dropped before its projection and delivery completed").await {
                    Ok(()) => return,
                    Err(error) => log::error!("check_lane_adoption_waiters exact consumer withdrawal failed: {error}; no acknowledgement is authorized; retrying"),
                }
                    tokio::time::sleep(RETAINED_RESULT_RETRY).await;
                }
            },
            || {
                log::error!("check consumer withdrawal retry could not start; its durable row remains unacknowledged")
            },
        );
    }
}

impl Fleet {
    pub(super) fn track_check_consumer(
        &self,
        subscriber: RequestIdentity,
        physical: RequestIdentity,
        check_name: &str,
        source: &store::Claim,
    ) {
        self.check_adoptions
            .lock()
            .unwrap()
            .subscriber_consumers
            .insert(
                subscriber,
                CheckConsumerIdentity {
                    physical,
                    check_name: check_name.into(),
                    consumer: source.consumer.clone(),
                    consumer_ref: source.consumer_ref.clone(),
                },
            );
    }

    pub(super) fn take_check_consumer_guard(
        &self,
        subscriber: &RequestIdentity,
    ) -> Option<Arc<CheckConsumerGuard>> {
        let mut state = lock_through_poison(&self.check_adoptions);
        let identity = state.subscriber_consumers.remove(subscriber)?;
        if let Some(guard) = state
            .consumer_guards
            .get(&identity)
            .and_then(std::sync::Weak::upgrade)
        {
            return Some(guard);
        }
        let guard = Arc::new(CheckConsumerGuard {
            fleet: self.clone(),
            identity: identity.clone(),
            settled: AtomicBool::new(false),
        });
        state
            .consumer_guards
            .insert(identity, Arc::downgrade(&guard));
        Some(guard)
    }

    pub(crate) fn check_consumer_guard_for(
        &self,
        identity: &RequestIdentity,
        check_name: &str,
        source: &store::Claim,
    ) -> Arc<CheckConsumerGuard> {
        let consumer = CheckConsumerIdentity {
            physical: identity.clone(),
            check_name: check_name.into(),
            consumer: source.consumer.clone(),
            consumer_ref: source.consumer_ref.clone(),
        };
        let mut state = self.check_adoptions.lock().unwrap();
        if let Some(guard) = state
            .consumer_guards
            .get(&consumer)
            .and_then(std::sync::Weak::upgrade)
        {
            return guard;
        }
        if state.templates.contains_key(identity) && !state.claims.contains_key(identity) {
            let consumers = state.consumers.entry(identity.clone()).or_default();
            if !consumers.iter().any(|waiter| {
                waiter.check_name == check_name
                    && waiter.consumer == source.consumer
                    && waiter.consumer_ref == source.consumer_ref
            }) {
                consumers.push(store::Waiter {
                    request_id: identity.0.clone(),
                    attempt_id: identity.1.clone(),
                    check_name: check_name.into(),
                    item_index: source
                        .check_names
                        .iter()
                        .position(|name| name == check_name)
                        .unwrap_or(0),
                    consumer: source.consumer.clone(),
                    consumer_ref: source.consumer_ref.clone(),
                    state: "running".into(),
                    evidence: None,
                    observation_id: None,
                    updated_ms: unix_time_ms() as i64,
                });
            }
        }
        let guard = Arc::new(CheckConsumerGuard {
            fleet: self.clone(),
            identity: consumer.clone(),
            settled: AtomicBool::new(false),
        });
        state
            .consumer_guards
            .insert(consumer, Arc::downgrade(&guard));
        guard
    }

    pub(crate) async fn withdraw_check_consumer(
        &self,
        identity: &RequestIdentity,
        check_name: &str,
        consumer: &str,
        consumer_ref: &str,
        evidence: &str,
    ) -> Result<(), String> {
        let (gate, db) = {
            let state = self.check_adoptions.lock().unwrap();
            (state.mutation.clone(), state.db.clone())
        };
        let Some(db) = db else {
            return Ok(());
        };
        let _mutation = gate.lock().await;
        let logical = CheckConsumerIdentity {
            physical: identity.clone(),
            check_name: check_name.into(),
            consumer: consumer.into(),
            consumer_ref: consumer_ref.into(),
        };
        let (registered, active) = {
            let state = self.check_adoptions.lock().unwrap();
            (
                state
                    .subscriber_consumers
                    .values()
                    .any(|candidate| candidate == &logical),
                state
                    .consumer_guards
                    .get(&logical)
                    .and_then(std::sync::Weak::upgrade),
            )
        };
        if registered || active.is_some() {
            return Ok(());
        }
        self.withdraw_check_consumer_locked(
            db,
            identity,
            check_name,
            consumer,
            consumer_ref,
            evidence,
        )
        .await
    }

    /// The caller owns the mutation gate and decides whether this is lifetime
    /// cleanup or an authoritative withdrawal of a superseded purpose.
    async fn withdraw_check_consumer_locked(
        &self,
        db: Arc<LocalDb>,
        identity: &RequestIdentity,
        check_name: &str,
        consumer: &str,
        consumer_ref: &str,
        evidence: &str,
    ) -> Result<(), String> {
        {
            let mut state = self.check_adoptions.lock().unwrap();
            if state.templates.contains_key(identity) && !state.claims.contains_key(identity) {
                if let Some(consumers) = state.consumers.get_mut(identity) {
                    for waiter in consumers.iter_mut().filter(|waiter| {
                        waiter.check_name == check_name
                            && waiter.consumer == consumer
                            && waiter.consumer_ref == consumer_ref
                    }) {
                        waiter.state = "cancelled".into();
                        waiter.evidence = Some(evidence.into());
                    }
                }
                return Ok(());
            }
        }
        let consumers = store::waiters(&db, &identity.0, &identity.1)
            .await
            .map_err(|error| error.to_string())?;
        let Some(waiter) = consumers.into_iter().find(|waiter| {
            waiter.check_name == check_name
                && waiter.consumer == consumer
                && waiter.consumer_ref == consumer_ref
        }) else {
            return Ok(());
        };
        let terminal = match waiter.state.as_str() {
            "recorded" => Some("recorded_undelivered"),
            "running" | "awaiting_adoption" | "joined" => Some("cancelled"),
            _ => None,
        };
        if let Some(terminal) = terminal {
            store::transition_waiter(&db, &waiter, terminal, Some(evidence), None)
                .await
                .map_err(|error| error.to_string())?;
        }
        let consumers = store::waiters(&db, &identity.0, &identity.1)
            .await
            .map_err(|error| error.to_string())?;
        self.check_adoptions
            .lock()
            .unwrap()
            .consumers
            .insert(identity.clone(), consumers);
        if let Some(claim) = store::lookup(&db, &identity.0, &identity.1)
            .await
            .map_err(|error| error.to_string())?
        {
            self.maybe_finish_check_locked(&db, &claim).await?;
        }
        Ok(())
    }

    pub(crate) fn register_check_claim(&self, identity: RequestIdentity, claim: store::Claim) {
        self.check_adoptions
            .lock()
            .unwrap()
            .templates
            .insert(identity, claim);
    }

    #[cfg(test)]
    pub(crate) fn remove_machine_for_check_adoption_test(&self, executor_id: &str, evidence: &str) {
        self.settle_purpose_debts_for_lost_machine(executor_id, evidence);
    }

    #[cfg(test)]
    pub(crate) fn has_pending_adopted_binder(&self, identity: &RequestIdentity) -> bool {
        self.check_adoptions
            .lock()
            .unwrap()
            .waiters
            .get(identity)
            .is_some_and(|waiters| waiters.iter().any(|waiter| !waiter.is_closed()))
    }

    pub(crate) fn is_check_adoption_reloaded(&self, identity: &RequestIdentity) -> bool {
        self.check_adoptions
            .lock()
            .unwrap()
            .reloaded
            .contains(identity)
    }

    pub(crate) fn check_claim(&self, identity: &RequestIdentity) -> Option<store::Claim> {
        self.check_adoptions
            .lock()
            .unwrap()
            .claims
            .get(identity)
            .cloned()
    }

    pub(crate) fn check_left_running(&self, identity: &RequestIdentity) -> Option<store::Claim> {
        self.runner_stop.at()?;
        self.adoption_owner(identity)?;
        self.check_claim(identity)
    }

    pub(crate) fn adoption_owner(&self, identity: &RequestIdentity) -> Option<AdoptionOwner> {
        let state = self.check_adoptions.lock().unwrap();
        let claim = state.claims.get(identity)?;
        if !can_adopt_claim(claim) {
            return None;
        }
        state
            .consumers
            .get(identity)
            .filter(|waiters| waiters.iter().any(|waiter| waiter.is_cadence_adopter()))
            .map(|_| AdoptionOwner::MainCadence)
    }

    pub(super) fn left_running_attempt(
        &self,
        identity: &RequestIdentity,
    ) -> Option<LeftRunningAttempt> {
        let stopped_at_ms = self.runner_stop.at()?;
        let claim = self.check_left_running(identity)?;
        Some(LeftRunningAttempt {
            machine: claim.machine,
            request_id: claim.request_id,
            attempt_id: claim.attempt_id,
            executor_id: claim.executor_id,
            stopped_at_ms,
        })
    }

    /// Production links wait here before attach/Ready, so ownership is known
    /// before any resent result or opening account can be ingested.
    pub async fn await_restart_adoptions_loaded(&self) {
        self.await_purpose_debts_loaded().await;
    }

    pub(super) async fn claim_check_at_grant(
        &self,
        identity: &RequestIdentity,
        selected: &SelectedExecutor,
        at: u64,
    ) {
        let gate = self.check_adoptions.lock().unwrap().mutation.clone();
        let _mutation = gate.lock().await;
        let (db, template) = {
            let mut state = self.check_adoptions.lock().unwrap();
            (state.db.clone(), state.templates.remove(identity))
        };
        if selected.colocated {
            return;
        }
        let (Some(db), Some(mut claim)) = (db, template) else {
            return;
        };
        claim.request_id = identity.0.clone();
        claim.attempt_id = identity.1.clone();
        claim.executor_id = selected.executor_id.clone();
        claim.machine = self.executor_name_for_text(&selected.executor_id);
        claim.platform = selected.capabilities.os.clone();
        claim.granted_at_ms = at as i64;
        claim.updated_ms = at as i64;
        claim.state = "running".into();
        claim.stopped_at_ms = None;
        if serde_json::to_vec(&claim.recording_plans).map_or(true, |plans| plans.len() > 256 * 1024)
        {
            claim.recording_plans.clear();
            claim.evidence = Some("not adoptable: recording plans exceed 256 KiB".into());
        } else if claim.recording_plans.len() != claim.check_names.len()
            || claim.recording_plans.iter().any(serde_json::Value::is_null)
        {
            claim.evidence = Some("not adoptable: recording plans are incomplete".into());
        }
        claim.observation_id = None;
        let pending = self
            .check_adoptions
            .lock()
            .unwrap()
            .consumers
            .remove(identity)
            .unwrap_or_default();
        let mut consumers: Vec<_> = claim
            .check_names
            .iter()
            .enumerate()
            .filter(|(_, check_name)| {
                !pending.iter().any(|waiter| {
                    &waiter.check_name == *check_name && waiter.consumer == claim.consumer
                })
            })
            .map(|(item_index, check_name)| store::Waiter {
                request_id: identity.0.clone(),
                attempt_id: identity.1.clone(),
                check_name: check_name.clone(),
                item_index,
                consumer: claim.consumer.clone(),
                consumer_ref: claim.consumer_ref.clone(),
                state: "running".into(),
                observation_id: None,
                evidence: None,
                updated_ms: at as i64,
            })
            .collect();
        for mut waiter in pending {
            if let Some(index) = claim
                .check_names
                .iter()
                .position(|name| name == &waiter.check_name)
            {
                waiter.item_index = index;
                if !consumers
                    .iter()
                    .any(|existing| same_waiter(existing, &waiter))
                {
                    consumers.push(waiter);
                }
            }
        }
        match store::insert_grant(&db, &claim, &consumers).await {
            Ok(()) => {
                self.deferred_acks.lock().unwrap().entry(identity.0.clone()).or_default();
                let mut state = self.check_adoptions.lock().unwrap();
                state.consumers.insert(identity.clone(), consumers);
                state.claims.insert(identity.clone(), claim);
            }
            Err(error) => log::error!("check_lane_adoptions grant write failed for request {} attempt {}: {error}; this lane is not adoptable", identity.0, identity.1),
        }
    }

    /// Called synchronously from result ingest as well as the durable adoption
    /// router. Holding here is essential: resends precede the opening snapshot,
    /// but the asynchronous router need not have read its store by that snapshot.
    pub(crate) fn hold_check_adoption(&self, mut result: UnboundResult) {
        if let CellOutcome::Completed { metadata, .. } = &mut result.outcome {
            let connections = self.lock_connections();
            let Some(connection) = connections.get(&result.executor_id) else {
                return;
            };
            metadata.executor_id = connection.identity.executor_id.clone();
            metadata.executor_device_id = connection.identity.device_id.clone();
            metadata.executor_connection_generation = connection.generation;
        }
        let key = (result.request_id.clone(), result.attempt_id.clone());
        let delivered = {
            let mut state = self.check_adoptions.lock().unwrap();
            if state
                .claims
                .get(&key)
                .is_none_or(|claim| claim.executor_id != result.executor_id)
            {
                return;
            }
            state.received.insert(key.clone());
            let mut delivered = false;
            for waiter in state.waiters.remove(&key).unwrap_or_default() {
                delivered |= waiter.send(result.outcome.clone()).is_ok();
            }
            let executor_id = result.executor_id.clone();
            state.held.insert(key.clone(), result);
            delivered.then_some(executor_id)
        };
        if let Some(executor_id) = delivered {
            self.acknowledge_settled_result(&executor_id, &key);
        }
    }

    /// Bind a previous runner's granted execution, without Offer or Granted.
    /// The caller installs its deferred acknowledgement before entering and
    /// keeps it until the observation and claim have both committed.
    pub(crate) async fn bind_adopted(
        &self,
        identity: RequestIdentity,
        executor_id: &str,
    ) -> ExecutedRequest {
        self.deferred_acks
            .lock()
            .unwrap()
            .entry(identity.0.clone())
            .or_default();
        let (tx, rx) = oneshot::channel();
        let held = {
            let mut state = self.check_adoptions.lock().unwrap();
            let Some(claim) = state.claims.get(&identity) else {
                return executor_unavailable("adopted check claim is no longer open".into()).into();
            };
            if claim.executor_id != executor_id {
                return executor_unavailable(
                    "adopted check claim names a different executor".into(),
                )
                .into();
            }
            state.waiters.entry(identity.clone()).or_default().push(tx);
            state.held.get(&identity).cloned()
        };
        if let Some(result) = held {
            self.hold_check_adoption(result);
        }
        let outcome = tokio::select! {
            biased;
            result = rx => result.unwrap_or_else(|_| executor_unavailable("adopted check binding was released".into())),
            at = self.runner_stop.stopped() => self.stopped_outcome(at, &identity, Some(executor_id)),
        };
        let mut executed: ExecutedRequest = outcome.into();
        executed.account.placement = Some(CellPlacement {
            executor_id: executor_id.into(),
            generation: self.link_generation(executor_id).unwrap_or(0),
            name: self.executor_name_for_text(executor_id),
            platform: self.executor_platform(executor_id),
        });
        if matches!(
            &executed.outcome,
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::RunnerStopping { .. },
                ..
            }
        ) {
            executed.account.left_running = self.left_running_attempt(&identity);
        }
        executed
    }

    /// The caller has already durably recorded loss. End any remaining work
    /// and release any retained result, then let the cadence's one waiter settle.
    pub(crate) fn cancel_adopted(&self, request_id: &str, attempt_id: &str, executor_id: &str) {
        let key = (request_id.to_owned(), attempt_id.to_owned());
        let (held, waiter, ended) = {
            let mut state = self.check_adoptions.lock().unwrap();
            let ended = state.received.contains(&key) || state.release_authorized.remove(&key);
            state.claims.remove(&key);
            state.forget_consumers(&key);
            state.reloaded.remove(&key);
            state.received.remove(&key);
            state.terminal.remove(&key);
            state.accounted.remove(&key);
            state.losing.remove(&key);
            (state.held.remove(&key), state.waiters.remove(&key), ended)
        };
        if let Some(generation) = self.link_generation(executor_id) {
            let _ = self.send_to(
                executor_id,
                generation,
                ExecutorMessage::Cancel {
                    request_id: request_id.into(),
                    attempt_id: attempt_id.into(),
                },
            );
        }
        if let Some(result) = held {
            self.send_result_ack(&result.executor_id, request_id, attempt_id);
        }
        let delivered = self
            .deferred_acks
            .lock()
            .unwrap()
            .remove(request_id)
            .unwrap_or_default();
        for (attempt, executor) in delivered {
            self.send_result_ack(&executor, request_id, &attempt);
        }
        for waiter in waiter.unwrap_or_default() {
            let _ = waiter.send(executor_unavailable(
                "remote check adoption ended; see durable check_lane_adoptions evidence".into(),
            ));
        }
        if ended {
            self.purposes.lock().unwrap().finish(&key);
        } else {
            self.purposes.lock().unwrap().end_unconfirmed(
                &key,
                purpose::UnconfirmedReason::GrantAbandoned {
                    detail: "adoption ended; cancellation still awaits the machine's confirmation"
                        .into(),
                },
                unix_time_ms(),
            );
        }
        self.purpose_quiesced.notify_waiters();
    }

    pub(super) fn withdraw_check_purpose(
        &self,
        key: &purpose::PurposeKey,
        physical: &RequestIdentity,
    ) {
        if self.runner_stop.at().is_some() {
            return;
        }
        let consumers = self
            .check_adoptions
            .lock()
            .unwrap()
            .consumers
            .get(physical)
            .cloned()
            .unwrap_or_default();
        for waiter in consumers.into_iter().filter(|waiter| {
            waiter.consumer == "main_cadence"
                && waiter.consumer_ref == key.as_str()
                && matches!(
                    waiter.state.as_str(),
                    "running" | "awaiting_adoption" | "joined" | "recorded"
                )
        }) {
            let fleet = self.clone();
            let runtime = fleet
                .host_runtime
                .clone()
                .or_else(|| tokio::runtime::Handle::try_current().ok());
            crate::orchestrator::spawn_onto(
                runtime.as_ref(),
                async move {
                    loop {
                        match fleet.withdraw_purpose_consumer_once(&waiter).await {
                        Ok(()) => return,
                        Err(error) => log::error!("cadence purpose consumer withdrawal failed: {error}; no acknowledgement is authorized; retrying"),
                    }
                        tokio::time::sleep(RETAINED_RESULT_RETRY).await;
                    }
                },
                || {
                    log::error!("cadence purpose withdrawal retry could not start; its durable row remains unacknowledged")
                },
            );
        }
    }

    async fn withdraw_purpose_consumer_once(&self, waiter: &store::Waiter) -> Result<(), String> {
        let (gate, db) = {
            let state = self.check_adoptions.lock().unwrap();
            (state.mutation.clone(), state.db.clone())
        };
        let Some(db) = db else {
            return Ok(());
        };
        let _mutation = gate.lock().await;
        let physical = (waiter.request_id.clone(), waiter.attempt_id.clone());
        let alias_interests: Vec<_> = self
            .purpose_joiners
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, (key, _))| key.as_str() == waiter.consumer_ref)
            .map(|(alias, (_, subscribers))| (alias.clone(), subscribers.clone()))
            .collect();
        for (alias, subscribers) in alias_interests {
            let shares = {
                let registry = self.in_flight.lock().unwrap();
                subscribers.iter().any(|subscriber| {
                    registry
                        .subscriber_keys
                        .get(subscriber)
                        .is_some_and(|shared| shared.leader == physical)
                })
            };
            if shares
                && self.purposes.lock().unwrap().is_live_holder(
                    &purpose::PurposeKey::from_persisted(waiter.consumer_ref.clone()),
                    &alias,
                )
            {
                return Ok(());
            }
        }
        self.withdraw_check_consumer_locked(
            db,
            &physical,
            &waiter.check_name,
            &waiter.consumer,
            &waiter.consumer_ref,
            "cadence requester withdrew or was superseded",
        )
        .await
    }

    pub(super) fn withdraw_joiner_check_consumers(&self, request: &RequestIdentity) {
        let joining = self.purpose_joiners.lock().unwrap().get(request).cloned();
        if let Some((key, subscribers)) = joining {
            let physical: HashSet<_> = {
                let registry = self.in_flight.lock().unwrap();
                subscribers
                    .iter()
                    .filter_map(|subscriber| {
                        registry
                            .subscriber_keys
                            .get(subscriber)
                            .map(|shared| shared.leader.clone())
                    })
                    .collect()
            };
            for physical in physical {
                self.withdraw_check_purpose(&key, &physical);
            }
        }
    }

    pub(super) fn authorize_check_release(&self, identity: &RequestIdentity) {
        self.check_adoptions
            .lock()
            .unwrap()
            .release_authorized
            .insert(identity.clone());
    }

    pub(super) fn settle_check_claim(
        &self,
        key: &RequestIdentity,
        state_name: &'static str,
        evidence: String,
        release: bool,
    ) {
        let (db, claim) = {
            let mut state = self.check_adoptions.lock().unwrap();
            let Some(claim) = state.claims.get(key).cloned() else {
                return;
            };
            if release && !state.losing.insert(key.clone()) {
                return;
            }
            (state.db.clone(), claim)
        };
        let Some(db) = db else {
            return;
        };
        let fleet = self.clone();
        let key = key.clone();
        // Freeze the authoritative account/removal/overlap evidence once.
        // Retry only its persistence; elapsed time never becomes death evidence.
        let runtime = fleet
            .host_runtime
            .clone()
            .or_else(|| tokio::runtime::Handle::try_current().ok());
        crate::orchestrator::spawn_onto(
            runtime.as_ref(),
            async move {
                let gate = fleet.check_adoptions.lock().unwrap().mutation.clone();
                loop {
                    let result = {
                        let _mutation = gate.lock().await;
                        let result = async {
                            if state_name == "lost" {
                                let consumers = store::waiters(&db, &key.0, &key.1)
                                    .await
                                    .map_err(|error| error.to_string())?;
                                let checks: Vec<_> = claim
                                    .check_names
                                    .iter()
                                    .filter(|name| {
                                        consumers.iter().any(|waiter| {
                                            waiter.check_name == **name
                                                && waiter.is_cadence_adopter()
                                        })
                                    })
                                    .cloned()
                                    .collect();
                                // A lost row can disappear from boot's open-claim
                                // scan. Its repair citation must already be durable.
                                crate::execution::checks_main::record_adoption_interruptions(
                                    &db, &claim, &checks,
                                )
                                .await?;
                            }
                            if release {
                                store::settle_failure(&db, &key.0, &key.1, state_name, &evidence)
                                    .await
                                    .map_err(|error| error.to_string())
                            } else {
                                store::transition(
                                    &db,
                                    &key.0,
                                    &key.1,
                                    state_name,
                                    Some(&evidence),
                                    None,
                                    None,
                                )
                                .await
                                .map_err(|error| error.to_string())
                            }
                        }
                        .await;
                        if result.is_ok() && release {
                            fleet.cancel_adopted(&key.0, &key.1, &claim.executor_id);
                        }
                        result
                    };
                    match result {
                        Ok(()) => return,
                        Err(error) => {
                            log::error!("check_lane_adoptions {state_name} write failed for request {}: {error}; no cancellation or acknowledgement is authorized; retrying the original evidence", key.0);
                            #[cfg(test)]
                            if let Some(probe) = fleet
                                .check_adoptions
                                .lock()
                                .unwrap()
                                .settlement_retry_probe
                                .clone()
                            {
                                let _ = probe.send((key.clone(), evidence.clone()));
                            }
                        }
                    }
                    tokio::time::sleep(RETAINED_RESULT_RETRY).await;
                }
            },
            || {
                log::error!("check adoption evidence settlement could not start; no cancellation or acknowledgement is authorized")
            },
        );
    }

    /// Caller holds the mutation gate from before coalescing admission, so a
    /// recorder cannot acknowledge an execution while its new join is persisting.
    pub(super) async fn register_check_join_locked(
        &self,
        physical: &RequestIdentity,
        check_name: &str,
        source: &store::Claim,
    ) -> Result<(), String> {
        let (db, granted, template) = {
            let state = self.check_adoptions.lock().unwrap();
            (
                state.db.clone(),
                state.claims.get(physical).cloned(),
                state.templates.get(physical).cloned(),
            )
        };
        let durable = granted.is_some();
        let claim = match granted.or(template) {
            Some(claim) => Some(claim),
            None => match &db {
                Some(db) => store::lookup(db, &physical.0, &physical.1)
                    .await
                    .map_err(|e| e.to_string())?,
                None => None,
            },
        };
        let Some(claim) = claim else {
            return Ok(());
        };
        let Some(item_index) = claim.check_names.iter().position(|name| name == check_name) else {
            return Err(format!(
                "joined check {check_name} is absent from physical grant {}",
                physical.0
            ));
        };
        let waiter = store::Waiter {
            request_id: physical.0.clone(),
            attempt_id: physical.1.clone(),
            check_name: check_name.into(),
            item_index,
            consumer: source.consumer.clone(),
            consumer_ref: source.consumer_ref.clone(),
            state: "running".into(),
            observation_id: None,
            evidence: None,
            updated_ms: unix_time_ms() as i64,
        };
        let physically_persisted = durable || !matches!(claim.state.as_str(), "" | "template");
        // A template is held in memory only; its grant commits all pending
        // joins atomically with the physical row before sending Granted.
        let pending_template = self
            .check_adoptions
            .lock()
            .unwrap()
            .templates
            .contains_key(physical);
        if physically_persisted && !pending_template {
            let db = db
                .as_ref()
                .ok_or_else(|| "check adoption store is not attached".to_string())?;
            store::insert_waiter(db, &waiter)
                .await
                .map_err(|e| e.to_string())?;
        }
        let mut state = self.check_adoptions.lock().unwrap();
        let consumers = state.consumers.entry(physical.clone()).or_default();
        if !consumers
            .iter()
            .any(|existing| same_waiter(existing, &waiter))
        {
            consumers.push(waiter);
        }
        Ok(())
    }

    pub(crate) async fn record_check_claim_observation(
        &self,
        identity: &RequestIdentity,
        check_name: &str,
        observation_id: &str,
    ) -> Result<(), String> {
        let (gate, db, reloaded) = {
            let state = self.check_adoptions.lock().unwrap();
            (
                state.mutation.clone(),
                state.db.clone(),
                state.reloaded.contains(identity),
            )
        };
        let Some(db) = db else {
            return Ok(());
        };
        let _mutation = gate.lock().await;
        let Some(claim) = store::lookup(&db, &identity.0, &identity.1)
            .await
            .map_err(|e| e.to_string())?
        else {
            return Ok(());
        };
        if !claim.check_names.iter().any(|name| name == check_name) {
            return Err(format!(
                "check {check_name} is not in remote grant {}",
                identity.0
            ));
        }
        let consumers = store::waiters(&db, &identity.0, &identity.1)
            .await
            .map_err(|e| e.to_string())?;
        for waiter in consumers
            .iter()
            .filter(|waiter| waiter.check_name == check_name)
        {
            if waiter.state == "cancelled" {
                store::record_cancelled_waiter_observation(&db, waiter, observation_id)
                    .await
                    .map_err(|error| error.to_string())?;
                continue;
            }
            let state_name = if reloaded {
                if waiter.consumer == "main_cadence" {
                    "recorded"
                } else {
                    "recorded_undelivered"
                }
            } else {
                "recorded"
            };
            let evidence = (state_name == "recorded_undelivered").then(|| {
                format!(
                    "no adopter for {} in this build; observation {observation_id}",
                    waiter.consumer
                )
            });
            store::transition_waiter(
                &db,
                waiter,
                state_name,
                evidence.as_deref(),
                Some(observation_id),
            )
            .await
            .map_err(|e| e.to_string())?;
        }
        self.maybe_finish_check_locked(&db, &claim).await
    }

    pub(crate) async fn deliver_check_consumer(
        &self,
        identity: &RequestIdentity,
        check_name: &str,
        consumer: &str,
        consumer_ref: &str,
        observation_id: &str,
    ) -> Result<(), String> {
        let db = self.check_adoptions.lock().unwrap().db.clone();
        let Some(db) = db else {
            return Ok(());
        };
        let Some(waiter) = store::waiters(&db, &identity.0, &identity.1)
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .find(|waiter| {
                waiter.check_name == check_name
                    && waiter.consumer == consumer
                    && waiter.consumer_ref == consumer_ref
            })
        else {
            if store::lookup(&db, &identity.0, &identity.1)
                .await
                .map_err(|error| error.to_string())?
                .is_none()
            {
                return Ok(());
            }
            return Err(format!(
                "no durable {consumer} consumer {consumer_ref} for {check_name} on request {}",
                identity.0
            ));
        };
        self.deliver_check_waiter(&waiter, observation_id).await
    }

    pub(crate) async fn deliver_check_waiter(
        &self,
        waiter: &store::Waiter,
        observation_id: &str,
    ) -> Result<(), String> {
        let (gate, db) = {
            let state = self.check_adoptions.lock().unwrap();
            (state.mutation.clone(), state.db.clone())
        };
        let db = db.ok_or_else(|| "check adoption store is not attached".to_string())?;
        let _mutation = gate.lock().await;
        let current = store::waiters(&db, &waiter.request_id, &waiter.attempt_id)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .find(|candidate| same_waiter(candidate, waiter))
            .ok_or_else(|| "check consumer no longer exists".to_string())?;
        if current.observation_id.as_deref() != Some(observation_id)
            || !matches!(current.state.as_str(), "recorded" | "delivered")
        {
            return Err("check consumer must have a committed observation before delivery".into());
        }
        store::transition_waiter(&db, waiter, "delivered", None, Some(observation_id))
            .await
            .map_err(|e| e.to_string())?;
        if let Some(claim) = store::lookup(&db, &waiter.request_id, &waiter.attempt_id)
            .await
            .map_err(|e| e.to_string())?
        {
            self.maybe_finish_check_locked(&db, &claim).await?;
        }
        Ok(())
    }

    async fn maybe_finish_check_locked(
        &self,
        db: &LocalDb,
        claim: &store::Claim,
    ) -> Result<(), String> {
        let consumers = store::waiters(db, &claim.request_id, &claim.attempt_id)
            .await
            .map_err(|e| e.to_string())?;
        let complete = claim.check_names.iter().all(|name| {
            consumers
                .iter()
                .any(|w| &w.check_name == name && w.observation_id.is_some())
        });
        let delivered = consumers.iter().all(|w| {
            matches!(
                w.state.as_str(),
                "delivered" | "recorded_undelivered" | "cancelled" | "lost"
            )
        });
        if !delivered {
            return Ok(());
        }
        let key = (claim.request_id.clone(), claim.attempt_id.clone());
        if !complete {
            // An empty coalescing tombstone remains joinable until its leader
            // publishes. Consumer withdrawal alone cannot end its physical claim.
            if self.check_adoptions.lock().unwrap().terminal.contains(&key) {
                store::settle_failure(
                    db,
                    &key.0,
                    &key.1,
                    "cancelled",
                    "physical check completed but no live consumer remained to record it",
                )
                .await
                .map_err(|error| error.to_string())?;
                self.authorize_check_release(&key);
                self.cancel_adopted(&key.0, &key.1, &claim.executor_id);
            }
            return Ok(());
        }
        let observation = consumers.iter().find_map(|w| w.observation_id.as_deref());
        store::transition(
            db,
            &claim.request_id,
            &claim.attempt_id,
            "recorded",
            None,
            observation,
            None,
        )
        .await
        .map_err(|e| e.to_string())?;
        let key = (claim.request_id.clone(), claim.attempt_id.clone());
        self.finish_check_claim(&key);
        drop(self.defer_result_ack(&key.0));
        Ok(())
    }

    pub(super) fn check_leader_completed(&self, identity: &RequestIdentity) {
        let (gate, db) = {
            let mut state = self.check_adoptions.lock().unwrap();
            if !state.claims.contains_key(identity) {
                state.forget_consumers(identity);
                return;
            }
            if !state.terminal.insert(identity.clone()) {
                return;
            }
            (state.mutation.clone(), state.db.clone())
        };
        let Some(db) = db else {
            return;
        };
        let fleet = self.clone();
        let identity = identity.clone();
        let runtime = fleet
            .host_runtime
            .clone()
            .or_else(|| tokio::runtime::Handle::try_current().ok());
        crate::orchestrator::spawn_onto(
            runtime.as_ref(),
            async move {
                loop {
                    let result = async {
                        let _mutation = gate.lock().await;
                        if !fleet
                            .check_adoptions
                            .lock()
                            .unwrap()
                            .claims
                            .contains_key(&identity)
                        {
                            return Ok(());
                        }
                        if let Some(claim) = store::lookup(&db, &identity.0, &identity.1)
                            .await
                            .map_err(|error| error.to_string())?
                        {
                            fleet.maybe_finish_check_locked(&db, &claim).await?;
                        }
                        Ok::<_, String>(())
                    }
                    .await;
                    match result {
                        Ok(()) => return,
                        Err(error) => {
                            log::error!("terminal check consumer settlement failed: {error}; no acknowledgement is authorized; retrying the original completion evidence");
                            #[cfg(test)]
                            if let Some(probe) = fleet
                                .check_adoptions
                                .lock()
                                .unwrap()
                                .settlement_retry_probe
                                .clone()
                            {
                                let _ = probe.send((identity.clone(), "physical check completed but no live consumer remained to record it".into()));
                            }
                        }
                    }
                    tokio::time::sleep(RETAINED_RESULT_RETRY).await;
                }
            },
            || {
                log::error!("terminal check settlement retry could not start; its result remains unacknowledged")
            },
        );
    }

    pub(crate) fn finish_check_claim(&self, identity: &RequestIdentity) {
        {
            let mut state = self.check_adoptions.lock().unwrap();
            state.claims.remove(identity);
            state.templates.remove(identity);
            state.held.remove(identity);
            state.received.remove(identity);
            state.terminal.remove(identity);
            state.reloaded.remove(identity);
            state.accounted.remove(identity);
            state.losing.remove(identity);
            state.consumers.remove(identity);
            state
                .consumer_guards
                .retain(|consumer, _| &consumer.physical != identity);
            state
                .subscriber_consumers
                .retain(|_, consumer| &consumer.physical != identity);
            state.waiters.remove(identity);
        }
        self.purposes.lock().unwrap().finish(identity);
        self.purpose_quiesced.notify_waiters();
    }

    /// Prior-runner claims remain debt until their recorder commits or loss is
    /// durable; an account alone cannot release that gate ahead of its write.
    pub(super) fn protected_check_claims(&self, executor_id: &str) -> HashSet<RequestIdentity> {
        self.check_claims_for_machine(executor_id)
            .into_iter()
            .filter(|(key, _)| self.adoption_owner(key).is_some())
            .map(|(key, _)| key)
            .collect()
    }

    pub(super) fn account_check_claims(
        &self,
        executor_id: &str,
        generation: u64,
        listed: &HashSet<RequestIdentity>,
        completed: &HashSet<RequestIdentity>,
    ) {
        for (key, claim) in self.check_claims_for_machine(executor_id) {
            let received = {
                let state = self.check_adoptions.lock().unwrap();
                if !state.reloaded.contains(&key) || state.accounted.contains(&key) {
                    continue;
                }
                state.received.contains(&key)
            };
            if self.adoption_owner(&key).is_none() {
                self.settle_check_claim(
                    &key,
                    "cancelled",
                    format!(
                        "cancelled by restart: no adopter for {} lanes yet",
                        claim.consumer
                    ),
                    true,
                );
                continue;
            }
            if received || (listed.contains(&key) && !completed.contains(&key)) {
                let purposes = self
                    .check_adoptions
                    .lock()
                    .unwrap()
                    .consumers
                    .get(&key)
                    .cloned()
                    .unwrap_or_default();
                let mut registry = self.purposes.lock().unwrap();
                for waiter in purposes.iter().filter(|w| w.is_cadence_adopter()) {
                    let purpose = purpose::PurposeKey::from_persisted(waiter.consumer_ref.clone());
                    let _ = registry.readopt(&purpose, &key);
                }
                drop(registry);
                self.check_adoptions
                    .lock()
                    .unwrap()
                    .accounted
                    .insert(key.clone());
                self.settle_check_claim(&key, "joined", format!("adopted on {} from the previous runner at connection generation {generation}", claim.machine), false);
                log::info!(
                    "adopted remote check request {} attempt {} on {} generation {generation}",
                    key.0,
                    key.1,
                    claim.machine
                );
            } else {
                self.authorize_check_release(&key);
                self.settle_check_claim(&key, "lost", format!("{}'s first report since this runner started (generation {generation}) does not list request {} attempt {} and it resent no result", claim.machine, key.0, key.1), true);
            }
        }
    }

    pub(super) fn check_claims_for_machine(
        &self,
        executor_id: &str,
    ) -> Vec<(RequestIdentity, store::Claim)> {
        self.check_adoptions
            .lock()
            .unwrap()
            .claims
            .iter()
            .filter(|(_, claim)| claim.executor_id == executor_id)
            .map(|(key, claim)| (key.clone(), claim.clone()))
            .collect()
    }

    pub(super) fn check_machine_lost(&self, executor_id: &str, evidence: &str) {
        for (key, claim) in self.check_claims_for_machine(executor_id) {
            self.authorize_check_release(&key);
            self.settle_check_claim(
                &key,
                "lost",
                format!(
                    "{} was removed at {}: {evidence}",
                    claim.machine,
                    purpose::stamp(unix_time_ms())
                ),
                true,
            );
        }
    }

    pub(super) fn check_shutdown(&self, key: &RequestIdentity, stopped_at: u64) -> bool {
        let Some(claim) = self.check_claim(key) else {
            return false;
        };
        if self.adoption_owner(key).is_some() {
            let db = self.check_adoptions.lock().unwrap().db.clone();
            if let Some(db) = db {
                let key = key.clone();
                let runtime = self
                    .host_runtime
                    .clone()
                    .or_else(|| tokio::runtime::Handle::try_current().ok());
                crate::orchestrator::spawn_onto(
                    runtime.as_ref(),
                    async move {
                        if let Err(error) = store::transition(
                            &db,
                            &key.0,
                            &key.1,
                            "awaiting_adoption",
                            None,
                            None,
                            Some(stopped_at as i64),
                        )
                        .await
                        {
                            log::warn!("check_lane_adoptions shutdown write failed for request {}: {error}; its running claim remains adoptable", key.0);
                        }
                    },
                    || {
                        log::warn!("check shutdown write could not start; its running claim remains adoptable")
                    },
                );
            }
            true
        } else {
            self.settle_check_claim(
                key,
                "cancelled",
                format!(
                    "cancelled by restart: no adopter for {} lanes yet",
                    claim.consumer
                ),
                true,
            );
            false
        }
    }
}

pub(super) async fn preload(state: &Arc<Mutex<CheckAdoptions>>, db: &Arc<LocalDb>) {
    let loaded = async {
        let claims = store::open(db).await?;
        let mut loaded = Vec::new();
        for claim in claims {
            let consumers = store::waiters(db, &claim.request_id, &claim.attempt_id).await?;
            loaded.push((claim, consumers));
        }
        Ok::<_, crate::storage::DbError>(loaded)
    }
    .await;
    match loaded {
        Ok(loaded) => {
            let mut state = state.lock().unwrap();
            for (claim, consumers) in loaded {
                let key = (claim.request_id.clone(), claim.attempt_id.clone());
                state.reloaded.insert(key.clone());
                state.consumers.insert(key.clone(), consumers);
                state.claims.insert(key, claim);
            }
        }
        Err(error) => log::error!("check_lane_adoptions boot preload failed: {error}; post-boot accounts use the existing Cancel path"),
    }
    if let Err(error) = store::prune(db).await {
        log::warn!("check_lane_adoptions terminal-row prune failed: {error}");
    }
}

/// A grant is authoritative even if the asynchronous purpose journal had not
/// flushed before the previous runner stopped.
pub(super) fn purpose_rows(state: &Arc<Mutex<CheckAdoptions>>) -> Vec<purpose::PurposeDebtRow> {
    let state = state.lock().unwrap();
    state
        .claims
        .iter()
        .filter(|(_, claim)| can_adopt_claim(claim))
        .flat_map(|(key, claim)| {
            state
                .consumers
                .get(key)
                .into_iter()
                .flatten()
                .filter(|w| w.is_cadence_adopter())
                .map(move |waiter| purpose::PurposeDebtRow {
                    key: purpose::PurposeKey::from_persisted(waiter.consumer_ref.clone()),
                    request: key.clone(),
                    state: purpose::DebtRowState::Live,
                    phase: purpose::ItemPhase::Dispatched,
                    placement: purpose::DebtPlacement {
                        executor_id: claim.executor_id.clone(),
                        generation: 0,
                        dispatched_at_unix_ms: claim.granted_at_ms as u64,
                        awaiting_post_boot_account: true,
                    },
                    cause: None,
                    confirmation: None,
                    retired_at_unix_ms: None,
                })
        })
        .collect()
}

fn same_waiter(left: &store::Waiter, right: &store::Waiter) -> bool {
    left.request_id == right.request_id
        && left.attempt_id == right.attempt_id
        && left.check_name == right.check_name
        && left.consumer == right.consumer
        && left.consumer_ref == right.consumer_ref
}

fn can_adopt_claim(claim: &store::Claim) -> bool {
    !claim.check_names.is_empty()
        && claim.recording_plans.len() == claim.check_names.len()
        && claim.recording_plans.iter().all(|plan| !plan.is_null())
        && !claim
            .evidence
            .as_deref()
            .is_some_and(|evidence| evidence.starts_with("not adoptable:"))
        && serde_json::to_vec(&claim.recording_plans).is_ok_and(|plans| plans.len() <= 256 * 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claim(consumer: &str, state: &str) -> store::Claim {
        store::Claim {
            request_id: "remote-request".into(),
            attempt_id: "remote-attempt".into(),
            project_id: "project".into(),
            check_names: vec!["rust-tests".into()],
            platform: "linux".into(),
            commit_sha: "original-commit".into(),
            input_hashes: vec!["original-input".into()],
            recording_plans: vec![serde_json::json!({"fixture": true})],
            consumer: consumer.into(),
            consumer_ref: purpose::PurposeKey::main_cadence("project", "rust-tests", "linux")
                .to_string(),
            executor_id: "remote-check".into(),
            machine: "remote-check".into(),
            granted_at_ms: 10,
            state: state.into(),
            stopped_at_ms: (state == "awaiting_adoption").then_some(20),
            evidence: None,
            observation_id: None,
            updated_ms: 10,
        }
    }

    fn identity(claim: &store::Claim) -> RequestIdentity {
        (claim.request_id.clone(), claim.attempt_id.clone())
    }

    async fn fixture(
        claim: store::Claim,
    ) -> (
        Fleet,
        Arc<LocalDb>,
        mpsc::UnboundedReceiver<ExecutorMessage>,
    ) {
        let db = Arc::new(crate::storage::migrated_test_db("fleet-check-adoption").await);
        let consumers: Vec<_> = claim
            .check_names
            .iter()
            .enumerate()
            .map(|(item_index, check_name)| store::Waiter {
                request_id: claim.request_id.clone(),
                attempt_id: claim.attempt_id.clone(),
                check_name: check_name.clone(),
                item_index,
                consumer: claim.consumer.clone(),
                consumer_ref: claim.consumer_ref.clone(),
                state: "running".into(),
                observation_id: None,
                evidence: None,
                updated_ms: 10,
            })
            .collect();
        store::insert_grant(&db, &claim, &consumers).await.unwrap();
        let fleet = Fleet::default();
        fleet.check_adoptions.lock().unwrap().db = Some(db.clone());
        preload(&fleet.check_adoptions, &db).await;
        fleet
            .purposes
            .lock()
            .unwrap()
            .reload(purpose_rows(&fleet.check_adoptions), 30);
        let remote = fleet.attach_unreported_machine_for_test(&claim.executor_id);
        (fleet, db, remote)
    }

    fn outcome(claim: &store::Claim) -> CellOutcome {
        CellOutcome::Cancelled {
            request_id: claim.request_id.clone(),
            attempt_id: claim.attempt_id.clone(),
        }
    }

    fn send_result(fleet: &Fleet, claim: &store::Claim) {
        fleet.handle_executor_message(
            &claim.executor_id,
            fleet.link_generation(&claim.executor_id).unwrap(),
            ExecutorMessage::Result {
                request_id: claim.request_id.clone(),
                attempt_id: claim.attempt_id.clone(),
                outcome: outcome(claim),
            },
        );
    }

    fn account(fleet: &Fleet, claim: &store::Claim, listed: bool) {
        let mut snapshot = FleetSnapshot::default();
        if listed {
            snapshot
                .granted_attempts
                .push(cairn_common::executor_protocol::GrantedAttempt {
                    request_id: claim.request_id.clone(),
                    attempt_id: claim.attempt_id.clone(),
                });
        }
        fleet.settle_purpose_debts_from_account(
            &claim.executor_id,
            fleet.link_generation(&claim.executor_id).unwrap(),
            true,
            &snapshot,
        );
    }

    fn take_frames(remote: &mut mpsc::UnboundedReceiver<ExecutorMessage>) -> Vec<ExecutorMessage> {
        let mut frames = Vec::new();
        while let Ok(frame) = remote.try_recv() {
            frames.push(frame);
        }
        frames
    }

    async fn record_and_deliver(fleet: &Fleet, db: &LocalDb, claim: &store::Claim) {
        let key = identity(claim);
        fleet
            .record_check_claim_observation(&key, "rust-tests", "observation")
            .await
            .unwrap();
        let waiter = store::waiters(db, &key.0, &key.1).await.unwrap().remove(0);
        assert_eq!(waiter.state, "recorded");
        fleet
            .deliver_check_waiter(&waiter, "observation")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn remote_check_adoption_running_and_abrupt_claims_readopt_without_cancel() {
        for state in ["running", "awaiting_adoption"] {
            let claim = claim("main_cadence", state);
            let (fleet, db, mut remote) = fixture(claim.clone()).await;
            let key = identity(&claim);
            account(&fleet, &claim, true);
            assert!(fleet.purposes.lock().unwrap().is_live_holder(
                &purpose::PurposeKey::from_persisted(claim.consumer_ref.clone()),
                &key
            ));
            assert!(!take_frames(&mut remote)
                .iter()
                .any(|f| matches!(f, ExecutorMessage::Cancel { .. })));
            let mut binding = Box::pin(fleet.bind_adopted(key.clone(), &claim.executor_id));
            tokio::select! { biased; _ = &mut binding => panic!("the request is still running"), _ = std::future::ready(()) => {} }
            send_result(&fleet, &claim);
            assert_eq!(binding.await.outcome, outcome(&claim));
            assert!(!take_frames(&mut remote)
                .iter()
                .any(|f| matches!(f, ExecutorMessage::ResultAck { .. })));
            record_and_deliver(&fleet, &db, &claim).await;
            assert_eq!(
                store::lookup(&db, &key.0, &key.1)
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                "recorded"
            );
            assert_eq!(
                take_frames(&mut remote)
                    .iter()
                    .filter(|f| matches!(f, ExecutorMessage::ResultAck { .. }))
                    .count(),
                1
            );
        }
    }

    #[tokio::test]
    async fn remote_check_adoption_result_precedes_account_or_binding_without_loss_or_ack() {
        for bind_first in [false, true] {
            let claim = claim("main_cadence", "running");
            let (fleet, db, mut remote) = fixture(claim.clone()).await;
            let key = identity(&claim);
            let mut binding = Box::pin(fleet.bind_adopted(key.clone(), &claim.executor_id));
            if bind_first {
                tokio::select! { biased; _ = &mut binding => panic!("no result yet"), _ = std::future::ready(()) => {} }
            }
            send_result(&fleet, &claim);
            account(&fleet, &claim, false);
            assert_eq!(binding.await.outcome, outcome(&claim));
            assert!(!take_frames(&mut remote).iter().any(|f| matches!(
                f,
                ExecutorMessage::Cancel { .. } | ExecutorMessage::ResultAck { .. }
            )));
            fleet
                .record_check_claim_observation(&key, "rust-tests", "observation")
                .await
                .unwrap();
            assert!(!take_frames(&mut remote)
                .iter()
                .any(|f| matches!(f, ExecutorMessage::ResultAck { .. })));
            let waiter = store::waiters(&db, &key.0, &key.1).await.unwrap().remove(0);
            fleet
                .deliver_check_waiter(&waiter, "observation")
                .await
                .unwrap();
            assert!(take_frames(&mut remote)
                .iter()
                .any(|f| matches!(f, ExecutorMessage::ResultAck { .. })));
        }
    }

    #[tokio::test]
    async fn remote_check_adoption_unknown_commits_loss_before_releasing_waiter() {
        let claim = claim("main_cadence", "running");
        let (fleet, db, mut remote) = fixture(claim.clone()).await;
        let key = identity(&claim);
        let mut binding = Box::pin(fleet.bind_adopted(key.clone(), &claim.executor_id));
        tokio::select! { biased; _ = &mut binding => panic!("no evidence yet"), _ = std::future::ready(()) => {} }
        account(&fleet, &claim, false);
        assert!(fleet.check_claim(&key).is_some());
        assert!(matches!(
            binding.await.outcome,
            CellOutcome::Unavailable { .. }
        ));
        let row = store::lookup(&db, &key.0, &key.1).await.unwrap().unwrap();
        assert_eq!(row.state, "lost");
        let evidence = row.evidence.unwrap();
        assert!(
            evidence.contains("remote-check")
                && evidence.contains("generation")
                && evidence.contains(&key.0)
        );
        assert!(fleet.purpose_debts().is_empty());
        assert!(take_frames(&mut remote)
            .iter()
            .any(|f| matches!(f, ExecutorMessage::Cancel { .. })));
    }

    #[tokio::test]
    async fn remote_check_adoption_failure_without_machine_evidence_preserves_debt() {
        let claim = claim("main_cadence", "running");
        let (fleet, db, _remote) = fixture(claim.clone()).await;
        let key = identity(&claim);
        fleet.lock_connections().remove(&claim.executor_id);
        store::settle_failure(
            &db,
            &key.0,
            &key.1,
            "lost",
            "reconstruction failed while offline",
        )
        .await
        .unwrap();
        fleet.cancel_adopted(&key.0, &key.1, &claim.executor_id);
        assert_eq!(
            fleet.purpose_debts().len(),
            1,
            "a lost adoption is not a confirmed process death"
        );
        fleet.settle_purpose_debts_from_account(
            &claim.executor_id,
            1,
            true,
            &FleetSnapshot::default(),
        );
        assert!(
            fleet.purpose_debts().is_empty(),
            "the machine's omitted account is evidence"
        );
    }

    #[tokio::test]
    async fn remote_check_adoption_removed_machine_and_operator_overlap_authorize_release() {
        for operator in [false, true] {
            let claim = claim("main_cadence", "running");
            let (fleet, db, _remote) = fixture(claim.clone()).await;
            let key = identity(&claim);
            let mut binding = Box::pin(fleet.bind_adopted(key.clone(), &claim.executor_id));
            tokio::select! { biased; _ = &mut binding => panic!("no evidence yet"), _ = std::future::ready(()) => {} }
            if operator {
                fleet
                    .authorize_purpose_overlap(
                        &purpose::PurposeKey::from_persisted(claim.consumer_ref.clone()),
                        &key,
                        Some(&claim.executor_id),
                        "operator",
                    )
                    .unwrap();
            } else {
                fleet.settle_purpose_debts_for_lost_machine(
                    &claim.executor_id,
                    "enrollment removed",
                );
            }
            assert!(matches!(
                binding.await.outcome,
                CellOutcome::Unavailable { .. }
            ));
            let row = store::lookup(&db, &key.0, &key.1).await.unwrap().unwrap();
            assert_eq!(row.state, "lost");
            assert!(row.evidence.unwrap().contains(if operator {
                "authorized the overlap"
            } else {
                "was removed"
            }));
            assert!(fleet.purpose_debts().is_empty());
        }
    }

    #[tokio::test]
    async fn remote_check_adoption_retained_guard_keeps_delivered_ack_until_durable_failure() {
        let claim = claim("main_cadence", "running");
        let (fleet, db, mut remote) = fixture(claim.clone()).await;
        let key = identity(&claim);
        let guard = fleet.defer_result_ack(&key.0);
        send_result(&fleet, &claim);
        let _ = fleet.bind_adopted(key.clone(), &claim.executor_id).await;
        guard.retain();
        assert!(!take_frames(&mut remote)
            .iter()
            .any(|f| matches!(f, ExecutorMessage::ResultAck { .. })));
        store::settle_failure(&db, &key.0, &key.1, "lost", "recorder failed")
            .await
            .unwrap();
        fleet.cancel_adopted(&key.0, &key.1, &claim.executor_id);
        assert!(take_frames(&mut remote)
            .iter()
            .any(|f| matches!(f, ExecutorMessage::ResultAck { .. })));
        assert!(
            fleet.purpose_debts().is_empty(),
            "the received result proves work ended"
        );
    }

    #[tokio::test]
    async fn remote_check_adoption_never_reconnecting_debt_names_operator_escape() {
        let claim = claim("main_cadence", "running");
        let (fleet, _db, _remote) = fixture(claim.clone()).await;
        let debt = fleet.purpose_debts().pop().unwrap();
        let text = fleet.purpose_debt_settles_when(&debt);
        assert!(
            text.contains("awaiting adoption on remote-check")
                && text.contains("authorize_overlap")
        );
        assert!(text.contains("reports or is removed"));
        assert_eq!(
            fleet.adoption_owner(&identity(&claim)),
            Some(AdoptionOwner::MainCadence)
        );
    }

    #[tokio::test]
    async fn remote_check_adoption_late_cadence_join_preserves_entire_manual_batch() {
        let mut physical = claim("manual", "running");
        physical.check_names = vec!["a".into(), "b".into(), "c".into()];
        physical.input_hashes = vec!["ha".into(), "hb".into(), "hc".into()];
        physical.recording_plans = vec![serde_json::json!({"fixture": true}); 3];
        let (fleet, db, mut remote) = fixture(physical.clone()).await;
        let key = identity(&physical);
        assert_eq!(fleet.adoption_owner(&key), None);
        let source = claim("main_cadence", "running");
        let gate = fleet.check_adoptions.lock().unwrap().mutation.clone();
        {
            let _mutation = gate.lock().await;
            fleet
                .register_check_join_locked(&key, "a", &source)
                .await
                .unwrap();
        }
        assert_eq!(fleet.adoption_owner(&key), Some(AdoptionOwner::MainCadence));
        send_result(&fleet, &physical);
        let first = fleet.bind_adopted(key.clone(), &physical.executor_id).await;
        let second = fleet.bind_adopted(key.clone(), &physical.executor_id).await;
        assert_eq!(
            first.outcome, second.outcome,
            "all item recorders share one raw result"
        );
        for name in ["a", "b", "c"] {
            fleet
                .record_check_claim_observation(&key, name, &format!("observation-{name}"))
                .await
                .unwrap();
        }
        assert!(!take_frames(&mut remote)
            .iter()
            .any(|f| matches!(f, ExecutorMessage::ResultAck { .. })));
        let consumers = store::waiters(&db, &key.0, &key.1).await.unwrap();
        assert_eq!(
            consumers
                .iter()
                .filter(|w| w.consumer == "manual" && w.state == "recorded_undelivered")
                .count(),
            3
        );
        let cadence = consumers
            .iter()
            .find(|w| w.consumer == "main_cadence")
            .unwrap();
        fleet
            .deliver_check_waiter(cadence, "observation-a")
            .await
            .unwrap();
        assert_eq!(
            store::lookup(&db, &key.0, &key.1)
                .await
                .unwrap()
                .unwrap()
                .state,
            "recorded"
        );
        assert_eq!(
            take_frames(&mut remote)
                .iter()
                .filter(|f| matches!(f, ExecutorMessage::ResultAck { .. }))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn remote_check_adoption_shutdown_guard_preserves_only_adoptable_remote_work() {
        for consumer in ["main_cadence", "turn_end"] {
            let claim = claim(consumer, "running");
            let (fleet, _db, mut remote) = fixture(claim.clone()).await;
            let key = identity(&claim);
            let generation = fleet.link_generation(&claim.executor_id).unwrap();
            let mut request = fleet.cell_request_for_test(&claim.request_id);
            request.attempt_id = claim.attempt_id.clone();
            request.wait_horizon_unix_ms = unix_time_ms() + 60_000;
            let (entry_id, _receiver) = fleet
                .queue
                .enqueue(
                    queue::NewEntry {
                        request,
                        admission_kind: CellAdmissionKind::Command,
                        interactive: false,
                        execution_residency: None,
                        standing: false,
                    },
                    unix_time_ms(),
                )
                .unwrap();
            assert!(fleet.queue.offer(
                &entry_id,
                &claim.executor_id,
                &claim.machine,
                generation,
                unix_time_ms()
            ));
            assert_eq!(
                fleet
                    .queue
                    .claim(&entry_id, &claim.executor_id, generation, unix_time_ms()),
                queue::ClaimVerdict::Granted
            );
            fleet.runner_stop.begin(50);
            drop(EntryDropGuard {
                pool: fleet.clone(),
                request_id: key.0.clone(),
                attempt_id: key.1.clone(),
                entry_id,
                granted_to: Some(claim.executor_id.clone()),
                cause: Some(WithdrawalCause::RunnerStopped(50)),
                armed: true,
            });
            assert_eq!(
                take_frames(&mut remote)
                    .iter()
                    .any(|f| matches!(f, ExecutorMessage::Cancel { .. })),
                consumer != "main_cadence"
            );
            assert_eq!(
                fleet.left_running_attempt(&key).is_some(),
                consumer == "main_cadence"
            );
        }
    }

    #[tokio::test]
    async fn remote_check_adoption_superseded_waiter_does_not_withdraw_new_physical_grant() {
        let old = claim("manual", "running");
        let (fleet, db, _remote) = fixture(old.clone()).await;
        let old_key = identity(&old);
        let source = claim("main_cadence", "running");
        {
            let gate = fleet.check_adoptions.lock().unwrap().mutation.clone();
            let _mutation = gate.lock().await;
            fleet
                .register_check_join_locked(&old_key, "rust-tests", &source)
                .await
                .unwrap();
        }
        let mut newer = source.clone();
        newer.request_id = "new-request".into();
        newer.attempt_id = "new-attempt".into();
        let newest_waiter = store::Waiter {
            request_id: newer.request_id.clone(),
            attempt_id: newer.attempt_id.clone(),
            check_name: "rust-tests".into(),
            item_index: 0,
            consumer: "main_cadence".into(),
            consumer_ref: newer.consumer_ref.clone(),
            state: "running".into(),
            observation_id: None,
            evidence: None,
            updated_ms: 20,
        };
        store::insert_grant(&db, &newer, &[newest_waiter])
            .await
            .unwrap();
        preload(&fleet.check_adoptions, &db).await;
        fleet.withdraw_check_purpose(
            &purpose::PurposeKey::from_persisted(source.consumer_ref),
            &old_key,
        );
        tokio::time::timeout(Duration::from_secs(10), async {
            while fleet.adoption_owner(&old_key).is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            fleet.adoption_owner(&identity(&newer)),
            Some(AdoptionOwner::MainCadence)
        );
        let waiters = store::waiters(&db, &old_key.0, &old_key.1).await.unwrap();
        assert!(waiters
            .iter()
            .any(|w| w.consumer == "main_cadence" && w.state == "cancelled"));
        assert!(waiters
            .iter()
            .any(|w| w.consumer == "manual" && w.state == "running"));
    }

    #[tokio::test]
    async fn remote_check_live_alias_delivery_does_not_ack_another_consumers_projection() {
        let mut physical = claim("manual", "running");
        physical.consumer_ref = "caller-a".into();
        let (fleet, db, mut remote) = fixture(physical.clone()).await;
        let key = identity(&physical);
        let mut sibling = physical.clone();
        sibling.consumer_ref = "caller-b".into();
        {
            let gate = fleet.check_adoptions.lock().unwrap().mutation.clone();
            let _mutation = gate.lock().await;
            fleet
                .register_check_join_locked(&key, "rust-tests", &sibling)
                .await
                .unwrap();
        }
        fleet.check_adoptions.lock().unwrap().reloaded.remove(&key);
        fleet.hold_check_adoption(UnboundResult {
            executor_id: physical.executor_id.clone(),
            machine: physical.machine.clone(),
            request_id: key.0.clone(),
            attempt_id: key.1.clone(),
            outcome: outcome(&physical),
        });
        let _ = fleet.bind_adopted(key.clone(), &physical.executor_id).await;
        fleet
            .record_check_claim_observation(&key, "rust-tests", "observation")
            .await
            .unwrap();
        fleet
            .deliver_check_consumer(&key, "rust-tests", "manual", "caller-a", "observation")
            .await
            .unwrap();
        assert!(!take_frames(&mut remote)
            .iter()
            .any(|frame| matches!(frame, ExecutorMessage::ResultAck { .. })));
        let consumers = store::waiters(&db, &key.0, &key.1).await.unwrap();
        assert_eq!(
            consumers
                .iter()
                .find(|waiter| waiter.consumer_ref == "caller-b")
                .unwrap()
                .state,
            "recorded"
        );
        fleet
            .deliver_check_consumer(&key, "rust-tests", "manual", "caller-b", "observation")
            .await
            .unwrap();
        assert_eq!(
            take_frames(&mut remote)
                .iter()
                .filter(|frame| matches!(frame, ExecutorMessage::ResultAck { .. }))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn remote_check_withdrawal_uses_recorded_durable_state_not_stale_mirror() {
        let physical = claim("main_cadence", "running");
        let (fleet, db, _remote) = fixture(physical.clone()).await;
        let key = identity(&physical);
        fleet
            .record_check_claim_observation(&key, "rust-tests", "observation")
            .await
            .unwrap();
        fleet.withdraw_check_purpose(
            &purpose::PurposeKey::from_persisted(physical.consumer_ref),
            &key,
        );
        tokio::time::timeout(Duration::from_secs(10), async {
            while fleet.adoption_owner(&key).is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let consumers = store::waiters(&db, &key.0, &key.1).await.unwrap();
        assert_eq!(consumers[0].state, "recorded_undelivered");
        assert_eq!(consumers[0].observation_id.as_deref(), Some("observation"));
    }

    #[tokio::test]
    async fn live_consumer_guard_withdraws_before_result_and_after_recording_without_pinning_siblings(
    ) {
        for before_result in [true, false] {
            let mut physical = claim("manual", "running");
            physical.consumer_ref = "caller-a".into();
            let (fleet, db, mut remote) = fixture(physical.clone()).await;
            let key = identity(&physical);
            let mut sibling = physical.clone();
            sibling.consumer_ref = "caller-b".into();
            {
                let gate = fleet.check_adoptions.lock().unwrap().mutation.clone();
                let _mutation = gate.lock().await;
                fleet
                    .register_check_join_locked(&key, "rust-tests", &sibling)
                    .await
                    .unwrap();
            }
            fleet.check_adoptions.lock().unwrap().reloaded.remove(&key);
            let a = ("subscriber-a".into(), "attempt-a".into());
            let b = ("subscriber-b".into(), "attempt-b".into());
            fleet.track_check_consumer(a.clone(), key.clone(), "rust-tests", &physical);
            fleet.track_check_consumer(b.clone(), key.clone(), "rust-tests", &sibling);
            let (a_tx, a_rx) = oneshot::channel();
            let (b_tx, b_rx) = oneshot::channel();
            let coalescing = CoalescingKey::new(
                CheckResultIdentity {
                    project_id: physical.project_id.clone(),
                    check_name: "rust-tests".into(),
                    input_hash: "original-input".into(),
                },
                key.clone(),
            );
            fleet.in_flight.lock().unwrap().open(
                coalescing.clone(),
                PlacementDemand::default(),
                HashMap::from([
                    (
                        a.clone(),
                        CoalescedSubscriber {
                            waiter: a_tx,
                            priority: CellPriority::ReviewCheck,
                            requesting_job_id: None,
                        },
                    ),
                    (
                        b.clone(),
                        CoalescedSubscriber {
                            waiter: b_tx,
                            priority: CellPriority::ReviewCheck,
                            requesting_job_id: None,
                        },
                    ),
                ]),
            );
            fleet
                .deferred_acks
                .lock()
                .unwrap()
                .entry(key.0.clone())
                .or_default();
            fleet.acknowledge_settled_result(&physical.executor_id, &key);
            if before_result {
                fleet.detach_coalesced_subscriber(&b, None);
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        let consumers = store::waiters(&db, &key.0, &key.1).await.unwrap();
                        if consumers.iter().any(|waiter| {
                            waiter.consumer_ref == "caller-b" && waiter.state == "cancelled"
                        }) {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
            fleet.complete_coalesced_for_leader(&coalescing, outcome(&physical));
            let a_delivery = a_rx.await.unwrap().unwrap();
            let b_delivery = if before_result {
                None
            } else {
                Some(b_rx.await.unwrap().unwrap())
            };
            let a_guard = a_delivery.publication.consumer_guard().unwrap();
            fleet
                .record_check_claim_observation(&key, "rust-tests", "observation")
                .await
                .unwrap();
            a_guard.deliver("observation").await.unwrap();
            if !before_result {
                assert!(!take_frames(&mut remote)
                    .iter()
                    .any(|frame| matches!(frame, ExecutorMessage::ResultAck { .. })));
                drop(b_delivery);
            }
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let row = store::lookup(&db, &key.0, &key.1).await.unwrap().unwrap();
                    if row.state == "recorded" {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let consumers = store::waiters(&db, &key.0, &key.1).await.unwrap();
            let b = consumers
                .iter()
                .find(|waiter| waiter.consumer_ref == "caller-b")
                .unwrap();
            assert_eq!(
                b.state,
                if before_result {
                    "cancelled"
                } else {
                    "recorded_undelivered"
                }
            );
            if !before_result {
                assert_eq!(b.observation_id.as_deref(), Some("observation"));
            }
            let ack = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Some(frame @ ExecutorMessage::ResultAck { .. }) = remote.recv().await {
                        break frame;
                    }
                }
            })
            .await
            .unwrap();
            assert!(matches!(ack, ExecutorMessage::ResultAck { .. }));
        }
    }

    #[tokio::test]
    async fn duplicate_logical_consumers_share_one_delivery_lifetime() {
        let physical = claim("manual", "running");
        let (fleet, db, _remote) = fixture(physical.clone()).await;
        let key = identity(&physical);
        let a = ("a".into(), "a".into());
        let b = ("b".into(), "b".into());
        fleet.track_check_consumer(a.clone(), key.clone(), "rust-tests", &physical);
        fleet.track_check_consumer(b.clone(), key.clone(), "rust-tests", &physical);
        let a_guard = fleet.take_check_consumer_guard(&a).unwrap();
        let b_guard = fleet.take_check_consumer_guard(&b).unwrap();
        assert!(Arc::ptr_eq(&a_guard, &b_guard));
        drop(a_guard);
        fleet.check_adoptions.lock().unwrap().reloaded.remove(&key);
        fleet
            .record_check_claim_observation(&key, "rust-tests", "observation")
            .await
            .unwrap();
        assert_eq!(
            store::waiters(&db, &key.0, &key.1).await.unwrap()[0].state,
            "recorded"
        );
        b_guard.deliver("observation").await.unwrap();
        assert_eq!(
            store::waiters(&db, &key.0, &key.1).await.unwrap()[0].state,
            "delivered"
        );
    }

    #[tokio::test]
    async fn direct_guard_abort_before_grant_does_not_resurrect_cancelled_consumer() {
        let source = claim("manual", "running");
        let key = identity(&source);
        let db = Arc::new(crate::storage::migrated_test_db("direct-check-guard").await);
        let fleet = Fleet::default();
        fleet.check_adoptions.lock().unwrap().db = Some(db.clone());
        let _remote = fleet.attach_unreported_machine_for_test(&source.executor_id);
        fleet.register_check_claim(key.clone(), source.clone());
        let guard = fleet.check_consumer_guard_for(&key, "rust-tests", &source);
        drop(guard);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let cancelled = fleet
                    .check_adoptions
                    .lock()
                    .unwrap()
                    .consumers
                    .get(&key)
                    .is_some_and(|waiters| {
                        waiters.iter().any(|waiter| waiter.state == "cancelled")
                    });
                if cancelled {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let selected = {
            let connections = fleet.lock_connections();
            let connection = &connections[&source.executor_id];
            SelectedExecutor {
                executor_id: source.executor_id.clone(),
                device_id: connection.identity.device_id.clone(),
                generation: connection.generation,
                sender: connection.sender.clone(),
                colocated: false,
                capabilities: connection.advertisement.capabilities.clone(),
            }
        };
        fleet.claim_check_at_grant(&key, &selected, 40).await;
        let consumers = store::waiters(&db, &key.0, &key.1).await.unwrap();
        assert_eq!(consumers.len(), 1);
        assert_eq!(consumers[0].state, "cancelled");
        assert_eq!(fleet.adoption_owner(&key), None);
    }

    #[tokio::test]
    async fn direct_guard_dropped_after_result_settles_accepted_observation_without_delivery() {
        let source = claim("manual", "running");
        let key = identity(&source);
        let (fleet, db, mut remote) = fixture(source.clone()).await;
        fleet.check_adoptions.lock().unwrap().reloaded.remove(&key);
        let guard = fleet.check_consumer_guard_for(&key, "rust-tests", &source);
        let publication =
            PublicationCoordination::for_request(key.clone()).with_consumer_guard(guard);
        fleet.hold_check_adoption(UnboundResult {
            executor_id: source.executor_id.clone(),
            machine: source.machine.clone(),
            request_id: key.0.clone(),
            attempt_id: key.1.clone(),
            outcome: outcome(&source),
        });
        let _ = fleet.bind_adopted(key.clone(), &source.executor_id).await;
        fleet
            .record_check_claim_observation(&key, "rust-tests", "observation")
            .await
            .unwrap();
        assert!(!take_frames(&mut remote)
            .iter()
            .any(|frame| matches!(frame, ExecutorMessage::ResultAck { .. })));
        drop(publication);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if store::lookup(&db, &key.0, &key.1)
                    .await
                    .unwrap()
                    .unwrap()
                    .state
                    == "recorded"
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            store::waiters(&db, &key.0, &key.1).await.unwrap()[0].state,
            "recorded_undelivered"
        );
    }

    #[tokio::test]
    async fn withdrawal_retry_finishes_physical_after_terminal_waiter_commit() {
        let source = claim("main_cadence", "running");
        let key = identity(&source);
        let (fleet, db, _remote) = fixture(source.clone()).await;
        fleet
            .record_check_claim_observation(&key, "rust-tests", "observation")
            .await
            .unwrap();
        db.execute_script("CREATE TRIGGER reject_physical_record BEFORE UPDATE OF state ON check_lane_adoptions WHEN NEW.state='recorded' BEGIN SELECT RAISE(ABORT,'reject physical record'); END;").await.unwrap();
        assert!(fleet
            .withdraw_check_consumer(
                &key,
                "rust-tests",
                "main_cadence",
                &source.consumer_ref,
                "caller ended"
            )
            .await
            .is_err());
        assert_eq!(
            store::waiters(&db, &key.0, &key.1).await.unwrap()[0].state,
            "recorded_undelivered"
        );
        db.execute_script("DROP TRIGGER reject_physical_record;")
            .await
            .unwrap();
        fleet
            .withdraw_check_consumer(
                &key,
                "rust-tests",
                "main_cadence",
                &source.consumer_ref,
                "caller ended",
            )
            .await
            .unwrap();
        assert_eq!(
            store::lookup(&db, &key.0, &key.1)
                .await
                .unwrap()
                .unwrap()
                .state,
            "recorded"
        );
    }

    #[tokio::test]
    async fn stale_withdrawal_waiting_on_gate_does_not_cancel_new_logical_lifetime() {
        let source = claim("main_cadence", "running");
        let key = identity(&source);
        let (fleet, db, _remote) = fixture(source.clone()).await;
        fleet.check_adoptions.lock().unwrap().reloaded.remove(&key);
        let old = fleet.check_consumer_guard_for(&key, "rust-tests", &source);
        let gate = fleet.check_adoptions.lock().unwrap().mutation.clone();
        let lock = gate.lock().await;
        drop(old);
        tokio::task::yield_now().await;
        let newer = fleet.check_consumer_guard_for(&key, "rust-tests", &source);
        drop(lock);
        {
            let _completed_cleanup = gate.lock().await;
        }
        assert_eq!(
            store::waiters(&db, &key.0, &key.1).await.unwrap()[0].state,
            "running"
        );
        fleet
            .record_check_claim_observation(&key, "rust-tests", "observation")
            .await
            .unwrap();
        newer.deliver("observation").await.unwrap();
        assert_eq!(
            store::lookup(&db, &key.0, &key.1)
                .await
                .unwrap()
                .unwrap()
                .state,
            "recorded"
        );
    }

    #[tokio::test]
    async fn superseded_last_recorded_purpose_finishes_physical_and_acknowledges() {
        let source = claim("main_cadence", "running");
        let key = identity(&source);
        let (fleet, db, mut remote) = fixture(source.clone()).await;
        send_result(&fleet, &source);
        let _ = fleet.bind_adopted(key.clone(), &source.executor_id).await;
        fleet
            .record_check_claim_observation(&key, "rust-tests", "observation")
            .await
            .unwrap();
        fleet.withdraw_check_purpose(
            &purpose::PurposeKey::from_persisted(source.consumer_ref.clone()),
            &key,
        );
        let frame = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(frame @ ExecutorMessage::ResultAck { .. }) = remote.recv().await {
                    break frame;
                }
            }
        })
        .await
        .unwrap();
        assert!(matches!(frame, ExecutorMessage::ResultAck { .. }));
        let row = store::lookup(&db, &key.0, &key.1).await.unwrap().unwrap();
        assert_eq!(row.state, "recorded");
        let consumers = store::waiters(&db, &key.0, &key.1).await.unwrap();
        assert_eq!(consumers[0].state, "recorded_undelivered");
        assert_eq!(consumers[0].observation_id.as_deref(), Some("observation"));
    }

    #[tokio::test]
    async fn authoritative_removal_and_overlap_loss_retry_after_store_recovery_without_a_second_action(
    ) {
        for operator in [false, true] {
            let source = claim("main_cadence", "running");
            let key = identity(&source);
            let (fleet, db, mut remote) = fixture(source.clone()).await;
            let (probe, mut retries) = mpsc::unbounded_channel();
            fleet.check_adoptions.lock().unwrap().settlement_retry_probe = Some(probe);
            db.execute_script("CREATE TRIGGER reject_authoritative_loss BEFORE UPDATE OF state ON check_lane_adoptions WHEN NEW.state='lost' BEGIN SELECT RAISE(ABORT,'reject authoritative loss'); END;").await.unwrap();
            let mut binding = Box::pin(fleet.bind_adopted(key.clone(), &source.executor_id));
            tokio::select! { biased; _ = &mut binding => panic!("no loss evidence yet"), _ = std::future::ready(()) => {} }
            if operator {
                fleet
                    .authorize_purpose_overlap(
                        &purpose::PurposeKey::from_persisted(source.consumer_ref.clone()),
                        &key,
                        Some(&source.executor_id),
                        "operator",
                    )
                    .unwrap();
            } else {
                fleet.settle_purpose_debts_for_lost_machine(
                    &source.executor_id,
                    "enrollment removed",
                );
            }
            let (retried, original_evidence) =
                tokio::time::timeout(Duration::from_secs(10), retries.recv())
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(retried, key);
            assert!(original_evidence.contains(if operator {
                "authorized the overlap"
            } else {
                "was removed"
            }));
            assert_eq!(
                store::lookup(&db, &key.0, &key.1)
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                "running"
            );
            assert_eq!(
                store::waiters(&db, &key.0, &key.1).await.unwrap()[0].state,
                "running",
                "failed settlement rolls every waiter back"
            );
            assert!(
                fleet.check_adoptions.lock().unwrap().losing.contains(&key),
                "the original retry retains ownership"
            );
            assert_eq!(fleet.purpose_debts().len(), 1);
            assert!(!take_frames(&mut remote).iter().any(|frame| matches!(
                frame,
                ExecutorMessage::Cancel { .. } | ExecutorMessage::ResultAck { .. }
            )));
            tokio::select! { biased; _ = &mut binding => panic!("failed persistence must not release its waiter"), _ = std::future::ready(()) => {} }
            db.execute_script("DROP TRIGGER reject_authoritative_loss;")
                .await
                .unwrap();
            let executed = tokio::time::timeout(Duration::from_secs(15), binding)
                .await
                .unwrap();
            assert!(matches!(executed.outcome, CellOutcome::Unavailable { .. }));
            let row = store::lookup(&db, &key.0, &key.1).await.unwrap().unwrap();
            assert_eq!(row.state, "lost");
            assert_eq!(
                row.evidence.as_deref(),
                Some(original_evidence.as_str()),
                "retry does not regenerate evidence or its timestamp"
            );
            assert_eq!(
                store::waiters(&db, &key.0, &key.1).await.unwrap()[0].state,
                "lost"
            );
            assert!(
                fleet.purpose_debts().is_empty(),
                "committed authoritative evidence releases the successor gate"
            );
        }
    }

    #[tokio::test]
    async fn fleet_loss_requires_durable_cadence_interruption_before_release_and_retries_original_evidence(
    ) {
        use crate::storage::RowExt;
        for cause in ["omitted", "removed", "overlap"] {
            let mut source = claim("main_cadence", "running");
            source.updated_ms = 17;
            source.stopped_at_ms = (cause != "removed").then_some(13);
            let key = identity(&source);
            let (fleet, db, mut remote) = fixture(source.clone()).await;
            let (probe, mut retries) = mpsc::unbounded_channel();
            fleet.check_adoptions.lock().unwrap().settlement_retry_probe = Some(probe);
            db.execute_script(
                "CREATE TRIGGER reject_fleet_interruption BEFORE INSERT ON main_cadence_interruptions
                 BEGIN SELECT RAISE(ABORT,'reject fleet interruption'); END;
                 CREATE TRIGGER require_fleet_interruption BEFORE UPDATE OF state ON check_lane_adoptions
                 WHEN NEW.state='lost' BEGIN
                   SELECT RAISE(ABORT,'lost before interruption') WHERE NOT EXISTS
                     (SELECT 1 FROM main_cadence_interruptions WHERE project_id=NEW.project_id
                      AND check_name='rust-tests' AND platform=NEW.platform AND commit_sha=NEW.commit_sha
                      AND stopped_at=COALESCE(NEW.stopped_at_ms,17));
                 END;"
            ).await.unwrap();
            let mut binding = Box::pin(fleet.bind_adopted(key.clone(), &source.executor_id));
            tokio::select! { biased; _ = &mut binding => panic!("no loss evidence yet"), _ = std::future::ready(()) => {} }
            assert!(fleet.has_pending_adopted_binder(&key));
            match cause {
                "omitted" => account(&fleet, &source, false),
                "removed" => fleet.settle_purpose_debts_for_lost_machine(
                    &source.executor_id,
                    "enrollment removed",
                ),
                "overlap" => {
                    fleet
                        .authorize_purpose_overlap(
                            &purpose::PurposeKey::from_persisted(source.consumer_ref.clone()),
                            &key,
                            Some(&source.executor_id),
                            "operator",
                        )
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let (retried, original_evidence) =
                tokio::time::timeout(Duration::from_secs(10), retries.recv())
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(retried, key);
            assert_eq!(
                store::lookup(&db, &key.0, &key.1)
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                "running"
            );
            assert_eq!(
                store::waiters(&db, &key.0, &key.1).await.unwrap()[0].state,
                "running"
            );
            assert!(fleet.check_claim(&key).is_some());
            assert!(fleet.has_pending_adopted_binder(&key));
            assert_eq!(fleet.purpose_debts().len(), 1);
            assert!(!take_frames(&mut remote).iter().any(|frame| matches!(
                frame,
                ExecutorMessage::Cancel { .. } | ExecutorMessage::ResultAck { .. }
            )));
            tokio::select! { biased; _ = &mut binding => panic!("failed interruption must not release the binder"), _ = std::future::ready(()) => {} }
            db.execute_script("DROP TRIGGER reject_fleet_interruption;")
                .await
                .unwrap();
            let executed = tokio::time::timeout(Duration::from_secs(15), binding)
                .await
                .unwrap();
            assert!(matches!(executed.outcome, CellOutcome::Unavailable { .. }));
            let lost = store::lookup(&db, &key.0, &key.1).await.unwrap().unwrap();
            assert_eq!(lost.state, "lost");
            assert_eq!(lost.evidence.as_deref(), Some(original_evidence.as_str()));
            assert!(fleet.purpose_debts().is_empty());
            assert!(fleet.check_claim(&key).is_none());
            let interruptions = db.query_all(
                "SELECT project_id,check_name,platform,commit_sha,stopped_at FROM main_cadence_interruptions",
                (), |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?, row.text(3)?, row.i64(4)?)),
            ).await.unwrap();
            assert_eq!(
                interruptions,
                vec![(
                    source.project_id,
                    "rust-tests".into(),
                    source.platform,
                    source.commit_sha,
                    source.stopped_at_ms.unwrap_or(source.updated_ms)
                )],
                "loss before the boot scan must leave one canonical durable repair citation"
            );
            assert_eq!(
                store::waiters(&db, &key.0, &key.1).await.unwrap()[0].state,
                "lost"
            );
        }
    }

    #[tokio::test]
    async fn sole_manual_consumer_abort_before_result_settles_before_exactly_one_ack() {
        sole_manual_abandonment(false, false, false).await;
    }

    #[tokio::test]
    async fn sole_manual_consumer_abort_after_result_settles_before_exactly_one_ack() {
        sole_manual_abandonment(true, false, false).await;
    }

    #[tokio::test]
    async fn sole_manual_consumer_terminal_settlement_retries_store_failure() {
        sole_manual_abandonment(false, true, false).await;
    }

    #[tokio::test]
    async fn late_consumer_can_record_a_tombstoned_execution_before_terminal() {
        sole_manual_abandonment(false, false, true).await;
    }

    async fn sole_manual_abandonment(result_first: bool, reject_settlement: bool, late_join: bool) {
        use crate::storage::SearchIndex;
        use cairn_common::executor_protocol::{ProcessBatchExecution, ProcessBatchItem};
        use std::sync::atomic::AtomicUsize;
        let home = tempfile::tempdir().unwrap();
        let db = Arc::new(
            LocalDb::open_migrated_for_test(home.path().join("runner.db"))
                .await
                .unwrap(),
        );
        let search = Arc::new(SearchIndex::open_or_create(home.path().join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(crate::db::DbState::new(db.clone(), search)),
            Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            home.path().to_path_buf(),
        )
        .build();
        orch.fleet.await_restart_adoptions_loaded().await;
        let mut physical = claim("manual", "template");
        physical.project_id = "p".into();
        physical.consumer_ref = "sole-manual".into();
        let key = identity(&physical);
        let mut remote = orch
            .fleet
            .attach_reported_machine_for_test(&physical.executor_id, &[]);
        orch.fleet
            .register_check_claim(key.clone(), physical.clone());
        let mut request = orch.fleet.cell_request_for_test(&key.0);
        request.attempt_id = key.1.clone();
        request.project_id = physical.project_id.clone();
        request.base_commit = physical.commit_sha.clone();
        request.repository = RepositoryLocator::ScratchOnly {
            owner_id: physical.project_id.clone(),
        };
        request.pinned_executor_id = Some(physical.executor_id.clone());
        request.placement_work_class = PlacementWorkClass::ReviewChecks;
        request.wait_horizon_unix_ms = u64::MAX;
        let item = PureVerdictBatchItem {
            result_identity: CheckResultIdentity {
                project_id: physical.project_id.clone(),
                check_name: "rust-tests".into(),
                input_hash: "original-input".into(),
            },
            process: ProcessBatchItem {
                header: "rust-tests".into(),
                stream_id: "sole-manual:0".into(),
                execution: ProcessBatchExecution::NativeShell,
                program: String::new(),
                args: vec!["echo check".into()],
                env: vec![],
                stdin: None,
                timeout_ms: 1000,
                command_resource_identity: None,
                verdict_environment_names: vec![],
                silence_bound_ms: 0,
            },
        };
        let follower_request = request.clone();
        let follower_item = PureVerdictBatchItem {
            result_identity: item.result_identity.clone(),
            process: item.process.clone(),
        };
        let (delivered, mut delivery_seen) = oneshot::channel();
        let submitting = orch.clone();
        let caller = tokio::spawn(async move {
            let deliveries = submitting
                .fleet
                .submit_pure_verdict_batch(&submitting, request, vec![item], None, None, None)
                .await;
            let outcomes = format!(
                "{:?}",
                deliveries
                    .iter()
                    .map(|delivery| delivery.as_ref().map(|delivery| &delivery.outcome))
                    .collect::<Vec<_>>()
            );
            let _ = delivered.send(outcomes.clone());
            assert!(
                deliveries.iter().all(Result::is_ok),
                "submission outcomes: {outcomes}"
            );
            std::future::pending::<()>().await;
            drop(deliveries);
        });
        let entry_id = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                tokio::select! {
                    frame = remote.recv() => if let Some(ExecutorMessage::Offer { entry_id, .. }) = frame { break entry_id; },
                    outcomes = &mut delivery_seen => panic!("submission ended before Offer: {outcomes:?}"),
                }
            }
        }).await.unwrap();
        let generation = orch.fleet.link_generation(&physical.executor_id).unwrap();
        orch.fleet.handle_executor_message(
            &physical.executor_id,
            generation,
            ExecutorMessage::Claim { entry_id },
        );
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Some(ExecutorMessage::Granted {
                    request,
                    batch: Some(batch),
                    ..
                }) = remote.recv().await
                {
                    assert_eq!((request.request_id, request.attempt_id), key);
                    assert_eq!(batch.items.len(), 1);
                    break;
                }
            }
        })
        .await
        .unwrap();
        let initial = store::waiters(&db, &key.0, &key.1).await.unwrap();
        assert_eq!(
            initial.len(),
            1,
            "no other recorder may mask the sole-consumer case"
        );
        assert_eq!(initial[0].consumer, "manual");
        let ack_count = Arc::new(AtomicUsize::new(0));
        let probe_count = ack_count.clone();
        let probe_db = db.clone();
        let probe_key = key.clone();
        orch.fleet
            .set_result_ack_probe_for_test(Arc::new(move |request, attempt| {
                assert_eq!(
                    (request, attempt),
                    (probe_key.0.as_str(), probe_key.1.as_str())
                );
                crate::storage::run_db_blocking(|| async {
                    let claim = store::lookup(&probe_db, request, attempt)
                        .await
                        .map_err(|e| e.to_string())?
                        .unwrap();
                    assert_eq!(
                        claim.state,
                        if late_join { "recorded" } else { "cancelled" }
                    );
                    let consumers = store::waiters(&probe_db, request, attempt)
                        .await
                        .map_err(|e| e.to_string())?;
                    assert_eq!(consumers.len(), if late_join { 2 } else { 1 });
                    assert!(consumers
                        .iter()
                        .all(|w| matches!(w.state.as_str(), "cancelled" | "delivered")));
                    if !late_join {
                        assert!(consumers.iter().all(|w| w.observation_id.is_none()));
                    }
                    Ok(())
                })
                .unwrap();
                probe_count.fetch_add(1, Ordering::SeqCst);
            }));
        if result_first {
            send_result(&orch.fleet, &physical);
            tokio::time::timeout(Duration::from_secs(10), &mut delivery_seen)
                .await
                .unwrap()
                .unwrap();
        }
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if store::waiters(&db, &key.0, &key.1).await.unwrap()[0].state == "cancelled" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let follower = if late_join {
            assert_eq!(
                store::lookup(&db, &key.0, &key.1)
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                "running",
                "withdrawal cannot settle a joinable physical tombstone"
            );
            let mut source = physical.clone();
            source.request_id = "late-request".into();
            source.attempt_id = "late-attempt".into();
            source.consumer_ref = "late-manual".into();
            orch.fleet
                .register_check_claim(identity(&source), source.clone());
            let mut request = follower_request;
            request.request_id = source.request_id;
            request.attempt_id = source.attempt_id;
            let joining = orch.clone();
            let task = tokio::spawn(async move {
                joining
                    .fleet
                    .submit_pure_verdict_batch(
                        &joining,
                        request,
                        vec![follower_item],
                        None,
                        None,
                        None,
                    )
                    .await
            });
            tokio::time::timeout(Duration::from_secs(10), async {
                while store::waiters(&db, &key.0, &key.1).await.unwrap().len() != 2 {
                    assert!(
                        !task.is_finished(),
                        "late join must wait on the original physical execution"
                    );
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            Some(task)
        } else {
            None
        };
        let (probe, mut retries) = mpsc::unbounded_channel();
        if reject_settlement {
            orch.fleet
                .check_adoptions
                .lock()
                .unwrap()
                .settlement_retry_probe = Some(probe);
            db.execute_script("CREATE TRIGGER reject_abandoned_terminal BEFORE UPDATE OF state ON check_lane_adoptions WHEN NEW.state='cancelled' BEGIN SELECT RAISE(ABORT,'reject abandoned terminal'); END;").await.unwrap();
        }
        if !result_first {
            send_result(&orch.fleet, &physical);
        }
        if reject_settlement {
            let (retried, evidence) = tokio::time::timeout(Duration::from_secs(10), retries.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(retried, key);
            assert!(evidence.contains("no live consumer"));
            assert_eq!(ack_count.load(Ordering::SeqCst), 0);
            assert_eq!(
                store::lookup(&db, &key.0, &key.1)
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                "running"
            );
            db.execute_script("DROP TRIGGER reject_abandoned_terminal;")
                .await
                .unwrap();
        }
        if let Some(follower) = follower {
            let deliveries = tokio::time::timeout(Duration::from_secs(10), follower)
                .await
                .unwrap()
                .unwrap();
            let delivery = deliveries.into_iter().next().unwrap().unwrap();
            orch.fleet
                .record_check_claim_observation(&key, "rust-tests", "late-observation")
                .await
                .unwrap();
            delivery
                .publication
                .consumer_guard()
                .unwrap()
                .deliver("late-observation")
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Some(ExecutorMessage::ResultAck {
                    request_id,
                    attempt_id,
                }) = remote.recv().await
                {
                    assert_eq!((request_id, attempt_id), key);
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(ack_count.load(Ordering::SeqCst), 1);
        assert!(!take_frames(&mut remote).iter().any(|f| matches!(
            f,
            ExecutorMessage::ResultAck { .. }
                | ExecutorMessage::Granted { .. }
                | ExecutorMessage::Offer { .. }
        )));
        assert!(orch.fleet.check_claim(&key).is_none());
    }

    #[test]
    fn terminal_undurable_check_attempts_do_not_accumulate_consumer_metadata() {
        let fleet = Fleet::default();
        for index in 0..64 {
            let mut source = claim("manual", "template");
            source.request_id = format!("undurable-{index}");
            let key = identity(&source);
            fleet.register_check_claim(key.clone(), source.clone());
            let guard = fleet.check_consumer_guard_for(&key, "rust-tests", &source);
            fleet.track_check_consumer(
                ("subscriber".into(), index.to_string()),
                key.clone(),
                "rust-tests",
                &source,
            );
            fleet.check_leader_completed(&key);
            // Settled only to keep this synchronous map-lifetime pin independent
            // of the asynchronous withdrawal behavior covered above.
            guard.settled.store(true, Ordering::Release);
        }
        let state = fleet.check_adoptions.lock().unwrap();
        assert!(state.templates.is_empty());
        assert!(state.consumers.is_empty());
        assert!(state.consumer_guards.is_empty());
        assert!(state.subscriber_consumers.is_empty());
        assert!(state.terminal.is_empty());
    }

    #[test]
    fn remote_check_adoption_requires_complete_bounded_plans() {
        let mut claim = claim("main_cadence", "running");
        assert!(can_adopt_claim(&claim));
        claim.recording_plans.clear();
        assert!(!can_adopt_claim(&claim));
        claim.recording_plans = vec![serde_json::Value::Null];
        assert!(!can_adopt_claim(&claim));
        claim.recording_plans = vec![serde_json::Value::String("x".repeat(256 * 1024))];
        assert!(!can_adopt_claim(&claim));
    }
}
