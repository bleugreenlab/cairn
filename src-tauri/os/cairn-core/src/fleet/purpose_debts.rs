use super::*;

/// Releases a purpose item whose caller stopped before a leader of its own took
/// it over, or whose only stake was in executions other leaders opened.
///
/// Such an item never reached a machine itself, so its end is its caller's end:
/// there is nothing running to wait for. Without this, a caller dropped at the
/// purpose gate, or a joiner whose subscriptions ended, would leave its
/// question held by an answer nothing will ever finish.
pub(super) struct PurposeAdmissionGuard {
    pub(super) pool: Fleet,
    pub(super) item: RequestIdentity,
    pub(super) armed: bool,
}

impl Drop for PurposeAdmissionGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if !std::thread::panicking() {
            self.pool.withdraw_joiner_check_consumers(&self.item);
        }
        lock_through_poison(&self.pool.purpose_joiners).remove(&self.item);
        lock_through_poison(&self.pool.cancelled_leaders).remove(&self.item);
        if lock_through_poison(&self.pool.purposes).finish(&self.item) {
            self.pool.purpose_quiesced.notify_waiters();
        }
    }
}

/// One question held back from its next answer because an earlier answer may
/// still be running: the operator's view of a purpose debt.
///
/// The identifiers are carried so the app can name the exact overlap when an
/// operator authorizes it; everything else is already in an operator's words.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockedQuestion {
    pub question: String,
    pub request_id: String,
    pub attempt_id: String,
    /// When, in the item's life, it was retired.
    pub phase: String,
    /// `None` for an item that was never placed on a machine.
    pub executor_id: Option<String>,
    /// That machine's public name.
    pub machine: Option<String>,
    /// The machine and connection generation, or "never placed on a machine".
    pub placement: String,
    pub retired_at_unix_ms: u64,
    pub retired_ago: String,
    pub cause: String,
    pub confirmation: String,
    /// Nothing is waiting on the attempt any more; only machine evidence or an
    /// operator settles it.
    pub unconfirmed: bool,
    pub settles_when: String,
}

/// Whether the purpose debts a previous runner process left have been read
/// back. Purpose admission waits on it.
///
/// Loaded from the start on a fleet with no store, which is every bare
/// `Fleet::default()`.
#[derive(Clone)]
pub(super) struct PurposeDebtsLoaded(pub(super) Arc<tokio::sync::watch::Sender<bool>>);

/// Each purpose item that only joined executions other leaders opened, with
/// the question it answers and the subscriber identities it waits under.
pub(super) type PurposeJoiners =
    HashMap<RequestIdentity, (purpose::PurposeKey, Vec<RequestIdentity>)>;

impl Default for PurposeDebtsLoaded {
    fn default() -> Self {
        Self(Arc::new(tokio::sync::watch::channel(true).0))
    }
}

/// The wait horizon for a caller with no tighter answer of its own.
///
/// A terminal, a REPL, a workflow, a dev instance: none of these has a
/// principled bound on how long it will wait for a busy machine, only on how
/// long it will wait for a broken one — and silence, not elapsed time, is what
/// says broken. So they declare the machine-wide default and let the liveness
/// report be what frees the queue slot if their caller goes away.
///
/// A batch that stated its own bound does not come here. `run` derives a horizon
/// from the item timeouts the agent declared, because a batch that bounded all of
/// its work said what that work is worth.
pub(crate) fn default_wait_horizon_unix_ms(config: &FleetConfig) -> u64 {
    unix_time_ms().saturating_add(config.capacity_wait_horizon_ms())
}

impl Fleet {
    /// Give purpose debts a durable journal, and read back what a previous
    /// runner process left owed.
    ///
    /// Every purpose item granted to a machine is journaled until evidence
    /// settles it, because a remote machine keeps running work through a
    /// runner restart. The rows reload as unconfirmed debts before any purpose
    /// item is admitted: admission waits for the reload, so nothing dispatches
    /// beside a debt this process has not read yet.
    ///
    /// Connections are accepted while the read runs, which leaves two windows,
    /// both closed. A machine the runner loses during the read is remembered,
    /// and the rows naming it settle as they load. A snapshot that arrives
    /// during the read finds nothing reloaded to account for, so the reloaded
    /// debts are read against that machine's next snapshot instead, which is
    /// equally an account from after the granting process stopped. A failed read is reported
    /// and admission proceeds, because blocking every attestation forever on an
    /// unreadable table would be worse than the overlap it might permit.
    pub(crate) fn attach_purpose_debt_journal(&self, db: Arc<crate::storage::LocalDb>) {
        let (sender, mut changes) = mpsc::unbounded_channel();
        self.purposes.lock().unwrap().attach_journal(sender);
        self.purpose_debts_loaded.0.send_replace(false);
        self.purposes.lock().unwrap().expect_reload();
        let boot_unix_ms = unix_time_ms();
        self.check_adoptions.lock().unwrap().db = Some(db.clone());
        let check_adoptions = self.check_adoptions.clone();
        // Weak on purpose: the registry owns the journal's sender, so a task
        // that held it would keep its own channel, and the database, open for
        // as long as the process lived.
        let registry = Arc::downgrade(&self.purposes);
        let errors = Arc::downgrade(&self.purpose_debt_journal_error);
        let loaded = self.purpose_debts_loaded.clone();
        let failed_loaded = loaded.clone();
        let failed_errors = errors.clone();
        // The task spawns nothing, so a runtime of its own is enough when the
        // caller has none.
        crate::orchestrator::spawn_onto(
            tokio::runtime::Handle::try_current().ok().as_ref(),
            async move {
                check_adoptions::preload(&check_adoptions, &db).await;
                match purpose::load_rows(&db).await {
                    Ok(mut rows) => {
                        rows.extend(check_adoptions::purpose_rows(&check_adoptions));
                        if !rows.is_empty() {
                            log::warn!(
                                "reloaded {} purpose debt(s) a previous runner process left on machines; each blocks its question until that machine accounts for it",
                                rows.len()
                            );
                        }
                        if let Some(registry) = registry.upgrade() {
                            let settled = registry.lock().unwrap().reload(rows, boot_unix_ms);
                            for debt in settled {
                                log::info!(
                                    "purpose {} debt for request {} attempt {} settled as it reloaded: its machine was lost while the journal was being read",
                                    debt.key,
                                    debt.request.0,
                                    debt.request.1,
                                );
                            }
                        }
                    }
                    Err(error) => {
                        log::error!("{error}");
                        if let Some(registry) = registry.upgrade() {
                            registry.lock().unwrap().reload(
                                check_adoptions::purpose_rows(&check_adoptions),
                                boot_unix_ms,
                            );
                        }
                        if let Some(errors) = errors.upgrade() {
                            *errors.lock().unwrap() = Some(error);
                        }
                    }
                }
                loaded.0.send_replace(true);
                while let Some(first) = changes.recv().await {
                    let mut batch = vec![first];
                    while let Ok(next) = changes.try_recv() {
                        batch.push(next);
                    }
                    if let Err(error) = purpose::apply_changes(&db, batch, unix_time_ms()).await {
                        // The registry stays authoritative in this process;
                        // what is lost is the next process's memory of it.
                        log::warn!("{error}");
                        if let Some(errors) = errors.upgrade() {
                            *errors.lock().unwrap() = Some(error);
                        }
                    }
                }
            },
            move || {
                failed_loaded.0.send_replace(true);
                let error =
                    "purpose debt journal could not start; debts stay in memory only".to_string();
                log::error!("{error}");
                if let Some(errors) = failed_errors.upgrade() {
                    *errors.lock().unwrap() = Some(error);
                }
            },
        );
    }

    /// Resolves once debts journaled by a previous runner process are back.
    pub(super) async fn await_purpose_debts_loaded(&self) {
        let mut loaded = self.purpose_debts_loaded.0.subscribe();
        let _ = loaded.wait_for(|loaded| *loaded).await;
    }

    /// The last failure to make a purpose debt change durable, if any.
    pub(crate) fn purpose_debt_journal_error(&self) -> Option<String> {
        self.purpose_debt_journal_error.lock().unwrap().clone()
    }
}

/// What the dispatch gate decided about an item that has won its question.
pub(super) enum PurposeGate {
    /// The question owes no unconfirmed deaths; this item may dispatch.
    Clear,
    /// A newer answer took the question while this item waited at the gate.
    /// It never dispatched, so it has been settled and must not proceed.
    Displaced,
    /// The debt did not clear within the budget.
    Owed {
        outstanding: Vec<purpose::PurposeDebtView>,
    },
}

impl Fleet {
    /// Wait until this question owes no outstanding debts, or until this
    /// caller's budget is spent.
    ///
    /// The gate that separates "one holder is recorded" from "one work item
    /// exists". Winning a purpose key establishes which answer is newest; it
    /// says nothing about whether the answer it displaced has stopped consuming
    /// a cell. Dispatching on the strength of the record alone is what left an
    /// operator watching several attestations of one project at once, and a
    /// registry that showed a single item throughout would have made that
    /// overlap harder to see rather than impossible.
    ///
    /// What settles a debt is evidence that the displaced work is no longer
    /// running: its own terminal outcome, its machine's own account of its
    /// work, that machine being gone by this runner's authority, or an operator
    /// naming the exact overlap (see [`purpose`]). The executor's quiescence
    /// proof guards reuse of the predecessor's own tree; it says nothing about
    /// a successor placed on a different cell or a different machine, which is
    /// exactly the overlap this gate exists to prevent. The budget bounds only
    /// this caller's wait: spending it settles nothing.
    pub(super) async fn await_purpose_quiescent(
        &self,
        key: &purpose::PurposeKey,
        me: &RequestIdentity,
    ) -> PurposeGate {
        let deadline = tokio::time::Instant::now() + RETIREMENT_CONFIRMATION_BUDGET;
        loop {
            // Arm the wakeup before reading the registry. A debt cleared in the
            // gap between reading and awaiting would otherwise be a wakeup this
            // waiter never hears, and the successor would sit out the whole
            // budget for a predecessor that had already died.
            let quiesced = self.purpose_quiesced.notified();
            {
                let mut registry = self.purposes.lock().unwrap();
                // Have I been displaced while waiting here? A burst of merges
                // makes this ordinary: this item took the question from its
                // predecessor, and a newer tip took it from this one before the
                // predecessor had finished dying.
                //
                // Being displaced is what puts an item into the debt set, so
                // without this an item would wait for a set that now contains
                // itself, and could only leave it by completing work the gate is
                // still holding it back from starting. Settling it here is sound
                // precisely because this gate sits upstream of everything: an
                // item displaced at this point has not been coalesced, placed,
                // or submitted, so there is provably nothing running to confirm
                // dead. Waiting for a terminal outcome nothing will produce is
                // what deadlocked the burst.
                if !registry.is_live_holder(key, me) {
                    registry.finish(me);
                    drop(registry);
                    self.purpose_quiesced.notify_waiters();
                    return PurposeGate::Displaced;
                }
                if registry.pending_retirements(key) == 0 {
                    return PurposeGate::Clear;
                }
            }
            if tokio::time::timeout_at(deadline, quiesced).await.is_err() {
                let outstanding = self.purposes.lock().unwrap().debts_for(key);
                if outstanding.is_empty() {
                    continue;
                }
                return PurposeGate::Owed { outstanding };
            }
        }
    }

    /// Terminate a work item the chokepoint has displaced or released.
    ///
    /// The one retirement path: displacement by a newer answer, an operator's
    /// retraction, and the retirement of a purpose item that only joined
    /// another leader's execution all come through here. The registry has
    /// already recorded the debt; this ends the work behind it.
    ///
    /// What ending it takes depends on where the work is, and the retirement is
    /// recorded before anything is delivered because the item may not be on the
    /// wire yet. Work this runner still holds (being prepared, queued, offered)
    /// is ended by the runner itself: the retirement wakes the placement loop in
    /// [`Fleet::execute_entry`], which withdraws the entry and answers
    /// `Cancelled`, and that answer is the attempt's own terminal outcome. It
    /// must not wait for a machine to claim the work first: a check's wait
    /// horizon is unbounded, and the only machine that could claim it may
    /// never come. Work already granted is told with `Cancel`, and an executor
    /// built from this tree observes that at every phase transition it has
    /// left, because it binds the request identity when it claims the offer.
    ///
    /// The last clause of "terminated means dead" — that the item's process
    /// tree is reaped and its checkout quiescent before the cell is reused — is
    /// deliberately not enforced here, because it is already enforced where the
    /// tree actually is. The executor takes one `TreeQuiescent` proof for the
    /// length of a batch hold and every reset on the cancellation path consumes
    /// it, so a retirement delivered from here cannot reach a tree reset that
    /// skipped the reaper: the type system on the far side refuses to compile
    /// one. This path composes with that gate rather than duplicating it, and
    /// duplicating it would be worse than redundant — a second liveness opinion
    /// formed a network hop away from the processes it describes is exactly the
    /// stale copy of a remote truth this design exists to abolish.
    pub(super) fn retire_item(&self, request: &RequestIdentity, cause: CancelCause) {
        let keys: Vec<_> = self
            .purposes
            .lock()
            .unwrap()
            .debts()
            .into_iter()
            .filter(|debt| &debt.request == request)
            .map(|debt| debt.key)
            .collect();
        for key in keys {
            self.withdraw_check_purpose(&key, request);
        }
        self.cancelled_leaders
            .lock()
            .unwrap()
            .insert(request.clone());
        self.cancel_causes
            .lock()
            .unwrap()
            .record(request.clone(), cause.clone());
        self.retirements.notify_waiters();
        self.detach_purpose_joiner(request, cause);
        if let Some((executor_id, generation)) = self.queue.binding_of(request) {
            let _ = self.send_to(
                &executor_id,
                generation,
                ExecutorMessage::Cancel {
                    request_id: request.0.clone(),
                    attempt_id: request.1.clone(),
                },
            );
        }
    }

    /// Batch cancellation also covers the gap before an attempt entered the
    /// queue, when a request-addressed fleet cancel could not find it yet.
    pub(super) async fn retired_or_stopped(
        &self,
        key: &RequestIdentity,
        stop: Option<&PlacementStop>,
    ) {
        tokio::select! {
            () = self.retired(key) => {}
            () = async {
                match stop {
                    Some(stop) => stop.stopped().await,
                    None => std::future::pending().await,
                }
            } => {}
        }
    }

    /// Resolves once `key` has been retired, taking the retirement.
    pub(super) async fn retired(&self, key: &RequestIdentity) {
        loop {
            let notified = self.retirements.notified();
            if self.cancelled_leaders.lock().unwrap().remove(key) {
                return;
            }
            notified.await;
        }
    }

    /// End a retired purpose item's subscriptions to executions other leaders
    /// opened.
    ///
    /// The item has no leader of its own, so its subscriptions ending is its
    /// end. An execution it was the last subscriber to is retired by that, and
    /// may already be on a machine; the question then owes that execution's
    /// death in the item's place.
    pub(super) fn detach_purpose_joiner(&self, request: &RequestIdentity, cause: CancelCause) {
        self.withdraw_joiner_check_consumers(request);
        let Some((key, subscribers)) = self.purpose_joiners.lock().unwrap().remove(request) else {
            return;
        };
        let retirement = self
            .purposes
            .lock()
            .unwrap()
            .debts_for(&key)
            .into_iter()
            .find(|debt| &debt.request == request)
            .map(|debt| debt.cause)
            .unwrap_or(purpose::RetirementCause::Retracted);
        for subscriber in subscribers {
            if let Detachment::Retired { leader: joined } =
                self.detach_coalesced_subscriber(&subscriber, Some(cause.clone()))
            {
                let placement =
                    self.queue
                        .claimed_binding_of(&joined)
                        .map(|(executor_id, generation)| purpose::DebtPlacement {
                            executor_id,
                            generation,
                            // The queue keeps no grant time for the joined
                            // execution; this is when the question took on
                            // its debt.
                            dispatched_at_unix_ms: unix_time_ms(),
                            awaiting_post_boot_account: false,
                        });
                self.purposes.lock().unwrap().owe_for_joined(
                    &key,
                    joined,
                    placement,
                    retirement.clone(),
                    unix_time_ms(),
                );
            }
        }
    }

    /// Record how a leader's attempt ended against its question.
    ///
    /// The terminal outcome is what actually retires an item, and this is
    /// where it lands. Fenced by identity inside the registry, so a displaced
    /// item clears only its own debt and never the entry its replacement now
    /// holds; the wakeup then releases whichever successor was waiting on this
    /// death. A grant the runner stopped waiting on is not an outcome: the
    /// work may still be running on that machine, so the debt stays,
    /// unconfirmed, until the machine's own word or its loss settles it.
    pub(super) fn settle_leader_purpose(
        &self,
        leader: &RequestIdentity,
        abandoned_grant: Option<String>,
    ) {
        let changed = {
            let mut registry = self.purposes.lock().unwrap();
            match abandoned_grant {
                Some(detail) => registry.end_unconfirmed(
                    leader,
                    purpose::UnconfirmedReason::GrantAbandoned { detail },
                    unix_time_ms(),
                ),
                None => registry.finish(leader),
            }
        };
        if changed {
            self.purpose_quiesced.notify_waiters();
        }
    }

    /// Log and announce debts that `evidence` settled.
    pub(super) fn note_purpose_settlements(
        &self,
        settled: &[purpose::PurposeDebtView],
        evidence: &str,
    ) {
        if settled.is_empty() {
            return;
        }
        for debt in settled {
            log::info!(
                "purpose {} debt for request {} attempt {} {} settled: {evidence}",
                debt.key,
                debt.request.0,
                debt.request.1,
                self.purpose_debt_machine(debt),
            );
        }
        self.purpose_quiesced.notify_waiters();
    }

    /// Settle every debt bound to `executor_id`, because the runner knows that
    /// machine's work is gone.
    pub(super) fn settle_purpose_debts_for_lost_machine(&self, executor_id: &str, evidence: &str) {
        let protected = self
            .check_claims_for_machine(executor_id)
            .into_iter()
            .map(|(key, _)| key)
            .collect::<HashSet<_>>();
        self.check_machine_lost(executor_id, evidence);
        let settled = if protected.is_empty() {
            self.purposes.lock().unwrap().machine_lost(executor_id)
        } else {
            self.purposes
                .lock()
                .unwrap()
                .settle_on_machine(executor_id, |key, _| !protected.contains(key))
        };
        self.note_purpose_settlements(&settled, evidence);
    }

    /// Read one snapshot as the machine's own account of the debts it holds.
    ///
    /// Three things in it are evidence. An attempt it names among its recent
    /// completions has ended. The first snapshot of a connection newer than
    /// the one a debt was granted on is the machine's statement of what
    /// survived, so a debt it does not list is not running. And the first
    /// snapshot read against a debt reloaded at boot is the same statement for
    /// a connection this process never saw the grant on. A later snapshot from
    /// the grant's own connection that merely omits an attempt is not
    /// evidence: it can cross the grant in flight.
    pub(super) fn settle_purpose_debts_from_account(
        &self,
        executor_id: &str,
        generation: u64,
        first_of_connection: bool,
        snapshot: &FleetSnapshot,
    ) {
        let identify =
            |request_id: &String, attempt_id: &String| (request_id.clone(), attempt_id.clone());
        let listed: HashSet<RequestIdentity> = snapshot
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
            .collect();
        let completed: HashSet<RequestIdentity> = snapshot
            .recent_completions
            .iter()
            .map(|completion| identify(&completion.request_id, &completion.attempt_id))
            .collect();
        let protected_checks = self.protected_check_claims(executor_id);
        let (post_boot, completions, superseded) = {
            let mut registry = self.purposes.lock().unwrap();
            let post_boot = registry.post_boot_account(executor_id, generation, |request| {
                protected_checks.contains(request)
                    || (listed.contains(request) && !completed.contains(request))
            });
            let completions = registry.settle_on_machine(executor_id, |request, _| {
                !protected_checks.contains(request) && completed.contains(request)
            });
            let superseded = if first_of_connection {
                registry.settle_on_machine(executor_id, |request, placement| {
                    !protected_checks.contains(request)
                        && !placement.awaiting_post_boot_account
                        && placement.generation < generation
                        && !listed.contains(request)
                })
            } else {
                Vec::new()
            };
            (post_boot, completions, superseded)
        };
        self.note_purpose_settlements(
            &post_boot.settled,
            &format!(
                "the machine's first report since this runner started (connection generation {generation}) does not list it"
            ),
        );
        self.note_purpose_settlements(&completions, "the machine reported the attempt ended");
        self.note_purpose_settlements(
            &superseded,
            &format!(
                "the first report of the machine's newer connection (generation {generation}) does not list it"
            ),
        );
        self.account_check_claims(
            executor_id,
            generation,
            first_of_connection,
            &listed,
            &completed,
        );
        for request in post_boot.still_running {
            if protected_checks.contains(&request) || self.check_claim(&request).is_some() {
                continue;
            }
            log::warn!(
                "request {} attempt {} is still running on {} although the runner that granted it restarted; cancelling it, and its question stays owed until the machine reports it ended",
                request.0,
                request.1,
                self.executor_name_for_text(executor_id),
            );
            let _ = self.send_to(
                executor_id,
                generation,
                ExecutorMessage::Cancel {
                    request_id: request.0,
                    attempt_id: request.1,
                },
            );
        }
    }

    /// The machine a debt names, in the words an operator reads.
    pub(crate) fn purpose_debt_machine(&self, debt: &purpose::PurposeDebtView) -> String {
        match &debt.placement {
            None => "never placed on a machine".to_string(),
            Some(placement) if placement.awaiting_post_boot_account => format!(
                "on {} (granted on connection generation {} of the runner process before the restart)",
                self.executor_name_for_text(&placement.executor_id),
                placement.generation
            ),
            Some(placement) => format!(
                "on {} (connection generation {})",
                self.executor_name_for_text(&placement.executor_id),
                placement.generation
            ),
        }
    }

    /// What would settle a debt, so the person reading a refusal knows what
    /// they are waiting for and what they could do instead.
    pub(crate) fn purpose_debt_settles_when(&self, debt: &purpose::PurposeDebtView) -> String {
        let Some(placement) = &debt.placement else {
            return "this runner ends its own attempt, which it does as soon as the retirement reaches it".to_string();
        };
        let machine = self.executor_name_for_text(&placement.executor_id);
        let escape = "or an operator authorizes the overlap with authorize_overlap from the app";
        if let Some(claim) = self
            .check_claim(&debt.request)
            .filter(|_| self.adoption_owner(&debt.request).is_some())
        {
            return format!("awaiting adoption on {} since {}; the lane re-arms when the machine reports or is removed; the operator may authorize the overlap with authorize_overlap", claim.machine, purpose::stamp(claim.granted_at_ms as u64));
        }
        if placement.awaiting_post_boot_account {
            return format!(
                "{machine} next reports without it or reports it ended, or {machine} is removed; {escape}"
            );
        }
        let own = match debt.confirmation {
            purpose::Confirmation::Awaiting => "its own terminal outcome arrives, ",
            purpose::Confirmation::Unconfirmed { .. } => "",
        };
        format!(
            "{own}{machine} reports it ended or its next connection does not list it, or {machine} is removed or its process recycled; {escape}"
        )
    }

    /// One outstanding debt, whole, as a refusal names it. It opens with
    /// `request {id} attempt {id},`: the main cadence keys an unchanged hold
    /// on those two ids (`checks_main::hold_debts`).
    pub(crate) fn describe_purpose_debt(
        &self,
        debt: &purpose::PurposeDebtView,
        now_unix_ms: u64,
    ) -> String {
        format!(
            "request {} attempt {}, retired {} ago {}, {}; {}; {}; settles when {}",
            debt.request.0,
            debt.request.1,
            compact_duration(now_unix_ms.saturating_sub(debt.retired_at_unix_ms)),
            debt.phase.describe_retirement(),
            self.purpose_debt_machine(debt),
            debt.cause.describe(),
            debt.confirmation.describe(),
            self.purpose_debt_settles_when(debt),
        )
    }

    pub(super) fn purpose_refusal_diagnostic(
        &self,
        key: &purpose::PurposeKey,
        outstanding: &[purpose::PurposeDebtView],
    ) -> String {
        let now = unix_time_ms();
        let owed = outstanding
            .iter()
            .map(|debt| self.describe_purpose_debt(debt, now))
            .collect::<Vec<_>>()
            .join(" | ");
        format!(
            "purpose {key} {PURPOSE_OWES_DEATH} after {}s; refusing to dispatch a second answer to one question. Outstanding: {owed}",
            RETIREMENT_CONFIRMATION_BUDGET.as_secs()
        )
    }

    /// Resolves once `key` owes nothing.
    ///
    /// A wake, not a settlement, and deliberately without a timeout: it is how
    /// a refused cadence lane learns it may run again, and it waits for as long
    /// as the evidence takes.
    pub(crate) async fn purpose_debt_cleared(&self, key: &purpose::PurposeKey) {
        loop {
            let quiesced = self.purpose_quiesced.notified();
            if self.purposes.lock().unwrap().pending_retirements(key) == 0 {
                return;
            }
            quiesced.await;
        }
    }

    /// When this runner began shutting down, if it has. Work that ends after
    /// this instant was ended by the stop, whatever shape its ending took.
    pub(crate) fn runner_stopping_since(&self) -> Option<u64> {
        self.runner_stop.at()
    }

    /// Whether `key` owes anything right now.
    pub(crate) fn purpose_owes(&self, key: &purpose::PurposeKey) -> bool {
        self.purposes.lock().unwrap().pending_retirements(key) > 0
    }

    #[cfg(test)]
    pub(crate) fn admit_purpose_for_test(
        &self,
        key: &purpose::PurposeKey,
        request: &RequestIdentity,
        armed_at_unix_ms: u64,
    ) {
        self.purposes
            .lock()
            .unwrap()
            .admit(key.clone(), request.clone(), armed_at_unix_ms);
    }

    #[cfg(test)]
    pub(crate) fn reload_purpose_debts_for_test(
        &self,
        rows: Vec<purpose::PurposeDebtRow>,
        boot_unix_ms: u64,
    ) {
        self.purposes.lock().unwrap().reload(rows, boot_unix_ms);
    }

    /// A terminal outcome for `request`, as its leader would record it.
    #[cfg(test)]
    pub(crate) fn finish_purpose_for_test(&self, request: &RequestIdentity) {
        self.settle_leader_purpose(request, None);
    }

    /// Every outstanding purpose debt, for rendering.
    pub(crate) fn purpose_debts(&self) -> Vec<purpose::PurposeDebtView> {
        self.purposes.lock().unwrap().debts()
    }

    /// Every question blocked by a debt, as the executors page and the app
    /// show it; `executor_id` narrows to the debts one machine holds.
    pub fn blocked_questions(
        &self,
        executor_id: Option<&str>,
        now_unix_ms: u64,
    ) -> Vec<BlockedQuestion> {
        self.purpose_debts()
            .into_iter()
            .filter(|debt| {
                executor_id.is_none_or(|executor_id| {
                    debt.placement
                        .as_ref()
                        .is_some_and(|placement| placement.executor_id == executor_id)
                })
            })
            .map(|debt| BlockedQuestion {
                question: debt.key.to_string(),
                request_id: debt.request.0.clone(),
                attempt_id: debt.request.1.clone(),
                phase: debt.phase.describe_retirement().to_string(),
                executor_id: debt
                    .placement
                    .as_ref()
                    .map(|placement| placement.executor_id.clone()),
                machine: debt
                    .placement
                    .as_ref()
                    .map(|placement| self.executor_name_for_text(&placement.executor_id)),
                placement: self.purpose_debt_machine(&debt),
                retired_at_unix_ms: debt.retired_at_unix_ms,
                retired_ago: compact_duration(now_unix_ms.saturating_sub(debt.retired_at_unix_ms)),
                cause: debt.cause.describe(),
                confirmation: debt.confirmation.describe(),
                unconfirmed: matches!(debt.confirmation, purpose::Confirmation::Unconfirmed { .. }),
                settles_when: self.purpose_debt_settles_when(&debt),
            })
            .collect()
    }

    /// Settle the one debt an operator named, because they are authorizing the
    /// overlap it was preventing.
    ///
    /// Reached only from the operator-gated invoke surface, which is the one
    /// place a request carries a person. `operator` names them in the audit
    /// line.
    pub(crate) fn authorize_purpose_overlap(
        &self,
        key: &purpose::PurposeKey,
        request: &RequestIdentity,
        executor_id: Option<&str>,
        operator: &str,
    ) -> Result<purpose::PurposeDebtView, String> {
        if self.adoption_owner(request).is_some() {
            let debt = self
                .purposes
                .lock()
                .unwrap()
                .debts_for(key)
                .into_iter()
                .find(|debt| {
                    &debt.request == request
                        && debt.placement.as_ref().map(|p| p.executor_id.as_str()) == executor_id
                })
                .ok_or_else(|| "the named adoption debt is no longer outstanding".to_string())?;
            self.authorize_check_release(request);
            self.settle_check_claim(
                request,
                "lost",
                format!(
                    "operator {operator} authorized the overlap at {}",
                    purpose::stamp(unix_time_ms())
                ),
                true,
            );
            return Ok(debt);
        }
        let settled = self
            .purposes
            .lock()
            .unwrap()
            .authorize_overlap(key, request, executor_id)?;
        log::warn!(
            "operator {operator} authorized an overlap on purpose {key}: a second answer may run beside request {} attempt {} {}",
            request.0,
            request.1,
            self.purpose_debt_machine(&settled),
        );
        self.purpose_quiesced.notify_waiters();
        Ok(settled)
    }
}
