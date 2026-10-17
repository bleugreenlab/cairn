//! The runner's execution queue: where work waits, and where it binds.
//!
//! Before this existed, placement chose a machine from a cached snapshot and
//! sent it the work. The machine parked that work in a queue of its own until
//! capacity appeared. Every part of that was a bet on stale central state, and
//! the machinery built to compensate was large: parked-placement journaling,
//! hub-side occupancy forecasts computed from what the machine said it was
//! running, and a silence watchdog whose budget had to absorb an unknown queue
//! wait on the far side.
//!
//! Now an entry is *offered* to one machine, and that machine *claims* or
//! declines it immediately from its own admission -- the only place with live
//! facts. Work binds at the claim, and the distinction between an offer and a
//! claim is what makes the central invariant expressible:
//!
//! - An [`EntryState::Offered`] entry has been accepted by nobody. Nothing is
//!   running. When the link that carried the offer ends, the entry returns to
//!   [`EntryState::Queued`] and is immediately re-offerable, so a binding that
//!   outlived its generation cannot exist.
//! - An [`EntryState::Claimed`] entry was accepted and may be running right now.
//!   A generation ending is *not* evidence that it stopped: a machine whose link
//!   drops keeps working and delivers on the next one. Such an entry is handed
//!   to the hold-and-rebind path instead of being re-offered, because
//!   re-offering it would run the same work on two machines at once.
//!
//! # Authority and durability
//!
//! The live queue is this in-memory structure under one mutex, and it is
//! authoritative. Routing has to be instant, an entry's waiter is an in-process
//! future that a durable queue outliving the runner would resurrect with nobody
//! listening, and generations are an in-memory registry that no SQL transaction
//! can arbitrate. What SQL holds is a journal: an append-only account of every
//! transition, written so an operator can explain a binding after the fact.
//! Nothing is ever read back out of it to reconstruct a queue. Entries left
//! non-terminal by a previous runner incarnation are closed as expired at start.
//!
//! # Lock order
//!
//! **connections, then queue, never the reverse.** The generation-end sweep is
//! the only place that holds both, and it holds them in that order because the
//! sweep must happen in the same critical section that removes the connection.

use std::collections::HashMap;
use std::sync::Mutex;

use cairn_common::executor_protocol::{
    BindingEvent, CellAdmissionKind, CellOutcome, CellRequest, DeclineReason, DiskDecline,
    DiskReliefOutlook, OfferSummary,
};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use super::RequestIdentity;

/// A queue entry's own identity, minted fresh per entry.
///
/// Deliberately not the request identity. One request may be enqueued, expire,
/// and be enqueued again, and the journal has to tell those apart; the
/// at-most-one-live-entry rule for a request identity is a separate index.
pub(crate) type EntryId = String;

/// A claim carried across a link gap onto the connection that came back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeldClaim {
    pub(crate) claimed_at_unix_ms: u64,
    /// The generation the claim was bound to when the link went: the process
    /// that was running it, and so the one that lost it if the new connection
    /// does not account for it.
    pub(crate) lost_generation: u64,
}

/// Where a live entry stands.
///
/// Terminal states are not represented here because a terminal entry is not
/// live: it leaves the map and its terminal row goes to the journal. Keeping
/// finished entries in memory is what let a map of in-flight work accumulate
/// rows nobody was waiting for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EntryState {
    /// Waiting for a machine to be chosen. Re-offerable now.
    Queued,
    /// Offered to exactly one machine, which has not answered. Nothing runs.
    Offered {
        executor_id: String,
        generation: u64,
        at_unix_ms: u64,
    },
    /// A machine took responsibility. Work may be running.
    Claimed {
        executor_id: String,
        generation: u64,
        at_unix_ms: u64,
    },
}

impl EntryState {
    pub(crate) fn executor_id(&self) -> Option<&str> {
        match self {
            Self::Queued => None,
            Self::Offered { executor_id, .. } | Self::Claimed { executor_id, .. } => {
                Some(executor_id)
            }
        }
    }

    pub(crate) fn generation(&self) -> Option<u64> {
        match self {
            Self::Queued => None,
            Self::Offered { generation, .. } | Self::Claimed { generation, .. } => {
                Some(*generation)
            }
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Offered { .. } => "offered",
            Self::Claimed { .. } => "claimed",
        }
    }
}

/// How an entry left the queue, for the journal and for the caller that has to
/// answer its waiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TerminalState {
    Completed,
    Cancelled,
    Expired,
}

impl TerminalState {
    fn name(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
        }
    }
}

/// One entry: the work, what it needs, where it stands, and who is waiting.
///
/// The placement constraints and the scoring inputs are not copied out of the
/// request into fields of their own. The request already states the platform,
/// the toolchains, the project, the repository, the selector, the pin, the
/// affinity key, the priority, and the mobility, and `choose_executor` reads
/// them from there. A second copy here would be a second home for the same
/// facts, free to drift from the one dispatch actually uses.
pub(crate) struct QueueEntry {
    pub(crate) id: EntryId,
    pub(crate) request: CellRequest,
    pub(crate) admission_kind: CellAdmissionKind,
    /// Whether someone is blocked on this, which admission weighs differently.
    pub(crate) interactive: bool,
    pub(crate) requesting_job_id: Option<String>,
    /// The requester's own instant past which this result is not wanted. Taken
    /// from the request and never refreshed, so a re-presentation cannot extend
    /// what the caller said it would wait.
    pub(crate) horizon_unix_ms: u64,
    pub(crate) state: EntryState,
    /// The latest answer each machine that declined gave, keyed by executor id.
    ///
    /// A decline stops this entry being offered to that machine until the
    /// machine posts new state, because new posted state is the only thing that
    /// makes reconsidering it meaningful. An ordering, deliberately, rather than
    /// a timer: a retry clock would re-ask a machine whose situation had not
    /// changed. Posting state supersedes the decline rather than erasing it, so
    /// a request that reaches its horizon can still say who refused it and why.
    pub(crate) declined: HashMap<String, EntryDecline>,
    /// Offer, claim, decline, in order, for the placement record.
    pub(crate) chain: Vec<BindingEvent>,
    /// A re-offer to the machine whose decline is this entry's latest binding
    /// step, not yet journaled. It is journaled when it resolves into anything
    /// other than the same decline again; an identical re-decline folds into
    /// the decline already recorded. Without this, a request parked behind a
    /// pressured machine that posts state every few seconds would append an
    /// offer and a decline to the chain and to the journal on every post for as
    /// long as the pressure lasts.
    reoffer: Option<PendingReoffer>,
    /// How many journal rows this entry has emitted, which is the next row's
    /// `sequence`.
    ///
    /// Not `chain.len()`: settling or taking an entry appends no binding event,
    /// so a terminal row numbered by the chain reuses its last transition's
    /// sequence, and `UNIQUE (entry_id, sequence)` under `INSERT OR IGNORE`
    /// discards it without a word. That is how the journal came to record no
    /// terminal state at all.
    journaled_rows: u64,
    waiter: Option<oneshot::Sender<CellOutcome>>,
}

impl QueueEntry {
    pub(crate) fn identity(&self) -> RequestIdentity {
        (
            self.request.request_id.clone(),
            self.request.attempt_id.clone(),
        )
    }

    /// What this entry tells a machine it is being offered.
    ///
    /// Built from `attempt`, the request as placement resolved it for this one
    /// machine, never from the entry's submitted request. The machine charges
    /// exactly what the offer names and later runs, and releases, what the
    /// grant names; the two must be the same request or the claim admits one
    /// reservation (the submitted default, or an unclamped whole-machine
    /// declaration) while the work consumes another.
    pub(crate) fn summary(
        &self,
        attempt: &CellRequest,
        predicted_cost_ms: Option<u64>,
    ) -> OfferSummary {
        debug_assert_eq!(
            (&attempt.request_id, &attempt.attempt_id),
            (&self.request.request_id, &self.request.attempt_id),
            "an offer describes an attempt of this entry"
        );
        OfferSummary {
            request_id: attempt.request_id.clone(),
            attempt_id: attempt.attempt_id.clone(),
            project_id: attempt.project_id.clone(),
            repository: attempt.repository.clone(),
            base_commit: attempt.base_commit.clone(),
            reservation: attempt.resource_reservation.clone(),
            admission_kind: self.admission_kind,
            interactive: self.interactive,
            priority: attempt.priority,
            waiting_since_unix_ms: attempt.waiting_since_unix_ms,
            wait_horizon_unix_ms: attempt.wait_horizon_unix_ms,
            predicted_cost_ms,
        }
    }

    /// Whether this machine may be offered this entry right now.
    ///
    /// A machine that declined is out until it posts new state. `generation` is
    /// the machine's current one: a decline recorded against an older link says
    /// nothing about this one, because the process behind it may have been
    /// replaced entirely.
    pub(crate) fn may_offer_to(&self, executor_id: &str, generation: u64) -> bool {
        !self
            .declined
            .get(executor_id)
            .is_some_and(|decline| decline.stands_on(generation))
    }

    fn answer(&mut self, outcome: CellOutcome) {
        if let Some(waiter) = self.waiter.take() {
            let _ = waiter.send(outcome);
        }
    }

    /// Journal a deferred re-offer ahead of whatever it resolved into.
    fn flush_reoffer(&mut self, rows: &mut Vec<QueueJournalRow>) {
        if let Some(reoffer) = self.reoffer.take() {
            rows.push(journal_row(
                self,
                "offered",
                Some((reoffer.executor_id.as_str(), reoffer.generation)),
                None,
                reoffer.at_unix_ms,
            ));
        }
    }
}

/// What one machine said when it declined one entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EntryDecline {
    /// The connection generation the machine answered on. A decline from an
    /// older link says nothing about the process behind the current one.
    pub(crate) generation: u64,
    pub(crate) reason: DeclineReason,
    /// The machine's own sentence, verbatim.
    pub(crate) detail: Option<String>,
    /// For a disk refusal, the floor it hit and whether reclamation can still
    /// relieve it.
    pub(crate) disk: Option<DiskDecline>,
    pub(crate) at_unix_ms: u64,
    /// The machine has posted state since. A superseded decline no longer keeps
    /// the machine from being offered this entry, but it is still the last
    /// thing that machine said about it.
    pub(crate) superseded: bool,
}

impl EntryDecline {
    /// Whether this decline still keeps the machine, on `generation`, from
    /// being offered the entry.
    pub(crate) fn stands_on(&self, generation: u64) -> bool {
        !self.superseded && self.generation == generation
    }
}

struct PendingReoffer {
    executor_id: String,
    generation: u64,
    at_unix_ms: u64,
}

/// What recording a decline did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeclineRecord {
    /// The entry was not offered to this machine on this generation; nothing
    /// changed.
    Stale,
    /// Recorded. `repeat` says this machine already gave this entry the same
    /// reason before, which is what lets the caller log the first answer of a
    /// kind loudly and the rest quietly.
    Recorded { repeat: bool },
}

impl DeclineRecord {
    #[cfg(test)]
    pub(crate) fn applied(self) -> bool {
        matches!(self, Self::Recorded { .. })
    }
}

/// One recorded transition, appended to `execution_queue`.
///
/// The batch and the request are deliberately absent. They can be large, and
/// the journal exists to explain a binding rather than to reconstruct one --
/// nothing is ever resurrected from it.
#[derive(Debug, Clone)]
pub(crate) struct QueueJournalRow {
    pub(crate) entry_id: EntryId,
    pub(crate) sequence: u64,
    pub(crate) request_id: String,
    pub(crate) attempt_id: String,
    pub(crate) project_id: String,
    pub(crate) state: String,
    pub(crate) executor_id: Option<String>,
    pub(crate) generation: Option<u64>,
    pub(crate) decline_reason: Option<String>,
    pub(crate) detail: Option<String>,
    pub(crate) constraints_json: String,
    pub(crate) scoring_json: String,
    pub(crate) horizon_unix_ms: u64,
    pub(crate) created_at: u64,
}

/// A batch of transitions that must reach the journal as one SQL transaction.
///
/// A generation ending emits its own row and every requeue it caused together,
/// which is what makes "in the same transaction that records the generation
/// end" true rather than merely usual.
pub(crate) type QueueJournalBatch = Vec<QueueJournalRow>;
pub(crate) type QueueJournalSender = mpsc::UnboundedSender<QueueJournalBatch>;

/// What a claim was answered with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClaimVerdict {
    /// The claim won. The caller sends `Granted` with the work.
    Granted,
    /// The entry had moved on. The caller answers `Lost`; nothing was started.
    Lost,
}

/// What a generation ending did to the entries bound to it.
///
/// The two halves are deliberately different, and the difference is the whole
/// safety argument: offers are released because nothing was running behind
/// them, and claims are not because something may be.
#[derive(Debug, Default)]
pub(crate) struct GenerationEndSweep {
    /// Offers released back to `Queued`, immediately re-offerable.
    pub(crate) released_offers: Vec<EntryId>,
    /// Claims left bound, for the reattach path to account for.
    pub(crate) held_claims: Vec<RequestIdentity>,
    /// Claims that could not be held because the runner ended this link on
    /// purpose, and that may move: returned to `Queued`.
    pub(crate) requeued_claims: Vec<EntryId>,
    /// Claims that could not be held and could not move. Already answered.
    pub(crate) failed_claims: Vec<RequestIdentity>,
}

/// Why a generation ended, which decides what happens to claimed work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GenerationEndCause {
    /// The link dropped for peer or I/O reasons. The machine is still running
    /// and reconnects on its own, so claimed work is held for it to deliver.
    LinkGap,
    /// The runner ended this link on purpose, or its supervisor declared the
    /// process gone. Claimed work is not coming back on this machine.
    MachineGone,
    /// The runner itself is shutting down. Claimed work is answered as
    /// withdrawn and never requeued: there is no machine to move it to that
    /// this runner will still be here to wait on.
    RunnerStopping { stopped_at_unix_ms: u64 },
}

pub(crate) struct ExecutionQueue {
    entries: Mutex<QueueState>,
    /// Where transitions go to be made durable. Absent means this queue
    /// journals nothing, which is what `Fleet::default()` gives every test:
    /// the queue is always authoritative, and the journal is an optional sink
    /// on top of it rather than a dependency underneath it.
    journal: Mutex<Option<QueueJournalSender>>,
    /// The last journal write that failed, kept rather than thrown so a
    /// synchronous mutation path never has to decide what to do about SQL.
    pub(crate) journal_error: Mutex<Option<String>>,
}

#[derive(Default)]
struct QueueState {
    entries: HashMap<EntryId, QueueEntry>,
    /// At most one live entry per request identity, which is what preserves the
    /// duplicate-submission refusal the pending map used to give.
    by_identity: HashMap<RequestIdentity, EntryId>,
    next_sequence: u64,
}

impl Default for ExecutionQueue {
    fn default() -> Self {
        Self {
            entries: Mutex::new(QueueState::default()),
            journal: Mutex::new(None),
            journal_error: Mutex::new(None),
        }
    }
}

/// What a caller hands the queue to create an entry.
///
/// The request and nothing more. The queue routes from what the request says;
/// the process batch and the runtime configuration are what a *grant* carries,
/// and the dispatch that sends the grant owns them for as long as the entry
/// lives. A second copy of a batch held under this mutex would cost real memory
/// for a reader that does not exist.
pub(crate) struct NewEntry {
    pub(crate) request: CellRequest,
    pub(crate) admission_kind: CellAdmissionKind,
    pub(crate) interactive: bool,
}

impl ExecutionQueue {
    /// Install the durable sink. Called once, when the runner has a database.
    pub(crate) fn attach_journal(&self, sender: QueueJournalSender) {
        *self.journal.lock().unwrap() = Some(sender);
    }

    pub(crate) fn journal_error(&self) -> Option<String> {
        self.journal_error.lock().unwrap().clone()
    }

    /// Admit a new entry, or refuse it because its identity is already live.
    pub(crate) fn enqueue(
        &self,
        new: NewEntry,
        now_unix_ms: u64,
    ) -> Result<(EntryId, oneshot::Receiver<CellOutcome>), ()> {
        let identity = (
            new.request.request_id.clone(),
            new.request.attempt_id.clone(),
        );
        let (waiter, receiver) = oneshot::channel();
        let mut state = self.entries.lock().unwrap();
        if state.by_identity.contains_key(&identity) {
            return Err(());
        }
        let sequence = state.next_sequence;
        state.next_sequence += 1;
        let id = format!("{}-{}-{sequence}", identity.0, identity.1);
        let mut entry = QueueEntry {
            id: id.clone(),
            horizon_unix_ms: new.request.wait_horizon_unix_ms,
            requesting_job_id: new.request.requesting_job_id.clone(),
            request: new.request,
            admission_kind: new.admission_kind,
            interactive: new.interactive,
            state: EntryState::Queued,
            declined: HashMap::new(),
            chain: Vec::new(),
            reoffer: None,
            journaled_rows: 0,
            waiter: Some(waiter),
        };
        let row = journal_row(
            &mut entry,
            EntryState::Queued.name(),
            None,
            None,
            now_unix_ms,
        );
        state.by_identity.insert(identity, id.clone());
        state.entries.insert(id.clone(), entry);
        drop(state);
        self.journal(vec![row]);
        Ok((id, receiver))
    }

    /// Whether this identity is live in the queue at all.
    ///
    /// Nothing in the runner asks this: production paths address an entry by
    /// its id or act on its binding. It exists so a test can state "this work
    /// is still outstanding" without reaching into the queue's internals.
    #[cfg(test)]
    pub(crate) fn holds(&self, identity: &RequestIdentity) -> bool {
        self.entries
            .lock()
            .unwrap()
            .by_identity
            .contains_key(identity)
    }

    pub(crate) fn with_entry<T>(&self, id: &str, read: impl FnOnce(&QueueEntry) -> T) -> Option<T> {
        let state = self.entries.lock().unwrap();
        state.entries.get(id).map(read)
    }

    pub(crate) fn with_entry_by_identity<T>(
        &self,
        identity: &RequestIdentity,
        read: impl FnOnce(&QueueEntry) -> T,
    ) -> Option<T> {
        let state = self.entries.lock().unwrap();
        let id = state.by_identity.get(identity)?;
        state.entries.get(id).map(read)
    }

    pub(crate) fn state_of(&self, id: &str) -> Option<EntryState> {
        self.with_entry(id, |entry| entry.state.clone())
    }

    /// Record that this entry has been offered to this machine.
    ///
    /// Refuses an entry that is not `Queued`, so two concurrent placement passes
    /// cannot offer the same entry to two machines.
    pub(crate) fn offer(
        &self,
        id: &str,
        executor_id: &str,
        executor_name: &str,
        generation: u64,
        now_unix_ms: u64,
    ) -> bool {
        let mut state = self.entries.lock().unwrap();
        let Some(entry) = state.entries.get_mut(id) else {
            return false;
        };
        if entry.state != EntryState::Queued {
            return false;
        }
        // The queue enforces its own rule rather than trusting the caller's
        // candidate filter to have applied it. A machine's own refusal is
        // better evidence than anything ranking can compute about it, and an
        // offer that ignores one would be answered the same way twice.
        if !entry.may_offer_to(executor_id, generation) {
            return false;
        }
        let reasks_last_decliner = matches!(
            entry.chain.last(),
            Some(BindingEvent::Declined { executor_id: decliner, .. }) if decliner == executor_id
        );
        entry.state = EntryState::Offered {
            executor_id: executor_id.to_string(),
            generation,
            at_unix_ms: now_unix_ms,
        };
        entry.chain.push(BindingEvent::Offered {
            executor_id: executor_id.to_string(),
            executor_name: executor_name.to_string(),
            generation,
            at_unix_ms: now_unix_ms,
        });
        if reasks_last_decliner {
            entry.reoffer = Some(PendingReoffer {
                executor_id: executor_id.to_string(),
                generation,
                at_unix_ms: now_unix_ms,
            });
            return true;
        }
        let row = journal_row(
            entry,
            "offered",
            Some((executor_id, generation)),
            None,
            now_unix_ms,
        );
        drop(state);
        self.journal(vec![row]);
        true
    }

    /// A machine claims an entry it was offered.
    ///
    /// The generation must be the one the offer went out on. Anything else is a
    /// claim from a link that has already ended, and the entry has moved on.
    pub(crate) fn claim(
        &self,
        id: &str,
        executor_id: &str,
        generation: u64,
        now_unix_ms: u64,
    ) -> ClaimVerdict {
        let mut state = self.entries.lock().unwrap();
        let Some(entry) = state.entries.get_mut(id) else {
            return ClaimVerdict::Lost;
        };
        let offered_here = matches!(
            &entry.state,
            EntryState::Offered { executor_id: offered, generation: offered_generation, .. }
                if offered == executor_id && *offered_generation == generation
        );
        if !offered_here {
            return ClaimVerdict::Lost;
        }
        entry.state = EntryState::Claimed {
            executor_id: executor_id.to_string(),
            generation,
            at_unix_ms: now_unix_ms,
        };
        entry.chain.push(BindingEvent::Claimed {
            executor_id: executor_id.to_string(),
            generation,
            at_unix_ms: now_unix_ms,
        });
        let mut rows = Vec::new();
        entry.flush_reoffer(&mut rows);
        rows.push(journal_row(
            entry,
            "claimed",
            Some((executor_id, generation)),
            None,
            now_unix_ms,
        ));
        drop(state);
        self.journal(rows);
        ClaimVerdict::Granted
    }

    /// A machine refuses an entry it was offered, and says why.
    ///
    /// The entry returns to `Queued` and the decliner is recorded against this
    /// generation, so the next placement pass routes elsewhere, or waits for
    /// this machine to post new state, rather than asking it the same question.
    ///
    /// A machine that is re-asked after posting state and gives the same reason
    /// again is one continuing refusal, not a new step: its decline is
    /// refreshed in place (newest detail, newest time) and nothing is appended
    /// to the chain or the journal.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn decline(
        &self,
        id: &str,
        executor_id: &str,
        generation: u64,
        reason: DeclineReason,
        detail: Option<String>,
        disk: Option<DiskDecline>,
        now_unix_ms: u64,
    ) -> DeclineRecord {
        let mut state = self.entries.lock().unwrap();
        let Some(entry) = state.entries.get_mut(id) else {
            return DeclineRecord::Stale;
        };
        if entry.state.executor_id() != Some(executor_id)
            || entry.state.generation() != Some(generation)
        {
            return DeclineRecord::Stale;
        }
        let repeat = entry.chain.iter().any(|event| {
            matches!(
                event,
                BindingEvent::Declined { executor_id: decliner, reason: earlier, .. }
                    if decliner == executor_id && *earlier == reason
            )
        });
        entry.state = EntryState::Queued;
        entry.declined.insert(
            executor_id.to_string(),
            EntryDecline {
                generation,
                reason,
                detail: detail.clone(),
                disk,
                at_unix_ms: now_unix_ms,
                superseded: false,
            },
        );
        let tail = entry.chain.len().saturating_sub(2);
        let folds = entry.reoffer.is_some()
            && matches!(
                &entry.chain[tail..],
                [BindingEvent::Declined { reason: previous, .. }, BindingEvent::Offered { .. }]
                    if *previous == reason
            );
        if folds {
            entry.chain.pop();
            entry.reoffer = None;
            if let Some(BindingEvent::Declined {
                detail: previous_detail,
                disk: previous_disk,
                at_unix_ms: previous_at,
                ..
            }) = entry.chain.last_mut()
            {
                *previous_detail = detail;
                // The outlook is the part of a continuing disk refusal most
                // likely to change: a sweep finishes, a pass yields.
                *previous_disk = disk;
                *previous_at = now_unix_ms;
            }
            return DeclineRecord::Recorded { repeat: true };
        }
        entry.chain.push(BindingEvent::Declined {
            executor_id: executor_id.to_string(),
            reason,
            detail: detail.clone(),
            disk,
            at_unix_ms: now_unix_ms,
        });
        // The journal's detail carries the typed disk outlook in words, so a
        // hold read from the journal says whether waiting on this machine can
        // help, not only that its disk refused.
        let detail = match (detail, disk.as_ref()) {
            (detail, Some(disk)) => Some(format!(
                "{}{}",
                detail
                    .map(|detail| format!("{detail}; "))
                    .unwrap_or_default(),
                disk_decline_summary(disk)
            )),
            (detail, None) => detail,
        };
        let mut rows = Vec::new();
        entry.flush_reoffer(&mut rows);
        rows.push(journal_row(
            entry,
            "queued",
            Some((executor_id, generation)),
            Some((reason, detail)),
            now_unix_ms,
        ));
        drop(state);
        self.journal(rows);
        DeclineRecord::Recorded { repeat }
    }

    /// Supersede this machine's declines, because it has posted new state.
    ///
    /// New posted state is the only thing that makes reconsidering a machine
    /// meaningful, so it is the only thing that clears a decline. The decline's
    /// facts stay on the entry as that machine's last answer. Returns whether
    /// anything became offerable again, which is worth a placement wake.
    pub(crate) fn clear_declines_for(&self, executor_id: &str) -> bool {
        let mut state = self.entries.lock().unwrap();
        let mut cleared = false;
        for entry in state.entries.values_mut() {
            if let Some(decline) = entry
                .declined
                .get_mut(executor_id)
                .filter(|decline| !decline.superseded)
            {
                decline.superseded = true;
                cleared = true;
            }
        }
        cleared
    }
}

impl ExecutionQueue {
    /// Settle an entry and answer its waiter.
    ///
    /// `bound_to` is checked when given, which is what keeps a result from a
    /// link that has already ended out of a waiter that has moved on.
    pub(crate) fn settle(
        &self,
        identity: &RequestIdentity,
        bound_to: Option<(&str, u64)>,
        terminal: TerminalState,
        outcome: CellOutcome,
        now_unix_ms: u64,
    ) -> bool {
        let mut state = self.entries.lock().unwrap();
        let Some(id) = state.by_identity.get(identity).cloned() else {
            return false;
        };
        if let Some((executor_id, generation)) = bound_to {
            let entry = &state.entries[&id];
            if entry.state.executor_id() != Some(executor_id)
                || entry.state.generation() != Some(generation)
            {
                return false;
            }
        }
        state.by_identity.remove(identity);
        let Some(mut entry) = state.entries.remove(&id) else {
            return false;
        };
        let bound = entry.state.executor_id().map(|executor| {
            (
                executor.to_string(),
                entry.state.generation().unwrap_or_default(),
            )
        });
        let mut rows = Vec::new();
        entry.flush_reoffer(&mut rows);
        rows.push(journal_row(
            &mut entry,
            terminal.name(),
            bound
                .as_ref()
                .map(|(executor, generation)| (executor.as_str(), *generation)),
            None,
            now_unix_ms,
        ));
        drop(state);
        entry.answer(outcome);
        self.journal(rows);
        true
    }

    /// Take an entry out without answering it, for a caller that owns the
    /// outcome itself.
    pub(crate) fn take(
        &self,
        identity: &RequestIdentity,
        terminal: TerminalState,
        now_unix_ms: u64,
    ) -> Option<QueueEntry> {
        self.take_with_detail(identity, terminal, None, now_unix_ms)
    }

    /// Take an entry out as cancelled, recording why on its terminal row.
    ///
    /// A withdrawal with no stated cause is indistinguishable in the journal
    /// from every other cancellation, which is how a caller that went away
    /// came to be mistaken for work that was never ended.
    pub(crate) fn withdraw(
        &self,
        identity: &RequestIdentity,
        cause: String,
        now_unix_ms: u64,
    ) -> Option<QueueEntry> {
        self.take_with_detail(identity, TerminalState::Cancelled, Some(cause), now_unix_ms)
    }

    fn take_with_detail(
        &self,
        identity: &RequestIdentity,
        terminal: TerminalState,
        detail: Option<String>,
        now_unix_ms: u64,
    ) -> Option<QueueEntry> {
        let mut state = self.entries.lock().unwrap();
        let id = state.by_identity.remove(identity)?;
        let mut entry = state.entries.remove(&id)?;
        let bound = entry.state.executor_id().map(|executor| {
            (
                executor.to_string(),
                entry.state.generation().unwrap_or_default(),
            )
        });
        let mut rows = Vec::new();
        entry.flush_reoffer(&mut rows);
        let mut row = journal_row(
            &mut entry,
            terminal.name(),
            bound
                .as_ref()
                .map(|(executor, generation)| (executor.as_str(), *generation)),
            None,
            now_unix_ms,
        );
        row.detail = detail;
        rows.push(row);
        drop(state);
        self.journal(rows);
        Some(entry)
    }

    /// End a generation, in the same critical section that removes its
    /// connection.
    ///
    /// Offers go back to `Queued` unconditionally: nothing was running behind
    /// them, so nothing can be run twice by re-offering them, and an offer that
    /// survived its link is exactly the stuck placement this design exists to
    /// make impossible.
    ///
    /// Claims do not, and that asymmetry is the safety argument. A claimed entry
    /// may be executing on a machine whose link merely dropped; the machine
    /// keeps working and delivers on its next connection. Re-offering it would
    /// dispatch the same work a second time. So a claim is held for the reattach
    /// to account for, and only a runner-initiated close -- where the process is
    /// known to be going away -- moves it, and then only if the work may move at
    /// all.
    ///
    /// `diagnostic` is what a claim that can neither be held nor moved is
    /// answered with. It comes from the caller because only the caller knows why
    /// the link ended, and that sentence is what tells an operator whether to
    /// wait: a connection the runner closed on purpose reads differently from a
    /// machine that was declared gone.
    pub(crate) fn end_generation(
        &self,
        executor_id: &str,
        generation: u64,
        cause: GenerationEndCause,
        diagnostic: &str,
        now_unix_ms: u64,
    ) -> GenerationEndSweep {
        let mut sweep = GenerationEndSweep::default();
        let mut rows = Vec::new();
        let mut answered: Vec<RequestIdentity> = Vec::new();
        let mut state = self.entries.lock().unwrap();
        let ids: Vec<EntryId> = state
            .entries
            .values()
            .filter(|entry| {
                entry.state.executor_id() == Some(executor_id)
                    && entry.state.generation() == Some(generation)
            })
            .map(|entry| entry.id.clone())
            .collect();
        for id in ids {
            let entry = state.entries.get_mut(&id).expect("entry just listed");
            entry.flush_reoffer(&mut rows);
            entry.chain.push(BindingEvent::GenerationEnded {
                executor_id: executor_id.to_string(),
                generation,
                at_unix_ms: now_unix_ms,
            });
            let claimed = matches!(entry.state, EntryState::Claimed { .. });
            if !claimed {
                entry.state = EntryState::Queued;
                rows.push(journal_row(
                    entry,
                    "queued",
                    Some((executor_id, generation)),
                    None,
                    now_unix_ms,
                ));
                sweep.released_offers.push(id);
                continue;
            }
            match cause {
                GenerationEndCause::LinkGap => sweep.held_claims.push(entry.identity()),
                GenerationEndCause::MachineGone if entry.request.placement_mobility.may_spill() => {
                    entry.state = EntryState::Queued;
                    entry.declined.insert(
                        executor_id.to_string(),
                        EntryDecline {
                            generation,
                            reason: DeclineReason::Unknown,
                            detail: Some(format!(
                                "the link to {executor_id} ended while this work was claimed there: {diagnostic}"
                            )),
                            disk: None,
                            at_unix_ms: now_unix_ms,
                            superseded: false,
                        },
                    );
                    rows.push(journal_row(
                        entry,
                        "queued",
                        Some((executor_id, generation)),
                        None,
                        now_unix_ms,
                    ));
                    sweep.requeued_claims.push(id);
                }
                GenerationEndCause::MachineGone | GenerationEndCause::RunnerStopping { .. } => {
                    let terminal = match cause {
                        GenerationEndCause::RunnerStopping { .. } => TerminalState::Cancelled,
                        _ => TerminalState::Completed,
                    };
                    let mut row = journal_row(
                        entry,
                        terminal.name(),
                        Some((executor_id, generation)),
                        None,
                        now_unix_ms,
                    );
                    row.detail = Some(diagnostic.to_string());
                    rows.push(row);
                    answered.push(entry.identity());
                }
            }
        }
        for identity in answered {
            if let Some(id) = state.by_identity.remove(&identity) {
                if let Some(mut entry) = state.entries.remove(&id) {
                    entry.answer(match cause {
                        GenerationEndCause::RunnerStopping { stopped_at_unix_ms } => {
                            super::runner_stopping(stopped_at_unix_ms, diagnostic.to_string())
                        }
                        _ => super::executor_unavailable(diagnostic.to_string()),
                    });
                }
            }
            sweep.failed_claims.push(identity);
        }
        drop(state);
        self.journal(rows);
        sweep
    }

    /// Re-stamp held work onto the connection that came back.
    ///
    /// What identifies an execution is what it is on the machine, not the link
    /// that carried the order, so a result computed during the gap arrives on
    /// the new generation and has to be accepted there.
    pub(crate) fn rebind(
        &self,
        executor_id: &str,
        generation: u64,
    ) -> HashMap<RequestIdentity, HeldClaim> {
        let mut state = self.entries.lock().unwrap();
        let mut carried = HashMap::new();
        for entry in state.entries.values_mut() {
            let EntryState::Claimed {
                executor_id: bound,
                generation: bound_generation,
                at_unix_ms,
            } = entry.state.clone()
            else {
                continue;
            };
            if bound != executor_id || bound_generation == generation {
                continue;
            }
            entry.state = EntryState::Claimed {
                executor_id: bound,
                generation,
                at_unix_ms,
            };
            carried.insert(
                entry.identity(),
                HeldClaim {
                    claimed_at_unix_ms: at_unix_ms,
                    lost_generation: bound_generation,
                },
            );
        }
        carried
    }

    /// Every identity this machine is bound to, offered or claimed.
    pub(crate) fn identities_bound_to(&self, executor_id: &str) -> Vec<RequestIdentity> {
        let state = self.entries.lock().unwrap();
        state
            .entries
            .values()
            .filter(|entry| entry.state.executor_id() == Some(executor_id))
            .map(QueueEntry::identity)
            .collect()
    }

    /// Answer everything bound to this machine, because none of it is coming.
    pub(crate) fn fail_claims_for(&self, executor_id: &str, diagnostic: &str, now_unix_ms: u64) {
        for identity in self.identities_bound_to(executor_id) {
            self.settle(
                &identity,
                None,
                TerminalState::Completed,
                super::executor_unavailable(diagnostic.to_string()),
                now_unix_ms,
            );
        }
    }

    fn journal(&self, rows: QueueJournalBatch) {
        if rows.is_empty() {
            return;
        }
        let sender = self.journal.lock().unwrap().clone();
        let Some(sender) = sender else { return };
        if let Err(error) = sender.send(rows) {
            *self.journal_error.lock().unwrap() = Some(format!(
                "execution queue journal is not being drained: {error}"
            ));
        }
    }
}

fn journal_row(
    entry: &mut QueueEntry,
    state: &str,
    bound_to: Option<(&str, u64)>,
    decline: Option<(DeclineReason, Option<String>)>,
    now_unix_ms: u64,
) -> QueueJournalRow {
    let sequence = entry.journaled_rows;
    entry.journaled_rows += 1;
    QueueJournalRow {
        entry_id: entry.id.clone(),
        sequence,
        request_id: entry.request.request_id.clone(),
        attempt_id: entry.request.attempt_id.clone(),
        project_id: entry.request.project_id.clone(),
        state: state.to_string(),
        executor_id: bound_to.map(|(executor, _)| executor.to_string()),
        generation: bound_to.map(|(_, generation)| generation),
        decline_reason: decline
            .as_ref()
            .map(|(reason, _)| decline_reason_key(*reason)),
        detail: decline.and_then(|(_, detail)| detail),
        constraints_json: serde_json::json!({
            "selector": entry.request.executor,
            "projectId": entry.request.project_id,
            "repository": entry.request.repository.identity(),
            "pinnedExecutorId": entry.request.pinned_executor_id,
            "verdictPlatforms": entry.request.verdict_platforms,
            "mobility": entry.request.placement_mobility,
        })
        .to_string(),
        scoring_json: serde_json::json!({
            "affinityKey": entry.request.affinity_key,
            "priority": entry.request.priority,
            "reservation": entry.request.resource_reservation,
        })
        .to_string(),
        horizon_unix_ms: entry.horizon_unix_ms,
        created_at: now_unix_ms,
    }
}

/// A typed disk refusal in one clause: the floor it hit and whether
/// reclamation can still relieve it.
pub(crate) fn disk_decline_summary(disk: &DiskDecline) -> String {
    let outlook = match disk.outlook {
        DiskReliefOutlook::InFlight => "a storage sweep is running and may relieve it".to_string(),
        DiskReliefOutlook::Relieved => {
            "the last pressure pass relieved the volume; this is new growth".to_string()
        }
        DiskReliefOutlook::Unrelieved { held_bytes } => format!(
            "the last pressure pass ran every permitted rung and fell short ({held_bytes} bytes held by policy); waiting here will not help"
        ),
        DiskReliefOutlook::CleanupFailed => "the storage sweep failed and admission is latched".to_string(),
        DiskReliefOutlook::NotAttempted => "no pressure pass has run yet".to_string(),
        DiskReliefOutlook::Deferred => {
            "the last pressure pass yielded to arriving work; the next one continues it".to_string()
        }
    };
    format!(
        "disk {} bytes free against a {}-byte floor: {outlook}",
        disk.free_bytes, disk.floor_bytes
    )
}

/// The journal's spelling of a decline reason, which is also the set the table's
/// CHECK constraint accepts.
fn decline_reason_key(reason: DeclineReason) -> String {
    match reason {
        DeclineReason::Capacity => "capacity",
        DeclineReason::Pressure => "pressure",
        DeclineReason::Disk => "disk",
        DeclineReason::Constraint => "constraint",
        DeclineReason::Unknown => "unknown",
        DeclineReason::ProjectProvisioning => "project_provisioning",
    }
    .to_string()
}

impl ExecutionQueue {
    /// Which machine and generation an identity is bound to, if any.
    pub(crate) fn binding_of(&self, identity: &RequestIdentity) -> Option<(String, u64)> {
        self.with_entry_by_identity(identity, |entry| {
            Some((
                entry.state.executor_id()?.to_string(),
                entry.state.generation()?,
            ))
        })
        .flatten()
    }

    /// Every bound entry a job owns, for cancelling that job's work.
    pub(crate) fn bindings_for_job(&self, job_id: &str) -> Vec<(RequestIdentity, String, u64)> {
        let state = self.entries.lock().unwrap();
        state
            .entries
            .values()
            .filter(|entry| entry.requesting_job_id.as_deref() == Some(job_id))
            .filter_map(|entry| {
                Some((
                    entry.identity(),
                    entry.state.executor_id()?.to_string(),
                    entry.state.generation()?,
                ))
            })
            .collect()
    }

    /// Which machine has *claimed* this identity, if any.
    ///
    /// Distinct from [`Self::binding_of`], which also answers for a mere offer.
    /// Only a claim means something may be running, and only something that may
    /// be running is worth sending a `Cancel` about.
    pub(crate) fn claimed_binding_of(&self, identity: &RequestIdentity) -> Option<(String, u64)> {
        self.with_entry_by_identity(identity, |entry| match &entry.state {
            EntryState::Claimed {
                executor_id,
                generation,
                ..
            } => Some((executor_id.clone(), *generation)),
            _ => None,
        })
        .flatten()
    }

    /// Every identity bound to exactly this link, offered or claimed.
    pub(crate) fn identities_on_generation(
        &self,
        executor_id: &str,
        generation: u64,
    ) -> Vec<RequestIdentity> {
        let state = self.entries.lock().unwrap();
        state
            .entries
            .values()
            .filter(|entry| {
                entry.state.executor_id() == Some(executor_id)
                    && entry.state.generation() == Some(generation)
            })
            .map(QueueEntry::identity)
            .collect()
    }

    /// The request ids bound to this link that somebody is still waiting for.
    pub(crate) fn waiting_request_ids(&self, executor_id: &str, generation: u64) -> Vec<String> {
        let state = self.entries.lock().unwrap();
        state
            .entries
            .values()
            .filter(|entry| {
                entry.state.executor_id() == Some(executor_id)
                    && entry.state.generation() == Some(generation)
                    && entry
                        .waiter
                        .as_ref()
                        .is_some_and(|waiter| !waiter.is_closed())
            })
            .map(|entry| entry.request.request_id.clone())
            .collect()
    }

    /// The live attempt of a request id, and where it is bound.
    ///
    /// A request id addresses at most one live attempt, which is what lets a
    /// caller that knows only the request name cancel the right work.
    pub(crate) fn attempt_binding_of(&self, request_id: &str) -> Option<(String, String, u64)> {
        let state = self.entries.lock().unwrap();
        state.entries.values().find_map(|entry| {
            (entry.request.request_id == request_id).then(|| {
                Some((
                    entry.request.attempt_id.clone(),
                    entry.state.executor_id()?.to_string(),
                    entry.state.generation()?,
                ))
            })?
        })
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().unwrap().entries.len()
    }
}

/// Drain queue transitions into the durable journal.
///
/// The channel IS the append and this task IS the consumer, so ordering is the
/// channel's and no mutation path ever awaits SQL: `Fleet`'s queue transitions
/// are synchronous and hold a `std::sync::Mutex`, and a journal write in front
/// of one would put a database round trip inside the dispatch path.
///
/// Each batch becomes one transaction. That is what makes the generation-end
/// sweep's promise true rather than merely usual: the row recording the
/// generation end and every requeue it caused land together or not at all.
pub(crate) fn spawn_queue_journal(
    db: std::sync::Arc<crate::storage::LocalDb>,
    runner_incarnation: String,
    queue: std::sync::Arc<ExecutionQueue>,
) {
    let (sender, mut receiver) = mpsc::unbounded_channel::<QueueJournalBatch>();
    queue.attach_journal(sender);
    // The consumer must not retain the queue that owns its sender. Otherwise
    // dropping the fleet can never close the channel and release the database.
    let queue = std::sync::Arc::downgrade(&queue);
    let on_spawn_failure = queue.clone();
    crate::orchestrator::lifecycle::detach_onto_runtime(
        async move {
            if let Err(error) = expire_previous_incarnations(&db, &runner_incarnation).await {
                log::warn!("{error}");
            }
            while let Some(batch) = receiver.recv().await {
                let incarnation = runner_incarnation.clone();
                let written = db
                .write(move |conn| {
                    let batch = batch.clone();
                    let incarnation = incarnation.clone();
                    Box::pin(async move {
                        for row in &batch {
                            conn.execute(
                                "INSERT OR IGNORE INTO execution_queue(
                                     event_id, entry_id, sequence, runner_incarnation,
                                     request_id, attempt_id, project_id, state,
                                     executor_id, generation, decline_reason, detail,
                                     constraints_json, scoring_json, horizon_unix_ms, created_at)
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                                (
                                    format!("{}:{}:{}", row.entry_id, row.state, row.sequence),
                                    row.entry_id.as_str(),
                                    row.sequence as i64,
                                    incarnation.as_str(),
                                    row.request_id.as_str(),
                                    row.attempt_id.as_str(),
                                    row.project_id.as_str(),
                                    row.state.as_str(),
                                    row.executor_id.as_deref(),
                                    row.generation.map(|generation| generation as i64),
                                    row.decline_reason.as_deref(),
                                    row.detail.as_deref(),
                                    row.constraints_json.as_str(),
                                    row.scoring_json.as_str(),
                                    row.horizon_unix_ms as i64,
                                    row.created_at as i64,
                                ),
                            )
                            .await?;
                        }
                        Ok(())
                    })
                })
                .await;
                if let Err(error) = written {
                    // Recorded rather than thrown. The queue is authoritative and
                    // keeps working; what is lost is explanation, not correctness.
                    if let Some(queue) = queue.upgrade() {
                        *queue.journal_error.lock().unwrap() =
                            Some(format!("execution queue journal write failed: {error}"));
                    }
                }
            }
        },
        move || {
            if let Some(queue) = on_spawn_failure.upgrade() {
                *queue.journal_error.lock().unwrap() =
                    Some("execution queue journal runtime could not start".into());
            }
        },
    );
}

/// Close entries a previous runner incarnation left open.
///
/// Nothing is resurrected. The waiters those entries answered were in-process
/// futures of a process that has ended, so the only honest thing to record is
/// that they expired with it.
pub(crate) async fn expire_previous_incarnations(
    db: &crate::storage::LocalDb,
    runner_incarnation: &str,
) -> Result<(), String> {
    let incarnation = runner_incarnation.to_string();
    db.write(move |conn| {
        let incarnation = incarnation.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT OR IGNORE INTO execution_queue(
                     event_id, entry_id, sequence, runner_incarnation,
                     request_id, attempt_id, project_id, state,
                     constraints_json, scoring_json, horizon_unix_ms, created_at, detail)
                 SELECT
                     live.entry_id || ':expired:restart',
                     live.entry_id,
                     live.sequence + 1,
                     ?1,
                     live.request_id,
                     live.attempt_id,
                     live.project_id,
                     'expired',
                     live.constraints_json,
                     live.scoring_json,
                     live.horizon_unix_ms,
                     unixepoch() * 1000,
                     'runner restarted'
                 FROM execution_queue live
                 JOIN (
                     SELECT entry_id, MAX(sequence) AS sequence
                     FROM execution_queue
                     GROUP BY entry_id
                 ) latest
                   ON latest.entry_id = live.entry_id AND latest.sequence = live.sequence
                 WHERE live.runner_incarnation != ?1
                   AND live.state IN ('queued', 'offered', 'claimed')",
                (incarnation.as_str(),),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("closing a previous runner's queue entries failed: {error}"))
}

/// Test seams on the fleet that drive this queue's own transitions. Kept here,
/// off the executor wire path, because they change nothing a peer receives.
#[cfg(any(test, feature = "test-utils"))]
impl super::Fleet {
    /// Put one command batch for `job_id` in the queue exactly as a real
    /// dispatch leaves it: offered to the colocated executor attached at
    /// `generation`, then claimed by it. For tests outside this crate that
    /// need a running batch a job-level cancel must reach.
    pub fn claim_job_command_for_test(
        &self,
        request_id: &str,
        job_id: &str,
        generation: u64,
    ) -> tokio::sync::oneshot::Receiver<super::CellOutcome> {
        let now = super::unix_time_ms();
        let request = super::CellRequest {
            request_id: request_id.into(),
            attempt_id: format!("{request_id}-attempt"),
            project_id: "p".into(),
            repository: super::RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "p".into(),
                absolute_path: "/repo".into(),
            },
            base_commit: "base".into(),
            command: "cargo test --workspace".into(),
            command_class: super::CellCommandClass::CargoTest,
            placement_work_class: super::PlacementWorkClass::AgentSessions,
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: super::CellPriority::ReviewCheck,
            wait_horizon_unix_ms: now + 60_000,
            waiting_since_unix_ms: 0,
            timeout_ms: 60_000,
            mutation_policy: super::MutationPolicy::PureVerdict,
            requesting_job_id: Some(job_id.into()),
            affinity_key: None,
            executor: None,
            pinned_executor_id: None,
            placement_mobility: Default::default(),
            verdict_platforms: Vec::new(),
            avoided_executor_ids: Vec::new(),
            project_requirements: Default::default(),
            command_resource_identity: None,
            resource_reservation: Default::default(),
            learned_estimate: None,
        };
        let (entry_id, receiver) = self
            .queue
            .enqueue(
                NewEntry {
                    request,
                    admission_kind: super::CellAdmissionKind::Command,
                    interactive: false,
                },
                now,
            )
            .expect("a fresh request identity enqueues");
        assert!(self.queue.offer(
            &entry_id,
            super::COLOCATED_EXECUTOR_ID,
            super::COLOCATED_EXECUTOR_ID,
            generation,
            now
        ));
        assert_eq!(
            self.queue
                .claim(&entry_id, super::COLOCATED_EXECUTOR_ID, generation, now),
            ClaimVerdict::Granted
        );
        receiver
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::RowExt;
    use cairn_common::executor_protocol::{
        CellCommandClass, CellPriority, MutationPolicy, PlacementMobility, PlacementWorkClass,
        RepositoryLocator,
    };

    #[test]
    fn a_synchronous_host_journals_and_releases_the_database_when_its_queue_drops() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let db = std::sync::Arc::new(
            runtime.block_on(crate::storage::migrated_test_db("sync_queue_journal.db")),
        );
        let db_lifetime = std::sync::Arc::downgrade(&db);
        let queue = std::sync::Arc::new(ExecutionQueue::default());
        assert!(tokio::runtime::Handle::try_current().is_err());
        spawn_queue_journal(db.clone(), "sync-runner".into(), queue.clone());
        let (_entry, _waiter) = enqueue(&queue, "sync-request", PlacementMobility::SpillEligible);
        runtime.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    let row = db
                        .query_opt(
                            "SELECT state FROM execution_queue WHERE request_id = ?1",
                            ["sync-request"],
                            |row| row.text(0),
                        )
                        .await
                        .unwrap();
                    if row == Some("queued".to_string()) {
                        break;
                    }
                    assert_eq!(queue.journal_error(), None);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("a synchronous host must drain queue journal events");
        });
        drop(queue);
        drop(db);
        runtime.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while db_lifetime.upgrade().is_some() {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("dropping the queue must stop its journal consumer");
        });
    }

    /// The journal's whole job is to explain what became of an entry, so the
    /// row that says so must land beside the transitions before it. A claimed
    /// entry that settles and a queued entry that is taken both end in a row
    /// of their own, each with a sequence no earlier row holds.
    #[test]
    fn terminal_transitions_reach_the_journal_beside_the_transitions_before_them() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let db = std::sync::Arc::new(runtime.block_on(crate::storage::migrated_test_db(
            "queue_journal_terminal_rows.db",
        )));
        let queue = std::sync::Arc::new(ExecutionQueue::default());
        spawn_queue_journal(db.clone(), "terminal-runner".into(), queue.clone());

        let (settled, _settled_waiter) =
            enqueue(&queue, "settled", PlacementMobility::SpillEligible);
        assert!(queue.offer(&settled, "machine", "machine", 1, 2));
        assert_eq!(
            queue.claim(&settled, "machine", 1, 3),
            ClaimVerdict::Granted
        );
        assert!(queue.settle(
            &("settled".to_string(), "attempt".to_string()),
            Some(("machine", 1)),
            TerminalState::Completed,
            super::super::executor_unavailable("finished".into()),
            4,
        ));
        let (_taken, _taken_waiter) = enqueue(&queue, "taken", PlacementMobility::SpillEligible);
        assert!(queue
            .take(
                &("taken".to_string(), "attempt".to_string()),
                TerminalState::Cancelled,
                5,
            )
            .is_some());

        let journaled = |request_id: &'static str| {
            let db = db.clone();
            async move {
                let joined = db
                    .query_opt(
                        "SELECT COALESCE(group_concat(state || ':' || sequence, ','), '')
                         FROM execution_queue WHERE request_id = ?1",
                        [request_id],
                        |row| row.text(0),
                    )
                    .await
                    .unwrap()
                    .unwrap_or_default();
                let mut rows: Vec<String> = joined
                    .split(',')
                    .filter(|row| !row.is_empty())
                    .map(str::to_string)
                    .collect();
                rows.sort();
                rows
            }
        };
        runtime.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    let settled_rows = journaled("settled").await;
                    let taken_rows = journaled("taken").await;
                    if settled_rows.len() == 4 && taken_rows.len() == 2 {
                        assert_eq!(
                            settled_rows,
                            vec!["claimed:2", "completed:3", "offered:1", "queued:0"]
                        );
                        assert_eq!(taken_rows, vec!["cancelled:1", "queued:0"]);
                        break;
                    }
                    assert_eq!(queue.journal_error(), None);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("terminal rows must be journaled, not discarded as duplicates");
        });
    }

    fn request(request_id: &str, mobility: PlacementMobility) -> CellRequest {
        CellRequest {
            request_id: request_id.into(),
            attempt_id: "attempt".into(),
            project_id: "p".into(),
            repository: RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "p".into(),
                absolute_path: "/repo".into(),
            },
            base_commit: "base".into(),
            command: "cargo test".into(),
            command_class: CellCommandClass::CargoTest,
            placement_work_class: PlacementWorkClass::AgentSessions,
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::ReviewCheck,
            wait_horizon_unix_ms: 10_000,
            waiting_since_unix_ms: 0,
            timeout_ms: 1_000,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            executor: None,
            pinned_executor_id: None,
            placement_mobility: mobility,
            verdict_platforms: Vec::new(),
            avoided_executor_ids: Vec::new(),
            project_requirements: Default::default(),
            command_resource_identity: None,
            resource_reservation: Default::default(),
            learned_estimate: None,
        }
    }

    fn enqueue(
        queue: &ExecutionQueue,
        request_id: &str,
        mobility: PlacementMobility,
    ) -> (EntryId, oneshot::Receiver<CellOutcome>) {
        queue
            .enqueue(
                NewEntry {
                    request: request(request_id, mobility),
                    admission_kind: CellAdmissionKind::Command,
                    interactive: false,
                },
                1,
            )
            .expect("a fresh identity enqueues")
    }

    /// The cairn/4380 class, closed. An offer confers nothing and starts
    /// nothing, so a link ending under it can only mean the work never left the
    /// runner -- and the entry has to become offerable again in the same breath.
    #[test]
    fn a_generation_ending_returns_its_offers_to_the_queue() {
        let queue = ExecutionQueue::default();
        let (entry, _waiter) = enqueue(&queue, "r", PlacementMobility::PinnedOrColocated);
        assert!(queue.offer(&entry, "machine", "machine", 7, 2));

        let sweep = queue.end_generation("machine", 7, GenerationEndCause::LinkGap, "gone", 3);

        assert_eq!(sweep.released_offers, vec![entry.clone()]);
        assert!(sweep.held_claims.is_empty());
        assert_eq!(queue.state_of(&entry), Some(EntryState::Queued));
    }

    /// The regression guard for the double-run this design avoids.
    ///
    /// A claim means a machine took responsibility and may be executing. A link
    /// dropping is not evidence that it stopped -- the machine keeps working and
    /// delivers on its next connection -- so requeueing here would dispatch the
    /// same work to a second machine while the first still runs it.
    #[test]
    fn a_generation_ending_leaves_a_claim_bound_rather_than_re_offering_it() {
        let queue = ExecutionQueue::default();
        let (entry, _waiter) = enqueue(&queue, "r", PlacementMobility::SpillEligible);
        assert!(queue.offer(&entry, "machine", "machine", 7, 2));
        assert_eq!(queue.claim(&entry, "machine", 7, 3), ClaimVerdict::Granted);

        let sweep = queue.end_generation("machine", 7, GenerationEndCause::LinkGap, "gone", 4);

        assert!(
            sweep.released_offers.is_empty(),
            "a claim is not an offer and must never be released as one"
        );
        assert_eq!(sweep.held_claims.len(), 1);
        assert!(
            matches!(queue.state_of(&entry), Some(EntryState::Claimed { .. })),
            "the work is still running on the machine that took it"
        );

        // The machine comes back and the entry re-stamps onto the new link, so
        // the result it computed across the gap is accepted there.
        let carried = queue.rebind("machine", 8);
        assert_eq!(carried.len(), 1);
        assert_eq!(
            queue.state_of(&entry),
            Some(EntryState::Claimed {
                executor_id: "machine".into(),
                generation: 8,
                at_unix_ms: 3,
            })
        );
    }

    /// A runner-initiated close is different: the process behind the link is
    /// being recycled, so work that may move goes back to the queue and work
    /// that cannot is answered.
    #[tokio::test]
    async fn a_machine_declared_gone_requeues_what_can_move_and_fails_what_cannot() {
        let queue = ExecutionQueue::default();
        let (mobile, _mobile_waiter) = enqueue(&queue, "mobile", PlacementMobility::SpillEligible);
        let (pinned, pinned_waiter) =
            enqueue(&queue, "pinned", PlacementMobility::PinnedOrColocated);
        for entry in [&mobile, &pinned] {
            assert!(queue.offer(entry, "machine", "machine", 1, 2));
            assert_eq!(queue.claim(entry, "machine", 1, 3), ClaimVerdict::Granted);
        }

        let sweep = queue.end_generation(
            "machine",
            1,
            GenerationEndCause::MachineGone,
            "executor connection closed before returning a result",
            4,
        );

        assert_eq!(sweep.requeued_claims, vec![mobile.clone()]);
        assert_eq!(queue.state_of(&mobile), Some(EntryState::Queued));
        assert_eq!(sweep.failed_claims.len(), 1);
        assert!(queue.state_of(&pinned).is_none());
        assert!(matches!(
            pinned_waiter.await.unwrap(),
            CellOutcome::Unavailable { ref diagnostic, .. }
                if diagnostic.contains("connection closed")
        ));
    }

    /// A decline routes rather than fails, and it is remembered against the link
    /// it was made on so the next pass does not ask the same machine again.
    #[test]
    fn a_decline_requeues_and_records_the_decliner() {
        let queue = ExecutionQueue::default();
        let (entry, _waiter) = enqueue(&queue, "r", PlacementMobility::SpillEligible);
        assert!(queue.offer(&entry, "first", "first", 1, 2));

        assert!(queue
            .decline(&entry, "first", 1, DeclineReason::Pressure, None, None, 3)
            .applied());

        assert_eq!(queue.state_of(&entry), Some(EntryState::Queued));
        assert!(!queue
            .with_entry(&entry, |held| held.may_offer_to("first", 1))
            .unwrap());
        // Another machine is free to take it immediately.
        assert!(queue
            .with_entry(&entry, |held| held.may_offer_to("second", 1))
            .unwrap());
        assert!(queue.offer(&entry, "second", "second", 1, 4));
    }

    /// New posted state is the only thing that makes reconsidering a machine
    /// meaningful, so it is the only thing that clears a decline. A timer here
    /// would re-ask a machine whose situation had not changed.
    #[test]
    fn a_decliner_becomes_eligible_again_only_when_it_posts_new_state() {
        let queue = ExecutionQueue::default();
        let (entry, _waiter) = enqueue(&queue, "r", PlacementMobility::SpillEligible);
        assert!(queue.offer(&entry, "machine", "machine", 1, 2));
        assert!(queue
            .decline(&entry, "machine", 1, DeclineReason::Capacity, None, None, 3)
            .applied());
        // Asserted through `offer` rather than through the predicate behind it:
        // the queue enforcing its own rule is the behaviour that matters, and a
        // caller whose candidate filter forgot the rule must still be refused.
        assert!(!queue.offer(&entry, "machine", "machine", 1, 4));

        assert!(queue.clear_declines_for("machine"));

        assert!(queue.offer(&entry, "machine", "machine", 1, 5));
    }

    /// A claim that arrives after its entry moved on is answered rather than
    /// honoured, which is what keeps a stale link from binding work twice.
    #[test]
    fn a_claim_from_a_link_that_has_moved_on_is_lost() {
        let queue = ExecutionQueue::default();
        let (entry, _waiter) = enqueue(&queue, "r", PlacementMobility::SpillEligible);
        assert!(queue.offer(&entry, "machine", "machine", 1, 2));
        queue.end_generation("machine", 1, GenerationEndCause::LinkGap, "gone", 3);

        assert_eq!(queue.claim(&entry, "machine", 1, 4), ClaimVerdict::Lost);
        assert_eq!(
            queue.claim("no-such-entry", "machine", 1, 4),
            ClaimVerdict::Lost
        );
    }

    /// An entry whose requester stopped waiting leaves the queue, and leaves it
    /// completely: the identity is free, so a genuine retry is not refused as a
    /// duplicate of work nobody is waiting for any more.
    #[test]
    fn an_expired_entry_leaves_the_queue_and_frees_its_identity() {
        let queue = ExecutionQueue::default();
        let (entry, _waiter) = enqueue(&queue, "r", PlacementMobility::SpillEligible);
        assert!(queue.offer(&entry, "machine", "machine", 1, 2));

        queue.take(
            &("r".to_string(), "attempt".to_string()),
            TerminalState::Expired,
            10_000,
        );

        assert_eq!(queue.state_of(&entry), None);
        let (second, _second_waiter) = enqueue(&queue, "r", PlacementMobility::SpillEligible);
        assert_ne!(second, entry);
    }

    /// One live entry per request identity, which is what preserves the
    /// duplicate-submission refusal.
    #[test]
    fn one_identity_holds_at_most_one_live_entry() {
        let queue = ExecutionQueue::default();
        let (_entry, _waiter) = enqueue(&queue, "r", PlacementMobility::SpillEligible);

        assert!(queue
            .enqueue(
                NewEntry {
                    request: request("r", PlacementMobility::SpillEligible),
                    admission_kind: CellAdmissionKind::Command,
                    interactive: false,
                },
                1,
            )
            .is_err());

        // Settling it frees the identity for a genuine second attempt.
        queue.settle(
            &("r".to_string(), "attempt".to_string()),
            None,
            TerminalState::Completed,
            super::super::executor_unavailable("done".into()),
            2,
        );
        assert!(queue
            .enqueue(
                NewEntry {
                    request: request("r", PlacementMobility::SpillEligible),
                    admission_kind: CellAdmissionKind::Command,
                    interactive: false,
                },
                3,
            )
            .is_ok());
    }

    /// Cancelling an entry that was only ever offered leaves nothing to stop,
    /// because an offer starts nothing. Only a claim names a machine worth
    /// sending a Cancel to.
    #[test]
    fn only_a_claimed_entry_names_a_machine_to_cancel() {
        let queue = ExecutionQueue::default();
        let identity = ("r".to_string(), "attempt".to_string());
        let (entry, _waiter) = enqueue(&queue, "r", PlacementMobility::SpillEligible);
        assert!(queue.offer(&entry, "machine", "machine", 1, 2));

        assert_eq!(queue.claimed_binding_of(&identity), None);

        assert_eq!(queue.claim(&entry, "machine", 1, 3), ClaimVerdict::Granted);
        assert_eq!(
            queue.claimed_binding_of(&identity),
            Some(("machine".to_string(), 1))
        );
    }

    /// The chaos case this design exists to close (cairn/4380): a machine that
    /// is killed between being offered work and answering. Nothing was running
    /// behind the offer, so the entry is simply free again and the next machine
    /// takes it — with no parked state anywhere, and nothing waiting out a
    /// timer.
    #[test]
    fn work_offered_to_a_machine_that_dies_lands_on_the_next_one() {
        let queue = ExecutionQueue::default();
        let (entry, _waiter) = enqueue(&queue, "r", PlacementMobility::SpillEligible);
        assert!(queue.offer(&entry, "first", "first", 3, 2));

        // The link dies before the claim arrives.
        let sweep = queue.end_generation("first", 3, GenerationEndCause::LinkGap, "gone", 3);
        assert_eq!(sweep.released_offers, vec![entry.clone()]);

        // The second machine is offered it and takes it.
        assert!(queue.offer(&entry, "second", "second", 1, 4));
        assert_eq!(queue.claim(&entry, "second", 1, 5), ClaimVerdict::Granted);
        assert!(matches!(
            queue.state_of(&entry),
            Some(EntryState::Claimed { ref executor_id, .. }) if executor_id == "second"
        ));

        // A claim from the dead link cannot bind the work a second time.
        assert_eq!(queue.claim(&entry, "first", 3, 6), ClaimVerdict::Lost);

        // The whole account of what happened is the chain, and nothing in it is
        // a park.
        let chain = queue.with_entry(&entry, |held| held.chain.clone()).unwrap();
        assert_eq!(chain.len(), 4);
        assert!(matches!(chain[1], BindingEvent::GenerationEnded { .. }));
    }

    /// A machine that refuses for want of memory does not hold the work while it
    /// waits for room: the entry is on another machine immediately, without any
    /// timer between the refusal and the next offer.
    #[test]
    fn a_machine_declining_for_pressure_routes_the_work_elsewhere_at_once() {
        let queue = ExecutionQueue::default();
        let (entry, _waiter) = enqueue(&queue, "r", PlacementMobility::SpillEligible);
        assert!(queue.offer(&entry, "saturated", "saturated", 1, 2));

        assert!(queue
            .decline(
                &entry,
                "saturated",
                1,
                DeclineReason::Pressure,
                Some("needs 2147483648 bytes of memory, has 943718400".into()),
                None,
                3,
            )
            .applied());

        assert_eq!(queue.state_of(&entry), Some(EntryState::Queued));
        assert!(queue.offer(&entry, "roomy", "roomy", 1, 3));
        assert_eq!(queue.claim(&entry, "roomy", 1, 3), ClaimVerdict::Granted);

        let chain = queue.with_entry(&entry, |held| held.chain.clone()).unwrap();
        let BindingEvent::Declined { detail, .. } = &chain[1] else {
            panic!("the refusal is recorded as a decline");
        };
        assert!(
            detail
                .as_deref()
                .is_some_and(|detail| detail.contains("943718400")),
            "an operator is owed which floor was crossed by how much, not a bare reason"
        );
    }

    /// The chain is the placement record's account of what actually happened,
    /// and it accumulates across every machine the entry visited.
    #[test]
    fn the_binding_chain_records_every_step_in_order() {
        let queue = ExecutionQueue::default();
        let (entry, _waiter) = enqueue(&queue, "r", PlacementMobility::SpillEligible);
        assert!(queue.offer(&entry, "first", "first", 1, 2));
        assert!(queue
            .decline(
                &entry,
                "first",
                1,
                DeclineReason::Pressure,
                Some("needs 2 GiB, has 900 MiB".into()),
                None,
                3
            )
            .applied());
        assert!(queue.offer(&entry, "second", "second", 1, 4));
        assert_eq!(queue.claim(&entry, "second", 1, 5), ClaimVerdict::Granted);

        let chain = queue.with_entry(&entry, |held| held.chain.clone()).unwrap();
        assert!(matches!(chain[0], BindingEvent::Offered { .. }));
        assert!(matches!(
            chain[1],
            BindingEvent::Declined {
                reason: DeclineReason::Pressure,
                ..
            }
        ));
        assert!(matches!(chain[2], BindingEvent::Offered { .. }));
        assert!(matches!(chain[3], BindingEvent::Claimed { .. }));
        assert_eq!(chain.len(), 4);
    }

    /// A machine posting new state makes it offerable again, but what it said
    /// stays on the entry: a request that runs out its horizon in the instant
    /// between that post and the next answer must still be able to say who
    /// refused it and why.
    #[test]
    fn a_decline_keeps_its_detail_and_time_after_the_machine_posts_state() {
        let queue = ExecutionQueue::default();
        let (entry, _waiter) = enqueue(&queue, "r", PlacementMobility::SpillEligible);
        assert!(queue.offer(&entry, "machine", "machine", 1, 2));
        assert!(queue
            .decline(
                &entry,
                "machine",
                1,
                DeclineReason::ProjectProvisioning,
                Some("project p is provisioning for request other".into()),
                None,
                3,
            )
            .applied());

        let recorded = |queue: &ExecutionQueue| {
            queue
                .with_entry(&entry, |held| held.declined.get("machine").cloned())
                .unwrap()
                .unwrap()
        };
        let standing = recorded(&queue);
        assert_eq!(standing.reason, DeclineReason::ProjectProvisioning);
        assert_eq!(
            standing.detail.as_deref(),
            Some("project p is provisioning for request other")
        );
        assert_eq!(standing.at_unix_ms, 3);
        assert!(standing.stands_on(1));

        assert!(queue.clear_declines_for("machine"));
        assert!(!queue.clear_declines_for("machine"), "already superseded");
        let superseded = recorded(&queue);
        assert!(!superseded.stands_on(1));
        assert_eq!(superseded.detail, standing.detail);
        assert_eq!(superseded.at_unix_ms, 3);
    }

    fn journaled_states(receiver: &mut mpsc::UnboundedReceiver<QueueJournalBatch>) -> Vec<String> {
        let mut states = Vec::new();
        while let Ok(batch) = receiver.try_recv() {
            states.extend(batch.into_iter().map(|row| row.state));
        }
        states
    }

    /// A request parked behind a pressured machine is re-asked every time the
    /// machine posts state, which is every few seconds. The same answer again
    /// is one continuing refusal: it refreshes the recorded decline rather than
    /// growing the chain and the journal by two rows per post for as long as
    /// the pressure lasts.
    #[test]
    fn an_identical_re_decline_folds_into_the_decline_already_recorded() {
        let queue = ExecutionQueue::default();
        let (sender, mut journal) = mpsc::unbounded_channel();
        queue.attach_journal(sender);
        let (entry, _waiter) = enqueue(&queue, "r", PlacementMobility::SpillEligible);
        assert!(queue.offer(&entry, "machine", "machine", 1, 2));
        assert_eq!(
            queue.decline(
                &entry,
                "machine",
                1,
                DeclineReason::Pressure,
                Some("cpu 99%".into()),
                None,
                3
            ),
            DeclineRecord::Recorded { repeat: false }
        );
        assert_eq!(
            journaled_states(&mut journal),
            vec!["queued", "offered", "queued"]
        );

        for at in [10, 20, 30] {
            assert!(queue.clear_declines_for("machine"));
            assert!(queue.offer(&entry, "machine", "machine", 1, at));
            assert_eq!(
                queue.decline(
                    &entry,
                    "machine",
                    1,
                    DeclineReason::Pressure,
                    Some(format!("cpu at {at}")),
                    None,
                    at + 1
                ),
                DeclineRecord::Recorded { repeat: true }
            );
        }
        assert!(
            journaled_states(&mut journal).is_empty(),
            "nothing new to journal"
        );
        let chain = queue.with_entry(&entry, |held| held.chain.clone()).unwrap();
        assert_eq!(chain.len(), 2, "{chain:?}");
        assert!(matches!(
            &chain[1],
            BindingEvent::Declined { detail: Some(detail), at_unix_ms: 31, .. } if detail == "cpu at 30"
        ));

        // A different answer is a new step, and the re-offer that produced it
        // is journaled ahead of it.
        assert!(queue.clear_declines_for("machine"));
        assert!(queue.offer(&entry, "machine", "machine", 1, 40));
        assert_eq!(
            queue.decline(
                &entry,
                "machine",
                1,
                DeclineReason::ProjectProvisioning,
                None,
                None,
                41
            ),
            DeclineRecord::Recorded { repeat: false }
        );
        assert_eq!(journaled_states(&mut journal), vec!["offered", "queued"]);
        assert_eq!(
            queue.with_entry(&entry, |held| held.chain.len()).unwrap(),
            4
        );

        // And a re-offer that is finally claimed is journaled before the claim.
        assert!(queue.clear_declines_for("machine"));
        assert!(queue.offer(&entry, "machine", "machine", 1, 50));
        assert_eq!(queue.claim(&entry, "machine", 1, 51), ClaimVerdict::Granted);
        assert_eq!(journaled_states(&mut journal), vec!["offered", "claimed"]);
    }
}
