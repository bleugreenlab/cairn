//! Identity for dispatched work.
//!
//! Every check and attestation Cairn dispatches answers a question. Until this
//! module existed the question was nowhere in the system: a request carried a
//! commit, a tree hash, and a result key, all of which name the *answer*. Two
//! runs asking the same question of different tips were, to every collection
//! that mattered, unrelated work — so nothing could notice they were redundant,
//! and every mechanism that tried to suppress the redundant one (supersede
//! tokens, drop guards, fire-and-forget cancels) was compensating after dispatch
//! for a fact the dispatch never recorded. Those mechanisms raced the work they
//! chased, and lost often enough for an operator to watch several attestations
//! of one project run at once.
//!
//! The fix is to record the fact. A [`PurposeKey`] names the question, the
//! registry admits at most one live item per key, and admitting a newer item
//! *is* the retirement of the older one, decided in a single transition under a
//! single lock. Two concurrent answers to one question stop being something the
//! system prevents and become something it cannot represent.
//!
//! Recording that fact is necessary and not sufficient, and the gap between the
//! two is where the original defect would otherwise reappear wearing better
//! bookkeeping. Displacing an item does not stop it: the instruction to stop
//! travels to another process on another machine, and the work keeps consuming
//! a cell until it acts on it. So a displaced item is kept here as a **debt**
//! against its question, cleared only by that item's own terminal outcome, and
//! the successor that displaced it may not dispatch while the debt stands.
//! Without that, the registry would show one live answer while two ran — the
//! same overlap as before, now harder to see because the record looked correct.
//!
//! Two properties are load-bearing and easy to lose in a later edit:
//!
//! - **The key names the question, never the commit.** A newer tip is a newer
//!   answer to the same question. Folding a revision into the key would make
//!   every tip its own question and restore exactly the behavior this replaces.
//! - **Phase is a field, not a collection.** The defect this retires was, on
//!   both sides of the wire, a lookup consulting one phase's map while the item
//!   sat in another's: the runner searched `pending` while the leader was still
//!   in `preparing_leaders`. Splitting phases back into separate maps that a
//!   lookup can miss reintroduces it.
//!
//! What this module deliberately does NOT hold is who wants an answer. That is
//! the coalescing registry's `subscribers`, which already tracks it per
//! execution for every check Cairn runs; a second copy here would be one more
//! thing to keep true.
//!
//! # What settles a debt
//!
//! A debt is cleared by evidence the runner already holds that the work is no
//! longer running, and by nothing else. There are four kinds, and each has one
//! entry point here:
//!
//! - the attempt's own terminal outcome, or a runner decision that proves
//!   nothing ran ([`PurposeRegistry::finish`]);
//! - the bound machine's own account of its work, read from its snapshots
//!   ([`PurposeRegistry::settle_on_machine`],
//!   [`PurposeRegistry::post_boot_account`]);
//! - the bound machine being gone by the runner's authority
//!   ([`PurposeRegistry::settle_on_machine`]);
//! - an operator naming the exact overlap they authorize
//!   ([`PurposeRegistry::authorize_overlap`]).
//!
//! A timer, a watchdog that gave up, a leader task that vanished, and a runner
//! restart are not on that list. Each leaves the debt in place as
//! [`Confirmation::Unconfirmed`] with its reason named
//! ([`PurposeRegistry::end_unconfirmed`]), because the work may still be
//! running on a machine the runner cannot currently hear.
//!
//! # Why dispatched items are persisted
//!
//! A runner restart forgets an in-memory debt, but the remote machine running
//! the work does not restart with it. So every item that reached
//! [`ItemPhase::Dispatched`] is journaled ([`DebtChange`]) from the grant until
//! it settles, and on boot every journaled row comes back as an unconfirmed
//! debt. Items that never reached a machine are not journaled: the restart
//! itself ends runner-owned work.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::RequestIdentity;
use crate::storage::{LocalDb, RowExt};

/// The question a dispatched work item answers.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct PurposeKey(String);

impl PurposeKey {
    /// The standing question "what is this check's whole-suite verdict on this
    /// project's default branch, on this platform" (one main-cadence lane).
    ///
    /// Deliberately free of the tip being attested: a newer tip is a newer
    /// answer to the same question. The cadence itself never arms a successor
    /// while a lane's run is live, so in practice this retires only a run an
    /// earlier process left behind.
    pub(crate) fn main_cadence(project_id: &str, check: &str, platform: &str) -> Self {
        Self(format!("main-cadence:{project_id}:{check}:{platform}"))
    }

    /// A key as it was written to the debt journal, or as an operator named it.
    pub(crate) fn from_persisted(key: String) -> Self {
        Self(key)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PurposeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Where a live item is in its lifecycle.
///
/// These are the runner's phases, which is the whole set the chokepoint can act
/// on. Once an item is `Dispatched` the executor owns its finer states — queued,
/// provisioning, running — and observes retirement at each of them (protocol
/// 68). Mirroring those here would be a second copy of a truth that lives on the
/// other side of a wire, and stale copies of that truth are what this module
/// exists to abolish.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ItemPhase {
    /// Being placed and provisioned by the runner; not yet on the wire.
    Preparing,
    /// Handed to an executor.
    Dispatched,
}

impl ItemPhase {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::Dispatched => "dispatched",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "preparing" => Some(Self::Preparing),
            "dispatched" => Some(Self::Dispatched),
            _ => None,
        }
    }

    /// When, in an item's life, it was retired.
    pub(crate) fn describe_retirement(self) -> &'static str {
        match self {
            Self::Preparing => "before it reached a machine",
            Self::Dispatched => "after it was granted to a machine",
        }
    }
}

/// A unix-millisecond instant as an operator reads it.
pub(crate) fn stamp(unix_ms: u64) -> String {
    crate::clock::stamp_millis_with_seconds(unix_ms as i64)
        .unwrap_or_else(|| format!("unix ms {unix_ms}"))
}

/// The machine an item was granted to, recorded at the grant.
///
/// Captured then rather than looked up later because the queue forgets a
/// binding once its entry is answered, and a debt whose work outlived its
/// entry (a watchdog that gave up, a runner restart) still has to name the
/// machine that may be running it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DebtPlacement {
    pub(crate) executor_id: String,
    pub(crate) generation: u64,
    pub(crate) dispatched_at_unix_ms: u64,
    /// Set on a debt reloaded at boot until that machine's first snapshot in
    /// this process has been read against it. Generations restart at boot, so
    /// the recorded one cannot be compared with a live connection's until that
    /// account rebases it.
    pub(crate) awaiting_post_boot_account: bool,
}

/// Why an item stopped being its question's live answer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub(crate) enum RetirementCause {
    /// A newer answer to the same question took the key.
    Displaced { by: RequestIdentity },
    /// Someone asked for this work to stop and nothing replaced it.
    Retracted,
    /// Nothing displaced it: its attempt ended on the runner's side without
    /// the machine saying it had stopped.
    Unanswered,
    /// It was the live answer when the runner last stopped, and the runner
    /// came back to find it still owed.
    Reloaded,
}

impl RetirementCause {
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Displaced { by } => {
                format!("displaced by request {} attempt {}", by.0, by.1)
            }
            Self::Retracted => "retracted with nothing replacing it".into(),
            Self::Unanswered => {
                "nothing displaced it; its attempt ended without the machine answering".into()
            }
            Self::Reloaded => "it was the live answer when the runner last stopped".into(),
        }
    }
}

/// Why the runner can no longer expect this debt's own terminal outcome.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub(crate) enum UnconfirmedReason {
    /// The runner stopped waiting on a grant it had delivered.
    GrantAbandoned { detail: String },
    /// The task waiting on the attempt panicked or was dropped.
    LeaderVanished,
    /// The runner restarted while the attempt was on a machine.
    RunnerRestarted { at_unix_ms: u64 },
}

impl UnconfirmedReason {
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::GrantAbandoned { detail } => format!("grant abandoned by the runner: {detail}"),
            Self::LeaderVanished => "leader task vanished".into(),
            Self::RunnerRestarted { at_unix_ms } => {
                format!("runner restarted at {}", stamp(*at_unix_ms))
            }
        }
    }
}

/// Whether a debt's own terminal outcome is still on its way.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub(crate) enum Confirmation {
    /// Something is still waiting on the attempt, and its answer will settle
    /// the debt.
    Awaiting,
    /// Nothing is waiting on the attempt any more. Only machine evidence or an
    /// operator settles it now.
    Unconfirmed {
        since_unix_ms: u64,
        reason: UnconfirmedReason,
    },
}

impl Confirmation {
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Awaiting => "awaiting its own terminal outcome".into(),
            Self::Unconfirmed {
                reason: reason @ UnconfirmedReason::RunnerRestarted { .. },
                ..
            } => format!("unconfirmed: {}", reason.describe()),
            Self::Unconfirmed {
                since_unix_ms,
                reason,
            } => format!(
                "unconfirmed since {}: {}",
                stamp(*since_unix_ms),
                reason.describe()
            ),
        }
    }
}

/// One live answer to one question.
#[derive(Clone, Debug)]
struct LiveItem {
    request: RequestIdentity,
    phase: ItemPhase,
    armed_at_unix_ms: u64,
    placement: Option<DebtPlacement>,
}

/// An item told to stop, whose death has not yet been observed.
///
/// The distinction between this and "gone" is the whole correctness content of
/// this module. Telling work to stop is a message; stopping is something the
/// work does, later, somewhere else. A registry that dropped a displaced item on
/// the instruction alone would show one live answer while two were running — the
/// original defect, reproduced one level up, and harder to see because the
/// bookkeeping now looks right.
#[derive(Clone, Debug)]
struct RetiringItem {
    request: RequestIdentity,
    phase: ItemPhase,
    placement: Option<DebtPlacement>,
    cause: RetirementCause,
    confirmation: Confirmation,
    retired_at_unix_ms: u64,
}

impl RetiringItem {
    fn from_live(
        live: LiveItem,
        cause: RetirementCause,
        confirmation: Confirmation,
        at_unix_ms: u64,
    ) -> Self {
        Self {
            request: live.request,
            phase: live.phase,
            placement: live.placement,
            cause,
            confirmation,
            retired_at_unix_ms: at_unix_ms,
        }
    }

    fn view(&self, key: &PurposeKey) -> PurposeDebtView {
        PurposeDebtView {
            key: key.clone(),
            request: self.request.clone(),
            phase: self.phase,
            placement: self.placement.clone(),
            cause: self.cause.clone(),
            confirmation: self.confirmation.clone(),
            retired_at_unix_ms: self.retired_at_unix_ms,
        }
    }
}

/// One outstanding debt, as the refusal, the executors page, and the operator
/// escape describe it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PurposeDebtView {
    pub(crate) key: PurposeKey,
    pub(crate) request: RequestIdentity,
    pub(crate) phase: ItemPhase,
    /// `None` for an item that was never granted to a machine.
    pub(crate) placement: Option<DebtPlacement>,
    pub(crate) cause: RetirementCause,
    pub(crate) confirmation: Confirmation,
    pub(crate) retired_at_unix_ms: u64,
}

/// Whether a journaled item was the live answer or a debt when last written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DebtRowState {
    Live,
    Retiring,
}

impl DebtRowState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Retiring => "retiring",
        }
    }
}

/// The durable form of one item that reached a machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PurposeDebtRow {
    pub(crate) key: PurposeKey,
    pub(crate) request: RequestIdentity,
    pub(crate) state: DebtRowState,
    pub(crate) phase: ItemPhase,
    pub(crate) placement: DebtPlacement,
    /// Absent while the item is still the live answer.
    pub(crate) cause: Option<RetirementCause>,
    pub(crate) confirmation: Option<Confirmation>,
    pub(crate) retired_at_unix_ms: Option<u64>,
}

/// One change to the durable set of placed items, in the order the registry
/// made it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DebtChange {
    Upsert(PurposeDebtRow),
    Delete {
        key: PurposeKey,
        request: RequestIdentity,
    },
}

/// What the first account of one machine read against the debts reloaded for
/// it in this process said about them. That is its first snapshot after the
/// reload finished, which is never earlier than its first snapshot after boot.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct PostBootAccount {
    /// Absent from the account, so not running: settled.
    pub(crate) settled: Vec<PurposeDebtView>,
    /// Still listed by the machine. The caller owes each a `Cancel`, and the
    /// debt stays until the machine says it ended.
    pub(crate) still_running: Vec<RequestIdentity>,
}

/// Everything a question currently owns: its live answer, if it has one, and
/// every displaced answer not yet confirmed dead.
///
/// The key survives while either is non-empty. A question whose live item
/// finished but whose predecessor is still dying is not free — admitting a new
/// item there would put it beside the survivor.
#[derive(Default)]
struct PurposeState {
    live: Option<LiveItem>,
    retiring: Vec<RetiringItem>,
}

impl PurposeState {
    fn is_vacant(&self) -> bool {
        self.live.is_none() && self.retiring.is_empty()
    }
}

/// What admitting an item to a key did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Admission {
    /// The key was free; this item is now its live answer.
    Admitted,
    /// This item is now the live answer and the named one is retired by that
    /// same act. The caller owes the displaced request a termination.
    Replaced {
        retire: RequestIdentity,
        phase: ItemPhase,
    },
    /// An item armed at or after this one already holds the key, so this one is
    /// stale on arrival and must not dispatch. Newest wins, and "newest" is
    /// decided here rather than by who reaches an executor first.
    Superseded { holder: RequestIdentity },
}

/// At most one live item per question.
#[derive(Default)]
pub(crate) struct PurposeRegistry {
    items: HashMap<PurposeKey, PurposeState>,
    /// Where changes to placed items are sent to be made durable. Absent on a
    /// registry with no store, which is every bare `Fleet::default()`.
    journal: Option<mpsc::UnboundedSender<DebtChange>>,
    /// Machines the runner lost while a boot reload was still reading the
    /// journal: `Some` from [`Self::expect_reload`] until [`Self::reload`]. A
    /// reloaded row naming one of them settles as it arrives, because the
    /// evidence that settles it landed before the row did.
    lost_before_reload: Option<HashSet<String>>,
}

impl PurposeRegistry {
    /// Note that journaled rows are about to be read back, so machines lost
    /// before they arrive are remembered against them.
    pub(crate) fn expect_reload(&mut self) {
        self.lost_before_reload = Some(HashSet::new());
    }

    /// Settle every debt bound to `executor_id`, because the runner knows that
    /// machine's work is gone, including any debt for it still to be reloaded.
    pub(crate) fn machine_lost(&mut self, executor_id: &str) -> Vec<PurposeDebtView> {
        if let Some(lost) = self.lost_before_reload.as_mut() {
            lost.insert(executor_id.to_string());
        }
        self.settle_on_machine(executor_id, |_, _| true)
    }

    /// Install the durable sink. Every later change to an item that reached a
    /// machine is sent there, in the order it was made.
    pub(crate) fn attach_journal(&mut self, journal: mpsc::UnboundedSender<DebtChange>) {
        self.journal = Some(journal);
    }

    /// Whether `request` currently holds a placement anywhere in the registry.
    fn placed(&self, request: &RequestIdentity) -> bool {
        self.items.values().any(|state| {
            state
                .live
                .as_ref()
                .is_some_and(|live| &live.request == request && live.placement.is_some())
                || state
                    .retiring
                    .iter()
                    .any(|item| &item.request == request && item.placement.is_some())
        })
    }

    /// Send the durable form of `request` under `key` as it now stands: its
    /// row if it is placed, or its deletion if it was placed before this
    /// change and is not now.
    fn journal_item(&self, key: &PurposeKey, request: &RequestIdentity, was_placed: bool) {
        let Some(journal) = &self.journal else {
            return;
        };
        let row = self.items.get(key).and_then(|state| {
            if let Some(live) = state.live.as_ref().filter(|live| &live.request == request) {
                return live.placement.clone().map(|placement| PurposeDebtRow {
                    key: key.clone(),
                    request: request.clone(),
                    state: DebtRowState::Live,
                    phase: live.phase,
                    placement,
                    cause: None,
                    confirmation: None,
                    retired_at_unix_ms: None,
                });
            }
            state
                .retiring
                .iter()
                .find(|item| &item.request == request)
                .and_then(|item| {
                    item.placement.clone().map(|placement| PurposeDebtRow {
                        key: key.clone(),
                        request: request.clone(),
                        state: DebtRowState::Retiring,
                        phase: item.phase,
                        placement,
                        cause: Some(item.cause.clone()),
                        confirmation: Some(item.confirmation.clone()),
                        retired_at_unix_ms: Some(item.retired_at_unix_ms),
                    })
                })
        });
        let change = match row {
            Some(row) => DebtChange::Upsert(row),
            None if was_placed => DebtChange::Delete {
                key: key.clone(),
                request: request.clone(),
            },
            None => return,
        };
        let _ = journal.send(change);
    }

    fn journal_removed(&self, key: &PurposeKey, request: &RequestIdentity) {
        if let Some(journal) = &self.journal {
            let _ = journal.send(DebtChange::Delete {
                key: key.clone(),
                request: request.clone(),
            });
        }
    }

    fn prune(&mut self) {
        self.items.retain(|_, state| !state.is_vacant());
    }

    /// Admit `request` as the live answer to `key`, displacing any older one.
    ///
    /// The whole transition happens here, under the caller's single lock: the
    /// decision to replace and the installation of the replacement cannot be
    /// separated, so there is no interval in which the key names both items or
    /// neither.
    ///
    /// That is a statement about the record, not about the fleet. The displaced
    /// item is still running somewhere until its terminal outcome says
    /// otherwise, which is why it is retained here as a debt and why winning the
    /// key does not entitle the caller to dispatch. See
    /// [`Self::pending_retirements`].
    pub(crate) fn admit(
        &mut self,
        key: PurposeKey,
        request: RequestIdentity,
        armed_at_unix_ms: u64,
    ) -> Admission {
        let state = self.items.entry(key.clone()).or_default();
        let Some(existing) = state.live.as_mut() else {
            state.live = Some(LiveItem {
                request,
                phase: ItemPhase::Preparing,
                armed_at_unix_ms,
                placement: None,
            });
            return Admission::Admitted;
        };
        if existing.request == request {
            return Admission::Admitted;
        }
        // Ties go to the incumbent. Two items armed in the same millisecond are
        // indistinguishable by age, and admitting both in turn would let them
        // replace each other indefinitely.
        if existing.armed_at_unix_ms >= armed_at_unix_ms {
            return Admission::Superseded {
                holder: existing.request.clone(),
            };
        }
        let displaced = state
            .live
            .replace(LiveItem {
                request: request.clone(),
                phase: ItemPhase::Preparing,
                armed_at_unix_ms,
                placement: None,
            })
            .expect("the live item was just read");
        let retire = displaced.request.clone();
        let phase = displaced.phase;
        let was_placed = displaced.placement.is_some();
        // The displaced item becomes this question's debt rather than leaving
        // it. Until its death is observed the key is not free, and the caller
        // that just won the key may not dispatch.
        state.retiring.push(RetiringItem::from_live(
            displaced,
            RetirementCause::Displaced { by: request },
            Confirmation::Awaiting,
            armed_at_unix_ms,
        ));
        self.journal_item(&key, &retire, was_placed);
        Admission::Replaced { retire, phase }
    }

    /// Move the live item for `request` into retirement without replacing it.
    ///
    /// The requester-facing path: someone asked for this work to stop and no
    /// newer answer is taking its place. The key stays occupied by the debt, so
    /// a fresh arrival still waits for the death rather than starting beside it.
    pub(crate) fn retire(&mut self, request: &RequestIdentity, at_unix_ms: u64) -> bool {
        self.retire_live(
            request,
            RetirementCause::Retracted,
            Confirmation::Awaiting,
            at_unix_ms,
        )
    }

    fn retire_live(
        &mut self,
        request: &RequestIdentity,
        cause: RetirementCause,
        confirmation: Confirmation,
        at_unix_ms: u64,
    ) -> bool {
        let Some((key, state)) = self.items.iter_mut().find(|(_, state)| {
            state
                .live
                .as_ref()
                .is_some_and(|live| &live.request == request)
        }) else {
            return false;
        };
        let key = key.clone();
        let live = state.live.take().expect("the live item was just matched");
        let was_placed = live.placement.is_some();
        state.retiring.push(RetiringItem::from_live(
            live,
            cause,
            confirmation,
            at_unix_ms,
        ));
        self.journal_item(&key, request, was_placed);
        true
    }

    /// Record a debt on `key` for an execution this question joined rather
    /// than led, because retiring the question's own item retired that
    /// execution too. Its leader's terminal outcome clears it through
    /// [`Self::finish`], like any other debt.
    pub(crate) fn owe_for_joined(
        &mut self,
        key: &PurposeKey,
        joined: RequestIdentity,
        placement: Option<DebtPlacement>,
        cause: RetirementCause,
        at_unix_ms: u64,
    ) {
        let state = self.items.entry(key.clone()).or_default();
        if state.retiring.iter().any(|item| item.request == joined) {
            return;
        }
        let phase = if placement.is_some() {
            ItemPhase::Dispatched
        } else {
            ItemPhase::Preparing
        };
        state.retiring.push(RetiringItem {
            request: joined.clone(),
            phase,
            placement,
            cause,
            confirmation: Confirmation::Awaiting,
            retired_at_unix_ms: at_unix_ms,
        });
        self.journal_item(key, &joined, false);
    }

    /// Whether `request` is still the live answer to `key`.
    ///
    /// An item waiting at the dispatch gate has to be able to discover that it
    /// was displaced while it waited. Without that it would wait on a set that
    /// now contains itself, since being displaced is exactly what puts it there.
    pub(crate) fn is_live_holder(&self, key: &PurposeKey, request: &RequestIdentity) -> bool {
        self.items
            .get(key)
            .and_then(|state| state.live.as_ref())
            .is_some_and(|live| &live.request == request)
    }

    /// How many displaced items this question is still waiting on.
    ///
    /// Zero is the only state in which a newly admitted item may dispatch. This
    /// is the difference between "the registry shows one holder" and "one work
    /// item exists", and it is the question the chokepoint actually has to ask.
    pub(crate) fn pending_retirements(&self, key: &PurposeKey) -> usize {
        self.items
            .get(key)
            .map(|state| state.retiring.len())
            .unwrap_or(0)
    }

    /// Every outstanding debt on `key`, oldest first.
    pub(crate) fn debts_for(&self, key: &PurposeKey) -> Vec<PurposeDebtView> {
        let mut debts: Vec<_> = self
            .items
            .get(key)
            .map(|state| state.retiring.iter().map(|item| item.view(key)).collect())
            .unwrap_or_default();
        debts.sort_by_key(|debt| debt.retired_at_unix_ms);
        debts
    }

    /// Every outstanding debt on every question, ordered by question and then
    /// by age.
    pub(crate) fn debts(&self) -> Vec<PurposeDebtView> {
        let mut debts: Vec<_> = self
            .items
            .iter()
            .flat_map(|(key, state)| state.retiring.iter().map(move |item| item.view(key)))
            .collect();
        debts.sort_by(|left, right| {
            (&left.key, left.retired_at_unix_ms).cmp(&(&right.key, right.retired_at_unix_ms))
        });
        debts
    }

    /// Record that `request` was granted to a machine.
    ///
    /// From here a runner that forgets the item would leave work running that
    /// nothing owes a death, so this is where the item becomes durable. The
    /// item may already be a debt: retirement can land between the last
    /// pre-grant check and the grant itself, and the debt must then name the
    /// machine it went to as much as a live item would.
    pub(crate) fn dispatched(
        &mut self,
        request: &RequestIdentity,
        executor_id: &str,
        generation: u64,
        at_unix_ms: u64,
    ) -> bool {
        let placement = DebtPlacement {
            executor_id: executor_id.to_string(),
            generation,
            dispatched_at_unix_ms: at_unix_ms,
            awaiting_post_boot_account: false,
        };
        let mut touched = Vec::new();
        for (key, state) in &mut self.items {
            if let Some(live) = state.live.as_mut().filter(|live| &live.request == request) {
                live.phase = ItemPhase::Dispatched;
                live.placement = Some(placement.clone());
                touched.push(key.clone());
            }
            for item in state
                .retiring
                .iter_mut()
                .filter(|item| &item.request == request)
            {
                item.phase = ItemPhase::Dispatched;
                item.placement = Some(placement.clone());
                touched.push(key.clone());
            }
        }
        for key in &touched {
            self.journal_item(key, request, false);
        }
        !touched.is_empty()
    }

    /// The attempt for `request` ended on the runner's side without proof that
    /// nothing is running.
    ///
    /// An item that never reached a machine has nothing running anywhere, so
    /// this is [`Self::finish`] for it. One that did keeps (or becomes) a debt,
    /// marked unconfirmed with `reason`: a watchdog giving up and a task
    /// vanishing are facts about this runner, not about the machine. A live
    /// item nobody displaced becomes a debt here too, because its work may
    /// still be running and the next answer must not start beside it.
    pub(crate) fn end_unconfirmed(
        &mut self,
        request: &RequestIdentity,
        reason: UnconfirmedReason,
        at_unix_ms: u64,
    ) -> bool {
        let unconfirmed = Confirmation::Unconfirmed {
            since_unix_ms: at_unix_ms,
            reason,
        };
        let mut touched = Vec::new();
        for (key, state) in &mut self.items {
            if state
                .live
                .as_ref()
                .is_some_and(|live| &live.request == request)
            {
                let live = state.live.take().expect("the live item was just matched");
                let was_placed = live.placement.is_some();
                if was_placed {
                    state.retiring.push(RetiringItem::from_live(
                        live,
                        RetirementCause::Unanswered,
                        unconfirmed.clone(),
                        at_unix_ms,
                    ));
                }
                touched.push((key.clone(), was_placed));
                continue;
            }
            let mut was_placed = false;
            let mut matched = false;
            state.retiring.retain_mut(|item| {
                if &item.request != request {
                    return true;
                }
                matched = true;
                if item.placement.is_none() {
                    return false;
                }
                was_placed = true;
                item.confirmation = unconfirmed.clone();
                true
            });
            if matched {
                touched.push((key.clone(), was_placed));
            }
        }
        for (key, was_placed) in &touched {
            self.journal_item(key, request, *was_placed);
        }
        self.prune();
        !touched.is_empty()
    }

    /// Release the key an item held, because it finished or was retired.
    ///
    /// Fenced by identity: an item that was already displaced must not clear the
    /// entry belonging to the item that displaced it. Without that fence a
    /// late-finishing predecessor would free a key its successor is still
    /// holding, and the next arrival would dispatch beside live work — the
    /// original defect, rebuilt inside its own fix.
    /// This is the only way an item leaves the registry, and it is driven by the
    /// item's terminal outcome rather than by the decision to stop it. A
    /// displaced item clears its own debt here, which is what releases whichever
    /// successor is waiting on it.
    pub(crate) fn finish(&mut self, request: &RequestIdentity) -> bool {
        let was_placed = self.placed(request);
        let mut cleared_keys = Vec::new();
        for (key, state) in &mut self.items {
            let mut cleared = false;
            if state
                .live
                .as_ref()
                .is_some_and(|live| &live.request == request)
            {
                state.live = None;
                cleared = true;
            }
            let before = state.retiring.len();
            state.retiring.retain(|item| &item.request != request);
            cleared |= state.retiring.len() != before;
            if cleared {
                cleared_keys.push(key.clone());
            }
        }
        if was_placed {
            for key in &cleared_keys {
                self.journal_removed(key, request);
            }
        }
        self.prune();
        !cleared_keys.is_empty()
    }

    /// Settle the debts bound to `executor_id` that `evidence` accepts, and
    /// report which.
    ///
    /// The entry point for what a machine says, or what the runner knows, about
    /// that machine as a whole: its own account of its work, or its being gone
    /// by the runner's authority. The caller decides which placements its
    /// evidence covers; a debt that names no machine is never touched here.
    pub(crate) fn settle_on_machine(
        &mut self,
        executor_id: &str,
        evidence: impl Fn(&RequestIdentity, &DebtPlacement) -> bool,
    ) -> Vec<PurposeDebtView> {
        let mut settled = Vec::new();
        for (key, state) in &mut self.items {
            state.retiring.retain(|item| {
                let covered = item.placement.as_ref().is_some_and(|placement| {
                    placement.executor_id == executor_id && evidence(&item.request, placement)
                });
                if covered {
                    settled.push(item.view(key));
                }
                !covered
            });
        }
        for debt in &settled {
            self.journal_removed(&debt.key, &debt.request);
        }
        self.prune();
        settled
    }

    /// Read one machine's first account in this process against the debts
    /// reloaded for it at boot.
    ///
    /// A debt the account does not list (as granted, executing, or queued) is
    /// not running there and settles. One it still lists is running, so the
    /// caller sends it `Cancel`; the debt stays, rebased onto `generation`, so
    /// the machine's later accounts can settle it the way they settle any
    /// other debt on a live connection.
    pub(crate) fn post_boot_account(
        &mut self,
        executor_id: &str,
        generation: u64,
        lists: impl Fn(&RequestIdentity) -> bool,
    ) -> PostBootAccount {
        let mut account = PostBootAccount::default();
        let mut rebased = Vec::new();
        for (key, state) in &mut self.items {
            state.retiring.retain_mut(|item| {
                let Some(placement) = item.placement.as_mut().filter(|placement| {
                    placement.executor_id == executor_id && placement.awaiting_post_boot_account
                }) else {
                    return true;
                };
                if lists(&item.request) {
                    placement.awaiting_post_boot_account = false;
                    placement.generation = generation;
                    account.still_running.push(item.request.clone());
                    rebased.push((key.clone(), item.request.clone()));
                    true
                } else {
                    account.settled.push(item.view(key));
                    false
                }
            });
        }
        for debt in &account.settled {
            self.journal_removed(&debt.key, &debt.request);
        }
        for (key, request) in &rebased {
            self.journal_item(key, request, true);
        }
        self.prune();
        account
    }

    /// Settle the one debt an operator named, because they authorized the
    /// overlap it was preventing.
    ///
    /// Exact by construction: the question, the item, and the machine must
    /// all match one outstanding debt (`executor_id` is `None` only for a debt
    /// that was never placed). Anything looser would be a generic override,
    /// and a refusal lists what does exist so the operator can name it.
    pub(crate) fn authorize_overlap(
        &mut self,
        key: &PurposeKey,
        request: &RequestIdentity,
        executor_id: Option<&str>,
    ) -> Result<PurposeDebtView, String> {
        let outstanding = self.debts_for(key);
        let matches = |debt: &PurposeDebtView| {
            &debt.request == request
                && debt
                    .placement
                    .as_ref()
                    .map(|placement| placement.executor_id.as_str())
                    == executor_id
        };
        let Some(debt) = outstanding.iter().find(|debt| matches(debt)).cloned() else {
            let listed = if outstanding.is_empty() {
                "it owes none".to_string()
            } else {
                outstanding
                    .iter()
                    .map(|debt| {
                        format!(
                            "request {} attempt {} on {}",
                            debt.request.0,
                            debt.request.1,
                            debt.placement
                                .as_ref()
                                .map_or("no machine", |placement| placement.executor_id.as_str())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("; ")
            };
            return Err(format!(
                "purpose {key} owes no debt for request {} attempt {} on {}; its outstanding debts: {listed}",
                request.0,
                request.1,
                executor_id.unwrap_or("no machine"),
            ));
        };
        if let Some(state) = self.items.get_mut(key) {
            state.retiring.retain(|item| &item.request != request);
        }
        if debt.placement.is_some() {
            self.journal_removed(key, request);
        }
        self.prune();
        Ok(debt)
    }

    /// Adopt the journaled items a previous runner process left, at boot.
    ///
    /// Every row comes back as a debt, whatever it was when written: a live
    /// item and a displaced one were both on a machine when that process
    /// stopped, and neither has a waiter in this one to hear its outcome. Each
    /// is unconfirmed because of the restart, and awaits the machine's first
    /// account read against it in this process.
    ///
    /// A row naming a machine lost since [`Self::expect_reload`] is settled
    /// instead, and returned.
    pub(crate) fn reload(
        &mut self,
        rows: Vec<PurposeDebtRow>,
        boot_unix_ms: u64,
    ) -> Vec<PurposeDebtView> {
        let lost = self.lost_before_reload.take().unwrap_or_default();
        let mut settled = Vec::new();
        for row in rows {
            let key = row.key;
            if self.items.get(&key).is_some_and(|state| {
                state
                    .retiring
                    .iter()
                    .any(|item| item.request == row.request)
            }) {
                continue;
            }
            let item = RetiringItem {
                request: row.request,
                phase: row.phase,
                placement: Some(DebtPlacement {
                    awaiting_post_boot_account: true,
                    ..row.placement
                }),
                cause: match row.state {
                    DebtRowState::Live => RetirementCause::Reloaded,
                    DebtRowState::Retiring => row.cause.unwrap_or(RetirementCause::Reloaded),
                },
                confirmation: Confirmation::Unconfirmed {
                    since_unix_ms: boot_unix_ms,
                    reason: UnconfirmedReason::RunnerRestarted {
                        at_unix_ms: boot_unix_ms,
                    },
                },
                retired_at_unix_ms: row.retired_at_unix_ms.unwrap_or(boot_unix_ms),
            };
            if item
                .placement
                .as_ref()
                .is_some_and(|placement| lost.contains(&placement.executor_id))
            {
                self.journal_removed(&key, &item.request);
                settled.push(item.view(&key));
                continue;
            }
            self.items.entry(key).or_default().retiring.push(item);
        }
        settled
    }

    /// The machines that hold debts reloaded at boot and not yet read against
    /// an account from this process.
    #[cfg(test)]
    fn machines_awaiting_post_boot_account(&self) -> Vec<String> {
        let mut machines: Vec<String> = self
            .items
            .values()
            .flat_map(|state| state.retiring.iter())
            .filter_map(|item| item.placement.as_ref())
            .filter(|placement| placement.awaiting_post_boot_account)
            .map(|placement| placement.executor_id.clone())
            .collect();
        machines.sort();
        machines.dedup();
        machines
    }

    #[cfg(test)]
    fn live(&self, key: &PurposeKey) -> Option<&RequestIdentity> {
        self.items
            .get(key)
            .and_then(|state| state.live.as_ref())
            .map(|item| &item.request)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.items.len()
    }
}

// ---------------------------------------------------------------------------
// The durable journal of placed items.
// ---------------------------------------------------------------------------

/// Every journaled row, for the boot reload.
pub(crate) async fn load_rows(db: &LocalDb) -> Result<Vec<PurposeDebtRow>, String> {
    let rows = db
        .query_all(
            "SELECT purpose_key, request_id, attempt_id, state, phase, executor_id, generation,
                    dispatched_at, cause_json, confirmation_json, retired_at
               FROM purpose_debts",
            (),
            |row| {
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.text(2)?,
                    row.text(3)?,
                    row.text(4)?,
                    row.text(5)?,
                    row.i64(6)?,
                    row.i64(7)?,
                    row.opt_text(8)?,
                    row.opt_text(9)?,
                    row.opt_i64(10)?,
                ))
            },
        )
        .await
        .map_err(|error| format!("load purpose debts: {error}"))?;
    let mut parsed = Vec::with_capacity(rows.len());
    for (
        key,
        request_id,
        attempt_id,
        state,
        phase,
        executor_id,
        generation,
        dispatched_at,
        cause,
        confirmation,
        retired_at,
    ) in rows
    {
        // A row this build cannot read is still a machine that may be running
        // something, so it reloads with the parts that did parse rather than
        // being dropped.
        let state = if state == "live" {
            DebtRowState::Live
        } else {
            DebtRowState::Retiring
        };
        parsed.push(PurposeDebtRow {
            key: PurposeKey::from_persisted(key),
            request: (request_id, attempt_id),
            state,
            phase: ItemPhase::parse(&phase).unwrap_or(ItemPhase::Dispatched),
            placement: DebtPlacement {
                executor_id,
                generation: u64::try_from(generation).unwrap_or_default(),
                dispatched_at_unix_ms: u64::try_from(dispatched_at).unwrap_or_default(),
                awaiting_post_boot_account: false,
            },
            cause: cause.and_then(|cause| serde_json::from_str(&cause).ok()),
            confirmation: confirmation
                .and_then(|confirmation| serde_json::from_str(&confirmation).ok()),
            retired_at_unix_ms: retired_at.and_then(|at| u64::try_from(at).ok()),
        });
    }
    Ok(parsed)
}

/// Apply one batch of changes, in order, as one transaction.
pub(crate) async fn apply_changes(
    db: &LocalDb,
    changes: Vec<DebtChange>,
    now_unix_ms: u64,
) -> Result<(), String> {
    db.write(move |conn| {
        let changes = changes.clone();
        Box::pin(async move {
            for change in &changes {
                match change {
                    DebtChange::Upsert(row) => {
                        conn.execute(
                            "INSERT OR REPLACE INTO purpose_debts(
                                 purpose_key, request_id, attempt_id, state, phase, executor_id,
                                 generation, dispatched_at, cause_json, confirmation_json,
                                 retired_at, updated_at)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                            (
                                row.key.as_str(),
                                row.request.0.as_str(),
                                row.request.1.as_str(),
                                row.state.as_str(),
                                row.phase.as_str(),
                                row.placement.executor_id.as_str(),
                                row.placement.generation as i64,
                                row.placement.dispatched_at_unix_ms as i64,
                                row.cause
                                    .as_ref()
                                    .and_then(|cause| serde_json::to_string(cause).ok()),
                                row.confirmation.as_ref().and_then(|confirmation| {
                                    serde_json::to_string(confirmation).ok()
                                }),
                                row.retired_at_unix_ms.map(|at| at as i64),
                                now_unix_ms as i64,
                            ),
                        )
                        .await?;
                    }
                    DebtChange::Delete { key, request } => {
                        conn.execute(
                            "DELETE FROM purpose_debts
                              WHERE purpose_key = ?1 AND request_id = ?2 AND attempt_id = ?3",
                            (key.as_str(), request.0.as_str(), request.1.as_str()),
                        )
                        .await?;
                    }
                }
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("purpose debt journal write failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(id: &str) -> RequestIdentity {
        (id.to_string(), format!("{id}-attempt"))
    }

    /// The property the operator was watching fail: a burst of merges advancing
    /// one project leaves one attestation, not one per tip.
    #[test]
    fn a_newer_tip_replaces_the_attestation_rather_than_joining_it() {
        let mut registry = PurposeRegistry::default();
        let key = PurposeKey::main_cadence("project", "rust-tests", "linux");

        assert_eq!(
            registry.admit(key.clone(), request("tip-x"), 10),
            Admission::Admitted
        );
        assert_eq!(
            registry.admit(key.clone(), request("tip-x1"), 20),
            Admission::Replaced {
                retire: request("tip-x"),
                phase: ItemPhase::Preparing,
            }
        );

        assert_eq!(registry.len(), 1);
        assert_eq!(registry.live(&key), Some(&request("tip-x1")));
    }

    /// Replacement is not a property of how far the old item got. Whatever phase
    /// it is in, the same owner retires it: there is no phase too late to
    /// displace, and none too early to have been dispatched.
    #[test]
    fn replacement_reports_the_phase_the_displaced_item_had_reached() {
        for phase in [ItemPhase::Preparing, ItemPhase::Dispatched] {
            let mut registry = PurposeRegistry::default();
            let key = PurposeKey::main_cadence("project", "rust-tests", "linux");
            registry.admit(key.clone(), request("old"), 10);
            if phase == ItemPhase::Dispatched {
                assert!(registry.dispatched(&request("old"), "machine", 1, 12));
            }

            assert_eq!(
                registry.admit(key, request("new"), 20),
                Admission::Replaced {
                    retire: request("old"),
                    phase,
                },
                "a {phase:?} item is displaced by the same transition as any other"
            );
        }
    }

    /// Newest-wins is decided by the registry, not by which item happens to
    /// reach an executor first. An item that lost arrives to find the key held
    /// and must not dispatch at all, so it never takes an occupancy row.
    #[test]
    fn an_item_older_than_the_live_one_is_refused_rather_than_dispatched() {
        let mut registry = PurposeRegistry::default();
        let key = PurposeKey::main_cadence("project", "rust-tests", "linux");
        registry.admit(key.clone(), request("newer"), 50);

        assert_eq!(
            registry.admit(key.clone(), request("older"), 20),
            Admission::Superseded {
                holder: request("newer")
            }
        );
        assert_eq!(registry.live(&key), Some(&request("newer")));
    }

    /// Equal arming instants are not an ordering. Letting a tie replace would
    /// let two items armed in one millisecond displace each other in turn.
    #[test]
    fn a_tie_leaves_the_incumbent_in_place() {
        let mut registry = PurposeRegistry::default();
        let key = PurposeKey::main_cadence("project", "rust-tests", "linux");
        registry.admit(key.clone(), request("first"), 10);

        assert_eq!(
            registry.admit(key.clone(), request("second"), 10),
            Admission::Superseded {
                holder: request("first")
            }
        );
        assert_eq!(registry.live(&key), Some(&request("first")));
    }

    /// Re-admitting the item that already holds a key is not a replacement, and
    /// must not ask the caller to retire the very item it is admitting.
    #[test]
    fn readmitting_the_live_item_is_not_a_replacement() {
        let mut registry = PurposeRegistry::default();
        let key = PurposeKey::main_cadence("project", "rust-tests", "linux");
        registry.admit(key.clone(), request("item"), 10);

        assert_eq!(
            registry.admit(key.clone(), request("item"), 30),
            Admission::Admitted
        );
        assert_eq!(registry.len(), 1);
    }

    /// A replaced item finishing late clears its own debt and nothing else. It
    /// must not disturb the entry its replacement now holds, because a stale
    /// completion that freed the key would let a third item dispatch beside
    /// live work.
    #[test]
    fn a_displaced_item_finishing_late_clears_only_its_own_debt() {
        let mut registry = PurposeRegistry::default();
        let key = PurposeKey::main_cadence("project", "rust-tests", "linux");
        registry.admit(key.clone(), request("old"), 10);
        registry.admit(key.clone(), request("new"), 20);

        assert!(registry.finish(&request("old")));
        assert_eq!(registry.live(&key), Some(&request("new")));
        assert_eq!(registry.pending_retirements(&key), 0);

        assert!(registry.finish(&request("new")));
        assert_eq!(registry.len(), 0);
    }

    /// The property the whole gate rests on: displacing an item does not make it
    /// gone. Until its terminal outcome lands the question still owes a death,
    /// and the successor that displaced it is not free to dispatch.
    #[test]
    fn a_displaced_item_is_owed_a_death_before_its_successor_may_run() {
        let mut registry = PurposeRegistry::default();
        let key = PurposeKey::main_cadence("project", "rust-tests", "linux");

        registry.admit(key.clone(), request("tip-x"), 10);
        assert_eq!(registry.pending_retirements(&key), 0);

        registry.admit(key.clone(), request("tip-x1"), 20);
        assert_eq!(
            registry.pending_retirements(&key),
            1,
            "the displaced item is owed a death, not assumed dead"
        );
        let debts = registry.debts_for(&key);
        assert_eq!(debts.len(), 1);
        assert_eq!(debts[0].request, request("tip-x"));
        assert_eq!(debts[0].retired_at_unix_ms, 20);

        registry.finish(&request("tip-x"));
        assert_eq!(registry.pending_retirements(&key), 0);
        assert_eq!(registry.live(&key), Some(&request("tip-x1")));
    }

    /// Two displacements in quick succession owe two deaths. Nothing may run for
    /// this question until both have landed, or a burst of merges would sneak a
    /// third answer in beside the first one that had not finished dying.
    #[test]
    fn every_displaced_item_is_owed_its_own_death() {
        let mut registry = PurposeRegistry::default();
        let key = PurposeKey::main_cadence("project", "rust-tests", "linux");

        registry.admit(key.clone(), request("tip-x"), 10);
        registry.admit(key.clone(), request("tip-x1"), 20);
        registry.admit(key.clone(), request("tip-x2"), 30);
        assert_eq!(registry.pending_retirements(&key), 2);

        registry.finish(&request("tip-x1"));
        assert_eq!(registry.pending_retirements(&key), 1);
        registry.finish(&request("tip-x"));
        assert_eq!(registry.pending_retirements(&key), 0);
    }

    /// The requester-facing path owes a death too. Cancelling the live item
    /// leaves no live answer, but the question is not free: a new one arriving
    /// now would start beside work that is still stopping.
    #[test]
    fn cancelling_the_live_item_leaves_the_question_owing_a_death() {
        let mut registry = PurposeRegistry::default();
        let key = PurposeKey::main_cadence("project", "rust-tests", "linux");
        registry.admit(key.clone(), request("item"), 10);

        assert!(registry.retire(&request("item"), 15));
        assert_eq!(registry.live(&key), None);
        assert_eq!(
            registry.pending_retirements(&key),
            1,
            "a cancelled item is stopping, not stopped"
        );

        registry.finish(&request("item"));
        assert_eq!(registry.pending_retirements(&key), 0);
        assert_eq!(registry.len(), 0, "the question is free only once it is");
    }

    /// A question whose live answer finished while a predecessor was still dying
    /// is still not free. Pruning on the live item alone would drop the debt and
    /// let the next arrival dispatch beside the survivor.
    #[test]
    fn a_finished_answer_does_not_free_a_question_that_still_owes_a_death() {
        let mut registry = PurposeRegistry::default();
        let key = PurposeKey::main_cadence("project", "rust-tests", "linux");
        registry.admit(key.clone(), request("old"), 10);
        registry.admit(key.clone(), request("new"), 20);

        registry.finish(&request("new"));
        assert_eq!(registry.live(&key), None);
        assert_eq!(registry.pending_retirements(&key), 1);
        assert_eq!(registry.len(), 1);

        registry.finish(&request("old"));
        assert_eq!(registry.len(), 0);
    }

    /// One project's attestation says nothing about another's.
    #[test]
    fn questions_about_different_projects_are_different_questions() {
        let mut registry = PurposeRegistry::default();
        registry.admit(
            PurposeKey::main_cadence("one", "rust-tests", "linux"),
            request("a"),
            10,
        );
        assert_eq!(
            registry.admit(
                PurposeKey::main_cadence("two", "rust-tests", "linux"),
                request("b"),
                10
            ),
            Admission::Admitted
        );
        assert_eq!(registry.len(), 2);
    }

    fn key() -> PurposeKey {
        PurposeKey::main_cadence("project", "rust-tests", "linux")
    }

    fn journaled() -> (PurposeRegistry, mpsc::UnboundedReceiver<DebtChange>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut registry = PurposeRegistry::default();
        registry.attach_journal(sender);
        (registry, receiver)
    }

    fn drain(receiver: &mut mpsc::UnboundedReceiver<DebtChange>) -> Vec<DebtChange> {
        let mut changes = Vec::new();
        while let Ok(change) = receiver.try_recv() {
            changes.push(change);
        }
        changes
    }

    /// A debt has to be able to say what blocks the question: where the item
    /// had got to, which machine has it, and why it is no longer the answer.
    #[test]
    fn a_debt_records_its_phase_its_machine_and_what_displaced_it() {
        let mut registry = PurposeRegistry::default();
        registry.admit(key(), request("old"), 10);
        registry.dispatched(&request("old"), "machine-a", 3, 11);
        registry.admit(key(), request("new"), 20);

        let debts = registry.debts_for(&key());
        assert_eq!(debts.len(), 1);
        let debt = &debts[0];
        assert_eq!(debt.phase, ItemPhase::Dispatched);
        let placement = debt.placement.as_ref().expect("the debt names its machine");
        assert_eq!(placement.executor_id, "machine-a");
        assert_eq!(placement.generation, 3);
        assert_eq!(
            debt.cause,
            RetirementCause::Displaced { by: request("new") }
        );
        assert_eq!(debt.confirmation, Confirmation::Awaiting);

        registry.admit(key(), request("newer"), 30);
        registry.retire(&request("newer"), 31);
        let retracted = registry
            .debts_for(&key())
            .into_iter()
            .find(|debt| debt.request == request("newer"))
            .unwrap();
        assert_eq!(retracted.phase, ItemPhase::Preparing);
        assert_eq!(retracted.placement, None);
        assert_eq!(retracted.cause, RetirementCause::Retracted);
    }

    /// Retirement landing between the last pre-grant check and the grant
    /// still leaves a debt that names where the grant went.
    #[test]
    fn a_grant_to_an_already_retired_item_is_recorded_on_its_debt() {
        let mut registry = PurposeRegistry::default();
        registry.admit(key(), request("old"), 10);
        registry.admit(key(), request("new"), 20);
        assert!(registry.dispatched(&request("old"), "machine-a", 1, 21));
        assert_eq!(
            registry.debts_for(&key())[0]
                .placement
                .as_ref()
                .map(|placement| placement.executor_id.as_str()),
            Some("machine-a")
        );
    }

    /// An attempt the runner gave up on proves nothing about a machine it was
    /// never sent to, and nothing either way about one it was.
    #[test]
    fn ending_unconfirmed_clears_an_unplaced_item_and_keeps_a_placed_one() {
        let mut registry = PurposeRegistry::default();
        registry.admit(key(), request("old"), 10);
        registry.admit(key(), request("new"), 20);
        assert!(registry.end_unconfirmed(&request("old"), UnconfirmedReason::LeaderVanished, 25));
        assert_eq!(
            registry.pending_retirements(&key()),
            0,
            "nothing was running for an item that never reached a machine"
        );

        registry.dispatched(&request("new"), "machine-a", 2, 26);
        registry.admit(key(), request("newest"), 30);
        registry.end_unconfirmed(
            &request("new"),
            UnconfirmedReason::GrantAbandoned {
                detail: "silent".into(),
            },
            31,
        );
        let debts = registry.debts_for(&key());
        assert_eq!(debts.len(), 1);
        assert_eq!(
            debts[0].confirmation,
            Confirmation::Unconfirmed {
                since_unix_ms: 31,
                reason: UnconfirmedReason::GrantAbandoned {
                    detail: "silent".into()
                },
            }
        );
    }

    /// A live answer whose attempt the runner abandoned is not finished just
    /// because nobody displaced it. Its work may still be running, and the
    /// next answer to arrive must wait for it.
    #[test]
    fn an_abandoned_live_answer_becomes_a_debt() {
        let mut registry = PurposeRegistry::default();
        registry.admit(key(), request("only"), 10);
        registry.dispatched(&request("only"), "machine-a", 1, 11);
        registry.end_unconfirmed(&request("only"), UnconfirmedReason::LeaderVanished, 20);

        assert_eq!(registry.live(&key()), None);
        let debts = registry.debts_for(&key());
        assert_eq!(debts.len(), 1);
        assert_eq!(debts[0].cause, RetirementCause::Unanswered);
        assert_eq!(
            registry.admit(key(), request("next"), 30),
            Admission::Admitted
        );
        assert_eq!(registry.pending_retirements(&key()), 1);
    }

    /// Evidence about one machine settles only the debts that machine holds.
    #[test]
    fn machine_evidence_settles_only_that_machines_debts() {
        let mut registry = PurposeRegistry::default();
        let other = PurposeKey::main_cadence("project", "rust-tests", "macos");
        registry.admit(key(), request("a"), 10);
        registry.dispatched(&request("a"), "machine-a", 1, 11);
        registry.admit(key(), request("a2"), 20);
        registry.admit(other.clone(), request("b"), 10);
        registry.dispatched(&request("b"), "machine-b", 1, 11);
        registry.admit(other.clone(), request("b2"), 20);
        registry.admit(key(), request("a3"), 30);

        let settled = registry.settle_on_machine("machine-a", |_, _| true);
        assert_eq!(settled.len(), 1);
        assert_eq!(settled[0].request, request("a"));
        assert_eq!(
            registry.pending_retirements(&key()),
            1,
            "the never-placed debt is not this machine's to settle"
        );
        assert_eq!(registry.pending_retirements(&other), 1);
    }

    /// The escape is exact. Naming the wrong item or the wrong machine is
    /// refused with what is actually owed, never treated as a generic
    /// override.
    #[test]
    fn authorizing_an_overlap_requires_the_exact_item_and_machine() {
        let mut registry = PurposeRegistry::default();
        registry.admit(key(), request("old"), 10);
        registry.dispatched(&request("old"), "machine-a", 1, 11);
        registry.admit(key(), request("new"), 20);

        let wrong_machine = registry
            .authorize_overlap(&key(), &request("old"), Some("machine-b"))
            .unwrap_err();
        assert!(
            wrong_machine.contains("old-attempt on machine-a"),
            "{wrong_machine}"
        );
        assert!(registry
            .authorize_overlap(&key(), &request("other"), Some("machine-a"))
            .is_err());
        assert!(registry
            .authorize_overlap(&key(), &request("old"), None)
            .is_err());
        assert_eq!(registry.pending_retirements(&key()), 1);

        let settled = registry
            .authorize_overlap(&key(), &request("old"), Some("machine-a"))
            .unwrap();
        assert_eq!(settled.request, request("old"));
        assert_eq!(registry.pending_retirements(&key()), 0);
    }

    /// Only items that reached a machine are journaled: from the grant until
    /// whatever settles them. Runner-owned work ends with the runner and
    /// needs no row.
    #[test]
    fn the_journal_follows_placed_items_from_the_grant_to_settlement() {
        let (mut registry, mut journal) = journaled();
        registry.admit(key(), request("unplaced"), 5);
        registry.finish(&request("unplaced"));
        assert_eq!(drain(&mut journal), Vec::new());

        registry.admit(key(), request("old"), 10);
        assert_eq!(drain(&mut journal), Vec::new());
        registry.dispatched(&request("old"), "machine-a", 1, 11);
        let changes = drain(&mut journal);
        assert!(
            matches!(&changes[..], [DebtChange::Upsert(row)] if row.state == DebtRowState::Live),
            "{changes:?}"
        );
        registry.admit(key(), request("new"), 20);
        let changes = drain(&mut journal);
        assert!(
            matches!(&changes[..], [DebtChange::Upsert(row)]
                if row.state == DebtRowState::Retiring
                    && row.cause == Some(RetirementCause::Displaced { by: request("new") })),
            "{changes:?}"
        );
        registry.finish(&request("old"));
        assert_eq!(
            drain(&mut journal),
            vec![DebtChange::Delete {
                key: key(),
                request: request("old")
            }]
        );

        for settle in 0..3 {
            let item = request(&format!("settled-{settle}"));
            registry.admit(key(), item.clone(), 100 + settle);
            registry.dispatched(&item, "machine-a", 1, 100 + settle);
            registry.end_unconfirmed(&item, UnconfirmedReason::LeaderVanished, 100 + settle);
            drain(&mut journal);
            match settle {
                0 => {
                    registry.settle_on_machine("machine-a", |_, _| true);
                }
                1 => {
                    registry
                        .authorize_overlap(&key(), &item, Some("machine-a"))
                        .unwrap();
                }
                _ => {
                    registry.finish(&item);
                }
            }
            assert_eq!(
                drain(&mut journal),
                vec![DebtChange::Delete {
                    key: key(),
                    request: item
                }]
            );
        }
        // `request("new")` was displaced before it was placed: nothing to journal.
        registry.finish(&request("new"));
        assert_eq!(drain(&mut journal), Vec::new());
    }

    fn rows_of(changes: Vec<DebtChange>) -> Vec<PurposeDebtRow> {
        let mut rows: Vec<PurposeDebtRow> = Vec::new();
        for change in changes {
            match change {
                DebtChange::Upsert(row) => {
                    rows.retain(|held| held.request != row.request);
                    rows.push(row);
                }
                DebtChange::Delete { request, .. } => rows.retain(|held| held.request != request),
            }
        }
        rows
    }

    /// A runner restart does not end work on a remote machine, so it must not
    /// end the debt either. The reloaded debt names its machine and the
    /// restart, blocks the question, and is settled only by that machine's
    /// account from the new process.
    #[test]
    fn a_restart_reloads_a_dispatched_debt_that_only_the_machine_can_settle() {
        let (mut before, mut journal) = journaled();
        before.admit(key(), request("gone"), 10);
        before.dispatched(&request("gone"), "machine-a", 7, 11);
        before.admit(key(), request("running"), 20);
        before.dispatched(&request("running"), "machine-a", 7, 21);
        let rows = rows_of(drain(&mut journal));
        drop(before);
        assert_eq!(rows.len(), 2);

        let mut after = PurposeRegistry::default();
        after.reload(rows, 1_000);
        let debts = after.debts_for(&key());
        assert_eq!(debts.len(), 2);
        for debt in &debts {
            assert_eq!(
                debt.confirmation,
                Confirmation::Unconfirmed {
                    since_unix_ms: 1_000,
                    reason: UnconfirmedReason::RunnerRestarted { at_unix_ms: 1_000 },
                }
            );
            assert_eq!(
                debt.placement.as_ref().unwrap().executor_id,
                "machine-a",
                "a reloaded debt still names its machine"
            );
        }
        assert_eq!(
            debts
                .iter()
                .find(|debt| debt.request == request("running"))
                .unwrap()
                .cause,
            RetirementCause::Reloaded
        );
        assert_eq!(
            after.admit(key(), request("successor"), 2_000),
            Admission::Admitted
        );
        assert_eq!(
            after.pending_retirements(&key()),
            2,
            "the question is still owed"
        );
        assert_eq!(
            after.machines_awaiting_post_boot_account(),
            vec!["machine-a".to_string()]
        );

        // Generation 1 of a new process: the number means nothing against the
        // recorded 7, and the account is what matters.
        let account = after.post_boot_account("machine-a", 1, |item| item == &request("running"));
        assert_eq!(account.settled.len(), 1);
        assert_eq!(account.settled[0].request, request("gone"));
        assert_eq!(account.still_running, vec![request("running")]);
        assert_eq!(after.pending_retirements(&key()), 1);
        assert!(after.machines_awaiting_post_boot_account().is_empty());

        // The survivor was rebased onto this process's connection, so the
        // machine's later word about it is ordinary evidence.
        let settled = after.settle_on_machine("machine-a", |item, placement| {
            item == &request("running") && placement.generation == 1
        });
        assert_eq!(settled.len(), 1);
        assert_eq!(after.pending_retirements(&key()), 0);
    }

    /// A machine lost while the journal is still being read is evidence for
    /// the rows that name it, even though they arrive after it. Once the
    /// reload is done, nothing more is remembered.
    #[test]
    fn a_machine_lost_during_the_reload_settles_the_rows_that_name_it() {
        let (mut registry, mut journal) = journaled();
        registry.expect_reload();
        assert!(registry.machine_lost("machine-a").is_empty());
        let row = |id: &str, machine: &str| PurposeDebtRow {
            key: key(),
            request: request(id),
            state: DebtRowState::Live,
            phase: ItemPhase::Dispatched,
            placement: DebtPlacement {
                executor_id: machine.into(),
                generation: 3,
                dispatched_at_unix_ms: 5,
                awaiting_post_boot_account: false,
            },
            cause: None,
            confirmation: None,
            retired_at_unix_ms: None,
        };

        let settled = registry.reload(
            vec![row("gone", "machine-a"), row("owed", "machine-b")],
            1_000,
        );

        assert_eq!(settled.len(), 1);
        assert_eq!(settled[0].request, request("gone"));
        let debts = registry.debts_for(&key());
        assert_eq!(debts.len(), 1);
        assert_eq!(debts[0].request, request("owed"));
        assert!(drain(&mut journal).iter().any(|change| matches!(
            change,
            DebtChange::Delete { request: deleted, .. } if *deleted == request("gone")
        )));
        registry.machine_lost("machine-c");
        assert!(registry.lost_before_reload.is_none());
    }

    /// What the registry journals is what a restart reads back: upserts and
    /// deletes land in order, and the reload sees exactly the outstanding
    /// rows, with their causes and confirmations intact.
    #[tokio::test]
    async fn the_journal_round_trips_through_the_database() {
        let db = crate::storage::migrated_test_db("purpose_debt_journal.db").await;
        let (mut registry, mut journal) = journaled();
        registry.admit(key(), request("settled"), 10);
        registry.dispatched(&request("settled"), "machine-a", 1, 11);
        registry.admit(key(), request("owed"), 20);
        registry.dispatched(&request("owed"), "machine-b", 4, 21);
        registry.admit(key(), request("live"), 30);
        registry.finish(&request("settled"));
        registry.end_unconfirmed(
            &request("owed"),
            UnconfirmedReason::GrantAbandoned {
                detail: "silent".into(),
            },
            32,
        );
        apply_changes(&db, drain(&mut journal), 40).await.unwrap();

        let rows = load_rows(&db).await.unwrap();
        assert_eq!(rows.len(), 1, "{rows:?}");
        let row = &rows[0];
        assert_eq!(row.request, request("owed"));
        assert_eq!(row.state, DebtRowState::Retiring);
        assert_eq!(row.phase, ItemPhase::Dispatched);
        assert_eq!(row.placement.executor_id, "machine-b");
        assert_eq!(row.placement.generation, 4);
        assert_eq!(
            row.cause,
            Some(RetirementCause::Displaced {
                by: request("live")
            })
        );
        assert_eq!(
            row.confirmation,
            Some(Confirmation::Unconfirmed {
                since_unix_ms: 32,
                reason: UnconfirmedReason::GrantAbandoned {
                    detail: "silent".into()
                },
            })
        );

        let mut reloaded = PurposeRegistry::default();
        reloaded.reload(rows, 1_000);
        assert_eq!(reloaded.pending_retirements(&key()), 1);
    }

    /// The key names the question. If a revision ever leaks into it, every tip
    /// becomes its own question and replacement stops happening at all — the
    /// exact behavior this module was written to end.
    #[test]
    fn a_purpose_key_names_the_question_and_not_the_answer() {
        let key = PurposeKey::main_cadence("project", "rust-tests", "linux");
        assert_eq!(key.to_string(), "main-cadence:project:rust-tests:linux");
        assert_eq!(
            key,
            PurposeKey::main_cadence("project", "rust-tests", "linux"),
            "the same question asked twice is one key"
        );
    }
}
