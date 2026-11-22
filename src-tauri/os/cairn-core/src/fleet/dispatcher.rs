//! The room-driven dispatcher (cairn/7303): machines announce their room, and
//! the waiting work that fits it is handed to them.
//!
//! This replaced a per-request placement loop that re-surveyed and re-ranked
//! every machine for every waiting request four times a second and on every
//! status post from any machine. Its cost was the number of waiting requests
//! times the posting rate, however cheap each pass was made. Here nothing
//! runs while nothing changes: each mutator below is called from the event
//! that changed something, and matches only the (entry, machine) pairs whose
//! inputs that event changed.
//!
//! - [`Dispatcher::set_room`]: one machine's room, walked against the entries
//!   that may run there, stopping once the room is spent.
//! - [`Dispatcher::enqueue`], [`Dispatcher::set_candidates`]: one entry,
//!   matched against the rooms already announced.
//! - [`Dispatcher::decline`]: the declined entry, against the other
//!   machines; and on the machine's first refusal since its last claim
//!   under its figures, that machine, for the room the offer took and gives
//!   back.
//! - [`Dispatcher::requeue`]: the entry, and the machine its offer went to,
//!   for the room the offer took and gives back.
//! - [`Dispatcher::withdraw`]: the machine its offer went to, for the room
//!   the offer took and gives back.
//! - [`Dispatcher::claimed`]: nothing is matched. The machine's next
//!   announcement carries what changed.
//! - [`Dispatcher::deadline_reached`]: the entries whose entitlement to hold
//!   room, or whose horizon, has arrived.
//!
//! Whether an entry may go to a machine is a pure function of four values,
//! all private to this type: the entry's need on that machine, whether the
//! machine is eligible for it, the machine's room less what was assigned since
//! that room was announced, and the room seq at which the machine last
//! refused the entry. Every mutator ends by matching the pairs whose inputs it
//! changed, so at rest no waiting entry fits a machine's room unless it was
//! refused there under that room, the room is short of it only by the shares
//! that machine kept after refusing twice under it, or the assignment-time
//! check (`admit`) turned it away there under that room. [`Dispatcher::stranded`]
//! checks that, and the tests check it after every event of random sequences,
//! with an `admit` that sometimes says no.
//!
//! Admission stays the machine's (tenet 834): the room is what the machine
//! announced, and the fit is [`fits_offer`], the function the machine's own
//! claim uses. The announced room is net of the machine's protected headroom
//! hold; an entry that passes that hold (its own, or an entitled older entry
//! the drained room fits: [`passes_protected_hold`]) is judged with the
//! hold's need given back, exactly as the claim judges it. A refusal is the
//! machine's word on a room that moved between its announcement and the
//! offer; it is paced by room seq, never interpreted.
//!
//! This type holds no lock, does no I/O and reads no clock: the caller passes
//! `now`, and `admit` is the caller's last look at a pair before it is
//! assigned (a link gone stale, a pipe floor), which costs one call per
//! assignment rather than one per waiting entry.

use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap, HashSet};

use cairn_common::executor_protocol::{
    fits, fits_offer, passes_protected_hold, queue_rank, AnnouncedRoom, HeadroomRoomHold, Need,
    Precedence, QueueRank, RoomShort,
};
use cairn_common::wire_vocabulary::CellPriority;

pub(crate) type MachineId = String;
pub(crate) type EntryId = String;

/// What one entry needs on one machine it may run on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Candidate {
    pub(crate) need: Need,
    /// The machine's warm roots hold the request's base commit: preferred
    /// among machines whose room fits (tenet 1395), never required.
    pub(crate) warm: bool,
    /// Where the machine stands among the entry's machines on what its
    /// memory can do for this work, before warmth or room.
    pub(crate) key: HardKey,
    /// Away for a reason that ends on its own (emptied for an executor
    /// update), or waiting for its figure at a changed slot width. Never
    /// offered the entry, and never asked to hold room for
    /// it; it can still keep a deferred or clamped entry off a machine its
    /// key ranks ahead of, until the entry's reserve-from instant.
    pub(crate) away: bool,
}

/// The memory-safety order of the machines an entry may run on: a machine
/// may take the entry only while no machine ahead of it on this key could
/// hold it ([`Dispatcher::blocker`]). Each part is a fact about the
/// machine's size against this work, worked out once when the entry's needs
/// are, never a reading of its load.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct HardKey {
    /// The demand is a class prior, not a measurement of this work, and the
    /// machine cannot give one process that much: its first run there would
    /// be the measurement, and a likely kill.
    pub(crate) deferred: bool,
    /// The work compiles Rust and no single process of it was ever measured:
    /// what the machine can give one process (`Some(None)` while its total is
    /// unread), larger first. `None` for any other work.
    pub(crate) unmeasured_capacity: Option<Option<u64>>,
    /// The measured demand is above what the machine will ever reserve, so
    /// its need there is the clamp, not the demand.
    pub(crate) clamped: bool,
}

impl HardKey {
    /// Smaller is better: undeferred first, then the most single-process
    /// capacity for an unmeasured compile, then unclamped.
    fn order(&self) -> (bool, Reverse<Option<Option<u64>>>, bool) {
        (
            self.deferred,
            Reverse(self.unmeasured_capacity),
            self.clamped,
        )
    }

    /// The machine would run the work at a size it was not measured to hold.
    /// Such a machine waits for a better one that is away or silent, too.
    fn disfavoured(&self) -> bool {
        self.deferred || self.clamped
    }
}

/// An entry joining the dispatcher.
#[derive(Debug, Clone)]
pub(crate) struct Waiting {
    pub(crate) entry_id: EntryId,
    /// The live answer to a standing question (a main-cadence lane): ahead of
    /// one-off work, and entitled to hold room at once (tenet 1444).
    pub(crate) standing: bool,
    pub(crate) priority: CellPriority,
    pub(crate) queued_at_unix_ms: u64,
    pub(crate) sequence: u64,
    pub(crate) horizon_unix_ms: u64,
    /// When this entry becomes entitled to hold room on a machine whose room
    /// it could fit once that machine's running work ends.
    pub(crate) reserve_from_unix_ms: u64,
    /// The machines this entry may run on, with its need on each. A machine
    /// missing here is one the entry can never be offered to.
    pub(crate) candidates: HashMap<MachineId, Candidate>,
    /// The caller left the machine to placement: neither pinned nor named,
    /// and free to move. Only such work waits for a better machine.
    pub(crate) may_spill: bool,
}

impl Waiting {
    /// Larger is earlier: standing work, then the executor's own order.
    fn rank(&self, now_unix_ms: u64) -> (bool, QueueRank) {
        (
            self.standing,
            queue_rank(
                self.priority,
                self.queued_at_unix_ms,
                self.sequence,
                now_unix_ms,
            ),
        )
    }

    fn entitled(&self, now_unix_ms: u64) -> bool {
        self.standing || now_unix_ms >= self.reserve_from_unix_ms
    }
}

/// An entry handed to a machine: the caller offers it there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Assignment {
    pub(crate) entry_id: EntryId,
    pub(crate) executor_id: MachineId,
    pub(crate) generation: u64,
    /// The room seq the assignment was made under.
    pub(crate) seq: u64,
    /// The entry fits no room, and this machine's room would fit it once its
    /// running work ended: the offer asks it to hold room (tenet 1444).
    pub(crate) reserve: bool,
    /// The offer carries the entry's headroom authorization: it asks to hold
    /// room, or goes to the machine already holding room for the entry. The
    /// machine judges an authorized offer's precedence over its protected
    /// hold by it, and keeps the entry's hold standing if it refuses.
    pub(crate) authorized: bool,
}

/// What a mutator decided, for the caller to carry out.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Effects {
    pub(crate) assigned: Vec<Assignment>,
    /// Entries whose horizon arrived while they waited.
    pub(crate) expired: Vec<EntryId>,
    /// Headroom an entry no longer needs on a machine: the caller sends
    /// `ReleaseHeadroom`.
    pub(crate) released: Vec<(EntryId, MachineId, u64)>,
}

impl Effects {
    pub(crate) fn absorb(&mut self, other: Effects) {
        self.assigned.extend(other.assigned);
        self.expired.extend(other.expired);
        self.released.extend(other.released);
    }
}

/// Work done, for [`super::counters::FleetCounters`]: what the old loop
/// counted as passes and fit evaluations.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Work {
    /// Matches run: one per walk of a machine, one per entry placed.
    pub(crate) passes: u64,
    pub(crate) fit_evaluations: u64,
}

#[derive(Debug, Clone)]
struct Announced {
    seq: u64,
    /// The seq of the frame that first announced these figures. A refusal
    /// given under it or later still stands: a frame repeating the figures
    /// moves `seq`, not this, and the machine would refuse the same need
    /// against the same figures again.
    figures_seq: u64,
    /// When a room with these figures was first announced: a later frame
    /// that repeats them moves the seq, not this.
    since_unix_ms: u64,
    room: AnnouncedRoom,
    /// [`AnnouncedRoom::capacity`] of `room`: whether this machine could ever
    /// hold a need, which is what keeps an entry off the machines behind it
    /// on the entry's hard key. It moves only with the machine's size and
    /// budgets, never with the work it runs or a brake.
    capacity: AnnouncedRoom,
    /// The room less every share taken from it: `offered`, `claimed` and
    /// `refused`.
    left: AnnouncedRoom,
    /// What each offer still out took from these figures. An offer that ends
    /// with neither a claim nor a refusal gives its share back: the machine
    /// never ran it, and never said its room could not.
    offered: HashMap<EntryId, Taken>,
    /// The shares of offers the machine claimed under these figures: it runs
    /// them. Figures that count them replace this whole.
    claimed: Vec<Taken>,
    /// The shares of offers the machine refused under these figures with an
    /// earlier refusal and no claim between (cairn/7339 comment 11), kept
    /// until new figures replace them. The first refusal gives its share
    /// back: the machine may have refused for a reason of that entry's own,
    /// and keeping it would leave an idle machine unoffered the next entry
    /// that fits. One after it is the machine's word that its room is short;
    /// giving that back too would offer a machine refusing everything each
    /// waiting entry in turn. So these figures offer what they fit and one
    /// more, whatever is waiting.
    refused: Vec<Taken>,
    /// Refusals of offers that took a share of these figures, since the
    /// machine's last claim under them.
    refusals: u32,
}

impl Announced {
    fn new(seq: u64, room: AnnouncedRoom, now_unix_ms: u64) -> Self {
        Self {
            seq,
            figures_seq: seq,
            since_unix_ms: now_unix_ms,
            capacity: room.capacity(),
            left: room.clone(),
            room,
            offered: HashMap::new(),
            claimed: Vec::new(),
            refused: Vec::new(),
            refusals: 0,
        }
    }

    /// What `left` would be with the refused shares given back.
    fn left_without_refused(&self) -> AnnouncedRoom {
        let mut without = self.clone();
        without.refused.clear();
        without.recharge();
        without.left
    }

    /// Work `left` out from the room and the shares taken from it: each
    /// passed hold given back, then each need charged, so a figure that
    /// saturates comes out the same whatever order the shares were taken in.
    fn recharge(&mut self) {
        let mut left = self.room.clone();
        let shares = || {
            self.offered
                .values()
                .chain(&self.claimed)
                .chain(&self.refused)
        };
        for share in shares() {
            if let Some((hold, hold_need)) = &share.hold {
                left.give_back(hold_need);
                left.holds.retain(|held| held.entry_id != hold.entry_id);
            }
        }
        for share in shares() {
            left.charge(&share.need);
        }
        self.left = left;
    }
}

#[derive(Debug, Clone)]
struct Machine {
    generation: u64,
    room: Option<Announced>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Standing {
    Waiting,
    /// Handed to a machine; the caller's offer is out. Nothing matches it
    /// until the machine answers.
    /// What it took from that machine's room is kept in the room's
    /// [`Announced::offered`].
    Assigned {
        executor_id: MachineId,
        generation: u64,
    },
}

/// What one offer took from the room it was assigned under: the entry's
/// need, and the entry's own protected hold, which its claim consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Taken {
    need: Need,
    hold: Option<(HeadroomRoomHold, Need)>,
}

/// A machine an entry could hold room on, in the order [`Dispatcher::reserve`]
/// prefers it.
struct HoldChoice {
    key: HardKey,
    holds_other_room: bool,
    holding_here: usize,
    committed_cores: u64,
    owed_memory: u64,
    executor_id: MachineId,
    generation: u64,
    seq: u64,
    need: Need,
}

impl HoldChoice {
    /// The hard key first (D3): a preference for an uncommitted machine never
    /// puts the hold on one that would clamp or defer the work.
    #[allow(clippy::type_complexity)]
    fn order(
        &self,
    ) -> (
        (bool, Reverse<Option<Option<u64>>>, bool),
        bool,
        usize,
        u64,
        u64,
        &str,
    ) {
        (
            self.key.order(),
            self.holds_other_room,
            self.holding_here,
            self.committed_cores,
            self.owed_memory,
            &self.executor_id,
        )
    }
}

#[derive(Debug, Clone)]
struct Entry {
    waiting: Waiting,
    standing: Standing,
    /// Machine to the room seq it last refused this entry under. A refusal
    /// belongs to the process that gave it: it is forgotten when that
    /// machine's link ends or a new process attaches, whose room seq starts
    /// again from 1.
    declined_at: HashMap<MachineId, u64>,
    /// Machine to the figures seq under which `admit` last turned this entry
    /// away there (a stale link, a degraded pipe, room a resident model
    /// holds). Such a machine keeps the entry off the machines behind it no
    /// more than a silent one does ([`Dispatcher::blocker`]). It ends with
    /// the process. New figures and [`Dispatcher::rewalk`] ask `admit` again
    /// ([`Dispatcher::ask_admit_again`]): it is kept under the new figures
    /// while `admit` still turns the entry away, and forgotten once it does
    /// not.
    turned_away: HashMap<MachineId, u64>,
    /// The machine holding room for this entry, sticky while it stays
    /// eligible.
    reserved_on: Option<MachineId>,
}

impl Entry {
    fn refused_under(&self, executor_id: &str, seq: u64) -> bool {
        self.declined_at
            .get(executor_id)
            .is_some_and(|declined| *declined >= seq)
    }

    fn turned_away_under(&self, executor_id: &str, figures_seq: u64) -> bool {
        self.turned_away
            .get(executor_id)
            .is_some_and(|turned| *turned >= figures_seq)
    }
}

/// The placement state of every waiting entry and every attached machine.
#[derive(Debug, Default)]
pub(crate) struct Dispatcher {
    machines: HashMap<MachineId, Machine>,
    entries: HashMap<EntryId, Entry>,
    /// For each machine, the entries that may run there.
    eligible: HashMap<MachineId, HashSet<EntryId>>,
    /// Every entry's pending reserve-from and horizon instants.
    deadlines: BTreeSet<(u64, EntryId)>,
    /// The entries entitled to hold room: standing work, and work whose
    /// reserve-from instant has passed. One holding none asks again when a
    /// machine it may run on announces new figures.
    entitled: HashSet<EntryId>,
    work: Work,
}

/// What is left of `room` for other work once `need` is placed there: free
/// cores for compile work, which is sized in slots, otherwise free memory.
fn left_after(room: &AnnouncedRoom, need: &Need) -> u64 {
    let mut after = room.clone();
    after.charge(need);
    if need.compile {
        after.cores.free()
    } else {
        after.memory.map_or(0, |memory| memory.free_bytes())
    }
}

impl Dispatcher {
    pub(crate) fn take_work(&mut self) -> Work {
        std::mem::take(&mut self.work)
    }

    pub(crate) fn contains(&self, entry_id: &str) -> bool {
        self.entries.contains_key(entry_id)
    }

    /// The entries [`Self::deadline_reached`] at `now` would expire: their
    /// horizon has arrived.
    pub(crate) fn expiring(&self, now_unix_ms: u64) -> Vec<EntryId> {
        self.deadlines
            .iter()
            .take_while(|(at, _)| *at <= now_unix_ms)
            .filter(|(_, entry_id)| {
                self.entries
                    .get(entry_id)
                    .is_some_and(|entry| now_unix_ms >= entry.waiting.horizon_unix_ms)
            })
            .map(|(_, entry_id)| entry_id.clone())
            .collect()
    }

    /// The earliest instant [`Self::deadline_reached`] has work at.
    pub(crate) fn next_deadline(&self) -> Option<u64> {
        self.deadlines.first().map(|(at, _)| *at)
    }

    /// The room seq `executor_id` last refused `entry_id` under.
    pub(crate) fn declined_at(&self, entry_id: &str, executor_id: &str) -> Option<u64> {
        self.entries
            .get(entry_id)?
            .declined_at
            .get(executor_id)
            .copied()
    }

    /// The machine asked to hold room for `entry_id`, if one is.
    pub(crate) fn reserved_on(&self, entry_id: &str) -> Option<&str> {
        self.entries.get(entry_id)?.reserved_on.as_deref()
    }

    /// Whether the refusal `executor_id` gave `entry_id` still stands: it was
    /// given under the figures the machine still announces.
    pub(crate) fn refusal_stands(&self, entry_id: &str, executor_id: &str) -> bool {
        let (Some(entry), Some(announced)) = (
            self.entries.get(entry_id),
            self.machines
                .get(executor_id)
                .and_then(|machine| machine.room.as_ref()),
        ) else {
            return false;
        };
        entry.refused_under(executor_id, announced.figures_seq)
    }

    /// Whether `admit`'s turning `entry_id` away from `executor_id` stands:
    /// the entry may run there, and it was turned away under the figures
    /// the machine still announces.
    #[cfg(test)]
    pub(crate) fn turned_away_stands(&self, entry_id: &str, executor_id: &str) -> bool {
        let (Some(entry), Some(announced)) = (
            self.entries.get(entry_id),
            self.machines
                .get(executor_id)
                .and_then(|machine| machine.room.as_ref()),
        ) else {
            return false;
        };
        entry.waiting.candidates.contains_key(executor_id)
            && entry.turned_away_under(executor_id, announced.figures_seq)
    }

    /// Every entitled waiting entry, with the machine holding room for it.
    #[cfg(test)]
    pub(crate) fn entitled_waiting(&self) -> HashMap<EntryId, Option<MachineId>> {
        self.entitled
            .iter()
            .filter_map(|entry_id| {
                let entry = self.entries.get(entry_id)?;
                (entry.standing == Standing::Waiting)
                    .then(|| (entry_id.clone(), entry.reserved_on.clone()))
            })
            .collect()
    }

    /// When `executor_id`'s current room figures were first announced.
    pub(crate) fn room_since(&self, executor_id: &str) -> Option<u64> {
        self.machines
            .get(executor_id)?
            .room
            .as_ref()
            .map(|announced| announced.since_unix_ms)
    }

    /// Whether `entry_id`'s need fits what is left of `executor_id`'s room at
    /// `now`, judged exactly as the walk judges it, with the shortfall when
    /// it does not. `None` when the entry may not run there or the machine
    /// has announced no room.
    pub(crate) fn shortfall(
        &self,
        entry_id: &str,
        executor_id: &str,
        now_unix_ms: u64,
    ) -> Option<Result<(), RoomShort>> {
        let entry = self.entries.get(entry_id)?;
        let need = &entry.waiting.candidates.get(executor_id)?.need;
        let announced = self.machines.get(executor_id)?.room.as_ref()?;
        Some(self.fit_left(entry, executor_id, &announced.left, need, now_unix_ms))
    }

    /// The machine that keeps `entry_id` off `executor_id` now, if one does:
    /// see [`Self::blocker`].
    pub(crate) fn held_off_by(
        &self,
        entry_id: &str,
        executor_id: &str,
        now_unix_ms: u64,
    ) -> Option<&str> {
        let entry = self.entries.get(entry_id)?;
        self.blocker(entry, executor_id, now_unix_ms)
    }

    /// The machine that keeps `entry` off `executor_id`: one ahead of it on
    /// the entry's hard key that could hold the entry. The best such machine
    /// is named, by key and then name. Nothing keeps off work its caller
    /// placed (pinned, named, or kept home).
    ///
    /// A machine that has announced room keeps the entry off only while that
    /// room's capacity fits the entry's need there. Offered work, it keeps
    /// the entry off; one that refused the entry under the figures it still
    /// announces does so only until the entry's reserve-from instant (D1), so
    /// a refusal that moves no figures cannot hold the entry to its horizon.
    ///
    /// A machine that cannot be offered the entry now stands as a silent one
    /// (D2): it keeps off only deferred or clamped work (what this machine
    /// would run at a size nobody measured it to hold), and only until the
    /// entry's reserve-from instant, which [`Self::deadline_reached`] places
    /// it at. That is one attached that has announced nothing yet (after a
    /// rebuild every machine re-attaches and announces in any order, and the
    /// small machine speaking first must not take work the large one,
    /// seconds from speaking, could hold), one away for an update, and one
    /// the caller's `admit` turned this entry away from under the figures it
    /// still announces. A machine with no link keeps nothing off.
    fn blocker<'a>(
        &'a self,
        entry: &'a Entry,
        executor_id: &str,
        now_unix_ms: u64,
    ) -> Option<&'a str> {
        let waiting = &entry.waiting;
        if !waiting.may_spill {
            return None;
        }
        let here = waiting.candidates.get(executor_id)?.key;
        let disfavoured = here.disfavoured();
        let early = now_unix_ms < waiting.reserve_from_unix_ms;
        waiting
            .candidates
            .iter()
            .filter(|(id, better)| {
                better.key.order() < here.order()
                    && self
                        .machines
                        .get(*id)
                        .is_some_and(|machine| match &machine.room {
                            None => disfavoured && early,
                            Some(announced) => {
                                let silent = better.away
                                    || entry.turned_away_under(id, announced.figures_seq);
                                fits(&announced.capacity, &better.need).is_ok()
                                    && if silent {
                                        disfavoured && early
                                    } else {
                                        early || !entry.refused_under(id, announced.figures_seq)
                                    }
                            }
                        })
            })
            .min_by(|a, b| a.1.key.order().cmp(&b.1.key.order()).then(a.0.cmp(b.0)))
            .map(|(id, _)| id.as_str())
    }

    /// Place again every waiting entry that may run on `executor_id` and on
    /// a machine behind it on the entry's hard key: whether this machine
    /// keeps the entry off that one turned on what just changed here (the
    /// capacity its room announces, or its link). Entries whose machines all
    /// stand level cannot be kept off by it and are not touched.
    fn place_behind(
        &mut self,
        executor_id: &str,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        let mut behind: Vec<((bool, QueueRank), EntryId)> = self
            .eligible
            .get(executor_id)
            .into_iter()
            .flatten()
            .filter_map(|entry_id| {
                let entry = self.entries.get(entry_id)?;
                let here = entry.waiting.candidates.get(executor_id)?.key.order();
                (entry.standing == Standing::Waiting
                    && entry.waiting.may_spill
                    && entry
                        .waiting
                        .candidates
                        .values()
                        .any(|other| other.key.order() > here))
                .then(|| (entry.waiting.rank(now_unix_ms), entry_id.clone()))
            })
            .collect();
        behind.sort_by(|a, b| b.cmp(a));
        let mut effects = Effects::default();
        for (_, entry_id) in behind {
            effects.absorb(self.place(&entry_id, now_unix_ms, admit));
        }
        effects
    }

    /// The latest room `executor_id` announced and what is left of it.
    pub(crate) fn room(&self, executor_id: &str) -> Option<(u64, &AnnouncedRoom, &AnnouncedRoom)> {
        self.machines
            .get(executor_id)?
            .room
            .as_ref()
            .map(|announced| (announced.seq, &announced.room, &announced.left))
    }

    /// The machines `entry_id` may run on, with its need on each.
    pub(crate) fn candidates(&self, entry_id: &str) -> Option<&HashMap<MachineId, Candidate>> {
        self.entries
            .get(entry_id)
            .map(|entry| &entry.waiting.candidates)
    }

    /// The link generation `executor_id` is attached on, if it is.
    pub(crate) fn generation(&self, executor_id: &str) -> Option<u64> {
        self.machines
            .get(executor_id)
            .map(|machine| machine.generation)
    }

    /// Walk `executor_id`'s room again as it stands. For the caller whose
    /// `admit` turned an entry away there for something that may have
    /// cleared since (a link that was stale and is fresh again): `admit` is
    /// asked again for each entry it turned away, so the machine keeps the
    /// entries it now takes off the machines behind it as its room says, and
    /// an entry holding room on one of those asks for room again, as on new
    /// figures.
    pub(crate) fn rewalk(
        &mut self,
        executor_id: &str,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        self.ask_admit_again(executor_id, admit);
        let mut effects = self.walk(executor_id, now_unix_ms, admit);
        effects.absorb(self.ask_for_headroom(executor_id, now_unix_ms, admit));
        effects
    }

    /// Ask `admit` again about each entry it turned away from `executor_id`:
    /// the turning-away is kept, under the figures the machine announces
    /// now, for each it still turns away, and forgotten for the rest. A
    /// record that lapsed unasked would stand the machine as one that keeps
    /// entries off the machines behind it ([`Self::blocker`]) though `admit`
    /// still turns them away there: an entry holding room behind it would
    /// give that room up for a machine that will not take it.
    fn ask_admit_again(&mut self, executor_id: &str, admit: &mut impl FnMut(&str, &str) -> bool) {
        let Some(figures_seq) = self
            .machines
            .get(executor_id)
            .and_then(|machine| machine.room.as_ref())
            .map(|announced| announced.figures_seq)
        else {
            return;
        };
        let turned: Vec<EntryId> = self
            .eligible
            .get(executor_id)
            .into_iter()
            .flatten()
            .filter(|entry_id| {
                self.entries
                    .get(*entry_id)
                    .is_some_and(|entry| entry.turned_away.contains_key(executor_id))
            })
            .cloned()
            .collect();
        for entry_id in turned {
            let still = !admit(&entry_id, executor_id);
            if let Some(entry) = self.entries.get_mut(&entry_id) {
                if still {
                    entry
                        .turned_away
                        .insert(executor_id.to_string(), figures_seq);
                } else {
                    entry.turned_away.remove(executor_id);
                }
            }
        }
    }

    /// Remember that `admit` turned `entry_id` away from `executor_id` under
    /// the figures it announces. Whether that is news: the entry was not
    /// already turned away there under them.
    fn turn_away(&mut self, entry_id: &str, executor_id: &str) -> bool {
        let Some(figures_seq) = self
            .machines
            .get(executor_id)
            .and_then(|machine| machine.room.as_ref())
            .map(|announced| announced.figures_seq)
        else {
            return false;
        };
        let Some(entry) = self.entries.get_mut(entry_id) else {
            return false;
        };
        entry
            .turned_away
            .insert(executor_id.to_string(), figures_seq)
            .is_none_or(|before| before < figures_seq)
    }

    /// A machine's link was made, or replaced by a new process. It has no
    /// room until it announces, and a new process has refused nothing yet.
    /// The link already attached changes nothing: its room may have arrived
    /// first. A link that replaces one that had announced room no longer
    /// keeps entries off the machines behind it by that room, so they are
    /// placed again.
    pub(crate) fn attach_link(
        &mut self,
        executor_id: &str,
        generation: u64,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        let replaced = match self.machines.get(executor_id) {
            Some(machine) if machine.generation == generation => return Effects::default(),
            Some(machine) => machine.room.is_some(),
            None => false,
        };
        self.machines.insert(
            executor_id.to_string(),
            Machine {
                generation,
                room: None,
            },
        );
        self.forget_refusals(executor_id);
        if replaced {
            self.place_behind(executor_id, now_unix_ms, admit)
        } else {
            Effects::default()
        }
    }

    /// [`Self::attach_link`] at time 0, admitting everything, for tests that
    /// attach a machine before anything waits.
    #[cfg(test)]
    pub(crate) fn attach(&mut self, executor_id: &str, generation: u64) {
        self.attach_link(executor_id, generation, 0, &mut |_, _| true);
    }

    /// Every refusal `executor_id`'s process gave, and every turning-away
    /// `admit` gave under its rooms: it has ended. The process after it
    /// counts its room from 1 again, so a refusal kept by seq would stand
    /// over thousands of its rooms.
    fn forget_refusals(&mut self, executor_id: &str) {
        for entry in self.entries.values_mut() {
            entry.declined_at.remove(executor_id);
            entry.turned_away.remove(executor_id);
        }
    }

    /// A machine's link ended. Entries reserving room there look elsewhere;
    /// entries offered there come back through [`Self::decline`] or
    /// [`Self::requeue`] as the queue ends the generation.
    pub(crate) fn detach(
        &mut self,
        executor_id: &str,
        generation: u64,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        if !self
            .machines
            .get(executor_id)
            .is_some_and(|machine| machine.generation == generation)
        {
            return Effects::default();
        }
        self.machines.remove(executor_id);
        self.forget_refusals(executor_id);
        let reserving: Vec<EntryId> = self
            .eligible
            .get(executor_id)
            .into_iter()
            .flatten()
            .filter(|entry_id| {
                self.entries
                    .get(*entry_id)
                    .is_some_and(|entry| entry.reserved_on.as_deref() == Some(executor_id))
            })
            .cloned()
            .collect();
        let mut effects = Effects::default();
        for entry_id in reserving {
            if let Some(entry) = self.entries.get_mut(&entry_id) {
                entry.reserved_on = None;
            }
            effects.absorb(self.place(&entry_id, now_unix_ms, admit));
        }
        // A machine with no link keeps nothing off the machines behind it.
        effects.absorb(self.place_behind(executor_id, now_unix_ms, admit));
        effects
    }

    /// A machine announced its room. A frame from a replaced link, or one no
    /// newer than the room held, changes nothing. A newer frame whose figures
    /// equal the room held moves the seq and matches nothing: the machine's
    /// room did not change, so no refusal it gave can have stopped standing.
    /// New figures ask `admit` again about the entries it turned away here
    /// ([`Self::ask_admit_again`]) before anything is matched. Figures whose
    /// capacity moved (the first the link announces among them) also place
    /// again the entries this machine may keep off another.
    pub(crate) fn set_room(
        &mut self,
        executor_id: &str,
        generation: u64,
        seq: u64,
        room: AnnouncedRoom,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        let Some(machine) = self
            .machines
            .get_mut(executor_id)
            .filter(|machine| machine.generation == generation)
        else {
            return Effects::default();
        };
        match machine.room.as_mut() {
            Some(held) if held.seq >= seq => return Effects::default(),
            Some(held) if held.room == room => {
                held.seq = seq;
                return Effects::default();
            }
            _ => {}
        }
        // A resident starting or ending changes the slot width, not the
        // reservation an offer already took. Only candidate needs are repriced;
        // Taken keeps the shares actually offered under earlier figures.
        let mut effects = Effects::default();
        for entry_id in self.eligible.get(executor_id).into_iter().flatten() {
            let Some(entry) = self.entries.get_mut(entry_id) else {
                continue;
            };
            let Some(candidate) = entry.waiting.candidates.get_mut(executor_id) else {
                continue;
            };
            candidate.need = candidate.need.with_slots(room.slots);
            if entry.reserved_on.as_deref() == Some(executor_id)
                && fits(&room.capacity(), &candidate.need).is_err()
            {
                entry.reserved_on = None;
                effects
                    .released
                    .push((entry_id.clone(), executor_id.to_string(), generation));
            }
        }
        let announced = Announced::new(seq, room, now_unix_ms);
        let capacity_moved = machine
            .room
            .as_ref()
            .is_none_or(|held| held.capacity != announced.capacity);
        machine.room = Some(announced);
        self.ask_admit_again(executor_id, admit);
        effects.absorb(self.walk(executor_id, now_unix_ms, admit));
        effects.absorb(self.ask_for_headroom(executor_id, now_unix_ms, admit));
        if capacity_moved {
            effects.absorb(self.place_behind(executor_id, now_unix_ms, admit));
        }
        effects
    }

    /// `executor_id` announced new figures, so what its drained room could
    /// hold may have changed: every entitled entry that may run there and
    /// holds no room asks for it again, oldest first. Without this an entry
    /// whose entitlement arrived while every drained room was closed (a
    /// momentary brake suffices) would not ask again until some event touched
    /// it, while younger work took the room machines freed (tenet 1444). An
    /// entry holding room on a machine this one now keeps it off (the
    /// refusal that let it go there no longer stands) asks again too. One
    /// look per entitled entry, not per waiting entry.
    fn ask_for_headroom(
        &mut self,
        executor_id: &str,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        let Some(eligible) = self.eligible.get(executor_id) else {
            return Effects::default();
        };
        let mut asking: Vec<((bool, QueueRank), EntryId)> = self
            .entitled
            .iter()
            .filter(|entry_id| eligible.contains(*entry_id))
            .filter_map(|entry_id| {
                let entry = self.entries.get(entry_id)?;
                let asks = match entry.reserved_on.as_deref() {
                    None => true,
                    Some(reserved) => {
                        reserved != executor_id
                            && self.blocker(entry, reserved, now_unix_ms).is_some()
                    }
                };
                (entry.standing == Standing::Waiting && asks)
                    .then(|| (entry.waiting.rank(now_unix_ms), entry_id.clone()))
            })
            .collect();
        asking.sort_by(|a, b| b.cmp(a));
        let mut effects = Effects::default();
        for (_, entry_id) in asking {
            effects.absorb(self.reserve(&entry_id, now_unix_ms, admit));
        }
        effects
    }

    fn price_candidates(&self, candidates: &mut HashMap<MachineId, Candidate>) {
        for (id, candidate) in candidates {
            if let Some(announced) = self
                .machines
                .get(id)
                .and_then(|machine| machine.room.as_ref())
            {
                candidate.need = candidate.need.with_slots(announced.room.slots);
            }
        }
    }

    /// An entry joins. It is matched once against the rooms already
    /// announced; after that only events that bear on it match it again.
    pub(crate) fn enqueue(
        &mut self,
        mut waiting: Waiting,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        self.price_candidates(&mut waiting.candidates);
        let entry_id = waiting.entry_id.clone();
        let mut effects = self.withdraw(&entry_id, now_unix_ms, admit);
        for executor_id in waiting.candidates.keys() {
            self.eligible
                .entry(executor_id.clone())
                .or_default()
                .insert(entry_id.clone());
        }
        self.deadlines
            .insert((waiting.horizon_unix_ms, entry_id.clone()));
        // Standing work is entitled before this instant, but the machine
        // reads the authorization's instant, so precedence moves here too.
        if waiting.reserve_from_unix_ms > now_unix_ms {
            self.deadlines
                .insert((waiting.reserve_from_unix_ms, entry_id.clone()));
        }
        if waiting.entitled(now_unix_ms) {
            self.entitled.insert(entry_id.clone());
        }
        let machines: Vec<MachineId> = waiting.candidates.keys().cloned().collect();
        self.entries.insert(
            entry_id.clone(),
            Entry {
                waiting,
                standing: Standing::Waiting,
                declined_at: HashMap::new(),
                turned_away: HashMap::new(),
                reserved_on: None,
            },
        );
        effects.absorb(self.place(&entry_id, now_unix_ms, admit));
        effects.absorb(self.walk_held_for(&entry_id, machines, now_unix_ms, admit));
        effects
    }

    /// What `entry_id` may run on, or needs there, changed: a figure was
    /// learned, or a machine's facts moved its eligibility. Only this entry is
    /// matched.
    pub(crate) fn set_candidates(
        &mut self,
        entry_id: &str,
        mut candidates: HashMap<MachineId, Candidate>,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        self.price_candidates(&mut candidates);
        let Some(entry) = self.entries.get_mut(entry_id) else {
            return Effects::default();
        };
        let mut before: Vec<MachineId> = entry.waiting.candidates.keys().cloned().collect();
        entry.waiting.candidates = candidates;
        // Nothing asks `admit` again on a machine the entry may not run on.
        let now_on = &entry.waiting.candidates;
        entry
            .turned_away
            .retain(|machine, _| now_on.contains_key(machine));
        let mut effects = Effects::default();
        if let Some(reserved) = entry
            .reserved_on
            .take_if(|machine| !entry.waiting.candidates.contains_key(machine))
        {
            let generation = self.machines.get(&reserved).map_or(0, |m| m.generation);
            effects
                .released
                .push((entry_id.to_string(), reserved, generation));
        }
        for executor_id in &before {
            if let Some(set) = self.eligible.get_mut(executor_id) {
                set.remove(entry_id);
            }
        }
        let now_eligible: Vec<MachineId> = self.entries[entry_id]
            .waiting
            .candidates
            .keys()
            .cloned()
            .collect();
        for executor_id in &now_eligible {
            self.eligible
                .entry(executor_id.clone())
                .or_default()
                .insert(entry_id.to_string());
        }
        effects.absorb(self.place(entry_id, now_unix_ms, admit));
        before.extend(now_eligible);
        before.sort();
        before.dedup();
        effects.absorb(self.walk_held_for(entry_id, before, now_unix_ms, admit));
        effects
    }

    /// `executor_id` refused `entry_id` under room `seq`, or the runner gave
    /// up on an offer it sent there (its link closed, or it never answered).
    /// The entry is not offered there again until new figures are announced,
    /// and it is matched now against the other machines' rooms. What the
    /// offer took from those figures is given back on the machine's first
    /// refusal since its last claim under them, and the machine is walked
    /// for it; after that it is kept until new figures
    /// ([`Announced::refused`]), and no other entry is evaluated.
    ///
    /// A refusal from a machine the entry is no longer offered to (a second
    /// report of an offer that already ended: the machine's own Decline,
    /// then the offer's timeout) is recorded, and no refusal is counted
    /// against a room twice. While the entry is offered elsewhere nothing
    /// else changes: that offer and its share stand. While it waits, it is
    /// matched again as it stands, which assigns it only if `admit`'s answer
    /// moved since it was last matched.
    pub(crate) fn decline(
        &mut self,
        entry_id: &str,
        executor_id: &str,
        seq: u64,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        let attached = self.machines.get(executor_id).map(|m| m.generation);
        let Some(entry) = self.entries.get_mut(entry_id) else {
            return Effects::default();
        };
        // Only the process the offer went to can have refused it: an offer
        // out on a link a new process has since replaced says nothing about
        // the new one.
        let given_by_attached = match &entry.standing {
            Standing::Assigned {
                executor_id: on,
                generation,
                ..
            } if on == executor_id => attached == Some(*generation),
            _ => attached.is_some(),
        };
        if given_by_attached {
            let held = entry
                .declined_at
                .entry(executor_id.to_string())
                .or_insert(0);
            *held = (*held).max(seq);
        }
        if matches!(&entry.standing, Standing::Assigned { executor_id: on, .. } if on != executor_id)
        {
            return Effects::default();
        }
        let standing = std::mem::replace(&mut entry.standing, Standing::Waiting);
        if !matches!(&standing, Standing::Assigned { executor_id: on, .. } if on == executor_id) {
            return self.offer_ended(entry_id, standing, now_unix_ms, admit);
        }
        let returned = self.refuse_share(entry_id, standing);
        let mut effects = self.place(entry_id, now_unix_ms, admit);
        if let Some(executor_id) = returned {
            effects.absorb(self.walk(&executor_id, now_unix_ms, admit));
        }
        effects
    }

    /// Settle what a refused offer took from the room it was assigned under,
    /// while those figures stand on the link the offer went to: given back
    /// on the machine's first such refusal since its last claim under them,
    /// kept among [`Announced::refused`] after that. The machine whose room
    /// got its share back.
    fn refuse_share(&mut self, entry_id: &str, standing: Standing) -> Option<MachineId> {
        let Standing::Assigned {
            executor_id,
            generation,
        } = standing
        else {
            return None;
        };
        let announced = self
            .machines
            .get_mut(&executor_id)
            .filter(|machine| machine.generation == generation)?
            .room
            .as_mut()?;
        let share = announced.offered.remove(entry_id)?;
        announced.refusals = announced.refusals.saturating_add(1);
        if announced.refusals > 1 {
            announced.refused.push(share);
            return None;
        }
        announced.recharge();
        Some(executor_id)
    }

    /// Keep what a claimed offer took from the room it was assigned under
    /// until new figures count it. The claim is the machine running work
    /// under these figures, so the refusals before it no longer run on.
    fn keep_claimed(&mut self, entry_id: &str, standing: Standing) {
        let Standing::Assigned {
            executor_id,
            generation,
        } = standing
        else {
            return;
        };
        if let Some(announced) = self
            .machines
            .get_mut(&executor_id)
            .filter(|machine| machine.generation == generation)
            .and_then(|machine| machine.room.as_mut())
        {
            announced.refusals = 0;
            if let Some(share) = announced.offered.remove(entry_id) {
                announced.claimed.push(share);
            }
        }
    }

    /// An offer came back without a refusal (the machine never heard of it):
    /// the entry waits again and is matched as it stands, and what the offer
    /// took is given back as for [`Self::decline`].
    pub(crate) fn requeue(
        &mut self,
        entry_id: &str,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        let Some(entry) = self.entries.get_mut(entry_id) else {
            return Effects::default();
        };
        let standing = std::mem::replace(&mut entry.standing, Standing::Waiting);
        self.offer_ended(entry_id, standing, now_unix_ms, admit)
    }

    /// `entry_id`, waiting again, is matched; and the machine its offer went
    /// to, when that offer's share of the room comes back, is walked.
    fn offer_ended(
        &mut self,
        entry_id: &str,
        standing: Standing,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        let returned = self.give_back(entry_id, standing);
        let mut effects = self.place(entry_id, now_unix_ms, admit);
        if let Some(executor_id) = returned {
            effects.absorb(self.walk(&executor_id, now_unix_ms, admit));
        }
        effects
    }

    /// Give back what an offer that ended without a claim took from the
    /// room it was assigned under, while those figures still stand (new
    /// figures replace the shares whole). The machine whose room it was.
    fn give_back(&mut self, entry_id: &str, standing: Standing) -> Option<MachineId> {
        let Standing::Assigned {
            executor_id,
            generation,
        } = standing
        else {
            return None;
        };
        let announced = self
            .machines
            .get_mut(&executor_id)
            .filter(|machine| machine.generation == generation)?
            .room
            .as_mut()?;
        announced.offered.remove(entry_id)?;
        announced.recharge();
        Some(executor_id)
    }

    /// A machine claimed the entry: it leaves the dispatcher, and any room
    /// held for it on another machine is released. What its offer took stays
    /// taken until the machine announces figures that count it, and the
    /// machine's count of refusals under its figures starts again.
    pub(crate) fn claimed(&mut self, entry_id: &str, executor_id: &str) -> Effects {
        let mut effects = Effects::default();
        let Some(entry) = self.remove(entry_id) else {
            return effects;
        };
        if let Some(reserved) = entry.reserved_on.filter(|reserved| reserved != executor_id) {
            let generation = self.machines.get(&reserved).map_or(0, |m| m.generation);
            effects
                .released
                .push((entry_id.to_string(), reserved, generation));
        }
        if matches!(&entry.standing, Standing::Assigned { executor_id: on, .. } if on == executor_id)
        {
            self.keep_claimed(entry_id, entry.standing);
        }
        effects
    }

    /// The entry left without a claim: settled, retired, cancelled, expired.
    /// Room held for it is released, and what an offer of it took is given
    /// back as for [`Self::decline`].
    pub(crate) fn withdraw(
        &mut self,
        entry_id: &str,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        let mut effects = Effects::default();
        let Some(entry) = self.remove(entry_id) else {
            return effects;
        };
        if let Some(reserved) = entry.reserved_on {
            let generation = self.machines.get(&reserved).map_or(0, |m| m.generation);
            effects
                .released
                .push((entry_id.to_string(), reserved, generation));
        }
        if let Some(executor_id) = self.give_back(entry_id, entry.standing) {
            effects.absorb(self.walk(&executor_id, now_unix_ms, admit));
        }
        effects
    }

    fn remove(&mut self, entry_id: &str) -> Option<Entry> {
        let entry = self.entries.remove(entry_id)?;
        self.entitled.remove(entry_id);
        for executor_id in entry.waiting.candidates.keys() {
            if let Some(set) = self.eligible.get_mut(executor_id) {
                set.remove(entry_id);
            }
        }
        self.deadlines
            .remove(&(entry.waiting.horizon_unix_ms, entry_id.to_string()));
        self.deadlines
            .remove(&(entry.waiting.reserve_from_unix_ms, entry_id.to_string()));
        Some(entry)
    }

    /// Every deadline at or before `now`: a horizon expires its entry, a
    /// reserve-from instant makes it entitled to hold room.
    pub(crate) fn deadline_reached(
        &mut self,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        let mut effects = Effects::default();
        while let Some((at, entry_id)) = self.deadlines.first().cloned() {
            if at > now_unix_ms {
                break;
            }
            self.deadlines.remove(&(at, entry_id.clone()));
            let Some(entry) = self.entries.get(&entry_id) else {
                continue;
            };
            if now_unix_ms >= entry.waiting.horizon_unix_ms {
                effects.absorb(self.withdraw(&entry_id, now_unix_ms, admit));
                effects.expired.push(entry_id);
            } else {
                if entry.waiting.entitled(now_unix_ms) {
                    self.entitled.insert(entry_id.clone());
                }
                effects.absorb(self.place(&entry_id, now_unix_ms, admit));
            }
        }
        effects
    }

    /// Match one machine's room against the entries that may run there,
    /// oldest and most urgent first, subtracting each assignment. An entry
    /// `admit` newly turns away here no longer waits for this machine, so it
    /// is placed again.
    fn walk(
        &mut self,
        executor_id: &str,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        self.work.passes += 1;
        let Some(machine) = self.machines.get(executor_id) else {
            return Effects::default();
        };
        let Some(announced) = machine.room.as_ref() else {
            return Effects::default();
        };
        let (generation, seq) = (machine.generation, announced.seq);
        let mut fitting = Vec::new();
        for entry_id in self.eligible.get(executor_id).into_iter().flatten() {
            let Some(entry) = self.entries.get(entry_id) else {
                continue;
            };
            if entry.standing != Standing::Waiting
                || entry.refused_under(executor_id, announced.figures_seq)
            {
                continue;
            }
            let Some(candidate) = entry
                .waiting
                .candidates
                .get(executor_id)
                .filter(|candidate| !candidate.away)
            else {
                continue;
            };
            self.work.fit_evaluations += 1;
            if self.fits_left(
                entry,
                executor_id,
                &announced.left,
                &candidate.need,
                now_unix_ms,
            ) && self.blocker(entry, executor_id, now_unix_ms).is_none()
            {
                fitting.push((entry.waiting.rank(now_unix_ms), entry_id.clone()));
            }
        }
        fitting.sort_by(|a, b| b.cmp(a));
        let mut effects = Effects::default();
        let mut turned = Vec::new();
        for (_, entry_id) in fitting {
            self.work.fit_evaluations += 1;
            let entry = &self.entries[&entry_id];
            let need = entry.waiting.candidates[executor_id].need.clone();
            let left = &self.machines[executor_id].room.as_ref().unwrap().left;
            if !self.fits_left(entry, executor_id, left, &need, now_unix_ms) {
                continue;
            }
            if !admit(&entry_id, executor_id) {
                if self.turn_away(&entry_id, executor_id) {
                    turned.push(entry_id);
                }
                continue;
            }
            effects.absorb(self.assign(
                &entry_id,
                executor_id,
                generation,
                seq,
                &need,
                false,
                now_unix_ms,
            ));
        }
        for entry_id in turned {
            effects.absorb(self.place(&entry_id, now_unix_ms, admit));
        }
        effects
    }

    /// Walk each of `machines` whose protected hold is `entry_id`'s: whether
    /// other entries pass that hold, and what passing it gives back, turn on
    /// this entry's presence and its need there.
    fn walk_held_for(
        &mut self,
        entry_id: &str,
        machines: Vec<MachineId>,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        let mut effects = Effects::default();
        for executor_id in machines {
            let held = self
                .machines
                .get(&executor_id)
                .and_then(|machine| machine.room.as_ref())
                .is_some_and(|announced| {
                    announced
                        .left
                        .holds
                        .iter()
                        .any(|hold| hold.protected && hold.entry_id == entry_id)
                });
            if held {
                effects.absorb(self.walk(&executor_id, now_unix_ms, admit));
            }
        }
        effects
    }

    /// The protected hold on `executor_id`'s room left-over that `entry`
    /// passes, with the hold's need there: the entry's own hold, or one the
    /// machine's claim lets it pass ([`passes_protected_hold`]) over the
    /// facts the offer carries. The offer carries the entry's authorization
    /// only to the machine holding room for it ([`Assignment::authorized`]),
    /// and seniority is the queue's waiting-since, the instant the offer
    /// states. An entry does not pass a hold whose entry this dispatcher
    /// does not hold, since it knows no need to give back for it.
    fn passed_hold<'a>(
        &'a self,
        entry: &Entry,
        executor_id: &str,
        left: &'a AnnouncedRoom,
        need: &Need,
        now_unix_ms: u64,
    ) -> Option<(&'a HeadroomRoomHold, &'a Need)> {
        let hold = left.holds.iter().find(|hold| hold.protected)?;
        let holder = self.entries.get(&hold.entry_id)?;
        let hold_need = &holder.waiting.candidates.get(executor_id)?.need;
        let waiting = &entry.waiting;
        let passes = hold.entry_id == waiting.entry_id || {
            let entitled = entry.reserved_on.as_deref() == Some(executor_id)
                && waiting.horizon_unix_ms > now_unix_ms
                && waiting.reserve_from_unix_ms <= now_unix_ms;
            passes_protected_hold(
                need.interactive,
                entitled && !need.interactive && fits(&left.drained(), need).is_ok(),
                (waiting.queued_at_unix_ms, &waiting.entry_id),
                (holder.waiting.queued_at_unix_ms, &hold.entry_id),
            )
        };
        passes.then_some((hold, hold_need))
    }

    /// Whether `need` fits `left`, `executor_id`'s room less what was
    /// assigned under it, the way the machine will judge `entry`'s offer.
    fn fits_left(
        &self,
        entry: &Entry,
        executor_id: &str,
        left: &AnnouncedRoom,
        need: &Need,
        now_unix_ms: u64,
    ) -> bool {
        self.fit_left(entry, executor_id, left, need, now_unix_ms)
            .is_ok()
    }

    // The shortfall carries the condition it is told from, as `fits` does.
    #[allow(clippy::result_large_err)]
    fn fit_left(
        &self,
        entry: &Entry,
        executor_id: &str,
        left: &AnnouncedRoom,
        need: &Need,
        now_unix_ms: u64,
    ) -> Result<(), RoomShort> {
        let precedence = match self.passed_hold(entry, executor_id, left, need, now_unix_ms) {
            Some((_, hold_need)) => Precedence::Passes(hold_need),
            None => Precedence::Behind,
        };
        fits_offer(left, need, precedence)
    }

    /// Match one waiting entry against every room it may use, and when none
    /// fits and it is entitled, hold room for it on one machine. Rooms are
    /// tried in hard-key order, but a blocked candidate does not end the
    /// search: a silent better machine can block a clamped candidate without
    /// blocking a later, unclamped candidate with less single-process capacity.
    fn place(
        &mut self,
        entry_id: &str,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        self.work.passes += 1;
        let Some(entry) = self.entries.get(entry_id) else {
            return Effects::default();
        };
        if entry.standing != Standing::Waiting {
            return Effects::default();
        }
        type Fitting = (
            (bool, Reverse<Option<Option<u64>>>, bool),
            bool,
            u64,
            MachineId,
            u64,
            u64,
            Need,
        );
        let mut fitting: Vec<Fitting> = Vec::new();
        for (executor_id, candidate) in &entry.waiting.candidates {
            if candidate.away {
                continue;
            }
            let Some(machine) = self.machines.get(executor_id) else {
                continue;
            };
            let Some(announced) = machine.room.as_ref() else {
                continue;
            };
            if entry.refused_under(executor_id, announced.figures_seq) {
                continue;
            }
            self.work.fit_evaluations += 1;
            if self.fits_left(
                entry,
                executor_id,
                &announced.left,
                &candidate.need,
                now_unix_ms,
            ) {
                fitting.push((
                    candidate.key.order(),
                    candidate.warm,
                    left_after(&announced.left, &candidate.need),
                    executor_id.clone(),
                    machine.generation,
                    announced.seq,
                    candidate.need.clone(),
                ));
            }
        }
        // The hard key, then warm first, then the most room left after the
        // need, then the name only to make the order total.
        fitting.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then(b.1.cmp(&a.1))
                .then(b.2.cmp(&a.2))
                .then(a.3.cmp(&b.3))
        });
        for (_, _, _, executor_id, generation, seq, need) in fitting {
            if self
                .blocker(&self.entries[entry_id], &executor_id, now_unix_ms)
                .is_some()
            {
                continue;
            }
            if admit(entry_id, &executor_id) {
                return self.assign(
                    entry_id,
                    &executor_id,
                    generation,
                    seq,
                    &need,
                    false,
                    now_unix_ms,
                );
            }
            self.turn_away(entry_id, &executor_id);
        }
        self.reserve(entry_id, now_unix_ms, admit)
    }

    /// Hold room for an entitled entry that fits no room. The machine that
    /// already holds it keeps it while it stays eligible and nothing keeps
    /// the entry off it; otherwise the choice is by fact: the hard key, then
    /// a machine holding no other entry's room, then the least committed
    /// against its drained room, then the name. A machine kept off the entry
    /// is never asked, so while the better machine cannot hold room (away,
    /// silent, or braked) the entry holds none.
    fn reserve(
        &mut self,
        entry_id: &str,
        now_unix_ms: u64,
        admit: &mut impl FnMut(&str, &str) -> bool,
    ) -> Effects {
        let mut effects = Effects::default();
        let entry = &self.entries[entry_id];
        if !entry.waiting.entitled(now_unix_ms) {
            return effects;
        }
        if let Some(reserved) = entry
            .reserved_on
            .clone()
            .filter(|reserved| entry.waiting.candidates.contains_key(reserved))
        {
            // The machine keeping the entry off its hold is asked first: one
            // `admit` newly turns away stands as a silent one, and may keep
            // nothing off, so the hold is not given up for a machine that
            // will not take the entry. It may have been all that kept the
            // entry off a room that fits it now, too: placed, not held for.
            let mut turned = false;
            while let Some(by) = self
                .blocker(&self.entries[entry_id], &reserved, now_unix_ms)
                .map(str::to_string)
            {
                if admit(entry_id, &by) || !self.turn_away(entry_id, &by) {
                    break;
                }
                turned = true;
            }
            if turned {
                return self.place(entry_id, now_unix_ms, admit);
            }
            let entry = &self.entries[entry_id];
            let still_fits = self
                .machines
                .get(&reserved)
                .zip(entry.waiting.candidates.get(&reserved))
                .is_some_and(|(machine, candidate)| {
                    !candidate.away
                        && machine.room.as_ref().is_none_or(|announced| {
                            fits(&announced.capacity, &candidate.need).is_ok()
                        })
                });
            if still_fits && self.blocker(entry, &reserved, now_unix_ms).is_none() {
                return effects;
            }
            let generation = self.machines.get(&reserved).map_or(0, |m| m.generation);
            if let Some(entry) = self.entries.get_mut(entry_id) {
                entry.reserved_on = None;
            }
            effects
                .released
                .push((entry_id.to_string(), reserved, generation));
        }
        let entry = &self.entries[entry_id];
        let holding: HashMap<&str, usize> = self
            .entries
            .values()
            .filter_map(|other| other.reserved_on.as_deref())
            .fold(HashMap::new(), |mut counts, machine| {
                *counts.entry(machine).or_default() += 1;
                counts
            });
        let mut choices: Vec<HoldChoice> = Vec::new();
        for (executor_id, candidate) in &entry.waiting.candidates {
            if candidate.away {
                continue;
            }
            let Some(machine) = self.machines.get(executor_id) else {
                continue;
            };
            let Some(announced) = machine.room.as_ref() else {
                continue;
            };
            if entry.refused_under(executor_id, announced.figures_seq) {
                continue;
            }
            self.work.fit_evaluations += 1;
            if fits(&announced.room.drained(), &candidate.need).is_err() {
                continue;
            }
            let ours = holding.get(executor_id.as_str()).copied().unwrap_or(0);
            let free_of_holds = announced.room.holds.is_empty() && ours == 0;
            let owed_memory = announced
                .room
                .memory
                .map_or(0, |memory| memory.owed_bytes)
                .saturating_add(announced.room.memory_budget.map_or(0, |b| b.committed));
            choices.push(HoldChoice {
                key: candidate.key,
                holds_other_room: !free_of_holds,
                holding_here: ours,
                committed_cores: announced.room.cores.committed,
                owed_memory,
                executor_id: executor_id.clone(),
                generation: machine.generation,
                seq: announced.seq,
                need: candidate.need.clone(),
            });
        }
        choices.sort_by(|a, b| a.order().cmp(&b.order()));
        for choice in choices {
            if self
                .blocker(&self.entries[entry_id], &choice.executor_id, now_unix_ms)
                .is_some()
            {
                continue;
            }
            if admit(entry_id, &choice.executor_id) {
                effects.absorb(self.assign(
                    entry_id,
                    &choice.executor_id,
                    choice.generation,
                    choice.seq,
                    &choice.need,
                    true,
                    now_unix_ms,
                ));
                return effects;
            }
            // A machine newly turned away may have been all that kept the
            // entry off a room that fits it now: placed, not held for.
            if self.turn_away(entry_id, &choice.executor_id) {
                effects.absorb(self.place(entry_id, now_unix_ms, admit));
                return effects;
            }
        }
        effects
    }

    /// Hand `entry_id` to `executor_id`. A fitting assignment takes its need
    /// from the room, and when the entry passes its own protected hold there,
    /// that hold too, which its claim consumes.
    #[allow(clippy::too_many_arguments)]
    fn assign(
        &mut self,
        entry_id: &str,
        executor_id: &str,
        generation: u64,
        seq: u64,
        need: &Need,
        reserve: bool,
        now_unix_ms: u64,
    ) -> Effects {
        let mut effects = Effects::default();
        let share = if reserve {
            None
        } else {
            let entry = &self.entries[entry_id];
            self.machines
                .get(executor_id)
                .and_then(|machine| machine.room.as_ref())
                .map(|announced| Taken {
                    need: need.clone(),
                    hold: self
                        .passed_hold(entry, executor_id, &announced.left, need, now_unix_ms)
                        .filter(|(hold, _)| hold.entry_id == entry_id)
                        .map(|(hold, hold_need)| (hold.clone(), hold_need.clone())),
                })
        };
        if let Some(share) = share {
            if let Some(announced) = self
                .machines
                .get_mut(executor_id)
                .and_then(|machine| machine.room.as_mut())
            {
                announced.offered.insert(entry_id.to_string(), share);
                announced.recharge();
            }
        }
        let entry = self
            .entries
            .get_mut(entry_id)
            .expect("assigning a held entry");
        let authorized = reserve || entry.reserved_on.as_deref() == Some(executor_id);
        entry.turned_away.remove(executor_id);
        entry.standing = Standing::Assigned {
            executor_id: executor_id.to_string(),
            generation,
        };
        if reserve {
            entry.reserved_on = Some(executor_id.to_string());
        } else if let Some(reserved) = entry
            .reserved_on
            .take_if(|reserved| reserved != executor_id)
        {
            let generation = self.machines.get(&reserved).map_or(0, |m| m.generation);
            effects
                .released
                .push((entry_id.to_string(), reserved, generation));
        }
        effects.assigned.push(Assignment {
            entry_id: entry_id.to_string(),
            executor_id: executor_id.to_string(),
            generation,
            seq,
            reserve,
            authorized,
        });
        effects
    }

    /// The entries whose offer is out to `executor_id`, on any link: what the
    /// queue hands back when that link ends.
    #[cfg(test)]
    pub(crate) fn offered_on(&self, executor_id: &str) -> Vec<EntryId> {
        let mut offered: Vec<EntryId> = self
            .entries
            .iter()
            .filter(|(_, entry)| {
                matches!(&entry.standing, Standing::Assigned { executor_id: on, .. } if on == executor_id)
            })
            .map(|(entry_id, _)| entry_id.clone())
            .collect();
        offered.sort();
        offered
    }

    /// Whether `entry_id` waits on `executor_id` only for the shares that
    /// machine kept after refusing twice under the figures it still
    /// announces (cairn/7339 comment 11): what is left of its room does not
    /// fit the entry's need, and the room less its live offers and claims
    /// alone would. Its next announcement replaces those shares.
    pub(crate) fn kept_after_second_refusal(
        &self,
        entry_id: &str,
        executor_id: &str,
        now_unix_ms: u64,
    ) -> bool {
        let (Some(entry), Some(announced)) = (
            self.entries.get(entry_id),
            self.machines
                .get(executor_id)
                .and_then(|machine| machine.room.as_ref()),
        ) else {
            return false;
        };
        let Some(candidate) = entry.waiting.candidates.get(executor_id) else {
            return false;
        };
        !announced.refused.is_empty()
            && !self.fits_left(
                entry,
                executor_id,
                &announced.left,
                &candidate.need,
                now_unix_ms,
            )
            && self.fits_left(
                entry,
                executor_id,
                &announced.left_without_refused(),
                &candidate.need,
                now_unix_ms,
            )
    }

    /// Every (entry, machine) pair that breaks the invariant: a waiting entry
    /// whose need fits an eligible machine's room at `now`, less what live
    /// offers and claims took from it, which it was not refused under and
    /// which is not the named exception below; an entitled waiting entry
    /// holding no room whose need an eligible machine's drained room fits,
    /// which it was not refused under; and an offer that has ended whose
    /// share of a room is still kept. A machine away, one `admit` turned the
    /// entry away from under the figures it still announces, or one a better
    /// machine keeps the entry off ([`Self::blocker`]), is owed neither; an
    /// entry holding room on a machine it is kept off holds none. The one
    /// exception is an entry the room is short of only by the shares its
    /// machine kept after refusing twice under these figures
    /// ([`Self::kept_after_second_refusal`]): that machine is owed nothing
    /// more until it announces. So every waiting entry is placeable nowhere,
    /// held for a named better machine that could hold it, refused or turned
    /// away there under the current figures, or waiting for a machine that
    /// refused twice to announce. Empty at rest, whatever `admit` says.
    #[cfg(test)]
    pub(crate) fn stranded(&self, now_unix_ms: u64) -> Vec<(EntryId, MachineId)> {
        let mut stranded = Vec::new();
        let mut measured: HashMap<&str, AnnouncedRoom> = HashMap::new();
        for (executor_id, machine) in &self.machines {
            let Some(announced) = machine.room.as_ref() else {
                continue;
            };
            let mut live = announced.clone();
            live.offered.retain(|entry_id, _| {
                let out = self.entries.get(entry_id).is_some_and(|entry| {
                    entry.standing
                        == Standing::Assigned {
                            executor_id: executor_id.clone(),
                            generation: machine.generation,
                        }
                });
                if !out {
                    stranded.push((entry_id.clone(), executor_id.clone()));
                }
                out
            });
            // Measured without the refused shares: an entry short only of
            // those is told apart below, by name.
            live.refused.clear();
            live.recharge();
            measured.insert(executor_id.as_str(), live.left);
        }
        for (entry_id, entry) in &self.entries {
            if entry.standing != Standing::Waiting {
                continue;
            }
            for (executor_id, candidate) in &entry.waiting.candidates {
                let (Some(announced), Some(left)) = (
                    self.machines
                        .get(executor_id)
                        .and_then(|machine| machine.room.as_ref()),
                    measured.get(executor_id.as_str()),
                ) else {
                    continue;
                };
                if candidate.away
                    || entry.refused_under(executor_id, announced.figures_seq)
                    || entry.turned_away_under(executor_id, announced.figures_seq)
                    || self.blocker(entry, executor_id, now_unix_ms).is_some()
                {
                    continue;
                }
                let held = entry
                    .reserved_on
                    .as_deref()
                    .is_some_and(|reserved| self.blocker(entry, reserved, now_unix_ms).is_none());
                let unheld = self.entitled.contains(entry_id)
                    && !held
                    && fits(&announced.room.drained(), &candidate.need).is_ok();
                let fits = self.fits_left(entry, executor_id, left, &candidate.need, now_unix_ms);
                // The named exception: the machine refused twice under these
                // figures with no claim between and keeps the second share,
                // so it is owed nothing more until it announces.
                let refused_twice =
                    fits && self.kept_after_second_refusal(entry_id, executor_id, now_unix_ms);
                if unheld || (fits && !refused_twice) {
                    stranded.push((entry_id.clone(), executor_id.clone()));
                }
            }
        }
        stranded.sort();
        stranded
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::{
        Allowance, CellCommandClass, CompileSlots, DiskRoom, MemoryRoom, ResourceReservation,
        ResourceReservationSource,
    };
    use cairn_common::pipe_health::PipeFloorWork;
    use rand::{Rng, SeedableRng};

    const GIB: u64 = 1 << 30;
    const HOUR: u64 = 3_600_000;

    fn room(cores: u64, memory_gib: u64) -> AnnouncedRoom {
        AnnouncedRoom {
            slots: CompileSlots::of(u32::try_from(cores).unwrap()),
            compiling: 0,
            test_cores: 0,
            standing: Default::default(),
            cores: Allowance {
                limit: cores,
                committed: 0,
            },
            memory: Some(MemoryRoom {
                available_bytes: memory_gib * GIB,
                owed_bytes: 0,
                total_bytes: memory_gib * GIB,
            }),
            memory_budget: None,
            disk: Some(DiskRoom {
                free_bytes: 1_000 * GIB,
                watermark_bytes: 10 * GIB,
                reserved_bytes: 0,
                inodes: None,
            }),
            disk_budget: None,
            reclaimable_memory_bytes: 0,
            alone: None,
            provisioning: Vec::new(),
            holds: Vec::new(),
            closures: Vec::new(),
        }
    }

    fn need(cores: u32, memory_gib: u64) -> Need {
        Need::of(
            &ResourceReservation {
                memory_bytes: memory_gib * GIB,
                disk_growth_bytes: GIB,
                concurrency_units: cores,
                source: ResourceReservationSource::Learned,
                ..ResourceReservation::default()
            },
            CellCommandClass::Other,
            false,
            PipeFloorWork::Process,
            true,
            None,
            CompileSlots::of(8),
        )
    }

    fn waiting(id: usize, machines: &[&str], need: &Need) -> Waiting {
        Waiting {
            entry_id: format!("e{id}"),
            standing: false,
            priority: CellPriority::ReviewCheck,
            queued_at_unix_ms: 1_000 + id as u64,
            sequence: id as u64,
            horizon_unix_ms: 10 * HOUR,
            reserve_from_unix_ms: 5 * HOUR,
            candidates: machines
                .iter()
                .map(|machine| {
                    (
                        (*machine).to_string(),
                        Candidate {
                            need: need.clone(),
                            warm: false,
                            key: HardKey::default(),
                            away: false,
                        },
                    )
                })
                .collect(),
            may_spill: true,
        }
    }

    #[test]
    fn an_idle_twelve_unit_machine_with_two_residents_admits_a_compile() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("mac", 1);
        let mut idle = room(12, 32);
        idle.cores.committed = 2;
        idle.standing.cores = 2;
        idle.slots = CompileSlots::net(12, idle.standing.cores);
        let compile = Need::of(
            &ResourceReservation {
                concurrency_units: 1,
                memory_bytes: GIB,
                ..ResourceReservation::default()
            },
            CellCommandClass::CargoTest,
            false,
            PipeFloorWork::Process,
            true,
            None,
            CompileSlots::of(12),
        );
        dispatcher.set_room("mac", 1, 1, idle, 0, &mut yes());
        let effects = dispatcher.enqueue(waiting(0, &["mac"], &compile), 0, &mut yes());
        assert_eq!(offered(&effects), vec!["e0"]);
        assert!(!effects.assigned[0].reserve);
    }

    #[test]
    fn a_standalone_test_phase_releases_a_compile_slot_for_waiting_work() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("m", 1);
        let mut compiling = room(20, 64);
        compiling.cores.committed = 20;
        compiling.compiling = 2;
        assert_eq!(
            compiling.slots,
            CompileSlots {
                slots: 2,
                width: 10
            }
        );
        assert_eq!(compiling.free_slots(), 0);
        let compile = Need::of(
            &ResourceReservation {
                concurrency_units: 10,
                memory_bytes: GIB,
                ..ResourceReservation::default()
            },
            CellCommandClass::CargoTest,
            false,
            PipeFloorWork::Process,
            true,
            None,
            compiling.slots,
        );
        assert!(compile.compile);
        assert_eq!(compile.cores, 10);
        dispatcher.set_room("m", 1, 1, compiling.clone(), 0, &mut yes());
        assert!(dispatcher
            .enqueue(waiting(0, &["m"], &compile), 0, &mut yes())
            .assigned
            .is_empty());

        // One lane still compiles at width 10; the standalone test process
        // holds one core, but no compile slot.
        let mut testing = compiling;
        testing.cores.committed = 11;
        testing.compiling = 1;
        testing.test_cores = 1;
        assert_eq!(testing.free_slots(), 1);
        let effects = dispatcher.set_room("m", 1, 2, testing.clone(), 1, &mut yes());
        assert_eq!(offered(&effects), vec!["e0"]);
        assert!(!effects.assigned[0].reserve);
        assert_eq!(dispatcher.candidates("e0").unwrap()["m"].need.cores, 10);
        assert_eq!(dispatcher.room("m").unwrap().2.free_slots(), 0);

        dispatcher.claimed("e0", "m");
        // The next announcement already includes the claim, so it must
        // replace the dispatcher's provisional charge rather than add to it.
        testing.charge(&compile);
        assert_eq!(testing.compiling, 2);
        assert_eq!(testing.cores.committed, 21);
        let effects = dispatcher.set_room("m", 1, 3, testing.clone(), 2, &mut yes());
        assert!(effects.assigned.is_empty());
        assert_eq!(dispatcher.room("m").unwrap().2, &testing);
        assert_eq!(dispatcher.room("m").unwrap().2.free_slots(), 0);
    }

    #[test]
    fn a_lane_larger_than_resident_net_capacity_never_holds_room() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("mac", 1);
        let mut idle = room(12, 32);
        idle.cores.committed = 2;
        idle.standing.cores = 2;
        idle.slots = CompileSlots::net(12, idle.standing.cores);
        dispatcher.set_room("mac", 1, 1, idle, 0, &mut yes());
        let mut impossible = waiting(0, &["mac"], &need(11, 1));
        impossible.standing = true;
        impossible.reserve_from_unix_ms = 0;
        let effects = dispatcher.enqueue(impossible, 0, &mut yes());
        assert!(
            effects.assigned.is_empty(),
            "impossible work must not reserve"
        );
        assert_eq!(dispatcher.reserved_on("e0"), None);
        let other = dispatcher.enqueue(waiting(1, &["mac"], &need(1, 1)), 0, &mut yes());
        assert_eq!(offered(&other), vec!["e1"]);
    }

    #[test]
    fn a_resident_change_reprices_a_waiting_compile() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("mac", 1);
        let mut occupied = room(12, 32);
        occupied.cores.committed = 12;
        occupied.compiling = occupied.slots.slots;
        let mut compile = need(1, 1);
        compile.compile = true;
        compile = compile.with_slots(occupied.slots);
        dispatcher.set_room("mac", 1, 1, occupied, 0, &mut yes());
        assert!(dispatcher
            .enqueue(waiting(0, &["mac"], &compile), 0, &mut yes())
            .assigned
            .is_empty());

        let mut idle = room(12, 32);
        idle.standing.cores = 2;
        idle.cores.committed = 2;
        idle.slots = CompileSlots::net(12, 2);
        let effects = dispatcher.set_room("mac", 1, 2, idle, 0, &mut yes());
        assert_eq!(offered(&effects), vec!["e0"]);
        assert_eq!(dispatcher.candidates("e0").unwrap()["mac"].need.cores, 10);
        assert!(!effects.assigned[0].reserve);
    }

    #[test]
    fn resident_growth_releases_a_hold_that_can_no_longer_fit() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("mac", 1);
        let mut occupied = room(12, 32);
        occupied.cores.committed = 12;
        dispatcher.set_room("mac", 1, 1, occupied, 0, &mut yes());
        let mut senior = waiting(0, &["mac"], &need(11, 1));
        senior.standing = true;
        senior.reserve_from_unix_ms = 0;
        let held = dispatcher.enqueue(senior, 0, &mut yes());
        assert!(held.assigned[0].reserve);
        dispatcher.decline("e0", "mac", 1, 0, &mut yes());

        let mut idle = room(12, 32);
        idle.standing.cores = 2;
        idle.cores.committed = 2;
        idle.slots = CompileSlots::net(12, 2);
        let effects = dispatcher.set_room("mac", 1, 2, idle, 0, &mut yes());
        assert_eq!(dispatcher.reserved_on("e0"), None);
        assert!(effects
            .released
            .iter()
            .any(|(id, machine, _)| id == "e0" && machine == "mac"));
        assert!(effects.assigned.is_empty());
        assert_eq!(
            offered(&dispatcher.enqueue(waiting(1, &["mac"], &need(1, 1)), 0, &mut yes())),
            vec!["e1"]
        );
    }

    fn yes() -> impl FnMut(&str, &str) -> bool {
        |_, _| true
    }

    fn offered(effects: &Effects) -> Vec<&str> {
        effects
            .assigned
            .iter()
            .map(|assignment| assignment.entry_id.as_str())
            .collect()
    }

    /// `room`, net of a protected hold for entry `entry` needing `need`, as
    /// the machine announces it.
    fn holding(mut room: AnnouncedRoom, entry: usize, need: &Need) -> AnnouncedRoom {
        room.charge(need);
        room.holds.push(HeadroomRoomHold {
            entry_id: format!("e{entry}"),
            request_id: format!("r{entry}"),
            memory_bytes: need.memory_bytes,
            cores: need.cores,
            disk_bytes: need.disk_bytes,
            protected: true,
        });
        room
    }

    /// The room a machine holds for an entry is the entry's: offered there,
    /// it is judged with its own hold given back, as the machine's claim
    /// sets the hold aside, and nobody else is (the cairn/7339 deadlock).
    #[test]
    fn an_entry_is_offered_into_its_own_hold() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("m", 1);
        let held = need(8, 40);
        dispatcher.enqueue(waiting(0, &["m"], &need(1, 30)), 0, &mut yes());
        dispatcher.enqueue(waiting(1, &["m"], &held), 0, &mut yes());
        let effects =
            dispatcher.set_room("m", 1, 1, holding(room(32, 64), 1, &held), 0, &mut yes());
        let assigned: Vec<_> = effects
            .assigned
            .iter()
            .map(|a| a.entry_id.as_str())
            .collect();
        assert_eq!(assigned, vec!["e1"], "24 GiB is left beside the hold");
        assert!(dispatcher.stranded(0).is_empty());
        let (_, announced, left) = dispatcher.room("m").unwrap();
        assert!(left.holds.is_empty(), "its claim consumes the hold");
        assert_eq!(left.memory, announced.memory);
    }

    /// An entitled entry that has waited longer than the entry a machine
    /// protects room for passes that hold where the drained room fits it,
    /// and its offer carries the authorization the machine judges that by.
    #[test]
    fn a_senior_entry_passes_a_junior_hold() {
        let now = 6 * HOUR;
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("m", 1);
        let mut busy = room(32, 64);
        busy.cores.committed = 32;
        dispatcher.set_room("m", 1, 1, busy, now, &mut yes());
        let each = need(8, 40);
        for id in [0, 5] {
            let effects = dispatcher.enqueue(waiting(id, &["m"], &each), now, &mut yes());
            assert!(effects.assigned[0].reserve && effects.assigned[0].authorized);
            dispatcher.decline(&format!("e{id}"), "m", 1, now, &mut yes());
        }
        let effects =
            dispatcher.set_room("m", 1, 2, holding(room(32, 64), 5, &each), now, &mut yes());
        assert_eq!(effects.assigned.len(), 1);
        let offer = &effects.assigned[0];
        assert_eq!(offer.entry_id, "e0", "e0 has waited longer than e5");
        assert!(!offer.reserve && offer.authorized);
        assert!(dispatcher.stranded(now).is_empty());

        // Not yet entitled, the same entry waits behind the hold.
        let mut early = Dispatcher::default();
        early.attach("m", 1);
        early.enqueue(waiting(5, &["m"], &each), 0, &mut yes());
        early.decline("e5", "m", 1, 0, &mut yes());
        early.enqueue(waiting(0, &["m"], &each), 0, &mut yes());
        let effects = early.set_room("m", 1, 1, holding(room(32, 64), 5, &each), 0, &mut yes());
        assert_eq!(effects, Effects::default());
        assert!(early.stranded(0).is_empty());
    }

    /// An offer that was never sent gives its room back while the room it
    /// was taken from stands, and the next entry gets it.
    #[test]
    fn a_requeue_gives_its_room_back() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("m", 1);
        let each = need(8, 8);
        dispatcher.enqueue(waiting(0, &["m"], &each), 0, &mut yes());
        dispatcher.enqueue(waiting(1, &["m"], &each), 0, &mut yes());
        let effects = dispatcher.set_room("m", 1, 1, room(8, 64), 0, &mut yes());
        assert_eq!(effects.assigned.len(), 1);
        assert_eq!(effects.assigned[0].entry_id, "e0");
        let mut not_e0 = |entry_id: &str, _: &str| entry_id != "e0";
        let effects = dispatcher.requeue("e0", 0, &mut not_e0);
        let assigned: Vec<_> = effects
            .assigned
            .iter()
            .map(|a| a.entry_id.as_str())
            .collect();
        assert_eq!(assigned, vec!["e1"], "e0's room went to e1");
        assert!(dispatcher.stranded(0).is_empty());
    }

    /// A refusal stands while the machine announces the figures it was given
    /// under: a frame repeating them moves the seq, yet neither the walk nor
    /// an entry's own deadline asks again, and nothing calls it superseded.
    /// New figures supersede it.
    #[test]
    fn a_refusal_stands_until_the_figures_it_was_given_under_change() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("m", 1);
        dispatcher.set_room("m", 1, 1, room(32, 64), 0, &mut yes());
        let effects = dispatcher.enqueue(waiting(0, &["m"], &need(1, 8)), 0, &mut yes());
        assert_eq!(effects.assigned.len(), 1);
        dispatcher.decline("e0", "m", 1, 0, &mut yes());

        let effects = dispatcher.set_room("m", 1, 2, room(32, 64), 0, &mut yes());
        assert_eq!(effects, Effects::default());
        assert!(dispatcher.refusal_stands("e0", "m"));
        let effects = dispatcher.deadline_reached(5 * HOUR, &mut yes());
        assert!(effects.assigned.is_empty(), "{effects:?}");
        assert!(dispatcher.stranded(5 * HOUR).is_empty());

        let effects = dispatcher.set_room("m", 1, 3, room(32, 48), 5 * HOUR, &mut yes());
        assert!(!dispatcher.refusal_stands("e0", "m"));
        assert_eq!(effects.assigned.len(), 1);
        assert_eq!(effects.assigned[0].seq, 3);
    }

    /// A refusal belongs to the process that gave it. An executor that
    /// restarts comes back on a new link and counts its room from 1 again, so
    /// a refusal its old process gave under room 6851 says nothing about the
    /// new one's room 1: the entry is offered there, and nothing calls the old
    /// refusal standing.
    #[test]
    fn a_restarted_machine_has_refused_nothing() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("m", 1);
        dispatcher.set_room("m", 1, 6_851, room(32, 64), 0, &mut yes());
        let effects = dispatcher.enqueue(waiting(0, &["m"], &need(1, 8)), 0, &mut yes());
        assert_eq!(offered(&effects), vec!["e0"]);
        dispatcher.decline("e0", "m", 6_851, 0, &mut yes());
        assert!(dispatcher.refusal_stands("e0", "m"));

        dispatcher.detach("m", 1, 0, &mut yes());
        dispatcher.attach("m", 2);
        let effects = dispatcher.set_room("m", 2, 1, room(32, 64), 0, &mut yes());
        assert!(!dispatcher.refusal_stands("e0", "m"));
        assert_eq!(offered(&effects), vec!["e0"], "the new process is asked");
        assert!(dispatcher.stranded(0).is_empty());
    }

    /// The per-room bound (cairn/7339 comment 2, restated by comment 11): a
    /// machine refusing everything is offered what one room fits, and one
    /// more for the share its first refusal gives back, whatever is waiting.
    /// A second refusal with no claim between keeps its share while the
    /// machine repeats its figures; new figures offer the oldest again.
    #[test]
    fn a_machine_refusing_everything_is_offered_what_its_room_fits_and_one_more() {
        for k in [1, 200] {
            let mut dispatcher = Dispatcher::default();
            dispatcher.attach("m", 1);
            let each = need(8, 8);
            for id in 0..k {
                dispatcher.enqueue(waiting(id, &["m"], &each), 0, &mut yes());
            }
            // Sixteen cores fit two eight-core entries.
            let fits = 2;
            let effects = dispatcher.set_room("m", 1, 1, room(16, 64), 0, &mut yes());
            let mut out: Vec<String> = offered(&effects).into_iter().map(String::from).collect();
            let mut offers = out.len();
            while let Some(entry_id) = out.pop() {
                let effects = dispatcher.decline(&entry_id, "m", 1, 0, &mut yes());
                offers += effects.assigned.len();
                out.extend(offered(&effects).into_iter().map(String::from));
                assert!(dispatcher.stranded(0).is_empty(), "k = {k}");
            }
            assert_eq!(
                offers,
                k.min(fits + 1),
                "k = {k}: what the room fits, and one more"
            );
            assert_eq!(
                dispatcher.kept_after_second_refusal("e3", "m", 0),
                k > fits + 1,
                "k = {k}: the next entry waits for the shares kept"
            );
            let repeated = dispatcher.set_room("m", 1, 2, room(16, 64), 0, &mut yes());
            assert_eq!(repeated, Effects::default(), "k = {k}: the same figures");
            let moved = dispatcher.set_room("m", 1, 3, room(16, 63), 0, &mut yes());
            assert!(!dispatcher.kept_after_second_refusal("e3", "m", 0));
            assert_eq!(
                offered(&moved),
                ["e0", "e1"][..k.min(fits)].to_vec(),
                "k = {k}: oldest first"
            );
            assert!(dispatcher.stranded(0).is_empty(), "k = {k}");
        }
    }

    /// An idle machine that refuses one entry for a reason of the entry's own
    /// (one its room does not show) gives the entry's share back: the next
    /// entry that fits is offered, and claimed, with no new announcement
    /// (cairn/7339 comment 11). A claim resets the count of refusals, so the
    /// refusal after it gives its share back too.
    #[test]
    fn an_idle_machine_refusing_one_entry_is_offered_the_next_that_fits() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("m", 1);
        let each = need(8, 8);
        for id in 0..4 {
            dispatcher.enqueue(waiting(id, &["m"], &each), 0, &mut yes());
        }
        let effects = dispatcher.set_room("m", 1, 1, room(16, 64), 0, &mut yes());
        assert_eq!(offered(&effects), vec!["e0", "e1"]);
        let effects = dispatcher.decline("e0", "m", 1, 0, &mut yes());
        assert_eq!(offered(&effects), vec!["e2"], "e0's share goes to e2");
        assert_eq!(dispatcher.claimed("e2", "m"), Effects::default());
        assert!(dispatcher.stranded(0).is_empty());
        let effects = dispatcher.decline("e1", "m", 1, 0, &mut yes());
        assert_eq!(
            offered(&effects),
            vec!["e3"],
            "the claim between the refusals reset their count"
        );
        assert!(dispatcher.stranded(0).is_empty());
    }

    /// An offer withdrawn before any claim (the work was cancelled) gives its
    /// room back, and the next entry that fits it is offered.
    #[test]
    fn a_withdrawn_offer_gives_its_room_to_the_next_entry() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("m", 1);
        let each = need(8, 8);
        dispatcher.enqueue(waiting(0, &["m"], &each), 0, &mut yes());
        dispatcher.enqueue(waiting(1, &["m"], &each), 0, &mut yes());
        let effects = dispatcher.set_room("m", 1, 1, room(8, 64), 0, &mut yes());
        assert_eq!(offered(&effects), vec!["e0"]);
        let withdrawn = dispatcher.withdraw("e0", 0, &mut yes());
        let walked = dispatcher.rewalk("m", 0, &mut yes());
        let mut asked = offered(&withdrawn);
        asked.extend(offered(&walked));
        assert_eq!(asked, vec!["e1"]);
        assert!(dispatcher.stranded(0).is_empty());
    }

    /// An entitled entry that no machine could hold even drained when its
    /// entitlement arrived (a brake stood) asks for headroom as soon as a
    /// machine it may run on announces figures that could hold it, rather
    /// than waiting for an unrelated event while younger work takes the room
    /// that machine frees (tenet 1444).
    #[test]
    fn an_entitled_entry_asks_for_headroom_when_a_brake_lifts() {
        use cairn_common::executor_protocol::{DeclineReason, RoomClosure, RoomScope};
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("m", 1);
        let mut busy = room(32, 128);
        busy.cores.committed = 32;
        let mut braked = busy.clone();
        braked.closures.push(RoomClosure {
            scope: RoomScope::NonInteractive,
            reason: DeclineReason::Pressure,
            detail: None,
            disk: None,
            conditions: Vec::new(),
        });
        dispatcher.set_room("m", 1, 1, braked, 0, &mut yes());
        dispatcher.enqueue(waiting(0, &["m"], &need(8, 8)), 0, &mut yes());
        let effects = dispatcher.deadline_reached(5 * HOUR, &mut yes());
        assert_eq!(
            effects,
            Effects::default(),
            "the brake leaves no room to hold"
        );

        let effects = dispatcher.set_room("m", 1, 2, busy, 5 * HOUR, &mut yes());
        assert_eq!(offered(&effects), vec!["e0"]);
        assert!(effects.assigned[0].reserve, "{effects:?}");
        assert_eq!(dispatcher.reserved_on("e0"), Some("m"));
    }

    /// A room no entry fits, and an unchanged room re-announced, cost nothing
    /// per waiting entry: no offers, and no fit evaluated when the figures
    /// did not change (cairn/7303 property 1, and the per-room bound).
    #[test]
    fn an_unchanged_room_matches_nothing_and_a_full_room_offers_nothing() {
        for k in [1, 200] {
            let mut dispatcher = Dispatcher::default();
            dispatcher.attach("m", 1);
            let big = need(4, 64);
            for id in 0..k {
                let effects = dispatcher.enqueue(waiting(id, &["m"], &big), 0, &mut yes());
                assert!(effects.assigned.is_empty());
            }
            let small = room(32, 16);
            let effects = dispatcher.set_room("m", 1, 1, small.clone(), 0, &mut yes());
            assert_eq!(effects, Effects::default(), "k = {k}: nothing fits");
            dispatcher.take_work();
            for seq in 2..1_000 {
                let effects = dispatcher.set_room("m", 1, seq, small.clone(), 0, &mut yes());
                assert_eq!(effects, Effects::default());
            }
            assert_eq!(dispatcher.take_work(), Work::default(), "k = {k}");
            assert!(dispatcher.stranded(0).is_empty());
        }
    }

    /// K entries that each fit a room of one: one offer per announcement,
    /// whatever K is.
    #[test]
    fn a_room_of_one_offers_one_whatever_is_waiting() {
        for k in [1, 200] {
            let mut dispatcher = Dispatcher::default();
            dispatcher.attach("m", 1);
            let each = need(8, 8);
            for id in 0..k {
                dispatcher.enqueue(waiting(id, &["m"], &each), 0, &mut yes());
            }
            let effects = dispatcher.set_room("m", 1, 1, room(8, 12), 0, &mut yes());
            assert_eq!(effects.assigned.len(), 1, "k = {k}");
            assert_eq!(effects.assigned[0].entry_id, "e0", "oldest first");
            assert!(dispatcher.stranded(0).is_empty());
        }
    }

    /// A refusal is paced by room: the refused entry is not offered there
    /// again under the same room, however often anything else happens, and
    /// is offered once more under a newer room that fits it (property 2).
    #[test]
    fn a_refusal_waits_for_a_newer_room() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("m", 1);
        let fits_once = room(8, 32);
        let mut offers = 0;
        offers += dispatcher
            .set_room("m", 1, 1, fits_once.clone(), 0, &mut yes())
            .assigned
            .len();
        offers += dispatcher
            .enqueue(waiting(0, &["m"], &need(4, 8)), 0, &mut yes())
            .assigned
            .len();
        assert_eq!(offers, 1);
        offers += dispatcher
            .decline("e0", "m", 1, 0, &mut yes())
            .assigned
            .len();
        for _ in 0..300 {
            offers += dispatcher
                .set_room("m", 1, 1, fits_once.clone(), 0, &mut yes())
                .assigned
                .len();
        }
        assert_eq!(offers, 1, "no re-offer under the room it was refused in");
        let mut moved = fits_once.clone();
        moved.cores.committed = 1;
        offers += dispatcher
            .set_room("m", 1, 2, moved, 0, &mut yes())
            .assigned
            .len();
        assert_eq!(offers, 2, "one more under a newer room");
    }

    /// A room from one machine evaluates only the entries that may run there
    /// (property 4). A machine's first refusal under its figures walks it for
    /// the share given back; a second, which keeps its share, evaluates only
    /// the declined entry.
    #[test]
    fn a_room_touches_only_its_machines_entries_and_a_kept_refusal_only_its_own() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("a", 1);
        dispatcher.attach("b", 1);
        let big = need(4, 64);
        for id in 0..50 {
            dispatcher.enqueue(waiting(id, &["b"], &big), 0, &mut yes());
        }
        dispatcher.enqueue(waiting(50, &["a"], &big), 0, &mut yes());
        dispatcher.take_work();
        dispatcher.set_room("a", 1, 1, room(32, 16), 0, &mut yes());
        assert_eq!(dispatcher.take_work().fit_evaluations, 1);
        dispatcher.set_room("b", 1, 1, room(32, 128), 0, &mut yes());
        let walked = dispatcher.take_work();
        assert!(walked.fit_evaluations >= 50);
        let given_back = dispatcher.decline("e0", "b", 1, 0, &mut yes());
        assert_eq!(offered(&given_back), vec!["e2"]);
        assert_eq!(
            dispatcher.take_work().passes,
            2,
            "e0 placed again, and b walked"
        );
        assert!(dispatcher
            .decline("e1", "b", 1, 0, &mut yes())
            .assigned
            .is_empty());
        assert_eq!(
            dispatcher.take_work(),
            Work {
                passes: 1,
                fit_evaluations: 0
            },
            "b refused e1 under 1, and keeps its share"
        );
    }

    /// An entitled entry that fits no room holds room on a machine chosen by
    /// fact: one holding no other entry's room before the alphabetically
    /// first one that does.
    #[test]
    fn headroom_goes_to_a_machine_holding_none_not_the_first_name() {
        let mut dispatcher = Dispatcher::default();
        for machine in ["bbs", "fedora"] {
            dispatcher.attach(machine, 1);
            let mut busy = room(32, 128);
            busy.cores.committed = 32;
            dispatcher.set_room(machine, 1, 1, busy, 0, &mut yes());
        }
        let mut first = waiting(0, &["bbs", "fedora"], &need(8, 16));
        first.standing = true;
        let mut second = waiting(1, &["bbs", "fedora"], &need(8, 16));
        second.standing = true;
        let a = dispatcher.enqueue(first, 0, &mut yes());
        assert_eq!(a.assigned[0].executor_id, "bbs");
        assert!(a.assigned[0].reserve);
        dispatcher.decline("e0", "bbs", 1, 0, &mut yes());
        let b = dispatcher.enqueue(second, 0, &mut yes());
        assert_eq!(b.assigned[0].executor_id, "fedora", "one hold per machine");
    }

    #[test]
    fn a_horizon_expires_its_entry_and_an_entitlement_reserves() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("m", 1);
        let mut busy = room(32, 128);
        busy.cores.committed = 32;
        dispatcher.set_room("m", 1, 1, busy, 0, &mut yes());
        dispatcher.enqueue(waiting(0, &["m"], &need(8, 8)), 0, &mut yes());
        assert_eq!(dispatcher.next_deadline(), Some(5 * HOUR));
        let effects = dispatcher.deadline_reached(5 * HOUR, &mut yes());
        assert!(effects.assigned[0].reserve);
        dispatcher.decline("e0", "m", 1, 5 * HOUR, &mut yes());
        let effects = dispatcher.deadline_reached(10 * HOUR, &mut yes());
        assert_eq!(effects.expired, vec!["e0".to_string()]);
        assert_eq!(effects.released.len(), 1);
        assert!(!dispatcher.contains("e0"));
    }

    const CLAMPED: HardKey = HardKey {
        deferred: false,
        unmeasured_capacity: None,
        clamped: true,
    };
    const DEFERRED: HardKey = HardKey {
        deferred: true,
        unmeasured_capacity: None,
        clamped: false,
    };

    fn unmeasured(gib: u64) -> HardKey {
        HardKey {
            unmeasured_capacity: Some(Some(gib * GIB)),
            ..HardKey::default()
        }
    }

    /// `w` with machine `machine`'s place on the hard key set to `key`.
    fn keyed(mut w: Waiting, machine: &str, key: HardKey) -> Waiting {
        w.candidates.get_mut(machine).unwrap().key = key;
        w
    }

    /// A room of `cores` cores, every one given out.
    fn busy(cores: u64, memory_gib: u64) -> AnnouncedRoom {
        let mut busy = room(cores, memory_gib);
        busy.cores.committed = cores;
        busy
    }

    fn assigned_to(effects: &Effects) -> Vec<(&str, &str)> {
        effects
            .assigned
            .iter()
            .map(|a| (a.entry_id.as_str(), a.executor_id.as_str()))
            .collect()
    }

    /// A machine on which the work would run deferred on a class prior, or
    /// clamped below its measured demand, does not take it while a machine
    /// ahead of it on the hard key could hold it, though that machine is
    /// busy and the disfavoured one idle. The better machine is offered it
    /// the moment it announces room that fits.
    #[test]
    fn a_disfavoured_machine_does_not_take_work_a_busy_better_machine_could_hold() {
        for key in [CLAMPED, DEFERRED] {
            let mut dispatcher = Dispatcher::default();
            dispatcher.attach("big", 1);
            dispatcher.attach("small", 1);
            dispatcher.set_room("big", 1, 1, busy(32, 128), 0, &mut yes());
            let entry = keyed(waiting(0, &["big", "small"], &need(8, 8)), "small", key);
            let joined = dispatcher.enqueue(entry, 0, &mut yes());
            let announced = dispatcher.set_room("small", 1, 1, room(32, 64), 0, &mut yes());
            assert_eq!(
                (assigned_to(&joined), assigned_to(&announced)),
                (vec![], vec![]),
                "{key:?}: the idle small machine waits behind the busy big one"
            );
            assert!(dispatcher.stranded(0).is_empty());
            let freed = dispatcher.set_room("big", 1, 2, room(32, 128), 0, &mut yes());
            assert_eq!(assigned_to(&freed), vec![("e0", "big")], "{key:?}");
        }
    }

    /// A Rust compile nothing has measured goes to the machine that can give
    /// one process the most, not to a smaller one that is idle now.
    #[test]
    fn an_unmeasured_compile_waits_for_the_largest_single_process_capacity() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("big", 1);
        dispatcher.attach("small", 1);
        dispatcher.set_room("big", 1, 1, busy(32, 128), 0, &mut yes());
        dispatcher.set_room("small", 1, 1, room(32, 64), 0, &mut yes());
        let entry = keyed(
            keyed(
                waiting(0, &["big", "small"], &need(8, 8)),
                "big",
                unmeasured(120),
            ),
            "small",
            unmeasured(12),
        );
        let joined = dispatcher.enqueue(entry, 0, &mut yes());
        assert_eq!(assigned_to(&joined), vec![]);
        assert!(dispatcher.stranded(0).is_empty());
        let freed = dispatcher.set_room("big", 1, 2, room(32, 128), 0, &mut yes());
        assert_eq!(assigned_to(&freed), vec![("e0", "big")]);
    }

    /// Warmth orders machines only within one hard key: a warm machine that
    /// would clamp the work loses to a cold one that holds it.
    #[test]
    fn warmth_does_not_cross_a_hard_key() {
        let mut dispatcher = Dispatcher::default();
        for machine in ["big", "small"] {
            dispatcher.attach(machine, 1);
            dispatcher.set_room(machine, 1, 1, room(32, 128), 0, &mut yes());
        }
        let mut entry = keyed(waiting(0, &["big", "small"], &need(8, 8)), "small", CLAMPED);
        entry.candidates.get_mut("small").unwrap().warm = true;
        let joined = dispatcher.enqueue(entry, 0, &mut yes());
        assert_eq!(assigned_to(&joined), vec![("e0", "big")]);
    }

    /// An entitled entry held off a disfavoured machine holds room on the
    /// better one, ahead of any preference for the less committed machine.
    #[test]
    fn a_held_off_entitled_entry_holds_room_on_the_better_machine() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("big", 1);
        dispatcher.attach("small", 1);
        dispatcher.set_room("big", 1, 1, busy(32, 128), 0, &mut yes());
        let mut half = room(32, 128);
        half.cores.committed = 16;
        dispatcher.set_room("small", 1, 1, half, 0, &mut yes());
        let mut entry = keyed(
            waiting(0, &["big", "small"], &need(24, 8)),
            "small",
            CLAMPED,
        );
        entry.standing = true;
        let joined = dispatcher.enqueue(entry, 0, &mut yes());
        assert_eq!(assigned_to(&joined), vec![("e0", "big")]);
        assert!(joined.assigned[0].reserve);
        assert_eq!(dispatcher.reserved_on("e0"), Some("big"));
    }

    /// The better machine's link ends: nothing keeps the entry from the
    /// disfavoured machine, which takes it at once.
    #[test]
    fn a_held_off_entry_goes_to_the_disfavoured_machine_when_the_better_one_detaches() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("big", 1);
        dispatcher.attach("small", 1);
        dispatcher.set_room("big", 1, 1, busy(32, 128), 0, &mut yes());
        dispatcher.set_room("small", 1, 1, room(32, 64), 0, &mut yes());
        let entry = keyed(waiting(0, &["big", "small"], &need(8, 8)), "small", CLAMPED);
        assert_eq!(dispatcher.enqueue(entry, 0, &mut yes()), Effects::default());
        let detached = dispatcher.detach("big", 1, 0, &mut yes());
        assert_eq!(assigned_to(&detached), vec![("e0", "small")]);
        assert!(dispatcher.stranded(0).is_empty());
    }

    /// A better machine that refused the entry under the figures it still
    /// announces keeps it off the disfavoured machine only until the entry's
    /// reserve-from instant (D1): then the disfavoured machine takes it.
    #[test]
    fn a_refusing_better_machine_holds_the_entry_off_until_its_reserve_from_instant() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("big", 1);
        dispatcher.attach("small", 1);
        dispatcher.set_room("big", 1, 1, room(32, 128), 0, &mut yes());
        dispatcher.set_room("small", 1, 1, room(32, 64), 0, &mut yes());
        let entry = keyed(waiting(0, &["big", "small"], &need(8, 8)), "small", CLAMPED);
        let joined = dispatcher.enqueue(entry, 0, &mut yes());
        assert_eq!(assigned_to(&joined), vec![("e0", "big")]);
        let declined = dispatcher.decline("e0", "big", 1, 0, &mut yes());
        assert_eq!(assigned_to(&declined), vec![], "held off small until 5h");
        assert!(dispatcher.stranded(HOUR).is_empty());
        let entitled = dispatcher.deadline_reached(5 * HOUR, &mut yes());
        assert_eq!(assigned_to(&entitled), vec![("e0", "small")]);
        assert!(!entitled.assigned[0].reserve);
    }

    /// What keeps the entry off the disfavoured machine is whether the better
    /// one could ever hold it: a pressure brake that closes its room now
    /// does not lift that, and a machine too small for the need never had it.
    #[test]
    fn a_braked_better_machine_holds_the_entry_off_and_a_too_small_one_does_not() {
        use cairn_common::executor_protocol::{DeclineReason, RoomClosure, RoomScope};
        let mut braked = room(32, 128);
        braked.closures.push(RoomClosure {
            scope: RoomScope::NonInteractive,
            reason: DeclineReason::Pressure,
            detail: None,
            disk: None,
            conditions: Vec::new(),
        });
        for (big, held_off) in [(braked, true), (room(32, 16), false)] {
            let mut dispatcher = Dispatcher::default();
            dispatcher.attach("big", 1);
            dispatcher.attach("small", 1);
            dispatcher.set_room("big", 1, 1, big, 0, &mut yes());
            dispatcher.set_room("small", 1, 1, room(32, 64), 0, &mut yes());
            let entry = keyed(
                waiting(0, &["big", "small"], &need(8, 40)),
                "small",
                CLAMPED,
            );
            let joined = dispatcher.enqueue(entry, 0, &mut yes());
            let expected = if held_off {
                vec![]
            } else {
                vec![("e0", "small")]
            };
            assert_eq!(assigned_to(&joined), expected, "held off: {held_off}");
            assert!(dispatcher.stranded(0).is_empty());
        }
    }

    /// A machine away for an update keeps a clamped entry off the machine
    /// that would clamp it, but not an unmeasured compile off a smaller
    /// machine: only work that would run unmeasured-for waits on an absence.
    #[test]
    fn an_away_machine_holds_off_only_disfavoured_work() {
        for (key_big, key_small, held_off) in [
            (HardKey::default(), CLAMPED, true),
            (unmeasured(120), unmeasured(12), false),
        ] {
            let mut dispatcher = Dispatcher::default();
            dispatcher.attach("big", 1);
            dispatcher.attach("small", 1);
            dispatcher.set_room("big", 1, 1, room(32, 128), 0, &mut yes());
            dispatcher.set_room("small", 1, 1, room(32, 64), 0, &mut yes());
            let mut entry = keyed(
                keyed(waiting(0, &["big", "small"], &need(8, 8)), "big", key_big),
                "small",
                key_small,
            );
            entry.candidates.get_mut("big").unwrap().away = true;
            let joined = dispatcher.enqueue(entry, 0, &mut yes());
            let expected = if held_off {
                vec![]
            } else {
                vec![("e0", "small")]
            };
            assert_eq!(assigned_to(&joined), expected, "held off: {held_off}");
            assert!(dispatcher.stranded(0).is_empty());
        }
    }

    /// Work its caller pinned, named or kept home never waits for another
    /// machine.
    #[test]
    fn work_that_may_not_spill_is_never_held_off() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("big", 1);
        dispatcher.attach("small", 1);
        dispatcher.set_room("big", 1, 1, busy(32, 128), 0, &mut yes());
        dispatcher.set_room("small", 1, 1, room(32, 64), 0, &mut yes());
        let mut entry = keyed(waiting(0, &["big", "small"], &need(8, 8)), "small", CLAMPED);
        entry.may_spill = false;
        let joined = dispatcher.enqueue(entry, 0, &mut yes());
        assert_eq!(assigned_to(&joined), vec![("e0", "small")]);
    }

    /// The better machine announces that it can no longer hold the work
    /// (its memory shrank): the entry it held off is placed again at once.
    #[test]
    fn a_better_machine_that_shrinks_lets_the_entry_go() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("big", 1);
        dispatcher.attach("small", 1);
        dispatcher.set_room("big", 1, 1, busy(32, 128), 0, &mut yes());
        dispatcher.set_room("small", 1, 1, room(32, 64), 0, &mut yes());
        let entry = keyed(waiting(0, &["big", "small"], &need(8, 8)), "small", CLAMPED);
        assert_eq!(dispatcher.enqueue(entry, 0, &mut yes()), Effects::default());
        let shrunk = dispatcher.set_room("big", 1, 2, busy(32, 4), 0, &mut yes());
        assert_eq!(assigned_to(&shrunk), vec![("e0", "small")]);
        assert!(dispatcher.stranded(0).is_empty());
    }

    /// After a rebuild every machine re-attaches and announces in any order.
    /// While the better machine is attached and silent, a disfavoured machine
    /// that announces first does not take clamped or deferred work until the
    /// entry's reserve-from instant; other work is not held off by silence
    /// (D2 as ruled for this PR).
    #[test]
    fn a_silent_better_machine_holds_off_disfavoured_work_until_reserve_from() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("big", 1);
        dispatcher.attach("small", 1);
        dispatcher.set_room("small", 1, 1, room(32, 64), 0, &mut yes());
        let clamped = keyed(waiting(0, &["big", "small"], &need(8, 8)), "small", CLAMPED);
        let compile = keyed(
            keyed(
                waiting(1, &["big", "small"], &need(8, 8)),
                "big",
                unmeasured(120),
            ),
            "small",
            unmeasured(12),
        );
        let mut joined = dispatcher.enqueue(clamped, 0, &mut yes());
        joined.absorb(dispatcher.enqueue(compile, 0, &mut yes()));
        assert_eq!(assigned_to(&joined), vec![("e1", "small")]);
        assert!(dispatcher.stranded(0).is_empty());
        let entitled = dispatcher.deadline_reached(5 * HOUR, &mut yes());
        assert_eq!(assigned_to(&entitled), vec![("e0", "small")]);
    }

    /// A machine away for an update keeps clamped or deferred work off the
    /// machine that would clamp or defer it only until the entry's
    /// reserve-from instant, as a silent one does (D2): the deadline then
    /// places it on the disfavoured machine.
    #[test]
    fn an_away_better_machine_holds_off_disfavoured_work_only_until_reserve_from() {
        for key in [CLAMPED, DEFERRED] {
            let mut dispatcher = Dispatcher::default();
            dispatcher.attach("big", 1);
            dispatcher.attach("small", 1);
            dispatcher.set_room("big", 1, 1, room(32, 128), 0, &mut yes());
            dispatcher.set_room("small", 1, 1, room(32, 64), 0, &mut yes());
            let mut entry = keyed(waiting(0, &["big", "small"], &need(8, 8)), "small", key);
            entry.candidates.get_mut("big").unwrap().away = true;
            let joined = dispatcher.enqueue(entry, 0, &mut yes());
            assert_eq!(assigned_to(&joined), vec![], "{key:?}: held before 5h");
            assert_eq!(dispatcher.held_off_by("e0", "small", HOUR), Some("big"));
            assert!(dispatcher.stranded(HOUR).is_empty());
            let entitled = dispatcher.deadline_reached(5 * HOUR, &mut yes());
            assert_eq!(
                assigned_to(&entitled),
                vec![("e0", "small")],
                "{key:?}: the away machine holds nothing past reserve-from"
            );
            assert!(!entitled.assigned[0].reserve);
            assert!(dispatcher.stranded(5 * HOUR).is_empty());
        }
    }

    /// An `admit` that turns away every pair on `machine`.
    fn refuses(machine: &'static str) -> impl FnMut(&str, &str) -> bool {
        move |_, executor_id| executor_id != machine
    }

    /// A better machine the runner's last look (`admit`) turns away (a stale
    /// link, a degraded pipe, room a resident model holds) is treated as a
    /// silent one: it keeps deferred or clamped work off the machine behind
    /// it until the entry's reserve-from instant, and nothing else. `stranded`
    /// agrees: past reserve-from the pair behind it is owed an offer.
    #[test]
    fn a_better_machine_admit_turns_away_holds_off_only_disfavoured_work_until_reserve_from() {
        for (key_big, key_small, may_spill, held) in [
            (unmeasured(120), unmeasured(12), true, false),
            (HardKey::default(), CLAMPED, true, true),
            (HardKey::default(), DEFERRED, true, true),
            (HardKey::default(), CLAMPED, false, false),
        ] {
            let case = format!("{key_small:?}, may spill: {may_spill}");
            let mut dispatcher = Dispatcher::default();
            dispatcher.attach("big", 1);
            dispatcher.attach("small", 1);
            dispatcher.set_room("big", 1, 1, room(32, 128), 0, &mut yes());
            dispatcher.set_room("small", 1, 1, room(32, 64), 0, &mut yes());
            let mut entry = keyed(
                keyed(waiting(0, &["big", "small"], &need(8, 8)), "big", key_big),
                "small",
                key_small,
            );
            entry.may_spill = may_spill;
            let joined = dispatcher.enqueue(entry, 0, &mut refuses("big"));
            let now = if held { vec![] } else { vec![("e0", "small")] };
            assert_eq!(assigned_to(&joined), now, "{case}");
            assert!(dispatcher.stranded(HOUR).is_empty(), "{case}");
            if held {
                assert_eq!(
                    dispatcher.stranded(5 * HOUR),
                    vec![("e0".to_string(), "small".to_string())],
                    "{case}: nothing excuses the pair past reserve-from"
                );
                let entitled = dispatcher.deadline_reached(5 * HOUR, &mut refuses("big"));
                assert_eq!(assigned_to(&entitled), vec![("e0", "small")], "{case}");
                assert!(dispatcher.stranded(5 * HOUR).is_empty(), "{case}");
            }
        }
    }

    /// The better machine frees room the entry fits, and `admit` turns the
    /// entry away there: no longer held off, it is placed on the machine
    /// behind in the same walk.
    #[test]
    fn an_entry_admit_turns_away_on_a_freed_better_machine_goes_to_the_next_at_once() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("big", 1);
        dispatcher.attach("small", 1);
        dispatcher.set_room("big", 1, 1, busy(32, 128), 0, &mut yes());
        dispatcher.set_room("small", 1, 1, room(32, 64), 0, &mut yes());
        let entry = keyed(
            keyed(
                waiting(0, &["big", "small"], &need(8, 8)),
                "big",
                unmeasured(120),
            ),
            "small",
            unmeasured(12),
        );
        assert_eq!(dispatcher.enqueue(entry, 0, &mut yes()), Effects::default());
        let freed = dispatcher.set_room("big", 1, 2, room(32, 128), 0, &mut refuses("big"));
        assert_eq!(assigned_to(&freed), vec![("e0", "small")]);
        assert!(dispatcher.stranded(0).is_empty());
    }

    /// An entitled entry clamped on the machine behind a better one that
    /// `admit` keeps turning away holds room behind it, and keeps that hold
    /// across the better machine's beat (the runner's rewalk) and its new
    /// figures: each asks `admit` again, and while it still says no the
    /// better machine stays turned away, so it keeps nothing off the hold.
    /// Once `admit` takes it there, the entry moves its hold to it.
    #[test]
    fn a_hold_behind_a_better_machine_admit_still_turns_away_is_kept() {
        let now = 6 * HOUR;
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("big", 1);
        dispatcher.attach("small", 1);
        dispatcher.set_room("big", 1, 1, busy(32, 128), now, &mut yes());
        dispatcher.set_room("small", 1, 1, busy(32, 64), now, &mut yes());
        let entry = keyed(waiting(0, &["big", "small"], &need(8, 8)), "small", CLAMPED);
        let joined = dispatcher.enqueue(entry, now, &mut refuses("big"));
        assert_eq!(assigned_to(&joined), vec![("e0", "small")]);
        assert!(joined.assigned[0].reserve);
        // The machine keeps the hold standing and refuses the offer.
        let declined = dispatcher.decline("e0", "small", 1, now, &mut refuses("big"));
        assert_eq!(declined, Effects::default());
        assert_eq!(dispatcher.reserved_on("e0"), Some("small"));

        let beat = dispatcher.rewalk("big", now, &mut refuses("big"));
        assert_eq!(beat, Effects::default(), "the beat leaves the hold");
        assert_eq!(dispatcher.reserved_on("e0"), Some("small"));
        let figures = dispatcher.set_room("big", 1, 2, busy(32, 120), now, &mut refuses("big"));
        assert_eq!(figures, Effects::default(), "new figures leave the hold");
        assert_eq!(dispatcher.reserved_on("e0"), Some("small"));
        assert!(dispatcher.stranded(now).is_empty());

        let admitted = dispatcher.rewalk("big", now, &mut yes());
        assert_eq!(
            admitted.released,
            vec![("e0".to_string(), "small".to_string(), 1)]
        );
        assert_eq!(assigned_to(&admitted), vec![("e0", "big")]);
        assert!(admitted.assigned[0].reserve);
        assert!(dispatcher.stranded(now).is_empty());
    }

    /// A better machine whose new figures first make it able to hold the
    /// entry keeps it off the room held behind it only if `admit` would take
    /// the entry there: asked before the hold is given up, it turns the
    /// entry away, and the hold stays.
    #[test]
    fn a_hold_is_kept_from_a_newly_able_better_machine_admit_turns_away() {
        let now = 6 * HOUR;
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("big", 1);
        dispatcher.attach("small", 1);
        dispatcher.set_room("big", 1, 1, busy(32, 4), now, &mut yes());
        dispatcher.set_room("small", 1, 1, busy(32, 64), now, &mut yes());
        let entry = keyed(waiting(0, &["big", "small"], &need(8, 8)), "small", CLAMPED);
        let joined = dispatcher.enqueue(entry, now, &mut yes());
        assert_eq!(assigned_to(&joined), vec![("e0", "small")]);
        assert!(joined.assigned[0].reserve);
        dispatcher.decline("e0", "small", 1, now, &mut yes());
        assert_eq!(dispatcher.reserved_on("e0"), Some("small"));

        let grown = dispatcher.set_room("big", 1, 2, busy(32, 128), now, &mut refuses("big"));
        assert_eq!(grown, Effects::default(), "the hold stays on small");
        assert_eq!(dispatcher.reserved_on("e0"), Some("small"));
        assert!(dispatcher.turned_away_stands("e0", "big"));
        assert!(dispatcher.stranded(now).is_empty());
    }

    /// The better machine that kept an entry off a fitting room, turned away
    /// by `admit` as the entry asks whether to keep its hold, keeps it off
    /// nothing more: the entry is placed on the room that fits, not held.
    #[test]
    fn an_entry_a_turned_away_machine_kept_off_a_fitting_room_is_placed_there() {
        let now = 6 * HOUR;
        let mut dispatcher = Dispatcher::default();
        for machine in ["big", "small", "mid"] {
            dispatcher.attach(machine, 1);
        }
        dispatcher.set_room("big", 1, 1, busy(32, 128), now, &mut yes());
        dispatcher.set_room("small", 1, 1, busy(32, 64), now, &mut yes());
        dispatcher.set_room("mid", 1, 1, room(32, 64), now, &mut yes());
        let all = keyed(
            keyed(
                waiting(0, &["big", "small", "mid"], &need(8, 8)),
                "small",
                CLAMPED,
            ),
            "mid",
            CLAMPED,
        );
        let mut first = all.clone();
        first.candidates.retain(|machine, _| machine == "small");
        let joined = dispatcher.enqueue(first, now, &mut yes());
        assert_eq!(assigned_to(&joined), vec![("e0", "small")]);
        dispatcher.decline("e0", "small", 1, now, &mut yes());
        assert_eq!(dispatcher.reserved_on("e0"), Some("small"));

        let moved = dispatcher.set_candidates("e0", all.candidates, now, &mut refuses("big"));
        assert_eq!(assigned_to(&moved), vec![("e0", "mid")]);
        assert!(!moved.assigned[0].reserve);
        assert_eq!(
            moved.released,
            vec![("e0".to_string(), "small".to_string(), 1)]
        );
        assert!(dispatcher.stranded(now).is_empty());
    }

    /// `admit`'s turning an entry away from a machine it may no longer run
    /// on is forgotten: nothing would ask `admit` there again, so if the
    /// machine becomes a candidate again the entry is judged there afresh.
    #[test]
    fn a_machine_an_entry_may_no_longer_run_on_forgets_turning_it_away() {
        let now = 6 * HOUR;
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("big", 1);
        dispatcher.attach("small", 1);
        dispatcher.set_room("big", 1, 1, busy(32, 128), now, &mut yes());
        dispatcher.set_room("small", 1, 1, busy(32, 64), now, &mut yes());
        let both = keyed(waiting(0, &["big", "small"], &need(8, 8)), "small", CLAMPED);
        let mut small_only = both.candidates.clone();
        small_only.remove("big");
        let joined = dispatcher.enqueue(both.clone(), now, &mut refuses("big"));
        assert_eq!(assigned_to(&joined), vec![("e0", "small")]);
        dispatcher.decline("e0", "small", 1, now, &mut refuses("big"));
        assert!(dispatcher.turned_away_stands("e0", "big"));

        dispatcher.set_candidates("e0", small_only, now, &mut yes());
        let back = dispatcher.set_candidates("e0", both.candidates, now, &mut yes());
        assert_eq!(assigned_to(&back), vec![("e0", "big")]);
        assert!(back.assigned[0].reserve);
        assert_eq!(
            back.released,
            vec![("e0".to_string(), "small".to_string(), 1)]
        );
        assert!(dispatcher.stranded(now).is_empty());
    }

    /// The runner can report one refusal twice: the machine's own Decline
    /// lands and places the entry elsewhere, and the offer's timeout, which
    /// read the offer as still out, reports it again. The second report
    /// changes nothing: the entry stays offered where the first placed it,
    /// nothing is offered twice, and the refusal still stands.
    #[test]
    fn a_decline_after_the_offer_already_ended_changes_nothing() {
        let mut dispatcher = Dispatcher::default();
        dispatcher.attach("m", 1);
        dispatcher.attach("n", 1);
        dispatcher.set_room("m", 1, 1, room(32, 64), 0, &mut yes());
        dispatcher.set_room("n", 1, 1, room(16, 64), 0, &mut yes());
        let joined = dispatcher.enqueue(waiting(0, &["m", "n"], &need(8, 8)), 0, &mut yes());
        assert_eq!(assigned_to(&joined), vec![("e0", "m")]);
        let declined = dispatcher.decline("e0", "m", 1, 0, &mut yes());
        assert_eq!(assigned_to(&declined), vec![("e0", "n")]);
        let (_, _, left) = dispatcher.room("n").unwrap();
        let left = left.clone();

        let timed_out = dispatcher.decline("e0", "m", 1, 0, &mut yes());
        assert_eq!(timed_out, Effects::default(), "offered to n once");
        assert_eq!(dispatcher.offered_on("n"), vec!["e0".to_string()]);
        assert_eq!(
            dispatcher.room("n").unwrap().2,
            &left,
            "n's share stays taken"
        );
        assert!(dispatcher.refusal_stands("e0", "m"));
        assert!(dispatcher.stranded(0).is_empty());

        // The same order with nowhere else to go: the entry waits, refused.
        let mut alone = Dispatcher::default();
        alone.attach("m", 1);
        alone.set_room("m", 1, 1, room(32, 64), 0, &mut yes());
        alone.enqueue(waiting(0, &["m"], &need(8, 8)), 0, &mut yes());
        assert_eq!(
            alone.decline("e0", "m", 1, 0, &mut yes()),
            Effects::default()
        );
        assert_eq!(
            alone.decline("e0", "m", 1, 0, &mut yes()),
            Effects::default()
        );
        assert!(alone.refusal_stands("e0", "m"));
        assert!(alone.stranded(0).is_empty());
    }

    /// A candidate with a random place on the hard key: deferred, clamped or
    /// neither, and for an unmeasured `compile` a random single-process
    /// capacity; now and then away for an update.
    fn random_candidate(rng: &mut rand::rngs::StdRng, compile: bool, need: Need) -> Candidate {
        Candidate {
            need,
            warm: rng.gen_bool(0.3),
            key: HardKey {
                deferred: rng.gen_bool(0.2),
                unmeasured_capacity: compile
                    .then(|| rng.gen_bool(0.9).then(|| rng.gen_range(1..4) * 16 * GIB)),
                clamped: rng.gen_bool(0.2),
            },
            away: rng.gen_bool(0.1),
        }
    }

    #[test]
    fn a_silent_better_machine_blocks_a_clamped_candidate_but_not_an_unclamped_one() {
        for reserve in [false, true] {
            let mut dispatcher = Dispatcher::default();
            dispatcher.attach("a", 1);
            let a = if reserve { busy(17, 84) } else { room(17, 84) };
            dispatcher.set_room("a", 1, 1, a, 0, &mut yes());
            let mut w = waiting(0, &["a"], &need(3, 31));
            w.standing = true;
            w.candidates.get_mut("a").unwrap().key = HardKey {
                unmeasured_capacity: Some(Some(48 * GIB)),
                clamped: true,
                ..HardKey::default()
            };
            dispatcher.enqueue(w.clone(), 0, &mut refuses("a"));
            assert!(dispatcher.turned_away_stands("e0", "a"));

            dispatcher.attach("d", 1);
            dispatcher.attach("b", 1);
            let b = if reserve { busy(21, 33) } else { room(21, 33) };
            dispatcher.set_room("b", 1, 1, b, 0, &mut yes());
            let mut candidates = w.candidates;
            let mut silent = candidates["a"].clone();
            silent.key.clamped = false;
            candidates.insert("d".to_string(), silent);
            let mut unclamped = candidates["d"].clone();
            unclamped.key.unmeasured_capacity = Some(Some(32 * GIB));
            candidates.insert("b".to_string(), unclamped);
            let effects = dispatcher.set_candidates("e0", candidates, 0, &mut refuses("a"));
            assert_eq!(assigned_to(&effects), vec![("e0", "b")]);
            assert_eq!(effects.assigned[0].reserve, reserve);
            assert!(dispatcher.stranded(0).is_empty());
        }
    }

    /// Property 5: after every event of a random sequence, no waiting entry
    /// fits an eligible machine's room unless that machine refused it under
    /// that room, and every refusal that stands was given by the process the
    /// machine runs now. Machines restart (a new link whose room seq starts
    /// again from 1) and their links end with offers out.
    ///
    /// The runner's side of `admit` is modelled as the runner keeps it: the
    /// machines it turned work away from (`admit_refused`), which a beat
    /// walks again and a room frame clears, walking the machine when the
    /// frame repeats its figures. So every turning-away that stands has an
    /// event coming that asks `admit` again, and an entitled waiting entry
    /// holding room keeps it unless a machine `admit` would take it on, or
    /// a silent one before its reserve-from instant, keeps it off that
    /// room.
    #[test]
    fn nothing_is_stranded_after_any_event() {
        let machines = ["a", "b", "c", "d"];
        for seed in 0..64 {
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            let mut dispatcher = Dispatcher::default();
            let mut generation: HashMap<&str, u64> = HashMap::new();
            let mut attached: HashSet<&str> = HashSet::new();
            let mut seq: HashMap<&str, u64> = HashMap::new();
            // The link generation each refusal was given on.
            let mut refused_on: HashMap<(String, &str), u64> = HashMap::new();
            // The runner's `admit_refused`.
            let mut admit_refused: HashSet<String> = HashSet::new();
            let mut next_entry = 0;
            let mut now = 0;
            for step in 0..400u64 {
                now += rng.gen_range(0..60_000);
                // The runner's last look turns some pairs away on some
                // machines, for spans of events at a time as a stale link or
                // a pipe floor does, and its answer moves between spans with
                // nothing telling the dispatcher.
                let span = step / 16;
                let takes = |entry_id: &str, executor_id: &str| {
                    use std::hash::{Hash, Hasher};
                    let mut strict = std::hash::DefaultHasher::new();
                    (seed, span, executor_id).hash(&mut strict);
                    let mut pair = std::hash::DefaultHasher::new();
                    (seed, span, entry_id, executor_id).hash(&mut pair);
                    strict.finish().is_multiple_of(2) || !pair.finish().is_multiple_of(4)
                };
                // The machines `admit` turned work away from in this step,
                // noted in `admit_refused` once the step's call returns.
                let turned_on: std::cell::RefCell<HashSet<String>> = Default::default();
                let mut admit = |entry_id: &str, executor_id: &str| {
                    let yes = takes(entry_id, executor_id);
                    if !yes {
                        turned_on.borrow_mut().insert(executor_id.to_string());
                    }
                    yes
                };
                // The runner arms a timer at the next deadline.
                dispatcher.deadline_reached(now, &mut admit);
                admit_refused.extend(turned_on.take());
                let held = dispatcher.entitled_waiting();
                let machine = machines[rng.gen_range(0..machines.len())];
                let event = rng.gen_range(0..14);
                // A room frame the machine sends, carried out below as the
                // runner's `dispatcher_room` carries it out.
                let mut frame = None;
                match event {
                    // A new process, replacing any link before it, counts
                    // its room from 1 again.
                    0 => {
                        let g = generation.entry(machine).or_insert(0);
                        *g += 1;
                        seq.insert(machine, 0);
                        attached.insert(machine);
                        dispatcher.attach_link(machine, *g, now, &mut admit);
                    }
                    // 9 and 11: a brake or a provisioning hold closes the
                    // room, or every core is given out on a machine whose
                    // size moved; the room's capacity is what keeps entries
                    // off the machines behind it.
                    1 | 2 | 9 | 11 => {
                        let event = rng.gen_range(0..3);
                        let g = generation.get(machine).copied().unwrap_or(0);
                        let s = seq.entry(machine).or_insert(0);
                        *s += 1;
                        let mut r = room(rng.gen_range(1..33), rng.gen_range(1..128));
                        r.standing.cores = rng.gen_range(0..=r.cores.limit as u32);
                        r.slots = CompileSlots::net(r.cores.limit as u32, r.standing.cores);
                        r.compiling = rng.gen_range(0..=r.slots.slots);
                        r.cores.committed =
                            rng.gen_range(u64::from(r.standing.cores)..=r.cores.limit);
                        match event {
                            0 => {
                                use cairn_common::executor_protocol::{
                                    DeclineReason, RoomClosure, RoomScope,
                                };
                                r.closures.push(RoomClosure {
                                    scope: RoomScope::NonInteractive,
                                    reason: DeclineReason::Pressure,
                                    detail: None,
                                    disk: None,
                                    conditions: Vec::new(),
                                });
                            }
                            1 => r.cores.committed = r.cores.limit,
                            _ => {}
                        }
                        if next_entry > 0 && rng.gen_bool(0.4) {
                            let held = need(rng.gen_range(1..17), rng.gen_range(1..64));
                            r = holding(r, rng.gen_range(0..next_entry), &held);
                        }
                        frame = Some((g, *s, r));
                    }
                    // A frame repeating the figures the machine announces.
                    13 => {
                        if let Some((_, r, _)) = dispatcher.room(machine) {
                            let r = r.clone();
                            let g = generation.get(machine).copied().unwrap_or(0);
                            let s = seq.entry(machine).or_insert(0);
                            *s += 1;
                            frame = Some((g, *s, r));
                        }
                    }
                    3 | 4 => {
                        let eligible: Vec<&str> = machines
                            .iter()
                            .copied()
                            .filter(|_| rng.gen_bool(0.5))
                            .collect();
                        let mut w = waiting(
                            next_entry,
                            &eligible,
                            &need(rng.gen_range(1..17), rng.gen_range(1..64)),
                        );
                        w.queued_at_unix_ms = now;
                        w.horizon_unix_ms = now + rng.gen_range(1..HOUR);
                        w.reserve_from_unix_ms = now + rng.gen_range(0..HOUR);
                        w.standing = rng.gen_bool(0.1);
                        w.may_spill = rng.gen_bool(0.9);
                        let compile = rng.gen_bool(0.3);
                        for candidate in w.candidates.values_mut() {
                            *candidate =
                                random_candidate(&mut rng, compile, candidate.need.clone());
                        }
                        next_entry += 1;
                        dispatcher.enqueue(w, now, &mut admit);
                    }
                    // Mostly the machine refuses an offer out to it, a hold
                    // offer among them; now and then any entry.
                    5 => {
                        let out = dispatcher.offered_on(machine);
                        let id = if !out.is_empty() && rng.gen_bool(0.7) {
                            out[rng.gen_range(0..out.len())].clone()
                        } else {
                            format!("e{}", rng.gen_range(0..next_entry.max(1)))
                        };
                        let s = seq.get(machine).copied().unwrap_or(0);
                        if attached.contains(machine) {
                            refused_on.insert((id.clone(), machine), generation[machine]);
                        }
                        dispatcher.decline(&id, machine, s, now, &mut admit);
                    }
                    // The entry leaves, or the machine its offer went to
                    // claims it.
                    6 => {
                        let id = format!("e{}", rng.gen_range(0..next_entry.max(1)));
                        let on = machines
                            .into_iter()
                            .find(|m| dispatcher.offered_on(m).contains(&id));
                        match on {
                            Some(on) if rng.gen_bool(0.5) => {
                                dispatcher.claimed(&id, on);
                            }
                            _ => {
                                dispatcher.withdraw(&id, now, &mut admit);
                            }
                        }
                    }
                    7 => {
                        let id = format!("e{}", rng.gen_range(0..next_entry.max(1)));
                        let mut candidates = HashMap::new();
                        let compile = rng.gen_bool(0.3);
                        for m in machines {
                            if rng.gen_bool(0.5) {
                                let need = need(rng.gen_range(1..17), rng.gen_range(1..64));
                                candidates.insert(
                                    m.to_string(),
                                    random_candidate(&mut rng, compile, need),
                                );
                            }
                        }
                        dispatcher.set_candidates(&id, candidates, now, &mut admit);
                    }
                    8 => {
                        let id = format!("e{}", rng.gen_range(0..next_entry.max(1)));
                        dispatcher.requeue(&id, now, &mut admit);
                    }
                    // The link ends, and the queue hands back the offers
                    // that were out on it.
                    10 => {
                        let g = generation.get(machine).copied().unwrap_or(0);
                        attached.remove(machine);
                        if dispatcher.generation(machine) == Some(g) {
                            admit_refused.remove(machine);
                        }
                        let out = dispatcher.offered_on(machine);
                        dispatcher.detach(machine, g, now, &mut admit);
                        for id in out {
                            dispatcher.requeue(&id, now, &mut admit);
                        }
                    }
                    // A beat: the runner walks the machine again when
                    // `admit` last turned work away there.
                    12 => {
                        if admit_refused.remove(machine) {
                            dispatcher.rewalk(machine, now, &mut admit);
                        }
                    }
                    _ => {
                        dispatcher.deadline_reached(now, &mut admit);
                    }
                }
                if let Some((g, s, r)) = frame.filter(|_| attached.contains(machine)) {
                    let turned_away = admit_refused.remove(machine);
                    let repeats = dispatcher
                        .room(machine)
                        .is_some_and(|(_, held, _)| *held == r);
                    dispatcher.set_room(machine, g, s, r, now, &mut admit);
                    if turned_away && repeats {
                        dispatcher.rewalk(machine, now, &mut admit);
                    }
                }
                admit_refused.extend(turned_on.take());
                assert_eq!(
                    dispatcher.stranded(now),
                    Vec::new(),
                    "seed {seed}, step {step}: event {event} on {machine}"
                );
                let still = dispatcher.entitled_waiting();
                for (id, on) in &held {
                    let (Some(on), Some(now_on)) = (on, still.get(id)) else {
                        continue;
                    };
                    let eligible = dispatcher.generation(on).is_some()
                        && dispatcher.candidates(id).is_some_and(|candidates| {
                            candidates.get(on).is_some_and(|candidate| {
                                !candidate.away
                                    && dispatcher.room(on).is_none_or(|(_, room, _)| {
                                        fits(&room.capacity(), &candidate.need).is_ok()
                                    })
                            })
                        });
                    if now_on.as_ref() == Some(on) || !eligible {
                        continue;
                    }
                    // A silent machine (no room yet, away, or turned away
                    // by `admit`) keeps deferred or clamped work off until
                    // reserve-from whatever `admit` says there (D2).
                    let silent = |by: &str| {
                        dispatcher.machines[by].room.is_none()
                            || dispatcher.entries[id].waiting.candidates[by].away
                            || dispatcher.turned_away_stands(id, by)
                    };
                    let by = dispatcher.held_off_by(id, on, now);
                    assert!(
                        by.is_some_and(|by| takes(id, by) || silent(by)),
                        "seed {seed}, step {step}: event {event} on {machine}: {id} gave up the room {on} held for it, kept off it by {by:?}"
                    );
                }
                for id in 0..next_entry {
                    let id = format!("e{id}");
                    for machine in machines {
                        if dispatcher.turned_away_stands(&id, machine) {
                            assert!(
                                admit_refused.contains(machine),
                                "seed {seed}, step {step}: event {event}: {id} stands turned away on {machine}, and nothing will ask admit again"
                            );
                        }
                    }
                }
                for id in 0..next_entry {
                    let id = format!("e{id}");
                    for machine in machines {
                        if dispatcher.refusal_stands(&id, machine) {
                            assert_eq!(
                                refused_on.get(&(id.clone(), machine)),
                                generation.get(machine),
                                "seed {seed}: {id}'s refusal on {machine} stands on a link that never gave it"
                            );
                        }
                    }
                }
            }
        }
    }
}
