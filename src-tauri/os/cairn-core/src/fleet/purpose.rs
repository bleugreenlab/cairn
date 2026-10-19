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

use std::collections::HashMap;

use super::RequestIdentity;

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

/// One live answer to one question.
#[derive(Clone, Debug)]
struct LiveItem {
    request: RequestIdentity,
    phase: ItemPhase,
    armed_at_unix_ms: u64,
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
    retired_at_unix_ms: u64,
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
}

impl PurposeRegistry {
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
        let state = self.items.entry(key).or_default();
        let Some(existing) = state.live.as_mut() else {
            state.live = Some(LiveItem {
                request,
                phase: ItemPhase::Preparing,
                armed_at_unix_ms,
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
        let retire = existing.request.clone();
        let phase = existing.phase;
        // The displaced item becomes this question's debt rather than leaving
        // it. Until its death is observed the key is not free, and the caller
        // that just won the key may not dispatch.
        state.retiring.push(RetiringItem {
            request: retire.clone(),
            retired_at_unix_ms: armed_at_unix_ms,
        });
        state.live = Some(LiveItem {
            request,
            phase: ItemPhase::Preparing,
            armed_at_unix_ms,
        });
        Admission::Replaced { retire, phase }
    }

    /// Move the live item for `request` into retirement without replacing it.
    ///
    /// The requester-facing path: someone asked for this work to stop and no
    /// newer answer is taking its place. The key stays occupied by the debt, so
    /// a fresh arrival still waits for the death rather than starting beside it.
    pub(crate) fn retire(&mut self, request: &RequestIdentity, at_unix_ms: u64) -> bool {
        let Some(state) = self.items.values_mut().find(|state| {
            state
                .live
                .as_ref()
                .is_some_and(|live| &live.request == request)
        }) else {
            return false;
        };
        state.live = None;
        state.retiring.push(RetiringItem {
            request: request.clone(),
            retired_at_unix_ms: at_unix_ms,
        });
        true
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

    /// The oldest outstanding retirement for `key`, for reporting a wait that
    /// has gone on long enough to be worth naming.
    pub(crate) fn oldest_pending_retirement(
        &self,
        key: &PurposeKey,
    ) -> Option<(RequestIdentity, u64)> {
        self.items
            .get(key)?
            .retiring
            .iter()
            .min_by_key(|item| item.retired_at_unix_ms)
            .map(|item| (item.request.clone(), item.retired_at_unix_ms))
    }

    /// Record that the live item for `request` reached `phase`.
    ///
    /// Fenced by request identity, so a transition reported by an item that has
    /// already been replaced cannot move the item that replaced it.
    pub(crate) fn advance(&mut self, request: &RequestIdentity, phase: ItemPhase) -> bool {
        let Some(item) = self
            .items
            .values_mut()
            .filter_map(|state| state.live.as_mut())
            .find(|item| &item.request == request)
        else {
            return false;
        };
        item.phase = phase;
        true
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
        let mut cleared = false;
        self.items.retain(|_, state| {
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
            !state.is_vacant()
        });
        cleared
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
            assert!(registry.advance(&request("old"), phase));

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
        assert_eq!(
            registry.oldest_pending_retirement(&key),
            Some((request("tip-x"), 20))
        );

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
