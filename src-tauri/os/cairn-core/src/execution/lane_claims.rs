//! Which write-check lanes are being verified right now, keyed by the input key
//! their verdict will be recorded under.
//!
//! A fix that lands on a head the branch moved to while its wave ran re-keys
//! that wave onto the head, and the head's own write wave is usually verifying
//! the same lanes at the same keys already. A claim is how the fold sees that:
//! it waits for the claimant to record, and the reuse lookup that follows
//! answers the lane from the claimant's verdict instead of spending a second
//! lane on it (docs/checks.md, "A fold onto a moved head").
//!
//! Claims decide nothing about correctness. Whatever ends a wait, the reuse
//! lookup alone decides what still runs, so a claim released without a verdict
//! (the lane died, or its wave re-keyed it away) costs only the wait.
//!
//! A wait cannot deadlock: a wave gives up every claim it holds before it waits
//! on another's, so no wave anyone waits on is itself waiting.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

/// One lane's verdict identity, as the result cache reuses it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct LaneKey {
    pub(crate) project_id: String,
    pub(crate) check_name: String,
    pub(crate) input_hash: String,
}

/// A lane another wave was verifying when a wait began, and the commit that
/// wave records its verdict at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClaimedElsewhere {
    pub(crate) check_name: String,
    pub(crate) commit: String,
}

#[derive(Debug, Default)]
struct State {
    next_wave: u64,
    /// Each claimed key, by the waves claiming it and the commit each records at.
    claims: HashMap<LaneKey, HashMap<u64, String>>,
    /// Waves that have not stated their claims yet, by job.
    planning: HashMap<String, HashSet<u64>>,
}

impl State {
    fn leave_planning(&mut self, job_id: &str, wave: u64) {
        if let Some(waves) = self.planning.get_mut(job_id) {
            waves.remove(&wave);
            if waves.is_empty() {
                self.planning.remove(job_id);
            }
        }
    }
}

/// The registry: one per orchestrator, runtime-only.
#[derive(Debug, Default)]
pub(crate) struct LaneClaims {
    state: Mutex<State>,
    changed: tokio::sync::Notify,
}

impl LaneClaims {
    /// Open a write wave for `job_id`. It starts out planning, and a sibling's
    /// fold waits that out, because a wave that has not planned has not claimed.
    pub(crate) fn open_wave(self: &Arc<Self>, job_id: &str) -> WaveClaims {
        let mut state = self.lock();
        let wave = state.next_wave;
        state.next_wave += 1;
        state
            .planning
            .entry(job_id.to_string())
            .or_default()
            .insert(wave);
        drop(state);
        WaveClaims {
            registry: self.clone(),
            wave,
            job_id: job_id.to_string(),
            held: HashSet::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One write wave's claims. Dropping it releases them, which is the wave
/// saying it has recorded everything it is going to.
pub(crate) struct WaveClaims {
    registry: Arc<LaneClaims>,
    wave: u64,
    job_id: String,
    held: HashSet<LaneKey>,
}

impl WaveClaims {
    /// Claim the lanes this wave is about to verify, whose verdicts it will
    /// record at `commit`. Stating its claims ends the wave's planning, so a
    /// wave calls this once it knows its misses, even when it has none.
    pub(crate) fn claim(&mut self, keys: impl IntoIterator<Item = LaneKey>, commit: &str) {
        let mut state = self.registry.lock();
        for key in keys {
            state
                .claims
                .entry(key.clone())
                .or_default()
                .insert(self.wave, commit.to_string());
            self.held.insert(key);
        }
        state.leave_planning(&self.job_id, self.wave);
        drop(state);
        self.registry.changed.notify_waiters();
    }

    /// Release claims whose verdicts this wave will no longer record, so no
    /// other wave waits on them.
    pub(crate) fn release(&mut self, keys: impl IntoIterator<Item = LaneKey>) {
        let mut state = self.registry.lock();
        let mut released = false;
        for key in keys {
            if self.held.remove(&key) {
                release_key(&mut state, &key, self.wave);
                released = true;
            }
        }
        drop(state);
        if released {
            self.registry.changed.notify_waiters();
        }
    }

    /// Wait until no other wave is verifying any of `keys` and no sibling wave
    /// of this job is still planning, or until `bound` elapses. Returns the
    /// lanes another wave was verifying, so the caller can say whose verdicts
    /// it reused. Before it waits on anything, this wave releases every claim it
    /// holds; that is the whole deadlock argument.
    pub(crate) async fn await_other_waves(
        &mut self,
        keys: &[LaneKey],
        bound: Duration,
    ) -> Vec<ClaimedElsewhere> {
        let deadline = tokio::time::Instant::now() + bound;
        let mut seen: Vec<ClaimedElsewhere> = Vec::new();
        loop {
            // Registered before the state is read, so a release between the
            // read and the await still wakes this wait.
            let notified = self.registry.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (blocked, released) = {
                let mut state = self.registry.lock();
                let sibling_planning = state
                    .planning
                    .get(&self.job_id)
                    .is_some_and(|waves| waves.iter().any(|wave| *wave != self.wave));
                let mut claimed = false;
                for key in keys {
                    let claimant = state.claims.get(key).and_then(|waves| {
                        waves
                            .iter()
                            .filter(|(wave, _)| **wave != self.wave)
                            .min_by_key(|(wave, _)| **wave)
                    });
                    if let Some((_, commit)) = claimant {
                        claimed = true;
                        if !seen.iter().any(|lane| lane.check_name == key.check_name) {
                            seen.push(ClaimedElsewhere {
                                check_name: key.check_name.clone(),
                                commit: commit.clone(),
                            });
                        }
                    }
                }
                let blocked = sibling_planning || claimed;
                let released = blocked && !self.held.is_empty();
                if released {
                    for key in std::mem::take(&mut self.held) {
                        release_key(&mut state, &key, self.wave);
                    }
                }
                (blocked, released)
            };
            if released {
                self.registry.changed.notify_waiters();
            }
            if !blocked {
                return seen;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                log::info!(
                    "write wave for job {}: stopped waiting for other waves' lanes after {}s",
                    self.job_id,
                    bound.as_secs()
                );
                return seen;
            }
        }
    }
}

fn release_key(state: &mut State, key: &LaneKey, wave: u64) {
    if let Some(waves) = state.claims.get_mut(key) {
        waves.remove(&wave);
        if waves.is_empty() {
            state.claims.remove(key);
        }
    }
}

impl Drop for WaveClaims {
    fn drop(&mut self) {
        let mut state = self.registry.lock();
        for key in std::mem::take(&mut self.held) {
            release_key(&mut state, &key, self.wave);
        }
        state.leave_planning(&self.job_id, self.wave);
        drop(state);
        self.registry.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PATIENT: Duration = Duration::from_secs(10);

    fn key(check: &str, input: &str) -> LaneKey {
        LaneKey {
            project_id: "project-a".into(),
            check_name: check.into(),
            input_hash: input.into(),
        }
    }

    fn elsewhere(check: &str, commit: &str) -> ClaimedElsewhere {
        ClaimedElsewhere {
            check_name: check.into(),
            commit: commit.into(),
        }
    }

    /// Resolves only if the wait returns well inside `PATIENT`.
    async fn promptly<T>(wait: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(2), wait)
            .await
            .expect("the wait should have returned promptly")
    }

    #[tokio::test]
    async fn nothing_claimed_means_no_wait() {
        let registry = Arc::new(LaneClaims::default());
        let mut fold = registry.open_wave("job-a");
        fold.claim([key("migrations", "k1")], "h1");
        let seen = promptly(fold.await_other_waves(&[key("migrations", "k2")], PATIENT)).await;
        assert!(seen.is_empty());
    }

    #[tokio::test]
    async fn a_wave_never_waits_on_its_own_claims() {
        let registry = Arc::new(LaneClaims::default());
        let mut fold = registry.open_wave("job-a");
        fold.claim([key("migrations", "k2")], "h2");
        let seen = promptly(fold.await_other_waves(&[key("migrations", "k2")], PATIENT)).await;
        assert!(seen.is_empty());
    }

    /// The moved-head shape: the head's own wave holds the key, and the fold
    /// waits for it to record, then learns whose verdict answers the lane.
    #[tokio::test]
    async fn the_wait_ends_when_the_claimant_releases_and_names_its_commit() {
        let registry = Arc::new(LaneClaims::default());
        let mut head = registry.open_wave("job-a");
        head.claim([key("migrations", "k2")], "h2");
        let mut fold = registry.open_wave("job-a");
        fold.claim([key("migrations", "k1")], "h1");

        let released = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            drop(head);
        });
        let seen = promptly(fold.await_other_waves(&[key("migrations", "k2")], PATIENT)).await;
        released.await.unwrap();
        assert_eq!(seen, vec![elsewhere("migrations", "h2")]);

        // The fold gave up its own claim before it waited.
        let mut third = registry.open_wave("job-b");
        third.claim([], "h9");
        assert!(
            promptly(third.await_other_waves(&[key("migrations", "k1")], PATIENT))
                .await
                .is_empty()
        );
    }

    /// A sibling wave that has not planned has not claimed yet, so the fold
    /// waits its planning out and then sees the claim it states.
    #[tokio::test]
    async fn a_sibling_still_planning_is_waited_out() {
        let registry = Arc::new(LaneClaims::default());
        let mut head = registry.open_wave("job-a");
        let mut fold = registry.open_wave("job-a");
        fold.claim([], "h1");

        let planned = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            head.claim([key("migrations", "k2")], "h2");
            tokio::time::sleep(Duration::from_millis(30)).await;
            drop(head);
        });
        let seen = promptly(fold.await_other_waves(&[key("migrations", "k2")], PATIENT)).await;
        planned.await.unwrap();
        assert_eq!(seen, vec![elsewhere("migrations", "h2")]);
    }

    #[tokio::test]
    async fn another_jobs_planning_is_not_waited_on() {
        let registry = Arc::new(LaneClaims::default());
        let _elsewhere = registry.open_wave("job-b");
        let mut fold = registry.open_wave("job-a");
        fold.claim([], "h1");
        assert!(
            promptly(fold.await_other_waves(&[key("migrations", "k2")], PATIENT))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_released_claim_stops_blocking() {
        let registry = Arc::new(LaneClaims::default());
        let mut head = registry.open_wave("job-a");
        head.claim([key("migrations", "k2")], "h2");
        head.release([key("migrations", "k2")]);
        let mut fold = registry.open_wave("job-a");
        fold.claim([], "h1");
        assert!(
            promptly(fold.await_other_waves(&[key("migrations", "k2")], PATIENT))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn the_wait_is_bounded() {
        let registry = Arc::new(LaneClaims::default());
        let mut head = registry.open_wave("job-a");
        head.claim([key("migrations", "k2")], "h2");
        let mut fold = registry.open_wave("job-a");
        fold.claim([], "h1");
        let seen =
            promptly(fold.await_other_waves(&[key("migrations", "k2")], Duration::from_millis(40)))
                .await;
        assert_eq!(seen, vec![elsewhere("migrations", "h2")]);
        drop(head);
    }

    /// Two folds, each waiting on the key the other holds. Each releases its
    /// own claim before waiting, so both return long before their bound.
    #[tokio::test]
    async fn two_folds_waiting_on_each_other_do_not_deadlock() {
        let registry = Arc::new(LaneClaims::default());
        let mut first = registry.open_wave("job-a");
        first.claim([key("fmt", "a")], "h1");
        let mut second = registry.open_wave("job-b");
        second.claim([key("fmt", "b")], "h2");

        let one = tokio::spawn(async move {
            first
                .await_other_waves(&[key("fmt", "b")], Duration::from_secs(30))
                .await
        });
        let two = tokio::spawn(async move {
            second
                .await_other_waves(&[key("fmt", "a")], Duration::from_secs(30))
                .await
        });
        promptly(async {
            one.await.unwrap();
            two.await.unwrap();
        })
        .await;
    }
}
