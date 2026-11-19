//! Placement's repository prices, computed once per immutable commit pair and
//! never on the placement path.
//!
//! Two prices order candidates for work on a repository: the object bytes a
//! machine still lacks to reach the target commit from the roots it already
//! holds (the sync cost), and how much of a warm slot's build the target
//! invalidates (the cache distance). Both are functions of commits alone, and
//! the object graph between two commits never changes, so the answer for one
//! (repository, warm roots, target) or (repository, slot head, target) is the
//! same number every time anyone asks.
//!
//! So a price is looked up, not computed. A miss schedules one computation on a
//! background thread and the placement that missed prices the candidate as
//! unmeasured, which ranks it but never gates it: both prices only break ties
//! behind predicted time. The next pass, or the next placement for the same
//! target, reads the answer.
//!
//! A sync price is keyed by (repository, machine, target) and stamped with the
//! revision of the machine's warm roots it was computed against, so a lookup
//! hashes three short strings and compares one integer, never the roots. When
//! the machine's roots move on, the previous price keeps being served while at
//! most one refresh runs, and a refresh for the same key is not started again
//! within [`REFRESH_INTERVAL`]. Roots only ever help, so the old price is an
//! upper bound on what the machine lacks now; a placement can be a little
//! stale in that direction, and only among candidates that tie on predicted
//! time.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::io::{BufRead, BufReader};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use cairn_common::cache_distance::CacheDistance;

use super::SyncCost;

/// Distance entries kept. Keys are commit pairs, so a long-lived runner sees
/// unboundedly many; the working set is the targets in flight times the slot
/// heads the fleet advertises.
const DISTANCE_ENTRY_LIMIT: usize = 512;

/// Sync entries kept, one per (repository, machine, target). The working set is
/// about 200 waiting targets times up to 16 priced machines, roughly 3,200;
/// 4,096 holds it with headroom, and an entry is three short strings and a few
/// integers, so the memo stays under a megabyte.
const SYNC_ENTRY_LIMIT: usize = 4096;

/// The least time between two walks for one (repository, machine, target). A
/// machine whose roots change every few seconds is priced against the roots it
/// had up to this long ago rather than keeping a git process running for it.
const REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Computations of one price allowed to run at once. A miss past this bound
/// schedules nothing and is asked again by the next pass, so a burst of new
/// targets cannot become a burst of git processes on a host short of them.
const REFINEMENTS_IN_FLIGHT_LIMIT: usize = 2;

type SyncWalk = dyn Fn(&str, &str, &[String]) -> Result<u64, String> + Send + Sync;
type DistanceMeasure = dyn Fn(&Path, &str, &str) -> Option<CacheDistance> + Send + Sync;

#[derive(Clone, PartialEq, Eq, Hash)]
struct SyncKey {
    repository: String,
    executor_id: String,
    target: String,
}

/// One machine's warm roots for one repository, as the sync price needs them:
/// the sorted, deduplicated list (shared, never copied) and the revision of the
/// machine's root set it was read at, which stands for the list in the memo.
pub(super) struct WarmHaves<'a> {
    pub(super) executor_id: &'a str,
    pub(super) revision: u64,
    pub(super) commits: Arc<[String]>,
}

impl Deref for WarmHaves<'_> {
    type Target = [String];

    fn deref(&self) -> &[String] {
        &self.commits
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct DistanceKey {
    repository: PathBuf,
    head: String,
    target: String,
}

/// The runner's repository prices for placement. One per [`super::Fleet`].
pub(super) struct PlacementPrices {
    sync: RefinedCache<SyncKey, SyncCost>,
    distance: RefinedCache<DistanceKey, Option<CacheDistance>>,
    walk: Arc<SyncWalk>,
    measure: Arc<DistanceMeasure>,
}

impl Default for PlacementPrices {
    fn default() -> Self {
        Self::with(
            missing_reachable_object_bytes,
            |repository: &Path, head: &str, target: &str| {
                cairn_common::cache_distance::cache_distance(repository, head, target).ok()
            },
        )
    }
}

impl PlacementPrices {
    pub(super) fn with(
        walk: impl Fn(&str, &str, &[String]) -> Result<u64, String> + Send + Sync + 'static,
        measure: impl Fn(&Path, &str, &str) -> Option<CacheDistance> + Send + Sync + 'static,
    ) -> Self {
        Self {
            sync: RefinedCache::new(SYNC_ENTRY_LIMIT, REFRESH_INTERVAL),
            distance: RefinedCache::new(DISTANCE_ENTRY_LIMIT, Duration::ZERO),
            walk: Arc::new(walk),
            measure: Arc::new(measure),
        }
    }

    /// The bytes a machine holding `haves` lacks to reach `target`, or
    /// [`SyncCost::Unknown`] while that has not been priced yet. Once priced, a
    /// change to the machine's roots keeps serving the earlier price while it
    /// is refreshed.
    pub(super) fn sync_cost(
        &self,
        repository: &str,
        haves: &WarmHaves<'_>,
        target: &str,
    ) -> SyncCost {
        let key = SyncKey {
            repository: repository.to_owned(),
            executor_id: haves.executor_id.to_owned(),
            target: target.to_owned(),
        };
        let walk = Arc::clone(&self.walk);
        let roots = Arc::clone(&haves.commits);
        let (repository, target) = (repository.to_owned(), target.to_owned());
        self.sync
            .get_or_refine(key, haves.revision, move || {
                walk(&repository, &target, &roots).map_or(SyncCost::Unknown, SyncCost::Known)
            })
            .unwrap_or(SyncCost::Unknown)
    }

    /// How far a slot at `head` sits from `target`, or `None` when that is
    /// unknowable or has not been measured yet.
    pub(super) fn cache_distance(
        &self,
        repository: &Path,
        head: &str,
        target: &str,
    ) -> Option<CacheDistance> {
        let key = DistanceKey {
            repository: repository.to_path_buf(),
            head: head.to_owned(),
            target: target.to_owned(),
        };
        let measure = Arc::clone(&self.measure);
        let job = key.clone();
        self.distance
            .get_or_refine(key, 0, move || {
                measure(&job.repository, &job.head, &job.target)
            })
            .flatten()
    }

    /// Computations started over this runner's life, sync walks and distance
    /// measurements together: each is the only place pricing starts git.
    #[cfg(test)]
    pub(super) fn computations_started(&self) -> u64 {
        self.sync.started() + self.distance.started()
    }

    /// Block until no computation is in flight.
    #[cfg(test)]
    pub(super) fn settle(&self) {
        self.sync.settle();
        self.distance.settle();
    }
}

/// A bounded memo whose misses are filled in the background.
///
/// Each value carries the revision of the inputs it was computed from. A
/// lookup at a different revision still returns the held value, and schedules
/// a recomputation unless the held one was started less than `refresh` ago.
struct RefinedCache<K, V> {
    shared: Arc<Shared<K, V>>,
    refresh: Duration,
}

struct Shared<K, V> {
    state: Mutex<State<K, V>>,
    settled: Condvar,
    started: AtomicU64,
    limit: usize,
}

struct State<K, V> {
    entries: HashMap<K, Entry<V>>,
    pending: HashSet<K>,
    tick: u64,
}

struct Entry<V> {
    value: V,
    /// The revision of the inputs `value` was computed from.
    revision: u64,
    /// When the computation that produced `value` started.
    computed_at: Instant,
    /// The tick this entry was last read or written at; the smallest is
    /// evicted first.
    used: u64,
}

impl<K, V> Shared<K, V> {
    fn lock(&self) -> MutexGuard<'_, State<K, V>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<K, V> RefinedCache<K, V>
where
    K: Clone + Eq + Hash + Send + 'static,
    V: Clone + Send + 'static,
{
    fn new(limit: usize, refresh: Duration) -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    entries: HashMap::new(),
                    pending: HashSet::new(),
                    tick: 0,
                }),
                settled: Condvar::new(),
                started: AtomicU64::new(0),
                limit,
            }),
            refresh,
        }
    }

    /// The value held for `key`, or `None` when nothing is held yet. A miss,
    /// or a held value computed at another revision and due a refresh,
    /// schedules `compute` to fill it at `revision`.
    fn get_or_refine(
        &self,
        key: K,
        revision: u64,
        compute: impl FnOnce() -> V + Send + 'static,
    ) -> Option<V> {
        let mut state = self.shared.lock();
        state.tick += 1;
        let tick = state.tick;
        let held = match state.entries.get_mut(&key) {
            Some(entry) => {
                entry.used = tick;
                if entry.revision == revision || entry.computed_at.elapsed() < self.refresh {
                    return Some(entry.value.clone());
                }
                Some(entry.value.clone())
            }
            None => None,
        };
        if state.pending.contains(&key) || state.pending.len() >= REFINEMENTS_IN_FLIGHT_LIMIT {
            return held;
        }
        state.pending.insert(key.clone());
        drop(state);
        self.shared.started.fetch_add(1, Ordering::Relaxed);
        let pending = Pending {
            shared: Arc::clone(&self.shared),
            key,
        };
        let computed_at = Instant::now();
        let spawned = std::thread::Builder::new()
            .name("cairn-placement-price".into())
            .spawn(move || {
                let value = compute();
                pending
                    .shared
                    .insert(pending.key.clone(), revision, computed_at, value);
            });
        if let Err(error) = spawned {
            // The closure, and the pending marker it owned, were dropped with
            // the failed spawn, so the next pass asks again.
            tracing::warn!(%error, "placement price refinement could not start a thread");
        }
        held
    }

    #[cfg(test)]
    fn started(&self) -> u64 {
        self.shared.started.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn settle(&self) {
        let mut state = self.shared.lock();
        while !state.pending.is_empty() {
            state = self
                .shared
                .settled
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    #[cfg(test)]
    fn contains(&self, key: &K) -> bool {
        self.shared.lock().entries.contains_key(key)
    }
}

impl<K: Clone + Eq + Hash, V> Shared<K, V> {
    fn insert(&self, key: K, revision: u64, computed_at: Instant, value: V) {
        let mut state = self.lock();
        state.tick += 1;
        let tick = state.tick;
        if !state.entries.contains_key(&key) && state.entries.len() >= self.limit {
            let oldest = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                state.entries.remove(&oldest);
            }
        }
        state.entries.insert(
            key,
            Entry {
                value,
                revision,
                computed_at,
                used: tick,
            },
        );
    }
}

/// A scheduled computation's claim on its key. Dropped when the computation
/// finishes, panics, or never starts, so a key is never left pending forever.
struct Pending<K: Eq + Hash, V> {
    shared: Arc<Shared<K, V>>,
    key: K,
}

impl<K: Eq + Hash, V> Drop for Pending<K, V> {
    fn drop(&mut self) {
        self.shared.lock().pending.remove(&self.key);
        self.shared.settled.notify_all();
    }
}

/// Canonical bytes of every object reachable from `base_commit` and not from
/// any of `warm_root_commits`.
///
/// Canonical object bytes are a stable placement approximation, not predicted
/// compressed wire bytes. Pack deltas, Git LFS, submodules, shallow/promisor
/// history, and stale advertised roots can all make the eventual transfer
/// differ. The object list streams from `rev-list` straight into `cat-file`
/// and the sizes are summed as they arrive, so a clone-sized walk never holds
/// the list in memory.
pub(super) fn missing_reachable_object_bytes(
    repository: &str,
    base_commit: &str,
    warm_root_commits: &[String],
) -> Result<u64, String> {
    let mut rev_list = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["rev-list", "--objects", "--no-object-names", base_commit])
        .args(warm_root_commits.iter().map(|commit| format!("^{commit}")))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("failed to enumerate repository objects: {error}"))?;
    let Some(objects) = rev_list.stdout.take() else {
        reap(rev_list);
        return Err("git rev-list stdout was unavailable".into());
    };
    let mut cat_file = match Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["cat-file", "--batch-check=%(objectsize)"])
        .stdin(Stdio::from(objects))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            reap(rev_list);
            return Err(format!("failed to inspect repository objects: {error}"));
        }
    };
    let Some(sizes) = cat_file.stdout.take() else {
        reap(cat_file);
        reap(rev_list);
        return Err("git cat-file stdout was unavailable".into());
    };
    let total = sum_object_sizes(BufReader::new(sizes));
    if total.is_err() {
        // Stop the walk rather than let it run to the end of a large graph
        // whose answer is already discarded.
        reap(cat_file);
        reap(rev_list);
        return total;
    }
    let listed = rev_list
        .wait_with_output()
        .map_err(|error| format!("failed to wait for git rev-list: {error}"))?;
    let inspected = cat_file
        .wait_with_output()
        .map_err(|error| format!("failed to wait for git cat-file: {error}"))?;
    if !listed.status.success() {
        return Err(String::from_utf8_lossy(&listed.stderr).trim().to_string());
    }
    if !inspected.status.success() {
        return Err(String::from_utf8_lossy(&inspected.stderr)
            .trim()
            .to_string());
    }
    total
}

fn sum_object_sizes(sizes: impl BufRead) -> Result<u64, String> {
    let mut total = 0_u64;
    for line in sizes.lines() {
        let line =
            line.map_err(|error| format!("failed to read repository object sizes: {error}"))?;
        let size = line
            .parse::<u64>()
            .map_err(|error| format!("git returned invalid object size {line:?}: {error}"))?;
        total = total
            .checked_add(size)
            .ok_or_else(|| "repository object size total overflowed u64".to_string())?;
    }
    Ok(total)
}

fn reap(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn counting() -> (PlacementPrices, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let walks = Arc::new(AtomicUsize::new(0));
        let measures = Arc::new(AtomicUsize::new(0));
        let (walked, measured) = (Arc::clone(&walks), Arc::clone(&measures));
        let prices = PlacementPrices::with(
            move |_, _, roots| {
                walked.fetch_add(1, Ordering::SeqCst);
                Ok(100 * (roots.len() as u64 + 1))
            },
            move |_, head, _| {
                measured.fetch_add(1, Ordering::SeqCst);
                (head != "unknowable").then(|| CacheDistance {
                    files: head.len() as u32,
                    ..Default::default()
                })
            },
        );
        (prices, walks, measures)
    }

    fn haves<'a>(executor_id: &'a str, revision: u64, commits: &[&str]) -> WarmHaves<'a> {
        WarmHaves {
            executor_id,
            revision,
            commits: commits.iter().map(|commit| (*commit).to_owned()).collect(),
        }
    }

    #[test]
    fn an_unpriced_pair_is_unknown_now_and_known_once_refined() {
        let (prices, walks, _) = counting();
        let roots = haves("m", 1, &["w"]);
        assert_eq!(prices.sync_cost("/repo", &roots, "t"), SyncCost::Unknown);
        prices.settle();
        assert_eq!(prices.sync_cost("/repo", &roots, "t"), SyncCost::Known(200));
        assert_eq!(walks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn moved_roots_serve_the_old_price_while_one_refresh_runs() {
        let (mut prices, walks, _) = counting();
        prices.sync = RefinedCache::new(SYNC_ENTRY_LIMIT, Duration::ZERO);
        prices.sync_cost("/repo", &haves("m", 1, &["a"]), "t");
        prices.settle();

        let moved = haves("m", 2, &["a", "b"]);
        assert_eq!(prices.sync_cost("/repo", &moved, "t"), SyncCost::Known(200));
        prices.settle();
        assert_eq!(walks.load(Ordering::SeqCst), 2);
        for _ in 0..3 {
            assert_eq!(prices.sync_cost("/repo", &moved, "t"), SyncCost::Known(300));
        }
        assert_eq!(walks.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn moved_roots_are_not_walked_again_within_the_refresh_interval() {
        let (prices, walks, _) = counting();
        prices.sync_cost("/repo", &haves("m", 1, &["a"]), "t");
        prices.settle();
        for revision in 2..5 {
            assert_eq!(
                prices.sync_cost("/repo", &haves("m", revision, &["a", "b"]), "t"),
                SyncCost::Known(200)
            );
            prices.settle();
        }
        assert_eq!(walks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn each_machine_is_priced_on_its_own() {
        let (prices, walks, _) = counting();
        prices.sync_cost("/repo", &haves("m", 1, &["a"]), "t");
        prices.sync_cost("/repo", &haves("n", 1, &["a"]), "t");
        prices.settle();
        assert_eq!(walks.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn a_failed_walk_is_remembered_as_unknown() {
        let walks = Arc::new(AtomicUsize::new(0));
        let walked = Arc::clone(&walks);
        let prices = PlacementPrices::with(
            move |_, _, _| {
                walked.fetch_add(1, Ordering::SeqCst);
                Err("bad object".into())
            },
            |_, _, _| None,
        );
        for _ in 0..3 {
            assert_eq!(
                prices.sync_cost("/repo", &haves("m", 0, &[]), "t"),
                SyncCost::Unknown
            );
            prices.settle();
        }
        assert_eq!(walks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn distances_are_measured_once_per_head_and_target() {
        let (prices, _, measures) = counting();
        let repository = Path::new("/repo");
        assert_eq!(prices.cache_distance(repository, "near", "t"), None);
        assert_eq!(prices.cache_distance(repository, "unknowable", "t"), None);
        prices.settle();
        for _ in 0..3 {
            assert_eq!(
                prices.cache_distance(repository, "near", "t"),
                Some(CacheDistance {
                    files: 4,
                    ..Default::default()
                })
            );
            assert_eq!(prices.cache_distance(repository, "unknowable", "t"), None);
        }
        assert_eq!(measures.load(Ordering::SeqCst), 2);
        assert_eq!(prices.computations_started(), 2);
    }

    #[test]
    fn misses_past_the_in_flight_bound_schedule_nothing() {
        let (release, gate) = std::sync::mpsc::channel::<()>();
        let gate = Mutex::new(gate);
        let prices = PlacementPrices::with(
            move |_, _, _| {
                let _ = gate.lock().unwrap().recv();
                Ok(1)
            },
            |_, _, _| None,
        );
        for target in ["a", "b", "c", "d"] {
            assert_eq!(
                prices.sync_cost("/repo", &haves("m", 0, &[]), target),
                SyncCost::Unknown
            );
        }
        assert_eq!(
            prices.computations_started(),
            REFINEMENTS_IN_FLIGHT_LIMIT as u64
        );
        for _ in 0..REFINEMENTS_IN_FLIGHT_LIMIT {
            release.send(()).unwrap();
        }
        prices.settle();
    }

    #[test]
    fn the_cache_is_bounded_and_evicts_the_least_recently_used() {
        const LIMIT: usize = 8;
        let cache: RefinedCache<usize, usize> = RefinedCache::new(LIMIT, Duration::ZERO);
        for key in 0..LIMIT {
            cache.shared.insert(key, 0, Instant::now(), key);
        }
        // Reading the first entry makes the second the oldest.
        assert_eq!(cache.get_or_refine(0, 0, || unreachable!()), Some(0));
        cache.shared.insert(LIMIT, 0, Instant::now(), LIMIT);
        assert_eq!(cache.shared.lock().entries.len(), LIMIT);
        assert!(cache.contains(&0));
        assert!(!cache.contains(&1));
        assert!(cache.contains(&LIMIT));
    }

    #[test]
    fn a_panicking_computation_releases_its_key() {
        let prices = PlacementPrices::with(|_, _, _| panic!("walk failed"), |_, _, _| None);
        assert_eq!(
            prices.sync_cost("/repo", &haves("m", 0, &[]), "t"),
            SyncCost::Unknown
        );
        prices.settle();
        assert!(prices.sync.shared.lock().pending.is_empty());
    }
}
