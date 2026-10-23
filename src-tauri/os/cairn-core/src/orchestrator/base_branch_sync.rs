//! One owner for "where does origin's copy of a base branch point, and when did
//! we last know".
//!
//! A base branch is any branch work is cut from and merges back into: the
//! project's default branch, and every integration branch an issue parent
//! confers on its children (see [`base_advance::load_tracked_base_branches`] for
//! exactly which). The shared jj store's copy of each is what `read ?branch=`,
//! `run branch:`, a new child's mint point and every replay resolve against, and
//! nothing reads origin at read time. So the store is only as current as its
//! last import, and every producer of that import feeds this module:
//!
//! - the GitHub `push` webhook (via the relay) observes a new tip;
//! - Cairn's own GitHub merge path observes the merge commit before fetching;
//! - a polling floor asks origin about every tracked branch with one
//!   `git ls-remote` per project every [`BASE_BRANCH_POLL_INTERVAL`], so the
//!   bound holds without the relay;
//! - startup catch-up and every origin fetch record what they learned.
//!
//! An observed tip the store does not hold schedules [`sync`], the single entry
//! point to the store's import of that branch, which runs at most once at a time
//! per branch. The two kinds of branch are imported differently because origin
//! means different things to them: origin is the sole authority for the default
//! branch ([`base_advance::reconcile_default_advance`]), while the store owns an
//! integration branch and takes origin's copy only as a pure fast-forward
//! ([`base_advance::reconcile_base_branch_advance`]). Every origin fetch goes
//! through the repository's fetch lock, so Cairn's own fetchers can no longer
//! race on a ref and lose.
//!
//! The state is in memory: it describes this process's knowledge of origin,
//! which a restart legitimately resets to "not checked since launch".
//!
//! [`base_advance::reconcile_default_advance`]:
//!     crate::orchestrator::base_advance::reconcile_default_advance
//! [`base_advance::reconcile_base_branch_advance`]:
//!     crate::orchestrator::base_advance::reconcile_base_branch_advance
//! [`base_advance::load_tracked_base_branches`]:
//!     crate::orchestrator::base_advance::load_tracked_base_branches

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::{Mutex as TokioMutex, Notify};

use crate::orchestrator::Orchestrator;

/// The polling floor. With the push relay healthy an advance of a tracked
/// branch arrives within its 15 s cadence; without it, this is the bound.
pub(crate) const BASE_BRANCH_POLL_INTERVAL: Duration = Duration::from_secs(60);

/// How long one project's origin check may take before the poll records it as
/// failed. Shorter than the poll interval, so a hung origin is reported by the
/// next tick instead of after the subprocess's own ten-minute network timeout.
const POLL_CHECK_TIMEOUT: Duration = Duration::from_secs(45);

/// A sync that keeps finding origin ahead after a successful pass stops here
/// and leaves the rest to the next observation, rather than spinning.
const MAX_PASSES_PER_SYNC: usize = 3;

/// Longest error excerpt a read header carries.
const ERROR_EXCERPT_CHARS: usize = 120;

/// Who told us where origin's copy of a branch points.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Source {
    Startup,
    Merge,
    Push,
    Poll,
    Fetch,
    /// The store's own push, verified at origin afterwards.
    Publish,
}

/// Which step a recorded failure happened in. A failed check means we do not
/// know where origin is; a failed sync means we know and the store has not
/// caught up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Check,
    Sync,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SyncError {
    pub at: DateTime<Utc>,
    pub phase: Phase,
    pub message: String,
}

/// What this process knows about one branch of one project on origin.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BranchSync {
    /// Last successful `ls-remote` or fetch against origin.
    pub checked_at: Option<DateTime<Utc>>,
    /// Origin's commit for the branch as last observed, by any source. `None`
    /// after a check that found origin holding no such branch.
    pub remote_tip: Option<String>,
    pub remote_tip_source: Option<Source>,
    /// Set while a reconcile toward `remote_tip` is running.
    pub sync_started_at: Option<DateTime<Utc>>,
    pub last_error: Option<SyncError>,
    /// The project has no `origin` remote, so the store's branch is the
    /// authority and there is nothing to be behind.
    pub no_origin: bool,
}

#[derive(Default)]
struct BranchEntry {
    state: BranchSync,
    in_flight: bool,
}

type BranchKey = (String, String);

fn key(project_id: &str, branch: &str) -> BranchKey {
    (project_id.to_string(), branch.to_string())
}

/// Per-process registry behind [`Orchestrator::base_branch_sync`].
#[derive(Default)]
pub(crate) struct BaseBranchSyncRegistry {
    branches: Mutex<HashMap<BranchKey, BranchEntry>>,
    /// Keyed by jj store directory: one store per repository, and every fetch
    /// writes that repository's refs.
    fetch_locks: Mutex<HashMap<PathBuf, Arc<TokioMutex<()>>>>,
    polls_in_flight: Mutex<HashSet<String>>,
    idle: Notify,
}

impl BaseBranchSyncRegistry {
    pub(crate) fn snapshot(&self, project_id: &str, branch: &str) -> Option<BranchSync> {
        self.branches
            .lock()
            .unwrap()
            .get(&key(project_id, branch))
            .map(|entry| entry.state.clone())
    }

    fn with_state<T>(
        &self,
        project_id: &str,
        branch: &str,
        f: impl FnOnce(&mut BranchSync) -> T,
    ) -> T {
        let mut branches = self.branches.lock().unwrap();
        f(&mut branches.entry(key(project_id, branch)).or_default().state)
    }

    /// Record a tip learned without asking origin ourselves (webhook, merge).
    pub(crate) fn record_tip(&self, project_id: &str, branch: &str, tip: &str, source: Source) {
        self.with_state(project_id, branch, |state| {
            state.remote_tip = Some(tip.to_string());
            state.remote_tip_source = Some(source);
            state.no_origin = false;
        });
    }

    /// Record a successful look at origin. `tip` is `None` when the check
    /// succeeded but could not name the commit, which leaves the last known tip
    /// in place.
    pub(crate) fn record_check(
        &self,
        project_id: &str,
        branch: &str,
        tip: Option<String>,
        source: Source,
        at: DateTime<Utc>,
    ) {
        self.with_state(project_id, branch, |state| {
            state.checked_at = Some(at);
            state.no_origin = false;
            if let Some(tip) = tip {
                state.remote_tip = Some(tip);
                state.remote_tip_source = Some(source);
            }
            if state
                .last_error
                .as_ref()
                .is_some_and(|error| error.phase == Phase::Check)
            {
                log::info!("origin check of `{branch}` for project {project_id} recovered");
                state.last_error = None;
            }
        });
    }

    /// Record a check that found origin holding no such branch: there is
    /// nothing to be behind. An integration branch the store has not published
    /// yet, or one deleted once its parent merged.
    pub(crate) fn record_absent(
        &self,
        project_id: &str,
        branch: &str,
        source: Source,
        at: DateTime<Utc>,
    ) {
        self.record_check(project_id, branch, None, source, at);
        self.with_state(project_id, branch, |state| {
            state.remote_tip = None;
            state.remote_tip_source = None;
        });
    }

    pub(crate) fn record_no_origin(&self, project_id: &str, branch: &str) {
        self.with_state(project_id, branch, |state| {
            *state = BranchSync {
                no_origin: true,
                ..BranchSync::default()
            };
        });
    }

    /// Record a failure, warning only when it is new: a poll that fails the
    /// same way every minute should not flood the log.
    pub(crate) fn record_error(
        &self,
        project_id: &str,
        branch: &str,
        phase: Phase,
        message: &str,
        at: DateTime<Utc>,
    ) {
        self.with_state(project_id, branch, |state| {
            let repeat = state
                .last_error
                .as_ref()
                .is_some_and(|error| error.phase == phase && error.message == message);
            if repeat {
                log::debug!(
                    "origin {phase:?} of `{branch}` for project {project_id} still failing: {message}"
                );
            } else {
                log::warn!(
                    "origin {phase:?} of `{branch}` for project {project_id} failed: {message}"
                );
            }
            state.last_error = Some(SyncError {
                at,
                phase,
                message: message.to_string(),
            });
        });
    }

    fn record_sync_success(&self, project_id: &str, branch: &str) {
        self.with_state(project_id, branch, |state| {
            if state.last_error.take().is_some() {
                log::info!("sync of `{branch}` for project {project_id} recovered");
            }
        });
    }

    fn try_begin(&self, project_id: &str, branch: &str) -> bool {
        let mut branches = self.branches.lock().unwrap();
        let entry = branches.entry(key(project_id, branch)).or_default();
        if entry.in_flight {
            return false;
        }
        entry.in_flight = true;
        true
    }

    fn in_flight(&self, project_id: &str, branch: &str) -> bool {
        self.branches
            .lock()
            .unwrap()
            .get(&key(project_id, branch))
            .is_some_and(|entry| entry.in_flight)
    }

    fn mark_started(&self, project_id: &str, branch: &str, at: DateTime<Utc>) {
        self.with_state(project_id, branch, |state| state.sync_started_at = Some(at));
    }

    fn end(&self, project_id: &str, branch: &str) {
        {
            let mut branches = self.branches.lock().unwrap();
            if let Some(entry) = branches.get_mut(&key(project_id, branch)) {
                entry.in_flight = false;
                entry.state.sync_started_at = None;
            }
        }
        self.idle.notify_waiters();
    }

    async fn wait_idle(&self, project_id: &str, branch: &str) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.in_flight(project_id, branch) {
                return;
            }
            notified.await;
        }
    }

    fn begin_poll(&self, project_id: &str) -> bool {
        self.polls_in_flight
            .lock()
            .unwrap()
            .insert(project_id.to_string())
    }

    fn end_poll(&self, project_id: &str) {
        self.polls_in_flight.lock().unwrap().remove(project_id);
    }

    fn fetch_lock(&self, store: &Path) -> Arc<TokioMutex<()>> {
        self.fetch_locks
            .lock()
            .unwrap()
            .entry(store.to_path_buf())
            .or_insert_with(|| Arc::new(TokioMutex::new(())))
            .clone()
    }
}

/// Clears the in-flight mark however the sync ends, a panic included, so one
/// failed pass can never leave a branch unable to sync again.
struct InFlight<'a> {
    registry: &'a BaseBranchSyncRegistry,
    project_id: &'a str,
    branch: &'a str,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.registry.end(self.project_id, self.branch);
    }
}

/// The single-flight driver. The caller that begins runs one pass
/// unconditionally, then another only while the pass succeeded and
/// `still_behind` says origin moved again meanwhile. A caller that finds a sync
/// already running waits for it instead of starting a second: the running
/// sync's recheck covers any tip observed while it worked.
async fn drive<P, PF, B, BF>(
    registry: &BaseBranchSyncRegistry,
    project_id: &str,
    branch: &str,
    mut pass: P,
    mut still_behind: B,
) where
    P: FnMut() -> PF,
    PF: Future<Output = Result<(), String>>,
    B: FnMut() -> BF,
    BF: Future<Output = bool>,
{
    if !registry.try_begin(project_id, branch) {
        registry.wait_idle(project_id, branch).await;
        return;
    }
    let _in_flight = InFlight {
        registry,
        project_id,
        branch,
    };
    for _ in 0..MAX_PASSES_PER_SYNC {
        registry.mark_started(project_id, branch, Utc::now());
        match pass().await {
            Ok(()) => registry.record_sync_success(project_id, branch),
            Err(error) => {
                registry.record_error(project_id, branch, Phase::Sync, &error, Utc::now());
                return;
            }
        }
        if !still_behind().await {
            return;
        }
    }
    log::warn!(
        "sync of `{branch}` for project {project_id} still behind origin after \
         {MAX_PASSES_PER_SYNC} passes; the next observation will retry"
    );
}

/// Bring the store's copy of `branch` to origin's tip and reconcile everything
/// downstream of it, at most once at a time per branch. Returns once a pass
/// that started no earlier than this call has finished.
pub(crate) async fn sync(orch: &Orchestrator, project_id: &str, branch: &str) {
    let store = match project_repo_path(orch, project_id).await {
        Some(repo) => crate::jj::project_store_dir(&orch.config_dir, &repo),
        None => {
            log::debug!("base-branch sync: project {project_id} has no local repository");
            return;
        }
    };
    let is_default = project_default_branch(orch, project_id).await.as_deref() == Some(branch);
    drive(
        &orch.base_branch_sync,
        project_id,
        branch,
        || async move {
            if is_default {
                crate::orchestrator::base_advance::reconcile_default_advance(
                    orch, project_id, branch,
                )
                .await
            } else {
                crate::orchestrator::base_advance::reconcile_base_branch_advance(
                    orch, project_id, branch,
                )
                .await
            }
        },
        || store_lags_remote(orch, project_id, &store, branch),
    )
    .await;
}

/// Record that origin's `branch` is at `tip`, and if the store does not hold
/// it, start a [`sync`] in the background. Cheap for an unchanged tip: one
/// in-process view lookup, no subprocess.
///
/// The caller has decided `branch` is one this module tracks (the default, or
/// a base branch with live work on it); an agent branch nothing is cut from is
/// the store's alone, and origin's view of it is not news.
pub(crate) async fn observe_remote_tip(
    orch: &Orchestrator,
    project_id: &str,
    branch: &str,
    tip: &str,
    source: Source,
) {
    orch.base_branch_sync
        .record_tip(project_id, branch, tip, source);
    let Some(repo) = project_repo_path(orch, project_id).await else {
        return;
    };
    let store = crate::jj::project_store_dir(&orch.config_dir, &repo);
    if !store_lags_remote(orch, project_id, &store, branch).await {
        return;
    }
    log::info!(
        "origin `{branch}` for project {project_id} is at {} ({source:?}); syncing the store",
        short(tip)
    );
    let orch = orch.clone();
    let project_id = project_id.to_string();
    let branch = branch.to_string();
    tokio::spawn(async move { sync(&orch, &project_id, &branch).await });
}

/// Whether the store's local bookmark for `branch` (the one `?branch=`
/// resolves) does not yet contain origin's last observed tip. Judging the local
/// bookmark rather than `<branch>@origin` matters: a sync can import the remote
/// ref and still fail to move the bookmark, and only the bookmark is what
/// readers get. An unknown tip is never "behind"; an unresolvable bookmark is,
/// so a missing store gets provisioned by the sync.
async fn store_lags_remote(
    orch: &Orchestrator,
    project_id: &str,
    store: &Path,
    branch: &str,
) -> bool {
    let Some(tip) = orch
        .base_branch_sync
        .snapshot(project_id, branch)
        .and_then(|state| state.remote_tip)
    else {
        return false;
    };
    let Ok(served) = cairn_vcs::resolve_coordinate(store, branch).await else {
        return true;
    };
    store_lacks_origin(orch, store, branch, &served, &tip).await
}

/// Whether the store's `local` copy of `branch` is missing something origin's
/// `remote` holds. Not containing origin's tip is not enough: after the store
/// rewrites a branch it published (a base-advance replay), origin holds the
/// pre-rewrite copy until the next push, and that copy is the store's own
/// replaced history rather than anything to take. See
/// [`crate::jj::origin_copy_is_superseded`].
async fn store_lacks_origin(
    orch: &Orchestrator,
    store: &Path,
    branch: &str,
    local: &str,
    remote: &str,
) -> bool {
    let contains = cairn_vcs::commit_stamp(store, local, Some(remote))
        .await
        .ok()
        .and_then(|stamp| stamp.contains);
    if contains == Some(true) {
        return false;
    }
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let (store, branch, local, remote) = (
        store.to_path_buf(),
        branch.to_string(),
        local.to_string(),
        remote.to_string(),
    );
    let superseded = tokio::task::spawn_blocking(move || {
        crate::jj::origin_copy_is_superseded(&jj, &store, &branch, &local, &remote)
    })
    .await
    .map_err(|error| error.to_string())
    .and_then(|answer| answer);
    match superseded {
        Ok(superseded) => !superseded,
        Err(error) => {
            log::debug!("could not tell whether origin's copy is the store's own: {error}");
            true
        }
    }
}

/// The store's copy of a branch, known not to contain origin's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OriginDrift {
    pub branch: String,
    /// The store's bookmark, when it resolves.
    pub local: Option<String>,
    pub remote: String,
    pub state: BranchSync,
}

impl OriginDrift {
    /// One sentence naming both commits and why the store has not caught up.
    pub(crate) fn describe(&self, now: DateTime<Utc>) -> String {
        let local = self
            .local
            .as_deref()
            .map(|local| format!("`{}`", short(local)))
            .unwrap_or_else(|| "nothing that resolves".to_string());
        format!(
            "the store's `{branch}` is at {local}, but origin's is at `{remote}`, which it does not \
             contain ({why})",
            branch = self.branch,
            remote = short(&self.remote),
            why = stale_reason(&self.state, now),
        )
    }
}

/// Whether this process knows the store's `branch` to be behind (or diverged
/// from) origin's copy, judged on the bookmark as it stands now. `None` when
/// it is current, when origin holds only the store's own replaced publication
/// of it, or when nothing is known about origin's copy.
pub(crate) async fn known_drift(
    orch: &Orchestrator,
    project_id: &str,
    store: &Path,
    branch: &str,
) -> Option<OriginDrift> {
    let state = orch.base_branch_sync.snapshot(project_id, branch)?;
    let remote = state.remote_tip.clone()?;
    let local = cairn_vcs::resolve_coordinate(store, branch).await.ok();
    let lacks = match local.as_deref() {
        Some(local) => store_lacks_origin(orch, store, branch, local, &remote).await,
        None => true,
    };
    lacks.then(|| OriginDrift {
        branch: branch.to_string(),
        local,
        remote,
        state,
    })
}

/// Refuse to publish `branch` from `store` while the store's copy is known not
/// to contain origin's, naming the drift.
///
/// Publishing then would push a commit that does not descend from origin's tip:
/// an unforced push fails at origin with a message about nothing in particular,
/// and a forced one would erase whatever origin gained. Origin holding the
/// store's own pre-rewrite publication is not drift: the store rewrote what it
/// published, and the publication is the lease-protected sideways move that
/// replaces it (see [`known_drift`]). The knowledge can be a
/// minute stale (Cairn's own rewrite of an integration branch moves origin
/// before the next poll sees it), so a known drift is first re-checked through
/// the ordinary [`sync`], which fetches. A pure fast-forward is taken there and
/// the publication goes ahead; only a branch still behind afterwards is refused.
pub(crate) async fn refuse_if_behind_origin(
    orch: &Orchestrator,
    store: &Path,
    branch: &str,
) -> Result<(), String> {
    for project_id in projects_for_store(orch, store).await {
        if known_drift(orch, &project_id, store, branch)
            .await
            .is_none()
        {
            continue;
        }
        sync_boxed(orch.clone(), project_id.clone(), branch.to_string()).await;
        if let Some(drift) = known_drift(orch, &project_id, store, branch).await {
            return Err(format!(
                "Refusing to publish `{branch}`: {}. Publishing it would push a commit that does \
                 not descend from origin's tip. The store takes origin's copy itself when it is a \
                 fast-forward; `cairn:~/rebase` on the branch's node says where that stands.",
                drift.describe(Utc::now())
            ));
        }
    }
    Ok(())
}

/// Record that the store just published `branch` and origin now holds `tip`.
///
/// A publication is the one move of origin this process makes itself, and the
/// push verified it, so it is an observation like any check. Without it the
/// last poll's tip outlives the push: after a sideways publication of a
/// rewritten branch, the store no longer has that tip as `<branch>@origin`, and
/// the branch would read as drifted until the next poll. Only a branch this
/// module already tracks is recorded; publishing does not start tracking one.
pub(crate) async fn record_publication(orch: &Orchestrator, store: &Path, branch: &str, tip: &str) {
    for project_id in projects_for_store(orch, store).await {
        if orch
            .base_branch_sync
            .snapshot(&project_id, branch)
            .is_some()
        {
            orch.base_branch_sync.record_check(
                &project_id,
                branch,
                Some(tip.to_string()),
                Source::Publish,
                Utc::now(),
            );
        }
    }
}

/// How long job preparation waits to learn whether origin's copy of a new
/// child's base is ahead, and to take it if so, before cutting the child
/// anyway and saying so.
const MINT_CHECK_TIMEOUT: Duration = Duration::from_secs(15);
const MINT_SYNC_TIMEOUT: Duration = Duration::from_secs(60);

/// Before a child is cut from a non-default base branch: ask origin where the
/// base is, take its copy if it is ahead and a pure fast-forward, and answer a
/// note for the child's opening prompt when the store's copy is STILL known to
/// be behind. `None` means the base is current, or nothing says otherwise.
///
/// Synchronous because job preparation is. The work runs on the
/// orchestrator's own runtime, never a throwaway one, so the replay the sync
/// starts for the base's other children is not cancelled when this returns.
pub(crate) fn bring_base_current_for_mint(
    orch: &Orchestrator,
    project_id: &str,
    base_branch: &str,
) -> Option<String> {
    let (answer, answered) = std::sync::mpsc::channel();
    let task = {
        let orch = orch.clone();
        let project_id = project_id.to_string();
        let base_branch = base_branch.to_string();
        async move {
            let _ = answer.send(bring_base_current(&orch, &project_id, &base_branch).await);
        }
    };
    orch.spawn_detached(task);
    match answered.recv_timeout(MINT_CHECK_TIMEOUT + MINT_SYNC_TIMEOUT + Duration::from_secs(5)) {
        Ok(note) => note,
        Err(_) => {
            log::warn!(
                "job preparation: could not settle origin's `{base_branch}` for project \
                 {project_id} in time; cutting from the store's copy"
            );
            None
        }
    }
}

async fn bring_base_current(
    orch: &Orchestrator,
    project_id: &str,
    base_branch: &str,
) -> Option<String> {
    if project_default_branch(orch, project_id).await.as_deref() == Some(base_branch) {
        return None;
    }
    let repo = project_repo_path(orch, project_id).await?;
    let store = crate::jj::project_store_dir(&orch.config_dir, &repo);
    poll_project_within(
        orch,
        project_id,
        &repo,
        &[base_branch.to_string()],
        MINT_CHECK_TIMEOUT,
    )
    .await;
    if store_lags_remote(orch, project_id, &store, base_branch).await {
        // The check's observation already started a sync; this waits for it.
        let _ = tokio::time::timeout(MINT_SYNC_TIMEOUT, sync(orch, project_id, base_branch)).await;
    }
    let drift = known_drift(orch, project_id, &store, base_branch).await?;
    Some(format!(
        "## Your starting point is behind origin\n\nThis branch was cut from `{base_branch}` as \
         the store holds it, but {}. Cairn could not take origin's copy before starting you. \
         Once the store takes it, your branch is replayed onto it automatically; until then \
         `cairn:~/rebase` says where that stands, and anything you read of `{base_branch}` is the \
         store's older copy.",
        drift.describe(Utc::now())
    ))
}

/// [`sync`] behind a nameable `Send` future type, for the one caller inside a
/// cycle: publication re-checks through `sync`, `sync` replays the children of
/// the branch, and a replay publishes. The compiler cannot compute an async
/// fn's future type through that recursion; a plain fn's signature ends it.
fn sync_boxed(
    orch: Orchestrator,
    project_id: String,
    branch: String,
) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move { sync(&orch, &project_id, &branch).await })
}

/// The sanctioned "fast-forward from origin" for one branch, run now rather
/// than at the next poll, with the outcome in a sentence. It is the same
/// [`sync`] the poll and the webhook start, so it takes origin's copy only as a
/// pure fast-forward and replays the work cut from the branch afterwards.
pub(crate) async fn fast_forward_now(
    orch: &Orchestrator,
    project_id: &str,
    branch: &str,
) -> Result<String, String> {
    let repo = project_repo_path(orch, project_id)
        .await
        .ok_or_else(|| "Cairn holds no repository path for this project".to_string())?;
    let store = crate::jj::project_store_dir(&orch.config_dir, &repo);
    let before = cairn_vcs::resolve_coordinate(&store, branch).await.ok();
    sync(orch, project_id, branch).await;
    let after = cairn_vcs::resolve_coordinate(&store, branch).await.ok();
    let state = orch
        .base_branch_sync
        .snapshot(project_id, branch)
        .unwrap_or_default();
    if let Some(error) = state.last_error {
        return Err(format!(
            "`{branch}` was not fast-forwarded from origin: {}",
            error.message
        ));
    }
    let named = |commit: &Option<String>| {
        commit
            .as_deref()
            .map(|commit| format!("`{}`", short(commit)))
            .unwrap_or_else(|| "nothing".to_string())
    };
    Ok(match (&before, &after, state.remote_tip.as_deref()) {
        (before, after, _) if before != after => format!(
            "Fast-forwarded `{branch}` from {} to origin's {}. Work cut from it is being \
             replayed onto the new tip.",
            named(before),
            named(after)
        ),
        (_, _, None) => format!("Origin holds no `{branch}`, so there is nothing to take."),
        (_, _, Some(tip)) => format!(
            "`{branch}` already contains origin's `{}`; there is nothing to take.",
            short(tip)
        ),
    })
}

/// The projects whose shared store is `store`. Publication is addressed by
/// store, and the sync's knowledge by project.
async fn projects_for_store(orch: &Orchestrator, store: &Path) -> Vec<String> {
    match crate::orchestrator::base_advance::load_projects_for_default_reconcile(orch).await {
        Ok(projects) => projects
            .into_iter()
            .filter(|(_, project)| {
                crate::jj::project_store_dir(&orch.config_dir, Path::new(&project.repo_path))
                    == store
            })
            .map(|(_, project)| project.id)
            .collect(),
        Err(error) => {
            log::warn!("could not list projects to judge origin drift: {error}");
            Vec::new()
        }
    }
}

/// Which project and branch a serialized fetch should record its result
/// against. Fetches that should not be recorded pass `None`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FetchRecord<'a> {
    pub project_id: &'a str,
    pub source: Source,
}

/// Run `fetch` under the repository's fetch lock. Every FETCH Cairn runs into
/// the repository goes through this, so two of them can no longer race on one
/// ref and have the loser give up.
///
/// Fetches are not the only writers of those refs. The store's own `jj git
/// push` updates `refs/remotes/origin/<branch>` for the branch it pushed, outside
/// this lock. That is safe because no two writers share a ref any more: every
/// fetch here names exactly one branch, a store-owned branch's origin copy is
/// fetched only into a private observation ref and promoted under the store
/// lock once it is known to be a fast-forward, and the store's import refuses to
/// let any tracking ref walk a store-owned branch backwards (see
/// `cairn_vcs::jj::import_git`).
///
/// `fetch` is a closure so the work is only created once the lock is held: a
/// `spawn_blocking` handle passed in directly would already be running.
///
/// Never acquire the jj store lock while holding this one: the merge fold
/// takes them in the opposite order.
pub(crate) async fn with_fetch_lock<T, F>(
    orch: &Orchestrator,
    store: &Path,
    fetch: impl FnOnce() -> F,
) -> T
where
    F: Future<Output = T>,
{
    let lock = orch.base_branch_sync.fetch_lock(store);
    let _guard = lock.lock().await;
    fetch().await
}

/// Fetch origin's `branch`, and only that branch, under the repository's fetch
/// lock, recording what it learned when `record` names the project. For the
/// default branch, whose authority is origin.
///
/// Scoped on purpose. A whole-remote fetch also rewrote every agent branch's
/// tracking ref from origin's advertisement, and an advertisement that lagged a
/// push this store had already made walked two agent branches backwards, hiding
/// their newest commits. Origin does not own an agent branch, so a fetch on the
/// default branch's behalf has no business touching one.
///
/// A fetch outside Cairn (the operator's own `git fetch`) can still race this
/// one on a ref lock. Git then reports `cannot lock ref '<ref>': is at <X> but
/// expected <Y>`; when `<ref>` now holds `<X>`, the other fetch landed the same
/// update and this one is treated as having succeeded.
pub(crate) async fn fetch_origin_serialized(
    orch: &Orchestrator,
    repo_path: &Path,
    branch: &str,
    record: Option<FetchRecord<'_>>,
) -> Result<(), String> {
    let store = crate::jj::project_store_dir(&orch.config_dir, repo_path);
    let git = orch.services.git.clone();
    let repo = repo_path.to_path_buf();
    let fetched_branch = branch.to_string();
    let result = with_fetch_lock(orch, &store, || async {
        tokio::task::spawn_blocking(move || {
            git.fetch_origin_branch(&repo, &fetched_branch).or_else(|error| {
                if lost_ref_race_is_settled(&repo, &error) {
                    log::debug!("git fetch origin lost a ref-lock race to a fetch that landed the same update: {error}");
                    Ok(())
                } else {
                    Err(error)
                }
            })
        })
        .await
        .map_err(|error| format!("git fetch origin task failed: {error}"))?
    })
    .await;
    if let Some(record) = record {
        let registry = &orch.base_branch_sync;
        match &result {
            Ok(()) => {
                let repo = repo_path.to_path_buf();
                let tracking = format!("refs/remotes/origin/{branch}");
                let tip = tokio::task::spawn_blocking(move || read_ref(&repo, &tracking))
                    .await
                    .ok()
                    .flatten();
                registry.record_check(record.project_id, branch, tip, record.source, Utc::now());
            }
            Err(error) => {
                registry.record_error(record.project_id, branch, Phase::Check, error, Utc::now())
            }
        }
    }
    result
}

/// Fetch origin's copy of a STORE-OWNED base branch into its private
/// observation ref, under the repository's fetch lock, and record what it
/// learned. `Ok(None)` when origin has no such branch.
///
/// Never into `refs/remotes/origin/<branch>`: whether origin's copy may enter
/// the store at all is [`crate::jj::fast_forward_branch_from_origin`]'s
/// decision, made under the store lock.
pub(crate) async fn fetch_origin_observation_serialized(
    orch: &Orchestrator,
    repo_path: &Path,
    branch: &str,
    record: FetchRecord<'_>,
) -> Result<Option<String>, String> {
    let store = crate::jj::project_store_dir(&orch.config_dir, repo_path);
    let fetch_store = store.clone();
    let fetched_branch = branch.to_string();
    let result = with_fetch_lock(orch, &store, || async {
        tokio::task::spawn_blocking(move || {
            crate::jj::fetch_origin_observation(&fetch_store, &fetched_branch)
        })
        .await
        .map_err(|error| format!("git fetch origin task failed: {error}"))?
    })
    .await;
    let registry = &orch.base_branch_sync;
    match &result {
        Ok(Some(tip)) => registry.record_check(
            record.project_id,
            branch,
            Some(tip.clone()),
            record.source,
            Utc::now(),
        ),
        Ok(None) => registry.record_absent(record.project_id, branch, record.source, Utc::now()),
        Err(error) => {
            registry.record_error(record.project_id, branch, Phase::Check, error, Utc::now())
        }
    }
    result
}

/// `(ref, value it is at)` for every `cannot lock ref` race in git's stderr.
pub(crate) fn parse_ref_lock_races(stderr: &str) -> Vec<(String, String)> {
    stderr
        .lines()
        .filter_map(|line| {
            let rest = &line[line.find("cannot lock ref '")? + "cannot lock ref '".len()..];
            let (refname, rest) = rest.split_once("': is at ")?;
            let (at, _) = rest.split_once(" but expected ")?;
            Some((refname.to_string(), at.trim().to_string()))
        })
        .collect()
}

fn lost_ref_race_is_settled(repo: &Path, stderr: &str) -> bool {
    let races = parse_ref_lock_races(stderr);
    !races.is_empty()
        && races
            .iter()
            .all(|(refname, at)| read_ref(repo, refname).as_deref() == Some(at.as_str()))
}

/// The commit a ref names right now, read directly so it reflects whatever
/// any fetcher, ours or not, last wrote.
fn read_ref(repo: &Path, refname: &str) -> Option<String> {
    let output = crate::env::git()
        .args(["rev-parse", "--verify", "--quiet", refname])
        .current_dir(repo)
        .output()
        .ok()?;
    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (output.status.success() && !value.is_empty()).then_some(value)
}

/// Start the polling floor. Ticks on a fixed cadence regardless of how long any
/// one origin takes to answer, and each pass runs as its own task so a panic
/// retires one pass rather than the loop.
pub(crate) fn spawn_base_branch_poll(orch: &Orchestrator) {
    let orch = orch.clone();
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(BASE_BRANCH_POLL_INTERVAL);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            let pass = orch.clone();
            let joined = crate::orchestrator::base_advance::supervised_sweep_pass(async move {
                poll_once(&pass).await;
            })
            .await;
            if let Err(error) = joined {
                log::error!("base-branch poll pass ended abnormally; retrying next cycle: {error}");
            }
        }
    });
}

/// Start one check per local project with a default branch, each in its own
/// task: a slow or unreachable origin delays only its own project. A project
/// whose previous check is still running is skipped this tick; that check's
/// timeout already records it as failing.
pub(crate) async fn poll_once(orch: &Orchestrator) {
    let projects =
        match crate::orchestrator::base_advance::load_projects_for_default_reconcile(orch).await {
            Ok(projects) => projects,
            Err(error) => {
                log::warn!("base-branch poll: failed to load projects: {error}");
                return;
            }
        };
    for (db, project) in projects {
        if !orch.base_branch_sync.begin_poll(&project.id) {
            continue;
        }
        let orch = orch.clone();
        tokio::spawn(async move {
            let _polling = Polling {
                registry: orch.base_branch_sync.clone(),
                project_id: project.id.clone(),
            };
            let mut branches = vec![project.default_branch.clone()];
            match crate::orchestrator::base_advance::load_tracked_base_branches(
                &db,
                &project.id,
                &project.default_branch,
            )
            .await
            {
                Ok(tracked) => branches.extend(tracked),
                Err(error) => log::warn!(
                    "base-branch poll: could not list project {}'s base branches; checking only \
                     `{}`: {error}",
                    project.id,
                    project.default_branch
                ),
            }
            poll_project(&orch, &project.id, Path::new(&project.repo_path), &branches).await;
        });
    }
}

/// Clears a project's poll-in-flight mark however its check ends.
struct Polling {
    registry: Arc<BaseBranchSyncRegistry>,
    project_id: String,
}

impl Drop for Polling {
    fn drop(&mut self) {
        self.registry.end_poll(&self.project_id);
    }
}

/// Ask origin about every branch in `branches` in one `ls-remote`, and sync
/// each one the store does not hold.
pub(crate) async fn poll_project(
    orch: &Orchestrator,
    project_id: &str,
    repo_path: &Path,
    branches: &[String],
) {
    poll_project_within(orch, project_id, repo_path, branches, POLL_CHECK_TIMEOUT).await
}

async fn poll_project_within(
    orch: &Orchestrator,
    project_id: &str,
    repo_path: &Path,
    branches: &[String],
    timeout: Duration,
) {
    let git = orch.services.git.clone();
    let repo = repo_path.to_path_buf();
    let asked = branches.to_vec();
    // The subprocess keeps its own, much longer network timeout; this bound is
    // how long the poll waits before saying the check failed.
    let answer = tokio::time::timeout(
        timeout,
        tokio::task::spawn_blocking(move || {
            let has_origin = git
                .remote_get_url(&repo)
                .ok()
                .is_some_and(|url| !url.trim().is_empty());
            has_origin.then(|| git.ls_remote_branches(&repo, "origin", &asked))
        }),
    )
    .await;
    let registry = &orch.base_branch_sync;
    let failed = |message: &str| {
        for branch in branches {
            registry.record_error(project_id, branch, Phase::Check, message, Utc::now());
        }
    };
    match answer {
        Ok(Ok(None)) => {
            for branch in branches {
                registry.record_no_origin(project_id, branch);
            }
        }
        Ok(Ok(Some(Ok(tips)))) => {
            for branch in branches {
                match tips.get(branch) {
                    Some(tip) => {
                        registry.record_check(
                            project_id,
                            branch,
                            Some(tip.clone()),
                            Source::Poll,
                            Utc::now(),
                        );
                        observe_remote_tip(orch, project_id, branch, tip, Source::Poll).await;
                    }
                    None => registry.record_absent(project_id, branch, Source::Poll, Utc::now()),
                }
            }
        }
        Ok(Ok(Some(Err(error)))) => failed(&error),
        Ok(Err(error)) => failed(&format!("ls-remote task failed: {error}")),
        Err(_) => failed(&format!(
            "git ls-remote origin did not answer within {}s",
            timeout.as_secs()
        )),
    }
}

async fn project_repo_path(orch: &Orchestrator, project_id: &str) -> Option<PathBuf> {
    let db = crate::execution::routing::owning_db_for_project(&orch.db, project_id)
        .await
        .ok()?;
    crate::orchestrator::base_advance::load_project_repo_path(&db, project_id)
        .await
        .ok()
        .flatten()
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
}

async fn project_default_branch(orch: &Orchestrator, project_id: &str) -> Option<String> {
    let db = crate::execution::routing::owning_db_for_project(&orch.db, project_id)
        .await
        .ok()?;
    crate::orchestrator::base_advance::load_project_default_branch(&db, project_id)
        .await
        .ok()
        .flatten()
}

// ---------------------------------------------------------------------------
// The revision label read surfaces render.
// ---------------------------------------------------------------------------

/// The label naming what a branch-scoped read or run was served from:
/// `agent/foo@1a2b3c4d committed 12m ago`, and for a branch this module tracks
/// (the default branch, or a base branch with live work on it), how current the
/// store's copy of origin is. Computed at read time so the rendered header is a
/// pure function of the segment's metadata.
pub(crate) async fn revision_label(
    orch: &Orchestrator,
    project_id: &str,
    rev: &str,
    default_branch: &str,
    store: &Path,
    commit_id: &str,
) -> String {
    let state = orch.base_branch_sync.snapshot(project_id, rev);
    let tracked = rev == default_branch || state.is_some();
    let tip = state.as_ref().and_then(|state| state.remote_tip.clone());
    let stamp = cairn_vcs::commit_stamp(store, commit_id, tip.as_deref())
        .await
        .ok();
    let freshness = tracked.then(|| Freshness {
        state,
        served_contains_tip: stamp.as_ref().and_then(|stamp| stamp.contains),
    });
    format_revision_label(
        rev,
        commit_id,
        stamp.map(|stamp| stamp.committed_at),
        Utc::now(),
        freshness.as_ref(),
    )
}

/// The origin half of a revision label's inputs.
#[derive(Debug, Clone, Default)]
pub(crate) struct Freshness {
    pub state: Option<BranchSync>,
    /// Whether the commit the read actually served contains origin's last
    /// observed tip. Judged on the served commit, not on anything the store
    /// learned afterwards: a sync landing between resolution and labelling
    /// must not make the old tree read as current.
    pub served_contains_tip: Option<bool>,
}

pub(crate) fn format_revision_label(
    rev: &str,
    commit_id: &str,
    committed_at: Option<i64>,
    now: DateTime<Utc>,
    freshness: Option<&Freshness>,
) -> String {
    let coordinate = format!("{rev}@{}", short(commit_id));
    let committed = committed_at
        .map(|at| format!(" committed {} ago", age(now.timestamp() - at)))
        .unwrap_or_default();
    let Some(freshness) = freshness else {
        return format!("{coordinate}{committed}");
    };
    let state = freshness.state.clone().unwrap_or_default();
    if state.no_origin {
        return format!("{coordinate}{committed}, no origin remote");
    }
    let since = |at: DateTime<Utc>| (now - at).num_seconds();
    if let Some(tip) = state
        .remote_tip
        .as_deref()
        .filter(|_| freshness.served_contains_tip != Some(true))
    {
        return format!(
            "{coordinate} STALE: origin {rev} is {} ({})",
            short(tip),
            stale_reason(&state, now)
        );
    }
    let checked = match state.checked_at {
        Some(at) => format!(", origin checked {} ago", age(since(at))),
        None => ", origin not checked since launch".to_string(),
    };
    let failing = state
        .last_error
        .as_ref()
        .filter(|error| state.checked_at.is_none_or(|at| error.at > at))
        .map(|error| {
            format!(
                " (last check failed {} ago: {})",
                age(since(error.at)),
                excerpt(&error.message)
            )
        })
        .unwrap_or_default();
    format!("{coordinate}{committed}{checked}{failing}")
}

/// Why a store known to be behind origin has not caught up yet.
fn stale_reason(state: &BranchSync, now: DateTime<Utc>) -> String {
    let since = |at: DateTime<Utc>| (now - at).num_seconds();
    if let Some(started) = state.sync_started_at {
        format!("sync in progress {}", elapsed(since(started)))
    } else if let Some(error) = &state.last_error {
        let step = match error.phase {
            Phase::Check => "origin check",
            Phase::Sync => "sync",
        };
        format!(
            "{step} failed {} ago: {}",
            age(since(error.at)),
            excerpt(&error.message)
        )
    } else {
        "sync pending".to_string()
    }
}

fn short(commit_id: &str) -> &str {
    commit_id.get(..8).unwrap_or(commit_id)
}

/// A coarse age: `42s`, `12m`, `3h`, `2d`. Clock skew never renders negative.
fn age(seconds: i64) -> String {
    let seconds = seconds.max(0);
    match seconds {
        0..=59 => format!("{seconds}s"),
        60..=3_599 => format!("{}m", seconds / 60),
        3_600..=86_399 => format!("{}h", seconds / 3_600),
        _ => format!("{}d", seconds / 86_400),
    }
}

/// A running duration, precise while it is short: `40s`, `1m40s`, then coarse.
fn elapsed(seconds: i64) -> String {
    let seconds = seconds.max(0);
    if (60..3_600).contains(&seconds) && seconds % 60 != 0 {
        format!("{}m{}s", seconds / 60, seconds % 60)
    } else {
        age(seconds)
    }
}

fn excerpt(message: &str) -> String {
    let line = message
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or_default();
    if line.chars().count() > ERROR_EXCERPT_CHARS {
        let cut: String = line.chars().take(ERROR_EXCERPT_CHARS).collect();
        format!("{cut}…")
    } else {
        line.to_string()
    }
}

#[cfg(test)]
#[path = "base_branch_sync_tests.rs"]
mod tests;
