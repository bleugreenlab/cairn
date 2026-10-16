//! One owner for "where does origin's default branch point, and when did we
//! last know".
//!
//! The shared jj store's copy of a project's default branch is what
//! `read ?branch=main` and `run branch:main` resolve against, and nothing reads
//! origin at read time. So the store is only as current as its last import, and
//! every producer of that import feeds this module:
//!
//! - the GitHub `push` webhook (via the relay) observes a new tip;
//! - Cairn's own GitHub merge path observes the merge commit before fetching;
//! - a polling floor asks origin with `git ls-remote` every
//!   [`DEFAULT_BRANCH_POLL_INTERVAL`], so the bound holds without the relay;
//! - startup catch-up and every origin fetch record what they learned.
//!
//! An observed tip the store does not hold schedules [`sync`], the single entry
//! point to [`base_advance::reconcile_default_advance`], which runs at most once
//! at a time per project. Every origin fetch goes through
//! [`fetch_origin_serialized`], so Cairn's own fetchers can no longer race on
//! `refs/remotes/origin/<default>` and lose.
//!
//! The state is in memory: it describes this process's knowledge of origin,
//! which a restart legitimately resets to "not checked since launch".
//!
//! [`base_advance::reconcile_default_advance`]:
//!     crate::orchestrator::base_advance::reconcile_default_advance

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::{Mutex as TokioMutex, Notify};

use crate::orchestrator::Orchestrator;

/// The polling floor. With the push relay healthy a default-branch advance
/// arrives within its 15 s cadence; without it, this is the bound.
pub(crate) const DEFAULT_BRANCH_POLL_INTERVAL: Duration = Duration::from_secs(60);

/// How long one project's origin check may take before the poll records it as
/// failed. Shorter than the poll interval, so a hung origin is reported by the
/// next tick instead of after the subprocess's own ten-minute network timeout.
const POLL_CHECK_TIMEOUT: Duration = Duration::from_secs(45);

/// A sync that keeps finding origin ahead after a successful pass stops here
/// and leaves the rest to the next observation, rather than spinning.
const MAX_PASSES_PER_SYNC: usize = 3;

/// Longest error excerpt a read header carries.
const ERROR_EXCERPT_CHARS: usize = 120;

/// Who told us where origin's default branch points.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Source {
    Startup,
    Merge,
    Push,
    Poll,
    Fetch,
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

/// What this process knows about one project's default branch on origin.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DefaultBranchSync {
    /// Last successful `ls-remote` or fetch against origin.
    pub checked_at: Option<DateTime<Utc>>,
    /// Origin's default-branch commit as last observed, by any source.
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
struct ProjectEntry {
    state: DefaultBranchSync,
    in_flight: bool,
}

/// Per-process registry behind [`Orchestrator::default_branch_sync`].
#[derive(Default)]
pub(crate) struct DefaultBranchSyncRegistry {
    projects: Mutex<HashMap<String, ProjectEntry>>,
    /// Keyed by jj store directory: one store per repository, and every fetch
    /// writes that repository's `refs/remotes/origin/*`.
    fetch_locks: Mutex<HashMap<PathBuf, Arc<TokioMutex<()>>>>,
    polls_in_flight: Mutex<std::collections::HashSet<String>>,
    idle: Notify,
}

impl DefaultBranchSyncRegistry {
    pub(crate) fn snapshot(&self, project_id: &str) -> Option<DefaultBranchSync> {
        self.projects
            .lock()
            .unwrap()
            .get(project_id)
            .map(|entry| entry.state.clone())
    }

    fn with_state<T>(&self, project_id: &str, f: impl FnOnce(&mut DefaultBranchSync) -> T) -> T {
        let mut projects = self.projects.lock().unwrap();
        f(&mut projects.entry(project_id.to_string()).or_default().state)
    }

    /// Record a tip learned without asking origin ourselves (webhook, merge).
    pub(crate) fn record_tip(&self, project_id: &str, tip: &str, source: Source) {
        self.with_state(project_id, |state| {
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
        tip: Option<String>,
        source: Source,
        at: DateTime<Utc>,
    ) {
        self.with_state(project_id, |state| {
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
                log::info!("default-branch check for project {project_id} recovered");
                state.last_error = None;
            }
        });
    }

    pub(crate) fn record_no_origin(&self, project_id: &str) {
        self.with_state(project_id, |state| {
            *state = DefaultBranchSync {
                no_origin: true,
                ..DefaultBranchSync::default()
            };
        });
    }

    /// Record a failure, warning only when it is new: a poll that fails the
    /// same way every minute should not flood the log.
    pub(crate) fn record_error(
        &self,
        project_id: &str,
        phase: Phase,
        message: &str,
        at: DateTime<Utc>,
    ) {
        self.with_state(project_id, |state| {
            let repeat = state
                .last_error
                .as_ref()
                .is_some_and(|error| error.phase == phase && error.message == message);
            if repeat {
                log::debug!(
                    "default-branch {phase:?} for project {project_id} still failing: {message}"
                );
            } else {
                log::warn!("default-branch {phase:?} for project {project_id} failed: {message}");
            }
            state.last_error = Some(SyncError {
                at,
                phase,
                message: message.to_string(),
            });
        });
    }

    fn record_sync_success(&self, project_id: &str) {
        self.with_state(project_id, |state| {
            if state.last_error.take().is_some() {
                log::info!("default-branch sync for project {project_id} recovered");
            }
        });
    }

    fn try_begin(&self, project_id: &str) -> bool {
        let mut projects = self.projects.lock().unwrap();
        let entry = projects.entry(project_id.to_string()).or_default();
        if entry.in_flight {
            return false;
        }
        entry.in_flight = true;
        true
    }

    fn in_flight(&self, project_id: &str) -> bool {
        self.projects
            .lock()
            .unwrap()
            .get(project_id)
            .is_some_and(|entry| entry.in_flight)
    }

    fn mark_started(&self, project_id: &str, at: DateTime<Utc>) {
        self.with_state(project_id, |state| state.sync_started_at = Some(at));
    }

    fn end(&self, project_id: &str) {
        {
            let mut projects = self.projects.lock().unwrap();
            if let Some(entry) = projects.get_mut(project_id) {
                entry.in_flight = false;
                entry.state.sync_started_at = None;
            }
        }
        self.idle.notify_waiters();
    }

    async fn wait_idle(&self, project_id: &str) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.in_flight(project_id) {
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
/// failed pass can never leave a project unable to sync again.
struct InFlight<'a> {
    registry: &'a DefaultBranchSyncRegistry,
    project_id: &'a str,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.registry.end(self.project_id);
    }
}

/// The single-flight driver. The caller that begins runs one pass
/// unconditionally, then another only while the pass succeeded and
/// `still_behind` says origin moved again meanwhile. A caller that finds a sync
/// already running waits for it instead of starting a second: the running
/// sync's recheck covers any tip observed while it worked.
async fn drive<P, PF, B, BF>(
    registry: &DefaultBranchSyncRegistry,
    project_id: &str,
    mut pass: P,
    mut still_behind: B,
) where
    P: FnMut() -> PF,
    PF: Future<Output = Result<(), String>>,
    B: FnMut() -> BF,
    BF: Future<Output = bool>,
{
    if !registry.try_begin(project_id) {
        registry.wait_idle(project_id).await;
        return;
    }
    let _in_flight = InFlight {
        registry,
        project_id,
    };
    for _ in 0..MAX_PASSES_PER_SYNC {
        registry.mark_started(project_id, Utc::now());
        match pass().await {
            Ok(()) => registry.record_sync_success(project_id),
            Err(error) => {
                registry.record_error(project_id, Phase::Sync, &error, Utc::now());
                return;
            }
        }
        if !still_behind().await {
            return;
        }
    }
    log::warn!(
        "default-branch sync for project {project_id} still behind origin after \
         {MAX_PASSES_PER_SYNC} passes; the next observation will retry"
    );
}

/// Bring the store's default branch to origin's tip and reconcile everything
/// downstream of it, at most once at a time per project. Returns once a pass
/// that started no earlier than this call has finished.
pub(crate) async fn sync(orch: &Orchestrator, project_id: &str, default_branch: &str) {
    let store = match project_repo_path(orch, project_id).await {
        Some(repo) => crate::jj::project_store_dir(&orch.config_dir, &repo),
        None => {
            log::debug!("default-branch sync: project {project_id} has no local repository");
            return;
        }
    };
    drive(
        &orch.default_branch_sync,
        project_id,
        || {
            crate::orchestrator::base_advance::reconcile_default_advance(
                orch,
                project_id,
                default_branch,
            )
        },
        || store_lags_remote(orch, project_id, &store, default_branch),
    )
    .await;
}

/// Record that origin's default branch is at `tip`, and if the store does not
/// hold it, start a [`sync`] in the background. Cheap for an unchanged tip:
/// one in-process view lookup, no subprocess.
pub(crate) async fn observe_remote_tip(
    orch: &Orchestrator,
    project_id: &str,
    tip: &str,
    source: Source,
) {
    orch.default_branch_sync.record_tip(project_id, tip, source);
    let Some(repo) = project_repo_path(orch, project_id).await else {
        return;
    };
    let Some(default_branch) = project_default_branch(orch, project_id).await else {
        return;
    };
    let store = crate::jj::project_store_dir(&orch.config_dir, &repo);
    if !store_lags_remote(orch, project_id, &store, &default_branch).await {
        return;
    }
    log::info!(
        "origin {default_branch} for project {project_id} is at {} ({source:?}); syncing the store",
        short(tip)
    );
    let orch = orch.clone();
    let project_id = project_id.to_string();
    tokio::spawn(async move { sync(&orch, &project_id, &default_branch).await });
}

/// Whether the store's local default bookmark (the one `?branch=main`
/// resolves) does not yet contain origin's last observed tip. Judging the
/// local bookmark rather than `<default>@origin` matters: a sync can import
/// the remote ref and still fail to move the bookmark, and only the bookmark is
/// what readers get. An unknown tip is never "behind"; an unresolvable
/// bookmark is, so a missing store gets provisioned by the sync.
async fn store_lags_remote(
    orch: &Orchestrator,
    project_id: &str,
    store: &Path,
    default_branch: &str,
) -> bool {
    let Some(tip) = orch
        .default_branch_sync
        .snapshot(project_id)
        .and_then(|state| state.remote_tip)
    else {
        return false;
    };
    let Ok(served) = cairn_vcs::resolve_coordinate(store, default_branch).await else {
        return true;
    };
    cairn_vcs::commit_stamp(store, &served, Some(&tip))
        .await
        .map_or(true, |stamp| stamp.contains != Some(true))
}

/// Which project and branch a serialized fetch should record its result
/// against. Fetches that do not update the default branch pass `None`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FetchRecord<'a> {
    pub project_id: &'a str,
    pub default_branch: &'a str,
    pub source: Source,
}

/// Run `fetch` under the repository's fetch lock. Every writer of the
/// repository's `refs/remotes/origin/*` that Cairn runs goes through this, so
/// two of them can no longer race on one ref and have the loser give up.
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
    let lock = orch.default_branch_sync.fetch_lock(store);
    let _guard = lock.lock().await;
    fetch().await
}

/// `git fetch origin` under the repository's fetch lock, recording what it
/// learned about the default branch when `record` names one.
///
/// A fetch outside Cairn (the operator's own `git fetch`) can still race this
/// one on a ref lock. Git then reports `cannot lock ref '<ref>': is at <X> but
/// expected <Y>`; when `<ref>` now holds `<X>`, the other fetch landed the same
/// update and this one is treated as having succeeded.
pub(crate) async fn fetch_origin_serialized(
    orch: &Orchestrator,
    repo_path: &Path,
    record: Option<FetchRecord<'_>>,
) -> Result<(), String> {
    let store = crate::jj::project_store_dir(&orch.config_dir, repo_path);
    let git = orch.services.git.clone();
    let repo = repo_path.to_path_buf();
    let result = with_fetch_lock(orch, &store, || async {
        tokio::task::spawn_blocking(move || {
            git.fetch_origin(&repo).or_else(|error| {
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
        let registry = &orch.default_branch_sync;
        match &result {
            Ok(()) => {
                let repo = repo_path.to_path_buf();
                let branch = record.default_branch.to_string();
                let tip = tokio::task::spawn_blocking(move || {
                    read_ref(&repo, &format!("refs/remotes/origin/{branch}"))
                })
                .await
                .ok()
                .flatten();
                registry.record_check(record.project_id, tip, record.source, Utc::now());
            }
            Err(error) => registry.record_error(record.project_id, Phase::Check, error, Utc::now()),
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
pub(crate) fn spawn_default_branch_poll(orch: &Orchestrator) {
    let orch = orch.clone();
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(DEFAULT_BRANCH_POLL_INTERVAL);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            let pass = orch.clone();
            let joined = crate::orchestrator::base_advance::supervised_sweep_pass(async move {
                poll_once(&pass).await;
            })
            .await;
            if let Err(error) = joined {
                log::error!(
                    "default-branch poll pass ended abnormally; retrying next cycle: {error}"
                );
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
                log::warn!("default-branch poll: failed to load projects: {error}");
                return;
            }
        };
    for (_, project) in projects {
        if !orch.default_branch_sync.begin_poll(&project.id) {
            continue;
        }
        let orch = orch.clone();
        tokio::spawn(async move {
            let _polling = Polling {
                registry: orch.default_branch_sync.clone(),
                project_id: project.id.clone(),
            };
            poll_project(
                &orch,
                &project.id,
                Path::new(&project.repo_path),
                &project.default_branch,
            )
            .await;
        });
    }
}

/// Clears a project's poll-in-flight mark however its check ends.
struct Polling {
    registry: Arc<DefaultBranchSyncRegistry>,
    project_id: String,
}

impl Drop for Polling {
    fn drop(&mut self) {
        self.registry.end_poll(&self.project_id);
    }
}

pub(crate) async fn poll_project(
    orch: &Orchestrator,
    project_id: &str,
    repo_path: &Path,
    default_branch: &str,
) {
    poll_project_within(
        orch,
        project_id,
        repo_path,
        default_branch,
        POLL_CHECK_TIMEOUT,
    )
    .await
}

async fn poll_project_within(
    orch: &Orchestrator,
    project_id: &str,
    repo_path: &Path,
    default_branch: &str,
    timeout: Duration,
) {
    let git = orch.services.git.clone();
    let repo = repo_path.to_path_buf();
    let branch = default_branch.to_string();
    // The subprocess keeps its own, much longer network timeout; this bound is
    // how long the poll waits before saying the check failed.
    let answer = tokio::time::timeout(
        timeout,
        tokio::task::spawn_blocking(move || {
            let has_origin = git
                .remote_get_url(&repo)
                .ok()
                .is_some_and(|url| !url.trim().is_empty());
            has_origin.then(|| git.ls_remote_branch(&repo, "origin", &branch))
        }),
    )
    .await;
    let registry = &orch.default_branch_sync;
    match answer {
        Ok(Ok(None)) => registry.record_no_origin(project_id),
        Ok(Ok(Some(Ok(tip)))) => {
            registry.record_check(project_id, tip.clone(), Source::Poll, Utc::now());
            if let Some(tip) = tip {
                observe_remote_tip(orch, project_id, &tip, Source::Poll).await;
            }
        }
        Ok(Ok(Some(Err(error)))) => {
            registry.record_error(project_id, Phase::Check, &error, Utc::now())
        }
        Ok(Err(error)) => registry.record_error(
            project_id,
            Phase::Check,
            &format!("ls-remote task failed: {error}"),
            Utc::now(),
        ),
        Err(_) => registry.record_error(
            project_id,
            Phase::Check,
            &format!(
                "git ls-remote origin did not answer within {}s",
                timeout.as_secs()
            ),
            Utc::now(),
        ),
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
/// `agent/foo@1a2b3c4d committed 12m ago`, and for the project's default
/// branch, how current the store's copy of origin is. Computed at read time so
/// the rendered header is a pure function of the segment's metadata.
pub(crate) async fn revision_label(
    orch: &Orchestrator,
    project_id: &str,
    rev: &str,
    default_branch: &str,
    store: &Path,
    commit_id: &str,
) -> String {
    let is_default = rev == default_branch;
    let state = is_default
        .then(|| orch.default_branch_sync.snapshot(project_id))
        .flatten();
    let tip = state.as_ref().and_then(|state| state.remote_tip.clone());
    let stamp = cairn_vcs::commit_stamp(store, commit_id, tip.as_deref())
        .await
        .ok();
    let freshness = is_default.then(|| Freshness {
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

/// The default-branch half of a revision label's inputs.
#[derive(Debug, Clone, Default)]
pub(crate) struct Freshness {
    pub state: Option<DefaultBranchSync>,
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
        let why = if let Some(started) = state.sync_started_at {
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
        };
        return format!("{coordinate} STALE: origin {rev} is {} ({why})", short(tip));
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
#[path = "default_branch_sync_tests.rs"]
mod tests;
