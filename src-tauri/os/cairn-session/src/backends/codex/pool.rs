//! Pooled Codex app-server for ephemeral calls (CAIRN-2549).
//!
//! Ordinary Codex node/task sessions keep one app-server process each. Ephemeral
//! CALLS instead share a long-lived app-server: each call is a lightweight
//! `thread/start` on it, so a deep-research workflow fanning ~40 calls uses ONE
//! process (N threads) instead of N processes. Each server's messages are
//! routed to the owning call's reader by the facet's [`ThreadRouter`]; what
//! this module adds is the pool's keys and lifecycle, and the
//! `threadId -> run_id` map the host uses to attribute each pooled call's MCP
//! tool results and `cairn:~/` targets to the right run.
//!
//! Isolation invariants (see CAIRN-2549):
//! - A per-call `RunHandle` carries a NULL child, so kill/stop/finalize never
//!   signal the shared process; only `turn/interrupt` aborts one call's turn.
//! - The pool lives here, NOT in `process_state.processes`, so warm-process GC
//!   never evicts it.
//! - When the shared app-server dies the dispatcher fails EVERY in-flight call
//!   through the normal error path (fail-closed, so parked workflow awaits
//!   resolve) and drops the pool entry so the next call respawns a fresh one.
//! - A key names the Codex release its server runs ([`versioned_key`]). When
//!   the managed Codex swaps releases,
//!   [`CodexAppServerPool::retire_versions_except`] takes older servers out of
//!   lookup; each is shut down once its last call deregisters, so in-flight
//!   calls finish on the release they started on.
//! - Each server has an id of its own, distinct from its key, so a credential
//!   issued to one server's `cairn-cmd` speaks for that server's threads alone
//!   ([`CodexAppServerPool::binding_for_server_thread`]) and dies with it
//!   ([`PoolHost::server_closed`]), even when a successor takes its key.
//! - That credential outlives every run it serves, so a run's bindings end when
//!   the run does ([`CodexAppServerPool::release_run`]), whatever its reader is
//!   still doing.
//! - A thread is let go by [`PooledCall::release`]: an active goal is paused,
//!   its live turn is interrupted, its binding ends, and it is unsubscribed,
//!   in that order, because unsubscribing leaves a running turn running and
//!   an active goal starting new ones. Codex unloads
//!   it only after its unload delay (30 minutes upstream), so the server
//!   remembers when each thread was released ([`PoolSnapshot`]).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::Receiver;
use serde::Serialize;
use serde_json::Value;

use cairn_models::codex::app_server::AppServerClient;
use cairn_models::codex::thread_router::{Scoped, ThreadRouter};
use cairn_models::session::codex::thread::{pause_active_goal, unsubscribe_thread_within};
use cairn_models::wire::codex_app_server::errors::is_no_active_turn_error;
use cairn_models::wire::codex_app_server::Notification;

/// How long a released thread is reported as awaiting unload. Codex unloads
/// an unsubscribed idle thread after 30 minutes; past this a thread still
/// listed is overdue, which only a liveness check can say.
const AWAITING_UNLOAD_HORIZON: Duration = Duration::from_secs(35 * 60);

/// How long the release worker waits for each answer. A server that has
/// stopped answering keeps its pipe open, so without this one stuck thread
/// would hold every later release on its server behind the full request
/// timeout.
const RELEASE_STEP: Duration = Duration::from_secs(3);

/// Upper bounds, in milliseconds, of the `thread/start` latency buckets; the
/// last bucket counts everything slower.
pub const THREAD_START_BUCKETS_MS: [u64; 7] = [5, 25, 100, 500, 2_000, 10_000, 60_000];
/// Application actions for a pool's shared identity and disconnected runs.
pub trait PoolHost: Send + Sync {
    fn refresh_tokens(&self, client: &AppServerClient, request_id: &Value);
    fn disconnected(&self, run_ids: &[String]);
    /// What a call whose turn fails is told about a token request this pool
    /// refused on its shared login, while that refusal stands.
    fn standing_token_refusal(&self) -> Option<String>;
    /// The server with this id has closed: whatever was issued to it for its
    /// lifetime alone ends here.
    fn server_closed(&self, server_id: &str);
}

/// Auth-identity fingerprint keying the pool.
///
/// The Codex app-server process carries no OS fence (`AppServerClient::spawn`
/// sets no sandbox) and each call thread passes its own `cwd` in thread/turn
/// params, so worktree/cwd is deliberately NOT part of the key: N scratch-dir
/// fan-out calls under one identity share ONE app-server. Auth, however, is
/// process-global (`account/login/start` runs once per app-server), so distinct
/// identities need distinct pools. Model/reasoning/sandbox are per-thread params
/// and likewise excluded. The Codex release is included (see [`versioned_key`]).
pub type PoolKey = String;

const VERSION_SEPARATOR: &str = "@codex-";

/// The key for `identity`'s server on Codex `version`. `None` (nothing was
/// installed yet when the key was made) is a key every retirement retires.
pub fn versioned_key(identity: &str, version: Option<&str>) -> PoolKey {
    match version {
        Some(version) => format!("{identity}{VERSION_SEPARATOR}{version}"),
        None => identity.to_string(),
    }
}

fn key_version(key: &str) -> Option<&str> {
    key.rsplit_once(VERSION_SEPARATOR)
        .map(|(_, version)| version)
}

/// Distinguishes every pooled server this process starts, across keys and
/// across successive servers on one key.
static SERVER_SERIAL: AtomicU64 = AtomicU64::new(0);

fn next_server_id() -> String {
    format!(
        "codex-pool-{}",
        SERVER_SERIAL.fetch_add(1, Ordering::Relaxed)
    )
}

/// The authenticated run bound to a pooled call's thread.
///
/// Process cwd is deliberately absent: repository identity and logical file
/// resolution come from the run. Each Codex thread still receives its own cwd in
/// its thread parameters, but that OS residence is not callback authority.
#[derive(Clone, Debug)]
pub struct CallBinding {
    pub run_id: String,
}

/// One long-lived app-server hosting N ephemeral call threads.
pub struct PooledAppServer {
    /// This server's own id, never reused within the process.
    id: String,
    /// Routes each `params.threadId` message to the owning call's reader, and
    /// hands pool-scoped ones (the token refresh) to the pool's [`PoolHost`].
    router: Arc<ThreadRouter>,
    /// `threadId -> run_id`. Read by the host (`binding_for_thread`) to map a
    /// pooled tool call's `_meta.threadId` back to its authenticated run.
    thread_runs: Arc<Mutex<HashMap<String, CallBinding>>>,
    /// Out of lookup: shut down when its last call deregisters.
    retiring: AtomicBool,
    /// The pool's shared identity, which answers the server's token requests.
    host: Arc<dyn PoolHost>,
    /// Its key's digest ([`key_digest`]): telemetry never sees the key.
    key_digest: String,
    created_at: Instant,
    stats: Arc<Mutex<ServerStats>>,
    /// The queue of this server's release worker, started by its first
    /// release. The worker alone writes a release's requests, so a server
    /// that stops reading its stdin blocks that one thread, never a stop.
    releaser: Mutex<Option<crossbeam_channel::Sender<ReleaseJob>>>,
}

/// One unit of work for a server's release worker.
enum ReleaseJob {
    Release {
        client: Arc<AppServerClient>,
        thread_id: String,
        live_turn: Option<String>,
        step: Duration,
        state: Arc<Mutex<ReleaseState>>,
        stats: Arc<Mutex<ServerStats>>,
    },
    /// Answered once every job queued before it has run.
    Settle(crossbeam_channel::Sender<()>),
}

/// Pause an active goal, interrupt the live turn and unsubscribe, waiting at
/// most `step` for each answer, then record whether Codex confirmed it.
fn run_release_job(job: ReleaseJob) {
    let ReleaseJob::Release {
        client,
        thread_id,
        live_turn,
        step,
        state,
        stats,
    } = job
    else {
        if let ReleaseJob::Settle(done) = job {
            let _ = done.send(());
        }
        return;
    };
    if let Err(error) = pause_active_goal(&client, &thread_id, step) {
        log::warn!(
            "codex pool: pausing thread {thread_id}'s goal before its release failed: {error}"
        );
    }
    if let Some(turn_id) = live_turn {
        let interrupted = client.send_request_within(
            "turn/interrupt",
            serde_json::json!({ "threadId": thread_id, "turnId": turn_id }),
            Instant::now() + step,
        );
        if let Err(error) = interrupted.as_ref() {
            if !is_no_active_turn_error(error) {
                log::warn!("codex pool: interrupting thread {thread_id} before its release failed: {error}");
            }
        }
    }
    let unsubscribed = unsubscribe_thread_within(&client, &thread_id, Instant::now() + step);
    let now = Instant::now();
    let outcome = {
        let mut stats = stats.lock().unwrap_or_else(|e| e.into_inner());
        stats.last_activity = Some(now);
        match unsubscribed {
            Ok(status) => {
                log::debug!("codex pool: released thread {thread_id} ({status:?})");
                stats.uncertain.remove(&thread_id);
                stats.released.insert(thread_id, now);
                ReleaseState::Released
            }
            Err(error) => {
                log::warn!("codex pool: unsubscribing thread {thread_id} failed: {error}");
                stats.uncertain.insert(thread_id, now);
                ReleaseState::Uncertain
            }
        }
    };
    *state.lock().unwrap_or_else(|e| e.into_inner()) = outcome;
}

#[derive(Default)]
struct ServerStats {
    last_activity: Option<Instant>,
    /// Threads released here, with when, until Codex should have unloaded them.
    released: HashMap<String, Instant>,
    /// Threads whose unsubscribe went unanswered, with when: they may still
    /// be subscribed, and their next release retries.
    uncertain: HashMap<String, Instant>,
    /// `thread/start` latencies, bucketed by [`THREAD_START_BUCKETS_MS`].
    thread_starts: [u64; THREAD_START_BUCKETS_MS.len() + 1],
}

impl PooledAppServer {
    fn new(
        id: String,
        key: &str,
        router: Arc<ThreadRouter>,
        thread_runs: Arc<Mutex<HashMap<String, CallBinding>>>,
        host: Arc<dyn PoolHost>,
    ) -> Self {
        Self {
            id,
            router,
            thread_runs,
            retiring: AtomicBool::new(false),
            host,
            key_digest: key_digest(key),
            created_at: Instant::now(),
            stats: Arc::new(Mutex::new(ServerStats::default())),
            releaser: Mutex::new(None),
        }
    }

    /// Hand `job` to this server's release worker, starting it if need be.
    /// The worker holds no reference to the server, so it ends with it.
    fn queue_release(&self, job: ReleaseJob) {
        let mut releaser = self.releaser.lock().unwrap_or_else(|e| e.into_inner());
        if releaser.is_none() {
            let (sender, jobs) = crossbeam_channel::unbounded::<ReleaseJob>();
            let started = std::thread::Builder::new()
                .name(format!("{}-release", self.id))
                .spawn(move || {
                    for job in jobs {
                        run_release_job(job);
                    }
                });
            match started {
                Ok(_) => *releaser = Some(sender),
                Err(error) => {
                    log::error!(
                        "codex pool: could not start {}'s release worker: {error}",
                        self.id
                    );
                    return;
                }
            }
        }
        if let Some(sender) = releaser.as_ref() {
            let _ = sender.send(job);
        }
    }

    /// Wait at most `within` for every release queued here so far to have
    /// been sent and answered or given up. Answers whether they all were.
    pub fn settle_releases(&self, within: Duration) -> bool {
        let (done, settled) = crossbeam_channel::bounded(1);
        {
            let releaser = self.releaser.lock().unwrap_or_else(|e| e.into_inner());
            match releaser.as_ref() {
                Some(sender) => {
                    if sender.send(ReleaseJob::Settle(done)).is_err() {
                        return false;
                    }
                }
                None => return true,
            }
        }
        settled.recv_timeout(within).is_ok()
    }

    fn stats(&self) -> std::sync::MutexGuard<'_, ServerStats> {
        self.stats.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn touch(&self) {
        self.stats().last_activity = Some(Instant::now());
    }

    /// Count one `thread/start` that took `elapsed` on this server.
    pub fn record_thread_start(&self, elapsed: Duration) {
        let ms = elapsed.as_millis() as u64;
        let bucket = THREAD_START_BUCKETS_MS
            .iter()
            .position(|bound| ms <= *bound)
            .unwrap_or(THREAD_START_BUCKETS_MS.len());
        let mut stats = self.stats();
        stats.thread_starts[bucket] += 1;
        stats.last_activity = Some(Instant::now());
    }

    /// The app-server's pid, while its child is attached.
    fn pid(&self) -> Option<u32> {
        let child = self.router.client().child_handle();
        let guard = child.lock().ok()?;
        guard.as_ref().map(|child| child.id())
    }

    /// What this server looks like now.
    fn snapshot(&self, retiring: bool) -> PooledServerSnapshot {
        let now = Instant::now();
        let mut stats = self.stats();
        stats
            .released
            .retain(|_, at| now.duration_since(*at) < AWAITING_UNLOAD_HORIZON);
        let mut awaiting_unload: Vec<ReleasedThread> = stats
            .released
            .iter()
            .map(|(thread_id, at)| ReleasedThread {
                thread_id: thread_id.clone(),
                released_secs_ago: now.duration_since(*at).as_secs(),
            })
            .collect();
        awaiting_unload.sort_by_key(|thread| thread.released_secs_ago);
        let mut release_uncertain: Vec<String> = stats.uncertain.keys().cloned().collect();
        release_uncertain.sort();
        PooledServerSnapshot {
            server_id: self.id.clone(),
            key_digest: self.key_digest.clone(),
            pid: self.pid(),
            age_secs: now.duration_since(self.created_at).as_secs(),
            idle_secs: stats
                .last_activity
                .map(|at| now.duration_since(at).as_secs()),
            live_threads: self.router.routed_count(),
            awaiting_unload,
            release_uncertain,
            thread_start_buckets_ms: THREAD_START_BUCKETS_MS.to_vec(),
            thread_start_counts: stats.thread_starts.to_vec(),
            retiring,
            closed: self.is_closed(),
        }
    }

    /// This server's id: what its `cairn-cmd`'s credential is scoped to.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The server's router: a thread opened on it is `CodexServer::Shared`
    /// with this, and its reader answers through the router's client.
    pub fn router(&self) -> Arc<ThreadRouter> {
        self.router.clone()
    }

    /// Register a call thread before `turn/start`, so its first tool call routes
    /// to the authenticated run, and return its notification receiver.
    pub fn register_call(&self, thread_id: &str, run_id: &str) -> Receiver<Value> {
        self.touch();
        self.thread_runs.lock().unwrap().insert(
            thread_id.to_string(),
            CallBinding {
                run_id: run_id.to_string(),
            },
        );
        self.router.route(thread_id)
    }

    /// Register a one-shot completion's thread before `turn/start` and return
    /// its notification receiver.
    ///
    /// Unlike [`Self::register_call`] there is no run to bind: a completion
    /// has no tools, so a tool call carrying its `threadId` finds no binding
    /// and fails closed, and a pool death reaches it only by closing this
    /// channel, never as a run [`PoolHost::disconnected`] must fail.
    pub fn register_completion(&self, thread_id: &str) -> Receiver<Value> {
        self.touch();
        self.router.route(thread_id)
    }

    /// Drop a call thread's routing entries. Idempotent: the reader calls it on
    /// exit and a crash-teardown may have already removed it.
    ///
    /// The run binding goes only with a live route. When the server dies the
    /// router closes every route first, and each reader then unwinds through
    /// here; its binding must survive that, so the death callback can still
    /// find and fail its run.
    fn deregister_call(&self, thread_id: &str) {
        if self.router.deregister(thread_id) {
            self.thread_runs.lock().unwrap().remove(thread_id);
        }
        self.close_if_drained();
    }

    /// Shut the process down if it is retiring and no call is left on it.
    /// The router decides "no call left" under the same lock a registration
    /// takes, so a call that races the retirement gets a closed channel and
    /// fails as it would on a server death, never a channel nobody serves.
    fn close_if_drained(&self) {
        if self.retiring.load(Ordering::SeqCst) && self.router.close_if_idle() {
            self.router.client().shutdown();
        }
    }

    pub fn is_closed(&self) -> bool {
        self.router.is_closed()
    }

    /// A pool-scoped message (one with no threadId): the token refresh is
    /// answered from the pool's shared login, exactly as a dedicated session's
    /// reader would; anything else is left to the router.
    fn scoped(host: &dyn PoolHost, client: &AppServerClient, message: &Value) -> Scoped {
        match Notification::parse(message) {
            Notification::AuthTokensRefreshRequest { id } => {
                host.refresh_tokens(client, id);
                Scoped::Answered
            }
            _ => Scoped::Passed,
        }
    }

    #[cfg(test)]
    fn deliver(&self, msg: Value) {
        self.router.deliver(msg);
    }

    #[cfg(test)]
    fn close_all_calls_for_test(&self) {
        self.router.close();
    }

    #[cfg(test)]
    fn for_test(client: Arc<AppServerClient>) -> Arc<Self> {
        let host: Arc<dyn PoolHost> = Arc::new(TestPoolHost);
        let scoped_host = host.clone();
        Arc::new(Self::new(
            next_server_id(),
            "test-identity",
            ThreadRouter::new(client, move |client, message| {
                Self::scoped(scoped_host.as_ref(), client, message)
            }),
            Arc::new(Mutex::new(HashMap::new())),
            host,
        ))
    }
}

/// A pooled thread's handle, held by whoever reads it and by its run's
/// process handle. The thread is released ([`Self::release`]) when its reader
/// closes, when its run is torn down, or when the last holder drops it,
/// whichever comes first. Once released or queued, later releases do
/// nothing; a release whose unsubscribe went unanswered is retried by the
/// next.
pub struct PooledCall {
    server: Arc<PooledAppServer>,
    thread_id: String,
    /// The thread's live provider turn, as its reader tracks it.
    current_turn: Arc<Mutex<Option<String>>>,
    state: Arc<Mutex<ReleaseState>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReleaseState {
    Held,
    /// Unbound and unrouted; its requests wait on the release worker.
    Queued,
    Released,
    /// Unbound, but Codex never confirmed the unsubscribe.
    Uncertain,
}

impl PooledCall {
    pub fn new(server: Arc<PooledAppServer>, thread_id: String) -> Self {
        Self {
            server,
            thread_id,
            current_turn: Arc::new(Mutex::new(None)),
            state: Arc::new(Mutex::new(ReleaseState::Held)),
        }
    }

    /// Track the thread's live turn through `current_turn`, the cell its
    /// reader keeps, so a release interrupts a turn still running.
    pub fn tracking_turn(mut self, current_turn: Arc<Mutex<Option<String>>>) -> Self {
        self.current_turn = current_turn;
        self
    }

    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    /// Let the thread go on its server. Here, at once: end its run binding,
    /// so a stray tool call from it is refused, and drop its route. Then, on
    /// the server's release worker: pause an active goal, interrupt its live
    /// turn, and unsubscribe it, in that order, because unsubscribing stops
    /// neither a running turn nor an active goal's next one.
    ///
    /// Nothing here writes to the server, so a stop that calls this returns
    /// promptly even when the server has stopped reading its stdin; only the
    /// worker waits, each answer for at most [`RELEASE_STEP`]. An unanswered
    /// unsubscribe leaves the thread uncertain
    /// ([`PooledServerSnapshot::release_uncertain`]) and the next release
    /// retries it.
    ///
    /// On a server that has died nothing is sent and the binding stays, so
    /// the death callback can still find and fail the run it belongs to.
    pub fn release(&self) {
        self.release_within(RELEASE_STEP);
    }

    fn release_within(&self, step: Duration) {
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if matches!(*state, ReleaseState::Released | ReleaseState::Queued) {
                return;
            }
            *state = ReleaseState::Queued;
        }
        let server = &self.server;
        if server.is_closed() {
            *self.state.lock().unwrap_or_else(|e| e.into_inner()) = ReleaseState::Released;
            server.deregister_call(&self.thread_id);
            return;
        }
        server
            .thread_runs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.thread_id);
        server.queue_release(ReleaseJob::Release {
            client: server.router.client(),
            thread_id: self.thread_id.clone(),
            live_turn: self.current_turn.lock().ok().and_then(|turn| turn.clone()),
            step,
            state: self.state.clone(),
            stats: server.stats.clone(),
        });
        server.deregister_call(&self.thread_id);
    }

    /// The pool's standing token-request refusal ([`PoolHost::standing_token_refusal`]):
    /// a call's own reader never sees the token request, which the pool answers.
    pub fn standing_token_refusal(&self) -> Option<String> {
        self.server.host.standing_token_refusal()
    }
}

impl Drop for PooledCall {
    fn drop(&mut self) {
        self.release();
    }
}

/// One released thread, not yet past [`AWAITING_UNLOAD_HORIZON`].
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReleasedThread {
    pub thread_id: String,
    pub released_secs_ago: u64,
}

/// One pooled app-server as telemetry sees it. Carries no account id, email
/// or credential: its key appears only as a digest.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PooledServerSnapshot {
    pub server_id: String,
    /// The first 12 hex digits of the SHA-256 of the pool key.
    pub key_digest: String,
    pub pid: Option<u32>,
    pub age_secs: u64,
    /// Since a thread last opened, started or was released here.
    pub idle_secs: Option<u64>,
    /// Threads routed on the server now.
    pub live_threads: usize,
    /// Released threads Codex should still be holding loaded.
    pub awaiting_unload: Vec<ReleasedThread>,
    /// Released threads whose unsubscribe went unanswered: they may still be
    /// subscribed until a later release confirms it.
    pub release_uncertain: Vec<String>,
    /// Upper bounds of the `thread/start` latency buckets, in milliseconds.
    pub thread_start_buckets_ms: Vec<u64>,
    /// Counts per bucket; one more entry than the bounds, for slower starts.
    pub thread_start_counts: Vec<u64>,
    pub retiring: bool,
    pub closed: bool,
}

/// Every pooled app-server this runner holds.
#[derive(Clone, Debug, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PoolSnapshot {
    pub servers: Vec<PooledServerSnapshot>,
}

fn key_digest(key: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(key.as_bytes());
    digest
        .iter()
        .take(6)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Per-key pool of long-lived app-servers, owned by the `Orchestrator`.
#[derive(Default)]
pub struct CodexAppServerPool {
    pools: Mutex<HashMap<PoolKey, Arc<PooledAppServer>>>,
    /// Per-key init lock so two simultaneous first-callers don't double-spawn or
    /// double-login the same app-server.
    init_locks: Mutex<HashMap<PoolKey, Arc<Mutex<()>>>>,
    /// Out of lookup, still serving the calls that started on them.
    retiring: Mutex<Vec<Arc<PooledAppServer>>>,
}

impl CodexAppServerPool {
    #[cfg(test)]
    fn additional_resident_process_units(&self, key: &PoolKey) -> u32 {
        u32::from(!self.pools.lock().unwrap().contains_key(key))
    }

    #[cfg(test)]
    fn insert_test_server(&self, key: PoolKey, server: Arc<PooledAppServer>) {
        self.pools.lock().unwrap().insert(key, server);
    }

    /// Every server this pool holds, in lookup and retiring.
    pub fn snapshot(&self) -> PoolSnapshot {
        let mut servers: Vec<PooledServerSnapshot> = {
            let pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
            pools
                .values()
                .map(|server| server.snapshot(false))
                .collect()
        };
        let retiring = self.retiring.lock().unwrap_or_else(|e| e.into_inner());
        servers.extend(retiring.iter().map(|server| server.snapshot(true)));
        servers.sort_by(|a, b| a.server_id.cmp(&b.server_id));
        PoolSnapshot { servers }
    }

    /// Map a pooled call's `threadId` (from `_meta.threadId`) to its owning run. A `threadId` is a globally-unique Codex uuid, so a
    /// linear scan across the (few) live pools is correct and cheap.
    pub fn binding_for_thread(&self, thread_id: &str) -> Option<CallBinding> {
        self.find_binding(|_| true, thread_id)
    }

    /// [`Self::binding_for_thread`], answered only from the server with id
    /// `server_id`: a thread on any other server, or on a server that has
    /// closed, has no binding here.
    pub fn binding_for_server_thread(
        &self,
        server_id: &str,
        thread_id: &str,
    ) -> Option<CallBinding> {
        self.find_binding(
            |server| server.id == server_id && !server.is_closed(),
            thread_id,
        )
    }

    fn find_binding(
        &self,
        on: impl Fn(&PooledAppServer) -> bool,
        thread_id: &str,
    ) -> Option<CallBinding> {
        let pools = self.pools.lock().ok()?;
        let retiring = self.retiring.lock().ok()?;
        for server in pools.values().chain(retiring.iter()) {
            if !on(server) {
                continue;
            }
            if let Some(binding) = server.thread_runs.lock().ok()?.get(thread_id) {
                return Some(binding.clone());
            }
        }
        None
    }

    /// End every binding `run_id` holds, on any server, retiring ones included.
    /// Idempotent.
    ///
    /// Called when the run finalizes. Only the bindings go: the threads' routes
    /// stay for their readers to wind down, and a server death after this has
    /// nothing left to fail for a run that has already ended.
    pub fn release_run(&self, run_id: &str) {
        let pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        let retiring = self.retiring.lock().unwrap_or_else(|e| e.into_inner());
        for server in pools.values().chain(retiring.iter()) {
            server
                .thread_runs
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .retain(|_, binding| binding.run_id != run_id);
        }
    }

    /// Take every server not on Codex `current` out of lookup, so the next
    /// call starts one on `current`. Each retired server is shut down now if
    /// it is idle, else when its last call deregisters.
    pub fn retire_versions_except(&self, current: &str) {
        let retired: Vec<Arc<PooledAppServer>> = {
            let mut pools = self.pools.lock().unwrap();
            let stale: Vec<PoolKey> = pools
                .keys()
                .filter(|key| key_version(key) != Some(current))
                .cloned()
                .collect();
            stale.iter().filter_map(|key| pools.remove(key)).collect()
        };
        for server in retired {
            log::info!("codex pool: retiring a server not on Codex {current}");
            server.retiring.store(true, Ordering::SeqCst);
            server.close_if_drained();
            if !server.is_closed() {
                self.retiring.lock().unwrap().push(server);
            }
        }
        self.retiring
            .lock()
            .unwrap()
            .retain(|server| !server.is_closed());
    }

    /// Get-or-spawn the app-server for `key`. `build` performs the full spawn +
    /// `initialize`/`initialized` + `account/login/start` handshake and returns
    /// the ready client and its identity host; it runs at most once per key
    /// under the per-key init lock, and is given the id the server will have,
    /// so anything it issues the process can be scoped to that one server.
    /// Starts the server's router on first build.
    pub fn ensure(
        self: &Arc<Self>,
        key: &PoolKey,
        build: impl FnOnce(&str) -> Result<(Arc<AppServerClient>, Arc<dyn PoolHost>), String>,
    ) -> Result<Arc<PooledAppServer>, String> {
        if let Some(existing) = self.pools.lock().unwrap().get(key).cloned() {
            return Ok(existing);
        }
        // Serialize first-caller construction for this key.
        let init_lock = self
            .init_locks
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = init_lock.lock().unwrap();
        // Another caller may have finished building while we waited.
        if let Some(existing) = self.pools.lock().unwrap().get(key).cloned() {
            return Ok(existing);
        }
        let id = next_server_id();
        let (client, host) = build(&id)?;
        let thread_runs = Arc::new(Mutex::new(HashMap::new()));
        let closed =
            self.on_server_closed(key.clone(), id.clone(), thread_runs.clone(), host.clone());
        let scoped_host = host.clone();
        let router = ThreadRouter::start(
            client,
            move |client, message| PooledAppServer::scoped(scoped_host.as_ref(), client, message),
            closed,
        );
        let server = Arc::new(PooledAppServer::new(id, key, router, thread_runs, host));
        self.pools
            .lock()
            .unwrap()
            .insert(key.clone(), server.clone());
        Ok(server)
    }

    /// Forget the server whose run map is `thread_runs`, whether it is still
    /// in lookup under `key` or retiring. Matched by identity, so a retired
    /// server's death never removes a newer server that took its key.
    fn remove_pool(&self, key: &PoolKey, thread_runs: &Arc<Mutex<HashMap<String, CallBinding>>>) {
        let ours = |server: &Arc<PooledAppServer>| Arc::ptr_eq(&server.thread_runs, thread_runs);
        let mut pools = self.pools.lock().unwrap();
        if pools.get(key).is_some_and(ours) {
            pools.remove(key);
        }
        drop(pools);
        self.retiring.lock().unwrap().retain(|server| !ours(server));
    }

    /// What the pool does when a server's channel closes (the app-server
    /// died, or a retired server was shut down). The router has already
    /// closed every per-call channel, so each blocked reader exits (no leaked
    /// threads) while its run binding stays (see
    /// `PooledAppServer::deregister_call`). Every in-flight call is then
    /// failed through the normal error path so parked workflow awaits resolve
    /// (never hang), and the pool entry is dropped so the next call spawns a
    /// fresh app-server.
    fn on_server_closed(
        self: &Arc<Self>,
        key: PoolKey,
        server_id: String,
        thread_runs: Arc<Mutex<HashMap<String, CallBinding>>>,
        host: Arc<dyn PoolHost>,
    ) -> impl FnOnce() + Send + 'static {
        let pool = self.clone();
        move || {
            host.server_closed(&server_id);
            let runs: Vec<String> = thread_runs
                .lock()
                .map(|m| m.values().map(|b| b.run_id.clone()).collect())
                .unwrap_or_default();
            log::warn!(
                "codex pooled app-server (key {}) closed; failing {} in-flight call(s)",
                key,
                runs.len()
            );
            host.disconnected(&runs);
            pool.remove_pool(&key, &thread_runs);
        }
    }
}

#[cfg(test)]
struct TestPoolHost;
#[cfg(test)]
impl PoolHost for TestPoolHost {
    fn refresh_tokens(&self, _client: &AppServerClient, _id: &Value) {
        panic!("unexpected refresh");
    }
    fn disconnected(&self, _runs: &[String]) {}
    fn standing_token_refusal(&self) -> Option<String> {
        None
    }
    fn server_closed(&self, _server_id: &str) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scripted_client() -> Arc<AppServerClient> {
        Arc::new(AppServerClient::for_test_scripted(
            Vec::new(),
            Arc::new(Mutex::new(Vec::new())),
        ))
    }

    fn notification(thread_id: &str, method: &str) -> Value {
        serde_json::json!({ "method": method, "params": { "threadId": thread_id } })
    }

    // Demux #2: the host maps a pooled call's threadId back to its authenticated
    // run, registered before turn/start and cleared on deregister.
    #[test]
    fn binding_for_thread_maps_run_and_clears_on_deregister() {
        let pool = CodexAppServerPool::default();
        assert_eq!(
            pool.additional_resident_process_units(&"identity-a".to_string()),
            1
        );
        let server = PooledAppServer::for_test(scripted_client());
        pool.insert_test_server("identity-a".to_string(), server.clone());
        assert_eq!(
            pool.additional_resident_process_units(&"identity-a".to_string()),
            0,
            "reusing an existing app-server must not charge another process"
        );

        let _rx = server.register_call("thread-1", "run-1");
        server.register_call("thread-2", "run-2");

        let b1 = pool
            .binding_for_thread("thread-1")
            .expect("thread-1 binding");
        assert_eq!(b1.run_id, "run-1");
        let b2 = pool
            .binding_for_thread("thread-2")
            .expect("thread-2 binding");
        assert_eq!(b2.run_id, "run-2");
        assert!(pool.binding_for_thread("thread-unknown").is_none());

        server.deregister_call("thread-1");
        assert!(pool.binding_for_thread("thread-1").is_none());
        assert_eq!(
            pool.binding_for_thread("thread-2").map(|b| b.run_id),
            Some("run-2".to_string())
        );
    }

    // Pool crash teardown: closing all per-call channels disconnects each blocked
    // reader's receiver so the reader exits (and deregisters) instead of leaking
    // a thread. Proves the receiver observes disconnection.
    #[test]
    fn close_all_calls_disconnects_per_call_receivers() {
        let server = PooledAppServer::for_test(scripted_client());
        let rx = server.register_call("thread-x", "run-x");
        assert!(rx.try_recv().is_err(), "no messages yet");

        server.close_all_calls_for_test();

        // The sender was dropped, so the receiver is disconnected: a blocking recv
        // returns immediately with an error rather than parking forever (which is
        // what the real per-call reader's `for msg in rx.iter()` relies on to
        // exit).
        assert!(
            matches!(rx.recv(), Err(crossbeam_channel::RecvError)),
            "receiver must be disconnected after close_all_calls"
        );
        assert_eq!(rx.iter().count(), 0, "iter() ends immediately");
    }

    // Demux #1: a notification carrying threadId lands only on the owning call's
    // per-call reader channel; the other call's channel stays empty.
    #[test]
    fn deliver_routes_notifications_by_thread_id() {
        let server = PooledAppServer::for_test(scripted_client());
        let rx_a = server.register_call("thread-a", "run-a");
        let rx_b = server.register_call("thread-b", "run-b");

        server.deliver(notification("thread-a", "item/agentMessage/delta"));
        server.deliver(notification("thread-b", "turn/completed"));
        server.deliver(notification("thread-a", "turn/completed"));

        let a1 = rx_a.try_recv().expect("first message for thread-a");
        assert_eq!(
            a1.pointer("/params/threadId").and_then(Value::as_str),
            Some("thread-a")
        );
        assert_eq!(
            a1.get("method").and_then(Value::as_str),
            Some("item/agentMessage/delta")
        );
        assert_eq!(
            rx_a.try_recv()
                .expect("second message for thread-a")
                .get("method")
                .and_then(Value::as_str),
            Some("turn/completed")
        );
        assert!(rx_a.try_recv().is_err(), "thread-a channel drained");

        assert_eq!(
            rx_b.try_recv()
                .expect("message for thread-b")
                .get("method")
                .and_then(Value::as_str),
            Some("turn/completed")
        );
        assert!(rx_b.try_recv().is_err(), "thread-b channel drained");
    }

    // A one-shot completion's thread routes its own notifications but binds no
    // run: a stray tool call on it resolves to nothing, a pool death closes its
    // channel, and dropping its guard removes it.
    #[test]
    fn a_completion_routes_without_a_run_binding_and_cleans_up_on_drop() {
        let pool = CodexAppServerPool::default();
        let server = PooledAppServer::for_test(scripted_client());
        pool.insert_test_server("identity-a".to_string(), server.clone());

        let rx = server.register_completion("thread-c");
        assert!(pool.binding_for_thread("thread-c").is_none());
        server.deliver(notification("thread-c", "turn/completed"));
        assert!(rx.try_recv().is_ok(), "its notifications still route");

        server.close_all_calls_for_test();
        assert!(matches!(rx.recv(), Err(crossbeam_channel::RecvError)));

        let rx = server.register_completion("thread-d");
        drop(PooledCall::new(server.clone(), "thread-d".to_string()));
        assert!(
            matches!(rx.recv(), Err(crossbeam_channel::RecvError)),
            "dropping the guard deregisters the thread"
        );
        assert!(!server.router.is_routed("thread-d"));
    }

    // A server death closes every route before the pool fails its runs. A
    // reader that unwinds through its guard in between must not erase its run
    // binding, or that run would never be failed; a call that ends on a live
    // route still clears its binding.
    #[test]
    fn a_route_closed_by_the_server_s_death_keeps_its_run_binding() {
        let pool = CodexAppServerPool::default();
        let server = PooledAppServer::for_test(scripted_client());
        pool.insert_test_server("identity-a".to_string(), server.clone());
        let _done = server.register_call("thread-done", "run-done");
        let _live = server.register_call("thread-live", "run-live");

        drop(PooledCall::new(server.clone(), "thread-done".to_string()));
        assert!(pool.binding_for_thread("thread-done").is_none());

        server.close_all_calls_for_test();
        drop(PooledCall::new(server.clone(), "thread-live".to_string()));
        assert_eq!(
            pool.binding_for_thread("thread-live").map(|b| b.run_id),
            Some("run-live".to_string())
        );
    }

    // A server-scoped lookup answers only for that server's own threads: a
    // credential issued to one pooled server cannot reach a call on another,
    // and nothing answers for a server once it has closed.
    #[test]
    fn a_server_scoped_lookup_sees_only_that_server_s_threads() {
        let pool = CodexAppServerPool::default();
        let a = PooledAppServer::for_test(scripted_client());
        let b = PooledAppServer::for_test(scripted_client());
        assert_ne!(a.id(), b.id());
        pool.insert_test_server("identity-a".to_string(), a.clone());
        pool.insert_test_server("identity-b".to_string(), b.clone());
        let _on_a = a.register_call("thread-a", "run-a");
        let _on_b = b.register_call("thread-b", "run-b");

        assert_eq!(
            pool.binding_for_server_thread(a.id(), "thread-a")
                .map(|binding| binding.run_id),
            Some("run-a".to_string())
        );
        assert!(pool.binding_for_server_thread(a.id(), "thread-b").is_none());
        assert!(pool
            .binding_for_server_thread("codex-pool-unknown", "thread-a")
            .is_none());

        a.close_all_calls_for_test();
        assert!(
            pool.binding_for_server_thread(a.id(), "thread-a").is_none(),
            "a closed server's credential speaks for nothing"
        );
    }

    // A pooled server's credential outlives every run it serves, so a finalized
    // run's threads stop resolving on every server, retiring ones included, while
    // its neighbours and its threads' routes stay.
    #[test]
    fn releasing_a_run_ends_its_bindings_and_nothing_else() {
        let pool = CodexAppServerPool::default();
        let live = PooledAppServer::for_test(scripted_client());
        let retiring = PooledAppServer::for_test(scripted_client());
        pool.insert_test_server("identity-a".to_string(), live.clone());
        pool.retiring.lock().unwrap().push(retiring.clone());
        let _a = live.register_call("thread-a", "run-ended");
        let _b = live.register_call("thread-b", "run-other");
        let _c = retiring.register_call("thread-c", "run-ended");

        pool.release_run("run-ended");

        for thread in ["thread-a", "thread-c"] {
            assert!(pool.binding_for_thread(thread).is_none(), "{thread}");
        }
        assert!(pool
            .binding_for_server_thread(live.id(), "thread-a")
            .is_none());
        assert_eq!(
            pool.binding_for_thread("thread-b").map(|b| b.run_id),
            Some("run-other".to_string())
        );
        assert!(live.router.is_routed("thread-a"), "its reader winds down");

        pool.release_run("run-ended");
    }

    #[test]
    fn a_key_names_the_codex_release_its_server_runs() {
        assert_eq!(
            versioned_key("codex-oauth:acct", Some("0.153.3")),
            "codex-oauth:acct@codex-0.153.3"
        );
        assert_eq!(
            key_version("codex-oauth:acct@codex-0.153.3"),
            Some("0.153.3")
        );
        assert_eq!(versioned_key("codex-oauth:acct", None), "codex-oauth:acct");
        assert_eq!(key_version("codex-oauth:acct"), None);
    }

    // After a swap, a drained old server is shut down at once, a busy one
    // keeps serving (and attributing) its calls until the last deregisters,
    // and the server on the current release is left alone.
    #[test]
    fn retiring_closes_old_servers_once_drained_and_keeps_the_current_one() {
        let pool = CodexAppServerPool::default();
        let idle_old = PooledAppServer::for_test(scripted_client());
        let busy_old = PooledAppServer::for_test(scripted_client());
        let current = PooledAppServer::for_test(scripted_client());
        pool.insert_test_server(versioned_key("a", Some("1.0.0")), idle_old.clone());
        pool.insert_test_server(versioned_key("b", None), busy_old.clone());
        pool.insert_test_server(versioned_key("a", Some("2.0.0")), current.clone());
        let _rx = busy_old.register_call("thread-old", "run-old");

        pool.retire_versions_except("2.0.0");

        assert!(idle_old.is_closed());
        assert!(!busy_old.is_closed());
        assert!(!current.is_closed());
        let keys: Vec<PoolKey> = pool.pools.lock().unwrap().keys().cloned().collect();
        assert_eq!(keys, vec![versioned_key("a", Some("2.0.0"))]);
        assert_eq!(
            pool.binding_for_thread("thread-old").map(|b| b.run_id),
            Some("run-old".to_string()),
            "a retiring server still attributes its calls"
        );

        drop(PooledCall::new(busy_old.clone(), "thread-old".to_string()));
        assert!(busy_old.is_closed());
        let late = busy_old.register_completion("thread-late");
        assert!(
            matches!(late.recv(), Err(crossbeam_channel::RecvError)),
            "a call that reaches a closed server fails closed"
        );
        assert!(!current.is_closed());
    }

    // Releasing a thread interrupts its live turn, ends its binding, and only
    // then unsubscribes it, because unsubscribing leaves a running turn
    // running; a second release sends nothing. The snapshot then reports the
    // thread as awaiting unload and names its key only by digest.
    #[test]
    fn release_interrupts_unbinds_then_unsubscribes_once() {
        let thread_runs: Arc<Mutex<HashMap<String, CallBinding>>> = Arc::default();
        let bound_at_unsubscribe = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = {
            let thread_runs = thread_runs.clone();
            let bound = bound_at_unsubscribe.clone();
            Arc::new(AppServerClient::for_test_responding(
                move |request| match request["method"].as_str() {
                    Some("thread/goal/get") => {
                        let thread = request["params"]["threadId"].as_str().unwrap();
                        let goal = (thread == "thread-busy").then(|| {
                            serde_json::json!({
                                "threadId": thread, "objective": "keep going", "status": "active",
                                "tokenBudget": null, "tokensUsed": 0, "timeUsedSeconds": 0,
                                "createdAt": 0, "updatedAt": 0
                            })
                        });
                        Ok(serde_json::json!({ "result": { "goal": goal } }))
                    }
                    Some("thread/goal/set") => Ok(serde_json::json!({ "result": {} })),
                    Some("turn/interrupt") => Ok(serde_json::json!({ "result": {} })),
                    Some("thread/unsubscribe") => {
                        let thread = request["params"]["threadId"].as_str().unwrap();
                        bound
                            .lock()
                            .unwrap()
                            .push(thread_runs.lock().unwrap().contains_key(thread));
                        Ok(serde_json::json!({ "result": { "status": "unsubscribed" } }))
                    }
                    other => Err(format!("unexpected request {other:?}")),
                },
                requests.clone(),
            ))
        };
        let host: Arc<dyn PoolHost> = Arc::new(TestPoolHost);
        let server = Arc::new(PooledAppServer::new(
            next_server_id(),
            "codex-oauth:secret-account@codex-1.0.0",
            ThreadRouter::new(client, |_, _| Scoped::Passed),
            thread_runs,
            host,
        ));
        let pool = CodexAppServerPool::default();
        pool.insert_test_server("codex-oauth:secret-account".to_string(), server.clone());
        let _busy = server.register_call("thread-busy", "run-busy");
        let _idle = server.register_call("thread-idle", "run-idle");

        let busy = PooledCall::new(server.clone(), "thread-busy".to_string())
            .tracking_turn(Arc::new(Mutex::new(Some("turn-1".to_string()))));
        busy.release();
        busy.release();
        PooledCall::new(server.clone(), "thread-idle".to_string()).release();
        assert!(server.settle_releases(Duration::from_secs(5)));

        let sent: Vec<(String, Value)> = requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| {
                (
                    request["method"].as_str().unwrap_or_default().to_string(),
                    request["params"].clone(),
                )
            })
            .collect();
        assert_eq!(
            sent,
            vec![
                (
                    "thread/goal/get".to_string(),
                    serde_json::json!({ "threadId": "thread-busy" })
                ),
                (
                    "thread/goal/set".to_string(),
                    serde_json::json!({ "threadId": "thread-busy", "status": "paused" })
                ),
                (
                    "turn/interrupt".to_string(),
                    serde_json::json!({ "threadId": "thread-busy", "turnId": "turn-1" })
                ),
                (
                    "thread/unsubscribe".to_string(),
                    serde_json::json!({ "threadId": "thread-busy" })
                ),
                (
                    "thread/goal/get".to_string(),
                    serde_json::json!({ "threadId": "thread-idle" })
                ),
                (
                    "thread/unsubscribe".to_string(),
                    serde_json::json!({ "threadId": "thread-idle" })
                ),
            ],
            "only an active goal is paused and a live turn interrupted, each before the unsubscribe, and each thread is released once"
        );
        assert_eq!(
            *bound_at_unsubscribe.lock().unwrap(),
            vec![false, false],
            "the binding ends before the unsubscribe"
        );
        assert!(pool.binding_for_thread("thread-busy").is_none());
        assert!(!server.router.is_routed("thread-busy"));

        let snapshot = pool.snapshot();
        let only = &snapshot.servers[0];
        assert_eq!(only.live_threads, 0);
        let mut awaiting: Vec<&str> = only
            .awaiting_unload
            .iter()
            .map(|thread| thread.thread_id.as_str())
            .collect();
        awaiting.sort();
        assert_eq!(awaiting, vec!["thread-busy", "thread-idle"]);
        assert_eq!(only.key_digest.len(), 12);
        let rendered = serde_json::to_string(&snapshot).unwrap();
        assert!(!rendered.contains("secret-account"), "{rendered}");
    }

    // A server that stops answering keeps its pipe open, so every request of
    // a release would otherwise wait out the full request timeout. Each is
    // bounded instead: the binding still ends, the thread is reported
    // uncertain, and the next release retries rather than doing nothing.
    #[test]
    fn a_silent_server_bounds_the_release_and_leaves_it_retryable() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server =
            PooledAppServer::for_test(Arc::new(AppServerClient::for_test_silent(requests.clone())));
        let pool = CodexAppServerPool::default();
        pool.insert_test_server("identity-a".to_string(), server.clone());
        let _rx = server.register_call("thread-stuck", "run-stuck");
        let call = PooledCall::new(server.clone(), "thread-stuck".to_string())
            .tracking_turn(Arc::new(Mutex::new(Some("turn-1".to_string()))));

        let started = Instant::now();
        call.release_within(Duration::from_millis(100));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the release took {:?}",
            started.elapsed()
        );
        assert!(pool.binding_for_thread("thread-stuck").is_none());
        assert!(!server.router.is_routed("thread-stuck"));
        assert!(server.settle_releases(Duration::from_secs(5)));
        let snapshot = server.snapshot(false);
        assert_eq!(snapshot.release_uncertain, vec!["thread-stuck".to_string()]);
        assert!(snapshot.awaiting_unload.is_empty());

        let sent = requests.lock().unwrap().len();
        call.release_within(Duration::from_millis(50));
        assert!(server.settle_releases(Duration::from_secs(5)));
        assert!(
            requests.lock().unwrap().len() > sent,
            "an unconfirmed release is retried"
        );
    }

    // A server that stops reading its stdin blocks every write to it. A
    // release writes nothing itself, so it still returns at once with the
    // binding and route gone; the server's one release worker waits on the
    // pipe instead, the later release queued behind it, and both are
    // reported unconfirmed once the pipe gives way.
    #[test]
    fn a_server_that_stops_reading_cannot_hold_up_a_release() {
        let (unblock, gate) = crossbeam_channel::bounded::<()>(0);
        let server = PooledAppServer::for_test(Arc::new(AppServerClient::for_test_unread(gate)));
        let pool = CodexAppServerPool::default();
        pool.insert_test_server("identity-a".to_string(), server.clone());
        let _a = server.register_call("thread-a", "run-a");
        let _b = server.register_call("thread-b", "run-b");

        let started = Instant::now();
        let calls: Vec<PooledCall> = ["thread-a", "thread-b"]
            .into_iter()
            .map(|thread| {
                let call = PooledCall::new(server.clone(), thread.to_string())
                    .tracking_turn(Arc::new(Mutex::new(Some("turn-1".to_string()))));
                call.release();
                call
            })
            .collect();
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "releasing on a server that stopped reading took {:?}",
            started.elapsed()
        );
        for thread in ["thread-a", "thread-b"] {
            assert!(pool.binding_for_thread(thread).is_none(), "{thread}");
            assert!(!server.router.is_routed(thread), "{thread}");
        }
        assert!(
            !server.settle_releases(Duration::from_millis(200)),
            "the worker is still blocked on the unread pipe"
        );

        drop(unblock);
        assert!(server.settle_releases(Duration::from_secs(5)));
        assert_eq!(
            server.snapshot(false).release_uncertain,
            vec!["thread-a".to_string(), "thread-b".to_string()]
        );
        drop(calls);
    }

    // The last release on a retiring server shuts it down on the caller's
    // thread. With an earlier release's worker blocked in a write the server
    // never reads, that shutdown must still return: it cannot wait on the
    // writer the blocked worker holds.
    #[test]
    fn the_last_release_on_a_retiring_server_returns_while_a_write_is_blocked() {
        let (unblock, gate) = crossbeam_channel::bounded::<()>(0);
        let server = PooledAppServer::for_test(Arc::new(AppServerClient::for_test_unread(gate)));
        let _a = server.register_call("thread-a", "run-a");
        let _b = server.register_call("thread-b", "run-b");
        server.retiring.store(true, Ordering::SeqCst);

        let a = PooledCall::new(server.clone(), "thread-a".to_string());
        a.release();
        assert!(
            !server.settle_releases(Duration::from_millis(200)),
            "A's release worker is blocked in its write"
        );
        assert!(!server.is_closed(), "B's route still holds the server open");

        let b = PooledCall::new(server.clone(), "thread-b".to_string());
        let started = Instant::now();
        b.release();
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the last release took {:?}",
            started.elapsed()
        );
        assert!(
            server.is_closed(),
            "the drained retiring server is shut down"
        );

        drop(unblock);
        assert!(server.settle_releases(Duration::from_secs(5)));
        drop((a, b));
    }

    #[test]
    fn thread_start_latencies_fall_into_their_buckets() {
        let server = PooledAppServer::for_test(scripted_client());
        server.record_thread_start(Duration::from_millis(3));
        server.record_thread_start(Duration::from_millis(40));
        server.record_thread_start(Duration::from_secs(54));
        server.record_thread_start(Duration::from_secs(90));
        let snapshot = server.snapshot(false);
        assert_eq!(snapshot.thread_start_counts, vec![1, 0, 1, 0, 0, 0, 1, 1]);
        assert_eq!(
            snapshot.thread_start_counts.len(),
            snapshot.thread_start_buckets_ms.len() + 1
        );
    }

    // A notification for a thread that already finalized is dropped, not routed
    // to some other call.
    #[test]
    fn deliver_drops_notifications_for_unknown_threads() {
        let server = PooledAppServer::for_test(scripted_client());
        let rx = server.register_call("thread-live", "run-live");

        server.deliver(notification("thread-gone", "turn/completed"));
        assert!(
            rx.try_recv().is_err(),
            "a message for an unknown thread must not reach a live call"
        );
    }
}
