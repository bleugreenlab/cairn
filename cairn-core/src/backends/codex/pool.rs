//! Pooled Codex app-server for ephemeral calls (CAIRN-2549).
//!
//! Ordinary Codex node/task sessions keep one app-server process each. Ephemeral
//! CALLS instead share a long-lived app-server: each call is a lightweight
//! `thread/start` on it, so a deep-research workflow fanning ~40 calls uses ONE
//! process (N threads) instead of N processes. `AppServerClient` is already a
//! multiplexing JSON-RPC transport with one shared notification channel; the
//! piece it lacks — and what this module adds — is a demultiplexer that routes
//! each `params.threadId` notification to the owning call's reader, plus the
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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

use crossbeam_channel::{unbounded, Receiver, Sender};
use serde_json::Value;

use super::app_server::AppServerClient;
use super::auth::{refresh_codex_tokens_for_session, CodexAuthState};
use crate::orchestrator::Orchestrator;

/// Auth-identity fingerprint keying the pool.
///
/// The Codex app-server process carries no OS fence (`AppServerClient::spawn`
/// sets no sandbox) and each call thread passes its own `cwd` in thread/turn
/// params, so worktree/cwd is deliberately NOT part of the key: N scratch-dir
/// fan-out calls under one identity share ONE app-server. Auth, however, is
/// process-global (`account/login/start` runs once per app-server), so distinct
/// identities need distinct pools. Model/reasoning/sandbox are per-thread params
/// and likewise excluded.
pub type PoolKey = String;

/// The owning run and working directory bound to a pooled call's thread.
///
/// The app-server process is spawned in the FIRST call's cwd and shared across N
/// calls whose scratch/worktree dirs differ, so the host must override BOTH
/// `run_id` and `cwd` on a pooled tool call from this binding — otherwise a
/// second call would attribute results to the right run but read, write, and
/// execute (`request.cwd`) in the first call's directory. Auth is the only pool
/// key; per-thread filesystem isolation rides here instead.
#[derive(Clone, Debug)]
pub struct CallBinding {
    pub(crate) run_id: String,
    pub(crate) cwd: String,
}

/// One long-lived app-server hosting N ephemeral call threads.
pub struct PooledAppServer {
    client: Arc<AppServerClient>,
    /// `threadId -> (run_id, cwd)`. Read by the host (`binding_for_thread`) to map
    /// a pooled tool call's `_meta.threadId` back to its owning run AND working
    /// directory, and by teardown to fail every in-flight call if the app-server
    /// dies.
    thread_runs: Arc<Mutex<HashMap<String, CallBinding>>>,
    /// `threadId -> per-call notification sender`. The dispatcher routes each
    /// notification carrying that threadId to the owning call's reader.
    senders: Arc<Mutex<HashMap<String, Sender<Value>>>>,
    /// Shared OAuth state for the pool's single login, used by the dispatcher to
    /// answer pool-scoped `account/chatgptAuthTokens/refresh` requests (which
    /// carry no threadId and so cannot be routed to a per-call reader).
    oauth_state: Option<Arc<Mutex<CodexAuthState>>>,
}

impl PooledAppServer {
    /// The shared transport (per-call readers send approval responses through it).
    pub(super) fn client(&self) -> Arc<AppServerClient> {
        self.client.clone()
    }

    /// Register a call thread: record its `threadId -> (run_id, cwd)` binding
    /// BEFORE `turn/start` (so the first tool call routes AND resolves its cwd
    /// correctly) and hand back the per-call notification receiver the dispatcher
    /// will feed.
    pub(super) fn register_call(
        &self,
        thread_id: &str,
        run_id: &str,
        cwd: &str,
    ) -> Receiver<Value> {
        let (tx, rx) = unbounded();
        self.thread_runs.lock().unwrap().insert(
            thread_id.to_string(),
            CallBinding {
                run_id: run_id.to_string(),
                cwd: cwd.to_string(),
            },
        );
        self.senders
            .lock()
            .unwrap()
            .insert(thread_id.to_string(), tx);
        rx
    }

    /// Drop a call thread's routing entries. Idempotent: the reader calls it on
    /// exit and a crash-teardown may have already removed it.
    fn deregister_call(&self, thread_id: &str) {
        self.thread_runs.lock().unwrap().remove(thread_id);
        self.senders.lock().unwrap().remove(thread_id);
    }

    /// Close EVERY per-call channel by dropping its sender. Used on app-server
    /// death: each per-call reader is blocked on `notifications.iter()`, so
    /// dropping the senders disconnects the receivers, unblocks the readers, and
    /// lets them run their cleanup/deregister path — otherwise a pool crash would
    /// leak one blocked OS thread (plus its captured orchestrator/client state)
    /// per in-flight call.
    fn close_all_calls(&self) {
        if let Ok(mut senders) = self.senders.lock() {
            senders.clear();
        }
    }

    fn thread_of(msg: &Value) -> Option<String> {
        msg.pointer("/params/threadId")
            .and_then(|v| v.as_str())
            .map(str::to_string)
    }

    /// Route one notification to the owning call's reader by `threadId` (demux
    /// #1). A message for a thread that has already finalized is dropped; if it
    /// expected a response, decline it so the app-server never hangs on us.
    fn deliver(&self, msg: Value) {
        let method = msg.get("method").and_then(|v| v.as_str());
        let expects_response = msg.get("id").is_some()
            && msg.get("result").is_none()
            && msg.get("error").is_none()
            && method.is_some();
        let decline = |id: &Value, reason: &str| {
            let _ = self.client.respond_error(id, -32601, reason);
        };
        match Self::thread_of(&msg) {
            Some(thread_id) => {
                let sender = self.senders.lock().unwrap().get(&thread_id).cloned();
                match sender {
                    Some(tx) => {
                        let _ = tx.send(msg);
                    }
                    None => {
                        if expects_response {
                            if let Some(id) = msg.get("id") {
                                decline(id, "pooled call thread no longer active");
                            }
                        }
                        log::debug!(
                            "codex pool: dropping notification for unknown thread {}",
                            thread_id
                        );
                    }
                }
            }
            None => {
                // A pool-scoped notification without a threadId
                // (account/rateLimits/updated, account/login/completed). Nothing
                // per-call to do; a stray expects_response is declined so the
                // server never blocks on us.
                if expects_response {
                    if let Some(id) = msg.get("id") {
                        decline(id, "unsupported pool-scoped request");
                    }
                }
            }
        }
    }

    #[cfg(test)]
    fn close_all_calls_for_test(&self) {
        self.close_all_calls();
    }

    #[cfg(test)]
    fn for_test(client: Arc<AppServerClient>) -> Arc<Self> {
        Arc::new(Self {
            client,
            thread_runs: Arc::new(Mutex::new(HashMap::new())),
            senders: Arc::new(Mutex::new(HashMap::new())),
            oauth_state: None,
        })
    }
}

/// A pooled call's cleanup handle, held by its reader thread. Dropping the
/// call's routing entries on reader exit is idempotent with crash teardown.
pub(crate) struct PooledCall {
    server: Arc<PooledAppServer>,
    thread_id: String,
}

impl PooledCall {
    pub(super) fn new(server: Arc<PooledAppServer>, thread_id: String) -> Self {
        Self { server, thread_id }
    }

    /// Remove this call's `threadId -> run_id` and per-call sender entries.
    pub(super) fn deregister(&self) {
        self.server.deregister_call(&self.thread_id);
    }
}

/// Per-key pool of long-lived app-servers, owned by the `Orchestrator`.
#[derive(Default)]
pub struct CodexAppServerPool {
    pools: Mutex<HashMap<PoolKey, Arc<PooledAppServer>>>,
    /// Per-key init lock so two simultaneous first-callers don't double-spawn or
    /// double-login the same app-server.
    init_locks: Mutex<HashMap<PoolKey, Arc<Mutex<()>>>>,
}

impl CodexAppServerPool {
    #[cfg(test)]
    fn insert_test_server(&self, key: PoolKey, server: Arc<PooledAppServer>) {
        self.pools.lock().unwrap().insert(key, server);
    }

    /// Map a pooled call's `threadId` (from `_meta.threadId`) to its owning run
    /// AND working directory. A `threadId` is a globally-unique Codex uuid, so a
    /// linear scan across the (few) live pools is correct and cheap.
    pub(crate) fn binding_for_thread(&self, thread_id: &str) -> Option<CallBinding> {
        let pools = self.pools.lock().ok()?;
        for server in pools.values() {
            if let Some(binding) = server.thread_runs.lock().ok()?.get(thread_id) {
                return Some(binding.clone());
            }
        }
        None
    }

    /// Get-or-spawn the app-server for `key`. `build` performs the full spawn +
    /// `initialize`/`initialized` + `account/login/start` handshake and returns
    /// the ready client and its shared OAuth state; it runs at most once per key
    /// under the per-key init lock. Starts the notification dispatcher on first
    /// build.
    pub(super) fn ensure(
        self: &Arc<Self>,
        key: &PoolKey,
        orch: &Orchestrator,
        build: impl FnOnce()
            -> Result<(Arc<AppServerClient>, Option<Arc<Mutex<CodexAuthState>>>), String>,
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
        let (client, oauth_state) = build()?;
        let server = Arc::new(PooledAppServer {
            client,
            thread_runs: Arc::new(Mutex::new(HashMap::new())),
            senders: Arc::new(Mutex::new(HashMap::new())),
            oauth_state,
        });
        self.spawn_dispatcher(key.clone(), server.clone(), orch.clone());
        self.pools
            .lock()
            .unwrap()
            .insert(key.clone(), server.clone());
        Ok(server)
    }

    fn remove_pool(&self, key: &PoolKey) {
        self.pools.lock().unwrap().remove(key);
    }

    /// The single dispatcher thread for a pooled app-server. It owns the shared
    /// notification receiver and routes every message by `threadId` to the
    /// owning call's reader (demux #1). Pool-scoped auth-refresh requests (no
    /// threadId) are answered here from the shared login. On channel close (the
    /// app-server died) it fails every in-flight call and drops the pool.
    fn spawn_dispatcher(
        self: &Arc<Self>,
        key: PoolKey,
        server: Arc<PooledAppServer>,
        orch: Orchestrator,
    ) {
        let notifications = server.client.notifications();
        let pool = self.clone();
        thread::spawn(move || {
            for msg in notifications.iter() {
                let method = msg.get("method").and_then(|v| v.as_str());
                let expects_response = msg.get("id").is_some()
                    && msg.get("result").is_none()
                    && msg.get("error").is_none()
                    && method.is_some();

                // Pool-scoped auth-token refresh carries no threadId, so it can't
                // be routed to a per-call reader — answer it here from the shared
                // login, exactly as a dedicated session's reader would.
                if expects_response && method == Some("account/chatgptAuthTokens/refresh") {
                    if let Some(id) = msg.get("id") {
                        answer_pool_token_refresh(&orch, &server, id);
                    }
                    continue;
                }

                let _ = expects_response;
                server.deliver(msg);
            }

            // Channel closed => the app-server process died. Fail EVERY in-flight
            // call through the normal error path so parked workflow awaits
            // resolve (never hang), then drop the pool so the next call spawns a
            // fresh app-server.
            let runs: Vec<String> = server
                .thread_runs
                .lock()
                .map(|m| m.values().map(|b| b.run_id.clone()).collect())
                .unwrap_or_default();
            log::warn!(
                "codex pooled app-server (key {}) closed; failing {} in-flight call(s)",
                key,
                runs.len()
            );
            // Close every per-call channel FIRST so each blocked reader exits and
            // deregisters (no leaked threads), then finalize the runs.
            server.close_all_calls();
            for run_id in runs {
                crate::orchestrator::lifecycle::fail_run(
                    &orch,
                    &run_id,
                    "codex pooled app-server closed",
                );
            }
            pool.remove_pool(&key);
        });
    }
}

/// Answer a pool-scoped `account/chatgptAuthTokens/refresh` request from the
/// pool's shared OAuth state (mirrors the dedicated-session reader path).
fn answer_pool_token_refresh(orch: &Orchestrator, server: &PooledAppServer, id: &Value) {
    let client = server.client.as_ref();
    let response = match server.oauth_state.as_ref() {
        Some(state_arc) => match refresh_codex_tokens_for_session(orch, state_arc) {
            Ok(new_tokens) => match new_tokens.chatgpt_account_id {
                Some(account_id) => client.respond(
                    id,
                    serde_json::json!({
                        "accessToken": new_tokens.access_token,
                        "chatgptAccountId": account_id,
                    }),
                ),
                None => client.respond_error(
                    id,
                    -32000,
                    "Codex token refresh did not provide a ChatGPT account id; run connect_codex_auth",
                ),
            },
            Err(err) => client.respond_error(
                id,
                -32000,
                &format!(
                    "Codex token refresh failed: {}. Please rerun connect_codex_auth.",
                    err
                ),
            ),
        },
        None => client.respond_error(
            id,
            -32000,
            "Codex OAuth tokens unavailable; run connect_codex_auth",
        ),
    };
    if let Err(e) = response {
        log::warn!("codex pool: failed to answer token refresh: {}", e);
    }
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

    // Demux #2: the host maps a pooled call's threadId back to its run AND its
    // own working directory, registered before turn/start and cleared on
    // deregister. Two calls sharing the app-server bind to DIFFERENT cwds, so a
    // scratch-dir fan-out keeps per-run filesystem isolation.
    #[test]
    fn binding_for_thread_maps_run_and_cwd_and_clears_on_deregister() {
        let pool = CodexAppServerPool::default();
        let server = PooledAppServer::for_test(scripted_client());
        pool.insert_test_server("identity-a".to_string(), server.clone());

        let _rx = server.register_call("thread-1", "run-1", "/scratch/one");
        server.register_call("thread-2", "run-2", "/scratch/two");

        let b1 = pool
            .binding_for_thread("thread-1")
            .expect("thread-1 binding");
        assert_eq!(b1.run_id, "run-1");
        assert_eq!(b1.cwd, "/scratch/one");
        let b2 = pool
            .binding_for_thread("thread-2")
            .expect("thread-2 binding");
        assert_eq!(b2.run_id, "run-2");
        assert_eq!(b2.cwd, "/scratch/two");
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
        let rx = server.register_call("thread-x", "run-x", "/scratch/x");
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
        let rx_a = server.register_call("thread-a", "run-a", "/scratch/a");
        let rx_b = server.register_call("thread-b", "run-b", "/scratch/b");

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

    // A notification for a thread that already finalized is dropped, not routed
    // to some other call.
    #[test]
    fn deliver_drops_notifications_for_unknown_threads() {
        let server = PooledAppServer::for_test(scripted_client());
        let rx = server.register_call("thread-live", "run-live", "/scratch/live");

        server.deliver(notification("thread-gone", "turn/completed"));
        assert!(
            rx.try_recv().is_err(),
            "a message for an unknown thread must not reach a live call"
        );
    }
}
