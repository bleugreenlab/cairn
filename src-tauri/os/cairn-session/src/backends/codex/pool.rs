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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crossbeam_channel::Receiver;
use serde_json::Value;

use cairn_models::codex::app_server::AppServerClient;
use cairn_models::codex::thread_router::{Scoped, ThreadRouter};
use cairn_models::wire::codex_app_server::Notification;
/// Application actions for a pool's shared identity and disconnected runs.
pub trait PoolHost: Send + Sync {
    fn refresh_tokens(&self, client: &AppServerClient, request_id: &Value);
    fn disconnected(&self, run_ids: &[String]);
}

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
    /// Routes each `params.threadId` message to the owning call's reader, and
    /// hands pool-scoped ones (the token refresh) to the pool's [`PoolHost`].
    router: Arc<ThreadRouter>,
    /// `threadId -> run_id`. Read by the host (`binding_for_thread`) to map a
    /// pooled tool call's `_meta.threadId` back to its authenticated run.
    thread_runs: Arc<Mutex<HashMap<String, CallBinding>>>,
}

impl PooledAppServer {
    /// The server's router: a thread opened on it is `CodexServer::Shared`
    /// with this, and its reader answers through the router's client.
    pub fn router(&self) -> Arc<ThreadRouter> {
        self.router.clone()
    }

    /// Register a call thread before `turn/start`, so its first tool call routes
    /// to the authenticated run, and return its notification receiver.
    pub fn register_call(&self, thread_id: &str, run_id: &str) -> Receiver<Value> {
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
        Arc::new(Self {
            router: ThreadRouter::new(client, move |client, message| {
                Self::scoped(host.as_ref(), client, message)
            }),
            thread_runs: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

/// A pooled thread's cleanup handle, held by whoever reads it. Its routing
/// entries go when it is dropped, on every exit path; removing them is
/// idempotent with an explicit [`Self::deregister`] and with crash teardown.
pub struct PooledCall {
    server: Arc<PooledAppServer>,
    thread_id: String,
}

impl PooledCall {
    pub fn new(server: Arc<PooledAppServer>, thread_id: String) -> Self {
        Self { server, thread_id }
    }

    /// Remove this call's `threadId -> run_id` and per-call sender entries.
    pub fn deregister(&self) {
        self.server.deregister_call(&self.thread_id);
    }
}

impl Drop for PooledCall {
    fn drop(&mut self) {
        self.deregister();
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
    fn additional_resident_process_units(&self, key: &PoolKey) -> u32 {
        u32::from(!self.pools.lock().unwrap().contains_key(key))
    }

    #[cfg(test)]
    fn insert_test_server(&self, key: PoolKey, server: Arc<PooledAppServer>) {
        self.pools.lock().unwrap().insert(key, server);
    }

    /// Map a pooled call's `threadId` (from `_meta.threadId`) to its owning run. A `threadId` is a globally-unique Codex uuid, so a
    /// linear scan across the (few) live pools is correct and cheap.
    pub fn binding_for_thread(&self, thread_id: &str) -> Option<CallBinding> {
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
    /// the ready client and its identity host; it runs at most once per key
    /// under the per-key init lock. Starts the server's router on first build.
    pub fn ensure(
        self: &Arc<Self>,
        key: &PoolKey,
        build: impl FnOnce() -> Result<(Arc<AppServerClient>, Arc<dyn PoolHost>), String>,
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
        let (client, host) = build()?;
        let thread_runs = Arc::new(Mutex::new(HashMap::new()));
        let closed = self.on_server_closed(key.clone(), thread_runs.clone(), host.clone());
        let router = ThreadRouter::start(
            client,
            move |client, message| PooledAppServer::scoped(host.as_ref(), client, message),
            closed,
        );
        let server = Arc::new(PooledAppServer {
            router,
            thread_runs,
        });
        self.pools
            .lock()
            .unwrap()
            .insert(key.clone(), server.clone());
        Ok(server)
    }

    fn remove_pool(&self, key: &PoolKey) {
        self.pools.lock().unwrap().remove(key);
    }

    /// What the pool does when a server's channel closes (the app-server
    /// died). The router has already closed every per-call channel, so each
    /// blocked reader exits (no leaked threads) while its run binding stays
    /// (see `PooledAppServer::deregister_call`). Every in-flight call is then
    /// failed through the normal error path so parked workflow awaits resolve
    /// (never hang), and the pool entry is dropped so the next call spawns a
    /// fresh app-server.
    fn on_server_closed(
        self: &Arc<Self>,
        key: PoolKey,
        thread_runs: Arc<Mutex<HashMap<String, CallBinding>>>,
        host: Arc<dyn PoolHost>,
    ) -> impl FnOnce() + Send + 'static {
        let pool = self.clone();
        move || {
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
            pool.remove_pool(&key);
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
