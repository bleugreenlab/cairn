//! One shared Codex app-server, many threads: routing each thread's messages
//! to the reader that owns it.
//!
//! [`AppServerClient::notifications`] is a single multi-consumer channel, so
//! each message reaches exactly one reader. A host running several threads on
//! one app-server at once therefore needs a demultiplexer: one dispatcher owns
//! the server's channel and hands every message carrying a `params.threadId`
//! to that thread's own channel. Messages without a `threadId` (account rate
//! limits, login completion, the `account/chatgptAuthTokens/refresh` request)
//! are the server's owner's to handle; the router passes them to the owner's
//! scoped handler and declines any request it leaves unanswered, so the server
//! never waits on a reply that will not come.
//!
//! A server with one owner (a session's own app-server) is read by an owned
//! router ([`ThreadRouter::start_owned`]) instead: no thread is routed, and
//! every message the scoped handler leaves goes, in order, to the owner's one
//! channel rather than being dropped or declined. Its dispatcher runs from
//! before the session's first thread opens, so a scoped request the server
//! makes while `thread/start` is pending is answered as it arrives. The token
//! refresh is the one that matters: the app-server gives up on it after ten
//! seconds, and a session's own reader only starts once its thread is open.
//!
//! When the server's channel closes (the process died) the router closes every
//! route, so each reader blocked on its channel unwinds, and then tells the
//! owner once.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

use crossbeam_channel::{unbounded, Receiver, Sender};
use serde_json::Value;

use crate::codex::app_server::AppServerClient;

/// The JSON-RPC error code a request the router cannot place is declined with.
const DECLINED: i32 = -32601;

/// What the owner's scoped handler did with a message that has no `threadId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scoped {
    /// The handler answered it (or, for a notification, consumed it).
    Answered,
    /// The handler left it; a request is declined by the router.
    Passed,
}

type ScopedHandler = Box<dyn Fn(&AppServerClient, &Value) -> Scoped + Send + Sync>;

#[derive(Default)]
struct Routes {
    senders: HashMap<String, Sender<Value>>,
    /// An owned router's owner: every message no route claims and the scoped
    /// handler passed. `None` on a router that drops and declines them.
    owner: Option<Sender<Value>>,
    /// Set once the server's channel has closed. A thread registered after
    /// that gets a channel that is already closed, so its reader never waits
    /// on a server that is gone.
    closed: bool,
}

/// Routes one app-server's messages to its threads by `params.threadId`.
pub struct ThreadRouter {
    client: Arc<AppServerClient>,
    routes: Mutex<Routes>,
    scoped: ScopedHandler,
}

impl ThreadRouter {
    /// Take `client`'s notification channel and route it on a dispatcher
    /// thread. `scoped` sees every message without a `threadId` and says
    /// whether it answered it; `closed` runs once when the server's channel
    /// closes, after every route has been closed.
    pub fn start(
        client: Arc<AppServerClient>,
        scoped: impl Fn(&AppServerClient, &Value) -> Scoped + Send + Sync + 'static,
        closed: impl FnOnce() + Send + 'static,
    ) -> Arc<Self> {
        let router = Self::new(client, scoped);
        router.dispatch(closed);
        router
    }

    /// Take `client`'s notification channel for its one owner, and route it
    /// on a dispatcher thread: `scoped` sees every message without a
    /// `threadId` as it arrives, and every message it passes (and every one
    /// with a `threadId`) reaches the returned channel, in order. A request
    /// passed this way is the owner's to answer, never declined. The channel
    /// closes when the server's does.
    pub fn start_owned(
        client: Arc<AppServerClient>,
        scoped: impl Fn(&AppServerClient, &Value) -> Scoped + Send + Sync + 'static,
    ) -> (Arc<Self>, Receiver<Value>) {
        let (owner, events) = unbounded();
        let router = Self::new(client, scoped);
        router.routes.lock().unwrap().owner = Some(owner);
        router.dispatch(|| {});
        (router, events)
    }

    fn dispatch(self: &Arc<Self>, closed: impl FnOnce() + Send + 'static) {
        let notifications = self.client.notifications();
        let dispatcher = self.clone();
        thread::spawn(move || {
            for message in notifications.iter() {
                dispatcher.deliver(message);
            }
            dispatcher.close();
            closed();
        });
    }

    /// A router with no dispatcher: its owner reads the server's channel and
    /// hands each message to [`Self::deliver`].
    pub fn new(
        client: Arc<AppServerClient>,
        scoped: impl Fn(&AppServerClient, &Value) -> Scoped + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            client,
            routes: Mutex::new(Routes::default()),
            scoped: Box::new(scoped),
        })
    }

    /// The shared transport: a thread's reader answers its server requests
    /// through it.
    pub fn client(&self) -> Arc<AppServerClient> {
        self.client.clone()
    }

    /// Route `thread_id`'s notifications to a channel of its own. Register
    /// before `turn/start`; dropping the route deregisters it.
    pub fn register(self: &Arc<Self>, thread_id: &str) -> (Receiver<Value>, ThreadRoute) {
        let receiver = self.route(thread_id);
        let route = ThreadRoute {
            router: self.clone(),
            thread_id: thread_id.to_string(),
        };
        (receiver, route)
    }

    /// Route `thread_id` without a guard; the caller owes a
    /// [`Self::deregister`]. Prefer [`Self::register`].
    pub fn route(&self, thread_id: &str) -> Receiver<Value> {
        let (sender, receiver) = unbounded();
        let mut routes = self.routes.lock().unwrap();
        if !routes.closed {
            routes.senders.insert(thread_id.to_string(), sender);
        }
        receiver
    }

    /// Stop routing `thread_id`, closing its channel. Says whether it was
    /// still routed: `false` once the thread was deregistered, or once the
    /// server's death closed every route.
    pub fn deregister(&self, thread_id: &str) -> bool {
        self.routes
            .lock()
            .unwrap()
            .senders
            .remove(thread_id)
            .is_some()
    }

    /// Whether `thread_id` is routed now.
    pub fn is_routed(&self, thread_id: &str) -> bool {
        self.routes.lock().unwrap().senders.contains_key(thread_id)
    }

    /// Whether the server's channel has closed (or [`Self::close`] ran).
    pub fn is_closed(&self) -> bool {
        self.routes.lock().unwrap().closed
    }

    /// Close the router if no thread is routed, and say whether this call
    /// closed it. Decided under the lock [`Self::route`] takes, so a thread
    /// registering at the same moment either keeps the router open or gets an
    /// already-closed channel; the caller shuts the server down on `true`.
    pub fn close_if_idle(&self) -> bool {
        let mut routes = self.routes.lock().unwrap();
        if routes.closed || !routes.senders.is_empty() || routes.owner.is_some() {
            return false;
        }
        routes.closed = true;
        true
    }

    /// Close every route, as the server's death does: each reader's channel
    /// disconnects, so a reader blocked on it unwinds instead of leaking.
    pub fn close(&self) {
        let mut routes = self.routes.lock().unwrap();
        routes.closed = true;
        routes.senders.clear();
        routes.owner = None;
    }

    /// One message, routed: to its thread's channel, or (for a finalized
    /// thread) dropped and, if it is a request, declined. A message without
    /// a `threadId` goes to the owner's scoped handler. On an owned router,
    /// what would be dropped goes to the owner instead.
    pub fn deliver(&self, message: Value) {
        let expects_response = message.get("id").is_some()
            && message.get("result").is_none()
            && message.get("error").is_none()
            && message.get("method").and_then(Value::as_str).is_some();
        let Some(thread_id) = message
            .pointer("/params/threadId")
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            if (self.scoped)(&self.client, &message) == Scoped::Passed {
                if let Some(message) = self.to_owner(message) {
                    if expects_response {
                        self.decline(&message, "unsupported pool-scoped request");
                    }
                }
            }
            return;
        };
        let sender = self.routes.lock().unwrap().senders.get(&thread_id).cloned();
        match sender {
            Some(sender) => {
                let _ = sender.send(message);
            }
            None => {
                let Some(message) = self.to_owner(message) else {
                    return;
                };
                if expects_response {
                    self.decline(&message, "pooled call thread no longer active");
                }
                log::debug!("codex router: dropping a message for unknown thread {thread_id}");
            }
        }
    }

    /// Hand `message` to the owner, if this router has one; give it back if
    /// it has none.
    fn to_owner(&self, message: Value) -> Option<Value> {
        let owner = self.routes.lock().unwrap().owner.clone();
        match owner {
            Some(owner) => {
                let _ = owner.send(message);
                None
            }
            None => Some(message),
        }
    }

    fn decline(&self, request: &Value, reason: &str) {
        if let Some(id) = request.get("id") {
            if let Err(error) = self.client.respond_error(id, DECLINED, reason) {
                log::debug!("codex router: could not decline a request: {error}");
            }
        }
    }
}

/// A thread's route, held by whoever reads it; dropping it deregisters the
/// thread on every exit path.
pub struct ThreadRoute {
    router: Arc<ThreadRouter>,
    thread_id: String,
}

impl ThreadRoute {
    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }
}

impl Drop for ThreadRoute {
    fn drop(&mut self) {
        self.router.deregister(&self.thread_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use serde_json::json;

    fn scripted() -> (Arc<AppServerClient>, Arc<Mutex<Vec<Value>>>) {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(Vec::new(), sent.clone());
        (Arc::new(client), sent)
    }

    fn passing(_: &AppServerClient, _: &Value) -> Scoped {
        Scoped::Passed
    }

    fn notification(thread_id: &str, method: &str) -> Value {
        json!({ "method": method, "params": { "threadId": thread_id } })
    }

    fn errors(sent: &Mutex<Vec<Value>>) -> Vec<(Value, String)> {
        sent.lock()
            .unwrap()
            .iter()
            .filter_map(|message| {
                let message_text = message.pointer("/error/message")?.as_str()?;
                Some((message["id"].clone(), message_text.to_string()))
            })
            .collect()
    }

    #[test]
    fn each_thread_receives_only_its_own_messages() {
        let (client, _) = scripted();
        let router = ThreadRouter::new(client, passing);
        let (rx_a, _a) = router.register("thread-a");
        let (rx_b, _b) = router.register("thread-b");

        router.deliver(notification("thread-a", "item/agentMessage/delta"));
        router.deliver(notification("thread-b", "turn/completed"));
        router.deliver(notification("thread-a", "turn/completed"));

        let methods = |rx: &Receiver<Value>| -> Vec<String> {
            rx.try_iter()
                .map(|message| message["method"].as_str().unwrap().to_string())
                .collect()
        };
        assert_eq!(
            methods(&rx_a),
            ["item/agentMessage/delta", "turn/completed"]
        );
        assert_eq!(methods(&rx_b), ["turn/completed"]);
    }

    #[test]
    fn a_message_for_an_unknown_thread_is_dropped_and_a_request_declined() {
        let (client, sent) = scripted();
        let router = ThreadRouter::new(client, passing);
        let (rx, _route) = router.register("thread-live");

        router.deliver(notification("thread-gone", "turn/completed"));
        router.deliver(json!({
            "id": 4,
            "method": "item/commandExecution/requestApproval",
            "params": { "threadId": "thread-gone" },
        }));

        assert!(rx.try_recv().is_err(), "a live thread sees none of it");
        assert_eq!(
            errors(&sent),
            [(json!(4), "pooled call thread no longer active".to_string())]
        );
    }

    #[test]
    fn closing_disconnects_every_receiver_and_later_registrations() {
        let (client, _) = scripted();
        let router = ThreadRouter::new(client, passing);
        let (rx_a, _a) = router.register("thread-a");
        let (rx_b, _b) = router.register("thread-b");

        router.close();

        assert!(matches!(rx_a.recv(), Err(crossbeam_channel::RecvError)));
        assert!(matches!(rx_b.recv(), Err(crossbeam_channel::RecvError)));
        let (late, _late) = router.register("thread-late");
        assert!(
            matches!(late.recv(), Err(crossbeam_channel::RecvError)),
            "a thread registered on a dead server never waits on it"
        );
        assert!(
            !router.deregister("thread-a"),
            "the death already removed it"
        );
    }

    #[test]
    fn dropping_a_route_deregisters_its_thread() {
        let (client, _) = scripted();
        let router = ThreadRouter::new(client, passing);
        let (rx, route) = router.register("thread-a");
        assert!(router.is_routed("thread-a"));
        assert_eq!(route.thread_id(), "thread-a");

        drop(route);

        assert!(!router.is_routed("thread-a"));
        assert!(matches!(rx.recv(), Err(crossbeam_channel::RecvError)));
    }

    #[test]
    fn a_scoped_request_is_answered_by_the_owner_or_declined() {
        let (client, sent) = scripted();
        let router = ThreadRouter::new(client, |client, message| {
            if message["method"] == "account/chatgptAuthTokens/refresh" {
                // The scripted server records the answer, then errors: no
                // request of its own waits on it.
                let _ = client.respond(&message["id"], json!({"ok": true}));
                Scoped::Answered
            } else {
                Scoped::Passed
            }
        });

        router
            .deliver(json!({"id": 1, "method": "account/chatgptAuthTokens/refresh", "params": {}}));
        router.deliver(json!({"id": 2, "method": "account/somethingElse", "params": {}}));
        router.deliver(json!({"method": "account/rateLimits/updated", "params": {}}));

        let sent = sent.lock().unwrap();
        assert_eq!(
            sent.len(),
            2,
            "one answer, one decline, nothing for the notification"
        );
        assert_eq!(sent[0]["id"], 1);
        assert_eq!(sent[0]["result"], json!({"ok": true}));
        assert_eq!(sent[1]["id"], 2);
        assert_eq!(
            sent[1]["error"]["message"],
            "unsupported pool-scoped request"
        );
    }

    /// An owned router answers what its handler takes as it arrives, and
    /// hands everything else, requests included, to its owner in order; the
    /// owner's channel closes with the server's.
    #[test]
    fn an_owned_router_hands_its_owner_everything_its_handler_leaves() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let (client, server) = AppServerClient::for_test_with_notifications(
            |_| Err("no requests".to_string()),
            sent.clone(),
        );
        let (router, events) = ThreadRouter::start_owned(Arc::new(client), |client, message| {
            if message["method"] == "account/chatgptAuthTokens/refresh" {
                let _ = client.respond(&message["id"], json!({"ok": true}));
                Scoped::Answered
            } else {
                Scoped::Passed
            }
        });

        server
            .send(json!({"method": "thread/started", "params": {"threadId": "thread-a"}}))
            .unwrap();
        server
            .send(json!({"id": 0, "method": "account/chatgptAuthTokens/refresh", "params": {}}))
            .unwrap();
        server
            .send(json!({"method": "account/rateLimits/updated", "params": {}}))
            .unwrap();
        server
            .send(json!({"id": 1, "method": "account/somethingElse", "params": {}}))
            .unwrap();
        server
            .send(
                json!({"id": 2, "method": "item/commandExecution/requestApproval",
                "params": {"threadId": "thread-a"}}),
            )
            .unwrap();

        let methods: Vec<Value> = (0..4)
            .map(|_| events.recv_timeout(Duration::from_secs(5)).unwrap()["method"].clone())
            .collect();
        assert_eq!(
            methods,
            [
                "thread/started",
                "account/rateLimits/updated",
                "account/somethingElse",
                "item/commandExecution/requestApproval"
            ]
        );
        let sent = sent.lock().unwrap().clone();
        assert_eq!(
            sent.len(),
            1,
            "the owner's requests are not declined: {sent:?}"
        );
        assert_eq!(sent[0]["id"], 0);
        assert!(!router.close_if_idle(), "an owned router is never idle");

        drop(server);
        assert!(matches!(
            events.recv_timeout(Duration::from_secs(5)),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected)
        ));
        assert!(router.is_closed());
    }

    #[test]
    fn the_dispatcher_routes_then_closes_routes_before_telling_the_owner() {
        let (client, server) = AppServerClient::for_test_with_notifications(
            |_| Err("no requests".to_string()),
            Arc::new(Mutex::new(Vec::new())),
        );
        let (told_tx, told_rx) = unbounded();
        let router = ThreadRouter::start(Arc::new(client), passing, move || {
            told_tx.send(()).unwrap();
        });
        let (rx, _route) = router.register("thread-a");

        server
            .send(notification("thread-a", "turn/completed"))
            .unwrap();
        let routed = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(routed["method"], "turn/completed");

        drop(server);
        told_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            matches!(
                rx.try_recv(),
                Err(crossbeam_channel::TryRecvError::Disconnected)
            ),
            "every route is closed by the time the owner hears"
        );
    }
}
