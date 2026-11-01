//! The relay doorbell: a WebSocket to the api that says "there are events".
//!
//! The api sends `{"kind":"events"}` when the socket opens and on every
//! stored row. No event body ever travels over it: a ring only cuts the sync
//! loop's wait short, and the loop reads `/events` as it always does. While
//! the socket is up the loop polls every minute; while it is down, every 15
//! seconds, so a lost doorbell costs latency and nothing else.
//!
//! The handshake carries the channel secret as `Authorization: Bearer`, never
//! in the URL, and neither the request nor its headers are ever logged.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use tokio::{net::TcpStream, sync::Notify};
use tokio_tungstenite::{
    tungstenite::{
        client::IntoClientRequest,
        http::{header::AUTHORIZATION, HeaderValue},
        Error as WsError, Message,
    },
    Connector, MaybeTlsStream, WebSocketStream,
};

use super::channel;
use crate::orchestrator::Orchestrator;

const PING_EVERY: Duration = Duration::from_secs(30);
/// Two missed pongs: the socket is dead even if the OS has not noticed.
const SILENCE_LIMIT: Duration = Duration::from_secs(75);
const RECONNECT_MIN: Duration = Duration::from_secs(2);
const RECONNECT_MAX: Duration = Duration::from_secs(60);

/// What the doorbell tells the sync loop: whether it is up, and a ring.
#[derive(Debug, Default)]
pub struct Doorbell {
    connected: AtomicBool,
    ring: Notify,
}

impl Doorbell {
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    /// Resolves on the next ring. A ring with no waiter is kept, so a burst
    /// of rings while the loop is syncing becomes one more sync.
    pub(crate) async fn rung(&self) {
        self.ring.notified().await;
    }

    fn ring(&self) {
        self.ring.notify_one();
    }

    fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Release);
    }
}

static INSTALL_DOORBELL: std::sync::OnceLock<Arc<Doorbell>> = std::sync::OnceLock::new();

/// Make `doorbell` the one [`connected`] reports.
pub(crate) fn publish(doorbell: Arc<Doorbell>) {
    let _ = INSTALL_DOORBELL.set(doorbell);
}

/// Whether this process's relay doorbell is connected, for status surfaces.
pub fn connected() -> bool {
    INSTALL_DOORBELL
        .get()
        .is_some_and(|doorbell| doorbell.is_connected())
}

/// The socket URL for a channel: `https://host/relay` becomes
/// `wss://host/relay/ws/{channel}`.
fn socket_url(relay_url: &str, channel_id: &str) -> Result<String, String> {
    let base = relay_url.trim_end_matches('/');
    let base = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        return Err("the relay URL is neither https nor http".to_string());
    };
    Ok(format!("{base}/ws/{channel_id}"))
}

enum ConnectError {
    /// 401 or 404 on the handshake: the api refused the secret or has no
    /// such channel.
    Rejected,
    Failed(String),
}

type RelaySocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn connect(url: &str, secret: &str) -> Result<RelaySocket, ConnectError> {
    let mut request = url
        .into_client_request()
        .map_err(|_| ConnectError::Failed("the relay socket URL is invalid".to_string()))?;
    let authorization = HeaderValue::from_str(&format!("Bearer {secret}")).map_err(|_| {
        ConnectError::Failed("the relay channel secret is not a valid header value".to_string())
    })?;
    request.headers_mut().insert(AUTHORIZATION, authorization);
    // The shared config, not tungstenite's default: that one reloads the
    // platform root store on every `wss://` dial, inside this task's poll.
    cairn_tls::ready().await;
    let connector = Connector::Rustls(cairn_tls::websocket_config());
    match tokio_tungstenite::connect_async_tls_with_config(request, None, false, Some(connector))
        .await
    {
        Ok((socket, _)) => Ok(socket),
        Err(WsError::Http(response)) if matches!(response.status().as_u16(), 401 | 404) => {
            Err(ConnectError::Rejected)
        }
        Err(WsError::Http(response)) => Err(ConnectError::Failed(format!(
            "the relay refused the socket ({})",
            response.status()
        ))),
        Err(error) => Err(ConnectError::Failed(error.to_string())),
    }
}

fn is_events_nudge(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .is_some_and(|frame| frame["kind"] == "events")
}

/// Read frames until the socket ends or goes silent, ringing on each nudge.
///
/// The stream is not split: reading through the whole `WebSocketStream` is
/// what lets tungstenite answer the api's protocol pings.
async fn listen(mut socket: RelaySocket, doorbell: &Doorbell) {
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping.tick().await;
    let mut heard = tokio::time::Instant::now();
    loop {
        tokio::select! {
            frame = socket.next() => {
                let frame = match frame {
                    None => return,
                    Some(Err(error)) => {
                        log::debug!("relay doorbell read failed: {error}");
                        return;
                    }
                    Some(Ok(frame)) => frame,
                };
                heard = tokio::time::Instant::now();
                match frame {
                    Message::Text(text) if is_events_nudge(&text) => doorbell.ring(),
                    Message::Close(_) => return,
                    _ => {}
                }
            }
            _ = ping.tick() => {
                if socket.send(Message::Text("ping".into())).await.is_err() {
                    return;
                }
            }
            // The channel was replaced or deleted: this socket may be on one
            // the install no longer holds. Reconnect, re-reading the row.
            _ = channel::channel_changed() => return,
            _ = tokio::time::sleep_until(heard + SILENCE_LIMIT) => {
                log::debug!(
                    "relay doorbell heard nothing for {}s; reconnecting",
                    SILENCE_LIMIT.as_secs()
                );
                return;
            }
        }
    }
}

/// Keep the doorbell connected for the life of the process.
pub(crate) async fn maintain(orch: Orchestrator, relay_url: String, doorbell: Arc<Doorbell>) {
    let mut backoff = RECONNECT_MIN;
    loop {
        let channel = channel::get_relay_channel(&orch.db.local)
            .await
            .unwrap_or_else(|error| {
                log::warn!("relay doorbell could not read the channel: {error}");
                channel::RelayChannel::default()
            });
        let Some((channel_id, secret)) = channel.credentials() else {
            // No channel yet. `ensure` announces one; the timer is a backstop
            // for an announcement that landed before this wait began.
            tokio::select! {
                _ = channel::channel_changed() => {}
                _ = tokio::time::sleep(RECONNECT_MAX) => {}
            }
            continue;
        };
        match socket_url(&relay_url, channel_id) {
            Err(error) => {
                log::warn!("relay doorbell disabled: {error}");
                backoff = RECONNECT_MAX;
            }
            Ok(url) => match connect(&url, secret).await {
                Ok(socket) => {
                    backoff = RECONNECT_MIN;
                    if let Err(error) = channel::clear_rejection(&orch.db.local).await {
                        log::warn!("relay doorbell could not record the accepted channel: {error}");
                    }
                    doorbell.set_connected(true);
                    listen(socket, &doorbell).await;
                    doorbell.set_connected(false);
                    // Back to 15-second polling, starting with a sync now:
                    // a nudge may have been lost with the socket.
                    doorbell.ring();
                }
                Err(ConnectError::Rejected) => {
                    if let Err(error) = channel::mark_rejected(&orch.db.local).await {
                        log::warn!("relay doorbell could not record the rejection: {error}");
                    }
                    backoff = RECONNECT_MAX;
                }
                Err(ConnectError::Failed(error)) => {
                    log::debug!("relay doorbell could not connect: {error}");
                }
            },
        }
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = channel::channel_changed() => backoff = RECONNECT_MIN,
        }
        backoff = (backoff * 2).min(RECONNECT_MAX);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::github::crypto;
    use axum::{
        extract::{
            ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade},
            Path, RawQuery, State,
        },
        http::{HeaderMap, StatusCode},
        response::{IntoResponse, Response},
        routing::get,
        Json, Router,
    };
    use serde_json::{json, Value};
    use std::sync::{atomic::AtomicUsize, Mutex};
    use tokio::sync::broadcast;

    const SECRET: &str = "channel-secret";

    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Signal {
        Nudge,
        Drop,
    }

    /// A stand-in relay: `/events` serves the queued events after `after_id`,
    /// `/ws` is the doorbell, and both refuse a wrong secret.
    struct MockRelay {
        horizon: Mutex<String>,
        socket_channels: Mutex<Vec<String>>,
        events: Mutex<Vec<Value>>,
        event_queries: Mutex<Vec<String>>,
        reject_socket: AtomicBool,
        reject_events: AtomicBool,
        connects: AtomicUsize,
        signals: broadcast::Sender<Signal>,
    }

    fn authorized(headers: &HeaderMap) -> bool {
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            == Some(&format!("Bearer {SECRET}"))
    }

    async fn events(
        State(relay): State<Arc<MockRelay>>,
        Path(_channel): Path<String>,
        RawQuery(query): RawQuery,
        headers: HeaderMap,
    ) -> Response {
        if !authorized(&headers) || relay.reject_events.load(Ordering::Acquire) {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "unauthorized"})),
            )
                .into_response();
        }
        let query = query.unwrap_or_default();
        relay.event_queries.lock().unwrap().push(query.clone());
        let after_id = query
            .split('&')
            .find_map(|pair| pair.strip_prefix("after_id="))
            .and_then(|id| id.parse::<i64>().ok())
            .unwrap_or(0);
        let events: Vec<Value> = relay
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event["id"].as_i64().unwrap() > after_id)
            .cloned()
            .collect();
        let horizon = relay.horizon.lock().unwrap().clone();
        Json(json!({"has_more": false, "retention_horizon": horizon, "events": events}))
            .into_response()
    }

    async fn socket(
        State(relay): State<Arc<MockRelay>>,
        Path(channel): Path<String>,
        headers: HeaderMap,
        upgrade: WebSocketUpgrade,
    ) -> Response {
        if !authorized(&headers) || relay.reject_socket.load(Ordering::Acquire) {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "unauthorized"})),
            )
                .into_response();
        }
        relay.connects.fetch_add(1, Ordering::AcqRel);
        relay.socket_channels.lock().unwrap().push(channel);
        let signals = relay.signals.subscribe();
        upgrade.on_upgrade(move |socket| doorbell_socket(socket, signals))
    }

    async fn doorbell_socket(mut socket: WebSocket, mut signals: broadcast::Receiver<Signal>) {
        let nudge = || AxumMessage::Text(json!({"kind": "events"}).to_string().into());
        if socket.send(nudge()).await.is_err() {
            return;
        }
        loop {
            tokio::select! {
                signal = signals.recv() => match signal {
                    Ok(Signal::Nudge) => { let _ = socket.send(nudge()).await; }
                    Ok(Signal::Drop) | Err(_) => return,
                },
                frame = socket.recv() => match frame {
                    Some(Ok(AxumMessage::Text(text))) if text.as_str() == "ping" => {
                        let _ = socket.send(AxumMessage::Text("pong".into())).await;
                    }
                    Some(Ok(_)) => {}
                    _ => return,
                },
            }
        }
    }

    async fn mock_relay(horizon: &str) -> (String, Arc<MockRelay>) {
        let relay = Arc::new(MockRelay {
            horizon: Mutex::new(horizon.to_string()),
            socket_channels: Mutex::default(),
            events: Mutex::default(),
            event_queries: Mutex::default(),
            reject_socket: AtomicBool::new(false),
            reject_events: AtomicBool::new(false),
            connects: AtomicUsize::new(0),
            signals: broadcast::channel(16).0,
        });
        let app = Router::new()
            .route("/relay/events/{channel}", get(events))
            .route("/relay/ws/{channel}", get(socket))
            .with_state(relay.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/relay", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, relay)
    }

    /// An orchestrator whose channel `ch-1` holds a fresh keypair, with the
    /// first-party bugs endpoint for project `p` mirrored. Returns the public
    /// key events are sealed to.
    async fn seeded(
        temp: &std::path::Path,
        cursor: Option<&str>,
    ) -> (Orchestrator, Arc<crate::storage::LocalDb>, String) {
        let (orch, db) = crate::routes::fixture::orchestrator(temp, &[]).await;
        crate::ingress::upsert_endpoint(
            &db,
            &crate::ingress::EndpointRecord {
                id: "src_bugs".into(),
                channel_id: "ch-1".into(),
                project_id: Some("p".into()),
                name: "bugs".into(),
                provider: "first-party".into(),
                topic: Some("cairn.bugs".into()),
                status: "active".into(),
                granted_at: Some("2026-10-18T00:00:00Z".into()),
                created_at: 1,
                revoked_at: None,
                url: None,
                rejected_count: 0,
            },
        )
        .await
        .unwrap();
        let (public_key, private_key) = crypto::generate_keypair();
        let encrypted =
            crypto::encrypt_private_key(&private_key, &crypto::get_machine_id()).unwrap();
        let cursor = cursor.map(str::to_string);
        channel::update_relay_channel(&db, {
            let public_key = public_key.clone();
            move |channel| {
                channel.channel_id = Some("ch-1".into());
                channel.secret = Some(SECRET.into());
                channel.public_key = Some(public_key);
                channel.private_key_encrypted = Some(encrypted);
                channel.last_event_sync = cursor;
            }
        })
        .await
        .unwrap();
        (orch, db, public_key)
    }

    fn sealed_bug(public_key: &str, id: i64, report: i64) -> Value {
        json!({
            "id": id,
            "event_type": "bug.reported",
            "payload": crypto::seal_v2_for_test(&json!({"id": report}).to_string(), public_key),
            "received_at": format!("2026-10-28T20:00:{id:02}.000Z"),
            "encrypted": true,
            "endpoint_id": "src_bugs",
            "delivery_id": format!("bug-report:{report}"),
            "verification": "first-party",
            "sender": null,
            "encryption": "sealed-v2",
        })
    }

    async fn eventually(what: &str, within: Duration, mut check: impl AsyncFnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + within;
        while tokio::time::Instant::now() < deadline {
            if check().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("{what} did not happen within {within:?}");
    }

    async fn delivered(db: &crate::storage::LocalDb) -> i64 {
        db.query_opt_i64(
            "SELECT COUNT(*) FROM ingress_events WHERE status = 'ok'",
            (),
        )
        .await
        .unwrap()
        .unwrap_or(0)
    }

    #[test]
    fn the_socket_url_rides_the_relay_url() {
        assert_eq!(
            socket_url("https://api.cairn.computer/relay", "ch-1").unwrap(),
            "wss://api.cairn.computer/relay/ws/ch-1"
        );
        assert_eq!(
            socket_url("http://127.0.0.1:9/relay/", "ch-1").unwrap(),
            "ws://127.0.0.1:9/relay/ws/ch-1"
        );
        assert!(socket_url("ftp://x/relay", "ch-1").is_err());
    }

    #[test]
    fn only_an_events_frame_rings() {
        assert!(is_events_nudge(r#"{"kind":"events"}"#));
        assert!(!is_events_nudge("pong"));
        assert!(!is_events_nudge(r#"{"kind":"other"}"#));
    }

    /// A ring reaches the loop within a second while its 60-second timer is
    /// still pending, and the event behind it is delivered.
    #[tokio::test]
    async fn a_doorbell_poke_syncs_within_a_second_while_the_timer_waits() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db, public_key) = seeded(temp.path(), Some("2026-10-28T19:00:00.000Z|1")).await;
        let (url, relay) = mock_relay("2026-10-01T00:00:00.000Z").await;
        let doorbell = Arc::new(Doorbell::default());
        tokio::spawn(maintain(orch.clone(), url.clone(), doorbell.clone()));
        tokio::spawn(crate::ingress::sync::run(
            orch.clone(),
            url,
            doorbell.clone(),
        ));

        eventually(
            "the doorbell connects",
            Duration::from_secs(5),
            async || doorbell.is_connected(),
        )
        .await;
        eventually("a first sync", Duration::from_secs(5), async || {
            !relay.event_queries.lock().unwrap().is_empty()
        })
        .await;
        // Let the open-time nudge's sync settle, so the loop is in its
        // 60-second wait when the event lands.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let syncs = relay.event_queries.lock().unwrap().len();

        relay
            .events
            .lock()
            .unwrap()
            .push(sealed_bug(&public_key, 2, 7));
        relay.signals.send(Signal::Nudge).unwrap();
        eventually(
            "the poked event is delivered",
            Duration::from_secs(1),
            async || delivered(&db).await == 1,
        )
        .await;
        assert_eq!(relay.event_queries.lock().unwrap().len(), syncs + 1);
        let channel = channel::get_relay_channel(&db).await.unwrap();
        assert_eq!(
            channel.last_event_sync.as_deref(),
            Some("2026-10-28T20:00:02.000Z|2")
        );
    }

    /// A refused handshake marks the channel `unauthorized`; nothing
    /// re-registers it.
    #[tokio::test]
    async fn a_rejected_handshake_marks_the_channel_unauthorized() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db, _) = seeded(temp.path(), None).await;
        let (url, relay) = mock_relay("2026-10-01T00:00:00.000Z").await;
        relay.reject_socket.store(true, Ordering::Release);
        let doorbell = Arc::new(Doorbell::default());
        tokio::spawn(maintain(orch.clone(), url, doorbell.clone()));

        eventually(
            "the rejection is recorded",
            Duration::from_secs(3),
            async || {
                channel::get_relay_channel(&db)
                    .await
                    .unwrap()
                    .health_state
                    .as_deref()
                    == Some("unauthorized")
            },
        )
        .await;
        let channel = channel::get_relay_channel(&db).await.unwrap();
        assert_eq!(
            channel.health_reason.as_deref(),
            Some("relay_channel_rejected")
        );
        assert_eq!(channel.channel_id.as_deref(), Some("ch-1"));
        assert!(!doorbell.is_connected());
        assert_eq!(relay.connects.load(Ordering::Acquire), 0);
    }

    /// `/events` refusing the secret is the same rejection, and the loop
    /// backs off at its maximum rather than doubling toward it.
    #[tokio::test]
    async fn a_rejected_sync_marks_the_channel_unauthorized() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db, _) = seeded(temp.path(), None).await;
        let (url, relay) = mock_relay("2026-10-01T00:00:00.000Z").await;
        relay.reject_events.store(true, Ordering::Release);
        let doorbell = Arc::new(Doorbell::default());
        tokio::spawn(crate::ingress::sync::run(orch.clone(), url, doorbell));
        eventually(
            "the rejection is recorded",
            Duration::from_secs(3),
            async || {
                channel::get_relay_channel(&db)
                    .await
                    .unwrap()
                    .health_reason
                    .as_deref()
                    == Some("relay_channel_rejected")
            },
        )
        .await;
    }

    /// A dropped socket puts the loop back on 15-second polling and the
    /// doorbell reconnects on its own.
    #[tokio::test]
    async fn a_dropped_socket_falls_back_to_polling_and_reconnects() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, _db, _) = seeded(temp.path(), None).await;
        let (url, relay) = mock_relay("2026-10-01T00:00:00.000Z").await;
        let doorbell = Arc::new(Doorbell::default());
        tokio::spawn(maintain(orch.clone(), url, doorbell.clone()));
        eventually(
            "the doorbell connects",
            Duration::from_secs(5),
            async || doorbell.is_connected(),
        )
        .await;

        relay.signals.send(Signal::Drop).unwrap();
        eventually("the drop is noticed", Duration::from_secs(2), async || {
            !doorbell.is_connected()
        })
        .await;
        assert_eq!(
            crate::ingress::sync::poll_interval_for_test(doorbell.is_connected()),
            Duration::from_secs(15)
        );
        eventually(
            "the doorbell reconnects",
            Duration::from_secs(6),
            async || doorbell.is_connected() && relay.connects.load(Ordering::Acquire) == 2,
        )
        .await;
    }

    /// A stored cursor behind the retention horizon syncs from the horizon,
    /// and the gap is recorded where the channel's health shows it.
    #[tokio::test]
    async fn a_stale_cursor_is_clamped_to_the_horizon_and_the_gap_recorded() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db, _) = seeded(temp.path(), Some("2026-09-01T00:00:00.000Z|5")).await;
        let (url, relay) = mock_relay("2026-10-01T00:00:00.000Z").await;
        let doorbell = Arc::new(Doorbell::default());
        tokio::spawn(crate::ingress::sync::run(orch.clone(), url, doorbell));
        eventually("a clamped sync", Duration::from_secs(3), async || {
            relay.event_queries.lock().unwrap().len() >= 2
        })
        .await;
        let queries = relay.event_queries.lock().unwrap().clone();
        assert_eq!(queries[0], "since=2026-09-01T00%3A00%3A00.000Z&after_id=5");
        assert_eq!(queries[1], "since=2026-09-30T23%3A59%3A59.999Z");
        let channel = channel::get_relay_channel(&db).await.unwrap();
        assert_eq!(channel.health_state.as_deref(), Some("skipping"));
        assert_eq!(
            channel.health_reason.as_deref(),
            Some("relay_retention_gap")
        );
        assert_eq!(
            channel.failing_event_at.as_deref(),
            Some("2026-09-01T00:00:00.000Z")
        );
        assert_eq!(
            channel.last_event_sync.as_deref(),
            Some("2026-09-30T23:59:59.999Z")
        );
    }

    /// The api's horizon advances on every request, so an idle channel's
    /// cursor falls behind it with nothing pruned unread: that is no gap.
    #[tokio::test]
    async fn an_idle_channel_behind_a_moving_horizon_is_not_a_gap() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db, _) = seeded(temp.path(), None).await;
        let (url, relay) = mock_relay("2026-10-01T00:00:00.000Z").await;
        crate::ingress::sync::sync_once(&orch, &url).await.unwrap();
        *relay.horizon.lock().unwrap() = "2026-10-02T00:00:00.000Z".into();
        crate::ingress::sync::sync_once(&orch, &url).await.unwrap();
        crate::ingress::sync::sync_once(&orch, &url).await.unwrap();
        let channel = channel::get_relay_channel(&db).await.unwrap();
        assert_ne!(channel.health_state.as_deref(), Some("skipping"));
        assert_eq!(channel.health_reason, None);
        assert!(channel.drained_at.is_some());
        assert_eq!(
            channel.last_event_sync.as_deref(),
            Some("2026-10-01T23:59:59.999Z")
        );
    }

    /// A channel last drained before the horizon really did lose events.
    #[tokio::test]
    async fn a_drain_older_than_the_horizon_is_a_gap() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db, _) = seeded(temp.path(), Some("2026-09-01T00:00:00.000Z|5")).await;
        channel::mark_drained(&db, "2026-09-01T00:00:01Z")
            .await
            .unwrap();
        let (url, _relay) = mock_relay("2026-10-01T00:00:00.000Z").await;
        crate::ingress::sync::sync_once(&orch, &url).await.unwrap();
        let channel = channel::get_relay_channel(&db).await.unwrap();
        assert_eq!(
            channel.health_reason.as_deref(),
            Some("relay_retention_gap")
        );
    }

    /// A replaced channel moves the doorbell: it leaves the old socket and
    /// connects on the new channel rather than listening to an orphan.
    #[tokio::test]
    async fn the_doorbell_follows_a_replaced_channel() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db, _) = seeded(temp.path(), None).await;
        let (url, relay) = mock_relay("2026-10-01T00:00:00.000Z").await;
        let doorbell = Arc::new(Doorbell::default());
        tokio::spawn(maintain(orch.clone(), url, doorbell.clone()));
        eventually(
            "the doorbell connects",
            Duration::from_secs(5),
            async || doorbell.is_connected(),
        )
        .await;
        channel::update_relay_channel(&db, |channel| channel.channel_id = Some("ch-new".into()))
            .await
            .unwrap();
        channel::notify_channel_changed();
        eventually(
            "the doorbell moves to ch-new",
            Duration::from_secs(6),
            async || {
                relay
                    .socket_channels
                    .lock()
                    .unwrap()
                    .last()
                    .map(String::as_str)
                    == Some("ch-new")
                    && doorbell.is_connected()
            },
        )
        .await;
        assert_eq!(
            relay
                .socket_channels
                .lock()
                .unwrap()
                .first()
                .map(String::as_str),
            Some("ch-1")
        );
    }
}
