//! Background sync task — consumes SyncMessages and batches durable ones via HTTP.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::MaybeTlsStream;

use super::message::SyncMessage;

type WsSink =
    SplitSink<tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, WsMessage>;

/// Background task: consumes SyncMessages and batches durable ones via HTTP.
pub struct SyncTask {
    rx: mpsc::UnboundedReceiver<SyncMessage>,
    api_url: String,
    jwt_provider: Arc<dyn Fn() -> Option<String> + Send + Sync>,
    device_id: String,
}

impl SyncTask {
    pub fn new(
        rx: mpsc::UnboundedReceiver<SyncMessage>,
        jwt_provider: Arc<dyn Fn() -> Option<String> + Send + Sync>,
        device_id: String,
        api_config: crate::api::ApiConfig,
    ) -> Self {
        Self {
            rx,
            api_url: api_config.base_url,
            jwt_provider,
            device_id,
        }
    }

    /// Try to establish a WebSocket connection to the cloud device endpoint.
    async fn try_connect_ws(&self) -> Option<WsSink> {
        let jwt = (self.jwt_provider)()?;
        let ws_url = self
            .api_url
            .replace("https://", "wss://")
            .replace("http://", "ws://");
        let url = format!("{}/remote/ws/{}?token={}", ws_url, self.device_id, jwt);

        match tokio_tungstenite::connect_async(&url).await {
            Ok((stream, _)) => {
                log::info!("Sync WS connected to {}", self.device_id);
                let (sink, mut read) = stream.split();

                // Spawn a reader task that handles heartbeat_ack and ack messages.
                // On close/error it just exits — the main loop detects via send failures.
                tokio::spawn(async move {
                    while let Some(msg) = read.next().await {
                        match msg {
                            Ok(WsMessage::Text(text)) => {
                                log::debug!("Sync WS recv: {}", &text[..text.len().min(120)]);
                            }
                            Ok(WsMessage::Close(_)) => break,
                            Err(e) => {
                                log::debug!("Sync WS read error: {}", e);
                                break;
                            }
                            _ => {}
                        }
                    }
                });

                Some(sink)
            }
            Err(e) => {
                log::warn!("Sync WS connection failed: {}", e);
                None
            }
        }
    }

    /// Run the sync task. This loops forever until the channel is closed.
    pub async fn run(mut self) {
        let mut batch: Vec<SyncMessage> = Vec::new();
        let mut batch_timer = tokio::time::interval(Duration::from_millis(500));
        let mut retry_queue: VecDeque<Vec<SyncMessage>> = VecDeque::new();
        let mut backoff = Backoff::new();
        let client = reqwest::Client::new();

        // WebSocket for low-latency stream_delta delivery
        let mut ws_backoff = Duration::from_secs(1);
        let ws_max_backoff = Duration::from_secs(60);
        let mut heartbeat = tokio::time::interval(Duration::from_secs(30));

        // Initial WS connection attempt
        let mut ws: Option<WsSink> = self.try_connect_ws().await;

        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(SyncMessage::StreamDelta(_)) => {}
                        Some(msg) if msg.is_durable() => {
                            batch.push(msg);
                        }
                        Some(_) => {} // Local-only non-durable, drop
                        None => {
                            // Channel closed — flush remaining batch and exit
                            if !batch.is_empty() {
                                let to_send = std::mem::take(&mut batch);
                                let _ = self.send_batch_http(&client, &to_send).await;
                            }
                            // Close WS cleanly
                            if let Some(ref mut sink) = ws {
                                let _ = sink.close().await;
                            }
                            log::info!("Sync task: channel closed, exiting");
                            return;
                        }
                    }
                }

                _ = batch_timer.tick() => {
                    // Flush batch via HTTP (durable messages)
                    if !batch.is_empty() {
                        let to_send = std::mem::take(&mut batch);
                        match self.send_batch_http(&client, &to_send).await {
                            Ok(_) => backoff.reset(),
                            Err(e) => {
                                log::warn!("Sync batch failed: {}", e);
                                retry_queue.push_back(to_send);
                            }
                        }
                    }

                    // Process retry queue
                    if let Some(retry_batch) = retry_queue.front() {
                        if backoff.ready() {
                            match self.send_batch_http(&client, retry_batch).await {
                                Ok(_) => {
                                    retry_queue.pop_front();
                                    backoff.reset();
                                }
                                Err(e) => {
                                    log::warn!("Sync retry failed: {}", e);
                                    backoff.next();
                                }
                            }
                        }
                    }

                    // Prevent unbounded retry queue growth
                    while retry_queue.len() > 100 {
                        let dropped = retry_queue.pop_front();
                        if let Some(batch) = dropped {
                            log::warn!("Sync: dropped {} messages from retry queue (overflow)", batch.len());
                        }
                    }

                    // Reconnect WS if disconnected
                    if ws.is_none() {
                        match self.try_connect_ws().await {
                            Some(sink) => {
                                ws = Some(sink);
                                ws_backoff = Duration::from_secs(1);
                            }
                            None => {
                                ws_backoff = (ws_backoff * 2).min(ws_max_backoff);
                            }
                        }
                    }
                }

                _ = heartbeat.tick() => {
                    if let Some(ref mut sink) = ws {
                        let hb = serde_json::json!({"type": "heartbeat"});
                        if sink.send(WsMessage::Text(hb.to_string().into())).await.is_err() {
                            log::debug!("Sync WS heartbeat failed, will reconnect");
                            ws = None;
                        }
                    }
                }
            }
        }
    }

    /// Send a batch of messages via HTTP POST to /remote/sync.
    async fn send_batch_http(
        &self,
        client: &reqwest::Client,
        messages: &[SyncMessage],
    ) -> Result<(), String> {
        let jwt = (self.jwt_provider)().ok_or("No JWT available")?;

        let payload = serde_json::json!({
            "messages": messages.iter().map(|m| {
                // Convert SyncMessage to the wire format expected by the API
                match m {
                    SyncMessage::Project(data) => serde_json::json!({
                        "table": "projects", "action": "upsert", "id": data.id, "data": data
                    }),
                    SyncMessage::Issue(data) => serde_json::json!({
                        "table": "issues", "action": "upsert", "id": data.id, "data": data
                    }),
                    SyncMessage::Job(data) => serde_json::json!({
                        "table": "jobs", "action": "upsert", "id": data.id, "data": data
                    }),
                    SyncMessage::Run(data) => serde_json::json!({
                        "table": "runs", "action": "upsert", "id": data.id, "data": data
                    }),
                    SyncMessage::Event(data) => serde_json::json!({
                        "table": "events", "action": "upsert", "id": data.id, "data": data
                    }),
                    SyncMessage::Artifact(data) => serde_json::json!({
                        "table": "artifacts", "action": "upsert", "id": data.id, "data": data
                    }),
                    SyncMessage::Comment(data) => serde_json::json!({
                        "table": "comments", "action": "upsert", "id": data.id, "data": data
                    }),
                    SyncMessage::StreamDelta(_) => serde_json::json!(null), // Skipped
                    SyncMessage::Delete { table, id } => serde_json::json!({
                        "table": table, "action": "delete", "id": id
                    }),
                }
            }).filter(|v| !v.is_null()).collect::<Vec<_>>()
        });

        let resp = client
            .post(format!("{}/remote/sync", self.api_url))
            .bearer_auth(&jwt)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("Sync HTTP request failed: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Sync API returned {}: {}", status, body));
        }

        let result: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse sync response: {}", e))?;

        let acked = result["acked"].as_u64().unwrap_or(0);
        let errors = result["errors"].as_array().map(|a| a.len()).unwrap_or(0);

        if errors > 0 {
            log::warn!("Sync batch: {} acked, {} errors", acked, errors);
        } else {
            log::debug!("Sync batch: {} messages acked", acked);
        }

        Ok(())
    }
}

/// Simple exponential backoff with jitter.
struct Backoff {
    attempt: u32,
    max_delay_secs: u64,
    last_attempt: Option<tokio::time::Instant>,
}

impl Backoff {
    fn new() -> Self {
        Self {
            attempt: 0,
            max_delay_secs: 300, // 5 minutes max
            last_attempt: None,
        }
    }

    fn reset(&mut self) {
        self.attempt = 0;
        self.last_attempt = None;
    }

    fn next(&mut self) {
        self.attempt = self.attempt.saturating_add(1);
        self.last_attempt = Some(tokio::time::Instant::now());
    }

    fn ready(&self) -> bool {
        match self.last_attempt {
            None => true,
            Some(last) => {
                let delay_secs =
                    std::cmp::min(2u64.saturating_pow(self.attempt), self.max_delay_secs);
                last.elapsed() >= Duration::from_secs(delay_secs)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_ready_initially() {
        let b = Backoff::new();
        assert!(b.ready());
    }

    #[test]
    fn backoff_not_ready_immediately_after_next() {
        let mut b = Backoff::new();
        b.next();
        // attempt=1 → delay=2s, so it should NOT be ready immediately
        assert!(!b.ready());
    }

    #[test]
    fn backoff_reset_makes_ready() {
        let mut b = Backoff::new();
        b.next();
        b.next();
        assert!(!b.ready());

        b.reset();
        assert!(b.ready());
        assert_eq!(b.attempt, 0);
    }

    #[test]
    fn backoff_delay_is_exponential() {
        let mut b = Backoff::new();

        // attempt 0 → ready (no last_attempt)
        assert!(b.ready());

        b.next(); // attempt=1 → 2s delay
        assert_eq!(b.attempt, 1);

        b.next(); // attempt=2 → 4s delay
        assert_eq!(b.attempt, 2);

        b.next(); // attempt=3 → 8s delay
        assert_eq!(b.attempt, 3);
    }

    #[test]
    fn backoff_caps_at_max_delay() {
        let mut b = Backoff::new();
        // Push attempt high enough that 2^attempt would exceed max_delay_secs (300)
        for _ in 0..20 {
            b.next();
        }
        // The delay calculation should cap at max_delay_secs
        let delay = std::cmp::min(2u64.saturating_pow(b.attempt), b.max_delay_secs);
        assert_eq!(delay, 300);
    }

    #[test]
    fn backoff_saturating_add_does_not_overflow() {
        let mut b = Backoff::new();
        b.attempt = u32::MAX;
        b.next(); // Should saturate, not panic
        assert_eq!(b.attempt, u32::MAX);
    }

    #[tokio::test]
    async fn sync_task_exits_on_channel_close() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        // No JWT: both try_connect_ws and send_batch_http short-circuit before any
        // network call, so the test exercises only the channel-close exit path and
        // never touches the network. ApiConfig::default() targets the real prod API,
        // so the previous fake-JWT form hit the network and flaked past the 5s
        // timeout under parallel load.
        let task = SyncTask::new(
            rx,
            Arc::new(|| None),
            "device-1".to_string(),
            crate::api::ApiConfig::default(),
        );

        // Send a message then close
        let _ = tx.send(SyncMessage::Delete {
            table: "t".into(),
            id: "1".into(),
        });
        drop(tx);

        // SyncTask::run will try to POST and fail (no server), but should
        // exit gracefully when the channel is closed.
        // Use a timeout to prevent test hanging.
        let result = tokio::time::timeout(Duration::from_secs(5), task.run()).await;

        assert!(result.is_ok(), "SyncTask should exit when channel closes");
    }
}
