//! Relay catch-up: the install's relay channel carries every ingress
//! endpoint's events and the GitHub App's webhooks on one shared cursor.
//!
//! Each event is handled on its own. An endpoint event goes to
//! [`super::consume`], which hands the GitHub App endpoint's events on to
//! [`crate::github::relay`]; a legacy `/hook` GitHub event (one with no
//! endpoint) goes to [`crate::github::relay`] directly. An
//! event that cannot be handled is recorded and skipped, and the cursor moves
//! past it, so one poison event never parks the feed for the events behind
//! it. The one exception is a key mismatch, which means every later event is
//! unreadable too: that pauses the channel until it is repaired.

use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;

use std::sync::Arc;

use super::channel::{self, RelayChannel};
use super::consume::NoBody;
use super::ws::Doorbell;
use crate::{github::crypto, orchestrator::Orchestrator};

/// How often the channel is polled while the doorbell is down.
const POLL_INTERVAL: Duration = Duration::from_secs(15);
/// How often it is polled while the doorbell is up: the doorbell rings on every
/// stored event, so the timer only catches a nudge the socket lost.
const DOORBELL_POLL_INTERVAL: Duration = Duration::from_secs(60);
const MAX_FAILURE_BACKOFF: Duration = Duration::from_secs(10 * 60);

fn poll_interval(doorbell_connected: bool) -> Duration {
    if doorbell_connected {
        DOORBELL_POLL_INTERVAL
    } else {
        POLL_INTERVAL
    }
}

#[derive(Debug, Deserialize)]
struct RelayResponse {
    events: Vec<RelayEvent>,
    #[serde(default)]
    has_more: bool,
    /// Rows older than this have been pruned. Absent from an older api.
    #[serde(default)]
    retention_horizon: Option<String>,
}

fn is_dead_epoch_event(event: &RelayEvent, rotated_at: Option<&str>) -> bool {
    let Some(rotated_at) = rotated_at else {
        return false;
    };
    let Ok(received_at) = chrono::DateTime::parse_from_rfc3339(&event.received_at) else {
        return false;
    };
    let Ok(rotated_at) = chrono::DateTime::parse_from_rfc3339(rotated_at) else {
        return false;
    };
    received_at < rotated_at
}

fn failure_backoff(consecutive_failures: u32) -> Duration {
    let multiplier = 1_u32
        .checked_shl(consecutive_failures.saturating_sub(1).min(31))
        .unwrap_or(u32::MAX);
    POLL_INTERVAL
        .checked_mul(multiplier)
        .unwrap_or(MAX_FAILURE_BACKOFF)
        .min(MAX_FAILURE_BACKOFF)
}

fn decode_cursor(cursor: &str) -> (&str, Option<i64>) {
    let Some((received_at, id)) = cursor.rsplit_once('|') else {
        return (cursor, None);
    };
    match id.parse() {
        Ok(id) => (received_at, Some(id)),
        Err(_) => (cursor, None),
    }
}

fn encode_cursor(received_at: &str, id: i64) -> String {
    format!("{received_at}|{id}")
}

/// Match the api's strict timestamp bound, with an id tie-breaker only for a
/// composite cursor. Compare instants, not RFC 3339 spellings.
fn is_after_cursor(event: &RelayEvent, cursor: Option<&str>) -> Result<bool, String> {
    let Some(cursor) = cursor else {
        return Ok(true);
    };
    let (since, after_id) = decode_cursor(cursor);
    let since = chrono::DateTime::parse_from_rfc3339(since)
        .map_err(|error| format!("invalid relay cursor timestamp: {error}"))?;
    let received_at = chrono::DateTime::parse_from_rfc3339(&event.received_at)
        .map_err(|error| format!("invalid relay event timestamp: {error}"))?;
    Ok(received_at > since || (received_at == since && after_id.is_some_and(|id| event.id > id)))
}

fn event_cursor(event: &RelayEvent) -> String {
    encode_cursor(&event.received_at, event.id)
}

#[derive(Debug, Default, Deserialize)]
pub struct RelayEvent {
    pub id: i64,
    pub event_type: String,
    pub payload: Value,
    pub received_at: String,
    #[serde(default)]
    pub encrypted: bool,
    /// Set only on an ingress endpoint's event; a GitHub webhook has none.
    #[serde(default)]
    pub endpoint_id: Option<String>,
    #[serde(default)]
    pub delivery_id: Option<String>,
    #[serde(default)]
    pub verification: Option<String>,
    #[serde(default)]
    pub sender: Option<Value>,
    /// The body format: `sealed-v1` or `sealed-v2`. An api that predates the
    /// column sends none, which means `sealed-v1`.
    #[serde(default)]
    pub encryption: Option<String>,
}

#[derive(Debug, PartialEq)]
enum RelayPayloadError {
    KeyMismatch,
    Malformed(String),
    UnknownEncryption(String),
}

fn decrypt_event_payload(
    payload: &Value,
    encryption: Option<&str>,
    private_key: &str,
) -> Result<Value, RelayPayloadError> {
    let encrypted = payload.as_str().ok_or_else(|| {
        RelayPayloadError::Malformed("encrypted relay payload was not a string".into())
    })?;
    let opened = match encryption.unwrap_or("sealed-v1") {
        "sealed-v1" => crypto::decrypt_payload(encrypted, private_key),
        "sealed-v2" => crypto::open_sealed_v2(encrypted, private_key),
        other => return Err(RelayPayloadError::UnknownEncryption(other.to_string())),
    };
    // Both openers fail a body sealed to another key as `Decryption failed`.
    let plaintext = opened.map_err(|error| {
        if error == "Decryption failed" {
            RelayPayloadError::KeyMismatch
        } else {
            RelayPayloadError::Malformed(error)
        }
    })?;
    serde_json::from_str(&plaintext).map_err(|error| {
        RelayPayloadError::Malformed(format!("invalid relay event payload: {error}"))
    })
}

fn record_key_mismatch(channel: &mut RelayChannel, event: &RelayEvent, now: &str) {
    let failures = channel.consecutive_failures.unwrap_or(0).saturating_add(1);
    channel.health_state = Some(
        if failures >= 2 {
            "key_mismatch"
        } else {
            "suspect"
        }
        .into(),
    );
    channel.health_reason = Some("relay_key_mismatch".into());
    channel
        .first_failure_at
        .get_or_insert_with(|| now.to_owned());
    channel.last_failure_at = Some(now.to_owned());
    channel.consecutive_failures = Some(failures);
    channel.failing_event_id = Some(event.id.to_string());
    channel.failing_event_at = Some(event.received_at.clone());
}

/// Record an event the consumer skipped. The feed keeps moving, so this is not
/// a failure state like `key_mismatch`: it is `skipping`, with the skipped
/// event and the reason, until the next successful delivery clears it. The
/// consecutive-failure count belongs to key mismatches and is left alone.
fn record_skip(
    channel: &mut RelayChannel,
    event_id: i64,
    received_at: &str,
    reason: &str,
    now: &str,
) {
    channel.health_state = Some("skipping".into());
    channel.health_reason = Some(reason.into());
    channel
        .first_failure_at
        .get_or_insert_with(|| now.to_owned());
    channel.last_failure_at = Some(now.to_owned());
    channel.failing_event_id = Some(event_id.to_string());
    channel.failing_event_at = Some(received_at.to_owned());
    channel.undeliverable_since_healthy = channel.undeliverable_since_healthy.saturating_add(1);
}

/// Record that the stored cursor fell behind the api's retention horizon: the
/// events between them were pruned unread. Visible like any skip, until the
/// next successful delivery clears it.
fn record_retention_gap(channel: &mut RelayChannel, stale_cursor_at: &str, now: &str) {
    channel.health_state = Some("skipping".into());
    channel.health_reason = Some("relay_retention_gap".into());
    channel
        .first_failure_at
        .get_or_insert_with(|| now.to_owned());
    channel.last_failure_at = Some(now.to_owned());
    channel.failing_event_id = None;
    channel.failing_event_at = Some(stale_cursor_at.to_owned());
}

/// Whether a sync from `cursor` would ask for events the api has pruned:
/// there is no cursor yet, or it is older than the horizon.
fn precedes_horizon(cursor: Option<&str>, horizon: &str) -> bool {
    let Ok(horizon) = chrono::DateTime::parse_from_rfc3339(horizon) else {
        return false;
    };
    let Some(cursor) = cursor else {
        return true;
    };
    chrono::DateTime::parse_from_rfc3339(decode_cursor(cursor).0)
        .is_ok_and(|received_at| received_at < horizon)
}

/// Whether the channel was drained at or after the horizon. A drain then read
/// every event the api has since pruned, so a cursor behind the horizon lost
/// nothing; only a runner offline longer than retention leaves a real gap.
fn drained_since(drained_at: Option<&str>, horizon: &str) -> bool {
    let (Some(drained_at), Ok(horizon)) = (
        drained_at.and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok()),
        chrono::DateTime::parse_from_rfc3339(horizon),
    ) else {
        return false;
    };
    drained_at >= horizon
}

/// The cursor a sync from the horizon starts at. One millisecond before it,
/// because the api returns events strictly after `since`.
fn horizon_cursor(horizon: &str) -> Option<String> {
    let horizon = chrono::DateTime::parse_from_rfc3339(horizon).ok()?;
    Some(
        (horizon - chrono::Duration::milliseconds(1))
            .with_timezone(&chrono::Utc)
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    )
}

fn record_successful_delivery(channel: &mut RelayChannel, now: &str) {
    channel.undeliverable_since_healthy = 0;
    channel.health_state = Some("healthy".into());
    channel.health_reason = None;
    channel.first_failure_at = None;
    channel.last_failure_at = None;
    channel.consecutive_failures = Some(0);
    channel.failing_event_id = None;
    channel.failing_event_at = None;
    channel.last_successful_delivery_at = Some(now.to_owned());
}

/// What became of one relay event.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum EventOutcome {
    /// Opened and handled: evidence the channel delivers.
    Delivered,
    /// Passed over without being opened: a dead-epoch event, or the skip
    /// marker the relay sends in place of an oversized payload.
    PassedOver,
    /// Could not be handled. Recorded, and the cursor still advances.
    Skipped {
        reason: &'static str,
        detail: String,
    },
}

/// What every event on one sync shares.
struct PageContext<'a> {
    request_cursor: Option<&'a str>,
    private_key: &'a str,
    key_rotated_at: Option<&'a str>,
    /// The GitHub App's slug, when an App is connected.
    app_slug: Option<&'a str>,
}

/// Handle one event. `Err` only for a key mismatch outside a dead epoch,
/// which is recorded first and pauses the channel.
async fn handle_event(
    orch: &Orchestrator,
    event: &RelayEvent,
    ctx: &PageContext<'_>,
) -> Result<EventOutcome, String> {
    // `Ok(None)` is a plaintext event: a skip marker in place of a payload.
    let opened: Result<Option<Value>, NoBody> = if event.encrypted {
        match decrypt_event_payload(&event.payload, event.encryption.as_deref(), ctx.private_key) {
            Ok(payload) => Ok(Some(payload)),
            Err(RelayPayloadError::KeyMismatch) => {
                if is_dead_epoch_event(event, ctx.key_rotated_at) {
                    log::warn!(
                        "skipping dead-epoch relay event {} (pre-rotation)",
                        event.id
                    );
                    return Ok(EventOutcome::PassedOver);
                }
                let now = chrono::Utc::now().to_rfc3339();
                channel::update_relay_channel(&orch.db.local, |channel| {
                    record_key_mismatch(channel, event, &now);
                })
                .await?;
                return Err(format!(
                    "relay key mismatch at event {}; cursor remains paused",
                    event.id
                ));
            }
            Err(RelayPayloadError::Malformed(error)) => Err(NoBody::missing(format!(
                "the sealed body could not be opened: {error}"
            ))),
            // Not a key problem: later events may well open, so the channel
            // keeps moving and this one is recorded undeliverable.
            Err(RelayPayloadError::UnknownEncryption(encryption)) => {
                Err(NoBody::unknown_encryption(&encryption))
            }
        }
    } else {
        Ok(None)
    };

    let Some(endpoint_id) = &event.endpoint_id else {
        let opened = opened.map_err(|why| why.detail);
        return Ok(
            crate::github::relay::handle_github_event(orch, event, opened, ctx.app_slug).await,
        );
    };
    let body = match opened {
        Ok(Some(payload)) => Ok(payload),
        Ok(None) => Err(NoBody::missing(format!(
            "the relay delivered no sealed body ({})",
            event.payload
        ))),
        Err(why) => Err(why),
    };
    let consumed = super::consume::consume(
        orch,
        super::consume::EndpointEvent {
            relay_event_id: event.id,
            endpoint_id: endpoint_id.clone(),
            delivery_id: event.delivery_id.clone(),
            event_type: event.event_type.clone(),
            verification: event.verification.clone(),
            sender: event.sender.clone(),
            received_at: event.received_at.clone(),
            body,
        },
        ctx.app_slug,
    )
    .await;
    Ok(match consumed {
        super::consume::Consumed::Delivered { .. } => EventOutcome::Delivered,
        super::consume::Consumed::Undeliverable { reason, detail } => {
            log::warn!(
                "Skipping relay event {} for ingress endpoint {endpoint_id}: {reason}: {detail}",
                event.id
            );
            EventOutcome::Skipped { reason, detail }
        }
    })
}

/// Handle a page of events in order, persisting the cursor and the health
/// evidence after each. Returns the cursor after the last event handled.
async fn apply_page(
    orch: &Orchestrator,
    events: Vec<RelayEvent>,
    ctx: &PageContext<'_>,
) -> Result<Option<String>, String> {
    let mut cursor = None;
    for event in events {
        if !is_after_cursor(&event, ctx.request_cursor)? {
            continue;
        }
        let event_cursor = event_cursor(&event);
        let outcome = handle_event(orch, &event, ctx).await?;
        let now = chrono::Utc::now().to_rfc3339();
        channel::update_relay_channel(&orch.db.local, |channel| {
            channel.last_event_sync = Some(event_cursor.clone());
            match &outcome {
                EventOutcome::Delivered => record_successful_delivery(channel, &now),
                EventOutcome::PassedOver => {}
                EventOutcome::Skipped { reason, .. } => {
                    record_skip(channel, event.id, &event.received_at, reason, &now)
                }
            }
        })
        .await?;
        cursor = Some(event_cursor);
    }
    Ok(cursor)
}

#[derive(Debug, PartialEq)]
pub(crate) enum SyncError {
    /// The api refused the channel secret (401) or no longer has the channel
    /// (404). Recorded as `unauthorized`; retrying sooner cannot help.
    Rejected,
    Failed(String),
}

impl From<String> for SyncError {
    fn from(error: String) -> Self {
        Self::Failed(error)
    }
}

/// One sync's request bound. The loop holds the relay operation lock for a
/// whole sync, and minting waits on it, so no request may hang.
const SYNC_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) async fn sync_once(orch: &Orchestrator, relay_url: &str) -> Result<(), SyncError> {
    let _relay_operation = orch.relay.operation_lock().lock().await;
    let channel = channel::get_relay_channel(&orch.db.local).await?;
    let (Some((channel_id, secret)), Some(encrypted_private_key)) = (
        channel.credentials(),
        channel.private_key_encrypted.as_deref(),
    ) else {
        return Ok(());
    };
    let private_key =
        crypto::decrypt_private_key(encrypted_private_key, &crypto::get_machine_id())?;
    let app_slug = crate::github::credentials::get_github_credentials(&orch.db.local)
        .await?
        .app_slug;
    let client = super::relay_client().await?;
    let events_url = format!("{}/events/{channel_id}", relay_url.trim_end_matches('/'));
    let mut cursor = channel.last_event_sync.clone();
    let mut horizon_checked = false;

    loop {
        let requested_at = chrono::Utc::now().to_rfc3339();
        let mut request = client
            .get(&events_url)
            .timeout(SYNC_REQUEST_TIMEOUT)
            .bearer_auth(secret);
        if let Some(cursor) = cursor.as_deref() {
            let (since, after_id) = decode_cursor(cursor);
            request = request.query(&[("since", since)]);
            if let Some(after_id) = after_id {
                request = request.query(&[("after_id", after_id)]);
            }
        }
        let response = request
            .send()
            .await
            .map_err(|error| format!("relay event sync failed: {error}"))?;
        if matches!(
            response.status(),
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::NOT_FOUND
        ) {
            channel::mark_rejected(&orch.db.local).await?;
            return Err(SyncError::Rejected);
        }
        if !response.status().is_success() {
            return Err(format!("relay event sync returned {}", response.status()).into());
        }
        let response: RelayResponse = response
            .json()
            .await
            .map_err(|error| format!("invalid relay response: {error}"))?;

        if !horizon_checked {
            horizon_checked = true;
            if channel.health_state.as_deref() == Some("unauthorized") {
                channel::clear_rejection(&orch.db.local).await?;
            }
            if let Some(clamped) = response
                .retention_horizon
                .as_deref()
                .filter(|horizon| {
                    precedes_horizon(cursor.as_deref(), horizon)
                        && !drained_since(channel.drained_at.as_deref(), horizon)
                })
                .and_then(horizon_cursor)
            {
                // The horizon moves forward on every request, so an idle
                // channel's cursor falls behind it with nothing lost. A gap is
                // real only when no drain has happened since the horizon. A
                // first sync has nothing to lose.
                let stale = cursor
                    .as_deref()
                    .map(|cursor| decode_cursor(cursor).0.to_string());
                if let Some(stale) = &stale {
                    log::warn!("relay cursor {stale} is behind the retention horizon; syncing from {clamped}");
                }
                if let Some(stale) = stale {
                    let now = chrono::Utc::now().to_rfc3339();
                    channel::update_relay_channel(&orch.db.local, {
                        let clamped = clamped.clone();
                        move |channel| {
                            channel.last_event_sync = Some(clamped);
                            record_retention_gap(channel, &stale, &now);
                        }
                    })
                    .await?;
                }
                cursor = Some(clamped);
                continue;
            }
        }

        let ctx = PageContext {
            request_cursor: cursor.as_deref(),
            private_key: &private_key,
            key_rotated_at: channel.key_rotated_at.as_deref(),
            app_slug: app_slug.as_deref(),
        };
        if let Some(last) = apply_page(orch, response.events, &ctx).await? {
            cursor = Some(last);
        }

        if !response.has_more {
            // Nothing after the cursor existed when this page was asked for.
            channel::mark_drained(&orch.db.local, &requested_at).await?;
            return Ok(());
        }
    }
}

/// How long the loop waits after a sync, before the doorbell or a repair
/// cuts the wait short.
fn delay_after(
    result: &Result<(), SyncError>,
    consecutive_failures: u32,
    doorbell_connected: bool,
) -> Duration {
    match result {
        Ok(()) => poll_interval(doorbell_connected),
        Err(SyncError::Rejected) => MAX_FAILURE_BACKOFF,
        Err(SyncError::Failed(_)) => failure_backoff(consecutive_failures),
    }
}

/// Start the relay channel's sync loop and its doorbell.
pub fn spawn(orch: Orchestrator, relay_url: String) {
    let doorbell = Arc::new(Doorbell::default());
    super::ws::publish(doorbell.clone());
    tokio::spawn({
        let orch = orch.clone();
        async move { super::custody::restore(&orch.db.local).await }
    });
    tokio::spawn(crate::github::webhook::maintain(
        orch.clone(),
        relay_url.clone(),
    ));
    tokio::spawn(super::ws::maintain(
        orch.clone(),
        relay_url.clone(),
        doorbell.clone(),
    ));
    tokio::spawn(run(orch, relay_url, doorbell));
}

/// Sync, then wait for the timer, the doorbell, or a repair, forever.
pub(crate) async fn run(orch: Orchestrator, relay_url: String, doorbell: Arc<Doorbell>) {
    let relay = orch.relay.clone();
    let mut consecutive_failures = 0;
    let mut repair_generation = relay.repair_generation();
    loop {
        let result = sync_once(&orch, &relay_url).await;
        match &result {
            Ok(()) => consecutive_failures = 0,
            Err(SyncError::Rejected) => {
                consecutive_failures = 0;
                log::warn!(
                    "The relay rejected this install's channel; retrying in {} seconds",
                    MAX_FAILURE_BACKOFF.as_secs()
                );
            }
            Err(SyncError::Failed(error)) => {
                consecutive_failures += 1;
                log::warn!(
                    "Relay sync failed: {error}; retrying in {} seconds",
                    failure_backoff(consecutive_failures).as_secs()
                );
            }
        }
        let delay = delay_after(&result, consecutive_failures, doorbell.is_connected());
        if relay.repair_generation() != repair_generation {
            repair_generation = relay.repair_generation();
            consecutive_failures = 0;
            continue;
        }
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = doorbell.rung() => {}
            _ = relay.repair_notified() => {
                repair_generation = relay.repair_generation();
                consecutive_failures = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cursor_is_stable_with_equal_timestamps_and_accepts_legacy_values() {
        let first = encode_cursor("2026-08-10T20:00:00.000Z", 41);
        let second = encode_cursor("2026-08-10T20:00:00.000Z", 42);
        assert_ne!(first, second);
        assert_eq!(
            decode_cursor(&first),
            ("2026-08-10T20:00:00.000Z", Some(41))
        );
        assert_eq!(
            decode_cursor("2026-08-10T20:00:00.000Z"),
            ("2026-08-10T20:00:00.000Z", None)
        );
    }

    #[test]
    fn relay_skip_marker_carries_its_composite_cursor() {
        let marker: RelayEvent = serde_json::from_value(json!({
            "id": 67241,
            "event_type": "cairn.relay_skipped",
            "payload": { "error": "payload_exceeds_hard_ceiling" },
            "received_at": "2026-07-08T17:35:12.732Z",
            "encrypted": false
        }))
        .unwrap();
        assert_eq!(event_cursor(&marker), "2026-07-08T17:35:12.732Z|67241");
    }

    #[test]
    fn relay_response_controls_immediate_page_draining() {
        let more: RelayResponse = serde_json::from_value(json!({
            "events": [],
            "has_more": true
        }))
        .unwrap();
        let final_page: RelayResponse = serde_json::from_value(json!({"events": []})).unwrap();
        assert!(more.has_more);
        assert!(!final_page.has_more);
    }

    fn encrypted_event(id: i64) -> RelayEvent {
        RelayEvent {
            id,
            event_type: "issue_comment".into(),
            payload: json!("ciphertext"),
            received_at: "2026-08-12T21:30:00Z".into(),
            encrypted: true,
            ..Default::default()
        }
    }

    #[test]
    fn malformed_encrypted_payload_is_not_key_mismatch_evidence() {
        assert!(matches!(
            decrypt_event_payload(&json!({"ciphertext": "not-a-string"}), None, "unused"),
            Err(RelayPayloadError::Malformed(message))
                if message == "encrypted relay payload was not a string"
        ));
        assert!(matches!(
            decrypt_event_payload(&json!("not base64"), None, "unused"),
            Err(RelayPayloadError::Malformed(message)) if message.starts_with("Invalid base64:")
        ));
    }

    #[test]
    fn only_pre_rotation_mismatches_belong_to_a_dead_epoch() {
        let mut event = encrypted_event(73);
        event.received_at = "2026-08-12T21:30:00Z".into();
        assert!(is_dead_epoch_event(&event, Some("2026-08-12T21:31:00Z")));
        event.received_at = "2026-08-12T21:32:00Z".into();
        assert!(!is_dead_epoch_event(&event, Some("2026-08-12T21:31:00Z")));
        assert!(!is_dead_epoch_event(&event, None));
    }

    #[test]
    fn repeated_key_mismatch_promotes_suspect_and_preserves_first_failure() {
        let event = encrypted_event(73);
        let mut channel = RelayChannel {
            last_event_sync: Some("previous|72".into()),
            ..Default::default()
        };

        record_key_mismatch(&mut channel, &event, "2026-08-12T21:31:00Z");
        assert_eq!(channel.health_state.as_deref(), Some("suspect"));
        assert_eq!(channel.consecutive_failures, Some(1));
        assert_eq!(channel.last_event_sync.as_deref(), Some("previous|72"));

        record_key_mismatch(&mut channel, &event, "2026-08-12T21:32:00Z");
        assert_eq!(channel.health_state.as_deref(), Some("key_mismatch"));
        assert_eq!(channel.health_reason.as_deref(), Some("relay_key_mismatch"));
        assert_eq!(channel.consecutive_failures, Some(2));
        assert_eq!(
            channel.first_failure_at.as_deref(),
            Some("2026-08-12T21:31:00Z")
        );
        assert_eq!(
            channel.last_failure_at.as_deref(),
            Some("2026-08-12T21:32:00Z")
        );
        assert_eq!(channel.failing_event_id.as_deref(), Some("73"));
        assert_eq!(
            channel.failing_event_at.as_deref(),
            Some(event.received_at.as_str())
        );
        assert_eq!(channel.last_event_sync.as_deref(), Some("previous|72"));
    }

    #[test]
    fn successful_delivery_clears_evidence_and_finishes_recovery() {
        let mut channel = RelayChannel {
            health_state: Some("recovering".into()),
            health_reason: Some("relay_key_mismatch".into()),
            first_failure_at: Some("first".into()),
            last_failure_at: Some("last".into()),
            consecutive_failures: Some(4),
            failing_event_id: Some("73".into()),
            failing_event_at: Some("event-time".into()),
            ..Default::default()
        };

        record_successful_delivery(&mut channel, "2026-08-12T21:35:00Z");

        assert_eq!(channel.health_state.as_deref(), Some("healthy"));
        assert_eq!(channel.consecutive_failures, Some(0));
        assert_eq!(
            channel.last_successful_delivery_at.as_deref(),
            Some("2026-08-12T21:35:00Z")
        );
        assert!(channel.health_reason.is_none());
        assert!(channel.first_failure_at.is_none());
        assert!(channel.last_failure_at.is_none());
        assert!(channel.failing_event_id.is_none());
        assert!(channel.failing_event_at.is_none());
    }

    /// The install's relay key, a bugs endpoint mirrored for project `p`, and a
    /// way to seal events to the key the way the api does.
    struct SealedFeed {
        public_key: String,
        private_key: String,
    }

    impl SealedFeed {
        async fn new(db: &crate::storage::LocalDb) -> Self {
            crate::ingress::upsert_endpoint(
                db,
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
                    orphaned_at: None,
                },
            )
            .await
            .unwrap();
            let (public_key, private_key) = crypto::generate_keypair();
            Self {
                public_key,
                private_key,
            }
        }

        fn ctx(&self) -> PageContext<'_> {
            PageContext {
                request_cursor: None,
                private_key: &self.private_key,
                key_rotated_at: None,
                app_slug: Some("cairn-app"),
            }
        }

        /// A mention whose repository name can never be resolved, so
        /// `process_event` fails on every attempt.
        fn poison_github_event(&self, id: i64) -> RelayEvent {
            let payload = json!({
                "action": "created",
                "repository": {"full_name": "not-a-repository-name"},
                "issue": {"number": 42},
                "comment": {"id": 99, "body": "@cairn-app please look", "html_url": "https://github.test/c/1", "user": {"login": "alice", "type": "User"}}
            });
            RelayEvent {
                id,
                event_type: "issue_comment".into(),
                payload: json!(crypto::seal_payload_for_test(
                    &payload.to_string(),
                    &self.public_key
                )),
                received_at: format!("2026-10-18T20:00:{id:02}.000Z"),
                encrypted: true,
                ..Default::default()
            }
        }

        fn bug_event(&self, id: i64, endpoint_id: &str, report: i64) -> RelayEvent {
            RelayEvent {
                id,
                event_type: "bug.reported".into(),
                payload: json!(crypto::seal_payload_for_test(
                    &json!({"id": report, "title": "Wakes stopped"}).to_string(),
                    &self.public_key
                )),
                received_at: format!("2026-10-18T20:00:{id:02}.000Z"),
                encrypted: true,
                endpoint_id: Some(endpoint_id.into()),
                delivery_id: Some(format!("bug-report:{report}")),
                verification: Some("first-party".into()),
                sender: None,
                encryption: None,
            }
        }

        fn sealed_v2_bug_event(&self, id: i64, report: i64) -> RelayEvent {
            RelayEvent {
                payload: json!(crypto::seal_v2_for_test(
                    &json!({"id": report, "title": "Sealed v2"}).to_string(),
                    &self.public_key
                )),
                encryption: Some("sealed-v2".into()),
                ..self.bug_event(id, "src_bugs", report)
            }
        }
    }

    async fn serve_page(events: Vec<RelayEvent>) -> (String, tokio::task::JoinHandle<()>) {
        let events: Vec<Value> = events
            .into_iter()
            .map(|event| {
                json!({
                    "id": event.id, "event_type": event.event_type, "payload": event.payload,
                    "received_at": event.received_at, "encrypted": event.encrypted,
                    "endpoint_id": event.endpoint_id, "delivery_id": event.delivery_id,
                    "verification": event.verification, "encryption": event.encryption
                })
            })
            .collect();
        let app = axum::Router::new().route(
            "/events/{channel}",
            axum::routing::get(move || {
                let events = events.clone();
                async move { axum::Json(json!({"events": events, "has_more": false})) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, task)
    }

    async fn seed_sync_channel(db: &crate::storage::LocalDb, feed: &SealedFeed, cursor: String) {
        let encrypted =
            crypto::encrypt_private_key(&feed.private_key, &crypto::get_machine_id()).unwrap();
        channel::update_relay_channel(db, move |channel| {
            channel.channel_id = Some("ch-1".into());
            channel.secret = Some("secret".into());
            channel.private_key_encrypted = Some(encrypted);
            channel.last_event_sync = Some(cursor);
        })
        .await
        .unwrap();
    }

    #[test]
    fn cursor_order_compares_instants_then_ids() {
        let cursor = Some("2026-10-18T20:00:01.114Z|9");
        let mut event = RelayEvent {
            id: 9,
            received_at: "2026-10-18T13:00:01.114000-07:00".into(),
            ..Default::default()
        };
        assert!(!is_after_cursor(&event, cursor).unwrap());
        event.id = 8;
        assert!(!is_after_cursor(&event, cursor).unwrap());
        event.id = 10;
        assert!(is_after_cursor(&event, cursor).unwrap());
        assert!(!is_after_cursor(&event, Some("2026-10-18T20:00:01.114Z")).unwrap());
        event.received_at = "2026-10-18T20:00:01.115Z".into();
        event.id = 1;
        assert!(is_after_cursor(&event, cursor).unwrap());
        event.received_at = "2026-10-18T20:00:01.113Z".into();
        event.id = 99;
        assert!(!is_after_cursor(&event, cursor).unwrap());
    }

    #[tokio::test]
    async fn sync_drops_cursor_row_before_opening_and_writes_only_the_drain() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
        let feed = SealedFeed::new(&db).await;
        let mut replay = feed.bug_event(2, "src_bugs", 7);
        let cursor = event_cursor(&replay);
        // A replay must be discarded before decryption, not merely deduped by
        // the endpoint consumer: opening this body would pause the channel.
        replay.payload = json!("not a sealed body");
        seed_sync_channel(&db, &feed, cursor).await;
        let before = channel::get_relay_channel(&db).await.unwrap();
        let (url, server) = serve_page(vec![replay]).await;
        let writes = db.write_transaction_count();
        sync_once(&orch, &url).await.unwrap();
        assert_eq!(db.write_transaction_count(), writes + 1);
        let mut after = channel::get_relay_channel(&db).await.unwrap();
        assert!(after.drained_at.is_some());
        after.drained_at = before.drained_at.clone();
        assert_eq!(after, before);
        assert_eq!(count(&db, "SELECT COUNT(*) FROM ingress_events").await, 0);
        server.abort();
    }

    #[tokio::test]
    async fn an_accepted_events_page_clears_only_an_existing_rejection() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
        let feed = SealedFeed::new(&db).await;
        seed_sync_channel(&db, &feed, "2026-10-18T20:00:02.000Z|2".into()).await;
        channel::mark_rejected(&db).await.unwrap();
        let (url, server) = serve_page(vec![]).await;
        let writes = db.write_transaction_count();
        sync_once(&orch, &url).await.unwrap();
        assert_eq!(db.write_transaction_count(), writes + 2);
        assert_eq!(
            channel::get_relay_channel(&db)
                .await
                .unwrap()
                .health_state
                .as_deref(),
            Some("healthy")
        );
        let writes = db.write_transaction_count();
        sync_once(&orch, &url).await.unwrap();
        assert_eq!(db.write_transaction_count(), writes + 1);
        server.abort();
    }

    #[tokio::test]
    async fn sync_handles_only_the_new_row_after_a_replayed_cursor() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
        let feed = SealedFeed::new(&db).await;
        let mut replay = feed.bug_event(2, "src_bugs", 7);
        seed_sync_channel(&db, &feed, event_cursor(&replay)).await;
        replay.payload = json!("not a sealed body");
        let new = feed.bug_event(3, "src_bugs", 8);
        let expected = event_cursor(&new);
        let (url, server) = serve_page(vec![replay, new]).await;
        sync_once(&orch, &url).await.unwrap();
        assert_eq!(count(&db, "SELECT COUNT(*) FROM ingress_events").await, 1);
        assert_eq!(count(&db, "SELECT COUNT(*) FROM ingress_events WHERE delivery_id = 'bug-report:8' AND status = 'ok'").await, 1);
        assert_eq!(
            channel::get_relay_channel(&db)
                .await
                .unwrap()
                .last_event_sync,
            Some(expected)
        );
        server.abort();
    }

    #[tokio::test]
    async fn replayed_delivery_and_skip_do_not_change_health_evidence() {
        for endpoint in ["src_bugs", "src_unknown"] {
            let temp = tempfile::tempdir().unwrap();
            let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
            let feed = SealedFeed::new(&db).await;
            let event = feed.bug_event(2, endpoint, 7);
            let cursor = event_cursor(&event);
            apply_page(&orch, vec![event], &feed.ctx()).await.unwrap();
            seed_sync_channel(&db, &feed, cursor.clone()).await;
            // Use a sentinel to prove a replay cannot restamp successful
            // delivery or clear failure evidence that arrived since it.
            channel::update_relay_channel(&db, |channel| {
                channel.last_successful_delivery_at = Some("2026-10-18T19:00:00Z".into());
                if endpoint == "src_bugs" {
                    channel.health_state = Some("suspect".into());
                    channel.health_reason = Some("relay_key_mismatch".into());
                    channel.consecutive_failures = Some(1);
                    channel.last_failure_at = Some("2026-10-18T21:00:00Z".into());
                }
            })
            .await
            .unwrap();
            let before = channel::get_relay_channel(&db).await.unwrap();
            let ctx = PageContext {
                request_cursor: Some(&cursor),
                ..feed.ctx()
            };
            let writes = db.write_transaction_count();
            assert_eq!(
                apply_page(&orch, vec![feed.bug_event(2, endpoint, 7)], &ctx)
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(db.write_transaction_count(), writes);
            assert_eq!(channel::get_relay_channel(&db).await.unwrap(), before);
        }
    }

    /// A sealed-v2 endpoint event opens with the same install key, becomes an
    /// `ingress` fact, and moves the cursor; the channel is not paused.
    #[tokio::test]
    async fn a_sealed_v2_endpoint_event_is_delivered() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
        let feed = SealedFeed::new(&db).await;
        let event = feed.sealed_v2_bug_event(8, 11);
        let event_cursor = event_cursor(&event);
        let cursor = apply_page(&orch, vec![event], &feed.ctx()).await.unwrap();
        assert_eq!(cursor.as_deref(), Some(event_cursor.as_str()));
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM ingress_events WHERE status = 'ok' AND delivery_id = 'bug-report:11'").await,
            1
        );
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM fact_log WHERE source = 'ingress'"
            )
            .await,
            1
        );
        let channel = channel::get_relay_channel(&db).await.unwrap();
        assert_eq!(channel.health_state.as_deref(), Some("healthy"));
        assert_eq!(
            channel.last_event_sync.as_deref(),
            Some(event_cursor.as_str())
        );
    }

    /// A sealed-v2 body sealed to another key is a key mismatch with v1's
    /// semantics: it pauses the channel.
    #[tokio::test]
    async fn a_sealed_v2_body_for_another_key_pauses_like_v1() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
        let feed = SealedFeed::new(&db).await;
        let (other_public, _) = crypto::generate_keypair();
        let mut foreign = feed.sealed_v2_bug_event(9, 12);
        foreign.payload = json!(crypto::seal_v2_for_test("{}", &other_public));
        let error = apply_page(&orch, vec![foreign], &feed.ctx())
            .await
            .unwrap_err();
        assert!(error.contains("key mismatch"), "{error}");
        let channel = channel::get_relay_channel(&db).await.unwrap();
        assert_eq!(channel.last_event_sync, None);
        assert_eq!(channel.health_state.as_deref(), Some("suspect"));
    }

    /// An endpoint event in a format this install cannot open is recorded
    /// undeliverable and passed; it does not pause the channel.
    #[tokio::test]
    async fn an_unknown_encryption_is_undeliverable_not_a_pause() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
        let feed = SealedFeed::new(&db).await;
        let mut event = feed.bug_event(10, "src_bugs", 13);
        event.encryption = Some("sealed-v9".into());
        let event_cursor = event_cursor(&event);
        let cursor = apply_page(&orch, vec![event], &feed.ctx()).await.unwrap();
        assert_eq!(cursor.as_deref(), Some(event_cursor.as_str()));
        let channel = channel::get_relay_channel(&db).await.unwrap();
        assert_eq!(channel.health_state.as_deref(), Some("skipping"));
        assert_eq!(
            channel.health_reason.as_deref(),
            Some("ingress_unknown_encryption")
        );
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM ingress_events WHERE status = 'undeliverable'"
            )
            .await,
            1
        );
    }

    #[test]
    fn an_older_api_without_an_encryption_column_means_sealed_v1() {
        let event: RelayEvent = serde_json::from_value(json!({
            "id": 1, "event_type": "push", "payload": "x", "received_at": "t", "encrypted": true
        }))
        .unwrap();
        assert_eq!(event.encryption, None);
        let response: RelayResponse = serde_json::from_value(json!({"events": []})).unwrap();
        assert_eq!(response.retention_horizon, None);
    }

    #[test]
    fn the_horizon_clamps_a_missing_or_older_cursor_only() {
        let horizon = "2026-10-01T00:00:00.000Z";
        assert!(precedes_horizon(None, horizon));
        assert!(precedes_horizon(
            Some("2026-09-30T23:59:59.000Z|4"),
            horizon
        ));
        assert!(!precedes_horizon(
            Some("2026-10-01T00:00:00.000Z|4"),
            horizon
        ));
        assert!(!precedes_horizon(
            Some("2026-10-02T00:00:00.000Z|4"),
            horizon
        ));
        assert!(!precedes_horizon(
            Some("2026-09-30T00:00:00.000Z"),
            "not a time"
        ));
        assert_eq!(
            horizon_cursor(horizon).as_deref(),
            Some("2026-09-30T23:59:59.999Z")
        );
    }

    #[test]
    fn the_wait_after_a_sync_follows_the_doorbell_and_a_rejection() {
        assert_eq!(delay_after(&Ok(()), 0, true), Duration::from_secs(60));
        assert_eq!(delay_after(&Ok(()), 0, false), Duration::from_secs(15));
        assert_eq!(
            delay_after(&Err(SyncError::Rejected), 0, true),
            MAX_FAILURE_BACKOFF
        );
        assert_eq!(
            delay_after(&Err(SyncError::Failed("x".into())), 2, true),
            Duration::from_secs(30)
        );
    }

    async fn count(db: &crate::storage::LocalDb, sql: &str) -> i64 {
        db.query_opt_i64(sql, ()).await.unwrap().unwrap_or(0)
    }

    /// A GitHub event that fails at `process_event` on every attempt is
    /// recorded and passed, and a bug event queued behind it on the shared
    /// cursor is still delivered and minted.
    #[tokio::test]
    async fn a_persistently_failing_github_event_does_not_park_the_bug_event_behind_it() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
        let feed = SealedFeed::new(&db).await;
        let poison = feed.poison_github_event(1);
        let poison_cursor = event_cursor(&poison);

        // Delivered alone first, the way a page boundary could leave it.
        let cursor = apply_page(&orch, vec![poison], &feed.ctx()).await.unwrap();
        assert_eq!(cursor.as_deref(), Some(poison_cursor.as_str()));
        let channel = channel::get_relay_channel(&db).await.unwrap();
        assert_eq!(
            channel.last_event_sync.as_deref(),
            Some(poison_cursor.as_str())
        );
        assert_eq!(channel.health_state.as_deref(), Some("skipping"));
        assert_eq!(
            channel.health_reason.as_deref(),
            Some("relay_event_processing_failed")
        );
        assert_eq!(channel.failing_event_id.as_deref(), Some("1"));
        assert_eq!(
            channel.failing_event_at.as_deref(),
            Some("2026-10-18T20:00:01.000Z")
        );
        assert_eq!(channel.undeliverable_since_healthy, 1);
        assert!(channel.first_failure_at.is_some());
        // Skipping is not a key failure: the channel's failure streak is untouched.
        assert_eq!(channel.consecutive_failures, None);

        // The same poison again, then a bug report, on one page.
        let bug = feed.bug_event(3, "src_bugs", 7);
        let bug_cursor = event_cursor(&bug);
        let cursor = apply_page(&orch, vec![feed.poison_github_event(2), bug], &feed.ctx())
            .await
            .unwrap();
        assert_eq!(cursor.as_deref(), Some(bug_cursor.as_str()));
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM ingress_events WHERE status = 'ok' AND delivery_id = 'bug-report:7'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM fact_log WHERE source = 'ingress'"
            )
            .await,
            1
        );
        let channel = channel::get_relay_channel(&db).await.unwrap();
        assert_eq!(
            channel.last_event_sync.as_deref(),
            Some(bug_cursor.as_str())
        );
        assert_eq!(channel.health_state.as_deref(), Some("healthy"));
        assert_eq!(channel.undeliverable_since_healthy, 0);
        assert!(channel.failing_event_id.is_none());
        assert!(channel.last_successful_delivery_at.is_some());
    }

    /// The same poison through the GitHub App's endpoint is an `undeliverable`
    /// row that keeps its body, and the bug event behind it is still
    /// delivered.
    #[tokio::test]
    async fn a_failing_github_app_endpoint_event_is_undeliverable_and_does_not_park_the_feed() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
        let feed = SealedFeed::new(&db).await;
        crate::ingress::consume::tests::github_app_endpoint(&db).await;
        let through_endpoint = |id: i64| RelayEvent {
            endpoint_id: Some("src_app".into()),
            delivery_id: Some(format!("gh-{id}")),
            verification: Some("github".into()),
            ..feed.poison_github_event(id)
        };

        apply_page(&orch, vec![through_endpoint(1)], &feed.ctx())
            .await
            .unwrap();
        let channel = channel::get_relay_channel(&db).await.unwrap();
        assert_eq!(channel.health_state.as_deref(), Some("skipping"));
        assert_eq!(
            channel.health_reason.as_deref(),
            Some("relay_event_processing_failed")
        );

        let bug = feed.bug_event(3, "src_bugs", 7);
        let bug_cursor = event_cursor(&bug);
        let cursor = apply_page(&orch, vec![through_endpoint(2), bug], &feed.ctx())
            .await
            .unwrap();
        assert_eq!(cursor.as_deref(), Some(bug_cursor.as_str()));
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM ingress_events WHERE endpoint_id = 'src_app' AND status = 'undeliverable' AND body LIKE '%not-a-repository-name%'"
            )
            .await,
            2
        );
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM ingress_events WHERE status = 'ok' AND delivery_id = 'bug-report:7'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM fact_log WHERE source = 'ingress'"
            )
            .await,
            1
        );
        let channel = channel::get_relay_channel(&db).await.unwrap();
        assert_eq!(channel.health_state.as_deref(), Some("healthy"));
    }

    /// An endpoint event this install cannot deliver is recorded undeliverable,
    /// counted, and passed.
    #[tokio::test]
    async fn an_undeliverable_ingress_event_is_recorded_and_the_cursor_advances() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
        let feed = SealedFeed::new(&db).await;
        let stray = feed.bug_event(4, "src_unknown", 8);
        let stray_cursor = event_cursor(&stray);

        let cursor = apply_page(&orch, vec![stray], &feed.ctx()).await.unwrap();

        assert_eq!(cursor.as_deref(), Some(stray_cursor.as_str()));
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM ingress_events WHERE status = 'undeliverable' AND endpoint_id = 'src_unknown'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM fact_log WHERE source = 'ingress'"
            )
            .await,
            0
        );
        let channel = channel::get_relay_channel(&db).await.unwrap();
        assert_eq!(
            channel.last_event_sync.as_deref(),
            Some(stray_cursor.as_str())
        );
        assert_eq!(channel.health_state.as_deref(), Some("skipping"));
        assert_eq!(
            channel.health_reason.as_deref(),
            Some("ingress_unknown_endpoint")
        );
        assert_eq!(channel.undeliverable_since_healthy, 1);
    }

    /// A key mismatch still pauses the whole channel: every later event is
    /// unreadable too, so the cursor must not move past it.
    #[tokio::test]
    async fn a_key_mismatch_still_pauses_the_cursor() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
        let feed = SealedFeed::new(&db).await;
        let (other_public, _) = crypto::generate_keypair();
        let mut foreign = feed.bug_event(5, "src_bugs", 9);
        foreign.payload = json!(crypto::seal_payload_for_test("{}", &other_public));

        let error = apply_page(
            &orch,
            vec![foreign, feed.bug_event(6, "src_bugs", 10)],
            &feed.ctx(),
        )
        .await
        .unwrap_err();

        assert!(error.contains("key mismatch"), "{error}");
        let channel = channel::get_relay_channel(&db).await.unwrap();
        assert_eq!(channel.last_event_sync, None);
        assert_eq!(channel.health_state.as_deref(), Some("suspect"));
        assert_eq!(count(&db, "SELECT COUNT(*) FROM ingress_events").await, 0);
    }

    /// With no GitHub App connected, a GitHub event is recorded as a skip, not
    /// dropped silently, and the feed keeps moving.
    #[tokio::test]
    async fn a_github_event_without_a_connected_app_is_a_visible_skip() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
        let feed = SealedFeed::new(&db).await;
        let ctx = PageContext {
            app_slug: None,
            ..feed.ctx()
        };
        let event = feed.poison_github_event(7);
        let cursor = apply_page(&orch, vec![event], &ctx).await.unwrap();
        assert_eq!(cursor.as_deref(), Some("2026-10-18T20:00:07.000Z|7"));
        let channel = channel::get_relay_channel(&db).await.unwrap();
        assert_eq!(channel.health_state.as_deref(), Some("skipping"));
        assert_eq!(
            channel.health_reason.as_deref(),
            Some("github_not_connected")
        );
    }

    #[test]
    fn failure_backoff_grows_exponentially_and_is_capped() {
        assert_eq!(failure_backoff(1), Duration::from_secs(15));
        assert_eq!(failure_backoff(2), Duration::from_secs(30));
        assert_eq!(failure_backoff(3), Duration::from_secs(60));
        assert_eq!(failure_backoff(7), MAX_FAILURE_BACKOFF);
        assert_eq!(failure_backoff(100), MAX_FAILURE_BACKOFF);
    }
}
