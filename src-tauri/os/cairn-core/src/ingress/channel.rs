//! The install's relay channel: the one feed every ingress endpoint and the
//! GitHub App's webhooks arrive on.
//!
//! The channel belongs to the install, not to any integration. It is
//! registered on the api the first time something needs it ([`ensure`]), and it
//! carries the shared secret the install authenticates with, the X25519 keypair
//! events are sealed to, the shared cursor, and the feed's health evidence.
//!
//! `secret` is encrypted at rest under the machine-derived key
//! ([`crypto::RELAY_SECRET_DOMAIN`]): [`get_relay_channel`] returns it
//! decrypted and [`update_relay_channel`] re-encrypts it on write. The private
//! key is stored as the relay key ciphertext ([`crypto::encrypt_private_key`])
//! and handled as-is here.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use cairn_db::turso::params;
use serde::Deserialize;
use tokio::sync::{Mutex, Notify};

use crate::github::crypto;
use crate::storage::{LocalDb, RowExt};

/// Registration and key repair run under the relay operation lock, which
/// minting and every sync wait on, so neither request may hang.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How one install's relay work coordinates: the lock its polling,
/// registration and key rotation share, and the signals its one sync loop and
/// one doorbell wait on. The orchestrator holds the install's
/// ([`crate::orchestrator::Orchestrator::relay`]); everything that touches the
/// channel row is handed it.
#[derive(Debug, Default)]
pub struct RelaySignals {
    operation: Mutex<()>,
    repaired: Notify,
    repair_generation: AtomicU64,
    channel_changed: Notify,
}

impl RelaySignals {
    /// Serializes relay polling, channel registration, and key rotation.
    pub fn operation_lock(&self) -> &Mutex<()> {
        &self.operation
    }

    pub fn repair_generation(&self) -> u64 {
        self.repair_generation.load(Ordering::Acquire)
    }

    pub async fn repair_notified(&self) {
        self.repaired.notified().await;
    }

    pub fn notify_repaired(&self) {
        self.repair_generation.fetch_add(1, Ordering::AcqRel);
        // There is one relay poller. `notify_one` retains a permit when repair
        // lands between its generation check and registration of the next waiter.
        self.repaired.notify_one();
    }

    /// Resolves when the channel's identity (its id or secret) may have
    /// changed: registered, repaired, or deleted. The doorbell is the one waiter.
    pub async fn channel_changed(&self) {
        self.channel_changed.notified().await;
    }

    pub fn notify_channel_changed(&self) {
        self.channel_changed.notify_one();
    }
}

/// The install's relay channel row. Every field is `None` (and the count zero)
/// when no channel is registered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelayChannel {
    pub channel_id: Option<String>,
    pub secret: Option<String>,
    pub public_key: Option<String>,
    pub private_key_encrypted: Option<String>,
    pub key_rotated_at: Option<String>,
    pub pending_public_key: Option<String>,
    pub pending_private_key_encrypted: Option<String>,
    /// The shared cursor, `received_at|id`.
    pub last_event_sync: Option<String>,
    /// When a sync last drained the channel completely (RFC 3339).
    pub drained_at: Option<String>,
    pub health_state: Option<String>,
    pub health_reason: Option<String>,
    pub first_failure_at: Option<String>,
    pub last_failure_at: Option<String>,
    pub consecutive_failures: Option<i64>,
    pub failing_event_id: Option<String>,
    pub failing_event_at: Option<String>,
    pub last_successful_delivery_at: Option<String>,
    /// Events the consumer recorded and skipped since its last successful
    /// delivery. Non-zero means the feed is `skipping`.
    pub undeliverable_since_healthy: i64,
}

impl RelayChannel {
    /// The channel id and secret the install authenticates with.
    pub fn credentials(&self) -> Option<(&str, &str)> {
        Some((self.channel_id.as_deref()?, self.secret.as_deref()?))
    }

    /// The private key, when the stored keypair opens on this machine and its
    /// public half matches the stored public key.
    pub fn intact_private_key(&self) -> Option<String> {
        open_keypair(
            self.public_key.as_deref()?,
            self.private_key_encrypted.as_deref()?,
        )
    }
}

fn open_keypair(public_key: &str, private_key_encrypted: &str) -> Option<String> {
    let private =
        crypto::decrypt_private_key(private_key_encrypted, &crypto::get_machine_id()).ok()?;
    (crypto::public_key_for_private(&private).ok()? == public_key).then_some(private)
}

const COLUMNS: &str = "channel_id, secret, public_key, private_key_encrypted, key_rotated_at,
     pending_public_key, pending_private_key_encrypted, last_event_sync, health_state,
     health_reason, first_failure_at, last_failure_at, consecutive_failures,
     failing_event_id, failing_event_at, last_successful_delivery_at,
     undeliverable_since_healthy, drained_at";

pub async fn get_relay_channel(db: &LocalDb) -> Result<RelayChannel, String> {
    let channel = db
        .query_opt(
            format!("SELECT {COLUMNS} FROM relay_channel WHERE id = 'default'"),
            (),
            |row| {
                Ok(RelayChannel {
                    channel_id: row.opt_text(0)?,
                    secret: row.opt_text(1)?,
                    public_key: row.opt_text(2)?,
                    private_key_encrypted: row.opt_text(3)?,
                    key_rotated_at: row.opt_text(4)?,
                    pending_public_key: row.opt_text(5)?,
                    pending_private_key_encrypted: row.opt_text(6)?,
                    last_event_sync: row.opt_text(7)?,
                    health_state: row.opt_text(8)?,
                    health_reason: row.opt_text(9)?,
                    first_failure_at: row.opt_text(10)?,
                    last_failure_at: row.opt_text(11)?,
                    consecutive_failures: row.opt_i64(12)?,
                    failing_event_id: row.opt_text(13)?,
                    failing_event_at: row.opt_text(14)?,
                    last_successful_delivery_at: row.opt_text(15)?,
                    undeliverable_since_healthy: row.opt_i64(16)?.unwrap_or(0),
                    drained_at: row.opt_text(17)?,
                })
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut channel = channel.unwrap_or_default();
    // A secret in our ciphertext format that does not open here (another
    // machine, or tampering) is unusable: report it as absent rather than
    // handing out ciphertext.
    channel.secret = channel.secret.take().and_then(|value| {
        match crypto::decrypt_at_rest(
            &value,
            &crypto::get_machine_id(),
            crypto::RELAY_SECRET_DOMAIN,
        ) {
            Ok(plaintext) => Some(plaintext),
            Err(error) => {
                log::warn!("Failed to decrypt relay_channel.secret: {error}");
                None
            }
        }
    });
    Ok(channel)
}

/// Read-modify-write the channel row, re-encrypting `secret`.
pub async fn update_relay_channel<F>(db: &LocalDb, update_fn: F) -> Result<(), String>
where
    F: FnOnce(&mut RelayChannel),
{
    let mut channel = get_relay_channel(db).await?;
    update_fn(&mut channel);
    let secret = channel
        .secret
        .as_deref()
        .map(|secret| {
            crypto::encrypt_at_rest(
                secret,
                &crypto::get_machine_id(),
                crypto::RELAY_SECRET_DOMAIN,
            )
        })
        .transpose()?;
    let now = chrono::Utc::now().timestamp();
    db.execute(
        format!(
            "INSERT INTO relay_channel (id, {COLUMNS}, created_at, updated_at)
             VALUES ('default', ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                     ?15, ?16, ?17, ?19, ?18, ?18)
             ON CONFLICT(id) DO UPDATE SET
                channel_id = excluded.channel_id,
                secret = excluded.secret,
                public_key = excluded.public_key,
                private_key_encrypted = excluded.private_key_encrypted,
                key_rotated_at = excluded.key_rotated_at,
                pending_public_key = excluded.pending_public_key,
                pending_private_key_encrypted = excluded.pending_private_key_encrypted,
                last_event_sync = excluded.last_event_sync,
                health_state = excluded.health_state,
                health_reason = excluded.health_reason,
                first_failure_at = excluded.first_failure_at,
                last_failure_at = excluded.last_failure_at,
                consecutive_failures = excluded.consecutive_failures,
                failing_event_id = excluded.failing_event_id,
                failing_event_at = excluded.failing_event_at,
                last_successful_delivery_at = excluded.last_successful_delivery_at,
                undeliverable_since_healthy = excluded.undeliverable_since_healthy,
                drained_at = excluded.drained_at,
                updated_at = excluded.updated_at"
        ),
        params![
            channel.channel_id,
            secret,
            channel.public_key,
            channel.private_key_encrypted,
            channel.key_rotated_at,
            channel.pending_public_key,
            channel.pending_private_key_encrypted,
            channel.last_event_sync,
            channel.health_state,
            channel.health_reason,
            channel.first_failure_at,
            channel.last_failure_at,
            channel.consecutive_failures,
            channel.failing_event_id,
            channel.failing_event_at,
            channel.last_successful_delivery_at,
            channel.undeliverable_since_healthy,
            now,
            channel.drained_at,
        ],
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

/// Record that the api refused the channel's secret (401) or no longer knows
/// the channel (404). Repair cannot fix either, and registering a new channel
/// would orphan every endpoint on this one, so the state stays until the api
/// accepts the channel again.
///
/// One statement, so the doorbell can record it while the sync loop rewrites
/// the row without either losing the other's cursor.
pub async fn mark_rejected(db: &LocalDb) -> Result<(), String> {
    let now = chrono::Utc::now().to_rfc3339();
    db.execute(
        "UPDATE relay_channel
            SET health_state = 'unauthorized', health_reason = 'relay_channel_rejected',
                first_failure_at = COALESCE(first_failure_at, ?1), last_failure_at = ?1
          WHERE id = 'default'",
        params![now],
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

/// Record that a sync drained the channel as of `at`. One statement, like
/// [`mark_rejected`].
pub async fn mark_drained(db: &LocalDb, at: &str) -> Result<(), String> {
    db.execute(
        "UPDATE relay_channel SET drained_at = ?1 WHERE id = 'default'",
        params![at.to_string()],
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

/// The api accepted the channel again: clear a rejection, and nothing else.
pub async fn clear_rejection(db: &LocalDb) -> Result<(), String> {
    db.execute(
        "UPDATE relay_channel
            SET health_state = 'healthy', health_reason = NULL,
                first_failure_at = NULL, last_failure_at = NULL
          WHERE id = 'default' AND health_state = 'unauthorized'",
        (),
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

/// Forget the channel. Its endpoints on the api are not touched; callers
/// revoke what should stop first.
pub async fn delete_relay_channel(db: &LocalDb, signals: &RelaySignals) -> Result<(), String> {
    db.execute("DELETE FROM relay_channel WHERE id = 'default'", ())
        .await
        .map_err(|error| error.to_string())?;
    signals.notify_channel_changed();
    Ok(())
}

/// The channel's delivery health as the GitHub card and the Event endpoints
/// section show it.
pub fn relay_channel_status(channel: &RelayChannel) -> crate::models::EventDeliveryStatus {
    let channel = channel.clone();
    crate::models::EventDeliveryStatus {
        configured: channel.channel_id.is_some()
            && channel.public_key.is_some()
            && channel.private_key_encrypted.is_some(),
        last_event_sync: channel.last_event_sync,
        health_state: channel
            .health_state
            .unwrap_or_else(|| "healthy".to_string()),
        health_reason: channel.health_reason,
        first_failure_at: channel.first_failure_at,
        last_failure_at: channel.last_failure_at,
        consecutive_failures: channel.consecutive_failures.unwrap_or(0),
        failing_event_id: channel.failing_event_id,
        failing_event_at: channel.failing_event_at,
        last_successful_delivery_at: channel.last_successful_delivery_at,
        undeliverable_since_healthy: channel.undeliverable_since_healthy,
    }
}

/// The install's channel, registering one when there is none.
///
/// Idempotent. An intact row is returned as it is. A row with an id and secret
/// but a broken keypair is repaired in place, keeping the channel: a new
/// channel would orphan every endpoint minted on the old one. Only an install
/// with no usable channel registers.
pub async fn ensure(
    db: &LocalDb,
    signals: &RelaySignals,
    relay_url: &str,
) -> Result<RelayChannel, String> {
    // The common case reads and returns: the lock is the sync loop's for a
    // whole sync, and an intact channel needs nothing from it.
    let channel = get_relay_channel(db).await?;
    if channel.credentials().is_some() && channel.intact_private_key().is_some() {
        return Ok(channel);
    }
    let _relay_operation = signals.operation_lock().lock().await;
    let channel = get_relay_channel(db).await?;
    if channel.credentials().is_some() {
        if channel.intact_private_key().is_some() {
            return Ok(channel);
        }
        repair_locked(db, signals, relay_url).await?;
        return get_relay_channel(db).await;
    }
    let registered = register(relay_url).await?;
    let fresh = RelayChannel {
        channel_id: Some(registered.channel_id),
        secret: Some(registered.secret),
        public_key: Some(registered.public_key),
        private_key_encrypted: Some(registered.private_key_encrypted),
        key_rotated_at: Some(chrono::Utc::now().to_rfc3339()),
        ..RelayChannel::default()
    };
    update_relay_channel(db, {
        let fresh = fresh.clone();
        move |channel| *channel = fresh
    })
    .await?;
    signals.notify_channel_changed();
    get_relay_channel(db).await
}

struct Registration {
    channel_id: String,
    secret: String,
    public_key: String,
    private_key_encrypted: String,
}

async fn register(relay_url: &str) -> Result<Registration, String> {
    let (public_key, private_key) = crypto::generate_keypair();
    let private_key_encrypted =
        crypto::encrypt_private_key(&private_key, &crypto::get_machine_id())?;
    let response = super::relay_client()
        .await?
        .post(format!("{}/channels", relay_url.trim_end_matches('/')))
        .timeout(REQUEST_TIMEOUT)
        .json(&serde_json::json!({ "publicKey": public_key }))
        .send()
        .await
        .map_err(|error| format!("Failed to register relay channel: {error}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("Relay error: {status} - {body}"));
    }
    #[derive(Deserialize)]
    struct ChannelResponse {
        channel_id: String,
        secret: String,
    }
    let channel: ChannelResponse = response
        .json()
        .await
        .map_err(|error| format!("Failed to parse relay channel response: {error}"))?;
    Ok(Registration {
        channel_id: channel.channel_id,
        secret: channel.secret,
        public_key,
        private_key_encrypted,
    })
}

#[derive(Deserialize)]
struct KeyRotationResponse {
    rotation_cursor: Option<RotationCursor>,
}

#[derive(Deserialize)]
struct RotationCursor {
    received_at: String,
    id: i64,
}

/// Re-register the channel's public key with the api and clear the failure
/// evidence, keeping the channel.
///
/// A self-consistent keypair is kept and re-sent. A missing or broken one is
/// replaced, reusing a pending key left by a promotion that failed after the
/// api accepted it. Only a rotation moves the cursor: the api answers with its
/// newest event as `rotation_cursor` whether or not the key changed, and the
/// events before it are sealed to the old key only when there was a new one.
pub async fn repair(db: &LocalDb, signals: &RelaySignals, relay_url: &str) -> Result<(), String> {
    let _relay_operation = signals.operation_lock().lock().await;
    repair_locked(db, signals, relay_url).await
}

async fn repair_locked(
    db: &LocalDb,
    signals: &RelaySignals,
    relay_url: &str,
) -> Result<(), String> {
    let mut channel = get_relay_channel(db).await?;
    let channel_id = channel
        .channel_id
        .clone()
        .ok_or_else(|| "The relay channel is not configured".to_string())?;
    let secret = channel
        .secret
        .clone()
        .ok_or_else(|| "The relay channel secret is not configured".to_string())?;

    let current_keypair = channel.intact_private_key().and(
        channel
            .public_key
            .clone()
            .zip(channel.private_key_encrypted.clone()),
    );
    let rotated = current_keypair.is_none();
    let (public_key, private_key_encrypted) = match current_keypair {
        Some(keypair) => keypair,
        None => match (
            channel.pending_public_key.take(),
            channel.pending_private_key_encrypted.take(),
        ) {
            (Some(public_key), Some(encrypted))
                if open_keypair(&public_key, &encrypted).is_some() =>
            {
                (public_key, encrypted)
            }
            _ => {
                let (public_key, private_key) = crypto::generate_keypair();
                let encrypted =
                    crypto::encrypt_private_key(&private_key, &crypto::get_machine_id())?;
                update_relay_channel(db, {
                    let public_key = public_key.clone();
                    let encrypted = encrypted.clone();
                    move |channel| {
                        channel.pending_public_key = Some(public_key);
                        channel.pending_private_key_encrypted = Some(encrypted);
                    }
                })
                .await?;
                (public_key, encrypted)
            }
        },
    };
    let response = super::relay_client()
        .await?
        .put(format!(
            "{}/channels/{channel_id}/public-key",
            relay_url.trim_end_matches('/')
        ))
        .timeout(REQUEST_TIMEOUT)
        .bearer_auth(secret)
        .json(&serde_json::json!({ "publicKey": public_key }))
        .send()
        .await
        .map_err(|error| format!("Failed to rotate relay key: {error}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("Relay error: {status} - {body}"));
    }
    let response: KeyRotationResponse = response
        .json()
        .await
        .map_err(|error| format!("Failed to parse relay key rotation response: {error}"))?;
    let rotation_cursor = response
        .rotation_cursor
        .map(|cursor| format!("{}|{}", cursor.received_at, cursor.id));

    let rotated_at = rotated.then(|| chrono::Utc::now().to_rfc3339());
    update_relay_channel(db, move |channel| {
        channel.public_key = Some(public_key);
        channel.private_key_encrypted = Some(private_key_encrypted);
        channel.pending_public_key = None;
        channel.pending_private_key_encrypted = None;
        if let Some(rotated_at) = rotated_at {
            channel.key_rotated_at = Some(rotated_at);
            channel.last_event_sync = rotation_cursor;
        }
        channel.health_state = Some("recovering".to_string());
        channel.health_reason = None;
        channel.first_failure_at = None;
        channel.last_failure_at = None;
        channel.consecutive_failures = Some(0);
        channel.failing_event_id = None;
        channel.failing_event_at = None;
    })
    .await?;
    signals.notify_repaired();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::migrated_test_db;

    #[tokio::test]
    async fn repair_notification_is_retained_for_a_late_poller_waiter() {
        let signals = RelaySignals::default();
        signals.notify_repaired();
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            signals.repair_notified(),
        )
        .await
        .expect("repair notification should retain a permit");
    }

    #[tokio::test]
    async fn the_secret_is_ciphertext_at_rest_and_plaintext_on_read() {
        let db = migrated_test_db("relay-channel-secret.db").await;
        update_relay_channel(&db, |channel| {
            channel.channel_id = Some("ch-1".into());
            channel.secret = Some("relay-shared-secret".into());
        })
        .await
        .unwrap();
        let stored = db
            .query_opt_text("SELECT secret FROM relay_channel WHERE id = 'default'", ())
            .await
            .unwrap()
            .unwrap();
        assert_ne!(stored, "relay-shared-secret");
        let channel = get_relay_channel(&db).await.unwrap();
        assert_eq!(channel.secret.as_deref(), Some("relay-shared-secret"));
        assert_eq!(channel.credentials(), Some(("ch-1", "relay-shared-secret")));

        delete_relay_channel(&db, &RelaySignals::default())
            .await
            .unwrap();
        assert_eq!(
            get_relay_channel(&db).await.unwrap(),
            RelayChannel::default()
        );
    }

    /// A stand-in api answering channel registration and key rotation.
    async fn relay() -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        use axum::{http::Method, http::Uri, Router};
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let record = seen.clone();
        let app = Router::new().fallback(move |method: Method, uri: Uri| {
            let record = record.clone();
            async move {
                record
                    .lock()
                    .unwrap()
                    .push(format!("{method} {}", uri.path()));
                if uri.path().ends_with("/public-key") {
                    axum::Json(serde_json::json!({ "rotation_cursor": null }))
                } else {
                    axum::Json(
                        serde_json::json!({ "channel_id": "ch-new", "secret": "new-secret" }),
                    )
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/relay", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, seen)
    }

    #[tokio::test]
    async fn ensure_registers_once_and_then_returns_the_same_channel() {
        let db = migrated_test_db("relay-channel-ensure.db").await;
        let (url, seen) = relay().await;
        let signals = RelaySignals::default();
        let first = ensure(&db, &signals, &url).await.unwrap();
        assert_eq!(first.channel_id.as_deref(), Some("ch-new"));
        assert_eq!(first.secret.as_deref(), Some("new-secret"));
        assert!(first.intact_private_key().is_some());
        assert!(first.key_rotated_at.is_some());
        let second = ensure(&db, &signals, &url).await.unwrap();
        assert_eq!(second, first);
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["POST /relay/channels".to_string()]
        );
    }

    #[tokio::test]
    async fn ensure_repairs_a_broken_keypair_in_place_instead_of_registering() {
        let db = migrated_test_db("relay-channel-ensure-repair.db").await;
        update_relay_channel(&db, |channel| {
            channel.channel_id = Some("ch-old".into());
            channel.secret = Some("old-secret".into());
            channel.public_key = Some("stale".into());
            channel.last_event_sync = Some("t|5".into());
        })
        .await
        .unwrap();
        let (url, seen) = relay().await;
        let channel = ensure(&db, &RelaySignals::default(), &url).await.unwrap();
        assert_eq!(channel.channel_id.as_deref(), Some("ch-old"));
        assert_eq!(channel.secret.as_deref(), Some("old-secret"));
        assert!(channel.intact_private_key().is_some());
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["PUT /relay/channels/ch-old/public-key".to_string()]
        );
    }
}
