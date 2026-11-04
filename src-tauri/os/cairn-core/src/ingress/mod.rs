//! Generic external ingress: events minted endpoints deliver over the
//! install's relay channel.
//!
//! An endpoint is minted through the api (see [`endpoints`]) and belongs to
//! the relay channel of the install that minted it, so its events arrive on
//! the same feed as the GitHub App's. The relay consumer hands each endpoint
//! event to [`consume::consume`], which records it in `ingress_events` (the
//! canonical local record, delivered or not) and mints one `ingress` fact log
//! row pointing at it. The GitHub App's own endpoint
//! ([`EndpointRecord::is_github_app`]) is the exception: its events are App
//! webhooks, handed on to [`crate::github::relay`]. Thread wakes then come
//! from the fact log's standing triggers, coalesced per thread and source like
//! any other fact.
//!
//! This module owns the two local tables: `ingress_endpoints`, this install's
//! mirror of the endpoints it minted, and `ingress_events`.

pub mod channel;
pub mod consume;
pub mod custody;
pub mod endpoints;
pub mod sync;
pub mod ws;

use std::sync::OnceLock;

use cairn_db::turso::params;

use crate::storage::{LocalDb, RowExt};

/// The api's relay, which every install's channel lives on. Follows
/// `CAIRN_API_URL`, so a dev instance can be pointed at a local api.
pub fn relay_url() -> String {
    crate::api::ApiConfig::default().relay_url()
}

/// The one HTTP client every relay call goes through: sync, registration, key
/// repair, and endpoint minting all talk to the same host with the same bearer
/// scheme, so they share one connection pool for the life of the process.
///
/// It carries no client-level timeout; each caller bounds its own requests
/// with `RequestBuilder::timeout`. Awaiting the shared TLS config first means
/// no relay task ever occupies a runtime worker while the platform root store
/// loads (see `cairn_tls`).
pub(crate) async fn relay_client() -> Result<&'static reqwest::Client, String> {
    static CLIENT: OnceLock<Result<reqwest::Client, String>> = OnceLock::new();
    if CLIENT.get().is_none() {
        cairn_tls::ready().await;
    }
    CLIENT
        .get_or_init(|| {
            cairn_tls::client_builder()
                .build()
                .map_err(|error| format!("could not build the relay client: {error}"))
        })
        .as_ref()
        .map_err(Clone::clone)
}

/// Longest body an `ingress_events` row keeps, the api's largest per-endpoint
/// body cap. A longer body is cut and the row says so.
pub const MAX_BODY_CHARS: usize = 1024 * 1024;

/// One endpoint as this install knows it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EndpointRecord {
    pub id: String,
    pub channel_id: String,
    pub project_id: Option<String>,
    pub name: String,
    pub provider: String,
    pub topic: Option<String>,
    /// `active` or `revoked`. An orphaned endpoint is revoked.
    pub status: String,
    /// The operator grant as the api last reported it (RFC 3339).
    pub granted_at: Option<String>,
    pub created_at: i64,
    pub revoked_at: Option<i64>,
    /// Where a sender posts; `None` for first-party endpoints.
    pub url: Option<String>,
    /// Deliveries the api refused, as it last reported.
    pub rejected_count: i64,
    /// When the operator re-registered the relay channel out from under it
    /// (epoch milliseconds): its sender still posts to a URL on a channel
    /// this install no longer holds. Set once, never cleared.
    pub orphaned_at: Option<i64>,
}

impl EndpointRecord {
    pub fn is_active(&self) -> bool {
        self.status == "active"
    }

    /// Whether this is the GitHub App's own endpoint: a `github` endpoint
    /// scoped to the install rather than a project. Its events are App
    /// webhooks and go to [`crate::github::relay`]; a project-scoped `github`
    /// endpoint an agent minted is generic ingress like any other.
    pub fn is_github_app(&self) -> bool {
        self.provider == "github" && self.project_id.is_none()
    }
}

const ENDPOINT_COLUMNS: &str = "id,channel_id,project_id,name,provider,topic,status,granted_at,created_at,revoked_at,url,rejected_count,orphaned_at";

fn endpoint_from_row(row: &cairn_db::turso::Row) -> crate::storage::DbResult<EndpointRecord> {
    Ok(EndpointRecord {
        id: row.text(0)?,
        channel_id: row.text(1)?,
        project_id: row.opt_text(2)?,
        name: row.text(3)?,
        provider: row.text(4)?,
        topic: row.opt_text(5)?,
        status: row.text(6)?,
        granted_at: row.opt_text(7)?,
        created_at: row.i64(8)?,
        revoked_at: row.opt_i64(9)?,
        url: row.opt_text(10)?,
        rejected_count: row.opt_i64(11)?.unwrap_or(0),
        orphaned_at: row.opt_i64(12)?,
    })
}

pub async fn get_endpoint(db: &LocalDb, id: &str) -> Result<Option<EndpointRecord>, String> {
    db.query_opt(
        format!("SELECT {ENDPOINT_COLUMNS} FROM ingress_endpoints WHERE id = ?1"),
        (id.to_string(),),
        endpoint_from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// The project's live endpoint with this name, if any.
pub async fn active_endpoint_named(
    db: &LocalDb,
    project_id: &str,
    name: &str,
) -> Result<Option<EndpointRecord>, String> {
    db.query_opt(
        format!(
            "SELECT {ENDPOINT_COLUMNS} FROM ingress_endpoints WHERE project_id = ?1 AND name = ?2 AND status = 'active' LIMIT 1"
        ),
        (project_id.to_string(), name.to_string()),
        endpoint_from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// The project's endpoint with this name: the live one, else the most
/// recently revoked.
pub async fn endpoint_named(
    db: &LocalDb,
    project_id: &str,
    name: &str,
) -> Result<Option<EndpointRecord>, String> {
    db.query_opt(
        format!(
            "SELECT {ENDPOINT_COLUMNS} FROM ingress_endpoints WHERE project_id = ?1 AND name = ?2 ORDER BY status = 'active' DESC, created_at DESC LIMIT 1"
        ),
        (project_id.to_string(), name.to_string()),
        endpoint_from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Every endpoint this install minted, live ones first.
pub async fn all_endpoints(db: &LocalDb) -> Result<Vec<EndpointRecord>, String> {
    db.query_all(
        format!(
            "SELECT {ENDPOINT_COLUMNS} FROM ingress_endpoints ORDER BY status = 'active' DESC, project_id, name, created_at"
        ),
        (),
        endpoint_from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Every live endpoint on this install.
pub async fn active_endpoints(db: &LocalDb) -> Result<Vec<EndpointRecord>, String> {
    db.query_all(
        format!(
            "SELECT {ENDPOINT_COLUMNS} FROM ingress_endpoints WHERE status = 'active' ORDER BY project_id, name, created_at"
        ),
        (),
        endpoint_from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Every endpoint minted for the project, live ones first.
pub async fn project_endpoints(
    db: &LocalDb,
    project_id: &str,
) -> Result<Vec<EndpointRecord>, String> {
    db.query_all(
        format!(
            "SELECT {ENDPOINT_COLUMNS} FROM ingress_endpoints WHERE project_id = ?1 ORDER BY status = 'active' DESC, name, created_at"
        ),
        (project_id.to_string(),),
        endpoint_from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn upsert_endpoint(db: &LocalDb, endpoint: &EndpointRecord) -> Result<(), String> {
    let endpoint = endpoint.clone();
    db.execute(
        "INSERT INTO ingress_endpoints (id, channel_id, project_id, name, provider, topic, status, granted_at, created_at, revoked_at, url, rejected_count, orphaned_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
         ON CONFLICT(id) DO UPDATE SET
            status = excluded.status,
            granted_at = excluded.granted_at,
            revoked_at = excluded.revoked_at,
            url = COALESCE(excluded.url, ingress_endpoints.url),
            rejected_count = excluded.rejected_count,
            orphaned_at = COALESCE(excluded.orphaned_at, ingress_endpoints.orphaned_at)",
        params![
            endpoint.id,
            endpoint.channel_id,
            endpoint.project_id,
            endpoint.name,
            endpoint.provider,
            endpoint.topic,
            endpoint.status,
            endpoint.granted_at,
            endpoint.created_at,
            endpoint.revoked_at,
            endpoint.url,
            endpoint.rejected_count,
            endpoint.orphaned_at
        ],
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

/// Retire every live endpoint minted on a channel other than `channel_id`,
/// the install's channel now, and return the ones whose senders have to be
/// reconfigured.
///
/// None of them can receive anything: their events land on a channel this
/// install no longer reads. The GitHub App's is only revoked, since its
/// maintainer mints it again on the new channel. Every other one is also
/// marked orphaned. Their secrets are forgotten, as a revocation's are.
pub async fn retire_endpoints_off_channel(
    db: &LocalDb,
    channel_id: &str,
) -> Result<Vec<EndpointRecord>, String> {
    let now = chrono::Utc::now().timestamp_millis();
    let mut orphaned = Vec::new();
    for endpoint in active_endpoints(db).await? {
        if endpoint.channel_id == channel_id {
            continue;
        }
        let derived = endpoint.is_github_app();
        let retired = EndpointRecord {
            status: "revoked".into(),
            revoked_at: Some(now),
            orphaned_at: (!derived).then_some(now),
            ..endpoint
        };
        upsert_endpoint(db, &retired).await?;
        if retired.provider != "first-party" {
            if let Err(error) = custody::forget(&retired.id) {
                log::warn!(
                    "retired endpoint {} but could not delete its keychain entry: {error}",
                    retired.id
                );
            }
        }
        if !derived {
            orphaned.push(retired);
        }
    }
    Ok(orphaned)
}

/// Delete an orphaned endpoint's mirror row, and any secret this machine
/// still keeps for it. Only an orphan: a live or revoked row mirrors the
/// api's own. `None` when `id` names no orphan.
pub async fn delete_orphaned_endpoint(
    db: &LocalDb,
    id: &str,
) -> Result<Option<EndpointRecord>, String> {
    let Some(endpoint) = get_endpoint(db, id)
        .await?
        .filter(|endpoint| endpoint.orphaned_at.is_some())
    else {
        return Ok(None);
    };
    db.execute(
        "DELETE FROM ingress_endpoints WHERE id = ?1 AND orphaned_at IS NOT NULL",
        (id.to_string(),),
    )
    .await
    .map_err(|error| error.to_string())?;
    if endpoint.provider != "first-party" {
        if let Err(error) = custody::forget(&endpoint.id) {
            log::warn!(
                "deleted orphaned endpoint {} but could not delete its keychain entry: {error}",
                endpoint.id
            );
        }
    }
    Ok(Some(endpoint))
}

/// How an endpoint's events have fared on this install.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EndpointStats {
    pub delivered: i64,
    pub undeliverable: i64,
    /// When the newest event was recorded, in milliseconds.
    pub last_event_at: Option<i64>,
}

pub async fn endpoint_stats(db: &LocalDb, endpoint_id: &str) -> Result<EndpointStats, String> {
    db.query_one(
        "SELECT COALESCE(SUM(status = 'ok'), 0), COALESCE(SUM(status = 'undeliverable'), 0), MAX(recorded_at)
         FROM ingress_events WHERE endpoint_id = ?1",
        (endpoint_id.to_string(),),
        |row| {
            Ok(EndpointStats {
                delivered: row.i64(0)?,
                undeliverable: row.i64(1)?,
                last_event_at: row.opt_i64(2)?,
            })
        },
    )
    .await
    .map_err(|error| error.to_string())
}

/// An `ingress_events` row as consumed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressEventRow {
    pub id: i64,
    pub endpoint_id: String,
    pub endpoint_name: Option<String>,
    pub project_id: Option<String>,
    pub relay_event_id: i64,
    pub delivery_id: Option<String>,
    pub delivery_key: String,
    pub event_type: String,
    pub verification: Option<String>,
    pub sender_json: Option<String>,
    pub body: Option<String>,
    pub received_at: String,
    pub recorded_at: i64,
    /// `ok` or `undeliverable`.
    pub status: String,
    pub note: Option<String>,
}

const EVENT_COLUMNS: &str = "id,endpoint_id,endpoint_name,project_id,relay_event_id,delivery_id,delivery_key,type,verification,sender_json,body,received_at,recorded_at,status,note";

fn event_from_row(row: &cairn_db::turso::Row) -> crate::storage::DbResult<IngressEventRow> {
    Ok(IngressEventRow {
        id: row.i64(0)?,
        endpoint_id: row.text(1)?,
        endpoint_name: row.opt_text(2)?,
        project_id: row.opt_text(3)?,
        relay_event_id: row.i64(4)?,
        delivery_id: row.opt_text(5)?,
        delivery_key: row.text(6)?,
        event_type: row.text(7)?,
        verification: row.opt_text(8)?,
        sender_json: row.opt_text(9)?,
        body: row.opt_text(10)?,
        received_at: row.text(11)?,
        recorded_at: row.i64(12)?,
        status: row.text(13)?,
        note: row.opt_text(14)?,
    })
}

/// A delivery to record. `id`, `recorded_at` are assigned on insert.
pub type NewIngressEvent = IngressEventRow;

/// Record one delivery. A redelivery of the same `(endpoint, delivery_key)`
/// records nothing new and returns the existing row's id with `false`.
pub async fn record_event(db: &LocalDb, event: NewIngressEvent) -> Result<(i64, bool), String> {
    let lookup = (event.endpoint_id.clone(), event.delivery_key.clone());
    let inserted = db
        .write(move |conn| {
            let event = event.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "INSERT INTO ingress_events (endpoint_id, endpoint_name, project_id, relay_event_id, delivery_id, delivery_key, type, verification, sender_json, body, received_at, recorded_at, status, note)
                         SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14
                         WHERE NOT EXISTS (SELECT 1 FROM ingress_events WHERE endpoint_id = ?1 AND delivery_key = ?6)
                         RETURNING id",
                        params![
                            event.endpoint_id,
                            event.endpoint_name,
                            event.project_id,
                            event.relay_event_id,
                            event.delivery_id,
                            event.delivery_key,
                            event.event_type,
                            event.verification,
                            event.sender_json,
                            event.body,
                            event.received_at,
                            event.recorded_at,
                            event.status,
                            event.note
                        ],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some(row.i64(0)?)),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    if let Some(id) = inserted {
        return Ok((id, true));
    }
    let existing = db
        .query_opt_i64(
            "SELECT id FROM ingress_events WHERE endpoint_id = ?1 AND delivery_key = ?2",
            lookup,
        )
        .await
        .map_err(|error| error.to_string())?
        .ok_or("an ingress event neither inserted nor found")?;
    Ok((existing, false))
}

pub async fn mark_undeliverable(db: &LocalDb, id: i64, note: &str) -> Result<(), String> {
    db.execute(
        "UPDATE ingress_events SET status = 'undeliverable', note = ?2 WHERE id = ?1",
        params![id, note.to_string()],
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

pub async fn get_event(db: &LocalDb, id: i64) -> Result<Option<IngressEventRow>, String> {
    db.query_opt(
        format!("SELECT {EVENT_COLUMNS} FROM ingress_events WHERE id = ?1"),
        (id,),
        event_from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// An event's body, for hydrating its fact log row. `Ok(None)` when the row
/// is gone (pruned); a row with no body hydrates as the empty string.
pub async fn event_body(db: &LocalDb, id: i64) -> Result<Option<String>, String> {
    Ok(get_event(db, id)
        .await?
        .map(|event| event.body.unwrap_or_default()))
}

/// An endpoint's newest events.
pub async fn recent_events(
    db: &LocalDb,
    endpoint_id: &str,
    limit: i64,
) -> Result<Vec<IngressEventRow>, String> {
    db.query_all(
        format!(
            "SELECT {EVENT_COLUMNS} FROM ingress_events WHERE endpoint_id = ?1 ORDER BY id DESC LIMIT ?2"
        ),
        (endpoint_id.to_string(), limit),
        event_from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Delete up to `batch` events recorded before `older_than_ms`. The fact log
/// prunes its `ingress` rows on the same retention, so a row never outlives
/// the record it points at for long.
pub async fn prune_events(db: &LocalDb, older_than_ms: i64, batch: i64) -> Result<u64, String> {
    db.execute(
        "DELETE FROM ingress_events WHERE id IN (SELECT id FROM ingress_events WHERE recorded_at < ?1 ORDER BY id LIMIT ?2)",
        params![older_than_ms, batch],
    )
    .await
    .map_err(|error| error.to_string())
}
