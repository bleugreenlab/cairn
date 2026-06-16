use serde::{Deserialize, Serialize};
use turso::params;

use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};

const SOURCE_KIND_ISSUE: &str = "issue";
const SOURCE_KIND_PEER: &str = "peer";
const SOURCE_KIND_USER: &str = "user";
const SOURCE_KIND_PROCESS: &str = "process";
const SOURCE_KIND_RESOURCE: &str = "resource";
const SOURCE_KIND_CONDITION: &str = "condition";
const SOURCE_KIND_ISSUE_COMMENT: &str = "issue_comment";
const SOURCE_KIND_ISSUE_MESSAGE: &str = "issue_message";
const FACT_KIND_MESSAGE: &str = "message";
pub const FACT_KIND_TERMINAL_EXIT: &str = "terminal_exit";

// CAIRN-1647: the attention ledger collapses the old `agent_idle_with_work` +
// `pr_state_change` fan-out into a single `review` item kind. Default child
// subscriptions carry the new vocabulary; legacy `agent_idle_with_work` /
// `pr_state_change` subscription rows still match `review` items via the alias
// map in attention_delivery::kind_aliases, so old rows keep working.
const DEFAULT_CHILD_FACT_KINDS: &[&str] = &[
    "question",
    "permission",
    "review",
    "resolved",
    FACT_KIND_MESSAGE,
];

/// Typed source taxonomy for every external wake a job can subscribe to.
///
/// Time is deliberately absent: wakes are event-routed, not polled/scheduled.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WakeSource {
    Issue { reference: String },
    Peer { reference: Option<String> },
    User,
    Process { reference: String },
    Resource { reference: String },
    Condition { reference: String },
}

impl WakeSource {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Issue { .. } => SOURCE_KIND_ISSUE,
            Self::Peer { .. } => SOURCE_KIND_PEER,
            Self::User => SOURCE_KIND_USER,
            Self::Process { .. } => SOURCE_KIND_PROCESS,
            Self::Resource { .. } => SOURCE_KIND_RESOURCE,
            Self::Condition { .. } => SOURCE_KIND_CONDITION,
        }
    }

    pub fn reference(&self) -> Option<&str> {
        match self {
            Self::Issue { reference }
            | Self::Process { reference }
            | Self::Resource { reference }
            | Self::Condition { reference } => Some(reference.as_str()),
            Self::Peer { reference } => reference.as_deref(),
            Self::User => None,
        }
    }

    pub fn from_parts(kind: &str, reference: Option<&str>) -> Result<Self, String> {
        match kind {
            SOURCE_KIND_ISSUE => Ok(Self::Issue { reference: required_ref(kind, reference)? }),
            SOURCE_KIND_PEER => Ok(Self::Peer { reference: reference.filter(|value| !value.is_empty()).map(ToString::to_string) }),
            SOURCE_KIND_USER => {
                if reference.is_some() {
                    return Err("wake source kind 'user' must not include ref".to_string());
                }
                Ok(Self::User)
            }
            SOURCE_KIND_PROCESS => Ok(Self::Process { reference: required_ref(kind, reference)? }),
            SOURCE_KIND_RESOURCE => Ok(Self::Resource { reference: required_ref(kind, reference)? }),
            SOURCE_KIND_CONDITION => Ok(Self::Condition { reference: required_ref(kind, reference)? }),
            _ => Err(format!(
                "unknown wake source kind '{kind}' (expected issue, peer, user, process, resource, or condition)"
            )),
        }
    }
}

fn required_ref(kind: &str, reference: Option<&str>) -> Result<String, String> {
    reference
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| format!("wake source kind '{kind}' requires ref"))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WakeScope {
    pub source: WakeSource,
    pub fact_kinds: Option<Vec<String>>,
}

impl WakeScope {
    pub fn new(source: WakeSource, fact_kinds: Option<Vec<String>>) -> Self {
        Self { source, fact_kinds }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WakeEvent {
    pub source: WakeSource,
    pub fact_kind: String,
    pub detail_uri: Option<String>,
    pub delivery: WakeDelivery,
    pub urgency: DeliveryUrgency,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WakeDelivery {
    /// Deliver one known subscriber job if its best matching subscription accepts.
    Targeted {
        subscriber_job_id: String,
        message: String,
    },
    /// Deliver every job whose wake subscriptions match the event source/fact.
    Broadcast { message: String },
    /// Message-like content for digest routing. Accepted active delivery remains
    /// with the durable message/side-channel row that already exists.
    MessageDigest {
        subscriber_job_id: String,
        content: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WakeRouteAction {
    Delivered,
    Suppressed,
    Dropped,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WakeSubscriptionState {
    Active,
    Muted,
    Unsubscribed,
}

impl WakeSubscriptionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Muted => "muted",
            Self::Unsubscribed => "unsubscribed",
        }
    }

    fn from_str(value: &str) -> Self {
        match value {
            "muted" => Self::Muted,
            "unsubscribed" => Self::Unsubscribed,
            _ => Self::Active,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WakeSubscription {
    pub id: String,
    pub job_id: String,
    pub source_kind: String,
    pub source_ref: Option<String>,
    pub fact_kinds: Option<Vec<String>>,
    pub state: WakeSubscriptionState,
    pub mute_until_kind: Option<String>,
    pub mute_until_ref: Option<String>,
    pub created_by: String,
    pub created_at: i64,
    pub updated_at: i64,
    /// Consumed (row deleted) the first time a matching wake routes to it, so a
    /// one-time fact like a terminal exit can never wake the subscriber twice.
    pub one_shot: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SuppressedWake {
    pub id: String,
    pub subscription_id: Option<String>,
    pub job_id: String,
    pub source_kind: String,
    pub source_ref: Option<String>,
    pub fact_kind: Option<String>,
    pub occurrences: i64,
    pub latest_detail_uri: Option<String>,
    pub content: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub delivered_at: Option<i64>,
}

impl SuppressedWake {
    pub fn render_digest(notices: &[SuppressedWake]) -> String {
        Self::render_digest_with_context(notices, None)
    }

    pub fn render_digest_with_context(
        notices: &[SuppressedWake],
        woken_by: Option<&WakeSource>,
    ) -> String {
        if notices.is_empty() {
            return String::new();
        }
        let mut facts = Vec::new();
        let mut messages = Vec::new();
        for notice in notices {
            if let Some(content) = &notice.content {
                messages.push(format!("  • {}", content));
            } else {
                let source = match &notice.source_ref {
                    Some(source_ref) => format!("{} {}", notice.source_kind, source_ref),
                    None => notice.source_kind.clone(),
                };
                let kind = notice.fact_kind.as_deref().unwrap_or("event");
                let mut line = format!("  • {source} / {kind} ×{}", notice.occurrences.max(1));
                if let Some(detail_uri) = &notice.latest_detail_uri {
                    line.push_str(&format!(" — latest: {detail_uri}"));
                }
                facts.push(line);
            }
        }
        let mut lifted = notices
            .iter()
            .map(|notice| match &notice.source_ref {
                Some(source_ref) => format!("{} {}", notice.source_kind, source_ref),
                None => notice.source_kind.clone(),
            })
            .collect::<Vec<_>>();
        lifted.sort();
        lifted.dedup();
        let lifted = lifted.join(", ");
        let woken_by = woken_by
            .map(|source| match source.reference() {
                Some(reference) => format!("{} {}", source.kind(), reference),
                None => source.kind().to_string(),
            })
            .unwrap_or_else(|| "live resume".to_string());
        let mut out = format!(
            "[Resuming — lifting wake snooze on {lifted}; woken by: {woken_by}]\nWhile snoozed:"
        );
        if facts.is_empty() {
            out.push_str("\n  • No attention facts.");
        } else {
            out.push('\n');
            out.push_str(&facts.join("\n"));
        }
        if !messages.is_empty() {
            out.push_str(&format!(
                "\nMessages ({}):\n{}",
                messages.len(),
                messages.join("\n")
            ));
        }
        out
    }
}

fn fact_kinds_json(fact_kinds: Option<&[String]>) -> Option<String> {
    fact_kinds.map(|values| {
        let mut values = values.to_vec();
        values.sort();
        values.dedup();
        serde_json::to_string(&values).unwrap_or_else(|_| "[]".to_string())
    })
}

fn subscription_from_row(row: &turso::Row) -> DbResult<WakeSubscription> {
    let fact_kinds_json = row.opt_text(4)?;
    let fact_kinds = fact_kinds_json
        .as_deref()
        .and_then(|value| serde_json::from_str::<Vec<String>>(value).ok());
    Ok(WakeSubscription {
        id: row.text(0)?,
        job_id: row.text(1)?,
        source_kind: row.text(2)?,
        source_ref: row.opt_text(3)?,
        fact_kinds,
        state: WakeSubscriptionState::from_str(&row.text(5)?),
        mute_until_kind: row.opt_text(6)?,
        mute_until_ref: row.opt_text(7)?,
        created_by: row.text(8)?,
        created_at: row.i64(9)?,
        updated_at: row.i64(10)?,
        one_shot: row.i64(11)? != 0,
    })
}

fn suppressed_from_row(row: &turso::Row) -> DbResult<SuppressedWake> {
    Ok(SuppressedWake {
        id: row.text(0)?,
        subscription_id: row.opt_text(1)?,
        job_id: row.text(2)?,
        source_kind: row.text(3)?,
        source_ref: row.opt_text(4)?,
        fact_kind: row.opt_text(5)?,
        occurrences: row.i64(6)?,
        latest_detail_uri: row.opt_text(7)?,
        content: row.opt_text(8)?,
        created_at: row.i64(9)?,
        updated_at: row.i64(10)?,
        delivered_at: row.opt_i64(11)?,
    })
}

pub async fn list_subscriptions_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<WakeSubscription>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, job_id, source_kind, source_ref, fact_kinds_json, state,
                            mute_until_kind, mute_until_ref, created_by, created_at, updated_at,
                            one_shot
                     FROM wake_subscriptions
                     WHERE job_id = ?1
                     ORDER BY created_at ASC, id ASC",
                    params![job_id.as_str()],
                )
                .await?;
            let mut subscriptions = Vec::new();
            while let Some(row) = rows.next().await? {
                subscriptions.push(subscription_from_row(&row)?);
            }
            Ok(subscriptions)
        })
    })
    .await
    .map_err(|error| format!("Failed to list wake subscriptions: {error}"))
}

async fn exact_subscription(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kinds: Option<&[String]>,
) -> Result<Option<WakeSubscription>, String> {
    let job_id = job_id.to_string();
    let source_kind = source_kind.to_string();
    let source_ref = source_ref.map(ToString::to_string);
    let fact_kinds_json = fact_kinds_json(fact_kinds);
    db.read(|conn| {
        let job_id = job_id.clone();
        let source_kind = source_kind.clone();
        let source_ref = source_ref.clone();
        let fact_kinds_json = fact_kinds_json.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, job_id, source_kind, source_ref, fact_kinds_json, state,
                            mute_until_kind, mute_until_ref, created_by, created_at, updated_at,
                            one_shot
                     FROM wake_subscriptions
                     WHERE job_id = ?1 AND source_kind = ?2
                       AND COALESCE(source_ref, '') = COALESCE(?3, '')
                       AND COALESCE(fact_kinds_json, '') = COALESCE(?4, '')
                     LIMIT 1",
                    params![
                        job_id.as_str(),
                        source_kind.as_str(),
                        source_ref.as_deref(),
                        fact_kinds_json.as_deref()
                    ],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| subscription_from_row(&row))
                .transpose()
        })
    })
    .await
    .map_err(|error| format!("Failed to read wake subscription: {error}"))
}

pub async fn peek_pending_suppressed_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<SuppressedWake>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move { select_pending_suppressed(conn, &job_id).await })
    })
    .await
    .map_err(|error| format!("Failed to peek suppressed wakes: {error}"))
}

pub async fn peek_claimable_suppressed_for_job_with_live_source(
    db: &LocalDb,
    job_id: &str,
    live_source: Option<&WakeSource>,
) -> Result<Vec<SuppressedWake>, String> {
    let job_id = job_id.to_string();
    let live_kind = live_source.map(WakeSource::kind).map(ToString::to_string);
    let live_ref = live_source
        .and_then(WakeSource::reference)
        .map(ToString::to_string);
    db.read(|conn| {
        let job_id = job_id.clone();
        let live_kind = live_kind.clone();
        let live_ref = live_ref.clone();
        Box::pin(async move {
            select_claimable_suppressed(conn, &job_id, live_kind.as_deref(), live_ref.as_deref())
                .await
        })
    })
    .await
    .map_err(|error| format!("Failed to peek claimable suppressed wakes: {error}"))
}

pub async fn claim_pending_suppressed_for_job_with_live_source(
    db: &LocalDb,
    job_id: &str,
    live_source: Option<&WakeSource>,
) -> Result<Vec<SuppressedWake>, String> {
    let job_id = job_id.to_string();
    let live_kind = live_source.map(WakeSource::kind).map(ToString::to_string);
    let live_ref = live_source
        .and_then(WakeSource::reference)
        .map(ToString::to_string);
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let job_id = job_id.clone();
        let live_kind = live_kind.clone();
        let live_ref = live_ref.clone();
        Box::pin(async move {
            let mut notices = select_claimable_suppressed(
                conn,
                &job_id,
                live_kind.as_deref(),
                live_ref.as_deref(),
            )
            .await?;
            let snapshots = notices
                .iter()
                .map(SuppressedWakeSnapshot::from)
                .collect::<Vec<_>>();
            if !snapshots.is_empty() {
                notices =
                    claim_suppressed_wake_snapshots_in_conn(conn, &job_id, &snapshots, now).await?;
            }
            Ok(notices)
        })
    })
    .await
    .map_err(|error| format!("Failed to claim suppressed wakes: {error}"))
}

pub async fn claim_suppressed_wake_preview(
    db: &LocalDb,
    job_id: &str,
    preview: &[SuppressedWake],
) -> Result<Vec<SuppressedWake>, String> {
    if preview.is_empty() {
        return Ok(Vec::new());
    }
    let job_id = job_id.to_string();
    let snapshots = preview
        .iter()
        .map(SuppressedWakeSnapshot::from)
        .collect::<Vec<_>>();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let job_id = job_id.clone();
        let snapshots = snapshots.clone();
        Box::pin(async move {
            claim_suppressed_wake_snapshots_in_conn(conn, &job_id, &snapshots, now).await
        })
    })
    .await
    .map_err(|error| format!("Failed to claim suppressed wake preview: {error}"))
}

#[derive(Clone)]
struct SuppressedWakeSnapshot {
    id: String,
    updated_at: i64,
    occurrences: i64,
    latest_detail_uri: Option<String>,
    content: Option<String>,
}

impl From<&SuppressedWake> for SuppressedWakeSnapshot {
    fn from(notice: &SuppressedWake) -> Self {
        Self {
            id: notice.id.clone(),
            updated_at: notice.updated_at,
            occurrences: notice.occurrences,
            latest_detail_uri: notice.latest_detail_uri.clone(),
            content: notice.content.clone(),
        }
    }
}

async fn claim_suppressed_wake_snapshots_in_conn(
    conn: &turso::Connection,
    job_id: &str,
    snapshots: &[SuppressedWakeSnapshot],
    now: i64,
) -> DbResult<Vec<SuppressedWake>> {
    let mut notices = Vec::new();
    for snapshot in snapshots {
        let mut rows = conn
            .query(
                "SELECT id, subscription_id, job_id, source_kind, source_ref, fact_kind,
                        occurrences, latest_detail_uri, content, created_at, updated_at, delivered_at
                 FROM suppressed_wakes
                 WHERE job_id = ?1 AND id = ?2 AND updated_at = ?3 AND occurrences = ?4
                   AND COALESCE(latest_detail_uri, '') = COALESCE(?5, '')
                   AND COALESCE(content, '') = COALESCE(?6, '')
                   AND delivered_at IS NULL
                 LIMIT 1",
                params![
                    job_id,
                    snapshot.id.as_str(),
                    snapshot.updated_at,
                    snapshot.occurrences,
                    snapshot.latest_detail_uri.as_deref(),
                    snapshot.content.as_deref()
                ],
            )
            .await?;
        if let Some(row) = rows.next().await? {
            notices.push(suppressed_from_row(&row)?);
        }
    }

    if notices.is_empty() {
        return Ok(notices);
    }

    for notice in &notices {
        conn.execute(
            "UPDATE suppressed_wakes SET delivered_at = ?1, updated_at = ?1
             WHERE job_id = ?2 AND id = ?3 AND delivered_at IS NULL",
            params![now, job_id, notice.id.as_str()],
        )
        .await?;
    }

    let subscription_ids = notices
        .iter()
        .filter_map(|notice| notice.subscription_id.clone())
        .collect::<std::collections::HashSet<_>>();
    for subscription_id in &subscription_ids {
        let mut rows = conn
            .query(
                "SELECT COUNT(*) FROM suppressed_wakes
                 WHERE job_id = ?1 AND subscription_id = ?2 AND delivered_at IS NULL",
                params![job_id, subscription_id.as_str()],
            )
            .await?;
        let remaining = crate::storage::next_i64(&mut rows, 0).await?.unwrap_or(0);
        if remaining == 0 {
            conn.execute(
                "UPDATE wake_subscriptions SET state = 'active', updated_at = ?1
                 WHERE id = ?2 AND state = 'muted'",
                params![now, subscription_id.as_str()],
            )
            .await?;
        }
    }

    for notice in &mut notices {
        notice.delivered_at = Some(now);
        notice.updated_at = now;
    }
    Ok(notices)
}

async fn select_claimable_suppressed(
    conn: &turso::Connection,
    job_id: &str,
    live_kind: Option<&str>,
    live_ref: Option<&str>,
) -> DbResult<Vec<SuppressedWake>> {
    let Some(kind) = live_kind else {
        return Ok(Vec::new());
    };
    let mut rows = conn
        .query(
            "SELECT sw.id, sw.subscription_id, sw.job_id, sw.source_kind, sw.source_ref, sw.fact_kind,
                    sw.occurrences, sw.latest_detail_uri, sw.content, sw.created_at, sw.updated_at, sw.delivered_at
             FROM suppressed_wakes sw
             JOIN wake_subscriptions ws ON ws.id = sw.subscription_id
             WHERE sw.job_id = ?1 AND sw.delivered_at IS NULL
               AND (ws.mute_until_kind IS NULL
                    OR (ws.mute_until_kind = ?2 AND COALESCE(ws.mute_until_ref, '') = COALESCE(?3, '')))
             ORDER BY sw.created_at ASC, sw.id ASC",
            params![job_id, kind, live_ref],
        )
        .await?;
    let mut notices = Vec::new();
    while let Some(row) = rows.next().await? {
        notices.push(suppressed_from_row(&row)?);
    }
    Ok(notices)
}

async fn select_pending_suppressed(
    conn: &turso::Connection,
    job_id: &str,
) -> DbResult<Vec<SuppressedWake>> {
    let mut rows = conn
        .query(
            "SELECT id, subscription_id, job_id, source_kind, source_ref, fact_kind,
                    occurrences, latest_detail_uri, content, created_at, updated_at, delivered_at
             FROM suppressed_wakes
             WHERE job_id = ?1 AND delivered_at IS NULL
               AND (subscription_id IS NOT NULL OR content IS NULL)
             ORDER BY created_at ASC, id ASC",
            params![job_id],
        )
        .await?;
    let mut notices = Vec::new();
    while let Some(row) = rows.next().await? {
        notices.push(suppressed_from_row(&row)?);
    }
    Ok(notices)
}

pub async fn subscribe_scope(
    db: &LocalDb,
    job_id: &str,
    scope: &WakeScope,
    created_by: &str,
) -> Result<WakeSubscription, String> {
    subscribe_scope_inner(db, job_id, scope, created_by, false).await
}

async fn subscribe_scope_inner(
    db: &LocalDb,
    job_id: &str,
    scope: &WakeScope,
    created_by: &str,
    one_shot: bool,
) -> Result<WakeSubscription, String> {
    upsert_subscription(
        db,
        job_id,
        scope.source.kind(),
        scope.source.reference(),
        scope.fact_kinds.as_deref(),
        WakeSubscriptionState::Active,
        None,
        None,
        created_by,
        one_shot,
    )
    .await
}

async fn seed_scope(
    db: &LocalDb,
    job_id: &str,
    scope: &WakeScope,
    created_by: &str,
) -> Result<WakeSubscription, String> {
    if let Some(existing) = exact_subscription(
        db,
        job_id,
        scope.source.kind(),
        scope.source.reference(),
        scope.fact_kinds.as_deref(),
    )
    .await?
    {
        return Ok(existing);
    }
    subscribe_scope(db, job_id, scope, created_by).await
}

pub async fn subscribe(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kinds: Option<&[String]>,
    created_by: &str,
) -> Result<WakeSubscription, String> {
    let source = WakeSource::from_parts(source_kind, source_ref)?;
    let scope = WakeScope::new(source, fact_kinds.map(|values| values.to_vec()));
    subscribe_scope(db, job_id, &scope, created_by).await
}

/// Subscribe a one-shot wake: the subscription is consumed (deleted) the first
/// time a matching wake routes to it. Used for terminal-exit subscriptions,
/// which fire exactly once.
pub async fn subscribe_one_shot(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kinds: Option<&[String]>,
    created_by: &str,
) -> Result<WakeSubscription, String> {
    let source = WakeSource::from_parts(source_kind, source_ref)?;
    let scope = WakeScope::new(source, fact_kinds.map(|values| values.to_vec()));
    subscribe_scope_inner(db, job_id, &scope, created_by, true).await
}

pub async fn seed_default_job_subscriptions(db: &LocalDb, job_id: &str) -> Result<(), String> {
    seed_scope(
        db,
        job_id,
        &WakeScope::new(WakeSource::User, None),
        "system",
    )
    .await?;
    seed_scope(
        db,
        job_id,
        &WakeScope::new(WakeSource::Peer { reference: None }, None),
        "system",
    )
    .await?;
    Ok(())
}

pub async fn mute_scope(
    db: &LocalDb,
    job_id: &str,
    scope: &WakeScope,
    until: Option<&WakeSource>,
    created_by: &str,
) -> Result<WakeSubscription, String> {
    upsert_subscription(
        db,
        job_id,
        scope.source.kind(),
        scope.source.reference(),
        scope.fact_kinds.as_deref(),
        WakeSubscriptionState::Muted,
        until.map(WakeSource::kind),
        until.and_then(WakeSource::reference),
        created_by,
        false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn mute(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kinds: Option<&[String]>,
    until_kind: Option<&str>,
    until_ref: Option<&str>,
    created_by: &str,
) -> Result<WakeSubscription, String> {
    let source = WakeSource::from_parts(source_kind, source_ref)?;
    let until = match until_kind {
        Some(kind) => Some(WakeSource::from_parts(kind, until_ref)?),
        None => None,
    };
    let scope = WakeScope::new(source, fact_kinds.map(|values| values.to_vec()));
    mute_scope(db, job_id, &scope, until.as_ref(), created_by).await
}

pub async fn unmute_matching(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
) -> Result<usize, String> {
    let source = WakeSource::from_parts(source_kind, source_ref)?;
    update_state_matching(
        db,
        job_id,
        source.kind(),
        source.reference(),
        WakeSubscriptionState::Active,
    )
    .await
}

pub async fn unsubscribe_matching(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
) -> Result<usize, String> {
    let source = WakeSource::from_parts(source_kind, source_ref)?;
    update_state_matching(
        db,
        job_id,
        source.kind(),
        source.reference(),
        WakeSubscriptionState::Unsubscribed,
    )
    .await
}

async fn update_state_matching(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    state: WakeSubscriptionState,
) -> Result<usize, String> {
    let job_id = job_id.to_string();
    let source_kind = source_kind.to_string();
    let source_ref = source_ref.map(ToString::to_string);
    let now = chrono::Utc::now().timestamp();
    let state_str = state.as_str().to_string();
    db.write(|conn| {
        let job_id = job_id.clone();
        let source_kind = source_kind.clone();
        let source_ref = source_ref.clone();
        let state_str = state_str.clone();
        Box::pin(async move {
            let changed = match source_ref.as_deref() {
                Some(source_ref) => {
                    conn.execute(
                        "UPDATE wake_subscriptions SET state = ?1, updated_at = ?2
                         WHERE job_id = ?3 AND source_kind = ?4 AND source_ref = ?5",
                        params![
                            state_str.as_str(),
                            now,
                            job_id.as_str(),
                            source_kind.as_str(),
                            source_ref
                        ],
                    )
                    .await?
                }
                None => {
                    conn.execute(
                        "UPDATE wake_subscriptions SET state = ?1, updated_at = ?2
                         WHERE job_id = ?3 AND source_kind = ?4",
                        params![
                            state_str.as_str(),
                            now,
                            job_id.as_str(),
                            source_kind.as_str()
                        ],
                    )
                    .await?
                }
            };
            Ok(changed as usize)
        })
    })
    .await
    .map_err(|error| format!("Failed to update wake subscriptions: {error}"))
}

#[allow(clippy::too_many_arguments)]
async fn upsert_subscription(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kinds: Option<&[String]>,
    state: WakeSubscriptionState,
    until_kind: Option<&str>,
    until_ref: Option<&str>,
    created_by: &str,
    one_shot: bool,
) -> Result<WakeSubscription, String> {
    let id = uuid::Uuid::new_v4().to_string();
    let job_id = job_id.to_string();
    let source_kind = source_kind.to_string();
    let source_ref = source_ref.map(ToString::to_string);
    let fact_kinds_json = fact_kinds_json(fact_kinds);
    let until_kind = until_kind.map(ToString::to_string);
    let until_ref = until_ref.map(ToString::to_string);
    let created_by = created_by.to_string();
    let state_str = state.as_str().to_string();
    let one_shot_int: i64 = if one_shot { 1 } else { 0 };
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let id = id.clone();
        let job_id = job_id.clone();
        let source_kind = source_kind.clone();
        let source_ref = source_ref.clone();
        let fact_kinds_json = fact_kinds_json.clone();
        let until_kind = until_kind.clone();
        let until_ref = until_ref.clone();
        let created_by = created_by.clone();
        let state_str = state_str.clone();
        Box::pin(async move {
            let mut existing = conn
                .query(
                    "SELECT id
                     FROM wake_subscriptions
                     WHERE job_id = ?1 AND source_kind = ?2
                       AND COALESCE(source_ref, '') = COALESCE(?3, '')
                       AND COALESCE(fact_kinds_json, '') = COALESCE(?4, '')
                     LIMIT 1",
                    params![
                        job_id.as_str(),
                        source_kind.as_str(),
                        source_ref.as_deref(),
                        fact_kinds_json.as_deref()
                    ],
                )
                .await?;
            let existing_id = existing.next().await?.map(|row| row.text(0)).transpose()?;
            drop(existing);
            if let Some(existing_id) = existing_id {
                conn.execute(
                    "UPDATE wake_subscriptions
                     SET state = ?1, mute_until_kind = ?2, mute_until_ref = ?3, updated_at = ?4,
                         one_shot = ?6
                     WHERE id = ?5",
                    params![
                        state_str.as_str(),
                        until_kind.as_deref(),
                        until_ref.as_deref(),
                        now,
                        existing_id.as_str(),
                        one_shot_int
                    ],
                )
                .await?;
            } else {
                conn.execute(
                    "INSERT INTO wake_subscriptions
                     (id, job_id, source_kind, source_ref, fact_kinds_json, state,
                      mute_until_kind, mute_until_ref, created_by, created_at, updated_at, one_shot)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, ?11)",
                    params![
                        id.as_str(),
                        job_id.as_str(),
                        source_kind.as_str(),
                        source_ref.as_deref(),
                        fact_kinds_json.as_deref(),
                        state_str.as_str(),
                        until_kind.as_deref(),
                        until_ref.as_deref(),
                        created_by.as_str(),
                        now,
                        one_shot_int
                    ],
                )
                .await?;
            }
            let mut rows = conn
                .query(
                    "SELECT id, job_id, source_kind, source_ref, fact_kinds_json, state,
                            mute_until_kind, mute_until_ref, created_by, created_at, updated_at,
                            one_shot
                     FROM wake_subscriptions
                     WHERE job_id = ?1 AND source_kind = ?2
                       AND COALESCE(source_ref, '') = COALESCE(?3, '')
                       AND COALESCE(fact_kinds_json, '') = COALESCE(?4, '')
                     LIMIT 1",
                    params![
                        job_id.as_str(),
                        source_kind.as_str(),
                        source_ref.as_deref(),
                        fact_kinds_json.as_deref()
                    ],
                )
                .await?;
            let row = rows.next().await?.ok_or_else(|| {
                crate::storage::DbError::Row("missing wake subscription".to_string())
            })?;
            subscription_from_row(&row)
        })
    })
    .await
    .map_err(|error| format!("Failed to upsert wake subscription: {error}"))
}

fn child_attention_message(
    issue_uri: &str,
    attention: &str,
    fact_kind: &str,
    detail_uri: Option<&str>,
) -> String {
    match detail_uri {
        Some(detail_uri) => format!("[Child update] {attention}/{fact_kind}. Read {detail_uri}."),
        None => format!("[Child update] {attention}/{fact_kind}. Read {issue_uri}."),
    }
}

pub fn route_child_attention(
    orch: &Orchestrator,
    _child_issue_id: &str,
    issue_uri: &str,
    attention: &str,
    fact_kind: &str,
    detail_uri: Option<&str>,
    urgency: DeliveryUrgency,
) -> Result<(), String> {
    let message = child_attention_message(issue_uri, attention, fact_kind, detail_uri);
    let event = WakeEvent {
        source: WakeSource::Issue {
            reference: issue_uri.to_string(),
        },
        fact_kind: fact_kind.to_string(),
        detail_uri: detail_uri.map(ToString::to_string),
        delivery: WakeDelivery::Broadcast { message },
        urgency,
    };
    route_wake_sync(orch, event).map(|_| ())
}

fn route_wake_sync(orch: &Orchestrator, event: WakeEvent) -> Result<WakeRouteAction, String> {
    let orch = orch.clone();
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("Failed to start database runtime: {e}"))?
                    .block_on(async move { route_wake(&orch, event).await })
            })
            .join()
            .map_err(|_| "Database task panicked".to_string())?
    })
}

/// Route one external wake through the subscription registry.
///
/// This is the subscription-governed attention choke point: it resolves either
/// the targeted subscriber named by the delivery or every job with a matching
/// subscription, delivers active subscriptions, records muted subscriptions into
/// the digest, and drops absent/unsubscribed scopes.
pub fn route_resource_updated(
    orch: &Orchestrator,
    resource_uri: &str,
) -> Result<WakeRouteAction, String> {
    let event = WakeEvent {
        source: WakeSource::Resource {
            reference: resource_uri.to_string(),
        },
        fact_kind: "updated".to_string(),
        detail_uri: Some(resource_uri.to_string()),
        delivery: WakeDelivery::Broadcast {
            message: format!("[Resource update] {resource_uri} was updated. Read {resource_uri}."),
        },
        urgency: DeliveryUrgency::Queue,
    };
    route_wake_sync(orch, event)
}

pub fn route_process_event(
    orch: &Orchestrator,
    process_ref: &str,
    fact_kind: &str,
    detail_uri: Option<&str>,
) -> Result<WakeRouteAction, String> {
    let event = WakeEvent {
        source: WakeSource::Process {
            reference: process_ref.to_string(),
        },
        fact_kind: fact_kind.to_string(),
        detail_uri: detail_uri.map(ToString::to_string),
        delivery: WakeDelivery::Broadcast {
            message: format!("[Process update] {process_ref} emitted {fact_kind}."),
        },
        urgency: DeliveryUrgency::Queue,
    };
    route_wake_sync(orch, event)
}

/// Compact runtime formatting for terminal-exit messages: `45s`, `2m05s`, `1h03m`.
fn fmt_runtime(secs: i64) -> String {
    let s = secs.max(0);
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else {
        format!("{}h{:02}m", s / 3600, (s % 3600) / 60)
    }
}

/// The rich resume message an agent sees when a subscribed terminal exits:
/// slug, exit code, runtime, the canonical URI to read, and a short output tail.
pub fn format_terminal_exit_message(
    slug: &str,
    detail_uri: &str,
    exit_code: Option<i32>,
    runtime_secs: Option<i64>,
    tail: Option<&str>,
) -> String {
    let code = match exit_code {
        Some(code) => format!("exit code {code}"),
        None => "exit code unknown".to_string(),
    };
    let mut out = format!("[Terminal exit] `{slug}` finished — {code}.");
    if let Some(rt) = runtime_secs {
        out.push_str(&format!(" Ran {}.", fmt_runtime(rt)));
    }
    out.push_str(&format!(" Read {detail_uri} for full output."));
    if let Some(tail) = tail.map(str::trim).filter(|t| !t.is_empty()) {
        out.push_str(&format!("\n\nFinal output:\n```\n{tail}\n```"));
    }
    out
}

fn terminal_exit_event(
    slug: &str,
    detail_uri: &str,
    exit_code: Option<i32>,
    runtime_secs: Option<i64>,
    tail: Option<&str>,
) -> WakeEvent {
    WakeEvent {
        // The match key is the terminal's canonical URI, NOT the bare slug:
        // slugs are only unique per job (and promotion auto-mints run-1, run-2,
        // … in every job), so a slug reference would cross-match every job's
        // same-slug subscriber. The slug stays in the human-readable message.
        source: WakeSource::Process {
            reference: detail_uri.to_string(),
        },
        fact_kind: FACT_KIND_TERMINAL_EXIT.to_string(),
        detail_uri: Some(detail_uri.to_string()),
        delivery: WakeDelivery::Broadcast {
            message: format_terminal_exit_message(slug, detail_uri, exit_code, runtime_secs, tail),
        },
        urgency: DeliveryUrgency::Queue,
    }
}

/// Route a terminal-exit wake (sync, from the PTY/promoted exit threads).
pub fn route_terminal_exit(
    orch: &Orchestrator,
    slug: &str,
    detail_uri: &str,
    exit_code: Option<i32>,
    runtime_secs: Option<i64>,
    tail: Option<&str>,
) -> Result<WakeRouteAction, String> {
    route_wake_sync(
        orch,
        terminal_exit_event(slug, detail_uri, exit_code, runtime_secs, tail),
    )
}

/// Route a terminal-exit wake from an async context (the immediate-fire path when
/// subscribing to an already-exited terminal).
pub async fn route_terminal_exit_async(
    orch: &Orchestrator,
    slug: &str,
    detail_uri: &str,
    exit_code: Option<i32>,
    runtime_secs: Option<i64>,
    tail: Option<&str>,
) -> Result<WakeRouteAction, String> {
    route_wake(
        orch,
        terminal_exit_event(slug, detail_uri, exit_code, runtime_secs, tail),
    )
    .await
}

pub fn route_condition_event(
    orch: &Orchestrator,
    condition_ref: &str,
    fact_kind: &str,
    detail_uri: Option<&str>,
) -> Result<WakeRouteAction, String> {
    let event = WakeEvent {
        source: WakeSource::Condition {
            reference: condition_ref.to_string(),
        },
        fact_kind: fact_kind.to_string(),
        detail_uri: detail_uri.map(ToString::to_string),
        delivery: WakeDelivery::Broadcast {
            message: format!("[Condition update] {condition_ref} emitted {fact_kind}."),
        },
        urgency: DeliveryUrgency::Queue,
    };
    route_wake_sync(orch, event)
}

pub async fn route_wake(orch: &Orchestrator, event: WakeEvent) -> Result<WakeRouteAction, String> {
    let subscriptions = subscriptions_for_event(&orch.db.local, &event).await?;
    if subscriptions.is_empty() {
        return Ok(WakeRouteAction::Dropped);
    }

    let mut delivered = false;
    let mut suppressed = false;
    for subscription in subscriptions {
        match route_wake_to_subscription(orch, &event, subscription).await? {
            WakeRouteAction::Delivered => delivered = true,
            WakeRouteAction::Suppressed => suppressed = true,
            WakeRouteAction::Dropped => {}
        }
    }

    if delivered {
        Ok(WakeRouteAction::Delivered)
    } else if suppressed {
        Ok(WakeRouteAction::Suppressed)
    } else {
        Ok(WakeRouteAction::Dropped)
    }
}

async fn subscriptions_for_event(
    db: &LocalDb,
    event: &WakeEvent,
) -> Result<Vec<WakeSubscription>, String> {
    match &event.delivery {
        WakeDelivery::Targeted {
            subscriber_job_id, ..
        }
        | WakeDelivery::MessageDigest {
            subscriber_job_id, ..
        } => matching_subscription(
            db,
            subscriber_job_id,
            event.source.kind(),
            event.source.reference(),
            &event.fact_kind,
        )
        .await
        .map(|sub| sub.into_iter().collect()),
        WakeDelivery::Broadcast { .. } => {
            matching_subscriptions_for_source(
                db,
                event.source.kind(),
                event.source.reference(),
                &event.fact_kind,
            )
            .await
        }
    }
}

async fn route_wake_to_subscription(
    orch: &Orchestrator,
    event: &WakeEvent,
    subscription: WakeSubscription,
) -> Result<WakeRouteAction, String> {
    let action = match subscription.state {
        WakeSubscriptionState::Active => {
            deliver_active_wake(orch, event, &subscription, None).await?;
            WakeRouteAction::Delivered
        }
        WakeSubscriptionState::Muted if event.urgency == DeliveryUrgency::Interrupt => {
            deliver_active_wake(
                orch,
                event,
                &subscription,
                Some("[Interrupt wake pierced mute] "),
            )
            .await?;
            WakeRouteAction::Delivered
        }
        WakeSubscriptionState::Muted => {
            suppress_wake_for_subscription(orch, event, &subscription).await?;
            WakeRouteAction::Suppressed
        }
        WakeSubscriptionState::Unsubscribed => WakeRouteAction::Dropped,
    };

    // A one-shot subscription (terminal exit) is consumed the first time a
    // matching wake routes to it — delivered or suppressed into the digest — so
    // it can never fire twice. An unsubscribed scope never fired, so leave it.
    if subscription.one_shot && action != WakeRouteAction::Dropped {
        consume_one_shot_subscription(orch, &subscription).await?;
    }

    Ok(action)
}

async fn consume_one_shot_subscription(
    orch: &Orchestrator,
    subscription: &WakeSubscription,
) -> Result<(), String> {
    let id = subscription.id.clone();
    orch.db
        .local
        .write(|conn| {
            let id = id.clone();
            Box::pin(async move {
                conn.execute(
                    "DELETE FROM wake_subscriptions WHERE id = ?1",
                    params![id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|error| format!("Failed to consume one-shot wake subscription: {error}"))?;
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "wake_subscriptions", "action": "delete"}),
    );
    Ok(())
}

async fn deliver_active_wake(
    orch: &Orchestrator,
    event: &WakeEvent,
    subscription: &WakeSubscription,
    prefix: Option<&str>,
) -> Result<(), String> {
    match &event.delivery {
        WakeDelivery::Targeted {
            subscriber_job_id,
            message,
        } => {
            let message = format_message_with_prefix(prefix, message);
            deliver_resume_wake(
                orch,
                subscriber_job_id,
                &message,
                &event.source,
                event.urgency,
            )
            .await?;
        }
        WakeDelivery::Broadcast { message } => {
            let message = format_message_with_prefix(prefix, message);
            deliver_resume_wake(
                orch,
                &subscription.job_id,
                &message,
                &event.source,
                event.urgency,
            )
            .await?;
        }
        WakeDelivery::MessageDigest { .. } => {
            // Active message-like wakes are handled by the durable message or
            // side-channel row that created the wake. Returning Delivered tells
            // callers not to suppress or drop that row here.
        }
    }
    Ok(())
}

fn format_message_with_prefix(prefix: Option<&str>, message: &str) -> String {
    match prefix {
        Some(prefix) => format!("{prefix}{message}"),
        None => message.to_string(),
    }
}

async fn suppress_wake_for_subscription(
    orch: &Orchestrator,
    event: &WakeEvent,
    subscription: &WakeSubscription,
) -> Result<(), String> {
    match &event.delivery {
        WakeDelivery::Targeted { .. } | WakeDelivery::Broadcast { .. } => {
            record_suppressed_fact(
                &orch.db.local,
                subscription,
                &event.fact_kind,
                event.detail_uri.as_deref(),
            )
            .await?;
        }
        WakeDelivery::MessageDigest { content, .. } => {
            record_suppressed_message_for_subscription(
                &orch.db.local,
                subscription,
                &event.fact_kind,
                content,
            )
            .await?;
        }
    }
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "suppressed_wakes", "action": "upsert"}),
    );
    Ok(())
}

async fn deliver_resume_wake(
    orch: &Orchestrator,
    job_id: &str,
    message: &str,
    live_source: &WakeSource,
    urgency: DeliveryUrgency,
) -> Result<(), String> {
    let digest = claim_pending_suppressed_for_job_with_live_source(
        &orch.db.local,
        job_id,
        Some(live_source),
    )
    .await?;
    let message = if digest.is_empty() {
        message.to_string()
    } else {
        format!(
            "{}\n\n{}",
            message,
            SuppressedWake::render_digest_with_context(&digest, Some(live_source))
        )
    };
    crate::orchestrator::parent_wake::queue_or_resume_parent(orch, job_id, &message, urgency);
    Ok(())
}

pub async fn seed_default_child_subscription_for_parent_job(
    db: &LocalDb,
    parent_job_id: &str,
    issue_uri: &str,
) -> Result<WakeSubscription, String> {
    let kinds = DEFAULT_CHILD_FACT_KINDS
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    let source = WakeSource::Issue {
        reference: issue_uri.to_string(),
    };
    let scope = WakeScope::new(source, Some(kinds));
    seed_scope(db, parent_job_id, &scope, "system").await
}

pub async fn seed_default_child_subscription_for_issue(
    db: &LocalDb,
    child_issue_id: &str,
    issue_uri: &str,
) -> Result<Option<WakeSubscription>, String> {
    let Some(parent_job_id) = parent_job_for_child_issue_async(db, child_issue_id).await? else {
        return Ok(None);
    };
    seed_default_child_subscription_for_parent_job(db, &parent_job_id, issue_uri)
        .await
        .map(Some)
}

async fn parent_job_for_child_issue_async(
    db: &LocalDb,
    child_issue_id: &str,
) -> Result<Option<String>, String> {
    let child_issue_id = child_issue_id.to_string();
    db.read(|conn| {
        let child_issue_id = child_issue_id.clone();
        Box::pin(async move {
            let mut issue_rows = conn
                .query(
                    "SELECT parent_job_id, parent_issue_id FROM issues WHERE id = ?1 LIMIT 1",
                    params![child_issue_id.as_str()],
                )
                .await?;
            let Some(issue_row) = issue_rows.next().await? else {
                return Ok(None);
            };
            let spawning_job_id = issue_row.opt_text(0)?;
            let parent_issue_id = issue_row.opt_text(1)?;
            drop(issue_rows);

            if let Some(job_id) = spawning_job_id {
                let mut rows = conn
                    .query(
                        "SELECT id FROM jobs
                         WHERE id = ?1 AND status != 'failed' AND current_session_id IS NOT NULL
                         LIMIT 1",
                        params![job_id.as_str()],
                    )
                    .await?;
                if let Some(row) = rows.next().await? {
                    return row.text(0).map(Some);
                }
            }

            let Some(parent_issue_id) = parent_issue_id else {
                return Ok(None);
            };
            let mut job_rows = conn
                .query(
                    "SELECT id
                     FROM jobs
                     WHERE issue_id = ?1
                       AND parent_job_id IS NULL
                       AND status != 'failed'
                       AND current_session_id IS NOT NULL
                     ORDER BY created_at DESC
                     LIMIT 1",
                    params![parent_issue_id.as_str()],
                )
                .await?;
            crate::storage::next_text(&mut job_rows, 0).await
        })
    })
    .await
    .map_err(|error| format!("Failed to resolve parent wake job: {error}"))
}

async fn matching_subscription(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kind: &str,
) -> Result<Option<WakeSubscription>, String> {
    let subscriptions = list_subscriptions_for_job(db, job_id).await?;
    Ok(best_matching_subscription(
        subscriptions,
        source_kind,
        source_ref,
        fact_kind,
    ))
}

async fn matching_subscriptions_for_source(
    db: &LocalDb,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kind: &str,
) -> Result<Vec<WakeSubscription>, String> {
    let source_kind = source_kind.to_string();
    let source_ref = source_ref.map(ToString::to_string);
    let fact_kind = fact_kind.to_string();
    db.read(|conn| {
        let source_kind = source_kind.clone();
        let source_ref = source_ref.clone();
        let fact_kind = fact_kind.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, job_id, source_kind, source_ref, fact_kinds_json, state,
                            mute_until_kind, mute_until_ref, created_by, created_at, updated_at,
                            one_shot
                     FROM wake_subscriptions
                     WHERE source_kind = ?1
                       AND (COALESCE(source_ref, '') = COALESCE(?2, '')
                            OR (source_kind = 'peer' AND source_ref IS NULL))
                     ORDER BY job_id ASC, created_at ASC, id ASC",
                    params![source_kind.as_str(), source_ref.as_deref()],
                )
                .await?;
            let mut by_job: std::collections::BTreeMap<String, Vec<WakeSubscription>> =
                std::collections::BTreeMap::new();
            while let Some(row) = rows.next().await? {
                let sub = subscription_from_row(&row)?;
                by_job.entry(sub.job_id.clone()).or_default().push(sub);
            }
            let mut matched = Vec::new();
            for subscriptions in by_job.into_values() {
                if let Some(sub) = best_matching_subscription(
                    subscriptions,
                    &source_kind,
                    source_ref.as_deref(),
                    &fact_kind,
                ) {
                    matched.push(sub);
                }
            }
            Ok(matched)
        })
    })
    .await
    .map_err(|error| format!("Failed to list matching wake subscriptions: {error}"))
}

fn best_matching_subscription(
    subscriptions: Vec<WakeSubscription>,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kind: &str,
) -> Option<WakeSubscription> {
    subscriptions
        .into_iter()
        .filter(|sub| {
            sub.source_kind == source_kind
                && (sub.source_ref.as_deref() == source_ref
                    || (sub.source_kind == SOURCE_KIND_PEER && sub.source_ref.is_none()))
                && sub
                    .fact_kinds
                    .as_ref()
                    .map(|kinds| kinds.iter().any(|kind| kind == fact_kind))
                    .unwrap_or(true)
        })
        .max_by_key(subscription_match_score)
}

fn subscription_match_score(sub: &WakeSubscription) -> (i32, i32, i32, i64) {
    let creator_score = if sub.created_by == "system" { 0 } else { 1 };
    let ref_specificity_score = if sub.source_ref.is_some() { 1 } else { 0 };
    let fact_specificity_score = match &sub.fact_kinds {
        Some(kinds) => 10_000i32.saturating_sub(kinds.len() as i32),
        None => 5_000,
    };
    (
        creator_score,
        ref_specificity_score,
        fact_specificity_score,
        sub.updated_at,
    )
}

pub async fn record_live_comment_side_channel_message(
    db: &LocalDb,
    job_id: &str,
    issue_uri: &str,
    rendered: &str,
) -> Result<SuppressedWake, String> {
    insert_message_wake_row(
        db,
        None,
        job_id,
        SOURCE_KIND_ISSUE_COMMENT,
        Some(issue_uri),
        FACT_KIND_MESSAGE,
        Some(issue_uri),
        rendered,
    )
    .await
}

pub async fn record_live_issue_message_side_channel_message(
    db: &LocalDb,
    job_id: &str,
    issue_uri: &str,
    rendered: &str,
) -> Result<SuppressedWake, String> {
    insert_message_wake_row(
        db,
        None,
        job_id,
        SOURCE_KIND_ISSUE_MESSAGE,
        Some(issue_uri),
        FACT_KIND_MESSAGE,
        Some(issue_uri),
        rendered,
    )
    .await
}

pub async fn peek_pending_live_side_channel_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<SuppressedWake>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move { select_pending_live_side_channel(conn, &job_id).await })
    })
    .await
    .map_err(|error| format!("Failed to peek pending side-channel wake messages: {error}"))
}

pub async fn claim_pending_live_side_channel_for_job_async(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<SuppressedWake>, String> {
    let job_id = job_id.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut notices = select_pending_live_side_channel(conn, &job_id).await?;
            if !notices.is_empty() {
                conn.execute(
                    "UPDATE suppressed_wakes
                     SET delivered_at = ?1, updated_at = ?1
                     WHERE job_id = ?2 AND delivered_at IS NULL
                       AND subscription_id IS NULL AND content IS NOT NULL
                       AND fact_kind = 'message'",
                    params![now, job_id.as_str()],
                )
                .await?;
                for notice in &mut notices {
                    notice.delivered_at = Some(now);
                    notice.updated_at = now;
                }
            }
            Ok(notices)
        })
    })
    .await
    .map_err(|error| format!("Failed to claim pending side-channel wake messages: {error}"))
}

async fn select_pending_live_side_channel(
    conn: &turso::Connection,
    job_id: &str,
) -> DbResult<Vec<SuppressedWake>> {
    let mut rows = conn
        .query(
            "SELECT id, subscription_id, job_id, source_kind, source_ref, fact_kind,
                    occurrences, latest_detail_uri, content, created_at, updated_at, delivered_at
             FROM suppressed_wakes
             WHERE job_id = ?1 AND delivered_at IS NULL
               AND subscription_id IS NULL AND content IS NOT NULL
               AND fact_kind = 'message'
             ORDER BY created_at ASC, id ASC",
            params![job_id],
        )
        .await?;
    let mut notices = Vec::new();
    while let Some(row) = rows.next().await? {
        notices.push(suppressed_from_row(&row)?);
    }
    Ok(notices)
}

async fn record_suppressed_message_for_subscription(
    db: &LocalDb,
    subscription: &WakeSubscription,
    fact_kind: &str,
    content: &str,
) -> Result<SuppressedWake, String> {
    insert_message_wake_row(
        db,
        Some(subscription.id.as_str()),
        &subscription.job_id,
        &subscription.source_kind,
        subscription.source_ref.as_deref(),
        fact_kind,
        None,
        content,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn insert_message_wake_row(
    db: &LocalDb,
    subscription_id: Option<&str>,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kind: &str,
    detail_uri: Option<&str>,
    content: &str,
) -> Result<SuppressedWake, String> {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp();
    let subscription_id = subscription_id.map(ToString::to_string);
    let job_id = job_id.to_string();
    let source_kind = source_kind.to_string();
    let source_ref = source_ref.map(ToString::to_string);
    let fact_kind = fact_kind.to_string();
    let detail_uri = detail_uri.map(ToString::to_string);
    let content = content.to_string();
    db.write(|conn| {
        let id = id.clone();
        let subscription_id = subscription_id.clone();
        let job_id = job_id.clone();
        let source_kind = source_kind.clone();
        let source_ref = source_ref.clone();
        let fact_kind = fact_kind.clone();
        let detail_uri = detail_uri.clone();
        let content = content.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO suppressed_wakes
                 (id, subscription_id, job_id, source_kind, source_ref, fact_kind,
                  occurrences, latest_detail_uri, content, created_at, updated_at, delivered_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, ?8, ?9, ?9, NULL)",
                params![
                    id.as_str(),
                    subscription_id.as_deref(),
                    job_id.as_str(),
                    source_kind.as_str(),
                    source_ref.as_deref(),
                    fact_kind.as_str(),
                    detail_uri.as_deref(),
                    content.as_str(),
                    now
                ],
            )
            .await?;
            let mut rows = conn
                .query(
                    "SELECT id, subscription_id, job_id, source_kind, source_ref, fact_kind,
                            occurrences, latest_detail_uri, content, created_at, updated_at, delivered_at
                     FROM suppressed_wakes WHERE id = ?1",
                    params![id.as_str()],
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| crate::storage::DbError::Row("missing wake message".to_string()))?;
            suppressed_from_row(&row)
        })
    })
    .await
    .map_err(|error| format!("Failed to record wake message: {error}"))
}

#[cfg(test)]
pub async fn record_suppressed_message(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    content: &str,
) -> Result<SuppressedWake, String> {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp();
    let job_id = job_id.to_string();
    let source_kind = source_kind.to_string();
    let source_ref = source_ref.map(ToString::to_string);
    let content = content.to_string();
    db.write(|conn| {
        let id = id.clone();
        let job_id = job_id.clone();
        let source_kind = source_kind.clone();
        let source_ref = source_ref.clone();
        let content = content.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO suppressed_wakes
                 (id, subscription_id, job_id, source_kind, source_ref, fact_kind,
                  occurrences, latest_detail_uri, content, created_at, updated_at, delivered_at)
                 VALUES (?1, NULL, ?2, ?3, ?4, NULL, 1, NULL, ?5, ?6, ?6, NULL)",
                params![id.as_str(), job_id.as_str(), source_kind.as_str(), source_ref.as_deref(), content.as_str(), now],
            )
            .await?;
            let mut rows = conn
                .query(
                    "SELECT id, subscription_id, job_id, source_kind, source_ref, fact_kind,
                            occurrences, latest_detail_uri, content, created_at, updated_at, delivered_at
                     FROM suppressed_wakes WHERE id = ?1",
                    params![id.as_str()],
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| crate::storage::DbError::Row("missing suppressed wake".to_string()))?;
            suppressed_from_row(&row)
        })
    })
    .await
    .map_err(|error| format!("Failed to record suppressed message: {error}"))
}

async fn record_suppressed_fact(
    db: &LocalDb,
    subscription: &WakeSubscription,
    fact_kind: &str,
    detail_uri: Option<&str>,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let subscription = subscription.clone();
        let fact_kind = fact_kind.to_string();
        let detail_uri = detail_uri.map(ToString::to_string);
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM suppressed_wakes
                     WHERE job_id = ?1 AND source_kind = ?2
                       AND COALESCE(source_ref, '') = COALESCE(?3, '')
                       AND COALESCE(fact_kind, '') = COALESCE(?4, '')
                       AND content IS NULL AND delivered_at IS NULL
                     LIMIT 1",
                    params![
                        subscription.job_id.as_str(),
                        subscription.source_kind.as_str(),
                        subscription.source_ref.as_deref(),
                        fact_kind.as_str(),
                    ],
                )
                .await?;
            let existing_id = crate::storage::next_text(&mut rows, 0).await?;
            drop(rows);
            if let Some(existing_id) = existing_id {
                conn.execute(
                    "UPDATE suppressed_wakes
                     SET occurrences = occurrences + 1,
                         latest_detail_uri = ?1,
                         updated_at = ?2
                     WHERE id = ?3",
                    params![detail_uri.as_deref(), now, existing_id.as_str()],
                )
                .await?;
            } else {
                let id = uuid::Uuid::new_v4().to_string();
                conn.execute(
                    "INSERT INTO suppressed_wakes
                     (id, subscription_id, job_id, source_kind, source_ref, fact_kind,
                      occurrences, latest_detail_uri, content, created_at, updated_at, delivered_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, NULL, ?8, ?8, NULL)",
                    params![
                        id.as_str(),
                        subscription.id.as_str(),
                        subscription.job_id.as_str(),
                        subscription.source_kind.as_str(),
                        subscription.source_ref.as_deref(),
                        fact_kind.as_str(),
                        detail_uri.as_deref(),
                        now
                    ],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("Failed to record suppressed wake: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::db::DbState;
    use crate::services::testing::{RecordingProcessSpawner, TestServicesBuilder};
    use crate::storage::{LocalDb, SearchIndex};
    use tempfile::tempdir;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("wakes.db").await
    }

    fn test_orchestrator(db: LocalDb) -> Orchestrator {
        test_orchestrator_with_services(db, TestServicesBuilder::new().build())
    }

    fn test_orchestrator_with_services(
        db: LocalDb,
        services: crate::services::Services,
    ) -> Orchestrator {
        let temp = tempdir().unwrap();
        let config_dir = temp.keep();
        let index_path = config_dir.join("search-index.db");
        let db_state = Arc::new(DbState::new(
            Arc::new(db),
            Arc::new(SearchIndex::open_or_create(index_path).unwrap()),
        ));
        Orchestrator::builder(db_state, Arc::new(services), config_dir).build()
    }

    async fn seed_job(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p','w','P','P','/tmp',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES('i','p',1,'I','active','active','none',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at) VALUES('j','p','i','complete','s',1,1);
            ",
        )
        .await
        .unwrap();
    }

    async fn seed_second_job(db: &LocalDb) {
        db.execute(
            "INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at) VALUES('j2','p','i','complete','s2',2,2)",
            (),
        )
        .await
        .unwrap();
    }

    struct QueueableNodeFixture {
        job_id: &'static str,
        run_id: &'static str,
        issue_uri: &'static str,
        terminal_uri: &'static str,
    }

    /// Seed a deliverable node whose wake delivery path queues but does not
    /// resume/spawn.
    ///
    /// The fixture includes a complete execution/session/run graph and a running
    /// head turn. That makes the node deliverable (`latest_run_for_job` resolves
    /// a recipient run) while `nudge_job_for_urgency` sees an active turn and
    /// leaves Queue/Steer wakes pending for the next prompt boundary. Insert the
    /// turn before updating `jobs.current_turn_id`; FK enforcement rejects the
    /// opposite order.
    async fn seed_queueable_node(db: &LocalDb) -> QueueableNodeFixture {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p','w','P','P','/tmp',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES('i','p',1,'I','active','active','none',1,1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES('e','recipe','i','p','running',1,1);
            INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, worktree_path, status, current_session_id, created_at, updated_at) VALUES('j','e','builder','i','p','builder','builder','/tmp','running','s',1,1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at) VALUES('s','j','claude','open',1,1,1);
            INSERT INTO runs(id, project_id, issue_id, job_id, session_id, status, created_at, updated_at) VALUES('r','p','i','j','s','live',1,1);
            INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, started_at, updated_at) VALUES('t','s','r','j',1,'running','initial',1,1,1);
            UPDATE jobs SET current_turn_id = 't' WHERE id = 'j';
            ",
        )
        .await
        .unwrap();

        QueueableNodeFixture {
            job_id: "j",
            run_id: "r",
            issue_uri: "cairn://p/P/1",
            terminal_uri: "cairn://p/P/1/1/builder/terminal/run-1",
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queueable_node_delivers_wake_without_resuming_or_spawning() {
        let db = migrated_db().await;
        let fixture = seed_queueable_node(&db).await;
        subscribe_one_shot(
            &db,
            fixture.job_id,
            "process",
            Some(fixture.terminal_uri),
            Some(&["terminal_exit".to_string()]),
            "agent",
        )
        .await
        .unwrap();
        let recorder = RecordingProcessSpawner::new();
        let orch = test_orchestrator_with_services(
            db,
            TestServicesBuilder::new()
                .with_process(recorder.clone())
                .build(),
        );

        let action = route_terminal_exit_async(
            &orch,
            "run-1",
            fixture.terminal_uri,
            Some(0),
            Some(12),
            Some("ok"),
        )
        .await
        .unwrap();

        assert_eq!(action, WakeRouteAction::Delivered);
        assert_eq!(
            recorder.spawn_count(),
            0,
            "queue wake must not resume/spawn"
        );
        assert_eq!(recorder.run_count(), 0, "queue wake must not run a process");
        let rows = orch
            .db
            .local
            .query_all(
                "SELECT sender_name, content, delivered_at FROM messages WHERE channel_type='direct' AND recipient_run_id = ?1",
                params![fixture.run_id],
                |row| Ok((row.text(0)?, row.text(1)?, row.opt_i64(2)?)),
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "system");
        assert!(rows[0].1.contains(fixture.terminal_uri));
        assert!(rows[0].2.is_none(), "queued direct remains pending");
        assert!(
            list_subscriptions_for_job(&orch.db.local, fixture.job_id)
                .await
                .unwrap()
                .is_empty(),
            "one-shot wake is consumed after delivery"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queueable_node_fixture_can_drive_issue_wakes_without_tribal_setup() {
        let db = migrated_db().await;
        let fixture = seed_queueable_node(&db).await;
        subscribe(
            &db,
            fixture.job_id,
            "issue",
            Some(fixture.issue_uri),
            Some(&["review".to_string()]),
            "agent",
        )
        .await
        .unwrap();
        let recorder = RecordingProcessSpawner::new();
        let orch = test_orchestrator_with_services(
            db,
            TestServicesBuilder::new()
                .with_process(recorder.clone())
                .build(),
        );

        let action = route_wake(
            &orch,
            WakeEvent {
                source: WakeSource::Issue {
                    reference: fixture.issue_uri.to_string(),
                },
                fact_kind: "review".to_string(),
                detail_uri: Some(format!("{}/review", fixture.issue_uri)),
                delivery: WakeDelivery::Broadcast {
                    message: "child needs review".to_string(),
                },
                urgency: DeliveryUrgency::Queue,
            },
        )
        .await
        .unwrap();

        assert_eq!(action, WakeRouteAction::Delivered);
        assert_eq!(recorder.spawn_count(), 0);
        let rows = orch
            .db
            .local
            .query_all(
                "SELECT COUNT(*) FROM messages WHERE channel_type='direct' AND recipient_run_id = ?1 AND delivered_at IS NULL",
                params![fixture.run_id],
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(rows, vec![1]);
    }

    #[test]
    fn terminal_exit_message_carries_slug_code_runtime_uri_and_tail() {
        let msg = format_terminal_exit_message(
            "run-1",
            "cairn://p/P/1/1/builder/terminal/run-1",
            Some(2),
            Some(125),
            Some("error: boom"),
        );
        assert!(msg.contains("run-1"), "{msg}");
        assert!(msg.contains("exit code 2"), "{msg}");
        assert!(msg.contains("2m05s"), "{msg}");
        assert!(
            msg.contains("cairn://p/P/1/1/builder/terminal/run-1"),
            "{msg}"
        );
        assert!(msg.contains("error: boom"), "{msg}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn subscribe_one_shot_sets_flag_and_persists() {
        let db = migrated_db().await;
        seed_job(&db).await;
        let sub = subscribe_one_shot(
            &db,
            "j",
            "process",
            Some("run-1"),
            Some(&["terminal_exit".to_string()]),
            "agent",
        )
        .await
        .unwrap();
        assert!(sub.one_shot);
        let listed = list_subscriptions_for_job(&db, "j").await.unwrap();
        assert!(listed
            .iter()
            .any(|s| s.one_shot && s.source_ref.as_deref() == Some("run-1")));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminal_exit_wake_fires_once_then_is_consumed() {
        let db = migrated_db().await;
        seed_job(&db).await;
        let orch = test_orchestrator(db);
        // The subscription is keyed on the canonical URI, matching what the route
        // side emits.
        let uri = "cairn://p/P/1/1/builder/terminal/run-1";
        subscribe_one_shot(
            &orch.db.local,
            "j",
            "process",
            Some(uri),
            Some(&["terminal_exit".to_string()]),
            "agent",
        )
        .await
        .unwrap();

        // A same-slug terminal in a different scope must NOT match.
        let other = route_terminal_exit_async(
            &orch,
            "run-1",
            "cairn://p/P/9/1/builder/terminal/run-1",
            Some(0),
            Some(3),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            other,
            WakeRouteAction::Dropped,
            "a same-slug terminal in another scope must not wake this subscriber"
        );
        assert!(
            list_subscriptions_for_job(&orch.db.local, "j")
                .await
                .unwrap()
                .iter()
                .any(|s| s.source_kind == "process"),
            "a non-matching exit must leave the one-shot subscription intact"
        );

        let action = route_terminal_exit_async(&orch, "run-1", uri, Some(0), Some(12), Some("ok"))
            .await
            .unwrap();
        assert_eq!(action, WakeRouteAction::Delivered);

        // The one-shot subscription is consumed on first matching fire.
        let subs = list_subscriptions_for_job(&orch.db.local, "j")
            .await
            .unwrap();
        assert!(
            !subs
                .iter()
                .any(|s| s.source_kind == "process" && s.source_ref.as_deref() == Some(uri)),
            "one-shot subscription should be gone after firing"
        );

        // A second exit event for the same terminal finds nothing.
        let again = route_terminal_exit_async(&orch, "run-1", uri, Some(0), None, None)
            .await
            .unwrap();
        assert_eq!(again, WakeRouteAction::Dropped);
    }

    #[test]
    fn child_attention_message_with_detail_reads_detail_once() {
        let issue_uri = "cairn://p/P/2";
        let detail_uri = "cairn://p/P/2/1/builder/permissions/perm-2";
        let message = child_attention_message(
            issue_uri,
            "needs_approval",
            "agent_idle_with_work",
            Some(detail_uri),
        );

        assert_eq!(
            message,
            "[Child update] needs_approval/agent_idle_with_work. Read cairn://p/P/2/1/builder/permissions/perm-2."
        );
        assert_eq!(message.matches(issue_uri).count(), 1);
        assert_eq!(message.matches(detail_uri).count(), 1);
    }

    #[test]
    fn child_attention_message_without_detail_reads_issue_once() {
        let issue_uri = "cairn://p/P/2";
        let message = child_attention_message(issue_uri, "needs_input", "question", None);

        assert_eq!(
            message,
            "[Child update] needs_input/question. Read cairn://p/P/2."
        );
        assert_eq!(message.matches(issue_uri).count(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scoped_fact_kinds_match_granularly() {
        let db = migrated_db().await;
        seed_job(&db).await;
        let kinds = vec![
            "pr_state_change".to_string(),
            "agent_idle_with_work".to_string(),
        ];
        subscribe(
            &db,
            "j",
            "issue",
            Some("cairn://p/P/2"),
            Some(&kinds),
            "agent",
        )
        .await
        .unwrap();
        mute(
            &db,
            "j",
            "issue",
            Some("cairn://p/P/2"),
            Some(&kinds),
            None,
            None,
            "agent",
        )
        .await
        .unwrap();
        assert!(
            matching_subscription(&db, "j", "issue", Some("cairn://p/P/2"), "pr_state_change")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            matching_subscription(&db, "j", "issue", Some("cairn://p/P/2"), "question")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn digest_collapses_facts_and_preserves_messages() {
        let db = migrated_db().await;
        seed_job(&db).await;
        subscribe(&db, "j", "issue", Some("cairn://p/P/2"), None, "agent")
            .await
            .unwrap();
        let sub = mute(
            &db,
            "j",
            "issue",
            Some("cairn://p/P/2"),
            None,
            None,
            None,
            "agent",
        )
        .await
        .unwrap();
        record_suppressed_fact(&db, &sub, "pr_state_change", Some("latest-1"))
            .await
            .unwrap();
        record_suppressed_fact(&db, &sub, "pr_state_change", Some("latest-2"))
            .await
            .unwrap();
        record_suppressed_message_for_subscription(&db, &sub, "message", "[user → child] hello")
            .await
            .unwrap();
        let claimed =
            claim_pending_suppressed_for_job_with_live_source(&db, "j", Some(&WakeSource::User))
                .await
                .unwrap();
        assert_eq!(claimed.len(), 2);
        assert!(claimed
            .iter()
            .any(|n| n.fact_kind.as_deref() == Some("pr_state_change")
                && n.occurrences == 2
                && n.latest_detail_uri.as_deref() == Some("latest-2")));
        assert!(claimed
            .iter()
            .any(|n| n.content.as_deref() == Some("[user → child] hello")));
        let subs = list_subscriptions_for_job(&db, "j").await.unwrap();
        assert_eq!(subs[0].state, WakeSubscriptionState::Active);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn narrow_mute_overrides_seeded_broad_default() {
        let db = migrated_db().await;
        seed_job(&db).await;
        seed_default_child_subscription_for_parent_job(&db, "j", "cairn://p/P/2")
            .await
            .unwrap();
        let kinds = vec![
            "pr_state_change".to_string(),
            "agent_idle_with_work".to_string(),
        ];
        mute(
            &db,
            "j",
            "issue",
            Some("cairn://p/P/2"),
            Some(&kinds),
            None,
            None,
            "agent",
        )
        .await
        .unwrap();

        let pr = matching_subscription(&db, "j", "issue", Some("cairn://p/P/2"), "pr_state_change")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pr.state, WakeSubscriptionState::Muted);
        assert_eq!(pr.fact_kinds.as_ref().unwrap().len(), 2);

        let question = matching_subscription(&db, "j", "issue", Some("cairn://p/P/2"), "question")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(question.state, WakeSubscriptionState::Active);
        assert_eq!(question.created_by, "system");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn narrow_active_overrides_broad_muted_scope() {
        let db = migrated_db().await;
        seed_job(&db).await;
        subscribe(&db, "j", "issue", Some("cairn://p/P/2"), None, "agent")
            .await
            .unwrap();
        mute(
            &db,
            "j",
            "issue",
            Some("cairn://p/P/2"),
            None,
            None,
            None,
            "agent",
        )
        .await
        .unwrap();
        let kinds = vec!["question".to_string()];
        subscribe(
            &db,
            "j",
            "issue",
            Some("cairn://p/P/2"),
            Some(&kinds),
            "agent",
        )
        .await
        .unwrap();

        let question = matching_subscription(&db, "j", "issue", Some("cairn://p/P/2"), "question")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(question.state, WakeSubscriptionState::Active);
        assert_eq!(question.fact_kinds.as_ref().unwrap(), &kinds);

        let pr = matching_subscription(&db, "j", "issue", Some("cairn://p/P/2"), "pr_state_change")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pr.state, WakeSubscriptionState::Muted);
        assert!(pr.fact_kinds.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn muted_queue_wake_records_digest_but_interrupt_pierces_mute() {
        let db = migrated_db().await;
        seed_job(&db).await;
        let sub = mute(
            &db,
            "j",
            "issue",
            Some("cairn://p/P/2"),
            None,
            None,
            None,
            "agent",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);

        let queue_action = route_wake(
            &orch,
            WakeEvent {
                source: WakeSource::Issue {
                    reference: "cairn://p/P/2".to_string(),
                },
                fact_kind: "pr_state_change".to_string(),
                detail_uri: Some("cairn://p/P/2/1/pr".to_string()),
                delivery: WakeDelivery::Broadcast {
                    message: "routine PR update".to_string(),
                },
                urgency: DeliveryUrgency::Queue,
            },
        )
        .await
        .unwrap();
        assert_eq!(queue_action, WakeRouteAction::Suppressed);
        assert_eq!(
            peek_pending_suppressed_for_job(&orch.db.local, "j")
                .await
                .unwrap()
                .len(),
            1
        );

        let interrupt_action = route_wake(
            &orch,
            WakeEvent {
                source: WakeSource::Issue {
                    reference: "cairn://p/P/2".to_string(),
                },
                fact_kind: "question".to_string(),
                detail_uri: Some("cairn://p/P/2/1/questions/q".to_string()),
                delivery: WakeDelivery::Targeted {
                    subscriber_job_id: sub.job_id.clone(),
                    message: "needs answer".to_string(),
                },
                urgency: DeliveryUrgency::Interrupt,
            },
        )
        .await
        .unwrap();
        assert_eq!(interrupt_action, WakeRouteAction::Delivered);
        assert!(
            peek_pending_suppressed_for_job(&orch.db.local, "j")
                .await
                .unwrap()
                .is_empty(),
            "interrupt pierces mute and claims the queued digest into the delivered wake"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn passive_message_like_wake_respects_subscription_state() {
        let db = migrated_db().await;
        seed_job(&db).await;
        seed_default_job_subscriptions(&db, "j").await.unwrap();
        let orch = test_orchestrator(db);

        let delivered = route_wake(
            &orch,
            WakeEvent {
                source: WakeSource::User,
                fact_kind: FACT_KIND_MESSAGE.to_string(),
                detail_uri: Some("cairn://p/P/1/1/builder".to_string()),
                delivery: WakeDelivery::MessageDigest {
                    subscriber_job_id: "j".to_string(),
                    content: "passive note".to_string(),
                },
                urgency: DeliveryUrgency::Passive,
            },
        )
        .await
        .unwrap();
        assert_eq!(delivered, WakeRouteAction::Delivered);
        assert!(
            peek_pending_suppressed_for_job(&orch.db.local, "j")
                .await
                .unwrap()
                .is_empty(),
            "active passive messages remain claimable through their original row, not wake digest"
        );

        unsubscribe_matching(&orch.db.local, "j", "user", None)
            .await
            .unwrap();
        let dropped = route_wake(
            &orch,
            WakeEvent {
                source: WakeSource::User,
                fact_kind: FACT_KIND_MESSAGE.to_string(),
                detail_uri: Some("cairn://p/P/1/1/builder".to_string()),
                delivery: WakeDelivery::MessageDigest {
                    subscriber_job_id: "j".to_string(),
                    content: "dropped note".to_string(),
                },
                urgency: DeliveryUrgency::Interrupt,
            },
        )
        .await
        .unwrap();
        assert_eq!(dropped, WakeRouteAction::Dropped);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn muted_child_side_channel_records_digest_instead_of_normal_notice() {
        let db = migrated_db().await;
        seed_job(&db).await;
        subscribe(&db, "j", "issue", Some("cairn://p/P/2"), None, "agent")
            .await
            .unwrap();
        mute(
            &db,
            "j",
            "issue",
            Some("cairn://p/P/2"),
            None,
            None,
            None,
            "agent",
        )
        .await
        .unwrap();

        let subscription =
            matching_subscription(&db, "j", "issue", Some("cairn://p/P/2"), "message")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(subscription.state, WakeSubscriptionState::Muted);
        record_suppressed_message_for_subscription(
            &db,
            &subscription,
            "message",
            "[Side-channel] the user messaged your child cairn://p/P/2/1/builder:\nplease polish",
        )
        .await
        .unwrap();
        let pending = peek_pending_suppressed_for_job(&db, "j").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].source_ref.as_deref(), Some("cairn://p/P/2"));
        assert_eq!(pending[0].fact_kind.as_deref(), Some("message"));
        assert!(pending[0]
            .content
            .as_deref()
            .unwrap()
            .contains("please polish"));
    }
    #[tokio::test(flavor = "current_thread")]
    async fn seeds_default_child_subscription_from_recorded_parent_job() {
        let db = migrated_db().await;
        seed_job(&db).await;
        db.execute(
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention, parent_issue_id, parent_job_id, created_at, updated_at)
             VALUES('child', 'p', 2, 'Child', 'active', 'active', 'none', 'i', 'j', 2, 2)",
            (),
        )
        .await
        .unwrap();

        let seeded = seed_default_child_subscription_for_issue(&db, "child", "cairn://p/P/2")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(seeded.job_id, "j");
        assert_eq!(seeded.source_kind, "issue");
        assert_eq!(seeded.source_ref.as_deref(), Some("cairn://p/P/2"));
        let kinds = seeded.fact_kinds.unwrap();
        assert!(kinds.contains(&"question".to_string()));
        assert!(kinds.contains(&"message".to_string()));

        let subs = list_subscriptions_for_job(&db, "j").await.unwrap();
        assert_eq!(subs.len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn source_taxonomy_is_validated() {
        let db = migrated_db().await;
        seed_job(&db).await;
        assert!(subscribe(&db, "j", "issue", None, None, "agent")
            .await
            .is_err());
        assert!(subscribe(&db, "j", "user", Some("nope"), None, "agent")
            .await
            .is_err());
        assert!(subscribe(&db, "j", "time", None, None, "agent")
            .await
            .is_err());
        let sub = subscribe(&db, "j", "user", None, None, "agent")
            .await
            .unwrap();
        assert_eq!(sub.source_kind, "user");
        assert!(sub.source_ref.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mute_creates_a_scoped_subscription() {
        let db = migrated_db().await;
        seed_job(&db).await;
        let sub = mute(
            &db,
            "j",
            "issue",
            Some("cairn://p/P/99"),
            None,
            None,
            None,
            "agent",
        )
        .await
        .unwrap();
        assert_eq!(sub.state, WakeSubscriptionState::Muted);
        assert_eq!(sub.source_kind, "issue");
        assert_eq!(sub.source_ref.as_deref(), Some("cairn://p/P/99"));
        assert_eq!(list_subscriptions_for_job(&db, "j").await.unwrap().len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn default_seed_does_not_reactivate_unsubscribed_scope() {
        let db = migrated_db().await;
        seed_job(&db).await;
        seed_default_job_subscriptions(&db, "j").await.unwrap();
        unsubscribe_matching(&db, "j", "user", None).await.unwrap();
        seed_default_job_subscriptions(&db, "j").await.unwrap();

        let user = matching_subscription(&db, "j", "user", None, "message")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(user.state, WakeSubscriptionState::Unsubscribed);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn default_job_subscriptions_cover_user_and_any_peer() {
        let db = migrated_db().await;
        seed_job(&db).await;
        seed_default_job_subscriptions(&db, "j").await.unwrap();
        let user = matching_subscription(&db, "j", "user", None, "message")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(user.state, WakeSubscriptionState::Active);
        let peer =
            matching_subscription(&db, "j", "peer", Some("cairn://p/P/1/1/planner"), "message")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(peer.state, WakeSubscriptionState::Active);
        assert!(
            peer.source_ref.is_none(),
            "broad peer subscription should match any peer ref"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn specific_peer_subscription_overrides_broad_default() {
        let db = migrated_db().await;
        seed_job(&db).await;
        seed_default_job_subscriptions(&db, "j").await.unwrap();
        subscribe(
            &db,
            "j",
            "peer",
            Some("cairn://p/P/1/1/planner"),
            None,
            "system",
        )
        .await
        .unwrap();
        mute(
            &db,
            "j",
            "peer",
            Some("cairn://p/P/1/1/planner"),
            None,
            None,
            None,
            "system",
        )
        .await
        .unwrap();

        let specific =
            matching_subscription(&db, "j", "peer", Some("cairn://p/P/1/1/planner"), "message")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(specific.state, WakeSubscriptionState::Muted);
        assert_eq!(
            specific.source_ref.as_deref(),
            Some("cairn://p/P/1/1/planner")
        );

        let other = matching_subscription(
            &db,
            "j",
            "peer",
            Some("cairn://p/P/1/1/reviewer"),
            "message",
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(other.state, WakeSubscriptionState::Active);
        assert!(other.source_ref.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn source_matching_returns_best_subscription_for_every_subscriber() {
        let db = migrated_db().await;
        seed_job(&db).await;
        seed_second_job(&db).await;
        subscribe(&db, "j", "issue", Some("cairn://p/P/2"), None, "agent")
            .await
            .unwrap();
        subscribe(&db, "j2", "issue", Some("cairn://p/P/2"), None, "agent")
            .await
            .unwrap();
        let routine = vec!["pr_state_change".to_string()];
        mute(
            &db,
            "j2",
            "issue",
            Some("cairn://p/P/2"),
            Some(&routine),
            None,
            None,
            "agent",
        )
        .await
        .unwrap();

        let matches = matching_subscriptions_for_source(
            &db,
            "issue",
            Some("cairn://p/P/2"),
            "pr_state_change",
        )
        .await
        .unwrap();
        assert_eq!(matches.len(), 2);
        let j = matches.iter().find(|sub| sub.job_id == "j").unwrap();
        assert_eq!(j.state, WakeSubscriptionState::Active);
        let j2 = matches.iter().find(|sub| sub.job_id == "j2").unwrap();
        assert_eq!(j2.state, WakeSubscriptionState::Muted);
        assert_eq!(j2.fact_kinds.as_ref().unwrap(), &routine);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn digest_render_names_lifted_scope_and_live_wake() {
        let notice = SuppressedWake {
            id: "n".to_string(),
            subscription_id: Some("s".to_string()),
            job_id: "j".to_string(),
            source_kind: "issue".to_string(),
            source_ref: Some("cairn://p/P/2".to_string()),
            fact_kind: Some("pr_state_change".to_string()),
            occurrences: 3,
            latest_detail_uri: Some("latest".to_string()),
            content: None,
            created_at: 1,
            updated_at: 1,
            delivered_at: None,
        };
        let rendered =
            SuppressedWake::render_digest_with_context(&[notice], Some(&WakeSource::User));
        assert!(rendered.contains("lifting wake snooze on issue cairn://p/P/2"));
        assert!(rendered.contains("woken by: user"));
        assert!(rendered.contains("pr_state_change ×3"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn claimable_preview_does_not_deliver_or_lift_mute() {
        let db = migrated_db().await;
        seed_job(&db).await;
        let sub = mute(
            &db,
            "j",
            "issue",
            Some("cairn://p/P/2"),
            None,
            None,
            None,
            "agent",
        )
        .await
        .unwrap();
        record_suppressed_fact(&db, &sub, "pr_state_change", Some("latest"))
            .await
            .unwrap();

        let preview =
            peek_claimable_suppressed_for_job_with_live_source(&db, "j", Some(&WakeSource::User))
                .await
                .unwrap();
        assert_eq!(preview.len(), 1);
        let pending = peek_pending_suppressed_for_job(&db, "j").await.unwrap();
        assert_eq!(
            pending.len(),
            1,
            "preview must not stamp the digest delivered"
        );
        let still_muted = list_subscriptions_for_job(&db, "j")
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.id == sub.id)
            .unwrap();
        assert_eq!(still_muted.state, WakeSubscriptionState::Muted);

        let claimed =
            claim_pending_suppressed_for_job_with_live_source(&db, "j", Some(&WakeSource::User))
                .await
                .unwrap();
        assert_eq!(claimed.len(), 1);
        let lifted = list_subscriptions_for_job(&db, "j")
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.id == sub.id)
            .unwrap();
        assert_eq!(lifted.state, WakeSubscriptionState::Active);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn preview_claim_leaves_post_preview_updates_pending_and_muted() {
        let db = migrated_db().await;
        seed_job(&db).await;
        let sub = mute(
            &db,
            "j",
            "issue",
            Some("cairn://p/P/2"),
            None,
            None,
            None,
            "agent",
        )
        .await
        .unwrap();
        record_suppressed_fact(&db, &sub, "pr_state_change", Some("latest-1"))
            .await
            .unwrap();
        let preview =
            peek_claimable_suppressed_for_job_with_live_source(&db, "j", Some(&WakeSource::User))
                .await
                .unwrap();
        assert_eq!(preview.len(), 1);
        assert_eq!(preview[0].occurrences, 1);
        assert_eq!(preview[0].latest_detail_uri.as_deref(), Some("latest-1"));

        // Simulate the direct-message race window: after the digest was rendered
        // into a successful resume prompt, the muted source emits more work before
        // the post-success claim runs. A same-kind fact collapses into the
        // previewed row id; a message creates a second row. Neither was rendered.
        record_suppressed_fact(&db, &sub, "pr_state_change", Some("latest-2"))
            .await
            .unwrap();
        record_suppressed_message_for_subscription(&db, &sub, "message", "[user → child] later")
            .await
            .unwrap();

        let claimed = claim_suppressed_wake_preview(&db, "j", &preview)
            .await
            .unwrap();
        assert!(
            claimed.is_empty(),
            "a rendered snapshot that changed after preview must not be delivered"
        );
        let pending = peek_pending_suppressed_for_job(&db, "j").await.unwrap();
        assert_eq!(
            pending.len(),
            2,
            "post-preview rows remain for the next live wake"
        );
        assert!(pending.iter().any(|notice| {
            notice.fact_kind.as_deref() == Some("pr_state_change")
                && notice.occurrences == 2
                && notice.latest_detail_uri.as_deref() == Some("latest-2")
        }));
        assert!(pending
            .iter()
            .any(|notice| notice.content.as_deref() == Some("[user → child] later")));
        let still_muted = list_subscriptions_for_job(&db, "j")
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.id == sub.id)
            .unwrap();
        assert_eq!(still_muted.state, WakeSubscriptionState::Muted);

        let later_preview =
            peek_claimable_suppressed_for_job_with_live_source(&db, "j", Some(&WakeSource::User))
                .await
                .unwrap();
        assert_eq!(later_preview.len(), 2);
        let later_claim = claim_suppressed_wake_preview(&db, "j", &later_preview)
            .await
            .unwrap();
        assert_eq!(later_claim.len(), 2);
        let lifted = list_subscriptions_for_job(&db, "j")
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.id == sub.id)
            .unwrap();
        assert_eq!(lifted.state, WakeSubscriptionState::Active);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn generic_self_resume_does_not_claim_no_until_digest_or_lift_mute() {
        let db = migrated_db().await;
        seed_job(&db).await;
        let sub = mute(
            &db,
            "j",
            "issue",
            Some("cairn://p/P/2"),
            None,
            None,
            None,
            "agent",
        )
        .await
        .unwrap();
        record_suppressed_fact(&db, &sub, "pr_state_change", Some("latest"))
            .await
            .unwrap();

        let self_resume_claim = claim_pending_suppressed_for_job_with_live_source(&db, "j", None)
            .await
            .unwrap();
        assert!(
            self_resume_claim.is_empty(),
            "self-suspend/generic resumes must not flush wake digests"
        );
        let pending = peek_pending_suppressed_for_job(&db, "j").await.unwrap();
        assert_eq!(
            pending.len(),
            1,
            "digest remains pending for the next live wake"
        );
        let still_muted = list_subscriptions_for_job(&db, "j")
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.id == sub.id)
            .unwrap();
        assert_eq!(still_muted.state, WakeSubscriptionState::Muted);

        let live_claim =
            claim_pending_suppressed_for_job_with_live_source(&db, "j", Some(&WakeSource::User))
                .await
                .unwrap();
        assert_eq!(live_claim.len(), 1, "external user wake claims the digest");
        let lifted = list_subscriptions_for_job(&db, "j")
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.id == sub.id)
            .unwrap();
        assert_eq!(lifted.state, WakeSubscriptionState::Active);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn until_filter_controls_digest_claim_and_lift() {
        let db = migrated_db().await;
        seed_job(&db).await;
        subscribe(&db, "j", "issue", Some("cairn://p/P/2"), None, "agent")
            .await
            .unwrap();
        let sub = mute(
            &db,
            "j",
            "issue",
            Some("cairn://p/P/2"),
            None,
            Some("user"),
            None,
            "agent",
        )
        .await
        .unwrap();
        record_suppressed_fact(&db, &sub, "pr_state_change", Some("latest"))
            .await
            .unwrap();

        let generic = claim_pending_suppressed_for_job_with_live_source(&db, "j", None)
            .await
            .unwrap();
        assert!(
            generic.is_empty(),
            "generic resume must not lift until:user mute"
        );
        let peer = WakeSource::Peer {
            reference: Some("cairn://p/P/1/1/peer".to_string()),
        };
        let peer_claim = claim_pending_suppressed_for_job_with_live_source(&db, "j", Some(&peer))
            .await
            .unwrap();
        assert!(
            peer_claim.is_empty(),
            "peer wake must not lift until:user mute"
        );
        let user_claim =
            claim_pending_suppressed_for_job_with_live_source(&db, "j", Some(&WakeSource::User))
                .await
                .unwrap();
        assert_eq!(user_claim.len(), 1);
        let sub = list_subscriptions_for_job(&db, "j")
            .await
            .unwrap()
            .into_iter()
            .find(|s| s.id == sub.id)
            .unwrap();
        assert_eq!(sub.state, WakeSubscriptionState::Active);
    }
}
