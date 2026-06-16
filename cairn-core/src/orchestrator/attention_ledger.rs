//! Attention ledger: durable, level-triggered attention items (CAIRN-1647).
//!
//! The legacy wake path froze every child-attention fact into a system
//! direct-message at emit time (`[Child update] … Read X.`) and never
//! reconciled it against reality at delivery. That produced stale wakes
//! (permission already answered), re-wakes on unchanged state (idle
//! heartbeats), fan-out (one transition expressed as three facts), and
//! mute-after-handling being lifted by a stale row.
//!
//! This module is the durable substitute: a ledger of **attention items**.
//! An item OPENs when something starts needing a watcher's attention, BUMPs
//! (version++) when its content meaningfully changes, and RESOLVEs when handled
//! through any path. A per-watcher [`attention_seen`] cursor records the last
//! version each job was briefed on, and [`attention_evaluations`] is a durable,
//! coalesced wake trigger (at most one pending evaluation per watcher).
//!
//! State-items (`question`, `permission`, `review`) are deliverable only while
//! `state = 'open'`. Event-items (`resolved`, `message`) are deliverable until
//! seen. In both cases a `version > seen_version` gate means a bump after
//! delivery re-surfaces the item exactly once.
//!
//! This is the pure DB layer (CAIRN-1647 step 1). Emission wiring and the
//! delivery engine that reads these tables land in later steps; the legacy wake
//! path keeps running until then, so the ledger is observable but inert.

// This module is the pure DB foundation; emission sites and the delivery engine
// that call these functions land in subsequent CAIRN-1647 steps. Until then the
// public API has no non-test callers, so allow dead_code crate-wide-lint-wise
// for this module rather than wiring half a delivery path prematurely.
#![allow(dead_code)]

use turso::params;
use uuid::Uuid;

use crate::messages::queued::DeliveryUrgency;
use crate::storage::{run_db_blocking, DbResult, LocalDb, RowExt};

/// Canonical `source_kind` for issue-scoped attention (the only kind this
/// redesign emits; the column is kept open for future sources).
pub const SOURCE_KIND_ISSUE: &str = "issue";

/// The kind of attention an item represents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ItemKind {
    /// A question is pending for the user. State-item.
    Question,
    /// A permission prompt is awaiting a decision. State-item.
    Permission,
    /// A reviewable work product (PR / gated artifact) exists. State-item;
    /// bumps when the reviewable content changes (new commits / new version).
    Review,
    /// The issue reached a terminal status. Event-item (deliverable until seen).
    Resolved,
    /// A side-channel message was delivered to a child. Event-item; bumps with
    /// the child's response when its consuming turn ends.
    Message,
}

impl ItemKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ItemKind::Question => "question",
            ItemKind::Permission => "permission",
            ItemKind::Review => "review",
            ItemKind::Resolved => "resolved",
            ItemKind::Message => "message",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "question" => Some(ItemKind::Question),
            "permission" => Some(ItemKind::Permission),
            "review" => Some(ItemKind::Review),
            "resolved" => Some(ItemKind::Resolved),
            "message" => Some(ItemKind::Message),
            _ => None,
        }
    }

    /// State-items are deliverable only while open; event-items until seen.
    pub fn is_state_item(self) -> bool {
        matches!(
            self,
            ItemKind::Question | ItemKind::Permission | ItemKind::Review
        )
    }
}

/// Open / resolved lifecycle state of an item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ItemState {
    Open,
    Resolved,
}

impl ItemState {
    pub fn as_str(self) -> &'static str {
        match self {
            ItemState::Open => "open",
            ItemState::Resolved => "resolved",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "open" => Some(ItemState::Open),
            "resolved" => Some(ItemState::Resolved),
            _ => None,
        }
    }
}

/// Who resolved an item — drives steer/FYI rendering at delivery time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolvedBy {
    User,
    Agent,
    System,
}

impl ResolvedBy {
    pub fn as_str(self) -> &'static str {
        match self {
            ResolvedBy::User => "user",
            ResolvedBy::Agent => "agent",
            ResolvedBy::System => "system",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "user" => Some(ResolvedBy::User),
            "agent" => Some(ResolvedBy::Agent),
            "system" => Some(ResolvedBy::System),
            _ => None,
        }
    }
}

/// Stable identity of an attention item: re-opening the same `(source_kind,
/// source_ref, kind, key)` updates the existing row in place.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ItemIdentity {
    pub source_kind: String,
    pub source_ref: String,
    pub kind: ItemKind,
    pub key: String,
}

impl ItemIdentity {
    /// Identity for an issue-scoped item (`source_kind = 'issue'`).
    pub fn issue(source_ref: impl Into<String>, kind: ItemKind, key: impl Into<String>) -> Self {
        Self {
            source_kind: SOURCE_KIND_ISSUE.to_string(),
            source_ref: source_ref.into(),
            kind,
            key: key.into(),
        }
    }
}

/// What an `open_item` call did to the ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenChange {
    /// A brand-new item row was inserted.
    Opened,
    /// An existing item changed content (or was reopened) — version bumped.
    Bumped,
    /// An already-open item with identical content — no write, no wake.
    Unchanged,
}

/// Outcome of an `open_item` upsert.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenOutcome {
    pub id: String,
    pub version: i64,
    pub change: OpenChange,
}

/// An item that was just resolved, returned so callers can dispatch resolution
/// steers / FYIs to watchers that were briefed on it.
#[derive(Clone, Debug)]
pub struct ResolvedItem {
    pub id: String,
    pub kind: ItemKind,
    pub source_ref: String,
    pub issue_id: Option<String>,
    pub key: String,
    pub fingerprint: String,
    pub detail_uri: Option<String>,
    pub version: i64,
}

/// A ledger item that is currently deliverable to a given watcher job, with that
/// job's last-seen version joined in.
#[derive(Clone, Debug)]
pub struct DeliverableItem {
    pub id: String,
    pub source_kind: String,
    pub source_ref: String,
    pub issue_id: Option<String>,
    pub kind: ItemKind,
    pub key: String,
    pub state: ItemState,
    pub version: i64,
    pub escalate: bool,
    pub fingerprint: String,
    pub detail_uri: Option<String>,
    pub opened_at: i64,
    pub updated_at: i64,
    pub resolved_at: Option<i64>,
    pub resolved_by: Option<ResolvedBy>,
    pub seen_version: Option<i64>,
}

fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Open (or idempotently re-open / bump) an attention item.
///
/// - No existing row → insert (`Opened`, version 1).
/// - Existing OPEN row with identical fingerprint/detail/escalate → no write
///   (`Unchanged`): the 20s-apart duplicate wake vanishes.
/// - Existing row with a changed fingerprint, or a RESOLVED row being re-opened
///   → bump version, refresh fingerprint/detail, clear resolution (`Bumped`).
///
/// `fingerprint` is a small change-detection string (e.g. `"pending"`,
/// `artifact:3`, a PR state digest, a terminal status, or a chat turn count) —
/// NOT content. Delivery resolves `detail_uri` for the actual content.
pub async fn open_item(
    db: &LocalDb,
    identity: ItemIdentity,
    issue_id: Option<String>,
    fingerprint: String,
    detail_uri: Option<String>,
    escalate: bool,
) -> DbResult<OpenOutcome> {
    let now = now_ts();
    db.write(|conn| {
        let identity = identity.clone();
        let issue_id = issue_id.clone();
        let fingerprint = fingerprint.clone();
        let detail_uri = detail_uri.clone();
        Box::pin(async move {
            let kind_str = identity.kind.as_str();
            let mut rows = conn
                .query(
                    "SELECT id, state, version, fingerprint, detail_uri, escalate
                     FROM attention_items
                     WHERE source_kind=?1 AND source_ref=?2 AND kind=?3 AND key=?4
                     LIMIT 1",
                    params![
                        identity.source_kind.as_str(),
                        identity.source_ref.as_str(),
                        kind_str,
                        identity.key.as_str()
                    ],
                )
                .await?;
            if let Some(row) = rows.next().await? {
                let id = row.text(0)?;
                let state = row.text(1)?;
                let version = row.i64(2)?;
                let existing_fingerprint = row.text(3)?;
                let existing_detail = row.opt_text(4)?;
                let existing_escalate = row.i64(5)? != 0;
                drop(rows);

                let same = state == "open"
                    && existing_fingerprint == fingerprint
                    && existing_detail == detail_uri
                    && existing_escalate == escalate;
                if same {
                    return Ok(OpenOutcome {
                        id,
                        version,
                        change: OpenChange::Unchanged,
                    });
                }

                let new_version = version + 1;
                conn.execute(
                    "UPDATE attention_items
                     SET state='open', version=?1, fingerprint=?2, detail_uri=?3,
                         escalate=?4, updated_at=?5, resolved_at=NULL, resolved_by=NULL
                     WHERE id=?6",
                    params![
                        new_version,
                        fingerprint.as_str(),
                        detail_uri.as_deref(),
                        escalate as i64,
                        now,
                        id.as_str()
                    ],
                )
                .await?;
                Ok(OpenOutcome {
                    id,
                    version: new_version,
                    change: OpenChange::Bumped,
                })
            } else {
                drop(rows);
                let id = Uuid::new_v4().to_string();
                conn.execute(
                    "INSERT INTO attention_items
                       (id, source_kind, source_ref, issue_id, kind, key, state, version,
                        escalate, fingerprint, detail_uri, opened_at, updated_at)
                     VALUES (?1,?2,?3,?4,?5,?6,'open',1,?7,?8,?9,?10,?10)",
                    params![
                        id.as_str(),
                        identity.source_kind.as_str(),
                        identity.source_ref.as_str(),
                        issue_id.as_deref(),
                        kind_str,
                        identity.key.as_str(),
                        escalate as i64,
                        fingerprint.as_str(),
                        detail_uri.as_deref(),
                        now
                    ],
                )
                .await?;
                Ok(OpenOutcome {
                    id,
                    version: 1,
                    change: OpenChange::Opened,
                })
            }
        })
    })
    .await
}

/// Blocking wrapper for sync emission sites.
pub fn open_item_blocking(
    db: &LocalDb,
    identity: ItemIdentity,
    issue_id: Option<String>,
    fingerprint: String,
    detail_uri: Option<String>,
    escalate: bool,
) -> Result<OpenOutcome, String> {
    run_db_blocking(move || async move {
        open_item(db, identity, issue_id, fingerprint, detail_uri, escalate)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Resolve an open item by identity. Returns the resolved item if it was open
/// (so the caller can steer briefed watchers); `None` if missing or already
/// resolved (idempotent).
pub async fn resolve_item(
    db: &LocalDb,
    identity: ItemIdentity,
    resolved_by: ResolvedBy,
) -> DbResult<Option<ResolvedItem>> {
    let now = now_ts();
    db.write(|conn| {
        let identity = identity.clone();
        Box::pin(async move {
            let kind_str = identity.kind.as_str();
            let mut rows = conn
                .query(
                    "SELECT id, state, version, fingerprint, detail_uri, issue_id
                     FROM attention_items
                     WHERE source_kind=?1 AND source_ref=?2 AND kind=?3 AND key=?4
                     LIMIT 1",
                    params![
                        identity.source_kind.as_str(),
                        identity.source_ref.as_str(),
                        kind_str,
                        identity.key.as_str()
                    ],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let id = row.text(0)?;
            let state = row.text(1)?;
            let version = row.i64(2)?;
            let fingerprint = row.text(3)?;
            let detail_uri = row.opt_text(4)?;
            let issue_id = row.opt_text(5)?;
            drop(rows);
            if state == "resolved" {
                return Ok(None);
            }
            conn.execute(
                "UPDATE attention_items
                 SET state='resolved', resolved_at=?1, resolved_by=?2, updated_at=?1
                 WHERE id=?3",
                params![now, resolved_by.as_str(), id.as_str()],
            )
            .await?;
            Ok(Some(ResolvedItem {
                id,
                kind: identity.kind,
                source_ref: identity.source_ref.clone(),
                issue_id,
                key: identity.key.clone(),
                fingerprint,
                detail_uri,
                version,
            }))
        })
    })
    .await
}

/// Blocking wrapper for sync resolution sites.
pub fn resolve_item_blocking(
    db: &LocalDb,
    identity: ItemIdentity,
    resolved_by: ResolvedBy,
) -> Result<Option<ResolvedItem>, String> {
    run_db_blocking(move || async move {
        resolve_item(db, identity, resolved_by)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Cascade-resolve every still-open item for an issue (e.g. when the issue
/// reaches a terminal status: pending questions/permissions/reviews no longer
/// need the driver). Returns the items that were open, for steer/FYI dispatch.
pub async fn resolve_open_items_for_issue(
    db: &LocalDb,
    issue_id: &str,
    resolved_by: ResolvedBy,
) -> DbResult<Vec<ResolvedItem>> {
    let now = now_ts();
    let issue_id = issue_id.to_string();
    db.write(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, kind, source_ref, key, fingerprint, detail_uri, version
                     FROM attention_items
                     WHERE issue_id=?1 AND state='open'",
                    params![issue_id.as_str()],
                )
                .await?;
            let mut resolved = Vec::new();
            while let Some(row) = rows.next().await? {
                let kind = ItemKind::from_db(&row.text(1)?);
                resolved.push(ResolvedItem {
                    id: row.text(0)?,
                    kind: kind.unwrap_or(ItemKind::Resolved),
                    source_ref: row.text(2)?,
                    issue_id: Some(issue_id.clone()),
                    key: row.text(3)?,
                    fingerprint: row.text(4)?,
                    detail_uri: row.opt_text(5)?,
                    version: row.i64(6)?,
                });
            }
            drop(rows);
            if !resolved.is_empty() {
                conn.execute(
                    "UPDATE attention_items
                     SET state='resolved', resolved_at=?1, resolved_by=?2, updated_at=?1
                     WHERE issue_id=?3 AND state='open'",
                    params![now, resolved_by.as_str(), issue_id.as_str()],
                )
                .await?;
            }
            Ok(resolved)
        })
    })
    .await
}

/// Arm (or refresh) the one-shot escalation deadline on a blocker item. The
/// item opens passive; if it is still open at `escalate_at` the escalation
/// worker wakes the watcher.
pub async fn arm_escalation(db: &LocalDb, item_id: &str, escalate_at: i64) -> DbResult<()> {
    let item_id = item_id.to_string();
    db.write(|conn| {
        let item_id = item_id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE attention_items SET escalate_at=?1 WHERE id=?2",
                params![escalate_at, item_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

/// Blocking wrapper for the sync open path.
pub fn arm_escalation_blocking(
    db: &LocalDb,
    item_id: &str,
    escalate_at: i64,
) -> Result<(), String> {
    run_db_blocking(move || async move {
        arm_escalation(db, item_id, escalate_at)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Clear the escalation deadline (one-shot: after firing, or when no longer
/// needed). Idempotent.
pub async fn clear_escalation(db: &LocalDb, item_id: &str) -> DbResult<()> {
    let item_id = item_id.to_string();
    db.write(|conn| {
        let item_id = item_id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE attention_items SET escalate_at=NULL WHERE id=?1",
                params![item_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

/// Soonest pending escalation deadline across open items, or `None` if no timer
/// is armed. The worker sleeps until this instant.
pub async fn min_pending_escalation(db: &LocalDb) -> DbResult<Option<i64>> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT MIN(escalate_at) FROM attention_items
                     WHERE state='open' AND escalate_at IS NOT NULL",
                    (),
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(row.opt_i64(0)?),
                None => Ok(None),
            }
        })
    })
    .await
}

/// Open items whose escalation is due at `now`: `(id, source_ref)`.
pub async fn due_escalation_items(db: &LocalDb, now: i64) -> DbResult<Vec<(String, String)>> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, source_ref FROM attention_items
                     WHERE state='open' AND escalate_at IS NOT NULL AND escalate_at <= ?1",
                    params![now],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.text(0)?, row.text(1)?));
            }
            Ok(out)
        })
    })
    .await
}

/// List open items of a given kind for an issue: `(id, source_ref, key,
/// fingerprint, detail_uri)`. Used to re-window/bump message items at the
/// child's turn end so the requesting watcher's next briefing re-resolves the
/// chat including the child's response. The fingerprint carries the message
/// item's `msg:`/`resp:` awaiting-response state.
pub async fn list_open_items_for_issue(
    db: &LocalDb,
    issue_id: &str,
    kind: ItemKind,
) -> DbResult<Vec<(String, String, String, String, Option<String>)>> {
    let issue_id = issue_id.to_string();
    let kind_str = kind.as_str();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, source_ref, key, fingerprint, detail_uri FROM attention_items
                     WHERE issue_id=?1 AND kind=?2 AND state='open'",
                    params![issue_id.as_str(), kind_str],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((
                    row.text(0)?,
                    row.text(1)?,
                    row.text(2)?,
                    row.text(3)?,
                    row.opt_text(4)?,
                ));
            }
            Ok(out)
        })
    })
    .await
}

/// Stamp a watcher's seen cursor for an item at (at least) `version`. Monotonic:
/// an older version never lowers the cursor.
pub async fn mark_seen(db: &LocalDb, item_id: &str, job_id: &str, version: i64) -> DbResult<()> {
    let now = now_ts();
    let item_id = item_id.to_string();
    let job_id = job_id.to_string();
    db.write(|conn| {
        let item_id = item_id.clone();
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT seen_version FROM attention_seen WHERE item_id=?1 AND job_id=?2 LIMIT 1",
                    params![item_id.as_str(), job_id.as_str()],
                )
                .await?;
            let existing = match rows.next().await? {
                Some(row) => Some(row.i64(0)?),
                None => None,
            };
            drop(rows);
            match existing {
                None => {
                    conn.execute(
                        "INSERT INTO attention_seen(item_id, job_id, seen_version, seen_at)
                         VALUES (?1,?2,?3,?4)",
                        params![item_id.as_str(), job_id.as_str(), version, now],
                    )
                    .await?;
                }
                Some(existing) if version > existing => {
                    conn.execute(
                        "UPDATE attention_seen SET seen_version=?1, seen_at=?2
                         WHERE item_id=?3 AND job_id=?4",
                        params![version, now, item_id.as_str(), job_id.as_str()],
                    )
                    .await?;
                }
                Some(_) => {}
            }
            Ok(())
        })
    })
    .await
}

/// The set of items currently deliverable to `job_id` from a single source.
///
/// Deliverable = (state-item that is open) OR (event-item), gated by
/// `version > seen_version` (or never seen). Mute filtering is applied by the
/// delivery engine against `wake_subscriptions`; this is the raw candidate set.
pub async fn deliverable_items_for_source(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: &str,
) -> DbResult<Vec<DeliverableItem>> {
    let job_id = job_id.to_string();
    let source_kind = source_kind.to_string();
    let source_ref = source_ref.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        let source_kind = source_kind.clone();
        let source_ref = source_ref.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT ai.id, ai.source_kind, ai.source_ref, ai.issue_id, ai.kind, ai.key,
                            ai.state, ai.version, ai.escalate, ai.fingerprint, ai.detail_uri,
                            ai.opened_at, ai.updated_at, ai.resolved_at, ai.resolved_by,
                            s.seen_version
                     FROM attention_items ai
                     LEFT JOIN attention_seen s
                       ON s.item_id = ai.id AND s.job_id = ?1
                     WHERE ai.source_kind = ?2 AND ai.source_ref = ?3
                       AND (
                         (ai.kind IN ('question','permission','review') AND ai.state = 'open')
                         OR ai.kind IN ('resolved','message')
                       )
                       AND (s.seen_version IS NULL OR ai.version > s.seen_version)
                     ORDER BY ai.opened_at ASC",
                    params![job_id.as_str(), source_kind.as_str(), source_ref.as_str()],
                )
                .await?;
            let mut items = Vec::new();
            while let Some(row) = rows.next().await? {
                let kind = ItemKind::from_db(&row.text(4)?).unwrap_or(ItemKind::Resolved);
                let state = ItemState::from_db(&row.text(6)?).unwrap_or(ItemState::Open);
                let resolved_by = row.opt_text(14)?.and_then(|v| ResolvedBy::from_db(&v));
                items.push(DeliverableItem {
                    id: row.text(0)?,
                    source_kind: row.text(1)?,
                    source_ref: row.text(2)?,
                    issue_id: row.opt_text(3)?,
                    kind,
                    key: row.text(5)?,
                    state,
                    version: row.i64(7)?,
                    escalate: row.i64(8)? != 0,
                    fingerprint: row.text(9)?,
                    detail_uri: row.opt_text(10)?,
                    opened_at: row.i64(11)?,
                    updated_at: row.i64(12)?,
                    resolved_at: row.opt_i64(13)?,
                    resolved_by,
                    seen_version: row.opt_i64(15)?,
                });
            }
            Ok(items)
        })
    })
    .await
}

/// Request a (coalesced) evaluation of `job_id`'s deliverable set. At most one
/// pending evaluation per job; the highest urgency requested since the last
/// evaluation wins.
pub async fn request_evaluation(
    db: &LocalDb,
    job_id: &str,
    urgency: DeliveryUrgency,
) -> DbResult<()> {
    let now = now_ts();
    let job_id = job_id.to_string();
    db.write(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT urgency FROM attention_evaluations WHERE job_id=?1 LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            let existing = match rows.next().await? {
                Some(row) => Some(parse_urgency(&row.text(0)?)),
                None => None,
            };
            drop(rows);
            match existing {
                None => {
                    conn.execute(
                        "INSERT INTO attention_evaluations(job_id, requested_at, urgency)
                         VALUES (?1,?2,?3)",
                        params![job_id.as_str(), now, urgency.as_str()],
                    )
                    .await?;
                }
                Some(existing) => {
                    let winner = existing.max(urgency);
                    conn.execute(
                        "UPDATE attention_evaluations SET requested_at=?1, urgency=?2
                         WHERE job_id=?3",
                        params![now, winner.as_str(), job_id.as_str()],
                    )
                    .await?;
                }
            }
            Ok(())
        })
    })
    .await
}

/// Blocking wrapper for sync schedulers.
pub fn request_evaluation_blocking(
    db: &LocalDb,
    job_id: &str,
    urgency: DeliveryUrgency,
) -> Result<(), String> {
    run_db_blocking(move || async move {
        request_evaluation(db, job_id, urgency)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Atomically take (read + delete) the pending evaluation for `job_id`, if any,
/// returning the requested urgency. Called when the engine evaluates a watcher;
/// an empty deliverable set after this is the "no resume" drop.
pub async fn take_evaluation(db: &LocalDb, job_id: &str) -> DbResult<Option<DeliveryUrgency>> {
    let job_id = job_id.to_string();
    db.write(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT urgency FROM attention_evaluations WHERE job_id=?1 LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            let urgency = match rows.next().await? {
                Some(row) => Some(parse_urgency(&row.text(0)?)),
                None => None,
            };
            drop(rows);
            if urgency.is_some() {
                conn.execute(
                    "DELETE FROM attention_evaluations WHERE job_id=?1",
                    params![job_id.as_str()],
                )
                .await?;
            }
            Ok(urgency)
        })
    })
    .await
}

/// Whether `job_id` currently has a pending evaluation row.
pub async fn has_pending_evaluation(db: &LocalDb, job_id: &str) -> DbResult<bool> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT 1 FROM attention_evaluations WHERE job_id=?1 LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            Ok(rows.next().await?.is_some())
        })
    })
    .await
}

/// All jobs with a pending evaluation, oldest request first.
pub async fn pending_evaluation_jobs(db: &LocalDb) -> DbResult<Vec<(String, DeliveryUrgency)>> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT job_id, urgency FROM attention_evaluations ORDER BY requested_at ASC",
                    (),
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.text(0)?, parse_urgency(&row.text(1)?)));
            }
            Ok(out)
        })
    })
    .await
}

fn parse_urgency(value: &str) -> DeliveryUrgency {
    match value {
        "passive" => DeliveryUrgency::Passive,
        "steer" => DeliveryUrgency::Steer,
        "interrupt" => DeliveryUrgency::Interrupt,
        _ => DeliveryUrgency::Queue,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalDb;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("attention-ledger.db").await
    }

    /// Seed one project, one issue (`issue-1` / `cairn://p/PROJ/2`), and one
    /// watcher job so FK constraints on the ledger tables are satisfied.
    async fn seed(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at)
              VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','w','Project','PROJ','/tmp/repo',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('issue-1','p',2,'Child','active','active','none',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
              VALUES('watcher','p','issue-1','running','sess',1,1);
            ",
        )
        .await
        .unwrap();
    }

    fn question_identity() -> ItemIdentity {
        ItemIdentity::issue("cairn://p/PROJ/2", ItemKind::Question, "q-1")
    }

    #[tokio::test]
    async fn open_is_idempotent_on_identical_content() {
        let db = migrated_db().await;
        seed(&db).await;

        let first = open_item(
            &db,
            question_identity(),
            Some("issue-1".into()),
            "{\"q\":1}".into(),
            Some("cairn://p/PROJ/2/1/planner/questions/q-1".into()),
            false,
        )
        .await
        .unwrap();
        assert_eq!(first.change, OpenChange::Opened);
        assert_eq!(first.version, 1);

        // Identical re-open: no write, no version bump (the 20s-apart duplicate).
        let again = open_item(
            &db,
            question_identity(),
            Some("issue-1".into()),
            "{\"q\":1}".into(),
            Some("cairn://p/PROJ/2/1/planner/questions/q-1".into()),
            false,
        )
        .await
        .unwrap();
        assert_eq!(again.change, OpenChange::Unchanged);
        assert_eq!(again.version, 1);
        assert_eq!(again.id, first.id);
    }

    #[tokio::test]
    async fn open_bumps_on_changed_content() {
        let db = migrated_db().await;
        seed(&db).await;
        let id_first = open_item(
            &db,
            question_identity(),
            Some("issue-1".into()),
            "{\"q\":1}".into(),
            None,
            false,
        )
        .await
        .unwrap();
        let bumped = open_item(
            &db,
            question_identity(),
            Some("issue-1".into()),
            "{\"q\":2}".into(),
            None,
            false,
        )
        .await
        .unwrap();
        assert_eq!(bumped.change, OpenChange::Bumped);
        assert_eq!(bumped.version, 2);
        assert_eq!(bumped.id, id_first.id);
    }

    #[tokio::test]
    async fn resolve_is_idempotent() {
        let db = migrated_db().await;
        seed(&db).await;
        open_item(
            &db,
            question_identity(),
            Some("issue-1".into()),
            "{}".into(),
            None,
            false,
        )
        .await
        .unwrap();

        let resolved = resolve_item(&db, question_identity(), ResolvedBy::User)
            .await
            .unwrap();
        assert!(resolved.is_some());
        assert_eq!(resolved.unwrap().kind, ItemKind::Question);

        // Second resolve is a no-op.
        let again = resolve_item(&db, question_identity(), ResolvedBy::User)
            .await
            .unwrap();
        assert!(again.is_none());
    }

    #[tokio::test]
    async fn reopen_after_resolve_bumps_version() {
        let db = migrated_db().await;
        seed(&db).await;
        open_item(
            &db,
            question_identity(),
            Some("issue-1".into()),
            "{}".into(),
            None,
            false,
        )
        .await
        .unwrap();
        resolve_item(&db, question_identity(), ResolvedBy::Agent)
            .await
            .unwrap();
        let reopened = open_item(
            &db,
            question_identity(),
            Some("issue-1".into()),
            "{}".into(),
            None,
            false,
        )
        .await
        .unwrap();
        // Even identical content reopens (state changed resolved -> open).
        assert_eq!(reopened.change, OpenChange::Bumped);
        assert_eq!(reopened.version, 2);
    }

    #[tokio::test]
    async fn state_item_deliverable_until_seen_then_bump_resurfaces() {
        let db = migrated_db().await;
        seed(&db).await;
        let opened = open_item(
            &db,
            question_identity(),
            Some("issue-1".into()),
            "{\"q\":1}".into(),
            None,
            false,
        )
        .await
        .unwrap();

        let deliverable =
            deliverable_items_for_source(&db, "watcher", SOURCE_KIND_ISSUE, "cairn://p/PROJ/2")
                .await
                .unwrap();
        assert_eq!(deliverable.len(), 1);
        assert_eq!(deliverable[0].kind, ItemKind::Question);

        mark_seen(&db, &opened.id, "watcher", opened.version)
            .await
            .unwrap();
        let after_seen =
            deliverable_items_for_source(&db, "watcher", SOURCE_KIND_ISSUE, "cairn://p/PROJ/2")
                .await
                .unwrap();
        assert!(after_seen.is_empty());

        // Bump after seen → deliverable again (version > seen_version).
        let bumped = open_item(
            &db,
            question_identity(),
            Some("issue-1".into()),
            "{\"q\":2}".into(),
            None,
            false,
        )
        .await
        .unwrap();
        assert_eq!(bumped.change, OpenChange::Bumped);
        let after_bump =
            deliverable_items_for_source(&db, "watcher", SOURCE_KIND_ISSUE, "cairn://p/PROJ/2")
                .await
                .unwrap();
        assert_eq!(after_bump.len(), 1);
    }

    #[tokio::test]
    async fn resolved_state_item_drops_out_of_deliverable_set() {
        let db = migrated_db().await;
        seed(&db).await;
        open_item(
            &db,
            question_identity(),
            Some("issue-1".into()),
            "{}".into(),
            None,
            false,
        )
        .await
        .unwrap();
        // Resolved between trigger and evaluation: drops out even though unseen.
        resolve_item(&db, question_identity(), ResolvedBy::User)
            .await
            .unwrap();
        let deliverable =
            deliverable_items_for_source(&db, "watcher", SOURCE_KIND_ISSUE, "cairn://p/PROJ/2")
                .await
                .unwrap();
        assert!(deliverable.is_empty());
    }

    #[tokio::test]
    async fn event_item_message_deliverable_until_seen() {
        let db = migrated_db().await;
        seed(&db).await;
        let identity = ItemIdentity::issue("cairn://p/PROJ/2", ItemKind::Message, "msg-1");
        // Message items now store a small fingerprint (e.g. a chat turn count) and
        // resolve their content from `detail_uri` at delivery.
        let opened = open_item(
            &db,
            identity.clone(),
            Some("issue-1".into()),
            "turns:0".into(),
            Some("cairn://p/PROJ/2/1/builder/chat?offset=0".into()),
            false,
        )
        .await
        .unwrap();

        // Event-item is deliverable while open AND while resolved-but-unseen.
        let before =
            deliverable_items_for_source(&db, "watcher", SOURCE_KIND_ISSUE, "cairn://p/PROJ/2")
                .await
                .unwrap();
        assert_eq!(before.len(), 1);

        mark_seen(&db, &opened.id, "watcher", opened.version)
            .await
            .unwrap();
        let after =
            deliverable_items_for_source(&db, "watcher", SOURCE_KIND_ISSUE, "cairn://p/PROJ/2")
                .await
                .unwrap();
        assert!(after.is_empty());
    }

    #[tokio::test]
    async fn cascade_resolves_open_issue_items() {
        let db = migrated_db().await;
        seed(&db).await;
        open_item(
            &db,
            question_identity(),
            Some("issue-1".into()),
            "{}".into(),
            None,
            false,
        )
        .await
        .unwrap();
        open_item(
            &db,
            ItemIdentity::issue("cairn://p/PROJ/2", ItemKind::Permission, "perm-1"),
            Some("issue-1".into()),
            "{}".into(),
            None,
            false,
        )
        .await
        .unwrap();

        let resolved = resolve_open_items_for_issue(&db, "issue-1", ResolvedBy::System)
            .await
            .unwrap();
        assert_eq!(resolved.len(), 2);
        let after =
            deliverable_items_for_source(&db, "watcher", SOURCE_KIND_ISSUE, "cairn://p/PROJ/2")
                .await
                .unwrap();
        assert!(after.is_empty());
    }

    #[tokio::test]
    async fn escalation_arm_min_due_and_clear() {
        let db = migrated_db().await;
        seed(&db).await;
        let o = open_item(
            &db,
            question_identity(),
            Some("issue-1".into()),
            "{}".into(),
            None,
            false,
        )
        .await
        .unwrap();
        // No timer until armed.
        assert!(min_pending_escalation(&db).await.unwrap().is_none());

        arm_escalation(&db, &o.id, 1000).await.unwrap();
        assert_eq!(min_pending_escalation(&db).await.unwrap(), Some(1000));

        // Not due before the deadline; due at/after it (with the source_ref).
        assert!(due_escalation_items(&db, 999).await.unwrap().is_empty());
        let due = due_escalation_items(&db, 1000).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].0, o.id);
        assert_eq!(due[0].1, "cairn://p/PROJ/2");

        // One-shot clear.
        clear_escalation(&db, &o.id).await.unwrap();
        assert!(min_pending_escalation(&db).await.unwrap().is_none());
        assert!(due_escalation_items(&db, 9999).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn resolving_blocker_removes_its_escalation() {
        let db = migrated_db().await;
        seed(&db).await;
        let o = open_item(
            &db,
            question_identity(),
            Some("issue-1".into()),
            "{}".into(),
            None,
            false,
        )
        .await
        .unwrap();
        arm_escalation(&db, &o.id, 1000).await.unwrap();
        // User answers before the deadline -> the item resolves.
        resolve_item(&db, question_identity(), ResolvedBy::User)
            .await
            .unwrap();
        // A resolved blocker is excluded from the escalation scan (state != open),
        // so the timer no-ops at fire time.
        assert!(min_pending_escalation(&db).await.unwrap().is_none());
        assert!(due_escalation_items(&db, 5000).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn evaluation_coalesces_and_keeps_max_urgency() {
        let db = migrated_db().await;
        seed(&db).await;
        request_evaluation(&db, "watcher", DeliveryUrgency::Queue)
            .await
            .unwrap();
        request_evaluation(&db, "watcher", DeliveryUrgency::Steer)
            .await
            .unwrap();
        request_evaluation(&db, "watcher", DeliveryUrgency::Queue)
            .await
            .unwrap();

        let pending = pending_evaluation_jobs(&db).await.unwrap();
        assert_eq!(pending.len(), 1, "coalesced to one evaluation row");
        assert_eq!(pending[0].1, DeliveryUrgency::Steer, "max urgency wins");

        let taken = take_evaluation(&db, "watcher").await.unwrap();
        assert_eq!(taken, Some(DeliveryUrgency::Steer));
        // Drained.
        assert!(take_evaluation(&db, "watcher").await.unwrap().is_none());
        assert!(pending_evaluation_jobs(&db).await.unwrap().is_empty());
    }
}
